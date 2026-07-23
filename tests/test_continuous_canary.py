from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hyperliquid_copytrader.action_journal import ActionJournal, ActionRecord, ActionState
from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.continuous_canary import (
    CanaryProofError,
    MAINNET_PROOF_ACKNOWLEDGEMENT,
    MAX_REDUCTION_BOOK_AGE_MS,
    TerminalTruth,
    _chunk,
    _snapshot,
    run_two_account_ws_proof,
)
from hyperliquid_copytrader.continuous_config import (
    BoundContinuousPlan,
    BoundContinuousSlot,
    ContinuousPlan,
    ContinuousSlotConfig,
)
from hyperliquid_copytrader.continuous_executor import ExecutionAttempt
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.market_catalog import (
    CatalogMarket,
    CatalogRevision,
    MarketReadiness,
)
from hyperliquid_copytrader.market_stream import MarketStream
from hyperliquid_copytrader.precision import quantize_size
from hyperliquid_copytrader.runtime_lock import (
    AccountRuntimeFileLock,
    account_runtime_lock_path,
)
from hyperliquid_copytrader.ws_actions import (
    PostOutcome,
    PostResult,
    WsPostMux,
    build_ioc_action,
)


NOW = 1_800_000_000_000
SOURCE7 = "0x" + "1" * 40
FOLLOWER7 = "0x" + "2" * 40
SIGNER7 = "0x" + "3" * 40
MASTER7 = "0x" + "4" * 40
SOURCE1 = "0x" + "5" * 40
FOLLOWER1 = "0x" + "6" * 40
SIGNER1 = "0x" + "7" * 40
MASTER1 = "0x" + "8" * 40


class _Socket:
    async def send(self, _raw: str) -> None:
        raise AssertionError("fake lanes and truth reader must not use the socket")

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


def _redact(address: str) -> str:
    return address[:8] + "..." + address[-6:]


def _slot(
    name: str,
    market: str,
    source: str,
    follower: str,
) -> ContinuousSlotConfig:
    return ContinuousSlotConfig(
        slot=name,
        source_address=source,
        follower_account_address=follower,
        credential_profile_id=name,
        multiplier=Decimal("0.75"),
        max_order_notional_usd=Decimal("12"),
        max_gross_exposure_usd=Decimal("15"),
        max_open_positions=1,
        max_leverage=1,
        action_limit_per_minute=6,
        allowed_markets=(market,),
        enabled=True,
    )


def _bound() -> BoundContinuousPlan:
    acc7 = _slot("acc7", "xyz:EWY", SOURCE7, FOLLOWER7)
    acc1 = _slot("acc1", "BTC", SOURCE1, FOLLOWER1)
    plan = ContinuousPlan(
        version=1,
        network="mainnet",
        runtime_id="continuous-v1-proof",
        startup_baseline_only=True,
        max_combined_gross_usd=Decimal("30"),
        slots=(acc7, acc1),
        path=Path("safe-plan.json"),
        sha256="a" * 64,
    )
    return BoundContinuousPlan(
        plan=plan,
        slots=(
            BoundContinuousSlot(
                config=acc7,
                api_wallet_address=SIGNER7,
                api_private_key_file=Path("unused-acc7.key"),
                global_account_address=MASTER7,
                expected_account_mode="unified",
            ),
            BoundContinuousSlot(
                config=acc1,
                api_wallet_address=SIGNER1,
                api_private_key_file=Path("unused-acc1.key"),
                global_account_address=MASTER1,
                expected_account_mode="unified",
            ),
        ),
    )


def _catalog() -> CatalogRevision:
    return CatalogRevision(
        sequence=1,
        revision_id="catalog-proof",
        policy_version="continuous-v1",
        network="mainnet",
        observed_ms=NOW - 50,
        wire_dexes=("", "xyz"),
        markets=(
            CatalogMarket(
                symbol="BTC",
                dex="",
                asset_id=0,
                dex_index=0,
                universe_index=0,
                sz_decimals=2,
                max_leverage=50,
                readiness=MarketReadiness.READY,
            ),
            CatalogMarket(
                symbol="xyz:EWY",
                dex="xyz",
                asset_id=110_000,
                dex_index=1,
                universe_index=0,
                sz_decimals=2,
                max_leverage=10,
                readiness=MarketReadiness.READY,
            ),
        ),
        snapshot_sha256="b" * 64,
        dex_bracket_before_sha256="c" * 64,
        dex_bracket_after_sha256="c" * 64,
    )


def _market_stream(
    *,
    received_ms: int = NOW - 1,
    book_time_ms: int = NOW - 10,
) -> MarketStream:
    stream = MarketStream(catalog=_catalog(), active_markets=("BTC", "xyz:EWY"))
    epoch = stream.begin_connection(received_ms=received_ms - 5)
    for index, market in enumerate(("BTC", "xyz:EWY")):
        stream.apply(
            {
                "channel": "activeAssetCtx",
                "data": {"coin": market, "ctx": {"oraclePx": "100", "markPx": "100"}},
            },
            epoch=epoch,
            received_ms=received_ms - 4 + index * 2,
        )
        stream.apply(
            {
                "channel": "l2Book",
                "data": {
                    "coin": market,
                    "time": book_time_ms + index,
                    "levels": [
                        [{"px": "99.9", "sz": "10", "n": 1}],
                        [{"px": "100.1", "sz": "10", "n": 1}],
                    ],
                },
            },
            epoch=epoch,
            received_ms=received_ms - 3 + index * 2,
        )
    return stream


def _preflight(bound: BoundContinuousPlan) -> dict[str, Any]:
    slots = []
    for slot in bound.slots:
        slots.append(
            {
                "slot": slot.config.slot,
                "passed": True,
                "blockers": [],
                "follower_nonflat": False,
                "follower_open_order_count": 0,
                "source_dexes": ["", "xyz"] if slot.config.slot == "acc7" else [""],
                "follower_dexes": ["", "xyz"],
                "identity": {
                    "follower_role": "subaccount",
                    "signer_authorized": True,
                    "expected_account_mode": "unified",
                    "source_account_mode": "unified",
                    "follower_account_mode": "unified",
                    "signing_vault_address": _redact(slot.config.follower_account_address),
                    "action_principal": _redact(slot.global_account_address),
                },
                "collateral": {
                    "follower": {
                        "token": 0,
                        "coin": "USDC",
                        "total": "50",
                        "hold": "0",
                        "available": "50",
                        "valid": True,
                    }
                },
            }
        )
    return {
        "version": 1,
        "network": "mainnet",
        "plan_network": "mainnet",
        "network_explicit": True,
        "observed_ms": NOW - 10,
        "plan_sha256": bound.plan.sha256,
        "require_flat_and_order_free": True,
        "passed": True,
        "blockers": [],
        "slots": slots,
        "rest_requests": {"total": 1, "by_type": {}, "by_slot": {}},
    }


class _FakeVenue:
    def __init__(
        self,
        *,
        unknown_first_entry: bool = False,
        partial_entry: bool = False,
        channel_error_entry: bool = False,
        resolver_error: bool = False,
        cancel_after_entry_fill: bool = False,
    ) -> None:
        self.unknown_first_entry = unknown_first_entry
        self.partial_entry = partial_entry
        self.channel_error_entry = channel_error_entry
        self.resolver_error = resolver_error
        self.cancel_after_entry_fill = cancel_after_entry_fill
        self.events: list[str] = []
        self.positions = {"acc7": Decimal("0"), "acc1": Decimal("0")}
        self._actions: dict[str, tuple[str, NextAction]] = {}
        self._unknown_used = False

    def lane_factory(
        self,
        slot: BoundContinuousSlot,
        journal: ActionJournal,
        vault_address: str | None,
    ) -> _FakeLane:
        assert vault_address == slot.config.follower_account_address
        return _FakeLane(self, slot, journal)

    async def truth_reader(
        self,
        slot: BoundContinuousSlot,
        _required_epoch: int,
    ) -> TerminalTruth:
        size = self.positions[slot.config.slot]
        positions = () if size == 0 else ((slot.config.allowed_markets[0], size),)
        self.events.append(f"truth:{slot.config.slot}:{size}")
        return TerminalTruth(
            slot=slot.config.slot,
            observed_ms=NOW,
            positions=positions,
            open_order_count=0,
            account_mode="unified",
            available_collateral_usd=Decimal("50"),
            transport="ws_post",
        )


class _FakeLane:
    def __init__(
        self,
        venue: _FakeVenue,
        slot: BoundContinuousSlot,
        journal: ActionJournal,
    ) -> None:
        self.venue = venue
        self.slot = slot
        self.journal = journal

    async def execute_ioc(
        self,
        *,
        action: NextAction,
        asset_id: int,
        limit_px: Decimal,
        mux: WsPostMux,
        required_epoch: int,
        received_mono_ns: int | None = None,
    ) -> ExecutionAttempt:
        assert required_epoch == mux.connection_epoch
        assert received_mono_ns is not None
        phase = "close" if action.reduce_only else "entry"
        self.venue.events.append(f"execute:{self.slot.config.slot}:{phase}")
        nonce = self.journal.reserve_nonce(
            follower_account=self.slot.config.follower_account_address,
            api_wallet=self.slot.api_wallet_address,
            wall_ms=NOW + len(self.venue.events),
        )
        cloid = deterministic_cloid(
            self.slot.config.slot,
            phase,
            action.desired_id,
            nonce,
        )
        ioc = build_ioc_action(
            asset_id=asset_id,
            is_buy=action.side == "buy",
            size=action.size,
            limit_px=limit_px,
            reduce_only=action.reduce_only,
            cloid=cloid,
        )
        attempt_no = self.journal.next_attempt_no(
            follower_account=self.slot.config.follower_account_address,
            api_wallet=self.slot.api_wallet_address,
            desired_id=action.desired_id,
            market=action.market,
        )
        record = self.journal.prepare_action(
            follower_account=self.slot.config.follower_account_address,
            api_wallet=self.slot.api_wallet_address,
            desired_id=action.desired_id,
            market=action.market,
            attempt_no=attempt_no,
            cloid=cloid,
            nonce=nonce,
            requested_size=action.size,
            action_json=json.dumps(ioc.action, sort_keys=True),
            signed_payload_json=json.dumps({"action": ioc.action}, sort_keys=True),
            expires_after_ms=NOW + 60_000,
            request_id="pending",
            created_ms=NOW,
        )
        record = self.journal.mark_send_attempted(
            cloid,
            request_id=100 + len(self.venue.events),
            observed_ms=NOW + 1,
        )
        self.venue._actions[cloid] = (self.slot.config.slot, action)
        if not action.reduce_only and self.venue.cancel_after_entry_fill:
            self.journal.record_outcome(
                cloid,
                state=ActionState.FILLED,
                cumulative_filled_size=action.size,
                detail="fake fill before cancellation",
                observed_ms=NOW + 2,
            )
            self._apply_fill(action, action.size)
            raise asyncio.CancelledError
        if not action.reduce_only and self.venue.channel_error_entry:
            record = self.journal.record_outcome(
                cloid,
                state=ActionState.REJECTED,
                detail="server_rejected",
                observed_ms=NOW + 2,
            )
            result = PostResult(1, PostOutcome.REJECTED, None, "server_rejected")
        elif (
            not action.reduce_only
            and self.venue.unknown_first_entry
            and not self.venue._unknown_used
        ):
            self.venue._unknown_used = True
            record = self.journal.mark_unknown(
                cloid,
                detail="fake lost acknowledgement",
                observed_ms=NOW + 2,
            )
            result = PostResult(1, PostOutcome.UNKNOWN, None, "fake_unknown")
        else:
            fill = action.size
            outcome = PostOutcome.FILLED
            state = ActionState.FILLED
            if not action.reduce_only and self.venue.partial_entry:
                # Venue fills conform to the market size quantum. An impossible
                # half-quantum fill would manufacture uncloseable dust in the test.
                fill = quantize_size(action.size / Decimal("2"), 2)
                outcome = PostOutcome.PARTIALLY_FILLED
                state = ActionState.PARTIALLY_FILLED
            record = self.journal.record_outcome(
                cloid,
                state=state,
                cumulative_filled_size=fill,
                detail="fake definitive IOC",
                observed_ms=NOW + 2,
            )
            self._apply_fill(action, fill)
            result = PostResult(1, outcome, {}, "fake", fill)
        return ExecutionAttempt(
            record=record,
            result=result,
            received_to_send_ms=Decimal("4.5"),
            send_to_response_ms=Decimal("18.25"),
        )

    async def resolve_by_cloid(
        self,
        cloid: str,
        *,
        mux: WsPostMux,
        required_epoch: int,
    ) -> ActionRecord:
        assert required_epoch == mux.connection_epoch
        if self.venue.resolver_error:
            self.venue.events.append(f"resolve_error:{self.slot.config.slot}")
            raise ValueError("fake canceled IOC with a partial fill")
        slot_id, action = self.venue._actions[cloid]
        self.venue.events.append(f"resolve:{slot_id}")
        record = self.journal.record_outcome(
            cloid,
            state=ActionState.FILLED,
            cumulative_filled_size=action.size,
            detail="fake orderStatus by CLOID",
            observed_ms=NOW + 3,
        )
        self._apply_fill(action, action.size)
        return record

    def _apply_fill(self, action: NextAction, fill: Decimal) -> None:
        signed = fill if action.side == "buy" else -fill
        self.venue.positions[self.slot.config.slot] += signed


def _mux() -> tuple[WsPostMux, int]:
    mux = WsPostMux(response_timeout_s=0.1)
    return mux, mux.attach(_Socket())


def test_entry_freshness_remains_strict_but_reduce_only_window_survives_one_second_ack() -> None:
    stream = _market_stream(received_ms=NOW - 1_500, book_time_ms=NOW - 1_500)

    with pytest.raises(CanaryProofError, match="fresh same-epoch book"):
        _snapshot(stream, "xyz:EWY", NOW)
    reduction = _snapshot(
        stream,
        "xyz:EWY",
        NOW,
        max_age_ms=MAX_REDUCTION_BOOK_AGE_MS,
    )

    assert reduction.market == "xyz:EWY"


def test_canary_chunk_allows_only_exact_subminimum_reduce_only_close() -> None:
    snapshot = _snapshot(_market_stream(), "BTC", NOW)

    exact = _chunk(
        snapshot,
        False,
        Decimal("0.05"),
        Decimal("12"),
        reduce_only=True,
        current_position_size=Decimal("0.05"),
    )

    assert exact.size == Decimal("0.05")
    with pytest.raises(CanaryProofError, match="partial reduce-only IOC"):
        _chunk(
            snapshot,
            False,
            Decimal("0.05"),
            Decimal("12"),
            reduce_only=True,
            current_position_size=Decimal("0.10"),
        )
    with pytest.raises(CanaryProofError, match="risk-increasing IOC"):
        _chunk(
            snapshot,
            True,
            Decimal("0.05"),
            Decimal("12"),
            reduce_only=False,
            current_position_size=Decimal("0"),
        )


@pytest.mark.asyncio
async def test_happy_path_is_ws_only_bounded_durable_and_flat(tmp_path: Path) -> None:
    bound = _bound()
    venue = _FakeVenue()
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.passed is True
    assert result.status == "passed"
    assert result.mutated is True
    assert result.recovery_required is False
    assert result.action_count == 4
    assert result.proof_dir.is_relative_to(tmp_path / "HyperliquidCopytrader" / "runtime")
    summary = json.loads((result.proof_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["rest_action_count"] == 0
    assert summary["duplicate_cloid_count"] == 0
    assert summary["entry_count"] == 2
    assert summary["close_count"] == 2
    rows = [
        json.loads(line)
        for line in (result.proof_dir / "ws-actions.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["reduce_only"] for row in rows] == [False, True, False, True]
    assert all(Decimal("10") <= Decimal(row["wire_limit_notional_usd"]) <= 12 for row in rows)
    assert all(row["received_to_send_ms"] == "4.5" for row in rows)
    assert venue.positions == {"acc7": Decimal("0"), "acc1": Decimal("0")}
    with ActionJournal(result.proof_dir.parent / "mainnet-proof-actions.sqlite3") as journal:
        assert all(journal.get_action(row["cloid"]) is not None for row in rows)


@pytest.mark.asyncio
async def test_missing_acknowledgement_aborts_before_lane_or_mutation(tmp_path: Path) -> None:
    bound = _bound()
    venue = _FakeVenue()
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement="yes",
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.status == "aborted_before_mutation"
    assert result.mutated is False
    assert result.action_count == 0
    assert venue.events == []
    assert not (result.proof_dir / "ws-actions.jsonl").exists()


@pytest.mark.asyncio
async def test_stale_book_aborts_before_any_info_or_action(tmp_path: Path) -> None:
    bound = _bound()
    venue = _FakeVenue()
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(received_ms=NOW - 2_000),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.status == "aborted_before_mutation"
    assert any("fresh same-epoch book" in blocker for blocker in result.blockers)
    assert venue.events == []


@pytest.mark.asyncio
async def test_unknown_entry_is_resolved_by_cloid_before_reduce_only_close(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(unknown_first_entry=True)
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.passed is True
    entry_index = venue.events.index("execute:acc7:entry")
    resolve_index = venue.events.index("resolve:acc7")
    close_index = venue.events.index("execute:acc7:close")
    assert entry_index < resolve_index < close_index


@pytest.mark.asyncio
async def test_partial_entry_at_venue_quantum_gets_exact_dust_cleanup(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(partial_entry=True)
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.passed is True
    assert result.status == "passed"
    assert result.recovery_required is False
    assert result.action_count == 4
    assert venue.positions["acc7"] == 0
    assert venue.positions["acc1"] == 0
    assert "execute:acc7:close" in venue.events
    truth = json.loads((result.proof_dir / "terminal-truth.json").read_text(encoding="utf-8"))
    assert truth["observations"][-1]["flat"] is True


@pytest.mark.asyncio
async def test_incomplete_all_dex_preflight_and_old_exchange_book_abort(
    tmp_path: Path,
) -> None:
    bound = _bound()
    preflight = _preflight(bound)
    preflight["slots"][0]["follower_dexes"] = [""]
    venue = _FakeVenue()
    mux, epoch = _mux()

    incomplete = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=preflight,
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path / "incomplete",
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )
    assert incomplete.status == "aborted_before_mutation"
    assert any("every catalog DEX" in blocker for blocker in incomplete.blockers)

    mux, epoch = _mux()
    old_exchange_book = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(book_time_ms=NOW - 2_000),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path / "old-book",
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )
    assert old_exchange_book.status == "aborted_before_mutation"
    assert any("fresh same-epoch book" in blocker for blocker in old_exchange_book.blockers)
    assert venue.events == []


@pytest.mark.asyncio
async def test_undocumented_channel_error_never_causes_replacement(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(channel_error_entry=True)
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.status == "recovery_required"
    assert result.action_count == 1
    assert venue.events.count("execute:acc7:entry") == 1
    assert not any(event.endswith(":close") for event in venue.events)
    assert not any(event.startswith("execute:acc1") for event in venue.events)


@pytest.mark.asyncio
async def test_order_status_parser_failure_is_contained_without_blind_close(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(unknown_first_entry=True, resolver_error=True)
    mux, epoch = _mux()
    clock_value = [NOW]

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: clock_value[0],
    )

    assert result.status == "recovery_required"
    assert result.action_count == 1
    assert venue.events.count("resolve_error:acc7") == 1
    assert "execute:acc7:close" not in venue.events


@pytest.mark.asyncio
async def test_next_invocation_resolves_prior_unknown_before_any_new_entry(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(unknown_first_entry=True, resolver_error=True)
    mux, epoch = _mux()
    clock_value = [NOW]
    arguments = dict(
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: clock_value[0],
    )

    first = await run_two_account_ws_proof(bound, **arguments)
    assert first.recovery_required is True
    assert venue.events.count("execute:acc7:entry") == 1

    venue.resolver_error = False
    clock_value[0] += 1
    second = await run_two_account_ws_proof(bound, **arguments)

    assert second.recovery_required is True
    assert venue.events.count("execute:acc7:entry") == 1
    assert "resolve:acc7" in venue.events
    assert any("not flat/order-free" in blocker for blocker in second.blockers)


@pytest.mark.asyncio
async def test_cancellation_after_fill_writes_recovery_artifacts_and_terminal_truth(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue(cancel_after_entry_fill=True)
    mux, epoch = _mux()

    result = await run_two_account_ws_proof(
        bound,
        acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
        preflight_report=_preflight(bound),
        catalog=_catalog(),
        market_stream=_market_stream(),
        mux=mux,
        required_epoch=epoch,
        local_appdata=tmp_path,
        lane_factory=venue.lane_factory,
        truth_reader=venue.truth_reader,
        now_ms=lambda: NOW,
    )

    assert result.status == "recovery_required"
    assert result.mutated is True
    assert any("CancelledError" in blocker for blocker in result.blockers)
    summary = json.loads((result.proof_dir / "summary.json").read_text(encoding="utf-8"))
    truth = json.loads((result.proof_dir / "terminal-truth.json").read_text(encoding="utf-8"))
    assert summary["recovery_required"] is True
    assert truth["observations"][-1]["flat"] is False


@pytest.mark.asyncio
async def test_busy_follower_runtime_lock_aborts_before_key_or_action(
    tmp_path: Path,
) -> None:
    bound = _bound()
    venue = _FakeVenue()
    mux, epoch = _mux()
    lock_dir = tmp_path / "runtime-locks"
    follower_lock = AccountRuntimeFileLock(
        account_runtime_lock_path(
            lock_dir,
            network="mainnet",
            action_account=FOLLOWER7,
        )
    )

    with follower_lock:
        result = await run_two_account_ws_proof(
            bound,
            acknowledgement=MAINNET_PROOF_ACKNOWLEDGEMENT,
            preflight_report=_preflight(bound),
            catalog=_catalog(),
            market_stream=_market_stream(),
            mux=mux,
            required_epoch=epoch,
            local_appdata=tmp_path / "artifacts",
            runtime_lock_dir=lock_dir,
            lane_factory=venue.lane_factory,
            truth_reader=venue.truth_reader,
            now_ms=lambda: NOW,
        )

    assert result.status == "aborted_before_mutation"
    assert result.mutated is False
    assert any("runtime locks unavailable" in blocker for blocker in result.blockers)
    assert venue.events == []
