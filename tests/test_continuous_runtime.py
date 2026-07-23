from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from hyperliquid_copytrader import continuous_runtime
from hyperliquid_copytrader.action_journal import ActionJournal, ActionRecord
from hyperliquid_copytrader.account_stream import FillRecord
from hyperliquid_copytrader.continuous_config import (
    BoundContinuousPlan,
    BoundContinuousSlot,
    ContinuousPlan,
    ContinuousSlotConfig,
)
from hyperliquid_copytrader.continuous_executor import ContinuousSignerLane, ExecutionAttempt
from hyperliquid_copytrader.continuous_runtime import (
    ContinuousRuntime,
    Dispatch,
    FollowerTruth,
    RuntimeState,
)
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.market_catalog import CatalogMarket, CatalogRevision, MarketReadiness
from hyperliquid_copytrader.models import Position
from hyperliquid_copytrader.precision import aggressive_ioc_price
from hyperliquid_copytrader.runtime_lock import RuntimeFileLockBusy
from hyperliquid_copytrader.ws_actions import PostOutcome, PostResult, WsPostMux


NOW = 1_000_000


class _Socket:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def send(self, _message: str) -> None:
        return None


class _Journal:
    def recovery_actions(self, **_kwargs: Any) -> tuple[()]:
        return ()

    def recent_send_attempts(self, **_kwargs: Any) -> tuple[()]:
        return ()


class _Lane:
    def __init__(self, follower: str, wallet: str) -> None:
        self.follower_account = follower
        self.api_wallet_address = wallet
        self.vault_address = follower
        self.journal = cast(ActionJournal, _Journal())
        self.actions: list[tuple[Any, Decimal, int]] = []
        self.outcome = PostOutcome.FILLED
        self.reason = "fake"
        self.filled_size: Decimal | None = None
        self.unresolved: dict[str, Decimal] = {}

    def recover_provably_unsent(self) -> tuple[()]:
        return ()

    def unresolved_signed_remaining(self) -> dict[str, Decimal]:
        return dict(self.unresolved)

    async def resolve_by_cloid(self, _cloid: str, **_kwargs: Any) -> ActionRecord:
        return cast(ActionRecord, object())

    async def execute_ioc(self, **kwargs: Any) -> ExecutionAttempt:
        action = kwargs["action"]
        self.actions.append((action, kwargs["limit_px"], kwargs["asset_id"]))
        filled = self.filled_size
        if filled is None:
            filled = (
                action.size
                if self.outcome in {PostOutcome.FILLED, PostOutcome.PARTIALLY_FILLED}
                else Decimal(0)
            )
        result = PostResult(1, self.outcome, {}, self.reason, filled)
        return ExecutionAttempt(cast(ActionRecord, object()), result, None, Decimal("1"))


class _LaneBarrier:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.started = 0
        self.ready = asyncio.Event()
        self.release = asyncio.Event()


class _BarrierLane(_Lane):
    def __init__(self, follower: str, wallet: str, barrier: _LaneBarrier) -> None:
        super().__init__(follower, wallet)
        self.barrier = barrier

    async def execute_ioc(self, **kwargs: Any) -> ExecutionAttempt:
        action = kwargs["action"]
        self.actions.append((action, kwargs["limit_px"], kwargs["asset_id"]))
        outcome, reason, filled = self.outcome, self.reason, self.filled_size
        if filled is None:
            filled = action.size if outcome in {PostOutcome.FILLED, PostOutcome.PARTIALLY_FILLED} else Decimal(0)
        self.barrier.started += 1
        if self.barrier.started >= self.barrier.expected:
            self.barrier.ready.set()
        await self.barrier.release.wait()
        result = PostResult(1, outcome, {}, reason, filled)
        return ExecutionAttempt(cast(ActionRecord, object()), result, None, Decimal("1"))


class _FollowerHook:
    def __init__(self) -> None:
        self.truths: dict[str, FollowerTruth] = {}

    async def __call__(self, *, slot, mux, epoch, now_ms):
        del mux, epoch
        return self.truths.get(slot.config.slot, FollowerTruth({}, Decimal("100"), now_ms))


class _DynamicFollowerHook(_FollowerHook):
    def __init__(self) -> None:
        super().__init__()
        self.full_calls: list[str] = []
        self.dex_calls: list[tuple[str, str, bool]] = []

    async def __call__(self, *, slot, mux, epoch, now_ms):
        self.full_calls.append(slot.config.slot)
        return await super().__call__(slot=slot, mux=mux, epoch=epoch, now_ms=now_ms)

    async def refresh_dex(self, *, slot, dex, mux, epoch, now_ms, audit_open_orders=False):
        audit = bool(audit_open_orders)
        del mux, epoch, now_ms
        self.dex_calls.append((slot.config.slot, dex, audit))
        return {}

    def replace_catalog(self, catalog) -> None:
        del catalog


class _BlockingFollowerHook:
    def __init__(self, truth: FollowerTruth) -> None:
        self.truth = truth
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, *, slot, mux, epoch, now_ms):
        del slot, mux, epoch, now_ms
        self.started.set()
        await self.release.wait()
        return self.truth


class _GapRepair:
    def __init__(self, source: str) -> None:
        self.source = source
        self.calls = 0

    async def __call__(self, *, slot, before, after):
        del slot
        self.calls += 1
        assert before == {}
        assert after == {"BTC": Decimal("0.2")}
        return (
            FillRecord(
                account=self.source,
                market="BTC",
                tx_hash="0xrepair",
                tid="100",
                time_ms=NOW + 1,
                side="buy",
                size=Decimal("0.2"),
                price=Decimal("100"),
                start_position=Decimal("0"),
                oid=100,
                received_ms=NOW + 1,
                connection_epoch=2,
                is_snapshot=False,
            ),
        )


class _StaticGapRepair:
    def __init__(
        self,
        fills: tuple[FillRecord, ...],
        *,
        before: dict[str, Decimal],
        after: dict[str, Decimal],
    ) -> None:
        self.fills, self.before, self.after = fills, before, after

    async def __call__(self, *, slot, before, after):
        del slot
        assert before == self.before
        assert after == self.after
        return self.fills


def _address(number: int) -> str:
    return "0x" + f"{number:040x}"


def _catalog(
    symbols: tuple[str, ...] = ("BTC", "ETH"),
    *,
    revision_id: str = "catalog-one",
    sequence: int = 1,
) -> CatalogRevision:
    markets = tuple(
        CatalogMarket(
            symbol=coin,
            dex="",
            asset_id=index,
            dex_index=0,
            universe_index=index,
            sz_decimals=3,
            max_leverage=20,
            readiness=MarketReadiness.READY,
        )
        for index, coin in enumerate(symbols)
    )
    return CatalogRevision(
        sequence=sequence,
        revision_id=revision_id,
        policy_version="test",
        network="mainnet",
        observed_ms=NOW,
        wire_dexes=("",),
        markets=markets,
        snapshot_sha256="a" * 64,
        dex_bracket_before_sha256="b" * 64,
        dex_bracket_after_sha256="b" * 64,
    )


def _two_dex_catalog() -> CatalogRevision:
    native = _catalog(("BTC",)).markets[0]
    hip3 = CatalogMarket(
        symbol="hyna:NEW",
        dex="hyna",
        asset_id=110_000,
        dex_index=1,
        universe_index=0,
        sz_decimals=2,
        max_leverage=10,
        readiness=MarketReadiness.READY,
    )
    return CatalogRevision(
        sequence=1,
        revision_id="catalog-two-dex",
        policy_version="test",
        network="mainnet",
        observed_ms=NOW,
        wire_dexes=("", "hyna"),
        markets=(native, hip3),
        snapshot_sha256="c" * 64,
        dex_bracket_before_sha256="d" * 64,
        dex_bracket_after_sha256="d" * 64,
    )


def _bound_plan(
    tmp_path: Path,
    *,
    count: int = 1,
    combined: str = "30",
    action_limit: int = 6,
    runtime_id: str = "runtime-one",
    allowed: tuple[tuple[str, ...], ...] | None = None,
    dynamic: bool = False,
    denied: tuple[tuple[str, ...], ...] | None = None,
) -> tuple[BoundContinuousPlan, dict[str, _Lane]]:
    configs, bound, lanes = [], [], {}
    for index in range(count):
        slot_id = f"slot{index + 1}"
        source, follower, wallet, master = (
            _address(10 + index),
            _address(20 + index),
            _address(30 + index),
            _address(40 + index),
        )
        config = ContinuousSlotConfig(
            slot=slot_id,
            source_address=source,
            follower_account_address=follower,
            credential_profile_id=slot_id,
            multiplier=Decimal("1"),
            max_order_notional_usd=Decimal("12"),
            max_gross_exposure_usd=Decimal("30"),
            max_open_positions=2,
            max_leverage=1,
            action_limit_per_minute=action_limit,
            allowed_markets=("BTC", "ETH") if allowed is None else allowed[index],
            enabled=True,
        )
        configs.append(config)
        bound.append(
            BoundContinuousSlot(
                config,
                wallet,
                tmp_path / f"{slot_id}.key",
                master,
                "unified",
                dynamic_market_eligibility=dynamic,
                denied_markets=() if denied is None else denied[index],
            )
        )
        lanes[slot_id] = _Lane(follower, wallet)
    plan = ContinuousPlan(
        version=1,
        network="mainnet",
        runtime_id=runtime_id,
        startup_baseline_only=True,
        max_combined_gross_usd=Decimal(combined),
        slots=tuple(configs),
        path=tmp_path / "plan.json",
        sha256=f"sha-{runtime_id}",
    )
    return BoundContinuousPlan(plan, tuple(bound)), lanes


def _runtime(
    tmp_path: Path,
    *,
    count: int = 1,
    combined: str = "30",
    action_limit: int = 6,
    runtime_id: str = "runtime-one",
    state_name: str = "state.sqlite3",
    hook: _FollowerHook | None = None,
    barrier: _LaneBarrier | None = None,
    source_modes: dict[str, str] | None = None,
    allowed: tuple[tuple[str, ...], ...] | None = None,
    catalog: CatalogRevision | None = None,
    dynamic: bool = False,
    denied: tuple[tuple[str, ...], ...] | None = None,
    journal: ActionJournal | None = None,
    execution_enabled: bool = True,
    use_default_lock_dir: bool = False,
    clock=lambda: NOW,
    action_clock=lambda: NOW,
    **kwargs: Any,
) -> tuple[ContinuousRuntime, dict[str, _Lane], _FollowerHook]:
    plan, lanes = _bound_plan(
        tmp_path,
        count=count,
        combined=combined,
        action_limit=action_limit,
        runtime_id=runtime_id,
        allowed=allowed,
        dynamic=dynamic,
        denied=denied,
    )
    if barrier is not None:
        lanes = {
            slot_id: _BarrierLane(lane.follower_account, lane.api_wallet_address, barrier)
            for slot_id, lane in lanes.items()
        }
    if journal is not None:
        if count != 1:
            raise ValueError("test journal injection supports one slot")
        lanes["slot1"].journal = journal
    follower_hook = hook or _FollowerHook()
    mux = WsPostMux()
    mux.attach(_Socket())
    runtime_kwargs: dict[str, Any] = {
        "lock_dir": None if use_default_lock_dir else tmp_path / "locks",
    }
    runtime = ContinuousRuntime(
        plan=plan,
        catalog=catalog or _catalog(),
        lanes=cast(dict[str, ContinuousSignerLane], lanes),
        mux=mux,
        follower_info=follower_hook,
        preflight_vaults={
            item.config.slot: item.config.follower_account_address for item in plan.slots
        },
        preflight_source_modes=source_modes or {item.config.slot: "unified" for item in plan.slots},
        state_path=tmp_path / state_name,
        execution_enabled=execution_enabled,
        monotonic_ms=clock,
        action_clock_ms=action_clock,
        **runtime_kwargs,
        **kwargs,
    )
    return runtime, lanes, follower_hook


def _positions(user: str, time_ms: int, **positions: str) -> dict[str, Any]:
    return {
        "channel": "allDexsClearinghouseState",
        "data": {
            "user": user,
            "clearinghouseStates": [
                [
                    "",
                    {
                        "time": time_ms,
                        "assetPositions": [
                            {
                                "position": {
                                    "coin": coin,
                                    "szi": size,
                                    "entryPx": "100",
                                    "leverage": {"type": "cross", "value": 1},
                                }
                            }
                            for coin, size in positions.items()
                        ],
                    },
                ]
            ],
        },
    }


@pytest.mark.asyncio
async def test_catalog_addition_updates_dynamic_scope_without_follower_audit(
    tmp_path: Path,
) -> None:
    hook = _DynamicFollowerHook()
    runtime, _, _ = _runtime(tmp_path, hook=hook, dynamic=True)
    try:
        change = await runtime.apply_catalog(
            _catalog(("BTC", "ETH", "SOL"), revision_id="catalog-two", sequence=2)
        )
        assert {item["coin"] for item in runtime.market_subscriptions} == {"BTC", "ETH", "SOL"}
        assert change.subscribe == (
            {"type": "activeAssetCtx", "coin": "SOL"},
            {"type": "l2Book", "coin": "SOL"},
            {"type": "bbo", "coin": "SOL"},
        )
        assert hook.full_calls == []
        assert hook.dex_calls == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_hot_added_market_reaches_existing_signer_lane_without_restart(
    tmp_path: Path,
) -> None:
    hook = _DynamicFollowerHook()
    runtime, lanes, _ = _runtime(tmp_path, hook=hook, dynamic=True)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)

        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW + 3,
                        tid=900,
                        coin="SOL",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
            drive=False,
        )
        assert lanes["slot1"].actions == []
        assert runtime.catalog_refresh_requested is True
        assert runtime.operational_status("slot1", now_ms=NOW + 3)["catalog_pending_markets"] == [
            "SOL"
        ]

        runtime.clear_catalog_refresh_request()
        await runtime.apply_catalog(
            _catalog(("BTC", "ETH", "SOL"), revision_id="catalog-two", sequence=2)
        )
        assert runtime.operational_status("slot1", now_ms=NOW + 3)["catalog_pending_markets"] == []
        await runtime.apply_market(
            _context("SOL"), epoch=market_epoch, received_ms=NOW + 4, drive=False
        )
        await runtime.apply_market(_book("SOL", NOW + 5), epoch=market_epoch, received_ms=NOW + 5)

        assert len(lanes["slot1"].actions) == 1
        action, _limit_px, asset_id = lanes["slot1"].actions[0]
        assert action.market == "SOL"
        assert asset_id == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_unknown_market_fill_is_reclassified_when_catalog_rejects_it(
    tmp_path: Path,
) -> None:
    runtime, lanes, _ = _runtime(tmp_path, dynamic=True)
    try:
        bound = runtime.plan.slots[0]
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                bound.config.source_address,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW + 3,
                        tid=902,
                        coin="SOL",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
            drive=False,
        )
        candidate = _catalog(("BTC", "ETH", "SOL"), revision_id="catalog-two", sequence=2)
        sol = candidate.market("SOL")
        assert sol is not None
        rejected_sol = replace(sol, collateral_token=1)
        await runtime.apply_catalog(
            replace(
                candidate,
                markets=tuple(
                    rejected_sol if market.symbol == "SOL" else market
                    for market in candidate.markets
                ),
            )
        )

        slot = runtime._slots["slot1"]
        assert slot.pending_catalog_markets == set()
        assert slot.attributable.get("SOL", Decimal(0)) == 0
        assert slot.unattributed["SOL"] == Decimal("0.2")
        assert lanes["slot1"].actions == []
        assert "SOL" not in {item["coin"] for item in runtime.market_subscriptions}
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_restart_reconstructs_pending_market_when_catalog_file_lags_slot_state(
    tmp_path: Path,
) -> None:
    runtime, _, _ = _runtime(tmp_path, dynamic=True)
    try:
        bound = runtime.plan.slots[0]
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                bound.config.source_address,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW + 3,
                        tid=903,
                        coin="SOL",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
            drive=False,
        )
        await runtime.apply_catalog(
            _catalog(("BTC", "ETH", "SOL"), revision_id="catalog-two", sequence=2)
        )
        assert runtime._slots["slot1"].pending_catalog_markets == set()
    finally:
        runtime.close()

    restored, _, _ = _runtime(tmp_path, dynamic=True)
    try:
        slot = restored._slots["slot1"]
        assert slot.attributable["SOL"] == Decimal("0.2")
        assert slot.pending_catalog_markets == {"SOL"}
        assert restored.catalog_refresh_requested is True
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_startup_audited_dex_does_not_repeat_first_action_audit(
    tmp_path: Path,
) -> None:
    hook = _DynamicFollowerHook()
    hook.truths["slot1"] = FollowerTruth({}, Decimal("100"), NOW, {"": NOW, "hyna": NOW})
    runtime, lanes, _ = _runtime(
        tmp_path,
        hook=hook,
        dynamic=True,
        catalog=_two_dex_catalog(),
        preflight_follower_dexes={"slot1": ("", "hyna")},
    )
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW + 2,
                        tid=910,
                        coin="hyna:NEW",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
            drive=False,
        )
        await runtime.apply_market(
            _context("hyna:NEW"), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )
        await runtime.apply_market(
            _book("hyna:NEW", NOW + 4), epoch=market_epoch, received_ms=NOW + 4
        )

        assert len(lanes["slot1"].actions) == 1
        assert hook.dex_calls == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_accepted_target_survives_first_use_dex_refresh_delay(
    tmp_path: Path,
) -> None:
    clock = [NOW]

    class SlowFirstUse(_DynamicFollowerHook):
        async def refresh_dex(self, **kwargs: Any):
            clock[0] += 6_000
            return await super().refresh_dex(**kwargs)

    hook = SlowFirstUse()
    runtime, lanes, _ = _runtime(
        tmp_path,
        hook=hook,
        dynamic=True,
        catalog=_two_dex_catalog(),
        clock=lambda: clock[0],
        max_source_fill_age_ms=5_000,
    )
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW,
                        tid=911,
                        coin="hyna:NEW",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW,
            drive=False,
        )
        await runtime.apply_market(
            _context("hyna:NEW"), epoch=market_epoch, received_ms=NOW, drive=False
        )
        result = await runtime.apply_market(
            _book("hyna:NEW", NOW), epoch=market_epoch, received_ms=NOW
        )

        assert len(lanes["slot1"].actions) == 1
        assert result[0].attempt is not None
        assert result[0].attempt.execution_context["leader_trigger_age_ms"] == 6_000
        assert result[0].attempt.execution_context["leader_trigger_admission_age_ms"] == 0
        assert result[0].attempt.execution_context["accepted_target_wait_ms"] == 6_000
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_hot_added_dex_waits_for_standard_source_equity_then_executes(
    tmp_path: Path,
) -> None:
    hook = _DynamicFollowerHook()
    runtime, lanes, _ = _runtime(
        tmp_path,
        hook=hook,
        dynamic=True,
        source_modes={"slot1": "standard"},
        catalog=_catalog(("BTC",), revision_id="catalog-native"),
    )
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW, account_value="100")

        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.2",
                        start="0",
                        time_ms=NOW + 3,
                        tid=901,
                        coin="hyna:NEW",
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
            drive=False,
        )
        assert runtime.catalog_refresh_requested is True
        runtime.clear_catalog_refresh_request()
        await runtime.apply_catalog(
            replace(
                _two_dex_catalog(),
                sequence=2,
                revision_id="catalog-hot-dex",
            )
        )
        assert runtime._slots["slot1"].account.source.baseline_complete is False
        await runtime.apply_market(
            _context("hyna:NEW"), epoch=market_epoch, received_ms=NOW + 4, drive=False
        )
        await runtime.apply_market(
            _book("hyna:NEW", NOW + 5), epoch=market_epoch, received_ms=NOW + 5
        )
        assert lanes["slot1"].actions == []

        await runtime.apply_source(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": source,
                    "clearinghouseStates": [
                        [
                            "",
                            {
                                "time": NOW + 6,
                                "assetPositions": [],
                                "marginSummary": {"accountValue": "100"},
                            },
                        ],
                        [
                            "hyna",
                            {
                                "time": NOW + 6,
                                "assetPositions": [
                                    {
                                        "position": {
                                            "coin": "NEW",
                                            "szi": "0.2",
                                            "entryPx": "100",
                                            "leverage": {"type": "cross", "value": 1},
                                        }
                                    }
                                ],
                                "marginSummary": {"accountValue": "50"},
                            },
                        ],
                    ],
                },
            },
            epoch=source_epoch,
            received_ms=NOW + 6,
        )

        assert len(lanes["slot1"].actions) == 1
        assert lanes["slot1"].actions[0][0].market == "hyna:NEW"
        assert hook.dex_calls == [("slot1", "hyna", True)]
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_first_use_of_new_dex_refreshes_only_relevant_follower(tmp_path: Path) -> None:
    hook = _DynamicFollowerHook()
    runtime, _, _ = _runtime(
        tmp_path,
        count=2,
        combined="60",
        hook=hook,
        dynamic=True,
        catalog=_two_dex_catalog(),
    )
    try:
        for slot in runtime._slots.values():
            slot.follower = FollowerTruth({}, Decimal("100"), NOW, {"": NOW})
        runtime._slots["slot1"].attributable["hyna:NEW"] = Decimal("1")
        await runtime.drive_slot("slot1", now_ms=NOW)
        assert hook.dex_calls == [("slot1", "hyna", True)]
    finally:
        runtime.close()


def _spot(user: str, time_ms: int, *, total: str = "100") -> dict[str, Any]:
    return {
        "channel": "spotState",
        "data": {
            "user": user,
            "time": time_ms,
            "balances": [{"coin": "USDC", "token": 0, "total": total, "hold": "0"}],
        },
    }


def _fills(user: str, *, snapshot: bool, fills: list[dict[str, Any]]) -> dict[str, Any]:
    return {"channel": "userFills", "data": {"user": user, "isSnapshot": snapshot, "fills": fills}}


def _twap_fills(user: str, *, snapshot: bool, fills: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "channel": "userTwapSliceFills",
        "data": {
            "user": user,
            "isSnapshot": snapshot,
            "twapSliceFills": fills,
        },
    }


def _fill(
    *,
    side: str,
    size: str,
    start: str,
    time_ms: int,
    tid: int,
    coin: str = "BTC",
    price: str = "100",
) -> dict[str, Any]:
    return {
        "hash": f"0x{tid:x}",
        "tid": tid,
        "time": time_ms,
        "coin": coin,
        "side": side,
        "sz": size,
        "px": price,
        "startPosition": start,
        "oid": tid,
    }


async def _baseline(
    runtime: ContinuousRuntime,
    slot: BoundContinuousSlot,
    epoch: int,
    *,
    time_ms: int,
    account_value: str | None = None,
    spot_total: str = "100",
    **positions: str,
) -> None:
    source = slot.config.source_address
    position_frame = _positions(source, time_ms, **positions)
    state = position_frame["data"]["clearinghouseStates"][0][1]
    if account_value is not None:
        state["marginSummary"] = {"accountValue": account_value}
    messages = [(position_frame, 0)]
    if any(
        spec.get("type") == "spotState" and spec.get("user") == source
        for spec in runtime.source_subscriptions
    ):
        messages.append((_spot(source, time_ms, total=spot_total), 1))
    fill_offset = 2 if len(messages) == 2 else 1
    messages.extend(
        (
            (_fills(source, snapshot=True, fills=[]), fill_offset),
            (_twap_fills(source, snapshot=True, fills=[]), fill_offset),
        )
    )
    for message, offset in messages:
        await runtime.apply_source(message, epoch=epoch, received_ms=time_ms + offset)


async def _prime_provable_close(tmp_path: Path, **runtime_kwargs: Any):
    runtime, lanes, hook = _runtime(tmp_path, **runtime_kwargs)
    bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
    source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
    await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
    await _baseline(runtime, bound, source_epoch, time_ms=NOW)
    await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[
                _fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=70),
                _fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=71, coin="ETH"),
            ],
        ),
        epoch=source_epoch,
        received_ms=NOW + 3,
        drive=False,
    )
    await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[_fill(side="A", size="0.11", start="0.11", time_ms=NOW + 4, tid=72)],
        ),
        epoch=source_epoch,
        received_ms=NOW + 4,
        drive=False,
    )
    hook.truths["slot1"] = FollowerTruth(
        {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW + 5
    )
    await runtime.reconcile_follower("slot1", now_ms=NOW + 5, drive=False)
    return runtime, lanes, market_epoch


def _context(coin: str, mark: str = "100") -> dict[str, Any]:
    return {
        "channel": "activeAssetCtx",
        "data": {"coin": coin, "ctx": {"oraclePx": mark, "markPx": mark}},
    }


def _book(
    coin: str, time_ms: int, *, bid: str = "99.9", ask: str = "100.1", size: str = "10"
) -> dict[str, Any]:
    return {
        "channel": "l2Book",
        "data": {
            "coin": coin,
            "time": time_ms,
            "levels": [[{"px": bid, "sz": size}], [{"px": ask, "sz": size}]],
        },
    }


def _bbo(
    coin: str,
    time_ms: int,
    *,
    bid: str = "99.9",
    ask: str = "100.1",
    size: str = "10",
) -> dict[str, Any]:
    return {
        "channel": "bbo",
        "data": {
            "coin": coin,
            "time": time_ms,
            "bbo": [
                {"px": bid, "sz": size, "n": 1},
                {"px": ask, "sz": size, "n": 1},
            ],
        },
    }


def _market_specs(*coins: str) -> tuple[dict[str, str], ...]:
    return tuple(
        spec
        for coin in sorted(coins)
        for spec in (
            {"type": "activeAssetCtx", "coin": coin},
            {"type": "l2Book", "coin": coin},
            {"type": "bbo", "coin": coin},
        )
    )


@pytest.mark.asyncio
async def test_startup_inventory_is_never_adopted_and_terminal_fill_is_folded(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        assert runtime.market_subscriptions == _market_specs("BTC", "ETH")
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW, BTC="1")
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )

        assert runtime.status("slot1")[0] is RuntimeState.RUNNING
        assert runtime.market_subscriptions == _market_specs("BTC", "ETH")
        assert lanes["slot1"].actions == []

        spot_fill = _fill(side="B", size="5", start="999", time_ms=NOW + 2, tid=99)
        spot_fill["coin"] = "@107"
        await runtime.apply_source(
            _fills(source, snapshot=False, fills=[spot_fill]),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        assert runtime.market_subscriptions == _market_specs("BTC", "ETH")
        assert lanes["slot1"].actions == []

        reduction = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="A", size="0.5", start="1", time_ms=NOW + 3, tid=1)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
        )
        assert reduction.market_change.added == ()
        assert lanes["slot1"].actions == []

        addition = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0.5", time_ms=NOW + 4, tid=2)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 4,
        )
        assert addition.market_change.added == ()
        assert addition.attempt is not None
        assert len(lanes["slot1"].actions) == 1

        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 5)
        await runtime.apply_market(_book("BTC", NOW + 6), epoch=market_epoch, received_ms=NOW + 6)
        assert len(lanes["slot1"].actions) == 1
        action, limit, _asset = lanes["slot1"].actions[0]
        assert Decimal("10") <= action.size * limit <= Decimal("12")

        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 7)
        assert len(lanes["slot1"].actions) == 1
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_disarmed_runtime_returns_candidate_without_invoking_signer_lane(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, execution_enabled=False)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 2, tid=200)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )
        result = await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4
        )

        assert result[0].reason == "execution disarmed"
        assert result[0].action is not None
        assert result[0].attempt is None
        assert lanes["slot1"].actions == []
        assert runtime._reservations == {}
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_shared_source_routing_and_bad_slot_are_isolated(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path, count=2, combined="60")
    try:
        epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        runtime.begin_market_connection(received_ms=NOW - 10)
        for slot in runtime.plan.slots:
            await runtime.reconcile_follower(slot.config.slot, now_ms=NOW)
            await _baseline(runtime, slot, epoch, time_ms=NOW)
        assert len(runtime.source_subscriptions) == 8
        assert runtime.slot_ids == ("slot1", "slot2")
        assert (
            runtime.source_slot_id(runtime.plan.slots[0].config.source_address.upper()) == "slot1"
        )
        assert runtime.source_slot_id(_address(999)) is None
        assert runtime.source_frame_status("slot1", received_ms=NOW + 2) == (True, True)

        source1 = runtime.plan.slots[0].config.source_address
        accepted = await runtime.apply_source(
            _fills(source1, snapshot=False, fills=[]), epoch=epoch, received_ms=NOW + 3
        )
        bad = await runtime.apply_source(
            _fills(
                source1,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="9", time_ms=NOW + 3, tid=3)],
            ),
            epoch=epoch,
            received_ms=NOW + 3,
        )
        assert accepted.source_frame_accepted is True
        assert bad.source_frame_accepted is False
        assert bad.state is RuntimeState.RECOVERING
        assert runtime.status("slot2")[0] is RuntimeState.RUNNING

        ignored = await runtime.apply_source(
            _fills(_address(999), snapshot=False, fills=[]), epoch=epoch, received_ms=NOW + 4
        )
        assert ignored.slot is None
        assert ignored.source_frame_accepted is False
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_standard_source_mode_is_pinned_and_uses_perp_equity(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, source_modes={"slot1": "standard"})
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW, account_value="100")

        assert tuple(spec["type"] for spec in runtime.source_subscriptions) == (
            "allDexsClearinghouseState",
            "userFills",
            "userTwapSliceFills",
        )
        assert runtime._slots["slot1"].identity["source_account_mode"] == "standard"
        assert runtime._slots["slot1"].identity["source_equity_basis"] == (
            "standard_sum_relevant_dex_account_value"
        )

        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=30)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        await runtime.apply_market(_book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4)

        assert len(lanes["slot1"].actions) == 1
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_bad_catalog_slot_and_zero_collateral_are_contained(tmp_path: Path) -> None:
    catalog_runtime, _lanes, _hook = _runtime(
        tmp_path / "catalog",
        count=2,
        combined="60",
        allowed=(("DOGE",), ("BTC",)),
    )
    try:
        assert catalog_runtime.status("slot1")[0] is RuntimeState.RECOVERING
        assert "absent from the pinned catalog" in catalog_runtime.status("slot1")[1]
        epoch = catalog_runtime.begin_source_connection(received_ms=NOW - 10)
        catalog_runtime.begin_market_connection(received_ms=NOW - 10)
        slot2 = catalog_runtime.plan.slots[1]
        await catalog_runtime.reconcile_follower("slot2", now_ms=NOW)
        await _baseline(catalog_runtime, slot2, epoch, time_ms=NOW)
        assert catalog_runtime.status("slot2")[0] is RuntimeState.RUNNING
    finally:
        catalog_runtime.close()

    zero_runtime, _lanes, _hook = _runtime(tmp_path / "zero")
    try:
        bound = zero_runtime.plan.slots[0]
        epoch = zero_runtime.begin_source_connection(received_ms=NOW - 10)
        zero_runtime.begin_market_connection(received_ms=NOW - 10)
        await zero_runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(zero_runtime, bound, epoch, time_ms=NOW, spot_total="0")
        assert zero_runtime.status("slot1") == (
            RuntimeState.RECOVERING,
            "source sizing equity is not positive and finite",
        )
    finally:
        zero_runtime.close()


@pytest.mark.asyncio
async def test_first_start_never_adopts_or_closes_preexisting_follower_position(
    tmp_path: Path,
) -> None:
    hook = _FollowerHook()
    hook.truths["slot1"] = FollowerTruth(
        {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW
    )
    runtime, lanes, _hook = _runtime(tmp_path, hook=hook)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=32, coin="ETH")],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
            drive=False,
        )
        blocked = await runtime.drive_slot("slot1", now_ms=NOW + 3)

        assert blocked.state is RuntimeState.RECOVERING
        assert blocked.reason == "startup follower exposure has no runtime attribution"
        assert lanes["slot1"].actions == []

        hook.truths["slot1"] = FollowerTruth({}, Decimal("100"), NOW + 4)
        await runtime.reconcile_follower("slot1", now_ms=NOW + 4, drive=False)
        assert runtime._slots["slot1"].initial_follower_nonflat is False
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_explicit_fail_close_ignores_stale_leader_and_flattens_follower(
    tmp_path: Path,
) -> None:
    hook = _FollowerHook()
    hook.truths["slot1"] = FollowerTruth(
        {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW
    )
    runtime, lanes, _hook = _runtime(tmp_path, hook=hook)
    try:
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW), epoch=market_epoch, received_ms=NOW, drive=False
        )
        runtime.request_fail_close(("slot1",), reason="planned run ended")

        closed = await runtime.drive_fail_close("slot1", now_ms=NOW)

        assert closed.attempt is not None
        action = lanes["slot1"].actions[0][0]
        assert action.side == "sell"
        assert action.size == Decimal("0.11")
        assert action.reduce_only is True
        assert lanes["slot1"].actions[0][1] == aggressive_ioc_price(
            Decimal("99.9"),
            is_buy=False,
            slippage_bps=Decimal("300"),
            sz_decimals=3,
        )
        assert runtime.follower_is_flat("slot1") is True
        assert runtime.status("slot1") == (
            RuntimeState.PAUSE_ENTRIES,
            "fail-close: planned run ended",
        )
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_explicit_operator_rearm_clears_durable_fail_close_latch(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path)
    runtime.request_fail_close(("slot1",), reason="planned run ended")
    runtime.close()

    restored, _lanes, _hook = _runtime(tmp_path)
    try:
        assert restored.fail_close_slots == ("slot1",)
        await restored.reconcile_follower("slot1", now_ms=NOW, drive=False)

        restored.operator_rearm(("slot1",))

        assert restored.fail_close_slots == ()
        assert restored.status("slot1") == (
            RuntimeState.RECOVERING,
            "operator re-arm awaiting live reconciliation",
        )
    finally:
        restored.close()

    verified, _lanes, _hook = _runtime(tmp_path)
    try:
        assert verified.fail_close_slots == ()
    finally:
        verified.close()


@pytest.mark.asyncio
async def test_rearm_after_flat_closeout_waits_for_a_new_leader_fill(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    bound = runtime.plan.slots[0]
    source = bound.config.source_address
    source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
    await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
    await _baseline(runtime, bound, source_epoch, time_ms=NOW)
    await runtime.apply_market(
        _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
    )
    await runtime.apply_market(
        _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
    )
    opened = await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=221)],
        ),
        epoch=source_epoch,
        received_ms=NOW + 3,
    )
    assert opened.attempt is not None
    assert runtime._slots["slot1"].attributable == {"BTC": Decimal("0.11")}
    runtime.request_fail_close(("slot1",), reason="operator stop requested")
    closed = await runtime.drive_fail_close("slot1", now_ms=NOW + 4)
    assert closed.attempt is not None
    assert runtime.follower_is_flat("slot1") is True
    runtime.close()

    restored_hook = _FollowerHook()
    restored_hook.truths["slot1"] = FollowerTruth({}, Decimal("100"), NOW + 10)
    restored, restored_lanes, _hook = _runtime(tmp_path, hook=restored_hook)
    try:
        source_epoch = restored.begin_source_connection(received_ms=NOW + 9)
        market_epoch = restored.begin_market_connection(received_ms=NOW + 9)
        await restored.reconcile_follower("slot1", now_ms=NOW + 10, drive=False)
        await _baseline(
            restored,
            restored.plan.slots[0],
            source_epoch,
            time_ms=NOW + 10,
            BTC="0.11",
        )

        restored.operator_rearm(("slot1",))
        slot = restored._slots["slot1"]
        assert slot.attributable == {}
        assert slot.unattributed == {"BTC": Decimal("0.11")}
        assert slot.triggers == {}
        idle = await restored.drive_slot("slot1", now_ms=NOW + 12)
        assert idle.attempt is None
        assert restored_lanes["slot1"].actions == []

        await restored.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 13, drive=False
        )
        await restored.apply_market(
            _book("BTC", NOW + 14), epoch=market_epoch, received_ms=NOW + 14, drive=False
        )
        fresh = await restored.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.11",
                        start="0.11",
                        time_ms=NOW + 15,
                        tid=222,
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 15,
        )
        assert fresh.attempt is not None
        assert restored_lanes["slot1"].actions[-1][0].size == Decimal("0.110")
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_restart_recognizes_exact_durable_nonflat_follower_truth(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    bound = runtime.plan.slots[0]
    source = bound.config.source_address
    source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
    await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
    await _baseline(runtime, bound, source_epoch, time_ms=NOW)
    await runtime.apply_market(
        _context("BTC"), epoch=market_epoch, received_ms=NOW + 2, drive=False
    )
    await runtime.apply_market(
        _book("BTC", NOW + 3), epoch=market_epoch, received_ms=NOW + 3, drive=False
    )
    opened = await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 4, tid=208)],
        ),
        epoch=source_epoch,
        received_ms=NOW + 4,
    )
    assert opened.attempt is not None
    assert lanes["slot1"].actions[0][0].side == "buy"

    await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[_fill(side="A", size="0.11", start="0.11", time_ms=NOW + 5, tid=209)],
        ),
        epoch=source_epoch,
        received_ms=NOW + 5,
        drive=False,
    )
    slot = runtime._slots["slot1"]
    assert slot.attributable == {}
    assert slot.follower is not None
    assert slot.follower.positions["BTC"].size == Decimal("0.11")
    runtime.close()

    restored_hook = _FollowerHook()
    restored_hook.truths["slot1"] = FollowerTruth(
        {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW + 10
    )
    restored, restored_lanes, _hook = _runtime(tmp_path, hook=restored_hook)
    try:
        source_epoch = restored.begin_source_connection(received_ms=NOW + 9)
        market_epoch = restored.begin_market_connection(received_ms=NOW + 9)
        await restored.reconcile_follower("slot1", now_ms=NOW + 10, drive=False)
        restored_slot = restored._slots["slot1"]
        assert restored_slot.initial_follower_nonflat is False
        await _baseline(
            restored,
            restored.plan.slots[0],
            source_epoch,
            time_ms=NOW + 10,
        )
        await restored.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 13, drive=False
        )
        closed = await restored.apply_market(
            _book("BTC", NOW + 14), epoch=market_epoch, received_ms=NOW + 14
        )
        assert closed[0].attempt is not None
        action = restored_lanes["slot1"].actions[0][0]
        assert action.side == "sell"
        assert action.size == Decimal("0.11")
        assert action.reduce_only is True
        assert restored_slot.follower is not None
        assert restored_slot.follower.positions == {}
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_slow_follower_refresh_cannot_block_or_overwrite_a_fresh_action(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 3), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )

        blocked = _BlockingFollowerHook(FollowerTruth({}, Decimal("100"), NOW + 4))
        runtime.follower_info = blocked
        refresh = asyncio.create_task(
            runtime.reconcile_follower("slot1", now_ms=NOW + 4, drive=False)
        )
        await blocked.started.wait()

        copied = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 5, tid=210)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 5,
        )
        assert copied.attempt is not None
        assert lanes["slot1"].actions[0][0].side == "buy"
        assert blocked.release.is_set() is False

        blocked.release.set()
        stale = await refresh
        slot = runtime._slots["slot1"]
        assert stale.reason == "stale follower truth discarded after concurrent action"
        assert slot.follower is not None
        assert slot.follower.positions["BTC"].size == Decimal("0.11")
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_source_truth_advances_while_same_slot_ioc_is_waiting(tmp_path: Path) -> None:
    barrier = _LaneBarrier(expected=1)
    runtime, lanes, _hook = _runtime(tmp_path, barrier=barrier)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )

        first = asyncio.create_task(
            runtime.apply_source(
                _fills(
                    source,
                    snapshot=False,
                    fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=301)],
                ),
                epoch=source_epoch,
                received_ms=NOW + 3,
            )
        )
        await barrier.ready.wait()

        latest = await asyncio.wait_for(
            runtime.apply_source(
                _fills(
                    source,
                    snapshot=False,
                    fills=[
                        _fill(
                            side="A",
                            size="0.11",
                            start="0.11",
                            time_ms=NOW + 4,
                            tid=302,
                        )
                    ],
                ),
                epoch=source_epoch,
                received_ms=NOW + 4,
                drive=False,
            ),
            timeout=0.2,
        )
        assert latest.source_frame_accepted is True
        assert runtime._slots["slot1"].attributable == {}

        barrier.release.set()
        assert (await first).attempt is not None
        follow_up = await runtime.drive_slot("slot1", now_ms=NOW + 5)
        assert follow_up.attempt is not None
        assert [item[0].side for item in lanes["slot1"].actions] == ["buy", "sell"]
        assert lanes["slot1"].actions[1][0].reduce_only is True
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_action_waiting_behind_ioc_executes_only_the_latest_accepted_target(
    tmp_path: Path,
) -> None:
    barrier = _LaneBarrier(expected=1)
    monotonic = [0]
    runtime, lanes, _hook = _runtime(
        tmp_path,
        barrier=barrier,
        clock=lambda: monotonic[0],
    )
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )

        first = asyncio.create_task(
            runtime.apply_source(
                _fills(
                    source,
                    snapshot=False,
                    fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=311)],
                ),
                epoch=source_epoch,
                received_ms=NOW + 3,
            )
        )
        await barrier.ready.wait()
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0.11", time_ms=NOW + 4, tid=312)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 4,
            drive=False,
        )
        queued = asyncio.create_task(runtime.drive_slot("slot1", now_ms=NOW + 4))
        await asyncio.sleep(0)

        monotonic[0] = 6_000
        barrier.release.set()
        assert (await first).attempt is not None
        follow_up = await queued
        assert follow_up.attempt is not None
        assert follow_up.attempt.execution_context["leader_trigger_time_ms"] == NOW + 4
        assert follow_up.attempt.execution_context["leader_trigger_age_ms"] == 6_000
        assert follow_up.attempt.execution_context["leader_trigger_admission_age_ms"] == 0
        assert follow_up.attempt.execution_context["accepted_target_wait_ms"] == 6_000
        assert len(lanes["slot1"].actions) == 2
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_refresh_started_during_ioc_cannot_overwrite_the_fill(tmp_path: Path) -> None:
    barrier = _LaneBarrier(expected=1)
    runtime, _lanes, _hook = _runtime(tmp_path, barrier=barrier)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        action = asyncio.create_task(
            runtime.apply_source(
                _fills(
                    source,
                    snapshot=False,
                    fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=303)],
                ),
                epoch=source_epoch,
                received_ms=NOW + 3,
            )
        )
        await barrier.ready.wait()

        post_fill = _FollowerHook()
        post_fill.truths["slot1"] = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW + 4
        )
        runtime.follower_info = post_fill
        refresh = await runtime.reconcile_follower("slot1", now_ms=NOW + 4, drive=False)
        assert refresh.reason == "follower refresh deferred during in-flight action"

        barrier.release.set()
        assert (await action).attempt is not None

        follower = runtime._slots["slot1"].follower
        assert follower is not None
        assert follower.positions["BTC"].size == Decimal("0.11")
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_checkpoint_restores_post_baseline_fill_before_submit(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    bound = runtime.plan.slots[0]
    source = bound.config.source_address
    epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    runtime.begin_market_connection(received_ms=NOW - 10)
    await runtime.reconcile_follower("slot1", now_ms=NOW)
    await _baseline(runtime, bound, epoch, time_ms=NOW)
    await runtime.apply_source(
        _fills(
            source,
            snapshot=False,
            fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=4)],
        ),
        epoch=epoch,
        received_ms=NOW + 3,
    )
    assert lanes["slot1"].actions == []
    runtime.close()

    restored, restored_lanes, _hook = _runtime(tmp_path)
    try:
        epoch = restored.begin_source_connection(received_ms=NOW + 4)
        market_epoch = restored.begin_market_connection(received_ms=NOW + 4)
        await restored.reconcile_follower("slot1", now_ms=NOW + 5)
        await _baseline(restored, restored.plan.slots[0], epoch, time_ms=NOW + 5, BTC="0.2")
        assert restored.market_subscriptions
        await restored.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 6)
        await restored.apply_market(_book("BTC", NOW + 7), epoch=market_epoch, received_ms=NOW + 7)
        assert len(restored_lanes["slot1"].actions) == 1
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_reconnect_gap_uses_injected_fill_repair_instead_of_guessing(tmp_path: Path) -> None:
    plan, _lanes = _bound_plan(tmp_path)
    repair = _GapRepair(plan.slots[0].config.source_address)
    runtime, _lanes, _hook = _runtime(tmp_path, gap_repair=repair)
    try:
        bound = runtime.plan.slots[0]
        first = runtime.begin_source_connection(received_ms=NOW - 10)
        runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, first, time_ms=NOW)
        runtime.connection_gap("source", epoch=first, reason="test disconnect")

        second = runtime.begin_source_connection(received_ms=NOW + 1)
        await _baseline(runtime, bound, second, time_ms=NOW + 2, BTC="0.2")

        assert repair.calls == 1
        assert runtime.market_subscriptions == _market_specs("BTC", "ETH")
        assert runtime.status("slot1")[0] is RuntimeState.RECOVERING
        assert "market BTC" in runtime.status("slot1")[1]
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_old_no_fill_response_cannot_throttle_a_newer_source_revision(
    tmp_path: Path,
) -> None:
    barrier = _LaneBarrier(expected=1)
    runtime, lanes, _hook = _runtime(tmp_path, barrier=barrier)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 1, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 2), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"

        first = asyncio.create_task(
            runtime.apply_source(
                _fills(
                    source,
                    snapshot=False,
                    fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=811)],
                ),
                epoch=source_epoch,
                received_ms=NOW + 3,
            )
        )
        await barrier.ready.wait()
        first_size = lane.actions[0][0].size

        # This fill refreshes source identity and the leader price cap, but is
        # too small to alter the venue-rounded follower target.
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.0001",
                        start="0.11",
                        price="100.2",
                        time_ms=NOW + 4,
                        tid=812,
                    ),
                    _fill(
                        side="A",
                        size="0.0001",
                        start="0.1101",
                        price="99.8",
                        time_ms=NOW + 4,
                        tid=813,
                    ),
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 4,
            drive=False,
        )
        lane.outcome = PostOutcome.FILLED
        lane.reason = "filled"
        barrier.release.set()
        assert (await first).attempt is not None

        follow_up = await runtime.drive_slot("slot1", now_ms=NOW + 5)
        assert follow_up.attempt is not None
        assert len(lane.actions) == 2
        assert runtime._slots["slot1"].source_revisions["BTC"] == 3
        assert lane.actions[1][0].size == first_size == Decimal("0.110")
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_failed_attribution_is_atomic_and_does_not_advance_trusted_source(
    tmp_path: Path,
) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, epoch, time_ms=NOW, BTC="0.2")
        slot = runtime._slots["slot1"]
        before_unattributed = dict(slot.unattributed)
        before_fills = dict(slot.applied_fills)
        before_source = dict(slot.expected_source or {})
        inconsistent = FillRecord(
            account=bound.config.source_address,
            market="BTC",
            tx_hash="0xinconsistent",
            tid="301",
            time_ms=NOW + 1,
            side="buy",
            size=Decimal("0.1"),
            price=Decimal("100"),
            start_position=Decimal("0.2"),
            oid=301,
            received_ms=NOW + 1,
            connection_epoch=epoch,
            is_snapshot=False,
        )

        with pytest.raises(ValueError, match="attribution does not match"):
            runtime._attribute(slot, (inconsistent,), recovered_at_ms=NOW + 1)

        assert slot.unattributed == before_unattributed
        assert slot.applied_fills == before_fills
        assert slot.expected_source == before_source
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_runtime_store_never_persists_an_untrusted_reconnect_snapshot(
    tmp_path: Path,
) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path)
    bound = runtime.plan.slots[0]
    epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
    await _baseline(runtime, bound, epoch, time_ms=NOW, BTC="0.2")
    slot = runtime._slots["slot1"]
    slot.account.apply(
        _positions(bound.config.source_address, NOW + 3, BTC="0.4"),
        epoch=epoch,
        received_ms=NOW + 3,
    )
    runtime._store.save(slot)
    runtime.close()

    restored, _lanes, _hook = _runtime(tmp_path)
    try:
        assert restored._slots["slot1"].expected_source == {"BTC": Decimal("0.2")}
    finally:
        restored.close()


@pytest.mark.asyncio
async def test_stale_gap_increase_is_never_released_by_a_later_fresh_fill(
    tmp_path: Path,
) -> None:
    source = _address(10)
    repair = _StaticGapRepair(
        (
            FillRecord(
                account=source,
                market="BTC",
                tx_hash="0xstaleincrease",
                tid="201",
                time_ms=NOW + 1,
                side="buy",
                size=Decimal("0.2"),
                price=Decimal("100"),
                start_position=Decimal("0"),
                oid=201,
                received_ms=NOW + 1,
                connection_epoch=2,
                is_snapshot=False,
            ),
        ),
        before={},
        after={"BTC": Decimal("0.2")},
    )
    runtime, lanes, _hook = _runtime(tmp_path, gap_repair=repair)
    try:
        bound = runtime.plan.slots[0]
        first = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, first, time_ms=NOW)
        runtime.connection_gap("source", epoch=first, reason="long disconnect")

        second = runtime.begin_source_connection(received_ms=NOW + 10_000)
        await _baseline(runtime, bound, second, time_ms=NOW + 10_000, BTC="0.2")
        slot = runtime._slots["slot1"]
        assert slot.attributable == {}
        assert slot.unattributed == {"BTC": Decimal("0.2")}
        assert runtime.market_subscriptions == _market_specs("BTC", "ETH")

        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.1",
                        start="0.2",
                        time_ms=NOW + 10_002,
                        tid=202,
                    )
                ],
            ),
            epoch=second,
            received_ms=NOW + 10_002,
            drive=False,
        )
        assert slot.attributable == {"BTC": Decimal("0.1")}
        assert slot.unattributed == {"BTC": Decimal("0.2")}

        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 10_003, drive=False
        )
        result = await runtime.apply_market(
            _book("BTC", NOW + 10_004),
            epoch=market_epoch,
            received_ms=NOW + 10_004,
        )
        assert result[0].attempt is not None
        assert lanes["slot1"].actions[0][0].size == Decimal("0.1")
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_stale_gap_reduction_consumes_existing_attributable_exposure(
    tmp_path: Path,
) -> None:
    source = _address(10)
    repair = _StaticGapRepair(
        (
            FillRecord(
                account=source,
                market="BTC",
                tx_hash="0xstalereduction",
                tid="203",
                time_ms=NOW + 4,
                side="sell",
                size=Decimal("0.1"),
                price=Decimal("100"),
                start_position=Decimal("0.2"),
                oid=203,
                received_ms=NOW + 4,
                connection_epoch=2,
                is_snapshot=False,
            ),
        ),
        before={"BTC": Decimal("0.2")},
        after={"BTC": Decimal("0.1")},
    )
    runtime, _lanes, _hook = _runtime(tmp_path, gap_repair=repair)
    try:
        bound = runtime.plan.slots[0]
        first = runtime.begin_source_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, first, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=204)],
            ),
            epoch=first,
            received_ms=NOW + 3,
            drive=False,
        )
        assert runtime._slots["slot1"].attributable == {"BTC": Decimal("0.2")}
        runtime.connection_gap("source", epoch=first, reason="long disconnect")

        second = runtime.begin_source_connection(received_ms=NOW + 10_000)
        await _baseline(runtime, bound, second, time_ms=NOW + 10_000, BTC="0.1")

        assert runtime._slots["slot1"].attributable == {"BTC": Decimal("0.1")}
        assert runtime._slots["slot1"].unattributed == {}
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_stale_gap_flip_keeps_opposite_residual_unattributed(
    tmp_path: Path,
) -> None:
    source = _address(10)
    repair = _StaticGapRepair(
        (
            FillRecord(
                account=source,
                market="BTC",
                tx_hash="0xstaleflip",
                tid="205",
                time_ms=NOW + 6,
                side="sell",
                size=Decimal("0.21"),
                price=Decimal("100"),
                start_position=Decimal("0.11"),
                oid=205,
                received_ms=NOW + 6,
                connection_epoch=2,
                is_snapshot=False,
            ),
        ),
        before={"BTC": Decimal("0.11")},
        after={"BTC": Decimal("-0.1")},
    )
    runtime, lanes, _hook = _runtime(tmp_path, gap_repair=repair)
    try:
        bound = runtime.plan.slots[0]
        first = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, first, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 3, tid=206)],
            ),
            epoch=first,
            received_ms=NOW + 3,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 4, drive=False
        )
        first_action = await runtime.apply_market(
            _book("BTC", NOW + 5), epoch=market_epoch, received_ms=NOW + 5
        )
        assert first_action[0].attempt is not None
        assert lanes["slot1"].actions[0][0].side == "buy"
        assert lanes["slot1"].actions[0][0].size == Decimal("0.11")
        runtime.connection_gap("source", epoch=first, reason="long disconnect")

        second = runtime.begin_source_connection(received_ms=NOW + 10_000)
        await _baseline(runtime, bound, second, time_ms=NOW + 10_000, BTC="-0.1")
        slot = runtime._slots["slot1"]
        assert slot.attributable == {}
        assert slot.unattributed == {"BTC": Decimal("-0.1")}
        assert slot.follower is not None
        assert slot.follower.positions == {}
        close = lanes["slot1"].actions[1][0]
        assert close.side == "sell"
        assert close.size == Decimal("0.11")
        assert close.reduce_only is True

        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 10_003, drive=False
        )
        result = await runtime.apply_market(
            _book("BTC", NOW + 10_004),
            epoch=market_epoch,
            received_ms=NOW + 10_004,
        )
        assert result == ()
        assert len(lanes["slot1"].actions) == 2
        assert slot.follower.positions == {}
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_source_frame_queued_before_disconnect_cannot_apply_after_gap(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path)
    slot = runtime._slots["slot1"]
    epoch = runtime.begin_source_connection(received_ms=NOW - 10)
    await _baseline(runtime, runtime.plan.slots[0], epoch, time_ms=NOW)
    activity_before = slot.account.source.last_connection_activity_ms
    task: asyncio.Task[Dispatch] | None = None
    await slot.lock.acquire()
    try:
        task = asyncio.create_task(
            runtime.apply_source(
                _fills(
                    runtime.plan.slots[0].config.source_address,
                    snapshot=False,
                    fills=[],
                ),
                epoch=epoch,
                received_ms=NOW + 1,
                drive=False,
            )
        )
        await asyncio.sleep(0)
        runtime.connection_gap("source", epoch=epoch, reason="test disconnect")
    finally:
        if slot.lock.locked():
            slot.lock.release()

    try:
        assert task is not None
        dispatch = await task
        assert dispatch.source_frame_accepted is False
        assert dispatch.reason == "source frame rejected for closed connection epoch"
        assert slot.account.source.last_connection_activity_ms == activity_before
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        runtime.close()


def test_checkpoint_identity_change_is_rejected(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path)
    slot = runtime._slots["slot1"]
    runtime._store.save(slot)
    runtime.close()

    with pytest.raises(ValueError, match="state identity mismatch"):
        _runtime(tmp_path, runtime_id="different-runtime")


def test_checkpoint_identity_adds_deny_policy_fingerprint_once(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path, dynamic=True, denied=(("ETH",),))
    slot = runtime._slots["slot1"]
    runtime._store.save(slot)
    row = runtime._store.db.execute(
        "SELECT payload FROM continuous_state WHERE slot=?", ("slot1",)
    ).fetchone()
    assert row is not None
    payload = json.loads(str(row[0]))
    payload["identity"].pop("denied_markets_sha256")
    runtime._store.db.execute(
        "UPDATE continuous_state SET payload=? WHERE slot=?",
        (json.dumps(payload, sort_keys=True, separators=(",", ":")), "slot1"),
    )
    runtime.close()

    restored, _lanes, _hook = _runtime(tmp_path, dynamic=True, denied=(("ETH",),))
    try:
        row = restored._store.db.execute(
            "SELECT payload FROM continuous_state WHERE slot=?", ("slot1",)
        ).fetchone()
        assert row is not None
        upgraded = json.loads(str(row[0]))
        assert upgraded["identity"] == restored._slots["slot1"].identity
        assert upgraded["identity"]["denied_markets_sha256"]
    finally:
        restored.close()


def test_unrelated_catalog_listing_does_not_invalidate_checkpoint(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path, catalog=_catalog(revision_id="catalog-one"))
    runtime._store.save(runtime._slots["slot1"])
    runtime.close()

    restored, _lanes, _hook = _runtime(
        tmp_path,
        catalog=_catalog(("BTC", "ETH", "DOGE"), revision_id="catalog-two"),
    )
    try:
        assert restored._slots["slot1"].identity["catalog_markets_sha256"]
    finally:
        restored.close()


@pytest.mark.parametrize(
    "restored_symbols",
    [("BTC", "ETH", "DOGE"), ("BTC",)],
)
def test_dynamic_catalog_change_does_not_invalidate_checkpoint(
    tmp_path: Path,
    restored_symbols: tuple[str, ...],
) -> None:
    runtime, _lanes, _hook = _runtime(
        tmp_path,
        dynamic=True,
        catalog=_catalog(("BTC", "ETH"), revision_id="catalog-one"),
    )
    runtime._store.save(runtime._slots["slot1"])
    runtime.close()

    restored, _lanes, _hook = _runtime(
        tmp_path,
        dynamic=True,
        catalog=_catalog(restored_symbols, revision_id="catalog-two"),
    )
    try:
        assert restored._slots["slot1"].identity["market_policy"] == ("all_active_token0_perps")
        assert restored._slots["slot1"].identity["catalog_markets_sha256"] == "dynamic"
    finally:
        restored.close()


def test_dynamic_deny_policy_change_rejects_checkpoint_restore(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(
        tmp_path,
        dynamic=True,
        denied=(("ETH",),),
    )
    runtime._store.save(runtime._slots["slot1"])
    runtime.close()

    with pytest.raises(ValueError, match="state identity mismatch"):
        _runtime(
            tmp_path,
            dynamic=True,
            denied=(("BTC",),),
        )


@pytest.mark.asyncio
async def test_restart_restores_action_window_from_durable_send_boundary(tmp_path: Path) -> None:
    follower, wallet = _address(20), _address(30)
    journal = ActionJournal(tmp_path / "actions.sqlite3")
    nonce = journal.reserve_nonce(follower_account=follower, api_wallet=wallet, wall_ms=NOW - 2_000)
    cloid = "0x" + "a" * 32
    journal.prepare_action(
        follower_account=follower,
        api_wallet=wallet,
        desired_id="prior-process",
        market="BTC",
        attempt_no=1,
        cloid=cloid,
        nonce=nonce,
        requested_size="0.1",
        action_json='{"orders":[{"b":true}]}',
        signed_payload_json="{}",
        expires_after_ms=NOW + 5_000,
        request_id="pending",
        created_ms=NOW - 2_000,
    )
    journal.mark_send_attempted(cloid, observed_ms=NOW - 1_000)
    journal.record_outcome(cloid, state="REJECTED", observed_ms=NOW - 900)
    runtime, lanes, _hook = _runtime(
        tmp_path, action_limit=1, journal=journal, action_clock=lambda: NOW
    )
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=31)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        blocked = await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4
        )

        assert blocked[0].state is RuntimeState.PAUSE_ENTRIES
        assert blocked[0].reason == "slot action limit reached"
        assert lanes["slot1"].actions == []
    finally:
        runtime.close()
        journal.close()


def test_runtime_holds_account_and_signer_os_locks(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path, state_name="one.sqlite3")
    try:
        with pytest.raises(RuntimeFileLockBusy):
            _runtime(tmp_path, state_name="two.sqlite3")
    finally:
        runtime.close()


def test_default_locks_conflict_across_distinct_engine_state_roots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        continuous_runtime,
        "default_runtime_lock_dir",
        lambda: tmp_path / "global-locks",
    )
    runtime, _lanes, _hook = _runtime(
        tmp_path / "engine-one",
        runtime_id="runtime-one",
        use_default_lock_dir=True,
    )
    try:
        with pytest.raises(RuntimeFileLockBusy):
            _runtime(
                tmp_path / "engine-two",
                runtime_id="runtime-two",
                use_default_lock_dir=True,
            )
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_tracking_drift_blocks_entry_but_does_not_mutate(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, max_tracking_bps=Decimal("20"))
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=5)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
        )
        await runtime.apply_market(_context("BTC", "101"), epoch=market_epoch, received_ms=NOW + 4)
        result = await runtime.apply_market(
            _book("BTC", NOW + 5, bid="100.9", ask="101.1"),
            epoch=market_epoch,
            received_ms=NOW + 5,
        )
        assert "outside the source-fill price cap" in result[0].reason
        assert result[0].state is RuntimeState.PAUSE_ENTRIES
        assert lanes["slot1"].actions == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_bbo_change_executes_waiting_entry_at_leader_price_cap(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 1, tid=801)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 1,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC", "100"), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        waiting = await runtime.apply_market(
            _book("BTC", NOW + 3, bid="100.4", ask="100.6"),
            epoch=market_epoch,
            received_ms=NOW + 3,
        )
        assert waiting[0].attempt is None
        assert lanes["slot1"].actions == []

        executed = await runtime.apply_market(
            _bbo("BTC", NOW + 4, bid="100.2", ask="100.4"),
            epoch=market_epoch,
            received_ms=NOW + 4,
        )
        assert executed[0].attempt is not None
        assert lanes["slot1"].actions[0][1] == Decimal("100.50")
        assert executed[0].attempt.execution_context["best_ask"] == "100.4"
        assert executed[0].attempt.execution_context["price_policy"] == (
            "latest-bbo-leader-cap-v1"
        )
        sizing = executed[0].attempt.execution_context
        assert sizing["source_equity_usd"] == "100"
        assert sizing["source_equity_basis"] == "unified_spot_token0_usdc_total"
        assert sizing["follower_equity_usd"] == "100"
        assert sizing["follower_equity_basis"] == "unified_spot_token0_usdc_total"
        assert sizing["copy_multiplier"] == "1"
        assert sizing["sizing_scale"] == "1"
        assert sizing["sizing_gross_scale"] == "1"
        assert sizing["source_position_size"] == "0.11"
        assert sizing["raw_scaled_target_size"] == "0.11"
        assert sizing["desired_target_size"] == "0.11"
        assert sizing["confirmed_follower_size"] == "0"
        assert sizing["projected_follower_size_after"] == "0.110"
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_subminimum_market_does_not_starve_other_market_in_live_runtime(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(side="B", size="0.01", start="0", time_ms=NOW + 1, tid=811),
                    _fill(
                        side="B",
                        size="0.11",
                        start="0",
                        time_ms=NOW + 2,
                        tid=812,
                        coin="ETH",
                    ),
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4, drive=False
        )
        await runtime.apply_market(
            _context("ETH"), epoch=market_epoch, received_ms=NOW + 5, drive=False
        )
        result = await runtime.apply_market(
            _book("ETH", NOW + 6), epoch=market_epoch, received_ms=NOW + 6
        )

        assert len(lanes["slot1"].actions) == 1
        assert lanes["slot1"].actions[0][0].market == "ETH"
        assert result[0].attempt is not None
        skipped = result[0].attempt.execution_context["selection_skipped_blockers"]
        assert len(skipped) == 1
        assert "BTC residual is sub-minimum debt" in skipped[0]
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_live_equity_changes_recalculate_proportional_target(tmp_path: Path) -> None:
    runtime, lanes, hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW, spot_total="100")
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 1, tid=821)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 1,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        first = await runtime.apply_market(
            _book("BTC", NOW + 3), epoch=market_epoch, received_ms=NOW + 3
        )
        assert first[0].attempt is not None
        assert lanes["slot1"].actions[-1][0].size == Decimal("0.110")

        hook.truths["slot1"] = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.11"), leverage=1)},
            Decimal("200"),
            NOW + 10,
        )
        increased = await runtime.reconcile_follower("slot1", now_ms=NOW + 10)
        assert increased.attempt is not None
        assert lanes["slot1"].actions[-1][0].side == "buy"
        assert lanes["slot1"].actions[-1][0].size == Decimal("0.110")
        assert increased.attempt.execution_context["follower_equity_usd"] == "200"
        assert increased.attempt.execution_context["sizing_scale"] == "2"

        hook.truths["slot1"] = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.22"), leverage=1)},
            Decimal("200"),
            NOW + 20,
        )
        source_equity_change = await runtime.apply_source(
            _spot(source, NOW + 20, total="200"),
            epoch=source_epoch,
            received_ms=NOW + 20,
        )
        assert source_equity_change.attempt is not None
        assert lanes["slot1"].actions[-1][0].side == "sell"
        assert lanes["slot1"].actions[-1][0].size == Decimal("0.110")
        assert source_equity_change.attempt.execution_context["source_equity_usd"] == "200"
        assert source_equity_change.attempt.execution_context["sizing_scale"] == "1"
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_full_placeable_ioc_is_not_shrunk_to_subminimum_visible_depth(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 1, tid=802)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 1,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        result = await runtime.apply_market(
            _book("BTC", NOW + 3, size="0.005"),
            epoch=market_epoch,
            received_ms=NOW + 3,
        )

        assert result[0].attempt is not None
        assert lanes["slot1"].actions[0][0].size == Decimal("0.110")
        assert result[0].attempt.execution_context["visible_size"] == "0.005"
        assert result[0].attempt.execution_context["submitted_size"] == "0.110"
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_stale_live_increase_is_observed_but_never_becomes_a_copy_signal(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, max_source_fill_age_ms=5_000)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.11", start="0", time_ms=NOW + 1, tid=803)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 6_002,
            drive=False,
        )

        slot = runtime._slots["slot1"]
        assert slot.attributable == {}
        assert slot.unattributed == {"BTC": Decimal("0.11")}
        assert lanes["slot1"].actions == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_partial_ioc_folds_once_then_replans_residual_on_newer_bbo(
    tmp_path: Path,
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.PARTIALLY_FILLED
        lane.filled_size = Decimal("0.01")
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 1, tid=804)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 1,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 2, drive=False
        )
        first = await runtime.apply_market(
            _book("BTC", NOW + 3), epoch=market_epoch, received_ms=NOW + 3
        )
        assert first[0].attempt is not None
        assert runtime._slots["slot1"].follower.positions["BTC"].size == Decimal("0.01")

        unchanged = await runtime.drive_slot("slot1", now_ms=NOW + 4)
        assert unchanged.reason == (
            "waiting for post-result market data after terminal IOC partial fill"
        )
        assert len(lane.actions) == 1

        lane.outcome = PostOutcome.FILLED
        lane.filled_size = None
        second = await runtime.apply_market(
            _bbo("BTC", NOW + 5, size="11"), epoch=market_epoch, received_ms=NOW + 5
        )
        assert second[0].attempt is not None
        assert len(lane.actions) == 2
        assert lane.actions[1][0].size == Decimal("0.110")
        assert runtime._slots["slot1"].follower.positions["BTC"].size == Decimal("0.12")
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_no_liquidity_rejection_waits_until_fresh_source_fill(tmp_path: Path) -> None:
    action_now = [NOW]
    runtime, lanes, _hook = _runtime(tmp_path, action_clock=lambda: action_now[0])
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=51)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        await runtime.apply_market(_book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4)
        assert len(lane.actions) == 1

        action_now[0] = NOW + 1_000
        repeated = await runtime.reconcile_follower("slot1", now_ms=NOW + 1_000)
        assert repeated.reason == "waiting for post-result market data after terminal IOC no-fill"
        assert len(lane.actions) == 1

        action_now[0] = NOW + 2_000
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[
                    _fill(
                        side="B",
                        size="0.01",
                        start="0.12",
                        time_ms=NOW + 2_000,
                        tid=52,
                    )
                ],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2_000,
        )
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_reduce_only_no_liquidity_retries_on_newer_market_state(
    tmp_path: Path,
) -> None:
    action_now = [NOW]
    runtime, lanes, market_epoch = await _prime_provable_close(
        tmp_path,
        action_clock=lambda: action_now[0],
    )
    try:
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 7), epoch=market_epoch, received_ms=NOW + 7, drive=False
        )
        first = await runtime.drive_slot("slot1", now_ms=NOW + 7)
        assert first.action is not None and first.action.reduce_only is True
        assert lane.actions[0][1] == aggressive_ioc_price(
            Decimal("99.9"),
            is_buy=False,
            slippage_bps=Decimal("100"),
            sz_decimals=3,
        )
        assert len(lane.actions) == 1

        action_now[0] = NOW + 1_000
        repeated = await runtime.drive_slot("slot1", now_ms=NOW + 1_000)
        assert repeated.reason == "waiting for post-result market data after terminal IOC no-fill"
        assert len(lane.actions) == 1

        await runtime.apply_market(
            _book("BTC", NOW + 1_001),
            epoch=market_epoch,
            received_ms=NOW + 1_001,
        )
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_unchanged_periodic_l2_does_not_hot_retry_a_failed_ioc(
    tmp_path: Path,
) -> None:
    action_now = [NOW]
    runtime, lanes, market_epoch = await _prime_provable_close(
        tmp_path,
        action_clock=lambda: action_now[0],
    )
    try:
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 7), epoch=market_epoch, received_ms=NOW + 7
        )
        assert len(lane.actions) == 1

        action_now[0] = NOW + 100
        periodic = await runtime.apply_market(
            _book("BTC", NOW + 100),
            epoch=market_epoch,
            received_ms=NOW + 100,
        )
        assert len(lane.actions) == 1
        assert periodic[0].reason == (
            "waiting before unchanged-liquidity retry after terminal IOC no-fill"
        )
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_reduce_only_sell_executes_when_thin_book_has_no_ask_side(
    tmp_path: Path,
) -> None:
    runtime, lanes, market_epoch = await _prime_provable_close(tmp_path)
    try:
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        one_sided = _book("BTC", NOW + 7)
        one_sided["data"]["levels"][1] = []
        result = await runtime.apply_market(
            one_sided,
            epoch=market_epoch,
            received_ms=NOW + 7,
        )

        assert result[0].attempt is not None
        assert lanes["slot1"].actions[0][0].reduce_only is True
        assert lanes["slot1"].actions[0][0].side == "sell"
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_fail_close_no_liquidity_waits_for_newer_market_state(
    tmp_path: Path,
) -> None:
    action_now = [NOW]
    hook = _FollowerHook()
    hook.truths["slot1"] = FollowerTruth(
        {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW
    )
    runtime, lanes, _hook = _runtime(
        tmp_path,
        hook=hook,
        action_clock=lambda: action_now[0],
    )
    try:
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW), epoch=market_epoch, received_ms=NOW, drive=False
        )
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"
        runtime.request_fail_close(("slot1",), reason="test closeout")
        first = await runtime.drive_fail_close("slot1", now_ms=NOW)
        assert first.action is not None and first.action.reduce_only is True
        assert len(lane.actions) == 1

        action_now[0] = NOW + 1_000
        repeated = await runtime.drive_fail_close("slot1", now_ms=NOW + 1_000)
        assert repeated.reason == "waiting for post-result market data after terminal IOC no-fill"
        assert len(lane.actions) == 1

        await runtime.apply_market(
            _book("BTC", NOW + 1_001),
            epoch=market_epoch,
            received_ms=NOW + 1_001,
            drive=False,
        )
        await runtime.drive_fail_close("slot1", now_ms=NOW + 1_001)
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejection_reason",
    (
        "item_error: Price has too many decimals",
        "server_error_response",
        "top_level_error: invalid order batch",
    ),
)
async def test_deterministic_rejection_is_not_retried_on_timestamp_only_refresh(
    tmp_path: Path,
    rejection_reason: str,
) -> None:
    runtime, lanes, hook = _runtime(tmp_path)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = rejection_reason
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=55)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        await runtime.apply_market(_book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4)
        assert len(lane.actions) == 1

        repeated = await runtime.reconcile_follower("slot1", now_ms=NOW + 1_000)
        assert "unchanged target remains blocked" in repeated.reason
        assert len(lane.actions) == 1

        hook.truths["slot1"] = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.01"), leverage=10)},
            Decimal("100"),
            NOW + 2_000,
        )
        lane.outcome = PostOutcome.FILLED
        await runtime.reconcile_follower("slot1", now_ms=NOW + 2_000)
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_explicit_rate_limit_rejection_retries_once_after_backoff(
    tmp_path: Path,
) -> None:
    action_now = [NOW]
    runtime, lanes, _hook = _runtime(tmp_path, action_clock=lambda: action_now[0])
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "top_level_error: rate limit budget exhausted"
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=815)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3
        )
        await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4
        )
        assert len(lane.actions) == 1

        action_now[0] = NOW + 4_999
        waiting = await runtime.reconcile_follower("slot1", now_ms=NOW + 4_999)
        assert waiting.reason == "waiting after transient exchange rejection"
        assert len(lane.actions) == 1

        action_now[0] = NOW + 5_000
        lane.outcome = PostOutcome.FILLED
        retried = await runtime.reconcile_follower("slot1", now_ms=NOW + 5_000)
        assert retried.attempt is not None
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "venue_reason",
    (
        "item_error: Order price too far from oracle",
        "item_error: Order would increase open interest while open interest is capped",
    ),
)
async def test_venue_market_state_rejection_retries_on_new_context(
    tmp_path: Path,
    venue_reason: str,
) -> None:
    action_now = [NOW]
    runtime, lanes, _hook = _runtime(tmp_path, action_clock=lambda: action_now[0])
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = venue_reason
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=816)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3
        )
        await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4
        )
        assert len(lane.actions) == 1

        action_now[0] = NOW + 50
        newer_book_only = await runtime.apply_market(
            _bbo("BTC", NOW + 50, bid="99.8", ask="100.2", size="11"),
            epoch=market_epoch,
            received_ms=NOW + 50,
        )
        assert newer_book_only[0].attempt is None
        assert newer_book_only[0].reason == (
            "waiting for post-result market data after venue market-state rejection"
        )
        assert len(lane.actions) == 1

        action_now[0] = NOW + 100
        lane.outcome = PostOutcome.FILLED
        retried = await runtime.apply_market(
            _context("BTC", "99.9"),
            epoch=market_epoch,
            received_ms=NOW + 100,
        )
        assert retried[0].attempt is not None
        assert len(lane.actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_no_liquidity_rejection_retries_persistent_target_on_newer_bbo(
    tmp_path: Path,
) -> None:
    action_now = [NOW]
    runtime, lanes, _hook = _runtime(tmp_path, action_clock=lambda: action_now[0])
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        lane = lanes["slot1"]
        lane.outcome = PostOutcome.REJECTED
        lane.reason = "item_error: Order could not immediately match against any resting orders"
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=61)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )
        await runtime.apply_market(_book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4)
        assert len(lane.actions) == 1

        action_now[0] = NOW + 6_000
        retried = await runtime.apply_market(
            _book("BTC", NOW + 6_000),
            epoch=market_epoch,
            received_ms=NOW + 6_000,
        )
        assert len(lane.actions) == 2
        assert retried[0].attempt is not None
        assert retried[0].attempt.execution_context["leader_trigger_age_ms"] == 5_998
        assert retried[0].attempt.execution_context["leader_trigger_admission_age_ms"] == 0
        assert retried[0].attempt.execution_context["accepted_target_wait_ms"] == 5_998
    finally:
        runtime.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("combined", "action_limit", "expected"),
    [("15", 6, "combined gross"), ("30", 1, "action limit")],
)
async def test_last_mile_combined_cap_and_slot_action_rate_are_enforced(
    tmp_path: Path, combined: str, action_limit: int, expected: str
) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, combined=combined, action_limit=action_limit)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 3, tid=6)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 4)
        await runtime.apply_market(_book("BTC", NOW + 5), epoch=market_epoch, received_ms=NOW + 5)
        assert len(lanes["slot1"].actions) == 1

        blocked = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0.12", time_ms=NOW + 6, tid=7)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 6,
        )
        assert expected in blocked.reason
        assert blocked.state is RuntimeState.PAUSE_ENTRIES
        assert len(lanes["slot1"].actions) == 1
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_slot_entry_rate_cap_never_blocks_reduce_only_close(tmp_path: Path) -> None:
    runtime, lanes, _hook = _runtime(tmp_path, action_limit=1)
    try:
        bound = runtime.plan.slots[0]
        source = bound.config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 3, tid=16)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
        )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 4)
        await runtime.apply_market(_book("BTC", NOW + 5), epoch=market_epoch, received_ms=NOW + 5)
        assert len(lanes["slot1"].actions) == 1

        closed = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="A", size="0.12", start="0.12", time_ms=NOW + 6, tid=17)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 6,
        )

        assert closed.action is not None
        assert closed.action.reduce_only is True
        assert len(lanes["slot1"].actions) == 2
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_reduction_is_dispatched_before_alphabetically_later_entry(tmp_path: Path) -> None:
    runtime, lanes, hook = _runtime(tmp_path)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        await runtime.reconcile_follower("slot1", now_ms=NOW)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.2", start="0", time_ms=NOW + 3, tid=8, coin="ETH")],
            ),
            epoch=source_epoch,
            received_ms=NOW + 3,
        )
        hook.truths["slot1"] = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.11"))}, Decimal("100"), NOW + 4
        )
        await runtime.reconcile_follower("slot1", now_ms=NOW + 4)
        for offset, message in enumerate(
            (_context("ETH"), _book("ETH", NOW + 6), _context("BTC"), _book("BTC", NOW + 8)),
            start=5,
        ):
            await runtime.apply_market(message, epoch=market_epoch, received_ms=NOW + offset)

        first_action = lanes["slot1"].actions[0][0]
        assert first_action.market == "BTC"
        assert first_action.side == "sell"
        assert first_action.reduce_only is True
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_stale_unrelated_entry_market_cannot_block_provable_close(tmp_path: Path) -> None:
    runtime, lanes, market_epoch = await _prime_provable_close(tmp_path)
    try:
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 7), epoch=market_epoch, received_ms=NOW + 7, drive=False
        )
        result = await runtime.drive_slot("slot1", now_ms=NOW + 7)

        assert result.action is not None
        assert result.action.market == "BTC"
        assert result.action.reduce_only is True
        assert result.action.size == Decimal("0.11")
        assert lanes["slot1"].actions[0][0] == result.action
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_stale_close_market_still_blocks_provable_close(tmp_path: Path) -> None:
    runtime, lanes, market_epoch = await _prime_provable_close(tmp_path)
    try:
        await runtime.apply_market(
            _context("ETH"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        await runtime.apply_market(
            _book("ETH", NOW + 7), epoch=market_epoch, received_ms=NOW + 7, drive=False
        )
        result = await runtime.drive_slot("slot1", now_ms=NOW + 7)

        assert result.state is RuntimeState.RECOVERING
        assert result.reason == "market BTC became stale before send"
        assert lanes["slot1"].actions == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_unresolved_same_market_prevents_duplicate_provable_close(tmp_path: Path) -> None:
    runtime, lanes, market_epoch = await _prime_provable_close(tmp_path)
    try:
        lanes["slot1"].unresolved = {"BTC": Decimal("-0.05")}
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 6, drive=False
        )
        await runtime.apply_market(
            _book("BTC", NOW + 7), epoch=market_epoch, received_ms=NOW + 7, drive=False
        )
        result = await runtime.drive_slot("slot1", now_ms=NOW + 7)

        assert result.state is RuntimeState.RECOVERING
        assert result.reason == "signer lane has unresolved action outcome"
        assert lanes["slot1"].actions == []
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_two_signer_lanes_send_without_waiting_for_each_others_ack(tmp_path: Path) -> None:
    barrier = _LaneBarrier(expected=2)
    runtime, lanes, _hook = _runtime(tmp_path, count=2, combined="30", barrier=barrier)
    try:
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        for index, bound in enumerate(runtime.plan.slots):
            await runtime.reconcile_follower(bound.config.slot, now_ms=NOW)
            await _baseline(runtime, bound, source_epoch, time_ms=NOW)
            await runtime.apply_source(
                _fills(
                    bound.config.source_address,
                    snapshot=False,
                    fills=[
                        _fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=20 + index)
                    ],
                ),
                epoch=source_epoch,
                received_ms=NOW + 2,
            )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        pending = asyncio.create_task(
            runtime.apply_market(
                _book("BTC", NOW + 4),
                epoch=market_epoch,
                received_ms=NOW + 4,
            )
        )
        await asyncio.wait_for(barrier.ready.wait(), timeout=1)

        assert barrier.started == 2
        assert sum(len(lane.actions) for lane in lanes.values()) == 2
        barrier.release.set()
        updates = await pending
        assert all(update.attempt is not None for update in updates)
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_reduce_only_api_never_waits_for_order_ack(tmp_path: Path) -> None:
    barrier = _LaneBarrier(expected=1)
    runtime, lanes, _hook = _runtime(tmp_path, barrier=barrier)
    try:
        bound, source = runtime.plan.slots[0], runtime.plan.slots[0].config.source_address
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        refreshed = await runtime.reconcile_follower("slot1", now_ms=NOW, drive=False)
        await _baseline(runtime, bound, source_epoch, time_ms=NOW)
        source_update = await runtime.apply_source(
            _fills(
                source,
                snapshot=False,
                fills=[_fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=35)],
            ),
            epoch=source_epoch,
            received_ms=NOW + 2,
            drive=False,
        )
        await runtime.apply_market(
            _context("BTC"), epoch=market_epoch, received_ms=NOW + 3, drive=False
        )
        market_updates = await runtime.apply_market(
            _book("BTC", NOW + 4), epoch=market_epoch, received_ms=NOW + 4, drive=False
        )

        assert refreshed.attempt is None
        assert source_update.source_frame_accepted is True
        assert source_update.attempt is None
        assert market_updates[0].attempt is None
        assert lanes["slot1"].actions == []

        pending = asyncio.create_task(runtime.drive_slot("slot1", now_ms=NOW + 4))
        await asyncio.wait_for(barrier.ready.wait(), timeout=1)
        assert len(lanes["slot1"].actions) == 1
        barrier.release.set()
        driven = await pending
        assert driven.attempt is not None
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_pending_reservation_is_included_in_combined_cap(tmp_path: Path) -> None:
    barrier = _LaneBarrier(expected=1)
    runtime, lanes, _hook = _runtime(tmp_path, count=2, combined="20", barrier=barrier)
    try:
        source_epoch = runtime.begin_source_connection(received_ms=NOW - 10)
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 10)
        for index, bound in enumerate(runtime.plan.slots):
            await runtime.reconcile_follower(bound.config.slot, now_ms=NOW)
            await _baseline(runtime, bound, source_epoch, time_ms=NOW)
            await runtime.apply_source(
                _fills(
                    bound.config.source_address,
                    snapshot=False,
                    fills=[
                        _fill(side="B", size="0.12", start="0", time_ms=NOW + 2, tid=40 + index)
                    ],
                ),
                epoch=source_epoch,
                received_ms=NOW + 2,
            )
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW + 3)
        pending = asyncio.create_task(
            runtime.apply_market(
                _book("BTC", NOW + 4),
                epoch=market_epoch,
                received_ms=NOW + 4,
            )
        )
        await asyncio.wait_for(barrier.ready.wait(), timeout=1)
        for _ in range(20):
            if any("combined gross" in runtime.status(slot_id)[1] for slot_id in runtime.slot_ids):
                break
            await asyncio.sleep(0)

        assert barrier.started == 1
        assert sum(len(lane.actions) for lane in lanes.values()) == 1
        assert any("combined gross" in runtime.status(slot_id)[1] for slot_id in runtime.slot_ids)
        barrier.release.set()
        updates = await pending
        assert any("combined gross" in update.reason for update in updates)
    finally:
        barrier.release.set()
        runtime.close()


@pytest.mark.asyncio
async def test_pending_reduction_does_not_free_capacity_before_confirmation(tmp_path: Path) -> None:
    runtime, _lanes, _hook = _runtime(tmp_path, count=2, combined="25")
    try:
        runtime._slots["slot1"].follower = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.2"))}, Decimal("100"), NOW
        )
        runtime._slots["slot2"].follower = FollowerTruth({}, Decimal("100"), NOW)
        runtime._reservations["slot1"] = {"BTC": Decimal("-0.1")}
        market_epoch = runtime.begin_market_connection(received_ms=NOW - 1)
        await runtime._activate()
        await runtime.apply_market(_context("BTC"), epoch=market_epoch, received_ms=NOW)
        await runtime.apply_market(_book("BTC", NOW), epoch=market_epoch, received_ms=NOW)
        action = NextAction("desired", "BTC", "buy", Decimal("0.1"), False, "test")

        async with runtime._exposure_lock:
            blocker = runtime._exposure(runtime._slots["slot2"], action, Decimal("100"), NOW)

        assert blocker == "projected combined gross exceeds the plan cap"

        runtime._reservations.clear()
        runtime._slots["slot1"].follower = FollowerTruth(
            {"BTC": Position("BTC", Decimal("0.1"))},
            Decimal("100"),
            NOW - runtime.follower_age_ms - 1,
        )
        async with runtime._exposure_lock:
            stale = runtime._exposure(runtime._slots["slot2"], action, Decimal("100"), NOW)
        assert stale == "combined exposure is unknown or stale for another slot"
    finally:
        runtime.close()
