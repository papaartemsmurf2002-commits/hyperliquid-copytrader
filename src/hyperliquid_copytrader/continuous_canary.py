from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any

from .action_journal import ActionJournal, ActionState
from .continuous_config import BoundContinuousPlan, BoundContinuousSlot
from .continuous_executor import ContinuousSignerLane, ExecutionAttempt
from .desired_engine import NextAction
from .market_catalog import CatalogRevision, MarketReadiness
from .market_stream import ExecutableIoc, MarketSnapshot, MarketStream, executable_ioc
from .order_preflight import HYPERLIQUID_PERP_MIN_NOTIONAL_USD
from .precision import quantize_size
from .runtime_lock import (
    AccountRuntimeFileLock,
    account_runtime_lock_path,
    default_runtime_lock_dir,
    signer_runtime_lock_path,
)
from .ws_actions import IocAction, WsPostMux


MAINNET_PROOF_ACKNOWLEDGEMENT = "I AUTHORIZE CONTINUOUS-V1 TWO-ACCOUNT MAINNET WS PROOF"
PROOF_MARKETS = {"acc7": "xyz:EWY", "acc1": "BTC"}
ORDER_CAP = Decimal("12")
COMBINED_CAP = Decimal("30")
ENTRY_TARGET = Decimal("11")
MAX_BOOK_AGE_MS = 1_000
MAX_REDUCTION_BOOK_AGE_MS = 5_000
REDUCTION_BOOK_WAIT_S = 5.0
MAX_PREFLIGHT_AGE_MS = 60_000
MAX_CATALOG_AGE_MS = 600_000
MAX_SLIPPAGE_BPS = Decimal("25")


class CanaryProofError(RuntimeError):
    pass


class RecoveryRequired(CanaryProofError):
    pass


TerminalTruth = dict[str, Any]


@dataclass(frozen=True, slots=True)
class CanaryProofResult:
    proof_dir: Path
    status: str
    passed: bool
    mutated: bool
    recovery_required: bool
    blockers: tuple[str, ...]
    action_count: int


LaneFactory = Callable[[BoundContinuousSlot, ActionJournal, str | None], Any]
TruthReader = Callable[[BoundContinuousSlot, int], Awaitable[TerminalTruth]]


async def run_two_account_ws_proof(
    bound: BoundContinuousPlan,
    *,
    acknowledgement: str,
    preflight_report: Mapping[str, Any],
    catalog: CatalogRevision,
    market_stream: MarketStream,
    mux: WsPostMux,
    required_epoch: int,
    truth_reader: TruthReader,
    local_appdata: Path | None = None,
    runtime_lock_dir: Path | None = None,
    lane_factory: LaneFactory | None = None,
    now_ms: Callable[[], int] | None = None,
) -> CanaryProofResult:
    """Run exactly one bounded entry/close proof per configured follower.

    The caller supplies connected market and WS-POST transports plus a WS truth
    reader.  This module has no REST client and exposes no mutation other than
    ``ContinuousSignerLane.execute_ioc``.
    """

    clock = now_ms or (lambda: time_ns() // 1_000_000)
    started = clock()
    proof_dir = _proof_dir(local_appdata, started)
    proof_dir.mkdir(parents=True, exist_ok=False)
    journal_path = proof_dir.parent / "mainnet-proof-actions.sqlite3"
    _write(proof_dir / "preflight.json", dict(preflight_report))
    _write(proof_dir / "catalog.json", catalog.to_payload())
    _write(
        proof_dir / "manifest.json",
        {
            "version": 1,
            "proof": "continuous-v1-two-account-ws",
            "started_ms": started,
            "plan_sha256": bound.plan.sha256,
            "catalog_revision": catalog.revision_id,
            "mutation_transport": "websocket_post_only",
            "stable_action_journal": str(journal_path),
            "rest_action_count": 0,
            "caps": {
                "order_usd": "12",
                "combined_usd": "30",
                "max_effective_gross_to_collateral": 1,
            },
            "markets": PROOF_MARKETS,
        },
    )
    rows: list[dict[str, Any]] = []
    truths: list[dict[str, Any]] = []
    blockers = _start_blockers(
        bound,
        acknowledgement,
        preflight_report,
        catalog,
        market_stream,
        mux,
        required_epoch,
        started,
    )
    attempted: set[str] = set()
    completed: set[str] = set()
    active: BoundContinuousSlot | None = None
    recovery = False

    with ActionJournal(journal_path) as journal:
        if blockers:
            return _finish(proof_dir, "aborted_before_mutation", blockers, rows, truths, completed)
        try:
            locks = _locks(bound, _lock_dir(local_appdata, runtime_lock_dir))
        except Exception as exc:
            blockers.append(f"exclusive runtime locks unavailable: {type(exc).__name__}: {exc}")
            return _finish(proof_dir, "aborted_before_mutation", blockers, rows, truths, completed)

        factory = lane_factory or _lane_factory()
        report_slots = {item["slot"]: item for item in preflight_report["slots"]}
        with locks:
            try:
                lanes = {
                    slot.config.slot: factory(
                        slot,
                        journal,
                        slot.config.follower_account_address
                        if report_slots[slot.config.slot]["identity"]["follower_role"]
                        == "subaccount"
                        else None,
                    )
                    for slot in bound.slots
                }
                owners = {
                    (slot.config.follower_account_address, slot.api_wallet_address)
                    for slot in bound.slots
                }
                foreign = [
                    record
                    for record in journal.recovery_actions()
                    if (record.follower_account, record.api_wallet) not in owners
                ]
                if foreign:
                    raise RecoveryRequired(
                        "stable proof journal contains unresolved actions for another identity"
                    )
                for slot in bound.slots:
                    lane = lanes[slot.config.slot]
                    for record in journal.recovery_actions(
                        follower_account=slot.config.follower_account_address,
                        api_wallet=slot.api_wallet_address,
                    ):
                        if record.state is ActionState.PREPARED:
                            journal.mark_not_sent(record.cloid)
                    for record in journal.recovery_actions(
                        follower_account=slot.config.follower_account_address,
                        api_wallet=slot.api_wallet_address,
                    ):
                        resolved = await lane.resolve_by_cloid(
                            record.cloid,
                            mux=mux,
                            required_epoch=mux.capture_epoch(),
                        )
                        if not resolved.terminal:
                            raise RecoveryRequired(
                                f"prior CLOID remains unresolved: {record.cloid}"
                            )
                for slot in bound.slots:
                    active = slot
                    baseline = await truth_reader(slot, mux.capture_epoch())
                    _check_truth(baseline, slot, "baseline")
                    truths.append(_truth_payload(baseline, "baseline"))
                    await _trade_slot(
                        slot,
                        lanes[slot.config.slot],
                        market_stream,
                        mux,
                        rows,
                        proof_dir / "ws-actions.jsonl",
                        attempted,
                        clock,
                        str(started),
                    )
                    terminal = await truth_reader(slot, mux.capture_epoch())
                    _check_truth(terminal, slot, "terminal")
                    truths.append(_truth_payload(terminal, "terminal"))
                    completed.add(slot.config.slot)
                    active = None
            except (Exception, asyncio.CancelledError) as exc:
                blockers.append(f"{type(exc).__name__}: {exc}")
                recovery = isinstance(exc, (RecoveryRequired, asyncio.CancelledError))
                unresolved = journal.recovery_actions()
                if unresolved:
                    recovery = True
                    blockers.append(
                        "unresolved CLOIDs: " + ",".join(row.cloid for row in unresolved)
                    )
                if active is not None and active.config.slot in attempted:
                    try:
                        truth = await truth_reader(active, mux.capture_epoch())
                        _check_truth(truth, active, "failure_terminal", require_flat=False)
                        truths.append(_truth_payload(truth, "failure_terminal"))
                        recovery = recovery or not _flat(truth) or truth["open_order_count"] != 0
                    except Exception as truth_exc:
                        recovery = True
                        blockers.append(
                            f"slot {active.config.slot} terminal truth unavailable: "
                            f"{type(truth_exc).__name__}: {truth_exc}"
                        )

    status = (
        "passed"
        if not blockers and completed == set(PROOF_MARKETS)
        else ("recovery_required" if recovery else "failed_safe")
    )
    return _finish(
        proof_dir,
        status,
        blockers,
        rows,
        truths,
        completed,
        recovery=recovery,
        mutated=bool(attempted),
    )


async def _trade_slot(
    slot: BoundContinuousSlot,
    lane: Any,
    markets: MarketStream,
    mux: WsPostMux,
    rows: list[dict[str, Any]],
    action_log: Path,
    attempted: set[str],
    now_ms: Callable[[], int],
    proof_id: str,
) -> None:
    market = PROOF_MARKETS[slot.config.slot]
    snapshot = _snapshot(markets, market, now_ms())
    size = quantize_size(ENTRY_TARGET / snapshot.mark_px, snapshot.sz_decimals)
    if size * snapshot.mark_px < ENTRY_TARGET:
        size += snapshot.size_quantum
    entry = _chunk(
        snapshot,
        True,
        size,
        slot.config.max_order_notional_usd,
        reduce_only=False,
        current_position_size=Decimal("0"),
    )
    attempt = await _send(
        slot,
        lane,
        NextAction(
            _desired(slot, proof_id, "entry", entry.size),
            market,
            "buy",
            entry.size,
            False,
            "proof",
        ),
        entry,
        snapshot,
        mux,
        rows,
        action_log,
        attempted,
        "entry",
    )
    remaining = attempt.record.cumulative_filled_size
    if remaining <= 0:
        raise CanaryProofError(f"slot {slot.config.slot} entry did not fill")

    for close_no in range(1, 4):
        snapshot = await _reduction_snapshot(
            markets,
            market,
            now_ms,
        )
        close = _chunk(
            snapshot,
            False,
            remaining,
            slot.config.max_order_notional_usd,
            reduce_only=True,
            current_position_size=remaining,
        )
        attempt = await _send(
            slot,
            lane,
            NextAction(
                _desired(slot, proof_id, f"close-{close_no}", remaining),
                market,
                "sell",
                close.size,
                True,
                "reduce-only proof close",
            ),
            close,
            snapshot,
            mux,
            rows,
            action_log,
            attempted,
            "close",
        )
        filled = attempt.record.cumulative_filled_size
        if filled > remaining:
            raise RecoveryRequired("reduce-only close overfilled the proof position")
        remaining -= filled
        if remaining == 0:
            return
        if filled == 0:
            raise RecoveryRequired("reduce-only close did not fill")
    raise RecoveryRequired("proof position remains after three reduce-only IOC closes")


async def _send(
    slot: BoundContinuousSlot,
    lane: Any,
    action: NextAction,
    executable: ExecutableIoc,
    snapshot: MarketSnapshot,
    mux: WsPostMux,
    rows: list[dict[str, Any]],
    log: Path,
    attempted: set[str],
    phase: str,
) -> ExecutionAttempt:
    attempted.add(slot.config.slot)
    attempt = await lane.execute_ioc(
        action=action,
        asset_id=snapshot.asset_id,
        limit_px=executable.limit_px,
        mux=mux,
        required_epoch=mux.capture_epoch(),
        received_mono_ns=monotonic_ns(),
    )
    try:
        if attempt.result.reason == "server_rejected":
            raise RecoveryRequired("undocumented channel:error is not a definitive rejection")
        if not attempt.record.terminal:
            record = await lane.resolve_by_cloid(
                attempt.record.cloid,
                mux=mux,
                required_epoch=mux.capture_epoch(),
            )
            attempt = ExecutionAttempt(
                record, attempt.result, attempt.received_to_send_ms, attempt.send_to_response_ms
            )
        if not attempt.record.terminal:
            raise RecoveryRequired("CLOID remained unresolved after orderStatus")
    except Exception as exc:
        _log_action(log, rows, slot, phase, action, executable, snapshot, attempt)
        if isinstance(exc, RecoveryRequired):
            raise
        raise RecoveryRequired("orderStatus could not prove the CLOID terminal") from exc
    _log_action(log, rows, slot, phase, action, executable, snapshot, attempt)
    return attempt


def _chunk(
    snapshot: MarketSnapshot,
    is_buy: bool,
    requested: Decimal,
    plan_cap: Decimal,
    *,
    reduce_only: bool,
    current_position_size: Decimal,
) -> ExecutableIoc:
    cap = min(ORDER_CAP, plan_cap)
    first = executable_ioc(
        snapshot,
        is_buy=is_buy,
        requested_size=requested,
        max_slippage_bps=MAX_SLIPPAGE_BPS,
    )
    if first is None:
        raise CanaryProofError(f"{snapshot.market} has no depth inside the slippage cap")
    cap_size = quantize_size(cap / first.limit_px, snapshot.sz_decimals)
    final = executable_ioc(
        snapshot,
        is_buy=is_buy,
        requested_size=min(first.size, cap_size),
        max_slippage_bps=MAX_SLIPPAGE_BPS,
    )
    if final is None:
        raise CanaryProofError(f"{snapshot.market} has no capped IOC size")
    notional = final.size * final.limit_px
    signed_size = final.size if is_buy else -final.size
    exact_close = bool(
        reduce_only and current_position_size != 0 and current_position_size + signed_size == 0
    )
    if notional < HYPERLIQUID_PERP_MIN_NOTIONAL_USD and not exact_close:
        action_class = "partial reduce-only" if reduce_only else "risk-increasing"
        raise CanaryProofError(
            f"{snapshot.market} {action_class} IOC is below the venue minimum ({notional})"
        )
    if notional > cap:
        raise CanaryProofError(f"{snapshot.market} terminal IOC exceeds the {cap} USD cap")
    return final


def _snapshot(
    markets: MarketStream,
    market: str,
    now_ms: int,
    *,
    max_age_ms: int = MAX_BOOK_AGE_MS,
) -> MarketSnapshot:
    snapshot = markets.fresh_snapshot(market, now_ms=now_ms, max_age_ms=max_age_ms)
    if snapshot is None:
        raise CanaryProofError(f"{market} has no fresh same-epoch book")
    return snapshot


async def _reduction_snapshot(
    markets: MarketStream,
    market: str,
    now_ms: Callable[[], int],
) -> MarketSnapshot:
    deadline = asyncio.get_running_loop().time() + REDUCTION_BOOK_WAIT_S
    while True:
        snapshot = markets.fresh_snapshot(
            market,
            now_ms=now_ms(),
            max_age_ms=MAX_REDUCTION_BOOK_AGE_MS,
        )
        if snapshot is not None:
            return snapshot
        if asyncio.get_running_loop().time() >= deadline:
            raise CanaryProofError(f"{market} has no fresh reduce-only recovery book")
        await asyncio.sleep(0.05)


def _log_action(
    path: Path,
    rows: list[dict[str, Any]],
    slot: BoundContinuousSlot,
    phase: str,
    action: NextAction,
    executable: ExecutableIoc,
    snapshot: MarketSnapshot,
    attempt: ExecutionAttempt,
) -> None:
    record = attempt.record
    wire = json.loads(record.action_json)
    signed = json.loads(record.signed_payload_json)
    ioc = IocAction(record.cloid, record.requested_size, wire)
    order = ioc.action["orders"][0]
    if signed.get("action") != wire or (
        int(order["a"]) != snapshot.asset_id
        or bool(order["b"]) != (action.side == "buy")
        or bool(order["r"]) != action.reduce_only
        or Decimal(str(order["s"])) != action.size
        or Decimal(str(order["p"])) != executable.limit_px
    ):
        raise RecoveryRequired("durable signed IOC differs from the bounded decision")
    row = {
        "slot": slot.config.slot,
        "market": action.market,
        "phase": phase,
        "cloid": record.cloid,
        "state": record.state.value,
        "reduce_only": action.reduce_only,
        "requested_size": str(action.size),
        "filled_size": str(record.cumulative_filled_size),
        "limit_px": str(executable.limit_px),
        "wire_limit_notional_usd": str(action.size * executable.limit_px),
        "send_attempted": record.send_attempted_ms is not None,
        "received_to_send_ms": _decimal(attempt.received_to_send_ms),
        "send_to_response_ms": _decimal(attempt.send_to_response_ms),
    }
    rows.append(row)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _start_blockers(
    bound: BoundContinuousPlan,
    acknowledgement: str,
    preflight: Mapping[str, Any],
    catalog: CatalogRevision,
    markets: MarketStream,
    mux: WsPostMux,
    epoch: int,
    now_ms: int,
) -> list[str]:
    blockers: list[str] = []
    if acknowledgement != MAINNET_PROOF_ACKNOWLEDGEMENT:
        blockers.append("exact mainnet proof acknowledgement is missing")
    plan = bound.plan
    if (
        plan.network != "mainnet"
        or plan.runtime_id != "continuous-v1-proof"
        or not plan.startup_baseline_only
        or plan.max_combined_gross_usd > COMBINED_CAP
        or len(bound.slots) != 2
        or {slot.config.slot for slot in bound.slots} != set(PROOF_MARKETS)
    ):
        blockers.append("bound plan is not the exact two-account mainnet proof plan")
    for slot in bound.slots:
        if (
            slot.config.allowed_markets != (PROOF_MARKETS.get(slot.config.slot),)
            or slot.config.max_order_notional_usd > ORDER_CAP
            or slot.config.max_gross_exposure_usd > 15
            or slot.config.max_leverage != 1
            or slot.config.max_open_positions != 1
        ):
            blockers.append(f"slot {slot.config.slot} exceeds its fixed proof limits")
    if (
        preflight.get("passed") is not True
        or preflight.get("blockers") not in ([], ())
        or preflight.get("network") != "mainnet"
        or preflight.get("plan_sha256") != plan.sha256
        or preflight.get("require_flat_and_order_free") is not True
    ):
        blockers.append("strict bounded preflight did not pass for this plan")
    observed = preflight.get("observed_ms")
    if not isinstance(observed, int) or not 0 <= now_ms - observed <= MAX_PREFLIGHT_AGE_MS:
        blockers.append("preflight is stale or future-dated")
    reports = {
        str(item.get("slot")): item
        for item in preflight.get("slots", [])
        if isinstance(item, Mapping)
    }
    if set(reports) != set(PROOF_MARKETS):
        blockers.append("preflight does not contain exactly acc7 and acc1")
    for slot in bound.slots:
        item = reports.get(slot.config.slot, {})
        identity = item.get("identity", {})
        if set(item.get("follower_dexes", [])) != set(catalog.wire_dexes):
            blockers.append(f"slot {slot.config.slot} preflight did not audit every catalog DEX")
        if (
            item.get("passed") is not True
            or item.get("follower_nonflat") is not False
            or item.get("follower_open_order_count") != 0
            or identity.get("follower_role") != "subaccount"
            or identity.get("signer_authorized") is not True
            or identity.get("signing_vault_address")
            != _redact(slot.config.follower_account_address)
            or identity.get("action_principal") != _redact(slot.global_account_address)
        ):
            blockers.append(f"slot {slot.config.slot} preflight identity/state is not exact")
    if (
        catalog.network != "mainnet"
        or markets.catalog.revision_id != catalog.revision_id
        or not 0 <= now_ms - catalog.observed_ms <= MAX_CATALOG_AGE_MS
    ):
        blockers.append("market stream is not pinned to a current mainnet catalog")
    for market in PROOF_MARKETS.values():
        spec = catalog.market(market)
        if spec is None or spec.readiness is not MarketReadiness.READY or spec.is_delisted:
            blockers.append(f"catalog market {market} is not ready")
        try:
            _snapshot(markets, market, now_ms)
        except CanaryProofError as exc:
            blockers.append(str(exc))
    if mux.connection_epoch != epoch:
        blockers.append("WS POST connection epoch changed before proof start")
    return blockers


def _check_truth(
    truth: TerminalTruth,
    slot: BoundContinuousSlot,
    phase: str,
    *,
    require_flat: bool = True,
) -> None:
    positions = truth.get("positions")
    collateral = truth.get("available_collateral_usd")
    if (
        truth.get("slot") != slot.config.slot
        or not isinstance(truth.get("open_order_count"), int)
        or truth["open_order_count"] < 0
        or truth.get("transport") != "ws_post"
        or truth.get("account_mode") != "unified"
        or not isinstance(collateral, Decimal)
        or not collateral.is_finite()
        or collateral < slot.config.max_gross_exposure_usd
        or not isinstance(positions, tuple)
        or len({market for market, _size in positions}) != len(positions)
        or any(not isinstance(size, Decimal) or not size.is_finite() for _market, size in positions)
    ):
        raise CanaryProofError("WS terminal truth, account mode, or collateral is invalid")
    if require_flat and (not _flat(truth) or truth["open_order_count"]):
        raise RecoveryRequired(f"slot {slot.config.slot} {phase} is not flat/order-free")


def _flat(truth: TerminalTruth) -> bool:
    return all(size == 0 for _market, size in truth["positions"])


def _truth_payload(truth: TerminalTruth, phase: str) -> dict[str, Any]:
    result = dict(truth)
    result.update(phase=phase, flat=_flat(truth), order_free=truth["open_order_count"] == 0)
    result["positions"] = {market: str(size) for market, size in truth["positions"]}
    result["available_collateral_usd"] = str(truth["available_collateral_usd"])
    return result


def _lane_factory() -> LaneFactory:
    def create(slot: BoundContinuousSlot, journal: ActionJournal, vault: str | None) -> Any:
        return ContinuousSignerLane(
            follower_account=slot.config.follower_account_address,
            api_wallet_address=slot.api_wallet_address,
            key_file=slot.api_private_key_file,
            vault_address=vault,
            is_mainnet=True,
            journal=journal,
        )

    return create


def _locks(bound: BoundContinuousPlan, directory: Path) -> ExitStack:
    paths = {
        path
        for slot in bound.slots
        for path in (
            account_runtime_lock_path(
                directory,
                network="mainnet",
                action_account=slot.config.follower_account_address,
            ),
            signer_runtime_lock_path(
                directory,
                network="mainnet",
                signer_address=slot.api_wallet_address,
            ),
        )
    }
    stack = ExitStack()
    try:
        for path in sorted(paths, key=lambda item: str(item).casefold()):
            stack.enter_context(AccountRuntimeFileLock(path))
    except BaseException:
        stack.close()
        raise
    return stack


def _finish(
    directory: Path,
    status: str,
    blockers: list[str],
    rows: list[dict[str, Any]],
    truths: list[dict[str, Any]],
    completed: set[str],
    *,
    recovery: bool = False,
    mutated: bool = False,
) -> CanaryProofResult:
    _write(directory / "terminal-truth.json", {"observations": truths})
    latency_keys = ("slot", "phase", "cloid", "received_to_send_ms", "send_to_response_ms")
    latency = {"samples": [{key: row[key] for key in latency_keys} for row in rows]}
    _write(directory / "latency-summary.json", latency)
    passed = status == "passed" and not blockers and completed == set(PROOF_MARKETS)
    summary = {
        "version": 1,
        "status": status,
        "passed": passed,
        "mutated": mutated,
        "recovery_required": recovery,
        "completed_slots": sorted(completed),
        "action_count": len(rows),
        "entry_count": sum(row["phase"] == "entry" for row in rows),
        "close_count": sum(row["phase"] == "close" for row in rows),
        "rest_action_count": 0,
        "duplicate_cloid_count": len(rows) - len({row["cloid"] for row in rows}),
        "blockers": blockers,
    }
    _write(directory / "summary.json", summary)
    return CanaryProofResult(
        directory,
        status,
        passed,
        mutated,
        recovery,
        tuple(blockers),
        len(rows),
    )


def _proof_dir(local_appdata: Path | None, started_ms: int) -> Path:
    base = local_appdata or Path(os.environ["LOCALAPPDATA"])
    stamp = datetime.fromtimestamp(started_ms / 1_000, tz=UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return (
        base.resolve()
        / "HyperliquidCopytrader"
        / "runtime"
        / "continuous-v1"
        / f"proof-{stamp[:-4]}Z"
    )


def _lock_dir(local_appdata: Path | None, explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    if local_appdata is not None:
        return local_appdata.resolve() / "hyperliquid-copytrader" / "runtime-locks"
    return default_runtime_lock_dir().resolve()


def _write(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _desired(slot: BoundContinuousSlot, proof_id: str, phase: str, size: Decimal) -> str:
    raw = f"{proof_id}|{slot.config.slot}|{phase}|{size}|{slot.config.follower_account_address}"
    return sha256(raw.encode()).hexdigest()


def _redact(address: str) -> str:
    return address[:8] + "..." + address[-6:]


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)
