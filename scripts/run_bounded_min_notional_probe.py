from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass, replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any

from hyperliquid_copytrader.action_journal import ActionJournal, ActionRecord
from hyperliquid_copytrader.continuous_config import (
    BoundContinuousPlan,
    BoundContinuousSlot,
    bind_continuous_plan,
    load_continuous_plan,
)
from hyperliquid_copytrader.continuous_executor import ContinuousSignerLane
from hyperliquid_copytrader.continuous_follower import WsFollowerInfo
from hyperliquid_copytrader.continuous_runner import (
    StartupInfo,
    _effective_catalog_slot,
    build_startup_catalog,
    ensure_engine_identity,
    run_ws_startup_preflight,
)
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.market_stream import MarketSnapshot, MarketStream, executable_ioc
from hyperliquid_copytrader.models import parse_decimal
from hyperliquid_copytrader.observer import parse_clearinghouse_positions
from hyperliquid_copytrader.order_preflight import (
    HYPERLIQUID_PERP_MIN_NOTIONAL_USD,
    HyperliquidPerpRules,
    preflight_hyperliquid_perp_order,
)
from hyperliquid_copytrader.precision import aggressive_ioc_price, quantize_size
from hyperliquid_copytrader.runtime_lock import (
    AccountRuntimeFileLock,
    account_runtime_lock_path,
    default_runtime_lock_dir,
    signer_runtime_lock_path,
)
from hyperliquid_copytrader.websocket_transport import connect_websocket_ipv6_preferred
from hyperliquid_copytrader.windows_runtime import atomic_json_write
from hyperliquid_copytrader.ws_actions import PostOutcome, WsPostMux


REST_URL = "https://api.hyperliquid.xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"
ACKNOWLEDGEMENT = (
    "I AUTHORIZE EXACTLY ONE APPROXIMATELY 1 USD MAINNET RISK-INCREASING IOC ON "
    "BTC/ACC1 AND ONE ON XYZ:CL/ACC5, PLUS BOUNDED REDUCE-ONLY CLEANUP"
)
TARGET_USD = Decimal("1")
MAX_WIRE_NOTIONAL_USD = Decimal("1.10")
ENTRY_SLIPPAGE_BPS = Decimal("25")
CLEANUP_SLIPPAGE_BPS = Decimal("100")
MAX_BOOK_AGE_MS = 2_000
MAX_CLEANUP_ATTEMPTS = 3


@dataclass(frozen=True, slots=True)
class ProbeCase:
    slot: str
    market: str


CASES = (ProbeCase("acc1", "BTC"), ProbeCase("acc5", "xyz:CL"))


class ProbeError(RuntimeError):
    pass


def now_ms() -> int:
    return time_ns() // 1_000_000


def _selected(bound: BoundContinuousPlan) -> tuple[BoundContinuousSlot, ...]:
    by_slot = {slot.config.slot: slot for slot in bound.slots}
    missing = [case.slot for case in CASES if case.slot not in by_slot]
    if missing:
        raise ProbeError("probe slots are absent from the fleet: " + ",".join(missing))
    return tuple(by_slot[case.slot] for case in CASES)


def _lock_stack(bound: BoundContinuousPlan, lock_dir: Path) -> ExitStack:
    paths = {
        path
        for slot in bound.slots
        for path in (
            account_runtime_lock_path(
                lock_dir,
                network="mainnet",
                action_account=slot.config.follower_account_address,
            ),
            signer_runtime_lock_path(
                lock_dir,
                network="mainnet",
                signer_address=slot.api_wallet_address,
            ),
        )
    }
    if len(paths) != len(bound.slots) * 2:
        raise ProbeError("fleet lock identities are not unique")
    stack = ExitStack()
    try:
        for path in sorted(paths, key=lambda item: str(item).casefold()):
            stack.enter_context(AccountRuntimeFileLock(path))
    except BaseException:
        stack.close()
        raise
    return stack


def _probe_dir() -> Path:
    root = Path(os.environ["LOCALAPPDATA"]).resolve()
    return root / "HyperliquidCopytrader" / "runtime" / "minimum-order-probes" / f"probe-{now_ms()}"


def _manifest(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_json_write(path, dict(payload))


async def _heartbeat(socket: Any) -> None:
    while True:
        await asyncio.sleep(20)
        await socket.send('{"method":"ping"}')


async def _market_pump(
    socket: Any,
    stream: MarketStream,
    epoch: int,
    ready: asyncio.Event,
) -> None:
    async for raw in socket:
        try:
            message = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(message, Mapping):
                continue
            if message.get("channel") not in {"activeAssetCtx", "l2Book"}:
                continue
            observed = now_ms()
            stream.apply(message, epoch=epoch, received_ms=observed)
            if all(
                stream.fresh_snapshot(
                    case.market,
                    now_ms=observed,
                    max_age_ms=MAX_BOOK_AGE_MS,
                )
                is not None
                for case in CASES
            ):
                ready.set()
        except (TypeError, ValueError):
            continue


async def _info_data(
    mux: WsPostMux,
    epoch: int,
    payload: Mapping[str, Any],
    *,
    timeout_s: float = 5.0,
) -> Any:
    result = await mux.post_info(payload, required_epoch=epoch, timeout_s=timeout_s)
    if result.outcome is not PostOutcome.INFO:
        detail = " ".join(str(result.response or result.reason).split())[:300]
        raise ProbeError(f"WS info {payload.get('type')} failed: {detail}")
    response = result.response
    if not isinstance(response, Mapping) or response.get("type") != "info":
        raise ProbeError(f"WS info {payload.get('type')} returned a malformed envelope")
    inner = response.get("payload")
    if not isinstance(inner, Mapping) or inner.get("type") != payload.get("type"):
        raise ProbeError(f"WS info {payload.get('type')} returned a mismatched payload")
    return inner.get("data")


def _dex_suffix(market: str) -> dict[str, str]:
    dex = market.split(":", 1)[0] if ":" in market else ""
    return {} if not dex else {"dex": dex}


async def _market_truth(
    slot: BoundContinuousSlot,
    market: str,
    mux: WsPostMux,
) -> tuple[Decimal, int]:
    epoch = mux.capture_epoch()
    suffix = _dex_suffix(market)
    state, orders = await asyncio.gather(
        _info_data(
            mux,
            epoch,
            {
                "type": "clearinghouseState",
                "user": slot.config.follower_account_address,
                **suffix,
            },
        ),
        _info_data(
            mux,
            epoch,
            {
                "type": "openOrders",
                "user": slot.config.follower_account_address,
                **suffix,
            },
        ),
    )
    if not isinstance(state, Mapping):
        raise ProbeError(f"{market} clearinghouse truth is malformed")
    if not isinstance(orders, list) or not all(isinstance(row, Mapping) for row in orders):
        raise ProbeError(f"{market} open-order truth is malformed")
    positions = parse_clearinghouse_positions(
        dict(state),
        observed_ms=now_ms(),
        dex=suffix.get("dex", ""),
    )
    foreign = {
        symbol: position.size
        for symbol, position in positions.items()
        if symbol != market and position.size != 0
    }
    if foreign:
        raise ProbeError(f"{slot.config.slot} has an unexpected same-DEX position: {foreign}")
    position = positions.get(market)
    return position.size if position is not None else Decimal("0"), len(orders)


async def _all_dex_positions(
    slot: BoundContinuousSlot,
    follower_info: WsFollowerInfo,
    mux: WsPostMux,
) -> dict[str, Decimal]:
    truth = await follower_info(
        slot=replace(slot, external_writers_allowed=True),
        mux=mux,
        epoch=mux.capture_epoch(),
        now_ms=now_ms(),
    )
    return {
        market: position.size for market, position in truth.positions.items() if position.size != 0
    }


async def _capacity(
    slot: BoundContinuousSlot,
    market: str,
    mux: WsPostMux,
) -> dict[str, Any]:
    payload = await _info_data(
        mux,
        mux.capture_epoch(),
        {
            "type": "activeAssetData",
            "user": slot.config.follower_account_address,
            "coin": market,
        },
    )
    if not isinstance(payload, Mapping):
        raise ProbeError(f"{market} activeAssetData is malformed")
    if str(payload.get("user") or "").lower() != slot.config.follower_account_address:
        raise ProbeError(f"{market} activeAssetData returned the wrong follower")
    if str(payload.get("coin") or "") != market:
        raise ProbeError(f"{market} activeAssetData returned the wrong market")
    leverage_payload = payload.get("leverage")
    if not isinstance(leverage_payload, Mapping):
        raise ProbeError(f"{market} activeAssetData has no leverage block")
    raw_leverage = leverage_payload.get("value")
    if isinstance(raw_leverage, bool) or not isinstance(raw_leverage, (int, str)):
        raise ProbeError(f"{market} activeAssetData leverage is malformed")
    leverage = int(raw_leverage)
    mode = str(leverage_payload.get("type") or "").lower()
    max_sizes = payload.get("maxTradeSzs")
    available = payload.get("availableToTrade")
    if (
        mode not in {"cross", "isolated"}
        or leverage <= 0
        or not isinstance(max_sizes, list)
        or len(max_sizes) != 2
        or not isinstance(available, list)
        or len(available) != 2
    ):
        raise ProbeError(f"{market} activeAssetData capacity is incomplete")
    return {
        "leverage": leverage,
        "margin_mode": mode,
        "max_buy_size": parse_decimal(max_sizes[0]),
        "available_buy_size": parse_decimal(available[0]),
    }


def _candidate(snapshot: MarketSnapshot, capacity: Mapping[str, Any]) -> dict[str, Any]:
    requested = quantize_size(TARGET_USD / snapshot.mark_px, snapshot.sz_decimals)
    if requested <= 0:
        raise ProbeError(f"{snapshot.market} one-dollar target rounds below one lot")
    executable = executable_ioc(
        snapshot,
        is_buy=True,
        requested_size=requested,
        max_slippage_bps=None,
        hard_limit_px=aggressive_ioc_price(
            snapshot.mark_px,
            is_buy=True,
            slippage_bps=ENTRY_SLIPPAGE_BPS,
            sz_decimals=snapshot.sz_decimals,
        ),
    )
    if executable is None:
        raise ProbeError(f"{snapshot.market} has no executable depth inside 25 bps")
    wire_notional = executable.size * executable.limit_px
    if wire_notional <= 0 or wire_notional > MAX_WIRE_NOTIONAL_USD:
        raise ProbeError(f"{snapshot.market} wire notional {wire_notional} exceeds the probe cap")
    if executable.size > capacity["max_buy_size"]:
        raise ProbeError(f"{snapshot.market} probe exceeds activeAssetData maxTradeSzs")
    if executable.size > capacity["available_buy_size"]:
        raise ProbeError(f"{snapshot.market} probe exceeds activeAssetData availableToTrade")
    rules = HyperliquidPerpRules(
        market=snapshot.market,
        sz_decimals=snapshot.sz_decimals,
        max_leverage=snapshot.max_leverage,
        margin_mode=str(capacity["margin_mode"]),
    )
    canonical = preflight_hyperliquid_perp_order(
        rules=rules,
        requested_quantity=executable.size,
        price=executable.limit_px,
        side="buy",
        max_order_notional_usd=MAX_WIRE_NOTIONAL_USD,
        reduce_only=False,
        leverage=int(capacity["leverage"]),
        available_collateral_usd=None,
    )
    if canonical.placeable or "below the perp minimum" not in canonical.reason:
        raise ProbeError(
            f"{snapshot.market} canonical preflight did not isolate minimum notional: "
            f"{canonical.reason}"
        )
    structural = preflight_hyperliquid_perp_order(
        rules=replace(rules, minimum_notional_usd=Decimal("0")),
        requested_quantity=executable.size,
        price=executable.limit_px,
        side="buy",
        max_order_notional_usd=MAX_WIRE_NOTIONAL_USD,
        reduce_only=False,
        leverage=int(capacity["leverage"]),
        available_collateral_usd=None,
    )
    if not structural.placeable:
        raise ProbeError(f"{snapshot.market} structural preflight failed: {structural.reason}")
    return {
        "requested_size": executable.size,
        "limit_px": executable.limit_px,
        "wire_notional_usd": wire_notional,
        "estimated_vwap": executable.estimated_vwap,
        "published_minimum_usd": HYPERLIQUID_PERP_MIN_NOTIONAL_USD,
        "canonical_reason": canonical.reason,
        "required_margin_usd": structural.required_margin_usd,
        **dict(capacity),
    }


def _desired_id(
    probe_id: str,
    slot: BoundContinuousSlot,
    market: str,
    phase: str,
    size: Decimal,
) -> str:
    value = (
        f"{probe_id}|{slot.config.slot}|{market}|{phase}|{size}|"
        f"{slot.config.follower_account_address}"
    )
    return sha256(value.encode()).hexdigest()


def _record_payload(record: ActionRecord) -> dict[str, Any]:
    return {
        "cloid": record.cloid,
        "market": record.market,
        "desired_id": record.desired_id,
        "requested_size": str(record.requested_size),
        "filled_size": str(record.cumulative_filled_size),
        "state": record.state.value,
        "outcome_detail": record.outcome_detail,
        "terminal": record.terminal,
    }


async def _terminal_record(
    lane: ContinuousSignerLane,
    record: ActionRecord,
    mux: WsPostMux,
) -> ActionRecord:
    if record.terminal:
        return record
    delays = (0.0, 0.5, 1.0, 2.0, 4.0, 8.0)
    current = record
    for delay in delays:
        if delay:
            await asyncio.sleep(delay)
        current = await lane.resolve_by_cloid(
            current.cloid,
            mux=mux,
            required_epoch=mux.capture_epoch(),
        )
        if current.terminal:
            return current
    raise ProbeError(f"CLOID {record.cloid} remains unresolved; recovery is required")


def _fresh_snapshot(stream: MarketStream, market: str) -> MarketSnapshot:
    snapshot = stream.fresh_snapshot(
        market,
        now_ms=now_ms(),
        max_age_ms=MAX_BOOK_AGE_MS,
    )
    if snapshot is None:
        raise ProbeError(f"{market} has no fresh same-epoch market snapshot")
    return snapshot


async def _cleanup(
    *,
    probe_id: str,
    slot: BoundContinuousSlot,
    market: str,
    lane: ContinuousSignerLane,
    stream: MarketStream,
    mux: WsPostMux,
    follower_info: WsFollowerInfo,
    expected_max_position: Decimal,
    actions: list[dict[str, Any]],
) -> None:
    if expected_max_position <= 0:
        raise ProbeError("cleanup bound must be positive")
    previous_abs: Decimal | None = None
    for attempt_no in range(1, MAX_CLEANUP_ATTEMPTS + 1):
        positions = await _all_dex_positions(slot, follower_info, mux)
        foreign = {symbol: size for symbol, size in positions.items() if symbol != market}
        if foreign:
            raise ProbeError(f"{slot.config.slot} has unrelated exposure during cleanup: {foreign}")
        position = positions.get(market, Decimal("0"))
        if position == 0:
            return
        if position < 0 or position > expected_max_position:
            raise ProbeError(
                f"{slot.config.slot}/{market} position {position} is outside the "
                f"authorized probe bound 0..{expected_max_position}"
            )
        if previous_abs is not None and abs(position) >= previous_abs:
            raise ProbeError(f"{slot.config.slot}/{market} cleanup made no progress")
        previous_abs = abs(position)
        snapshot = _fresh_snapshot(stream, market)
        executable = executable_ioc(
            snapshot,
            is_buy=False,
            requested_size=abs(position),
            max_slippage_bps=CLEANUP_SLIPPAGE_BPS,
        )
        if executable is None or executable.size != abs(position):
            raise ProbeError(f"{slot.config.slot}/{market} exact cleanup has insufficient depth")
        action = NextAction(
            _desired_id(probe_id, slot, market, f"cleanup-{attempt_no}", abs(position)),
            market,
            "sell",
            abs(position),
            True,
            "bounded minimum-order probe cleanup",
        )
        result = await lane.execute_ioc(
            action=action,
            asset_id=snapshot.asset_id,
            limit_px=executable.limit_px,
            mux=mux,
            required_epoch=mux.capture_epoch(),
            received_mono_ns=monotonic_ns(),
        )
        record = await _terminal_record(lane, result.record, mux)
        actions.append({"phase": f"cleanup-{attempt_no}", **_record_payload(record)})
        if record.cumulative_filled_size <= 0:
            raise ProbeError(f"{slot.config.slot}/{market} cleanup did not fill")
    positions = await _all_dex_positions(slot, follower_info, mux)
    if positions:
        raise ProbeError(f"{slot.config.slot}/{market} remains exposed after bounded cleanup")


async def _resolve_probe_recovery(
    *,
    lane: ContinuousSignerLane,
    mux: WsPostMux,
    actions: list[dict[str, Any]],
) -> tuple[ActionRecord, ...]:
    resolved: list[ActionRecord] = []
    for recovered in lane.recover_provably_unsent():
        actions.append({"phase": "recovery-not-sent", **_record_payload(recovered)})
        resolved.append(recovered)
    for record in lane.journal.recovery_actions(
        follower_account=lane.follower_account,
        api_wallet=lane.api_wallet_address,
    ):
        terminal = await _terminal_record(lane, record, mux)
        actions.append({"phase": "recovery-order-status", **_record_payload(terminal)})
        resolved.append(terminal)
    return tuple(resolved)


async def _wait_for_recorded_fill_truth(
    *,
    slot: BoundContinuousSlot,
    market: str,
    recorded_fill: Decimal,
    requested_size: Decimal,
    follower_info: WsFollowerInfo,
    mux: WsPostMux,
) -> None:
    for delay in (0.0, 0.25, 0.5, 1.0, 2.0, 4.0):
        if delay:
            await asyncio.sleep(delay)
        positions = await _all_dex_positions(slot, follower_info, mux)
        foreign = {symbol: size for symbol, size in positions.items() if symbol != market}
        if foreign:
            raise ProbeError(f"{slot.config.slot} gained unrelated exposure: {foreign}")
        position = positions.get(market, Decimal("0"))
        if position < 0 or position > requested_size:
            raise ProbeError(
                f"{slot.config.slot}/{market} authoritative position {position} is outside "
                f"the submitted probe bound 0..{requested_size}"
            )
        if position == recorded_fill:
            return
    raise ProbeError(
        f"{slot.config.slot}/{market} journal fill {recorded_fill} did not converge "
        "with authoritative all-DEX account truth"
    )


async def _execute_cases(
    *,
    probe_id: str,
    selected: tuple[BoundContinuousSlot, ...],
    engine_state_dir: Path,
    catalog: Any,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> None:
    stream = MarketStream(catalog=catalog, active_markets=(case.market for case in CASES))
    follower_info = WsFollowerInfo(catalog=catalog)
    ready = asyncio.Event()
    mux = WsPostMux(response_timeout_s=5.0, write_timeout_s=2.0)
    journals: list[ActionJournal] = []
    try:
        lanes: dict[str, ContinuousSignerLane] = {}
        for slot in selected:
            journal_path = engine_state_dir / "actions" / f"{slot.config.slot}.sqlite3"
            if not journal_path.is_file():
                raise ProbeError(f"canonical action journal is missing: {journal_path}")
            journal = ActionJournal(journal_path)
            journals.append(journal)
            recovery = journal.recovery_actions(
                follower_account=slot.config.follower_account_address,
                api_wallet=slot.api_wallet_address,
            )
            if recovery:
                raise ProbeError(
                    f"{slot.config.slot} canonical journal has unresolved actions: "
                    + ",".join(row.cloid for row in recovery)
                )
            recent = journal.recent_send_attempts(
                follower_account=slot.config.follower_account_address,
                api_wallet=slot.api_wallet_address,
                after_ms=max(0, now_ms() - 60_000),
            )
            if len(recent) + 1 + MAX_CLEANUP_ATTEMPTS > slot.config.action_limit_per_minute:
                raise ProbeError(f"{slot.config.slot} lacks rolling action-budget headroom")
            lanes[slot.config.slot] = ContinuousSignerLane(
                follower_account=slot.config.follower_account_address,
                api_wallet_address=slot.api_wallet_address,
                key_file=slot.api_private_key_file,
                vault_address=slot.config.follower_account_address,
                is_mainnet=True,
                journal=journal,
            )

        async with (
            connect_websocket_ipv6_preferred(
                WS_URL,
                proxy=None,
                ping_interval=None,
                open_timeout=10,
                close_timeout=2,
                max_queue=1_024,
            ) as market_socket,
            connect_websocket_ipv6_preferred(
                WS_URL,
                proxy=None,
                ping_interval=None,
                open_timeout=10,
                close_timeout=2,
                max_queue=1_024,
            ) as action_socket,
        ):
            market_epoch = stream.begin_connection(received_ms=now_ms())
            for subscription in stream.subscription_specs:
                await market_socket.send(
                    json.dumps({"method": "subscribe", "subscription": subscription})
                )
            action_epoch = mux.attach(action_socket)
            tasks = [
                asyncio.create_task(_market_pump(market_socket, stream, market_epoch, ready)),
                asyncio.create_task(mux.receive_loop(action_epoch)),
                asyncio.create_task(_heartbeat(market_socket)),
                asyncio.create_task(_heartbeat(action_socket)),
            ]
            try:
                await asyncio.wait_for(ready.wait(), timeout=15)
                by_slot = {slot.config.slot: slot for slot in selected}
                for case in CASES:
                    slot = by_slot[case.slot]
                    positions = await _all_dex_positions(slot, follower_info, mux)
                    if positions:
                        raise ProbeError(f"{case.slot} is not flat and order-free: {positions}")
                    snapshot = _fresh_snapshot(stream, case.market)
                    capacity = await _capacity(slot, case.market, mux)
                    candidate = _candidate(snapshot, capacity)
                    case_actions: list[dict[str, Any]] = []
                    case_payload: dict[str, Any] = {
                        "slot": case.slot,
                        "market": case.market,
                        "candidate": {key: str(value) for key, value in candidate.items()},
                        "actions": case_actions,
                        "desired_id": _desired_id(
                            probe_id,
                            slot,
                            case.market,
                            "entry",
                            candidate["requested_size"],
                        ),
                        "requested_size": str(candidate["requested_size"]),
                        "status": "prepared",
                    }
                    manifest["cases"].append(case_payload)
                    _manifest(manifest_path, manifest)
                    entry_error: Exception | None = None
                    record: ActionRecord | None = None

                    def persist_prepared(prepared: ActionRecord) -> None:
                        case_payload.update(
                            {
                                "prepared_action": _record_payload(prepared),
                                "status": "entry_durable_before_send",
                            }
                        )
                        _manifest(manifest_path, manifest)

                    try:
                        action = NextAction(
                            case_payload["desired_id"],
                            case.market,
                            "buy",
                            candidate["requested_size"],
                            False,
                            "bounded live minimum-notional hypothesis probe",
                        )
                        attempt = await lanes[case.slot].execute_ioc(
                            action=action,
                            asset_id=snapshot.asset_id,
                            limit_px=candidate["limit_px"],
                            mux=mux,
                            required_epoch=mux.capture_epoch(),
                            received_mono_ns=monotonic_ns(),
                            on_prepared=persist_prepared,
                        )
                        record = await _terminal_record(lanes[case.slot], attempt.record, mux)
                        case_actions.append({"phase": "entry", **_record_payload(record)})
                        case_payload["status"] = "entry_terminal"
                        _manifest(manifest_path, manifest)
                    except Exception as exc:
                        entry_error = exc
                        case_payload["status"] = "entry_failed_recovery_required"
                        case_payload["entry_error"] = (
                            f"{type(exc).__name__}: {' '.join(str(exc).split())[:300]}"
                        )
                        _manifest(manifest_path, manifest)
                    finally:
                        # The journal response is evidence, not authoritative account truth.
                        # Resolve any ambiguous CLOID, then query the exchange and close any
                        # observed position even when the entry appeared rejected locally.
                        recovered = await _resolve_probe_recovery(
                            lane=lanes[case.slot],
                            mux=mux,
                            actions=case_actions,
                        )
                        reported_fills = [
                            row.cumulative_filled_size
                            for row in recovered
                            if row.market == case.market
                        ]
                        if record is not None and record.market == case.market:
                            reported_fills.append(record.cumulative_filled_size)
                        recorded_fill = max(reported_fills, default=Decimal("0"))
                        if recorded_fill > 0:
                            await _wait_for_recorded_fill_truth(
                                slot=slot,
                                market=case.market,
                                recorded_fill=recorded_fill,
                                requested_size=candidate["requested_size"],
                                follower_info=follower_info,
                                mux=mux,
                            )
                        await _cleanup(
                            probe_id=probe_id,
                            slot=slot,
                            market=case.market,
                            lane=lanes[case.slot],
                            stream=stream,
                            mux=mux,
                            follower_info=follower_info,
                            expected_max_position=candidate["requested_size"],
                            actions=case_actions,
                        )
                    positions = await _all_dex_positions(slot, follower_info, mux)
                    if positions:
                        raise ProbeError(
                            f"{case.slot} terminal all-DEX truth is nonflat: {positions}"
                        )
                    case_payload["status"] = (
                        "recovered_flat_after_entry_error"
                        if entry_error is not None
                        else "flat_terminal"
                    )
                    case_payload["terminal_positions"] = {}
                    case_payload["terminal_open_orders"] = 0
                    _manifest(manifest_path, manifest)
                    if entry_error is not None:
                        raise entry_error
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for journal in journals:
            journal.close()


async def _run(args: argparse.Namespace) -> int:
    plan_path = args.plan.resolve()
    if not args.engine_state_dir.is_absolute():
        raise ProbeError("engine state directory must be absolute")
    engine_state_dir = args.engine_state_dir.resolve(strict=True)
    plan = load_continuous_plan(plan_path)
    if plan.network != "mainnet":
        raise ProbeError("minimum-order probe requires the mainnet fleet plan")
    if args.execute and args.acknowledgement != ACKNOWLEDGEMENT:
        raise ProbeError(f"acknowledgement must equal: {ACKNOWLEDGEMENT}")
    repo_root = Path(__file__).resolve().parents[1]
    public_bound = bind_continuous_plan(plan, repo_root=repo_root, verify_secrets=False)
    ensure_engine_identity(
        engine_state_dir,
        network=plan.network,
        runtime_id=plan.runtime_id,
        plan_sha256=plan.sha256,
        create=False,
    )
    lock_dir = default_runtime_lock_dir()
    proof_dir = _probe_dir()
    proof_dir.mkdir(parents=True, exist_ok=False)
    manifest_path = proof_dir / "manifest.json"
    manifest: dict[str, Any] = {
        "version": 1,
        "probe_id": proof_dir.name,
        "status": "preflight",
        "mutated": False,
        "plan_sha256": plan.sha256,
        "runtime_id": plan.runtime_id,
        "engine_state_dir": str(engine_state_dir),
        "cases": [],
    }
    _manifest(manifest_path, manifest)

    with _lock_stack(public_bound, lock_dir):
        bound = public_bound
        selected = _selected(bound)
        with StartupInfo(REST_URL) as info:
            catalog = build_startup_catalog(info, network="mainnet")
        effective_selected = tuple(_effective_catalog_slot(slot, catalog) for slot in selected)
        for case, slot in zip(CASES, effective_selected, strict=True):
            if case.market not in slot.config.allowed_markets:
                raise ProbeError(f"{case.slot}/{case.market} is not dynamically eligible")
        subset_plan = replace(plan, slots=tuple(slot.config for slot in effective_selected))
        subset = BoundContinuousPlan(plan=subset_plan, slots=effective_selected)
        preflight_mux = WsPostMux(response_timeout_s=5.0, write_timeout_s=2.0)
        preflight = await run_ws_startup_preflight(
            subset,
            network="mainnet",
            ws_url=WS_URL,
            mux=preflight_mux,
            catalog=catalog,
        )
        manifest["startup_http_logical_requests"] = info.logical_count
        manifest["startup_http_requests"] = info.count
        manifest["startup_http_weight"] = info.weight
        manifest["preflight"] = preflight
        manifest["preflight_scope"] = "selected_probe_slots"
        _manifest(manifest_path, manifest)
        rows = {str(row["slot"]): row for row in preflight.get("slots", [])}
        for case in CASES:
            row = rows.get(case.slot, {})
            if (
                row.get("passed") is not True
                or row.get("follower_nonflat") is not False
                or row.get("follower_open_order_count") != 0
                or row.get("identity", {}).get("signer_authorized") is not True
                or row.get("identity", {}).get("follower_account_mode") != "unified"
            ):
                raise ProbeError(f"{case.slot} did not pass the exact flat startup gate")
        if not args.execute:
            manifest["status"] = "preflight_passed"
            _manifest(manifest_path, manifest)
            print(json.dumps({"status": manifest["status"], "proof_dir": str(proof_dir)}))
            return 0
        manifest["mutated"] = True
        manifest["status"] = "executing"
        _manifest(manifest_path, manifest)
        await _execute_cases(
            probe_id=proof_dir.name,
            selected=effective_selected,
            engine_state_dir=engine_state_dir,
            catalog=catalog,
            manifest_path=manifest_path,
            manifest=manifest,
        )
        manifest["status"] = "passed_flat"
        _manifest(manifest_path, manifest)
    print(json.dumps({"status": manifest["status"], "proof_dir": str(proof_dir)}))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a fixed, bounded BTC and xyz:CL mainnet minimum-notional probe."
    )
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path(".secrets/mainnet-ten-account-continuous.json"),
    )
    parser.add_argument("--engine-state-dir", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--acknowledgement", default="")
    args = parser.parse_args()
    try:
        raise SystemExit(asyncio.run(_run(args)))
    except ProbeError as exc:
        print(json.dumps({"status": "failed_safe", "error": str(exc)}, sort_keys=True))
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
