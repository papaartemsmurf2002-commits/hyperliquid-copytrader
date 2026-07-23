from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from time import monotonic_ns
from typing import Any, Mapping

from websockets.asyncio.client import connect

from hyperliquid_copytrader.action_journal import ActionJournal, ActionState
from hyperliquid_copytrader.continuous_canary import (
    MAINNET_PROOF_ACKNOWLEDGEMENT,
    PROOF_MARKETS,
    _check_truth,
    _chunk,
    _lane_factory,
    _locks,
    _log_action,
    _truth_payload,
)
from hyperliquid_copytrader.continuous_config import bind_continuous_plan, load_continuous_plan
from hyperliquid_copytrader.continuous_executor import ExecutionAttempt
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.market_catalog import CatalogRevision
from hyperliquid_copytrader.market_stream import MarketSnapshot, MarketStream
from hyperliquid_copytrader.runtime_lock import default_runtime_lock_dir
from hyperliquid_copytrader.ws_actions import WsPostMux

from run_continuous_mainnet_proof import (
    WS_URL,
    _heartbeat,
    _now_ms,
    _truth_reader,
)


class RecoveryError(RuntimeError):
    pass


async def _market_pump(
    socket: Any,
    stream: MarketStream,
    epoch: int,
    market: str,
) -> None:
    async for raw in socket:
        message = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(message, Mapping):
            continue
        channel = message.get("channel")
        if channel == "error":
            raise RecoveryError(f"market subscription failed: {message}")
        if channel in {"activeAssetCtx", "l2Book"}:
            stream.apply(message, epoch=epoch, received_ms=_now_ms())


async def _fresh_snapshot(
    stream: MarketStream,
    market: str,
    *,
    timeout_s: float = 10.0,
    max_age_ms: int = 5_000,
) -> MarketSnapshot:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        snapshot = stream.fresh_snapshot(market, now_ms=_now_ms(), max_age_ms=max_age_ms)
        if snapshot is not None:
            return snapshot
        await asyncio.sleep(0.05)
    raise RecoveryError(f"{market} has no fresh recovery book")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RecoveryError(f"{path.name} is not an object")
    return value


def _proof_catalog(path: Path) -> CatalogRevision:
    payload = _read_json(path)
    claimed_revision = str(payload.get("revision_id") or "")
    if not claimed_revision.endswith("-proof"):
        raise RecoveryError("proof catalog does not have the expected proof revision suffix")
    payload["revision_id"] = claimed_revision.removesuffix("-proof")
    base = CatalogRevision.from_payload(payload)
    return replace(base, revision_id=claimed_revision)


def _entry_evidence(proof_dir: Path) -> dict[str, Any]:
    rows = [
        json.loads(line)
        for line in (proof_dir / "ws-actions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise RecoveryError("proof must contain exactly one attempted action")
    row = rows[0]
    if (
        row.get("slot") != "acc7"
        or row.get("market") != PROOF_MARKETS["acc7"]
        or row.get("phase") != "entry"
        or row.get("reduce_only") is not False
        or row.get("state") != ActionState.FILLED.value
    ):
        raise RecoveryError("the only proof action is not the expected filled acc7 entry")
    return row


async def _run(
    repo_root: Path,
    plan_path: Path,
    proof_dir: Path,
    acknowledgement: str,
) -> int:
    if acknowledgement != MAINNET_PROOF_ACKNOWLEDGEMENT:
        raise RecoveryError("exact mainnet proof acknowledgement is missing")
    proof_dir = proof_dir.resolve()
    if not proof_dir.is_dir():
        raise RecoveryError("proof directory does not exist")
    manifest = _read_json(proof_dir / "manifest.json")
    summary = _read_json(proof_dir / "summary.json")
    preflight = _read_json(proof_dir / "preflight.json")
    catalog = _proof_catalog(proof_dir / "catalog.json")
    entry = _entry_evidence(proof_dir)

    plan = load_continuous_plan(plan_path)
    bound = bind_continuous_plan(plan, repo_root=repo_root, verify_secrets=True)
    if (
        plan.network != "mainnet"
        or plan.runtime_id != "continuous-v1-proof"
        or manifest.get("plan_sha256") != plan.sha256
        or manifest.get("catalog_revision") != catalog.revision_id
        or manifest.get("mutation_transport") != "websocket_post_only"
        or summary.get("status") != "recovery_required"
        or summary.get("action_count") != 1
        or summary.get("entry_count") != 1
        or summary.get("close_count") != 0
        or preflight.get("passed") is not True
    ):
        raise RecoveryError("proof artifacts do not bind to this recovery plan")

    slot_by_id = {slot.config.slot: slot for slot in bound.slots}
    if set(slot_by_id) != set(PROOF_MARKETS):
        raise RecoveryError("recovery plan is not the exact acc7/acc1 plan")
    active_slot = slot_by_id["acc7"]
    market = PROOF_MARKETS["acc7"]
    if active_slot.config.allowed_markets != (market,):
        raise RecoveryError("acc7 recovery market is outside the bound allowlist")

    journal_path = proof_dir.parent / "mainnet-proof-actions.sqlite3"
    expected_journal = Path(str(manifest.get("stable_action_journal") or "")).resolve()
    if expected_journal != journal_path.resolve() or not journal_path.is_file():
        raise RecoveryError("stable action journal provenance does not match the proof")

    stream = MarketStream(catalog=catalog, active_markets=(market,))
    mux = WsPostMux(response_timeout_s=3.0)
    recovery_rows: list[dict[str, Any]] = []
    truths: list[dict[str, Any]] = []
    recovery_log = proof_dir / "recovery-ws-actions.jsonl"
    if recovery_log.exists():
        raise RecoveryError("this proof already has a recovery action log")

    async with (
        connect(WS_URL, ping_interval=None, close_timeout=2) as market_socket,
        connect(WS_URL, ping_interval=None, close_timeout=2) as action_socket,
    ):
        market_epoch = stream.begin_connection(received_ms=_now_ms())
        for subscription in stream.subscription_specs:
            await market_socket.send(
                json.dumps({"method": "subscribe", "subscription": subscription})
            )
        action_epoch = mux.attach(action_socket)
        tasks = [
            asyncio.create_task(_market_pump(market_socket, stream, market_epoch, market)),
            asyncio.create_task(mux.receive_loop(action_epoch)),
            asyncio.create_task(_heartbeat(market_socket)),
            asyncio.create_task(_heartbeat(action_socket)),
        ]
        try:
            await _fresh_snapshot(stream, market)
            with ActionJournal(journal_path) as journal, _locks(bound, default_runtime_lock_dir()):
                record = journal.get_owned_action(
                    str(entry["cloid"]),
                    follower_account=active_slot.config.follower_account_address,
                    api_wallet=active_slot.api_wallet_address,
                )
                if (
                    record is None
                    or record.state is not ActionState.FILLED
                    or record.cumulative_filled_size != Decimal(str(entry["filled_size"]))
                ):
                    raise RecoveryError("durable entry record does not match proof evidence")
                if journal.recovery_actions():
                    raise RecoveryError(
                        "journal contains an unresolved action; do not issue a close"
                    )

                lanes = {
                    slot.config.slot: _lane_factory()(
                        slot,
                        journal,
                        slot.config.follower_account_address,
                    )
                    for slot in bound.slots
                }

                async def truth(slot_id: str) -> dict[str, Any]:
                    slot = slot_by_id[slot_id]
                    value = await _truth_reader(
                        slot,
                        mux.capture_epoch(),
                        mux=mux,
                        catalog=catalog,
                    )
                    _check_truth(value, slot, "recovery", require_flat=False)
                    if value["open_order_count"] != 0:
                        raise RecoveryError(f"{slot_id} has an open order during recovery")
                    return value

                before_acc7 = await truth("acc7")
                before_acc1 = await truth("acc1")
                truths.extend(
                    (
                        _truth_payload(before_acc7, "recovery_before"),
                        _truth_payload(before_acc1, "recovery_before"),
                    )
                )
                if any(size != 0 for _coin, size in before_acc1["positions"]):
                    raise RecoveryError("acc1 changed while acc7 recovery was pending")
                positions = {coin: size for coin, size in before_acc7["positions"] if size}
                if positions != {market: record.cumulative_filled_size}:
                    raise RecoveryError(
                        "authoritative acc7 position no longer matches the proof fill"
                    )

                remaining = positions[market]
                if remaining <= 0:
                    raise RecoveryError("expected a positive acc7 proof position")
                for close_no in range(1, 4):
                    snapshot = await _fresh_snapshot(stream, market)
                    executable = _chunk(
                        snapshot,
                        False,
                        remaining,
                        active_slot.config.max_order_notional_usd,
                        reduce_only=True,
                        current_position_size=remaining,
                    )
                    action = NextAction(
                        desired_id=f"proof-recovery-{entry['cloid']}-{close_no}",
                        market=market,
                        side="sell",
                        size=executable.size,
                        reduce_only=True,
                        reason="recover bounded proof position",
                    )
                    attempt = await lanes["acc7"].execute_ioc(
                        action=action,
                        asset_id=snapshot.asset_id,
                        limit_px=executable.limit_px,
                        mux=mux,
                        required_epoch=mux.capture_epoch(),
                        received_mono_ns=monotonic_ns(),
                    )
                    if not attempt.record.terminal:
                        resolved = await lanes["acc7"].resolve_by_cloid(
                            attempt.record.cloid,
                            mux=mux,
                            required_epoch=mux.capture_epoch(),
                        )
                        attempt = ExecutionAttempt(
                            resolved,
                            attempt.result,
                            attempt.received_to_send_ms,
                            attempt.send_to_response_ms,
                        )
                    if not attempt.record.terminal:
                        raise RecoveryError("reduce-only recovery CLOID remains unresolved")
                    _log_action(
                        recovery_log,
                        recovery_rows,
                        active_slot,
                        "recovery_close",
                        action,
                        executable,
                        snapshot,
                        attempt,
                    )
                    after = await truth("acc7")
                    truths.append(_truth_payload(after, f"recovery_after_{close_no}"))
                    after_positions = {coin: size for coin, size in after["positions"] if size}
                    if not after_positions:
                        remaining = Decimal("0")
                        break
                    if (
                        set(after_positions) != {market}
                        or not 0 < after_positions[market] < remaining
                    ):
                        raise RecoveryError(
                            "recovery did not monotonically reduce the proof position"
                        )
                    remaining = after_positions[market]
                if remaining != 0:
                    raise RecoveryError("proof position remains after three reduce-only closes")

                terminal_acc1 = await truth("acc1")
                truths.append(_truth_payload(terminal_acc1, "recovery_terminal"))
                if any(size != 0 for _coin, size in terminal_acc1["positions"]):
                    raise RecoveryError("acc1 is not flat after recovery")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    output = {
        "status": "recovered_flat",
        "passed": True,
        "rest_action_count": 0,
        "close_action_count": len(recovery_rows),
        "truth": truths,
    }
    (proof_dir / "recovery-summary.json").write_text(
        json.dumps(output, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps({key: value for key, value in output.items() if key != "truth"}, sort_keys=True)
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover the exact bounded WS proof position")
    parser.add_argument("--proof-dir", type=Path, required=True)
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path(".secrets/mainnet-two-account-continuous-proof.json"),
    )
    parser.add_argument("--acknowledgement", required=True)
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    raise SystemExit(
        asyncio.run(
            _run(
                repo_root,
                args.plan.resolve(),
                args.proof_dir.resolve(),
                args.acknowledgement,
            )
        )
    )


if __name__ == "__main__":
    main()
