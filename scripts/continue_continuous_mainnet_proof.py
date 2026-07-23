from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Mapping

from websockets.asyncio.client import connect

from hyperliquid_copytrader.action_journal import ActionJournal
from hyperliquid_copytrader.continuous_canary import (
    MAINNET_PROOF_ACKNOWLEDGEMENT,
    PROOF_MARKETS,
    _check_truth,
    _lane_factory,
    _locks,
    _trade_slot,
    _truth_payload,
    _write,
)
from hyperliquid_copytrader.continuous_config import bind_continuous_plan, load_continuous_plan
from hyperliquid_copytrader.continuous_preflight import run_continuous_preflight
from hyperliquid_copytrader.market_catalog import CatalogRevision
from hyperliquid_copytrader.market_stream import MarketStream
from hyperliquid_copytrader.runtime_lock import default_runtime_lock_dir
from hyperliquid_copytrader.ws_actions import WsPostMux

from recover_continuous_mainnet_proof import _proof_catalog
from run_continuous_mainnet_proof import (
    WS_URL,
    _heartbeat,
    _HttpInfo,
    _catalog,
    _now_ms,
    _truth_reader,
)


class ContinuationError(RuntimeError):
    pass


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ContinuationError(f"{path.name} is not an object")
    return value


def _prior_proof_is_recovered(proof_dir: Path, plan_sha256: str) -> CatalogRevision:
    manifest = _json(proof_dir / "manifest.json")
    failed = _json(proof_dir / "summary.json")
    recovered = _json(proof_dir / "recovery-summary.json")
    entry_rows = (proof_dir / "ws-actions.jsonl").read_text(encoding="utf-8").splitlines()
    close_rows = (proof_dir / "recovery-ws-actions.jsonl").read_text(encoding="utf-8").splitlines()
    if (
        manifest.get("plan_sha256") != plan_sha256
        or manifest.get("mutation_transport") != "websocket_post_only"
        or failed.get("status") != "recovery_required"
        or failed.get("entry_count") != 1
        or failed.get("close_count") != 0
        or recovered.get("status") != "recovered_flat"
        or recovered.get("passed") is not True
        or recovered.get("close_action_count") != 1
        or recovered.get("rest_action_count") != 0
        or len(entry_rows) != 1
        or len(close_rows) != 1
    ):
        raise ContinuationError("prior acc7 proof is not exactly recovered and flat")
    entry, close = json.loads(entry_rows[0]), json.loads(close_rows[0])
    if (
        entry.get("slot") != "acc7"
        or entry.get("market") != PROOF_MARKETS["acc7"]
        or entry.get("phase") != "entry"
        or entry.get("reduce_only") is not False
        or entry.get("state") != "FILLED"
        or close.get("slot") != "acc7"
        or close.get("market") != PROOF_MARKETS["acc7"]
        or close.get("phase") != "recovery_close"
        or close.get("reduce_only") is not True
        or close.get("state") != "FILLED"
        or close.get("filled_size") != entry.get("filled_size")
    ):
        raise ContinuationError("acc7 entry/recovery action evidence is inconsistent")
    return _proof_catalog(proof_dir / "catalog.json")


async def _market_pump(
    socket: Any,
    stream: MarketStream,
    epoch: int,
    ready: asyncio.Event,
) -> None:
    market = PROOF_MARKETS["acc1"]
    async for raw in socket:
        message = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(message, Mapping):
            continue
        if message.get("channel") == "error":
            raise ContinuationError(f"market subscription failed: {message}")
        if message.get("channel") in {"activeAssetCtx", "l2Book"}:
            stream.apply(message, epoch=epoch, received_ms=_now_ms())
            if stream.fresh_snapshot(market, now_ms=_now_ms(), max_age_ms=1_000):
                ready.set()


async def _run(
    repo_root: Path,
    plan_path: Path,
    proof_dir: Path,
    acknowledgement: str,
) -> int:
    if acknowledgement != MAINNET_PROOF_ACKNOWLEDGEMENT:
        raise ContinuationError("exact mainnet proof acknowledgement is missing")
    plan = load_continuous_plan(plan_path)
    bound = bind_continuous_plan(plan, repo_root=repo_root, verify_secrets=True)
    prior_catalog = _prior_proof_is_recovered(proof_dir, plan.sha256)
    slot_by_id = {slot.config.slot: slot for slot in bound.slots}
    if set(slot_by_id) != set(PROOF_MARKETS):
        raise ContinuationError("continuation is not bound to exact acc7/acc1 slots")

    continuation_dir = proof_dir / f"continuation-acc1-{_now_ms()}"
    continuation_dir.mkdir(parents=False, exist_ok=False)
    with _HttpInfo() as info:
        catalog = _catalog(info)
        for market in PROOF_MARKETS.values():
            old, new = prior_catalog.market(market), catalog.market(market)
            if (
                old is None
                or new is None
                or (old.asset_id, old.sz_decimals, old.dex)
                != (new.asset_id, new.sz_decimals, new.dex)
            ):
                raise ContinuationError(f"catalog identity changed for {market}")
        preflight = run_continuous_preflight(
            bound,
            network="mainnet",
            info=info,
            observed_ms=_now_ms(),
            require_flat_and_order_free=True,
            audit_dexes=catalog.wire_dexes,
            catalog=catalog,
        )
    _write(continuation_dir / "preflight.json", preflight)
    _write(continuation_dir / "catalog.json", catalog.to_payload())
    if preflight.get("passed") is not True:
        _write(
            continuation_dir / "summary.json",
            {
                "status": "preflight_failed",
                "passed": False,
                "mutated": False,
                "blockers": preflight.get("blockers", []),
            },
        )
        return 2

    market = PROOF_MARKETS["acc1"]
    stream = MarketStream(catalog=catalog, active_markets=(market,))
    ready = asyncio.Event()
    mux = WsPostMux(response_timeout_s=3.0)
    rows: list[dict[str, Any]] = []
    truths: list[dict[str, Any]] = []
    blockers: list[str] = []
    attempted: set[str] = set()
    journal_path = proof_dir.parent / "mainnet-proof-actions.sqlite3"

    async with connect(WS_URL, ping_interval=None, close_timeout=2) as action_socket:
        action_epoch = mux.attach(action_socket)
        action_tasks = [
            asyncio.create_task(mux.receive_loop(action_epoch)),
            asyncio.create_task(_heartbeat(action_socket)),
        ]
        try:
            with ActionJournal(journal_path) as journal, _locks(bound, default_runtime_lock_dir()):
                if journal.recovery_actions():
                    raise ContinuationError("stable journal has an unresolved action")
                report_slots = {item["slot"]: item for item in preflight["slots"]}
                lanes = {
                    slot.config.slot: _lane_factory()(
                        slot,
                        journal,
                        (
                            slot.config.follower_account_address
                            if report_slots[slot.config.slot]["identity"]["follower_role"]
                            == "subaccount"
                            else None
                        ),
                    )
                    for slot in bound.slots
                }

                async def truth(slot_id: str, phase: str) -> dict[str, Any]:
                    slot = slot_by_id[slot_id]
                    value = await _truth_reader(
                        slot,
                        mux.capture_epoch(),
                        mux=mux,
                        catalog=catalog,
                    )
                    _check_truth(value, slot, phase)
                    truths.append(_truth_payload(value, phase))
                    return value

                await truth("acc7", "continuation_baseline")
                await truth("acc1", "continuation_baseline")
                # Account truth can take multiple seconds across every DEX.  Open the
                # market socket only after that work so the strict 1s entry snapshot
                # cannot expire while unrelated baseline requests are running.
                async with connect(WS_URL, ping_interval=None, close_timeout=2) as market_socket:
                    market_epoch = stream.begin_connection(received_ms=_now_ms())
                    for subscription in stream.subscription_specs:
                        await market_socket.send(
                            json.dumps({"method": "subscribe", "subscription": subscription})
                        )
                    market_tasks = [
                        asyncio.create_task(
                            _market_pump(market_socket, stream, market_epoch, ready)
                        ),
                        asyncio.create_task(_heartbeat(market_socket)),
                    ]
                    try:
                        await asyncio.wait_for(ready.wait(), timeout=10)
                        await _trade_slot(
                            slot_by_id["acc1"],
                            lanes["acc1"],
                            stream,
                            mux,
                            rows,
                            continuation_dir / "ws-actions.jsonl",
                            attempted,
                            _now_ms,
                            str(_now_ms()),
                        )
                    finally:
                        for task in market_tasks:
                            task.cancel()
                        await asyncio.gather(*market_tasks, return_exceptions=True)
                await truth("acc1", "continuation_terminal")
                await truth("acc7", "continuation_terminal")
        except (Exception, asyncio.CancelledError) as exc:
            blockers.append(f"{type(exc).__name__}: {exc}")
            for slot_id in ("acc1", "acc7"):
                try:
                    slot = slot_by_id[slot_id]
                    value = await _truth_reader(
                        slot,
                        mux.capture_epoch(),
                        mux=mux,
                        catalog=catalog,
                    )
                    _check_truth(value, slot, "continuation_failure", require_flat=False)
                    truths.append(_truth_payload(value, "continuation_failure"))
                except Exception as truth_exc:
                    blockers.append(
                        f"{slot_id} truth unavailable: {type(truth_exc).__name__}: {truth_exc}"
                    )
        finally:
            for task in action_tasks:
                task.cancel()
            await asyncio.gather(*action_tasks, return_exceptions=True)

    passed = (
        not blockers
        and len(rows) == 2
        and {row["phase"] for row in rows}
        == {
            "entry",
            "close",
        }
    )
    summary = {
        "status": "passed" if passed else "recovery_required",
        "passed": passed,
        "mutated": bool(attempted),
        "action_count": len(rows),
        "rest_action_count": 0,
        "blockers": blockers,
        "truth": truths,
    }
    _write(continuation_dir / "summary.json", summary)
    print(
        json.dumps(
            {key: value for key, value in summary.items() if key != "truth"},
            sort_keys=True,
        )
    )
    return 0 if passed else 3


def main() -> None:
    parser = argparse.ArgumentParser(description="Continue the recovered proof with acc1 only")
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
