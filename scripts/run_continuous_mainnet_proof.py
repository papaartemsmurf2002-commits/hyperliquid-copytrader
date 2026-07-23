from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from time import time_ns
from typing import Any, Mapping

import httpx
from websockets.asyncio.client import connect

from hyperliquid_copytrader.continuous_canary import (
    MAINNET_PROOF_ACKNOWLEDGEMENT,
    PROOF_MARKETS,
    TerminalTruth,
    run_two_account_ws_proof,
)
from hyperliquid_copytrader.continuous_config import (
    BoundContinuousSlot,
    bind_continuous_plan,
    load_continuous_plan,
)
from hyperliquid_copytrader.continuous_preflight import run_continuous_preflight
from hyperliquid_copytrader.market_catalog import (
    CatalogRevision,
    MarketReadiness,
    build_dynamic_catalog_revision,
)
from hyperliquid_copytrader.market_stream import MarketStream
from hyperliquid_copytrader.models import Position, parse_decimal
from hyperliquid_copytrader.observer import parse_clearinghouse_positions
from hyperliquid_copytrader.unified_account import (
    HyperliquidUserAbstraction,
    classify_user_abstraction,
)
from hyperliquid_copytrader.ws_actions import PostOutcome, WsPostMux


REST_URL = "https://api.hyperliquid.xyz"
WS_URL = "wss://api.hyperliquid.xyz/ws"


def _now_ms() -> int:
    return time_ns() // 1_000_000


class _HttpInfo:
    def __init__(self) -> None:
        self.client = httpx.Client(base_url=REST_URL, timeout=10.0)

    def __enter__(self) -> _HttpInfo:
        return self

    def __exit__(self, *_: object) -> None:
        self.client.close()

    def __call__(self, payload: dict[str, Any]) -> Any:
        response = self.client.post("/info", json=payload)
        response.raise_for_status()
        return response.json()


def _catalog(info: _HttpInfo) -> CatalogRevision:
    observed = _now_ms()
    base = build_dynamic_catalog_revision(
        network="mainnet",
        policy_version="continuous-v1-proof",
        sequence=1,
        observed_ms=observed,
        dexes_before_payload=info({"type": "perpDexs"}),
        all_perp_metas_payload=info({"type": "allPerpMetas"}),
        dexes_after_payload=info({"type": "perpDexs"}),
    )
    targets = set(PROOF_MARKETS.values())
    missing = sorted(targets - {market.symbol for market in base.markets})
    if missing:
        raise RuntimeError("proof markets are absent from catalog: " + ",".join(missing))
    markets = tuple(
        replace(
            market,
            readiness=MarketReadiness.READY,
            context_observed_ms=observed,
        )
        if market.symbol in targets and not market.is_delisted
        else market
        for market in base.markets
    )
    return replace(
        base,
        revision_id=base.revision_id + "-proof",
        observed_ms=observed,
        markets=markets,
    )


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
    targets = set(PROOF_MARKETS.values())
    async for raw in socket:
        try:
            message = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(message, Mapping):
                continue
            if message.get("channel") in {"activeAssetCtx", "l2Book"}:
                stream.apply(message, epoch=epoch, received_ms=_now_ms())
                now = _now_ms()
                if all(
                    stream.fresh_snapshot(market, now_ms=now, max_age_ms=1_000) is not None
                    for market in targets
                ):
                    ready.set()
        except (TypeError, ValueError):
            continue


async def _info_data(
    mux: WsPostMux,
    epoch: int,
    payload: Mapping[str, Any],
) -> Any:
    result = await mux.post_info(payload, required_epoch=epoch, timeout_s=3.0)
    if result.outcome is not PostOutcome.INFO:
        raise RuntimeError(f"WS info {payload.get('type')} failed: {result.reason}")
    response = result.response
    if not isinstance(response, Mapping) or response.get("type") != "info":
        raise RuntimeError(f"WS info {payload.get('type')} returned a malformed envelope")
    inner = response.get("payload")
    if not isinstance(inner, Mapping) or inner.get("type") != payload.get("type"):
        raise RuntimeError(f"WS info {payload.get('type')} returned a mismatched payload")
    return inner.get("data")


def _spot_collateral(payload: Any) -> tuple[Decimal, Decimal]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("balances"), list):
        raise RuntimeError("follower Spot collateral is malformed")
    token_zero = [
        row
        for row in payload["balances"]
        if isinstance(row, Mapping) and str(row.get("token")) == "0"
    ]
    if len(token_zero) != 1 or str(token_zero[0].get("coin")).upper() != "USDC":
        raise RuntimeError("follower does not have exactly one token-0 USDC balance")
    total = parse_decimal(token_zero[0].get("total"))
    hold = parse_decimal(token_zero[0].get("hold", "0"))
    if total < 0 or hold < 0 or hold > total:
        raise RuntimeError("follower token-0 USDC balance is inconsistent")
    return total, total - hold


async def _truth_reader(
    slot: BoundContinuousSlot,
    epoch: int,
    *,
    mux: WsPostMux,
    catalog: CatalogRevision,
) -> TerminalTruth:
    follower = slot.config.follower_account_address
    specs: list[dict[str, Any]] = [
        {"type": "userAbstraction", "user": follower},
        {"type": "spotClearinghouseState", "user": follower},
    ]
    for dex in catalog.wire_dexes:
        suffix = {} if not dex else {"dex": dex}
        specs.append({"type": "clearinghouseState", "user": follower, **suffix})
        specs.append({"type": "openOrders", "user": follower, **suffix})
    values = await asyncio.gather(*(_info_data(mux, epoch, spec) for spec in specs))
    mode = classify_user_abstraction(values[0])
    if mode is not HyperliquidUserAbstraction.UNIFIED:
        raise RuntimeError("proof follower is no longer Unified")
    _total, available = _spot_collateral(values[1])
    positions: dict[str, Position] = {}
    open_order_count = 0
    index = 2
    observed = _now_ms()
    for dex in catalog.wire_dexes:
        state, orders = values[index], values[index + 1]
        index += 2
        if not isinstance(state, Mapping):
            raise RuntimeError(f"follower {dex or 'native'} state is malformed")
        for market, position in parse_clearinghouse_positions(
            dict(state),
            observed_ms=observed,
            dex=dex,
        ).items():
            if market in positions:
                raise RuntimeError(f"duplicate follower market across DEXes: {market}")
            if position.size:
                positions[market] = position
        if not isinstance(orders, list) or not all(isinstance(row, Mapping) for row in orders):
            raise RuntimeError(f"follower {dex or 'native'} open orders are malformed")
        open_order_count += len(orders)
    return TerminalTruth(
        slot=slot.config.slot,
        observed_ms=observed,
        positions=tuple((market, position.size) for market, position in sorted(positions.items())),
        open_order_count=open_order_count,
        account_mode="unified",
        available_collateral_usd=available,
        transport="ws_post",
    )


async def _run(
    repo_root: Path,
    plan_path: Path,
    acknowledgement: str,
    *,
    preflight_only: bool = False,
    preflight_output: Path | None = None,
) -> int:
    if not preflight_only and acknowledgement != MAINNET_PROOF_ACKNOWLEDGEMENT:
        print(json.dumps({"status": "acknowledgement_failed"}))
        return 2
    plan = load_continuous_plan(plan_path)
    bound = bind_continuous_plan(plan, repo_root=repo_root, verify_secrets=True)
    with _HttpInfo() as info:
        catalog = _catalog(info)
        preflight = run_continuous_preflight(
            bound,
            network="mainnet",
            info=info,
            observed_ms=_now_ms(),
            require_flat_and_order_free=True,
            audit_dexes=catalog.wire_dexes,
            catalog=catalog,
        )
    if preflight_output is not None:
        preflight_output.parent.mkdir(parents=True, exist_ok=True)
        preflight_output.write_text(
            json.dumps(preflight, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if not preflight["passed"]:
        print(
            json.dumps(
                {
                    "status": "preflight_failed",
                    "blockers": preflight["blockers"],
                    "preflight_output": (
                        None if preflight_output is None else str(preflight_output)
                    ),
                },
                sort_keys=True,
            )
        )
        return 2
    if preflight_only:
        print(
            json.dumps(
                {
                    "status": "preflight_passed",
                    "passed": True,
                    "mutated": False,
                    "rest_requests": preflight.get("rest_requests"),
                    "preflight_output": (
                        None if preflight_output is None else str(preflight_output)
                    ),
                },
                sort_keys=True,
            )
        )
        return 0

    market_stream = MarketStream(catalog=catalog, active_markets=PROOF_MARKETS.values())
    market_ready = asyncio.Event()
    mux = WsPostMux(response_timeout_s=3.0)
    async with (
        connect(WS_URL, ping_interval=None, close_timeout=2) as market_socket,
        connect(WS_URL, ping_interval=None, close_timeout=2) as action_socket,
    ):
        market_epoch = market_stream.begin_connection(received_ms=_now_ms())
        for subscription in market_stream.subscription_specs:
            await market_socket.send(
                json.dumps({"method": "subscribe", "subscription": subscription})
            )
        action_epoch = mux.attach(action_socket)
        tasks = [
            asyncio.create_task(
                _market_pump(market_socket, market_stream, market_epoch, market_ready)
            ),
            asyncio.create_task(mux.receive_loop(action_epoch)),
            asyncio.create_task(_heartbeat(market_socket)),
            asyncio.create_task(_heartbeat(action_socket)),
        ]
        try:
            await asyncio.wait_for(market_ready.wait(), timeout=10)

            async def truth(slot: BoundContinuousSlot, epoch: int) -> TerminalTruth:
                return await _truth_reader(slot, epoch, mux=mux, catalog=catalog)

            result = await run_two_account_ws_proof(
                bound,
                acknowledgement=acknowledgement,
                preflight_report=preflight,
                catalog=catalog,
                market_stream=market_stream,
                mux=mux,
                required_epoch=action_epoch,
                truth_reader=truth,
            )
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    output = {
        "status": result.status,
        "passed": result.passed,
        "mutated": result.mutated,
        "recovery_required": result.recovery_required,
        "action_count": result.action_count,
        "proof_dir": str(result.proof_dir),
        "blockers": result.blockers,
    }
    print(json.dumps(output, sort_keys=True))
    return 0 if result.passed else 3


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the bounded two-account WS mainnet proof")
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path(".secrets/mainnet-two-account-continuous-proof.json"),
    )
    parser.add_argument("--acknowledgement", default="")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="run strict read-only mainnet identity/state validation without opening action sockets",
    )
    parser.add_argument(
        "--preflight-output",
        type=Path,
        help="optional path for the redacted read-only preflight report",
    )
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    raise SystemExit(
        asyncio.run(
            _run(
                repo_root,
                args.plan.resolve(),
                args.acknowledgement,
                preflight_only=args.preflight_only,
                preflight_output=(
                    None if args.preflight_output is None else args.preflight_output.resolve()
                ),
            )
        )
    )


if __name__ == "__main__":
    main()
