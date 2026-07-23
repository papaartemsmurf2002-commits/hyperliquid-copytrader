from __future__ import annotations

import argparse
import asyncio
import json
import socket
import time
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Sequence
from typing import Any

from hyperliquid_copytrader.websocket_transport import connect_websocket_ipv6_preferred
from hyperliquid_copytrader.ws_actions import PostOutcome, WsPostMux


ENDPOINTS = {
    "mainnet": ("wss://api.hyperliquid.xyz/ws", "https://api.hyperliquid.xyz/info"),
    "testnet": ("wss://api.hyperliquid-testnet.xyz/ws", "https://api.hyperliquid-testnet.xyz/info"),
}


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _summary(
    values: Sequence[float],
    *,
    attempts: int | None = None,
    failure_classes: Counter[str] | None = None,
) -> dict[str, Any]:
    total = len(values) if attempts is None else attempts
    failures = max(0, total - len(values))
    result: dict[str, Any] = {
        "attempts": total,
        "samples": len(values),
        "failures": failures,
        "success_rate": round(len(values) / total, 4) if total else None,
        "minimum_ms": round(min(values), 3) if values else None,
        "median_ms": _rounded(_percentile(values, 0.50)),
        "p95_ms": _rounded(_percentile(values, 0.95)) if len(values) >= 20 else None,
        "p99_ms": _rounded(_percentile(values, 0.99)) if len(values) >= 100 else None,
        "maximum_ms": round(max(values), 3) if values else None,
    }
    if failure_classes:
        result["failure_classes"] = dict(sorted(failure_classes.items()))
    return result


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 3)


def _rest_all_mids(url: str, timeout_s: float) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps({"type": "allMids"}, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "hlct-read-only-probe/1"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_s) as response:  # noqa: S310
        payload = json.loads(response.read())
    if not isinstance(payload, dict) or not payload:
        raise RuntimeError("REST allMids returned no market data")


async def _measure(
    *,
    network: str,
    samples: int,
    timeout_s: float,
    include_rest: bool,
) -> dict[str, Any]:
    ws_url, rest_url = ENDPOINTS[network]
    host = urllib.parse.urlparse(rest_url).hostname
    if not host:
        raise RuntimeError("could not determine endpoint host")

    dns_started = time.perf_counter_ns()
    addresses = await asyncio.to_thread(socket.getaddrinfo, host, 443, type=socket.SOCK_STREAM)
    dns_ms = (time.perf_counter_ns() - dns_started) / 1_000_000

    opened = time.perf_counter_ns()
    async with connect_websocket_ipv6_preferred(
        ws_url,
        proxy=None,
        ping_interval=None,
        open_timeout=timeout_s,
        close_timeout=2,
        max_queue=128,
    ) as websocket:
        websocket_open_ms = (time.perf_counter_ns() - opened) / 1_000_000
        mux = WsPostMux(response_timeout_s=timeout_s, write_timeout_s=timeout_s)
        epoch = mux.attach(websocket)
        receiver = asyncio.create_task(mux.receive_loop(epoch))
        ping_ms: list[float] = []
        info_ms: list[float] = []
        ping_failures: Counter[str] = Counter()
        info_failures: Counter[str] = Counter()
        try:
            for _ in range(samples):
                started = time.perf_counter_ns()
                try:
                    pong = await websocket.ping()
                    await asyncio.wait_for(pong, timeout=timeout_s)
                    ping_ms.append((time.perf_counter_ns() - started) / 1_000_000)
                except Exception as exc:  # diagnostic boundary: report, never retry
                    ping_failures[type(exc).__name__] += 1

                started = time.perf_counter_ns()
                try:
                    result = await mux.post_info(
                        {"type": "allMids"},
                        required_epoch=epoch,
                        timeout_s=timeout_s,
                    )
                    if result.outcome is PostOutcome.INFO:
                        info_ms.append((time.perf_counter_ns() - started) / 1_000_000)
                    else:
                        info_failures[f"outcome:{result.outcome.value}"] += 1
                except Exception as exc:  # diagnostic boundary: report, never retry
                    info_failures[type(exc).__name__] += 1
        finally:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)

    rest_ms: list[float] = []
    rest_failures: Counter[str] = Counter()
    if include_rest:
        for _ in range(samples):
            started = time.perf_counter_ns()
            try:
                await asyncio.to_thread(_rest_all_mids, rest_url, timeout_s)
                rest_ms.append((time.perf_counter_ns() - started) / 1_000_000)
            except Exception as exc:  # diagnostic boundary: report, never retry
                rest_failures[type(exc).__name__] += 1

    return {
        "version": 1,
        "network": network,
        "read_only": True,
        "method": {
            "dns": "socket.getaddrinfo; operating-system cache may apply",
            "websocket_open": "DNS/TCP/TLS/WebSocket upgrade on a fresh direct connection",
            "websocket_ping": "protocol ping to pong on one persistent connection",
            "websocket_info": "read-only allMids WS POST on the same persistent connection",
            "rest_info": "read-only allMids HTTPS POST using a fresh urlopen call",
            "orders_submitted": 0,
        },
        "resolved_address_count": len(addresses),
        "dns_ms": round(dns_ms, 3),
        "websocket_open_ms": round(websocket_open_ms, 3),
        "websocket_ping": _summary(ping_ms, attempts=samples, failure_classes=ping_failures),
        "websocket_info": _summary(info_ms, attempts=samples, failure_classes=info_failures),
        "rest_info": _summary(
            rest_ms,
            attempts=samples if include_rest else 0,
            failure_classes=rest_failures,
        ),
        "interpretation": (
            "These values isolate network and read-only API latency. They do not measure "
            "signed-order commit, matching, or follower fill latency."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Measure Hyperliquid network/read latency without signing or trading."
    )
    parser.add_argument("--network", choices=sorted(ENDPOINTS), default="mainnet")
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--skip-rest", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.samples <= 500:
        parser.error("--samples must be between 1 and 500")
    if not 0 < args.timeout_s <= 30:
        parser.error("--timeout-s must be between 0 and 30")
    payload = asyncio.run(
        _measure(
            network=args.network,
            samples=args.samples,
            timeout_s=args.timeout_s,
            include_rest=not args.skip_rest,
        )
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
