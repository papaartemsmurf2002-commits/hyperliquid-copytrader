from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from time import monotonic, time_ns

import websockets


DEFAULT_MARKETS = ("BTC", "CASHCAT", "xyz:KR200", "xyz:CXMT", "xyz:CL")


async def collect(*, output: Path, duration_s: float, markets: tuple[str, ...]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    deadline = monotonic() + duration_s
    counts: dict[str, int] = {}
    async with websockets.connect(
        "wss://api.hyperliquid.xyz/ws",
        open_timeout=20,
        ping_interval=None,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    ) as socket:
        with output.open("w", encoding="utf-8", buffering=1) as stream:
            for market in markets:
                for feed in ("l2Book", "bbo", "trades"):
                    await socket.send(
                        json.dumps(
                            {"method": "subscribe", "subscription": {"type": feed, "coin": market}},
                            separators=(",", ":"),
                        )
                    )
            last_ping = monotonic()
            while monotonic() < deadline:
                if monotonic() - last_ping >= 25:
                    await socket.send('{"method":"ping"}')
                    last_ping = monotonic()
                timeout = min(5.0, max(0.1, deadline - monotonic()))
                try:
                    raw = await asyncio.wait_for(socket.recv(), timeout=timeout)
                except TimeoutError:
                    continue
                message = json.loads(raw)
                channel = str(message.get("channel", "unknown"))
                counts[channel] = counts.get(channel, 0) + 1
                if channel not in {"l2Book", "bbo", "trades"}:
                    continue
                stream.write(
                    json.dumps(
                        {
                            "received_wall_ns": time_ns(),
                            "channel": channel,
                            "data": message.get("data"),
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                )
    print(json.dumps({"output": str(output), "counts": counts}, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Record a bounded read-only L2/trade sample.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-s", type=float, default=300.0)
    parser.add_argument("--market", action="append", dest="markets")
    args = parser.parse_args()
    markets = tuple(args.markets or DEFAULT_MARKETS)
    asyncio.run(collect(output=args.output, duration_s=args.duration_s, markets=markets))


if __name__ == "__main__":
    main()
