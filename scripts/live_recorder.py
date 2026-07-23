from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hyperliquid_copytrader.observer import (  # noqa: E402
    WEBSOCKET_CONNECTION_BANNER,
    source_websocket_subscriptions,
)
from hyperliquid_copytrader.websocket_transport import (  # noqa: E402
    connect_websocket_ipv6_preferred,
)


INFO_URL = "https://api.hyperliquid.xyz/info"
WS_URL = "wss://api.hyperliquid.xyz/ws"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
MS_PER_MINUTE = 60_000

SNAPSHOT_REQUESTS = [
    {"type": "clearinghouseState"},
    {"type": "openOrders"},
    {"type": "spotClearinghouseState"},
    {"type": "portfolio"},
    {"type": "userAbstraction"},
    {"type": "userDexAbstraction"},
]
LEAN_STREAM_TYPES = {
    "orderUpdates",
    "userEvents",
    "userFills",
    "userFundings",
    "userNonFundingLedgerUpdates",
    "userTwapSliceFills",
    "userTwapHistory",
    "twapStates",
}


@dataclass(frozen=True)
class ActiveCandidate:
    address: str
    recent_fills: int
    recent_coins: int
    account_value_usd: Decimal
    day_volume_usd: Decimal
    week_volume_usd: Decimal
    month_volume_usd: Decimal


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_stamp() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


def decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        parsed = Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def valid_address(value: str) -> bool:
    text = value.lower()
    return (
        len(text) == 42
        and text.startswith("0x")
        and all(char in "0123456789abcdef" for char in text[2:])
    )


def fetch_json(url: str, timeout_s: float = 20.0) -> Any:
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "hl-live-recorder/0.1"},
    )
    with urlopen(request, timeout=timeout_s) as response:  # nosec B310
        return json.loads(response.read().decode("utf-8"))


def post_info(payload: dict[str, Any], timeout_s: float = 15.0, retries: int = 4) -> Any:
    body = json.dumps(payload).encode("utf-8")
    request = Request(
        INFO_URL,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "hl-live-recorder/0.1"},
        method="POST",
    )
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout_s) as response:  # nosec B310
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt >= retries:
                raise
            delay = _retry_delay(exc.headers.get("Retry-After"), attempt)
        except URLError:
            if attempt >= retries:
                raise
            delay = 2 + attempt * 2
        time.sleep(delay)
    raise RuntimeError("unreachable retry state")


def _retry_delay(retry_after: str | None, attempt: int) -> float:
    if retry_after:
        try:
            return float(Decimal(retry_after))
        except InvalidOperation:
            pass
    return float(2 + attempt * 2)


def leaderboard_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("leaderboardRows") or payload.get("rows") or payload.get("data")
        return rows if isinstance(rows, list) else []
    return payload if isinstance(payload, list) else []


def window_performances(value: Any) -> dict[str, dict[str, Any]]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items() if isinstance(item, dict)}
    result: dict[str, dict[str, Any]] = {}
    if isinstance(value, list):
        for item in value:
            if isinstance(item, list) and len(item) == 2 and isinstance(item[1], dict):
                result[str(item[0])] = item[1]
    return result


def discover_active_addresses(
    *,
    max_addresses: int,
    candidate_count: int,
    lookback_minutes: int,
    include_addresses: list[str],
) -> list[ActiveCandidate]:
    payload = fetch_json(LEADERBOARD_URL)
    candidates: list[tuple[Decimal, str, Decimal, Decimal, Decimal, Decimal]] = []
    for raw in leaderboard_rows(payload):
        if not isinstance(raw, dict):
            continue
        address = str(raw.get("ethAddress") or raw.get("address") or "").lower()
        if not valid_address(address):
            continue
        performances = window_performances(raw.get("windowPerformances"))
        day = performances.get("day", {})
        week = performances.get("week", {})
        month = performances.get("month", {})
        account_value = decimal(raw.get("accountValue"))
        day_volume = decimal(day.get("vlm"))
        week_volume = decimal(week.get("vlm"))
        month_volume = decimal(month.get("vlm"))
        if account_value < Decimal("5000"):
            continue
        if day_volume < Decimal("25000") and week_volume < Decimal("250000"):
            continue
        score = day_volume * Decimal("8") + week_volume + month_volume / Decimal("10")
        candidates.append((score, address, account_value, day_volume, week_volume, month_volume))

    seen = {item[1] for item in candidates}
    for address in include_addresses:
        normalized = address.lower()
        if valid_address(normalized) and normalized not in seen:
            candidates.append(
                (Decimal("1"), normalized, Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0"))
            )
            seen.add(normalized)

    end_ms = now_ms()
    start_ms = end_ms - lookback_minutes * MS_PER_MINUTE
    scored: list[ActiveCandidate] = []
    for _score, address, account_value, day_volume, week_volume, month_volume in sorted(
        candidates, reverse=True
    )[:candidate_count]:
        fills = post_info(
            {
                "type": "userFillsByTime",
                "user": address,
                "startTime": start_ms,
                "endTime": end_ms,
                "aggregateByTime": False,
            }
        )
        recent_fills = len(fills) if isinstance(fills, list) else -1
        recent_coins = (
            len({str(fill.get("coin")) for fill in fills if isinstance(fill, dict)})
            if isinstance(fills, list)
            else 0
        )
        scored.append(
            ActiveCandidate(
                address=address,
                recent_fills=recent_fills,
                recent_coins=recent_coins,
                account_value_usd=account_value,
                day_volume_usd=day_volume,
                week_volume_usd=week_volume,
                month_volume_usd=month_volume,
            )
        )
        time.sleep(0.2)

    scored.sort(
        key=lambda item: (
            item.recent_fills,
            item.recent_coins,
            item.day_volume_usd,
            item.week_volume_usd,
        ),
        reverse=True,
    )
    return scored[:max_addresses]


def recorder_subscriptions(address: str, stream_profile: str) -> list[dict[str, Any]]:
    subscriptions = source_websocket_subscriptions(address, active_asset_symbols=())
    if stream_profile == "full":
        return subscriptions
    if stream_profile != "lean":
        raise ValueError(f"unsupported stream profile: {stream_profile}")
    return [
        subscription
        for subscription in subscriptions
        if subscription.get("type") in LEAN_STREAM_TYPES
    ]


class LiveRecorder:
    def __init__(
        self,
        *,
        addresses: list[str],
        out_dir: Path,
        snapshot_interval_s: int,
        idle_timeout_s: int,
        heartbeat_timeout_s: int,
        reconnect_backoff_s: int,
        duration_s: int | None,
        stream_profile: str,
    ) -> None:
        self.addresses = [address.lower() for address in addresses]
        self.out_dir = out_dir
        self.snapshot_interval_s = snapshot_interval_s
        self.idle_timeout_s = idle_timeout_s
        self.heartbeat_timeout_s = heartbeat_timeout_s
        self.reconnect_backoff_s = reconnect_backoff_s
        self.duration_s = duration_s
        self.stream_profile = stream_profile
        self.stop_event = asyncio.Event()
        self.started_ms = now_ms()
        self.counters: Counter[str] = Counter()

    async def run(self) -> None:
        self._prepare_dirs()
        self._write_manifest()
        tasks = [
            asyncio.create_task(self._record_address(address), name=f"ws:{address[:10]}")
            for address in self.addresses
        ]
        tasks.extend(
            asyncio.create_task(self._snapshot_address(address), name=f"snapshot:{address[:10]}")
            for address in self.addresses
        )
        tasks.append(asyncio.create_task(self._write_metrics_loop(), name="metrics"))
        if self.duration_s is not None:
            tasks.append(asyncio.create_task(self._stop_after_duration(), name="duration"))
        try:
            await self.stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._write_metrics(final=True)

    def _prepare_dirs(self) -> None:
        (self.out_dir / "events").mkdir(parents=True, exist_ok=True)
        (self.out_dir / "snapshots").mkdir(parents=True, exist_ok=True)

    def _write_manifest(self) -> None:
        subscriptions = {
            address: recorder_subscriptions(address, self.stream_profile)
            for address in self.addresses
        }
        manifest = {
            "started_ms": self.started_ms,
            "started_utc": datetime.fromtimestamp(self.started_ms / 1000, tz=UTC).isoformat(),
            "ws_url": WS_URL,
            "info_url": INFO_URL,
            "addresses": self.addresses,
            "address_count": len(self.addresses),
            "snapshot_interval_s": self.snapshot_interval_s,
            "idle_timeout_s": self.idle_timeout_s,
            "heartbeat_timeout_s": self.heartbeat_timeout_s,
            "reconnect_backoff_s": self.reconnect_backoff_s,
            "duration_s": self.duration_s,
            "stream_profile": self.stream_profile,
            "snapshot_requests": SNAPSHOT_REQUESTS,
            "subscriptions": subscriptions,
        }
        (self.out_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    async def _record_address(self, address: str) -> None:
        event_path = self.out_dir / "events" / f"{address}.jsonl"
        subscriptions = recorder_subscriptions(address, self.stream_profile)
        while not self.stop_event.is_set():
            try:
                await self._record_address_once(address, event_path, subscriptions)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.counters[f"{address}:ws_errors"] += 1
                self._append_jsonl(
                    event_path,
                    {
                        "received_ms": now_ms(),
                        "address": address,
                        "kind": "control",
                        "event": "websocket_error",
                        "error": repr(exc),
                    },
                )
                await asyncio.sleep(self.reconnect_backoff_s)

    async def _record_address_once(
        self,
        address: str,
        event_path: Path,
        subscriptions: list[dict[str, Any]],
    ) -> None:
        async with connect_websocket_ipv6_preferred(WS_URL, ping_interval=None) as ws:
            self.counters[f"{address}:ws_connects"] += 1
            self._append_jsonl(
                event_path,
                {
                    "received_ms": now_ms(),
                    "address": address,
                    "kind": "control",
                    "event": "connected",
                    "subscription_count": len(subscriptions),
                },
            )
            for subscription in subscriptions:
                await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
                self.counters[f"{address}:subscriptions_sent"] += 1
            while not self.stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout_s)
                except TimeoutError:
                    await ws.send(json.dumps({"method": "ping"}))
                    self.counters[f"{address}:pings_sent"] += 1
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.heartbeat_timeout_s)
                if raw == WEBSOCKET_CONNECTION_BANNER:
                    continue
                received = now_ms()
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    message = {"raw": raw, "parse_error": True}
                channel = (
                    str(message.get("channel") or "unknown")
                    if isinstance(message, dict)
                    else "unknown"
                )
                if channel == "pong":
                    self.counters[f"{address}:pongs"] += 1
                    continue
                self.counters["messages"] += 1
                self.counters[f"channel:{channel}"] += 1
                self.counters[f"{address}:messages"] += 1
                self._append_jsonl(
                    event_path,
                    {
                        "received_ms": received,
                        "address": address,
                        "kind": "websocket",
                        "channel": channel,
                        "message": message,
                    },
                )

    async def _snapshot_address(self, address: str) -> None:
        snapshot_path = self.out_dir / "snapshots" / f"{address}.jsonl"
        while not self.stop_event.is_set():
            started = now_ms()
            results: dict[str, Any] = {}
            for template in SNAPSHOT_REQUESTS:
                payload = {**template, "user": address}
                request_type = str(payload["type"])
                try:
                    results[request_type] = {
                        "ok": True,
                        "payload": await asyncio.to_thread(post_info, payload),
                    }
                    self.counters[f"snapshot:{request_type}:ok"] += 1
                except Exception as exc:
                    results[request_type] = {"ok": False, "error": repr(exc)}
                    self.counters[f"snapshot:{request_type}:error"] += 1
                await asyncio.sleep(0.1)
            self.counters[f"{address}:snapshots"] += 1
            self._append_jsonl(
                snapshot_path,
                {
                    "received_ms": now_ms(),
                    "started_ms": started,
                    "address": address,
                    "kind": "rest_snapshot",
                    "results": results,
                },
            )
            await asyncio.sleep(self.snapshot_interval_s)

    async def _write_metrics_loop(self) -> None:
        while not self.stop_event.is_set():
            self._write_metrics(final=False)
            await asyncio.sleep(30)

    async def _stop_after_duration(self) -> None:
        assert self.duration_s is not None
        await asyncio.sleep(self.duration_s)
        self.stop_event.set()

    def _write_metrics(self, *, final: bool) -> None:
        payload = {
            "final": final,
            "updated_ms": now_ms(),
            "started_ms": self.started_ms,
            "uptime_s": max(0, (now_ms() - self.started_ms) // 1000),
            "addresses": self.addresses,
            "counters": dict(sorted(self.counters.items())),
        }
        (self.out_dir / "metrics.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    @staticmethod
    def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
            handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only Hyperliquid multi-account websocket recorder."
    )
    parser.add_argument(
        "--address", action="append", default=[], help="Address to record. Can be repeated."
    )
    parser.add_argument(
        "--discover-active", action="store_true", help="Discover active leaderboard accounts."
    )
    parser.add_argument("--max-addresses", type=int, default=7)
    parser.add_argument("--discovery-candidates", type=int, default=24)
    parser.add_argument("--discovery-lookback-min", type=int, default=120)
    parser.add_argument("--snapshot-interval-s", type=int, default=180)
    parser.add_argument("--idle-timeout-s", type=int, default=55)
    parser.add_argument("--heartbeat-timeout-s", type=int, default=5)
    parser.add_argument("--reconnect-backoff-s", type=int, default=5)
    parser.add_argument("--duration-min", type=int, default=0, help="0 means run until stopped.")
    parser.add_argument("--stream-profile", choices=["lean", "full"], default="lean")
    parser.add_argument("--out-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    include_addresses = [
        address.lower() for address in args.address if valid_address(address.lower())
    ]
    if len(include_addresses) != len(args.address):
        raise SystemExit("one or more --address values are invalid")
    if args.max_addresses <= 0 or args.max_addresses > 10:
        raise SystemExit("--max-addresses must be between 1 and 10")
    if args.snapshot_interval_s < 30:
        raise SystemExit("--snapshot-interval-s must be at least 30")

    if args.discover_active:
        selected = discover_active_addresses(
            max_addresses=args.max_addresses,
            candidate_count=args.discovery_candidates,
            lookback_minutes=args.discovery_lookback_min,
            include_addresses=include_addresses,
        )
        addresses = [item.address for item in selected]
        selection = [item.__dict__ for item in selected]
    else:
        addresses = include_addresses[: args.max_addresses]
        selection = [{"address": address, "manual": True} for address in addresses]
    if not addresses:
        raise SystemExit("no addresses selected; pass --address or --discover-active")

    out_dir = args.out_dir or ROOT / "data" / "live_recordings" / f"recording_{utc_stamp()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "selection.json").write_text(
        json.dumps(selection, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    duration_s = args.duration_min * 60 if args.duration_min > 0 else None
    recorder = LiveRecorder(
        addresses=addresses,
        out_dir=out_dir,
        snapshot_interval_s=args.snapshot_interval_s,
        idle_timeout_s=args.idle_timeout_s,
        heartbeat_timeout_s=args.heartbeat_timeout_s,
        reconnect_backoff_s=args.reconnect_backoff_s,
        duration_s=duration_s,
        stream_profile=args.stream_profile,
    )
    print(json.dumps({"out_dir": str(out_dir), "addresses": addresses}, indent=2), flush=True)
    try:
        asyncio.run(recorder.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
