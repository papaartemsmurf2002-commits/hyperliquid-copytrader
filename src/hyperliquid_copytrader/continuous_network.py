from __future__ import annotations

import asyncio
import json
import math
import sqlite3
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any, Protocol

from .account_stream import AccountStream, FillRecord
from .continuous_config import BoundContinuousSlot
from .continuous_runtime import ContinuousRuntime, Dispatch
from .market_stream import MarketSubscriptionChange
from .markets import market_dex
from .ws_actions import PostOutcome, WsPostMux
from .websocket_transport import connect_websocket_ipv6_preferred


class ContinuousNetworkError(RuntimeError):
    """A bounded continuous transport could not establish trustworthy progress."""


class FatalContinuousNetworkError(ContinuousNetworkError):
    """A non-retryable subscription or protocol contract is invalid."""


class _Socket(Protocol):
    def __aiter__(self) -> AsyncIterator[Any]: ...

    async def send(self, message: str) -> Any: ...


class _Connection(Protocol):
    async def __aenter__(self) -> _Socket: ...

    async def __aexit__(self, *exc: object) -> Any: ...


Connector = Callable[[str, str], _Connection]
MetricSink = Callable[[Mapping[str, Any]], None]


class FillRecoveryFallback(Protocol):
    async def __call__(
        self, *, user: str, start_ms: int, end_ms: int
    ) -> Sequence[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    attempts: int | None = None
    minimum_delay_s: float = 0.25
    maximum_delay_s: float = 5.0
    jitter_fraction: float = 0.20
    heartbeat_s: float = 30.0
    heartbeat_timeout_s: float = 5.0
    write_timeout_s: float = 2.0
    reconciliation_s: float = 60.0
    reconciliation_stagger_s: float = 0.25
    source_fail_close_s: float = 60.0
    stable_connection_s: float = 30.0
    connection_attempts_per_minute: int = 24

    def __post_init__(self) -> None:
        if self.attempts is not None and self.attempts < 1:
            raise ValueError("reconnect attempts must be positive")
        if not 0 < self.minimum_delay_s <= self.maximum_delay_s:
            raise ValueError("reconnect delays must be positive and ordered")
        if not 0 <= self.jitter_fraction <= 1:
            raise ValueError("reconnect jitter must be between zero and one")
        if (
            not 0 < self.heartbeat_s < 60
            or not 0 < self.heartbeat_timeout_s
            or not 0 < self.write_timeout_s
        ):
            raise ValueError(
                "application heartbeat must be below 60 seconds and I/O timeouts positive"
            )
        if self.heartbeat_s + self.write_timeout_s + self.heartbeat_timeout_s >= 60:
            raise ValueError("application heartbeat failure must be known below 60 seconds")
        if (
            self.reconciliation_s <= 0
            or self.reconciliation_stagger_s < 0
            or self.source_fail_close_s <= 0
            or self.stable_connection_s <= 0
        ):
            raise ValueError("reconciliation and stable-connection intervals must be positive")
        if not 1 <= self.connection_attempts_per_minute <= 29:
            raise ValueError("shared WebSocket connection attempts must be between 1 and 29")


class DurableSourceGapRepair:
    """Bounded source-fill recovery over WS POST with a durable overlap cursor.

    The driver records only frames accepted by ``AccountStream``.  On a gap,
    ``userFillsByTime`` is queried over a small frozen window and durable fill
    identities remove overlap.  The returned chain must connect the persisted
    pre-gap position to the reconnect snapshot exactly; otherwise the runtime
    remains blocked.  An injected fallback may use HTTP, but is never required
    or used on the normal live path.
    """

    def __init__(
        self,
        *,
        mux: WsPostMux,
        path: Path | str,
        fallback: FillRecoveryFallback | None = None,
        clock_ms: Callable[[], int] | None = None,
        overlap_ms: int = 3_000,
        maximum_window_ms: int = 86_400_000,
        maximum_pages: int = 3,
        page_size: int = 2_000,
        identity_capacity: int = 100_000,
    ) -> None:
        if min(overlap_ms, maximum_window_ms, maximum_pages, page_size, identity_capacity) < 1:
            raise ValueError("gap-repair bounds must be positive")
        if overlap_ms >= maximum_window_ms:
            raise ValueError("gap-repair overlap must be smaller than its maximum window")
        self.mux, self.fallback = mux, fallback
        self.clock_ms = clock_ms or (lambda: time_ns() // 1_000_000)
        self.overlap_ms, self.maximum_window_ms = overlap_ms, maximum_window_ms
        self.maximum_pages, self.page_size = maximum_pages, page_size
        self.identity_capacity = identity_capacity
        self._snapshot_end_ms: dict[str, dict[str, int]] = {}
        self._pending: dict[str, list[tuple[int, list[Mapping[str, Any]]]]] = {}
        self._pending_recovered: dict[str, tuple[FillRecord, ...]] = {}
        target = Path(path).resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(target, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS source_cursor ("
            "source TEXT PRIMARY KEY,last_good_ms INTEGER NOT NULL,gap_ms INTEGER)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS source_fill_identity ("
            "source TEXT NOT NULL,tx_hash TEXT NOT NULL,tid TEXT NOT NULL,time_ms INTEGER NOT NULL,"
            "PRIMARY KEY(source,tx_hash,tid))"
        )

    def close(self) -> None:
        self.db.close()

    def begin_connection(self, sources: Sequence[str]) -> None:
        for source in sources:
            canonical = source.lower()
            self._snapshot_end_ms.pop(canonical, None)
            self._pending.pop(canonical, None)
            self._pending_recovered.pop(canonical, None)

    def stage_source_frame(self, message: Mapping[str, Any]) -> None:
        """Capture the exact position-snapshot horizon before runtime reduction.

        This is deliberately in-memory: a malformed/unaccepted reconnect frame
        must never advance the durable cursor.  ``record_accepted`` below is
        the only persistence boundary.
        """

        if message.get("channel") != "allDexsClearinghouseState":
            return
        data = message.get("data")
        if not isinstance(data, Mapping):
            return
        source = str(data.get("user") or data.get("userAddress") or "").lower()
        states = data.get("clearinghouseStates")
        entries = (
            states.items()
            if isinstance(states, Mapping)
            else (
                ((item[0], item[1]) for item in states if isinstance(item, list) and len(item) == 2)
                if isinstance(states, list)
                else ()
            )
        )
        horizons: dict[str, int] = {}
        try:
            for raw_dex, state in entries:
                if isinstance(raw_dex, str) and isinstance(state, Mapping):
                    observed = int(str(state.get("time")))
                    if observed > 0:
                        horizons[raw_dex.strip()] = observed
        except (TypeError, ValueError):
            return
        if source and horizons:
            self._snapshot_end_ms[source] = horizons

    def mark_gap(self, sources: Sequence[str], *, when_ms: int) -> None:
        if when_ms <= 0:
            raise ValueError("gap timestamp must be positive")
        for raw in sources:
            source = raw.lower()
            self.db.execute(
                "INSERT INTO source_cursor(source,last_good_ms,gap_ms) VALUES(?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET gap_ms=COALESCE(gap_ms,excluded.gap_ms)",
                (source, 0, when_ms),
            )

    def record_accepted(
        self,
        *,
        source: str,
        message: Mapping[str, Any],
        received_ms: int,
        source_ready: bool,
    ) -> None:
        source = source.lower()
        rows = _frame_fill_rows(message)
        pending = self._pending.setdefault(source, [])
        pending.append((received_ms, rows))
        if len(pending) > 256:
            raise ContinuousNetworkError("untrusted source baseline exceeded 256 frames")
        if not source_ready:
            return
        commit = self._pending.pop(source)
        newest_received = max(item[0] for item in commit)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO source_cursor(source,last_good_ms,gap_ms) VALUES(?,?,NULL) "
                "ON CONFLICT(source) DO UPDATE SET "
                "last_good_ms=MAX(last_good_ms,excluded.last_good_ms),"
                "gap_ms=CASE WHEN ? THEN NULL ELSE gap_ms END",
                (source, newest_received, 1),
            )
            for _frame_ms, frame_rows in commit:
                for row in frame_rows:
                    tx_hash, tid, fill_ms = _raw_fill_identity(row)
                    self.db.execute(
                        "INSERT OR IGNORE INTO source_fill_identity(source,tx_hash,tid,time_ms) "
                        "VALUES(?,?,?,?)",
                        (source, tx_hash, tid, fill_ms),
                    )
            for fill in self._pending_recovered.pop(source, ()):
                self.db.execute(
                    "INSERT OR IGNORE INTO source_fill_identity(source,tx_hash,tid,time_ms) "
                    "VALUES(?,?,?,?)",
                    (source, fill.tx_hash, fill.tid, fill.time_ms),
                )
            count = int(
                self.db.execute(
                    "SELECT COUNT(*) FROM source_fill_identity WHERE source=?", (source,)
                ).fetchone()[0]
            )
            if count > self.identity_capacity:
                remove = count - self.identity_capacity
                self.db.execute(
                    "DELETE FROM source_fill_identity WHERE rowid IN ("
                    "SELECT rowid FROM source_fill_identity WHERE source=? "
                    "ORDER BY time_ms,tx_hash,tid LIMIT ?)",
                    (source, remove),
                )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    async def __call__(
        self,
        *,
        slot: BoundContinuousSlot,
        before: Mapping[str, Decimal],
        after: Mapping[str, Decimal],
    ) -> tuple[FillRecord, ...]:
        source = slot.config.source_address.lower()
        now_ms = self.clock_ms()
        horizons = self._snapshot_end_ms.get(source)
        changed_dexes = {
            market_dex(market)
            for market in set(before) | set(after)
            if before.get(market, Decimal(0)) != after.get(market, Decimal(0))
        }
        if not horizons or not changed_dexes or not changed_dexes.issubset(horizons):
            raise ContinuousNetworkError("gap repair lacks the reconnect position horizon")
        end_ms = max(horizons[dex] for dex in changed_dexes)
        row = self.db.execute(
            "SELECT last_good_ms,gap_ms FROM source_cursor WHERE source=?", (source,)
        ).fetchone()
        if row is None:
            raise ContinuousNetworkError("gap repair has no durable source cursor")
        last_good_ms = int(row[0])
        anchor = last_good_ms - self.overlap_ms if last_good_ms > 0 else 1
        start_ms = max(1, anchor, end_ms - self.maximum_window_ms)
        if start_ms >= end_ms or end_ms > now_ms:
            raise ContinuousNetworkError("gap-repair window is empty or in the future")
        rows, used_fallback = await self._query_rows(
            source=source, start_ms=start_ms, end_ms=end_ms
        )

        def records(payload: Sequence[Mapping[str, Any]]) -> tuple[FillRecord, ...]:
            parser = AccountStream(
                source=source,
                follower=slot.config.follower_account_address,
                # Dynamic plans deliberately have no explicit allowlist.  Gap
                # repair must reconstruct every market in the authoritative
                # before/after position states, including newly listed and
                # HIP-3 markets, rather than filtering all recovered fills.
                source_markets=(
                    None
                    if getattr(slot, "dynamic_market_eligibility", False)
                    else slot.config.allowed_markets
                ),
            )
            epoch = parser.begin_connection(received_ms=now_ms)
            parser.apply(
                {
                    "channel": "userFills",
                    "data": {"user": source, "isSnapshot": True, "fills": list(payload)},
                },
                epoch=epoch,
                received_ms=now_ms,
            )
            return parser.source.fills

        known = {
            (str(item[0]), str(item[1]))
            for item in self.db.execute(
                "SELECT tx_hash,tid FROM source_fill_identity WHERE source=?", (source,)
            )
        }
        try:
            chain = _connect_fill_chain(records(rows), known=known, before=before, after=after)
        except ContinuousNetworkError:
            if self.fallback is None or used_fallback:
                raise
            fallback_rows = await self._fallback_rows(
                source=source, start_ms=start_ms, end_ms=end_ms
            )
            chain = _connect_fill_chain(
                records(fallback_rows), known=known, before=before, after=after
            )
        self._pending_recovered[source] = chain
        return chain

    async def _query_rows(
        self, *, source: str, start_ms: int, end_ms: int
    ) -> tuple[list[Mapping[str, Any]], bool]:
        try:
            epoch = self.mux.capture_epoch()
            cursor, result, identities = start_ms, [], set()
            for _ in range(self.maximum_pages):
                response = await self.mux.post_info(
                    {
                        "type": "userFillsByTime",
                        "user": source,
                        "startTime": cursor,
                        "endTime": end_ms,
                        "aggregateByTime": False,
                    },
                    required_epoch=epoch,
                )
                if response.outcome is not PostOutcome.INFO:
                    raise ContinuousNetworkError(
                        f"WS fill recovery was {response.outcome.value}: {response.reason}"
                    )
                page = _info_rows(response.response, expected_type="userFillsByTime")
                if len(page) > self.page_size:
                    raise ContinuousNetworkError("WS fill recovery page exceeded its bound")
                newest = cursor
                added = 0
                for item in page:
                    tx_hash, tid, fill_ms = _raw_fill_identity(item)
                    if not start_ms <= fill_ms <= end_ms:
                        raise ContinuousNetworkError(
                            "WS fill recovery returned an out-of-window row"
                        )
                    identity = (tx_hash, tid)
                    newest = max(newest, fill_ms)
                    if identity not in identities:
                        identities.add(identity)
                        result.append(item)
                        added += 1
                if len(page) < self.page_size:
                    return result, False
                if newest <= cursor or added == 0:
                    raise ContinuousNetworkError("WS fill recovery pagination did not advance")
                cursor = newest
            raise ContinuousNetworkError("WS fill recovery exceeded its bounded page count")
        except (asyncio.CancelledError, KeyboardInterrupt):
            raise
        except Exception as exc:
            if self.fallback is None:
                if isinstance(exc, ContinuousNetworkError):
                    raise
                raise ContinuousNetworkError(f"WS fill recovery failed: {exc}") from exc
            return (
                await self._fallback_rows(source=source, start_ms=start_ms, end_ms=end_ms),
                True,
            )

    async def _fallback_rows(
        self, *, source: str, start_ms: int, end_ms: int
    ) -> list[Mapping[str, Any]]:
        if self.fallback is None:
            raise ContinuousNetworkError("HTTP fill recovery fallback is disabled")
        fallback = await self.fallback(user=source, start_ms=start_ms, end_ms=end_ms)
        if len(fallback) > self.maximum_pages * self.page_size:
            raise ContinuousNetworkError("fallback fill recovery exceeded its row bound")
        result: list[Mapping[str, Any]] = [dict(item) for item in fallback]
        if any(not start_ms <= _raw_fill_identity(item)[2] <= end_ms for item in result):
            raise ContinuousNetworkError("fallback fill recovery returned an out-of-window row")
        return result


class _Writer:
    def __init__(self, socket: _Socket, *, timeout_s: float) -> None:
        self.socket, self.timeout_s, self.lock = socket, timeout_s, asyncio.Lock()

    async def frame(self, value: Mapping[str, Any]) -> None:
        encoded = json.dumps(value, separators=(",", ":"))
        async with self.lock:
            await asyncio.wait_for(self.socket.send(encoded), timeout=self.timeout_s)


class _PongSocket:
    """Let the action mux own reads while exposing application pong liveness."""

    def __init__(self, socket: _Socket, pong: asyncio.Event) -> None:
        self.socket, self.pong = socket, pong
        self.iterator = socket.__aiter__()
        self.send_lock = asyncio.Lock()

    def __aiter__(self) -> _PongSocket:
        return self

    async def __anext__(self) -> Any:
        raw = await self.iterator.__anext__()
        try:
            if _message(raw).get("channel") == "pong":
                self.pong.set()
        except ContinuousNetworkError:
            pass
        return raw

    async def send(self, message: str) -> Any:
        async with self.send_lock:
            return await self.socket.send(message)


@dataclass(slots=True)
class _SlotWake:
    event: asyncio.Event
    now_ms: int = 0
    received_mono_ns: int | None = None
    wake_mono_ns: int | None = None

    def push(self, *, now_ms: int, received_mono_ns: int | None, wake_mono_ns: int) -> None:
        self.now_ms = max(self.now_ms, now_ms)
        if received_mono_ns is not None and (
            self.received_mono_ns is None or received_mono_ns < self.received_mono_ns
        ):
            self.received_mono_ns = received_mono_ns
        if self.wake_mono_ns is None:
            self.wake_mono_ns = wake_mono_ns
        self.event.set()

    async def pop(self) -> tuple[int, int | None, int | None]:
        await self.event.wait()
        self.event.clear()
        now_ms, received_mono_ns, wake_mono_ns = (
            self.now_ms,
            self.received_mono_ns,
            self.wake_mono_ns,
        )
        self.now_ms = 0
        self.received_mono_ns = None
        self.wake_mono_ns = None
        return now_ms, received_mono_ns, wake_mono_ns


@dataclass(frozen=True, slots=True)
class _SourceFrame:
    message: Mapping[str, Any]
    epoch: int
    received_ms: int
    received_mono_ns: int


@dataclass(slots=True)
class _ConnectionStatus:
    state: str = "offline"
    outage_started_ns: int | None = None
    connected_ns: int | None = None
    last_good_ms: int = 0
    last_error: str = ""
    alarm_latched: bool = False


@dataclass(slots=True)
class _SlotOperations:
    latest_leader_event_ms: int = 0
    latest_follower_ack_ms: int = 0
    last_successful_sync_ms: int = 0
    latest_market: str = ""
    latest_cloid: str = ""
    latest_outcome: str = ""
    latest_latency_ms: float | None = None
    latest_trigger_age_ms: int | None = None
    latest_target_wait_ms: int | None = None
    latest_price_deviation_bps: float | None = None
    latest_execution_class: str = ""
    latest_source_revision: int | None = None
    attempts: int = 0
    failed: int = 0
    retrying: int = 0
    latest_error: str = ""


class ContinuousNetworkDriver:
    """Three-socket caller for ``ContinuousRuntime`` with no REST live path."""

    def __init__(
        self,
        *,
        runtime: ContinuousRuntime,
        mux: WsPostMux,
        ws_url: str,
        repair: DurableSourceGapRepair | None = None,
        connector: Connector | None = None,
        policy: ReconnectPolicy = ReconnectPolicy(),
        event_capacity: int = 256,
        source_queue_capacity: int = 256,
        wall_ms: Callable[[], int] | None = None,
        mono_ns: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_unit: Callable[[], float] | None = None,
        metric_sink: MetricSink | None = None,
        rearm_restored_fail_close: bool = False,
    ) -> None:
        if not ws_url.startswith(("wss://", "ws://")):
            raise ValueError("continuous WebSocket URL must use ws:// or wss://")
        if min(event_capacity, source_queue_capacity) < 1:
            raise ValueError("network queue capacities must be positive")
        if getattr(runtime, "mux", mux) is not mux:
            raise ValueError("runtime and network driver must share one WS POST mux")
        if getattr(runtime, "gap_repair", repair) is not repair:
            raise ValueError("runtime and network driver must share one gap-repair hook")
        if repair is not None and repair.mux is not mux:
            raise ValueError("gap repair and network driver must share one WS POST mux")
        self.runtime, self.mux, self.ws_url = runtime, mux, ws_url
        self.repair, self.policy = repair, policy
        self.events: asyncio.Queue[Dispatch] = asyncio.Queue(maxsize=event_capacity)
        self.dropped_events = 0
        self.connector = connector or _direct_connector
        self.wall_ms = wall_ms or (lambda: time_ns() // 1_000_000)
        self.mono_ns = mono_ns or monotonic_ns
        self.sleep = sleep
        self.random_unit = random_unit or (lambda: 0.5)
        self.metric_sink = metric_sink
        self.rearm_restored_fail_close = rearm_restored_fail_close
        self.dropped_metrics = 0
        self._market_lock = asyncio.Lock()
        self._market_writer: _Writer | None = None
        self._market_sent: dict[str, Mapping[str, str]] = {}
        self._market_sync = asyncio.Event()
        self._action_connected = asyncio.Event()
        self._connection_attempt_lock = asyncio.Lock()
        self._connection_attempts: deque[int] = deque()
        self._reconcile_wakes = {slot_id: asyncio.Event() for slot_id in runtime.slot_ids}
        self._slot_wakes = {slot_id: _SlotWake(asyncio.Event()) for slot_id in runtime.slot_ids}
        self._source_queues = {
            slot_id: asyncio.Queue[_SourceFrame](maxsize=source_queue_capacity)
            for slot_id in runtime.slot_ids
        }
        self._source_reduce_locks = {slot_id: asyncio.Lock() for slot_id in runtime.slot_ids}
        self._source_active_epoch: int | None = None
        self._connections = {name: _ConnectionStatus() for name in ("source", "market", "action")}
        self._slot_operations = {slot_id: _SlotOperations() for slot_id in runtime.slot_ids}
        self._stopping = False
        self._closeout_lock = asyncio.Lock()
        self.discarded_source_frames = 0
        self._sources = tuple(
            sorted({str(spec["user"]).lower() for spec in runtime.source_subscriptions})
        )
        if not 1 <= len(self._sources) <= 10:
            raise ValueError("shared source socket requires one to ten unique leaders")

    def operational_status(self) -> dict[str, Any]:
        """Return a cheap canonical transport/queue snapshot for the local UI."""

        now_ns, now_ms = self.mono_ns(), self.wall_ms()
        connections: dict[str, Any] = {}
        alarms: list[dict[str, Any]] = []
        threshold_ms = int(self.policy.source_fail_close_s * 1_000)
        stable_ns = int(self.policy.stable_connection_s * 1_000_000_000)
        for name, item in self._connections.items():
            outage_ms = (
                0
                if item.outage_started_ns is None
                else max(0, (now_ns - item.outage_started_ns) // 1_000_000)
            )
            stable = bool(
                item.state == "connected"
                and item.connected_ns is not None
                and now_ns - item.connected_ns >= stable_ns
            )
            if item.outage_started_ns is not None and outage_ms >= threshold_ms:
                item.alarm_latched = True
            if stable:
                item.outage_started_ns = None
                item.alarm_latched = False
                outage_ms = 0
            state = item.state
            if state == "connected" and not stable and item.outage_started_ns is not None:
                state = "reconnecting"
            connections[name] = {
                "state": state,
                "last_good_ms": item.last_good_ms,
                "outage_ms": int(outage_ms),
                "last_error": item.last_error,
                "stable": stable,
            }
            if item.alarm_latched:
                alarms.append(
                    {
                        "id": f"prolonged-disconnect:{name}",
                        "kind": "prolonged_disconnect",
                        "connection": name,
                        "started_ms": now_ms - int(outage_ms),
                        "duration_ms": int(outage_ms),
                        "threshold_ms": threshold_ms,
                        "message": f"{name} connection has been unavailable too long",
                    }
                )
        slots: dict[str, Any] = {}
        for slot_id, ops in self._slot_operations.items():
            slots[slot_id] = {
                "latest_leader_event_ms": ops.latest_leader_event_ms,
                "latest_follower_ack_ms": ops.latest_follower_ack_ms,
                "latest_ack_ms": ops.latest_follower_ack_ms,
                "last_successful_sync_ms": ops.last_successful_sync_ms,
                "latest_market": ops.latest_market,
                "market": ops.latest_market,
                "latest_cloid": ops.latest_cloid,
                "latest_outcome": ops.latest_outcome,
                "copy_latency_ms": ops.latest_latency_ms,
                "recent_latency_ms": ops.latest_latency_ms,
                "latest_trigger_age_ms": ops.latest_trigger_age_ms,
                "latest_target_wait_ms": ops.latest_target_wait_ms,
                "latest_price_deviation_bps": ops.latest_price_deviation_bps,
                "latest_execution_class": ops.latest_execution_class,
                "latest_source_revision": ops.latest_source_revision,
                "queued": self._source_queues[slot_id].qsize()
                + int(self._slot_wakes[slot_id].event.is_set()),
                "retrying": ops.retrying,
                "failed": ops.failed,
                "attempts": ops.attempts,
                "latest_error": ops.latest_error,
            }
        return {
            "connections": connections,
            "alarms": alarms,
            "alarm_threshold_ms": threshold_ms,
            "backlog": {
                "source": sum(queue.qsize() for queue in self._source_queues.values()),
                "action": sum(int(wake.event.is_set()) for wake in self._slot_wakes.values()),
                "recovery": sum(int(wake.is_set()) for wake in self._reconcile_wakes.values()),
            },
            "slots": slots,
        }

    def _connection_connecting(self, name: str) -> None:
        item = self._connections[name]
        if item.outage_started_ns is None:
            item.outage_started_ns = self.mono_ns()
        item.state = "reconnecting" if item.last_good_ms else "connecting"

    def _connection_opened(self, name: str) -> None:
        item = self._connections[name]
        item.state = "connected"
        item.connected_ns = self.mono_ns()
        item.last_good_ms = self.wall_ms()

    def _connection_failed(self, name: str, exc: BaseException) -> None:
        item = self._connections[name]
        if item.outage_started_ns is None:
            item.outage_started_ns = self.mono_ns()
        item.state = "reconnecting"
        item.connected_ns = None
        item.last_error = f"{type(exc).__name__}: {exc}"[:300]

    def _connection_good(self, name: str) -> None:
        item = self._connections[name]
        item.last_good_ms = self.wall_ms()

    async def run(self, *, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        tasks = [
            asyncio.create_task(self._supervise("source", self._source_session)),
            asyncio.create_task(self._supervise("market", self._market_session)),
            asyncio.create_task(self._supervise("action", self._action_session)),
            *(
                asyncio.create_task(self._reconcile_slot_loop(slot_id))
                for slot_id in self.runtime.slot_ids
            ),
            *(
                asyncio.create_task(self._source_reduce_loop(slot_id))
                for slot_id in self.runtime.slot_ids
            ),
            *(asyncio.create_task(self._slot_actor(slot_id)) for slot_id in self.runtime.slot_ids),
            *(
                [asyncio.create_task(self._source_fail_close_loop())]
                if getattr(self.runtime, "execution_enabled", False)
                else []
            ),
            asyncio.create_task(stop.wait()),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failures = [
                task.exception()
                for task in done
                if task is not tasks[-1] and not task.cancelled() and task.exception() is not None
            ]
            if failures:
                raise failures[0]  # type: ignore[misc]
            if tasks[-1] not in done:
                raise ContinuousNetworkError("continuous network task stopped unexpectedly")
        finally:
            self._stopping = True
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def notify_market_change(self, change: MarketSubscriptionChange) -> None:
        """Synchronize runtime catalog changes onto the existing market socket."""

        await self._market_change(change)

    async def close_out_all(self, *, reason: str) -> None:
        await self.close_out(self.runtime.slot_ids, reason=reason)

    async def close_out(self, slot_ids: Sequence[str], *, reason: str) -> None:
        if not self.runtime.execution_enabled:
            return
        selected = tuple(dict.fromkeys(slot_ids))
        if not selected:
            return
        async with self._closeout_lock:
            self.runtime.request_fail_close(selected, reason=reason)
            self._metric("fail_close_started", slots=list(selected), reason=reason)
            while True:
                await self._action_connected.wait()
                all_authoritatively_flat = True
                for slot_id in selected:
                    refresh_started_ms = self.wall_ms()
                    reconciled = await self.runtime.reconcile_follower(
                        slot_id, now_ms=refresh_started_ms, drive=False
                    )
                    await self._publish(reconciled)
                    await self._market_change(reconciled.market_change)
                    if self.runtime.follower_is_flat(
                        slot_id,
                        observed_at_least_ms=refresh_started_ms,
                    ):
                        continue
                    all_authoritatively_flat = False
                    closing = await self.runtime.drive_fail_close(slot_id, now_ms=self.wall_ms())
                    await self._publish(closing)
                    await self._market_change(closing.market_change)
                    self._metric(
                        "fail_close_attempt",
                        slot=slot_id,
                        outcome=(
                            None
                            if closing.attempt is None
                            else closing.attempt.result.outcome.value
                        ),
                        reason=closing.reason,
                    )
                if all_authoritatively_flat:
                    self._metric("fail_close_complete", slots=list(selected), reason=reason)
                    return
                await self.sleep(1.0)

    async def _source_fail_close_loop(self) -> None:
        unready_since: dict[str, int] = {}
        # Latches present when the runner starts are interrupted prior closeouts.
        # Latches created later are owned by the caller that requested them and
        # must not be started a second time by this source-gap watcher.
        already_closed = set(self.runtime.fail_close_slots)
        if already_closed:
            await self.close_out(
                tuple(sorted(already_closed)), reason="restored incomplete fail-close"
            )
            if self.rearm_restored_fail_close:
                restored = tuple(sorted(already_closed))
                self.runtime.operator_rearm(restored)
                self._metric("fail_close_rearmed", slots=list(restored))
                already_closed.clear()
        threshold_ns = int(self.policy.source_fail_close_s * 1_000_000_000)
        while True:
            now_ms, now_ns = self.wall_ms(), self.mono_ns()
            expired: list[str] = []
            for slot_id in self.runtime.slot_ids:
                if slot_id in already_closed:
                    continue
                if self.runtime.source_is_ready(slot_id, now_ms=now_ms):
                    unready_since.pop(slot_id, None)
                    continue
                started = unready_since.setdefault(slot_id, now_ns)
                if now_ns - started >= threshold_ns:
                    expired.append(slot_id)
            if expired:
                reason = (
                    f"leader feed unavailable for at least {self.policy.source_fail_close_s:g}s"
                )
                await self.close_out(tuple(expired), reason=reason)
                already_closed.update(expired)
            await self.sleep(1.0)

    async def _supervise(self, name: str, session: Callable[[_Socket], Awaitable[None]]) -> None:
        last_failure = ""
        consecutive = 0
        while self.policy.attempts is None or consecutive < self.policy.attempts:
            connected_ns: int | None = None
            try:
                self._connection_connecting(name)
                await self._admit_connection_attempt(name)
                async with self.connector(name, self.ws_url) as socket:
                    connected_ns = self.mono_ns()
                    self._connection_opened(name)
                    await session(socket)
                raise ConnectionError("socket reached clean EOF")
            except asyncio.CancelledError:
                raise
            except FatalContinuousNetworkError:
                raise
            except Exception as exc:
                self._connection_failed(name, exc)
                if (
                    connected_ns is not None
                    and self.mono_ns() - connected_ns
                    >= self.policy.stable_connection_s * 1_000_000_000
                ):
                    consecutive = 0
                    last_failure = ""
                consecutive += 1
                last_failure = f"{type(exc).__name__}: {exc}"
                self._metric(
                    "socket_reconnect",
                    socket=name,
                    consecutive=consecutive,
                    error_type=type(exc).__name__,
                )
                if self.policy.attempts is not None and consecutive == self.policy.attempts:
                    break
                await self.sleep(self._delay(consecutive - 1))
        assert self.policy.attempts is not None
        raise ContinuousNetworkError(
            f"{name} socket exhausted {self.policy.attempts} consecutive attempts; "
            f"last={last_failure}"
        )

    async def _admit_connection_attempt(self, socket_name: str) -> None:
        window_ns = 60_000_000_000
        while True:
            wait_s = 0.0
            async with self._connection_attempt_lock:
                now = self.mono_ns()
                cutoff = now - window_ns
                while self._connection_attempts and self._connection_attempts[0] <= cutoff:
                    self._connection_attempts.popleft()
                if len(self._connection_attempts) < self.policy.connection_attempts_per_minute:
                    self._connection_attempts.append(now)
                    self._metric(
                        "socket_connect_attempt",
                        socket=socket_name,
                        attempts_in_window=len(self._connection_attempts),
                    )
                    return
                wait_s = max(
                    0.001,
                    (self._connection_attempts[0] + window_ns - now) / 1_000_000_000,
                )
            self._metric(
                "socket_connect_throttle",
                socket=socket_name,
                wait_ms=wait_s * 1_000,
            )
            await self.sleep(wait_s)

    async def _source_session(self, socket: _Socket) -> None:
        writer = _Writer(socket, timeout_s=self.policy.write_timeout_s)
        await self._acquire_source_reduce_locks()
        try:
            self._discard_source_backlog()
            epoch = self.runtime.begin_source_connection(received_ms=self.wall_ms())
            self._source_active_epoch = epoch
        finally:
            self._release_source_reduce_locks()
        pong = asyncio.Event()
        if self.repair is not None:
            self.repair.begin_connection(self._sources)
        try:
            for spec in self.runtime.source_subscriptions:
                await writer.frame({"method": "subscribe", "subscription": spec})

            async def receive() -> None:
                async for raw in socket:
                    message = _message(raw)
                    if message.get("channel") == "pong":
                        pong.set()
                        self.runtime.note_source_activity(epoch=epoch, received_ms=self.wall_ms())
                        continue
                    if message.get("channel") == "subscriptionResponse":
                        continue
                    if message.get("channel") == "error":
                        raise FatalContinuousNetworkError(f"source subscription error: {message}")
                    received_ms, received_ns = self.wall_ms(), self.mono_ns()
                    data = message.get("data")
                    source = (
                        str(data.get("user") or data.get("userAddress") or "")
                        if isinstance(data, Mapping)
                        else ""
                    )
                    slot_id = self.runtime.source_slot_id(source)
                    if slot_id is None:
                        continue
                    frame = _SourceFrame(message, epoch, received_ms, received_ns)
                    try:
                        self._source_queues[slot_id].put_nowait(frame)
                    except asyncio.QueueFull as exc:
                        raise ContinuousNetworkError(
                            f"source queue overflow for {slot_id}"
                        ) from exc
                raise ConnectionError("source socket reached EOF")

            await self._duplex(receive(), writer, pong, "source")
        finally:
            # Close the epoch before waiting on reducers. A reducer that has not
            # entered its per-slot barrier will discard this epoch's frame; one
            # already applying finishes before the gap is recorded. No old frame
            # can then leak into the next baseline epoch.
            if self._source_active_epoch == epoch:
                self._source_active_epoch = None
            if not self._stopping:
                reason = "source transport disconnected"
                # Close admission in the runtime before a reducer waiting behind a
                # slot's action ACK can acquire that slot lock. The runtime repeats
                # this epoch check *inside* its slot lock.
                self.runtime.connection_gap("source", epoch=epoch, reason=reason)
                self._metric("connection_gap", socket="source", epoch=epoch)
                await self._acquire_source_reduce_locks()
                try:
                    self._discard_source_backlog()
                    # A reducer already inside runtime reduction when the socket
                    # closed is allowed to finish, then this second call restores
                    # the fail-closed state before the next epoch can begin.
                    self.runtime.connection_gap("source", epoch=epoch, reason=reason)
                    if self.repair is not None:
                        self.repair.mark_gap(self._sources, when_ms=self.wall_ms())
                finally:
                    self._release_source_reduce_locks()

    async def _source_reduce_loop(self, slot_id: str) -> None:
        queue = self._source_queues[slot_id]
        reduce_lock = self._source_reduce_locks[slot_id]
        while True:
            frame = await queue.get()
            try:
                async with reduce_lock:
                    if frame.epoch != self._source_active_epoch:
                        self.discarded_source_frames += 1
                        continue
                    if self.repair is not None:
                        self.repair.stage_source_frame(frame.message)
                    reduce_started_ns = self.mono_ns()
                    dispatch = await self.runtime.apply_source(
                        frame.message,
                        epoch=frame.epoch,
                        received_ms=frame.received_ms,
                        received_mono_ns=frame.received_mono_ns,
                        drive=False,
                    )
                    reduced_ns = self.mono_ns()
                    self._metric(
                        "source_reduce",
                        slot=slot_id,
                        queue_delay_ms=max(
                            0.0,
                            (reduce_started_ns - frame.received_mono_ns) / 1_000_000,
                        ),
                        source_receive_to_reduce_ms=max(
                            0.0,
                            (reduced_ns - frame.received_mono_ns) / 1_000_000,
                        ),
                        accepted=dispatch.source_frame_accepted,
                    )
                    if frame.epoch != self._source_active_epoch:
                        self.discarded_source_frames += 1
                        continue
                    await self._publish(dispatch)
                    await self._market_change(dispatch.market_change)
                    if dispatch.slot != slot_id:
                        raise ContinuousNetworkError(
                            f"source frame routed to {slot_id} but reduced as {dispatch.slot}"
                        )
                    _timestamp_match, ready = self.runtime.source_frame_status(
                        slot_id, received_ms=frame.received_ms
                    )
                    if not dispatch.source_frame_accepted:
                        continue
                    self._slot_operations[slot_id].latest_leader_event_ms = frame.received_ms
                    if dispatch.reason.startswith("source frame failed:"):
                        continue
                    if self.repair is not None:
                        data = frame.message.get("data")
                        assert isinstance(data, Mapping)
                        source = str(data.get("user") or data.get("userAddress") or "")
                        self.repair.record_accepted(
                            source=source,
                            message=frame.message,
                            received_ms=frame.received_ms,
                            source_ready=ready,
                        )
                    self._wake_slot(
                        slot_id,
                        now_ms=frame.received_ms,
                        received_mono_ns=frame.received_mono_ns,
                    )
            finally:
                queue.task_done()

    async def _acquire_source_reduce_locks(self) -> None:
        acquired: list[asyncio.Lock] = []
        try:
            for slot_id in sorted(self._source_reduce_locks):
                lock = self._source_reduce_locks[slot_id]
                await lock.acquire()
                acquired.append(lock)
        except BaseException:
            for lock in reversed(acquired):
                lock.release()
            raise

    def _release_source_reduce_locks(self) -> None:
        for slot_id in reversed(sorted(self._source_reduce_locks)):
            lock = self._source_reduce_locks[slot_id]
            if not lock.locked():
                raise RuntimeError("source reducer transition lock was not held")
            lock.release()

    def _discard_source_backlog(self) -> None:
        for queue in self._source_queues.values():
            while True:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    self.discarded_source_frames += 1
                    queue.task_done()

    async def _market_session(self, socket: _Socket) -> None:
        writer = _Writer(socket, timeout_s=self.policy.write_timeout_s)
        pong = asyncio.Event()
        epoch = self.runtime.begin_market_connection(received_ms=self.wall_ms())
        async with self._market_lock:
            self._market_writer = writer
            self._market_sent.clear()
        await self._sync_market_subscriptions()
        try:

            async def receive() -> None:
                async for raw in socket:
                    message = _message(raw)
                    if message.get("channel") == "pong":
                        pong.set()
                        self.runtime.note_market_activity(epoch=epoch, received_ms=self.wall_ms())
                        continue
                    if message.get("channel") == "subscriptionResponse":
                        continue
                    if message.get("channel") == "error":
                        raise FatalContinuousNetworkError(f"market subscription error: {message}")
                    received_ms = self.wall_ms()
                    for dispatch in await self.runtime.apply_market(
                        message, epoch=epoch, received_ms=received_ms, drive=False
                    ):
                        await self._publish(dispatch)
                        if dispatch.slot is not None:
                            self._wake_slot(
                                dispatch.slot,
                                now_ms=received_ms,
                                received_mono_ns=None,
                            )
                raise ConnectionError("market socket reached EOF")

            await self._market_duplex(receive(), writer, pong, "market")
        finally:
            async with self._market_lock:
                if self._market_writer is writer:
                    self._market_writer = None
                    self._market_sent.clear()
            if not self._stopping:
                self.runtime.connection_gap(
                    "market", epoch=epoch, reason="market transport disconnected"
                )
                self._metric("connection_gap", socket="market", epoch=epoch)

    async def _action_session(self, socket: _Socket) -> None:
        pong = asyncio.Event()
        tapped = _PongSocket(socket, pong)
        writer = _Writer(tapped, timeout_s=self.policy.write_timeout_s)
        epoch = self.mux.attach(tapped)
        self._action_connected.set()
        for wake in self._reconcile_wakes.values():
            wake.set()
        try:
            await self._duplex(self.mux.receive_loop(epoch), writer, pong, "action")
        finally:
            self._action_connected.clear()
            self.mux.detach(epoch, reason="action transport disconnected")
            if not self._stopping:
                self._metric("connection_gap", socket="action", epoch=epoch)

    async def _reconcile_slot_loop(self, slot_id: str) -> None:
        wake = self._reconcile_wakes[slot_id]
        slot_index = self.runtime.slot_ids.index(slot_id)
        while True:
            await self._action_connected.wait()
            await wake.wait()
            wake.clear()
            if slot_index:
                await self.sleep(slot_index * self.policy.reconciliation_stagger_s)
            if not self._action_connected.is_set():
                continue
            now_ms = self.wall_ms()
            started_ns = self.mono_ns()
            dispatch = await self.runtime.reconcile_follower(slot_id, now_ms=now_ms, drive=False)
            self._metric(
                "follower_refresh",
                slot=slot_id,
                elapsed_ms=max(0.0, (self.mono_ns() - started_ns) / 1_000_000),
                reason=dispatch.reason,
            )
            runtime_status = self.runtime.operational_status(slot_id, now_ms=now_ms)
            accepted_sync_ms = int(runtime_status.get("last_successful_sync_ms") or 0)
            if accepted_sync_ms > self._slot_operations[slot_id].last_successful_sync_ms:
                self._slot_operations[slot_id].last_successful_sync_ms = accepted_sync_ms
            await self._publish(dispatch)
            await self._market_change(dispatch.market_change)
            self._wake_slot(slot_id, now_ms=now_ms, received_mono_ns=None)
            try:
                await asyncio.wait_for(wake.wait(), timeout=self.policy.reconciliation_s)
            except TimeoutError:
                wake.set()

    async def _slot_actor(self, slot_id: str) -> None:
        wake = self._slot_wakes[slot_id]
        while True:
            now_ms, received_mono_ns, wake_mono_ns = await wake.pop()
            dispatch = await self.runtime.drive_slot(
                slot_id,
                now_ms=max(now_ms, self.wall_ms()),
                received_mono_ns=received_mono_ns,
            )
            await self._publish(dispatch)
            await self._market_change(dispatch.market_change)
            attempt = dispatch.attempt
            if attempt is not None:
                ops = self._slot_operations[slot_id]
                context = dict(getattr(attempt, "execution_context", {}) or {})
                ops.latest_follower_ack_ms = self.wall_ms()
                ops.latest_market = attempt.record.market
                ops.latest_cloid = attempt.record.cloid
                ops.latest_outcome = attempt.result.outcome.value
                ops.latest_latency_ms = (
                    None
                    if attempt.received_to_send_ms is None
                    else float(attempt.received_to_send_ms)
                )
                ops.latest_trigger_age_ms = _optional_int(context.get("leader_trigger_age_ms"))
                ops.latest_target_wait_ms = _optional_int(context.get("accepted_target_wait_ms"))
                ops.latest_execution_class = str(context.get("execution_class") or "")
                ops.latest_source_revision = _optional_int(context.get("source_revision"))
                price_deviation_bps = _leader_to_fill_bps(
                    average_fill_price=attempt.result.average_fill_price,
                    leader_trigger_px=context.get("leader_trigger_px"),
                    side=context.get("side"),
                )
                ops.latest_price_deviation_bps = price_deviation_bps
                ops.attempts += 1
                if attempt.result.outcome in {PostOutcome.REJECTED, PostOutcome.UNKNOWN}:
                    ops.failed += 1
                    ops.latest_error = attempt.record.outcome_detail or attempt.result.reason
                else:
                    ops.latest_error = ""
                if attempt.result.outcome is PostOutcome.UNKNOWN:
                    self._reconcile_wakes[slot_id].set()
                actor_wake_to_send_ms: float | None = None
                if (
                    attempt.received_to_send_ms is not None
                    and received_mono_ns is not None
                    and wake_mono_ns is not None
                ):
                    actor_wake_to_send_ms = max(
                        0.0,
                        float(attempt.received_to_send_ms)
                        - max(0.0, (wake_mono_ns - received_mono_ns) / 1_000_000),
                    )
                self._metric(
                    "action_attempt",
                    slot=slot_id,
                    cloid=attempt.record.cloid,
                    desired_id=attempt.record.desired_id,
                    market=attempt.record.market,
                    requested_size=str(attempt.record.requested_size),
                    cumulative_filled_size=str(attempt.record.cumulative_filled_size),
                    action_state=attempt.record.state.value,
                    outcome_detail=attempt.record.outcome_detail,
                    outcome=attempt.result.outcome.value,
                    average_fill_price=(
                        None
                        if attempt.result.average_fill_price is None
                        else str(attempt.result.average_fill_price)
                    ),
                    order_id=attempt.result.order_id,
                    source_receive_to_send_ms=(
                        None
                        if attempt.received_to_send_ms is None
                        else float(attempt.received_to_send_ms)
                    ),
                    actor_wake_to_send_ms=actor_wake_to_send_ms,
                    send_to_response_ms=(
                        None
                        if attempt.send_to_response_ms is None
                        else float(attempt.send_to_response_ms)
                    ),
                    leader_to_fill_bps=price_deviation_bps,
                    **context,
                )

    def _wake_slot(self, slot_id: str, *, now_ms: int, received_mono_ns: int | None) -> None:
        self._slot_wakes[slot_id].push(
            now_ms=now_ms,
            received_mono_ns=received_mono_ns,
            wake_mono_ns=self.mono_ns(),
        )

    async def _market_change(self, change: MarketSubscriptionChange) -> None:
        if change.added or change.removed:
            self._market_sync.set()

    async def _market_control_session(self) -> None:
        while True:
            await self._market_sync.wait()
            self._market_sync.clear()
            await self._sync_market_subscriptions()

    async def _sync_market_subscriptions(self) -> None:
        async with self._market_lock:
            writer = self._market_writer
            if writer is None:
                return
            desired = {
                json.dumps(spec, sort_keys=True, separators=(",", ":")): spec
                for spec in self.runtime.market_subscriptions
            }
            for key in sorted(set(self._market_sent) - set(desired)):
                spec = self._market_sent[key]
                await writer.frame({"method": "unsubscribe", "subscription": spec})
                self._market_sent.pop(key)
            for key in sorted(set(desired) - set(self._market_sent)):
                spec = desired[key]
                await writer.frame({"method": "subscribe", "subscription": spec})
                self._market_sent[key] = spec

    async def _duplex(
        self,
        receive: Awaitable[None],
        writer: _Writer,
        pong: asyncio.Event,
        connection_name: str,
    ) -> None:
        reader: asyncio.Future[None] = asyncio.ensure_future(receive)
        heartbeat = asyncio.create_task(self._heartbeat(writer, pong, connection_name))
        try:
            done, _ = await asyncio.wait((reader, heartbeat), return_when=asyncio.FIRST_COMPLETED)
            failure = next((task.exception() for task in done if task.exception()), None)
            if failure is not None:
                raise failure
            raise ConnectionError("WebSocket session ended")
        finally:
            reader.cancel()
            heartbeat.cancel()
            await asyncio.gather(reader, heartbeat, return_exceptions=True)

    async def _market_duplex(
        self,
        receive: Awaitable[None],
        writer: _Writer,
        pong: asyncio.Event,
        connection_name: str,
    ) -> None:
        reader: asyncio.Future[None] = asyncio.ensure_future(receive)
        heartbeat = asyncio.create_task(self._heartbeat(writer, pong, connection_name))
        control = asyncio.create_task(self._market_control_session())
        tasks = (reader, heartbeat, control)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            failure = next((task.exception() for task in done if task.exception()), None)
            if failure is not None:
                raise failure
            raise ConnectionError("market WebSocket session ended")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _heartbeat(
        self, writer: _Writer, pong: asyncio.Event, connection_name: str = "source"
    ) -> None:
        while True:
            await self.sleep(self.policy.heartbeat_s)
            pong.clear()
            await writer.frame({"method": "ping"})
            try:
                await asyncio.wait_for(pong.wait(), timeout=self.policy.heartbeat_timeout_s)
            except TimeoutError as exc:
                raise ConnectionError("application ping received no pong") from exc
            self._connection_good(connection_name)

    async def _publish(self, dispatch: Dispatch) -> None:
        if self.events.full():
            self.events.get_nowait()
            self.dropped_events += 1
            self._metric("monitoring_drop", dropped_events=self.dropped_events)
        self.events.put_nowait(dispatch)

    def _metric(self, event: str, **fields: Any) -> None:
        sink = self.metric_sink
        if sink is None:
            return
        try:
            sink({"event": event, "wall_ms": self.wall_ms(), **fields})
        except Exception:
            self.dropped_metrics += 1

    def _delay(self, attempt: int) -> float:
        saturation_attempt = math.ceil(
            math.log2(self.policy.maximum_delay_s / self.policy.minimum_delay_s)
        )
        base = (
            self.policy.maximum_delay_s
            if attempt >= saturation_attempt
            else self.policy.minimum_delay_s * 2**attempt
        )
        factor = 1 + self.policy.jitter_fraction * (2 * self.random_unit() - 1)
        return min(self.policy.maximum_delay_s, max(0, base * factor))


@asynccontextmanager
async def _direct_connector(_name: str, url: str) -> AsyncIterator[_Socket]:
    async with connect_websocket_ipv6_preferred(
        url,
        proxy=None,
        ping_interval=None,
        open_timeout=10,
        close_timeout=2,
        max_queue=1_024,
    ) as socket:
        yield socket


def _message(raw: Any) -> Mapping[str, Any]:
    try:
        value = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContinuousNetworkError(f"WebSocket sent malformed JSON: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ContinuousNetworkError("WebSocket frame is not an object")
    return value


def _frame_fill_rows(message: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    data = message.get("data")
    if not isinstance(data, Mapping):
        return []
    channel = message.get("channel")
    if channel == "userFills":
        rows = data.get("fills")
        return (
            [item for item in rows if isinstance(item, Mapping)] if isinstance(rows, list) else []
        )
    if channel == "userTwapSliceFills":
        rows = data.get("twapSliceFills")
        if not isinstance(rows, list):
            return []
        return [
            item["fill"]
            for item in rows
            if isinstance(item, Mapping) and isinstance(item.get("fill"), Mapping)
        ]
    return []


def _raw_fill_identity(row: Mapping[str, Any]) -> tuple[str, str, int]:
    tx_hash, tid = str(row.get("hash") or "").strip().lower(), str(row.get("tid") or "").strip()
    if not tx_hash or not tid:
        raise ContinuousNetworkError("accepted source fill has no durable hash/tid identity")
    try:
        fill_ms = int(str(row.get("time")))
    except (TypeError, ValueError):
        raise ContinuousNetworkError("accepted source fill has invalid time") from None
    if fill_ms <= 0:
        raise ContinuousNetworkError("accepted source fill has invalid time")
    return tx_hash, tid, fill_ms


def _info_rows(response: Any, *, expected_type: str) -> list[Mapping[str, Any]]:
    value = response
    if isinstance(value, Mapping) and value.get("type") == "info":
        value = value.get("payload")
    if not isinstance(value, Mapping) or value.get("type") != expected_type:
        raise ContinuousNetworkError(f"unexpected WS info response for {expected_type}")
    rows = value.get("data")
    if not isinstance(rows, list) or not all(isinstance(item, Mapping) for item in rows):
        raise ContinuousNetworkError(f"{expected_type} response data is not a row list")
    return list(rows)


def _connect_fill_chain(
    records: Sequence[FillRecord],
    *,
    known: set[tuple[str, str]],
    before: Mapping[str, Decimal],
    after: Mapping[str, Decimal],
) -> tuple[FillRecord, ...]:
    selected: list[FillRecord] = []
    markets = {
        market
        for market in set(before) | set(after)
        if before.get(market, Decimal(0)) != after.get(market, Decimal(0))
    }
    ordered = sorted(records, key=lambda item: (item.time_ms, item.tx_hash, item.tid))
    for market in sorted(markets):
        current, target = before.get(market, Decimal(0)), after.get(market, Decimal(0))
        candidates: dict[Decimal, list[FillRecord]] = {}
        for fill in ordered:
            if (
                fill.market == market
                and (fill.tx_hash, fill.tid) not in known
                and fill.start_position is not None
            ):
                candidates.setdefault(fill.start_position, []).append(fill)
        used: set[tuple[str, str]] = set()
        minimum_time_ms = 0
        while current != target:
            candidate = next(
                (
                    item
                    for item in candidates.get(current, ())
                    if (item.tx_hash, item.tid) not in used and item.time_ms >= minimum_time_ms
                ),
                None,
            )
            if candidate is None:
                break
            selected.append(candidate)
            used.add((candidate.tx_hash, candidate.tid))
            current += candidate.signed_size
            minimum_time_ms = candidate.time_ms
        if current != target:
            raise ContinuousNetworkError(
                f"bounded fill recovery cannot connect {market} {before.get(market, 0)} "
                f"to {after.get(market, 0)}"
            )
    return tuple(selected)


def _optional_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _leader_to_fill_bps(
    *,
    average_fill_price: Decimal | None,
    leader_trigger_px: Any,
    side: Any,
) -> float | None:
    if average_fill_price is None or not average_fill_price.is_finite():
        return None
    try:
        leader_px = Decimal(str(leader_trigger_px))
    except (ArithmeticError, ValueError):
        return None
    if not leader_px.is_finite() or leader_px <= 0:
        return None
    direction = str(side or "").lower()
    if direction == "buy":
        value = (average_fill_price / leader_px - Decimal("1")) * Decimal("10000")
    elif direction == "sell":
        value = (Decimal("1") - average_fill_price / leader_px) * Decimal("10000")
    else:
        return None
    return float(value)


__all__ = [
    "ContinuousNetworkDriver",
    "ContinuousNetworkError",
    "DurableSourceGapRepair",
    "FatalContinuousNetworkError",
    "FillRecoveryFallback",
    "MetricSink",
    "ReconnectPolicy",
]
