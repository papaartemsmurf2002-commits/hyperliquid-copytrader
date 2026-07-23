from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from dataclasses import dataclass, field
from hashlib import sha256
from time import monotonic_ns, time_ns
from typing import Any, Awaitable, Callable, Mapping

from .account_state import StreamState
from .journal_writer import JournalQueueFull, JournalWriter
from .markets import canonical_market_symbol
from .network_evidence import (
    ACTIVE_CONTEXT_MAX_SILENCE_MS,
    ALL_DEXS_CONTEXT_MAX_SILENCE_MS,
    BOOK_MAX_SILENCE_MS,
    RUNTIME_FEED_LIVENESS_POLICY_VERSION,
    RUNTIME_FEED_LIVENESS_VERSION,
)
from .websocket_transport import connect_websocket_ipv6_preferred


CONTEXT_STRATEGY_ALL_DEXS = "continuous_all_dex_context"
CONTEXT_STRATEGY_ACTIVE_MARKETS = "active_market_context"
MARKET_CONTEXT_STRATEGIES = frozenset({CONTEXT_STRATEGY_ALL_DEXS, CONTEXT_STRATEGY_ACTIVE_MARKETS})


def stable_shard(value: str, shard_count: int, *, domain: str = "generic") -> int:
    if shard_count < 1:
        raise ValueError("shard count must be positive")
    if not domain or "|" in domain:
        raise ValueError("stable shard domain is invalid")
    digest = sha256(f"{domain}|{value.strip().lower()}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


@dataclass(frozen=True, slots=True)
class ConnectionLimits:
    maximum_connections: int = 10
    normal_connections: int = 5
    overlap_connections: int = 8
    subscriptions: int = 1_000
    unique_users: int = 10
    outbound_per_minute: int = 2_000
    inflight_posts: int = 100
    new_connections_per_minute: int = 30

    def __post_init__(self) -> None:
        if not 0 < self.normal_connections <= self.overlap_connections <= self.maximum_connections:
            raise ValueError("connection topology exceeds its bounded overlap")
        if (
            min(
                self.subscriptions,
                self.unique_users,
                self.outbound_per_minute,
                self.inflight_posts,
                self.new_connections_per_minute,
            )
            <= 0
        ):
            raise ValueError("websocket limits must be positive")


class ConnectionBudget:
    """Central same-process accounting for all five fleet WebSocket owners."""

    def __init__(self, limits: ConnectionLimits) -> None:
        self.limits = limits
        self._lock = asyncio.Lock()
        self._connections: set[str] = set()
        self._subscriptions: dict[str, set[str]] = {}
        self._users: set[str] = set()
        self._subscription_users: dict[tuple[str, str], str] = {}
        self._outbound_ms: list[int] = []
        self._connection_open_ms: list[int] = []
        self._inflight_posts = 0

    async def open_connection(self, owner: str, *, overlap: bool = False) -> None:
        async with self._lock:
            if owner in self._connections:
                raise RuntimeError(f"connection owner {owner} already open")
            limit = self.limits.overlap_connections if overlap else self.limits.normal_connections
            if len(self._connections) >= limit:
                raise RuntimeError("fleet websocket connection budget exhausted")
            now = time_ns() // 1_000_000
            cutoff = now - 60_000
            self._connection_open_ms = [
                stamp for stamp in self._connection_open_ms if stamp > cutoff
            ]
            if len(self._connection_open_ms) >= self.limits.new_connections_per_minute:
                raise RuntimeError("fleet websocket new-connection budget exhausted")
            self._connection_open_ms.append(now)
            self._connections.add(owner)
            self._subscriptions[owner] = set()

    async def close_connection(self, owner: str) -> None:
        async with self._lock:
            self._connections.discard(owner)
            self._subscriptions.pop(owner, None)
            self._subscription_users = {
                key: user for key, user in self._subscription_users.items() if key[0] != owner
            }
            self._users = {user for user in self._subscription_users.values() if user}

    async def add_subscription(self, owner: str, subscription_key: str, *, user: str = "") -> None:
        async with self._lock:
            if owner not in self._connections:
                raise RuntimeError("subscription owner has no connection")
            if subscription_key in self._subscriptions[owner]:
                return
            all_subscriptions = sum(len(items) for items in self._subscriptions.values())
            if all_subscriptions >= self.limits.subscriptions:
                raise RuntimeError("fleet websocket subscription budget exhausted")
            if user:
                prospective = self._users | {user.lower()}
                if len(prospective) > self.limits.unique_users:
                    raise RuntimeError("fleet websocket unique-user budget exhausted")
                self._users = prospective
            self._subscriptions[owner].add(subscription_key)
            self._subscription_users[(owner, subscription_key)] = user.lower() if user else ""

    async def remove_subscription(self, owner: str, subscription_key: str) -> None:
        async with self._lock:
            if owner not in self._connections:
                return
            self._subscriptions[owner].discard(subscription_key)
            self._subscription_users.pop((owner, subscription_key), None)
            self._users = {user for user in self._subscription_users.values() if user}

    async def record_outbound(self, now_ms: int) -> None:
        async with self._lock:
            cutoff = now_ms - 60_000
            self._outbound_ms = [stamp for stamp in self._outbound_ms if stamp > cutoff]
            if len(self._outbound_ms) >= self.limits.outbound_per_minute:
                raise RuntimeError("fleet websocket outbound budget exhausted")
            self._outbound_ms.append(now_ms)

    async def acquire_post(self) -> None:
        async with self._lock:
            if self._inflight_posts >= self.limits.inflight_posts:
                raise RuntimeError("fleet websocket POST in-flight budget exhausted")
            self._inflight_posts += 1

    async def release_post(self) -> None:
        async with self._lock:
            if self._inflight_posts <= 0:
                raise RuntimeError("websocket POST accounting underflow")
            self._inflight_posts -= 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "connections": len(self._connections),
            "owners": sorted(self._connections),
            "subscriptions": sum(len(items) for items in self._subscriptions.values()),
            "unique_users": len(self._users),
            "outbound_last_minute": len(self._outbound_ms),
            "new_connections_last_minute": len(self._connection_open_ms),
            "inflight_posts": self._inflight_posts,
        }


@dataclass(frozen=True, slots=True)
class SourceStreamEvent:
    event_key: str
    partition_key: str
    source: str
    event_class: str
    exchange_ts_ms: int
    receive_wall_ms: int
    receive_mono_ns: int
    ingress_seq: int
    payload: Mapping[str, Any]
    direct_eligible: bool
    seed_snapshot: bool
    stream_state: StreamState = StreamState.SNAPSHOT
    health_epoch: int = 0


@dataclass(slots=True)
class PartitionHealth:
    source: str
    state: StreamState = StreamState.CONNECTING
    acknowledged: set[str] = field(default_factory=set)
    required_acknowledgements: set[str] = field(default_factory=set)
    last_frame_wall_ms: int = 0
    last_valid_event_wall_ms: int = 0
    last_durable_checkpoint_wall_ms: int = 0
    ingress_cursor: int = 0
    applied_cursor: int = 0
    duplicates: int = 0
    overflow_count: int = 0
    stale_direct_events: int = 0
    gap_detail: str = ""
    repair_target_cursor: int = 0
    baseline_target_cursor: int = 0
    repair_barrier_seen: bool = False
    live_since_mono_ns: int = 0
    health_epoch: int = 0
    pending_raw_frames: int = 0


class SourceFrameContinuityError(RuntimeError):
    def __init__(self, source: str, detail: str) -> None:
        super().__init__(detail)
        self.source = source
        self.detail = detail


@dataclass(frozen=True, slots=True)
class _RawFrame:
    payload: Any
    receive_wall_ms: int
    receive_mono_ns: int
    source_epochs: tuple[tuple[str, int], ...] = ()


class SourceStreamGateway:
    """One source-shard connection owner with durable split cursors."""

    SUBSCRIPTION_TYPES = (
        "orderUpdates",
        "userEvents",
        "userFills",
        "allDexsClearinghouseState",
        "spotState",
        "clearinghouseState",
        "openOrders",
        "userFundings",
        "userNonFundingLedgerUpdates",
        "userTwapSliceFills",
        "userTwapHistory",
        "twapStates",
        "notification",
        "webData3",
    )

    def __init__(
        self,
        *,
        generation: str,
        shard: int,
        sources: tuple[str, ...],
        journal: JournalWriter,
        connection_budget: ConnectionBudget,
        queue_capacity: int,
        direct_max_age_ms: int,
        heartbeat_ms: int,
        output_capacity: int = 1024,
        slot_by_source: Mapping[str, str] | None = None,
        on_transition: (Callable[[str, StreamState, str], Awaitable[None] | None] | None) = None,
    ) -> None:
        if shard not in {0, 1}:
            raise ValueError("source shard must be 0 or 1")
        if queue_capacity < 1 or output_capacity < 1 or direct_max_age_ms <= 0:
            raise ValueError("source gateway bounds must be positive")
        for source in sources:
            if stable_shard(source, 2, domain="source") != shard:
                raise ValueError(f"source {source} is assigned to the wrong stable shard")
        self.generation = generation
        self.shard = shard
        self.sources = tuple(source.lower() for source in sources)
        self.slot_by_source = {
            str(source).lower(): str(slot) for source, slot in (slot_by_source or {}).items()
        }
        if set(self.slot_by_source) - set(self.sources):
            raise ValueError("source gateway slot map contains an unowned source")
        self.journal = journal
        self.connection_budget = connection_budget
        self.direct_max_age_ms = direct_max_age_ms
        self.heartbeat_ms = heartbeat_ms
        self.on_transition = on_transition
        self.owner = f"source-shard-{shard}"
        self._raw: asyncio.Queue[_RawFrame | None] = asyncio.Queue(queue_capacity)
        self.events: asyncio.Queue[SourceStreamEvent] = asyncio.Queue(output_capacity)
        self.partitions = {
            source: PartitionHealth(
                source=source,
                required_acknowledgements=set(self.SUBSCRIPTION_TYPES),
            )
            for source in self.sources
        }
        self._worker: asyncio.Task[None] | None = None
        self._health_worker: asyncio.Task[None] | None = None
        self._reconnect_requested = asyncio.Event()
        self._seen_fill_keys: OrderedDict[tuple[str, int, str, str], None] = OrderedDict()
        self._dedupe_capacity = max(queue_capacity * 32, 10_000)
        self._first_channels: set[tuple[str, str]] = set()
        self._baseline_channels: dict[str, set[str]] = {source: set() for source in self.sources}
        self._repair_complete: dict[str, bool] = {source: True for source in self.sources}
        self.raw_high_water = 0
        self.event_high_water = 0
        self.connection_attempts = 0
        self.reconnects = 0
        self.backfills = 0
        self.last_socket_error = ""
        self.last_socket_error_wall_ms = 0
        self._stopped = False

    async def start(self) -> None:
        if self._worker is not None:
            return
        self._worker = asyncio.create_task(
            self._process_frames(), name=f"source-{self.shard}-journal"
        )
        self._health_worker = asyncio.create_task(
            self._health_monitor(), name=f"source-{self.shard}-health"
        )
        for source in self.sources:
            await self._transition(source, StreamState.CONNECTING)

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._worker is not None and not self._worker.done():
            await self._raw.put(None)
        if self._worker is not None:
            await asyncio.gather(self._worker, return_exceptions=True)
        if self._health_worker is not None:
            self._health_worker.cancel()
            await asyncio.gather(self._health_worker, return_exceptions=True)
        for source in self.sources:
            await self._transition(source, StreamState.STOPPED)

    def background_tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(task for task in (self._worker, self._health_worker) if task is not None)

    def health_snapshot(self, source: str) -> tuple[StreamState, int]:
        partition = self._partition(source)
        return partition.state, partition.health_epoch

    def offer_frame(self, payload: Any) -> bool:
        """Stamp and enqueue a frame; this is the only socket-callback operation."""

        if self._stopped:
            return False

        frame = self._raw_frame(payload)
        try:
            self._raw.put_nowait(frame)
        except asyncio.QueueFull:
            for partition in self.partitions.values():
                if partition.state is not StreamState.GAP:
                    partition.health_epoch += 1
                partition.state = StreamState.GAP
                partition.overflow_count += 1
                partition.gap_detail = "source raw-frame queue overflow"
                try:
                    self.journal.offer(
                        "set_stream_partition_state",
                        partition_key=self._partition_key(partition.source),
                        stream_state=StreamState.GAP.value,
                        generation=self.generation,
                        gap_detail=partition.gap_detail,
                        last_frame_wall_ms=frame.receive_wall_ms,
                    )
                except JournalQueueFull:
                    pass
            self._reconnect_requested.set()
            return False
        self._record_raw_enqueued(frame)
        return True

    async def _enqueue_convergence_frame(self, payload: Any) -> None:
        """Apply bounded backpressure while publishing a finite reconnect batch."""

        if self._stopped:
            raise RuntimeError("source gateway stopped during reconnect convergence")
        frame = self._raw_frame(payload)
        await self._raw.put(frame)
        self._record_raw_enqueued(frame)

    def _raw_frame(self, payload: Any) -> _RawFrame:
        source_epochs = tuple(
            (source, self.partitions[source].health_epoch)
            for source in _frame_sources(payload)
            if source in self.partitions
        )
        return _RawFrame(
            payload=payload,
            receive_wall_ms=time_ns() // 1_000_000,
            receive_mono_ns=monotonic_ns(),
            source_epochs=source_epochs,
        )

    def _record_raw_enqueued(self, frame: _RawFrame) -> None:
        for source, _epoch in frame.source_epochs:
            named_partition = self.partitions.get(source)
            if named_partition is not None:
                named_partition.last_frame_wall_ms = frame.receive_wall_ms
                named_partition.pending_raw_frames += 1
        self.raw_high_water = max(self.raw_high_water, self._raw.qsize())

    async def run_socket(
        self,
        ws_url: str,
        *,
        backfill: Callable[[str], Awaitable[tuple[bool, list[Any]]]],
        reconnect_min_ms: int = 250,
        reconnect_max_ms: int = 5_000,
    ) -> None:
        """Own the shard socket and converge each reconnect before returning LIVE."""

        delay_ms = reconnect_min_ms
        first_connection = True
        while not self._stopped:
            self.connection_attempts += 1
            if not first_connection:
                self.reconnects += 1
            overlap = not first_connection
            await self.connection_budget.open_connection(self.owner, overlap=overlap)
            try:
                async with connect_websocket_ipv6_preferred(
                    ws_url,
                    ping_interval=None,
                ) as ws:
                    self._reconnect_requested.clear()
                    self._first_channels.clear()
                    for source in self.sources:
                        partition = self.partitions[source]
                        partition.acknowledged.clear()
                        partition.repair_barrier_seen = False
                        partition.repair_target_cursor = partition.ingress_cursor
                        partition.baseline_target_cursor = partition.ingress_cursor
                        self._baseline_channels[source].clear()
                        self._repair_complete[source] = False
                        await self._transition(source, StreamState.SNAPSHOT)
                        for subscription_type in self.SUBSCRIPTION_TYPES:
                            subscription: dict[str, Any] = {
                                "type": subscription_type,
                                "user": source,
                            }
                            if subscription_type == "userFills":
                                subscription["aggregateByTime"] = False
                            key = self._subscription_key(subscription)
                            await self.connection_budget.add_subscription(
                                self.owner, key, user=source
                            )
                            await self.connection_budget.record_outbound(time_ns() // 1_000_000)
                            await ws.send(
                                json.dumps(
                                    {"method": "subscribe", "subscription": subscription},
                                    separators=(",", ":"),
                                )
                            )
                    repair_frames: dict[str, list[Any]] = {}
                    incomplete_sources: list[str] = []
                    for source in self.sources:
                        await self._transition(source, StreamState.REPLAYING)
                        self.backfills += 1
                        complete, frames = await backfill(source)
                        self._repair_complete[source] = complete
                        repair_frames[source] = list(frames)
                        if not complete:
                            incomplete_sources.append(source)
                    if incomplete_sources:
                        detail = "copy-trigger history could not prove its durable anchor"
                        for source in self.sources:
                            self._repair_complete[source] = False
                            await self._transition(
                                source,
                                StreamState.GAP,
                                gap_detail=detail,
                            )
                        raise RuntimeError(detail + ": " + ",".join(sorted(incomplete_sources)))
                    socket_acknowledgements: dict[str, set[str]] = {
                        source: set() for source in self.sources
                    }
                    socket_baselines: dict[str, set[str]] = {
                        source: set() for source in self.sources
                    }
                    buffered_socket_frames: list[Any] = []
                    convergence_published = False
                    delay_ms = reconnect_min_ms
                    first_connection = False
                    while not self._stopped:
                        if self._reconnect_requested.is_set():
                            self._reconnect_requested.clear()
                            raise RuntimeError("source partition requested reconnect/backfill")
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(), timeout=self.heartbeat_ms / 1000
                            )
                        except asyncio.TimeoutError:
                            now_wall_ms = time_ns() // 1_000_000
                            stale_after_ms = max(self.heartbeat_ms * 2, 60_000)
                            stale_sources = [
                                source
                                for source, partition in self.partitions.items()
                                if partition.last_frame_wall_ms <= 0
                                or now_wall_ms - partition.last_frame_wall_ms >= stale_after_ms
                            ]
                            if stale_sources:
                                for source in stale_sources:
                                    await self._transition(
                                        source,
                                        StreamState.STALE,
                                        gap_detail=(
                                            f"per-source stream silent for {stale_after_ms}ms"
                                        ),
                                    )
                                raise RuntimeError(
                                    "per-source stream stale: " + ",".join(stale_sources)
                                )
                            await self.connection_budget.record_outbound(time_ns() // 1_000_000)
                            await ws.ping()
                            continue
                        try:
                            decoded = json.loads(raw)
                        except (TypeError, json.JSONDecodeError):
                            decoded = raw
                        if convergence_published:
                            if not self.offer_frame(decoded):
                                raise RuntimeError("source raw-frame queue overflow")
                            continue
                        channel = (
                            str(decoded.get("channel") or "")
                            if isinstance(decoded, Mapping)
                            else ""
                        )
                        data = decoded.get("data") if isinstance(decoded, Mapping) else None
                        frame_sources = _frame_sources(decoded)
                        is_control_frame = False
                        if channel == "subscriptionResponse" and isinstance(data, Mapping):
                            subscription = data.get("subscription", data)
                            if isinstance(subscription, Mapping):
                                source = str(subscription.get("user") or "").lower()
                                subscription_type = str(subscription.get("type") or "")
                                if source in socket_acknowledgements and subscription_type:
                                    socket_acknowledgements[source].add(subscription_type)
                                    is_control_frame = True
                        if channel in {"allDexsClearinghouseState", "spotState"}:
                            for source in frame_sources:
                                if source in socket_baselines:
                                    socket_baselines[source].add(channel)
                                    is_control_frame = True
                        if is_control_frame:
                            await self._enqueue_convergence_frame(decoded)
                        else:
                            if len(buffered_socket_frames) >= self._raw.maxsize:
                                raise RuntimeError(
                                    "source reconnect convergence buffer exceeded its bound"
                                )
                            buffered_socket_frames.append(decoded)
                        if not all(
                            socket_acknowledgements[source]
                            == self.partitions[source].required_acknowledgements
                            and {"allDexsClearinghouseState", "spotState"}.issubset(
                                socket_baselines[source]
                            )
                            for source in self.sources
                        ):
                            continue
                        # Baselines are queued first. Fill continuity then makes
                        # repair idempotent whether the baseline is pre- or
                        # post-fill, and buffered live overlap follows repair.
                        for source in self.sources:
                            for frame in repair_frames[source]:
                                await self._enqueue_convergence_frame(frame)
                        for frame in buffered_socket_frames:
                            await self._enqueue_convergence_frame(frame)
                        buffered_socket_frames.clear()
                        for source in self.sources:
                            await self._enqueue_convergence_frame(
                                {"__source_repair_barrier__": source}
                            )
                        convergence_published = True
            except Exception as exc:
                # A frame can race with an intentional containment stop.  A
                # stopped gateway is not a queue overflow or reconnect fault;
                # preserve the last real socket error and finish cleanly.
                if self._stopped:
                    return
                self.last_socket_error = f"{type(exc).__name__}: {exc}"[:500]
                self.last_socket_error_wall_ms = time_ns() // 1_000_000
            finally:
                await self.connection_budget.close_connection(self.owner)
            if self._stopped:
                return
            for source in self.sources:
                await self._transition(
                    source,
                    StreamState.REPLAYING,
                    gap_detail=self.last_socket_error,
                )
            await asyncio.sleep(delay_ms / 1000)
            delay_ms = min(reconnect_max_ms, max(reconnect_min_ms, delay_ms * 2))

    async def _health_monitor(self) -> None:
        interval_ms = max(250, min(5_000, self.heartbeat_ms // 2))
        stale_after_ms = max(self.heartbeat_ms * 2, 60_000)
        while not self._stopped:
            await asyncio.sleep(interval_ms / 1000)
            now_wall_ms = time_ns() // 1_000_000
            for source, partition in self.partitions.items():
                if partition.state is not StreamState.LIVE:
                    continue
                if (
                    partition.last_frame_wall_ms <= 0
                    or now_wall_ms - partition.last_frame_wall_ms >= stale_after_ms
                ):
                    await self._transition(
                        source,
                        StreamState.STALE,
                        gap_detail=f"per-source stream silent for {stale_after_ms}ms",
                    )

    async def mark_reconciled_live(self, source: str) -> None:
        partition = self._partition(source)
        if partition.acknowledged != partition.required_acknowledgements:
            raise RuntimeError("source subscriptions are not fully acknowledged")
        if partition.state is StreamState.GAP:
            raise RuntimeError("a GAP partition cannot be made live without proven repair")
        await self._transition(partition.source, StreamState.LIVE)

    def restore_partition(
        self,
        *,
        source: str,
        ingress_cursor: int,
        applied_cursor: int,
        last_valid_event_wall_ms: int,
        last_durable_checkpoint_wall_ms: int,
    ) -> None:
        partition = self._partition(source)
        if applied_cursor < 0 or ingress_cursor < applied_cursor:
            raise ValueError("restored stream cursors are invalid")
        partition.ingress_cursor = ingress_cursor
        partition.applied_cursor = applied_cursor
        partition.last_valid_event_wall_ms = last_valid_event_wall_ms
        partition.last_durable_checkpoint_wall_ms = last_durable_checkpoint_wall_ms

    async def commit_disposition(
        self,
        event: SourceStreamEvent,
        *,
        result_id: str,
        disposition: str,
        result_payload: Any,
        source_event_keys: tuple[str, ...] = (),
    ) -> None:
        await self.journal.submit(
            "commit_runtime_disposition",
            partition_key=event.partition_key,
            through_ingress_seq=event.ingress_seq,
            result_id=result_id,
            disposition=disposition,
            result_payload=result_payload,
            source_event_keys=source_event_keys,
        )
        partition = self._partition(event.source)
        partition.applied_cursor = event.ingress_seq
        partition.last_durable_checkpoint_wall_ms = time_ns() // 1_000_000

    def mark_applied(self, event: SourceStreamEvent) -> None:
        partition = self._partition(event.source)
        if event.ingress_seq <= partition.applied_cursor:
            raise RuntimeError("source in-memory applied cursor is not contiguous")
        partition.applied_cursor = event.ingress_seq
        partition.last_durable_checkpoint_wall_ms = time_ns() // 1_000_000
        try:
            asyncio.get_running_loop().create_task(self._promote_reconciled_partitions())
        except RuntimeError:
            pass

    async def _process_frames(self) -> None:
        while True:
            frame = await self._raw.get()
            try:
                if frame is None:
                    return
                if any(
                    source not in self.partitions
                    or self.partitions[source].health_epoch != frame_epoch
                    for source, frame_epoch in frame.source_epochs
                ):
                    # Undurable control/data from a superseded socket epoch
                    # cannot satisfy the replacement connection. Backfill is
                    # responsible for recovering any omitted source facts.
                    continue
                if (
                    isinstance(frame.payload, Mapping)
                    and frame.payload.get("__source_repair_barrier__") in self.partitions
                ):
                    source = str(frame.payload["__source_repair_barrier__"])
                    partition = self._partition(source)
                    partition.repair_target_cursor = partition.ingress_cursor
                    partition.repair_barrier_seen = True
                    await self._promote_reconciled_partitions()
                    continue
                try:
                    parsed_events = self._parse_frame(frame)
                except SourceFrameContinuityError as exc:
                    await self._transition(
                        exc.source,
                        StreamState.GAP,
                        gap_detail=exc.detail,
                    )
                    continue
                for parsed in parsed_events:
                    source, event_class, exchange_ms, payload, seed_snapshot, event_key = parsed
                    partition = self._partition(source)
                    partition_key = self._partition_key(source)
                    ingress_seq, inserted = await self.journal.submit(
                        "append_runtime_event",
                        event_key=event_key,
                        partition_key=partition_key,
                        event_class=event_class,
                        exchange_ts_ms=exchange_ms,
                        receive_wall_ms=frame.receive_wall_ms,
                        receive_mono_ns=frame.receive_mono_ns,
                        payload=payload,
                        stream_state=partition.state.value,
                        generation=self.generation,
                        seed_snapshot=seed_snapshot,
                    )
                    ingress_commit_mono_ns = monotonic_ns()
                    if not inserted:
                        partition.duplicates += 1
                        continue
                    partition.ingress_cursor = ingress_seq
                    self.journal.offer(
                        "record_stage_timing",
                        timing_id="timing-"
                        + sha256(f"{event_key}|durable-ingress".encode("utf-8")).hexdigest()[:32],
                        generation=self.generation,
                        source_shard=self.shard,
                        slot_id=self.slot_by_source.get(source, ""),
                        event_key=event_key,
                        stage="local_receive_to_durable_ingress_commit",
                        wall_ms=time_ns() // 1_000_000,
                        mono_ns=ingress_commit_mono_ns,
                        duration_ns=max(0, ingress_commit_mono_ns - frame.receive_mono_ns),
                        excluded_reason="replay_or_seed" if seed_snapshot else "",
                        payload={"partition_key": partition_key},
                    )
                    if event_class in {"source_account_state", "source_spot_state"}:
                        partition.baseline_target_cursor = max(
                            partition.baseline_target_cursor, ingress_seq
                        )
                    partition.last_valid_event_wall_ms = frame.receive_wall_ms
                    age_ms = frame.receive_wall_ms - exchange_ms
                    direct_eligible = (
                        partition.state is StreamState.LIVE
                        and not seed_snapshot
                        and frame.receive_mono_ns >= partition.live_since_mono_ns
                        and 0 <= age_ms <= self.direct_max_age_ms
                    )
                    if not seed_snapshot and age_ms > self.direct_max_age_ms:
                        partition.stale_direct_events += 1
                    event = SourceStreamEvent(
                        event_key=event_key,
                        partition_key=partition_key,
                        source=source,
                        event_class=event_class,
                        exchange_ts_ms=exchange_ms,
                        receive_wall_ms=frame.receive_wall_ms,
                        receive_mono_ns=frame.receive_mono_ns,
                        ingress_seq=ingress_seq,
                        payload=payload,
                        direct_eligible=direct_eligible,
                        seed_snapshot=seed_snapshot,
                        stream_state=partition.state,
                        health_epoch=partition.health_epoch,
                    )
                    try:
                        self.events.put_nowait(event)
                    except asyncio.QueueFull:
                        await self._transition(
                            source,
                            StreamState.GAP,
                            gap_detail="durable source event output queue overflow",
                        )
                        raise RuntimeError("durable source event output queue overflow")
                    self.event_high_water = max(self.event_high_water, self.events.qsize())
                await self._promote_reconciled_partitions()
            finally:
                if frame is not None:
                    for source, _epoch in frame.source_epochs:
                        pending_partition = self.partitions.get(source)
                        if pending_partition is not None:
                            pending_partition.pending_raw_frames = max(
                                0, pending_partition.pending_raw_frames - 1
                            )
                self._raw.task_done()
                if frame is not None:
                    await self._promote_reconciled_partitions()

    def _parse_frame(
        self, frame: _RawFrame
    ) -> list[tuple[str, str, int, Mapping[str, Any], bool, str]]:
        message = frame.payload
        if not isinstance(message, Mapping):
            return []
        channel = str(message.get("channel") or "")
        data = message.get("data")
        if channel == "subscriptionResponse" and isinstance(data, Mapping):
            subscription = data.get("subscription", data)
            if isinstance(subscription, Mapping):
                source = str(subscription.get("user") or "").lower()
                subscription_type = str(subscription.get("type") or "")
                if source in self.partitions and subscription_type:
                    self.partitions[source].acknowledged.add(subscription_type)
            return []
        if not isinstance(data, Mapping):
            return []
        source = str(data.get("user") or data.get("userAddress") or "").lower()
        if source not in self.partitions:
            return []
        if channel in {"allDexsClearinghouseState", "spotState"}:
            self._baseline_channels[source].add(channel)
        first_channel = (source, channel) not in self._first_channels
        self._first_channels.add((source, channel))
        seed_snapshot = data.get("isSnapshot") is True or (
            first_channel
            and channel
            in {
                "allDexsClearinghouseState",
                "spotState",
                "clearinghouseState",
                "openOrders",
                "webData3",
                "twapStates",
            }
        )
        if channel == "userFills":
            fills = data.get("fills", [])
            if not isinstance(fills, list):
                raise SourceFrameContinuityError(source, "userFills payload is not a list")
            fill_result: list[tuple[str, str, int, Mapping[str, Any], bool, str]] = []
            for fill in fills:
                if not isinstance(fill, Mapping):
                    raise SourceFrameContinuityError(source, "userFills row is malformed")
                exchange_ms = _event_time_ms(fill, frame.receive_wall_ms)
                try:
                    coin = canonical_market_symbol(str(fill.get("coin") or ""))
                except ValueError as exc:
                    raise SourceFrameContinuityError(
                        source, "userFills row has an invalid canonical market"
                    ) from exc
                tid = str(fill.get("tid") or "")
                if not tid:
                    raise SourceFrameContinuityError(
                        source, "userFills row is missing its trade identity"
                    )
                dedupe = (source, exchange_ms, coin, tid)
                if dedupe in self._seen_fill_keys:
                    self._seen_fill_keys.move_to_end(dedupe)
                    continue
                self._seen_fill_keys[dedupe] = None
                while len(self._seen_fill_keys) > self._dedupe_capacity:
                    self._seen_fill_keys.popitem(last=False)
                canonical_payload = dict(fill)
                canonical_payload["coin"] = coin
                event_key = (
                    "fill-"
                    + sha256(f"{source}|{exchange_ms}|{coin}|{tid}".encode("utf-8")).hexdigest()
                )
                fill_result.append(
                    (source, "user_fill", exchange_ms, canonical_payload, seed_snapshot, event_key)
                )
            return fill_result
        if channel == "userEvents" and isinstance(data.get("fills"), list):
            return self._parse_frame(
                _RawFrame(
                    payload={
                        "channel": "userFills",
                        "data": {
                            "user": source,
                            "isSnapshot": seed_snapshot,
                            "fills": data["fills"],
                        },
                    },
                    receive_wall_ms=frame.receive_wall_ms,
                    receive_mono_ns=frame.receive_mono_ns,
                    source_epochs=frame.source_epochs,
                )
            )
        if channel == "userTwapSliceFills":
            raw_items = data.get("twapSliceFills")
            if raw_items is None:
                raw_items = [data]
            if not isinstance(raw_items, list):
                raise SourceFrameContinuityError(source, "userTwapSliceFills payload is not a list")
            result: list[tuple[str, str, int, Mapping[str, Any], bool, str]] = []
            for wrapper in raw_items:
                if not isinstance(wrapper, Mapping):
                    raise SourceFrameContinuityError(
                        source, "userTwapSliceFills wrapper is malformed"
                    )
                raw_fill = wrapper.get("fill", wrapper)
                if not isinstance(raw_fill, Mapping):
                    raise SourceFrameContinuityError(source, "userTwapSliceFills fill is malformed")
                try:
                    coin = canonical_market_symbol(str(raw_fill.get("coin") or ""))
                except ValueError as exc:
                    raise SourceFrameContinuityError(
                        source, "userTwapSliceFills row has an invalid canonical market"
                    ) from exc
                exchange_ms = _event_time_ms(raw_fill, frame.receive_wall_ms)
                raw_tid = raw_fill.get("tid")
                if raw_tid is not None and str(raw_tid):
                    identity = str(raw_tid)
                else:
                    raise SourceFrameContinuityError(
                        source, "userTwapSliceFills row is missing its trade identity"
                    )
                dedupe = (source, exchange_ms, coin, identity)
                if dedupe in self._seen_fill_keys:
                    self._seen_fill_keys.move_to_end(dedupe)
                    continue
                self._seen_fill_keys[dedupe] = None
                while len(self._seen_fill_keys) > self._dedupe_capacity:
                    self._seen_fill_keys.popitem(last=False)
                canonical_payload = dict(raw_fill)
                canonical_payload["coin"] = coin
                if wrapper.get("twapId") is not None:
                    canonical_payload["twapId"] = wrapper["twapId"]
                event_key = (
                    "twap-fill-"
                    + sha256(
                        f"{source}|{exchange_ms}|{coin}|{identity}".encode("utf-8")
                    ).hexdigest()
                )
                result.append(
                    (
                        source,
                        "source_twap_fill",
                        exchange_ms,
                        canonical_payload,
                        seed_snapshot,
                        event_key,
                    )
                )
            return result
        exchange_ms = _event_time_ms(data, frame.receive_wall_ms)
        event_class = {
            "allDexsClearinghouseState": "source_account_state",
            "spotState": "source_spot_state",
            "clearinghouseState": "source_default_state",
            "openOrders": "source_open_orders",
            "orderUpdates": "source_order_update",
            "userEvents": "source_user_event",
            "userFundings": "source_funding",
            "userNonFundingLedgerUpdates": "source_ledger",
            "userTwapHistory": "source_twap_history",
            "twapStates": "source_twap_state",
            "notification": "source_notification",
            "webData3": "source_webdata3_untrusted",
        }.get(channel, f"source_{channel or 'unknown'}")
        event_key = (
            "source-"
            + sha256(
                json.dumps(
                    {
                        "source": source,
                        "channel": channel,
                        "exchange_ms": exchange_ms,
                        "data": data,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        )
        return [(source, event_class, exchange_ms, dict(data), seed_snapshot, event_key)]

    async def _transition(self, source: str, state: StreamState, *, gap_detail: str = "") -> None:
        partition = self._partition(source)
        prior_state = partition.state
        # The epoch identifies a socket/continuity generation, not each
        # intermediate state label.  SNAPSHOT starts a new connection epoch;
        # GAP/STALE/STOPPED invalidate every queued event from the prior one.
        # REPLAYING -> RECONCILING -> LIVE therefore keeps one epoch so valid
        # live-overlap rows already in the slot queue cannot self-induce GAP.
        if state is not prior_state and state in {
            StreamState.SNAPSHOT,
            StreamState.GAP,
            StreamState.STALE,
            StreamState.STOPPED,
        }:
            partition.health_epoch += 1
        partition.state = state
        if state is StreamState.LIVE:
            partition.live_since_mono_ns = monotonic_ns()
        partition.gap_detail = gap_detail
        await self.journal.submit(
            "set_stream_partition_state",
            partition_key=self._partition_key(source),
            stream_state=state.value,
            generation=self.generation,
            gap_detail=gap_detail,
            last_frame_wall_ms=partition.last_frame_wall_ms,
        )
        if self.on_transition is not None:
            pending = self.on_transition(source, state, gap_detail)
            if pending is not None:
                await pending
        if state in {StreamState.GAP, StreamState.STALE} and prior_state in {
            StreamState.SNAPSHOT,
            StreamState.REPLAYING,
            StreamState.RECONCILING,
            StreamState.LIVE,
        }:
            self._reconnect_requested.set()

    async def _promote_reconciled_partitions(self) -> None:
        required_baseline = {"allDexsClearinghouseState", "spotState"}
        for source, partition in self.partitions.items():
            if partition.state not in {
                StreamState.SNAPSHOT,
                StreamState.REPLAYING,
                StreamState.RECONCILING,
            }:
                continue
            if partition.acknowledged != partition.required_acknowledgements:
                continue
            if not required_baseline.issubset(self._baseline_channels[source]):
                continue
            if not self._repair_complete[source]:
                await self._transition(
                    source,
                    StreamState.GAP,
                    gap_detail="copy-trigger repair did not prove its durable anchor",
                )
                continue
            if not partition.repair_barrier_seen:
                continue
            repair_target = max(partition.repair_target_cursor, partition.baseline_target_cursor)
            if partition.applied_cursor < repair_target:
                continue
            if partition.state is not StreamState.RECONCILING:
                await self._transition(source, StreamState.RECONCILING)
            # The RECONCILING transition is an await point. Recheck the full
            # current-epoch local backlog immediately before LIVE; _transition
            # sets state before its first await, so no frame can interleave
            # between this proof and the LIVE state assignment.
            if partition.pending_raw_frames != 0:
                continue
            if partition.applied_cursor != partition.ingress_cursor:
                continue
            await self._transition(source, StreamState.LIVE)

    def _partition(self, source: str) -> PartitionHealth:
        key = source.lower()
        if key not in self.partitions:
            raise KeyError(f"source {source} is not owned by shard {self.shard}")
        return self.partitions[key]

    def _partition_key(self, source: str) -> str:
        return f"source:{self.shard}:{source.lower()}"

    @staticmethod
    def _subscription_key(subscription: Mapping[str, Any]) -> str:
        return json.dumps(subscription, sort_keys=True, separators=(",", ":"))


MARKET_SUBSCRIPTION_CONTROL_HEADROOM = 20


def active_market_subscription_capacity(
    *,
    unique_source_count: int,
    subscription_limit: int = 1_000,
    control_headroom: int = MARKET_SUBSCRIPTION_CONTROL_HEADROOM,
) -> int:
    """Return the one frozen active-union cap used by plan, benchmark and runtime.

    Each source owns the complete native user-subscription set.  Market capacity
    is then budgeted against the more expensive selectable context strategy
    (activeAssetCtx plus l2Book), with explicit reconnect/control headroom.
    Risk limits such as max-open-positions intentionally do not participate in
    this resource calculation.
    """

    if unique_source_count < 1 or subscription_limit < 1 or control_headroom < 0:
        raise ValueError("active-market subscription capacity inputs are invalid")
    fixed = unique_source_count * len(SourceStreamGateway.SUBSCRIPTION_TYPES) + control_headroom
    capacity = (subscription_limit - fixed) // 2
    if capacity < 1:
        raise ValueError("source subscriptions leave no active-market capacity")
    return capacity


def _frame_sources(payload: Any) -> tuple[str, ...]:
    """Return only source partitions explicitly identified by a shard frame."""

    if not isinstance(payload, Mapping):
        return ()
    repair_source = payload.get("__source_repair_barrier__")
    if isinstance(repair_source, str) and repair_source:
        return (repair_source.lower(),)
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return ()
    subscription = data.get("subscription", data)
    candidates = [data.get("user"), data.get("userAddress")]
    if isinstance(subscription, Mapping):
        candidates.extend([subscription.get("user"), subscription.get("userAddress")])
    return tuple(
        dict.fromkeys(
            str(candidate).lower()
            for candidate in candidates
            if isinstance(candidate, str) and candidate
        )
    )


def _event_time_ms(payload: Mapping[str, Any], fallback: int) -> int:
    for key in ("time", "timestamp", "T"):
        value = payload.get(key)
        if isinstance(value, int) and value > 0:
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return fallback


@dataclass(frozen=True, slots=True)
class MarketFrame:
    channel: str
    key: str
    payload: Any
    receive_wall_ms: int
    receive_mono_ns: int
    connection_epoch: int


@dataclass(slots=True)
class RequiredMarketStreamLiveness:
    key: str
    category: str
    policy_limit_ms: float
    required: bool = False
    activation_count: int = 0
    deactivation_count: int = 0
    zero_frame_ended_count: int = 0
    overlimit_zero_frame_count: int = 0
    required_since_mono_ns: int = 0
    required_since_wall_ms: int = 0
    first_valid_receive_mono_ns: int = 0
    last_valid_receive_mono_ns: int = 0
    last_valid_receive_wall_ms: int = 0
    validated_frame_count: int = 0
    current_activation_validated_frame_count: int = 0
    maximum_silence_ms: float = 0.0
    violation_latched: bool = False
    last_valid_receive_epoch: int = 0


class MarketDataGateway:
    """One connection owner; only superseded full snapshots may coalesce."""

    def __init__(
        self,
        *,
        capacity: int,
        context_strategy: str = CONTEXT_STRATEGY_ALL_DEXS,
        subscription_queue_capacity: int = 1_024,
    ) -> None:
        if capacity < 1 or subscription_queue_capacity < 1:
            raise ValueError("market queue capacity must be positive")
        if context_strategy not in MARKET_CONTEXT_STRATEGIES:
            raise ValueError("market context strategy is invalid")
        self.capacity = capacity
        self.context_strategy = context_strategy
        self.frames: asyncio.Queue[MarketFrame] = asyncio.Queue(capacity)
        self.coalesced = 0
        self.overflow = 0
        self.queue_high_water = 0
        self.connection_attempts = 0
        self.reconnects = 0
        self.active_books: set[str] = set()
        self._coalescible: dict[tuple[str, str], MarketFrame] = {}
        self._subscription_changes: asyncio.Queue[tuple[str, bool]] = asyncio.Queue(
            subscription_queue_capacity
        )
        self._stopped = False
        self.connected = asyncio.Event()
        self.ready = asyncio.Event()
        self.last_error = ""
        self.connection_epoch = 0
        self.aggregate_context_epoch = 0
        self.context_epoch_by_market: dict[str, int] = {}
        self.book_epoch_by_market: dict[str, int] = {}
        self._applied_mono_by_key: dict[tuple[int, str, str], int] = {}
        self._stream_liveness: dict[str, RequiredMarketStreamLiveness] = {}

    def set_market_active(self, market: str, active: bool) -> None:
        canonical = canonical_market_symbol(market)
        changed = (canonical in self.active_books) != active
        if active:
            self.active_books.add(canonical)
        else:
            self.active_books.discard(canonical)
        if changed:
            # A same-connection deactivate/reactivate cycle must earn fresh
            # context/book readiness; a prior epoch marker cannot qualify it.
            self.book_epoch_by_market.pop(canonical, None)
            if self.context_strategy == CONTEXT_STRATEGY_ACTIVE_MARKETS:
                self.context_epoch_by_market.pop(canonical, None)
            self._set_market_streams_required(canonical, active)
            try:
                self._subscription_changes.put_nowait((canonical, active))
            except asyncio.QueueFull as exc:
                raise RuntimeError("market subscription control queue overflow") from exc

    def offer(self, channel: str, key: str, payload: Any, *, full_snapshot: bool) -> bool:
        frame = MarketFrame(
            channel=channel,
            key=key,
            payload=payload,
            receive_wall_ms=time_ns() // 1_000_000,
            receive_mono_ns=monotonic_ns(),
            connection_epoch=self.connection_epoch,
        )
        if full_snapshot:
            coalesce_key = (channel, key)
            if coalesce_key in self._coalescible:
                self._coalescible[coalesce_key] = frame
                self.coalesced += 1
                return True
            self._coalescible[coalesce_key] = frame
        try:
            self.frames.put_nowait(frame)
        except asyncio.QueueFull:
            self.overflow += 1
            if full_snapshot:
                self._coalescible.pop((channel, key), None)
            return False
        self.queue_high_water = max(self.queue_high_water, self.frames.qsize())
        return True

    def consumed(self, frame: MarketFrame) -> MarketFrame:
        replacement = self._coalescible.pop((frame.channel, frame.key), None)
        self.frames.task_done()
        return frame if replacement is None else replacement

    async def run_socket(
        self,
        ws_url: str,
        *,
        budget: ConnectionBudget,
        reconnect_min_ms: int = 250,
        reconnect_max_ms: int = 5_000,
        heartbeat_ms: int = 30_000,
    ) -> None:
        owner = "market-data"
        delay = reconnect_min_ms
        while not self._stopped:
            self.connection_attempts += 1
            if self.connection_attempts > 1:
                self.reconnects += 1
            await budget.open_connection(owner)
            try:
                async with connect_websocket_ipv6_preferred(
                    ws_url,
                    ping_interval=heartbeat_ms / 1_000,
                ) as socket:
                    self.connection_epoch += 1
                    self.connected.set()
                    self.ready.clear()
                    if self.context_strategy == CONTEXT_STRATEGY_ALL_DEXS:
                        aggregate = {"type": "allDexsAssetCtxs"}
                        await budget.add_subscription(
                            owner,
                            json.dumps(aggregate, sort_keys=True, separators=(",", ":")),
                        )
                        await budget.record_outbound(time_ns() // 1_000_000)
                        # The desired-required boundary precedes the send so
                        # send, ACK, and reconnect delay all remain observable.
                        self._set_stream_required(
                            "allDexsAssetCtxs",
                            category="all_dex_context",
                            policy_limit_ms=ALL_DEXS_CONTEXT_MAX_SILENCE_MS,
                            required=True,
                        )
                        await socket.send(
                            json.dumps(
                                {"method": "subscribe", "subscription": aggregate},
                                separators=(",", ":"),
                            )
                        )
                    for market in sorted(self.active_books):
                        await self._send_market_subscriptions(
                            socket, budget=budget, owner=owner, market=market, subscribe=True
                        )
                    if self.context_strategy == CONTEXT_STRATEGY_ACTIVE_MARKETS:
                        # Entry admission remains market-locally NO_CONTEXT until
                        # each subscription produces its own validated frame.
                        self.ready.set()
                    delay = reconnect_min_ms
                    receive_task = asyncio.create_task(socket.recv())
                    control_task = asyncio.create_task(self._subscription_changes.get())
                    try:
                        while not self._stopped:
                            done, _ = await asyncio.wait(
                                {receive_task, control_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if receive_task in done:
                                raw = receive_task.result()
                                try:
                                    message = json.loads(raw)
                                except (TypeError, json.JSONDecodeError):
                                    message = None
                                if isinstance(message, Mapping):
                                    channel = str(message.get("channel") or "")
                                    if channel == "subscriptionResponse":
                                        response = message.get("data")
                                        subscription = (
                                            response.get("subscription", response)
                                            if isinstance(response, Mapping)
                                            else None
                                        )
                                        if isinstance(subscription, Mapping) and (
                                            subscription.get("type") == "allDexsAssetCtxs"
                                            or self.context_strategy
                                            == CONTEXT_STRATEGY_ACTIVE_MARKETS
                                            and subscription.get("type")
                                            in {"activeAssetCtx", "l2Book"}
                                        ):
                                            self.ready.set()
                                    if channel not in {"subscriptionResponse", "pong"}:
                                        data = message.get("data")
                                        key = _market_frame_key(channel, data)
                                        accepted = self.offer(
                                            channel,
                                            key,
                                            data,
                                            full_snapshot=channel
                                            in {
                                                "allDexsAssetCtxs",
                                                "activeAssetCtx",
                                                "l2Book",
                                            },
                                        )
                                        if not accepted:
                                            raise RuntimeError(
                                                "market data queue overflow; reconnect/reconcile required"
                                            )
                                receive_task = asyncio.create_task(socket.recv())
                            if control_task in done:
                                market, subscribe = control_task.result()
                                self._subscription_changes.task_done()
                                await self._send_market_subscriptions(
                                    socket,
                                    budget=budget,
                                    owner=owner,
                                    market=market,
                                    subscribe=subscribe,
                                )
                                control_task = asyncio.create_task(self._subscription_changes.get())
                    finally:
                        receive_task.cancel()
                        control_task.cancel()
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
                if self._stopped:
                    return
            finally:
                self.connected.clear()
                self.ready.clear()
                await budget.close_connection(owner)
            await asyncio.sleep(delay / 1000)
            delay = min(reconnect_max_ms, max(reconnect_min_ms, delay * 2))

    async def stop(self) -> None:
        self._stopped = True

    def mark_context_applied(self, frame: MarketFrame) -> None:
        if frame.connection_epoch != self.connection_epoch:
            return
        if frame.channel == "allDexsAssetCtxs":
            recorded = self._record_stream_validated("allDexsAssetCtxs", frame)
            if recorded is not False:
                self.aggregate_context_epoch = frame.connection_epoch
        elif frame.channel == "activeAssetCtx":
            market = canonical_market_symbol(frame.key)
            recorded = self._record_stream_validated(f"activeAssetCtx:{market}", frame)
            if recorded is not False:
                self.context_epoch_by_market[market] = frame.connection_epoch

    def accept_for_apply(self, frame: MarketFrame) -> bool:
        if frame.connection_epoch <= 0 or frame.connection_epoch != self.connection_epoch:
            return False
        identity = (frame.connection_epoch, frame.channel, frame.key)
        prior_mono = self._applied_mono_by_key.get(identity, 0)
        if frame.receive_mono_ns <= prior_mono:
            return False
        self._applied_mono_by_key[identity] = frame.receive_mono_ns
        return True

    def mark_book_applied(self, frame: MarketFrame, market: str) -> None:
        if frame.connection_epoch == self.connection_epoch:
            canonical = canonical_market_symbol(market)
            recorded = self._record_stream_validated(f"l2Book:{canonical}", frame)
            if recorded is not False:
                self.book_epoch_by_market[canonical] = frame.connection_epoch

    def current_epoch_ready_for(self, market: str) -> bool:
        canonical = canonical_market_symbol(market)
        context_epoch = (
            self.aggregate_context_epoch
            if self.context_strategy == CONTEXT_STRATEGY_ALL_DEXS
            else self.context_epoch_by_market.get(canonical, 0)
        )
        return bool(
            self.connection_epoch > 0
            and self.connected.is_set()
            and self.ready.is_set()
            and context_epoch == self.connection_epoch
            and self.book_epoch_by_market.get(canonical, 0) == self.connection_epoch
        )

    def market_feed_liveness(self, *, now_mono_ns: int | None = None) -> dict[str, Any]:
        now_ns = monotonic_ns() if now_mono_ns is None else now_mono_ns
        by_stream: dict[str, dict[str, Any]] = {}
        violating_keys: list[str] = []
        incomplete_keys: list[str] = []
        for key, state in sorted(self._stream_liveness.items()):
            current_age_ms: float | None = None
            first_delay_ms: float | None = None
            if state.required:
                boundary_ns = state.last_valid_receive_mono_ns or state.required_since_mono_ns
                current_age_ms = max(0.0, (now_ns - boundary_ns) / 1_000_000)
                state.maximum_silence_ms = max(state.maximum_silence_ms, current_age_ms)
                if current_age_ms > state.policy_limit_ms:
                    state.violation_latched = True
                if state.current_activation_validated_frame_count == 0:
                    incomplete_keys.append(key)
            if state.first_valid_receive_mono_ns > 0 and state.required_since_mono_ns > 0:
                first_delay_ms = max(
                    0.0,
                    (state.first_valid_receive_mono_ns - state.required_since_mono_ns) / 1_000_000,
                )
            if state.violation_latched:
                violating_keys.append(key)
            by_stream[key] = {
                "category": state.category,
                "required": state.required,
                "policy_limit_ms": state.policy_limit_ms,
                "activation_count": state.activation_count,
                "deactivation_count": state.deactivation_count,
                "zero_frame_ended_count": state.zero_frame_ended_count,
                "overlimit_zero_frame_count": state.overlimit_zero_frame_count,
                "validated_frame_count": state.validated_frame_count,
                "current_activation_validated_frame_count": (
                    state.current_activation_validated_frame_count
                ),
                "first_valid_receive_delay_ms": first_delay_ms,
                "current_age_ms": current_age_ms,
                "maximum_silence_ms": state.maximum_silence_ms,
                "violation_latched": state.violation_latched,
                "required_since_mono_ns": state.required_since_mono_ns,
                "required_since_wall_ms": state.required_since_wall_ms,
                "last_valid_receive_mono_ns": state.last_valid_receive_mono_ns,
                "last_valid_receive_wall_ms": state.last_valid_receive_wall_ms,
                "last_valid_receive_epoch": state.last_valid_receive_epoch,
            }
        return {
            "version": RUNTIME_FEED_LIVENESS_VERSION,
            "policy_version": RUNTIME_FEED_LIVENESS_POLICY_VERSION,
            "observation_mono_ns": now_ns,
            "context_strategy": self.context_strategy,
            "active_markets": sorted(self.active_books),
            "required_keys": sorted(
                key for key, state in self._stream_liveness.items() if state.required
            ),
            "violating_keys": violating_keys,
            "incomplete_keys": incomplete_keys,
            "violation_latched": bool(violating_keys),
            "by_stream": by_stream,
        }

    def _set_market_streams_required(self, market: str, required: bool) -> None:
        if self.context_strategy == CONTEXT_STRATEGY_ACTIVE_MARKETS:
            self._set_stream_required(
                f"activeAssetCtx:{market}",
                category="active_context",
                policy_limit_ms=ACTIVE_CONTEXT_MAX_SILENCE_MS,
                required=required,
            )
        self._set_stream_required(
            f"l2Book:{market}",
            category="book",
            policy_limit_ms=BOOK_MAX_SILENCE_MS,
            required=required,
        )

    def _set_stream_required(
        self,
        key: str,
        *,
        category: str,
        policy_limit_ms: float,
        required: bool,
    ) -> None:
        now_ns = monotonic_ns()
        now_wall_ms = time_ns() // 1_000_000
        state = self._stream_liveness.get(key)
        if state is None:
            state = RequiredMarketStreamLiveness(
                key=key,
                category=category,
                policy_limit_ms=policy_limit_ms,
            )
            self._stream_liveness[key] = state
        if (
            state.category != category
            or state.policy_limit_ms != policy_limit_ms
            or state.required == required
        ):
            if state.category != category or state.policy_limit_ms != policy_limit_ms:
                raise RuntimeError("market stream liveness policy changed in place")
            return
        if required:
            state.required = True
            state.activation_count += 1
            state.required_since_mono_ns = now_ns
            state.required_since_wall_ms = now_wall_ms
            state.first_valid_receive_mono_ns = 0
            state.last_valid_receive_mono_ns = 0
            state.last_valid_receive_wall_ms = 0
            state.current_activation_validated_frame_count = 0
            return
        boundary_ns = state.last_valid_receive_mono_ns or state.required_since_mono_ns
        terminal_silence_ms = max(0.0, (now_ns - boundary_ns) / 1_000_000)
        state.maximum_silence_ms = max(state.maximum_silence_ms, terminal_silence_ms)
        if terminal_silence_ms > state.policy_limit_ms:
            state.violation_latched = True
        if state.current_activation_validated_frame_count == 0:
            state.zero_frame_ended_count += 1
            if terminal_silence_ms > state.policy_limit_ms:
                state.overlimit_zero_frame_count += 1
        state.required = False
        state.deactivation_count += 1
        state.required_since_mono_ns = 0
        state.required_since_wall_ms = 0
        state.first_valid_receive_mono_ns = 0
        state.last_valid_receive_mono_ns = 0
        state.last_valid_receive_wall_ms = 0
        state.current_activation_validated_frame_count = 0

    def _record_stream_validated(self, key: str, frame: MarketFrame) -> bool | None:
        state = self._stream_liveness.get(key)
        if state is None:
            # Offline benchmarks don't own live subscription state.
            return None
        if (
            not state.required
            or frame.receive_mono_ns < state.required_since_mono_ns
            or frame.receive_wall_ms < state.required_since_wall_ms
        ):
            return False
        boundary_ns = state.last_valid_receive_mono_ns or state.required_since_mono_ns
        silence_ms = max(0.0, (frame.receive_mono_ns - boundary_ns) / 1_000_000)
        state.maximum_silence_ms = max(state.maximum_silence_ms, silence_ms)
        if silence_ms > state.policy_limit_ms:
            state.violation_latched = True
        if state.first_valid_receive_mono_ns <= 0:
            state.first_valid_receive_mono_ns = frame.receive_mono_ns
        state.last_valid_receive_mono_ns = frame.receive_mono_ns
        state.last_valid_receive_wall_ms = frame.receive_wall_ms
        state.validated_frame_count += 1
        state.current_activation_validated_frame_count += 1
        state.last_valid_receive_epoch = frame.connection_epoch
        return True

    async def _send_market_subscriptions(
        self,
        socket: Any,
        *,
        budget: ConnectionBudget,
        owner: str,
        market: str,
        subscribe: bool,
    ) -> None:
        subscription_types = ["l2Book"]
        if self.context_strategy == CONTEXT_STRATEGY_ACTIVE_MARKETS:
            subscription_types.insert(0, "activeAssetCtx")
        for subscription_type in subscription_types:
            subscription = {"type": subscription_type, "coin": market}
            key = json.dumps(subscription, sort_keys=True, separators=(",", ":"))
            if subscribe:
                await budget.add_subscription(owner, key)
            else:
                await budget.remove_subscription(owner, key)
            await budget.record_outbound(time_ns() // 1_000_000)
            await socket.send(
                json.dumps(
                    {
                        "method": "subscribe" if subscribe else "unsubscribe",
                        "subscription": subscription,
                    },
                    separators=(",", ":"),
                )
            )


def _market_frame_key(channel: str, data: Any) -> str:
    if isinstance(data, Mapping):
        return str(data.get("coin") or data.get("dex") or "fleet")
    return "fleet"
