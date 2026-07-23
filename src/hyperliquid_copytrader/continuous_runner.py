from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from time import monotonic, time_ns
from typing import Any, Awaitable, Protocol

import httpx

from .action_journal import ActionJournal
from .continuous_config import BoundContinuousPlan, bind_continuous_plan, load_continuous_plan
from .continuous_executor import ContinuousSignerLane
from .continuous_follower import WsFollowerInfo, catalog_position_dexes
from .continuous_network import ContinuousNetworkDriver, DurableSourceGapRepair, ReconnectPolicy
from .continuous_preflight import run_continuous_preflight
from .continuous_runtime import ContinuousRuntime
from .market_catalog import CatalogRevision, MarketReadiness, build_dynamic_catalog_revision
from .rest_throttle import (
    call_with_rest_backoff,
    info_rest_throttle_enabled_for_base_url,
    rest_request_weight,
)
from .websocket_transport import connect_websocket_ipv6_preferred
from .windows_runtime import atomic_json_write, verify_local_ntfs_runtime
from .ws_actions import PostOutcome, WsPostMux


ARM_TOKEN = "LIVE_CONTINUOUS"
STARTUP_HTTP_REQUESTS = 3
STARTUP_HTTP_WEIGHT = 60
CONTINUOUS_INFO_WEIGHT_PER_MINUTE = 720
CATALOG_REFRESH_S = 300.0
UNKNOWN_MARKET_REFRESH_COOLDOWN_S = 60.0
URLS = {
    "mainnet": (
        "https://api.hyperliquid.xyz",
        "wss://api.hyperliquid.xyz/ws",
    ),
    "testnet": (
        "https://api.hyperliquid-testnet.xyz",
        "wss://api.hyperliquid-testnet.xyz/ws",
    ),
}


class _StartupSocket(Protocol):
    async def send(self, message: str) -> Any: ...


def now_ms() -> int:
    return time_ns() // 1_000_000


class StartupInfo:
    """One-shot unsigned HTTP info lane; closed before continuous sockets start."""

    def __init__(self, base_url: str, *, timeout_s: float = 10.0) -> None:
        self.base_url, self.timeout_s = base_url, timeout_s
        self.count = 0
        self.logical_count = 0
        self.weight = 0

    def __enter__(self) -> StartupInfo:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def __call__(self, payload: dict[str, Any]) -> Any:
        self.logical_count += 1
        label = f"info:{str(payload.get('type') or 'unknown')}"
        weight = rest_request_weight(label)

        def request() -> Any:
            self.count += 1
            self.weight += weight
            return _http_info(self.base_url, payload, timeout_s=self.timeout_s)

        # These three catalog calls are idempotent and startup-critical. A
        # transient timeout gets a small bounded retry, while every wire attempt
        # remains rate-limited and visible in the startup budget.
        return call_with_rest_backoff(
            label,
            request,
            enabled=info_rest_throttle_enabled_for_base_url(self.base_url),
            weight=weight,
            attempts=3,
            backoff_ms=500,
        )


def _http_info(base_url: str, payload: Mapping[str, Any], *, timeout_s: float) -> Any:
    with httpx.Client(
        base_url=base_url.rstrip("/"),
        timeout=timeout_s,
        headers={"User-Agent": "hl-copytrader/continuous"},
    ) as client:
        response = client.post("/info", json=dict(payload))
        response.raise_for_status()
        return response.json()


def build_startup_catalog(
    info: StartupInfo,
    *,
    network: str,
    observed_ms: int | None = None,
    previous: CatalogRevision | None = None,
    retain_symbols: set[str] | frozenset[str] = frozenset(),
) -> CatalogRevision:
    observed = now_ms() if observed_ms is None else observed_ms
    before = info({"type": "perpDexs"})
    metas = info({"type": "allPerpMetas"})
    after = info({"type": "perpDexs"})
    return build_dynamic_catalog_revision(
        network=network,
        policy_version="continuous-ws-v1",
        sequence=1 if previous is None else previous.sequence + 1,
        observed_ms=observed,
        dexes_before_payload=before,
        all_perp_metas_payload=metas,
        dexes_after_payload=after,
        previous=previous,
        retain_symbols=retain_symbols,
    )


def load_durable_catalog(path: Path, *, network: str) -> CatalogRevision | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("catalog payload is not an object")
        catalog = CatalogRevision.from_payload(payload)
    except Exception as exc:
        raise RuntimeError("durable engine catalog is unreadable or invalid") from exc
    if catalog.network != network or catalog.policy_version != "continuous-ws-v1":
        raise RuntimeError("durable engine catalog belongs to a different runtime policy")
    return catalog


class _WsStartupInfo:
    """Synchronous audit callback bridged to one request-correlated WS mux."""

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        mux: WsPostMux,
        socket: _StartupSocket,
        epoch: int,
        timeout_s: float,
        attempts: int = 3,
        retry_delay_s: float = 0.5,
        weight_limit: int = 1_000,
        weight_window_s: float = 60.0,
        clock: Callable[[], float] = monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if (
            attempts < 1
            or retry_delay_s < 0
            or weight_limit < 1
            or weight_window_s <= 0
        ):
            raise ValueError("startup WS retry policy is invalid")
        self.loop, self.mux, self.socket, self.epoch = loop, mux, socket, epoch
        self.timeout_s = timeout_s
        self.attempts = attempts
        self.retry_delay_s = retry_delay_s
        self.weight_limit = weight_limit
        self.weight_window_s = weight_window_s
        self.clock = clock
        self.sleep = sleep
        self.count = 0
        self.logical_count = 0
        self.wire_weight = 0
        self._weight_events: deque[tuple[float, int]] = deque()
        self._last_ping = self.clock()

    def __call__(self, payload: dict[str, Any]) -> Any:
        self.logical_count += 1
        future = asyncio.run_coroutine_threadsafe(self._query(dict(payload)), self.loop)
        per_attempt = self.timeout_s + self.mux.write_timeout_s + 2.0
        retry_wait = self.retry_delay_s * sum(2**index for index in range(self.attempts - 1))
        pacing_wait = self.weight_window_s * 2
        return future.result(
            timeout=per_attempt * self.attempts + retry_wait + pacing_wait
        )

    async def _heartbeat_if_due(self) -> None:
        if self.clock() - self._last_ping < 30.0:
            return
        await asyncio.wait_for(
            self.socket.send('{"method":"ping"}'),
            timeout=self.mux.write_timeout_s,
        )
        self._last_ping = self.clock()

    async def _reserve_weight(self, weight: int) -> None:
        if weight > self.weight_limit:
            raise RuntimeError("startup WS info request exceeds its pacing envelope")
        while True:
            current = self.clock()
            cutoff = current - self.weight_window_s
            while self._weight_events and self._weight_events[0][0] <= cutoff:
                self._weight_events.popleft()
            used = sum(item[1] for item in self._weight_events)
            if used + weight <= self.weight_limit:
                self._weight_events.append((current, weight))
                self.wire_weight += weight
                return
            wait_s = max(
                0.001,
                self._weight_events[0][0] + self.weight_window_s - current,
            )
            # Preserve the startup WebSocket while deliberately crossing a
            # rolling-limit window; Hyperliquid closes idle sockets at 60 s.
            await self.sleep(min(wait_s, 25.0))
            await self._heartbeat_if_due()

    async def _query(self, payload: dict[str, Any]) -> Any:
        await self._heartbeat_if_due()
        expected = str(payload.get("type") or "")
        weight = rest_request_weight(f"info:{expected}")
        result = None
        for attempt in range(self.attempts):
            await self._reserve_weight(weight)
            self.count += 1
            result = await self.mux.post_info(
                payload,
                required_epoch=self.epoch,
                timeout_s=self.timeout_s,
            )
            if result.outcome is PostOutcome.INFO:
                break
            if attempt + 1 < self.attempts:
                # Startup information requests are read-only and idempotent.
                # A bounded retry absorbs the venue's observed transient
                # server-error bursts without weakening any startup proof.
                await self.sleep(self.retry_delay_s * 2**attempt)
        assert result is not None
        if result.outcome is not PostOutcome.INFO:
            raise RuntimeError(
                f"startup WS info {expected} was {result.outcome.value}: {result.reason} "
                f"after {self.attempts} attempts"
            )
        response = result.response
        if not isinstance(response, Mapping) or response.get("type") != "info":
            raise RuntimeError(f"startup WS info {expected} envelope is malformed")
        inner = response.get("payload")
        if not isinstance(inner, Mapping) or inner.get("type") != expected or "data" not in inner:
            raise RuntimeError(f"startup WS info {expected} payload is mismatched")
        return inner["data"]


async def run_ws_startup_preflight(
    bound: BoundContinuousPlan,
    *,
    network: str,
    ws_url: str,
    mux: WsPostMux,
    catalog: CatalogRevision,
    timeout_s: float = 5.0,
) -> dict[str, Any]:
    """Run bounded account/identity proof over WS; ordinary HTTP remains catalog-only."""

    async with connect_websocket_ipv6_preferred(
        ws_url,
        proxy=None,
        ping_interval=None,
        open_timeout=10,
        close_timeout=2,
        max_queue=1_024,
    ) as socket:
        epoch = mux.attach(socket)
        reader = asyncio.create_task(mux.receive_loop(epoch))
        audit: asyncio.Task[dict[str, Any]] | None = None
        try:
            bridge = _WsStartupInfo(
                loop=asyncio.get_running_loop(),
                mux=mux,
                socket=socket,
                epoch=epoch,
                timeout_s=timeout_s,
            )
            audit = asyncio.create_task(
                asyncio.to_thread(
                    run_continuous_preflight,
                    bound,
                    network=network,
                    info=bridge,
                    observed_ms=now_ms(),
                    require_flat_and_order_free=False,
                    require_open_orders=True,
                    audit_dexes=None,
                    catalog=catalog,
                )
            )
            done, _ = await asyncio.wait((audit, reader), return_when=asyncio.FIRST_COMPLETED)
            if reader in done and audit not in done:
                raise ConnectionError("startup WS info connection closed during preflight")
            report = await audit
            result = dict(report)
            result["transport"] = "websocket_post"
            result["info_requests"] = result.pop("rest_requests", {})
            result["ws_post_count"] = bridge.count
            result["ws_post_logical_count"] = bridge.logical_count
            result["ws_post_calculated_weight"] = bridge.wire_weight
            result["ws_post_weight_limit_per_minute"] = bridge.weight_limit
            return result
        finally:
            if audit is not None and not audit.done():
                audit.cancel()
            mux.detach(epoch, reason="startup WS preflight complete")
            reader.cancel()
            await asyncio.gather(
                reader,
                *(tuple() if audit is None else (audit,)),
                return_exceptions=True,
            )


def ensure_engine_identity(
    engine_state_dir: Path,
    *,
    network: str,
    runtime_id: str,
    plan_sha256: str,
    create: bool = True,
) -> Path:
    """Bind one durable engine directory to one exact plan identity."""

    expected = {
        "version": 1,
        "network": network,
        "runtime_id": runtime_id,
        "plan_sha256": plan_sha256,
    }
    path = engine_state_dir / "identity.json"
    if path.exists():
        try:
            observed = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"durable engine identity is unreadable: {exc}") from exc
        if observed != expected:
            raise RuntimeError("durable engine directory belongs to a different plan identity")
    elif create:
        atomic_json_write(path, expected)
    return path


class RecoveryHttpFills:
    """Optional HTTP recovery lane; never called during healthy continuous operation."""

    def __init__(
        self,
        base_url: str,
        metric: JsonlMetrics,
        *,
        minimum_interval_s: float = 30.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if minimum_interval_s <= 0:
            raise ValueError("recovery HTTP minimum interval must be positive")
        self.base_url, self.metric = base_url, metric
        self.minimum_interval_s = minimum_interval_s
        self.clock = clock
        self._lock = asyncio.Lock()
        self._next_by_user: dict[str, float] = {}

    async def __call__(self, *, user: str, start_ms: int, end_ms: int) -> list[Mapping[str, Any]]:
        canonical = user.lower()
        async with self._lock:
            current = self.clock()
            remaining = self._next_by_user.get(canonical, 0.0) - current
            if remaining > 0:
                self.metric.sink(
                    {
                        "event": "recovery_http_cooldown",
                        "wall_ms": now_ms(),
                        "user": _redact(user),
                        "remaining_ms": round(remaining * 1_000),
                    }
                )
                raise RuntimeError("HTTP fill recovery cooldown is active")
            # Set the cooldown before transport so a 429 or timeout cannot
            # become a five-second reconnect request storm.
            self._next_by_user[canonical] = current + self.minimum_interval_s
            self.metric.sink(
                {
                    "event": "recovery_http",
                    "wall_ms": now_ms(),
                    "user": _redact(user),
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            )
            request_payload = {
                "type": "userFillsByTime",
                "user": user,
                "startTime": start_ms,
                "endTime": end_ms,
                "aggregateByTime": False,
            }

            def fetch() -> Any:
                return call_with_rest_backoff(
                    "info:userFillsByTime",
                    lambda: _http_info(
                        self.base_url,
                        request_payload,
                        timeout_s=10.0,
                    ),
                    enabled=info_rest_throttle_enabled_for_base_url(self.base_url),
                    weight=rest_request_weight("info:userFillsByTime"),
                    attempts=1,
                )

            payload = await asyncio.to_thread(fetch)
        if not isinstance(payload, list) or not all(isinstance(row, Mapping) for row in payload):
            raise RuntimeError("HTTP recovery fills returned a malformed payload")
        return payload


class JsonlMetrics:
    """Non-blocking hot-path metric sink with a dedicated file-writer thread."""

    def __init__(self, path: Path, *, capacity: int = 4_096) -> None:
        if capacity < 1:
            raise ValueError("metrics capacity must be positive")
        self.path = path
        self.queue: queue.Queue[Mapping[str, Any] | None] = queue.Queue(capacity)
        self.dropped = 0
        self.thread: threading.Thread | None = None
        self.failure: BaseException | None = None

    def start(self) -> None:
        if self.thread is not None:
            raise RuntimeError("metrics writer already started")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.thread = threading.Thread(
            target=self._run,
            name="continuous-jsonl-metrics",
            daemon=True,
        )
        self.thread.start()

    def sink(self, payload: Mapping[str, Any]) -> None:
        if self.failure is not None:
            self.dropped += 1
            return
        try:
            self.queue.put_nowait(dict(payload))
        except queue.Full:
            self.dropped += 1

    async def close(self) -> None:
        thread = self.thread
        if thread is None:
            return
        while True:
            try:
                self.queue.put_nowait(None)
                break
            except queue.Full:
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    continue
                self.dropped += 1
        await asyncio.to_thread(thread.join, 5.0)
        self.thread = None
        if thread.is_alive():
            raise RuntimeError("metrics writer did not stop within five seconds")
        self.raise_if_failed()

    def raise_if_failed(self) -> None:
        if self.failure is not None:
            raise RuntimeError("metrics writer failed") from self.failure

    def _run(self) -> None:
        try:
            with self.path.open("a", encoding="utf-8", newline="\n") as handle:
                while True:
                    item = self.queue.get()
                    if item is None:
                        return
                    batch = [item]
                    stop = False
                    for _ in range(127):
                        try:
                            candidate = self.queue.get_nowait()
                        except queue.Empty:
                            break
                        if candidate is None:
                            stop = True
                            break
                        batch.append(candidate)
                    handle.writelines(
                        json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                        for row in batch
                    )
                    handle.flush()
                    if stop:
                        return
        except BaseException as exc:
            self.failure = exc


@dataclass(frozen=True, slots=True)
class FleetRunResult:
    status: str
    armed: bool
    status_path: Path
    metrics_path: Path
    engine_state_dir: Path
    startup_http_requests: int
    startup_http_weight: int
    metrics_dropped: int


async def run_continuous_fleet(
    *,
    repo_root: Path,
    plan_path: Path,
    state_dir: Path,
    engine_state_dir: Path,
    arm: str = "",
    operator_rearm: str = "",
    duration_s: float | None = None,
    stop_file: Path | None = None,
    enable_rest_recovery_fallback: bool = False,
    policy: ReconnectPolicy = ReconnectPolicy(),
) -> FleetRunResult:
    """Construct and run the real continuous engine; disarmed unless token matches exactly."""

    repo_root = _absolute(repo_root, "repo root")
    plan_path = _absolute(plan_path, "plan")
    state_dir = _absolute(state_dir, "state directory")
    engine_state_dir = _absolute(engine_state_dir, "engine state directory")
    if stop_file is not None:
        stop_file = _absolute(stop_file, "stop file")
    if duration_s is not None and duration_s <= 0:
        raise ValueError("duration must be positive")
    if arm not in {"", ARM_TOKEN}:
        raise ValueError(f"arm must be empty or exactly {ARM_TOKEN}")
    if operator_rearm not in {"", ARM_TOKEN}:
        raise ValueError(f"operator rearm must be empty or exactly {ARM_TOKEN}")
    armed = arm == ARM_TOKEN
    rearm_requested = operator_rearm == ARM_TOKEN
    if rearm_requested and not armed:
        raise ValueError("operator rearm requires an armed launch")

    for label, path in (
        ("generation state", state_dir),
        ("durable engine state", engine_state_dir),
    ):
        storage = verify_local_ntfs_runtime(path)
        if not storage.local_non_cloud_ntfs:
            raise RuntimeError(f"unsafe {label}: " + "; ".join(storage.reasons))
    status_path, metrics_path = state_dir / "status.json", state_dir / "metrics.jsonl"
    base_status: dict[str, Any] = {
        "version": 1,
        "status": "starting",
        "pid": os.getpid(),
        "arm_requested": armed,
        "operator_rearm_requested": rearm_requested,
        "execution_enabled": False,
        "plan": str(plan_path),
        "state_dir": str(state_dir),
        "engine_state_dir": str(engine_state_dir),
        "started_ms": now_ms(),
    }
    atomic_json_write(status_path, base_status)
    metrics = JsonlMetrics(metrics_path)
    metrics.start()
    startup_http_requests = 0
    startup_http_logical_requests = 0
    startup_http_weight = 0
    runtime: ContinuousRuntime | None = None
    driver: ContinuousNetworkDriver | None = None
    repair: DurableSourceGapRepair | None = None
    journals: list[ActionJournal] = []
    stop = asyncio.Event()
    auxiliaries: list[asyncio.Task[Any]] = []
    final_state = "error"
    final_slots: dict[str, dict[str, str]] | None = None
    final_followers_flat: bool | None = None
    try:
        plan = load_continuous_plan(plan_path)
        ensure_engine_identity(
            engine_state_dir,
            network=plan.network,
            runtime_id=plan.runtime_id,
            plan_sha256=plan.sha256,
            create=False,
        )
        bound = bind_continuous_plan(plan, repo_root=repo_root, verify_secrets=armed)
        rest_url, ws_url = URLS[plan.network]
        durable_catalog_path = engine_state_dir / "catalog.json"
        previous_catalog = load_durable_catalog(
            durable_catalog_path,
            network=plan.network,
        )
        retained_catalog_symbols = (
            frozenset()
            if previous_catalog is None
            else frozenset(market.symbol for market in previous_catalog.markets)
        )
        with StartupInfo(rest_url) as info:
            try:
                catalog = build_startup_catalog(
                    info,
                    network=plan.network,
                    previous=previous_catalog,
                    retain_symbols=retained_catalog_symbols,
                )
            finally:
                startup_http_requests = info.count
                startup_http_logical_requests = info.logical_count
                startup_http_weight = info.weight
        if startup_http_logical_requests != STARTUP_HTTP_REQUESTS:
            raise RuntimeError("normal fleet startup HTTP must be exactly three catalog calls")
        position_dexes = catalog_position_dexes(catalog)
        atomic_json_write(state_dir / "catalog.json", catalog.to_payload())
        effective_bound = replace(
            bound,
            slots=tuple(_effective_catalog_slot(slot, catalog) for slot in bound.slots),
        )

        mux = WsPostMux(
            response_timeout_s=max(2.0, policy.write_timeout_s),
            write_timeout_s=policy.write_timeout_s,
        )
        preflight = await run_ws_startup_preflight(
            effective_bound,
            network=plan.network,
            ws_url=ws_url,
            mux=mux,
            catalog=catalog,
        )
        atomic_json_write(state_dir / "preflight.json", preflight)
        if not preflight.get("passed"):
            blockers = preflight.get("blockers")
            raise RuntimeError(f"continuous startup preflight failed: {blockers}")
        follower_info = WsFollowerInfo(catalog=catalog)
        budget_slots = effective_bound.slots
        projected_posts_per_minute = (
            sum(follower_info.requests_per_refresh(slot) for slot in budget_slots)
            * 60
            / policy.reconciliation_s
        )
        if projected_posts_per_minute > 1_600:
            raise RuntimeError(
                "follower WS reconciliation would consume the reserved message envelope: "
                f"{projected_posts_per_minute:.1f}/minute"
            )
        projected_info_weight_per_minute = (
            sum(follower_info.weight_per_refresh(slot) for slot in budget_slots)
            * 60
            / policy.reconciliation_s
        )
        if projected_info_weight_per_minute > CONTINUOUS_INFO_WEIGHT_PER_MINUTE:
            raise RuntimeError(
                "follower WS reconciliation exceeds the ordinary information budget: "
                f"{projected_info_weight_per_minute:.1f}/minute"
            )

        ensure_engine_identity(
            engine_state_dir,
            network=plan.network,
            runtime_id=plan.runtime_id,
            plan_sha256=plan.sha256,
        )
        lanes, journals = _build_lanes(
            effective_bound,
            engine_state_dir=engine_state_dir,
            armed=armed,
        )
        report_slots = {
            str(row["slot"]): row
            for row in preflight["slots"]
            if isinstance(row, Mapping) and isinstance(row.get("slot"), str)
        }
        source_modes = {
            slot.config.slot: str(report_slots[slot.config.slot]["identity"]["source_account_mode"])
            for slot in effective_bound.slots
        }
        preflight_follower_dexes = {
            slot.config.slot: tuple(
                str(dex) for dex in report_slots[slot.config.slot]["follower_dexes"]
            )
            for slot in effective_bound.slots
        }
        preflight_vaults = {
            slot.config.slot: (
                slot.config.follower_account_address
                if slot.config.follower_account_address != slot.global_account_address
                else None
            )
            for slot in effective_bound.slots
        }
        fallback = RecoveryHttpFills(rest_url, metrics) if enable_rest_recovery_fallback else None
        repair = DurableSourceGapRepair(
            mux=mux,
            path=engine_state_dir / "source-gap.sqlite3",
            fallback=fallback,
        )
        runtime = ContinuousRuntime(
            # Runtime market scope is derived dynamically from the catalog. Keep the
            # original bound plan here so catalog additions/removals cannot change durable
            # slot identity across a restart.
            plan=bound,
            catalog=catalog,
            lanes=lanes,
            mux=mux,
            follower_info=follower_info,
            preflight_vaults=preflight_vaults,
            preflight_source_modes=source_modes,
            state_path=engine_state_dir / "runtime-state.sqlite3",
            preflight_follower_dexes=preflight_follower_dexes,
            gap_repair=repair,
            execution_enabled=armed,
        )
        driver = ContinuousNetworkDriver(
            runtime=runtime,
            mux=mux,
            ws_url=ws_url,
            repair=repair,
            policy=policy,
            metric_sink=metrics.sink,
            rearm_restored_fail_close=rearm_requested,
        )
        # Replace the accepted comparison base only after durable runtime state
        # has restored and every runtime/network component accepts this catalog.
        atomic_json_write(durable_catalog_path, catalog.to_payload())
        metrics.sink(
            {
                "event": "startup_complete",
                "wall_ms": now_ms(),
                "armed": armed,
                "startup_http_requests": startup_http_requests,
                "startup_http_logical_requests": startup_http_logical_requests,
                "startup_http_weight": startup_http_weight,
                "normal_rest_enabled": False,
                "catalog_rest_refresh_s": CATALOG_REFRESH_S,
                "recovery_rest_enabled": enable_rest_recovery_fallback,
                "catalog_position_dexes": len(position_dexes),
                "projected_ws_info_posts_per_minute": projected_posts_per_minute,
                "projected_ws_info_weight_per_minute": projected_info_weight_per_minute,
            }
        )
        running_status = {
            **base_status,
            "status": "running",
            "execution_enabled": armed,
            "catalog_revision": catalog.revision_id,
            "slot_count": len(effective_bound.slots),
            "startup_http_requests": startup_http_requests,
            "startup_http_logical_requests": startup_http_logical_requests,
            "startup_http_weight": startup_http_weight,
            "normal_rest_enabled": False,
            "catalog_rest_refresh_s": CATALOG_REFRESH_S,
            "recovery_rest_enabled": enable_rest_recovery_fallback,
        }
        atomic_json_write(status_path, running_status)
        if duration_s is not None:
            auxiliaries.append(
                asyncio.create_task(
                    _duration_stop(stop, driver, armed=armed, duration_s=duration_s)
                )
            )
        if stop_file is not None:
            auxiliaries.append(
                asyncio.create_task(_file_stop(stop, driver, stop_file, armed=armed))
            )
        auxiliaries.append(
            asyncio.create_task(
                _status_guard(
                    stop,
                    status_path=status_path,
                    base=running_status,
                    runtime=runtime,
                    driver=driver,
                    metrics=metrics,
                )
            )
        )
        auxiliaries.append(
            asyncio.create_task(
                _catalog_refresh_loop(
                    stop,
                    runtime=runtime,
                    driver=driver,
                    rest_url=rest_url,
                    network=plan.network,
                    catalog_path=state_dir / "catalog.json",
                    durable_catalog_path=durable_catalog_path,
                    metrics=metrics,
                )
            )
        )
        await driver.run(stop=stop)
        failures = [
            task.exception()
            for task in auxiliaries
            if task.done() and not task.cancelled() and task.exception() is not None
        ]
        if failures:
            raise failures[0]  # type: ignore[misc]
        if armed:
            nonflat = [slot for slot in runtime.slot_ids if not runtime.follower_is_flat(slot)]
            if nonflat:
                raise RuntimeError(
                    "armed runner stopped before authoritative follower flatten: "
                    + ", ".join(nonflat)
                )
        final_state = "stopped"
    except asyncio.CancelledError:
        final_state = "cancelled"
        raise
    except Exception as exc:
        atomic_json_write(
            status_path,
            {
                **base_status,
                "status": "error",
                "stopped_ms": now_ms(),
                "error_type": type(exc).__name__,
                "error": str(exc),
                "startup_http_requests": startup_http_requests,
                "startup_http_logical_requests": startup_http_logical_requests,
                "startup_http_weight": startup_http_weight,
            },
        )
        raise
    finally:
        stop.set()
        for task in auxiliaries:
            task.cancel()
        await asyncio.gather(*auxiliaries, return_exceptions=True)
        if runtime is not None:
            final_slots = {
                slot: {
                    "state": runtime.status(slot)[0].value,
                    "reason": runtime.status(slot)[1],
                }
                for slot in runtime.slot_ids
            }
            final_followers_flat = all(runtime.follower_is_flat(slot) for slot in runtime.slot_ids)
            runtime.close()
        if repair is not None:
            repair.close()
        for journal in journals:
            journal.close()
        metrics_error: BaseException | None = None
        try:
            await metrics.close()
        except BaseException as exc:
            metrics_error = exc
        last_status = _read_json_object(status_path)
        total_metrics_dropped = metrics.dropped + (0 if driver is None else driver.dropped_metrics)
        if metrics_error is not None and final_state != "error":
            atomic_json_write(
                status_path,
                {
                    **base_status,
                    **last_status,
                    "status": "error",
                    "execution_enabled": False,
                    "stopped_ms": now_ms(),
                    "error_type": type(metrics_error).__name__,
                    "error": str(metrics_error),
                    "startup_http_requests": startup_http_requests,
                    "startup_http_logical_requests": startup_http_logical_requests,
                    "startup_http_weight": startup_http_weight,
                    "metrics_dropped": total_metrics_dropped,
                    **(
                        {}
                        if final_slots is None
                        else {
                            "slots": final_slots,
                            "followers_flat": final_followers_flat,
                        }
                    ),
                },
            )
            raise metrics_error
        if final_state == "error":
            atomic_json_write(
                status_path,
                {
                    **base_status,
                    **last_status,
                    "status": "error",
                    "execution_enabled": False,
                    "metrics_dropped": total_metrics_dropped,
                    **(
                        {}
                        if final_slots is None
                        else {
                            "slots": final_slots,
                            "followers_flat": final_followers_flat,
                        }
                    ),
                },
            )
        else:
            atomic_json_write(
                status_path,
                {
                    **base_status,
                    **last_status,
                    "status": final_state,
                    "execution_enabled": False,
                    "stopped_ms": now_ms(),
                    "startup_http_requests": startup_http_requests,
                    "startup_http_logical_requests": startup_http_logical_requests,
                    "startup_http_weight": startup_http_weight,
                    "metrics_dropped": total_metrics_dropped,
                    **(
                        {}
                        if final_slots is None
                        else {
                            "slots": final_slots,
                            "followers_flat": final_followers_flat,
                        }
                    ),
                },
            )
    return FleetRunResult(
        final_state,
        armed,
        status_path,
        metrics_path,
        engine_state_dir,
        startup_http_requests,
        startup_http_weight,
        total_metrics_dropped,
    )


async def _duration_stop(
    stop: asyncio.Event,
    driver: ContinuousNetworkDriver,
    *,
    armed: bool,
    duration_s: float,
) -> None:
    await asyncio.sleep(duration_s)
    try:
        if armed:
            await driver.close_out_all(reason="planned duration elapsed")
    finally:
        stop.set()


async def _file_stop(
    stop: asyncio.Event,
    driver: ContinuousNetworkDriver,
    path: Path,
    *,
    armed: bool,
) -> None:
    while not path.exists():
        await asyncio.sleep(0.25)
    try:
        if armed:
            await driver.close_out_all(reason="operator stop requested")
    finally:
        stop.set()


def _build_lanes(
    bound: BoundContinuousPlan,
    *,
    engine_state_dir: Path,
    armed: bool,
) -> tuple[dict[str, ContinuousSignerLane], list[ActionJournal]]:
    lanes: dict[str, ContinuousSignerLane] = {}
    journals: list[ActionJournal] = []
    try:
        for slot in bound.slots:
            journal = ActionJournal(engine_state_dir / "actions" / f"{slot.config.slot}.sqlite3")
            journals.append(journal)
            vault = (
                slot.config.follower_account_address
                if slot.config.follower_account_address != slot.global_account_address
                else None
            )
            if armed:
                lane = ContinuousSignerLane(
                    follower_account=slot.config.follower_account_address,
                    api_wallet_address=slot.api_wallet_address,
                    key_file=slot.api_private_key_file,
                    vault_address=vault,
                    is_mainnet=bound.plan.network == "mainnet",
                    journal=journal,
                )
            else:
                lane = ContinuousSignerLane.monitor_only(
                    follower_account=slot.config.follower_account_address,
                    api_wallet_address=slot.api_wallet_address,
                    vault_address=vault,
                    is_mainnet=bound.plan.network == "mainnet",
                    journal=journal,
                )
            lanes[slot.config.slot] = lane
    except BaseException:
        for journal in journals:
            journal.close()
        raise
    return lanes, journals


async def _status_guard(
    stop: asyncio.Event,
    **kwargs: Any,
) -> None:
    try:
        await _status_loop(stop, **kwargs)
    except asyncio.CancelledError:
        raise
    except Exception:
        driver = kwargs.get("driver")
        try:
            if isinstance(driver, ContinuousNetworkDriver) and driver.runtime.execution_enabled:
                await driver.close_out_all(reason="status writer failed")
        finally:
            stop.set()
        raise


async def _status_loop(
    stop: asyncio.Event,
    *,
    status_path: Path,
    base: Mapping[str, Any],
    runtime: ContinuousRuntime,
    driver: ContinuousNetworkDriver,
    metrics: JsonlMetrics,
) -> None:
    while not stop.is_set():
        metrics.raise_if_failed()
        latest: dict[str, Any] = {}
        while True:
            try:
                dispatch = driver.events.get_nowait()
            except asyncio.QueueEmpty:
                break
            if dispatch.slot is not None:
                latest[dispatch.slot] = {
                    "state": None if dispatch.state is None else dispatch.state.value,
                    "reason": dispatch.reason,
                    "action": None if dispatch.action is None else dispatch.action.market,
                    "attempt": (
                        None if dispatch.attempt is None else dispatch.attempt.result.outcome.value
                    ),
                }
        updated_ms = now_ms()
        operations = driver.operational_status()
        statuses: dict[str, Any] = {}
        for slot in runtime.slot_ids:
            canonical = runtime.operational_status(slot, now_ms=updated_ms)
            transport = dict(operations["slots"].get(slot, {}))
            transport["latest_leader_event_ms"] = max(
                int(canonical.get("latest_leader_event_ms") or 0),
                int(transport.get("latest_leader_event_ms") or 0),
            )
            transport["last_successful_sync_ms"] = max(
                int(canonical.get("last_successful_sync_ms") or 0),
                int(transport.get("last_successful_sync_ms") or 0),
            )
            statuses[slot] = {**canonical, **transport, **latest.get(slot, {})}
        await asyncio.to_thread(
            atomic_json_write,
            status_path,
            {
                **base,
                "updated_ms": updated_ms,
                "slots": statuses,
                "connections": operations["connections"],
                "alarms": operations["alarms"],
                "alarm_threshold_ms": operations["alarm_threshold_ms"],
                "backlog": operations["backlog"],
                "monitoring_dropped": driver.dropped_events,
                "source_backlog_discarded": driver.discarded_source_frames,
                "metrics_dropped": metrics.dropped + driver.dropped_metrics,
                "catalog_revision": runtime.catalog.revision_id,
                "catalog_observed_ms": runtime.catalog.observed_ms,
                "followers_flat": all(
                    runtime.follower_is_flat(slot) for slot in runtime.slot_ids
                ),
            },
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
        except TimeoutError:
            pass


async def _catalog_refresh_loop(
    stop: asyncio.Event,
    *,
    runtime: ContinuousRuntime,
    driver: ContinuousNetworkDriver,
    rest_url: str,
    network: str,
    catalog_path: Path,
    durable_catalog_path: Path,
    metrics: JsonlMetrics,
    interval_s: float = CATALOG_REFRESH_S,
) -> None:
    """Refresh public identity metadata every five minutes; never schedule follower audits."""

    last_unknown_refresh = float("-inf")
    while not stop.is_set():
        stop_wait = asyncio.create_task(stop.wait())
        unknown_wait = asyncio.create_task(runtime.wait_for_catalog_refresh_request())
        waiters = (stop_wait, unknown_wait)
        try:
            done, _ = await asyncio.wait(
                waiters,
                timeout=interval_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
        if stop_wait in done:
            return
        unknown_requested = unknown_wait in done
        if unknown_requested:
            runtime.clear_catalog_refresh_request()
            remaining = UNKNOWN_MARKET_REFRESH_COOLDOWN_S - (monotonic() - last_unknown_refresh)
            if remaining > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=remaining)
                    return
                except TimeoutError:
                    pass
            last_unknown_refresh = monotonic()
        previous = runtime.catalog
        retained = runtime.catalog_retention_markets

        def load() -> tuple[CatalogRevision, int, int, int]:
            with StartupInfo(rest_url) as info:
                candidate = build_startup_catalog(
                    info,
                    network=network,
                    previous=previous,
                    retain_symbols=retained,
                )
                return candidate, info.logical_count, info.count, info.weight

        try:
            catalog, logical_requests, requests, weight = await asyncio.to_thread(load)
            if logical_requests != STARTUP_HTTP_REQUESTS:
                raise RuntimeError("catalog refresh must use exactly three public info requests")
            change = await runtime.apply_catalog(catalog)
            # Keep the live runtime and its socket subscriptions coherent first. If
            # persistence fails, the next refresh can retry the same complete catalog;
            # on a crash, the older durable base will rediscover the delta at startup.
            await driver.notify_market_change(change)
            await asyncio.to_thread(
                atomic_json_write,
                durable_catalog_path,
                catalog.to_payload(),
            )
            await asyncio.to_thread(atomic_json_write, catalog_path, catalog.to_payload())
            metrics.sink(
                {
                    "event": "catalog_refreshed",
                    "wall_ms": now_ms(),
                    "catalog_revision": catalog.revision_id,
                    "catalog_markets": len(catalog.markets),
                    "subscriptions_added": len(change.added),
                    "subscriptions_removed": len(change.removed),
                    "http_logical_requests": logical_requests,
                    "http_requests": requests,
                    "http_weight": weight,
                    "trigger": "unknown_market_fill" if unknown_requested else "periodic",
                }
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            metrics.sink(
                {
                    "event": "catalog_refresh_failed",
                    "wall_ms": now_ms(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )


def _effective_catalog_slot(slot: Any, catalog: CatalogRevision) -> Any:
    if not slot.dynamic_market_eligibility:
        return slot
    denied = set(slot.denied_markets)
    allowed = tuple(
        sorted(
            market.symbol
            for market in catalog.markets
            if market.symbol not in denied
            and not market.is_delisted
            and not market.removal_tombstone
            and market.collateral_token == 0
            and market.readiness not in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
        )
    )
    return replace(slot, config=replace(slot.config, allowed_markets=allowed))


def _absolute(path: Path, label: str) -> Path:
    if not path.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    return path.resolve()


def _redact(address: str) -> str:
    return address if len(address) < 12 else f"{address[:6]}...{address[-4:]}"


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


__all__ = [
    "ARM_TOKEN",
    "FleetRunResult",
    "JsonlMetrics",
    "RecoveryHttpFills",
    "STARTUP_HTTP_REQUESTS",
    "STARTUP_HTTP_WEIGHT",
    "StartupInfo",
    "build_startup_catalog",
    "load_durable_catalog",
    "run_continuous_fleet",
]
