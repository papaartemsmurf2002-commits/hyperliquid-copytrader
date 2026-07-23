from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from time import monotonic_ns
from typing import Any, Callable

from .persistence import SQLiteStore


_TELEMETRY_BATCH_SIZE = 256
_CRITICAL_BATCH_SIZE = 32
_MAX_CRITICAL_BURST = 64
_CRITICAL_CAPACITY_DIVISOR = 8
_CRITICAL_BATCH_METHODS = {
    "append_runtime_event": ("append_runtime_events", "events", True),
    "append_state_revision": ("append_state_revisions", "revisions", False),
    "commit_fast_reaction": ("commit_fast_reactions", "reactions", False),
    "commit_fast_reaction_head": ("commit_fast_reaction_heads", "reactions", False),
    "commit_signed_action": ("commit_signed_actions", "actions", False),
    "transition_action_state": ("transition_action_states", "transitions", False),
    "peek_signer_nonce": ("peek_signer_nonces", "requests", True),
    "adjust_signed_unsent": ("adjust_signed_unsents", "adjustments", True),
    "commit_transport_attempt": ("commit_transport_attempts", "attempts", False),
    "commit_signed_expiry": ("commit_signed_expiries", "expiries", False),
}


@dataclass(frozen=True, slots=True)
class JournalWriterHealth:
    queued: int
    capacity: int
    high_water: int
    completed: int
    failed: int
    closed: bool
    oldest_age_ms: float


@dataclass(slots=True)
class _JournalCommand:
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: asyncio.Future[Any]
    enqueued_mono_ns: int


class JournalQueueFull(RuntimeError):
    pass


class JournalWriter:
    """The sole execution-journal connection and bounded write queue.

    WebSocket callbacks can use :meth:`offer` without performing disk I/O. The
    returned future completes only after the command commits, which lets a worker
    forward an event after ingress durability while keeping callbacks enqueue-only.

    Stage timings are non-authoritative telemetry. They use a separate batched
    queue so safety-critical journal work can pass a timing backlog, while a
    bounded critical burst prevents telemetry starvation.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        capacity: int = 4096,
        on_overflow: Callable[[str], None] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("journal writer capacity must be positive")
        self.path = Path(path)
        self.capacity = capacity
        critical_reserve = (
            min(capacity - 1, max(1, capacity // _CRITICAL_CAPACITY_DIVISOR)) if capacity > 1 else 0
        )
        self._telemetry_capacity = capacity - critical_reserve
        self._critical_queue: deque[_JournalCommand] = deque()
        self._telemetry_queue: deque[_JournalCommand] = deque()
        self._available = asyncio.Event()
        self._drained = asyncio.Event()
        self._drained.set()
        self._on_overflow = on_overflow
        self._task: asyncio.Task[None] | None = None
        self._store: SQLiteStore | None = None
        self._high_water = 0
        self._completed = 0
        self._failed = 0
        self._queued = 0
        self._unfinished = 0
        self._closed = False
        self._stop_requested = False
        self._critical_since_telemetry = 0
        self._worker_failure: BaseException | None = None
        self._active_commands: list[_JournalCommand] = []

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._closed:
            raise RuntimeError("journal writer is closed")
        self._store = SQLiteStore(self.path)
        self._task = asyncio.create_task(self._run(), name="execution-journal-writer")

    def offer(self, method: str, /, *args: Any, **kwargs: Any) -> asyncio.Future[Any]:
        if self._worker_failure is not None:
            raise RuntimeError("journal writer stopped unexpectedly") from self._worker_failure
        if self._stop_requested:
            raise RuntimeError("journal writer is stopping")
        if self._task is None or self._store is None:
            raise RuntimeError("journal writer is not started")
        if self._closed:
            raise RuntimeError("journal writer is closed")
        loop = asyncio.get_running_loop()
        result: asyncio.Future[Any] = loop.create_future()
        command = _JournalCommand(
            method=method,
            args=args,
            kwargs=kwargs,
            result=result,
            enqueued_mono_ns=monotonic_ns(),
        )
        queue = self._telemetry_queue if method == "record_stage_timing" else self._critical_queue
        telemetry_at_limit = (
            queue is self._telemetry_queue
            and len(self._telemetry_queue) >= self._telemetry_capacity
        )
        if self._queued >= self.capacity or telemetry_at_limit:
            detail = f"journal queue overflow while enqueuing {method}"
            if self._on_overflow is not None:
                self._on_overflow(detail)
            raise JournalQueueFull(detail)
        queue.append(command)
        self._queued += 1
        self._unfinished += 1
        self._drained.clear()
        self._available.set()
        self._high_water = max(self._high_water, self._queued)
        return result

    async def submit(self, method: str, /, *args: Any, **kwargs: Any) -> Any:
        # Once queued, a durable command cannot be cancelled safely: its SQLite
        # thread may already be committing.  Shield the authoritative result
        # future so critical callers can retain/drain it to a definitive outcome.
        return await asyncio.shield(self.offer(method, *args, **kwargs))

    async def flush(self) -> None:
        await self._drained.wait()
        if self._worker_failure is not None:
            raise RuntimeError("journal writer stopped unexpectedly") from self._worker_failure

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        task = self._task
        if task is None:
            return
        flush_error: BaseException | None = None
        try:
            await self.flush()
        except BaseException as exc:
            flush_error = exc
        self._stop_requested = True
        self._available.set()
        task_error: BaseException | None = None
        try:
            await task
        except BaseException as exc:
            task_error = exc
        finally:
            self._task = None
        if task_error is not None:
            raise task_error
        if flush_error is not None:
            raise flush_error

    def health(self) -> JournalWriterHealth:
        queue_heads = [queue[0] for queue in (self._critical_queue, self._telemetry_queue) if queue]
        queue_heads.extend(self._active_commands)
        oldest = min(queue_heads, key=lambda item: item.enqueued_mono_ns, default=None)
        return JournalWriterHealth(
            queued=self._queued,
            capacity=self.capacity,
            high_water=self._high_water,
            completed=self._completed,
            failed=self._failed,
            closed=self._closed,
            oldest_age_ms=0.0
            if oldest is None
            else max(0.0, (monotonic_ns() - oldest.enqueued_mono_ns) / 1_000_000),
        )

    async def _next_commands(self) -> list[_JournalCommand] | None:
        while True:
            if self._critical_queue and (
                not self._telemetry_queue or self._critical_since_telemetry < _MAX_CRITICAL_BURST
            ):
                commands = [self._critical_queue.popleft()]
                first_method = commands[0].method
                batch_limit = _CRITICAL_BATCH_SIZE
                if self._telemetry_queue:
                    batch_limit = min(
                        batch_limit,
                        _MAX_CRITICAL_BURST - self._critical_since_telemetry,
                    )
                if first_method in _CRITICAL_BATCH_METHODS:
                    while (
                        len(commands) < batch_limit
                        and self._critical_queue
                        and self._critical_queue[0].method == first_method
                    ):
                        commands.append(self._critical_queue.popleft())
                self._critical_since_telemetry += len(commands)
                self._queued -= len(commands)
                return commands
            if self._telemetry_queue:
                commands = [self._telemetry_queue.popleft()]
                while len(commands) < _TELEMETRY_BATCH_SIZE and self._telemetry_queue:
                    commands.append(self._telemetry_queue.popleft())
                self._queued -= len(commands)
                self._critical_since_telemetry = 0
                return commands
            if self._stop_requested:
                return None
            self._available.clear()
            if self._critical_queue or self._telemetry_queue:
                continue
            await self._available.wait()

    def _finish(self, count: int) -> None:
        self._unfinished -= count
        if self._unfinished < 0:
            raise RuntimeError("journal unfinished-command accounting underflow")
        if self._unfinished == 0:
            self._drained.set()

    @staticmethod
    def _fail_commands(commands: list[_JournalCommand], exc: BaseException) -> None:
        for command in commands:
            if not command.result.done():
                command.result.set_exception(exc)

    async def _run(self) -> None:
        assert self._store is not None
        try:
            while True:
                commands = await self._next_commands()
                if commands is None:
                    return
                self._active_commands = commands
                try:
                    command = commands[0]
                    if command.method == "record_stage_timing":
                        if any(item.args for item in commands):
                            raise TypeError("record_stage_timing accepts keyword arguments only")
                        await asyncio.to_thread(
                            self._store.record_stage_timings,
                            timings=[dict(item.kwargs) for item in commands],
                        )
                        self._completed += len(commands)
                        for item in commands:
                            if not item.result.done():
                                item.result.set_result(None)
                    elif command.method in _CRITICAL_BATCH_METHODS:
                        if any(item.args for item in commands):
                            raise TypeError(f"{command.method} accepts keyword arguments only")
                        plural_method_name, argument_name, returns_values = _CRITICAL_BATCH_METHODS[
                            command.method
                        ]
                        plural_method = getattr(self._store, plural_method_name)
                        value = await asyncio.to_thread(
                            plural_method,
                            **{argument_name: [dict(item.kwargs) for item in commands]},
                        )
                        if returns_values:
                            if not isinstance(value, list) or len(value) != len(commands):
                                raise RuntimeError(
                                    f"{plural_method_name} returned the wrong result count"
                                )
                            results = value
                        else:
                            results = [None] * len(commands)
                        self._completed += len(commands)
                        for item, result in zip(commands, results, strict=True):
                            if not item.result.done():
                                item.result.set_result(result)
                    else:
                        method = getattr(self._store, command.method, None)
                        if method is None or not callable(method):
                            raise AttributeError(f"unknown journal operation {command.method!r}")
                        value = await asyncio.to_thread(
                            method,
                            *command.args,
                            **command.kwargs,
                        )
                        self._completed += 1
                        if not command.result.done():
                            command.result.set_result(value)
                except Exception as exc:
                    self._failed += len(commands)
                    self._fail_commands(commands, exc)
                except BaseException as exc:
                    self._failed += len(commands)
                    fatal_error = RuntimeError("journal writer stopped during a durable command")
                    fatal_error.__cause__ = exc
                    self._fail_commands(commands, fatal_error)
                    raise
                finally:
                    self._active_commands = []
                    self._finish(len(commands))
        except BaseException as exc:
            self._worker_failure = exc
            raise
        finally:
            pending = [*self._critical_queue, *self._telemetry_queue]
            self._critical_queue.clear()
            self._telemetry_queue.clear()
            self._queued = 0
            if pending:
                self._fail_commands(
                    pending,
                    RuntimeError("journal writer stopped before queued work completed"),
                )
                self._failed += len(pending)
                self._finish(len(pending))
            try:
                await asyncio.to_thread(self._store.close)
            except BaseException as exc:
                if self._worker_failure is None:
                    self._worker_failure = exc
                raise
            finally:
                self._store = None
