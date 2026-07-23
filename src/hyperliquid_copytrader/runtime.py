from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from .models import SafeModeReason, now_ms


@dataclass(frozen=True)
class RuntimeDecision:
    ok: bool
    reason: SafeModeReason
    detail: str


class SlidingWindowRateLimiter:
    def __init__(self, *, max_events: int, window_ms: int = 60_000):
        if max_events <= 0:
            raise ValueError("max_events must be positive")
        self.max_events = max_events
        self.window_ms = window_ms
        self.events: deque[int] = deque()

    def check(self, observed_ms: int | None = None) -> RuntimeDecision:
        observed = observed_ms or now_ms()
        self._prune(observed)
        if len(self.events) >= self.max_events:
            oldest = self.events[0]
            retry_ms = max(0, self.window_ms - (observed - oldest))
            return RuntimeDecision(
                ok=False,
                reason=SafeModeReason.RATE_LIMIT,
                detail=f"local action rate limit hit; retry after {retry_ms}ms",
            )
        return RuntimeDecision(True, SafeModeReason.NONE, "")

    def record(self, observed_ms: int | None = None) -> None:
        observed = observed_ms or now_ms()
        self._prune(observed)
        self.events.append(observed)

    def _prune(self, observed_ms: int) -> None:
        while self.events and observed_ms - self.events[0] >= self.window_ms:
            self.events.popleft()


class CircuitBreaker:
    def __init__(self, *, failure_threshold: int, cooldown_ms: int):
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be positive")
        if cooldown_ms < 0:
            raise ValueError("cooldown_ms cannot be negative")
        self.failure_threshold = failure_threshold
        self.cooldown_ms = cooldown_ms
        self.consecutive_failures = 0
        self.opened_ms: int | None = None

    def check(self, observed_ms: int | None = None) -> RuntimeDecision:
        observed = observed_ms or now_ms()
        if self.opened_ms is None:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        elapsed = observed - self.opened_ms
        if elapsed < self.cooldown_ms:
            return RuntimeDecision(
                ok=False,
                reason=SafeModeReason.CIRCUIT_BREAKER,
                detail=(
                    f"circuit breaker open after {self.consecutive_failures} consecutive failures; "
                    f"{self.cooldown_ms - elapsed}ms cooldown remaining"
                ),
            )
        self.opened_ms = None
        self.consecutive_failures = 0
        return RuntimeDecision(True, SafeModeReason.NONE, "")

    def record_success(self) -> None:
        self.consecutive_failures = 0
        self.opened_ms = None

    def record_failure(self, observed_ms: int | None = None) -> RuntimeDecision:
        observed = observed_ms or now_ms()
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold:
            self.opened_ms = observed
            return RuntimeDecision(
                ok=False,
                reason=SafeModeReason.CIRCUIT_BREAKER,
                detail=f"circuit breaker opened after {self.consecutive_failures} consecutive failures",
            )
        return RuntimeDecision(
            ok=True,
            reason=SafeModeReason.NONE,
            detail=f"{self.consecutive_failures} consecutive exchange failures",
        )
