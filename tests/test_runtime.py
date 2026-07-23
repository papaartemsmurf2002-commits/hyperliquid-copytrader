from __future__ import annotations

from hyperliquid_copytrader.models import SafeModeReason
from hyperliquid_copytrader.runtime import CircuitBreaker, SlidingWindowRateLimiter


def test_sliding_window_rate_limiter_blocks_until_window_expires():
    limiter = SlidingWindowRateLimiter(max_events=2, window_ms=1000)
    assert limiter.check(1000).ok
    limiter.record(1000)
    assert limiter.check(1100).ok
    limiter.record(1100)
    blocked = limiter.check(1200)
    assert not blocked.ok
    assert blocked.reason == SafeModeReason.RATE_LIMIT
    assert limiter.check(2100).ok


def test_circuit_breaker_opens_and_recovers_after_cooldown():
    breaker = CircuitBreaker(failure_threshold=2, cooldown_ms=1000)
    assert breaker.record_failure(1000).ok
    opened = breaker.record_failure(1100)
    assert not opened.ok
    assert opened.reason == SafeModeReason.CIRCUIT_BREAKER
    assert not breaker.check(1500).ok
    assert breaker.check(2200).ok
    assert breaker.consecutive_failures == 0
