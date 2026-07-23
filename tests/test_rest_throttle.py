from __future__ import annotations

import json
import urllib.error
from email.message import Message

import pytest

import hyperliquid_copytrader.rest_throttle as rest_throttle


class FakeClock:
    def __init__(self, *, monotonic_s: float = 100.0, wall_s: float = 1_000.0):
        self.monotonic_s = monotonic_s
        self.wall_s = wall_s
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.monotonic_s

    def time(self) -> float:
        return self.wall_s

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.monotonic_s += seconds
        self.wall_s += seconds


@pytest.fixture
def fake_clock(monkeypatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(rest_throttle.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(rest_throttle.time, "time", clock.time)
    monkeypatch.setattr(rest_throttle.time, "sleep", clock.sleep)
    return clock


def test_hyperliquid_request_weights_match_documented_classes():
    assert rest_throttle.rest_request_weight("info:allMids") == 2
    assert rest_throttle.rest_request_weight("sdk-info:user_state") == 2
    assert rest_throttle.rest_request_weight("sdk-info:user_role") == 60
    assert rest_throttle.rest_request_weight("info:userFillsByTime") == 20
    assert rest_throttle.rest_request_weight("exchange:order") == 1


def test_configured_shared_throttle_covers_mainnet_and_testnet(monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(tmp_path / "shared.lock"))

    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")
    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid-testnet.xyz")
    assert not rest_throttle.rest_throttle_enabled_for_base_url("https://example.com")

    monkeypatch.setenv("HLCT_REST_THROTTLE_ALL", "false")
    assert not rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")


def test_info_reads_can_opt_out_without_disabling_exchange_throttle(monkeypatch):
    monkeypatch.setenv("HLCT_REST_THROTTLE_ALL", "true")
    monkeypatch.setenv("HLCT_REST_THROTTLE_INFO", "false")

    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")
    assert not rest_throttle.info_rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")


def test_configured_throttle_hosts_can_isolate_testnet_budget(monkeypatch):
    monkeypatch.delenv("HLCT_REST_THROTTLE_ALL", raising=False)
    monkeypatch.setenv("HLCT_REST_THROTTLE_HOSTS", "testnet")

    assert not rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")
    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid-testnet.xyz")

    monkeypatch.setenv("HLCT_REST_THROTTLE_ALL", "true")
    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")


def test_official_hosts_use_stable_user_local_throttle_by_default(
    fake_clock,
    monkeypatch,
    tmp_path,
):
    monkeypatch.delenv("HLCT_REST_THROTTLE_PATH", raising=False)
    monkeypatch.delenv("HLCT_REST_THROTTLE_ALL", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))

    assert rest_throttle.rest_throttle_enabled_for_base_url("https://api.hyperliquid.xyz")
    rest_throttle.apply_rest_throttle("info:allMids")

    state_path = tmp_path / "hyperliquid-copytrader" / "rest_throttle.lock.state.json"
    assert state_path.exists()


def test_shared_throttle_reserves_documented_weight(fake_clock, monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(tmp_path / "shared.lock"))
    monkeypatch.setenv("HLCT_REST_WEIGHT_INTERVAL_MS", "10")
    monkeypatch.setenv("HLCT_REST_THROTTLE_MAX_WAIT_MS", "1000")

    rest_throttle.apply_rest_throttle("info:allMids")
    rest_throttle.apply_rest_throttle("sdk-info:user_role")
    rest_throttle.apply_rest_throttle("exchange:order")

    assert fake_clock.sleeps == pytest.approx([0.02, 0.6])


def test_response_size_adds_shared_weight_debt(fake_clock, monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(tmp_path / "shared.lock"))
    monkeypatch.setenv("HLCT_REST_WEIGHT_INTERVAL_MS", "10")
    monkeypatch.setenv("HLCT_REST_THROTTLE_MAX_WAIT_MS", "1000")

    rows = rest_throttle.call_with_rest_backoff(
        "info:userFillsByTime",
        lambda: [{"fill": index} for index in range(41)],
    )
    rest_throttle.apply_rest_throttle("exchange:order")

    assert len(rows) == 41
    assert fake_clock.sleeps == pytest.approx([0.23])


def test_prior_boot_monotonic_state_is_reset(fake_clock, monkeypatch, tmp_path):
    lock_path = tmp_path / "shared.lock"
    state_path = lock_path.with_suffix(lock_path.suffix + ".state.json")
    state_path.write_text(
        json.dumps(
            {
                "next_request_monotonic_ms": 900_000,
                "updated_monotonic_ms": 899_000,
                "updated_wall_ms": int(fake_clock.wall_s * 1000),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(lock_path))
    monkeypatch.setenv("HLCT_REST_WEIGHT_INTERVAL_MS", "10")

    rest_throttle.apply_rest_throttle("info:allMids")

    assert fake_clock.sleeps == []


def test_excessive_shared_backlog_fails_closed(fake_clock, monkeypatch, tmp_path):
    lock_path = tmp_path / "shared.lock"
    state_path = lock_path.with_suffix(lock_path.suffix + ".state.json")
    state_path.write_text(
        json.dumps(
            {
                "next_request_monotonic_ms": int(fake_clock.monotonic_s * 1000) + 500,
                "updated_monotonic_ms": int(fake_clock.monotonic_s * 1000),
                "updated_wall_ms": int(fake_clock.wall_s * 1000),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(lock_path))
    monkeypatch.setenv("HLCT_REST_WEIGHT_INTERVAL_MS", "10")
    monkeypatch.setenv("HLCT_REST_THROTTLE_MAX_WAIT_MS", "100")

    with pytest.raises(rest_throttle.RestThrottleBacklogError, match="backlog"):
        rest_throttle.apply_rest_throttle("info:allMids")


def test_429_retry_honors_retry_after(fake_clock, monkeypatch, tmp_path):
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(tmp_path / "shared.lock"))
    monkeypatch.setenv("HLCT_REST_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("HLCT_REST_RETRY_BACKOFF_MS", "100")
    monkeypatch.setattr(rest_throttle.random, "uniform", lambda _start, _end: 0)
    headers = Message()
    headers["Retry-After"] = "2"
    attempts = 0

    def request() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise urllib.error.HTTPError("https://example", 429, "limited", headers, None)
        return "ok"

    assert rest_throttle.call_with_rest_backoff("info:allMids", request) == "ok"
    assert fake_clock.sleeps == pytest.approx([2.0])


def test_idempotent_info_retry_recovers_from_transient_server_error(
    fake_clock, monkeypatch, tmp_path
):
    monkeypatch.setenv("HLCT_REST_THROTTLE_PATH", str(tmp_path / "shared.lock"))
    monkeypatch.setenv("HLCT_REST_RETRY_ATTEMPTS", "2")
    monkeypatch.setenv("HLCT_REST_RETRY_BACKOFF_MS", "100")
    monkeypatch.setattr(rest_throttle.random, "uniform", lambda _start, _end: 0)
    attempts = 0

    def request() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise urllib.error.HTTPError("https://example", 500, "transient", Message(), None)
        return "ok"

    assert rest_throttle.call_with_rest_backoff("info:l2Book", request) == "ok"
    assert attempts == 2
    assert fake_clock.sleeps == pytest.approx([0.1, 0.02])


def test_retryable_rest_error_supports_sdk_status_and_requests_timeout():
    class ServerError(Exception):
        def __init__(self):
            super().__init__("sdk server error")
            self.status_code = 503

    RequestsTimeout = type("Timeout", (Exception,), {"__module__": "requests.exceptions"})

    assert rest_throttle._retryable_rest_error(ServerError()) is True
    assert rest_throttle._retryable_rest_error(RequestsTimeout("timed out")) is True
