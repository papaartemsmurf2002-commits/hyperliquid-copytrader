from __future__ import annotations

import json
import math
import os
import random
import shutil
import time
import urllib.error
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar
from urllib.parse import urlsplit


T = TypeVar("T")

# Hyperliquid documents a shared IP budget of 1,200 weighted REST units per minute.
# Reserving one unit every 60 ms caps managed traffic near 1,000 units/minute, about
# 17% below the published ceiling, leaving capacity for operator/debug traffic.
DEFAULT_WEIGHT_INTERVAL_MS = 60
DEFAULT_MAX_WAIT_MS = 60_000
DEFAULT_LOCK_TIMEOUT_MS = 30_000
STATE_VALIDITY_MS = 5 * 60_000

_WEIGHT_TWO_INFO_TYPES = frozenset(
    {
        "allmids",
        "clearinghousestate",
        "exchangestatus",
        "l2book",
        "orderstatus",
        "spotclearinghousestate",
        "userstate",
    }
)
_WEIGHT_SIXTY_INFO_TYPES = frozenset({"user_role", "userrole"})
_RESPONSE_WEIGHTED_INFO_TYPES = frozenset(
    {
        "fundinghistory",
        "historicalorders",
        "nonuserfundingupdates",
        "recenttrades",
        "twaphistory",
        "userfills",
        "userfillsbytime",
        "userfunding",
        "usertwaphistory",
        "usertwapslicefills",
        "usertwapslicefillsbytime",
    }
)


class RestThrottleBacklogError(RuntimeError):
    """Raised instead of sleeping indefinitely behind a stale or overloaded shared queue."""


def call_with_rest_backoff(
    label: str,
    fn: Callable[[], T],
    *,
    enabled: bool = True,
    weight: int | None = None,
    attempts: int | None = None,
    backoff_ms: int | None = None,
) -> T:
    attempt_limit = max(
        _int_env("HLCT_REST_RETRY_ATTEMPTS", 1) if attempts is None else attempts,
        1,
    )
    retry_backoff_ms = max(
        _int_env("HLCT_REST_RETRY_BACKOFF_MS", 1_000) if backoff_ms is None else backoff_ms,
        0,
    )
    for attempt in range(1, attempt_limit + 1):
        apply_rest_throttle(label, enabled=enabled, weight=weight)
        try:
            result = fn()
        except Exception as exc:
            if attempt >= attempt_limit or not _retryable_rest_error(exc):
                raise
            retry_after_ms = _retry_after_ms(exc)
            exponential_ms = retry_backoff_ms * (2 ** (attempt - 1))
            jitter_ms = (
                random.uniform(0, max(retry_backoff_ms * 0.25, 1)) if retry_backoff_ms else 0
            )
            time.sleep(max(retry_after_ms, exponential_ms + jitter_ms) / 1000)
            continue
        _record_response_weight(label, result, enabled=enabled)
        return result
    raise RuntimeError(f"{label} exhausted REST retries")


def apply_rest_throttle(
    label: str,
    *,
    enabled: bool = True,
    weight: int | None = None,
) -> None:
    if not enabled:
        return
    path = _throttle_path()
    interval_ms = _weight_interval_ms()
    if interval_ms <= 0:
        return
    request_weight = max(weight if weight is not None else rest_request_weight(label), 1)
    wait_ms = _reserve_weight(
        path,
        label=label,
        weight=request_weight,
        interval_ms=interval_ms,
        wait=True,
    )
    if wait_ms > 0:
        time.sleep(wait_ms / 1000)


def rest_throttle_enabled_for_base_url(base_url: str) -> bool:
    host = (urlsplit(base_url).hostname or "").lower()
    forced = os.getenv("HLCT_REST_THROTTLE_ALL", "").strip().lower()
    if forced in {"1", "true", "yes", "on"}:
        return host in {"api.hyperliquid.xyz", "api.hyperliquid-testnet.xyz"}
    if forced in {"0", "false", "no", "off"}:
        return False
    configured_hosts = os.getenv("HLCT_REST_THROTTLE_HOSTS", "").strip()
    if configured_hosts:
        aliases = {
            "mainnet": "api.hyperliquid.xyz",
            "testnet": "api.hyperliquid-testnet.xyz",
        }
        hosts = {
            aliases.get(item.strip().lower(), item.strip().lower())
            for item in configured_hosts.split(",")
            if item.strip()
        }
        return host in hosts
    return host in {"api.hyperliquid.xyz", "api.hyperliquid-testnet.xyz"}


def info_rest_throttle_enabled_for_base_url(base_url: str) -> bool:
    """Allow source/info reads to opt out without disabling signed-call throttling."""

    forced = os.getenv("HLCT_REST_THROTTLE_INFO", "").strip().lower()
    if forced in {"0", "false", "no", "off"}:
        return False
    return rest_throttle_enabled_for_base_url(base_url)


def rest_request_weight(label: str) -> int:
    normalized = _normalized_request_type(label)
    if label.lower().startswith("exchange:"):
        return 1
    if normalized in _WEIGHT_TWO_INFO_TYPES:
        return 2
    if normalized in _WEIGHT_SIXTY_INFO_TYPES:
        return 60
    return 20


def _record_response_weight(label: str, result: Any, *, enabled: bool) -> None:
    if not enabled:
        return
    request_type = _normalized_request_type(label)
    if request_type not in _RESPONSE_WEIGHTED_INFO_TYPES:
        return
    item_count = _response_item_count(result)
    if item_count <= 0:
        return
    _reserve_weight(
        _throttle_path(),
        label=f"{label}:response",
        weight=math.ceil(item_count / 20),
        interval_ms=_weight_interval_ms(),
        wait=False,
    )


def _reserve_weight(
    lock_path: Path,
    *,
    label: str,
    weight: int,
    interval_ms: int,
    wait: bool,
) -> int:
    state_path = lock_path.with_suffix(lock_path.suffix + ".state.json")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    now_monotonic_ms = _monotonic_ms()
    now_wall_ms = _wall_ms()
    max_wait_ms = max(_int_env("HLCT_REST_THROTTLE_MAX_WAIT_MS", DEFAULT_MAX_WAIT_MS), 0)
    with _interprocess_lock(lock_path):
        state = _read_state(state_path)
        next_available_ms = _valid_next_available_ms(
            state,
            now_monotonic_ms=now_monotonic_ms,
            now_wall_ms=now_wall_ms,
            max_wait_ms=max_wait_ms,
        )
        scheduled_ms = max(now_monotonic_ms, next_available_ms)
        wait_ms = scheduled_ms - now_monotonic_ms
        if wait and wait_ms > max_wait_ms:
            raise RestThrottleBacklogError(
                f"{label} shared REST throttle backlog is {wait_ms}ms; limit is {max_wait_ms}ms"
            )
        next_request_ms = scheduled_ms + max(weight, 1) * interval_ms
        _write_state(
            state_path,
            label=label,
            next_request_monotonic_ms=next_request_ms,
            updated_monotonic_ms=now_monotonic_ms,
            updated_wall_ms=now_wall_ms,
            weight=weight,
        )
    return wait_ms if wait else 0


def _valid_next_available_ms(
    state: dict[str, Any],
    *,
    now_monotonic_ms: int,
    now_wall_ms: int,
    max_wait_ms: int,
) -> int:
    try:
        next_available = int(state.get("next_request_monotonic_ms") or 0)
        updated_monotonic = int(state.get("updated_monotonic_ms") or 0)
        updated_wall = int(state.get("updated_wall_ms") or 0)
    except (TypeError, ValueError):
        return now_monotonic_ms
    wall_age_ms = now_wall_ms - updated_wall
    if updated_wall <= 0 or wall_age_ms < 0 or wall_age_ms > STATE_VALIDITY_MS:
        return now_monotonic_ms
    # A monotonic clock moving backwards indicates a reboot. A wildly future reservation is
    # treated as corrupt/stale rather than producing a multi-hour sleep while holding execution.
    if updated_monotonic > now_monotonic_ms:
        return now_monotonic_ms
    if next_available < 0 or next_available > now_monotonic_ms + max_wait_ms + STATE_VALIDITY_MS:
        return now_monotonic_ms
    return next_available


def _retryable_rest_error(exc: Exception) -> bool:
    raw_status = getattr(exc, "status_code", None)
    if raw_status is None:
        raw_status = getattr(getattr(exc, "response", None), "status_code", None)
    try:
        status_code = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status_code = None
    if status_code in {408, 429, 500, 502, 503, 504}:
        return True
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in {408, 429, 500, 502, 503, 504}
    if isinstance(exc, (urllib.error.URLError, TimeoutError, ConnectionError)):
        return True
    exception_type = f"{type(exc).__module__}.{type(exc).__name__}".lower()
    if exception_type.startswith(("requests.", "httpx.", "httpcore.")) and any(
        marker in exception_type for marker in ("timeout", "connection", "network")
    ):
        return True
    text = str(exc).lower()
    return (
        "too many requests" in text
        or "http error 429" in text
        or "(429," in text
        or any(f"http error {code}" in text for code in (408, 500, 502, 503, 504))
    )


def _retry_after_ms(exc: Exception) -> int:
    if not isinstance(exc, urllib.error.HTTPError) or exc.headers is None:
        return 0
    raw = exc.headers.get("Retry-After")
    if raw is None:
        return 0
    try:
        return max(int(float(raw) * 1000), 0)
    except (TypeError, ValueError):
        return 0


def _normalized_request_type(label: str) -> str:
    value = label.rsplit(":", 1)[-1].strip().lower().replace("-", "_")
    return value.replace(" ", "").replace("_", "")


def _response_item_count(result: Any) -> int:
    if isinstance(result, list):
        return len(result)
    if not isinstance(result, dict):
        return 0
    for key in ("fills", "orders", "rows", "data"):
        value = result.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def _weight_interval_ms() -> int:
    explicit = os.getenv("HLCT_REST_WEIGHT_INTERVAL_MS", "").strip()
    if explicit:
        return _int_value(explicit, DEFAULT_WEIGHT_INTERVAL_MS)
    legacy = os.getenv("HLCT_REST_THROTTLE_INTERVAL_MS", "").strip()
    if legacy:
        return _int_value(legacy, DEFAULT_WEIGHT_INTERVAL_MS)
    return DEFAULT_WEIGHT_INTERVAL_MS


def _throttle_path() -> Path:
    configured = os.getenv("HLCT_REST_THROTTLE_PATH", "").strip()
    if configured:
        return Path(configured).expanduser()
    local_app_data = os.getenv("LOCALAPPDATA", "").strip()
    if local_app_data:
        root = Path(local_app_data)
    else:
        state_home = os.getenv("XDG_STATE_HOME", "").strip()
        root = Path(state_home).expanduser() if state_home else Path.home() / ".local" / "state"
    return root / "hyperliquid-copytrader" / "rest_throttle.lock"


def _int_env(name: str, default: int) -> int:
    return _int_value(os.getenv(name, "").strip(), default)


def _int_value(raw: str, default: int) -> int:
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def _wall_ms() -> int:
    return int(time.time() * 1000)


def _read_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_state(
    path: Path,
    *,
    label: str,
    next_request_monotonic_ms: int,
    updated_monotonic_ms: int,
    updated_wall_ms: int,
    weight: int,
) -> None:
    payload = {
        "label": label,
        "next_request_monotonic_ms": next_request_monotonic_ms,
        "updated_monotonic_ms": updated_monotonic_ms,
        "updated_wall_ms": updated_wall_ms,
        "weight": weight,
    }
    temp_path = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


@contextmanager
def _interprocess_lock(path: Path) -> Iterator[None]:
    lock_dir = Path(str(path) + ".dir")
    timeout_ms = max(_int_env("HLCT_REST_THROTTLE_LOCK_TIMEOUT_MS", DEFAULT_LOCK_TIMEOUT_MS), 0)
    deadline = _monotonic_ms() + timeout_ms
    while True:
        try:
            lock_dir.mkdir(parents=True)
            break
        except FileExistsError:
            _remove_stale_lock(lock_dir)
            if _monotonic_ms() >= deadline:
                raise RestThrottleBacklogError(
                    f"shared REST throttle lock was unavailable for {timeout_ms}ms"
                )
            time.sleep(0.05)
    try:
        yield
    finally:
        shutil.rmtree(lock_dir, ignore_errors=True)


def _remove_stale_lock(path: Path) -> None:
    try:
        age_s = time.time() - path.stat().st_mtime
    except OSError:
        return
    if age_s > 30:
        shutil.rmtree(path, ignore_errors=True)
