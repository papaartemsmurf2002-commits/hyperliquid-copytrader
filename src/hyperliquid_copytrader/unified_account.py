from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from time import sleep
from typing import Any, Callable, Mapping

from .markets import market_dex, qualify_market_symbol
from .models import now_ms, parse_decimal
from .websocket_transport import connect_websocket_sync_ipv6_preferred


class UnifiedAccountStateError(RuntimeError):
    """Raised when aggregate unified-account truth is missing, stale, or malformed."""


class SourceDexScope(str, Enum):
    """Controls how Unified source positions outside the default perp DEX are treated."""

    STRICT = "strict"
    DEFAULT_ONLY_ACCOUNT_EQUITY = "default_only_account_equity"
    ALL_CONFIGURED_MARKETS = "all_configured_markets"


class HyperliquidUserAbstraction(str, Enum):
    """Canonical interpretation of Hyperliquid's ``userAbstraction`` response.

    Hyperliquid currently uses ``default`` for the app's default Unified account mode;
    ``disabled`` is the explicit Standard-account state.  Portfolio margin and DEX
    abstraction stay distinct so callers can reject them without accidentally treating
    either as Standard or Unified.
    """

    STANDARD = "standard"
    UNIFIED = "unified"
    PORTFOLIO_MARGIN = "portfoliomargin"
    DEX_ABSTRACTION = "dexabstraction"


def normalized_abstraction_mode(value: Any) -> str | None:
    """Extract and normalize an account- or DEX-abstraction API response value."""

    if isinstance(value, bool):
        raw = str(value)
    elif isinstance(value, str):
        raw = value
    elif isinstance(value, Mapping):
        raw = ""
        for key in (
            "userAbstraction",
            "userDexAbstraction",
            "accountAbstraction",
            "abstraction",
            "mode",
            "state",
            "type",
            "enabled",
            "isEnabled",
            "value",
        ):
            if value.get(key) is not None:
                raw = str(value[key])
                break
        if not raw:
            return None
    else:
        return None
    return raw.strip().replace("_", "").replace("-", "").lower()


def classify_user_abstraction(value: Any) -> HyperliquidUserAbstraction | None:
    """Classify ``userAbstraction`` without collapsing unsupported exchange modes."""

    mode = normalized_abstraction_mode(value)
    if mode in {"disabled", "standard"}:
        return HyperliquidUserAbstraction.STANDARD
    if mode in {"default", "unified", "unifiedaccount"}:
        return HyperliquidUserAbstraction.UNIFIED
    if mode == HyperliquidUserAbstraction.PORTFOLIO_MARGIN.value:
        return HyperliquidUserAbstraction.PORTFOLIO_MARGIN
    if mode == HyperliquidUserAbstraction.DEX_ABSTRACTION.value:
        return HyperliquidUserAbstraction.DEX_ABSTRACTION
    return None


@dataclass(frozen=True)
class UnifiedAccountSnapshot:
    account: str
    clearinghouse_states: dict[str, dict[str, Any]]
    observed_ms: int
    received_ms: int

    @property
    def default_state(self) -> dict[str, Any]:
        return self.clearinghouse_states[""]

    @property
    def dex_count(self) -> int:
        return len(self.clearinghouse_states)


def websocket_url(rest_url: str) -> str:
    base = rest_url.rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base.removeprefix("https://") + "/ws"
    if base.startswith("http://"):
        return "ws://" + base.removeprefix("http://") + "/ws"
    raise UnifiedAccountStateError(f"unsupported Hyperliquid REST URL: {rest_url!r}")


def parse_all_dexs_message(
    value: str | bytes | Mapping[str, Any],
    *,
    expected_account: str,
    received_ms: int | None = None,
) -> UnifiedAccountSnapshot:
    try:
        payload = json.loads(value) if isinstance(value, (str, bytes)) else dict(value)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise UnifiedAccountStateError(
            f"aggregate follower message is not valid JSON: {exc}"
        ) from exc
    if payload.get("channel") != "allDexsClearinghouseState":
        raise UnifiedAccountStateError(
            f"unexpected aggregate follower channel: {payload.get('channel')!r}"
        )
    data = payload.get("data")
    if not isinstance(data, dict):
        raise UnifiedAccountStateError("aggregate follower payload data must be an object")
    account = str(data.get("user") or "").strip().lower()
    if account != expected_account.strip().lower():
        raise UnifiedAccountStateError(
            f"aggregate follower account mismatch: expected {expected_account.lower()}, got {account}"
        )
    states = _clearinghouse_states(data.get("clearinghouseStates"))
    if "" not in states:
        raise UnifiedAccountStateError("aggregate follower payload is missing the default perp DEX")
    observed_times: list[int] = []
    for dex, state in states.items():
        if not isinstance(state.get("assetPositions"), list):
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {dex or '<default>'} is missing assetPositions"
            )
        try:
            observed = int(str(state.get("time")))
        except (TypeError, ValueError):
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {dex or '<default>'} has invalid time"
            ) from None
        if observed <= 0:
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {dex or '<default>'} has non-positive time"
            )
        observed_times.append(observed)
    return UnifiedAccountSnapshot(
        account=account,
        clearinghouse_states=states,
        observed_ms=min(observed_times),
        received_ms=received_ms if received_ms is not None else now_ms(),
    )


def non_default_dex_activity(snapshot: UnifiedAccountSnapshot) -> list[str]:
    active: list[str] = []
    for dex, state in snapshot.clearinghouse_states.items():
        if dex == "":
            continue
        if _state_has_activity(state, dex=dex):
            active.append(dex)
    return sorted(active)


def _clearinghouse_states(value: Any) -> dict[str, dict[str, Any]]:
    if isinstance(value, dict):
        entries = list(value.items())
    elif isinstance(value, (list, tuple)):
        entries = []
        for item in value:
            if not isinstance(item, list) or len(item) != 2:
                raise UnifiedAccountStateError(
                    "aggregate follower clearinghouseStates must contain [dex, state] pairs"
                )
            entries.append((item[0], item[1]))
    else:
        raise UnifiedAccountStateError(
            "aggregate follower clearinghouseStates must be an object or pair list"
        )
    states: dict[str, dict[str, Any]] = {}
    for raw_dex, raw_state in entries:
        if not isinstance(raw_dex, str) or not isinstance(raw_state, dict):
            raise UnifiedAccountStateError(
                "aggregate follower DEX entries must be string/object pairs"
            )
        try:
            dex = "" if raw_dex == "" else market_dex(qualify_market_symbol(raw_dex, "X"))
        except ValueError as exc:
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {raw_dex!r} is invalid: {exc}"
            ) from exc
        if dex in states:
            raise UnifiedAccountStateError(f"aggregate follower DEX {dex!r} is duplicated")
        states[dex] = raw_state
    if not states:
        raise UnifiedAccountStateError("aggregate follower clearinghouseStates is empty")
    return states


def _state_has_activity(state: Mapping[str, Any], *, dex: str) -> bool:
    positions = state.get("assetPositions")
    if not isinstance(positions, list):
        raise UnifiedAccountStateError(f"aggregate follower DEX {dex!r} positions are malformed")
    for item in positions:
        if not isinstance(item, dict) or not isinstance(item.get("position"), dict):
            raise UnifiedAccountStateError(f"aggregate follower DEX {dex!r} position is malformed")
        try:
            if parse_decimal(item["position"].get("szi")) != 0:
                return True
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {dex!r} position size is invalid: {exc}"
            ) from exc
    numeric_fields = [
        state.get("crossMaintenanceMarginUsed"),
        _summary_value(state, "marginSummary", "totalMarginUsed"),
        _summary_value(state, "marginSummary", "totalNtlPos"),
        _summary_value(state, "crossMarginSummary", "totalMarginUsed"),
        _summary_value(state, "crossMarginSummary", "totalNtlPos"),
    ]
    for raw in numeric_fields:
        try:
            if parse_decimal(raw) != Decimal("0"):
                return True
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise UnifiedAccountStateError(
                f"aggregate follower DEX {dex!r} risk summary is invalid: {exc}"
            ) from exc
    return False


def _summary_value(state: Mapping[str, Any], summary_key: str, value_key: str) -> Any:
    summary = state.get(summary_key)
    if not isinstance(summary, dict) or summary.get(value_key) is None:
        raise UnifiedAccountStateError(
            f"aggregate follower state is missing {summary_key}.{value_key}"
        )
    return summary[value_key]


class UnifiedAccountStateStream:
    """Maintains a fresh aggregate all-DEX follower snapshot for unified collateral safety."""

    def __init__(
        self,
        *,
        rest_url: str,
        account: str,
        timeout_s: float,
        stale_after_ms: int,
        reconnect_attempts: int,
        reconnect_backoff_ms: int,
        connector: Callable[..., Any] = connect_websocket_sync_ipv6_preferred,
    ):
        self.ws_url = websocket_url(rest_url)
        self.account = account.strip().lower()
        self.timeout_s = max(float(timeout_s), 0.1)
        self.stale_after_ms = max(int(stale_after_ms), 1)
        self.reconnect_attempts = max(int(reconnect_attempts), 0)
        self.reconnect_backoff_ms = max(int(reconnect_backoff_ms), 0)
        self._connector = connector
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._snapshot: UnifiedAccountSnapshot | None = None
        self._error = ""

    def snapshot(self) -> UnifiedAccountSnapshot:
        self._start()
        if not self._ready.wait(self.timeout_s):
            raise UnifiedAccountStateError("timed out waiting for aggregate unified-account truth")
        with self._lock:
            snapshot = self._snapshot
            error = self._error
        if snapshot is None:
            raise UnifiedAccountStateError(
                error or "aggregate unified-account truth is unavailable"
            )
        # The nested DEX time is the last account-state mutation and can be old for a flat account.
        # Receipt time proves when this process obtained the aggregate snapshot; subsequent account
        # changes update the live subscription.
        age_ms = max(0, now_ms() - snapshot.received_ms)
        if age_ms > self.stale_after_ms:
            raise UnifiedAccountStateError(
                f"aggregate unified-account truth is stale: age_ms={age_ms} "
                f"threshold_ms={self.stale_after_ms}"
            )
        return snapshot

    def close(self) -> None:
        self._stop.set()

    def _start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._ready.clear()
            self._error = ""
            self._thread = threading.Thread(
                target=self._run,
                name=f"hlct-unified-{self.account[-8:]}",
                daemon=True,
            )
            self._thread.start()

    def _run(self) -> None:
        failures = 0
        subscription = {
            "method": "subscribe",
            "subscription": {"type": "allDexsClearinghouseState", "user": self.account},
        }
        while not self._stop.is_set():
            try:
                with self._connector(
                    self.ws_url,
                    open_timeout=self.timeout_s,
                    close_timeout=1,
                    ping_interval=20,
                    ping_timeout=10,
                ) as connection:
                    connection.send(json.dumps(subscription, separators=(",", ":")))
                    while not self._stop.is_set():
                        message = connection.recv(timeout=self.timeout_s)
                        raw = json.loads(message)
                        if not isinstance(raw, dict):
                            continue
                        if raw.get("channel") == "subscriptionResponse":
                            continue
                        if raw.get("channel") != "allDexsClearinghouseState":
                            continue
                        snapshot = parse_all_dexs_message(
                            raw,
                            expected_account=self.account,
                            received_ms=now_ms(),
                        )
                        with self._lock:
                            self._snapshot = snapshot
                            self._error = ""
                        failures = 0
                        self._ready.set()
            except Exception as exc:  # pragma: no cover - real network path
                failures += 1
                with self._lock:
                    self._error = f"aggregate unified-account stream failed: {exc}"
                self._ready.set()
                if failures > self.reconnect_attempts or self._stop.is_set():
                    return
                if self.reconnect_backoff_ms:
                    sleep((self.reconnect_backoff_ms * (2 ** (failures - 1))) / 1000)
