from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from time import time
from typing import Any


SOURCE_WALLET = "0xcf7c4feb434751146a48b895e96caeb15838f92c"


class Mode(str, Enum):
    SHADOW = "shadow"
    PAPER = "paper"
    TESTNET = "testnet"
    LIVE = "live"


class SourceEventType(str, Enum):
    POSITION = "position"
    FILL = "fill"
    OPEN_ORDER = "open_order"
    CANCEL = "cancel"
    LEVERAGE = "leverage"
    RECONCILE = "reconcile"
    SNAPSHOT = "snapshot"


class IntentAction(str, Enum):
    OPEN = "open"
    REDUCE = "reduce"
    CLOSE = "close"
    CANCEL = "cancel"
    NOOP = "noop"


class IntentStatus(str, Enum):
    PREPARED = "prepared"
    COMMITTED_TO_JOURNAL = "committed_to_journal"
    SIGNED = "signed"
    PENDING = "pending"
    SENT = "sent"
    ACCEPTED = "accepted"
    RESTING = "resting"
    PARTIALLY_FILLED = "partially_filled"
    ACKED = "acked"
    FILLED = "filled"
    CANCELED = "canceled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN_TRANSPORT_OUTCOME = "unknown_transport_outcome"
    RECONCILED_TERMINAL = "reconciled_terminal"
    DEFERRED = "deferred"
    SKIPPED = "skipped"


class ExecutionAttemptPhase(str, Enum):
    """Durable boundary around the only ambiguous part of signed execution."""

    PREPARED = "prepared"
    DISPATCHING = "dispatching"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"
    LEGACY_UNRESOLVED = "legacy_unresolved"


class SafeModeReason(str, Enum):
    NONE = "none"
    ACCOUNT_NOT_CONFIGURED = "account_not_configured"
    CONFIG_INVALID = "config_invalid"
    PREFLIGHT_FAILED = "preflight_failed"
    LIVE_BLOCKED = "live_blocked"
    TESTNET_BLOCKED = "testnet_blocked"
    STARTUP_RECONCILE = "startup_reconcile"
    DUPLICATE_EVENT = "duplicate_event"
    OUT_OF_ORDER_EVENT = "out_of_order_event"
    MISSED_EVENT_GAP = "missed_event_gap"
    WEBSOCKET_DISCONNECT = "websocket_disconnect"
    REST_LAG = "rest_lag"
    RESTART_MID_FILL = "restart_mid_fill"
    PARTIAL_FILL = "partial_fill"
    CANCEL_REJECT = "cancel_reject"
    ORDER_TIMEOUT = "order_timeout"
    RAPID_FLIP = "rapid_flip"
    UNSUPPORTED_SYMBOL = "unsupported_symbol"
    RATE_LIMIT = "rate_limit"
    PRECISION_ERROR = "precision_error"
    MARGIN_ERROR = "margin_error"
    CLOCK_SKEW = "clock_skew"
    STALE_SOURCE = "stale_source"
    STALE_FOLLOWER = "stale_follower"
    MANUAL_INTERVENTION = "manual_intervention"
    AMBIGUOUS_EXCHANGE_RESPONSE = "ambiguous_exchange_response"
    OPERATOR_KILL_SWITCH = "operator_kill_switch"
    RISK_LIMIT = "risk_limit"
    DUPLICATE_INTENT = "duplicate_intent"
    CIRCUIT_BREAKER = "circuit_breaker"
    CONCURRENT_INSTANCE = "concurrent_instance"


@dataclass(frozen=True)
class Position:
    coin: str
    size: Decimal
    entry_px: Decimal | None = None
    leverage: int | None = None
    updated_ms: int = 0

    @property
    def side(self) -> str:
        if self.size > 0:
            return "long"
        if self.size < 0:
            return "short"
        return "flat"

    @property
    def abs_size(self) -> Decimal:
        return abs(self.size)


@dataclass(frozen=True)
class OpenOrder:
    coin: str
    side: str
    size: Decimal
    price: Decimal | None
    oid: int | None = None
    cloid: str | None = None
    reduce_only: bool = False
    updated_ms: int = 0


@dataclass(frozen=True)
class SourceEvent:
    idempotency_key: str
    event_type: SourceEventType
    source_wallet: str = SOURCE_WALLET
    exchange_ts_ms: int = 0
    observed_ts_ms: int = 0
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DesiredState:
    state_id: str
    source_event_key: str
    mode: Mode
    positions: dict[str, Position]
    reason: str
    created_ms: int
    source_wallet: str = ""
    action_account: str = ""
    source_network: str = ""


@dataclass(frozen=True)
class FollowerIntent:
    intent_id: str
    cloid: str
    action: IntentAction
    coin: str
    side: str
    size: Decimal
    price: Decimal | None
    reduce_only: bool
    mode: Mode
    source_event_key: str
    reason: str
    created_ms: int
    desired_state_id: str = ""
    status: IntentStatus = IntentStatus.PENDING
    execution_proof: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionReport:
    report_id: str
    intent_id: str
    cloid: str
    status: IntentStatus
    exchange_status: str
    exchange_ts_ms: int
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReconcileSnapshot:
    snapshot_id: str
    account: str
    positions: dict[str, Position]
    open_orders: list[OpenOrder]
    observed_ms: int
    source: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SafeModeTransition:
    transition_id: str
    enabled: bool
    reason: SafeModeReason
    detail: str
    created_ms: int


def now_ms() -> int:
    return int(time() * 1000)


def decimal_to_wire(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError(f"decimal value must be finite, got {value}")
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal(1)))
    return format(normalized, "f").rstrip("0").rstrip(".")


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_wire(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return {k: to_jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return value


def parse_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        parsed = value
    elif value is None:
        parsed = Decimal("0")
    else:
        parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError(f"decimal value must be finite, got {value!r}")
    return parsed
