from __future__ import annotations

import asyncio
import json
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Mapping, Protocol

from .cloid import validate_cloid


class PostOutcome(str, Enum):
    """Definitive state of one WebSocket POST attempt."""

    NOT_SENT = "not_sent"
    UNKNOWN = "unknown"
    REJECTED = "rejected"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class PostResult:
    request_id: int
    outcome: PostOutcome
    response: Any
    reason: str
    filled_size: Decimal = Decimal("0")
    average_fill_price: Decimal | None = None
    order_id: int | None = None

    @property
    def terminal(self) -> bool:
        """Whether this attempt is settled without exchange reconciliation."""

        return self.outcome is not PostOutcome.UNKNOWN


def _wire_decimal(value: Decimal) -> str:
    if not value.is_finite() or value <= 0:
        raise ValueError("wire decimal must be finite and positive")
    normalized = format(value.normalize(), "f")
    if "." in normalized and len(normalized.split(".", 1)[1]) > 8:
        raise ValueError("wire decimal exceeds eight decimal places")
    return normalized


@dataclass(frozen=True, slots=True)
class IocAction:
    cloid: str
    expected_size: Decimal
    action: Mapping[str, Any]

    def __post_init__(self) -> None:
        normalized = validate_cloid(self.cloid)
        if normalized != self.cloid:
            raise ValueError("IOC CLOID must be lowercase")
        if not self.expected_size.is_finite() or self.expected_size <= 0:
            raise ValueError("IOC expected size must be finite and positive")
        _validate_ioc_wire(self.action, outer_cloid=self.cloid)


def _validate_ioc_wire(action: Mapping[str, Any], *, outer_cloid: str) -> None:
    if action.get("type") != "order" or action.get("grouping") != "na":
        raise ValueError("IOC action envelope is invalid")
    orders = action.get("orders")
    if not isinstance(orders, list) or len(orders) != 1:
        raise ValueError("IOC action must contain exactly one order")
    order = orders[0]
    if not isinstance(order, Mapping):
        raise ValueError("IOC wire order is invalid")
    wire_cloid = order.get("c")
    if (
        not isinstance(wire_cloid, str)
        or validate_cloid(wire_cloid) != wire_cloid
        or wire_cloid != outer_cloid
    ):
        raise ValueError("outer CLOID does not match IOC wire order CLOID")
    limit = order.get("t")
    if (
        not isinstance(limit, Mapping)
        or not isinstance(limit.get("limit"), Mapping)
        or limit["limit"].get("tif") != "Ioc"
    ):
        raise ValueError("wire order is not IOC")


def build_ioc_action(
    *,
    asset_id: int,
    is_buy: bool,
    size: Decimal,
    limit_px: Decimal,
    reduce_only: bool,
    cloid: str,
) -> IocAction:
    """Build one signed-size IOC action with an exact outer/wire CLOID identity."""

    normalized_cloid = validate_cloid(cloid)
    if isinstance(asset_id, bool) or not isinstance(asset_id, int) or asset_id < 0:
        raise ValueError("asset ID must be a non-negative integer")
    expected_size = abs(size)
    action = {
        "type": "order",
        "orders": [
            {
                "a": int(asset_id),
                "b": bool(is_buy),
                "p": _wire_decimal(limit_px),
                "s": _wire_decimal(expected_size),
                "r": bool(reduce_only),
                "t": {"limit": {"tif": "Ioc"}},
                "c": normalized_cloid,
            }
        ],
        "grouping": "na",
    }
    return IocAction(cloid=normalized_cloid, expected_size=expected_size, action=action)


@dataclass(frozen=True, slots=True)
class SignedIocAction:
    ioc: IocAction
    nonce: int
    expires_after_ms: int
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        _validate_ioc_wire(self.ioc.action, outer_cloid=self.ioc.cloid)
        if isinstance(self.nonce, bool) or self.nonce < 0:
            raise ValueError("nonce must be a non-negative integer")
        if isinstance(self.expires_after_ms, bool) or self.expires_after_ms <= 0:
            raise ValueError("expiresAfter must be a positive millisecond timestamp")
        if self.payload.get("action") != self.ioc.action:
            raise ValueError("signed payload action does not match IOC action")
        if self.payload.get("nonce") != self.nonce:
            raise ValueError("signed payload nonce does not match assigned nonce")
        if self.payload.get("expiresAfter") != self.expires_after_ms:
            raise ValueError("signed payload expiresAfter does not match assigned expiry")


def sign_ioc_action(
    ioc: IocAction,
    *,
    wallet: Any,
    nonce: int,
    expires_after_ms: int,
    is_mainnet: bool,
    vault_address: str | None = None,
) -> SignedIocAction:
    """Sign an IOC using a nonce already serialized and allocated by the caller."""

    if isinstance(nonce, bool) or nonce < 0:
        raise ValueError("nonce must be a non-negative integer")
    if isinstance(expires_after_ms, bool) or expires_after_ms <= 0:
        raise ValueError("expiresAfter must be a positive millisecond timestamp")
    _validate_ioc_wire(ioc.action, outer_cloid=ioc.cloid)

    from hyperliquid.utils.signing import sign_l1_action

    signature = sign_l1_action(
        wallet,
        dict(ioc.action),
        vault_address,
        nonce,
        expires_after_ms,
        is_mainnet,
    )
    payload = {
        "action": dict(ioc.action),
        "nonce": nonce,
        "signature": signature,
        "vaultAddress": vault_address,
        "expiresAfter": expires_after_ms,
    }
    return SignedIocAction(
        ioc=ioc,
        nonce=nonce,
        expires_after_ms=expires_after_ms,
        payload=payload,
    )


def _item_statuses(payload: Any) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    data = payload.get("data")
    if isinstance(data, Mapping) and isinstance(data.get("statuses"), list):
        return [item for item in data["statuses"] if isinstance(item, Mapping)]
    if isinstance(payload.get("statuses"), list):
        return [item for item in payload["statuses"] if isinstance(item, Mapping)]
    for value in payload.values():
        nested = _item_statuses(value)
        if nested:
            return nested
    return []


def _filled_size(status: Mapping[str, Any]) -> Decimal | None:
    filled = status.get("filled")
    if not isinstance(filled, Mapping):
        return None
    for key in ("totalSz", "sz", "size"):
        if filled.get(key) is None:
            continue
        try:
            value = abs(Decimal(str(filled[key])))
        except Exception:
            return None
        return value if value.is_finite() else None
    return None


def _filled_price_and_oid(status: Mapping[str, Any]) -> tuple[Decimal | None, int | None]:
    filled = status.get("filled")
    if not isinstance(filled, Mapping):
        return None, None
    average: Decimal | None = None
    for key in ("avgPx", "averagePx", "averagePrice"):
        if filled.get(key) is None:
            continue
        try:
            parsed = Decimal(str(filled[key]))
        except Exception:
            break
        if parsed.is_finite() and parsed > 0:
            average = parsed
        break
    oid: int | None = None
    if filled.get("oid") is not None:
        try:
            candidate = int(filled["oid"])
        except Exception:
            candidate = -1
        if candidate >= 0:
            oid = candidate
    return average, oid


def classify_ioc_response(response: Any, *, expected_size: Decimal) -> PostResult:
    """Classify one IOC response; a partial IOC fill is economically terminal."""

    if not expected_size.is_finite() or expected_size <= 0:
        raise ValueError("expected size must be finite and positive")
    payload = response
    if isinstance(payload, Mapping) and payload.get("type") == "action":
        payload = payload.get("payload")
    if not isinstance(payload, Mapping):
        return PostResult(0, PostOutcome.UNKNOWN, response, "malformed_action_response")
    if payload.get("type") == "error":
        detail = _bounded_exchange_error(payload)
        reason = "server_error_response" if not detail else f"server_error_response: {detail}"
        return PostResult(0, PostOutcome.REJECTED, response, reason)
    status = str(payload.get("status") or "").casefold()
    if status in {"err", "error", "failed", "rejected"}:
        detail = _bounded_exchange_error(payload)
        reason = "top_level_error" if not detail else f"top_level_error: {detail}"
        return PostResult(0, PostOutcome.REJECTED, response, reason)

    statuses = _item_statuses(payload.get("response", payload))
    if len(statuses) != 1:
        return PostResult(0, PostOutcome.UNKNOWN, response, "missing_or_ambiguous_item_status")
    item = statuses[0]
    for key, value in item.items():
        if str(key).casefold() in {"error", "err"}:
            detail = " ".join(str(value).split())[:240]
            reason = "item_error" if not detail else f"item_error: {detail}"
            return PostResult(0, PostOutcome.REJECTED, response, reason)
    filled = _filled_size(item)
    if filled is not None:
        average_fill_price, order_id = _filled_price_and_oid(item)
        if filled <= 0:
            return PostResult(0, PostOutcome.UNKNOWN, response, "invalid_filled_size")
        if filled > expected_size:
            return PostResult(
                0,
                PostOutcome.UNKNOWN,
                response,
                "reported_fill_exceeds_submitted_size",
            )
        if filled < expected_size:
            return PostResult(
                0,
                PostOutcome.PARTIALLY_FILLED,
                response,
                "ioc_partial_fill_terminal",
                filled,
                average_fill_price,
                order_id,
            )
        return PostResult(
            0,
            PostOutcome.FILLED,
            response,
            "filled",
            filled,
            average_fill_price,
            order_id,
        )
    if "resting" in item:
        return PostResult(0, PostOutcome.UNKNOWN, response, "ioc_resting_protocol_error")
    if any(str(key).casefold() in {"cancel", "cancelled", "canceled"} for key in item):
        return PostResult(0, PostOutcome.CANCELLED, response, "ioc_cancelled")
    return PostResult(0, PostOutcome.UNKNOWN, response, "unclassified_item_status")


class _Socket(Protocol):
    def __aiter__(self) -> AsyncIterator[Any]: ...

    async def send(self, message: str) -> Any: ...


class _ConnectionLost(ConnectionError):
    pass


class _WireOutcome(str, Enum):
    NOT_SENT = "not_sent"
    RESPONSE = "response"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class _WireResult:
    request_id: int
    outcome: _WireOutcome
    response: Any
    reason: str


@dataclass(slots=True)
class _Pending:
    epoch: int
    future: asyncio.Future[Any]


class _PrioritySendGate:
    """Serialize socket writes while admitting actions ahead of queued info work.

    A write already in progress is never interrupted.  Once it completes, any
    waiting action is admitted before another reconciliation/info write.  This
    prevents a bounded follower refresh wave from turning the action socket's
    write timeout into an equally large leader-event latency multiplier.
    """

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._held = False
        self._action_waiters = 0

    @asynccontextmanager
    async def acquire(self, *, action: bool) -> AsyncIterator[None]:
        acquired = False
        if action:
            async with self._condition:
                self._action_waiters += 1
                self._condition.notify_all()
        try:
            async with self._condition:
                await self._condition.wait_for(
                    lambda: not self._held and (action or self._action_waiters == 0)
                )
                self._held = True
                acquired = True
                if action:
                    self._action_waiters -= 1
            try:
                yield
            finally:
                async with self._condition:
                    self._held = False
                    self._condition.notify_all()
        finally:
            if action and not acquired:
                async with self._condition:
                    self._action_waiters -= 1
                    self._condition.notify_all()


_ERROR_ID_RE = re.compile(r"\bid=(\d+)\b")


class WsPostMux:
    """Request-correlated WS POST mux with caller-enforced connection epochs.

    The owner attaches an already-connected socket, starts ``receive_loop``, and
    captures ``connection_epoch`` immediately before its execution gate.  Action
    submission never waits for a connection and requires that same epoch.
    """

    def __init__(
        self,
        *,
        response_timeout_s: float = 2.0,
        write_timeout_s: float = 2.0,
    ) -> None:
        if response_timeout_s <= 0 or write_timeout_s <= 0:
            raise ValueError("response and write timeouts must be positive")
        self.response_timeout_s = response_timeout_s
        self.write_timeout_s = write_timeout_s
        self._socket: _Socket | None = None
        self._epoch = 0
        self._request_id = 0
        self._send_gate = _PrioritySendGate()
        self._pending: dict[int, _Pending] = {}

    @property
    def connection_epoch(self) -> int | None:
        return self._epoch if self._socket is not None else None

    def capture_epoch(self) -> int:
        epoch = self.connection_epoch
        if epoch is None:
            raise ConnectionError("WebSocket POST transport is disconnected")
        return epoch

    def attach(self, socket: _Socket) -> int:
        if self._socket is not None:
            raise RuntimeError("a WebSocket POST connection is already attached")
        self._epoch += 1
        self._socket = socket
        return self._epoch

    def detach(self, epoch: int, *, reason: str = "WebSocket connection closed") -> None:
        self._close_epoch(epoch, _ConnectionLost(reason))

    async def receive_loop(self, epoch: int) -> None:
        socket = self._socket
        if socket is None or epoch != self._epoch:
            raise RuntimeError("receive loop epoch is not the attached connection")
        reason = "WebSocket connection reached clean EOF"
        try:
            async for raw in socket:
                self.handle_message(epoch, raw)
        except asyncio.CancelledError:
            reason = "WebSocket receive loop was cancelled"
            raise
        except Exception as exc:
            reason = f"WebSocket receive loop failed: {type(exc).__name__}: {exc}"
        finally:
            self._close_epoch(epoch, _ConnectionLost(reason))

    def handle_message(self, epoch: int, raw: Any) -> None:
        if isinstance(raw, str):
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                return
        else:
            message = raw
        if not isinstance(message, Mapping):
            return
        channel = message.get("channel")
        if channel == "post":
            data = message.get("data")
            if not isinstance(data, Mapping):
                return
            request_id = data.get("id")
            if isinstance(request_id, bool) or not isinstance(request_id, int):
                return
            pending = self._pending.get(request_id)
            if pending is not None and pending.epoch == epoch and not pending.future.done():
                pending.future.set_result(data.get("response"))
            return
        if channel != "error":
            return
        request_id, detail = _server_error_identity(message.get("data"))
        if request_id is None:
            return
        pending = self._pending.get(request_id)
        if pending is not None and pending.epoch == epoch and not pending.future.done():
            # ``channel:error`` is a transport/control-plane frame, not the
            # item-level action response.  Once send was attempted it cannot
            # prove that the exchange rejected (or did not execute) an action.
            # Surface UNKNOWN so the caller resolves the durable CLOID.
            pending.future.set_exception(_ConnectionLost(detail))

    async def post_info(
        self,
        payload: Mapping[str, Any],
        *,
        required_epoch: int,
        timeout_s: float | None = None,
    ) -> PostResult:
        if not isinstance(payload.get("type"), str) or not payload["type"]:
            raise ValueError("info payload requires a request type")
        wire = await self._post(
            "info",
            payload,
            required_epoch=required_epoch,
            timeout_s=timeout_s,
        )
        if wire.outcome is _WireOutcome.RESPONSE:
            if isinstance(wire.response, Mapping) and wire.response.get("type") == "error":
                return PostResult(
                    wire.request_id,
                    PostOutcome.REJECTED,
                    wire.response,
                    "server_error_response",
                )
            return PostResult(
                wire.request_id,
                PostOutcome.INFO,
                wire.response,
                "info_response",
            )
        return _public_wire_result(wire)

    async def post_action(
        self,
        signed: SignedIocAction,
        *,
        required_epoch: int,
        timeout_s: float | None = None,
        before_send: Callable[[int], Awaitable[None]] | None = None,
    ) -> PostResult:
        _validate_ioc_wire(signed.ioc.action, outer_cloid=signed.ioc.cloid)
        wire = await self._post(
            "action",
            signed.payload,
            required_epoch=required_epoch,
            timeout_s=timeout_s,
            before_send=before_send,
        )
        if wire.outcome is not _WireOutcome.RESPONSE:
            return _public_wire_result(wire)
        classified = classify_ioc_response(
            wire.response,
            expected_size=signed.ioc.expected_size,
        )
        return PostResult(
            request_id=wire.request_id,
            outcome=classified.outcome,
            response=classified.response,
            reason=classified.reason,
            filled_size=classified.filled_size,
            average_fill_price=classified.average_fill_price,
            order_id=classified.order_id,
        )

    async def _post(
        self,
        request_type: str,
        payload: Mapping[str, Any],
        *,
        required_epoch: int,
        timeout_s: float | None,
        before_send: Callable[[int], Awaitable[None]] | None = None,
    ) -> _WireResult:
        timeout = self.response_timeout_s if timeout_s is None else timeout_s
        if timeout <= 0:
            raise ValueError("response timeout must be positive")
        request_id = 0
        pending: _Pending | None = None
        send_attempted = False
        try:
            async with self._send_gate.acquire(action=request_type == "action"):
                socket = self._socket
                if socket is None or required_epoch != self._epoch:
                    return _WireResult(
                        0,
                        _WireOutcome.NOT_SENT,
                        None,
                        "connection_epoch_changed_before_send",
                    )
                self._request_id += 1
                request_id = self._request_id
                frame = json.dumps(
                    {
                        "method": "post",
                        "id": request_id,
                        "request": {"type": request_type, "payload": dict(payload)},
                    },
                    separators=(",", ":"),
                )
                future = asyncio.get_running_loop().create_future()
                pending = _Pending(required_epoch, future)
                self._pending[request_id] = pending
                if before_send is not None:
                    # The caller uses this exact last-mile boundary to durably
                    # bind the request ID and mark SEND_ATTEMPTED.  It runs only
                    # after the connection epoch is proven current and before
                    # the socket write is invoked.
                    await before_send(request_id)
                send_attempted = True
                try:
                    await asyncio.wait_for(socket.send(frame), timeout=self.write_timeout_s)
                except TimeoutError as exc:
                    raise TimeoutError(f"socket write exceeded {self.write_timeout_s:g}s") from exc

            response = await asyncio.wait_for(pending.future, timeout=timeout)
            return _WireResult(request_id, _WireOutcome.RESPONSE, response, "response")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not send_attempted:
                return _WireResult(
                    request_id,
                    _WireOutcome.NOT_SENT,
                    None,
                    f"not_sent:{type(exc).__name__}:{exc}",
                )
            return _WireResult(
                request_id,
                _WireOutcome.UNKNOWN,
                None,
                f"send_outcome_unknown:{type(exc).__name__}:{exc}",
            )
        finally:
            if request_id:
                self._pending.pop(request_id, None)
            if pending is not None and not pending.future.done():
                pending.future.cancel()

    def _close_epoch(self, epoch: int, error: BaseException) -> None:
        if self._socket is not None and self._epoch == epoch:
            self._socket = None
        for pending in tuple(self._pending.values()):
            if pending.epoch == epoch and not pending.future.done():
                pending.future.set_exception(error)


def _server_error_identity(data: Any) -> tuple[int | None, str]:
    if isinstance(data, Mapping):
        raw_id = data.get("id")
        request_id = raw_id if isinstance(raw_id, int) and not isinstance(raw_id, bool) else None
        detail = str(data.get("error") or data.get("message") or data)
        return request_id, detail
    detail = str(data)
    match = _ERROR_ID_RE.search(detail)
    return (int(match.group(1)) if match else None), detail


def _bounded_exchange_error(data: Any) -> str:
    _request_id, detail = _server_error_identity(data)
    normalized = " ".join(detail.split())[:240]
    # A bare serialized envelope adds no operator value.
    if normalized in {"", "{}"}:
        return ""
    return normalized


def _public_wire_result(wire: _WireResult) -> PostResult:
    if wire.outcome is _WireOutcome.NOT_SENT:
        outcome = PostOutcome.NOT_SENT
    elif wire.outcome is _WireOutcome.REJECTED:
        outcome = PostOutcome.REJECTED
    else:
        outcome = PostOutcome.UNKNOWN
    return PostResult(wire.request_id, outcome, wire.response, wire.reason)


__all__ = [
    "IocAction",
    "PostOutcome",
    "PostResult",
    "SignedIocAction",
    "WsPostMux",
    "build_ioc_action",
    "classify_ioc_response",
    "sign_ioc_action",
]
