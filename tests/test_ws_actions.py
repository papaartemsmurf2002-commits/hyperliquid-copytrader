from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import pytest

from hyperliquid_copytrader.ws_actions import (
    IocAction,
    PostOutcome,
    SignedIocAction,
    WsPostMux,
    build_ioc_action,
    classify_ioc_response,
    sign_ioc_action,
)


CLOID = "0x" + "1" * 32
OTHER_CLOID = "0x" + "2" * 32


class FakeSocket:
    _EOF = object()

    def __init__(self, *, fail_send: bool = False) -> None:
        self.fail_send = fail_send
        self.sent: list[str] = []
        self.sent_event = asyncio.Event()
        self.incoming: asyncio.Queue[Any] = asyncio.Queue()

    def __aiter__(self) -> FakeSocket:
        return self

    async def __anext__(self) -> Any:
        item = await self.incoming.get()
        if item is self._EOF:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    async def send(self, message: str) -> None:
        self.sent.append(message)
        self.sent_event.set()
        if self.fail_send:
            raise ConnectionError("write failed")

    async def emit(self, message: Any) -> None:
        await self.incoming.put(json.dumps(message))

    async def eof(self) -> None:
        await self.incoming.put(self._EOF)


class BlockingSendSocket(FakeSocket):
    def __init__(self) -> None:
        super().__init__()
        self.send_started = asyncio.Event()
        self.send_cancelled = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)
        self.sent_event.set()
        self.send_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.send_cancelled.set()


class FirstSendBlocksSocket(FakeSocket):
    def __init__(self) -> None:
        super().__init__()
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)
        self.sent_event.set()
        if len(self.sent) == 1:
            self.first_started.set()
            await self.release_first.wait()


def _ioc() -> IocAction:
    return build_ioc_action(
        asset_id=0,
        is_buy=True,
        size=Decimal("0.25"),
        limit_px=Decimal("100.5"),
        reduce_only=False,
        cloid=CLOID,
    )


def _signed() -> SignedIocAction:
    ioc = _ioc()
    return SignedIocAction(
        ioc=ioc,
        nonce=1_750_000_000_000,
        expires_after_ms=1_750_000_002_500,
        payload={
            "action": ioc.action,
            "nonce": 1_750_000_000_000,
            "signature": {"r": "0x1", "s": "0x2", "v": 27},
            "vaultAddress": None,
            "expiresAfter": 1_750_000_002_500,
        },
    )


def _post_response(request_id: int, response: Any) -> dict[str, Any]:
    return {"channel": "post", "data": {"id": request_id, "response": response}}


def _filled_response(size: str) -> dict[str, Any]:
    return {
        "type": "action",
        "payload": {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {
                    "statuses": [
                        {"filled": {"totalSz": size, "avgPx": "100", "oid": 42}}
                    ]
                },
            },
        },
    }


def test_ioc_builder_rejects_outer_wire_cloid_mismatch() -> None:
    ioc = _ioc()
    wire = dict(ioc.action)
    wire["orders"] = [{**ioc.action["orders"][0], "c": OTHER_CLOID}]

    with pytest.raises(ValueError, match="outer CLOID"):
        IocAction(cloid=CLOID, expected_size=ioc.expected_size, action=wire)


def test_signer_uses_caller_nonce_and_signs_exact_action(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_sign(
        wallet: Any,
        action: dict[str, Any],
        vault_address: str | None,
        nonce: int,
        expires_after_ms: int,
        is_mainnet: bool,
    ) -> dict[str, Any]:
        captured.update(
            wallet=wallet,
            action=action,
            vault_address=vault_address,
            nonce=nonce,
            expires_after_ms=expires_after_ms,
            is_mainnet=is_mainnet,
        )
        return {"r": "0x1", "s": "0x2", "v": 27}

    import hyperliquid.utils.signing

    monkeypatch.setattr(hyperliquid.utils.signing, "sign_l1_action", fake_sign)
    wallet = object()
    signed = sign_ioc_action(
        _ioc(),
        wallet=wallet,
        nonce=1234,
        expires_after_ms=5678,
        is_mainnet=True,
    )

    assert captured["wallet"] is wallet
    assert captured["nonce"] == 1234
    assert captured["action"] == signed.ioc.action
    assert signed.payload["nonce"] == 1234
    assert signed.payload["expiresAfter"] == 5678


def test_ioc_partial_fill_is_terminal_and_resting_is_unknown() -> None:
    partial = classify_ioc_response(_filled_response("0.1"), expected_size=Decimal("0.25"))
    assert partial.outcome is PostOutcome.PARTIALLY_FILLED
    assert partial.filled_size == Decimal("0.1")
    assert partial.average_fill_price == Decimal("100")
    assert partial.order_id == 42
    assert partial.terminal is True

    overfill = classify_ioc_response(_filled_response("0.3"), expected_size=Decimal("0.25"))
    assert overfill.outcome is PostOutcome.UNKNOWN
    assert overfill.reason == "reported_fill_exceeds_submitted_size"
    assert overfill.terminal is False

    resting = classify_ioc_response(
        {
            "status": "ok",
            "response": {"data": {"statuses": [{"resting": {"oid": 42}}]}},
        },
        expected_size=Decimal("0.25"),
    )
    assert resting.outcome is PostOutcome.UNKNOWN
    assert resting.reason == "ioc_resting_protocol_error"
    assert resting.terminal is False


def test_ioc_item_error_preserves_exchange_reason() -> None:
    rejected = classify_ioc_response(
        {
            "status": "ok",
            "response": {
                "data": {"statuses": [{"error": "Order must have minimum value of $10."}]}
            },
        },
        expected_size=Decimal("1"),
    )

    assert rejected.outcome is PostOutcome.REJECTED
    assert rejected.reason == "item_error: Order must have minimum value of $10."


def test_ioc_top_level_errors_preserve_bounded_exchange_reason() -> None:
    server = classify_ioc_response(
        {"type": "error", "error": "temporarily overloaded"},
        expected_size=Decimal("1"),
    )
    top = classify_ioc_response(
        {"status": "err", "message": "rate limit budget exhausted"},
        expected_size=Decimal("1"),
    )

    assert server.reason == "server_error_response: temporarily overloaded"
    assert top.reason == "top_level_error: rate limit budget exhausted"


@pytest.mark.asyncio
async def test_info_posts_are_correlated_by_request_id_out_of_order() -> None:
    socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=0.5)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))

    first = asyncio.create_task(
        mux.post_info({"type": "orderStatus", "oid": CLOID}, required_epoch=epoch)
    )
    second = asyncio.create_task(mux.post_info({"type": "allMids"}, required_epoch=epoch))
    while len(socket.sent) < 2:
        await asyncio.sleep(0)
    sent = [json.loads(frame) for frame in socket.sent]

    await socket.emit(_post_response(sent[1]["id"], {"type": "info", "payload": {"n": 2}}))
    await socket.emit(_post_response(sent[0]["id"], {"type": "info", "payload": {"n": 1}}))

    first_result, second_result = await asyncio.gather(first, second)
    assert first_result.outcome is PostOutcome.INFO
    assert first_result.response["payload"]["n"] == 1
    assert second_result.response["payload"]["n"] == 2
    assert first_result.request_id != second_result.request_id

    await socket.eof()
    await reader


@pytest.mark.asyncio
async def test_required_epoch_prevents_action_from_waiting_across_reconnect() -> None:
    first_socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=0.2)
    first_epoch = mux.attach(first_socket)
    mux.detach(first_epoch)

    second_socket = FakeSocket()
    second_epoch = mux.attach(second_socket)
    assert second_epoch != first_epoch

    result = await mux.post_action(_signed(), required_epoch=first_epoch)

    assert result.outcome is PostOutcome.NOT_SENT
    assert second_socket.sent == []


@pytest.mark.asyncio
async def test_pre_send_disconnect_is_not_sent_but_send_failure_is_unknown() -> None:
    mux = WsPostMux(response_timeout_s=0.2)
    disconnected = await mux.post_action(_signed(), required_epoch=1)
    assert disconnected.outcome is PostOutcome.NOT_SENT
    assert disconnected.request_id == 0

    socket = FakeSocket(fail_send=True)
    epoch = mux.attach(socket)
    attempted = await mux.post_action(_signed(), required_epoch=epoch)
    assert attempted.outcome is PostOutcome.UNKNOWN
    assert attempted.request_id > 0
    assert len(socket.sent) == 1


@pytest.mark.asyncio
async def test_stalled_socket_write_is_bounded_and_unknown_after_durable_boundary() -> None:
    socket = BlockingSendSocket()
    mux = WsPostMux(response_timeout_s=30, write_timeout_s=0.01)
    epoch = mux.attach(socket)
    attempted: list[int] = []

    async def before_send(request_id: int) -> None:
        attempted.append(request_id)

    result = await asyncio.wait_for(
        mux.post_action(_signed(), required_epoch=epoch, before_send=before_send),
        timeout=0.25,
    )

    assert result.outcome is PostOutcome.UNKNOWN
    assert result.request_id == attempted[0]
    assert "socket write exceeded" in result.reason
    assert socket.send_cancelled.is_set()
    assert mux._pending == {}


@pytest.mark.asyncio
async def test_cancelling_stalled_write_cancels_socket_send_and_cleans_pending() -> None:
    socket = BlockingSendSocket()
    mux = WsPostMux(response_timeout_s=30, write_timeout_s=30)
    epoch = mux.attach(socket)
    pending = asyncio.create_task(mux.post_action(_signed(), required_epoch=epoch))
    await socket.send_started.wait()

    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    assert socket.send_cancelled.is_set()
    assert mux._pending == {}


@pytest.mark.asyncio
async def test_server_error_frame_after_send_is_unknown_until_cloid_resolution() -> None:
    socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=0.5)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    pending = asyncio.create_task(mux.post_action(_signed(), required_epoch=epoch))
    await socket.sent_event.wait()
    request_id = json.loads(socket.sent[0])["id"]

    await socket.emit(
        {"channel": "error", "data": f"too many pending post requests id={request_id}"}
    )
    result = await pending

    assert result.outcome is PostOutcome.UNKNOWN
    assert "ConnectionLost" in result.reason
    assert result.terminal is False

    await socket.eof()
    await reader


@pytest.mark.asyncio
async def test_clean_eof_fails_pending_request_without_waiting_for_timeout() -> None:
    socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=30)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    pending = asyncio.create_task(mux.post_info({"type": "allMids"}, required_epoch=epoch))
    await socket.sent_event.wait()

    await socket.eof()
    await reader
    result = await asyncio.wait_for(pending, timeout=0.25)

    assert result.outcome is PostOutcome.UNKNOWN
    assert "ConnectionLost" in result.reason


@pytest.mark.asyncio
async def test_action_post_uses_ws_envelope_and_classifies_full_fill() -> None:
    socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=0.5)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    pending = asyncio.create_task(mux.post_action(_signed(), required_epoch=epoch))
    await socket.sent_event.wait()
    frame = json.loads(socket.sent[0])

    assert frame["method"] == "post"
    assert frame["request"]["type"] == "action"
    assert frame["request"]["payload"]["action"]["orders"][0]["c"] == CLOID

    await socket.emit(_post_response(frame["id"], _filled_response("0.25")))
    result = await pending
    assert result.outcome is PostOutcome.FILLED
    assert result.filled_size == Decimal("0.25")
    assert result.average_fill_price == Decimal("100")
    assert result.order_id == 42

    await socket.eof()
    await reader


@pytest.mark.asyncio
async def test_action_exposes_exact_durable_boundary_before_socket_send() -> None:
    socket = FakeSocket()
    mux = WsPostMux(response_timeout_s=0.5)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    boundaries: list[tuple[int, int]] = []

    async def before_send(request_id: int) -> None:
        boundaries.append((request_id, len(socket.sent)))

    pending = asyncio.create_task(
        mux.post_action(_signed(), required_epoch=epoch, before_send=before_send)
    )
    await socket.sent_event.wait()
    frame = json.loads(socket.sent[0])

    assert boundaries == [(frame["id"], 0)]
    await socket.emit(_post_response(frame["id"], _filled_response("0.25")))
    assert (await pending).outcome is PostOutcome.FILLED

    await socket.eof()
    await reader


@pytest.mark.asyncio
async def test_action_write_jumps_queued_reconciliation_info_writes() -> None:
    socket = FirstSendBlocksSocket()
    mux = WsPostMux(response_timeout_s=1, write_timeout_s=1)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    infos = [
        asyncio.create_task(
            mux.post_info({"type": "openOrders", "user": f"0x{index:040x}"}, required_epoch=epoch)
        )
        for index in range(12)
    ]
    await socket.first_started.wait()
    action = asyncio.create_task(mux.post_action(_signed(), required_epoch=epoch))
    await asyncio.sleep(0)
    socket.release_first.set()
    while len(socket.sent) < 13:
        await asyncio.sleep(0)
    frames = [json.loads(frame) for frame in socket.sent]

    assert frames[0]["request"]["type"] == "info"
    assert frames[1]["request"]["type"] == "action"
    assert all(frame["request"]["type"] == "info" for frame in frames[2:])

    for frame in frames:
        response = (
            _filled_response("0.25")
            if frame["request"]["type"] == "action"
            else {"type": "info", "payload": {"type": "openOrders", "data": []}}
        )
        await socket.emit(_post_response(frame["id"], response))
    assert (await action).outcome is PostOutcome.FILLED
    assert all(result.outcome is PostOutcome.INFO for result in await asyncio.gather(*infos))

    await socket.eof()
    await reader


@pytest.mark.asyncio
async def test_cancelled_action_waiter_does_not_strand_info_queue() -> None:
    socket = FirstSendBlocksSocket()
    mux = WsPostMux(response_timeout_s=1, write_timeout_s=1)
    epoch = mux.attach(socket)
    reader = asyncio.create_task(mux.receive_loop(epoch))
    first = asyncio.create_task(mux.post_info({"type": "allMids"}, required_epoch=epoch))
    await socket.first_started.wait()
    action = asyncio.create_task(mux.post_action(_signed(), required_epoch=epoch))
    second = asyncio.create_task(
        mux.post_info({"type": "openOrders", "user": "0x" + "3" * 40}, required_epoch=epoch)
    )
    await asyncio.sleep(0)
    action.cancel()
    with pytest.raises(asyncio.CancelledError):
        await action
    socket.release_first.set()
    while len(socket.sent) < 2:
        await asyncio.sleep(0)
    frames = [json.loads(frame) for frame in socket.sent]

    assert [frame["request"]["type"] for frame in frames] == ["info", "info"]
    for frame in frames:
        expected = frame["request"]["payload"]["type"]
        await socket.emit(
            _post_response(
                frame["id"],
                {"type": "info", "payload": {"type": expected, "data": []}},
            )
        )
    assert (await first).outcome is PostOutcome.INFO
    assert (await second).outcome is PostOutcome.INFO

    await socket.eof()
    await reader
