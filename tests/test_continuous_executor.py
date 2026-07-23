from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from eth_account import Account

from hyperliquid_copytrader.action_journal import ActionJournal, ActionState
from hyperliquid_copytrader.continuous_executor import ContinuousSignerLane
from hyperliquid_copytrader.desired_engine import NextAction
from hyperliquid_copytrader.ws_actions import PostOutcome, WsPostMux


class _Socket:
    EOF = object()

    def __init__(self, response: dict[str, Any] | None) -> None:
        self.response = response
        self.queue: asyncio.Queue[Any] = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.queue.get()
        if item is self.EOF:
            raise StopAsyncIteration
        return item

    async def send(self, raw: str) -> None:
        frame = json.loads(raw)
        if self.response is not None:
            await self.queue.put(
                json.dumps(
                    {
                        "channel": "post",
                        "data": {"id": frame["id"], "response": self.response},
                    }
                )
            )


def _action() -> NextAction:
    return NextAction(
        desired_id="desired-1",
        market="BTC",
        side="buy",
        size=Decimal("0.01"),
        reduce_only=False,
        reason="test",
    )


def _lane(tmp_path: Path, journal: ActionJournal) -> ContinuousSignerLane:
    wallet = Account.create()
    key_file = tmp_path / "key"
    key_file.write_text(wallet.key.hex(), encoding="utf-8")
    return ContinuousSignerLane(
        follower_account="0x" + "1" * 40,
        api_wallet_address=wallet.address,
        key_file=key_file,
        vault_address="0x" + "1" * 40,
        is_mainnet=False,
        journal=journal,
    )


@pytest.mark.asyncio
async def test_signed_payload_is_durable_before_correlated_ws_send(tmp_path: Path) -> None:
    response = {
        "type": "action",
        "payload": {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {"statuses": [{"filled": {"totalSz": "0.01"}}]},
            },
        },
    }
    with ActionJournal(tmp_path / "journal.sqlite3") as journal:
        lane = _lane(tmp_path, journal)
        mux = WsPostMux(response_timeout_s=0.5)
        socket = _Socket(response)
        epoch = mux.attach(socket)
        reader = asyncio.create_task(mux.receive_loop(epoch))

        attempt = await lane.execute_ioc(
            action=_action(),
            asset_id=0,
            limit_px=Decimal("1000"),
            mux=mux,
            required_epoch=epoch,
        )

        assert attempt.result.outcome is PostOutcome.FILLED
        assert attempt.record.state is ActionState.FILLED
        assert attempt.record.request_id == "1"
        assert (
            json.loads(attempt.record.signed_payload_json)["action"]["orders"][0]["c"]
            == attempt.record.cloid
        )
        await socket.queue.put(_Socket.EOF)
        await reader


@pytest.mark.asyncio
async def test_prepared_callback_runs_after_durable_prepare_and_before_send(
    tmp_path: Path,
) -> None:
    observed: list[ActionState] = []

    class _InspectingSocket(_Socket):
        async def send(self, raw: str) -> None:
            assert observed == [ActionState.PREPARED]
            await super().send(raw)

    response = {
        "type": "action",
        "payload": {
            "status": "ok",
            "response": {"data": {"statuses": [{"cancelled": {}}]}},
        },
    }
    with ActionJournal(tmp_path / "journal.sqlite3") as journal:
        lane = _lane(tmp_path, journal)
        mux = WsPostMux(response_timeout_s=0.5)
        socket = _InspectingSocket(response)
        epoch = mux.attach(socket)
        reader = asyncio.create_task(mux.receive_loop(epoch))

        attempt = await lane.execute_ioc(
            action=_action(),
            asset_id=0,
            limit_px=Decimal("1000"),
            mux=mux,
            required_epoch=epoch,
            on_prepared=lambda record: observed.append(record.state),
        )

        assert attempt.record.state is ActionState.CANCELED
        assert observed == [ActionState.PREPARED]
        await socket.queue.put(_Socket.EOF)
        await reader


@pytest.mark.asyncio
async def test_epoch_change_is_provably_not_sent_and_new_attempt_gets_new_cloid(
    tmp_path: Path,
) -> None:
    with ActionJournal(tmp_path / "journal.sqlite3") as journal:
        lane = _lane(tmp_path, journal)
        mux = WsPostMux(response_timeout_s=0.1)
        socket = _Socket(None)
        old_epoch = mux.attach(socket)
        mux.detach(old_epoch)

        first = await lane.execute_ioc(
            action=_action(),
            asset_id=0,
            limit_px=Decimal("1000"),
            mux=mux,
            required_epoch=old_epoch,
        )
        assert first.record.state is ActionState.NOT_SENT

        replacement = _Socket(
            {
                "type": "action",
                "payload": {
                    "status": "ok",
                    "response": {"data": {"statuses": [{"cancelled": {}}]}},
                },
            }
        )
        epoch = mux.attach(replacement)
        reader = asyncio.create_task(mux.receive_loop(epoch))
        second = await lane.execute_ioc(
            action=_action(),
            asset_id=0,
            limit_px=Decimal("1000"),
            mux=mux,
            required_epoch=epoch,
        )

        assert second.record.state is ActionState.CANCELED
        assert second.record.cloid != first.record.cloid
        await replacement.queue.put(_Socket.EOF)
        await reader


@pytest.mark.asyncio
async def test_lost_ack_partial_then_cancel_resolves_as_terminal_partial_fill(
    tmp_path: Path,
) -> None:
    with ActionJournal(tmp_path / "journal.sqlite3") as journal:
        lane = _lane(tmp_path, journal)
        mux = WsPostMux(response_timeout_s=0.01)
        sending = _Socket(None)
        send_epoch = mux.attach(sending)
        send_reader = asyncio.create_task(mux.receive_loop(send_epoch))
        attempt = await lane.execute_ioc(
            action=_action(),
            asset_id=0,
            limit_px=Decimal("1000"),
            mux=mux,
            required_epoch=send_epoch,
        )
        assert attempt.record.state is ActionState.UNKNOWN
        await sending.queue.put(_Socket.EOF)
        await send_reader

        status = _Socket(
            {
                "type": "info",
                "payload": {
                    "type": "orderStatus",
                    "data": {
                        "status": "order",
                        "order": {
                            "order": {"origSz": "0.01", "sz": "0.006"},
                            "status": "canceled",
                        },
                    },
                },
            }
        )
        status_epoch = mux.attach(status)
        status_reader = asyncio.create_task(mux.receive_loop(status_epoch))
        resolved = await lane.resolve_by_cloid(
            attempt.record.cloid,
            mux=mux,
            required_epoch=status_epoch,
        )

        assert resolved.state is ActionState.PARTIALLY_FILLED
        assert resolved.cumulative_filled_size == Decimal("0.004")
        await status.queue.put(_Socket.EOF)
        await status_reader
