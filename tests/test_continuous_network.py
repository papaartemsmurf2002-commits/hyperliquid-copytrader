from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from hyperliquid_copytrader import continuous_network
from hyperliquid_copytrader.account_stream import AccountStream
from hyperliquid_copytrader.continuous_network import (
    ContinuousNetworkDriver,
    ContinuousNetworkError,
    DurableSourceGapRepair,
    FatalContinuousNetworkError,
    ReconnectPolicy,
)
from hyperliquid_copytrader.continuous_config import BoundContinuousSlot
from hyperliquid_copytrader.continuous_runtime import ContinuousRuntime, Dispatch
from hyperliquid_copytrader.market_stream import MarketSubscriptionChange
from hyperliquid_copytrader.ws_actions import PostOutcome, PostResult, WsPostMux


SOURCE = "0x" + "1" * 40
SOURCE_2 = "0x" + "3" * 40
FOLLOWER = "0x" + "2" * 40


class _Socket:
    _EOF = object()

    def __init__(self) -> None:
        self.sent: list[str] = []
        self.sent_event = asyncio.Event()
        self.incoming: asyncio.Queue[Any] = asyncio.Queue()

    def __aiter__(self) -> _Socket:
        return self

    async def __anext__(self) -> Any:
        value = await self.incoming.get()
        if value is self._EOF:
            raise StopAsyncIteration
        if isinstance(value, BaseException):
            raise value
        return value

    async def send(self, message: str) -> None:
        self.sent.append(message)
        self.sent_event.set()

    async def emit(self, value: MappingLike) -> None:
        await self.incoming.put(json.dumps(value))


class _PingWriteHangsSocket(_Socket):
    async def send(self, message: str) -> None:
        self.sent.append(message)
        self.sent_event.set()
        if json.loads(message) == {"method": "ping"}:
            await asyncio.Event().wait()


MappingLike = dict[str, Any]


class _Runtime:
    source_subscriptions: tuple[dict[str, Any], ...] = ({"type": "userFills", "user": SOURCE},)
    market_subscriptions: tuple[dict[str, str], ...] = ()
    slot_ids: tuple[str, ...] = ("slot-1",)

    def __init__(self) -> None:
        self.source_epoch = 0
        self.market_epoch = 0
        self.source_messages: list[dict[str, Any]] = []
        self.market_messages: list[dict[str, Any]] = []
        self.gaps: list[tuple[str, int]] = []
        self.accepted_ms = 0
        self.reject_source = False
        self.drive_args: list[tuple[str, int, int | None]] = []
        self.reconciles: list[str] = []
        self.reconciliation_observed_ms = 0
        self.accept_reconciliation = True

    def begin_source_connection(self, *, received_ms: int) -> int:
        self.source_epoch += 1
        return self.source_epoch

    def begin_market_connection(self, *, received_ms: int) -> int:
        self.market_epoch += 1
        return self.market_epoch

    def connection_gap(self, kind: str, *, epoch: int, reason: str) -> None:
        self.gaps.append((kind, epoch))

    def source_slot_id(self, source_address: str) -> str | None:
        source = str(source_address).strip().lower()
        if source == SOURCE:
            return "slot-1"
        if source == SOURCE_2 and "slot-2" in self.slot_ids:
            return "slot-2"
        return None

    async def apply_source(
        self,
        message: dict[str, Any],
        *,
        epoch: int,
        received_ms: int,
        received_mono_ns: int,
        drive: bool = True,
    ) -> Dispatch:
        assert epoch == self.source_epoch
        self.source_messages.append(message)
        self.accepted_ms = received_ms
        if self.reject_source:
            return Dispatch("slot-1", None, "source frame failed: rejected")
        self.market_subscriptions = (
            {"type": "activeAssetCtx", "coin": "BTC"},
            {"type": "l2Book", "coin": "BTC"},
        )
        return Dispatch(
            "slot-1",
            None,
            "accepted",
            MarketSubscriptionChange(("BTC",), ()),
            source_frame_accepted=True,
        )

    async def apply_market(
        self, message: dict[str, Any], *, epoch: int, received_ms: int, drive: bool = True
    ) -> tuple[Dispatch, ...]:
        assert epoch == self.market_epoch
        self.market_messages.append(message)
        return (Dispatch("slot-1", None, "market accepted"),)

    async def reconcile_follower(
        self, slot_id: str, *, now_ms: int, drive: bool = True
    ) -> Dispatch:
        self.reconciles.append(slot_id)
        if self.accept_reconciliation:
            self.reconciliation_observed_ms = now_ms
            return Dispatch(slot_id, None, "reconciled")
        return Dispatch(slot_id, None, "follower refresh deferred while action is reserved")

    def operational_status(self, slot_id: str, *, now_ms: int) -> dict[str, object]:
        del slot_id, now_ms
        return {"last_successful_sync_ms": self.reconciliation_observed_ms}

    async def drive_slot(
        self, slot_id: str, *, now_ms: int, received_mono_ns: int | None = None
    ) -> Dispatch:
        self.drive_args.append((slot_id, now_ms, received_mono_ns))
        return Dispatch(slot_id, None, "driven")

    def source_frame_status(self, slot_id: str, *, received_ms: int) -> tuple[bool, bool]:
        return received_ms == self.accepted_ms, True


class _AckBlockingRuntime(_Runtime):
    source_subscriptions = (
        {"type": "userFills", "user": SOURCE},
        {"type": "userFills", "user": SOURCE_2},
    )
    slot_ids = ("slot-1", "slot-2")

    def __init__(self) -> None:
        super().__init__()
        self.ack_started = asyncio.Event()
        self.ack_release = asyncio.Event()
        self.drives: list[str] = []
        self.slot_locks = {slot_id: asyncio.Lock() for slot_id in self.slot_ids}
        self.open_source_epoch: int | None = None
        self.source_apply_attempts = 0
        self.second_slot_one_apply_started = asyncio.Event()

    def begin_source_connection(self, *, received_ms: int) -> int:
        epoch = super().begin_source_connection(received_ms=received_ms)
        self.open_source_epoch = epoch
        return epoch

    def connection_gap(self, kind: str, *, epoch: int, reason: str) -> None:
        super().connection_gap(kind, epoch=epoch, reason=reason)
        if kind == "source" and self.open_source_epoch == epoch:
            self.open_source_epoch = None

    async def apply_source(
        self,
        message: dict[str, Any],
        *,
        epoch: int,
        received_ms: int,
        received_mono_ns: int,
        drive: bool = True,
    ) -> Dispatch:
        assert not drive
        user = str(message["data"]["user"])
        slot = "slot-1" if user == SOURCE else "slot-2"
        self.source_apply_attempts += 1
        if slot == "slot-1" and self.source_apply_attempts == 2:
            self.second_slot_one_apply_started.set()
        async with self.slot_locks[slot]:
            if epoch != self.open_source_epoch:
                return Dispatch(slot, None, "closed source epoch")
            self.source_messages.append(message)
            return Dispatch(slot, None, "reduced", source_frame_accepted=True)

    async def drive_slot(
        self, slot_id: str, *, now_ms: int, received_mono_ns: int | None = None
    ) -> Dispatch:
        async with self.slot_locks[slot_id]:
            self.drives.append(slot_id)
            if slot_id == "slot-1":
                self.ack_started.set()
                await self.ack_release.wait()
            return Dispatch(slot_id, None, "acknowledged")

    def source_frame_status(self, slot_id: str, *, received_ms: int) -> tuple[bool, bool]:
        return True, True


class _CloseRuntime(_Runtime):
    execution_enabled = True

    def __init__(self) -> None:
        super().__init__()
        self.close_requested: tuple[tuple[str, ...], str] | None = None
        self.close_drives = 0
        self.authoritative_refreshes = 0
        self.last_refresh_ms = 0
        self.required_refresh_ms: list[int] = []
        self._fail_close_slots: tuple[str, ...] = ("slot-1",)
        self.rearmed: tuple[str, ...] = ()
        self.rearmed_event = asyncio.Event()

    @property
    def fail_close_slots(self) -> tuple[str, ...]:
        return self._fail_close_slots

    def operator_rearm(self, slot_ids) -> None:  # type: ignore[no-untyped-def]
        self.rearmed = tuple(slot_ids)
        self._fail_close_slots = ()
        self.rearmed_event.set()

    def source_is_ready(self, slot_id: str, *, now_ms: int) -> bool:
        del slot_id, now_ms
        return True

    def request_fail_close(self, slot_ids, *, reason: str) -> None:
        self.close_requested = (tuple(slot_ids), reason)

    async def reconcile_follower(
        self, slot_id: str, *, now_ms: int, drive: bool = True
    ) -> Dispatch:
        assert drive is False
        self.authoritative_refreshes += 1
        self.last_refresh_ms = now_ms
        return Dispatch(slot_id, None, "authoritative follower refresh")

    def follower_is_flat(
        self,
        slot_id: str,
        *,
        observed_at_least_ms: int | None = None,
    ) -> bool:
        del slot_id
        if observed_at_least_ms is not None:
            self.required_refresh_ms.append(observed_at_least_ms)
        return bool(
            self.close_drives > 0
            and self.authoritative_refreshes > 1
            and (observed_at_least_ms is None or self.last_refresh_ms >= observed_at_least_ms)
        )

    async def drive_fail_close(self, slot_id: str, *, now_ms: int) -> Dispatch:
        del now_ms
        self.close_drives += 1
        return Dispatch(slot_id, None, "reduce-only close submitted")


def _driver(runtime: _Runtime, mux: WsPostMux, **kwargs: Any) -> ContinuousNetworkDriver:
    return ContinuousNetworkDriver(
        runtime=cast(ContinuousRuntime, runtime),
        mux=mux,
        ws_url="wss://example.invalid/ws",
        wall_ms=iter(range(1_750_000_000_000, 1_750_000_001_000)).__next__,
        mono_ns=iter(range(9_000_000, 10_000_000)).__next__,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_close_out_waits_for_authoritative_flat_refresh() -> None:
    runtime, mux = _CloseRuntime(), WsPostMux()

    async def no_delay(_seconds: float) -> None:
        return None

    driver = _driver(runtime, mux, sleep=no_delay)
    driver._action_connected.set()

    await driver.close_out_all(reason="planned duration elapsed")

    assert runtime.close_requested == (("slot-1",), "planned duration elapsed")
    assert runtime.close_drives == 1
    assert runtime.authoritative_refreshes == 2
    assert runtime.required_refresh_ms == [1_750_000_000_000, 1_750_000_000_002]


def test_reconnect_delay_saturates_without_large_integer_overflow() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    driver = _driver(runtime, mux, random_unit=lambda: 1.0)

    assert driver._delay(1_024) == driver.policy.maximum_delay_s
    assert driver._delay(1_000_000) == driver.policy.maximum_delay_s


@pytest.mark.asyncio
async def test_restored_fail_close_is_rearmed_only_after_authoritative_flatness() -> None:
    runtime, mux = _CloseRuntime(), WsPostMux()

    async def yield_control(_seconds: float) -> None:
        await asyncio.sleep(0)

    driver = _driver(
        runtime,
        mux,
        rearm_restored_fail_close=True,
        sleep=yield_control,
    )
    driver._action_connected.set()

    task = asyncio.create_task(driver._source_fail_close_loop())
    try:
        await asyncio.wait_for(runtime.rearmed_event.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert runtime.close_requested == (("slot-1",), "restored incomplete fail-close")
    assert runtime.close_drives == 1
    assert runtime.authoritative_refreshes == 2
    assert runtime.rearmed == ("slot-1",)
    assert runtime.fail_close_slots == ()


@pytest.mark.asyncio
async def test_source_routes_dynamic_market_subscriptions_and_both_sockets_ping() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    driver = _driver(runtime, mux, policy=ReconnectPolicy(heartbeat_s=0.01))
    source, market = _Socket(), _Socket()
    market_task = asyncio.create_task(driver._market_session(market))
    source_task = asyncio.create_task(driver._source_session(source))
    reducer = asyncio.create_task(driver._source_reduce_loop("slot-1"))
    try:
        await source.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": True, "fills": []},
            }
        )
        for _ in range(100):
            frames = [json.loads(item) for item in market.sent]
            if any(
                item.get("method") == "subscribe"
                and item.get("subscription", {}).get("coin") == "BTC"
                for item in frames
            ) and any(json.loads(item) == {"method": "ping"} for item in source.sent):
                break
            await asyncio.sleep(0.002)
        assert runtime.source_messages
        market_frames = [json.loads(item) for item in market.sent]
        assert {
            item["subscription"]["type"]
            for item in market_frames
            if item.get("method") == "subscribe"
        } == {"activeAssetCtx", "l2Book"}
        assert {"method": "ping"} in [json.loads(item) for item in source.sent]
        assert {"method": "ping"} in market_frames
    finally:
        source_task.cancel()
        market_task.cancel()
        reducer.cancel()
        await asyncio.gather(source_task, market_task, reducer, return_exceptions=True)
    assert ("source", 1) in runtime.gaps
    assert ("market", 1) in runtime.gaps


@pytest.mark.asyncio
async def test_live_market_change_replaces_socket_subscriptions_without_reconnect() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    runtime.market_subscriptions = (
        {"type": "activeAssetCtx", "coin": "BTC"},
        {"type": "l2Book", "coin": "BTC"},
    )
    driver = _driver(runtime, mux, policy=ReconnectPolicy(heartbeat_s=30))
    market = _Socket()
    session = asyncio.create_task(driver._market_session(market))
    try:
        for _ in range(100):
            frames = [json.loads(item) for item in market.sent]
            if sum(item.get("method") == "subscribe" for item in frames) == 2:
                break
            await asyncio.sleep(0.002)

        runtime.market_subscriptions = (
            {"type": "activeAssetCtx", "coin": "ETH"},
            {"type": "l2Book", "coin": "ETH"},
        )
        await driver.notify_market_change(MarketSubscriptionChange(("ETH",), ("BTC",)))
        for _ in range(100):
            frames = [json.loads(item) for item in market.sent]
            if (
                sum(item.get("method") == "unsubscribe" for item in frames) == 2
                and sum(
                    item.get("method") == "subscribe"
                    and item.get("subscription", {}).get("coin") == "ETH"
                    for item in frames
                )
                == 2
            ):
                break
            await asyncio.sleep(0.002)

        frames = [json.loads(item) for item in market.sent]
        assert {
            (item["method"], item["subscription"]["type"], item["subscription"]["coin"])
            for item in frames
            if item.get("method") in {"subscribe", "unsubscribe"}
        } == {
            ("subscribe", "activeAssetCtx", "BTC"),
            ("subscribe", "l2Book", "BTC"),
            ("unsubscribe", "activeAssetCtx", "BTC"),
            ("unsubscribe", "l2Book", "BTC"),
            ("subscribe", "activeAssetCtx", "ETH"),
            ("subscribe", "l2Book", "ETH"),
        }
        assert not session.done()
    finally:
        session.cancel()
        await asyncio.gather(session, return_exceptions=True)


@pytest.mark.asyncio
async def test_action_socket_is_request_correlated_and_application_heartbeat_is_separate() -> None:
    runtime, mux, socket = _Runtime(), WsPostMux(), _Socket()
    driver = _driver(runtime, mux, policy=ReconnectPolicy(heartbeat_s=30))
    session = asyncio.create_task(driver._action_session(socket))
    try:
        await asyncio.wait_for(driver._action_connected.wait(), timeout=1)
        request = asyncio.create_task(
            mux.post_info({"type": "allMids"}, required_epoch=mux.capture_epoch())
        )
        while not socket.sent:
            await asyncio.sleep(0)
        frame = json.loads(socket.sent[-1])
        assert frame["request"] == {"type": "info", "payload": {"type": "allMids"}}
        await socket.emit(
            {
                "channel": "post",
                "data": {
                    "id": frame["id"],
                    "response": {
                        "type": "info",
                        "payload": {"type": "allMids", "data": {"BTC": "100"}},
                    },
                },
            }
        )
        assert (await request).outcome is PostOutcome.INFO
    finally:
        session.cancel()
        await asyncio.gather(session, return_exceptions=True)
    assert mux.connection_epoch is None


@pytest.mark.asyncio
async def test_each_action_reconnect_wakes_immediate_follower_reconciliation() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    driver = _driver(runtime, mux, policy=ReconnectPolicy(reconciliation_s=60))
    reconcile = asyncio.create_task(driver._reconcile_slot_loop("slot-1"))
    first = asyncio.create_task(driver._action_session(_Socket()))
    second: asyncio.Task[None] | None = None
    try:
        for _ in range(100):
            if runtime.reconciles == ["slot-1"]:
                break
            await asyncio.sleep(0)
        assert runtime.reconciles == ["slot-1"]
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)

        second = asyncio.create_task(driver._action_session(_Socket()))
        for _ in range(100):
            if runtime.reconciles == ["slot-1", "slot-1"]:
                break
            await asyncio.sleep(0)
        assert runtime.reconciles == ["slot-1", "slot-1"]
    finally:
        first.cancel()
        if second is not None:
            second.cancel()
        reconcile.cancel()
        await asyncio.gather(
            first,
            *(tuple([second]) if second is not None else ()),
            reconcile,
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_deferred_refresh_does_not_advance_last_successful_sync() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    runtime.accept_reconciliation = False
    driver = _driver(runtime, mux, policy=ReconnectPolicy(reconciliation_s=60))
    driver._action_connected.set()
    driver._reconcile_wakes["slot-1"].set()
    reconcile = asyncio.create_task(driver._reconcile_slot_loop("slot-1"))
    try:
        for _ in range(100):
            if len(runtime.reconciles) == 1:
                break
            await asyncio.sleep(0)
        assert runtime.reconciles == ["slot-1"]
        assert driver.operational_status()["slots"]["slot-1"]["last_successful_sync_ms"] == 0

        runtime.accept_reconciliation = True
        driver._reconcile_wakes["slot-1"].set()
        for _ in range(100):
            if len(runtime.reconciles) == 2:
                break
            await asyncio.sleep(0)
        assert runtime.reconciles == ["slot-1", "slot-1"]
        assert (
            driver.operational_status()["slots"]["slot-1"]["last_successful_sync_ms"]
            == runtime.reconciliation_observed_ms
        )
        assert runtime.reconciliation_observed_ms > 0
    finally:
        reconcile.cancel()
        await asyncio.gather(reconcile, return_exceptions=True)


@pytest.mark.asyncio
async def test_explicit_acceptance_bit_beats_a_colliding_receive_timestamp() -> None:
    runtime, mux, source = _Runtime(), WsPostMux(), _Socket()
    runtime.reject_source = True
    driver = _driver(runtime, mux)
    session = asyncio.create_task(driver._source_session(source))
    reducer = asyncio.create_task(driver._source_reduce_loop("slot-1"))
    await source.emit(
        {
            "channel": "userFills",
            "data": {"user": SOURCE, "isSnapshot": True, "fills": []},
        }
    )
    for _ in range(100):
        if runtime.source_messages:
            break
        await asyncio.sleep(0)
    assert runtime.source_messages
    assert not reducer.done()
    assert not driver._slot_wakes["slot-1"].event.is_set()
    reducer.cancel()
    session.cancel()
    await asyncio.gather(session, reducer, return_exceptions=True)
    # The compatibility timestamp hook reports true, demonstrating why the
    # explicit Dispatch bit is the persistence/continuation boundary.
    assert runtime.source_frame_status("slot-1", received_ms=runtime.accepted_ms)[0]


@pytest.mark.asyncio
async def test_second_leader_frame_is_reduced_while_first_slot_ack_is_held() -> None:
    runtime, mux, source = _AckBlockingRuntime(), WsPostMux(), _Socket()
    driver = _driver(runtime, mux)
    reader = asyncio.create_task(driver._source_session(source))
    reducers = [
        asyncio.create_task(driver._source_reduce_loop(slot_id)) for slot_id in runtime.slot_ids
    ]
    actors = [asyncio.create_task(driver._slot_actor(slot_id)) for slot_id in runtime.slot_ids]
    try:
        await source.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": True, "fills": []},
            }
        )
        await asyncio.wait_for(runtime.ack_started.wait(), timeout=1)
        await source.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE_2, "isSnapshot": True, "fills": []},
            }
        )
        for _ in range(100):
            if len(runtime.source_messages) == 2 and "slot-2" in runtime.drives:
                break
            await asyncio.sleep(0)
        assert len(runtime.source_messages) == 2
        assert "slot-2" in runtime.drives
        assert not runtime.ack_release.is_set()
    finally:
        runtime.ack_release.set()
        reader.cancel()
        for reducer in reducers:
            reducer.cancel()
        for actor in actors:
            actor.cancel()
        await asyncio.gather(reader, *reducers, *actors, return_exceptions=True)


@pytest.mark.asyncio
async def test_disconnect_discards_same_slot_backlog_before_next_source_epoch() -> None:
    runtime, mux, first = _AckBlockingRuntime(), WsPostMux(), _Socket()
    driver = _driver(runtime, mux)
    reducers = [
        asyncio.create_task(driver._source_reduce_loop(slot_id)) for slot_id in runtime.slot_ids
    ]
    actors = [asyncio.create_task(driver._slot_actor(slot_id)) for slot_id in runtime.slot_ids]
    first_session = asyncio.create_task(driver._source_session(first))
    second_session: asyncio.Task[None] | None = None
    try:
        await first.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": True, "fills": []},
            }
        )
        await asyncio.wait_for(runtime.ack_started.wait(), timeout=1)
        await first.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": False, "fills": []},
            }
        )
        await asyncio.wait_for(runtime.second_slot_one_apply_started.wait(), timeout=1)
        await first.incoming.put(first._EOF)
        for _ in range(100):
            if runtime.open_source_epoch is None:
                break
            await asyncio.sleep(0)
        assert runtime.open_source_epoch is None

        runtime.ack_release.set()
        with pytest.raises(ConnectionError, match="source socket"):
            await first_session
        assert len(runtime.source_messages) == 1
        assert driver.discarded_source_frames == 1

        second = _Socket()
        second_session = asyncio.create_task(driver._source_session(second))
        await second.emit(
            {
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": True, "fills": []},
            }
        )
        for _ in range(100):
            if len(runtime.source_messages) == 2:
                break
            await asyncio.sleep(0)
        assert len(runtime.source_messages) == 2
        assert runtime.source_epoch == 2
    finally:
        runtime.ack_release.set()
        first_session.cancel()
        if second_session is not None:
            second_session.cancel()
        for task in [*reducers, *actors]:
            task.cancel()
        await asyncio.gather(
            first_session,
            *(tuple([second_session]) if second_session is not None else ()),
            *reducers,
            *actors,
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_slot_wake_uses_current_wall_time_and_earliest_pending_ingress() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    wall = iter((1_000, 2_000, 3_000)).__next__
    driver = ContinuousNetworkDriver(
        runtime=cast(ContinuousRuntime, runtime),
        mux=mux,
        ws_url="wss://example.invalid/ws",
        wall_ms=wall,
    )
    driver._wake_slot("slot-1", now_ms=100, received_mono_ns=900)
    driver._wake_slot("slot-1", now_ms=200, received_mono_ns=950)
    actor = asyncio.create_task(driver._slot_actor("slot-1"))
    try:
        for _ in range(100):
            if runtime.drive_args:
                break
            await asyncio.sleep(0)
        assert runtime.drive_args == [("slot-1", 1_000, 900)]
    finally:
        actor.cancel()
        await asyncio.gather(actor, return_exceptions=True)


class _InfoMux:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.requests: list[dict[str, Any]] = []

    def capture_epoch(self) -> int:
        return 7

    async def post_info(self, payload: dict[str, Any], *, required_epoch: int) -> PostResult:
        assert required_epoch == 7
        self.requests.append(payload)
        return PostResult(
            1,
            PostOutcome.INFO,
            {
                "type": "info",
                "payload": {"type": "userFillsByTime", "data": self.rows},
            },
            "info_response",
        )


def _fill(
    *, tx: str, tid: int, start: str, size: str, time_ms: int, coin: str = "BTC"
) -> dict[str, Any]:
    return {
        "coin": coin,
        "hash": tx,
        "tid": tid,
        "time": time_ms,
        "side": "B",
        "sz": size,
        "px": "100",
        "startPosition": start,
    }


@pytest.mark.asyncio
async def test_gap_repair_uses_durable_identities_and_exact_bounded_chain(tmp_path: Path) -> None:
    known = _fill(tx="0xknown", tid=1, start="0", size="1", time_ms=10_000)
    missing = _fill(tx="0xmissing", tid=2, start="1", size="2", time_ms=11_000)
    mux = _InfoMux([known, missing])
    repair = DurableSourceGapRepair(
        mux=cast(WsPostMux, mux),
        path=tmp_path / "gap.sqlite3",
        clock_ms=lambda: 15_000,
        overlap_ms=1_000,
        maximum_window_ms=10_000,
    )
    try:
        repair.record_accepted(
            source=SOURCE,
            message={
                "channel": "userFills",
                "data": {"user": SOURCE, "isSnapshot": False, "fills": [known]},
            },
            received_ms=10_000,
            source_ready=True,
        )
        repair.mark_gap((SOURCE,), when_ms=10_500)
        repair.begin_connection((SOURCE,))
        repair.stage_source_frame(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": SOURCE,
                    "clearinghouseStates": [["", {"time": 15_000, "assetPositions": []}]],
                },
            }
        )
        repair.record_accepted(
            source=SOURCE,
            message={"channel": "spotState", "data": {"user": SOURCE}},
            received_ms=12_000,
            source_ready=False,
        )
        assert (
            repair.db.execute(
                "SELECT last_good_ms FROM source_cursor WHERE source=?", (SOURCE,)
            ).fetchone()[0]
            == 10_000
        )
        slot = cast(
            BoundContinuousSlot,
            SimpleNamespace(
                config=SimpleNamespace(
                    source_address=SOURCE,
                    follower_account_address=FOLLOWER,
                    allowed_markets=("BTC",),
                )
            ),
        )
        records = await repair(
            slot=slot,
            before={"BTC": Decimal("1")},
            after={"BTC": Decimal("3")},
        )
        assert [(item.tx_hash, item.tid) for item in records] == [("0xmissing", "2")]
        repair.record_accepted(
            source=SOURCE,
            message={
                "channel": "allDexsClearinghouseState",
                "data": {"user": SOURCE},
            },
            received_ms=15_000,
            source_ready=True,
        )
        assert (
            repair.db.execute(
                "SELECT COUNT(*) FROM source_fill_identity WHERE source=? AND tx_hash=?",
                (SOURCE, "0xmissing"),
            ).fetchone()[0]
            == 1
        )
        assert mux.requests == [
            {
                "type": "userFillsByTime",
                "user": SOURCE,
                "startTime": 9_000,
                "endTime": 15_000,
                "aggregateByTime": False,
            }
        ]
    finally:
        repair.close()


@pytest.mark.asyncio
async def test_dynamic_gap_repair_does_not_filter_new_market_fills(tmp_path: Path) -> None:
    missing = _fill(
        tx="0xdynamic",
        tid=2,
        start="-238.831",
        size="0.438",
        time_ms=11_000,
        coin="xyz:KIOXIA",
    )
    missing["side"] = "A"
    mux = _InfoMux([missing])
    repair = DurableSourceGapRepair(
        mux=cast(WsPostMux, mux),
        path=tmp_path / "gap.sqlite3",
        clock_ms=lambda: 15_000,
        overlap_ms=1_000,
        maximum_window_ms=10_000,
    )
    try:
        repair.record_accepted(
            source=SOURCE,
            message={"channel": "spotState", "data": {"user": SOURCE}},
            received_ms=10_000,
            source_ready=True,
        )
        repair.begin_connection((SOURCE,))
        repair.stage_source_frame(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": SOURCE,
                    "clearinghouseStates": [
                        ["xyz", {"time": 15_000, "assetPositions": []}]
                    ],
                },
            }
        )
        slot = cast(
            BoundContinuousSlot,
            SimpleNamespace(
                dynamic_market_eligibility=True,
                config=SimpleNamespace(
                    source_address=SOURCE,
                    follower_account_address=FOLLOWER,
                    allowed_markets=(),
                ),
            ),
        )

        records = await repair(
            slot=slot,
            before={"xyz:KIOXIA": Decimal("-238.831")},
            after={"xyz:KIOXIA": Decimal("-239.269")},
        )

        assert [(item.market, item.signed_size) for item in records] == [
            ("xyz:KIOXIA", Decimal("-0.438"))
        ]
    finally:
        repair.close()


@pytest.mark.asyncio
async def test_gap_repair_uses_http_fallback_when_ws_rows_do_not_connect(tmp_path: Path) -> None:
    missing = _fill(tx="0xmissing", tid=2, start="1", size="2", time_ms=11_000)
    mux = _InfoMux([])
    fallback_calls: list[dict[str, Any]] = []

    async def fallback(*, user: str, start_ms: int, end_ms: int):  # type: ignore[no-untyped-def]
        fallback_calls.append({"user": user, "start_ms": start_ms, "end_ms": end_ms})
        return [missing]

    repair = DurableSourceGapRepair(
        mux=cast(WsPostMux, mux),
        path=tmp_path / "gap.sqlite3",
        fallback=fallback,
        clock_ms=lambda: 15_000,
        overlap_ms=1_000,
        maximum_window_ms=10_000,
    )
    try:
        repair.record_accepted(
            source=SOURCE,
            message={"channel": "spotState", "data": {"user": SOURCE}},
            received_ms=10_000,
            source_ready=True,
        )
        repair.mark_gap((SOURCE,), when_ms=10_500)
        repair.begin_connection((SOURCE,))
        repair.stage_source_frame(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": SOURCE,
                    "clearinghouseStates": [["", {"time": 15_000, "assetPositions": []}]],
                },
            }
        )
        slot = cast(
            BoundContinuousSlot,
            SimpleNamespace(
                config=SimpleNamespace(
                    source_address=SOURCE,
                    follower_account_address=FOLLOWER,
                    allowed_markets=("BTC",),
                )
            ),
        )

        records = await repair(
            slot=slot,
            before={"BTC": Decimal("1")},
            after={"BTC": Decimal("3")},
        )

        assert [item.tx_hash for item in records] == ["0xmissing"]
        assert fallback_calls == [{"user": SOURCE, "start_ms": 9_000, "end_ms": 15_000}]
    finally:
        repair.close()


def test_fill_chain_uses_start_position_for_same_millisecond_ordering() -> None:
    parser = AccountStream(source=SOURCE, follower=FOLLOWER, source_markets=("BTC",))
    epoch = parser.begin_connection(received_ms=12_000)
    parser.apply(
        {
            "channel": "userFills",
            "data": {
                "user": SOURCE,
                "isSnapshot": True,
                "fills": [
                    _fill(tx="0xzz", tid=2, start="1", size="1", time_ms=11_000),
                    _fill(tx="0xaa", tid=3, start="2", size="1", time_ms=11_000),
                ],
            },
        },
        epoch=epoch,
        received_ms=12_000,
    )

    chain = continuous_network._connect_fill_chain(
        parser.source.fills,
        known=set(),
        before={"BTC": Decimal("1")},
        after={"BTC": Decimal("3")},
    )

    assert [item.start_position for item in chain] == [Decimal("1"), Decimal("2")]


@pytest.mark.asyncio
async def test_gap_horizon_ignores_old_flat_native_dex_for_active_hip3(tmp_path: Path) -> None:
    missing = _fill(
        tx="0xhip3",
        tid=3,
        start="0",
        size="2",
        time_ms=11_000,
        coin="xyz:EWY",
    )
    mux = _InfoMux([missing])
    repair = DurableSourceGapRepair(
        mux=cast(WsPostMux, mux),
        path=tmp_path / "hip3-gap.sqlite3",
        clock_ms=lambda: 15_000,
        overlap_ms=1_000,
        maximum_window_ms=10_000,
    )
    try:
        repair.record_accepted(
            source=SOURCE,
            message={"channel": "spotState", "data": {"user": SOURCE}},
            received_ms=9_000,
            source_ready=True,
        )
        repair.mark_gap((SOURCE,), when_ms=10_000)
        repair.begin_connection((SOURCE,))
        repair.stage_source_frame(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": SOURCE,
                    "clearinghouseStates": [
                        ["", {"time": 1_000, "assetPositions": []}],
                        ["xyz", {"time": 15_000, "assetPositions": []}],
                    ],
                },
            }
        )
        slot = cast(
            BoundContinuousSlot,
            SimpleNamespace(
                config=SimpleNamespace(
                    source_address=SOURCE,
                    follower_account_address=FOLLOWER,
                    allowed_markets=("xyz:EWY",),
                )
            ),
        )
        records = await repair(
            slot=slot,
            before={"xyz:EWY": Decimal("0")},
            after={"xyz:EWY": Decimal("2")},
        )
        assert [item.tx_hash for item in records] == ["0xhip3"]
        assert mux.requests[0]["startTime"] == 8_000
        assert mux.requests[0]["endTime"] == 15_000
    finally:
        repair.close()


@pytest.mark.asyncio
async def test_silent_socket_repair_starts_at_last_accepted_frame_not_detection(
    tmp_path: Path,
) -> None:
    missing = _fill(tx="0xsilent", tid=4, start="0", size="1", time_ms=20_000)
    mux = _InfoMux([missing])
    repair = DurableSourceGapRepair(
        mux=cast(WsPostMux, mux),
        path=tmp_path / "silent-gap.sqlite3",
        clock_ms=lambda: 50_000,
        overlap_ms=1_000,
        maximum_window_ms=100_000,
    )
    try:
        repair.record_accepted(
            source=SOURCE,
            message={"channel": "spotState", "data": {"user": SOURCE}},
            received_ms=10_000,
            source_ready=True,
        )
        # Detection happens 30 seconds later; replay must still include the
        # fill that arrived while the dead socket looked connected.
        repair.mark_gap((SOURCE,), when_ms=40_000)
        repair.begin_connection((SOURCE,))
        repair.stage_source_frame(
            {
                "channel": "allDexsClearinghouseState",
                "data": {
                    "user": SOURCE,
                    "clearinghouseStates": [["", {"time": 50_000, "assetPositions": []}]],
                },
            }
        )
        slot = cast(
            BoundContinuousSlot,
            SimpleNamespace(
                config=SimpleNamespace(
                    source_address=SOURCE,
                    follower_account_address=FOLLOWER,
                    allowed_markets=("BTC",),
                )
            ),
        )
        records = await repair(
            slot=slot,
            before={"BTC": Decimal("0")},
            after={"BTC": Decimal("1")},
        )
        assert [item.tx_hash for item in records] == ["0xsilent"]
        assert mux.requests[0]["startTime"] == 9_000
        assert mux.requests[0]["endTime"] == 50_000
    finally:
        repair.close()


@pytest.mark.asyncio
async def test_reconnect_attempts_and_backoff_are_bounded() -> None:
    attempts: list[str] = []
    delays: list[float] = []

    @asynccontextmanager
    async def connector(name: str, _url: str):  # type: ignore[no-untyped-def]
        attempts.append(name)
        raise ConnectionError("offline")
        yield _Socket()

    async def no_sleep(delay: float) -> None:
        delays.append(delay)

    runtime, mux = _Runtime(), WsPostMux()
    driver = _driver(
        runtime,
        mux,
        connector=connector,
        policy=ReconnectPolicy(
            attempts=3,
            minimum_delay_s=0.25,
            maximum_delay_s=1,
            jitter_fraction=0,
        ),
        sleep=no_sleep,
    )
    with pytest.raises(ContinuousNetworkError, match="exhausted 3 consecutive attempts"):
        await driver._supervise("source", driver._source_session)
    assert attempts == ["source", "source", "source"]
    assert delays == [0.25, 0.5]


@pytest.mark.asyncio
async def test_default_reconnect_survives_ten_transient_failures_then_recovers() -> None:
    attempts: list[str] = []
    delays: list[float] = []
    connected = asyncio.Event()

    @asynccontextmanager
    async def connector(name: str, _url: str):  # type: ignore[no-untyped-def]
        attempts.append(name)
        if len(attempts) <= 10:
            raise ConnectionError("offline")
        yield _Socket()

    async def no_sleep(delay: float) -> None:
        delays.append(delay)

    async def session(_socket: _Socket) -> None:
        connected.set()
        await asyncio.Event().wait()

    driver = _driver(_Runtime(), WsPostMux(), connector=connector, sleep=no_sleep)
    supervisor = asyncio.create_task(driver._supervise("source", session))
    try:
        await asyncio.wait_for(connected.wait(), timeout=1)
        assert attempts == ["source"] * 11
        assert len(delays) == 10
        assert max(delays) <= driver.policy.maximum_delay_s
        assert not supervisor.done()
    finally:
        supervisor.cancel()
        await asyncio.gather(supervisor, return_exceptions=True)


@pytest.mark.asyncio
async def test_three_socket_flapping_shares_one_connection_attempt_window() -> None:
    clock_ns = 0
    sleeps: list[float] = []

    async def advance(delay: float) -> None:
        nonlocal clock_ns
        sleeps.append(delay)
        clock_ns += int(delay * 1_000_000_000)

    runtime, mux = _Runtime(), WsPostMux()
    driver = ContinuousNetworkDriver(
        runtime=cast(ContinuousRuntime, runtime),
        mux=mux,
        ws_url="wss://example.invalid/ws",
        wall_ms=lambda: 1_750_000_000_000,
        mono_ns=lambda: clock_ns,
        sleep=advance,
        policy=ReconnectPolicy(connection_attempts_per_minute=3),
    )

    await asyncio.gather(
        driver._admit_connection_attempt("source"),
        driver._admit_connection_attempt("market"),
        driver._admit_connection_attempt("action"),
    )
    assert sleeps == []
    assert len(driver._connection_attempts) == 3

    await driver._admit_connection_attempt("source")
    assert sleeps == [60.0]
    assert len(driver._connection_attempts) == 1


@pytest.mark.asyncio
async def test_non_retryable_protocol_error_is_terminal_without_reconnect() -> None:
    attempts = 0

    @asynccontextmanager
    async def connector(_name: str, _url: str):  # type: ignore[no-untyped-def]
        nonlocal attempts
        attempts += 1
        yield _Socket()

    async def session(_socket: _Socket) -> None:
        raise FatalContinuousNetworkError("invalid subscription")

    driver = _driver(_Runtime(), WsPostMux(), connector=connector)
    with pytest.raises(FatalContinuousNetworkError, match="invalid subscription"):
        await driver._supervise("source", session)
    assert attempts == 1


@pytest.mark.asyncio
async def test_monitoring_overflow_never_blocks_the_copy_path() -> None:
    driver = _driver(_Runtime(), WsPostMux(), event_capacity=1)
    first = Dispatch("slot-1", None, "first")
    second = Dispatch("slot-1", None, "second")
    await driver._publish(first)
    await driver._publish(second)
    assert driver.events.get_nowait() is second
    assert driver.dropped_events == 1


@pytest.mark.asyncio
async def test_unknown_action_wakes_targeted_reconciliation_without_reconnect() -> None:
    class UnknownRuntime(_Runtime):
        async def drive_slot(
            self, slot_id: str, *, now_ms: int, received_mono_ns: int | None = None
        ) -> Dispatch:
            del now_ms, received_mono_ns
            record = SimpleNamespace(
                cloid="0x" + "a" * 32,
                desired_id="desired-1",
                market="BTC",
                requested_size=Decimal("0.01"),
                cumulative_filled_size=Decimal("0"),
                state=SimpleNamespace(value="UNKNOWN"),
                outcome_detail="send outcome unknown",
            )
            result = PostResult(1, PostOutcome.UNKNOWN, None, "response_timeout_after_send")
            attempt = SimpleNamespace(
                record=record,
                result=result,
                received_to_send_ms=Decimal("2"),
                send_to_response_ms=Decimal("2000"),
                execution_context={
                    "execution_class": "entry",
                    "leader_trigger_age_ms": 321,
                    "accepted_target_wait_ms": 123,
                    "source_revision": 7,
                },
            )
            return Dispatch(slot_id, None, "IOC unknown", attempt=attempt)

    runtime, mux = UnknownRuntime(), WsPostMux()
    driver = _driver(runtime, mux)
    driver._action_connected.set()
    reconcile = asyncio.create_task(driver._reconcile_slot_loop("slot-1"))
    actor = asyncio.create_task(driver._slot_actor("slot-1"))
    try:
        driver._wake_slot("slot-1", now_ms=1_000, received_mono_ns=0)
        for _ in range(100):
            if runtime.reconciles:
                break
            await asyncio.sleep(0)
        assert runtime.reconciles == ["slot-1"]
        status = driver.operational_status()["slots"]["slot-1"]
        assert status["latest_execution_class"] == "entry"
        assert status["latest_trigger_age_ms"] == 321
        assert status["latest_target_wait_ms"] == 123
        assert status["latest_source_revision"] == 7
    finally:
        actor.cancel()
        reconcile.cancel()
        await asyncio.gather(actor, reconcile, return_exceptions=True)


def test_leader_to_fill_bps_is_adverse_for_buys_and_sells() -> None:
    assert continuous_network._leader_to_fill_bps(
        average_fill_price=Decimal("100.25"), leader_trigger_px="100", side="buy"
    ) == pytest.approx(25.0)
    assert continuous_network._leader_to_fill_bps(
        average_fill_price=Decimal("99.75"), leader_trigger_px="100", side="sell"
    ) == pytest.approx(25.0)


def test_prolonged_disconnect_alarm_latches_until_stable_restore() -> None:
    clock_ns = [0]
    wall_ms = [1_000_000]
    runtime, mux = _Runtime(), WsPostMux()
    driver = ContinuousNetworkDriver(
        runtime=cast(ContinuousRuntime, runtime),
        mux=mux,
        ws_url="wss://example.invalid/ws",
        mono_ns=lambda: clock_ns[0],
        wall_ms=lambda: wall_ms[0],
        policy=ReconnectPolicy(source_fail_close_s=10, stable_connection_s=3),
    )
    driver._connection_connecting("source")
    clock_ns[0] = 11_000_000_000
    wall_ms[0] += 11_000
    alarmed = driver.operational_status()
    assert alarmed["alarms"][0]["connection"] == "source"

    driver._connection_opened("source")
    restoring = driver.operational_status()
    assert restoring["connections"]["source"]["state"] == "reconnecting"
    assert restoring["alarms"]

    clock_ns[0] += 3_000_000_000
    wall_ms[0] += 3_000
    restored = driver.operational_status()
    assert restored["connections"]["source"]["state"] == "connected"
    assert restored["alarms"] == []


@pytest.mark.asyncio
async def test_application_ping_without_pong_terminates_the_socket() -> None:
    runtime, mux = _Runtime(), WsPostMux()
    frames: list[dict[str, Any]] = []

    async def frame(value: dict[str, Any]) -> None:
        frames.append(value)

    driver = _driver(
        runtime,
        mux,
        policy=ReconnectPolicy(heartbeat_s=0.001, heartbeat_timeout_s=0.005),
    )
    with pytest.raises(ConnectionError, match="no pong"):
        await driver._heartbeat(
            writer=cast(Any, SimpleNamespace(frame=frame)),
            pong=asyncio.Event(),
        )
    assert frames == [{"method": "ping"}]


@pytest.mark.asyncio
async def test_hung_heartbeat_write_terminates_source_session() -> None:
    runtime, mux, source = _Runtime(), WsPostMux(), _PingWriteHangsSocket()
    driver = _driver(
        runtime,
        mux,
        policy=ReconnectPolicy(
            heartbeat_s=0.001,
            heartbeat_timeout_s=0.005,
            write_timeout_s=0.005,
        ),
    )
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(driver._source_session(source), timeout=0.25)
    assert {"method": "ping"} in [json.loads(item) for item in source.sent]
    assert ("source", 1) in runtime.gaps


@pytest.mark.asyncio
async def test_run_starts_and_stops_all_network_actors() -> None:
    @asynccontextmanager
    async def connector(_name: str, _url: str):  # type: ignore[no-untyped-def]
        yield _Socket()

    runtime = _Runtime()
    driver = _driver(runtime, WsPostMux(), connector=connector)
    stop = asyncio.Event()
    stop.set()
    await asyncio.wait_for(driver.run(stop=stop), timeout=1)
    assert runtime.gaps == []


def test_driver_rejects_split_action_mux_ownership() -> None:
    runtime = _Runtime()
    runtime.mux = WsPostMux()  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="share one WS POST mux"):
        _driver(runtime, WsPostMux())


def test_policy_forbids_a_server_timeout_heartbeat() -> None:
    with pytest.raises(ValueError, match="below 60"):
        ReconnectPolicy(heartbeat_s=60)
