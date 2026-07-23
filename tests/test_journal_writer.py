from __future__ import annotations

import asyncio
import sqlite3
from threading import Event, Lock
from time import time_ns
from typing import Any, Mapping

import pytest

from hyperliquid_copytrader import journal_writer as journal_writer_module
from hyperliquid_copytrader.journal_writer import JournalQueueFull, JournalWriter
from hyperliquid_copytrader.persistence import SQLiteStore


def _timing(index: int) -> dict[str, Any]:
    return {
        "timing_id": f"timing-{index}",
        "generation": "generation-test",
        "source_shard": index % 2,
        "slot_id": f"slot-{index % 10}",
        "stage": "test-stage",
        "wall_ms": 1_000 + index,
        "mono_ns": 2_000 + index,
        "duration_ns": index,
        "event_key": f"event-{index}",
        "payload": {"index": index},
    }


@pytest.mark.asyncio
async def test_journal_writer_prioritizes_critical_work_without_starving_telemetry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first_timing_started = Event()
    release_first_timing = Event()
    calls: list[tuple[str, int]] = []
    calls_lock = Lock()

    class FakeStore:
        def __init__(self, _path) -> None:
            self.first_batch = True

        def record_stage_timings(self, *, timings) -> None:
            rows = list(timings)
            with calls_lock:
                calls.append(("timing", len(rows)))
            if self.first_batch:
                self.first_batch = False
                first_timing_started.set()
                if not release_first_timing.wait(2):
                    raise TimeoutError("test did not release first timing batch")

        def critical_write(self, value: int) -> int:
            with calls_lock:
                calls.append(("critical", value))
            return value

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    first = writer.offer("record_stage_timing", **_timing(0))
    assert await asyncio.to_thread(first_timing_started.wait, 1)
    remaining = [writer.offer("record_stage_timing", **_timing(index)) for index in range(1, 21)]
    critical = writer.offer("critical_write", 99)
    release_first_timing.set()

    await asyncio.wait_for(asyncio.gather(first, *remaining), timeout=2)
    assert await asyncio.wait_for(critical, timeout=2) == 99
    await writer.close()

    assert calls == [("timing", 1), ("critical", 99), ("timing", 20)]
    assert writer.health().completed == 22
    assert writer.health().failed == 0


@pytest.mark.asyncio
async def test_cancelled_submit_preserves_authoritative_durable_ack(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    write_started = Event()
    release_write = Event()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def blocking_write(value: int) -> int:
            write_started.set()
            if not release_write.wait(2):
                raise TimeoutError("test did not release durable write")
            return value

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    submit = asyncio.create_task(writer.submit("blocking_write", 99))
    assert await asyncio.to_thread(write_started.wait, 1)
    assert len(writer._active_commands) == 1  # type: ignore[attr-defined]
    ack = writer._active_commands[0].result  # type: ignore[attr-defined]

    submit.cancel()
    with pytest.raises(asyncio.CancelledError):
        await submit
    assert not ack.cancelled()

    release_write.set()
    await asyncio.wait_for(writer.flush(), timeout=2)
    assert ack.result() == 99
    assert writer.health().completed == 1
    assert writer.health().failed == 0
    await writer.close()


@pytest.mark.asyncio
async def test_journal_writer_reserves_queue_capacity_for_critical_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    timing_started = Event()
    release_timing = Event()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        def record_stage_timings(self, *, timings) -> None:
            list(timings)
            timing_started.set()
            if not release_timing.wait(2):
                raise TimeoutError("test did not release timing batch")

        @staticmethod
        def critical_write(value: int) -> int:
            return value

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3", capacity=8)
    await writer.start()
    first = writer.offer("record_stage_timing", **_timing(0))
    assert await asyncio.to_thread(timing_started.wait, 1)
    telemetry = [writer.offer("record_stage_timing", **_timing(index)) for index in range(1, 8)]
    with pytest.raises(JournalQueueFull, match="record_stage_timing"):
        writer.offer("record_stage_timing", **_timing(8))
    critical = writer.offer("critical_write", 99)
    release_timing.set()

    await asyncio.wait_for(asyncio.gather(first, *telemetry), timeout=2)
    assert await asyncio.wait_for(critical, timeout=2) == 99
    await writer.close()


@pytest.mark.asyncio
async def test_journal_writer_limits_critical_burst_before_telemetry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first_started = Event()
    release_first = Event()
    calls: list[tuple[str, int]] = []
    calls_lock = Lock()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        def blocking_critical(self, value: int) -> int:
            with calls_lock:
                calls.append(("critical", value))
            if value == 0:
                first_started.set()
                if not release_first.wait(2):
                    raise TimeoutError("test did not release critical write")
            return value

        def record_stage_timings(self, *, timings) -> None:
            rows = list(timings)
            with calls_lock:
                calls.append(("timing", len(rows)))

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3", capacity=128)
    await writer.start()
    first = writer.offer("blocking_critical", 0)
    assert await asyncio.to_thread(first_started.wait, 1)
    critical = [writer.offer("blocking_critical", index) for index in range(1, 80)]
    timing = writer.offer("record_stage_timing", **_timing(1))
    release_first.set()

    await asyncio.wait_for(asyncio.gather(first, *critical, timing), timeout=2)
    await writer.close()

    timing_index = calls.index(("timing", 1))
    assert calls[:timing_index] == [("critical", index) for index in range(64)]
    assert calls[timing_index + 1 :] == [("critical", index) for index in range(64, 80)]


@pytest.mark.asyncio
async def test_journal_writer_batch_failure_does_not_wedge_later_control_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    failure = RuntimeError("timing transaction failed")

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def record_stage_timings(*, timings) -> None:
            assert len(list(timings)) == 5
            raise failure

        @staticmethod
        def critical_write(value: int) -> int:
            return value

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    timing = [writer.offer("record_stage_timing", **_timing(index)) for index in range(5)]
    outcomes = await asyncio.gather(*timing, return_exceptions=True)
    assert outcomes == [failure] * 5
    assert await writer.submit("critical_write", 99) == 99
    await writer.flush()
    await writer.close()

    assert writer.health().failed == 5
    assert writer.health().completed == 1


@pytest.mark.asyncio
async def test_journal_writer_keeps_event_loop_responsive_and_flush_waits_for_disk(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    timing_started = Event()
    release_timing = Event()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def record_stage_timings(*, timings) -> None:
            list(timings)
            timing_started.set()
            if not release_timing.wait(2):
                raise TimeoutError("test did not release timing write")

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    timing = writer.offer("record_stage_timing", **_timing(0))
    assert await asyncio.to_thread(timing_started.wait, 1)

    loop_tick = asyncio.Event()
    asyncio.get_running_loop().call_soon(loop_tick.set)
    await asyncio.wait_for(loop_tick.wait(), timeout=0.2)
    flushing = asyncio.create_task(writer.flush())
    await asyncio.sleep(0)
    assert not flushing.done()

    release_timing.set()
    await asyncio.wait_for(timing, timeout=2)
    await asyncio.wait_for(flushing, timeout=2)
    await writer.close()
    await writer.close()
    with pytest.raises(RuntimeError, match="stopping"):
        writer.offer("record_stage_timing", **_timing(1))


@pytest.mark.asyncio
async def test_journal_writer_fatal_worker_failure_settles_and_rejects_all_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    class FatalJournalError(BaseException):
        pass

    fatal = FatalJournalError("fatal store failure")

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def fatal_write() -> None:
            raise fatal

        @staticmethod
        def queued_write() -> None:
            raise AssertionError("queued work must not run after a fatal failure")

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    active = writer.offer("fatal_write")
    queued = writer.offer("queued_write")

    outcomes = await asyncio.gather(active, queued, return_exceptions=True)
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    assert all("stopped" in str(outcome) for outcome in outcomes)
    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        writer.offer("queued_write")
    with pytest.raises(RuntimeError, match="stopped unexpectedly"):
        await asyncio.wait_for(writer.flush(), timeout=0.2)
    with pytest.raises(FatalJournalError, match="fatal store failure"):
        await asyncio.wait_for(writer.close(), timeout=0.2)

    assert writer.health().queued == 0
    assert writer.health().failed == 2


@pytest.mark.asyncio
async def test_journal_writer_persists_batched_timing_rows_and_ignores_exact_duplicates(
    tmp_path,
) -> None:
    database = tmp_path / "journal.sqlite3"
    writer = JournalWriter(database)
    await writer.start()
    futures = [writer.offer("record_stage_timing", **_timing(index)) for index in range(50)]
    futures.append(writer.offer("record_stage_timing", **_timing(7)))
    await asyncio.gather(*futures)
    await writer.close()

    connection = sqlite3.connect(database)
    try:
        count = connection.execute("SELECT count(*) FROM stage_timings").fetchone()[0]
        payload = connection.execute(
            "SELECT payload_json FROM stage_timings WHERE timing_id='timing-7'"
        ).fetchone()[0]
    finally:
        connection.close()

    assert count == 50
    assert payload == '{"index":7}'
    assert writer.health().completed == 51
    assert writer.health().failed == 0


@pytest.mark.asyncio
async def test_journal_writer_batches_only_adjacent_matching_critical_commands(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first_started = Event()
    release_first = Event()
    calls: list[tuple[str, object]] = []

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def blocking_critical() -> None:
            first_started.set()
            if not release_first.wait(2):
                raise TimeoutError("test did not release critical write")

        @staticmethod
        def append_runtime_events(*, events):
            items = list(events)
            calls.append(("events", [item["event_key"] for item in items]))
            return [(index, True) for index, _item in enumerate(items, start=1)]

        @staticmethod
        def control_write(value: int) -> int:
            calls.append(("control", value))
            return value

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    blocking = writer.offer("blocking_critical")
    assert await asyncio.to_thread(first_started.wait, 1)
    first = writer.offer("append_runtime_event", event_key="a")
    second = writer.offer("append_runtime_event", event_key="b")
    control = writer.offer("control_write", 7)
    third = writer.offer("append_runtime_event", event_key="c")
    fourth = writer.offer("append_runtime_event", event_key="d")
    release_first.set()

    assert await asyncio.gather(blocking, first, second, control, third, fourth) == [
        None,
        (1, True),
        (2, True),
        7,
        (1, True),
        (2, True),
    ]
    await writer.close()

    assert calls == [
        ("events", ["a", "b"]),
        ("control", 7),
        ("events", ["c", "d"]),
    ]
    assert writer.health().completed == 6


@pytest.mark.asyncio
async def test_journal_writer_resolves_critical_batch_only_after_shared_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first_started = Event()
    release_first = Event()
    batch_started = Event()
    release_batch = Event()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def blocking_critical() -> None:
            first_started.set()
            if not release_first.wait(2):
                raise TimeoutError("test did not release critical write")

        @staticmethod
        def commit_fast_reactions(*, reactions) -> None:
            assert [item["result_id"] for item in reactions] == ["one", "two"]
            batch_started.set()
            if not release_batch.wait(2):
                raise TimeoutError("test did not release critical batch")

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    blocking = writer.offer("blocking_critical")
    assert await asyncio.to_thread(first_started.wait, 1)
    first = writer.offer("commit_fast_reaction", result_id="one")
    second = writer.offer("commit_fast_reaction", result_id="two")
    release_first.set()
    assert await asyncio.to_thread(batch_started.wait, 1)
    assert not first.done()
    assert not second.done()
    assert writer.health().queued == 0
    assert writer.health().oldest_age_ms > 0
    release_batch.set()

    await asyncio.gather(blocking, first, second)
    await writer.close()
    assert writer.health().completed == 3


def test_runtime_event_batch_preserves_order_duplicates_and_atomic_rollback(tmp_path) -> None:
    database = tmp_path / "journal.sqlite3"
    store = SQLiteStore(database)
    base = {
        "partition_key": "partition",
        "event_class": "orderUpdates",
        "exchange_ts_ms": 1,
        "receive_wall_ms": 2,
        "receive_mono_ns": 3,
        "payload": {"ok": True},
        "stream_state": "live",
        "generation": "generation",
    }
    try:
        results = store.append_runtime_events(
            events=(
                {**base, "event_key": "first"},
                {**base, "event_key": "second"},
                {**base, "event_key": "first"},
            )
        )
        assert results == [(1, True), (2, True), (1, False)]

        with pytest.raises(ValueError, match="identity fields"):
            store.append_runtime_events(
                events=(
                    {**base, "event_key": "rolled-back"},
                    {**base, "event_key": ""},
                )
            )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM runtime_events WHERE event_key='rolled-back'"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


def test_runtime_event_batch_rolls_back_fatal_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    class FatalJournalError(BaseException):
        pass

    database = tmp_path / "journal.sqlite3"
    store = SQLiteStore(database)
    original = SQLiteStore._append_runtime_event_in_transaction

    def fatal_after_write(self: SQLiteStore, **kwargs: Any) -> tuple[int, bool]:
        result = original(self, **kwargs)
        if kwargs["event_key"] == "fatal":
            raise FatalJournalError("fatal transaction interruption")
        return result

    monkeypatch.setattr(
        SQLiteStore,
        "_append_runtime_event_in_transaction",
        fatal_after_write,
    )
    base = {
        "partition_key": "partition",
        "event_class": "orderUpdates",
        "exchange_ts_ms": 1,
        "receive_wall_ms": 2,
        "receive_mono_ns": 3,
        "payload": {"ok": True},
        "stream_state": "live",
        "generation": "generation",
    }
    try:
        with pytest.raises(FatalJournalError, match="fatal transaction interruption"):
            store.append_runtime_events(
                events=(
                    {**base, "event_key": "first"},
                    {**base, "event_key": "fatal"},
                )
            )
        assert store.conn.in_transaction is False
        assert store.conn.execute("SELECT count(*) FROM runtime_events").fetchone()[0] == 0
    finally:
        store.close()


def _runtime_event(event_key: str, partition_key: str) -> dict[str, Any]:
    return {
        "event_key": event_key,
        "partition_key": partition_key,
        "event_class": "orderUpdates",
        "exchange_ts_ms": 1,
        "receive_wall_ms": 2,
        "receive_mono_ns": 3,
        "payload": {"event_key": event_key},
        "stream_state": "live",
        "generation": "generation",
    }


def _fast_reaction(
    *,
    suffix: str,
    partition_key: str,
    ingress_seq: int,
    prepared_actions: tuple[dict[str, Any], ...] = (),
    state_id: str | None = None,
) -> dict[str, Any]:
    return {
        "partition_key": partition_key,
        "through_ingress_seq": ingress_seq,
        "source_revision": {
            "revision_id": f"source-revision-{suffix}",
            "source_wallet": f"source-{suffix}",
            "revision": ingress_seq,
            "catalog_revision": "catalog",
            "checkpoint": ingress_seq,
            "observed_wall_ms": 2,
            "observed_mono_ns": 3,
            "provenance": "test",
        },
        "desired_state": {
            "state_id": state_id or f"desired-state-{suffix}",
            "source_event_key": f"event-{suffix}",
            "mode": "mainnet",
        },
        "result_id": f"result-{suffix}",
        "disposition": "test",
        "disposition_payload": {"suffix": suffix},
        "prepared_actions": prepared_actions,
        "action_rate_limit": 100,
    }


def _prepared_action(
    suffix: str,
    *,
    market: str,
    reduce_only: bool = False,
) -> dict[str, Any]:
    return {
        "intent_id": f"intent-{suffix}",
        "cloid": "0x" + suffix.zfill(32)[-32:],
        "generation": "generation",
        "follower_account": "0x" + "1" * 40,
        "canonical_market": market,
        "action_shard": 0,
        "signer_epoch": 1,
        "kind": "order",
        "action": {
            "type": "order",
            "orders": [{"coin": market, "b": True, "s": "1", "r": reduce_only}],
        },
        "expected_size": "1",
        "risk_increasing": not reduce_only,
        "reduce_only": reduce_only,
        "revisions": {
            "source_revision": 1,
            "desired_revision": 1,
            "follower_revision": 1,
            "catalog_revision": "catalog",
            "book_revision": 1,
        },
        "nonce": None,
        "request_id": None,
        "created_ms": 2,
        "deadline_wall_ms": time_ns() // 1_000_000 + 60_000,
    }


def _follower_revision_record(
    *,
    follower: str,
    revision: int,
    inflight_by_cloid: dict[str, Any],
    durable_checkpoint: int = 7,
) -> dict[str, Any]:
    inflight_entry = next(iter(inflight_by_cloid.values()), None)
    cause = "action_inflight_reserved" if revision > 1 else "full_audit"
    detail = (
        {
            "intent_id": inflight_entry["intent_id"],
            "cloid": inflight_entry["cloid"],
        }
        if isinstance(inflight_entry, dict)
        else {}
    )
    return {
        "kind": "follower",
        "revision_id": f"follower:{follower}:{revision}",
        "owner": follower,
        "revision": revision,
        "catalog_revision": "catalog",
        "observed_wall_ms": 2 + revision,
        "observed_mono_ns": 3 + revision,
        "provenance": "committed_action_response" if revision > 1 else "full_audit",
        "payload": {
            "follower": follower,
            "revision": revision,
            "cause": cause,
            "durable_checkpoint": durable_checkpoint,
            "inflight_by_cloid": inflight_by_cloid,
            "detail": detail,
        },
    }


def _inflight_entry(
    action: Mapping[str, Any],
    *,
    observed_wall_ms: int = 4,
    durable_checkpoint: int = 7,
) -> dict[str, Any]:
    signed_qty = "1" if action["action"]["orders"][0]["b"] else "-1"
    return {
        "cloid": str(action["cloid"]).lower(),
        "intent_id": action["intent_id"],
        "market": action["canonical_market"],
        "original_signed_qty": signed_qty,
        "cumulative_filled_qty": "0",
        "remaining_signed_qty": signed_qty,
        "signed_qty": signed_qty,
        "state": "committed_to_journal",
        "action_kind": "order",
        "target_leverage": None,
        "is_cross": None,
        "reduce_only": action["reduce_only"],
        "planned_follower_revision": 1,
        "truth_checkpoint": durable_checkpoint,
        "updated_ms": observed_wall_ms,
    }


def test_fast_reaction_batch_sequences_partition_and_rolls_back_item_n(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    try:
        first_seq, second_seq = (
            row[0]
            for row in store.append_runtime_events(
                events=(
                    _runtime_event("event-one", "partition-success"),
                    _runtime_event("event-two", "partition-success"),
                )
            )
        )
        store.commit_fast_reactions(
            reactions=(
                _fast_reaction(
                    suffix="one",
                    partition_key="partition-success",
                    ingress_seq=first_seq,
                ),
                _fast_reaction(
                    suffix="two",
                    partition_key="partition-success",
                    ingress_seq=second_seq,
                ),
            )
        )
        assert (
            store.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key='partition-success'"
            ).fetchone()[0]
            == second_seq
        )

        rollback_first, rollback_second = (
            row[0]
            for row in store.append_runtime_events(
                events=(
                    _runtime_event("event-rollback-one", "partition-rollback"),
                    _runtime_event("event-rollback-two", "partition-rollback"),
                )
            )
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.commit_fast_reactions(
                reactions=(
                    _fast_reaction(
                        suffix="rollback-one",
                        partition_key="partition-rollback",
                        ingress_seq=rollback_first,
                        state_id="duplicate-state",
                    ),
                    _fast_reaction(
                        suffix="rollback-two",
                        partition_key="partition-rollback",
                        ingress_seq=rollback_second,
                        state_id="duplicate-state",
                    ),
                )
            )
        assert (
            store.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key='partition-rollback'"
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM source_state_revisions WHERE revision_id LIKE 'source-revision-rollback-%'"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


def test_fast_reaction_head_commits_only_head_through_sent(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    first = _prepared_action("401", market="BTC")
    second = _prepared_action("402", market="ETH")
    first_entry = _inflight_entry(first)
    head_wall_ms = time_ns() // 1_000_000
    signed_payload = {
        "action": first["action"],
        "nonce": 1_000,
        "signature": {"r": "synthetic", "s": "synthetic", "v": 27},
        "expiresAfter": head_wall_ms + 9_000,
    }
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        store.initialize_signer_epoch(
            follower_account=follower,
            generation="generation",
            signer_epoch=1,
            transport_epoch=1,
            rest_epoch=1,
            next_nonce=1_000,
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event("event-head", "partition-head")
        )
        store.commit_fast_reaction_head(
            **_fast_reaction(
                suffix="head",
                partition_key="partition-head",
                ingress_seq=ingress_seq,
                prepared_actions=(first, second),
            ),
            follower_revision=_follower_revision_record(
                follower=follower,
                revision=2,
                inflight_by_cloid={first["cloid"].lower(): first_entry},
            ),
            head_intent_id=first["intent_id"],
            head_wall_ms=head_wall_ms,
            head_mono_ns=6,
            dispatch={
                "nonce": 1_000,
                "request_id": 77,
                "signed_payload": signed_payload,
                "action": {**first, "nonce": 1_000, "signed_payload": signed_payload},
                "minimum_remaining_ms": 250,
                "cause": "test-atomic-send",
                "payload": {"request_id": 77},
            },
        )
        states = {
            row["intent_id"]: (row["state"], row["request_id"])
            for row in store.conn.execute(
                "SELECT intent_id,state,request_id FROM action_states ORDER BY intent_id"
            )
        }
        assert states == {
            first["intent_id"]: ("sent", 77),
            second["intent_id"]: ("prepared", None),
        }
        assert [
            row["to_state"]
            for row in store.conn.execute(
                "SELECT to_state FROM action_state_transitions WHERE intent_id=? ORDER BY seq",
                (first["intent_id"],),
            )
        ] == ["prepared", "committed_to_journal", "signed", "sent"]
        assert (
            store.conn.execute(
                "SELECT count(*) FROM follower_state_revisions WHERE revision=2"
            ).fetchone()[0]
            == 1
        )
        assert tuple(
            store.conn.execute(
                "SELECT next_nonce,signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()
        ) == (1_001, 0)
        store.transition_action_state(
            action=second,
            transition_id="reject-second-after-head",
            from_state="prepared",
            to_state="rejected",
            cause="head_unresolved",
            wall_ms=7,
            mono_ns=8,
            action_rate_limit=100,
        )
        assert (
            store.conn.execute(
                "SELECT state FROM action_states WHERE intent_id=?",
                (second["intent_id"],),
            ).fetchone()[0]
            == "rejected"
        )
    finally:
        store.close()


def test_fast_reaction_head_commits_last_mile_rejection_atomically(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    action = _prepared_action("407", market="BTC")
    entry = _inflight_entry(action)
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event("event-rejected-head", "partition-rejected-head")
        )
        store.commit_fast_reaction_head(
            **_fast_reaction(
                suffix="rejected-head",
                partition_key="partition-rejected-head",
                ingress_seq=ingress_seq,
                prepared_actions=(action,),
            ),
            follower_revision=_follower_revision_record(
                follower=follower,
                revision=2,
                inflight_by_cloid={action["cloid"].lower(): entry},
            ),
            head_intent_id=action["intent_id"],
            head_wall_ms=5,
            head_mono_ns=6,
            dispatch=None,
            terminal_rejection={
                "cause": "last_mile_blocked",
                "payload": {
                    "blockers": ["kill_not_clear"],
                    "gate": {"kill_clear": False},
                },
            },
        )
        assert (
            store.conn.execute(
                "SELECT state FROM action_states WHERE intent_id=?",
                (action["intent_id"],),
            ).fetchone()[0]
            == "rejected"
        )
        transitions = list(
            store.conn.execute(
                "SELECT from_state,to_state,cause,wall_ms,mono_ns "
                "FROM action_state_transitions WHERE intent_id=? ORDER BY seq",
                (action["intent_id"],),
            )
        )
        assert [(row["from_state"], row["to_state"]) for row in transitions] == [
            ("", "prepared"),
            ("prepared", "committed_to_journal"),
            ("committed_to_journal", "rejected"),
        ]
        assert transitions[-1]["cause"] == "last_mile_blocked"
        assert (transitions[-2]["wall_ms"], transitions[-2]["mono_ns"]) == (5, 6)
        assert (transitions[-1]["wall_ms"], transitions[-1]["mono_ns"]) == (5, 6)
    finally:
        store.close()


def test_fast_reaction_head_rejects_signature_without_transaction_time_margin(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    action = _prepared_action("408", market="BTC")
    entry = _inflight_entry(action)
    head_wall_ms = time_ns() // 1_000_000
    signed_payload = {
        "action": action["action"],
        "nonce": 3_000,
        "signature": {"r": "synthetic", "s": "synthetic", "v": 27},
        "expiresAfter": head_wall_ms + 100,
    }
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event("event-expiring-head", "partition-expiring-head")
        )
        with pytest.raises(Exception, match="insufficient expiry margin"):
            store.commit_fast_reaction_head(
                **_fast_reaction(
                    suffix="expiring-head",
                    partition_key="partition-expiring-head",
                    ingress_seq=ingress_seq,
                    prepared_actions=(action,),
                ),
                follower_revision=_follower_revision_record(
                    follower=follower,
                    revision=2,
                    inflight_by_cloid={action["cloid"].lower(): entry},
                ),
                head_intent_id=action["intent_id"],
                head_wall_ms=head_wall_ms,
                head_mono_ns=6,
                dispatch={
                    "nonce": 3_000,
                    "request_id": 99,
                    "signed_payload": signed_payload,
                    "action": {
                        **action,
                        "nonce": 3_000,
                        "signed_payload": signed_payload,
                    },
                    "minimum_remaining_ms": 250,
                    "payload": {"request_id": 99},
                },
            )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_states WHERE intent_id=?",
                (action["intent_id"],),
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key=?",
                ("partition-expiring-head",),
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


def test_fast_reaction_head_late_identity_failure_rolls_back_every_half(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    action = _prepared_action("403", market="BTC")
    entry = _inflight_entry(action)
    head_wall_ms = time_ns() // 1_000_000
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        store.initialize_signer_epoch(
            follower_account=follower,
            generation="generation",
            signer_epoch=1,
            transport_epoch=1,
            rest_epoch=1,
            next_nonce=2_000,
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event("event-rollback-head", "partition-rollback-head")
        )
        with pytest.raises(Exception, match="transport payload request identity"):
            store.commit_fast_reaction_head(
                **_fast_reaction(
                    suffix="rollback-head",
                    partition_key="partition-rollback-head",
                    ingress_seq=ingress_seq,
                    prepared_actions=(action,),
                ),
                follower_revision=_follower_revision_record(
                    follower=follower,
                    revision=2,
                    inflight_by_cloid={action["cloid"].lower(): entry},
                ),
                head_intent_id=action["intent_id"],
                head_wall_ms=head_wall_ms,
                head_mono_ns=6,
                dispatch={
                    "nonce": 2_000,
                    "request_id": 88,
                    "signed_payload": {
                        "action": action["action"],
                        "nonce": 2_000,
                        "signature": {"r": "synthetic", "s": "synthetic", "v": 27},
                        "expiresAfter": head_wall_ms + 9_000,
                    },
                    "action": {
                        **action,
                        "nonce": 2_000,
                        "signed_payload": {
                            "action": action["action"],
                            "nonce": 2_000,
                            "signature": {
                                "r": "synthetic",
                                "s": "synthetic",
                                "v": 27,
                            },
                            "expiresAfter": head_wall_ms + 9_000,
                        },
                    },
                    "minimum_remaining_ms": 250,
                    "payload": {"request_id": 89},
                },
            )
        assert (
            store.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key=?",
                ("partition-rollback-head",),
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_states WHERE intent_id=?",
                (action["intent_id"],),
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM source_state_revisions WHERE revision_id=?",
                ("source-revision-rollback-head",),
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM follower_state_revisions WHERE revision=2"
            ).fetchone()[0]
            == 0
        )
        assert tuple(
            store.conn.execute(
                "SELECT next_nonce,signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()
        ) == (2_000, 0)
        assert store.conn.in_transaction is False
    finally:
        store.close()


@pytest.mark.parametrize(
    ("field", "mutated"),
    (
        ("original_signed_qty", "-1"),
        ("remaining_signed_qty", "0"),
        ("signed_qty", "2"),
        ("cumulative_filled_qty", "0.1"),
        ("action_kind", "updateLeverage"),
        ("target_leverage", 5),
        ("is_cross", False),
        ("reduce_only", True),
        ("updated_ms", 999),
    ),
)
def test_fast_reaction_head_rejects_mutated_inflight_risk_identity(
    tmp_path,
    field: str,
    mutated: Any,
) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    action = _prepared_action("404", market="BTC")
    entry = _inflight_entry(action)
    entry[field] = mutated
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event(f"event-risk-{field}", f"partition-risk-{field}")
        )
        with pytest.raises(Exception, match="in-flight identity changed"):
            store.commit_fast_reaction_head(
                **_fast_reaction(
                    suffix=f"risk-{field}",
                    partition_key=f"partition-risk-{field}",
                    ingress_seq=ingress_seq,
                    prepared_actions=(action,),
                ),
                follower_revision=_follower_revision_record(
                    follower=follower,
                    revision=2,
                    inflight_by_cloid={action["cloid"].lower(): entry},
                ),
                head_intent_id=action["intent_id"],
                head_wall_ms=5,
                head_mono_ns=6,
                dispatch=None,
            )
        assert (
            store.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key=?",
                (f"partition-risk-{field}",),
            ).fetchone()[0]
            == 0
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_states WHERE intent_id=?",
                (action["intent_id"],),
            ).fetchone()[0]
            == 0
        )
        assert store.conn.in_transaction is False
    finally:
        store.close()


def test_fast_reaction_head_rejects_noncanonical_priority_order(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40
    increase = _prepared_action("405", market="BTC")
    reduction = _prepared_action("406", market="ETH", reduce_only=True)
    entry = _inflight_entry(increase)
    try:
        store.append_state_revision(
            **_follower_revision_record(
                follower=follower,
                revision=1,
                inflight_by_cloid={},
            )
        )
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event("event-priority-order", "partition-priority-order")
        )
        with pytest.raises(Exception, match="canonical priority order"):
            store.commit_fast_reaction_head(
                **_fast_reaction(
                    suffix="priority-order",
                    partition_key="partition-priority-order",
                    ingress_seq=ingress_seq,
                    prepared_actions=(increase, reduction),
                ),
                follower_revision=_follower_revision_record(
                    follower=follower,
                    revision=2,
                    inflight_by_cloid={increase["cloid"].lower(): entry},
                ),
                head_intent_id=increase["intent_id"],
                head_wall_ms=5,
                head_mono_ns=6,
                dispatch=None,
            )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_states WHERE intent_id IN (?,?)",
                (increase["intent_id"], reduction["intent_id"]),
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


def test_signed_action_batch_accumulates_nonce_state_and_rolls_back_item_n(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40

    def prepare(suffix: str, market: str) -> dict[str, Any]:
        action = _prepared_action(suffix, market=market)
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event(f"event-{suffix}", f"partition-{suffix}")
        )
        store.commit_fast_reaction(
            **_fast_reaction(
                suffix=suffix,
                partition_key=f"partition-{suffix}",
                ingress_seq=ingress_seq,
                prepared_actions=(action,),
            )
        )
        store.transition_action_state(
            action=action,
            transition_id=f"commit-{suffix}",
            from_state="prepared",
            to_state="committed_to_journal",
            cause="test",
            wall_ms=3,
            mono_ns=4,
            action_rate_limit=100,
        )
        return action

    try:
        store.initialize_signer_epoch(
            follower_account=follower,
            generation="generation",
            signer_epoch=1,
            transport_epoch=1,
            rest_epoch=1,
            next_nonce=1_000,
        )
        first = prepare("1", "BTC")
        second = prepare("2", "ETH")
        store.commit_signed_actions(
            actions=(
                {
                    "action": first,
                    "transition_id": "signed-1",
                    "nonce": 1_000,
                    "signed_payload": {"signature": "one"},
                    "wall_ms": 5,
                    "mono_ns": 6,
                },
                {
                    "action": second,
                    "transition_id": "signed-2",
                    "nonce": 1_001,
                    "signed_payload": {"signature": "two"},
                    "wall_ms": 5,
                    "mono_ns": 6,
                },
            )
        )
        signer = store.conn.execute(
            "SELECT next_nonce, signed_unsent_count FROM signer_epochs WHERE follower_account=?",
            (follower,),
        ).fetchone()
        assert tuple(signer) == (1_002, 2)

        third = prepare("3", "SOL")
        fourth = prepare("4", "DOGE")
        with pytest.raises(Exception, match="nonce fence"):
            store.commit_signed_actions(
                actions=(
                    {
                        "action": third,
                        "transition_id": "signed-3",
                        "nonce": 1_002,
                        "signed_payload": {"signature": "three"},
                        "wall_ms": 7,
                        "mono_ns": 8,
                    },
                    {
                        "action": fourth,
                        "transition_id": "signed-4",
                        "nonce": 1_001,
                        "signed_payload": {"signature": "four"},
                        "wall_ms": 7,
                        "mono_ns": 8,
                    },
                )
            )
        signer = store.conn.execute(
            "SELECT next_nonce, signed_unsent_count FROM signer_epochs WHERE follower_account=?",
            (follower,),
        ).fetchone()
        assert tuple(signer) == (1_002, 2)
        states = dict(
            store.conn.execute(
                "SELECT intent_id, state FROM action_states WHERE intent_id IN (?, ?)",
                (third["intent_id"], fourth["intent_id"]),
            ).fetchall()
        )
        assert states == {
            third["intent_id"]: "committed_to_journal",
            fourth["intent_id"]: "committed_to_journal",
        }
    finally:
        store.close()


def test_action_rate_index_migrates_v10_store_and_forces_full_durability(tmp_path) -> None:
    database = tmp_path / "journal.sqlite3"
    store = SQLiteStore(database)
    try:
        store.conn.execute("DROP INDEX idx_action_states_generation_follower_created")
        store.conn.execute(
            """
            INSERT INTO action_states(
              intent_id,cloid,generation,follower_account,canonical_market,
              action_shard,signer_epoch,nonce,request_id,state,payload_json,
              created_ms,updated_ms
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "mixed-case-intent",
                "0x" + "a" * 32,
                "generation",
                "0xAa" + "1" * 38,
                "BTC",
                0,
                1,
                None,
                None,
                "prepared",
                "{}",
                1_000,
                1_000,
            ),
        )
        store.conn.commit()
    finally:
        store.close()

    migrated = SQLiteStore(database)
    try:
        assert migrated.schema_version() == 10
        assert migrated.conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert (
            migrated.conn.execute(
                """
            SELECT count(*) FROM action_states
            WHERE generation=? AND lower(follower_account)=? AND created_ms>?
            """,
                ("generation", ("0xAa" + "1" * 38).lower(), 0),
            ).fetchone()[0]
            == 1
        )
        plan = " ".join(
            str(row[3])
            for row in migrated.conn.execute(
                """
                EXPLAIN QUERY PLAN SELECT count(*) FROM action_states
                WHERE generation=? AND lower(follower_account)=? AND created_ms>?
                """,
                ("generation", ("0xAa" + "1" * 38).lower(), 0),
            )
        )
        assert "USING COVERING INDEX idx_action_states_generation_follower_created" in plan
        assert "SCAN action_states" not in plan
    finally:
        migrated.close()


def test_follower_revision_lookup_index_migrates_v10_store(tmp_path) -> None:
    database = tmp_path / "journal.sqlite3"
    follower = "0xAa" + "1" * 38
    store = SQLiteStore(database)
    try:
        store.conn.execute("DROP INDEX idx_follower_state_revisions_account_revision")
        store.conn.executemany(
            """
            INSERT INTO follower_state_revisions(
              revision_id, follower_account, revision, catalog_revision,
              observed_wall_ms, observed_mono_ns, provenance, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ("follower-revision-1", follower, 1, "catalog", 1, 1, "test", "{}", 1),
                ("follower-revision-2", follower, 2, "catalog", 2, 2, "test", "{}", 2),
                (
                    "legacy-case-duplicate-revision-2",
                    follower.lower(),
                    2,
                    "catalog",
                    2,
                    2,
                    "test",
                    "{}",
                    2,
                ),
                (
                    "legacy-latest-revision-3",
                    follower.lower(),
                    3,
                    "catalog",
                    3,
                    3,
                    "test",
                    "{}",
                    3,
                ),
            ),
        )
        store.conn.commit()
    finally:
        store.close()

    migrated = SQLiteStore(database)
    try:
        assert migrated.schema_version() == 10
        row = migrated.conn.execute(
            """
            SELECT revision, catalog_revision, payload_json
            FROM follower_state_revisions
            WHERE lower(follower_account)=lower(?)
            ORDER BY revision DESC LIMIT 1
            """,
            (follower.lower(),),
        ).fetchone()
        assert row is not None
        assert int(row["revision"]) == 3
        plan = " ".join(
            str(item[3])
            for item in migrated.conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT revision, catalog_revision, payload_json
                FROM follower_state_revisions
                WHERE lower(follower_account)=lower(?)
                ORDER BY revision DESC LIMIT 1
                """,
                (follower.lower(),),
            )
        )
        assert "USING INDEX idx_follower_state_revisions_account_revision" in plan
        assert "SCAN follower_state_revisions" not in plan
        assert "USE TEMP B-TREE FOR ORDER BY" not in plan
    finally:
        migrated.close()


def test_state_revision_and_action_transition_batches_preserve_order_and_rollback(
    tmp_path,
) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "2" * 40

    def revision(revision_id: str, value: int) -> dict[str, Any]:
        return {
            "kind": "follower",
            "revision_id": revision_id,
            "owner": follower,
            "revision": value,
            "catalog_revision": "catalog",
            "observed_wall_ms": value,
            "observed_mono_ns": value,
            "provenance": "test",
            "payload": {"revision": value},
        }

    try:
        store.append_state_revisions(
            revisions=(revision("revision-one", 1), revision("revision-two", 2))
        )
        assert [
            row[0]
            for row in store.conn.execute(
                "SELECT revision_id FROM follower_state_revisions ORDER BY rowid"
            )
        ] == ["revision-one", "revision-two"]
        with pytest.raises(sqlite3.IntegrityError):
            store.append_state_revisions(
                revisions=(
                    revision("rollback-revision", 3),
                    revision("rollback-revision", 4),
                )
            )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM follower_state_revisions WHERE revision_id='rollback-revision'"
            ).fetchone()[0]
            == 0
        )

        actions = (
            _prepared_action("101", market="BTC"),
            _prepared_action("102", market="ETH"),
        )
        store.transition_action_states(
            transitions=tuple(
                {
                    "action": action,
                    "transition_id": f"prepare-{index}",
                    "from_state": "",
                    "to_state": "prepared",
                    "cause": "test",
                    "wall_ms": 10 + index,
                    "mono_ns": 20 + index,
                    "action_rate_limit": 100,
                }
                for index, action in enumerate(actions, start=1)
            )
        )
        assert [
            row[0]
            for row in store.conn.execute(
                "SELECT transition_id FROM action_state_transitions ORDER BY seq"
            )
        ] == ["prepare-1", "prepare-2"]

        rate_follower = "0x" + "3" * 40
        rate_actions = tuple(
            {
                **_prepared_action(str(200 + index), market=market),
                "generation": "rate-generation",
                "follower_account": rate_follower,
            }
            for index, market in enumerate(("SOL", "DOGE"), start=1)
        )
        with pytest.raises(Exception, match="action-rate ceiling"):
            store.transition_action_states(
                transitions=tuple(
                    {
                        "action": action,
                        "transition_id": f"rate-{index}",
                        "from_state": "",
                        "to_state": "prepared",
                        "cause": "test",
                        "wall_ms": 30,
                        "mono_ns": 40,
                        "action_rate_limit": 1,
                    }
                    for index, action in enumerate(rate_actions, start=1)
                )
            )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_states WHERE generation='rate-generation'"
            ).fetchone()[0]
            == 0
        )
    finally:
        store.close()


def test_signer_nonce_and_signed_unsent_batches_sequence_and_rollback(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "4" * 40
    try:
        store.initialize_signer_epoch(
            follower_account=follower,
            generation="generation",
            signer_epoch=1,
            transport_epoch=1,
            rest_epoch=1,
            next_nonce=1_000,
        )
        assert store.peek_signer_nonces(
            requests=(
                {
                    "follower_account": follower,
                    "generation": "generation",
                    "signer_epoch": 1,
                    "wall_ms": 900,
                },
                {
                    "follower_account": follower.upper(),
                    "generation": "generation",
                    "signer_epoch": 1,
                    "wall_ms": 2_000,
                },
            )
        ) == [1_000, 2_000]
        assert store.adjust_signed_unsents(
            adjustments=(
                {
                    "follower_account": follower,
                    "generation": "generation",
                    "signer_epoch": 1,
                    "delta": 1,
                },
                {
                    "follower_account": follower,
                    "generation": "generation",
                    "signer_epoch": 1,
                    "delta": 1,
                },
                {
                    "follower_account": follower,
                    "generation": "generation",
                    "signer_epoch": 1,
                    "delta": -1,
                },
            )
        ) == [1, 2, 1]
        with pytest.raises(Exception, match="cannot be negative"):
            store.adjust_signed_unsents(
                adjustments=(
                    {
                        "follower_account": follower,
                        "generation": "generation",
                        "signer_epoch": 1,
                        "delta": -1,
                    },
                    {
                        "follower_account": follower,
                        "generation": "generation",
                        "signer_epoch": 1,
                        "delta": -1,
                    },
                )
            )
        assert (
            store.conn.execute(
                "SELECT signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


def test_transport_attempt_and_signed_expiry_are_atomic_and_identity_fenced(tmp_path) -> None:
    store = SQLiteStore(tmp_path / "journal.sqlite3")
    follower = "0x" + "1" * 40

    def prepare(suffix: str, market: str) -> dict[str, Any]:
        action = _prepared_action(suffix, market=market)
        ingress_seq, _inserted = store.append_runtime_event(
            **_runtime_event(f"event-{suffix}", f"partition-{suffix}")
        )
        store.commit_fast_reaction(
            **_fast_reaction(
                suffix=suffix,
                partition_key=f"partition-{suffix}",
                ingress_seq=ingress_seq,
                prepared_actions=(action,),
            )
        )
        store.transition_action_state(
            action=action,
            transition_id=f"commit-{suffix}",
            from_state="prepared",
            to_state="committed_to_journal",
            cause="test",
            wall_ms=3,
            mono_ns=4,
            action_rate_limit=100,
        )
        return action

    try:
        store.initialize_signer_epoch(
            follower_account=follower,
            generation="generation",
            signer_epoch=1,
            transport_epoch=1,
            rest_epoch=1,
            next_nonce=1_000,
        )
        first = prepare("301", "BTC")
        second = prepare("302", "ETH")
        first_expiry = time_ns() // 1_000_000 + 30_000
        payloads = (
            {"nonce": 1_000, "signature": {"r": "one"}, "expiresAfter": first_expiry},
            {"nonce": 1_001, "signature": {"r": "two"}, "expiresAfter": first_expiry},
        )
        store.commit_signed_actions(
            actions=tuple(
                {
                    "action": action,
                    "transition_id": f"signed-{index}",
                    "nonce": 999 + index,
                    "signed_payload": payload,
                    "wall_ms": 5,
                    "mono_ns": 6,
                }
                for index, (action, payload) in enumerate(
                    zip((first, second), payloads, strict=True), start=1
                )
            )
        )
        store.commit_transport_attempts(
            attempts=tuple(
                {
                    "action": {
                        **action,
                        "nonce": 999 + index,
                        "request_id": 10 + index,
                        "signed_payload": payload,
                    },
                    "transition_id": f"sent-{index}",
                    "cause": "test-send",
                    "wall_ms": 7,
                    "mono_ns": 8,
                    "minimum_remaining_ms": 250,
                }
                for index, (action, payload) in enumerate(
                    zip((first, second), payloads, strict=True), start=1
                )
            )
        )
        rows = store.conn.execute(
            "SELECT state,request_id,payload_json FROM action_states "
            "WHERE intent_id IN (?,?) ORDER BY intent_id",
            (first["intent_id"], second["intent_id"]),
        ).fetchall()
        assert [(row["state"], row["request_id"]) for row in rows] == [
            ("sent", 11),
            ("sent", 12),
        ]
        assert all('"signed_payload"' in str(row["payload_json"]) for row in rows)
        assert (
            store.conn.execute(
                "SELECT signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()[0]
            == 0
        )

        third = prepare("303", "SOL")
        fourth = prepare("304", "DOGE")
        later_expiry = time_ns() // 1_000_000 + 30_000
        later_payloads = (
            {"nonce": 1_002, "signature": {"r": "three"}, "expiresAfter": later_expiry},
            {"nonce": 1_003, "signature": {"r": "four"}, "expiresAfter": later_expiry},
        )
        store.commit_signed_actions(
            actions=tuple(
                {
                    "action": action,
                    "transition_id": f"signed-later-{index}",
                    "nonce": 1_001 + index,
                    "signed_payload": payload,
                    "wall_ms": 9,
                    "mono_ns": 10,
                }
                for index, (action, payload) in enumerate(
                    zip((third, fourth), later_payloads, strict=True), start=1
                )
            )
        )
        with pytest.raises(Exception, match="insufficient expiry margin"):
            store.commit_transport_attempt(
                action={
                    **third,
                    "nonce": 1_002,
                    "request_id": 13,
                    "signed_payload": later_payloads[0],
                },
                transition_id="sent-expiry-fenced",
                cause="test-send",
                wall_ms=11,
                mono_ns=12,
                minimum_remaining_ms=100_000,
            )
        assert (
            store.conn.execute(
                "SELECT state FROM action_states WHERE intent_id=?", (third["intent_id"],)
            ).fetchone()[0]
            == "signed"
        )
        with pytest.raises(Exception, match="payload identity changed"):
            store.commit_transport_attempts(
                attempts=(
                    {
                        "action": {
                            **third,
                            "nonce": 1_002,
                            "request_id": 13,
                            "signed_payload": later_payloads[0],
                        },
                        "transition_id": "sent-later-1",
                        "cause": "test-send",
                        "wall_ms": 11,
                        "mono_ns": 12,
                        "minimum_remaining_ms": 250,
                    },
                    {
                        "action": {
                            **fourth,
                            "nonce": 1_003,
                            "request_id": 14,
                            "signed_payload": {
                                **later_payloads[1],
                                "signature": {"r": "mutated"},
                            },
                        },
                        "transition_id": "sent-later-2",
                        "cause": "test-send",
                        "wall_ms": 11,
                        "mono_ns": 12,
                        "minimum_remaining_ms": 250,
                    },
                )
            )
        assert dict(
            store.conn.execute(
                "SELECT intent_id,state FROM action_states WHERE intent_id IN (?,?)",
                (third["intent_id"], fourth["intent_id"]),
            ).fetchall()
        ) == {third["intent_id"]: "signed", fourth["intent_id"]: "signed"}
        assert (
            store.conn.execute(
                "SELECT signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()[0]
            == 2
        )
        assert (
            store.conn.execute(
                "SELECT count(*) FROM action_state_transitions "
                "WHERE transition_id IN ('sent-later-1','sent-later-2')"
            ).fetchone()[0]
            == 0
        )

        store.commit_signed_expiry(
            action={
                **third,
                "nonce": 1_002,
                "signed_payload": later_payloads[0],
            },
            transition_id="expired-later-1",
            cause="test-expiry",
            wall_ms=13,
            mono_ns=14,
        )
        assert (
            store.conn.execute(
                "SELECT state FROM action_states WHERE intent_id=?", (third["intent_id"],)
            ).fetchone()[0]
            == "expired"
        )
        assert (
            store.conn.execute(
                "SELECT signed_unsent_count FROM signer_epochs WHERE follower_account=?",
                (follower,),
            ).fetchone()[0]
            == 1
        )
    finally:
        store.close()


@pytest.mark.asyncio
async def test_journal_writer_transport_batch_futures_wait_for_shared_commit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    first_started = Event()
    release_first = Event()
    batch_started = Event()
    release_batch = Event()

    class FakeStore:
        def __init__(self, _path) -> None:
            return None

        @staticmethod
        def blocking_critical() -> None:
            first_started.set()
            if not release_first.wait(2):
                raise TimeoutError("test did not release critical write")

        @staticmethod
        def commit_transport_attempts(*, attempts) -> None:
            assert [item["transition_id"] for item in attempts] == ["one", "two"]
            batch_started.set()
            if not release_batch.wait(2):
                raise TimeoutError("test did not release transport batch")

        def close(self) -> None:
            return None

    monkeypatch.setattr(journal_writer_module, "SQLiteStore", FakeStore)
    writer = JournalWriter(tmp_path / "fake.sqlite3")
    await writer.start()
    blocking = writer.offer("blocking_critical")
    assert await asyncio.to_thread(first_started.wait, 1)
    first = writer.offer("commit_transport_attempt", transition_id="one")
    second = writer.offer("commit_transport_attempt", transition_id="two")
    release_first.set()
    assert await asyncio.to_thread(batch_started.wait, 1)
    assert not first.done()
    assert not second.done()
    release_batch.set()
    await asyncio.gather(blocking, first, second)
    await writer.close()
