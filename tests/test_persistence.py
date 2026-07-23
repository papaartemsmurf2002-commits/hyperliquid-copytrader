from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import replace
from decimal import Decimal

import pytest

from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.models import (
    DesiredState,
    ExecutionAttemptPhase,
    ExecutionReport,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    Position,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.persistence import SCHEMA_VERSION, JournalIntegrityError, SQLiteStore
from hyperliquid_copytrader.safety import SafeModeController


def test_sqlite_store_is_append_only_and_dedupes_source_events(store):
    event = SourceEvent(
        idempotency_key="event-1",
        event_type=SourceEventType.FILL,
        exchange_ts_ms=now_ms(),
        observed_ts_ms=now_ms(),
        payload={"fill": 1},
    )
    assert store.append_source_event(event) is True
    assert store.append_source_event(event) is False
    assert store.count("source_events") == 1
    state = store.rebuild_runtime_state()
    assert state["source_event_count"] == 1


def test_duplicate_source_event_can_atomically_restore_missing_reaction_obligation(store):
    event = SourceEvent(
        idempotency_key="event-reaction",
        event_type=SourceEventType.FILL,
        source_wallet="0x" + "a" * 40,
        exchange_ts_ms=100,
        observed_ts_ms=101,
        payload={"event_subtype": "fill"},
    )
    assert store.append_source_event(event) is True
    assert store.unfinished_source_reaction_count() == 0

    assert store.append_source_event(event, reaction_required=True) is False
    assert store.unfinished_source_reaction_count() == 1
    assert store.pending_source_reaction_events() == [event]
    assert store.claim_source_reactions([event.idempotency_key]) == 1
    assert store.source_reaction_status(event.idempotency_key) == "processing"
    assert (
        store.finish_source_reactions(
            [event.idempotency_key],
            status="completed",
            outcome={"validated": True},
        )
        == 1
    )
    assert store.unfinished_source_reaction_count() == 0
    assert (
        store.finish_source_reactions(
            [event.idempotency_key],
            status="failed",
            outcome={"stale_worker": True},
        )
        == 0
    )
    assert store.source_reaction_status(event.idempotency_key) == "completed"


def test_source_reaction_retry_due_filter_preserves_untyped_blocking_work(store):
    wallet = "0x" + "a" * 40
    other_wallet = "0x" + "b" * 40

    def append_reaction(key: str, *, source_wallet: str = wallet) -> SourceEvent:
        event = SourceEvent(
            idempotency_key=key,
            event_type=SourceEventType.FILL,
            source_wallet=source_wallet,
            exchange_ts_ms=100,
            observed_ts_ms=101,
            payload={"event_subtype": "fill"},
        )
        assert store.append_source_event(event, reaction_required=True)
        return event

    waiting = append_reaction("hip3-waiting")
    due = append_reaction("hip3-due")
    generic_blocked = append_reaction("safe-mode-blocked")
    failed = append_reaction("failed-reaction")
    malformed_typed = append_reaction("malformed-typed-retry")
    other_waiting = append_reaction("other-wallet-waiting", source_wallet=other_wallet)

    def block_hip3(event: SourceEvent, retry_not_before_ms):
        assert (
            store.finish_source_reactions(
                [event.idempotency_key],
                status="blocked",
                outcome={
                    "disposition": "deferred",
                    "retry": {
                        "class": "hip3_liquidity",
                        "retry_not_before_ms": retry_not_before_ms,
                    },
                },
            )
            == 1
        )

    block_hip3(waiting, 200)
    block_hip3(due, 150)
    block_hip3(other_waiting, 300)
    assert (
        store.finish_source_reactions(
            [generic_blocked.idempotency_key],
            status="blocked",
            outcome={"safe_mode": {"reason": "risk_limit"}},
        )
        == 1
    )
    assert (
        store.finish_source_reactions(
            [failed.idempotency_key],
            status="failed",
            outcome={"error": "temporary REST failure"},
        )
        == 1
    )
    block_hip3(malformed_typed, "200")

    assert store.source_reaction_retry_counts(
        source_wallet=wallet,
        retry_due_ms=150,
    ) == {
        "hip3_liquidity_waiting": 1,
        "hip3_liquidity_due": 1,
        "other_blocking_unfinished": 3,
    }
    assert [
        event.idempotency_key
        for event in store.pending_source_reaction_events(source_wallet=wallet)
    ] == [
        waiting.idempotency_key,
        due.idempotency_key,
        generic_blocked.idempotency_key,
        failed.idempotency_key,
        malformed_typed.idempotency_key,
    ]
    due_or_unpaced = store.pending_source_reaction_events(
        source_wallet=wallet,
        retry_due_ms=150,
    )
    assert [event.idempotency_key for event in due_or_unpaced] == [
        due.idempotency_key,
        generic_blocked.idempotency_key,
        failed.idempotency_key,
        malformed_typed.idempotency_key,
    ]

    claimed = store.claim_source_reaction_keys(
        [
            waiting.idempotency_key,
            due.idempotency_key,
            generic_blocked.idempotency_key,
            failed.idempotency_key,
            malformed_typed.idempotency_key,
        ],
        retry_due_ms=150,
    )
    assert claimed == (
        due.idempotency_key,
        generic_blocked.idempotency_key,
        failed.idempotency_key,
        malformed_typed.idempotency_key,
    )
    rows = {row["source_event_key"]: row for row in store.source_reaction_rows()}
    assert rows[waiting.idempotency_key]["status"] == "blocked"
    assert rows[waiting.idempotency_key]["attempt_count"] == 0
    assert rows[due.idempotency_key]["status"] == "processing"
    assert rows[due.idempotency_key]["attempt_count"] == 1

    # Omitting retry_due_ms preserves the historical immediately claimable behavior.
    assert store.claim_source_reaction_keys([other_waiting.idempotency_key]) == (
        other_waiting.idempotency_key,
    )


def test_typed_hip3_retry_wakeup_queries_exclude_generic_backlog(store):
    wallet = "0x" + "c" * 40
    other_wallet = "0x" + "d" * 40

    def append_blocked(
        key: str,
        *,
        retry_not_before_ms: int | None,
        source_wallet: str = wallet,
    ) -> SourceEvent:
        event = SourceEvent(
            idempotency_key=key,
            event_type=SourceEventType.POSITION,
            source_wallet=source_wallet,
            exchange_ts_ms=200,
            observed_ts_ms=201,
            payload={"event_subtype": "position_snapshot"},
        )
        assert store.append_source_event(event, reaction_required=True)
        outcome = (
            {
                "retry": {
                    "class": "hip3_liquidity",
                    "retry_not_before_ms": retry_not_before_ms,
                }
            }
            if retry_not_before_ms is not None
            else {"safe_mode": {"reason": "manual_intervention"}}
        )
        assert (
            store.finish_source_reactions(
                [event.idempotency_key],
                status="blocked",
                outcome=outcome,
            )
            == 1
        )
        return event

    due = append_blocked("typed-due", retry_not_before_ms=100)
    waiting = append_blocked("typed-waiting", retry_not_before_ms=200)
    append_blocked("generic-blocked", retry_not_before_ms=None)
    append_blocked(
        "other-wallet-due",
        retry_not_before_ms=50,
        source_wallet=other_wallet,
    )

    assert store.next_hip3_liquidity_retry_ms(source_wallet=wallet) == 100
    assert [
        event.idempotency_key
        for event in store.due_hip3_liquidity_reaction_events(
            source_wallet=wallet,
            retry_due_ms=150,
        )
    ] == [due.idempotency_key]
    assert [
        event.idempotency_key
        for event in store.due_hip3_liquidity_reaction_events(
            source_wallet=wallet,
            retry_due_ms=250,
        )
    ] == [due.idempotency_key, waiting.idempotency_key]

    assert store.claim_source_reaction_keys(
        [due.idempotency_key],
        retry_due_ms=150,
    ) == (due.idempotency_key,)
    assert store.next_hip3_liquidity_retry_ms(source_wallet=wallet) == 200
    assert (
        store.due_hip3_liquidity_reaction_events(
            source_wallet=wallet,
            retry_due_ms=150,
        )
        == []
    )


def test_source_event_queries_are_scoped_to_configured_wallet(store):
    wallet_a = "0x" + "a" * 40
    wallet_b = "0x" + "b" * 40
    for index, wallet in enumerate((wallet_a, wallet_b), start=1):
        store.append_source_event(
            SourceEvent(
                idempotency_key=f"event-{index}",
                event_type=SourceEventType.FILL,
                source_wallet=wallet,
                exchange_ts_ms=index * 100,
                observed_ts_ms=index * 100,
                payload={"event_subtype": "fill"},
            )
        )

    assert store.count_source_events(wallet_a) == 1
    latest = store.latest_source_event(wallet_a)
    assert latest is not None
    assert latest["idempotency_key"] == "event-1"
    recent = store.recent_source_events(source_wallet=wallet_a)
    assert recent[0]["idempotency_key"] == "event-1"
    assert store.latest_source_event_ts(source_wallet=wallet_a) == 100
    assert (
        store.latest_source_event_ts_by_subtypes(
            ("fill",),
            source_wallet=wallet_a,
        )
        == 100
    )
    scoped = store.rebuild_runtime_state(source_wallet=wallet_a)
    assert scoped["source_event_count"] == 1
    assert scoped["latest_source_events"][0]["idempotency_key"] == "event-1"


def test_runtime_lease_blocks_other_owner_until_expiry(store):
    assert store.acquire_runtime_lease(name="run", owner="a", ttl_ms=1000, observed_ms=1000)
    assert not store.acquire_runtime_lease(name="run", owner="b", ttl_ms=1000, observed_ms=1200)
    assert store.runtime_lease("run")["owner"] == "a"
    assert store.acquire_runtime_lease(name="run", owner="b", ttl_ms=1000, observed_ms=2101)
    assert store.runtime_lease("run")["owner"] == "b"
    assert not store.release_runtime_lease(name="run", owner="a")
    assert store.release_runtime_lease(name="run", owner="b")
    assert store.runtime_lease("run") is None


def test_runtime_lease_acquisition_is_atomic_across_connections(tmp_path):
    path = tmp_path / "lease.sqlite3"
    stores = [SQLiteStore(path) for _ in range(6)]
    barrier = threading.Barrier(len(stores))
    results: list[bool] = []
    results_lock = threading.Lock()

    def attempt(index: int) -> None:
        barrier.wait(timeout=5)
        acquired = stores[index].acquire_runtime_lease(
            name="run",
            owner=f"owner-{index}",
            ttl_ms=1000,
            observed_ms=1000,
        )
        with results_lock:
            results.append(acquired)

    threads = [threading.Thread(target=attempt, args=(index,)) for index in range(len(stores))]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        assert len(results) == len(stores)
        assert sum(1 for acquired in results if acquired) == 1
    finally:
        for store in stores:
            store.close()


def test_store_rejects_newer_schema_version(tmp_path):
    path = tmp_path / "future.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION + 1)),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(JournalIntegrityError, match="newer than supported"):
        SQLiteStore(path)


def test_v4_source_events_migrate_to_legacy_unverified_reactions(tmp_path):
    path = tmp_path / "v4.sqlite3"
    event = SourceEvent(
        idempotency_key="legacy-source-event",
        event_type=SourceEventType.RECONCILE,
        source_wallet="0x" + "a" * 40,
        exchange_ts_ms=100,
        observed_ts_ms=101,
        payload={"event_subtype": "reconcile"},
    )
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE source_events (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              idempotency_key TEXT NOT NULL UNIQUE,
              event_type TEXT NOT NULL,
              exchange_ts_ms INTEGER NOT NULL,
              observed_ts_ms INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );
            """
        )
        conn.execute("INSERT INTO schema_meta(key, value) VALUES ('schema_version', '4')")
        conn.execute("PRAGMA user_version = 4")
        conn.execute(
            """
            INSERT INTO source_events(
              idempotency_key, event_type, exchange_ts_ms, observed_ts_ms, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.idempotency_key,
                event.event_type.value,
                event.exchange_ts_ms,
                event.observed_ts_ms,
                json.dumps(
                    {
                        "idempotency_key": event.idempotency_key,
                        "event_type": event.event_type.value,
                        "source_wallet": event.source_wallet,
                        "exchange_ts_ms": event.exchange_ts_ms,
                        "observed_ts_ms": event.observed_ts_ms,
                        "payload": event.payload,
                    }
                ),
                101,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    migrated = SQLiteStore(path)
    try:
        assert migrated.schema_version() == SCHEMA_VERSION
        assert migrated.legacy_source_reaction_count(source_wallet=event.source_wallet) == 1
        assert migrated.pending_source_reaction_events(source_wallet=event.source_wallet) == [event]
    finally:
        migrated.close()


def test_store_exposes_checkpoint_backup_and_control_audit(store, tmp_path):
    assert store.schema_version() == SCHEMA_VERSION
    assert store.append_control_audit(
        control="pause",
        status="success",
        detail="control accepted",
        payload={"token_supplied": True, "operator_token": "secret-token"},
        created_ms=1234,
    )

    audit = store.recent("control_audit", 1)[0]
    assert audit["control"] == "pause"
    assert audit["status"] == "success"
    assert "secret-token" not in audit["payload_json"]
    assert store.checkpoint_wal("TRUNCATE").keys() == {"busy", "log", "checkpointed"}
    backup_path = store.backup_to(tmp_path / "backup.sqlite3")
    backup = SQLiteStore(backup_path)
    try:
        assert backup.schema_version() == SCHEMA_VERSION
        assert backup.recent("control_audit", 1)[0]["control"] == "pause"
    finally:
        backup.close()


def test_recent_counted_exchange_action_stats_excludes_local_blockers_and_reads(store):
    created = now_ms()
    reports = [
        ExecutionReport(
            report_id="actual-place",
            intent_id="intent-1",
            cloid="0x11111111111111111111111111111111",
            status=IntentStatus.ACKED,
            exchange_status="acked",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="blocked-local",
            intent_id="intent-2",
            cloid="0x22222222222222222222222222222222",
            status=IntentStatus.SKIPPED,
            exchange_status="blocked:rate_limit",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="settled-read",
            intent_id="intent-3",
            cloid="0x33333333333333333333333333333333",
            status=IntentStatus.FILLED,
            exchange_status="settled:filled",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="cancel-action",
            intent_id="cancel:0x44444444444444444444444444444444",
            cloid="0x44444444444444444444444444444444",
            status=IntentStatus.CANCELED,
            exchange_status="canceled",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="auth-probe-action",
            intent_id="auth-probe:testnet:0xf000000000000000000000000000000000000000",
            cloid="0x55555555555555555555555555555555",
            status=IntentStatus.ACKED,
            exchange_status="auth_probe_ok",
            exchange_ts_ms=created,
        ),
    ]
    for report in reports:
        store.append_execution_report(report)
    stats = store.recent_counted_exchange_action_stats(created - 1)
    assert stats["count"] == 1
    assert stats["oldest_ms"] >= created


@pytest.mark.parametrize(
    "include_foreground_report",
    (False, True),
    ids=("watchdog-only", "foreground-with-watchdog-corroboration"),
)
def test_watchdog_settlement_never_adds_a_recent_exchange_action(
    store,
    include_foreground_report,
):
    created = now_ms()
    cloid = "0x77777777777777777777777777777777"
    if include_foreground_report:
        store.append_execution_report(
            ExecutionReport(
                report_id="foreground-terminal",
                intent_id="intent-with-watchdog-corroboration",
                cloid=cloid,
                status=IntentStatus.FILLED,
                exchange_status="filled",
                exchange_ts_ms=created,
            )
        )
    store.append_execution_report(
        ExecutionReport(
            report_id="watchdog-terminal-corroboration",
            intent_id="intent-with-watchdog-corroboration",
            cloid=cloid,
            status=IntentStatus.FILLED,
            exchange_status="watchdog_settled:filled",
            exchange_ts_ms=created,
        )
    )

    stats = store.recent_counted_exchange_action_stats(created - 1)

    assert stats["count"] == int(include_foreground_report)
    if include_foreground_report:
        assert stats["oldest_ms"] >= created
    else:
        assert stats["oldest_ms"] == 0


def test_latest_successful_auth_probe_is_scoped_by_mode_account_and_time(store):
    created = now_ms()
    matching = ExecutionReport(
        report_id="auth-probe-match",
        intent_id="auth-probe:testnet:0xf000000000000000000000000000000000000000",
        cloid="0x11111111111111111111111111111111",
        status=IntentStatus.ACKED,
        exchange_status="auth_probe_ok",
        exchange_ts_ms=created,
    )
    wrong_status = ExecutionReport(
        report_id="auth-probe-rejected",
        intent_id="auth-probe:testnet:0xf000000000000000000000000000000000000000",
        cloid="0x22222222222222222222222222222222",
        status=IntentStatus.REJECTED,
        exchange_status="rejected",
        exchange_ts_ms=created,
    )
    wrong_account = ExecutionReport(
        report_id="auth-probe-other-account",
        intent_id="auth-probe:testnet:0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        cloid="0x33333333333333333333333333333333",
        status=IntentStatus.ACKED,
        exchange_status="auth_probe_ok",
        exchange_ts_ms=created,
    )
    for report in (wrong_status, wrong_account, matching):
        store.append_execution_report(report)

    row = store.latest_successful_auth_probe(
        mode=Mode.TESTNET,
        account="0xF000000000000000000000000000000000000000",
        since_ms=created - 1,
    )
    assert row is not None
    assert row["report_id"] == "auth-probe-match"
    assert (
        store.latest_successful_auth_probe(
            mode=Mode.LIVE,
            account="0xf000000000000000000000000000000000000000",
            since_ms=created - 1,
        )
        is None
    )
    assert (
        store.latest_successful_auth_probe(
            mode=Mode.TESTNET,
            account="0xf000000000000000000000000000000000000000",
            since_ms=now_ms() + 1,
        )
        is None
    )


def test_consecutive_exchange_failure_stats_resets_on_success_and_excludes_local_rows(store):
    created = now_ms()
    rows = [
        ExecutionReport(
            report_id="old-failure",
            intent_id="intent-old",
            cloid="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="success",
            intent_id="intent-success",
            cloid="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="local-block",
            intent_id="intent-blocked",
            cloid="0xcccccccccccccccccccccccccccccccc",
            status=IntentStatus.SKIPPED,
            exchange_status="blocked:rate_limit",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="auth-probe-success",
            intent_id="auth-probe:testnet:0xf000000000000000000000000000000000000000",
            cloid="0xcacacacacacacacacacacacacacacaca",
            status=IntentStatus.ACKED,
            exchange_status="auth_probe_ok",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="expected-testnet-dead-man-rejection",
            intent_id=("dead-man-schedule:testnet:0xf000000000000000000000000000000000000000"),
            cloid="0xcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbcb",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=created,
            payload={"testnet_dead_man_volume_rejection": True},
        ),
        ExecutionReport(
            report_id="expected-cross-margin-fallback",
            intent_id="leverage:xyz:JPY:1",
            cloid="0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=created,
            payload={"expected_cross_margin_fallback": True},
        ),
        ExecutionReport(
            report_id="new-failure-1",
            intent_id="intent-new-1",
            cloid="0xdddddddddddddddddddddddddddddddd",
            status=IntentStatus.REJECTED,
            exchange_status="exception",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="new-failure-2",
            intent_id="intent-new-2",
            cloid="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=created,
        ),
    ]
    for report in rows:
        store.append_execution_report(report)
    stats = store.consecutive_exchange_failure_stats()
    assert stats["consecutive_failures"] == 2
    assert stats["latest_failure_ms"] >= created


def _complete_hip3_ioc_zero_fill_payload(
    *,
    base_cloid: str,
    attempt_cloid: str,
    oid: int = 12345,
    filled_size: str = "0",
    remaining_size: str = "0.1",
    proof_cloid: str | None = None,
    top_level_proof_id: str | None = None,
    identity_attempt_cloid: str | None = None,
    predecessor_zero_fill_proof_id: str | None = None,
) -> dict:
    expected_attempt_cloid = (
        base_cloid
        if predecessor_zero_fill_proof_id is None
        else deterministic_cloid(
            "hip3-ioc-zero-fill-retry",
            base_cloid,
            predecessor_zero_fill_proof_id,
        )
    )
    assert attempt_cloid == expected_attempt_cloid
    coin = "xyz:KR200"
    side = "buy"
    size = "0.1"
    price = "100"
    order_timestamp = 1_700_000_000_000 + oid
    status_timestamp = order_timestamp + 100
    proof_id = deterministic_cloid(
        "hip3-ioc-zero-fill-proof",
        attempt_cloid,
        oid,
        status_timestamp,
    )
    order_status = {
        "status": "order",
        "order": {
            "status": "iocCancelRejected",
            "statusTimestamp": status_timestamp,
            "order": {
                "coin": coin,
                "side": "B",
                "limitPx": price,
                "sz": remaining_size,
                "origSz": size,
                "oid": oid,
                "timestamp": order_timestamp,
                "cloid": attempt_cloid,
                "reduceOnly": False,
                "tif": "Ioc",
                "orderType": "Limit",
                "children": [],
            },
        },
    }
    proof = {
        "kind": "hip3_ioc_zero_fill_v1",
        "proof_id": proof_id,
        "cloid": proof_cloid or attempt_cloid,
        "coin": coin,
        "side": side,
        "size": size,
        "price": price,
        "reduce_only": False,
        "oid": oid,
        "order_timestamp": order_timestamp,
        "status_timestamp": status_timestamp,
        "order_status": order_status,
    }
    return {
        "order_status": order_status,
        "signed_action_performed": True,
        "proven_zero_fill": True,
        "filled_size": filled_size,
        "requires_post_action_reconcile": True,
        "post_send_retry_identity": {
            "base_cloid": base_cloid,
            "attempt_cloid": identity_attempt_cloid or attempt_cloid,
            "predecessor_zero_fill_proof_id": predecessor_zero_fill_proof_id,
        },
        "zero_fill_proof_id": top_level_proof_id or proof_id,
        "zero_fill_proof": proof,
    }


def test_consecutive_exchange_failures_ignore_proven_hip3_ioc_zero_fill_rows(store):
    created = now_ms()
    reports = [
        ExecutionReport(
            report_id="failure-streak-reset",
            intent_id="intent-success",
            cloid="0x11111111111111111111111111111111",
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=created,
        ),
        ExecutionReport(
            report_id="actual-exchange-failure",
            intent_id="intent-failure",
            cloid="0x22222222222222222222222222222222",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=created,
        ),
    ]
    for index, exchange_status in enumerate(
        (
            "hip3_ioc_no_fill_deferred",
            "hip3_ioc_no_fill_cleanup_retry",
            "watchdog_settled:hip3_ioc_no_fill",
        ),
        start=3,
    ):
        cloid = f"0x{index:032x}"
        reports.append(
            ExecutionReport(
                report_id=f"proven-zero-fill-{index}",
                intent_id=f"intent-zero-fill-{index}",
                cloid=cloid,
                status=IntentStatus.REJECTED,
                exchange_status=exchange_status,
                exchange_ts_ms=created,
                payload=_complete_hip3_ioc_zero_fill_payload(
                    base_cloid=cloid,
                    attempt_cloid=cloid,
                    oid=10_000 + index,
                ),
            )
        )
    for report in reports:
        store.append_execution_report(report)

    stats = store.consecutive_exchange_failure_stats()

    # Neutral zero-fill evidence is excluded rather than treated as either a
    # failure or a success, so the preceding genuine rejection remains visible.
    assert stats["consecutive_failures"] == 1
    assert stats["latest_failure_ms"] >= created


def test_latest_hip3_ioc_zero_fill_proof_returns_latest_strict_matching_proof(store):
    created = now_ms()
    base_cloid = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    old_cloid = base_cloid
    old_payload = _complete_hip3_ioc_zero_fill_payload(
        base_cloid=base_cloid,
        attempt_cloid=old_cloid,
        oid=20_001,
    )
    predecessor = str(old_payload["zero_fill_proof_id"])
    new_cloid = deterministic_cloid(
        "hip3-ioc-zero-fill-retry",
        base_cloid,
        predecessor,
    )
    lookalike_cloid = base_cloid
    lookalike_payload = _complete_hip3_ioc_zero_fill_payload(
        base_cloid=base_cloid,
        attempt_cloid=lookalike_cloid,
        oid=20_002,
    )
    lookalike_payload["proven_zero_fill"] = False
    other_base_cloid = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    other_base_payload = _complete_hip3_ioc_zero_fill_payload(
        base_cloid=other_base_cloid,
        attempt_cloid=other_base_cloid,
        oid=20_003,
    )
    new_payload = _complete_hip3_ioc_zero_fill_payload(
        base_cloid=base_cloid,
        attempt_cloid=new_cloid,
        oid=20_004,
        predecessor_zero_fill_proof_id=predecessor,
    )

    rows = (
        ExecutionReport(
            report_id="zero-fill-old",
            intent_id="intent-zero-fill-old",
            cloid=old_cloid,
            status=IntentStatus.REJECTED,
            exchange_status="hip3_ioc_no_fill_deferred",
            exchange_ts_ms=created,
            payload=old_payload,
        ),
        ExecutionReport(
            report_id="zero-fill-lookalike-unproven",
            intent_id="intent-zero-fill-lookalike",
            cloid=lookalike_cloid,
            status=IntentStatus.REJECTED,
            exchange_status="hip3_ioc_no_fill_deferred",
            exchange_ts_ms=created,
            payload=lookalike_payload,
        ),
        ExecutionReport(
            report_id="zero-fill-other-base",
            intent_id="intent-zero-fill-other-base",
            cloid=other_base_cloid,
            status=IntentStatus.REJECTED,
            exchange_status="hip3_ioc_no_fill_deferred",
            exchange_ts_ms=created,
            payload=other_base_payload,
        ),
        ExecutionReport(
            report_id="zero-fill-new",
            intent_id="intent-zero-fill-new",
            cloid=new_cloid,
            status=IntentStatus.REJECTED,
            exchange_status="hip3_ioc_no_fill_deferred",
            exchange_ts_ms=created,
            payload=new_payload,
        ),
    )
    for report in rows:
        store.append_execution_report(report)

    assert store.latest_hip3_ioc_zero_fill_proof(base_cloid.upper()) == str(
        new_payload["zero_fill_proof_id"]
    )
    assert store.latest_hip3_ioc_zero_fill_proof("0x99999999999999999999999999999999") is None


@pytest.mark.parametrize(
    "malformation",
    (
        "partial_size",
        "mismatched_proof_id",
        "mismatched_proof_cloid",
        "mismatched_attempt_identity",
        "non_derivable_retry_identity",
        "non_neutral_exchange_status",
    ),
)
def test_malformed_hip3_ioc_zero_fill_rows_are_failures_and_cannot_seed_retry(
    store,
    malformation,
):
    created = now_ms()
    base_cloid = "0x11111111111111111111111111111111"
    attempt_cloid = base_cloid
    filled_size = "0"
    remaining_size = "0.1"
    proof_cloid = None
    top_level_proof_id = None
    identity_attempt_cloid = None
    exchange_status = "hip3_ioc_no_fill_deferred"
    if malformation == "partial_size":
        filled_size = "0.01"
        remaining_size = "0.09"
    elif malformation == "mismatched_proof_id":
        top_level_proof_id = deterministic_cloid("wrong-proof")
    elif malformation == "mismatched_proof_cloid":
        proof_cloid = "0x33333333333333333333333333333333"
    elif malformation == "mismatched_attempt_identity":
        identity_attempt_cloid = "0x44444444444444444444444444444444"
    elif malformation == "non_neutral_exchange_status":
        exchange_status = "rejected"
    payload = _complete_hip3_ioc_zero_fill_payload(
        base_cloid=base_cloid,
        attempt_cloid=attempt_cloid,
        oid=30_001,
        filled_size=filled_size,
        remaining_size=remaining_size,
        proof_cloid=proof_cloid,
        top_level_proof_id=top_level_proof_id,
        identity_attempt_cloid=identity_attempt_cloid,
    )
    if malformation == "non_derivable_retry_identity":
        payload["post_send_retry_identity"]["predecessor_zero_fill_proof_id"] = deterministic_cloid(
            "unrelated-predecessor-proof"
        )
    store.append_execution_report(
        ExecutionReport(
            report_id=f"malformed-zero-fill-{malformation}",
            intent_id=f"intent-malformed-{malformation}",
            cloid=attempt_cloid,
            status=IntentStatus.REJECTED,
            exchange_status=exchange_status,
            exchange_ts_ms=created,
            payload=payload,
        )
    )

    assert store.latest_hip3_ioc_zero_fill_proof(base_cloid) is None
    stats = store.consecutive_exchange_failure_stats()
    assert stats["consecutive_failures"] == 1
    assert stats["latest_failure_ms"] >= created


@pytest.mark.parametrize(
    "exchange_statuses",
    (
        ("settled:rejected",),
        ("rejected", "settled:rejected"),
    ),
    ids=("sole-settlement", "same-status-corroboration"),
)
def test_settled_rejection_counts_once_with_or_without_same_status_corroboration(
    store,
    exchange_statuses,
):
    created = now_ms()
    cloid = "0x55555555555555555555555555555555"
    store.append_execution_report(
        ExecutionReport(
            report_id="failure-reset",
            intent_id="intent-success-before-settlement",
            cloid="0x66666666666666666666666666666666",
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=created,
        )
    )
    for index, exchange_status in enumerate(exchange_statuses):
        store.append_execution_report(
            ExecutionReport(
                report_id=f"settled-rejection-{index}",
                intent_id="intent-settled-rejection",
                cloid=cloid,
                status=IntentStatus.REJECTED,
                exchange_status=exchange_status,
                exchange_ts_ms=created,
            )
        )

    stats = store.consecutive_exchange_failure_stats()
    assert stats["consecutive_failures"] == 1
    assert stats["latest_failure_ms"] >= created


def test_pending_intents_can_be_scoped_by_mode(store):
    shadow = FollowerIntent(
        intent_id="shadow-intent",
        cloid="0x11111111111111111111111111111111",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.SHADOW,
        source_event_key="source-shadow",
        reason="shadow",
        created_ms=now_ms(),
    )
    testnet = FollowerIntent(
        intent_id="testnet-intent",
        cloid="0x22222222222222222222222222222222",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source-testnet",
        reason="testnet",
        created_ms=now_ms(),
    )
    store.append_intent(shadow)
    store.append_intent(testnet)

    assert store.pending_intent_count() == 2
    assert store.pending_intent_count(Mode.SHADOW) == 1
    assert store.pending_intent_count(Mode.TESTNET) == 1
    assert store.pending_intent_count(Mode.PAPER) == 0
    assert [row["intent_id"] for row in store.pending_intents(Mode.TESTNET)] == ["testnet-intent"]


def test_execution_plan_is_atomic_and_attempt_phase_tracks_dispatch_boundary(store):
    desired = DesiredState(
        state_id="desired-atomic",
        source_event_key="source-atomic",
        mode=Mode.TESTNET,
        positions={"BTC": Position("BTC", Decimal("0.01"), leverage=1)},
        reason="atomic test",
        created_ms=now_ms(),
        source_wallet="0x" + "a" * 40,
        action_account="0x" + "b" * 40,
        source_network="testnet",
    )
    intent = FollowerIntent(
        intent_id="intent-atomic",
        cloid="0x" + "1" * 32,
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source-atomic",
        reason="atomic test",
        created_ms=now_ms(),
        desired_state_id=desired.state_id,
    )

    assert store.prepare_execution_plan(desired, [intent])
    prepared = store.intent_by_cloid(intent.cloid)
    assert prepared["desired_state_id"] == desired.state_id
    assert prepared["attempt_phase"] == ExecutionAttemptPhase.PREPARED.value
    assert store.begin_intent_dispatch(intent.intent_id) is True
    assert store.intent_by_cloid(intent.cloid)["attempt_phase"] == "dispatching"

    unknown = ExecutionReport(
        report_id="unknown-atomic",
        intent_id=intent.intent_id,
        cloid=intent.cloid,
        status=IntentStatus.SENT,
        exchange_status="transport_unknown",
        exchange_ts_ms=now_ms(),
    )
    store.append_execution_report(unknown)
    assert store.intent_by_cloid(intent.cloid)["attempt_phase"] == "unknown"
    terminal = ExecutionReport(
        report_id="terminal-atomic",
        intent_id=intent.intent_id,
        cloid=intent.cloid,
        status=IntentStatus.FILLED,
        exchange_status="settled:filled",
        exchange_ts_ms=now_ms(),
    )
    store.append_execution_report(terminal)
    assert store.intent_by_cloid(intent.cloid)["attempt_phase"] == "terminal"
    assert store.pending_intent_count(Mode.TESTNET) == 0
    assert not store.prepare_execution_plan(desired, [intent])


def test_non_order_signed_action_attempts_track_terminal_and_unknown_outcomes(store):
    account = "0x" + "b" * 40
    first_cloid = "0x" + "3" * 32
    assert store.prepare_signed_action_attempt(
        attempt_id=first_cloid,
        intent_id="dead-man-schedule:testnet:account",
        cloid=first_cloid,
        action="dead_man_schedule",
        mode=Mode.TESTNET,
        account=account,
        network="testnet",
        payload={"scheduled_time_ms": 12345, "api_private_key": "must-not-persist"},
    )
    assert store.recent("signed_action_attempts", 1)[0]["attempt_phase"] == "prepared"
    assert "must-not-persist" not in store.recent("signed_action_attempts", 1)[0]["payload_json"]
    assert store.begin_signed_action_dispatch(first_cloid)
    assert store.finish_signed_action_attempt(
        first_cloid,
        ExecutionReport(
            report_id="dead-man-acked",
            intent_id="dead-man-schedule:testnet:account",
            cloid=first_cloid,
            status=IntentStatus.ACKED,
            exchange_status="dead_man_scheduled",
            exchange_ts_ms=now_ms(),
        ),
    )
    assert store.recent("signed_action_attempts", 1)[0]["attempt_phase"] == "terminal"
    assert store.unresolved_signed_action_attempt_count(Mode.TESTNET, account=account) == 0

    second_cloid = "0x" + "4" * 32
    assert store.prepare_signed_action_attempt(
        attempt_id=second_cloid,
        intent_id="leverage:BTC:1",
        cloid=second_cloid,
        action="update_leverage_cross",
        mode=Mode.TESTNET,
        account=account,
        network="testnet",
        payload={"coin": "BTC", "leverage": 1},
    )
    assert store.begin_signed_action_dispatch(second_cloid)
    assert store.finish_signed_action_attempt(
        second_cloid,
        ExecutionReport(
            report_id="leverage-unknown",
            intent_id="leverage:BTC:1",
            cloid=second_cloid,
            status=IntentStatus.SENT,
            exchange_status="transport_unknown",
            exchange_ts_ms=now_ms(),
        ),
    )
    unresolved = store.unresolved_signed_action_attempts(Mode.TESTNET, account=account)
    assert [row["attempt_phase"] for row in unresolved] == ["unknown"]
    assert not store.prepare_signed_action_attempt(
        attempt_id="0x" + "5" * 32,
        intent_id="dead-man-clear:testnet:account",
        cloid="0x" + "5" * 32,
        action="dead_man_clear",
        mode=Mode.TESTNET,
        account=account,
        network="testnet",
        payload={},
    )


def test_execution_plan_rearms_only_after_durable_never_dispatched_proof(store):
    desired = DesiredState(
        state_id="desired-rearm",
        source_event_key="source-rearm",
        mode=Mode.TESTNET,
        positions={"BTC": Position("BTC", Decimal("0.01"), leverage=1)},
        reason="rearm test",
        created_ms=now_ms(),
        source_wallet="0x" + "a" * 40,
        action_account="0x" + "b" * 40,
        source_network="testnet",
    )
    intent = FollowerIntent(
        intent_id="intent-rearm",
        cloid="0x" + "9" * 32,
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source-rearm",
        reason="rearm test",
        created_ms=now_ms(),
        desired_state_id=desired.state_id,
    )
    assert store.prepare_execution_plan(desired, [intent])
    assert store.append_execution_report(
        ExecutionReport(
            report_id="never-dispatched-rearm",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="recovered:never_dispatched",
            exchange_ts_ms=now_ms(),
        )
    )

    for changed_intent in (
        replace(intent, side="sell"),
        replace(intent, size=Decimal("9")),
        replace(intent, price=Decimal("1")),
        replace(intent, reduce_only=True),
    ):
        assert not store.prepare_execution_plan(desired, [changed_intent])
    changed_state = replace(
        desired,
        positions={"BTC": Position("BTC", Decimal("9"), leverage=1)},
    )
    assert not store.prepare_execution_plan(changed_state, [intent])

    refreshed_desired = replace(
        desired,
        created_ms=desired.created_ms + 1,
        positions={"BTC": replace(desired.positions["BTC"], updated_ms=desired.created_ms + 1)},
    )
    refreshed_intent = replace(intent, created_ms=intent.created_ms + 1)
    assert store.prepare_execution_plan(refreshed_desired, [refreshed_intent])
    assert store.intent_by_cloid(intent.cloid)["attempt_phase"] == "prepared"
    assert store.has_dispatch_evidence_for_cloid(intent.cloid) is False
    assert store.begin_intent_dispatch(intent.intent_id) is True
    assert store.has_dispatch_evidence_for_cloid(intent.cloid) is True
    audit = store.recent("control_audit", 1)[0]
    assert audit["control"] == "rearm_execution_plan"


def test_execution_plan_rearm_atomically_refreshes_hip3_proof_and_price(store):
    observed = now_ms()
    desired = DesiredState(
        state_id="desired-hip3-rearm",
        source_event_key="source-hip3-rearm",
        mode=Mode.TESTNET,
        positions={"xyz:AAPL": Position("xyz:AAPL", Decimal("0.1"), leverage=1)},
        reason="HIP-3 proof refresh",
        created_ms=observed,
        source_wallet="0x" + "a" * 40,
        action_account="0x" + "b" * 40,
        source_network="mainnet",
    )
    proof = {
        "kind": "hip3_round_trip",
        "coin": "xyz:AAPL",
        "opening_side": "buy",
        "requested_size": Decimal("0.1"),
        "observed_ms": observed,
        "book_time_ms": observed,
        "oracle_px": Decimal("300"),
        "entry_limit": Decimal("300.5"),
        "exit_limit": Decimal("299.5"),
        "entry_visible_size": Decimal("1"),
        "exit_visible_size": Decimal("1"),
        "entry_worst_px": Decimal("300.5"),
        "exit_worst_px": Decimal("299.5"),
        "oracle_envelope_bps": Decimal("100"),
    }
    intent = FollowerIntent(
        intent_id="intent-hip3-rearm",
        cloid="0x" + "8" * 32,
        action=IntentAction.OPEN,
        coin="xyz:AAPL",
        side="buy",
        size=Decimal("0.1"),
        price=Decimal("300.5"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key=desired.source_event_key,
        reason="HIP-3 proof refresh",
        created_ms=observed,
        desired_state_id=desired.state_id,
        execution_proof=proof,
    )
    assert store.prepare_execution_plan(desired, [intent])
    assert store.append_execution_report(
        ExecutionReport(
            report_id="hip3-pre-send-blocked",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="pre_send_blocked",
            exchange_ts_ms=now_ms(),
        )
    )
    refreshed_proof = {
        **proof,
        "observed_ms": observed + 1,
        "book_time_ms": observed + 1,
        "entry_limit": Decimal("300.6"),
        "entry_worst_px": Decimal("300.6"),
    }
    refreshed = replace(
        intent,
        price=Decimal("300.6"),
        execution_proof=refreshed_proof,
        created_ms=observed + 1,
    )

    assert store.prepare_execution_plan(desired, [refreshed]) is True

    row = store.intent_by_cloid(intent.cloid)
    assert row is not None
    assert row["attempt_phase"] == ExecutionAttemptPhase.PREPARED.value
    stored_payload = json.loads(row["payload_json"])
    assert stored_payload["price"] == "300.6"
    assert stored_payload["execution_proof"]["entry_worst_px"] == "300.6"

    final = replace(
        refreshed,
        price=Decimal("300.7"),
        execution_proof={
            **refreshed_proof,
            "observed_ms": observed + 2,
            "book_time_ms": observed + 2,
            "entry_limit": Decimal("300.7"),
            "entry_worst_px": Decimal("300.7"),
        },
    )
    assert store.refresh_prepared_hip3_intent(final) is True
    row = store.intent_by_cloid(intent.cloid)
    assert row is not None
    assert json.loads(row["payload_json"])["price"] == "300.7"
    sdk_bound = replace(
        final,
        price=Decimal("300.8"),
        execution_proof={
            **final.execution_proof,
            "observed_ms": observed + 3,
            "book_time_ms": observed + 3,
            "entry_limit": Decimal("300.8"),
            "entry_worst_px": Decimal("300.8"),
        },
    )
    assert store.freeze_prepared_hip3_dispatch(replace(sdk_bound, size=Decimal("0.2"))) is False
    assert store.freeze_prepared_hip3_dispatch(sdk_bound) is True
    row = store.intent_by_cloid(intent.cloid)
    assert row is not None
    assert row["attempt_phase"] == ExecutionAttemptPhase.DISPATCHING.value
    assert json.loads(row["payload_json"])["price"] == "300.8"
    assert store.refresh_prepared_hip3_intent(refreshed) is False
    assert store.freeze_prepared_hip3_dispatch(replace(sdk_bound, price=Decimal("300.9"))) is False


def test_execution_plan_cannot_refresh_hip3_proof_after_dispatch_evidence(store):
    observed = now_ms()
    desired = DesiredState(
        state_id="desired-hip3-dispatched",
        source_event_key="source-hip3-dispatched",
        mode=Mode.TESTNET,
        positions={"xyz:AAPL": Position("xyz:AAPL", Decimal("0.1"), leverage=1)},
        reason="HIP-3 dispatched proof",
        created_ms=observed,
    )
    proof = {
        "kind": "hip3_round_trip",
        "coin": "xyz:AAPL",
        "opening_side": "buy",
        "requested_size": Decimal("0.1"),
    }
    intent = FollowerIntent(
        intent_id="intent-hip3-dispatched",
        cloid="0x" + "7" * 32,
        action=IntentAction.OPEN,
        coin="xyz:AAPL",
        side="buy",
        size=Decimal("0.1"),
        price=Decimal("300.5"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key=desired.source_event_key,
        reason="HIP-3 dispatched proof",
        created_ms=observed,
        desired_state_id=desired.state_id,
        execution_proof=proof,
    )
    assert store.prepare_execution_plan(desired, [intent])
    assert store.append_execution_report(
        ExecutionReport(
            report_id="hip3-dispatched",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.SENT,
            exchange_status="accepted",
            exchange_ts_ms=now_ms(),
        )
    )
    refreshed = replace(
        intent,
        price=Decimal("300.6"),
        execution_proof={**proof, "observed_ms": observed + 1},
    )

    assert store.prepare_execution_plan(desired, [refreshed]) is False


def test_prepare_execution_plan_rolls_back_whole_plan_on_identity_conflict(store):
    desired = DesiredState(
        state_id="desired-conflict",
        source_event_key="source-conflict",
        mode=Mode.TESTNET,
        positions={},
        reason="conflict test",
        created_ms=now_ms(),
    )
    conflicting = FollowerIntent(
        intent_id="intent-conflict",
        cloid="0x" + "2" * 32,
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source-conflict",
        reason="existing",
        created_ms=now_ms(),
    )
    store.append_intent(conflicting)
    linked = FollowerIntent(
        **{
            **conflicting.__dict__,
            "desired_state_id": desired.state_id,
        }
    )

    assert not store.prepare_execution_plan(desired, [linked])
    assert store.desired_state(desired.state_id) is None
    assert store.count("follower_intents") == 1


def test_runtime_state_and_recent_intents_can_be_scoped_by_mode(store):
    shadow = FollowerIntent(
        intent_id="shadow-intent",
        cloid="0x33333333333333333333333333333333",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.SHADOW,
        source_event_key="source-shadow",
        reason="shadow",
        created_ms=now_ms(),
    )
    testnet = FollowerIntent(
        intent_id="testnet-intent",
        cloid="0x44444444444444444444444444444444",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source-testnet",
        reason="testnet",
        created_ms=now_ms(),
    )
    store.append_intent(shadow)
    store.append_intent(testnet)
    store.append_desired_state(
        DesiredState(
            state_id="shadow-desired",
            source_event_key="source-shadow",
            mode=Mode.SHADOW,
            positions={"BTC": Position("BTC", Decimal("0.01"))},
            reason="shadow desired",
            created_ms=now_ms(),
        )
    )
    store.append_desired_state(
        DesiredState(
            state_id="testnet-desired",
            source_event_key="source-testnet",
            mode=Mode.TESTNET,
            positions={"BTC": Position("BTC", Decimal("0.01"))},
            reason="testnet desired",
            created_ms=now_ms(),
        )
    )

    runtime = store.rebuild_runtime_state(Mode.TESTNET)
    assert runtime["desired_state_count"] == 1
    assert [row["intent_id"] for row in runtime["pending_intents"]] == ["testnet-intent"]
    assert [row["intent_id"] for row in store.recent_intents(Mode.TESTNET)] == ["testnet-intent"]
    assert store.rebuild_runtime_state()["desired_state_count"] == 2
    assert {row["intent_id"] for row in store.recent_intents()} == {
        "shadow-intent",
        "testnet-intent",
    }


def test_latest_desired_positions_rebuilds_expected_exchange_baseline(store):
    desired = DesiredState(
        state_id="desired-1",
        source_event_key="source-1",
        mode=Mode.TESTNET,
        positions={"BTC": Position("BTC", Decimal("0.005"), entry_px=Decimal("50000"), leverage=2)},
        reason="test",
        created_ms=now_ms(),
    )
    assert store.latest_desired_positions(Mode.TESTNET) is None
    store.append_desired_state(desired)
    positions = store.latest_desired_positions(Mode.TESTNET)
    assert positions == {
        "BTC": Position("BTC", Decimal("0.005"), Decimal("50000"), 2, positions["BTC"].updated_ms)
    }
    assert store.latest_desired_positions(Mode.PAPER) is None


def test_latest_desired_positions_rejects_malformed_journal_payload(store):
    with store.lock:
        with store.conn:
            store.conn.execute(
                """
                INSERT INTO desired_states(state_id, source_event_key, mode, payload_json, created_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "bad-desired",
                    "source-1",
                    Mode.TESTNET.value,
                    '{"positions":{"BTC":{"coin":"BTC","size":"not-a-number"}}}',
                    now_ms(),
                ),
            )
    with pytest.raises(JournalIntegrityError, match="cannot be rebuilt"):
        store.latest_desired_positions(Mode.TESTNET)


def test_safe_mode_controller_trips_when_latest_reason_is_invalid(store):
    with store.lock:
        with store.conn:
            store.conn.execute(
                """
                INSERT INTO safe_mode_transitions(
                  transition_id, enabled, reason, detail, payload_json, created_ms
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("bad-safe", 1, "not_a_reason", "bad row", "{}", now_ms()),
            )
    controller = SafeModeController(store)
    assert controller.enabled
    assert controller.reason.value == "config_invalid"
    assert "safe-mode journal reason is invalid" in controller.detail


def test_runtime_recovery_preserves_seed_and_stream_state(store):
    sequence, inserted = store.append_runtime_event(
        event_key="runtime-seed-1",
        partition_key="source:0:0x" + "1" * 40,
        event_class="source_account_state",
        exchange_ts_ms=100,
        receive_wall_ms=101,
        receive_mono_ns=102,
        payload={"user": "0x" + "1" * 40, "isSnapshot": True},
        stream_state="REPLAYING",
        generation="generation-1",
        seed_snapshot=True,
    )

    assert inserted is True and sequence > 0
    recovery = store.fast_runtime_recovery(generation="generation-1")
    assert len(recovery["events"]) == 1
    assert recovery["events"][0]["seed_snapshot"] == 1
    assert recovery["events"][0]["stream_state"] == "REPLAYING"
