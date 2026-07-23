from __future__ import annotations

import json
import sqlite3
import threading
from time import monotonic
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

import hyperliquid_copytrader.reconciliation as reconciliation_module
from hyperliquid_copytrader.reconciliation import (
    BudgetedInfoClient,
    RestAdmissionClosed,
    RestBudgetDenied,
    RestRetrySession,
    RestRetrySessionExpired,
    launch_info_priority,
)
from hyperliquid_copytrader.rest_budget import (
    RestBudgetCoordinator,
    RestGrant,
    RestLoadModel,
    RestPriority,
    RestTakeoverProof,
    authoritative_rest_weight,
)


def _grant(
    *,
    granted: bool,
    reason: str = "granted",
    retry_after_ms: int = 0,
    priority: RestPriority = RestPriority.BROAD_AUDIT,
    endpoint: str = "info:clearinghouseState",
) -> RestGrant:
    return RestGrant(
        grant_id="grant-1",
        granted=granted,
        generation="generation-rest-client",
        coordinator_epoch=1,
        sender="fleet-runtime",
        sender_epoch=1,
        message_id=1,
        priority=priority,
        endpoint=endpoint,
        weight=2,
        pool="ordinary",
        granted_wall_ms=1,
        retry_after_ms=retry_after_ms,
        reason=reason,
    )


def test_full_fleet_load_model_counts_periodic_nonfunding_ledger_reads() -> None:
    model = RestLoadModel(
        fleet_slots=10,
        audited_dexes=10,
        cheap_follower_queries_per_cycle=10,
    ).per_minute()

    assert model["nonfunding_ledger_audit"] == 20
    assert model["full_follower_dex_discovery"] == 20
    assert model["total"] == 540


def test_response_weighted_history_calls_reserve_the_documented_maximum() -> None:
    assert authoritative_rest_weight("info:historicalOrders") == 120
    assert authoritative_rest_weight("info:userFillsByTime") == 120
    assert authoritative_rest_weight("info:userTwapSliceFillsByTime") == 120
    assert authoritative_rest_weight("info:subAccounts") == 20


def test_launch_info_reads_use_only_launch_admission_priorities(tmp_path: Path) -> None:
    budget = RestBudgetCoordinator(
        tmp_path / "launch.sqlite3",
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=budget,
        sender="fleet-supervisor-launch",
        priority_resolver=launch_info_priority,
    )
    client.raw = SimpleNamespace(info=lambda payload: payload)
    try:
        client.info({"type": "spotClearinghouseState", "user": "0x" + "1" * 40})
        client.info({"type": "clearinghouseState", "user": "0x" + "1" * 40})
        snapshot = budget.recent_grant_snapshot()
    finally:
        budget.close()

    assert [row["priority"] for row in snapshot["grants"]] == [
        int(RestPriority.BROAD_AUDIT),
        int(RestPriority.BROAD_AUDIT),
    ]


def test_launch_info_client_defers_real_coordinator_budget_until_window_drains(
    tmp_path: Path,
) -> None:
    budget = RestBudgetCoordinator(
        tmp_path / "launch-wait.sqlite3",
        generation="host-wide-launch-preview-v1",
        ordinary_weight=20,
        reserve_weight=480,
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    assert budget.request_grant(
        sender="ui-launch-preview",
        sender_epoch=1,
        message_id=1,
        priority=RestPriority.BROAD_AUDIT,
        endpoint="info:openOrders",
        weight=20,
    ).granted
    waits: list[float] = []

    def drain_without_wall_sleep(wait_s: float) -> None:
        waits.append(wait_s)
        with budget._conn:
            budget._conn.execute("UPDATE rest_grants SET granted_mono_ms=granted_mono_ms-60001")

    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=budget,
        sender="fleet-supervisor-launch",
        priority_resolver=launch_info_priority,
        wait_for_budget=True,
        budget_deadline_mono=monotonic() + 120,
        budget_waiter=drain_without_wall_sleep,
    )
    client.raw = SimpleNamespace(info=lambda payload: payload)
    try:
        assert client.info({"type": "perpDexs"}) == {"type": "perpDexs"}
        assert waits and waits[0] > 0
    finally:
        budget.close()


def test_inflight_startup_request_retains_bounded_wait_contract_during_cutover() -> None:
    client: BudgetedInfoClient
    waits: list[float] = []

    class Grants:
        def __init__(self) -> None:
            self.calls = 0

        def request_grant(self, **_payload: object) -> RestGrant:
            self.calls += 1
            if self.calls == 1:
                client.wait_for_budget = False
                client.budget_deadline_mono = None
                client.budget_waiter = lambda _wait_s: pytest.fail(
                    "in-flight request used the post-activation waiter"
                )
                return _grant(
                    granted=False,
                    reason="rolling_weight_budget_exhausted",
                    retry_after_ms=10,
                )
            return _grant(granted=True)

    grants = Grants()
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=grants,  # type: ignore[arg-type]
        wait_for_budget=True,
        budget_deadline_mono=monotonic() + 10,
        budget_waiter=waits.append,
    )
    client.raw = SimpleNamespace(info=lambda payload: payload)

    assert client.info({"type": "perpDexs"}) == {"type": "perpDexs"}
    assert waits == [0.01]
    assert grants.calls == 2


def test_terminal_retry_session_preserves_request_phase_across_retryable_denials() -> None:
    class Grants:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []
            self.responses = [
                _grant(
                    granted=False,
                    reason="rolling_weight_budget_exhausted",
                    retry_after_ms=10,
                ),
                _grant(
                    granted=False,
                    reason="monotonic_clock_epoch_quarantine",
                    retry_after_ms=20,
                ),
                _grant(granted=True),
            ]

        def request_grant(self, **payload: object) -> RestGrant:
            self.calls.append(dict(payload))
            return self.responses.pop(0)

    grants = Grants()
    waits: list[float] = []
    raw_calls: list[dict[str, object]] = []
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=grants,  # type: ignore[arg-type]
        budget_waiter=waits.append,
    )

    def raw_info(payload: dict[str, object]) -> dict[str, object]:
        raw_calls.append(dict(payload))
        return {"assetPositions": []}

    client.raw = SimpleNamespace(info=raw_info)
    request = {"type": "clearinghouseState", "user": "0x" + "1" * 40}
    session = RestRetrySession(wait_deadline_mono=monotonic() + 10)

    assert client.info(request, retry_session=session) == {"assetPositions": []}
    assert [call["endpoint"] for call in grants.calls] == [
        "info:clearinghouseState",
        "info:clearinghouseState",
        "info:clearinghouseState",
    ]
    assert raw_calls == [request]
    assert waits == [0.01, 0.02]
    assert session.first_response_wall_ms > 0


def test_terminal_retry_session_rejects_nonretryable_denial_without_http() -> None:
    class Grants:
        def request_grant(self, **_payload: object) -> RestGrant:
            return _grant(granted=False, reason="host_wide_launch_egress_fenced")

    raw_calls: list[dict[str, object]] = []
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=Grants(),  # type: ignore[arg-type]
    )
    client.raw = SimpleNamespace(info=lambda payload: raw_calls.append(dict(payload)))

    with pytest.raises(RestBudgetDenied) as raised:
        client.info(
            {"type": "clearinghouseState", "user": "0x" + "1" * 40},
            retry_session=RestRetrySession(wait_deadline_mono=monotonic() + 10),
        )

    assert raised.value.grant.reason == "host_wide_launch_egress_fenced"
    assert raw_calls == []


def test_terminal_retry_session_has_independent_response_window_and_rejects_late_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    wall = [1_000_000_000]
    monkeypatch.setattr(reconciliation_module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(reconciliation_module, "time_ns", lambda: wall[0])
    session = RestRetrySession(wait_deadline_mono=400.0, response_window_s=60.0)

    clock[0] = 399.0
    session.observe_response()
    assert session.deadline_mono == 459.0
    accepted_wall_ms = session.last_response_wall_ms

    clock[0] = 458.9
    wall[0] += 1_000_000
    session.observe_response()
    assert session.last_response_wall_ms > accepted_wall_ms
    accepted_wall_ms = session.last_response_wall_ms

    clock[0] = 459.1
    wall[0] += 1_000_000
    with pytest.raises(RestRetrySessionExpired):
        session.observe_response()
    assert session.last_response_wall_ms == accepted_wall_ms


def test_terminal_session_expiry_after_grant_prevents_http_egress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [100.0]
    monkeypatch.setattr(reconciliation_module, "monotonic", lambda: clock[0])

    class Grants:
        def request_grant(self, **_payload: object) -> RestGrant:
            clock[0] = 101.0
            return _grant(granted=True)

    raw_calls: list[dict[str, object]] = []
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=Grants(),  # type: ignore[arg-type]
    )
    client.raw = SimpleNamespace(info=lambda payload: raw_calls.append(dict(payload)))

    with pytest.raises(RestRetrySessionExpired):
        client.info(
            {"type": "clearinghouseState", "user": "0x" + "1" * 40},
            retry_session=RestRetrySession(wait_deadline_mono=100.5),
        )
    assert raw_calls == []


def test_closing_ordinary_admission_is_an_inflight_barrier_and_keeps_priority_zero() -> None:
    started = threading.Event()
    release = threading.Event()

    class Grants:
        def request_grant(self, **payload: object) -> RestGrant:
            priority = RestPriority(int(str(payload["priority"])))
            endpoint = str(payload["endpoint"])
            return _grant(granted=True, priority=priority, endpoint=endpoint)

    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=Grants(),  # type: ignore[arg-type]
    )

    def raw_info(payload: dict[str, object]) -> dict[str, object]:
        if payload["type"] == "clearinghouseState":
            started.set()
            assert release.wait(5)
        return dict(payload)

    client.raw = SimpleNamespace(info=raw_info)
    ordinary = {"type": "clearinghouseState", "user": "0x" + "1" * 40}
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.info, ordinary)
        assert started.wait(5)
        assert client.close_ordinary_admission() == 1
        with pytest.raises(RestAdmissionClosed):
            client.info(ordinary)
        assert (
            client.info(
                {"type": "orderStatus", "user": "0x" + "1" * 40, "oid": "1"},
                priority=RestPriority.AMBIGUITY_CONTAINMENT,
            )["type"]
            == "orderStatus"
        )
        release.set()
        assert future.result(timeout=5) == ordinary
    assert client.ordinary_inflight_count() == 0


def test_closing_admission_during_grant_acquisition_prevents_late_http() -> None:
    grant_started = threading.Event()
    release_grant = threading.Event()

    class Grants:
        def request_grant(self, **_payload: object) -> RestGrant:
            grant_started.set()
            assert release_grant.wait(5)
            return _grant(granted=True)

    raw_calls: list[dict[str, object]] = []
    client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=Grants(),  # type: ignore[arg-type]
    )
    client.raw = SimpleNamespace(info=lambda payload: raw_calls.append(dict(payload)))
    request = {"type": "clearinghouseState", "user": "0x" + "1" * 40}
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.info, request)
        assert grant_started.wait(5)
        assert client.close_ordinary_admission() == 0
        release_grant.set()
        with pytest.raises(RestAdmissionClosed):
            future.result(timeout=5)
    assert raw_calls == []


def test_host_wide_snapshot_captures_all_senders_and_fences_new_egress(
    tmp_path: Path,
) -> None:
    path = tmp_path / "host-wide.sqlite3"
    first = RestBudgetCoordinator(
        path,
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    second = RestBudgetCoordinator(
        path,
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    try:
        assert first.request_grant(
            sender="context-feed-measurement",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.BROAD_AUDIT,
            endpoint="info:spotClearinghouseState",
            weight=2,
        ).granted
        assert second.request_grant(
            sender="fast-execution-benchmark",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.CATALOG,
            endpoint="info:perpDexs",
            weight=20,
        ).granted
        snapshot = first.recent_grant_snapshot()
        first.freeze_egress(
            preview_sha256="a" * 64,
            expected_snapshot=snapshot,
        )
        denied = second.request_grant(
            sender="ui-launch-preview",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.BROAD_AUDIT,
            endpoint="info:clearinghouseState",
            weight=2,
        )
        assert denied.granted is False
        assert denied.reason == "host_wide_launch_egress_fenced"
        assert first.release_egress_fence(preview_sha256="a" * 64)
        assert second.request_grant(
            sender="ui-launch-preview",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.BROAD_AUDIT,
            endpoint="info:clearinghouseState",
            weight=2,
        ).granted
    finally:
        second.close()
        first.close()

    assert {row["sender"] for row in snapshot["grants"]} == {
        "context-feed-measurement",
        "fast-execution-benchmark",
    }


def test_preview_snapshot_import_preserves_original_grant_age(tmp_path: Path) -> None:
    source = RestBudgetCoordinator(
        tmp_path / "preview.sqlite3",
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    try:
        assert source.request_grant(
            sender="ui-launch-preview",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.CATALOG,
            endpoint="info:perpDexs",
            weight=20,
        ).granted
        snapshot = source.recent_grant_snapshot()
    finally:
        source.close()
    source_mono = int(snapshot["grants"][0]["granted_mono_ms"])
    run = coordinator(tmp_path / "run.sqlite3")
    try:
        imported = run.import_recent_grant_snapshot(
            snapshot,
            source_generation="host-wide-launch-preview-v1",
        )
        replay = run.import_recent_grant_snapshot(
            snapshot,
            source_generation="host-wide-launch-preview-v1",
        )
        row = run._conn.execute("SELECT granted_mono_ms,sender FROM rest_grants").fetchone()
        assert imported == replay
        assert imported["imported_live_grant_count"] == 1
        assert imported["imported_weight"] == 20
        assert int(row["granted_mono_ms"]) == source_mono
        assert row["sender"] == "fleet-supervisor-preview-reservation"
        assert run.usage()["ordinary_used"] == 20
    finally:
        run.close()


def test_reboot_clock_epoch_quarantines_without_mutating_old_grants(
    tmp_path: Path,
) -> None:
    path = tmp_path / "reboot.sqlite3"
    first = coordinator(path)
    assert request(
        first,
        message_id=1,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    ).granted
    old_grant = tuple(
        first._conn.execute(
            "SELECT grant_id,granted_wall_ms,granted_mono_ms,clock_epoch FROM rest_grants"
        ).fetchone()
    )
    first.close()
    connection = sqlite3.connect(path)
    connection.execute("UPDATE coordinator_owner SET last_mono_ms=last_mono_ms+100000")
    connection.commit()
    connection.close()

    reopened = coordinator(path)
    try:
        assert reopened.clock_epoch == 2
        denied = request(
            reopened,
            message_id=2,
            priority=RestPriority.AFFECTED_FOLLOWER,
            endpoint="info:clearinghouseState",
            weight=2,
        )
        assert denied.granted is False
        assert denied.reason == "monotonic_clock_epoch_quarantine"
        owner = reopened._conn.execute(
            "SELECT last_wall_ms,last_mono_ms FROM coordinator_owner"
        ).fetchone()
        granted = reopened.request_grant(
            sender="fleet-runtime",
            sender_epoch=1,
            message_id=3,
            priority=RestPriority.AFFECTED_FOLLOWER,
            endpoint="info:clearinghouseState",
            weight=2,
            now_wall_ms=int(owner["last_wall_ms"]) + 60_001,
            now_mono_ms=int(owner["last_mono_ms"]) + 60_001,
        )
        assert granted.granted is True
        assert (
            tuple(
                reopened._conn.execute(
                    "SELECT grant_id,granted_wall_ms,granted_mono_ms,clock_epoch "
                    "FROM rest_grants ORDER BY seq LIMIT 1"
                ).fetchone()
            )
            == old_grant
        )
        assert (
            reopened._conn.execute("SELECT count(*) FROM coordinator_clock_epochs").fetchone()[0]
            == 2
        )
    finally:
        reopened.close()


def test_claim_initial_false_same_owner_requires_durable_clock_epoch_adoption(
    tmp_path: Path,
) -> None:
    path = tmp_path / "passive-clock-adoption.sqlite3"
    first = coordinator(path)
    first.close()
    connection = sqlite3.connect(path)
    connection.execute("UPDATE coordinator_owner SET last_mono_ms=last_mono_ms+100000")
    connection.commit()
    connection.close()
    rebooted = coordinator(path)
    assert rebooted.clock_epoch == 2
    rebooted.close()

    passive = RestBudgetCoordinator(
        path,
        generation="generation-1",
        ordinary_weight=720,
        reserve_weight=480,
        process_identity="rest-process-1",
        epoch=1,
        priority_coalesce_ms=0,
        claim_initial=False,
    )
    try:
        owner = passive.owner_snapshot()
        assert owner is not None and int(owner["clock_epoch"]) == 2
        passive.epoch = int(owner["epoch"])
        passive.process_identity = str(owner["process_identity"])
        with pytest.raises(RuntimeError, match="REST coordinator epoch fence failed"):
            request(
                passive,
                message_id=1,
                priority=RestPriority.AFFECTED_FOLLOWER,
                endpoint="info:clearinghouseState",
                weight=2,
            )
        passive.clock_epoch = int(owner["clock_epoch"])
        denied = request(
            passive,
            message_id=1,
            priority=RestPriority.AFFECTED_FOLLOWER,
            endpoint="info:clearinghouseState",
            weight=2,
        )
        assert denied.granted is False
        assert denied.reason == "monotonic_clock_epoch_quarantine"
    finally:
        passive.close()


def test_prior_boot_orphan_fence_releases_only_after_epoch_drain(tmp_path: Path) -> None:
    path = tmp_path / "fence-reboot.sqlite3"
    first = RestBudgetCoordinator(
        path,
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    assert first.request_grant(
        sender="ui-launch-preview",
        sender_epoch=1,
        message_id=1,
        priority=RestPriority.CATALOG,
        endpoint="info:perpDexs",
        weight=20,
    ).granted
    first.freeze_egress(
        preview_sha256="a" * 64,
        expected_snapshot=first.recent_grant_snapshot(),
    )
    first.close()
    connection = sqlite3.connect(path)
    connection.execute("UPDATE coordinator_owner SET last_mono_ms=last_mono_ms+100000")
    connection.commit()
    connection.close()

    reopened = RestBudgetCoordinator(
        path,
        generation="host-wide-launch-preview-v1",
        process_identity="host-wide-launch-preview-ledger",
        priority_coalesce_ms=0,
    )
    try:
        status = reopened.stale_egress_fence_release_status()
        assert status["ready"] is False
        assert status["reason"] == "prior_clock_epoch_quarantine"
        epoch = reopened._conn.execute(
            "SELECT quarantine_until_mono_ms FROM coordinator_clock_epochs WHERE clock_epoch=2"
        ).fetchone()
        assert (
            reopened.stale_egress_fence_release_status(
                now_mono_ms=int(epoch["quarantine_until_mono_ms"])
            )["ready"]
            is True
        )
    finally:
        reopened.close()


def coordinator(
    path: Path,
    *,
    ordinary: int = 720,
    reserve: int = 480,
    coalesce_ms: int = 0,
) -> RestBudgetCoordinator:
    return RestBudgetCoordinator(
        path,
        generation="generation-1",
        ordinary_weight=ordinary,
        reserve_weight=reserve,
        process_identity="rest-process-1",
        priority_coalesce_ms=coalesce_ms,
    )


def request(
    budget: RestBudgetCoordinator,
    *,
    message_id: int,
    priority: RestPriority,
    endpoint: str,
    weight: int,
):
    return budget.request_grant(
        sender="fleet-runtime",
        sender_epoch=1,
        message_id=message_id,
        priority=priority,
        endpoint=endpoint,
        weight=weight,
    )


def test_budget_and_endpoint_policy_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="IP allowance"):
        RestBudgetCoordinator(
            tmp_path / "oversubscribed.sqlite3",
            generation="g",
            ordinary_weight=721,
            reserve_weight=480,
            process_identity="p",
        )
    budget = coordinator(tmp_path / "policy.sqlite3")
    with pytest.raises(ValueError, match="weight mismatch"):
        request(
            budget,
            message_id=1,
            priority=RestPriority.AFFECTED_FOLLOWER,
            endpoint="info:clearinghouseState",
            weight=1,
        )
    with pytest.raises(ValueError, match="sender"):
        budget.request_grant(
            sender="untrusted-child",
            sender_epoch=1,
            message_id=1,
            priority=RestPriority.AMBIGUITY_CONTAINMENT,
            endpoint="info:orderStatus",
            weight=2,
        )
    budget.close()


def test_concurrent_priority_batch_admits_affected_before_broad(tmp_path: Path) -> None:
    budget = coordinator(
        tmp_path / "priority.sqlite3",
        ordinary=20,
        reserve=480,
        coalesce_ms=30,
    )
    barrier = threading.Barrier(3)

    def submit(message_id: int, priority: RestPriority, endpoint: str, weight: int):
        barrier.wait()
        return request(
            budget,
            message_id=message_id,
            priority=priority,
            endpoint=endpoint,
            weight=weight,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        broad = pool.submit(submit, 1, RestPriority.BROAD_AUDIT, "info:openOrders", 20)
        affected = pool.submit(
            submit,
            2,
            RestPriority.AFFECTED_FOLLOWER,
            "info:clearinghouseState",
            2,
        )
        barrier.wait()
        affected_grant = affected.result(timeout=2)
        broad_grant = broad.result(timeout=2)
    assert affected_grant.granted is True
    assert affected_grant.pool == "ordinary"
    assert broad_grant.granted is False
    assert broad_grant.reason == "rolling_weight_budget_exhausted"
    budget.close()


def test_containment_uses_reserve_before_affected_ordinary(tmp_path: Path) -> None:
    budget = coordinator(tmp_path / "pools.sqlite3", ordinary=20, reserve=20)
    containment = request(
        budget,
        message_id=1,
        priority=RestPriority.AMBIGUITY_CONTAINMENT,
        endpoint="info:openOrders",
        weight=20,
    )
    affected = request(
        budget,
        message_id=2,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    )
    assert containment.granted and containment.pool == "reserve"
    assert affected.granted and affected.pool == "ordinary"
    assert budget.usage()["reserve_used"] == 20
    assert budget.usage()["ordinary_used"] == 2
    budget.close()


def test_out_of_order_delivery_and_exact_replay_are_safe(tmp_path: Path) -> None:
    budget = coordinator(tmp_path / "replay.sqlite3")
    second = request(
        budget,
        message_id=2,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    )
    first = request(
        budget,
        message_id=1,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    )
    replay = request(
        budget,
        message_id=1,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    )
    assert second.granted and first.granted
    assert replay == first
    assert budget.usage()["ordinary_used"] == 4
    with pytest.raises(RuntimeError, match="different content"):
        request(
            budget,
            message_id=1,
            priority=RestPriority.BROAD_AUDIT,
            endpoint="info:openOrders",
            weight=20,
        )
    budget.close()


def test_recovery_client_restart_seeds_durable_sender_highwater(tmp_path: Path) -> None:
    path = tmp_path / "recovery-highwater.sqlite3"
    first = RestBudgetCoordinator(
        path,
        generation="generation-recovery-highwater",
        ordinary_weight=720,
        reserve_weight=480,
        process_identity="failed-rest-owner",
        epoch=1,
        priority_coalesce_ms=0,
    )
    raw_calls: list[dict[str, object]] = []

    def raw_info(payload: dict[str, object]) -> dict[str, object]:
        raw_calls.append(dict(payload))
        return dict(payload)

    first_client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=first,
        sender="containment-guardian-info-takeover",
        sender_epoch=2,
        priority_resolver=lambda _request_type: RestPriority.AMBIGUITY_CONTAINMENT,
    )
    first_client.raw = SimpleNamespace(info=raw_info)
    first_client.info({"type": "perpDexs"})
    assert (
        first.sender_message_highwater(sender="containment-guardian-info-takeover", sender_epoch=2)
        == 1
    )
    first.close()

    successor = RestBudgetCoordinator(
        path,
        generation="generation-recovery-highwater",
        ordinary_weight=720,
        reserve_weight=480,
        process_identity="failed-rest-owner",
        epoch=1,
        priority_coalesce_ms=0,
        claim_initial=False,
    )
    successor.takeover(
        expected_epoch=1,
        new_epoch=2,
        prior_process_identity="failed-rest-owner",
        new_process_identity="recovery-owner",
        proof=RestTakeoverProof(
            exact_prior_process_exited=True,
            exit_attestation_sha256="a" * 64,
            prior_grants_retained=True,
        ),
    )
    highwater = successor.sender_message_highwater(
        sender="containment-guardian-info-takeover", sender_epoch=2
    )
    second_client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=successor,
        sender="containment-guardian-info-takeover",
        sender_epoch=2,
        priority_resolver=lambda _request_type: RestPriority.AMBIGUITY_CONTAINMENT,
        initial_message_id=highwater,
    )
    second_client.raw = SimpleNamespace(info=raw_info)
    try:
        second_client.info({"type": "perpDexs"})
        assert len(raw_calls) == 2
        assert (
            successor.sender_message_highwater(
                sender="containment-guardian-info-takeover", sender_epoch=2
            )
            == 2
        )
        rows = successor._conn.execute(
            """
            SELECT message_id,coordinator_epoch FROM rest_grants
            WHERE sender='containment-guardian-info-takeover' ORDER BY message_id
            """
        ).fetchall()
        assert [tuple(row) for row in rows] == [(1, 1), (2, 2)]
    finally:
        successor.close()

    restarted = RestBudgetCoordinator(
        path,
        generation="generation-recovery-highwater",
        ordinary_weight=720,
        reserve_weight=480,
        process_identity="recovery-owner",
        epoch=2,
        priority_coalesce_ms=0,
        claim_initial=False,
    )
    restarted.takeover(
        expected_epoch=2,
        new_epoch=3,
        prior_process_identity="recovery-owner",
        new_process_identity="restarted-recovery-owner",
        proof=RestTakeoverProof(
            exact_prior_process_exited=True,
            exit_attestation_sha256="b" * 64,
            prior_grants_retained=True,
        ),
    )
    third_client = BudgetedInfoClient(
        base_url="https://api.hyperliquid.xyz",
        timeout_s=1,
        grants=restarted,
        sender="containment-guardian-info-takeover",
        sender_epoch=2,
        priority_resolver=lambda _request_type: RestPriority.AMBIGUITY_CONTAINMENT,
        initial_message_id=restarted.sender_message_highwater(
            sender="containment-guardian-info-takeover", sender_epoch=2
        ),
    )
    third_client.raw = SimpleNamespace(info=raw_info)
    try:
        third_client.info({"type": "perpDexs"})
        rows = restarted._conn.execute(
            """
            SELECT message_id,coordinator_epoch FROM rest_grants
            WHERE sender='containment-guardian-info-takeover' ORDER BY message_id
            """
        ).fetchall()
        assert len(raw_calls) == 3
        assert [tuple(row) for row in rows] == [(1, 1), (2, 2), (3, 3)]
    finally:
        restarted.close()


def test_same_priority_is_fifo_by_durable_enqueue_sequence(tmp_path: Path) -> None:
    path = tmp_path / "fifo.sqlite3"
    budget = coordinator(path, ordinary=20, reserve=20, coalesce_ms=30)
    barrier = threading.Barrier(3)

    def submit(message_id: int):
        barrier.wait()
        return request(
            budget,
            message_id=message_id,
            priority=RestPriority.BROAD_AUDIT,
            endpoint="info:openOrders",
            weight=20,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(submit, 1)
        two = pool.submit(submit, 2)
        barrier.wait()
        decisions = {1: one.result(timeout=2), 2: two.result(timeout=2)}
    connection = sqlite3.connect(path)
    first_message = int(
        connection.execute("SELECT message_id FROM rest_requests ORDER BY seq LIMIT 1").fetchone()[
            0
        ]
    )
    connection.close()
    assert decisions[first_message].granted is True
    assert sum(int(decision.granted) for decision in decisions.values()) == 1
    budget.close()


def test_takeover_validates_attestation_retains_grants_and_orphans_pending(
    tmp_path: Path,
) -> None:
    path = tmp_path / "takeover.sqlite3"
    budget = coordinator(path)
    granted = request(
        budget,
        message_id=1,
        priority=RestPriority.AFFECTED_FOLLOWER,
        endpoint="info:clearinghouseState",
        weight=2,
    )
    budget.close()
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO rest_requests(
          generation, sender, sender_epoch, message_id, priority, endpoint,
          weight, request_sha256, requested_wall_ms, requested_mono_ms, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        """,
        (
            "generation-1",
            "fleet-runtime",
            1,
            2,
            int(RestPriority.BROAD_AUDIT),
            "info:openOrders",
            20,
            "b" * 64,
            1,
            1,
        ),
    )
    connection.commit()
    connection.close()
    successor = RestBudgetCoordinator(
        path,
        generation="generation-1",
        ordinary_weight=720,
        reserve_weight=480,
        process_identity="rest-process-1",
        epoch=1,
        claim_initial=False,
        priority_coalesce_ms=0,
    )
    with pytest.raises(RuntimeError, match="incomplete"):
        successor.takeover(
            expected_epoch=1,
            new_epoch=2,
            prior_process_identity="rest-process-1",
            new_process_identity="guardian-process-2",
            proof=RestTakeoverProof(True, "not-a-sha", True),
        )
    successor.takeover(
        expected_epoch=1,
        new_epoch=2,
        prior_process_identity="rest-process-1",
        new_process_identity="guardian-process-2",
        proof=RestTakeoverProof(True, "a" * 64, True),
    )
    assert successor.usage()["ordinary_used"] == granted.weight
    connection = sqlite3.connect(path)
    response = json.loads(
        connection.execute("SELECT response_json FROM rest_requests WHERE message_id=2").fetchone()[
            0
        ]
    )
    grant_count = int(connection.execute("SELECT count(*) FROM rest_grants").fetchone()[0])
    connection.close()
    assert response["granted"] is False
    assert response["reason"] == "coordinator_takeover_requires_exact_request_replay"
    assert grant_count == 1
    successor.close()
