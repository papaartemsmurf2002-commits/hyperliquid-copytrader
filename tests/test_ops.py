from __future__ import annotations

from copy import deepcopy

from hyperliquid_copytrader.ops import prometheus_metrics, readiness_snapshot


def _dashboard() -> dict:
    return {
        "mode": "shadow",
        "preflight": {"passed": True, "blockers": [], "warnings": []},
        "safe_mode": {"enabled": False, "reason": "none", "detail": ""},
        "ops": {
            "kill_switch_active": False,
            "kill_switch_path": "data/KILL_SWITCH",
            "pending_intent_count": 0,
            "max_exchange_actions_per_minute": 30,
            "source_websocket_idle_timeout_ms": 55_000,
            "source_websocket_heartbeat_timeout_ms": 5_000,
            "dead_man_policy": "watchdog_fallback",
        },
        "containment_watchdog": {
            "ready": True,
            "status": "running",
            "heartbeat_age_ms": 25,
        },
        "runtime": {
            "exchange_lease_status": "clear",
            "exchange_lease_ms_remaining": 0,
            "circuit_breaker_open": False,
            "circuit_breaker_failures": 0,
            "persistent_circuit_breaker_failures": 0,
            "rate_limiter_events": 0,
            "persistent_rate_limiter_events": 0,
        },
        "security": {
            "passed": True,
            "configured_secret_occurrences": [],
            "sensitive_value_findings": [],
            "cached": True,
            "cache_age_ms": 10,
        },
        "reconciliation_status": {
            "source": {"status": "missing", "latest_age_ms": None},
            "follower": {"status": "not_required", "latest_age_ms": None, "required": False},
            "blockers": ["source_missing"],
            "ready_for_planning": False,
        },
        "active_subaccount_assignment": {
            "status": "not_configured",
            "passed": True,
            "blockers": [],
            "matched_slot": "",
            "action_account": "",
        },
        "account_context": {
            "required": False,
            "expected_mode": "auto",
            "detected_mode": "standard",
            "status": "ready",
            "collateral_source": "perp_margin_summary",
            "account_value": "50",
            "active_non_default_dexes": [],
        },
    }


def test_readiness_snapshot_passes_for_clear_shadow_dashboard():
    snapshot = readiness_snapshot(_dashboard())
    assert snapshot["ready"] is True
    assert snapshot["readiness_label"] == "shadow_operational"
    assert {check["name"] for check in snapshot["checks"]} >= {
        "safe_mode_clear",
        "kill_switch_absent",
        "local_preflight_passed",
        "security_audit_passed",
        "source_reconciliation_fresh",
        "follower_reconciliation_fresh",
    }


def test_readiness_snapshot_hides_stale_safe_mode_detail_when_clear():
    dashboard = _dashboard()
    dashboard["safe_mode"]["detail"] = "previous clearance detail"
    snapshot = readiness_snapshot(dashboard)
    safe_check = next(check for check in snapshot["checks"] if check["name"] == "safe_mode_clear")
    assert safe_check["passed"] is True
    assert safe_check["detail"] == "clear"


def test_readiness_snapshot_never_marks_live_production_ready():
    dashboard = deepcopy(_dashboard())
    dashboard["mode"] = "live"
    snapshot = readiness_snapshot(dashboard)
    assert snapshot["ready"] is False
    assert snapshot["readiness_label"].startswith("live_blocked")
    live_check = next(
        check for check in snapshot["checks"] if check["name"] == "live_production_review_complete"
    )
    assert live_check["passed"] is False


def test_readiness_snapshot_allows_clear_testnet_dashboard():
    dashboard = deepcopy(_dashboard())
    dashboard["mode"] = "testnet"
    dashboard["reconciliation_status"] = {
        "source": {"status": "fresh", "latest_age_ms": 100},
        "follower": {"status": "fresh", "latest_age_ms": 100, "required": True},
        "blockers": [],
        "ready_for_planning": True,
    }
    snapshot = readiness_snapshot(dashboard)
    assert snapshot["ready"] is True
    assert snapshot["readiness_label"] == "testnet_operational"
    testnet_check = next(
        check for check in snapshot["checks"] if check["name"] == "testnet_local_gates_clear"
    )
    assert testnet_check["passed"] is True
    account_mode_check = next(
        check for check in snapshot["checks"] if check["name"] == "account_mode_supported"
    )
    assert account_mode_check["passed"] is True


def test_readiness_snapshot_blocks_account_mode_mismatch():
    dashboard = deepcopy(_dashboard())
    dashboard["mode"] = "testnet"
    dashboard["reconciliation_status"] = {
        "source": {"status": "fresh", "latest_age_ms": 100},
        "follower": {"status": "fresh", "latest_age_ms": 100, "required": True},
        "blockers": [],
        "ready_for_planning": True,
    }
    dashboard["account_context"].update(
        {
            "expected_mode": "standard",
            "detected_mode": "unified",
            "status": "mismatch",
            "collateral_source": "spot_usdc_unified",
        }
    )

    snapshot = readiness_snapshot(dashboard)

    assert snapshot["ready"] is False
    check = next(item for item in snapshot["checks"] if item["name"] == "account_mode_supported")
    assert check["passed"] is False
    assert "expected=standard detected=unified" in check["detail"]


def test_readiness_snapshot_blocks_testnet_without_fresh_reconcile_status():
    dashboard = deepcopy(_dashboard())
    dashboard["mode"] = "testnet"
    dashboard["reconciliation_status"] = {
        "source": {"status": "stale", "latest_age_ms": 12_000},
        "follower": {"status": "missing", "latest_age_ms": None, "required": True},
        "blockers": ["source_stale", "follower_missing"],
        "ready_for_planning": False,
    }

    snapshot = readiness_snapshot(dashboard)

    assert snapshot["ready"] is False
    assert snapshot["readiness_label"] == "testnet_blocked"
    source_check = next(
        check for check in snapshot["checks"] if check["name"] == "source_reconciliation_fresh"
    )
    follower_check = next(
        check for check in snapshot["checks"] if check["name"] == "follower_reconciliation_fresh"
    )
    assert source_check["passed"] is False
    assert "source=stale" in source_check["detail"]
    assert follower_check["passed"] is False
    assert "follower=missing" in follower_check["detail"]


def test_readiness_snapshot_blocks_testnet_with_active_assignment_mismatch():
    dashboard = deepcopy(_dashboard())
    dashboard["mode"] = "testnet"
    dashboard["reconciliation_status"] = {
        "source": {"status": "fresh", "latest_age_ms": 100},
        "follower": {"status": "fresh", "latest_age_ms": 100, "required": True},
        "blockers": [],
        "ready_for_planning": True,
    }
    dashboard["active_subaccount_assignment"] = {
        "status": "missing_match",
        "passed": False,
        "blockers": ["no verified enabled testnet subaccount assignment matches"],
        "matched_slot": "",
        "action_account": "0xf000000000000000000000000000000000000000",
    }

    snapshot = readiness_snapshot(dashboard)

    assert snapshot["ready"] is False
    assignment_check = next(
        check for check in snapshot["checks"] if check["name"] == "active_subaccount_assignment"
    )
    assert assignment_check["passed"] is False
    assert "no verified enabled testnet" in assignment_check["detail"]


def test_readiness_snapshot_blocks_when_local_preflight_fails():
    dashboard = deepcopy(_dashboard())
    dashboard["preflight"] = {"passed": False, "blockers": ["source wallet missing"]}

    snapshot = readiness_snapshot(dashboard)

    assert snapshot["ready"] is False
    preflight_check = next(
        check for check in snapshot["checks"] if check["name"] == "local_preflight_passed"
    )
    assert preflight_check == {
        "name": "local_preflight_passed",
        "passed": False,
        "detail": "blockers=1",
    }


def test_prometheus_metrics_escape_labels_and_report_core_gauges():
    dashboard = _dashboard()
    dashboard["safe_mode"] = {
        "enabled": True,
        "reason": 'operator_"kill"',
        "detail": "blocked",
    }
    dashboard["runtime"]["exchange_lease_status"] = "active"
    dashboard["runtime"]["exchange_lease_ms_remaining"] = 12_345
    dashboard["runtime"]["persistent_circuit_breaker_failures"] = 2
    text = prometheus_metrics(dashboard)
    assert 'hlct_safe_mode_enabled{mode="shadow",reason="operator_\\"kill\\""} 1' in text
    assert 'hlct_pending_intents{mode="shadow"} 0' in text
    assert (
        'hlct_containment_watchdog_ready{mode="shadow",policy="watchdog_fallback",status="running"} 1'
        in text
    )
    assert 'hlct_containment_watchdog_heartbeat_age_ms{mode="shadow"} 25' in text
    assert 'hlct_pending_source_reactions{mode="shadow"} 0' in text
    assert 'hlct_local_preflight_passed{mode="shadow"} 1' in text
    assert 'hlct_security_audit_passed{mode="shadow"} 1' in text
    assert 'hlct_security_audit_cached{mode="shadow"} 1' in text
    assert 'hlct_security_audit_cache_age_ms{mode="shadow"} 10' in text
    assert 'hlct_reconciliation_source_fresh{mode="shadow",status="missing"} 0' in text
    assert 'hlct_reconciliation_follower_fresh{mode="shadow",status="not_required"} 0' in text
    assert 'hlct_reconciliation_ready_for_planning{mode="shadow"} 0' in text
    assert (
        'hlct_active_subaccount_assignment_ready{mode="shadow",status="not_configured"} 1' in text
    )
    assert 'hlct_runtime_lease_active{mode="shadow",status="active"} 1' in text
    assert 'hlct_runtime_lease_ms_remaining{mode="shadow",status="active"} 12345' in text
    assert 'hlct_persistent_circuit_breaker_failures{mode="shadow"} 2' in text
    assert 'hlct_max_exchange_actions_per_minute{mode="shadow"} 30' in text
    assert 'hlct_source_websocket_idle_timeout_seconds{mode="shadow"} 55' in text
    assert 'hlct_source_websocket_heartbeat_timeout_seconds{mode="shadow"} 5' in text
