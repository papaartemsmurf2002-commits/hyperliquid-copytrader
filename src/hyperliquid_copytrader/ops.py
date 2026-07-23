from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def readiness_snapshot(dashboard: Mapping[str, Any]) -> dict[str, Any]:
    """Build a read-only operational readiness snapshot from dashboard state."""

    mode = str(dashboard.get("mode") or "unknown")
    safe_mode = _mapping(dashboard.get("safe_mode"))
    preflight = _mapping(dashboard.get("preflight"))
    ops = _mapping(dashboard.get("ops"))
    runtime = _mapping(dashboard.get("runtime"))
    security = _mapping(dashboard.get("security"))
    reconciliation = _mapping(dashboard.get("reconciliation_status"))
    account_context = _mapping(dashboard.get("account_context"))
    active_assignment = _mapping(dashboard.get("active_subaccount_assignment"))
    safe_mode_enabled = bool(safe_mode.get("enabled"))

    mode_gate_check = _mode_gate_check(mode)
    checks = [
        _preflight_check(preflight),
        {
            "name": "safe_mode_clear",
            "passed": not safe_mode_enabled,
            "detail": (
                str(safe_mode.get("detail") or safe_mode.get("reason") or "")
                if safe_mode_enabled
                else "clear"
            ),
        },
        {
            "name": "kill_switch_absent",
            "passed": not bool(ops.get("kill_switch_active")),
            "detail": str(ops.get("kill_switch_path") or ""),
        },
        {
            "name": "no_pending_intents",
            "passed": _int(ops.get("pending_intent_count")) == 0,
            "detail": str(ops.get("pending_intent_count") or 0),
        },
        {
            "name": "no_pending_source_reactions",
            "passed": _int(ops.get("pending_source_reaction_count")) == 0,
            "detail": str(ops.get("pending_source_reaction_count") or 0),
        },
        {
            "name": "security_audit_passed",
            "passed": bool(security.get("passed", False)),
            "detail": _security_detail(security),
        },
        {
            "name": "runtime_lease_available",
            "passed": runtime.get("exchange_lease_status") in {None, "clear", "stale"},
            "detail": str(runtime.get("exchange_lease_status") or "clear"),
        },
        {
            "name": "circuit_breaker_closed",
            "passed": not bool(runtime.get("circuit_breaker_open")),
            "detail": str(runtime.get("circuit_breaker_failures") or 0),
        },
        _source_reconciliation_check(mode, reconciliation),
        _follower_reconciliation_check(mode, reconciliation),
        _account_mode_check(mode, account_context),
        _active_subaccount_assignment_check(mode, active_assignment),
        mode_gate_check,
    ]
    ready = all(bool(check["passed"]) for check in checks)
    return {
        "mode": mode,
        "ready": ready,
        "status": "ready" if ready else "blocked",
        "readiness_label": _readiness_label(mode, ready),
        "checks": checks,
    }


def prometheus_metrics(dashboard: Mapping[str, Any]) -> str:
    readiness = readiness_snapshot(dashboard)
    mode = _label(str(dashboard.get("mode") or "unknown"))
    safe_mode = _mapping(dashboard.get("safe_mode"))
    preflight = _mapping(dashboard.get("preflight"))
    ops = _mapping(dashboard.get("ops"))
    runtime = _mapping(dashboard.get("runtime"))
    security = _mapping(dashboard.get("security"))
    reconciliation = _mapping(dashboard.get("reconciliation_status"))
    active_assignment = _mapping(dashboard.get("active_subaccount_assignment"))
    account_context = _mapping(dashboard.get("account_context"))
    containment_watchdog = _mapping(dashboard.get("containment_watchdog"))
    reason = _label(str(safe_mode.get("reason") or "none"))
    lease_status = _label(str(runtime.get("exchange_lease_status") or "clear"))
    source_reconciliation = _mapping(reconciliation.get("source"))
    follower_reconciliation = _mapping(reconciliation.get("follower"))
    source_reconciliation_status = _label(str(source_reconciliation.get("status") or "missing"))
    follower_reconciliation_status = _label(str(follower_reconciliation.get("status") or "missing"))
    assignment_check = _active_subaccount_assignment_check(mode, active_assignment)
    assignment_status = _label(str(active_assignment.get("status") or "missing"))
    account_mode_status = _label(str(account_context.get("status") or "missing"))
    expected_account_mode = _label(str(account_context.get("expected_mode") or "unknown"))
    detected_account_mode = _label(str(account_context.get("detected_mode") or "unknown"))
    dead_man_policy = _label(str(ops.get("dead_man_policy") or "exchange_required"))
    watchdog_status = _label(str(containment_watchdog.get("status") or "not_running"))

    samples = [
        "# HELP hlct_readiness_ready Read-only operational readiness from local gates.",
        "# TYPE hlct_readiness_ready gauge",
        f'hlct_readiness_ready{{mode="{mode}"}} {_bool(readiness["ready"])}',
        "# HELP hlct_local_preflight_passed Whether local config preflight passed.",
        "# TYPE hlct_local_preflight_passed gauge",
        f'hlct_local_preflight_passed{{mode="{mode}"}} {_bool(preflight.get("passed"))}',
        "# HELP hlct_safe_mode_enabled Whether safe mode is currently enabled.",
        "# TYPE hlct_safe_mode_enabled gauge",
        f'hlct_safe_mode_enabled{{mode="{mode}",reason="{reason}"}} '
        f"{_bool(safe_mode.get('enabled'))}",
        "# HELP hlct_kill_switch_active Whether the configured kill-switch file exists.",
        "# TYPE hlct_kill_switch_active gauge",
        f'hlct_kill_switch_active{{mode="{mode}"}} {_bool(ops.get("kill_switch_active"))}',
        "# HELP hlct_containment_watchdog_ready Whether independent order containment is fresh and healthy.",
        "# TYPE hlct_containment_watchdog_ready gauge",
        f'hlct_containment_watchdog_ready{{mode="{mode}",policy="{dead_man_policy}",'
        f'status="{watchdog_status}"}} {_bool(containment_watchdog.get("ready"))}',
        "# HELP hlct_containment_watchdog_heartbeat_age_ms Age of the latest watchdog heartbeat.",
        "# TYPE hlct_containment_watchdog_heartbeat_age_ms gauge",
        f'hlct_containment_watchdog_heartbeat_age_ms{{mode="{mode}"}} '
        f"{_int(containment_watchdog.get('heartbeat_age_ms'))}",
        "# HELP hlct_pending_intents Active-mode unresolved pending, sent, or acked intents.",
        "# TYPE hlct_pending_intents gauge",
        f'hlct_pending_intents{{mode="{mode}"}} {_int(ops.get("pending_intent_count"))}',
        "# HELP hlct_pending_source_reactions Source events awaiting current-truth validation.",
        "# TYPE hlct_pending_source_reactions gauge",
        f'hlct_pending_source_reactions{{mode="{mode}"}} '
        f"{_int(ops.get('pending_source_reaction_count'))}",
        "# HELP hlct_security_audit_passed Whether dashboard journal secret scans passed; "
        "expensive scans may be cached.",
        "# TYPE hlct_security_audit_passed gauge",
        f'hlct_security_audit_passed{{mode="{mode}"}} {_bool(security.get("passed"))}',
        "# HELP hlct_security_audit_cached Whether the dashboard security audit came from cache.",
        "# TYPE hlct_security_audit_cached gauge",
        f'hlct_security_audit_cached{{mode="{mode}"}} {_bool(security.get("cached"))}',
        "# HELP hlct_security_audit_cache_age_ms Dashboard security audit cache age in milliseconds.",
        "# TYPE hlct_security_audit_cache_age_ms gauge",
        f'hlct_security_audit_cache_age_ms{{mode="{mode}"}} {_int(security.get("cache_age_ms"))}',
        "# HELP hlct_reconciliation_source_fresh Whether source truth is fresh by dashboard "
        "reconciliation status.",
        "# TYPE hlct_reconciliation_source_fresh gauge",
        f'hlct_reconciliation_source_fresh{{mode="{mode}",status="{source_reconciliation_status}"}} '
        f"{_bool(source_reconciliation.get('status') == 'fresh')}",
        "# HELP hlct_reconciliation_follower_fresh Whether follower truth is fresh by dashboard "
        "reconciliation status.",
        "# TYPE hlct_reconciliation_follower_fresh gauge",
        f'hlct_reconciliation_follower_fresh{{mode="{mode}",status="{follower_reconciliation_status}"}} '
        f"{_bool(follower_reconciliation.get('status') == 'fresh')}",
        "# HELP hlct_reconciliation_ready_for_planning Whether reconciliation status has no planning blockers.",
        "# TYPE hlct_reconciliation_ready_for_planning gauge",
        f'hlct_reconciliation_ready_for_planning{{mode="{mode}"}} '
        f"{_bool(reconciliation.get('ready_for_planning'))}",
        "# HELP hlct_active_subaccount_assignment_ready Whether active assignment gating is clear.",
        "# TYPE hlct_active_subaccount_assignment_ready gauge",
        f'hlct_active_subaccount_assignment_ready{{mode="{mode}",status="{assignment_status}"}} '
        f"{_bool(assignment_check['passed'])}",
        "# HELP hlct_account_mode_ready Whether detected follower account mode and collateral truth match configuration.",
        "# TYPE hlct_account_mode_ready gauge",
        f'hlct_account_mode_ready{{mode="{mode}",expected="{expected_account_mode}",'
        f'detected="{detected_account_mode}",status="{account_mode_status}"}} '
        f"{_bool(_account_mode_check(mode, account_context)['passed'])}",
        "# HELP hlct_circuit_breaker_open Whether the runtime circuit breaker is open.",
        "# TYPE hlct_circuit_breaker_open gauge",
        f'hlct_circuit_breaker_open{{mode="{mode}"}} {_bool(runtime.get("circuit_breaker_open"))}',
        "# HELP hlct_circuit_breaker_failures Current in-memory consecutive exchange failures.",
        "# TYPE hlct_circuit_breaker_failures gauge",
        f'hlct_circuit_breaker_failures{{mode="{mode}"}} '
        f"{_int(runtime.get('circuit_breaker_failures'))}",
        "# HELP hlct_persistent_circuit_breaker_failures Journal-backed consecutive exchange failures.",
        "# TYPE hlct_persistent_circuit_breaker_failures gauge",
        f'hlct_persistent_circuit_breaker_failures{{mode="{mode}"}} '
        f"{_int(runtime.get('persistent_circuit_breaker_failures'))}",
        "# HELP hlct_runtime_lease_active Whether an active runtime lease exists.",
        "# TYPE hlct_runtime_lease_active gauge",
        f'hlct_runtime_lease_active{{mode="{mode}",status="{lease_status}"}} '
        f"{_bool(runtime.get('exchange_lease_status') == 'active')}",
        "# HELP hlct_runtime_lease_ms_remaining Remaining milliseconds on the exchange runtime lease.",
        "# TYPE hlct_runtime_lease_ms_remaining gauge",
        f'hlct_runtime_lease_ms_remaining{{mode="{mode}",status="{lease_status}"}} '
        f"{_int(runtime.get('exchange_lease_ms_remaining'))}",
        "# HELP hlct_rate_limiter_events In-process exchange actions in the local window.",
        "# TYPE hlct_rate_limiter_events gauge",
        f'hlct_rate_limiter_events{{mode="{mode}"}} {_int(runtime.get("rate_limiter_events"))}',
        "# HELP hlct_persistent_rate_limiter_events Journal-backed exchange actions in the local window.",
        "# TYPE hlct_persistent_rate_limiter_events gauge",
        f'hlct_persistent_rate_limiter_events{{mode="{mode}"}} '
        f"{_int(runtime.get('persistent_rate_limiter_events'))}",
        "# HELP hlct_max_exchange_actions_per_minute Configured exchange action rate budget.",
        "# TYPE hlct_max_exchange_actions_per_minute gauge",
        f'hlct_max_exchange_actions_per_minute{{mode="{mode}"}} '
        f"{_int(ops.get('max_exchange_actions_per_minute'))}",
        "# HELP hlct_source_websocket_idle_timeout_seconds Configured idle period before source "
        "websocket heartbeat ping.",
        "# TYPE hlct_source_websocket_idle_timeout_seconds gauge",
        f'hlct_source_websocket_idle_timeout_seconds{{mode="{mode}"}} '
        f"{_ms_to_seconds(ops.get('source_websocket_idle_timeout_ms'))}",
        "# HELP hlct_source_websocket_heartbeat_timeout_seconds Configured wait for source "
        "websocket heartbeat response.",
        "# TYPE hlct_source_websocket_heartbeat_timeout_seconds gauge",
        f'hlct_source_websocket_heartbeat_timeout_seconds{{mode="{mode}"}} '
        f"{_ms_to_seconds(ops.get('source_websocket_heartbeat_timeout_ms'))}",
    ]
    return "\n".join(samples) + "\n"


def _readiness_label(mode: str, ready: bool) -> str:
    if mode == "shadow":
        return "shadow_operational" if ready else "shadow_blocked"
    if mode == "paper":
        return "paper_locally_testable" if ready else "paper_blocked"
    if mode == "testnet":
        return "testnet_operational" if ready else "testnet_blocked"
    if mode == "live":
        return "live_blocked_pending_canary_monitoring_alerting_deployment_security_review"
    return "unknown_mode"


def _mode_gate_check(mode: str) -> dict[str, Any]:
    if mode in {"shadow", "paper"}:
        return {
            "name": "mode_operator_review",
            "passed": True,
            "detail": f"{mode} readiness can be assessed from local fail-closed gates",
        }
    if mode == "testnet":
        return {
            "name": "testnet_local_gates_clear",
            "passed": True,
            "detail": (
                "testnet readiness is derived from local safety gates; run the automated "
                "testnet canary to exercise signed preflight and smoke placement/cancel"
            ),
        }
    if mode == "live":
        return {
            "name": "live_production_review_complete",
            "passed": False,
            "detail": (
                "live mode remains blocked for production readiness until canary, monitoring, "
                "alerting, deployment, and security review are complete"
            ),
        }
    return {
        "name": "mode_known",
        "passed": False,
        "detail": f"unknown mode: {mode}",
    }


def _preflight_check(preflight: Mapping[str, Any]) -> dict[str, Any]:
    if not preflight:
        return {
            "name": "local_preflight_passed",
            "passed": False,
            "detail": "local preflight report is missing",
        }
    blockers = preflight.get("blockers") or []
    return {
        "name": "local_preflight_passed",
        "passed": bool(preflight.get("passed")),
        "detail": "pass" if preflight.get("passed") else f"blockers={len(blockers)}",
    }


def _source_reconciliation_check(mode: str, reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    if mode in {"shadow", "paper"}:
        return {
            "name": "source_reconciliation_fresh",
            "passed": True,
            "detail": f"{mode} mode validates source freshness during the run loop",
        }
    source = _mapping(reconciliation.get("source"))
    blockers = set(_strings(reconciliation.get("blockers")))
    status = str(source.get("status") or "missing")
    passed = (
        status == "fresh" and "source_missing" not in blockers and "source_stale" not in blockers
    )
    return {
        "name": "source_reconciliation_fresh",
        "passed": passed,
        "detail": f"source={status} age_ms={source.get('latest_age_ms')}",
    }


def _follower_reconciliation_check(mode: str, reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    if mode in {"shadow", "paper"}:
        return {
            "name": "follower_reconciliation_fresh",
            "passed": True,
            "detail": f"{mode} mode does not require exchange follower reconcile",
        }
    follower = _mapping(reconciliation.get("follower"))
    status = str(follower.get("status") or "missing")
    passed = status == "fresh"
    observed_account = str(follower.get("account") or "")
    expected_account = str(follower.get("expected_account") or "")
    return {
        "name": "follower_reconciliation_fresh",
        "passed": passed,
        "detail": (
            f"follower={status} age_ms={follower.get('latest_age_ms')} "
            f"expected_account={expected_account} observed_account={observed_account}"
        ),
    }


def _account_mode_check(mode: str, account_context: Mapping[str, Any]) -> dict[str, Any]:
    if mode in {"shadow", "paper"}:
        return {
            "name": "account_mode_supported",
            "passed": True,
            "detail": f"{mode} mode does not use exchange collateral",
        }
    status = str(account_context.get("status") or "missing")
    expected = str(account_context.get("expected_mode") or "unknown")
    detected = str(account_context.get("detected_mode") or "unknown")
    active = account_context.get("active_non_default_dexes")
    active_count = len(active) if isinstance(active, list) else 0
    unsupported = account_context.get("unsupported_non_default_dexes")
    unsupported_count = len(unsupported) if isinstance(unsupported, list) else 0
    return {
        "name": "account_mode_supported",
        "passed": status == "ready",
        "detail": (
            f"status={status} expected={expected} detected={detected} "
            f"collateral={account_context.get('collateral_source') or 'unknown'} "
            f"non_default_dexes={active_count} unsupported_dexes={unsupported_count}"
        ),
    }


def _active_subaccount_assignment_check(
    mode: str,
    active_assignment: Mapping[str, Any],
) -> dict[str, Any]:
    if mode not in {"testnet", "live"}:
        return {
            "name": "active_subaccount_assignment",
            "passed": True,
            "detail": f"{mode} mode does not require an exchange subaccount assignment",
        }
    if not active_assignment:
        return {
            "name": "active_subaccount_assignment",
            "passed": False,
            "detail": "active assignment status is missing",
        }
    status = str(active_assignment.get("status") or "unknown")
    blockers = _strings(active_assignment.get("blockers"))
    passed = status in {"matched", "not_configured"} and not blockers
    if status == "matched":
        detail = (
            f"slot={active_assignment.get('matched_slot') or ''} "
            f"action_account={active_assignment.get('action_account') or ''}"
        )
    elif status == "not_configured":
        detail = "single-account exchange config; no enabled assignment for current mode"
    elif blockers:
        detail = "; ".join(blockers)
    else:
        detail = str(active_assignment.get("detail") or status)
    return {
        "name": "active_subaccount_assignment",
        "passed": passed,
        "detail": detail,
    }


def _security_detail(security: Mapping[str, Any]) -> str:
    configured = security.get("configured_secret_occurrences") or []
    sensitive = security.get("sensitive_value_findings") or []
    if configured or sensitive:
        return (
            f"configured_secret_occurrences={len(configured)} sensitive_findings={len(sensitive)}"
        )
    return "no configured secret or sensitive-field findings"


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _bool(value: Any) -> int:
    return 1 if bool(value) else 0


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _ms_to_seconds(value: Any) -> str:
    milliseconds = _int(value)
    sign = "-" if milliseconds < 0 else ""
    whole, remainder = divmod(abs(milliseconds), 1000)
    if remainder == 0:
        return f"{sign}{whole}"
    fraction = str(remainder).rjust(3, "0").rstrip("0")
    return f"{sign}{whole}.{fraction}"


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
