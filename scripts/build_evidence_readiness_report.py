from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


EVIDENCE_READINESS_REPORT_VERSION = 2
SUPPORTED_SLOT_STATE_REPORT_VERSION = 2
SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION = 2
DEFAULT_STRATEGY = "risk_budget_50_cap_10x"
CORRECTED_DEFAULT_BACKTEST = {
    "days": 180,
    "source_count": 10,
    "strategy_rows": 10,
    "total_start_equity_usd": Decimal("500.0"),
    "ending_equity_usd": Decimal("4148.89283278893981"),
    "net_pnl_usd": Decimal("3648.892832788939795"),
    "copied_fills": 28770,
    "skipped_min_notional_fills": 36207,
    "capped_fills": 413,
    "zeroed_slots": 2,
    "truncated_sources": 0,
}
SYNTHETIC_NOTE_MARKERS = (
    "analysis-only",
    "not live recovery proof",
    "not verified",
    "recorded source",
    "synthetic",
    "whatif",
    "what-if",
)


class EvidenceReadinessInputError(RuntimeError):
    """Raised when evidence artifacts cannot be loaded or interpreted."""


def build_evidence_readiness_report(
    *,
    backtest: dict[str, Any],
    slot_state: dict[str, Any],
    recovery_evidence: dict[str, Any] | None = None,
    strategy: str = DEFAULT_STRATEGY,
    backtest_path: Path | None = None,
    slot_state_path: Path | None = None,
    recovery_evidence_path: Path | None = None,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = [
        "public-data backtest is a rough strategy comparator, not live-risk proof",
        "testnet canary still requires explicit testnet credentials, fresh runtime preflight, "
        "flat cleanup, settlement, metrics, and dashboard checks",
        "mainnet live remains not default-ready and requires a separate operator request",
    ]

    backtest_summary, backtest_checks = summarize_backtest(backtest, strategy=strategy)
    backtest_companion_summary, backtest_companion_checks = summarize_backtest_companion_csv(
        backtest_path=backtest_path,
        backtest_summary=backtest_summary,
        strategy=strategy,
    )
    slot_summary, slot_checks = summarize_slot_state(slot_state)
    recovery_summary, recovery_checks = summarize_recovery_evidence(recovery_evidence)
    checks = [*backtest_checks, *backtest_companion_checks, *slot_checks, *recovery_checks]
    for check in checks:
        if not check["passed"]:
            blockers.append(f"{check['name']}: {check['detail']}")

    shadow_replay_ready = not blockers
    testnet_canary_candidate = shadow_replay_ready
    status = "shadow_replay_ready" if shadow_replay_ready else "blocked"
    next_required_actions = next_actions(
        shadow_replay_ready=shadow_replay_ready,
        slot_summary=slot_summary,
        recovery_summary=recovery_summary,
    )

    return {
        "evidence_readiness_report_version": EVIDENCE_READINESS_REPORT_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "input_paths": {
            "backtest": path_str(backtest_path),
            "backtest_companion_csv": clean(backtest_companion_summary.get("path")),
            "slot_state_report": path_str(slot_state_path),
            "recovery_evidence_report": path_str(recovery_evidence_path),
        },
        "strategy": strategy,
        "status": status,
        "shadow_replay_ready": shadow_replay_ready,
        "testnet_canary_candidate": testnet_canary_candidate,
        "mainnet_ready": False,
        "mainnet_reason": "mainnet live is gated by explicit operator approval and is never default-ready",
        "checks": checks,
        "blockers": blockers,
        "warnings": warnings,
        "backtest": backtest_summary,
        "backtest_companion_csv": backtest_companion_summary,
        "slot_state": slot_summary,
        "recovery_evidence": recovery_summary,
        "next_required_actions": next_required_actions,
    }


def summarize_backtest(
    backtest: dict[str, Any],
    *,
    strategy: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sources = list_items(backtest.get("sources"))
    strategy_rows: list[dict[str, Any]] = []
    truncated_sources = 0
    for source in sources:
        if source.get("truncated") is True:
            truncated_sources += 1
        for simulation in list_items(source.get("simulations")):
            if clean(simulation.get("strategy")) == strategy:
                strategy_rows.append(simulation)

    total_initial = sum_decimal(strategy_rows, "initial_equity_usd")
    total_ending = sum_decimal(strategy_rows, "ending_equity_usd")
    total_net = sum_decimal(strategy_rows, "net_pnl_usd")
    copied_fills = sum_int(strategy_rows, "copied_fills")
    skipped_min_notional = sum_int(strategy_rows, "skipped_min_notional_fills")
    capped_fills = sum_int(strategy_rows, "capped_fills")
    zeroed_slots = sum(1 for row in strategy_rows if row.get("liquidated_or_zero_equity") is True)
    days = int_optional(backtest.get("days")) or 0

    summary = {
        "days": days,
        "source_count": len(sources),
        "strategy_rows": len(strategy_rows),
        "strategy": strategy,
        "total_start_equity_usd": decimal_str(total_initial),
        "ending_equity_usd": decimal_str(total_ending),
        "net_pnl_usd": decimal_str(total_net),
        "copied_fills": copied_fills,
        "skipped_min_notional_fills": skipped_min_notional,
        "capped_fills": capped_fills,
        "zeroed_slots": zeroed_slots,
        "truncated_sources": truncated_sources,
        "limitations": [
            "public-data backtest does not prove live slippage, fills, queueing, or liquidation risk",
            "skipped_min_notional_fills are dust/min-notional skips, not successful copies",
            "zeroed slots indicate paths where this rough simulation depleted a follower allocation",
        ],
    }
    checks = [
        check(
            "backtest_sources_present",
            bool(sources),
            f"sources={len(sources)}",
        ),
        check(
            "backtest_strategy_present",
            bool(strategy_rows),
            f"strategy={strategy} rows={len(strategy_rows)}",
        ),
        check(
            "backtest_no_source_truncation",
            truncated_sources == 0,
            f"truncated_sources={truncated_sources}",
        ),
        check(
            "backtest_strategy_has_copied_fills",
            copied_fills > 0,
            f"copied_fills={copied_fills}",
        ),
        corrected_backtest_check(summary, strategy=strategy),
    ]
    return summary, checks


def summarize_backtest_companion_csv(
    *,
    backtest_path: Path | None,
    backtest_summary: dict[str, Any],
    strategy: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if backtest_path is None:
        return {
            "checked": False,
            "path": "",
            "status": "not_checked",
            "reason": "backtest path was not provided",
            "strategy": strategy,
        }, []

    csv_path = backtest_path.with_suffix(".csv")
    summary: dict[str, Any] = {
        "checked": True,
        "path": str(csv_path),
        "status": "missing",
        "strategy": strategy,
    }
    if not csv_path.exists():
        return summary, [
            check(
                "backtest_companion_csv_present",
                False,
                f"missing sibling CSV artifact: {csv_path}",
            )
        ]

    try:
        rows, fieldnames = read_csv_rows(csv_path)
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        summary["status"] = "unreadable"
        summary["error"] = str(exc)
        return summary, [
            check("backtest_companion_csv_present", True, f"path={csv_path}"),
            check("backtest_companion_csv_readable", False, str(exc)),
        ]

    missing_columns = sorted(required_backtest_csv_columns() - set(fieldnames))
    strategy_rows = [row for row in rows if clean(row.get("strategy")) == strategy]
    source_addresses = {
        clean(row.get("address")) for row in strategy_rows if clean(row.get("address"))
    }
    csv_summary = {
        "checked": True,
        "path": str(csv_path),
        "status": "checked",
        "row_count": len(rows),
        "strategy": strategy,
        "source_count": len(source_addresses) if source_addresses else len(strategy_rows),
        "strategy_rows": len(strategy_rows),
        "total_start_equity_usd": decimal_str(sum_decimal(strategy_rows, "initial_equity_usd")),
        "ending_equity_usd": decimal_str(sum_decimal(strategy_rows, "ending_equity_usd")),
        "net_pnl_usd": decimal_str(sum_decimal(strategy_rows, "net_pnl_usd")),
        "copied_fills": sum_int(strategy_rows, "copied_fills"),
        "skipped_min_notional_fills": sum_int(strategy_rows, "skipped_min_notional_fills"),
        "capped_fills": sum_int(strategy_rows, "capped_fills"),
        "zeroed_slots": sum(
            1
            for row in strategy_rows
            if bool_optional(row.get("liquidated_or_zero_equity")) is True
        ),
        "truncated_sources": sum(
            1 for row in strategy_rows if bool_optional(row.get("truncated")) is True
        ),
        "missing_columns": missing_columns,
    }
    mismatches = companion_csv_mismatches(
        csv_summary=csv_summary,
        backtest_summary=backtest_summary,
    )
    checks = [
        check("backtest_companion_csv_present", True, f"path={csv_path}"),
        check(
            "backtest_companion_csv_columns",
            not missing_columns,
            "all required columns present"
            if not missing_columns
            else f"missing={','.join(missing_columns)}",
        ),
        check(
            "backtest_companion_csv_strategy_present",
            bool(strategy_rows),
            f"strategy={strategy} rows={len(strategy_rows)}",
        ),
        check(
            "backtest_companion_csv_matches_json",
            not mismatches,
            "matches JSON aggregate for selected strategy"
            if not mismatches
            else "; ".join(mismatches),
        ),
    ]
    return csv_summary, checks


def required_backtest_csv_columns() -> set[str]:
    return {
        "address",
        "capped_fills",
        "copied_fills",
        "ending_equity_usd",
        "initial_equity_usd",
        "liquidated_or_zero_equity",
        "net_pnl_usd",
        "skipped_min_notional_fills",
        "strategy",
        "truncated",
    }


def read_csv_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def companion_csv_mismatches(
    *,
    csv_summary: dict[str, Any],
    backtest_summary: dict[str, Any],
) -> list[str]:
    mismatches: list[str] = []
    decimal_keys = [
        "total_start_equity_usd",
        "ending_equity_usd",
        "net_pnl_usd",
    ]
    integer_keys = [
        "source_count",
        "strategy_rows",
        "copied_fills",
        "skipped_min_notional_fills",
        "capped_fills",
        "zeroed_slots",
        "truncated_sources",
    ]
    for key in decimal_keys:
        actual = decimal_optional(csv_summary.get(key))
        expected = decimal_optional(backtest_summary.get(key))
        if actual != expected:
            mismatches.append(f"{key}={csv_summary.get(key)} expected={backtest_summary.get(key)}")
    for key in integer_keys:
        actual_int = int_optional(csv_summary.get(key))
        expected_int = int_optional(backtest_summary.get(key))
        if actual_int != expected_int:
            mismatches.append(f"{key}={csv_summary.get(key)} expected={backtest_summary.get(key)}")
    return mismatches


def corrected_backtest_check(summary: dict[str, Any], *, strategy: str) -> dict[str, Any]:
    if strategy != DEFAULT_STRATEGY:
        return check(
            "backtest_corrected_180d_aggregate",
            True,
            f"skipped for non-default strategy={strategy}",
        )
    mismatches: list[str] = []
    for key, expected in CORRECTED_DEFAULT_BACKTEST.items():
        actual = summary.get(key)
        if isinstance(expected, Decimal):
            actual_decimal = decimal_optional(actual)
            if actual_decimal != expected:
                mismatches.append(f"{key}={actual} expected={expected}")
        elif actual != expected:
            mismatches.append(f"{key}={actual} expected={expected}")
    return check(
        "backtest_corrected_180d_aggregate",
        not mismatches,
        "matches corrected copy10_180d aggregate" if not mismatches else "; ".join(mismatches),
    )


def summarize_slot_state(slot_state: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    slots = list_items(slot_state.get("slots"))
    input_blockers = list_text(slot_state.get("input_blockers"))
    input_warnings = list_text(slot_state.get("input_warnings"))
    recovery_completion = dict_value(slot_state.get("recovery_completion"))
    recovery_note = clean(recovery_completion.get("notes"))
    synthetic_notes = synthetic_recovery_note_markers(recovery_note)
    recovery_gate_decisions: Counter[str] = Counter()
    slot_statuses: Counter[str] = Counter()
    incomplete_slots: list[dict[str, Any]] = []
    actionable_slots = 0
    matched_subaccount_slots = 0
    matched_slot_label_slots = 0
    verified_follower_snapshot_slots = 0
    unverified_follower_snapshot_slots: list[dict[str, Any]] = []
    source_recovery_windows: list[dict[str, Any]] = []

    for slot in slots:
        slot_name = clean(slot.get("slot")) or "unnamed"
        slot_statuses[clean(slot.get("status")) or "unknown"] += 1
        if slot.get("follower_matches_subaccount") is True:
            matched_subaccount_slots += 1
        if slot.get("follower_matches_slot") is True:
            matched_slot_label_slots += 1
        if slot.get("follower_snapshot_verified") is True:
            verified_follower_snapshot_slots += 1
        else:
            verification = dict_value(slot.get("follower_snapshot_verification"))
            unverified_follower_snapshot_slots.append(
                {
                    "slot": slot_name,
                    "reason": clean(verification.get("reason"))
                    or "follower snapshot address verification is missing",
                    "observed_address": clean(verification.get("observed_address")) or None,
                    "expected_subaccount": clean(verification.get("expected_subaccount")) or None,
                }
            )
        recovery_gate = dict_value(slot.get("recovery_gate"))
        decision = clean(recovery_gate.get("decision")) or "missing"
        recovery_gate_decisions[decision] += 1
        if recovery_gate.get("repair_intents_actionable") is True:
            actionable_slots += 1
        if recovery_gate.get("complete") is not True:
            incomplete_slots.append(
                {
                    "slot": slot_name,
                    "decision": decision,
                    "missing_requirements": list_text(recovery_gate.get("missing_requirements")),
                    "blocker_reasons": list_text(recovery_gate.get("blocker_reasons")),
                }
            )
        source_windows = source_recovery_window_summary(slot)
        if source_windows:
            source_recovery_windows.append({"slot": slot_name, **source_windows})

    read_only = slot_state.get("read_only") is True
    exchange_touched = slot_state.get("exchange_touched") is True
    valid = slot_state.get("valid") is True
    report_version = int_optional(slot_state.get("slot_state_report_version"))
    version_supported = report_version == SUPPORTED_SLOT_STATE_REPORT_VERSION
    recovery_gates_complete = bool(slots) and not incomplete_slots
    all_repair_intents_actionable = bool(slots) and actionable_slots == len(slots)

    summary = {
        "slot_state_report_version": report_version,
        "valid": valid,
        "read_only": read_only,
        "exchange_touched": exchange_touched,
        "slot_count": len(slots),
        "input_blockers": input_blockers,
        "input_warnings": input_warnings,
        "slot_statuses": counter_dict(slot_statuses),
        "recovery_gate_decisions": counter_dict(recovery_gate_decisions),
        "incomplete_recovery_slots": incomplete_slots,
        "repair_intents_actionable_slots": actionable_slots,
        "follower_matches_subaccount_slots": matched_subaccount_slots,
        "follower_matches_slot_label_slots": matched_slot_label_slots,
        "follower_snapshot_verified_slots": verified_follower_snapshot_slots,
        "unverified_follower_snapshot_slots": unverified_follower_snapshot_slots,
        "recovery_completion": recovery_completion,
        "source_recovery_windows": source_recovery_windows,
        "synthetic_recovery_note_markers": synthetic_notes,
        "execution_scope": (
            "read-only evidence gate; passing this report permits only consideration of passive "
            "testnet canary, not live exchange approval"
        ),
    }
    checks = [
        check(
            "slot_state_version_supported",
            version_supported,
            f"version={report_version} expected={SUPPORTED_SLOT_STATE_REPORT_VERSION}",
        ),
        check("slot_state_valid", valid, f"valid={valid}"),
        check(
            "slot_state_read_only",
            read_only and not exchange_touched,
            f"read_only={read_only} exchange_touched={exchange_touched}",
        ),
        check(
            "slot_state_no_input_blockers",
            not input_blockers,
            f"input_blockers={len(input_blockers)}",
        ),
        check("slot_state_slots_present", bool(slots), f"slots={len(slots)}"),
        check(
            "slot_state_follower_subaccount_matches",
            bool(slots) and matched_subaccount_slots == len(slots),
            f"matched={matched_subaccount_slots}/{len(slots)}",
        ),
        check(
            "slot_state_follower_snapshot_verified",
            bool(slots) and verified_follower_snapshot_slots == len(slots),
            follower_snapshot_verification_detail(unverified_follower_snapshot_slots),
        ),
        check(
            "slot_state_recovery_gates_complete",
            recovery_gates_complete,
            recovery_gate_detail(incomplete_slots),
        ),
        check(
            "slot_state_repair_intents_actionable",
            all_repair_intents_actionable,
            f"actionable_slots={actionable_slots}/{len(slots)}",
        ),
        check(
            "slot_state_follower_snapshot_not_synthetic",
            not synthetic_notes,
            "no synthetic/analysis-only note markers"
            if not synthetic_notes
            else f"markers={','.join(synthetic_notes)}",
        ),
    ]
    return summary, checks


def source_recovery_window_summary(slot: dict[str, Any]) -> dict[str, Any]:
    source_state = dict_value(slot.get("source_state"))
    source_summary = dict_value(source_state.get("summary"))
    recovery = dict_value(source_summary.get("recovery"))
    windows = dict_value(recovery.get("windows"))
    if not windows:
        return {}
    return {
        "count": int_optional(windows.get("count")) or 0,
        "complete": int_optional(windows.get("complete")) or 0,
        "incomplete": int_optional(windows.get("incomplete")) or 0,
        "max_gap_ms": int_optional(windows.get("max_gap_ms")),
        "missing_requirements": dict_value(windows.get("missing_requirements")),
        "sample": list_items(windows.get("sample")),
    }


def summarize_recovery_evidence(
    recovery_evidence: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if recovery_evidence is None:
        return {
            "attached": False,
            "status": "not_attached",
            "recovery_evidence_ready": False,
            "blockers": [],
            "window_count": 0,
            "incomplete_windows": [],
        }, [
            check(
                "recovery_evidence_attached",
                False,
                "attach build_recovery_evidence_report.py output before considering testnet canary",
            )
        ]
    read_only = recovery_evidence.get("read_only") is True
    exchange_touched = recovery_evidence.get("exchange_touched") is True
    report_version = int_optional(recovery_evidence.get("recovery_evidence_report_version"))
    version_supported = report_version == SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION
    ready = recovery_evidence.get("recovery_evidence_ready") is True
    windows = list_items(recovery_evidence.get("windows"))
    incomplete_windows = [
        {
            "sequence": int_optional(window.get("sequence")) or 0,
            "missing_requirements": list_text(window.get("missing_requirements")),
        }
        for window in windows
        if window.get("complete") is not True
    ]
    blockers = list_text(recovery_evidence.get("blockers"))
    summary = {
        "attached": True,
        "recovery_evidence_report_version": report_version,
        "status": clean(recovery_evidence.get("status")),
        "read_only": read_only,
        "exchange_touched": exchange_touched,
        "recovery_evidence_ready": ready,
        "blockers": blockers,
        "window_count": len(windows),
        "incomplete_windows": incomplete_windows,
    }
    checks = [
        check(
            "recovery_evidence_version_supported",
            version_supported,
            f"version={report_version} expected={SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION}",
        ),
        check(
            "recovery_evidence_read_only",
            read_only and not exchange_touched,
            f"read_only={read_only} exchange_touched={exchange_touched}",
        ),
        check(
            "recovery_evidence_ready",
            ready,
            "ready" if ready else recovery_evidence_detail(blockers, incomplete_windows),
        ),
    ]
    return summary, checks


def next_actions(
    *,
    shadow_replay_ready: bool,
    slot_summary: dict[str, Any],
    recovery_summary: dict[str, Any],
) -> list[str]:
    if shadow_replay_ready:
        return [
            "Run passive testnet canary only with explicit testnet credentials and verified subaccount.",
            "After passive canary, run active smoke, settlement, readiness, metrics, and dashboard checks.",
            "Keep mainnet disabled unless the operator explicitly starts a separate mainnet phase.",
        ]
    actions = [
        "Collect a verified read-only follower subaccount snapshot with normalize_follower_snapshot.py.",
        "Prove source REST backfill and reconcile completion after replay reconnect gaps.",
        "Rebuild drift and slot-state reports until every recovery_gate is complete.",
    ]
    if recovery_summary.get("attached") is not True:
        actions.insert(
            0,
            "Attach build_recovery_evidence_report.py output to the evidence readiness report.",
        )
    elif recovery_summary.get("recovery_evidence_ready") is not True:
        actions.insert(0, "Resolve blockers in the attached recovery evidence report.")
    incomplete = list_items(slot_summary.get("incomplete_recovery_slots"))
    if incomplete:
        missing = sorted(
            {item for slot in incomplete for item in list_text(slot.get("missing_requirements"))}
        )
        if missing:
            actions.insert(0, "Resolve missing recovery requirements: " + ", ".join(missing))
    return actions


def recovery_evidence_detail(
    blockers: list[str],
    incomplete_windows: list[dict[str, Any]],
) -> str:
    if blockers:
        return "; ".join(blockers[:3])
    if incomplete_windows:
        return "; ".join(
            f"window {window['sequence']}: {', '.join(window['missing_requirements'])}"
            for window in incomplete_windows[:3]
        )
    return "recovery evidence report is not ready"


def recovery_gate_detail(incomplete_slots: list[dict[str, Any]]) -> str:
    if not incomplete_slots:
        return "all slot recovery gates complete"
    details = []
    for slot in incomplete_slots[:5]:
        missing = ", ".join(list_text(slot.get("missing_requirements"))) or "unknown"
        details.append(f"{slot.get('slot')}: {missing}")
    if len(incomplete_slots) > 5:
        details.append(f"+{len(incomplete_slots) - 5} more")
    return "; ".join(details)


def follower_snapshot_verification_detail(unverified_slots: list[dict[str, Any]]) -> str:
    if not unverified_slots:
        return "all follower snapshots include observed-address verification"
    details = []
    for slot in unverified_slots[:5]:
        details.append(f"{slot.get('slot')}: {slot.get('reason') or 'unverified'}")
    if len(unverified_slots) > 5:
        details.append(f"+{len(unverified_slots) - 5} more")
    return "; ".join(details)


def synthetic_recovery_note_markers(note: str) -> list[str]:
    lowered = note.lower()
    return [marker for marker in SYNTHETIC_NOTE_MARKERS if marker in lowered]


def check(name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


def read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise EvidenceReadinessInputError(f"{label} does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise EvidenceReadinessInputError(f"{label} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise EvidenceReadinessInputError(f"{label} must be a JSON object: {path}")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_from_paths(
    *,
    backtest_path: Path,
    slot_state_report_path: Path,
    recovery_evidence_report_path: Path | None = None,
    out: Path | None = None,
    strategy: str = DEFAULT_STRATEGY,
) -> dict[str, Any]:
    backtest = read_json_object(backtest_path, label="backtest artifact")
    slot_state = read_json_object(slot_state_report_path, label="slot-state report")
    recovery_evidence = (
        read_json_object(recovery_evidence_report_path, label="recovery evidence report")
        if recovery_evidence_report_path is not None
        else None
    )
    report = build_evidence_readiness_report(
        backtest=backtest,
        slot_state=slot_state,
        recovery_evidence=recovery_evidence,
        strategy=strategy,
        backtest_path=backtest_path,
        slot_state_path=slot_state_report_path,
        recovery_evidence_path=recovery_evidence_report_path,
    )
    if out is not None:
        write_json(out, report)
    return report


def list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def list_text(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item).strip()]


def dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def clean(value: Any) -> str:
    return str(value or "").strip()


def path_str(path: Path | None) -> str:
    return "" if path is None else str(path)


def decimal_optional(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return format(value, "f")


def sum_decimal(rows: list[dict[str, Any]], key: str) -> Decimal:
    total = Decimal("0")
    for row in rows:
        value = decimal_optional(row.get(key))
        if value is not None:
            total += value
    return total


def int_optional(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def bool_optional(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    lowered = clean(value).lower()
    if lowered in {"1", "true", "yes", "y"}:
        return True
    if lowered in {"0", "false", "no", "n"}:
        return False
    return None


def sum_int(rows: list[dict[str, Any]], key: str) -> int:
    total = 0
    for row in rows:
        value = int_optional(row.get(key))
        if value is not None:
            total += value
    return total


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only evidence readiness report from rough backtest and replay/shadow "
            "slot-state artifacts."
        )
    )
    parser.add_argument("--backtest-json", type=Path, required=True)
    parser.add_argument("--slot-state-report", type=Path, required=True)
    parser.add_argument("--recovery-evidence-report", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--strategy", default=DEFAULT_STRATEGY)
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit nonzero when shadow/replay evidence is not ready for a passive testnet canary.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        report = build_from_paths(
            backtest_path=args.backtest_json,
            slot_state_report_path=args.slot_state_report,
            recovery_evidence_report_path=args.recovery_evidence_report,
            out=args.out,
            strategy=args.strategy,
        )
    except EvidenceReadinessInputError as exc:
        print(f"evidence readiness input error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "shadow_replay_ready": report["shadow_replay_ready"],
                "testnet_canary_candidate": report["testnet_canary_candidate"],
                "blockers": report["blockers"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    if args.fail_on_blocked and not report["shadow_replay_ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
