from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


MAX_REPORTS = 50
MAX_REPORT_BYTES = 5_000_000
SUPPORTED_POST_RUN_REPORT_VERSION = 4
SUPPORTED_SLOT_SUPERVISOR_VERSION = 4
AGGREGATE_FIELDS = (
    "ambiguous_reports",
    "copied_intents",
    "open_orders",
    "partial_reports",
    "pending_intents",
    "rejected_reports",
    "skipped_intents",
    "terminal_order_intents",
    "unfinished_source_reactions",
    "unexplained_drift",
)
_ADDRESS_PATTERN = re.compile(r"0x[a-fA-F0-9]{40}")


class RunHistoryService:
    """Read bounded, redacted summaries from supervisor post-run reports."""

    def __init__(self, root: Path | str = Path("data/runs"), *, limit: int = 20):
        self.root = Path(root)
        self.limit = min(max(int(limit), 1), MAX_REPORTS)

    def snapshot(self) -> dict[str, Any]:
        if not self.root.exists():
            return self._payload([], [], status="empty")
        dated_candidates: list[tuple[int, Path]] = []
        errors: list[dict[str, str]] = []
        for path in self.root.glob("*/post_run_report.json"):
            try:
                dated_candidates.append((path.stat().st_mtime_ns, path))
            except OSError as exc:
                errors.append({"run": _redacted_run_name(path), "error": _safe_error(exc)})
        candidates = [
            path
            for _, path in sorted(dated_candidates, key=lambda item: item[0], reverse=True)[
                : self.limit
            ]
        ]
        rows: list[dict[str, Any]] = []
        for path in candidates:
            try:
                rows.append(self._summarize(path))
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append({"run": _redacted_run_name(path), "error": _safe_error(exc)})
        issue_count = sum(len(row.get("quality_issues") or []) for row in rows)
        if errors and not rows:
            status = "error"
        elif not rows:
            status = "empty"
        elif errors:
            status = "partial"
        elif issue_count:
            status = "inconsistent"
        elif any(row["ready_for_next_phase"] is not True for row in rows):
            status = "blocked"
        else:
            status = "ready"
        return self._payload(rows, errors, status=status)

    def _summarize(self, path: Path) -> dict[str, Any]:
        size = path.stat().st_size
        if size > MAX_REPORT_BYTES:
            raise ValueError(f"report exceeds {MAX_REPORT_BYTES} byte safety limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("report root must be an object")
        raw_slots = payload.get("slots")
        slots = raw_slots if isinstance(raw_slots, list) else []
        raw_excluded_slots = payload.get("excluded_slots")
        excluded_slots = raw_excluded_slots if isinstance(raw_excluded_slots, list) else []
        retired_slots = [
            slot
            for slot in excluded_slots
            if isinstance(slot, dict)
            and slot.get("operational_status") == "retired_quarantined"
            and slot.get("execution_excluded") is True
        ]
        declared = _integer_map(payload.get("aggregate"))
        calculated = _aggregate_slots(slots)
        post_run_report_version = _strict_integer(payload.get("post_run_report_version"))
        slot_supervisor_version = _strict_integer(payload.get("slot_supervisor_version"))
        quality_issues = [
            f"aggregate.{field} declared={declared.get(field, 0)} calculated={calculated[field]}"
            for field in AGGREGATE_FIELDS
            if declared.get(field, 0) != calculated[field]
        ]
        if post_run_report_version != SUPPORTED_POST_RUN_REPORT_VERSION:
            quality_issues.append(
                "unsupported post-run report version: "
                f"{post_run_report_version}, expected {SUPPORTED_POST_RUN_REPORT_VERSION}"
            )
        if slot_supervisor_version != SUPPORTED_SLOT_SUPERVISOR_VERSION:
            quality_issues.append(
                "unsupported slot supervisor version: "
                f"{slot_supervisor_version}, expected {SUPPORTED_SLOT_SUPERVISOR_VERSION}"
            )
        declared_slot_count = _integer(payload.get("slot_count"), default=len(slots))
        if declared_slot_count != len(slots):
            quality_issues.append(
                f"slot_count declared={declared_slot_count} calculated={len(slots)}"
            )
        declared_excluded_count = _integer(
            payload.get("excluded_slot_count"), default=len(retired_slots)
        )
        if declared_excluded_count != len(retired_slots):
            quality_issues.append(
                "excluded_slot_count "
                f"declared={declared_excluded_count} calculated={len(retired_slots)}"
            )
        if len(retired_slots) != len(excluded_slots):
            quality_issues.append(
                "excluded_slots contains a record that is not retired_quarantined"
            )
        declared_fleet_count = _integer(
            payload.get("fleet_slot_count"), default=len(slots) + len(retired_slots)
        )
        if declared_fleet_count != len(slots) + len(retired_slots):
            quality_issues.append(
                "fleet_slot_count "
                f"declared={declared_fleet_count} calculated={len(slots) + len(retired_slots)}"
            )
        blockers = payload.get("blockers")
        blocker_count = len(blockers) if isinstance(blockers, list) else 0
        ready = payload.get("ready_for_next_phase") is True
        status = str(payload.get("status") or "unknown")
        if ready and status != "ready_for_next_phase":
            quality_issues.append("ready verdict does not match status")
        if ready and blocker_count:
            quality_issues.append("ready verdict contains blockers")
        if ready:
            for field in (
                "ambiguous_reports",
                "open_orders",
                "partial_reports",
                "pending_intents",
                "unfinished_source_reactions",
                "unexplained_drift",
            ):
                if calculated[field]:
                    quality_issues.append(f"ready verdict has {field}={calculated[field]}")
            if str(payload.get("mode") or "unknown") == "testnet":
                unchecked_slots = [
                    str(slot.get("slot") or "unknown")
                    for slot in slots
                    if not isinstance(slot.get("position_drift"), dict)
                    or slot["position_drift"].get("checked") is not True
                ]
                if unchecked_slots:
                    quality_issues.append(
                        f"ready testnet verdict lacks checked follower drift for {unchecked_slots}"
                    )
        calculated_lifecycle = (
            ready and not quality_issues and calculated["terminal_order_intents"] > 0
        )
        if payload.get("signed_order_lifecycle_verified") is True and not calculated_lifecycle:
            quality_issues.append("signed lifecycle claim lacks terminal follower-order evidence")
        for field, value in calculated.items():
            if value < 0:
                quality_issues.append(f"aggregate.{field} cannot be negative")
        return {
            "run": _redacted_run_name(path),
            "status": status,
            "post_run_report_version": post_run_report_version,
            "slot_supervisor_version": slot_supervisor_version,
            "ready_for_next_phase": ready,
            "signed_order_lifecycle_verified": calculated_lifecycle,
            "mode": str(payload.get("mode") or "unknown"),
            "source_network": str(payload.get("source_network") or "unknown"),
            "slot_count": len(slots),
            "retired_slot_count": len(retired_slots),
            "fleet_slot_count": len(slots) + len(retired_slots),
            "blocker_count": blocker_count,
            "aggregate": calculated,
            "quality_status": "valid" if not quality_issues else "inconsistent",
            "quality_issues": quality_issues,
            "updated_ms": path.stat().st_mtime_ns // 1_000_000,
        }

    @staticmethod
    def _payload(
        rows: list[dict[str, Any]],
        errors: list[dict[str, str]],
        *,
        status: str,
    ) -> dict[str, Any]:
        return {
            "status": status,
            "report_count": len(rows),
            "ready_count": sum(
                row["ready_for_next_phase"] is True and row.get("quality_status") == "valid"
                for row in rows
            ),
            "claimed_ready_count": sum(row["ready_for_next_phase"] is True for row in rows),
            "signed_lifecycle_count": sum(
                row.get("signed_order_lifecycle_verified") is True for row in rows
            ),
            "blocked_count": sum(row["ready_for_next_phase"] is not True for row in rows),
            "inconsistent_count": sum(row.get("quality_status") != "valid" for row in rows),
            "quality_issue_count": sum(len(row.get("quality_issues") or []) for row in rows),
            "rows": rows,
            "errors": errors,
        }


def _aggregate_slots(slots: list[Any]) -> dict[str, int]:
    totals = {field: 0 for field in AGGREGATE_FIELDS}
    for raw_slot in slots:
        if not isinstance(raw_slot, dict):
            continue
        intents_raw = raw_slot.get("intents")
        reports_raw = raw_slot.get("execution_reports")
        drift_raw = raw_slot.get("position_drift")
        intents: dict[Any, Any] = intents_raw if isinstance(intents_raw, dict) else {}
        reports: dict[Any, Any] = reports_raw if isinstance(reports_raw, dict) else {}
        drift: dict[Any, Any] = drift_raw if isinstance(drift_raw, dict) else {}
        totals["copied_intents"] += _integer(intents.get("copied"))
        totals["skipped_intents"] += _integer(intents.get("skipped"))
        totals["ambiguous_reports"] += _integer(reports.get("ambiguous"))
        totals["partial_reports"] += _integer(reports.get("partial"))
        totals["rejected_reports"] += _integer(reports.get("rejected"))
        totals["open_orders"] += _integer(raw_slot.get("open_orders"))
        totals["pending_intents"] += _integer(raw_slot.get("pending_intents"))
        totals["unfinished_source_reactions"] += _integer(
            raw_slot.get("unfinished_source_reactions")
        )
        totals["unexplained_drift"] += _integer(drift.get("unexplained"))
        totals["terminal_order_intents"] += _integer(reports.get("terminal_order_intents"))
    return totals


def _integer_map(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    return {str(key): _integer(item) for key, item in value.items()}


def _integer(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _strict_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _redacted_run_name(path: Path) -> str:
    return _ADDRESS_PATTERN.sub("0x-redacted", path.parent.name)[:120]


def _safe_error(exc: Exception) -> str:
    # Keep operator diagnostics useful without echoing absolute paths from OSError messages.
    if isinstance(exc, OSError):
        return exc.__class__.__name__
    return str(exc)[:200]
