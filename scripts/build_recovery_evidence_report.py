from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


RECOVERY_EVIDENCE_REPORT_VERSION = 2
RECOVERY_PROOF_TEMPLATE_VERSION = 1
SUPPORTED_FOLLOWER_SNAPSHOT_NORMALIZER_VERSION = 2
WINDOW_REQUIREMENTS = (
    "explicit_rest_backfill_complete",
    "explicit_reconcile_complete",
    "live_stream_hints_allowed_after_window",
)


class RecoveryEvidenceInputError(RuntimeError):
    """Raised when recovery evidence inputs cannot be loaded or interpreted."""


def build_recovery_evidence_report(
    *,
    source_state_report: dict[str, Any],
    follower_snapshot: dict[str, Any] | None = None,
    proof: dict[str, Any] | None = None,
    source_state_report_path: Path | None = None,
    follower_snapshot_path: Path | None = None,
    proof_path: Path | None = None,
) -> dict[str, Any]:
    source_summary = summarize_source_state(source_state_report)
    follower_summary = summarize_follower_snapshot(follower_snapshot)
    source_sequences = {
        sequence
        for window in source_summary["windows"]
        if (sequence := int_optional(window.get("sequence"))) is not None
    }
    proof_summary = summarize_proof(
        proof,
        source_address=source_summary["source_address"],
        source_sequences=source_sequences,
    )
    window_reports = [
        summarize_window(
            window,
            proof_windows=proof_summary["windows_by_sequence"],
            follower_ready=follower_summary["recovery_complete"],
        )
        for window in source_summary["windows"]
    ]

    checks = [
        check(
            "source_state_read_only",
            source_state_report.get("read_only") is True
            and source_state_report.get("exchange_touched") is False,
            "source state report must be read_only=true and exchange_touched=false",
        ),
        check(
            "source_state_source_address_valid",
            source_summary["source_address_valid"] is True,
            source_summary["source_address_reason"],
        ),
        check(
            "source_state_source_address_unambiguous",
            source_summary["source_address_unambiguous"] is True,
            source_summary["source_address_reason"],
        ),
        check(
            "source_recovery_windows_present",
            bool(window_reports),
            f"windows={len(window_reports)}",
        ),
        check(
            "proof_attached",
            proof is not None,
            "proof JSON attached" if proof is not None else "no proof JSON attached",
        ),
        *proof_summary["checks"],
        *follower_summary["checks"],
    ]
    for window in window_reports:
        checks.append(
            check(
                f"window_{window['sequence']}_complete",
                window["complete"] is True,
                window["detail"],
            )
        )

    blockers = [f"{item['name']}: {item['detail']}" for item in checks if not item["passed"]]
    ready = not blockers
    return {
        "recovery_evidence_report_version": RECOVERY_EVIDENCE_REPORT_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "input_paths": {
            "source_state_report": path_str(source_state_report_path),
            "follower_snapshot": path_str(follower_snapshot_path),
            "proof": path_str(proof_path),
        },
        "status": "recovery_evidence_ready" if ready else "blocked",
        "recovery_evidence_ready": ready,
        "checks": checks,
        "blockers": blockers,
        "source_state": {key: value for key, value in source_summary.items() if key != "windows"},
        "follower_snapshot": follower_summary["summary"],
        "proof": proof_summary["summary"],
        "windows": window_reports,
        "next_required_actions": next_required_actions(
            ready=ready,
            follower_summary=follower_summary["summary"],
            proof_attached=proof is not None,
            window_reports=window_reports,
        ),
    }


def build_recovery_proof_template(source_state_report: dict[str, Any]) -> dict[str, Any]:
    source_summary = summarize_source_state(source_state_report)
    windows = []
    for window in source_summary["windows"]:
        windows.append(
            {
                "sequence": int_optional(window.get("sequence")) or 0,
                "reconnect_row_id": clean(window.get("reconnect_row_id")),
                "degraded_ts_ms": int_optional(window.get("degraded_ts_ms")),
                "reconnected_ts_ms": int_optional(window.get("reconnected_ts_ms")),
                "gap_ms": int_optional(window.get("gap_ms")),
                "post_reconnect_state_refreshes": int_optional(
                    window.get("post_reconnect_state_refreshes")
                )
                or 0,
                "post_reconnect_rest_snapshots": int_optional(
                    window.get("post_reconnect_rest_snapshots")
                )
                or 0,
                "post_reconnect_account_context_rows": int_optional(
                    window.get("post_reconnect_account_context_rows")
                )
                or 0,
                "post_reconnect_source_actions": int_optional(
                    window.get("post_reconnect_source_actions")
                )
                or 0,
                "explicit_rest_backfill_complete": False,
                "explicit_reconcile_complete": False,
                "live_stream_hints_allowed_after_window": False,
                "evidence_refs": [],
                "operator_notes": "",
            }
        )
    return {
        "recovery_proof_template_version": RECOVERY_PROOF_TEMPLATE_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "source_address": source_summary["source_address"],
        "slot": source_summary["slot"],
        "notes": (
            "Fill evidence_refs and set completion flags only after source backfill, "
            "source/follower reconciliation, and post-window live-stream trust are proven."
        ),
        "windows": windows,
    }


def summarize_source_state(source_state_report: dict[str, Any]) -> dict[str, Any]:
    counts = dict_value(source_state_report.get("counts"))
    recovery = dict_value(source_state_report.get("recovery"))
    slot_metadata = dict_value(source_state_report.get("slot"))
    explicit_source = clean(source_state_report.get("source_address")).lower()
    slot_source = clean(slot_metadata.get("source_address")).lower()
    counter_sources = sorted(
        key.lower() for key in counter_keys(counts.get("by_address")) if valid_address(key.lower())
    )
    candidates = [
        candidate
        for candidate in [explicit_source, slot_source, *counter_sources]
        if valid_address(candidate)
    ]
    unique_candidates = sorted(set(candidates))
    source_address = unique_candidates[0] if len(unique_candidates) == 1 else ""
    source_address_valid = valid_address(source_address)
    source_address_unambiguous = len(unique_candidates) == 1
    if not candidates:
        source_address_reason = "no valid source address found in source_state_report"
    elif not source_address_unambiguous:
        source_address_reason = "ambiguous source addresses: " + ",".join(unique_candidates)
    else:
        source_address_reason = "source address valid and unambiguous"
    return {
        "source_address": source_address,
        "source_address_valid": source_address_valid,
        "source_address_unambiguous": source_address_unambiguous,
        "source_address_reason": source_address_reason,
        "source_address_candidates": {
            "source_state_report": explicit_source if valid_address(explicit_source) else "",
            "slot_metadata": slot_source if valid_address(slot_source) else "",
            "counts_by_address": counter_sources,
        },
        "slot": clean(slot_metadata.get("slot") or source_state_report.get("slot")),
        "stream_state": clean(recovery.get("stream_state")),
        "pending_rest_backfill": recovery.get("pending_rest_backfill") is True,
        "pending_reconcile": recovery.get("pending_reconcile") is True,
        "live_stream_hints_allowed": recovery.get("live_stream_hints_allowed") is True,
        "degraded_events": int_optional(recovery.get("degraded_events")) or 0,
        "reconnect_recovered_events": int_optional(recovery.get("reconnect_recovered_events")) or 0,
        "state_refreshes_after_reconnect": int_optional(
            recovery.get("state_refreshes_after_reconnect")
        )
        or 0,
        "windows": list_items(recovery.get("windows")),
    }


def summarize_follower_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot is None:
        summary = {
            "attached": False,
            "read_only": False,
            "exchange_touched": False,
            "follower_subaccount": "",
            "follower_subaccount_verified": False,
            "snapshot_normalizer_version": None,
            "snapshot_normalizer_supported": False,
            "request_status_valid": False,
            "snapshot_shape_valid": False,
            "validation_warnings": [],
            "address_verification": {
                "verified": False,
                "expected_follower_subaccount": "",
                "observed_address": "",
                "observed_address_source": "",
                "reason": "no follower snapshot attached",
            },
            "follower_refresh_complete": False,
            "source_backfill_complete": False,
            "reconcile_complete": False,
            "recovery_complete": False,
            "notes": "",
        }
        return {
            "summary": summary,
            "recovery_complete": False,
            "checks": [
                check("follower_snapshot_attached", False, "no follower snapshot attached"),
            ],
        }

    recovery = dict_value(snapshot.get("recovery"))
    raw_normalizer_version = snapshot.get("snapshot_normalizer_version")
    normalizer_version = (
        raw_normalizer_version
        if isinstance(raw_normalizer_version, int) and not isinstance(raw_normalizer_version, bool)
        else None
    )
    normalizer_supported = normalizer_version == SUPPORTED_FOLLOWER_SNAPSHOT_NORMALIZER_VERSION
    request_status = dict_value(snapshot.get("request_status"))
    request_status_valid = (
        request_status.get("clearinghouseState_ok") is True
        and request_status.get("openOrders_ok") is True
    )
    positions = snapshot.get("positions")
    open_orders = snapshot.get("open_orders")
    counts = dict_value(snapshot.get("counts"))
    snapshot_shape_valid = (
        isinstance(positions, list)
        and all(isinstance(item, dict) for item in positions)
        and isinstance(open_orders, list)
        and all(isinstance(item, dict) for item in open_orders)
        and counts.get("positions") == len(positions)
        and counts.get("open_orders") == len(open_orders)
    )
    validation_warnings = [
        warning
        for warning in list_text(snapshot.get("warnings"))
        if "validation failed" in warning.lower()
        or "follower refresh incomplete" in warning.lower()
    ]
    validation_warnings_clear = not validation_warnings
    read_only = snapshot.get("read_only") is True
    exchange_touched = snapshot.get("exchange_touched") is True
    follower_subaccount = clean(snapshot.get("follower_subaccount")).lower()
    address_verification = strict_address_verification(snapshot)
    follower_verified = address_verification["verified"]
    source_backfill_complete = recovery.get("source_backfill_complete") is True
    follower_refresh_complete = recovery.get("follower_refresh_complete") is True
    reconcile_complete = recovery.get("reconcile_complete") is True
    recovery_complete = (
        read_only
        and not exchange_touched
        and follower_verified
        and source_backfill_complete
        and follower_refresh_complete
        and reconcile_complete
        and normalizer_supported
        and request_status_valid
        and snapshot_shape_valid
        and validation_warnings_clear
    )
    summary = {
        "attached": True,
        "read_only": read_only,
        "exchange_touched": exchange_touched,
        "follower_subaccount": follower_subaccount,
        "follower_subaccount_verified": follower_verified,
        "snapshot_normalizer_version": normalizer_version,
        "snapshot_normalizer_supported": normalizer_supported,
        "request_status_valid": request_status_valid,
        "snapshot_shape_valid": snapshot_shape_valid,
        "validation_warnings": validation_warnings,
        "address_verification": address_verification,
        "follower_refresh_complete": follower_refresh_complete,
        "source_backfill_complete": source_backfill_complete,
        "reconcile_complete": reconcile_complete,
        "recovery_complete": recovery_complete,
        "notes": clean(recovery.get("notes")),
    }
    checks = [
        check("follower_snapshot_attached", True, "follower snapshot attached"),
        check(
            "follower_snapshot_read_only",
            read_only and not exchange_touched,
            f"read_only={read_only} exchange_touched={exchange_touched}",
        ),
        check(
            "follower_snapshot_address_verified",
            follower_verified,
            "observed address matches follower_subaccount"
            if follower_verified
            else address_verification["reason"],
        ),
        check(
            "follower_snapshot_normalizer_supported",
            normalizer_supported,
            f"version={normalizer_version} expected={SUPPORTED_FOLLOWER_SNAPSHOT_NORMALIZER_VERSION}",
        ),
        check(
            "follower_snapshot_requests_validated",
            request_status_valid,
            f"request_status={request_status}",
        ),
        check(
            "follower_snapshot_shape_valid",
            snapshot_shape_valid,
            "positions/open_orders are complete lists with matching counts"
            if snapshot_shape_valid
            else "positions/open_orders or counts are malformed",
        ),
        check(
            "follower_snapshot_validation_warnings_clear",
            validation_warnings_clear,
            "none" if validation_warnings_clear else "; ".join(validation_warnings),
        ),
        check(
            "follower_snapshot_recovery_complete",
            source_backfill_complete
            and follower_refresh_complete
            and reconcile_complete
            and normalizer_supported
            and request_status_valid
            and snapshot_shape_valid
            and validation_warnings_clear,
            follower_recovery_detail(
                source_backfill_complete=source_backfill_complete,
                follower_refresh_complete=follower_refresh_complete,
                reconcile_complete=reconcile_complete,
            ),
        ),
    ]
    return {"summary": summary, "recovery_complete": recovery_complete, "checks": checks}


def strict_address_verification(snapshot: dict[str, Any]) -> dict[str, Any]:
    follower_subaccount = clean(snapshot.get("follower_subaccount")).lower()
    claimed_verified = snapshot.get("follower_subaccount_verified") is True
    verification = dict_value(snapshot.get("address_verification"))
    verification_verified = verification.get("verified") is True
    expected = clean(
        verification.get("expected_follower_subaccount") or verification.get("expected_subaccount")
    ).lower()
    observed = clean(verification.get("observed_address")).lower()
    observed_source = clean(verification.get("observed_address_source"))
    checks = {
        "follower_subaccount_valid": valid_address(follower_subaccount),
        "follower_subaccount_verified_true": claimed_verified,
        "address_verification_attached": bool(verification),
        "address_verification_verified_true": verification_verified,
        "expected_matches_follower_subaccount": expected == follower_subaccount,
        "observed_address_valid": valid_address(observed),
        "observed_matches_follower_subaccount": observed == follower_subaccount,
        "observed_address_source_present": observed_source not in {"", "unknown"},
    }
    missing = [name for name, passed in checks.items() if not passed]
    verified = not missing
    if verified:
        reason = "raw snapshot observed address matches follower_subaccount"
    else:
        reason = "missing=" + ",".join(missing)
    return {
        "verified": verified,
        "claimed_verified": claimed_verified,
        "expected_follower_subaccount": expected,
        "observed_address": observed,
        "observed_address_source": "" if observed_source == "unknown" else observed_source,
        "checks": checks,
        "reason": reason,
    }


def summarize_proof(
    proof: dict[str, Any] | None,
    *,
    source_address: str,
    source_sequences: set[int],
) -> dict[str, Any]:
    if proof is None:
        return {
            "summary": {
                "attached": False,
                "read_only": False,
                "exchange_touched": False,
                "window_count": 0,
            },
            "windows_by_sequence": {},
            "checks": [],
        }
    windows = list_items(proof.get("windows"))
    windows_by_sequence: dict[int, dict[str, Any]] = {}
    duplicate_sequences: list[int] = []
    invalid_sequence_count = 0
    for window in windows:
        sequence = int_optional(window.get("sequence"))
        if sequence is None:
            invalid_sequence_count += 1
            continue
        if sequence in windows_by_sequence:
            duplicate_sequences.append(sequence)
            continue
        windows_by_sequence[sequence] = window
    unexpected_sequences = sorted(set(windows_by_sequence) - source_sequences)
    read_only = proof.get("read_only") is True
    exchange_touched = proof.get("exchange_touched") is True
    proof_version = int_optional(proof.get("recovery_proof_template_version"))
    version_supported = proof_version == RECOVERY_PROOF_TEMPLATE_VERSION
    proof_source = clean(proof.get("source_address")).lower()
    expected_source = clean(source_address).lower()
    source_matches = (
        bool(proof_source) and proof_source != "unknown" and proof_source == expected_source
    )
    checks = [
        check(
            "proof_version_supported",
            version_supported,
            f"version={proof_version} expected={RECOVERY_PROOF_TEMPLATE_VERSION}",
        ),
        check(
            "proof_read_only",
            read_only and not exchange_touched,
            f"read_only={read_only} exchange_touched={exchange_touched}",
        ),
        check(
            "proof_source_matches",
            source_matches,
            "source matches"
            if source_matches
            else f"proof source={proof_source or 'missing'} expected={expected_source or 'missing'}",
        ),
        check(
            "proof_windows_unique",
            not duplicate_sequences,
            "unique sequences"
            if not duplicate_sequences
            else "duplicates=" + ",".join(str(item) for item in duplicate_sequences),
        ),
        check(
            "proof_windows_have_sequence",
            invalid_sequence_count == 0,
            "all proof windows include a valid sequence"
            if invalid_sequence_count == 0
            else f"invalid_sequence_windows={invalid_sequence_count}",
        ),
        check(
            "proof_windows_match_source_sequences",
            not unexpected_sequences,
            "all proof windows match source reconnect windows"
            if not unexpected_sequences
            else "unexpected_sequences=" + ",".join(str(item) for item in unexpected_sequences),
        ),
    ]
    return {
        "summary": {
            "attached": True,
            "recovery_proof_template_version": proof_version,
            "read_only": read_only,
            "exchange_touched": exchange_touched,
            "source_address": proof_source,
            "window_count": len(windows),
            "invalid_sequence_windows": invalid_sequence_count,
            "unexpected_sequences": unexpected_sequences,
            "notes": clean(proof.get("notes")),
        },
        "windows_by_sequence": windows_by_sequence,
        "checks": checks,
    }


def summarize_window(
    window: dict[str, Any],
    *,
    proof_windows: dict[int, dict[str, Any]],
    follower_ready: bool,
) -> dict[str, Any]:
    sequence = int_optional(window.get("sequence")) or 0
    proof = proof_windows.get(sequence)
    blockers: list[str] = []
    if proof is None:
        blockers.append("proof window is missing")
        proof_requirements = {requirement: False for requirement in WINDOW_REQUIREMENTS}
        evidence_refs: list[str] = []
    else:
        proof_requirements = {
            requirement: proof.get(requirement) is True for requirement in WINDOW_REQUIREMENTS
        }
        evidence_refs = list_text(proof.get("evidence_refs"))
        if not evidence_refs:
            blockers.append("proof window must include evidence_refs")
        reconnect_row_id = clean(window.get("reconnect_row_id"))
        proof_reconnect_row_id = clean(proof.get("reconnect_row_id"))
        if reconnect_row_id and reconnect_row_id != proof_reconnect_row_id:
            blockers.append("proof reconnect_row_id does not match source window")

    if (int_optional(window.get("post_reconnect_state_refreshes")) or 0) <= 0:
        blockers.append("source state refresh after reconnect is missing")
    if (int_optional(window.get("post_reconnect_rest_snapshots")) or 0) <= 0:
        blockers.append("source REST snapshot after reconnect is missing")
    for requirement, passed in proof_requirements.items():
        if not passed:
            blockers.append(requirement)
    if not follower_ready:
        blockers.append("verified follower refresh/reconcile snapshot is missing")

    missing = sorted(set(blockers))
    return {
        "sequence": sequence,
        "degraded_ts_ms": int_optional(window.get("degraded_ts_ms")),
        "reconnected_ts_ms": int_optional(window.get("reconnected_ts_ms")),
        "gap_ms": int_optional(window.get("gap_ms")),
        "reconnect_row_id": clean(window.get("reconnect_row_id")),
        "post_reconnect_state_refreshes": int_optional(window.get("post_reconnect_state_refreshes"))
        or 0,
        "post_reconnect_rest_snapshots": int_optional(window.get("post_reconnect_rest_snapshots"))
        or 0,
        "post_reconnect_account_context_rows": int_optional(
            window.get("post_reconnect_account_context_rows")
        )
        or 0,
        "post_reconnect_source_actions": int_optional(window.get("post_reconnect_source_actions"))
        or 0,
        "proof_attached": proof is not None,
        "proof_requirements": proof_requirements,
        "evidence_refs": evidence_refs,
        "complete": not missing,
        "missing_requirements": missing,
        "detail": "complete" if not missing else ", ".join(missing),
    }


def next_required_actions(
    *,
    ready: bool,
    follower_summary: dict[str, Any],
    proof_attached: bool,
    window_reports: list[dict[str, Any]],
) -> list[str]:
    if ready:
        return [
            "Rebuild the slot-state report with recovery completion inputs.",
            "Rerun build_evidence_readiness_report.py before considering passive testnet canary.",
        ]
    actions = []
    if not proof_attached:
        actions.append(
            "Create proof JSON with --proof-template-out, then fill evidence_refs and completion flags only after evidence exists."
        )
    if not follower_summary.get("recovery_complete"):
        actions.append(
            "Collect a verified follower snapshot and mark source_backfill, follower_refresh, and reconcile completion only after evidence exists."
        )
    incomplete = [window for window in window_reports if window.get("complete") is not True]
    if incomplete:
        actions.append(
            "Resolve recovery window blockers: "
            + "; ".join(
                f"window {window['sequence']}: {', '.join(window['missing_requirements'])}"
                for window in incomplete[:3]
            )
        )
    return actions


def follower_recovery_detail(
    *,
    source_backfill_complete: bool,
    follower_refresh_complete: bool,
    reconcile_complete: bool,
) -> str:
    missing = []
    if not source_backfill_complete:
        missing.append("source_backfill_complete")
    if not follower_refresh_complete:
        missing.append("follower_refresh_complete")
    if not reconcile_complete:
        missing.append("reconcile_complete")
    return "complete" if not missing else "missing=" + ",".join(missing)


def check(name: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


def read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if not path.exists():
        raise RecoveryEvidenceInputError(f"{label} does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RecoveryEvidenceInputError(f"{label} is not valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise RecoveryEvidenceInputError(f"{label} must be a JSON object: {path}")
    return payload


def build_from_paths(
    *,
    source_state_report_path: Path,
    follower_snapshot_path: Path | None = None,
    proof_path: Path | None = None,
    proof_template_out: Path | None = None,
    out: Path | None = None,
) -> dict[str, Any]:
    source_state_report = read_json_object(source_state_report_path, label="source state report")
    follower_snapshot = (
        read_json_object(follower_snapshot_path, label="follower snapshot")
        if follower_snapshot_path is not None
        else None
    )
    proof = read_json_object(proof_path, label="proof JSON") if proof_path is not None else None
    report = build_recovery_evidence_report(
        source_state_report=source_state_report,
        follower_snapshot=follower_snapshot,
        proof=proof,
        source_state_report_path=source_state_report_path,
        follower_snapshot_path=follower_snapshot_path,
        proof_path=proof_path,
    )
    if out is not None:
        write_json(out, report)
    if proof_template_out is not None:
        write_json(proof_template_out, build_recovery_proof_template(source_state_report))
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


def counter_keys(value: Any) -> list[str]:
    if not isinstance(value, dict) or not value:
        return []
    return [str(key).strip() for key in value]


def clean(value: Any) -> str:
    return str(value or "").strip()


def int_optional(value: Any) -> int | None:
    if value in (None, "", "unknown"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def valid_address(value: str) -> bool:
    return (
        len(value) == 42
        and value.startswith("0x")
        and all(char in "0123456789abcdef" for char in value[2:])
    )


def path_str(path: Path | None) -> str:
    return "" if path is None else str(path)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp_path.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only recovery evidence report from replay source-state windows, "
            "a verified follower snapshot, and optional operator proof JSON."
        )
    )
    parser.add_argument("--source-state-report", type=Path, required=True)
    parser.add_argument("--follower-snapshot", type=Path)
    parser.add_argument("--proof-json", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--proof-template-out",
        type=Path,
        help="Write an all-false operator proof template derived from source recovery windows.",
    )
    parser.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit nonzero when recovery evidence is blocked.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = build_from_paths(
            source_state_report_path=args.source_state_report,
            follower_snapshot_path=args.follower_snapshot,
            proof_path=args.proof_json,
            proof_template_out=args.proof_template_out,
            out=args.out,
        )
    except RecoveryEvidenceInputError as exc:
        print(f"recovery evidence input error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "recovery_evidence_ready": report["recovery_evidence_ready"],
                "blockers": report["blockers"],
                "report": str(args.out),
                "proof_template": str(args.proof_template_out) if args.proof_template_out else "",
            },
            indent=2,
            sort_keys=True,
        )
    )
    if args.fail_on_blocked and not report["recovery_evidence_ready"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
