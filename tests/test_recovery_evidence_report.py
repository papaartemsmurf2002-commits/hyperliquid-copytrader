from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_recovery_evidence_report.py"
SPEC = importlib.util.spec_from_file_location("build_recovery_evidence_report", SCRIPT_PATH)
assert SPEC is not None
build_recovery_evidence_report = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = build_recovery_evidence_report
SPEC.loader.exec_module(build_recovery_evidence_report)


SOURCE = "0x1111111111111111111111111111111111111111"
OTHER_SOURCE = "0x2222222222222222222222222222222222222222"
FOLLOWER = "0xf000000000000000000000000000000000000001"


def source_state_report() -> dict:
    return {
        "counts": {"by_address": {SOURCE: 4}},
        "exchange_touched": False,
        "read_only": True,
        "recovery": {
            "degraded_events": 1,
            "live_stream_hints_allowed": False,
            "pending_reconcile": True,
            "pending_rest_backfill": True,
            "reconnect_recovered_events": 1,
            "state_refreshes_after_reconnect": 2,
            "stream_state": "recovery_required",
            "windows": [
                {
                    "complete": False,
                    "degraded_ts_ms": 1_000,
                    "first_post_reconnect_rest_snapshot_ms": 1_300,
                    "first_post_reconnect_state_refresh_ms": 1_300,
                    "gap_ms": 200,
                    "missing_requirements": [
                        "explicit_rest_backfill_complete",
                        "explicit_reconcile_complete",
                        "live_stream_hints_allowed_after_window",
                    ],
                    "post_reconnect_account_context_rows": 2,
                    "post_reconnect_rest_snapshots": 1,
                    "post_reconnect_source_actions": 3,
                    "post_reconnect_state_refreshes": 2,
                    "reconnect_row_id": "reconnect-1",
                    "reconnected_ts_ms": 1_200,
                    "sequence": 1,
                    "status": "requires_rest_backfill_and_reconcile",
                }
            ],
        },
    }


def follower_snapshot(
    *,
    verified: bool = True,
    recovery_complete: bool = True,
    include_address_verification: bool = True,
    observed_address: str | None = None,
) -> dict:
    snapshot = {
        "exchange_touched": False,
        "follower_subaccount": FOLLOWER,
        "follower_subaccount_verified": verified,
        "snapshot_normalizer_version": 2,
        "request_status": {
            "clearinghouseState_ok": True,
            "openOrders_ok": True,
        },
        "positions": [],
        "open_orders": [],
        "counts": {"positions": 0, "open_orders": 0},
        "warnings": [],
        "read_only": True,
        "recovery": {
            "follower_refresh_complete": recovery_complete,
            "reconcile_complete": recovery_complete,
            "source_backfill_complete": recovery_complete,
        },
    }
    if include_address_verification:
        observed = (
            observed_address if observed_address is not None else (FOLLOWER if verified else None)
        )
        snapshot["address_verification"] = {
            "expected_follower_subaccount": FOLLOWER,
            "observed_address": observed,
            "observed_address_source": "raw.address" if observed else None,
            "reason": (
                "raw snapshot address matches follower_subaccount"
                if verified
                else "raw snapshot does not include a valid observed address"
            ),
            "verified": verified,
        }
    return snapshot


def proof(*, reconnect_row_id: str = "reconnect-1") -> dict:
    return {
        "exchange_touched": False,
        "read_only": True,
        "recovery_proof_template_version": 1,
        "source_address": SOURCE,
        "windows": [
            {
                "evidence_refs": ["source backfill report 1", "follower reconcile report 1"],
                "explicit_reconcile_complete": True,
                "explicit_rest_backfill_complete": True,
                "live_stream_hints_allowed_after_window": True,
                "reconnect_row_id": reconnect_row_id,
                "sequence": 1,
            }
        ],
    }


def test_recovery_evidence_rejects_legacy_fail_open_follower_snapshot():
    legacy = follower_snapshot()
    legacy["snapshot_normalizer_version"] = 1

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=legacy,
        proof=proof(),
    )

    assert report["status"] == "blocked"
    assert report["follower_snapshot"]["snapshot_normalizer_supported"] is False
    assert any("follower_snapshot_normalizer_supported" in item for item in report["blockers"])


def test_recovery_evidence_blocks_without_proof_or_follower_snapshot():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
    )

    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["status"] == "blocked"
    assert report["recovery_evidence_ready"] is False
    assert any("proof_attached" in blocker for blocker in report["blockers"])
    assert any("follower_snapshot_attached" in blocker for blocker in report["blockers"])
    window = report["windows"][0]
    assert window["proof_attached"] is False
    assert window["complete"] is False
    assert window["missing_requirements"] == [
        "explicit_reconcile_complete",
        "explicit_rest_backfill_complete",
        "live_stream_hints_allowed_after_window",
        "proof window is missing",
        "verified follower refresh/reconcile snapshot is missing",
    ]


def test_recovery_evidence_blocks_missing_source_identity():
    source_report = source_state_report()
    source_report["counts"]["by_address"] = {}

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_report,
        follower_snapshot=follower_snapshot(),
        proof=proof(),
    )

    assert report["status"] == "blocked"
    assert report["source_state"]["source_address"] == ""
    assert report["source_state"]["source_address_candidates"] == {
        "counts_by_address": [],
        "slot_metadata": "",
        "source_state_report": "",
    }
    assert any("source_state_source_address_valid" in blocker for blocker in report["blockers"])
    assert any("no valid source address" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_ambiguous_source_identity():
    source_report = source_state_report()
    source_report["slot"] = {"source_address": SOURCE}
    source_report["counts"]["by_address"] = {SOURCE: 4, OTHER_SOURCE: 1}

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_report,
        follower_snapshot=follower_snapshot(),
        proof=proof(),
    )

    assert report["status"] == "blocked"
    assert report["source_state"]["source_address"] == ""
    assert report["source_state"]["source_address_candidates"] == {
        "counts_by_address": [SOURCE, OTHER_SOURCE],
        "slot_metadata": SOURCE,
        "source_state_report": "",
    }
    assert any(
        "source_state_source_address_unambiguous" in blocker for blocker in report["blockers"]
    )
    assert any("ambiguous source addresses" in blocker for blocker in report["blockers"])


def test_recovery_proof_template_uses_source_window_ids_and_stays_false_by_default():
    template = build_recovery_evidence_report.build_recovery_proof_template(source_state_report())

    assert template == {
        "exchange_touched": False,
        "notes": (
            "Fill evidence_refs and set completion flags only after source backfill, "
            "source/follower reconciliation, and post-window live-stream trust are proven."
        ),
        "read_only": True,
        "recovery_proof_template_version": 1,
        "slot": "",
        "source_address": SOURCE,
        "windows": [
            {
                "degraded_ts_ms": 1_000,
                "evidence_refs": [],
                "explicit_reconcile_complete": False,
                "explicit_rest_backfill_complete": False,
                "gap_ms": 200,
                "live_stream_hints_allowed_after_window": False,
                "operator_notes": "",
                "post_reconnect_account_context_rows": 2,
                "post_reconnect_rest_snapshots": 1,
                "post_reconnect_source_actions": 3,
                "post_reconnect_state_refreshes": 2,
                "reconnect_row_id": "reconnect-1",
                "reconnected_ts_ms": 1_200,
                "sequence": 1,
            }
        ],
    }


def test_recovery_evidence_blocks_when_unfilled_template_is_used_as_proof():
    template = build_recovery_evidence_report.build_recovery_proof_template(source_state_report())

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=template,
    )

    assert report["status"] == "blocked"
    assert report["windows"][0]["missing_requirements"] == [
        "explicit_reconcile_complete",
        "explicit_rest_backfill_complete",
        "live_stream_hints_allowed_after_window",
        "proof window must include evidence_refs",
    ]


def test_recovery_evidence_blocks_mismatched_proof_row_id():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof(reconnect_row_id="stale-row"),
    )

    assert report["status"] == "blocked"
    window = report["windows"][0]
    assert window["proof_attached"] is True
    assert "proof reconnect_row_id does not match source window" in window["missing_requirements"]
    assert any("window_1_complete" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_missing_proof_source_address():
    proof_payload = proof()
    proof_payload.pop("source_address")

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["source_address"] == ""
    assert any("proof_source_matches" in blocker for blocker in report["blockers"])
    assert any("proof source=missing" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_unknown_proof_source_address():
    proof_payload = proof()
    proof_payload["source_address"] = "unknown"

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["source_address"] == "unknown"
    assert any("proof_source_matches" in blocker for blocker in report["blockers"])
    assert any("proof source=unknown" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_missing_proof_version():
    proof_payload = proof()
    proof_payload.pop("recovery_proof_template_version")

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["recovery_proof_template_version"] is None
    assert any("proof_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=None expected=1" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_unsupported_proof_version():
    proof_payload = proof()
    proof_payload["recovery_proof_template_version"] = 999

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["recovery_proof_template_version"] == 999
    assert any("proof_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=999 expected=1" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_extra_proof_window_sequence():
    proof_payload = proof()
    proof_payload["windows"].append(
        {
            "evidence_refs": ["stale source backfill report"],
            "explicit_reconcile_complete": True,
            "explicit_rest_backfill_complete": True,
            "live_stream_hints_allowed_after_window": True,
            "reconnect_row_id": "stale-row",
            "sequence": 99,
        }
    )

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["unexpected_sequences"] == [99]
    assert any("proof_windows_match_source_sequences" in blocker for blocker in report["blockers"])
    assert any("unexpected_sequences=99" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_proof_window_without_valid_sequence():
    proof_payload = proof()
    proof_payload["windows"].append(
        {
            "evidence_refs": ["source backfill report without sequence"],
            "explicit_reconcile_complete": True,
            "explicit_rest_backfill_complete": True,
            "live_stream_hints_allowed_after_window": True,
            "reconnect_row_id": "missing-sequence",
        }
    )

    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof_payload,
    )

    assert report["status"] == "blocked"
    assert report["proof"]["invalid_sequence_windows"] == 1
    assert any("proof_windows_have_sequence" in blocker for blocker in report["blockers"])
    assert any("invalid_sequence_windows=1" in blocker for blocker in report["blockers"])


def test_recovery_evidence_ready_with_matching_proof_and_verified_follower_snapshot():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(),
        proof=proof(),
    )

    assert report["status"] == "recovery_evidence_ready"
    assert report["recovery_evidence_ready"] is True
    assert report["blockers"] == []
    assert report["source_state"]["pending_rest_backfill"] is True
    assert report["follower_snapshot"]["recovery_complete"] is True
    assert report["follower_snapshot"]["address_verification"]["verified"] is True
    assert report["proof"]["attached"] is True
    assert report["windows"] == [
        {
            "complete": True,
            "degraded_ts_ms": 1_000,
            "detail": "complete",
            "evidence_refs": ["source backfill report 1", "follower reconcile report 1"],
            "gap_ms": 200,
            "missing_requirements": [],
            "post_reconnect_account_context_rows": 2,
            "post_reconnect_rest_snapshots": 1,
            "post_reconnect_source_actions": 3,
            "post_reconnect_state_refreshes": 2,
            "proof_attached": True,
            "proof_requirements": {
                "explicit_reconcile_complete": True,
                "explicit_rest_backfill_complete": True,
                "live_stream_hints_allowed_after_window": True,
            },
            "reconnect_row_id": "reconnect-1",
            "reconnected_ts_ms": 1_200,
            "sequence": 1,
        }
    ]


def test_recovery_evidence_blocks_unverified_follower_snapshot():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(verified=False),
        proof=proof(),
    )

    assert report["status"] == "blocked"
    assert any("follower_snapshot_address_verified" in blocker for blocker in report["blockers"])
    assert report["windows"][0]["missing_requirements"] == [
        "verified follower refresh/reconcile snapshot is missing"
    ]


def test_recovery_evidence_blocks_boolean_only_follower_verification():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(include_address_verification=False),
        proof=proof(),
    )

    assert report["status"] == "blocked"
    assert report["follower_snapshot"]["follower_subaccount_verified"] is False
    assert report["follower_snapshot"]["address_verification"]["claimed_verified"] is True
    assert (
        report["follower_snapshot"]["address_verification"]["checks"][
            "address_verification_attached"
        ]
        is False
    )
    assert any("address_verification_attached" in blocker for blocker in report["blockers"])


def test_recovery_evidence_blocks_mismatched_observed_follower_address():
    report = build_recovery_evidence_report.build_recovery_evidence_report(
        source_state_report=source_state_report(),
        follower_snapshot=follower_snapshot(observed_address=SOURCE),
        proof=proof(),
    )

    assert report["status"] == "blocked"
    verification = report["follower_snapshot"]["address_verification"]
    assert verification["observed_address"] == SOURCE
    assert verification["checks"]["observed_matches_follower_subaccount"] is False
    assert any("observed_matches_follower_subaccount" in blocker for blocker in report["blockers"])
