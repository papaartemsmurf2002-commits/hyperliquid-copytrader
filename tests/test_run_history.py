from __future__ import annotations

import json
from pathlib import Path

from hyperliquid_copytrader.run_history import RunHistoryService


def _slot(
    *,
    copied: int = 1,
    pending: int = 0,
    drift: int = 0,
    source_reactions: int = 0,
    terminal_order_intents: int = 0,
) -> dict:
    return {
        "slot": "slot-1",
        "source_address": "0x" + "1" * 40,
        "subaccount_address": "0x" + "2" * 40,
        "intents": {"copied": copied, "skipped": 2},
        "execution_reports": {
            "ambiguous": 0,
            "partial": 0,
            "rejected": 1,
            "terminal_order_intents": terminal_order_intents,
        },
        "open_orders": 0,
        "pending_intents": pending,
        "unfinished_source_reactions": source_reactions,
        "position_drift": {"checked": True, "unexplained": drift},
    }


def _write_report(root: Path, name: str, payload: dict) -> None:
    run_dir = root / name
    run_dir.mkdir(parents=True)
    (run_dir / "post_run_report.json").write_text(json.dumps(payload), encoding="utf-8")


def test_run_history_recomputes_aggregate_and_omits_sensitive_rows(tmp_path):
    slot = _slot()
    aggregate = {
        "ambiguous_reports": 0,
        "copied_intents": 1,
        "open_orders": 0,
        "partial_reports": 0,
        "pending_intents": 0,
        "rejected_reports": 1,
        "skipped_intents": 2,
        "terminal_order_intents": 0,
        "unfinished_source_reactions": 0,
        "unexplained_drift": 0,
    }
    _write_report(
        tmp_path,
        "latest-run",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "mode": "testnet",
            "source_network": "mainnet",
            "slot_count": 1,
            "blockers": [],
            "aggregate": aggregate,
            "slots": [slot],
            "api_private_key": "must-not-leak",
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["status"] == "ready"
    assert snapshot["ready_count"] == 1
    assert snapshot["quality_issue_count"] == 0
    assert snapshot["rows"][0]["aggregate"] == aggregate
    serialized = json.dumps(snapshot)
    assert slot["source_address"] not in serialized
    assert slot["subaccount_address"] not in serialized
    assert "must-not-leak" not in serialized


def test_run_history_separates_readiness_from_signed_lifecycle(tmp_path):
    slot = _slot(terminal_order_intents=2)
    _write_report(
        tmp_path,
        "signed-run",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "signed_order_lifecycle_verified": True,
            "mode": "testnet",
            "source_network": "mainnet",
            "slot_count": 1,
            "blockers": [],
            "aggregate": _aggregate_for_slot(slot),
            "slots": [slot],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["ready_count"] == 1
    assert snapshot["signed_lifecycle_count"] == 1
    assert snapshot["rows"][0]["signed_order_lifecycle_verified"] is True


def test_run_history_reports_retired_slots_separately_from_active_readiness(tmp_path):
    slot = _slot()
    _write_report(
        tmp_path,
        "active-with-retired-account",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "mode": "testnet",
            "source_network": "mainnet",
            "slot_count": 1,
            "excluded_slot_count": 1,
            "fleet_slot_count": 2,
            "blockers": [],
            "aggregate": _aggregate_for_slot(slot),
            "slots": [slot],
            "excluded_slots": [
                {
                    "slot": "slot-2",
                    "operational_status": "retired_quarantined",
                    "execution_excluded": True,
                }
            ],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["ready_count"] == 1
    row = snapshot["rows"][0]
    assert row["slot_count"] == 1
    assert row["retired_slot_count"] == 1
    assert row["fleet_slot_count"] == 2
    assert row["quality_status"] == "valid"


def test_run_history_flags_inconsistent_ready_verdict(tmp_path):
    slot = _slot(pending=1, drift=1)
    _write_report(
        tmp_path,
        "bad-run",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "slot_count": 2,
            "blockers": ["unexpected blocker"],
            "aggregate": {},
            "slots": [slot],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()
    row = snapshot["rows"][0]

    assert snapshot["status"] == "inconsistent"
    assert snapshot["ready_count"] == 0
    assert snapshot["claimed_ready_count"] == 1
    assert snapshot["inconsistent_count"] == 1
    assert row["quality_status"] == "inconsistent"
    assert any("pending_intents" in issue for issue in row["quality_issues"])
    assert any("slot_count" in issue for issue in row["quality_issues"])
    assert any("contains blockers" in issue for issue in row["quality_issues"])


def test_run_history_rejects_ready_testnet_claim_without_checked_follower_drift(tmp_path):
    slot = _slot()
    slot["position_drift"]["checked"] = False
    aggregate = {
        "ambiguous_reports": 0,
        "copied_intents": 1,
        "open_orders": 0,
        "partial_reports": 0,
        "pending_intents": 0,
        "rejected_reports": 1,
        "skipped_intents": 2,
        "terminal_order_intents": 0,
        "unfinished_source_reactions": 0,
        "unexplained_drift": 0,
    }
    _write_report(
        tmp_path,
        "unchecked-run",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "mode": "testnet",
            "slot_count": 1,
            "blockers": [],
            "aggregate": aggregate,
            "slots": [slot],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["ready_count"] == 0
    assert snapshot["rows"][0]["quality_status"] == "inconsistent"
    assert any(
        "lacks checked follower drift" in issue for issue in snapshot["rows"][0]["quality_issues"]
    )


def test_run_history_handles_missing_and_malformed_reports(tmp_path):
    assert RunHistoryService(tmp_path / "missing").snapshot()["status"] == "empty"
    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    (bad_dir / "post_run_report.json").write_text("not-json", encoding="utf-8")

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["status"] == "error"
    assert snapshot["errors"][0]["run"] == "bad"


def test_run_history_redacts_address_in_directory_name(tmp_path):
    address = "0x" + "a" * 40
    _write_report(
        tmp_path,
        f"testnet-{address}",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "blocked",
            "ready_for_next_phase": False,
            "aggregate": {},
            "slots": [],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert address not in json.dumps(snapshot)
    assert snapshot["status"] == "blocked"
    assert snapshot["ready_count"] == 0
    assert snapshot["blocked_count"] == 1
    assert snapshot["rows"][0]["run"] == "testnet-0x-redacted"


def test_run_history_downgrades_legacy_ready_reports(tmp_path):
    slot = _slot()
    _write_report(
        tmp_path,
        "legacy-ready",
        {
            "post_run_report_version": 3,
            "slot_supervisor_version": 3,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "mode": "testnet",
            "slot_count": 1,
            "blockers": [],
            "aggregate": _aggregate_for_slot(slot),
            "slots": [slot],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["status"] == "inconsistent"
    assert snapshot["ready_count"] == 0
    assert snapshot["claimed_ready_count"] == 1
    assert any(
        "unsupported post-run report version" in issue
        for issue in snapshot["rows"][0]["quality_issues"]
    )


def test_run_history_rejects_ready_claim_with_unfinished_source_reactions(tmp_path):
    slot = _slot(source_reactions=1)
    _write_report(
        tmp_path,
        "unfinished-reaction",
        {
            "post_run_report_version": 4,
            "slot_supervisor_version": 4,
            "status": "ready_for_next_phase",
            "ready_for_next_phase": True,
            "mode": "testnet",
            "slot_count": 1,
            "blockers": [],
            "aggregate": _aggregate_for_slot(slot),
            "slots": [slot],
        },
    )

    snapshot = RunHistoryService(tmp_path).snapshot()

    assert snapshot["ready_count"] == 0
    assert any(
        "unfinished_source_reactions=1" in issue for issue in snapshot["rows"][0]["quality_issues"]
    )


def _aggregate_for_slot(slot: dict) -> dict[str, int]:
    return {
        "ambiguous_reports": slot["execution_reports"]["ambiguous"],
        "copied_intents": slot["intents"]["copied"],
        "open_orders": slot["open_orders"],
        "partial_reports": slot["execution_reports"]["partial"],
        "pending_intents": slot["pending_intents"],
        "rejected_reports": slot["execution_reports"]["rejected"],
        "skipped_intents": slot["intents"]["skipped"],
        "terminal_order_intents": slot["execution_reports"]["terminal_order_intents"],
        "unfinished_source_reactions": slot["unfinished_source_reactions"],
        "unexplained_drift": slot["position_drift"]["unexplained"],
    }
