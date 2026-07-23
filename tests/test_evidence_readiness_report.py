from __future__ import annotations

import csv
import importlib.util
import io
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_evidence_readiness_report.py"
SPEC = importlib.util.spec_from_file_location("build_evidence_readiness_report", SCRIPT_PATH)
assert SPEC is not None
build_evidence_readiness_report = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = build_evidence_readiness_report
SPEC.loader.exec_module(build_evidence_readiness_report)


def backtest_fixture(*, truncated: bool = False, corrected: bool = True) -> dict:
    if corrected:
        sources = []
        for slot in range(1, 11):
            sources.append(
                {
                    "slot": slot,
                    "truncated": truncated if slot == 1 else False,
                    "simulations": [
                        {
                            "strategy": "risk_budget_50_cap_10x",
                            "initial_equity_usd": "500.0" if slot == 1 else "0",
                            "ending_equity_usd": "4148.89283278893981" if slot == 1 else "0",
                            "net_pnl_usd": "3648.892832788939795" if slot == 1 else "0",
                            "copied_fills": 28770 if slot == 1 else 0,
                            "skipped_min_notional_fills": 36207 if slot == 1 else 0,
                            "capped_fills": 413 if slot == 1 else 0,
                            "liquidated_or_zero_equity": slot in {2, 3},
                        }
                    ],
                }
            )
        return {
            "days": 180,
            "initial_equity_usd": 50,
            "sources": sources,
        }
    return {
        "days": 180,
        "initial_equity_usd": 50,
        "sources": [
            {
                "slot": 1,
                "truncated": truncated,
                "simulations": [
                    {
                        "strategy": "risk_budget_50_cap_10x",
                        "initial_equity_usd": 50,
                        "ending_equity_usd": 150,
                        "net_pnl_usd": 100,
                        "copied_fills": 7,
                        "skipped_min_notional_fills": 3,
                        "capped_fills": 1,
                        "liquidated_or_zero_equity": False,
                    }
                ],
            },
            {
                "slot": 2,
                "truncated": False,
                "simulations": [
                    {
                        "strategy": "risk_budget_50_cap_10x",
                        "initial_equity_usd": 50,
                        "ending_equity_usd": 0,
                        "net_pnl_usd": -50,
                        "copied_fills": 2,
                        "skipped_min_notional_fills": 5,
                        "capped_fills": 0,
                        "liquidated_or_zero_equity": True,
                    }
                ],
            },
        ],
    }


def slot_state_fixture(
    *,
    recovery_complete: bool,
    follower_snapshot_verified: bool = True,
    recovery_notes: str = "normalized read-only follower REST snapshot",
) -> dict:
    missing = [] if recovery_complete else ["source.pending_rest_backfill_cleared"]
    return {
        "valid": True,
        "read_only": True,
        "exchange_touched": False,
        "slot_state_report_version": 2,
        "input_blockers": [],
        "input_warnings": [],
        "recovery_completion": {
            "complete": True,
            "source_backfill_complete": True,
            "follower_refresh_complete": True,
            "reconcile_complete": True,
            "missing_requirements": [],
            "notes": recovery_notes,
        },
        "slots": [
            {
                "slot": "slot-01",
                "status": "ready" if recovery_complete else "blocked",
                "follower_matches_subaccount": True,
                "follower_matches_slot": True,
                "follower_snapshot_verified": follower_snapshot_verified,
                "follower_snapshot_verification": {
                    "expected_subaccount": "0xf000000000000000000000000000000000000001",
                    "observed_address": (
                        "0xf000000000000000000000000000000000000001"
                        if follower_snapshot_verified
                        else None
                    ),
                    "reason": (
                        "follower snapshot observed address matches configured subaccount"
                        if follower_snapshot_verified
                        else "follower snapshot lacks address verification"
                    ),
                    "verified": follower_snapshot_verified,
                },
                "recovery_gate": {
                    "complete": recovery_complete,
                    "decision": "recovery_complete"
                    if recovery_complete
                    else "blocked_recovery_incomplete",
                    "repair_intents_actionable": recovery_complete,
                    "missing_requirements": missing,
                    "blocker_reasons": (
                        [] if recovery_complete else ["source state still requires REST backfill"]
                    ),
                },
                "source_state": {
                    "summary": {
                        "recovery": {
                            "windows": {
                                "complete": 0 if not recovery_complete else 1,
                                "count": 1,
                                "incomplete": 1 if not recovery_complete else 0,
                                "max_gap_ms": 250,
                                "missing_requirements": (
                                    {"explicit_rest_backfill_complete": 1}
                                    if not recovery_complete
                                    else {}
                                ),
                                "sample": [
                                    {
                                        "complete": recovery_complete,
                                        "gap_ms": 250,
                                        "missing_requirements": missing,
                                        "post_reconnect_rest_snapshots": 2,
                                        "post_reconnect_state_refreshes": 3,
                                        "sequence": 1,
                                        "status": "complete"
                                        if recovery_complete
                                        else "requires_rest_backfill_and_reconcile",
                                    }
                                ],
                            }
                        }
                    }
                },
            }
        ],
    }


def recovery_evidence_fixture(*, ready: bool) -> dict:
    blockers = [] if ready else ["window_1_complete: proof window is missing"]
    return {
        "exchange_touched": False,
        "read_only": True,
        "recovery_evidence_report_version": 2,
        "recovery_evidence_ready": ready,
        "status": "recovery_evidence_ready" if ready else "blocked",
        "blockers": blockers,
        "windows": [
            {
                "complete": ready,
                "missing_requirements": [] if ready else ["proof window is missing"],
                "sequence": 1,
            }
        ],
    }


def backtest_csv_text(backtest: dict, *, copied_fills_delta: int = 0) -> str:
    fieldnames = [
        "slot",
        "address",
        "fills",
        "fill_pages",
        "truncated",
        "portfolio_window",
        "source_current_account_value_usd",
        "source_window_net_pnl_usd",
        "strategy",
        "initial_equity_usd",
        "ending_equity_usd",
        "net_pnl_usd",
        "roi_pct",
        "max_drawdown_usd",
        "min_equity_usd",
        "max_effective_leverage",
        "copied_fills",
        "skipped_min_notional_fills",
        "capped_fills",
        "copied_notional_usd",
        "source_net_pnl_seen_usd",
        "liquidated_or_zero_equity",
    ]
    rows = []
    for source in backtest["sources"]:
        slot = int(source["slot"])
        for simulation in source["simulations"]:
            rows.append(
                {
                    "slot": slot,
                    "address": f"0x{slot:040x}",
                    "fills": 0,
                    "fill_pages": 0,
                    "truncated": str(source.get("truncated") is True),
                    "portfolio_window": "allTime",
                    "source_current_account_value_usd": "0",
                    "source_window_net_pnl_usd": "0",
                    "strategy": simulation["strategy"],
                    "initial_equity_usd": simulation["initial_equity_usd"],
                    "ending_equity_usd": simulation["ending_equity_usd"],
                    "net_pnl_usd": simulation["net_pnl_usd"],
                    "roi_pct": "0",
                    "max_drawdown_usd": "0",
                    "min_equity_usd": "0",
                    "max_effective_leverage": "0",
                    "copied_fills": simulation["copied_fills"],
                    "skipped_min_notional_fills": simulation["skipped_min_notional_fills"],
                    "capped_fills": simulation["capped_fills"],
                    "copied_notional_usd": "0",
                    "source_net_pnl_seen_usd": "0",
                    "liquidated_or_zero_equity": str(
                        simulation.get("liquidated_or_zero_equity") is True
                    ),
                }
            )
    if rows and copied_fills_delta:
        rows[0]["copied_fills"] = int(rows[0]["copied_fills"]) + copied_fills_delta

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def build_ready_report_from_paths(
    tmp_path: Path,
    *,
    companion_csv: str | None,
) -> dict:
    backtest = backtest_fixture()
    backtest_path = tmp_path / "copy10_180d_20260704-000211.json"
    slot_state_path = tmp_path / "slot_state.json"
    recovery_evidence_path = tmp_path / "recovery_evidence.json"
    build_evidence_readiness_report.write_json(backtest_path, backtest)
    build_evidence_readiness_report.write_json(
        slot_state_path,
        slot_state_fixture(recovery_complete=True),
    )
    build_evidence_readiness_report.write_json(
        recovery_evidence_path,
        recovery_evidence_fixture(ready=True),
    )
    if companion_csv is not None:
        backtest_path.with_suffix(".csv").write_text(companion_csv, encoding="utf-8")
    return build_evidence_readiness_report.build_from_paths(
        backtest_path=backtest_path,
        slot_state_report_path=slot_state_path,
        recovery_evidence_report_path=recovery_evidence_path,
    )


def test_evidence_report_blocks_when_slot_recovery_is_incomplete():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(corrected=False),
        slot_state=slot_state_fixture(recovery_complete=False),
    )

    assert report["status"] == "blocked"
    assert report["shadow_replay_ready"] is False
    assert report["testnet_canary_candidate"] is False
    assert any("slot_state_recovery_gates_complete" in blocker for blocker in report["blockers"])
    assert report["backtest"]["ending_equity_usd"] == "150"
    assert report["backtest"]["net_pnl_usd"] == "50"
    assert report["backtest"]["copied_fills"] == 9
    assert report["backtest"]["skipped_min_notional_fills"] == 8
    assert report["backtest"]["zeroed_slots"] == 1
    assert any("backtest_corrected_180d_aggregate" in blocker for blocker in report["blockers"])
    incomplete = report["slot_state"]["incomplete_recovery_slots"][0]
    assert incomplete["missing_requirements"] == ["source.pending_rest_backfill_cleared"]
    assert report["recovery_evidence"] == {
        "attached": False,
        "blockers": [],
        "incomplete_windows": [],
        "recovery_evidence_ready": False,
        "status": "not_attached",
        "window_count": 0,
    }
    assert report["slot_state"]["source_recovery_windows"] == [
        {
            "complete": 0,
            "count": 1,
            "incomplete": 1,
            "max_gap_ms": 250,
            "missing_requirements": {"explicit_rest_backfill_complete": 1},
            "sample": [
                {
                    "complete": False,
                    "gap_ms": 250,
                    "missing_requirements": ["source.pending_rest_backfill_cleared"],
                    "post_reconnect_rest_snapshots": 2,
                    "post_reconnect_state_refreshes": 3,
                    "sequence": 1,
                    "status": "requires_rest_backfill_and_reconcile",
                }
            ],
            "slot": "slot-01",
        }
    ]


def test_evidence_report_allows_only_testnet_candidate_when_shadow_evidence_is_clean():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence_fixture(ready=True),
    )

    assert report["status"] == "shadow_replay_ready"
    assert report["evidence_readiness_report_version"] == 2
    assert report["shadow_replay_ready"] is True
    assert report["testnet_canary_candidate"] is True
    assert report["mainnet_ready"] is False
    assert report["blockers"] == []
    assert any(
        "mainnet live remains not default-ready" in warning for warning in report["warnings"]
    )
    assert report["slot_state"]["slot_state_report_version"] == 2
    assert report["slot_state"]["repair_intents_actionable_slots"] == 1
    assert report["slot_state"]["follower_snapshot_verified_slots"] == 1


def test_evidence_report_blocks_missing_slot_state_version():
    slot_state = slot_state_fixture(recovery_complete=True)
    slot_state.pop("slot_state_report_version")

    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state,
        recovery_evidence=recovery_evidence_fixture(ready=True),
    )

    assert report["status"] == "blocked"
    assert report["slot_state"]["slot_state_report_version"] is None
    assert any("slot_state_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=None expected=2" in blocker for blocker in report["blockers"])


def test_evidence_report_blocks_unsupported_slot_state_version():
    slot_state = slot_state_fixture(recovery_complete=True)
    slot_state["slot_state_report_version"] = 999

    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state,
        recovery_evidence=recovery_evidence_fixture(ready=True),
    )

    assert report["status"] == "blocked"
    assert report["slot_state"]["slot_state_report_version"] == 999
    assert any("slot_state_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=999 expected=2" in blocker for blocker in report["blockers"])


def test_evidence_report_blocks_without_recovery_evidence_even_when_slot_state_is_clean():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
    )

    assert report["status"] == "blocked"
    assert report["shadow_replay_ready"] is False
    assert report["testnet_canary_candidate"] is False
    assert any("recovery_evidence_attached" in blocker for blocker in report["blockers"])
    assert report["next_required_actions"][0] == (
        "Attach build_recovery_evidence_report.py output to the evidence readiness report."
    )


def test_evidence_report_includes_ready_recovery_evidence_when_attached():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence_fixture(ready=True),
    )

    assert report["status"] == "shadow_replay_ready"
    assert report["blockers"] == []
    assert report["recovery_evidence"] == {
        "attached": True,
        "blockers": [],
        "exchange_touched": False,
        "incomplete_windows": [],
        "read_only": True,
        "recovery_evidence_report_version": 2,
        "recovery_evidence_ready": True,
        "status": "recovery_evidence_ready",
        "window_count": 1,
    }


def test_evidence_report_blocks_missing_recovery_evidence_version():
    recovery_evidence = recovery_evidence_fixture(ready=True)
    recovery_evidence.pop("recovery_evidence_report_version")

    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence,
    )

    assert report["status"] == "blocked"
    assert report["recovery_evidence"]["recovery_evidence_report_version"] is None
    assert any("recovery_evidence_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=None expected=2" in blocker for blocker in report["blockers"])


def test_evidence_report_blocks_unsupported_recovery_evidence_version():
    recovery_evidence = recovery_evidence_fixture(ready=True)
    recovery_evidence["recovery_evidence_report_version"] = 999

    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence,
    )

    assert report["status"] == "blocked"
    assert report["recovery_evidence"]["recovery_evidence_report_version"] == 999
    assert any("recovery_evidence_version_supported" in blocker for blocker in report["blockers"])
    assert any("version=999 expected=2" in blocker for blocker in report["blockers"])


def test_evidence_report_blocks_when_attached_recovery_evidence_is_blocked():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence_fixture(ready=False),
    )

    assert report["status"] == "blocked"
    assert report["shadow_replay_ready"] is False
    assert report["recovery_evidence"]["incomplete_windows"] == [
        {"missing_requirements": ["proof window is missing"], "sequence": 1}
    ]
    assert any("recovery_evidence_ready" in blocker for blocker in report["blockers"])
    assert report["next_required_actions"][0] == (
        "Resolve blockers in the attached recovery evidence report."
    )


def test_evidence_report_rejects_synthetic_follower_recovery_notes():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(
            recovery_complete=True,
            recovery_notes="Synthetic flat follower snapshot for analysis-only replay; not live recovery proof.",
        ),
    )

    assert report["shadow_replay_ready"] is False
    assert report["slot_state"]["synthetic_recovery_note_markers"] == [
        "analysis-only",
        "not live recovery proof",
        "synthetic",
    ]
    assert any(
        "slot_state_follower_snapshot_not_synthetic" in blocker for blocker in report["blockers"]
    )


def test_evidence_report_rejects_unverified_follower_snapshot_address():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(
            recovery_complete=True,
            follower_snapshot_verified=False,
        ),
    )

    assert report["shadow_replay_ready"] is False
    assert report["slot_state"]["follower_snapshot_verified_slots"] == 0
    assert report["slot_state"]["unverified_follower_snapshot_slots"] == [
        {
            "expected_subaccount": "0xf000000000000000000000000000000000000001",
            "observed_address": None,
            "reason": "follower snapshot lacks address verification",
            "slot": "slot-01",
        }
    ]
    assert any("slot_state_follower_snapshot_verified" in blocker for blocker in report["blockers"])


def test_evidence_report_flags_backtest_truncation():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(truncated=True),
        slot_state=slot_state_fixture(recovery_complete=True),
    )

    assert report["shadow_replay_ready"] is False
    assert report["backtest"]["truncated_sources"] == 1
    assert any("backtest_no_source_truncation" in blocker for blocker in report["blockers"])


def test_evidence_report_skips_companion_csv_check_without_backtest_path():
    report = build_evidence_readiness_report.build_evidence_readiness_report(
        backtest=backtest_fixture(),
        slot_state=slot_state_fixture(recovery_complete=True),
        recovery_evidence=recovery_evidence_fixture(ready=True),
    )

    assert report["status"] == "shadow_replay_ready"
    assert report["backtest_companion_csv"] == {
        "checked": False,
        "path": "",
        "reason": "backtest path was not provided",
        "status": "not_checked",
        "strategy": "risk_budget_50_cap_10x",
    }


def test_evidence_report_blocks_missing_backtest_companion_csv(tmp_path: Path):
    report = build_ready_report_from_paths(tmp_path, companion_csv=None)

    assert report["status"] == "blocked"
    assert report["backtest_companion_csv"]["status"] == "missing"
    assert any("backtest_companion_csv_present" in blocker for blocker in report["blockers"])


def test_evidence_report_blocks_mismatched_backtest_companion_csv(tmp_path: Path):
    companion_csv = backtest_csv_text(backtest_fixture(), copied_fills_delta=1)

    report = build_ready_report_from_paths(tmp_path, companion_csv=companion_csv)

    assert report["status"] == "blocked"
    assert report["backtest_companion_csv"]["copied_fills"] == 28771
    assert any(
        "backtest_companion_csv_matches_json" in blocker and "copied_fills" in blocker
        for blocker in report["blockers"]
    )


def test_evidence_report_accepts_matching_backtest_companion_csv(tmp_path: Path):
    companion_csv = backtest_csv_text(backtest_fixture())

    report = build_ready_report_from_paths(tmp_path, companion_csv=companion_csv)

    assert report["status"] == "shadow_replay_ready"
    assert report["blockers"] == []
    assert report["backtest_companion_csv"]["strategy_rows"] == 10
    assert report["backtest_companion_csv"]["copied_fills"] == 28770
    assert any(
        check["name"] == "backtest_companion_csv_matches_json" and check["passed"] is True
        for check in report["checks"]
    )
