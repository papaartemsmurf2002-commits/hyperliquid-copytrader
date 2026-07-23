from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "track_replay_state.py"
SPEC = importlib.util.spec_from_file_location("track_replay_state", SCRIPT_PATH)
assert SPEC is not None
track_replay_state = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = track_replay_state
SPEC.loader.exec_module(track_replay_state)


ADDRESS = "0x1111111111111111111111111111111111111111"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def scorecard_row(
    *,
    row_id: str,
    sort_ts_ms: int,
    category: str,
    source_action: str,
    classifier_decision: str,
    event_type: str = "fill",
    subtype: str = "user:fills",
    confidence: str = "medium",
    metadata: dict | None = None,
) -> dict:
    return {
        "address": ADDRESS,
        "category": category,
        "classifier_decision": classifier_decision,
        "confidence": confidence,
        "event_id": f"event-{row_id}",
        "event_type": event_type,
        "intended_followup": "fixture followup",
        "metadata": metadata or {},
        "reason": "fixture reason",
        "row_id": row_id,
        "sort_ts_ms": sort_ts_ms,
        "source_action": source_action,
        "subtype": subtype,
    }


def test_state_tracker_tracks_order_lifecycle_and_fills(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            scorecard_row(
                row_id="open",
                sort_ts_ms=1_000,
                category="source_action",
                source_action="source_order_open",
                classifier_decision="would_map_or_refresh_target_order",
                event_type="order_update",
                subtype="order_update:open",
                metadata={"coin": "BTC", "side": "B", "oid": "42", "status": "open"},
            ),
            scorecard_row(
                row_id="fill",
                sort_ts_ms=1_100,
                category="source_action",
                source_action="source_fill",
                classifier_decision="would_validate_position_and_drift",
                metadata={
                    "coin": "BTC",
                    "dir": "Open Long",
                    "hash": "0xfill",
                    "notional_usd": 10.0,
                    "oid": "42",
                    "px": "100",
                    "side": "B",
                    "sz": "0.1",
                    "tid": "7",
                },
            ),
            scorecard_row(
                row_id="filled",
                sort_ts_ms=1_200,
                category="source_action",
                source_action="source_order_filled",
                classifier_decision="would_validate_position_and_drift",
                event_type="order_update",
                subtype="order_update:filled",
                metadata={"coin": "BTC", "side": "B", "oid": "42", "status": "filled"},
            ),
        ],
    )

    report = track_replay_state.track_replay_state(rows_path, facts_out=facts_out, slot="slot-1")

    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["rows_processed"] == 3
    assert report["orders"]["seen"] == 1
    assert report["orders"]["open"] == 0
    assert report["orders"]["by_status"] == {"filled": 1}
    assert report["orders"]["unmatched_terminal_updates"] == 0
    assert report["fills"]["source_fills"] == 1
    assert report["fills"]["source_fills_by_coin"] == {"BTC": 1}
    assert report["fills"]["source_fill_notional_usd"] == 10.0
    assert report["fills"]["source_fill_notional_observations"] == 1
    assert report["policy_neutral_shadow"]["fill_price_size_available"] is True
    assert report["counts"]["by_fact_type"] == {
        "order_open_seen": 1,
        "order_terminal_seen": 1,
        "source_fill_seen": 1,
    }
    facts = [json.loads(line) for line in facts_out.read_text(encoding="utf-8").splitlines()]
    assert [fact["fact_type"] for fact in facts] == [
        "order_open_seen",
        "source_fill_seen",
        "order_terminal_seen",
    ]
    assert facts[0]["slot"] == "slot-1"


def test_state_tracker_models_non_user_cancel_as_reconcile_signal(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            scorecard_row(
                row_id="open",
                sort_ts_ms=1_000,
                category="source_action",
                source_action="source_order_open",
                classifier_decision="would_map_or_refresh_target_order",
                event_type="order_update",
                subtype="order_update:open",
                metadata={"coin": "BTC", "side": "B", "oid": "42", "status": "open"},
            ),
            scorecard_row(
                row_id="matched-cancel",
                sort_ts_ms=1_050,
                category="source_action",
                source_action="source_non_user_cancel",
                classifier_decision="would_cancel_or_reconcile_mapped_order",
                event_type="cancel",
                subtype="cancel",
                metadata={"coin": "BTC", "side": "B", "oid": "42"},
            ),
            scorecard_row(
                row_id="unmatched-cancel",
                sort_ts_ms=1_100,
                category="source_action",
                source_action="source_non_user_cancel",
                classifier_decision="would_cancel_or_reconcile_mapped_order",
                event_type="cancel",
                subtype="cancel",
                metadata={"coin": "ETH", "side": "A", "oid": "99"},
            ),
        ],
    )

    report = track_replay_state.track_replay_state(rows_path, facts_out=facts_out)

    assert report["counts"]["by_fact_type"] == {
        "order_open_seen": 1,
        "source_cancel_seen": 2,
    }
    assert report["counts"]["by_source_action"]["source_non_user_cancel"] == 2
    assert report["orders"]["open"] == 1
    assert report["orders"]["terminal"] == 0
    assert report["orders"]["open_order_samples"][0]["cancel_request_count"] == 1
    assert report["source_cancels"] == {
        "requests": 2,
        "matched_known_order": 1,
        "unmatched_requires_reconcile": 1,
        "unmatched_samples": [
            {
                "address": ADDRESS,
                "category": "source_action",
                "classifier_decision": "would_cancel_or_reconcile_mapped_order",
                "confidence": "medium",
                "event_id": "event-unmatched-cancel",
                "event_type": "cancel",
                "intended_followup": "fixture followup",
                "metadata": {"coin": "ETH", "oid": "99", "side": "A"},
                "reason": "fixture reason",
                "row_id": "unmatched-cancel",
                "sort_ts_ms": 1_100,
                "source_action": "source_non_user_cancel",
                "subtype": "cancel",
            }
        ],
    }
    assert report["account_context"]["confidence_downgrade_reasons"] == {
        "source_cancel_requires_reconcile": 1
    }
    facts = [json.loads(line) for line in facts_out.read_text(encoding="utf-8").splitlines()]
    cancel_facts = [fact for fact in facts if fact["fact_type"] == "source_cancel_seen"]
    assert [fact["metadata"]["matched_known_order"] for fact in cancel_facts] == [True, False]
    assert {fact["metadata"]["target_action"] for fact in cancel_facts} == {
        "cancel_mapped_order_or_reconcile"
    }
    assert "source_action_unmodeled" not in report["counts"]["by_fact_type"]


def test_state_tracker_keeps_passive_snapshots_out_of_source_actions(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    write_jsonl(
        rows_path,
        [
            scorecard_row(
                row_id="seed",
                sort_ts_ms=1_000,
                category="passive_snapshot",
                source_action="snapshot_seed_or_refresh",
                classifier_decision="snapshot_seed_or_refresh",
                confidence="high",
                metadata={"is_snapshot": True, "coin": "ETH", "oid": "1"},
            ),
            scorecard_row(
                row_id="duplicate",
                sort_ts_ms=1_000,
                category="passive_snapshot",
                source_action="snapshot_duplicate",
                classifier_decision="duplicate_snapshot_skipped",
                confidence="high",
                metadata={"is_snapshot": True, "coin": "ETH", "oid": "1"},
            ),
        ],
    )

    report = track_replay_state.track_replay_state(rows_path)

    assert report["orders"]["seen"] == 0
    assert report["fills"]["source_fills"] == 0
    assert report["passive_snapshots"] == {
        "duplicate_snapshot_skipped": 1,
        "snapshot_seed_or_refresh": 1,
    }
    assert report["counts"]["by_fact_type"] == {
        "duplicate_snapshot_skipped": 1,
        "snapshot_seed_or_refresh_seen": 1,
    }


def test_state_tracker_tracks_twap_lifecycle(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    write_jsonl(
        rows_path,
        [
            scorecard_row(
                row_id="activated",
                sort_ts_ms=1_000,
                category="source_action",
                source_action="source_twap_activated",
                classifier_decision="would_map_target_twap_when_supported",
                event_type="twap_history",
                subtype="twap_history:activated",
                metadata={"coin": "SOL", "reduce_only": False, "side": "A", "twap_id": "77"},
            ),
            scorecard_row(
                row_id="refresh",
                sort_ts_ms=1_050,
                category="state_refresh",
                source_action="twap_state_refresh",
                classifier_decision="state_refresh_only",
                event_type="twap_state",
                subtype="twap_state",
                metadata={"coin": "SOL", "side": "A", "twap_id": "77"},
            ),
            scorecard_row(
                row_id="slice",
                sort_ts_ms=1_100,
                category="source_action",
                source_action="source_twap_slice_fill",
                classifier_decision="would_validate_twap_progress",
                event_type="twap_slice_fill",
                subtype="user:twapSliceFills",
                metadata={
                    "coin": "SOL",
                    "dir": "Open Short",
                    "notional_usd": 20.0,
                    "px": "10",
                    "side": "A",
                    "sz": "2",
                    "twap_id": "77",
                },
            ),
            scorecard_row(
                row_id="finished",
                sort_ts_ms=1_200,
                category="source_action",
                source_action="source_twap_finished",
                classifier_decision="would_reconcile_twap_terminal_state",
                event_type="twap_history",
                subtype="twap_history:finished",
                metadata={"coin": "SOL", "side": "A", "status": "finished", "twap_id": "77"},
            ),
        ],
    )

    report = track_replay_state.track_replay_state(rows_path)

    assert report["twaps"]["seen"] == 1
    assert report["twaps"]["active"] == 0
    assert report["twaps"]["terminal"] == 1
    assert report["twaps"]["by_status"] == {"finished": 1}
    assert report["twaps"]["slice_fills"] == 1
    assert report["twaps"]["unmatched_terminal_updates"] == 0
    assert report["fills"]["twap_slice_notional_usd"] == 20.0
    assert report["fills"]["twap_slice_notional_observations"] == 1
    assert report["account_context"]["twap_state_refreshes"] == 1
    assert report["counts"]["by_fact_type"] == {
        "twap_activated_seen": 1,
        "twap_slice_fill_seen": 1,
        "twap_state_refresh_seen": 1,
        "twap_terminal_seen": 1,
    }


def test_state_tracker_reports_recovery_and_equity_confidence(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            scorecard_row(
                row_id="snapshot-1",
                sort_ts_ms=1_000,
                category="state_refresh",
                source_action="rest_snapshot",
                classifier_decision="state_refresh_only",
                event_type="snapshot",
                subtype="rest_snapshot",
                confidence="high",
                metadata={
                    "account_value_usd": 100.5,
                    "error_count": 0,
                    "ok_count": 6,
                    "position_coins": ["ETH"],
                    "position_count": 1,
                    "position_leverage_by_coin": {"ETH": "isolated:3"},
                    "position_leverage_counts": {"isolated:3": 1},
                    "position_margin_used_observations": 1,
                    "position_margin_used_usd": 25,
                    "position_notional_observations": 1,
                    "position_notional_usd": 75,
                    "position_unrealized_pnl_observations": 1,
                    "position_unrealized_pnl_usd": -1.5,
                },
            ),
            scorecard_row(
                row_id="funding",
                sort_ts_ms=1_100,
                category="account_state",
                source_action="funding_update",
                classifier_decision="equity_context_update",
                event_type="funding",
                subtype="user:funding",
                metadata={"coin": "BTC", "usdc": "0.01"},
            ),
            scorecard_row(
                row_id="ledger",
                sort_ts_ms=1_150,
                category="account_state",
                source_action="ledger_deposit",
                classifier_decision="equity_context_update",
                event_type="ledger",
                subtype="user:ledger",
                metadata={"ledger_type": "deposit", "to_perp": True, "usdc": "2.50"},
            ),
            scorecard_row(
                row_id="degraded",
                sort_ts_ms=1_200,
                category="recovery",
                source_action="stream_degraded",
                classifier_decision="pause_live_stream_hints",
                event_type="recovery",
                subtype="stream_degraded",
                confidence="high",
                metadata={"reason": "websocket_error"},
            ),
            scorecard_row(
                row_id="recovered",
                sort_ts_ms=1_300,
                category="recovery",
                source_action="reconnect_recovered",
                classifier_decision="requires_rest_backfill_and_reconcile",
                event_type="recovery",
                subtype="reconnect_recovered",
                confidence="high",
                metadata={"gap_ms": 500},
            ),
            scorecard_row(
                row_id="snapshot-2",
                sort_ts_ms=1_400,
                category="state_refresh",
                source_action="rest_snapshot",
                classifier_decision="state_refresh_only",
                event_type="snapshot",
                subtype="rest_snapshot",
                confidence="high",
                metadata={
                    "account_value_usd": 101.25,
                    "error_count": 0,
                    "ok_count": 6,
                    "position_coins": ["BTC"],
                    "position_count": 1,
                    "position_leverage_by_coin": {"BTC": "cross:5"},
                    "position_leverage_counts": {"cross:5": 1},
                    "position_margin_used_observations": 1,
                    "position_margin_used_usd": 200,
                    "position_notional_observations": 1,
                    "position_notional_usd": 1000,
                    "position_unrealized_pnl_observations": 1,
                    "position_unrealized_pnl_usd": 12.5,
                },
            ),
        ],
    )

    report = track_replay_state.track_replay_state(rows_path, facts_out=facts_out)

    assert report["account_context"]["latest_account_value_usd"] == 101.25
    assert report["account_context"]["equity_confidence"] == "high"
    assert report["account_context"]["position_snapshot_observations"] == 2
    assert report["account_context"]["latest_position_count"] == 1
    assert report["account_context"]["latest_position_coins"] == ["BTC"]
    assert report["account_context"]["latest_position_leverage_by_coin"] == {"BTC": "cross:5"}
    assert report["account_context"]["latest_position_leverage_counts"] == {"cross:5": 1}
    assert report["account_context"]["latest_position_notional_usd"] == 1000.0
    assert report["account_context"]["latest_position_notional_observations"] == 1
    assert report["account_context"]["latest_position_margin_used_usd"] == 200.0
    assert report["account_context"]["latest_position_margin_used_observations"] == 1
    assert report["account_context"]["latest_position_unrealized_pnl_usd"] == 12.5
    assert report["account_context"]["latest_position_unrealized_pnl_observations"] == 1
    assert report["account_context"]["funding_updates_by_coin"] == {"BTC": 1}
    assert report["account_context"]["funding_amount_usd"] == 0.01
    assert report["account_context"]["funding_amount_observations"] == 1
    assert report["account_context"]["ledger_updates_by_type"] == {"deposit": 1}
    assert report["account_context"]["ledger_amount_usd"] == 2.5
    assert report["account_context"]["ledger_amount_observations"] == 1
    assert report["account_context"]["net_account_context_amount_usd"] == 2.51
    assert report["recovery"]["stream_state"] == "recovery_required"
    assert report["recovery"]["live_stream_hints_allowed"] is False
    assert report["recovery"]["pending_rest_backfill"] is True
    assert report["recovery"]["pending_reconcile"] is True
    assert report["recovery"]["state_refreshes_after_reconnect"] == 1
    assert report["recovery"]["windows"] == [
        {
            "complete": False,
            "degraded_row_id": "degraded",
            "degraded_ts_ms": 1_200,
            "explicit_reconcile_complete": False,
            "explicit_rest_backfill_complete": False,
            "first_post_reconnect_rest_snapshot_ms": 1_400,
            "first_post_reconnect_state_refresh_ms": 1_400,
            "gap_ms": 500,
            "live_stream_hints_allowed_after_window": False,
            "missing_requirements": [
                "explicit_rest_backfill_complete",
                "explicit_reconcile_complete",
                "live_stream_hints_allowed_after_window",
            ],
            "post_reconnect_account_context_rows": 0,
            "post_reconnect_rest_snapshots": 1,
            "post_reconnect_source_actions": 0,
            "post_reconnect_state_refreshes": 1,
            "reconnect_row_id": "recovered",
            "reconnected_ts_ms": 1_300,
            "sequence": 1,
            "source_error": "unknown",
            "status": "requires_rest_backfill_and_reconcile",
        }
    ]
    assert report["account_context"]["confidence_downgrade_reasons"] == {
        "funding_update_requires_fresh_snapshot": 1,
        "ledger_deposit_requires_fresh_snapshot": 1,
        "reconnect_requires_rest_backfill": 1,
        "stream_degraded": 1,
    }
    facts = [json.loads(line) for line in facts_out.read_text(encoding="utf-8").splitlines()]
    funding_fact = next(
        fact
        for fact in facts
        if fact["fact_type"] == "account_context_update"
        and fact["metadata"]["source_action"] == "funding_update"
    )
    ledger_fact = next(
        fact
        for fact in facts
        if fact["fact_type"] == "account_context_update"
        and fact["metadata"]["source_action"] == "ledger_deposit"
    )
    snapshot_fact = next(
        fact
        for fact in facts
        if fact["fact_type"] == "rest_snapshot_seen" and fact["row_id"] == "snapshot-2"
    )
    assert funding_fact["metadata"]["funding_amount_usd"] == 0.01
    assert ledger_fact["metadata"]["ledger_amount_usd"] == 2.5
    assert ledger_fact["metadata"]["to_perp"] is True
    assert snapshot_fact["metadata"]["position_count"] == 1
    assert snapshot_fact["metadata"]["position_leverage_by_coin"] == {"BTC": "cross:5"}
    assert snapshot_fact["metadata"]["position_margin_used_usd"] == 200.0
