from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "build_shadow_slot_state.py"
SPEC = importlib.util.spec_from_file_location("build_shadow_slot_state", SCRIPT_PATH)
assert SPEC is not None
build_shadow_slot_state = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = build_shadow_slot_state
SPEC.loader.exec_module(build_shadow_slot_state)


SOURCE = "0x1111111111111111111111111111111111111111"
OTHER_SOURCE = "0x2222222222222222222222222222222222222222"
SUBACCOUNT = "0xf000000000000000000000000000000000000001"


def slot_plan_report(*, valid: bool = True, equity_confidence_policy: str = "block_low") -> dict:
    return {
        "exchange_touched": False,
        "read_only": True,
        "slots": [
            {
                "enabled": False,
                "equity_confidence_policy": equity_confidence_policy,
                "sizing_policy": "pure_compound",
                "slot": "slot-1",
                "source_address": SOURCE,
                "subaccount_address": SUBACCOUNT,
                "subaccount_verified": False,
            },
            {
                "enabled": False,
                "equity_confidence_policy": equity_confidence_policy,
                "sizing_policy": "fixed_risk_budget",
                "slot": "slot-2",
                "source_address": OTHER_SOURCE,
                "subaccount_address": "0xf000000000000000000000000000000000000002",
                "subaccount_verified": False,
            },
        ],
        "valid": valid,
        "warnings": ["slot slot-1 subaccount is not verified through UI/API evidence"],
    }


def target_snapshot() -> dict:
    return {
        "exchange_touched": False,
        "policies": {
            "pure_compound": {
                "blocked_actions": 0,
                "dust_actions": 2,
                "executed_actions": 1,
                "policy": "pure_compound",
                "skipped_actions": 0,
                "source_rows": 3,
                "target_positions": [
                    {
                        "blocked_actions": 0,
                        "coin": "BTC",
                        "decision_counts": {"would_send_now": 1},
                        "dust_actions": 0,
                        "executed_actions": 1,
                        "side": "long",
                        "signed_target_notional_usd": "10",
                        "skipped_actions": 0,
                        "source_rows": 1,
                    },
                    {
                        "blocked_actions": 0,
                        "coin": "ETH",
                        "decision_counts": {"would_accumulate_dust": 2},
                        "dust_actions": 2,
                        "executed_actions": 0,
                        "side": "flat",
                        "signed_target_notional_usd": "0",
                        "skipped_actions": 0,
                        "source_rows": 2,
                    },
                ],
            },
            "fixed_risk_budget": {
                "blocked_actions": 0,
                "dust_actions": 0,
                "executed_actions": 1,
                "policy": "fixed_risk_budget",
                "skipped_actions": 0,
                "source_rows": 1,
                "target_positions": [
                    {
                        "coin": "BTC",
                        "executed_actions": 1,
                        "signed_target_notional_usd": "5",
                    }
                ],
            },
        },
        "read_only": True,
        "slots": {"replay-slot": 10},
        "source_addresses": {SOURCE: 10},
    }


def follower_snapshot(*, recovery_complete: bool = True, positions=None, open_orders=None) -> dict:
    normalized_positions = positions or []
    normalized_orders = open_orders or []
    return {
        "account_value_usd": "50",
        "snapshot_normalizer_version": 2,
        "request_status": {
            "clearinghouseState_ok": True,
            "openOrders_ok": True,
        },
        "warnings": [],
        "exchange_touched": False,
        "follower_subaccount": "shadow-synthetic",
        "open_orders": normalized_orders,
        "positions": normalized_positions,
        "counts": {
            "positions": len(normalized_positions),
            "open_orders": len(normalized_orders),
        },
        "recovery": {
            "follower_refresh_complete": recovery_complete,
            "reconcile_complete": recovery_complete,
            "source_backfill_complete": recovery_complete,
        },
        "slot": "replay-slot",
    }


def test_recovery_state_rejects_legacy_fail_open_snapshot():
    legacy = follower_snapshot(recovery_complete=True)
    legacy["snapshot_normalizer_version"] = 1

    recovery = build_shadow_slot_state.recovery_state(legacy)

    assert recovery["complete"] is False
    assert recovery["missing_requirements"] == ["snapshot_normalizer_supported"]


def test_slot_state_input_rejects_legacy_recovery_evidence_version():
    legacy_evidence = recovery_evidence_report(ready=True)
    legacy_evidence["recovery_evidence_report_version"] = 1

    blockers = build_shadow_slot_state.input_blocker_list(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(),
        recovery_evidence_report=legacy_evidence,
    )

    assert any("recovery evidence report version is unsupported" in item for item in blockers)


def source_state_report(
    *,
    pending_recovery: bool = True,
    equity_confidence: str = "high",
    latest_account_value_usd=1000,
) -> dict:
    return {
        "account_context": {
            "account_value_observations": 2,
            "account_state_updates_by_action": {"funding_update": 1, "ledger_send": 1},
            "confidence_downgrade_reasons": {"stream_degraded": 1},
            "equity_confidence": equity_confidence,
            "equity_confidence_reason": "latest REST snapshot carried account_value_usd",
            "funding_amount_observations": 1,
            "funding_amount_usd": "-0.25",
            "funding_updates_by_coin": {"BTC": 1},
            "latest_account_value_ts_ms": 123456,
            "latest_account_value_usd": latest_account_value_usd,
            "ledger_amount_observations": 1,
            "ledger_amount_usd": "5.50",
            "ledger_updates_by_type": {"send": 1},
            "latest_position_coins": ["BTC"],
            "latest_position_count": 1,
            "latest_position_leverage_by_coin": {"BTC": "cross:5"},
            "latest_position_leverage_counts": {"cross:5": 1},
            "latest_position_margin_used_observations": 1,
            "latest_position_margin_used_usd": "200",
            "latest_position_notional_observations": 1,
            "latest_position_notional_usd": "1000",
            "latest_position_unrealized_pnl_observations": 1,
            "latest_position_unrealized_pnl_usd": "12.5",
            "net_account_context_amount_usd": "5.25",
            "position_snapshot_observations": 2,
            "rest_snapshots": 2,
            "twap_state_refreshes": 3,
        },
        "counts": {"by_address": {SOURCE: 5}},
        "exchange_touched": False,
        "fills": {
            "source_fill_notional_observations": 1,
            "source_fill_notional_usd": 125,
            "source_fills": 1,
            "twap_slice_fills": 2,
            "twap_slice_notional_observations": 2,
            "twap_slice_notional_usd": 250,
        },
        "input_rows_seen": 5,
        "orders": {
            "by_status": {"filled": 2},
            "open": 0,
            "open_order_samples": [],
            "seen": 2,
            "terminal": 2,
            "unmatched_terminal_updates": 0,
        },
        "policy_neutral_shadow": {
            "facts_emitted": 5,
            "fill_price_size_available": True,
            "latest_source_account_value_available": True,
            "sizing_policy_applied": False,
        },
        "read_only": True,
        "recovery": {
            "degraded_events": 1,
            "live_stream_hints_allowed": not pending_recovery,
            "pending_reconcile": pending_recovery,
            "pending_rest_backfill": pending_recovery,
            "reconnect_recovered_events": 1,
            "state_refreshes_after_reconnect": 4,
            "stream_state": "recovery_required" if pending_recovery else "trusted",
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
                    "post_reconnect_source_actions": 0,
                    "post_reconnect_state_refreshes": 1,
                    "reconnected_ts_ms": 1_200,
                    "sequence": 1,
                    "status": "requires_rest_backfill_and_reconcile",
                },
                {
                    "complete": True,
                    "degraded_ts_ms": 2_000,
                    "first_post_reconnect_rest_snapshot_ms": 2_400,
                    "first_post_reconnect_state_refresh_ms": 2_300,
                    "gap_ms": 300,
                    "missing_requirements": [],
                    "post_reconnect_account_context_rows": 3,
                    "post_reconnect_rest_snapshots": 2,
                    "post_reconnect_source_actions": 4,
                    "post_reconnect_state_refreshes": 2,
                    "reconnected_ts_ms": 2_300,
                    "sequence": 2,
                    "status": "complete",
                },
            ],
        },
        "rows_processed": 5,
        "twaps": {
            "active": 0,
            "active_samples": [],
            "by_status": {"finished": 1},
            "error_samples": [],
            "seen": 1,
            "slice_fills": 2,
            "terminal": 1,
            "unmatched_terminal_updates": 0,
        },
    }


def recovery_evidence_report(*, ready: bool = True, source: str = SOURCE) -> dict:
    return {
        "blockers": [] if ready else ["window_1_complete: proof window is missing"],
        "exchange_touched": False,
        "read_only": True,
        "recovery_evidence_report_version": 2,
        "recovery_evidence_ready": ready,
        "source_state": {"source_address": source},
        "status": "recovery_evidence_ready" if ready else "blocked",
        "windows": [
            {
                "complete": ready,
                "missing_requirements": [] if ready else ["proof window is missing"],
                "sequence": 1,
            }
        ],
    }


def source_state_facts() -> list[dict]:
    return [
        {
            "address": SOURCE,
            "entity_id": "oid:1",
            "fact_type": "order_open_seen",
            "metadata": {
                "coin": "BTC",
                "opened_count": 1,
                "order_key": "oid:1",
                "side": "B",
                "source_notional_usd": "100",
                "status": "open",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 1_000,
        },
        {
            "address": SOURCE,
            "entity_id": "oid:1",
            "fact_type": "order_terminal_seen",
            "metadata": {
                "coin": "BTC",
                "order_key": "oid:1",
                "side": "B",
                "source_notional_usd": "100",
                "status": "filled",
                "terminal_count": 1,
            },
            "slot": "replay-slot",
            "sort_ts_ms": 1_100,
        },
        {
            "address": SOURCE,
            "entity_id": "twap:7",
            "fact_type": "twap_activated_seen",
            "metadata": {
                "activated_count": 1,
                "coin": "ETH",
                "reduce_only": True,
                "side": "A",
                "status": "active",
                "twap_key": "twap:7",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 2_000,
        },
        {
            "address": SOURCE,
            "entity_id": "twap:7",
            "fact_type": "twap_slice_fill_seen",
            "metadata": {
                "coin": "ETH",
                "reduce_only": True,
                "side": "A",
                "slice_fill_count": 3,
                "closed_pnl_usd": "-3",
                "fee_usd": "0.25",
                "source_notional_usd": "40",
                "status": "active",
                "twap_key": "twap:7",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 2_100,
        },
        {
            "address": SOURCE,
            "entity_id": "twap:7",
            "fact_type": "twap_terminal_seen",
            "metadata": {
                "coin": "ETH",
                "reduce_only": True,
                "side": "A",
                "slice_fill_count": 3,
                "status": "finished",
                "terminal_count": 1,
                "twap_key": "twap:7",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 2_200,
        },
    ]


def source_cashflow_facts() -> list[dict]:
    return [
        *source_state_facts(),
        {
            "address": SOURCE,
            "entity_id": "fill:1",
            "fact_type": "source_fill_seen",
            "metadata": {
                "closed_pnl_usd": "4",
                "coin": "BTC",
                "fee_usd": "0.5",
                "side": "B",
                "source_notional_usd": "100",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 3_000,
        },
        {
            "address": SOURCE,
            "entity_id": SOURCE,
            "fact_type": "account_context_update",
            "metadata": {
                "coin": "BTC",
                "ledger_type": "unknown",
                "source_action": "funding_update",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 3_100,
        },
        {
            "address": SOURCE,
            "entity_id": SOURCE,
            "fact_type": "account_context_update",
            "metadata": {
                "coin": "unknown",
                "ledger_type": "send",
                "source_action": "ledger_send",
            },
            "slot": "replay-slot",
            "sort_ts_ms": 3_200,
        },
    ]


def source_cashflow_facts_with_amounts() -> list[dict]:
    facts = source_cashflow_facts()
    for fact in facts:
        metadata = fact.get("metadata", {})
        if metadata.get("source_action") == "funding_update":
            metadata["funding_amount_usd"] = "-0.25"
            metadata["usdc"] = "-0.25"
        if metadata.get("source_action") == "ledger_send":
            metadata["ledger_amount_usd"] = "5.50"
            metadata["to_perp"] = True
            metadata["usdc"] = "5.50"
    return facts


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")


def test_slot_state_combines_plan_targets_and_follower_truth():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(),
    )

    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["valid"] is True
    assert report["counts"]["slot_statuses"] == {"analysis_only": 1, "blocked": 1}
    slot_1 = report["slots"][0]
    assert slot_1["slot"] == "slot-1"
    assert slot_1["target_matches_slot"] is True
    assert slot_1["execution_ready"] is False
    assert slot_1["follower_snapshot_verified"] is False
    assert slot_1["follower_snapshot_verification"] == {
        "declared_expected_follower_subaccount": None,
        "expected_subaccount": SUBACCOUNT,
        "observed_address": None,
        "observed_address_source": None,
        "reason": "follower snapshot lacks address verification",
        "verified": False,
    }
    assert "subaccount is not verified" in " ".join(slot_1["warnings"])
    assert "follower snapshot address is not verified" in " ".join(slot_1["warnings"])
    policy = slot_1["policies"]["pure_compound"]
    assert policy["decision_counts"] == {"drift_detected": 1, "in_sync": 1}
    btc = {row["coin"]: row for row in policy["position_states"]}["BTC"]
    assert btc["decision"] == "drift_detected"
    assert btc["repair_intent"] == "would_open_or_place_follower_exposure"
    assert btc["target"]["signed_target_notional_usd"] == "10.00000000"
    slot_2 = report["slots"][1]
    assert "no target snapshot matched" in " ".join(slot_2["blockers"])


def test_slot_state_blocks_repairs_when_recovery_is_incomplete():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=False),
    )

    slot_1 = report["slots"][0]
    policy = slot_1["policies"]["pure_compound"]
    assert slot_1["status"] == "blocked"
    assert policy["decision_counts"] == {"blocked_recovery_incomplete": 2}
    assert policy["repair_intent_counts"] == {
        "do_not_repair_until_recovery_complete": 2,
    }
    recovery_gate = slot_1["recovery_gate"]
    assert recovery_gate["decision"] == "blocked_recovery_incomplete"
    assert recovery_gate["repair_intents_actionable"] is False
    assert "follower.source_backfill_complete" in recovery_gate["missing_requirements"]
    assert "follower.follower_refresh_complete" in recovery_gate["missing_requirements"]
    assert "follower.reconcile_complete" in recovery_gate["missing_requirements"]
    assert "source.source_state_report_attached" in recovery_gate["missing_requirements"]


def test_slot_state_treats_open_orders_as_projected_exposure():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(
            positions=[{"coin": "BTC", "signed_notional_usd": "7"}],
            open_orders=[{"coin": "BTC", "notional_usd": "3", "side": "B"}],
        ),
    )

    policy = report["slots"][0]["policies"]["pure_compound"]
    btc = {row["coin"]: row for row in policy["position_states"]}["BTC"]
    assert btc["decision"] == "projected_in_sync_position_pending"
    assert btc["repair_intent"] == "wait_for_open_orders_or_reconcile"
    assert btc["follower"]["projected_notional_usd"] == "10.00000000"


def test_slot_state_attaches_source_state_and_blocks_pending_source_recovery():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
    )

    slot_1 = report["slots"][0]
    assert slot_1["status"] == "blocked"
    assert "source state recovery completion is not proven" in slot_1["blockers"]
    source_state = slot_1["source_state"]
    assert source_state["attached"] is True
    assert source_state["matched"] is True
    assert source_state["recovery_complete"] is False
    assert source_state["recovery_requirements"] == {
        "complete": False,
        "live_stream_hints_allowed": False,
        "missing_requirements": [
            "pending_rest_backfill_cleared",
            "pending_reconcile_cleared",
            "live_stream_hints_allowed",
        ],
        "pending_reconcile_cleared": False,
        "pending_rest_backfill_cleared": False,
        "recovery_evidence_applied": False,
        "source_state_report_attached": True,
        "source_state_report_matched": True,
    }
    recovery_gate = slot_1["recovery_gate"]
    assert recovery_gate["complete"] is False
    assert recovery_gate["repair_intents_actionable"] is False
    assert recovery_gate["missing_requirements"] == [
        "source.pending_rest_backfill_cleared",
        "source.pending_reconcile_cleared",
        "source.live_stream_hints_allowed",
    ]
    assert recovery_gate["blocker_reasons"] == [
        "source state still requires REST backfill after reconnect",
        "source state still requires reconciliation after reconnect",
        "source live stream hints are not trusted after reconnect",
    ]
    summary = source_state["summary"]
    assert summary["orders"]["seen"] == 2
    assert summary["orders"]["by_status"] == {"filled": 2}
    assert summary["twaps"]["seen"] == 1
    assert summary["twaps"]["slice_fills"] == 2
    assert summary["fills"]["source_fill_notional_usd"] == "125.00000000"
    assert summary["fills"]["twap_slice_notional_usd"] == "250.00000000"
    assert summary["account_context"]["latest_account_value_usd"] == "1000.00000000"
    assert summary["account_context"]["position_snapshot_observations"] == 2
    assert summary["account_context"]["latest_position_count"] == 1
    assert summary["account_context"]["latest_position_coins"] == ["BTC"]
    assert summary["account_context"]["latest_position_leverage_by_coin"] == {"BTC": "cross:5"}
    assert summary["account_context"]["latest_position_leverage_counts"] == {"cross:5": 1}
    assert summary["account_context"]["latest_position_notional_usd"] == "1000.00000000"
    assert summary["account_context"]["latest_position_notional_observations"] == 1
    assert summary["account_context"]["latest_position_margin_used_usd"] == "200.00000000"
    assert summary["account_context"]["latest_position_margin_used_observations"] == 1
    assert summary["account_context"]["latest_position_unrealized_pnl_usd"] == "12.50000000"
    assert summary["account_context"]["latest_position_unrealized_pnl_observations"] == 1
    assert summary["account_context"]["funding_amount_usd"] == "-0.25000000"
    assert summary["account_context"]["funding_amount_observations"] == 1
    assert summary["account_context"]["ledger_amount_usd"] == "5.50000000"
    assert summary["account_context"]["ledger_amount_observations"] == 1
    assert summary["account_context"]["net_account_context_amount_usd"] == "5.25000000"
    assert summary["account_context"]["account_state_updates_by_action"] == {
        "funding_update": 1,
        "ledger_send": 1,
    }
    assert summary["account_context"]["ledger_updates_by_type"] == {"send": 1}
    assert summary["account_context"]["funding_updates_by_coin"] == {"BTC": 1}
    assert summary["recovery"]["pending_rest_backfill"] is True
    assert summary["recovery"]["windows"] == {
        "complete": 1,
        "count": 2,
        "incomplete": 1,
        "max_gap_ms": 300,
        "missing_requirements": {
            "explicit_reconcile_complete": 1,
            "explicit_rest_backfill_complete": 1,
            "live_stream_hints_allowed_after_window": 1,
        },
        "sample": [
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
                "post_reconnect_source_actions": 0,
                "post_reconnect_state_refreshes": 1,
                "reconnected_ts_ms": 1_200,
                "sequence": 1,
                "status": "requires_rest_backfill_and_reconcile",
            },
            {
                "complete": True,
                "degraded_ts_ms": 2_000,
                "first_post_reconnect_rest_snapshot_ms": 2_400,
                "first_post_reconnect_state_refresh_ms": 2_300,
                "gap_ms": 300,
                "missing_requirements": [],
                "post_reconnect_account_context_rows": 3,
                "post_reconnect_rest_snapshots": 2,
                "post_reconnect_source_actions": 4,
                "post_reconnect_state_refreshes": 2,
                "reconnected_ts_ms": 2_300,
                "sequence": 2,
                "status": "complete",
            },
        ],
    }
    assert summary["policy_neutral_shadow"]["latest_source_account_value_available"] is True
    denominator = slot_1["sizing_context"]["source_denominator"]
    assert denominator["source_account_value_usd"] == "1000.00000000"
    assert denominator["decision"] == "use_latest_source_account_value"
    assert denominator["equity_confidence"] == "high"
    assert denominator["equity_confidence_policy"] == "block_low"


def test_blocked_recovery_evidence_does_not_clear_source_recovery():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        recovery_evidence_report=recovery_evidence_report(ready=False),
    )

    slot_1 = report["slots"][0]
    source_state = slot_1["source_state"]
    assert source_state["recovery_complete"] is False
    assert source_state["recovery_evidence"]["attached"] is True
    assert source_state["recovery_evidence"]["applied_to_source_recovery"] is False
    assert source_state["recovery_evidence"]["reason"] == "recovery evidence report is not ready"
    assert source_state["recovery_requirements"]["missing_requirements"] == [
        "pending_rest_backfill_cleared",
        "pending_reconcile_cleared",
        "live_stream_hints_allowed",
    ]
    assert slot_1["recovery_gate"]["missing_requirements"] == [
        "source.pending_rest_backfill_cleared",
        "source.pending_reconcile_cleared",
        "source.live_stream_hints_allowed",
    ]


def test_ready_source_matched_recovery_evidence_clears_source_recovery():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        recovery_evidence_report=recovery_evidence_report(ready=True),
    )

    slot_1 = report["slots"][0]
    source_state = slot_1["source_state"]
    assert source_state["recovery_complete"] is True
    assert source_state["recovery_evidence"]["attached"] is True
    assert source_state["recovery_evidence"]["source_matches"] is True
    assert source_state["recovery_evidence"]["applied_to_source_recovery"] is True
    assert (
        source_state["recovery_evidence"]["reason"]
        == "recovery evidence is ready and source-matched"
    )
    assert source_state["recovery_requirements"]["recovery_evidence_applied"] is True
    assert source_state["recovery_requirements"]["missing_requirements"] == []
    assert slot_1["recovery_gate"]["source"]["complete"] is True


def test_ready_mismatched_recovery_evidence_does_not_clear_source_recovery():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        recovery_evidence_report=recovery_evidence_report(ready=True, source=OTHER_SOURCE),
    )

    slot_1 = report["slots"][0]
    source_state = slot_1["source_state"]
    assert source_state["recovery_complete"] is False
    assert source_state["recovery_evidence"]["source_matches"] is False
    assert source_state["recovery_evidence"]["applied_to_source_recovery"] is False
    assert (
        source_state["recovery_evidence"]["reason"]
        == "recovery evidence source does not match this slot"
    )
    assert source_state["recovery_requirements"]["missing_requirements"] == [
        "pending_rest_backfill_cleared",
        "pending_reconcile_cleared",
        "live_stream_hints_allowed",
    ]


def test_slot_state_marks_repair_intents_actionable_only_when_recovery_is_complete():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=False),
        source_state_facts=source_state_facts(),
    )

    slot_1 = report["slots"][0]
    recovery_gate = slot_1["recovery_gate"]
    assert slot_1["execution_ready"] is False
    assert recovery_gate["complete"] is True
    assert recovery_gate["decision"] == "recovery_complete"
    assert recovery_gate["repair_intents_actionable"] is True
    assert recovery_gate["missing_requirements"] == []
    assert "exchange execution still requires" in recovery_gate["actionability_scope"]
    assert recovery_gate["source"] == {
        "complete": True,
        "live_stream_hints_allowed": True,
        "missing_requirements": [],
        "pending_reconcile_cleared": True,
        "pending_rest_backfill_cleared": True,
        "recovery_evidence_applied": False,
        "source_state_report_attached": True,
        "source_state_report_matched": True,
    }
    assert report["counts"]["recovery_gate_decisions"] == {
        "blocked_recovery_incomplete": 1,
        "recovery_complete": 1,
    }


def test_slot_state_materializes_target_order_and_twap_intents():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        source_state_facts=source_state_facts(),
    )

    intents = report["slots"][0]["target_intents"]
    assert intents["attached"] is True
    assert intents["matched"] is True
    assert intents["matched_fact_rows"] == 5
    assert intents["counts"]["orders"] == 1
    assert intents["counts"]["twaps"] == 1
    assert intents["counts"]["decisions"] == {"blocked_source_recovery_pending": 2}
    assert intents["counts"]["sizing_statuses"] == {"computed": 2}
    order = intents["orders"][0]
    assert order["target_action"] == "validate_terminal_order_and_position"
    assert order["decision"] == "blocked_source_recovery_pending"
    assert order["source_status"] == "filled"
    assert order["source_notional_usd"] == "100.00000000"
    assert order["sizing"]["status"] == "computed"
    assert order["sizing"]["policy"] == "pure_compound"
    assert order["sizing"]["source_account_value_usd"] == "1000.00000000"
    assert order["sizing"]["denominator"]["decision"] == "use_latest_source_account_value"
    assert order["sizing"]["denominator"]["usable"] is True
    assert order["sizing"]["sizing_equity_usd"] == "50.00000000"
    assert order["sizing"]["copy_ratio"] == "0.05000000"
    assert order["sizing"]["target_notional_usd"] == "5.00000000"
    assert order["sizing"]["signed_target_notional_usd"] == "5.00000000"
    assert order["source_fact_types"] == {"order_open_seen": 1, "order_terminal_seen": 1}
    twap = intents["twaps"][0]
    assert twap["target_action"] == "reconcile_or_cancel_mapped_twap"
    assert twap["decision"] == "blocked_source_recovery_pending"
    assert twap["source_status"] == "finished"
    assert twap["source_notional_usd"] == "40.00000000"
    assert twap["source_notional_observations"] == 1
    assert twap["source_notional_basis"] == "twap_slice_fill_seen_sum"
    assert twap["sizing"]["status"] == "computed"
    assert twap["sizing"]["missing_inputs"] == []
    assert twap["sizing"]["source_account_value_usd"] == "1000.00000000"
    assert twap["sizing"]["denominator"]["decision"] == "use_latest_source_account_value"
    assert twap["sizing"]["sizing_equity_usd"] == "50.00000000"
    assert twap["sizing"]["target_notional_usd"] == "2.00000000"
    assert twap["sizing"]["signed_target_notional_usd"] == "-2.00000000"
    assert twap["slice_fill_count"] == 3
    assert twap["source_fact_types"] == {
        "twap_activated_seen": 1,
        "twap_slice_fill_seen": 1,
        "twap_terminal_seen": 1,
    }


def test_slot_state_blocks_intent_sizing_when_denominator_confidence_is_low():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(equity_confidence_policy="block_low"),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(
            pending_recovery=False,
            equity_confidence="low",
        ),
        source_state_facts=source_state_facts(),
    )

    intents = report["slots"][0]["target_intents"]
    assert intents["counts"]["sizing_statuses"] == {"blocked_low_equity_confidence": 2}
    order_sizing = intents["orders"][0]["sizing"]
    assert order_sizing["status"] == "blocked_low_equity_confidence"
    assert order_sizing["missing_inputs"] == []
    assert order_sizing["blockers"] == ["blocked_low_equity_confidence"]
    assert order_sizing["copy_ratio"] is None
    assert order_sizing["target_notional_usd"] is None
    assert order_sizing["denominator"]["usable"] is False
    assert order_sizing["denominator"]["decision"] == "blocked_low_equity_confidence"
    assert (
        order_sizing["denominator"]["fallback_decision"]
        == "fallback_not_used_policy_blocks_low_confidence"
    )


def test_slot_state_labels_degraded_low_confidence_denominator_sizing():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(equity_confidence_policy="degrade_low"),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(
            pending_recovery=False,
            equity_confidence="low",
        ),
        source_state_facts=source_state_facts(),
    )

    intents = report["slots"][0]["target_intents"]
    assert intents["counts"]["sizing_statuses"] == {"computed_low_confidence": 2}
    order_sizing = intents["orders"][0]["sizing"]
    assert order_sizing["status"] == "computed_low_confidence"
    assert order_sizing["copy_ratio"] == "0.05000000"
    assert order_sizing["target_notional_usd"] == "5.00000000"
    assert order_sizing["denominator"]["usable"] is True
    assert order_sizing["denominator"]["decision"] == "degraded_low_equity_confidence"
    assert (
        order_sizing["denominator"]["fallback_decision"]
        == "using_latest_account_value_with_low_confidence"
    )


def test_slot_state_reports_fail_closed_denominator_fallback_candidates():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(
            pending_recovery=False,
            latest_account_value_usd=None,
        ),
        source_state_facts=source_state_facts(),
    )

    order_sizing = report["slots"][0]["target_intents"]["orders"][0]["sizing"]
    denominator = order_sizing["denominator"]
    assert order_sizing["status"] == "missing_inputs"
    assert order_sizing["missing_inputs"] == ["source_account_value_usd"]
    assert order_sizing["copy_ratio"] is None
    assert denominator["usable"] is False
    assert denominator["decision"] == "missing_source_account_value"
    assert denominator["fallback_required"] is True
    assert (
        denominator["fallback_decision"] == "fallback_candidates_available_but_not_used_fail_closed"
    )
    assert denominator["fallback_candidate_count"] == 2
    assert denominator["fallback_candidates"] == [
        {
            "name": "latest_position_notional_usd",
            "observations": 1,
            "reason": "position notional is exposure, not source equity",
            "source": "source_state.account_context.latest_position_notional_usd",
            "usable": False,
            "value_usd": "1000.00000000",
        },
        {
            "name": "latest_position_margin_used_usd",
            "observations": 1,
            "reason": (
                "margin used excludes free collateral and cannot safely replace account value"
            ),
            "source": "source_state.account_context.latest_position_margin_used_usd",
            "usable": False,
            "value_usd": "200.00000000",
        },
    ]


def test_slot_state_summarizes_source_cashflows_without_inventing_ledger_amounts():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        source_state_facts=source_cashflow_facts(),
    )

    cashflows = report["slots"][0]["source_cashflows"]
    assert cashflows["attached"] is True
    assert cashflows["matched"] is True
    assert cashflows["matched_fact_rows"] == 8
    assert cashflows["relevant_fact_rows"] == 4
    fills = cashflows["fill_cashflows"]
    assert fills["fact_rows"] == 2
    assert fills["by_fact_type"] == {"source_fill_seen": 1, "twap_slice_fill_seen": 1}
    assert fills["by_coin"] == {"BTC": 1, "ETH": 1}
    assert fills["source_notional_usd"] == "140.00000000"
    assert fills["source_notional_observations"] == 2
    assert fills["closed_pnl_usd"] == "1.00000000"
    assert fills["closed_pnl_observations"] == 2
    assert fills["fee_usd"] == "0.75000000"
    assert fills["fee_observations"] == 2
    assert fills["closed_pnl_less_recorded_fee_usd"] == "0.25000000"
    account_events = cashflows["account_context_events"]
    assert account_events["fact_rows"] == 2
    assert account_events["by_source_action"] == {
        "funding_update": 1,
        "ledger_send": 1,
    }
    assert account_events["funding_updates_by_coin"] == {"BTC": 1}
    assert account_events["ledger_updates_by_type"] == {"send": 1}
    assert account_events["ledger_amount_usd"] == "0.00000000"
    assert account_events["ledger_amount_observations"] == 0
    assert account_events["funding_amount_usd"] == "0.00000000"
    assert account_events["funding_amount_observations"] == 0
    assert account_events["net_account_context_amount_usd"] == "0.00000000"
    assert account_events["ledger_amounts_available"] is False
    assert account_events["funding_amounts_available"] is False
    assert (
        account_events["amounts_unavailable_reason"]
        == "ledger/funding amount fields were not present in matched source-state facts"
    )


def test_slot_state_summarizes_source_cashflow_amounts_when_present():
    report = build_shadow_slot_state.build_shadow_slot_state_payload(
        slot_plan_report(),
        target_snapshot(),
        follower_snapshot(recovery_complete=True),
        source_state_report=source_state_report(pending_recovery=True),
        source_state_facts=source_cashflow_facts_with_amounts(),
    )

    cashflows = report["slots"][0]["source_cashflows"]
    account_events = cashflows["account_context_events"]
    assert account_events["fact_rows"] == 2
    assert account_events["ledger_amount_usd"] == "5.50000000"
    assert account_events["ledger_amount_observations"] == 1
    assert account_events["funding_amount_usd"] == "-0.25000000"
    assert account_events["funding_amount_observations"] == 1
    assert account_events["net_account_context_amount_usd"] == "5.25000000"
    assert account_events["ledger_amounts_available"] is True
    assert account_events["funding_amounts_available"] is True
    assert account_events["amounts_unavailable_reason"] == ""


def test_slot_state_reports_invalid_input_and_cli_exit(tmp_path):
    plan_path = tmp_path / "plan.json"
    target_path = tmp_path / "targets.json"
    follower_path = tmp_path / "follower.json"
    out_path = tmp_path / "slot-state.json"
    write_json(plan_path, slot_plan_report(valid=False))
    write_json(target_path, target_snapshot())
    write_json(follower_path, follower_snapshot())

    exit_code = build_shadow_slot_state.main(
        [str(plan_path), str(target_path), str(follower_path), "--out", str(out_path)]
    )

    assert exit_code == 1
    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["valid"] is False
    assert "slot plan report is not valid" in report["input_blockers"]
