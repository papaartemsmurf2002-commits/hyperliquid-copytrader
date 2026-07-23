from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "compare_shadow_policies.py"
SPEC = importlib.util.spec_from_file_location("compare_shadow_policies", SCRIPT_PATH)
assert SPEC is not None
compare_shadow_policies = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = compare_shadow_policies
SPEC.loader.exec_module(compare_shadow_policies)


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
    event_type: str,
    subtype: str,
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


def rest_snapshot(row_id: str, sort_ts_ms: int, account_value: str = "100") -> dict:
    return scorecard_row(
        row_id=row_id,
        sort_ts_ms=sort_ts_ms,
        category="state_refresh",
        source_action="rest_snapshot",
        classifier_decision="state_refresh_only",
        event_type="snapshot",
        subtype="rest_snapshot",
        confidence="high",
        metadata={"account_value_usd": account_value, "error_count": 0, "ok_count": 6},
    )


def source_order(row_id: str, sort_ts_ms: int, notional: str | None) -> dict:
    metadata = {"coin": "BTC", "limit_px": "100", "oid": row_id, "side": "B", "sz": "1"}
    if notional is not None:
        metadata["notional_usd"] = notional
    return scorecard_row(
        row_id=row_id,
        sort_ts_ms=sort_ts_ms,
        category="source_action",
        source_action="source_order_open",
        classifier_decision="would_map_or_refresh_target_order",
        event_type="order_update",
        subtype="order_update:open",
        metadata=metadata,
    )


def source_fill(row_id: str, sort_ts_ms: int, notional: str, closed_pnl: str = "0") -> dict:
    return scorecard_row(
        row_id=row_id,
        sort_ts_ms=sort_ts_ms,
        category="source_action",
        source_action="source_fill",
        classifier_decision="would_validate_position_and_drift",
        event_type="fill",
        subtype="user:fills",
        metadata={
            "closed_pnl_usd": closed_pnl,
            "coin": "BTC",
            "dir": "Open Long",
            "fee_usd": "0",
            "notional_usd": notional,
            "oid": row_id,
            "px": "100",
            "side": "B",
            "sz": "1",
            "tid": row_id,
        },
    )


def policy_facts(path: Path, *, row_id: str | None = None) -> list[dict]:
    facts = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    if row_id is None:
        return facts
    return [fact for fact in facts if fact["row_id"] == row_id and fact["policy"] is not None]


def test_policy_comparison_sends_and_accumulates_dust(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            rest_snapshot("snapshot", 1_000),
            source_order("large-order", 1_100, "30"),
            source_fill("dust-1", 1_200, "5"),
            source_fill("dust-2", 1_300, "5"),
            source_fill("dust-3", 1_400, "5"),
            source_fill("dust-4", 1_500, "5"),
        ],
    )

    report = compare_shadow_policies.compare_shadow_policies(rows_path, facts_out=facts_out)

    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["min_notional"]["perp_min_notional_usd"] == "10.00000000"
    assert report["copyable_actions"] == {
        "missing_notional": 0,
        "rows": 5,
        "state_only_source_actions": 0,
        "with_notional": 5,
    }
    for policy in report["policies"].values():
        assert policy["decision_counts"] == {
            "would_accumulate_dust": 3,
            "would_send_accumulated_dust": 1,
            "would_send_now": 1,
        }
        assert policy["target_notional_sent_usd"] == "25.00000000"
        assert policy["target_notional_dust_usd"] == "10.00000000"
        assert policy["dust_buckets"][0]["executed_events"] == 1

    decisions = [fact["decision"] for fact in policy_facts(facts_out)]
    assert decisions.count("would_send_now") == 2
    assert decisions.count("would_accumulate_dust") == 6
    assert decisions.count("would_send_accumulated_dust") == 2


def test_policy_comparison_distinguishes_compound_from_fixed_budget(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            rest_snapshot("snapshot", 1_000),
            source_fill("profitable-fill", 1_100, "100", closed_pnl="100"),
            source_order("next-order", 1_200, "100"),
        ],
    )

    report = compare_shadow_policies.compare_shadow_policies(rows_path, facts_out=facts_out)

    assert report["policies"]["pure_compound"]["final_equity_usd"] == "100.00000000"
    assert report["policies"]["fixed_risk_budget"]["final_equity_usd"] == "100.00000000"
    next_order_facts = {
        fact["policy"]: fact for fact in policy_facts(facts_out, row_id="next-order")
    }
    assert next_order_facts["pure_compound"]["metadata"]["sizing_equity_usd"] == "100.00000000"
    assert next_order_facts["pure_compound"]["metadata"]["target_notional_usd"] == "100.00000000"
    assert next_order_facts["fixed_risk_budget"]["metadata"]["sizing_equity_usd"] == "50.00000000"
    assert next_order_facts["fixed_risk_budget"]["metadata"]["target_notional_usd"] == "50.00000000"


def test_policy_comparison_blocks_after_recovery_until_explicit_reconcile(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    facts_out = tmp_path / "facts.jsonl"
    write_jsonl(
        rows_path,
        [
            rest_snapshot("snapshot-1", 1_000),
            scorecard_row(
                row_id="recovery",
                sort_ts_ms=1_100,
                category="recovery",
                source_action="reconnect_recovered",
                classifier_decision="requires_rest_backfill_and_reconcile",
                event_type="recovery",
                subtype="reconnect_recovered",
                confidence="high",
                metadata={"gap_ms": 500},
            ),
            rest_snapshot("snapshot-2", 1_200),
            source_order("blocked-order", 1_300, "100"),
        ],
    )

    report = compare_shadow_policies.compare_shadow_policies(rows_path, facts_out=facts_out)

    assert report["recovery"]["pending_reconcile"] is True
    assert report["recovery"]["pending_rest_backfill"] is True
    assert report["recovery"]["live_stream_hints_allowed"] is False
    assert report["recovery"]["state_refreshes_after_recovery"] == 1
    for policy in report["policies"].values():
        assert policy["decision_counts"] == {"blocked_recovery_pending": 1}
        assert policy["target_notional_sent_usd"] == "0.00000000"
    blocked_facts = policy_facts(facts_out, row_id="blocked-order")
    assert {fact["decision"] for fact in blocked_facts} == {"blocked_recovery_pending"}

    what_if = compare_shadow_policies.compare_shadow_policies(
        rows_path,
        analysis_clear_recovery_after_rest_snapshot=True,
    )
    assert what_if["recovery_model"] == {
        "analysis_clear_recovery_after_rest_snapshot": True,
        "fail_closed_default": False,
        "not_live_recovery_proof": True,
    }
    assert what_if["recovery"]["analysis_recovery_clears"] == 1
    assert what_if["recovery"]["pending_reconcile"] is False
    for policy in what_if["policies"].values():
        assert policy["decision_counts"] == {"would_send_now": 1}


def test_policy_comparison_skips_missing_denominator_and_notional(tmp_path):
    rows_path = tmp_path / "rows.jsonl"
    write_jsonl(
        rows_path,
        [
            source_order("before-snapshot", 1_000, "100"),
            rest_snapshot("snapshot", 1_100),
            source_order("missing-notional", 1_200, None),
        ],
    )

    report = compare_shadow_policies.compare_shadow_policies(rows_path)

    assert report["copyable_actions"]["missing_notional"] == 1
    for policy in report["policies"].values():
        assert policy["decision_counts"] == {
            "skipped_missing_source_account_value": 1,
            "skipped_missing_source_notional": 1,
        }
