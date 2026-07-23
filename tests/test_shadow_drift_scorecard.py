from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "score_shadow_drift.py"
SPEC = importlib.util.spec_from_file_location("score_shadow_drift", SCRIPT_PATH)
assert SPEC is not None
score_shadow_drift = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = score_shadow_drift
SPEC.loader.exec_module(score_shadow_drift)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")


def policy_fact(
    *,
    policy: str,
    decision: str,
    coin: str = "BTC",
    side: str = "B",
    executed: str = "10",
    row_id: str | None = None,
) -> dict:
    row_id = row_id or f"{policy}-{coin}-{decision}"
    return {
        "address": "0x1111111111111111111111111111111111111111",
        "confidence": "medium",
        "decision": decision,
        "event_id": f"event-{row_id}",
        "exchange_touched": False,
        "fact_id": f"fact-{row_id}",
        "fact_type": "policy_shadow_decision",
        "intent": "place_scaled_order",
        "metadata": {
            "coin": coin,
            "executed_target_notional_usd": executed,
            "side": side,
            "target_notional_usd": executed,
        },
        "policy": policy,
        "read_only": True,
        "reason": "fixture",
        "row_id": row_id,
        "slot": "slot-1",
        "sort_ts_ms": 1_000,
    }


def global_fact() -> dict:
    return {
        "decision": "source_account_value_seen",
        "fact_type": "source_account_value_seen",
        "metadata": {},
        "policy": None,
        "row_id": "global",
    }


def snapshot(*, recovery_complete: bool, positions=None, open_orders=None, policies=None) -> dict:
    return {
        "account_value_usd": "50",
        "captured_ms": 1_500,
        "follower_subaccount": "shadow-subaccount",
        "open_orders": open_orders or [],
        "policies": policies or {},
        "positions": positions or [],
        "recovery": {
            "source_backfill_complete": recovery_complete,
            "follower_refresh_complete": recovery_complete,
            "reconcile_complete": recovery_complete,
        },
        "slot": "slot-1",
    }


def drift_facts(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_drift_scorer_blocks_repairs_until_recovery_complete(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    out = tmp_path / "report.json"
    facts_out = tmp_path / "drift.jsonl"
    write_jsonl(
        facts_path, [global_fact(), policy_fact(policy="pure_compound", decision="would_send_now")]
    )
    write_json(snapshot_path, snapshot(recovery_complete=False))

    report = score_shadow_drift.score_shadow_drift(
        facts_path,
        snapshot_path,
        out=out,
        facts_out=facts_out,
    )

    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["input_counts"]["global_fact_rows"] == 1
    assert report["input_counts"]["policy_fact_rows"] == 1
    assert report["recovery_completion"]["complete"] is False
    assert report["drift_counts"] == {"blocked_recovery_incomplete": 1}
    assert report["repair_intent_counts"] == {"do_not_repair_until_recovery_complete": 1}
    fact = drift_facts(facts_out)[0]
    assert fact["decision"] == "blocked_recovery_incomplete"
    assert "source_backfill_complete" in fact["metadata"]["missing_recovery_requirements"]


def test_drift_scorer_reports_recovery_block_even_without_exposure(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    write_jsonl(
        facts_path,
        [policy_fact(policy="pure_compound", decision="blocked_recovery_pending", executed="0")],
    )
    write_json(snapshot_path, snapshot(recovery_complete=False))

    report = score_shadow_drift.score_shadow_drift(facts_path, snapshot_path)

    assert report["drift_counts"] == {"blocked_recovery_incomplete": 1}
    assert report["repair_intent_counts"] == {"do_not_repair_until_recovery_complete": 1}
    assert report["sample_drift_facts"][0]["coin"] == "all"


def test_drift_scorer_treats_open_orders_as_projected_exposure(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    write_jsonl(facts_path, [policy_fact(policy="pure_compound", decision="would_send_now")])
    write_json(
        snapshot_path,
        snapshot(
            recovery_complete=True,
            positions=[{"coin": "BTC", "signed_notional_usd": "7"}],
            open_orders=[{"coin": "BTC", "notional_usd": "3", "side": "B"}],
        ),
    )

    report = score_shadow_drift.score_shadow_drift(facts_path, snapshot_path)

    assert report["drift_counts"] == {"projected_in_sync_position_pending": 1}
    assert report["repair_intent_counts"] == {"wait_for_open_orders_or_reconcile": 1}
    fact = report["sample_drift_facts"][0]
    assert fact["metadata"]["target_delta_notional_usd"] == "10.00000000"
    assert fact["metadata"]["follower_projected_notional_usd"] == "10.00000000"
    assert fact["metadata"]["follower_position_notional_usd"] == "7.00000000"


def test_drift_scorer_uses_policy_specific_follower_views(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    write_jsonl(
        facts_path,
        [
            policy_fact(policy="pure_compound", decision="would_send_now", executed="10"),
            policy_fact(policy="fixed_risk_budget", decision="would_send_now", executed="5"),
        ],
    )
    write_json(
        snapshot_path,
        snapshot(
            recovery_complete=True,
            policies={
                "pure_compound": {"positions": [{"coin": "BTC", "signed_notional_usd": "10"}]},
                "fixed_risk_budget": {"positions": [{"coin": "BTC", "signed_notional_usd": "0"}]},
            },
        ),
    )

    report = score_shadow_drift.score_shadow_drift(facts_path, snapshot_path)

    assert report["drift_counts"] == {"drift_detected": 1, "in_sync": 1}
    facts = {fact["policy"]: fact for fact in report["sample_drift_facts"]}
    assert facts["pure_compound"]["decision"] == "in_sync"
    assert facts["fixed_risk_budget"]["repair_intent"] == "would_open_or_place_follower_exposure"


def test_drift_scorer_classifies_reduce_and_flip_repairs(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    write_jsonl(
        facts_path,
        [
            policy_fact(
                policy="pure_compound",
                decision="would_send_accumulated_dust",
                coin="ETH",
                side="A",
                executed="12",
            )
        ],
    )
    write_json(
        snapshot_path,
        snapshot(
            recovery_complete=True,
            positions=[{"coin": "ETH", "signed_notional_usd": "4"}],
        ),
    )

    report = score_shadow_drift.score_shadow_drift(facts_path, snapshot_path)

    assert report["drift_counts"] == {"drift_detected": 1}
    fact = report["sample_drift_facts"][0]
    assert fact["metadata"]["target_delta_notional_usd"] == "-12.00000000"
    assert fact["repair_intent"] == "would_reduce_flip_or_flatten_then_reopen"


def test_drift_scorer_writes_first_class_target_snapshot(tmp_path):
    facts_path = tmp_path / "policy.jsonl"
    snapshot_path = tmp_path / "snapshot.json"
    targets_out = tmp_path / "targets.json"
    write_jsonl(
        facts_path,
        [
            policy_fact(policy="pure_compound", decision="would_send_now", executed="10"),
            policy_fact(
                policy="pure_compound",
                decision="would_send_accumulated_dust",
                coin="ETH",
                side="A",
                executed="12",
            ),
            policy_fact(policy="pure_compound", decision="blocked_recovery_pending", executed="0"),
            policy_fact(policy="fixed_risk_budget", decision="would_accumulate_dust", executed="0"),
        ],
    )
    write_json(snapshot_path, snapshot(recovery_complete=True))

    report = score_shadow_drift.score_shadow_drift(
        facts_path,
        snapshot_path,
        targets_out=targets_out,
    )

    target_snapshot = json.loads(targets_out.read_text(encoding="utf-8"))
    assert target_snapshot["read_only"] is True
    assert target_snapshot["exchange_touched"] is False
    assert target_snapshot == report["shadow_target_snapshot"]
    pure_positions = {
        row["coin"]: row for row in target_snapshot["policies"]["pure_compound"]["target_positions"]
    }
    assert pure_positions["BTC"]["signed_target_notional_usd"] == "10.00000000"
    assert pure_positions["BTC"]["side"] == "long"
    assert pure_positions["BTC"]["executed_actions"] == 1
    assert pure_positions["BTC"]["blocked_actions"] == 1
    assert pure_positions["ETH"]["signed_target_notional_usd"] == "-12.00000000"
    assert pure_positions["ETH"]["side"] == "short"
    fixed_positions = target_snapshot["policies"]["fixed_risk_budget"]["target_positions"]
    assert fixed_positions[0]["dust_actions"] == 1
