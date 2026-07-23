from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "validate_slot_plan.py"
SPEC = importlib.util.spec_from_file_location("validate_slot_plan", SCRIPT_PATH)
assert SPEC is not None
validate_slot_plan = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = validate_slot_plan
SPEC.loader.exec_module(validate_slot_plan)


SOURCE_1 = "0x1111111111111111111111111111111111111111"
SOURCE_2 = "0x2222222222222222222222222222222222222222"
SUBACCOUNT_1 = "0xf000000000000000000000000000000000000001"
SUBACCOUNT_2 = "0xf000000000000000000000000000000000000002"


def valid_slot(**overrides) -> dict:
    row = {
        "allowed_coins": ["BTC", "ETH"],
        "denied_coins": [],
        "dust_policy": {"mode": "accumulate", "stale_after_ms": 300000},
        "enabled": False,
        "entry_slippage_bps": "20",
        "exchange_action_timeout_s": "20",
        "equity_confidence_policy": "block_low",
        "expected_account_mode": "standard",
        "expected_margin_mode": "cross",
        "fixed_risk_budget_usd": None,
        "initial_budget_usd": "50",
        "max_emergency_leverage": "10",
        "max_gross_notional_usd": "500",
        "min_notional_usd": "10",
        "mode": "shadow",
        "reduce_only_slippage_bps": "300",
        "sizing_policy": "pure_compound",
        "slot": "slot-1",
        "source_address": SOURCE_1,
        "subaccount_address": SUBACCOUNT_1,
        "subaccount_verified": False,
    }
    row.update(overrides)
    return row


def valid_plan(*slots: dict, **overrides) -> dict:
    plan = {
        "environment": "shadow",
        "slots": list(slots) or [valid_slot()],
        "version": 1,
    }
    plan.update(overrides)
    return plan


def test_validator_accepts_disabled_unverified_shadow_slots():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(),
            valid_slot(
                allowed_coins=[],
                denied_coins=["PURR"],
                initial_budget_usd="100",
                slot="slot-2",
                source_address=SOURCE_2,
                subaccount_address=SUBACCOUNT_2,
                sizing_policy="fixed_risk_budget",
                fixed_risk_budget_usd="75",
            ),
        )
    )

    assert report["valid"] is True
    assert report["read_only"] is True
    assert report["exchange_touched"] is False
    assert report["counts"]["slots"] == 2
    assert report["counts"]["sizing_policies"] == {
        "fixed_risk_budget": 1,
        "pure_compound": 1,
    }
    assert report["slots"][0]["fixed_multiplier"] == "1.00000000"
    assert report["slots"][0]["exchange_action_timeout_s"] == "20.00000000"
    assert report["min_notional_reference"]["perp_min_notional_usd"] == "10.00000000"
    assert len(report["warnings"]) == 2
    assert all("not verified" in warning for warning in report["warnings"])


def test_validator_accepts_utf8_bom_plan_file(tmp_path: Path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(valid_plan()), encoding="utf-8-sig")

    report = validate_slot_plan.validate_slot_plan(plan_path)

    assert report["valid"] is True


def test_validator_rejects_duplicate_slot_source_and_subaccount():
    duplicate = valid_slot(
        slot="slot-1",
        source_address=SOURCE_1,
        subaccount_address=SUBACCOUNT_1,
    )

    report = validate_slot_plan.validate_slot_plan_payload(valid_plan(valid_slot(), duplicate))

    assert report["valid"] is False
    assert any("slot slot-1 is duplicated" in blocker for blocker in report["blockers"])
    assert any(
        "source_address" in blocker and "duplicated" in blocker for blocker in report["blockers"]
    )
    assert any(
        "subaccount_address" in blocker and "duplicated" in blocker
        for blocker in report["blockers"]
    )


def test_validator_rejects_malformed_addresses_and_unsafe_mainnet_modes():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=True,
                mode="mainnet",
                source_address="not-address",
                subaccount_address="0xalso-bad",
            ),
            environment="live",
        )
    )

    assert report["valid"] is False
    blockers = " ".join(report["blockers"])
    assert "environment 'live' is blocked" in blockers
    assert "mode 'mainnet' is blocked" in blockers
    assert "source_address must be a 42-character hex address" in blockers
    assert "subaccount_address must be a 42-character hex address" in blockers
    assert "subaccount_verified must be true before an enabled slot" in blockers


def test_validator_requires_real_operator_timestamp_for_enabled_testnet_slot():
    missing = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=True,
                mode="testnet",
                subaccount_verified=True,
            ),
            environment="testnet",
        )
    )
    malformed = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=True,
                mode="testnet",
                operator_verified_at="2026-07-08T00:00:00",
                subaccount_verified=True,
            ),
            environment="testnet",
        )
    )

    assert any("operator_verified_at is required" in item for item in missing["blockers"])
    assert any("timezone-aware ISO-8601" in item for item in malformed["blockers"])


def test_validator_accepts_disabled_retired_quarantined_account():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=False,
                known_residual_positions={"xyz:JPY": "0.07"},
                mode="testnet",
                operational_status="retired_quarantined",
                operator_verified_at="2026-07-11T00:00:00Z",
                retired_at="2026-07-11T00:00:00Z",
                retirement_evidence="data/runs/slot-6/post_run_report.json",
                retirement_open_orders=0,
                retirement_reason="testnet market cannot absorb a safe reduce-only close",
                subaccount_verified=True,
            ),
            environment="testnet",
        )
    )

    assert report["valid"] is True
    assert report["counts"]["retired_quarantined_slots"] == 1
    assert report["slots"][0]["operational_status"] == "retired_quarantined"
    assert report["slots"][0]["known_residual_positions"] == {"xyz:JPY": "0.07000000"}
    assert any("permanently excluded" in warning for warning in report["warnings"])


def test_validator_rejects_active_or_undocumented_retired_account():
    active_retired = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=True,
                mode="testnet",
                operational_status="retired_quarantined",
                operator_verified_at="2026-07-11T00:00:00Z",
                subaccount_verified=True,
            ),
            environment="testnet",
        )
    )
    undocumented = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                enabled=False,
                mode="testnet",
                operational_status="retired_quarantined",
                operator_verified_at="2026-07-11T00:00:00Z",
                subaccount_verified=True,
            ),
            environment="testnet",
        )
    )

    assert any(
        "enabled slots must have operational_status=active" in item
        for item in active_retired["blockers"]
    )
    blockers = " ".join(undocumented["blockers"])
    assert "retirement_reason is required" in blockers
    assert "retired_at is required" in blockers
    assert "retirement_evidence is required" in blockers
    assert "known_residual_positions is required" in blockers
    assert "retirement_open_orders is required" in blockers


def test_validator_rejects_fixed_risk_without_budget_and_low_min_notional():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                fixed_risk_budget_usd=None,
                fixed_multiplier="0",
                min_notional_usd="5",
                sizing_policy="fixed_risk_budget",
            )
        )
    )

    assert report["valid"] is False
    assert any("fixed_risk_budget_usd is required" in blocker for blocker in report["blockers"])
    assert any("fixed_multiplier must be positive" in blocker for blocker in report["blockers"])
    assert any("min_notional_usd must be at least" in blocker for blocker in report["blockers"])


def test_validator_accepts_explicit_default_only_source_scope_with_fixed_budget():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                source_dex_scope="default_only_account_equity",
                sizing_policy="fixed_risk_budget",
                fixed_risk_budget_usd="50",
            )
        )
    )

    assert report["valid"] is True
    assert report["slots"][0]["source_dex_scope"] == "default_only_account_equity"
    assert any("total shared Unified collateral" in item for item in report["warnings"])


def test_validator_rejects_unbounded_or_unknown_source_dex_scope():
    unbounded = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(source_dex_scope="default_only_account_equity"))
    )
    unknown = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(source_dex_scope="copy_everything"))
    )

    assert any("requires fixed_risk_budget sizing" in item for item in unbounded["blockers"])
    assert any("source_dex_scope must be one of" in item for item in unknown["blockers"])


def test_validator_accepts_canonical_hip3_markets_and_all_configured_scope():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(
                allowed_coins=["BTC", "xyz:AAPL"],
                source_dex_scope="all_configured_markets",
            )
        )
    )

    assert report["valid"] is True
    assert report["slots"][0]["allowed_coins"] == ["BTC", "xyz:AAPL"]
    assert report["slots"][0]["source_dex_scope"] == "all_configured_markets"
    assert any(
        "all configured DEX positions use total Unified collateral" in item
        for item in report["warnings"]
    )


def test_validator_preserves_default_compatibility_and_dex_case():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(allowed_coins=[" btc ", "XYZ:aapl"]))
    )

    assert report["valid"] is True
    assert report["slots"][0]["allowed_coins"] == ["BTC", "XYZ:AAPL"]


def test_validator_rejects_malformed_and_duplicate_canonical_markets():
    malformed = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(allowed_coins=["xyz:AAPL:PERP", "xyz:", ":AAPL", "xyz:AAPL/USD"]))
    )
    duplicate = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(allowed_coins=["xyz:aapl", "xyz:AAPL"]))
    )

    assert malformed["valid"] is False
    assert sum("must be a valid market symbol" in item for item in malformed["blockers"]) == 4
    assert any("duplicate coin xyz:AAPL" in item for item in duplicate["blockers"])


def test_validator_treats_dex_prefix_case_as_exchange_identity():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(allowed_coins=["KNETIQ:FOO", "knetiq:FOO"]))
    )

    assert report["valid"] is True
    assert report["slots"][0]["allowed_coins"] == ["KNETIQ:FOO", "knetiq:FOO"]


def test_validator_rejects_nonfinite_and_excessive_reduce_only_slippage():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(initial_budget_usd="NaN", reduce_only_slippage_bps="1000.01"))
    )

    assert report["valid"] is False
    assert any(
        "initial_budget_usd must be a decimal value" in blocker for blocker in report["blockers"]
    )
    assert any(
        "reduce_only_slippage_bps cannot exceed" in blocker for blocker in report["blockers"]
    )


def test_validator_bounds_supervisor_exchange_action_timeout():
    too_short = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(exchange_action_timeout_s="10"))
    )
    too_long = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(valid_slot(exchange_action_timeout_s="29"))
    )

    assert any("must exceed" in item and "signed expiry" in item for item in too_short["blockers"])
    assert any("must be at most" in item and "dead-man" in item for item in too_long["blockers"])


def test_validator_rejects_coin_filter_ambiguity_and_secret_fields():
    report = validate_slot_plan.validate_slot_plan_payload(
        valid_plan(
            valid_slot(allowed_coins=["BTC"], denied_coins=["ETH"]),
            api_private_key="0x" + "1" * 64,
        )
    )

    assert report["valid"] is False
    assert report["counts"]["secret_field_count"] == 1
    assert any("$.api_private_key must not be present" in blocker for blocker in report["blockers"])
    assert any(
        "must not define both allowed_coins and denied_coins" in blocker
        for blocker in report["blockers"]
    )


def test_validator_cli_writes_report(tmp_path):
    plan_path = tmp_path / "plan.json"
    out_path = tmp_path / "report.json"
    plan_path.write_text(json.dumps(valid_plan(), sort_keys=True), encoding="utf-8")

    exit_code = validate_slot_plan.main([str(plan_path), "--out", str(out_path)])

    assert exit_code == 0
    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["valid"] is True
    assert report["counts"]["slots"] == 1
