from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from hyperliquid_copytrader.continuous_config import bind_continuous_plan, load_continuous_plan


def _plan() -> dict[str, object]:
    return {
        "version": 1,
        "network": "mainnet",
        "runtime_id": "continuous-v1",
        "startup_baseline_only": True,
        "max_combined_gross_usd": "30",
        "slots": [
            {
                "slot": "acc1",
                "source_address": "0x" + "1" * 40,
                "follower_account_address": "0x" + "2" * 40,
                "credential_profile_id": "acc1",
                "multiplier": "0.01",
                "max_order_notional_usd": "12",
                "max_gross_exposure_usd": "15",
                "max_open_positions": 1,
                "max_leverage": 1,
                "action_limit_per_minute": 6,
                "allowed_markets": ["BTC"],
                "enabled": True,
            },
            {
                "slot": "acc7",
                "source_address": "0x" + "3" * 40,
                "follower_account_address": "0x" + "4" * 40,
                "credential_profile_id": "acc7",
                "multiplier": "0.01",
                "max_order_notional_usd": "12",
                "max_gross_exposure_usd": "15",
                "max_open_positions": 1,
                "max_leverage": 1,
                "action_limit_per_minute": 6,
                "allowed_markets": ["xyz:EWY"],
                "enabled": True,
            },
        ],
    }


def _records(plan: Any, tmp_path: Path) -> list[dict[str, object]]:
    return [
        {
            "profile_id": slot.credential_profile_id,
            "source_wallet": slot.source_address,
            "follower_account_address": slot.follower_account_address,
            "api_wallet_address": "0x" + ("a" if slot.slot == "acc1" else "b") * 40,
            "api_private_key_file": str(tmp_path / f"{slot.slot}.key"),
            "global_account_address": "0x" + "c" * 40,
            "expected_account_mode": "unified",
            "denied_symbols": [],
        }
        for slot in plan.enabled_slots
    ]


def test_plan_accepts_utf8_bom_and_preserves_explicit_limits(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(_plan()), encoding="utf-8-sig")

    plan = load_continuous_plan(path)

    assert len(plan.enabled_slots) == 2
    assert plan.max_combined_gross_usd == 30
    assert plan.enabled_slots[1].allowed_markets == ("xyz:EWY",)


def test_plan_refuses_startup_position_adoption(tmp_path: Path) -> None:
    payload = _plan()
    payload["startup_baseline_only"] = False
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="startup_baseline_only"):
        load_continuous_plan(path)


def test_plan_refuses_duplicate_follower(tmp_path: Path) -> None:
    payload = _plan()
    slots = payload["slots"]
    assert isinstance(slots, list)
    slots[1]["follower_account_address"] = slots[0]["follower_account_address"]
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate follower"):
        load_continuous_plan(path)


def test_plan_refuses_cross_slot_source_follower_feedback_loop(tmp_path: Path) -> None:
    payload = _plan()
    slots = payload["slots"]
    assert isinstance(slots, list)
    slots[1]["source_address"] = slots[0]["follower_account_address"]
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="source address collides with owned follower"):
        load_continuous_plan(path)


def test_enabled_slot_accepts_plan_without_legacy_market_allowlist(tmp_path: Path) -> None:
    payload = _plan()
    slots = payload["slots"]
    assert isinstance(slots, list)
    slots[0].pop("allowed_markets")
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    plan = load_continuous_plan(path)

    assert plan.enabled_slots[0].allowed_markets == ()


def test_binding_refuses_profile_denied_market(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(_plan()), encoding="utf-8")
    plan = load_continuous_plan(path)
    records = _records(plan, tmp_path)
    records[0]["denied_symbols"] = ["BTC"]
    monkeypatch.setattr(
        "hyperliquid_copytrader.continuous_config.FleetCredentialProfileRegistry._records_with_health",
        lambda self, *, verify_secrets: (records, []),
    )

    with pytest.raises(ValueError, match="allows markets denied"):
        bind_continuous_plan(plan, repo_root=tmp_path, verify_secrets=False)


@pytest.mark.parametrize(
    ("field", "role"),
    [
        ("api_wallet_address", "API wallet"),
        ("global_account_address", "global/action principal"),
    ],
)
def test_binding_refuses_source_owned_identity_feedback_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    role: str,
) -> None:
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(_plan()), encoding="utf-8")
    plan = load_continuous_plan(path)
    records = _records(plan, tmp_path)
    records[0][field] = plan.enabled_slots[1].source_address
    monkeypatch.setattr(
        "hyperliquid_copytrader.continuous_config.FleetCredentialProfileRegistry._records_with_health",
        lambda self, *, verify_secrets: (records, []),
    )

    with pytest.raises(ValueError, match=role):
        bind_continuous_plan(plan, repo_root=tmp_path, verify_secrets=False)
