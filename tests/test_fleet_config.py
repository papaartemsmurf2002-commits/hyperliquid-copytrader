from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
from eth_account import Account

from hyperliquid_copytrader.fleet_config import (
    DETERMINISTIC_LOCAL_REACTION_MAXIMA_MS,
    FLEET_LIVE_LOCAL_REACTION_MAXIMA_MS,
    FLEET_POLICY_VERSION,
    LOCAL_REACTION_LATENCY_GATE_VERSION,
    PRODUCTION_BENCHMARK_VERSION,
    CredentialMap,
    FleetPlan,
    FleetSlot,
    _parse_slot,
    actual_catalog_rest_load_model,
    fleet_rest_budget_payload,
    load_credential_map,
    load_fleet_plan,
    selected_credential_map_sha256,
    validate_fleet_provisioning,
)
from hyperliquid_copytrader.stream_gateway import stable_shard


def test_operator_authorized_local_reaction_latency_contract_is_frozen() -> None:
    assert LOCAL_REACTION_LATENCY_GATE_VERSION == "operator-authorized-plus-25ms-20260718-v1"
    assert PRODUCTION_BENCHMARK_VERSION == "production-path-windows-v6"
    assert DETERMINISTIC_LOCAL_REACTION_MAXIMA_MS == {
        "p50_ms": 50,
        "p95_ms": 125,
        "p99_ms": 275,
    }
    assert FLEET_LIVE_LOCAL_REACTION_MAXIMA_MS == {
        "p95_ms": 125,
        "p99_ms": 275,
    }


def _slot(index: int, *, lifecycle: str = "native") -> FleetSlot:
    shard = (index - 1) % 2
    return FleetSlot(
        slot=f"acc{index}",
        source_address=f"0x{index:040x}",
        follower_account_address=f"0x{index + 100:040x}",
        credential_profile_id=f"profile-{index}",
        required_lifecycle_class=lifecycle,
        expected_account_mode="unified",
        eligibility="all_active_markets",
        denied_symbols=(),
        fixed_multiplier=Decimal("0.75"),
        max_initial_margin_utilization=Decimal("0.75"),
        max_notional_usd=Decimal("100"),
        max_gross_exposure_usd=Decimal("100"),
        max_open_positions=5,
        max_leverage=5,
        action_limit_per_minute=12,
        max_audited_dexes=1,
        source_shard=shard,
        action_shard=shard,
        enabled=True,
        operator_verified_at="2026-07-16T00:00:00Z",
    )


def _plan(*, purpose: str, slots: tuple[FleetSlot, ...], complete: bool) -> FleetPlan:
    return FleetPlan(
        version=3,
        environment="mainnet",
        purpose=purpose,
        policy_version="fleet-fast-execution-v1",
        intended_fleet_complete=complete,
        slots=slots,
        sha256=("a" if purpose == "pilot_12h" else "b") * 64,
        path=Path("plan.json"),
    )


def test_full_fleet_cannot_rename_a_frozen_pilot_identity(tmp_path) -> None:
    pilot_slots = (_slot(1, lifecycle="native"), _slot(2, lifecycle="hip3"))
    pilot = _plan(purpose="pilot_12h", slots=pilot_slots, complete=False)
    full_slots = (
        replace(pilot_slots[0], slot="renamed-acc1"),
        pilot_slots[1],
        *(_slot(index) for index in range(3, 11)),
    )
    full = _plan(purpose="full_fleet_12h", slots=full_slots, complete=True)
    credentials = CredentialMap(
        version=1,
        references={},
        sha256="c" * 64,
        path=tmp_path / "credentials.json",
    )
    budget = fleet_rest_budget_payload()

    result = validate_fleet_provisioning(
        scope="fleet",
        pilot_plan=pilot,
        full_fleet_plan=full,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )

    assert "full-fleet pilot identity acc1 changes frozen field slot" in result["blockers"]


def test_fleet_slot_and_profile_ids_cannot_escape_artifact_or_vault_paths(tmp_path) -> None:
    raw_slot = _slot(1).to_payload()
    raw_slot["subaccount_verified"] = True
    raw_slot["slot"] = "../escaped-slot"
    with pytest.raises(ValueError, match="identity is invalid"):
        _parse_slot(raw_slot, environment="mainnet")

    credential_map = tmp_path / "credential-map.json"
    credential_map.write_text(
        '{"version":1,"slots":{"../escaped-profile":{}}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="identity is invalid"):
        load_credential_map(credential_map)


def _address_for_shard(*, domain: str, shard: int, start: int) -> tuple[str, int]:
    for value in range(start, start + 100_000):
        address = f"0x{value:040x}"
        if stable_shard(address, 2, domain=domain) == shard:
            return address, value + 1
    raise AssertionError(f"no {domain} address for shard {shard}")


def _write_valid_ten_slot_fleet(tmp_path: Path) -> tuple[Path, Path, Path]:
    vault = tmp_path / "operator-profiles"
    vault.mkdir()
    slots: list[dict[str, object]] = []
    credential_slots: dict[str, dict[str, str]] = {}
    next_source = 1
    next_follower = 10_001
    for index in range(1, 11):
        target_shard = index - 1 if index <= 2 else index % 2
        source, next_source = _address_for_shard(
            domain="source", shard=target_shard, start=next_source
        )
        follower, next_follower = _address_for_shard(
            domain="action", shard=target_shard, start=next_follower
        )
        profile_id = f"profile-{index}"
        private_key = "0x" + f"{index + 100:064x}"
        api_wallet = Account.from_key(private_key).address.lower()
        profile_root = vault / profile_id
        profile_root.mkdir()
        (profile_root / "api.key").write_text(private_key + "\n", encoding="utf-8")
        (profile_root / "profile.json").write_text(
            json.dumps(
                {
                    "profile_id": profile_id,
                    "source_wallet": source,
                    "follower_account_address": follower,
                    "expected_account_mode": "unified",
                    "eligibility": "all_active_markets",
                    "denied_symbols": [],
                    "api_wallet_address": api_wallet,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        credential_slots[profile_id] = {
            "api_wallet_address": api_wallet,
            "api_private_key_file": f"{profile_id}/api.key",
        }
        slots.append(
            {
                "slot": f"acc{index}",
                "source_address": source,
                "follower_account_address": follower,
                "credential_profile_id": profile_id,
                "required_lifecycle_class": (
                    "native" if index == 1 else "hip3" if index == 2 else "both"
                ),
                "expected_account_mode": "unified",
                "eligibility": "all_active_markets",
                "denied_symbols": [],
                "fixed_multiplier": "0.75",
                "max_initial_margin_utilization": "0.75",
                "max_notional_usd": "100",
                "max_gross_exposure_usd": "100",
                "max_open_positions": 5,
                "max_leverage": 5,
                "action_limit_per_minute": 12,
                "max_audited_dexes": 1,
                "source_shard": stable_shard(source, 2, domain="source"),
                "action_shard": stable_shard(follower, 2, domain="action"),
                "enabled": True,
                "operator_verified_at": "2026-07-16T00:00:00Z",
                "subaccount_verified": True,
            }
        )
    common = {
        "version": 3,
        "environment": "mainnet",
        "policy_version": FLEET_POLICY_VERSION,
    }
    pilot_path = tmp_path / "pilot.json"
    full_path = tmp_path / "full.json"
    credential_map = vault / "credential-map.json"
    pilot_path.write_text(
        json.dumps(
            {
                **common,
                "purpose": "pilot_12h",
                "intended_fleet_complete": False,
                "slots": slots[:2],
            }
        ),
        encoding="utf-8",
    )
    full_path.write_text(
        json.dumps(
            {
                **common,
                "purpose": "full_fleet_12h",
                "intended_fleet_complete": True,
                "slots": slots,
            }
        ),
        encoding="utf-8",
    )
    credential_map.write_text(
        json.dumps({"version": 1, "slots": credential_slots}), encoding="utf-8"
    )
    return pilot_path, full_path, credential_map


def test_real_fleet_artifacts_load_and_validate_both_key_modes(tmp_path: Path) -> None:
    pilot_path, full_path, credential_path = _write_valid_ten_slot_fleet(tmp_path)
    pilot = load_fleet_plan(pilot_path)
    full = load_fleet_plan(full_path)
    credentials = load_credential_map(credential_path)
    budget = fleet_rest_budget_payload()
    private = validate_fleet_provisioning(
        scope="fleet",
        pilot_plan=pilot,
        full_fleet_plan=full,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=True,
    )
    redacted = validate_fleet_provisioning(
        scope="fleet",
        pilot_plan=pilot,
        full_fleet_plan=full,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )
    assert private["passed"] is True, private["blockers"]
    assert redacted["passed"] is True, redacted["blockers"]
    assert len(full.enabled_slots) == 10
    assert {slot.source_shard for slot in pilot.enabled_slots} == {0, 1}
    assert {slot.action_shard for slot in pilot.enabled_slots} == {0, 1}
    assert all(row["derived_address_matches"] for row in private["credential_checks"])
    assert not any(row["derived_address_matches"] for row in redacted["credential_checks"])
    assert "private_key" not in json.dumps(credentials.redacted_payload()).lower()


def test_pilot_provisioning_needs_only_selected_profiles_and_digest_is_stable(
    tmp_path: Path,
) -> None:
    pilot_path, _full_path, credential_path = _write_valid_ten_slot_fleet(tmp_path)
    pilot = load_fleet_plan(pilot_path)
    credentials = load_credential_map(credential_path)
    pilot_profile_ids = tuple(slot.credential_profile_id for slot in pilot.enabled_slots)
    selected_references = {
        profile_id: credentials.references[profile_id] for profile_id in pilot_profile_ids
    }
    pilot_credentials = replace(
        credentials,
        references=selected_references,
        sha256="d" * 64,
    )
    budget = fleet_rest_budget_payload()

    result = validate_fleet_provisioning(
        scope="pilot",
        pilot_plan=pilot,
        full_fleet_plan=None,
        credentials=pilot_credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )
    assert result["passed"] is True, result["blockers"]
    assert result["scope"] == "pilot"
    assert result["selected_plan_sha256"] == pilot.sha256
    assert result["full_fleet_plan_sha256"] == ""
    assert result["selected_profile_ids"] == sorted(pilot_profile_ids)

    original_digest = selected_credential_map_sha256(
        credentials=pilot_credentials,
        profile_ids=pilot_profile_ids,
    )
    unrelated = replace(
        next(iter(selected_references.values())),
        profile_id="unrelated",
    )
    expanded = replace(
        pilot_credentials,
        references={**selected_references, "unrelated": unrelated},
        sha256="e" * 64,
    )
    assert (
        selected_credential_map_sha256(
            credentials=expanded,
            profile_ids=pilot_profile_ids,
        )
        == original_digest
    )

    changed_profile = pilot_profile_ids[0]
    changed = replace(
        expanded,
        references={
            **expanded.references,
            changed_profile: replace(
                expanded.references[changed_profile],
                api_wallet_address="0x" + "f" * 40,
            ),
        },
    )
    assert (
        selected_credential_map_sha256(
            credentials=changed,
            profile_ids=pilot_profile_ids,
        )
        != original_digest
    )


@pytest.mark.parametrize("audited_dex_caps", [(2, 1), (1, 2)])
def test_pilot_rest_admission_excludes_synthetic_capacity_accounts(
    tmp_path: Path,
    audited_dex_caps: tuple[int, int],
) -> None:
    pilot_path, _full_path, credential_path = _write_valid_ten_slot_fleet(tmp_path)
    loaded_pilot = load_fleet_plan(pilot_path)
    pilot = replace(
        loaded_pilot,
        slots=tuple(
            replace(slot, max_audited_dexes=cap)
            for slot, cap in zip(loaded_pilot.slots, audited_dex_caps, strict=True)
        ),
    )
    credentials = load_credential_map(credential_path)
    budget = fleet_rest_budget_payload()

    result = validate_fleet_provisioning(
        scope="pilot",
        pilot_plan=pilot,
        full_fleet_plan=None,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )

    assert result["passed"] is True, result["blockers"]
    assert result["rest_load_model"]["evidence_scope"] == (
        "selected_launch_plan_static_pre_catalog_floor"
    )
    assert result["rest_load_model"]["components"] == {
        "affected_follower": 72,
        "full_follower_dex_discovery": 6,
        "full_open_order_audit": 60,
        "nonfunding_ledger_audit": 20,
        "catalog": 60,
        "total": 218,
    }
    assert result["capacity_workload_binding"]["workload_slot_count"] == 10
    assert result["capacity_workload_binding"]["source_shard_counts"] == {"0": 9, "1": 1}
    assert result["websocket_load_model"]["unique_users"] == 10


def test_exact_full_fleet_static_rest_floor_remains_blocked_at_954(
    tmp_path: Path,
) -> None:
    pilot_path, full_path, credential_path = _write_valid_ten_slot_fleet(tmp_path)
    loaded_pilot = load_fleet_plan(pilot_path)
    loaded_full = load_fleet_plan(full_path)
    pilot_caps = (2, 1)
    pilot = replace(
        loaded_pilot,
        slots=tuple(
            replace(slot, max_audited_dexes=cap)
            for slot, cap in zip(loaded_pilot.slots, pilot_caps, strict=True)
        ),
    )
    full = replace(
        loaded_full,
        slots=tuple(
            replace(slot, max_audited_dexes=(1 if index == 1 else 2))
            for index, slot in enumerate(loaded_full.slots)
        ),
    )
    credentials = load_credential_map(credential_path)
    budget = fleet_rest_budget_payload()

    result = validate_fleet_provisioning(
        scope="fleet",
        pilot_plan=pilot,
        full_fleet_plan=full,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )

    assert result["rest_load_model"]["components"]["total"] == 954
    assert result["rest_load_model"]["passed"] is False
    assert any("requires 954 ordinary REST weight/min" in item for item in result["blockers"])


@pytest.mark.parametrize(
    ("dex_count", "expected_total", "expected_passed"),
    [(10, 592, True), (13, 724, False), (14, 768, False)],
)
def test_live_catalog_rest_admission_uses_every_full_audit_dex(
    dex_count: int,
    expected_total: int,
    expected_passed: bool,
) -> None:
    plan = _plan(
        purpose="pilot_12h",
        slots=(
            replace(_slot(1, lifecycle="native"), max_audited_dexes=2),
            replace(_slot(2, lifecycle="hip3"), max_audited_dexes=1),
        ),
        complete=False,
    )
    wire_dexes = [""] + [f"dex-{index}" for index in range(1, dex_count)]

    result = actual_catalog_rest_load_model(
        plan,
        {"wire_dexes": wire_dexes, "dex_count": dex_count},
        ordinary_rest_budget=720,
        reserve_rest_budget=480,
    )

    assert result["components"]["total"] == expected_total
    assert result["passed"] is expected_passed
    assert result["full_audit_dex_queries_per_cycle"] == 2 * dex_count
    assert len(result["model_sha256"]) == 64


def test_live_catalog_rest_admission_rejects_malformed_topology_and_full_fleet_load() -> None:
    pilot = _plan(
        purpose="pilot_12h",
        slots=(_slot(1, lifecycle="native"), _slot(2, lifecycle="hip3")),
        complete=False,
    )
    malformed = actual_catalog_rest_load_model(
        pilot,
        {"wire_dexes": ["", "dex", "dex"], "dex_count": 3},
        ordinary_rest_budget=720,
        reserve_rest_budget=480,
    )
    full = _plan(
        purpose="full_fleet_12h",
        slots=tuple(
            replace(_slot(index), max_audited_dexes=(1 if index == 2 else 2))
            for index in range(1, 11)
        ),
        complete=True,
    )
    full_result = actual_catalog_rest_load_model(
        full,
        {"wire_dexes": [""] + [f"dex-{index}" for index in range(1, 10)], "dex_count": 10},
        ordinary_rest_budget=720,
        reserve_rest_budget=480,
    )

    assert malformed["passed"] is False
    assert malformed["blockers"] == ["live catalog REST load topology is malformed"]
    assert full_result["components"]["total"] == 2736
    assert full_result["passed"] is False


@pytest.mark.parametrize(
    ("pilot_environment", "full_environment", "expected_blocker"),
    [
        ("testnet", "testnet", "mainnet pilot plan"),
        ("mainnet", "testnet", "same environment"),
    ],
)
def test_mainnet_provisioning_rejects_testnet_or_mixed_plan_binding(
    tmp_path: Path,
    pilot_environment: str,
    full_environment: str,
    expected_blocker: str,
) -> None:
    pilot_path, full_path, credential_path = _write_valid_ten_slot_fleet(tmp_path)
    pilot = replace(load_fleet_plan(pilot_path), environment=pilot_environment)
    full = replace(load_fleet_plan(full_path), environment=full_environment)
    credentials = load_credential_map(credential_path)
    budget = fleet_rest_budget_payload()

    result = validate_fleet_provisioning(
        scope="fleet",
        pilot_plan=pilot,
        full_fleet_plan=full,
        credentials=credentials,
        ordinary_rest_budget=budget["ordinary"],
        reserve_rest_budget=budget["reserve"],
        verify_private_keys=False,
    )

    assert result["passed"] is False
    assert any(expected_blocker in blocker for blocker in result["blockers"])
