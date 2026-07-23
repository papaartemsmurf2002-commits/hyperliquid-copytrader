from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from hyperliquid_copytrader.continuous_config import (
    BoundContinuousPlan,
    BoundContinuousSlot,
    ContinuousPlan,
    ContinuousSlotConfig,
)
from hyperliquid_copytrader.continuous_preflight import run_continuous_preflight
from hyperliquid_copytrader.market_catalog import CatalogRevision


SOURCE = "0x" + "1" * 40
FOLLOWER = "0x" + "2" * 40
SIGNER = "0x" + "3" * 40
MASTER = "0x" + "4" * 40
NOW_MS = 1_750_000_000_000


def _catalog(*dexes: str) -> CatalogRevision:
    return CatalogRevision(
        sequence=1,
        revision_id="catalog-test",
        policy_version="continuous-v1",
        network="mainnet",
        observed_ms=NOW_MS,
        wire_dexes=tuple(dexes),
        markets=(),
        snapshot_sha256="a" * 64,
        dex_bracket_before_sha256="b" * 64,
        dex_bracket_after_sha256="b" * 64,
    )


def _slot(
    *,
    slot: str = "slot1",
    source: str = SOURCE,
    follower: str = FOLLOWER,
) -> ContinuousSlotConfig:
    return ContinuousSlotConfig(
        slot=slot,
        source_address=source,
        follower_account_address=follower,
        credential_profile_id=f"profile-{slot}",
        multiplier=Decimal("1"),
        max_order_notional_usd=Decimal("10"),
        max_gross_exposure_usd=Decimal("20"),
        max_open_positions=2,
        max_leverage=2,
        action_limit_per_minute=30,
        allowed_markets=("BTC", "xyz:JP225"),
        enabled=True,
    )


def _bound(*slots: tuple[ContinuousSlotConfig, str]) -> BoundContinuousPlan:
    configs = tuple(item[0] for item in slots)
    plan = ContinuousPlan(
        version=1,
        network="mainnet",
        runtime_id="continuous-test",
        startup_baseline_only=True,
        max_combined_gross_usd=Decimal("20"),
        slots=configs,
        path=Path("plan.json"),
        sha256="a" * 64,
    )
    return BoundContinuousPlan(
        plan=plan,
        slots=tuple(
            BoundContinuousSlot(
                config=config,
                api_wallet_address=signer,
                api_private_key_file=Path(f"never-read-{config.slot}.key"),
                global_account_address=MASTER,
                expected_account_mode="unified",
            )
            for config, signer in slots
        ),
    )


class FakeInfo:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.nonflat = False
        self.open_order = False
        self.follower_role: Any = {"role": "subAccount", "data": {"master": MASTER}}
        self.signer_owner = MASTER
        self.valid_until = NOW_MS + 60_000
        self.abstraction: Any = "unifiedAccount"
        self.source_abstraction: Any | None = None
        self.follower_abstraction: Any | None = None
        self.source_spot_total = "100"
        self.source_spot_hold = "10"
        self.subaccounts: list[dict[str, Any]] | None = None
        self.extra_agents: list[dict[str, Any]] | None = None

    def __call__(self, payload: dict[str, Any]) -> Any:
        self.calls.append(dict(payload))
        request_type = payload["type"]
        user = payload.get("user")
        dex = payload.get("dex", "")
        if request_type == "subAccounts":
            if self.subaccounts is not None:
                return self.subaccounts
            positions = [{"position": {"coin": "BTC", "szi": "0.1"}}] if self.nonflat else []
            return [
                {
                    "subAccountUser": FOLLOWER,
                    "master": MASTER,
                    "clearinghouseState": {
                        "assetPositions": positions,
                        "marginSummary": {"accountValue": "0"},
                    },
                    "spotState": {
                        "balances": [{"coin": "USDC", "token": 0, "total": "50", "hold": "5"}]
                    },
                }
            ]
        if request_type == "userRole":
            if user == SIGNER:
                return {"role": "agent", "data": {"user": self.signer_owner}}
            if user == FOLLOWER:
                return self.follower_role
            return {"role": "user"}
        if request_type == "extraAgents":
            return (
                self.extra_agents
                if self.extra_agents is not None
                else [{"address": SIGNER, "validUntil": self.valid_until}]
            )
        if request_type == "userAbstraction":
            if user == SOURCE and self.source_abstraction is not None:
                return self.source_abstraction
            if user == FOLLOWER and self.follower_abstraction is not None:
                return self.follower_abstraction
            return self.abstraction
        if request_type == "vaultDetails":
            return {"leader": MASTER}
        if request_type == "spotClearinghouseState":
            return {
                "balances": [
                    {
                        "coin": "USDC",
                        "token": 0,
                        "total": self.source_spot_total if user == SOURCE else "50",
                        "hold": self.source_spot_hold if user == SOURCE else "5",
                    }
                ]
            }
        if request_type == "clearinghouseState":
            positions = []
            if user == SOURCE:
                positions = [
                    {
                        "position": {
                            "coin": "BTC" if not dex else "JP225",
                            "szi": "1",
                        }
                    }
                ]
            elif user == FOLLOWER and self.nonflat and not dex:
                positions = [{"position": {"coin": "BTC", "szi": "0.1"}}]
            return {
                "assetPositions": positions,
                "marginSummary": {"accountValue": "100" if not dex else "50"},
            }
        if request_type == "openOrders":
            if user == FOLLOWER and self.open_order and not dex:
                return [{"coin": "BTC", "oid": 1}]
            return []
        raise AssertionError(payload)


def test_dynamic_bound_may_expand_only_the_catalog_owned_market_scope() -> None:
    configured = _bound((_slot(), SIGNER))
    expanded_config = replace(
        configured.slots[0].config,
        allowed_markets=("BTC", "xyz:JP225", "hyna:FOO"),
    )
    dynamic = replace(
        configured,
        slots=(
            replace(
                configured.slots[0],
                config=expanded_config,
                dynamic_market_eligibility=True,
            ),
        ),
    )

    report = run_continuous_preflight(
        dynamic,
        network="mainnet",
        info=FakeInfo(),
        observed_ms=NOW_MS,
    )

    assert not any("bound slot does not match" in item for item in report["blockers"])

    changed_risk = replace(
        dynamic,
        slots=(replace(dynamic.slots[0], config=replace(expanded_config, max_leverage=3)),),
    )
    rejected = run_continuous_preflight(
        changed_risk,
        network="mainnet",
        info=FakeInfo(),
        observed_ms=NOW_MS,
    )
    assert any("bound slot does not match" in item for item in rejected["blockers"])


def test_happy_path_is_redacted_and_counts_only_relevant_dex_requests() -> None:
    fake = FakeInfo()
    bound = _bound((_slot(), SIGNER))

    report = run_continuous_preflight(
        bound,
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    result = report["slots"][0]
    assert result["source_dexes"] == ["", "xyz"]
    assert result["follower_dexes"] == ["", "xyz"]
    assert result["identity"]["signer_authorized"] is True
    assert result["identity"]["action_principal"] == MASTER[:8] + "..." + MASTER[-6:]
    assert result["collateral"]["source"] == {
        "token": 0,
        "coin": "USDC",
        "total": "100",
        "hold": "10",
        "available": "90",
        "valid": True,
    }
    assert report["rest_requests"] == {
        "total": 8,
        "calculated_weight": 88,
        "by_type": {
            "clearinghouseState": 3,
            "extraAgents": 1,
            "spotClearinghouseState": 1,
            "subAccounts": 1,
            "userAbstraction": 2,
        },
        "weight_by_type": {
            "clearinghouseState": 6,
            "extraAgents": 20,
            "spotClearinghouseState": 2,
            "subAccounts": 20,
            "userAbstraction": 40,
        },
        "by_slot": {
            "master-1": {
                "total": 2,
                "calculated_weight": 40,
                "by_type": {"extraAgents": 1, "subAccounts": 1},
            },
            "slot1": {
                "total": 6,
                "calculated_weight": 48,
                "by_type": {
                    "clearinghouseState": 3,
                    "spotClearinghouseState": 1,
                    "userAbstraction": 2,
                },
            },
        },
    }
    assert not any(call["type"] == "userRole" for call in fake.calls)
    serialized = json.dumps(report, sort_keys=True)
    assert SOURCE not in serialized
    assert FOLLOWER not in serialized
    assert SIGNER not in serialized
    assert MASTER not in serialized
    assert "never-read-slot1.key" not in serialized


def test_flatness_is_only_required_for_explicit_bounded_canary() -> None:
    fake = FakeInfo()
    fake.nonflat = True
    fake.open_order = True
    bound = _bound((_slot(), SIGNER))

    continuous = run_continuous_preflight(
        bound,
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )
    bounded = run_continuous_preflight(
        bound,
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
        require_flat_and_order_free=True,
        audit_dexes=("", "xyz"),
        catalog=_catalog("", "xyz"),
    )

    assert continuous["passed"] is True
    assert continuous["slots"][0]["follower_nonflat"] is True
    assert continuous["slots"][0]["follower_open_order_count"] is None
    assert continuous["slots"][0]["open_orders_checked"] is False
    assert bounded["passed"] is False
    assert any("requires a flat follower" in item for item in bounded["blockers"])
    assert any("requires an order-free follower" in item for item in bounded["blockers"])


def test_bounded_canary_rejects_an_incomplete_dex_audit_set() -> None:
    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=FakeInfo(),
        observed_ms=NOW_MS,
        require_flat_and_order_free=True,
        audit_dexes=("",),
        catalog=_catalog("", "xyz"),
    )

    assert report["passed"] is False
    assert any("not the pinned complete catalog" in item for item in report["blockers"])


def test_follower_mode_is_proven_once_at_startup() -> None:
    fake = FakeInfo()
    fake.abstraction = {"mode": "default"}

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    assert report["slots"][0]["identity"]["follower_account_mode"] == "unified"

    fake.follower_abstraction = "standard"
    rejected = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )
    assert rejected["passed"] is False
    assert any("follower account mode" in item for item in rejected["blockers"])


def test_standard_source_uses_relevant_dex_equity_without_spot_balance() -> None:
    fake = FakeInfo()
    fake.source_abstraction = "standard"

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    result = report["slots"][0]
    assert result["identity"]["source_account_mode"] == "standard"
    assert result["identity"]["source_equity_basis"] == ("standard_sum_relevant_dex_account_value")
    assert result["collateral"]["source"] == {
        "basis": "sum_relevant_dex_margin_summary_account_value",
        "total": "150",
        "by_dex": {"<default>": "100", "xyz": "50"},
        "valid": True,
    }
    assert not any(
        call["type"] == "spotClearinghouseState" and call.get("user") == SOURCE
        for call in fake.calls
    )


def test_unified_source_may_use_all_usdc_as_perp_collateral() -> None:
    fake = FakeInfo()
    fake.source_spot_hold = fake.source_spot_total

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    source = report["slots"][0]["collateral"]["source"]
    assert source["total"] == "100"
    assert source["available"] == "0"
    assert source["valid"] is True


def test_standard_hip3_only_source_does_not_count_default_dex_equity() -> None:
    fake = FakeInfo()
    fake.source_abstraction = "standard"
    slot = _slot()
    slot = ContinuousSlotConfig(
        slot=slot.slot,
        source_address=slot.source_address,
        follower_account_address=slot.follower_account_address,
        credential_profile_id=slot.credential_profile_id,
        multiplier=slot.multiplier,
        max_order_notional_usd=slot.max_order_notional_usd,
        max_gross_exposure_usd=slot.max_gross_exposure_usd,
        max_open_positions=slot.max_open_positions,
        max_leverage=slot.max_leverage,
        action_limit_per_minute=slot.action_limit_per_minute,
        allowed_markets=("xyz:JP225",),
        enabled=True,
    )

    report = run_continuous_preflight(
        _bound((slot, SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    result = report["slots"][0]
    assert result["source_dexes"] == ["xyz"]
    assert result["collateral"]["source"]["total"] == "50"
    assert result["collateral"]["source"]["by_dex"] == {"xyz": "50"}
    assert not any(
        call["type"] == "clearinghouseState"
        and call.get("user") == SOURCE
        and call.get("dex", "") == ""
        for call in fake.calls
    )


def test_subaccount_principal_and_expired_agent_are_reported_without_secrets() -> None:
    fake = FakeInfo()
    fake.follower_role = {"role": "subAccount", "data": {"master": MASTER}}
    fake.signer_owner = MASTER
    fake.valid_until = NOW_MS

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    identity = report["slots"][0]["identity"]
    assert identity["follower_role"] == "subaccount_inventory"
    assert identity["action_principal"] == MASTER[:8] + "..." + MASTER[-6:]
    assert identity["signer_authorized"] is False
    assert identity["signer_valid_until_ms"] == NOW_MS
    assert any("expired or expires" in item for item in report["blockers"])


def test_shared_master_topology_and_agents_are_queried_once_for_multiple_slots() -> None:
    follower2 = "0x" + "6" * 40
    signer2 = "0x" + "7" * 40
    second = _slot(slot="slot2", source="0x" + "5" * 40, follower=follower2)
    fake = FakeInfo()
    base = fake({"type": "subAccounts", "user": MASTER})[0]
    fake.calls.clear()
    fake.subaccounts = [
        base,
        {
            **base,
            "subAccountUser": follower2,
        },
    ]
    fake.extra_agents = [
        {"address": SIGNER, "validUntil": fake.valid_until},
        {"address": signer2, "validUntil": fake.valid_until},
    ]

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER), (second, signer2)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is True
    assert [call["type"] for call in fake.calls].count("subAccounts") == 1
    assert [call["type"] for call in fake.calls].count("extraAgents") == 1
    assert [call["type"] for call in fake.calls].count("userRole") == 0


def test_ten_slot_clean_startup_uses_one_master_inventory_and_stays_below_budget() -> None:
    fake = FakeInfo()
    slots: list[tuple[ContinuousSlotConfig, str]] = []
    subaccounts: list[dict[str, Any]] = []
    agents: list[dict[str, Any]] = []
    for index in range(1, 11):
        source = f"0x{0x1000 + index:040x}"
        follower = f"0x{0x2000 + index:040x}"
        signer = f"0x{0x3000 + index:040x}"
        slots.append(
            (
                replace(
                    _slot(slot=f"slot{index}", source=source, follower=follower),
                    allowed_markets=("BTC",),
                ),
                signer,
            )
        )
        subaccounts.append(
            {
                "subAccountUser": follower,
                "master": MASTER,
                "clearinghouseState": {
                    "assetPositions": [],
                    "marginSummary": {"accountValue": "0"},
                },
                "spotState": {
                    "balances": [{"coin": "USDC", "token": 0, "total": "50", "hold": "0"}]
                },
            }
        )
        agents.append({"address": signer, "validUntil": NOW_MS + 60_000})
    fake.subaccounts = subaccounts
    fake.extra_agents = agents

    report = run_continuous_preflight(
        _bound(*slots),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    requests = report["rest_requests"]
    assert report["passed"] is True
    assert requests["total"] == 42
    assert requests["calculated_weight"] == 480
    assert requests["by_type"]["subAccounts"] == 1
    assert requests["by_type"]["extraAgents"] == 1
    assert requests["by_type"].get("userRole", 0) == 0
    assert requests["calculated_weight"] < 720


def test_missing_subaccount_or_signer_blocks_only_the_affected_slot() -> None:
    fake = FakeInfo()
    fake.subaccounts = []
    fake.extra_agents = []

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is False
    assert any("absent from" in item for item in report["blockers"])
    assert any("not listed" in item for item in report["blockers"])


def test_duplicate_signers_are_a_top_level_blocker() -> None:
    second = _slot(
        slot="slot2",
        source="0x" + "5" * 40,
        follower="0x" + "6" * 40,
    )
    fake = FakeInfo()
    report = run_continuous_preflight(
        _bound((_slot(), SIGNER), (second, SIGNER)),
        network="mainnet",
        info=fake,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is False
    assert "enabled slots require distinct API wallet signers" in report["blockers"]


def test_network_mismatch_fails_before_any_info_request() -> None:
    calls: list[dict[str, Any]] = []

    def should_not_call(payload: dict[str, Any]) -> Any:
        calls.append(payload)
        raise AssertionError("network mismatch must not query")

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="testnet",
        info=should_not_call,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is False
    assert report["rest_requests"]["total"] == 0
    assert calls == []


def test_query_exception_message_is_not_exposed() -> None:
    secret = "private-key-material-must-not-appear"

    def broken(_payload: dict[str, Any]) -> Any:
        raise RuntimeError(secret)

    report = run_continuous_preflight(
        _bound((_slot(), SIGNER)),
        network="mainnet",
        info=broken,
        observed_ms=NOW_MS,
    )

    assert report["passed"] is False
    assert report["rest_requests"]["total"] > 0
    assert secret not in json.dumps(report)
