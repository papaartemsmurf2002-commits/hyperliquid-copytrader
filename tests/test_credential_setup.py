from __future__ import annotations

import json
import os
import stat
from dataclasses import replace

import pytest
from eth_account import Account
from fastapi.testclient import TestClient

from hyperliquid_copytrader.config import AccountMode
from hyperliquid_copytrader.credential_setup import (
    CredentialProfileStore,
    CredentialSetupError,
    FleetCredentialProfileRegistry,
    SubaccountResolver,
)
from hyperliquid_copytrader.service import CopyTraderService
from hyperliquid_copytrader.web.app import create_app


API_PRIVATE_KEY = "0x" + "1" * 64
API_WALLET_ADDRESS = str(Account.from_key(API_PRIVATE_KEY).address).lower()
SOURCE_WALLET = "0x1000000000000000000000000000000000000001"
GLOBAL_ACCOUNT = "0x2000000000000000000000000000000000000002"
FOLLOWER_ACCOUNT = "0x3000000000000000000000000000000000000003"


def _payload(**overrides):
    payload = {
        "profile_label": "First mainnet canary",
        "network": "mainnet",
        "source_wallet": SOURCE_WALLET,
        "global_account_address": GLOBAL_ACCOUNT,
        "subaccount_name": "acc7",
        "follower_account_address": FOLLOWER_ACCOUNT,
        "api_wallet_address": API_WALLET_ADDRESS,
        "api_private_key": API_PRIVATE_KEY,
        "expected_account_mode": "unified",
        "coin": "BTC",
        "api_key_is_dedicated": True,
    }
    payload.update(overrides)
    return payload


def _store(tmp_path):
    (tmp_path / ".gitignore").write_text(".secrets/\n", encoding="utf-8")
    return CredentialProfileStore(tmp_path)


def _registry(tmp_path):
    legacy = _store(tmp_path)
    return FleetCredentialProfileRegistry(tmp_path, legacy_store=legacy)


class FakeSubaccountResolver(SubaccountResolver):
    def resolve(self, *, network, global_account_address):
        assert network in {"mainnet", "testnet"}
        assert global_account_address.lower() == GLOBAL_ACCOUNT
        return {
            "status": "ok",
            "network": network,
            "global_account_address": global_account_address,
            "subaccount_count": 1,
            "subaccounts": [
                {
                    "name": "acc7",
                    "address": FOLLOWER_ACCOUNT,
                    "address_length": 42,
                    "perps_account_value_usd": "20",
                    "spot_usdc_usd": "0",
                }
            ],
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def details(self, *, network, follower_account_address):
        assert network in {"mainnet", "testnet"}
        assert follower_account_address.lower() == FOLLOWER_ACCOUNT
        return {
            "status": "ok",
            "network": network,
            "follower_account_address": FOLLOWER_ACCOUNT,
            "detected_account_mode": "unified",
            "perps_account_value_usd": "20",
            "spot_usdc_usd": "0",
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def assert_selection(self, raw):
        if (
            raw.get("subaccount_name") != "acc7"
            or str(raw.get("follower_account_address") or "").lower() != FOLLOWER_ACCOUNT
        ):
            raise CredentialSetupError("subaccount selection mismatch")


def _app(service, tmp_path):
    return create_app(
        service=service,
        credential_root=tmp_path,
        subaccount_resolver=FakeSubaccountResolver(),
    )


def test_credential_store_routes_roles_without_leaking_secret(tmp_path, base_config):
    store = _store(tmp_path)

    result = store.save(_payload(), active_config=base_config)

    assert result["configured"] is True
    assert result["restart_required"] is True
    assert result["signed_action_performed"] is False
    assert result["generic_live_copy_enabled"] is False
    assert result["storage"]["private_key_returned_by_api"] is False
    assert result["storage"]["browser_storage_used"] is False
    assert store.key_path.read_text(encoding="utf-8").strip() == API_PRIVATE_KEY
    profile_text = store.profile_path.read_text(encoding="utf-8")
    env_text = store.env_path.read_text(encoding="utf-8")
    assert API_PRIVATE_KEY not in profile_text
    assert API_PRIVATE_KEY not in env_text
    assert f'HLCT_SOURCE_WALLET="{SOURCE_WALLET}"' in env_text
    assert f'HLCT_GLOBAL_ACCOUNT_ADDRESS="{GLOBAL_ACCOUNT}"' in env_text
    assert 'HLCT_SUBACCOUNT_NAME="acc7"' in env_text
    assert f'HLCT_FOLLOWER_ACCOUNT_ADDRESS="{FOLLOWER_ACCOUNT}"' in env_text
    assert f'HLCT_VAULT_ADDRESS="{FOLLOWER_ACCOUNT}"' in env_text
    assert f'HLCT_API_WALLET_ADDRESS="{API_WALLET_ADDRESS}"' in env_text
    assert 'HLCT_LIVE_COPY_ENABLE="false"' in env_text
    assert 'HLCT_MAX_NOTIONAL_USD="15"' in env_text
    assert str(store.key_path).replace("\\", "/") in env_text
    assert API_PRIVATE_KEY not in json.dumps(result)


def test_credential_store_reports_active_only_after_matching_restart(tmp_path, base_config):
    store = _store(tmp_path)
    store.save(_payload(), active_config=base_config)
    active_config = replace(
        base_config,
        source_wallet=SOURCE_WALLET,
        exchange=replace(
            base_config.exchange,
            follower_account_address=FOLLOWER_ACCOUNT,
            api_wallet_address=API_WALLET_ADDRESS,
            api_private_key=API_PRIVATE_KEY,
            api_private_key_file=str(store.key_path),
            vault_address=FOLLOWER_ACCOUNT,
            expected_account_mode=AccountMode.UNIFIED,
        ),
    )

    status = store.status(active_config=active_config)

    assert status["active_in_current_process"] is True
    assert status["restart_required"] is False


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"api_wallet_address": GLOBAL_ACCOUNT}, "API wallet must be different"),
        ({"follower_account_address": GLOBAL_ACCOUNT}, "must be different"),
        ({"api_private_key": "0x" + "2" * 64}, "belongs to"),
        ({"api_key_is_dedicated": False}, "dedicated API-wallet key"),
    ],
)
def test_credential_store_rejects_role_mixups_without_writing(
    tmp_path, base_config, overrides, message
):
    store = _store(tmp_path)

    with pytest.raises(CredentialSetupError, match=message):
        store.save(_payload(**overrides), active_config=base_config)

    assert not store.key_path.exists()
    assert not store.profile_path.exists()
    assert not store.env_path.exists()


def test_credential_store_preserves_unrelated_env_and_removes_direct_key(tmp_path, base_config):
    store = _store(tmp_path)
    store.env_path.write_text(
        "UNRELATED=value\nHLCT_API_PRIVATE_KEY=must-not-survive\nHLCT_SOURCE_WALLET=old\n",
        encoding="utf-8",
    )

    store.save(_payload(), active_config=base_config)

    env_text = store.env_path.read_text(encoding="utf-8")
    assert "UNRELATED=value" in env_text
    assert "must-not-survive" not in env_text
    assert env_text.count("HLCT_SOURCE_WALLET=") == 1


def test_credential_store_clear_removes_only_managed_profile(tmp_path, base_config):
    store = _store(tmp_path)
    store.env_path.write_text("UNRELATED=value\n", encoding="utf-8")
    store.save(_payload(), active_config=base_config)

    result = store.clear(active_config=base_config)

    assert result["configured"] is False
    assert not store.key_path.exists()
    assert not store.profile_path.exists()
    assert store.env_path.read_text(encoding="utf-8") == "UNRELATED=value\n"


@pytest.mark.parametrize("user_abstraction", ["unified", "unifiedAccount", "default"])
def test_subaccount_resolver_returns_fixed_length_action_address(user_abstraction):
    class FakeInfoClient:
        def info(self, payload):
            if payload == {"type": "subAccounts", "user": GLOBAL_ACCOUNT}:
                return [
                    {
                        "name": "acc7",
                        "subAccountUser": FOLLOWER_ACCOUNT,
                        "master": GLOBAL_ACCOUNT,
                        "clearinghouseState": {"marginSummary": {"accountValue": "20.25"}},
                        "spotState": {"balances": [{"coin": "USDC", "total": "1.5"}]},
                    }
                ]
            if payload == {"type": "userAbstraction", "user": FOLLOWER_ACCOUNT}:
                return user_abstraction
            if payload == {"type": "clearinghouseState", "user": FOLLOWER_ACCOUNT}:
                return {"marginSummary": {"accountValue": "20.25"}}
            if payload == {"type": "spotClearinghouseState", "user": FOLLOWER_ACCOUNT}:
                return {"balances": [{"coin": "USDC", "total": "1.5"}]}
            raise AssertionError(payload)

    resolver = SubaccountResolver(lambda _base_url: FakeInfoClient())

    result = resolver.resolve(network="mainnet", global_account_address=GLOBAL_ACCOUNT)

    assert result["read_only_query"] is True
    assert result["signed_action_performed"] is False
    assert result["subaccounts"] == [
        {
            "name": "acc7",
            "address": FOLLOWER_ACCOUNT,
            "address_length": 42,
            "perps_account_value_usd": "20.25",
            "spot_usdc_usd": "1.5",
        }
    ]
    details = resolver.details(network="mainnet", follower_account_address=FOLLOWER_ACCOUNT)
    assert details["detected_account_mode"] == "unified"
    assert details["perps_account_value_usd"] == "20.25"
    assert details["spot_usdc_usd"] == "1.5"


@pytest.mark.parametrize("user_abstraction", ["portfolioMargin", "dexAbstraction"])
def test_subaccount_details_fail_closed_for_unsupported_account_modes(user_abstraction):
    class FakeInfoClient:
        def info(self, payload):
            if payload == {"type": "userAbstraction", "user": FOLLOWER_ACCOUNT}:
                return user_abstraction
            if payload == {"type": "clearinghouseState", "user": FOLLOWER_ACCOUNT}:
                return {"marginSummary": {"accountValue": "20.25"}}
            if payload == {"type": "spotClearinghouseState", "user": FOLLOWER_ACCOUNT}:
                return {"balances": [{"coin": "USDC", "total": "1.5"}]}
            raise AssertionError(payload)

    resolver = SubaccountResolver(lambda _base_url: FakeInfoClient())

    with pytest.raises(CredentialSetupError, match="unsupported"):
        resolver.details(network="mainnet", follower_account_address=FOLLOWER_ACCOUNT)


def test_subaccount_name_migration_never_changes_action_address_or_key(tmp_path, base_config):
    store = _store(tmp_path)
    store.save(_payload(), active_config=base_config)
    original_key = store.key_path.read_bytes()
    profile = json.loads(store.profile_path.read_text(encoding="utf-8"))
    profile["profile_version"] = 1
    profile.pop("subaccount_name")
    store.profile_path.write_text(json.dumps(profile), encoding="utf-8")

    status = store.migrate_subaccount_name(
        subaccount_name="acc7",
        follower_account_address=FOLLOWER_ACCOUNT,
        active_config=base_config,
    )

    migrated = json.loads(store.profile_path.read_text(encoding="utf-8"))
    assert migrated["profile_version"] == 2
    assert migrated["subaccount_name"] == "acc7"
    assert migrated["follower_account_address"] == FOLLOWER_ACCOUNT
    assert store.key_path.read_bytes() == original_key
    assert status["subaccount_resolution_required"] is False

    mode_status = store.migrate_expected_account_mode(
        expected_account_mode="standard",
        active_config=base_config,
    )
    mode_profile = json.loads(store.profile_path.read_text(encoding="utf-8"))
    assert mode_profile["expected_account_mode"] == "standard"
    assert store.key_path.read_bytes() == original_key
    assert mode_status["profile"]["expected_account_mode"] == "standard"


def test_gui_credential_setup_is_local_redacted_and_restart_scoped(tmp_path, base_config):
    store = _store(tmp_path)
    service = CopyTraderService(base_config)
    client = TestClient(_app(service, tmp_path), base_url="http://testserver")

    lookup = client.post(
        "/api/credentials/subaccounts",
        headers={"origin": "http://testserver"},
        json={"network": "mainnet", "global_account_address": GLOBAL_ACCOUNT},
    )
    details = client.post(
        "/api/credentials/subaccount-details",
        headers={"origin": "http://testserver"},
        json={
            "network": "mainnet",
            "follower_account_address": FOLLOWER_ACCOUNT,
        },
    )
    response = client.post(
        "/api/credentials",
        headers={"origin": "http://testserver"},
        json=_payload(),
    )

    assert lookup.status_code == 200
    assert lookup.json()["subaccounts"][0]["name"] == "acc7"
    assert lookup.json()["signed_action_performed"] is False
    assert details.status_code == 200
    assert details.json()["detected_account_mode"] == "unified"
    assert details.json()["signed_action_performed"] is False
    assert response.status_code == 200
    body = response.json()
    assert body["configured"] is True
    assert body["restart_required"] is True
    assert API_PRIVATE_KEY not in response.text
    status = client.get("/api/credentials")
    assert status.status_code == 200
    assert API_PRIVATE_KEY not in status.text
    assert status.json()["storage"]["application_database_used"] is False
    assert store.key_path.is_file()
    audit = client.get("/api/status").json()["recent_control_audit"][0]
    assert audit["control"] == "credential setup"
    assert API_PRIVATE_KEY not in json.dumps(audit)


def test_gui_credential_setup_rejects_cross_origin_and_never_echoes_bad_key(tmp_path, base_config):
    _store(tmp_path)
    service = CopyTraderService(base_config)
    client = TestClient(_app(service, tmp_path), base_url="http://testserver")
    bad_key = "0x" + "9" * 64

    cross_origin = client.post(
        "/api/credentials",
        headers={"origin": "https://attacker.example"},
        json=_payload(api_private_key=bad_key),
    )
    invalid = client.post(
        "/api/credentials",
        headers={"origin": "http://testserver"},
        json=_payload(api_private_key=bad_key),
    )

    assert cross_origin.status_code == 403
    assert invalid.status_code == 422
    assert bad_key not in cross_origin.text
    assert bad_key not in invalid.text


def test_gui_credential_clear_requires_exact_confirmation(tmp_path, base_config):
    store = _store(tmp_path)
    service = CopyTraderService(base_config)
    client = TestClient(_app(service, tmp_path), base_url="http://testserver")
    headers = {"origin": "http://testserver"}
    assert client.post("/api/credentials", headers=headers, json=_payload()).status_code == 200

    denied = client.post("/api/credentials/clear", headers=headers, json={"confirmation": "forget"})
    cleared = client.post(
        "/api/credentials/clear",
        headers=headers,
        json={"confirmation": "FORGET_LOCAL_CREDENTIALS"},
    )

    assert denied.status_code == 422
    assert store.key_path.exists() is False
    assert cleared.status_code == 200
    assert cleared.json()["configured"] is False


def test_fleet_registry_uses_one_restricted_key_file_and_preserves_legacy_profile(
    tmp_path, base_config
):
    legacy = _store(tmp_path)
    legacy.save(_payload(), active_config=base_config)
    original_env = legacy.env_path.read_bytes()
    original_profile = legacy.profile_path.read_bytes()
    original_key = legacy.key_path.read_bytes()
    registry = FleetCredentialProfileRegistry(tmp_path, legacy_store=legacy)

    result = registry.save(
        _payload(profile_id="acc7", denied_symbols="ETH, xyz:XYZ100"),
        active_config=base_config,
    )

    card = result["profiles"][0]
    vault_key = registry.secret_dir / "acc7" / "api-wallet.key"
    vault_profile = registry.secret_dir / "acc7" / "profile.json"
    assert legacy.env_path.read_bytes() == original_env
    assert legacy.profile_path.read_bytes() == original_profile
    assert legacy.key_path.read_bytes() == original_key
    assert vault_key.is_file() and vault_key != legacy.key_path
    assert vault_key.read_text(encoding="utf-8").strip() == API_PRIVATE_KEY
    assert API_PRIVATE_KEY not in vault_profile.read_text(encoding="utf-8")
    assert API_PRIVATE_KEY not in registry.credential_map_path.read_text(encoding="utf-8")
    assert API_PRIVATE_KEY not in json.dumps(result)
    assert card["eligibility"] == "all_active_markets"
    assert card["denied_symbols"] == ["ETH", "xyz:XYZ100"]
    assert card["private_key_matches_api_wallet"] is True
    assert result["legacy"]["selection_is_explicit_only"] is True
    assert result["runner_credential_map"] == {
        "version": 1,
        "slots": {
            "acc7": {
                "api_wallet_address": API_WALLET_ADDRESS,
                "api_private_key_file": str(vault_key.resolve()),
            }
        },
    }
    if os.name != "nt":
        assert stat.S_IMODE(vault_key.stat().st_mode) == 0o600
        assert stat.S_IMODE(vault_key.parent.stat().st_mode) == 0o700


@pytest.mark.parametrize("duplicate", ["api_wallet", "follower"])
def test_fleet_registry_rejects_duplicate_signer_or_follower(tmp_path, base_config, duplicate):
    registry = _registry(tmp_path)
    registry.save(
        _payload(profile_id="acc7", denied_symbols=["ETH"]),
        active_config=base_config,
    )
    second_key = "0x" + "2" * 64
    second_wallet = str(Account.from_key(second_key).address).lower()
    overrides = {
        "profile_id": "acc8",
        "subaccount_name": "acc8",
        "follower_account_address": "0x4000000000000000000000000000000000000004",
        "api_wallet_address": second_wallet,
        "api_private_key": second_key,
    }
    if duplicate == "api_wallet":
        overrides["api_wallet_address"] = API_WALLET_ADDRESS
        overrides["api_private_key"] = API_PRIVATE_KEY
    else:
        overrides["follower_account_address"] = FOLLOWER_ACCOUNT

    with pytest.raises(CredentialSetupError, match="already assigned"):
        registry.save(_payload(**overrides), active_config=base_config)

    assert not (registry.secret_dir / "acc8").exists()


def test_fleet_registry_legacy_import_selection_and_delete_are_explicit(tmp_path, base_config):
    legacy = _store(tmp_path)
    legacy.save(_payload(), active_config=base_config)
    registry = FleetCredentialProfileRegistry(tmp_path, legacy_store=legacy)
    initial_env = legacy.env_path.read_bytes()

    with pytest.raises(CredentialSetupError, match="Type IMPORT acc7"):
        registry.import_legacy(
            profile_id="acc7",
            confirmation="IMPORT",
            active_config=base_config,
        )
    assert not registry.secret_dir.exists()
    imported = registry.import_legacy(
        profile_id="acc7",
        confirmation="IMPORT acc7",
        active_config=base_config,
    )
    assert imported["legacy"]["selected_profile_id"] == "acc7"
    assert legacy.env_path.read_bytes() == initial_env

    with pytest.raises(CredentialSetupError, match="Type ACTIVATE acc7"):
        registry.select_legacy(
            profile_id="acc7",
            confirmation="ACTIVATE",
            active_config=base_config,
        )
    assert legacy.env_path.read_bytes() == initial_env
    selected = registry.select_legacy(
        profile_id="acc7",
        confirmation="ACTIVATE acc7",
        active_config=base_config,
    )
    assert selected["legacy_selection"]["profile_id"] == "acc7"
    assert selected["legacy_selection"]["signed_action_performed"] is False

    with pytest.raises(CredentialSetupError, match="Type DELETE acc7"):
        registry.delete(
            profile_id="acc7",
            confirmation="DELETE",
            active_config=base_config,
        )
    deleted = registry.delete(
        profile_id="acc7",
        confirmation="DELETE acc7",
        active_config=base_config,
    )
    assert deleted["profile_count"] == 0
    assert deleted["runner_credential_map"] == {"version": 1, "slots": {}}
    assert legacy.key_path.is_file()


def test_fleet_registry_denylist_update_is_explicit_and_does_not_touch_key(tmp_path, base_config):
    registry = _registry(tmp_path)
    registry.save(
        _payload(profile_id="acc7", denied_symbols=["ETH"]),
        active_config=base_config,
    )
    key_path = registry.secret_dir / "acc7" / "api-wallet.key"
    original_key = key_path.read_bytes()

    with pytest.raises(CredentialSetupError, match="Type UPDATE acc7 MARKETS"):
        registry.update_market_policy(
            profile_id="acc7",
            denied_symbols="ETH,SOL",
            confirmation="UPDATE acc7",
            active_config=base_config,
        )

    updated = registry.update_market_policy(
        profile_id="acc7",
        denied_symbols="ETH,SOL",
        confirmation="UPDATE acc7 MARKETS",
        active_config=base_config,
    )

    assert key_path.read_bytes() == original_key
    assert updated["profiles"][0]["eligibility"] == "all_active_markets"
    assert updated["profiles"][0]["denied_symbols"] == ["ETH", "SOL"]
    assert updated["market_policy_update"] == {
        "profile_id": "acc7",
        "eligibility": "all_active_markets",
        "denied_symbols": ["ETH", "SOL"],
        "private_key_file_unchanged": True,
        "signed_action_performed": False,
    }
    assert API_PRIVATE_KEY not in json.dumps(updated)

    full_catalog = [f"M{i}" for i in range(263)]
    expanded = registry.update_market_policy(
        profile_id="acc7",
        denied_symbols=full_catalog,
        confirmation="UPDATE acc7 MARKETS",
        active_config=base_config,
    )
    assert expanded["profiles"][0]["denied_symbols"] == sorted(full_catalog)
    assert key_path.read_bytes() == original_key


def test_gui_multi_profile_routes_and_ui_are_redacted(tmp_path, base_config):
    _store(tmp_path)
    service = CopyTraderService(base_config)
    client = TestClient(_app(service, tmp_path), base_url="http://testserver")
    headers = {"origin": "http://testserver"}

    saved = client.post(
        "/api/credential-profiles",
        headers=headers,
        json=_payload(profile_id="acc7", denied_symbols="ETH"),
    )
    status = client.get("/api/credential-profiles")
    page = client.get("/")

    assert saved.status_code == 200
    assert status.status_code == 200
    assert API_PRIVATE_KEY not in saved.text + status.text + page.text
    assert "Multi-Account Credential Vault" not in page.text
    assert "API wallet private key" not in page.text
    assert "Fleet accounts" in page.text
    assert "30D Profit Leaderboard" in page.text

    duplicate = client.post(
        "/api/credential-profiles",
        headers=headers,
        json=_payload(profile_id="acc8"),
    )
    assert duplicate.status_code == 422
    assert API_PRIVATE_KEY not in duplicate.text

    denied_delete = client.post(
        "/api/credential-profiles/delete",
        headers=headers,
        json={"profile_id": "acc7", "confirmation": "DELETE"},
    )
    deleted = client.post(
        "/api/credential-profiles/delete",
        headers=headers,
        json={"profile_id": "acc7", "confirmation": "DELETE acc7"},
    )
    assert denied_delete.status_code == 422
    assert deleted.status_code == 200
    assert deleted.json()["profile_count"] == 0


def test_gui_market_update_route_requires_exact_confirmation(tmp_path, base_config):
    _store(tmp_path)
    service = CopyTraderService(base_config)
    client = TestClient(_app(service, tmp_path), base_url="http://testserver")
    headers = {"origin": "http://testserver"}
    assert (
        client.post(
            "/api/credential-profiles",
            headers=headers,
            json=_payload(profile_id="acc7", denied_symbols="ETH"),
        ).status_code
        == 200
    )

    denied = client.post(
        "/api/credential-profiles/market-policy",
        headers=headers,
        json={
            "profile_id": "acc7",
            "denied_symbols": "ETH,SOL",
            "confirmation": "UPDATE acc7",
        },
    )
    updated = client.post(
        "/api/credential-profiles/market-policy",
        headers=headers,
        json={
            "profile_id": "acc7",
            "denied_symbols": "ETH,SOL",
            "confirmation": "UPDATE acc7 MARKETS",
        },
    )

    assert denied.status_code == 422
    assert updated.status_code == 200
    assert updated.json()["profiles"][0]["eligibility"] == "all_active_markets"
    assert updated.json()["profiles"][0]["denied_symbols"] == ["ETH", "SOL"]
    assert updated.json()["market_policy_update"]["private_key_file_unchanged"] is True
    assert API_PRIVATE_KEY not in denied.text + updated.text


def test_gui_blocks_market_policy_change_while_continuous_runner_is_online(tmp_path, base_config):
    registry = _registry(tmp_path)
    registry.save(_payload(profile_id="acc7", denied_symbols="ETH"), active_config=base_config)

    class OnlineController:
        def status(self):
            return {"online": True}

    app = create_app(
        service=CopyTraderService(base_config),
        credential_root=tmp_path,
        subaccount_resolver=FakeSubaccountResolver(),
        continuous_launch_controller=OnlineController(),
    )
    client = TestClient(app, base_url="http://testserver")
    response = client.post(
        "/api/credential-profiles/market-policy",
        headers={"origin": "http://testserver"},
        json={
            "profile_id": "acc7",
            "denied_symbols": "ETH,SOL",
            "confirmation": "UPDATE acc7 MARKETS",
        },
    )

    assert response.status_code == 409
    assert "stop the continuous runner" in response.text
    assert registry.status(active_config=base_config)["profiles"][0]["denied_symbols"] == ["ETH"]
