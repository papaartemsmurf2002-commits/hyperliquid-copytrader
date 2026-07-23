from __future__ import annotations

import json
import os
import re
import stat
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from eth_account import Account

from .config import AccountMode, AppConfig, MAINNET_REST, TESTNET_REST
from .markets import canonical_market_symbol
from .observer import HyperliquidInfoClient
from .unified_account import HyperliquidUserAbstraction, classify_user_abstraction


ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
PROFILE_VERSION = 2
FLEET_PROFILE_VERSION = 3
FLEET_CREDENTIAL_MAP_VERSION = 1
PROFILE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
MANAGED_BLOCK_START = "# BEGIN HLCT GUI CREDENTIAL PROFILE"
MANAGED_BLOCK_END = "# END HLCT GUI CREDENTIAL PROFILE"
MANAGED_ENV_KEYS = (
    "HLCT_PROFILE_LABEL",
    "HLCT_PROFILE_NETWORK",
    "HLCT_GLOBAL_ACCOUNT_ADDRESS",
    "HLCT_SUBACCOUNT_NAME",
    "HLCT_MODE",
    "HLCT_SOURCE_WALLET",
    "HLCT_SOURCE_NETWORK",
    "HLCT_SOURCE_DEX_SCOPE",
    "HLCT_FOLLOWER_ACCOUNT_ADDRESS",
    "HLCT_VAULT_ADDRESS",
    "HLCT_API_WALLET_ADDRESS",
    "HLCT_API_PRIVATE_KEY",
    "HLCT_API_PRIVATE_KEY_FILE",
    "HLCT_EXPECTED_ACCOUNT_MODE",
    "HLCT_ALLOWED_SYMBOLS",
    "HLCT_LIVE_ENABLE",
    "HLCT_CONFIRM_MAINNET_LIVE",
    "HLCT_LIVE_COPY_ENABLE",
    "HLCT_ALLOW_MASTER_PRIVATE_KEY",
    "HLCT_MAX_NOTIONAL_USD",
    "HLCT_MAX_GROSS_EXPOSURE_USD",
    "HLCT_MAX_LEVERAGE",
    "HLCT_MAX_NEW_INTENTS_PER_CYCLE",
    "HLCT_MAX_OPEN_INTENTS",
    "HLCT_MAX_EXCHANGE_ACTIONS_PER_MINUTE",
    "HLCT_CIRCUIT_BREAKER_FAILURE_THRESHOLD",
    "HLCT_CIRCUIT_BREAKER_COOLDOWN_MS",
    "HLCT_EXCHANGE_ACTION_TIMEOUT_S",
    "HLCT_EXCHANGE_EXPIRES_AFTER_MS",
    "HLCT_DEAD_MAN_CANCEL_MS",
    "HLCT_DEAD_MAN_POLICY",
    "HLCT_CONTAINMENT_WATCHDOG_TTL_MS",
    "HLCT_DB_PATH",
    "HLCT_KILL_SWITCH_PATH",
    "HLCT_RUNTIME_LOCK_DIR",
)


class CredentialSetupError(ValueError):
    pass


@dataclass(frozen=True)
class CredentialSetup:
    profile_label: str
    network: str
    source_wallet: str
    global_account_address: str
    subaccount_name: str
    follower_account_address: str
    api_wallet_address: str
    api_private_key: str
    expected_account_mode: str
    coin: str
    api_key_is_dedicated: bool


@dataclass(frozen=True)
class FleetCredentialSetup:
    profile_id: str
    credentials: CredentialSetup
    eligibility: str
    denied_symbols: tuple[str, ...]


class SubaccountResolver:
    """Resolve display names to exact on-chain subaccount addresses read-only."""

    def __init__(self, client_factory: Callable[[str], Any] | None = None) -> None:
        self.client_factory = client_factory or (
            lambda base_url: HyperliquidInfoClient(base_url, timeout_s=8.0)
        )

    def resolve(self, *, network: str, global_account_address: str) -> dict[str, Any]:
        normalized_network = str(network or "").strip().lower()
        if normalized_network not in {"mainnet", "testnet"}:
            raise CredentialSetupError("Network must be mainnet or testnet.")
        owner = str(global_account_address or "").strip().lower()
        if not ADDRESS_RE.fullmatch(owner):
            raise CredentialSetupError(
                "Main / global account address must be a 42-character 0x address."
            )
        base_url = MAINNET_REST if normalized_network == "mainnet" else TESTNET_REST
        payload = self.client_factory(base_url).info({"type": "subAccounts", "user": owner})
        if payload is None:
            rows: list[Any] = []
        elif isinstance(payload, list):
            rows = payload
        else:
            raise CredentialSetupError("Hyperliquid subaccount response must be a list.")
        if len(rows) > 50:
            raise CredentialSetupError("Hyperliquid returned more than 50 subaccounts.")

        resolved: list[dict[str, Any]] = []
        seen_addresses: set[str] = set()
        for index, item in enumerate(rows):
            if not isinstance(item, dict):
                raise CredentialSetupError(
                    f"Hyperliquid subaccount row {index + 1} must be an object."
                )
            name = str(item.get("name") or "").strip()
            address = str(item.get("subAccountUser") or "").strip().lower()
            master = str(item.get("master") or "").strip().lower()
            if not name or len(name) > 64 or not name.isprintable():
                raise CredentialSetupError(
                    f"Hyperliquid subaccount row {index + 1} has an invalid name."
                )
            if not ADDRESS_RE.fullmatch(address):
                raise CredentialSetupError(
                    f"Hyperliquid subaccount {name} has an invalid action address."
                )
            if master != owner:
                raise CredentialSetupError(
                    f"Hyperliquid subaccount {name} does not belong to the entered main account."
                )
            if address in seen_addresses:
                raise CredentialSetupError("Hyperliquid returned a duplicate subaccount address.")
            seen_addresses.add(address)
            resolved.append(
                {
                    "name": name,
                    "address": address,
                    "address_length": len(address),
                    "perps_account_value_usd": _subaccount_account_value(item),
                    "spot_usdc_usd": _subaccount_spot_usdc(item),
                }
            )
        return {
            "status": "ok",
            "network": normalized_network,
            "global_account_address": owner,
            "subaccount_count": len(resolved),
            "subaccounts": resolved,
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def details(self, *, network: str, follower_account_address: str) -> dict[str, Any]:
        normalized_network = str(network or "").strip().lower()
        if normalized_network not in {"mainnet", "testnet"}:
            raise CredentialSetupError("Network must be mainnet or testnet.")
        address = str(follower_account_address or "").strip().lower()
        if not ADDRESS_RE.fullmatch(address):
            raise CredentialSetupError(
                "Trading subaccount action address must be a 42-character 0x address."
            )
        base_url = MAINNET_REST if normalized_network == "mainnet" else TESTNET_REST
        client = self.client_factory(base_url)
        abstraction = client.info({"type": "userAbstraction", "user": address})
        clearinghouse = client.info({"type": "clearinghouseState", "user": address})
        spot = client.info({"type": "spotClearinghouseState", "user": address})
        detected_mode = _detected_account_mode(abstraction)
        return {
            "status": "ok",
            "network": normalized_network,
            "follower_account_address": address,
            "detected_account_mode": detected_mode,
            "perps_account_value_usd": _margin_account_value(clearinghouse),
            "spot_usdc_usd": _spot_usdc_total(spot),
            "read_only_query": True,
            "signed_action_performed": False,
        }

    def assert_selection(self, raw: Mapping[str, Any]) -> None:
        name = str(raw.get("subaccount_name") or "").strip()
        address = str(raw.get("follower_account_address") or "").strip().lower()
        result = self.resolve(
            network=str(raw.get("network") or ""),
            global_account_address=str(raw.get("global_account_address") or ""),
        )
        matches = [
            item
            for item in result["subaccounts"]
            if item["name"] == name and item["address"] == address
        ]
        if len(matches) != 1:
            raise CredentialSetupError(
                "Selected subaccount name and on-chain address do not match the entered main "
                "account. Use Find My Subaccounts and choose the intended row."
            )
        details = self.details(
            network=str(raw.get("network") or ""),
            follower_account_address=address,
        )
        expected_mode = str(raw.get("expected_account_mode") or "").strip().lower()
        if details["detected_account_mode"] != expected_mode:
            raise CredentialSetupError(
                "Selected subaccount mode mismatch: entered "
                f"{expected_mode}, detected {details['detected_account_mode']}. "
                "Refresh the subaccount selection and use the detected mode."
            )


class CredentialProfileStore:
    """Persist one active local credential bundle without exposing its secret."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.env_path = self.root / ".env"
        self.secret_dir = self.root / ".secrets" / "operator-profile"
        self.key_path = self.secret_dir / "api-wallet.key"
        self.profile_path = self.secret_dir / "profile.json"
        self.state_dir = self.root / "data" / "mainnet-canary"

    def validate(self, raw: Mapping[str, Any]) -> CredentialSetup:
        return self._validate(raw)

    def save(self, raw: Mapping[str, Any], *, active_config: AppConfig) -> dict[str, Any]:
        setup = self._validate(raw)
        self._assert_secret_directory_is_ignored()
        self.secret_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        _restrict_permissions(self.secret_dir, directory=True)

        normalized_key = setup.api_private_key.lower()
        _atomic_write(self.key_path, normalized_key + "\n", mode=0o600)

        profile = {
            "profile_version": PROFILE_VERSION,
            "profile_label": setup.profile_label,
            "network": setup.network,
            "source_wallet": setup.source_wallet,
            "global_account_address": setup.global_account_address,
            "subaccount_name": setup.subaccount_name,
            "follower_account_address": setup.follower_account_address,
            "api_wallet_address": setup.api_wallet_address,
            "expected_account_mode": setup.expected_account_mode,
            "coin": setup.coin,
            "api_private_key_file": str(self.key_path),
            "generic_live_copy_enabled": False,
        }
        _atomic_write(
            self.profile_path,
            json.dumps(profile, indent=2, sort_keys=True) + "\n",
            mode=0o600,
        )
        self._write_env(profile)
        return self.status(active_config=active_config, saved=True)

    def clear(self, *, active_config: AppConfig) -> dict[str, Any]:
        self._remove_managed_env()
        for path in (self.key_path, self.profile_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        return self.status(active_config=active_config, saved=False)

    def migrate_subaccount_name(
        self,
        *,
        subaccount_name: str,
        follower_account_address: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        """Add a verified display name without changing the saved action address or key."""

        profile = self._read_profile()
        if profile is None or not self.key_path.is_file():
            raise CredentialSetupError("No configured local profile is available to migrate.")
        name = str(subaccount_name or "").strip()
        if not 1 <= len(name) <= 64 or not name.isprintable():
            raise CredentialSetupError("Subaccount name / ID must be 1-64 printable characters.")
        address = str(follower_account_address or "").strip().lower()
        if address != str(profile.get("follower_account_address") or "").lower():
            raise CredentialSetupError(
                "Subaccount-name migration cannot change the configured action address."
            )
        profile["profile_version"] = PROFILE_VERSION
        profile["subaccount_name"] = name
        _atomic_write(
            self.profile_path,
            json.dumps(profile, indent=2, sort_keys=True) + "\n",
            mode=0o600,
        )
        self._write_env(profile)
        return self.status(active_config=active_config, saved=True)

    def migrate_expected_account_mode(
        self,
        *,
        expected_account_mode: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        """Correct only the public expected-mode field after read-only detection."""

        profile = self._read_profile()
        if profile is None or not self.key_path.is_file():
            raise CredentialSetupError("No configured local profile is available to migrate.")
        mode = str(expected_account_mode or "").strip().lower()
        if mode not in {AccountMode.STANDARD.value, AccountMode.UNIFIED.value}:
            raise CredentialSetupError("Account mode must be explicitly Standard or Unified.")
        profile["profile_version"] = PROFILE_VERSION
        profile["expected_account_mode"] = mode
        _atomic_write(
            self.profile_path,
            json.dumps(profile, indent=2, sort_keys=True) + "\n",
            mode=0o600,
        )
        self._write_env(profile)
        return self.status(active_config=active_config, saved=True)

    def status(self, *, active_config: AppConfig, saved: bool | None = None) -> dict[str, Any]:
        return self._status(active_config=active_config, saved=saved, verify_secret=True)

    def public_status(
        self, *, active_config: AppConfig, saved: bool | None = None
    ) -> dict[str, Any]:
        """Return public profile metadata without opening or deriving the signer key."""

        return self._status(active_config=active_config, saved=saved, verify_secret=False)

    def _status(
        self,
        *,
        active_config: AppConfig,
        saved: bool | None,
        verify_secret: bool,
    ) -> dict[str, Any]:
        profile = self._read_profile()
        secret_present = self.key_path.is_file()
        secret_matches: bool | None = None if not verify_secret else False
        if verify_secret and secret_present and profile:
            try:
                key = self.key_path.read_text(encoding="utf-8").strip()
                derived = str(Account.from_key(key).address).lower()
                secret_matches = derived == str(profile.get("api_wallet_address") or "").lower()
            except (OSError, ValueError):
                secret_matches = False

        active = (
            profile is not None
            and secret_present
            and self._profile_is_active(profile, active_config=active_config)
        )
        configured = (
            bool(profile) and secret_present and (secret_matches is True or not verify_secret)
        )
        subaccount_resolution_required = (
            profile is not None and not str(profile.get("subaccount_name") or "").strip()
        )
        payload: dict[str, Any] = {
            "status": "configured" if configured else "not_configured",
            "configured": configured,
            "saved": configured if saved is None else saved,
            "profile_version": (
                int(profile.get("profile_version") or PROFILE_VERSION)
                if profile
                else PROFILE_VERSION
            ),
            "storage": {
                "profile_file": str(self.profile_path),
                "private_key_file": str(self.key_path),
                "env_file": str(self.env_path),
                "private_key_present": secret_present,
                "private_key_matches_api_wallet": secret_matches,
                "private_key_returned_by_api": False,
                "browser_storage_used": False,
                "application_database_used": False,
                "filesystem_permissions": "current_user_only",
            },
            "active_in_current_process": active,
            "restart_required": configured and not active,
            "subaccount_resolution_required": subaccount_resolution_required,
            "generic_live_copy_enabled": False,
            "signed_action_performed": False,
            "credential_content_read": verify_secret,
            "private_key_verification_deferred_to_fenced_runtime": not verify_secret,
            "verification_scope": (
                (
                    "local format, role separation, and private-key-to-API-wallet match; "
                    "exchange authorization is verified later by read-only canary readiness"
                )
                if verify_secret
                else (
                    "public profile metadata and key-file presence only; signer content is "
                    "loaded and verified only after fenced launch admission"
                )
            ),
            "restart_command": ".\\.venv\\Scripts\\hl-copytrader.exe serve",
        }
        if profile:
            payload["profile"] = {
                key: profile.get(key)
                for key in (
                    "profile_label",
                    "network",
                    "source_wallet",
                    "global_account_address",
                    "subaccount_name",
                    "follower_account_address",
                    "api_wallet_address",
                    "expected_account_mode",
                    "coin",
                )
            }
            payload["next_read_only_command"] = (
                ".\\.venv\\Scripts\\hl-copytrader.exe serve "
                "--continuous-plan <continuous-plan.json>"
                if profile.get("network") == "mainnet"
                else ".\\.venv\\Scripts\\hl-copytrader.exe preflight --mode testnet"
            )
        return payload

    def _validate(self, raw: Mapping[str, Any]) -> CredentialSetup:
        profile_label = str(raw.get("profile_label") or "Mainnet canary").strip()
        if not 1 <= len(profile_label) <= 64:
            raise CredentialSetupError("Profile name must be between 1 and 64 characters.")

        network = str(raw.get("network") or "").strip().lower()
        if network not in {"mainnet", "testnet"}:
            raise CredentialSetupError("Network must be mainnet or testnet.")
        expected_mode = str(raw.get("expected_account_mode") or "").strip().lower()
        if expected_mode not in {AccountMode.STANDARD.value, AccountMode.UNIFIED.value}:
            raise CredentialSetupError("Account mode must be explicitly Standard or Unified.")
        coin = str(raw.get("coin") or "").strip().upper()
        if coin not in {"BTC", "ETH"}:
            raise CredentialSetupError("First canary market must be BTC or ETH.")
        subaccount_name = str(raw.get("subaccount_name") or "").strip()
        if not 1 <= len(subaccount_name) <= 64 or not subaccount_name.isprintable():
            raise CredentialSetupError("Subaccount name / ID must be 1-64 printable characters.")

        addresses = {
            "source_wallet": "Source trader address",
            "global_account_address": "Main / global account address",
            "follower_account_address": "Trading subaccount address",
            "api_wallet_address": "API wallet address",
        }
        normalized: dict[str, str] = {}
        for field, label in addresses.items():
            value = str(raw.get(field) or "").strip().lower()
            if not ADDRESS_RE.fullmatch(value):
                raise CredentialSetupError(f"{label} must be a 42-character 0x address.")
            normalized[field] = value

        if normalized["source_wallet"] == normalized["follower_account_address"]:
            raise CredentialSetupError("Source trader and trading subaccount must be different.")
        if normalized["global_account_address"] == normalized["follower_account_address"]:
            raise CredentialSetupError(
                "Main / global account and trading subaccount must be different; use an isolated subaccount."
            )
        api_wallet = normalized["api_wallet_address"]
        for field, label in (
            ("source_wallet", "source trader"),
            ("global_account_address", "main / global account"),
            ("follower_account_address", "trading subaccount"),
        ):
            if api_wallet == normalized[field]:
                raise CredentialSetupError(f"API wallet must be different from the {label}.")

        private_key = str(raw.get("api_private_key") or "").strip()
        if not PRIVATE_KEY_RE.fullmatch(private_key):
            raise CredentialSetupError("API private key must be a 0x-prefixed 32-byte value.")
        if set(private_key[2:]) == {"0"}:
            raise CredentialSetupError("API private key cannot be all zeroes.")
        try:
            derived = str(Account.from_key(private_key).address).lower()
        except (ValueError, TypeError) as exc:
            raise CredentialSetupError(f"API private key could not be loaded: {exc}") from exc
        if derived != api_wallet:
            raise CredentialSetupError(
                f"API private key belongs to {derived}, not the entered API wallet address."
            )
        dedicated = raw.get("api_key_is_dedicated") is True
        if not dedicated:
            raise CredentialSetupError(
                "Confirm that this is the dedicated API-wallet key, never the main-account key."
            )

        return CredentialSetup(
            profile_label=profile_label,
            network=network,
            source_wallet=normalized["source_wallet"],
            global_account_address=normalized["global_account_address"],
            subaccount_name=subaccount_name,
            follower_account_address=normalized["follower_account_address"],
            api_wallet_address=api_wallet,
            api_private_key=private_key,
            expected_account_mode=expected_mode,
            coin=coin,
            api_key_is_dedicated=dedicated,
        )

    def _write_env(self, profile: Mapping[str, Any]) -> None:
        network = str(profile["network"])
        mainnet = network == "mainnet"
        state_dir = self.state_dir.resolve()
        values = {
            "HLCT_PROFILE_LABEL": str(profile["profile_label"]),
            "HLCT_PROFILE_NETWORK": network,
            "HLCT_GLOBAL_ACCOUNT_ADDRESS": str(profile["global_account_address"]),
            "HLCT_SUBACCOUNT_NAME": str(profile["subaccount_name"]),
            "HLCT_MODE": "live" if mainnet else "testnet",
            "HLCT_SOURCE_WALLET": str(profile["source_wallet"]),
            "HLCT_SOURCE_NETWORK": network,
            "HLCT_SOURCE_DEX_SCOPE": "strict",
            "HLCT_FOLLOWER_ACCOUNT_ADDRESS": str(profile["follower_account_address"]),
            "HLCT_VAULT_ADDRESS": str(profile["follower_account_address"]),
            "HLCT_API_WALLET_ADDRESS": str(profile["api_wallet_address"]),
            "HLCT_API_PRIVATE_KEY": "",
            "HLCT_API_PRIVATE_KEY_FILE": str(self.key_path),
            "HLCT_EXPECTED_ACCOUNT_MODE": str(profile["expected_account_mode"]),
            "HLCT_ALLOWED_SYMBOLS": str(profile["coin"]),
            "HLCT_LIVE_ENABLE": _env_bool(mainnet),
            "HLCT_CONFIRM_MAINNET_LIVE": _env_bool(mainnet),
            "HLCT_LIVE_COPY_ENABLE": "false",
            "HLCT_ALLOW_MASTER_PRIVATE_KEY": "false",
            "HLCT_MAX_NOTIONAL_USD": "15",
            "HLCT_MAX_GROSS_EXPOSURE_USD": "15",
            "HLCT_MAX_LEVERAGE": "1",
            "HLCT_MAX_NEW_INTENTS_PER_CYCLE": "1",
            "HLCT_MAX_OPEN_INTENTS": "1",
            "HLCT_MAX_EXCHANGE_ACTIONS_PER_MINUTE": "12",
            "HLCT_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "1",
            "HLCT_CIRCUIT_BREAKER_COOLDOWN_MS": "300000",
            "HLCT_EXCHANGE_ACTION_TIMEOUT_S": "15",
            "HLCT_EXCHANGE_EXPIRES_AFTER_MS": "10000",
            "HLCT_DEAD_MAN_CANCEL_MS": "60000",
            "HLCT_DEAD_MAN_POLICY": "watchdog_fallback" if mainnet else "exchange_required",
            "HLCT_CONTAINMENT_WATCHDOG_TTL_MS": "15000",
            "HLCT_DB_PATH": str(state_dir / "mainnet-canary.sqlite3"),
            "HLCT_KILL_SWITCH_PATH": str(state_dir / "KILL_SWITCH"),
            "HLCT_RUNTIME_LOCK_DIR": str(state_dir / "runtime-locks"),
        }
        original = self.env_path.read_text(encoding="utf-8") if self.env_path.exists() else ""
        cleaned = _without_managed_env(original)
        block = [MANAGED_BLOCK_START]
        block.extend(f"{key}={_quote_env(values[key])}" for key in MANAGED_ENV_KEYS)
        block.append(MANAGED_BLOCK_END)
        content = cleaned.rstrip() + ("\n\n" if cleaned.strip() else "") + "\n".join(block) + "\n"
        _atomic_write(self.env_path, content, mode=0o600)

    def _remove_managed_env(self) -> None:
        if not self.env_path.exists():
            return
        content = _without_managed_env(self.env_path.read_text(encoding="utf-8"))
        _atomic_write(
            self.env_path, content.rstrip() + ("\n" if content.strip() else ""), mode=0o600
        )

    def _read_profile(self) -> dict[str, Any] | None:
        try:
            raw = json.loads(self.profile_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(raw, dict) or raw.get("profile_version") not in {1, PROFILE_VERSION}:
            return None
        raw.setdefault("subaccount_name", "")
        return raw

    def _profile_is_active(self, profile: Mapping[str, Any], *, active_config: AppConfig) -> bool:
        return all(
            (
                active_config.source_wallet.lower()
                == str(profile.get("source_wallet") or "").lower(),
                active_config.exchange.follower_account_address.lower()
                == str(profile.get("follower_account_address") or "").lower(),
                active_config.exchange.api_wallet_address.lower()
                == str(profile.get("api_wallet_address") or "").lower(),
                active_config.exchange.api_private_key_file
                and Path(active_config.exchange.api_private_key_file).resolve()
                == self.key_path.resolve(),
                active_config.exchange.expected_account_mode.value
                == str(profile.get("expected_account_mode") or ""),
            )
        )

    def _assert_secret_directory_is_ignored(self) -> None:
        ignore_path = self.root / ".gitignore"
        try:
            content = ignore_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CredentialSetupError(f"Cannot verify .secrets ignore rule: {exc}") from exc
        ignored = any(
            line.strip().rstrip("/") == ".secrets"
            for line in content.splitlines()
            if not line.lstrip().startswith("#")
        )
        if not ignored:
            raise CredentialSetupError("Refusing to save: .secrets/ is not ignored by Git.")


class FleetCredentialProfileRegistry:
    """Store multiple isolated signer profiles without changing the active legacy profile.

    Public role metadata and a runner-compatible credential map are JSON. Every secret lives in
    its own ACL-restricted file. Registry reads may verify that a key still derives to the declared
    API wallet, but key content is never included in a returned payload.
    """

    def __init__(self, root: Path, *, legacy_store: CredentialProfileStore | None = None) -> None:
        self.root = root.resolve()
        self.secret_dir = self.root / ".secrets" / "operator-profiles"
        self.credential_map_path = self.secret_dir / "credential-map.json"
        self.legacy_store = legacy_store or CredentialProfileStore(self.root)

    def validate(self, raw: Mapping[str, Any]) -> FleetCredentialSetup:
        profile_id = str(raw.get("profile_id") or "").strip().lower()
        if not PROFILE_ID_RE.fullmatch(profile_id):
            raise CredentialSetupError(
                "Profile ID must be 1-64 lowercase letters, numbers, underscores, or hyphens."
            )
        credentials = self.legacy_store.validate(raw)
        if credentials.network != "mainnet":
            raise CredentialSetupError("Fleet credential profiles are mainnet-only.")
        if "allowed_symbols" in raw:
            raise CredentialSetupError(
                "Fleet profiles use all_active_markets eligibility and a denylist, not a snapshot allowlist."
            )
        eligibility = str(raw.get("eligibility") or "all_active_markets").strip().lower()
        if eligibility != "all_active_markets":
            raise CredentialSetupError("Fleet eligibility must be all_active_markets.")
        denied_symbols = _parse_denied_symbols(raw.get("denied_symbols", []))
        return FleetCredentialSetup(
            profile_id=profile_id,
            credentials=credentials,
            eligibility=eligibility,
            denied_symbols=denied_symbols,
        )

    def save(self, raw: Mapping[str, Any], *, active_config: AppConfig) -> dict[str, Any]:
        setup = self.validate(raw)
        self.legacy_store._assert_secret_directory_is_ignored()
        profile_dir = self._profile_dir(setup.profile_id)
        if profile_dir.exists():
            raise CredentialSetupError(f"Profile ID {setup.profile_id!r} already exists.")

        records = self._valid_records()
        api_wallet = setup.credentials.api_wallet_address
        follower = setup.credentials.follower_account_address
        if any(record["api_wallet_address"] == api_wallet for record in records):
            raise CredentialSetupError(
                "That API wallet address is already assigned to another saved profile."
            )
        if any(record["follower_account_address"] == follower for record in records):
            raise CredentialSetupError(
                "That follower subaccount is already assigned to another saved profile."
            )

        self.secret_dir.mkdir(parents=True, exist_ok=True)
        _restrict_permissions(self.secret_dir, directory=True)
        profile_dir.mkdir(parents=False, exist_ok=False)
        _restrict_permissions(profile_dir, directory=True)
        key_path = profile_dir / "api-wallet.key"
        profile_path = profile_dir / "profile.json"
        profile = {
            "profile_version": FLEET_PROFILE_VERSION,
            "profile_id": setup.profile_id,
            "profile_label": setup.credentials.profile_label,
            "network": setup.credentials.network,
            "source_wallet": setup.credentials.source_wallet,
            "global_account_address": setup.credentials.global_account_address,
            "subaccount_name": setup.credentials.subaccount_name,
            "follower_account_address": setup.credentials.follower_account_address,
            "api_wallet_address": setup.credentials.api_wallet_address,
            "expected_account_mode": setup.credentials.expected_account_mode,
            "coin": setup.credentials.coin,
            "eligibility": setup.eligibility,
            "denied_symbols": list(setup.denied_symbols),
            "api_private_key_file": str(key_path.resolve()),
            "selected_as_legacy_profile": False,
        }
        try:
            _atomic_write(
                key_path,
                setup.credentials.api_private_key.lower() + "\n",
                mode=0o600,
            )
            _atomic_write(
                profile_path,
                json.dumps(profile, indent=2, sort_keys=True) + "\n",
                mode=0o600,
            )
            self._rewrite_credential_map()
        except Exception:
            for path in (profile_path, key_path):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
            try:
                profile_dir.rmdir()
            except OSError:
                pass
            raise
        return self.status(active_config=active_config, saved_profile_id=setup.profile_id)

    def status(
        self,
        *,
        active_config: AppConfig,
        saved_profile_id: str | None = None,
    ) -> dict[str, Any]:
        return self._status(
            active_config=active_config,
            saved_profile_id=saved_profile_id,
            verify_secrets=True,
        )

    def public_status(
        self,
        *,
        active_config: AppConfig,
        saved_profile_id: str | None = None,
    ) -> dict[str, Any]:
        """Return vault metadata without opening any signer-key file."""

        return self._status(
            active_config=active_config,
            saved_profile_id=saved_profile_id,
            verify_secrets=False,
        )

    def _status(
        self,
        *,
        active_config: AppConfig,
        saved_profile_id: str | None,
        verify_secrets: bool,
    ) -> dict[str, Any]:
        records, invalid_ids = self._records_with_health(verify_secrets=verify_secrets)
        legacy_status = (
            self.legacy_store.status(active_config=active_config)
            if verify_secrets
            else self.legacy_store.public_status(active_config=active_config)
        )
        legacy_profile = legacy_status.get("profile") if legacy_status.get("configured") else None
        profiles = [
            self._public_card(
                record,
                selected_as_legacy=self._same_public_profile(record, legacy_profile),
                active_in_current_process=(
                    bool(legacy_status.get("active_in_current_process"))
                    and self._same_public_profile(record, legacy_profile)
                ),
                private_key_verified=verify_secrets,
            )
            for record in records
        ]
        for profile_id in invalid_ids:
            profiles.append(
                {
                    "profile_id": profile_id,
                    "status": "invalid",
                    "configured": False,
                    "private_key_returned_by_api": False,
                    "action_required": "Delete and recreate this profile; its local files are incomplete or invalid.",
                }
            )
        profiles.sort(key=lambda item: str(item["profile_id"]))
        return {
            "status": (
                "ready"
                if verify_secrets and profiles and not invalid_ids
                else "metadata_only"
                if not verify_secrets and profiles and not invalid_ids
                else "needs_profiles"
            ),
            "profile_count": len(records),
            "invalid_profile_count": len(invalid_ids),
            "saved_profile_id": saved_profile_id,
            "profiles": profiles,
            "legacy": {
                "configured": bool(legacy_status.get("configured")),
                "active_in_current_process": bool(legacy_status.get("active_in_current_process")),
                "selected_profile_id": next(
                    (
                        record["profile_id"]
                        for record in records
                        if self._same_public_profile(record, legacy_profile)
                    ),
                    None,
                ),
                "selection_is_explicit_only": True,
                "restart_required": bool(legacy_status.get("restart_required")),
            },
            "storage": {
                "directory": str(self.secret_dir),
                "credential_map_file": str(self.credential_map_path),
                "one_private_key_file_per_profile": True,
                "private_keys_returned_by_api": False,
                "private_keys_stored_in_json": False,
                "private_keys_stored_in_env": False,
                "filesystem_permissions": "current_user_only",
            },
            "runner_export_ready": verify_secrets and not invalid_ids,
            "runner_credential_map": (
                self._credential_map_from_records(records)
                if verify_secrets and not invalid_ids
                else None
            ),
            "credential_content_read": verify_secrets,
            "private_key_verification_deferred_to_fenced_runtime": not verify_secrets,
            "signed_action_performed": False,
        }

    def credential_map(self, profile_ids: tuple[str, ...] | None = None) -> dict[str, Any]:
        records = self._valid_records()
        by_id = {str(record["profile_id"]): record for record in records}
        selected_ids = tuple(by_id) if profile_ids is None else profile_ids
        normalized_ids: list[str] = []
        for raw_id in selected_ids:
            profile_id = str(raw_id or "").strip().lower()
            if not PROFILE_ID_RE.fullmatch(profile_id):
                raise CredentialSetupError("Requested export contains an invalid profile ID.")
            if profile_id in normalized_ids:
                raise CredentialSetupError("Requested export contains a duplicate profile ID.")
            if profile_id not in by_id:
                raise CredentialSetupError(
                    f"Saved profile {profile_id!r} was not found or is invalid."
                )
            normalized_ids.append(profile_id)
        return self._credential_map_from_records([by_id[item] for item in normalized_ids])

    def replace_sources(self, profile_sources: Mapping[str, str]) -> dict[str, str]:
        """Replace public leader addresses without reopening signer key files."""

        if not profile_sources:
            raise CredentialSetupError("At least one profile source is required.")
        records, invalid = self._records_with_health(verify_secrets=False)
        if invalid:
            raise CredentialSetupError(
                "One or more saved credential profiles are invalid; repair them first."
            )
        by_id = {str(record["profile_id"]): record for record in records}
        normalized: dict[str, str] = {}
        for raw_id, raw_source in profile_sources.items():
            profile_id = str(raw_id or "").strip().lower()
            source = str(raw_source or "").strip().lower()
            if not PROFILE_ID_RE.fullmatch(profile_id) or profile_id not in by_id:
                raise CredentialSetupError(f"Saved profile {profile_id!r} was not found.")
            if not ADDRESS_RE.fullmatch(source):
                raise CredentialSetupError(
                    f"Profile {profile_id} leader must be a 42-character 0x address."
                )
            normalized[profile_id] = source

        final_sources = {
            profile_id: normalized.get(profile_id, str(record["source_wallet"]).lower())
            for profile_id, record in by_id.items()
        }
        if len(final_sources.values()) != len(set(final_sources.values())):
            raise CredentialSetupError("Fleet leader addresses must remain unique.")
        owned_roles = {
            str(record[field]).lower()
            for record in records
            for field in (
                "global_account_address",
                "follower_account_address",
                "api_wallet_address",
            )
        }
        collisions = sorted(set(final_sources.values()) & owned_roles)
        if collisions:
            raise CredentialSetupError(
                "A leader address collides with an owned account role: " + ", ".join(collisions)
            )

        originals: dict[Path, str] = {}
        try:
            for profile_id, source in normalized.items():
                profile_path = self._profile_dir(profile_id) / "profile.json"
                text = profile_path.read_text(encoding="utf-8-sig")
                raw = json.loads(text)
                if not isinstance(raw, dict):
                    raise CredentialSetupError(f"Profile {profile_id} metadata is unreadable.")
                originals[profile_path] = text
                raw["source_wallet"] = source
                _atomic_write(
                    profile_path,
                    json.dumps(raw, indent=2, sort_keys=True) + "\n",
                    mode=0o600,
                )
        except Exception:
            for profile_path, text in originals.items():
                _atomic_write(profile_path, text, mode=0o600)
            raise
        return normalized

    def update_market_policy(
        self,
        *,
        profile_id: str,
        denied_symbols: Any,
        confirmation: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        """Change one profile's denylist while retaining dynamic all-active eligibility.

        The API-wallet key file is deliberately left untouched.  Requiring the exact profile ID
        in the confirmation prevents a stray UI action from silently widening another account's
        trading scope.
        """

        normalized_id = str(profile_id or "").strip().lower()
        if not PROFILE_ID_RE.fullmatch(normalized_id):
            raise CredentialSetupError("Profile ID is invalid.")
        required = f"UPDATE {normalized_id} MARKETS"
        if confirmation != required:
            raise CredentialSetupError(
                f"Type {required} to replace only that profile's saved market denylist."
            )
        profile_dir = self._profile_dir(normalized_id)
        self._read_record(profile_dir, expected_id=normalized_id)
        symbols = _parse_denied_symbols(denied_symbols)
        profile_path = profile_dir / "profile.json"
        try:
            raw = json.loads(profile_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CredentialSetupError("Profile metadata is unreadable.") from exc
        if not isinstance(raw, dict):
            raise CredentialSetupError("Profile metadata is unreadable.")
        raw["eligibility"] = "all_active_markets"
        raw["denied_symbols"] = list(symbols)
        raw.pop("allowed_symbols", None)
        _atomic_write(
            profile_path,
            json.dumps(raw, indent=2, sort_keys=True) + "\n",
            mode=0o600,
        )
        self._rewrite_credential_map()
        result = self.status(active_config=active_config)
        result["market_policy_update"] = {
            "profile_id": normalized_id,
            "eligibility": "all_active_markets",
            "denied_symbols": list(symbols),
            "private_key_file_unchanged": True,
            "signed_action_performed": False,
        }
        return result

    def delete(
        self,
        *,
        profile_id: str,
        confirmation: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        normalized_id = str(profile_id or "").strip().lower()
        if not PROFILE_ID_RE.fullmatch(normalized_id):
            raise CredentialSetupError("Profile ID is invalid.")
        required = f"DELETE {normalized_id}"
        if confirmation != required:
            raise CredentialSetupError(f"Type {required} to delete only that saved profile.")
        profile_dir = self._profile_dir(normalized_id)
        if not profile_dir.is_dir():
            raise CredentialSetupError(f"Saved profile {normalized_id!r} does not exist.")
        expected = {profile_dir / "profile.json", profile_dir / "api-wallet.key"}
        unexpected = [path for path in profile_dir.iterdir() if path not in expected]
        if unexpected:
            raise CredentialSetupError(
                "Profile directory contains unexpected files; refusing automatic deletion."
            )
        for path in expected:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        profile_dir.rmdir()
        self._rewrite_credential_map()
        return self.status(active_config=active_config)

    def import_legacy(
        self,
        *,
        profile_id: str,
        confirmation: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        normalized_id = str(profile_id or "").strip().lower()
        required = f"IMPORT {normalized_id}"
        if confirmation != required:
            raise CredentialSetupError(
                f"Type {required} to copy the legacy profile into the vault."
            )
        profile = self.legacy_store._read_profile()
        if profile is None or not self.legacy_store.key_path.is_file():
            raise CredentialSetupError(
                "No complete legacy credential profile is available to import."
            )
        try:
            private_key = self.legacy_store.key_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise CredentialSetupError("The legacy API-wallet key file is not readable.") from exc
        return self.save(
            {
                **profile,
                "profile_id": normalized_id,
                "api_private_key": private_key,
                "api_key_is_dedicated": True,
                "eligibility": "all_active_markets",
                "denied_symbols": [],
            },
            active_config=active_config,
        )

    def select_legacy(
        self,
        *,
        profile_id: str,
        confirmation: str,
        active_config: AppConfig,
    ) -> dict[str, Any]:
        normalized_id = str(profile_id or "").strip().lower()
        required = f"ACTIVATE {normalized_id}"
        if confirmation != required:
            raise CredentialSetupError(
                f"Type {required} to replace the restart-loaded legacy canary profile."
            )
        record = next(
            (item for item in self._valid_records() if item["profile_id"] == normalized_id),
            None,
        )
        if record is None:
            raise CredentialSetupError(
                f"Saved profile {normalized_id!r} was not found or is invalid."
            )
        try:
            private_key = Path(record["api_private_key_file"]).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise CredentialSetupError("The selected profile key file is not readable.") from exc
        legacy = self.legacy_store.save(
            {
                **record,
                "api_private_key": private_key,
                "api_key_is_dedicated": True,
            },
            active_config=active_config,
        )
        result = self.status(active_config=active_config)
        result["legacy_selection"] = {
            "profile_id": normalized_id,
            "restart_required": bool(legacy.get("restart_required")),
            "generic_live_copy_enabled": False,
            "signed_action_performed": False,
        }
        return result

    def _profile_dir(self, profile_id: str) -> Path:
        path = (self.secret_dir / profile_id).resolve()
        if path.parent != self.secret_dir.resolve():
            raise CredentialSetupError("Profile ID resolves outside the profile vault.")
        return path

    def _valid_records(self) -> list[dict[str, Any]]:
        records, invalid_ids = self._records_with_health()
        if invalid_ids:
            raise CredentialSetupError(
                "One or more saved credential profiles are invalid; repair or delete them first."
            )
        return records

    def _records_with_health(
        self, *, verify_secrets: bool = True
    ) -> tuple[list[dict[str, Any]], list[str]]:
        if not self.secret_dir.is_dir():
            return [], []
        records: list[dict[str, Any]] = []
        invalid_ids: list[str] = []
        seen_wallets: set[str] = set()
        seen_followers: set[str] = set()
        for profile_dir in sorted(path for path in self.secret_dir.iterdir() if path.is_dir()):
            profile_id = profile_dir.name.lower()
            try:
                record = self._read_record(
                    profile_dir,
                    expected_id=profile_id,
                    verify_secret=verify_secrets,
                )
                wallet = str(record["api_wallet_address"])
                follower = str(record["follower_account_address"])
                if wallet in seen_wallets or follower in seen_followers:
                    raise CredentialSetupError("duplicate signer or follower")
                seen_wallets.add(wallet)
                seen_followers.add(follower)
                records.append(record)
            except (CredentialSetupError, OSError, ValueError):
                invalid_ids.append(profile_id)
        return records, invalid_ids

    def _read_record(
        self,
        profile_dir: Path,
        *,
        expected_id: str,
        verify_secret: bool = True,
    ) -> dict[str, Any]:
        profile_path = profile_dir / "profile.json"
        key_path = profile_dir / "api-wallet.key"
        try:
            raw = json.loads(profile_path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CredentialSetupError("Profile metadata is unreadable.") from exc
        if not isinstance(raw, dict) or raw.get("profile_version") != FLEET_PROFILE_VERSION:
            raise CredentialSetupError("Profile metadata version is unsupported.")
        if raw.get("profile_id") != expected_id or not PROFILE_ID_RE.fullmatch(expected_id):
            raise CredentialSetupError("Profile metadata ID does not match its directory.")
        for field in (
            "source_wallet",
            "global_account_address",
            "follower_account_address",
            "api_wallet_address",
        ):
            if not ADDRESS_RE.fullmatch(str(raw.get(field) or "")):
                raise CredentialSetupError(f"Profile metadata has an invalid {field}.")
        if raw.get("network") != "mainnet":
            raise CredentialSetupError("Fleet profile network must be mainnet.")
        if raw.get("expected_account_mode") not in {
            AccountMode.STANDARD.value,
            AccountMode.UNIFIED.value,
        }:
            raise CredentialSetupError("Fleet profile account mode is invalid.")
        if raw.get("eligibility") != "all_active_markets":
            raise CredentialSetupError("Fleet profile eligibility is invalid.")
        denied = _parse_denied_symbols(raw.get("denied_symbols", []))
        if "allowed_symbols" in raw:
            raise CredentialSetupError("Fleet profile contains an obsolete market allowlist.")
        resolved_key_path = Path(str(raw.get("api_private_key_file") or "")).resolve()
        if resolved_key_path != key_path.resolve() or not key_path.is_file():
            raise CredentialSetupError("Fleet profile key-file path is invalid.")
        if verify_secret:
            private_key = key_path.read_text(encoding="utf-8").strip()
            if not PRIVATE_KEY_RE.fullmatch(private_key):
                raise CredentialSetupError("Fleet profile key file is malformed.")
            try:
                derived = str(Account.from_key(private_key).address).lower()
            except (TypeError, ValueError) as exc:
                raise CredentialSetupError("Fleet profile key file cannot be loaded.") from exc
            if derived != raw["api_wallet_address"]:
                raise CredentialSetupError("Fleet profile key does not match its API wallet.")
        record = {
            key: raw.get(key)
            for key in (
                "profile_id",
                "profile_label",
                "network",
                "source_wallet",
                "global_account_address",
                "subaccount_name",
                "follower_account_address",
                "api_wallet_address",
                "expected_account_mode",
                "coin",
            )
        }
        record["eligibility"] = "all_active_markets"
        record["denied_symbols"] = list(denied)
        record["api_private_key_file"] = str(key_path.resolve())
        return record

    def _rewrite_credential_map(self) -> None:
        self.secret_dir.mkdir(parents=True, exist_ok=True)
        _restrict_permissions(self.secret_dir, directory=True)
        _atomic_write(
            self.credential_map_path,
            json.dumps(self.credential_map(), indent=2, sort_keys=True) + "\n",
            mode=0o600,
        )

    @staticmethod
    def _credential_map_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "version": FLEET_CREDENTIAL_MAP_VERSION,
            "slots": {
                str(record["profile_id"]): {
                    "api_wallet_address": record["api_wallet_address"],
                    "api_private_key_file": record["api_private_key_file"],
                }
                for record in records
            },
        }

    @staticmethod
    def _same_public_profile(
        record: Mapping[str, Any], legacy_profile: Mapping[str, Any] | None
    ) -> bool:
        if not legacy_profile:
            return False
        return all(
            str(record.get(field) or "").lower() == str(legacy_profile.get(field) or "").lower()
            for field in (
                "source_wallet",
                "global_account_address",
                "follower_account_address",
                "api_wallet_address",
                "expected_account_mode",
            )
        )

    @staticmethod
    def _public_card(
        record: Mapping[str, Any],
        *,
        selected_as_legacy: bool,
        active_in_current_process: bool,
        private_key_verified: bool,
    ) -> dict[str, Any]:
        return {
            **record,
            "status": "configured" if private_key_verified else "metadata_present",
            "configured": True,
            "private_key_present": True,
            "private_key_matches_api_wallet": True if private_key_verified else None,
            "private_key_verification_deferred": not private_key_verified,
            "private_key_returned_by_api": False,
            "selected_as_legacy_profile": selected_as_legacy,
            "active_in_current_process": active_in_current_process,
            "signed_action_performed": False,
        }


def _parse_allowed_symbols(raw: Any, fallback_coin: str) -> tuple[str, ...]:
    if raw is None or raw == "":
        items: list[Any] = [fallback_coin]
    elif isinstance(raw, str):
        items = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        raise CredentialSetupError("Allowed symbols must be a comma-separated string or list.")
    # Mainnet currently exposes more than 128 active default-perp markets, and a full-account
    # validation may also pin one or more HIP-3 universes into its immutable manifest.
    if not 1 <= len(items) <= 512:
        raise CredentialSetupError("Allowed symbols must contain between 1 and 512 markets.")
    canonical: list[str] = []
    for item in items:
        try:
            symbol = canonical_market_symbol(item)
        except (TypeError, ValueError) as exc:
            raise CredentialSetupError(f"Allowed symbol {item!r} is invalid: {exc}") from exc
        if symbol in canonical:
            raise CredentialSetupError(f"Allowed symbols contain duplicate market {symbol}.")
        canonical.append(symbol)
    return tuple(canonical)


def _parse_denied_symbols(raw: Any) -> tuple[str, ...]:
    if raw in (None, ""):
        items: list[Any] = []
    elif isinstance(raw, str):
        items = [item.strip() for item in raw.split(",") if item.strip()]
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        raise CredentialSetupError("Denied symbols must be a comma-separated string or list.")
    if len(items) > 512:
        raise CredentialSetupError("Denied symbols cannot contain more than 512 markets.")
    canonical: list[str] = []
    for item in items:
        try:
            symbol = canonical_market_symbol(item)
        except (TypeError, ValueError) as exc:
            raise CredentialSetupError(f"Denied symbol {item!r} is invalid: {exc}") from exc
        if symbol in canonical:
            raise CredentialSetupError(f"Denied symbols contain duplicate market {symbol}.")
        canonical.append(symbol)
    return tuple(sorted(canonical))


def _without_managed_env(content: str) -> str:
    result: list[str] = []
    inside_block = False
    managed_pattern = re.compile(
        r"^\s*(?:export\s+)?(" + "|".join(re.escape(key) for key in MANAGED_ENV_KEYS) + r")\s*="
    )
    for line in content.splitlines():
        stripped = line.strip()
        if stripped == MANAGED_BLOCK_START:
            inside_block = True
            continue
        if stripped == MANAGED_BLOCK_END:
            inside_block = False
            continue
        if inside_block or managed_pattern.match(line):
            continue
        result.append(line)
    return "\n".join(result)


def _quote_env(value: str) -> str:
    return '"' + value.replace("\\", "/").replace('"', '\\"') + '"'


def _env_bool(value: bool) -> str:
    return "true" if value else "false"


def _subaccount_account_value(item: Mapping[str, Any]) -> str | None:
    return _margin_account_value(item.get("clearinghouseState"))


def _margin_account_value(clearinghouse: Any) -> str | None:
    if not isinstance(clearinghouse, dict):
        return None
    margin = clearinghouse.get("marginSummary")
    if not isinstance(margin, dict):
        return None
    raw = margin.get("accountValue")
    if raw is None:
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None
    return str(value) if value.is_finite() else None


def _subaccount_spot_usdc(item: Mapping[str, Any]) -> str | None:
    return _spot_usdc_total(item.get("spotState"))


def _spot_usdc_total(spot_state: Any) -> str | None:
    if not isinstance(spot_state, dict) or not isinstance(spot_state.get("balances"), list):
        return None
    matches = [
        row
        for row in spot_state["balances"]
        if isinstance(row, dict) and str(row.get("coin") or "").upper() == "USDC"
    ]
    if len(matches) != 1:
        return None
    try:
        value = Decimal(str(matches[0].get("total")))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return str(value) if value.is_finite() else None


def _detected_account_mode(abstraction: Any) -> str:
    mode = classify_user_abstraction(abstraction)
    if mode == HyperliquidUserAbstraction.STANDARD:
        return AccountMode.STANDARD.value
    if mode == HyperliquidUserAbstraction.UNIFIED:
        return AccountMode.UNIFIED.value
    if mode in {
        HyperliquidUserAbstraction.PORTFOLIO_MARGIN,
        HyperliquidUserAbstraction.DEX_ABSTRACTION,
    }:
        raise CredentialSetupError(
            f"Hyperliquid account mode {mode.value} is unsupported by this copytrader."
        )
    raise CredentialSetupError("Hyperliquid returned an unrecognized account mode.")


def _atomic_write(path: Path, content: str, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _restrict_permissions(temporary, directory=False, posix_mode=mode)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _restrict_permissions(path: Path, *, directory: bool, posix_mode: int | None = None) -> None:
    if os.name != "nt":
        permissions = posix_mode or (
            stat.S_IRUSR | stat.S_IWUSR | (stat.S_IXUSR if directory else 0)
        )
        os.chmod(path, permissions)
        return

    identity_result = subprocess.run(
        ["whoami"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=5,
        creationflags=0x08000000,
    )
    identity = identity_result.stdout.strip()
    if identity_result.returncode or not identity:
        raise OSError("could not determine the current Windows identity for secret ACLs")
    grant = f"{identity}:(OI)(CI)F" if directory else f"{identity}:(F)"
    acl_result = subprocess.run(
        ["icacls", str(path), "/inheritance:r", "/grant:r", grant],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=10,
        creationflags=0x08000000,
    )
    if acl_result.returncode:
        raise OSError("could not restrict local credential file permissions")
