from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any

from .fleet_config import FLEET_RUNTIME_POLICY
from .markets import canonical_market_symbol
from .models import Mode
from .runtime_lock import default_runtime_lock_dir
from .unified_account import SourceDexScope


MAINNET_REST = "https://api.hyperliquid.xyz"
TESTNET_REST = "https://api.hyperliquid-testnet.xyz"
MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS = "wss://api.hyperliquid-testnet.xyz/ws"
MAX_LEADERBOARD_ROWS = 100
MAX_ADDRESS_ANALYSIS_PAGES = 30


def default_fleet_runtime_root() -> Path:
    """Return the user-local Windows root for mutable fleet state."""

    local = os.getenv("LOCALAPPDATA", "").strip()
    if local:
        return Path(local) / "HyperliquidCopytrader" / "runtime"
    return Path.home() / "AppData" / "Local" / "HyperliquidCopytrader" / "runtime"


CONFIG_SCHEMA: dict[str, Any] = {
    "mode": [m.value for m in Mode],
    "source_wallet": "42-character hex account address to observe",
    "source_network": "read-only source network: mode, mainnet, or testnet",
    "source_dex_scope": (
        "strict blocks Unified sources with non-default DEX activity; "
        "default_only_account_equity copies only default-DEX positions while sizing against "
        "total Unified collateral and reporting excluded DEX activity; "
        "all_configured_markets copies allowlisted default and HIP-3 markets while sizing "
        "against total Unified collateral"
    ),
    "allowed_symbols": "comma-separated symbols that may be copied",
    "fixed_multiplier": "Decimal multiplier applied after automatic follower/source balance scaling",
    "equity_ratio": "explicit scale override; disables automatic balance scaling when set",
    "balance_sizing_enabled": "scale copied size by fresh follower/source accountValue snapshots",
    "sizing_equity_cap_usd": (
        "optional finite positive cap applied to follower accountValue for automatic sizing"
    ),
    "max_initial_margin_utilization": (
        "optional follower initial-margin budget as a ratio above 0 and at or below 1"
    ),
    "max_balance_scale": "upper bound for follower/source accountValue scale before fixed multiplier",
    "max_notional_usd": "nonzero Decimal cap required for testnet/live",
    "max_gross_exposure_usd": "nonzero Decimal cap across all projected follower positions",
    "max_open_positions": "maximum distinct nonzero projected follower positions",
    "max_leverage": "integer leverage cap required for testnet/live",
    "min_order_size": "Decimal size threshold below which intents are skipped",
    "slippage_bps": "entry IOC price padding in basis points",
    "close_slippage_bps": (
        "reduce-only close IOC price padding in basis points; exchange modes cap this at 1000"
    ),
    "hip3_oracle_envelope_bps": (
        "application safety envelope around the HIP-3 oracle used for round-trip depth admission"
    ),
    "stale_source_ms": "source data age after which new risk is paused",
    "stale_follower_ms": "follower data age after which new risk is paused",
    "rapid_flip_ms": "window used to pause on rapid source long/short flips",
    "kill_switch_path": "local file path that blocks all risk when present",
    "max_new_intents_per_cycle": "maximum non-NOOP intents allowed per run loop",
    "max_open_intents": "maximum persisted pending/sent/acked intents before restart guard pauses",
    "max_exchange_actions_per_minute": "local exchange action burst limiter",
    "circuit_breaker_failure_threshold": "consecutive exchange failures before pause",
    "circuit_breaker_cooldown_ms": "minimum pause window after circuit breaker opens",
    "exchange_action_timeout_s": "maximum acceptable exchange action latency",
    "exchange_expires_after_ms": "signed exchange-action deadline window in milliseconds",
    "dead_man_cancel_ms": "future scheduleCancel deadline before exchange order placement",
    "dead_man_policy": (
        "exchange_required requires Hyperliquid scheduleCancel; watchdog_fallback permits an "
        "independent containment watchdog only when scheduleCancel is volume-gated"
    ),
    "containment_watchdog_ttl_ms": "maximum accepted age of the independent watchdog heartbeat",
    "auth_probe_interval_ms": "minimum interval between signed exchange no-op auth probes",
    "info_timeout_s": "HTTP timeout for info endpoint calls",
    "api_wallet_address": "optional expected API wallet signer address",
    "api_private_key_file": "local file containing the API wallet private key",
    "expected_account_mode": (
        "expected Hyperliquid account abstraction mode: auto, standard, or unified"
    ),
    "allow_master_private_key": (
        "testnet-only explicit acknowledgement allowing the trading account owner key"
    ),
    "live_copy_enable": (
        "separate opt-in for the generic live copy runner; mainnet canary commands do not use it"
    ),
    "gui_token": "operator token; optional for console and required by fleet-capable serve",
    "dashboard_control_max_per_minute": "maximum accepted dashboard control POSTs per minute",
    "runtime_lease_ttl_ms": "SQLite lease TTL used to block concurrent exchange actors",
    "runtime_lock_dir": "stable user-local directory for account-global exchange process locks",
    "dashboard_security_audit_ttl_ms": "milliseconds to cache expensive dashboard security scans",
    "source_reaction_queue_size": "bounded websocket event reaction queue before fail-closed overflow",
    "source_websocket_idle_timeout_ms": "milliseconds without server messages before sending a websocket heartbeat",
    "source_websocket_heartbeat_timeout_ms": "milliseconds to wait for any server message after heartbeat ping",
    "source_websocket_reconnect_attempts": "bounded reconnect attempts after a source websocket gap",
    "source_websocket_reconnect_backoff_ms": "base reconnect backoff after a source websocket gap",
    "source_fill_backfill_lookback_ms": "initial source fill/TWAP-slice backfill window when no prior event exists",
    "source_fill_backfill_overlap_ms": "overlap with latest fill or TWAP-slice timestamp for gap recovery",
    "source_fill_backfill_max_pages": "maximum userFillsByTime/userTwapSliceFillsByTime pages before fail-closed gap",
    "connection_siren_after_ms": "source event age that raises the dashboard connection-integrity siren",
    "validation_effective_config_sha256": (
        "expected immutable effective-runtime configuration SHA-256 for this validation child"
    ),
    "validation_effective_config_set_sha256": (
        "expected immutable two-slot effective-runtime configuration-set SHA-256"
    ),
    "validation_supervisor_incarnation_id": (
        "opaque supervisor generation pinned into each bounded-validation child"
    ),
    "validation_follower_set": (
        "exact two-address follower set jointly fenced by the validation supervisor"
    ),
    "leaderboard_enabled": "enable non-blocking public Hyperliquid leaderboard panel",
    "leaderboard_url": "public leaderboard source URL",
    "leaderboard_cache_ttl_ms": "milliseconds to cache leaderboard responses",
    "leaderboard_timeout_s": "HTTP timeout for public leaderboard fetches",
    "leaderboard_min_volume_usd": "minimum 30D volume required for leaderboard rows",
    "leaderboard_min_account_value_usd": (
        "minimum current account value required for leaderboard rows"
    ),
    "leaderboard_limit": "maximum leaderboard rows to expose, capped at 100",
    "address_analytics_enabled": "enable read-only address analysis panel",
    "address_analytics_url": "public Hyperliquid info base URL used for address analysis",
    "address_analytics_cache_ttl_ms": "milliseconds to cache address analysis responses",
    "address_analytics_timeout_s": "HTTP timeout for address analysis info calls",
    "address_analytics_window_days": "default lookback window for address analysis",
    "address_analytics_max_pages": "maximum userFillsByTime pages fetched per analysis, capped at 30",
    "subaccount_assignments_json": (
        "JSON array of planned manual source-to-subaccount assignments; enabled rows "
        "require subaccount_verified=true and active testnet/live rows must match "
        "the current source plus follower/vault action account"
    ),
}


class SourceNetwork(str, Enum):
    MODE = "mode"
    MAINNET = "mainnet"
    TESTNET = "testnet"


class AccountMode(str, Enum):
    AUTO = "auto"
    STANDARD = "standard"
    UNIFIED = "unified"


class DeadManPolicy(str, Enum):
    EXCHANGE_REQUIRED = "exchange_required"
    WATCHDOG_FALLBACK = "watchdog_fallback"


class MarketEligibility(str, Enum):
    ALLOWLIST = "allowlist"
    ALL_ACTIVE_MARKETS = "all_active_markets"


@dataclass(frozen=True)
class RiskConfig:
    allowed_symbols: tuple[str, ...] = ("BTC", "ETH", "SOL")
    market_eligibility: MarketEligibility = MarketEligibility.ALLOWLIST
    denied_symbols: tuple[str, ...] = ()
    fixed_multiplier: Decimal = Decimal("0.10")
    equity_ratio: Decimal | None = None
    balance_sizing_enabled: bool = True
    sizing_equity_cap_usd: Decimal | None = None
    max_initial_margin_utilization: Decimal | None = None
    max_balance_scale: Decimal = Decimal("1")
    max_notional_usd: Decimal = Decimal("250")
    max_gross_exposure_usd: Decimal = Decimal("1000")
    max_open_positions: int = 20
    max_leverage: int = 3
    min_order_size: Decimal = Decimal("0.0001")
    slippage_bps: Decimal = Decimal("20")
    close_slippage_bps: Decimal = Decimal("300")
    hip3_oracle_envelope_bps: Decimal = Decimal("100")
    stale_source_ms: int = 10_000
    stale_follower_ms: int = 10_000
    rapid_flip_ms: int = 1_500


@dataclass(frozen=True)
class ExchangeConfig:
    follower_account_address: str = ""
    api_wallet_address: str = ""
    api_private_key: str = ""
    api_private_key_file: str = ""
    vault_address: str = ""
    expected_account_mode: AccountMode = AccountMode.AUTO
    allow_master_private_key: bool = False
    testnet_enable: bool = True
    live_enable: bool = False
    confirm_mainnet_live: bool = False
    live_copy_enable: bool = False


@dataclass(frozen=True)
class OpsConfig:
    kill_switch_path: Path = Path("data/KILL_SWITCH")
    max_new_intents_per_cycle: int = 10
    max_open_intents: int = 20
    max_exchange_actions_per_minute: int = 30
    circuit_breaker_failure_threshold: int = 3
    circuit_breaker_cooldown_ms: int = 60_000
    exchange_action_timeout_s: Decimal = Decimal("15")
    exchange_expires_after_ms: int = 10_000
    dead_man_cancel_ms: int = 30_000
    dead_man_policy: DeadManPolicy = DeadManPolicy.EXCHANGE_REQUIRED
    containment_watchdog_ttl_ms: int = 15_000
    auth_probe_interval_ms: int = 600_000
    info_timeout_s: Decimal = Decimal("10")
    gui_token: str = ""
    dashboard_control_max_per_minute: int = 20
    runtime_lease_ttl_ms: int = 30_000
    runtime_lock_dir: Path = field(default_factory=default_runtime_lock_dir)
    dashboard_security_audit_ttl_ms: int = 60_000
    source_reaction_queue_size: int = 100
    source_websocket_idle_timeout_ms: int = 55_000
    source_websocket_heartbeat_timeout_ms: int = 5_000
    source_websocket_reconnect_attempts: int = 3
    source_websocket_reconnect_backoff_ms: int = 1_000
    source_fill_backfill_lookback_ms: int = 300_000
    source_fill_backfill_overlap_ms: int = 5_000
    source_fill_backfill_max_pages: int = 5
    connection_siren_after_ms: int = 30_000
    # Optional fail-closed boundary used by the bounded two-account mainnet
    # validation.  Normal CLI/GUI runtimes leave these fields empty.  When a
    # lease path is configured every exposure-increasing signed action must
    # prove that the supervising run is still the exact expected owner and has
    # not crossed its immutable deadline.
    validation_supervisor_lease_path: Path | None = None
    validation_controller_registry_path: Path | None = None
    validation_run_id: str = ""
    validation_owner_token: str = ""
    validation_state_identity_sha256: str = ""
    validation_effective_config_sha256: str = ""
    validation_effective_config_set_sha256: str = ""
    validation_supervisor_incarnation_id: str = ""
    validation_follower_set: tuple[str, ...] = ()
    validation_deadline_ms: int = 0
    validation_market_universe_manifest_path: Path | None = None
    validation_market_universe_sha256: str = ""
    validation_market_universe_refresh_ms: int = 60_000
    # Legacy fleet fast-path policy retained for the non-continuous analytics path.
    fast_execution_enabled: bool = False
    deferred_delta_window_ms: int = 300_000
    deferred_scheduler_bound_ms: int = 100
    affected_follower_freshness_ms: int = 5_000
    full_follower_audit_ms: int = 60_000
    market_catalog_refresh_ms: int = 60_000
    source_shard_count: int = 2
    action_shard_count: int = 2
    market_data_connection_count: int = 1
    market_event_queue_size: int = 4_096
    action_queue_size: int = 256
    websocket_heartbeat_ms: int = 30_000
    websocket_reconnect_min_ms: int = 250
    websocket_reconnect_max_ms: int = 5_000
    websocket_connection_limit: int = 10
    websocket_overlap_limit: int = 8
    websocket_subscription_limit: int = 1_000
    websocket_unique_user_limit: int = 10
    websocket_outbound_per_minute: int = 2_000
    websocket_inflight_post_limit: int = 100
    rest_ordinary_weight_per_minute: int = 720
    rest_reserve_weight_per_minute: int = 480
    clock_max_skew_ms: int = 500
    clock_max_jump_ms: int = 500
    direct_source_max_age_ms: int = 5_000
    primary_cleanup_timeout_ms: int = 1_800_000
    catalog_policy_version: str = "dynamic-all-active-v1"


def reviewed_fleet_runtime_policy_errors(ops: OpsConfig) -> tuple[str, ...]:
    """Reject environment drift from the benchmarked fleet runtime policy."""

    actual: dict[str, int | str] = {
        "defer_window_ms": ops.deferred_delta_window_ms,
        "scheduler_bound_ms": ops.deferred_scheduler_bound_ms,
        "affected_follower_refresh_ms": ops.affected_follower_freshness_ms,
        "full_follower_audit_ms": ops.full_follower_audit_ms,
        "catalog_refresh_ms": ops.market_catalog_refresh_ms,
        "source_shards": ops.source_shard_count,
        "action_shards": ops.action_shard_count,
        "market_data_connections": ops.market_data_connection_count,
        "market_queue_capacity": ops.market_event_queue_size,
        "execution_lane_capacity": ops.action_queue_size,
        "websocket_heartbeat_ms": ops.websocket_heartbeat_ms,
        "websocket_reconnect_min_ms": ops.websocket_reconnect_min_ms,
        "websocket_reconnect_max_ms": ops.websocket_reconnect_max_ms,
        "websocket_connection_limit": ops.websocket_connection_limit,
        "websocket_overlap_limit": ops.websocket_overlap_limit,
        "websocket_subscription_limit": ops.websocket_subscription_limit,
        "websocket_unique_user_limit": ops.websocket_unique_user_limit,
        "websocket_outbound_per_minute": ops.websocket_outbound_per_minute,
        "websocket_inflight_post_limit": ops.websocket_inflight_post_limit,
        "rest_ordinary_weight_per_minute": ops.rest_ordinary_weight_per_minute,
        "rest_reserve_weight_per_minute": ops.rest_reserve_weight_per_minute,
        "clock_max_skew_ms": ops.clock_max_skew_ms,
        "clock_max_jump_ms": ops.clock_max_jump_ms,
        "direct_source_max_age_ms": ops.direct_source_max_age_ms,
        "primary_cleanup_timeout_ms": ops.primary_cleanup_timeout_ms,
        "catalog_policy_version": ops.catalog_policy_version,
    }
    return tuple(
        f"fleet runtime policy {name} must equal reviewed value {expected!r}, got {actual[name]!r}"
        for name, expected in FLEET_RUNTIME_POLICY.items()
        if name != "version" and name in actual and actual[name] != expected
    )


@dataclass(frozen=True)
class LeaderboardConfig:
    enabled: bool = True
    url: str = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
    cache_ttl_ms: int = 300_000
    timeout_s: Decimal = Decimal("8")
    min_volume_usd: Decimal = Decimal("100000")
    min_account_value_usd: Decimal = Decimal("2000")
    limit: int = 100


@dataclass(frozen=True)
class AddressAnalyticsConfig:
    enabled: bool = True
    url: str = MAINNET_REST
    cache_ttl_ms: int = 120_000
    timeout_s: Decimal = Decimal("6")
    window_days: int = 30
    max_pages: int = 12


@dataclass(frozen=True)
class SubaccountAssignment:
    slot: str
    subaccount: str
    source_wallet: str
    mode: str = "planned"
    note: str = "manual assignment"
    enabled: bool = False
    subaccount_verified: bool = False
    operator_verified_at: str = ""


@dataclass(frozen=True)
class AppConfig:
    mode: Mode = Mode.SHADOW
    source_wallet: str = ""
    source_network: SourceNetwork = SourceNetwork.MODE
    source_dex_scope: SourceDexScope = SourceDexScope.STRICT
    db_path: Path = Path("data/copytrader.sqlite3")
    host: str = "127.0.0.1"
    port: int = 8080
    risk: RiskConfig = field(default_factory=RiskConfig)
    exchange: ExchangeConfig = field(default_factory=ExchangeConfig)
    ops: OpsConfig = field(default_factory=OpsConfig)
    leaderboard: LeaderboardConfig = field(default_factory=LeaderboardConfig)
    address_analytics: AddressAnalyticsConfig = field(default_factory=AddressAnalyticsConfig)
    subaccount_assignments: tuple[SubaccountAssignment, ...] = ()
    config_errors: tuple[str, ...] = ()

    @property
    def rest_url(self) -> str:
        return TESTNET_REST if self.mode == Mode.TESTNET else MAINNET_REST

    @property
    def ws_url(self) -> str:
        return TESTNET_WS if self.mode == Mode.TESTNET else MAINNET_WS

    @property
    def resolved_source_network(self) -> SourceNetwork:
        if self.source_network == SourceNetwork.MODE:
            return SourceNetwork.TESTNET if self.mode == Mode.TESTNET else SourceNetwork.MAINNET
        return self.source_network

    @property
    def source_rest_url(self) -> str:
        return (
            TESTNET_REST if self.resolved_source_network == SourceNetwork.TESTNET else MAINNET_REST
        )

    @property
    def source_ws_url(self) -> str:
        return TESTNET_WS if self.resolved_source_network == SourceNetwork.TESTNET else MAINNET_WS


def _bool_env(name: str, default: bool = False, errors: list[str] | None = None) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    if errors is not None:
        errors.append(f"{name} must be a boolean value")
    return default


def _decimal_env(
    name: str,
    default: Decimal | None = None,
    errors: list[str] | None = None,
) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = Decimal(raw.strip())
    except (InvalidOperation, ValueError):
        if errors is not None:
            errors.append(f"{name} must be a decimal value")
        return default
    if not parsed.is_finite():
        if errors is not None:
            errors.append(f"{name} must be a finite decimal value")
        return default
    return parsed


def _optional_positive_decimal_env(name: str, errors: list[str]) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        parsed = Decimal(raw.strip())
    except (InvalidOperation, ValueError):
        errors.append(f"{name} must be a finite positive decimal value")
        return Decimal("0")
    if not parsed.is_finite() or parsed <= 0:
        errors.append(f"{name} must be a finite positive decimal value")
    return parsed


def _optional_unit_ratio_env(name: str, errors: list[str]) -> Decimal | None:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    try:
        parsed = Decimal(raw.strip())
    except (InvalidOperation, ValueError):
        errors.append(f"{name} must be a finite decimal above 0 and at or below 1")
        return Decimal("0")
    if not parsed.is_finite() or not Decimal("0") < parsed <= Decimal("1"):
        errors.append(f"{name} must be a finite decimal above 0 and at or below 1")
    return parsed


def _int_env(name: str, default: int, errors: list[str] | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        if errors is not None:
            errors.append(f"{name} must be an integer value")
        return default


def _symbols_env(
    name: str,
    default: tuple[str, ...],
    errors: list[str] | None = None,
) -> tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    symbols: list[str] = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            symbol = canonical_market_symbol(value)
        except ValueError as exc:
            if errors is not None:
                errors.append(f"{name} contains invalid market {value!r}: {exc}")
            continue
        if symbol not in symbols:
            symbols.append(symbol)
    return tuple(symbols)


def _validation_follower_set_env(errors: list[str]) -> tuple[str, ...]:
    name = "HLCT_VALIDATION_FOLLOWER_SET_JSON"
    raw = os.getenv(name, "").strip()
    if not raw:
        return ()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        errors.append(f"{name} must be valid JSON: {exc.msg}")
        return ()
    if not isinstance(payload, list) or not payload:
        errors.append(f"{name} must be a non-empty JSON array of addresses")
        return ()
    addresses = tuple(str(item).strip().lower() for item in payload)
    if len(set(addresses)) != len(addresses) or any(
        re.fullmatch(r"0x[0-9a-f]{40}", address) is None for address in addresses
    ):
        errors.append(f"{name} must contain distinct canonical 42-character addresses")
        return ()
    return tuple(sorted(addresses))


def _json_bool(value: Any, *, default: bool, errors: list[str], field: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    errors.append(f"{field} must be a boolean value")
    return default


def _api_private_key_env(errors: list[str]) -> tuple[str, str]:
    direct = os.getenv("HLCT_API_PRIVATE_KEY", "").strip()
    file_path = os.getenv("HLCT_API_PRIVATE_KEY_FILE", "").strip()
    if direct:
        if file_path:
            errors.append(
                "HLCT_API_PRIVATE_KEY and HLCT_API_PRIVATE_KEY_FILE are mutually exclusive"
            )
        return direct, file_path
    if not file_path:
        return "", ""
    path = Path(file_path)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        errors.append(f"HLCT_API_PRIVATE_KEY_FILE could not be read: {exc}")
        return "", file_path
    if not value:
        errors.append("HLCT_API_PRIVATE_KEY_FILE must not be empty")
    return value, file_path


def _subaccount_assignments_env(errors: list[str]) -> tuple[SubaccountAssignment, ...]:
    raw = os.getenv("HLCT_SUBACCOUNT_ASSIGNMENTS_JSON", "").strip()
    if not raw:
        return ()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        errors.append(f"HLCT_SUBACCOUNT_ASSIGNMENTS_JSON must be valid JSON: {exc.msg}")
        return ()
    if not isinstance(payload, list):
        errors.append("HLCT_SUBACCOUNT_ASSIGNMENTS_JSON must be a JSON array")
        return ()
    rows: list[SubaccountAssignment] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            errors.append(f"HLCT_SUBACCOUNT_ASSIGNMENTS_JSON[{index}] must be an object")
            continue
        slot = str(item.get("slot") or f"slot-{index + 1}").strip()
        subaccount = (
            str(item.get("subaccount") or item.get("follower_account") or "").strip().lower()
        )
        source_wallet = str(item.get("source_wallet") or item.get("source") or "").strip().lower()
        mode = str(item.get("mode") or "planned").strip().lower()
        note = str(item.get("note") or "manual assignment").strip()
        enabled = _json_bool(
            item.get("enabled"),
            default=False,
            errors=errors,
            field=f"HLCT_SUBACCOUNT_ASSIGNMENTS_JSON[{index}].enabled",
        )
        subaccount_verified = _json_bool(
            item.get("subaccount_verified"),
            default=False,
            errors=errors,
            field=f"HLCT_SUBACCOUNT_ASSIGNMENTS_JSON[{index}].subaccount_verified",
        )
        operator_verified_at = str(item.get("operator_verified_at") or "").strip()
        rows.append(
            SubaccountAssignment(
                slot=slot,
                subaccount=subaccount,
                source_wallet=source_wallet,
                mode=mode,
                note=note,
                enabled=enabled,
                subaccount_verified=subaccount_verified,
                operator_verified_at=operator_verified_at,
            )
        )
    return tuple(rows)


def load_config(mode_override: str | None = None) -> AppConfig:
    errors: list[str] = []
    mode_raw = (
        mode_override
        if mode_override is not None
        else (os.getenv("HLCT_MODE") or Mode.SHADOW.value)
    )
    try:
        mode = Mode(mode_raw.lower())
    except ValueError:
        errors.append(f"HLCT_MODE must be one of: {', '.join(m.value for m in Mode)}")
        mode = Mode.SHADOW
    source_network_raw = os.getenv("HLCT_SOURCE_NETWORK", SourceNetwork.MODE.value).strip().lower()
    try:
        source_network = SourceNetwork(source_network_raw)
    except ValueError:
        allowed_source_networks = ", ".join(network.value for network in SourceNetwork)
        errors.append(f"HLCT_SOURCE_NETWORK must be one of: {allowed_source_networks}")
        source_network = SourceNetwork.MODE
    source_dex_scope_raw = (
        os.getenv("HLCT_SOURCE_DEX_SCOPE", SourceDexScope.STRICT.value).strip().lower()
    )
    try:
        source_dex_scope = SourceDexScope(source_dex_scope_raw)
    except ValueError:
        errors.append(
            "HLCT_SOURCE_DEX_SCOPE must be one of: "
            + ", ".join(item.value for item in SourceDexScope)
        )
        source_dex_scope = SourceDexScope.STRICT
    market_eligibility_raw = (
        os.getenv("HLCT_MARKET_ELIGIBILITY", MarketEligibility.ALLOWLIST.value).strip().lower()
    )
    try:
        market_eligibility = MarketEligibility(market_eligibility_raw)
    except ValueError:
        errors.append(
            "HLCT_MARKET_ELIGIBILITY must be one of: "
            + ", ".join(item.value for item in MarketEligibility)
        )
        market_eligibility = MarketEligibility.ALLOWLIST
    risk = RiskConfig(
        allowed_symbols=_symbols_env("HLCT_ALLOWED_SYMBOLS", ("BTC", "ETH", "SOL"), errors),
        market_eligibility=market_eligibility,
        denied_symbols=_symbols_env("HLCT_DENIED_SYMBOLS", (), errors),
        fixed_multiplier=_decimal_env("HLCT_FIXED_MULTIPLIER", Decimal("0.10"), errors)
        or Decimal("0"),
        equity_ratio=_decimal_env("HLCT_EQUITY_RATIO", None, errors),
        balance_sizing_enabled=_bool_env("HLCT_BALANCE_SIZING_ENABLE", True, errors),
        sizing_equity_cap_usd=_optional_positive_decimal_env("HLCT_SIZING_EQUITY_CAP_USD", errors),
        max_initial_margin_utilization=_optional_unit_ratio_env(
            "HLCT_MAX_INITIAL_MARGIN_UTILIZATION", errors
        ),
        max_balance_scale=_decimal_env("HLCT_MAX_BALANCE_SCALE", Decimal("1"), errors)
        or Decimal("0"),
        max_notional_usd=_decimal_env("HLCT_MAX_NOTIONAL_USD", Decimal("250"), errors)
        or Decimal("0"),
        max_gross_exposure_usd=_decimal_env("HLCT_MAX_GROSS_EXPOSURE_USD", Decimal("1000"), errors)
        or Decimal("0"),
        max_open_positions=_int_env("HLCT_MAX_OPEN_POSITIONS", 20, errors),
        max_leverage=_int_env("HLCT_MAX_LEVERAGE", 3, errors),
        min_order_size=_decimal_env("HLCT_MIN_ORDER_SIZE", Decimal("0.0001"), errors)
        or Decimal("0"),
        slippage_bps=_decimal_env("HLCT_SLIPPAGE_BPS", Decimal("20"), errors) or Decimal("0"),
        close_slippage_bps=_decimal_env("HLCT_CLOSE_SLIPPAGE_BPS", Decimal("300"), errors)
        or Decimal("0"),
        hip3_oracle_envelope_bps=_decimal_env(
            "HLCT_HIP3_ORACLE_ENVELOPE_BPS", Decimal("100"), errors
        )
        or Decimal("0"),
        stale_source_ms=_int_env("HLCT_STALE_SOURCE_MS", 10_000, errors),
        stale_follower_ms=_int_env("HLCT_STALE_FOLLOWER_MS", 10_000, errors),
        rapid_flip_ms=_int_env("HLCT_RAPID_FLIP_MS", 1_500, errors),
    )
    api_private_key, api_private_key_file = _api_private_key_env(errors)
    source_wallet = os.getenv("HLCT_SOURCE_WALLET", "").strip().lower()
    if not source_wallet:
        errors.append("HLCT_SOURCE_WALLET is required")
    expected_account_mode_raw = (
        os.getenv("HLCT_EXPECTED_ACCOUNT_MODE", AccountMode.AUTO.value).strip().lower()
    )
    if expected_account_mode_raw == "perp":
        expected_account_mode_raw = AccountMode.STANDARD.value
    try:
        expected_account_mode = AccountMode(expected_account_mode_raw)
    except ValueError:
        errors.append(
            "HLCT_EXPECTED_ACCOUNT_MODE must be one of: "
            + ", ".join(item.value for item in AccountMode)
        )
        expected_account_mode = AccountMode.AUTO
    exchange = ExchangeConfig(
        follower_account_address=os.getenv("HLCT_FOLLOWER_ACCOUNT_ADDRESS", "").strip().lower(),
        api_wallet_address=os.getenv("HLCT_API_WALLET_ADDRESS", "").strip().lower(),
        api_private_key=api_private_key,
        api_private_key_file=api_private_key_file,
        vault_address=os.getenv("HLCT_VAULT_ADDRESS", "").strip().lower(),
        expected_account_mode=expected_account_mode,
        allow_master_private_key=_bool_env("HLCT_ALLOW_MASTER_PRIVATE_KEY", False, errors),
        testnet_enable=_bool_env("HLCT_TESTNET_ENABLE", True),
        live_enable=_bool_env("HLCT_LIVE_ENABLE", False, errors),
        confirm_mainnet_live=_bool_env("HLCT_CONFIRM_MAINNET_LIVE", False, errors),
        live_copy_enable=_bool_env("HLCT_LIVE_COPY_ENABLE", False, errors),
    )
    dead_man_policy_raw = (
        os.getenv("HLCT_DEAD_MAN_POLICY", DeadManPolicy.EXCHANGE_REQUIRED.value).strip().lower()
    )
    try:
        dead_man_policy = DeadManPolicy(dead_man_policy_raw)
    except ValueError:
        errors.append(
            "HLCT_DEAD_MAN_POLICY must be one of: "
            + ", ".join(item.value for item in DeadManPolicy)
        )
        dead_man_policy = DeadManPolicy.EXCHANGE_REQUIRED
    ops = OpsConfig(
        kill_switch_path=Path(os.getenv("HLCT_KILL_SWITCH_PATH", "data/KILL_SWITCH")),
        max_new_intents_per_cycle=_int_env("HLCT_MAX_NEW_INTENTS_PER_CYCLE", 10, errors),
        max_open_intents=_int_env("HLCT_MAX_OPEN_INTENTS", 20, errors),
        max_exchange_actions_per_minute=_int_env(
            "HLCT_MAX_EXCHANGE_ACTIONS_PER_MINUTE", 30, errors
        ),
        circuit_breaker_failure_threshold=_int_env(
            "HLCT_CIRCUIT_BREAKER_FAILURE_THRESHOLD", 3, errors
        ),
        circuit_breaker_cooldown_ms=_int_env("HLCT_CIRCUIT_BREAKER_COOLDOWN_MS", 60_000, errors),
        exchange_action_timeout_s=_decimal_env(
            "HLCT_EXCHANGE_ACTION_TIMEOUT_S", Decimal("15"), errors
        )
        or Decimal("0"),
        exchange_expires_after_ms=_int_env("HLCT_EXCHANGE_EXPIRES_AFTER_MS", 10_000, errors),
        dead_man_cancel_ms=_int_env("HLCT_DEAD_MAN_CANCEL_MS", 30_000, errors),
        dead_man_policy=dead_man_policy,
        containment_watchdog_ttl_ms=_int_env("HLCT_CONTAINMENT_WATCHDOG_TTL_MS", 15_000, errors),
        auth_probe_interval_ms=_int_env("HLCT_AUTH_PROBE_INTERVAL_MS", 600_000, errors),
        info_timeout_s=_decimal_env("HLCT_INFO_TIMEOUT_S", Decimal("10"), errors) or Decimal("0"),
        gui_token=os.getenv("HLCT_GUI_TOKEN", "").strip(),
        dashboard_control_max_per_minute=_int_env(
            "HLCT_DASHBOARD_CONTROL_MAX_PER_MINUTE",
            20,
            errors,
        ),
        runtime_lease_ttl_ms=_int_env("HLCT_RUNTIME_LEASE_TTL_MS", 30_000, errors),
        runtime_lock_dir=Path(
            os.getenv("HLCT_RUNTIME_LOCK_DIR", str(default_runtime_lock_dir())).strip()
            or str(default_runtime_lock_dir())
        ).expanduser(),
        dashboard_security_audit_ttl_ms=_int_env(
            "HLCT_DASHBOARD_SECURITY_AUDIT_TTL_MS",
            60_000,
            errors,
        ),
        source_reaction_queue_size=_int_env("HLCT_SOURCE_REACTION_QUEUE_SIZE", 100, errors),
        source_websocket_idle_timeout_ms=_int_env(
            "HLCT_SOURCE_WEBSOCKET_IDLE_TIMEOUT_MS",
            55_000,
            errors,
        ),
        source_websocket_heartbeat_timeout_ms=_int_env(
            "HLCT_SOURCE_WEBSOCKET_HEARTBEAT_TIMEOUT_MS",
            5_000,
            errors,
        ),
        source_websocket_reconnect_attempts=_int_env(
            "HLCT_SOURCE_WEBSOCKET_RECONNECT_ATTEMPTS",
            3,
            errors,
        ),
        source_websocket_reconnect_backoff_ms=_int_env(
            "HLCT_SOURCE_WEBSOCKET_RECONNECT_BACKOFF_MS",
            1_000,
            errors,
        ),
        source_fill_backfill_lookback_ms=_int_env(
            "HLCT_SOURCE_FILL_BACKFILL_LOOKBACK_MS",
            300_000,
            errors,
        ),
        source_fill_backfill_overlap_ms=_int_env(
            "HLCT_SOURCE_FILL_BACKFILL_OVERLAP_MS",
            5_000,
            errors,
        ),
        source_fill_backfill_max_pages=_int_env(
            "HLCT_SOURCE_FILL_BACKFILL_MAX_PAGES",
            5,
            errors,
        ),
        connection_siren_after_ms=_int_env("HLCT_CONNECTION_SIREN_AFTER_MS", 30_000, errors),
        validation_supervisor_lease_path=(
            Path(os.environ["HLCT_VALIDATION_SUPERVISOR_LEASE_PATH"].strip()).expanduser()
            if os.getenv("HLCT_VALIDATION_SUPERVISOR_LEASE_PATH", "").strip()
            else None
        ),
        validation_controller_registry_path=(
            Path(os.environ["HLCT_VALIDATION_CONTROLLER_REGISTRY_PATH"].strip()).expanduser()
            if os.getenv("HLCT_VALIDATION_CONTROLLER_REGISTRY_PATH", "").strip()
            else None
        ),
        validation_run_id=os.getenv("HLCT_VALIDATION_RUN_ID", "").strip(),
        validation_owner_token=os.getenv("HLCT_VALIDATION_OWNER_TOKEN", "").strip(),
        validation_state_identity_sha256=os.getenv("HLCT_VALIDATION_STATE_IDENTITY_SHA256", "")
        .strip()
        .lower(),
        validation_effective_config_sha256=os.getenv("HLCT_VALIDATION_EFFECTIVE_CONFIG_SHA256", "")
        .strip()
        .lower(),
        validation_effective_config_set_sha256=os.getenv(
            "HLCT_VALIDATION_EFFECTIVE_CONFIG_SET_SHA256", ""
        )
        .strip()
        .lower(),
        validation_supervisor_incarnation_id=os.getenv(
            "HLCT_VALIDATION_SUPERVISOR_INCARNATION_ID", ""
        ).strip(),
        validation_follower_set=_validation_follower_set_env(errors),
        validation_deadline_ms=_int_env("HLCT_VALIDATION_DEADLINE_MS", 0, errors),
        validation_market_universe_manifest_path=(
            Path(os.environ["HLCT_VALIDATION_MARKET_UNIVERSE_MANIFEST_PATH"].strip()).expanduser()
            if os.getenv("HLCT_VALIDATION_MARKET_UNIVERSE_MANIFEST_PATH", "").strip()
            else None
        ),
        validation_market_universe_sha256=os.getenv("HLCT_VALIDATION_MARKET_UNIVERSE_SHA256", "")
        .strip()
        .lower(),
        validation_market_universe_refresh_ms=_int_env(
            "HLCT_VALIDATION_MARKET_UNIVERSE_REFRESH_MS", 60_000, errors
        ),
        fast_execution_enabled=_bool_env("HLCT_FAST_EXECUTION_ENABLE", False, errors),
        deferred_delta_window_ms=_int_env("HLCT_DEFERRED_DELTA_WINDOW_MS", 300_000, errors),
        deferred_scheduler_bound_ms=_int_env("HLCT_DEFERRED_SCHEDULER_BOUND_MS", 100, errors),
        affected_follower_freshness_ms=_int_env(
            "HLCT_AFFECTED_FOLLOWER_FRESHNESS_MS", 5_000, errors
        ),
        full_follower_audit_ms=_int_env("HLCT_FULL_FOLLOWER_AUDIT_MS", 60_000, errors),
        market_catalog_refresh_ms=_int_env("HLCT_MARKET_CATALOG_REFRESH_MS", 60_000, errors),
        source_shard_count=_int_env("HLCT_SOURCE_SHARD_COUNT", 2, errors),
        action_shard_count=_int_env("HLCT_ACTION_SHARD_COUNT", 2, errors),
        market_data_connection_count=_int_env("HLCT_MARKET_DATA_CONNECTION_COUNT", 1, errors),
        market_event_queue_size=_int_env("HLCT_MARKET_EVENT_QUEUE_SIZE", 4_096, errors),
        action_queue_size=_int_env("HLCT_ACTION_QUEUE_SIZE", 256, errors),
        websocket_heartbeat_ms=_int_env("HLCT_WEBSOCKET_HEARTBEAT_MS", 30_000, errors),
        websocket_reconnect_min_ms=_int_env("HLCT_WEBSOCKET_RECONNECT_MIN_MS", 250, errors),
        websocket_reconnect_max_ms=_int_env("HLCT_WEBSOCKET_RECONNECT_MAX_MS", 5_000, errors),
        websocket_connection_limit=_int_env("HLCT_WEBSOCKET_CONNECTION_LIMIT", 10, errors),
        websocket_overlap_limit=_int_env("HLCT_WEBSOCKET_OVERLAP_LIMIT", 8, errors),
        websocket_subscription_limit=_int_env("HLCT_WEBSOCKET_SUBSCRIPTION_LIMIT", 1_000, errors),
        websocket_unique_user_limit=_int_env("HLCT_WEBSOCKET_UNIQUE_USER_LIMIT", 10, errors),
        websocket_outbound_per_minute=_int_env("HLCT_WEBSOCKET_OUTBOUND_PER_MINUTE", 2_000, errors),
        websocket_inflight_post_limit=_int_env("HLCT_WEBSOCKET_INFLIGHT_POST_LIMIT", 100, errors),
        rest_ordinary_weight_per_minute=_int_env(
            "HLCT_REST_ORDINARY_WEIGHT_PER_MINUTE", 720, errors
        ),
        rest_reserve_weight_per_minute=_int_env("HLCT_REST_RESERVE_WEIGHT_PER_MINUTE", 480, errors),
        clock_max_skew_ms=_int_env("HLCT_CLOCK_MAX_SKEW_MS", 500, errors),
        clock_max_jump_ms=_int_env("HLCT_CLOCK_MAX_JUMP_MS", 500, errors),
        direct_source_max_age_ms=_int_env("HLCT_DIRECT_SOURCE_MAX_AGE_MS", 5_000, errors),
        primary_cleanup_timeout_ms=_int_env("HLCT_PRIMARY_CLEANUP_TIMEOUT_MS", 1_800_000, errors),
        catalog_policy_version=os.getenv(
            "HLCT_CATALOG_POLICY_VERSION", "dynamic-all-active-v1"
        ).strip()
        or "dynamic-all-active-v1",
    )
    validation_guard_values = (
        ops.validation_supervisor_lease_path is not None,
        ops.validation_controller_registry_path is not None,
        bool(ops.validation_run_id),
        bool(ops.validation_owner_token),
        bool(ops.validation_state_identity_sha256),
        bool(ops.validation_supervisor_incarnation_id),
        len(ops.validation_follower_set) >= 1,
        ops.validation_deadline_ms > 0,
    )
    if any(validation_guard_values) and not all(validation_guard_values):
        errors.append(
            "HLCT validation supervisor guard requires lease path, run id, owner token, "
            "controller registry path, state identity SHA-256, supervisor incarnation, "
            "non-empty follower set, and a positive immutable deadline together"
        )
    effective_config_guard_values = (
        bool(ops.validation_effective_config_sha256),
        bool(ops.validation_effective_config_set_sha256),
    )
    if any(effective_config_guard_values) and not all(effective_config_guard_values):
        errors.append(
            "HLCT validation effective configuration requires per-slot and set SHA-256 together"
        )
    validation_sha256_values = {
        "HLCT_VALIDATION_STATE_IDENTITY_SHA256": ops.validation_state_identity_sha256,
        "HLCT_VALIDATION_EFFECTIVE_CONFIG_SHA256": ops.validation_effective_config_sha256,
        "HLCT_VALIDATION_EFFECTIVE_CONFIG_SET_SHA256": (ops.validation_effective_config_set_sha256),
    }
    for name, value in validation_sha256_values.items():
        if value and not re.fullmatch(r"[0-9a-f]{64}", value):
            errors.append(f"{name} must be 64 lowercase hex characters")
    market_guard_values = (
        ops.validation_market_universe_manifest_path is not None,
        bool(ops.validation_market_universe_sha256),
    )
    if any(market_guard_values) and not all(market_guard_values):
        errors.append("HLCT validation market universe requires manifest path and SHA-256 together")
    if ops.validation_market_universe_sha256 and not re.fullmatch(
        r"[0-9a-f]{64}", ops.validation_market_universe_sha256
    ):
        errors.append("HLCT_VALIDATION_MARKET_UNIVERSE_SHA256 must be 64 lowercase hex characters")
    if not 30_000 <= ops.validation_market_universe_refresh_ms <= 300_000:
        errors.append("HLCT_VALIDATION_MARKET_UNIVERSE_REFRESH_MS must be between 30000 and 300000")
    if ops.fast_execution_enabled:
        errors.extend(reviewed_fleet_runtime_policy_errors(ops))
        positive_fast_values = {
            "HLCT_DEFERRED_DELTA_WINDOW_MS": ops.deferred_delta_window_ms,
            "HLCT_DEFERRED_SCHEDULER_BOUND_MS": ops.deferred_scheduler_bound_ms,
            "HLCT_AFFECTED_FOLLOWER_FRESHNESS_MS": ops.affected_follower_freshness_ms,
            "HLCT_FULL_FOLLOWER_AUDIT_MS": ops.full_follower_audit_ms,
            "HLCT_MARKET_CATALOG_REFRESH_MS": ops.market_catalog_refresh_ms,
            "HLCT_SOURCE_SHARD_COUNT": ops.source_shard_count,
            "HLCT_ACTION_SHARD_COUNT": ops.action_shard_count,
            "HLCT_MARKET_DATA_CONNECTION_COUNT": ops.market_data_connection_count,
            "HLCT_MARKET_EVENT_QUEUE_SIZE": ops.market_event_queue_size,
            "HLCT_ACTION_QUEUE_SIZE": ops.action_queue_size,
            "HLCT_REST_ORDINARY_WEIGHT_PER_MINUTE": ops.rest_ordinary_weight_per_minute,
            "HLCT_REST_RESERVE_WEIGHT_PER_MINUTE": ops.rest_reserve_weight_per_minute,
            "HLCT_CLOCK_MAX_SKEW_MS": ops.clock_max_skew_ms,
            "HLCT_CLOCK_MAX_JUMP_MS": ops.clock_max_jump_ms,
            "HLCT_DIRECT_SOURCE_MAX_AGE_MS": ops.direct_source_max_age_ms,
            "HLCT_PRIMARY_CLEANUP_TIMEOUT_MS": ops.primary_cleanup_timeout_ms,
        }
        for setting_name, setting_value in positive_fast_values.items():
            if setting_value <= 0:
                errors.append(f"{setting_name} must be positive when fast execution is enabled")
        if ops.source_shard_count != 2 or ops.action_shard_count != 2:
            errors.append("fast execution requires exactly two source and two action shards")
        if ops.market_data_connection_count != 1:
            errors.append("fast execution requires exactly one market-data connection")
        if ops.websocket_overlap_limit > ops.websocket_connection_limit:
            errors.append("HLCT_WEBSOCKET_OVERLAP_LIMIT cannot exceed the connection limit")
        if ops.websocket_inflight_post_limit > 100:
            errors.append("HLCT_WEBSOCKET_INFLIGHT_POST_LIMIT cannot exceed 100")
        if risk.market_eligibility is not MarketEligibility.ALL_ACTIVE_MARKETS:
            errors.append("fast execution requires market eligibility all_active_markets")
    leaderboard = LeaderboardConfig(
        enabled=_bool_env("HLCT_LEADERBOARD_ENABLE", True, errors),
        url=os.getenv(
            "HLCT_LEADERBOARD_URL",
            "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard",
        ).strip(),
        cache_ttl_ms=_int_env("HLCT_LEADERBOARD_CACHE_TTL_MS", 300_000, errors),
        timeout_s=_decimal_env("HLCT_LEADERBOARD_TIMEOUT_S", Decimal("8"), errors) or Decimal("0"),
        min_volume_usd=_decimal_env("HLCT_LEADERBOARD_MIN_VOLUME_USD", Decimal("100000"), errors)
        or Decimal("0"),
        min_account_value_usd=_decimal_env(
            "HLCT_LEADERBOARD_MIN_ACCOUNT_VALUE_USD", Decimal("2000"), errors
        )
        or Decimal("0"),
        limit=_int_env("HLCT_LEADERBOARD_LIMIT", 100, errors),
    )
    address_analytics = AddressAnalyticsConfig(
        enabled=_bool_env("HLCT_ADDRESS_ANALYTICS_ENABLE", True, errors),
        url=os.getenv("HLCT_ADDRESS_ANALYTICS_URL", MAINNET_REST).strip(),
        cache_ttl_ms=_int_env("HLCT_ADDRESS_ANALYTICS_CACHE_TTL_MS", 120_000, errors),
        timeout_s=_decimal_env("HLCT_ADDRESS_ANALYTICS_TIMEOUT_S", Decimal("6"), errors)
        or Decimal("0"),
        window_days=_int_env("HLCT_ADDRESS_ANALYTICS_WINDOW_DAYS", 30, errors),
        max_pages=_int_env("HLCT_ADDRESS_ANALYTICS_MAX_PAGES", 12, errors),
    )
    subaccount_assignments = _subaccount_assignments_env(errors)
    return AppConfig(
        mode=mode,
        source_wallet=source_wallet,
        source_network=source_network,
        source_dex_scope=source_dex_scope,
        db_path=Path(
            os.getenv(
                "HLCT_DB_PATH",
                str(default_fleet_runtime_root() / "execution.sqlite3")
                if ops.fast_execution_enabled
                else "data/copytrader.sqlite3",
            )
        ),
        host=os.getenv("HLCT_HOST", "127.0.0.1"),
        port=_int_env("HLCT_PORT", 8080, errors),
        risk=risk,
        exchange=exchange,
        ops=ops,
        leaderboard=leaderboard,
        address_analytics=address_analytics,
        subaccount_assignments=subaccount_assignments,
        config_errors=tuple(errors),
    )
