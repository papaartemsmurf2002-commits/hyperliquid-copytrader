from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from ipaddress import ip_address
from typing import Any, Protocol

from .config import (
    MAX_ADDRESS_ANALYSIS_PAGES,
    MAX_LEADERBOARD_ROWS,
    AccountMode,
    AppConfig,
    DeadManPolicy,
    SourceNetwork,
)
from .markets import canonical_market_symbol, market_dex
from .models import Mode
from .unified_account import SourceDexScope


ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
PRIVATE_KEY_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
SUBACCOUNT_ASSIGNMENT_MODES = frozenset({"planned", *(mode.value for mode in Mode)})
EXCHANGE_MODES = frozenset({Mode.TESTNET, Mode.LIVE})


class AccountPreflightClient(Protocol):
    def account_preflight(self) -> list[str]: ...


@dataclass(frozen=True)
class PreflightReport:
    mode: Mode
    passed: bool
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def assert_passed(self) -> None:
        if not self.passed:
            raise PreflightError("; ".join(self.blockers))


class PreflightError(RuntimeError):
    pass


def _valid_address(value: str) -> bool:
    return bool(ADDRESS_RE.fullmatch(value or ""))


def _valid_private_key(value: str) -> bool:
    return bool(PRIVATE_KEY_RE.fullmatch(value or ""))


def _zero_private_key(value: str) -> bool:
    return bool(value and value.lower() == "0x" + "0" * 64)


def _same_address(left: str, right: str) -> bool:
    return bool(left and right and left.strip().lower() == right.strip().lower())


def _assignment_mode(value: str) -> str:
    return (value or "").strip().lower()


def _exchange_action_account(config: AppConfig) -> str:
    return (
        (config.exchange.vault_address or config.exchange.follower_account_address).strip().lower()
    )


def active_subaccount_assignment_status(config: AppConfig) -> dict[str, Any]:
    mode = config.mode.value
    source_wallet = (config.source_wallet or "").strip().lower()
    follower_account = (config.exchange.follower_account_address or "").strip().lower()
    vault_address = (config.exchange.vault_address or "").strip().lower()
    action_account = _exchange_action_account(config)
    enabled_for_mode = tuple(
        assignment
        for assignment in config.subaccount_assignments
        if assignment.enabled and _assignment_mode(assignment.mode) == mode
    )
    verified_for_mode = tuple(
        assignment for assignment in enabled_for_mode if assignment.subaccount_verified
    )
    matches = tuple(
        assignment
        for assignment in verified_for_mode
        if _same_address(assignment.source_wallet, source_wallet)
        and _same_address(assignment.subaccount, action_account)
    )
    required = config.mode in EXCHANGE_MODES and bool(enabled_for_mode)
    blockers: list[str] = []

    if config.mode not in EXCHANGE_MODES:
        return {
            "required": False,
            "passed": True,
            "status": "not_required",
            "detail": f"{mode} mode does not send exchange actions",
            "mode": mode,
            "source_wallet": source_wallet,
            "follower_account": follower_account,
            "vault_address": vault_address,
            "action_account": action_account,
            "matched_slot": "",
            "enabled_for_mode": len(enabled_for_mode),
            "verified_for_mode": len(verified_for_mode),
            "matches": len(matches),
            "blockers": blockers,
        }

    if not enabled_for_mode:
        status = "not_configured"
        detail = "single-account exchange config; no enabled assignment for current mode"
    else:
        unverified_slots = [
            assignment.slot or f"assignment-{index + 1}"
            for index, assignment in enumerate(enabled_for_mode)
            if not assignment.subaccount_verified
        ]
        if unverified_slots:
            status = "blocked_unverified"
            detail = "enabled assignment rows require operator verification"
            blockers.append(
                f"active {mode} subaccount assignment(s) are unverified: "
                f"{', '.join(unverified_slots)}"
            )
        elif not _valid_address(source_wallet) or not _valid_address(action_account):
            status = "blocked_config"
            detail = "active assignment cannot resolve the current source/action account"
            blockers.append(
                f"active {mode} subaccount assignment requires valid HLCT_SOURCE_WALLET "
                "and follower/vault action account addresses"
            )
        elif len(matches) == 0:
            status = "missing_match"
            detail = "no enabled verified assignment matches this runtime"
            blockers.append(
                f"no verified enabled {mode} subaccount assignment matches "
                "HLCT_SOURCE_WALLET and the follower/vault action account"
            )
        elif len(matches) > 1:
            status = "multiple_matches"
            detail = "more than one assignment matches this runtime"
            slots = ", ".join(assignment.slot or "unnamed" for assignment in matches)
            blockers.append(
                f"multiple verified enabled {mode} subaccount assignments match "
                f"the current source/action account: {slots}"
            )
        else:
            status = "matched"
            detail = f"slot {matches[0].slot} matches current source/action account"

    return {
        "required": required,
        "passed": not blockers,
        "status": status,
        "detail": detail,
        "mode": mode,
        "source_wallet": source_wallet,
        "follower_account": follower_account,
        "vault_address": vault_address,
        "action_account": action_account,
        "matched_slot": matches[0].slot if len(matches) == 1 else "",
        "enabled_for_mode": len(enabled_for_mode),
        "verified_for_mode": len(verified_for_mode),
        "matches": len(matches),
        "blockers": blockers,
    }


def _append_unique(target: list[str], values: list[str]) -> None:
    for value in values:
        if value not in target:
            target.append(value)


def _finite_decimal_or_zero(value: Decimal, field: str, blockers: list[str]) -> Decimal:
    if value.is_finite():
        return value
    blockers.append(f"{field} must be a finite decimal value")
    return Decimal("0")


def _derive_private_key_address(private_key: str) -> tuple[str | None, str | None]:
    try:
        from eth_account import Account
    except Exception as exc:  # pragma: no cover - dependency/environment path
        return None, f"eth_account is not importable: {exc}"
    try:
        return Account.from_key(private_key).address.lower(), None
    except Exception as exc:
        return None, str(exc)


def is_loopback_host(host: str) -> bool:
    lowered = (host or "").strip().lower()
    if lowered in {"localhost", "127.0.0.1", "::1"}:
        return True
    try:
        return ip_address(lowered).is_loopback
    except ValueError:
        return False


def build_preflight_report(
    config: AppConfig, client: AccountPreflightClient | None = None
) -> PreflightReport:
    blockers: list[str] = []
    warnings: list[str] = []

    blockers.extend(config.config_errors)

    fixed_multiplier = _finite_decimal_or_zero(
        config.risk.fixed_multiplier,
        "HLCT_FIXED_MULTIPLIER",
        blockers,
    )
    equity_ratio = config.risk.equity_ratio
    if equity_ratio is not None and not equity_ratio.is_finite():
        blockers.append("HLCT_EQUITY_RATIO must be a finite decimal value")
        equity_ratio = None
    sizing_equity_cap = config.risk.sizing_equity_cap_usd
    if sizing_equity_cap is not None and (
        not sizing_equity_cap.is_finite() or sizing_equity_cap <= 0
    ):
        cap_error = "HLCT_SIZING_EQUITY_CAP_USD must be a finite positive decimal value"
        if cap_error not in blockers:
            blockers.append(cap_error)
    max_balance_scale = _finite_decimal_or_zero(
        config.risk.max_balance_scale,
        "HLCT_MAX_BALANCE_SCALE",
        blockers,
    )
    max_notional_usd = _finite_decimal_or_zero(
        config.risk.max_notional_usd,
        "HLCT_MAX_NOTIONAL_USD",
        blockers,
    )
    max_gross_exposure_usd = _finite_decimal_or_zero(
        config.risk.max_gross_exposure_usd,
        "HLCT_MAX_GROSS_EXPOSURE_USD",
        blockers,
    )
    min_order_size = _finite_decimal_or_zero(
        config.risk.min_order_size,
        "HLCT_MIN_ORDER_SIZE",
        blockers,
    )
    slippage_bps = _finite_decimal_or_zero(
        config.risk.slippage_bps,
        "HLCT_SLIPPAGE_BPS",
        blockers,
    )
    close_slippage_bps = _finite_decimal_or_zero(
        config.risk.close_slippage_bps,
        "HLCT_CLOSE_SLIPPAGE_BPS",
        blockers,
    )
    hip3_oracle_envelope_bps = _finite_decimal_or_zero(
        config.risk.hip3_oracle_envelope_bps,
        "HLCT_HIP3_ORACLE_ENVELOPE_BPS",
        blockers,
    )
    exchange_action_timeout_s = _finite_decimal_or_zero(
        config.ops.exchange_action_timeout_s,
        "HLCT_EXCHANGE_ACTION_TIMEOUT_S",
        blockers,
    )
    info_timeout_s = _finite_decimal_or_zero(
        config.ops.info_timeout_s,
        "HLCT_INFO_TIMEOUT_S",
        blockers,
    )

    if not _valid_address(config.source_wallet):
        blockers.append("source wallet must be a 42-character hex address")

    if config.mode == Mode.LIVE and config.resolved_source_network != SourceNetwork.MAINNET:
        blockers.append("live mode requires HLCT_SOURCE_NETWORK=mode or mainnet")

    if config.mode == Mode.TESTNET and config.resolved_source_network == SourceNetwork.MAINNET:
        warnings.append(
            "testnet mode is observing the mainnet source read-only while executing on testnet"
        )

    if not config.risk.allowed_symbols:
        blockers.append("at least one allowed symbol is required")

    canonical_symbols: list[str] = []
    for symbol in config.risk.allowed_symbols:
        try:
            canonical = canonical_market_symbol(symbol)
        except ValueError as exc:
            blockers.append(f"allowed symbol {symbol!r} is invalid: {exc}")
            continue
        if canonical in canonical_symbols:
            blockers.append(f"allowed symbols contain duplicate market {canonical}")
            continue
        canonical_symbols.append(canonical)

    configured_hip3 = [symbol for symbol in canonical_symbols if market_dex(symbol)]
    if configured_hip3 and config.source_dex_scope.value != "all_configured_markets":
        blockers.append(
            "DEX-qualified allowed symbols require HLCT_SOURCE_DEX_SCOPE=all_configured_markets"
        )

    if config.source_dex_scope == SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY:
        if not config.risk.balance_sizing_enabled:
            blockers.append(
                "default_only_account_equity source scope requires automatic balance sizing"
            )
        if equity_ratio is not None:
            blockers.append(
                "default_only_account_equity source scope forbids an explicit equity ratio"
            )
        if sizing_equity_cap is None or not sizing_equity_cap.is_finite() or sizing_equity_cap <= 0:
            blockers.append(
                "default_only_account_equity source scope requires a finite positive sizing equity cap"
            )
        if any(":" in symbol for symbol in config.risk.allowed_symbols):
            blockers.append(
                "default_only_account_equity source scope cannot allow non-default DEX symbols"
            )
        warnings.append(
            "source scope copies default-perp positions only and uses total shared Unified "
            "collateral as the sizing denominator; non-default source exposure is excluded "
            "from copy targets and reported as reduced fidelity"
        )
    elif config.source_dex_scope.value == "all_configured_markets":
        if (
            config.mode in EXCHANGE_MODES
            and config.exchange.expected_account_mode != AccountMode.UNIFIED
        ):
            blockers.append(
                "all_configured_markets exchange mode requires HLCT_EXPECTED_ACCOUNT_MODE=unified"
            )
        warnings.append(
            "source scope observes all DEX positions, copies only configured markets, and uses "
            "total shared Unified Spot USDC as the account-level sizing denominator"
        )

    for index, assignment in enumerate(config.subaccount_assignments):
        label = assignment.slot or f"assignment-{index + 1}"
        assignment_mode = _assignment_mode(assignment.mode)
        if not assignment.slot:
            blockers.append(f"subaccount assignment {index + 1} slot is required")
        if assignment_mode not in SUBACCOUNT_ASSIGNMENT_MODES:
            allowed_modes = ", ".join(sorted(SUBACCOUNT_ASSIGNMENT_MODES))
            blockers.append(f"subaccount assignment {label} mode must be one of: {allowed_modes}")
        if not _valid_address(assignment.subaccount):
            blockers.append(
                f"subaccount assignment {label} subaccount must be a 42-character hex address"
            )
        if not _valid_address(assignment.source_wallet):
            blockers.append(
                f"subaccount assignment {label} source wallet must be a 42-character hex address"
            )
        if _same_address(assignment.subaccount, assignment.source_wallet):
            blockers.append(
                f"subaccount assignment {label} source wallet and subaccount must differ"
            )
        if assignment.enabled and not assignment.subaccount_verified:
            blockers.append(
                f"subaccount assignment {label} is enabled but subaccount_verified is not true"
            )
        if not assignment.subaccount_verified:
            warnings.append(
                f"subaccount assignment {label} is not verified through UI/API evidence"
            )
        if assignment.subaccount_verified and not assignment.operator_verified_at:
            warnings.append(
                f"subaccount assignment {label} is verified without operator_verified_at evidence"
            )

    if fixed_multiplier <= 0 and (equity_ratio is None or equity_ratio <= 0):
        blockers.append("fixed multiplier or equity ratio must be positive")

    if max_balance_scale <= Decimal("0"):
        blockers.append("max balance scale must be positive")

    if max_notional_usd <= Decimal("0"):
        blockers.append("max notional cap must be nonzero")

    if max_gross_exposure_usd <= Decimal("0"):
        blockers.append("max gross exposure cap must be nonzero")

    if max_gross_exposure_usd > Decimal("0") and max_gross_exposure_usd < max_notional_usd:
        warnings.append("max gross exposure cap is below per-symbol max notional cap")

    if config.risk.max_open_positions <= 0:
        blockers.append("max open positions must be positive")

    if config.risk.max_leverage <= 0:
        blockers.append("max leverage must be positive")

    if min_order_size < 0:
        blockers.append("min order size cannot be negative")

    if slippage_bps < Decimal("0"):
        blockers.append("slippage bps cannot be negative")

    if slippage_bps >= Decimal("10000"):
        blockers.append("slippage bps must be below 10000")

    if close_slippage_bps < Decimal("0"):
        blockers.append("close slippage bps cannot be negative")

    if close_slippage_bps >= Decimal("10000"):
        blockers.append("close slippage bps must be below 10000")

    if config.mode in EXCHANGE_MODES and close_slippage_bps > Decimal("1000"):
        blockers.append("exchange modes require close slippage bps at or below 1000")

    if config.mode in EXCHANGE_MODES and slippage_bps > Decimal("1000"):
        blockers.append("exchange modes require entry slippage bps at or below 1000")

    if not Decimal("0") < hip3_oracle_envelope_bps <= Decimal("1000"):
        blockers.append("HIP-3 oracle envelope bps must be above 0 and at or below 1000")

    if config.risk.stale_source_ms <= 0:
        blockers.append("stale source threshold must be positive")

    if config.risk.stale_follower_ms <= 0:
        blockers.append("stale follower threshold must be positive")

    if config.ops.max_new_intents_per_cycle <= 0:
        blockers.append("max new intents per cycle must be positive")

    if config.ops.max_open_intents < 0:
        blockers.append("max open intents cannot be negative")

    if config.ops.max_exchange_actions_per_minute <= 0:
        blockers.append("max exchange actions per minute must be positive")
    if config.mode in EXCHANGE_MODES and 0 < config.ops.max_exchange_actions_per_minute < 3:
        blockers.append(
            "exchange modes require at least 3 exchange actions per minute for safe cleanup"
        )

    if config.ops.circuit_breaker_failure_threshold <= 0:
        blockers.append("circuit breaker failure threshold must be positive")

    if config.ops.circuit_breaker_cooldown_ms < 0:
        blockers.append("circuit breaker cooldown cannot be negative")

    if exchange_action_timeout_s <= 0:
        blockers.append("exchange action timeout must be positive")

    if config.ops.exchange_expires_after_ms <= 0:
        blockers.append("exchange expires-after window must be positive")
    if config.mode in EXCHANGE_MODES and 0 < config.ops.exchange_expires_after_ms < 2_000:
        blockers.append("exchange modes require an expires-after window of at least 2000ms")

    exchange_action_timeout_ms = exchange_action_timeout_s * Decimal("1000")
    if (
        config.ops.exchange_expires_after_ms > 0
        and exchange_action_timeout_s > 0
        and Decimal(config.ops.exchange_expires_after_ms) > exchange_action_timeout_ms
    ):
        blockers.append("exchange expires-after window cannot exceed exchange action timeout")

    if config.ops.dead_man_cancel_ms < 0:
        blockers.append("dead-man cancel window cannot be negative")

    if config.mode in {Mode.TESTNET, Mode.LIVE} and config.ops.dead_man_cancel_ms < 6_000:
        blockers.append("dead-man cancel window must be at least 6000ms in exchange modes")
    if (
        config.mode in {Mode.TESTNET, Mode.LIVE}
        and exchange_action_timeout_s > 0
        and Decimal(config.ops.dead_man_cancel_ms) <= exchange_action_timeout_ms + Decimal("1000")
    ):
        blockers.append(
            "dead-man cancel window must exceed exchange action timeout by more than 1000ms"
        )
    if config.ops.containment_watchdog_ttl_ms <= 0:
        blockers.append("containment watchdog heartbeat TTL must be positive")
    if (
        config.mode in EXCHANGE_MODES
        and config.ops.dead_man_policy == DeadManPolicy.WATCHDOG_FALLBACK
        and not 1_000 <= config.ops.containment_watchdog_ttl_ms <= 15_000
    ):
        blockers.append(
            "watchdog fallback requires containment heartbeat TTL between 1000ms and 15000ms"
        )

    if config.ops.auth_probe_interval_ms <= 0:
        blockers.append("auth probe interval must be positive")

    if info_timeout_s <= 0:
        blockers.append("info timeout must be positive")

    if config.ops.dashboard_control_max_per_minute <= 0:
        blockers.append("dashboard control max per minute must be positive")

    if config.ops.runtime_lease_ttl_ms <= 0:
        blockers.append("runtime lease TTL must be positive")

    if config.mode in EXCHANGE_MODES:
        if not config.db_path.is_absolute():
            blockers.append("exchange modes require HLCT_DB_PATH to be an absolute durable path")
        if not config.ops.kill_switch_path.is_absolute():
            blockers.append(
                "exchange modes require HLCT_KILL_SWITCH_PATH to be an absolute durable path"
            )
        if not config.ops.runtime_lock_dir.is_absolute():
            blockers.append("exchange modes require HLCT_RUNTIME_LOCK_DIR to be an absolute path")
        elif config.ops.runtime_lock_dir.exists() and not config.ops.runtime_lock_dir.is_dir():
            blockers.append("HLCT_RUNTIME_LOCK_DIR must be a directory")

    if config.ops.dashboard_security_audit_ttl_ms <= 0:
        blockers.append("dashboard security audit TTL must be positive")

    if config.ops.source_reaction_queue_size <= 0:
        blockers.append("source reaction queue size must be positive")

    if config.ops.source_websocket_idle_timeout_ms <= 0:
        blockers.append("source websocket idle timeout must be positive")

    if config.ops.source_websocket_idle_timeout_ms >= 60_000:
        blockers.append(
            "source websocket idle timeout must be below Hyperliquid's 60s heartbeat window"
        )

    if config.ops.source_websocket_heartbeat_timeout_ms <= 0:
        blockers.append("source websocket heartbeat timeout must be positive")

    if config.ops.connection_siren_after_ms <= 0:
        blockers.append("connection siren threshold must be positive")

    if config.ops.source_websocket_reconnect_attempts < 0:
        blockers.append("source websocket reconnect attempts cannot be negative")

    if config.ops.source_websocket_reconnect_backoff_ms < 0:
        blockers.append("source websocket reconnect backoff cannot be negative")

    if config.ops.source_fill_backfill_lookback_ms < 0:
        blockers.append("source fill backfill lookback cannot be negative")

    if config.ops.source_fill_backfill_overlap_ms < 0:
        blockers.append("source fill backfill overlap cannot be negative")

    if config.ops.source_fill_backfill_max_pages <= 0:
        blockers.append("source fill backfill max pages must be positive")

    if config.leaderboard.enabled:
        leaderboard_timeout_s = _finite_decimal_or_zero(
            config.leaderboard.timeout_s,
            "HLCT_LEADERBOARD_TIMEOUT_S",
            blockers,
        )
        leaderboard_min_volume_usd = _finite_decimal_or_zero(
            config.leaderboard.min_volume_usd,
            "HLCT_LEADERBOARD_MIN_VOLUME_USD",
            blockers,
        )
        leaderboard_min_account_value_usd = _finite_decimal_or_zero(
            config.leaderboard.min_account_value_usd,
            "HLCT_LEADERBOARD_MIN_ACCOUNT_VALUE_USD",
            blockers,
        )
        if not config.leaderboard.url.startswith(("https://", "http://")):
            blockers.append("leaderboard URL must be an HTTP(S) URL")
        if config.leaderboard.cache_ttl_ms <= 0:
            blockers.append("leaderboard cache TTL must be positive")
        if leaderboard_timeout_s <= 0:
            blockers.append("leaderboard timeout must be positive")
        if leaderboard_min_volume_usd < Decimal("0"):
            blockers.append("leaderboard minimum volume cannot be negative")
        if leaderboard_min_account_value_usd < Decimal("0"):
            blockers.append("leaderboard minimum account value cannot be negative")
        if config.leaderboard.limit <= 0:
            blockers.append("leaderboard limit must be positive")
        if config.leaderboard.limit > MAX_LEADERBOARD_ROWS:
            blockers.append(f"leaderboard limit cannot exceed {MAX_LEADERBOARD_ROWS}")

    if config.address_analytics.enabled:
        address_analytics_timeout_s = _finite_decimal_or_zero(
            config.address_analytics.timeout_s,
            "HLCT_ADDRESS_ANALYTICS_TIMEOUT_S",
            blockers,
        )
        if not config.address_analytics.url.startswith(("https://", "http://")):
            blockers.append("address analytics URL must be an HTTP(S) URL")
        if config.address_analytics.cache_ttl_ms <= 0:
            blockers.append("address analytics cache TTL must be positive")
        if address_analytics_timeout_s <= 0:
            blockers.append("address analytics timeout must be positive")
        if config.address_analytics.window_days <= 0:
            blockers.append("address analytics window days must be positive")
        if config.address_analytics.max_pages <= 0:
            blockers.append("address analytics max pages must be positive")
        if config.address_analytics.max_pages > MAX_ADDRESS_ANALYSIS_PAGES:
            blockers.append(
                f"address analytics max pages cannot exceed {MAX_ADDRESS_ANALYSIS_PAGES}"
            )

    if config.port <= 0 or config.port > 65535:
        blockers.append("GUI port must be between 1 and 65535")

    if config.ops.gui_token and len(config.ops.gui_token) < 16:
        blockers.append("GUI token must be at least 16 characters when configured")

    if not is_loopback_host(config.host):
        blockers.append(
            "direct non-loopback GUI binding is unsupported; bind loopback behind a trusted "
            "TLS reverse proxy"
        )

    if config.mode in {Mode.TESTNET, Mode.LIVE}:
        follower_address_valid = _valid_address(config.exchange.follower_account_address)
        api_wallet_address_valid = _valid_address(config.exchange.api_wallet_address)
        source_address_valid = _valid_address(config.source_wallet)
        vault_address_valid = _valid_address(config.exchange.vault_address)
        if not _valid_address(config.exchange.follower_account_address):
            blockers.append("follower account address is required for exchange modes")
        elif _same_address(config.exchange.follower_account_address, config.source_wallet):
            blockers.append("follower account must not be the source wallet")
        if config.exchange.api_wallet_address:
            if not api_wallet_address_valid:
                blockers.append(
                    "api wallet address must be a 42-character hex address when configured"
                )
            elif source_address_valid and _same_address(
                config.exchange.api_wallet_address, config.source_wallet
            ):
                blockers.append("api wallet address must not be the source wallet")
            elif follower_address_valid and _same_address(
                config.exchange.api_wallet_address,
                config.exchange.follower_account_address,
            ):
                blockers.append("api wallet address must be distinct from the follower account")
        if not config.exchange.api_private_key:
            blockers.append("api private key is required for exchange modes")
        elif not _valid_private_key(config.exchange.api_private_key):
            blockers.append("api private key must be a 0x-prefixed 32-byte hex value")
        elif _zero_private_key(config.exchange.api_private_key):
            blockers.append("api private key cannot be all zeroes")
        else:
            signer_address, signer_error = _derive_private_key_address(
                config.exchange.api_private_key
            )
            if signer_error:
                blockers.append(f"api private key could not be loaded: {signer_error}")
            elif api_wallet_address_valid and not _same_address(
                signer_address or "", config.exchange.api_wallet_address
            ):
                blockers.append("api private key does not match HLCT_API_WALLET_ADDRESS")
            elif source_address_valid and _same_address(signer_address or "", config.source_wallet):
                blockers.append("api signer wallet must not be the source wallet")
            elif vault_address_valid and _same_address(
                signer_address or "", config.exchange.vault_address
            ):
                blockers.append(
                    "api signer wallet must not be the configured vault/subaccount address"
                )
            elif follower_address_valid and _same_address(
                signer_address or "", config.exchange.follower_account_address
            ):
                if config.mode == Mode.LIVE:
                    blockers.append("live mode forbids the follower/master private key")
                elif not config.exchange.allow_master_private_key:
                    blockers.append(
                        "trading-account private key requires the explicit testnet-only "
                        "HLCT_ALLOW_MASTER_PRIVATE_KEY=true acknowledgement"
                    )
                else:
                    warnings.append(
                        "testnet is using the trading-account private key; a dedicated approved "
                        "API wallet is safer"
                    )
        if config.exchange.api_private_key and config.exchange.api_private_key == "changeme":
            blockers.append("api private key cannot be the example value")
        if config.exchange.vault_address:
            if not vault_address_valid:
                blockers.append("vault address must be a 42-character hex address when configured")
            elif source_address_valid and _same_address(
                config.exchange.vault_address, config.source_wallet
            ):
                blockers.append("vault address must not be the source wallet")
            elif follower_address_valid and not _same_address(
                config.exchange.vault_address,
                config.exchange.follower_account_address,
            ):
                blockers.append(
                    "vault address must match follower account address so signed actions and "
                    "reconcile query the same account"
                )
        assignment_status = active_subaccount_assignment_status(config)
        _append_unique(blockers, list(assignment_status["blockers"]))
        if max_notional_usd < Decimal("10"):
            blockers.append("exchange modes require max notional cap of at least $10")
        if max_gross_exposure_usd < Decimal("10"):
            blockers.append("exchange modes require max gross exposure cap of at least $10")

    if config.mode == Mode.LIVE:
        if not config.exchange.live_enable:
            blockers.append("live mode requires HLCT_LIVE_ENABLE=true")
        if not config.exchange.confirm_mainnet_live:
            blockers.append("live mode requires HLCT_CONFIRM_MAINNET_LIVE=true")
        if not config.exchange.api_wallet_address:
            blockers.append("live mode requires explicit HLCT_API_WALLET_ADDRESS")
        if config.exchange.allow_master_private_key:
            blockers.append(
                "HLCT_ALLOW_MASTER_PRIVATE_KEY is testnet-only and forbidden in live mode"
            )
        if max_notional_usd <= Decimal("0"):
            blockers.append("live mode requires nonzero max notional cap")
        if not config.exchange.live_copy_enable:
            warnings.append(
                "generic live copy runner is disabled by HLCT_LIVE_COPY_ENABLE=false; "
                "the isolated mainnet canary remains available"
            )

    if config.mode in {Mode.SHADOW, Mode.PAPER} and config.exchange.api_private_key:
        warnings.append("api key is configured but this mode will not send exchange actions")

    if equity_ratio is not None:
        warnings.append(
            "HLCT_EQUITY_RATIO is an explicit scale override and disables automatic "
            "source/follower accountValue balance sizing"
        )

    if client is not None and config.mode in {Mode.TESTNET, Mode.LIVE}:
        try:
            client_blockers = client.account_preflight()
        except Exception as exc:  # pragma: no cover - defensive runtime path
            blockers.append(f"exchange account preflight failed: {exc}")
        else:
            blockers.extend(client_blockers)

    return PreflightReport(
        mode=config.mode,
        passed=not blockers,
        blockers=blockers,
        warnings=warnings,
    )
