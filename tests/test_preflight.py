from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from hyperliquid_copytrader.config import (
    AccountMode,
    AddressAnalyticsConfig,
    ExchangeConfig,
    LeaderboardConfig,
    MAINNET_REST,
    OpsConfig,
    SourceNetwork,
    SubaccountAssignment,
    TESTNET_REST,
    load_config,
)
from hyperliquid_copytrader.models import Mode
from hyperliquid_copytrader.preflight import (
    active_subaccount_assignment_status,
    build_preflight_report,
)
from hyperliquid_copytrader.unified_account import SourceDexScope


PRIVATE_KEY_1 = "0x" + "1" * 64
PRIVATE_KEY_1_ADDRESS = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a"
SOURCE_WALLET = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
FOLLOWER_ACCOUNT = "0xf000000000000000000000000000000000000000"
ALT_FOLLOWER_ACCOUNT = "0xf111111111111111111111111111111111111111"


def set_source_wallet(monkeypatch) -> None:
    monkeypatch.setenv("HLCT_SOURCE_WALLET", SOURCE_WALLET)


def test_default_only_account_equity_scope_requires_bounded_automatic_sizing(base_config):
    valid = replace(
        base_config,
        source_dex_scope=SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY,
        risk=replace(base_config.risk, sizing_equity_cap_usd=Decimal("50")),
    )
    invalid = replace(
        valid,
        risk=replace(
            valid.risk,
            balance_sizing_enabled=False,
            equity_ratio=Decimal("0.1"),
            sizing_equity_cap_usd=None,
            allowed_symbols=("xyz:FOO",),
        ),
    )

    valid_report = build_preflight_report(valid)
    invalid_report = build_preflight_report(invalid)

    assert valid_report.passed is True
    assert any("reduced fidelity" in warning for warning in valid_report.warnings)
    blockers = " ".join(invalid_report.blockers)
    assert "requires automatic balance sizing" in blockers
    assert "forbids an explicit equity ratio" in blockers
    assert "requires a finite positive sizing equity cap" in blockers
    assert "cannot allow non-default DEX symbols" in blockers


def test_all_configured_markets_scope_accepts_hip3_and_reports_unified_equity_basis(base_config):
    config = replace(
        base_config,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("BTC", "xyz:AAPL")),
    )

    report = build_preflight_report(config)

    assert report.passed is True
    assert any("total shared Unified Spot USDC" in warning for warning in report.warnings)


def test_all_configured_markets_exchange_mode_requires_explicit_unified_follower(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("BTC", "xyz:AAPL")),
        exchange=replace(
            base_config.exchange,
            expected_account_mode=AccountMode.STANDARD,
        ),
    )

    report = build_preflight_report(config)

    assert any("HLCT_EXPECTED_ACCOUNT_MODE=unified" in item for item in report.blockers)


def test_hip3_allowlist_requires_all_configured_markets_scope(base_config):
    config = replace(
        base_config,
        source_dex_scope=SourceDexScope.STRICT,
        risk=replace(base_config.risk, allowed_symbols=("xyz:AAPL",)),
    )

    report = build_preflight_report(config)

    assert report.passed is False
    assert any("all_configured_markets" in blocker for blocker in report.blockers)


def test_live_is_blocked_by_default(base_config):
    config = replace(base_config, mode=Mode.LIVE)
    report = build_preflight_report(config)
    assert not report.passed
    assert any("HLCT_LIVE_ENABLE" in blocker for blocker in report.blockers)
    assert any("HLCT_CONFIRM_MAINNET_LIVE" in blocker for blocker in report.blockers)


def test_live_requires_credentials_and_nonzero_caps(base_config):
    risk = replace(
        base_config.risk,
        max_notional_usd=Decimal("0"),
        max_gross_exposure_usd=Decimal("0"),
    )
    config = replace(
        base_config,
        mode=Mode.LIVE,
        risk=risk,
        exchange=ExchangeConfig(live_enable=True, confirm_mainnet_live=True),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("follower account address" in blocker for blocker in report.blockers)
    assert any("max notional" in blocker for blocker in report.blockers)
    assert any("gross exposure" in blocker for blocker in report.blockers)


def test_testnet_does_not_require_enable_flag(base_config):
    config = replace(base_config, mode=Mode.TESTNET)
    report = build_preflight_report(config)
    assert not report.passed
    assert not any("HLCT_TESTNET_ENABLE" in blocker for blocker in report.blockers)
    assert any("follower account address" in blocker for blocker in report.blockers)


def test_non_loopback_gui_host_requires_token(base_config):
    config = replace(base_config, host="0.0.0.0")
    report = build_preflight_report(config)
    assert not report.passed
    assert any("direct non-loopback GUI binding" in blocker for blocker in report.blockers)


def test_short_gui_token_is_rejected(base_config):
    config = replace(base_config, ops=OpsConfig(gui_token="short"))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("GUI token" in blocker for blocker in report.blockers)


def test_non_loopback_gui_host_is_rejected_even_with_token(base_config):
    config = replace(base_config, host="0.0.0.0", ops=OpsConfig(gui_token="long-enough-token"))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("TLS reverse proxy" in blocker for blocker in report.blockers)


def test_runtime_lease_ttl_must_be_positive(base_config):
    config = replace(base_config, ops=OpsConfig(runtime_lease_ttl_ms=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("runtime lease TTL" in blocker for blocker in report.blockers)


def test_exchange_mode_requires_absolute_account_global_lock_dir(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(base_config.ops, runtime_lock_dir=Path("relative-locks")),
    )

    report = build_preflight_report(config)

    assert "exchange modes require HLCT_RUNTIME_LOCK_DIR to be an absolute path" in report.blockers


def test_exchange_mode_requires_absolute_journal_and_kill_switch_paths(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        db_path=Path("relative.sqlite3"),
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, kill_switch_path=Path("relative-kill")),
    )

    report = build_preflight_report(config)

    assert "exchange modes require HLCT_DB_PATH to be an absolute durable path" in report.blockers
    assert any("HLCT_KILL_SWITCH_PATH" in blocker for blocker in report.blockers)


def test_exchange_dead_man_window_must_outlive_action_timeout(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            exchange_action_timeout_s=Decimal("10"),
            exchange_expires_after_ms=5_000,
            dead_man_cancel_ms=11_000,
        ),
    )

    report = build_preflight_report(config)

    assert any("must exceed exchange action timeout" in blocker for blocker in report.blockers)


def test_dashboard_security_audit_ttl_must_be_positive(base_config):
    config = replace(base_config, ops=OpsConfig(dashboard_security_audit_ttl_ms=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("dashboard security audit TTL" in blocker for blocker in report.blockers)


def test_dashboard_control_rate_limit_must_be_positive(base_config):
    config = replace(base_config, ops=OpsConfig(dashboard_control_max_per_minute=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("dashboard control max per minute" in blocker for blocker in report.blockers)


def test_source_fill_backfill_config_must_be_valid(base_config):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_websocket_idle_timeout_ms=0,
            source_websocket_heartbeat_timeout_ms=0,
            source_websocket_reconnect_attempts=-1,
            source_websocket_reconnect_backoff_ms=-1,
            source_fill_backfill_lookback_ms=-1,
            source_fill_backfill_overlap_ms=-1,
            source_fill_backfill_max_pages=0,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("websocket idle timeout" in blocker for blocker in report.blockers)
    assert any("websocket heartbeat timeout" in blocker for blocker in report.blockers)
    assert any("websocket reconnect attempts" in blocker for blocker in report.blockers)
    assert any("websocket reconnect backoff" in blocker for blocker in report.blockers)
    assert any("backfill lookback" in blocker for blocker in report.blockers)
    assert any("backfill overlap" in blocker for blocker in report.blockers)
    assert any("backfill max pages" in blocker for blocker in report.blockers)


def test_source_websocket_idle_timeout_must_be_inside_hyperliquid_window(base_config):
    config = replace(base_config, ops=OpsConfig(source_websocket_idle_timeout_ms=60_000))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("60s heartbeat window" in blocker for blocker in report.blockers)


def test_leaderboard_config_must_be_valid(base_config):
    config = replace(
        base_config,
        leaderboard=LeaderboardConfig(
            url="file:///tmp/leaderboard.json",
            cache_ttl_ms=0,
            timeout_s=Decimal("0"),
            min_volume_usd=Decimal("-1"),
            min_account_value_usd=Decimal("-1"),
            limit=0,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("leaderboard URL" in blocker for blocker in report.blockers)
    assert any("leaderboard cache TTL" in blocker for blocker in report.blockers)
    assert any("leaderboard timeout" in blocker for blocker in report.blockers)
    assert any("leaderboard minimum volume" in blocker for blocker in report.blockers)
    assert any("leaderboard minimum account value" in blocker for blocker in report.blockers)
    assert any("leaderboard limit" in blocker for blocker in report.blockers)

    too_many = replace(base_config, leaderboard=LeaderboardConfig(limit=101))
    report = build_preflight_report(too_many)
    assert not report.passed
    assert "leaderboard limit cannot exceed 100" in report.blockers


def test_address_analytics_config_must_be_valid(base_config):
    config = replace(
        base_config,
        address_analytics=AddressAnalyticsConfig(
            url="file:///tmp/info.json",
            cache_ttl_ms=0,
            timeout_s=Decimal("0"),
            window_days=0,
            max_pages=0,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("address analytics URL" in blocker for blocker in report.blockers)
    assert any("address analytics cache TTL" in blocker for blocker in report.blockers)
    assert any("address analytics timeout" in blocker for blocker in report.blockers)
    assert any("address analytics window days" in blocker for blocker in report.blockers)
    assert any("address analytics max pages" in blocker for blocker in report.blockers)

    too_many = replace(base_config, address_analytics=AddressAnalyticsConfig(max_pages=31))
    report = build_preflight_report(too_many)
    assert not report.passed
    assert "address analytics max pages cannot exceed 30" in report.blockers


def test_auth_probe_interval_must_be_positive(base_config):
    config = replace(base_config, ops=OpsConfig(auth_probe_interval_ms=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("auth probe interval" in blocker for blocker in report.blockers)


def test_exchange_expires_after_window_must_be_positive_and_within_action_timeout(base_config):
    config = replace(base_config, ops=OpsConfig(exchange_expires_after_ms=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("expires-after window" in blocker for blocker in report.blockers)

    config = replace(
        base_config,
        ops=OpsConfig(
            exchange_action_timeout_s=Decimal("1"),
            exchange_expires_after_ms=2_000,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("expires-after window cannot exceed" in blocker for blocker in report.blockers)

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        ops=OpsConfig(exchange_expires_after_ms=1_999),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("expires-after window of at least 2000ms" in blocker for blocker in report.blockers)


def test_exchange_action_budget_reserves_room_for_cleanup(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        ops=OpsConfig(max_exchange_actions_per_minute=2),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("at least 3 exchange actions per minute" in blocker for blocker in report.blockers)


def test_dead_man_cancel_window_must_be_valid_for_exchange_modes(base_config):
    config = replace(base_config, ops=OpsConfig(dead_man_cancel_ms=-1))
    report = build_preflight_report(config)
    assert not report.passed
    assert any(
        "dead-man cancel window cannot be negative" in blocker for blocker in report.blockers
    )

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        ops=OpsConfig(dead_man_cancel_ms=5_000),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any(
        "dead-man cancel window must be at least 6000ms" in blocker for blocker in report.blockers
    )


def test_exchange_credentials_must_be_well_formed(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="not-a-key",
            vault_address="0x123",
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("api private key" in blocker and "32-byte" in blocker for blocker in report.blockers)
    assert any("vault address" in blocker for blocker in report.blockers)


def test_exchange_modes_reject_follower_equal_to_source(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=base_config.source_wallet,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert "follower account must not be the source wallet" in report.blockers


def test_exchange_modes_reject_api_signer_equal_to_source_wallet(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_wallet=PRIVATE_KEY_1_ADDRESS,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert "api signer wallet must not be the source wallet" in report.blockers


def test_exchange_modes_reject_follower_main_wallet_without_explicit_testnet_ack(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=PRIVATE_KEY_1_ADDRESS,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("HLCT_ALLOW_MASTER_PRIVATE_KEY" in blocker for blocker in report.blockers)


def test_testnet_can_explicitly_acknowledge_follower_main_wallet(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=PRIVATE_KEY_1_ADDRESS,
            api_private_key=PRIVATE_KEY_1,
            allow_master_private_key=True,
            testnet_enable=True,
        ),
    )

    report = build_preflight_report(config)

    assert report.passed
    assert any("trading-account private key" in warning for warning in report.warnings)


def test_exchange_modes_accept_expected_api_wallet_address(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_wallet_address=PRIVATE_KEY_1_ADDRESS,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert report.passed


def test_exchange_modes_reject_api_private_key_that_does_not_match_expected_wallet(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_wallet_address="0xf111111111111111111111111111111111111111",
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert "api private key does not match HLCT_API_WALLET_ADDRESS" in report.blockers


def test_exchange_modes_reject_vault_target_mismatch(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key=PRIVATE_KEY_1,
            vault_address="0xf111111111111111111111111111111111111111",
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any(
        "vault address must match follower account address" in blocker
        for blocker in report.blockers
    )


def test_exchange_modes_reject_api_signer_equal_to_vault_target(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=PRIVATE_KEY_1_ADDRESS,
            api_private_key=PRIVATE_KEY_1,
            vault_address=PRIVATE_KEY_1_ADDRESS,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any(
        "api signer wallet must not be the configured vault" in blocker
        for blocker in report.blockers
    )


def test_exchange_modes_reject_all_zero_private_key_placeholder(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "0" * 64,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert "api private key cannot be all zeroes" in report.blockers


def test_exchange_modes_reject_unloadable_private_key_scalar(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "f" * 64,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert not report.passed
    assert any("api private key could not be loaded" in blocker for blocker in report.blockers)


def test_gui_port_must_be_valid(base_config):
    config = replace(base_config, port=70000)
    report = build_preflight_report(config)
    assert not report.passed
    assert any("GUI port" in blocker for blocker in report.blockers)


def test_slippage_bps_must_be_in_safe_range(base_config):
    high = replace(base_config, risk=replace(base_config.risk, slippage_bps=Decimal("10000")))
    report = build_preflight_report(high)
    assert not report.passed
    assert any("slippage" in blocker for blocker in report.blockers)

    high_close = replace(
        base_config, risk=replace(base_config.risk, close_slippage_bps=Decimal("10000"))
    )
    report = build_preflight_report(high_close)
    assert not report.passed
    assert any("close slippage" in blocker for blocker in report.blockers)

    negative_close = replace(
        base_config, risk=replace(base_config.risk, close_slippage_bps=Decimal("-1"))
    )
    report = build_preflight_report(negative_close)
    assert not report.passed
    assert any("close slippage" in blocker for blocker in report.blockers)

    negative = replace(base_config, risk=replace(base_config.risk, slippage_bps=Decimal("-1")))
    report = build_preflight_report(negative)
    assert not report.passed
    assert any("slippage" in blocker for blocker in report.blockers)

    exchange_high_close = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, close_slippage_bps=Decimal("1000.01")),
    )
    report = build_preflight_report(exchange_high_close)
    assert not report.passed
    assert "exchange modes require close slippage bps at or below 1000" in report.blockers

    exchange_high_entry = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, slippage_bps=Decimal("1000.01")),
    )
    report = build_preflight_report(exchange_high_entry)
    assert not report.passed
    assert "exchange modes require entry slippage bps at or below 1000" in report.blockers

    invalid_envelope = replace(
        base_config,
        risk=replace(base_config.risk, hip3_oracle_envelope_bps=Decimal("0")),
    )
    report = build_preflight_report(invalid_envelope)
    assert not report.passed
    assert "HIP-3 oracle envelope bps must be above 0 and at or below 1000" in report.blockers


def test_preflight_rejects_nonfinite_decimals_without_raising(base_config):
    risk = replace(
        base_config.risk,
        fixed_multiplier=Decimal("NaN"),
        max_notional_usd=Decimal("Infinity"),
        close_slippage_bps=Decimal("-Infinity"),
    )
    config = replace(
        base_config,
        risk=risk,
        ops=replace(base_config.ops, exchange_action_timeout_s=Decimal("NaN")),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert any("HLCT_FIXED_MULTIPLIER must be a finite" in blocker for blocker in report.blockers)
    assert any("HLCT_MAX_NOTIONAL_USD must be a finite" in blocker for blocker in report.blockers)
    assert any("HLCT_CLOSE_SLIPPAGE_BPS must be a finite" in blocker for blocker in report.blockers)
    assert any(
        "HLCT_EXCHANGE_ACTION_TIMEOUT_S must be a finite" in blocker for blocker in report.blockers
    )


def test_stale_thresholds_must_be_positive(base_config):
    risk = replace(base_config.risk, stale_source_ms=0, stale_follower_ms=0)
    config = replace(base_config, risk=risk)
    report = build_preflight_report(config)
    assert not report.passed
    assert any("stale source" in blocker for blocker in report.blockers)
    assert any("stale follower" in blocker for blocker in report.blockers)


def test_max_open_positions_must_be_positive(base_config):
    config = replace(
        base_config,
        risk=replace(base_config.risk, max_open_positions=0),
    )

    report = build_preflight_report(config)

    assert report.passed is False
    assert "max open positions must be positive" in report.blockers


def test_preflight_warns_when_gross_cap_is_below_symbol_cap(base_config):
    risk = replace(
        base_config.risk,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("100"),
    )
    config = replace(base_config, risk=risk)
    report = build_preflight_report(config)
    assert report.passed
    assert any("gross exposure" in warning for warning in report.warnings)


def test_subaccount_assignments_require_valid_addresses(base_config):
    config = replace(
        base_config,
        subaccount_assignments=(
            SubaccountAssignment(
                slot="btc-copy",
                subaccount="not-address",
                source_wallet="0x1111111111111111111111111111111111111111",
            ),
        ),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert any("btc-copy subaccount" in blocker for blocker in report.blockers)


def test_enabled_subaccount_assignment_requires_verification(base_config):
    config = replace(
        base_config,
        subaccount_assignments=(
            SubaccountAssignment(
                slot="btc-copy",
                subaccount="0xf000000000000000000000000000000000000000",
                source_wallet="0x1111111111111111111111111111111111111111",
                enabled=True,
            ),
        ),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert any(
        "btc-copy is enabled but subaccount_verified is not true" in blocker
        for blocker in report.blockers
    )
    assert any("btc-copy is not verified" in warning for warning in report.warnings)


def test_subaccount_assignment_source_and_subaccount_must_differ(base_config):
    address = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        subaccount_assignments=(
            SubaccountAssignment(
                slot="self-copy",
                subaccount=address,
                source_wallet=address,
            ),
        ),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert any(
        "self-copy source wallet and subaccount must differ" in blocker
        for blocker in report.blockers
    )


def test_subaccount_assignment_mode_must_be_known(base_config):
    config = replace(
        base_config,
        subaccount_assignments=(
            SubaccountAssignment(
                slot="btc-copy",
                subaccount=FOLLOWER_ACCOUNT,
                source_wallet=ALT_FOLLOWER_ACCOUNT,
                mode="mainnet",
                subaccount_verified=True,
                operator_verified_at="2026-07-07T00:00:00Z",
            ),
        ),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert any("btc-copy mode must be one of" in blocker for blocker in report.blockers)


def test_exchange_mode_active_subaccount_assignment_must_match_runtime(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        subaccount_assignments=(
            SubaccountAssignment(
                slot="wrong-follower",
                subaccount=ALT_FOLLOWER_ACCOUNT,
                source_wallet=base_config.source_wallet,
                mode="testnet",
                enabled=True,
                subaccount_verified=True,
                operator_verified_at="2026-07-07T00:00:00Z",
            ),
        ),
    )

    report = build_preflight_report(config)
    status = active_subaccount_assignment_status(config)

    assert not report.passed
    assert status["status"] == "missing_match"
    assert any(
        "no verified enabled testnet subaccount assignment matches" in blocker
        for blocker in report.blockers
    )


def test_exchange_mode_active_subaccount_assignment_match_passes(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        subaccount_assignments=(
            SubaccountAssignment(
                slot="btc-copy",
                subaccount=FOLLOWER_ACCOUNT,
                source_wallet=base_config.source_wallet,
                mode="testnet",
                enabled=True,
                subaccount_verified=True,
                operator_verified_at="2026-07-07T00:00:00Z",
            ),
        ),
    )

    report = build_preflight_report(config)
    status = active_subaccount_assignment_status(config)

    assert report.passed
    assert status["status"] == "matched"
    assert status["matched_slot"] == "btc-copy"


def test_exchange_mode_active_subaccount_assignment_rejects_duplicate_match(base_config):
    assignment = SubaccountAssignment(
        slot="btc-copy",
        subaccount=FOLLOWER_ACCOUNT,
        source_wallet=base_config.source_wallet,
        mode="testnet",
        enabled=True,
        subaccount_verified=True,
        operator_verified_at="2026-07-07T00:00:00Z",
    )
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
        subaccount_assignments=(
            assignment,
            replace(assignment, slot="btc-copy-duplicate"),
        ),
    )

    report = build_preflight_report(config)
    status = active_subaccount_assignment_status(config)

    assert not report.passed
    assert status["status"] == "multiple_matches"
    assert any("multiple verified enabled testnet" in blocker for blocker in report.blockers)


def test_equity_ratio_is_warning_only_for_shadow_and_paper(base_config):
    config = replace(base_config, risk=replace(base_config.risk, equity_ratio=Decimal("0.5")))
    report = build_preflight_report(config)
    assert report.passed
    assert any("explicit scale override" in warning for warning in report.warnings)

    paper = replace(config, mode=Mode.PAPER)
    report = build_preflight_report(paper)
    assert report.passed
    assert any("explicit scale override" in warning for warning in report.warnings)


def test_exchange_modes_allow_equity_ratio_as_explicit_balance_sizing_override(base_config):
    risk = replace(base_config.risk, equity_ratio=Decimal("0.5"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=risk,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    report = build_preflight_report(config)
    assert report.passed
    assert any("accountValue balance sizing" in warning for warning in report.warnings)


def test_testnet_can_observe_mainnet_source_read_only(base_config):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            testnet_enable=True,
        ),
    )

    report = build_preflight_report(config)

    assert config.rest_url == TESTNET_REST
    assert config.source_rest_url == MAINNET_REST
    assert report.passed
    assert any("observing the mainnet source" in warning for warning in report.warnings)


def test_live_blocks_testnet_source_network(base_config):
    config = replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER_ACCOUNT,
            api_private_key=PRIVATE_KEY_1,
            live_enable=True,
            confirm_mainnet_live=True,
        ),
    )

    report = build_preflight_report(config)

    assert not report.passed
    assert "live mode requires HLCT_SOURCE_NETWORK=mode or mainnet" in report.blockers


def test_load_config_records_malformed_env_values_as_preflight_blockers(monkeypatch):
    monkeypatch.setenv("HLCT_MODE", "not-a-mode")
    monkeypatch.setenv("HLCT_SOURCE_NETWORK", "not-a-network")
    monkeypatch.setenv("HLCT_SOURCE_DEX_SCOPE", "not-a-scope")
    monkeypatch.setenv("HLCT_MAX_NOTIONAL_USD", "not-a-decimal")
    monkeypatch.setenv("HLCT_PORT", "not-an-int")
    monkeypatch.setenv("HLCT_TESTNET_ENABLE", "definitely")
    monkeypatch.setenv("HLCT_DASHBOARD_SECURITY_AUDIT_TTL_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_REACTION_QUEUE_SIZE", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_WEBSOCKET_IDLE_TIMEOUT_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_WEBSOCKET_HEARTBEAT_TIMEOUT_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_WEBSOCKET_RECONNECT_ATTEMPTS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_WEBSOCKET_RECONNECT_BACKOFF_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_FILL_BACKFILL_LOOKBACK_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_FILL_BACKFILL_OVERLAP_MS", "not-an-int")
    monkeypatch.setenv("HLCT_SOURCE_FILL_BACKFILL_MAX_PAGES", "not-an-int")
    monkeypatch.setenv("HLCT_MAX_OPEN_POSITIONS", "not-an-int")
    monkeypatch.setenv("HLCT_EXPECTED_ACCOUNT_MODE", "not-an-account-mode")
    config = load_config()
    assert config.mode == Mode.SHADOW
    report = build_preflight_report(config)
    assert not report.passed
    assert "HLCT_MODE must be one of: shadow, paper, testnet, live" in report.blockers
    assert "HLCT_SOURCE_NETWORK must be one of: mode, mainnet, testnet" in report.blockers
    assert (
        "HLCT_SOURCE_DEX_SCOPE must be one of: strict, default_only_account_equity, "
        "all_configured_markets" in report.blockers
    )
    assert "HLCT_SOURCE_WALLET is required" in report.blockers
    assert "HLCT_MAX_NOTIONAL_USD must be a decimal value" in report.blockers
    assert "HLCT_PORT must be an integer value" in report.blockers
    assert "HLCT_DASHBOARD_SECURITY_AUDIT_TTL_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_REACTION_QUEUE_SIZE must be an integer value" in report.blockers
    assert "HLCT_SOURCE_WEBSOCKET_IDLE_TIMEOUT_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_WEBSOCKET_HEARTBEAT_TIMEOUT_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_WEBSOCKET_RECONNECT_ATTEMPTS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_WEBSOCKET_RECONNECT_BACKOFF_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_FILL_BACKFILL_LOOKBACK_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_FILL_BACKFILL_OVERLAP_MS must be an integer value" in report.blockers
    assert "HLCT_SOURCE_FILL_BACKFILL_MAX_PAGES must be an integer value" in report.blockers
    assert "HLCT_MAX_OPEN_POSITIONS must be an integer value" in report.blockers
    assert "HLCT_EXPECTED_ACCOUNT_MODE must be one of: auto, standard, unified" in report.blockers
    assert not any("HLCT_TESTNET_ENABLE" in blocker for blocker in report.blockers)


def test_load_config_parses_expected_account_modes_and_legacy_perp_alias(monkeypatch):
    set_source_wallet(monkeypatch)
    for raw, expected in (
        ("auto", AccountMode.AUTO),
        ("standard", AccountMode.STANDARD),
        ("unified", AccountMode.UNIFIED),
        ("perp", AccountMode.STANDARD),
    ):
        monkeypatch.setenv("HLCT_EXPECTED_ACCOUNT_MODE", raw)
        assert load_config().exchange.expected_account_mode == expected


def test_load_config_parses_source_dex_scope(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_SOURCE_DEX_SCOPE", "default_only_account_equity")
    monkeypatch.setenv("HLCT_SIZING_EQUITY_CAP_USD", "50")

    config = load_config()

    assert config.source_dex_scope == SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY
    assert build_preflight_report(config).passed is True


def test_load_config_parses_real_max_open_positions_cap(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_MAX_OPEN_POSITIONS", "4")

    config = load_config()

    assert config.risk.max_open_positions == 4
    assert build_preflight_report(config).passed is True


def test_load_config_binds_validation_effective_configuration_hashes(monkeypatch, tmp_path):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_VALIDATION_SUPERVISOR_LEASE_PATH", str(tmp_path / "lease.json"))
    monkeypatch.setenv(
        "HLCT_VALIDATION_CONTROLLER_REGISTRY_PATH", str(tmp_path / "controllers.sqlite3")
    )
    monkeypatch.setenv("HLCT_VALIDATION_RUN_ID", "validation-run")
    monkeypatch.setenv("HLCT_VALIDATION_OWNER_TOKEN", "opaque-owner-token")
    monkeypatch.setenv("HLCT_VALIDATION_SUPERVISOR_INCARNATION_ID", "incarnation-1")
    monkeypatch.setenv(
        "HLCT_VALIDATION_FOLLOWER_SET_JSON",
        '["0x1111111111111111111111111111111111111111",'
        '"0x2222222222222222222222222222222222222222"]',
    )
    monkeypatch.setenv("HLCT_VALIDATION_STATE_IDENTITY_SHA256", "a" * 64)
    monkeypatch.setenv("HLCT_VALIDATION_EFFECTIVE_CONFIG_SHA256", "b" * 64)
    monkeypatch.setenv("HLCT_VALIDATION_EFFECTIVE_CONFIG_SET_SHA256", "c" * 64)
    monkeypatch.setenv("HLCT_VALIDATION_DEADLINE_MS", "123456789")

    config = load_config()

    assert config.ops.validation_effective_config_sha256 == "b" * 64
    assert config.ops.validation_effective_config_set_sha256 == "c" * 64
    assert config.ops.validation_supervisor_incarnation_id == "incarnation-1"
    assert config.ops.validation_follower_set == (
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    )
    assert config.config_errors == ()


def test_load_config_rejects_malformed_validation_effective_configuration_hash(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_VALIDATION_EFFECTIVE_CONFIG_SHA256", "not-a-sha256")

    config = load_config()

    assert any(
        "HLCT_VALIDATION_EFFECTIVE_CONFIG_SHA256 must be 64 lowercase hex characters" in item
        for item in config.config_errors
    )


def test_load_config_canonicalizes_hip3_allowlist(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_SOURCE_DEX_SCOPE", "all_configured_markets")
    monkeypatch.setenv("HLCT_ALLOWED_SYMBOLS", "btc,xyz:aapl,xyz:AAPL")

    config = load_config()

    assert config.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
    assert config.risk.allowed_symbols == ("BTC", "xyz:AAPL")
    assert build_preflight_report(config).passed is True


def test_load_config_records_nonfinite_env_values_as_preflight_blockers(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_FIXED_MULTIPLIER", "NaN")
    monkeypatch.setenv("HLCT_MAX_NOTIONAL_USD", "Infinity")

    report = build_preflight_report(load_config())

    assert not report.passed
    assert "HLCT_FIXED_MULTIPLIER must be a finite decimal value" in report.blockers
    assert "HLCT_MAX_NOTIONAL_USD must be a finite decimal value" in report.blockers


def test_load_config_parses_and_rejects_invalid_sizing_equity_caps(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_SIZING_EQUITY_CAP_USD", "50")

    config = load_config()

    assert config.risk.sizing_equity_cap_usd == Decimal("50")
    assert build_preflight_report(config).passed

    for invalid in ("0", "-1", "NaN", "Infinity", "not-a-number"):
        monkeypatch.setenv("HLCT_SIZING_EQUITY_CAP_USD", invalid)
        report = build_preflight_report(load_config())
        assert not report.passed
        assert (
            "HLCT_SIZING_EQUITY_CAP_USD must be a finite positive decimal value" in report.blockers
        )


def test_preflight_rejects_invalid_direct_sizing_equity_cap(base_config):
    for invalid in (Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")):
        config = replace(
            base_config,
            risk=replace(base_config.risk, sizing_equity_cap_usd=invalid),
        )

        report = build_preflight_report(config)

        assert not report.passed
        assert (
            "HLCT_SIZING_EQUITY_CAP_USD must be a finite positive decimal value" in report.blockers
        )


def test_load_config_requires_explicit_source_wallet(monkeypatch):
    monkeypatch.delenv("HLCT_SOURCE_WALLET", raising=False)

    config = load_config()
    report = build_preflight_report(config)

    assert config.source_wallet == ""
    assert not report.passed
    assert "HLCT_SOURCE_WALLET is required" in report.blockers


def test_load_config_parses_source_network(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv("HLCT_MODE", "testnet")
    monkeypatch.setenv("HLCT_SOURCE_NETWORK", "mainnet")

    config = load_config()

    assert config.source_network == SourceNetwork.MAINNET
    assert config.resolved_source_network == SourceNetwork.MAINNET
    assert config.rest_url == TESTNET_REST
    assert config.source_rest_url == MAINNET_REST


def test_load_config_parses_subaccount_assignment_json(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON",
        (
            '[{"slot":"btc-copy","subaccount":"0xf000000000000000000000000000000000000000",'
            '"source_wallet":"0x1111111111111111111111111111111111111111",'
            '"mode":"testnet","enabled":true,"subaccount_verified":true,'
            '"operator_verified_at":"2026-07-07T00:00:00Z","note":"manual BTC source"}]'
        ),
    )

    config = load_config()

    assert config.subaccount_assignments[0].slot == "btc-copy"
    assert (
        config.subaccount_assignments[0].subaccount == "0xf000000000000000000000000000000000000000"
    )
    assert (
        config.subaccount_assignments[0].source_wallet
        == "0x1111111111111111111111111111111111111111"
    )
    assert config.subaccount_assignments[0].enabled is True
    assert config.subaccount_assignments[0].subaccount_verified is True
    assert config.subaccount_assignments[0].operator_verified_at == "2026-07-07T00:00:00Z"
    assert build_preflight_report(config).passed


def test_load_config_parses_assignment_boolean_strings(monkeypatch):
    set_source_wallet(monkeypatch)
    monkeypatch.setenv(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON",
        (
            '[{"slot":"btc-copy","subaccount":"0xf000000000000000000000000000000000000000",'
            '"source_wallet":"0x1111111111111111111111111111111111111111",'
            '"enabled":"false","subaccount_verified":"false"}]'
        ),
    )

    config = load_config()

    assert config.subaccount_assignments[0].enabled is False
    assert config.subaccount_assignments[0].subaccount_verified is False
    report = build_preflight_report(config)
    assert report.passed
    assert any("btc-copy is not verified" in warning for warning in report.warnings)


def test_malformed_subaccount_assignment_json_is_preflight_blocker(monkeypatch):
    monkeypatch.setenv("HLCT_SUBACCOUNT_ASSIGNMENTS_JSON", "{not json")

    config = load_config()
    report = build_preflight_report(config)

    assert not report.passed
    assert any(
        "HLCT_SUBACCOUNT_ASSIGNMENTS_JSON must be valid JSON" in blocker
        for blocker in report.blockers
    )


def test_load_config_reads_api_private_key_from_file(monkeypatch, tmp_path):
    set_source_wallet(monkeypatch)
    key_file = tmp_path / "api-key.txt"
    key_file.write_text(PRIVATE_KEY_1 + "\n", encoding="utf-8")
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY_FILE", str(key_file))

    config = load_config()

    assert config.exchange.api_private_key == PRIVATE_KEY_1
    assert config.exchange.api_private_key_file == str(key_file)
    assert config.config_errors == ()


def test_direct_api_private_key_and_file_are_mutually_exclusive(monkeypatch, tmp_path):
    set_source_wallet(monkeypatch)
    key_file = tmp_path / "api-key.txt"
    key_file.write_text("not-a-key", encoding="utf-8")
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY", PRIVATE_KEY_1)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY_FILE", str(key_file))

    config = load_config()

    assert config.exchange.api_private_key == PRIVATE_KEY_1
    assert config.exchange.api_private_key_file == str(key_file)
    assert "HLCT_API_PRIVATE_KEY and HLCT_API_PRIVATE_KEY_FILE are mutually exclusive" in (
        config.config_errors
    )


def test_missing_api_private_key_file_is_preflight_blocker(monkeypatch, tmp_path):
    monkeypatch.delenv("HLCT_API_PRIVATE_KEY", raising=False)
    monkeypatch.setenv("HLCT_API_PRIVATE_KEY_FILE", str(tmp_path / "missing.txt"))

    config = load_config()
    report = build_preflight_report(config)

    assert not report.passed
    assert any(
        "HLCT_API_PRIVATE_KEY_FILE could not be read" in blocker for blocker in report.blockers
    )
