from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from .config import AccountMode, AppConfig, SourceNetwork
from .markets import canonical_market_symbol
from .models import Mode


MAINNET_CANARY_ACKNOWLEDGEMENT = "I_UNDERSTAND_THIS_SENDS_ONE_MAINNET_ORDER"
MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT = (
    "I_UNDERSTAND_THIS_SENDS_ONE_MAINNET_ENTRY_AND_UP_TO_THREE_REDUCE_ONLY_CLOSE_ORDERS"
)
MAINNET_CANARY_MIN_NOTIONAL_USD = Decimal("12")
MAINNET_CANARY_MAX_NOTIONAL_USD = Decimal("15")
MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD = Decimal("15")
MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD = Decimal("55")
MAINNET_CANARY_MIN_DEAD_MAN_MS = 60_000
MAINNET_CANARY_MAX_DEAD_MAN_MS = 120_000
MAINNET_CANARY_SYMBOLS = frozenset({"BTC", "ETH"})


def build_mainnet_canary_profile(config: AppConfig, *, coin: str) -> dict[str, Any]:
    """Validate the deliberately narrow first-mainnet-canary configuration.

    This is stricter than ordinary live preflight. It describes a single isolated
    passive placement/cancel experiment, never a production or multi-slot rollout.
    """

    blockers: list[str] = []
    warnings = [
        "candidate scope is one isolated account and one passive order; it is not production readiness",
        "same-IP multi-slot live rollout remains blocked until WebSocket user/connection budgets are solved",
        "the live GUI and integrated console remain read-only/disabled for signed controls",
    ]
    try:
        symbol = canonical_market_symbol(coin)
    except ValueError as exc:
        symbol = str(coin).strip()
        blockers.append(f"mainnet canary coin is invalid: {exc}")

    if config.mode != Mode.LIVE:
        blockers.append("mainnet canary requires HLCT_MODE=live")
    if config.resolved_source_network != SourceNetwork.MAINNET:
        blockers.append("mainnet canary requires a mainnet source network")
    if symbol not in MAINNET_CANARY_SYMBOLS:
        blockers.append("first mainnet canary coin must be BTC or ETH")

    allowed = tuple(canonical_market_symbol(item) for item in config.risk.allowed_symbols)
    if allowed != (symbol,):
        blockers.append(
            f"first mainnet canary requires HLCT_ALLOWED_SYMBOLS={symbol} with no other markets"
        )
    if not (
        MAINNET_CANARY_MIN_NOTIONAL_USD
        <= config.risk.max_notional_usd
        <= MAINNET_CANARY_MAX_NOTIONAL_USD
    ):
        blockers.append("first mainnet canary requires HLCT_MAX_NOTIONAL_USD between $12 and $15")
    if not (
        config.risk.max_notional_usd
        <= config.risk.max_gross_exposure_usd
        <= MAINNET_CANARY_MAX_NOTIONAL_USD
    ):
        blockers.append(
            "first mainnet canary requires gross exposure at least max notional and at most $15"
        )
    if config.risk.max_leverage != 1:
        blockers.append("first mainnet canary requires HLCT_MAX_LEVERAGE=1")
    if config.ops.max_new_intents_per_cycle != 1:
        blockers.append("first mainnet canary requires HLCT_MAX_NEW_INTENTS_PER_CYCLE=1")
    if config.ops.max_open_intents != 1:
        blockers.append("first mainnet canary requires HLCT_MAX_OPEN_INTENTS=1")
    if not 6 <= config.ops.max_exchange_actions_per_minute <= 12:
        blockers.append(
            "first mainnet canary requires 6-12 exchange actions per minute for bounded cleanup"
        )
    if config.ops.circuit_breaker_failure_threshold != 1:
        blockers.append("first mainnet canary requires circuit breaker failure threshold 1")
    if not (
        MAINNET_CANARY_MIN_DEAD_MAN_MS
        <= config.ops.dead_man_cancel_ms
        <= MAINNET_CANARY_MAX_DEAD_MAN_MS
    ):
        blockers.append("first mainnet canary requires a dead-man window between 60s and 120s")
    if config.ops.exchange_action_timeout_s > Decimal("15"):
        blockers.append("first mainnet canary exchange action timeout cannot exceed 15s")
    if not 2_000 <= config.ops.exchange_expires_after_ms <= 10_000:
        blockers.append("first mainnet canary expires-after window must be 2-10 seconds")

    if config.exchange.expected_account_mode == AccountMode.AUTO:
        blockers.append(
            "first mainnet canary requires explicit HLCT_EXPECTED_ACCOUNT_MODE=standard or unified"
        )
    if not config.exchange.api_private_key_file:
        blockers.append("first mainnet canary requires HLCT_API_PRIVATE_KEY_FILE")
    if not config.exchange.vault_address:
        blockers.append("first mainnet canary requires a dedicated vault/subaccount address")
    elif config.exchange.vault_address.lower() != config.exchange.follower_account_address.lower():
        blockers.append("mainnet canary vault/subaccount must match the follower action account")
    if config.exchange.allow_master_private_key:
        blockers.append("mainnet canary forbids the trading-account owner private key")

    if not _same_parent(config.db_path, config.ops.kill_switch_path):
        blockers.append(
            "mainnet canary journal and kill switch must use one dedicated state directory"
        )
    if config.db_path.name.lower() == "copytrader.sqlite3" and _looks_like_default_data_path(
        config.db_path
    ):
        blockers.append("mainnet canary cannot use the default data/copytrader.sqlite3 journal")

    return {
        "profile_version": 2,
        "scope": "single_account_passive_mainnet_canary",
        "passed": not blockers,
        "coin": symbol,
        "blockers": blockers,
        "warnings": warnings,
        "limits": {
            "notional_usd_min": str(MAINNET_CANARY_MIN_NOTIONAL_USD),
            "notional_usd_max": str(MAINNET_CANARY_MAX_NOTIONAL_USD),
            "gross_exposure_usd_max": str(MAINNET_CANARY_MAX_NOTIONAL_USD),
            "account_value_usd_min": str(MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD),
            "account_value_usd_max": str(MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD),
            "max_leverage": 1,
            "max_new_intents": 1,
            "max_open_intents": 1,
            "dead_man_ms_min": MAINNET_CANARY_MIN_DEAD_MAN_MS,
            "dead_man_ms_max": MAINNET_CANARY_MAX_DEAD_MAN_MS,
        },
        "dead_man_policy": config.ops.dead_man_policy.value,
        "acknowledgement": MAINNET_CANARY_ACKNOWLEDGEMENT,
        "fleet_mainnet_ready": False,
    }


def _same_parent(left: Path, right: Path) -> bool:
    try:
        return left.resolve().parent == right.resolve().parent
    except OSError:
        return left.absolute().parent == right.absolute().parent


def _looks_like_default_data_path(path: Path) -> bool:
    parts = tuple(part.lower() for part in path.parts)
    return len(parts) >= 2 and parts[-2:] == ("data", "copytrader.sqlite3")
