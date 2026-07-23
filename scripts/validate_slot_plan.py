from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from hyperliquid_copytrader.markets import MarketIdentityError, canonical_market


SLOT_PLAN_VALIDATOR_VERSION = 1
OFFICIAL_MIN_NOTIONAL_USD = Decimal("10")
MAX_REDUCE_ONLY_SLIPPAGE_BPS = Decimal("1000")
DEFAULT_EXCHANGE_ACTION_TIMEOUT_S = Decimal("15")
MAX_SUPERVISOR_EXCHANGE_ACTION_TIMEOUT_S = Decimal("28")
OFFICIAL_MIN_NOTIONAL_SOURCE = (
    "Hyperliquid official Error responses docs: MinTradeNtl says orders must have minimum "
    "value of $10."
)
OFFICIAL_MIN_NOTIONAL_SOURCE_URL = (
    "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
)

ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
SAFE_ENVIRONMENTS = {"analysis", "paper", "replay", "shadow", "testnet"}
UNSAFE_ENVIRONMENTS = {"live", "mainnet"}
SIZING_POLICIES = {"fixed_risk_budget", "pure_compound"}
DUST_MODES = {"accumulate", "block", "skip"}
MARGIN_MODES = {"cross", "isolated"}
ACCOUNT_MODES = {"standard", "unified"}
EQUITY_CONFIDENCE_POLICIES = {"block_low", "degrade_low"}
SOURCE_DEX_SCOPES = {"strict", "default_only_account_equity", "all_configured_markets"}
OPERATIONAL_STATUSES = {"active", "standby", "retired_quarantined"}
SECRET_KEY_FRAGMENTS = (
    "api_private_key",
    "private_key",
    "secret",
    "seed",
    "mnemonic",
)


class SlotPlanInputError(RuntimeError):
    """Raised when a slot plan cannot be loaded."""


def validate_slot_plan(path: Path) -> dict[str, Any]:
    payload = read_json(path)
    return validate_slot_plan_payload(payload, source=str(path))


def validate_slot_plan_payload(payload: Any, *, source: str = "memory") -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    normalized_slots: list[dict[str, Any]] = []

    if not isinstance(payload, dict):
        raise SlotPlanInputError("slot plan must be a JSON object")

    secret_paths = list(find_secret_fields(payload))
    for field_path in secret_paths:
        blockers.append(f"{field_path} must not be present in a slot plan")

    version = payload.get("version")
    if version != SLOT_PLAN_VALIDATOR_VERSION:
        blockers.append(f"version must be {SLOT_PLAN_VALIDATOR_VERSION}")

    environment = clean(payload.get("environment")).lower()
    if environment == "unknown":
        blockers.append("environment is required")
    elif environment in UNSAFE_ENVIRONMENTS:
        blockers.append(f"environment {environment!r} is blocked for slot plans in this workflow")
    elif environment not in SAFE_ENVIRONMENTS:
        blockers.append(
            "environment must be one of: "
            + ", ".join(sorted(SAFE_ENVIRONMENTS | UNSAFE_ENVIRONMENTS))
        )
    elif environment == "testnet":
        warnings.append(
            "testnet slot plans are validation-only here; execution still requires fresh operator approval"
        )

    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, list):
        blockers.append("slots must be a JSON array")
        raw_slots = []
    if not raw_slots:
        blockers.append("slots must contain at least one slot")

    seen_slots: dict[str, int] = {}
    seen_sources: dict[str, int] = {}
    seen_subaccounts: dict[str, int] = {}

    for index, raw_slot in enumerate(raw_slots):
        label = f"slots[{index}]"
        if not isinstance(raw_slot, dict):
            blockers.append(f"{label} must be an object")
            continue
        normalized = validate_slot(
            raw_slot,
            index=index,
            environment=environment,
            blockers=blockers,
            warnings=warnings,
        )
        normalized_slots.append(normalized)
        record_duplicate(
            seen_slots,
            normalized["slot"],
            index,
            field="slot",
            blockers=blockers,
        )
        record_duplicate(
            seen_sources,
            normalized["source_address"],
            index,
            field="source_address",
            blockers=blockers,
        )
        record_duplicate(
            seen_subaccounts,
            normalized["subaccount_address"],
            index,
            field="subaccount_address",
            blockers=blockers,
        )

    sizing_counts = Counter(slot["sizing_policy"] for slot in normalized_slots)
    environment_counts = Counter(slot["mode"] for slot in normalized_slots)
    account_mode_counts = Counter(slot["expected_account_mode"] for slot in normalized_slots)
    operational_status_counts = Counter(slot["operational_status"] for slot in normalized_slots)
    enabled_slots = [slot for slot in normalized_slots if slot["enabled"]]
    verified_slots = [slot for slot in normalized_slots if slot["subaccount_verified"]]
    plan_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return {
        "slot_plan_validator_version": SLOT_PLAN_VALIDATOR_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "source": source,
        "plan_hash": plan_hash,
        "valid": not blockers,
        "blockers": blockers,
        "warnings": warnings,
        "environment": environment,
        "counts": {
            "slots": len(normalized_slots),
            "enabled_slots": len(enabled_slots),
            "subaccount_verified_slots": len(verified_slots),
            "unverified_slots": len(normalized_slots) - len(verified_slots),
            "sizing_policies": counter_dict(sizing_counts),
            "slot_modes": counter_dict(environment_counts),
            "account_modes": counter_dict(account_mode_counts),
            "operational_statuses": counter_dict(operational_status_counts),
            "retired_quarantined_slots": operational_status_counts["retired_quarantined"],
            "secret_field_count": len(secret_paths),
        },
        "min_notional_reference": {
            "perp_min_notional_usd": decimal_str(OFFICIAL_MIN_NOTIONAL_USD),
            "source": OFFICIAL_MIN_NOTIONAL_SOURCE,
            "url": OFFICIAL_MIN_NOTIONAL_SOURCE_URL,
        },
        "slots": normalized_slots,
    }


def validate_slot(
    raw: dict[str, Any],
    *,
    index: int,
    environment: str,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    label = slot_label(raw, index)
    slot = required_text(raw, "slot", label=label, blockers=blockers)
    source_address = required_address(raw, "source_address", label=label, blockers=blockers)
    subaccount_address = required_address(raw, "subaccount_address", label=label, blockers=blockers)
    if source_address != "unknown" and source_address == subaccount_address:
        blockers.append(f"{label} source_address and subaccount_address must be different")

    mode = clean(raw.get("mode") or environment).lower()
    if mode in UNSAFE_ENVIRONMENTS:
        blockers.append(f"{label} mode {mode!r} is blocked for slot plans in this workflow")
    elif mode not in SAFE_ENVIRONMENTS and mode != "unknown":
        blockers.append(f"{label} mode must be one of: {', '.join(sorted(SAFE_ENVIRONMENTS))}")

    enabled = bool_value(raw.get("enabled", False))
    operational_status = clean(raw.get("operational_status")).lower()
    if operational_status == "unknown":
        operational_status = "active" if enabled else "standby"
    if operational_status not in OPERATIONAL_STATUSES:
        blockers.append(
            f"{label} operational_status must be one of: " + ", ".join(sorted(OPERATIONAL_STATUSES))
        )
    if enabled and operational_status != "active":
        blockers.append(f"{label} enabled slots must have operational_status=active")
    if not enabled and operational_status == "active":
        blockers.append(f"{label} operational_status=active requires enabled=true")
    subaccount_verified = bool_value(raw.get("subaccount_verified", False))
    operator_verified_at = none_if_unknown(raw.get("operator_verified_at"))
    if enabled and not subaccount_verified:
        blockers.append(f"{label} subaccount_verified must be true before an enabled slot")
    if not subaccount_verified:
        warnings.append(f"{label} subaccount is not verified through UI/API evidence")
    if enabled and mode == "testnet" and operator_verified_at is None:
        blockers.append(f"{label} operator_verified_at is required for an enabled testnet slot")
    elif operator_verified_at is not None and not is_timezone_aware_timestamp(operator_verified_at):
        blockers.append(f"{label} operator_verified_at must be a timezone-aware ISO-8601 timestamp")
    elif subaccount_verified and operator_verified_at is None:
        warnings.append(
            f"{label} subaccount_verified is true without operator_verified_at evidence"
        )

    retirement_reason = none_if_unknown(raw.get("retirement_reason"))
    retired_at = none_if_unknown(raw.get("retired_at"))
    retirement_evidence = none_if_unknown(raw.get("retirement_evidence"))
    known_residual_positions = residual_position_map(
        raw.get("known_residual_positions"),
        label=f"{label} known_residual_positions",
        blockers=blockers,
        required=operational_status == "retired_quarantined",
    )
    retirement_open_orders = nonnegative_integer(
        raw,
        "retirement_open_orders",
        label=label,
        blockers=blockers,
        required=operational_status == "retired_quarantined",
    )
    retirement_fields_present = any(
        key in raw
        for key in (
            "retirement_reason",
            "retired_at",
            "retirement_evidence",
            "known_residual_positions",
            "retirement_open_orders",
        )
    )
    if operational_status == "retired_quarantined":
        if enabled:
            blockers.append(f"{label} retired_quarantined slots must be disabled")
        if not subaccount_verified:
            blockers.append(
                f"{label} retired_quarantined slots require a verified account identity"
            )
        if retirement_reason is None:
            blockers.append(f"{label} retirement_reason is required for a retired account")
        if retired_at is None:
            blockers.append(f"{label} retired_at is required for a retired account")
        elif not is_timezone_aware_timestamp(retired_at):
            blockers.append(f"{label} retired_at must be a timezone-aware ISO-8601 timestamp")
        if retirement_evidence is None:
            blockers.append(f"{label} retirement_evidence is required for a retired account")
        warnings.append(
            f"{label} is permanently excluded from execution and fleet readiness as retired_quarantined"
        )
    elif retirement_fields_present:
        blockers.append(
            f"{label} retirement metadata requires operational_status=retired_quarantined"
        )

    sizing_policy = clean(raw.get("sizing_policy")).lower()
    if sizing_policy not in SIZING_POLICIES:
        blockers.append(
            f"{label} sizing_policy must be one of: {', '.join(sorted(SIZING_POLICIES))}"
        )

    initial_budget_usd = positive_decimal(
        raw,
        "initial_budget_usd",
        label=label,
        blockers=blockers,
        required=True,
    )
    fixed_risk_budget_usd = positive_decimal(
        raw,
        "fixed_risk_budget_usd",
        label=label,
        blockers=blockers,
        required=sizing_policy == "fixed_risk_budget",
    )
    fixed_multiplier = positive_decimal(
        raw,
        "fixed_multiplier",
        label=label,
        blockers=blockers,
        required=False,
    )
    if fixed_multiplier is None:
        fixed_multiplier = Decimal("1")
    if sizing_policy == "pure_compound" and fixed_risk_budget_usd is not None:
        warnings.append(f"{label} fixed_risk_budget_usd is ignored by pure_compound sizing")
    if (
        sizing_policy == "fixed_risk_budget"
        and initial_budget_usd is not None
        and fixed_risk_budget_usd is not None
        and fixed_risk_budget_usd > initial_budget_usd
    ):
        warnings.append(f"{label} fixed_risk_budget_usd exceeds initial_budget_usd")

    allowed_coins = coin_list(
        raw.get("allowed_coins"), label=f"{label} allowed_coins", blockers=blockers
    )
    denied_coins = coin_list(
        raw.get("denied_coins"), label=f"{label} denied_coins", blockers=blockers
    )
    if not allowed_coins and not denied_coins:
        blockers.append(f"{label} must define either allowed_coins or denied_coins")
    if allowed_coins and denied_coins:
        blockers.append(f"{label} must not define both allowed_coins and denied_coins")

    min_notional_usd = positive_decimal(
        raw,
        "min_notional_usd",
        label=label,
        blockers=blockers,
        required=True,
    )
    if min_notional_usd is not None and min_notional_usd < OFFICIAL_MIN_NOTIONAL_USD:
        blockers.append(
            f"{label} min_notional_usd must be at least {decimal_str(OFFICIAL_MIN_NOTIONAL_USD)}"
        )

    dust_policy = validate_dust_policy(raw.get("dust_policy"), label=label, blockers=blockers)
    entry_slippage_bps = slippage_decimal(
        raw,
        "entry_slippage_bps",
        label=label,
        blockers=blockers,
    )
    reduce_only_slippage_bps = slippage_decimal(
        raw,
        "reduce_only_slippage_bps",
        label=label,
        blockers=blockers,
    )
    if (
        reduce_only_slippage_bps is not None
        and reduce_only_slippage_bps > MAX_REDUCE_ONLY_SLIPPAGE_BPS
    ):
        blockers.append(
            f"{label} reduce_only_slippage_bps cannot exceed "
            f"{decimal_str(MAX_REDUCE_ONLY_SLIPPAGE_BPS)}"
        )
    if (
        entry_slippage_bps is not None
        and reduce_only_slippage_bps is not None
        and reduce_only_slippage_bps < entry_slippage_bps
    ):
        warnings.append(f"{label} reduce_only_slippage_bps is tighter than entry_slippage_bps")

    max_emergency_leverage = positive_decimal(
        raw,
        "max_emergency_leverage",
        label=label,
        blockers=blockers,
        required=True,
    )
    max_gross_notional_usd = positive_decimal(
        raw,
        "max_gross_notional_usd",
        label=label,
        blockers=blockers,
        required=True,
    )
    exchange_action_timeout_s = positive_decimal(
        raw,
        "exchange_action_timeout_s",
        label=label,
        blockers=blockers,
        required=False,
    )
    if exchange_action_timeout_s is None:
        exchange_action_timeout_s = DEFAULT_EXCHANGE_ACTION_TIMEOUT_S
    if exchange_action_timeout_s <= Decimal("10"):
        blockers.append(
            f"{label} exchange_action_timeout_s must exceed the supervisor's 10-second signed expiry"
        )
    if exchange_action_timeout_s > MAX_SUPERVISOR_EXCHANGE_ACTION_TIMEOUT_S:
        blockers.append(
            f"{label} exchange_action_timeout_s must be at most "
            f"{decimal_str(MAX_SUPERVISOR_EXCHANGE_ACTION_TIMEOUT_S)} so the 30-second dead-man remains later"
        )

    expected_margin_mode = clean(raw.get("expected_margin_mode")).lower()
    if expected_margin_mode not in MARGIN_MODES:
        blockers.append(f"{label} expected_margin_mode must be one of: cross, isolated")
    expected_account_mode = clean(raw.get("expected_account_mode")).lower()
    if expected_account_mode == "perp":
        warnings.append(f"{label} expected_account_mode=perp is deprecated; normalized to standard")
        expected_account_mode = "standard"
    if expected_account_mode not in ACCOUNT_MODES:
        blockers.append(f"{label} expected_account_mode must be one of: standard, unified")
    equity_confidence_policy = clean(raw.get("equity_confidence_policy")).lower()
    if equity_confidence_policy not in EQUITY_CONFIDENCE_POLICIES:
        blockers.append(
            f"{label} equity_confidence_policy must be one of: "
            + ", ".join(sorted(EQUITY_CONFIDENCE_POLICIES))
        )
    source_dex_scope = clean(raw.get("source_dex_scope") or "strict").lower()
    if source_dex_scope not in SOURCE_DEX_SCOPES:
        blockers.append(
            f"{label} source_dex_scope must be one of: " + ", ".join(sorted(SOURCE_DEX_SCOPES))
        )
    elif source_dex_scope == "default_only_account_equity":
        if sizing_policy != "fixed_risk_budget" or fixed_risk_budget_usd is None:
            blockers.append(
                f"{label} default_only_account_equity requires fixed_risk_budget sizing"
            )
        warnings.append(
            f"{label} excludes non-default source DEX positions and sizes default-perp "
            "positions against total shared Unified collateral"
        )
    elif source_dex_scope == "all_configured_markets":
        warnings.append(
            f"{label} includes all configured DEX positions; all configured DEX positions "
            "use total Unified collateral"
        )

    return {
        "slot": slot,
        "source_address": source_address,
        "subaccount_address": subaccount_address,
        "mode": mode,
        "enabled": enabled,
        "operational_status": operational_status,
        "subaccount_verified": subaccount_verified,
        "sizing_policy": sizing_policy,
        "initial_budget_usd": decimal_str(initial_budget_usd),
        "fixed_risk_budget_usd": decimal_str(fixed_risk_budget_usd),
        "fixed_multiplier": decimal_str(fixed_multiplier),
        "allowed_coins": allowed_coins,
        "denied_coins": denied_coins,
        "min_notional_usd": decimal_str(min_notional_usd),
        "dust_policy": dust_policy,
        "entry_slippage_bps": decimal_str(entry_slippage_bps),
        "reduce_only_slippage_bps": decimal_str(reduce_only_slippage_bps),
        "max_emergency_leverage": decimal_str(max_emergency_leverage),
        "max_gross_notional_usd": decimal_str(max_gross_notional_usd),
        "exchange_action_timeout_s": decimal_str(exchange_action_timeout_s),
        "expected_margin_mode": expected_margin_mode,
        "expected_account_mode": expected_account_mode,
        "equity_confidence_policy": equity_confidence_policy,
        "source_dex_scope": source_dex_scope,
        "operator_verified_at": operator_verified_at,
        "retirement_reason": retirement_reason,
        "retired_at": retired_at,
        "retirement_evidence": retirement_evidence,
        "known_residual_positions": known_residual_positions,
        "retirement_open_orders": retirement_open_orders,
        "note": none_if_unknown(raw.get("note")),
    }


def is_timezone_aware_timestamp(value: str) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except OSError as exc:
        raise SlotPlanInputError(f"could not read slot plan: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SlotPlanInputError(f"slot plan must be valid JSON: {exc.msg}") from exc


def slot_label(raw: dict[str, Any], index: int) -> str:
    slot = clean(raw.get("slot"))
    if slot == "unknown":
        return f"slots[{index}]"
    return f"slot {slot}"


def required_text(
    raw: dict[str, Any],
    key: str,
    *,
    label: str,
    blockers: list[str],
) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        blockers.append(f"{label} {key} is required")
        return "unknown"
    return value.strip()


def required_address(
    raw: dict[str, Any],
    key: str,
    *,
    label: str,
    blockers: list[str],
) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not ADDRESS_RE.fullmatch(value.strip()):
        blockers.append(f"{label} {key} must be a 42-character hex address")
        return "unknown"
    return value.strip().lower()


def positive_decimal(
    raw: dict[str, Any],
    key: str,
    *,
    label: str,
    blockers: list[str],
    required: bool,
) -> Decimal | None:
    if key not in raw or raw.get(key) in (None, ""):
        if required:
            blockers.append(f"{label} {key} is required")
        return None
    value = decimal_optional(raw.get(key))
    if value is None:
        blockers.append(f"{label} {key} must be a decimal value")
        return None
    if value <= Decimal("0"):
        blockers.append(f"{label} {key} must be positive")
    return value


def nonnegative_integer(
    raw: dict[str, Any],
    key: str,
    *,
    label: str,
    blockers: list[str],
    required: bool,
) -> int | None:
    if key not in raw or raw.get(key) in (None, ""):
        if required:
            blockers.append(f"{label} {key} is required")
        return None
    value = raw.get(key)
    if isinstance(value, bool):
        blockers.append(f"{label} {key} must be a nonnegative integer")
        return None
    text = str(value).strip()
    try:
        parsed = int(text)
    except (TypeError, ValueError):
        blockers.append(f"{label} {key} must be a nonnegative integer")
        return None
    if str(parsed) != text or parsed < 0:
        blockers.append(f"{label} {key} must be a nonnegative integer")
        return None
    return parsed


def slippage_decimal(
    raw: dict[str, Any],
    key: str,
    *,
    label: str,
    blockers: list[str],
) -> Decimal | None:
    if key not in raw or raw.get(key) in (None, ""):
        blockers.append(f"{label} {key} is required")
        return None
    value = decimal_optional(raw.get(key))
    if value is None:
        blockers.append(f"{label} {key} must be a decimal value")
        return None
    if value < Decimal("0"):
        blockers.append(f"{label} {key} cannot be negative")
        return None
    if value is None:
        return None
    if value >= Decimal("10000"):
        blockers.append(f"{label} {key} must be below 10000")
    return value


def validate_dust_policy(value: Any, *, label: str, blockers: list[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        blockers.append(f"{label} dust_policy must be an object")
        return {"mode": "unknown", "stale_after_ms": None}
    mode = clean(value.get("mode")).lower()
    if mode not in DUST_MODES:
        blockers.append(f"{label} dust_policy.mode must be one of: {', '.join(sorted(DUST_MODES))}")
    stale_after_ms = value.get("stale_after_ms")
    normalized_stale_after_ms: int | None = None
    if stale_after_ms is not None and stale_after_ms != "":
        if isinstance(stale_after_ms, bool):
            blockers.append(f"{label} dust_policy.stale_after_ms must be an integer")
        else:
            try:
                normalized_stale_after_ms = int(stale_after_ms)
            except (TypeError, ValueError):
                blockers.append(f"{label} dust_policy.stale_after_ms must be an integer")
            else:
                if normalized_stale_after_ms < 0:
                    blockers.append(f"{label} dust_policy.stale_after_ms cannot be negative")
    return {
        "mode": mode,
        "stale_after_ms": normalized_stale_after_ms,
    }


def coin_list(value: Any, *, label: str, blockers: list[str]) -> list[str]:
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        blockers.append(f"{label} must be an array")
        return []
    result: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        try:
            coin = canonical_market(item)
        except MarketIdentityError as exc:
            blockers.append(f"{label}[{index}] must be a valid market symbol: {exc}")
            continue
        if coin in seen:
            blockers.append(f"{label} contains duplicate coin {coin}")
            continue
        seen.add(coin)
        result.append(coin)
    return result


def residual_position_map(
    value: Any,
    *,
    label: str,
    blockers: list[str],
    required: bool,
) -> dict[str, str]:
    if value in (None, ""):
        if required:
            blockers.append(f"{label} is required")
        return {}
    if not isinstance(value, dict) or not value:
        blockers.append(f"{label} must be a non-empty market-to-size object")
        return {}
    result: dict[str, str] = {}
    for raw_coin, raw_size in value.items():
        try:
            coin = canonical_market(raw_coin)
        except MarketIdentityError as exc:
            blockers.append(f"{label}.{raw_coin} must be a valid market symbol: {exc}")
            continue
        size = decimal_optional(raw_size)
        if size is None or size == 0:
            blockers.append(f"{label}.{coin} must be a finite nonzero decimal size")
            continue
        if coin in result:
            blockers.append(f"{label} contains duplicate market {coin}")
            continue
        normalized = decimal_str(size)
        assert normalized is not None
        result[coin] = normalized
    return result


def record_duplicate(
    seen: dict[str, int],
    value: str,
    index: int,
    *,
    field: str,
    blockers: list[str],
) -> None:
    if value == "unknown":
        return
    if value in seen:
        blockers.append(f"{field} {value} is duplicated in slots[{seen[value]}] and slots[{index}]")
        return
    seen[value] = index


def find_secret_fields(value: Any, *, path: str = "$") -> list[str]:
    matches: list[str] = []
    if isinstance(value, dict):
        for key, nested in value.items():
            next_path = f"{path}.{key}"
            lowered = str(key).lower()
            if any(fragment in lowered for fragment in SECRET_KEY_FRAGMENTS):
                matches.append(next_path)
            matches.extend(find_secret_fields(nested, path=next_path))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            matches.extend(find_secret_fields(nested, path=f"{path}[{index}]"))
    return matches


def bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def decimal_optional(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.8f}"


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value).strip() or "unknown"


def none_if_unknown(value: Any) -> str | None:
    cleaned = clean(value)
    return None if cleaned == "unknown" else cleaned


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a read-only Hyperliquid source-to-subaccount slot plan."
    )
    parser.add_argument("plan", type=Path, help="Slot plan JSON to validate.")
    parser.add_argument("--out", type=Path, default=None, help="Write validation report JSON.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = validate_slot_plan(args.plan)
    except SlotPlanInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.out is not None:
        write_json(args.out, report)
        print(
            json.dumps(
                {
                    "report": str(args.out),
                    "valid": report["valid"],
                    "blockers": len(report["blockers"]),
                    "warnings": len(report["warnings"]),
                    "slots": report["counts"]["slots"],
                    "exchange_touched": report["exchange_touched"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
