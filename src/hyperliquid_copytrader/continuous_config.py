from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping

from .credential_setup import FleetCredentialProfileRegistry
from .markets import canonical_market_symbol


ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}")
ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
CONTINUOUS_PLAN_VERSION = 1


@dataclass(frozen=True, slots=True)
class ContinuousSlotConfig:
    slot: str
    source_address: str
    follower_account_address: str
    credential_profile_id: str
    multiplier: Decimal
    max_order_notional_usd: Decimal
    max_gross_exposure_usd: Decimal
    max_open_positions: int
    max_leverage: int
    action_limit_per_minute: int
    allowed_markets: tuple[str, ...]
    enabled: bool


@dataclass(frozen=True, slots=True)
class ContinuousPlan:
    version: int
    network: str
    runtime_id: str
    startup_baseline_only: bool
    max_combined_gross_usd: Decimal
    slots: tuple[ContinuousSlotConfig, ...]
    path: Path
    sha256: str

    @property
    def enabled_slots(self) -> tuple[ContinuousSlotConfig, ...]:
        return tuple(slot for slot in self.slots if slot.enabled)


@dataclass(frozen=True, slots=True)
class BoundContinuousSlot:
    config: ContinuousSlotConfig
    api_wallet_address: str
    api_private_key_file: Path
    global_account_address: str
    expected_account_mode: str
    dynamic_market_eligibility: bool = False
    denied_markets: tuple[str, ...] = ()
    external_writers_allowed: bool = False


@dataclass(frozen=True, slots=True)
class BoundContinuousPlan:
    plan: ContinuousPlan
    slots: tuple[BoundContinuousSlot, ...]


def load_continuous_plan(path: Path | str) -> ContinuousPlan:
    source = Path(path).resolve()
    try:
        text = source.read_text(encoding="utf-8-sig")
        payload = json.loads(text)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"continuous plan is unreadable: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("continuous plan must be a JSON object")
    _only_fields(
        payload,
        {
            "version",
            "network",
            "runtime_id",
            "startup_baseline_only",
            "max_combined_gross_usd",
            "slots",
        },
        "continuous plan",
    )
    if payload.get("version") != CONTINUOUS_PLAN_VERSION:
        raise ValueError(f"continuous plan version must be {CONTINUOUS_PLAN_VERSION}")
    network = str(payload.get("network") or "").lower()
    if network not in {"mainnet", "testnet"}:
        raise ValueError("continuous plan network must be mainnet or testnet")
    runtime_id = str(payload.get("runtime_id") or "").lower()
    if not ID_RE.fullmatch(runtime_id):
        raise ValueError("continuous plan runtime_id is invalid")
    if payload.get("startup_baseline_only") is not True:
        raise ValueError("continuous runtime requires startup_baseline_only=true")
    combined_cap = _positive_decimal(
        payload.get("max_combined_gross_usd"), "max_combined_gross_usd"
    )
    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, list) or not 1 <= len(raw_slots) <= 10:
        raise ValueError("continuous plan requires between one and ten slots")
    slots = tuple(_parse_slot(item) for item in raw_slots)
    _validate_slots(slots, combined_cap)
    canonical = {
        "version": CONTINUOUS_PLAN_VERSION,
        "network": network,
        "runtime_id": runtime_id,
        "startup_baseline_only": True,
        "max_combined_gross_usd": str(combined_cap),
        "slots": [_slot_payload(slot) for slot in slots],
    }
    digest = sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return ContinuousPlan(
        version=CONTINUOUS_PLAN_VERSION,
        network=network,
        runtime_id=runtime_id,
        startup_baseline_only=True,
        max_combined_gross_usd=combined_cap,
        slots=slots,
        path=source,
        sha256=digest,
    )


def bind_continuous_plan(
    plan: ContinuousPlan,
    *,
    repo_root: Path,
    verify_secrets: bool = True,
) -> BoundContinuousPlan:
    """Bind public slot identities to file-only signer credentials.

    A directly injected key is intentionally rejected even when a valid key file
    exists.  There must be exactly one inspectable credential source.
    """

    if os.getenv("HLCT_API_PRIVATE_KEY", "").strip():
        raise ValueError("direct private-key environment input is forbidden for continuous runtime")
    registry = FleetCredentialProfileRegistry(repo_root)
    records, invalid = registry._records_with_health(verify_secrets=verify_secrets)
    if invalid:
        raise ValueError(f"invalid credential profiles: {', '.join(sorted(invalid))}")
    sources = {slot.source_address for slot in plan.enabled_slots}
    for role, field in {
        "owned follower account": "follower_account_address",
        "API wallet": "api_wallet_address",
        "global/action principal": "global_account_address",
    }.items():
        collisions = sorted(
            sources & {str(record.get(field) or "").strip().lower() for record in records}
        )
        if collisions:
            raise ValueError(
                f"enabled source address collides with {role}: {', '.join(collisions)}"
            )
    by_id = {str(record["profile_id"]): record for record in records}
    bound: list[BoundContinuousSlot] = []
    api_wallets: set[str] = set()
    for slot in plan.enabled_slots:
        record = by_id.get(slot.credential_profile_id)
        if record is None:
            raise ValueError(f"slot {slot.slot} credential profile is missing")
        if str(record["source_wallet"]).lower() != slot.source_address:
            raise ValueError(f"slot {slot.slot} source does not match its credential profile")
        if str(record["follower_account_address"]).lower() != slot.follower_account_address:
            raise ValueError(f"slot {slot.slot} follower does not match its credential profile")
        denied = {
            canonical_market_symbol(str(symbol)) for symbol in record.get("denied_symbols", ())
        }
        dynamic_market_eligibility = (
            str(record.get("eligibility") or "").strip().lower() == "all_active_markets"
        )
        conflicts = sorted(set(slot.allowed_markets) & denied)
        if conflicts and not dynamic_market_eligibility:
            raise ValueError(
                f"slot {slot.slot} allows markets denied by its credential profile: "
                f"{', '.join(conflicts)}"
            )
        if not dynamic_market_eligibility and not slot.allowed_markets:
            raise ValueError(
                f"slot {slot.slot} requires either all-active profile eligibility or "
                "an explicit legacy market allowlist"
            )
        api_wallet = str(record["api_wallet_address"]).lower()
        if api_wallet in api_wallets:
            raise ValueError("continuous slots require distinct API wallets")
        api_wallets.add(api_wallet)
        bound.append(
            BoundContinuousSlot(
                # The main runtime ignores this legacy tuple for all-active profiles, but
                # proof/recovery plans retain their explicit pin and provenance.
                config=slot,
                api_wallet_address=api_wallet,
                api_private_key_file=Path(str(record["api_private_key_file"])).resolve(),
                global_account_address=str(record["global_account_address"]).lower(),
                expected_account_mode=str(record["expected_account_mode"]),
                dynamic_market_eligibility=dynamic_market_eligibility,
                denied_markets=tuple(sorted(denied)),
            )
        )
    return BoundContinuousPlan(plan=plan, slots=tuple(bound))


def _parse_slot(raw: Any) -> ContinuousSlotConfig:
    if not isinstance(raw, Mapping):
        raise ValueError("continuous slot must be an object")
    _only_fields(
        raw,
        {
            "slot",
            "source_address",
            "follower_account_address",
            "credential_profile_id",
            "multiplier",
            "max_order_notional_usd",
            "max_gross_exposure_usd",
            "max_open_positions",
            "max_leverage",
            "action_limit_per_minute",
            "allowed_markets",
            "enabled",
        },
        "continuous slot",
    )
    slot_id = str(raw.get("slot") or "").lower()
    profile_id = str(raw.get("credential_profile_id") or "").lower()
    if not ID_RE.fullmatch(slot_id) or not ID_RE.fullmatch(profile_id):
        raise ValueError("continuous slot/profile ID is invalid")
    source = str(raw.get("source_address") or "").lower()
    follower = str(raw.get("follower_account_address") or "").lower()
    if not ADDRESS_RE.fullmatch(source) or not ADDRESS_RE.fullmatch(follower):
        raise ValueError(f"slot {slot_id} has an invalid account address")
    if source == follower:
        raise ValueError(f"slot {slot_id} source and follower must differ")
    multiplier = _positive_decimal(raw.get("multiplier"), f"{slot_id}.multiplier")
    order_cap = _positive_decimal(
        raw.get("max_order_notional_usd"), f"{slot_id}.max_order_notional_usd"
    )
    gross_cap = _positive_decimal(
        raw.get("max_gross_exposure_usd"), f"{slot_id}.max_gross_exposure_usd"
    )
    if order_cap > gross_cap:
        raise ValueError(f"slot {slot_id} order cap exceeds gross cap")
    max_positions = _positive_int(raw.get("max_open_positions"), f"{slot_id}.max_open_positions")
    max_leverage = _positive_int(raw.get("max_leverage"), f"{slot_id}.max_leverage")
    action_limit = _positive_int(
        raw.get("action_limit_per_minute"), f"{slot_id}.action_limit_per_minute"
    )
    allowed_raw = raw.get("allowed_markets", [])
    if not isinstance(allowed_raw, list):
        raise ValueError(f"slot {slot_id} allowed_markets must be a list")
    markets: list[str] = []
    for value in allowed_raw:
        market = canonical_market_symbol(str(value))
        if market in markets:
            raise ValueError(f"slot {slot_id} has duplicate allowed market {market}")
        markets.append(market)
    enabled = raw.get("enabled")
    if not isinstance(enabled, bool):
        raise ValueError(f"slot {slot_id} enabled must be boolean")
    return ContinuousSlotConfig(
        slot=slot_id,
        source_address=source,
        follower_account_address=follower,
        credential_profile_id=profile_id,
        multiplier=multiplier,
        max_order_notional_usd=order_cap,
        max_gross_exposure_usd=gross_cap,
        max_open_positions=max_positions,
        max_leverage=max_leverage,
        action_limit_per_minute=action_limit,
        allowed_markets=tuple(markets),
        enabled=enabled,
    )


def _validate_slots(slots: tuple[ContinuousSlotConfig, ...], combined_cap: Decimal) -> None:
    enabled = tuple(slot for slot in slots if slot.enabled)
    if not enabled:
        raise ValueError("continuous plan has no enabled slots")
    for name, values in {
        "slot IDs": [slot.slot for slot in slots],
        "source addresses": [slot.source_address for slot in enabled],
        "follower addresses": [slot.follower_account_address for slot in enabled],
        "credential profiles": [slot.credential_profile_id for slot in enabled],
    }.items():
        if len(values) != len(set(values)):
            raise ValueError(f"continuous plan has duplicate {name}")
    feedback = sorted(
        {slot.source_address for slot in enabled}
        & {slot.follower_account_address for slot in slots}
    )
    if feedback:
        raise ValueError(
            "enabled source address collides with owned follower account: " + ", ".join(feedback)
        )
    if sum((slot.max_gross_exposure_usd for slot in enabled), Decimal("0")) < combined_cap:
        raise ValueError("combined gross cap exceeds the sum of enabled slot caps")


def _slot_payload(slot: ContinuousSlotConfig) -> dict[str, Any]:
    return {
        "slot": slot.slot,
        "source_address": slot.source_address,
        "follower_account_address": slot.follower_account_address,
        "credential_profile_id": slot.credential_profile_id,
        "multiplier": str(slot.multiplier),
        "max_order_notional_usd": str(slot.max_order_notional_usd),
        "max_gross_exposure_usd": str(slot.max_gross_exposure_usd),
        "max_open_positions": slot.max_open_positions,
        "max_leverage": slot.max_leverage,
        "action_limit_per_minute": slot.action_limit_per_minute,
        "allowed_markets": list(slot.allowed_markets),
        "enabled": slot.enabled,
    }


def _only_fields(payload: Mapping[str, Any], allowed: set[str], label: str) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"{label} has unknown fields: {', '.join(unknown)}")


def _positive_decimal(value: Any, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a decimal") from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return result


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if result <= 0 or str(result) != str(value):
        raise ValueError(f"{label} must be a positive integer")
    return result
