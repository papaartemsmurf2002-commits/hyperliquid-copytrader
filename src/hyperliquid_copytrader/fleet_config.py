from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from .markets import canonical_market_symbol
from .rest_budget import RestLoadModel
from .stream_gateway import (
    ConnectionLimits,
    MARKET_SUBSCRIPTION_CONTROL_HEADROOM,
    SourceStreamGateway,
    active_market_subscription_capacity,
    stable_shard,
)


ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
FLEET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
FLEET_PLAN_VERSION = 3
FLEET_POLICY_VERSION = "fleet-fast-execution-v1"
FLEET_RUNTIME_POLICY_VERSION = "windows-fast-runtime-v2"
PRODUCTION_BENCHMARK_VERSION = "production-path-windows-v6"
LOCAL_REACTION_LATENCY_GATE_VERSION = "operator-authorized-plus-25ms-20260718-v1"
# The operator explicitly raised every local-reaction release percentile by
# exactly 25 ms on 2026-07-18.  Keep these values centralized so generation,
# preview, immutable-SQLite revalidation, and terminal diagnostics cannot drift.
DETERMINISTIC_LOCAL_REACTION_MAXIMA_MS: dict[str, int] = {
    "p50_ms": 50,
    "p95_ms": 125,
    "p99_ms": 275,
}
FLEET_LIVE_LOCAL_REACTION_MAXIMA_MS: dict[str, int] = {
    "p95_ms": 125,
    "p99_ms": 275,
}
# Two sequential follower scans across the full dynamic DEX catalog take about
# 18-22 seconds on the production host. Bind guardian truth to the first REST
# response and cap proof creation at half the 60-second session ceiling.
TERMINAL_TRUTH_COHERENCE_MS = 30_000
# Backward-compatible name for external imports while the terminal-truth
# coherence contract is shared by runtime and guardian proofs.
GUARDIAN_TERMINAL_TRUTH_COHERENCE_MS = TERMINAL_TRUTH_COHERENCE_MS
FLEET_RUNTIME_POLICY: dict[str, int | str] = {
    "version": FLEET_RUNTIME_POLICY_VERSION,
    "defer_window_ms": 300_000,
    "rearm_min_notional_usd": "1",
    "scheduler_bound_ms": 100,
    "affected_follower_refresh_ms": 5_000,
    "full_follower_audit_ms": 60_000,
    "nonfunding_ledger_audit_ms": 600_000,
    "catalog_refresh_ms": 60_000,
    "source_shards": 2,
    "action_shards": 2,
    "market_data_connections": 1,
    "maximum_fleet_sources": 10,
    "maximum_active_markets": 420,
    "source_queue_capacity": 4_096,
    "source_event_queue_capacity": 1_024,
    "slot_queue_capacity": 1_024,
    "journal_queue_capacity": 4_096,
    "market_queue_capacity": 4_096,
    "market_subscription_queue_capacity": 1_024,
    "execution_lane_capacity": 256,
    "ambiguity_queue_capacity": 1_024,
    "websocket_heartbeat_ms": 30_000,
    "websocket_reconnect_min_ms": 250,
    "websocket_reconnect_max_ms": 5_000,
    "websocket_connection_limit": 10,
    "websocket_normal_connection_limit": 5,
    "websocket_overlap_limit": 8,
    "websocket_subscription_limit": 1_000,
    "websocket_unique_user_limit": 10,
    "websocket_outbound_per_minute": 2_000,
    "websocket_inflight_post_limit": 100,
    "websocket_new_connections_per_minute": 30,
    "rest_ordinary_weight_per_minute": 720,
    "rest_reserve_weight_per_minute": 480,
    "clock_max_skew_ms": 500,
    "clock_max_jump_ms": 500,
    "direct_source_max_age_ms": 5_000,
    "action_limit_per_minute": 12,
    "primary_cleanup_timeout_ms": 1_800_000,
    "catalog_policy_version": "dynamic-all-active-v1",
}


def fleet_connection_limits() -> ConnectionLimits:
    """Build the connection budget from the same policy frozen into launch evidence."""

    return ConnectionLimits(
        maximum_connections=int(FLEET_RUNTIME_POLICY["websocket_connection_limit"]),
        normal_connections=int(FLEET_RUNTIME_POLICY["websocket_normal_connection_limit"]),
        overlap_connections=int(FLEET_RUNTIME_POLICY["websocket_overlap_limit"]),
        subscriptions=int(FLEET_RUNTIME_POLICY["websocket_subscription_limit"]),
        unique_users=int(FLEET_RUNTIME_POLICY["websocket_unique_user_limit"]),
        outbound_per_minute=int(FLEET_RUNTIME_POLICY["websocket_outbound_per_minute"]),
        inflight_posts=int(FLEET_RUNTIME_POLICY["websocket_inflight_post_limit"]),
        new_connections_per_minute=int(
            FLEET_RUNTIME_POLICY["websocket_new_connections_per_minute"]
        ),
    )


def fleet_connection_budget_payload() -> dict[str, int]:
    limits = fleet_connection_limits()
    return {
        "normal_connections": limits.normal_connections,
        "source_shards": int(FLEET_RUNTIME_POLICY["source_shards"]),
        "market_data_connections": int(FLEET_RUNTIME_POLICY["market_data_connections"]),
        "action_shards": int(FLEET_RUNTIME_POLICY["action_shards"]),
        "bounded_reconnect_overlap": limits.overlap_connections,
        "venue_connection_limit": limits.maximum_connections,
        "subscription_limit": limits.subscriptions,
        "unique_user_limit": limits.unique_users,
        "outbound_per_minute": limits.outbound_per_minute,
        "inflight_posts": limits.inflight_posts,
        "new_connections_per_minute": limits.new_connections_per_minute,
    }


def fleet_rest_budget_payload() -> dict[str, int]:
    return {
        "ordinary": int(FLEET_RUNTIME_POLICY["rest_ordinary_weight_per_minute"]),
        "reserve": int(FLEET_RUNTIME_POLICY["rest_reserve_weight_per_minute"]),
    }


@dataclass(frozen=True, slots=True)
class FleetSlot:
    slot: str
    source_address: str
    follower_account_address: str
    credential_profile_id: str
    required_lifecycle_class: str
    expected_account_mode: str
    eligibility: str
    denied_symbols: tuple[str, ...]
    fixed_multiplier: Decimal
    max_initial_margin_utilization: Decimal
    max_notional_usd: Decimal
    max_gross_exposure_usd: Decimal
    max_open_positions: int
    max_leverage: int
    action_limit_per_minute: int
    max_audited_dexes: int
    source_shard: int
    action_shard: int
    enabled: bool
    operator_verified_at: str

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        for field in (
            "fixed_multiplier",
            "max_initial_margin_utilization",
            "max_notional_usd",
            "max_gross_exposure_usd",
        ):
            payload[field] = str(payload[field])
        payload["denied_symbols"] = list(self.denied_symbols)
        return payload


@dataclass(frozen=True, slots=True)
class FleetPlan:
    version: int
    environment: str
    purpose: str
    policy_version: str
    intended_fleet_complete: bool
    slots: tuple[FleetSlot, ...]
    sha256: str
    path: Path

    @property
    def enabled_slots(self) -> tuple[FleetSlot, ...]:
        return tuple(slot for slot in self.slots if slot.enabled)

    def public_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "environment": self.environment,
            "purpose": self.purpose,
            "policy_version": self.policy_version,
            "intended_fleet_complete": self.intended_fleet_complete,
            "slots": [slot.to_payload() for slot in self.slots],
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class CredentialReference:
    profile_id: str
    api_wallet_address: str
    api_private_key_file: Path
    profile_path: Path
    profile_sha256: str
    source_address: str
    follower_account_address: str
    expected_account_mode: str
    eligibility: str
    denied_symbols: tuple[str, ...]

    def redacted_payload(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "api_wallet_address": self.api_wallet_address,
            "key_reference": self.api_private_key_file.name,
            "profile_sha256": self.profile_sha256,
            "source_address": self.source_address,
            "follower_account_address": self.follower_account_address,
            "expected_account_mode": self.expected_account_mode,
            "eligibility": self.eligibility,
            "denied_symbols": list(self.denied_symbols),
        }


@dataclass(frozen=True, slots=True)
class CredentialMap:
    version: int
    references: Mapping[str, CredentialReference]
    sha256: str
    path: Path

    def redacted_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "slots": {
                key: value.redacted_payload() for key, value in sorted(self.references.items())
            },
            "sha256": self.sha256,
        }


ProvisioningScope = Literal["pilot", "fleet"]

STATIC_REST_LOAD_MODEL_VERSION = "selected-launch-plan-rest-floor-v1"
CATALOG_REST_LOAD_MODEL_VERSION = "selected-launch-plan-live-catalog-v1"


def _sealed_rest_load_model(
    validation: Mapping[str, Any],
    *,
    model_version: str,
    evidence_scope: str,
    slot_count: int,
    affected_dex_queries_per_cycle: int,
    full_audit_dex_queries_per_cycle: int,
    wire_dexes_sha256: str = "",
) -> dict[str, Any]:
    payload = {
        **dict(validation),
        "model_version": model_version,
        "evidence_scope": evidence_scope,
        "slot_count": slot_count,
        "affected_dex_queries_per_cycle": affected_dex_queries_per_cycle,
        "full_audit_dex_queries_per_cycle": full_audit_dex_queries_per_cycle,
        "wire_dexes_sha256": wire_dexes_sha256,
    }
    payload["model_sha256"] = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload


def selected_plan_rest_load_model(
    plan: FleetPlan,
    *,
    ordinary_rest_budget: int,
    reserve_rest_budget: int,
) -> dict[str, Any]:
    """Build the pre-catalog REST floor for only the accounts that can launch."""

    slots = plan.enabled_slots
    slot_count = len(slots)
    affected_dexes = sum(slot.max_audited_dexes for slot in slots)
    if slot_count < 1 or affected_dexes < slot_count:
        validation: dict[str, Any] = {
            "components": {},
            "ordinary_budget": ordinary_rest_budget,
            "reserve_budget": reserve_rest_budget,
            "ordinary_headroom": ordinary_rest_budget,
            "blockers": ["selected launch plan has no valid REST workload"],
            "passed": False,
        }
    else:
        validation = RestLoadModel(
            fleet_slots=slot_count,
            audited_dexes=affected_dexes,
            cheap_follower_queries_per_cycle=affected_dexes,
            cheap_follower_period_ms=int(FLEET_RUNTIME_POLICY["affected_follower_refresh_ms"]),
            full_follower_period_ms=int(FLEET_RUNTIME_POLICY["full_follower_audit_ms"]),
            nonfunding_ledger_period_ms=int(FLEET_RUNTIME_POLICY["nonfunding_ledger_audit_ms"]),
            catalog_period_ms=int(FLEET_RUNTIME_POLICY["catalog_refresh_ms"]),
        ).validate(
            ordinary_budget=ordinary_rest_budget,
            reserve_budget=reserve_rest_budget,
        )
    return _sealed_rest_load_model(
        validation,
        model_version=STATIC_REST_LOAD_MODEL_VERSION,
        evidence_scope="selected_launch_plan_static_pre_catalog_floor",
        slot_count=slot_count,
        affected_dex_queries_per_cycle=affected_dexes,
        full_audit_dex_queries_per_cycle=affected_dexes,
    )


def actual_catalog_rest_load_model(
    plan: FleetPlan,
    discovery: Mapping[str, Any],
    *,
    ordinary_rest_budget: int,
    reserve_rest_budget: int,
) -> dict[str, Any]:
    """Bind selected-account REST admission to the exact live DEX catalog."""

    slots = plan.enabled_slots
    slot_count = len(slots)
    affected_dexes = sum(slot.max_audited_dexes for slot in slots)
    raw_wire_dexes = discovery.get("wire_dexes")
    wire_dexes = (
        list(raw_wire_dexes)
        if isinstance(raw_wire_dexes, list)
        and all(isinstance(item, str) for item in raw_wire_dexes)
        else []
    )
    declared_dex_count = discovery.get("dex_count")
    topology_blockers: list[str] = []
    if (
        not wire_dexes
        or "" not in wire_dexes
        or len(set(wire_dexes)) != len(wire_dexes)
        or not isinstance(declared_dex_count, int)
        or isinstance(declared_dex_count, bool)
        or declared_dex_count != len(wire_dexes)
    ):
        topology_blockers.append("live catalog REST load topology is malformed")
    if slot_count < 1 or affected_dexes < slot_count:
        topology_blockers.append("selected launch plan has no valid REST workload")
    wire_dexes_sha256 = (
        sha256(
            json.dumps(wire_dexes, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if wire_dexes
        else ""
    )
    if topology_blockers:
        validation = {
            "components": {},
            "ordinary_budget": ordinary_rest_budget,
            "reserve_budget": reserve_rest_budget,
            "ordinary_headroom": ordinary_rest_budget,
            "blockers": topology_blockers,
            "passed": False,
        }
        full_audit_dexes = 0
    else:
        full_audit_dexes = slot_count * len(wire_dexes)
        validation = RestLoadModel(
            fleet_slots=slot_count,
            audited_dexes=full_audit_dexes,
            cheap_follower_queries_per_cycle=affected_dexes,
            cheap_follower_period_ms=int(FLEET_RUNTIME_POLICY["affected_follower_refresh_ms"]),
            full_follower_period_ms=int(FLEET_RUNTIME_POLICY["full_follower_audit_ms"]),
            nonfunding_ledger_period_ms=int(FLEET_RUNTIME_POLICY["nonfunding_ledger_audit_ms"]),
            catalog_period_ms=int(FLEET_RUNTIME_POLICY["catalog_refresh_ms"]),
        ).validate(
            ordinary_budget=ordinary_rest_budget,
            reserve_budget=reserve_rest_budget,
        )
    return _sealed_rest_load_model(
        validation,
        model_version=CATALOG_REST_LOAD_MODEL_VERSION,
        evidence_scope="selected_launch_plan_live_catalog",
        slot_count=slot_count,
        affected_dex_queries_per_cycle=affected_dexes,
        full_audit_dex_queries_per_cycle=full_audit_dexes,
        wire_dexes_sha256=wire_dexes_sha256,
    )


@dataclass(frozen=True, slots=True)
class BenchmarkWorkload:
    scope: ProvisioningScope
    evidence_scope: str
    launch_plan_sha256: str
    plan: FleetPlan
    synthetic_slot_ids: tuple[str, ...]

    def binding_payload(self) -> dict[str, Any]:
        source_counts = Counter(slot.source_shard for slot in self.plan.enabled_slots)
        action_counts = Counter(slot.action_shard for slot in self.plan.enabled_slots)
        payload = {
            "scope": self.scope,
            "evidence_scope": self.evidence_scope,
            "launch_plan_sha256": self.launch_plan_sha256,
            "workload_plan_sha256": self.plan.sha256,
            "workload_slot_count": len(self.plan.enabled_slots),
            "synthetic_slot_ids": list(self.synthetic_slot_ids),
            "source_shard_counts": {
                str(shard): source_counts.get(shard, 0)
                for shard in range(int(FLEET_RUNTIME_POLICY["source_shards"]))
            },
            "action_shard_counts": {
                str(shard): action_counts.get(shard, 0)
                for shard in range(int(FLEET_RUNTIME_POLICY["action_shards"]))
            },
            "maximum_active_markets": int(FLEET_RUNTIME_POLICY["maximum_active_markets"]),
        }
        payload["capacity_profile_sha256"] = sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return payload


def _plan_payload_sha256(
    *,
    version: int,
    environment: str,
    purpose: str,
    policy_version: str,
    intended_fleet_complete: bool,
    slots: Iterable[FleetSlot],
) -> str:
    payload = {
        "version": version,
        "environment": environment,
        "purpose": purpose,
        "policy_version": policy_version,
        "intended_fleet_complete": intended_fleet_complete,
        "slots": [slot.to_payload() for slot in slots],
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _synthetic_shard_zero_address(
    *,
    domain: Literal["source", "action"],
    seed: int,
    used: set[str],
) -> str:
    for value in range(seed, seed + 1_000_000):
        address = f"0x{value:040x}"
        if address not in used and stable_shard(address, 2, domain=domain) == 0:
            used.add(address)
            return address
    raise RuntimeError(f"could not construct deterministic synthetic {domain} identity")


def build_benchmark_workload(
    *,
    scope: ProvisioningScope,
    selected_plan: FleetPlan,
) -> BenchmarkWorkload:
    if scope == "fleet":
        if (
            selected_plan.purpose != "full_fleet_12h"
            or not selected_plan.intended_fleet_complete
            or len(selected_plan.enabled_slots)
            != int(FLEET_RUNTIME_POLICY["maximum_fleet_sources"])
        ):
            raise ValueError("fleet benchmark requires the exact complete full-fleet plan")
        return BenchmarkWorkload(
            scope="fleet",
            evidence_scope="fleet_exact_10_slot",
            launch_plan_sha256=selected_plan.sha256,
            plan=selected_plan,
            synthetic_slot_ids=(),
        )
    if scope != "pilot":
        raise ValueError("benchmark workload scope must be pilot or fleet")
    if selected_plan.purpose != "pilot_12h" or len(selected_plan.enabled_slots) != 2:
        raise ValueError("pilot capacity benchmark requires the exact two-slot pilot plan")
    if {slot.source_shard for slot in selected_plan.enabled_slots} != {0, 1} or {
        slot.action_shard for slot in selected_plan.enabled_slots
    } != {0, 1}:
        raise ValueError("pilot capacity benchmark requires both frozen shard dimensions")
    slots = list(selected_plan.enabled_slots)
    used = {
        identity
        for slot in slots
        for identity in (slot.source_address, slot.follower_account_address)
    }
    template = slots[0]
    synthetic_ids: list[str] = []
    for index in range(3, int(FLEET_RUNTIME_POLICY["maximum_fleet_sources"]) + 1):
        slot_id = f"capacity-{index:02d}"
        synthetic_ids.append(slot_id)
        slots.append(
            replace(
                template,
                slot=slot_id,
                source_address=_synthetic_shard_zero_address(
                    domain="source",
                    seed=10_000_000 + index * 10_000,
                    used=used,
                ),
                follower_account_address=_synthetic_shard_zero_address(
                    domain="action",
                    seed=20_000_000 + index * 10_000,
                    used=used,
                ),
                credential_profile_id=slot_id,
                source_shard=0,
                action_shard=0,
                operator_verified_at="1970-01-01T00:00:00+00:00",
            )
        )
    slot_tuple = tuple(slots)
    purpose = "synthetic_pilot_capacity"
    plan_sha256 = _plan_payload_sha256(
        version=selected_plan.version,
        environment=selected_plan.environment,
        purpose=purpose,
        policy_version=selected_plan.policy_version,
        intended_fleet_complete=False,
        slots=slot_tuple,
    )
    plan = FleetPlan(
        version=selected_plan.version,
        environment=selected_plan.environment,
        purpose=purpose,
        policy_version=selected_plan.policy_version,
        intended_fleet_complete=False,
        slots=slot_tuple,
        sha256=plan_sha256,
        path=selected_plan.path.parent / "__internal_pilot_capacity__.json",
    )
    workload = BenchmarkWorkload(
        scope="pilot",
        evidence_scope="pilot_synthetic_10_slot_capacity",
        launch_plan_sha256=selected_plan.sha256,
        plan=plan,
        synthetic_slot_ids=tuple(synthetic_ids),
    )
    binding = workload.binding_payload()
    if (
        binding["workload_slot_count"] != 10
        or binding["source_shard_counts"] != {"0": 9, "1": 1}
        or binding["action_shard_counts"] != {"0": 9, "1": 1}
        or len(
            {
                identity
                for slot in plan.enabled_slots
                for identity in (slot.source_address, slot.follower_account_address)
            }
        )
        != 20
    ):
        raise RuntimeError("synthetic pilot capacity workload is not the frozen 10-slot 9/1 case")
    return workload


def selected_credential_map_sha256(
    *,
    credentials: CredentialMap,
    profile_ids: Iterable[str],
) -> str:
    """Hash only selected public credential routing identity, never private key bytes."""

    selected_ids = tuple(sorted(set(profile_ids)))
    if not selected_ids:
        raise ValueError("selected credential digest requires at least one profile")
    missing = [
        profile_id for profile_id in selected_ids if profile_id not in credentials.references
    ]
    if missing:
        raise ValueError("selected credential profiles are missing: " + ", ".join(missing))
    payload = {
        "version": 1,
        "profiles": [
            {
                "profile_id": reference.profile_id,
                "api_wallet_address": reference.api_wallet_address,
                "api_private_key_file": str(reference.api_private_key_file.resolve()).casefold(),
                "profile_path": str(reference.profile_path.resolve()).casefold(),
                "profile_sha256": reference.profile_sha256,
                "source_address": reference.source_address,
                "follower_account_address": reference.follower_account_address,
                "expected_account_mode": reference.expected_account_mode,
                "eligibility": reference.eligibility,
                "denied_symbols": list(reference.denied_symbols),
            }
            for profile_id in selected_ids
            for reference in (credentials.references[profile_id],)
        ],
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def load_fleet_plan(path: Path | str) -> FleetPlan:
    source = Path(path).resolve()
    raw_bytes = source.read_bytes()
    try:
        payload = json.loads(raw_bytes)
    except json.JSONDecodeError as exc:
        raise ValueError(f"fleet plan is invalid JSON: {exc.msg}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("fleet plan must be an object")
    allowed_top = {
        "version",
        "environment",
        "purpose",
        "policy_version",
        "intended_fleet_complete",
        "slots",
    }
    unknown_top = sorted(set(payload) - allowed_top)
    if unknown_top:
        raise ValueError("fleet plan has unknown fields: " + ", ".join(unknown_top))
    version = payload.get("version")
    if version != FLEET_PLAN_VERSION:
        raise ValueError(f"fleet plan version must be {FLEET_PLAN_VERSION}")
    environment = str(payload.get("environment") or "").lower()
    if environment not in {"mainnet", "testnet"}:
        raise ValueError("fleet plan environment must be mainnet or testnet")
    purpose = str(payload.get("purpose") or "")
    if purpose not in {"pilot_12h", "full_fleet_12h", "test"}:
        raise ValueError("fleet plan purpose is invalid")
    policy_version = str(payload.get("policy_version") or "")
    if policy_version != FLEET_POLICY_VERSION:
        raise ValueError(f"fleet plan policy_version must be {FLEET_POLICY_VERSION}")
    intended_complete = payload.get("intended_fleet_complete") is True
    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, list) or not raw_slots:
        raise ValueError("fleet plan requires at least one slot")
    slots = tuple(_parse_slot(raw, environment=environment) for raw in raw_slots)
    _validate_slot_set(slots)
    canonical = {
        "version": version,
        "environment": environment,
        "purpose": purpose,
        "policy_version": policy_version,
        "intended_fleet_complete": intended_complete,
        "slots": [slot.to_payload() for slot in slots],
    }
    digest = sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return FleetPlan(
        version=version,
        environment=environment,
        purpose=purpose,
        policy_version=policy_version,
        intended_fleet_complete=intended_complete,
        slots=slots,
        sha256=digest,
        path=source,
    )


def _parse_slot(raw: Any, *, environment: str) -> FleetSlot:
    if not isinstance(raw, Mapping):
        raise ValueError("fleet slot must be an object")
    allowed_fields = {
        "slot",
        "source_address",
        "follower_account_address",
        "credential_profile_id",
        "required_lifecycle_class",
        "expected_account_mode",
        "eligibility",
        "denied_symbols",
        "fixed_multiplier",
        "max_initial_margin_utilization",
        "max_notional_usd",
        "max_gross_exposure_usd",
        "max_open_positions",
        "max_leverage",
        "action_limit_per_minute",
        "max_audited_dexes",
        "source_shard",
        "action_shard",
        "enabled",
        "operator_verified_at",
        "subaccount_verified",
    }
    unknown = sorted(set(raw) - allowed_fields)
    if unknown:
        raise ValueError(
            f"fleet slot {raw.get('slot') or '<unnamed>'} has unknown fields: " + ", ".join(unknown)
        )
    slot_id = str(raw.get("slot") or "").strip()
    source = str(raw.get("source_address") or "").strip().lower()
    follower = (
        str(raw.get("follower_account_address") or raw.get("subaccount_address") or "")
        .strip()
        .lower()
    )
    credential = str(raw.get("credential_profile_id") or "").strip()
    if (
        not FLEET_ID_RE.fullmatch(slot_id)
        or not FLEET_ID_RE.fullmatch(credential)
        or not ADDRESS_RE.fullmatch(source)
        or not ADDRESS_RE.fullmatch(follower)
    ):
        raise ValueError(f"fleet slot {slot_id or '<unnamed>'} identity is invalid")
    if raw.get("subaccount_verified") is not True:
        raise ValueError(f"fleet slot {slot_id} is not operator-verified")
    lifecycle = str(raw.get("required_lifecycle_class") or "").lower()
    if lifecycle not in {"native", "hip3", "both"}:
        raise ValueError(f"fleet slot {slot_id} lifecycle class is invalid")
    account_mode = str(raw.get("expected_account_mode") or "").lower()
    if account_mode != "unified":
        raise ValueError(f"fleet slot {slot_id} must use the frozen unified account mode")
    eligibility = str(raw.get("eligibility") or "").lower()
    if eligibility != "all_active_markets":
        raise ValueError(f"fleet slot {slot_id} must use eligibility=all_active_markets")
    denied_raw = raw.get("denied_symbols", [])
    if not isinstance(denied_raw, list):
        raise ValueError(f"fleet slot {slot_id} denied_symbols must be an array")
    denied = tuple(sorted({canonical_market_symbol(str(item)) for item in denied_raw}))
    fixed_multiplier = _positive_decimal(raw, "fixed_multiplier", slot_id)
    margin = _positive_decimal(raw, "max_initial_margin_utilization", slot_id)
    if fixed_multiplier != Decimal("0.75") or margin != Decimal("0.75"):
        raise ValueError(f"fleet slot {slot_id} must preserve frozen 0.75 sizing policy")
    reviewed_action_limit = int(FLEET_RUNTIME_POLICY["action_limit_per_minute"])
    action_limit = int(raw.get("action_limit_per_minute", reviewed_action_limit))
    if action_limit != reviewed_action_limit:
        raise ValueError(
            f"fleet slot {slot_id} action ceiling must be {reviewed_action_limit}/minute"
        )
    max_audited_dexes = _positive_int(raw, "max_audited_dexes", slot_id)
    source_shard = stable_shard(
        source,
        int(FLEET_RUNTIME_POLICY["source_shards"]),
        domain="source",
    )
    action_shard = stable_shard(
        follower,
        int(FLEET_RUNTIME_POLICY["action_shards"]),
        domain="action",
    )
    declared_source_shard = raw.get("source_shard")
    declared_action_shard = raw.get("action_shard")
    if declared_source_shard is not None and int(declared_source_shard) != source_shard:
        raise ValueError(f"fleet slot {slot_id} source shard conflicts with frozen hashing")
    if declared_action_shard is not None and int(declared_action_shard) != action_shard:
        raise ValueError(f"fleet slot {slot_id} action shard conflicts with frozen hashing")
    verified_at = str(raw.get("operator_verified_at") or "")
    try:
        verified = datetime.fromisoformat(verified_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"fleet slot {slot_id} operator_verified_at is invalid") from exc
    if verified.tzinfo is None or verified.astimezone(timezone.utc).utcoffset() is None:
        raise ValueError(f"fleet slot {slot_id} operator_verified_at must include UTC offset")
    return FleetSlot(
        slot=slot_id,
        source_address=source,
        follower_account_address=follower,
        credential_profile_id=credential,
        required_lifecycle_class=lifecycle,
        expected_account_mode=account_mode,
        eligibility=eligibility,
        denied_symbols=denied,
        fixed_multiplier=fixed_multiplier,
        max_initial_margin_utilization=margin,
        max_notional_usd=_positive_decimal(raw, "max_notional_usd", slot_id),
        max_gross_exposure_usd=_positive_decimal(raw, "max_gross_exposure_usd", slot_id),
        max_open_positions=_positive_int(raw, "max_open_positions", slot_id),
        max_leverage=_positive_int(raw, "max_leverage", slot_id),
        action_limit_per_minute=action_limit,
        max_audited_dexes=max_audited_dexes,
        source_shard=source_shard,
        action_shard=action_shard,
        enabled=raw.get("enabled") is True,
        operator_verified_at=verified.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
    )


def _positive_decimal(raw: Mapping[str, Any], field: str, slot_id: str) -> Decimal:
    try:
        value = Decimal(str(raw[field]))
    except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"fleet slot {slot_id} {field} must be a decimal") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(f"fleet slot {slot_id} {field} must be positive")
    return value


def _positive_int(raw: Mapping[str, Any], field: str, slot_id: str) -> int:
    try:
        value = int(raw[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"fleet slot {slot_id} {field} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"fleet slot {slot_id} {field} must be positive")
    return value


def _validate_slot_set(slots: tuple[FleetSlot, ...]) -> None:
    identities = {
        "slot": [slot.slot for slot in slots],
        "source": [slot.source_address for slot in slots],
        "follower": [slot.follower_account_address for slot in slots],
        "credential": [slot.credential_profile_id for slot in slots],
    }
    for name, values in identities.items():
        if len(values) != len(set(values)):
            raise ValueError(f"fleet plan contains duplicate {name} identity")
    public_accounts = [
        identity
        for slot in slots
        for identity in (slot.source_address, slot.follower_account_address)
    ]
    if len(public_accounts) != len(set(public_accounts)):
        raise ValueError("fleet plan contains a cross-role source/follower identity collision")


def load_credential_map(path: Path | str) -> CredentialMap:
    source = Path(path).resolve()
    raw_bytes = source.read_bytes()
    payload = json.loads(raw_bytes)
    if not isinstance(payload, Mapping) or payload.get("version") != 1:
        raise ValueError("credential map version must be 1")
    raw_slots = payload.get("slots")
    if not isinstance(raw_slots, Mapping):
        raise ValueError("credential map slots must be an object")
    references: dict[str, CredentialReference] = {}
    for profile_id, raw in raw_slots.items():
        if not isinstance(profile_id, str) or not isinstance(raw, Mapping):
            raise ValueError("credential map entry is malformed")
        if not FLEET_ID_RE.fullmatch(profile_id):
            raise ValueError(f"credential profile {profile_id!r} identity is invalid")
        wallet = str(raw.get("api_wallet_address") or "").lower()
        allowed = {"api_wallet_address", "api_private_key_file"}
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise ValueError(
                f"credential profile {profile_id} contains forbidden fields: " + ", ".join(unknown)
            )
        raw_key = Path(str(raw.get("api_private_key_file") or ""))
        key_file = (raw_key if raw_key.is_absolute() else source.parent / raw_key).resolve()
        if not ADDRESS_RE.fullmatch(wallet):
            raise ValueError(f"credential profile {profile_id} API wallet is invalid")
        profile_root = (source.parent / profile_id).resolve()
        if profile_root.parent != source.parent:
            raise ValueError(f"credential profile {profile_id} escapes the profile vault")
        try:
            key_file.relative_to(profile_root)
        except ValueError as exc:
            raise ValueError(
                f"credential profile {profile_id} key path escapes its profile directory"
            ) from exc
        profile_path = profile_root / "profile.json"
        try:
            profile_bytes = profile_path.read_bytes()
            profile = json.loads(profile_bytes)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"credential profile {profile_id} metadata is invalid") from exc
        if not isinstance(profile, Mapping):
            raise ValueError(f"credential profile {profile_id} metadata must be an object")
        source_address = str(profile.get("source_wallet") or "").lower()
        follower = str(profile.get("follower_account_address") or "").lower()
        account_mode = str(profile.get("expected_account_mode") or "").lower()
        eligibility = str(profile.get("eligibility") or "").lower()
        denied_raw = profile.get("denied_symbols", [])
        if not isinstance(denied_raw, list) or any(
            not isinstance(item, str) for item in denied_raw
        ):
            raise ValueError(f"credential profile {profile_id} denylist is invalid")
        denied_symbols = tuple(canonical_market_symbol(item) for item in denied_raw)
        if len(denied_symbols) != len(set(denied_symbols)):
            raise ValueError(f"credential profile {profile_id} denylist has duplicates")
        if (
            profile.get("profile_id") != profile_id
            or not ADDRESS_RE.fullmatch(source_address)
            or not ADDRESS_RE.fullmatch(follower)
            or account_mode != "unified"
            or eligibility != "all_active_markets"
            or "allowed_symbols" in profile
            or str(profile.get("api_wallet_address") or "").lower() != wallet
        ):
            raise ValueError(f"credential profile {profile_id} public identity is inconsistent")
        references[profile_id] = CredentialReference(
            profile_id,
            wallet,
            key_file,
            profile_path,
            sha256(profile_bytes).hexdigest(),
            source_address,
            follower,
            account_mode,
            eligibility,
            denied_symbols,
        )
    digest = sha256(raw_bytes).hexdigest()
    return CredentialMap(1, references, digest, source)


def validate_fleet_provisioning(
    *,
    scope: ProvisioningScope,
    pilot_plan: FleetPlan,
    full_fleet_plan: FleetPlan | None,
    credentials: CredentialMap,
    ordinary_rest_budget: int,
    reserve_rest_budget: int,
    verify_private_keys: bool,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if scope not in {"pilot", "fleet"}:
        raise ValueError("provisioning scope must be pilot or fleet")
    if pilot_plan.environment != "mainnet":
        blockers.append("mainnet fleet control requires a mainnet pilot plan")
    if pilot_plan.purpose != "pilot_12h" or len(pilot_plan.enabled_slots) != 2:
        blockers.append("12-hour pilot plan must enable exactly two slots")
    if {slot.required_lifecycle_class for slot in pilot_plan.enabled_slots} != {
        "native",
        "hip3",
    }:
        blockers.append("12-hour pilot must contain one native and one HIP-3 lifecycle slot")
    if {slot.source_shard for slot in pilot_plan.enabled_slots} != set(
        range(int(FLEET_RUNTIME_POLICY["source_shards"]))
    ):
        blockers.append("12-hour pilot does not exercise both source shards")
    if {slot.action_shard for slot in pilot_plan.enabled_slots} != set(
        range(int(FLEET_RUNTIME_POLICY["action_shards"]))
    ):
        blockers.append("12-hour pilot does not exercise both action shards")
    full_slots: tuple[FleetSlot, ...] = ()
    if scope == "fleet" and full_fleet_plan is None:
        blockers.append("complete intended full-fleet plan is missing")
    elif scope == "fleet" and full_fleet_plan is not None:
        full_slots = full_fleet_plan.slots
        if full_fleet_plan.environment != "mainnet":
            blockers.append("mainnet fleet control requires a mainnet full-fleet plan")
        if full_fleet_plan.environment != pilot_plan.environment:
            blockers.append("pilot and full-fleet plans must use the same environment")
        if full_fleet_plan.purpose != "full_fleet_12h":
            blockers.append("full-fleet plan purpose must be full_fleet_12h")
        if not full_fleet_plan.intended_fleet_complete:
            blockers.append("full-fleet plan lacks intended_fleet_complete operator attestation")
        maximum_fleet_sources = int(FLEET_RUNTIME_POLICY["maximum_fleet_sources"])
        if (
            len(full_fleet_plan.enabled_slots) != maximum_fleet_sources
            or len(full_slots) != maximum_fleet_sources
        ):
            blockers.append("complete intended fleet must contain exactly ten enabled source slots")
        full_by_public: dict[tuple[str, str, str], list[FleetSlot]] = {}
        for slot in full_slots:
            identity = (
                slot.source_address,
                slot.follower_account_address,
                slot.credential_profile_id,
            )
            full_by_public.setdefault(identity, []).append(slot)
        for pilot_slot in pilot_plan.enabled_slots:
            identity = (
                pilot_slot.source_address,
                pilot_slot.follower_account_address,
                pilot_slot.credential_profile_id,
            )
            matches = full_by_public.get(identity, [])
            if len(matches) != 1:
                blockers.append(
                    "full-fleet plan must contain exactly one frozen pilot identity for "
                    f"slot {pilot_slot.slot}"
                )
                continue
            pilot_payload = pilot_slot.to_payload()
            full_payload = matches[0].to_payload()
            permitted_differences = {"enabled"}
            for key, value in pilot_payload.items():
                if key in permitted_differences:
                    continue
                if full_payload.get(key) != value:
                    blockers.append(
                        f"full-fleet pilot identity {pilot_slot.slot} changes frozen field {key}"
                    )
    selected_slots = pilot_plan.enabled_slots if scope == "pilot" else full_slots
    required_profiles = {slot.credential_profile_id for slot in selected_slots}
    missing_profiles = sorted(required_profiles - set(credentials.references))
    if missing_profiles:
        blockers.append("credential references missing for: " + ", ".join(missing_profiles))
    used_refs = [
        credentials.references[profile]
        for profile in sorted(required_profiles)
        if profile in credentials.references
    ]
    if len({ref.api_wallet_address for ref in used_refs}) != len(used_refs):
        blockers.append("each fleet follower requires a distinct API wallet")
    if len({str(ref.api_private_key_file).lower() for ref in used_refs}) != len(used_refs):
        blockers.append("each fleet follower requires a distinct API key file")
    for index, left in enumerate(used_refs):
        for right in used_refs[index + 1 :]:
            try:
                same_file = os.path.samefile(left.api_private_key_file, right.api_private_key_file)
            except OSError:
                same_file = False
            if same_file:
                blockers.append(
                    f"credential profiles {left.profile_id} and {right.profile_id} share one key file identity"
                )
    public_identities = {
        identity
        for slot in selected_slots
        for identity in (slot.source_address, slot.follower_account_address)
    }
    collisions = sorted(
        ref.api_wallet_address for ref in used_refs if ref.api_wallet_address in public_identities
    )
    if collisions:
        blockers.append(
            "API wallet collides with source/follower identity: " + ", ".join(collisions)
        )
    for slot in selected_slots:
        reference = credentials.references.get(slot.credential_profile_id)
        if reference is None:
            continue
        if (
            reference.source_address != slot.source_address
            or reference.follower_account_address != slot.follower_account_address
            or reference.expected_account_mode != slot.expected_account_mode
            or reference.eligibility != slot.eligibility
            or reference.denied_symbols != slot.denied_symbols
        ):
            blockers.append(
                f"credential profile {reference.profile_id} public metadata does not match slot {slot.slot}"
            )
    credential_checks: list[dict[str, Any]] = []
    for reference in used_refs:
        check = _validate_credential_reference(reference, verify_private_key=verify_private_keys)
        credential_checks.append(check)
        blockers.extend(check["blockers"])
    # Credential and public-identity checks above remain scoped to the accounts
    # that can actually launch.  Capacity admission is deliberately different:
    # the pilot must still prove the frozen ten-slot topology without requiring
    # eight future credentials.  The deterministic synthetic workload contains
    # no private material and exercises the reviewed 9/1 worst-case shard split.
    selected_plan = pilot_plan if scope == "pilot" else full_fleet_plan
    try:
        if selected_plan is None:
            raise ValueError("selected fleet plan is missing")
        capacity_workload = build_benchmark_workload(
            scope=scope,
            selected_plan=selected_plan,
        )
    except ValueError:
        # Shape blockers have already been collected above.  Keep validation a
        # structured result for malformed operator input instead of turning it
        # into an exception; no failed shape can reach launch admission.
        capacity_workload = None
    capacity_plan_slots = (
        selected_slots if capacity_workload is None else capacity_workload.plan.enabled_slots
    )
    # Synthetic pilot accounts exercise the reviewed ten-slot CPU, queue, shard,
    # and websocket topology.  They have no credentials and cannot launch, so
    # charging their imagined REST traffic to the two-account pilot makes
    # admission depend on which pilot slot happened to be listed first.  REST
    # admission is instead scoped to the exact launchable plan here and is
    # replaced by an exhaustive live-catalog model at both launch edges.
    assert selected_plan is not None
    rest_model = selected_plan_rest_load_model(
        selected_plan,
        ordinary_rest_budget=ordinary_rest_budget,
        reserve_rest_budget=reserve_rest_budget,
    )
    blockers.extend(rest_model["blockers"])
    ws_model = _websocket_load_model(capacity_plan_slots)
    blockers.extend(ws_model["blockers"])
    selected_profile_ids = tuple(sorted(required_profiles))
    selected_credentials_sha256 = (
        selected_credential_map_sha256(
            credentials=credentials,
            profile_ids=selected_profile_ids,
        )
        if not missing_profiles and selected_profile_ids
        else ""
    )
    selected_plan_sha256 = "" if selected_plan is None else selected_plan.sha256
    provisioning_identity = {
        "scope": scope,
        "selected_plan_sha256": selected_plan_sha256,
        "pilot_plan_sha256": pilot_plan.sha256,
        "full_fleet_plan_sha256": (
            "" if scope == "pilot" or full_fleet_plan is None else full_fleet_plan.sha256
        ),
        "credential_map_sha256": credentials.sha256,
        "selected_credential_map_sha256": selected_credentials_sha256,
        "selected_profile_ids": list(selected_profile_ids),
        "credential_profiles": [
            {
                "profile_id": check["profile_id"],
                "api_wallet_address": check["api_wallet_address"],
                "key_reference": check["key_reference"],
                "profile_sha256": check["profile_sha256"],
            }
            for check in credential_checks
        ],
        "capacity_workload_binding": (
            {} if capacity_workload is None else capacity_workload.binding_payload()
        ),
        "rest_load_model": rest_model,
        "websocket_load_model": ws_model,
    }
    provisioning_identity_sha256 = sha256(
        json.dumps(
            provisioning_identity,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "passed": not blockers,
        "blockers": blockers,
        "warnings": warnings,
        "scope": scope,
        "selected_plan_sha256": selected_plan_sha256,
        "pilot_plan_sha256": pilot_plan.sha256,
        "full_fleet_plan_sha256": (
            "" if scope == "pilot" or full_fleet_plan is None else full_fleet_plan.sha256
        ),
        "credential_map_sha256": credentials.sha256,
        "selected_credential_map_sha256": selected_credentials_sha256,
        "selected_profile_ids": list(selected_profile_ids),
        "pilot_slot_count": len(pilot_plan.enabled_slots),
        "full_fleet_slot_count": len(full_slots),
        "credential_checks": credential_checks,
        "capacity_workload_binding": (
            {} if capacity_workload is None else capacity_workload.binding_payload()
        ),
        "rest_load_model": rest_model,
        "websocket_load_model": ws_model,
        "provisioning_identity_sha256": provisioning_identity_sha256,
    }


def _websocket_load_model(slots: tuple[FleetSlot, ...]) -> dict[str, Any]:
    limits = fleet_connection_limits()
    blockers: list[str] = []
    sources = {slot.source_address for slot in slots}
    source_subscriptions = len(sources) * len(SourceStreamGateway.SUBSCRIPTION_TYPES)
    # This is not merely a planning estimate: FleetRuntime enforces the same
    # cap against the complete union of source, desired, follower, order,
    # inflight, deferred, recovery-fence, and catalog-pending markets.
    maximum_active_markets = active_market_subscription_capacity(
        unique_source_count=len(sources),
        subscription_limit=limits.subscriptions,
    )
    if maximum_active_markets != int(FLEET_RUNTIME_POLICY["maximum_active_markets"]):
        blockers.append("full-fleet active-market cap differs from the reviewed runtime policy")
    # The aggregate strategy uses one allDexsAssetCtxs subscription plus one
    # l2Book subscription per active market.  The active-market strategy uses
    # both activeAssetCtx and l2Book per market.  Admission must budget the
    # larger strategy because the measured launch selector may choose either.
    aggregate_market_subscriptions = 1 + maximum_active_markets
    active_market_subscriptions = 2 * maximum_active_markets
    maximum_market_subscriptions = max(
        aggregate_market_subscriptions,
        active_market_subscriptions,
    )
    subscriptions = (
        source_subscriptions + maximum_market_subscriptions + MARKET_SUBSCRIPTION_CONTROL_HEADROOM
    )
    if len(sources) > limits.unique_users:
        blockers.append(
            f"source unique-user subscriptions exceed the venue limit of {limits.unique_users}"
        )
    if subscriptions > limits.subscriptions:
        blockers.append(
            f"worst-case source/context/book subscriptions exceed {limits.subscriptions}"
        )
    expected_connections = (
        int(FLEET_RUNTIME_POLICY["source_shards"])
        + int(FLEET_RUNTIME_POLICY["market_data_connections"])
        + int(FLEET_RUNTIME_POLICY["action_shards"])
    )
    if expected_connections != limits.normal_connections:
        blockers.append("fleet connection topology does not match its normal connection budget")
    return {
        "connections": expected_connections,
        "bounded_reconnect_overlap": limits.overlap_connections,
        "unique_users": len(sources),
        "source_subscriptions": source_subscriptions,
        "maximum_active_books": maximum_active_markets,
        "active_market_union_scope": (
            "source+desired+follower+orders+inflight+deferred+recovery+catalog_pending"
        ),
        "active_market_union_runtime_enforced": True,
        "aggregate_market_subscriptions": aggregate_market_subscriptions,
        "active_market_subscriptions": active_market_subscriptions,
        "maximum_market_subscriptions": maximum_market_subscriptions,
        "control_subscription_headroom": MARKET_SUBSCRIPTION_CONTROL_HEADROOM,
        "subscriptions": subscriptions,
        "blockers": blockers,
        "passed": not blockers,
    }


def _validate_credential_reference(
    reference: CredentialReference, *, verify_private_key: bool
) -> dict[str, Any]:
    blockers: list[str] = []
    if not reference.api_private_key_file.is_file():
        blockers.append(
            f"credential profile {reference.profile_id} key file is missing: "
            f"{reference.api_private_key_file}"
        )
    derived = ""
    if verify_private_key and not blockers:
        from eth_account import Account

        key = reference.api_private_key_file.read_text(encoding="utf-8").strip()
        try:
            derived = Account.from_key(key).address.lower()
        except Exception as exc:
            blockers.append(
                f"credential profile {reference.profile_id} key is invalid: {type(exc).__name__}"
            )
        if derived and derived != reference.api_wallet_address:
            blockers.append(
                f"credential profile {reference.profile_id} key does not match API wallet"
            )
    return {
        "profile_id": reference.profile_id,
        "api_wallet_address": reference.api_wallet_address,
        "key_reference": reference.api_private_key_file.name,
        "profile_sha256": reference.profile_sha256,
        "derived_address_matches": bool(derived) and derived == reference.api_wallet_address,
        "blockers": blockers,
    }
