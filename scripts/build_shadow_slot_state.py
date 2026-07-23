from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


SLOT_STATE_REPORT_VERSION = 2
SUPPORTED_FOLLOWER_SNAPSHOT_NORMALIZER_VERSION = 2
SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION = 2
DEFAULT_DRIFT_THRESHOLD_USD = Decimal("1")


class SlotStateInputError(RuntimeError):
    """Raised when local slot-state inputs cannot be loaded."""


def build_shadow_slot_state(
    slot_plan_report_path: Path,
    target_snapshot_path: Path,
    follower_snapshot_path: Path,
    *,
    out: Path | None = None,
    source_state_report_path: Path | None = None,
    source_state_facts_path: Path | None = None,
    recovery_evidence_report_path: Path | None = None,
    drift_threshold_usd: Decimal = DEFAULT_DRIFT_THRESHOLD_USD,
) -> dict[str, Any]:
    if drift_threshold_usd < 0:
        raise SlotStateInputError("drift threshold must be non-negative")
    slot_plan_report = read_json_object(slot_plan_report_path, label="slot plan report")
    target_snapshot = read_json_object(target_snapshot_path, label="target snapshot")
    follower_snapshot = read_json_object(follower_snapshot_path, label="follower snapshot")
    source_state_report = (
        read_json_object(source_state_report_path, label="source state report")
        if source_state_report_path is not None
        else None
    )
    source_state_facts = (
        read_jsonl_objects(source_state_facts_path, label="source state facts")
        if source_state_facts_path is not None
        else None
    )
    recovery_evidence_report = (
        read_json_object(recovery_evidence_report_path, label="recovery evidence report")
        if recovery_evidence_report_path is not None
        else None
    )

    report = build_shadow_slot_state_payload(
        slot_plan_report,
        target_snapshot,
        follower_snapshot,
        source_state_report=source_state_report,
        source_state_facts=source_state_facts,
        slot_plan_report_path=slot_plan_report_path,
        target_snapshot_path=target_snapshot_path,
        follower_snapshot_path=follower_snapshot_path,
        source_state_report_path=source_state_report_path,
        source_state_facts_path=source_state_facts_path,
        recovery_evidence_report_path=recovery_evidence_report_path,
        recovery_evidence_report=recovery_evidence_report,
        drift_threshold_usd=drift_threshold_usd,
    )
    if out is not None:
        write_json(out, report)
    return report


def build_shadow_slot_state_payload(
    slot_plan_report: dict[str, Any],
    target_snapshot: dict[str, Any],
    follower_snapshot: dict[str, Any],
    *,
    source_state_report: dict[str, Any] | None = None,
    source_state_facts: list[dict[str, Any]] | None = None,
    recovery_evidence_report: dict[str, Any] | None = None,
    slot_plan_report_path: Path | None = None,
    target_snapshot_path: Path | None = None,
    follower_snapshot_path: Path | None = None,
    source_state_report_path: Path | None = None,
    source_state_facts_path: Path | None = None,
    recovery_evidence_report_path: Path | None = None,
    drift_threshold_usd: Decimal = DEFAULT_DRIFT_THRESHOLD_USD,
) -> dict[str, Any]:
    input_blockers = input_blocker_list(
        slot_plan_report,
        target_snapshot,
        follower_snapshot,
        source_state_report=source_state_report,
        recovery_evidence_report=recovery_evidence_report,
    )
    input_warnings: list[str] = []
    if slot_plan_report.get("warnings"):
        input_warnings.extend(
            f"slot plan warning: {warning}"
            for warning in list_text(slot_plan_report.get("warnings"))
        )
    recovery = recovery_state(follower_snapshot)
    plan_slots = list_items(slot_plan_report.get("slots"))
    target_sources = lower_counter(target_snapshot.get("source_addresses"))
    target_slot_names = set(counter_keys(target_snapshot.get("slots")))
    target_policies = policies_from_target_snapshot(target_snapshot)

    slots: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    decision_counts: Counter[str] = Counter()
    repair_counts: Counter[str] = Counter()
    recovery_gate_counts: Counter[str] = Counter()

    for slot_config in plan_slots:
        slot_state = build_slot_state(
            slot_config,
            target_sources=target_sources,
            target_slot_names=target_slot_names,
            target_policies=target_policies,
            follower_snapshot=follower_snapshot,
            source_state_report=source_state_report,
            source_state_facts=source_state_facts,
            recovery_evidence_report=recovery_evidence_report,
            recovery=recovery,
            drift_threshold_usd=drift_threshold_usd,
            input_blocked=bool(input_blockers),
        )
        slots.append(slot_state)
        status_counts[slot_state["status"]] += 1
        recovery_gate_counts[slot_state["recovery_gate"]["decision"]] += 1
        for policy in slot_state["policies"].values():
            for position in policy["position_states"]:
                decision_counts[position["decision"]] += 1
                repair_counts[position["repair_intent"]] += 1

    report = {
        "slot_state_report_version": SLOT_STATE_REPORT_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "valid": not input_blockers,
        "input_blockers": input_blockers,
        "input_warnings": input_warnings,
        "input_paths": {
            "slot_plan_report": path_str(slot_plan_report_path),
            "target_snapshot": path_str(target_snapshot_path),
            "follower_snapshot": path_str(follower_snapshot_path),
            "source_state_report": path_str(source_state_report_path),
            "source_state_facts": path_str(source_state_facts_path),
            "recovery_evidence_report": path_str(recovery_evidence_report_path),
        },
        "drift_threshold_usd": decimal_str(drift_threshold_usd),
        "recovery_completion": recovery,
        "counts": {
            "slots": len(slots),
            "slot_statuses": counter_dict(status_counts),
            "position_decisions": counter_dict(decision_counts),
            "repair_intents": counter_dict(repair_counts),
            "recovery_gate_decisions": counter_dict(recovery_gate_counts),
            "target_policies": len(target_policies),
            "target_source_addresses": len(target_sources),
            "source_state_attached": source_state_report is not None,
            "source_state_facts_attached": source_state_facts is not None,
            "recovery_evidence_attached": recovery_evidence_report is not None,
            "recovery_evidence_ready": (
                recovery_evidence_report.get("recovery_evidence_ready") is True
                if isinstance(recovery_evidence_report, dict)
                else False
            ),
        },
        "slots": slots,
    }
    return report


def input_blocker_list(
    slot_plan_report: dict[str, Any],
    target_snapshot: dict[str, Any],
    follower_snapshot: dict[str, Any],
    *,
    source_state_report: dict[str, Any] | None = None,
    recovery_evidence_report: dict[str, Any] | None = None,
) -> list[str]:
    blockers: list[str] = []
    if slot_plan_report.get("valid") is not True:
        blockers.append("slot plan report is not valid")
    if slot_plan_report.get("read_only") is not True:
        blockers.append("slot plan report must be read_only=true")
    if slot_plan_report.get("exchange_touched") is not False:
        blockers.append("slot plan report must be exchange_touched=false")
    if not isinstance(slot_plan_report.get("slots"), list):
        blockers.append("slot plan report must include slots")
    if target_snapshot.get("read_only") is not True:
        blockers.append("target snapshot must be read_only=true")
    if target_snapshot.get("exchange_touched") is not False:
        blockers.append("target snapshot must be exchange_touched=false")
    if not isinstance(target_snapshot.get("policies"), dict):
        blockers.append("target snapshot must include policies")
    if follower_snapshot.get("exchange_touched") not in (None, False):
        blockers.append("follower snapshot must be exchange_touched=false when present")
    if not isinstance(follower_snapshot.get("recovery"), dict):
        blockers.append("follower snapshot must include recovery state")
    if source_state_report is not None:
        if source_state_report.get("read_only") is not True:
            blockers.append("source state report must be read_only=true")
        if source_state_report.get("exchange_touched") is not False:
            blockers.append("source state report must be exchange_touched=false")
        if not isinstance(source_state_report.get("counts"), dict):
            blockers.append("source state report must include counts")
    if recovery_evidence_report is not None:
        if (
            recovery_evidence_report.get("recovery_evidence_report_version")
            != SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION
        ):
            blockers.append(
                "recovery evidence report version is unsupported; "
                f"expected {SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION}"
            )
        if recovery_evidence_report.get("read_only") is not True:
            blockers.append("recovery evidence report must be read_only=true")
        if recovery_evidence_report.get("exchange_touched") is not False:
            blockers.append("recovery evidence report must be exchange_touched=false")
        if not isinstance(recovery_evidence_report.get("windows"), list):
            blockers.append("recovery evidence report must include windows")
    return blockers


def build_slot_state(
    slot_config: dict[str, Any],
    *,
    target_sources: dict[str, int],
    target_slot_names: set[str],
    target_policies: dict[str, dict[str, Any]],
    follower_snapshot: dict[str, Any],
    source_state_report: dict[str, Any] | None,
    source_state_facts: list[dict[str, Any]] | None,
    recovery_evidence_report: dict[str, Any] | None,
    recovery: dict[str, Any],
    drift_threshold_usd: Decimal,
    input_blocked: bool,
) -> dict[str, Any]:
    slot_name = clean(slot_config.get("slot"))
    source_address = clean(slot_config.get("source_address")).lower()
    subaccount_address = clean(slot_config.get("subaccount_address")).lower()
    configured_policy = clean(slot_config.get("sizing_policy"))
    slot_blockers: list[str] = []
    slot_warnings: list[str] = []

    target_matches = source_address in target_sources or slot_name in target_slot_names
    if input_blocked:
        slot_blockers.append("one or more input artifacts failed validation")
    if not target_matches:
        slot_blockers.append("no target snapshot matched this slot source or slot name")
    follower_subaccount = clean(follower_snapshot.get("follower_subaccount")).lower()
    follower_slot = clean(follower_snapshot.get("slot"))
    follower_matches_subaccount = follower_subaccount == subaccount_address
    follower_matches_slot = follower_slot == slot_name or follower_slot in target_slot_names
    follower_verification = follower_snapshot_verification(
        follower_snapshot,
        expected_subaccount=subaccount_address,
    )
    if not follower_matches_subaccount:
        slot_warnings.append(
            "follower snapshot subaccount does not match configured subaccount; analysis-only state"
        )
    if not follower_matches_slot:
        slot_warnings.append("follower snapshot slot label does not match configured slot")
    if not follower_verification["verified"]:
        slot_warnings.append("follower snapshot address is not verified; analysis-only state")
    if slot_config.get("subaccount_verified") is not True:
        slot_warnings.append("subaccount is not verified; execution readiness remains false")
    if not recovery["complete"]:
        slot_blockers.append("recovery completion is not proven")
    source_state = source_state_for_slot(
        source_state_report,
        recovery_evidence_report=recovery_evidence_report,
        source_address=source_address,
        slot_name=slot_name,
    )
    if source_state["attached"] and not source_state["matched"]:
        slot_warnings.append("source state report did not match this slot source or slot name")
    if source_state["matched"] and not source_state["recovery_complete"]:
        slot_blockers.append("source state recovery completion is not proven")
    recovery_gate = recovery_gate_for_slot(
        follower_recovery=recovery,
        source_state=source_state,
    )
    sizing = sizing_context(
        slot_config=slot_config,
        follower_snapshot=follower_snapshot,
        source_state=source_state,
    )
    target_intents = target_intents_for_slot(
        source_state_facts,
        source_address=source_address,
        slot_name=slot_name,
        sizing=sizing,
        follower_recovery_complete=recovery["complete"],
        source_recovery_complete=source_state["recovery_complete"],
    )
    if target_intents["attached"] and not target_intents["matched"]:
        slot_warnings.append("source state facts did not match this slot source or slot name")
    source_cashflows = source_cashflows_for_slot(
        source_state_facts,
        source_address=source_address,
        slot_name=slot_name,
    )

    policies: dict[str, Any] = {}
    if target_matches:
        policy_names = sorted(target_policies)
        if configured_policy in target_policies:
            policy_names = [configured_policy]
        for policy_name in policy_names:
            policies[policy_name] = build_policy_state(
                policy_name,
                target_policies[policy_name],
                follower_snapshot,
                recovery=recovery,
                drift_threshold_usd=drift_threshold_usd,
            )

    status = slot_status(
        slot_blockers=slot_blockers,
        policies=policies,
        follower_matches_subaccount=follower_matches_subaccount,
        follower_snapshot_verified=follower_verification["verified"],
        subaccount_verified=slot_config.get("subaccount_verified") is True,
    )
    return {
        "slot": slot_name,
        "source_address": source_address,
        "subaccount_address": subaccount_address,
        "enabled": slot_config.get("enabled") is True,
        "subaccount_verified": slot_config.get("subaccount_verified") is True,
        "configured_sizing_policy": configured_policy,
        "status": status,
        "execution_ready": False,
        "execution_ready_reason": "read-only slot-state report; execution requires separate preflight and operator approval",
        "target_matches_slot": target_matches,
        "follower_matches_subaccount": follower_matches_subaccount,
        "follower_matches_slot": follower_matches_slot,
        "follower_snapshot_verified": follower_verification["verified"],
        "follower_snapshot_verification": follower_verification,
        "blockers": slot_blockers,
        "warnings": slot_warnings,
        "recovery_complete": recovery["complete"],
        "recovery_gate": recovery_gate,
        "source_state": source_state,
        "sizing_context": sizing_context_view(sizing),
        "target_intents": target_intents,
        "source_cashflows": source_cashflows,
        "policies": policies,
    }


def build_policy_state(
    policy_name: str,
    target_policy: dict[str, Any],
    follower_snapshot: dict[str, Any],
    *,
    recovery: dict[str, Any],
    drift_threshold_usd: Decimal,
) -> dict[str, Any]:
    follower = follower_view_for_policy(follower_snapshot, policy=policy_name)
    target_positions = target_positions_by_coin(target_policy)
    coins = sorted(
        set(target_positions)
        | set(follower["positions_by_coin"])
        | set(follower["open_orders_by_coin"])
    )
    position_states = [
        score_position_state(
            coin=coin,
            target_position=target_positions.get(coin, empty_target_position(coin)),
            follower_position=follower["positions_by_coin"].get(coin, Decimal("0")),
            follower_open_orders=follower["open_orders_by_coin"].get(coin, Decimal("0")),
            recovery=recovery,
            drift_threshold_usd=drift_threshold_usd,
        )
        for coin in coins
    ]
    decision_counts = Counter(item["decision"] for item in position_states)
    repair_counts = Counter(item["repair_intent"] for item in position_states)
    nonzero_targets = [
        item
        for item in position_states
        if decimal_optional(item["target"]["signed_target_notional_usd"])
        not in (None, Decimal("0"))
    ]
    return {
        "policy": policy_name,
        "source_rows": int_optional(target_policy.get("source_rows")) or 0,
        "executed_actions": int_optional(target_policy.get("executed_actions")) or 0,
        "dust_actions": int_optional(target_policy.get("dust_actions")) or 0,
        "blocked_actions": int_optional(target_policy.get("blocked_actions")) or 0,
        "skipped_actions": int_optional(target_policy.get("skipped_actions")) or 0,
        "follower_view": follower_view_dict(follower),
        "position_count": len(position_states),
        "nonzero_target_count": len(nonzero_targets),
        "decision_counts": counter_dict(decision_counts),
        "repair_intent_counts": counter_dict(repair_counts),
        "position_states": position_states,
    }


def score_position_state(
    *,
    coin: str,
    target_position: dict[str, Any],
    follower_position: Decimal,
    follower_open_orders: Decimal,
    recovery: dict[str, Any],
    drift_threshold_usd: Decimal,
) -> dict[str, Any]:
    target_notional = decimal_optional(
        target_position.get("signed_target_notional_usd")
    ) or Decimal("0")
    follower_projected = follower_position + follower_open_orders
    projected_drift = target_notional - follower_projected
    position_drift = target_notional - follower_position
    if not recovery["complete"]:
        decision = "blocked_recovery_incomplete"
        repair_intent = "do_not_repair_until_recovery_complete"
        confidence = "low"
        reason = "source backfill, follower refresh, and reconcile completion are not all proven"
    elif abs(projected_drift) <= drift_threshold_usd:
        if abs(position_drift) <= drift_threshold_usd:
            decision = "in_sync"
            repair_intent = "no_repair_needed"
            confidence = "high"
            reason = "follower projected exposure matches target within threshold"
        else:
            decision = "projected_in_sync_position_pending"
            repair_intent = "wait_for_open_orders_or_reconcile"
            confidence = "medium"
            reason = "open orders cover drift but current position is not yet at target"
    else:
        decision = "drift_detected"
        repair_intent = repair_intent_for(target_notional, follower_projected)
        confidence = "medium"
        reason = "follower projected exposure differs from target beyond threshold"
    return {
        "coin": coin,
        "decision": decision,
        "repair_intent": repair_intent,
        "confidence": confidence,
        "reason": reason,
        "target": target_position_view(target_position, target_notional),
        "follower": {
            "position_notional_usd": decimal_str(follower_position),
            "open_order_delta_notional_usd": decimal_str(follower_open_orders),
            "projected_notional_usd": decimal_str(follower_projected),
        },
        "drift": {
            "projected_drift_notional_usd": decimal_str(projected_drift),
            "position_drift_notional_usd": decimal_str(position_drift),
            "abs_projected_drift_notional_usd": decimal_str(abs(projected_drift)),
        },
    }


def slot_status(
    *,
    slot_blockers: list[str],
    policies: dict[str, Any],
    follower_matches_subaccount: bool,
    follower_snapshot_verified: bool,
    subaccount_verified: bool,
) -> str:
    if slot_blockers:
        return "blocked"
    if not policies:
        return "no_target"
    if not follower_matches_subaccount or not follower_snapshot_verified or not subaccount_verified:
        return "analysis_only"
    if any("drift_detected" in policy["decision_counts"] for policy in policies.values()):
        return "drift_detected"
    return "in_sync"


def repair_intent_for(target_notional: Decimal, follower_projected: Decimal) -> str:
    if target_notional == 0 and follower_projected != 0:
        return "would_reduce_or_close_follower_exposure"
    if follower_projected == 0 and target_notional != 0:
        return "would_open_or_place_follower_exposure"
    if signs_conflict(target_notional, follower_projected):
        return "would_reduce_flip_or_flatten_then_reopen"
    if abs(target_notional) > abs(follower_projected):
        return "would_increase_follower_exposure"
    return "would_reduce_follower_exposure"


def policies_from_target_snapshot(target_snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    policies = target_snapshot.get("policies")
    if not isinstance(policies, dict):
        return {}
    return {clean(name): value for name, value in policies.items() if isinstance(value, dict)}


def target_positions_by_coin(policy: dict[str, Any]) -> dict[str, dict[str, Any]]:
    positions = list_items(policy.get("target_positions"))
    return {clean(position.get("coin")): position for position in positions}


def empty_target_position(coin: str) -> dict[str, Any]:
    return {
        "coin": coin,
        "signed_target_notional_usd": "0.00000000",
        "abs_target_notional_usd": "0.00000000",
        "side": "flat",
        "executed_actions": 0,
        "blocked_actions": 0,
        "skipped_actions": 0,
        "dust_actions": 0,
        "source_rows": 0,
        "decision_counts": {},
    }


def target_position_view(position: dict[str, Any], target_notional: Decimal) -> dict[str, Any]:
    return {
        "signed_target_notional_usd": decimal_str(target_notional),
        "abs_target_notional_usd": decimal_str(abs(target_notional)),
        "side": target_side(target_notional),
        "executed_actions": int_optional(position.get("executed_actions")) or 0,
        "blocked_actions": int_optional(position.get("blocked_actions")) or 0,
        "skipped_actions": int_optional(position.get("skipped_actions")) or 0,
        "dust_actions": int_optional(position.get("dust_actions")) or 0,
        "source_rows": int_optional(position.get("source_rows")) or 0,
        "decision_counts": dict_value(position.get("decision_counts")),
    }


def follower_view_for_policy(snapshot: dict[str, Any], *, policy: str) -> dict[str, Any]:
    policy_payload = snapshot
    policies = snapshot.get("policies")
    if isinstance(policies, dict) and isinstance(policies.get(policy), dict):
        policy_payload = {**snapshot, **policies[policy]}
    positions = list_items(policy_payload.get("positions"))
    open_orders = list_items(policy_payload.get("open_orders"))
    return {
        "policy": policy,
        "account_value_usd": decimal_optional(policy_payload.get("account_value_usd")),
        "positions_by_coin": aggregate_positions(positions),
        "open_orders_by_coin": aggregate_open_orders(open_orders),
        "raw_position_count": len(positions),
        "raw_open_order_count": len(open_orders),
    }


def follower_view_dict(view: dict[str, Any]) -> dict[str, Any]:
    projected: defaultdict[str, Decimal] = defaultdict(Decimal)
    for coin, value in view["positions_by_coin"].items():
        projected[coin] += value
    for coin, value in view["open_orders_by_coin"].items():
        projected[coin] += value
    return {
        "policy": view["policy"],
        "account_value_usd": decimal_str(view["account_value_usd"]),
        "positions_by_coin": decimal_map(view["positions_by_coin"]),
        "open_orders_by_coin": decimal_map(view["open_orders_by_coin"]),
        "projected_by_coin": decimal_map(dict(projected)),
        "raw_position_count": view["raw_position_count"],
        "raw_open_order_count": view["raw_open_order_count"],
    }


def aggregate_positions(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows:
        coin = clean(row.get("coin"))
        signed = decimal_optional(row.get("signed_notional_usd"))
        if signed is None:
            notional = decimal_optional(row.get("notional_usd")) or Decimal("0")
            signed = signed_amount(notional, row.get("side") or row.get("direction"))
        result[coin] += signed
    return dict(result)


def aggregate_open_orders(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = defaultdict(Decimal)
    for row in rows:
        coin = clean(row.get("coin"))
        signed = decimal_optional(row.get("signed_notional_usd"))
        if signed is None:
            notional = decimal_optional(row.get("notional_usd")) or Decimal("0")
            signed = signed_amount(notional, row.get("side") or row.get("direction"))
        result[coin] += signed
    return dict(result)


def recovery_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    recovery = dict_value(snapshot.get("recovery"))
    raw_version = snapshot.get("snapshot_normalizer_version")
    normalizer_version = (
        raw_version if isinstance(raw_version, int) and not isinstance(raw_version, bool) else None
    )
    request_status = dict_value(snapshot.get("request_status"))
    positions = snapshot.get("positions")
    open_orders = snapshot.get("open_orders")
    counts = dict_value(snapshot.get("counts"))
    validation_warnings = [
        warning
        for warning in list_text(snapshot.get("warnings"))
        if "validation failed" in warning.lower()
        or "follower refresh incomplete" in warning.lower()
    ]
    requirements = {
        "snapshot_normalizer_supported": (
            normalizer_version == SUPPORTED_FOLLOWER_SNAPSHOT_NORMALIZER_VERSION
        ),
        "request_status_valid": (
            request_status.get("clearinghouseState_ok") is True
            and request_status.get("openOrders_ok") is True
        ),
        "snapshot_shape_valid": (
            isinstance(positions, list)
            and all(isinstance(item, dict) for item in positions)
            and isinstance(open_orders, list)
            and all(isinstance(item, dict) for item in open_orders)
            and counts.get("positions") == len(positions)
            and counts.get("open_orders") == len(open_orders)
        ),
        "validation_warnings_clear": not validation_warnings,
        "source_backfill_complete": recovery.get("source_backfill_complete") is True,
        "follower_refresh_complete": recovery.get("follower_refresh_complete") is True,
        "reconcile_complete": recovery.get("reconcile_complete") is True,
    }
    missing = [key for key, value in requirements.items() if not value]
    return {
        **requirements,
        "complete": not missing,
        "missing_requirements": missing,
        "snapshot_normalizer_version": normalizer_version,
        "validation_warnings": validation_warnings,
        "notes": clean(recovery.get("notes")),
    }


def follower_snapshot_verification(
    snapshot: dict[str, Any],
    *,
    expected_subaccount: str,
) -> dict[str, Any]:
    verification = dict_value(snapshot.get("address_verification"))
    expected = expected_subaccount.lower()
    declared_expected = clean(verification.get("expected_follower_subaccount")).lower()
    observed = clean(verification.get("observed_address")).lower()
    verified = (
        snapshot.get("follower_subaccount_verified") is True
        and verification.get("verified") is True
        and declared_expected == expected
        and observed == expected
    )
    if verified:
        reason = "follower snapshot observed address matches configured subaccount"
    elif not verification:
        reason = "follower snapshot lacks address verification"
    elif declared_expected != expected:
        reason = "follower snapshot was verified against a different expected subaccount"
    elif observed != expected:
        reason = "follower snapshot observed address does not match configured subaccount"
    else:
        reason = clean(verification.get("reason"))
    return {
        "verified": verified,
        "expected_subaccount": expected,
        "declared_expected_follower_subaccount": (
            declared_expected if declared_expected != "unknown" else None
        ),
        "observed_address": observed if observed != "unknown" else None,
        "observed_address_source": (
            clean(verification.get("observed_address_source"))
            if verification.get("observed_address_source")
            else None
        ),
        "reason": reason,
    }


def recovery_gate_for_slot(
    *,
    follower_recovery: dict[str, Any],
    source_state: dict[str, Any],
) -> dict[str, Any]:
    source_requirements = dict_value(source_state.get("recovery_requirements"))
    follower_missing = [
        f"follower.{key}" for key in list_text(follower_recovery.get("missing_requirements"))
    ]
    source_missing = [
        f"source.{key}" for key in list_text(source_requirements.get("missing_requirements"))
    ]
    missing = follower_missing + source_missing
    complete = not missing
    decision = "recovery_complete" if complete else "blocked_recovery_incomplete"
    return {
        "complete": complete,
        "decision": decision,
        "repair_intents_actionable": complete,
        "actionability_scope": (
            "replay/shadow repair intent eligibility only; exchange execution still requires "
            "separate preflight, verified subaccount, canary evidence, and operator approval"
        ),
        "missing_requirements": missing,
        "blocker_reasons": [recovery_requirement_reason(item) for item in missing],
        "follower": {
            "source_backfill_complete": follower_recovery.get("source_backfill_complete") is True,
            "follower_refresh_complete": follower_recovery.get("follower_refresh_complete") is True,
            "reconcile_complete": follower_recovery.get("reconcile_complete") is True,
            "complete": follower_recovery.get("complete") is True,
            "missing_requirements": list_text(follower_recovery.get("missing_requirements")),
            "notes": clean(follower_recovery.get("notes")),
        },
        "source": source_requirements,
    }


def recovery_requirement_reason(requirement: str) -> str:
    reasons = {
        "follower.source_backfill_complete": (
            "follower snapshot does not prove source backfill completion"
        ),
        "follower.follower_refresh_complete": (
            "follower snapshot does not prove follower refresh completion"
        ),
        "follower.reconcile_complete": (
            "follower snapshot does not prove source/follower reconciliation completion"
        ),
        "source.source_state_report_attached": "source state report is not attached",
        "source.source_state_report_matched": (
            "source state report does not match this slot source or slot name"
        ),
        "source.pending_rest_backfill_cleared": (
            "source state still requires REST backfill after reconnect"
        ),
        "source.pending_reconcile_cleared": (
            "source state still requires reconciliation after reconnect"
        ),
        "source.live_stream_hints_allowed": (
            "source live stream hints are not trusted after reconnect"
        ),
    }
    return reasons.get(requirement, f"{requirement} is not proven")


def source_state_for_slot(
    source_state_report: dict[str, Any] | None,
    *,
    recovery_evidence_report: dict[str, Any] | None,
    source_address: str,
    slot_name: str,
) -> dict[str, Any]:
    recovery_evidence = recovery_evidence_for_source(
        recovery_evidence_report,
        source_address=source_address,
    )
    if source_state_report is None:
        return {
            "attached": False,
            "matched": False,
            "recovery_complete": False,
            "recovery_requirements": source_recovery_requirements(
                attached=False,
                matched=False,
                recovery=None,
                recovery_evidence=recovery_evidence,
            ),
            "recovery_evidence": recovery_evidence,
            "summary": None,
        }
    counts = dict_value(source_state_report.get("counts"))
    by_address = lower_counter(counts.get("by_address"))
    report_slot = clean(source_state_report.get("slot"))
    matched = source_address in by_address or report_slot == slot_name
    if not matched:
        return {
            "attached": True,
            "matched": False,
            "recovery_complete": False,
            "recovery_requirements": source_recovery_requirements(
                attached=True,
                matched=False,
                recovery=None,
                recovery_evidence=recovery_evidence,
            ),
            "recovery_evidence": recovery_evidence,
            "summary": None,
        }
    recovery = dict_value(source_state_report.get("recovery"))
    requirements = source_recovery_requirements(
        attached=True,
        matched=True,
        recovery=recovery,
        recovery_evidence=recovery_evidence,
    )
    recovery_complete = requirements["complete"]
    return {
        "attached": True,
        "matched": True,
        "recovery_complete": recovery_complete,
        "recovery_requirements": requirements,
        "recovery_evidence": recovery_evidence,
        "summary": source_state_summary(source_state_report, recovery_complete=recovery_complete),
    }


def recovery_evidence_for_source(
    recovery_evidence_report: dict[str, Any] | None,
    *,
    source_address: str,
) -> dict[str, Any]:
    if recovery_evidence_report is None:
        return {
            "attached": False,
            "read_only": False,
            "exchange_touched": False,
            "status": "not_attached",
            "recovery_evidence_report_version": None,
            "version_supported": False,
            "recovery_evidence_ready": False,
            "source_address": "",
            "source_matches": False,
            "window_count": 0,
            "incomplete_window_count": 0,
            "blockers": [],
            "applied_to_source_recovery": False,
            "reason": "recovery evidence report is not attached",
        }
    read_only = recovery_evidence_report.get("read_only") is True
    exchange_touched = recovery_evidence_report.get("exchange_touched") is True
    raw_version = recovery_evidence_report.get("recovery_evidence_report_version")
    report_version = (
        raw_version if isinstance(raw_version, int) and not isinstance(raw_version, bool) else None
    )
    version_supported = report_version == SUPPORTED_RECOVERY_EVIDENCE_REPORT_VERSION
    ready = recovery_evidence_report.get("recovery_evidence_ready") is True
    source_state = dict_value(recovery_evidence_report.get("source_state"))
    evidence_source = clean(source_state.get("source_address")).lower()
    source_matches = evidence_source == source_address.lower()
    windows = list_items(recovery_evidence_report.get("windows"))
    incomplete_count = sum(1 for window in windows if window.get("complete") is not True)
    applied = read_only and not exchange_touched and version_supported and ready and source_matches
    if applied:
        reason = "recovery evidence is ready and source-matched"
    elif not read_only or exchange_touched:
        reason = "recovery evidence report is not read-only"
    elif not version_supported:
        reason = "recovery evidence report version is unsupported"
    elif not source_matches:
        reason = "recovery evidence source does not match this slot"
    elif not ready:
        reason = "recovery evidence report is not ready"
    else:
        reason = "recovery evidence is not applicable"
    return {
        "attached": True,
        "read_only": read_only,
        "exchange_touched": exchange_touched,
        "status": clean(recovery_evidence_report.get("status")),
        "recovery_evidence_report_version": report_version,
        "version_supported": version_supported,
        "recovery_evidence_ready": ready,
        "source_address": evidence_source,
        "source_matches": source_matches,
        "window_count": len(windows),
        "incomplete_window_count": incomplete_count,
        "blockers": list_text(recovery_evidence_report.get("blockers")),
        "applied_to_source_recovery": applied,
        "reason": reason,
    }


def source_recovery_requirements(
    *,
    attached: bool,
    matched: bool,
    recovery: dict[str, Any] | None,
    recovery_evidence: dict[str, Any],
) -> dict[str, Any]:
    pending_backfill_cleared = False
    pending_reconcile_cleared = False
    live_stream_hints_allowed = False
    if recovery is not None:
        pending_backfill_cleared = recovery.get("pending_rest_backfill") is not True
        pending_reconcile_cleared = recovery.get("pending_reconcile") is not True
        live_stream_hints_allowed = recovery.get("live_stream_hints_allowed") is True
    if recovery_evidence.get("applied_to_source_recovery") is True:
        pending_backfill_cleared = True
        pending_reconcile_cleared = True
        live_stream_hints_allowed = True
    requirements = {
        "source_state_report_attached": attached,
        "source_state_report_matched": matched,
        "pending_rest_backfill_cleared": pending_backfill_cleared,
        "pending_reconcile_cleared": pending_reconcile_cleared,
        "live_stream_hints_allowed": live_stream_hints_allowed,
    }
    missing = [key for key, value in requirements.items() if not value]
    return {
        **requirements,
        "recovery_evidence_applied": recovery_evidence.get("applied_to_source_recovery") is True,
        "complete": not missing,
        "missing_requirements": missing,
    }


def source_state_summary(
    source_state_report: dict[str, Any],
    *,
    recovery_complete: bool,
) -> dict[str, Any]:
    orders = dict_value(source_state_report.get("orders"))
    twaps = dict_value(source_state_report.get("twaps"))
    fills = dict_value(source_state_report.get("fills"))
    account_context = dict_value(source_state_report.get("account_context"))
    recovery = dict_value(source_state_report.get("recovery"))
    policy_neutral = dict_value(source_state_report.get("policy_neutral_shadow"))
    return {
        "rows_processed": int_optional(source_state_report.get("rows_processed")) or 0,
        "input_rows_seen": int_optional(source_state_report.get("input_rows_seen")) or 0,
        "orders": {
            "seen": int_optional(orders.get("seen")) or 0,
            "open": int_optional(orders.get("open")) or 0,
            "terminal": int_optional(orders.get("terminal")) or 0,
            "by_status": dict_value(orders.get("by_status")),
            "unmatched_terminal_updates": int_optional(orders.get("unmatched_terminal_updates"))
            or 0,
            "open_order_sample_count": len(list_items(orders.get("open_order_samples"))),
        },
        "twaps": {
            "seen": int_optional(twaps.get("seen")) or 0,
            "active": int_optional(twaps.get("active")) or 0,
            "terminal": int_optional(twaps.get("terminal")) or 0,
            "by_status": dict_value(twaps.get("by_status")),
            "slice_fills": int_optional(twaps.get("slice_fills")) or 0,
            "unmatched_terminal_updates": int_optional(twaps.get("unmatched_terminal_updates"))
            or 0,
            "active_sample_count": len(list_items(twaps.get("active_samples"))),
            "error_sample_count": len(list_items(twaps.get("error_samples"))),
        },
        "fills": {
            "source_fills": int_optional(fills.get("source_fills")) or 0,
            "source_fill_notional_usd": decimal_str(
                decimal_optional(fills.get("source_fill_notional_usd"))
            ),
            "source_fill_notional_observations": int_optional(
                fills.get("source_fill_notional_observations")
            )
            or 0,
            "twap_slice_fills": int_optional(fills.get("twap_slice_fills")) or 0,
            "twap_slice_notional_usd": decimal_str(
                decimal_optional(fills.get("twap_slice_notional_usd"))
            ),
            "twap_slice_notional_observations": int_optional(
                fills.get("twap_slice_notional_observations")
            )
            or 0,
        },
        "account_context": {
            "equity_confidence": clean(account_context.get("equity_confidence")),
            "equity_confidence_reason": clean(account_context.get("equity_confidence_reason")),
            "latest_account_value_usd": decimal_str(
                decimal_optional(account_context.get("latest_account_value_usd"))
            ),
            "latest_account_value_ts_ms": int_optional(
                account_context.get("latest_account_value_ts_ms")
            ),
            "account_value_observations": int_optional(
                account_context.get("account_value_observations")
            )
            or 0,
            "confidence_downgrade_reasons": dict_value(
                account_context.get("confidence_downgrade_reasons")
            ),
            "rest_snapshots": int_optional(account_context.get("rest_snapshots")) or 0,
            "twap_state_refreshes": int_optional(account_context.get("twap_state_refreshes")) or 0,
            "account_state_updates_by_action": dict_value(
                account_context.get("account_state_updates_by_action")
            ),
            "ledger_updates_by_type": dict_value(account_context.get("ledger_updates_by_type")),
            "funding_updates_by_coin": dict_value(account_context.get("funding_updates_by_coin")),
            "ledger_amount_usd": decimal_str(
                decimal_optional(account_context.get("ledger_amount_usd"))
            ),
            "ledger_amount_observations": int_optional(
                account_context.get("ledger_amount_observations")
            )
            or 0,
            "funding_amount_usd": decimal_str(
                decimal_optional(account_context.get("funding_amount_usd"))
            ),
            "funding_amount_observations": int_optional(
                account_context.get("funding_amount_observations")
            )
            or 0,
            "net_account_context_amount_usd": decimal_str(
                decimal_optional(account_context.get("net_account_context_amount_usd"))
            ),
            "position_snapshot_observations": int_optional(
                account_context.get("position_snapshot_observations")
            )
            or 0,
            "latest_position_count": int_optional(account_context.get("latest_position_count"))
            or 0,
            "latest_position_coins": list_text(account_context.get("latest_position_coins")),
            "latest_position_leverage_by_coin": dict_value(
                account_context.get("latest_position_leverage_by_coin")
            ),
            "latest_position_leverage_counts": dict_value(
                account_context.get("latest_position_leverage_counts")
            ),
            "latest_position_notional_usd": decimal_str(
                decimal_optional(account_context.get("latest_position_notional_usd"))
            ),
            "latest_position_notional_observations": int_optional(
                account_context.get("latest_position_notional_observations")
            )
            or 0,
            "latest_position_margin_used_usd": decimal_str(
                decimal_optional(account_context.get("latest_position_margin_used_usd"))
            ),
            "latest_position_margin_used_observations": int_optional(
                account_context.get("latest_position_margin_used_observations")
            )
            or 0,
            "latest_position_unrealized_pnl_usd": decimal_str(
                decimal_optional(account_context.get("latest_position_unrealized_pnl_usd"))
            ),
            "latest_position_unrealized_pnl_observations": int_optional(
                account_context.get("latest_position_unrealized_pnl_observations")
            )
            or 0,
        },
        "recovery": {
            "complete": recovery_complete,
            "stream_state": clean(recovery.get("stream_state")),
            "live_stream_hints_allowed": recovery.get("live_stream_hints_allowed") is True,
            "pending_rest_backfill": recovery.get("pending_rest_backfill") is True,
            "pending_reconcile": recovery.get("pending_reconcile") is True,
            "degraded_events": int_optional(recovery.get("degraded_events")) or 0,
            "reconnect_recovered_events": int_optional(recovery.get("reconnect_recovered_events"))
            or 0,
            "state_refreshes_after_reconnect": int_optional(
                recovery.get("state_refreshes_after_reconnect")
            )
            or 0,
            "windows": recovery_window_summary(recovery.get("windows")),
        },
        "policy_neutral_shadow": {
            "facts_emitted": int_optional(policy_neutral.get("facts_emitted")) or 0,
            "fill_price_size_available": policy_neutral.get("fill_price_size_available") is True,
            "latest_source_account_value_available": (
                policy_neutral.get("latest_source_account_value_available") is True
            ),
            "sizing_policy_applied": policy_neutral.get("sizing_policy_applied") is True,
        },
    }


def recovery_window_summary(value: Any) -> dict[str, Any]:
    windows = list_items(value)
    incomplete = [window for window in windows if window.get("complete") is not True]
    missing_counts: Counter[str] = Counter()
    max_gap_ms: int | None = None
    sample_windows: list[dict[str, Any]] = []
    for window in windows:
        gap_ms = int_optional(window.get("gap_ms"))
        if gap_ms is not None and (max_gap_ms is None or gap_ms > max_gap_ms):
            max_gap_ms = gap_ms
        for requirement in list_text(window.get("missing_requirements")):
            missing_counts[requirement] += 1
        if len(sample_windows) < 3:
            sample_windows.append(
                {
                    "sequence": int_optional(window.get("sequence")),
                    "status": clean(window.get("status")),
                    "complete": window.get("complete") is True,
                    "degraded_ts_ms": int_optional(window.get("degraded_ts_ms")),
                    "reconnected_ts_ms": int_optional(window.get("reconnected_ts_ms")),
                    "gap_ms": gap_ms,
                    "post_reconnect_state_refreshes": int_optional(
                        window.get("post_reconnect_state_refreshes")
                    )
                    or 0,
                    "post_reconnect_rest_snapshots": int_optional(
                        window.get("post_reconnect_rest_snapshots")
                    )
                    or 0,
                    "post_reconnect_account_context_rows": int_optional(
                        window.get("post_reconnect_account_context_rows")
                    )
                    or 0,
                    "post_reconnect_source_actions": int_optional(
                        window.get("post_reconnect_source_actions")
                    )
                    or 0,
                    "first_post_reconnect_state_refresh_ms": int_optional(
                        window.get("first_post_reconnect_state_refresh_ms")
                    ),
                    "first_post_reconnect_rest_snapshot_ms": int_optional(
                        window.get("first_post_reconnect_rest_snapshot_ms")
                    ),
                    "missing_requirements": list_text(window.get("missing_requirements")),
                }
            )
    return {
        "count": len(windows),
        "complete": len(windows) - len(incomplete),
        "incomplete": len(incomplete),
        "max_gap_ms": max_gap_ms,
        "missing_requirements": counter_dict(missing_counts),
        "sample": sample_windows,
    }


def source_cashflows_for_slot(
    source_state_facts: list[dict[str, Any]] | None,
    *,
    source_address: str,
    slot_name: str,
) -> dict[str, Any]:
    if source_state_facts is None:
        return {
            "attached": False,
            "matched": False,
            "matched_fact_rows": 0,
            "relevant_fact_rows": 0,
            "fill_cashflows": empty_fill_cashflows(),
            "account_context_events": empty_account_context_events(),
        }
    matched_rows = 0
    fill_fact_counts: Counter[str] = Counter()
    fill_coin_counts: Counter[str] = Counter()
    account_action_counts: Counter[str] = Counter()
    ledger_type_counts: Counter[str] = Counter()
    funding_coin_counts: Counter[str] = Counter()
    closed_pnl = Decimal("0")
    fee = Decimal("0")
    source_notional = Decimal("0")
    ledger_amount = Decimal("0")
    funding_amount = Decimal("0")
    closed_pnl_observations = 0
    fee_observations = 0
    source_notional_observations = 0
    ledger_amount_observations = 0
    funding_amount_observations = 0

    for fact in source_state_facts:
        if not fact_matches_slot(fact, source_address=source_address, slot_name=slot_name):
            continue
        matched_rows += 1
        fact_type = clean(fact.get("fact_type"))
        metadata = dict_value(fact.get("metadata"))
        if fact_type in {"source_fill_seen", "twap_slice_fill_seen"}:
            fill_fact_counts[fact_type] += 1
            fill_coin_counts[clean(metadata.get("coin"))] += 1
            observed_closed_pnl = decimal_optional(metadata.get("closed_pnl_usd"))
            observed_fee = decimal_optional(metadata.get("fee_usd"))
            observed_notional = decimal_optional(metadata.get("source_notional_usd"))
            if observed_closed_pnl is not None:
                closed_pnl += observed_closed_pnl
                closed_pnl_observations += 1
            if observed_fee is not None:
                fee += observed_fee
                fee_observations += 1
            if observed_notional is not None:
                source_notional += abs(observed_notional)
                source_notional_observations += 1
        elif fact_type == "account_context_update":
            action = clean(metadata.get("source_action"))
            account_action_counts[action] += 1
            if action == "funding_update":
                funding_coin_counts[clean(metadata.get("coin"))] += 1
                observed_funding_amount = decimal_optional(metadata.get("funding_amount_usd"))
                if observed_funding_amount is None:
                    observed_funding_amount = decimal_optional(metadata.get("usdc"))
                if observed_funding_amount is not None:
                    funding_amount += observed_funding_amount
                    funding_amount_observations += 1
            if action.startswith("ledger_"):
                ledger_type_counts[clean(metadata.get("ledger_type"))] += 1
                observed_ledger_amount = decimal_optional(metadata.get("ledger_amount_usd"))
                if observed_ledger_amount is None:
                    observed_ledger_amount = decimal_optional(metadata.get("usdc"))
                if observed_ledger_amount is not None:
                    ledger_amount += observed_ledger_amount
                    ledger_amount_observations += 1

    relevant_rows = sum(fill_fact_counts.values()) + sum(account_action_counts.values())
    return {
        "attached": True,
        "matched": matched_rows > 0,
        "matched_fact_rows": matched_rows,
        "relevant_fact_rows": relevant_rows,
        "fill_cashflows": {
            "fact_rows": sum(fill_fact_counts.values()),
            "by_fact_type": counter_dict(fill_fact_counts),
            "by_coin": counter_dict(fill_coin_counts),
            "source_notional_usd": decimal_str(source_notional),
            "source_notional_observations": source_notional_observations,
            "closed_pnl_usd": decimal_str(closed_pnl),
            "closed_pnl_observations": closed_pnl_observations,
            "fee_usd": decimal_str(fee),
            "fee_observations": fee_observations,
            "closed_pnl_less_recorded_fee_usd": decimal_str(closed_pnl - fee),
        },
        "account_context_events": {
            "fact_rows": sum(account_action_counts.values()),
            "by_source_action": counter_dict(account_action_counts),
            "ledger_updates_by_type": counter_dict(ledger_type_counts),
            "funding_updates_by_coin": counter_dict(funding_coin_counts),
            "ledger_amount_usd": decimal_str(ledger_amount),
            "ledger_amount_observations": ledger_amount_observations,
            "funding_amount_usd": decimal_str(funding_amount),
            "funding_amount_observations": funding_amount_observations,
            "net_account_context_amount_usd": decimal_str(ledger_amount + funding_amount),
            "ledger_amounts_available": ledger_amount_observations > 0,
            "funding_amounts_available": funding_amount_observations > 0,
            "amounts_unavailable_reason": account_amounts_unavailable_reason(
                ledger_amount_observations,
                funding_amount_observations,
            ),
        },
    }


def empty_fill_cashflows() -> dict[str, Any]:
    return {
        "fact_rows": 0,
        "by_fact_type": {},
        "by_coin": {},
        "source_notional_usd": "0.00000000",
        "source_notional_observations": 0,
        "closed_pnl_usd": "0.00000000",
        "closed_pnl_observations": 0,
        "fee_usd": "0.00000000",
        "fee_observations": 0,
        "closed_pnl_less_recorded_fee_usd": "0.00000000",
    }


def empty_account_context_events() -> dict[str, Any]:
    return {
        "fact_rows": 0,
        "by_source_action": {},
        "ledger_updates_by_type": {},
        "funding_updates_by_coin": {},
        "ledger_amount_usd": "0.00000000",
        "ledger_amount_observations": 0,
        "funding_amount_usd": "0.00000000",
        "funding_amount_observations": 0,
        "net_account_context_amount_usd": "0.00000000",
        "ledger_amounts_available": False,
        "funding_amounts_available": False,
        "amounts_unavailable_reason": "source-state facts are not attached",
    }


def account_amounts_unavailable_reason(
    ledger_amount_observations: int,
    funding_amount_observations: int,
) -> str:
    missing = []
    if ledger_amount_observations == 0:
        missing.append("ledger")
    if funding_amount_observations == 0:
        missing.append("funding")
    if not missing:
        return ""
    return f"{'/'.join(missing)} amount fields were not present in matched source-state facts"


def target_intents_for_slot(
    source_state_facts: list[dict[str, Any]] | None,
    *,
    source_address: str,
    slot_name: str,
    sizing: dict[str, Any],
    follower_recovery_complete: bool,
    source_recovery_complete: bool,
) -> dict[str, Any]:
    if source_state_facts is None:
        return {
            "attached": False,
            "matched": False,
            "matched_fact_rows": 0,
            "orders": [],
            "twaps": [],
            "counts": {},
        }
    orders: dict[str, dict[str, Any]] = {}
    twaps: dict[str, dict[str, Any]] = {}
    matched_rows = 0
    for fact in source_state_facts:
        if not fact_matches_slot(fact, source_address=source_address, slot_name=slot_name):
            continue
        matched_rows += 1
        fact_type = clean(fact.get("fact_type"))
        metadata = dict_value(fact.get("metadata"))
        if fact_type in {"order_open_seen", "order_terminal_seen"}:
            observe_order_intent(orders, fact=fact, fact_type=fact_type, metadata=metadata)
        elif fact_type in {
            "twap_activated_seen",
            "twap_slice_fill_seen",
            "twap_state_refresh_seen",
            "twap_terminal_seen",
        }:
            observe_twap_intent(twaps, fact=fact, fact_type=fact_type, metadata=metadata)

    order_rows = [
        finalize_order_intent(
            order,
            sizing=sizing,
            follower_recovery_complete=follower_recovery_complete,
            source_recovery_complete=source_recovery_complete,
        )
        for order in sorted(orders.values(), key=lambda item: (item["first_seen_ms"], item["key"]))
    ]
    twap_rows = [
        finalize_twap_intent(
            twap,
            sizing=sizing,
            follower_recovery_complete=follower_recovery_complete,
            source_recovery_complete=source_recovery_complete,
        )
        for twap in sorted(twaps.values(), key=lambda item: (item["first_seen_ms"], item["key"]))
    ]
    decision_counts = Counter(row["decision"] for row in order_rows + twap_rows)
    action_counts = Counter(row["target_action"] for row in order_rows + twap_rows)
    sizing_counts = Counter(row["sizing"]["status"] for row in order_rows + twap_rows)
    return {
        "attached": True,
        "matched": matched_rows > 0,
        "matched_fact_rows": matched_rows,
        "orders": order_rows,
        "twaps": twap_rows,
        "counts": {
            "orders": len(order_rows),
            "twaps": len(twap_rows),
            "decisions": counter_dict(decision_counts),
            "target_actions": counter_dict(action_counts),
            "sizing_statuses": counter_dict(sizing_counts),
        },
    }


def observe_order_intent(
    orders: dict[str, dict[str, Any]],
    *,
    fact: dict[str, Any],
    fact_type: str,
    metadata: dict[str, Any],
) -> None:
    key = clean(fact.get("entity_id") or metadata.get("order_key"))
    row = orders.setdefault(
        key,
        {
            "key": key,
            "coin": clean(metadata.get("coin")),
            "side": clean(metadata.get("side")),
            "status": "unknown",
            "source_notional_usd": None,
            "opened_count": 0,
            "terminal_count": 0,
            "source_fill_events": 0,
            "first_seen_ms": int_optional(fact.get("sort_ts_ms")) or 0,
            "last_seen_ms": int_optional(fact.get("sort_ts_ms")) or 0,
            "fact_types": Counter(),
        },
    )
    row["coin"] = prefer_known(row["coin"], metadata.get("coin"))
    row["side"] = prefer_known(row["side"], metadata.get("side"))
    row["status"] = lifecycle_status(
        row["status"], metadata.get("status"), terminal=fact_type == "order_terminal_seen"
    )
    row["source_notional_usd"] = prefer_decimal(
        row["source_notional_usd"],
        metadata.get("source_notional_usd"),
    )
    row["opened_count"] = max(row["opened_count"], int_optional(metadata.get("opened_count")) or 0)
    row["terminal_count"] = max(
        row["terminal_count"],
        int_optional(metadata.get("terminal_count"))
        or (1 if fact_type == "order_terminal_seen" else 0),
    )
    row["source_fill_events"] = max(
        row["source_fill_events"],
        int_optional(metadata.get("source_fill_events")) or 0,
    )
    observed_ms = int_optional(fact.get("sort_ts_ms")) or row["first_seen_ms"]
    row["first_seen_ms"] = min(row["first_seen_ms"], observed_ms)
    row["last_seen_ms"] = max(row["last_seen_ms"], observed_ms)
    row["fact_types"][fact_type] += 1


def observe_twap_intent(
    twaps: dict[str, dict[str, Any]],
    *,
    fact: dict[str, Any],
    fact_type: str,
    metadata: dict[str, Any],
) -> None:
    key = clean(fact.get("entity_id") or metadata.get("twap_key"))
    row = twaps.setdefault(
        key,
        {
            "key": key,
            "coin": clean(metadata.get("coin")),
            "side": clean(metadata.get("side")),
            "reduce_only": metadata.get("reduce_only") is True,
            "status": "unknown",
            "activated_count": 0,
            "terminal_count": 0,
            "slice_fill_count": 0,
            "source_notional_usd": Decimal("0"),
            "source_notional_observations": 0,
            "state_refresh_count": 0,
            "first_seen_ms": int_optional(fact.get("sort_ts_ms")) or 0,
            "last_seen_ms": int_optional(fact.get("sort_ts_ms")) or 0,
            "fact_types": Counter(),
        },
    )
    row["coin"] = prefer_known(row["coin"], metadata.get("coin"))
    row["side"] = prefer_known(row["side"], metadata.get("side"))
    row["status"] = lifecycle_status(
        row["status"], metadata.get("status"), terminal=fact_type == "twap_terminal_seen"
    )
    row["reduce_only"] = row["reduce_only"] or metadata.get("reduce_only") is True
    row["activated_count"] = max(
        row["activated_count"],
        int_optional(metadata.get("activated_count")) or 0,
    )
    row["terminal_count"] = max(
        row["terminal_count"],
        int_optional(metadata.get("terminal_count"))
        or (1 if fact_type == "twap_terminal_seen" else 0),
    )
    row["slice_fill_count"] = max(
        row["slice_fill_count"],
        int_optional(metadata.get("slice_fill_count"))
        or (1 if fact_type == "twap_slice_fill_seen" else 0),
    )
    if fact_type == "twap_slice_fill_seen":
        source_notional = decimal_optional(metadata.get("source_notional_usd"))
        if source_notional is not None:
            row["source_notional_usd"] += abs(source_notional)
            row["source_notional_observations"] += 1
    row["state_refresh_count"] = max(
        row["state_refresh_count"],
        int_optional(metadata.get("state_refresh_count"))
        or (1 if fact_type == "twap_state_refresh_seen" else 0),
    )
    observed_ms = int_optional(fact.get("sort_ts_ms")) or row["first_seen_ms"]
    row["first_seen_ms"] = min(row["first_seen_ms"], observed_ms)
    row["last_seen_ms"] = max(row["last_seen_ms"], observed_ms)
    row["fact_types"][fact_type] += 1


def finalize_order_intent(
    order: dict[str, Any],
    *,
    sizing: dict[str, Any],
    follower_recovery_complete: bool,
    source_recovery_complete: bool,
) -> dict[str, Any]:
    terminal = order["terminal_count"] > 0 or clean(order["status"]).lower() in {
        "filled",
        "canceled",
        "cancelled",
        "rejected",
    }
    target_action = (
        "validate_terminal_order_and_position" if terminal else "place_or_refresh_scaled_order"
    )
    decision, reason = intent_decision(
        target_action,
        follower_recovery_complete=follower_recovery_complete,
        source_recovery_complete=source_recovery_complete,
    )
    return {
        "source_order_key": order["key"],
        "coin": order["coin"],
        "side": order["side"],
        "source_status": order["status"],
        "source_notional_usd": decimal_str(order["source_notional_usd"]),
        "opened_count": order["opened_count"],
        "terminal_count": order["terminal_count"],
        "source_fill_events": order["source_fill_events"],
        "first_seen_ms": order["first_seen_ms"],
        "last_seen_ms": order["last_seen_ms"],
        "source_fact_types": counter_dict(order["fact_types"]),
        "sizing": scaled_intent_sizing(
            source_notional=order["source_notional_usd"],
            side=order["side"],
            sizing=sizing,
        ),
        "target_action": target_action,
        "decision": decision,
        "reason": reason,
    }


def finalize_twap_intent(
    twap: dict[str, Any],
    *,
    sizing: dict[str, Any],
    follower_recovery_complete: bool,
    source_recovery_complete: bool,
) -> dict[str, Any]:
    status = clean(twap["status"]).lower()
    terminal = twap["terminal_count"] > 0 or status in {"finished", "terminated", "error"}
    if terminal:
        target_action = "reconcile_or_cancel_mapped_twap"
    else:
        target_action = "place_or_refresh_scaled_twap"
    decision, reason = intent_decision(
        target_action,
        follower_recovery_complete=follower_recovery_complete,
        source_recovery_complete=source_recovery_complete,
    )
    return {
        "source_twap_key": twap["key"],
        "coin": twap["coin"],
        "side": twap["side"],
        "reduce_only": twap["reduce_only"],
        "source_status": twap["status"],
        "activated_count": twap["activated_count"],
        "terminal_count": twap["terminal_count"],
        "slice_fill_count": twap["slice_fill_count"],
        "source_notional_usd": decimal_str(twap["source_notional_usd"]),
        "source_notional_observations": twap["source_notional_observations"],
        "source_notional_basis": "twap_slice_fill_seen_sum",
        "state_refresh_count": twap["state_refresh_count"],
        "first_seen_ms": twap["first_seen_ms"],
        "last_seen_ms": twap["last_seen_ms"],
        "source_fact_types": counter_dict(twap["fact_types"]),
        "sizing": scaled_intent_sizing(
            source_notional=(
                twap["source_notional_usd"] if twap["source_notional_observations"] > 0 else None
            ),
            side=twap["side"],
            sizing=sizing,
        ),
        "target_action": target_action,
        "decision": decision,
        "reason": reason,
    }


def intent_decision(
    target_action: str,
    *,
    follower_recovery_complete: bool,
    source_recovery_complete: bool,
) -> tuple[str, str]:
    if not source_recovery_complete:
        return (
            "blocked_source_recovery_pending",
            f"{target_action} is blocked until source backfill and reconcile complete",
        )
    if not follower_recovery_complete:
        return (
            "blocked_follower_recovery_pending",
            f"{target_action} is blocked until follower refresh and reconcile complete",
        )
    return (
        f"would_{target_action}",
        f"{target_action} would be evaluated by the shadow slot actor",
    )


def sizing_context(
    *,
    slot_config: dict[str, Any],
    follower_snapshot: dict[str, Any],
    source_state: dict[str, Any],
) -> dict[str, Any]:
    policy = clean(slot_config.get("sizing_policy"))
    equity_confidence_policy = clean(
        slot_config.get("equity_confidence_policy") or "block_low"
    ).lower()
    follower_equity = decimal_optional(follower_snapshot.get("account_value_usd"))
    fixed_budget = decimal_optional(slot_config.get("fixed_risk_budget_usd"))
    initial_budget = decimal_optional(slot_config.get("initial_budget_usd"))
    if follower_equity is None and initial_budget is not None:
        follower_equity_source = "slot_initial_budget_usd"
        follower_equity = initial_budget
    else:
        follower_equity_source = "follower_snapshot.account_value_usd"
    source_denominator = source_denominator_context(
        source_state=source_state,
        equity_confidence_policy=equity_confidence_policy,
    )
    source_account_value = source_denominator["source_account_value_usd"]
    sizing_equity = follower_equity
    if policy == "fixed_risk_budget" and follower_equity is not None and fixed_budget is not None:
        sizing_equity = min(follower_equity, fixed_budget)
    return {
        "policy": policy,
        "follower_equity_usd": follower_equity,
        "follower_equity_source": follower_equity_source,
        "source_account_value_usd": source_account_value,
        "source_denominator": source_denominator,
        "fixed_risk_budget_usd": fixed_budget,
        "initial_budget_usd": initial_budget,
        "sizing_equity_usd": sizing_equity,
    }


def source_denominator_context(
    *,
    source_state: dict[str, Any],
    equity_confidence_policy: str,
) -> dict[str, Any]:
    account_context: dict[str, Any] = {}
    if isinstance(source_state.get("summary"), dict):
        account_context = dict_value(source_state["summary"].get("account_context"))
    source_account_value = decimal_optional(account_context.get("latest_account_value_usd"))
    raw_confidence = clean(account_context.get("equity_confidence")).lower()
    confidence = normalized_equity_confidence(raw_confidence)
    fallback_candidates = denominator_fallback_candidates(account_context)
    if source_account_value is None:
        decision = "missing_source_account_value"
        usable = False
        fallback_decision = (
            "fallback_candidates_available_but_not_used_fail_closed"
            if fallback_candidates
            else "no_safe_denominator_fallback_available"
        )
    elif source_account_value <= 0:
        decision = "nonpositive_source_account_value"
        usable = False
        fallback_decision = (
            "fallback_candidates_available_but_not_used_fail_closed"
            if fallback_candidates
            else "no_safe_denominator_fallback_available"
        )
    elif confidence == "low" and equity_confidence_policy == "block_low":
        decision = "blocked_low_equity_confidence"
        usable = False
        fallback_decision = "fallback_not_used_policy_blocks_low_confidence"
    elif confidence == "low" and equity_confidence_policy == "degrade_low":
        decision = "degraded_low_equity_confidence"
        usable = True
        fallback_decision = "using_latest_account_value_with_low_confidence"
    else:
        decision = "use_latest_source_account_value"
        usable = True
        fallback_decision = "not_needed"
    return {
        "source_account_value_usd": source_account_value,
        "source": (
            "source_state.account_context.latest_account_value_usd"
            if source_account_value is not None
            else None
        ),
        "usable": usable,
        "decision": decision,
        "fallback_decision": fallback_decision,
        "fallback_required": decision
        in {"missing_source_account_value", "nonpositive_source_account_value"},
        "fallback_candidate_count": len(fallback_candidates),
        "fallback_candidates": fallback_candidates,
        "equity_confidence_policy": equity_confidence_policy,
        "equity_confidence": confidence,
        "raw_equity_confidence": raw_confidence,
        "equity_confidence_reason": clean(account_context.get("equity_confidence_reason")),
        "latest_account_value_ts_ms": int_optional(
            account_context.get("latest_account_value_ts_ms")
        ),
        "account_value_observations": int_optional(
            account_context.get("account_value_observations")
        )
        or 0,
        "confidence_downgrade_reasons": dict_value(
            account_context.get("confidence_downgrade_reasons")
        ),
    }


def denominator_fallback_candidates(account_context: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    append_denominator_candidate(
        candidates,
        name="latest_position_notional_usd",
        value=decimal_optional(account_context.get("latest_position_notional_usd")),
        observations=int_optional(account_context.get("latest_position_notional_observations"))
        or 0,
        source="source_state.account_context.latest_position_notional_usd",
        reason="position notional is exposure, not source equity",
    )
    append_denominator_candidate(
        candidates,
        name="latest_position_margin_used_usd",
        value=decimal_optional(account_context.get("latest_position_margin_used_usd")),
        observations=int_optional(account_context.get("latest_position_margin_used_observations"))
        or 0,
        source="source_state.account_context.latest_position_margin_used_usd",
        reason="margin used excludes free collateral and cannot safely replace account value",
    )
    return candidates


def append_denominator_candidate(
    candidates: list[dict[str, Any]],
    *,
    name: str,
    value: Decimal | None,
    observations: int,
    source: str,
    reason: str,
) -> None:
    if value is None or value <= 0:
        return
    candidates.append(
        {
            "name": name,
            "value_usd": value,
            "observations": observations,
            "source": source,
            "usable": False,
            "reason": reason,
        }
    )


def normalized_equity_confidence(value: str) -> str:
    if value in {"high", "medium", "low"}:
        return value
    return "low"


def sizing_context_view(sizing: dict[str, Any]) -> dict[str, Any]:
    return {
        "policy": sizing["policy"],
        "follower_equity_usd": decimal_str(sizing.get("follower_equity_usd")),
        "follower_equity_source": sizing["follower_equity_source"],
        "source_account_value_usd": decimal_str(sizing.get("source_account_value_usd")),
        "source_denominator": denominator_view(sizing.get("source_denominator")),
        "fixed_risk_budget_usd": decimal_str(sizing.get("fixed_risk_budget_usd")),
        "initial_budget_usd": decimal_str(sizing.get("initial_budget_usd")),
        "sizing_equity_usd": decimal_str(sizing.get("sizing_equity_usd")),
    }


def scaled_intent_sizing(
    *,
    source_notional: Decimal | None,
    side: Any,
    sizing: dict[str, Any],
) -> dict[str, Any]:
    source_account_value = sizing.get("source_account_value_usd")
    denominator = dict_value(sizing.get("source_denominator"))
    sizing_equity = sizing.get("sizing_equity_usd")
    missing: list[str] = []
    if source_notional is None:
        missing.append("source_notional_usd")
    if source_account_value is None:
        missing.append("source_account_value_usd")
    elif source_account_value <= 0:
        missing.append("positive_source_account_value_usd")
    if sizing_equity is None:
        missing.append("sizing_equity_usd")
    elif sizing_equity <= 0:
        missing.append("positive_sizing_equity_usd")
    denominator_blockers: list[str] = []
    if denominator and denominator.get("usable") is not True:
        decision = clean(denominator.get("decision"))
        if decision not in {"missing_source_account_value", "nonpositive_source_account_value"}:
            denominator_blockers.append(decision)
    if missing:
        return {
            "status": "missing_inputs",
            "missing_inputs": missing,
            "blockers": denominator_blockers,
            "policy": sizing["policy"],
            "source_notional_usd": decimal_str(source_notional),
            "source_account_value_usd": decimal_str(source_account_value),
            "denominator": denominator_view(denominator),
            "follower_equity_usd": decimal_str(sizing.get("follower_equity_usd")),
            "follower_equity_source": sizing["follower_equity_source"],
            "sizing_equity_usd": decimal_str(sizing_equity),
            "fixed_risk_budget_usd": decimal_str(sizing.get("fixed_risk_budget_usd")),
            "copy_ratio": None,
            "target_notional_usd": None,
            "signed_target_notional_usd": None,
        }
    assert source_notional is not None
    assert isinstance(source_account_value, Decimal)
    assert isinstance(sizing_equity, Decimal)
    if denominator_blockers:
        return {
            "status": denominator_blockers[0],
            "missing_inputs": [],
            "blockers": denominator_blockers,
            "policy": sizing["policy"],
            "source_notional_usd": decimal_str(abs(source_notional)),
            "source_account_value_usd": decimal_str(source_account_value),
            "denominator": denominator_view(denominator),
            "follower_equity_usd": decimal_str(sizing.get("follower_equity_usd")),
            "follower_equity_source": sizing["follower_equity_source"],
            "sizing_equity_usd": decimal_str(sizing_equity),
            "fixed_risk_budget_usd": decimal_str(sizing.get("fixed_risk_budget_usd")),
            "copy_ratio": None,
            "target_notional_usd": None,
            "signed_target_notional_usd": None,
        }
    copy_ratio = sizing_equity / source_account_value
    target_notional = abs(source_notional) * copy_ratio
    signed_target = signed_amount(target_notional, side)
    status = (
        "computed_low_confidence"
        if clean(denominator.get("decision")) == "degraded_low_equity_confidence"
        else "computed"
    )
    return {
        "status": status,
        "missing_inputs": [],
        "blockers": [],
        "policy": sizing["policy"],
        "source_notional_usd": decimal_str(abs(source_notional)),
        "source_account_value_usd": decimal_str(source_account_value),
        "denominator": denominator_view(denominator),
        "follower_equity_usd": decimal_str(sizing.get("follower_equity_usd")),
        "follower_equity_source": sizing["follower_equity_source"],
        "sizing_equity_usd": decimal_str(sizing_equity),
        "fixed_risk_budget_usd": decimal_str(sizing.get("fixed_risk_budget_usd")),
        "copy_ratio": decimal_str(copy_ratio),
        "target_notional_usd": decimal_str(target_notional),
        "signed_target_notional_usd": decimal_str(signed_target),
    }


def denominator_view(value: Any) -> dict[str, Any]:
    denominator = dict_value(value)
    return {
        "source_account_value_usd": decimal_str(
            decimal_optional(denominator.get("source_account_value_usd"))
        ),
        "source": denominator.get("source"),
        "usable": denominator.get("usable") is True,
        "decision": clean(denominator.get("decision")),
        "fallback_decision": clean(denominator.get("fallback_decision")),
        "fallback_required": denominator.get("fallback_required") is True,
        "fallback_candidate_count": int_optional(denominator.get("fallback_candidate_count")) or 0,
        "fallback_candidates": denominator_fallback_candidate_views(
            denominator.get("fallback_candidates")
        ),
        "equity_confidence_policy": clean(denominator.get("equity_confidence_policy")),
        "equity_confidence": clean(denominator.get("equity_confidence")),
        "raw_equity_confidence": clean(denominator.get("raw_equity_confidence")),
        "equity_confidence_reason": clean(denominator.get("equity_confidence_reason")),
        "latest_account_value_ts_ms": int_optional(denominator.get("latest_account_value_ts_ms")),
        "account_value_observations": int_optional(denominator.get("account_value_observations"))
        or 0,
        "confidence_downgrade_reasons": dict_value(denominator.get("confidence_downgrade_reasons")),
    }


def denominator_fallback_candidate_views(value: Any) -> list[dict[str, Any]]:
    rows = []
    for candidate in list_items(value):
        rows.append(
            {
                "name": clean(candidate.get("name")),
                "value_usd": decimal_str(decimal_optional(candidate.get("value_usd"))),
                "observations": int_optional(candidate.get("observations")) or 0,
                "source": clean(candidate.get("source")),
                "usable": candidate.get("usable") is True,
                "reason": clean(candidate.get("reason")),
            }
        )
    return rows


def fact_matches_slot(fact: dict[str, Any], *, source_address: str, slot_name: str) -> bool:
    address = clean(fact.get("address")).lower()
    fact_slot = clean(fact.get("slot"))
    return address == source_address or fact_slot == slot_name


def read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SlotStateInputError(f"could not read {label}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise SlotStateInputError(f"{label} must be valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise SlotStateInputError(f"{label} must be a JSON object")
    return payload


def read_jsonl_objects(path: Path, *, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise SlotStateInputError(
                        f"{label} {path}:{line_no} must be valid JSON: {exc.msg}"
                    ) from exc
                if not isinstance(row, dict):
                    raise SlotStateInputError(f"{label} {path}:{line_no} must be a JSON object")
                rows.append(row)
    except OSError as exc:
        raise SlotStateInputError(f"could not read {label}: {exc}") from exc
    return rows


def signed_amount(amount: Decimal, side: Any) -> Decimal:
    label = clean(side).lower()
    if label in {"a", "ask", "sell", "short"}:
        return -abs(amount)
    if label in {"b", "bid", "buy", "long"}:
        return abs(amount)
    return amount


def signs_conflict(left: Decimal, right: Decimal) -> bool:
    return (left > 0 and right < 0) or (left < 0 and right > 0)


def target_side(signed: Decimal) -> str:
    if signed > 0:
        return "long"
    if signed < 0:
        return "short"
    return "flat"


def int_optional(value: Any) -> int | None:
    if value in (None, "", "unknown"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def decimal_optional(value: Any) -> Decimal | None:
    if value in (None, "", "unknown"):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def decimal_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.quantize(Decimal("0.00000001")), "f")


def decimal_map(values: dict[str, Decimal]) -> dict[str, str]:
    return {key: decimal_str(values[key]) or "0.00000000" for key in sorted(values)}


def prefer_known(current: Any, candidate: Any) -> str:
    current_clean = clean(current)
    candidate_clean = clean(candidate)
    if current_clean == "unknown" and candidate_clean != "unknown":
        return candidate_clean
    return current_clean


def prefer_decimal(current: Decimal | None, candidate: Any) -> Decimal | None:
    parsed = decimal_optional(candidate)
    return parsed if current is None and parsed is not None else current


def lifecycle_status(current: Any, candidate: Any, *, terminal: bool) -> str:
    current_clean = clean(current)
    candidate_clean = clean(candidate)
    if terminal and candidate_clean != "unknown":
        return candidate_clean
    if current_clean == "unknown" and candidate_clean != "unknown":
        return candidate_clean
    return current_clean


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def lower_counter(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, count in value.items():
        parsed = int_optional(count)
        if parsed is not None:
            result[clean(key).lower()] = parsed
    return result


def counter_keys(value: Any) -> list[str]:
    if not isinstance(value, dict):
        return []
    return [clean(key) for key in value]


def list_items(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def list_text(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [clean(item) for item in value]


def dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def path_str(path: Path | None) -> str | None:
    return str(path) if path is not None else None


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def parse_decimal_arg(value: str) -> Decimal:
    parsed = decimal_optional(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(f"expected decimal value, got {value!r}")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a read-only shared shadow slot-state report from a validated slot plan, "
            "target snapshot, and follower snapshot."
        )
    )
    parser.add_argument("slot_plan_report_path", type=Path)
    parser.add_argument("target_snapshot_path", type=Path)
    parser.add_argument("follower_snapshot_path", type=Path)
    parser.add_argument("--out", type=Path, default=None, help="Write slot-state report JSON.")
    parser.add_argument(
        "--source-state-report",
        type=Path,
        default=None,
        help="Optional read-only source state report from track_replay_state.py.",
    )
    parser.add_argument(
        "--source-state-facts",
        type=Path,
        default=None,
        help="Optional read-only source state facts JSONL from track_replay_state.py.",
    )
    parser.add_argument(
        "--recovery-evidence-report",
        type=Path,
        default=None,
        help="Optional read-only recovery evidence report from build_recovery_evidence_report.py.",
    )
    parser.add_argument(
        "--drift-threshold-usd",
        type=parse_decimal_arg,
        default=DEFAULT_DRIFT_THRESHOLD_USD,
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = build_shadow_slot_state(
            args.slot_plan_report_path,
            args.target_snapshot_path,
            args.follower_snapshot_path,
            out=args.out,
            source_state_report_path=args.source_state_report,
            source_state_facts_path=args.source_state_facts,
            recovery_evidence_report_path=args.recovery_evidence_report,
            drift_threshold_usd=args.drift_threshold_usd,
        )
    except SlotStateInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.out is None:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "report": str(args.out),
                    "valid": report["valid"],
                    "slots": report["counts"]["slots"],
                    "slot_statuses": report["counts"]["slot_statuses"],
                    "exchange_touched": report["exchange_touched"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
