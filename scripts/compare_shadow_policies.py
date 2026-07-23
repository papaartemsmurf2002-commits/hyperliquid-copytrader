from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator


POLICY_COMPARISON_VERSION = 1
DEFAULT_SAMPLE_SIZE = 50
DEFAULT_INITIAL_FOLLOWER_EQUITY_USD = Decimal("50")
DEFAULT_FIXED_RISK_BUDGET_USD = Decimal("50")
DEFAULT_MIN_NOTIONAL_USD = Decimal("10")
MIN_NOTIONAL_SOURCE = (
    "Hyperliquid official Error responses docs: MinTradeNtl says orders must have minimum "
    "value of $10."
)
MIN_NOTIONAL_SOURCE_URL = (
    "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses"
)

COPYABLE_ACTIONS = {
    "source_order_open": "place_scaled_order",
    "source_fill": "validate_scaled_fill",
    "source_twap_slice_fill": "validate_scaled_twap_slice",
}

STATE_ONLY_SOURCE_ACTIONS = {
    "source_order_filled",
    "source_order_cancel",
    "source_order_reject",
    "source_twap_activated",
    "source_twap_finished",
    "source_twap_terminated",
    "source_twap_error",
}

EXECUTED_DECISIONS = {
    "would_send_now",
    "would_send_accumulated_dust",
}


class PolicyComparisonInputError(RuntimeError):
    """Raised when scorecard rows cannot be evaluated."""


@dataclass
class DustBucket:
    key: str
    coin: str
    side: str
    reduce_only: bool
    pending_notional_usd: Decimal = Decimal("0")
    accumulated_events: int = 0
    executed_events: int = 0
    total_accumulated_notional_usd: Decimal = Decimal("0")
    total_executed_notional_usd: Decimal = Decimal("0")

    def add(self, amount: Decimal, min_notional: Decimal) -> tuple[str, Decimal, Decimal, Decimal]:
        before = self.pending_notional_usd
        self.pending_notional_usd += amount
        self.total_accumulated_notional_usd += amount
        self.accumulated_events += 1
        if self.pending_notional_usd >= min_notional:
            executed = self.pending_notional_usd
            self.total_executed_notional_usd += executed
            self.executed_events += 1
            self.pending_notional_usd = Decimal("0")
            return "would_send_accumulated_dust", before, self.pending_notional_usd, executed
        return "would_accumulate_dust", before, self.pending_notional_usd, Decimal("0")

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "coin": self.coin,
            "side": self.side,
            "reduce_only": self.reduce_only,
            "pending_notional_usd": decimal_str(self.pending_notional_usd),
            "accumulated_events": self.accumulated_events,
            "executed_events": self.executed_events,
            "total_accumulated_notional_usd": decimal_str(self.total_accumulated_notional_usd),
            "total_executed_notional_usd": decimal_str(self.total_executed_notional_usd),
        }


@dataclass
class PolicyState:
    name: str
    mode: str
    initial_equity_usd: Decimal
    fixed_risk_budget_usd: Decimal | None
    min_notional_usd: Decimal
    current_equity_usd: Decimal = field(init=False)
    realized_net_pnl_usd: Decimal = Decimal("0")
    copied_source_notional_usd: Decimal = Decimal("0")
    target_notional_sent_usd: Decimal = Decimal("0")
    target_notional_dust_usd: Decimal = Decimal("0")
    decision_counts: Counter[str] = field(default_factory=Counter)
    action_counts: Counter[str] = field(default_factory=Counter)
    dust_buckets: dict[str, DustBucket] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.current_equity_usd = self.initial_equity_usd

    def sizing_equity(self) -> Decimal:
        if self.mode == "fixed_risk_budget":
            budget = self.fixed_risk_budget_usd or self.initial_equity_usd
            return min(self.current_equity_usd, budget)
        return self.current_equity_usd

    def bucket(self, *, coin: str, side: str, reduce_only: bool) -> DustBucket:
        key = f"{coin}:{side}:reduce_only={str(reduce_only).lower()}"
        if key not in self.dust_buckets:
            self.dust_buckets[key] = DustBucket(
                key=key,
                coin=coin,
                side=side,
                reduce_only=reduce_only,
            )
        return self.dust_buckets[key]

    def as_dict(self) -> dict[str, Any]:
        pending_buckets = [
            bucket.as_dict()
            for bucket in sorted(self.dust_buckets.values(), key=lambda item: item.key)
            if bucket.pending_notional_usd > 0 or bucket.accumulated_events > 0
        ]
        return {
            "name": self.name,
            "mode": self.mode,
            "initial_equity_usd": decimal_str(self.initial_equity_usd),
            "fixed_risk_budget_usd": (
                decimal_str(self.fixed_risk_budget_usd)
                if self.fixed_risk_budget_usd is not None
                else None
            ),
            "final_equity_usd": decimal_str(self.current_equity_usd),
            "realized_net_pnl_usd": decimal_str(self.realized_net_pnl_usd),
            "copied_source_notional_usd": decimal_str(self.copied_source_notional_usd),
            "target_notional_sent_usd": decimal_str(self.target_notional_sent_usd),
            "target_notional_dust_usd": decimal_str(self.target_notional_dust_usd),
            "decision_counts": counter_dict(self.decision_counts),
            "action_counts": counter_dict(self.action_counts),
            "dust_bucket_count": len(self.dust_buckets),
            "dust_buckets": pending_buckets,
        }


class ShadowPolicyComparator:
    def __init__(
        self,
        *,
        rows_path: Path,
        slot: str,
        source_address: str | None,
        follower_subaccount: str | None,
        facts_out: Path | None,
        sample_size: int,
        initial_follower_equity_usd: Decimal,
        fixed_risk_budget_usd: Decimal,
        min_notional_usd: Decimal,
        analysis_clear_recovery_after_rest_snapshot: bool,
    ) -> None:
        self.rows_path = rows_path
        self.slot = slot
        self.source_address = source_address.lower() if source_address else None
        self.follower_subaccount = follower_subaccount.lower() if follower_subaccount else None
        self.facts_out = facts_out
        self.sample_size = sample_size
        self.min_notional_usd = min_notional_usd
        self.analysis_clear_recovery_after_rest_snapshot = (
            analysis_clear_recovery_after_rest_snapshot
        )
        self.policies = {
            "pure_compound": PolicyState(
                name="pure_compound",
                mode="pure_compound",
                initial_equity_usd=initial_follower_equity_usd,
                fixed_risk_budget_usd=None,
                min_notional_usd=min_notional_usd,
            ),
            "fixed_risk_budget": PolicyState(
                name="fixed_risk_budget",
                mode="fixed_risk_budget",
                initial_equity_usd=initial_follower_equity_usd,
                fixed_risk_budget_usd=fixed_risk_budget_usd,
                min_notional_usd=min_notional_usd,
            ),
        }

        self.input_rows_seen = 0
        self.rows_processed = 0
        self.skipped_by_address = 0
        self.first_sort_ts_ms: int | None = None
        self.last_sort_ts_ms: int | None = None
        self.address_counts: Counter[str] = Counter()
        self.category_counts: Counter[str] = Counter()
        self.source_action_counts: Counter[str] = Counter()
        self.global_fact_counts: Counter[str] = Counter()
        self.policy_fact_counts: Counter[str] = Counter()
        self.copyable_rows = 0
        self.copyable_rows_with_notional = 0
        self.copyable_rows_missing_notional = 0
        self.state_only_source_actions = 0
        self.unknown_rows: list[dict[str, Any]] = []
        self.fact_samples: list[dict[str, Any]] = []

        self.source_account_value_usd: Decimal | None = None
        self.source_account_value_ts_ms: int | None = None
        self.account_value_observations = 0
        self.equity_confidence = "unknown"
        self.equity_confidence_reason = "no account-value evidence processed"
        self.confidence_downgrade_reasons: Counter[str] = Counter()
        self.recovery_pending = False
        self.pending_rest_backfill = False
        self.live_stream_hints_allowed = True
        self.recovery_events = 0
        self.state_refreshes_after_recovery = 0
        self.analysis_recovery_clears = 0

    def should_process(self, row: dict[str, Any]) -> bool:
        self.input_rows_seen += 1
        if self.source_address is None:
            return True
        if row["address"] == self.source_address:
            return True
        self.skipped_by_address += 1
        return False

    def observe(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        self.rows_processed += 1
        self.first_sort_ts_ms = (
            row["sort_ts_ms"] if self.first_sort_ts_ms is None else self.first_sort_ts_ms
        )
        self.last_sort_ts_ms = row["sort_ts_ms"]
        self.address_counts[row["address"]] += 1
        self.category_counts[row["category"]] += 1
        self.source_action_counts[row["source_action"]] += 1

        facts: list[dict[str, Any]]
        category = row["category"]
        action = row["source_action"]
        if category == "state_refresh":
            facts = [self._observe_state_refresh(row)]
        elif category == "account_state":
            facts = [self._observe_account_state(row)]
        elif category == "recovery":
            facts = [self._observe_recovery(row)]
        elif category == "passive_snapshot":
            facts = [self._observe_passive_snapshot(row)]
        elif category == "source_action" and action in COPYABLE_ACTIONS:
            facts = self._observe_copyable_action(row)
        elif category == "source_action" and action in STATE_ONLY_SOURCE_ACTIONS:
            self.state_only_source_actions += 1
            facts = [self._source_action_state_only(row)]
        elif category in {"passive_control", "connection_control"}:
            facts = [
                self._global_fact(
                    row,
                    fact_type="control_observed",
                    confidence=row["confidence"],
                    reason=row["reason"],
                    metadata={"source_action": action, "category": category},
                )
            ]
        else:
            if len(self.unknown_rows) < self.sample_size:
                self.unknown_rows.append(row)
            self._downgrade("unknown_scorecard_row")
            facts = [
                self._global_fact(
                    row,
                    fact_type="unknown_row_needs_review",
                    confidence="low",
                    reason="unrecognized row for policy comparison",
                    metadata={
                        "category": category,
                        "source_action": action,
                        "classifier_decision": row["classifier_decision"],
                    },
                )
            ]

        for fact in facts:
            if fact.get("policy") is None:
                self.global_fact_counts[fact["fact_type"]] += 1
            else:
                self.policy_fact_counts[fact["fact_type"]] += 1
            if len(self.fact_samples) < self.sample_size:
                self.fact_samples.append(fact)
        return facts

    def _observe_copyable_action(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        self.copyable_rows += 1
        source_notional = decimal_optional(row["metadata"].get("notional_usd"))
        if source_notional is None or source_notional <= 0:
            self.copyable_rows_missing_notional += 1
        else:
            self.copyable_rows_with_notional += 1
        return [
            self._evaluate_policy(policy, row, source_notional=source_notional)
            for policy in self.policies.values()
        ]

    def _evaluate_policy(
        self,
        policy: PolicyState,
        row: dict[str, Any],
        *,
        source_notional: Decimal | None,
    ) -> dict[str, Any]:
        action = row["source_action"]
        metadata = row["metadata"]
        policy.action_counts[action] += 1
        intent = COPYABLE_ACTIONS[action]
        coin = clean(metadata.get("coin"))
        side = clean(metadata.get("side"))
        reduce_only = is_reduce_only(metadata)
        source_account_value = self.source_account_value_usd
        sizing_equity = policy.sizing_equity()
        ratio: Decimal | None = None
        target_notional: Decimal | None = None
        executed_target_notional = Decimal("0")
        source_net_pnl: Decimal | None = None
        scaled_net_pnl = Decimal("0")
        dust_before: Decimal | None = None
        dust_after: Decimal | None = None

        if self.recovery_pending:
            decision = "blocked_recovery_pending"
            confidence = "low"
            reason = "stream recovery requires REST backfill and source/follower reconciliation"
        elif source_account_value is None or source_account_value <= 0:
            decision = "skipped_missing_source_account_value"
            confidence = "low"
            reason = "no trusted source account value denominator is available"
        elif self.equity_confidence == "low":
            decision = "skipped_low_equity_confidence"
            confidence = "low"
            reason = self.equity_confidence_reason
        elif source_notional is None or source_notional <= 0:
            decision = "skipped_missing_source_notional"
            confidence = "low"
            reason = "source action lacks price/size notional metadata"
        elif sizing_equity <= 0:
            decision = "skipped_no_follower_equity"
            confidence = "low"
            reason = "policy follower equity is depleted"
        else:
            ratio = sizing_equity / source_account_value
            target_notional = source_notional * ratio
            if target_notional >= policy.min_notional_usd:
                decision = "would_send_now"
                executed_target_notional = target_notional
                confidence = self._decision_confidence()
                reason = "scaled target notional meets current configured minimum notional"
            else:
                bucket = policy.bucket(coin=coin, side=side, reduce_only=reduce_only)
                decision, dust_before, dust_after, executed_target_notional = bucket.add(
                    target_notional, policy.min_notional_usd
                )
                policy.target_notional_dust_usd += target_notional
                confidence = (
                    "medium" if self.equity_confidence == "high" else self.equity_confidence
                )
                if decision == "would_send_accumulated_dust":
                    reason = "dust bucket reached minimum notional after accumulation"
                else:
                    reason = "scaled target notional is below minimum notional and was accumulated"

        policy.decision_counts[decision] += 1
        if target_notional is not None and source_notional is not None:
            policy.copied_source_notional_usd += source_notional
        if decision in EXECUTED_DECISIONS and target_notional is not None:
            policy.target_notional_sent_usd += executed_target_notional
            source_net_pnl = source_net_pnl_usd(metadata)
            if source_net_pnl is not None and ratio is not None:
                scaled_net_pnl = source_net_pnl * ratio
                policy.current_equity_usd += scaled_net_pnl
                policy.realized_net_pnl_usd += scaled_net_pnl

        return self._policy_fact(
            row,
            policy=policy,
            fact_type="policy_shadow_decision",
            decision=decision,
            intent=intent,
            confidence=confidence,
            reason=reason,
            metadata={
                "coin": coin,
                "side": side,
                "dir": clean(metadata.get("dir")),
                "reduce_only": reduce_only,
                "source_notional_usd": decimal_str(source_notional),
                "source_account_value_usd": decimal_str(source_account_value),
                "sizing_equity_usd": decimal_str(sizing_equity),
                "copy_ratio": decimal_str(ratio),
                "target_notional_usd": decimal_str(target_notional),
                "executed_target_notional_usd": decimal_str(executed_target_notional),
                "min_notional_usd": decimal_str(policy.min_notional_usd),
                "dust_bucket_before_usd": decimal_str(dust_before),
                "dust_bucket_after_usd": decimal_str(dust_after),
                "source_net_pnl_usd": decimal_str(source_net_pnl),
                "scaled_net_pnl_usd": decimal_str(scaled_net_pnl),
                "follower_equity_after_usd": decimal_str(policy.current_equity_usd),
                "px": clean(metadata.get("px") or metadata.get("limit_px")),
                "sz": clean(metadata.get("sz")),
                "oid": clean(metadata.get("oid")),
                "twap_id": clean(metadata.get("twap_id")),
                "recovery_pending": self.recovery_pending,
                "equity_confidence": self.equity_confidence,
            },
        )

    def _decision_confidence(self) -> str:
        if self.equity_confidence == "high":
            return "medium"
        if self.equity_confidence == "medium":
            return "medium"
        return "low"

    def _observe_state_refresh(self, row: dict[str, Any]) -> dict[str, Any]:
        if self.recovery_pending:
            self.state_refreshes_after_recovery += 1
        if row["source_action"] != "rest_snapshot":
            return self._global_fact(
                row,
                fact_type="state_refresh_seen",
                confidence=row["confidence"],
                reason=row["reason"],
                metadata={"source_action": row["source_action"]},
            )
        metadata = row["metadata"]
        recovery_was_pending = self.recovery_pending
        account_value = decimal_optional(metadata.get("account_value_usd"))
        error_count = int_optional(metadata.get("error_count"))
        if error_count and error_count > 0:
            self.equity_confidence = "low"
            self.equity_confidence_reason = "REST snapshot carried request errors"
            self._downgrade("rest_snapshot_error")
        elif account_value is not None and account_value > 0:
            self.source_account_value_usd = account_value
            self.source_account_value_ts_ms = row["sort_ts_ms"]
            self.account_value_observations += 1
            self.equity_confidence = "high"
            self.equity_confidence_reason = "latest REST snapshot carried account_value_usd"
            if self.analysis_clear_recovery_after_rest_snapshot and self.recovery_pending:
                self.recovery_pending = False
                self.pending_rest_backfill = False
                self.live_stream_hints_allowed = True
                self.analysis_recovery_clears += 1
        else:
            self.equity_confidence = "medium"
            self.equity_confidence_reason = "REST snapshot lacked account_value_usd"
            self._downgrade("missing_account_value")
        fact_type = (
            "analysis_recovery_cleared_by_source_rest_snapshot"
            if recovery_was_pending
            and self.analysis_clear_recovery_after_rest_snapshot
            and not self.recovery_pending
            else "source_account_value_seen"
        )
        reason = (
            "analysis-only recovery clear from source REST snapshot; not live follower proof"
            if fact_type == "analysis_recovery_cleared_by_source_rest_snapshot"
            else self.equity_confidence_reason
        )
        return self._global_fact(
            row,
            fact_type=fact_type,
            confidence=self.equity_confidence,
            reason=reason,
            metadata={
                "account_value_usd": decimal_str(account_value),
                "error_count": error_count,
                "ok_count": int_optional(metadata.get("ok_count")),
                "recovery_pending": self.recovery_pending,
                "analysis_clear_recovery_after_rest_snapshot": (
                    self.analysis_clear_recovery_after_rest_snapshot
                ),
            },
        )

    def _observe_account_state(self, row: dict[str, Any]) -> dict[str, Any]:
        action = row["source_action"]
        if self.equity_confidence == "high":
            self.equity_confidence = "medium"
        elif self.equity_confidence == "unknown":
            self.equity_confidence = "medium"
        self.equity_confidence_reason = f"{action}_requires_fresh_snapshot"
        self._downgrade(self.equity_confidence_reason)
        return self._global_fact(
            row,
            fact_type="equity_confidence_downgrade",
            confidence=self.equity_confidence,
            reason=self.equity_confidence_reason,
            metadata={
                "source_action": action,
                "coin": clean(row["metadata"].get("coin")),
                "ledger_type": clean(row["metadata"].get("ledger_type")),
            },
        )

    def _observe_recovery(self, row: dict[str, Any]) -> dict[str, Any]:
        self.recovery_events += 1
        self.recovery_pending = True
        self.pending_rest_backfill = True
        self.live_stream_hints_allowed = False
        reason = "recovery event requires REST backfill and source/follower reconciliation"
        if row["source_action"] == "stream_degraded":
            self._downgrade("stream_degraded")
        elif row["source_action"] == "reconnect_recovered":
            self._downgrade("reconnect_requires_rest_backfill")
        else:
            self._downgrade("unknown_recovery_event")
        return self._global_fact(
            row,
            fact_type="recovery_blocks_policy_actions",
            confidence="low",
            reason=reason,
            metadata={
                "source_action": row["source_action"],
                "gap_ms": int_optional(row["metadata"].get("gap_ms")),
                "pending_rest_backfill": self.pending_rest_backfill,
                "pending_reconcile": self.recovery_pending,
                "live_stream_hints_allowed": self.live_stream_hints_allowed,
            },
        )

    def _observe_passive_snapshot(self, row: dict[str, Any]) -> dict[str, Any]:
        if row["classifier_decision"] == "duplicate_snapshot_skipped":
            fact_type = "duplicate_snapshot_skipped"
            reason = "passive duplicate snapshot skipped; no policy action"
        else:
            fact_type = "snapshot_seed_or_refresh_seen"
            reason = "passive subscription snapshot observed; no policy action"
        return self._global_fact(
            row,
            fact_type=fact_type,
            confidence="high",
            reason=reason,
            metadata={
                "event_type": row["event_type"],
                "subtype": row["subtype"],
                "source_action": row["source_action"],
            },
        )

    def _source_action_state_only(self, row: dict[str, Any]) -> dict[str, Any]:
        confidence = "low" if row["source_action"] == "source_twap_error" else row["confidence"]
        if row["source_action"] == "source_twap_error":
            self._downgrade("source_twap_error")
        return self._global_fact(
            row,
            fact_type="source_action_state_only",
            confidence=confidence,
            reason="source action updates state/confidence but lacks enough metadata for sizing",
            metadata={
                "source_action": row["source_action"],
                "classifier_decision": row["classifier_decision"],
                "has_notional": decimal_optional(row["metadata"].get("notional_usd")) is not None,
            },
        )

    def _downgrade(self, reason: str) -> None:
        self.confidence_downgrade_reasons[reason] += 1

    def _global_fact(
        self,
        row: dict[str, Any],
        *,
        fact_type: str,
        confidence: str,
        reason: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        fact_payload = f"{row['row_id']}|{fact_type}|global"
        return {
            "fact_id": hashlib.sha256(fact_payload.encode("utf-8")).hexdigest()[:24],
            "policy_comparison_version": POLICY_COMPARISON_VERSION,
            "slot": self.slot,
            "policy": None,
            "address": row["address"],
            "sort_ts_ms": row["sort_ts_ms"],
            "row_id": row["row_id"],
            "event_id": row["event_id"],
            "fact_type": fact_type,
            "decision": fact_type,
            "confidence": confidence,
            "read_only": True,
            "exchange_touched": False,
            "reason": reason,
            "metadata": metadata,
        }

    def _policy_fact(
        self,
        row: dict[str, Any],
        *,
        policy: PolicyState,
        fact_type: str,
        decision: str,
        intent: str,
        confidence: str,
        reason: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        fact_payload = f"{row['row_id']}|{fact_type}|{policy.name}|{decision}"
        return {
            "fact_id": hashlib.sha256(fact_payload.encode("utf-8")).hexdigest()[:24],
            "policy_comparison_version": POLICY_COMPARISON_VERSION,
            "slot": self.slot,
            "policy": policy.name,
            "address": row["address"],
            "sort_ts_ms": row["sort_ts_ms"],
            "row_id": row["row_id"],
            "event_id": row["event_id"],
            "fact_type": fact_type,
            "decision": decision,
            "intent": intent,
            "confidence": confidence,
            "read_only": True,
            "exchange_touched": False,
            "reason": reason,
            "metadata": metadata,
        }

    def summary(self, *, truncated_by_limit: bool) -> dict[str, Any]:
        return {
            "policy_comparison_version": POLICY_COMPARISON_VERSION,
            "read_only": True,
            "exchange_touched": False,
            "rows_path": str(self.rows_path),
            "facts_out": str(self.facts_out) if self.facts_out is not None else None,
            "slot": {
                "slot": self.slot,
                "source_address": self.source_address,
                "follower_subaccount": self.follower_subaccount,
                "follower_truth_tracked": False,
                "follower_truth_reason": (
                    "this comparator sizes shadow intents from source replay metadata only; "
                    "actual follower positions/open orders are a later slot-actor input"
                ),
            },
            "min_notional": {
                "perp_min_notional_usd": decimal_str(self.min_notional_usd),
                "source": MIN_NOTIONAL_SOURCE,
                "source_url": MIN_NOTIONAL_SOURCE_URL,
            },
            "recovery_model": {
                "analysis_clear_recovery_after_rest_snapshot": (
                    self.analysis_clear_recovery_after_rest_snapshot
                ),
                "fail_closed_default": not self.analysis_clear_recovery_after_rest_snapshot,
                "not_live_recovery_proof": self.analysis_clear_recovery_after_rest_snapshot,
            },
            "input_rows_seen": self.input_rows_seen,
            "rows_processed": self.rows_processed,
            "skipped_by_address": self.skipped_by_address,
            "truncated_by_limit": truncated_by_limit,
            "time_range": {
                "first_sort_ts_ms": self.first_sort_ts_ms,
                "last_sort_ts_ms": self.last_sort_ts_ms,
            },
            "counts": {
                "by_address": counter_dict(self.address_counts),
                "by_category": counter_dict(self.category_counts),
                "by_source_action": counter_dict(self.source_action_counts),
                "global_fact_types": counter_dict(self.global_fact_counts),
                "policy_fact_types": counter_dict(self.policy_fact_counts),
            },
            "copyable_actions": {
                "rows": self.copyable_rows,
                "with_notional": self.copyable_rows_with_notional,
                "missing_notional": self.copyable_rows_missing_notional,
                "state_only_source_actions": self.state_only_source_actions,
            },
            "source_account_context": {
                "account_value_usd": decimal_str(self.source_account_value_usd),
                "account_value_ts_ms": self.source_account_value_ts_ms,
                "account_value_observations": self.account_value_observations,
                "equity_confidence": self.equity_confidence,
                "equity_confidence_reason": self.equity_confidence_reason,
                "confidence_downgrade_reasons": counter_dict(self.confidence_downgrade_reasons),
            },
            "recovery": {
                "pending_reconcile": self.recovery_pending,
                "pending_rest_backfill": self.pending_rest_backfill,
                "live_stream_hints_allowed": self.live_stream_hints_allowed,
                "recovery_events": self.recovery_events,
                "state_refreshes_after_recovery": self.state_refreshes_after_recovery,
                "analysis_recovery_clears": self.analysis_recovery_clears,
            },
            "policies": {name: policy.as_dict() for name, policy in sorted(self.policies.items())},
            "sample_facts": self.fact_samples,
            "unknown_rows": self.unknown_rows,
        }


def compare_shadow_policies(
    rows_path: Path,
    *,
    facts_out: Path | None = None,
    source_address: str | None = None,
    follower_subaccount: str | None = None,
    slot: str = "replay-slot",
    initial_follower_equity_usd: Decimal = DEFAULT_INITIAL_FOLLOWER_EQUITY_USD,
    fixed_risk_budget_usd: Decimal = DEFAULT_FIXED_RISK_BUDGET_USD,
    min_notional_usd: Decimal = DEFAULT_MIN_NOTIONAL_USD,
    analysis_clear_recovery_after_rest_snapshot: bool = False,
    limit_rows: int | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    if sample_size < 0:
        raise PolicyComparisonInputError("sample_size must be non-negative")
    if limit_rows is not None and limit_rows < 0:
        raise PolicyComparisonInputError("limit_rows must be non-negative")
    if initial_follower_equity_usd <= 0:
        raise PolicyComparisonInputError("initial follower equity must be positive")
    if fixed_risk_budget_usd <= 0:
        raise PolicyComparisonInputError("fixed risk budget must be positive")
    if min_notional_usd <= 0:
        raise PolicyComparisonInputError("min notional must be positive")

    comparator = ShadowPolicyComparator(
        rows_path=rows_path,
        slot=slot,
        source_address=source_address,
        follower_subaccount=follower_subaccount,
        facts_out=facts_out,
        sample_size=sample_size,
        initial_follower_equity_usd=initial_follower_equity_usd,
        fixed_risk_budget_usd=fixed_risk_budget_usd,
        min_notional_usd=min_notional_usd,
        analysis_clear_recovery_after_rest_snapshot=(analysis_clear_recovery_after_rest_snapshot),
    )
    fact_handle = None
    if facts_out is not None:
        facts_out.parent.mkdir(parents=True, exist_ok=True)
        fact_handle = facts_out.open("w", encoding="utf-8")
    try:
        for row in iter_scorecard_rows(rows_path):
            if not comparator.should_process(row):
                continue
            if limit_rows is not None and comparator.rows_processed >= limit_rows:
                break
            facts = comparator.observe(row)
            if fact_handle is not None:
                for fact in facts:
                    fact_handle.write(json.dumps(fact, separators=(",", ":"), sort_keys=True))
                    fact_handle.write("\n")
    finally:
        if fact_handle is not None:
            fact_handle.close()
    return comparator.summary(
        truncated_by_limit=limit_rows is not None and comparator.rows_processed >= limit_rows
    )


def iter_scorecard_rows(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        raise PolicyComparisonInputError(f"scorecard rows file does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise PolicyComparisonInputError(
                    f"{path}:{line_no} invalid JSON: {exc.msg}"
                ) from exc
            if not isinstance(row, dict):
                raise PolicyComparisonInputError(
                    f"{path}:{line_no} scorecard row must be an object"
                )
            yield normalize_scorecard_row(row, path=path, line_no=line_no)


def normalize_scorecard_row(row: dict[str, Any], *, path: Path, line_no: int) -> dict[str, Any]:
    required = (
        "row_id",
        "address",
        "sort_ts_ms",
        "event_type",
        "subtype",
        "category",
        "source_action",
        "classifier_decision",
        "reason",
        "confidence",
        "event_id",
    )
    for key in required:
        if key not in row:
            raise PolicyComparisonInputError(f"{path}:{line_no} scorecard row missing {key}")
    metadata = row.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "row_id": clean(row["row_id"]),
        "address": clean(row["address"]).lower(),
        "sort_ts_ms": parse_int(row["sort_ts_ms"], path=path, line_no=line_no),
        "event_type": clean(row["event_type"]),
        "subtype": clean(row["subtype"]),
        "category": clean(row["category"]),
        "source_action": clean(row["source_action"]),
        "classifier_decision": clean(row["classifier_decision"]),
        "intended_followup": clean(row.get("intended_followup")),
        "reason": clean(row["reason"]),
        "confidence": clean(row["confidence"]),
        "event_id": clean(row["event_id"]),
        "metadata": metadata,
    }


def is_reduce_only(metadata: dict[str, Any]) -> bool:
    value = metadata.get("reduce_only")
    if isinstance(value, bool):
        return value
    direction = clean(metadata.get("dir")).lower()
    return direction.startswith("close") or "close " in direction


def source_net_pnl_usd(metadata: dict[str, Any]) -> Decimal | None:
    closed_pnl = decimal_optional(metadata.get("closed_pnl_usd"))
    fee = decimal_optional(metadata.get("fee_usd"))
    if closed_pnl is None and fee is None:
        return None
    return (closed_pnl or Decimal("0")) - (fee or Decimal("0"))


def parse_int(value: Any, *, path: Path, line_no: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PolicyComparisonInputError(
            f"{path}:{line_no} expected integer sort_ts_ms, got {value!r}"
        ) from exc


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


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


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
            "Compare pure-compound and fixed-risk-budget copy sizing over read-only replay "
            "scorecard rows, including min-notional dust accounting."
        )
    )
    parser.add_argument(
        "rows_path", type=Path, help="Scorecard rows JSONL from score_replay_events.py."
    )
    parser.add_argument(
        "--out", type=Path, default=None, help="Write policy comparison report JSON."
    )
    parser.add_argument(
        "--facts-out", type=Path, default=None, help="Write policy decision facts JSONL."
    )
    parser.add_argument("--source-address", default=None, help="Optional source address filter.")
    parser.add_argument(
        "--follower-subaccount", default=None, help="Optional follower slot label/address."
    )
    parser.add_argument(
        "--slot", default="replay-slot", help="Slot name to stamp on generated facts."
    )
    parser.add_argument(
        "--initial-follower-equity-usd",
        type=parse_decimal_arg,
        default=DEFAULT_INITIAL_FOLLOWER_EQUITY_USD,
    )
    parser.add_argument(
        "--fixed-risk-budget-usd",
        type=parse_decimal_arg,
        default=DEFAULT_FIXED_RISK_BUDGET_USD,
    )
    parser.add_argument(
        "--min-notional-usd",
        type=parse_decimal_arg,
        default=DEFAULT_MIN_NOTIONAL_USD,
        help="Configured perp minimum notional. Default follows current official MinTradeNtl docs.",
    )
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument(
        "--analysis-clear-recovery-after-rest-snapshot",
        action="store_true",
        help=(
            "Analysis-only what-if: clear replay recovery after a source REST account snapshot. "
            "This is not sufficient live recovery proof because follower truth/backfill is absent."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = compare_shadow_policies(
            args.rows_path,
            facts_out=args.facts_out,
            source_address=args.source_address,
            follower_subaccount=args.follower_subaccount,
            slot=args.slot,
            initial_follower_equity_usd=args.initial_follower_equity_usd,
            fixed_risk_budget_usd=args.fixed_risk_budget_usd,
            min_notional_usd=args.min_notional_usd,
            analysis_clear_recovery_after_rest_snapshot=(
                args.analysis_clear_recovery_after_rest_snapshot
            ),
            limit_rows=args.limit_rows,
            sample_size=args.sample_size,
        )
    except PolicyComparisonInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.out is not None:
        write_json(args.out, report)
        print(
            json.dumps(
                {
                    "report": str(args.out),
                    "facts_out": str(args.facts_out) if args.facts_out is not None else None,
                    "rows_processed": report["rows_processed"],
                    "copyable_rows_with_notional": report["copyable_actions"]["with_notional"],
                    "recovery_pending": report["recovery"]["pending_reconcile"],
                    "exchange_touched": report["exchange_touched"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
