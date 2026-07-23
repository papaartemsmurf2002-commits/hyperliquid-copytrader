from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


STATE_TRACKER_VERSION = 1
DEFAULT_SAMPLE_SIZE = 50

TERMINAL_ORDER_ACTIONS = {
    "source_order_filled",
    "source_order_cancel",
    "source_order_reject",
}

TERMINAL_TWAP_ACTIONS = {
    "source_twap_finished": "finished",
    "source_twap_terminated": "terminated",
    "source_twap_error": "error",
}


class StateTrackerInputError(RuntimeError):
    """Raised when scorecard rows are malformed."""


@dataclass
class OrderState:
    key: str
    coin: str = "unknown"
    side: str = "unknown"
    cloid: str = "unknown"
    status: str = "unknown"
    first_seen_ms: int | None = None
    last_seen_ms: int | None = None
    opened_count: int = 0
    terminal_count: int = 0
    filled_count: int = 0
    canceled_count: int = 0
    rejected_count: int = 0
    cancel_request_count: int = 0
    source_fill_events: int = 0
    latest_notional_usd: float | None = None
    notional_observations: int = 0

    def observe_metadata(self, row: dict[str, Any]) -> None:
        metadata = row["metadata"]
        self.coin = choose_known(metadata.get("coin"), self.coin)
        self.side = choose_known(metadata.get("side"), self.side)
        self.cloid = choose_known(metadata.get("cloid"), self.cloid)
        notional = parse_optional_float(metadata.get("notional_usd"))
        if notional is not None:
            self.latest_notional_usd = notional
            self.notional_observations += 1
        self.first_seen_ms = row["sort_ts_ms"] if self.first_seen_ms is None else self.first_seen_ms
        self.last_seen_ms = row["sort_ts_ms"]

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "coin": self.coin,
            "side": self.side,
            "cloid": self.cloid,
            "status": self.status,
            "first_seen_ms": self.first_seen_ms,
            "last_seen_ms": self.last_seen_ms,
            "opened_count": self.opened_count,
            "terminal_count": self.terminal_count,
            "filled_count": self.filled_count,
            "canceled_count": self.canceled_count,
            "rejected_count": self.rejected_count,
            "cancel_request_count": self.cancel_request_count,
            "source_fill_events": self.source_fill_events,
            "latest_notional_usd": self.latest_notional_usd,
            "notional_observations": self.notional_observations,
        }


@dataclass
class TwapState:
    key: str
    coin: str = "unknown"
    side: str = "unknown"
    reduce_only: bool | None = None
    status: str = "unknown"
    first_seen_ms: int | None = None
    last_seen_ms: int | None = None
    activated_count: int = 0
    terminal_count: int = 0
    finished_count: int = 0
    terminated_count: int = 0
    error_count: int = 0
    slice_fill_count: int = 0
    state_refresh_count: int = 0
    slice_fill_without_activation: bool = False

    def observe_metadata(self, row: dict[str, Any]) -> None:
        metadata = row["metadata"]
        self.coin = choose_known(metadata.get("coin"), self.coin)
        self.side = choose_known(metadata.get("side"), self.side)
        if isinstance(metadata.get("reduce_only"), bool):
            self.reduce_only = metadata["reduce_only"]
        self.first_seen_ms = row["sort_ts_ms"] if self.first_seen_ms is None else self.first_seen_ms
        self.last_seen_ms = row["sort_ts_ms"]

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "coin": self.coin,
            "side": self.side,
            "reduce_only": self.reduce_only,
            "status": self.status,
            "first_seen_ms": self.first_seen_ms,
            "last_seen_ms": self.last_seen_ms,
            "activated_count": self.activated_count,
            "terminal_count": self.terminal_count,
            "finished_count": self.finished_count,
            "terminated_count": self.terminated_count,
            "error_count": self.error_count,
            "slice_fill_count": self.slice_fill_count,
            "state_refresh_count": self.state_refresh_count,
            "slice_fill_without_activation": self.slice_fill_without_activation,
        }


class SlotStateTracker:
    def __init__(
        self,
        *,
        rows_path: Path,
        slot: str,
        source_address: str | None,
        follower_subaccount: str | None,
        facts_out: Path | None,
        sample_size: int,
    ) -> None:
        self.rows_path = rows_path
        self.slot = slot
        self.source_address = source_address.lower() if source_address else None
        self.follower_subaccount = follower_subaccount.lower() if follower_subaccount else None
        self.facts_out = facts_out
        self.sample_size = sample_size

        self.input_rows_seen = 0
        self.rows_processed = 0
        self.skipped_by_address = 0
        self.first_sort_ts_ms: int | None = None
        self.last_sort_ts_ms: int | None = None
        self.address_counts: Counter[str] = Counter()
        self.category_counts: Counter[str] = Counter()
        self.source_action_counts: Counter[str] = Counter()
        self.decision_counts: Counter[str] = Counter()
        self.confidence_counts: Counter[str] = Counter()
        self.fact_counts: Counter[str] = Counter()

        self.orders: dict[str, OrderState] = {}
        self.twaps: dict[str, TwapState] = {}
        self.order_unmatched_terminal_count = 0
        self.order_unmatched_terminal_samples: list[dict[str, Any]] = []
        self.source_cancel_request_count = 0
        self.source_cancel_matched_count = 0
        self.source_cancel_unmatched_count = 0
        self.source_cancel_unmatched_samples: list[dict[str, Any]] = []
        self.twap_unmatched_terminal_count = 0
        self.twap_unmatched_terminal_samples: list[dict[str, Any]] = []
        self.twap_slice_without_known_id = 0

        self.source_fill_count = 0
        self.twap_slice_fill_count = 0
        self.fill_coin_counts: Counter[str] = Counter()
        self.fill_dir_counts: Counter[str] = Counter()
        self.fill_side_counts: Counter[str] = Counter()
        self.twap_slice_coin_counts: Counter[str] = Counter()
        self.source_fill_notional_usd = 0.0
        self.source_fill_notional_observations = 0
        self.twap_slice_notional_usd = 0.0
        self.twap_slice_notional_observations = 0

        self.passive_snapshot_counts: Counter[str] = Counter()
        self.state_refresh_counts: Counter[str] = Counter()
        self.rest_snapshot_count = 0
        self.twap_state_refresh_count = 0
        self.latest_account_value_usd: float | None = None
        self.latest_account_value_ts_ms: int | None = None
        self.account_value_observations = 0
        self.position_snapshot_observations = 0
        self.latest_position_count = 0
        self.latest_position_coins: list[str] = []
        self.latest_position_leverage_by_coin: dict[str, str] = {}
        self.latest_position_leverage_counts: dict[str, int] = {}
        self.latest_position_notional_usd: float | None = None
        self.latest_position_notional_observations = 0
        self.latest_position_margin_used_usd: float | None = None
        self.latest_position_margin_used_observations = 0
        self.latest_position_unrealized_pnl_usd: float | None = None
        self.latest_position_unrealized_pnl_observations = 0
        self.account_state_counts: Counter[str] = Counter()
        self.ledger_type_counts: Counter[str] = Counter()
        self.funding_coin_counts: Counter[str] = Counter()
        self.ledger_amount_usd = 0.0
        self.ledger_amount_observations = 0
        self.funding_amount_usd = 0.0
        self.funding_amount_observations = 0
        self.equity_confidence = "unknown"
        self.equity_confidence_reason = "no account-value evidence processed"
        self.confidence_downgrade_reasons: Counter[str] = Counter()

        self.stream_state = "trusted"
        self.live_stream_hints_allowed = True
        self.pending_rest_backfill = False
        self.pending_reconcile = False
        self.degraded_events = 0
        self.reconnect_recovered_events = 0
        self.state_refreshes_after_reconnect = 0
        self.recovery_rows: list[dict[str, Any]] = []
        self.recovery_windows: list[dict[str, Any]] = []
        self.current_recovery_window: dict[str, Any] | None = None

        self.unknown_rows: list[dict[str, Any]] = []
        self.fact_samples: list[dict[str, Any]] = []

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
        self.decision_counts[row["classifier_decision"]] += 1
        self.confidence_counts[row["confidence"]] += 1

        category = row["category"]
        if category != "recovery":
            self._record_recovery_followup(row)
        if category == "source_action":
            facts = self._observe_source_action(row)
        elif category == "state_refresh":
            facts = self._observe_state_refresh(row)
        elif category == "account_state":
            facts = self._observe_account_state(row)
        elif category == "recovery":
            facts = self._observe_recovery(row)
        elif category == "passive_snapshot":
            facts = self._observe_passive_snapshot(row)
        elif category in {"passive_control", "connection_control"}:
            facts = [
                self._fact(
                    row,
                    fact_type="control_observed",
                    entity_type=category,
                    entity_id=row["source_action"],
                    confidence=row["confidence"],
                    reason=row["reason"],
                    metadata={"classifier_decision": row["classifier_decision"]},
                )
            ]
        elif category == "risk_event":
            self._downgrade("risk_event")
            facts = [
                self._fact(
                    row,
                    fact_type="risk_event_seen",
                    entity_type="source",
                    entity_id=row["source_action"],
                    confidence="low",
                    reason=row["reason"],
                    metadata={"source_action": row["source_action"]},
                )
            ]
        else:
            if len(self.unknown_rows) < self.sample_size:
                self.unknown_rows.append(row)
            self._downgrade("unknown_scorecard_row")
            facts = [
                self._fact(
                    row,
                    fact_type="unknown_row_needs_review",
                    entity_type="scorecard_row",
                    entity_id=row["row_id"],
                    confidence="low",
                    reason="unrecognized scorecard category",
                    metadata={
                        "category": category,
                        "source_action": row["source_action"],
                        "classifier_decision": row["classifier_decision"],
                    },
                )
            ]

        for fact in facts:
            self.fact_counts[fact["fact_type"]] += 1
            if len(self.fact_samples) < self.sample_size:
                self.fact_samples.append(fact)
        return facts

    def _observe_source_action(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        action = row["source_action"]
        if action == "source_order_open":
            return [self._observe_order_open(row)]
        if action == "source_non_user_cancel":
            return [self._observe_source_cancel_request(row)]
        if action in TERMINAL_ORDER_ACTIONS or action.startswith("source_order_"):
            return [self._observe_order_terminal(row)]
        if action == "source_fill":
            return [self._observe_source_fill(row)]
        if action == "source_twap_activated":
            return [self._observe_twap_activation(row)]
        if action == "source_twap_slice_fill":
            return [self._observe_twap_slice_fill(row)]
        if action in TERMINAL_TWAP_ACTIONS:
            return [self._observe_twap_terminal(row)]
        return [
            self._fact(
                row,
                fact_type="source_action_unmodeled",
                entity_type="source_action",
                entity_id=action,
                confidence="low",
                reason="source action is classified but not yet modeled by the state tracker",
                metadata={
                    "source_action": action,
                    "classifier_decision": row["classifier_decision"],
                },
            )
        ]

    def _observe_order_open(self, row: dict[str, Any]) -> dict[str, Any]:
        order = self._order(row)
        order.observe_metadata(row)
        order.status = "open"
        order.opened_count += 1
        return self._fact(
            row,
            fact_type="order_open_seen",
            entity_type="source_order",
            entity_id=order.key,
            confidence="medium",
            reason="source order lifecycle open observed",
            metadata=order_fact_metadata(order),
        )

    def _observe_order_terminal(self, row: dict[str, Any]) -> dict[str, Any]:
        key = order_key(row)
        existed = key in self.orders
        order = self._order(row)
        order.observe_metadata(row)
        action = row["source_action"]
        terminal_status = clean(row["metadata"].get("status")).lower()
        if terminal_status == "unknown":
            terminal_status = action.removeprefix("source_order_")
        order.status = terminal_status
        order.terminal_count += 1
        if action == "source_order_filled":
            order.filled_count += 1
        elif action == "source_order_cancel":
            order.canceled_count += 1
        elif action == "source_order_reject":
            order.rejected_count += 1
            self._downgrade("source_order_rejected")
        else:
            self._downgrade(f"unmodeled_order_terminal_{terminal_status}")
        if not existed:
            self.order_unmatched_terminal_count += 1
            if len(self.order_unmatched_terminal_samples) < self.sample_size:
                self.order_unmatched_terminal_samples.append(row)
        return self._fact(
            row,
            fact_type="order_terminal_seen",
            entity_type="source_order",
            entity_id=order.key,
            confidence="medium" if existed else "low",
            reason=f"source order terminal status {terminal_status} observed",
            metadata={**order_fact_metadata(order), "matched_prior_open": existed},
        )

    def _observe_source_cancel_request(self, row: dict[str, Any]) -> dict[str, Any]:
        metadata = row["metadata"]
        key = order_key(row)
        order = self.orders.get(key)
        matched = order is not None
        self.source_cancel_request_count += 1
        if order is not None:
            order.observe_metadata(row)
            order.cancel_request_count += 1
            self.source_cancel_matched_count += 1
        else:
            self.source_cancel_unmatched_count += 1
            self._downgrade("source_cancel_requires_reconcile")
            if len(self.source_cancel_unmatched_samples) < self.sample_size:
                self.source_cancel_unmatched_samples.append(row)
        return self._fact(
            row,
            fact_type="source_cancel_seen",
            entity_type="source_order",
            entity_id=key,
            confidence="medium" if matched else "low",
            reason="source cancel event requires mapped-order cancel or reconcile",
            metadata={
                "order_key": key,
                "matched_known_order": matched,
                "coin": clean(metadata.get("coin")),
                "side": clean(metadata.get("side")),
                "oid": clean(metadata.get("oid")),
                "cloid": clean(metadata.get("cloid")),
                "target_action": "cancel_mapped_order_or_reconcile",
            },
        )

    def _observe_source_fill(self, row: dict[str, Any]) -> dict[str, Any]:
        metadata = row["metadata"]
        self.source_fill_count += 1
        coin = clean(metadata.get("coin"))
        side = clean(metadata.get("side"))
        direction = clean(metadata.get("dir"))
        notional = parse_optional_float(metadata.get("notional_usd"))
        if notional is not None:
            self.source_fill_notional_usd += notional
            self.source_fill_notional_observations += 1
        self.fill_coin_counts[coin] += 1
        self.fill_side_counts[side] += 1
        self.fill_dir_counts[direction] += 1
        key = order_key(row)
        order = self.orders.get(key)
        if order is not None:
            order.source_fill_events += 1
            order.observe_metadata(row)
        return self._fact(
            row,
            fact_type="source_fill_seen",
            entity_type="source_fill",
            entity_id=fill_key(row),
            confidence="medium",
            reason="source fill updates target-position validation context",
            metadata={
                "coin": coin,
                "side": side,
                "dir": direction,
                "order_key": key,
                "twap_id": clean(metadata.get("twap_id")),
                "matched_known_order": order is not None,
                "px": clean(metadata.get("px")),
                "sz": clean(metadata.get("sz")),
                "source_notional_usd": notional,
                "closed_pnl_usd": parse_optional_float(metadata.get("closed_pnl_usd")),
                "fee_usd": parse_optional_float(metadata.get("fee_usd")),
                "price_size_available": notional is not None,
            },
        )

    def _observe_twap_activation(self, row: dict[str, Any]) -> dict[str, Any]:
        twap = self._twap(row)
        twap.observe_metadata(row)
        twap.status = "active"
        twap.activated_count += 1
        return self._fact(
            row,
            fact_type="twap_activated_seen",
            entity_type="source_twap",
            entity_id=twap.key,
            confidence="medium",
            reason="source TWAP activation observed",
            metadata=twap_fact_metadata(twap),
        )

    def _observe_twap_slice_fill(self, row: dict[str, Any]) -> dict[str, Any]:
        metadata = row["metadata"]
        self.twap_slice_fill_count += 1
        coin = clean(metadata.get("coin"))
        notional = parse_optional_float(metadata.get("notional_usd"))
        if notional is not None:
            self.twap_slice_notional_usd += notional
            self.twap_slice_notional_observations += 1
        self.twap_slice_coin_counts[coin] += 1
        key = twap_key(row)
        if key == "twap:unknown":
            self.twap_slice_without_known_id += 1
            matched = False
        else:
            twap = self._twap(row)
            twap.observe_metadata(row)
            twap.slice_fill_count += 1
            if twap.activated_count == 0:
                twap.slice_fill_without_activation = True
            matched = twap.activated_count > 0
        return self._fact(
            row,
            fact_type="twap_slice_fill_seen",
            entity_type="source_twap",
            entity_id=key,
            confidence="medium" if matched else "low",
            reason="source TWAP slice fill updates TWAP progress and drift context",
            metadata={
                "coin": coin,
                "side": clean(metadata.get("side")),
                "dir": clean(metadata.get("dir")),
                "twap_id": clean(metadata.get("twap_id")),
                "matched_activation": matched,
                "px": clean(metadata.get("px")),
                "sz": clean(metadata.get("sz")),
                "source_notional_usd": notional,
                "closed_pnl_usd": parse_optional_float(metadata.get("closed_pnl_usd")),
                "fee_usd": parse_optional_float(metadata.get("fee_usd")),
                "price_size_available": notional is not None,
            },
        )

    def _observe_twap_terminal(self, row: dict[str, Any]) -> dict[str, Any]:
        key = twap_key(row)
        existed = key in self.twaps
        twap = self._twap(row)
        twap.observe_metadata(row)
        status = TERMINAL_TWAP_ACTIONS[row["source_action"]]
        twap.status = status
        twap.terminal_count += 1
        if status == "finished":
            twap.finished_count += 1
        elif status == "terminated":
            twap.terminated_count += 1
        elif status == "error":
            twap.error_count += 1
            self._downgrade("source_twap_error")
        if not existed:
            self.twap_unmatched_terminal_count += 1
            if len(self.twap_unmatched_terminal_samples) < self.sample_size:
                self.twap_unmatched_terminal_samples.append(row)
        return self._fact(
            row,
            fact_type="twap_terminal_seen",
            entity_type="source_twap",
            entity_id=twap.key,
            confidence="medium" if existed and status != "error" else "low",
            reason=f"source TWAP terminal status {status} observed",
            metadata={**twap_fact_metadata(twap), "matched_prior_activation_or_refresh": existed},
        )

    def _observe_state_refresh(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        action = row["source_action"]
        self.state_refresh_counts[action] += 1
        if self.pending_reconcile:
            self.state_refreshes_after_reconnect += 1
        if action == "rest_snapshot":
            return [self._observe_rest_snapshot(row)]
        if action == "twap_state_refresh":
            return [self._observe_twap_state_refresh(row)]
        return [
            self._fact(
                row,
                fact_type="state_refresh_seen",
                entity_type="source_state",
                entity_id=action,
                confidence=row["confidence"],
                reason=row["reason"],
                metadata={"source_action": action},
            )
        ]

    def _observe_rest_snapshot(self, row: dict[str, Any]) -> dict[str, Any]:
        self.rest_snapshot_count += 1
        metadata = row["metadata"]
        account_value = parse_optional_float(metadata.get("account_value_usd"))
        error_count = parse_optional_int(metadata.get("error_count"))
        position_count = parse_optional_int(metadata.get("position_count"))
        if account_value is not None:
            self.latest_account_value_usd = account_value
            self.latest_account_value_ts_ms = row["sort_ts_ms"]
            self.account_value_observations += 1
        if position_count is not None:
            self.position_snapshot_observations += 1
            self.latest_position_count = position_count
            self.latest_position_coins = list_clean(metadata.get("position_coins"))
            self.latest_position_leverage_by_coin = dict_clean(
                metadata.get("position_leverage_by_coin")
            )
            self.latest_position_leverage_counts = dict_int(
                metadata.get("position_leverage_counts")
            )
            self.latest_position_notional_usd = parse_optional_float(
                metadata.get("position_notional_usd")
            )
            self.latest_position_notional_observations = (
                parse_optional_int(metadata.get("position_notional_observations")) or 0
            )
            self.latest_position_margin_used_usd = parse_optional_float(
                metadata.get("position_margin_used_usd")
            )
            self.latest_position_margin_used_observations = (
                parse_optional_int(metadata.get("position_margin_used_observations")) or 0
            )
            self.latest_position_unrealized_pnl_usd = parse_optional_float(
                metadata.get("position_unrealized_pnl_usd")
            )
            self.latest_position_unrealized_pnl_observations = (
                parse_optional_int(metadata.get("position_unrealized_pnl_observations")) or 0
            )
        if error_count and error_count > 0:
            self.equity_confidence = "low"
            self.equity_confidence_reason = "REST snapshot carried request errors"
            self._downgrade("rest_snapshot_error")
        elif account_value is not None:
            self.equity_confidence = "high"
            self.equity_confidence_reason = "latest REST snapshot carried account_value_usd"
        else:
            self.equity_confidence = "medium"
            self.equity_confidence_reason = "REST snapshot lacked account_value_usd"
            self._downgrade("missing_account_value")
        return self._fact(
            row,
            fact_type="rest_snapshot_seen",
            entity_type="source_account",
            entity_id=row["address"],
            confidence=self.equity_confidence,
            reason=self.equity_confidence_reason,
            metadata={
                "account_value_usd": account_value,
                "error_count": error_count,
                "ok_count": parse_optional_int(metadata.get("ok_count")),
                "request_types": metadata.get("request_types", []),
                "recovery_pending": self.pending_reconcile,
                "position_count": position_count,
                "position_coins": list_clean(metadata.get("position_coins")),
                "position_leverage_by_coin": dict_clean(metadata.get("position_leverage_by_coin")),
                "position_leverage_counts": dict_int(metadata.get("position_leverage_counts")),
                "position_notional_usd": parse_optional_float(
                    metadata.get("position_notional_usd")
                ),
                "position_notional_observations": parse_optional_int(
                    metadata.get("position_notional_observations")
                )
                or 0,
                "position_margin_used_usd": parse_optional_float(
                    metadata.get("position_margin_used_usd")
                ),
                "position_margin_used_observations": parse_optional_int(
                    metadata.get("position_margin_used_observations")
                )
                or 0,
                "position_unrealized_pnl_usd": parse_optional_float(
                    metadata.get("position_unrealized_pnl_usd")
                ),
                "position_unrealized_pnl_observations": parse_optional_int(
                    metadata.get("position_unrealized_pnl_observations")
                )
                or 0,
            },
        )

    def _observe_twap_state_refresh(self, row: dict[str, Any]) -> dict[str, Any]:
        self.twap_state_refresh_count += 1
        twap = self._twap(row)
        twap.observe_metadata(row)
        twap.state_refresh_count += 1
        if twap.status == "unknown":
            twap.status = "state_refreshed"
        return self._fact(
            row,
            fact_type="twap_state_refresh_seen",
            entity_type="source_twap",
            entity_id=twap.key,
            confidence="medium",
            reason="source TWAP state refresh observed without follower action",
            metadata=twap_fact_metadata(twap),
        )

    def _observe_account_state(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        action = row["source_action"]
        metadata = row["metadata"]
        self.account_state_counts[action] += 1
        funding_amount = None
        ledger_amount = None
        if action == "funding_update":
            funding_amount = parse_optional_float(metadata.get("funding_amount_usd"))
            if funding_amount is None:
                funding_amount = parse_optional_float(metadata.get("usdc"))
            if funding_amount is not None:
                self.funding_amount_usd += funding_amount
                self.funding_amount_observations += 1
            self.funding_coin_counts[clean(metadata.get("coin"))] += 1
            self._mark_account_context_stale("funding_update_requires_fresh_snapshot")
        elif action.startswith("ledger_"):
            ledger_type = clean(metadata.get("ledger_type"))
            ledger_amount = parse_optional_float(metadata.get("ledger_amount_usd"))
            if ledger_amount is None:
                ledger_amount = parse_optional_float(metadata.get("usdc"))
            if ledger_amount is not None:
                self.ledger_amount_usd += ledger_amount
                self.ledger_amount_observations += 1
            self.ledger_type_counts[ledger_type] += 1
            self._mark_account_context_stale(f"ledger_{ledger_type}_requires_fresh_snapshot")
        else:
            self._mark_account_context_stale("account_state_update_requires_review")
        return [
            self._fact(
                row,
                fact_type="account_context_update",
                entity_type="source_account",
                entity_id=row["address"],
                confidence=self.equity_confidence,
                reason=self.equity_confidence_reason,
                metadata={
                    "source_action": action,
                    "ledger_type": clean(metadata.get("ledger_type")),
                    "coin": clean(metadata.get("coin")),
                    "funding_amount_usd": funding_amount,
                    "ledger_amount_usd": ledger_amount,
                    "usdc": clean(metadata.get("usdc")),
                    "to_perp": (
                        metadata.get("to_perp")
                        if isinstance(metadata.get("to_perp"), bool)
                        else None
                    ),
                    "latest_account_value_usd": self.latest_account_value_usd,
                },
            )
        ]

    def _observe_recovery(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        action = row["source_action"]
        if len(self.recovery_rows) < self.sample_size:
            self.recovery_rows.append(row)
        self.live_stream_hints_allowed = False
        if action == "stream_degraded":
            self._start_recovery_window(row)
            self.stream_state = "degraded"
            self.pending_rest_backfill = True
            self.pending_reconcile = True
            self.degraded_events += 1
            self._downgrade("stream_degraded")
            fact_type = "stream_degraded"
            reason = "live stream hints paused until backfill and reconciliation complete"
        elif action == "reconnect_recovered":
            self._mark_reconnect_recovered(row)
            self.stream_state = "recovery_required"
            self.pending_rest_backfill = True
            self.pending_reconcile = True
            self.reconnect_recovered_events += 1
            self._downgrade("reconnect_requires_rest_backfill")
            fact_type = "recovery_requires_backfill"
            reason = (
                "reconnect observed; REST backfill and source/follower reconcile still required"
            )
        else:
            self.stream_state = "recovery_review"
            self.pending_rest_backfill = True
            self.pending_reconcile = True
            self._downgrade("unknown_recovery_event")
            fact_type = "recovery_needs_review"
            reason = "unknown recovery event requires manual review"
        return [
            self._fact(
                row,
                fact_type=fact_type,
                entity_type="stream_recovery",
                entity_id=action,
                confidence="low" if action != "stream_degraded" else "medium",
                reason=reason,
                metadata={
                    "gap_ms": parse_optional_int(row["metadata"].get("gap_ms")),
                    "pending_rest_backfill": self.pending_rest_backfill,
                    "pending_reconcile": self.pending_reconcile,
                    "live_stream_hints_allowed": self.live_stream_hints_allowed,
                },
            )
        ]

    def _start_recovery_window(self, row: dict[str, Any]) -> None:
        window = {
            "sequence": len(self.recovery_windows) + 1,
            "status": "stream_degraded_pending_reconnect",
            "complete": False,
            "degraded_ts_ms": row["sort_ts_ms"],
            "reconnected_ts_ms": None,
            "gap_ms": None,
            "degraded_row_id": row["row_id"],
            "reconnect_row_id": "",
            "source_error": clean(row["metadata"].get("source_error")),
            "post_reconnect_state_refreshes": 0,
            "post_reconnect_rest_snapshots": 0,
            "post_reconnect_account_context_rows": 0,
            "post_reconnect_source_actions": 0,
            "first_post_reconnect_state_refresh_ms": None,
            "first_post_reconnect_rest_snapshot_ms": None,
            "explicit_rest_backfill_complete": False,
            "explicit_reconcile_complete": False,
            "live_stream_hints_allowed_after_window": False,
            "missing_requirements": [
                "explicit_rest_backfill_complete",
                "explicit_reconcile_complete",
                "live_stream_hints_allowed_after_window",
            ],
        }
        self.recovery_windows.append(window)
        self.current_recovery_window = window

    def _mark_reconnect_recovered(self, row: dict[str, Any]) -> None:
        if self.current_recovery_window is None or self.current_recovery_window.get(
            "reconnected_ts_ms"
        ):
            self._start_recovery_window(
                {
                    **row,
                    "source_action": "stream_degraded",
                    "row_id": f"{row['row_id']}:implicit_degraded",
                    "metadata": {
                        "source_error": clean(row["metadata"].get("source_error")),
                    },
                }
            )
        assert self.current_recovery_window is not None
        self.current_recovery_window.update(
            {
                "status": "requires_rest_backfill_and_reconcile",
                "reconnected_ts_ms": row["sort_ts_ms"],
                "gap_ms": parse_optional_int(row["metadata"].get("gap_ms")),
                "reconnect_row_id": row["row_id"],
            }
        )

    def _record_recovery_followup(self, row: dict[str, Any]) -> None:
        window = self.current_recovery_window
        if window is None or window.get("reconnected_ts_ms") is None or window.get("complete"):
            return
        category = row["category"]
        action = row["source_action"]
        if category == "state_refresh":
            window["post_reconnect_state_refreshes"] += 1
            if window["first_post_reconnect_state_refresh_ms"] is None:
                window["first_post_reconnect_state_refresh_ms"] = row["sort_ts_ms"]
            if action == "rest_snapshot":
                window["post_reconnect_rest_snapshots"] += 1
                if window["first_post_reconnect_rest_snapshot_ms"] is None:
                    window["first_post_reconnect_rest_snapshot_ms"] = row["sort_ts_ms"]
        elif category == "account_state":
            window["post_reconnect_account_context_rows"] += 1
        elif category == "source_action":
            window["post_reconnect_source_actions"] += 1

    def _observe_passive_snapshot(self, row: dict[str, Any]) -> list[dict[str, Any]]:
        decision = row["classifier_decision"]
        self.passive_snapshot_counts[decision] += 1
        if decision == "duplicate_snapshot_skipped":
            fact_type = "duplicate_snapshot_skipped"
            reason = "repeated subscription snapshot was ignored as a new source action"
        else:
            fact_type = "snapshot_seed_or_refresh_seen"
            reason = "subscription snapshot can seed state but is not a copy action"
        return [
            self._fact(
                row,
                fact_type=fact_type,
                entity_type="subscription_snapshot",
                entity_id=snapshot_entity_id(row),
                confidence="high",
                reason=reason,
                metadata={
                    "event_type": row["event_type"],
                    "subtype": row["subtype"],
                    "source_action": row["source_action"],
                },
            )
        ]

    def _order(self, row: dict[str, Any]) -> OrderState:
        key = order_key(row)
        if key not in self.orders:
            self.orders[key] = OrderState(key=key)
        return self.orders[key]

    def _twap(self, row: dict[str, Any]) -> TwapState:
        key = twap_key(row)
        if key not in self.twaps:
            self.twaps[key] = TwapState(key=key)
        return self.twaps[key]

    def _mark_account_context_stale(self, reason: str) -> None:
        if self.equity_confidence == "unknown":
            self.equity_confidence = "medium"
        elif self.equity_confidence == "high":
            self.equity_confidence = "medium"
        self.equity_confidence_reason = reason
        self._downgrade(reason)

    def _downgrade(self, reason: str) -> None:
        self.confidence_downgrade_reasons[reason] += 1

    def _fact(
        self,
        row: dict[str, Any],
        *,
        fact_type: str,
        entity_type: str,
        entity_id: str,
        confidence: str,
        reason: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        fact_payload = f"{row['row_id']}|{fact_type}|{entity_type}|{entity_id}"
        return {
            "fact_id": hashlib.sha256(fact_payload.encode("utf-8")).hexdigest()[:24],
            "state_tracker_version": STATE_TRACKER_VERSION,
            "slot": self.slot,
            "address": row["address"],
            "sort_ts_ms": row["sort_ts_ms"],
            "row_id": row["row_id"],
            "event_id": row["event_id"],
            "fact_type": fact_type,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "confidence": confidence,
            "policy_neutral": True,
            "reason": reason,
            "metadata": metadata,
        }

    def summary(self, *, truncated_by_limit: bool) -> dict[str, Any]:
        order_status_counts = Counter(order.status for order in self.orders.values())
        twap_status_counts = Counter(twap.status for twap in self.twaps.values())
        active_twaps = [
            twap.as_dict()
            for twap in sorted(self.twaps.values(), key=lambda item: item.key)
            if twap.status in {"active", "state_refreshed"}
        ]
        error_twaps = [
            twap.as_dict()
            for twap in sorted(self.twaps.values(), key=lambda item: item.key)
            if twap.error_count
        ]
        open_orders = [
            order.as_dict()
            for order in sorted(self.orders.values(), key=lambda item: item.key)
            if order.status == "open"
        ]
        return {
            "state_tracker_version": STATE_TRACKER_VERSION,
            "read_only": True,
            "exchange_touched": False,
            "rows_path": str(self.rows_path),
            "facts_out": str(self.facts_out) if self.facts_out is not None else None,
            "slot": {
                "slot": self.slot,
                "source_address": self.source_address,
                "follower_subaccount": self.follower_subaccount,
                "follower_state_tracked": False,
                "follower_state_reason": (
                    "scorecard rows contain source replay state only; follower truth must be "
                    "added by a later shadow slot actor"
                ),
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
                "by_classifier_decision": counter_dict(self.decision_counts),
                "by_confidence": counter_dict(self.confidence_counts),
                "by_fact_type": counter_dict(self.fact_counts),
            },
            "orders": {
                "seen": len(self.orders),
                "open": order_status_counts.get("open", 0),
                "terminal": sum(
                    count for status, count in order_status_counts.items() if status != "open"
                ),
                "by_status": counter_dict(order_status_counts),
                "unmatched_terminal_updates": self.order_unmatched_terminal_count,
                "open_order_samples": open_orders[: self.sample_size],
                "unmatched_terminal_samples": self.order_unmatched_terminal_samples,
            },
            "source_cancels": {
                "requests": self.source_cancel_request_count,
                "matched_known_order": self.source_cancel_matched_count,
                "unmatched_requires_reconcile": self.source_cancel_unmatched_count,
                "unmatched_samples": self.source_cancel_unmatched_samples,
            },
            "twaps": {
                "seen": len(self.twaps),
                "active": sum(
                    count
                    for status, count in twap_status_counts.items()
                    if status in {"active", "state_refreshed"}
                ),
                "terminal": sum(
                    count
                    for status, count in twap_status_counts.items()
                    if status in {"finished", "terminated", "error"}
                ),
                "by_status": counter_dict(twap_status_counts),
                "slice_fills": self.twap_slice_fill_count,
                "slice_fills_by_coin": counter_dict(self.twap_slice_coin_counts),
                "slice_fills_without_known_twap_id": self.twap_slice_without_known_id,
                "unmatched_terminal_updates": self.twap_unmatched_terminal_count,
                "active_samples": active_twaps[: self.sample_size],
                "error_samples": error_twaps[: self.sample_size],
                "unmatched_terminal_samples": self.twap_unmatched_terminal_samples,
            },
            "fills": {
                "source_fills": self.source_fill_count,
                "twap_slice_fills": self.twap_slice_fill_count,
                "source_fills_by_coin": counter_dict(self.fill_coin_counts),
                "source_fills_by_dir": counter_dict(self.fill_dir_counts),
                "source_fills_by_side": counter_dict(self.fill_side_counts),
                "source_fill_notional_usd": round(self.source_fill_notional_usd, 8),
                "source_fill_notional_observations": self.source_fill_notional_observations,
                "twap_slice_notional_usd": round(self.twap_slice_notional_usd, 8),
                "twap_slice_notional_observations": self.twap_slice_notional_observations,
            },
            "account_context": {
                "rest_snapshots": self.rest_snapshot_count,
                "twap_state_refreshes": self.twap_state_refresh_count,
                "state_refreshes_by_action": counter_dict(self.state_refresh_counts),
                "account_state_updates_by_action": counter_dict(self.account_state_counts),
                "ledger_updates_by_type": counter_dict(self.ledger_type_counts),
                "funding_updates_by_coin": counter_dict(self.funding_coin_counts),
                "ledger_amount_usd": round(self.ledger_amount_usd, 8),
                "ledger_amount_observations": self.ledger_amount_observations,
                "funding_amount_usd": round(self.funding_amount_usd, 8),
                "funding_amount_observations": self.funding_amount_observations,
                "net_account_context_amount_usd": round(
                    self.ledger_amount_usd + self.funding_amount_usd, 8
                ),
                "latest_account_value_usd": self.latest_account_value_usd,
                "latest_account_value_ts_ms": self.latest_account_value_ts_ms,
                "account_value_observations": self.account_value_observations,
                "position_snapshot_observations": self.position_snapshot_observations,
                "latest_position_count": self.latest_position_count,
                "latest_position_coins": self.latest_position_coins,
                "latest_position_leverage_by_coin": self.latest_position_leverage_by_coin,
                "latest_position_leverage_counts": self.latest_position_leverage_counts,
                "latest_position_notional_usd": self.latest_position_notional_usd,
                "latest_position_notional_observations": (
                    self.latest_position_notional_observations
                ),
                "latest_position_margin_used_usd": self.latest_position_margin_used_usd,
                "latest_position_margin_used_observations": (
                    self.latest_position_margin_used_observations
                ),
                "latest_position_unrealized_pnl_usd": (self.latest_position_unrealized_pnl_usd),
                "latest_position_unrealized_pnl_observations": (
                    self.latest_position_unrealized_pnl_observations
                ),
                "equity_confidence": self.equity_confidence,
                "equity_confidence_reason": self.equity_confidence_reason,
                "confidence_downgrade_reasons": counter_dict(self.confidence_downgrade_reasons),
            },
            "recovery": {
                "stream_state": self.stream_state,
                "live_stream_hints_allowed": self.live_stream_hints_allowed,
                "pending_rest_backfill": self.pending_rest_backfill,
                "pending_reconcile": self.pending_reconcile,
                "degraded_events": self.degraded_events,
                "reconnect_recovered_events": self.reconnect_recovered_events,
                "state_refreshes_after_reconnect": self.state_refreshes_after_reconnect,
                "windows": self.recovery_windows,
                "sample_rows": self.recovery_rows,
            },
            "passive_snapshots": counter_dict(self.passive_snapshot_counts),
            "policy_neutral_shadow": {
                "facts_emitted": sum(self.fact_counts.values()),
                "latest_source_account_value_available": self.latest_account_value_usd is not None,
                "fill_price_size_available": (
                    self.source_fill_notional_observations + self.twap_slice_notional_observations
                )
                > 0,
                "sizing_policy_applied": False,
                "next_supported_policy_work": [
                    "pure_compound_vs_fixed_risk_budget",
                    "dust_and_min_notional_accounting",
                    "follower_truth_and_drift_scoring",
                ],
            },
            "sample_facts": self.fact_samples,
            "unknown_rows": self.unknown_rows,
        }


def track_replay_state(
    rows_path: Path,
    *,
    facts_out: Path | None = None,
    source_address: str | None = None,
    follower_subaccount: str | None = None,
    slot: str = "replay-slot",
    limit_rows: int | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    if sample_size < 0:
        raise StateTrackerInputError("sample_size must be non-negative")
    if limit_rows is not None and limit_rows < 0:
        raise StateTrackerInputError("limit_rows must be non-negative")

    tracker = SlotStateTracker(
        rows_path=rows_path,
        slot=slot,
        source_address=source_address,
        follower_subaccount=follower_subaccount,
        facts_out=facts_out,
        sample_size=sample_size,
    )
    fact_handle = None
    if facts_out is not None:
        facts_out.parent.mkdir(parents=True, exist_ok=True)
        fact_handle = facts_out.open("w", encoding="utf-8")
    try:
        for row in iter_scorecard_rows(rows_path):
            if not tracker.should_process(row):
                continue
            if limit_rows is not None and tracker.rows_processed >= limit_rows:
                break
            facts = tracker.observe(row)
            if fact_handle is not None:
                for fact in facts:
                    fact_handle.write(json.dumps(fact, separators=(",", ":"), sort_keys=True))
                    fact_handle.write("\n")
    finally:
        if fact_handle is not None:
            fact_handle.close()
    return tracker.summary(
        truncated_by_limit=limit_rows is not None and tracker.rows_processed >= limit_rows
    )


def iter_scorecard_rows(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        raise StateTrackerInputError(f"scorecard rows file does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise StateTrackerInputError(f"{path}:{line_no} invalid JSON: {exc.msg}") from exc
            if not isinstance(row, dict):
                raise StateTrackerInputError(f"{path}:{line_no} scorecard row must be an object")
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
            raise StateTrackerInputError(f"{path}:{line_no} scorecard row missing {key}")
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


def order_key(row: dict[str, Any]) -> str:
    metadata = row["metadata"]
    oid = clean(metadata.get("oid"))
    if oid != "unknown":
        return f"oid:{oid}"
    cloid = clean(metadata.get("cloid"))
    if cloid != "unknown":
        return f"cloid:{cloid}"
    coin = clean(metadata.get("coin"))
    side = clean(metadata.get("side"))
    return f"synthetic-order:{coin}:{side}:{row['row_id']}"


def twap_key(row: dict[str, Any]) -> str:
    twap_id = clean(row["metadata"].get("twap_id"))
    return f"twap:{twap_id}"


def fill_key(row: dict[str, Any]) -> str:
    metadata = row["metadata"]
    transaction_hash = clean(metadata.get("hash"))
    tid = clean(metadata.get("tid"))
    if transaction_hash != "unknown" and tid != "unknown":
        return f"fill:{transaction_hash}:{tid}"
    if tid != "unknown":
        return f"fill-tid:{tid}"
    return f"fill-row:{row['row_id']}"


def snapshot_entity_id(row: dict[str, Any]) -> str:
    metadata = row["metadata"]
    pieces = [
        row["event_type"],
        row["subtype"],
        clean(metadata.get("coin")),
        clean(metadata.get("oid")),
        clean(metadata.get("twap_id")),
        clean(metadata.get("hash")),
    ]
    return ":".join(pieces)


def order_fact_metadata(order: OrderState) -> dict[str, Any]:
    return {
        "order_key": order.key,
        "coin": order.coin,
        "side": order.side,
        "cloid": order.cloid,
        "status": order.status,
        "opened_count": order.opened_count,
        "terminal_count": order.terminal_count,
        "cancel_request_count": order.cancel_request_count,
        "source_fill_events": order.source_fill_events,
        "source_notional_usd": order.latest_notional_usd,
        "notional_observations": order.notional_observations,
    }


def twap_fact_metadata(twap: TwapState) -> dict[str, Any]:
    return {
        "twap_key": twap.key,
        "coin": twap.coin,
        "side": twap.side,
        "reduce_only": twap.reduce_only,
        "status": twap.status,
        "activated_count": twap.activated_count,
        "terminal_count": twap.terminal_count,
        "slice_fill_count": twap.slice_fill_count,
        "state_refresh_count": twap.state_refresh_count,
    }


def parse_int(value: Any, *, path: Path, line_no: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise StateTrackerInputError(
            f"{path}:{line_no} expected integer sort_ts_ms, got {value!r}"
        ) from exc


def parse_optional_int(value: Any) -> int | None:
    if value in (None, "", "unknown"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_optional_float(value: Any) -> float | None:
    if value in (None, "", "unknown"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def list_clean(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [clean(item) for item in value]


def dict_clean(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {clean(key): clean(item) for key, item in value.items()}


def dict_int(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, item in value.items():
        parsed = parse_optional_int(item)
        if parsed is not None:
            result[clean(key)] = parsed
    return result


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def choose_known(new_value: Any, old_value: str) -> str:
    cleaned = clean(new_value)
    if cleaned == "unknown":
        return old_value
    return cleaned


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Track conservative source slot state from read-only replay scorecard rows and emit "
            "policy-neutral shadow facts."
        )
    )
    parser.add_argument(
        "rows_path", type=Path, help="Scorecard rows JSONL from score_replay_events.py."
    )
    parser.add_argument("--out", type=Path, default=None, help="Write state report JSON.")
    parser.add_argument("--facts-out", type=Path, default=None, help="Write state facts JSONL.")
    parser.add_argument("--source-address", default=None, help="Optional source address filter.")
    parser.add_argument(
        "--follower-subaccount", default=None, help="Optional follower slot label/address."
    )
    parser.add_argument(
        "--slot", default="replay-slot", help="Slot name to stamp on generated facts."
    )
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = track_replay_state(
            args.rows_path,
            facts_out=args.facts_out,
            source_address=args.source_address,
            follower_subaccount=args.follower_subaccount,
            slot=args.slot,
            limit_rows=args.limit_rows,
            sample_size=args.sample_size,
        )
    except StateTrackerInputError as exc:
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
                    "facts_emitted": report["policy_neutral_shadow"]["facts_emitted"],
                    "exchange_touched": report["exchange_touched"],
                    "recovery_pending": report["recovery"]["pending_reconcile"],
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
