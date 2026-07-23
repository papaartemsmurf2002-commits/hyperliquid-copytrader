from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

from .cloid import deterministic_cloid
from .models import (
    FollowerIntent,
    OpenOrder,
    Position,
    SafeModeReason,
    SafeModeTransition,
    SourceEvent,
    now_ms,
)
from .markets import canonical_market_symbol
from .persistence import SQLiteStore


RECOVERABLE_TERMINAL_STATUSES = {
    "filled",
    "canceled",
    "rejected",
    "marginCanceled",
    "openInterestCapCanceled",
    "selfTradeCanceled",
    "reduceOnlyCanceled",
    "scheduledCancel",
}


class SafeModeController:
    def __init__(self, store: SQLiteStore | None = None):
        self.store = store
        self.revision = 0
        self.enabled = False
        self.reason = SafeModeReason.NONE
        self.detail = ""
        self.refresh_from_store()

    def refresh_from_store(self) -> bool:
        """Refresh from the append-only journal when another process wrote a newer transition."""

        if self.store is None:
            return False
        latest = self.store.latest_safe_mode()
        if latest is None:
            return False
        revision = int(latest.get("seq") or 0)
        if revision <= self.revision:
            return False
        try:
            reason = SafeModeReason(str(latest.get("reason") or SafeModeReason.NONE.value))
        except ValueError:
            self.trip(
                SafeModeReason.CONFIG_INVALID,
                f"safe-mode journal reason is invalid: {latest.get('reason')}",
            )
            return True
        self.revision = revision
        self.enabled = bool(latest.get("enabled"))
        self.reason = reason
        self.detail = str(latest.get("detail") or "")
        return True

    def trip(self, reason: SafeModeReason, detail: str) -> SafeModeTransition:
        self.enabled = True
        self.reason = reason
        self.detail = detail
        observed = now_ms()
        transition = SafeModeTransition(
            transition_id=deterministic_cloid("safe", reason.value, detail, observed, uuid4().hex),
            enabled=True,
            reason=reason,
            detail=detail,
            created_ms=observed,
        )
        if self.store is not None:
            self.store.append_safe_mode(transition)
            self.refresh_from_store()
        return transition

    def clear(self, detail: str = "manual resume after reconcile") -> SafeModeTransition:
        self.enabled = False
        self.reason = SafeModeReason.NONE
        self.detail = detail
        observed = now_ms()
        transition = SafeModeTransition(
            transition_id=deterministic_cloid("safe-clear", detail, observed, uuid4().hex),
            enabled=False,
            reason=SafeModeReason.NONE,
            detail=detail,
            created_ms=observed,
        )
        if self.store is not None:
            self.store.append_safe_mode(transition)
            self.refresh_from_store()
        return transition

    def clear_if_revision(
        self,
        expected_revision: int,
        detail: str = "manual resume after reconcile",
    ) -> SafeModeTransition | None:
        """Clear only if the incident inspected by the caller is still the latest transition."""

        if self.store is None:
            return self.clear(detail)
        observed = now_ms()
        transition = SafeModeTransition(
            transition_id=deterministic_cloid(
                "safe-clear",
                detail,
                observed,
                expected_revision,
                uuid4().hex,
            ),
            enabled=False,
            reason=SafeModeReason.NONE,
            detail=detail,
            created_ms=observed,
        )
        inserted = self.store.append_safe_mode_if_revision(
            transition,
            expected_seq=expected_revision,
        )
        self.refresh_from_store()
        return transition if inserted else None


@dataclass(frozen=True)
class ShieldResult:
    ok: bool
    action: str
    reason: SafeModeReason
    detail: str


class ConsistencyShield:
    def __init__(self, safe_mode: SafeModeController, *, rapid_flip_ms: int = 1500):
        self.safe_mode = safe_mode
        self.rapid_flip_ms = rapid_flip_ms
        self.last_event_ts_by_key: dict[str, int] = {}
        self.last_position_side_by_coin: dict[str, tuple[str, int]] = {}

    def observe_source_event(self, event: SourceEvent, already_seen: bool = False) -> ShieldResult:
        if already_seen:
            return ShieldResult(
                True, "deduped", SafeModeReason.DUPLICATE_EVENT, event.idempotency_key
            )

        ordering_key = self._source_event_ordering_key(event)
        previous = self.last_event_ts_by_key.get(ordering_key)
        if previous is not None and event.exchange_ts_ms and event.exchange_ts_ms < previous:
            detail = f"{event.idempotency_key} older than prior {previous} on {ordering_key}"
            self.safe_mode.trip(SafeModeReason.OUT_OF_ORDER_EVENT, detail)
            return ShieldResult(False, "paused", SafeModeReason.OUT_OF_ORDER_EVENT, detail)

        if event.exchange_ts_ms:
            self.last_event_ts_by_key[ordering_key] = event.exchange_ts_ms
        return ShieldResult(True, "accepted", SafeModeReason.NONE, "")

    @staticmethod
    def _source_event_ordering_key(event: SourceEvent) -> str:
        timestamp_source = str(event.payload.get("timestamp_source") or "exchange").lower()
        if timestamp_source not in {"exchange", "observed"}:
            timestamp_source = "exchange"
        stream = str(
            event.payload.get("channel")
            or event.payload.get("event_subtype")
            or event.event_type.value
        )
        stream = stream.strip().lower()
        if ":" in stream:
            stream = stream.split(":", 1)[0]
        if not stream:
            stream = event.event_type.value
        return f"{event.event_type.value}:{timestamp_source}:{stream}"

    def check_startup_reconcile(self, mismatches: list[str]) -> ShieldResult:
        if not mismatches:
            return ShieldResult(
                True, "recovered", SafeModeReason.STARTUP_RECONCILE, "startup clean"
            )
        detail = "; ".join(mismatches)
        self.safe_mode.trip(SafeModeReason.STARTUP_RECONCILE, detail)
        return ShieldResult(False, "paused", SafeModeReason.STARTUP_RECONCILE, detail)

    def websocket_disconnect(self, detail: str = "websocket disconnected") -> ShieldResult:
        self.safe_mode.trip(SafeModeReason.WEBSOCKET_DISCONNECT, detail)
        return ShieldResult(False, "paused", SafeModeReason.WEBSOCKET_DISCONNECT, detail)

    def rest_lag(self, lag_ms: int, threshold_ms: int) -> ShieldResult:
        if lag_ms <= threshold_ms:
            return ShieldResult(True, "accepted", SafeModeReason.NONE, "")
        detail = f"REST lag {lag_ms}ms exceeds {threshold_ms}ms"
        self.safe_mode.trip(SafeModeReason.REST_LAG, detail)
        return ShieldResult(False, "paused", SafeModeReason.REST_LAG, detail)

    def restart_mid_fill(self, pending_intents: list[FollowerIntent]) -> ShieldResult:
        if not pending_intents:
            return ShieldResult(
                True, "recovered", SafeModeReason.RESTART_MID_FILL, "no pending intents"
            )
        detail = f"{len(pending_intents)} pending intents require exchange truth"
        self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
        return ShieldResult(False, "paused", SafeModeReason.RESTART_MID_FILL, detail)

    def partial_fill(self, expected: Decimal, filled: Decimal, cloid: str) -> ShieldResult:
        if filled == expected:
            return ShieldResult(True, "filled", SafeModeReason.NONE, "")
        detail = f"{cloid} filled {filled} of {expected}"
        self.safe_mode.trip(SafeModeReason.PARTIAL_FILL, detail)
        return ShieldResult(False, "paused", SafeModeReason.PARTIAL_FILL, detail)

    def cancel_reject(self, cloid: str, order_status: str | None = None) -> ShieldResult:
        if order_status in RECOVERABLE_TERMINAL_STATUSES:
            return ShieldResult(True, "recovered", SafeModeReason.CANCEL_REJECT, order_status)
        detail = f"cancel rejected for {cloid}; status={order_status or 'unknown'}"
        self.safe_mode.trip(SafeModeReason.CANCEL_REJECT, detail)
        return ShieldResult(False, "paused", SafeModeReason.CANCEL_REJECT, detail)

    def order_timeout(self, cloid: str) -> ShieldResult:
        detail = f"order activity timed out for {cloid}"
        self.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, detail)
        return ShieldResult(False, "paused", SafeModeReason.ORDER_TIMEOUT, detail)

    def rapid_flip(self, coin: str, new_size: Decimal, now: int | None = None) -> ShieldResult:
        observed = now or now_ms()
        side = "long" if new_size > 0 else "short" if new_size < 0 else "flat"
        previous = self.last_position_side_by_coin.get(coin)
        self.last_position_side_by_coin[coin] = (side, observed)
        if previous is None:
            return ShieldResult(True, "accepted", SafeModeReason.NONE, "")
        old_side, old_ts = previous
        if old_side in {"long", "short"} and side in {"long", "short"} and old_side != side:
            if observed - old_ts <= self.rapid_flip_ms:
                detail = f"{coin} flipped {old_side}->{side} in {observed - old_ts}ms"
                self.safe_mode.trip(SafeModeReason.RAPID_FLIP, detail)
                return ShieldResult(False, "paused", SafeModeReason.RAPID_FLIP, detail)
        return ShieldResult(True, "accepted", SafeModeReason.NONE, "")

    def unsupported_symbol(self, coin: str) -> ShieldResult:
        detail = f"{coin} is not in allowlist or exchange metadata"
        self.safe_mode.trip(SafeModeReason.UNSUPPORTED_SYMBOL, detail)
        return ShieldResult(False, "paused", SafeModeReason.UNSUPPORTED_SYMBOL, detail)

    def missed_event_gap(self, detail: str) -> ShieldResult:
        self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
        return ShieldResult(False, "paused", SafeModeReason.MISSED_EVENT_GAP, detail)

    def stale_source(self, age_ms: int, threshold_ms: int) -> ShieldResult:
        if age_ms <= threshold_ms:
            return ShieldResult(True, "fresh", SafeModeReason.NONE, "")
        detail = f"source data age {age_ms}ms exceeds {threshold_ms}ms"
        self.safe_mode.trip(SafeModeReason.STALE_SOURCE, detail)
        return ShieldResult(False, "paused", SafeModeReason.STALE_SOURCE, detail)

    def stale_follower(self, age_ms: int, threshold_ms: int) -> ShieldResult:
        if age_ms <= threshold_ms:
            return ShieldResult(True, "fresh", SafeModeReason.NONE, "")
        detail = f"follower data age {age_ms}ms exceeds {threshold_ms}ms"
        self.safe_mode.trip(SafeModeReason.STALE_FOLLOWER, detail)
        return ShieldResult(False, "paused", SafeModeReason.STALE_FOLLOWER, detail)

    def manual_intervention(
        self,
        expected_positions: dict[str, Position],
        actual_positions: dict[str, Position],
        expected_open_cloids: set[str],
        actual_open_orders: list[OpenOrder],
        expected_open_orders: dict[str, OpenOrder] | None = None,
        position_size_tolerance: Decimal = Decimal("0"),
        position_notional_tolerance_usd: Decimal | None = None,
        position_mid_prices: dict[str, Decimal] | None = None,
    ) -> ShieldResult:
        actual_by_cloid: dict[str, OpenOrder] = {}
        duplicate_cloids: set[str] = set()
        for order in actual_open_orders:
            if not order.cloid:
                continue
            cloid = order.cloid.lower()
            if cloid in actual_by_cloid:
                duplicate_cloids.add(cloid)
                continue
            actual_by_cloid[cloid] = order
        actual_open_cloids = set(actual_by_cloid)
        mismatches: list[str] = []
        all_coins = set(expected_positions) | set(actual_positions)
        for coin in sorted(all_coins):
            expected = expected_positions.get(coin, Position(coin, Decimal("0")))
            actual = actual_positions.get(coin, Position(coin, Decimal("0")))
            size_delta = abs(actual.size - expected.size)
            # Dust tolerance may forgive a small residual difference in a bot-managed market,
            # but it must never adopt a brand-new nonzero exchange position into journal truth.
            if expected.size == 0 and actual.size != 0:
                tolerated_delta = False
            elif position_notional_tolerance_usd is not None:
                reference_px = (position_mid_prices or {}).get(coin)
                tolerated_delta = (
                    reference_px is not None
                    and reference_px.is_finite()
                    and reference_px > 0
                    and size_delta * reference_px < position_notional_tolerance_usd
                )
            else:
                tolerated_delta = size_delta <= position_size_tolerance
            if not tolerated_delta:
                mismatches.append(f"{coin} expected {expected.size} actual {actual.size}")
            if (
                expected.size != 0
                and actual.size != 0
                and expected.leverage is not None
                and actual.leverage != expected.leverage
            ):
                actual_leverage = "unknown" if actual.leverage is None else str(actual.leverage)
                mismatches.append(
                    f"{coin} expected leverage {expected.leverage} actual {actual_leverage}"
                )
        uncloided_orders = [order for order in actual_open_orders if not order.cloid]
        if uncloided_orders:
            coins = sorted({order.coin for order in uncloided_orders})
            mismatches.append(f"unexpected open orders without cloid on {coins}")
        missing_cloids = expected_open_cloids - actual_open_cloids
        extra_cloids = actual_open_cloids - expected_open_cloids
        if missing_cloids:
            mismatches.append(f"missing open cloids {sorted(missing_cloids)}")
        if extra_cloids:
            mismatches.append(f"unexpected open cloids {sorted(extra_cloids)}")
        if duplicate_cloids:
            mismatches.append(f"duplicate open cloids {sorted(duplicate_cloids)}")
        if expected_open_orders:
            for cloid in sorted(expected_open_cloids & actual_open_cloids):
                expected_order = expected_open_orders.get(cloid)
                actual_order = actual_by_cloid[cloid]
                if expected_order is None:
                    continue
                order_mismatches = _open_order_mismatches(expected_order, actual_order)
                if order_mismatches:
                    mismatches.append(f"{cloid} open order mismatch: {', '.join(order_mismatches)}")
        if not mismatches:
            return ShieldResult(True, "matched", SafeModeReason.NONE, "")
        detail = "; ".join(mismatches)
        self.safe_mode.trip(SafeModeReason.MANUAL_INTERVENTION, detail)
        return ShieldResult(False, "paused", SafeModeReason.MANUAL_INTERVENTION, detail)

    def exchange_error(self, message: str) -> ShieldResult:
        lowered = message.lower()
        rate_limited = (
            "rate limit" in lowered
            or "too many request" in lowered
            or re.search(r"(?<![0-9a-f])429(?![0-9a-f])", lowered) is not None
        )
        mapping = [
            (rate_limited, SafeModeReason.RATE_LIMIT),
            (
                any(
                    needle in lowered
                    for needle in ("tick", "precision", "divisible", "invalid size")
                ),
                SafeModeReason.PRECISION_ERROR,
            ),
            (
                any(needle in lowered for needle in ("margin", "insufficient", "max position")),
                SafeModeReason.MARGIN_ERROR,
            ),
            ("could not immediately match" in lowered, SafeModeReason.RISK_LIMIT),
            (
                any(needle in lowered for needle in ("nonce", "clock", "timestamp")),
                SafeModeReason.CLOCK_SKEW,
            ),
        ]
        for matches, reason in mapping:
            if matches:
                self.safe_mode.trip(reason, message)
                return ShieldResult(False, "paused", reason, message)
        self.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, message)
        return ShieldResult(False, "paused", SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, message)


def _open_order_mismatches(expected: OpenOrder, actual: OpenOrder) -> list[str]:
    mismatches: list[str] = []
    if canonical_market_symbol(actual.coin) != canonical_market_symbol(expected.coin):
        mismatches.append(
            "coin expected "
            f"{canonical_market_symbol(expected.coin)} actual {canonical_market_symbol(actual.coin)}"
        )
    if actual.side.lower() != expected.side.lower():
        mismatches.append(f"side expected {expected.side.lower()} actual {actual.side.lower()}")
    if actual.size != expected.size:
        mismatches.append(f"size expected {expected.size} actual {actual.size}")
    if actual.price != expected.price:
        mismatches.append(f"price expected {expected.price} actual {actual.price}")
    if actual.reduce_only != expected.reduce_only:
        mismatches.append(
            f"reduce_only expected {expected.reduce_only} actual {actual.reduce_only}"
        )
    return mismatches
