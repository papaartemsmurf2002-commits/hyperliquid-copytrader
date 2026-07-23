from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .config import OpsConfig, RiskConfig
from .copy_engine import AssetMeta
from .markets import canonical_market_symbol, market_dex
from .models import (
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    Position,
    SafeModeReason,
    now_ms,
    parse_decimal,
)
from .order_preflight import HYPERLIQUID_PERP_MIN_NOTIONAL_USD
from .persistence import SQLiteStore
from .precision import quantize_price


@dataclass(frozen=True)
class GuardDecision:
    ok: bool
    reason: SafeModeReason
    detail: str
    terminal_skip: bool = False


def increases_exposure(intent: FollowerIntent) -> bool:
    """Return whether an intent creates or adds follower market exposure."""

    return (
        intent.action == IntentAction.OPEN
        and not intent.reduce_only
        and intent.size.is_finite()
        and intent.size > 0
    )


def pending_exposure_increasing_count(
    store: SQLiteStore,
    mode: Mode | None = None,
) -> int:
    """Count unresolved OPEN/add attempts, conservatively handling corrupt rows."""

    count = 0
    for row in store.pending_intents(mode):
        if str(row.get("action") or "") != IntentAction.OPEN.value:
            continue
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
            if not isinstance(payload, dict):
                count += 1
                continue
            size = parse_decimal(payload.get("size"))
            if not bool(payload.get("reduce_only")) and size.is_finite() and size > 0:
                count += 1
        except (json.JSONDecodeError, TypeError, ValueError):
            # An unresolved OPEN row with unreadable semantics must consume capacity.
            count += 1
    return count


class ExecutionGuard:
    """Final local risk checks that must pass before any non-shadow execution."""

    def __init__(
        self,
        *,
        risk: RiskConfig,
        ops: OpsConfig,
        store: SQLiteStore,
        asset_meta: dict[str, AssetMeta],
        mids: dict[str, Decimal],
        mode: Mode | None = None,
    ):
        self.risk = risk
        self.ops = ops
        self.store = store
        self.asset_meta = asset_meta
        self.mids = mids
        self.mode = mode
        self.allowlist = {canonical_market_symbol(symbol) for symbol in risk.allowed_symbols}

    def check_cycle(self, intents: list[FollowerIntent]) -> GuardDecision:
        kill = self.kill_switch_path()
        if kill.exists():
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.OPERATOR_KILL_SWITCH,
                detail=f"kill switch file exists: {kill}",
            )
        if any(not intent.size.is_finite() for intent in intents):
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.CONFIG_INVALID,
                detail="intent sizes must be finite",
            )
        exposure_increasing_intents = [intent for intent in intents if increases_exposure(intent)]
        if len(exposure_increasing_intents) > self.ops.max_new_intents_per_cycle:
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.RISK_LIMIT,
                detail=(
                    f"{len(exposure_increasing_intents)} exposure-increasing intents exceeds "
                    f"HLCT_MAX_NEW_INTENTS_PER_CYCLE={self.ops.max_new_intents_per_cycle}"
                ),
            )
        pending_count = pending_exposure_increasing_count(self.store, self.mode)
        additional_pending_count = 0
        for intent in exposure_increasing_intents:
            existing = self.store.intent_by_cloid(intent.cloid)
            if existing is None or existing.get("intent_id") != intent.intent_id:
                additional_pending_count += 1
        projected_pending_count = pending_count + additional_pending_count
        if projected_pending_count > self.ops.max_open_intents:
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.RESTART_MID_FILL,
                detail=(
                    f"{pending_count} pending exposure-increasing intents plus "
                    f"{additional_pending_count} new exposure-increasing intents exceeds "
                    f"HLCT_MAX_OPEN_INTENTS={self.ops.max_open_intents}"
                ),
            )
        return GuardDecision(True, SafeModeReason.NONE, "")

    def check_intent(
        self,
        intent: FollowerIntent,
        *,
        projected_positions: dict[str, Position],
    ) -> GuardDecision:
        if intent.action == IntentAction.NOOP:
            if intent.status == IntentStatus.SKIPPED:
                reason = intent.reason.lower()
                if "metadata" in reason:
                    return GuardDecision(
                        False,
                        SafeModeReason.UNSUPPORTED_SYMBOL,
                        f"{canonical_market_symbol(intent.coin)} metadata missing for skipped intent",
                    )
                if "mid" in reason or "price" in reason:
                    return GuardDecision(
                        False,
                        SafeModeReason.STALE_SOURCE,
                        f"{canonical_market_symbol(intent.coin)} mid price missing for skipped intent",
                    )
            return GuardDecision(True, SafeModeReason.NONE, "")

        if self.store.has_dispatch_evidence_for_cloid(intent.cloid):
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.DUPLICATE_INTENT,
                detail=f"{intent.cloid} already has execution evidence",
                terminal_skip=True,
            )

        existing = self.store.intent_by_cloid(intent.cloid)
        if existing is not None and existing["intent_id"] != intent.intent_id:
            return GuardDecision(
                ok=False,
                reason=SafeModeReason.DUPLICATE_INTENT,
                detail=f"{intent.cloid} already belongs to {existing['intent_id']}",
            )

        try:
            coin = canonical_market_symbol(intent.coin)
        except ValueError as exc:
            return GuardDecision(
                False,
                SafeModeReason.CONFIG_INVALID,
                f"invalid intent market {intent.coin!r}: {exc}",
            )
        if coin not in self.allowlist:
            return GuardDecision(False, SafeModeReason.UNSUPPORTED_SYMBOL, f"{coin} is not allowed")
        meta = self.asset_meta.get(coin)
        if meta is None:
            return GuardDecision(
                False, SafeModeReason.UNSUPPORTED_SYMBOL, f"{coin} metadata missing"
            )
        mid = self.mids.get(coin)
        if mid is None or not mid.is_finite() or mid <= 0:
            return GuardDecision(False, SafeModeReason.STALE_SOURCE, f"{coin} mid price missing")
        if intent.side not in {"buy", "sell"}:
            return GuardDecision(
                False, SafeModeReason.CONFIG_INVALID, f"invalid side {intent.side}"
            )
        if not intent.size.is_finite() or intent.size <= 0:
            return GuardDecision(
                False, SafeModeReason.CONFIG_INVALID, "intent size must be positive"
            )
        if intent.price is None or not intent.price.is_finite() or intent.price <= 0:
            return GuardDecision(
                False, SafeModeReason.CONFIG_INVALID, "intent price must be positive"
            )

        current = projected_positions.get(coin, Position(coin=coin, size=Decimal("0")))
        if not current.size.is_finite():
            return GuardDecision(
                False,
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                f"{coin} current projected size is not finite",
            )
        signed_delta = intent.size if intent.side == "buy" else -intent.size
        next_size = current.size + signed_delta
        genuinely_reducing = (
            intent.reduce_only
            and intent.action in {IntentAction.REDUCE, IntentAction.CLOSE}
            and current.size != 0
            and (signed_delta > 0) != (current.size > 0)
            and (next_size == 0 or (next_size > 0) == (current.size > 0))
            and abs(next_size) < abs(current.size)
        )
        if intent.reduce_only or intent.action in {IntentAction.REDUCE, IntentAction.CLOSE}:
            if not genuinely_reducing:
                return GuardDecision(
                    False,
                    SafeModeReason.RISK_LIMIT,
                    (
                        f"{coin} reduce-only intent must strictly reduce exposure without "
                        f"crossing zero: current={current.size} next={next_size}"
                    ),
                )
            if intent.action == IntentAction.CLOSE and next_size != 0:
                return GuardDecision(
                    False,
                    SafeModeReason.RISK_LIMIT,
                    f"{coin} close intent must flatten exposure, projected size is {next_size}",
                )
        elif (
            intent.action == IntentAction.OPEN
            and current.size != 0
            and ((signed_delta > 0) != (current.size > 0))
        ):
            return GuardDecision(
                False,
                SafeModeReason.RAPID_FLIP,
                f"{coin} open intent cannot reduce or cross existing exposure",
            )

        active_projected_coins = {
            canonical_market_symbol(key)
            for key, position in projected_positions.items()
            if position.size != 0
        }
        opens_new_position = (
            intent.action == IntentAction.OPEN
            and not intent.reduce_only
            and not genuinely_reducing
            and next_size != 0
            and coin not in active_projected_coins
        )
        if opens_new_position and len(active_projected_coins) >= self.risk.max_open_positions:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                (
                    f"{coin} would open projected position "
                    f"{len(active_projected_coins) + 1}, exceeding "
                    f"HLCT_MAX_OPEN_POSITIONS={self.risk.max_open_positions}"
                ),
            )

        notional = intent.size * mid
        exact_full_close = genuinely_reducing and next_size == 0
        if notional < HYPERLIQUID_PERP_MIN_NOTIONAL_USD and not exact_full_close:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} notional {notional} below Hyperliquid perp minimum "
                f"{HYPERLIQUID_PERP_MIN_NOTIONAL_USD}",
                terminal_skip=True,
            )
        if notional > self.risk.max_notional_usd and not genuinely_reducing:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} notional {notional} exceeds cap {self.risk.max_notional_usd}",
            )

        if intent.reduce_only or intent.action in {IntentAction.REDUCE, IntentAction.CLOSE}:
            max_buy = quantize_price(
                mid * (Decimal("1") + self.risk.close_slippage_bps / Decimal("10000")),
                meta.sz_decimals,
            )
            min_sell = quantize_price(
                mid * (Decimal("1") - self.risk.close_slippage_bps / Decimal("10000")),
                meta.sz_decimals,
            )
            if intent.side == "buy" and intent.price > max_buy:
                return GuardDecision(
                    False,
                    SafeModeReason.RISK_LIMIT,
                    (
                        f"{coin} reduce-only buy price {intent.price} exceeds close slippage "
                        f"bound {max_buy} (HLCT_CLOSE_SLIPPAGE_BPS={self.risk.close_slippage_bps})"
                    ),
                )
            if intent.side == "sell" and intent.price < min_sell:
                return GuardDecision(
                    False,
                    SafeModeReason.RISK_LIMIT,
                    (
                        f"{coin} reduce-only sell price {intent.price} below close slippage "
                        f"bound {min_sell} (HLCT_CLOSE_SLIPPAGE_BPS={self.risk.close_slippage_bps})"
                    ),
                )
        if not intent.reduce_only and intent.action == IntentAction.OPEN:
            proof_decision = self._hip3_round_trip_proof_decision(intent, coin=coin)
            if proof_decision is not None:
                return proof_decision
            hip3_proof_controls_price = self.mode in {Mode.TESTNET, Mode.LIVE} and market_dex(coin)
            if not hip3_proof_controls_price:
                max_buy = quantize_price(
                    mid * (Decimal("1") + self.risk.slippage_bps / Decimal("10000")),
                    meta.sz_decimals,
                )
                min_sell = quantize_price(
                    mid * (Decimal("1") - self.risk.slippage_bps / Decimal("10000")),
                    meta.sz_decimals,
                )
                if intent.side == "buy" and intent.price > max_buy:
                    return GuardDecision(
                        False,
                        SafeModeReason.RISK_LIMIT,
                        f"{coin} buy price {intent.price} exceeds slippage bound {max_buy}",
                    )
                if intent.side == "sell" and intent.price < min_sell:
                    return GuardDecision(
                        False,
                        SafeModeReason.RISK_LIMIT,
                        f"{coin} sell price {intent.price} below slippage bound {min_sell}",
                    )
        projected_notional = abs(next_size) * mid
        current_notional = abs(current.size) * mid
        if projected_notional > self.risk.max_notional_usd and not (
            genuinely_reducing and projected_notional < current_notional
        ):
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} projected notional {projected_notional} exceeds cap {self.risk.max_notional_usd}",
            )
        gross_decision = self._projected_gross_decision(
            coin=coin,
            next_size=next_size,
            projected_positions=projected_positions,
            allow_above_cap_reduction=genuinely_reducing,
        )
        if gross_decision is not None:
            return gross_decision
        return GuardDecision(True, SafeModeReason.NONE, "")

    def _hip3_round_trip_proof_decision(
        self, intent: FollowerIntent, *, coin: str
    ) -> GuardDecision | None:
        if self.mode not in {Mode.TESTNET, Mode.LIVE} or not market_dex(coin):
            return None
        proof = intent.execution_proof
        if not isinstance(proof, dict) or proof.get("kind") != "hip3_round_trip":
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} exchange-mode opening requires a fresh HIP-3 round-trip depth proof",
            )
        try:
            proof_coin = canonical_market_symbol(str(proof.get("coin") or ""))
            proof_side = str(proof.get("opening_side") or "")
            proof_size = parse_decimal(proof.get("requested_size"))
            oracle_px = parse_decimal(proof.get("oracle_px"))
            entry_limit = parse_decimal(proof.get("entry_limit"))
            exit_limit = parse_decimal(proof.get("exit_limit"))
            entry_visible = parse_decimal(proof.get("entry_visible_size"))
            exit_visible = parse_decimal(proof.get("exit_visible_size"))
            entry_best = parse_decimal(proof.get("entry_best_px"))
            entry_worst = parse_decimal(proof.get("entry_worst_px"))
            exit_worst = parse_decimal(proof.get("exit_worst_px"))
            entry_notional_bound = parse_decimal(proof.get("entry_notional_bound_px"))
            envelope_bps = parse_decimal(proof.get("oracle_envelope_bps"))
            observed_ms = int(proof.get("observed_ms") or 0)
            book_time_ms = int(proof.get("book_time_ms") or 0)
        except (TypeError, ValueError):
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 round-trip proof is malformed",
            )
        decimals = (
            proof_size,
            oracle_px,
            entry_limit,
            exit_limit,
            entry_visible,
            exit_visible,
            entry_best,
            entry_worst,
            exit_worst,
            entry_notional_bound,
            envelope_bps,
        )
        if any(value is None or not value.is_finite() or value <= 0 for value in decimals):
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 round-trip proof has invalid numeric fields",
            )
        assert all(value is not None for value in decimals)
        if proof_coin != coin or proof_side != intent.side or proof_size != intent.size:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 round-trip proof does not match the opening intent",
            )
        if entry_limit != intent.price or envelope_bps != self.risk.hip3_oracle_envelope_bps:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 round-trip proof does not match the configured price envelope",
            )
        if entry_visible < intent.size or exit_visible < intent.size:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 round-trip proof lacks full entry and exit depth",
            )
        current_ms = now_ms()
        book_age_ms = current_ms - book_time_ms
        observed_age_ms = current_ms - observed_ms
        if (
            observed_ms <= 0
            or book_time_ms <= 0
            or book_time_ms > observed_ms + 1_000
            or observed_age_ms < -1_000
            or book_age_ms < -1_000
            or observed_age_ms > self.risk.stale_source_ms
            or book_age_ms > self.risk.stale_source_ms
        ):
            return GuardDecision(
                False,
                SafeModeReason.STALE_SOURCE,
                (
                    f"{coin} HIP-3 round-trip proof is stale or time-inconsistent "
                    f"(observed_age={observed_age_ms}ms book_age={book_age_ms}ms)"
                ),
            )
        distance = oracle_px * envelope_bps / Decimal("10000")
        lower = oracle_px - distance
        upper = oracle_px + distance
        expected_entry_notional_bound = max(upper, entry_best, entry_worst)
        entry_prices_ordered = (
            entry_best <= entry_worst if intent.side == "buy" else entry_best >= entry_worst
        )
        if not entry_prices_ordered or entry_notional_bound != expected_entry_notional_bound:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 entry notional proof is inconsistent with visible depth",
            )
        bounded_notional = proof_size * entry_notional_bound
        if bounded_notional > self.risk.max_notional_usd:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                (
                    f"{coin} HIP-3 bounded opening notional {bounded_notional} exceeds cap "
                    f"{self.risk.max_notional_usd}"
                ),
            )
        if not lower <= entry_limit <= upper or not lower <= entry_worst <= upper:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 entry price is outside the application oracle envelope",
            )
        if not lower <= exit_limit <= upper or not lower <= exit_worst <= upper:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 exit price is outside the application oracle envelope",
            )
        entry_crosses = (
            entry_limit >= entry_worst if intent.side == "buy" else entry_limit <= entry_worst
        )
        exit_crosses = (
            exit_limit <= exit_worst if intent.side == "buy" else exit_limit >= exit_worst
        )
        if not entry_crosses or not exit_crosses:
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"{coin} HIP-3 proof limits do not cross the proven entry and exit depth",
            )
        return None

    def apply_projection(
        self, intent: FollowerIntent, projected_positions: dict[str, Position]
    ) -> None:
        if intent.action == IntentAction.NOOP or intent.size <= 0:
            return
        current = projected_positions.get(
            intent.coin, Position(coin=intent.coin, size=Decimal("0"))
        )
        signed_delta = intent.size if intent.side == "buy" else -intent.size
        next_size = current.size + signed_delta
        if next_size == 0:
            projected_positions.pop(intent.coin, None)
            return
        projected_positions[intent.coin] = Position(
            coin=intent.coin,
            size=next_size,
            entry_px=intent.price or current.entry_px,
            leverage=current.leverage,
        )

    def kill_switch_path(self) -> Path:
        path = self.ops.kill_switch_path
        if path.is_absolute():
            return path
        return (
            self.store.path.parent / path.name if str(path.parent) == "data" else Path.cwd() / path
        )

    def _projected_gross_decision(
        self,
        *,
        coin: str,
        next_size: Decimal,
        projected_positions: dict[str, Position],
        allow_above_cap_reduction: bool = False,
    ) -> GuardDecision | None:
        sizes: dict[str, Decimal] = {}
        for key, position in projected_positions.items():
            try:
                symbol = canonical_market_symbol(position.coin or key)
            except ValueError as exc:
                return GuardDecision(
                    False,
                    SafeModeReason.CONFIG_INVALID,
                    f"invalid projected market {(position.coin or key)!r}: {exc}",
                )
            if not position.size.is_finite():
                return GuardDecision(
                    False,
                    SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                    f"{symbol} current projected size is not finite",
                )
            sizes[symbol] = sizes.get(symbol, Decimal("0")) + position.size
        if next_size == 0:
            sizes.pop(coin, None)
        else:
            sizes[coin] = next_size

        current_gross = Decimal("0")
        gross = Decimal("0")
        for symbol, size in sizes.items():
            if size == 0:
                continue
            if symbol not in self.allowlist:
                return GuardDecision(
                    False,
                    SafeModeReason.UNSUPPORTED_SYMBOL,
                    f"{symbol} existing projected exposure is not allowed",
                )
            mid = self.mids.get(symbol)
            if mid is None or not mid.is_finite() or mid <= 0:
                return GuardDecision(
                    False,
                    SafeModeReason.STALE_SOURCE,
                    f"{symbol} mid price missing for gross exposure check",
                )
            gross += abs(size) * mid

        if allow_above_cap_reduction:
            for key, position in projected_positions.items():
                try:
                    symbol = canonical_market_symbol(position.coin or key)
                except ValueError as exc:
                    return GuardDecision(
                        False,
                        SafeModeReason.CONFIG_INVALID,
                        f"invalid projected market {(position.coin or key)!r}: {exc}",
                    )
                if position.size == 0:
                    continue
                mid = self.mids.get(symbol)
                if mid is None or not mid.is_finite() or mid <= 0:
                    return GuardDecision(
                        False,
                        SafeModeReason.STALE_SOURCE,
                        f"{symbol} mid price missing for current gross exposure check",
                    )
                current_gross += abs(position.size) * mid

        if gross > self.risk.max_gross_exposure_usd and not (
            allow_above_cap_reduction and gross < current_gross
        ):
            return GuardDecision(
                False,
                SafeModeReason.RISK_LIMIT,
                f"projected gross exposure {gross} exceeds cap {self.risk.max_gross_exposure_usd}",
            )
        return None
