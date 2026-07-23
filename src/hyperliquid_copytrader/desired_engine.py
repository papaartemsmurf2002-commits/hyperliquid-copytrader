from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Mapping

from .continuous_config import ContinuousSlotConfig
from .markets import canonical_market_symbol
from .models import Position
from .order_preflight import (
    HyperliquidPerpRules,
    preflight_hyperliquid_perp_order,
)
from .precision import quantize_size


@dataclass(frozen=True, slots=True)
class DesiredPortfolio:
    desired_id: str
    positions: dict[str, Position]
    source_equity: Decimal
    follower_equity: Decimal
    scale: Decimal
    gross_scale: Decimal
    gross_notional_usd: Decimal


@dataclass(frozen=True, slots=True)
class NextAction:
    desired_id: str
    market: str
    side: str
    size: Decimal
    reduce_only: bool
    reason: str

    @property
    def signed_size(self) -> Decimal:
        return self.size if self.side == "buy" else -self.size


@dataclass(frozen=True, slots=True)
class ActionDecision:
    action: NextAction | None
    residual: Decimal
    blocker: str | None = None
    skipped_blockers: tuple[str, ...] = ()


def build_desired_portfolio(
    slot: ContinuousSlotConfig,
    *,
    source_positions: Mapping[str, Position],
    source_equity: Decimal,
    follower_equity: Decimal,
    mids: Mapping[str, Decimal],
) -> DesiredPortfolio:
    if not source_equity.is_finite() or source_equity <= 0:
        raise ValueError("source equity must be finite and positive")
    if not follower_equity.is_finite() or follower_equity <= 0:
        raise ValueError("follower equity must be finite and positive")
    allowed = set(slot.allowed_markets)
    scale = follower_equity / source_equity * slot.multiplier
    candidates: list[tuple[str, Position, Decimal]] = []
    for raw_market, source in source_positions.items():
        market = canonical_market_symbol(raw_market)
        if allowed and market not in allowed:
            continue
        mid = mids.get(market)
        if mid is None or not mid.is_finite() or mid <= 0:
            continue
        if not source.size.is_finite():
            raise ValueError(f"source position {market} is not finite")
        size = source.size * scale
        if size == 0:
            continue
        leverage = source.leverage
        if leverage is not None:
            leverage = min(leverage, slot.max_leverage)
        target = Position(
            coin=market,
            size=size,
            entry_px=source.entry_px,
            leverage=leverage,
            updated_ms=source.updated_ms,
        )
        candidates.append((market, target, abs(size) * mid))

    candidates.sort(key=lambda row: (-row[2], row[0]))
    selected = candidates[: slot.max_open_positions]
    gross = sum((row[2] for row in selected), Decimal("0"))
    gross_scale = (
        slot.max_gross_exposure_usd / gross if gross > slot.max_gross_exposure_usd else Decimal("1")
    )
    positions = {
        market: Position(
            coin=market,
            size=position.size * gross_scale,
            entry_px=position.entry_px,
            leverage=position.leverage,
            updated_ms=position.updated_ms,
        )
        for market, position, _notional in selected
    }
    final_gross = sum(
        (abs(position.size) * mids[market] for market, position in positions.items()), Decimal("0")
    )
    identity = {
        "slot": slot.slot,
        "scale": str(scale),
        "positions": {market: str(position.size) for market, position in sorted(positions.items())},
    }
    desired_id = sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return DesiredPortfolio(
        desired_id=desired_id,
        positions=positions,
        source_equity=source_equity,
        follower_equity=follower_equity,
        scale=scale,
        gross_scale=gross_scale,
        gross_notional_usd=final_gross,
    )


def choose_next_action(
    slot: ContinuousSlotConfig,
    desired: DesiredPortfolio,
    *,
    follower_positions: Mapping[str, Position],
    unresolved_signed_remaining: Mapping[str, Decimal],
    mids: Mapping[str, Decimal],
    size_decimals: Mapping[str, int],
    market_rules: Mapping[str, HyperliquidPerpRules] | None = None,
) -> ActionDecision:
    """Return one current action, never a stale close-and-reopen pair."""

    markets = sorted(set(desired.positions) | set(follower_positions))

    def needs_reduction(market: str) -> bool:
        desired_size = desired.positions.get(market, Position(market, Decimal("0"))).size
        confirmed = follower_positions.get(market, Position(market, Decimal("0"))).size
        if confirmed == 0:
            return False
        return (
            desired_size == 0
            or (confirmed > 0) != (desired_size > 0)
            or abs(desired_size) < abs(confirmed)
        )

    # One-action convergence must not open or increase a market while another
    # confirmed position still needs to shrink. Lexical ordering would create a
    # transient extra position and can breach gross/position limits on rotation.
    markets.sort(key=lambda market: (not needs_reduction(market), market))
    skipped: list[str] = []
    first_blocked_residual = Decimal("0")

    def defer(message: str, residual: Decimal) -> None:
        nonlocal first_blocked_residual
        if not skipped:
            first_blocked_residual = residual
        skipped.append(message)

    for market in markets:
        desired_size = desired.positions.get(market, Position(market, Decimal("0"))).size
        confirmed = follower_positions.get(market, Position(market, Decimal("0"))).size
        unresolved = unresolved_signed_remaining.get(market, Decimal("0"))
        projected = confirmed + unresolved
        residual = desired_size - projected
        if residual == 0:
            continue
        if unresolved != 0:
            return ActionDecision(
                action=None,
                residual=residual,
                blocker=f"{market} has an unresolved attempted action",
            )
        mid = mids.get(market)
        decimals = size_decimals.get(market)
        if mid is None or not mid.is_finite() or mid <= 0 or decimals is None:
            return ActionDecision(
                action=None,
                residual=residual,
                blocker=f"{market} lacks fresh market metadata",
            )

        reversing = confirmed != 0 and desired_size != 0 and (confirmed > 0) != (desired_size > 0)
        raw_action_size = -confirmed if reversing else desired_size - confirmed
        reduce_only = reversing or (
            confirmed != 0
            and raw_action_size != 0
            and (raw_action_size > 0) != (confirmed > 0)
            and abs(desired_size) < abs(confirmed)
        )
        max_size = slot.max_order_notional_usd / mid
        if abs(raw_action_size) > max_size:
            raw_action_size = max_size.copy_sign(raw_action_size)
        signed_size = quantize_size(raw_action_size, decimals)
        if signed_size == 0:
            defer(f"{market} residual is below one venue lot", residual)
            continue
        reason = (
            "close reversal and replan"
            if reversing
            else "move confirmed follower position toward current desired target"
        )
        rules = (
            market_rules.get(market)
            if market_rules is not None
            else HyperliquidPerpRules(
                market=market,
                sz_decimals=decimals,
                max_leverage=slot.max_leverage,
            )
        )
        if rules is None:
            return ActionDecision(
                action=None,
                residual=residual,
                blocker=f"{market} lacks venue order rules",
            )
        follower_leverage = follower_positions.get(market, Position(market, Decimal("0"))).leverage
        preflight = preflight_hyperliquid_perp_order(
            rules=rules,
            requested_quantity=abs(signed_size),
            price=mid,
            side="buy" if signed_size > 0 else "sell",
            max_order_notional_usd=slot.max_order_notional_usd,
            reduce_only=reduce_only,
            current_position_size=confirmed,
            leverage=follower_leverage,
        )
        if not preflight.placeable:
            if not reduce_only:
                defer(
                    f"{market} residual is sub-minimum debt "
                    f"({preflight.submitted_notional_usd} USD): {preflight.reason}",
                    residual,
                )
                continue
            full_chunk = quantize_size(
                min(abs(confirmed), max_size).copy_sign(-confirmed), decimals
            )
            after_chunk = confirmed + full_chunk
            improves_target = abs(desired_size - after_chunk) < abs(desired_size - confirmed)
            close_preflight = preflight_hyperliquid_perp_order(
                rules=rules,
                requested_quantity=abs(full_chunk),
                price=mid,
                side="buy" if full_chunk > 0 else "sell",
                max_order_notional_usd=slot.max_order_notional_usd,
                reduce_only=True,
                current_position_size=confirmed,
                leverage=follower_leverage,
            )
            if not improves_target:
                defer(
                    f"{market} reduction is below the venue minimum and a full close "
                    "would move farther from the target",
                    residual,
                )
                continue
            if not close_preflight.placeable:
                defer(close_preflight.reason, residual)
                continue
            signed_size = full_chunk
            reason = "move to the nearest executable lower-risk position"
        return ActionDecision(
            action=NextAction(
                desired_id=desired.desired_id,
                market=market,
                side="buy" if signed_size > 0 else "sell",
                size=abs(signed_size),
                reduce_only=reduce_only,
                reason=reason,
            ),
            residual=residual,
            skipped_blockers=tuple(skipped),
        )
    if skipped:
        return ActionDecision(
            action=None,
            residual=first_blocked_residual,
            blocker=skipped[0],
            skipped_blockers=tuple(skipped),
        )
    return ActionDecision(action=None, residual=Decimal("0"))
