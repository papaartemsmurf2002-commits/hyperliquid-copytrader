from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from .precision import quantize_price, quantize_size


HYPERLIQUID_PERP_MIN_NOTIONAL_USD = Decimal("10")


class OrderEligibility(str, Enum):
    PLACEABLE = "placeable"
    PLACEABLE_AFTER_ROUNDING = "placeable_after_rounding"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class HyperliquidPerpRules:
    """Venue-local rules needed by the continuous IOC path.

    Hyperliquid currently publishes a USD 10 minimum for ordinary perp orders.
    Keeping the value on this adapter rule (rather than in the portfolio engine)
    makes the scope explicit and leaves room for market-specific venue metadata.
    """

    market: str
    sz_decimals: int
    max_leverage: int
    minimum_notional_usd: Decimal = HYPERLIQUID_PERP_MIN_NOTIONAL_USD
    contract_size: Decimal = Decimal("1")
    market_type: str = "perp"
    margin_mode: str = "unknown"

    @property
    def quantity_step(self) -> Decimal:
        return Decimal(1).scaleb(-self.sz_decimals)


@dataclass(frozen=True, slots=True)
class OrderPreflightResult:
    eligibility: OrderEligibility
    market: str
    market_type: str
    side: str
    requested_quantity: Decimal
    requested_notional_usd: Decimal
    requested_price: Decimal
    rounded_quantity: Decimal
    rounded_price: Decimal
    submitted_notional_usd: Decimal
    quantity_step: Decimal
    minimum_notional_usd: Decimal
    required_margin_usd: Decimal | None
    available_collateral_usd: Decimal | None
    leverage: int | None
    margin_mode: str
    reduce_only: bool
    exact_reduce_only_close: bool
    reason: str
    validation_method: str = "metadata_with_optional_account_state"

    @property
    def placeable(self) -> bool:
        return self.eligibility is not OrderEligibility.REJECTED


def preflight_hyperliquid_perp_order(
    *,
    rules: HyperliquidPerpRules,
    requested_quantity: Decimal,
    price: Decimal,
    side: str,
    max_order_notional_usd: Decimal,
    reduce_only: bool,
    current_position_size: Decimal = Decimal("0"),
    leverage: int | None = None,
    available_collateral_usd: Decimal | None = None,
) -> OrderPreflightResult:
    """Validate a single perp order without signing or contacting the venue.

    The result deliberately keeps order notional, rounded quantity, required
    margin and account collateral separate. Exact venue admission remains
    authoritative when account leverage or available collateral is unavailable.
    """

    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            Decimal("0"),
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            "side must be buy or sell",
        )
    values = (requested_quantity, price, max_order_notional_usd, current_position_size)
    if any(not value.is_finite() for value in values):
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            Decimal("0"),
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            "order inputs must be finite",
        )
    if requested_quantity <= 0 or price <= 0 or max_order_notional_usd <= 0:
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            Decimal("0"),
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            "quantity, price and order-notional cap must be positive",
        )
    try:
        rounded_price = quantize_price(
            price,
            rules.sz_decimals,
            is_spot=rules.market_type == "spot",
        )
    except ValueError as exc:
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            Decimal("0"),
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            f"price cannot be represented by venue rules: {exc}",
        )
    rounded = abs(quantize_size(requested_quantity, rules.sz_decimals))
    if rounded <= 0:
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            rounded,
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            f"requested quantity is below one venue lot ({rules.quantity_step})",
        )

    submitted_notional = rounded * rules.contract_size * rounded_price
    exact_close = _is_exact_reduce_only_close(
        side=normalized_side,
        reduce_only=reduce_only,
        rounded_quantity=rounded,
        current_position_size=current_position_size,
    )
    if submitted_notional > max_order_notional_usd:
        reason = (
            f"submitted notional {submitted_notional} USD exceeds local order cap "
            f"{max_order_notional_usd} USD"
        )
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            rounded,
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            reason,
        )
    if submitted_notional < rules.minimum_notional_usd and not exact_close:
        action_class = "partial reduce-only" if reduce_only else "risk-increasing"
        reason = (
            f"{action_class} submitted notional {submitted_notional} USD is below "
            f"the {rules.market_type} minimum {rules.minimum_notional_usd} USD"
        )
        return _result(
            rules,
            normalized_side,
            OrderEligibility.REJECTED,
            requested_quantity,
            price,
            rounded,
            max_order_notional_usd,
            reduce_only,
            current_position_size,
            leverage,
            available_collateral_usd,
            reason,
        )

    required_margin: Decimal | None = None
    if not reduce_only and leverage is not None:
        if isinstance(leverage, bool) or leverage < 1 or leverage > rules.max_leverage:
            return _result(
                rules,
                normalized_side,
                OrderEligibility.REJECTED,
                requested_quantity,
                price,
                rounded,
                max_order_notional_usd,
                reduce_only,
                current_position_size,
                leverage,
                available_collateral_usd,
                f"leverage {leverage} is outside venue range 1..{rules.max_leverage}",
            )
        required_margin = submitted_notional / Decimal(leverage)
        if (
            available_collateral_usd is not None
            and available_collateral_usd.is_finite()
            and required_margin > available_collateral_usd
        ):
            return _result(
                rules,
                normalized_side,
                OrderEligibility.REJECTED,
                requested_quantity,
                price,
                rounded,
                max_order_notional_usd,
                reduce_only,
                current_position_size,
                leverage,
                available_collateral_usd,
                f"estimated required margin {required_margin} USD exceeds available "
                f"collateral {available_collateral_usd} USD",
            )

    status = (
        OrderEligibility.PLACEABLE_AFTER_ROUNDING
        if rounded != requested_quantity or rounded_price != price
        else OrderEligibility.PLACEABLE
    )
    if exact_close and submitted_notional < rules.minimum_notional_usd:
        reason = "exact full reduce-only close is placeable under the proven runtime policy"
    elif required_margin is None and not reduce_only:
        reason = (
            "venue quantity and notional rules pass; exact margin admission remains "
            "exchange-authoritative because current leverage is unavailable"
        )
    elif rounded != requested_quantity or rounded_price != price:
        changes: list[str] = []
        if rounded != requested_quantity:
            changes.append(f"quantity toward zero to {rounded}")
        if rounded_price != price:
            changes.append(f"price to venue precision {rounded_price}")
        reason = "placeable after rounding " + " and ".join(changes)
        if not reduce_only and available_collateral_usd is None:
            reason += (
                f"; estimated initial margin is {required_margin} USD at observed "
                f"{leverage}x leverage, but available perp collateral is not "
                "authoritative locally"
            )
    elif not reduce_only and available_collateral_usd is None:
        reason = (
            f"venue quantity and notional rules pass; estimated initial margin is "
            f"{required_margin} USD at observed {leverage}x leverage, but available "
            "perp collateral is not authoritative locally"
        )
    else:
        reason = "venue quantity, notional and available-margin checks pass"
    result = _result(
        rules,
        normalized_side,
        status,
        requested_quantity,
        price,
        rounded,
        max_order_notional_usd,
        reduce_only,
        current_position_size,
        leverage,
        available_collateral_usd,
        reason,
    )
    if required_margin is not None and result.required_margin_usd != required_margin:
        raise AssertionError("preflight margin calculation diverged")
    return result


def _result(
    rules: HyperliquidPerpRules,
    side: str,
    eligibility: OrderEligibility,
    requested_quantity: Decimal,
    price: Decimal,
    rounded_quantity: Decimal,
    max_order_notional_usd: Decimal,
    reduce_only: bool,
    current_position_size: Decimal,
    leverage: int | None,
    available_collateral_usd: Decimal | None,
    reason: str,
) -> OrderPreflightResult:
    del max_order_notional_usd
    rounded_price = Decimal("NaN")
    if price.is_finite() and price > 0:
        try:
            rounded_price = quantize_price(
                price,
                rules.sz_decimals,
                is_spot=rules.market_type == "spot",
            )
        except ValueError:
            pass
    requested_notional = (
        abs(requested_quantity) * rules.contract_size * price
        if requested_quantity.is_finite() and price.is_finite()
        else Decimal("NaN")
    )
    submitted_notional = (
        abs(rounded_quantity) * rules.contract_size * rounded_price
        if rounded_quantity.is_finite() and rounded_price.is_finite()
        else Decimal("NaN")
    )
    exact_close = _is_exact_reduce_only_close(
        side=side,
        reduce_only=reduce_only,
        rounded_quantity=rounded_quantity,
        current_position_size=current_position_size,
    )
    required_margin = (
        submitted_notional / Decimal(leverage)
        if not reduce_only
        and leverage is not None
        and not isinstance(leverage, bool)
        and leverage > 0
        and submitted_notional.is_finite()
        else Decimal("0")
        if reduce_only
        else None
    )
    return OrderPreflightResult(
        eligibility=eligibility,
        market=rules.market,
        market_type=rules.market_type,
        side=side,
        requested_quantity=requested_quantity,
        requested_notional_usd=requested_notional,
        requested_price=price,
        rounded_quantity=rounded_quantity,
        rounded_price=rounded_price,
        submitted_notional_usd=submitted_notional,
        quantity_step=rules.quantity_step,
        minimum_notional_usd=rules.minimum_notional_usd,
        required_margin_usd=required_margin,
        available_collateral_usd=available_collateral_usd,
        leverage=leverage,
        margin_mode=rules.margin_mode,
        reduce_only=reduce_only,
        exact_reduce_only_close=exact_close,
        reason=reason,
    )


def _is_exact_reduce_only_close(
    *,
    side: str,
    reduce_only: bool,
    rounded_quantity: Decimal,
    current_position_size: Decimal,
) -> bool:
    if (
        not reduce_only
        or side not in {"buy", "sell"}
        or not rounded_quantity.is_finite()
        or not current_position_size.is_finite()
        or rounded_quantity <= 0
        or current_position_size == 0
    ):
        return False
    signed_quantity = rounded_quantity if side == "buy" else -rounded_quantity
    return current_position_size + signed_quantity == 0


__all__ = [
    "HYPERLIQUID_PERP_MIN_NOTIONAL_USD",
    "HyperliquidPerpRules",
    "OrderEligibility",
    "OrderPreflightResult",
    "preflight_hyperliquid_perp_order",
]
