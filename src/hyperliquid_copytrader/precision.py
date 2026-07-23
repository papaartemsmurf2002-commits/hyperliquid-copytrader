from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, localcontext

from .models import decimal_to_wire


PERP_MAX_DECIMALS = 6
SPOT_MAX_DECIMALS = 8


def quantize_size(size: Decimal, sz_decimals: int) -> Decimal:
    """Round size toward zero to the asset size precision."""

    if not size.is_finite():
        raise ValueError("size must be finite")
    step = Decimal(1).scaleb(-sz_decimals)
    rounding = ROUND_FLOOR if size >= 0 else ROUND_CEILING
    return size.quantize(step, rounding=rounding)


def _five_sig(value: Decimal) -> Decimal:
    if value == 0:
        return Decimal("0")
    with localcontext() as ctx:
        ctx.prec = 50
        adjusted = value.adjusted()
        decimals = max(0, 4 - adjusted)
        quantum = Decimal(1).scaleb(-decimals)
        return value.quantize(quantum, rounding=ROUND_HALF_UP)


def quantize_price(price: Decimal, sz_decimals: int, is_spot: bool = False) -> Decimal:
    """Apply Hyperliquid 5-significant-figure and decimal-cap price rules."""

    if not price.is_finite() or price <= 0:
        raise ValueError("price must be positive")
    max_decimals = (SPOT_MAX_DECIMALS if is_spot else PERP_MAX_DECIMALS) - sz_decimals
    if max_decimals < 0:
        raise ValueError("sz_decimals exceeds exchange price decimal budget")
    rounded = _five_sig(price)
    if rounded == rounded.to_integral():
        return rounded
    step = Decimal(1).scaleb(-max_decimals)
    return rounded.quantize(step, rounding=ROUND_HALF_UP)


def _quantize_price_inside_limit(price: Decimal, *, sz_decimals: int, is_buy: bool) -> Decimal:
    """Round an IOC limit toward the permitted side of its raw envelope."""

    if not price.is_finite() or price <= 0:
        raise ValueError("price must be positive")
    max_decimals = PERP_MAX_DECIMALS - sz_decimals
    if max_decimals < 0:
        raise ValueError("sz_decimals exceeds exchange price decimal budget")
    sig_decimals = max(0, 4 - price.adjusted())
    quantum = max(Decimal(1).scaleb(-sig_decimals), Decimal(1).scaleb(-max_decimals))
    rounded = price.quantize(quantum, rounding=ROUND_FLOOR if is_buy else ROUND_CEILING)
    if rounded <= 0:
        raise ValueError("no positive venue price exists inside the IOC limit")
    return rounded


def aggressive_ioc_price(
    mid: Decimal, is_buy: bool, slippage_bps: Decimal, sz_decimals: int
) -> Decimal:
    if not mid.is_finite() or mid <= 0 or not slippage_bps.is_finite():
        raise ValueError("mid must be positive and slippage_bps must be finite")
    if slippage_bps < 0 or slippage_bps >= Decimal("10000"):
        raise ValueError("slippage_bps must be between 0 and 10000")
    multiplier = Decimal("1") + (slippage_bps / Decimal("10000"))
    if not is_buy:
        multiplier = Decimal("1") - (slippage_bps / Decimal("10000"))
    return _quantize_price_inside_limit(
        mid * multiplier,
        sz_decimals=sz_decimals,
        is_buy=is_buy,
    )


def wire_size(size: Decimal, sz_decimals: int) -> str:
    return decimal_to_wire(quantize_size(size, sz_decimals))


def wire_price(price: Decimal, sz_decimals: int, is_spot: bool = False) -> str:
    return decimal_to_wire(quantize_price(price, sz_decimals, is_spot=is_spot))
