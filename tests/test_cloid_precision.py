from __future__ import annotations

from decimal import Decimal

import pytest

from hyperliquid_copytrader.cloid import deterministic_cloid, validate_cloid
from hyperliquid_copytrader.models import decimal_to_wire, parse_decimal
from hyperliquid_copytrader.precision import aggressive_ioc_price, quantize_price, quantize_size


def test_deterministic_cloid_is_stable_and_valid():
    first = deterministic_cloid("source", 1, {"coin": "BTC"})
    second = deterministic_cloid("source", 1, {"coin": "BTC"})
    assert first == second
    assert validate_cloid(first) == first


def test_invalid_cloid_rejected():
    with pytest.raises(ValueError):
        validate_cloid("0xabc")


def test_size_rounds_toward_zero():
    assert quantize_size(Decimal("1.234567"), 3) == Decimal("1.234")
    assert quantize_size(Decimal("-1.234567"), 3) == Decimal("-1.234")


def test_price_respects_sig_figs_and_decimal_budget():
    assert quantize_price(Decimal("1234.56"), sz_decimals=1) == Decimal("1234.6")
    assert quantize_price(Decimal("0.0012345"), sz_decimals=1) == Decimal("0.00123")
    assert aggressive_ioc_price(Decimal("100"), True, Decimal("25"), 3) == Decimal("100.25")


def test_aggressive_ioc_rounding_never_crosses_slippage_envelope():
    mid = Decimal("1.04734")
    bps = Decimal("25")
    buy = aggressive_ioc_price(mid, True, bps, 5)
    sell = aggressive_ioc_price(mid, False, bps, 5)

    assert buy == Decimal("1.0")
    assert buy <= mid * (Decimal(1) + bps / Decimal("10000"))
    assert sell == Decimal("1.1")
    assert sell >= mid * (Decimal(1) - bps / Decimal("10000"))


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_nonfinite_decimal_values_are_rejected(value):
    with pytest.raises(ValueError):
        parse_decimal(value)
    with pytest.raises(ValueError):
        decimal_to_wire(value)
    with pytest.raises(ValueError):
        quantize_size(value, 3)
    with pytest.raises(ValueError):
        quantize_price(value, 3)
    with pytest.raises(ValueError):
        aggressive_ioc_price(value, True, Decimal("20"), 3)
