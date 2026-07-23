from decimal import Decimal

import pytest

from hyperliquid_copytrader.order_preflight import (
    HyperliquidPerpRules,
    OrderEligibility,
    preflight_hyperliquid_perp_order,
)


@pytest.mark.parametrize("market", ["BTC", "xyz:XYZ100"])
def test_risk_increasing_perp_below_published_minimum_is_rejected(market: str) -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules(market, sz_decimals=3, max_leverage=20),
        requested_quantity=Decimal("0.009"),
        price=Decimal("1000"),
        side="buy",
        max_order_notional_usd=Decimal("100"),
        reduce_only=False,
        leverage=10,
        available_collateral_usd=Decimal("50"),
    )

    assert result.eligibility is OrderEligibility.REJECTED
    assert result.requested_notional_usd == Decimal("9.000")
    assert result.required_margin_usd == Decimal("0.900")
    assert "minimum 10" in result.reason


def test_exact_reduce_only_close_below_minimum_remains_placeable() -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules("ACE", sz_decimals=2, max_leverage=5),
        requested_quantity=Decimal("0.75"),
        price=Decimal("1"),
        side="buy",
        max_order_notional_usd=Decimal("100"),
        reduce_only=True,
        current_position_size=Decimal("-0.75"),
        available_collateral_usd=Decimal("0.1"),
    )

    assert result.eligibility is OrderEligibility.PLACEABLE
    assert result.exact_reduce_only_close is True
    assert result.required_margin_usd == 0


def test_preflight_reports_valid_rounding_and_margin_separately() -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules("BTC", sz_decimals=3, max_leverage=20),
        requested_quantity=Decimal("0.0109"),
        price=Decimal("1000"),
        side="buy",
        max_order_notional_usd=Decimal("100"),
        reduce_only=False,
        leverage=10,
        available_collateral_usd=Decimal("2"),
    )

    assert result.eligibility is OrderEligibility.PLACEABLE_AFTER_ROUNDING
    assert result.rounded_quantity == Decimal("0.010")
    assert result.submitted_notional_usd == Decimal("10.000")
    assert result.required_margin_usd == Decimal("1.000")
    assert result.available_collateral_usd == Decimal("2")


def test_preflight_rejects_insufficient_available_collateral() -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules("BTC", sz_decimals=3, max_leverage=20),
        requested_quantity=Decimal("0.020"),
        price=Decimal("1000"),
        side="buy",
        max_order_notional_usd=Decimal("100"),
        reduce_only=False,
        leverage=10,
        available_collateral_usd=Decimal("1"),
    )

    assert result.eligibility is OrderEligibility.REJECTED
    assert "required margin 2.000" in result.reason


def test_preflight_reports_valid_price_precision_rounding() -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules("BTC", sz_decimals=3, max_leverage=20),
        requested_quantity=Decimal("0.100"),
        price=Decimal("100.1234567"),
        side="buy",
        max_order_notional_usd=Decimal("100"),
        reduce_only=False,
        leverage=10,
    )

    assert result.eligibility is OrderEligibility.PLACEABLE_AFTER_ROUNDING
    assert result.requested_price == Decimal("100.1234567")
    assert result.rounded_price == Decimal("100.120")
    assert result.submitted_notional_usd == Decimal("10.012000")
    assert "price to venue precision" in result.reason
    assert "available perp collateral is not authoritative" in result.reason


def test_same_direction_reduce_only_size_is_not_misclassified_as_exact_close() -> None:
    result = preflight_hyperliquid_perp_order(
        rules=HyperliquidPerpRules("ACE", sz_decimals=2, max_leverage=5),
        requested_quantity=Decimal("0.75"),
        price=Decimal("1"),
        side="sell",
        max_order_notional_usd=Decimal("100"),
        reduce_only=True,
        current_position_size=Decimal("-0.75"),
    )

    assert result.eligibility is OrderEligibility.REJECTED
    assert result.exact_reduce_only_close is False
    assert "partial reduce-only" in result.reason
