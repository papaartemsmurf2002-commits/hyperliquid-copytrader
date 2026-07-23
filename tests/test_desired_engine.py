from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from hyperliquid_copytrader.continuous_config import ContinuousSlotConfig
from hyperliquid_copytrader.desired_engine import build_desired_portfolio, choose_next_action
from hyperliquid_copytrader.models import Position


def _slot() -> ContinuousSlotConfig:
    return ContinuousSlotConfig(
        slot="one",
        source_address="0x" + "1" * 40,
        follower_account_address="0x" + "2" * 40,
        credential_profile_id="one",
        multiplier=Decimal("1"),
        max_order_notional_usd=Decimal("12"),
        max_gross_exposure_usd=Decimal("15"),
        max_open_positions=1,
        max_leverage=1,
        action_limit_per_minute=6,
        allowed_markets=("BTC",),
        enabled=True,
    )


def _desired(source_size: str = "0.01"):
    return build_desired_portfolio(
        _slot(),
        source_positions={"BTC": Position("BTC", Decimal(source_size), leverage=20)},
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("1000")},
    )


def test_target_caps_gross_and_leverage() -> None:
    desired = _desired("1")

    assert desired.gross_notional_usd == Decimal("15")
    assert desired.gross_scale == Decimal("0.015")
    assert desired.positions["BTC"].leverage == 1


def test_larger_follower_equity_scales_target_until_explicit_gross_cap() -> None:
    slot = ContinuousSlotConfig(
        slot="one",
        source_address="0x" + "1" * 40,
        follower_account_address="0x" + "2" * 40,
        credential_profile_id="one",
        multiplier=Decimal("0.75"),
        max_order_notional_usd=Decimal("10000"),
        max_gross_exposure_usd=Decimal("10000"),
        max_open_positions=1,
        max_leverage=20,
        action_limit_per_minute=6,
        allowed_markets=("BTC",),
        enabled=True,
    )
    inputs = {
        "slot": slot,
        "source_positions": {"BTC": Position("BTC", Decimal("0.01"))},
        "source_equity": Decimal("1000"),
        "mids": {"BTC": Decimal("60000")},
    }

    small = build_desired_portfolio(follower_equity=Decimal("50"), **inputs)
    large = build_desired_portfolio(follower_equity=Decimal("500"), **inputs)

    assert small.positions["BTC"].size == Decimal("0.000375")
    assert large.positions["BTC"].size == Decimal("0.00375")
    assert large.positions["BTC"].size == small.positions["BTC"].size * 10
    assert small.gross_scale == large.gross_scale == Decimal("1")

    capped = build_desired_portfolio(
        follower_equity=Decimal("50000"),
        **{**inputs, "slot": replace(slot, max_gross_exposure_usd=Decimal("500"))},
    )
    assert capped.gross_notional_usd <= Decimal("500")
    assert Decimal("500") - capped.gross_notional_usd < Decimal("1e-20")
    assert capped.gross_scale < Decimal("1")


def test_one_hundred_dollar_follower_crosses_minimum_without_inflating_dust() -> None:
    slot = replace(
        _slot(),
        multiplier=Decimal("0.75"),
        max_order_notional_usd=Decimal("5000"),
        max_gross_exposure_usd=Decimal("5000"),
        max_leverage=50,
    )
    inputs = {
        "slot": slot,
        "source_positions": {"BTC": Position("BTC", Decimal("0.2"))},
        "source_equity": Decimal("1000"),
        "mids": {"BTC": Decimal("1000")},
    }
    fifty_dollar_target = build_desired_portfolio(
        follower_equity=Decimal("50"),
        **inputs,
    )
    hundred_dollar_target = build_desired_portfolio(
        follower_equity=Decimal("100"),
        **inputs,
    )

    assert fifty_dollar_target.positions["BTC"].size == Decimal("0.00750")
    assert hundred_dollar_target.positions["BTC"].size == Decimal("0.0150")
    assert hundred_dollar_target.positions["BTC"].size == (
        fifty_dollar_target.positions["BTC"].size * 2
    )

    deferred = choose_next_action(
        slot,
        fifty_dollar_target,
        follower_positions={},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )
    executable = choose_next_action(
        slot,
        hundred_dollar_target,
        follower_positions={},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )

    assert deferred.action is None
    assert "sub-minimum debt" in str(deferred.blocker)
    assert executable.action is not None
    assert executable.action.size == Decimal("0.015")


def test_reversal_emits_only_reduce_only_close() -> None:
    desired = _desired("0.01")

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={"BTC": Position("BTC", Decimal("-0.02"))},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is not None
    assert decision.action.side == "buy"
    assert decision.action.reduce_only is True
    assert decision.action.size == Decimal("0.012")
    assert decision.action.reason == "close reversal and replan"


def test_unresolved_action_blocks_second_send() -> None:
    desired = _desired("0.01")

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={},
        unresolved_signed_remaining={"BTC": Decimal("0.005")},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is None
    assert decision.blocker == "BTC has an unresolved attempted action"


def test_subminimum_debt_is_not_inflated() -> None:
    desired = _desired("0.001")

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is None
    assert "sub-minimum debt" in str(decision.blocker)


def test_subminimum_confirmed_position_can_be_closed_reduce_only() -> None:
    desired = build_desired_portfolio(
        _slot(),
        source_positions={},
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("1000")},
    )

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={"BTC": Position("BTC", Decimal("0.005"))},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("1000")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is not None
    assert decision.action.side == "sell"
    assert decision.action.size == Decimal("0.005")
    assert decision.action.reduce_only is True


def test_subminimum_partial_reduction_closes_when_flat_is_closer_to_target() -> None:
    desired = build_desired_portfolio(
        _slot(),
        source_positions={"BTC": Position("BTC", Decimal("0.095"))},
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("60")},
    )

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={"BTC": Position("BTC", Decimal("0.203"))},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("60")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is not None
    assert decision.action.side == "sell"
    assert decision.action.size == Decimal("0.2")
    assert decision.action.reduce_only is True
    assert decision.action.reason == "move to the nearest executable lower-risk position"


def test_subminimum_partial_reduction_waits_when_current_position_is_closer() -> None:
    desired = build_desired_portfolio(
        _slot(),
        source_positions={"BTC": Position("BTC", Decimal("0.15"))},
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("60")},
    )

    decision = choose_next_action(
        _slot(),
        desired,
        follower_positions={"BTC": Position("BTC", Decimal("0.203"))},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("60")},
        size_decimals={"BTC": 3},
    )

    assert decision.action is None
    assert "below the venue minimum" in str(decision.blocker)


def test_unrelated_exposure_is_closed_before_alphabetically_earlier_entry() -> None:
    slot = ContinuousSlotConfig(
        slot="one",
        source_address="0x" + "1" * 40,
        follower_account_address="0x" + "2" * 40,
        credential_profile_id="one",
        multiplier=Decimal("1"),
        max_order_notional_usd=Decimal("100"),
        max_gross_exposure_usd=Decimal("100"),
        max_open_positions=2,
        max_leverage=1,
        action_limit_per_minute=6,
        allowed_markets=("BTC", "ETH"),
        enabled=True,
    )
    desired = build_desired_portfolio(
        slot,
        source_positions={"BTC": Position("BTC", Decimal("0.001"))},
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("60000"), "ETH": Decimal("3000")},
    )

    decision = choose_next_action(
        slot,
        desired,
        follower_positions={"ETH": Position("ETH", Decimal("0.02"))},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("60000"), "ETH": Decimal("3000")},
        size_decimals={"BTC": 5, "ETH": 4},
    )

    assert decision.action is not None
    assert decision.action.market == "ETH"
    assert decision.action.side == "sell"
    assert decision.action.reduce_only is True


def test_subminimum_market_does_not_starve_an_executable_market() -> None:
    slot = ContinuousSlotConfig(
        slot="one",
        source_address="0x" + "1" * 40,
        follower_account_address="0x" + "2" * 40,
        credential_profile_id="one",
        multiplier=Decimal("1"),
        max_order_notional_usd=Decimal("100"),
        max_gross_exposure_usd=Decimal("100"),
        max_open_positions=2,
        max_leverage=1,
        action_limit_per_minute=6,
        allowed_markets=("BTC", "ETH"),
        enabled=True,
    )
    desired = build_desired_portfolio(
        slot,
        source_positions={
            "BTC": Position("BTC", Decimal("0.0001")),
            "ETH": Position("ETH", Decimal("0.01")),
        },
        source_equity=Decimal("100"),
        follower_equity=Decimal("100"),
        mids={"BTC": Decimal("60000"), "ETH": Decimal("3000")},
    )

    decision = choose_next_action(
        slot,
        desired,
        follower_positions={},
        unresolved_signed_remaining={},
        mids={"BTC": Decimal("60000"), "ETH": Decimal("3000")},
        size_decimals={"BTC": 5, "ETH": 4},
    )

    assert decision.action is not None
    assert decision.action.market == "ETH"
    assert decision.skipped_blockers
    assert "BTC residual is sub-minimum debt" in decision.skipped_blockers[0]
