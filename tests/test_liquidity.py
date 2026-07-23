from decimal import Decimal

import pytest

from hyperliquid_copytrader.liquidity import (
    assess_round_trip_quote,
    build_reduce_only_quote,
    build_round_trip_quote,
    parse_market_liquidity_snapshot,
)


def shaped_market(*, oracle: str = "100", bid: str = "99.8", ask: str = "100.2"):
    return (
        [
            {"universe": [{"name": "xyz:TEST", "szDecimals": 2}]},
            [{"oraclePx": oracle, "markPx": "100.1", "midPx": "100"}],
        ],
        {
            "coin": "xyz:TEST",
            "time": 1_000_000,
            "levels": [
                [{"px": bid, "sz": "2", "n": 1}],
                [{"px": ask, "sz": "2", "n": 1}],
            ],
        },
    )


def test_round_trip_quote_requires_full_depth_inside_oracle_envelope():
    meta_ctx, book = shaped_market()
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    quote, blockers = build_round_trip_quote(
        snapshot,
        opening_side="buy",
        requested_size=Decimal("1.5"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert blockers == []
    assert quote is not None
    assert quote.entry_limit == Decimal("100.2")
    assert quote.exit_limit == Decimal("99.8")
    assert quote.entry_visible_size == Decimal("2")
    assert quote.exit_visible_size == Decimal("2")
    assert quote.to_payload()["kind"] == "hip3_round_trip"


def test_round_trip_quote_blocks_when_exit_book_is_outside_application_envelope():
    meta_ctx, book = shaped_market(bid="96", ask="100.2")
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    quote, blockers = build_round_trip_quote(
        snapshot,
        opening_side="buy",
        requested_size=Decimal("0.2"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert quote is None
    assert any("visible sell exit depth" in blocker for blocker in blockers)


@pytest.mark.parametrize(
    (
        "opening_side",
        "bid",
        "ask",
        "entry_shortfall",
        "exit_shortfall",
        "entry_visible",
        "exit_visible",
    ),
    [
        ("sell", "98", "100.2", True, False, Decimal("0"), Decimal("2")),
        ("buy", "96", "100.2", False, True, Decimal("2"), Decimal("0")),
        ("buy", "98", "102", True, True, Decimal("0"), Decimal("0")),
    ],
)
def test_round_trip_assessment_marks_only_valid_book_depth_shortfalls_retryable(
    opening_side,
    bid,
    ask,
    entry_shortfall,
    exit_shortfall,
    entry_visible,
    exit_visible,
):
    meta_ctx, book = shaped_market(bid=bid, ask=ask)
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    assessment = assess_round_trip_quote(
        snapshot,
        opening_side=opening_side,
        requested_size=Decimal("0.2"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert assessment.quote is None
    assert assessment.retryable_liquidity is True
    assert assessment.entry_depth_shortfall is entry_shortfall
    assert assessment.exit_depth_shortfall is exit_shortfall
    assert assessment.entry_visible_size == entry_visible
    assert assessment.exit_visible_size == exit_visible
    assert assessment.blockers

    quote, blockers = build_round_trip_quote(
        snapshot,
        opening_side=opening_side,
        requested_size=Decimal("0.2"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )
    assert quote is assessment.quote
    assert blockers == list(assessment.blockers)


@pytest.mark.parametrize(
    (
        "opening_side",
        "empty_bids",
        "empty_asks",
        "entry_shortfall",
        "exit_shortfall",
        "entry_visible",
        "exit_visible",
    ),
    [
        ("sell", True, False, True, False, Decimal("0"), Decimal("2")),
        ("buy", True, False, False, True, Decimal("2"), Decimal("0")),
        ("buy", False, True, True, False, Decimal("0"), Decimal("2")),
        ("sell", False, True, False, True, Decimal("2"), Decimal("0")),
        ("buy", True, True, True, True, Decimal("0"), Decimal("0")),
    ],
)
def test_round_trip_assessment_treats_empty_book_sides_as_retryable_liquidity(
    opening_side,
    empty_bids,
    empty_asks,
    entry_shortfall,
    exit_shortfall,
    entry_visible,
    exit_visible,
):
    meta_ctx, book = shaped_market()
    if empty_bids:
        book["levels"][0] = []
    if empty_asks:
        book["levels"][1] = []
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    assessment = assess_round_trip_quote(
        snapshot,
        opening_side=opening_side,
        requested_size=Decimal("0.2"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert assessment.quote is None
    assert assessment.retryable_liquidity is True
    assert assessment.entry_depth_shortfall is entry_shortfall
    assert assessment.exit_depth_shortfall is exit_shortfall
    assert assessment.entry_visible_size == entry_visible
    assert assessment.exit_visible_size == exit_visible


@pytest.mark.parametrize(
    ("levels", "message"),
    [
        (
            [
                {"px": "99.8", "sz": "2", "n": 1},
                [{"px": "100.2", "sz": "2", "n": 1}],
            ],
            "bid levels have an invalid shape",
        ),
        (
            [[{"px": "99.8", "sz": "2", "n": 1}], ["malformed"]],
            "ask level has an invalid shape",
        ),
        (
            [
                [{"px": "100.2", "sz": "2", "n": 1}],
                [{"px": "100.1", "sz": "2", "n": 1}],
            ],
            "l2Book is crossed or locked",
        ),
    ],
)
def test_market_liquidity_parser_keeps_malformed_or_crossed_books_fatal(levels, message):
    meta_ctx, book = shaped_market()
    book["levels"] = levels

    with pytest.raises(ValueError, match=message):
        parse_market_liquidity_snapshot(
            "xyz:TEST",
            meta_and_contexts=meta_ctx,
            l2_book=book,
            observed_ms=1_000_100,
        )


@pytest.mark.parametrize(
    ("opening_side", "entry_limit", "exit_limit"),
    [
        ("buy", Decimal("100.24"), Decimal("99.876")),
        ("sell", Decimal("99.876"), Decimal("100.24")),
    ],
)
def test_round_trip_quote_rounds_each_limit_outward_to_preserve_book_crossing(
    opening_side, entry_limit, exit_limit
):
    meta_ctx, book = shaped_market(bid="99.8764", ask="100.234")
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    quote, blockers = build_round_trip_quote(
        snapshot,
        opening_side=opening_side,
        requested_size=Decimal("1"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert blockers == []
    assert quote is not None
    assert quote.entry_limit == entry_limit
    assert quote.exit_limit == exit_limit


def test_round_trip_quote_fails_closed_when_crossing_price_exceeds_oracle_envelope():
    meta_ctx, book = shaped_market(bid="99.9", ask="100.2344")
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    assessment = assess_round_trip_quote(
        snapshot,
        opening_side="buy",
        requested_size=Decimal("1"),
        oracle_envelope_bps=Decimal("23.45"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert assessment.quote is None
    assert assessment.retryable_liquidity is False
    assert assessment.entry_depth_shortfall is False
    assert assessment.exit_depth_shortfall is False
    assert any("cannot be represented" in blocker for blocker in assessment.blockers)

    quote, blockers = build_round_trip_quote(
        snapshot,
        opening_side="buy",
        requested_size=Decimal("1"),
        oracle_envelope_bps=Decimal("23.45"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )
    assert quote is assessment.quote
    assert blockers == list(assessment.blockers)


def test_reduce_only_quote_uses_exact_safe_book_level_and_blocks_unsafe_bid():
    meta_ctx, book = shaped_market(bid="99.5")
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )
    quote, blockers = build_reduce_only_quote(
        snapshot,
        side="sell",
        requested_size=Decimal("0.5"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )
    assert blockers == []
    assert quote is not None
    assert quote.limit_price == Decimal("99.5")

    unsafe_meta, unsafe_book = shaped_market(bid="98")
    unsafe = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=unsafe_meta,
        l2_book=unsafe_book,
        observed_ms=1_000_100,
    )
    quote, blockers = build_reduce_only_quote(
        unsafe,
        side="sell",
        requested_size=Decimal("0.5"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )
    assert quote is None
    assert any("visible sell reduce-only depth" in blocker for blocker in blockers)


@pytest.mark.parametrize(
    ("side", "expected_limit"),
    [
        ("buy", Decimal("100.24")),
        ("sell", Decimal("99.876")),
    ],
)
def test_reduce_only_quote_rounds_outward_to_preserve_book_crossing(side, expected_limit):
    meta_ctx, book = shaped_market(bid="99.8764", ask="100.234")
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_000_100,
    )

    quote, blockers = build_reduce_only_quote(
        snapshot,
        side=side,
        requested_size=Decimal("1"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_000_100,
    )

    assert blockers == []
    assert quote is not None
    assert quote.limit_price == expected_limit


def test_market_liquidity_parser_rejects_stale_or_misaligned_data():
    meta_ctx, book = shaped_market()
    snapshot = parse_market_liquidity_snapshot(
        "xyz:TEST",
        meta_and_contexts=meta_ctx,
        l2_book=book,
        observed_ms=1_010_000,
    )
    assessment = assess_round_trip_quote(
        snapshot,
        opening_side="sell",
        requested_size=Decimal("0.1"),
        oracle_envelope_bps=Decimal("100"),
        max_age_ms=1_000,
        sz_decimals=2,
        current_ms=1_010_000,
    )
    assert assessment.quote is None
    assert assessment.retryable_liquidity is False
    assert assessment.entry_visible_size is None
    assert assessment.exit_visible_size is None
    assert any("stale" in blocker for blocker in assessment.blockers)


def test_market_liquidity_parser_rejects_invalid_oracle_before_assessment():
    meta_ctx, book = shaped_market()
    meta_ctx[1][0]["oraclePx"] = "NaN"

    with pytest.raises(ValueError, match="decimal value must be finite"):
        parse_market_liquidity_snapshot(
            "xyz:TEST",
            meta_and_contexts=meta_ctx,
            l2_book=book,
            observed_ms=1_000_100,
        )


@pytest.mark.parametrize("response_coin", ["other:TEST", "TEST"])
def test_market_liquidity_parser_rejects_l2_book_for_a_different_market(response_coin):
    meta_ctx, book = shaped_market()
    book["coin"] = response_coin

    with pytest.raises(ValueError, match="does not match the requested market"):
        parse_market_liquidity_snapshot(
            "xyz:TEST",
            meta_and_contexts=meta_ctx,
            l2_book=book,
            observed_ms=1_000_100,
        )
