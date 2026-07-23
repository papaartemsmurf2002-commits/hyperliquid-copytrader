from __future__ import annotations

from decimal import Decimal

import pytest

from hyperliquid_copytrader.market_stream import DepthLevel, MarketSnapshot
from scripts.run_bounded_min_notional_probe import ProbeError, _candidate


def _snapshot(*, mark: str, sz_decimals: int, ask: str, ask_size: str) -> MarketSnapshot:
    return MarketSnapshot(
        market="BTC",
        catalog_revision="probe-test",
        asset_id=0,
        sz_decimals=sz_decimals,
        max_leverage=50,
        oracle_px=Decimal(mark),
        mark_px=Decimal(mark),
        bids=(DepthLevel(Decimal(mark) - Decimal("1"), Decimal(ask_size)),),
        asks=(DepthLevel(Decimal(ask), Decimal(ask_size)),),
        book_time_ms=1,
        context_received_ms=1,
        book_received_ms=1,
        bbo_time_ms=1,
        bbo_received_ms=1,
        connection_epoch=1,
    )


def test_candidate_bypasses_only_canonical_minimum_for_bounded_probe() -> None:
    candidate = _candidate(
        _snapshot(mark="66000", sz_decimals=5, ask="66001", ask_size="1"),
        {
            "leverage": 20,
            "margin_mode": "cross",
            "max_buy_size": Decimal("0.01"),
            "available_buy_size": Decimal("1"),
        },
    )

    assert candidate["requested_size"] == Decimal("0.00001")
    assert Decimal("0") < candidate["wire_notional_usd"] <= Decimal("1.10")
    assert "below the perp minimum" in candidate["canonical_reason"]
    assert candidate["required_margin_usd"] is not None


def test_candidate_refuses_probe_when_depth_cannot_fit_bounded_envelope() -> None:
    with pytest.raises(ProbeError, match="no executable depth"):
        _candidate(
            _snapshot(mark="66000", sz_decimals=5, ask="67000", ask_size="1"),
            {
                "leverage": 20,
                "margin_mode": "cross",
                "max_buy_size": Decimal("0.01"),
                "available_buy_size": Decimal("1"),
            },
        )
