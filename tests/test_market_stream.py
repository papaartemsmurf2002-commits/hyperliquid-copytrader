from __future__ import annotations

from decimal import Decimal

import pytest

from hyperliquid_copytrader.market_catalog import (
    CatalogMarket,
    CatalogRevision,
    MarketReadiness,
)
from hyperliquid_copytrader.market_stream import (
    MarketStream,
    MarketStreamError,
    executable_ioc,
)


NOW = 2_000_000


def _market(
    symbol: str,
    *,
    dex: str,
    asset_id: int,
    index: int,
    sz_decimals: int = 3,
    readiness: MarketReadiness = MarketReadiness.READY,
    delisted: bool = False,
) -> CatalogMarket:
    return CatalogMarket(
        symbol=symbol,
        dex=dex,
        asset_id=asset_id,
        dex_index=0 if dex == "" else 1,
        universe_index=index,
        sz_decimals=sz_decimals,
        max_leverage=20,
        readiness=readiness,
        is_delisted=delisted,
    )


def _catalog() -> CatalogRevision:
    return CatalogRevision(
        sequence=1,
        revision_id="catalog-1",
        policy_version="test-v1",
        network="mainnet",
        observed_ms=NOW,
        wire_dexes=("", "xyz"),
        markets=(
            _market("BTC", dex="", asset_id=0, index=0),
            _market("xyz:FOO", dex="xyz", asset_id=110_000, index=0, sz_decimals=2),
            _market(
                "OLD",
                dex="",
                asset_id=2,
                index=2,
                readiness=MarketReadiness.DELISTED,
                delisted=True,
            ),
            _market(
                "xyz:BAD",
                dex="xyz",
                asset_id=110_001,
                index=1,
                readiness=MarketReadiness.UNTRUSTED,
            ),
        ),
        snapshot_sha256="a" * 64,
        dex_bracket_before_sha256="b" * 64,
        dex_bracket_after_sha256="b" * 64,
    )


def _context(coin: str = "BTC", *, oracle: str = "100", mark: str = "100.1") -> dict:
    return {
        "channel": "activeAssetCtx",
        "data": {"coin": coin, "ctx": {"oraclePx": oracle, "markPx": mark}},
    }


def _book(
    coin: str = "BTC",
    *,
    time_ms: int = NOW,
    bids: list[dict] | None = None,
    asks: list[dict] | None = None,
) -> dict:
    return {
        "channel": "l2Book",
        "data": {
            "coin": coin,
            "time": time_ms,
            "levels": [
                bids
                if bids is not None
                else [{"px": "99", "sz": "2", "n": 3}, {"px": "98", "sz": "4"}],
                asks
                if asks is not None
                else [{"px": "101", "sz": "5", "n": 2}, {"px": "102", "sz": "6"}],
            ],
        },
    }


def _bbo(
    coin: str = "BTC",
    *,
    time_ms: int = NOW,
    bid: str | None = "99",
    ask: str | None = "101",
    bid_size: str = "2",
    ask_size: str = "5",
) -> dict:
    return {
        "channel": "bbo",
        "data": {
            "coin": coin,
            "time": time_ms,
            "bbo": [
                None if bid is None else {"px": bid, "sz": bid_size, "n": 1},
                None if ask is None else {"px": ask, "sz": ask_size, "n": 1},
            ],
        },
    }


def test_subscriptions_follow_only_the_active_catalog_markets() -> None:
    stream = MarketStream(catalog=_catalog())
    change = stream.set_active_markets(["btc", "xyz:foo", "BTC"])

    assert change.added == ("BTC", "xyz:FOO")
    assert change.removed == ()
    assert change.subscribe == stream.subscription_specs
    assert stream.subscription_specs == (
        {"type": "activeAssetCtx", "coin": "BTC"},
        {"type": "l2Book", "coin": "BTC"},
        {"type": "bbo", "coin": "BTC"},
        {"type": "activeAssetCtx", "coin": "xyz:FOO"},
        {"type": "l2Book", "coin": "xyz:FOO"},
        {"type": "bbo", "coin": "xyz:FOO"},
    )

    removed = stream.set_active_markets(["xyz:FOO"])
    assert removed.added == ()
    assert removed.removed == ("BTC",)
    assert removed.unsubscribe == (
        {"type": "activeAssetCtx", "coin": "BTC"},
        {"type": "l2Book", "coin": "BTC"},
        {"type": "bbo", "coin": "BTC"},
    )


@pytest.mark.parametrize("market", ["ETH", "OLD", "xyz:BAD"])
def test_active_market_requires_a_trusted_non_delisted_catalog_identity(market: str) -> None:
    stream = MarketStream(catalog=_catalog())
    with pytest.raises(MarketStreamError):
        stream.set_active_markets([market])
    assert stream.active_markets == ()


def test_complete_snapshot_exposes_catalog_precision_depth_and_freshness() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)

    assert stream.apply(_context(), epoch=epoch, received_ms=NOW) is None
    snapshot = stream.apply(_book(), epoch=epoch, received_ms=NOW + 1)

    assert snapshot is not None
    assert snapshot.market == "BTC"
    assert snapshot.catalog_revision == "catalog-1"
    assert snapshot.asset_id == 0
    assert snapshot.sz_decimals == 3
    assert snapshot.size_quantum == Decimal("0.001")
    assert snapshot.oracle_px == Decimal("100")
    assert snapshot.mark_px == Decimal("100.1")
    assert snapshot.best_bid == Decimal("99")
    assert snapshot.best_ask == Decimal("101")
    assert snapshot.bids[0].size == Decimal("2")
    assert snapshot.bids[0].order_count == 3
    assert snapshot.is_fresh(now_ms=NOW + 50, max_age_ms=100, connection_epoch=epoch)
    assert not snapshot.is_fresh(now_ms=NOW + 200, max_age_ms=100, connection_epoch=epoch)
    assert stream.fresh_snapshot("BTC", now_ms=NOW + 50, max_age_ms=100) == snapshot

    stream.note_connection_activity(epoch=epoch, received_ms=NOW + 180)
    assert stream.fresh_snapshot("BTC", now_ms=NOW + 200, max_age_ms=100) is None


def test_other_market_activity_cannot_keep_a_frozen_market_snapshot_fresh() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(), epoch=epoch, received_ms=NOW)
    snapshot = stream.apply(_book(), epoch=epoch, received_ms=NOW + 1)

    assert snapshot is not None
    assert not snapshot.is_fresh(
        now_ms=NOW + 10_002,
        max_age_ms=100,
        connection_epoch=epoch,
        connection_activity_ms=NOW + 10_001,
    )


def test_one_sided_book_preserves_the_available_reduction_side() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(), epoch=epoch, received_ms=NOW)
    snapshot = stream.apply(
        _book(asks=[]),
        epoch=epoch,
        received_ms=NOW + 1,
    )

    assert snapshot is not None
    assert snapshot.best_bid == Decimal("99")
    assert snapshot.best_ask_or_none is None
    assert executable_ioc(
        snapshot,
        is_buy=False,
        requested_size=Decimal("0.2"),
        max_slippage_bps=Decimal("100"),
    ) is not None
    assert (
        executable_ioc(
            snapshot,
            is_buy=True,
            requested_size=Decimal("0.2"),
            max_slippage_bps=Decimal("100"),
        )
        is None
    )


def test_empty_book_is_valid_non_executable_market_state() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(), epoch=epoch, received_ms=NOW)
    stream.apply(
        _book(bids=[], asks=[]),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    snapshot = stream.apply(
        _bbo(time_ms=NOW + 2, bid=None, ask=None),
        epoch=epoch,
        received_ms=NOW + 2,
    )

    assert snapshot is not None
    assert snapshot.bids == ()
    assert snapshot.asks == ()
    assert executable_ioc(
        snapshot,
        is_buy=True,
        requested_size=Decimal("0.2"),
        max_slippage_bps=Decimal("50"),
    ) is None
    assert executable_ioc(
        snapshot,
        is_buy=False,
        requested_size=Decimal("0.2"),
        max_slippage_bps=Decimal("50"),
    ) is None


def test_hip3_snapshot_uses_qualified_identity_and_its_own_precision() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["xyz:foo"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context("xyz:FOO"), epoch=epoch, received_ms=NOW)
    snapshot = stream.apply(_book("xyz:FOO"), epoch=epoch, received_ms=NOW + 1)

    assert snapshot is not None
    assert snapshot.market == "xyz:FOO"
    assert snapshot.asset_id == 110_000
    assert snapshot.sz_decimals == 2
    assert snapshot.size_quantum == Decimal("0.01")


def test_reconnect_does_not_mix_old_context_or_accept_old_epoch_frames() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    first_epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(), epoch=first_epoch, received_ms=NOW)
    assert stream.apply(_book(), epoch=first_epoch, received_ms=NOW + 1) is not None

    second_epoch = stream.begin_connection(received_ms=NOW + 2)
    assert stream.snapshot("BTC") is None
    with pytest.raises(MarketStreamError, match="stale connection epoch"):
        stream.apply(_context(), epoch=first_epoch, received_ms=NOW + 3)
    assert stream.apply(_book(), epoch=second_epoch, received_ms=NOW + 3) is None
    snapshot = stream.apply(_context(), epoch=second_epoch, received_ms=NOW + 4)
    assert snapshot is not None
    assert snapshot.connection_epoch == second_epoch


@pytest.mark.parametrize(
    "message, match",
    [
        (
            _book(bids=[{"px": "99", "sz": "1"}, {"px": "100", "sz": "1"}]),
            "strictly descending",
        ),
        (
            _book(asks=[{"px": "101", "sz": "1"}, {"px": "100", "sz": "1"}]),
            "strictly ascending",
        ),
        (
            _book(bids=[{"px": "101", "sz": "1"}], asks=[{"px": "101", "sz": "1"}]),
            "locked or crossed",
        ),
        (
            _book(bids=[{"px": "99", "sz": "0"}]),
            "finite and positive",
        ),
    ],
)
def test_malformed_depth_is_rejected_without_replacing_current_book(
    message: dict, match: str
) -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(), epoch=epoch, received_ms=NOW)
    stream.apply(_book(), epoch=epoch, received_ms=NOW + 1)

    with pytest.raises(MarketStreamError, match=match):
        stream.apply(message, epoch=epoch, received_ms=NOW + 2)
    assert stream.snapshot("BTC") is not None
    assert stream.snapshot("BTC").book_time_ms == NOW  # type: ignore[union-attr]


def test_out_of_order_book_and_inactive_market_frames_are_rejected() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_book(time_ms=NOW), epoch=epoch, received_ms=NOW)
    with pytest.raises(MarketStreamError, match="backwards in exchange time"):
        stream.apply(_book(time_ms=NOW - 1), epoch=epoch, received_ms=NOW + 1)
    with pytest.raises(MarketStreamError, match="inactive xyz:FOO"):
        stream.apply(_context("xyz:FOO"), epoch=epoch, received_ms=NOW + 1)


def test_executable_ioc_submits_full_request_and_records_visible_depth() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(mark="100"), epoch=epoch, received_ms=NOW)
    snapshot = stream.apply(
        _book(
            asks=[{"px": "100.1", "sz": "0.004"}, {"px": "100.2", "sz": "0.006"}],
            bids=[{"px": "99.9", "sz": "0.001"}],
        ),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    assert snapshot is not None

    chunk = executable_ioc(
        snapshot,
        is_buy=True,
        requested_size=Decimal("0.02"),
        max_slippage_bps=Decimal("25"),
    )

    assert chunk is not None
    assert chunk.size == Decimal("0.020")
    assert chunk.visible_size == Decimal("0.010")
    assert chunk.limit_px == Decimal("100.35")
    assert chunk.estimated_vwap == Decimal("100.16")


def test_latest_bbo_replaces_stale_l2_top_for_ioc_pricing() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(mark="100"), epoch=epoch, received_ms=NOW)
    stream.apply(
        _book(
            asks=[{"px": "100.1", "sz": "1"}, {"px": "100.2", "sz": "2"}],
            bids=[{"px": "99.9", "sz": "1"}, {"px": "99.8", "sz": "2"}],
        ),
        epoch=epoch,
        received_ms=NOW + 1,
    )
    snapshot = stream.apply(
        _bbo(time_ms=NOW + 2, bid="100.2", ask="100.4", bid_size="0.3", ask_size="0.4"),
        epoch=epoch,
        received_ms=NOW + 2,
    )

    assert snapshot is not None
    assert snapshot.best_bid == Decimal("100.2")
    assert snapshot.best_ask == Decimal("100.4")
    planned = executable_ioc(
        snapshot,
        is_buy=True,
        requested_size=Decimal("0.5"),
        max_slippage_bps=Decimal("50"),
    )
    assert planned is not None
    assert planned.size == Decimal("0.500")
    assert planned.visible_size == Decimal("0.400")
    assert planned.limit_px == Decimal("100.90")


def test_hard_economic_limit_refuses_a_bbo_outside_the_leader_cap() -> None:
    stream = MarketStream(catalog=_catalog(), active_markets=["BTC"])
    epoch = stream.begin_connection(received_ms=NOW - 1)
    stream.apply(_context(mark="100"), epoch=epoch, received_ms=NOW)
    stream.apply(_book(), epoch=epoch, received_ms=NOW + 1)
    snapshot = stream.apply(
        _bbo(time_ms=NOW + 2, bid="100.4", ask="100.6"),
        epoch=epoch,
        received_ms=NOW + 2,
    )
    assert snapshot is not None

    assert (
        executable_ioc(
            snapshot,
            is_buy=True,
            requested_size=Decimal("0.2"),
            max_slippage_bps=None,
            hard_limit_px=Decimal("100.5"),
        )
        is None
    )
