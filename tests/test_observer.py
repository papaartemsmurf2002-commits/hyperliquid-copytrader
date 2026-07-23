from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from hyperliquid_copytrader import observer as observer_module
from hyperliquid_copytrader.exchange.hyperliquid import HyperliquidExecutionAdapter
from hyperliquid_copytrader.models import SafeModeReason, SourceEvent, SourceEventType, now_ms
from hyperliquid_copytrader.observer import (
    SourceObserver,
    SourceWebsocketMessageError,
    normalize_fill_backfill,
    normalize_twap_slice_fill_backfill,
    normalize_ws_message,
    parse_clearinghouse_positions,
    parse_open_orders,
    source_websocket_subscriptions,
)
from hyperliquid_copytrader.safety import ConsistencyShield, SafeModeController
from hyperliquid_copytrader.unified_account import SourceDexScope, UnifiedAccountSnapshot

from .fixtures.fake_hyperliquid import FakeInfoClient


def make_observer(store, *, ws_url: str | None = "wss://example.invalid/ws", **kwargs):
    safe = SafeModeController(store)
    shield = ConsistencyShield(safe)
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=FakeInfoClient(),
        store=store,
        ws_url=ws_url,
        shield=shield,
        **kwargs,
    )
    return observer, safe


def test_record_source_event_dedupes_before_journal(store):
    observer, safe = make_observer(store)
    event = SourceEvent(
        "event-1",
        SourceEventType.FILL,
        exchange_ts_ms=1000,
        observed_ts_ms=1001,
    )
    assert observer.record_source_event(event) is True
    assert observer.record_source_event(event) is False
    assert store.count("source_events") == 1
    assert not safe.enabled


def test_record_source_event_repairs_missing_reaction_obligation_for_duplicate(store):
    observer, _ = make_observer(store)
    event = SourceEvent(
        "event-without-outbox",
        SourceEventType.FILL,
        exchange_ts_ms=1000,
        observed_ts_ms=1001,
    )
    assert store.append_source_event(event) is True
    assert store.source_reaction_status(event.idempotency_key) is None

    assert observer.record_source_event(event) is False

    assert store.source_reaction_status(event.idempotency_key) == "pending"
    assert store.count("source_events") == 1


def test_record_source_event_pauses_on_out_of_order_stream(store):
    observer, safe = make_observer(store)
    assert observer.record_source_event(
        SourceEvent("new", SourceEventType.FILL, exchange_ts_ms=2000)
    )
    with pytest.raises(SourceWebsocketMessageError, match="rejected by consistency shield"):
        observer.record_source_event(SourceEvent("old", SourceEventType.FILL, exchange_ts_ms=1000))
    assert store.count("source_events") == 1
    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT


def test_source_open_orders_cache_reuses_rest_baseline_and_applies_stream_delta(store, monkeypatch):
    clock = {"now": 100_000}
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: clock["now"])
    info = FakeInfoClient()
    info.open_orders = [{"coin": "BTC", "side": "B", "sz": "0.01", "limitPx": "50000", "oid": 11}]
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC",),
        open_orders_cache_ttl_ms=30_000,
    )

    first = observer.reconcile_once()
    clock["now"] += 1_000
    observer.record_ws_message_event(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "sz": "0",
                        "origSz": "0.01",
                        "limitPx": "50000",
                        "oid": 11,
                    },
                    "status": "filled",
                    "statusTimestamp": clock["now"],
                }
            ],
        }
    )
    # If the cache incorrectly refreshes, this would reintroduce the old REST order.
    second = observer.reconcile_once()

    assert [order.oid for order in first.open_orders] == [11]
    assert second.open_orders == []
    assert len([call for call in info.calls if call["type"] == "openOrders"]) == 1
    status = observer.open_order_cache_status()
    assert status["complete"] is True
    assert status["fresh"] is True
    assert status["dexes"]["<default>"]["age_ms"] == 1_000
    assert status["dexes"]["<default>"]["last_update_ms"] == 101_000


def test_source_open_orders_cache_expiry_requires_fresh_rest_truth(store, monkeypatch):
    clock = {"now": 200_000}
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: clock["now"])

    class ExpiringInfo(FakeInfoClient):
        reject_open_orders = False

        def info(self, payload):
            if payload["type"] == "openOrders" and self.reject_open_orders:
                raise RuntimeError("fresh openOrders unavailable")
            return super().info(payload)

    info = ExpiringInfo()
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC",),
        open_orders_cache_ttl_ms=30_000,
    )
    observer.reconcile_once()
    clock["now"] += 30_000
    info.reject_open_orders = True

    with pytest.raises(RuntimeError, match="fresh openOrders unavailable"):
        observer.reconcile_once()

    status = observer.open_order_cache_status()
    assert status["complete"] is True
    assert status["fresh"] is False
    assert status["dexes"]["<default>"]["age_ms"] == 30_000


def test_normalize_ws_message_uses_channel_and_timestamp():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {"channel": "userFills", "data": [{"time": 1234, "coin": "BTC"}]},
    )
    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 1234
    assert event.idempotency_key.startswith("0x")
    assert event.payload["event_subtype"] == "fill"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["event_count"] == 1


def test_normalize_user_events_fill_extracts_classification():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "userEvents",
            "data": {
                "fills": [
                    {
                        "coin": "BTC",
                        "side": "B",
                        "dir": "Open Long",
                        "time": 1234,
                        "oid": 55,
                        "cloid": "0xABC",
                        "hash": "0xFILL",
                    }
                ]
            },
        },
    )
    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "fill"
    assert event.payload["timestamp_source"] == "exchange"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["oids"] == ["55"]
    assert event.payload["cloids"] == ["0xabc"]
    assert event.payload["hashes"] == ["0xfill"]


def test_normalize_fill_backfill_extracts_classification():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_fill_backfill(
        source,
        {
            "coin": "BTC",
            "side": "B",
            "dir": "Open Long",
            "time": 1234,
            "oid": 55,
            "hash": "0xFILL",
            "tid": 99,
            "user": source.upper(),
        },
    )
    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "fill_backfill"
    assert event.payload["timestamp_source"] == "exchange"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["oids"] == ["55"]
    assert event.payload["hashes"] == ["0xfill"]


def test_normalize_twap_slice_fill_backfill_extracts_classification():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_twap_slice_fill_backfill(
        source,
        {
            "twapId": 42,
            "fill": {
                "coin": "BTC",
                "side": "B",
                "dir": "Open Long",
                "time": 1234,
                "oid": 55,
                "hash": "0xFILL",
                "tid": 99,
            },
        },
    )
    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "twap_slice_fill_backfill"
    assert event.payload["timestamp_source"] == "exchange"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["oids"] == ["55"]
    assert event.payload["hashes"] == ["0xfill"]
    assert event.payload["twap_ids"] == ["42"]


def test_normalize_order_updates_cancel_distinguishes_cancel_status():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "ETH",
                        "side": "A",
                        "limitPx": "3000",
                        "sz": "0.2",
                        "oid": 66,
                        "timestamp": 1900,
                        "cloid": "0xDEF",
                    },
                    "status": "canceled",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )
    assert event.event_type == SourceEventType.CANCEL
    assert event.exchange_ts_ms == 2000
    assert event.payload["event_subtype"] == "order_update:canceled"
    assert event.payload["statuses"] == ["canceled"]
    assert event.payload["coins"] == ["ETH"]
    assert event.payload["oids"] == ["66"]
    assert event.payload["cloids"] == ["0xdef"]


def test_normalize_order_updates_scheduled_cancel_is_cancel_event():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "sz": "0.01",
                        "oid": 77,
                        "timestamp": 1900,
                    },
                    "status": "scheduledCancel",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )
    assert event.event_type == SourceEventType.CANCEL
    assert event.payload["event_subtype"] == "order_update:scheduledcancel"


def test_normalize_order_updates_rejected_status_is_order_lifecycle_event():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "SOL",
                        "side": "B",
                        "sz": "1",
                        "oid": 99,
                        "timestamp": 1900,
                    },
                    "status": "perpMarginRejected",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )

    assert event.event_type == SourceEventType.OPEN_ORDER
    assert event.exchange_ts_ms == 2000
    assert event.payload["event_subtype"] == "order_update:perpmarginrejected"
    assert event.payload["statuses"] == ["perpmarginrejected"]
    assert event.payload["coins"] == ["SOL"]


def test_normalize_order_updates_triggered_status_is_order_lifecycle_event():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "ETH",
                        "side": "A",
                        "sz": "0.2",
                        "oid": 100,
                        "timestamp": 1900,
                    },
                    "status": "triggered",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )

    assert event.event_type == SourceEventType.OPEN_ORDER
    assert event.exchange_ts_ms == 2000
    assert event.payload["event_subtype"] == "order_update:triggered"
    assert event.payload["statuses"] == ["triggered"]
    assert event.payload["coins"] == ["ETH"]


def test_normalize_order_updates_filled_stays_order_lifecycle_event():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "50000",
                        "sz": "0",
                        "origSz": "0.01",
                        "oid": 88,
                        "timestamp": 1900,
                    },
                    "status": "filled",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )

    assert event.event_type == SourceEventType.OPEN_ORDER
    assert event.exchange_ts_ms == 2000
    assert event.payload["event_subtype"] == "order_update:filled"
    assert event.payload["statuses"] == ["filled"]
    assert event.payload["coins"] == ["BTC"]


def test_normalize_order_updates_uses_status_timestamp_not_order_creation_timestamp():
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "BTC",
                        "side": "B",
                        "sz": "0.01",
                        "oid": 101,
                        "timestamp": 5000,
                    },
                    "status": "filled",
                    "statusTimestamp": 2000,
                }
            ],
        },
    )

    assert event.exchange_ts_ms == 2000


def test_normalize_order_updates_requires_status_timestamp():
    with pytest.raises(SourceWebsocketMessageError, match="missing timestamp"):
        normalize_ws_message(
            "0xcf7c4feb434751146a48b895e96caeb15838f92c",
            {
                "channel": "orderUpdates",
                "data": [
                    {
                        "order": {
                            "coin": "BTC",
                            "side": "B",
                            "sz": "0.01",
                            "oid": 102,
                            "timestamp": 5000,
                        },
                        "status": "filled",
                    }
                ],
            },
        )


def test_empty_order_updates_use_observed_time_without_reaction_payload(monkeypatch):
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 3333)

    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {"channel": "orderUpdates", "data": []},
    )

    assert event.event_type == SourceEventType.OPEN_ORDER
    assert event.exchange_ts_ms == 3333
    assert event.observed_ts_ms == 3333
    assert event.payload["event_subtype"] == "order_update"
    assert event.payload["event_count"] == 0
    assert event.payload["timestamp_source"] == "observed"


def test_empty_user_events_fills_use_observed_time(monkeypatch):
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 4444)

    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {"channel": "userEvents", "data": {"fills": []}},
    )

    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 4444
    assert event.observed_ts_ms == 4444
    assert event.payload["event_subtype"] == "fill"
    assert event.payload["event_count"] == 0
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_active_asset_data_is_leverage_event(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 7777)

    event = normalize_ws_message(
        source,
        {
            "channel": "activeAssetData",
            "data": {
                "user": source,
                "coin": "BTC",
                "leverage": {"type": "cross", "value": 3},
                "maxTradeSzs": ["1.25", "1.50"],
                "availableToTrade": ["0.5", "0.75"],
            },
        },
    )

    assert event.event_type == SourceEventType.LEVERAGE
    assert event.exchange_ts_ms == 7777
    assert event.payload["event_subtype"] == "active_asset_data"
    assert event.payload["timestamp_source"] == "observed"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["leverage"] == "cross:3"
    assert event.payload["max_trade_sizes"] == "1.25/1.50"
    assert event.payload["available_to_trade"] == "0.5/0.75"
    assert event.payload["copy_signal_key"]


def test_copy_signal_keys_ignore_non_copy_snapshot_noise(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    observed = iter((8000, 8001, 8002, 8003))
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: next(observed))

    flat_a = normalize_ws_message(
        source,
        {
            "channel": "clearinghouseState",
            "data": {
                "user": source,
                "dex": "",
                "clearinghouseState": {"assetPositions": [], "time": 1},
            },
        },
    )
    flat_b = normalize_ws_message(
        source,
        {
            "channel": "clearinghouseState",
            "data": {
                "user": source,
                "dex": "",
                "clearinghouseState": {"assetPositions": [], "time": 2},
            },
        },
    )
    leverage_a = normalize_ws_message(
        source,
        {
            "channel": "activeAssetData",
            "data": {
                "user": source,
                "coin": "BTC",
                "leverage": {"type": "cross", "value": 3},
                "maxTradeSzs": ["1", "1"],
            },
        },
    )
    leverage_b = normalize_ws_message(
        source,
        {
            "channel": "activeAssetData",
            "data": {
                "user": source,
                "coin": "BTC",
                "leverage": {"type": "cross", "value": 3},
                "maxTradeSzs": ["2", "2"],
            },
        },
    )

    assert flat_a.payload["event_count"] == 0
    assert flat_a.payload["copy_signal_key"] == flat_b.payload["copy_signal_key"]
    assert leverage_a.payload["copy_signal_key"] == leverage_b.payload["copy_signal_key"]


def test_source_websocket_subscriptions_cover_official_user_streams():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    subscriptions = source_websocket_subscriptions(
        source.upper(),
        active_asset_symbols=("btc", "ETH", "BTC", ""),
    )
    types = {subscription["type"] for subscription in subscriptions}

    assert {
        "orderUpdates",
        "userEvents",
        "userFills",
        "userFundings",
        "userNonFundingLedgerUpdates",
        "userTwapSliceFills",
        "userTwapHistory",
        "twapStates",
        "notification",
        "webData3",
        "spotState",
        "allDexsClearinghouseState",
        "openOrders",
        "clearinghouseState",
        "activeAssetData",
    } <= types
    assert all(subscription["user"] == source for subscription in subscriptions)
    active_asset_subscriptions = [
        subscription for subscription in subscriptions if subscription["type"] == "activeAssetData"
    ]
    assert active_asset_subscriptions == [
        {"type": "activeAssetData", "user": source, "coin": "BTC"},
        {"type": "activeAssetData", "user": source, "coin": "ETH"},
    ]


def test_source_websocket_subscriptions_preserve_canonical_dex_prefix():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    subscriptions = source_websocket_subscriptions(
        source,
        active_asset_symbols=("xyz:aapl", "xyz:AAPL"),
    )

    assert [
        subscription for subscription in subscriptions if subscription["type"] == "activeAssetData"
    ] == [{"type": "activeAssetData", "user": source, "coin": "xyz:AAPL"}]


def test_full_universe_does_not_create_one_noisy_active_asset_stream_per_market():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    symbols = tuple(f"MKT{index}" for index in range(290))

    subscriptions = source_websocket_subscriptions(source, active_asset_symbols=symbols)

    assert not any(item["type"] == "activeAssetData" for item in subscriptions)
    assert {"orderUpdates", "userFills", "allDexsClearinghouseState", "webData3"} <= {
        item["type"] for item in subscriptions
    }
    assert len(subscriptions) == 14


@pytest.mark.parametrize(
    ("channel", "data", "subtype"),
    [
        ("userFills", {"isSnapshot": True, "user": "SOURCE", "fills": []}, "fill_snapshot"),
        (
            "userTwapSliceFills",
            {"isSnapshot": True, "user": "SOURCE", "twapSliceFills": []},
            "twap_slice_fill_snapshot",
        ),
        (
            "userFundings",
            {"isSnapshot": True, "user": "SOURCE", "fundings": []},
            "funding_snapshot",
        ),
        (
            "userNonFundingLedgerUpdates",
            {"isSnapshot": True, "user": "SOURCE", "nonFundingLedgerUpdates": []},
            "ledger_update",
        ),
        (
            "userTwapHistory",
            {"isSnapshot": True, "user": "SOURCE", "history": []},
            "twap_history",
        ),
        (
            "user",
            {"isSnapshot": True, "user": "SOURCE", "assetPositions": []},
            "user_snapshot",
        ),
    ],
)
def test_empty_official_user_snapshots_use_observed_time(monkeypatch, channel, data, subtype):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    data["user"] = source
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 7777)

    event = normalize_ws_message(source, {"channel": channel, "data": data})

    assert event.exchange_ts_ms == 7777
    assert event.observed_ts_ms == 7777
    assert event.payload["event_subtype"] == subtype
    assert event.payload["event_count"] == 0
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_user_twap_slice_fills_trigger_fill_validation():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userTwapSliceFills",
            "data": {
                "isSnapshot": False,
                "user": source,
                "twapSliceFills": [
                    {
                        "twapId": 42,
                        "fill": {
                            "coin": "BTC",
                            "side": "B",
                            "dir": "Open Long",
                            "time": 1234,
                            "oid": 55,
                            "hash": "0xFILL",
                            "tid": 99,
                        },
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.FILL
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "twap_slice_fill"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["oids"] == ["55"]
    assert event.payload["hashes"] == ["0xfill"]
    assert event.payload["twap_ids"] == ["42"]


def test_normalize_user_fundings_channel_is_snapshot_event():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userFundings",
            "data": {
                "isSnapshot": True,
                "user": source,
                "fundings": [
                    {
                        "time": 1234,
                        "coin": "ETH",
                        "usdc": "-1.25",
                        "szi": "0.2",
                        "fundingRate": "0.0001",
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "funding_snapshot"
    assert event.payload["coins"] == ["ETH"]
    assert event.payload["event_count"] == 1


def test_normalize_non_funding_ledger_liquidation_is_position_event():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "isSnapshot": False,
                "user": source,
                "nonFundingLedgerUpdates": [
                    {
                        "time": 1234,
                        "hash": "0xLEDGER",
                        "delta": {
                            "type": "liquidation",
                            "accountValue": "10",
                            "liquidatedPositions": [{"coin": "SOL", "szi": "-2"}],
                        },
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.POSITION
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "ledger_update:liquidation"
    assert event.payload["coins"] == ["SOL"]
    assert event.payload["hashes"] == ["0xledger"]
    assert event.payload["ledger_types"] == ["liquidation"]


def test_normalize_non_funding_ledger_deposit_is_snapshot_event():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "user": source,
                "nonFundingLedgerUpdates": [
                    {
                        "time": 1234,
                        "hash": "0xLEDGER",
                        "delta": {"type": "deposit", "usdc": "50"},
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.payload["event_subtype"] == "ledger_update:deposit"
    assert event.payload["ledger_types"] == ["deposit"]


def test_non_funding_ledger_allows_nested_counterparty_user():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    counterparty = "0x1111111111111111111111111111111111111111"
    event = normalize_ws_message(
        source,
        {
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "user": source,
                "nonFundingLedgerUpdates": [
                    {
                        "time": 1234,
                        "hash": "0xLEDGER",
                        "delta": {
                            "type": "internalTransfer",
                            "usdc": "50",
                            "user": counterparty,
                            "destination": source,
                        },
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.payload["event_subtype"] == "ledger_update:internaltransfer"


def test_normalize_user_twap_history_is_snapshot_event():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userTwapHistory",
            "data": {
                "isSnapshot": False,
                "user": source,
                "history": [
                    {
                        "time": 1234,
                        "state": {
                            "coin": "BTC",
                            "user": source,
                            "side": "B",
                            "sz": "0.1",
                            "executedSz": "0",
                            "timestamp": 9999,
                        },
                        "status": {"status": "activated", "description": "started"},
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "twap_history:activated"
    assert event.payload["coins"] == ["BTC"]
    assert event.payload["statuses"] == ["activated"]


def test_normalize_user_twap_history_requires_history_time_not_state_timestamp():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"

    with pytest.raises(SourceWebsocketMessageError, match="missing timestamp"):
        normalize_ws_message(
            source,
            {
                "channel": "userTwapHistory",
                "data": {
                    "user": source,
                    "history": [
                        {
                            "state": {
                                "coin": "BTC",
                                "user": source,
                                "side": "B",
                                "sz": "0.1",
                                "executedSz": "0",
                                "timestamp": 9999,
                            },
                            "status": {"status": "finished", "description": "done"},
                        }
                    ],
                },
            },
        )


def test_normalize_user_twap_history_finished_status_is_extracted():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userTwapHistory",
            "data": {
                "user": source,
                "history": [
                    {
                        "time": 1234,
                        "state": {
                            "coin": "ETH",
                            "user": source,
                            "side": "A",
                            "sz": "0.2",
                            "executedSz": "0.2",
                            "timestamp": 1200,
                        },
                        "status": {"status": "finished", "description": "done"},
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "twap_history:finished"
    assert event.payload["statuses"] == ["finished"]
    assert event.payload["coins"] == ["ETH"]


def test_normalize_account_notification_accepts_string_payload(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 8888)

    event = normalize_ws_message(
        source,
        {"channel": "notification", "data": "risk update"},
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 8888
    assert event.payload["event_subtype"] == "account_notification"
    assert event.payload["event_count"] == 1
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_web_data3_treats_server_time_as_observed_snapshot_time(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 8765)

    event = normalize_ws_message(
        source,
        {
            "channel": "webData3",
            "data": {
                "userState": {
                    "user": source,
                    "serverTime": 4321,
                    "agentAddress": "0x1111111111111111111111111111111111111111",
                },
                "perpDexStates": [{"totalVaultEquity": 0}, {"totalVaultEquity": 1}],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 8765
    assert event.payload["event_subtype"] == "web_data_snapshot"
    assert event.payload["event_count"] == 2
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_open_orders_snapshot_ignores_nested_order_timestamp(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 9876)

    event = normalize_ws_message(
        source,
        {
            "channel": "openOrders",
            "data": {
                "user": source,
                "orders": [
                    {
                        "coin": "BTC",
                        "side": "B",
                        "limitPx": "50000",
                        "sz": "0.1",
                        "oid": 123,
                        "timestamp": 1111,
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.OPEN_ORDER
    assert event.exchange_ts_ms == 9876
    assert event.payload["event_subtype"] == "open_order_snapshot"
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_spot_state_is_non_exposure_snapshot(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 6789)

    event = normalize_ws_message(
        source,
        {
            "channel": "spotState",
            "data": {
                "user": source,
                "spotState": {
                    "balances": [
                        {"coin": "USDC", "total": "100", "hold": "0"},
                        {"coin": "HYPE", "total": "2", "hold": "0"},
                    ]
                },
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 6789
    assert event.payload["event_subtype"] == "spot_state_snapshot"
    assert event.payload["event_count"] == 2
    assert event.payload["coins"] == ["HYPE", "USDC"]


def test_normalize_all_dexs_state_journals_dex_and_position_count(monkeypatch):
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 2468)

    event = normalize_ws_message(
        source,
        {
            "channel": "allDexsClearinghouseState",
            "data": {
                "user": source,
                "clearinghouseStates": {
                    "": {
                        "assetPositions": [
                            {"position": {"coin": "BTC", "szi": "0.1", "entryPx": "50000"}}
                        ]
                    },
                    "testdex": {
                        "assetPositions": [
                            {"position": {"coin": "ETH", "szi": "-0.2", "entryPx": "3000"}}
                        ]
                    },
                },
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 2468
    assert event.payload["event_subtype"] == "all_dexs_position_snapshot"
    assert event.payload["event_count"] == 2
    assert event.payload["coins"] == ["BTC", "ETH"]
    assert event.payload["dexs"] == ["testdex"]


def test_normalize_snapshots_without_exchange_time_use_observed_time(monkeypatch):
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 9999)
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "clearinghouseState",
            "data": {
                "assetPositions": [{"position": {"coin": "BTC", "szi": "0.1", "entryPx": "50000"}}]
            },
        },
    )
    assert event.event_type == SourceEventType.POSITION
    assert event.exchange_ts_ms == 9999
    assert event.observed_ts_ms == 9999
    assert event.payload["event_subtype"] == "position_snapshot"
    assert event.payload["timestamp_source"] == "observed"
    assert event.payload["coins"] == ["BTC"]


def test_normalize_liquidation_event_is_position_event_with_observed_time(monkeypatch):
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: 5555)
    event = normalize_ws_message(
        "0xcf7c4feb434751146a48b895e96caeb15838f92c",
        {
            "channel": "userEvents",
            "data": {
                "liquidation": {
                    "lid": 1,
                    "liquidator": "0x1111111111111111111111111111111111111111",
                    "liquidated_user": "0xcf7c4feb434751146a48b895e96caeb15838f92c",
                    "liquidated_ntl_pos": "100",
                    "liquidated_account_value": "10",
                }
            },
        },
    )
    assert event.event_type == SourceEventType.POSITION
    assert event.exchange_ts_ms == 5555
    assert event.payload["event_subtype"] == "liquidation"
    assert event.payload["timestamp_source"] == "observed"


def test_normalize_liquidation_event_rejects_liquidated_user_mismatch():
    with pytest.raises(SourceWebsocketMessageError, match="user mismatch"):
        normalize_ws_message(
            "0xcf7c4feb434751146a48b895e96caeb15838f92c",
            {
                "channel": "userEvents",
                "data": {
                    "liquidation": {
                        "lid": 1,
                        "liquidator": "0x2222222222222222222222222222222222222222",
                        "liquidated_user": "0x1111111111111111111111111111111111111111",
                        "liquidated_ntl_pos": "100",
                        "liquidated_account_value": "10",
                    }
                },
            },
        )


def test_normalize_account_class_transfer_ledger_extracts_type():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {
            "channel": "userNonFundingLedgerUpdates",
            "data": {
                "user": source,
                "nonFundingLedgerUpdates": [
                    {
                        "time": 1234,
                        "hash": "0xLEDGER",
                        "delta": {
                            "type": "accountClassTransfer",
                            "usdc": "50",
                            "toPerp": True,
                        },
                    }
                ],
            },
        },
    )

    assert event.event_type == SourceEventType.SNAPSHOT
    assert event.exchange_ts_ms == 1234
    assert event.payload["event_subtype"] == "ledger_update:accountclasstransfer"
    assert event.payload["ledger_types"] == ["accountclasstransfer"]


def test_parse_open_orders_normalizes_hyperliquid_side_literals():
    orders = parse_open_orders(
        [
            {
                "coin": "BTC",
                "side": "B",
                "sz": "0.01",
                "limitPx": "50000",
                "cloid": "0x" + "a" * 32,
            },
            {
                "coin": "ETH",
                "side": "A",
                "sz": "0.2",
                "limitPx": "3000",
                "cloid": "0x" + "b" * 32,
            },
            {
                "coin": "SOL",
                "isBuy": True,
                "sz": "1",
                "px": "150",
                "oid": 123,
                "reduceOnly": True,
            },
            {"coin": "HYPE", "dir": "Close Long", "origSz": "2", "oid": 124},
            {"coin": "XRP", "dir": "Close Short", "origSz": "3", "oid": 125},
        ],
        observed_ms=1234,
    )
    assert [(order.coin, order.side, order.size, order.price) for order in orders] == [
        ("BTC", "buy", Decimal("0.01"), Decimal("50000")),
        ("ETH", "sell", Decimal("0.2"), Decimal("3000")),
        ("SOL", "buy", Decimal("1"), Decimal("150")),
        ("HYPE", "sell", Decimal("2"), None),
        ("XRP", "buy", Decimal("3"), None),
    ]
    assert orders[2].reduce_only is True


def test_parse_clearinghouse_positions_accepts_official_shape_and_empty_list():
    positions = parse_clearinghouse_positions(
        {
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "0.01",
                        "entryPx": "50000",
                        "leverage": {"type": "cross", "value": 3},
                    }
                },
                {
                    "position": {
                        "coin": "ETH",
                        "szi": "0",
                        "entryPx": None,
                        "leverage": {"type": "cross", "value": 2},
                    }
                },
            ]
        },
        observed_ms=1234,
    )

    assert positions == {
        "BTC": positions["BTC"],
    }
    assert positions["BTC"].size == Decimal("0.01")
    assert positions["BTC"].entry_px == Decimal("50000")
    assert positions["BTC"].leverage == 3
    assert positions["BTC"].updated_ms == 1234
    assert parse_clearinghouse_positions({"assetPositions": []}) == {}


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({}, "missing assetPositions"),
        ({"assetPositions": {}}, "assetPositions must be a list"),
        ({"assetPositions": [None]}, r"assetPositions\[0\] must be an object"),
        (
            {"assetPositions": [{"position": None}]},
            r"assetPositions\[0\]\.position must be an object",
        ),
        (
            {"assetPositions": [{"position": {"szi": "1"}}]},
            r"assetPositions\[0\] is missing a valid coin",
        ),
        (
            {"assetPositions": [{"position": {"coin": "BTC"}}]},
            r"assetPositions\[0\] is missing szi",
        ),
        (
            {"assetPositions": [{"position": {"coin": "BTC", "szi": "NaN"}}]},
            "must be finite",
        ),
        (
            {
                "assetPositions": [
                    {"position": {"coin": "BTC", "szi": "1"}},
                    {"position": {"coin": "btc", "szi": "2"}},
                ]
            },
            "duplicate position coin BTC",
        ),
    ],
)
def test_parse_clearinghouse_positions_rejects_untrusted_shapes(payload, error):
    with pytest.raises(ValueError, match=error):
        parse_clearinghouse_positions(payload)


@pytest.mark.parametrize(
    "leverage",
    [True, 1.5, 1.9, "NaN", "Infinity", 0, -1, {"value": True}, {"value": "1.5"}],
)
def test_parse_clearinghouse_positions_rejects_nonintegral_leverage(leverage):
    payload = {
        "assetPositions": [
            {
                "position": {
                    "coin": "BTC",
                    "szi": "1",
                    "entryPx": "50000",
                    "leverage": leverage,
                }
            }
        ]
    }

    with pytest.raises(ValueError, match="leverage must be a positive integer"):
        parse_clearinghouse_positions(payload)


def test_parse_open_orders_accepts_list_and_official_wrapper():
    row = {
        "coin": "BTC",
        "side": "B",
        "sz": "0.01",
        "limitPx": "50000",
        "oid": "123",
    }

    assert parse_open_orders([row])[0].oid == 123
    assert parse_open_orders({"orders": [row]})[0].side == "buy"
    assert parse_open_orders([]) == []
    assert parse_open_orders({"orders": []}) == []


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({}, "missing orders"),
        ({"orders": {}}, "orders must be a list"),
        ([None], "open order 0 must be an object"),
        ([{"side": "B", "sz": "1", "oid": 1}], "missing a valid coin"),
        ([{"coin": "BTC", "side": "?", "sz": "1", "oid": 1}], "invalid side"),
        (
            [{"coin": "BTC", "side": "A", "isBuy": True, "sz": "1", "oid": 1}],
            "conflicting side signals",
        ),
        (
            [{"coin": "BTC", "side": "B", "isBuy": 1, "sz": "1", "oid": 1}],
            "isBuy must be boolean",
        ),
        ([{"coin": "BTC", "side": "B", "oid": 1}], "missing sz/origSz"),
        ([{"coin": "BTC", "side": "B", "sz": "0", "oid": 1}], "size must be positive"),
        ([{"coin": "BTC", "side": "B", "sz": "NaN", "oid": 1}], "must be finite"),
        ([{"coin": "BTC", "side": "B", "sz": "1"}], "missing oid and cloid"),
        ([{"coin": "BTC", "side": "B", "sz": "1", "oid": "bad"}], "invalid oid"),
        ([{"coin": "BTC", "side": "B", "sz": "1", "cloid": "0xabc"}], "invalid"),
        (
            [
                {"coin": "BTC", "side": "B", "sz": "1", "oid": 1},
                {"coin": "ETH", "side": "A", "sz": "1", "oid": 1},
            ],
            "duplicate oid 1",
        ),
    ],
)
def test_parse_open_orders_rejects_untrusted_shapes(payload, error):
    with pytest.raises(ValueError, match=error):
        parse_open_orders(payload)


def test_source_reconcile_rejects_malformed_flat_truth_without_journaling(store):
    info = FakeInfoClient()
    info.state = {}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
    )

    with pytest.raises(ValueError, match="missing assetPositions"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


@pytest.mark.parametrize("user_abstraction", ["unifiedAccount", "default"])
def test_source_reconcile_uses_spot_usdc_for_unified_account_value(store, user_abstraction):
    info = FakeInfoClient()
    info.user_abstraction = user_abstraction
    info.state["marginSummary"] = {"accountValue": "0"}
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        unified_state_provider=_source_unified_snapshot,
    )

    snapshot = observer.reconcile_once()

    assert snapshot.raw_state["accountMode"] == "unified"
    assert snapshot.raw_state["accountValue"] == Decimal("75.25")
    row = store.recent("source_events", 1)[0]
    payload = json.loads(row["payload_json"])["payload"]
    assert payload["account_mode"] == "unified"
    assert payload["account_value"] == "75.25"
    assert payload["unified_aggregate"]["dex_count"] == 2


def _source_unified_snapshot(*, non_default_size: str = "0") -> UnifiedAccountSnapshot:
    observed = now_ms()
    summary = {
        "accountValue": "0",
        "totalMarginUsed": "0",
        "totalNtlPos": "0",
        "totalRawUsd": "0",
    }
    default = {
        "assetPositions": [
            {
                "position": {
                    "coin": "BTC",
                    "szi": "1.0",
                    "entryPx": "50000",
                    "leverage": {"type": "cross", "value": 2},
                }
            }
        ],
        "crossMaintenanceMarginUsed": "0",
        "crossMarginSummary": summary,
        "marginSummary": summary,
        "time": observed,
        "withdrawable": "0",
    }
    other = {
        **default,
        "assetPositions": (
            []
            if non_default_size == "0"
            else [{"position": {"coin": "xyz:FOO", "szi": non_default_size}}]
        ),
    }
    return UnifiedAccountSnapshot(
        account="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        clearinghouse_states={"": default, "xyz": other},
        observed_ms=observed,
        received_ms=observed,
    )


def test_source_reconcile_blocks_unified_non_default_dex_activity(store):
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        unified_state_provider=lambda: _source_unified_snapshot(non_default_size="1"),
    )

    with pytest.raises(ValueError, match="non-default DEX activity"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


def test_source_reconcile_explicit_default_scope_reports_excluded_dex_activity(store):
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC", "ETH"),
        source_dex_scope=SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY,
        unified_state_provider=lambda: _source_unified_snapshot(non_default_size="1"),
    )

    snapshot = observer.reconcile_once()

    assert set(snapshot.positions) == {"BTC"}
    assert "xyz:FOO" not in snapshot.positions
    aggregate = snapshot.raw_state["unifiedAggregate"]
    assert aggregate == {
        "observed_ms": aggregate["observed_ms"],
        "received_ms": aggregate["received_ms"],
        "dex_count": 2,
        "active_non_default_dexes": ["xyz"],
        "source_dex_scope": "default_only_account_equity",
        "positions_scope": "default_perp_dex",
        "account_value_basis": "total_unified_spot_usdc",
        "fidelity": "reduced_non_default_positions_excluded",
    }
    payload = json.loads(store.recent("source_events", 1)[0]["payload_json"])["payload"]
    assert payload["unified_aggregate"] == aggregate
    assert payload["account_value"] == "75.25"


def test_source_reconcile_default_scope_rejects_non_default_allowed_symbol(store):
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("xyz:FOO",),
        source_dex_scope=SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY,
        unified_state_provider=lambda: _source_unified_snapshot(non_default_size="1"),
    )

    with pytest.raises(ValueError, match="cannot select non-default DEX symbols"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


class _MultiDexSourceInfo(FakeInfoClient):
    def __init__(self):
        super().__init__()
        self.open_orders = [
            {"coin": "BTC", "side": "B", "sz": "0.1", "limitPx": "50000", "oid": 101}
        ]
        self.dex_open_orders = {
            "xyz": [{"coin": "AAPL", "side": "A", "sz": "2", "limitPx": "220", "oid": 202}]
        }
        self.dex_mids = {"xyz": {"AAPL": "220", "SPCX": "145"}}

    def info(self, payload):
        request_type = payload["type"]
        dex = payload.get("dex", "")
        if request_type == "openOrders" and dex:
            self.calls.append(payload)
            return self.dex_open_orders[dex]
        if request_type == "allMids" and dex:
            self.calls.append(payload)
            return self.dex_mids[dex]
        return super().info(payload)


def _mixed_dex_source_snapshot(*, xyz_positions=None) -> UnifiedAccountSnapshot:
    snapshot = _source_unified_snapshot()
    snapshot.clearinghouse_states["xyz"]["assetPositions"] = xyz_positions or [
        {
            "position": {
                "coin": "AAPL",
                "szi": "-2",
                "entryPx": "225",
                "leverage": {"type": "cross", "value": 20},
            }
        }
    ]
    return snapshot


def test_source_reconcile_all_configured_markets_combines_dex_truth(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC", "xyz:aapl"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
    )

    snapshot = observer.reconcile_once()

    assert set(snapshot.positions) == {"BTC", "xyz:AAPL"}
    assert snapshot.positions["xyz:AAPL"].size == Decimal("-2")
    assert {(order.coin, order.oid) for order in snapshot.open_orders} == {
        ("BTC", 101),
        ("xyz:AAPL", 202),
    }
    assert snapshot.mids["BTC"] == Decimal("50000")
    assert snapshot.mids["xyz:AAPL"] == Decimal("220")
    assert snapshot.raw_state["accountValue"] == Decimal("75.25")
    assert {tuple(sorted(call.items())) for call in info.calls if call["type"] == "allMids"} == {
        (("type", "allMids"),),
        (("dex", "xyz"), ("type", "allMids")),
    }
    assert any(
        call == {"type": "openOrders", "user": observer.source_wallet, "dex": "xyz"}
        for call in info.calls
    )
    aggregate = snapshot.raw_state["unifiedAggregate"]
    assert aggregate["dexes"] == ["", "xyz"]
    assert aggregate["market_data_dexes"] == ["", "xyz"]
    assert aggregate["source_dex_scope"] == "all_configured_markets"
    assert aggregate["positions_scope"] == "all_perp_dexes"
    assert aggregate["account_value_basis"] == "total_unified_spot_usdc"
    assert aggregate["fidelity"] == "full_all_dex_positions_configured_market_data"
    payload = json.loads(store.recent("source_events", 1)[0]["payload_json"])["payload"]
    assert payload["unified_aggregate"] == aggregate
    assert payload["account_value"] == "75.25"


def test_source_reconcile_uses_shared_execution_mids_without_duplicate_rest_load(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    provider_calls: list[str] = []

    def shared_mids() -> dict[str, Decimal]:
        provider_calls.append("load")
        return {"BTC": Decimal("50001"), "xyz:AAPL": Decimal("221")}

    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC", "xyz:AAPL"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
        market_mids_provider=shared_mids,
    )

    snapshot = observer.reconcile_once()

    assert provider_calls == ["load"]
    assert snapshot.mids == {
        "BTC": Decimal("50001"),
        "xyz:AAPL": Decimal("221"),
    }
    assert not any(call["type"] == "allMids" for call in info.calls)
    assert snapshot.raw_state["allMids"] == {
        "shared_execution_cache": {"BTC": Decimal("50001"), "xyz:AAPL": Decimal("221")}
    }


def test_source_open_order_cache_routes_hip3_order_updates_to_exact_dex(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC", "xyz:AAPL"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
    )
    first = observer.reconcile_once()
    observer.record_ws_message_event(
        {
            "channel": "orderUpdates",
            "data": [
                {
                    "order": {
                        "coin": "xyz:AAPL",
                        "side": "A",
                        "sz": "0",
                        "origSz": "2",
                        "limitPx": "220",
                        "oid": 202,
                    },
                    "status": "filled",
                    "statusTimestamp": now_ms(),
                }
            ],
        }
    )
    second = observer.reconcile_once()

    assert {order.oid for order in first.open_orders} == {101, 202}
    assert {order.oid for order in second.open_orders} == {101}
    assert (
        len(
            [
                call
                for call in info.calls
                if call["type"] == "openOrders" and call.get("dex") == "xyz"
            ]
        )
        == 1
    )
    assert observer.open_order_cache_status()["dexes"]["xyz"]["order_count"] == 0


def test_source_reconcile_skips_configured_dex_absent_from_source_network(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("BTC", "idx:IDX0"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
    )

    snapshot = observer.reconcile_once()

    aggregate = snapshot.raw_state["unifiedAggregate"]
    assert aggregate["market_data_dexes"] == [""]
    assert aggregate["unavailable_configured_dexes"] == ["idx"]
    assert not any(call.get("dex") == "idx" for call in info.calls)


def test_source_reconcile_all_configured_markets_requires_unified_source(store):
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=FakeInfoClient(),
        store=store,
        active_asset_symbols=("BTC", "xyz:AAPL"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
    )

    with pytest.raises(ValueError, match="requires a Unified source account"):
        observer.reconcile_once()


def test_source_reconcile_all_configured_markets_rejects_conflicting_position_dex(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("xyz:AAPL",),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=lambda: _mixed_dex_source_snapshot(
            xyz_positions=[{"position": {"coin": "other:AAPL", "szi": "1"}}]
        ),
    )

    with pytest.raises(ValueError, match="conflicts with response DEX"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


def test_source_reconcile_all_configured_markets_rejects_cross_dex_duplicate_oid(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    info.dex_open_orders["xyz"] = [
        {"coin": "AAPL", "side": "A", "sz": "2", "limitPx": "220", "oid": 101}
    ]
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("xyz:AAPL",),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
    )

    with pytest.raises(ValueError, match="duplicate oid 101"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


def test_source_reconcile_all_configured_markets_rejects_duplicate_canonical_mid(store):
    info = _MultiDexSourceInfo()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "75.25", "hold": "0"}]}
    info.dex_mids["xyz"] = {"AAPL": "220", "xyz:aapl": "221"}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        active_asset_symbols=("xyz:AAPL",),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        unified_state_provider=_mixed_dex_source_snapshot,
    )

    with pytest.raises(ValueError, match="duplicate market xyz:AAPL"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


@pytest.mark.parametrize(
    ("abstraction", "dex_abstraction", "error"),
    [
        ("portfolioMargin", False, "portfoliomargin"),
        ("unifiedAccount", True, "DEX abstraction"),
    ],
)
def test_source_reconcile_rejects_unsafe_abstraction_modes(
    store, abstraction, dex_abstraction, error
):
    info = FakeInfoClient()
    info.user_abstraction = abstraction
    info.user_dex_abstraction = dex_abstraction
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "50", "hold": "0"}]}
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
    )

    with pytest.raises(ValueError, match=error):
        observer.reconcile_once()

    assert store.count("source_events") == 0


class _FollowerTruthInfo:
    def __init__(self, *, state, orders):
        self.state = state
        self.orders = orders

    def user_state(self, _account):
        return self.state

    def open_orders(self, _account):
        return self.orders


@pytest.mark.parametrize(
    ("state", "orders", "error"),
    [
        ({}, [], "missing assetPositions"),
        ({"assetPositions": []}, {}, "missing orders"),
    ],
)
def test_follower_reconcile_rejects_malformed_flat_truth(base_config, state, orders, error):
    adapter = HyperliquidExecutionAdapter(base_config)
    adapter._info = _FollowerTruthInfo(state=state, orders=orders)

    with pytest.raises(ValueError, match=error):
        adapter.reconcile()


def test_reconcile_records_stable_state_keys_for_identical_source_state(store, monkeypatch):
    observed_ticks = iter([1_000_000, 1_000_250])
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: next(observed_ticks))
    observer, _ = make_observer(store)

    first = observer.reconcile_once()
    second = observer.reconcile_once()

    assert first.state_key == second.state_key
    assert first.planning_key == second.planning_key
    assert store.count("source_events") == 2
    rows = store.recent("source_events", 2)
    assert rows[0]["idempotency_key"] != rows[1]["idempotency_key"]
    payloads = [json.loads(row["payload_json"])["payload"] for row in rows]
    assert {payload["state_key"] for payload in payloads} == {first.state_key}
    assert len({payload["planning_exposure_key"] for payload in payloads}) == 1
    assert {payload["planning_key"] for payload in payloads} == {first.planning_key}
    identity_calls = [
        call
        for call in observer.info_client.calls
        if call["type"] in {"userAbstraction", "userDexAbstraction"}
    ]
    assert [call["type"] for call in identity_calls] == [
        "userAbstraction",
        "userDexAbstraction",
    ]


def test_reconcile_preserves_planning_key_across_interleaved_websocket_event(store, monkeypatch):
    observed_ticks = iter([1_000_000, 1_000_250])
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: next(observed_ticks))
    observer, _ = make_observer(store)

    first = observer.reconcile_once()
    observer.record_source_event(
        SourceEvent(
            idempotency_key="interleaved-source-fill",
            event_type=SourceEventType.FILL,
            source_wallet=observer.source_wallet,
            exchange_ts_ms=1_000_100,
            observed_ts_ms=1_000_100,
            payload={"event_subtype": "fill", "event_count": 1},
        )
    )
    second = observer.reconcile_once()

    assert first.planning_key == second.planning_key
    rows = store.recent_source_events(source_wallet=observer.source_wallet, limit=3)
    assert rows[0]["event_type"] == SourceEventType.RECONCILE.value
    assert rows[1]["event_type"] == SourceEventType.FILL.value


def test_reconcile_rejects_nonfinite_external_market_data(store):
    info = FakeInfoClient()
    info.mids["BTC"] = "NaN"
    safe = SafeModeController(store)
    observer = SourceObserver(
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        info_client=info,
        store=store,
        shield=ConsistencyShield(safe),
    )

    with pytest.raises(ValueError, match="must be finite"):
        observer.reconcile_once()

    assert store.count("source_events") == 0


def test_normalize_ws_message_accepts_matching_explicit_user():
    source = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
    event = normalize_ws_message(
        source,
        {"channel": "userFills", "data": [{"time": 1234, "coin": "BTC", "user": source.upper()}]},
    )
    assert event.source_wallet == source
    assert event.exchange_ts_ms == 1234


def test_normalize_ws_message_rejects_explicit_user_mismatch():
    with pytest.raises(SourceWebsocketMessageError, match="user mismatch"):
        normalize_ws_message(
            "0xcf7c4feb434751146a48b895e96caeb15838f92c",
            {
                "channel": "userFills",
                "data": [
                    {
                        "time": 1234,
                        "coin": "BTC",
                        "account": "0x1111111111111111111111111111111111111111",
                    }
                ],
            },
        )


def test_normalize_ws_message_rejects_unknown_or_timestampless_payloads():
    with pytest.raises(SourceWebsocketMessageError, match="unsupported"):
        normalize_ws_message(
            "0xcf7c4feb434751146a48b895e96caeb15838f92c",
            {"channel": "mystery", "data": {"time": 1234}},
        )
    with pytest.raises(SourceWebsocketMessageError, match="missing timestamp"):
        normalize_ws_message(
            "0xcf7c4feb434751146a48b895e96caeb15838f92c",
            {"channel": "userFills", "data": [{"coin": "BTC"}]},
        )


def test_record_ws_message_trips_gap_without_journaling_unknown_message(store):
    observer, safe = make_observer(store)
    with pytest.raises(SourceWebsocketMessageError):
        observer.record_ws_message({"channel": "mystery", "data": {"time": 1234}})
    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert store.count("source_events") == 0


def test_record_ws_message_trips_gap_without_journaling_user_mismatch(store):
    observer, safe = make_observer(store)
    with pytest.raises(SourceWebsocketMessageError):
        observer.record_ws_message(
            {
                "channel": "userFills",
                "data": {
                    "time": 1234,
                    "userAddress": "0x1111111111111111111111111111111111111111",
                },
            }
        )
    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "user mismatch" in safe.detail
    assert store.count("source_events") == 0


def test_record_ws_message_rejects_out_of_order_without_journaling(store):
    observer, safe = make_observer(store)

    first, inserted = observer.record_ws_message_event(
        {"channel": "userFills", "data": [{"time": 2000, "coin": "BTC", "oid": 1}]}
    )

    assert inserted is True
    assert first.exchange_ts_ms == 2000
    with pytest.raises(SourceWebsocketMessageError, match="rejected by consistency shield"):
        observer.record_ws_message_event(
            {"channel": "userFills", "data": [{"time": 1000, "coin": "BTC", "oid": 2}]}
        )
    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT
    assert store.count("source_events") == 1


def test_observe_websocket_does_not_callback_out_of_order_event(monkeypatch, store):
    observer, safe = make_observer(store)
    callbacks: list[tuple[str, bool]] = []

    class FakeWebsocket:
        def __init__(self):
            self.recv_calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            self.recv_calls += 1
            timestamp = 2000 if self.recv_calls == 1 else 1000
            return json.dumps(
                {
                    "channel": "userFills",
                    "data": [{"time": timestamp, "coin": "BTC", "oid": self.recv_calls}],
                }
            )

    def on_event(event, inserted):
        callbacks.append((event.idempotency_key, inserted))

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    with pytest.raises(SourceWebsocketMessageError, match="rejected by consistency shield"):
        asyncio.run(observer.observe_websocket(stop_after_messages=2, on_event=on_event))

    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT
    assert store.count("source_events") == 1
    assert len(callbacks) == 1


def test_record_ws_message_event_returns_normalized_event_and_inserted_flag(store):
    observer, safe = make_observer(store)

    event, inserted = observer.record_ws_message_event(
        {"channel": "userFills", "data": [{"time": 1234, "coin": "BTC"}]}
    )
    duplicate, inserted_again = observer.record_ws_message_event(
        {"channel": "userFills", "data": [{"time": 1234, "coin": "BTC"}]}
    )

    assert inserted is True
    assert inserted_again is False
    assert duplicate.idempotency_key == event.idempotency_key
    assert event.payload["event_subtype"] == "fill"
    assert store.count("source_events") == 1
    assert not safe.enabled


def test_backfill_fills_by_time_journals_missing_fills_and_dedupes(store):
    observer, safe = make_observer(store)
    fake = observer.info_client
    fake.fills = [
        {"time": 1000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 10},
        {"time": 1001, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 11},
    ]

    first = observer.backfill_fills_by_time(start_time_ms=900, end_time_ms=2000)
    second = observer.backfill_fills_by_time(start_time_ms=900, end_time_ms=2000)

    assert first.inserted == 2
    assert first.duplicates == 0
    assert second.inserted == 0
    assert second.duplicates == 2
    assert store.count("source_events") == 2
    assert not safe.enabled
    assert fake.calls[-1]["type"] == "userFillsByTime"
    assert fake.calls[-1]["aggregateByTime"] is False


def test_backfill_fills_by_time_allows_late_missing_overlap_fill(store):
    observer, safe = make_observer(store)
    fake = observer.info_client
    fake.fills = [{"time": 2000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 10}]
    first = observer.backfill_fills_by_time(start_time_ms=1750, end_time_ms=2100)

    fake.fills.append({"time": 1800, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 11})
    second = observer.backfill_fills_by_time(start_time_ms=1750, end_time_ms=2100)

    assert first.inserted == 1
    assert second.inserted == 1
    assert second.duplicates == 1
    assert store.count("source_events") == 2
    assert not safe.enabled


def test_backfill_twap_slice_fills_by_time_journals_missing_fills_and_dedupes(store):
    observer, safe = make_observer(store)
    fake = observer.info_client
    fake.twap_slice_fills = [
        {
            "twapId": 42,
            "fill": {"time": 1000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 10},
        },
        {
            "twapId": 43,
            "fill": {"time": 1001, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 11},
        },
    ]

    first = observer.backfill_twap_slice_fills_by_time(start_time_ms=900, end_time_ms=2000)
    second = observer.backfill_twap_slice_fills_by_time(start_time_ms=900, end_time_ms=2000)

    assert first.inserted == 2
    assert first.duplicates == 0
    assert second.inserted == 0
    assert second.duplicates == 2
    assert store.count("source_events") == 2
    assert not safe.enabled
    assert fake.calls[-1]["type"] == "userTwapSliceFillsByTime"


def test_backfill_twap_slice_allows_late_missing_overlap_fill(store):
    observer, safe = make_observer(store)
    fake = observer.info_client
    fake.twap_slice_fills = [
        {
            "twapId": 42,
            "fill": {"time": 2000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 10},
        }
    ]
    first = observer.backfill_twap_slice_fills_by_time(start_time_ms=1750, end_time_ms=2100)

    fake.twap_slice_fills.append(
        {
            "twapId": 43,
            "fill": {"time": 1800, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 11},
        }
    )
    second = observer.backfill_twap_slice_fills_by_time(start_time_ms=1750, end_time_ms=2100)

    assert first.inserted == 1
    assert second.inserted == 1
    assert second.duplicates == 1
    assert store.count("source_events") == 2
    assert not safe.enabled


def test_backfill_fills_by_time_short_page_at_max_pages_is_complete(store):
    observer, safe = make_observer(store)
    observer.info_client.fills = [
        {"time": 1000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 10}
    ]

    report = observer.backfill_fills_by_time(
        start_time_ms=900,
        end_time_ms=2000,
        max_pages=1,
    )

    assert report.inserted == 1
    assert report.warnings == []
    assert not safe.enabled


def test_backfill_fills_by_time_empty_page_at_max_pages_is_complete(store):
    observer, safe = make_observer(store)

    report = observer.backfill_fills_by_time(
        start_time_ms=900,
        end_time_ms=2000,
        max_pages=1,
    )

    assert report.fetched == 0
    assert report.warnings == []
    assert not safe.enabled


def test_backfill_fills_by_time_full_non_advancing_page_trips_gap(store):
    observer, safe = make_observer(store)
    observer.info_client.fills = [
        {"time": 1000, "coin": "BTC", "oid": idx, "hash": f"0x{idx:064x}", "tid": idx}
        for idx in range(2000)
    ]

    report = observer.backfill_fills_by_time(
        start_time_ms=1000,
        end_time_ms=2000,
        max_pages=3,
    )

    assert report.fetched == 2000
    assert report.warnings
    assert "timestamps did not advance" in report.warnings[0]
    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP


def test_backfill_fills_by_time_non_list_response_trips_gap(store):
    observer, safe = make_observer(store)

    def malformed_info(_payload):
        return {"unexpected": "shape"}

    observer.info_client.info = malformed_info

    with pytest.raises(SourceWebsocketMessageError, match="response is not a list"):
        observer.backfill_fills_by_time(start_time_ms=900, end_time_ms=2000)

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "userFillsByTime response is not a list" in safe.detail
    assert store.count("source_events") == 0


def test_backfill_fills_by_time_malformed_fill_trips_gap(store):
    observer, safe = make_observer(store)
    observer.info_client.fills = [{"coin": "BTC", "oid": 1, "hash": "0xaaa"}]

    with pytest.raises(SourceWebsocketMessageError, match="malformed fill"):
        observer.backfill_fills_by_time(start_time_ms=0, end_time_ms=2000)

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "missing timestamp" in safe.detail
    assert store.count("source_events") == 0


def test_backfill_twap_slice_non_list_response_trips_gap(store):
    observer, safe = make_observer(store)

    def malformed_info(_payload):
        return {"unexpected": "shape"}

    observer.info_client.info = malformed_info

    with pytest.raises(SourceWebsocketMessageError, match="response is not a list"):
        observer.backfill_twap_slice_fills_by_time(start_time_ms=900, end_time_ms=2000)

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "userTwapSliceFillsByTime response is not a list" in safe.detail
    assert store.count("source_events") == 0


def test_websocket_disconnect_trips_safe_mode(store):
    observer, safe = make_observer(store, ws_url="wss://127.0.0.1:1/ws")
    try:
        asyncio.run(observer.observe_websocket(stop_after_messages=1))
    except Exception:
        pass
    assert safe.reason == SafeModeReason.WEBSOCKET_DISCONNECT


def test_websocket_malformed_json_trips_gap_without_reconnect(monkeypatch, store):
    observer, safe = make_observer(store)

    class FakeWebsocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            return "{not-json"

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    with pytest.raises(SourceWebsocketMessageError, match="malformed JSON"):
        asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "malformed JSON" in safe.detail
    assert store.count("source_events") == 0


def test_websocket_non_object_json_trips_gap_without_reconnect(monkeypatch, store):
    observer, safe = make_observer(store)

    class FakeWebsocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            return "[]"

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    with pytest.raises(SourceWebsocketMessageError, match="not an object"):
        asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "not an object" in safe.detail
    assert store.count("source_events") == 0


def test_websocket_connection_banner_is_ignored(monkeypatch, store):
    observer, safe = make_observer(store)

    class FakeWebsocket:
        def __init__(self):
            self.recv_calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            self.recv_calls += 1
            if self.recv_calls == 1:
                return "Websocket connection established."
            return json.dumps(
                {
                    "channel": "userFills",
                    "data": {
                        "user": observer.source_wallet,
                        "fills": [{"time": 1234, "coin": "BTC", "oid": 1, "hash": "0xaaa"}],
                    },
                }
            )

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.NONE
    assert store.count("source_events") == 1


def test_websocket_subscription_response_is_validated_and_skipped(monkeypatch, store):
    observer, safe = make_observer(store)
    sent_payloads: list[dict] = []

    class FakeWebsocket:
        def __init__(self):
            self.recv_calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, payload):
            sent_payloads.append(json.loads(payload))

        async def recv(self):
            self.recv_calls += 1
            if self.recv_calls == 1:
                subscription = next(
                    dict(payload["subscription"])
                    for payload in sent_payloads
                    if payload["subscription"]["type"] == "twapStates"
                )
                subscription["user"] = subscription["user"].upper()
                subscription["dex"] = ""
                return json.dumps({"channel": "subscriptionResponse", "data": subscription})
            if self.recv_calls == 2:
                subscription = next(
                    dict(payload["subscription"])
                    for payload in sent_payloads
                    if payload["subscription"]["type"] == "spotState"
                )
                subscription["user"] = subscription["user"].upper()
                subscription["ignorePortfolioMargin"] = False
                return json.dumps({"channel": "subscriptionResponse", "data": subscription})
            return json.dumps(
                {
                    "channel": "userFills",
                    "data": {
                        "user": observer.source_wallet,
                        "fills": [{"time": 1234, "coin": "BTC", "oid": 1, "hash": "0xaaa"}],
                    },
                }
            )

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.NONE
    assert store.count("source_events") == 1


def test_websocket_unexpected_subscription_response_trips_gap(monkeypatch, store):
    observer, safe = make_observer(store)

    class FakeWebsocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            return json.dumps(
                {
                    "channel": "subscriptionResponse",
                    "data": {"type": "candle", "coin": "BTC", "interval": "1m"},
                }
            )

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    with pytest.raises(SourceWebsocketMessageError, match="unexpected subscription"):
        asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP
    assert "unexpected subscription" in safe.detail
    assert store.count("source_events") == 0


def test_websocket_heartbeat_timeout_trips_disconnect(monkeypatch, store):
    observer, safe = make_observer(
        store,
        websocket_idle_timeout_ms=1,
        websocket_heartbeat_timeout_ms=1,
    )

    class FakeWebsocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, _payload):
            return None

        async def recv(self):
            await asyncio.sleep(0.05)
            return json.dumps({"channel": "pong"})

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    with pytest.raises(TimeoutError, match="heartbeat response"):
        asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert safe.reason == SafeModeReason.WEBSOCKET_DISCONNECT
    assert "heartbeat response not received" in safe.detail
    assert store.count("source_events") == 0


def test_websocket_heartbeat_pong_keeps_observing(monkeypatch, store):
    observer, safe = make_observer(
        store,
        websocket_idle_timeout_ms=1,
        websocket_heartbeat_timeout_ms=50,
    )
    sent_payloads: list[dict] = []

    class FakeWebsocket:
        def __init__(self):
            self.recv_calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send(self, payload):
            sent_payloads.append(json.loads(payload))

        async def recv(self):
            self.recv_calls += 1
            if self.recv_calls == 1:
                await asyncio.sleep(0.05)
                return json.dumps({"channel": "pong"})
            if self.recv_calls == 2:
                return json.dumps({"channel": "pong"})
            return json.dumps(
                {
                    "channel": "userFills",
                    "data": {
                        "user": observer.source_wallet,
                        "fills": [{"time": 1234, "coin": "BTC", "oid": 1, "hash": "0xaaa"}],
                    },
                }
            )

    monkeypatch.setattr(
        observer_module,
        "connect_websocket_ipv6_preferred",
        lambda *_args, **_kwargs: FakeWebsocket(),
    )

    asyncio.run(observer.observe_websocket(stop_after_messages=1))

    assert any(payload == {"method": "ping"} for payload in sent_payloads)
    assert safe.reason == SafeModeReason.NONE
    assert store.count("source_events") == 1
