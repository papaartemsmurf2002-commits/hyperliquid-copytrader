from __future__ import annotations

from decimal import Decimal

from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.models import (
    FollowerIntent,
    IntentAction,
    Mode,
    OpenOrder,
    Position,
    SafeModeReason,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.persistence import SQLiteStore
from hyperliquid_copytrader.safety import ConsistencyShield, SafeModeController


def make_shield(store):
    safe = SafeModeController(store)
    return safe, ConsistencyShield(safe, rapid_flip_ms=1500)


def test_duplicate_event_is_deduped_without_safe_mode(store):
    safe, shield = make_shield(store)
    event = SourceEvent("dup", SourceEventType.FILL, exchange_ts_ms=100, observed_ts_ms=100)
    result = shield.observe_source_event(event, already_seen=True)
    assert result.ok
    assert result.reason == SafeModeReason.DUPLICATE_EVENT
    assert not safe.enabled


def test_safe_mode_transitions_append_even_with_same_reason_detail_and_timestamp(
    store, monkeypatch
):
    monkeypatch.setattr("hyperliquid_copytrader.safety.now_ms", lambda: 1234)
    safe = SafeModeController(store)

    first = safe.trip(SafeModeReason.ORDER_TIMEOUT, "same blocker")
    second = safe.trip(SafeModeReason.ORDER_TIMEOUT, "same blocker")
    clear = safe.clear("same clear")
    clear_again = safe.clear("same clear")

    assert (
        len(
            {
                first.transition_id,
                second.transition_id,
                clear.transition_id,
                clear_again.transition_id,
            }
        )
        == 4
    )
    assert store.count("safe_mode_transitions") == 4
    rows = store.recent("safe_mode_transitions", 4)
    assert [row["created_ms"] for row in rows] == [1234, 1234, 1234, 1234]


def test_safe_mode_refreshes_transition_written_by_another_controller(tmp_path):
    runner_store = SQLiteStore(tmp_path / "shared-safe-mode.sqlite3")
    operator_store = SQLiteStore(tmp_path / "shared-safe-mode.sqlite3")
    try:
        runner = SafeModeController(runner_store)
        operator = SafeModeController(operator_store)

        operator.trip(SafeModeReason.WEBSOCKET_DISCONNECT, "operator process observed disconnect")

        assert runner.enabled is False
        assert runner.refresh_from_store() is True
        assert runner.enabled is True
        assert runner.reason == SafeModeReason.WEBSOCKET_DISCONNECT
        assert runner.revision == runner_store.latest_safe_mode()["seq"]
    finally:
        operator_store.close()
        runner_store.close()


def test_safe_mode_revision_prevents_stale_resume_from_clearing_new_incident(tmp_path):
    operator_store = SQLiteStore(tmp_path / "shared-safe-mode.sqlite3")
    runner_store = SQLiteStore(tmp_path / "shared-safe-mode.sqlite3")
    try:
        operator = SafeModeController(operator_store)
        runner = SafeModeController(runner_store)
        operator.trip(SafeModeReason.WEBSOCKET_DISCONNECT, "first incident")
        runner.refresh_from_store()
        inspected_revision = runner.revision

        operator.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, "newer incident")
        cleared = runner.clear_if_revision(inspected_revision, "stale operator resume")

        assert cleared is None
        assert runner.enabled is True
        assert runner.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    finally:
        runner_store.close()
        operator_store.close()


def test_out_of_order_event_pauses(store):
    safe, shield = make_shield(store)
    shield.observe_source_event(SourceEvent("new", SourceEventType.FILL, exchange_ts_ms=200))
    result = shield.observe_source_event(
        SourceEvent("old", SourceEventType.FILL, exchange_ts_ms=100)
    )
    assert not result.ok
    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT


def test_observed_snapshot_time_does_not_order_exchange_stream(store):
    safe, shield = make_shield(store)

    snapshot = SourceEvent(
        "open-orders-snapshot",
        SourceEventType.OPEN_ORDER,
        exchange_ts_ms=2_000,
        payload={
            "event_subtype": "open_order_snapshot",
            "timestamp_source": "observed",
        },
    )
    filled_update = SourceEvent(
        "filled-order-update",
        SourceEventType.OPEN_ORDER,
        exchange_ts_ms=1_500,
        payload={
            "event_subtype": "order_update:filled",
            "timestamp_source": "exchange",
        },
    )

    assert shield.observe_source_event(snapshot).ok
    assert shield.observe_source_event(filled_update).ok
    assert not safe.enabled


def test_out_of_order_checks_are_scoped_to_source_stream(store):
    safe, shield = make_shield(store)

    funding = SourceEvent(
        "funding",
        SourceEventType.SNAPSHOT,
        exchange_ts_ms=2_000,
        payload={
            "channel": "userFundings",
            "event_subtype": "funding",
            "timestamp_source": "exchange",
        },
    )
    older_twap_history = SourceEvent(
        "twap-history",
        SourceEventType.SNAPSHOT,
        exchange_ts_ms=1_500,
        payload={
            "channel": "userTwapHistory",
            "event_subtype": "twap_history:finished",
            "timestamp_source": "exchange",
        },
    )
    older_funding = SourceEvent(
        "older-funding",
        SourceEventType.SNAPSHOT,
        exchange_ts_ms=1_000,
        payload={
            "channel": "userFundings",
            "event_subtype": "funding",
            "timestamp_source": "exchange",
        },
    )

    assert shield.observe_source_event(funding).ok
    assert shield.observe_source_event(older_twap_history).ok
    result = shield.observe_source_event(older_funding)

    assert not result.ok
    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT


def test_out_of_order_observed_snapshots_still_pause(store):
    safe, shield = make_shield(store)
    shield.observe_source_event(
        SourceEvent(
            "new-snapshot",
            SourceEventType.POSITION,
            exchange_ts_ms=2_000,
            payload={"event_subtype": "position_snapshot", "timestamp_source": "observed"},
        )
    )

    result = shield.observe_source_event(
        SourceEvent(
            "old-snapshot",
            SourceEventType.POSITION,
            exchange_ts_ms=1_500,
            payload={"event_subtype": "position_snapshot", "timestamp_source": "observed"},
        )
    )

    assert not result.ok
    assert safe.reason == SafeModeReason.OUT_OF_ORDER_EVENT


def test_startup_reconcile_recovers_when_clean_and_pauses_on_mismatch(store):
    safe, shield = make_shield(store)
    assert shield.check_startup_reconcile([]).ok
    result = shield.check_startup_reconcile(["open cloid missing"])
    assert not result.ok
    assert safe.reason == SafeModeReason.STARTUP_RECONCILE


def test_missed_event_gap_pauses(store):
    safe, shield = make_shield(store)
    result = shield.missed_event_gap("fill backfill has a timestamp gap")
    assert not result.ok
    assert safe.reason == SafeModeReason.MISSED_EVENT_GAP


def test_websocket_disconnect_pauses(store):
    safe, shield = make_shield(store)
    result = shield.websocket_disconnect()
    assert not result.ok
    assert safe.reason == SafeModeReason.WEBSOCKET_DISCONNECT


def test_rest_lag_pauses_when_threshold_exceeded(store):
    safe, shield = make_shield(store)
    assert shield.rest_lag(10, 100).ok
    result = shield.rest_lag(200, 100)
    assert not result.ok
    assert safe.reason == SafeModeReason.REST_LAG


def test_restart_mid_fill_pauses_with_pending_intents(store):
    safe, shield = make_shield(store)
    intent = FollowerIntent(
        intent_id="intent",
        cloid=deterministic_cloid("intent"),
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source",
        reason="test",
        created_ms=now_ms(),
    )
    result = shield.restart_mid_fill([intent])
    assert not result.ok
    assert safe.reason == SafeModeReason.RESTART_MID_FILL


def test_partial_fill_pauses_until_reconciled(store):
    safe, shield = make_shield(store)
    result = shield.partial_fill(Decimal("1"), Decimal("0.5"), deterministic_cloid("pf"))
    assert not result.ok
    assert safe.reason == SafeModeReason.PARTIAL_FILL


def test_cancel_reject_recovers_if_order_is_terminal(store):
    safe, shield = make_shield(store)
    assert shield.cancel_reject(deterministic_cloid("cancel"), order_status="filled").ok
    assert not safe.enabled
    result = shield.cancel_reject(deterministic_cloid("cancel-unknown"))
    assert not result.ok
    assert safe.reason == SafeModeReason.CANCEL_REJECT


def test_order_timeout_pauses(store):
    safe, shield = make_shield(store)
    result = shield.order_timeout(deterministic_cloid("timeout"))
    assert not result.ok
    assert safe.reason == SafeModeReason.ORDER_TIMEOUT


def test_source_rapid_flip_pauses(store):
    safe, shield = make_shield(store)
    assert shield.rapid_flip("BTC", Decimal("1"), now=1000).ok
    result = shield.rapid_flip("BTC", Decimal("-1"), now=1200)
    assert not result.ok
    assert safe.reason == SafeModeReason.RAPID_FLIP


def test_unsupported_symbol_pauses(store):
    safe, shield = make_shield(store)
    result = shield.unsupported_symbol("DOGE")
    assert not result.ok
    assert safe.reason == SafeModeReason.UNSUPPORTED_SYMBOL


def test_rate_limit_precision_margin_and_clock_errors_pause(store):
    safe, shield = make_shield(store)
    cases = [
        ("429 too many requests", SafeModeReason.RATE_LIMIT),
        ("Price must be divisible by tick size", SafeModeReason.PRECISION_ERROR),
        ("Order has invalid size.", SafeModeReason.PRECISION_ERROR),
        ("Insufficient margin to place order", SafeModeReason.MARGIN_ERROR),
        (
            "Order could not immediately match against any resting orders.",
            SafeModeReason.RISK_LIMIT,
        ),
        ("nonce timestamp outside accepted window", SafeModeReason.CLOCK_SKEW),
    ]
    for message, reason in cases:
        safe.clear("next case")
        result = shield.exchange_error(message)
        assert not result.ok
        assert safe.reason == reason


def test_opaque_identifier_containing_429_is_not_misclassified_as_rate_limit(store):
    safe, shield = make_shield(store)
    result = shield.exchange_error("{'cloid': '0xabc429def', 'error': 'cancel rejected'}")
    assert not result.ok
    assert safe.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_stale_source_and_follower_pause(store):
    safe, shield = make_shield(store)
    assert shield.stale_source(10, 100).ok
    result = shield.stale_source(200, 100)
    assert not result.ok
    assert safe.reason == SafeModeReason.STALE_SOURCE
    safe.clear("next")
    result = shield.stale_follower(200, 100)
    assert not result.ok
    assert safe.reason == SafeModeReason.STALE_FOLLOWER


def test_manual_exchange_side_intervention_pauses(store):
    safe, shield = make_shield(store)
    expected = {"BTC": Position("BTC", Decimal("0.01"))}
    actual = {"BTC": Position("BTC", Decimal("0.02")), "ETH": Position("ETH", Decimal("0.1"))}
    result = shield.manual_intervention(
        expected_positions=expected,
        actual_positions=actual,
        expected_open_cloids={deterministic_cloid("expected")},
        actual_open_orders=[
            OpenOrder(
                "BTC", "buy", Decimal("0.01"), Decimal("50000"), cloid=deterministic_cloid("extra")
            ),
            OpenOrder("ETH", "sell", Decimal("0.1"), Decimal("3000")),
        ],
    )
    assert not result.ok
    assert safe.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "ETH expected 0 actual 0.1" in result.detail
    assert "without cloid" in result.detail


def test_manual_exchange_side_dust_position_delta_is_allowed(store):
    safe, shield = make_shield(store)
    result = shield.manual_intervention(
        expected_positions={"BTC": Position("BTC", Decimal("0.00239"), leverage=3)},
        actual_positions={"BTC": Position("BTC", Decimal("0.00237"), leverage=3)},
        expected_open_cloids=set(),
        actual_open_orders=[],
        position_size_tolerance=Decimal("0.0001"),
    )

    assert result.ok
    assert safe.reason == SafeModeReason.NONE


def test_unexpected_below_min_notional_position_is_never_adopted_as_dust(store):
    safe, shield = make_shield(store)

    result = shield.manual_intervention(
        expected_positions={},
        actual_positions={"xyz:AAPL": Position("xyz:AAPL", Decimal("0.01"))},
        expected_open_cloids=set(),
        actual_open_orders=[],
        position_size_tolerance=Decimal("0.1"),
        position_notional_tolerance_usd=Decimal("10"),
        position_mid_prices={"xyz:AAPL": Decimal("300")},
    )

    assert result.ok is False
    assert safe.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "xyz:AAPL expected 0 actual 0.01" in result.detail


def test_manual_exchange_side_below_min_notional_delta_is_allowed(store):
    safe, shield = make_shield(store)
    result = shield.manual_intervention(
        expected_positions={
            "MORPHO": Position("MORPHO", Decimal("29.7"), entry_px=Decimal("2.09"), leverage=5)
        },
        actual_positions={
            "MORPHO": Position("MORPHO", Decimal("29.9"), entry_px=Decimal("2.06"), leverage=5)
        },
        expected_open_cloids=set(),
        actual_open_orders=[],
        position_size_tolerance=Decimal("0.0001"),
        position_notional_tolerance_usd=Decimal("10"),
        position_mid_prices={"MORPHO": Decimal("2.08")},
    )

    assert result.ok
    assert safe.reason == SafeModeReason.NONE


def test_manual_exchange_side_uses_fresh_mid_for_drift_tolerance(store):
    safe, shield = make_shield(store)
    result = shield.manual_intervention(
        expected_positions={"MORPHO": Position("MORPHO", Decimal("29.7"), entry_px=Decimal("2"))},
        actual_positions={"MORPHO": Position("MORPHO", Decimal("29.9"), entry_px=Decimal("2"))},
        expected_open_cloids=set(),
        actual_open_orders=[],
        position_size_tolerance=Decimal("1"),
        position_notional_tolerance_usd=Decimal("10"),
        position_mid_prices={"MORPHO": Decimal("100")},
    )

    assert not result.ok
    assert safe.reason == SafeModeReason.MANUAL_INTERVENTION


def test_manual_exchange_side_leverage_mismatch_pauses(store):
    safe, shield = make_shield(store)
    result = shield.manual_intervention(
        expected_positions={"BTC": Position("BTC", Decimal("0.01"), leverage=2)},
        actual_positions={"BTC": Position("BTC", Decimal("0.01"), leverage=5)},
        expected_open_cloids=set(),
        actual_open_orders=[],
    )
    assert not result.ok
    assert safe.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "BTC expected leverage 2 actual 5" in result.detail


def test_manual_exchange_side_known_cloid_detail_mismatch_pauses(store):
    safe, shield = make_shield(store)
    cloid = deterministic_cloid("expected-open")
    result = shield.manual_intervention(
        expected_positions={},
        actual_positions={},
        expected_open_cloids={cloid},
        expected_open_orders={
            cloid: OpenOrder(
                "BTC",
                "buy",
                Decimal("0.01"),
                Decimal("50000"),
                cloid=cloid,
                reduce_only=True,
            )
        },
        actual_open_orders=[
            OpenOrder(
                "BTC",
                "sell",
                Decimal("0.02"),
                Decimal("50010"),
                cloid=cloid,
                reduce_only=False,
            )
        ],
    )
    assert not result.ok
    assert safe.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "side expected buy actual sell" in result.detail
    assert "size expected 0.01 actual 0.02" in result.detail
    assert "reduce_only expected True actual False" in result.detail
