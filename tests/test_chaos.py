from __future__ import annotations

from decimal import Decimal

from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.models import (
    FollowerIntent,
    IntentAction,
    Mode,
    SafeModeReason,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.safety import ConsistencyShield, SafeModeController


def test_chaos_event_stream_dedupes_then_pauses_on_gap_and_restart(store):
    safe = SafeModeController(store)
    shield = ConsistencyShield(safe)
    assert shield.observe_source_event(
        SourceEvent("fill-1", SourceEventType.FILL, exchange_ts_ms=1000)
    ).ok
    assert shield.observe_source_event(
        SourceEvent("fill-1", SourceEventType.FILL, exchange_ts_ms=1000),
        already_seen=True,
    ).ok
    assert (
        shield.missed_event_gap("expected fill ids 2-3 before fill-4").reason
        == SafeModeReason.MISSED_EVENT_GAP
    )
    assert safe.enabled
    safe.clear("simulate operator restart")
    pending = FollowerIntent(
        intent_id="intent-restart",
        cloid=deterministic_cloid("restart"),
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="fill-4",
        reason="restart chaos",
        created_ms=now_ms(),
    )
    assert shield.restart_mid_fill([pending]).reason == SafeModeReason.RESTART_MID_FILL
