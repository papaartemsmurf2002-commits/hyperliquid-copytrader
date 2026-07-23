from __future__ import annotations

import json
from collections import deque

import pytest

from hyperliquid_copytrader.models import now_ms
from hyperliquid_copytrader.unified_account import (
    HyperliquidUserAbstraction,
    SourceDexScope,
    UnifiedAccountStateError,
    UnifiedAccountStateStream,
    classify_user_abstraction,
    non_default_dex_activity,
    parse_all_dexs_message,
    websocket_url,
)


ACCOUNT = "0xf000000000000000000000000000000000000001"


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ("disabled", HyperliquidUserAbstraction.STANDARD),
        ("standard", HyperliquidUserAbstraction.STANDARD),
        ("default", HyperliquidUserAbstraction.UNIFIED),
        ({"userAbstraction": "default"}, HyperliquidUserAbstraction.UNIFIED),
        ("unifiedAccount", HyperliquidUserAbstraction.UNIFIED),
        ("portfolioMargin", HyperliquidUserAbstraction.PORTFOLIO_MARGIN),
        ("dexAbstraction", HyperliquidUserAbstraction.DEX_ABSTRACTION),
    ],
)
def test_user_abstraction_mapping_matches_current_exchange_enum(response, expected):
    assert classify_user_abstraction(response) == expected


def test_user_abstraction_mapping_rejects_unknown_or_missing_values():
    assert classify_user_abstraction(None) is None
    assert classify_user_abstraction({"unexpected": "default"}) is None


def state(*, time_ms: int, coin: str | None = None, size: str = "0") -> dict:
    positions = []
    if coin is not None:
        positions.append({"type": "oneWay", "position": {"coin": coin, "szi": size}})
    return {
        "assetPositions": positions,
        "crossMaintenanceMarginUsed": "0",
        "crossMarginSummary": {
            "accountValue": "0",
            "totalMarginUsed": "0",
            "totalNtlPos": "0",
            "totalRawUsd": "0",
        },
        "marginSummary": {
            "accountValue": "0",
            "totalMarginUsed": "0",
            "totalNtlPos": "0",
            "totalRawUsd": "0",
        },
        "time": time_ms,
        "withdrawable": "0",
    }


def message(*states: list) -> dict:
    return {
        "channel": "allDexsClearinghouseState",
        "data": {"user": ACCOUNT, "clearinghouseStates": states},
    }


def test_parse_all_dexs_message_accepts_real_pair_list_shape():
    observed = now_ms()
    snapshot = parse_all_dexs_message(
        message(["", state(time_ms=observed)], ["xyz", state(time_ms=observed + 1)]),
        expected_account=ACCOUNT,
        received_ms=observed + 2,
    )

    assert snapshot.account == ACCOUNT
    assert snapshot.dex_count == 2
    assert snapshot.default_state["assetPositions"] == []
    assert snapshot.observed_ms == observed
    assert non_default_dex_activity(snapshot) == []


def test_source_dex_scope_exposes_all_configured_markets_value():
    assert SourceDexScope.ALL_CONFIGURED_MARKETS.value == "all_configured_markets"


def test_parse_all_dexs_message_preserves_case_sensitive_dex_names():
    observed = now_ms()
    snapshot = parse_all_dexs_message(
        message(["", state(time_ms=observed)], ["XYZ", state(time_ms=observed)]),
        expected_account=ACCOUNT,
    )

    assert set(snapshot.clearinghouse_states) == {"", "XYZ"}


def test_parse_all_dexs_message_keeps_case_distinct_and_official_punctuation():
    snapshot = parse_all_dexs_message(
        message(
            ["", state(time_ms=1)],
            ["KNETIQ", state(time_ms=1)],
            ["knetiq", state(time_ms=1)],
            ["i<3fl", state(time_ms=1)],
        ),
        expected_account=ACCOUNT,
    )

    assert set(snapshot.clearinghouse_states) == {"", "KNETIQ", "knetiq", "i<3fl"}


def test_non_default_dex_activity_detects_shared_collateral_position():
    observed = now_ms()
    snapshot = parse_all_dexs_message(
        message(
            ["", state(time_ms=observed)],
            ["xyz", state(time_ms=observed, coin="xyz:FOO", size="1")],
        ),
        expected_account=ACCOUNT,
    )

    assert non_default_dex_activity(snapshot) == ["xyz"]


@pytest.mark.parametrize(
    "payload,match",
    [
        ({"channel": "wrong", "data": {}}, "unexpected aggregate follower channel"),
        (message(["xyz", state(time_ms=1)]), "missing the default perp DEX"),
        (
            message(["", state(time_ms=1)], ["", state(time_ms=1)]),
            "is duplicated",
        ),
        (
            message(["", {"assetPositions": [], "time": "bad"}]),
            "has invalid time",
        ),
    ],
)
def test_parse_all_dexs_message_fails_closed(payload, match):
    with pytest.raises(UnifiedAccountStateError, match=match):
        parse_all_dexs_message(payload, expected_account=ACCOUNT)


class FakeConnection:
    def __init__(self, messages):
        self.messages = deque(messages)
        self.sent: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def send(self, value):
        self.sent.append(value)

    def recv(self, *, timeout):
        assert timeout == 1.0
        if not self.messages:
            raise RuntimeError("stream complete")
        return json.dumps(self.messages.popleft())


def test_unified_state_stream_returns_fresh_snapshot_and_subscribes_exact_account():
    observed = now_ms()
    connection = FakeConnection(
        [
            {"channel": "subscriptionResponse", "data": {}},
            message(["", state(time_ms=observed)]),
        ]
    )
    stream = UnifiedAccountStateStream(
        rest_url="https://api.hyperliquid-testnet.xyz",
        account=ACCOUNT,
        timeout_s=1,
        stale_after_ms=10_000,
        reconnect_attempts=0,
        reconnect_backoff_ms=0,
        connector=lambda *_args, **_kwargs: connection,
    )

    snapshot = stream.snapshot()
    stream.close()

    assert snapshot.account == ACCOUNT
    assert websocket_url("https://api.hyperliquid-testnet.xyz") == (
        "wss://api.hyperliquid-testnet.xyz/ws"
    )
    subscription = json.loads(connection.sent[0])
    assert subscription["subscription"] == {
        "type": "allDexsClearinghouseState",
        "user": ACCOUNT,
    }
