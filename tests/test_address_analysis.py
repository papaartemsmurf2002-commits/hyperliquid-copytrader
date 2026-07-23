from __future__ import annotations

from decimal import Decimal
from threading import Event, Thread
from collections.abc import Mapping
from typing import Any

import pytest

from hyperliquid_copytrader.address_analysis import (
    AddressAnalysisService,
    analyze_address_payloads,
    compact_fills,
    daily_account_value_points,
    daily_history_points,
    daily_trade_points,
    fetch_public_info_json,
)
from hyperliquid_copytrader.config import AddressAnalyticsConfig


ADDRESS = "0x1111111111111111111111111111111111111111"
DAY_MS = 86_400_000


def test_daily_histories_do_not_seed_buckets_from_a_future_observation() -> None:
    trade_points = daily_trade_points([], start_ms=0, end_ms=2 * DAY_MS)
    history = [{"time_ms": DAY_MS + 1, "pnl_usd": Decimal("25")}]
    account_history = [{"time_ms": DAY_MS + 1, "account_value_usd": Decimal("125")}]

    pnl_points = daily_history_points(
        history,
        value_key="pnl_usd",
        trade_points=trade_points,
        start_ms=0,
        end_ms=2 * DAY_MS,
    )
    account_points = daily_account_value_points(
        account_history,
        trade_points=trade_points,
        start_ms=0,
        end_ms=2 * DAY_MS,
    )

    assert pnl_points[0]["day_index"] == 2
    assert pnl_points[0]["pnl_usd"] == Decimal("25")
    assert pnl_points[0]["history_count"] == 1
    assert account_points[0]["day_index"] == 2
    assert account_points[0]["account_value_usd"] == Decimal("125")
    assert account_points[0]["attribution_complete"] is False


def test_trade_and_account_history_use_the_same_exact_day_boundary() -> None:
    trades = compact_fills(
        [
            {
                "time": DAY_MS,
                "coin": "BTC",
                "px": "10",
                "sz": "1",
                "side": "A",
                "dir": "Close Long",
                "closedPnl": "10",
                "fee": "0",
                "oid": 1,
                "hash": "0xaaa",
            }
        ]
    )
    trade_points = daily_trade_points(trades, start_ms=0, end_ms=2 * DAY_MS)
    account_points = daily_account_value_points(
        [
            {"time_ms": 0, "account_value_usd": Decimal("100")},
            {"time_ms": DAY_MS, "account_value_usd": Decimal("110")},
        ],
        trade_points=trade_points,
        start_ms=0,
        end_ms=2 * DAY_MS,
    )

    assert trade_points[0]["trade_count"] == 1
    assert account_points[0]["trade_delta_usd"] == Decimal("10")
    assert account_points[0]["non_trade_delta_usd"] == Decimal("0")
    assert account_points[0]["residual_available"] is True


def test_carried_account_value_waits_for_a_real_observation_before_attribution() -> None:
    trades = compact_fills(
        [
            {
                "time": 1_000,
                "coin": "BTC",
                "px": "10",
                "sz": "1",
                "side": "A",
                "dir": "Close Long",
                "closedPnl": "5",
                "fee": "0",
                "oid": 1,
                "hash": "0xaaa",
            }
        ]
    )
    trade_points = daily_trade_points(trades, start_ms=0, end_ms=2 * DAY_MS)
    account_points = daily_account_value_points(
        [
            {"time_ms": 0, "account_value_usd": Decimal("100")},
            {"time_ms": DAY_MS + 100, "account_value_usd": Decimal("105")},
        ],
        trade_points=trade_points,
        start_ms=0,
        end_ms=2 * DAY_MS,
    )

    assert account_points[0]["account_value_carried_forward"] is True
    assert account_points[0]["residual_available"] is False
    assert account_points[0]["non_trade_delta_usd"] == Decimal("0")
    assert account_points[1]["account_value_observed"] is True
    assert account_points[1]["trade_delta_usd"] == Decimal("5")
    assert account_points[1]["non_trade_delta_usd"] == Decimal("0")


def _fills() -> list[dict[str, Any]]:
    return [
        {
            "time": 1000,
            "coin": "BTC",
            "px": "100",
            "sz": "1",
            "side": "B",
            "dir": "Close Short",
            "closedPnl": "10",
            "fee": "1",
            "oid": 7,
            "hash": "0xaaa",
        },
        {
            "time": 1000,
            "coin": "BTC",
            "px": "110",
            "sz": "2",
            "side": "B",
            "dir": "Close Short",
            "closedPnl": "20",
            "fee": "2",
            "oid": 7,
            "hash": "0xaaa",
        },
        {
            "time": 2000,
            "coin": "ETH",
            "px": "10",
            "sz": "5",
            "side": "A",
            "dir": "Close Long",
            "closedPnl": "-5",
            "fee": "0.5",
            "oid": 8,
            "hash": "0xbbb",
        },
    ]


def test_compact_fills_groups_same_order_time_and_sums_pnl():
    trades = compact_fills(_fills())

    assert len(trades) == 2
    assert trades[0].coin == "BTC"
    assert trades[0].size == Decimal("3")
    assert trades[0].notional_usd == Decimal("320")
    assert trades[0].closed_pnl_usd == Decimal("30")
    assert trades[0].fee_usd == Decimal("3")
    assert trades[0].net_pnl_usd == Decimal("27")
    assert trades[0].fill_count == 2


def test_analyze_address_payloads_returns_compact_stats_and_charts():
    calls: list[Mapping[str, Any]] = []

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        calls.append(payload)
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            return _fills()
        if request_type == "portfolio":
            return [
                [
                    "week",
                    {
                        "accountValueHistory": [[0, "1000"], [1000, "1030"], [2000, "1010"]],
                        "pnlHistory": [[0, "0"], [1000, "30"], [2000, "21.5"]],
                        "vlm": "370",
                    },
                ]
            ]
        if request_type == "clearinghouseState":
            return {
                "marginSummary": {"accountValue": "1010"},
                "assetPositions": [
                    {
                        "position": {
                            "coin": "BTC",
                            "szi": "0.1",
                            "entryPx": "100",
                            "unrealizedPnl": "2",
                            "leverage": {"type": "cross", "value": 2},
                        }
                    }
                ],
            }
        raise AssertionError(f"unexpected payload {payload}")

    payload = analyze_address_payloads(
        address=ADDRESS,
        start_ms=0,
        end_ms=86_400_000,
        base_url="https://api.hyperliquid.xyz",
        timeout_s=Decimal("1"),
        max_pages=1,
        fetch_info_json=fetch,
    )

    assert [call["type"] for call in calls] == [
        "userFillsByTime",
        "portfolio",
        "clearinghouseState",
    ]
    assert payload["summary"]["compacted_trades"] == 2
    assert payload["summary"]["fills"] == 3
    assert payload["summary"]["net_pnl_usd"] == "21.5"
    assert payload["summary"]["fill_page_limit"] == 2000
    assert payload["summary"]["max_fill_pages"] == 1
    assert payload["summary"]["recent_trade_limit"] == 120
    assert payload["summary"]["point_order_limit"] == 12
    assert payload["summary"]["first_fill_time_ms"] == 1000
    assert payload["summary"]["last_fill_time_ms"] == 2000
    assert payload["summary"]["max_trade_pnl_drawdown_usd"] == "5.5"
    assert payload["summary"]["max_account_drawdown_usd"] == "20"
    assert payload["summary"]["current_account_value_usd"] == "1010"
    assert payload["summary"]["current_account_value_source"] == ("clearinghouse_margin_summary")
    assert payload["summary"]["current_account_value_confidence"] == "high"
    assert payload["summary"]["current_account_value_fallback_used"] is False
    assert payload["summary"]["portfolio_window"] == "week"
    assert payload["summary"]["portfolio_window_pnl_usd"] == "21.5"
    assert payload["summary"]["portfolio_window_pnl_scope"] == "week"
    assert payload["summary"]["portfolio_window_pnl_provenance"] == ("upstream_portfolio_window")
    assert payload["summary"]["portfolio_window_pnl_partial"] is False
    assert payload["summary"]["portfolio_window_volume_usd"] == "370"
    assert payload["summary"]["portfolio_window_volume_scope"] == "week"
    assert payload["summary"]["portfolio_window_volume_provenance"] == ("upstream_portfolio_window")
    assert payload["summary"]["portfolio_window_volume_partial"] is False
    assert payload["summary"]["portfolio_week_pnl_usd"] == "21.5"
    assert payload["summary"]["portfolio_week_volume_usd"] == "370"
    assert payload["asset_stats"][0]["coin"] == "BTC"
    assert payload["asset_stats"][0]["volume_usd"] == "320"
    assert payload["recent_trades"][0]["coin"] == "ETH"
    assert payload["open_positions"][0]["leverage"] == "cross:2"
    assert payload["charts"]["trade_pnl"][-1]["pnl_usd"] == "21.5"
    assert payload["charts"]["portfolio_pnl"][-1]["pnl_usd"] == "21.5"


@pytest.mark.parametrize(
    "window_ms",
    [30 * 86_400_000 + 1, 90 * 86_400_000, 180 * 86_400_000],
)
def test_long_all_time_metrics_are_rebased_and_bounded_to_requested_window(window_ms):
    start_ms = 10 * 86_400_000
    end_ms = start_ms + window_ms

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            return [
                {
                    "time": start_ms + 1,
                    "coin": "BTC",
                    "px": "10",
                    "sz": "2",
                    "side": "B",
                    "dir": "Open Long",
                    "closedPnl": "0",
                    "fee": "0",
                    "oid": 1,
                    "hash": "0xaaa",
                },
                {
                    "time": start_ms + 2,
                    "coin": "ETH",
                    "px": "5",
                    "sz": "3",
                    "side": "A",
                    "dir": "Open Short",
                    "closedPnl": "0",
                    "fee": "0",
                    "oid": 2,
                    "hash": "0xbbb",
                },
            ]
        if request_type == "portfolio":
            return [
                [
                    "allTime",
                    {
                        "pnlHistory": [
                            [start_ms - 86_400_000, "100"],
                            [start_ms + 86_400_000, "125"],
                            [end_ms, "180"],
                            [end_ms + 86_400_000, "999"],
                        ],
                        "vlm": "123456",
                    },
                ]
            ]
        if request_type == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    payload = analyze_address_payloads(
        address=ADDRESS,
        start_ms=start_ms,
        end_ms=end_ms,
        base_url="https://api.hyperliquid.xyz",
        timeout_s=Decimal("1"),
        max_pages=1,
        fetch_info_json=fetch,
    )

    summary = payload["summary"]
    assert summary["portfolio_window"] == "allTime"
    assert summary["portfolio_window_pnl_usd"] == "80"
    assert summary["portfolio_window_pnl_scope"] == "requested_window"
    assert summary["portfolio_window_pnl_provenance"] == "all_time_pnl_history_delta"
    assert summary["portfolio_window_pnl_partial"] is False
    assert summary["portfolio_window_pnl_coverage_start_ms"] == start_ms - 86_400_000
    assert summary["portfolio_window_pnl_coverage_end_ms"] == end_ms
    assert summary["portfolio_window_volume_usd"] == "35"
    assert summary["portfolio_window_volume_scope"] == "requested_window"
    assert summary["portfolio_window_volume_provenance"] == "user_fills_by_time"
    assert summary["portfolio_window_volume_partial"] is False
    assert summary["portfolio_upstream_pnl_usd"] == "999"
    assert summary["portfolio_upstream_volume_usd"] == "123456"
    assert summary["portfolio_upstream_scope"] == "allTime"
    assert summary["portfolio_week_pnl_usd"] == "80"
    assert summary["portfolio_week_volume_usd"] == "35"
    assert payload["charts"]["portfolio_pnl"][0]["pnl_usd"] == "25"
    assert payload["charts"]["portfolio_pnl"][-1]["pnl_usd"] == "80"


def test_long_all_time_metrics_report_partial_fill_and_pnl_coverage():
    start_ms = 0
    end_ms = 180 * 86_400_000
    full_fill_page = [
        {
            "time": index + 1,
            "coin": "BTC",
            "px": "1",
            "sz": "1",
            "side": "B",
            "dir": "Open Long",
            "closedPnl": "0",
            "fee": "0",
            "oid": index,
            "hash": f"0x{index:x}",
        }
        for index in range(2000)
    ]

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            return full_fill_page
        if request_type == "portfolio":
            return [
                [
                    "all_time",
                    {
                        "pnlHistory": [
                            [86_400_000, "50"],
                            [end_ms, "100"],
                        ],
                        "vlm": "999999",
                    },
                ]
            ]
        if request_type == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    payload = analyze_address_payloads(
        address=ADDRESS,
        start_ms=start_ms,
        end_ms=end_ms,
        base_url="https://api.hyperliquid.xyz",
        timeout_s=Decimal("1"),
        max_pages=1,
        fetch_info_json=fetch,
    )

    summary = payload["summary"]
    assert summary["truncated"] is True
    assert summary["portfolio_window"] == "allTime"
    assert summary["portfolio_window_pnl_usd"] == "50"
    assert summary["portfolio_window_pnl_partial"] is True
    assert summary["portfolio_window_pnl_coverage_start_ms"] == 86_400_000
    assert summary["portfolio_window_volume_usd"] == "2000"
    assert summary["portfolio_window_volume_scope"] == "requested_window"
    assert summary["portfolio_window_volume_provenance"] == ("user_fills_by_time_truncated")
    assert summary["portfolio_window_volume_partial"] is True


def test_analyze_address_uses_positive_portfolio_value_when_clearinghouse_is_zero():
    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            return []
        if request_type == "portfolio":
            return [
                [
                    "month",
                    {
                        "accountValueHistory": [[0, "100"], [1000, "125.5"]],
                        "pnlHistory": [[0, "0"], [1000, "25.5"]],
                        "vlm": "250",
                    },
                ]
            ]
        if request_type == "clearinghouseState":
            return {
                "assetPositions": [],
                "marginSummary": {"accountValue": "0"},
                "crossMarginSummary": {"accountValue": "0"},
            }
        raise AssertionError(f"unexpected payload {payload}")

    payload = analyze_address_payloads(
        address=ADDRESS,
        start_ms=0,
        end_ms=30 * 86_400_000,
        base_url="https://api.hyperliquid.xyz",
        timeout_s=Decimal("1"),
        max_pages=1,
        fetch_info_json=fetch,
    )

    summary = payload["summary"]
    assert summary["current_account_value_usd"] == "125.5"
    assert summary["current_account_value_source"] == ("portfolio_account_value_history_latest")
    assert summary["current_account_value_confidence"] == "medium"
    assert summary["current_account_value_fallback_used"] is True


def test_daily_trade_points_cover_no_trade_days():
    trades = compact_fills(
        [
            {
                "time": 1000,
                "coin": "BTC",
                "px": "100",
                "sz": "1",
                "side": "B",
                "dir": "Close Short",
                "closedPnl": "12",
                "fee": "1",
                "oid": 1,
                "hash": "0xaaa",
            },
            {
                "time": 2 * 86_400_000 + 1000,
                "coin": "ETH",
                "px": "10",
                "sz": "2",
                "side": "A",
                "dir": "Close Long",
                "closedPnl": "-4",
                "fee": "0.5",
                "oid": 2,
                "hash": "0xbbb",
            },
        ]
    )

    points = daily_trade_points(trades, start_ms=0, end_ms=3 * 86_400_000)

    assert len(points) == 3
    assert [point["order_count"] for point in points] == [1, 0, 1]
    assert points[1]["no_trade"] is True
    assert points[-1]["pnl_usd"] == Decimal("6.5")
    assert points[-1]["daily_move_label"] == "largest daily loss"
    assert points[-1]["asset_drivers"][0]["coin"] == "ETH"
    assert points[-1]["top_loss_orders"][0]["coin"] == "ETH"


def test_analyze_address_payloads_daily_account_points_mark_unexplained_residuals():
    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            return [
                {
                    "time": 1000,
                    "coin": "BTC",
                    "px": "100",
                    "sz": "1",
                    "side": "B",
                    "dir": "Close Short",
                    "closedPnl": "9",
                    "fee": "0",
                    "oid": 1,
                    "hash": "0xaaa",
                },
                {
                    "time": 2 * 86_400_000 + 1000,
                    "coin": "ETH",
                    "px": "10",
                    "sz": "1",
                    "side": "A",
                    "dir": "Close Long",
                    "closedPnl": "-3",
                    "fee": "0",
                    "oid": 2,
                    "hash": "0xbbb",
                },
            ]
        if request_type == "portfolio":
            return [
                [
                    "week",
                    {
                        "accountValueHistory": [
                            [0, "100"],
                            [86_400_000, "110"],
                            [2 * 86_400_000, "100"],
                            [3 * 86_400_000, "120"],
                        ],
                        "pnlHistory": [[0, "0"], [3 * 86_400_000, "6"]],
                        "vlm": "110",
                    },
                ]
            ]
        if request_type == "clearinghouseState":
            return {"marginSummary": {"accountValue": "120"}}
        raise AssertionError(f"unexpected payload {payload}")

    payload = analyze_address_payloads(
        address=ADDRESS,
        start_ms=0,
        end_ms=3 * 86_400_000,
        base_url="https://api.hyperliquid.xyz",
        timeout_s=Decimal("1"),
        max_pages=1,
        fetch_info_json=fetch,
    )

    account_points = payload["charts"]["account_value"]
    assert len(account_points) == 3
    assert [point["cashflow_type"] for point in account_points] == [
        "injection",
        "outflow",
        "injection",
    ]
    assert [point["residual_direction"] for point in account_points] == [
        "positive",
        "negative",
        "positive",
    ]
    assert all(point["residual_cashflow_confirmed"] is False for point in account_points)
    assert payload["summary"]["non_trade_move_is_transfer_proof"] is False
    assert payload["summary"]["cashflow_confirmation_available"] is False
    assert account_points[1]["no_trade"] is True
    assert account_points[1]["non_trade_delta_usd"] == "-10"


def test_address_analysis_service_uses_cache_and_returns_stale_on_refresh_failure():
    calls = {"count": 0}
    now = {"ms": 86_400_000}

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        if payload["type"] == "userFillsByTime":
            calls["count"] += 1
            if calls["count"] > 1:
                raise RuntimeError("upstream down")
            return _fills()
        if payload["type"] == "portfolio":
            return []
        if payload["type"] == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    service = AddressAnalysisService(
        AddressAnalyticsConfig(cache_ttl_ms=10, timeout_s=Decimal("1")),
        fetch_info_json=fetch,
        clock_ms=lambda: now["ms"],
    )

    fresh = service.analyze(ADDRESS)
    cached = service.analyze(ADDRESS)
    now["ms"] += 100
    stale = service.analyze(ADDRESS)

    assert fresh["status"] == "fresh"
    assert cached["status"] == "cached"
    assert stale["status"] == "stale"
    assert stale["summary"]["net_pnl_usd"] == "21.5"
    assert "refresh failed: upstream down" in stale["warnings"][-1]


def test_address_analysis_marks_auxiliary_upstream_failure_as_partial() -> None:
    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        if payload["type"] == "userFillsByTime":
            return _fills()
        if payload["type"] == "portfolio":
            raise RuntimeError("portfolio unavailable")
        if payload["type"] == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    service = AddressAnalysisService(
        AddressAnalyticsConfig(timeout_s=Decimal("1")),
        fetch_info_json=fetch,
        clock_ms=lambda: DAY_MS,
    )

    payload = service.analyze(ADDRESS)

    assert payload["status"] == "partial"
    assert any("portfolio unavailable" in warning for warning in payload["warnings"])


def test_address_analysis_public_fetch_retries_one_transient_read_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size: int) -> bytes:
            return b"[]"

    def open_request(_request, *, timeout: float):
        assert timeout == 1.0
        calls["count"] += 1
        if calls["count"] == 1:
            raise TimeoutError("transient read timeout")
        return Response()

    monkeypatch.setenv("HLCT_REST_THROTTLE_INFO", "false")
    monkeypatch.setattr("hyperliquid_copytrader.address_analysis.urlopen", open_request)

    assert fetch_public_info_json(
        "https://api.hyperliquid.xyz",
        {"type": "userFillsByTime", "user": ADDRESS},
        Decimal("1"),
    ) == []
    assert calls["count"] == 2


def test_address_analysis_service_rejects_invalid_address_without_fetching():
    def fetch(*_args, **_kwargs):
        raise AssertionError("invalid address should not fetch")

    service = AddressAnalysisService(AddressAnalyticsConfig(), fetch_info_json=fetch)

    payload = service.analyze("not-an-address")

    assert payload["status"] == "error"
    assert payload["summary"] == {}
    assert "address must be a 42-character hex value" in payload["warnings"]


def test_address_analysis_cache_is_bounded_and_evicts_least_recently_used():
    fill_calls: list[str] = []

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            fill_calls.append(payload["user"])
            return []
        if request_type == "portfolio":
            return []
        if request_type == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    service = AddressAnalysisService(
        AddressAnalyticsConfig(cache_ttl_ms=60_000, timeout_s=Decimal("1")),
        fetch_info_json=fetch,
        clock_ms=lambda: 86_400_000,
        max_cache_entries=2,
    )
    address_a = "0x" + "1" * 40
    address_b = "0x" + "2" * 40
    address_c = "0x" + "3" * 40

    assert service.analyze(address_a)["status"] == "fresh"
    assert service.analyze(address_b)["status"] == "fresh"
    assert service.analyze(address_a)["status"] == "cached"
    assert service.analyze(address_c)["status"] == "fresh"
    assert service.analyze(address_a)["status"] == "cached"
    assert service.analyze(address_b)["status"] == "fresh"

    assert fill_calls == [address_a, address_b, address_c, address_b]
    assert len(service._cache) == 2


def test_address_analysis_cache_preserves_nonblocking_single_flight():
    refresh_started = Event()
    release_refresh = Event()
    fill_calls: list[str] = []
    result: dict[str, dict[str, Any]] = {}

    def fetch(_base_url: str, payload: Mapping[str, Any], _timeout_s: Decimal):
        request_type = payload["type"]
        if request_type == "userFillsByTime":
            fill_calls.append(payload["user"])
            refresh_started.set()
            if not release_refresh.wait(timeout=2):
                raise TimeoutError("test did not release analytics refresh")
            return []
        if request_type == "portfolio":
            return []
        if request_type == "clearinghouseState":
            return {}
        raise AssertionError(f"unexpected payload {payload}")

    service = AddressAnalysisService(
        AddressAnalyticsConfig(timeout_s=Decimal("1")),
        fetch_info_json=fetch,
        max_cache_entries=2,
    )
    other_address = "0x" + "2" * 40
    refresh_thread = Thread(
        target=lambda: result.setdefault("payload", service.analyze(ADDRESS)),
        daemon=True,
    )
    refresh_thread.start()
    assert refresh_started.wait(timeout=2)

    concurrent = service.analyze(other_address)
    release_refresh.set()
    refresh_thread.join(timeout=2)

    assert not refresh_thread.is_alive()
    assert concurrent["status"] == "refreshing"
    assert concurrent["warnings"] == ["analysis refresh already in progress"]
    assert result["payload"]["status"] == "fresh"
    assert fill_calls == [ADDRESS]


@pytest.mark.parametrize("max_cache_entries", [0, -1, True, 1.5])
def test_address_analysis_cache_requires_positive_integer_capacity(max_cache_entries):
    with pytest.raises(ValueError, match="positive integer"):
        AddressAnalysisService(
            AddressAnalyticsConfig(),
            max_cache_entries=max_cache_entries,
        )
