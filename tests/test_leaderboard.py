from __future__ import annotations

import threading
from decimal import Decimal

from hyperliquid_copytrader.config import LeaderboardConfig
from hyperliquid_copytrader.leaderboard import LeaderboardService, normalize_leaderboard_rows


def _row(
    address: str,
    *,
    week_roi: str,
    week_vlm: str,
    month_roi: str | None = None,
    month_vlm: str | None = None,
    month_pnl: str = "2",
    pnl: str = "10",
    account_value: str = "3000",
    display_name: object = None,
    roi_history_30d: object = None,
) -> dict:
    month_roi = month_roi if month_roi is not None else week_roi
    month_vlm = month_vlm if month_vlm is not None else week_vlm
    row = {
        "ethAddress": address,
        "accountValue": account_value,
        "displayName": display_name,
        "windowPerformances": [
            ["day", {"pnl": "1", "roi": "0.01", "vlm": "100"}],
            ["week", {"pnl": pnl, "roi": week_roi, "vlm": week_vlm}],
            ["month", {"pnl": month_pnl, "roi": month_roi, "vlm": month_vlm}],
            ["allTime", {"pnl": "3", "roi": "0.03", "vlm": "300"}],
        ],
    }
    if roi_history_30d is not None:
        row["roiHistory30d"] = roi_history_30d
    return row


def test_normalize_leaderboard_filters_active_volume_and_sorts_by_month_roi():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.05",
                    week_vlm="100",
                    month_roi="0.50",
                    month_vlm="0",
                ),
                _row(
                    "0x2222222222222222222222222222222222222222",
                    week_roi="0.12",
                    week_vlm="100",
                    month_roi="0.03",
                    month_vlm="100",
                ),
                _row(
                    "0x3333333333333333333333333333333333333333",
                    week_roi="0.03",
                    week_vlm="50",
                    month_roi="0.30",
                    month_vlm="50",
                ),
            ]
        },
        limit=20,
        min_volume_usd=Decimal("1"),
    )

    assert warnings == []
    assert [row.address for row in rows] == [
        "0x3333333333333333333333333333333333333333",
        "0x2222222222222222222222222222222222222222",
    ]
    assert rows[0].rank == 1
    assert rows[0].roi_30d_pct == Decimal("30.00")
    assert rows[0].roi_7d_pct == Decimal("3.00")
    assert rows[0].chart == []
    assert rows[0].chart_status == "unavailable"
    assert rows[0].chart_source == ""


def test_normalize_leaderboard_includes_exact_minimum_active_volume():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.05",
                    week_vlm="10",
                    month_vlm="0",
                ),
                _row(
                    "0x2222222222222222222222222222222222222222",
                    week_roi="0.04",
                    week_vlm="10",
                    month_vlm="0.99",
                ),
                _row(
                    "0x3333333333333333333333333333333333333333",
                    week_roi="0.03",
                    week_vlm="10",
                    month_vlm="1",
                ),
            ]
        },
        limit=20,
        min_volume_usd=Decimal("1"),
    )

    assert warnings == []
    assert [row.address for row in rows] == ["0x3333333333333333333333333333333333333333"]
    assert rows[0].volume_30d_usd == Decimal("1")


def test_normalize_leaderboard_applies_default_volume_and_account_value_eligibility():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.50",
                    week_vlm="100000",
                    month_vlm="100000",
                    account_value="1999.99",
                ),
                _row(
                    "0x2222222222222222222222222222222222222222",
                    week_roi="0.40",
                    week_vlm="99999.99",
                    month_vlm="99999.99",
                    account_value="2000",
                ),
                _row(
                    "0x3333333333333333333333333333333333333333",
                    week_roi="0.30",
                    week_vlm="100000",
                    month_vlm="100000",
                    account_value="2000",
                ),
            ]
        }
    )

    assert warnings == []
    assert [row.address for row in rows] == ["0x3333333333333333333333333333333333333333"]


def test_normalize_leaderboard_uses_only_explicit_reported_30d_chart_points():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.05",
                    week_vlm="100000",
                    month_vlm="100000",
                    roi_history_30d=[
                        {"timestamp_ms": 2000, "roi": "0.03", "pnl": "30"},
                        {"timestamp_ms": 1000, "roi_pct": "1.5", "pnl_usd": "15"},
                    ],
                )
            ]
        }
    )

    assert warnings == []
    assert rows[0].chart_status == "reported"
    assert rows[0].chart_source == "roiHistory30d"
    assert rows[0].chart == [
        {
            "timestamp_ms": 1000,
            "label": "1000",
            "roi_pct": Decimal("1.5"),
            "reported": True,
            "estimated": False,
            "pnl_usd": Decimal("15"),
        },
        {
            "timestamp_ms": 2000,
            "label": "2000",
            "roi_pct": Decimal("3.00"),
            "reported": True,
            "estimated": False,
            "pnl_usd": Decimal("30"),
        },
    ]


def test_malformed_optional_chart_is_ignored_without_dropping_eligible_row():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.05",
                    week_vlm="100000",
                    month_vlm="100000",
                    roi_history_30d=[{"timestamp_ms": 1000, "roi_pct": "NaN"}],
                )
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0].chart == []
    assert rows[0].chart_status == "unavailable"
    assert warnings == ["ignored malformed roiHistory30d chart history"]


def test_normalize_leaderboard_never_returns_more_than_top_one_hundred():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    f"0x{index:040x}",
                    week_roi=str(Decimal(index) / Decimal("100")),
                    week_vlm="100",
                )
                for index in range(1, 121)
            ]
        },
        limit=150,
        min_volume_usd=Decimal("1"),
    )

    assert warnings == []
    assert len(rows) == 100
    assert rows[0].rank == 1
    assert rows[-1].rank == 100
    assert rows[0].address == "0x0000000000000000000000000000000000000078"
    assert rows[-1].address == "0x0000000000000000000000000000000000000015"


def test_normalize_leaderboard_cleans_public_display_names():
    rows, warnings = normalize_leaderboard_rows(
        {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.05",
                    week_vlm="100",
                    display_name="  <img src=x onerror=alert(1)>\nTop\tTrader  " + "x" * 80,
                )
            ]
        },
        limit=20,
        min_volume_usd=Decimal("1"),
    )

    assert warnings == []
    assert rows[0].display_name == "img src=x onerror=alert(1) Top Trader " + "x" * 26
    assert len(rows[0].display_name) == 64


def test_leaderboard_service_uses_cache_and_returns_stale_on_refresh_failure():
    calls = {"count": 0}

    def fetch(_url: str, _timeout_s: Decimal):
        calls["count"] += 1
        if calls["count"] > 1:
            raise RuntimeError("upstream down")
        return {
            "leaderboardRows": [
                _row("0x4444444444444444444444444444444444444444", week_roi="0.04", week_vlm="10")
            ]
        }

    now = {"ms": 1000}
    service = LeaderboardService(
        LeaderboardConfig(cache_ttl_ms=50, limit=20, min_volume_usd=Decimal("1")),
        fetch_json=fetch,
        clock_ms=lambda: now["ms"],
    )

    fresh = service.snapshot()
    cached = service.snapshot()
    now["ms"] = 2000
    stale = service.snapshot()

    assert fresh["status"] == "fresh"
    assert cached["status"] == "cached"
    assert stale["status"] == "stale"
    assert stale["rows"][0]["address"] == "0x4444444444444444444444444444444444444444"
    assert "refresh failed: upstream down" in stale["warnings"][-1]
    assert calls["count"] == 2


def test_leaderboard_service_treats_malformed_refresh_payload_as_refresh_failure():
    calls = {"count": 0}

    def fetch(_url: str, _timeout_s: Decimal):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "leaderboardRows": [
                    _row(
                        "0x4444444444444444444444444444444444444444",
                        week_roi="0.04",
                        week_vlm="10",
                    )
                ]
            }
        return {"unexpected": "shape"}

    now = {"ms": 1000}
    service = LeaderboardService(
        LeaderboardConfig(cache_ttl_ms=50, limit=20, min_volume_usd=Decimal("1")),
        fetch_json=fetch,
        clock_ms=lambda: now["ms"],
    )

    assert service.snapshot()["status"] == "fresh"
    now["ms"] = 2000
    stale = service.snapshot()

    assert stale["status"] == "stale"
    assert stale["rows"][0]["address"] == "0x4444444444444444444444444444444444444444"
    assert "leaderboard payload does not contain rows" in stale["warnings"][-1]


def test_leaderboard_service_exposes_filter_counts_and_reuses_payload_for_overrides():
    calls = {"count": 0}

    def fetch(_url: str, _timeout_s: Decimal):
        calls["count"] += 1
        return {
            "leaderboardRows": [
                _row(
                    "0x1111111111111111111111111111111111111111",
                    week_roi="0.10",
                    week_vlm="200000",
                    month_vlm="200000",
                    account_value="3000",
                ),
                _row(
                    "0x2222222222222222222222222222222222222222",
                    week_roi="0.20",
                    week_vlm="150000",
                    month_vlm="150000",
                    account_value="2500",
                ),
                _row(
                    "0x3333333333333333333333333333333333333333",
                    week_roi="0.30",
                    week_vlm="50000",
                    month_vlm="50000",
                    account_value="5000",
                ),
            ]
        }

    service = LeaderboardService(
        LeaderboardConfig(cache_ttl_ms=50_000, limit=100, min_volume_usd=Decimal("100000")),
        fetch_json=fetch,
    )

    default = service.snapshot()
    stricter = service.snapshot(
        limit=1,
        min_volume_usd=Decimal("175000"),
        min_account_value_usd=Decimal("2750"),
    )

    assert calls["count"] == 1
    assert default["counts"] == {
        "source_rows": 3,
        "valid_rows": 3,
        "eligible_rows": 2,
        "returned_rows": 2,
        "filtered_out_rows": 1,
        "below_min_volume_rows": 1,
        "below_min_account_value_rows": 0,
        "invalid_address_rows": 0,
        "malformed_rows": 0,
        "truncated_rows": 0,
        "rows_with_reported_chart": 0,
        "rows_without_reported_chart": 2,
    }
    assert default["eligibility"]["filters_applied_before_ranking_limit"] is True
    assert default["chart_metadata"]["fabricated_points"] is False
    assert stricter["status"] == "cached"
    assert stricter["active_volume_filter_usd"] == "175000"
    assert stricter["active_account_value_filter_usd"] == "2750"
    assert [row["address"] for row in stricter["rows"]] == [
        "0x1111111111111111111111111111111111111111"
    ]
    assert stricter["counts"]["eligible_rows"] == 1
    assert stricter["counts"]["returned_rows"] == 1


def test_leaderboard_service_can_be_disabled():
    service = LeaderboardService(
        LeaderboardConfig(enabled=False), fetch_json=lambda _url, _timeout: {}
    )

    snapshot = service.snapshot()

    assert snapshot["status"] == "disabled"
    assert snapshot["rows"] == []


def test_leaderboard_service_returns_refreshing_without_cache_when_refresh_active():
    started = threading.Event()
    release = threading.Event()
    results: list[dict] = []
    errors: list[BaseException] = []

    def fetch(_url: str, _timeout_s: Decimal):
        started.set()
        release.wait(timeout=2)
        return {
            "leaderboardRows": [
                _row("0x5555555555555555555555555555555555555555", week_roi="0.05", week_vlm="10")
            ]
        }

    service = LeaderboardService(
        LeaderboardConfig(cache_ttl_ms=50, limit=20, min_volume_usd=Decimal("1")),
        fetch_json=fetch,
    )

    def refresh() -> None:
        try:
            results.append(service.snapshot(force_refresh=True))
        except BaseException as exc:  # pragma: no cover - surfaced by assertion below
            errors.append(exc)

    thread = threading.Thread(target=refresh)
    thread.start()
    try:
        assert started.wait(timeout=1)

        snapshot = service.snapshot(force_refresh=True)
    finally:
        release.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []
    assert results[0]["status"] == "fresh"
    assert snapshot["status"] == "refreshing"
    assert snapshot["rows"] == []
    assert snapshot["cache_age_ms"] is None
    assert snapshot["warnings"] == ["refresh already in progress"]


def test_leaderboard_service_returns_cached_rows_when_refresh_active():
    started = threading.Event()
    release = threading.Event()
    mode = {"block": False}
    now = {"ms": 1000}
    results: list[dict] = []
    errors: list[BaseException] = []

    def fetch(_url: str, _timeout_s: Decimal):
        if mode["block"]:
            started.set()
            release.wait(timeout=2)
            return {
                "leaderboardRows": [
                    _row(
                        "0x7777777777777777777777777777777777777777",
                        week_roi="0.07",
                        week_vlm="10",
                    )
                ]
            }
        return {
            "leaderboardRows": [
                _row("0x6666666666666666666666666666666666666666", week_roi="0.06", week_vlm="10")
            ]
        }

    service = LeaderboardService(
        LeaderboardConfig(cache_ttl_ms=50, limit=20, min_volume_usd=Decimal("1")),
        fetch_json=fetch,
        clock_ms=lambda: now["ms"],
    )
    assert service.snapshot()["status"] == "fresh"

    mode["block"] = True
    now["ms"] = 2000

    def refresh() -> None:
        try:
            results.append(service.snapshot(force_refresh=True))
        except BaseException as exc:  # pragma: no cover - surfaced by assertion below
            errors.append(exc)

    thread = threading.Thread(target=refresh)
    thread.start()
    try:
        assert started.wait(timeout=1)

        snapshot = service.snapshot(force_refresh=True)
    finally:
        release.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []
    assert results[0]["status"] == "fresh"
    assert snapshot["status"] == "refreshing"
    assert snapshot["rows"][0]["address"] == "0x6666666666666666666666666666666666666666"
    assert snapshot["cache_age_ms"] == 1000
    assert "refresh already in progress" in snapshot["warnings"]
