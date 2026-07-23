from __future__ import annotations

import json
from collections import OrderedDict, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from statistics import median
from threading import Lock
from typing import Any, overload
from urllib.request import Request, urlopen

from .config import MAX_ADDRESS_ANALYSIS_PAGES, AddressAnalyticsConfig
from .markets import MarketIdentityError, canonical_market_symbol
from .models import now_ms, parse_decimal, to_jsonable
from .rest_throttle import call_with_rest_backoff, info_rest_throttle_enabled_for_base_url


FetchInfoJSON = Callable[[str, Mapping[str, Any], Decimal], Any]
FILL_PAGE_LIMIT = 2000
MS_PER_DAY = 86_400_000
MAX_CHART_POINTS = 900
MAX_RECENT_TRADES = 120
MAX_POINT_ORDERS = 12
MAX_ASSET_ROWS = 8
MAX_ADDRESS_ANALYSIS_CACHE_ENTRIES = 128
MAX_INFO_RESPONSE_BYTES = 5_000_000
ADDRESS_ANALYSIS_INFO_ATTEMPTS = 2


@dataclass(frozen=True)
class CompactTrade:
    time_ms: int
    coin: str
    side: str
    direction: str
    size: Decimal
    avg_price: Decimal
    notional_usd: Decimal
    closed_pnl_usd: Decimal
    fee_usd: Decimal
    net_pnl_usd: Decimal
    order_id: str
    tx_hash: str
    fill_count: int


@dataclass(frozen=True)
class PortfolioPnlMetric:
    points: tuple[dict[str, Any], ...]
    value_usd: Decimal
    scope: str
    provenance: str
    partial: bool
    coverage_start_ms: int
    coverage_end_ms: int


class AddressAnalysisService:
    def __init__(
        self,
        config: AddressAnalyticsConfig,
        *,
        fetch_info_json: FetchInfoJSON | None = None,
        clock_ms: Callable[[], int] = now_ms,
        max_cache_entries: int = MAX_ADDRESS_ANALYSIS_CACHE_ENTRIES,
    ) -> None:
        if (
            isinstance(max_cache_entries, bool)
            or not isinstance(max_cache_entries, int)
            or max_cache_entries <= 0
        ):
            raise ValueError("max_cache_entries must be a positive integer")
        self.config = config
        self._fetch_info_json = fetch_info_json or fetch_public_info_json
        self._clock_ms = clock_ms
        self._lock = Lock()
        self._cache_lock = Lock()
        self._max_cache_entries = max_cache_entries
        self._cache: OrderedDict[tuple[str, int], dict[str, Any]] = OrderedDict()
        self._cache_ms: dict[tuple[str, int], int] = {}

    def analyze(
        self,
        address: str,
        *,
        force_refresh: bool = False,
        window_days: int | None = None,
    ) -> dict[str, Any]:
        observed = self._clock_ms()
        normalized = normalize_address(address)
        days = window_days or self.config.window_days
        cache_key = (normalized, days)
        if not self.config.enabled:
            return self._empty_snapshot(
                address=normalized,
                days=days,
                observed=observed,
                status="disabled",
                warnings=["address analytics disabled"],
            )
        if not valid_address(normalized):
            return self._empty_snapshot(
                address=normalized,
                days=days,
                observed=observed,
                status="error",
                warnings=["address must be a 42-character hex value"],
            )
        if days <= 0:
            return self._empty_snapshot(
                address=normalized,
                days=days,
                observed=observed,
                status="error",
                warnings=["window days must be positive"],
            )

        if not self._lock.acquire(blocking=False):
            cached = self._cached_snapshot(cache_key, status="refreshing", observed=observed)
            if cached is not None:
                return cached
            return self._empty_snapshot(
                address=normalized,
                days=days,
                observed=observed,
                status="refreshing",
                warnings=["analysis refresh already in progress"],
            )

        try:
            if not force_refresh:
                cached = self._cached_snapshot(cache_key, status="cached", observed=observed)
                if cached is not None and cached["cache_age_ms"] <= self.config.cache_ttl_ms:
                    return cached

            start_ms = observed - days * MS_PER_DAY
            try:
                snapshot = analyze_address_payloads(
                    address=normalized,
                    start_ms=start_ms,
                    end_ms=observed,
                    base_url=self.config.url,
                    timeout_s=self.config.timeout_s,
                    max_pages=min(self.config.max_pages, MAX_ADDRESS_ANALYSIS_PAGES),
                    fetch_info_json=self._fetch_info_json,
                )
            except Exception as exc:
                cached = self._cached_snapshot(
                    cache_key,
                    status="stale",
                    observed=observed,
                    warning=f"refresh failed: {exc}",
                )
                if cached is not None:
                    return cached
                return self._empty_snapshot(
                    address=normalized,
                    days=days,
                    observed=observed,
                    status="error",
                    warnings=[str(exc)],
                )
            snapshot["status"] = "partial" if snapshot.get("warnings") else "fresh"
            snapshot["source"] = self.config.url
            snapshot["generated_ms"] = observed
            snapshot["cache_age_ms"] = 0
            self._store_snapshot(cache_key, snapshot, observed=observed)
            return snapshot
        finally:
            self._lock.release()

    def local_snapshot(
        self,
        address: str,
        *,
        window_days: int | None = None,
    ) -> dict[str, Any]:
        """Return only in-process cache state; never perform an external info request."""

        observed = self._clock_ms()
        normalized = normalize_address(address)
        days = window_days or self.config.window_days
        cached = self._cached_snapshot(
            (normalized, days),
            status="local_cache",
            observed=observed,
            warning="external address analysis is disabled in the fleet-capable UI",
        )
        if cached is not None:
            return cached
        return self._empty_snapshot(
            address=normalized,
            days=days,
            observed=observed,
            status="local_only_unavailable",
            warnings=[
                "no local address-analysis cache is available; external info requests are disabled "
                "in the fleet-capable UI"
            ],
        )

    def _cached_snapshot(
        self,
        cache_key: tuple[str, int],
        *,
        status: str,
        observed: int,
        warning: str | None = None,
    ) -> dict[str, Any] | None:
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached is None:
                return None
            self._cache.move_to_end(cache_key)
            generated_ms = self._cache_ms.get(cache_key, observed)
            snapshot = dict(cached)
        snapshot["status"] = status
        snapshot["cache_age_ms"] = max(0, observed - generated_ms)
        if warning is not None:
            snapshot["warnings"] = [*snapshot.get("warnings", []), warning]
        return snapshot

    def _store_snapshot(
        self,
        cache_key: tuple[str, int],
        snapshot: dict[str, Any],
        *,
        observed: int,
    ) -> None:
        with self._cache_lock:
            self._cache[cache_key] = snapshot
            self._cache.move_to_end(cache_key)
            self._cache_ms[cache_key] = observed
            while len(self._cache) > self._max_cache_entries:
                evicted_key, _snapshot = self._cache.popitem(last=False)
                self._cache_ms.pop(evicted_key, None)

    def _empty_snapshot(
        self,
        *,
        address: str,
        days: int,
        observed: int,
        status: str,
        warnings: list[str],
    ) -> dict[str, Any]:
        end_ms = observed
        start_ms = observed - max(days, 0) * MS_PER_DAY
        return {
            "status": status,
            "source": self.config.url,
            "generated_ms": observed,
            "cache_age_ms": None,
            "address": address,
            "window_days": days,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "summary": {},
            "charts": {"trade_pnl": [], "portfolio_pnl": [], "account_value": []},
            "asset_stats": [],
            "recent_trades": [],
            "open_positions": [],
            "warnings": warnings,
        }


def fetch_public_info_json(
    base_url: str,
    payload: Mapping[str, Any],
    timeout_s: Decimal,
) -> Any:
    request_type = str(payload.get("type") or "unknown")

    def request_info() -> Any:
        body = json.dumps(dict(payload)).encode("utf-8")
        request = Request(
            base_url.rstrip("/") + "/info",
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "hl-copytrader/0.1",
            },
            method="POST",
        )
        with urlopen(request, timeout=float(timeout_s)) as response:  # nosec B310
            raw = response.read(MAX_INFO_RESPONSE_BYTES + 1)
        if len(raw) > MAX_INFO_RESPONSE_BYTES:
            raise ValueError("address-analysis info response exceeds the size bound")
        return json.loads(raw.decode("utf-8"))

    return call_with_rest_backoff(
        f"info:{request_type}",
        request_info,
        enabled=info_rest_throttle_enabled_for_base_url(base_url),
        attempts=ADDRESS_ANALYSIS_INFO_ATTEMPTS,
    )


def analyze_address_payloads(
    *,
    address: str,
    start_ms: int,
    end_ms: int,
    base_url: str,
    timeout_s: Decimal,
    max_pages: int,
    fetch_info_json: FetchInfoJSON,
) -> dict[str, Any]:
    fills, fill_pages, truncated, warnings = fetch_fill_pages(
        address=address,
        start_ms=start_ms,
        end_ms=end_ms,
        base_url=base_url,
        timeout_s=timeout_s,
        max_pages=max_pages,
        fetch_info_json=fetch_info_json,
    )
    portfolio = _try_info(
        fetch_info_json,
        base_url,
        {"type": "portfolio", "user": address},
        timeout_s,
        warnings,
        "portfolio",
    )
    clearinghouse = _try_info(
        fetch_info_json,
        base_url,
        {"type": "clearinghouseState", "user": address},
        timeout_s,
        warnings,
        "clearinghouseState",
    )
    if not fills and portfolio is None and clearinghouse is None:
        raise RuntimeError("; ".join(warnings) or "no address analysis data returned")

    trades = compact_fills(fills)
    portfolio_windows = portfolio_windows_payload(portfolio)
    portfolio_window_label, portfolio_window = select_portfolio_window(
        portfolio_windows,
        window_days=_window_day_count(start_ms, end_ms),
    )
    summary = build_summary(
        address=address,
        start_ms=start_ms,
        end_ms=end_ms,
        trades=trades,
        fill_count=len(fills),
        fill_pages=fill_pages,
        max_pages=max_pages,
        truncated=truncated,
        portfolio_window=portfolio_window,
        portfolio_window_label=portfolio_window_label,
        clearinghouse=clearinghouse,
    )
    charts = build_charts(
        trades=trades,
        portfolio_window=portfolio_window,
        portfolio_window_label=portfolio_window_label,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    return {
        "address": address,
        "window_days": _window_days(start_ms, end_ms),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "summary": to_jsonable(summary),
        "charts": to_jsonable(charts),
        "asset_stats": to_jsonable(asset_stats(trades)),
        "recent_trades": to_jsonable(recent_trades(trades)),
        "open_positions": to_jsonable(open_positions(clearinghouse)),
        "warnings": warnings,
    }


def fetch_fill_pages(
    *,
    address: str,
    start_ms: int,
    end_ms: int,
    base_url: str,
    timeout_s: Decimal,
    max_pages: int,
    fetch_info_json: FetchInfoJSON,
) -> tuple[list[Mapping[str, Any]], int, bool, list[str]]:
    if max_pages <= 0:
        raise ValueError("max pages must be positive")
    page_start = start_ms
    pages = 0
    fills: list[Mapping[str, Any]] = []
    warnings: list[str] = []
    truncated = False
    exhausted = False
    while page_start <= end_ms and pages < max_pages:
        payload = {
            "type": "userFillsByTime",
            "user": address,
            "startTime": page_start,
            "endTime": end_ms,
            "aggregateByTime": True,
        }
        page = fetch_info_json(base_url, payload, timeout_s)
        if not isinstance(page, list):
            raise ValueError("userFillsByTime response is not a list")
        pages += 1
        if not page:
            exhausted = True
            break
        valid_page = [item for item in page if isinstance(item, Mapping)]
        if len(valid_page) != len(page):
            warnings.append("skipped non-object fill rows")
        fills.extend(valid_page)
        max_seen = max((_int(item.get("time")) for item in valid_page), default=page_start)
        if len(page) < FILL_PAGE_LIMIT:
            exhausted = True
            break
        if max_seen <= page_start:
            warnings.append("fill page was full but timestamps did not advance")
            truncated = True
            break
        page_start = max_seen + 1
    if not exhausted and page_start <= end_ms and pages >= max_pages:
        warnings.append("fill analysis hit max page limit before the end of the window")
        truncated = True
    fills.sort(key=lambda item: (_int(item.get("time")), str(item.get("tid") or "")))
    return fills, pages, truncated, warnings


def compact_fills(fills: Sequence[Mapping[str, Any]]) -> list[CompactTrade]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for fill in fills:
        time_ms = _int(fill.get("time"))
        raw_coin = str(fill.get("coin") or "").strip()
        try:
            coin = canonical_market_symbol(raw_coin)
        except MarketIdentityError:
            coin = raw_coin.upper()
        if time_ms <= 0 or not coin:
            continue
        direction = _clean_text(fill.get("dir"))
        side = _normalize_side(fill.get("side"))
        order_id = _clean_text(fill.get("oid"))
        tx_hash = _clean_text(fill.get("hash")).lower()
        key = (time_ms, coin, order_id, tx_hash, direction, side)
        size = _decimal(fill.get("sz"))
        price = _decimal(fill.get("px"))
        notional = size * price
        bucket = grouped.setdefault(
            key,
            {
                "time_ms": time_ms,
                "coin": coin,
                "side": side,
                "direction": direction,
                "size": Decimal("0"),
                "notional_usd": Decimal("0"),
                "closed_pnl_usd": Decimal("0"),
                "fee_usd": Decimal("0"),
                "order_id": order_id,
                "tx_hash": tx_hash,
                "fill_count": 0,
            },
        )
        bucket["size"] += size
        bucket["notional_usd"] += notional
        bucket["closed_pnl_usd"] += _decimal(fill.get("closedPnl"))
        bucket["fee_usd"] += _decimal(fill.get("fee"))
        bucket["fill_count"] += 1
    trades: list[CompactTrade] = []
    for bucket in grouped.values():
        size = bucket["size"]
        notional = bucket["notional_usd"]
        avg_price = notional / size if size else Decimal("0")
        closed_pnl = bucket["closed_pnl_usd"]
        fee = bucket["fee_usd"]
        trades.append(
            CompactTrade(
                time_ms=bucket["time_ms"],
                coin=bucket["coin"],
                side=bucket["side"],
                direction=bucket["direction"],
                size=size,
                avg_price=avg_price,
                notional_usd=notional,
                closed_pnl_usd=closed_pnl,
                fee_usd=fee,
                net_pnl_usd=closed_pnl - fee,
                order_id=bucket["order_id"],
                tx_hash=bucket["tx_hash"],
                fill_count=bucket["fill_count"],
            )
        )
    return sorted(trades, key=lambda trade: (trade.time_ms, trade.order_id, trade.coin))


def build_summary(
    *,
    address: str,
    start_ms: int,
    end_ms: int,
    trades: list[CompactTrade],
    fill_count: int,
    fill_pages: int,
    max_pages: int,
    truncated: bool,
    portfolio_window: Mapping[str, Any],
    portfolio_window_label: str,
    clearinghouse: Any,
) -> dict[str, Any]:
    total_closed = sum((trade.closed_pnl_usd for trade in trades), Decimal("0"))
    total_fees = sum((trade.fee_usd for trade in trades), Decimal("0"))
    total_net = sum((trade.net_pnl_usd for trade in trades), Decimal("0"))
    total_volume = sum((trade.notional_usd for trade in trades), Decimal("0"))
    pnl_trades = [trade for trade in trades if trade.closed_pnl_usd != 0]
    wins = [trade for trade in pnl_trades if trade.net_pnl_usd > 0]
    losses = [trade for trade in pnl_trades if trade.net_pnl_usd < 0]
    gross_profit = sum(
        (trade.net_pnl_usd for trade in trades if trade.net_pnl_usd > 0), Decimal("0")
    )
    gross_loss = sum((trade.net_pnl_usd for trade in trades if trade.net_pnl_usd < 0), Decimal("0"))
    drawdown = max_drawdown(cumulative_trade_points(trades))
    account_values = _history_points(
        portfolio_window.get("accountValueHistory"), "account_value_usd"
    )
    account_drawdown = max_drawdown(account_values, value_key="account_value_usd")
    portfolio_pnl = portfolio_pnl_metric(
        portfolio_window,
        portfolio_window_label=portfolio_window_label,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    cashflow_points = annotate_account_value_moves(
        account_values,
        cumulative_trade_points(trades, limit=0),
    )
    cashflow_events = [
        point
        for point in cashflow_points
        if abs(_decimal(point.get("non_trade_delta_usd"))) >= Decimal("1")
    ]
    largest_cashflow = max(
        cashflow_events,
        key=lambda point: abs(_decimal(point.get("non_trade_delta_usd"))),
        default=None,
    )
    best_trade = max(trades, key=lambda trade: trade.net_pnl_usd, default=None)
    worst_trade = min(trades, key=lambda trade: trade.net_pnl_usd, default=None)
    positions = open_positions(clearinghouse)
    upstream_portfolio_pnl = _last_history_value(
        _history_points(portfolio_window.get("pnlHistory"), "pnl_usd"),
        "pnl_usd",
    )
    upstream_portfolio_volume = _decimal(portfolio_window.get("vlm"))
    use_fill_derived_volume = _uses_bounded_all_time_metric(
        portfolio_window_label,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    portfolio_window_volume = total_volume if use_fill_derived_volume else upstream_portfolio_volume
    portfolio_volume_scope = (
        "requested_window" if use_fill_derived_volume else portfolio_window_label
    )
    portfolio_volume_provenance = (
        "user_fills_by_time_truncated"
        if use_fill_derived_volume and truncated
        else "user_fills_by_time"
        if use_fill_derived_volume
        else "upstream_portfolio_window"
    )
    (
        current_account_value,
        current_account_value_source,
        current_account_value_confidence,
        current_account_value_fallback_used,
    ) = _current_account_value_context(clearinghouse, account_values)
    return {
        "address": address,
        "fills": fill_count,
        "fill_pages": fill_pages,
        "fill_page_limit": FILL_PAGE_LIMIT,
        "max_fill_pages": max_pages,
        "recent_trade_limit": MAX_RECENT_TRADES,
        "point_order_limit": MAX_POINT_ORDERS,
        "truncated": truncated,
        "compacted_trades": len(trades),
        "first_fill_time_ms": trades[0].time_ms if trades else 0,
        "last_fill_time_ms": trades[-1].time_ms if trades else 0,
        "window_days": _window_days(start_ms, end_ms),
        "active_days": active_days(trades),
        "portfolio_window": portfolio_window_label,
        "trades_per_day": _decimal_div(len(trades), Decimal(str(_window_days(start_ms, end_ms)))),
        "trades_per_active_day": _decimal_div(len(trades), Decimal(max(1, active_days(trades)))),
        "median_minutes_between_trades": median_gap_minutes(trades),
        "volume_usd": total_volume,
        "closed_pnl_usd": total_closed,
        "fees_usd": total_fees,
        "net_pnl_usd": total_net,
        "win_rate_pct": _decimal_div(len(wins) * 100, Decimal(max(1, len(pnl_trades)))),
        "closed_profit_trades": len(wins),
        "closed_loss_trades": len(losses),
        "profit_factor": (gross_profit / abs(gross_loss) if gross_loss < 0 else None),
        "max_trade_pnl_drawdown_usd": drawdown["drawdown_usd"],
        "max_trade_pnl_drawdown_from_ms": drawdown["from_ms"],
        "max_trade_pnl_drawdown_to_ms": drawdown["to_ms"],
        "max_account_drawdown_usd": account_drawdown["drawdown_usd"],
        "max_account_drawdown_pct": account_drawdown["drawdown_pct"],
        "best_trade": trade_brief(best_trade),
        "worst_trade": trade_brief(worst_trade),
        "current_account_value_usd": current_account_value,
        "current_account_value_source": current_account_value_source,
        "current_account_value_confidence": current_account_value_confidence,
        "current_account_value_fallback_used": current_account_value_fallback_used,
        "portfolio_window_pnl_usd": portfolio_pnl.value_usd,
        "portfolio_window_pnl_scope": portfolio_pnl.scope,
        "portfolio_window_pnl_provenance": portfolio_pnl.provenance,
        "portfolio_window_pnl_partial": portfolio_pnl.partial,
        "portfolio_window_pnl_coverage_start_ms": portfolio_pnl.coverage_start_ms,
        "portfolio_window_pnl_coverage_end_ms": portfolio_pnl.coverage_end_ms,
        "portfolio_window_volume_usd": portfolio_window_volume,
        "portfolio_window_volume_scope": portfolio_volume_scope,
        "portfolio_window_volume_provenance": portfolio_volume_provenance,
        "portfolio_window_volume_partial": use_fill_derived_volume and truncated,
        "portfolio_upstream_pnl_usd": upstream_portfolio_pnl,
        "portfolio_upstream_volume_usd": upstream_portfolio_volume,
        "portfolio_upstream_scope": portfolio_window_label,
        "portfolio_week_pnl_usd": portfolio_pnl.value_usd,
        "portfolio_week_volume_usd": portfolio_window_volume,
        "non_trade_move_count": len(cashflow_events),
        "unexplained_residual_count": len(cashflow_events),
        "non_trade_move_is_transfer_proof": False,
        "cashflow_confirmation_available": False,
        "cashflow_type_semantics": "legacy_residual_direction_not_confirmed_transfer",
        "unexplained_residual_provenance": (
            "account_value_delta_minus_compacted_closed_fill_net_pnl"
        ),
        "non_trade_move_method": (
            "account-value delta minus compacted closed trade PnL; residual may include "
            "unrealized PnL, funding, fees, transfers, or other balance effects"
        ),
        "largest_non_trade_move_usd": (
            _decimal(largest_cashflow.get("non_trade_delta_usd"))
            if largest_cashflow is not None
            else Decimal("0")
        ),
        "largest_non_trade_move_time_ms": (
            _int(largest_cashflow.get("time_ms")) if largest_cashflow is not None else 0
        ),
        "largest_unexplained_residual_usd": (
            _decimal(largest_cashflow.get("non_trade_delta_usd"))
            if largest_cashflow is not None
            else Decimal("0")
        ),
        "largest_unexplained_residual_time_ms": (
            _int(largest_cashflow.get("time_ms")) if largest_cashflow is not None else 0
        ),
        "open_position_count": len(positions),
    }


def build_charts(
    *,
    trades: list[CompactTrade],
    portfolio_window: Mapping[str, Any],
    portfolio_window_label: str,
    start_ms: int,
    end_ms: int,
) -> dict[str, list[dict[str, Any]]]:
    account_values = _history_points(
        portfolio_window.get("accountValueHistory"), "account_value_usd"
    )
    trade_points = daily_trade_points(trades, start_ms=start_ms, end_ms=end_ms)
    portfolio_pnl = portfolio_pnl_metric(
        portfolio_window,
        portfolio_window_label=portfolio_window_label,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    return {
        "trade_pnl": trade_points,
        "portfolio_pnl": daily_history_points(
            list(portfolio_pnl.points),
            value_key="pnl_usd",
            trade_points=trade_points,
            start_ms=start_ms,
            end_ms=end_ms,
        ),
        "account_value": daily_account_value_points(
            account_values,
            trade_points=trade_points,
            start_ms=start_ms,
            end_ms=end_ms,
        ),
    }


def daily_trade_points(
    trades: list[CompactTrade],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    buckets = day_buckets(start_ms, end_ms)
    if not buckets:
        return []
    sorted_trades = sorted(trades, key=lambda trade: (trade.time_ms, trade.order_id, trade.coin))
    trade_index = 0
    cumulative = Decimal("0")
    points: list[dict[str, Any]] = []
    for day_index, (day_start, day_end) in enumerate(buckets, start=1):
        bucket: list[CompactTrade] = []
        while trade_index < len(sorted_trades) and sorted_trades[trade_index].time_ms <= day_end:
            trade = sorted_trades[trade_index]
            if trade.time_ms >= day_start:
                bucket.append(trade)
            trade_index += 1
        day_pnl = sum((trade.net_pnl_usd for trade in bucket), Decimal("0"))
        cumulative += day_pnl
        points.append(
            {
                "time_ms": day_end,
                "day_start_ms": day_start,
                "day_end_ms": day_end,
                "day_index": day_index,
                "pnl_usd": cumulative,
                "order_count": len(bucket),
                "trade_count": len(bucket),
                "orders": [trade_brief(trade) for trade in bucket[-MAX_POINT_ORDERS:]],
                "top_loss_orders": top_loss_orders(bucket),
                "asset_drivers": trade_asset_drivers(bucket),
                "bucket_net_pnl_usd": day_pnl,
                "bucket_volume_usd": sum(
                    (trade.notional_usd for trade in bucket),
                    Decimal("0"),
                ),
                "no_trade": len(bucket) == 0,
            }
        )
    annotate_trade_point_moves(points)
    return points


def top_loss_orders(trades: list[CompactTrade]) -> list[dict[str, Any]]:
    losses = sorted(trades, key=lambda trade: (trade.net_pnl_usd, -trade.notional_usd))
    return [trade_brief(trade) for trade in losses[:MAX_POINT_ORDERS] if trade.net_pnl_usd < 0]


def trade_asset_drivers(trades: list[CompactTrade], limit: int = 6) -> list[dict[str, Any]]:
    if not trades:
        return []
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "coin": "",
            "trades": 0,
            "volume_usd": Decimal("0"),
            "net_pnl_usd": Decimal("0"),
            "closed_pnl_usd": Decimal("0"),
            "fees_usd": Decimal("0"),
        }
    )
    for trade in trades:
        row = stats[trade.coin]
        row["coin"] = trade.coin
        row["trades"] += 1
        row["volume_usd"] += trade.notional_usd
        row["net_pnl_usd"] += trade.net_pnl_usd
        row["closed_pnl_usd"] += trade.closed_pnl_usd
        row["fees_usd"] += trade.fee_usd
    rows = list(stats.values())
    rows.sort(key=lambda row: (_decimal(row["net_pnl_usd"]), _decimal(row["volume_usd"])))
    loss_rows = [row for row in rows if _decimal(row["net_pnl_usd"]) < 0]
    if len(loss_rows) >= limit:
        return loss_rows[:limit]
    gain_rows = [row for row in reversed(rows) if _decimal(row["net_pnl_usd"]) >= 0]
    return [*loss_rows, *gain_rows[: max(0, limit - len(loss_rows))]]


def annotate_trade_point_moves(points: list[dict[str, Any]]) -> None:
    if not points:
        return
    largest_loss = min(points, key=lambda point: _decimal(point.get("bucket_net_pnl_usd")))
    if _decimal(largest_loss.get("bucket_net_pnl_usd")) < 0:
        largest_loss["daily_move_label"] = "largest daily loss"
        largest_loss["daily_move_severity"] = "loss"
    largest_gain = max(points, key=lambda point: _decimal(point.get("bucket_net_pnl_usd")))
    if _decimal(largest_gain.get("bucket_net_pnl_usd")) > 0:
        largest_gain["daily_move_label"] = "largest daily gain"
        largest_gain["daily_move_severity"] = "gain"
    drawdown = max_drawdown(points)
    drawdown_to = _int(drawdown.get("to_ms"))
    drawdown_from = _int(drawdown.get("from_ms"))
    if drawdown_to and _decimal(drawdown.get("drawdown_usd")) > 0:
        for point in points:
            if _int(point.get("time_ms")) == drawdown_to:
                point["drawdown_label"] = "max drawdown trough"
                point["drawdown_from_ms"] = drawdown_from
                point["drawdown_usd"] = drawdown["drawdown_usd"]
                break


def daily_history_points(
    history_points: list[dict[str, Any]],
    *,
    value_key: str,
    trade_points: list[dict[str, Any]],
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    buckets = day_buckets(start_ms, end_ms)
    if not buckets:
        return []
    sorted_history = sorted(history_points, key=lambda point: _int(point.get("time_ms")))
    history_index = 0
    last_value: Decimal | None = None
    while (
        history_index < len(sorted_history)
        and _int(sorted_history[history_index].get("time_ms")) <= start_ms
    ):
        last_value = _decimal(sorted_history[history_index].get(value_key))
        history_index += 1
    points: list[dict[str, Any]] = []
    for day_index, (day_start, day_end) in enumerate(buckets, start=1):
        observed = 0
        while (
            history_index < len(sorted_history)
            and _int(sorted_history[history_index].get("time_ms")) <= day_end
        ):
            last_value = _decimal(sorted_history[history_index].get(value_key))
            if _int(sorted_history[history_index].get("time_ms")) >= day_start:
                observed += 1
            history_index += 1
        if last_value is None:
            continue
        trade_point = trade_points[day_index - 1] if day_index - 1 < len(trade_points) else {}
        points.append(
            {
                "time_ms": day_end,
                "day_start_ms": day_start,
                "day_end_ms": day_end,
                "day_index": day_index,
                value_key: last_value,
                "order_count": trade_point.get("order_count", 0),
                "trade_count": trade_point.get("trade_count", 0),
                "orders": trade_point.get("orders", []),
                "bucket_net_pnl_usd": trade_point.get("bucket_net_pnl_usd", Decimal("0")),
                "bucket_volume_usd": trade_point.get("bucket_volume_usd", Decimal("0")),
                "history_count": observed,
                "history_observed": observed > 0,
                "carried_forward": observed == 0,
                "no_trade": not trade_point.get("order_count", 0),
            }
        )
    return points


def daily_account_value_points(
    account_values: list[dict[str, Any]],
    *,
    trade_points: list[dict[str, Any]],
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    buckets = day_buckets(start_ms, end_ms)
    if not buckets or not account_values:
        return []
    sorted_values = sorted(account_values, key=lambda point: _int(point.get("time_ms")))
    history_index = 0
    last_value: Decimal | None = None
    while (
        history_index < len(sorted_values)
        and _int(sorted_values[history_index].get("time_ms")) <= start_ms
    ):
        last_value = _decimal(sorted_values[history_index].get("account_value_usd"))
        history_index += 1
    previous_account = last_value
    previous_trade = Decimal("0")
    points: list[dict[str, Any]] = []
    for day_index, (day_start, day_end) in enumerate(buckets, start=1):
        observed = 0
        while (
            history_index < len(sorted_values)
            and _int(sorted_values[history_index].get("time_ms")) <= day_end
        ):
            last_value = _decimal(sorted_values[history_index].get("account_value_usd"))
            if _int(sorted_values[history_index].get("time_ms")) >= day_start:
                observed += 1
            history_index += 1
        trade_point = trade_points[day_index - 1] if day_index - 1 < len(trade_points) else {}
        cumulative_trade = _decimal(trade_point.get("pnl_usd"))
        if last_value is None:
            continue
        account_value_observed = observed > 0
        if account_value_observed and previous_account is not None:
            residual_available = True
            account_delta = last_value - previous_account
            trade_delta = cumulative_trade - previous_trade
            non_trade_delta = account_delta - trade_delta
        else:
            residual_available = False
            account_delta = Decimal("0")
            trade_delta = Decimal("0")
            non_trade_delta = Decimal("0")
        row = {
            "time_ms": day_end,
            "day_start_ms": day_start,
            "day_end_ms": day_end,
            "day_index": day_index,
            "account_value_usd": last_value,
            "account_delta_usd": account_delta,
            "trade_delta_usd": trade_delta,
            "non_trade_delta_usd": non_trade_delta,
            "move_label": (
                account_move_label(
                    account_delta=account_delta,
                    trade_delta=trade_delta,
                    non_trade_delta=non_trade_delta,
                )
                if residual_available
                else "residual unavailable until a new account-value observation"
            ),
            "account_value_residual_usd": non_trade_delta,
            "residual_direction": residual_direction(non_trade_delta, available=residual_available),
            "residual_available": residual_available,
            "residual_cashflow_confirmed": False,
            # Legacy compatibility field: this is residual direction only,
            # not proof of a deposit or withdrawal.
            "cashflow_type": cashflow_type(non_trade_delta, available=residual_available),
            "attribution_complete": residual_available,
            "account_value_observed": account_value_observed,
            "account_value_carried_forward": not account_value_observed,
            "non_trade_delta_caveat": (
                "account-value delta minus compacted closed trade PnL; may include "
                "unrealized PnL, funding, fees, transfers, or other balance effects"
            ),
            "order_count": trade_point.get("order_count", 0),
            "trade_count": trade_point.get("trade_count", 0),
            "orders": trade_point.get("orders", []),
            "bucket_net_pnl_usd": trade_point.get("bucket_net_pnl_usd", Decimal("0")),
            "bucket_volume_usd": trade_point.get("bucket_volume_usd", Decimal("0")),
            "history_count": observed,
            "no_trade": not trade_point.get("order_count", 0),
        }
        points.append(row)
        if account_value_observed:
            previous_account = last_value
            previous_trade = cumulative_trade
    return points


def day_buckets(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    """Return right-closed daily intervals; the first interval also includes ``start_ms``."""

    if end_ms <= start_ms:
        return []
    count = max(1, (end_ms - start_ms + MS_PER_DAY - 1) // MS_PER_DAY)
    return [
        (
            start_ms + index * MS_PER_DAY,
            min(end_ms, start_ms + (index + 1) * MS_PER_DAY),
        )
        for index in range(count)
    ]


def residual_direction(non_trade_delta: Decimal, *, available: bool) -> str:
    if not available:
        return "unavailable"
    if non_trade_delta >= Decimal("1"):
        return "positive"
    if non_trade_delta <= Decimal("-1"):
        return "negative"
    return "none"


def cashflow_type(non_trade_delta: Decimal, *, available: bool = True) -> str:
    """Return the legacy residual-direction label; this does not prove a transfer."""

    if not available:
        return "none"
    if non_trade_delta >= Decimal("1"):
        return "injection"
    if non_trade_delta <= Decimal("-1"):
        return "outflow"
    return "none"


def cumulative_trade_points(
    trades: list[CompactTrade],
    *,
    limit: int = MAX_CHART_POINTS,
) -> list[dict[str, Any]]:
    if not trades:
        return []
    if limit <= 0 or len(trades) <= limit:
        return cumulative_trade_points_exact(trades)
    bucket_size = max(1, (len(trades) + limit - 1) // limit)
    total = Decimal("0")
    points: list[dict[str, Any]] = []
    for index in range(0, len(trades), bucket_size):
        bucket = trades[index : index + bucket_size]
        for trade in bucket:
            total += trade.net_pnl_usd
        orders = [trade_brief(trade) for trade in bucket[-MAX_POINT_ORDERS:]]
        points.append(
            {
                "time_ms": bucket[-1].time_ms,
                "pnl_usd": total,
                "order_count": len(bucket),
                "orders": orders,
                "bucket_net_pnl_usd": sum(
                    (trade.net_pnl_usd for trade in bucket),
                    Decimal("0"),
                ),
                "bucket_volume_usd": sum(
                    (trade.notional_usd for trade in bucket),
                    Decimal("0"),
                ),
            }
        )
    return points


def cumulative_trade_points_exact(trades: list[CompactTrade]) -> list[dict[str, Any]]:
    total = Decimal("0")
    points: list[dict[str, Any]] = []
    for trade in trades:
        total += trade.net_pnl_usd
        points.append(
            {
                "time_ms": trade.time_ms,
                "pnl_usd": total,
                "order_count": 1,
                "orders": [trade_brief(trade)],
                "bucket_net_pnl_usd": trade.net_pnl_usd,
                "bucket_volume_usd": trade.notional_usd,
            }
        )
    return points


def asset_stats(trades: list[CompactTrade]) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "coin": "",
            "trades": 0,
            "volume_usd": Decimal("0"),
            "closed_pnl_usd": Decimal("0"),
            "fees_usd": Decimal("0"),
            "net_pnl_usd": Decimal("0"),
        }
    )
    for trade in trades:
        row = stats[trade.coin]
        row["coin"] = trade.coin
        row["trades"] += 1
        row["volume_usd"] += trade.notional_usd
        row["closed_pnl_usd"] += trade.closed_pnl_usd
        row["fees_usd"] += trade.fee_usd
        row["net_pnl_usd"] += trade.net_pnl_usd
    total_volume = sum((row["volume_usd"] for row in stats.values()), Decimal("0"))
    rows = []
    for row in stats.values():
        rows.append(
            {
                **row,
                "volume_share_pct": (
                    row["volume_usd"] / total_volume * Decimal("100")
                    if total_volume > 0
                    else Decimal("0")
                ),
            }
        )
    rows.sort(key=lambda row: (row["volume_usd"], row["trades"]), reverse=True)
    return rows[:MAX_ASSET_ROWS]


def recent_trades(trades: list[CompactTrade]) -> list[dict[str, Any]]:
    return [
        trade_brief(trade) for trade in reversed(trades[-MAX_RECENT_TRADES:]) if trade is not None
    ]


@overload
def trade_brief(trade: CompactTrade) -> dict[str, Any]: ...


@overload
def trade_brief(trade: None) -> None: ...


def trade_brief(trade: CompactTrade | None) -> dict[str, Any] | None:
    if trade is None:
        return None
    return {
        "time_ms": trade.time_ms,
        "coin": trade.coin,
        "side": trade.side,
        "direction": trade.direction,
        "size": trade.size,
        "avg_price": trade.avg_price,
        "notional_usd": trade.notional_usd,
        "closed_pnl_usd": trade.closed_pnl_usd,
        "fee_usd": trade.fee_usd,
        "net_pnl_usd": trade.net_pnl_usd,
        "order_id": trade.order_id,
        "tx_hash": trade.tx_hash,
        "fill_count": trade.fill_count,
    }


def open_positions(clearinghouse: Any) -> list[dict[str, Any]]:
    if not isinstance(clearinghouse, Mapping):
        return []
    rows = []
    for item in clearinghouse.get("assetPositions", []) or []:
        if not isinstance(item, Mapping):
            continue
        position = item.get("position", item)
        if not isinstance(position, Mapping):
            continue
        raw_coin = str(position.get("coin") or "").strip()
        try:
            coin = canonical_market_symbol(raw_coin)
        except MarketIdentityError:
            coin = raw_coin.upper()
        size = _decimal(position.get("szi"))
        if not coin or size == 0:
            continue
        rows.append(
            {
                "coin": coin,
                "size": size,
                "side": "long" if size > 0 else "short",
                "entry_px": _decimal_or_none(position.get("entryPx")),
                "unrealized_pnl_usd": _decimal_or_none(position.get("unrealizedPnl")),
                "return_on_equity_pct": _decimal_or_none(position.get("returnOnEquity")),
                "leverage": _leverage_value(position.get("leverage")),
            }
        )
    return rows


def portfolio_windows_payload(portfolio: Any) -> dict[str, Mapping[str, Any]]:
    if isinstance(portfolio, Mapping):
        return {str(key): item for key, item in portfolio.items() if isinstance(item, Mapping)}
    if isinstance(portfolio, list):
        result: dict[str, Mapping[str, Any]] = {}
        for item in portfolio:
            if isinstance(item, list | tuple) and len(item) == 2 and isinstance(item[1], Mapping):
                result[str(item[0])] = item[1]
        return result
    return {}


def max_drawdown(
    points: Sequence[Mapping[str, Any]],
    *,
    value_key: str = "pnl_usd",
) -> dict[str, Any]:
    peak: Decimal | None = None
    peak_time = 0
    max_drop = Decimal("0")
    max_from = 0
    max_to = 0
    for point in points:
        value = _decimal(point.get(value_key))
        time_ms = _int(point.get("time_ms"))
        if peak is None or value > peak:
            peak = value
            peak_time = time_ms
        drop = (peak or Decimal("0")) - value
        if drop > max_drop:
            max_drop = drop
            max_from = peak_time
            max_to = time_ms
    pct = Decimal("0")
    if value_key == "account_value_usd" and peak and peak > 0:
        pct = max_drop / peak * Decimal("100")
    return {"drawdown_usd": max_drop, "drawdown_pct": pct, "from_ms": max_from, "to_ms": max_to}


def active_days(trades: list[CompactTrade]) -> int:
    return len({trade.time_ms // MS_PER_DAY for trade in trades})


def median_gap_minutes(trades: list[CompactTrade]) -> Decimal | None:
    if len(trades) < 2:
        return None
    gaps = [
        Decimal(str(next_trade.time_ms - trade.time_ms)) / Decimal("60000")
        for trade, next_trade in zip(trades, trades[1:])
        if next_trade.time_ms >= trade.time_ms
    ]
    if not gaps:
        return None
    return median(gaps)


def downsample(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(points) <= limit or limit <= 0:
        return points
    if limit == 1:
        return [points[-1]]
    step = (len(points) - 1) / (limit - 1)
    result: list[dict[str, Any]] = []
    seen: set[int] = set()
    for index in range(limit):
        source_index = round(index * step)
        if source_index in seen:
            continue
        seen.add(source_index)
        result.append(points[source_index])
    if result[-1] != points[-1]:
        result[-1] = points[-1]
    return result


def downsample_account_points(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if len(points) <= limit or limit <= 0:
        return points
    sampled = downsample(points, limit)
    flagged = [
        point for point in points if abs(_decimal(point.get("non_trade_delta_usd"))) >= Decimal("1")
    ]
    by_time = {point["time_ms"]: point for point in sampled if "time_ms" in point}
    for point in flagged:
        by_time[_int(point.get("time_ms"))] = point
    merged = sorted(by_time.values(), key=lambda point: _int(point.get("time_ms")))
    if len(merged) <= limit:
        return merged
    required_times = {_int(point.get("time_ms")) for point in flagged}
    required_times.add(_int(points[0].get("time_ms")))
    required_times.add(_int(points[-1].get("time_ms")))
    required = [point for point in merged if _int(point.get("time_ms")) in required_times]
    remaining = [point for point in merged if _int(point.get("time_ms")) not in required_times]
    return sorted(
        (required + downsample(remaining, max(0, limit - len(required))))[:limit],
        key=lambda point: _int(point.get("time_ms")),
    )


def portfolio_pnl_metric(
    portfolio_window: Mapping[str, Any],
    *,
    portfolio_window_label: str,
    start_ms: int,
    end_ms: int,
) -> PortfolioPnlMetric:
    raw_points = sorted(
        _history_points(portfolio_window.get("pnlHistory"), "pnl_usd"),
        key=lambda point: _int(point.get("time_ms")),
    )
    if not _uses_bounded_all_time_metric(
        portfolio_window_label,
        start_ms=start_ms,
        end_ms=end_ms,
    ):
        return PortfolioPnlMetric(
            points=tuple(raw_points),
            value_usd=_last_history_value(raw_points, "pnl_usd"),
            scope=portfolio_window_label,
            provenance="upstream_portfolio_window",
            partial=False,
            coverage_start_ms=_int(raw_points[0].get("time_ms")) if raw_points else 0,
            coverage_end_ms=_int(raw_points[-1].get("time_ms")) if raw_points else 0,
        )

    through_end = [point for point in raw_points if _int(point.get("time_ms")) <= end_ms]
    baseline_candidates = [point for point in through_end if _int(point.get("time_ms")) <= start_ms]
    in_window = [point for point in through_end if start_ms < _int(point.get("time_ms")) <= end_ms]
    baseline = baseline_candidates[-1] if baseline_candidates else None
    partial = baseline is None or not in_window
    if baseline is None and in_window:
        baseline = in_window.pop(0)
    if baseline is None:
        return PortfolioPnlMetric(
            points=(),
            value_usd=Decimal("0"),
            scope="requested_window",
            provenance="all_time_pnl_history_delta",
            partial=True,
            coverage_start_ms=0,
            coverage_end_ms=0,
        )

    baseline_value = _decimal(baseline.get("pnl_usd"))
    baseline_time = _int(baseline.get("time_ms"))
    rebased_points = [{"time_ms": max(start_ms, baseline_time), "pnl_usd": Decimal("0")}]
    rebased_points.extend(
        {
            "time_ms": _int(point.get("time_ms")),
            "pnl_usd": _decimal(point.get("pnl_usd")) - baseline_value,
        }
        for point in in_window
    )
    coverage_end_ms = _int(in_window[-1].get("time_ms")) if in_window else baseline_time
    return PortfolioPnlMetric(
        points=tuple(rebased_points),
        value_usd=_decimal(rebased_points[-1].get("pnl_usd")),
        scope="requested_window",
        provenance="all_time_pnl_history_delta",
        partial=partial,
        coverage_start_ms=baseline_time,
        coverage_end_ms=coverage_end_ms,
    )


def _uses_bounded_all_time_metric(
    portfolio_window_label: str,
    *,
    start_ms: int,
    end_ms: int,
) -> bool:
    normalized_label = portfolio_window_label.replace("_", "").lower()
    return normalized_label in {"alltime", "all"} and _window_days(
        start_ms,
        end_ms,
    ) > Decimal("30")


def select_portfolio_window(
    windows: Mapping[str, Mapping[str, Any]],
    *,
    window_days: int,
) -> tuple[str, Mapping[str, Any]]:
    normalized = {key.lower(): (key, value) for key, value in windows.items()}
    preferences = (
        ("day", "day"),
        ("week", "week"),
        ("month", "month"),
        ("alltime", "allTime"),
        ("all_time", "allTime"),
        ("all", "allTime"),
    )
    if window_days <= 1:
        order = ("day", "week", "month", "alltime", "all_time", "all")
    elif window_days <= 7:
        order = ("week", "month", "alltime", "all_time", "all", "day")
    elif window_days <= 30:
        order = ("month", "alltime", "all_time", "all", "week", "day")
    else:
        order = ("alltime", "all_time", "all", "month", "week", "day")
    labels = dict(preferences)
    for key in order:
        if key in normalized:
            original, payload = normalized[key]
            return labels.get(key, original), payload
    return "none", {}


def annotate_account_value_moves(
    account_values: list[dict[str, Any]],
    trade_points: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not account_values:
        return []
    result: list[dict[str, Any]] = []
    trade_index = 0
    cumulative_trade = Decimal("0")

    def trade_pnl_through(time_ms: int) -> Decimal:
        nonlocal trade_index, cumulative_trade
        while (
            trade_index < len(trade_points)
            and _int(trade_points[trade_index].get("time_ms")) <= time_ms
        ):
            cumulative_trade = _decimal(trade_points[trade_index].get("pnl_usd"))
            trade_index += 1
        return cumulative_trade

    previous_account = _decimal(account_values[0].get("account_value_usd"))
    previous_trade = trade_pnl_through(_int(account_values[0].get("time_ms")))
    first = dict(account_values[0])
    first.update(
        {
            "account_delta_usd": Decimal("0"),
            "trade_delta_usd": Decimal("0"),
            "non_trade_delta_usd": Decimal("0"),
            "move_label": "starting account value",
        }
    )
    result.append(first)
    for point in account_values[1:]:
        time_ms = _int(point.get("time_ms"))
        account_value = _decimal(point.get("account_value_usd"))
        trade_pnl = trade_pnl_through(time_ms)
        account_delta = account_value - previous_account
        trade_delta = trade_pnl - previous_trade
        non_trade_delta = account_delta - trade_delta
        row = dict(point)
        row.update(
            {
                "account_delta_usd": account_delta,
                "trade_delta_usd": trade_delta,
                "non_trade_delta_usd": non_trade_delta,
                "move_label": account_move_label(
                    account_delta=account_delta,
                    trade_delta=trade_delta,
                    non_trade_delta=non_trade_delta,
                ),
            }
        )
        result.append(row)
        previous_account = account_value
        previous_trade = trade_pnl
    return result


def account_move_label(
    *,
    account_delta: Decimal,
    trade_delta: Decimal,
    non_trade_delta: Decimal,
) -> str:
    if abs(account_delta) < Decimal("1") and abs(non_trade_delta) < Decimal("1"):
        return "flat account value"
    if account_delta < 0 and abs(non_trade_delta) >= max(
        Decimal("1"), abs(account_delta) * Decimal("0.45")
    ):
        return "drop mostly from unexplained/non-trade account-value residual"
    if account_delta > 0 and abs(non_trade_delta) >= max(
        Decimal("1"), abs(account_delta) * Decimal("0.45")
    ):
        return "rise mostly from unexplained/non-trade account-value residual"
    if account_delta < 0 and trade_delta < 0:
        return "drop mostly from trading PnL"
    if account_delta > 0 and trade_delta > 0:
        return "rise mostly from trading PnL"
    return "mixed trading and non-trade move"


def normalize_address(address: str) -> str:
    return (address or "").strip().lower()


def valid_address(address: str) -> bool:
    normalized = normalize_address(address)
    return (
        len(normalized) == 42
        and normalized.startswith("0x")
        and all(char in "0123456789abcdef" for char in normalized[2:])
    )


def _try_info(
    fetch_info_json: FetchInfoJSON,
    base_url: str,
    payload: Mapping[str, Any],
    timeout_s: Decimal,
    warnings: list[str],
    label: str,
) -> Any:
    try:
        return fetch_info_json(base_url, payload, timeout_s)
    except Exception as exc:
        warnings.append(f"{label} unavailable: {exc}")
        return None


def _history_points(value: Any, value_key: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    points = []
    for item in value:
        if not isinstance(item, list | tuple) or len(item) != 2:
            continue
        points.append({"time_ms": _int(item[0]), value_key: _decimal(item[1])})
    return points


def _last_history_value(points: Sequence[Mapping[str, Any]], value_key: str) -> Decimal:
    if not points:
        return Decimal("0")
    return _decimal(points[-1].get(value_key))


def _current_account_value_context(
    clearinghouse: Any,
    account_values: Sequence[Mapping[str, Any]],
) -> tuple[Decimal, str, str, bool]:
    clearinghouse_values: list[tuple[Decimal, str]] = []
    if isinstance(clearinghouse, Mapping):
        for key in ("marginSummary", "crossMarginSummary"):
            summary = clearinghouse.get(key)
            if isinstance(summary, Mapping) and summary.get("accountValue") is not None:
                value = _decimal(summary.get("accountValue"))
                source = (
                    "clearinghouse_margin_summary"
                    if key == "marginSummary"
                    else "clearinghouse_cross_margin_summary"
                )
                if value > 0:
                    return value, source, "high", False
                clearinghouse_values.append((value, source))

    history_value = _last_history_value(account_values, "account_value_usd")
    if history_value > 0:
        return history_value, "portfolio_account_value_history_latest", "medium", True
    if clearinghouse_values:
        value, source = clearinghouse_values[0]
        return value, source, "low", False
    if account_values:
        return history_value, "portfolio_account_value_history_latest", "low", True
    return Decimal("0"), "unavailable", "low", False


def _leverage_value(value: Any) -> str:
    if isinstance(value, Mapping):
        leverage_type = str(value.get("type") or "").strip()
        raw_value = value.get("value")
        if leverage_type and raw_value not in (None, ""):
            return f"{leverage_type}:{raw_value}"
        if raw_value not in (None, ""):
            return str(raw_value)
        return leverage_type
    return "" if value in (None, "") else str(value)


def _normalize_side(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"b", "buy", "bid"}:
        return "buy"
    if raw in {"a", "ask", "sell"}:
        return "sell"
    return raw or "unknown"


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return parse_decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _decimal_div(numerator: int | Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return Decimal(str(numerator)) / denominator


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _window_days(start_ms: int, end_ms: int) -> Decimal:
    return Decimal(str(max(0, end_ms - start_ms))) / Decimal(str(MS_PER_DAY))


def _window_day_count(start_ms: int, end_ms: int) -> int:
    duration_ms = max(0, end_ms - start_ms)
    return max(1, (duration_ms + MS_PER_DAY - 1) // MS_PER_DAY)
