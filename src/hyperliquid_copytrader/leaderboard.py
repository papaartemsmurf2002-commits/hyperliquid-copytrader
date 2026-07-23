from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from threading import Lock
from typing import Any
from urllib.request import Request, urlopen

from .config import MAX_LEADERBOARD_ROWS, LeaderboardConfig
from .models import now_ms, parse_decimal, to_jsonable


FetchJSON = Callable[[str, Decimal], Any]
MAX_DISPLAY_NAME_CHARS = 64
DEFAULT_MIN_VOLUME_USD = Decimal("100000")
DEFAULT_MIN_ACCOUNT_VALUE_USD = Decimal("2000")


@dataclass(frozen=True)
class LeaderboardRow:
    rank: int
    address: str
    display_name: str
    account_value_usd: Decimal
    pnl_30d_usd: Decimal
    roi_30d_pct: Decimal
    volume_30d_usd: Decimal
    pnl_7d_usd: Decimal
    roi_7d_pct: Decimal
    volume_7d_usd: Decimal
    day_roi_pct: Decimal
    day_volume_usd: Decimal
    month_roi_pct: Decimal
    all_time_roi_pct: Decimal
    day_pnl_usd: Decimal
    month_pnl_usd: Decimal
    all_time_pnl_usd: Decimal
    chart: list[dict[str, Any]]
    chart_status: str
    chart_source: str


class LeaderboardService:
    def __init__(
        self,
        config: LeaderboardConfig,
        *,
        fetch_json: FetchJSON | None = None,
        clock_ms: Callable[[], int] = now_ms,
    ) -> None:
        self.config = config
        self._fetch_json = fetch_json or fetch_public_json
        self._clock_ms = clock_ms
        self._lock = Lock()
        self._cache_payload: Any | None = None
        self._cache_ms = 0

    def snapshot(
        self,
        *,
        force_refresh: bool = False,
        limit: int | None = None,
        min_volume_usd: Decimal | None = None,
        min_account_value_usd: Decimal | None = None,
    ) -> dict[str, Any]:
        observed = self._clock_ms()
        effective_limit = _effective_limit(self.config.limit if limit is None else limit)
        effective_min_volume = _nonnegative_decimal(
            self.config.min_volume_usd if min_volume_usd is None else min_volume_usd,
            "minimum 30D volume",
        )
        configured_min_account = getattr(
            self.config,
            "min_account_value_usd",
            DEFAULT_MIN_ACCOUNT_VALUE_USD,
        )
        effective_min_account = _nonnegative_decimal(
            configured_min_account if min_account_value_usd is None else min_account_value_usd,
            "minimum account value",
        )
        if not self.config.enabled:
            return self._empty_snapshot(
                status="disabled",
                observed=observed,
                warnings=["leaderboard disabled"],
                limit=effective_limit,
                min_volume_usd=effective_min_volume,
                min_account_value_usd=effective_min_account,
            )

        if not self._lock.acquire(blocking=False):
            cached = self._cached_snapshot(
                status="refreshing",
                observed=observed,
                warning="refresh already in progress",
                limit=effective_limit,
                min_volume_usd=effective_min_volume,
                min_account_value_usd=effective_min_account,
            )
            if cached is not None:
                return cached
            return self._empty_snapshot(
                status="refreshing",
                observed=observed,
                warnings=["refresh already in progress"],
                limit=effective_limit,
                min_volume_usd=effective_min_volume,
                min_account_value_usd=effective_min_account,
            )

        try:
            cache_fresh = (
                self._cache_payload is not None
                and observed - self._cache_ms <= self.config.cache_ttl_ms
            )
            if cache_fresh and not force_refresh:
                cached = self._cached_snapshot(
                    status="cached",
                    observed=observed,
                    limit=effective_limit,
                    min_volume_usd=effective_min_volume,
                    min_account_value_usd=effective_min_account,
                )
                if cached is not None:
                    return cached

            try:
                payload = self._fetch_json(self.config.url, self.config.timeout_s)
                snapshot = self._snapshot_from_payload(
                    payload,
                    status="fresh",
                    observed=observed,
                    cache_age_ms=0,
                    limit=effective_limit,
                    min_volume_usd=effective_min_volume,
                    min_account_value_usd=effective_min_account,
                )
            except Exception as exc:
                cached = self._cached_snapshot(
                    status="stale",
                    observed=observed,
                    warning=f"refresh failed: {exc}",
                    limit=effective_limit,
                    min_volume_usd=effective_min_volume,
                    min_account_value_usd=effective_min_account,
                )
                if cached is not None:
                    return cached
                return self._empty_snapshot(
                    status="error",
                    observed=observed,
                    warnings=[str(exc)],
                    limit=effective_limit,
                    min_volume_usd=effective_min_volume,
                    min_account_value_usd=effective_min_account,
                )

            self._cache_ms = observed
            self._cache_payload = payload
            return snapshot
        finally:
            self._lock.release()

    def local_snapshot(
        self,
        *,
        limit: int | None = None,
        min_volume_usd: Decimal | None = None,
        min_account_value_usd: Decimal | None = None,
    ) -> dict[str, Any]:
        """Return only in-process cache state; never perform an external refresh."""

        observed = self._clock_ms()
        effective_limit = _effective_limit(self.config.limit if limit is None else limit)
        effective_min_volume = _nonnegative_decimal(
            self.config.min_volume_usd if min_volume_usd is None else min_volume_usd,
            "minimum 30D volume",
        )
        configured_min_account = getattr(
            self.config,
            "min_account_value_usd",
            DEFAULT_MIN_ACCOUNT_VALUE_USD,
        )
        effective_min_account = _nonnegative_decimal(
            configured_min_account if min_account_value_usd is None else min_account_value_usd,
            "minimum account value",
        )
        cached = self._cached_snapshot(
            status="local_cache",
            observed=observed,
            warning="external analytics refresh is disabled in the fleet-capable UI",
            limit=effective_limit,
            min_volume_usd=effective_min_volume,
            min_account_value_usd=effective_min_account,
        )
        if cached is not None:
            return cached
        return self._empty_snapshot(
            status="local_only_unavailable",
            observed=observed,
            warnings=[
                "no local leaderboard cache is available; external refresh is disabled in the "
                "fleet-capable UI"
            ],
            limit=effective_limit,
            min_volume_usd=effective_min_volume,
            min_account_value_usd=effective_min_account,
        )

    def _cached_snapshot(
        self,
        *,
        status: str,
        observed: int,
        limit: int,
        min_volume_usd: Decimal,
        min_account_value_usd: Decimal,
        warning: str | None = None,
    ) -> dict[str, Any] | None:
        if self._cache_payload is None:
            return None
        return self._snapshot_from_payload(
            self._cache_payload,
            status=status,
            observed=observed,
            cache_age_ms=max(0, observed - self._cache_ms),
            limit=limit,
            min_volume_usd=min_volume_usd,
            min_account_value_usd=min_account_value_usd,
            warning=warning,
        )

    def _snapshot_from_payload(
        self,
        payload: Any,
        *,
        status: str,
        observed: int,
        cache_age_ms: int,
        limit: int,
        min_volume_usd: Decimal,
        min_account_value_usd: Decimal,
        warning: str | None = None,
    ) -> dict[str, Any]:
        rows, warnings, metadata = _normalize_leaderboard_rows(
            payload,
            limit=limit,
            min_volume_usd=min_volume_usd,
            min_account_value_usd=min_account_value_usd,
        )
        if warning is not None:
            warnings.append(warning)
        return {
            "status": status,
            "source": self.config.url,
            "generated_ms": observed,
            "cache_age_ms": cache_age_ms,
            "window": "30d",
            "sort": "roi_30d_pct_desc",
            "limit": limit,
            "active_volume_filter_usd": to_jsonable(min_volume_usd),
            "active_account_value_filter_usd": to_jsonable(min_account_value_usd),
            "eligibility": metadata["eligibility"],
            "counts": metadata["counts"],
            "chart_metadata": metadata["chart"],
            "rows": [to_jsonable(row) for row in rows],
            "warnings": warnings[:10],
        }

    def _empty_snapshot(
        self,
        *,
        status: str,
        observed: int,
        warnings: list[str],
        limit: int,
        min_volume_usd: Decimal,
        min_account_value_usd: Decimal,
    ) -> dict[str, Any]:
        metadata = _empty_metadata(
            limit=limit,
            min_volume_usd=min_volume_usd,
            min_account_value_usd=min_account_value_usd,
        )
        return {
            "status": status,
            "source": self.config.url,
            "generated_ms": observed,
            "cache_age_ms": None,
            "window": "30d",
            "sort": "roi_30d_pct_desc",
            "limit": limit,
            "active_volume_filter_usd": to_jsonable(min_volume_usd),
            "active_account_value_filter_usd": to_jsonable(min_account_value_usd),
            "eligibility": metadata["eligibility"],
            "counts": metadata["counts"],
            "chart_metadata": metadata["chart"],
            "rows": [],
            "warnings": warnings,
        }


def fetch_public_json(url: str, timeout_s: Decimal) -> Any:
    request = Request(
        url, headers={"Accept": "application/json", "User-Agent": "hl-copytrader/0.1"}
    )
    with urlopen(request, timeout=float(timeout_s)) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8"))


def normalize_leaderboard_rows(
    payload: Any,
    *,
    limit: int = MAX_LEADERBOARD_ROWS,
    min_volume_usd: Decimal = DEFAULT_MIN_VOLUME_USD,
    min_account_value_usd: Decimal = DEFAULT_MIN_ACCOUNT_VALUE_USD,
) -> tuple[list[LeaderboardRow], list[str]]:
    rows, warnings, _metadata = _normalize_leaderboard_rows(
        payload,
        limit=limit,
        min_volume_usd=min_volume_usd,
        min_account_value_usd=min_account_value_usd,
    )
    return rows, warnings


def _normalize_leaderboard_rows(
    payload: Any,
    *,
    limit: int,
    min_volume_usd: Decimal,
    min_account_value_usd: Decimal,
) -> tuple[list[LeaderboardRow], list[str], dict[str, Any]]:
    rows_payload = _leaderboard_rows_payload(payload)
    warnings: list[str] = []
    candidates: list[LeaderboardRow] = []
    effective_limit = _effective_limit(limit)
    effective_min_volume = _nonnegative_decimal(min_volume_usd, "minimum 30D volume")
    effective_min_account = _nonnegative_decimal(
        min_account_value_usd,
        "minimum account value",
    )
    counts = {
        "source_rows": len(rows_payload),
        "valid_rows": 0,
        "eligible_rows": 0,
        "returned_rows": 0,
        "filtered_out_rows": 0,
        "below_min_volume_rows": 0,
        "below_min_account_value_rows": 0,
        "invalid_address_rows": 0,
        "malformed_rows": 0,
        "truncated_rows": 0,
        "rows_with_reported_chart": 0,
        "rows_without_reported_chart": 0,
    }

    for raw in rows_payload:
        if not isinstance(raw, Mapping):
            counts["malformed_rows"] += 1
            warnings.append("skipped non-object leaderboard row")
            continue
        try:
            performances = _window_performances(raw.get("windowPerformances"))
            month = performances["month"]
            week = performances["week"]
            volume = _finite_decimal(month.get("vlm"))
            account_value = _finite_decimal(raw.get("accountValue"))
            month_roi = _finite_decimal(month.get("roi"))
            month_pnl = _finite_decimal(month.get("pnl"))
            address = str(raw.get("ethAddress") or raw.get("address") or "").lower()
            if not _valid_address(address):
                counts["invalid_address_rows"] += 1
                warnings.append("skipped row with invalid address")
                continue
            counts["valid_rows"] += 1
            volume_eligible = volume >= effective_min_volume
            account_eligible = account_value >= effective_min_account
            if not volume_eligible:
                counts["below_min_volume_rows"] += 1
            if not account_eligible:
                counts["below_min_account_value_rows"] += 1
            if not volume_eligible or not account_eligible:
                counts["filtered_out_rows"] += 1
                continue
            day = performances.get("day", {})
            all_time = performances.get("allTime", {})
            chart, chart_source, chart_warning = _reported_30d_chart(raw)
            if chart_warning:
                warnings.append(chart_warning)
            if chart:
                counts["rows_with_reported_chart"] += 1
            else:
                counts["rows_without_reported_chart"] += 1
            row = LeaderboardRow(
                rank=0,
                address=address,
                display_name=_display_name(raw.get("displayName")),
                account_value_usd=account_value,
                pnl_30d_usd=month_pnl,
                roi_30d_pct=month_roi * Decimal("100"),
                volume_30d_usd=volume,
                pnl_7d_usd=_decimal(week.get("pnl")),
                roi_7d_pct=_decimal(week.get("roi")) * Decimal("100"),
                volume_7d_usd=_decimal(week.get("vlm")),
                day_roi_pct=_decimal(day.get("roi")) * Decimal("100"),
                day_volume_usd=_decimal(day.get("vlm")),
                month_roi_pct=month_roi * Decimal("100"),
                all_time_roi_pct=_decimal(all_time.get("roi")) * Decimal("100"),
                day_pnl_usd=_decimal(day.get("pnl")),
                month_pnl_usd=month_pnl,
                all_time_pnl_usd=_decimal(all_time.get("pnl")),
                chart=chart,
                chart_status="reported" if chart else "unavailable",
                chart_source=chart_source,
            )
        except (KeyError, InvalidOperation, ValueError) as exc:
            counts["malformed_rows"] += 1
            warnings.append(f"skipped malformed leaderboard row: {exc}")
            continue
        candidates.append(row)

    candidates.sort(
        key=lambda row: (row.roi_30d_pct, row.pnl_30d_usd, row.volume_30d_usd),
        reverse=True,
    )
    ranked = [
        LeaderboardRow(**{**row.__dict__, "rank": rank})
        for rank, row in enumerate(candidates[:effective_limit], start=1)
    ]
    counts["eligible_rows"] = len(candidates)
    counts["returned_rows"] = len(ranked)
    counts["truncated_rows"] = max(0, len(candidates) - len(ranked))
    metadata = _metadata(
        limit=effective_limit,
        min_volume_usd=effective_min_volume,
        min_account_value_usd=effective_min_account,
        counts=counts,
    )
    return ranked, warnings[:10], metadata


def _leaderboard_rows_payload(payload: Any) -> list[Any]:
    if isinstance(payload, Mapping):
        rows = payload.get("leaderboardRows") or payload.get("rows") or payload.get("data")
        if isinstance(rows, list):
            return rows
    if isinstance(payload, list):
        return payload
    raise ValueError("leaderboard payload does not contain rows")


def _window_performances(value: Any) -> dict[str, Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items() if isinstance(item, Mapping)}
    if isinstance(value, list):
        result: dict[str, Mapping[str, Any]] = {}
        for item in value:
            if isinstance(item, list) and len(item) == 2 and isinstance(item[1], Mapping):
                result[str(item[0])] = item[1]
        return result
    raise ValueError("missing window performances")


def _reported_30d_chart(
    row: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str, str | None]:
    """Parse explicit upstream 30D ROI observations without synthesizing missing days."""

    source = ""
    raw_points: Any = None
    for key in ("roiHistory30d", "roi_history_30d"):
        if key in row:
            source = key
            raw_points = row.get(key)
            break
    if raw_points is None:
        return [], "", None
    if not isinstance(raw_points, list):
        return [], source, f"ignored malformed {source} chart history"

    points_by_time: dict[int, dict[str, Any]] = {}
    for raw_point in raw_points:
        if not isinstance(raw_point, Mapping):
            return [], source, f"ignored malformed {source} chart history"
        try:
            timestamp = _positive_int(
                raw_point.get("timestamp_ms")
                or raw_point.get("timestamp")
                or raw_point.get("time")
                or raw_point.get("ts")
            )
            if timestamp is None:
                return [], source, f"ignored malformed {source} chart history"
            if "roi_pct" in raw_point:
                roi_pct = _finite_decimal(raw_point.get("roi_pct"))
            elif "roi" in raw_point:
                roi_pct = _finite_decimal(raw_point.get("roi")) * Decimal("100")
            else:
                return [], source, f"ignored malformed {source} chart history"
            point: dict[str, Any] = {
                "timestamp_ms": timestamp,
                "label": str(raw_point.get("label") or timestamp),
                "roi_pct": roi_pct,
                "reported": True,
                "estimated": False,
            }
            for source_keys, output_key in (
                (("pnl_usd", "pnl"), "pnl_usd"),
                (("volume_usd", "vlm"), "volume_usd"),
            ):
                for source_key in source_keys:
                    if source_key in raw_point:
                        point[output_key] = _finite_decimal(raw_point.get(source_key))
                        break
        except (InvalidOperation, TypeError, ValueError):
            return [], source, f"ignored malformed {source} chart history"
        points_by_time[timestamp] = point
    return [points_by_time[key] for key in sorted(points_by_time)], source, None


def _metadata(
    *,
    limit: int,
    min_volume_usd: Decimal,
    min_account_value_usd: Decimal,
    counts: dict[str, int],
) -> dict[str, Any]:
    return {
        "eligibility": {
            "window": "30d",
            "inclusive": True,
            "min_volume_30d_usd": to_jsonable(min_volume_usd),
            "min_account_value_usd": to_jsonable(min_account_value_usd),
            "ranking": "roi_30d_pct_desc",
            "limit": limit,
            "filters_applied_before_ranking_limit": True,
        },
        "counts": dict(counts),
        "chart": {
            "window": "30d",
            "policy": "reported_history_only",
            "fabricated_points": False,
            "rows_with_reported_history": counts["rows_with_reported_chart"],
            "rows_without_reported_history": counts["rows_without_reported_chart"],
        },
    }


def _empty_metadata(
    *,
    limit: int,
    min_volume_usd: Decimal,
    min_account_value_usd: Decimal,
) -> dict[str, Any]:
    counts = {
        "source_rows": 0,
        "valid_rows": 0,
        "eligible_rows": 0,
        "returned_rows": 0,
        "filtered_out_rows": 0,
        "below_min_volume_rows": 0,
        "below_min_account_value_rows": 0,
        "invalid_address_rows": 0,
        "malformed_rows": 0,
        "truncated_rows": 0,
        "rows_with_reported_chart": 0,
        "rows_without_reported_chart": 0,
    }
    return _metadata(
        limit=limit,
        min_volume_usd=min_volume_usd,
        min_account_value_usd=min_account_value_usd,
        counts=counts,
    )


def _effective_limit(value: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("leaderboard limit must be an integer") from exc
    return max(0, min(parsed, MAX_LEADERBOARD_ROWS))


def _nonnegative_decimal(value: Any, label: str) -> Decimal:
    parsed = _finite_decimal(value)
    if parsed < 0:
        raise ValueError(f"{label} must be non-negative")
    return parsed


def _finite_decimal(value: Any) -> Decimal:
    parsed = parse_decimal(value)
    if parsed is None or not parsed.is_finite():
        raise ValueError("numeric value must be finite")
    return parsed


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return parse_decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _display_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    text = "".join(ch if ch.isprintable() else " " for ch in text)
    text = text.replace("<", "").replace(">", "")
    text = " ".join(text.split())
    return text[:MAX_DISPLAY_NAME_CHARS]


def _valid_address(value: str) -> bool:
    return (
        len(value) == 42
        and value.startswith("0x")
        and all(c in "0123456789abcdef" for c in value[2:])
    )
