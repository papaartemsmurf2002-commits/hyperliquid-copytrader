from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


API_URL = "https://api.hyperliquid.xyz/info"
MS_PER_DAY = 86_400_000
FILL_PAGE_LIMIT = 2000
MIN_NOTIONAL = Decimal("10")
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


SOURCES = [
    "0xb1039883265d21395850d6d9bc4d7d141cc41343",
    "0x0e708a906c47925d07ab25ca55f57be55bf56842",
    "0xcd87ea212314217b6aa64fdffb9954330db5de4f",
    "0x337189f12dccb10013de352f56ba34dc91b580d3",
    "0x8360ca41abec39c46323f90c41b963c7e3251590",
    "0x192dfd9c08cd9e17cc695913bca39b36ec425324",
    "0xf12474d3dc642f9712d94643477affebeebcd738",
    "0x0526345bf8e09eb32256008c2844c8949ee3bb9a",
    "0xc73b427d778bc04728843dc62895b472d7ac1e37",
    "0xf5b0af852e3dedc03b551f7050b616b5c77c7645",
]


@dataclass
class Strategy:
    name: str
    cap_leverage: Decimal | None = None
    min_notional: Decimal = MIN_NOTIONAL
    dynamic_equity: bool = True
    sizing_equity_cap: Decimal | None = None


STRATEGIES = [
    Strategy("risk_budget_50_cap_20x", cap_leverage=Decimal("20"), sizing_equity_cap=Decimal("50")),
    Strategy("risk_budget_50_cap_10x", cap_leverage=Decimal("10"), sizing_equity_cap=Decimal("50")),
    Strategy("risk_budget_50_cap_5x", cap_leverage=Decimal("5"), sizing_equity_cap=Decimal("50")),
    Strategy("risk_budget_50_cap_3x", cap_leverage=Decimal("3"), sizing_equity_cap=Decimal("50")),
    Strategy("risk_budget_50_cap_2x", cap_leverage=Decimal("2"), sizing_equity_cap=Decimal("50")),
    Strategy("risk_budget_50_cap_1x", cap_leverage=Decimal("1"), sizing_equity_cap=Decimal("50")),
    Strategy("compound_cap_10x", cap_leverage=Decimal("10")),
    Strategy(
        "fixed_start_ratio_cap_10x",
        cap_leverage=Decimal("10"),
        dynamic_equity=False,
        sizing_equity_cap=Decimal("50"),
    ),
]


def decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def post_info(payload: dict[str, Any], timeout_s: int, retries: int = 5) -> Any:
    request = Request(
        API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "hl-copytrader-backtest/0.1",
        },
        method="POST",
    )
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout_s) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            retryable = exc.code in {429, 500, 502, 503, 504}
            if not retryable or attempt >= retries:
                raise
            retry_after = exc.headers.get("Retry-After")
            try:
                delay = Decimal(retry_after) if retry_after else Decimal(2 + attempt * 2)
            except InvalidOperation:
                delay = Decimal(2 + attempt * 2)
        except URLError:
            if attempt >= retries:
                raise
            delay = Decimal(2 + attempt * 2)
        time.sleep(float(delay))
    raise RuntimeError("unreachable retry state")


def fetch_fills(
    address: str, start_ms: int, end_ms: int, max_pages: int, timeout_s: int
) -> tuple[list[dict[str, Any]], int, bool]:
    page_start = start_ms
    pages = 0
    fills: list[dict[str, Any]] = []
    truncated = False
    while page_start <= end_ms and pages < max_pages:
        page = post_info(
            {
                "type": "userFillsByTime",
                "user": address,
                "startTime": page_start,
                "endTime": end_ms,
                "aggregateByTime": True,
            },
            timeout_s,
        )
        if not isinstance(page, list):
            raise RuntimeError("userFillsByTime did not return a list")
        pages += 1
        if not page:
            break
        valid = [item for item in page if isinstance(item, dict)]
        fills.extend(valid)
        max_seen = max((int(item.get("time") or page_start) for item in valid), default=page_start)
        if len(page) < FILL_PAGE_LIMIT:
            break
        if max_seen <= page_start:
            truncated = True
            break
        page_start = max_seen + 1
    if page_start <= end_ms and pages >= max_pages:
        truncated = True
    fills.sort(
        key=lambda item: (
            int(item.get("time") or 0),
            str(item.get("oid") or ""),
            str(item.get("hash") or ""),
        )
    )
    return fills, pages, truncated


def portfolio_windows(value: Any) -> dict[str, dict[str, Any]]:
    if isinstance(value, dict):
        return {str(key): item for key, item in value.items() if isinstance(item, dict)}
    if isinstance(value, list):
        result: dict[str, dict[str, Any]] = {}
        for item in value:
            if isinstance(item, list) and len(item) == 2 and isinstance(item[1], dict):
                result[str(item[0])] = item[1]
        return result
    return {}


def history_points(portfolio: dict[str, Any], key: str) -> list[tuple[int, Decimal]]:
    raw = portfolio.get(key)
    if not isinstance(raw, list):
        return []
    points: list[tuple[int, Decimal]] = []
    for item in raw:
        if isinstance(item, list) and len(item) == 2:
            points.append((int(item[0] or 0), decimal(item[1])))
    points.sort(key=lambda point: point[0])
    return points


def choose_portfolio_window(
    windows: dict[str, dict[str, Any]], days: int
) -> tuple[str, dict[str, Any]]:
    normalized = {key.lower(): (key, value) for key, value in windows.items()}
    order = (
        ["alltime", "all_time", "all", "month", "week", "day"]
        if days > 30
        else ["month", "week", "day", "alltime"]
    )
    for key in order:
        if key in normalized:
            return normalized[key]
    return "none", {}


def value_at(points: list[tuple[int, Decimal]], time_ms: int, fallback: Decimal) -> Decimal:
    value = fallback
    for point_time, point_value in points:
        if point_time > time_ms:
            break
        if point_value > 0:
            value = point_value
    return value if value > 0 else fallback


def max_drawdown(equity_curve: list[tuple[int, Decimal]]) -> Decimal:
    peak: Decimal | None = None
    worst = Decimal("0")
    for _time_ms, value in equity_curve:
        if peak is None or value > peak:
            peak = value
        drop = (peak or value) - value
        if drop > worst:
            worst = drop
    return worst


def simulate(
    fills: list[dict[str, Any]],
    account_values: list[tuple[int, Decimal]],
    *,
    initial_equity: Decimal,
    strategy: Strategy,
) -> dict[str, Any]:
    equity = initial_equity
    start_source_value = (
        value_at(account_values, int(fills[0].get("time") or 0), Decimal("0"))
        if fills
        else Decimal("0")
    )
    equity_curve = [(int(fills[0].get("time") or 0), equity)] if fills else [(0, equity)]
    copied = 0
    skipped_min = 0
    capped = 0
    liquidated = False
    min_equity = equity
    max_effective_leverage = Decimal("0")
    total_copied_notional = Decimal("0")
    copied_net_pnl = Decimal("0")
    source_net_pnl = Decimal("0")

    for fill in fills:
        time_ms = int(fill.get("time") or 0)
        source_value = (
            start_source_value
            if not strategy.dynamic_equity
            else value_at(account_values, time_ms, start_source_value)
        )
        sizing_equity = equity
        if strategy.sizing_equity_cap is not None:
            sizing_equity = min(sizing_equity, strategy.sizing_equity_cap)
        if source_value <= 0 or equity <= 0 or sizing_equity <= 0:
            continue
        source_notional = decimal(fill.get("px")) * decimal(fill.get("sz"))
        if source_notional <= 0:
            continue
        ratio = sizing_equity / source_value
        copied_notional = source_notional * ratio
        effective_leverage = copied_notional / sizing_equity if sizing_equity > 0 else Decimal("0")
        if strategy.cap_leverage is not None and effective_leverage > strategy.cap_leverage:
            copied_notional = sizing_equity * strategy.cap_leverage
            ratio = copied_notional / source_notional
            effective_leverage = strategy.cap_leverage
            capped += 1
        if copied_notional < strategy.min_notional:
            skipped_min += 1
            continue
        source_pnl = decimal(fill.get("closedPnl")) - decimal(fill.get("fee"))
        follower_pnl = source_pnl * ratio
        if equity + follower_pnl <= 0:
            follower_pnl = -equity
            liquidated = True
        equity += follower_pnl
        copied_net_pnl += follower_pnl
        source_net_pnl += source_pnl
        total_copied_notional += copied_notional
        max_effective_leverage = max(max_effective_leverage, effective_leverage)
        min_equity = min(min_equity, equity)
        copied += 1
        equity_curve.append((time_ms, equity))
        if liquidated:
            break

    return {
        "strategy": strategy.name,
        "initial_equity_usd": initial_equity,
        "ending_equity_usd": equity,
        "net_pnl_usd": copied_net_pnl,
        "roi_pct": (copied_net_pnl / initial_equity * Decimal("100"))
        if initial_equity
        else Decimal("0"),
        "max_drawdown_usd": max_drawdown(equity_curve),
        "min_equity_usd": min_equity,
        "max_effective_leverage": max_effective_leverage,
        "copied_fills": copied,
        "skipped_min_notional_fills": skipped_min,
        "capped_fills": capped,
        "copied_notional_usd": total_copied_notional,
        "source_net_pnl_seen_usd": source_net_pnl,
        "liquidated_or_zero_equity": liquidated,
    }


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    return value


def load_source_addresses(
    *,
    sources_file: Path | None,
    sources_csv: str,
    max_sources: int | None,
) -> list[str]:
    if sources_file is None and not sources_csv.strip():
        addresses = list(SOURCES)
    else:
        addresses = []
        if sources_file is not None:
            text = sources_file.read_text(encoding="utf-8")
            addresses.extend(text.splitlines())
        if sources_csv.strip():
            addresses.extend(sources_csv.split(","))
        seen: set[str] = set()
        normalized: list[str] = []
        for raw in addresses:
            address = raw.strip().strip('"').lower()
            if not address or address in seen:
                continue
            if not ADDRESS_RE.fullmatch(address):
                raise ValueError(f"invalid source address: {raw!r}")
            seen.add(address)
            normalized.append(address)
        addresses = normalized
    if max_sources is not None:
        if max_sources <= 0:
            raise ValueError("--max-sources must be positive")
        addresses = addresses[:max_sources]
    if not addresses:
        raise ValueError("at least one source address is required")
    return addresses


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--equity", type=Decimal, default=Decimal("50"))
    parser.add_argument("--max-pages", type=int, default=30)
    parser.add_argument("--timeout-s", type=int, default=15)
    parser.add_argument("--out-dir", type=Path, default=Path("data/backtests"))
    parser.add_argument("--sources-file", type=Path, default=None)
    parser.add_argument("--sources", default="", help="Comma-separated source addresses.")
    parser.add_argument("--max-sources", type=int, default=None)
    args = parser.parse_args()
    sources = load_source_addresses(
        sources_file=args.sources_file,
        sources_csv=args.sources,
        max_sources=args.max_sources,
    )

    end_ms = int(__import__("time").time() * 1000)
    start_ms = end_ms - args.days * MS_PER_DAY
    args.out_dir.mkdir(parents=True, exist_ok=True)
    stamp = __import__("datetime").datetime.now().strftime("%Y%m%d-%H%M%S")
    detail_path = args.out_dir / f"copy{len(sources)}_{args.days}d_{stamp}.json"
    csv_path = args.out_dir / f"copy{len(sources)}_{args.days}d_{stamp}.csv"

    details: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    for slot, address in enumerate(sources, start=1):
        fills, pages, truncated = fetch_fills(
            address, start_ms, end_ms, args.max_pages, args.timeout_s
        )
        portfolio = post_info({"type": "portfolio", "user": address}, args.timeout_s)
        clearinghouse = post_info({"type": "clearinghouseState", "user": address}, args.timeout_s)
        windows = portfolio_windows(portfolio)
        window_label, window = choose_portfolio_window(windows, args.days)
        account_values = history_points(window, "accountValueHistory")
        pnl_points = history_points(window, "pnlHistory")
        source_pnl = (
            pnl_points[-1][1]
            if pnl_points
            else sum(
                (decimal(fill.get("closedPnl")) - decimal(fill.get("fee")) for fill in fills),
                Decimal("0"),
            )
        )
        current_account_value = (
            decimal((clearinghouse.get("marginSummary") or {}).get("accountValue"))
            if isinstance(clearinghouse, dict)
            else Decimal("0")
        )
        if current_account_value <= 0 and account_values:
            current_account_value = account_values[-1][1]
        simulations = [
            simulate(fills, account_values, initial_equity=args.equity, strategy=strategy)
            for strategy in STRATEGIES
        ]
        detail = {
            "slot": slot,
            "address": address,
            "fills": len(fills),
            "fill_pages": pages,
            "truncated": truncated,
            "portfolio_window": window_label,
            "account_value_points": len(account_values),
            "source_current_account_value_usd": current_account_value,
            "source_window_net_pnl_usd": source_pnl,
            "source_approx_roi_pct": (source_pnl / current_account_value * Decimal("100"))
            if current_account_value > 0
            else Decimal("0"),
            "simulations": simulations,
        }
        details.append(detail)
        for simulation in simulations:
            rows.append(
                {
                    "slot": slot,
                    "address": address,
                    "fills": len(fills),
                    "fill_pages": pages,
                    "truncated": truncated,
                    "portfolio_window": window_label,
                    "source_current_account_value_usd": current_account_value,
                    "source_window_net_pnl_usd": source_pnl,
                    "strategy": simulation["strategy"],
                    **simulation,
                }
            )

    with detail_path.open("w", encoding="utf-8") as handle:
        json.dump(
            jsonable(
                {
                    "days": args.days,
                    "initial_equity_usd": args.equity,
                    "min_notional_usd": MIN_NOTIONAL,
                    "sources": details,
                }
            ),
            handle,
            indent=2,
        )
    fieldnames = list(rows[0].keys()) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jsonable(rows))
    print(
        json.dumps(
            jsonable(
                {
                    "json": str(detail_path),
                    "csv": str(csv_path),
                    "rows": len(rows),
                    "sources": len(details),
                }
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
