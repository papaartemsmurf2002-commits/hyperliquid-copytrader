from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Iterable

from .markets import canonical_market_symbol, market_dex, qualify_market_symbol
from .models import parse_decimal
from .precision import PERP_MAX_DECIMALS, quantize_price


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class MarketLiquiditySnapshot:
    coin: str
    observed_ms: int
    book_time_ms: int
    oracle_px: Decimal
    mark_px: Decimal | None
    mid_px: Decimal | None
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True)
class RoundTripQuote:
    coin: str
    opening_side: str
    requested_size: Decimal
    observed_ms: int
    book_time_ms: int
    oracle_px: Decimal
    mark_px: Decimal | None
    entry_limit: Decimal
    exit_limit: Decimal
    entry_visible_size: Decimal
    exit_visible_size: Decimal
    entry_best_px: Decimal
    entry_worst_px: Decimal
    exit_worst_px: Decimal
    entry_notional_bound_px: Decimal
    oracle_envelope_bps: Decimal

    def to_payload(self) -> dict[str, Any]:
        return {"kind": "hip3_round_trip", **asdict(self)}


@dataclass(frozen=True)
class RoundTripAssessment:
    """Structured result for a HIP-3 round-trip quote assessment.

    ``retryable_liquidity`` is true only after the snapshot and request pass all
    fatal validation and one or both book legs lack the requested depth inside
    the oracle envelope. Callers can therefore defer that OPEN without parsing
    human-readable blocker text.
    """

    quote: RoundTripQuote | None
    blockers: tuple[str, ...]
    retryable_liquidity: bool
    entry_depth_shortfall: bool = False
    exit_depth_shortfall: bool = False
    entry_visible_size: Decimal | None = None
    exit_visible_size: Decimal | None = None


@dataclass(frozen=True)
class ReduceOnlyQuote:
    coin: str
    side: str
    requested_size: Decimal
    observed_ms: int
    book_time_ms: int
    oracle_px: Decimal
    mark_px: Decimal | None
    limit_price: Decimal
    visible_size: Decimal
    worst_px: Decimal
    oracle_envelope_bps: Decimal

    def to_payload(self) -> dict[str, Any]:
        return {"kind": "hip3_reduce_only", **asdict(self)}


@dataclass(frozen=True)
class DepthExecutionQuote:
    """One-sided cached-book quote used by direct and deferred IOC admission."""

    coin: str
    side: str
    requested_size: Decimal
    filled_size: Decimal
    sufficient: bool
    expected_vwap: Decimal | None
    best_px: Decimal | None
    worst_px: Decimal | None
    rounded_limit_px: Decimal | None
    visible_size: Decimal
    book_time_ms: int
    observed_ms: int

    def to_payload(self) -> dict[str, Any]:
        return {"kind": "depth_execution", **asdict(self)}


def build_depth_execution_quote(
    snapshot: MarketLiquiditySnapshot,
    *,
    side: str,
    requested_size: Decimal,
    sz_decimals: int,
) -> DepthExecutionQuote:
    """Walk visible depth and outward-round the worst crossed price.

    This function performs no network I/O.  A caller must separately enforce book
    freshness, source-basis slippage, portfolio margin, and HIP-3 oracle bounds.
    """

    if side not in {"buy", "sell"}:
        raise ValueError("depth quote side must be buy or sell")
    if not requested_size.is_finite() or requested_size <= 0:
        raise ValueError("depth quote size must be finite and positive")
    levels = snapshot.asks if side == "buy" else snapshot.bids
    remaining = requested_size
    filled = Decimal("0")
    notional = Decimal("0")
    visible = Decimal("0")
    best = levels[0].price if levels else None
    worst: Decimal | None = None
    for level in levels:
        visible += level.size
        if remaining <= 0:
            continue
        take = min(level.size, remaining)
        if take <= 0:
            continue
        filled += take
        notional += take * level.price
        remaining -= take
        worst = level.price
    sufficient = filled >= requested_size
    vwap = notional / filled if filled > 0 else None
    rounded = (
        _quantize_crossing_price(worst, side=side, sz_decimals=sz_decimals)
        if sufficient and worst is not None
        else None
    )
    return DepthExecutionQuote(
        coin=snapshot.coin,
        side=side,
        requested_size=requested_size,
        filled_size=filled,
        sufficient=sufficient,
        expected_vwap=vwap,
        best_px=best,
        worst_px=worst,
        rounded_limit_px=rounded,
        visible_size=visible,
        book_time_ms=snapshot.book_time_ms,
        observed_ms=snapshot.observed_ms,
    )


def parse_market_liquidity_snapshot(
    coin: str,
    *,
    meta_and_contexts: Any,
    l2_book: Any,
    observed_ms: int,
) -> MarketLiquiditySnapshot:
    market = canonical_market_symbol(coin)
    if not isinstance(meta_and_contexts, list) or len(meta_and_contexts) != 2:
        raise ValueError(f"{market} metaAndAssetCtxs response has an invalid shape")
    meta, contexts = meta_and_contexts
    if not isinstance(meta, dict) or not isinstance(contexts, list):
        raise ValueError(f"{market} metaAndAssetCtxs response has an invalid shape")
    universe = meta.get("universe")
    if not isinstance(universe, list) or len(universe) != len(contexts):
        raise ValueError(f"{market} metadata and asset contexts are not aligned")

    dex = market_dex(market)
    context: dict[str, Any] | None = None
    for index, item in enumerate(universe):
        if not isinstance(item, dict):
            continue
        raw_name = str(item.get("name") or "").strip()
        if not raw_name:
            continue
        try:
            candidate = qualify_market_symbol(dex, raw_name)
        except ValueError:
            continue
        if candidate == market and isinstance(contexts[index], dict):
            context = contexts[index]
            break
    if context is None:
        raise ValueError(f"{market} is absent from metaAndAssetCtxs")

    oracle_px = _positive_decimal(context.get("oraclePx"), f"{market} oraclePx")
    mark_px = _optional_positive_decimal(context.get("markPx"), f"{market} markPx")
    mid_px = _optional_positive_decimal(context.get("midPx"), f"{market} midPx")

    if not isinstance(l2_book, dict):
        raise ValueError(f"{market} l2Book response has an invalid shape")
    try:
        book_market = canonical_market_symbol(l2_book.get("coin"))
    except ValueError as exc:
        raise ValueError(f"{market} l2Book response has an invalid coin identity") from exc
    if book_market != market:
        raise ValueError(
            f"{market} l2Book response coin {book_market} does not match the requested market"
        )
    raw_levels = l2_book.get("levels")
    if not isinstance(raw_levels, list) or len(raw_levels) != 2:
        raise ValueError(f"{market} l2Book response must contain bid and ask levels")
    book_time_ms = _positive_int(l2_book.get("time"), f"{market} l2Book time")
    bids = _parse_levels(raw_levels[0], market=market, side="bid", reverse=True)
    asks = _parse_levels(raw_levels[1], market=market, side="ask", reverse=False)
    # An empty side is valid exchange evidence that no executable depth is currently
    # visible. Preserve it for the structured assessment instead of treating temporary
    # illiquidity as a malformed response. Crossing can only be evaluated when both
    # sides contain at least one valid level.
    if bids and asks and bids[0].price >= asks[0].price:
        raise ValueError(f"{market} l2Book is crossed or locked")
    return MarketLiquiditySnapshot(
        coin=market,
        observed_ms=observed_ms,
        book_time_ms=book_time_ms,
        oracle_px=oracle_px,
        mark_px=mark_px,
        mid_px=mid_px,
        bids=bids,
        asks=asks,
    )


def assess_round_trip_quote(
    snapshot: MarketLiquiditySnapshot,
    *,
    opening_side: str,
    requested_size: Decimal,
    oracle_envelope_bps: Decimal,
    max_age_ms: int,
    sz_decimals: int,
    current_ms: int,
) -> RoundTripAssessment:
    blockers: list[str] = []
    if opening_side not in {"buy", "sell"}:
        blockers.append(f"{snapshot.coin} opening side must be buy or sell")
    if not requested_size.is_finite() or requested_size <= 0:
        blockers.append(f"{snapshot.coin} round-trip size must be finite and positive")
    if not oracle_envelope_bps.is_finite() or not (
        Decimal("0") < oracle_envelope_bps <= Decimal("1000")
    ):
        blockers.append(f"{snapshot.coin} HIP-3 oracle envelope must be in the range (0, 1000] bps")
    age_ms = current_ms - snapshot.book_time_ms
    if age_ms < -1_000:
        blockers.append(f"{snapshot.coin} l2Book timestamp is ahead of the local clock")
    elif age_ms > max_age_ms:
        blockers.append(f"{snapshot.coin} l2Book is stale by {age_ms}ms (limit {max_age_ms}ms)")
    if blockers:
        return RoundTripAssessment(
            quote=None,
            blockers=tuple(blockers),
            retryable_liquidity=False,
        )

    distance = snapshot.oracle_px * oracle_envelope_bps / Decimal("10000")
    lower = snapshot.oracle_px - distance
    upper = snapshot.oracle_px + distance
    entry_levels = snapshot.asks if opening_side == "buy" else snapshot.bids
    exit_levels = snapshot.bids if opening_side == "buy" else snapshot.asks
    entry_levels = tuple(
        level
        for level in entry_levels
        if (level.price <= upper if opening_side == "buy" else level.price >= lower)
    )
    exit_levels = tuple(
        level
        for level in exit_levels
        if (level.price >= lower if opening_side == "buy" else level.price <= upper)
    )
    entry_worst, entry_visible = _walk_depth(entry_levels, requested_size)
    exit_worst, exit_visible = _walk_depth(exit_levels, requested_size)
    entry_depth_shortfall = entry_worst is None
    exit_depth_shortfall = exit_worst is None
    if entry_depth_shortfall:
        blockers.append(
            f"{snapshot.coin} has only {entry_visible} visible {opening_side} entry depth "
            f"inside the {oracle_envelope_bps}bps oracle envelope; {requested_size} required"
        )
    exit_side = "sell" if opening_side == "buy" else "buy"
    if exit_depth_shortfall:
        blockers.append(
            f"{snapshot.coin} has only {exit_visible} visible {exit_side} exit depth "
            f"inside the {oracle_envelope_bps}bps oracle envelope; {requested_size} required"
        )
    if entry_depth_shortfall or exit_depth_shortfall:
        return RoundTripAssessment(
            quote=None,
            blockers=tuple(blockers),
            retryable_liquidity=True,
            entry_depth_shortfall=entry_depth_shortfall,
            exit_depth_shortfall=exit_depth_shortfall,
            entry_visible_size=entry_visible,
            exit_visible_size=exit_visible,
        )
    assert entry_worst is not None and exit_worst is not None
    entry_best = entry_levels[0].price
    entry_side = opening_side
    exit_side = "sell" if opening_side == "buy" else "buy"
    entry_limit = _quantize_crossing_price(
        entry_worst,
        side=entry_side,
        sz_decimals=sz_decimals,
    )
    exit_limit = _quantize_crossing_price(
        exit_worst,
        side=exit_side,
        sz_decimals=sz_decimals,
    )
    if not lower <= entry_limit <= upper:
        blockers.append(
            f"{snapshot.coin} {entry_side} entry depth cannot be represented at exchange "
            f"price precision inside the {oracle_envelope_bps}bps oracle envelope"
        )
    if not lower <= exit_limit <= upper:
        blockers.append(
            f"{snapshot.coin} {exit_side} exit depth cannot be represented at exchange "
            f"price precision inside the {oracle_envelope_bps}bps oracle envelope"
        )
    if blockers:
        return RoundTripAssessment(
            quote=None,
            blockers=tuple(blockers),
            retryable_liquidity=False,
            entry_visible_size=entry_visible,
            exit_visible_size=exit_visible,
        )
    return RoundTripAssessment(
        quote=RoundTripQuote(
            coin=snapshot.coin,
            opening_side=opening_side,
            requested_size=requested_size,
            observed_ms=snapshot.observed_ms,
            book_time_ms=snapshot.book_time_ms,
            oracle_px=snapshot.oracle_px,
            mark_px=snapshot.mark_px,
            entry_limit=entry_limit,
            exit_limit=exit_limit,
            entry_visible_size=entry_visible,
            exit_visible_size=exit_visible,
            entry_best_px=entry_best,
            entry_worst_px=entry_worst,
            exit_worst_px=exit_worst,
            # A sell IOC may fill at bids above its lowest crossing limit. Bound
            # current visible fill notional by both the oracle upper envelope and
            # the best entry level that the IOC can consume. For buys, the oracle
            # upper envelope is already at least as conservative as every admitted
            # ask. This value is proof data, not the signed limit price.
            entry_notional_bound_px=max(upper, entry_best, entry_worst),
            oracle_envelope_bps=oracle_envelope_bps,
        ),
        blockers=(),
        retryable_liquidity=False,
        entry_visible_size=entry_visible,
        exit_visible_size=exit_visible,
    )


def build_round_trip_quote(
    snapshot: MarketLiquiditySnapshot,
    *,
    opening_side: str,
    requested_size: Decimal,
    oracle_envelope_bps: Decimal,
    max_age_ms: int,
    sz_decimals: int,
    current_ms: int,
) -> tuple[RoundTripQuote | None, list[str]]:
    """Compatibility wrapper returning the original ``(quote, blockers)`` API."""

    assessment = assess_round_trip_quote(
        snapshot,
        opening_side=opening_side,
        requested_size=requested_size,
        oracle_envelope_bps=oracle_envelope_bps,
        max_age_ms=max_age_ms,
        sz_decimals=sz_decimals,
        current_ms=current_ms,
    )
    return assessment.quote, list(assessment.blockers)


def build_reduce_only_quote(
    snapshot: MarketLiquiditySnapshot,
    *,
    side: str,
    requested_size: Decimal,
    oracle_envelope_bps: Decimal,
    max_age_ms: int,
    sz_decimals: int,
    current_ms: int,
) -> tuple[ReduceOnlyQuote | None, list[str]]:
    blockers: list[str] = []
    if side not in {"buy", "sell"}:
        blockers.append(f"{snapshot.coin} reduce-only side must be buy or sell")
    if not requested_size.is_finite() or requested_size <= 0:
        blockers.append(f"{snapshot.coin} reduce-only size must be finite and positive")
    if not oracle_envelope_bps.is_finite() or not (
        Decimal("0") < oracle_envelope_bps <= Decimal("1000")
    ):
        blockers.append(f"{snapshot.coin} HIP-3 oracle envelope must be in the range (0, 1000] bps")
    age_ms = current_ms - snapshot.book_time_ms
    if age_ms < -1_000:
        blockers.append(f"{snapshot.coin} l2Book timestamp is ahead of the local clock")
    elif age_ms > max_age_ms:
        blockers.append(f"{snapshot.coin} l2Book is stale by {age_ms}ms (limit {max_age_ms}ms)")
    if blockers:
        return None, blockers

    distance = snapshot.oracle_px * oracle_envelope_bps / Decimal("10000")
    lower = snapshot.oracle_px - distance
    upper = snapshot.oracle_px + distance
    levels = snapshot.asks if side == "buy" else snapshot.bids
    levels = tuple(
        level
        for level in levels
        if (level.price <= upper if side == "buy" else level.price >= lower)
    )
    worst_px, visible = _walk_depth(levels, requested_size)
    if worst_px is None:
        return None, [
            f"{snapshot.coin} has only {visible} visible {side} reduce-only depth inside the "
            f"{oracle_envelope_bps}bps oracle envelope; {requested_size} required"
        ]
    limit_price = _quantize_crossing_price(
        worst_px,
        side=side,
        sz_decimals=sz_decimals,
    )
    if not lower <= limit_price <= upper:
        return None, [
            f"{snapshot.coin} {side} reduce-only depth cannot be represented at exchange "
            f"price precision inside the {oracle_envelope_bps}bps oracle envelope"
        ]
    return (
        ReduceOnlyQuote(
            coin=snapshot.coin,
            side=side,
            requested_size=requested_size,
            observed_ms=snapshot.observed_ms,
            book_time_ms=snapshot.book_time_ms,
            oracle_px=snapshot.oracle_px,
            mark_px=snapshot.mark_px,
            limit_price=limit_price,
            visible_size=visible,
            worst_px=worst_px,
            oracle_envelope_bps=oracle_envelope_bps,
        ),
        [],
    )


def _parse_levels(
    raw_levels: Any, *, market: str, side: str, reverse: bool
) -> tuple[BookLevel, ...]:
    if not isinstance(raw_levels, list):
        raise ValueError(f"{market} {side} levels have an invalid shape")
    levels: list[BookLevel] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            raise ValueError(f"{market} {side} level has an invalid shape")
        price = _positive_decimal(raw.get("px"), f"{market} {side} price")
        size = _positive_decimal(raw.get("sz"), f"{market} {side} size")
        levels.append(BookLevel(price=price, size=size))
    prices = [level.price for level in levels]
    if prices != sorted(prices, reverse=reverse):
        raise ValueError(f"{market} {side} levels are not price ordered")
    return tuple(levels)


def _walk_depth(
    levels: Iterable[BookLevel], requested_size: Decimal
) -> tuple[Decimal | None, Decimal]:
    visible = Decimal("0")
    for level in levels:
        visible += level.size
        if visible >= requested_size:
            return level.price, visible
    return None, visible


def _quantize_crossing_price(
    price: Decimal,
    *,
    side: str,
    sz_decimals: int,
) -> Decimal:
    """Return a valid exchange price that preserves crossing of ``price``.

    Buy limits round up and sell limits round down. The effective price quantum is the
    stricter of Hyperliquid's five-significant-figure rule and its perp decimal cap.
    """

    if side not in {"buy", "sell"}:
        raise ValueError("price side must be buy or sell")
    if not price.is_finite() or price <= 0:
        raise ValueError("price must be finite and positive")
    max_decimals = PERP_MAX_DECIMALS - sz_decimals
    if max_decimals < 0:
        raise ValueError("sz_decimals exceeds exchange price decimal budget")
    significant_decimals = max(0, 4 - price.adjusted())
    decimals = min(significant_decimals, max_decimals)
    quantum = Decimal(1).scaleb(-decimals)
    rounding = ROUND_CEILING if side == "buy" else ROUND_FLOOR
    candidate = price.quantize(quantum, rounding=rounding)
    normalized = quantize_price(candidate, sz_decimals)
    crosses = normalized >= price if side == "buy" else normalized <= price
    if not crosses:
        raise ValueError(f"{side} price cannot be quantized without losing book crossing")
    return normalized


def _positive_decimal(value: Any, label: str) -> Decimal:
    parsed = parse_decimal(value)
    if parsed is None or not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return parsed


def _optional_positive_decimal(value: Any, label: str) -> Decimal | None:
    if value in {None, ""}:
        return None
    return _positive_decimal(value, label)


def _positive_int(value: Any, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed
