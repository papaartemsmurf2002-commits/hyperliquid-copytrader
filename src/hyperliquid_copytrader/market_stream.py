from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .market_catalog import CatalogMarket, CatalogRevision, MarketReadiness
from .markets import canonical_market_symbol
from .models import parse_decimal
from .precision import aggressive_ioc_price, quantize_size


class MarketStreamError(ValueError):
    """A market frame or active subscription contradicts the pinned catalog."""


@dataclass(frozen=True, slots=True)
class MarketSubscriptionChange:
    added: tuple[str, ...]
    removed: tuple[str, ...]

    @property
    def subscribe(self) -> tuple[dict[str, str], ...]:
        return _subscription_specs(self.added)

    @property
    def unsubscribe(self) -> tuple[dict[str, str], ...]:
        return _subscription_specs(self.removed)


@dataclass(frozen=True, slots=True)
class DepthLevel:
    price: Decimal
    size: Decimal
    order_count: int | None = None


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    market: str
    catalog_revision: str
    asset_id: int
    sz_decimals: int
    max_leverage: int
    oracle_px: Decimal
    mark_px: Decimal
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    book_time_ms: int
    context_received_ms: int
    book_received_ms: int
    bbo_time_ms: int
    bbo_received_ms: int
    connection_epoch: int

    @property
    def best_bid(self) -> Decimal:
        if not self.bids:
            raise MarketStreamError(f"{self.market} has no executable bid")
        return self.bids[0].price

    @property
    def best_ask(self) -> Decimal:
        if not self.asks:
            raise MarketStreamError(f"{self.market} has no executable ask")
        return self.asks[0].price

    @property
    def best_bid_or_none(self) -> Decimal | None:
        return None if not self.bids else self.bids[0].price

    @property
    def best_ask_or_none(self) -> Decimal | None:
        return None if not self.asks else self.asks[0].price

    @property
    def size_quantum(self) -> Decimal:
        return Decimal(1).scaleb(-self.sz_decimals)

    def is_fresh(
        self,
        *,
        now_ms: int,
        max_age_ms: int,
        connection_epoch: int,
        connection_activity_ms: int | None = None,
    ) -> bool:
        if max_age_ms <= 0:
            raise ValueError("freshness max age must be positive")
        if connection_epoch != self.connection_epoch:
            return False
        # BBO subscriptions publish on a changed best quote. A quiet market does
        # not invalidate the last quote while the ordered connection remains
        # live. L2 is slower and is used only for depth estimation; every limit
        # is anchored to the latest BBO and remains a hard price boundary.
        if connection_activity_ms is None:
            context_age = now_ms - self.context_received_ms
            book_age = now_ms - self.book_received_ms
            bbo_age = now_ms - self.bbo_received_ms
            exchange_book_age = now_ms - self.book_time_ms
            exchange_bbo_age = now_ms - self.bbo_time_ms
            return (
                0 <= context_age <= max_age_ms
                and 0 <= book_age <= max_age_ms
                and 0 <= bbo_age <= max_age_ms
                and 0 <= exchange_book_age <= max_age_ms
                and 0 <= exchange_bbo_age <= max_age_ms
            )
        activity_ms = connection_activity_ms
        context_age = now_ms - self.context_received_ms
        book_age = now_ms - self.book_received_ms
        bbo_age = now_ms - self.bbo_received_ms
        return (
            0 <= now_ms - activity_ms <= max_age_ms
            # Global traffic proves only that the socket is alive.  It cannot
            # keep a frozen thin-market quote executable indefinitely.
            and 0 <= context_age <= max_age_ms
            and 0 <= book_age <= max_age_ms
            and 0 <= bbo_age <= max_age_ms
            and self.book_time_ms <= now_ms
            and self.bbo_time_ms <= now_ms
            and abs(self.book_received_ms - self.book_time_ms) <= max_age_ms
            and abs(self.bbo_received_ms - self.bbo_time_ms) <= max_age_ms
        )


@dataclass(frozen=True, slots=True)
class ExecutableIoc:
    """One full IOC plus the fill currently visible inside its hard limit."""

    size: Decimal
    visible_size: Decimal
    limit_px: Decimal
    estimated_vwap: Decimal
    estimated_notional: Decimal


def executable_ioc(
    snapshot: MarketSnapshot,
    *,
    is_buy: bool,
    requested_size: Decimal,
    max_slippage_bps: Decimal | None,
    hard_limit_px: Decimal | None = None,
) -> ExecutableIoc | None:
    """Plan a BBO-anchored IOC inside an optional stricter economic boundary.

    The submitted size is the full risk-approved request. L2 is observation,
    not a promise about the book when Hyperliquid commits the action. The
    currently visible portion and VWAP are retained as decision evidence; the
    venue atomically fills what remains available and cancels the remainder.
    """

    if not requested_size.is_finite() or requested_size <= 0:
        raise ValueError("requested IOC size must be finite and positive")
    if max_slippage_bps is not None and (
        not max_slippage_bps.is_finite()
        or max_slippage_bps < 0
        or max_slippage_bps >= Decimal("10000")
    ):
        raise ValueError("IOC slippage must be finite and between 0 and 10000 bps")
    if hard_limit_px is not None and (
        not hard_limit_px.is_finite() or hard_limit_px <= 0
    ):
        raise ValueError("IOC hard limit must be finite and positive")
    if max_slippage_bps is None:
        if hard_limit_px is None:
            raise ValueError("IOC requires a BBO buffer or a hard economic limit")
        limit_px = hard_limit_px
    else:
        levels = snapshot.asks if is_buy else snapshot.bids
        if not levels:
            return None
        bbo = levels[0].price
        limit_px = aggressive_ioc_price(
            bbo,
            is_buy=is_buy,
            slippage_bps=max_slippage_bps,
            sz_decimals=snapshot.sz_decimals,
        )
        if hard_limit_px is not None:
            limit_px = min(limit_px, hard_limit_px) if is_buy else max(limit_px, hard_limit_px)
    levels = snapshot.asks if is_buy else snapshot.bids
    eligible = [
        level
        for level in levels
        if (level.price <= limit_px if is_buy else level.price >= limit_px)
    ]
    available = sum((level.size for level in eligible), Decimal("0"))
    submitted_size = quantize_size(requested_size, snapshot.sz_decimals)
    visible_size = quantize_size(min(submitted_size, available), snapshot.sz_decimals)
    if submitted_size <= 0 or visible_size <= 0:
        return None
    remaining = visible_size
    notional = Decimal("0")
    for level in eligible:
        consumed = min(remaining, level.size)
        notional += consumed * level.price
        remaining -= consumed
        if remaining == 0:
            break
    if remaining != 0:
        raise MarketStreamError("eligible depth changed during pure IOC calculation")
    return ExecutableIoc(
        size=submitted_size,
        visible_size=visible_size,
        limit_px=limit_px,
        estimated_vwap=notional / visible_size,
        estimated_notional=notional,
    )


@dataclass(frozen=True, slots=True)
class _Context:
    oracle_px: Decimal
    mark_px: Decimal
    received_ms: int


@dataclass(frozen=True, slots=True)
class _Book:
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    exchange_time_ms: int
    received_ms: int


@dataclass(frozen=True, slots=True)
class _Bbo:
    bid: DepthLevel | None
    ask: DepthLevel | None
    exchange_time_ms: int
    received_ms: int


class MarketStream:
    """Pure reducer for context and L2 frames for only the currently active markets."""

    def __init__(
        self,
        *,
        catalog: CatalogRevision,
        active_markets: Iterable[str] = (),
    ) -> None:
        self.catalog = catalog
        self.connection_epoch = 0
        self.last_connection_activity_ms = 0
        self._active: set[str] = set()
        self._contexts: dict[str, _Context] = {}
        self._books: dict[str, _Book] = {}
        self._bbos: dict[str, _Bbo] = {}
        self.set_active_markets(active_markets)

    @property
    def active_markets(self) -> tuple[str, ...]:
        return tuple(sorted(self._active))

    @property
    def subscription_specs(self) -> tuple[dict[str, str], ...]:
        return _subscription_specs(self.active_markets)

    def catalog_market(self, market: str) -> CatalogMarket:
        canonical = canonical_market_symbol(market)
        spec = self.catalog.market(canonical)
        if spec is None:
            raise MarketStreamError(f"market {canonical} is absent from the pinned catalog")
        if spec.is_delisted or spec.readiness is MarketReadiness.DELISTED:
            raise MarketStreamError(f"market {canonical} is delisted")
        if spec.readiness is MarketReadiness.UNTRUSTED:
            raise MarketStreamError(f"market {canonical} has untrusted catalog identity")
        return spec

    def replace_catalog(self, catalog: CatalogRevision) -> MarketSubscriptionChange:
        """Adopt a coherent catalog revision and drop only no-longer-executable feeds."""

        before = set(self._active)
        self.catalog = catalog
        executable = {
            market
            for market in before
            if (spec := catalog.market(market)) is not None
            and not spec.is_delisted
            and spec.readiness not in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
        }
        self._active = executable
        removed = tuple(sorted(before - executable))
        for market in removed:
            self._contexts.pop(market, None)
            self._books.pop(market, None)
            self._bbos.pop(market, None)
        return MarketSubscriptionChange(added=(), removed=removed)

    def set_active_markets(self, markets: Iterable[str]) -> MarketSubscriptionChange:
        requested: set[str] = set()
        for market in markets:
            canonical = canonical_market_symbol(market)
            self.catalog_market(canonical)
            requested.add(canonical)
        added = tuple(sorted(requested - self._active))
        removed = tuple(sorted(self._active - requested))
        self._active = requested
        for market in removed:
            self._contexts.pop(market, None)
            self._books.pop(market, None)
            self._bbos.pop(market, None)
        return MarketSubscriptionChange(added=added, removed=removed)

    def begin_connection(self, *, received_ms: int = 0) -> int:
        if received_ms < 0:
            raise MarketStreamError("connection timestamp cannot be negative")
        self.connection_epoch += 1
        self.last_connection_activity_ms = received_ms
        self._contexts.clear()
        self._books.clear()
        self._bbos.clear()
        return self.connection_epoch

    def note_connection_activity(self, *, epoch: int, received_ms: int) -> None:
        self._validate_frame(epoch=epoch, received_ms=received_ms)
        self.last_connection_activity_ms = received_ms

    def apply(
        self,
        message: Mapping[str, Any],
        *,
        epoch: int,
        received_ms: int,
    ) -> MarketSnapshot | None:
        self._validate_frame(epoch=epoch, received_ms=received_ms)
        if not isinstance(message, Mapping):
            raise MarketStreamError("market stream message must be an object")
        channel = str(message.get("channel") or "")
        data = message.get("data")
        if not isinstance(data, Mapping):
            raise MarketStreamError("market stream data must be an object")
        try:
            market = canonical_market_symbol(data.get("coin"))
        except (TypeError, ValueError) as exc:
            raise MarketStreamError(f"market frame has invalid coin: {exc}") from exc
        if market not in self._active:
            raise MarketStreamError(f"received {channel or 'market'} frame for inactive {market}")
        self.catalog_market(market)

        try:
            if channel == "activeAssetCtx":
                context = _parse_context(data, received_ms=received_ms)
                self._contexts[market] = context
            elif channel == "l2Book":
                book = _parse_book(data, received_ms=received_ms)
                prior_book = self._books.get(market)
                if (
                    prior_book is not None
                    and book.exchange_time_ms < prior_book.exchange_time_ms
                ):
                    raise MarketStreamError("L2 book moved backwards in exchange time")
                self._books[market] = book
                prior_bbo = self._bbos.get(market)
                if prior_bbo is None or book.exchange_time_ms >= prior_bbo.exchange_time_ms:
                    self._bbos[market] = _Bbo(
                        bid=None if not book.bids else book.bids[0],
                        ask=None if not book.asks else book.asks[0],
                        exchange_time_ms=book.exchange_time_ms,
                        received_ms=received_ms,
                    )
            elif channel == "bbo":
                bbo = _parse_bbo(data, received_ms=received_ms)
                prior_bbo = self._bbos.get(market)
                if (
                    prior_bbo is not None
                    and bbo.exchange_time_ms < prior_bbo.exchange_time_ms
                ):
                    raise MarketStreamError("BBO moved backwards in exchange time")
                self._bbos[market] = bbo
            else:
                raise MarketStreamError(f"unsupported market stream channel: {channel!r}")
        except MarketStreamError:
            raise
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise MarketStreamError(f"invalid {channel or 'market'} frame: {exc}") from exc
        self.last_connection_activity_ms = received_ms
        return self.snapshot(market)

    def snapshot(self, market: str) -> MarketSnapshot | None:
        canonical = canonical_market_symbol(market)
        if canonical not in self._active:
            raise MarketStreamError(f"market {canonical} is not active")
        spec = self.catalog_market(canonical)
        context = self._contexts.get(canonical)
        book = self._books.get(canonical)
        bbo = self._bbos.get(canonical)
        if context is None or book is None or bbo is None:
            return None
        # A one-sided book is valid venue state.  Preserve the executable side
        # so a risk reduction is not blocked merely because the opposite side
        # disappeared in a thin market.
        bids = _merge_bbo(book.bids, bbo.bid, is_bid=True)
        asks = _merge_bbo(book.asks, bbo.ask, is_bid=False)
        if bids and asks and bids[0].price >= asks[0].price:
            raise MarketStreamError("latest BBO is locked or crossed")
        return MarketSnapshot(
            market=canonical,
            catalog_revision=self.catalog.revision_id,
            asset_id=spec.asset_id,
            sz_decimals=spec.sz_decimals,
            max_leverage=spec.max_leverage,
            oracle_px=context.oracle_px,
            mark_px=context.mark_px,
            bids=bids,
            asks=asks,
            book_time_ms=book.exchange_time_ms,
            context_received_ms=context.received_ms,
            book_received_ms=book.received_ms,
            bbo_time_ms=bbo.exchange_time_ms,
            bbo_received_ms=bbo.received_ms,
            connection_epoch=self.connection_epoch,
        )

    def fresh_snapshot(self, market: str, *, now_ms: int, max_age_ms: int) -> MarketSnapshot | None:
        snapshot = self.snapshot(market)
        if snapshot is None:
            return None
        if not snapshot.is_fresh(
            now_ms=now_ms,
            max_age_ms=max_age_ms,
            connection_epoch=self.connection_epoch,
            connection_activity_ms=self.last_connection_activity_ms,
        ):
            return None
        return snapshot

    def _validate_frame(self, *, epoch: int, received_ms: int) -> None:
        if self.connection_epoch == 0:
            raise MarketStreamError("begin_connection must be called before applying frames")
        if epoch != self.connection_epoch:
            raise MarketStreamError(
                f"stale connection epoch: expected {self.connection_epoch}, got {epoch}"
            )
        if received_ms <= 0:
            raise MarketStreamError("frame receive timestamp must be positive")
        if received_ms < self.last_connection_activity_ms:
            raise MarketStreamError("frame receive timestamp moved backwards")


def _subscription_specs(markets: Iterable[str]) -> tuple[dict[str, str], ...]:
    return tuple(
        spec
        for market in sorted(markets)
        for spec in (
            {"type": "activeAssetCtx", "coin": market},
            {"type": "l2Book", "coin": market},
            {"type": "bbo", "coin": market},
        )
    )


def _parse_context(data: Mapping[str, Any], *, received_ms: int) -> _Context:
    raw_context = data.get("ctx", data)
    if not isinstance(raw_context, Mapping):
        raise MarketStreamError("activeAssetCtx ctx must be an object")
    if "oraclePx" not in raw_context or "markPx" not in raw_context:
        raise MarketStreamError("activeAssetCtx is missing oraclePx or markPx")
    oracle_px = parse_decimal(raw_context["oraclePx"])
    mark_px = parse_decimal(raw_context["markPx"])
    if oracle_px <= 0 or mark_px <= 0:
        raise MarketStreamError("context prices must be positive")
    return _Context(oracle_px=oracle_px, mark_px=mark_px, received_ms=received_ms)


def _parse_book(data: Mapping[str, Any], *, received_ms: int) -> _Book:
    levels = data.get("levels")
    if not isinstance(levels, list) or len(levels) != 2:
        raise MarketStreamError("l2Book levels must contain exactly bid and ask arrays")
    bids = _parse_levels(levels[0], side="bid")
    asks = _parse_levels(levels[1], side="ask")
    if bids and asks and bids[0].price >= asks[0].price:
        raise MarketStreamError("l2Book is locked or crossed")
    return _Book(
        bids=bids,
        asks=asks,
        exchange_time_ms=_positive_int(data.get("time"), name="l2Book time"),
        received_ms=received_ms,
    )


def _parse_bbo(data: Mapping[str, Any], *, received_ms: int) -> _Bbo:
    raw = data.get("bbo")
    if not isinstance(raw, list) or len(raw) != 2:
        raise MarketStreamError("bbo must contain exactly bid and ask levels")
    bid = _parse_bbo_level(raw[0], side="bid")
    ask = _parse_bbo_level(raw[1], side="ask")
    if bid is not None and ask is not None and bid.price >= ask.price:
        raise MarketStreamError("BBO is locked or crossed")
    return _Bbo(
        bid=bid,
        ask=ask,
        exchange_time_ms=_positive_int(data.get("time"), name="bbo time"),
        received_ms=received_ms,
    )


def _parse_bbo_level(value: Any, *, side: str) -> DepthLevel | None:
    if value is None:
        return None
    levels = _parse_levels([value], side=side)
    return levels[0]


def _merge_bbo(
    levels: tuple[DepthLevel, ...], bbo: DepthLevel | None, *, is_bid: bool
) -> tuple[DepthLevel, ...]:
    if bbo is None:
        return ()
    tail = tuple(
        level
        for level in levels
        if (level.price < bbo.price if is_bid else level.price > bbo.price)
    )
    return (bbo, *tail)


def _parse_levels(value: Any, *, side: str) -> tuple[DepthLevel, ...]:
    if not isinstance(value, list):
        raise MarketStreamError(f"l2Book {side}s must be a list")
    levels: list[DepthLevel] = []
    for index, row in enumerate(value):
        if not isinstance(row, Mapping):
            raise MarketStreamError(f"l2Book {side}[{index}] must be an object")
        if "px" not in row or "sz" not in row:
            raise MarketStreamError(f"l2Book {side}[{index}] is missing px or sz")
        price = parse_decimal(row["px"])
        size = parse_decimal(row["sz"])
        if price <= 0 or size <= 0:
            raise MarketStreamError(f"l2Book {side}[{index}] must be finite and positive")
        raw_count = row.get("n")
        order_count = None
        if raw_count is not None:
            order_count = _positive_int(raw_count, name=f"l2Book {side}[{index}] n")
        levels.append(DepthLevel(price=price, size=size, order_count=order_count))
    prices = [level.price for level in levels]
    if side == "bid" and any(left <= right for left, right in zip(prices, prices[1:])):
        raise MarketStreamError("l2Book bids are not strictly descending")
    if side == "ask" and any(left >= right for left, right in zip(prices, prices[1:])):
        raise MarketStreamError("l2Book asks are not strictly ascending")
    return tuple(levels)


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise MarketStreamError(f"{name} must be a positive integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        raise MarketStreamError(f"{name} must be a positive integer") from None
    if parsed <= 0:
        raise MarketStreamError(f"{name} must be a positive integer")
    return parsed
