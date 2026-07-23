from __future__ import annotations

from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from threading import RLock
from typing import Any, Callable, Iterable, Mapping

from .markets import canonical_market_symbol
from .models import OpenOrder, Position


def fetch_all_dex_clearinghouse_states(
    info: Callable[[dict[str, Any]], Any],
    *,
    user: str,
    dexes: Iterable[str],
) -> dict[str, Any]:
    """Build an all-DEX account snapshot from supported HTTP info calls.

    ``allDexsClearinghouseState`` is a WebSocket subscription, not a supported
    HTTP info request.  Launch, reconciliation, and containment therefore read
    one ``clearinghouseState`` per catalog DEX and preserve the aggregate shape
    consumed by the unified-account parsers.
    """

    account = str(user).strip().lower()
    if not account:
        raise ValueError("clearinghouse snapshot user is required")
    ordered_dexes = tuple(str(dex) for dex in dexes)
    if not ordered_dexes or "" not in ordered_dexes:
        raise ValueError("clearinghouse snapshot requires the default DEX")
    if len(set(ordered_dexes)) != len(ordered_dexes):
        raise ValueError("clearinghouse snapshot DEXes must be unique")

    states: list[list[Any]] = []
    for dex in ordered_dexes:
        request: dict[str, Any] = {"type": "clearinghouseState", "user": account}
        if dex:
            request["dex"] = dex
        state = info(request)
        if not isinstance(state, Mapping):
            label = dex or "default"
            raise ValueError(f"{label} clearinghouseState is malformed")
        states.append([dex, dict(state)])
    return {"user": account, "clearinghouseStates": states}


class StreamState(str, Enum):
    CONNECTING = "CONNECTING"
    SNAPSHOT = "SNAPSHOT"
    LIVE = "LIVE"
    STALE = "STALE"
    REPLAYING = "REPLAYING"
    GAP = "GAP"
    RECONCILING = "RECONCILING"
    STOPPED = "STOPPED"

    @property
    def permits_increase(self) -> bool:
        return self is StreamState.LIVE


class StateProvenance(str, Enum):
    WEBSOCKET = "ws"
    COMMITTED_ACTION = "committed_action_response"
    TARGETED_REST = "targeted_rest"
    FULL_AUDIT = "full_audit"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class SourceAccountRevision:
    slot: str
    source_address: str
    revision: int
    source_event_key: str
    event_class: str
    exchange_ts_ms: int
    receive_wall_ms: int
    receive_monotonic_ns: int
    positions: Mapping[str, Position]
    account_value: Decimal
    denominator_confidence: str
    catalog_revision: str
    stream_state: StreamState
    durable_partition: str
    applied_cursor: int
    fresh_until_ms: int
    provenance: StateProvenance
    relevant_context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("source revision must be positive")
        if self.applied_cursor < 0:
            raise ValueError("source applied cursor must be non-negative")
        if not self.account_value.is_finite() or self.account_value <= 0:
            raise ValueError("source account value must be finite and positive")
        _validate_positions(self.positions)

    def is_fresh(self, now_ms: int) -> bool:
        return self.fresh_until_ms >= now_ms

    def is_market_fresh(self, market: str, now_ms: int) -> bool:
        """Require the denominator and this market's own source evidence.

        A fill may refresh one position, but it must never refresh the source
        denominator or unrelated portfolio members.  Legacy revisions without
        component clocks intentionally fail closed.
        """

        canonical = canonical_market_symbol(market)
        denominator_until = int(self.relevant_context.get("denominator_fresh_until_ms") or 0)
        raw_positions = self.relevant_context.get("position_fresh_until_ms")
        position_until = 0
        if isinstance(raw_positions, Mapping):
            position_until = int(raw_positions.get(canonical) or 0)
        if position_until <= 0:
            position_until = int(self.relevant_context.get("portfolio_fresh_until_ms") or 0)
        portfolio_until = int(self.relevant_context.get("portfolio_fresh_until_ms") or 0)
        return (
            denominator_until >= now_ms and portfolio_until >= now_ms and position_until >= now_ms
        )


@dataclass(frozen=True, slots=True)
class FollowerAccountRevision:
    follower_address: str
    revision: int
    confirmed_positions: Mapping[str, Position]
    projected_positions: Mapping[str, Position]
    open_orders: tuple[OpenOrder, ...]
    inflight_by_cloid: Mapping[str, Mapping[str, Any]]
    account_value: Decimal
    available_margin: Decimal
    account_mode: str
    exchange_ts_ms: int
    receive_wall_ms: int
    reconcile_wall_ms: int
    fresh_until_ms: int
    confidence: str
    provenance: StateProvenance
    catalog_revision: str
    durable_checkpoint: int
    leverage_blocks: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    nonfunding_ledger_checkpoint: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("follower revision must be positive")
        for name, value in (
            ("account_value", self.account_value),
            ("available_margin", self.available_margin),
        ):
            if not value.is_finite() or value < 0:
                raise ValueError(f"follower {name} must be finite and non-negative")
        _validate_positions(self.confirmed_positions)
        _validate_positions(self.projected_positions)
        if self.nonfunding_ledger_checkpoint:
            cursor_ms = self.nonfunding_ledger_checkpoint.get("cursor_ms")
            identities = self.nonfunding_ledger_checkpoint.get("seen_identities")
            if not isinstance(cursor_ms, int) or cursor_ms < 0:
                raise ValueError("follower ledger cursor must be a non-negative integer")
            if not isinstance(identities, (list, tuple)) or any(
                not isinstance(item, str) or not item for item in identities
            ):
                raise ValueError("follower ledger identities must be non-empty strings")

    def is_fresh(self, now_ms: int) -> bool:
        return self.fresh_until_ms >= now_ms

    def with_committed_fill(
        self,
        *,
        coin: str,
        signed_delta: Decimal,
        cloid: str,
        receive_wall_ms: int,
        terminal: bool,
    ) -> FollowerAccountRevision:
        market = canonical_market_symbol(coin)
        projected = dict(self.projected_positions)
        prior = projected.get(market, Position(coin=market, size=Decimal("0")))
        projected[market] = replace(
            prior,
            size=prior.size + signed_delta,
            updated_ms=receive_wall_ms,
        )
        inflight = dict(self.inflight_by_cloid)
        if terminal:
            inflight.pop(cloid.lower(), None)
        return replace(
            self,
            revision=self.revision + 1,
            projected_positions=projected,
            inflight_by_cloid=inflight,
            receive_wall_ms=receive_wall_ms,
            provenance=StateProvenance.COMMITTED_ACTION,
        )

    def with_inflight_action(
        self,
        *,
        cloid: str,
        market: str,
        signed_qty: Decimal,
        intent_id: str,
        state: str,
        receive_wall_ms: int,
        planned_follower_revision: int | None = None,
        action_kind: str = "order",
        target_leverage: int | None = None,
        is_cross: bool | None = None,
        reduce_only: bool = False,
    ) -> FollowerAccountRevision:
        canonical = canonical_market_symbol(market)
        inflight = dict(self.inflight_by_cloid)
        inflight[cloid.lower()] = {
            "cloid": cloid.lower(),
            "intent_id": intent_id,
            "market": canonical,
            "original_signed_qty": str(signed_qty),
            "cumulative_filled_qty": "0",
            "remaining_signed_qty": str(signed_qty),
            # Retained for readers of pre-v3 snapshots.  New calculations use
            # remaining_signed_qty so partial fills are never double-counted.
            "signed_qty": str(signed_qty),
            "state": state,
            "action_kind": action_kind,
            "target_leverage": target_leverage,
            "is_cross": is_cross,
            "reduce_only": reduce_only,
            # The revision used to plan the action and the most recent exchange
            # truth checkpoint are separate fences.  Local in-flight/fill
            # projections may advance the ordinary revision several times in a
            # multi-market reaction, while any later REST truth revision must
            # invalidate the plan before signing.
            "planned_follower_revision": (
                self.revision
                if planned_follower_revision is None
                else int(planned_follower_revision)
            ),
            "truth_checkpoint": self.durable_checkpoint,
            "updated_ms": receive_wall_ms,
        }
        return replace(
            self,
            revision=self.revision + 1,
            inflight_by_cloid=inflight,
            receive_wall_ms=receive_wall_ms,
            provenance=StateProvenance.COMMITTED_ACTION,
        )

    def apply_leverage_observation(
        self,
        *,
        cloid: str,
        accepted: bool,
        terminal: bool,
        state: str,
        receive_wall_ms: int,
        leverage_block: Mapping[str, Any] | None = None,
        clear_block: bool = False,
    ) -> FollowerAccountRevision:
        key = cloid.lower()
        prior_payload = self.inflight_by_cloid.get(key)
        if prior_payload is None or prior_payload.get("action_kind") != "updateLeverage":
            raise ValueError("leverage observation requires a durable leverage action")
        projected = dict(self.projected_positions)
        market = canonical_market_symbol(str(prior_payload["market"]))
        if accepted:
            target_leverage = int(prior_payload["target_leverage"])
            prior_position = projected.get(market, Position(coin=market, size=Decimal("0")))
            projected[market] = replace(
                prior_position,
                leverage=target_leverage,
                updated_ms=receive_wall_ms,
            )
        inflight = dict(self.inflight_by_cloid)
        if terminal:
            inflight.pop(key, None)
        else:
            inflight[key] = {
                **dict(prior_payload),
                "state": state,
                "updated_ms": receive_wall_ms,
            }
        blocks = dict(self.leverage_blocks)
        if clear_block:
            blocks.pop(market, None)
        elif leverage_block is not None:
            blocks[market] = dict(leverage_block)
        return replace(
            self,
            revision=self.revision + 1,
            projected_positions=projected,
            inflight_by_cloid=inflight,
            leverage_blocks=blocks,
            receive_wall_ms=receive_wall_ms,
            provenance=StateProvenance.COMMITTED_ACTION,
        )

    def apply_action_observation(
        self,
        *,
        cloid: str,
        cumulative_filled_abs: Decimal,
        state: str,
        terminal: bool,
        receive_wall_ms: int,
        order_oid: int | None = None,
    ) -> FollowerAccountRevision:
        key = cloid.lower()
        prior_payload = self.inflight_by_cloid.get(key)
        if prior_payload is None:
            raise ValueError("action observation requires a durable in-flight action")
        if not cumulative_filled_abs.is_finite() or cumulative_filled_abs < 0:
            raise ValueError("cumulative action fill must be finite and non-negative")
        original = Decimal(
            str(prior_payload.get("original_signed_qty", prior_payload.get("signed_qty", "0")))
        )
        prior_filled = Decimal(str(prior_payload.get("cumulative_filled_qty", "0")))
        if cumulative_filled_abs < prior_filled or cumulative_filled_abs > abs(original):
            raise ValueError("cumulative action fill regressed or exceeded the order size")
        incremental_abs = cumulative_filled_abs - prior_filled
        signed_increment = incremental_abs if original >= 0 else -incremental_abs
        projected = dict(self.projected_positions)
        market = canonical_market_symbol(str(prior_payload["market"]))
        if signed_increment:
            prior_position = projected.get(market, Position(coin=market, size=Decimal("0")))
            projected[market] = replace(
                prior_position,
                size=prior_position.size + signed_increment,
                updated_ms=receive_wall_ms,
            )
        inflight = dict(self.inflight_by_cloid)
        if terminal:
            inflight.pop(key, None)
        else:
            remaining_abs = abs(original) - cumulative_filled_abs
            remaining = remaining_abs if original >= 0 else -remaining_abs
            inflight[key] = {
                **dict(prior_payload),
                "cumulative_filled_qty": str(cumulative_filled_abs),
                "remaining_signed_qty": str(remaining),
                "state": state,
                "order_oid": order_oid,
                "last_reconcile_wall_ms": receive_wall_ms,
                "updated_ms": receive_wall_ms,
            }
        return replace(
            self,
            revision=self.revision + 1,
            projected_positions=projected,
            inflight_by_cloid=inflight,
            receive_wall_ms=receive_wall_ms,
            provenance=StateProvenance.COMMITTED_ACTION,
        )

    def without_inflight_action(
        self, *, cloid: str, receive_wall_ms: int
    ) -> FollowerAccountRevision:
        inflight = dict(self.inflight_by_cloid)
        inflight.pop(cloid.lower(), None)
        return replace(
            self,
            revision=self.revision + 1,
            inflight_by_cloid=inflight,
            receive_wall_ms=receive_wall_ms,
            provenance=StateProvenance.COMMITTED_ACTION,
        )


@dataclass(frozen=True, slots=True)
class BookRevision:
    market: str
    revision: int
    catalog_revision: str
    book_time_ms: int
    receive_wall_ms: int
    receive_monotonic_ns: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    oracle_px: Decimal | None = None
    mark_px: Decimal | None = None
    connection_epoch: int = 0

    def __post_init__(self) -> None:
        if canonical_market_symbol(self.market) != self.market:
            raise ValueError("book market must be canonical")
        if self.revision < 1 or not self.catalog_revision:
            raise ValueError("book revision and catalog identity must be present")
        if self.book_time_ms <= 0 or self.receive_wall_ms <= 0:
            raise ValueError("book timestamps must be positive")
        if self.connection_epoch < 0:
            raise ValueError("book connection epoch cannot be negative")
        for side in (self.bids, self.asks):
            for price, size in side:
                if not price.is_finite() or price <= 0 or not size.is_finite() or size <= 0:
                    raise ValueError("book levels must be finite and positive")

    def is_fresh(self, now_ms: int, max_age_ms: int) -> bool:
        return 0 <= now_ms - self.receive_wall_ms <= max_age_ms


@dataclass(frozen=True, slots=True)
class RevisionFence:
    source_revision: int
    follower_revision: int
    catalog_revision: str
    book_revision: int

    def matches(
        self,
        *,
        source: SourceAccountRevision,
        follower: FollowerAccountRevision,
        catalog_revision: str,
        book: BookRevision,
    ) -> bool:
        return (
            self.source_revision == source.revision
            and self.follower_revision == follower.revision
            and self.catalog_revision == catalog_revision
            and self.book_revision == book.revision
        )


class AccountStateBook:
    """One in-process owner for immutable source, follower, and book revisions."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._sources: dict[str, SourceAccountRevision] = {}
        self._followers: dict[str, FollowerAccountRevision] = {}
        self._books: dict[str, BookRevision] = {}

    def publish_source(self, revision: SourceAccountRevision) -> None:
        key = revision.slot
        with self._lock:
            previous = self._sources.get(key)
            if previous is not None and revision.revision != previous.revision + 1:
                raise ValueError("source revision must advance by exactly one")
            self._sources[key] = revision

    def publish_follower(self, revision: FollowerAccountRevision) -> None:
        key = revision.follower_address.lower()
        with self._lock:
            previous = self._followers.get(key)
            if previous is not None and revision.revision != previous.revision + 1:
                raise ValueError("follower revision must advance by exactly one")
            self._followers[key] = revision

    def publish_book(self, revision: BookRevision) -> None:
        with self._lock:
            previous = self._books.get(revision.market)
            if previous is not None and revision.revision != previous.revision + 1:
                raise ValueError("book revision must advance by exactly one")
            self._books[revision.market] = revision

    def retire_book(self, market: str) -> None:
        with self._lock:
            self._books.pop(canonical_market_symbol(market), None)

    def source(self, slot: str) -> SourceAccountRevision | None:
        with self._lock:
            return self._sources.get(slot)

    def follower(self, address: str) -> FollowerAccountRevision | None:
        with self._lock:
            return self._followers.get(address.lower())

    def book(self, market: str) -> BookRevision | None:
        with self._lock:
            return self._books.get(canonical_market_symbol(market))

    def fence(self, *, slot: str, follower: str, market: str) -> RevisionFence | None:
        with self._lock:
            source = self._sources.get(slot)
            target = self._followers.get(follower.lower())
            book = self._books.get(canonical_market_symbol(market))
            if source is None or target is None or book is None:
                return None
            return RevisionFence(
                source_revision=source.revision,
                follower_revision=target.revision,
                catalog_revision=source.catalog_revision,
                book_revision=book.revision,
            )


def _validate_positions(positions: Mapping[str, Position]) -> None:
    for key, position in positions.items():
        if canonical_market_symbol(key) != key or canonical_market_symbol(position.coin) != key:
            raise ValueError("position maps must use canonical market keys")
        if not position.size.is_finite():
            raise ValueError("position size must be finite")
