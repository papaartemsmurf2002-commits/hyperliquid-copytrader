from __future__ import annotations

import re
from collections import OrderedDict
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .markets import canonical_market_symbol, market_dex, qualify_market_symbol
from .models import OpenOrder, Position, parse_decimal
from .observer import parse_clearinghouse_positions, parse_open_orders
from .unified_account import UnifiedAccountStateError, parse_all_dexs_message


_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_UNIFIED_SOURCE_COMPONENTS = frozenset({"positions", "collateral", "fills", "twap_fills"})
_STANDARD_SOURCE_COMPONENTS = frozenset({"positions", "perp_equity", "fills", "twap_fills"})
_FOLLOWER_COMPONENTS = frozenset({"positions", "collateral", "open_orders", "fills"})


class AccountStreamError(ValueError):
    """A WebSocket account frame is malformed or contradicts prior stream truth."""


def _account(value: str) -> str:
    account = str(value).strip().lower()
    if not _ADDRESS_RE.fullmatch(account):
        raise AccountStreamError(f"invalid account address: {value!r}")
    return account


def source_subscription_specs(
    source: str, *, account_mode: str = "unified"
) -> tuple[dict[str, Any], ...]:
    """Return the minimal source subscriptions used by the copy path."""

    user = _account(source)
    mode = account_mode.strip().lower()
    if mode not in {"standard", "unified"}:
        raise AccountStreamError("source account mode must be standard or unified")
    specs: list[dict[str, Any]] = [
        {"type": "allDexsClearinghouseState", "user": user},
    ]
    if mode == "unified":
        specs.append({"type": "spotState", "user": user})
    specs.extend(
        (
            {"type": "userFills", "user": user, "aggregateByTime": False},
            {"type": "userTwapSliceFills", "user": user},
        )
    )
    return tuple(specs)


def follower_subscription_specs(follower: str) -> tuple[dict[str, Any], ...]:
    """Return the minimal follower subscriptions used for current execution state."""

    user = _account(follower)
    return (
        {"type": "allDexsClearinghouseState", "user": user},
        {"type": "spotState", "user": user},
        {"type": "openOrders", "user": user},
        {"type": "orderUpdates", "user": user},
        {"type": "userFills", "user": user, "aggregateByTime": False},
    )


@dataclass(frozen=True, slots=True)
class UsdcCollateral:
    total: Decimal
    hold: Decimal
    available: Decimal
    observed_ms: int
    received_ms: int


@dataclass(frozen=True, slots=True)
class FillRecord:
    account: str
    market: str
    tx_hash: str
    tid: str
    time_ms: int
    side: str
    size: Decimal
    price: Decimal
    start_position: Decimal | None
    oid: int | None
    received_ms: int
    connection_epoch: int
    is_snapshot: bool

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.account, self.tx_hash, self.tid)

    @property
    def signed_size(self) -> Decimal:
        return self.size if self.side == "buy" else -self.size


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account: str
    connection_epoch: int
    positions: dict[str, Position]
    collateral: UsdcCollateral | None
    perp_equity_by_dex: dict[str, Decimal]
    open_orders: tuple[OpenOrder, ...]
    fills: tuple[FillRecord, ...]
    component_received_ms: dict[str, int]
    required_components: frozenset[str]
    last_connection_activity_ms: int

    @property
    def baseline_complete(self) -> bool:
        return self.required_components.issubset(self.component_received_ms)

    @property
    def perp_equity_total(self) -> Decimal:
        return sum(self.perp_equity_by_dex.values(), Decimal("0"))

    def is_fresh(self, *, now_ms: int, max_age_ms: int) -> bool:
        if max_age_ms <= 0:
            raise ValueError("freshness max age must be positive")
        age_ms = now_ms - self.last_connection_activity_ms
        return self.baseline_complete and 0 <= age_ms <= max_age_ms


@dataclass(frozen=True, slots=True)
class AccountStreamUpdate:
    channel: str
    account: str
    role: str
    connection_epoch: int
    initial_snapshot: bool
    baseline_ready: bool
    new_fills: tuple[FillRecord, ...] = ()
    duplicate_fills: int = 0


@dataclass(slots=True)
class _MutableAccount:
    positions: dict[str, Position]
    collateral: UsdcCollateral | None
    perp_equity_by_dex: dict[str, Decimal]
    open_orders: list[OpenOrder]
    fills: list[FillRecord]
    component_received_ms: dict[str, int]
    component_observed_ms: dict[str, int]
    position_observed_ms_by_dex: dict[str, int]

    @classmethod
    def empty(cls) -> _MutableAccount:
        return cls({}, None, {}, [], [], {}, {}, {})


_FillFingerprint = tuple[str, int, str, Decimal, Decimal, Decimal | None, int | None]


class AccountStream:
    """Pure state reducer for one source/follower WebSocket connection.

    The reducer does not perform network I/O, REST recovery, or action dispatch. A caller starts
    an epoch, passes that epoch with every frame, and decides what to do with returned live fills.
    Snapshot fills seed dedupe state but are never returned as new fills.
    """

    def __init__(
        self,
        *,
        source: str,
        follower: str,
        source_account_mode: str = "unified",
        source_markets: Iterable[str] | None = None,
        follower_markets: Iterable[str] | None = None,
        accept_source_markets_outside_scope: bool = False,
        fill_dedupe_capacity: int = 100_000,
    ) -> None:
        self.source_address = _account(source)
        self.follower_address = _account(follower)
        if self.source_address == self.follower_address:
            raise AccountStreamError("source and follower accounts must be different")
        if fill_dedupe_capacity < 1:
            raise AccountStreamError("fill dedupe capacity must be positive")
        self.fill_dedupe_capacity = fill_dedupe_capacity
        self.source_account_mode = source_account_mode.strip().lower()
        if self.source_account_mode not in {"standard", "unified"}:
            raise AccountStreamError("source account mode must be standard or unified")
        self._fill_markets = {
            "source": _market_scope(source_markets),
            "follower": _market_scope(follower_markets),
        }
        self.accept_source_markets_outside_scope = bool(accept_source_markets_outside_scope)
        source_scope = self._fill_markets["source"]
        if self.source_account_mode == "standard" and not source_scope:
            raise AccountStreamError("standard source requires explicit perp markets")
        self.source_dexes = frozenset(
            market_dex(market) for market in (source_scope or frozenset())
        )
        self.connection_epoch = 0
        self.last_connection_activity_ms = 0
        self._source = _MutableAccount.empty()
        self._follower = _MutableAccount.empty()
        self._fill_fingerprints: OrderedDict[tuple[str, str, str], _FillFingerprint] = OrderedDict()

    def set_market_scope(
        self,
        *,
        source_markets: Iterable[str] | None,
        follower_markets: Iterable[str] | None,
    ) -> None:
        """Adopt a refreshed perp universe, invalidating only newly incomplete truth."""

        source_scope = _market_scope(source_markets)
        if self.source_account_mode == "standard" and not source_scope:
            raise AccountStreamError("standard source requires explicit perp markets")
        next_source_dexes = frozenset(
            market_dex(market) for market in (source_scope or frozenset())
        )
        dex_scope_changed = next_source_dexes != self.source_dexes
        self._fill_markets = {
            "source": source_scope,
            "follower": _market_scope(follower_markets),
        }
        self.source_dexes = next_source_dexes
        if self.source_account_mode == "standard" and dex_scope_changed:
            # accountValue is separate per DEX in Standard mode. A newly relevant DEX must
            # not be sized using the old subset's denominator; wait for one coherent
            # allDexsClearinghouseState frame that covers the refreshed scope.
            self._source.perp_equity_by_dex.clear()
            self._source.component_received_ms.pop("perp_equity", None)
            self._source.component_observed_ms.pop("perp_equity", None)

    @property
    def source(self) -> AccountSnapshot:
        required = (
            _STANDARD_SOURCE_COMPONENTS
            if self.source_account_mode == "standard"
            else _UNIFIED_SOURCE_COMPONENTS
        )
        return self._snapshot(self.source_address, self._source, required)

    @property
    def follower(self) -> AccountSnapshot:
        return self._snapshot(self.follower_address, self._follower, _FOLLOWER_COMPONENTS)

    @property
    def baseline_ready(self) -> bool:
        return self.source.baseline_complete and self.follower.baseline_complete

    def begin_connection(self, *, received_ms: int = 0) -> int:
        if received_ms < 0:
            raise AccountStreamError("connection timestamp cannot be negative")
        self.connection_epoch += 1
        self.last_connection_activity_ms = received_ms
        self._source = _MutableAccount.empty()
        self._follower = _MutableAccount.empty()
        return self.connection_epoch

    def note_connection_activity(self, *, epoch: int, received_ms: int) -> None:
        self._validate_frame(epoch=epoch, received_ms=received_ms)
        self.last_connection_activity_ms = max(self.last_connection_activity_ms, received_ms)

    def is_fresh(self, *, now_ms: int, max_age_ms: int) -> bool:
        if max_age_ms <= 0:
            raise ValueError("freshness max age must be positive")
        age_ms = now_ms - self.last_connection_activity_ms
        return self.baseline_ready and 0 <= age_ms <= max_age_ms

    def apply(
        self,
        message: Mapping[str, Any],
        *,
        epoch: int,
        received_ms: int,
    ) -> AccountStreamUpdate:
        self._validate_frame(epoch=epoch, received_ms=received_ms)
        if not isinstance(message, Mapping):
            raise AccountStreamError("account stream message must be an object")
        channel = str(message.get("channel") or "")
        try:
            if channel == "allDexsClearinghouseState":
                update = self._apply_positions(message, received_ms=received_ms)
            elif channel == "spotState":
                update = self._apply_spot(message, received_ms=received_ms)
            elif channel == "openOrders":
                update = self._apply_open_orders(message, received_ms=received_ms)
            elif channel == "orderUpdates":
                update = self._apply_order_updates(message, received_ms=received_ms)
            elif channel == "userFills":
                update = self._apply_fills(message, received_ms=received_ms)
            elif channel == "userTwapSliceFills":
                update = self._apply_twap_fills(message, received_ms=received_ms)
            else:
                raise AccountStreamError(f"unsupported account stream channel: {channel!r}")
        except AccountStreamError:
            raise
        except (ArithmeticError, TypeError, UnifiedAccountStateError, ValueError) as exc:
            raise AccountStreamError(f"invalid {channel or 'account'} frame: {exc}") from exc
        # Pongs are handled immediately while account frames are reduced through
        # per-slot queues.  A frame received just before a pong is still valid
        # even if it reaches this reducer just after the pong.
        self.last_connection_activity_ms = max(self.last_connection_activity_ms, received_ms)
        return update

    def _validate_frame(self, *, epoch: int, received_ms: int) -> None:
        if self.connection_epoch == 0:
            raise AccountStreamError("begin_connection must be called before applying frames")
        if epoch != self.connection_epoch:
            raise AccountStreamError(
                f"stale connection epoch: expected {self.connection_epoch}, got {epoch}"
            )
        if received_ms <= 0:
            raise AccountStreamError("frame receive timestamp must be positive")

    def _snapshot(
        self,
        account: str,
        state: _MutableAccount,
        required: frozenset[str],
    ) -> AccountSnapshot:
        return AccountSnapshot(
            account=account,
            connection_epoch=self.connection_epoch,
            positions=dict(state.positions),
            collateral=state.collateral,
            perp_equity_by_dex=dict(state.perp_equity_by_dex),
            open_orders=tuple(state.open_orders),
            fills=tuple(state.fills),
            component_received_ms=dict(state.component_received_ms),
            required_components=required,
            last_connection_activity_ms=self.last_connection_activity_ms,
        )

    def _role_for_data(self, data: Any) -> tuple[str, str, _MutableAccount]:
        if not isinstance(data, Mapping):
            raise AccountStreamError("account frame data must be an object")
        account = _account(str(data.get("user") or data.get("userAddress") or ""))
        if account == self.source_address:
            return account, "source", self._source
        if account == self.follower_address:
            return account, "follower", self._follower
        raise AccountStreamError(f"frame belongs to unconfigured account {account}")

    def _base_update(
        self,
        *,
        channel: str,
        account: str,
        role: str,
        initial_snapshot: bool,
        new_fills: tuple[FillRecord, ...] = (),
        duplicate_fills: int = 0,
    ) -> AccountStreamUpdate:
        return AccountStreamUpdate(
            channel=channel,
            account=account,
            role=role,
            connection_epoch=self.connection_epoch,
            initial_snapshot=initial_snapshot,
            baseline_ready=self.baseline_ready,
            new_fills=new_fills,
            duplicate_fills=duplicate_fills,
        )

    def _apply_positions(
        self, message: Mapping[str, Any], *, received_ms: int
    ) -> AccountStreamUpdate:
        data = message.get("data")
        account, role, state = self._role_for_data(data)
        snapshot = parse_all_dexs_message(
            message,
            expected_account=account,
            received_ms=received_ms,
        )
        positions: dict[str, Position] = {}
        positions_by_dex: dict[str, dict[str, Position]] = {}
        observed_by_dex: dict[str, int] = {}
        for dex in sorted(snapshot.clearinghouse_states, key=lambda item: (item != "", item)):
            dex_state = snapshot.clearinghouse_states[dex]
            observed_ms = _positive_int(
                dex_state.get("time"),
                name=f"allDexsClearinghouseState {dex or '<default>'} time",
            )
            dex_positions = parse_clearinghouse_positions(
                dex_state,
                observed_ms=observed_ms,
                dex=dex,
            )
            positions_by_dex[dex] = dex_positions
            observed_by_dex[dex] = observed_ms
            for market, position in dex_positions.items():
                if market in positions:
                    raise AccountStreamError(
                        f"duplicate canonical position across DEX states: {market}"
                    )
                positions[market] = position
        for dex, dex_positions in positions_by_dex.items():
            observed_ms = observed_by_dex[dex]
            prior_observed = state.position_observed_ms_by_dex.get(dex, 0)
            if observed_ms < prior_observed:
                raise AccountStreamError(
                    f"positions snapshot for DEX {dex or '<default>'} moved backwards "
                    "in exchange time"
                )
            prior_sizes = {
                market: position.size
                for market, position in state.positions.items()
                if market_dex(market) == dex and position.size
            }
            next_sizes = {
                market: position.size for market, position in dex_positions.items() if position.size
            }
            if prior_observed and observed_ms == prior_observed and next_sizes != prior_sizes:
                raise AccountStreamError(
                    f"same-time positions snapshot for DEX {dex or '<default>'} "
                    "contradicts current fill truth"
                )
        perp_equity_by_dex: dict[str, Decimal] = {}
        if role == "source" and self.source_account_mode == "standard":
            for dex in self.source_dexes:
                raw_state = snapshot.clearinghouse_states.get(dex)
                if raw_state is None:
                    raise AccountStreamError(
                        f"standard source is missing relevant DEX {dex or '<default>'}"
                    )
                summary = raw_state.get("marginSummary")
                if not isinstance(summary, Mapping) or summary.get("accountValue") is None:
                    raise AccountStreamError(
                        f"standard source DEX {dex or '<default>'} is missing accountValue"
                    )
                equity = parse_decimal(summary["accountValue"])
                if equity < 0:
                    raise AccountStreamError(
                        f"standard source DEX {dex or '<default>'} has negative accountValue"
                    )
                dex_nonflat = any(
                    position.size != 0
                    for market, position in positions.items()
                    if market_dex(market) == dex
                )
                if dex_nonflat and equity <= 0:
                    raise AccountStreamError(
                        f"standard source DEX {dex or '<default>'} is nonflat with no equity"
                    )
                perp_equity_by_dex[dex] = equity
            if sum(perp_equity_by_dex.values(), Decimal("0")) <= 0:
                raise AccountStreamError("standard source relevant DEX equity is not positive")
        initial = "positions" not in state.component_received_ms
        state.positions = positions
        if role == "source" and self.source_account_mode == "standard":
            state.perp_equity_by_dex = perp_equity_by_dex
            relevant_observed = [observed_by_dex[dex] for dex in self.source_dexes]
            state.component_received_ms["perp_equity"] = received_ms
            state.component_observed_ms["perp_equity"] = min(relevant_observed)
        state.component_received_ms["positions"] = received_ms
        state.component_observed_ms["positions"] = max(observed_by_dex.values())
        state.position_observed_ms_by_dex.update(observed_by_dex)
        return self._base_update(
            channel="allDexsClearinghouseState",
            account=account,
            role=role,
            initial_snapshot=initial,
        )

    def _apply_spot(self, message: Mapping[str, Any], *, received_ms: int) -> AccountStreamUpdate:
        data = message.get("data")
        account, role, state = self._role_for_data(data)
        assert isinstance(data, Mapping)
        payload = data.get("spotState", data)
        if not isinstance(payload, Mapping):
            raise AccountStreamError("spotState payload must be an object")
        balances = payload.get("balances")
        if not isinstance(balances, list):
            raise AccountStreamError("spotState balances must be a list")
        token_zero: list[Mapping[str, Any]] = []
        for index, row in enumerate(balances):
            if not isinstance(row, Mapping):
                raise AccountStreamError(f"spotState balance {index} must be an object")
            raw_token = row.get("token")
            # Current mainnet snapshots can retain inactive/placeholder spot
            # balances without a token index (for example ``o458``).  They are
            # irrelevant to unified collateral; token 0 remains mandatory and
            # uniquely identifies USDC below.
            if raw_token is None:
                continue
            if isinstance(raw_token, bool):
                raise AccountStreamError(f"spotState balance {index} has invalid token")
            try:
                token = int(str(raw_token))
            except (TypeError, ValueError):
                raise AccountStreamError(f"spotState balance {index} has invalid token") from None
            if token == 0:
                token_zero.append(row)
        if len(token_zero) != 1:
            raise AccountStreamError(
                f"spotState must contain exactly one token-0 balance, got {len(token_zero)}"
            )
        usdc = token_zero[0]
        if str(usdc.get("coin") or "").strip().upper() != "USDC":
            raise AccountStreamError("spot token 0 is not identified as USDC")
        if "total" not in usdc:
            raise AccountStreamError("spot token-0 USDC balance is missing total")
        total = parse_decimal(usdc["total"])
        hold = parse_decimal(usdc.get("hold", "0"))
        if total < 0 or hold < 0 or hold > total:
            raise AccountStreamError("spot token-0 USDC total/hold values are inconsistent")
        raw_observed = payload.get("time", data.get("time", received_ms))
        observed_ms = _positive_int(raw_observed, name="spotState time")
        prior_observed = state.component_observed_ms.get("collateral", 0)
        if observed_ms < prior_observed:
            raise AccountStreamError("collateral snapshot moved backwards in exchange time")
        initial = "collateral" not in state.component_received_ms
        state.collateral = UsdcCollateral(
            total=total,
            hold=hold,
            available=total - hold,
            observed_ms=observed_ms,
            received_ms=received_ms,
        )
        state.component_received_ms["collateral"] = received_ms
        state.component_observed_ms["collateral"] = observed_ms
        return self._base_update(
            channel="spotState",
            account=account,
            role=role,
            initial_snapshot=initial,
        )

    def _apply_open_orders(
        self, message: Mapping[str, Any], *, received_ms: int
    ) -> AccountStreamUpdate:
        data = message.get("data")
        account, role, state = self._role_for_data(data)
        if role != "follower":
            raise AccountStreamError("openOrders is only subscribed for the follower")
        assert isinstance(data, Mapping)
        rows = data.get("orders")
        if not isinstance(rows, list):
            raise AccountStreamError("openOrders payload must contain an orders list")
        orders = _parse_order_rows(rows, outer=data, observed_ms=received_ms)
        initial = "open_orders" not in state.component_received_ms
        state.open_orders = orders
        state.component_received_ms["open_orders"] = received_ms
        state.component_observed_ms["open_orders"] = received_ms
        return self._base_update(
            channel="openOrders",
            account=account,
            role=role,
            initial_snapshot=initial,
        )

    def _apply_order_updates(
        self, message: Mapping[str, Any], *, received_ms: int
    ) -> AccountStreamUpdate:
        raw_data = message.get("data")
        if isinstance(raw_data, Mapping):
            raw_user = raw_data.get("user") or raw_data.get("userAddress")
            if raw_user and _account(str(raw_user)) != self.follower_address:
                raise AccountStreamError("orderUpdates belongs to a non-follower account")
            updates = raw_data.get("updates", raw_data.get("orders"))
        else:
            updates = raw_data
        if not isinstance(updates, list):
            raise AccountStreamError("orderUpdates data must be a list")

        next_orders = list(self._follower.open_orders)
        latest_status_ms = received_ms
        for index, update in enumerate(updates):
            if not isinstance(update, Mapping):
                raise AccountStreamError(f"orderUpdates[{index}] must be an object")
            raw_user = update.get("user") or update.get("userAddress")
            if raw_user and _account(str(raw_user)) != self.follower_address:
                raise AccountStreamError("orderUpdates row belongs to a non-follower account")
            order = update.get("order")
            if not isinstance(order, Mapping):
                raise AccountStreamError(f"orderUpdates[{index}] is missing order")
            status = str(update.get("status") or "").strip().lower()
            if not status:
                raise AccountStreamError(f"orderUpdates[{index}] is missing status")
            status_ms = _positive_int(
                update.get("statusTimestamp"), name=f"orderUpdates[{index}] statusTimestamp"
            )
            latest_status_ms = max(latest_status_ms, status_ms)
            oid, cloid = _order_identity(order, index=index)
            matching = [
                candidate
                for candidate in next_orders
                if (oid is not None and candidate.oid == oid)
                or (cloid is not None and candidate.cloid == cloid)
            ]
            if len(matching) > 1:
                raise AccountStreamError(
                    f"orderUpdates[{index}] oid/cloid identify different open orders"
                )
            next_orders = [candidate for candidate in next_orders if candidate not in matching]
            if status == "open":
                parsed = _parse_order_rows([order], outer=update, observed_ms=status_ms)[0]
                _assert_order_identity_available(next_orders, parsed)
                next_orders.append(parsed)
            elif not _terminal_order_status(status):
                raise AccountStreamError(
                    f"orderUpdates[{index}] has unknown lifecycle status {status!r}"
                )

        self._follower.open_orders = next_orders
        if "open_orders" in self._follower.component_received_ms:
            self._follower.component_received_ms["open_orders"] = received_ms
            self._follower.component_observed_ms["open_orders"] = latest_status_ms
        return self._base_update(
            channel="orderUpdates",
            account=self.follower_address,
            role="follower",
            initial_snapshot=False,
        )

    def _apply_fills(
        self,
        message: Mapping[str, Any],
        *,
        received_ms: int,
        component: str = "fills",
        channel: str = "userFills",
    ) -> AccountStreamUpdate:
        data = message.get("data")
        account, role, state = self._role_for_data(data)
        assert isinstance(data, Mapping)
        if component not in {"fills", "twap_fills"}:
            raise AssertionError(f"unsupported internal fill component: {component}")
        has_baseline = component in state.component_received_ms
        raw_is_snapshot = data.get("isSnapshot")
        if raw_is_snapshot is None and has_baseline:
            # Hyperliquid declares isSnapshot optional and current mainnet can
            # omit it on an incremental userFills update.  Missing is only
            # unambiguous after this connection already received its baseline.
            is_snapshot = False
        elif isinstance(raw_is_snapshot, bool):
            is_snapshot = raw_is_snapshot
        else:
            raise AccountStreamError(
                "userFills must declare boolean isSnapshot before the initial snapshot"
            )
        if not is_snapshot and not has_baseline:
            raise AccountStreamError(
                f"live {channel} arrived before the initial fill snapshot for this channel"
            )
        rows = data.get("fills")
        if not isinstance(rows, list):
            raise AccountStreamError("userFills fills must be a list")

        scope = self._fill_markets[role]
        records: list[FillRecord] = []
        for index, row in enumerate(rows):
            if not isinstance(row, Mapping):
                raise AccountStreamError(f"userFills[{index}] must be an object")
            raw_coin = row.get("coin")
            if isinstance(raw_coin, str) and raw_coin.strip().startswith("@"):
                continue
            market = canonical_market_symbol(raw_coin)
            # userFills is an account-wide feed and includes Spot fills.  A
            # perp copy slot must ignore markets outside its pinned catalog
            # rather than applying Spot startPosition to perp state.
            if (
                scope is not None
                and market not in scope
                and not (role == "source" and self.accept_source_markets_outside_scope)
            ):
                continue
            records.append(
                _parse_fill(
                    row,
                    account=account,
                    received_ms=received_ms,
                    epoch=self.connection_epoch,
                    is_snapshot=is_snapshot,
                    index=index,
                )
            )
        payload_unique: dict[tuple[str, str, str], FillRecord] = {}
        for record in records:
            prior = payload_unique.get(record.identity)
            if prior is not None and _fill_fingerprint(prior) != _fill_fingerprint(record):
                raise AccountStreamError(
                    f"conflicting duplicate fill identity in frame: {record.identity}"
                )
            payload_unique.setdefault(record.identity, record)

        duplicate_count = len(records) - len(payload_unique)
        new_records: list[FillRecord] = []
        for identity, record in payload_unique.items():
            fingerprint = _fill_fingerprint(record)
            prior_fingerprint = self._fill_fingerprints.get(identity)
            if prior_fingerprint is not None and prior_fingerprint != fingerprint:
                raise AccountStreamError(f"fill identity changed payload: {identity}")
            if prior_fingerprint is None:
                new_records.append(record)
            else:
                duplicate_count += 1

        next_positions: dict[str, Position] | None = None
        if not is_snapshot:
            next_positions = self._validated_fill_positions(state.positions, new_records)

        for record in new_records:
            self._fill_fingerprints[record.identity] = _fill_fingerprint(record)
            self._fill_fingerprints.move_to_end(record.identity)
        for identity in payload_unique:
            if identity in self._fill_fingerprints:
                self._fill_fingerprints.move_to_end(identity)
        while len(self._fill_fingerprints) > self.fill_dedupe_capacity:
            self._fill_fingerprints.popitem(last=False)

        if next_positions is not None:
            state.positions = next_positions
            if new_records:
                state.component_received_ms["positions"] = received_ms
                state.component_observed_ms["positions"] = max(
                    state.component_observed_ms.get("positions", 0),
                    *(record.time_ms for record in new_records),
                )
                for record in new_records:
                    dex = market_dex(record.market)
                    state.position_observed_ms_by_dex[dex] = max(
                        state.position_observed_ms_by_dex.get(dex, 0),
                        record.time_ms,
                    )

        if is_snapshot:
            any_fill_baseline = any(
                key in state.component_received_ms for key in ("fills", "twap_fills")
            )
            if not any_fill_baseline:
                state.fills = list(payload_unique.values())[-self.fill_dedupe_capacity :]
            else:
                # userFills and userTwapSliceFills both deliver an initial
                # snapshot. Merge the second feed instead of replacing the
                # first feed's broader history with a narrower/empty snapshot.
                state.fills.extend(new_records)
                if len(state.fills) > self.fill_dedupe_capacity:
                    del state.fills[: -self.fill_dedupe_capacity]
        else:
            state.fills.extend(new_records)
            if len(state.fills) > self.fill_dedupe_capacity:
                del state.fills[: -self.fill_dedupe_capacity]
        state.component_received_ms[component] = received_ms
        state.component_observed_ms[component] = max(
            (record.time_ms for record in records), default=received_ms
        )
        return self._base_update(
            channel=channel,
            account=account,
            role=role,
            initial_snapshot=is_snapshot,
            new_fills=() if is_snapshot else tuple(new_records),
            duplicate_fills=duplicate_count,
        )

    def _apply_twap_fills(
        self, message: Mapping[str, Any], *, received_ms: int
    ) -> AccountStreamUpdate:
        data = message.get("data")
        if not isinstance(data, Mapping):
            raise AccountStreamError("userTwapSliceFills data must be an object")
        rows = data.get("twapSliceFills")
        if not isinstance(rows, list):
            raise AccountStreamError("userTwapSliceFills payload must be a list")
        fills: list[Mapping[str, Any]] = []
        for index, wrapper in enumerate(rows):
            if not isinstance(wrapper, Mapping) or not isinstance(wrapper.get("fill"), Mapping):
                raise AccountStreamError(f"userTwapSliceFills[{index}] must wrap one fill object")
            fills.append(wrapper["fill"])
        normalized = {
            "channel": "userFills",
            "data": {
                "user": data.get("user"),
                "isSnapshot": data.get("isSnapshot"),
                "fills": fills,
            },
        }
        return replace(
            self._apply_fills(
                normalized,
                received_ms=received_ms,
                component="twap_fills",
                channel="userTwapSliceFills",
            ),
            channel="userTwapSliceFills",
        )

    @staticmethod
    def _validated_fill_positions(
        positions: Mapping[str, Position],
        fills: list[FillRecord],
    ) -> dict[str, Position]:
        """Validate a whole venue startPosition chain before committing it.

        Waiting for a later account-state snapshot would turn a low-latency fill
        stream into a snapshot poller.  A contradiction pauses the stream rather
        than guessing which position is current. Validation uses a copy so one
        bad row cannot partially mutate the account or its dedupe state.
        """

        next_positions = dict(positions)
        for fill in fills:
            if fill.start_position is None:
                raise AccountStreamError(
                    f"live fill {fill.identity} is missing startPosition continuity"
                )
            end_position = fill.start_position + fill.signed_size
            prior = next_positions.get(fill.market)
            current_size = Decimal("0") if prior is None else prior.size
            if current_size == end_position:
                continue
            if current_size != fill.start_position:
                raise AccountStreamError(
                    f"live fill {fill.identity} startPosition {fill.start_position} "
                    f"does not match current {current_size}"
                )
            next_positions[fill.market] = Position(
                coin=fill.market,
                size=end_position,
                entry_px=(fill.price if prior is None else prior.entry_px),
                leverage=None if prior is None else prior.leverage,
                updated_ms=fill.time_ms,
            )
        return next_positions


def _positive_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool):
        raise AccountStreamError(f"{name} must be a positive integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        raise AccountStreamError(f"{name} must be a positive integer") from None
    if parsed <= 0:
        raise AccountStreamError(f"{name} must be a positive integer")
    return parsed


def _market_scope(markets: Iterable[str] | None) -> frozenset[str] | None:
    if markets is None:
        return None
    return frozenset(canonical_market_symbol(market) for market in markets)


def _order_identity(order: Mapping[str, Any], *, index: int) -> tuple[int | None, str | None]:
    raw_oid = order.get("oid")
    oid: int | None = None
    if raw_oid not in (None, ""):
        oid = _positive_int(raw_oid, name=f"orderUpdates[{index}] oid")
    raw_cloid = order.get("cloid")
    cloid: str | None = None
    if raw_cloid not in (None, ""):
        if not isinstance(raw_cloid, str):
            raise AccountStreamError(f"orderUpdates[{index}] cloid must be a string")
        # parse_open_orders performs the exact 128-bit Hyperliquid cloid validation for live rows.
        cloid = raw_cloid.strip().lower()
        if not re.fullmatch(r"0x[0-9a-f]{32}", cloid):
            raise AccountStreamError(f"orderUpdates[{index}] has invalid cloid")
    if oid is None and cloid is None:
        raise AccountStreamError(f"orderUpdates[{index}] is missing oid and cloid")
    return oid, cloid


def _parse_order_rows(
    rows: list[Any], *, outer: Mapping[str, Any], observed_ms: int
) -> list[OpenOrder]:
    result: list[OpenOrder] = []
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise AccountStreamError(f"open order {index} must be an object")
        row = dict(raw)
        explicit_dex = row.get("dex", outer.get("dex", ""))
        if explicit_dex is None:
            explicit_dex = ""
        if not isinstance(explicit_dex, str):
            raise AccountStreamError(f"open order {index} DEX must be a string")
        row["coin"] = qualify_market_symbol(explicit_dex, row.get("coin"))
        parsed = parse_open_orders([row], observed_ms=observed_ms)[0]
        _assert_order_identity_available(result, parsed)
        result.append(parsed)
    return result


def _assert_order_identity_available(orders: list[OpenOrder], candidate: OpenOrder) -> None:
    for prior in orders:
        if candidate.oid is not None and candidate.oid == prior.oid:
            raise AccountStreamError(f"duplicate open-order oid {candidate.oid}")
        if candidate.cloid is not None and candidate.cloid == prior.cloid:
            raise AccountStreamError(f"duplicate open-order cloid {candidate.cloid}")


def _terminal_order_status(status: str) -> bool:
    return status in {"filled", "canceled", "cancelled", "triggered", "scheduledcancel"} or (
        status.endswith("canceled") or status.endswith("cancelled") or status.endswith("rejected")
    )


def _parse_fill(
    row: Any,
    *,
    account: str,
    received_ms: int,
    epoch: int,
    is_snapshot: bool,
    index: int,
) -> FillRecord:
    if not isinstance(row, Mapping):
        raise AccountStreamError(f"userFills[{index}] must be an object")
    market = canonical_market_symbol(row.get("coin"))
    raw_hash = row.get("hash")
    if not isinstance(raw_hash, str) or not raw_hash.strip():
        raise AccountStreamError(f"userFills[{index}] is missing transaction hash")
    tx_hash = raw_hash.strip().lower()
    raw_tid = row.get("tid")
    if raw_tid is None or raw_tid == "" or isinstance(raw_tid, bool):
        raise AccountStreamError(f"userFills[{index}] is missing trade id")
    tid = str(raw_tid).strip()
    if not tid:
        raise AccountStreamError(f"userFills[{index}] is missing trade id")
    raw_side = str(row.get("side") or "").strip().lower()
    if raw_side in {"b", "buy"}:
        side = "buy"
    elif raw_side in {"a", "s", "sell"}:
        side = "sell"
    else:
        raise AccountStreamError(f"userFills[{index}] has invalid side")
    if "sz" not in row or "px" not in row:
        raise AccountStreamError(f"userFills[{index}] is missing size or price")
    size = parse_decimal(row["sz"])
    price = parse_decimal(row["px"])
    if size <= 0 or price <= 0:
        raise AccountStreamError(f"userFills[{index}] size and price must be positive")
    start_raw = row.get("startPosition")
    start_position = None if start_raw in (None, "") else parse_decimal(start_raw)
    raw_oid = row.get("oid")
    oid = None if raw_oid in (None, "") else _positive_int(raw_oid, name=f"userFills[{index}] oid")
    return FillRecord(
        account=account,
        market=market,
        tx_hash=tx_hash,
        tid=tid,
        time_ms=_positive_int(row.get("time"), name=f"userFills[{index}] time"),
        side=side,
        size=size,
        price=price,
        start_position=start_position,
        oid=oid,
        received_ms=received_ms,
        connection_epoch=epoch,
        is_snapshot=is_snapshot,
    )


def _fill_fingerprint(record: FillRecord) -> _FillFingerprint:
    return (
        record.market,
        record.time_ms,
        record.side,
        record.size,
        record.price,
        record.start_position,
        record.oid,
    )
