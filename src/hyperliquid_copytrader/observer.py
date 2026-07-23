from __future__ import annotations

import asyncio
import inspect
import json
import urllib.request
from collections.abc import Awaitable, Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from threading import RLock
from typing import Any, Protocol

from .cloid import deterministic_cloid, validate_cloid
from .markets import canonical_market_symbol, market_dex, qualify_market_symbol
from .models import (
    OpenOrder,
    Position,
    SourceEvent,
    SourceEventType,
    now_ms,
    parse_decimal,
)
from .persistence import SQLiteStore
from .rest_throttle import call_with_rest_backoff, info_rest_throttle_enabled_for_base_url
from .safety import ConsistencyShield
from .unified_account import (
    HyperliquidUserAbstraction,
    SourceDexScope,
    UnifiedAccountSnapshot,
    UnifiedAccountStateStream,
    classify_user_abstraction,
    non_default_dex_activity,
    normalized_abstraction_mode,
)
from .websocket_transport import connect_websocket_ipv6_preferred


class InfoClient(Protocol):
    def info(self, payload: dict[str, Any]) -> Any: ...


class HyperliquidInfoClient:
    def __init__(self, base_url: str, timeout_s: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.rest_throttle_enabled = info_rest_throttle_enabled_for_base_url(self.base_url)

    def info(self, payload: dict[str, Any]) -> Any:
        def request_info() -> Any:
            body = json.dumps(payload).encode("utf-8")
            request = urllib.request.Request(
                self.base_url + "/info",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:  # nosec B310
                return json.loads(response.read().decode("utf-8"))

        return call_with_rest_backoff(
            f"info:{payload.get('type', 'unknown')}",
            request_info,
            enabled=self.rest_throttle_enabled,
        )


@dataclass(frozen=True)
class SourceSnapshot:
    positions: dict[str, Position]
    open_orders: list[OpenOrder]
    mids: dict[str, Decimal]
    observed_ms: int
    state_key: str
    planning_key: str
    raw_state: dict[str, Any]


@dataclass(frozen=True)
class FillBackfillReport:
    start_time_ms: int
    end_time_ms: int
    pages: int
    fetched: int
    inserted: int
    duplicates: int
    warnings: list[str]


@dataclass(frozen=True)
class SourceGapBackfillReport:
    start_time_ms: int
    end_time_ms: int
    pages: int
    fetched: int
    inserted: int
    duplicates: int
    warnings: list[str]
    fills: FillBackfillReport
    twap_slice_fills: FillBackfillReport


class SourceWebsocketMessageError(ValueError):
    pass


SourceEventCallback = Callable[[SourceEvent, bool], Awaitable[None] | None]


@dataclass(frozen=True)
class WsEventClassification:
    event_type: SourceEventType
    subtype: str
    timestamp_required: bool
    summary: dict[str, Any]


USER_ADDRESS_KEYS = {"user", "useraddress", "account", "accountaddress"}
TIMESTAMP_KEYS = {"time", "T", "timestamp", "statusTimestamp"}
EXCHANGE_TIMESTAMP_CHANNELS = {
    "orderUpdates",
    "userEvents",
    "userFills",
    "userFundings",
    "userNonFundingLedgerUpdates",
    "userTwapHistory",
    "userTwapSliceFills",
}
WEBSOCKET_CONNECTION_BANNER = "Websocket connection established."
SOURCE_OPEN_ORDERS_CACHE_TTL_MS = 60_000
SOURCE_ACTIVE_ASSET_SUBSCRIPTION_LIMIT = 32
SOURCE_ACCOUNT_IDENTITY_CACHE_TTL_MS = 60_000


@dataclass
class _SourceOpenOrderCacheEntry:
    orders: list[dict[str, Any]]
    baseline_observed_ms: int
    last_update_ms: int
    complete: bool
    refresh_count: int


class _SourceOpenOrderCache:
    """Bounded REST baseline with immediate orderUpdates mutation.

    A WebSocket delta never upgrades an incomplete cache into authoritative truth.  Each DEX
    therefore receives a validated REST baseline at least once per TTL, while orderUpdates keep
    that baseline current between refreshes.  An expired or future-dated baseline is never
    returned: the caller must obtain fresh REST truth or fail the reconcile.
    """

    def __init__(self, *, source_wallet: str, ttl_ms: int):
        if not 30_000 <= ttl_ms <= 60_000:
            raise ValueError("source open-order cache TTL must be between 30000 and 60000ms")
        self.source_wallet = source_wallet.lower()
        self.ttl_ms = ttl_ms
        self._entries: dict[str, _SourceOpenOrderCacheEntry] = {}
        self._lock = RLock()

    def get_or_refresh(
        self,
        dex: str,
        *,
        loader: Callable[[], Any],
        current_ms: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        with self._lock:
            entry = self._entries.get(dex)
            if entry is not None and self._fresh(entry, current_ms=current_ms):
                return deepcopy(entry.orders), self._status(
                    dex, entry, current_ms=current_ms, refreshed=False
                )

            payload = loader()
            # Validate the exact DEX-qualified response before it can become an authoritative
            # baseline.  This also rejects duplicate OIDs/CLOIDs and malformed order identities.
            parse_open_orders(payload, current_ms, dex=dex)
            orders = _raw_open_orders(payload)
            prior_refreshes = entry.refresh_count if entry is not None else 0
            entry = _SourceOpenOrderCacheEntry(
                orders=orders,
                baseline_observed_ms=current_ms,
                last_update_ms=current_ms,
                complete=True,
                refresh_count=prior_refreshes + 1,
            )
            self._entries[dex] = entry
            return deepcopy(entry.orders), self._status(
                dex, entry, current_ms=current_ms, refreshed=True
            )

    def apply_order_updates(self, data: Any, *, observed_ms: int) -> None:
        updates = data if isinstance(data, list) else [data]
        with self._lock:
            for index, update in enumerate(updates):
                if not isinstance(update, dict):
                    raise ValueError(f"orderUpdates[{index}] must be an object")
                order = update.get("order")
                if not isinstance(order, dict):
                    raise ValueError(f"orderUpdates[{index}] is missing order")
                raw_status = update.get("status")
                if not isinstance(raw_status, str) or not raw_status.strip():
                    raise ValueError(f"orderUpdates[{index}] is missing status")
                dex = _order_update_dex(update, order)
                entry = self._entries.get(dex)
                if entry is None:
                    entry = _SourceOpenOrderCacheEntry(
                        orders=[],
                        baseline_observed_ms=0,
                        last_update_ms=observed_ms,
                        complete=False,
                        refresh_count=0,
                    )
                    self._entries[dex] = entry
                entry.orders = [
                    candidate
                    for candidate in entry.orders
                    if not _same_order_identity(candidate, order)
                ]
                if raw_status.strip().lower() == "open":
                    # Only an explicitly open order remains in the cache.  Every terminal,
                    # rejected, triggered, or canceled status removes the previous identity.
                    parse_open_orders([order], observed_ms, dex=dex)
                    entry.orders.append(deepcopy(order))
                entry.last_update_ms = max(entry.last_update_ms, observed_ms)

    def status(self, *, current_ms: int) -> dict[str, Any]:
        with self._lock:
            dexes = {
                (dex or "<default>"): self._status(
                    dex, entry, current_ms=current_ms, refreshed=False
                )
                for dex, entry in sorted(
                    self._entries.items(), key=lambda item: (item[0] != "", item[0])
                )
            }
        return {
            "source_wallet": self.source_wallet,
            "ttl_ms": self.ttl_ms,
            "complete": bool(dexes) and all(item["complete"] for item in dexes.values()),
            "fresh": bool(dexes) and all(item["fresh"] for item in dexes.values()),
            "dexes": dexes,
        }

    def _fresh(self, entry: _SourceOpenOrderCacheEntry, *, current_ms: int) -> bool:
        age_ms = current_ms - entry.baseline_observed_ms
        return entry.complete and 0 <= age_ms < self.ttl_ms

    def _status(
        self,
        dex: str,
        entry: _SourceOpenOrderCacheEntry,
        *,
        current_ms: int,
        refreshed: bool,
    ) -> dict[str, Any]:
        age_ms = current_ms - entry.baseline_observed_ms if entry.baseline_observed_ms > 0 else None
        return {
            "dex": dex,
            "complete": entry.complete,
            "fresh": self._fresh(entry, current_ms=current_ms),
            "age_ms": age_ms,
            "baseline_observed_ms": entry.baseline_observed_ms,
            "last_update_ms": entry.last_update_ms,
            "order_count": len(entry.orders),
            "refresh_count": entry.refresh_count,
            "refreshed": refreshed,
        }


def _raw_open_orders(payload: Any) -> list[dict[str, Any]]:
    raw_orders = payload.get("orders") if isinstance(payload, dict) else payload
    if not isinstance(raw_orders, list):
        raise ValueError("open-orders response must contain an orders list")
    return [deepcopy(order) for order in raw_orders]


def _order_update_dex(update: dict[str, Any], order: dict[str, Any]) -> str:
    raw_coin = order.get("coin")
    if not isinstance(raw_coin, str) or not raw_coin.strip():
        raise ValueError("orderUpdates order is missing a valid coin")
    embedded_dex = market_dex(canonical_market_symbol(raw_coin))
    raw_explicit = order.get("dex", update.get("dex"))
    if raw_explicit is None:
        return embedded_dex
    if not isinstance(raw_explicit, str):
        raise ValueError("orderUpdates DEX must be a string")
    explicit_dex = raw_explicit.strip()
    # qualify_market_symbol validates DEX syntax and catches an embedded-prefix conflict.
    qualified = qualify_market_symbol(explicit_dex, raw_coin)
    qualified_dex = market_dex(qualified)
    if embedded_dex and qualified_dex != embedded_dex:
        raise ValueError("orderUpdates contains conflicting DEX identities")
    return qualified_dex


def _same_order_identity(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_oid = left.get("oid")
    right_oid = right.get("oid")
    if left_oid not in (None, "") and right_oid not in (None, ""):
        try:
            if int(str(left_oid)) == int(str(right_oid)):
                return True
        except (TypeError, ValueError):
            pass
    left_cloid = left.get("cloid")
    right_cloid = right.get("cloid")
    return (
        isinstance(left_cloid, str)
        and isinstance(right_cloid, str)
        and bool(left_cloid.strip())
        and left_cloid.strip().lower() == right_cloid.strip().lower()
    )


def _parse_position_leverage(raw: Any, *, index: int) -> int | None:
    value = raw.get("value") if isinstance(raw, dict) else raw
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"assetPositions[{index}] leverage must be a positive integer")
    try:
        parsed = parse_decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"assetPositions[{index}] leverage must be a positive integer") from exc
    if parsed <= 0 or parsed != parsed.to_integral_value():
        raise ValueError(f"assetPositions[{index}] leverage must be a positive integer")
    return int(parsed)


def parse_clearinghouse_positions(
    payload: dict[str, Any], observed_ms: int | None = None, *, dex: str = ""
) -> dict[str, Position]:
    if not isinstance(payload, dict):
        raise ValueError("clearinghouse state must be an object")
    if "assetPositions" not in payload:
        raise ValueError("clearinghouse state is missing assetPositions")
    raw_positions = payload["assetPositions"]
    if not isinstance(raw_positions, list):
        raise ValueError("clearinghouse state assetPositions must be a list")

    positions: dict[str, Position] = {}
    seen_coins: set[str] = set()
    updated_ms = observed_ms if observed_ms is not None else now_ms()
    for index, item in enumerate(raw_positions):
        if not isinstance(item, dict):
            raise ValueError(f"assetPositions[{index}] must be an object")
        position = item.get("position", item)
        if not isinstance(position, dict):
            raise ValueError(f"assetPositions[{index}].position must be an object")
        raw_coin = position.get("coin")
        if not isinstance(raw_coin, str) or not raw_coin.strip():
            raise ValueError(f"assetPositions[{index}] is missing a valid coin")
        coin = qualify_market_symbol(dex, raw_coin)
        if coin in seen_coins:
            raise ValueError(f"clearinghouse state contains duplicate position coin {coin}")
        seen_coins.add(coin)
        if "szi" not in position:
            raise ValueError(f"assetPositions[{index}] is missing szi")
        size = parse_decimal(position["szi"])
        leverage = _parse_position_leverage(position.get("leverage"), index=index)
        if size == 0:
            continue
        entry_raw = position.get("entryPx")
        entry_px = parse_decimal(entry_raw) if entry_raw is not None and entry_raw != "" else None
        if entry_px is not None and entry_px <= 0:
            raise ValueError(f"assetPositions[{index}] entryPx must be positive")
        positions[coin] = Position(
            coin=coin,
            size=size,
            entry_px=entry_px,
            leverage=leverage,
            updated_ms=updated_ms,
        )
    return positions


def parse_open_orders(
    payload: list[dict[str, Any]] | dict[str, Any],
    observed_ms: int | None = None,
    *,
    dex: str = "",
) -> list[OpenOrder]:
    if isinstance(payload, dict):
        if "orders" not in payload:
            raise ValueError("open-orders response is missing orders")
        raw_orders = payload["orders"]
    elif isinstance(payload, list):
        raw_orders = payload
    else:
        raise ValueError("open-orders response must be a list or an orders wrapper")
    if not isinstance(raw_orders, list):
        raise ValueError("open-orders orders must be a list")

    orders: list[OpenOrder] = []
    seen_oids: set[int] = set()
    seen_cloids: set[str] = set()
    updated_ms = observed_ms if observed_ms is not None else now_ms()
    for index, order in enumerate(raw_orders):
        if not isinstance(order, dict):
            raise ValueError(f"open order {index} must be an object")
        raw_coin = order.get("coin")
        if not isinstance(raw_coin, str) or not raw_coin.strip():
            raise ValueError(f"open order {index} is missing a valid coin")
        coin = qualify_market_symbol(dex, raw_coin)
        side_signals: list[str] = []
        for field in ("side", "dir"):
            raw_side = order.get(field)
            if raw_side is None or raw_side == "":
                continue
            normalized_side = _normalize_order_side(raw_side)
            if normalized_side not in {"buy", "sell"}:
                raise ValueError(f"open order {index} has invalid {field} {raw_side!r}")
            side_signals.append(normalized_side)
        if "isBuy" in order:
            is_buy = order["isBuy"]
            if not isinstance(is_buy, bool):
                raise ValueError(f"open order {index} isBuy must be boolean")
            side_signals.append("buy" if is_buy else "sell")
        if not side_signals:
            raise ValueError(f"open order {index} has invalid side ''")
        if len(set(side_signals)) != 1:
            raise ValueError(f"open order {index} has conflicting side signals")
        side = side_signals[0]
        if "sz" in order:
            raw_size = order["sz"]
        elif "origSz" in order:
            raw_size = order["origSz"]
        else:
            raise ValueError(f"open order {index} is missing sz/origSz")
        size = parse_decimal(raw_size)
        if size <= 0:
            raise ValueError(f"open order {index} size must be positive")
        raw_price = order.get("limitPx", order.get("px"))
        price = parse_decimal(raw_price) if raw_price is not None and raw_price != "" else None
        if price is not None and price <= 0:
            raise ValueError(f"open order {index} price must be positive")

        raw_oid = order.get("oid")
        oid: int | None = None
        if raw_oid is not None and raw_oid != "":
            if isinstance(raw_oid, bool):
                raise ValueError(f"open order {index} has invalid oid")
            try:
                oid = int(str(raw_oid))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"open order {index} has invalid oid") from exc
            if oid <= 0:
                raise ValueError(f"open order {index} oid must be positive")
            if oid in seen_oids:
                raise ValueError(f"open-orders response contains duplicate oid {oid}")
            seen_oids.add(oid)

        raw_cloid = order.get("cloid")
        cloid: str | None = None
        if raw_cloid is not None and raw_cloid != "":
            if not isinstance(raw_cloid, str):
                raise ValueError(f"open order {index} has invalid cloid")
            cloid = validate_cloid(raw_cloid)
            if cloid in seen_cloids:
                raise ValueError(f"open-orders response contains duplicate cloid {cloid}")
            seen_cloids.add(cloid)
        if oid is None and cloid is None:
            raise ValueError(f"open order {index} is missing oid and cloid")

        reduce_only = order.get("reduceOnly", False)
        if not isinstance(reduce_only, bool):
            raise ValueError(f"open order {index} reduceOnly must be boolean")
        orders.append(
            OpenOrder(
                coin=coin,
                side=side,
                size=size,
                price=price,
                oid=oid,
                cloid=cloid,
                reduce_only=reduce_only,
                updated_ms=updated_ms,
            )
        )
    return orders


def _sorted_dexes(dexes: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(dexes), key=lambda dex: (dex != "", dex)))


def _configured_market_dexes(symbols: Iterable[str]) -> tuple[str, ...]:
    return _sorted_dexes(["", *(market_dex(symbol) for symbol in symbols)])


def _aggregate_positions(
    snapshot: UnifiedAccountSnapshot,
    *,
    observed_ms: int,
) -> dict[str, Position]:
    positions: dict[str, Position] = {}
    for dex in _sorted_dexes(snapshot.clearinghouse_states):
        state_positions = parse_clearinghouse_positions(
            snapshot.clearinghouse_states[dex],
            observed_ms,
            dex=dex,
        )
        for symbol, position in state_positions.items():
            if symbol in positions:
                raise ValueError(
                    f"aggregate clearinghouse state contains duplicate canonical position {symbol}"
                )
            positions[symbol] = position
    return positions


def _merge_open_orders(
    responses: dict[str, Any],
    *,
    observed_ms: int,
) -> list[OpenOrder]:
    orders: list[OpenOrder] = []
    seen_oids: set[int] = set()
    seen_cloids: set[str] = set()
    for dex in _sorted_dexes(responses):
        dex_orders = parse_open_orders(responses[dex], observed_ms, dex=dex)
        for order in dex_orders:
            if order.oid is not None:
                if order.oid in seen_oids:
                    raise ValueError(
                        f"multi-DEX open-orders responses contain duplicate oid {order.oid}"
                    )
                seen_oids.add(order.oid)
            if order.cloid is not None:
                if order.cloid in seen_cloids:
                    raise ValueError(
                        f"multi-DEX open-orders responses contain duplicate cloid {order.cloid}"
                    )
                seen_cloids.add(order.cloid)
            orders.append(order)
    return orders


def _merge_mids(
    responses: dict[str, Any],
    *,
    allowed_symbols: Iterable[str] | None = None,
) -> tuple[dict[str, Decimal], dict[str, Any]]:
    mids: dict[str, Decimal] = {}
    raw_mids: dict[str, Any] = {}
    allowed = (
        {canonical_market_symbol(symbol) for symbol in allowed_symbols}
        if allowed_symbols is not None
        else None
    )
    for dex in _sorted_dexes(responses):
        response = responses[dex]
        if not isinstance(response, dict):
            raise ValueError(f"allMids response for DEX {dex or '<default>'} must be an object")
        for raw_symbol, raw_mid in response.items():
            candidate = str(raw_symbol).strip()
            if dex and ":" not in candidate:
                candidate = f"{dex}:{candidate}"
            try:
                candidate = canonical_market_symbol(candidate)
            except ValueError:
                continue
            if allowed is not None and candidate not in allowed:
                continue
            symbol = qualify_market_symbol(dex, raw_symbol)
            if symbol in mids:
                raise ValueError(f"multi-DEX allMids responses contain duplicate market {symbol}")
            mids[symbol] = parse_decimal(raw_mid)
            raw_mids[symbol] = raw_mid
    return mids, raw_mids


def source_state_key(
    source_wallet: str,
    positions: dict[str, Position],
    open_orders: list[OpenOrder],
) -> str:
    return deterministic_cloid(
        "source-state",
        source_wallet.lower(),
        _stable_positions(positions),
        _stable_open_orders(open_orders),
    )


def source_planning_exposure_key(source_wallet: str, positions: dict[str, Position]) -> str:
    return deterministic_cloid(
        "source-planning-exposure",
        source_wallet.lower(),
        _stable_positions(positions),
    )


def source_planning_key(
    source_wallet: str,
    *,
    planning_exposure_key: str,
    event_key: str,
    previous: tuple[str, str] | None = None,
) -> str:
    if previous is not None:
        previous_exposure_key, previous_planning_key = previous
        if previous_exposure_key == planning_exposure_key:
            return previous_planning_key
    return deterministic_cloid(
        "source-planning-epoch",
        source_wallet.lower(),
        planning_exposure_key,
        event_key,
    )


def _stable_positions(positions: dict[str, Position]) -> dict[str, dict[str, Any]]:
    return {
        canonical_market_symbol(coin): {
            "coin": canonical_market_symbol(position.coin),
            "size": position.size,
            "entry_px": position.entry_px,
            "leverage": position.leverage,
        }
        for coin, position in sorted(positions.items())
    }


def _stable_open_orders(open_orders: list[OpenOrder]) -> list[dict[str, Any]]:
    orders = [
        {
            "coin": canonical_market_symbol(order.coin),
            "side": order.side.lower(),
            "size": order.size,
            "price": order.price,
            "oid": order.oid,
            "cloid": order.cloid.lower() if order.cloid else None,
            "reduce_only": order.reduce_only,
        }
        for order in open_orders
    ]
    return sorted(
        orders,
        key=lambda order: (
            order["coin"],
            order["side"],
            str(order["size"]),
            str(order["price"]),
            order["oid"] if order["oid"] is not None else -1,
            order["cloid"] or "",
            order["reduce_only"],
        ),
    )


def _normalize_order_side(side_raw: Any) -> str:
    side = str(side_raw or "").strip().lower()
    if side in {"b", "bid", "buy", "open long", "close short"}:
        return "buy"
    if side in {"a", "ask", "sell", "open short", "close long"}:
        return "sell"
    return side


def normalize_ws_message(source_wallet: str, message: dict[str, Any]) -> SourceEvent:
    if not isinstance(message, dict):
        raise SourceWebsocketMessageError("websocket message is not an object")
    source_wallet = source_wallet.lower()
    channel = str(message.get("channel", "unknown"))
    data = message.get("data", {})
    observed = now_ms()
    classification = classify_ws_source_event(channel, data)
    if data is None or data == "":
        raise SourceWebsocketMessageError(f"{channel} websocket message has empty data")
    explicit_users = _explicit_user_addresses(
        {key: message.get(key) for key in ("user", "userAddress", "account", "accountAddress")}
    )
    if channel == "userNonFundingLedgerUpdates":
        explicit_users.update(_top_level_user_addresses(data))
    else:
        explicit_users.update(_explicit_user_addresses(data))
    if channel == "userEvents":
        explicit_users.update(_user_event_owner_addresses(data))
    mismatched_users = sorted(user for user in explicit_users if user != source_wallet)
    if mismatched_users:
        raise SourceWebsocketMessageError(
            f"{channel} websocket user mismatch: expected {source_wallet}, got {mismatched_users}"
        )

    if not isinstance(data, dict | list) and channel != "notification":
        raise SourceWebsocketMessageError(f"{channel} websocket data shape is unsupported")
    exchange_ts = _extract_channel_exchange_timestamp(channel, data)
    timestamp_source = "exchange"
    if exchange_ts <= 0 and not classification.timestamp_required:
        exchange_ts = observed
        timestamp_source = "observed"
    if exchange_ts <= 0:
        raise SourceWebsocketMessageError(f"{channel} websocket message is missing timestamp")
    key = deterministic_cloid(
        "source-ws", source_wallet.lower(), channel, classification.subtype, data
    )
    copy_signal_key = _copy_signal_key(
        source_wallet,
        subtype=classification.subtype,
        data=data,
        summary=classification.summary,
    )
    return SourceEvent(
        idempotency_key=key,
        event_type=classification.event_type,
        source_wallet=source_wallet.lower(),
        exchange_ts_ms=exchange_ts,
        observed_ts_ms=observed,
        payload={
            "channel": channel,
            "data": data,
            "event_subtype": classification.subtype,
            "timestamp_source": timestamp_source,
            **classification.summary,
            **({"copy_signal_key": copy_signal_key} if copy_signal_key else {}),
            "raw_message": message,
        },
    )


def _copy_signal_key(
    source_wallet: str,
    *,
    subtype: str,
    data: Any,
    summary: dict[str, Any],
) -> str:
    """Return a stable copy-relevant identity for repeating state snapshots."""

    if subtype == "position_snapshot":
        if not isinstance(data, dict):
            return ""
        state = data.get("clearinghouseState", data)
        if not isinstance(state, dict):
            return ""
        dex = str(data.get("dex") or "")
        positions = parse_clearinghouse_positions(state, observed_ms=0, dex=dex)
        return source_planning_exposure_key(source_wallet, positions)
    if subtype == "active_asset_data":
        return deterministic_cloid(
            "source-leverage-signal",
            source_wallet.lower(),
            summary.get("coins", []),
            summary.get("leverage", ""),
        )
    return ""


def classify_ws_source_event(channel: str, data: Any) -> WsEventClassification:
    if channel == "userFills":
        subtype = "fill_snapshot" if _is_snapshot(data) else "fill"
        return _classified(
            SourceEventType.FILL,
            subtype,
            data,
            timestamp_required=_collection_has_items(data, "fills"),
        )
    if channel == "userTwapSliceFills":
        subtype = "twap_slice_fill_snapshot" if _is_snapshot(data) else "twap_slice_fill"
        return _classified(
            SourceEventType.FILL,
            subtype,
            data,
            timestamp_required=_collection_has_items(data, "twapSliceFills"),
        )
    if channel == "userFundings":
        subtype = "funding_snapshot" if _is_snapshot(data) else "funding"
        return _classified(
            SourceEventType.SNAPSHOT,
            subtype,
            data,
            timestamp_required=_collection_has_items(data, "fundings"),
        )
    if channel == "userNonFundingLedgerUpdates":
        ledger_types = _ledger_update_types(data)
        subtype = "ledger_update" + (":" + ",".join(ledger_types) if ledger_types else "")
        event_type = (
            SourceEventType.POSITION if "liquidation" in ledger_types else SourceEventType.SNAPSHOT
        )
        return _classified(
            event_type,
            subtype,
            data,
            timestamp_required=_collection_has_items(
                data,
                "nonFundingLedgerUpdates",
                "updates",
            ),
        )
    if channel == "userTwapHistory":
        statuses = _unique_strings(_collect_values(data, {"status"}), lowercase=True)
        subtype = "twap_history" + (":" + ",".join(statuses) if statuses else "")
        return _classified(
            SourceEventType.SNAPSHOT,
            subtype,
            data,
            timestamp_required=_collection_has_items(data, "history"),
        )
    if channel == "twapStates":
        return _classified(
            SourceEventType.SNAPSHOT, "twap_state_snapshot", data, timestamp_required=False
        )
    if channel == "activeAssetData":
        return _classified(
            SourceEventType.LEVERAGE, "active_asset_data", data, timestamp_required=False
        )
    if channel == "notification":
        return _classified(
            SourceEventType.SNAPSHOT, "account_notification", data, timestamp_required=False
        )
    if channel == "user":
        return _classified(
            SourceEventType.SNAPSHOT, "user_snapshot", data, timestamp_required=False
        )
    if channel in {"webData", "webData2", "webData3"}:
        return _classified(
            SourceEventType.SNAPSHOT, "web_data_snapshot", data, timestamp_required=False
        )
    if channel == "spotState":
        return _classified(
            SourceEventType.SNAPSHOT, "spot_state_snapshot", data, timestamp_required=False
        )
    if channel == "allDexsClearinghouseState":
        return _classified(
            SourceEventType.SNAPSHOT,
            "all_dexs_position_snapshot",
            data,
            timestamp_required=False,
        )
    if channel == "userEvents":
        if not isinstance(data, dict):
            raise SourceWebsocketMessageError("userEvents websocket data shape is unsupported")
        if "fills" in data:
            subtype = "fill_snapshot" if _is_snapshot(data) else "fill"
            return _classified(
                SourceEventType.FILL,
                subtype,
                data,
                timestamp_required=_collection_has_items(data, "fills"),
            )
        if "funding" in data:
            return _classified(SourceEventType.SNAPSHOT, "funding", data, timestamp_required=True)
        if "liquidation" in data:
            return _classified(
                SourceEventType.POSITION, "liquidation", data, timestamp_required=False
            )
        if "nonUserCancel" in data:
            return _classified(
                SourceEventType.CANCEL, "non_user_cancel", data, timestamp_required=False
            )
        raise SourceWebsocketMessageError("unsupported userEvents websocket event")
    if channel == "orderUpdates":
        statuses = _unique_strings(_collect_values(data, {"status"}), lowercase=True)
        subtype = "order_update" + (":" + ",".join(statuses) if statuses else "")
        return _classified(
            _order_update_event_type(statuses),
            subtype,
            data,
            timestamp_required=_collection_has_items(data),
        )
    if channel == "clearinghouseState":
        return _classified(
            SourceEventType.POSITION, "position_snapshot", data, timestamp_required=False
        )
    if channel == "openOrders":
        return _classified(
            SourceEventType.OPEN_ORDER, "open_order_snapshot", data, timestamp_required=False
        )
    raise SourceWebsocketMessageError(f"unsupported source websocket channel: {channel}")


def _classified(
    event_type: SourceEventType,
    subtype: str,
    data: Any,
    *,
    timestamp_required: bool,
) -> WsEventClassification:
    return WsEventClassification(
        event_type=event_type,
        subtype=subtype,
        timestamp_required=timestamp_required,
        summary=_ws_event_summary(data, subtype),
    )


def _ws_event_summary(data: Any, subtype: str) -> dict[str, Any]:
    return {
        "event_count": _event_count(data, subtype),
        "coins": _unique_strings(_collect_values(data, {"coin"}), uppercase=True),
        "oids": _unique_strings(_collect_values(data, {"oid"})),
        "cloids": _unique_strings(_collect_values(data, {"cloid"}), lowercase=True),
        "statuses": _unique_strings(_collect_values(data, {"status"}), lowercase=True),
        "sides": _unique_strings(_collect_values(data, {"side", "dir"})),
        "hashes": _unique_strings(_collect_values(data, {"hash"}), lowercase=True),
        "twap_ids": _unique_strings(_collect_values(data, {"twapId"})),
        "ledger_types": _ledger_update_types(data),
        "dexs": _dex_names(data),
        "leverage": _leverage_summary(data),
        "available_to_trade": _numeric_pair_summary(data, "availableToTrade"),
        "max_trade_sizes": _numeric_pair_summary(data, "maxTradeSzs"),
        "is_snapshot": _is_snapshot(data),
    }


def _event_count(data: Any, subtype: str) -> int:
    if isinstance(data, list):
        return len(data)
    if not isinstance(data, dict):
        return 1
    if subtype in {"fill", "fill_snapshot"}:
        fills = data.get("fills", data)
        return len(fills) if isinstance(fills, list) else 1
    if subtype == "non_user_cancel":
        cancels = data.get("nonUserCancel", [])
        return len(cancels) if isinstance(cancels, list) else 1
    if subtype == "position_snapshot":
        state = data.get("clearinghouseState", data)
        positions = state.get("assetPositions", []) if isinstance(state, dict) else []
        return len(positions) if isinstance(positions, list) else 1
    if subtype == "user_snapshot":
        positions = data.get("assetPositions", [])
        return len(positions) if isinstance(positions, list) else 1
    if subtype == "open_order_snapshot":
        orders = data.get("orders", [])
        return len(orders) if isinstance(orders, list) else 1
    if subtype.startswith("funding"):
        fundings = data.get("fundings", data)
        return len(fundings) if isinstance(fundings, list) else 1
    if subtype.startswith("ledger_update"):
        updates = data.get("nonFundingLedgerUpdates", data.get("updates", data))
        return len(updates) if isinstance(updates, list) else 1
    if subtype.startswith("twap_slice_fill"):
        fills = data.get("twapSliceFills", data)
        return len(fills) if isinstance(fills, list) else 1
    if subtype.startswith("twap_history"):
        history = data.get("history", data)
        return len(history) if isinstance(history, list) else 1
    if subtype == "twap_state_snapshot":
        states = data.get("states", data)
        return len(states) if isinstance(states, list) else 1
    if subtype == "spot_state_snapshot":
        spot_state = data.get("spotState", data)
        balances = spot_state.get("balances", []) if isinstance(spot_state, dict) else []
        return len(balances) if isinstance(balances, list) else 1
    if subtype == "all_dexs_position_snapshot":
        return _all_dex_position_count(data)
    if subtype == "web_data_snapshot":
        dex_states = data.get("perpDexStates", []) if isinstance(data, dict) else []
        return len(dex_states) if isinstance(dex_states, list) else 1
    if subtype == "active_asset_data":
        return 1
    return 1


def _leverage_summary(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    leverage = data.get("leverage")
    if isinstance(leverage, dict):
        leverage_type = str(leverage.get("type") or "").strip()
        value = leverage.get("value")
        if leverage_type and value not in (None, ""):
            return f"{leverage_type}:{value}"
        if leverage_type:
            return leverage_type
        if value not in (None, ""):
            return str(value)
        return ""
    if leverage not in (None, ""):
        return str(leverage)
    return ""


def _numeric_pair_summary(data: Any, key: str) -> str:
    if not isinstance(data, dict):
        return ""
    value = data.get(key)
    if not isinstance(value, list | tuple):
        return ""
    return "/".join(str(item) for item in value)


def _all_dex_position_count(data: Any) -> int:
    if not isinstance(data, dict):
        return 1
    states = data.get("clearinghouseStates", {})
    if not isinstance(states, dict):
        return 1
    count = 0
    for state in states.values():
        if isinstance(state, dict):
            positions = state.get("assetPositions", [])
            if isinstance(positions, list):
                count += len(positions)
    return count


def _dex_names(data: Any) -> list[str]:
    names = set(_unique_strings(_collect_values(data, {"dex"}), lowercase=True))
    if isinstance(data, dict) and isinstance(data.get("clearinghouseStates"), dict):
        names.update(str(key).lower() for key in data["clearinghouseStates"] if str(key).strip())
    return sorted(names)


def _ledger_update_types(data: Any) -> list[str]:
    updates: list[Any]
    if isinstance(data, dict):
        raw_updates = (
            data["nonFundingLedgerUpdates"]
            if "nonFundingLedgerUpdates" in data
            else data.get("updates")
        )
        updates = raw_updates if isinstance(raw_updates, list) else [data]
    elif isinstance(data, list):
        updates = data
    else:
        updates = [data]
    types: list[Any] = []
    for update in updates:
        if isinstance(update, dict):
            delta = update.get("delta", update)
            if isinstance(delta, dict):
                types.append(delta.get("type"))
    return _unique_strings(types, lowercase=True)


def _order_update_event_type(statuses: list[str]) -> SourceEventType:
    if any(
        status in {"canceled", "scheduledcancel"} or status.endswith("canceled")
        for status in statuses
    ):
        return SourceEventType.CANCEL
    return SourceEventType.OPEN_ORDER


def _channel_uses_exchange_timestamps(channel: str) -> bool:
    return channel in EXCHANGE_TIMESTAMP_CHANNELS


def _extract_channel_exchange_timestamp(channel: str, data: Any) -> int:
    if not _channel_uses_exchange_timestamps(channel):
        return 0
    if channel == "orderUpdates":
        return _extract_record_timestamp(data, "statusTimestamp")
    if channel == "userTwapHistory":
        return _extract_record_timestamp(_twap_history_records(data), "time")
    return _extract_exchange_timestamp(data)


def _twap_history_records(value: Any) -> list[Any]:
    if isinstance(value, dict):
        history = value.get("history")
        if isinstance(history, list):
            return history
        return [value]
    if isinstance(value, list):
        return value
    return []


def _extract_record_timestamp(value: Any, key: str) -> int:
    records = value if isinstance(value, list) else [value]
    timestamps = [
        timestamp
        for record in records
        if isinstance(record, dict)
        for timestamp in [_as_positive_int(record.get(key))]
        if timestamp
    ]
    return max(timestamps, default=0)


def _extract_exchange_timestamp(value: Any) -> int:
    timestamps: list[int] = []
    _collect_timestamps(value, timestamps)
    return max(timestamps, default=0)


def _collect_timestamps(value: Any, timestamps: list[int]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in TIMESTAMP_KEYS:
                timestamp = _as_positive_int(item)
                if timestamp:
                    timestamps.append(timestamp)
            _collect_timestamps(item, timestamps)
        return
    if isinstance(value, list):
        for item in value:
            _collect_timestamps(item, timestamps)


def _collect_values(value: Any, keys: set[str]) -> list[Any]:
    found: list[Any] = []
    normalized_keys = {key.lower() for key in keys}
    if isinstance(value, dict):
        for key, item in value.items():
            if key.lower() in normalized_keys and item is not None and item != "":
                found.append(item)
            found.extend(_collect_values(item, keys))
    elif isinstance(value, list):
        for item in value:
            found.extend(_collect_values(item, keys))
    return found


def _unique_strings(
    values: list[Any],
    *,
    uppercase: bool = False,
    lowercase: bool = False,
) -> list[str]:
    result: set[str] = set()
    for value in values:
        if isinstance(value, dict | list):
            continue
        text = str(value).strip()
        if not text:
            continue
        if uppercase:
            text = text.upper()
        elif lowercase:
            text = text.lower()
        result.add(text)
    return sorted(result)


def _is_snapshot(data: Any) -> bool:
    return isinstance(data, dict) and data.get("isSnapshot") is True


def _collection_has_items(data: Any, *keys: str) -> bool:
    if isinstance(data, list):
        return len(data) > 0
    if not isinstance(data, dict):
        return True
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return len(value) > 0
    return True


def _as_positive_int(value: Any) -> int:
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return 0
    return timestamp if timestamp > 0 else 0


def normalize_fill_backfill(source_wallet: str, fill: dict[str, Any]) -> SourceEvent:
    if not isinstance(fill, dict):
        raise SourceWebsocketMessageError("backfilled fill is not an object")
    source_wallet = source_wallet.lower()
    exchange_ts = _as_positive_int(fill.get("time") or fill.get("T") or fill.get("timestamp"))
    if exchange_ts <= 0:
        raise SourceWebsocketMessageError("backfilled fill is missing timestamp")
    explicit_users = _explicit_user_addresses(fill)
    mismatched_users = sorted(user for user in explicit_users if user != source_wallet)
    if mismatched_users:
        raise SourceWebsocketMessageError(
            f"backfilled fill user mismatch: expected {source_wallet}, got {mismatched_users}"
        )
    key = deterministic_cloid(
        "source-fill-backfill",
        source_wallet,
        fill.get("hash"),
        fill.get("tid"),
        fill.get("oid"),
        exchange_ts,
        fill,
    )
    summary = _ws_event_summary(fill, "fill_backfill")
    return SourceEvent(
        idempotency_key=key,
        event_type=SourceEventType.FILL,
        source_wallet=source_wallet,
        exchange_ts_ms=exchange_ts,
        observed_ts_ms=now_ms(),
        payload={
            "channel": "userFillsByTime",
            "data": fill,
            "event_subtype": "fill_backfill",
            "timestamp_source": "exchange",
            **summary,
            "raw_fill": fill,
        },
    )


def normalize_twap_slice_fill_backfill(
    source_wallet: str, slice_fill: dict[str, Any]
) -> SourceEvent:
    if not isinstance(slice_fill, dict):
        raise SourceWebsocketMessageError("backfilled TWAP slice fill is not an object")
    source_wallet = source_wallet.lower()
    exchange_ts = _extract_exchange_timestamp(slice_fill)
    if exchange_ts <= 0:
        raise SourceWebsocketMessageError("backfilled TWAP slice fill is missing timestamp")
    explicit_users = _explicit_user_addresses(slice_fill)
    mismatched_users = sorted(user for user in explicit_users if user != source_wallet)
    if mismatched_users:
        raise SourceWebsocketMessageError(
            f"backfilled TWAP slice fill user mismatch: expected {source_wallet}, got {mismatched_users}"
        )
    fill = slice_fill.get("fill", {}) if isinstance(slice_fill.get("fill"), dict) else {}
    key = deterministic_cloid(
        "source-twap-slice-backfill",
        source_wallet,
        slice_fill.get("twapId"),
        fill.get("hash"),
        fill.get("tid"),
        fill.get("oid"),
        exchange_ts,
        slice_fill,
    )
    summary = _ws_event_summary({"twapSliceFills": [slice_fill]}, "twap_slice_fill_backfill")
    return SourceEvent(
        idempotency_key=key,
        event_type=SourceEventType.FILL,
        source_wallet=source_wallet,
        exchange_ts_ms=exchange_ts,
        observed_ts_ms=now_ms(),
        payload={
            "channel": "userTwapSliceFillsByTime",
            "data": slice_fill,
            "event_subtype": "twap_slice_fill_backfill",
            "timestamp_source": "exchange",
            **summary,
            "raw_twap_slice_fill": slice_fill,
        },
    )


def _explicit_user_addresses(value: Any) -> set[str]:
    if isinstance(value, dict):
        found: set[str] = set()
        for key, item in value.items():
            if key.replace("_", "").replace("-", "").lower() in USER_ADDRESS_KEYS:
                if isinstance(item, str) and _looks_like_address(item):
                    found.add(item.lower())
                continue
            found.update(_explicit_user_addresses(item))
        return found
    if isinstance(value, list):
        nested: set[str] = set()
        for item in value:
            nested.update(_explicit_user_addresses(item))
        return nested
    return set()


def _top_level_user_addresses(value: Any) -> set[str]:
    if not isinstance(value, dict):
        return set()
    return _explicit_user_addresses(
        {key: value.get(key) for key in ("user", "userAddress", "account", "accountAddress")}
    )


def _user_event_owner_addresses(value: Any) -> set[str]:
    if not isinstance(value, dict):
        return set()
    liquidation = value.get("liquidation")
    if not isinstance(liquidation, dict):
        return set()
    found: set[str] = set()
    for key in ("liquidated_user", "liquidatedUser"):
        user = liquidation.get(key)
        if isinstance(user, str) and _looks_like_address(user):
            found.add(user.lower())
    return found


def _looks_like_address(value: str) -> bool:
    lowered = value.lower()
    return (
        len(lowered) == 42
        and lowered.startswith("0x")
        and all(char in "0123456789abcdef" for char in lowered[2:])
    )


def _source_account_mode(value: Any) -> str:
    mode = classify_user_abstraction(value)
    if mode == HyperliquidUserAbstraction.STANDARD:
        return "standard"
    if mode == HyperliquidUserAbstraction.UNIFIED:
        return "unified"
    if mode in {
        HyperliquidUserAbstraction.PORTFOLIO_MARGIN,
        HyperliquidUserAbstraction.DEX_ABSTRACTION,
    }:
        raise ValueError(f"source account abstraction mode {mode.value} is unsupported")
    raise ValueError(f"source userAbstraction response is unrecognized: {value}")


def _source_dex_abstraction_enabled(value: Any) -> bool:
    if value is None:
        return False
    mode = normalized_abstraction_mode(value)
    if mode in {"false", "disabled", "default"}:
        return False
    if mode in {"true", "enabled", "dexabstraction"}:
        return True
    raise ValueError(f"source userDexAbstraction response is unrecognized: {value}")


def _source_account_value(
    state: Any,
    *,
    account_mode: str,
    spot_state: Any,
) -> Decimal:
    if account_mode == "standard":
        if not isinstance(state, dict):
            raise ValueError("source clearinghouseState must be an object")
        for key in ("marginSummary", "crossMarginSummary"):
            summary = state.get(key)
            if isinstance(summary, dict) and summary.get("accountValue") is not None:
                return parse_decimal(summary["accountValue"])
        raise ValueError("source clearinghouseState is missing accountValue")
    if not isinstance(spot_state, dict) or not isinstance(spot_state.get("balances"), list):
        raise ValueError("unified source spotClearinghouseState is missing balances")
    balances = [
        item
        for item in spot_state["balances"]
        if isinstance(item, dict) and str(item.get("coin") or "").upper() == "USDC"
    ]
    if len(balances) != 1:
        raise ValueError("unified source requires exactly one Spot USDC balance")
    token = balances[0].get("token")
    if token is None or int(str(token)) != 0:
        raise ValueError("unified source Spot USDC must use collateral token index 0")
    total = parse_decimal(balances[0].get("total"))
    hold = parse_decimal(balances[0].get("hold"))
    if total < 0 or hold < 0 or hold > total:
        raise ValueError("unified source Spot USDC total/hold is invalid")
    return total


def _require_fresh_unified_snapshot(
    snapshot: UnifiedAccountSnapshot,
    *,
    expected_account: str,
    stale_after_ms: int,
    current_ms: int,
) -> None:
    if snapshot.account.strip().lower() != expected_account.strip().lower():
        raise ValueError(
            "aggregate unified source account mismatch: "
            f"expected {expected_account.lower()}, got {snapshot.account.lower()}"
        )
    if "" not in snapshot.clearinghouse_states:
        raise ValueError("aggregate unified source is missing the default perp DEX")
    if snapshot.received_ms <= 0:
        raise ValueError("aggregate unified source has invalid receipt time")
    age_ms = max(0, current_ms - snapshot.received_ms)
    if age_ms > stale_after_ms:
        raise ValueError(
            f"aggregate unified source is stale: age_ms={age_ms} threshold_ms={stale_after_ms}"
        )


class SourceObserver:
    def __init__(
        self,
        *,
        source_wallet: str,
        info_client: InfoClient,
        store: SQLiteStore,
        ws_url: str | None = None,
        shield: ConsistencyShield | None = None,
        active_asset_symbols: Iterable[str] = (),
        websocket_idle_timeout_ms: int = 55_000,
        websocket_heartbeat_timeout_ms: int = 5_000,
        rest_url: str | None = None,
        info_timeout_s: float = 10.0,
        stale_after_ms: int = 10_000,
        reconnect_attempts: int = 5,
        reconnect_backoff_ms: int = 1_000,
        unified_state_provider: Callable[[], UnifiedAccountSnapshot] | None = None,
        source_dex_scope: SourceDexScope = SourceDexScope.STRICT,
        open_orders_cache_ttl_ms: int = SOURCE_OPEN_ORDERS_CACHE_TTL_MS,
        market_mids_provider: Callable[[], dict[str, Decimal]] | None = None,
    ):
        self.source_wallet = source_wallet.lower()
        self.info_client = info_client
        self.store = store
        self.ws_url = ws_url
        self.shield = shield
        self.active_asset_symbols = _normalized_symbols(active_asset_symbols)
        self.websocket_idle_timeout_ms = websocket_idle_timeout_ms
        self.websocket_heartbeat_timeout_ms = websocket_heartbeat_timeout_ms
        self.rest_url = rest_url
        self.info_timeout_s = info_timeout_s
        self.stale_after_ms = stale_after_ms
        self.reconnect_attempts = reconnect_attempts
        self.reconnect_backoff_ms = reconnect_backoff_ms
        self.source_dex_scope = SourceDexScope(source_dex_scope)
        self._open_orders_cache = _SourceOpenOrderCache(
            source_wallet=self.source_wallet,
            ttl_ms=open_orders_cache_ttl_ms,
        )
        self._market_mids_provider = market_mids_provider
        self._account_identity_cache: tuple[str, bool, int] | None = None
        self._unified_state_provider = unified_state_provider
        self._unified_state_stream: UnifiedAccountStateStream | None = None

    def _unified_snapshot(self) -> UnifiedAccountSnapshot:
        if self._unified_state_provider is not None:
            return self._unified_state_provider()
        if not self.rest_url:
            raise ValueError("unified source requires aggregate all-DEX WebSocket truth")
        if self._unified_state_stream is None:
            self._unified_state_stream = UnifiedAccountStateStream(
                rest_url=self.rest_url,
                account=self.source_wallet,
                timeout_s=self.info_timeout_s,
                stale_after_ms=self.stale_after_ms,
                reconnect_attempts=self.reconnect_attempts,
                reconnect_backoff_ms=self.reconnect_backoff_ms,
            )
        return self._unified_state_stream.snapshot()

    def _source_account_identity(
        self,
        *,
        observed_ms: int,
    ) -> tuple[str, bool, dict[str, Any]]:
        cached = self._account_identity_cache
        if cached is not None:
            account_mode, dex_abstraction_enabled, cached_ms = cached
            age_ms = observed_ms - cached_ms
            if 0 <= age_ms < SOURCE_ACCOUNT_IDENTITY_CACHE_TTL_MS:
                return (
                    account_mode,
                    dex_abstraction_enabled,
                    {
                        "fresh": True,
                        "age_ms": age_ms,
                        "observed_ms": cached_ms,
                        "ttl_ms": SOURCE_ACCOUNT_IDENTITY_CACHE_TTL_MS,
                        "refreshed": False,
                    },
                )
        abstraction = self.info_client.info({"type": "userAbstraction", "user": self.source_wallet})
        dex_abstraction = self.info_client.info(
            {"type": "userDexAbstraction", "user": self.source_wallet}
        )
        account_mode = _source_account_mode(abstraction)
        dex_abstraction_enabled = _source_dex_abstraction_enabled(dex_abstraction)
        self._account_identity_cache = (
            account_mode,
            dex_abstraction_enabled,
            observed_ms,
        )
        return (
            account_mode,
            dex_abstraction_enabled,
            {
                "fresh": True,
                "age_ms": 0,
                "observed_ms": observed_ms,
                "ttl_ms": SOURCE_ACCOUNT_IDENTITY_CACHE_TTL_MS,
                "refreshed": True,
            },
        )

    def reconcile_once(self) -> SourceSnapshot:
        # Use one observation timestamp for the complete reconciliation. Besides keeping the
        # snapshot internally coherent, this preserves deterministic clocks in replay/tests and
        # prevents cache bookkeeping from consuming an extra wall-clock sample.
        observed = now_ms()
        account_mode, dex_abstraction_enabled, account_identity_cache = (
            self._source_account_identity(observed_ms=observed)
        )
        if dex_abstraction_enabled:
            raise ValueError("source DEX abstraction is unsupported for deterministic sizing")
        state: Any = None
        spot_state = None
        unified_aggregate: UnifiedAccountSnapshot | None = None
        active_dexes: list[str] = []
        market_data_dexes: tuple[str, ...] = ("",)
        unavailable_configured_dexes: tuple[str, ...] = ()
        if (
            self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
            and account_mode != "unified"
        ):
            raise ValueError("all_configured_markets requires a Unified source account")
        if account_mode == "unified":
            spot_state = self.info_client.info(
                {"type": "spotClearinghouseState", "user": self.source_wallet}
            )
            unified_aggregate = self._unified_snapshot()
            active_dexes = non_default_dex_activity(unified_aggregate)
            if active_dexes and self.source_dex_scope == SourceDexScope.STRICT:
                raise ValueError(
                    "unified source has unsupported non-default DEX activity: "
                    + ", ".join(active_dexes)
                )
            if (
                active_dexes
                and self.source_dex_scope == SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY
                and any(market_dex(symbol) for symbol in self.active_asset_symbols)
            ):
                raise ValueError(
                    "default-only source DEX scope cannot select non-default DEX symbols"
                )
            if self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS:
                _require_fresh_unified_snapshot(
                    unified_aggregate,
                    expected_account=self.source_wallet,
                    stale_after_ms=self.stale_after_ms,
                    current_ms=observed,
                )
                configured_market_dexes = _configured_market_dexes(self.active_asset_symbols)
                available_market_dexes = set(unified_aggregate.clearinghouse_states)
                market_data_dexes = tuple(
                    dex for dex in configured_market_dexes if dex in available_market_dexes
                )
                unavailable_configured_dexes = tuple(
                    dex for dex in configured_market_dexes if dex not in available_market_dexes
                )
            state = unified_aggregate.default_state
        else:
            state = self.info_client.info(
                {"type": "clearinghouseState", "user": self.source_wallet}
            )
        open_order_responses: dict[str, Any] = {}
        open_order_cache_dexes: dict[str, Any] = {}
        open_order_responses[""], open_order_cache_dexes["<default>"] = (
            self._open_orders_cache.get_or_refresh(
                "",
                loader=lambda: self.info_client.info(
                    {"type": "openOrders", "user": self.source_wallet}
                ),
                current_ms=observed,
            )
        )
        if (
            account_mode == "unified"
            and self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
        ):
            assert unified_aggregate is not None
            positions = _aggregate_positions(unified_aggregate, observed_ms=observed)
            for dex in market_data_dexes:
                if not dex:
                    continue
                open_order_responses[dex], open_order_cache_dexes[dex] = (
                    self._open_orders_cache.get_or_refresh(
                        dex,
                        loader=lambda: self.info_client.info(
                            {"type": "openOrders", "user": self.source_wallet, "dex": dex}
                        ),
                        current_ms=observed,
                    )
                )
            orders = _merge_open_orders(open_order_responses, observed_ms=observed)
        else:
            positions = parse_clearinghouse_positions(state, observed)
            orders = parse_open_orders(open_order_responses[""], observed)
        open_order_cache = {
            "source_wallet": self.source_wallet,
            "ttl_ms": self._open_orders_cache.ttl_ms,
            "complete": bool(open_order_cache_dexes)
            and all(item["complete"] for item in open_order_cache_dexes.values()),
            "fresh": bool(open_order_cache_dexes)
            and all(item["fresh"] for item in open_order_cache_dexes.values()),
            "dexes": open_order_cache_dexes,
        }
        if not open_order_cache["complete"] or not open_order_cache["fresh"]:
            # This is a last-resort invariant. get_or_refresh normally raises before an expired
            # baseline can reach planning, so an incomplete/stale result must never authorize
            # new risk even if the cache implementation changes later.
            raise ValueError("source open-order cache is incomplete or stale")
        account_value = _source_account_value(
            state,
            account_mode=account_mode,
            spot_state=spot_state,
        )
        mid_responses: dict[str, Any]
        if self._market_mids_provider is not None:
            provided_mids = self._market_mids_provider()
            if not isinstance(provided_mids, dict):
                raise ValueError("shared market mids provider must return an object")
            allowed = set(self.active_asset_symbols) | set(positions)
            mids = {}
            mids_raw = {}
            for raw_symbol, raw_mid in provided_mids.items():
                symbol = canonical_market_symbol(raw_symbol)
                if symbol not in allowed:
                    continue
                price = parse_decimal(raw_mid)
                if price <= 0:
                    raise ValueError(f"shared market mid for {symbol} must be positive")
                mids[symbol] = price
                mids_raw[symbol] = raw_mid
            mid_responses = {"shared_execution_cache": mids_raw}
        elif (
            account_mode == "unified"
            and self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
        ):
            mid_responses = {}
            for dex in market_data_dexes:
                payload: dict[str, Any] = {"type": "allMids"}
                if dex:
                    payload["dex"] = dex
                mid_responses[dex] = self.info_client.info(payload)
            mids, mids_raw = _merge_mids(
                mid_responses,
                allowed_symbols=(*self.active_asset_symbols, *positions),
            )
        else:
            mid_responses = {"": self.info_client.info({"type": "allMids"})}
            mids_raw = mid_responses[""]
            allowed = set(self.active_asset_symbols) | set(positions)
            filtered_mids: dict[str, Decimal] = {}
            filtered_raw: dict[str, Any] = {}
            for raw_symbol, raw_mid in dict(mids_raw).items():
                candidate = str(raw_symbol).strip()
                if candidate not in allowed:
                    continue
                symbol = canonical_market_symbol(candidate)
                filtered_mids[symbol] = parse_decimal(raw_mid)
                filtered_raw[symbol] = raw_mid
            mids = filtered_mids
            mids_raw = filtered_raw
        unified_metadata: dict[str, Any] | None = None
        if unified_aggregate is not None:
            if self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS:
                unified_metadata = {
                    "observed_ms": unified_aggregate.observed_ms,
                    "received_ms": unified_aggregate.received_ms,
                    "dex_count": unified_aggregate.dex_count,
                    "dexes": list(_sorted_dexes(unified_aggregate.clearinghouse_states)),
                    "market_data_dexes": list(market_data_dexes),
                    "unavailable_configured_dexes": list(unavailable_configured_dexes),
                    "active_non_default_dexes": active_dexes,
                    "source_dex_scope": self.source_dex_scope.value,
                    "positions_scope": "all_perp_dexes",
                    "account_value_basis": "total_unified_spot_usdc",
                    "fidelity": "full_all_dex_positions_configured_market_data",
                }
            else:
                unified_metadata = {
                    "observed_ms": unified_aggregate.observed_ms,
                    "received_ms": unified_aggregate.received_ms,
                    "dex_count": unified_aggregate.dex_count,
                    "active_non_default_dexes": active_dexes,
                    "source_dex_scope": self.source_dex_scope.value,
                    "positions_scope": "default_perp_dex",
                    "account_value_basis": "total_unified_spot_usdc",
                    "fidelity": (
                        "reduced_non_default_positions_excluded"
                        if active_dexes
                        else "full_default_scope"
                    ),
                }
        state_key = source_state_key(self.source_wallet, positions, orders)
        planning_exposure_key = source_planning_exposure_key(self.source_wallet, positions)
        event_key = deterministic_cloid(
            "source-reconcile", self.source_wallet, observed, positions, orders
        )
        planning_key = source_planning_key(
            self.source_wallet,
            planning_exposure_key=planning_exposure_key,
            event_key=event_key,
            previous=self._previous_planning_identity(),
        )
        event = SourceEvent(
            idempotency_key=event_key,
            event_type=SourceEventType.RECONCILE,
            source_wallet=self.source_wallet,
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={
                "positions": positions,
                "open_orders": orders,
                "mids": mids_raw,
                "state_key": state_key,
                "planning_exposure_key": planning_exposure_key,
                "planning_key": planning_key,
                "account_mode": account_mode,
                "account_identity_cache": account_identity_cache,
                "account_value": account_value,
                "unified_aggregate": unified_metadata,
                "open_orders_cache": open_order_cache,
            },
        )
        self.store.append_source_event(event)
        return SourceSnapshot(
            positions=positions,
            open_orders=orders,
            mids=mids,
            observed_ms=observed,
            state_key=state_key,
            planning_key=planning_key,
            raw_state={
                "accountMode": account_mode,
                "accountIdentityCache": account_identity_cache,
                "accountValue": account_value,
                "clearinghouseState": state,
                "spotClearinghouseState": spot_state,
                "allDexsClearinghouseState": (
                    unified_aggregate.clearinghouse_states
                    if unified_aggregate is not None
                    and self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
                    else None
                ),
                "unifiedAggregate": unified_metadata,
                "openOrders": (
                    open_order_responses
                    if unified_aggregate is not None
                    and self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
                    else open_order_responses[""]
                ),
                "openOrdersCache": open_order_cache,
                "allMids": (
                    mid_responses
                    if unified_aggregate is not None
                    and self.source_dex_scope == SourceDexScope.ALL_CONFIGURED_MARKETS
                    else mids_raw
                ),
            },
        )

    def _previous_planning_identity(self) -> tuple[str, str] | None:
        return self.store.latest_source_planning_identity(
            source_wallet=self.source_wallet,
        )

    def record_source_event(self, event: SourceEvent) -> bool:
        already_seen = self.store.has_source_event(event.idempotency_key)
        if self.shield is not None and not _is_recovered_backfill_event(event):
            result = self.shield.observe_source_event(event, already_seen=already_seen)
            if not result.ok:
                raise SourceWebsocketMessageError(
                    f"source event rejected by consistency shield: {result.detail}"
                )
        return self.store.append_source_event(event, reaction_required=True)

    def record_ws_message(self, message: dict[str, Any]) -> bool:
        _, inserted = self.record_ws_message_event(message)
        return inserted

    def record_ws_message_event(self, message: dict[str, Any]) -> tuple[SourceEvent, bool]:
        try:
            event = normalize_ws_message(self.source_wallet, message)
        except SourceWebsocketMessageError as exc:
            if self.shield is not None:
                self.shield.missed_event_gap(f"unsupported source websocket message: {exc}")
            raise
        inserted = self.record_source_event(event)
        if message.get("channel") == "orderUpdates":
            try:
                self._open_orders_cache.apply_order_updates(
                    message.get("data"),
                    observed_ms=event.observed_ts_ms,
                )
            except ValueError as exc:
                detail = f"source orderUpdates could not update open-order cache: {exc}"
                if self.shield is not None:
                    self.shield.missed_event_gap(detail)
                raise SourceWebsocketMessageError(detail) from exc
        return event, inserted

    def open_order_cache_status(self) -> dict[str, Any]:
        """Return operator-visible cache age/completeness without making a REST request."""

        return self._open_orders_cache.status(current_ms=now_ms())

    def backfill_fills_by_time(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        max_pages: int = 5,
    ) -> FillBackfillReport:
        if start_time_ms < 0:
            raise ValueError("start_time_ms cannot be negative")
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        end = end_time_ms if end_time_ms is not None else now_ms()
        if end < start_time_ms:
            raise ValueError("end_time_ms cannot be before start_time_ms")
        page_start = start_time_ms
        pages = 0
        fetched = 0
        inserted = 0
        duplicates = 0
        warnings: list[str] = []
        completed = False
        while page_start <= end and pages < max_pages:
            payload = {
                "type": "userFillsByTime",
                "user": self.source_wallet,
                "startTime": page_start,
                "endTime": end,
                "aggregateByTime": False,
            }
            fills = self.info_client.info(payload)
            if not isinstance(fills, list):
                detail = "userFillsByTime response is not a list"
                if self.shield is not None:
                    self.shield.missed_event_gap(detail)
                raise SourceWebsocketMessageError(detail)
            pages += 1
            if not fills:
                completed = True
                break
            fetched += len(fills)
            max_seen_ts = page_start
            for fill in sorted(fills, key=lambda item: _extract_exchange_timestamp(item)):
                try:
                    event = normalize_fill_backfill(self.source_wallet, fill)
                except SourceWebsocketMessageError as exc:
                    detail = f"userFillsByTime returned malformed fill: {exc}"
                    if self.shield is not None:
                        self.shield.missed_event_gap(detail)
                    raise SourceWebsocketMessageError(detail) from exc
                max_seen_ts = max(max_seen_ts, event.exchange_ts_ms)
                if self.record_source_event(event):
                    inserted += 1
                else:
                    duplicates += 1
            if len(fills) < 2000:
                completed = True
                break
            if max_seen_ts <= page_start:
                warnings.append(
                    "userFillsByTime page was full but timestamps did not advance; "
                    "fill backfill may be incomplete"
                )
                break
            page_start = max_seen_ts + 1
        if not completed and page_start <= end and pages >= max_pages:
            warnings.append("source fill backfill hit max page limit before reaching end time")
        if warnings and self.shield is not None:
            self.shield.missed_event_gap("; ".join(warnings))
        return FillBackfillReport(
            start_time_ms=start_time_ms,
            end_time_ms=end,
            pages=pages,
            fetched=fetched,
            inserted=inserted,
            duplicates=duplicates,
            warnings=warnings,
        )

    def backfill_twap_slice_fills_by_time(
        self,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        max_pages: int = 5,
    ) -> FillBackfillReport:
        if start_time_ms < 0:
            raise ValueError("start_time_ms cannot be negative")
        if max_pages <= 0:
            raise ValueError("max_pages must be positive")
        end = end_time_ms if end_time_ms is not None else now_ms()
        if end < start_time_ms:
            raise ValueError("end_time_ms cannot be before start_time_ms")
        page_start = start_time_ms
        pages = 0
        fetched = 0
        inserted = 0
        duplicates = 0
        warnings: list[str] = []
        completed = False
        while page_start <= end and pages < max_pages:
            payload = {
                "type": "userTwapSliceFillsByTime",
                "user": self.source_wallet,
                "startTime": page_start,
                "endTime": end,
            }
            fills = self.info_client.info(payload)
            if not isinstance(fills, list):
                detail = "userTwapSliceFillsByTime response is not a list"
                if self.shield is not None:
                    self.shield.missed_event_gap(detail)
                raise SourceWebsocketMessageError(detail)
            pages += 1
            if not fills:
                completed = True
                break
            fetched += len(fills)
            max_seen_ts = page_start
            for fill in sorted(fills, key=lambda item: _extract_exchange_timestamp(item)):
                try:
                    event = normalize_twap_slice_fill_backfill(self.source_wallet, fill)
                except SourceWebsocketMessageError as exc:
                    detail = f"userTwapSliceFillsByTime returned malformed fill: {exc}"
                    if self.shield is not None:
                        self.shield.missed_event_gap(detail)
                    raise SourceWebsocketMessageError(detail) from exc
                max_seen_ts = max(max_seen_ts, event.exchange_ts_ms)
                if self.record_source_event(event):
                    inserted += 1
                else:
                    duplicates += 1
            if len(fills) < 2000:
                completed = True
                break
            if max_seen_ts <= page_start:
                warnings.append(
                    "userTwapSliceFillsByTime page was full but timestamps did not advance; "
                    "TWAP slice fill backfill may be incomplete"
                )
                break
            page_start = max_seen_ts + 1
        if not completed and page_start <= end and pages >= max_pages:
            warnings.append(
                "source TWAP slice fill backfill hit max page limit before reaching end time"
            )
        if warnings and self.shield is not None:
            self.shield.missed_event_gap("; ".join(warnings))
        return FillBackfillReport(
            start_time_ms=start_time_ms,
            end_time_ms=end,
            pages=pages,
            fetched=fetched,
            inserted=inserted,
            duplicates=duplicates,
            warnings=warnings,
        )

    async def observe_websocket(
        self,
        stop_after_messages: int | None = None,
        on_event: SourceEventCallback | None = None,
    ) -> None:
        if not self.ws_url:
            raise RuntimeError("ws_url is required for websocket observation")

        subscriptions = source_websocket_subscriptions(
            self.source_wallet,
            active_asset_symbols=self.active_asset_symbols,
        )
        subscription_keys = {_subscription_key(subscription) for subscription in subscriptions}
        seen = 0
        try:
            async with connect_websocket_ipv6_preferred(
                self.ws_url,
                ping_interval=None,
            ) as ws:
                for subscription in subscriptions:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))
                while stop_after_messages is None or seen < stop_after_messages:
                    try:
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=self.websocket_idle_timeout_ms / 1000,
                        )
                    except TimeoutError:
                        await ws.send(json.dumps({"method": "ping"}))
                        try:
                            raw = await asyncio.wait_for(
                                ws.recv(),
                                timeout=self.websocket_heartbeat_timeout_ms / 1000,
                            )
                        except TimeoutError as heartbeat_exc:
                            raise TimeoutError(
                                "source websocket heartbeat response not received within "
                                f"{self.websocket_heartbeat_timeout_ms}ms"
                            ) from heartbeat_exc
                    if raw == WEBSOCKET_CONNECTION_BANNER:
                        continue
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError as exc:
                        detail = f"source websocket sent malformed JSON: {exc}"
                        if self.shield is not None:
                            self.shield.missed_event_gap(detail)
                        raise SourceWebsocketMessageError(detail) from exc
                    if not isinstance(message, dict):
                        detail = "source websocket message is not an object"
                        if self.shield is not None:
                            self.shield.missed_event_gap(detail)
                        raise SourceWebsocketMessageError(detail)
                    channel = message.get("channel")
                    if channel == "subscriptionResponse":
                        subscription_error = _subscription_response_error(
                            message, subscription_keys
                        )
                        if subscription_error is not None:
                            if self.shield is not None:
                                self.shield.missed_event_gap(subscription_error)
                            raise SourceWebsocketMessageError(subscription_error)
                        continue
                    if channel == "pong":
                        continue
                    event, inserted = self.record_ws_message_event(message)
                    if on_event is not None:
                        result = on_event(event, inserted)
                        if inspect.isawaitable(result):
                            await result
                    seen += 1
        except SourceWebsocketMessageError:
            raise
        except Exception as exc:
            if self.shield is not None:
                self.shield.websocket_disconnect(f"source websocket disconnected: {exc}")
            raise


def source_websocket_subscriptions(
    source_wallet: str,
    *,
    active_asset_symbols: Iterable[str] = (),
) -> list[dict[str, Any]]:
    source_wallet = source_wallet.lower()
    subscriptions: list[dict[str, Any]] = [
        {"type": "orderUpdates", "user": source_wallet},
        {"type": "userEvents", "user": source_wallet},
        {"type": "userFills", "user": source_wallet, "aggregateByTime": False},
        {"type": "userFundings", "user": source_wallet},
        {"type": "userNonFundingLedgerUpdates", "user": source_wallet},
        {"type": "userTwapSliceFills", "user": source_wallet},
        {"type": "userTwapHistory", "user": source_wallet},
        {"type": "twapStates", "user": source_wallet},
        {"type": "notification", "user": source_wallet},
        {"type": "webData3", "user": source_wallet},
        {"type": "spotState", "user": source_wallet},
        {"type": "allDexsClearinghouseState", "user": source_wallet},
        {"type": "openOrders", "user": source_wallet},
        {"type": "clearinghouseState", "user": source_wallet},
    ]
    normalized_assets = _normalized_symbols(active_asset_symbols)
    # activeAssetData is one noisy subscription per market. A frozen full-universe run would
    # otherwise create hundreds of user-specific streams and can exhaust the IP-wide 1,000
    # subscription / 2,000-message-per-minute envelope. Global orderUpdates, fills,
    # allDexsClearinghouseState, and webData3 remain subscribed and are sufficient to trigger an
    # authoritative REST reconcile for every position/order lifecycle. Keep the per-market feed
    # only for small interactive allowlists where it adds leverage-change responsiveness.
    if len(normalized_assets) <= SOURCE_ACTIVE_ASSET_SUBSCRIPTION_LIMIT:
        subscriptions.extend(
            {"type": "activeAssetData", "user": source_wallet, "coin": coin}
            for coin in normalized_assets
        )
    return subscriptions


def _is_recovered_backfill_event(event: SourceEvent) -> bool:
    return str(event.payload.get("channel") or "") in {
        "userFillsByTime",
        "userTwapSliceFillsByTime",
    }


def _subscription_response_error(
    message: dict[str, Any],
    expected_subscription_keys: set[str],
) -> str | None:
    data = message.get("data")
    subscription = data.get("subscription") if isinstance(data, dict) else None
    if not isinstance(subscription, dict) and isinstance(data, dict):
        subscription = data
    if not isinstance(subscription, dict):
        return "source websocket subscription response is missing subscription data"
    key = _subscription_key(subscription)
    if key not in expected_subscription_keys:
        return f"source websocket subscription response for unexpected subscription: {subscription}"
    return None


def _subscription_key(subscription: dict[str, Any]) -> str:
    return json.dumps(
        _normalized_subscription(subscription),
        sort_keys=True,
        separators=(",", ":"),
    )


def _normalized_subscription(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {
            str(item_key): _normalized_subscription(item, str(item_key))
            for item_key, item in value.items()
            if not (str(item_key).lower() == "dex" and item in {"", None})
            and not (str(item_key).lower() == "ignoreportfoliomargin" and item in {False, None})
        }
    if isinstance(value, list):
        return [_normalized_subscription(item, key) for item in value]
    if isinstance(value, str) and key.lower() in USER_ADDRESS_KEYS | {
        "useraddress",
        "accountaddress",
    }:
        return value.lower()
    if isinstance(value, str) and key.lower() == "coin":
        return canonical_market_symbol(value)
    return value


def _normalized_symbols(symbols: Iterable[str]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for symbol in symbols:
        raw_symbol = str(symbol).strip()
        if raw_symbol:
            normalized.add(canonical_market_symbol(raw_symbol))
    return tuple(sorted(normalized))
