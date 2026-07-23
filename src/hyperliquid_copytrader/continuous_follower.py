from __future__ import annotations

import asyncio
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from .continuous_config import BoundContinuousSlot
from .continuous_runtime import FollowerTruth
from .market_catalog import CatalogRevision, MarketReadiness
from .markets import market_dex
from .models import Position
from .observer import parse_clearinghouse_positions
from .rest_budget import authoritative_rest_weight
from .ws_actions import PostOutcome, WsPostMux


class FollowerTruthError(RuntimeError):
    """Follower exchange truth is incomplete, malformed, or unsafe to use."""


class WsFollowerInfo:
    """Follower equity and per-DEX positions using request-correlated WS info posts.

    The catalog DEX scan is intentionally low priority and bounded separately
    from action posts.  Each slot contributes only a small number of workers to
    the shared semaphore, preventing one large DEX scan from monopolizing the
    100-post venue inflight allowance. Startup preflight owns the expensive
    account-mode and broad open-order proof. The minute loop is deliberately
    limited to equity and positions; durable unknown CLOIDs are resolved by the
    signer lane's targeted ``orderStatus`` request.
    """

    def __init__(
        self,
        *,
        catalog: CatalogRevision,
        maximum_inflight: int = 12,
        per_slot_workers: int = 2,
        request_timeout_s: float = 5.0,
    ) -> None:
        if maximum_inflight < 1 or per_slot_workers < 1 or request_timeout_s <= 0:
            raise ValueError("follower WS bounds must be positive")
        if per_slot_workers > maximum_inflight:
            raise ValueError("per-slot follower workers exceed the shared inflight bound")
        self.catalog = catalog
        self.catalog_dexes = catalog_position_dexes(catalog)
        if not self.catalog_dexes:
            raise ValueError("follower truth requires at least one catalog DEX")
        self.maximum_inflight = maximum_inflight
        self.per_slot_workers = per_slot_workers
        self.request_timeout_s = request_timeout_s
        self._inflight = asyncio.Semaphore(maximum_inflight)
        self._active_requests = 0
        self.peak_inflight = 0

    def dexes_for(self, slot: BoundContinuousSlot) -> tuple[str, ...]:
        """Audit only DEXes this exclusive follower lane is allowed to trade."""

        dexes = tuple(
            sorted(
                {market_dex(market) for market in slot.config.allowed_markets},
                key=lambda item: (item != "", item),
            )
        )
        if not dexes or any(dex not in self.catalog_dexes for dex in dexes):
            raise FollowerTruthError("follower plan contains an unavailable DEX")
        return dexes

    def replace_catalog(self, catalog: CatalogRevision) -> None:
        dexes = catalog_position_dexes(catalog)
        if not dexes:
            raise ValueError("follower truth requires at least one catalog DEX")
        self.catalog, self.catalog_dexes = catalog, dexes

    def requests_per_refresh(self, slot: BoundContinuousSlot) -> int:
        return (
            1
            + len(self.dexes_for(slot))
            + (len(self.dexes_for(slot)) if slot.external_writers_allowed else 0)
        )

    def weight_per_refresh(self, slot: BoundContinuousSlot) -> int:
        dexes = len(self.dexes_for(slot))
        return authoritative_rest_weight("info:spotClearinghouseState") + dexes * (
            authoritative_rest_weight("info:clearinghouseState")
            + (authoritative_rest_weight("info:openOrders") if slot.external_writers_allowed else 0)
        )

    async def __call__(
        self,
        *,
        slot: BoundContinuousSlot,
        mux: WsPostMux,
        epoch: int,
        now_ms: int,
    ) -> FollowerTruth:
        follower = slot.config.follower_account_address
        spot = await self._info(
            mux,
            epoch,
            {"type": "spotClearinghouseState", "user": follower},
        )
        # Token-0 Spot USDC is the Unified-account sizing equity basis.  The
        # unheld amount is useful operator evidence, but is not authoritative
        # free perp collateral because DEX margin consumption is separate.
        equity, spot_spendable = _token_zero_usdc_balance(spot)

        dexes = self.dexes_for(slot)
        queue: asyncio.Queue[str] = asyncio.Queue()
        for dex in dexes:
            queue.put_nowait(dex)
        positions: dict[str, Position] = {}
        positions_lock = asyncio.Lock()

        async def worker() -> None:
            while True:
                try:
                    dex = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    suffix = {} if not dex else {"dex": dex}
                    state = await self._info(
                        mux, epoch, {"type": "clearinghouseState", "user": follower, **suffix}
                    )
                    if not isinstance(state, Mapping):
                        raise FollowerTruthError(
                            f"follower {dex or 'native'} clearinghouse state is malformed"
                        )
                    parsed = parse_clearinghouse_positions(dict(state), observed_ms=now_ms, dex=dex)
                    if slot.external_writers_allowed:
                        orders = await self._info(
                            mux, epoch, {"type": "openOrders", "user": follower, **suffix}
                        )
                        if not isinstance(orders, list) or not all(
                            isinstance(order, Mapping) for order in orders
                        ):
                            raise FollowerTruthError(
                                f"follower {dex or 'native'} open-order truth is malformed"
                            )
                        if orders:
                            raise FollowerTruthError(
                                f"follower {dex or 'native'} has {len(orders)} resting order(s)"
                            )
                    async with positions_lock:
                        for market, position in parsed.items():
                            if market in positions:
                                raise FollowerTruthError(
                                    f"duplicate follower position identity {market}"
                                )
                            catalog_market = self.catalog.market(market)
                            if catalog_market is None:
                                raise FollowerTruthError(
                                    f"follower position {market} is absent from the pinned catalog"
                                )
                            if catalog_market.readiness is MarketReadiness.UNTRUSTED:
                                raise FollowerTruthError(
                                    f"follower position {market} has an untrusted identity"
                                )
                            positions[market] = position
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(min(self.per_slot_workers, len(dexes)))))
        return FollowerTruth(
            dict(sorted(positions.items())),
            equity,
            now_ms,
            {dex: now_ms for dex in dexes},
            spot_spendable,
        )

    async def refresh_dex(
        self,
        *,
        slot: BoundContinuousSlot,
        dex: str,
        mux: WsPostMux,
        epoch: int,
        now_ms: int,
        audit_open_orders: bool = False,
    ) -> Mapping[str, Position]:
        """Refresh one DEX on first use without auditing unrelated followers or DEXes."""

        if dex not in self.catalog_dexes:
            raise FollowerTruthError(f"follower DEX {dex or 'native'} is unavailable")
        suffix = {} if not dex else {"dex": dex}
        state = await self._info(
            mux,
            epoch,
            {"type": "clearinghouseState", "user": slot.config.follower_account_address, **suffix},
        )
        if not isinstance(state, Mapping):
            raise FollowerTruthError(f"follower {dex or 'native'} clearinghouse state is malformed")
        parsed = parse_clearinghouse_positions(dict(state), observed_ms=now_ms, dex=dex)
        if audit_open_orders or slot.external_writers_allowed:
            orders = await self._info(
                mux,
                epoch,
                {"type": "openOrders", "user": slot.config.follower_account_address, **suffix},
            )
            if not isinstance(orders, list) or not all(
                isinstance(order, Mapping) for order in orders
            ):
                raise FollowerTruthError(
                    f"follower {dex or 'native'} open-order truth is malformed"
                )
            if orders:
                raise FollowerTruthError(
                    f"follower {dex or 'native'} has {len(orders)} resting order(s)"
                )
        for market in parsed:
            spec = self.catalog.market(market)
            if spec is None or spec.readiness is MarketReadiness.UNTRUSTED:
                raise FollowerTruthError(f"follower position {market} has no trusted identity")
        return parsed

    async def _info(
        self,
        mux: WsPostMux,
        epoch: int,
        payload: Mapping[str, Any],
    ) -> Any:
        expected = str(payload.get("type") or "")
        async with self._inflight:
            self._active_requests += 1
            self.peak_inflight = max(self.peak_inflight, self._active_requests)
            try:
                result = await mux.post_info(
                    payload,
                    required_epoch=epoch,
                    timeout_s=self.request_timeout_s,
                )
            finally:
                self._active_requests -= 1
        if result.outcome is not PostOutcome.INFO:
            raise FollowerTruthError(
                f"follower WS info {expected} was {result.outcome.value}: {result.reason}"
            )
        response = result.response
        if not isinstance(response, Mapping) or response.get("type") != "info":
            raise FollowerTruthError(f"follower WS info {expected} envelope is malformed")
        inner = response.get("payload")
        if not isinstance(inner, Mapping) or inner.get("type") != expected or "data" not in inner:
            raise FollowerTruthError(f"follower WS info {expected} payload is mismatched")
        return inner["data"]


def _token_zero_usdc_balance(payload: Any) -> tuple[Decimal, Decimal]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("balances"), list):
        raise FollowerTruthError("follower Spot collateral is malformed")
    matches = [
        row
        for row in payload["balances"]
        if isinstance(row, Mapping)
        and str(row.get("token")) == "0"
        and str(row.get("coin") or "").upper() == "USDC"
    ]
    if len(matches) != 1:
        raise FollowerTruthError("follower requires exactly one token-0 Spot USDC balance")
    try:
        total = Decimal(str(matches[0].get("total")))
        hold = Decimal(str(matches[0].get("hold")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FollowerTruthError("follower token-0 Spot USDC balance is malformed") from exc
    if not total.is_finite() or not hold.is_finite() or total <= 0 or not 0 <= hold <= total:
        raise FollowerTruthError("follower token-0 Spot USDC balance is inconsistent")
    return total, total - hold


def catalog_position_dexes(catalog: CatalogRevision) -> tuple[str, ...]:
    """Return every wire DEX that owns at least one retained market identity."""

    dexes_with_identity = {market.dex for market in catalog.markets if not market.removal_tombstone}
    return tuple(dex for dex in catalog.wire_dexes if dex in dexes_with_identity)


__all__ = ["FollowerTruthError", "WsFollowerInfo", "catalog_position_dexes"]
