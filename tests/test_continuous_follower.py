from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import pytest

from hyperliquid_copytrader.continuous_config import (
    BoundContinuousSlot,
    ContinuousSlotConfig,
)
from hyperliquid_copytrader.continuous_follower import FollowerTruthError, WsFollowerInfo
from hyperliquid_copytrader.market_catalog import (
    CatalogMarket,
    CatalogRevision,
    MarketReadiness,
)
from hyperliquid_copytrader.ws_actions import PostOutcome, PostResult


FOLLOWER = "0x" + "2" * 40


def _catalog(dex_count: int = 2) -> CatalogRevision:
    dexes = ("",) + tuple(f"dex{index}" for index in range(1, dex_count))
    markets = tuple(
        CatalogMarket(
            symbol="BTC" if not dex else f"{dex}:COIN",
            dex=dex,
            asset_id=index if not dex else 100_000 + index * 10_000,
            dex_index=index,
            universe_index=0,
            sz_decimals=3,
            max_leverage=10,
            readiness=MarketReadiness.READY,
        )
        for index, dex in enumerate(dexes)
    )
    return CatalogRevision(
        sequence=1,
        revision_id="catalog-test",
        policy_version="test",
        network="mainnet",
        observed_ms=1,
        wire_dexes=dexes,
        markets=markets,
        snapshot_sha256="a" * 64,
        dex_bracket_before_sha256="b" * 64,
        dex_bracket_after_sha256="b" * 64,
    )


def _slot(tmp_path: Path) -> BoundContinuousSlot:
    config = ContinuousSlotConfig(
        slot="slot1",
        source_address="0x" + "1" * 40,
        follower_account_address=FOLLOWER,
        credential_profile_id="slot1",
        multiplier=Decimal("1"),
        max_order_notional_usd=Decimal("10"),
        max_gross_exposure_usd=Decimal("20"),
        max_open_positions=2,
        max_leverage=2,
        action_limit_per_minute=6,
        allowed_markets=("BTC", "dex1:COIN"),
        enabled=True,
    )
    return BoundContinuousSlot(
        config,
        "0x" + "3" * 40,
        tmp_path / "key",
        "0x" + "4" * 40,
        "unified",
    )


class _Mux:
    def __init__(self, *, resting_dex: str | None = None, delay: bool = False) -> None:
        self.resting_dex = resting_dex
        self.delay = delay
        self.calls: list[dict[str, Any]] = []
        self.active = 0
        self.peak = 0

    async def post_info(
        self,
        payload: Mapping[str, Any],
        *,
        required_epoch: int,
        timeout_s: float | None = None,
    ) -> PostResult:
        assert required_epoch == 7
        self.calls.append(dict(payload))
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            if self.delay:
                await asyncio.sleep(0.001)
            request_type = str(payload["type"])
            dex = str(payload.get("dex") or "")
            if request_type == "userAbstraction":
                data: Any = "unifiedAccount"
            elif request_type == "spotClearinghouseState":
                data = {"balances": [{"coin": "USDC", "token": 0, "total": "100", "hold": "1"}]}
            elif request_type == "clearinghouseState":
                data = {
                    "assetPositions": (
                        [{"position": {"coin": "COIN", "szi": "2", "entryPx": "10"}}]
                        if dex == "dex1"
                        else []
                    )
                }
            elif request_type == "openOrders":
                data = [{"coin": "COIN", "oid": 1}] if dex == self.resting_dex else []
            else:
                raise AssertionError(request_type)
            return PostResult(
                len(self.calls),
                PostOutcome.INFO,
                {"type": "info", "payload": {"type": request_type, "data": data}},
                "info_response",
            )
        finally:
            self.active -= 1


@pytest.mark.asyncio
async def test_ws_follower_truth_covers_only_allowed_dexes_and_unified_collateral(
    tmp_path: Path,
) -> None:
    mux = _Mux()
    hook = WsFollowerInfo(catalog=_catalog(), maximum_inflight=4, per_slot_workers=2)
    truth = await hook(
        slot=_slot(tmp_path),
        mux=mux,  # type: ignore[arg-type]
        epoch=7,
        now_ms=1_000,
    )
    assert truth.equity == Decimal("100")
    assert truth.positions["dex1:COIN"].size == Decimal("2")
    assert hook.requests_per_refresh(_slot(tmp_path)) == 3
    assert hook.weight_per_refresh(_slot(tmp_path)) == 6
    assert {(call["type"], str(call.get("dex") or "")) for call in mux.calls} == {
        ("spotClearinghouseState", ""),
        ("clearinghouseState", ""),
        ("clearinghouseState", "dex1"),
    }
    assert not any(call["type"] == "openOrders" for call in mux.calls)


@pytest.mark.asyncio
async def test_external_writer_mode_audits_and_blocks_resting_orders(tmp_path: Path) -> None:
    hook = WsFollowerInfo(catalog=_catalog())
    slot = replace(_slot(tmp_path), external_writers_allowed=True)
    with pytest.raises(FollowerTruthError, match="resting order"):
        await hook(
            slot=slot,
            mux=_Mux(resting_dex="dex1"),  # type: ignore[arg-type]
            epoch=7,
            now_ms=1_000,
        )


@pytest.mark.asyncio
async def test_first_dex_use_performs_one_local_open_order_audit(tmp_path: Path) -> None:
    hook = WsFollowerInfo(catalog=_catalog())
    mux = _Mux(resting_dex="dex1")
    with pytest.raises(FollowerTruthError, match="resting order"):
        await hook.refresh_dex(
            slot=_slot(tmp_path),
            dex="dex1",
            mux=mux,  # type: ignore[arg-type]
            epoch=7,
            now_ms=1_000,
            audit_open_orders=True,
        )
    assert [call["type"] for call in mux.calls] == ["clearinghouseState", "openOrders"]


def test_ten_followers_across_five_dexes_fit_ordinary_budget(tmp_path: Path) -> None:
    catalog = _catalog(dex_count=5)
    hook = WsFollowerInfo(catalog=catalog)
    markets = tuple(market.symbol for market in catalog.markets)
    slot = _slot(tmp_path)
    slot = replace(slot, config=replace(slot.config, allowed_markets=markets))
    assert hook.requests_per_refresh(slot) * 10 == 60
    assert hook.weight_per_refresh(slot) * 10 == 120


@pytest.mark.asyncio
async def test_ten_slot_scans_share_a_strict_low_priority_inflight_bound(tmp_path: Path) -> None:
    mux = _Mux(delay=True)
    hook = WsFollowerInfo(
        catalog=_catalog(dex_count=11),
        maximum_inflight=12,
        per_slot_workers=2,
    )
    slot = _slot(tmp_path)
    await asyncio.gather(
        *(
            hook(
                slot=slot,
                mux=mux,  # type: ignore[arg-type]
                epoch=7,
                now_ms=1_000,
            )
            for _ in range(10)
        )
    )
    assert mux.peak <= 12
    assert hook.peak_inflight <= 12
    assert mux.peak > 1
