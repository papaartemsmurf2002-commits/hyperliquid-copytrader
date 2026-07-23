from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

import hyperliquid_copytrader.exchange.hyperliquid as exchange_module
from hyperliquid_copytrader.config import AccountMode, AppConfig, ExchangeConfig, RiskConfig
from hyperliquid_copytrader.copy_engine import AssetMeta, CopyEngine
from hyperliquid_copytrader.exchange.hyperliquid import HyperliquidExecutionAdapter
from hyperliquid_copytrader.models import IntentStatus, Mode, now_ms
from hyperliquid_copytrader.observer import SourceObserver
from hyperliquid_copytrader.persistence import SQLiteStore
from hyperliquid_copytrader.unified_account import SourceDexScope, UnifiedAccountSnapshot


SOURCE = "0xcf7c4feb434751146a48b895e96caeb15838f92c"
FOLLOWER = "0xf000000000000000000000000000000000000000"


def _dex_state(coin: str | None = None, size: str = "0") -> dict[str, Any]:
    timestamp = now_ms()
    summary = {
        "accountValue": "0",
        "totalMarginUsed": "0",
        "totalNtlPos": "0",
        "totalRawUsd": "0",
    }
    positions = []
    if coin is not None:
        positions.append(
            {
                "type": "oneWay",
                "position": {
                    "coin": coin,
                    "szi": size,
                    "entryPx": "190",
                    "leverage": {"type": "cross", "value": 2},
                },
            }
        )
    return {
        "assetPositions": positions,
        "crossMaintenanceMarginUsed": "0",
        "crossMarginSummary": summary,
        "marginSummary": summary,
        "time": timestamp,
        "withdrawable": "0",
    }


def _aggregate(account: str, *, dex: str = "xyz") -> UnifiedAccountSnapshot:
    observed = now_ms()
    return UnifiedAccountSnapshot(
        account=account,
        clearinghouse_states={"": _dex_state(), dex: _dex_state("AAPL", "2")},
        observed_ms=observed,
        received_ms=observed,
    )


class SourceInfo:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []

    def info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(dict(payload))
        request_type = payload["type"]
        dex = payload.get("dex", "")
        if request_type == "clearinghouseState":
            return _dex_state()
        if request_type == "openOrders":
            if dex == "xyz":
                return [
                    {
                        "coin": "AAPL",
                        "side": "B",
                        "sz": "0.25",
                        "limitPx": "195",
                        "oid": 44,
                    }
                ]
            return []
        if request_type == "userAbstraction":
            return "unifiedAccount"
        if request_type == "userDexAbstraction":
            return False
        if request_type == "spotClearinghouseState":
            return {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "10"}]}
        if request_type == "allMids":
            return {"AAPL": "200"} if dex == "xyz" else {"BTC": "50000"}
        raise AssertionError(f"unexpected source info request: {payload}")


class FollowerInfo:
    def user_state(self, _account: str) -> dict[str, Any]:
        return _dex_state()

    def open_orders(self, _account: str, _dex: str = "") -> list[dict[str, Any]]:
        return []

    def historical_orders(self, _account: str) -> list[dict[str, Any]]:
        return []

    def query_user_abstraction_state(self, _account: str) -> str:
        return "unifiedAccount"

    def query_user_dex_abstraction_state(self, _account: str) -> bool:
        return False

    def spot_user_state(self, _account: str) -> dict[str, Any]:
        return {"balances": [{"coin": "USDC", "token": 0, "total": "50", "hold": "0"}]}


class FakeCloid:
    @classmethod
    def from_str(cls, value: str) -> str:
        return value


class RecordingExchange:
    def __init__(self):
        self.expires_after: int | None = None
        self.order_markets: list[str] = []

    def order(self, market: str, **_kwargs: Any) -> dict[str, Any]:
        self.order_markets.append(market)
        return {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 7}}]}},
        }


def test_hip3_unified_source_to_safe_follower_action_contract(tmp_path, monkeypatch):
    store = SQLiteStore(tmp_path / "hip3-integration.sqlite3")
    try:
        source_info = SourceInfo()
        observer = SourceObserver(
            source_wallet=SOURCE,
            info_client=source_info,
            store=store,
            active_asset_symbols=("xyz:aapl",),
            source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
            unified_state_provider=lambda: _aggregate(SOURCE),
        )

        source = observer.reconcile_once()

        assert set(source.positions) == {"xyz:AAPL"}
        assert source.open_orders[0].coin == "xyz:AAPL"
        assert source.mids == {"xyz:AAPL": Decimal("200")}
        assert source.raw_state["accountValue"] == Decimal("1000")
        assert source.raw_state["unifiedAggregate"]["account_value_basis"] == (
            "total_unified_spot_usdc"
        )
        assert source.raw_state["unifiedAggregate"]["market_data_dexes"] == ["", "xyz"]
        assert source.raw_state["openOrders"]["xyz"][0]["coin"] == "AAPL"
        assert source.raw_state["allMids"]["xyz"] == {"AAPL": "200"}

        risk = RiskConfig(
            allowed_symbols=("xyz:aapl",),
            fixed_multiplier=Decimal("1"),
            sizing_equity_cap_usd=Decimal("50"),
            max_notional_usd=Decimal("250"),
            max_gross_exposure_usd=Decimal("250"),
            max_leverage=3,
            min_order_size=Decimal("0.001"),
            slippage_bps=Decimal("25"),
        )
        result = CopyEngine(risk, Mode.TESTNET, follower_account=FOLLOWER).plan(
            source_event_key=source.planning_key,
            source_positions=source.positions,
            follower_positions={},
            asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3, max_leverage=5)},
            mids=source.mids,
            source_account_value=source.raw_state["accountValue"],
            follower_account_value=Decimal("50"),
        )

        assert result.blockers == []
        assert result.sizing["source_account_value"] == Decimal("1000")
        assert result.sizing["sizing_equity_usd"] == Decimal("50")
        assert result.desired_state.positions["xyz:AAPL"].size == Decimal("0.100")
        assert len(result.intents) == 1
        intent = result.intents[0]
        assert intent.coin == "xyz:AAPL"
        assert intent.coin.split(":", 1)[0] == "xyz"

        config = AppConfig(
            mode=Mode.TESTNET,
            source_wallet=SOURCE,
            source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
            risk=risk,
            exchange=ExchangeConfig(
                follower_account_address=FOLLOWER,
                api_private_key="0x" + "1" * 64,
                expected_account_mode=AccountMode.UNIFIED,
                testnet_enable=True,
            ),
        )
        exchange = RecordingExchange()
        adapter = HyperliquidExecutionAdapter(
            config,
            pre_send_check=lambda _action, _risk_increasing: intent,
        )
        adapter._exchange = exchange
        monkeypatch.setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
        monkeypatch.setattr(exchange_module, "apply_rest_throttle", lambda *_args, **_kwargs: None)

        report = adapter.place_intent(intent)

        assert report.status == IntentStatus.ACKED
        assert exchange.order_markets == ["xyz:AAPL"]
        assert adapter._configured_perp_dexs() == ["", "xyz"]

        unknown = HyperliquidExecutionAdapter(
            config,
            unified_state_provider=lambda: _aggregate(FOLLOWER, dex="other"),
        )
        unknown._info = FollowerInfo()
        with pytest.raises(RuntimeError, match="active unconfigured DEXes: other"):
            unknown.reconcile()
    finally:
        store.close()
