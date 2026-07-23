from __future__ import annotations

from typing import Any


class FakeInfoClient:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []
        self.state: dict[str, Any] = {
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1.0",
                        "entryPx": "50000",
                        "leverage": {"type": "cross", "value": 2},
                    }
                }
            ],
            "marginSummary": {"accountValue": "1000"},
        }
        self.open_orders: list[dict[str, Any]] = []
        self.user_abstraction: Any = "disabled"
        self.user_dex_abstraction: Any = False
        self.spot_state: dict[str, Any] = {"balances": []}
        self.fills: list[dict[str, Any]] = []
        self.twap_slice_fills: list[dict[str, Any]] = []
        self.mids = {"BTC": "50000", "ETH": "3000", "SOL": "150"}
        self.meta = {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 50},
                {"name": "SOL", "szDecimals": 2, "maxLeverage": 20},
            ]
        }
        self.dex_states: dict[str, dict[str, Any]] = {}
        self.dex_open_orders: dict[str, list[dict[str, Any]]] = {}
        self.dex_mids: dict[str, dict[str, str]] = {}
        self.dex_meta: dict[str, dict[str, Any]] = {}
        self.dex_meta_and_contexts: dict[str, list[Any]] = {}
        self.books: dict[str, dict[str, Any]] = {}

    def info(self, payload: dict[str, Any]) -> Any:
        self.calls.append(payload)
        request_type = payload["type"]
        dex = str(payload.get("dex") or "")
        if request_type == "clearinghouseState":
            return self.dex_states.get(dex, self.state)
        if request_type == "openOrders":
            return self.dex_open_orders.get(dex, self.open_orders)
        if request_type == "userAbstraction":
            return self.user_abstraction
        if request_type == "userDexAbstraction":
            return self.user_dex_abstraction
        if request_type == "spotClearinghouseState":
            return self.spot_state
        if request_type == "allMids":
            return self.dex_mids.get(dex, self.mids)
        if request_type == "meta":
            return self.dex_meta.get(dex, self.meta)
        if request_type == "metaAndAssetCtxs":
            return self.dex_meta_and_contexts.get(
                dex,
                [
                    self.dex_meta.get(dex, self.meta),
                    [
                        {"oraclePx": self.dex_mids.get(dex, self.mids).get(item["name"])}
                        for item in self.dex_meta.get(dex, self.meta)["universe"]
                    ],
                ],
            )
        if request_type == "l2Book":
            coin = str(payload.get("coin") or "")
            if coin in self.books:
                return self.books[coin]
            raise AssertionError(f"missing fake l2Book for {coin}")
        if request_type == "userFillsByTime":
            start = int(payload.get("startTime") or 0)
            end = int(payload.get("endTime") or 2**63 - 1)
            return [fill for fill in self.fills if start <= int(fill.get("time") or 0) <= end]
        if request_type == "userTwapSliceFillsByTime":
            start = int(payload.get("startTime") or 0)
            end = int(payload.get("endTime") or 2**63 - 1)
            return [
                fill
                for fill in self.twap_slice_fills
                if start
                <= int((fill.get("fill") or {}).get("time") or fill.get("time") or 0)
                <= end
            ]
        if request_type == "userRateLimit":
            return {"nRequestsUsed": 0}
        raise AssertionError(f"unexpected info request {payload}")


def add_eth_position(fake: FakeInfoClient, size: str = "1.0") -> FakeInfoClient:
    fake.state["assetPositions"].append(
        {
            "position": {
                "coin": "ETH",
                "szi": size,
                "entryPx": "3000",
                "leverage": {"type": "cross", "value": 2},
            }
        }
    )
    return fake
