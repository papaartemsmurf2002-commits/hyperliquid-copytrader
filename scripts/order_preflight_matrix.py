from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from enum import Enum
from typing import Any

from hyperliquid_copytrader.order_preflight import (
    HyperliquidPerpRules,
    preflight_hyperliquid_perp_order,
)


def _wire(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _wire(item) for key, item in value.items()}
    return value


def main() -> None:
    cases = (
        (
            "native-risk-below-min",
            HyperliquidPerpRules("BTC", 5, 50),
            "0.00007",
            "100000",
            False,
            "0",
        ),
        (
            "native-rounded-placeable",
            HyperliquidPerpRules("BTC", 5, 50),
            "0.000109",
            "100000",
            False,
            "0",
        ),
        (
            "hip3-risk-below-min",
            HyperliquidPerpRules("xyz:XYZ100", 3, 10),
            "0.09",
            "100",
            False,
            "0",
        ),
        (
            "hip3-exact-close",
            HyperliquidPerpRules("xyz:XYZ100", 3, 10),
            "0.007",
            "100",
            True,
            "-0.007",
        ),
    )
    rows = []
    for case, rules, quantity, price, reduce_only, current in cases:
        result = preflight_hyperliquid_perp_order(
            rules=rules,
            requested_quantity=Decimal(quantity),
            price=Decimal(price),
            side=("buy" if not reduce_only or Decimal(current) < 0 else "sell"),
            max_order_notional_usd=Decimal("2500"),
            reduce_only=reduce_only,
            current_position_size=Decimal(current),
            leverage=10,
            available_collateral_usd=Decimal("50"),
        )
        rows.append(
            {
                "case": case,
                **_wire(asdict(result)),
                "validation_method": "synthetic_metadata_only",
            }
        )
    print(json.dumps({"exchange": "Hyperliquid", "rows": rows}, indent=2))


if __name__ == "__main__":
    main()
