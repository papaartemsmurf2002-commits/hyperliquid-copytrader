from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from hyperliquid_copytrader.markets import canonical_market_symbol
from hyperliquid_copytrader.observer import parse_clearinghouse_positions, parse_open_orders


INFO_URL = "https://api.hyperliquid.xyz/info"
SNAPSHOT_NORMALIZER_VERSION = 2
USER_AGENT = "hl-copytrader-follower-snapshot-normalizer/0.1"


class SnapshotInputError(RuntimeError):
    """Raised when a follower snapshot cannot be normalized."""


def normalize_follower_snapshot(
    raw: dict[str, Any],
    *,
    slot: str,
    follower_subaccount: str,
    captured_ms: int | None = None,
    source_backfill_complete: bool = False,
    reconcile_complete: bool = False,
    notes: str = "",
    source: str = "local",
    info_touched: bool = False,
) -> dict[str, Any]:
    bundle = snapshot_bundle(raw)
    clearinghouse_result = request_result(bundle, "clearinghouseState")
    open_orders_result = request_result(bundle, "openOrders")
    warnings: list[str] = []

    clearinghouse_ok = result_ok(clearinghouse_result)
    open_orders_ok = result_ok(open_orders_result)
    clearinghouse_payload = result_payload(clearinghouse_result)
    open_orders_payload = result_payload(open_orders_result)
    if not isinstance(clearinghouse_payload, dict):
        clearinghouse_payload = {}
        clearinghouse_ok = False
        warnings.append("clearinghouseState payload is not an object")
    observed_ms = captured_ms or int(raw.get("received_ms") or raw.get("captured_ms") or now_ms())
    try:
        positions = normalize_positions(clearinghouse_payload, observed_ms=observed_ms)
    except (InvalidOperation, ValueError) as exc:
        positions = []
        clearinghouse_ok = False
        warnings.append(f"clearinghouseState validation failed: {exc}")
    try:
        orders = normalize_open_orders(open_orders_payload, observed_ms=observed_ms)
    except (InvalidOperation, ValueError) as exc:
        orders = []
        open_orders_ok = False
        warnings.append(f"openOrders validation failed: {exc}")
    account_value = account_value_usd(clearinghouse_payload)
    follower_refresh_complete = clearinghouse_ok and open_orders_ok
    if account_value is None:
        warnings.append("clearinghouseState did not include marginSummary.accountValue")
    if not follower_refresh_complete:
        warnings.append("follower refresh incomplete because one or more REST payloads failed")
    address_verification = follower_address_verification(
        raw,
        expected_follower_subaccount=follower_subaccount,
    )
    if not address_verification["verified"]:
        warnings.append(address_verification["reason"])

    return {
        "snapshot_normalizer_version": SNAPSHOT_NORMALIZER_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "info_touched": info_touched,
        "source": source,
        "slot": slot,
        "follower_subaccount": follower_subaccount.lower(),
        "follower_subaccount_verified": address_verification["verified"],
        "address_verification": address_verification,
        "captured_ms": observed_ms,
        "account_value_usd": decimal_str(account_value),
        "positions": positions,
        "open_orders": orders,
        "recovery": {
            "source_backfill_complete": source_backfill_complete,
            "follower_refresh_complete": follower_refresh_complete,
            "reconcile_complete": reconcile_complete,
            "notes": notes or "normalized read-only follower REST snapshot",
        },
        "counts": {
            "positions": len(positions),
            "open_orders": len(orders),
            "position_sides": counter_dict(Counter(item["side"] for item in positions)),
            "open_order_sides": counter_dict(Counter(item["side"] for item in orders)),
        },
        "request_status": {
            "clearinghouseState_ok": clearinghouse_ok,
            "openOrders_ok": open_orders_ok,
        },
        "warnings": warnings,
    }


def snapshot_bundle(raw: dict[str, Any]) -> dict[str, Any]:
    results = raw.get("results")
    if isinstance(results, dict):
        return results
    if "clearinghouseState" in raw or "openOrders" in raw:
        return raw
    return {"clearinghouseState": raw, "openOrders": []}


def request_result(bundle: dict[str, Any], key: str) -> Any:
    return bundle.get(key, {} if key == "clearinghouseState" else [])


def result_ok(value: Any) -> bool:
    if isinstance(value, dict) and "ok" in value:
        return value.get("ok") is True
    return True


def result_payload(value: Any) -> Any:
    if isinstance(value, dict) and "payload" in value:
        return value.get("payload")
    return value


def normalize_positions(
    clearinghouse: dict[str, Any], *, observed_ms: int | None = None
) -> list[dict[str, Any]]:
    parsed = parse_clearinghouse_positions(clearinghouse, observed_ms=observed_ms)
    raw_by_coin: dict[str, dict[str, Any]] = {}
    for item in clearinghouse["assetPositions"]:
        position = item.get("position", item)
        coin = canonical_market_symbol(position["coin"])
        for field in ("entryPx", "positionValue", "unrealizedPnl", "marginUsed"):
            strict_optional_decimal(position.get(field), field=f"{coin}.{field}")
        raw_by_coin[coin] = position
    rows: list[dict[str, Any]] = []
    for coin, position in sorted(parsed.items()):
        raw_position = raw_by_coin[coin]
        size = position.size
        entry_px = position.entry_px
        position_value = strict_optional_decimal(
            raw_position.get("positionValue"),
            field=f"{coin}.positionValue",
        )
        notional = abs(position_value) if position_value is not None else None
        if notional is None and entry_px is not None:
            notional = abs(size * entry_px)
        signed_notional = signed_from_size(notional or Decimal("0"), size)
        rows.append(
            {
                "coin": coin,
                "side": "long" if size > 0 else "short",
                "size": decimal_str(size),
                "entry_px": decimal_str(entry_px),
                "notional_usd": decimal_str(abs(signed_notional)),
                "signed_notional_usd": decimal_str(signed_notional),
                "leverage": position.leverage,
                "unrealized_pnl_usd": decimal_str(
                    strict_optional_decimal(
                        raw_position.get("unrealizedPnl"),
                        field=f"{coin}.unrealizedPnl",
                    )
                ),
                "margin_used_usd": decimal_str(
                    strict_optional_decimal(
                        raw_position.get("marginUsed"),
                        field=f"{coin}.marginUsed",
                    )
                ),
                "source": "clearinghouseState.assetPositions",
            }
        )
    return rows


def normalize_open_orders(
    open_orders: list[dict[str, Any]] | dict[str, Any],
    *,
    observed_ms: int | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for order in parse_open_orders(open_orders, observed_ms=observed_ms):
        notional = abs(order.size * order.price) if order.price is not None else Decimal("0")
        rows.append(
            {
                "coin": order.coin,
                "side": order.side,
                "size": decimal_str(order.size),
                "price": decimal_str(order.price),
                "notional_usd": decimal_str(notional),
                "signed_notional_usd": decimal_str(signed_amount(notional, order.side)),
                "oid": clean(order.oid),
                "cloid": clean(order.cloid),
                "reduce_only": order.reduce_only,
                "source": "openOrders",
            }
        )
    rows.sort(key=lambda item: (item["coin"], item["side"], item["oid"], item["cloid"]))
    return rows


def account_value_usd(clearinghouse: dict[str, Any]) -> Decimal | None:
    for key in ("marginSummary", "crossMarginSummary"):
        summary = clearinghouse.get(key)
        if isinstance(summary, dict):
            value = decimal_optional(summary.get("accountValue"))
            if value is not None:
                return value
    return None


def signed_amount(notional: Decimal, side: str) -> Decimal:
    if side in {"sell", "short", "a", "ask"}:
        return -abs(notional)
    return abs(notional)


def signed_from_size(notional: Decimal, size: Decimal) -> Decimal:
    return abs(notional) if size > 0 else -abs(notional)


def read_input_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SnapshotInputError(f"input file does not exist: {path}")
    if path.suffix.lower() == ".jsonl":
        last: dict[str, Any] | None = None
        for _line_no, row in iter_jsonl(path):
            last = row
        if last is None:
            raise SnapshotInputError(f"input JSONL has no rows: {path}")
        return last
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SnapshotInputError(f"{path} invalid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise SnapshotInputError(f"{path} input snapshot must be an object")
    return payload


def iter_jsonl(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError as exc:
                raise SnapshotInputError(f"{path}:{line_no} invalid JSON: {exc.msg}") from exc
            if not isinstance(row, dict):
                raise SnapshotInputError(f"{path}:{line_no} JSONL row must be an object")
            yield line_no, row


def fetch_info_snapshot(address: str, *, base_url: str, timeout_s: float) -> dict[str, Any]:
    if not valid_address(address):
        raise SnapshotInputError("fetch address must be a 42-character hex address")
    return {
        "address": address.lower(),
        "kind": "read_only_info_snapshot",
        "received_ms": now_ms(),
        "results": {
            "clearinghouseState": {
                "ok": True,
                "payload": post_info(
                    {"type": "clearinghouseState", "user": address.lower()},
                    base_url=base_url,
                    timeout_s=timeout_s,
                ),
            },
            "openOrders": {
                "ok": True,
                "payload": post_info(
                    {"type": "openOrders", "user": address.lower()},
                    base_url=base_url,
                    timeout_s=timeout_s,
                ),
            },
        },
    }


def follower_address_verification(
    raw: dict[str, Any],
    *,
    expected_follower_subaccount: str,
) -> dict[str, Any]:
    expected = expected_follower_subaccount.lower()
    observed = clean(raw.get("address")).lower()
    observed_valid = valid_address(observed)
    expected_valid = valid_address(expected)
    verified = observed_valid and expected_valid and observed == expected
    if verified:
        reason = "raw snapshot address matches follower_subaccount"
    elif not observed_valid:
        reason = "raw snapshot does not include a valid observed address"
    elif not expected_valid:
        reason = "follower_subaccount is not a valid address"
    else:
        reason = "raw snapshot address does not match follower_subaccount"
    return {
        "verified": verified,
        "expected_follower_subaccount": expected,
        "observed_address": observed if observed_valid else None,
        "observed_address_source": "raw.address" if observed_valid else None,
        "reason": reason,
    }


def post_info(payload: dict[str, Any], *, base_url: str, timeout_s: float) -> Any:
    request = Request(
        base_url.rstrip("/") + "/info",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_s) as response:  # nosec B310
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise SnapshotInputError(f"read-only info request failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise SnapshotInputError(f"read-only info request failed: {exc.reason}") from exc


def valid_address(value: str) -> bool:
    text = value.lower()
    return (
        len(text) == 42
        and text.startswith("0x")
        and all(char in "0123456789abcdef" for char in text[2:])
    )


def now_ms() -> int:
    return int(time.time() * 1000)


def decimal_optional(value: Any) -> Decimal | None:
    if value in (None, "", "unknown"):
        return None
    try:
        parsed = Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def strict_optional_decimal(value: Any, *, field: str) -> Decimal | None:
    if value in (None, "", "unknown"):
        return None
    parsed = decimal_optional(value)
    if parsed is None:
        raise ValueError(f"{field} must be a finite decimal")
    return parsed


def decimal_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.quantize(Decimal("0.00000001")), "f")


def clean(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize read-only Hyperliquid follower REST snapshots into the local drift-scorer "
            "snapshot schema."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="Local JSON or JSONL snapshot/bundle.")
    source.add_argument("--fetch-address", help="Read-only /info address to fetch and normalize.")
    parser.add_argument(
        "--out", type=Path, required=True, help="Write normalized follower snapshot."
    )
    parser.add_argument("--slot", required=True)
    parser.add_argument("--follower-subaccount", required=True)
    parser.add_argument("--source-backfill-complete", action="store_true")
    parser.add_argument("--reconcile-complete", action="store_true")
    parser.add_argument("--notes", default="")
    parser.add_argument("--base-url", default="https://api.hyperliquid.xyz")
    parser.add_argument("--timeout-s", type=float, default=15.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.fetch_address:
            if not valid_address(args.follower_subaccount):
                raise SnapshotInputError(
                    "--follower-subaccount must be a 42-character hex address in fetch mode"
                )
            if args.fetch_address.lower() != args.follower_subaccount.lower():
                raise SnapshotInputError(
                    "--fetch-address must match --follower-subaccount in fetch mode"
                )
            raw = fetch_info_snapshot(
                args.fetch_address, base_url=args.base_url, timeout_s=args.timeout_s
            )
            source = f"info:{args.fetch_address.lower()}"
            info_touched = True
        else:
            raw = read_input_snapshot(args.input)
            source = str(args.input)
            info_touched = False
        normalized = normalize_follower_snapshot(
            raw,
            slot=args.slot,
            follower_subaccount=args.follower_subaccount,
            source_backfill_complete=args.source_backfill_complete,
            reconcile_complete=args.reconcile_complete,
            notes=args.notes,
            source=source,
            info_touched=info_touched,
        )
    except SnapshotInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    write_json(args.out, normalized)
    print(
        json.dumps(
            {
                "out": str(args.out),
                "positions": normalized["counts"]["positions"],
                "open_orders": normalized["counts"]["open_orders"],
                "follower_refresh_complete": normalized["recovery"]["follower_refresh_complete"],
                "follower_subaccount_verified": normalized["follower_subaccount_verified"],
                "info_touched": normalized["info_touched"],
                "exchange_touched": normalized["exchange_touched"],
                "warnings": normalized["warnings"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
