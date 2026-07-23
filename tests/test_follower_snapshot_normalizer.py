from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "normalize_follower_snapshot.py"
SPEC = importlib.util.spec_from_file_location("normalize_follower_snapshot", SCRIPT_PATH)
assert SPEC is not None
normalize_follower_snapshot = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = normalize_follower_snapshot
SPEC.loader.exec_module(normalize_follower_snapshot)


def raw_bundle() -> dict:
    return {
        "received_ms": 123456,
        "results": {
            "clearinghouseState": {
                "ok": True,
                "payload": {
                    "marginSummary": {"accountValue": "50"},
                    "assetPositions": [
                        {
                            "position": {
                                "coin": "BTC",
                                "entryPx": "50000",
                                "leverage": {"type": "cross", "value": 20},
                                "marginUsed": "250",
                                "positionValue": "5000",
                                "szi": "0.1",
                                "unrealizedPnl": "3",
                            }
                        },
                        {
                            "position": {
                                "coin": "ETH",
                                "entryPx": "2500",
                                "positionValue": "500",
                                "szi": "-0.2",
                            }
                        },
                    ],
                },
            },
            "openOrders": {
                "ok": True,
                "payload": [
                    {"coin": "BTC", "limitPx": "51000", "oid": 1, "side": "B", "sz": "0.01"},
                    {"coin": "ETH", "limitPx": "2400", "oid": 2, "side": "A", "sz": "0.1"},
                ],
            },
        },
    }


def valid_address(seed: str = "a") -> str:
    return "0x" + seed * 40


def test_normalizer_converts_rest_bundle_to_drift_snapshot():
    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw_bundle(),
        slot="slot-1",
        follower_subaccount="0xABCDEFabcdefABCDEFabcdefABCDEFabcdefABCD",
        source_backfill_complete=True,
        reconcile_complete=True,
        notes="fixture",
    )

    assert normalized["read_only"] is True
    assert normalized["exchange_touched"] is False
    assert normalized["info_touched"] is False
    assert normalized["follower_subaccount_verified"] is False
    assert normalized["address_verification"] == {
        "expected_follower_subaccount": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        "observed_address": None,
        "observed_address_source": None,
        "reason": "raw snapshot does not include a valid observed address",
        "verified": False,
    }
    assert normalized["follower_subaccount"] == "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
    assert normalized["account_value_usd"] == "50.00000000"
    assert normalized["recovery"] == {
        "follower_refresh_complete": True,
        "notes": "fixture",
        "reconcile_complete": True,
        "source_backfill_complete": True,
    }
    assert normalized["counts"]["position_sides"] == {"long": 1, "short": 1}
    assert normalized["counts"]["open_order_sides"] == {"buy": 1, "sell": 1}
    positions = {position["coin"]: position for position in normalized["positions"]}
    assert positions["BTC"]["signed_notional_usd"] == "5000.00000000"
    assert positions["BTC"]["leverage"] == 20
    assert positions["ETH"]["signed_notional_usd"] == "-500.00000000"
    orders = {order["oid"]: order for order in normalized["open_orders"]}
    assert orders["1"]["signed_notional_usd"] == "510.00000000"
    assert orders["2"]["signed_notional_usd"] == "-240.00000000"


def test_normalizer_verifies_snapshot_address_when_present():
    raw = raw_bundle()
    raw["address"] = valid_address("b")

    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw,
        slot="slot-1",
        follower_subaccount=valid_address("b").upper(),
    )

    assert normalized["follower_subaccount"] == valid_address("b")
    assert normalized["follower_subaccount_verified"] is True
    assert normalized["address_verification"] == {
        "expected_follower_subaccount": valid_address("b"),
        "observed_address": valid_address("b"),
        "observed_address_source": "raw.address",
        "reason": "raw snapshot address matches follower_subaccount",
        "verified": True,
    }
    assert "raw snapshot does not include a valid observed address" not in normalized["warnings"]


def test_normalizer_reads_latest_jsonl_row(tmp_path):
    path = tmp_path / "snapshots.jsonl"
    first = raw_bundle()
    first["received_ms"] = 1
    second = raw_bundle()
    second["received_ms"] = 2
    second["results"]["openOrders"]["payload"] = []
    path.write_text(
        "\n".join(json.dumps(item, separators=(",", ":")) for item in [first, second]) + "\n",
        encoding="utf-8",
    )

    raw = normalize_follower_snapshot.read_input_snapshot(path)
    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw,
        slot="slot-1",
        follower_subaccount="shadow-sub",
    )

    assert normalized["captured_ms"] == 2
    assert normalized["counts"]["positions"] == 2
    assert normalized["counts"]["open_orders"] == 0
    assert normalized["recovery"]["source_backfill_complete"] is False
    assert normalized["recovery"]["reconcile_complete"] is False
    assert normalized["recovery"]["follower_refresh_complete"] is True


def test_normalizer_marks_follower_refresh_incomplete_for_bad_payload():
    raw = raw_bundle()
    raw["results"]["openOrders"] = {"ok": False, "payload": {"error": "unavailable"}}

    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw,
        slot="slot-1",
        follower_subaccount="shadow-sub",
        source_backfill_complete=True,
        reconcile_complete=True,
    )

    assert normalized["recovery"]["follower_refresh_complete"] is False
    assert normalized["request_status"]["openOrders_ok"] is False
    assert any("openOrders validation failed" in item for item in normalized["warnings"])
    assert "follower refresh incomplete" in " ".join(normalized["warnings"])


def test_normalizer_never_treats_missing_positions_as_verified_flat_truth():
    raw = raw_bundle()
    raw["address"] = valid_address("b")
    raw["results"]["clearinghouseState"]["payload"].pop("assetPositions")

    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw,
        slot="slot-1",
        follower_subaccount=valid_address("b"),
        source_backfill_complete=True,
        reconcile_complete=True,
    )

    assert normalized["follower_subaccount_verified"] is True
    assert normalized["recovery"]["follower_refresh_complete"] is False
    assert normalized["positions"] == []
    assert normalized["request_status"]["clearinghouseState_ok"] is False
    assert any("missing assetPositions" in item for item in normalized["warnings"])


def test_normalizer_rejects_malformed_position_and_order_rows():
    raw = raw_bundle()
    raw["results"]["clearinghouseState"]["payload"]["assetPositions"] = [
        {"position": {"coin": "BTC", "szi": "not-a-size"}}
    ]
    raw["results"]["openOrders"]["payload"] = [{"coin": "BTC", "side": "B", "sz": "1"}]

    normalized = normalize_follower_snapshot.normalize_follower_snapshot(
        raw,
        slot="slot-1",
        follower_subaccount="shadow-sub",
    )

    assert normalized["recovery"]["follower_refresh_complete"] is False
    assert normalized["request_status"] == {
        "clearinghouseState_ok": False,
        "openOrders_ok": False,
    }
    assert normalized["positions"] == []
    assert normalized["open_orders"] == []
    assert any("clearinghouseState validation failed" in item for item in normalized["warnings"])
    assert any("openOrders validation failed" in item for item in normalized["warnings"])


def test_fetch_snapshot_rejects_malformed_address():
    try:
        normalize_follower_snapshot.fetch_info_snapshot(
            "0xnot-an-address",
            base_url="https://api.hyperliquid.xyz",
            timeout_s=0.01,
        )
    except normalize_follower_snapshot.SnapshotInputError as exc:
        assert "42-character hex address" in str(exc)
    else:
        raise AssertionError("expected malformed address to fail before network fetch")


def test_fetch_info_snapshot_uses_read_only_info_requests(monkeypatch):
    calls = []

    def fake_post_info(payload, *, base_url, timeout_s):
        calls.append((payload, base_url, timeout_s))
        if payload["type"] == "clearinghouseState":
            return {"marginSummary": {"accountValue": "25"}, "assetPositions": []}
        if payload["type"] == "openOrders":
            return []
        raise AssertionError(f"unexpected payload: {payload}")

    monkeypatch.setattr(normalize_follower_snapshot, "post_info", fake_post_info)

    raw = normalize_follower_snapshot.fetch_info_snapshot(
        valid_address("c"),
        base_url="https://example.test",
        timeout_s=3.5,
    )

    assert raw["address"] == valid_address("c")
    assert raw["kind"] == "read_only_info_snapshot"
    assert raw["results"]["clearinghouseState"]["payload"]["marginSummary"]["accountValue"] == "25"
    assert raw["results"]["openOrders"]["payload"] == []
    assert calls == [
        (
            {"type": "clearinghouseState", "user": valid_address("c")},
            "https://example.test",
            3.5,
        ),
        ({"type": "openOrders", "user": valid_address("c")}, "https://example.test", 3.5),
    ]


def test_fetch_mode_requires_fetch_address_to_match_follower_subaccount(tmp_path, monkeypatch):
    def fail_post_info(*args, **kwargs):
        raise AssertionError("fetch mismatch must fail before network calls")

    monkeypatch.setattr(normalize_follower_snapshot, "post_info", fail_post_info)

    exit_code = normalize_follower_snapshot.main(
        [
            "--fetch-address",
            valid_address("d"),
            "--follower-subaccount",
            valid_address("e"),
            "--slot",
            "slot-1",
            "--out",
            str(tmp_path / "snapshot.json"),
        ]
    )

    assert exit_code == 2
    assert not (tmp_path / "snapshot.json").exists()


def test_fetch_mode_writes_verified_snapshot(tmp_path, monkeypatch):
    def fake_post_info(payload, *, base_url, timeout_s):
        if payload["type"] == "clearinghouseState":
            return {"marginSummary": {"accountValue": "25"}, "assetPositions": []}
        return []

    monkeypatch.setattr(normalize_follower_snapshot, "post_info", fake_post_info)
    out = tmp_path / "snapshot.json"

    exit_code = normalize_follower_snapshot.main(
        [
            "--fetch-address",
            valid_address("f"),
            "--follower-subaccount",
            valid_address("f"),
            "--slot",
            "slot-1",
            "--out",
            str(out),
        ]
    )

    assert exit_code == 0
    normalized = json.loads(out.read_text(encoding="utf-8"))
    assert normalized["info_touched"] is True
    assert normalized["exchange_touched"] is False
    assert normalized["follower_subaccount_verified"] is True
    assert normalized["address_verification"]["observed_address"] == valid_address("f")
