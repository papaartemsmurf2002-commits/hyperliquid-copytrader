from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "index_live_recording.py"
SPEC = importlib.util.spec_from_file_location("index_live_recording", SCRIPT_PATH)
assert SPEC is not None
index_live_recording = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = index_live_recording
SPEC.loader.exec_module(index_live_recording)


ADDRESS = "0x1111111111111111111111111111111111111111"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def make_recording(tmp_path: Path) -> Path:
    recording_dir = tmp_path / "recording"
    (recording_dir / "events").mkdir(parents=True)
    (recording_dir / "snapshots").mkdir()
    (recording_dir / "manifest.json").write_text(
        json.dumps(
            {
                "addresses": [ADDRESS],
                "address_count": 1,
                "started_ms": 1_000,
                "duration_s": 2,
                "stream_profile": "lean",
                "subscriptions": {
                    ADDRESS: [
                        {"type": "userFills", "user": ADDRESS},
                        {"type": "orderUpdates", "user": ADDRESS},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    (recording_dir / "metrics.json").write_text(
        json.dumps({"final": True, "counters": {"messages": 9, "channel:userFills": 2}}),
        encoding="utf-8",
    )
    write_jsonl(
        recording_dir / "events" / f"{ADDRESS}.jsonl",
        [
            {
                "received_ms": 1_000,
                "address": ADDRESS,
                "kind": "control",
                "event": "connected",
                "subscription_count": 8,
            },
            {
                "received_ms": 1_010,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "subscriptionResponse",
                "message": {
                    "channel": "subscriptionResponse",
                    "data": {
                        "method": "subscribe",
                        "subscription": {"type": "userFills", "user": ADDRESS},
                    },
                },
            },
            {
                "received_ms": 1_020,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "userFills",
                "message": {
                    "channel": "userFills",
                    "data": {
                        "isSnapshot": True,
                        "user": ADDRESS,
                        "fills": [
                            {
                                "coin": "ETH",
                                "px": "10",
                                "sz": "2",
                                "side": "A",
                                "time": 900,
                                "oid": 10,
                                "tid": 20,
                                "hash": "0xold",
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": 1_100,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "userFills",
                "message": {
                    "channel": "userFills",
                    "data": {
                        "isSnapshot": False,
                        "user": ADDRESS,
                        "fills": [
                            {
                                "coin": "BTC",
                                "px": "100",
                                "sz": "0.2",
                                "side": "B",
                                "dir": "Open Long",
                                "time": 1_090,
                                "oid": 1,
                                "tid": 11,
                                "hash": "0xfill",
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": 1_110,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "orderUpdates",
                "message": {
                    "channel": "orderUpdates",
                    "data": [
                        {
                            "order": {
                                "coin": "BTC",
                                "side": "B",
                                "limitPx": "101",
                                "sz": "0.2",
                                "origSz": "0.2",
                                "oid": 1,
                                "timestamp": 1_085,
                            },
                            "status": "open",
                            "statusTimestamp": 1_105,
                        }
                    ],
                },
            },
            {
                "received_ms": 1_120,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "orderUpdates",
                "message": {
                    "channel": "orderUpdates",
                    "data": [
                        {
                            "order": {
                                "coin": "BTC",
                                "side": "B",
                                "limitPx": "102",
                                "sz": "0.2",
                                "origSz": "0.2",
                                "oid": 1,
                            },
                            "status": "open",
                            "statusTimestamp": 1_115,
                        }
                    ],
                },
            },
            {
                "received_ms": 1_130,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "orderUpdates",
                "message": {
                    "channel": "orderUpdates",
                    "data": [
                        {
                            "order": {
                                "coin": "BTC",
                                "side": "B",
                                "limitPx": "102",
                                "sz": "0.2",
                                "origSz": "0.2",
                                "oid": 1,
                            },
                            "status": "filled",
                            "statusTimestamp": 1_125,
                        }
                    ],
                },
            },
            {
                "received_ms": 1_140,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "user",
                "message": {
                    "channel": "user",
                    "data": {
                        "nonUserCancel": [{"coin": "ETH", "oid": 12}],
                        "twapHistory": [
                            {
                                "time": 1_140,
                                "state": {"coin": "SOL", "side": "B", "sz": 1},
                                "status": {"status": "finished", "description": ""},
                                "twapId": 8,
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": 1_150,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "userTwapHistory",
                "message": {
                    "channel": "userTwapHistory",
                    "data": {
                        "isSnapshot": False,
                        "user": ADDRESS,
                        "history": [
                            {
                                "state": {"coin": "SOL", "side": "B", "sz": 1, "timestamp": 1_140},
                                "status": {"status": "activated", "description": ""},
                                "time": 1_145,
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": 1_160,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "userTwapSliceFills",
                "message": {
                    "channel": "userTwapSliceFills",
                    "data": {
                        "isSnapshot": False,
                        "user": ADDRESS,
                        "twapSliceFills": [
                            {
                                "twapId": 7,
                                "fill": {
                                    "coin": "SOL",
                                    "px": "10",
                                    "sz": "1",
                                    "side": "B",
                                    "time": 1_155,
                                    "oid": 2,
                                    "tid": 12,
                                    "hash": "0xtwap",
                                },
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": 1_170,
                "address": ADDRESS,
                "kind": "websocket",
                "channel": "twapStates",
                "message": {
                    "channel": "twapStates",
                    "data": {
                        "user": ADDRESS,
                        "states": [
                            [
                                7,
                                {
                                    "coin": "SOL",
                                    "user": ADDRESS,
                                    "side": "B",
                                    "sz": 1,
                                    "timestamp": 1_165,
                                },
                            ]
                        ],
                    },
                },
            },
            {
                "received_ms": 2_000,
                "address": ADDRESS,
                "kind": "control",
                "event": "websocket_error",
                "error": "ConnectionClosedOK(Close(code=1000, reason='Expired'))",
            },
            {
                "received_ms": 2_500,
                "address": ADDRESS,
                "kind": "control",
                "event": "connected",
                "subscription_count": 8,
            },
        ],
    )
    write_jsonl(
        recording_dir / "snapshots" / f"{ADDRESS}.jsonl",
        [
            {
                "received_ms": 1_300,
                "started_ms": 1_200,
                "address": ADDRESS,
                "kind": "rest_snapshot",
                "results": {
                    "clearinghouseState": {
                        "ok": True,
                        "payload": {
                            "marginSummary": {"accountValue": "100.5"},
                            "assetPositions": [{"position": {"coin": "BTC"}}],
                        },
                    },
                    "openOrders": {"ok": True, "payload": [{"coin": "BTC"}]},
                    "spotClearinghouseState": {
                        "ok": True,
                        "payload": {"balances": [{"coin": "USDC"}]},
                    },
                    "portfolio": {"ok": True, "payload": [["day", {}], ["week", {}]]},
                },
            }
        ],
    )
    return recording_dir


def test_index_live_recording_summarizes_core_activity(tmp_path):
    recording_dir = make_recording(tmp_path)

    summary = index_live_recording.index_recording(recording_dir, large_gap_ms=400)

    assert summary["totals"]["schema_error_count"] == 0
    assert summary["totals"]["shape_warning_count"] == 0
    assert summary["totals"]["channels"]["userFills"] == 2
    account = summary["accounts"][ADDRESS]
    assert account["orders"]["items"] == 3
    assert account["orders"]["status_counts"] == {"filled": 1, "open": 2}
    assert account["orders"]["modified_open_updates"] == 1
    assert account["orders"]["transition_counts"] == [{"key": "open->filled", "count": 1}]
    assert account["twap"]["unique_twap_ids"] == 2
    assert account["twap"]["history_status_counts"] == {"activated": 1, "finished": 1}
    assert account["snapshots"]["request_counts"]["clearinghouseState:ok"] == 1
    assert account["snapshots"]["account_value_usd"]["max"] == 100.5
    assert account["reconnect_windows"][0]["gap_ms"] == 500
    assert account["gaps"]["large_gap_count"] == 2
    assert account["gaps"]["event_received_gap_ms"]["max"] == 830.0


def test_index_live_recording_writes_idempotent_index(tmp_path):
    recording_dir = make_recording(tmp_path)
    out_path = recording_dir / "index.json"

    summary = index_live_recording.index_recording(recording_dir)
    index_live_recording.write_index(summary, out_path)
    first = out_path.read_text(encoding="utf-8")
    index_live_recording.write_index(summary, out_path)
    second = out_path.read_text(encoding="utf-8")

    assert first == second
    assert json.loads(second)["indexer_version"] == 1


def test_index_live_recording_rejects_malformed_required_fields(tmp_path):
    recording_dir = tmp_path / "recording"
    (recording_dir / "events").mkdir(parents=True)
    (recording_dir / "snapshots").mkdir()
    (recording_dir / "manifest.json").write_text(
        json.dumps({"addresses": [ADDRESS], "address_count": 1}),
        encoding="utf-8",
    )
    write_jsonl(
        recording_dir / "events" / f"{ADDRESS}.jsonl",
        [{"address": ADDRESS, "kind": "control", "event": "connected"}],
    )

    with pytest.raises(index_live_recording.RecordingIndexError, match="malformed required"):
        index_live_recording.index_recording(recording_dir)
