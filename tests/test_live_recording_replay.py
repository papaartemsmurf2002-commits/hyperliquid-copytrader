from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "replay_live_recording.py"
SPEC = importlib.util.spec_from_file_location("replay_live_recording", SCRIPT_PATH)
assert SPEC is not None
replay_live_recording = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = replay_live_recording
SPEC.loader.exec_module(replay_live_recording)


ADDRESS_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ADDRESS_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
BASE_MS = 1_785_000_000_000


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
        json.dumps({"addresses": [ADDRESS_A, ADDRESS_B], "address_count": 2}),
        encoding="utf-8",
    )
    write_jsonl(
        recording_dir / "events" / f"{ADDRESS_A}.jsonl",
        [
            {
                "received_ms": BASE_MS + 100,
                "address": ADDRESS_A,
                "kind": "control",
                "event": "connected",
                "subscription_count": 8,
            },
            {
                "received_ms": BASE_MS + 500,
                "address": ADDRESS_A,
                "kind": "websocket",
                "channel": "orderUpdates",
                "message": {
                    "channel": "orderUpdates",
                    "data": [
                        {
                            "order": {
                                "coin": "BTC",
                                "side": "B",
                                "limitPx": "50000",
                                "sz": "0.01",
                                "oid": 1,
                                "cloid": "0xabc",
                            },
                            "status": "open",
                            "statusTimestamp": BASE_MS + 10,
                        }
                    ],
                },
            },
            {
                "received_ms": BASE_MS + 900,
                "address": ADDRESS_A,
                "kind": "control",
                "event": "websocket_error",
                "error": "ConnectionClosedOK(Close(code=1000, reason='Expired'))",
            },
            {
                "received_ms": BASE_MS + 1_500,
                "address": ADDRESS_A,
                "kind": "control",
                "event": "connected",
                "subscription_count": 8,
            },
        ],
    )
    write_jsonl(
        recording_dir / "events" / f"{ADDRESS_B}.jsonl",
        [
            {
                "received_ms": BASE_MS + 90,
                "address": ADDRESS_B,
                "kind": "control",
                "event": "connected",
                "subscription_count": 8,
            },
            {
                "received_ms": BASE_MS + 250,
                "address": ADDRESS_B,
                "kind": "websocket",
                "channel": "user",
                "message": {
                    "channel": "user",
                    "data": {
                        "twapHistory": [
                            {
                                "time": BASE_MS // 1000,
                                "state": {
                                    "coin": "SOL",
                                    "side": "A",
                                    "reduceOnly": True,
                                    "timestamp": BASE_MS - 100,
                                },
                                "status": {"status": "finished"},
                                "twapId": 7,
                            }
                        ]
                    },
                },
            },
            {
                "received_ms": BASE_MS + 300,
                "address": ADDRESS_B,
                "kind": "websocket",
                "channel": "userFills",
                "message": {
                    "channel": "userFills",
                    "data": {
                        "isSnapshot": False,
                        "user": ADDRESS_B,
                        "fills": [
                            {
                                "coin": "ETH",
                                "px": "100",
                                "sz": "0.1",
                                "side": "B",
                                "dir": "Open Long",
                                "time": BASE_MS + 20,
                                "oid": 2,
                                "tid": 3,
                                "hash": "0xfill",
                            }
                        ],
                    },
                },
            },
        ],
    )
    for address in (ADDRESS_A, ADDRESS_B):
        asset_positions = []
        if address == ADDRESS_A:
            asset_positions = [
                {
                    "position": {
                        "coin": "BTC",
                        "entryPx": "50000",
                        "leverage": {"type": "cross", "value": 5},
                        "marginUsed": "200",
                        "positionValue": "1000",
                        "szi": "0.02",
                        "unrealizedPnl": "12.5",
                    }
                }
            ]
        write_jsonl(
            recording_dir / "snapshots" / f"{address}.jsonl",
            [
                {
                    "received_ms": BASE_MS + 2_000,
                    "started_ms": BASE_MS + 1_950,
                    "address": address,
                    "kind": "rest_snapshot",
                    "results": {
                        "clearinghouseState": {
                            "ok": True,
                            "payload": {
                                "marginSummary": {"accountValue": "100"},
                                "assetPositions": asset_positions,
                            },
                        },
                        "openOrders": {"ok": True, "payload": []},
                    },
                }
            ],
        )
    return recording_dir


def test_replay_orders_by_exchange_timestamp_with_observed_fallback(tmp_path):
    recording_dir = make_recording(tmp_path)

    summary = replay_live_recording.replay_recording(
        recording_dir,
        include_snapshots=False,
        chunk_size=2,
    )

    sample = summary["sample_events"]
    assert [event["event_type"] for event in sample[:5]] == [
        "twap_history",
        "order_update",
        "fill",
        "control",
        "control",
    ]
    assert sample[0]["sort_ts_ms"] == BASE_MS
    assert sample[0]["timestamp_source"] == "exchange"
    assert sample[0]["subtype"] == "twap_history:finished"
    assert sample[1]["metadata"]["status"] == "open"
    assert sample[1]["metadata"]["limit_px"] == "50000"
    assert sample[1]["metadata"]["sz"] == "0.01"
    assert sample[1]["metadata"]["notional_usd"] == 500.0
    assert sample[2]["metadata"]["px"] == "100"
    assert sample[2]["metadata"]["sz"] == "0.1"
    assert sample[2]["metadata"]["notional_usd"] == 10.0
    assert summary["counts"]["by_timestamp_source"] == {"exchange": 3, "observed": 6}
    assert summary["exchange_touched"] is False
    assert summary["read_only"] is True


def test_replay_preserves_funding_and_ledger_amount_metadata(tmp_path):
    recording_dir = tmp_path / "recording"
    (recording_dir / "events").mkdir(parents=True)
    (recording_dir / "manifest.json").write_text(
        json.dumps({"addresses": [ADDRESS_A], "address_count": 1}),
        encoding="utf-8",
    )
    write_jsonl(
        recording_dir / "events" / f"{ADDRESS_A}.jsonl",
        [
            {
                "received_ms": BASE_MS + 100,
                "address": ADDRESS_A,
                "kind": "websocket",
                "channel": "userFundings",
                "message": {
                    "channel": "userFundings",
                    "data": {
                        "isSnapshot": False,
                        "fundings": [
                            {
                                "coin": "BTC",
                                "time": BASE_MS + 10,
                                "usdc": "-0.25",
                            }
                        ],
                    },
                },
            },
            {
                "received_ms": BASE_MS + 200,
                "address": ADDRESS_A,
                "kind": "websocket",
                "channel": "userNonFundingLedgerUpdates",
                "message": {
                    "channel": "userNonFundingLedgerUpdates",
                    "data": {
                        "isSnapshot": False,
                        "nonFundingLedgerUpdates": [
                            {
                                "delta": {
                                    "toPerp": True,
                                    "type": "deposit",
                                    "usdc": "5.50",
                                },
                                "hash": "0xledger",
                                "time": BASE_MS + 20,
                            }
                        ],
                    },
                },
            },
        ],
    )

    summary = replay_live_recording.replay_recording(
        recording_dir,
        include_snapshots=False,
        inject_gaps=False,
        chunk_size=1,
    )

    events = {event["event_type"]: event for event in summary["sample_events"]}
    assert events["funding"]["metadata"]["usdc"] == "-0.25"
    assert events["ledger"]["metadata"]["ledger_type"] == "deposit"
    assert events["ledger"]["metadata"]["usdc"] == "5.50"
    assert events["ledger"]["metadata"]["to_perp"] is True


def test_replay_rest_snapshot_preserves_margin_and_leverage_context(tmp_path):
    recording_dir = make_recording(tmp_path)

    summary = replay_live_recording.replay_recording(
        recording_dir,
        addresses=[ADDRESS_A],
        include_snapshots=True,
        inject_gaps=False,
        chunk_size=2,
    )

    snapshot = next(
        event for event in summary["sample_events"] if event["event_type"] == "snapshot"
    )
    metadata = snapshot["metadata"]
    assert metadata["account_value_usd"] == 100.0
    assert metadata["position_count"] == 1
    assert metadata["position_coins"] == ["BTC"]
    assert metadata["position_leverage_by_coin"] == {"BTC": "cross:5"}
    assert metadata["position_leverage_counts"] == {"cross:5": 1}
    assert metadata["position_notional_usd"] == 1000.0
    assert metadata["position_notional_observations"] == 1
    assert metadata["position_margin_used_usd"] == 200.0
    assert metadata["position_margin_used_observations"] == 1
    assert metadata["position_unrealized_pnl_usd"] == 12.5
    assert metadata["position_unrealized_pnl_observations"] == 1


def test_replay_filters_addresses_and_is_deterministic(tmp_path):
    recording_dir = make_recording(tmp_path)

    first = replay_live_recording.replay_recording(
        recording_dir,
        addresses=[ADDRESS_B],
        include_snapshots=False,
        chunk_size=1,
    )
    second = replay_live_recording.replay_recording(
        recording_dir,
        addresses=[ADDRESS_B],
        include_snapshots=False,
        chunk_size=1,
    )

    assert first == second
    assert first["addresses"] == [ADDRESS_B]
    assert first["counts"]["by_address"] == {ADDRESS_B: 3}
    assert set(first["counts"]["by_event_type"]) == {"control", "fill", "twap_history"}


def test_replay_injects_reconnect_recovery_decisions(tmp_path):
    recording_dir = make_recording(tmp_path)

    summary = replay_live_recording.replay_recording(
        recording_dir,
        addresses=[ADDRESS_A],
        include_snapshots=False,
        chunk_size=2,
    )

    assert summary["counts"]["synthetic"] == {
        "reconnect_recovered": 1,
        "stream_degraded": 1,
    }
    decisions = summary["recovery_decisions"]
    assert [decision["event"] for decision in decisions] == [
        "stream_degraded",
        "reconnect_recovered",
    ]
    assert decisions[1]["gap_ms"] == 600
    assert "REST backfill" in decisions[1]["decision"]


def test_replay_writes_events_and_resumes_from_checkpoint(tmp_path):
    recording_dir = make_recording(tmp_path)
    events_out = tmp_path / "events.jsonl"
    checkpoint = tmp_path / "checkpoint.json"

    first = replay_live_recording.replay_recording(
        recording_dir,
        include_snapshots=False,
        limit_events=3,
        events_out=events_out,
        checkpoint_path=checkpoint,
        checkpoint_every=1,
        chunk_size=2,
    )
    resumed = replay_live_recording.replay_recording(
        recording_dir,
        include_snapshots=False,
        resume_checkpoint=checkpoint,
        chunk_size=2,
    )

    emitted_lines = events_out.read_text(encoding="utf-8").strip().splitlines()
    cursor = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert len(emitted_lines) == 3
    assert cursor["sort_key"] == first["last_cursor"]["sort_key"]
    assert resumed["skipped_by_resume"] == 3
    assert resumed["emitted_events"] == first["total_normalized_events"] - 3
    assert resumed["first_cursor"]["sort_key"] > cursor["sort_key"]
