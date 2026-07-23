from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "score_replay_events.py"
SPEC = importlib.util.spec_from_file_location("score_replay_events", SCRIPT_PATH)
assert SPEC is not None
score_replay_events = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = score_replay_events
SPEC.loader.exec_module(score_replay_events)


ADDRESS = "0x1111111111111111111111111111111111111111"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def replay_event(
    *,
    event_type: str,
    subtype: str,
    sort_ts_ms: int,
    metadata: dict | None = None,
    event_id: str | None = None,
) -> dict:
    event_id = event_id or f"{ADDRESS}:{event_type}:{subtype}:{sort_ts_ms}:{len(metadata or {})}"
    return {
        "address": ADDRESS,
        "channel": subtype.split(":")[0],
        "event_id": event_id,
        "event_type": event_type,
        "item_index": 0,
        "kind": "websocket",
        "line_no": 1,
        "metadata": metadata or {},
        "observed_ms": sort_ts_ms + 5,
        "sort_key": [sort_ts_ms, 0, sort_ts_ms + 5, ADDRESS, 0, 1, 0, 0, event_id],
        "sort_ts_ms": sort_ts_ms,
        "source_kind": "event",
        "subtype": subtype,
        "synthetic": event_type == "recovery",
        "timestamp_source": "exchange",
    }


def test_scorecard_classifies_actions_and_passive_snapshots(tmp_path):
    events_path = tmp_path / "events.jsonl"
    rows_out = tmp_path / "rows.jsonl"
    snapshot = replay_event(
        event_type="fill",
        subtype="userFills:snapshot",
        sort_ts_ms=1_000,
        metadata={
            "is_snapshot": True,
            "coin": "BTC",
            "side": "B",
            "oid": "1",
            "tid": "2",
            "hash": "0xfill",
        },
        event_id="snapshot-1",
    )
    duplicate_snapshot = {**snapshot, "event_id": "snapshot-duplicate"}
    rows = [
        snapshot,
        duplicate_snapshot,
        replay_event(
            event_type="fill",
            subtype="user:fills",
            sort_ts_ms=1_100,
            metadata={"coin": "ETH", "side": "A", "dir": "Close Long"},
        ),
        replay_event(
            event_type="order_update",
            subtype="order_update:open",
            sort_ts_ms=1_200,
            metadata={"status": "open", "coin": "BTC", "side": "B", "oid": "9"},
        ),
        replay_event(
            event_type="twap_history",
            subtype="twap_history:finished",
            sort_ts_ms=1_300,
            metadata={"status": "finished", "twap_id": "7", "coin": "SOL"},
        ),
        replay_event(
            event_type="twap_state",
            subtype="twap_state",
            sort_ts_ms=1_400,
            metadata={"twap_id": "7", "coin": "SOL"},
        ),
        replay_event(
            event_type="snapshot",
            subtype="rest_snapshot",
            sort_ts_ms=1_500,
            metadata={"account_value_usd": 100},
        ),
    ]
    write_jsonl(events_path, rows)

    report = score_replay_events.score_replay_events(events_path, rows_out=rows_out)

    assert report["exchange_touched"] is False
    assert report["snapshot_dedupe"] == {
        "duplicate_subscription_snapshots": 1,
        "unique_subscription_snapshots": 1,
    }
    assert report["counts"]["by_classifier_decision"]["snapshot_seed_or_refresh"] == 1
    assert report["counts"]["by_classifier_decision"]["duplicate_snapshot_skipped"] == 1
    assert report["counts"]["by_source_action"]["source_fill"] == 1
    assert report["counts"]["by_source_action"]["source_order_open"] == 1
    assert report["counts"]["by_source_action"]["source_twap_finished"] == 1
    assert report["counts"]["by_category"]["state_refresh"] == 2

    scored_rows = [json.loads(line) for line in rows_out.read_text(encoding="utf-8").splitlines()]
    assert scored_rows[0]["classifier_decision"] == "snapshot_seed_or_refresh"
    assert scored_rows[1]["classifier_decision"] == "duplicate_snapshot_skipped"
    assert "do not copy" in scored_rows[0]["intended_followup"]


def test_scorecard_records_recovery_decisions(tmp_path):
    events_path = tmp_path / "events.jsonl"
    write_jsonl(
        events_path,
        [
            replay_event(
                event_type="recovery",
                subtype="stream_degraded",
                sort_ts_ms=2_000,
                metadata={"reason": "websocket_error", "source_error": "expired"},
            ),
            replay_event(
                event_type="recovery",
                subtype="reconnect_recovered",
                sort_ts_ms=2_500,
                metadata={"gap_ms": 500},
            ),
        ],
    )

    report = score_replay_events.score_replay_events(events_path)

    assert report["counts"]["by_category"] == {"recovery": 2}
    assert report["counts"]["by_classifier_decision"] == {
        "pause_live_stream_hints": 1,
        "requires_rest_backfill_and_reconcile": 1,
    }
    assert report["recovery_rows"][0]["intended_followup"].startswith("stop acting")
    assert "REST backfill" in report["recovery_rows"][1]["intended_followup"]


def test_scorecard_surfaces_unknown_events(tmp_path):
    events_path = tmp_path / "events.jsonl"
    write_jsonl(
        events_path,
        [
            replay_event(
                event_type="mystery",
                subtype="mystery",
                sort_ts_ms=3_000,
                metadata={"shape": "new"},
            )
        ],
    )

    report = score_replay_events.score_replay_events(events_path)

    assert report["counts"]["by_category"] == {"unknown": 1}
    assert report["unknown_rows"][0]["classifier_decision"] == "needs_review"
    assert report["unknown_rows"][0]["confidence"] == "low"
