from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


SCORECARD_VERSION = 1
DEFAULT_SAMPLE_SIZE = 50

CANCEL_STATUSES = {
    "canceled",
    "delistedcanceled",
    "liquidatedcanceled",
    "margincanceled",
    "openinterestcapcanceled",
    "reduceonlycanceled",
    "scheduledcancel",
    "selftradecanceled",
    "siblingfilledcanceled",
    "vaultwithdrawalcanceled",
}

TERMINAL_TWAP_STATUSES = {"finished", "terminated", "error"}


class ScorecardInputError(RuntimeError):
    """Raised when replay events are malformed."""


@dataclass(frozen=True)
class ScorecardRow:
    row_id: str
    address: str
    sort_ts_ms: int
    event_type: str
    subtype: str
    category: str
    source_action: str
    classifier_decision: str
    intended_followup: str
    reason: str
    confidence: str
    event_id: str
    metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "row_id": self.row_id,
            "address": self.address,
            "sort_ts_ms": self.sort_ts_ms,
            "event_type": self.event_type,
            "subtype": self.subtype,
            "category": self.category,
            "source_action": self.source_action,
            "classifier_decision": self.classifier_decision,
            "intended_followup": self.intended_followup,
            "reason": self.reason,
            "confidence": self.confidence,
            "event_id": self.event_id,
            "metadata": self.metadata,
        }


def score_replay_events(
    events_path: Path,
    *,
    rows_out: Path | None = None,
    limit_events: int | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> dict[str, Any]:
    if sample_size < 0:
        raise ScorecardInputError("sample_size must be non-negative")
    if limit_events is not None and limit_events < 0:
        raise ScorecardInputError("limit_events must be non-negative")

    scorer = ReplayScorecardScorer()
    event_count = 0
    sample_rows: list[dict[str, Any]] = []
    row_handle = None
    if rows_out is not None:
        rows_out.parent.mkdir(parents=True, exist_ok=True)
        row_handle = rows_out.open("w", encoding="utf-8")
    try:
        for event in iter_replay_events(events_path):
            if limit_events is not None and event_count >= limit_events:
                break
            event_count += 1
            row = scorer.classify(event)
            row_dict = row.as_dict()
            if len(sample_rows) < sample_size:
                sample_rows.append(row_dict)
            if row_handle is not None:
                row_handle.write(json.dumps(row_dict, separators=(",", ":"), sort_keys=True))
                row_handle.write("\n")
    finally:
        if row_handle is not None:
            row_handle.close()

    return scorer.summary(
        events_path=events_path,
        rows_out=rows_out,
        event_count=event_count,
        sample_rows=sample_rows,
        truncated_by_limit=limit_events is not None and event_count >= limit_events,
    )


class ReplayScorecardScorer:
    def __init__(self) -> None:
        self.category_counts: Counter[str] = Counter()
        self.decision_counts: Counter[str] = Counter()
        self.source_action_counts: Counter[str] = Counter()
        self.event_type_counts: Counter[str] = Counter()
        self.confidence_counts: Counter[str] = Counter()
        self.address_counts: Counter[str] = Counter()
        self.snapshot_fingerprints: set[str] = set()
        self.duplicate_snapshot_count = 0
        self.unique_snapshot_count = 0
        self.unknown_rows: list[dict[str, Any]] = []
        self.recovery_rows: list[dict[str, Any]] = []

    def classify(self, event: dict[str, Any]) -> ScorecardRow:
        normalized = normalize_event(event)
        metadata = normalized["metadata"]
        event_type = normalized["event_type"]
        subtype = normalized["subtype"]

        if is_subscription_snapshot(normalized):
            fingerprint = subscription_snapshot_fingerprint(normalized)
            if fingerprint in self.snapshot_fingerprints:
                row = self._row(
                    normalized,
                    category="passive_snapshot",
                    source_action="snapshot_duplicate",
                    classifier_decision="duplicate_snapshot_skipped",
                    intended_followup="do not create a follower action; keep prior seeded state",
                    reason="repeated websocket subscription snapshot item",
                    confidence="high",
                )
                self.duplicate_snapshot_count += 1
            else:
                self.snapshot_fingerprints.add(fingerprint)
                row = self._row(
                    normalized,
                    category="passive_snapshot",
                    source_action="snapshot_seed_or_refresh",
                    classifier_decision="snapshot_seed_or_refresh",
                    intended_followup="seed or refresh source state; do not copy as a new source action",
                    reason="websocket subscription snapshot item",
                    confidence="high",
                )
                self.unique_snapshot_count += 1
        elif event_type == "recovery":
            row = classify_recovery(normalized, self._row)
        elif event_type == "snapshot":
            row = self._row(
                normalized,
                category="state_refresh",
                source_action="rest_snapshot",
                classifier_decision="state_refresh_only",
                intended_followup="refresh source state and equity denominator evidence",
                reason="periodic REST snapshot",
                confidence="high",
            )
        elif event_type == "subscription":
            row = self._row(
                normalized,
                category="passive_control",
                source_action="subscription_ack",
                classifier_decision="observe_only",
                intended_followup="confirm stream subscription; no follower action",
                reason="websocket subscription acknowledgement",
                confidence="high",
            )
        elif event_type == "control":
            row = self._row(
                normalized,
                category="connection_control",
                source_action=subtype,
                classifier_decision="observe_connection_state",
                intended_followup="track connection lifecycle",
                reason="recorder control row",
                confidence="high",
            )
        elif event_type == "order_update":
            row = classify_order_update(normalized, self._row)
        elif event_type == "fill":
            row = self._row(
                normalized,
                category="source_action",
                source_action="source_fill",
                classifier_decision="would_validate_position_and_drift",
                intended_followup="update source fill state, recompute target position, and score follower drift",
                reason="streaming source fill",
                confidence="medium",
            )
        elif event_type == "twap_slice_fill":
            row = self._row(
                normalized,
                category="source_action",
                source_action="source_twap_slice_fill",
                classifier_decision="would_validate_twap_progress",
                intended_followup="update TWAP progress, recompute scaled target, and score drift",
                reason="streaming source TWAP slice fill",
                confidence="medium",
            )
        elif event_type == "twap_history":
            row = classify_twap_history(normalized, self._row)
        elif event_type == "twap_state":
            row = self._row(
                normalized,
                category="state_refresh",
                source_action="twap_state_refresh",
                classifier_decision="state_refresh_only",
                intended_followup="refresh active TWAP state; no follower action by itself",
                reason="TWAP state telemetry",
                confidence="medium",
            )
        elif event_type == "funding":
            row = self._row(
                normalized,
                category="account_state",
                source_action="funding_update",
                classifier_decision="equity_context_update",
                intended_followup="update equity/account context and denominator confidence",
                reason="source funding update",
                confidence="medium",
            )
        elif event_type == "ledger":
            row = self._row(
                normalized,
                category="account_state",
                source_action=f"ledger_{metadata.get('ledger_type', 'unknown')}",
                classifier_decision="equity_context_update",
                intended_followup="update cashflow/equity context and downgrade confidence if needed",
                reason="source non-funding ledger update",
                confidence="medium",
            )
        elif event_type == "cancel":
            row = self._row(
                normalized,
                category="source_action",
                source_action="source_non_user_cancel",
                classifier_decision="would_cancel_or_reconcile_mapped_order",
                intended_followup="cancel mapped follower order if present, otherwise reconcile order state",
                reason="source non-user cancel event",
                confidence="medium",
            )
        elif event_type == "liquidation":
            row = self._row(
                normalized,
                category="risk_event",
                source_action="source_liquidation",
                classifier_decision="confidence_downgrade",
                intended_followup="downgrade source confidence and require state reconciliation",
                reason="source liquidation event",
                confidence="low",
            )
        else:
            row = self._row(
                normalized,
                category="unknown",
                source_action="unknown",
                classifier_decision="needs_review",
                intended_followup="inspect schema before using this event in shadow/live logic",
                reason="unclassified replay event type",
                confidence="low",
            )

        self._record(row)
        return row

    def _row(
        self,
        event: dict[str, Any],
        *,
        category: str,
        source_action: str,
        classifier_decision: str,
        intended_followup: str,
        reason: str,
        confidence: str,
    ) -> ScorecardRow:
        row_payload = [
            event["event_id"],
            category,
            source_action,
            classifier_decision,
        ]
        row_id = hashlib.sha256("|".join(row_payload).encode("utf-8")).hexdigest()[:24]
        return ScorecardRow(
            row_id=row_id,
            address=event["address"],
            sort_ts_ms=event["sort_ts_ms"],
            event_type=event["event_type"],
            subtype=event["subtype"],
            category=category,
            source_action=source_action,
            classifier_decision=classifier_decision,
            intended_followup=intended_followup,
            reason=reason,
            confidence=confidence,
            event_id=event["event_id"],
            metadata=event["metadata"],
        )

    def _record(self, row: ScorecardRow) -> None:
        self.category_counts[row.category] += 1
        self.decision_counts[row.classifier_decision] += 1
        self.source_action_counts[row.source_action] += 1
        self.event_type_counts[row.event_type] += 1
        self.confidence_counts[row.confidence] += 1
        self.address_counts[row.address] += 1
        if row.category == "unknown" and len(self.unknown_rows) < 25:
            self.unknown_rows.append(row.as_dict())
        if row.category == "recovery" and len(self.recovery_rows) < 100:
            self.recovery_rows.append(row.as_dict())

    def summary(
        self,
        *,
        events_path: Path,
        rows_out: Path | None,
        event_count: int,
        sample_rows: list[dict[str, Any]],
        truncated_by_limit: bool,
    ) -> dict[str, Any]:
        return {
            "scorecard_version": SCORECARD_VERSION,
            "read_only": True,
            "exchange_touched": False,
            "events_path": str(events_path),
            "rows_out": str(rows_out) if rows_out is not None else None,
            "events_scored": event_count,
            "truncated_by_limit": truncated_by_limit,
            "snapshot_dedupe": {
                "unique_subscription_snapshots": self.unique_snapshot_count,
                "duplicate_subscription_snapshots": self.duplicate_snapshot_count,
            },
            "counts": {
                "by_category": counter_dict(self.category_counts),
                "by_classifier_decision": counter_dict(self.decision_counts),
                "by_source_action": counter_dict(self.source_action_counts),
                "by_event_type": counter_dict(self.event_type_counts),
                "by_confidence": counter_dict(self.confidence_counts),
                "by_address": counter_dict(self.address_counts),
            },
            "sample_rows": sample_rows,
            "recovery_rows": self.recovery_rows,
            "unknown_rows": self.unknown_rows,
        }


def classify_recovery(event: dict[str, Any], make_row: Any) -> ScorecardRow:
    metadata = event["metadata"]
    if event["subtype"] == "stream_degraded":
        return make_row(
            event,
            category="recovery",
            source_action="stream_degraded",
            classifier_decision="pause_live_stream_hints",
            intended_followup="stop acting on stream hints until REST backfill and reconciliation complete",
            reason=clean(metadata.get("reason")) or "websocket error",
            confidence="high",
        )
    if event["subtype"] == "reconnect_recovered":
        return make_row(
            event,
            category="recovery",
            source_action="reconnect_recovered",
            classifier_decision="requires_rest_backfill_and_reconcile",
            intended_followup="REST backfill fills/TWAPs, refresh source/follower truth, then reconcile",
            reason=f"reconnected after {metadata.get('gap_ms', 'unknown')}ms gap",
            confidence="high",
        )
    return make_row(
        event,
        category="recovery",
        source_action=event["subtype"],
        classifier_decision="recovery_review",
        intended_followup="inspect recovery event before resuming stream trust",
        reason="unknown recovery subtype",
        confidence="low",
    )


def classify_order_update(event: dict[str, Any], make_row: Any) -> ScorecardRow:
    metadata = event["metadata"]
    status = clean(metadata.get("status")).lower()
    if status == "open":
        return make_row(
            event,
            category="source_action",
            source_action="source_order_open",
            classifier_decision="would_map_or_refresh_target_order",
            intended_followup="map scaled follower order intent if policy and state allow",
            reason="source order opened",
            confidence="medium",
        )
    if status == "filled":
        return make_row(
            event,
            category="source_action",
            source_action="source_order_filled",
            classifier_decision="would_validate_position_and_drift",
            intended_followup="use as fill fallback and reconcile source-scaled target state",
            reason="source order filled",
            confidence="medium",
        )
    if status in CANCEL_STATUSES:
        return make_row(
            event,
            category="source_action",
            source_action="source_order_cancel",
            classifier_decision="would_cancel_or_reconcile_mapped_order",
            intended_followup="cancel mapped follower order or repair drift if follower filled",
            reason=f"source order status {status}",
            confidence="medium",
        )
    if status.endswith("rejected"):
        return make_row(
            event,
            category="source_action",
            source_action="source_order_reject",
            classifier_decision="would_drop_or_reconcile_rejected_source_order",
            intended_followup="remove mapped intent if present and reconcile current source truth",
            reason=f"source order status {status}",
            confidence="medium",
        )
    return make_row(
        event,
        category="source_action",
        source_action=f"source_order_{status}",
        classifier_decision="would_reconcile_order_lifecycle",
        intended_followup="refresh order lifecycle state and reconcile source/follower target",
        reason=f"source order status {status}",
        confidence="low",
    )


def classify_twap_history(event: dict[str, Any], make_row: Any) -> ScorecardRow:
    metadata = event["metadata"]
    status = clean(metadata.get("status")).lower()
    if status == "activated":
        return make_row(
            event,
            category="source_action",
            source_action="source_twap_activated",
            classifier_decision="would_map_target_twap_when_supported",
            intended_followup="record source TWAP intent; future TWAP actor should place scaled follower TWAP",
            reason="source TWAP activated",
            confidence="medium",
        )
    if status in TERMINAL_TWAP_STATUSES:
        confidence = "low" if status == "error" else "medium"
        return make_row(
            event,
            category="source_action",
            source_action=f"source_twap_{status}",
            classifier_decision="would_reconcile_twap_terminal_state",
            intended_followup="finish/cancel mapped follower TWAP and reconcile residual drift",
            reason=f"source TWAP terminal status {status}",
            confidence=confidence,
        )
    return make_row(
        event,
        category="unknown",
        source_action=f"source_twap_{status}",
        classifier_decision="needs_review",
        intended_followup="inspect TWAP lifecycle status before mapping",
        reason=f"unknown TWAP status {status}",
        confidence="low",
    )


def iter_replay_events(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        raise ScorecardInputError(f"replay events file does not exist: {path}")
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                event = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ScorecardInputError(f"{path}:{line_no} invalid JSON: {exc.msg}") from exc
            if not isinstance(event, dict):
                raise ScorecardInputError(f"{path}:{line_no} replay event must be an object")
            yield event


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    required = ("address", "event_type", "subtype", "sort_ts_ms", "event_id")
    for key in required:
        if key not in event:
            raise ScorecardInputError(f"replay event missing {key}: {event!r}")
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return {
        "address": str(event["address"]).lower(),
        "event_type": clean(event["event_type"]),
        "subtype": clean(event["subtype"]),
        "sort_ts_ms": parse_int(event["sort_ts_ms"]),
        "event_id": clean(event["event_id"]),
        "metadata": metadata,
    }


def is_subscription_snapshot(event: dict[str, Any]) -> bool:
    metadata = event["metadata"]
    return metadata.get("is_snapshot") is True


def subscription_snapshot_fingerprint(event: dict[str, Any]) -> str:
    metadata = event["metadata"]
    payload = {
        "address": event["address"],
        "event_type": event["event_type"],
        "subtype": event["subtype"],
        "sort_ts_ms": event["sort_ts_ms"],
        "coin": metadata.get("coin"),
        "side": metadata.get("side"),
        "dir": metadata.get("dir"),
        "oid": metadata.get("oid"),
        "tid": metadata.get("tid"),
        "hash": metadata.get("hash"),
        "twap_id": metadata.get("twap_id"),
        "status": metadata.get("status"),
        "ledger_type": metadata.get("ledger_type"),
        "usdc": metadata.get("usdc"),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def parse_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ScorecardInputError(f"expected integer value, got {value!r}") from exc


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
        description="Classify read-only replay events into source-action and passive scorecard rows."
    )
    parser.add_argument(
        "events_path", type=Path, help="Replay events JSONL from replay_live_recording.py."
    )
    parser.add_argument("--out", type=Path, default=None, help="Write scorecard report JSON.")
    parser.add_argument("--rows-out", type=Path, default=None, help="Write scorecard rows JSONL.")
    parser.add_argument("--limit-events", type=int, default=None)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = score_replay_events(
            args.events_path,
            rows_out=args.rows_out,
            limit_events=args.limit_events,
            sample_size=args.sample_size,
        )
    except ScorecardInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.out is not None:
        write_json(args.out, report)
        print(
            json.dumps(
                {
                    "report": str(args.out),
                    "rows_out": str(args.rows_out) if args.rows_out is not None else None,
                    "events_scored": report["events_scored"],
                    "exchange_touched": report["exchange_touched"],
                    "unknown_rows": len(report["unknown_rows"]),
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
