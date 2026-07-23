from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


INDEXER_VERSION = 1
MS_PER_MINUTE = 60_000
DEFAULT_LARGE_GAP_MS = 10_000
DEFAULT_MAX_EXAMPLES = 25

EXPECTED_WEBSOCKET_CHANNELS = {
    "orderUpdates",
    "subscriptionResponse",
    "twapStates",
    "user",
    "userFills",
    "userFundings",
    "userNonFundingLedgerUpdates",
    "userTwapHistory",
    "userTwapSliceFills",
}

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


class RecordingIndexError(RuntimeError):
    """Raised when a recording contains malformed required JSONL fields."""


@dataclass
class RunningStats:
    count: int = 0
    minimum: float | None = None
    maximum: float | None = None
    total: float = 0.0

    def add(self, value: float | int | None) -> None:
        if value is None:
            return
        number = float(value)
        self.count += 1
        self.total += number
        self.minimum = number if self.minimum is None else min(self.minimum, number)
        self.maximum = number if self.maximum is None else max(self.maximum, number)

    def as_dict(self) -> dict[str, Any]:
        if not self.count:
            return {"count": 0}
        return {
            "count": self.count,
            "min": round(self.minimum or 0.0, 3),
            "max": round(self.maximum or 0.0, 3),
            "avg": round(self.total / self.count, 3),
        }


@dataclass
class AccountAccumulator:
    address: str
    large_gap_ms: int
    max_examples: int
    event_file_bytes: int = 0
    snapshot_file_bytes: int = 0
    event_lines: int = 0
    snapshot_lines: int = 0
    first_received_ms: int | None = None
    last_received_ms: int | None = None
    kind_counts: Counter[str] = field(default_factory=Counter)
    channel_counts: Counter[str] = field(default_factory=Counter)
    channel_item_counts: Counter[str] = field(default_factory=Counter)
    channel_snapshot_messages: Counter[str] = field(default_factory=Counter)
    subscription_response_types: Counter[str] = field(default_factory=Counter)
    control_event_counts: Counter[str] = field(default_factory=Counter)
    user_event_subtypes: Counter[str] = field(default_factory=Counter)
    fill_side_counts: Counter[str] = field(default_factory=Counter)
    fill_dir_counts: Counter[str] = field(default_factory=Counter)
    fill_coin_counts: Counter[str] = field(default_factory=Counter)
    order_coin_counts: Counter[str] = field(default_factory=Counter)
    order_side_counts: Counter[str] = field(default_factory=Counter)
    order_status_counts: Counter[str] = field(default_factory=Counter)
    order_transition_counts: Counter[str] = field(default_factory=Counter)
    twap_coin_counts: Counter[str] = field(default_factory=Counter)
    twap_history_status_counts: Counter[str] = field(default_factory=Counter)
    funding_coin_counts: Counter[str] = field(default_factory=Counter)
    ledger_type_counts: Counter[str] = field(default_factory=Counter)
    ledger_coin_counts: Counter[str] = field(default_factory=Counter)
    snapshot_request_counts: Counter[str] = field(default_factory=Counter)
    snapshot_error_counts: Counter[str] = field(default_factory=Counter)
    snapshot_portfolio_windows: Counter[str] = field(default_factory=Counter)
    unique_order_ids: set[str] = field(default_factory=set)
    unique_fill_ids: set[str] = field(default_factory=set)
    unique_twap_ids: set[str] = field(default_factory=set)
    stream_lag_ms: RunningStats = field(default_factory=RunningStats)
    snapshot_event_age_ms: RunningStats = field(default_factory=RunningStats)
    received_gap_ms: RunningStats = field(default_factory=RunningStats)
    snapshot_received_gap_ms: RunningStats = field(default_factory=RunningStats)
    snapshot_duration_ms: RunningStats = field(default_factory=RunningStats)
    account_value_usd: RunningStats = field(default_factory=RunningStats)
    fill_notional_usd: float = 0.0
    order_notional_usd: float = 0.0
    order_update_items: int = 0
    cancel_like_order_updates: int = 0
    rejected_like_order_updates: int = 0
    modified_open_updates: int = 0
    twap_state_items: int = 0
    twap_history_items: int = 0
    twap_slice_fill_items: int = 0
    funding_items: int = 0
    ledger_items: int = 0
    open_order_snapshot_items: int = 0
    position_snapshot_items: int = 0
    spot_balance_snapshot_items: int = 0
    large_received_gap_count: int = 0
    large_received_gap_examples: list[dict[str, Any]] = field(default_factory=list)
    reconnect_windows: list[dict[str, Any]] = field(default_factory=list)
    _last_received_ms: int | None = None
    _last_snapshot_received_ms: int | None = None
    _pending_disconnect: dict[str, Any] | None = None
    _last_order_status: dict[str, str] = field(default_factory=dict)
    _last_order_signature: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    _minute_buckets: dict[str, Counter[int]] = field(default_factory=lambda: defaultdict(Counter))

    def observe_record(
        self,
        received_ms: int,
        channel: str | None = None,
        *,
        track_event_gap: bool = True,
    ) -> None:
        if self.first_received_ms is None:
            self.first_received_ms = received_ms
        self.last_received_ms = received_ms
        if track_event_gap and self._last_received_ms is not None:
            gap_ms = received_ms - self._last_received_ms
            if gap_ms >= 0:
                self.received_gap_ms.add(gap_ms)
                if gap_ms >= self.large_gap_ms:
                    self.large_received_gap_count += 1
                    if len(self.large_received_gap_examples) < self.max_examples:
                        self.large_received_gap_examples.append(
                            {
                                "previous_received_ms": self._last_received_ms,
                                "received_ms": received_ms,
                                "gap_ms": gap_ms,
                            }
                        )
        if track_event_gap:
            self._last_received_ms = received_ms
        if channel:
            minute = received_ms // MS_PER_MINUTE
            self._minute_buckets[channel][minute] += 1
            self._minute_buckets["all"][minute] += 1

    def observe_snapshot_record(self, received_ms: int) -> None:
        if self._last_snapshot_received_ms is not None:
            gap_ms = received_ms - self._last_snapshot_received_ms
            if gap_ms >= 0:
                self.snapshot_received_gap_ms.add(gap_ms)
        self._last_snapshot_received_ms = received_ms

    def observe_control_event(self, event: str, received_ms: int, error: str | None) -> None:
        self.control_event_counts[event] += 1
        if event == "websocket_error":
            self._pending_disconnect = {"disconnected_ms": received_ms, "error": error}
        elif event == "connected" and self._pending_disconnect is not None:
            disconnected_ms = self._pending_disconnect["disconnected_ms"]
            self.reconnect_windows.append(
                {
                    "disconnected_ms": disconnected_ms,
                    "reconnected_ms": received_ms,
                    "gap_ms": max(0, received_ms - disconnected_ms),
                    "error": self._pending_disconnect.get("error"),
                }
            )
            self._pending_disconnect = None

    def observe_order_update(
        self,
        update: dict[str, Any],
        *,
        received_ms: int,
    ) -> None:
        self.order_update_items += 1
        status = str(update.get("status") or "unknown")
        normalized_status = status.lower()
        self.order_status_counts[status] += 1
        self.channel_item_counts["orderUpdates"] += 1
        status_ts = event_time_ms(update.get("statusTimestamp"))
        self.stream_lag_ms.add(received_ms - status_ts if status_ts is not None else None)
        if normalized_status in CANCEL_STATUSES:
            self.cancel_like_order_updates += 1
        if normalized_status.endswith("rejected"):
            self.rejected_like_order_updates += 1

        order = update.get("order")
        if not isinstance(order, dict):
            return
        coin = clean_label(order.get("coin"))
        oid = clean_label(order.get("oid"))
        side = clean_label(order.get("side"))
        if coin != "unknown":
            self.order_coin_counts[coin] += 1
        if side != "unknown":
            self.order_side_counts[side] += 1
        if oid != "unknown":
            key = f"{coin}:{oid}"
            self.unique_order_ids.add(key)
            previous_status = self._last_order_status.get(key)
            if previous_status and previous_status != status:
                self.order_transition_counts[f"{previous_status}->{status}"] += 1
            signature = (
                clean_label(order.get("limitPx")),
                clean_label(order.get("sz")),
                clean_label(order.get("origSz")),
                clean_label(order.get("side")),
                clean_label(order.get("reduceOnly")),
            )
            previous_signature = self._last_order_signature.get(key)
            if (
                normalized_status == "open"
                and previous_signature
                and previous_signature != signature
            ):
                self.modified_open_updates += 1
            self._last_order_status[key] = status
            self._last_order_signature[key] = signature
        self.order_notional_usd += notional(order.get("limitPx"), order.get("sz"))

    def observe_fill(
        self,
        fill: dict[str, Any],
        *,
        channel: str,
        received_ms: int,
        is_snapshot: bool,
        twap_id: Any = None,
    ) -> None:
        self.channel_item_counts[channel] += 1
        coin = clean_label(fill.get("coin"))
        side = clean_label(fill.get("side"))
        direction = clean_label(fill.get("dir"))
        if coin != "unknown":
            self.fill_coin_counts[coin] += 1
        if side != "unknown":
            self.fill_side_counts[side] += 1
        if direction != "unknown":
            self.fill_dir_counts[direction] += 1
        fill_twap_id = twap_id if twap_id is not None else fill.get("twapId")
        if fill_twap_id not in (None, ""):
            self.unique_twap_ids.add(clean_label(fill_twap_id))
        fill_id = ":".join(
            [
                clean_label(fill.get("time")),
                coin,
                clean_label(fill.get("oid")),
                clean_label(fill.get("tid")),
                clean_label(fill.get("hash")),
            ]
        )
        self.unique_fill_ids.add(fill_id)
        fill_ts = event_time_ms(fill.get("time"))
        if fill_ts is not None:
            lag = received_ms - fill_ts
            if is_snapshot:
                self.snapshot_event_age_ms.add(lag)
            else:
                self.stream_lag_ms.add(lag)
        self.fill_notional_usd += notional(fill.get("px"), fill.get("sz"))

    def observe_twap_state(self, twap_id: Any, state: dict[str, Any]) -> None:
        self.twap_state_items += 1
        self.channel_item_counts["twapStates"] += 1
        if twap_id not in (None, ""):
            self.unique_twap_ids.add(clean_label(twap_id))
        coin = clean_label(state.get("coin"))
        if coin != "unknown":
            self.twap_coin_counts[coin] += 1

    def observe_twap_history(
        self,
        item: dict[str, Any],
        *,
        received_ms: int,
        is_snapshot: bool,
        channel: str = "userTwapHistory",
    ) -> None:
        self.twap_history_items += 1
        self.channel_item_counts[channel] += 1
        if item.get("twapId") not in (None, ""):
            self.unique_twap_ids.add(clean_label(item.get("twapId")))
        status = item.get("status")
        if isinstance(status, dict):
            self.twap_history_status_counts[clean_label(status.get("status"))] += 1
        else:
            self.twap_history_status_counts[clean_label(status)] += 1
        state = item.get("state")
        if isinstance(state, dict):
            coin = clean_label(state.get("coin"))
            if coin != "unknown":
                self.twap_coin_counts[coin] += 1
        event_ts = event_time_ms(item.get("time"))
        if event_ts is not None:
            lag = received_ms - event_ts
            if is_snapshot:
                self.snapshot_event_age_ms.add(lag)
            else:
                self.stream_lag_ms.add(lag)

    def observe_funding(
        self,
        funding: dict[str, Any],
        *,
        received_ms: int,
        is_snapshot: bool,
    ) -> None:
        self.funding_items += 1
        coin = clean_label(funding.get("coin"))
        if coin != "unknown":
            self.funding_coin_counts[coin] += 1
        event_ts = event_time_ms(funding.get("time"))
        if event_ts is not None:
            lag = received_ms - event_ts
            if is_snapshot:
                self.snapshot_event_age_ms.add(lag)
            else:
                self.stream_lag_ms.add(lag)

    def observe_ledger_update(
        self,
        update: dict[str, Any],
        *,
        received_ms: int,
        is_snapshot: bool,
    ) -> None:
        self.ledger_items += 1
        delta = update.get("delta")
        if isinstance(delta, dict):
            ledger_type = clean_label(delta.get("type"))
            self.ledger_type_counts[ledger_type] += 1
            for position in as_list(delta.get("liquidatedPositions")):
                if isinstance(position, dict):
                    coin = clean_label(position.get("coin"))
                    if coin != "unknown":
                        self.ledger_coin_counts[coin] += 1
        else:
            self.ledger_type_counts[clean_label(update.get("type"))] += 1
        event_ts = event_time_ms(update.get("time"))
        if event_ts is not None:
            lag = received_ms - event_ts
            if is_snapshot:
                self.snapshot_event_age_ms.add(lag)
            else:
                self.stream_lag_ms.add(lag)

    def observe_snapshot_result(
        self,
        request_type: str,
        result: dict[str, Any],
    ) -> None:
        ok = result.get("ok") is True
        self.snapshot_request_counts[f"{request_type}:{'ok' if ok else 'error'}"] += 1
        if not ok:
            self.snapshot_error_counts[request_type] += 1
            return
        payload = result.get("payload")
        if request_type == "clearinghouseState" and isinstance(payload, dict):
            margin = payload.get("marginSummary")
            if isinstance(margin, dict):
                self.account_value_usd.add(parse_float(margin.get("accountValue")))
            positions = as_list(payload.get("assetPositions"))
            self.position_snapshot_items += len(positions)
        elif request_type == "openOrders":
            orders = as_list(payload)
            self.open_order_snapshot_items += len(orders)
        elif request_type == "spotClearinghouseState" and isinstance(payload, dict):
            self.spot_balance_snapshot_items += len(as_list(payload.get("balances")))
        elif request_type == "portfolio":
            for window in portfolio_windows(payload):
                self.snapshot_portfolio_windows[window] += 1

    def as_dict(self) -> dict[str, Any]:
        duration_ms = (
            self.last_received_ms - self.first_received_ms
            if self.first_received_ms is not None and self.last_received_ms is not None
            else None
        )
        duration_min = (duration_ms / MS_PER_MINUTE) if duration_ms and duration_ms > 0 else 0.0
        return {
            "address": self.address,
            "files": {
                "event_bytes": self.event_file_bytes,
                "snapshot_bytes": self.snapshot_file_bytes,
                "event_lines": self.event_lines,
                "snapshot_lines": self.snapshot_lines,
            },
            "time_window": {
                "first_received_ms": self.first_received_ms,
                "last_received_ms": self.last_received_ms,
                "duration_ms": duration_ms,
                "duration_minutes": round(duration_min, 3),
            },
            "kinds": dict(sorted(self.kind_counts.items())),
            "websocket": {
                "channels": self._channel_summary(duration_min),
                "subscription_response_types": counter_dict(self.subscription_response_types),
                "stream_lag_ms": self.stream_lag_ms.as_dict(),
                "snapshot_event_age_ms": self.snapshot_event_age_ms.as_dict(),
            },
            "source_activity": {
                "unique_fills_seen": len(self.unique_fill_ids),
                "fill_items_seen": sum(self.fill_coin_counts.values()),
                "top_fill_coins": top_counter(self.fill_coin_counts),
                "fill_sides": counter_dict(self.fill_side_counts),
                "fill_dirs": counter_dict(self.fill_dir_counts),
                "approx_fill_notional_usd": round(self.fill_notional_usd, 6),
                "funding_items": self.funding_items,
                "funding_coins": top_counter(self.funding_coin_counts),
                "ledger_items": self.ledger_items,
                "ledger_types": counter_dict(self.ledger_type_counts),
                "ledger_coins": top_counter(self.ledger_coin_counts),
                "user_event_subtypes": counter_dict(self.user_event_subtypes),
            },
            "orders": {
                "items": self.order_update_items,
                "unique_order_ids": len(self.unique_order_ids),
                "status_counts": counter_dict(self.order_status_counts),
                "transition_counts": top_counter(self.order_transition_counts),
                "top_order_coins": top_counter(self.order_coin_counts),
                "sides": counter_dict(self.order_side_counts),
                "cancel_like_updates": self.cancel_like_order_updates,
                "rejected_like_updates": self.rejected_like_order_updates,
                "modified_open_updates": self.modified_open_updates,
                "cancel_like_rate": ratio(self.cancel_like_order_updates, self.order_update_items),
                "modified_open_rate": ratio(self.modified_open_updates, self.order_update_items),
                "approx_order_notional_usd": round(self.order_notional_usd, 6),
            },
            "twap": {
                "unique_twap_ids": len(self.unique_twap_ids),
                "state_items": self.twap_state_items,
                "history_items": self.twap_history_items,
                "slice_fill_items": self.twap_slice_fill_items,
                "history_status_counts": counter_dict(self.twap_history_status_counts),
                "top_twap_coins": top_counter(self.twap_coin_counts),
            },
            "snapshots": {
                "request_counts": counter_dict(self.snapshot_request_counts),
                "error_counts": counter_dict(self.snapshot_error_counts),
                "duration_ms": self.snapshot_duration_ms.as_dict(),
                "account_value_usd": self.account_value_usd.as_dict(),
                "position_items": self.position_snapshot_items,
                "open_order_items": self.open_order_snapshot_items,
                "spot_balance_items": self.spot_balance_snapshot_items,
                "portfolio_windows": counter_dict(self.snapshot_portfolio_windows),
            },
            "gaps": {
                "event_received_gap_ms": self.received_gap_ms.as_dict(),
                "snapshot_received_gap_ms": self.snapshot_received_gap_ms.as_dict(),
                "large_gap_threshold_ms": self.large_gap_ms,
                "large_gap_count": self.large_received_gap_count,
                "large_gap_examples": self.large_received_gap_examples,
            },
            "reconnect_windows": self.reconnect_windows,
        }

    def _channel_summary(self, duration_min: float) -> dict[str, Any]:
        channels: dict[str, Any] = {}
        for channel, count in sorted(self.channel_counts.items()):
            peak = peak_minute(self._minute_buckets.get(channel, Counter()))
            channels[channel] = {
                "messages": count,
                "items": self.channel_item_counts.get(channel, 0),
                "snapshot_messages": self.channel_snapshot_messages.get(channel, 0),
                "avg_messages_per_minute": round(count / duration_min, 6) if duration_min else 0.0,
                "peak_minute": peak,
            }
        return channels


class RecordingIndexer:
    def __init__(
        self,
        recording_dir: Path,
        *,
        large_gap_ms: int = DEFAULT_LARGE_GAP_MS,
        max_examples: int = DEFAULT_MAX_EXAMPLES,
    ) -> None:
        self.recording_dir = recording_dir
        self.large_gap_ms = large_gap_ms
        self.max_examples = max_examples
        self.accounts: dict[str, AccountAccumulator] = {}
        self.schema_error_count = 0
        self.schema_error_examples: list[dict[str, Any]] = []
        self.shape_warning_count = 0
        self.shape_warning_examples: list[dict[str, Any]] = []
        self.unexpected_channels: Counter[str] = Counter()

    def index(self) -> dict[str, Any]:
        if not self.recording_dir.exists():
            raise RecordingIndexError(f"recording directory does not exist: {self.recording_dir}")
        manifest = read_json_file(self.recording_dir / "manifest.json")
        metrics = read_json_file(self.recording_dir / "metrics.json")
        addresses = self._discover_addresses(manifest)
        for address in addresses:
            account = self._account(address)
            event_path = self.recording_dir / "events" / f"{address}.jsonl"
            snapshot_path = self.recording_dir / "snapshots" / f"{address}.jsonl"
            if event_path.exists():
                account.event_file_bytes = event_path.stat().st_size
                self._index_jsonl(
                    event_path, lambda record, line_no: self._handle_event(record, line_no)
                )
            else:
                self._shape_warning(event_path, 0, "missing event file", {"address": address})
            if snapshot_path.exists():
                account.snapshot_file_bytes = snapshot_path.stat().st_size
                self._index_jsonl(
                    snapshot_path,
                    lambda record, line_no: self._handle_snapshot(record, line_no),
                )
            else:
                self._shape_warning(snapshot_path, 0, "missing snapshot file", {"address": address})

        summary = self._summary(manifest, metrics)
        if self.schema_error_count:
            first = self.schema_error_examples[0] if self.schema_error_examples else {}
            raise RecordingIndexError(
                f"{self.schema_error_count} malformed required JSONL fields; first={first!r}"
            )
        return summary

    def _discover_addresses(self, manifest: Any) -> list[str]:
        addresses: set[str] = set()
        if isinstance(manifest, dict):
            addresses.update(str(address).lower() for address in as_list(manifest.get("addresses")))
        for folder_name in ("events", "snapshots"):
            folder = self.recording_dir / folder_name
            if folder.exists():
                addresses.update(path.stem.lower() for path in folder.glob("*.jsonl"))
        return sorted(address for address in addresses if valid_address(address))

    def _account(self, address: str) -> AccountAccumulator:
        if address not in self.accounts:
            self.accounts[address] = AccountAccumulator(
                address=address,
                large_gap_ms=self.large_gap_ms,
                max_examples=self.max_examples,
            )
        return self.accounts[address]

    def _index_jsonl(self, path: Path, handler: Any) -> None:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    record = json.loads(text)
                except json.JSONDecodeError as exc:
                    self._schema_error(path, line_no, f"invalid JSON: {exc.msg}", None)
                    continue
                if not isinstance(record, dict):
                    self._schema_error(path, line_no, "JSONL record must be an object", record)
                    continue
                handler(record, line_no)

    def _handle_event(self, record: dict[str, Any], line_no: int) -> None:
        path = self.recording_dir / "events" / f"{record.get('address', 'unknown')}.jsonl"
        address = self._required_address(record, path, line_no)
        received_ms = self._required_int(record, "received_ms", path, line_no)
        kind = self._required_string(record, "kind", path, line_no)
        if address is None or received_ms is None or kind is None:
            return
        account = self._account(address)
        account.event_lines += 1
        account.kind_counts[kind] += 1
        if kind == "control":
            account.observe_record(received_ms, "control")
            event = self._required_string(record, "event", path, line_no)
            if event is None:
                return
            error = record.get("error")
            account.observe_control_event(
                event,
                received_ms,
                str(error) if error not in (None, "") else None,
            )
        elif kind == "websocket":
            channel = self._required_string(record, "channel", path, line_no)
            message = record.get("message")
            if channel is None:
                return
            account.observe_record(received_ms, channel)
            account.channel_counts[channel] += 1
            if channel not in EXPECTED_WEBSOCKET_CHANNELS:
                self.unexpected_channels[channel] += 1
                self._shape_warning(
                    path, line_no, f"unexpected websocket channel {channel}", record
                )
            if not isinstance(message, dict):
                self._schema_error(path, line_no, "websocket message must be an object", record)
                return
            if message.get("parse_error") is True:
                self._shape_warning(path, line_no, "recorded websocket raw parse error", record)
            message_channel = message.get("channel")
            if isinstance(message_channel, str) and message_channel != channel:
                self._shape_warning(
                    path,
                    line_no,
                    "record channel differs from nested message channel",
                    {"record_channel": channel, "message_channel": message_channel},
                )
            self._handle_websocket_message(account, channel, message, received_ms, path, line_no)
        else:
            self._schema_error(path, line_no, f"unsupported event kind {kind!r}", record)

    def _handle_snapshot(self, record: dict[str, Any], line_no: int) -> None:
        path = self.recording_dir / "snapshots" / f"{record.get('address', 'unknown')}.jsonl"
        address = self._required_address(record, path, line_no)
        received_ms = self._required_int(record, "received_ms", path, line_no)
        started_ms = self._required_int(record, "started_ms", path, line_no)
        kind = self._required_string(record, "kind", path, line_no)
        results = record.get("results")
        if address is None or received_ms is None or started_ms is None or kind is None:
            return
        account = self._account(address)
        account.snapshot_lines += 1
        account.kind_counts[kind] += 1
        account.observe_snapshot_record(received_ms)
        account.observe_record(received_ms, "rest_snapshot", track_event_gap=False)
        account.snapshot_duration_ms.add(received_ms - started_ms)
        if kind != "rest_snapshot":
            self._schema_error(path, line_no, f"unsupported snapshot kind {kind!r}", record)
            return
        if not isinstance(results, dict):
            self._schema_error(path, line_no, "snapshot results must be an object", record)
            return
        for request_type, result in sorted(results.items()):
            if not isinstance(result, dict):
                self._schema_error(
                    path, line_no, f"snapshot result {request_type} must be an object", record
                )
                continue
            account.observe_snapshot_result(str(request_type), result)

    def _handle_websocket_message(
        self,
        account: AccountAccumulator,
        channel: str,
        message: dict[str, Any],
        received_ms: int,
        path: Path,
        line_no: int,
    ) -> None:
        data = message.get("data")
        if channel == "subscriptionResponse":
            self._handle_subscription_response(account, data, path, line_no)
        elif channel == "orderUpdates":
            for update in self._required_list(data, path, line_no, "orderUpdates data"):
                if isinstance(update, dict):
                    account.observe_order_update(update, received_ms=received_ms)
                else:
                    self._shape_warning(path, line_no, "order update item is not an object", update)
        elif channel == "userFills":
            data_dict = self._required_dict(data, path, line_no, "userFills data")
            if data_dict is not None:
                is_snapshot = data_dict.get("isSnapshot") is True
                if is_snapshot:
                    account.channel_snapshot_messages[channel] += 1
                for fill in self._list_field(data_dict, "fills", path, line_no):
                    if isinstance(fill, dict):
                        account.observe_fill(
                            fill,
                            channel=channel,
                            received_ms=received_ms,
                            is_snapshot=is_snapshot,
                        )
        elif channel == "user":
            self._handle_user_event(account, data, received_ms, path, line_no)
        elif channel == "userFundings":
            data_dict = self._required_dict(data, path, line_no, "userFundings data")
            if data_dict is not None:
                is_snapshot = data_dict.get("isSnapshot") is True
                if is_snapshot:
                    account.channel_snapshot_messages[channel] += 1
                for funding in self._list_field(data_dict, "fundings", path, line_no):
                    if isinstance(funding, dict):
                        account.channel_item_counts[channel] += 1
                        account.observe_funding(
                            funding,
                            received_ms=received_ms,
                            is_snapshot=is_snapshot,
                        )
        elif channel == "userNonFundingLedgerUpdates":
            data_dict = self._required_dict(data, path, line_no, "ledger data")
            if data_dict is not None:
                is_snapshot = data_dict.get("isSnapshot") is True
                if is_snapshot:
                    account.channel_snapshot_messages[channel] += 1
                updates = self._first_list_field(
                    data_dict,
                    ("nonFundingLedgerUpdates", "ledgerUpdates", "updates"),
                    path,
                    line_no,
                )
                for update in updates:
                    if isinstance(update, dict):
                        account.channel_item_counts[channel] += 1
                        account.observe_ledger_update(
                            update,
                            received_ms=received_ms,
                            is_snapshot=is_snapshot,
                        )
        elif channel == "userTwapSliceFills":
            data_dict = self._required_dict(data, path, line_no, "userTwapSliceFills data")
            if data_dict is not None:
                is_snapshot = data_dict.get("isSnapshot") is True
                if is_snapshot:
                    account.channel_snapshot_messages[channel] += 1
                for slice_fill in self._list_field(data_dict, "twapSliceFills", path, line_no):
                    if isinstance(slice_fill, dict):
                        fill = slice_fill.get("fill")
                        if isinstance(fill, dict):
                            account.twap_slice_fill_items += 1
                            account.observe_fill(
                                fill,
                                channel=channel,
                                received_ms=received_ms,
                                is_snapshot=is_snapshot,
                                twap_id=slice_fill.get("twapId"),
                            )
        elif channel == "userTwapHistory":
            data_dict = self._required_dict(data, path, line_no, "userTwapHistory data")
            if data_dict is not None:
                is_snapshot = data_dict.get("isSnapshot") is True
                if is_snapshot:
                    account.channel_snapshot_messages[channel] += 1
                for item in self._list_field(data_dict, "history", path, line_no):
                    if isinstance(item, dict):
                        account.observe_twap_history(
                            item,
                            received_ms=received_ms,
                            is_snapshot=is_snapshot,
                        )
        elif channel == "twapStates":
            data_dict = self._required_dict(data, path, line_no, "twapStates data")
            if data_dict is not None:
                for raw_state in self._list_field(data_dict, "states", path, line_no):
                    twap_id: Any = None
                    state: Any = None
                    if isinstance(raw_state, list | tuple) and len(raw_state) >= 2:
                        twap_id, state = raw_state[0], raw_state[1]
                    elif isinstance(raw_state, dict):
                        state = raw_state
                        twap_id = raw_state.get("twapId") or raw_state.get("id")
                    if isinstance(state, dict):
                        account.observe_twap_state(twap_id, state)
                    else:
                        self._shape_warning(
                            path, line_no, "twap state item shape is unexpected", raw_state
                        )

    def _handle_subscription_response(
        self,
        account: AccountAccumulator,
        data: Any,
        path: Path,
        line_no: int,
    ) -> None:
        if not isinstance(data, dict):
            self._shape_warning(path, line_no, "subscriptionResponse data is not an object", data)
            return
        subscription = data.get("subscription")
        if isinstance(subscription, dict):
            account.subscription_response_types[clean_label(subscription.get("type"))] += 1
        else:
            self._shape_warning(path, line_no, "subscriptionResponse missing subscription", data)

    def _handle_user_event(
        self,
        account: AccountAccumulator,
        data: Any,
        received_ms: int,
        path: Path,
        line_no: int,
    ) -> None:
        if not isinstance(data, dict):
            self._shape_warning(path, line_no, "user event data is not an object", data)
            return
        known = False
        if "fills" in data:
            known = True
            account.user_event_subtypes["fills"] += 1
            for fill in self._list_field(data, "fills", path, line_no):
                if isinstance(fill, dict):
                    account.observe_fill(
                        fill,
                        channel="user",
                        received_ms=received_ms,
                        is_snapshot=False,
                    )
        if "funding" in data:
            known = True
            account.user_event_subtypes["funding"] += 1
            funding = data.get("funding")
            if isinstance(funding, dict):
                account.channel_item_counts["user"] += 1
                account.observe_funding(funding, received_ms=received_ms, is_snapshot=False)
        if "liquidation" in data:
            known = True
            account.user_event_subtypes["liquidation"] += 1
            account.channel_item_counts["user"] += 1
        if "nonUserCancel" in data:
            known = True
            account.user_event_subtypes["nonUserCancel"] += 1
            for cancel in self._list_field(data, "nonUserCancel", path, line_no):
                account.channel_item_counts["user"] += 1
                if isinstance(cancel, dict):
                    coin = clean_label(cancel.get("coin"))
                    if coin != "unknown":
                        account.order_coin_counts[coin] += 1
        if "twapHistory" in data:
            known = True
            account.user_event_subtypes["twapHistory"] += 1
            for item in self._list_field(data, "twapHistory", path, line_no):
                if isinstance(item, dict):
                    account.observe_twap_history(
                        item,
                        received_ms=received_ms,
                        is_snapshot=False,
                        channel="user",
                    )
        if "twapSliceFills" in data:
            known = True
            account.user_event_subtypes["twapSliceFills"] += 1
            for slice_fill in self._list_field(data, "twapSliceFills", path, line_no):
                if isinstance(slice_fill, dict):
                    fill = slice_fill.get("fill")
                    if isinstance(fill, dict):
                        account.twap_slice_fill_items += 1
                        account.observe_fill(
                            fill,
                            channel="user",
                            received_ms=received_ms,
                            is_snapshot=False,
                            twap_id=slice_fill.get("twapId"),
                        )
        if not known:
            self._shape_warning(
                path, line_no, "unknown user event payload keys", sorted(data.keys())
            )

    def _required_address(
        self,
        record: dict[str, Any],
        path: Path,
        line_no: int,
    ) -> str | None:
        address = record.get("address")
        if not isinstance(address, str) or not valid_address(address.lower()):
            self._schema_error(
                path, line_no, "record address must be a 42-character hex string", record
            )
            return None
        return address.lower()

    def _required_int(
        self,
        record: dict[str, Any],
        key: str,
        path: Path,
        line_no: int,
    ) -> int | None:
        value = parse_int(record.get(key))
        if value is None:
            self._schema_error(path, line_no, f"record {key} must be an integer", record)
        return value

    def _required_string(
        self,
        record: dict[str, Any],
        key: str,
        path: Path,
        line_no: int,
    ) -> str | None:
        value = record.get(key)
        if not isinstance(value, str) or not value:
            self._schema_error(path, line_no, f"record {key} must be a non-empty string", record)
            return None
        return value

    def _required_dict(
        self,
        value: Any,
        path: Path,
        line_no: int,
        label: str,
    ) -> dict[str, Any] | None:
        if isinstance(value, dict):
            return value
        self._shape_warning(path, line_no, f"{label} should be an object", value)
        return None

    def _required_list(
        self,
        value: Any,
        path: Path,
        line_no: int,
        label: str,
    ) -> list[Any]:
        if isinstance(value, list):
            return value
        self._shape_warning(path, line_no, f"{label} should be a list", value)
        return []

    def _list_field(
        self,
        data: dict[str, Any],
        key: str,
        path: Path,
        line_no: int,
    ) -> list[Any]:
        return self._required_list(data.get(key), path, line_no, f"{key} field")

    def _first_list_field(
        self,
        data: dict[str, Any],
        keys: tuple[str, ...],
        path: Path,
        line_no: int,
    ) -> list[Any]:
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
        self._shape_warning(path, line_no, f"expected one of list fields {keys}", data)
        return []

    def _schema_error(self, path: Path, line_no: int, message: str, sample: Any) -> None:
        self.schema_error_count += 1
        if len(self.schema_error_examples) < self.max_examples:
            self.schema_error_examples.append(
                {
                    "path": str(path),
                    "line": line_no,
                    "message": message,
                    "sample": compact_sample(sample),
                }
            )

    def _shape_warning(self, path: Path, line_no: int, message: str, sample: Any) -> None:
        self.shape_warning_count += 1
        if len(self.shape_warning_examples) < self.max_examples:
            self.shape_warning_examples.append(
                {
                    "path": str(path),
                    "line": line_no,
                    "message": message,
                    "sample": compact_sample(sample),
                }
            )

    def _summary(self, manifest: Any, metrics: Any) -> dict[str, Any]:
        accounts = {
            address: account.as_dict() for address, account in sorted(self.accounts.items())
        }
        channel_counts: Counter[str] = Counter()
        channel_item_counts: Counter[str] = Counter()
        kind_counts: Counter[str] = Counter()
        for account in self.accounts.values():
            channel_counts.update(account.channel_counts)
            channel_item_counts.update(account.channel_item_counts)
            kind_counts.update(account.kind_counts)
        return {
            "indexer_version": INDEXER_VERSION,
            "recording_dir": str(self.recording_dir),
            "manifest": manifest_summary(manifest),
            "metrics": metrics_summary(metrics),
            "totals": {
                "accounts": len(accounts),
                "event_lines": sum(account.event_lines for account in self.accounts.values()),
                "snapshot_lines": sum(account.snapshot_lines for account in self.accounts.values()),
                "event_bytes": sum(account.event_file_bytes for account in self.accounts.values()),
                "snapshot_bytes": sum(
                    account.snapshot_file_bytes for account in self.accounts.values()
                ),
                "kinds": counter_dict(kind_counts),
                "channels": counter_dict(channel_counts),
                "payload_items_by_channel": counter_dict(channel_item_counts),
                "schema_error_count": self.schema_error_count,
                "shape_warning_count": self.shape_warning_count,
                "unexpected_channels": counter_dict(self.unexpected_channels),
            },
            "accounts": accounts,
            "schema_errors": {
                "total": self.schema_error_count,
                "examples": self.schema_error_examples,
            },
            "shape_warnings": {
                "total": self.shape_warning_count,
                "examples": self.shape_warning_examples,
            },
        }


def index_recording(
    recording_dir: Path,
    *,
    large_gap_ms: int = DEFAULT_LARGE_GAP_MS,
    max_examples: int = DEFAULT_MAX_EXAMPLES,
) -> dict[str, Any]:
    return RecordingIndexer(
        recording_dir,
        large_gap_ms=large_gap_ms,
        max_examples=max_examples,
    ).index()


def write_index(summary: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(out_path)


def read_json_file(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def valid_address(value: str) -> bool:
    text = value.lower()
    return (
        len(text) == 42
        and text.startswith("0x")
        and all(char in "0123456789abcdef" for char in text[2:])
    )


def parse_int(value: Any) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def event_time_ms(value: Any) -> int | None:
    timestamp = parse_int(value)
    if timestamp is None:
        return None
    if 1_000_000_000 <= timestamp < 10_000_000_000:
        return timestamp * 1000
    return timestamp


def parse_float(value: Any) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_label(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def notional(px: Any, sz: Any) -> float:
    price = parse_float(px)
    size = parse_float(sz)
    if price is None or size is None:
        return 0.0
    return abs(price * size)


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def portfolio_windows(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        return sorted(str(key) for key in payload)
    windows: list[str] = []
    for item in as_list(payload):
        if isinstance(item, list) and item:
            windows.append(str(item[0]))
    return sorted(windows)


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def top_counter(counter: Counter[str], limit: int = 20) -> list[dict[str, Any]]:
    return [
        {"key": key, "count": count}
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
    ]


def peak_minute(counter: Counter[int]) -> dict[str, Any]:
    if not counter:
        return {"minute_ms": None, "count": 0}
    minute, count = max(counter.items(), key=lambda item: (item[1], -item[0]))
    return {"minute_ms": minute * MS_PER_MINUTE, "count": count}


def ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 6)


def compact_sample(sample: Any, max_chars: int = 500) -> Any:
    if sample is None:
        return None
    text = repr(sample)
    if len(text) <= max_chars:
        return sample
    return text[: max_chars - 3] + "..."


def manifest_summary(manifest: Any) -> dict[str, Any] | None:
    if not isinstance(manifest, dict):
        return None
    return {
        "started_ms": manifest.get("started_ms"),
        "started_utc": manifest.get("started_utc"),
        "duration_s": manifest.get("duration_s"),
        "address_count": manifest.get("address_count"),
        "addresses": manifest.get("addresses"),
        "stream_profile": manifest.get("stream_profile"),
        "snapshot_interval_s": manifest.get("snapshot_interval_s"),
        "subscription_types": {
            address: [subscription.get("type") for subscription in as_list(subscriptions)]
            for address, subscriptions in sorted((manifest.get("subscriptions") or {}).items())
        },
    }


def metrics_summary(metrics: Any) -> dict[str, Any] | None:
    if not isinstance(metrics, dict):
        return None
    counters = metrics.get("counters")
    channel_counts = {
        key.removeprefix("channel:"): value
        for key, value in (counters or {}).items()
        if isinstance(key, str) and key.startswith("channel:")
    }
    return {
        "final": metrics.get("final"),
        "started_ms": metrics.get("started_ms"),
        "updated_ms": metrics.get("updated_ms"),
        "uptime_s": metrics.get("uptime_s"),
        "messages": (counters or {}).get("messages") if isinstance(counters, dict) else None,
        "channel_counts": dict(sorted(channel_counts.items())),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a read-only analytics index for a Hyperliquid live recording."
    )
    parser.add_argument(
        "recording_dir",
        type=Path,
        help="Recording directory containing manifest.json, events/, and snapshots/.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output index path. Defaults to <recording_dir>/index.json.",
    )
    parser.add_argument("--large-gap-ms", type=int, default=DEFAULT_LARGE_GAP_MS)
    parser.add_argument("--max-examples", type=int, default=DEFAULT_MAX_EXAMPLES)
    parser.add_argument("--no-write", action="store_true", help="Validate and print summary only.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = index_recording(
            args.recording_dir,
            large_gap_ms=args.large_gap_ms,
            max_examples=args.max_examples,
        )
    except RecordingIndexError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    out_path = args.out or args.recording_dir / "index.json"
    if not args.no_write:
        write_index(summary, out_path)
    print(
        json.dumps(
            {
                "index": None if args.no_write else str(out_path),
                "accounts": summary["totals"]["accounts"],
                "event_lines": summary["totals"]["event_lines"],
                "snapshot_lines": summary["totals"]["snapshot_lines"],
                "schema_errors": summary["totals"]["schema_error_count"],
                "shape_warnings": summary["totals"]["shape_warning_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
