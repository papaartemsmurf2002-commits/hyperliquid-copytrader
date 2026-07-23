from __future__ import annotations

import argparse
import heapq
import json
import sys
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from hyperliquid_copytrader.markets import MarketIdentityError, canonical_market_symbol

REPLAY_VERSION = 1
MS_PER_SECOND = 1000
DEFAULT_CHUNK_SIZE = 100_000
DEFAULT_SAMPLE_SIZE = 25

TIMESTAMP_SOURCE_RANK = {
    "exchange": 0,
    "observed": 1,
}

SOURCE_KIND_RANK = {
    "event": 0,
    "snapshot": 1,
}


class ReplayInputError(RuntimeError):
    """Raised when a recording cannot be replayed deterministically."""


@dataclass(frozen=True)
class ReplayEvent:
    address: str
    kind: str
    channel: str
    event_type: str
    subtype: str
    sort_ts_ms: int
    timestamp_source: str
    observed_ms: int
    source_kind: str
    line_no: int
    item_index: int
    event_id: str
    synthetic: bool = False
    metadata: dict[str, Any] | None = None

    @property
    def sort_key(self) -> list[Any]:
        return [
            self.sort_ts_ms,
            TIMESTAMP_SOURCE_RANK.get(self.timestamp_source, 9),
            self.observed_ms,
            self.address,
            SOURCE_KIND_RANK.get(self.source_kind, 9),
            self.line_no,
            self.item_index,
            1 if self.synthetic else 0,
            self.event_id,
        ]

    def as_dict(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "kind": self.kind,
            "channel": self.channel,
            "event_type": self.event_type,
            "subtype": self.subtype,
            "sort_ts_ms": self.sort_ts_ms,
            "timestamp_source": self.timestamp_source,
            "observed_ms": self.observed_ms,
            "source_kind": self.source_kind,
            "line_no": self.line_no,
            "item_index": self.item_index,
            "event_id": self.event_id,
            "synthetic": self.synthetic,
            "metadata": self.metadata or {},
            "sort_key": self.sort_key,
        }


def replay_recording(
    recording_dir: Path,
    *,
    addresses: list[str] | None = None,
    max_records_per_file: int | None = None,
    limit_events: int | None = None,
    include_snapshots: bool = True,
    inject_gaps: bool = True,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    events_out: Path | None = None,
    checkpoint_path: Path | None = None,
    resume_checkpoint: Path | None = None,
    checkpoint_every: int = 10_000,
    tmp_dir: Path | None = None,
) -> dict[str, Any]:
    if chunk_size <= 0:
        raise ReplayInputError("chunk_size must be positive")
    if limit_events is not None and limit_events < 0:
        raise ReplayInputError("limit_events must be non-negative")
    selected_addresses = discover_addresses(recording_dir, addresses)
    resume_cursor = read_checkpoint(resume_checkpoint) if resume_checkpoint else None

    with tempfile.TemporaryDirectory(dir=tmp_dir) as temp_name:
        chunk_dir = Path(temp_name)
        chunk_paths, total_normalized = write_sorted_chunks(
            iter_recording_events(
                recording_dir,
                selected_addresses,
                max_records_per_file=max_records_per_file,
                include_snapshots=include_snapshots,
                inject_gaps=inject_gaps,
            ),
            chunk_dir=chunk_dir,
            chunk_size=chunk_size,
        )
        summary = consume_replay(
            chunk_paths,
            recording_dir=recording_dir,
            addresses=selected_addresses,
            total_normalized=total_normalized,
            limit_events=limit_events,
            sample_size=sample_size,
            events_out=events_out,
            checkpoint_path=checkpoint_path,
            resume_cursor=resume_cursor,
            checkpoint_every=checkpoint_every,
            options={
                "max_records_per_file": max_records_per_file,
                "include_snapshots": include_snapshots,
                "inject_gaps": inject_gaps,
                "chunk_size": chunk_size,
            },
        )
    return summary


def iter_recording_events(
    recording_dir: Path,
    addresses: list[str],
    *,
    max_records_per_file: int | None,
    include_snapshots: bool,
    inject_gaps: bool,
) -> Iterator[ReplayEvent]:
    for address in addresses:
        event_path = recording_dir / "events" / f"{address}.jsonl"
        if not event_path.exists():
            raise ReplayInputError(f"missing event file for {address}: {event_path}")
        yield from iter_event_file(
            event_path,
            address,
            max_records=max_records_per_file,
            inject_gaps=inject_gaps,
        )
        if include_snapshots:
            snapshot_path = recording_dir / "snapshots" / f"{address}.jsonl"
            if not snapshot_path.exists():
                raise ReplayInputError(f"missing snapshot file for {address}: {snapshot_path}")
            yield from iter_snapshot_file(snapshot_path, address, max_records=max_records_per_file)


def iter_event_file(
    path: Path,
    address: str,
    *,
    max_records: int | None,
    inject_gaps: bool,
) -> Iterator[ReplayEvent]:
    pending_disconnect: dict[str, Any] | None = None
    for line_no, record in iter_jsonl(path, max_records=max_records):
        record_address = required_address(record, path, line_no)
        if record_address != address:
            raise ReplayInputError(
                f"{path}:{line_no} address {record_address} does not match file address {address}"
            )
        received_ms = required_int(record, "received_ms", path, line_no)
        kind = required_str(record, "kind", path, line_no)
        if kind == "control":
            event = required_str(record, "event", path, line_no)
            yield make_observed_event(
                address=address,
                kind=kind,
                channel="control",
                event_type="control",
                subtype=event,
                observed_ms=received_ms,
                source_kind="event",
                line_no=line_no,
                item_index=0,
                metadata={
                    key: record[key] for key in ("error", "subscription_count") if key in record
                },
            )
            if inject_gaps and event == "websocket_error":
                pending_disconnect = {
                    "disconnected_ms": received_ms,
                    "error": record.get("error"),
                }
                yield make_observed_event(
                    address=address,
                    kind="synthetic",
                    channel="replay",
                    event_type="recovery",
                    subtype="stream_degraded",
                    observed_ms=received_ms,
                    source_kind="event",
                    line_no=line_no,
                    item_index=1,
                    synthetic=True,
                    metadata={
                        "reason": "websocket_error",
                        "source_error": record.get("error"),
                    },
                )
            elif inject_gaps and event == "connected" and pending_disconnect is not None:
                disconnected_ms = (
                    parse_int(pending_disconnect.get("disconnected_ms")) or received_ms
                )
                yield make_observed_event(
                    address=address,
                    kind="synthetic",
                    channel="replay",
                    event_type="recovery",
                    subtype="reconnect_recovered",
                    observed_ms=received_ms,
                    source_kind="event",
                    line_no=line_no,
                    item_index=1,
                    synthetic=True,
                    metadata={
                        "disconnected_ms": disconnected_ms,
                        "reconnected_ms": received_ms,
                        "gap_ms": max(0, received_ms - disconnected_ms),
                        "source_error": pending_disconnect.get("error"),
                    },
                )
                pending_disconnect = None
        elif kind == "websocket":
            channel = required_str(record, "channel", path, line_no)
            message = record.get("message")
            if not isinstance(message, dict):
                raise ReplayInputError(f"{path}:{line_no} websocket message must be an object")
            yield from websocket_message_events(
                address=address,
                channel=channel,
                message=message,
                observed_ms=received_ms,
                line_no=line_no,
            )
        else:
            raise ReplayInputError(f"{path}:{line_no} unsupported event kind {kind!r}")


def websocket_message_events(
    *,
    address: str,
    channel: str,
    message: dict[str, Any],
    observed_ms: int,
    line_no: int,
) -> Iterator[ReplayEvent]:
    data = message.get("data")
    if channel == "subscriptionResponse":
        subscription_type = "unknown"
        if isinstance(data, dict) and isinstance(data.get("subscription"), dict):
            subscription_type = clean(data["subscription"].get("type"))
        yield make_observed_event(
            address=address,
            kind="websocket",
            channel=channel,
            event_type="subscription",
            subtype=subscription_type,
            observed_ms=observed_ms,
            source_kind="event",
            line_no=line_no,
            item_index=0,
        )
    elif channel == "orderUpdates":
        for item_index, update in enumerate(required_list(data, "orderUpdates data")):
            if not isinstance(update, dict):
                continue
            raw_order = update.get("order")
            order: dict[str, Any] = raw_order if isinstance(raw_order, dict) else {}
            status = clean(update.get("status"))
            metadata = order_update_metadata(order, status=status)
            yield make_event(
                address=address,
                kind="websocket",
                channel=channel,
                event_type="order_update",
                subtype=f"order_update:{status}",
                event_ts_ms=event_time_ms(update.get("statusTimestamp")),
                observed_ms=observed_ms,
                source_kind="event",
                line_no=line_no,
                item_index=item_index,
                metadata=metadata,
            )
    elif channel == "userFills":
        if isinstance(data, dict):
            is_snapshot = data.get("isSnapshot") is True
            for item_index, fill in enumerate(required_list(data.get("fills"), "userFills.fills")):
                if isinstance(fill, dict):
                    yield fill_event(
                        address=address,
                        channel=channel,
                        subtype="userFills:snapshot" if is_snapshot else "userFills:stream",
                        fill=fill,
                        observed_ms=observed_ms,
                        line_no=line_no,
                        item_index=item_index,
                        is_snapshot=is_snapshot,
                    )
    elif channel == "user":
        if isinstance(data, dict):
            yield from user_channel_events(
                address=address,
                data=data,
                observed_ms=observed_ms,
                line_no=line_no,
            )
    elif channel == "userFundings":
        if isinstance(data, dict):
            is_snapshot = data.get("isSnapshot") is True
            for item_index, funding in enumerate(required_list(data.get("fundings"), "fundings")):
                if isinstance(funding, dict):
                    yield funding_event(
                        address=address,
                        channel=channel,
                        subtype="userFundings:snapshot" if is_snapshot else "userFundings:stream",
                        funding=funding,
                        observed_ms=observed_ms,
                        line_no=line_no,
                        item_index=item_index,
                        is_snapshot=is_snapshot,
                    )
    elif channel == "userNonFundingLedgerUpdates":
        if isinstance(data, dict):
            updates = first_list_field(
                data,
                ("nonFundingLedgerUpdates", "ledgerUpdates", "updates"),
            )
            is_snapshot = data.get("isSnapshot") is True
            for item_index, update in enumerate(updates):
                if isinstance(update, dict):
                    raw_delta = update.get("delta")
                    delta: dict[str, Any] = raw_delta if isinstance(raw_delta, dict) else {}
                    yield make_event(
                        address=address,
                        kind="websocket",
                        channel=channel,
                        event_type="ledger",
                        subtype=f"ledger:{clean(delta.get('type') or update.get('type'))}",
                        event_ts_ms=event_time_ms(update.get("time")),
                        observed_ms=observed_ms,
                        source_kind="event",
                        line_no=line_no,
                        item_index=item_index,
                        metadata={
                            "is_snapshot": is_snapshot,
                            "ledger_type": clean(delta.get("type") or update.get("type")),
                            "hash": clean(update.get("hash")),
                            "usdc": clean(delta.get("usdc") or update.get("usdc")),
                            "to_perp": (
                                delta.get("toPerp")
                                if isinstance(delta.get("toPerp"), bool)
                                else None
                            ),
                        },
                    )
    elif channel == "userTwapSliceFills":
        if isinstance(data, dict):
            is_snapshot = data.get("isSnapshot") is True
            for item_index, slice_fill in enumerate(
                required_list(data.get("twapSliceFills"), "twapSliceFills")
            ):
                if isinstance(slice_fill, dict):
                    fill = slice_fill.get("fill")
                    if isinstance(fill, dict):
                        yield twap_slice_fill_event(
                            address=address,
                            channel=channel,
                            subtype=(
                                "userTwapSliceFills:snapshot"
                                if is_snapshot
                                else "userTwapSliceFills:stream"
                            ),
                            slice_fill=slice_fill,
                            fill=fill,
                            observed_ms=observed_ms,
                            line_no=line_no,
                            item_index=item_index,
                            is_snapshot=is_snapshot,
                        )
    elif channel == "userTwapHistory":
        if isinstance(data, dict):
            is_snapshot = data.get("isSnapshot") is True
            for item_index, item in enumerate(required_list(data.get("history"), "history")):
                if isinstance(item, dict):
                    yield twap_history_event(
                        address=address,
                        channel=channel,
                        item=item,
                        observed_ms=observed_ms,
                        line_no=line_no,
                        item_index=item_index,
                        is_snapshot=is_snapshot,
                    )
    elif channel == "twapStates":
        if isinstance(data, dict):
            for item_index, raw_state in enumerate(required_list(data.get("states"), "states")):
                twap_id: Any = None
                state: Any = None
                if isinstance(raw_state, list | tuple) and len(raw_state) >= 2:
                    twap_id, state = raw_state[0], raw_state[1]
                elif isinstance(raw_state, dict):
                    state = raw_state
                    twap_id = raw_state.get("twapId") or raw_state.get("id")
                if isinstance(state, dict):
                    yield make_observed_event(
                        address=address,
                        kind="websocket",
                        channel=channel,
                        event_type="twap_state",
                        subtype="twap_state",
                        observed_ms=observed_ms,
                        source_kind="event",
                        line_no=line_no,
                        item_index=item_index,
                        metadata={
                            "twap_id": clean(twap_id),
                            "coin": clean(state.get("coin")),
                            "side": clean(state.get("side")),
                            "state_timestamp_ms": event_time_ms(state.get("timestamp")),
                        },
                    )
    else:
        yield make_observed_event(
            address=address,
            kind="websocket",
            channel=channel,
            event_type="websocket",
            subtype=channel,
            observed_ms=observed_ms,
            source_kind="event",
            line_no=line_no,
            item_index=0,
        )


def user_channel_events(
    *,
    address: str,
    data: dict[str, Any],
    observed_ms: int,
    line_no: int,
) -> Iterator[ReplayEvent]:
    item_index = 0
    for fill in required_list(data.get("fills"), "user.fills") if "fills" in data else []:
        if isinstance(fill, dict):
            yield fill_event(
                address=address,
                channel="user",
                subtype="user:fills",
                fill=fill,
                observed_ms=observed_ms,
                line_no=line_no,
                item_index=item_index,
                is_snapshot=False,
            )
            item_index += 1
    if isinstance(data.get("funding"), dict):
        yield funding_event(
            address=address,
            channel="user",
            subtype="user:funding",
            funding=data["funding"],
            observed_ms=observed_ms,
            line_no=line_no,
            item_index=item_index,
            is_snapshot=False,
        )
        item_index += 1
    if isinstance(data.get("liquidation"), dict):
        liquidation = data["liquidation"]
        yield make_observed_event(
            address=address,
            kind="websocket",
            channel="user",
            event_type="liquidation",
            subtype="user:liquidation",
            observed_ms=observed_ms,
            source_kind="event",
            line_no=line_no,
            item_index=item_index,
            metadata={
                "lid": clean(liquidation.get("lid")),
                "liquidated_user": clean(liquidation.get("liquidated_user")),
            },
        )
        item_index += 1
    for cancel in (
        required_list(data.get("nonUserCancel"), "user.nonUserCancel")
        if "nonUserCancel" in data
        else []
    ):
        if isinstance(cancel, dict):
            yield make_observed_event(
                address=address,
                kind="websocket",
                channel="user",
                event_type="cancel",
                subtype="user:nonUserCancel",
                observed_ms=observed_ms,
                source_kind="event",
                line_no=line_no,
                item_index=item_index,
                metadata={
                    "coin": clean(cancel.get("coin")),
                    "oid": clean(cancel.get("oid")),
                },
            )
            item_index += 1
    for item in (
        required_list(data.get("twapHistory"), "user.twapHistory") if "twapHistory" in data else []
    ):
        if isinstance(item, dict):
            yield twap_history_event(
                address=address,
                channel="user",
                item=item,
                observed_ms=observed_ms,
                line_no=line_no,
                item_index=item_index,
                is_snapshot=False,
            )
            item_index += 1
    for slice_fill in (
        required_list(data.get("twapSliceFills"), "user.twapSliceFills")
        if "twapSliceFills" in data
        else []
    ):
        if isinstance(slice_fill, dict):
            fill = slice_fill.get("fill")
            if isinstance(fill, dict):
                yield twap_slice_fill_event(
                    address=address,
                    channel="user",
                    subtype="user:twapSliceFills",
                    slice_fill=slice_fill,
                    fill=fill,
                    observed_ms=observed_ms,
                    line_no=line_no,
                    item_index=item_index,
                    is_snapshot=False,
                )
                item_index += 1


def iter_snapshot_file(
    path: Path, address: str, *, max_records: int | None
) -> Iterator[ReplayEvent]:
    for line_no, record in iter_jsonl(path, max_records=max_records):
        record_address = required_address(record, path, line_no)
        if record_address != address:
            raise ReplayInputError(
                f"{path}:{line_no} address {record_address} does not match file address {address}"
            )
        received_ms = required_int(record, "received_ms", path, line_no)
        kind = required_str(record, "kind", path, line_no)
        if kind != "rest_snapshot":
            raise ReplayInputError(f"{path}:{line_no} unsupported snapshot kind {kind!r}")
        results = record.get("results")
        if not isinstance(results, dict):
            raise ReplayInputError(f"{path}:{line_no} snapshot results must be an object")
        ok_count = 0
        error_count = 0
        account_value = None
        position_context = empty_snapshot_position_context()
        for request_type, result in results.items():
            if not isinstance(result, dict):
                continue
            if result.get("ok") is True:
                ok_count += 1
            else:
                error_count += 1
            if request_type == "clearinghouseState" and isinstance(result.get("payload"), dict):
                payload = result["payload"]
                margin = payload.get("marginSummary")
                if isinstance(margin, dict):
                    account_value = parse_float(margin.get("accountValue"))
                position_context = snapshot_position_context(payload)
        yield make_observed_event(
            address=address,
            kind=kind,
            channel="rest_snapshot",
            event_type="snapshot",
            subtype="rest_snapshot",
            observed_ms=received_ms,
            source_kind="snapshot",
            line_no=line_no,
            item_index=0,
            metadata={
                "ok_count": ok_count,
                "error_count": error_count,
                "request_types": sorted(str(key) for key in results),
                "account_value_usd": account_value,
                **position_context,
            },
        )


def empty_snapshot_position_context() -> dict[str, Any]:
    return {
        "position_count": 0,
        "position_coins": [],
        "position_leverage_by_coin": {},
        "position_leverage_counts": {},
        "position_notional_usd": None,
        "position_notional_observations": 0,
        "position_margin_used_usd": None,
        "position_margin_used_observations": 0,
        "position_unrealized_pnl_usd": None,
        "position_unrealized_pnl_observations": 0,
    }


def snapshot_position_context(clearinghouse_payload: dict[str, Any]) -> dict[str, Any]:
    raw_positions = clearinghouse_payload.get("assetPositions")
    if not isinstance(raw_positions, list):
        return empty_snapshot_position_context()
    coins: set[str] = set()
    leverage_by_coin: dict[str, str] = {}
    leverage_counts: Counter[str] = Counter()
    position_notional_usd = 0.0
    position_margin_used_usd = 0.0
    position_unrealized_pnl_usd = 0.0
    notional_observations = 0
    margin_used_observations = 0
    unrealized_pnl_observations = 0
    for item in raw_positions:
        if not isinstance(item, dict):
            continue
        raw_position = item.get("position")
        position: dict[str, Any] = raw_position if isinstance(raw_position, dict) else item
        try:
            coin = canonical_market_symbol(clean(position.get("coin")))
        except MarketIdentityError:
            continue
        size = parse_float(position.get("szi"))
        if coin == "UNKNOWN" or size is None or size == 0.0:
            continue
        coins.add(coin)
        notional = parse_float(position.get("positionValue"))
        if notional is None:
            entry_px = parse_float(position.get("entryPx"))
            if entry_px is not None:
                notional = size * entry_px
        if notional is not None:
            position_notional_usd += abs(notional)
            notional_observations += 1
        margin_used = parse_float(position.get("marginUsed"))
        if margin_used is not None:
            position_margin_used_usd += margin_used
            margin_used_observations += 1
        unrealized_pnl = parse_float(position.get("unrealizedPnl"))
        if unrealized_pnl is not None:
            position_unrealized_pnl_usd += unrealized_pnl
            unrealized_pnl_observations += 1
        leverage = leverage_label(position.get("leverage"))
        if leverage != "unknown":
            leverage_by_coin[coin] = leverage
            leverage_counts[leverage] += 1
    return {
        "position_count": len(coins),
        "position_coins": sorted(coins),
        "position_leverage_by_coin": {
            coin: leverage_by_coin[coin] for coin in sorted(leverage_by_coin)
        },
        "position_leverage_counts": counter_dict(leverage_counts),
        "position_notional_usd": (
            round(position_notional_usd, 8) if notional_observations else None
        ),
        "position_notional_observations": notional_observations,
        "position_margin_used_usd": (
            round(position_margin_used_usd, 8) if margin_used_observations else None
        ),
        "position_margin_used_observations": margin_used_observations,
        "position_unrealized_pnl_usd": (
            round(position_unrealized_pnl_usd, 8) if unrealized_pnl_observations else None
        ),
        "position_unrealized_pnl_observations": unrealized_pnl_observations,
    }


def leverage_label(value: Any) -> str:
    if isinstance(value, dict):
        leverage_type = clean(value.get("type"))
        leverage_value = clean(value.get("value"))
        if leverage_type == "unknown":
            return leverage_value
        if leverage_value == "unknown":
            return leverage_type
        return f"{leverage_type}:{leverage_value}"
    return clean(value)


def fill_event(
    *,
    address: str,
    channel: str,
    subtype: str,
    fill: dict[str, Any],
    observed_ms: int,
    line_no: int,
    item_index: int,
    is_snapshot: bool,
) -> ReplayEvent:
    metadata = fill_metadata(fill, is_snapshot=is_snapshot)
    return make_event(
        address=address,
        kind="websocket",
        channel=channel,
        event_type="fill",
        subtype=subtype,
        event_ts_ms=event_time_ms(fill.get("time")),
        observed_ms=observed_ms,
        source_kind="event",
        line_no=line_no,
        item_index=item_index,
        metadata=metadata,
    )


def twap_slice_fill_event(
    *,
    address: str,
    channel: str,
    subtype: str,
    slice_fill: dict[str, Any],
    fill: dict[str, Any],
    observed_ms: int,
    line_no: int,
    item_index: int,
    is_snapshot: bool,
) -> ReplayEvent:
    metadata = fill_metadata(fill, is_snapshot=is_snapshot)
    metadata["twap_id"] = clean(slice_fill.get("twapId"))
    return make_event(
        address=address,
        kind="websocket",
        channel=channel,
        event_type="twap_slice_fill",
        subtype=subtype,
        event_ts_ms=event_time_ms(fill.get("time")),
        observed_ms=observed_ms,
        source_kind="event",
        line_no=line_no,
        item_index=item_index,
        metadata=metadata,
    )


def fill_metadata(fill: dict[str, Any], *, is_snapshot: bool) -> dict[str, Any]:
    price = parse_float(fill.get("px"))
    size = parse_float(fill.get("sz"))
    notional = price * size if price is not None and size is not None else None
    return {
        "is_snapshot": is_snapshot,
        "coin": clean(fill.get("coin")),
        "side": clean(fill.get("side")),
        "dir": clean(fill.get("dir")),
        "oid": clean(fill.get("oid")),
        "tid": clean(fill.get("tid")),
        "hash": clean(fill.get("hash")),
        "twap_id": clean(fill.get("twapId")),
        "px": clean(fill.get("px")),
        "sz": clean(fill.get("sz")),
        "notional_usd": notional,
        "closed_pnl_usd": parse_float(fill.get("closedPnl")),
        "fee_usd": parse_float(fill.get("fee")),
        "fee_token": clean(fill.get("feeToken")),
        "crossed": fill.get("crossed") if isinstance(fill.get("crossed"), bool) else None,
        "start_position": clean(fill.get("startPosition")),
    }


def order_update_metadata(order: dict[str, Any], *, status: str) -> dict[str, Any]:
    price = parse_float(order.get("limitPx"))
    size = parse_float(order.get("sz"))
    notional = price * size if price is not None and size is not None else None
    return {
        "status": status,
        "coin": clean(order.get("coin")),
        "side": clean(order.get("side")),
        "oid": clean(order.get("oid")),
        "cloid": clean(order.get("cloid")),
        "limit_px": clean(order.get("limitPx")),
        "sz": clean(order.get("sz")),
        "orig_sz": clean(order.get("origSz")),
        "notional_usd": notional,
        "reduce_only": order.get("reduceOnly")
        if isinstance(order.get("reduceOnly"), bool)
        else None,
        "order_type": clean(order.get("orderType")),
        "order_timestamp_ms": parse_int(order.get("timestamp")),
    }


def twap_history_event(
    *,
    address: str,
    channel: str,
    item: dict[str, Any],
    observed_ms: int,
    line_no: int,
    item_index: int,
    is_snapshot: bool,
) -> ReplayEvent:
    status = item.get("status")
    status_label = clean(status.get("status") if isinstance(status, dict) else status)
    raw_state = item.get("state")
    state: dict[str, Any] = raw_state if isinstance(raw_state, dict) else {}
    return make_event(
        address=address,
        kind="websocket",
        channel=channel,
        event_type="twap_history",
        subtype=f"twap_history:{status_label}",
        event_ts_ms=event_time_ms(item.get("time")),
        observed_ms=observed_ms,
        source_kind="event",
        line_no=line_no,
        item_index=item_index,
        metadata={
            "is_snapshot": is_snapshot,
            "twap_id": clean(item.get("twapId")),
            "status": status_label,
            "coin": clean(state.get("coin")),
            "side": clean(state.get("side")),
            "reduce_only": state.get("reduceOnly"),
            "state_timestamp_ms": event_time_ms(state.get("timestamp")),
        },
    )


def funding_event(
    *,
    address: str,
    channel: str,
    subtype: str,
    funding: dict[str, Any],
    observed_ms: int,
    line_no: int,
    item_index: int,
    is_snapshot: bool,
) -> ReplayEvent:
    return make_event(
        address=address,
        kind="websocket",
        channel=channel,
        event_type="funding",
        subtype=subtype,
        event_ts_ms=event_time_ms(funding.get("time")),
        observed_ms=observed_ms,
        source_kind="event",
        line_no=line_no,
        item_index=item_index,
        metadata={
            "is_snapshot": is_snapshot,
            "coin": clean(funding.get("coin")),
            "usdc": clean(funding.get("usdc")),
        },
    )


def make_event(
    *,
    address: str,
    kind: str,
    channel: str,
    event_type: str,
    subtype: str,
    event_ts_ms: int | None,
    observed_ms: int,
    source_kind: str,
    line_no: int,
    item_index: int,
    synthetic: bool = False,
    metadata: dict[str, Any] | None = None,
) -> ReplayEvent:
    timestamp_source = "exchange" if event_ts_ms is not None else "observed"
    sort_ts_ms = event_ts_ms if event_ts_ms is not None else observed_ms
    event_id_parts = [
        address,
        source_kind,
        str(line_no),
        str(item_index),
        channel,
        event_type,
        subtype,
        str(sort_ts_ms),
    ]
    if metadata:
        for key in ("coin", "oid", "tid", "hash", "twap_id", "status"):
            value = metadata.get(key)
            if value not in (None, "", "unknown"):
                event_id_parts.append(str(value))
    return ReplayEvent(
        address=address,
        kind=kind,
        channel=channel,
        event_type=event_type,
        subtype=subtype,
        sort_ts_ms=sort_ts_ms,
        timestamp_source=timestamp_source,
        observed_ms=observed_ms,
        source_kind=source_kind,
        line_no=line_no,
        item_index=item_index,
        event_id=":".join(event_id_parts),
        synthetic=synthetic,
        metadata=metadata,
    )


def make_observed_event(
    *,
    address: str,
    kind: str,
    channel: str,
    event_type: str,
    subtype: str,
    observed_ms: int,
    source_kind: str,
    line_no: int,
    item_index: int,
    synthetic: bool = False,
    metadata: dict[str, Any] | None = None,
) -> ReplayEvent:
    return make_event(
        address=address,
        kind=kind,
        channel=channel,
        event_type=event_type,
        subtype=subtype,
        event_ts_ms=None,
        observed_ms=observed_ms,
        source_kind=source_kind,
        line_no=line_no,
        item_index=item_index,
        synthetic=synthetic,
        metadata=metadata,
    )


def write_sorted_chunks(
    events: Iterable[ReplayEvent],
    *,
    chunk_dir: Path,
    chunk_size: int,
) -> tuple[list[Path], int]:
    chunk_paths: list[Path] = []
    chunk: list[dict[str, Any]] = []
    total = 0
    for event in events:
        chunk.append(event.as_dict())
        total += 1
        if len(chunk) >= chunk_size:
            chunk_paths.append(write_chunk(chunk, chunk_dir, len(chunk_paths)))
            chunk = []
    if chunk:
        chunk_paths.append(write_chunk(chunk, chunk_dir, len(chunk_paths)))
    return chunk_paths, total


def write_chunk(chunk: list[dict[str, Any]], chunk_dir: Path, index: int) -> Path:
    chunk.sort(key=sort_tuple)
    path = chunk_dir / f"chunk_{index:06d}.jsonl"
    with path.open("w", encoding="utf-8") as handle:
        for event in chunk:
            handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
            handle.write("\n")
    return path


def consume_replay(
    chunk_paths: list[Path],
    *,
    recording_dir: Path,
    addresses: list[str],
    total_normalized: int,
    limit_events: int | None,
    sample_size: int,
    events_out: Path | None,
    checkpoint_path: Path | None,
    resume_cursor: dict[str, Any] | None,
    checkpoint_every: int,
    options: dict[str, Any],
) -> dict[str, Any]:
    event_type_counts: Counter[str] = Counter()
    channel_counts: Counter[str] = Counter()
    subtype_counts: Counter[str] = Counter()
    timestamp_source_counts: Counter[str] = Counter()
    address_counts: Counter[str] = Counter()
    synthetic_counts: Counter[str] = Counter()
    recovery_decisions: list[dict[str, Any]] = []
    sample_events: list[dict[str, Any]] = []
    first_cursor: dict[str, Any] | None = None
    last_cursor: dict[str, Any] | None = None
    emitted = 0
    skipped_by_resume = 0
    resume_key = sort_tuple(resume_cursor) if resume_cursor else None

    event_handle = None
    if events_out is not None:
        events_out.parent.mkdir(parents=True, exist_ok=True)
        event_handle = events_out.open("w", encoding="utf-8")
    try:
        for event in merge_chunks(chunk_paths):
            if resume_key is not None and sort_tuple(event) <= resume_key:
                skipped_by_resume += 1
                continue
            if limit_events is not None and emitted >= limit_events:
                break
            emitted += 1
            cursor = cursor_from_event(event, emitted)
            first_cursor = first_cursor or cursor
            last_cursor = cursor
            event_type_counts[event["event_type"]] += 1
            channel_counts[event["channel"]] += 1
            subtype_counts[event["subtype"]] += 1
            timestamp_source_counts[event["timestamp_source"]] += 1
            address_counts[event["address"]] += 1
            if event.get("synthetic"):
                synthetic_counts[event["subtype"]] += 1
            if event.get("event_type") == "recovery":
                recovery_decisions.append(recovery_decision(event))
            if len(sample_events) < sample_size:
                sample_events.append(event)
            if event_handle is not None:
                event_handle.write(json.dumps(event, separators=(",", ":"), sort_keys=True))
                event_handle.write("\n")
            if (
                checkpoint_path is not None
                and checkpoint_every > 0
                and emitted % checkpoint_every == 0
            ):
                write_checkpoint(checkpoint_path, cursor)
        if checkpoint_path is not None and last_cursor is not None:
            write_checkpoint(checkpoint_path, last_cursor)
    finally:
        if event_handle is not None:
            event_handle.close()

    return {
        "replay_version": REPLAY_VERSION,
        "read_only": True,
        "exchange_touched": False,
        "recording_dir": str(recording_dir),
        "addresses": addresses,
        "options": options,
        "total_normalized_events": total_normalized,
        "emitted_events": emitted,
        "skipped_by_resume": skipped_by_resume,
        "truncated_by_limit": bool(limit_events is not None and emitted >= limit_events),
        "counts": {
            "by_event_type": counter_dict(event_type_counts),
            "by_channel": counter_dict(channel_counts),
            "by_subtype": counter_dict(subtype_counts),
            "by_timestamp_source": counter_dict(timestamp_source_counts),
            "by_address": counter_dict(address_counts),
            "synthetic": counter_dict(synthetic_counts),
        },
        "first_cursor": first_cursor,
        "last_cursor": last_cursor,
        "sample_events": sample_events,
        "recovery_decisions": recovery_decisions,
    }


def merge_chunks(chunk_paths: list[Path]) -> Iterator[dict[str, Any]]:
    handles = [path.open("r", encoding="utf-8") for path in chunk_paths]
    heap: list[tuple[tuple[Any, ...], int, dict[str, Any]]] = []
    try:
        for index, handle in enumerate(handles):
            event = read_event_line(handle)
            if event is not None:
                heapq.heappush(heap, (sort_tuple(event), index, event))
        while heap:
            _key, index, event = heapq.heappop(heap)
            yield event
            next_event = read_event_line(handles[index])
            if next_event is not None:
                heapq.heappush(heap, (sort_tuple(next_event), index, next_event))
    finally:
        for handle in handles:
            handle.close()


def read_event_line(handle: Any) -> dict[str, Any] | None:
    line = handle.readline()
    if not line:
        return None
    return json.loads(line)


def recovery_decision(event: dict[str, Any]) -> dict[str, Any]:
    raw_metadata = event.get("metadata")
    metadata: dict[str, Any] = raw_metadata if isinstance(raw_metadata, dict) else {}
    if event.get("subtype") == "stream_degraded":
        return {
            "address": event["address"],
            "event": "stream_degraded",
            "at_ms": event["sort_ts_ms"],
            "decision": "stop acting on live stream hints until REST backfill and reconcile complete",
            "source_error": metadata.get("source_error"),
        }
    if event.get("subtype") == "reconnect_recovered":
        return {
            "address": event["address"],
            "event": "reconnect_recovered",
            "at_ms": event["sort_ts_ms"],
            "gap_ms": metadata.get("gap_ms"),
            "decision": "run REST backfill for fills/TWAPs, refresh source and follower truth, then reconcile",
            "source_error": metadata.get("source_error"),
        }
    return {
        "address": event["address"],
        "event": clean(event.get("subtype")),
        "at_ms": event["sort_ts_ms"],
        "decision": "observe only",
    }


def cursor_from_event(event: dict[str, Any], emitted_count: int) -> dict[str, Any]:
    return {
        "emitted_count": emitted_count,
        "sort_key": event["sort_key"],
        "event_id": event["event_id"],
    }


def write_checkpoint(path: Path, cursor: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(cursor, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def read_checkpoint(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    if not path.exists():
        raise ReplayInputError(f"checkpoint does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("sort_key"), list):
        raise ReplayInputError(f"checkpoint is missing sort_key: {path}")
    return payload


def sort_tuple(event: dict[str, Any] | None) -> tuple[Any, ...]:
    if event is None:
        return ()
    sort_key = event.get("sort_key")
    if not isinstance(sort_key, list):
        raise ReplayInputError(f"event missing sort_key: {event!r}")
    return tuple(sort_key)


def iter_jsonl(path: Path, *, max_records: int | None) -> Iterator[tuple[int, dict[str, Any]]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            if max_records is not None and line_no > max_records:
                break
            text = line.strip()
            if not text:
                continue
            try:
                record = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ReplayInputError(f"{path}:{line_no} invalid JSON: {exc.msg}") from exc
            if not isinstance(record, dict):
                raise ReplayInputError(f"{path}:{line_no} JSONL record must be an object")
            yield line_no, record


def discover_addresses(recording_dir: Path, addresses: list[str] | None) -> list[str]:
    if not recording_dir.exists():
        raise ReplayInputError(f"recording directory does not exist: {recording_dir}")
    if addresses:
        normalized = [address.lower() for address in addresses]
        invalid = [address for address in normalized if not valid_address(address)]
        if invalid:
            raise ReplayInputError(f"invalid address filter: {invalid[0]}")
        return sorted(dict.fromkeys(normalized))
    manifest_path = recording_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(manifest, dict):
            manifest_addresses = [
                str(address).lower()
                for address in manifest.get("addresses", [])
                if valid_address(str(address).lower())
            ]
            if manifest_addresses:
                return sorted(manifest_addresses)
    event_dir = recording_dir / "events"
    if not event_dir.exists():
        raise ReplayInputError(f"recording has no events directory: {event_dir}")
    discovered = sorted(path.stem.lower() for path in event_dir.glob("*.jsonl"))
    return [address for address in discovered if valid_address(address)]


def required_address(record: dict[str, Any], path: Path, line_no: int) -> str:
    value = record.get("address")
    if not isinstance(value, str) or not valid_address(value.lower()):
        raise ReplayInputError(f"{path}:{line_no} address must be a 42-character hex string")
    return value.lower()


def required_int(record: dict[str, Any], key: str, path: Path, line_no: int) -> int:
    value = parse_int(record.get(key))
    if value is None:
        raise ReplayInputError(f"{path}:{line_no} {key} must be an integer")
    return value


def required_str(record: dict[str, Any], key: str, path: Path, line_no: int) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise ReplayInputError(f"{path}:{line_no} {key} must be a non-empty string")
    return value


def required_list(value: Any, label: str) -> list[Any]:
    return value if isinstance(value, list) else []


def first_list_field(data: dict[str, Any], keys: tuple[str, ...]) -> list[Any]:
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


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


def parse_float(value: Any) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def event_time_ms(value: Any) -> int | None:
    timestamp = parse_int(value)
    if timestamp is None:
        return None
    if 1_000_000_000 <= timestamp < 10_000_000_000:
        return timestamp * MS_PER_SECOND
    return timestamp


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
        description="Replay recorded Hyperliquid source events in deterministic read-only order."
    )
    parser.add_argument("recording_dir", type=Path)
    parser.add_argument(
        "--address", action="append", default=None, help="Source address to replay."
    )
    parser.add_argument("--out", type=Path, default=None, help="Write replay report JSON.")
    parser.add_argument(
        "--events-out", type=Path, default=None, help="Write emitted replay events JSONL."
    )
    parser.add_argument(
        "--checkpoint", type=Path, default=None, help="Write replay cursor checkpoint."
    )
    parser.add_argument(
        "--resume", type=Path, default=None, help="Resume after a checkpoint cursor."
    )
    parser.add_argument("--checkpoint-every", type=int, default=10_000)
    parser.add_argument("--limit-events", type=int, default=None)
    parser.add_argument("--max-records-per-file", type=int, default=None)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--tmp-dir", type=Path, default=None)
    parser.add_argument("--no-snapshots", action="store_true")
    parser.add_argument("--no-gap-injection", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        summary = replay_recording(
            args.recording_dir,
            addresses=args.address,
            max_records_per_file=args.max_records_per_file,
            limit_events=args.limit_events,
            include_snapshots=not args.no_snapshots,
            inject_gaps=not args.no_gap_injection,
            chunk_size=args.chunk_size,
            sample_size=args.sample_size,
            events_out=args.events_out,
            checkpoint_path=args.checkpoint,
            resume_checkpoint=args.resume,
            checkpoint_every=args.checkpoint_every,
            tmp_dir=args.tmp_dir,
        )
    except ReplayInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.out is not None:
        write_json(args.out, summary)
        print(
            json.dumps(
                {
                    "report": str(args.out),
                    "addresses": summary["addresses"],
                    "emitted_events": summary["emitted_events"],
                    "total_normalized_events": summary["total_normalized_events"],
                    "exchange_touched": summary["exchange_touched"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
