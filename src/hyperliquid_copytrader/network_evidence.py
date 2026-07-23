from __future__ import annotations

import hmac
import json
import math
import os
from collections.abc import Mapping
from hashlib import sha256
from typing import Any


NETWORK_EVIDENCE_VERSION = "windows-network-evidence-v4"
THROTTLE_MODE_WINDOWS_QOS = "verified_windows_qos_egress"
THROTTLE_MODE_EXTERNAL = "operator_attested_external"
THROTTLE_MODE_NONE = "no_verified_throttle"
THROTTLE_EVIDENCE_MODES = frozenset(
    {THROTTLE_MODE_WINDOWS_QOS, THROTTLE_MODE_EXTERNAL, THROTTLE_MODE_NONE}
)

EXTERNAL_THROTTLE_ATTESTATION_ENV = "HLCT_EXTERNAL_THROTTLE_ATTESTATION"
EXTERNAL_THROTTLE_ATTESTATION = "CURRENT CONNECTION IS EXTERNALLY THROTTLED"
EXTERNAL_THROTTLE_ATTESTATION_VERSION = "operator-external-throttle-v1"
FEED_CAPACITY_BASIS = "post_ack_live_market_gateway_queue_v1"
MIN_LIVE_RECEIVE_WINDOW_S = 60.0
# Local fail-closed release thresholds, calibrated by the 2026-07-17 host probe.
# They are qualification policy, not exchange cadence guarantees or API SLAs.
ACTIVE_CONTEXT_MAX_SILENCE_MS = 10_000.0
ALL_DEXS_CONTEXT_MAX_SILENCE_MS = 25_000.0
BOOK_MAX_SILENCE_MS = 10_000.0
RECEIVE_WINDOW_BASIS = "post_all_subscription_acks_local_monotonic"
LIVENESS_POLICY_VERSION = "post-ack-required-stream-liveness-v1"
SUBSCRIPTION_SETUP_TIMEOUT_S = 60.0
RUNTIME_FEED_LIVENESS_VERSION = "runtime-required-stream-liveness-v1"
RUNTIME_FEED_LIVENESS_POLICY_VERSION = "runtime-required-stream-silence-policy-v1"
RUNTIME_FEED_LIVENESS_SAMPLE_MAX_GAP_MS = 5_000.0


def external_throttle_attested(environ: Mapping[str, str] | None = None) -> bool:
    source = os.environ if environ is None else environ
    return hmac.compare_digest(
        str(source.get(EXTERNAL_THROTTLE_ATTESTATION_ENV) or ""),
        EXTERNAL_THROTTLE_ATTESTATION,
    )


def sha256_text(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    return sha256(
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def expected_throttle_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("throttle_evidence_mode") or "")
    return {
        "mode": mode,
        "network_path_identity_sha256": payload.get("network_path_identity_sha256"),
        "active_qos_policy_sha256": payload.get("active_qos_policy_sha256"),
        "target_throttle_rates_bps": (
            payload.get("target_throttle_rates_bps") if mode == THROTTLE_MODE_WINDOWS_QOS else []
        ),
        "external_throttle_attestation_version": (
            EXTERNAL_THROTTLE_ATTESTATION_VERSION if mode == THROTTLE_MODE_EXTERNAL else ""
        ),
    }


def qos_policy_sha256(policies: Any) -> str:
    if not isinstance(policies, list) or any(not isinstance(item, Mapping) for item in policies):
        return ""
    normalized = json.dumps(
        sorted(
            (dict(item) for item in policies),
            key=lambda item: json.dumps(item, sort_keys=True),
        ),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(normalized).hexdigest()


def _policy_throttle_rates(policies: Any) -> list[int] | None:
    if (
        not isinstance(policies, list)
        or not policies
        or any(not isinstance(item, Mapping) for item in policies)
    ):
        return None
    rates: set[int] = set()
    for item in policies:
        raw_rate = item.get("ThrottleRateActionBitsPerSecond")
        if isinstance(raw_rate, bool):
            return None
        try:
            rate = int(raw_rate)
        except (TypeError, ValueError, OverflowError):
            return None
        if rate <= 0:
            return None
        rates.add(rate)
    return sorted(rates)


def usable_throttle_evidence(payload: Mapping[str, Any]) -> bool:
    mode = str(payload.get("throttle_evidence_mode") or "")
    path_identity = payload.get("network_path_identity")
    throttle_identity = payload.get("throttle_evidence_identity")
    if (
        payload.get("methodology_version") != NETWORK_EVIDENCE_VERSION
        or payload.get("measured") is not True
        or payload.get("qos_query_succeeded") is not True
        or payload.get("qos_parse_succeeded") is not True
        or not isinstance(path_identity, Mapping)
        or canonical_sha256(path_identity) != payload.get("network_path_identity_sha256")
        or not isinstance(throttle_identity, Mapping)
        or dict(throttle_identity) != expected_throttle_identity(payload)
        or canonical_sha256(throttle_identity) != payload.get("throttle_evidence_identity_sha256")
    ):
        return False
    if mode == THROTTLE_MODE_WINDOWS_QOS:
        websocket = payload.get("websocket_transport")
        target_rates = payload.get("target_throttle_rates_bps")
        possible_rates = payload.get("possibly_applicable_throttle_rates_bps")
        policy_rates = _policy_throttle_rates(payload.get("target_qos_policies"))
        return bool(
            payload.get("active_qos_policy_present") is True
            and payload.get("target_qos_unambiguous") is True
            and isinstance(target_rates, list)
            and len(target_rates) == 1
            and isinstance(target_rates[0], int)
            and not isinstance(target_rates[0], bool)
            and target_rates[0] > 0
            and policy_rates == target_rates
            and possible_rates == target_rates
            and payload.get("frozen_throttled_byte_rate_bps") == target_rates[0] // 8
            and qos_policy_sha256(payload.get("target_qos_policies"))
            == payload.get("active_qos_policy_sha256")
            and payload.get("throttle_enforcement_queryable") is True
            and payload.get("external_throttle_attestation_supplied") is False
            and payload.get("external_throttle_attested") is False
            and payload.get("external_throttle_attestation_version") == ""
            and payload.get("inbound_feed_rate_cap_proven") is False
            and isinstance(websocket, Mapping)
            and websocket.get("proxy_configured") is False
        )
    if mode == THROTTLE_MODE_EXTERNAL:
        return bool(
            payload.get("external_throttle_attested") is True
            and payload.get("external_throttle_attestation_supplied") is True
            and payload.get("external_throttle_attestation_version")
            == EXTERNAL_THROTTLE_ATTESTATION_VERSION
            and payload.get("throttle_enforcement_queryable") is False
            and payload.get("active_qos_policy_present") is False
            and payload.get("target_qos_unambiguous") is False
            and payload.get("target_throttle_rates_bps") == []
            and not payload.get("possibly_applicable_throttle_rates_bps")
            and payload.get("target_qos_policies") == []
            and payload.get("frozen_throttled_byte_rate_bps") is None
            and payload.get("inbound_feed_rate_cap_proven") is False
            and qos_policy_sha256(payload.get("target_qos_policies"))
            == payload.get("active_qos_policy_sha256")
        )
    return False


def throttle_binding(payload: Mapping[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("throttle_evidence_mode") or "")
    return {
        "network_methodology_version": payload.get("methodology_version"),
        "throttle_evidence_mode": mode,
        "throttle_evidence_identity_sha256": payload.get("throttle_evidence_identity_sha256"),
        "network_path_identity_sha256": payload.get("network_path_identity_sha256"),
        "external_throttle_attestation_version": payload.get(
            "external_throttle_attestation_version"
        ),
        "throttle_enforcement_queryable": payload.get("throttle_enforcement_queryable"),
        "active_qos_policy_sha256": payload.get("active_qos_policy_sha256"),
        "frozen_throttled_byte_rate_bps": payload.get("frozen_throttled_byte_rate_bps"),
    }


def throttle_bindings_match(
    first: Mapping[str, Any],
    second: Mapping[str, Any],
) -> bool:
    return throttle_binding(first) == throttle_binding(second)


def observed_feed_is_bounded(strategy: Mapping[str, Any]) -> bool:
    exact_integer_fields = (
        "frames",
        "bytes",
        "queue_capacity",
        "queue_high_water",
        "overflow_count",
        "alignment_anomaly_count",
        "discarded_pre_ack_feed_frames",
        "discarded_pre_ack_feed_bytes",
        "received_relevant_frames",
        "subscription_ack_count",
        "subscription_count",
        "subscriptions",
    )
    if any(
        not isinstance(strategy.get(field), int) or isinstance(strategy.get(field), bool)
        for field in exact_integer_fields
    ):
        return False
    try:
        capacity = int(strategy.get("queue_capacity") or 0)
        high_water = int(strategy.get("queue_high_water") or 0)
        overflow = int(strategy.get("overflow_count") or 0)
        alignment_anomalies = int(strategy.get("alignment_anomaly_count") or 0)
        discarded_pre_ack_frames = int(strategy.get("discarded_pre_ack_feed_frames") or 0)
        discarded_pre_ack_bytes = int(strategy.get("discarded_pre_ack_feed_bytes") or 0)
        duration_s = float(strategy.get("duration_s") or 0)
        requested_window_s = float(strategy.get("requested_receive_window_s") or 0)
        observed_window_s = float(strategy.get("observed_receive_window_s") or 0)
        setup_duration_s = float(strategy.get("subscription_setup_duration_s") or 0)
        received_relevant_frames = int(strategy.get("received_relevant_frames") or 0)
    except (TypeError, ValueError, OverflowError):
        return False
    required_keys = strategy.get("required_stream_keys")
    liveness = strategy.get("required_stream_liveness")
    if (
        not isinstance(required_keys, list)
        or not required_keys
        or any(not isinstance(key, str) or not key for key in required_keys)
        or len(set(required_keys)) != len(required_keys)
        or not isinstance(liveness, Mapping)
        or set(liveness) != set(required_keys)
    ):
        return False
    strategy_name = str(strategy.get("strategy") or "")
    context_streams = 0
    book_streams = 0
    total_liveness_frames = 0
    for key in required_keys:
        if key == "allDexsAssetCtxs" and strategy_name == "continuous_all_dex_context":
            expected_limit_ms = ALL_DEXS_CONTEXT_MAX_SILENCE_MS
            context_streams += 1
        elif key.startswith("activeAssetCtx:") and strategy_name == "active_market_context":
            expected_limit_ms = ACTIVE_CONTEXT_MAX_SILENCE_MS
            context_streams += 1
        elif key.startswith("l2Book:"):
            expected_limit_ms = BOOK_MAX_SILENCE_MS
            book_streams += 1
        else:
            return False
        row = liveness.get(key)
        if not isinstance(row, Mapping):
            return False
        numeric_fields = (
            "first_delay_ms",
            "last_age_ms",
            "observed_span_ms",
            "max_silence_ms",
            "policy_limit_ms",
        )
        if any(
            not isinstance(row.get(field), (int, float)) or isinstance(row.get(field), bool)
            for field in numeric_fields
        ):
            return False
        try:
            count = int(row.get("count") or 0)
            first_delay_ms = float(row.get("first_delay_ms") or 0)
            last_age_ms = float(row.get("last_age_ms") or 0)
            observed_span_ms = float(row.get("observed_span_ms") or 0)
            maximum_silence_ms = float(row.get("max_silence_ms") or 0)
            policy_limit_ms = float(row.get("policy_limit_ms") or 0)
        except (TypeError, ValueError, OverflowError):
            return False
        if (
            not isinstance(row.get("count"), int)
            or isinstance(row.get("count"), bool)
            or count < 1
            or not all(
                math.isfinite(value)
                for value in (
                    first_delay_ms,
                    last_age_ms,
                    observed_span_ms,
                    maximum_silence_ms,
                    policy_limit_ms,
                )
            )
            or min(first_delay_ms, last_age_ms, observed_span_ms, maximum_silence_ms) < 0
            or policy_limit_ms != expected_limit_ms
            or first_delay_ms > maximum_silence_ms
            or last_age_ms > maximum_silence_ms
            or maximum_silence_ms > policy_limit_ms
            or observed_span_ms > observed_window_s * 1_000 + 1
            or (count == 1 and observed_span_ms > 1.0)
            or (count > 1 and maximum_silence_ms + 1.0 < observed_span_ms / (count - 1))
            or abs(first_delay_ms + observed_span_ms + last_age_ms - observed_window_s * 1_000)
            > 1.0
        ):
            return False
        total_liveness_frames += count
    return bool(
        strategy.get("complete") is True
        and strategy.get("context_complete") is True
        and strategy.get("books_complete") is True
        and strategy.get("capacity_basis") == FEED_CAPACITY_BASIS
        and strategy.get("receive_window_basis") == RECEIVE_WINDOW_BASIS
        and strategy.get("liveness_policy_version") == LIVENESS_POLICY_VERSION
        and strategy.get("subscription_setup_timeout_s") == SUBSCRIPTION_SETUP_TIMEOUT_S
        and strategy.get("subscriptions_acknowledged") is True
        and isinstance(strategy.get("subscription_ack_count"), int)
        and not isinstance(strategy.get("subscription_ack_count"), bool)
        and isinstance(strategy.get("subscription_count"), int)
        and not isinstance(strategy.get("subscription_count"), bool)
        and isinstance(strategy.get("subscriptions"), int)
        and not isinstance(strategy.get("subscriptions"), bool)
        and strategy.get("subscription_ack_count") == strategy.get("subscription_count")
        and strategy.get("subscription_count") == len(required_keys)
        and strategy.get("subscriptions") == len(required_keys)
        and isinstance(strategy.get("discarded_pre_ack_feed_frames"), int)
        and not isinstance(strategy.get("discarded_pre_ack_feed_frames"), bool)
        and isinstance(strategy.get("discarded_pre_ack_feed_bytes"), int)
        and not isinstance(strategy.get("discarded_pre_ack_feed_bytes"), bool)
        and discarded_pre_ack_frames >= 0
        and discarded_pre_ack_bytes >= 0
        and int(strategy["frames"]) > 0
        and int(strategy["bytes"]) > 0
        and isinstance(strategy.get("received_relevant_frames"), int)
        and not isinstance(strategy.get("received_relevant_frames"), bool)
        and received_relevant_frames == total_liveness_frames
        and received_relevant_frames >= len(required_keys)
        and context_streams >= 1
        and book_streams >= 1
        and isinstance(strategy.get("duration_s"), (int, float))
        and not isinstance(strategy.get("duration_s"), bool)
        and isinstance(strategy.get("requested_receive_window_s"), (int, float))
        and not isinstance(strategy.get("requested_receive_window_s"), bool)
        and isinstance(strategy.get("observed_receive_window_s"), (int, float))
        and not isinstance(strategy.get("observed_receive_window_s"), bool)
        and isinstance(strategy.get("subscription_setup_duration_s"), (int, float))
        and not isinstance(strategy.get("subscription_setup_duration_s"), bool)
        and all(
            math.isfinite(value)
            for value in (
                duration_s,
                requested_window_s,
                observed_window_s,
                setup_duration_s,
            )
        )
        and setup_duration_s >= 0
        and requested_window_s >= MIN_LIVE_RECEIVE_WINDOW_S
        and observed_window_s >= requested_window_s
        and duration_s >= requested_window_s
        and abs(duration_s - observed_window_s) <= 0.25
        and overflow == 0
        and alignment_anomalies == 0
        and capacity > 0
        and 0 <= high_water < 0.8 * capacity
    )
