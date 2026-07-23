from __future__ import annotations

from typing import Any

from hyperliquid_copytrader.network_evidence import (
    ACTIVE_CONTEXT_MAX_SILENCE_MS,
    ALL_DEXS_CONTEXT_MAX_SILENCE_MS,
    BOOK_MAX_SILENCE_MS,
    EXTERNAL_THROTTLE_ATTESTATION_VERSION,
    FEED_CAPACITY_BASIS,
    LIVENESS_POLICY_VERSION,
    NETWORK_EVIDENCE_VERSION,
    THROTTLE_MODE_EXTERNAL,
    THROTTLE_MODE_WINDOWS_QOS,
    canonical_sha256,
    expected_throttle_identity,
    qos_policy_sha256,
    RECEIVE_WINDOW_BASIS,
    SUBSCRIPTION_SETUP_TIMEOUT_S,
    throttle_binding,
)
from hyperliquid_copytrader.stream_gateway import (
    CONTEXT_STRATEGY_ALL_DEXS,
)


def network_evidence(
    *,
    external: bool = False,
    qos_sha256: str | None = None,
    measured: bool = True,
) -> dict[str, Any]:
    mode = THROTTLE_MODE_EXTERNAL if external else THROTTLE_MODE_WINDOWS_QOS
    path_identity = {
        "target_host": "api.hyperliquid.xyz",
        "target_port": 443,
        "python_executable": r"C:\Python\python.exe",
        "active_route_interface_indexes": [6],
        "active_route_profiles": ["private"],
        "local_addresses": ["198.18.0.1"],
        "websocket_policy": "ipv6_preferred_happy_eyeballs_v1",
        "websocket_origin_resolution": "local_dns",
        "websocket_proxy_configured": False,
    }
    target_policies = (
        []
        if external
        else [
            {
                "Name": "HLCT-Pilot-0",
                "AppPathNameMatchCondition": r"C:\Python\python.exe",
                "IPDstPrefixMatchCondition": "192.0.2.1/32",
                "IPDstPortMatchCondition": "443",
                "IPProtocolMatchCondition": "TCP",
                "NetworkProfile": "Private",
                "ThrottleRateActionBitsPerSecond": 8_000,
            }
        ]
    )
    active_qos_sha256 = qos_sha256 or qos_policy_sha256(target_policies)
    payload: dict[str, Any] = {
        "methodology_version": NETWORK_EVIDENCE_VERSION,
        "measured": measured,
        "qos_query_succeeded": True,
        "qos_parse_succeeded": True,
        "active_qos_policy_present": not external,
        "active_qos_policy_sha256": active_qos_sha256,
        "target_qos_unambiguous": not external,
        "target_throttle_rates_bps": [] if external else [8_000],
        "target_qos_policies": target_policies,
        "possibly_applicable_throttle_rates_bps": [] if external else [8_000],
        "frozen_throttled_byte_rate_bps": None if external else 1_000,
        "throttle_evidence_mode": mode,
        "network_path_identity": path_identity,
        "network_path_identity_sha256": canonical_sha256(path_identity),
        "external_throttle_attested": external,
        "external_throttle_attestation_supplied": external,
        "external_throttle_attestation_version": (
            EXTERNAL_THROTTLE_ATTESTATION_VERSION if external else ""
        ),
        "throttle_enforcement_queryable": not external,
        "inbound_feed_rate_cap_proven": False,
        "websocket_transport": {
            "policy": "ipv6_preferred_happy_eyeballs_v1",
            "proxy_configured": False,
            "origin_resolution": "local_dns",
            "ipv6_available": False,
            "selected_family": "ipv4",
            "ipv4_fallback_used": False,
        },
        "handshake_samples": [{}, {}, {}],
        "websocket_ping_samples": [{}, {}, {}],
        "packet_loss_evidence": {"attempts": 9, "successes": 9},
    }
    identity = expected_throttle_identity(payload)
    payload["throttle_evidence_identity"] = identity
    payload["throttle_evidence_identity_sha256"] = canonical_sha256(identity)
    return payload


def context_network_fields(network: dict[str, Any]) -> dict[str, Any]:
    qualification_keys = (
        "methodology_version",
        "measured",
        "qos_query_succeeded",
        "qos_parse_succeeded",
        "active_qos_policy_present",
        "active_qos_policy_sha256",
        "target_qos_unambiguous",
        "target_qos_policies",
        "target_throttle_rates_bps",
        "possibly_applicable_throttle_rates_bps",
        "frozen_throttled_byte_rate_bps",
        "throttle_evidence_mode",
        "throttle_evidence_identity",
        "throttle_evidence_identity_sha256",
        "network_path_identity",
        "network_path_identity_sha256",
        "external_throttle_attested",
        "external_throttle_attestation_supplied",
        "external_throttle_attestation_version",
        "throttle_enforcement_queryable",
        "inbound_feed_rate_cap_proven",
        "websocket_transport",
    )
    qualification = {key: network.get(key) for key in qualification_keys}
    return {
        "frozen_throttled_byte_rate_bps": network.get("frozen_throttled_byte_rate_bps"),
        "active_qos_policy_sha256": network.get("active_qos_policy_sha256"),
        "network_methodology_version": network.get("methodology_version"),
        "target_qos_unambiguous": network.get("target_qos_unambiguous"),
        "active_qos_policy_present": network.get("active_qos_policy_present"),
        "possibly_applicable_throttle_rates_bps": network.get(
            "possibly_applicable_throttle_rates_bps"
        ),
        "throttle_evidence_mode": network.get("throttle_evidence_mode"),
        "throttle_evidence_identity_sha256": network.get("throttle_evidence_identity_sha256"),
        "network_path_identity_sha256": network.get("network_path_identity_sha256"),
        "external_throttle_attested": network.get("external_throttle_attested"),
        "external_throttle_attestation_supplied": network.get(
            "external_throttle_attestation_supplied"
        ),
        "external_throttle_attestation_version": network.get(
            "external_throttle_attestation_version"
        ),
        "throttle_enforcement_queryable": network.get("throttle_enforcement_queryable"),
        "websocket_transport": network.get("websocket_transport"),
        "network_qualification": qualification,
        "throttle_binding": throttle_binding(network),
        "feed_capacity_basis": FEED_CAPACITY_BASIS,
        "inbound_feed_rate_cap_proven": False,
    }


def bounded_strategy(
    *,
    strategy: str = CONTEXT_STRATEGY_ALL_DEXS,
    active_markets: list[str] | tuple[str, ...] = ("MARKET-0",),
    **overrides: Any,
) -> dict[str, Any]:
    context_keys = (
        ["allDexsAssetCtxs"]
        if strategy == CONTEXT_STRATEGY_ALL_DEXS
        else [f"activeAssetCtx:{coin}" for coin in active_markets]
    )
    required_keys = sorted(context_keys + [f"l2Book:{coin}" for coin in active_markets])
    liveness = {}
    for key in required_keys:
        if key == "allDexsAssetCtxs":
            limit = ALL_DEXS_CONTEXT_MAX_SILENCE_MS
        elif key.startswith("activeAssetCtx:"):
            limit = ACTIVE_CONTEXT_MAX_SILENCE_MS
        else:
            limit = BOOK_MAX_SILENCE_MS
        liveness[key] = {
            "count": 61,
            "first_delay_ms": 10.0,
            "last_age_ms": 10.0,
            "observed_span_ms": 59_980.0,
            "max_silence_ms": 1_000.0,
            "policy_limit_ms": limit,
        }
    payload: dict[str, Any] = {
        "strategy": strategy,
        "duration_s": 60.0,
        "subscriptions": len(required_keys),
        "frames": 10,
        "received_relevant_frames": len(required_keys) * 61,
        "bytes": 1_000,
        "complete": True,
        "context_complete": True,
        "books_complete": True,
        "bounded": True,
        "input_byte_rate_bps": 100.0,
        "parse": {"p99_ms": 2.0},
        "queue_capacity": 4_096,
        "queue_high_water": 16,
        "overflow_count": 0,
        "alignment_anomaly_count": 0,
        "capacity_basis": FEED_CAPACITY_BASIS,
        "receive_window_basis": RECEIVE_WINDOW_BASIS,
        "requested_receive_window_s": 60.0,
        "observed_receive_window_s": 60.0,
        "subscription_setup_duration_s": 1.0,
        "subscription_setup_timeout_s": SUBSCRIPTION_SETUP_TIMEOUT_S,
        "subscriptions_acknowledged": True,
        "subscription_count": len(required_keys),
        "subscription_ack_count": len(required_keys),
        "discarded_pre_ack_feed_frames": 0,
        "discarded_pre_ack_feed_bytes": 0,
        "liveness_policy_version": LIVENESS_POLICY_VERSION,
        "required_stream_keys": required_keys,
        "required_stream_liveness": liveness,
        "observed_markets": (list(active_markets) if strategy != CONTEXT_STRATEGY_ALL_DEXS else []),
        "observed_books": list(active_markets),
        "observed_dexes": [],
        "declared_egress_qos_byte_rate_bps": 1_000,
        "inbound_feed_rate_cap_proven": False,
    }
    payload.update(overrides)
    return payload
