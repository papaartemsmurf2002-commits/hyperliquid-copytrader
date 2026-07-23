from __future__ import annotations

from copy import deepcopy

import pytest

from hyperliquid_copytrader.network_evidence import (
    EXTERNAL_THROTTLE_ATTESTATION,
    EXTERNAL_THROTTLE_ATTESTATION_ENV,
    canonical_sha256,
    expected_throttle_identity,
    external_throttle_attested,
    observed_feed_is_bounded,
    qos_policy_sha256,
    usable_throttle_evidence,
)
from hyperliquid_copytrader.stream_gateway import CONTEXT_STRATEGY_ACTIVE_MARKETS

from .network_fixtures import bounded_strategy, network_evidence


def _rebind(payload: dict[str, object]) -> None:
    path_identity = payload["network_path_identity"]
    assert isinstance(path_identity, dict)
    payload["network_path_identity_sha256"] = canonical_sha256(path_identity)
    payload["active_qos_policy_sha256"] = qos_policy_sha256(payload["target_qos_policies"])
    identity = expected_throttle_identity(payload)
    payload["throttle_evidence_identity"] = identity
    payload["throttle_evidence_identity_sha256"] = canonical_sha256(identity)


def test_external_attestation_is_exact_and_external_evidence_is_usable() -> None:
    assert external_throttle_attested(
        {EXTERNAL_THROTTLE_ATTESTATION_ENV: EXTERNAL_THROTTLE_ATTESTATION}
    )
    assert not external_throttle_attested(
        {EXTERNAL_THROTTLE_ATTESTATION_ENV: EXTERNAL_THROTTLE_ATTESTATION + " "}
    )
    assert usable_throttle_evidence(network_evidence(external=True))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("external_throttle_attestation_supplied", False),
        ("external_throttle_attested", False),
        ("frozen_throttled_byte_rate_bps", 0),
        ("target_throttle_rates_bps", [8_000]),
        ("possibly_applicable_throttle_rates_bps", [8_000]),
    ],
)
def test_external_evidence_rejects_inconsistent_claims(field: str, value: object) -> None:
    payload = network_evidence(external=True)
    payload[field] = value
    _rebind(payload)
    assert not usable_throttle_evidence(payload)


def test_external_evidence_rejects_hidden_qos_policy_even_when_rehashed() -> None:
    payload = network_evidence(external=True)
    payload["target_qos_policies"] = [{"Name": "hidden", "ThrottleRateActionBitsPerSecond": 999}]
    _rebind(payload)
    assert not usable_throttle_evidence(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("possibly_applicable_throttle_rates_bps", [16_000]),
        ("frozen_throttled_byte_rate_bps", 999),
    ],
)
def test_windows_qos_evidence_rejects_parallel_field_inconsistency(
    field: str,
    value: object,
) -> None:
    payload = network_evidence()
    payload[field] = value
    _rebind(payload)
    assert not usable_throttle_evidence(payload)


def test_windows_qos_rate_is_derived_from_policy_rows() -> None:
    payload = network_evidence()
    policies = payload["target_qos_policies"]
    assert isinstance(policies, list)
    policies[0]["ThrottleRateActionBitsPerSecond"] = 16_000
    _rebind(payload)
    assert not usable_throttle_evidence(payload)


def test_windows_qos_evidence_rejects_proxy_owned_origin() -> None:
    payload = network_evidence()
    websocket = payload["websocket_transport"]
    path = payload["network_path_identity"]
    assert isinstance(websocket, dict)
    assert isinstance(path, dict)
    websocket["proxy_configured"] = True
    path["websocket_proxy_configured"] = True
    _rebind(payload)
    assert not usable_throttle_evidence(payload)


def test_evidence_rejects_path_or_throttle_identity_tampering() -> None:
    path_tampered = network_evidence()
    path = path_tampered["network_path_identity"]
    assert isinstance(path, dict)
    path["local_addresses"] = ["203.0.113.7"]
    assert not usable_throttle_evidence(path_tampered)

    throttle_tampered = network_evidence()
    identity = throttle_tampered["throttle_evidence_identity"]
    assert isinstance(identity, dict)
    identity["mode"] = "forged"
    assert not usable_throttle_evidence(throttle_tampered)


def test_required_stream_liveness_rejects_one_stalled_market() -> None:
    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC", "ETH"],
    )
    assert observed_feed_is_bounded(strategy)
    stalled = deepcopy(strategy)
    stalled_row = stalled["required_stream_liveness"]["l2Book:ETH"]
    stalled_row["observed_span_ms"] = 48_990.0
    stalled_row["last_age_ms"] = 11_000.0
    stalled_row["max_silence_ms"] = 11_000.0
    assert not observed_feed_is_bounded(stalled)

    interior_stall = deepcopy(strategy)
    interior_stall["required_stream_liveness"]["l2Book:ETH"]["max_silence_ms"] = 11_000.0
    assert not observed_feed_is_bounded(interior_stall)


def test_required_stream_liveness_rejects_forged_zero_gap_row() -> None:
    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC"],
    )
    row = strategy["required_stream_liveness"]["activeAssetCtx:BTC"]
    row.update(
        {
            "count": 1,
            "first_delay_ms": 0.0,
            "last_age_ms": 0.0,
            "observed_span_ms": 0.0,
            "max_silence_ms": 0.0,
        }
    )
    assert not observed_feed_is_bounded(strategy)


def test_required_stream_liveness_rejects_impossible_gap_distribution() -> None:
    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC"],
    )
    row = strategy["required_stream_liveness"]["activeAssetCtx:BTC"]
    row["count"] = 60
    row["max_silence_ms"] = 1_000.0
    assert not observed_feed_is_bounded(strategy)


def test_required_stream_liveness_rejects_aggregate_count_mismatch() -> None:
    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC"],
    )
    strategy["received_relevant_frames"] -= 1
    assert not observed_feed_is_bounded(strategy)


def test_required_stream_liveness_rejects_numeric_type_coercion() -> None:
    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC"],
    )
    strategy["queue_capacity"] = 4_096.0
    assert not observed_feed_is_bounded(strategy)

    strategy = bounded_strategy(
        strategy=CONTEXT_STRATEGY_ACTIVE_MARKETS,
        active_markets=["BTC"],
    )
    strategy["required_stream_liveness"]["activeAssetCtx:BTC"]["first_delay_ms"] = "10.0"
    assert not observed_feed_is_bounded(strategy)
