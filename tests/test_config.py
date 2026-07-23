from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from hyperliquid_copytrader.config import (
    OpsConfig,
    load_config,
    reviewed_fleet_runtime_policy_errors,
)


SOURCE_WALLET = "0xcf7c4feb434751146a48b895e96caeb15838f92c"


def test_load_config_parses_optional_initial_margin_utilization(monkeypatch):
    monkeypatch.setenv("HLCT_SOURCE_WALLET", SOURCE_WALLET)

    assert load_config().risk.max_initial_margin_utilization is None

    monkeypatch.setenv("HLCT_MAX_INITIAL_MARGIN_UTILIZATION", "0.75")
    config = load_config()

    assert config.risk.max_initial_margin_utilization == Decimal("0.75")
    assert config.config_errors == ()


@pytest.mark.parametrize("raw", ["0", "-0.1", "1.01", "NaN", "Infinity", "bad"])
def test_load_config_rejects_invalid_initial_margin_utilization(monkeypatch, raw):
    monkeypatch.setenv("HLCT_SOURCE_WALLET", SOURCE_WALLET)
    monkeypatch.setenv("HLCT_MAX_INITIAL_MARGIN_UTILIZATION", raw)

    config = load_config()

    assert (
        "HLCT_MAX_INITIAL_MARGIN_UTILIZATION must be a finite decimal above 0 and at or below 1"
        in config.config_errors
    )


def test_reviewed_fleet_runtime_policy_accepts_defaults_and_rejects_drift() -> None:
    assert reviewed_fleet_runtime_policy_errors(OpsConfig()) == ()

    errors = reviewed_fleet_runtime_policy_errors(
        replace(OpsConfig(), deferred_scheduler_bound_ms=101)
    )

    assert errors == (
        "fleet runtime policy scheduler_bound_ms must equal reviewed value 100, got 101",
    )


def test_fast_config_records_reviewed_policy_drift(monkeypatch) -> None:
    monkeypatch.setenv("HLCT_SOURCE_WALLET", SOURCE_WALLET)
    monkeypatch.setenv("HLCT_FAST_EXECUTION_ENABLE", "true")
    monkeypatch.setenv("HLCT_DEFERRED_SCHEDULER_BOUND_MS", "101")

    config = load_config()

    assert any("fleet runtime policy scheduler_bound_ms" in error for error in config.config_errors)
