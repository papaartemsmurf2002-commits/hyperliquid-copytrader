from __future__ import annotations

import os
from decimal import Decimal

import pytest

from hyperliquid_copytrader.config import AppConfig, ExchangeConfig, OpsConfig, RiskConfig
from hyperliquid_copytrader.models import Mode
from hyperliquid_copytrader.persistence import SQLiteStore


@pytest.fixture(autouse=True)
def clean_inherited_hlct_environment(monkeypatch):
    """Keep a real local .env/profile from changing test semantics."""

    for name in tuple(os.environ):
        if name.startswith("HLCT_"):
            monkeypatch.delenv(name, raising=False)


@pytest.fixture()
def store(tmp_path):
    db = SQLiteStore(tmp_path / "copytrader.sqlite3")
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def base_config(tmp_path):
    return AppConfig(
        mode=Mode.SHADOW,
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        db_path=tmp_path / "copytrader.sqlite3",
        risk=RiskConfig(
            allowed_symbols=("BTC", "ETH", "SOL"),
            fixed_multiplier=Decimal("0.10"),
            max_notional_usd=Decimal("250"),
            max_leverage=3,
            min_order_size=Decimal("0.00001"),
            slippage_bps=Decimal("25"),
        ),
        exchange=ExchangeConfig(),
        ops=OpsConfig(
            kill_switch_path=tmp_path / "KILL_SWITCH",
            runtime_lock_dir=tmp_path / "runtime-locks",
        ),
    )
