from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from hyperliquid_copytrader.copy_engine import AssetMeta, CopyEngine
from hyperliquid_copytrader.models import IntentAction, Mode, Position
from hyperliquid_copytrader.precision import quantize_size


def test_copy_engine_caps_and_generates_deterministic_intent(base_config):
    engine = CopyEngine(base_config.risk, Mode.SHADOW, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-1",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )
    assert result.blockers == []
    assert result.desired_state.positions["BTC"].size == Decimal("0.005")
    assert len(result.intents) == 1
    assert result.intents[0].action == IntentAction.OPEN
    again = engine.plan(
        source_event_key="source-1",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )
    assert result.intents[0].cloid == again.intents[0].cloid


def test_copy_engine_does_not_invent_leverage_when_source_omits_it(base_config):
    engine = CopyEngine(base_config.risk, Mode.SHADOW, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-no-lev",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=None)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )
    assert result.blockers == []
    assert result.desired_state.positions["BTC"].leverage is None


def test_copy_engine_preserves_canonical_hip3_market_identity(base_config):
    risk = replace(base_config.risk, allowed_symbols=("xyz:aapl",))
    engine = CopyEngine(risk, Mode.SHADOW, follower_account="0xf0")

    result = engine.plan(
        source_event_key="source-hip3",
        source_positions={"xyz:AAPL": Position("xyz:aapl", Decimal("1"), leverage=3)},
        follower_positions={},
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3, max_leverage=20)},
        mids={"xyz:AAPL": Decimal("300")},
    )

    assert result.blockers == []
    assert set(result.desired_state.positions) == {"xyz:AAPL"}
    assert result.desired_state.positions["xyz:AAPL"].coin == "xyz:AAPL"
    assert result.intents[0].coin == "xyz:AAPL"


@pytest.mark.parametrize(
    ("source_size", "oracle_reference", "entry_price", "expected_wire_size"),
    [
        (Decimal("1"), Decimal("100"), Decimal("100.3"), Decimal("0.148")),
        (Decimal("1"), Decimal("100"), Decimal("101"), Decimal("0.148")),
        (Decimal("1"), Decimal("101"), Decimal("102.01"), Decimal("0.147")),
        (Decimal("-1"), Decimal("101"), Decimal("102.01"), Decimal("-0.147")),
    ],
)
def test_copy_engine_reserves_hip3_envelope_below_hard_order_cap(
    base_config, source_size, oracle_reference, entry_price, expected_wire_size
):
    risk = replace(
        base_config.risk,
        allowed_symbols=("xyz:CAP",),
        balance_sizing_enabled=False,
        fixed_multiplier=Decimal("1"),
        max_notional_usd=Decimal("15"),
        max_gross_exposure_usd=Decimal("40"),
        hip3_oracle_envelope_bps=Decimal("100"),
    )

    result = CopyEngine(risk, Mode.LIVE, follower_account="0xf0").plan(
        source_event_key="source-cap-bound-hip3-buy",
        source_positions={"xyz:CAP": Position("xyz:CAP", source_size, leverage=1)},
        follower_positions={},
        asset_meta={"xyz:CAP": AssetMeta("xyz:CAP", 3, max_leverage=20)},
        mids={"xyz:CAP": oracle_reference},
    )

    assert result.blockers == []
    exact_target = (
        risk.max_notional_usd
        / (oracle_reference * (Decimal("1") + risk.hip3_oracle_envelope_bps / Decimal("10000")))
    ).copy_sign(source_size)
    assert result.desired_state.positions["xyz:CAP"].size == exact_target
    assert result.intents[0].size == abs(expected_wire_size)
    assert quantize_size(exact_target, 3) == expected_wire_size
    assert result.intents[0].side == ("buy" if source_size > 0 else "sell")
    assert abs(result.intents[0].size * entry_price) <= risk.max_notional_usd
    assert abs(result.intents[0].size * oracle_reference) >= Decimal("10")


def test_copy_engine_automatically_scales_to_follower_account_value(base_config):
    engine = CopyEngine(base_config.risk, Mode.TESTNET, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-balanced",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
        source_account_value=Decimal("100000"),
        follower_account_value=Decimal("1000"),
    )

    assert result.blockers == []
    assert result.sizing["mode"] == "balance_scaled"
    assert result.sizing["effective_scale"] == Decimal("0.0010")
    assert result.desired_state.positions["BTC"].size == Decimal("0.00100")


def test_balance_scaled_075_policy_preserves_wide_mixed_leverage_portfolio(base_config):
    mids = {
        "BTC": Decimal("50000"),
        "ETH": Decimal("2500"),
        "SOL": Decimal("100"),
        "DOGE": Decimal("0.2"),
        "xyz:AAPL": Decimal("200"),
    }
    source_positions = {
        "BTC": Position("BTC", Decimal("40"), leverage=40),
        "ETH": Position("ETH", Decimal("160"), leverage=20),
        "SOL": Position("SOL", Decimal("2000"), leverage=10),
        "DOGE": Position("DOGE", Decimal("-400000"), leverage=5),
        "xyz:AAPL": Position("xyz:AAPL", Decimal("200"), leverage=3),
    }
    risk = replace(
        base_config.risk,
        allowed_symbols=tuple(source_positions),
        fixed_multiplier=Decimal("0.75"),
        max_initial_margin_utilization=Decimal("0.75"),
        max_balance_scale=Decimal("1000000"),
        max_notional_usd=Decimal("1000000"),
        max_gross_exposure_usd=Decimal("1000000"),
        max_leverage=50,
    )
    engine = CopyEngine(risk, Mode.LIVE)
    asset_meta = {
        coin: AssetMeta(coin, 5 if coin == "BTC" else 3, max_leverage=50)
        for coin in source_positions
    }

    result = engine.plan(
        source_event_key="proportional-075-wide",
        source_positions=source_positions,
        follower_positions={},
        asset_meta=asset_meta,
        mids=mids,
        source_account_value=Decimal("100000"),
        follower_account_value=Decimal("50"),
    )

    assert result.blockers == []
    assert result.sizing["effective_scale"] == Decimal("0.000375")
    assert result.sizing["initial_margin_budget_usd"] == Decimal("37.50")
    assert result.sizing["initial_margin_before_cap"] == Decimal("44.7500000")
    assert result.sizing["initial_margin_budget_status"] == "scaled"
    margin_scale = result.sizing["initial_margin_cap_scale"]
    assert margin_scale == Decimal("37.50") / Decimal("44.7500000")
    assert result.sizing["initial_margin_after_cap"] <= Decimal("37.50")
    assert set(result.desired_state.positions) == set(source_positions)
    for coin, source in source_positions.items():
        expected_size = source.size * Decimal("0.000375") * margin_scale
        assert result.desired_state.positions[coin].size == expected_size
        assert result.desired_state.positions[coin].side == source.side
        assert result.desired_state.positions[coin].leverage == source.leverage
        wire = next(intent for intent in result.intents if intent.coin == coin)
        assert wire.size == abs(quantize_size(expected_size, asset_meta[coin].sz_decimals))

    unconstrained = engine.plan(
        source_event_key="proportional-075-two-million-btc",
        source_positions={"BTC": source_positions["BTC"]},
        follower_positions={},
        asset_meta={"BTC": asset_meta["BTC"]},
        mids={"BTC": mids["BTC"]},
        source_account_value=Decimal("100000"),
        follower_account_value=Decimal("50"),
    )
    assert unconstrained.blockers == []
    assert unconstrained.sizing["initial_margin_budget_status"] == "within_budget"
    assert unconstrained.sizing["initial_margin_cap_scale"] is None
    assert unconstrained.desired_state.positions["BTC"].size * mids["BTC"] == Decimal("750")
    assert unconstrained.sizing["initial_margin_after_cap"] == Decimal("18.75000")


def test_initial_margin_budget_fails_closed_without_equity_or_leverage(base_config):
    risk = replace(
        base_config.risk,
        equity_ratio=Decimal("0.001"),
        max_initial_margin_utilization=Decimal("0.75"),
        max_notional_usd=Decimal("1000000"),
    )
    engine = CopyEngine(risk, Mode.LIVE)
    inputs = {
        "source_event_key": "invalid-margin-input",
        "follower_positions": {},
        "asset_meta": {"BTC": AssetMeta("BTC", 5, max_leverage=50)},
        "mids": {"BTC": Decimal("50000")},
        "source_account_value": Decimal("100000"),
    }

    for invalid_equity in (
        None,
        Decimal("0"),
        Decimal("NaN"),
        Decimal("Infinity"),
    ):
        missing_equity = engine.plan(
            source_positions={"BTC": Position("BTC", Decimal("1"), leverage=20)},
            follower_account_value=invalid_equity,
            **inputs,
        )
        assert missing_equity.desired_state.positions == {}
        assert missing_equity.sizing["initial_margin_budget_status"] == "blocked_invalid_equity"
        assert missing_equity.blockers == [
            "initial-margin budget requires a finite positive follower accountValue snapshot"
        ]

    missing_leverage = engine.plan(
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=None)},
        follower_account_value=Decimal("50"),
        **inputs,
    )
    zero_leverage = engine.plan(
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=0)},
        follower_account_value=Decimal("50"),
        **inputs,
    )

    assert missing_leverage.desired_state.positions == {}
    assert missing_leverage.sizing["initial_margin_budget_status"] == "blocked_invalid_leverage"
    assert missing_leverage.blockers == [
        "BTC target leverage is required for initial-margin budgeting"
    ]
    assert zero_leverage.desired_state.positions == {}
    assert zero_leverage.blockers == ["BTC source leverage 0 is invalid"]


def test_copy_engine_caps_runtime_sizing_equity(base_config):
    risk = replace(
        base_config.risk,
        fixed_multiplier=Decimal("1"),
        sizing_equity_cap_usd=Decimal("50"),
    )
    result = CopyEngine(risk, Mode.TESTNET, follower_account="0xf0").plan(
        source_event_key="source-fixed-risk",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 4)},
        mids={"BTC": Decimal("1000")},
        source_account_value=Decimal("1000"),
        follower_account_value=Decimal("250"),
    )

    assert result.blockers == []
    assert result.sizing["sizing_equity_cap_usd"] == Decimal("50")
    assert result.sizing["sizing_equity_usd"] == Decimal("50")
    assert result.sizing["raw_balance_scale"] == Decimal("0.05")
    assert result.sizing["effective_scale"] == Decimal("0.05")
    assert result.desired_state.positions["BTC"].size == Decimal("0.0500")

    below_cap = CopyEngine(risk, Mode.TESTNET, follower_account="0xf0").plan(
        source_event_key="source-fixed-risk-below-cap",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 4)},
        mids={"BTC": Decimal("1000")},
        source_account_value=Decimal("1000"),
        follower_account_value=Decimal("25"),
    )
    assert below_cap.sizing["sizing_equity_usd"] == Decimal("25")
    assert below_cap.sizing["effective_scale"] == Decimal("0.025")


def test_copy_engine_blocks_invalid_runtime_sizing_equity_cap(base_config):
    for cap in (Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")):
        risk = replace(base_config.risk, sizing_equity_cap_usd=cap)
        result = CopyEngine(risk, Mode.TESTNET).plan(
            source_event_key="source-invalid-fixed-risk",
            source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
            follower_positions={},
            asset_meta={"BTC": AssetMeta("BTC", 4)},
            mids={"BTC": Decimal("1000")},
            source_account_value=Decimal("1000"),
            follower_account_value=Decimal("250"),
        )

        assert result.sizing["mode"] == "blocked_invalid_sizing"
        assert result.intents == []
        assert result.blockers == ["sizing equity cap must be finite and positive"]


def test_copy_engine_blocks_source_leverage_above_exchange_max(base_config):
    engine = CopyEngine(base_config.risk, Mode.SHADOW, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-high-lev",
        source_positions={"SOL": Position("SOL", Decimal("10"), leverage=3)},
        follower_positions={},
        asset_meta={"SOL": AssetMeta("SOL", 2, max_leverage=2)},
        mids={"SOL": Decimal("150")},
    )
    assert result.intents == []
    assert result.blockers == ["SOL source leverage 3 exceeds exchange max 2"]


def test_copy_engine_caps_source_leverage_to_configured_follower_cap(base_config):
    engine = CopyEngine(base_config.risk, Mode.SHADOW, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-high-lev",
        source_positions={"BTC": Position("BTC", Decimal("1"), leverage=40)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5, max_leverage=50)},
        mids={"BTC": Decimal("50000")},
    )

    assert result.blockers == []
    assert result.desired_state.positions["BTC"].leverage == base_config.risk.max_leverage


def test_copy_engine_closes_before_opening_opposite_side(base_config):
    engine = CopyEngine(base_config.risk, Mode.PAPER, follower_account="0xf0")
    result = engine.plan(
        source_event_key="source-flip",
        source_positions={"BTC": Position("BTC", Decimal("-1"), leverage=2)},
        follower_positions={"BTC": Position("BTC", Decimal("0.005"))},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )
    assert [intent.action for intent in result.intents] == [IntentAction.CLOSE, IntentAction.OPEN]
    assert result.intents[0].reduce_only is True
    assert result.intents[0].price == Decimal("48500")
    assert result.intents[1].price == Decimal("49875")


def test_copy_engine_does_not_close_when_follower_was_never_open(base_config):
    engine = CopyEngine(base_config.risk, Mode.TESTNET, follower_account="0xf0")

    result = engine.plan(
        source_event_key="source-flat",
        source_positions={},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
        source_account_value=Decimal("1000"),
        follower_account_value=Decimal("100"),
    )

    assert result.blockers == []
    assert result.intents == []
    assert result.desired_state.positions == {}


def test_copy_engine_preserves_exact_dust_flatten_for_final_admission(base_config):
    engine = CopyEngine(base_config.risk, Mode.TESTNET, follower_account="0xf0")

    result = engine.plan(
        source_event_key="source-flat-dust",
        source_positions={},
        follower_positions={"BTC": Position("BTC", Decimal("0.00001"))},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
        source_account_value=Decimal("1000"),
        follower_account_value=Decimal("50"),
    )

    assert len(result.intents) == 1
    assert result.intents[0].action is IntentAction.REDUCE
    assert result.intents[0].reduce_only is True
    assert result.intents[0].side == "sell"
    assert result.intents[0].size == Decimal("0.00001")


def test_copy_engine_skips_symbols_outside_allowlist(base_config):
    engine = CopyEngine(base_config.risk, Mode.SHADOW)
    result = engine.plan(
        source_event_key="source-doge",
        source_positions={"DOGE": Position("DOGE", Decimal("1000"), leverage=1)},
        follower_positions={},
        asset_meta={},
        mids={"DOGE": Decimal("0.1")},
    )
    assert result.blockers == []
    assert result.desired_state.positions == {}
    assert result.intents == []


def test_copy_engine_marks_missing_mid_for_follower_delta_as_skipped(base_config):
    engine = CopyEngine(base_config.risk, Mode.TESTNET)
    result = engine.plan(
        source_event_key="source-flat",
        source_positions={},
        follower_positions={"BTC": Position("BTC", Decimal("0.005"))},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={},
        source_account_value=Decimal("1000"),
        follower_account_value=Decimal("100"),
    )
    assert len(result.intents) == 1
    assert result.intents[0].action == IntentAction.NOOP
    assert result.intents[0].reason == "missing mid price"
    assert result.blockers == ["BTC missing positive mid price for follower delta"]


def test_copy_engine_blocks_exchange_balance_sizing_without_finite_positive_values(base_config):
    engine = CopyEngine(base_config.risk, Mode.TESTNET)

    for source_value, follower_value in (
        (None, Decimal("100")),
        (Decimal("1000"), None),
        (Decimal("NaN"), Decimal("100")),
        (Decimal("1000"), Decimal("0")),
    ):
        result = engine.plan(
            source_event_key="source-missing-balance",
            source_positions={"BTC": Position("BTC", Decimal("1"), leverage=2)},
            follower_positions={},
            asset_meta={"BTC": AssetMeta("BTC", 5)},
            mids={"BTC": Decimal("50000")},
            source_account_value=source_value,
            follower_account_value=follower_value,
        )
        assert result.sizing["mode"] == "blocked_missing_balances"
        assert result.intents == []
        assert any("finite positive" in blocker for blocker in result.blockers)


def test_copy_engine_keeps_explicit_and_disabled_balance_sizing_paths(base_config):
    source_positions = {"BTC": Position("BTC", Decimal("1"), leverage=2)}
    asset_meta = {"BTC": AssetMeta("BTC", 5)}
    mids = {"BTC": Decimal("50000")}

    override = CopyEngine(
        replace(base_config.risk, equity_ratio=Decimal("0.001")),
        Mode.TESTNET,
    ).plan(
        source_event_key="source-override",
        source_positions=source_positions,
        follower_positions={},
        asset_meta=asset_meta,
        mids=mids,
    )
    disabled = CopyEngine(
        replace(base_config.risk, balance_sizing_enabled=False, fixed_multiplier=Decimal("0.001")),
        Mode.TESTNET,
    ).plan(
        source_event_key="source-disabled",
        source_positions=source_positions,
        follower_positions={},
        asset_meta=asset_meta,
        mids=mids,
    )

    assert override.blockers == []
    assert override.sizing["mode"] == "explicit_equity_ratio"
    assert disabled.blockers == []
    assert disabled.sizing["mode"] == "fixed_multiplier"


def test_copy_engine_blocks_nonfinite_source_and_follower_positions(base_config):
    source = CopyEngine(base_config.risk, Mode.SHADOW).plan(
        source_event_key="source-nan",
        source_positions={"BTC": Position("BTC", Decimal("NaN"), leverage=2)},
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )
    follower = CopyEngine(base_config.risk, Mode.SHADOW).plan(
        source_event_key="follower-nan",
        source_positions={},
        follower_positions={"BTC": Position("BTC", Decimal("NaN"))},
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
    )

    assert source.blockers == ["BTC source size is not finite"]
    assert follower.blockers == ["BTC follower size is not finite"]


def test_copy_engine_scales_targets_to_projected_gross_exposure_cap(base_config):
    risk = replace(base_config.risk, max_gross_exposure_usd=Decimal("300"))
    engine = CopyEngine(risk, Mode.SHADOW)
    result = engine.plan(
        source_event_key="source-wide",
        source_positions={
            "BTC": Position("BTC", Decimal("1"), leverage=2),
            "ETH": Position("ETH", Decimal("2"), leverage=2),
        },
        follower_positions={},
        asset_meta={"BTC": AssetMeta("BTC", 5), "ETH": AssetMeta("ETH", 4)},
        mids={"BTC": Decimal("50000"), "ETH": Decimal("3000")},
    )
    assert result.blockers == []
    assert result.desired_state.positions["BTC"].size == Decimal("0.00300")
    assert result.desired_state.positions["ETH"].size == Decimal("0.05")
    assert result.sizing["gross_before_cap"] == Decimal("500")
    assert result.sizing["gross_after_cap"] == Decimal("300")
