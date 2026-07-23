from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from hyperliquid_copytrader.copy_engine import AssetMeta
from hyperliquid_copytrader.guard import ExecutionGuard
from hyperliquid_copytrader.models import (
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    Position,
    SafeModeReason,
    now_ms,
)


def make_intent(
    *,
    cloid: str = "0x11111111111111111111111111111111",
    coin: str = "BTC",
    side: str = "buy",
    size: Decimal = Decimal("0.01"),
    price: Decimal = Decimal("50125"),
    reduce_only: bool = False,
) -> FollowerIntent:
    return FollowerIntent(
        intent_id="intent-" + cloid[-4:],
        cloid=cloid,
        action=IntentAction.OPEN,
        coin=coin,
        side=side,
        size=size,
        price=price,
        reduce_only=reduce_only,
        mode=Mode.TESTNET,
        source_event_key="source",
        reason="test",
        created_ms=now_ms(),
    )


def make_guard(
    base_config,
    store,
    *,
    risk=None,
    asset_meta=None,
    mids=None,
    mode=None,
    **ops_overrides,
) -> ExecutionGuard:
    ops = replace(base_config.ops, **ops_overrides)
    return ExecutionGuard(
        risk=risk or base_config.risk,
        ops=ops,
        store=store,
        asset_meta=asset_meta or {"BTC": AssetMeta("BTC", 5)},
        mids=mids or {"BTC": Decimal("50000")},
        mode=mode,
    )


def test_kill_switch_blocks_cycle(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    guard = make_guard(base_config, store, kill_switch_path=kill)
    decision = guard.check_cycle([make_intent()])
    assert not decision.ok
    assert decision.reason == SafeModeReason.OPERATOR_KILL_SWITCH


def test_guard_accepts_canonical_hip3_market_without_uppercasing_dex(base_config, store):
    risk = replace(base_config.risk, allowed_symbols=("xyz:aapl",))
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3)},
        mids={"xyz:AAPL": Decimal("300")},
    )

    decision = guard.check_intent(
        make_intent(coin="xyz:aapl", size=Decimal("0.1"), price=Decimal("300.5")),
        projected_positions={},
    )

    assert decision.ok is True


def test_exchange_mode_hip3_open_requires_matching_fresh_round_trip_proof(base_config, store):
    risk = replace(
        base_config.risk,
        allowed_symbols=("xyz:AAPL",),
        slippage_bps=Decimal("20"),
    )
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3)},
        mids={"xyz:AAPL": Decimal("300")},
        mode=Mode.TESTNET,
    )
    intent = make_intent(
        coin="xyz:AAPL",
        size=Decimal("0.1"),
        price=Decimal("301.2"),
    )
    missing = guard.check_intent(intent, projected_positions={})
    assert missing.ok is False
    assert "round-trip depth proof" in missing.detail

    observed = now_ms()
    proof = {
        "kind": "hip3_round_trip",
        "coin": "xyz:AAPL",
        "opening_side": "buy",
        "requested_size": Decimal("0.1"),
        "observed_ms": observed,
        "book_time_ms": observed,
        "oracle_px": Decimal("300"),
        "mark_px": Decimal("300"),
        "entry_limit": Decimal("301.2"),
        "exit_limit": Decimal("298.8"),
        "entry_visible_size": Decimal("1"),
        "exit_visible_size": Decimal("1"),
        "entry_best_px": Decimal("301.2"),
        "entry_worst_px": Decimal("301.2"),
        "exit_worst_px": Decimal("298.8"),
        "entry_notional_bound_px": Decimal("303"),
        "oracle_envelope_bps": risk.hip3_oracle_envelope_bps,
    }
    admitted = guard.check_intent(
        replace(intent, execution_proof=proof),
        projected_positions={},
    )
    assert admitted.ok is True

    native_guard = make_guard(
        base_config,
        store,
        risk=replace(base_config.risk, slippage_bps=Decimal("20")),
        asset_meta={"BTC": AssetMeta("BTC", 5)},
        mids={"BTC": Decimal("50000")},
        mode=Mode.TESTNET,
    )
    native = native_guard.check_intent(
        make_intent(size=Decimal("0.001"), price=Decimal("50200")),
        projected_positions={},
    )
    assert native.ok is False
    assert "slippage bound" in native.detail


def test_exchange_mode_hip3_open_rejects_stale_round_trip_proof(base_config, store):
    risk = replace(base_config.risk, allowed_symbols=("xyz:AAPL",), stale_source_ms=100)
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3)},
        mids={"xyz:AAPL": Decimal("300")},
        mode=Mode.TESTNET,
    )
    stale = now_ms() - 1_000
    intent = replace(
        make_intent(coin="xyz:AAPL", size=Decimal("0.1"), price=Decimal("300.5")),
        execution_proof={
            "kind": "hip3_round_trip",
            "coin": "xyz:AAPL",
            "opening_side": "buy",
            "requested_size": Decimal("0.1"),
            "observed_ms": stale,
            "book_time_ms": stale,
            "oracle_px": Decimal("300"),
            "entry_limit": Decimal("300.5"),
            "exit_limit": Decimal("299.5"),
            "entry_visible_size": Decimal("1"),
            "exit_visible_size": Decimal("1"),
            "entry_best_px": Decimal("300.5"),
            "entry_worst_px": Decimal("300.5"),
            "exit_worst_px": Decimal("299.5"),
            "entry_notional_bound_px": Decimal("303"),
            "oracle_envelope_bps": risk.hip3_oracle_envelope_bps,
        },
    )
    decision = guard.check_intent(intent, projected_positions={})
    assert decision.ok is False
    assert decision.reason == SafeModeReason.STALE_SOURCE


def test_exchange_mode_hip3_open_rejects_forged_non_crossing_round_trip_proof(base_config, store):
    risk = replace(base_config.risk, allowed_symbols=("xyz:AAPL",))
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3)},
        mids={"xyz:AAPL": Decimal("300")},
        mode=Mode.TESTNET,
    )
    observed = now_ms()
    proof = {
        "kind": "hip3_round_trip",
        "coin": "xyz:AAPL",
        "opening_side": "buy",
        "requested_size": Decimal("0.1"),
        "observed_ms": observed,
        "book_time_ms": observed,
        "oracle_px": Decimal("300"),
        "entry_limit": Decimal("300.4"),
        "exit_limit": Decimal("299.6"),
        "entry_visible_size": Decimal("1"),
        "exit_visible_size": Decimal("1"),
        "entry_best_px": Decimal("300.5"),
        "entry_worst_px": Decimal("300.5"),
        "exit_worst_px": Decimal("299.5"),
        "entry_notional_bound_px": Decimal("303"),
        "oracle_envelope_bps": risk.hip3_oracle_envelope_bps,
    }
    intent = replace(
        make_intent(coin="xyz:AAPL", size=Decimal("0.1"), price=Decimal("300.4")),
        execution_proof=proof,
    )

    decision = guard.check_intent(intent, projected_positions={})

    assert decision.ok is False
    assert decision.reason == SafeModeReason.RISK_LIMIT
    assert "do not cross" in decision.detail


def test_exchange_mode_hip3_open_rejects_time_inconsistent_round_trip_proof(base_config, store):
    risk = replace(base_config.risk, allowed_symbols=("xyz:AAPL",))
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"xyz:AAPL": AssetMeta("xyz:AAPL", 3)},
        mids={"xyz:AAPL": Decimal("300")},
        mode=Mode.TESTNET,
    )
    observed = now_ms()
    proof = {
        "kind": "hip3_round_trip",
        "coin": "xyz:AAPL",
        "opening_side": "sell",
        "requested_size": Decimal("0.1"),
        "observed_ms": observed,
        "book_time_ms": observed + 5_000,
        "oracle_px": Decimal("300"),
        "entry_limit": Decimal("299.5"),
        "exit_limit": Decimal("300.5"),
        "entry_visible_size": Decimal("1"),
        "exit_visible_size": Decimal("1"),
        "entry_best_px": Decimal("299.5"),
        "entry_worst_px": Decimal("299.5"),
        "exit_worst_px": Decimal("300.5"),
        "entry_notional_bound_px": Decimal("303"),
        "oracle_envelope_bps": risk.hip3_oracle_envelope_bps,
    }
    intent = replace(
        make_intent(
            coin="xyz:AAPL",
            side="sell",
            size=Decimal("0.1"),
            price=Decimal("299.5"),
        ),
        execution_proof=proof,
    )

    decision = guard.check_intent(intent, projected_positions={})

    assert decision.ok is False
    assert decision.reason == SafeModeReason.STALE_SOURCE
    assert "time-inconsistent" in decision.detail


def test_max_new_intents_blocks_cycle(base_config, store):
    guard = make_guard(base_config, store, max_new_intents_per_cycle=1)
    decision = guard.check_cycle(
        [
            make_intent(cloid="0x11111111111111111111111111111111"),
            make_intent(cloid="0x22222222222222222222222222222222"),
        ]
    )
    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT


def test_two_reduce_only_closes_do_not_consume_new_risk_capacity(base_config, store):
    guard = make_guard(base_config, store, max_new_intents_per_cycle=1)
    closes = [
        replace(
            make_intent(
                cloid="0x11111111111111111111111111111111",
                coin="BTC",
                reduce_only=True,
            ),
            action=IntentAction.CLOSE,
        ),
        replace(
            make_intent(
                cloid="0x22222222222222222222222222222222",
                coin="ETH",
                reduce_only=True,
            ),
            action=IntentAction.CLOSE,
        ),
    ]

    decision = guard.check_cycle(closes)

    assert decision.ok is True


def test_reduction_cancel_plus_one_open_fit_one_new_risk_slot(base_config, store):
    guard = make_guard(base_config, store, max_new_intents_per_cycle=1)
    reduction = replace(
        make_intent(
            cloid="0x11111111111111111111111111111111",
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )
    opening = make_intent(cloid="0x22222222222222222222222222222222", coin="ETH")
    cancel = replace(
        make_intent(cloid="0x33333333333333333333333333333333", coin="SOL"),
        action=IntentAction.CANCEL,
    )

    decision = guard.check_cycle([reduction, cancel, opening])

    assert decision.ok is True


def test_pending_reduction_does_not_consume_open_intent_capacity(base_config, store):
    reduction = replace(
        make_intent(
            cloid="0x11111111111111111111111111111111",
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )
    assert store.append_intent(reduction)
    guard = make_guard(base_config, store, mode=Mode.TESTNET, max_open_intents=1)

    decision = guard.check_cycle(
        [make_intent(cloid="0x22222222222222222222222222222222", coin="ETH")]
    )

    assert decision.ok is True


def test_max_open_intents_does_not_double_count_prepared_current_plan(base_config, store):
    intent = make_intent()
    store.append_intent(intent)
    guard = make_guard(base_config, store, mode=Mode.TESTNET, max_open_intents=1)

    decision = guard.check_cycle([intent])

    assert decision.ok is True


def test_guard_rejects_slippage_and_notional_violations(base_config, store):
    guard = make_guard(base_config, store)
    too_aggressive = make_intent(price=Decimal("51000"))
    decision = guard.check_intent(too_aggressive, projected_positions={})
    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT

    too_large = make_intent(size=Decimal("1"), price=Decimal("50125"))
    decision = guard.check_intent(too_large, projected_positions={})
    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT


def test_guard_accepts_price_at_quantized_slippage_bound(base_config, store):
    risk = replace(base_config.risk, allowed_symbols=("SOL",), slippage_bps=Decimal("30"))
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"SOL": AssetMeta("SOL", 2)},
        mids={"SOL": Decimal("77.0945")},
    )
    intent = make_intent(
        coin="SOL",
        side="buy",
        size=Decimal("1.29"),
        price=Decimal("77.3260"),
    )

    decision = guard.check_intent(intent, projected_positions={})

    assert decision.ok


def test_guard_uses_wider_slippage_for_reduce_only_closes(base_config, store):
    guard = make_guard(base_config, store)
    close_intent = replace(
        make_intent(
            side="buy",
            size=Decimal("0.005"),
            price=Decimal("51000"),
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )

    decision = guard.check_intent(
        close_intent,
        projected_positions={"BTC": Position("BTC", Decimal("-0.01"))},
    )

    assert decision.ok


def test_guard_rejects_reduce_only_beyond_close_slippage(base_config, store):
    guard = make_guard(base_config, store)
    close_intent = replace(
        make_intent(
            side="buy",
            size=Decimal("0.005"),
            price=Decimal("99000"),
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )

    decision = guard.check_intent(
        close_intent,
        projected_positions={"BTC": Position("BTC", Decimal("-0.01"))},
    )

    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT
    assert "HLCT_CLOSE_SLIPPAGE_BPS" in decision.detail


def test_guard_rejects_reduce_only_that_increases_exposure(base_config, store):
    guard = make_guard(base_config, store)
    intent = make_intent(side="buy", reduce_only=True)
    decision = guard.check_intent(
        intent,
        projected_positions={"BTC": Position("BTC", Decimal("0.01"))},
    )
    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT


def test_guard_allows_strict_de_risking_while_still_above_caps(base_config, store):
    risk = replace(
        base_config.risk,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("250"),
    )
    guard = make_guard(base_config, store, risk=risk)
    intent = replace(
        make_intent(side="sell", size=Decimal("0.005"), price=Decimal("49000"), reduce_only=True),
        action=IntentAction.REDUCE,
    )

    decision = guard.check_intent(
        intent,
        projected_positions={"BTC": Position("BTC", Decimal("0.02"))},
    )

    assert decision.ok


def test_guard_allows_full_close_above_caps_but_rejects_zero_cross(base_config, store):
    risk = replace(
        base_config.risk,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("250"),
    )
    guard = make_guard(base_config, store, risk=risk)
    close = replace(
        make_intent(side="sell", size=Decimal("0.02"), price=Decimal("49000"), reduce_only=True),
        action=IntentAction.CLOSE,
    )
    crossing = replace(close, size=Decimal("0.021"), action=IntentAction.REDUCE)

    assert guard.check_intent(
        close,
        projected_positions={"BTC": Position("BTC", Decimal("0.02"))},
    ).ok
    rejected = guard.check_intent(
        crossing,
        projected_positions={"BTC": Position("BTC", Decimal("0.02"))},
    )
    assert not rejected.ok
    assert "crossing zero" in rejected.detail


def test_guard_allows_exact_subminimum_close_but_rejects_partial_dust(base_config, store):
    guard = make_guard(base_config, store)
    exact = replace(
        make_intent(
            side="sell",
            size=Decimal("0.00001"),
            price=Decimal("49000"),
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )
    partial = replace(exact, size=Decimal("0.000005"))
    position = {"BTC": Position("BTC", Decimal("0.00001"))}

    accepted = guard.check_intent(exact, projected_positions=position)
    rejected = guard.check_intent(partial, projected_positions=position)

    assert accepted.ok is True
    assert rejected.ok is False
    assert "below Hyperliquid perp minimum" in rejected.detail


def test_guard_blocks_fifth_distinct_projected_position(base_config, store):
    coins = ("BTC", "ETH", "SOL", "DOGE", "XRP")
    risk = replace(
        base_config.risk,
        allowed_symbols=coins,
        max_open_positions=4,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("1000"),
    )
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={coin: AssetMeta(coin, 4) for coin in coins},
        mids={coin: Decimal("20") for coin in coins},
    )
    projected = {coin: Position(coin, Decimal("1")) for coin in ("BTC", "ETH", "SOL", "DOGE")}

    decision = guard.check_intent(
        make_intent(coin="XRP", size=Decimal("1"), price=Decimal("20")),
        projected_positions=projected,
    )

    assert decision.ok is False
    assert decision.reason == SafeModeReason.RISK_LIMIT
    assert "HLCT_MAX_OPEN_POSITIONS=4" in decision.detail


def test_guard_allows_increasing_existing_market_at_position_cap(base_config, store):
    coins = ("BTC", "ETH", "SOL", "DOGE")
    risk = replace(
        base_config.risk,
        allowed_symbols=coins,
        max_open_positions=4,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("1000"),
    )
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={coin: AssetMeta(coin, 4) for coin in coins},
        mids={coin: Decimal("20") for coin in coins},
    )

    decision = guard.check_intent(
        make_intent(coin="BTC", size=Decimal("1"), price=Decimal("20")),
        projected_positions={coin: Position(coin, Decimal("1")) for coin in coins},
    )

    assert decision.ok is True


def test_full_reduction_releases_position_capacity_for_new_market(base_config, store):
    coins = ("BTC", "ETH", "SOL", "DOGE", "XRP")
    risk = replace(
        base_config.risk,
        allowed_symbols=coins,
        max_open_positions=4,
        max_notional_usd=Decimal("250"),
        max_gross_exposure_usd=Decimal("1000"),
    )
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={coin: AssetMeta(coin, 4) for coin in coins},
        mids={coin: Decimal("20") for coin in coins},
    )
    projected = {coin: Position(coin, Decimal("1")) for coin in ("BTC", "ETH", "SOL", "DOGE")}
    close = replace(
        make_intent(
            cloid="0x44444444444444444444444444444444",
            coin="DOGE",
            side="sell",
            size=Decimal("1"),
            price=Decimal("20"),
            reduce_only=True,
        ),
        action=IntentAction.CLOSE,
    )

    assert guard.check_intent(close, projected_positions=projected).ok is True
    guard.apply_projection(close, projected)
    assert "DOGE" not in projected
    opening = make_intent(
        cloid="0x55555555555555555555555555555555",
        coin="XRP",
        size=Decimal("1"),
        price=Decimal("20"),
    )
    assert guard.check_intent(opening, projected_positions=projected).ok is True


def test_guard_position_cap_never_blocks_strict_reduction(base_config, store):
    risk = replace(base_config.risk, max_open_positions=1)
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        mids={"BTC": Decimal("50000"), "ETH": Decimal("3000")},
    )
    reduction = replace(
        make_intent(
            side="sell",
            size=Decimal("0.005"),
            price=Decimal("50000"),
            reduce_only=True,
        ),
        action=IntentAction.REDUCE,
    )

    decision = guard.check_intent(
        reduction,
        projected_positions={
            "BTC": Position("BTC", Decimal("0.01")),
            "ETH": Position("ETH", Decimal("1")),
        },
    )

    assert decision.ok is True


def test_guard_position_cap_never_blocks_cancel(base_config, store):
    risk = replace(
        base_config.risk,
        allowed_symbols=("BTC", "ETH"),
        max_open_positions=1,
    )
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"BTC": AssetMeta("BTC", 5), "ETH": AssetMeta("ETH", 4)},
        mids={"BTC": Decimal("50000"), "ETH": Decimal("3000")},
    )
    cancel = replace(
        make_intent(
            cloid="0x22222222222222222222222222222222",
            coin="ETH",
            size=Decimal("0.01"),
            price=Decimal("3000"),
        ),
        action=IntentAction.CANCEL,
    )

    decision = guard.check_intent(
        cancel,
        projected_positions={"BTC": Position("BTC", Decimal("0.005"))},
    )

    assert decision.ok is True


def test_guard_rejects_nonfinite_inputs_without_decimal_exceptions(base_config, store):
    guard = make_guard(base_config, store)
    intent = make_intent(size=Decimal("NaN"))

    cycle = guard.check_cycle([intent])
    decision = guard.check_intent(intent, projected_positions={})
    projected = guard.check_intent(
        make_intent(size=Decimal("0.005")),
        projected_positions={"ETH": Position("ETH", Decimal("NaN"))},
    )

    assert not cycle.ok
    assert cycle.reason == SafeModeReason.CONFIG_INVALID
    assert not decision.ok
    assert decision.reason == SafeModeReason.CONFIG_INVALID
    assert not projected.ok
    assert projected.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_guard_rejects_projected_portfolio_gross_cap(base_config, store):
    risk = replace(base_config.risk, max_gross_exposure_usd=Decimal("300"))
    guard = make_guard(
        base_config,
        store,
        risk=risk,
        asset_meta={"BTC": AssetMeta("BTC", 5), "ETH": AssetMeta("ETH", 4)},
        mids={"BTC": Decimal("50000"), "ETH": Decimal("3000")},
    )
    intent = make_intent(
        cloid="0x33333333333333333333333333333333",
        coin="ETH",
        size=Decimal("0.03"),
        price=Decimal("3007.5"),
    )
    decision = guard.check_intent(
        intent,
        projected_positions={"BTC": Position("BTC", Decimal("0.005"))},
    )
    assert not decision.ok
    assert decision.reason == SafeModeReason.RISK_LIMIT
    assert "gross exposure" in decision.detail


def test_guard_rejects_skipped_noop_from_missing_market_data(base_config, store):
    guard = make_guard(base_config, store)
    missing_mid = replace(
        make_intent(),
        action=IntentAction.NOOP,
        side="none",
        size=Decimal("0"),
        price=None,
        status=IntentStatus.SKIPPED,
        reason="missing mid price",
    )
    decision = guard.check_intent(missing_mid, projected_positions={})
    assert not decision.ok
    assert decision.reason == SafeModeReason.STALE_SOURCE

    missing_metadata = replace(missing_mid, reason="missing metadata")
    decision = guard.check_intent(missing_metadata, projected_positions={})
    assert not decision.ok
    assert decision.reason == SafeModeReason.UNSUPPORTED_SYMBOL


def test_guard_skips_cloid_with_execution_evidence(base_config, store):
    guard = make_guard(base_config, store)
    intent = make_intent()
    store.append_intent(intent)
    from hyperliquid_copytrader.models import ExecutionReport, IntentStatus

    store.append_execution_report(
        ExecutionReport(
            report_id="report-1",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=now_ms(),
        )
    )
    decision = guard.check_intent(intent, projected_positions={})
    assert not decision.ok
    assert decision.reason == SafeModeReason.DUPLICATE_INTENT
    assert decision.terminal_skip
