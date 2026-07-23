from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from typing import Any, cast

import pytest

from hyperliquid_copytrader import service as service_module
from hyperliquid_copytrader.config import (
    DeadManPolicy,
    ExchangeConfig,
    OpsConfig,
    SourceNetwork,
)
from hyperliquid_copytrader.copy_engine import AssetMeta, CopyResult
from hyperliquid_copytrader.exchange.hyperliquid import FakeExecutionAdapter
from hyperliquid_copytrader.models import (
    DesiredState,
    ExecutionReport,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    OpenOrder,
    Position,
    SafeModeReason,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.observer import SourceSnapshot, SourceWebsocketMessageError
from hyperliquid_copytrader.preflight import PreflightReport, build_preflight_report
from hyperliquid_copytrader.service import CopyTraderService

from .fixtures.fake_hyperliquid import FakeInfoClient


def _event(key: str, event_type: SourceEventType, subtype: str) -> SourceEvent:
    return SourceEvent(
        idempotency_key=key,
        event_type=event_type,
        exchange_ts_ms=1000,
        observed_ts_ms=1001,
        payload={"event_subtype": subtype},
    )


def _record_run(calls: list[str], result: dict | None = None) -> dict:
    calls.append("run")
    return result or {}


def _completed_cycle_result() -> dict[str, Any]:
    return {
        "preflight": {"passed": True},
        "safe_mode": {"enabled": False},
        "desired_state_committed": True,
        "intents": [],
    }


def _actual_checkpoint_dust_cycle_result(
    *,
    action_status: str = "filled",
    action_exchange_status: str = "filled",
) -> dict[str, Any]:
    state_id = "state-ewy-kr200"
    return {
        "preflight": {"passed": True},
        "safe_mode": {"enabled": False},
        "desired_state_committed": False,
        "desired_state": {
            "state_id": state_id,
            "positions": {
                "EWY": {"coin": "EWY", "size": "0.083", "leverage": 1},
                "xyz:KR200": {
                    "coin": "xyz:KR200",
                    "size": "-0.013",
                    "leverage": 1,
                },
            },
        },
        "intents": [
            {
                "intent_id": "intent-ewy-open",
                "cloid": "0xewy",
                "action": "open",
                "coin": "EWY",
                "side": "buy",
                "size": "0.083",
                "reduce_only": False,
                "status": "pending",
            },
            {
                "intent_id": "intent-kr200-dust",
                "cloid": "0xkr200",
                "action": "noop",
                "coin": "xyz:KR200",
                "side": "none",
                "size": "0",
                "reduce_only": False,
                "status": "skipped",
                "reason": "delta below min size/notional: size=0.0001 notional=0.19",
            },
        ],
        "reports": [
            {
                "report_id": "report-ewy",
                "intent_id": "intent-ewy-open",
                "cloid": "0xewy",
                "status": action_status,
                "exchange_status": action_exchange_status,
            },
            {
                "report_id": "report-kr200",
                "intent_id": "intent-kr200-dust",
                "cloid": "0xkr200",
                "status": "skipped",
                "exchange_status": "skipped",
            },
        ],
        "execution_finalization": {
            "status": "actual_checkpoint_committed",
            "target_state_id": state_id,
            "committed_target": False,
            "checkpoint": {
                "state_id": "checkpoint-ewy-kr200",
                "positions": {
                    "EWY": {"coin": "EWY", "size": "0.083", "leverage": 1},
                    "xyz:KR200": {
                        "coin": "xyz:KR200",
                        "size": "-0.0129",
                        "leverage": 1,
                    },
                },
            },
        },
    }


def _all_noop_current_truth_case(
    base_config,
    store,
    *,
    committed_scope_overrides: dict[str, str] | None = None,
):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            vault_address=follower,
            api_private_key="0x" + "1" * 64,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=True,
        ),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    follower_positions = {
        "EWY": Position("EWY", Decimal("0.083"), leverage=1),
        "xyz:KR200": Position("xyz:KR200", Decimal("-0.0129"), leverage=1),
    }
    committed_scope = {
        "source_wallet": config.source_wallet.lower(),
        "action_account": follower,
        "source_network": SourceNetwork.MAINNET.value,
    }
    committed_scope.update(committed_scope_overrides or {})
    committed = DesiredState(
        state_id="committed-ewy-kr200-checkpoint",
        source_event_key="prior-fill",
        mode=Mode.LIVE,
        positions=follower_positions,
        reason="fresh actual follower checkpoint",
        created_ms=now_ms(),
        **committed_scope,
    )
    assert store.append_desired_state(committed)
    store.commit_desired_state(committed.state_id)

    target_positions = {
        "EWY": Position("EWY", Decimal("0.083"), leverage=1),
        "xyz:KR200": Position("xyz:KR200", Decimal("-0.013"), leverage=1),
    }
    prior_state = replace(
        committed,
        state_id="prior-ewy-kr200-target",
        source_event_key="stable-source-planning-key",
        positions=target_positions,
        reason="prior target with terminal KR200 dust",
    )
    prior_noop = FollowerIntent(
        intent_id="0x53b5a02bbc270bc4bbb19794c9a64953",
        cloid="0x1c37d375bb017bfd6d0822e5ff5975d7",
        action=IntentAction.NOOP,
        coin="xyz:KR200",
        side="none",
        size=Decimal("0"),
        price=None,
        reduce_only=False,
        mode=Mode.LIVE,
        source_event_key=prior_state.source_event_key,
        reason="delta below min size/notional: size=0.0001 notional=0.10421",
        created_ms=now_ms(),
        desired_state_id=prior_state.state_id,
        status=IntentStatus.SKIPPED,
    )
    assert store.prepare_execution_plan(prior_state, [prior_noop])
    assert store.append_execution_report(
        ExecutionReport(
            report_id="prior-kr200-skipped",
            intent_id=prior_noop.intent_id,
            cloid=prior_noop.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="skipped",
            exchange_ts_ms=now_ms(),
            payload={"reason": prior_noop.reason},
        )
    )
    assert store.pending_intent_count(Mode.LIVE) == 0

    desired = replace(
        prior_state,
        state_id="fresh-ewy-kr200-target",
        reason="fresh all-NOOP target",
        source_wallet=config.source_wallet.lower(),
        action_account=follower,
        source_network=SourceNetwork.MAINNET.value,
    )
    current_noop = replace(
        prior_noop,
        desired_state_id=desired.state_id,
        reason="delta below min size/notional: size=0.0001 notional=0.10500",
        created_ms=now_ms(),
    )
    result = CopyResult(
        desired_state=desired,
        intents=[current_noop],
        blockers=[],
        sizing={},
    )
    return service, result, follower_positions


def _all_noop_with_hip3_liquidity_case(base_config, store):
    service, prior_result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    source_event_key = prior_result.desired_state.source_event_key
    prior_state = replace(
        prior_result.desired_state,
        state_id="prior-ewy-dust-target",
        positions={
            **follower_positions,
            "EWY": replace(follower_positions["EWY"], size=Decimal("0.082")),
        },
        reason="prior EWY below-min target",
    )
    prior_noop = replace(
        prior_result.intents[0],
        intent_id="0x61870000000000000000000000000000",
        cloid="0x61870000000000000000000000000001",
        coin="EWY",
        source_event_key=source_event_key,
        reason="delta below min size/notional: size=0.001 notional=0.1636",
        desired_state_id=prior_state.state_id,
        created_ms=now_ms(),
    )
    assert store.prepare_execution_plan(prior_state, [prior_noop])
    assert store.append_execution_report(
        ExecutionReport(
            report_id="prior-ewy-skipped",
            intent_id=prior_noop.intent_id,
            cloid=prior_noop.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="skipped",
            exchange_ts_ms=now_ms(),
            payload={"reason": prior_noop.reason},
        )
    )

    desired = replace(
        prior_state,
        state_id="fresh-ewy-noop-with-hip3-liquidity",
        reason="fresh staged target with paced HIP-3 liquidity retries",
    )
    current_noop = replace(
        prior_noop,
        desired_state_id=desired.state_id,
        created_ms=now_ms(),
    )
    kr200 = FollowerIntent(
        intent_id="liquidity-deferred-kr200",
        cloid="0x71000000000000000000000000000001",
        action=IntentAction.OPEN,
        coin="xyz:KR200",
        side="sell",
        size=Decimal("0.0125"),
        price=Decimal("1042"),
        reduce_only=False,
        mode=Mode.LIVE,
        source_event_key=source_event_key,
        reason="add source KR200 exposure",
        created_ms=now_ms(),
        desired_state_id=desired.state_id,
        status=IntentStatus.PENDING,
    )
    skhx = replace(
        kr200,
        intent_id="liquidity-deferred-skhx",
        cloid="0x71000000000000000000000000000002",
        coin="xyz:SKHX",
        size=Decimal("0.1"),
        price=Decimal("100"),
        reason="open source SKHX exposure",
    )
    deadline = now_ms() + 60_000
    liquidity_deferrals = [
        service_module.Hip3LiquidityDeferral(
            intent=kr200,
            blockers=("xyz:KR200 has no usable oracle-bounded entry depth",),
            retry_not_before_ms=deadline,
            stage="planning_admission",
        ),
        service_module.Hip3LiquidityDeferral(
            intent=skhx,
            blockers=("xyz:SKHX has no usable oracle-bounded exit depth",),
            retry_not_before_ms=deadline,
            stage="planning_admission",
        ),
    ]
    result = CopyResult(
        desired_state=desired,
        intents=[current_noop],
        blockers=[],
        sizing={},
    )
    return service, result, follower_positions, liquidity_deferrals


def _liquidity_deferred_cycle_result(*, retry_not_before_ms: int) -> dict[str, Any]:
    return {
        **_completed_cycle_result(),
        "deferred_intents": [],
        "liquidity_deferred_intents": [
            {
                "intent": {
                    "intent_id": "liquidity-deferred-kr200",
                    "action": "open",
                    "coin": "xyz:KR200",
                    "side": "sell",
                    "size": "0.0125",
                    "reduce_only": False,
                },
                "blockers": [
                    "xyz:KR200 has only 0 visible sell entry depth inside the 100bps "
                    "oracle envelope; 0.0125 required"
                ],
                "retry_not_before_ms": retry_not_before_ms,
            }
        ],
    }


def _persist_hip3_liquidity_retry(store, event: SourceEvent, *, retry_not_before_ms: int) -> None:
    assert store.append_source_event(event, reaction_required=True)
    assert (
        store.finish_source_reactions(
            [event.idempotency_key],
            status="blocked",
            outcome={
                "source_event_key": event.idempotency_key,
                "action": "run_once",
                "result": _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms),
                "retry": {
                    "class": "hip3_liquidity",
                    "disposition": "deferred",
                    "retry_not_before_ms": retry_not_before_ms,
                    "retry_interval_ms": 60_000,
                    "coins": ["xyz:KR200"],
                    "deferral_count": 1,
                },
            },
        )
        == 1
    )


def _set_run_once(
    service: CopyTraderService,
    run_once: Callable[[], dict[str, Any]],
) -> None:
    cast(Any, service).run_once = run_once


def _live_exposed_recovery_service(base_config, store, monkeypatch):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            vault_address=follower,
            api_private_key="0x" + "1" * 64,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=True,
        ),
    )
    follower_position = Position("BTC", Decimal("0.005"), leverage=2)
    adapter = FakeExecutionAdapter(
        account=follower,
        positions={"BTC": follower_position},
        forced_status=None,
    )
    info = FakeInfoClient()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_adapter=adapter,
    )
    monkeypatch.setattr(
        service,
        "preflight",
        lambda **_kwargs: PreflightReport(mode=Mode.LIVE, passed=True),
    )
    desired = DesiredState(
        state_id="committed-live-btc",
        source_event_key="prior-source-state",
        mode=Mode.LIVE,
        positions={"BTC": follower_position},
        reason="test committed follower baseline",
        created_ms=now_ms(),
        source_wallet=config.source_wallet.lower(),
        action_account=follower,
        source_network=SourceNetwork.MAINNET.value,
    )
    assert store.append_desired_state(desired)
    store.commit_desired_state(desired.state_id)
    blocked = replace(
        _event("blocked-live-recovery", SourceEventType.FILL, "fill"),
        source_wallet=config.source_wallet,
    )
    assert store.append_source_event(blocked, reaction_required=True)
    store.finish_source_reactions(
        [blocked.idempotency_key],
        status="blocked",
        outcome={"reason": "source websocket expired"},
    )
    return service, adapter, info, blocked


def _live_flat_recovery_service(base_config, store, monkeypatch):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            vault_address=follower,
            api_private_key="0x" + "1" * 64,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(account=follower, positions={}, forced_status=None)
    info = FakeInfoClient()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_adapter=adapter,
    )
    monkeypatch.setattr(
        service,
        "preflight",
        lambda **_kwargs: PreflightReport(mode=Mode.LIVE, passed=True),
    )
    desired = DesiredState(
        state_id="committed-live-flat",
        source_event_key="prior-flat-source-state",
        mode=Mode.LIVE,
        positions={},
        reason="test committed flat follower baseline",
        created_ms=now_ms(),
        source_wallet=config.source_wallet.lower(),
        action_account=follower,
        source_network=SourceNetwork.MAINNET.value,
    )
    assert store.append_desired_state(desired)
    store.commit_desired_state(desired.state_id)
    blocked = replace(
        _event("blocked-live-flat-recovery", SourceEventType.FILL, "fill"),
        source_wallet=config.source_wallet,
    )
    assert store.append_source_event(blocked, reaction_required=True)
    store.finish_source_reactions(
        [blocked.idempotency_key],
        status="blocked",
        outcome={"reason": "source websocket expired"},
    )
    return service, adapter, info, blocked


def test_source_reaction_queue_size_must_be_positive(base_config):
    config = replace(base_config, ops=OpsConfig(source_reaction_queue_size=0))
    report = build_preflight_report(config)
    assert not report.passed
    assert any("source reaction queue size" in blocker for blocker in report.blockers)


@pytest.mark.parametrize(
    "result",
    [
        {},
        {"preflight": {"passed": True}},
        {
            "preflight": {"passed": False},
            "safe_mode": {"enabled": False},
            "desired_state_committed": True,
        },
        {
            "preflight": {"passed": True},
            "safe_mode": {},
            "desired_state_committed": True,
        },
        {"preflight": {"passed": True}, "safe_mode": {"enabled": False}},
    ],
)
def test_source_reaction_completion_requires_explicit_safe_cycle(base_config, store, result):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    assert service._source_reaction_run_completed(result) is False
    assert service._source_reaction_run_completed(_completed_cycle_result()) is True


def test_source_reaction_remains_incomplete_while_open_work_is_deferred(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = {
        **_completed_cycle_result(),
        "deferred_intents": [
            {
                "intent_id": "deferred-open",
                "action": "open",
                "coin": "ETH",
                "side": "buy",
                "size": "0.1",
                "reduce_only": False,
            }
        ],
    }

    assert service._execution_cycle_completed(result) is True
    assert service._source_reaction_run_completed(result) is False


def test_actual_checkpoint_completes_filled_action_with_below_min_noop_residual(
    base_config,
    store,
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = _actual_checkpoint_dust_cycle_result()

    assert service._execution_cycle_completed(result) is True
    assert service._source_reaction_run_completed(result) is True


@pytest.mark.parametrize(
    ("status", "exchange_status"),
    [
        ("rejected", "rejected"),
        ("sent", "transport_unknown"),
        ("acked", "open"),
    ],
)
def test_actual_checkpoint_remains_incomplete_without_terminal_action_success(
    base_config,
    store,
    status,
    exchange_status,
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = _actual_checkpoint_dust_cycle_result(
        action_status=status,
        action_exchange_status=exchange_status,
    )

    assert service._execution_cycle_completed(result) is False
    assert service._source_reaction_run_completed(result) is False


def test_actual_checkpoint_remains_incomplete_when_actionable_coin_diverged(
    base_config,
    store,
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = _actual_checkpoint_dust_cycle_result()
    result["execution_finalization"]["checkpoint"]["positions"]["EWY"]["size"] = "0.082"

    assert service._execution_cycle_completed(result) is False
    assert service._source_reaction_run_completed(result) is False


def test_fresh_all_noop_collision_uses_matching_committed_truth_without_replay(
    base_config,
    store,
):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    intent_count = store.count("follower_intents")
    report_count = store.count("execution_reports")

    # This is the production incident shape: the new state is distinct, but
    # the deterministic dust-NOOP identity already belongs to a terminal plan.
    assert store.prepare_execution_plan(result.desired_state, result.intents) is False
    observed_ms = now_ms()
    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is not None
    assert cycle["execution_finalization"]["status"] == ("committed_baseline_noop_verified")
    assert cycle["desired_state_committed"] is False
    assert cycle["reports"] == []
    assert cycle["safe_mode"]["enabled"] is False
    assert service._execution_cycle_completed(cycle) is True
    assert service._source_reaction_run_completed(cycle) is True
    assert store.count("follower_intents") == intent_count
    assert store.count("execution_reports") == report_count


def test_fresh_all_noop_collision_preserves_typed_hip3_liquidity_retry(
    base_config,
    store,
):
    service, result, follower_positions, liquidity_deferrals = _all_noop_with_hip3_liquidity_case(
        base_config, store
    )
    intent_count = store.count("follower_intents")
    report_count = store.count("execution_reports")
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=liquidity_deferrals,
    )

    assert cycle is not None
    assert cycle["desired_state_committed"] is False
    assert cycle["reports"] == []
    assert [item["intent"]["coin"] for item in cycle["liquidity_deferred_intents"]] == [
        "xyz:KR200",
        "xyz:SKHX",
    ]
    assert service._execution_cycle_completed(cycle) is True
    assert service._source_reaction_run_completed(cycle) is False
    retry = service._hip3_liquidity_retry_payload(cycle)
    assert retry == {
        "class": "hip3_liquidity",
        "disposition": "deferred",
        "retry_not_before_ms": liquidity_deferrals[0].retry_not_before_ms,
        "retry_interval_ms": 60_000,
        "coins": ["xyz:KR200", "xyz:SKHX"],
        "deferral_count": 2,
    }
    assert store.count("follower_intents") == intent_count
    assert store.count("execution_reports") == report_count


def test_verified_all_noop_liquidity_cycle_stays_complete_after_retry_is_due(
    base_config,
    store,
    monkeypatch,
):
    service, result, follower_positions, liquidity_deferrals = _all_noop_with_hip3_liquidity_case(
        base_config, store
    )
    observed_ms = now_ms()
    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=liquidity_deferrals,
    )
    assert cycle is not None

    monkeypatch.setattr(
        service_module,
        "now_ms",
        lambda: liquidity_deferrals[0].retry_not_before_ms + 1,
    )

    assert service._execution_cycle_completed(cycle) is True
    assert service._source_reaction_run_completed(cycle) is False
    retry = service._hip3_liquidity_retry_payload(cycle)
    assert retry is not None
    assert retry["retry_not_before_ms"] == liquidity_deferrals[0].retry_not_before_ms


def test_live_run_cycle_handles_fresh_all_noop_collision_before_prepare(
    base_config,
    store,
    monkeypatch,
):
    service, planned_result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    intent_count = store.count("follower_intents")
    report_count = store.count("execution_reports")
    signed_action_count = store.count("signed_action_attempts")
    adapter = FakeExecutionAdapter(
        account=service.config.exchange.follower_account_address,
        positions=dict(follower_positions),
    )
    service.execution_adapter = adapter
    mutation_calls: list[str] = []

    def unexpected_mutation(name: str):
        def call(*_args, **_kwargs):
            mutation_calls.append(name)
            raise AssertionError(f"verified all-NOOP cycle must not call {name}")

        return call

    monkeypatch.setattr(adapter, "update_leverage", unexpected_mutation("update_leverage"))
    monkeypatch.setattr(adapter, "schedule_cancel", unexpected_mutation("schedule_cancel"))
    monkeypatch.setattr(adapter, "place_intent", unexpected_mutation("place_intent"))
    observed_ms = now_ms()
    source_snapshot = SourceSnapshot(
        positions=planned_result.desired_state.positions,
        open_orders=[],
        mids={"EWY": Decimal("108"), "xyz:KR200": Decimal("1042")},
        observed_ms=observed_ms,
        state_key="fresh-source-state",
        planning_key=planned_result.desired_state.source_event_key,
        raw_state={},
    )
    monkeypatch.setattr(service.observer, "reconcile_once", lambda: source_snapshot)
    monkeypatch.setattr(
        service,
        "load_asset_meta",
        lambda: {
            "EWY": AssetMeta("EWY", sz_decimals=3),
            "xyz:KR200": AssetMeta("xyz:KR200", sz_decimals=4),
        },
    )
    monkeypatch.setattr(service, "load_execution_mids", lambda: source_snapshot.mids)
    monkeypatch.setattr(
        service,
        "_current_follower_truth",
        lambda: (follower_positions, [], observed_ms),
    )
    monkeypatch.setattr(service, "_check_manual_intervention", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service_module.CopyEngine,
        "plan",
        lambda _engine, **_kwargs: planned_result,
    )
    prepare_calls: list[str] = []

    def unexpected_prepare(*_args, **_kwargs):
        prepare_calls.append("prepare")
        raise AssertionError("verified all-NOOP cycle must return before plan preparation")

    monkeypatch.setattr(store, "prepare_execution_plan", unexpected_prepare)

    cycle = service._run_once_with_lease(
        PreflightReport(mode=Mode.LIVE, passed=True),
    )

    assert prepare_calls == []
    assert cycle["execution_finalization"]["status"] == ("committed_baseline_noop_verified")
    assert cycle["desired_state_committed"] is False
    assert cycle["reports"] == []
    assert cycle["safe_mode"]["enabled"] is False
    assert service._execution_cycle_completed(cycle) is True
    assert mutation_calls == []
    assert adapter.reports == []
    assert adapter.schedule_cancel_reports == []
    assert adapter.leverage_updates == []
    assert store.count("follower_intents") == intent_count
    assert store.count("execution_reports") == report_count
    assert store.count("signed_action_attempts") == signed_action_count == 0


def test_live_run_cycle_preserves_hip3_deferrals_before_duplicate_noop_prepare(
    base_config,
    store,
    monkeypatch,
):
    service, planned_result, follower_positions, liquidity_deferrals = (
        _all_noop_with_hip3_liquidity_case(base_config, store)
    )
    full_target_positions = {
        **planned_result.desired_state.positions,
        "xyz:KR200": replace(
            follower_positions["xyz:KR200"],
            size=Decimal("-0.0254"),
        ),
        "xyz:SKHX": Position("xyz:SKHX", Decimal("-0.1"), leverage=1),
    }
    planned_result = replace(
        planned_result,
        desired_state=replace(
            planned_result.desired_state,
            positions=full_target_positions,
        ),
        intents=[
            *planned_result.intents,
            *(item.intent for item in liquidity_deferrals),
        ],
    )
    intent_count = store.count("follower_intents")
    report_count = store.count("execution_reports")
    signed_action_count = store.count("signed_action_attempts")
    observed_ms = now_ms()
    source_snapshot = SourceSnapshot(
        positions=full_target_positions,
        open_orders=[],
        mids={
            "EWY": Decimal("163.6"),
            "xyz:KR200": Decimal("1042"),
            "xyz:SKHX": Decimal("100"),
        },
        observed_ms=observed_ms,
        state_key="fresh-source-state-with-hip3-liquidity",
        planning_key=planned_result.desired_state.source_event_key,
        raw_state={},
    )
    adapter = FakeExecutionAdapter(
        account=service.config.exchange.follower_account_address,
        positions=dict(follower_positions),
    )
    service.execution_adapter = adapter
    monkeypatch.setattr(service.observer, "reconcile_once", lambda: source_snapshot)
    monkeypatch.setattr(
        service,
        "load_asset_meta",
        lambda: {
            "EWY": AssetMeta("EWY", sz_decimals=3),
            "xyz:KR200": AssetMeta("xyz:KR200", sz_decimals=4),
            "xyz:SKHX": AssetMeta("xyz:SKHX", sz_decimals=4),
        },
    )
    monkeypatch.setattr(service, "load_execution_mids", lambda: source_snapshot.mids)
    monkeypatch.setattr(
        service,
        "_current_follower_truth",
        lambda: (follower_positions, [], observed_ms),
    )
    monkeypatch.setattr(service, "_check_manual_intervention", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        service_module.CopyEngine,
        "plan",
        lambda _engine, **_kwargs: planned_result,
    )
    monkeypatch.setattr(
        service,
        "_admit_hip3_open_intents",
        lambda intents, *, asset_meta: (
            [intent for intent in intents if intent.action == IntentAction.NOOP],
            liquidity_deferrals,
            [],
        ),
    )
    prepare_calls: list[str] = []

    def unexpected_prepare(*_args, **_kwargs):
        prepare_calls.append("prepare")
        raise AssertionError("paced HIP-3 deferrals must return before NOOP preparation")

    monkeypatch.setattr(store, "prepare_execution_plan", unexpected_prepare)

    cycle, drain = service._run_once_until_deferred_opens_drained(
        cycle_runner=lambda: service._run_once_with_lease(
            PreflightReport(mode=Mode.LIVE, passed=True)
        )
    )

    assert prepare_calls == []
    assert cycle["safe_mode"]["enabled"] is False
    assert cycle["desired_state_committed"] is False
    assert cycle["reports"] == []
    assert [item["intent"]["coin"] for item in cycle["liquidity_deferred_intents"]] == [
        "xyz:KR200",
        "xyz:SKHX",
    ]
    assert service._execution_cycle_completed(cycle) is True
    assert service._source_reaction_run_completed(cycle) is False
    assert drain["status"] == "drained_with_liquidity_deferrals"
    retry = service._hip3_liquidity_retry_payload(cycle)
    assert retry is not None
    assert retry["retry_not_before_ms"] == liquidity_deferrals[0].retry_not_before_ms
    assert retry["coins"] == ["xyz:KR200", "xyz:SKHX"]
    assert adapter.reports == []
    assert adapter.schedule_cancel_reports == []
    assert adapter.leverage_updates == []
    assert store.count("follower_intents") == intent_count
    assert store.count("execution_reports") == report_count
    assert store.count("signed_action_attempts") == signed_action_count == 0


@pytest.mark.parametrize(
    ("scope_field", "scope_value"),
    [
        ("source_wallet", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
        ("action_account", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        ("source_network", SourceNetwork.TESTNET.value),
    ],
)
def test_all_noop_shortcut_refuses_matching_committed_truth_outside_exact_scope(
    base_config,
    store,
    scope_field,
    scope_value,
):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
        committed_scope_overrides={scope_field: scope_value},
    )
    unscoped_positions = store.latest_desired_positions(Mode.LIVE, committed_only=True)
    assert unscoped_positions is not None
    assert service._positions_match_exact(unscoped_positions, follower_positions)
    assert (
        store.latest_desired_positions(
            Mode.LIVE,
            source_wallet=service.config.source_wallet,
            action_account=service.config.exchange.follower_account_address,
            source_network=SourceNetwork.MAINNET.value,
            committed_only=True,
        )
        is None
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_uncovered_leverage_mutation(base_config, store):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    target_positions = dict(result.desired_state.positions)
    target_positions["EWY"] = replace(target_positions["EWY"], leverage=2)
    result = replace(
        result,
        desired_state=replace(result.desired_state, positions=target_positions),
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_any_actionable_member(base_config, store):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    actionable = replace(
        result.intents[0],
        action=IntentAction.OPEN,
        side="sell",
        size=Decimal("0.0001"),
        price=Decimal("1042"),
        status=IntentStatus.PENDING,
    )
    result = replace(result, intents=[actionable])
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_open_follower_order(base_config, store):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[
            OpenOrder(
                coin="EWY",
                side="sell",
                size=Decimal("0.001"),
                price=Decimal("108"),
                cloid="0xopen-order",
            )
        ],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_committed_checkpoint_mismatch(base_config, store):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    follower_positions = dict(follower_positions)
    follower_positions["xyz:KR200"] = replace(
        follower_positions["xyz:KR200"],
        size=Decimal("-0.0128"),
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_unresolved_signed_mutation(base_config, store):
    service, result, follower_positions = _all_noop_current_truth_case(
        base_config,
        store,
    )
    attempt_id = "0x" + "9" * 32
    assert store.prepare_signed_action_attempt(
        attempt_id=attempt_id,
        intent_id="leverage:EWY:1",
        cloid=attempt_id,
        action="update_leverage_cross",
        mode=Mode.LIVE,
        account=service.config.exchange.follower_account_address,
        network="mainnet",
        payload={"coin": "EWY", "leverage": 1},
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[],
    )

    assert cycle is None


@pytest.mark.parametrize(
    "mutation",
    ["overdue", "far_future", "wrong_stage", "non_hip3", "empty_blockers"],
)
def test_all_noop_shortcut_refuses_noncanonical_hip3_liquidity_deferral(
    base_config,
    store,
    mutation,
):
    service, result, follower_positions, liquidity_deferrals = _all_noop_with_hip3_liquidity_case(
        base_config, store
    )
    first = liquidity_deferrals[0]
    observed_ms = now_ms()
    if mutation == "overdue":
        first = replace(first, retry_not_before_ms=observed_ms - 1)
    elif mutation == "far_future":
        # Leave enough margin that time spent entering the production verifier cannot move this
        # deliberately invalid deadline onto the exact permitted retry boundary.
        first = replace(
            first,
            retry_not_before_ms=observed_ms + service_module.HIP3_LIQUIDITY_RETRY_MS + 10_000,
        )
    elif mutation == "wrong_stage":
        first = replace(first, stage="signed_ioc_zero_fill_open")
    elif mutation == "non_hip3":
        first = replace(first, intent=replace(first.intent, coin="BTC"))
    else:
        first = replace(first, blockers=())
    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[],
        liquidity_deferred_intents=[first, *liquidity_deferrals[1:]],
    )

    assert cycle is None


def test_all_noop_shortcut_refuses_ordinary_deferred_work_beside_hip3_retry(
    base_config,
    store,
):
    service, result, follower_positions, liquidity_deferrals = _all_noop_with_hip3_liquidity_case(
        base_config, store
    )
    observed_ms = now_ms()

    cycle = service._verified_all_noop_current_truth_cycle(
        preflight=PreflightReport(mode=Mode.LIVE, passed=True),
        source_positions=result.desired_state.positions,
        source_observed_ms=observed_ms,
        result=result,
        follower_positions=follower_positions,
        follower_open_orders=[],
        follower_observed_ms=observed_ms,
        manual_ok=True,
        deferred_intents=[liquidity_deferrals[0].intent],
        liquidity_deferred_intents=liquidity_deferrals,
    )

    assert cycle is None


def test_completed_source_reaction_noop_is_explicitly_skipped(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = {
        **_completed_cycle_result(),
        "desired_state": {"reason": "follower already matches current source truth"},
    }

    disposition = service._completed_source_reaction_disposition(result)

    assert disposition["disposition"] == "skipped"
    assert "no concrete follower action" in disposition["reason"]
    assert "follower already matches" in disposition["reason"]
    assert disposition["execution_evidence"] == []


def test_completed_source_reaction_requires_filled_execution_to_be_copied(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    intent = {
        "intent_id": "intent-open",
        "cloid": "0xabc",
        "action": "open",
        "coin": "BTC",
        "size": "0.001",
        "reduce_only": False,
        "reason": "copy source BTC exposure",
    }

    acknowledged = service._completed_source_reaction_disposition(
        {
            **_completed_cycle_result(),
            "intents": [intent],
            "reports": [
                {
                    "report_id": "report-ack",
                    "intent_id": "intent-open",
                    "cloid": "0xabc",
                    "status": "acked",
                    "exchange_status": "open",
                }
            ],
        }
    )
    canceled = service._completed_source_reaction_disposition(
        {
            **_completed_cycle_result(),
            "intents": [intent],
            "reports": [
                {
                    "report_id": "report-canceled",
                    "intent_id": "intent-open",
                    "cloid": "0xabc",
                    "status": "canceled",
                    "exchange_status": "canceled",
                }
            ],
        }
    )
    filled = service._completed_source_reaction_disposition(
        {
            **_completed_cycle_result(),
            "intents": [intent],
            "reports": [
                {
                    "report_id": "report-filled",
                    "intent_id": "intent-open",
                    "cloid": "0xabc",
                    "status": "filled",
                    "exchange_status": "filled",
                    "filled_size": "0.001",
                }
            ],
        }
    )

    assert acknowledged["disposition"] == "skipped"
    assert canceled["disposition"] == "skipped"
    assert filled["disposition"] == "copied"
    assert filled["execution_evidence"] == [
        {
            "intent_id": "intent-open",
            "cloid": "0xabc",
            "action": "open",
            "coin": "BTC",
            "side": "",
            "reduce_only": False,
            "execution_status": "filled",
            "exchange_status": "filled",
            "filled_size": "0.001",
            "report_id": "report-filled",
        }
    ]


def test_completed_source_reaction_never_credits_cancel_or_empty_fill(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    cancel_intent = {
        "intent_id": "intent-cancel",
        "cloid": "0xcancel",
        "action": "cancel",
        "coin": "BTC",
        "size": "0",
        "reduce_only": True,
    }
    open_intent = {
        "intent_id": "intent-open-empty",
        "cloid": "0xempty",
        "action": "open",
        "coin": "BTC",
        "size": "0.001",
        "reduce_only": False,
    }

    for intent, filled_size in ((cancel_intent, "0.001"), (open_intent, "0")):
        disposition = service._completed_source_reaction_disposition(
            {
                **_completed_cycle_result(),
                "intents": [intent],
                "reports": [
                    {
                        "report_id": "report-non-exposure",
                        "intent_id": intent["intent_id"],
                        "cloid": intent["cloid"],
                        "status": "filled",
                        "exchange_status": "filled",
                        "filled_size": filled_size,
                    }
                ],
            }
        )

        assert disposition["disposition"] == "skipped"
        assert disposition["execution_evidence"] == []


def test_react_to_source_event_runs_validated_cycle_for_actionable_event(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []

    def run_once() -> dict:
        calls.append("run")
        return {"preflight": {"passed": True}, "intents": []}

    _set_run_once(service, run_once)

    reaction = service.react_to_source_event(_event("fill", SourceEventType.FILL, "fill"))

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["source_event_key"] == "fill"


def test_react_to_empty_position_snapshot_still_validates_flat_source(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, _completed_cycle_result()))
    event = SourceEvent(
        idempotency_key="flat-position-snapshot",
        event_type=SourceEventType.POSITION,
        exchange_ts_ms=1000,
        observed_ts_ms=1001,
        payload={"event_subtype": "position_snapshot", "event_count": 0, "positions": []},
    )

    reaction = service.react_to_source_event(event)

    assert calls == ["run"]
    assert reaction["action"] == "run_once"


def test_source_reaction_refreshes_safe_mode_cleared_by_another_service(base_config, store):
    follower = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    operator = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    follower.safe_mode.trip(SafeModeReason.STALE_SOURCE, "stale before operator review")
    operator.safe_mode.refresh_from_store()
    operator.safe_mode.clear("operator completed reconcile")
    assert follower.safe_mode.enabled is True
    calls: list[str] = []
    _set_run_once(follower, lambda: _record_run(calls, _completed_cycle_result()))

    reaction = follower.react_to_source_event(
        _event("fill-after-clear", SourceEventType.FILL, "fill")
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert follower.safe_mode.enabled is False


def test_react_to_source_event_ignores_non_exposure_event(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls))

    reaction = service.react_to_source_event(
        _event("open-order", SourceEventType.OPEN_ORDER, "open_order_snapshot")
    )

    assert calls == []
    assert reaction["action"] == "ignored"


def test_react_to_source_event_runs_for_filled_order_update(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "filled-order-update",
            SourceEventType.OPEN_ORDER,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "order_update:filled",
                "statuses": ["filled"],
                "event_count": 1,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "order_update:filled"


def test_react_to_source_event_runs_for_mixed_order_update_with_fill(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "mixed-order-update",
            SourceEventType.OPEN_ORDER,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "order_update:filled,open",
                "statuses": ["filled", "open"],
                "event_count": 2,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "order_update:filled,open"


def test_react_to_source_event_runs_for_rejected_order_update(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "rejected-order-update",
            SourceEventType.OPEN_ORDER,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "order_update:perpmarginrejected",
                "event_count": 1,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "order_update:perpmarginrejected"


def test_react_to_source_event_runs_for_triggered_order_update(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "triggered-order-update",
            SourceEventType.OPEN_ORDER,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "order_update:triggered",
                "statuses": ["triggered"],
                "event_count": 1,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "order_update:triggered"


def test_react_to_source_event_runs_for_triggered_order_update_from_subtype(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "triggered-order-update-subtype",
            SourceEventType.OPEN_ORDER,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "order_update:triggered",
                "event_count": 1,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "order_update:triggered"


def test_react_to_source_event_ignores_account_state_snapshots(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls))

    for subtype in (
        "account_notification",
        "web_data_snapshot",
        "spot_state_snapshot",
        "all_dexs_position_snapshot",
    ):
        reaction = service.react_to_source_event(_event(subtype, SourceEventType.SNAPSHOT, subtype))
        assert reaction["action"] == "ignored"

    assert calls == []


def test_react_to_source_event_ignores_empty_snapshots(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls))

    reaction = service.react_to_source_event(
        SourceEvent(
            "empty-fill-snapshot",
            SourceEventType.FILL,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "fill_snapshot",
                "is_snapshot": True,
                "event_count": 0,
            },
        )
    )

    assert calls == []
    assert reaction["action"] == "ignored"


def test_react_to_source_event_ignores_zero_count_fill_event(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls))

    reaction = service.react_to_source_event(
        SourceEvent(
            "empty-user-event-fills",
            SourceEventType.FILL,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={"event_subtype": "fill", "event_count": 0},
        )
    )

    assert calls == []
    assert reaction["action"] == "ignored"


def test_react_to_source_event_runs_for_twap_fill_and_ledger_liquidation(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    twap_fill = service.react_to_source_event(
        _event("twap-fill", SourceEventType.FILL, "twap_slice_fill")
    )
    ledger_liquidation = service.react_to_source_event(
        _event("ledger-liquidation", SourceEventType.POSITION, "ledger_update:liquidation")
    )

    assert calls == ["run", "run"]
    assert twap_fill["action"] == "run_once"
    assert ledger_liquidation["action"] == "run_once"


def test_react_to_source_event_runs_for_terminal_twap_history(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    for status in ("finished", "terminated", "error"):
        reaction = service.react_to_source_event(
            SourceEvent(
                f"twap-{status}",
                SourceEventType.SNAPSHOT,
                exchange_ts_ms=1000,
                observed_ts_ms=1001,
                payload={
                    "event_subtype": f"twap_history:{status}",
                    "statuses": [status],
                    "event_count": 1,
                },
            )
        )
        assert reaction["action"] == "run_once"
        assert reaction["event_subtype"] == f"twap_history:{status}"

    assert calls == ["run", "run", "run"]


def test_react_to_source_event_runs_for_account_class_transfer_ledger_update(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "account-class-transfer",
            SourceEventType.SNAPSHOT,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "ledger_update:accountclasstransfer",
                "ledger_types": ["accountclasstransfer"],
                "event_count": 1,
            },
        )
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"
    assert reaction["event_subtype"] == "ledger_update:accountclasstransfer"


def test_react_to_source_event_ignores_activated_twap_history(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        SourceEvent(
            "twap-activated",
            SourceEventType.SNAPSHOT,
            exchange_ts_ms=1000,
            observed_ts_ms=1001,
            payload={
                "event_subtype": "twap_history:activated",
                "statuses": ["activated"],
                "event_count": 1,
            },
        )
    )

    assert calls == []
    assert reaction["action"] == "ignored"


def test_react_to_source_event_runs_for_active_asset_data(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))

    reaction = service.react_to_source_event(
        _event("active-asset-data", SourceEventType.LEVERAGE, "active_asset_data")
    )

    assert calls == ["run"]
    assert reaction["action"] == "run_once"


def test_react_to_source_event_skips_when_safe_mode_is_active(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(service, lambda: _record_run(calls, {"intents": []}))
    service.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, "operator reconcile required")

    reaction = service.react_to_source_event(_event("fill", SourceEventType.FILL, "fill"))

    assert calls == []
    assert reaction["action"] == "skipped"
    assert reaction["safe_mode"]["reason"] == SafeModeReason.MISSED_EVENT_GAP.value
    assert "requires reconcile" in reaction["detail"]


def test_follow_source_websocket_queues_inserted_actionable_events(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    actionable = _event("fill", SourceEventType.FILL, "fill")
    ignored = _event("open-order", SourceEventType.OPEN_ORDER, "open_order_snapshot")
    duplicate = _event("dup", SourceEventType.FILL, "fill")
    calls: list[str] = []

    def run_once() -> dict:
        calls.append("run")
        return _completed_cycle_result()

    async def fake_observe(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 3
        await on_event(ignored, service.observer.record_source_event(ignored))
        await on_event(actionable, service.observer.record_source_event(actionable))
        service.observer.record_source_event(duplicate)
        store.finish_source_reactions(
            [duplicate.idempotency_key],
            status="completed",
            outcome={"validated": True},
        )
        await on_event(duplicate, service.observer.record_source_event(duplicate))

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)
    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=3))

    assert calls == ["run", "run"]
    assert result["stats"] == {
        "observed_events": 3,
        "duplicate_events": 1,
        "ignored_events": 1,
        "scheduled_reactions": 1,
        "completed_reactions": 1,
        "failed_reactions": 0,
        "skipped_reactions": 0,
        "validation_runs": 1,
        "coalesced_reactions": 0,
        "queue_overflows": 0,
        "websocket_attempts": 1,
        "websocket_errors": 0,
        "websocket_reconnects": 0,
        "stale_source_recoveries": 0,
        "liquidity_retry_wakeups": 0,
    }
    assert result["startup_backfill"]["fetched"] == 0
    assert result["disconnect_backfill"] is None
    assert result["disconnect_recoveries"] == []
    assert result["backfill_reactions"] == []
    assert result["reactions"][0]["source_event_key"] == "fill"
    assert result["reactions"][0]["action"] == "run_once"
    assert result["reactions"][0]["batched_event_count"] == 1


def test_follow_source_websocket_coalesces_actionable_burst(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    events = [
        _event("fill", SourceEventType.FILL, "fill"),
        _event("cancel", SourceEventType.CANCEL, "non_user_cancel"),
        _event("leverage", SourceEventType.LEVERAGE, "active_asset_data"),
    ]
    calls: list[str] = []

    def run_once() -> dict:
        calls.append("run")
        return _completed_cycle_result()

    async def fake_observe(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 3
        for event in events:
            await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)
    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=3))

    assert calls == ["run", "run"]
    assert result["stats"]["scheduled_reactions"] == 3
    assert result["stats"]["completed_reactions"] == 3
    assert result["stats"]["validation_runs"] == 1
    assert result["stats"]["coalesced_reactions"] == 2
    assert len(result["reactions"]) == 1
    assert result["reactions"][0]["source_event_keys"] == ["fill", "cancel", "leverage"]
    assert result["reactions"][0]["event_subtypes"] == [
        "fill",
        "non_user_cancel",
        "active_asset_data",
    ]
    assert result["reactions"][0]["batched_event_count"] == 3


def test_follow_source_websocket_ignores_repeated_copy_signal(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    events = [
        SourceEvent(
            idempotency_key=f"flat-position-{index}",
            event_type=SourceEventType.POSITION,
            exchange_ts_ms=1000 + index,
            observed_ts_ms=1000 + index,
            payload={
                "event_subtype": "position_snapshot",
                "event_count": 0,
                "copy_signal_key": "same-flat-exposure",
            },
        )
        for index in range(2)
    ]
    calls: list[str] = []

    async def fake_observe(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 2
        for event in events:
            await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)
    _set_run_once(
        service,
        lambda: _record_run(calls, _completed_cycle_result()),
    )

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    assert calls == ["run", "run"]
    assert result["stats"]["scheduled_reactions"] == 1
    assert result["stats"]["ignored_events"] == 1
    assert store.source_reaction_status(events[0].idempotency_key) == "completed"
    assert store.source_reaction_status(events[1].idempotency_key) == "ignored"


def test_follow_source_websocket_drains_blocked_backlog_after_safe_mode_clear(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    blocked = SourceEvent(
        idempotency_key="blocked-flat-position",
        event_type=SourceEventType.POSITION,
        exchange_ts_ms=1000,
        observed_ts_ms=1000,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 0,
            "copy_signal_key": "same-flat-exposure",
            "source_wallet": base_config.source_wallet,
        },
    )
    wake = SourceEvent(
        idempotency_key="trusted-flat-position",
        event_type=SourceEventType.POSITION,
        exchange_ts_ms=2000,
        observed_ts_ms=2000,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 0,
            "copy_signal_key": "same-flat-exposure",
            "source_wallet": base_config.source_wallet,
        },
    )
    calls: list[str] = []

    async def fake_observe(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 1
        service.observer.record_source_event(blocked)
        store.finish_source_reactions(
            [blocked.idempotency_key],
            status="blocked",
            outcome={"reason": "safe mode"},
        )
        await on_event(wake, service.observer.record_source_event(wake))

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)
    service._last_source_copy_signal_by_subtype["position_snapshot"] = "same-flat-exposure"
    _set_run_once(
        service,
        lambda: _record_run(calls, _completed_cycle_result()),
    )

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert calls == ["run", "run"]
    assert result["stats"]["validation_runs"] == 1
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"
    assert store.source_reaction_status(wake.idempotency_key) == "completed"
    assert store.blocked_source_reaction_count(source_wallet=base_config.source_wallet) == 0


def test_follow_source_websocket_queue_overflow_trips_safe_mode(base_config, store, monkeypatch):
    config = replace(base_config, ops=OpsConfig(source_reaction_queue_size=1))
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    first = _event("fill-1", SourceEventType.FILL, "fill")
    second = _event("fill-2", SourceEventType.FILL, "fill")
    calls: list[str] = []

    async def fake_observe(stop_after_messages=None, on_event=None):
        await on_event(first, service.observer.record_source_event(first))
        await on_event(second, service.observer.record_source_event(second))

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)

    def run_once():
        calls.append("run")
        return {}

    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    # Startup current-exposure adoption is intentionally outside event stats;
    # overflow then blocks both queued event reactions without another cycle.
    assert calls == ["run"]
    assert result["stats"]["queue_overflows"] == 1
    assert result["stats"]["scheduled_reactions"] == 1
    assert result["stats"]["failed_reactions"] == 1
    # The overflowed event is durably failed, then the same worker sees both open
    # obligations and records that safe mode blocked validation for each of them.
    assert result["stats"]["skipped_reactions"] == 2
    assert result["stats"]["validation_runs"] == 0
    dropped = next(
        reaction for reaction in result["reactions"] if reaction["source_event_key"] == "fill-2"
    )
    skipped = next(reaction for reaction in result["reactions"] if reaction["action"] == "skipped")
    assert dropped["action"] == "failed"
    assert dropped["failure_reason"] == "queue_overflow"
    assert dropped["safe_mode"]["reason"] == SafeModeReason.MISSED_EVENT_GAP.value
    assert skipped["action"] == "skipped"
    assert skipped["source_event_keys"] == ["fill-1", "fill-2"]
    assert result["safe_mode"]["reason"] == SafeModeReason.MISSED_EVENT_GAP.value


def test_follow_source_websocket_records_failed_reaction_with_safe_mode(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    first = _event("fill-1", SourceEventType.FILL, "fill")
    second = _event("cancel-1", SourceEventType.CANCEL, "non_user_cancel")

    async def fake_observe(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 2
        await on_event(first, service.observer.record_source_event(first))
        await on_event(second, service.observer.record_source_event(second))

    def fail_reaction(events):
        raise RuntimeError(f"boom {len(events)}")

    monkeypatch.setattr(service.observer, "observe_websocket", fake_observe)
    monkeypatch.setattr(service, "react_to_source_events", fail_reaction)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    assert result["stats"]["scheduled_reactions"] == 2
    assert result["stats"]["failed_reactions"] == 2
    assert result["stats"]["validation_runs"] == 0
    assert result["stats"]["coalesced_reactions"] == 1
    reaction = result["reactions"][0]
    assert reaction["action"] == "failed"
    assert reaction["failure_reason"] == "reaction_exception"
    assert reaction["source_event_keys"] == ["fill-1", "cancel-1"]
    assert reaction["event_subtypes"] == ["fill", "non_user_cancel"]
    assert reaction["batched_event_count"] == 2
    assert reaction["error"] == "boom 2"
    assert reaction["safe_mode"]["reason"] == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE.value
    assert result["safe_mode"]["reason"] == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE.value


def test_backfill_source_fills_uses_latest_fill_with_overlap(base_config, store):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_fill_backfill_lookback_ms=10_000,
            source_fill_backfill_overlap_ms=250,
        ),
    )
    fake = FakeInfoClient()
    fake.fills = [{"time": 1000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    service = CopyTraderService(config, store=store, info_client=fake)

    first = service.backfill_source_fills(start_time_ms=900, end_time_ms=1100)
    fake.fills.append({"time": 1500, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 2})
    second = service.backfill_source_fills(end_time_ms=1600)

    assert first["inserted"] == 1
    assert second["inserted"] == 1
    fill_calls = [call for call in fake.calls if call["type"] == "userFillsByTime"]
    assert fill_calls[-1]["startTime"] == 750
    assert fill_calls[-1]["endTime"] == 1600
    assert second["fills"]["inserted"] == 1
    assert second["twap_slice_fills"]["inserted"] == 0


def test_backfill_source_fills_recovers_late_overlap_fill_without_safe_mode(base_config, store):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_fill_backfill_lookback_ms=10_000,
            source_fill_backfill_overlap_ms=500,
        ),
    )
    fake = FakeInfoClient()
    fake.fills = [{"time": 2000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    service = CopyTraderService(config, store=store, info_client=fake)

    first = service.backfill_source_fills(start_time_ms=1750, end_time_ms=2100)
    fake.fills.append({"time": 1800, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 2})
    second = service.backfill_source_fills(end_time_ms=2200)

    assert first["inserted"] == 1
    assert second["fills"]["inserted"] == 1
    assert second["fills"]["duplicates"] == 1
    assert second["inserted"] == 1
    assert second["warnings"] == []
    assert service.safe_mode.reason == SafeModeReason.NONE


def test_backfill_source_fills_uses_independent_twap_slice_overlap(base_config, store):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_fill_backfill_lookback_ms=10_000,
            source_fill_backfill_overlap_ms=250,
        ),
    )
    fake = FakeInfoClient()
    fake.fills = [{"time": 2000, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    fake.twap_slice_fills = [
        {
            "twapId": 42,
            "fill": {"time": 1000, "coin": "ETH", "oid": 2, "hash": "0xbbb", "tid": 2},
        }
    ]
    service = CopyTraderService(config, store=store, info_client=fake)

    first = service.backfill_source_fills(start_time_ms=900, end_time_ms=2100)
    fake.twap_slice_fills.append(
        {
            "twapId": 43,
            "fill": {"time": 1500, "coin": "SOL", "oid": 3, "hash": "0xccc", "tid": 3},
        }
    )
    second = service.backfill_source_fills(end_time_ms=3000)

    twap_calls = [call for call in fake.calls if call["type"] == "userTwapSliceFillsByTime"]
    fill_calls = [call for call in fake.calls if call["type"] == "userFillsByTime"]
    assert first["inserted"] == 2
    assert second["inserted"] == 1
    assert fill_calls[-1]["startTime"] == 1750
    assert twap_calls[-1]["startTime"] == 750
    assert second["fills"]["inserted"] == 0
    assert second["twap_slice_fills"]["inserted"] == 1


def test_follow_source_websocket_backfills_after_disconnect(base_config, store, monkeypatch):
    config = replace(
        base_config,
        ops=OpsConfig(source_websocket_reconnect_attempts=0),
    )
    fill_time = 10_000
    monkeypatch.setattr("hyperliquid_copytrader.service.now_ms", lambda: fill_time + 100)
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: fill_time + 100)
    fake = FakeInfoClient()
    fake.fills = [{"time": fill_time, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    service = CopyTraderService(config, store=store, info_client=fake)

    async def disconnected(stop_after_messages=None, on_event=None):
        raise RuntimeError("socket closed")

    monkeypatch.setattr(service.observer, "observe_websocket", disconnected)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert result["stats"]["websocket_errors"] == 1
    assert result["stats"]["websocket_reconnects"] == 0
    assert result["websocket_error"] == "socket closed"
    assert result["startup_backfill"]["inserted"] == 1
    assert result["disconnect_backfill"]["duplicates"] == 1
    assert (
        result["disconnect_recoveries"][0]["auto_resume_skipped"] == "no reconnect attempt remains"
    )
    assert result["safe_mode"]["reason"] == SafeModeReason.WEBSOCKET_DISCONNECT.value


def test_follow_source_websocket_validates_recovered_startup_backfill(
    base_config, store, monkeypatch
):
    fill_time = 10_000
    monkeypatch.setattr("hyperliquid_copytrader.service.now_ms", lambda: fill_time + 100)
    fake = FakeInfoClient()
    fake.fills = [{"time": fill_time, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    service = CopyTraderService(base_config, store=store, info_client=fake)
    calls: list[str] = []

    async def quiet_observe(stop_after_messages=None, on_event=None):
        return None

    _set_run_once(
        service,
        lambda: _record_run(
            calls,
            _completed_cycle_result(),
        ),
    )
    monkeypatch.setattr(service.observer, "observe_websocket", quiet_observe)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=0))

    assert calls == ["run", "run"]
    assert result["startup_backfill"]["inserted"] == 1
    assert result["backfill_reactions"][0]["stage"] == "startup_backfill"
    assert result["backfill_reactions"][0]["action"] == "run_once"
    assert result["backfill_reactions"][0]["inserted"] == 1


def test_zero_backfill_adopts_nonflat_source_before_websocket_running_barrier(
    base_config, store, monkeypatch
):
    service, adapter, _, prior = _live_flat_recovery_service(base_config, store, monkeypatch)
    store.finish_source_reactions(
        [prior.idempotency_key],
        status="completed",
        outcome={"reason": "startup fixture has no pending source reaction"},
    )
    adapter.forced_status = IntentStatus.FILLED
    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    stages: list[str] = []

    async def observe_after_adoption(stop_after_messages=None, on_event=None):
        stages.append("websocket")
        startup = service.source_follow_startup_sync_status()
        assert startup["ready"] is True
        assert startup["startup_cycle_complete"] is True
        assert startup["deferred_open_drain"]["status"] == "drained"
        assert adapter.positions["BTC"].size == Decimal("0.005")
        assert len([report for report in adapter.reports if report.payload.get("intent")]) == 1

    monkeypatch.setattr(service.observer, "observe_websocket", observe_after_adoption)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert stages == ["websocket"]
    assert result["startup_backfill"]["inserted"] == 0
    assert result["stats"]["validation_runs"] == 0
    audit = store.recent("control_audit", 1)[0]
    assert audit["control"] == "source_follow_startup_sync"
    assert audit["status"] == "ready"


@pytest.mark.parametrize("safe_mode_active", [False, True])
def test_startup_running_barrier_blocks_incomplete_or_safe_adoption(
    base_config, store, monkeypatch, safe_mode_active
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    if safe_mode_active:
        service.safe_mode.trip(SafeModeReason.RISK_LIMIT, "startup safety fixture")
        adoption_result = _completed_cycle_result()
    else:
        adoption_result = {}
    _set_run_once(service, lambda: adoption_result)

    async def observe_only_after_blocked_barrier(stop_after_messages=None, on_event=None):
        startup = service.source_follow_startup_sync_status()
        assert startup["ready"] is False
        assert startup["stage"] == "blocked"

    monkeypatch.setattr(
        service.observer,
        "observe_websocket",
        observe_only_after_blocked_barrier,
    )

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=0))

    assert service.source_follow_startup_sync_status()["ready"] is False
    assert result["stats"]["validation_runs"] == 0
    audit = store.recent("control_audit", 1)[0]
    assert audit["control"] == "source_follow_startup_sync"
    assert audit["status"] == "blocked"


def test_startup_running_barrier_allows_explicit_liquidity_deferrals(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    retry_not_before_ms = now_ms() + 60_000
    adoption_result = _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms)
    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    _set_run_once(service, lambda: adoption_result)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=0))

    startup = service.source_follow_startup_sync_status()
    assert startup["ready"] is True
    assert startup["stage"] == "ready_with_liquidity_deferrals"
    assert startup["startup_cycle_complete"] is True
    assert startup["startup_fully_synced"] is False
    assert startup["safe_mode"]["enabled"] is False
    assert startup["deferred_open_drain"]["status"] == "drained_with_liquidity_deferrals"
    assert (
        startup["adoption_result"]["liquidity_deferred_intents"]
        == adoption_result["liquidity_deferred_intents"]
    )
    obligation = startup["startup_liquidity_obligation"]
    assert obligation["source_event_key"]
    assert obligation["inserted"] is True
    assert obligation["outcome_updated"] is True
    assert obligation["retry"] == {
        "class": "hip3_liquidity",
        "disposition": "deferred",
        "retry_not_before_ms": retry_not_before_ms,
        "retry_interval_ms": 60_000,
        "coins": ["xyz:KR200"],
        "deferral_count": 1,
    }
    assert startup["unfinished_source_reactions"] == 1
    assert startup["source_reaction_retry_counts"] == {
        "hip3_liquidity_waiting": 1,
        "hip3_liquidity_due": 0,
        "other_blocking_unfinished": 0,
    }
    assert result["stats"]["validation_runs"] == 0
    audit = store.recent("control_audit", 1)[0]
    assert audit["control"] == "source_follow_startup_sync"
    assert audit["status"] == "ready"


def test_blocked_startup_promotes_after_completed_source_reaction(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    event = _event("startup-promotion-reaction", SourceEventType.FILL, "fill")
    results = iter([{}, _completed_cycle_result()])
    calls: list[str] = []

    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )

    def run_once() -> dict[str, Any]:
        calls.append("run")
        return next(results)

    _set_run_once(service, run_once)

    async def observe_completed_reaction(stop_after_messages=None, on_event=None):
        startup = service.source_follow_startup_sync_status()
        assert startup["ready"] is False
        assert startup["stage"] == "blocked"
        await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(
        service.observer,
        "observe_websocket",
        observe_completed_reaction,
    )

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    startup = service.source_follow_startup_sync_status()
    assert calls == ["run", "run"]
    assert result["stats"]["validation_runs"] == 1
    assert store.source_reaction_status(event.idempotency_key) == "completed"
    assert startup["ready"] is True
    assert startup["stage"] == "ready"
    assert startup["startup_cycle_complete"] is True
    assert startup["startup_fully_synced"] is True
    assert startup["promotion_trigger"] == "source_reaction"


def test_blocked_startup_promotes_typed_liquidity_deferral_with_durable_retry(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    event = _event("startup-promotion-liquidity", SourceEventType.FILL, "fill")
    retry_not_before_ms = now_ms() + 60_000
    deferred_result = _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms)
    results = iter([{}, deferred_result])

    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    _set_run_once(service, lambda: next(results))

    async def observe_liquidity_deferral(stop_after_messages=None, on_event=None):
        await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(
        service.observer,
        "observe_websocket",
        observe_liquidity_deferral,
    )

    asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    startup = service.source_follow_startup_sync_status()
    row = next(
        item
        for item in store.source_reaction_rows()
        if item["source_event_key"] == event.idempotency_key
    )
    outcome = json.loads(row["outcome_json"])
    assert row["status"] == "blocked"
    assert outcome["retry"] == {
        "class": "hip3_liquidity",
        "disposition": "deferred",
        "retry_not_before_ms": retry_not_before_ms,
        "retry_interval_ms": 60_000,
        "coins": ["xyz:KR200"],
        "deferral_count": 1,
    }
    assert startup["ready"] is True
    assert startup["stage"] == "ready_with_liquidity_deferrals"
    assert startup["startup_cycle_complete"] is True
    assert startup["startup_fully_synced"] is False
    assert startup["source_reaction_retry_counts"] == {
        "hip3_liquidity_waiting": 1,
        "hip3_liquidity_due": 0,
        "other_blocking_unfinished": 0,
    }


def test_startup_promotion_requires_durable_typed_liquidity_retry(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    malformed_deferral = _liquidity_deferred_cycle_result(retry_not_before_ms=now_ms() + 60_000)
    malformed_deferral["liquidity_deferred_intents"][0].pop("retry_not_before_ms")

    promoted = service._maybe_promote_source_follow_startup_sync(
        malformed_deferral,
        {"status": "drained_with_liquidity_deferrals"},
        trigger="source_reaction",
    )

    assert promoted is False
    assert service.source_follow_startup_sync_status()["ready"] is False
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 0


def test_startup_promotion_rejects_incomplete_unsafe_and_generic_blockers(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    assert (
        service._maybe_promote_source_follow_startup_sync(
            {},
            None,
            trigger="source_reaction",
        )
        is False
    )

    service.safe_mode.trip(SafeModeReason.RISK_LIMIT, "unsafe startup fixture")
    assert (
        service._maybe_promote_source_follow_startup_sync(
            _completed_cycle_result(),
            {"status": "drained"},
            trigger="source_reaction",
        )
        is False
    )
    service.safe_mode.clear("continue startup blocker test")

    generic = replace(
        _event("startup-generic-blocker", SourceEventType.FILL, "fill"),
        source_wallet=base_config.source_wallet,
    )
    assert store.append_source_event(generic, reaction_required=True)
    assert (
        store.finish_source_reactions(
            [generic.idempotency_key],
            status="blocked",
            outcome={"reason": "generic unfinished source reaction"},
        )
        == 1
    )
    assert (
        service._maybe_promote_source_follow_startup_sync(
            _completed_cycle_result(),
            {"status": "drained"},
            trigger="source_reaction",
        )
        is False
    )
    startup = service.source_follow_startup_sync_status()
    assert startup["ready"] is False
    assert (
        store.source_reaction_retry_counts(
            source_wallet=base_config.source_wallet,
            retry_due_ms=now_ms(),
        )["other_blocking_unfinished"]
        == 1
    )


def test_startup_readiness_promotion_is_monotonic(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    assert service._maybe_promote_source_follow_startup_sync(
        _completed_cycle_result(),
        {"status": "drained"},
        trigger="source_reaction",
    )
    ready = service.source_follow_startup_sync_status()

    service.safe_mode.trip(SafeModeReason.STALE_SOURCE, "later transient source change")
    assert (
        service._maybe_promote_source_follow_startup_sync(
            {},
            None,
            trigger="source_reaction",
        )
        is False
    )
    assert service.source_follow_startup_sync_status() == ready


def test_startup_promotion_finalization_failure_keeps_reaction_worker_alive(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    first = _event("startup-promotion-finalization-failure", SourceEventType.FILL, "fill")
    second = _event("startup-promotion-after-finalization-failure", SourceEventType.FILL, "fill")
    results = iter([{}, _completed_cycle_result(), _completed_cycle_result()])

    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    _set_run_once(service, lambda: next(results))
    original_promote = service._maybe_promote_source_follow_startup_sync
    promotion_calls = 0

    def fail_first_promotion(*args, **kwargs):
        nonlocal promotion_calls
        promotion_calls += 1
        if promotion_calls == 1:
            raise RuntimeError("startup promotion audit write failed")
        return original_promote(*args, **kwargs)

    monkeypatch.setattr(
        service,
        "_maybe_promote_source_follow_startup_sync",
        fail_first_promotion,
    )

    async def observe_two_events(stop_after_messages=None, on_event=None):
        await on_event(first, service.observer.record_source_event(first))
        for _ in range(100):
            if service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE:
                break
            await asyncio.sleep(0)
        assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
        service.safe_mode.clear("test acknowledges startup promotion finalization failure")
        await on_event(second, service.observer.record_source_event(second))

    monkeypatch.setattr(service.observer, "observe_websocket", observe_two_events)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    startup = service.source_follow_startup_sync_status()
    assert promotion_calls == 2
    assert result["stats"]["failed_reactions"] == 1
    assert result["stats"]["completed_reactions"] == 1
    assert result["reactions"][0]["failure_reason"] == "reaction_finalization_exception"
    assert store.source_reaction_status(first.idempotency_key) == "completed"
    assert store.source_reaction_status(second.idempotency_key) == "completed"
    assert startup["ready"] is True
    assert startup["promotion_trigger"] == "source_reaction"


def test_safe_active_startup_event_recovers_and_promotes_canonical_cycle(
    base_config, store, monkeypatch
):
    service, adapter, _, prior = _live_flat_recovery_service(base_config, store, monkeypatch)
    store.finish_source_reactions(
        [prior.idempotency_key],
        status="completed",
        outcome={"reason": "startup recovery fixture"},
    )
    adapter.forced_status = IntentStatus.FILLED
    event = replace(
        _event("startup-stale-source-recovery", SourceEventType.FILL, "fill"),
        source_wallet=service.config.source_wallet,
    )
    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    _set_run_once(service, lambda: {})

    async def observe_stale_source_event(stop_after_messages=None, on_event=None):
        startup = service.source_follow_startup_sync_status()
        assert startup["ready"] is False
        assert startup["stage"] == "blocked"
        service.safe_mode.trip(
            SafeModeReason.STALE_SOURCE,
            "source changed before signed leverage dispatch",
        )
        await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(
        service.observer,
        "observe_websocket",
        observe_stale_source_event,
    )

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    startup = service.source_follow_startup_sync_status()
    recovery = result["disconnect_recoveries"][0]
    assert result["stats"]["stale_source_recoveries"] == 1
    assert recovery["trigger"] == "stale_source_reaction"
    assert recovery["bounded_startup_adoption"] is True
    assert recovery["recovery_mode"] == "startup_bounded_adoption"
    assert recovery["auto_resume"]["cleared"] is True
    assert recovery["containment_cycle"]["desired_state_committed"] is True
    assert store.source_reaction_status(event.idempotency_key) == "completed"
    assert startup["ready"] is True
    assert startup["stage"] == "ready"
    assert startup["promotion_trigger"] == "stale_source_recovery"
    assert startup["startup_recovery"]["trigger"] == "stale_source_reaction"
    assert service.safe_mode.enabled is False


def test_repeated_startup_reuses_and_deduplicates_internal_liquidity_retry(
    base_config, store, monkeypatch
):
    retry_not_before_ms = now_ms() + 60_000
    cycle_result = _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms)

    def run_startup() -> dict[str, Any]:
        service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
        monkeypatch.setattr(
            service,
            "backfill_source_fills",
            lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
        )
        _set_run_once(service, lambda: cycle_result)
        asyncio.run(service.follow_source_websocket(stop_after_messages=0))
        return service.source_follow_startup_sync_status()

    first_startup = run_startup()
    first_obligation = first_startup["startup_liquidity_obligation"]
    obligation_key = first_obligation["source_event_key"]
    assert first_obligation["inserted"] is True
    assert first_obligation["reused_existing"] is False
    assert first_obligation["superseded_duplicate_count"] == 0
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 1

    second_startup = run_startup()
    second_obligation = second_startup["startup_liquidity_obligation"]
    assert second_obligation["source_event_key"] == obligation_key
    assert second_obligation["inserted"] is False
    assert second_obligation["reused_existing"] is True
    assert second_obligation["superseded_duplicate_count"] == 0
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 1

    duplicate_keys = ["duplicate-startup-hip3-retry-1", "duplicate-startup-hip3-retry-2"]
    for index, duplicate_key in enumerate(duplicate_keys, start=1):
        duplicate = SourceEvent(
            idempotency_key=duplicate_key,
            event_type=SourceEventType.POSITION,
            source_wallet=base_config.source_wallet,
            exchange_ts_ms=3_000 + index,
            observed_ts_ms=3_000 + index,
            payload={
                "event_subtype": "internal_hip3_liquidity_retry",
                "event_count": 1,
                "copy_signal_key": f"duplicate-startup-signal-{index}",
                "internal_retry_obligation": True,
            },
        )
        _persist_hip3_liquidity_retry(
            store,
            duplicate,
            retry_not_before_ms=retry_not_before_ms,
        )
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 3

    third_startup = run_startup()
    third_obligation = third_startup["startup_liquidity_obligation"]
    assert third_obligation["source_event_key"] == obligation_key
    assert third_obligation["inserted"] is False
    assert third_obligation["reused_existing"] is True
    assert third_obligation["superseded_duplicate_count"] == 2
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 1

    rows = {row["source_event_key"]: row for row in store.source_reaction_rows()}
    assert rows[obligation_key]["status"] == "blocked"
    for duplicate_key in duplicate_keys:
        assert rows[duplicate_key]["status"] == "ignored"
        duplicate_outcome = json.loads(rows[duplicate_key]["outcome_json"])
        assert duplicate_outcome["skip_class"] == "duplicate_internal_liquidity_retry"
        assert duplicate_outcome["superseded_by"] == obligation_key


def test_follow_source_websocket_persists_sixty_second_liquidity_retry_outcome(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    retry_not_before_ms = now_ms() + 60_000
    cycle_result = _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms)
    event = _event("hip3-liquidity-retry", SourceEventType.FILL, "fill")
    _set_run_once(service, lambda: cycle_result)

    async def observe_one(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 1
        await on_event(event, service.observer.record_source_event(event))

    monkeypatch.setattr(service.observer, "observe_websocket", observe_one)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert result["stats"]["validation_runs"] == 1
    assert store.source_reaction_status(event.idempotency_key) == "blocked"
    row = next(
        item
        for item in store.source_reaction_rows()
        if item["source_event_key"] == event.idempotency_key
    )
    outcome = json.loads(row["outcome_json"])
    assert outcome["retry"] == {
        "class": "hip3_liquidity",
        "disposition": "deferred",
        "retry_not_before_ms": retry_not_before_ms,
        "retry_interval_ms": 60_000,
        "coins": ["xyz:KR200"],
        "deferral_count": 1,
    }
    persisted = outcome["result"]["liquidity_deferred_intents"][0]
    assert persisted["intent"]["coin"] == "xyz:KR200"
    assert persisted["retry_not_before_ms"] == retry_not_before_ms
    assert persisted["blockers"] == cycle_result["liquidity_deferred_intents"][0]["blockers"]


def test_waiting_liquidity_retry_is_not_rerun_by_unchanged_websocket_signal(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    retry_not_before_ms = now_ms() + 60_000
    events = [
        SourceEvent(
            idempotency_key=f"waiting-liquidity-same-signal-{index}",
            event_type=SourceEventType.POSITION,
            source_wallet=base_config.source_wallet,
            exchange_ts_ms=2_000 + index,
            observed_ts_ms=2_000 + index,
            payload={
                "event_subtype": "position_snapshot",
                "event_count": 1,
                "copy_signal_key": "same-hip3-exposure",
            },
        )
        for index in range(2)
    ]
    calls: list[str] = []
    results = iter(
        [
            _completed_cycle_result(),
            _liquidity_deferred_cycle_result(retry_not_before_ms=retry_not_before_ms),
        ]
    )

    def run_once() -> dict[str, Any]:
        calls.append("run")
        return next(results)

    async def observe_repeated_signal(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 2
        await on_event(events[0], service.observer.record_source_event(events[0]))
        for _ in range(200):
            if store.source_reaction_status(events[0].idempotency_key) == "blocked":
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("first liquidity retry was not persisted as blocked")
        await on_event(events[1], service.observer.record_source_event(events[1]))

    _set_run_once(service, run_once)
    monkeypatch.setattr(service.observer, "observe_websocket", observe_repeated_signal)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    assert calls == ["run", "run"]
    assert result["stats"]["scheduled_reactions"] == 1
    assert result["stats"]["ignored_events"] == 1
    assert store.source_reaction_status(events[0].idempotency_key) == "blocked"
    assert store.source_reaction_status(events[1].idempotency_key) == "ignored"
    ignored_row = next(
        item
        for item in store.source_reaction_rows()
        if item["source_event_key"] == events[1].idempotency_key
    )
    assert json.loads(ignored_row["outcome_json"])["skip_class"] == ("hip3_liquidity_retry_not_due")


def test_due_liquidity_retry_is_woken_once_without_new_source_event(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    due = SourceEvent(
        idempotency_key="due-hip3-liquidity-wakeup",
        event_type=SourceEventType.POSITION,
        source_wallet=base_config.source_wallet,
        exchange_ts_ms=2_500,
        observed_ts_ms=2_500,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 1,
            "copy_signal_key": "due-hip3-exposure",
        },
    )
    _persist_hip3_liquidity_retry(store, due, retry_not_before_ms=now_ms() - 1)
    calls: list[str] = []
    results = iter([_completed_cycle_result(), _completed_cycle_result()])

    def run_once() -> dict[str, Any]:
        calls.append("run")
        return next(results)

    async def observe_nothing(stop_after_messages=None, on_event=None):
        assert stop_after_messages is None
        for _ in range(200):
            if store.source_reaction_status(due.idempotency_key) == "completed":
                return
            await asyncio.sleep(0.01)
        raise AssertionError("due liquidity retry was not woken")

    monkeypatch.setattr(service, "_validate_recovered_backfill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service.observer, "observe_websocket", observe_nothing)
    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket())

    assert calls == ["run", "run"]
    assert result["stats"]["observed_events"] == 0
    assert result["stats"]["validation_runs"] == 1
    assert result["stats"]["liquidity_retry_wakeups"] == 1
    assert store.source_reaction_status(due.idempotency_key) == "completed"
    row = next(
        item
        for item in store.source_reaction_rows()
        if item["source_event_key"] == due.idempotency_key
    )
    assert row["attempt_count"] == 1


def test_staggered_liquidity_retries_wake_only_at_their_own_deadlines(
    base_config, store, monkeypatch
):
    fake_now = [1_000_000]
    first_retry_ms = fake_now[0] + 1_000
    second_retry_ms = fake_now[0] + 2_000
    first = SourceEvent(
        idempotency_key="staggered-hip3-liquidity-first",
        event_type=SourceEventType.POSITION,
        source_wallet=base_config.source_wallet,
        exchange_ts_ms=fake_now[0],
        observed_ts_ms=fake_now[0],
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 1,
            "copy_signal_key": "staggered-hip3-first-signal",
        },
    )
    second = SourceEvent(
        idempotency_key="staggered-hip3-liquidity-second",
        event_type=SourceEventType.POSITION,
        source_wallet=base_config.source_wallet,
        exchange_ts_ms=fake_now[0] + 1,
        observed_ts_ms=fake_now[0] + 1,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 1,
            "copy_signal_key": "staggered-hip3-second-signal",
        },
    )
    _persist_hip3_liquidity_retry(
        store,
        first,
        retry_not_before_ms=first_retry_ms,
    )
    _persist_hip3_liquidity_retry(
        store,
        second,
        retry_not_before_ms=second_retry_ms,
    )

    real_asyncio = asyncio

    class AdvancingAsyncio:
        def __getattr__(self, name: str) -> Any:
            return getattr(real_asyncio, name)

        async def sleep(self, delay: float) -> None:
            if delay > 0:
                fake_now[0] += max(1, int(round(delay * 1_000)))
            await real_asyncio.sleep(0)

    monkeypatch.setattr(service_module, "asyncio", AdvancingAsyncio())
    monkeypatch.setattr(service_module, "now_ms", lambda: fake_now[0])

    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    monkeypatch.setattr(
        service,
        "backfill_source_fills",
        lambda: {"fetched": 0, "inserted": 0, "duplicates": 0, "warnings": []},
    )
    calls: list[str] = []
    retry_snapshots: list[dict[str, Any]] = []

    def run_once() -> dict[str, Any]:
        calls.append("run")
        if len(calls) > 1:
            rows = {row["source_event_key"]: row for row in store.source_reaction_rows()}
            retry_snapshots.append(
                {
                    "now_ms": fake_now[0],
                    "first_status": rows[first.idempotency_key]["status"],
                    "first_attempt_count": rows[first.idempotency_key]["attempt_count"],
                    "second_status": rows[second.idempotency_key]["status"],
                    "second_attempt_count": rows[second.idempotency_key]["attempt_count"],
                }
            )
        return _completed_cycle_result()

    async def observe_nothing(stop_after_messages=None, on_event=None):
        assert stop_after_messages is None
        for _ in range(2_000):
            if (
                store.source_reaction_status(first.idempotency_key) == "completed"
                and store.source_reaction_status(second.idempotency_key) == "completed"
            ):
                return
            await real_asyncio.sleep(0.001)
        raise AssertionError("staggered HIP-3 liquidity retries were not both woken")

    _set_run_once(service, run_once)
    monkeypatch.setattr(service.observer, "observe_websocket", observe_nothing)

    result = real_asyncio.run(service.follow_source_websocket())

    assert calls == ["run", "run", "run"]
    assert retry_snapshots == [
        {
            "now_ms": first_retry_ms,
            "first_status": "processing",
            "first_attempt_count": 1,
            "second_status": "blocked",
            "second_attempt_count": 0,
        },
        {
            "now_ms": second_retry_ms,
            "first_status": "completed",
            "first_attempt_count": 1,
            "second_status": "processing",
            "second_attempt_count": 1,
        },
    ]
    assert result["stats"]["observed_events"] == 0
    assert result["stats"]["validation_runs"] == 2
    assert result["stats"]["liquidity_retry_wakeups"] >= 2
    assert store.source_reaction_status(first.idempotency_key) == "completed"
    assert store.source_reaction_status(second.idempotency_key) == "completed"


def test_changed_source_signal_runs_immediately_while_liquidity_retry_waits(
    base_config, store, monkeypatch
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    retry_not_before_ms = now_ms() + 60_000
    waiting = SourceEvent(
        idempotency_key="future-hip3-liquidity-retry",
        event_type=SourceEventType.POSITION,
        source_wallet=base_config.source_wallet,
        exchange_ts_ms=3_000,
        observed_ts_ms=3_000,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 1,
            "copy_signal_key": "old-hip3-exposure",
        },
    )
    changed = SourceEvent(
        idempotency_key="changed-source-signal",
        event_type=SourceEventType.POSITION,
        source_wallet=base_config.source_wallet,
        exchange_ts_ms=3_100,
        observed_ts_ms=3_100,
        payload={
            "event_subtype": "position_snapshot",
            "event_count": 1,
            "copy_signal_key": "new-source-exposure",
        },
    )
    _persist_hip3_liquidity_retry(
        store,
        waiting,
        retry_not_before_ms=retry_not_before_ms,
    )
    service._last_source_copy_signal_by_subtype["position_snapshot"] = "old-hip3-exposure"
    calls: list[str] = []
    results = iter([_completed_cycle_result(), _completed_cycle_result()])

    def run_once() -> dict[str, Any]:
        calls.append("run")
        return next(results)

    async def observe_changed_signal(stop_after_messages=None, on_event=None):
        assert stop_after_messages == 1
        await on_event(changed, service.observer.record_source_event(changed))

    monkeypatch.setattr(service, "_validate_recovered_backfill", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service.observer, "observe_websocket", observe_changed_signal)
    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert calls == ["run", "run"]
    assert result["stats"]["scheduled_reactions"] == 1
    assert result["stats"]["validation_runs"] == 1
    assert store.source_reaction_status(waiting.idempotency_key) == "completed"
    assert store.source_reaction_status(changed.idempotency_key) == "completed"


def test_follow_source_restart_reacts_to_duplicate_journaled_before_callback(
    base_config, store, monkeypatch
):
    fill_time = 10_000
    monkeypatch.setattr("hyperliquid_copytrader.service.now_ms", lambda: fill_time + 100)
    fake = FakeInfoClient()
    fake.fills = [{"time": fill_time, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    crashed = CopyTraderService(base_config, store=store, info_client=fake)
    first = crashed.backfill_source_fills(start_time_ms=fill_time, end_time_ms=fill_time + 1)
    assert first["inserted"] == 1
    assert store.unfinished_source_reaction_count() == 1

    restarted = CopyTraderService(base_config, store=store, info_client=fake)
    calls: list[str] = []
    _set_run_once(
        restarted,
        lambda: _record_run(calls, _completed_cycle_result()),
    )

    async def quiet_observe(stop_after_messages=None, on_event=None):
        return None

    monkeypatch.setattr(restarted.observer, "observe_websocket", quiet_observe)
    result = asyncio.run(restarted.follow_source_websocket(stop_after_messages=0))

    assert calls == ["run", "run"]
    assert result["startup_backfill"]["inserted"] == 0
    assert result["startup_backfill"]["duplicates"] >= 1
    assert result["backfill_reactions"][0]["pending_reactions"] == 1
    assert store.unfinished_source_reaction_count() == 0
    assert store.source_reaction_rows()[0]["status"] == "completed"


def test_recovered_validation_does_not_complete_reaction_inserted_during_run(base_config, store):
    early = replace(
        _event("early-reaction", SourceEventType.FILL, "fill"),
        source_wallet=base_config.source_wallet,
    )
    late = replace(
        _event("late-reaction", SourceEventType.FILL, "fill"),
        source_wallet=base_config.source_wallet,
    )
    store.append_source_event(early, reaction_required=True)
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    def run_once() -> dict:
        store.append_source_event(late, reaction_required=True)
        return _completed_cycle_result()

    _set_run_once(service, run_once)

    reaction = service._validate_recovered_backfill(
        {"inserted": 0, "warnings": []},
        stage="startup_backfill",
    )

    assert reaction is not None
    assert reaction["action"] == "run_once"
    assert reaction["disposition"] == "skipped"
    assert "no concrete follower action" in reaction["reason"]
    assert store.source_reaction_status(early.idempotency_key) == "completed"
    assert store.source_reaction_status(late.idempotency_key) == "pending"
    assert store.unfinished_source_reaction_count(source_wallet=base_config.source_wallet) == 1
    early_row = next(
        row
        for row in store.source_reaction_rows()
        if row["source_event_key"] == early.idempotency_key
    )
    persisted_outcome = json.loads(early_row["outcome_json"])
    assert persisted_outcome["action"] == "run_once"
    assert persisted_outcome["disposition"] == "skipped"
    assert "no concrete follower action" in persisted_outcome["reason"]


def test_recovered_validation_preserves_ignored_event_disposition(base_config, store):
    actionable = replace(
        _event("recovered-fill", SourceEventType.FILL, "fill"),
        source_wallet=base_config.source_wallet,
    )
    informational = replace(
        _event(
            "recovered-open-order-snapshot",
            SourceEventType.OPEN_ORDER,
            "open_order_snapshot",
        ),
        source_wallet=base_config.source_wallet,
    )
    store.append_source_event(actionable, reaction_required=True)
    store.append_source_event(informational, reaction_required=True)
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    _set_run_once(service, _completed_cycle_result)

    reaction = service._validate_recovered_backfill(
        {"inserted": 0, "warnings": []},
        stage="startup_backfill",
    )

    assert reaction is not None
    assert reaction["disposition"] == "skipped"
    assert store.source_reaction_status(actionable.idempotency_key) == "completed"
    assert store.source_reaction_status(informational.idempotency_key) == "ignored"
    outcomes = {
        row["source_event_key"]: json.loads(row["outcome_json"])
        for row in store.source_reaction_rows()
    }
    assert outcomes[actionable.idempotency_key]["disposition"] == "skipped"
    assert outcomes[informational.idempotency_key]["reason"] == (
        "event does not change copy exposure"
    )


def test_follow_source_restart_replays_processing_reaction_after_ack_crash(
    base_config, store, monkeypatch
):
    event = _event("processing-crash", SourceEventType.FILL, "fill")
    store.append_source_event(event, reaction_required=True)
    assert store.claim_source_reactions([event.idempotency_key]) == 1
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(
        service,
        lambda: _record_run(calls, _completed_cycle_result()),
    )
    monkeypatch.setattr(service, "backfill_source_fills", lambda: {"inserted": 0, "warnings": []})

    async def quiet_observe(stop_after_messages=None, on_event=None):
        return None

    monkeypatch.setattr(service.observer, "observe_websocket", quiet_observe)
    asyncio.run(service.follow_source_websocket(stop_after_messages=0))

    assert calls == ["run", "run"]
    assert store.source_reaction_status(event.idempotency_key) == "completed"


def test_legacy_non_actionable_source_journal_requires_one_current_truth_validation(
    base_config, store, monkeypatch
):
    event = _event("legacy-reconcile", SourceEventType.RECONCILE, "reconcile")
    store.append_source_event(event, reaction_required=True)
    store.conn.execute(
        "UPDATE source_event_reactions SET status = 'legacy_unverified' WHERE source_event_key = ?",
        (event.idempotency_key,),
    )
    store.conn.commit()
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    calls: list[str] = []
    _set_run_once(
        service,
        lambda: _record_run(calls, _completed_cycle_result()),
    )
    monkeypatch.setattr(service, "backfill_source_fills", lambda: {"inserted": 0, "warnings": []})

    async def quiet_observe(stop_after_messages=None, on_event=None):
        return None

    monkeypatch.setattr(service.observer, "observe_websocket", quiet_observe)
    result = asyncio.run(service.follow_source_websocket(stop_after_messages=0))

    assert calls == ["run", "run"]
    assert result["backfill_reactions"][0]["action"] == "run_once"
    assert store.source_reaction_status(event.idempotency_key) == "completed"


def test_live_worker_marks_source_reaction_completed(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    event = _event("live-outbox", SourceEventType.FILL, "fill")
    calls: list[str] = []
    _set_run_once(
        service,
        lambda: _record_run(calls, _completed_cycle_result()),
    )
    monkeypatch.setattr(service, "backfill_source_fills", lambda: {"inserted": 0, "warnings": []})

    async def observe_new_event(stop_after_messages=None, on_event=None):
        inserted = service.observer.record_source_event(event)
        await on_event(event, inserted)

    monkeypatch.setattr(service.observer, "observe_websocket", observe_new_event)
    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert calls == ["run", "run"]
    assert result["stats"]["duplicate_events"] == 0
    assert result["stats"]["validation_runs"] == 1
    assert store.source_reaction_status(event.idempotency_key) == "completed"
    outcome = json.loads(store.source_reaction_rows()[0]["outcome_json"])
    assert outcome["disposition"] == "skipped"
    assert outcome["skip_class"] == "current_truth_already_converged"
    assert "no concrete follower action" in outcome["reason"]


def test_live_worker_links_filled_copy_to_matching_event_and_marks_coalesced_context(
    base_config,
    store,
    monkeypatch,
):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    source_fill = replace(
        _event("source-close-fill", SourceEventType.FILL, "fill"),
        exchange_ts_ms=2_000,
        observed_ts_ms=2_001,
        payload={
            "event_subtype": "fill",
            "data": {"fills": [{"coin": "BTC", "dir": "Close Long"}]},
        },
    )
    coalesced_cancel = replace(
        _event("later-cancel", SourceEventType.CANCEL, "cancel"),
        exchange_ts_ms=2_100,
        observed_ts_ms=2_101,
    )
    completed = {
        **_completed_cycle_result(),
        "intents": [
            {
                "intent_id": "intent-reduce",
                "cloid": "0xreduce",
                "action": "reduce",
                "coin": "BTC",
                "side": "sell",
                "size": "0.001",
                "reduce_only": True,
            }
        ],
        "reports": [
            {
                "report_id": "report-reduce",
                "intent_id": "intent-reduce",
                "cloid": "0xreduce",
                "status": "filled",
                "exchange_status": "filled",
                "filled_size": "0.001",
            }
        ],
    }
    _set_run_once(service, lambda: completed)
    monkeypatch.setattr(service, "backfill_source_fills", lambda: {"inserted": 0, "warnings": []})

    async def observe_events(stop_after_messages=None, on_event=None):
        for event in (source_fill, coalesced_cancel):
            inserted = service.observer.record_source_event(event)
            await on_event(event, inserted)

    monkeypatch.setattr(service.observer, "observe_websocket", observe_events)
    result = asyncio.run(service.follow_source_websocket(stop_after_messages=2))

    assert result["stats"]["validation_runs"] == 1
    rows = {
        row["source_event_key"]: json.loads(row["outcome_json"])
        for row in store.source_reaction_rows()
    }
    assert rows[source_fill.idempotency_key]["disposition"] == "copied"
    assert rows[source_fill.idempotency_key]["execution_evidence"][0]["report_id"] == (
        "report-reduce"
    )
    assert rows[coalesced_cancel.idempotency_key]["disposition"] == "skipped"
    assert rows[coalesced_cancel.idempotency_key]["skip_class"] == (
        "current_truth_already_converged"
    )
    assert rows[coalesced_cancel.idempotency_key]["coalesced_into_source_event_keys"] == [
        source_fill.idempotency_key
    ]


def test_follow_source_websocket_recovers_and_reconnects_after_gap(base_config, store, monkeypatch):
    fill_time = 10_000
    monkeypatch.setattr("hyperliquid_copytrader.service.now_ms", lambda: fill_time + 100)
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: fill_time + 100)
    config = replace(
        base_config,
        ops=OpsConfig(
            source_websocket_reconnect_attempts=1,
            source_websocket_reconnect_backoff_ms=0,
        ),
    )
    fake = FakeInfoClient()
    fake.fills = [{"time": fill_time, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
    service = CopyTraderService(config, store=store, info_client=fake)
    actionable = _event("fill-reconnected", SourceEventType.FILL, "fill")
    calls: list[str] = []
    attempts: list[int | None] = []

    async def flaky_observe(stop_after_messages=None, on_event=None):
        attempts.append(stop_after_messages)
        if len(attempts) == 1:
            raise RuntimeError("socket closed")
        await on_event(actionable, service.observer.record_source_event(actionable))

    def run_once() -> dict:
        calls.append("run")
        return _completed_cycle_result()

    monkeypatch.setattr(service.observer, "observe_websocket", flaky_observe)
    _set_run_once(service, run_once)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert attempts == [1, 1]
    assert calls == ["run", "run", "run"]
    assert result["stats"]["websocket_attempts"] == 2
    assert result["stats"]["websocket_errors"] == 1
    assert result["stats"]["websocket_reconnects"] == 1
    assert result["stats"]["completed_reactions"] == 1
    assert result["backfill_reactions"][0]["stage"] == "startup_backfill"
    assert result["backfill_reactions"][0]["action"] == "run_once"
    assert result["disconnect_recoveries"][0]["auto_resume"]["cleared"] is True
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value


def test_follow_source_websocket_resets_retry_budget_after_trusted_messages(
    base_config, store, monkeypatch
):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_websocket_reconnect_attempts=1,
            source_websocket_reconnect_backoff_ms=0,
        ),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    attempts: list[int | None] = []

    async def expiring_but_healthy(stop_after_messages=None, on_event=None):
        attempts.append(stop_after_messages)
        event = _event(
            f"healthy-before-expiry-{len(attempts)}",
            SourceEventType.FILL,
            "fill",
        )
        await on_event(event, service.observer.record_source_event(event))
        if len(attempts) < 3:
            raise RuntimeError("1000 Expired")

    monkeypatch.setattr(service.observer, "observe_websocket", expiring_but_healthy)
    _set_run_once(service, lambda: _completed_cycle_result())

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=3))

    assert attempts == [3, 2, 1]
    assert result["stats"]["observed_events"] == 3
    assert result["stats"]["websocket_errors"] == 2
    assert result["stats"]["websocket_reconnects"] == 2
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value


def test_follow_source_websocket_validates_recovered_disconnect_backfill(
    base_config, store, monkeypatch
):
    fill_time = 10_000
    monkeypatch.setattr("hyperliquid_copytrader.service.now_ms", lambda: fill_time + 100)
    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", lambda: fill_time + 100)
    config = replace(
        base_config,
        ops=OpsConfig(
            source_websocket_reconnect_attempts=1,
            source_websocket_reconnect_backoff_ms=0,
        ),
    )
    fake = FakeInfoClient()
    service = CopyTraderService(config, store=store, info_client=fake)
    calls: list[str] = []
    attempts: list[int | None] = []

    async def disconnected_then_clean(stop_after_messages=None, on_event=None):
        attempts.append(stop_after_messages)
        if len(attempts) == 1:
            fake.fills = [{"time": fill_time, "coin": "BTC", "oid": 1, "hash": "0xaaa", "tid": 1}]
            raise RuntimeError("socket closed")
        return None

    _set_run_once(
        service,
        lambda: _record_run(
            calls,
            _completed_cycle_result(),
        ),
    )
    monkeypatch.setattr(service.observer, "observe_websocket", disconnected_then_clean)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert attempts == [1, 1]
    assert calls == ["run", "run"]
    recovery = result["disconnect_recoveries"][0]
    assert recovery["backfill"]["inserted"] == 1
    assert recovery["auto_resume"]["cleared"] is True
    assert recovery["backfill_validation"]["stage"] == "disconnect_backfill"
    assert recovery["backfill_validation"]["action"] == "run_once"
    assert recovery["backfill_validation"]["inserted"] == 1


def _append_recovery_source_position(
    info: FakeInfoClient,
    *,
    coin: str,
    size: str,
    entry_px: str,
) -> None:
    info.state["assetPositions"].append(
        {
            "position": {
                "coin": coin,
                "szi": size,
                "entryPx": entry_px,
                "leverage": {"type": "cross", "value": 2},
            }
        }
    )


@pytest.mark.parametrize(
    "reason",
    [SafeModeReason.WEBSOCKET_DISCONNECT, SafeModeReason.STALE_SOURCE],
)
def test_live_exposed_follower_recovers_and_drains_blocked_reactions(
    base_config,
    store,
    monkeypatch,
    reason,
):
    service, adapter, _, blocked = _live_exposed_recovery_service(base_config, store, monkeypatch)
    service.safe_mode.trip(reason, "recoverable source truth incident")

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"]["cleared"] is True
    assert recovery["containment_cycle"]["desired_state_committed"] is True
    assert recovery["backfill_validation"]["action"] == "run_once"
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"
    assert store.blocked_source_reaction_count(source_wallet=service.config.source_wallet) == 0
    assert adapter.reports == []
    assert service.safe_mode.enabled is False


def test_live_follow_loop_recovers_expired_connection_unattended(base_config, store, monkeypatch):
    service, adapter, _, prior = _live_exposed_recovery_service(base_config, store, monkeypatch)
    store.finish_source_reactions(
        [prior.idempotency_key],
        status="completed",
        outcome={"reason": "test setup"},
    )
    gap_event = replace(
        _event("expired-connection-gap", SourceEventType.FILL, "fill"),
        source_wallet=service.config.source_wallet,
    )
    attempts = 0

    async def expired_then_reconnected(stop_after_messages=None, on_event=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            assert store.append_source_event(gap_event, reaction_required=True)
            raise RuntimeError("1000 Expired")
        return None

    monkeypatch.setattr(service.observer, "observe_websocket", expired_then_reconnected)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert attempts == 2
    assert result["stats"]["websocket_reconnects"] == 1
    assert result["disconnect_recoveries"][0]["auto_resume"]["cleared"] is True
    assert store.source_reaction_status(gap_event.idempotency_key) == "completed"
    assert adapter.reports == []
    assert service.safe_mode.enabled is False


def test_live_exposed_recovery_blocks_new_symbol_risk(base_config, store, monkeypatch):
    service, adapter, info, blocked = _live_exposed_recovery_service(
        base_config, store, monkeypatch
    )
    _append_recovery_source_position(
        info,
        coin="ETH",
        size="1.0",
        entry_px="3000",
    )
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was exposed",
    )

    recovery = service._recover_source_websocket_gap(
        allow_auto_clear=True,
        allow_bounded_startup_adoption=False,
    )

    assert recovery["bounded_startup_adoption"] is False
    assert recovery["recovery_mode"] == "exposed_containment"
    assert recovery["auto_resume"]["cleared"] is False
    assert recovery["containment_cycle"]["recovery_containment_blocked"] is True
    assert any(
        "ETH" in blocker
        for blocker in recovery["containment_cycle"]["recovery_containment_blockers"]
    )
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"
    assert adapter.reports == []
    assert adapter.positions == {"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
    assert service.safe_mode.reason == SafeModeReason.WEBSOCKET_DISCONNECT


def test_live_exposed_bounded_startup_recovery_adopts_missing_allowed_symbol(
    base_config,
    store,
    monkeypatch,
):
    service, adapter, info, blocked = _live_exposed_recovery_service(
        base_config, store, monkeypatch
    )
    _append_recovery_source_position(
        info,
        coin="ETH",
        size="1.0",
        entry_px="3000",
    )
    adapter.forced_status = IntentStatus.FILLED
    service.safe_mode.trip(
        SafeModeReason.STALE_SOURCE,
        "source changed during bounded startup adoption",
    )

    recovery = service._recover_source_websocket_gap(
        allow_auto_clear=True,
        allow_bounded_startup_adoption=True,
    )

    assert recovery["bounded_startup_adoption"] is True
    assert recovery["recovery_mode"] == "startup_bounded_adoption"
    assert recovery["auto_resume"]["cleared"] is True
    assert recovery["deferred_open_drain"]["status"] == "drained"
    assert adapter.positions["ETH"].size > 0
    eth_orders = [
        report.payload["intent"]
        for report in adapter.reports
        if report.payload.get("intent") and report.payload["intent"].coin == "ETH"
    ]
    assert len(eth_orders) == 1
    assert eth_orders[0].action.value == "open"
    assert eth_orders[0].reduce_only is False
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"
    assert service.safe_mode.enabled is False


@pytest.mark.parametrize("authority_loss", ["supervisor", "safe_mode_revision"])
def test_live_exposed_bounded_startup_recovery_requires_current_authority(
    base_config,
    store,
    monkeypatch,
    authority_loss,
):
    service, adapter, info, blocked = _live_exposed_recovery_service(
        base_config, store, monkeypatch
    )
    _append_recovery_source_position(
        info,
        coin="ETH",
        size="1.0",
        entry_px="3000",
    )
    adapter.forced_status = IntentStatus.FILLED
    if authority_loss == "supervisor":
        monkeypatch.setattr(
            service,
            "_validation_supervisor_decision",
            lambda: service_module.RuntimeDecision(
                False,
                SafeModeReason.OPERATOR_KILL_SWITCH,
                "validation supervisor denied bounded adoption",
            ),
        )
        expected_skip = "bounded startup adoption lacks current supervisor authority"
    else:
        reconcile = adapter.reconcile

        def reconcile_after_new_incident():
            snapshot = reconcile()
            service.safe_mode.trip(
                SafeModeReason.STALE_SOURCE,
                "newer source incident arrived during bounded startup recovery",
            )
            return snapshot

        monkeypatch.setattr(adapter, "reconcile", reconcile_after_new_incident)
        expected_skip = "exact follower truth did not satisfy safe-mode clearance"
    service.safe_mode.trip(
        SafeModeReason.STALE_SOURCE,
        "source changed during bounded startup adoption",
    )

    recovery = service._recover_source_websocket_gap(
        allow_auto_clear=True,
        allow_bounded_startup_adoption=True,
    )

    assert recovery["auto_resume"] is None
    assert expected_skip in recovery["auto_resume_skipped"]
    assert "ETH" not in adapter.positions
    assert adapter.reports == []
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"
    assert service.safe_mode.enabled is True


def test_live_bounded_startup_recovery_drain_reuses_outer_exchange_lease(
    base_config,
    store,
    monkeypatch,
):
    config = replace(
        base_config,
        ops=replace(base_config.ops, max_new_intents_per_cycle=1),
    )
    service, adapter, info, blocked = _live_exposed_recovery_service(config, store, monkeypatch)
    _append_recovery_source_position(
        info,
        coin="ETH",
        size="1.0",
        entry_px="3000",
    )
    _append_recovery_source_position(
        info,
        coin="SOL",
        size="10.0",
        entry_px="150",
    )
    adapter.forced_status = IntentStatus.FILLED
    lease_operations: list[str] = []
    acquire_exchange_lease = service._acquire_exchange_lease

    def record_exchange_lease(operation: str) -> bool:
        lease_operations.append(operation)
        return acquire_exchange_lease(operation)

    monkeypatch.setattr(service, "_acquire_exchange_lease", record_exchange_lease)
    service.safe_mode.trip(
        SafeModeReason.STALE_SOURCE,
        "source changed during bounded startup adoption",
    )

    recovery = service._recover_source_websocket_gap(
        allow_auto_clear=True,
        allow_bounded_startup_adoption=True,
    )

    assert recovery["auto_resume"]["cleared"] is True
    assert recovery["deferred_open_drain"]["additional_cycle_count"] >= 1
    assert set(adapter.positions) == {"BTC", "ETH", "SOL"}
    assert lease_operations == ["source_websocket_recovery"]
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"


def test_live_exposed_recovery_executes_only_reduce_only_close(base_config, store, monkeypatch):
    service, adapter, info, blocked = _live_exposed_recovery_service(
        base_config, store, monkeypatch
    )
    info.state["assetPositions"] = []
    adapter.forced_status = IntentStatus.FILLED
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired before source close was observed",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"]["cleared"] is True
    cycle = recovery["containment_cycle"]
    assert len(cycle["intents"]) == 1
    assert cycle["intents"][0]["action"] in {"close", "reduce"}
    assert cycle["intents"][0]["reduce_only"] is True
    assert adapter.positions == {}
    assert len(adapter.reports) == 1
    assert adapter.reports[0].payload["intent"].reduce_only is True
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"


def test_live_recovery_does_not_complete_event_appended_after_convergence_cycle(
    base_config, store, monkeypatch
):
    service, adapter, _, blocked = _live_exposed_recovery_service(base_config, store, monkeypatch)
    later = replace(
        _event("post-cycle-source-event", SourceEventType.FILL, "fill"),
        source_wallet=service.config.source_wallet,
    )
    run_cycle = service._run_once_with_lease

    def run_cycle_then_append_later_event(*args, **kwargs):
        result = run_cycle(*args, **kwargs)
        assert store.append_source_event(later, reaction_required=True)
        return result

    monkeypatch.setattr(
        service,
        "_run_once_with_lease",
        run_cycle_then_append_later_event,
    )
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was exposed",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"]["cleared"] is True
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"
    assert store.source_reaction_status(later.idempotency_key) == "pending"
    assert store.unfinished_source_reaction_count(source_wallet=service.config.source_wallet) == 1
    assert adapter.reports == []


def test_live_exposed_recovery_never_signs_leverage_or_margin_mode_update(
    base_config, store, monkeypatch
):
    service, adapter, info, blocked = _live_exposed_recovery_service(
        base_config, store, monkeypatch
    )
    info.state["assetPositions"][0]["position"].update(
        {"szi": "0.01", "leverage": {"type": "isolated", "value": 1}}
    )
    adapter.forced_status = IntentStatus.FILLED
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was exposed",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"]["cleared"] is True
    cycle = recovery["containment_cycle"]
    position_orders = [
        report.payload["intent"] for report in adapter.reports if report.payload.get("intent")
    ]
    assert position_orders
    assert all(
        intent.reduce_only and intent.action.value in {"close", "reduce"}
        for intent in position_orders
    )
    assert adapter.leverage_updates == []
    assert cycle["execution_finalization"]["status"] == "actual_checkpoint_committed"
    assert cycle["reconciled_checkpoint"]["positions"]["BTC"]["leverage"] == 2
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"


def test_live_flat_follower_cleanly_recovers_and_copies_missed_fill(
    base_config, store, monkeypatch
):
    service, adapter, _, blocked = _live_flat_recovery_service(base_config, store, monkeypatch)
    adapter.forced_status = IntentStatus.FILLED
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was flat",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["recovery_mode"] == "flat_bounded_resume"
    assert recovery["auto_resume"]["cleared"] is True
    cycle = recovery["containment_cycle"]
    assert len(cycle["intents"]) == 1
    assert cycle["intents"][0]["action"] == "open"
    assert cycle["intents"][0]["reduce_only"] is False
    assert adapter.positions["BTC"].size == Decimal("0.005")
    placement_reports = [report for report in adapter.reports if report.payload.get("intent")]
    assert len(placement_reports) == 1
    assert store.source_reaction_status(blocked.idempotency_key) == "completed"
    assert service.safe_mode.enabled is False


def test_live_flat_recovery_stays_blocked_when_account_scope_is_ambiguous(
    base_config, store, monkeypatch
):
    service, adapter, _, blocked = _live_flat_recovery_service(base_config, store, monkeypatch)
    adapter.account = "0xe000000000000000000000000000000000000000"
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was flat",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"] is None
    assert recovery["auto_resume_skipped"] == "follower account scope could not be proven"
    assert recovery["containment_cycle"] is None
    assert adapter.reports == []
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"
    assert service.safe_mode.reason == SafeModeReason.ACCOUNT_NOT_CONFIGURED


def test_live_flat_recovery_requires_ready_configured_watchdog(base_config, store, monkeypatch):
    config = replace(
        base_config,
        ops=replace(
            base_config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
        ),
    )
    service, adapter, _, blocked = _live_flat_recovery_service(config, store, monkeypatch)
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was flat",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"] is None
    assert recovery["auto_resume_skipped"] == (
        "independent containment watchdog did not pass recovery admission"
    )
    assert recovery["containment_watchdog"]["ready"] is False
    assert adapter.reports == []
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"
    assert service.safe_mode.reason == SafeModeReason.ORDER_TIMEOUT


def test_live_flat_recovery_does_not_clear_changed_safe_mode_revision(
    base_config, store, monkeypatch
):
    service, adapter, _, blocked = _live_flat_recovery_service(base_config, store, monkeypatch)
    reconcile = adapter.reconcile

    def reconcile_after_new_incident():
        snapshot = reconcile()
        service.safe_mode.trip(
            SafeModeReason.WEBSOCKET_DISCONNECT,
            "newer disconnect incident arrived during recovery",
        )
        return snapshot

    monkeypatch.setattr(adapter, "reconcile", reconcile_after_new_incident)
    service.safe_mode.trip(
        SafeModeReason.WEBSOCKET_DISCONNECT,
        "source websocket expired while follower was flat",
    )

    recovery = service._recover_source_websocket_gap(allow_auto_clear=True)

    assert recovery["auto_resume"] is None
    assert recovery["auto_resume_skipped"] == (
        "exact follower truth did not satisfy safe-mode clearance"
    )
    assert adapter.reports == []
    assert store.source_reaction_status(blocked.idempotency_key) == "blocked"
    assert service.safe_mode.enabled is True


def test_follow_source_websocket_does_not_reconnect_after_untrusted_payload(
    base_config,
    store,
    monkeypatch,
):
    config = replace(
        base_config,
        ops=OpsConfig(
            source_websocket_reconnect_attempts=3,
            source_websocket_reconnect_backoff_ms=0,
        ),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    attempts = 0

    async def bad_payload(stop_after_messages=None, on_event=None):
        nonlocal attempts
        attempts += 1
        raise SourceWebsocketMessageError("user mismatch")

    monkeypatch.setattr(service.observer, "observe_websocket", bad_payload)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert attempts == 1
    assert result["stats"]["websocket_attempts"] == 1
    assert result["stats"]["websocket_errors"] == 1
    assert result["stats"]["websocket_reconnects"] == 0
    assert result["safe_mode"]["reason"] == SafeModeReason.MISSED_EVENT_GAP.value


def test_follow_source_websocket_returns_safe_payload_when_startup_backfill_fails(
    base_config,
    store,
    monkeypatch,
):
    fake = FakeInfoClient()

    def malformed_info(payload):
        if payload["type"] == "userFillsByTime":
            return {"unexpected": "shape"}
        return FakeInfoClient.info(fake, payload)

    monkeypatch.setattr(fake, "info", malformed_info)
    service = CopyTraderService(base_config, store=store, info_client=fake)

    async def unexpected_observe(*_args, **_kwargs):
        raise AssertionError("websocket should not start after failed startup backfill")

    monkeypatch.setattr(service.observer, "observe_websocket", unexpected_observe)

    result = asyncio.run(service.follow_source_websocket(stop_after_messages=1))

    assert result["startup_backfill"]["error"] == "userFillsByTime response is not a list"
    assert result["stats"]["websocket_attempts"] == 0
    assert result["reactions"] == []
    assert result["backfill_reactions"] == []
    assert result["safe_mode"]["reason"] == SafeModeReason.MISSED_EVENT_GAP.value
    assert "userFillsByTime response is not a list" in result["safe_mode"]["detail"]
