from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json

import pytest

from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.config import (
    AccountMode,
    DeadManPolicy,
    ExchangeConfig,
    SourceNetwork,
)
from hyperliquid_copytrader.exchange.hyperliquid import FakeExecutionAdapter
from hyperliquid_copytrader.mainnet_canary import (
    MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT,
    MAINNET_CANARY_ACKNOWLEDGEMENT,
    build_mainnet_canary_profile,
)
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
    now_ms,
)
from hyperliquid_copytrader.runtime import RuntimeDecision
from hyperliquid_copytrader.service import (
    VALIDATION_SUPERVISOR_CONTAINMENT_DETAIL_PREFIX,
    VALIDATION_SUPERVISOR_LEASE_CONTAINMENT_BLOCK,
    CopyTraderService,
)
from hyperliquid_copytrader.validation_guardian import ControllerClaim, ControllerRegistry

from .fixtures.fake_hyperliquid import FakeInfoClient


PRIVATE_KEY = "0x" + "1" * 64
API_WALLET = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a"
FOLLOWER = "0xf000000000000000000000000000000000000000"
PEER_FOLLOWER = "0xe000000000000000000000000000000000000000"


class FillingMainnetActiveCanaryAdapter(FakeExecutionAdapter):
    def __init__(self):
        super().__init__(
            account=FOLLOWER,
            account_value=Decimal("50"),
            cumulative_volume=Decimal("0"),
        )

    def place_limit_order(
        self,
        *,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal,
        cloid: str,
        reduce_only: bool = False,
        tif: str = "Gtc",
    ) -> ExecutionReport:
        if side == "buy" and not reduce_only:
            self.positions[coin] = Position(
                coin=coin,
                size=size,
                entry_px=price,
                updated_ms=now_ms(),
            )
        elif side == "sell" and reduce_only:
            self.positions.pop(coin, None)
            self.account_value -= Decimal("0.01")
        report = ExecutionReport(
            report_id=deterministic_cloid("mainnet-active-fill", cloid, len(self.reports)),
            intent_id="limit:" + cloid,
            cloid=cloid,
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=now_ms(),
            payload={
                "coin": coin,
                "side": side,
                "size": size,
                "price": price,
                "tif": tif,
                "reduce_only": reduce_only,
                "expected_size": size,
                "filled_size": size,
            },
        )
        self.status_by_cloid[cloid] = {"status": "filled"}
        self.reports.append(report)
        return report


def _seed_passive_canary_proof(store) -> str:
    cloid = "0x" + "c" * 32
    store.append_execution_report(
        ExecutionReport(
            report_id="0x" + "1" * 32,
            intent_id="limit:" + cloid,
            cloid=cloid,
            status=IntentStatus.ACKED,
            exchange_status="resting",
            exchange_ts_ms=now_ms() - 2,
            payload={"tif": "Alo"},
        )
    )
    store.append_execution_report(
        ExecutionReport(
            report_id="0x" + "2" * 32,
            intent_id="cancel:" + cloid,
            cloid=cloid,
            status=IntentStatus.CANCELED,
            exchange_status="canceled",
            exchange_ts_ms=now_ms() - 1,
            payload={},
        )
    )
    return cloid


def _mainnet_config(base_config, tmp_path):
    state_dir = tmp_path / "mainnet-canary"
    return replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.MAINNET,
        db_path=state_dir / "mainnet-canary.sqlite3",
        risk=replace(
            base_config.risk,
            allowed_symbols=("BTC",),
            max_notional_usd=Decimal("15"),
            max_gross_exposure_usd=Decimal("15"),
            max_leverage=1,
        ),
        exchange=ExchangeConfig(
            follower_account_address=FOLLOWER,
            api_wallet_address=API_WALLET,
            api_private_key=PRIVATE_KEY,
            api_private_key_file=str(tmp_path / "mainnet-key.txt"),
            vault_address=FOLLOWER,
            expected_account_mode=AccountMode.STANDARD,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=False,
        ),
        ops=replace(
            base_config.ops,
            kill_switch_path=state_dir / "KILL_SWITCH",
            runtime_lock_dir=state_dir / "runtime-locks",
            max_new_intents_per_cycle=1,
            max_open_intents=1,
            max_exchange_actions_per_minute=12,
            circuit_breaker_failure_threshold=1,
            circuit_breaker_cooldown_ms=300_000,
            exchange_action_timeout_s=Decimal("15"),
            exchange_expires_after_ms=10_000,
            dead_man_cancel_ms=60_000,
        ),
    )


def test_mainnet_canary_profile_accepts_only_narrow_single_account_config(base_config, tmp_path):
    config = _mainnet_config(base_config, tmp_path)

    report = build_mainnet_canary_profile(config, coin="BTC")

    assert report["passed"] is True
    assert report["fleet_mainnet_ready"] is False
    assert report["limits"]["notional_usd_max"] == "15"


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("symbols", ("BTC", "ETH"), "no other markets"),
        ("notional", Decimal("25"), "between $12 and $15"),
        ("leverage", 2, "MAX_LEVERAGE=1"),
        ("dead_man", 30_000, "between 60s and 120s"),
    ],
)
def test_mainnet_canary_profile_rejects_broad_or_weak_config(
    base_config, tmp_path, field, value, expected
):
    config = _mainnet_config(base_config, tmp_path)
    if field == "symbols":
        config = replace(config, risk=replace(config.risk, allowed_symbols=value))
    elif field == "notional":
        config = replace(config, risk=replace(config.risk, max_notional_usd=value))
    elif field == "leverage":
        config = replace(config, risk=replace(config.risk, max_leverage=value))
    else:
        config = replace(config, ops=replace(config.ops, dead_man_cancel_ms=value))

    report = build_mainnet_canary_profile(config, coin="BTC")

    assert report["passed"] is False
    assert expected in " ".join(report["blockers"])


def test_mainnet_readiness_is_read_only_and_proves_flat_truth(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is True
    assert result["next_command"] is None
    assert result["signed_actions_performed"] is False
    assert adapter.auth_probe_reports == []
    assert adapter.reports == []
    assert adapter.schedule_cancel_reports == []
    assert result["truth_refresh"]["follower"]["positions"] == []
    assert result["truth_refresh"]["follower"]["open_orders"] == 0


def test_mainnet_readiness_requires_kill_switch_absent(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    config.ops.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
    config.ops.kill_switch_path.write_text("stop", encoding="utf-8")
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20")),
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is False
    assert "kill switch file exists" in " ".join(result["blockers"])


def test_mainnet_readiness_blocks_before_signing_when_dead_man_volume_is_too_low(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("999999.99"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is False
    assert result["signed_actions_performed"] is False
    assert result["dead_man_eligibility"] == {
        "eligible": False,
        "cumulative_volume_usd": "999999.99",
        "required_volume_usd": "1000000",
        "read_only_query": True,
        "signed_action_performed": False,
    }
    assert "requires $1,000,000 cumulative volume" in " ".join(result["blockers"])
    assert adapter.auth_probe_reports == []
    assert adapter.schedule_cancel_reports == []


def test_mainnet_readiness_accepts_fresh_watchdog_for_zero_volume_account(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            containment_watchdog_ttl_ms=5_000,
        ),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("0"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=5_000,
        role="containment_watchdog",
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is True
    assert result["signed_actions_performed"] is False
    assert result["dead_man_eligibility"]["eligible"] is False
    assert result["watchdog_protection"]["ready"] is True
    assert adapter.schedule_cancel_reports == []


def test_mainnet_readiness_enforces_dedicated_account_funding_cap(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("100")),
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is False
    assert "account value must be between $15 and $55" in " ".join(result["blockers"])


def test_mainnet_readiness_accepts_fifty_dollar_fresh_follower(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            containment_watchdog_ttl_ms=5_000,
        ),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("50"),
        cumulative_volume=Decimal("0"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=5_000,
        role="containment_watchdog",
    )

    result = service.mainnet_canary_readiness("BTC")

    assert result["candidate"] is True
    assert result["dead_man_eligibility"]["eligible"] is False
    assert result["watchdog_protection"]["ready"] is True
    assert result["truth_refresh"]["follower"]["account_value"] == "50"
    assert result["profile"]["limits"]["account_value_usd_max"] == "55"


def test_mainnet_active_readiness_requires_prior_passive_proof(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(config.ops, dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(
            account=FOLLOWER,
            account_value=Decimal("50"),
            cumulative_volume=Decimal("0"),
        ),
    )
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=15_000,
        role="containment_watchdog",
    )

    result = service.mainnet_active_canary_readiness("BTC")

    assert result["candidate"] is False
    assert result["next_command"] is None
    assert result["passive_canary_proof"]["passed"] is False
    assert "prior journaled" in " ".join(result["blockers"])


def test_mainnet_active_canary_fills_and_flattens_zero_volume_follower(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(config.ops, dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK),
    )
    adapter = FillingMainnetActiveCanaryAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    _seed_passive_canary_proof(store)
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=15_000,
        role="containment_watchdog",
    )

    result = service.mainnet_active_canary(
        "BTC",
        acknowledgement=MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert result["readiness"]["blockers"] == []
    assert result["readiness"]["candidate"] is True
    assert result["passed"] is True, result
    assert result["scope"] == "single_account_active_mainnet_canary"
    assert result["protection_mode"] == "independent_containment_watchdog"
    assert result["entry"]["status"] == "filled"
    assert result["exit"]["status"] == "filled"
    assert result["after_reconcile"]["positions"] == {}
    assert result["after_reconcile"]["open_orders"] == []
    assert Decimal(result["balance_delta"]) < 0
    assert store.pending_intent_count(Mode.LIVE) == 0

    evidence_dir = config.db_path.parent / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "20260712-000000-active-canary.json").write_text(
        json.dumps(result),
        encoding="utf-8",
    )
    validation = service._mainnet_validation_summary()
    assert validation["status"] == "active_round_trip_passed"
    assert validation["active"]["flat"] is True
    assert validation["active"]["balance_delta"] == "-0.01"
    assert validation["active_journal_proof"] is True

    replay = service.mainnet_active_canary(
        "BTC",
        acknowledgement=MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )
    assert replay["passed"] is False
    assert replay["entry"] is None
    assert "replay is refused" in " ".join(replay["readiness"]["blockers"])


def test_completed_passive_canary_is_one_use_but_postcheck_remains_available(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(config.ops, dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("0"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    _seed_passive_canary_proof(store)
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=15_000,
        role="containment_watchdog",
    )

    replay_readiness = service.mainnet_canary_readiness("BTC")
    postcheck = service.mainnet_canary_readiness("BTC", allow_completed_passive=True)
    replay = service.mainnet_passive_canary(
        "BTC",
        acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert replay_readiness["candidate"] is False
    assert "replay is refused" in " ".join(replay_readiness["blockers"])
    assert postcheck["candidate"] is True
    assert replay["passed"] is False
    assert replay["signed_actions_performed"] is False
    assert adapter.reports == []


def test_mainnet_active_canary_requires_exact_new_acknowledgement(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(account=FOLLOWER),
    )

    with pytest.raises(RuntimeError, match="exact acknowledgement"):
        service.mainnet_active_canary(
            "BTC",
            acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
            expected_account=FOLLOWER,
        )


def test_mainnet_readiness_blocks_unknown_prior_signed_mutation(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    assert store.prepare_signed_action_attempt(
        attempt_id="0x11111111111111111111111111111111",
        intent_id="dead-man-schedule:live:test",
        cloid="0x11111111111111111111111111111111",
        action="dead_man_schedule",
        mode=Mode.LIVE,
        account=FOLLOWER,
        network="mainnet",
        payload={"scheduled_time_ms": 1},
    )

    preflight = service.preflight()
    result = service.mainnet_canary_readiness("BTC")

    assert preflight.passed is False
    assert result["candidate"] is False
    assert len(result["unresolved_signed_actions"]) == 1
    assert "explicit operator review" in " ".join(result["blockers"])
    assert adapter.auth_probe_reports == []


def test_generic_live_runner_is_separately_disabled(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.run_once()

    assert result["intents"] == []
    assert result["reports"] == []
    assert service.safe_mode.reason == SafeModeReason.LIVE_BLOCKED
    assert adapter.auth_probe_reports == []
    assert adapter.reports == []


def test_mainnet_passive_canary_requires_exact_per_invocation_ack(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    with pytest.raises(ValueError, match="acknowledgement"):
        service.mainnet_passive_canary(
            "BTC",
            acknowledgement="yes",
            expected_account=FOLLOWER,
        )

    assert adapter.auth_probe_reports == []
    assert adapter.reports == []


def test_mainnet_passive_canary_binds_confirmation_to_exact_action_account(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    with pytest.raises(ValueError, match="--account"):
        service.mainnet_passive_canary(
            "BTC",
            acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
            expected_account="0xf111111111111111111111111111111111111111",
        )

    assert adapter.auth_probe_reports == []
    assert adapter.reports == []


def test_mainnet_passive_canary_places_cancels_and_finishes_flat(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(account=FOLLOWER, account_value=Decimal("20"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.mainnet_passive_canary(
        "BTC",
        acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert result["passed"] is True
    assert Decimal(result["signed_order_notional"]) <= Decimal("15")
    assert Decimal(result["risk_notional"]) <= Decimal("15")
    assert result["dead_man"]["exchange_status"] == "dead_man_scheduled"
    assert result["dead_man_clear"]["exchange_status"] == "dead_man_cleared"
    assert result["place"]["status"] == "acked"
    assert result["cancel"]["status"] == "canceled"
    assert result["reconcile"]["positions"] == {}
    assert result["reconcile"]["open_orders"] == []
    assert result["safe_mode"]["enabled"] is False


def test_mainnet_passive_canary_uses_independent_watchdog_for_fresh_subaccount(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            containment_watchdog_ttl_ms=5_000,
        ),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("0"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service.record_runner_heartbeat(
        status="ready",
        detail="pytest watchdog",
        ttl_ms=5_000,
        role="containment_watchdog",
    )

    result = service.mainnet_passive_canary(
        "BTC",
        acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert result["passed"] is True
    assert result["protection_mode"] == "independent_containment_watchdog"
    assert result["dead_man"]["exchange_status"] == "watchdog_containment_armed"
    assert result["dead_man_clear"]["exchange_status"] == "watchdog_containment_released"
    assert result["place"]["status"] == "acked"
    assert result["cancel"]["status"] == "canceled"
    assert adapter.schedule_cancel_reports == []


def test_watchdog_cancels_expired_bot_order_even_with_kill_switch(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            dead_man_cancel_ms=6_000,
        ),
    )
    cloid = "0x" + "7" * 32
    created = now_ms() - 10_000
    desired = DesiredState(
        state_id="watchdog-plan",
        source_event_key="watchdog-test",
        mode=Mode.LIVE,
        positions={},
        reason="pytest simulated parent crash",
        created_ms=created,
        source_wallet=config.source_wallet,
        action_account=FOLLOWER,
        source_network="mainnet",
    )
    intent = FollowerIntent(
        intent_id="watchdog-intent",
        cloid=cloid,
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.0001"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.LIVE,
        source_event_key="watchdog-test",
        reason="pytest simulated parent crash",
        created_ms=created,
        desired_state_id=desired.state_id,
    )
    assert store.prepare_execution_plan(desired, [intent])
    assert store.begin_intent_dispatch(intent.intent_id)
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("0"),
        open_orders=[
            OpenOrder(
                coin="BTC",
                side="buy",
                size=intent.size,
                price=intent.price or Decimal("0"),
                cloid=cloid,
                updated_ms=created,
            )
        ],
        status_by_cloid={cloid: {"status": "open"}},
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    config.ops.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
    config.ops.kill_switch_path.write_text("stop", encoding="utf-8")

    first = service.containment_watchdog_once()
    assert first["cancellations"][0]["status"] == "canceled"
    assert first["open_orders"] == []
    assert store.pending_intent_count(Mode.LIVE) == 0


def test_idle_containment_watchdog_performs_no_exchange_call(
    base_config, store, tmp_path, monkeypatch
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(config.ops, dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        cumulative_volume=Decimal("0"),
    )

    def unexpected_exchange_call(*_args, **_kwargs):
        raise AssertionError("idle watchdog must remain local")

    monkeypatch.setattr(adapter, "reconcile", unexpected_exchange_call)
    monkeypatch.setattr(adapter, "order_status", unexpected_exchange_call)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.containment_watchdog_once()

    assert result["pending_before"] == 0
    assert result["cancellations"] == []
    assert result["errors"] == []
    assert result["positions"] is None
    assert result["open_orders"] is None


def test_watchdog_reduce_only_flattens_when_validation_supervisor_disappears(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    deadline_ms = now_ms() + 60_000
    identity = "c" * 64
    registry_path = tmp_path / "controller-registry.sqlite3"
    registry = ControllerRegistry(registry_path)
    claims = [
        ControllerClaim(
            follower=follower,
            owner_token="opaque-owner-token",
            run_id="two-account-validation",
            state_identity_sha256=identity,
            deadline_ms=deadline_ms,
        )
        for follower in (FOLLOWER, PEER_FOLLOWER)
    ]
    assert registry.acquire_exclusive_set(
        claims,
        incarnation_id="guardian-incarnation",
        observed_ms=now_ms(),
        ttl_ms=30_000,
    )[0]
    registry.close()
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            validation_supervisor_lease_path=tmp_path / "missing-supervisor.json",
            validation_controller_registry_path=registry_path,
            validation_run_id="two-account-validation",
            validation_owner_token="opaque-owner-token",
            validation_supervisor_incarnation_id="guardian-incarnation",
            validation_follower_set=tuple(sorted((FOLLOWER, PEER_FOLLOWER))),
            validation_state_identity_sha256=identity,
            validation_deadline_ms=deadline_ms,
        ),
    )
    adapter = FillingMainnetActiveCanaryAdapter()
    adapter.positions["BTC"] = Position(
        coin="BTC",
        size=Decimal("0.0002"),
        entry_px=Decimal("50000"),
        leverage=1,
        updated_ms=now_ms(),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.containment_watchdog_once()

    assert result["validation_supervisor"]["configured"] is True
    assert result["validation_supervisor"]["allows_new_risk"] is False
    assert result["validation_supervisor"]["containment_active"] is True
    assert result["validation_supervisor"]["controller_registry_renewed"] is True
    assert result["validation_cleanup"][0]["coin"] == "BTC"
    assert result["validation_cleanup"][0]["flat"] is True
    assert result["positions"] == {}
    assert adapter.reports[-1].payload["reduce_only"] is True
    assert service.safe_mode.reason == SafeModeReason.LIVE_BLOCKED


def test_watchdog_preserves_exact_supervisor_containment_for_containment_lease(
    base_config,
    store,
    tmp_path,
):
    config = _mainnet_config(base_config, tmp_path)
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            validation_supervisor_lease_path=tmp_path / "supervisor.json",
        ),
    )
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("50"),
        cumulative_volume=Decimal("0"),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    detail = VALIDATION_SUPERVISOR_CONTAINMENT_DETAIL_PREFIX + "validation deadline reached"
    service.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
    service._validation_supervisor_decision = lambda: RuntimeDecision(  # noqa: SLF001
        False,
        SafeModeReason.LIVE_BLOCKED,
        VALIDATION_SUPERVISOR_LEASE_CONTAINMENT_BLOCK,
    )

    result = service.containment_watchdog_once(
        authority={
            "configured": True,
            "authoritative": True,
            "controller_registry_renewed": True,
        }
    )

    assert result["validation_supervisor"]["containment_active"] is True
    assert result["validation_supervisor"]["detail"] == (
        VALIDATION_SUPERVISOR_LEASE_CONTAINMENT_BLOCK
    )
    assert result["positions"] == {}
    assert result["open_orders"] == []
    assert adapter.reports == []
    assert service.safe_mode.enabled is True
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert service.safe_mode.detail == detail


def test_validation_guardian_never_touches_position_outside_frozen_scope(
    base_config, store, tmp_path
):
    config = _mainnet_config(base_config, tmp_path)
    deadline_ms = now_ms() + 60_000
    registry_path = tmp_path / "controller-registry.sqlite3"
    registry = ControllerRegistry(registry_path)
    claims = [
        ControllerClaim(
            follower=follower,
            owner_token="opaque-owner-token",
            run_id="two-account-validation",
            state_identity_sha256="d" * 64,
            deadline_ms=deadline_ms,
        )
        for follower in (FOLLOWER, PEER_FOLLOWER)
    ]
    assert registry.acquire_exclusive_set(
        claims,
        incarnation_id="guardian-incarnation",
        observed_ms=now_ms(),
        ttl_ms=30_000,
    )[0]
    registry.close()
    config = replace(
        config,
        ops=replace(
            config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
            validation_supervisor_lease_path=tmp_path / "missing-supervisor.json",
            validation_controller_registry_path=registry_path,
            validation_run_id="two-account-validation",
            validation_owner_token="opaque-owner-token",
            validation_supervisor_incarnation_id="guardian-incarnation",
            validation_follower_set=tuple(sorted((FOLLOWER, PEER_FOLLOWER))),
            validation_state_identity_sha256="d" * 64,
            validation_deadline_ms=deadline_ms,
        ),
    )
    adapter = FillingMainnetActiveCanaryAdapter()
    adapter.positions["ETH"] = Position(
        coin="ETH",
        size=Decimal("0.01"),
        entry_px=Decimal("3000"),
        leverage=1,
        updated_ms=now_ms(),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.containment_watchdog_once()

    assert result["positions"]["ETH"]["size"] == "0.01"
    assert result["validation_cleanup"] == []
    assert any("outside the frozen validation scope" in row["error"] for row in result["errors"])
    assert not any(report.payload.get("reduce_only") for report in adapter.reports)


def test_mainnet_passive_canary_refuses_any_preexisting_position(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        positions={"ETH": Position(coin="ETH", size=Decimal("0.01"))},
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.mainnet_passive_canary(
        "BTC",
        acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert result["place"] is None
    assert result["passed"] is False
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert adapter.schedule_cancel_reports == []


def test_mainnet_passive_canary_treats_any_fill_as_failed_incident(base_config, store, tmp_path):
    config = _mainnet_config(base_config, tmp_path)
    adapter = FakeExecutionAdapter(
        account=FOLLOWER,
        account_value=Decimal("20"),
        forced_status=IntentStatus.FILLED,
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.mainnet_passive_canary(
        "BTC",
        acknowledgement=MAINNET_CANARY_ACKNOWLEDGEMENT,
        expected_account=FOLLOWER,
    )

    assert result["passed"] is False
    assert result["place"]["status"] == "filled"
    assert result["safe_mode"]["enabled"] is True
