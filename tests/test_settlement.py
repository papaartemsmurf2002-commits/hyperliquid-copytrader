from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

import hyperliquid_copytrader.exchange.hyperliquid as exchange_module
from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.config import AccountMode, ExchangeConfig, OpsConfig
from hyperliquid_copytrader.exchange.hyperliquid import (
    FakeExecutionAdapter,
    HyperliquidExecutionAdapter,
    PreSendBlockedError,
    classify_action_response,
    classify_auth_probe_response,
    classify_leverage_response,
    classify_order_status,
    classify_schedule_cancel_response,
)
from hyperliquid_copytrader.models import (
    DesiredState,
    FollowerIntent,
    IntentAction,
    IntentStatus,
    Mode,
    OpenOrder,
    Position,
    SafeModeReason,
    now_ms,
)
from hyperliquid_copytrader.service import CopyTraderService
from hyperliquid_copytrader.unified_account import SourceDexScope, UnifiedAccountSnapshot

from .fixtures.fake_hyperliquid import FakeInfoClient


TEST_SIGNER_ADDRESS = "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a"
TEST_FOLLOWER_ADDRESS = "0xf000000000000000000000000000000000000000"
TEST_MASTER_ADDRESS = "0xf111111111111111111111111111111111111111"


class ReconcileFailingAdapter(FakeExecutionAdapter):
    def reconcile(self):
        raise RuntimeError("post-settlement reconcile down")


class RecordingInfo:
    def __init__(self):
        self.calls = []

    def query_order_by_oid(self, user, oid):
        self.calls.append(("oid", user, oid))
        return {"status": "oid", "oid": oid}

    def query_order_by_cloid(self, user, cloid):
        self.calls.append(("cloid", user, cloid))
        return {"status": "cloid", "cloid": cloid}


class FakeCloid:
    @classmethod
    def from_str(cls, value):
        return ("cloid-object", value)


class DeadlineRecordingExchange:
    def __init__(self, *, fail_order: bool = False):
        self.expires_after = None
        self.fail_order = fail_order
        self.calls: list[tuple[Any, ...]] = []

    def noop(self, nonce):
        self.calls.append(("noop", self.expires_after))
        return {"status": "ok", "response": {"type": "default"}, "nonce": nonce}

    def order(self, *args, **kwargs):
        self.calls.append(("order", self.expires_after))
        if self.fail_order:
            raise RuntimeError("order rejected by transport")
        return {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 1}}]}},
        }

    def cancel_by_cloid(self, *args, **kwargs):
        self.calls.append(("cancel_by_cloid", self.expires_after))
        return {"status": "ok", "response": {"type": "cancel", "data": {"statuses": ["success"]}}}

    def update_leverage(self, *args, **kwargs):
        self.calls.append(("update_leverage", self.expires_after))
        return {
            "status": "ok",
            "response": {"type": "updateLeverage", "data": {"status": "success"}},
        }

    def schedule_cancel(self, scheduled_time_ms):
        self.calls.append(("schedule_cancel", self.expires_after, scheduled_time_ms))
        return {"status": "ok", "response": {"type": "default"}}


class MarketRecordingExchange(DeadlineRecordingExchange):
    def __init__(self):
        super().__init__()
        self.market_calls: list[tuple[str, str]] = []

    def order(self, coin, *args, **kwargs):
        self.market_calls.append(("order", coin))
        return super().order(coin, *args, **kwargs)

    def cancel_by_cloid(self, coin, *args, **kwargs):
        self.market_calls.append(("cancel_by_cloid", coin))
        return super().cancel_by_cloid(coin, *args, **kwargs)

    def update_leverage(self, leverage, coin, *args, **kwargs):
        self.market_calls.append(("update_leverage", coin))
        return super().update_leverage(leverage, coin, *args, **kwargs)


class OracleRejectingExchange(DeadlineRecordingExchange):
    def order(self, *args, **kwargs):
        self.calls.append(("order", self.expires_after))
        return {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {"statuses": [{"error": "Price too far from oracle asset=750024"}]},
            },
        }


class AccountPreflightInfo:
    def __init__(
        self,
        *,
        state: Any | None = None,
        open_orders: Any | None = None,
        rate_limit: Any | None = None,
        role: Any | None = None,
        abstraction: Any | None = None,
        dex_abstraction: Any | None = None,
        spot_state: Any | None = None,
        signer_role: Any | None = None,
        extra_agents: Any | None = None,
        vault_details: Any | None = None,
    ):
        self.state = (
            {
                "assetPositions": [],
                "crossMarginSummary": {"accountValue": "1000"},
                "marginSummary": {"accountValue": "1000"},
                "withdrawable": "1000",
            }
            if state is None
            else state
        )
        self.open_orders_response = [] if open_orders is None else open_orders
        self.rate_limit = (
            {"nRequestsUsed": 1, "nRequestsCap": 10000} if rate_limit is None else rate_limit
        )
        self.role = {"role": "user"} if role is None else role
        self.signer_role = signer_role
        self.extra_agents_response = extra_agents
        self.vault_details_response = (
            {"leader": TEST_MASTER_ADDRESS} if vault_details is None else vault_details
        )
        self.abstraction = "disabled" if abstraction is None else abstraction
        self.dex_abstraction = False if dex_abstraction is None else dex_abstraction
        self.spot_state = {"balances": []} if spot_state is None else spot_state
        self.calls: list[tuple[str, str]] = []

    def user_state(self, address: str):
        self.calls.append(("user_state", address))
        return self.state

    def open_orders(self, address: str):
        self.calls.append(("open_orders", address))
        return self.open_orders_response

    def spot_user_state(self, address: str):
        self.calls.append(("spot_user_state", address))
        return self.spot_state

    def user_rate_limit(self, address: str):
        self.calls.append(("user_rate_limit", address))
        return self.rate_limit

    def user_role(self, address: str):
        self.calls.append(("user_role", address))
        if address.lower() == TEST_SIGNER_ADDRESS:
            if self.signer_role is not None:
                return self.signer_role
            owner = TEST_FOLLOWER_ADDRESS
            if isinstance(self.role, dict) and isinstance(self.role.get("data"), dict):
                owner = self.role["data"].get("master", owner)
            if exchange_module._account_role(self.role) == "vault":
                owner = self.vault_details_response.get("leader", owner)
            return {"role": "agent", "data": {"user": owner}}
        return self.role

    def extra_agents(self, address: str):
        self.calls.append(("extra_agents", address))
        if self.extra_agents_response is not None:
            return self.extra_agents_response
        return [
            {
                "name": "pytest signer",
                "address": TEST_SIGNER_ADDRESS,
                "validUntil": now_ms() + 60_000,
            }
        ]

    def query_user_abstraction_state(self, address: str):
        self.calls.append(("query_user_abstraction_state", address))
        return self.abstraction

    def query_user_dex_abstraction_state(self, address: str):
        self.calls.append(("query_user_dex_abstraction_state", address))
        return self.dex_abstraction


class DexAwareAccountPreflightInfo(AccountPreflightInfo):
    def __init__(
        self,
        *,
        orders_by_dex: dict[str, Any] | None = None,
        historical_orders: Any | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.orders_by_dex = orders_by_dex or {"": self.open_orders_response}
        self.historical_orders_response = [] if historical_orders is None else historical_orders

    def open_orders(self, address: str, dex: str = ""):
        self.calls.append(("open_orders" if not dex else f"open_orders:{dex}", address))
        return self.orders_by_dex.get(dex, [])

    def historical_orders(self, address: str):
        self.calls.append(("historical_orders", address))
        return self.historical_orders_response


class PostFallbackAccountPreflightInfo(AccountPreflightInfo):
    query_user_abstraction_state: Any = None
    query_user_dex_abstraction_state: Any = None

    def post(self, url_path: str, payload: dict[str, Any]):
        address = payload.get("user") or payload.get("vaultAddress")
        self.calls.append(("post:" + payload["type"], str(address or "")))
        if payload["type"] == "userAbstraction":
            return self.abstraction
        if payload["type"] == "userDexAbstraction":
            return self.dex_abstraction
        if payload["type"] == "vaultDetails":
            return self.vault_details_response
        raise AssertionError(f"unexpected post payload {payload}")


def pending_intent(cloid: str = "0x11111111111111111111111111111111") -> FollowerIntent:
    return FollowerIntent(
        intent_id="intent-" + cloid[-4:],
        cloid=cloid,
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="source",
        reason="test pending",
        created_ms=now_ms(),
    )


def test_classify_order_status_handles_common_terminal_and_open_shapes():
    assert classify_order_status({"status": "filled"})[0] == IntentStatus.FILLED
    assert classify_order_status({"order": {"status": "canceled"}})[0] == IntentStatus.CANCELED
    assert (
        classify_order_status({"response": {"data": {"statuses": [{"status": "rejected"}]}}})[0]
        == IntentStatus.REJECTED
    )
    assert classify_order_status({"status": "open"})[0] == IntentStatus.ACKED
    assert classify_order_status({"status": "unknown"})[0] is None


def test_classify_order_status_ignores_free_text_terminal_words():
    payload = {
        "status": "ok",
        "response": {
            "message": "order was not filled and may already be canceled",
            "error": "rejected is descriptive text here, not a structured order status",
        },
    }

    status, exchange_status = classify_order_status(payload)

    assert status is None
    assert exchange_status == "ok"


def test_classify_order_status_handles_documented_terminal_variants():
    scheduled_status, scheduled_exchange = classify_order_status({"status": "scheduledCancel"})
    sibling_status, sibling_exchange = classify_order_status({"status": "siblingFilledCanceled"})
    rejected_status, rejected_exchange = classify_order_status({"status": "iocCancelRejected"})

    assert scheduled_status == IntentStatus.CANCELED
    assert scheduled_exchange == "scheduledcancel"
    assert sibling_status == IntentStatus.CANCELED
    assert sibling_exchange == "siblingfilledcanceled"
    assert rejected_status == IntentStatus.REJECTED
    assert rejected_exchange == "ioccancelrejected"


def test_real_adapter_order_status_uses_cloid_specific_query(base_config):
    config = replace(
        base_config,
        exchange=ExchangeConfig(
            follower_account_address=TEST_FOLLOWER_ADDRESS,
            api_wallet_address=TEST_SIGNER_ADDRESS,
            api_private_key="0x" + "1" * 64,
        ),
    )
    adapter = HyperliquidExecutionAdapter(config)
    info = RecordingInfo()
    adapter._info = info
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))

    cloid = "0x11111111111111111111111111111111"
    assert adapter.order_status(cloid) == {
        "status": "cloid",
        "cloid": ("cloid-object", cloid),
    }
    assert adapter.order_status(12345) == {"status": "oid", "oid": 12345}
    assert adapter.order_status("67890") == {"status": "oid", "oid": 67890}
    assert info.calls == [
        ("cloid", "0xf000000000000000000000000000000000000000", ("cloid-object", cloid)),
        ("oid", "0xf000000000000000000000000000000000000000", 12345),
        ("oid", "0xf000000000000000000000000000000000000000", 67890),
    ]


def test_real_adapter_runs_last_mile_gate_after_throttle_before_order(base_config, monkeypatch):
    events: list[str] = []

    def pre_send_check(_action: str, _risk_increasing: bool) -> None:
        events.append("gate")
        raise PreSendBlockedError("operator paused")

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    exchange = DeadlineRecordingExchange()
    adapter = HyperliquidExecutionAdapter(config, pre_send_check=pre_send_check)
    adapter._exchange = exchange
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
    monkeypatch.setattr(
        exchange_module,
        "apply_rest_throttle",
        lambda *_args, **_kwargs: events.append("throttle"),
    )
    intent = FollowerIntent(
        intent_id="last-mile-intent",
        cloid="0x12121212121212121212121212121212",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.001"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="last-mile",
        reason="test",
        created_ms=now_ms(),
    )

    report = adapter.place_intent(intent)

    assert events == ["throttle", "gate"]
    assert report.status == IntentStatus.SKIPPED
    assert report.exchange_status == "pre_send_blocked"
    assert exchange.calls == []


def test_real_adapter_uses_last_mile_resolved_intent_after_throttle(base_config, monkeypatch):
    events: list[str] = []
    order_call: dict[str, Any] = {}
    intent = FollowerIntent(
        intent_id="last-mile-resolved-intent",
        cloid="0x13131313131313131313131313131313",
        action=IntentAction.OPEN,
        coin="xyz:KR200",
        side="sell",
        size=Decimal("0.0122"),
        price=Decimal("1095"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="last-mile-resolved",
        reason="test final book repricing",
        created_ms=now_ms(),
        execution_proof={"kind": "hip3_round_trip"},
    )

    def pre_send_check(_action: str, _risk_increasing: bool) -> FollowerIntent:
        events.append("gate")
        return replace(
            intent,
            price=Decimal("1090.6"),
            execution_proof={
                "kind": "hip3_round_trip",
                "entry_limit": Decimal("1090.6"),
            },
        )

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    exchange = DeadlineRecordingExchange()
    real_order = exchange.order

    def recording_order(*args, **kwargs):
        events.append("order")
        order_call["args"] = args
        order_call["kwargs"] = kwargs
        return real_order(*args, **kwargs)

    monkeypatch.setattr(exchange, "order", recording_order)
    adapter = HyperliquidExecutionAdapter(config, pre_send_check=pre_send_check)
    adapter._exchange = exchange
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
    monkeypatch.setattr(
        exchange_module,
        "apply_rest_throttle",
        lambda *_args, **_kwargs: events.append("throttle"),
    )

    report = adapter.place_intent(intent)

    assert events == ["throttle", "gate", "order"]
    assert order_call["args"] == ("xyz:KR200",)
    assert order_call["kwargs"] == {
        "is_buy": False,
        "sz": 0.0122,
        "limit_px": 1090.6,
        "order_type": {"limit": {"tif": "Ioc"}},
        "reduce_only": False,
        "cloid": ("cloid-object", intent.cloid),
    }
    assert report.payload["order_request"] == {
        "coin": "xyz:KR200",
        "side": "sell",
        "size": Decimal("0.0122"),
        "price": Decimal("1090.6"),
        "reduce_only": False,
        "tif": "Ioc",
    }


def test_real_adapter_blocks_last_mile_semantic_identity_change(base_config, monkeypatch):
    intent = FollowerIntent(
        intent_id="last-mile-identity-intent",
        cloid="0x14141414141414141414141414141414",
        action=IntentAction.OPEN,
        coin="xyz:KR200",
        side="sell",
        size=Decimal("0.0122"),
        price=Decimal("1095"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="last-mile-identity",
        reason="test immutable identity",
        created_ms=now_ms(),
        execution_proof={"kind": "hip3_round_trip"},
    )
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    exchange = DeadlineRecordingExchange()
    adapter = HyperliquidExecutionAdapter(
        config,
        pre_send_check=lambda _action, _risk_increasing: replace(
            intent,
            price=Decimal("1090.6"),
            source_event_key="changed-source-event",
        ),
    )
    adapter._exchange = exchange
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
    monkeypatch.setattr(exchange_module, "apply_rest_throttle", lambda *_a, **_k: None)

    report = adapter.place_intent(intent)

    assert report.status == IntentStatus.SKIPPED
    assert report.exchange_status == "pre_send_blocked"
    assert "immutable order identity" in report.payload["error"]
    assert exchange.calls == []


def test_auth_probe_can_prove_clearance_while_safe_mode_is_active(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=TEST_FOLLOWER_ADDRESS,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    exchange = DeadlineRecordingExchange()
    adapter = HyperliquidExecutionAdapter(config)
    adapter._exchange = exchange
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service.safe_mode.trip(SafeModeReason.PREFLIGHT_FAILED, "operator paused")

    report = adapter.auth_probe(
        intent_id="auth-probe:clearance",
        cloid="0x" + "a" * 32,
    )

    assert report.status == IntentStatus.ACKED
    assert report.exchange_status == "auth_probe_ok"
    assert [call[0] for call in exchange.calls] == ["noop"]
    assert service.safe_mode.enabled is True


def test_kill_switch_wins_last_mile_race_for_every_signed_mutation(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=TEST_FOLLOWER_ADDRESS,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    exchange = DeadlineRecordingExchange()
    adapter = HyperliquidExecutionAdapter(config)
    adapter._exchange = exchange
    adapter._info = AccountPreflightInfo()
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service._active_plan_source_observed_ms = now_ms()
    service._active_plan_follower_observed_ms = now_ms()
    assert service._runtime_allows_exchange_action(
        count_rate=False,
        risk_reducing=True,
    ).ok

    config.ops.kill_switch_path.write_text("stop", encoding="utf-8")
    outer_decision = service._runtime_allows_exchange_action(
        count_rate=False,
        risk_reducing=True,
    )
    cancel = adapter.cancel_by_cloid("BTC", "0x" + "b" * 32)
    dead_man = adapter.schedule_cancel(
        scheduled_time_ms=now_ms() + 30_000,
        intent_id="dead-man-race",
        cloid="0x" + "c" * 32,
    )

    assert outer_decision.ok is False
    assert outer_decision.reason == SafeModeReason.OPERATOR_KILL_SWITCH
    assert cancel.status == IntentStatus.SKIPPED
    assert cancel.exchange_status == "pre_send_blocked"
    assert dead_man.status == IntentStatus.SKIPPED
    assert dead_man.exchange_status == "pre_send_blocked"
    assert exchange.calls == []
    assert adapter.reconcile().account == TEST_FOLLOWER_ADDRESS
    assert service.preflight().passed is False
    assert service.safe_mode.reason == SafeModeReason.OPERATOR_KILL_SWITCH


def test_explicit_containment_does_not_require_source_freshness(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=TEST_FOLLOWER_ADDRESS,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    service._active_plan_source_observed_ms = 0
    service._active_plan_follower_observed_ms = now_ms()

    service._last_mile_pre_send_check("cancel_by_cloid", False)

    service._active_plan_follower_observed_ms = 0
    try:
        service._last_mile_pre_send_check("cancel_by_cloid", False)
    except PreSendBlockedError as exc:
        assert "follower planning truth" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("stale follower truth must block signed containment")


def make_account_preflight_adapter(
    base_config,
    info: AccountPreflightInfo,
    *,
    vault_address: str = "",
    expected_account_mode: AccountMode = AccountMode.AUTO,
    unified_state_provider=None,
    source_dex_scope: SourceDexScope = SourceDexScope.STRICT,
    allowed_symbols: tuple[str, ...] | None = None,
) -> HyperliquidExecutionAdapter:
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=source_dex_scope,
        risk=(
            replace(base_config.risk, allowed_symbols=allowed_symbols)
            if allowed_symbols is not None
            else base_config.risk
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            vault_address=vault_address,
            expected_account_mode=expected_account_mode,
            testnet_enable=True,
        ),
    )
    adapter = HyperliquidExecutionAdapter(
        config,
        unified_state_provider=unified_state_provider,
    )
    adapter._info = info
    return adapter


def make_deadline_adapter(
    base_config, exchange: DeadlineRecordingExchange
) -> HyperliquidExecutionAdapter:
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=OpsConfig(
            exchange_action_timeout_s=Decimal("3"),
            exchange_expires_after_ms=2_000,
        ),
    )
    adapter = HyperliquidExecutionAdapter(config)
    adapter._exchange = exchange
    setattr(adapter, "_load_sdk", lambda: (None, None, None, FakeCloid))
    return adapter


def test_real_adapter_exposes_read_only_dead_man_volume_eligibility(base_config):
    adapter = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(rate_limit={"cumVlm": "81484.85"}),
    )

    assert adapter.dead_man_eligibility() == {
        "eligible": False,
        "cumulative_volume_usd": Decimal("81484.85"),
        "required_volume_usd": Decimal("1000000"),
        "read_only_query": True,
        "signed_action_performed": False,
    }


def test_real_adapter_account_preflight_requires_usable_account_state(base_config):
    info = AccountPreflightInfo()
    adapter = make_account_preflight_adapter(base_config, info)

    assert adapter.account_preflight() == []
    assert info.calls == [
        ("user_state", "0xf000000000000000000000000000000000000000"),
        ("open_orders", "0xf000000000000000000000000000000000000000"),
        ("spot_user_state", "0xf000000000000000000000000000000000000000"),
        ("user_rate_limit", "0xf000000000000000000000000000000000000000"),
        ("user_role", "0xf000000000000000000000000000000000000000"),
        ("query_user_abstraction_state", "0xf000000000000000000000000000000000000000"),
        ("query_user_dex_abstraction_state", "0xf000000000000000000000000000000000000000"),
        ("user_role", TEST_SIGNER_ADDRESS),
        ("extra_agents", TEST_FOLLOWER_ADDRESS),
    ]


def test_real_adapter_account_preflight_rejects_unrelated_valid_signer(base_config):
    info = AccountPreflightInfo(
        signer_role={"role": "agent", "data": {"user": TEST_MASTER_ADDRESS}},
        extra_agents=[
            {
                "name": "unrelated",
                "address": TEST_SIGNER_ADDRESS,
                "validUntil": now_ms() + 60_000,
            }
        ],
    )

    blockers = make_account_preflight_adapter(base_config, info).account_preflight()

    assert any("agent owner does not match" in blocker for blocker in blockers)


def test_real_adapter_account_preflight_rejects_missing_or_expired_agent_registration(
    base_config,
):
    missing = AccountPreflightInfo(extra_agents=[])
    missing_blockers = make_account_preflight_adapter(base_config, missing).account_preflight()
    assert any("not listed" in blocker for blocker in missing_blockers)

    expired = AccountPreflightInfo(
        extra_agents=[
            {
                "name": "expired",
                "address": TEST_SIGNER_ADDRESS,
                "validUntil": now_ms() - 1,
            }
        ]
    )
    expired_blockers = make_account_preflight_adapter(base_config, expired).account_preflight()
    assert any("expired" in blocker for blocker in expired_blockers)


def test_real_adapter_sets_and_restores_expires_after_for_signed_actions(base_config):
    exchange = DeadlineRecordingExchange()
    adapter = make_deadline_adapter(base_config, exchange)
    before = now_ms()

    reports = [
        adapter.auth_probe(intent_id="auth-probe:testnet:account", cloid="0x" + "1" * 32),
        adapter.place_intent(pending_intent("0x" + "2" * 32)),
        adapter.place_limit_order(
            coin="BTC",
            side="buy",
            size=Decimal("0.01"),
            price=Decimal("50000"),
            cloid="0x" + "3" * 32,
        ),
        adapter.cancel_by_cloid("BTC", "0x" + "4" * 32),
        adapter.update_leverage("BTC", 2),
        adapter.schedule_cancel(
            scheduled_time_ms=before + 30_000,
            intent_id="dead-man-schedule:testnet:account",
            cloid="0x" + "6" * 32,
        ),
        adapter.schedule_cancel(
            scheduled_time_ms=None,
            intent_id="dead-man-clear:testnet:account",
            cloid="0x" + "7" * 32,
        ),
    ]
    after = now_ms()

    assert exchange.expires_after is None
    assert [call[0] for call in exchange.calls] == [
        "noop",
        "order",
        "order",
        "cancel_by_cloid",
        "update_leverage",
        "schedule_cancel",
        "schedule_cancel",
    ]
    for call, report in zip(exchange.calls, reports):
        deadline = call[1]
        assert before + 2_000 <= deadline <= after + 2_000
        assert report.payload["expires_after_ms"] == deadline
        assert report.payload["expires_after_window_ms"] == 2_000


def test_rejected_limit_order_report_preserves_exact_request_context(base_config):
    adapter = make_deadline_adapter(base_config, OracleRejectingExchange())

    report = adapter.place_limit_order(
        coin="xyz:JPY",
        side="sell",
        size=Decimal("0.07"),
        price=Decimal("156.00"),
        cloid="0x" + "b" * 32,
        reduce_only=True,
        tif="Ioc",
    )

    assert report.status == IntentStatus.REJECTED
    assert report.payload["order_request"] == {
        "coin": "xyz:JPY",
        "side": "sell",
        "size": Decimal("0.07"),
        "price": Decimal("156.00"),
        "reduce_only": True,
        "tif": "Ioc",
    }


def test_real_adapter_restores_expires_after_after_signed_action_exception(base_config):
    exchange = DeadlineRecordingExchange(fail_order=True)
    adapter = make_deadline_adapter(base_config, exchange)

    report = adapter.place_intent(pending_intent("0x" + "5" * 32))

    assert exchange.expires_after is None
    assert report.status == IntentStatus.SENT
    assert report.exchange_status == "transport_unknown"
    assert "order rejected by transport" in report.payload["error"]
    assert report.payload["expires_after_ms"] == exchange.calls[0][1]
    assert report.payload["expires_after_window_ms"] == 2_000


def test_real_adapter_initializes_sdk_with_configured_hip3_dexes(base_config, monkeypatch):
    calls: list[tuple[str, dict[str, Any]]] = []

    class RecordingSdkInfo:
        def __init__(self, _base_url, **kwargs):
            calls.append(("info", kwargs))

    class RecordingSdkExchange:
        def __init__(self, _wallet, **kwargs):
            calls.append(("exchange", kwargs))

    class RecordingAccount:
        @staticmethod
        def from_key(_key):
            return "wallet"

    config = replace(
        base_config,
        risk=replace(
            base_config.risk,
            allowed_symbols=("BTC", "xyz:AAPL", "ABC:FOO", "xyz:MSFT"),
        ),
        exchange=replace(
            base_config.exchange,
            follower_account_address=TEST_FOLLOWER_ADDRESS,
            api_private_key="0x" + "1" * 64,
        ),
    )
    adapter = HyperliquidExecutionAdapter(config)
    monkeypatch.setattr(
        adapter,
        "_load_sdk",
        lambda: (
            RecordingAccount,
            RecordingSdkExchange,
            RecordingSdkInfo,
            FakeCloid,
        ),
    )

    _ = adapter.info
    _ = adapter.exchange

    assert calls[0][0] == "info"
    assert calls[0][1]["perp_dexs"] == ["", "ABC", "xyz"]
    assert calls[1][0] == "exchange"
    assert calls[1][1]["perp_dexs"] == ["", "ABC", "xyz"]


def test_real_adapter_preserves_canonical_hip3_market_for_signed_actions(base_config):
    exchange = MarketRecordingExchange()
    adapter = make_deadline_adapter(base_config, exchange)
    intent = replace(pending_intent("0x" + "8" * 32), coin="xyz:aapl")
    adapter.set_pre_send_check(lambda _action, _risk_increasing: intent)

    adapter.place_intent(intent)
    adapter.place_limit_order(
        coin="xyz:aapl",
        side="buy",
        size=Decimal("1"),
        price=Decimal("200"),
        cloid="0x" + "9" * 32,
    )
    adapter.cancel_by_cloid("xyz:aapl", "0x" + "a" * 32)
    adapter.update_leverage("xyz:AAPL", 2)
    adapter.update_leverage("kPEPE", 2)

    assert exchange.market_calls == [
        ("order", "xyz:AAPL"),
        ("order", "xyz:AAPL"),
        ("cancel_by_cloid", "xyz:AAPL"),
        ("update_leverage", "xyz:AAPL"),
        ("update_leverage", "kPEPE"),
    ]


def test_real_adapter_account_preflight_rejects_empty_agent_wallet_state(base_config):
    adapter = make_account_preflight_adapter(base_config, AccountPreflightInfo(state={}))
    blockers = adapter.account_preflight()
    assert any("account state is empty" in blocker for blocker in blockers)
    assert any("not an API wallet" in blocker for blocker in blockers)


def test_real_adapter_account_preflight_rejects_unfunded_or_malformed_state(base_config):
    unfunded = AccountPreflightInfo(
        state={
            "assetPositions": [],
            "marginSummary": {"accountValue": "0"},
            "withdrawable": "0",
        }
    )
    blockers = make_account_preflight_adapter(base_config, unfunded).account_preflight()
    assert "Hyperliquid follower accountValue must be positive before exchange mode" in blockers

    spot_only = AccountPreflightInfo(
        state={
            "assetPositions": [],
            "marginSummary": {"accountValue": "0"},
            "withdrawable": "0",
        },
        spot_state={"balances": [{"coin": "USDC", "total": "691.909846"}]},
    )
    blockers = make_account_preflight_adapter(base_config, spot_only).account_preflight()
    assert any("Spot USDC balance is 691.909846" in blocker for blocker in blockers)
    assert any("Transfer USDC to Perps" in blocker for blocker in blockers)

    malformed = AccountPreflightInfo(state={"assetPositions": {}, "marginSummary": {}})
    blockers = make_account_preflight_adapter(base_config, malformed).account_preflight()
    assert "Hyperliquid account state missing assetPositions list" in blockers
    assert "Hyperliquid account state missing marginSummary accountValue" in blockers


def test_real_adapter_account_preflight_rejects_bad_orders_or_rate_limit_diagnostics(base_config):
    bad_orders = AccountPreflightInfo(open_orders={"unexpected": "shape"})
    blockers = make_account_preflight_adapter(base_config, bad_orders).account_preflight()
    assert "Hyperliquid open-orders query returned non-list response" in blockers

    bad_rate_limit = AccountPreflightInfo(rate_limit={"error": "rate limited"})
    blockers = make_account_preflight_adapter(base_config, bad_rate_limit).account_preflight()
    assert any("userRateLimit diagnostic returned error" in blocker for blocker in blockers)


def test_real_adapter_account_preflight_rejects_agent_or_missing_roles(base_config):
    agent = AccountPreflightInfo(role={"role": "agent", "data": {"user": "0xabc"}})
    blockers = make_account_preflight_adapter(base_config, agent).account_preflight()
    assert any("userRole is agent/API wallet" in blocker for blocker in blockers)

    missing = AccountPreflightInfo(role={"role": "missing"})
    blockers = make_account_preflight_adapter(base_config, missing).account_preflight()
    assert "Hyperliquid userRole is missing; verify follower account address" in blockers


def test_real_adapter_account_preflight_rejects_unsupported_account_abstraction(base_config):
    for mode in ("portfolioMargin", "dexAbstraction"):
        blockers = make_account_preflight_adapter(
            base_config,
            AccountPreflightInfo(abstraction=mode),
        ).account_preflight()
        assert any("account abstraction mode" in blocker for blocker in blockers)
        assert any("unsupported" in blocker for blocker in blockers)

    blockers = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(abstraction={"unexpected": "shape"}),
    ).account_preflight()
    assert any("userAbstraction returned unrecognized response" in blocker for blocker in blockers)


def unified_snapshot(*, non_default_size: str = "0") -> UnifiedAccountSnapshot:
    observed = now_ms()
    summary = {
        "accountValue": "0",
        "totalMarginUsed": "0",
        "totalNtlPos": "0",
        "totalRawUsd": "0",
    }
    default = {
        "assetPositions": [],
        "crossMaintenanceMarginUsed": "0",
        "crossMarginSummary": summary,
        "marginSummary": summary,
        "time": observed,
        "withdrawable": "0",
    }
    other = {
        **default,
        "assetPositions": (
            []
            if non_default_size == "0"
            else [
                {
                    "type": "oneWay",
                    "position": {"coin": "xyz:FOO", "szi": non_default_size},
                }
            ]
        ),
    }
    return UnifiedAccountSnapshot(
        account="0xf000000000000000000000000000000000000000",
        clearinghouse_states={"": default, "xyz": other},
        observed_ms=observed,
        received_ms=observed,
    )


def mixed_unified_snapshot(*, non_default_dex: str = "xyz") -> UnifiedAccountSnapshot:
    observed = now_ms()
    summary = {
        "accountValue": "0",
        "totalMarginUsed": "0",
        "totalNtlPos": "0",
        "totalRawUsd": "0",
    }

    def state(coin: str, size: str) -> dict[str, Any]:
        return {
            "assetPositions": [
                {
                    "type": "oneWay",
                    "position": {
                        "coin": coin,
                        "szi": size,
                        "entryPx": "100",
                        "leverage": {"type": "cross", "value": 2},
                    },
                }
            ],
            "crossMaintenanceMarginUsed": "0",
            "crossMarginSummary": summary,
            "marginSummary": summary,
            "time": observed,
            "withdrawable": "0",
        }

    return UnifiedAccountSnapshot(
        account=TEST_FOLLOWER_ADDRESS,
        clearinghouse_states={"": state("BTC", "0.1"), non_default_dex: state("AAPL", "2")},
        observed_ms=observed,
        received_ms=observed,
    )


@pytest.mark.parametrize("abstraction", ["unifiedAccount", "default"])
def test_real_adapter_supports_explicit_unified_account_with_spot_collateral(
    base_config, abstraction
):
    info = AccountPreflightInfo(
        abstraction=abstraction,
        state={
            "assetPositions": [],
            "marginSummary": {"accountValue": "0"},
        },
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "60.5", "hold": "0"}]},
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=unified_snapshot,
    )

    assert adapter.account_preflight() == []
    assert adapter.account_context() == {
        "expected_mode": "unified",
        "detected_mode": "unified",
        "collateral_source": "spot_usdc_unified",
        "account_value": Decimal("60.5"),
        "spot_usdc_total": Decimal("60.5"),
        "spot_usdc_hold": Decimal("0"),
        "aggregate_observed_ms": adapter.account_context()["aggregate_observed_ms"],
        "aggregate_dex_count": 2,
        "active_non_default_dexes": [],
    }


@pytest.mark.parametrize("abstraction", ["unifiedAccount", "default"])
def test_real_adapter_unified_account_rejects_mode_mismatch_and_other_dex_activity(
    base_config, abstraction
):
    info = AccountPreflightInfo(
        abstraction=abstraction,
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "50", "hold": "0"}]},
    )
    mismatch = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.STANDARD,
        unified_state_provider=unified_snapshot,
    ).account_preflight()
    assert any("account mode mismatch" in blocker for blocker in mismatch)

    other_dex = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=lambda: unified_snapshot(non_default_size="1"),
    ).account_preflight()
    assert any("unsupported non-default DEX activity: xyz" in blocker for blocker in other_dex)


def test_real_adapter_unified_reconcile_persists_normalized_collateral_context(base_config):
    info = AccountPreflightInfo(
        abstraction="unifiedAccount",
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "55.25", "hold": "0"}]},
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=unified_snapshot,
    )

    snapshot = adapter.reconcile()

    assert snapshot.source == "hyperliquid-info-unified"
    assert snapshot.payload["account_mode"] == "unified"
    assert snapshot.payload["account_value"] == Decimal("55.25")
    assert snapshot.payload["account_context"]["collateral_source"] == "spot_usdc_unified"
    assert snapshot.payload["unified_aggregate"]["dex_count"] == 2


def test_real_adapter_unified_all_configured_markets_combines_positions_and_orders(base_config):
    info = DexAwareAccountPreflightInfo(
        abstraction="unifiedAccount",
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "80", "hold": "3"}]},
        orders_by_dex={
            "": [{"coin": "BTC", "side": "B", "sz": "0.01", "limitPx": "50000", "oid": 1}],
            "xyz": [{"coin": "AAPL", "side": "A", "sz": "1", "limitPx": "200", "oid": 2}],
        },
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=mixed_unified_snapshot,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        allowed_symbols=("BTC", "xyz:AAPL"),
    )

    assert adapter.account_preflight() == []
    snapshot = adapter.reconcile()

    assert set(snapshot.positions) == {"BTC", "xyz:AAPL"}
    assert snapshot.positions["xyz:AAPL"].size == Decimal("2")
    assert [order.coin for order in snapshot.open_orders] == ["BTC", "xyz:AAPL"]
    assert snapshot.payload["account_value"] == Decimal("80")
    assert snapshot.payload["unified_aggregate"]["configured_perp_dexes"] == ["", "xyz"]
    assert snapshot.payload["unified_aggregate"]["active_non_default_dexes"] == ["xyz"]
    assert snapshot.payload["unified_aggregate"]["unsupported_non_default_dexes"] == []
    assert snapshot.payload["account_context"]["unsupported_non_default_dexes"] == []


def test_real_adapter_unified_all_configured_markets_blocks_unknown_active_dex(base_config):
    info = DexAwareAccountPreflightInfo(
        abstraction="unifiedAccount",
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "80", "hold": "0"}]},
        orders_by_dex={"": [], "xyz": [], "abc": []},
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=lambda: mixed_unified_snapshot(non_default_dex="abc"),
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        allowed_symbols=("BTC", "xyz:AAPL"),
    )

    blockers = adapter.account_preflight()

    assert any("active unconfigured DEXes: abc" in blocker for blocker in blockers)
    assert adapter.account_context()["unsupported_non_default_dexes"] == ["abc"]


def test_real_adapter_all_configured_markets_requires_unified_follower(base_config):
    adapter = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(abstraction="disabled"),
        expected_account_mode=AccountMode.STANDARD,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        allowed_symbols=("BTC", "xyz:AAPL"),
    )

    blockers = adapter.account_preflight()

    assert "all_configured_markets requires a Unified follower account" in blockers


def test_real_adapter_discovers_and_blocks_flat_unknown_dex_open_order(base_config):
    aggregate = unified_snapshot()
    states = dict(aggregate.clearinghouse_states)
    states["other"] = states["xyz"]
    aggregate = replace(aggregate, clearinghouse_states=states)
    info = DexAwareAccountPreflightInfo(
        abstraction="unifiedAccount",
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "80", "hold": "0"}]},
        historical_orders=[{"order": {"coin": "other:FOO"}, "status": "open"}],
        orders_by_dex={
            "": [],
            "xyz": [],
            "other": [
                {
                    "coin": "other:FOO",
                    "side": "B",
                    "sz": "1",
                    "limitPx": "1",
                    "oid": 77,
                }
            ],
        },
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=lambda: aggregate,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        allowed_symbols=("BTC", "xyz:AAPL"),
    )

    blockers = adapter.account_preflight()

    assert any("open orders on unconfigured DEXes: other" in item for item in blockers)
    assert ("open_orders:other", TEST_FOLLOWER_ADDRESS) in info.calls


def test_real_adapter_blocks_truncated_global_order_discovery(base_config):
    info = DexAwareAccountPreflightInfo(
        abstraction="unifiedAccount",
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "80", "hold": "0"}]},
        historical_orders=[{"order": {"coin": "BTC"}}] * 2_000,
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=unified_snapshot,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        allowed_symbols=("BTC", "xyz:AAPL"),
    )

    blockers = adapter.account_preflight()

    assert any("historicalOrders discovery reached the 2000-row limit" in item for item in blockers)


def test_real_adapter_account_preflight_rejects_dex_abstraction_state(base_config):
    blockers = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(dex_abstraction=None),
    ).account_preflight()
    assert blockers == []

    blockers = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(dex_abstraction=True),
    ).account_preflight()
    assert any("DEX abstraction is enabled" in blocker for blocker in blockers)

    blockers = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(dex_abstraction={"unexpected": "shape"}),
    ).account_preflight()
    assert any(
        "userDexAbstraction returned unrecognized response" in blocker for blocker in blockers
    )


def test_real_adapter_account_preflight_uses_post_fallback_for_abstraction_queries(base_config):
    info = PostFallbackAccountPreflightInfo(
        abstraction={"type": "default"},
        dex_abstraction={"enabled": False},
        state={"assetPositions": [], "marginSummary": {"accountValue": "0"}},
        spot_state={"balances": [{"coin": "USDC", "token": 0, "total": "60.5", "hold": "0"}]},
    )
    adapter = make_account_preflight_adapter(
        base_config,
        info,
        expected_account_mode=AccountMode.UNIFIED,
        unified_state_provider=unified_snapshot,
    )

    assert adapter.account_preflight() == []
    assert adapter.account_context()["detected_mode"] == "unified"
    assert adapter.account_context()["collateral_source"] == "spot_usdc_unified"
    assert ("post:userAbstraction", "0xf000000000000000000000000000000000000000") in info.calls
    assert ("post:userDexAbstraction", "0xf000000000000000000000000000000000000000") in info.calls


def test_real_adapter_account_preflight_requires_vault_address_for_vault_or_subaccount_roles(
    base_config,
):
    subaccount = AccountPreflightInfo(
        role={"role": "subAccount", "data": {"master": TEST_MASTER_ADDRESS}}
    )
    blockers = make_account_preflight_adapter(base_config, subaccount).account_preflight()
    assert any("userRole is subaccount" in blocker for blocker in blockers)
    assert any("HLCT_VAULT_ADDRESS" in blocker for blocker in blockers)

    account = "0xf000000000000000000000000000000000000000"
    blockers = make_account_preflight_adapter(
        base_config,
        subaccount,
        vault_address=account,
    ).account_preflight()
    assert blockers == []

    vault = AccountPreflightInfo(role="vault")
    blockers = make_account_preflight_adapter(base_config, vault).account_preflight()
    assert any("userRole is vault" in blocker for blocker in blockers)


def test_real_adapter_account_preflight_rejects_vault_address_for_normal_user_role(base_config):
    account = "0xf000000000000000000000000000000000000000"
    user = AccountPreflightInfo(role={"role": "user"})
    blockers = make_account_preflight_adapter(
        base_config,
        user,
        vault_address=account,
    ).account_preflight()
    assert any(
        "HLCT_VAULT_ADDRESS is set but Hyperliquid userRole is user" in blocker
        for blocker in blockers
    )


def test_real_adapter_account_preflight_rejects_unrecognized_role_response(base_config):
    blockers = make_account_preflight_adapter(
        base_config,
        AccountPreflightInfo(role={"unexpected": "shape"}),
    ).account_preflight()
    assert any(
        "userRole diagnostic returned unrecognized response" in blocker for blocker in blockers
    )


def test_classify_action_response_distinguishes_partial_and_full_fills():
    resting = {
        "status": "ok",
        "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 1}}]}},
    }
    assert classify_action_response(
        resting, expected_size=Decimal("0.01"), action_type="order"
    ) == (
        IntentStatus.ACKED,
        "resting",
        None,
    )

    partial = {"response": {"data": {"statuses": [{"filled": {"totalSz": "0.005"}}]}}}
    status, exchange_status, filled = classify_action_response(
        partial,
        expected_size=Decimal("0.01"),
    )
    assert status == IntentStatus.ACKED
    assert exchange_status == "partial_fill"
    assert filled == Decimal("0.005")

    full = {"response": {"data": {"statuses": [{"filled": {"totalSz": "0.01"}}]}}}
    assert classify_action_response(full, expected_size=Decimal("0.01"))[0] == IntentStatus.FILLED

    overfill = {"response": {"data": {"statuses": [{"filled": {"totalSz": "0.02"}}]}}}
    status, exchange_status, filled = classify_action_response(
        overfill,
        expected_size=Decimal("0.01"),
    )
    assert status == IntentStatus.ACKED
    assert exchange_status == "overfill"
    assert filled == Decimal("0.02")

    no_fill = {"response": {"data": {"statuses": [{"filled": {"totalSz": "0"}}]}}}
    status, exchange_status, filled = classify_action_response(
        no_fill,
        expected_size=Decimal("0.01"),
    )
    assert status == IntentStatus.ACKED
    assert exchange_status == "no_fill"
    assert filled == Decimal("0")

    order_error = {
        "status": "ok",
        "response": {
            "type": "order",
            "data": {"statuses": [{"error": "Order must have minimum value of $10."}]},
        },
    }
    assert classify_action_response(
        order_error, expected_size=Decimal("0.01"), action_type="order"
    ) == (
        IntentStatus.REJECTED,
        "rejected",
        None,
    )

    ambiguous = {"status": "ok", "response": {"type": "default"}}
    assert classify_action_response(
        ambiguous, expected_size=Decimal("0.01"), action_type="order"
    ) == (
        IntentStatus.ACKED,
        "ambiguous_order_response",
        None,
    )


def test_classify_action_response_handles_documented_cancel_shapes():
    cancel_success = {
        "status": "ok",
        "response": {"type": "cancel", "data": {"statuses": ["success"]}},
    }
    assert classify_action_response(cancel_success, action_type="cancel") == (
        IntentStatus.CANCELED,
        "canceled",
        None,
    )

    cancel_error = {
        "status": "ok",
        "response": {
            "type": "cancel",
            "data": {
                "statuses": [{"error": "Order was never placed, already canceled, or filled."}]
            },
        },
    }
    assert classify_action_response(cancel_error, action_type="cancel") == (
        IntentStatus.REJECTED,
        "rejected",
        None,
    )


def test_classify_action_response_rejects_top_level_error_status():
    top_level = {"status": "err", "response": "Invalid multi-sig outer signer"}

    assert classify_action_response(
        top_level,
        expected_size=Decimal("0.01"),
        action_type="order",
    ) == (
        IntentStatus.REJECTED,
        "rejected",
        None,
    )
    assert classify_action_response(top_level, action_type="cancel") == (
        IntentStatus.REJECTED,
        "rejected",
        None,
    )


def test_classify_action_response_does_not_trust_free_text_terminal_words():
    malformed_order = {
        "status": "ok",
        "response": {
            "type": "order",
            "data": {"message": "order not filled yet"},
        },
    }
    assert classify_action_response(
        malformed_order,
        expected_size=Decimal("0.01"),
        action_type="order",
    ) == (
        IntentStatus.ACKED,
        "ambiguous_order_response",
        None,
    )

    malformed_cancel = {
        "status": "ok",
        "response": {
            "type": "cancel",
            "data": {"message": "Order was never placed, already canceled, or filled."},
        },
    }
    assert classify_action_response(malformed_cancel, action_type="cancel") == (
        IntentStatus.ACKED,
        "ambiguous_cancel_response",
        None,
    )


def test_classify_auth_probe_response_requires_ok_without_error():
    assert classify_auth_probe_response({"status": "ok", "response": {"type": "default"}}) == (
        IntentStatus.ACKED,
        "auth_probe_ok",
    )
    assert classify_auth_probe_response({"status": "ok", "response": {"error": "bad agent"}}) == (
        IntentStatus.REJECTED,
        "rejected",
    )
    assert classify_auth_probe_response({"status": "wat"}) == (
        IntentStatus.REJECTED,
        "ambiguous_auth_probe_response",
    )


def test_bounded_signed_noop_auth_probe_sends_exact_redacted_action(monkeypatch):
    expected_wallet = "0x" + "a" * 40
    vault = "0x" + "b" * 40
    wallet = SimpleNamespace(
        address=expected_wallet,
        _key_obj=SimpleNamespace(backend=object()),
    )
    monkeypatch.setattr("eth_account.Account.from_key", lambda _key: wallet)
    monkeypatch.setattr(exchange_module, "require_release_signing_backend", lambda **_kw: None)
    monkeypatch.setattr(
        exchange_module,
        "rest_throttle_enabled_for_base_url",
        lambda _url: True,
    )
    throttles: list[str] = []
    monkeypatch.setattr(
        exchange_module,
        "apply_rest_throttle",
        lambda label, **_kw: throttles.append(label),
    )
    signed: dict[str, Any] = {}

    def fake_sign(_wallet, action, vault_address, nonce, expires_after, is_mainnet):
        signed.update(
            {
                "action": action,
                "vault": vault_address,
                "nonce": nonce,
                "expires": expires_after,
                "mainnet": is_mainnet,
            }
        )
        return {"r": 1, "s": 2, "v": 27}

    monkeypatch.setattr("hyperliquid.utils.signing.sign_l1_action", fake_sign)
    outbound: dict[str, Any] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def read(_limit: int) -> bytes:
            return b'{"status":"ok","response":{"type":"default"}}'

    def fake_urlopen(request, *, timeout):
        outbound["url"] = request.full_url
        outbound["payload"] = json.loads(request.data)
        outbound["timeout"] = timeout
        return Response()

    monkeypatch.setattr(exchange_module.urllib.request, "urlopen", fake_urlopen)
    result = exchange_module.bounded_signed_noop_auth_probe(
        private_key="private-material",
        expected_api_wallet=expected_wallet,
        vault_address=vault,
    )

    assert result["passed"] is True
    assert result["status"] == "acked"
    assert result["exchange_status"] == "auth_probe_ok"
    assert set(result) == {
        "passed",
        "status",
        "exchange_status",
        "observed_ms",
        "expires_after_ms",
    }
    assert throttles == ["exchange:noop"]
    assert signed["action"] == {"type": "noop"}
    assert signed["vault"] == vault
    assert signed["mainnet"] is True
    assert signed["expires"] - signed["nonce"] == 10_000
    assert outbound["url"] == "https://api.hyperliquid.xyz/exchange"
    assert outbound["payload"]["action"] == {"type": "noop"}
    assert outbound["payload"]["vaultAddress"] == vault
    assert "private-material" not in json.dumps(result)
    assert "signature" not in result


def test_bounded_signed_noop_auth_probe_rejects_key_mismatch_before_transport(monkeypatch):
    wallet = SimpleNamespace(
        address="0x" + "c" * 40,
        _key_obj=SimpleNamespace(backend=object()),
    )
    monkeypatch.setattr("eth_account.Account.from_key", lambda _key: wallet)
    monkeypatch.setattr(exchange_module, "require_release_signing_backend", lambda **_kw: None)
    monkeypatch.setattr(
        exchange_module.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("transport must not run for a mismatched key")
        ),
    )
    with pytest.raises(ValueError, match="does not derive"):
        exchange_module.bounded_signed_noop_auth_probe(
            private_key="wrong-private-material",
            expected_api_wallet="0x" + "d" * 40,
            vault_address=None,
        )


def test_action_helpers_reject_top_level_error_status():
    top_level = {"status": "err", "response": "Invalid signer"}

    assert classify_auth_probe_response(top_level) == (IntentStatus.REJECTED, "rejected")
    assert classify_schedule_cancel_response(top_level, 12345) == (
        IntentStatus.REJECTED,
        "rejected",
    )
    assert classify_leverage_response(top_level) == (IntentStatus.REJECTED, "rejected")


def test_classify_schedule_cancel_response_requires_ok_without_error():
    assert classify_schedule_cancel_response({"status": "ok"}, 12345) == (
        IntentStatus.ACKED,
        "dead_man_scheduled",
    )
    assert classify_schedule_cancel_response({"status": "ok"}, None) == (
        IntentStatus.ACKED,
        "dead_man_cleared",
    )
    assert classify_schedule_cancel_response(
        {"status": "ok", "response": {"error": "bad"}}, 12345
    ) == (
        IntentStatus.REJECTED,
        "rejected",
    )
    assert classify_schedule_cancel_response({"status": "wat"}, 12345) == (
        IntentStatus.REJECTED,
        "ambiguous_dead_man_response",
    )


def test_classify_leverage_response_accepts_update_success_shapes():
    success = {
        "status": "ok",
        "response": {"type": "updateLeverage", "data": {"status": "success"}},
    }
    assert classify_leverage_response(success) == (IntentStatus.ACKED, "leverage_updated")
    assert classify_leverage_response({"status": "ok", "response": {"type": "default"}}) == (
        IntentStatus.ACKED,
        "leverage_updated",
    )
    assert classify_leverage_response({"status": "err", "error": "insufficient margin"}) == (
        IntentStatus.REJECTED,
        "rejected",
    )


def make_testnet_service(base_config, store, adapter):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    return CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )


def prepare_linked_plan(service, *, target_size: Decimal, cloid: str):
    desired = DesiredState(
        state_id=deterministic_cloid("durable-test-plan", cloid, target_size),
        source_event_key="durable-crash-test",
        mode=Mode.TESTNET,
        positions=(
            {"BTC": Position("BTC", target_size, Decimal("50000"), 1)} if target_size else {}
        ),
        reason="durable crash-window test",
        created_ms=now_ms(),
        source_wallet=service.config.source_wallet,
        action_account=TEST_FOLLOWER_ADDRESS,
        source_network="testnet",
    )
    intent = replace(
        pending_intent(cloid),
        desired_state_id=desired.state_id,
        size=abs(target_size) or Decimal("0.01"),
    )
    assert service.store.prepare_execution_plan(desired, [intent])
    return desired, intent


def test_settlement_terminalizes_prepared_without_exchange_lookup(base_config, store):
    adapter = FakeExecutionAdapter()
    service = make_testnet_service(base_config, store, adapter)
    desired, intent = prepare_linked_plan(
        service,
        target_size=Decimal("0"),
        cloid="0x" + "3" * 32,
    )

    def unexpected_lookup(_cloid):
        raise AssertionError("PREPARED attempt must not query exchange order status")

    setattr(adapter, "order_status", unexpected_lookup)
    result = service.settle_pending_intents()

    assert result["settled"][0]["exchange_status"] == "recovered:never_dispatched"
    assert result["pending_after"] == 0
    assert result["finalization"]["status"] == "target_committed"
    assert service.store.latest_desired_positions(Mode.TESTNET, committed_only=True) == {}
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert service.store.desired_state(desired.state_id) is not None


def test_settlement_queries_dispatching_and_commits_exact_fresh_target(base_config, store):
    adapter = FakeExecutionAdapter(
        positions={"BTC": Position("BTC", Decimal("0.01"), Decimal("50000"), 1)}
    )
    service = make_testnet_service(base_config, store, adapter)
    desired, intent = prepare_linked_plan(
        service,
        target_size=Decimal("0.01"),
        cloid="0x" + "4" * 32,
    )
    assert store.begin_intent_dispatch(intent.intent_id) is True
    adapter.status_by_cloid[intent.cloid] = {"status": "filled"}

    result = service.settle_pending_intents()

    assert result["settled"][0]["status"] == "filled"
    assert result["finalization"]["target_state_id"] == desired.state_id
    assert result["finalization"]["committed_target"] is True
    assert service.safe_mode.enabled


def test_restart_finalizes_terminal_report_crash_window_without_replaying_order(base_config, store):
    adapter = FakeExecutionAdapter(
        positions={"BTC": Position("BTC", Decimal("0.01"), Decimal("50000"), 1)}
    )
    service = make_testnet_service(base_config, store, adapter)
    desired, intent = prepare_linked_plan(
        service,
        target_size=Decimal("0.01"),
        cloid="0x" + "5" * 32,
    )
    assert store.begin_intent_dispatch(intent.intent_id) is True
    store.append_execution_report(
        exchange_module.ExecutionReport(
            report_id="crash-after-terminal-report",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=now_ms(),
        )
    )

    def unexpected_lookup(_cloid):
        raise AssertionError("terminal report must not replay or query the order")

    setattr(adapter, "order_status", unexpected_lookup)
    result = service.settle_pending_intents()

    assert result["pending_before"] == 0
    assert result["finalization"]["status"] == "target_committed"
    assert result["finalization"]["target_state_id"] == desired.state_id
    assert service.safe_mode.enabled


def test_restart_commits_actual_checkpoint_not_unmatched_target(base_config, store):
    adapter = FakeExecutionAdapter(
        positions={"BTC": Position("BTC", Decimal("0.004"), Decimal("50000"), 1)}
    )
    service = make_testnet_service(base_config, store, adapter)
    desired, intent = prepare_linked_plan(
        service,
        target_size=Decimal("0.01"),
        cloid="0x" + "6" * 32,
    )
    assert store.begin_intent_dispatch(intent.intent_id) is True
    store.append_execution_report(
        exchange_module.ExecutionReport(
            report_id="partial-crash-terminal-report",
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=now_ms(),
        )
    )

    result = service.settle_pending_intents()

    assert result["finalization"]["status"] == "actual_checkpoint_committed"
    assert result["finalization"]["checkpoint"]["positions"]["BTC"]["size"] == "0.004"
    committed_target = store.conn.execute(
        "SELECT 1 FROM desired_state_commits WHERE state_id = ?", (desired.state_id,)
    ).fetchone()
    assert committed_target is None
    committed = store.latest_desired_positions(Mode.TESTNET, committed_only=True)
    assert committed["BTC"].size == Decimal("0.004")
    assert service.safe_mode.enabled


def test_settle_pending_terminal_status_appends_report_and_clears_pending(base_config, store):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "filled"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_before"] == 1
    assert result["pending_after"] == 0
    assert result["settled"][0]["status"] == "filled"
    assert store.count("execution_reports") == 1
    assert store.count("reconcile_snapshots") == 1
    assert service.safe_mode.enabled
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert result["finalization"]["status"] == "actual_checkpoint_committed"


def test_settle_pending_rejected_status_trips_exchange_error_after_terminal_cleanup(
    base_config, store
):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "rejected", "error": "insufficient margin"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_after"] == 0
    assert result["settled"][0]["status"] == "rejected"
    assert service.safe_mode.reason == SafeModeReason.MARGIN_ERROR
    assert "insufficient margin" in service.safe_mode.detail
    assert store.count("execution_reports") == 1
    assert store.count("reconcile_snapshots") == 1


def test_settle_pending_canceled_open_status_requires_reconcile_review(base_config, store):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "canceled"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_after"] == 0
    assert result["settled"][0]["status"] == "canceled"
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert "settled canceled" in service.safe_mode.detail
    assert store.count("execution_reports") == 1
    assert store.count("reconcile_snapshots") == 1


def test_settle_pending_terminal_status_trips_stale_follower_when_reconcile_fails(
    base_config, store
):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = ReconcileFailingAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "filled"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_before"] == 1
    assert result["pending_after"] == 0
    assert result["settled"][0]["status"] == "filled"
    assert result["errors"] == [
        {"operation": "reconcile", "error": "post-settlement reconcile down"}
    ]
    assert service.safe_mode.reason.value == "stale_follower"
    assert "follower reconcile after settlement failed" in service.safe_mode.detail
    assert store.count("execution_reports") == 1
    assert store.count("reconcile_snapshots") == 0
    assert store.runtime_lease(service._runtime_lease_name("settle_pending")) is None


def test_settle_pending_open_status_stays_pending_and_safe(base_config, store):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "open"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_after"] == 1
    assert result["still_open"] == [
        {"intent_id": intent.intent_id, "cloid": intent.cloid, "status": "open"}
    ]
    assert service.safe_mode.reason.value == "restart_mid_fill"


def test_settle_pending_detects_known_cloid_open_order_detail_mismatch(base_config, store):
    desired = DesiredState(
        state_id=deterministic_cloid("desired-settlement-detail", now_ms()),
        source_event_key="source",
        mode=Mode.TESTNET,
        positions={"BTC": Position("BTC", Decimal("0.01"), Decimal("50000"), 2)},
        reason="test baseline",
        created_ms=now_ms(),
        source_wallet=base_config.source_wallet,
        action_account="0xf000000000000000000000000000000000000000",
        source_network="testnet",
    )
    store.append_desired_state(desired)
    store.commit_desired_state(desired.state_id)
    filled = pending_intent("0x11111111111111111111111111111111")
    still_open = pending_intent("0x22222222222222222222222222222222")
    store.append_intent(filled)
    store.append_intent(still_open)
    adapter = FakeExecutionAdapter(
        open_orders=[
            OpenOrder(
                "BTC",
                "sell",
                Decimal("0.02"),
                Decimal("50100"),
                cloid=still_open.cloid,
                reduce_only=True,
            )
        ]
    )
    adapter.status_by_cloid[filled.cloid] = {"status": "filled"}
    adapter.status_by_cloid[still_open.cloid] = {"status": "open"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_after"] == 1
    assert result["settled"][0]["status"] == "filled"
    assert result["still_open"] == [
        {"intent_id": still_open.intent_id, "cloid": still_open.cloid, "status": "open"}
    ]
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "open order mismatch" in service.safe_mode.detail
    assert "side expected buy actual sell" in service.safe_mode.detail
    assert "size expected 0.01 actual 0.02" in service.safe_mode.detail


def test_settle_pending_unknown_status_stays_pending_and_safe(base_config, store):
    intent = pending_intent()
    store.append_intent(intent)
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[intent.cloid] = {"status": "unknown"}
    service = make_testnet_service(base_config, store, adapter)
    result = service.settle_pending_intents()
    assert result["pending_after"] == 1
    assert result["ambiguous"] == [
        {"intent_id": intent.intent_id, "cloid": intent.cloid, "status": "unknown"}
    ]
    assert service.safe_mode.reason.value == "restart_mid_fill"
