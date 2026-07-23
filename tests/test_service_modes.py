from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from time import sleep
from typing import Any, cast

import pytest

import hyperliquid_copytrader.service as service_module
import hyperliquid_copytrader.validation_guardian as validation_guardian
from hyperliquid_copytrader.cloid import deterministic_cloid
from hyperliquid_copytrader.config import (
    AccountMode,
    DeadManPolicy,
    ExchangeConfig,
    MAINNET_REST,
    MAINNET_WS,
    OpsConfig,
    SourceNetwork,
    TESTNET_REST,
    load_config,
)
from hyperliquid_copytrader.copy_engine import AssetMeta
from hyperliquid_copytrader.exchange.hyperliquid import (
    FakeExecutionAdapter,
    PreSendBlockedError,
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
    ReconcileSnapshot,
    SafeModeReason,
    SourceEvent,
    SourceEventType,
    now_ms,
)
from hyperliquid_copytrader.markets import build_frozen_market_universe_manifest
from hyperliquid_copytrader.observer import SourceSnapshot
from hyperliquid_copytrader.preflight import PreflightReport
from hyperliquid_copytrader.service import CopyTraderService
from hyperliquid_copytrader.safety import SafeModeController
from hyperliquid_copytrader.unified_account import SourceDexScope, UnifiedAccountSnapshot
from hyperliquid_copytrader.validation_guardian import ControllerClaim, ControllerRegistry

from .fixtures.fake_hyperliquid import FakeInfoClient, add_eth_position


class SlowInfoClient(FakeInfoClient):
    def __init__(self, delay_s: float):
        super().__init__()
        self.delay_s = delay_s

    def info(self, payload):
        sleep(self.delay_s)
        return super().info(payload)


class FailingInfoClient(FakeInfoClient):
    def __init__(self, fail_on_type: str, message: str):
        super().__init__()
        self.fail_on_type = fail_on_type
        self.message = message

    def info(self, payload):
        if payload["type"] == self.fail_on_type:
            raise RuntimeError(self.message)
        return super().info(payload)


class MissingMidInfoClient(FakeInfoClient):
    def __init__(self, missing_coin: str = "BTC"):
        super().__init__()
        self.mids.pop(missing_coin, None)


class FrozenCatalogInfoClient(FakeInfoClient):
    def __init__(self, *entries: dict[str, Any]):
        super().__init__()
        self.catalog_meta = {"universe": list(entries)}

    def info(self, payload):
        if payload["type"] == "perpDexs":
            self.calls.append(payload)
            return [None]
        if payload["type"] == "allPerpMetas":
            self.calls.append(payload)
            return [self.catalog_meta]
        return super().info(payload)


def _write_frozen_testnet_manifest(tmp_path, *entries: dict[str, Any]):
    manifest = build_frozen_market_universe_manifest(
        network="testnet",
        observed_ms=123,
        perp_dexs_payload=[None],
        all_perp_metas_payload=[{"universe": list(entries)}],
    )
    path = tmp_path / "market-universe.json"
    path.write_text(json.dumps(manifest.to_payload()), encoding="utf-8")
    return manifest, path


class StaleReconcileAdapter(FakeExecutionAdapter):
    def reconcile(self):
        snapshot = super().reconcile()
        return replace(snapshot, observed_ms=now_ms() - 1000)


class FailingReconcileAdapter(FakeExecutionAdapter):
    def reconcile(self):
        raise RuntimeError("reconcile down")


class RaisingPlaceIntentAdapter(FakeExecutionAdapter):
    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        raise RuntimeError("placement exploded")


class AmbiguousLeverageAdapter(FakeExecutionAdapter):
    def update_leverage(
        self, coin: str, leverage: int, is_cross: bool = True, *, risk_increasing: bool = True
    ) -> ExecutionReport:
        report = ExecutionReport(
            report_id=deterministic_cloid("ambiguous-leverage", coin, leverage, len(self.reports)),
            intent_id=f"leverage:{coin}:{leverage}",
            cloid=deterministic_cloid("ambiguous-leverage-cloid", coin, leverage),
            status=IntentStatus.REJECTED,
            exchange_status="ambiguous_leverage_response",
            exchange_ts_ms=now_ms(),
            payload={"response": {"status": "ok", "response": {"type": "default"}}},
        )
        self.reports.append(report)
        return report


class CrossMarginRejectedAdapter(FakeExecutionAdapter):
    def update_leverage(
        self, coin: str, leverage: int, is_cross: bool = True, *, risk_increasing: bool = True
    ) -> ExecutionReport:
        if not is_cross:
            forced_status = cast(IntentStatus | None, getattr(self, "forced_status", None))
            self.forced_status = None
            try:
                return super().update_leverage(
                    coin,
                    leverage,
                    is_cross=is_cross,
                    risk_increasing=risk_increasing,
                )
            finally:
                self.forced_status = forced_status
        report = ExecutionReport(
            report_id=deterministic_cloid(
                "cross-margin-rejected", coin, leverage, len(self.reports)
            ),
            intent_id=f"leverage:{coin}:{leverage}",
            cloid=deterministic_cloid("cross-margin-rejected-cloid", coin, leverage),
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=now_ms(),
            payload={
                "coin": coin,
                "leverage": leverage,
                "is_cross": True,
                "response": {
                    "status": "err",
                    "response": "Cross margin is not allowed for this asset.",
                },
            },
        )
        self.reports.append(report)
        return report


class RaisingLimitOrderAdapter(FakeExecutionAdapter):
    def place_limit_order(self, **kwargs) -> ExecutionReport:
        raise RuntimeError("limit placement exploded")


class RejectingCancelAdapter(FakeExecutionAdapter):
    def cancel_by_cloid(self, coin: str, cloid: str) -> ExecutionReport:
        report = ExecutionReport(
            report_id=deterministic_cloid("rejected-cancel", cloid, len(self.reports)),
            intent_id="cancel:" + cloid,
            cloid=cloid,
            status=IntentStatus.REJECTED,
            exchange_status="cancel_rejected",
            exchange_ts_ms=now_ms(),
            payload={"coin": coin, "cloid": cloid, "error": "cancel rejected"},
        )
        self.status_by_cloid[cloid] = {"status": "open", "order": {"cloid": cloid}}
        self.reports.append(report)
        return report


class VolumeRejectedScheduleAdapter(FakeExecutionAdapter):
    def schedule_cancel(self, *, scheduled_time_ms, intent_id, cloid):
        report = super().schedule_cancel(
            scheduled_time_ms=scheduled_time_ms,
            intent_id=intent_id,
            cloid=cloid,
        )
        report = replace(
            report,
            payload={"error": "Cannot set scheduled cancel time until enough volume traded."},
        )
        self.schedule_cancel_reports[-1] = report
        return report


class FillingActiveSmokeAdapter(FakeExecutionAdapter):
    def __init__(self):
        super().__init__()
        self.balance = Decimal("516.401359")

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
                coin=coin, size=size, entry_px=price, updated_ms=now_ms()
            )
        elif side == "sell" and reduce_only:
            self.positions.pop(coin, None)
            self.balance -= Decimal("0.012345")
        report = ExecutionReport(
            report_id=deterministic_cloid("active-fill", cloid, len(self.reports)),
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

    def reconcile(self) -> ReconcileSnapshot:
        observed = now_ms()
        return ReconcileSnapshot(
            snapshot_id=deterministic_cloid(
                "active-reconcile", observed, self.balance, self.positions
            ),
            account=self.account,
            positions=dict(self.positions),
            open_orders=[],
            observed_ms=observed,
            source="fake",
            payload={
                "clearinghouseState": {
                    "marginSummary": {
                        "accountValue": self.balance,
                        "totalNtlPos": "0.0" if not self.positions else "11",
                        "totalRawUsd": self.balance,
                    },
                    "crossMarginSummary": {
                        "accountValue": self.balance,
                        "totalNtlPos": "0.0" if not self.positions else "11",
                        "totalRawUsd": self.balance,
                    },
                    "assetPositions": [],
                    "withdrawable": self.balance,
                },
                "openOrders": [],
            },
        )


class RejectingDeadManClearAdapter(FakeExecutionAdapter):
    def schedule_cancel(self, *, scheduled_time_ms, intent_id, cloid) -> ExecutionReport:
        if scheduled_time_ms is not None:
            return super().schedule_cancel(
                scheduled_time_ms=scheduled_time_ms,
                intent_id=intent_id,
                cloid=cloid,
            )
        report = ExecutionReport(
            report_id=deterministic_cloid("reject-dead-man-clear", cloid),
            intent_id=intent_id,
            cloid=cloid,
            status=IntentStatus.REJECTED,
            exchange_status="dead_man_rejected",
            exchange_ts_ms=now_ms(),
            payload={"error": "dead-man clear rejected"},
        )
        self.reports.append(report)
        self.schedule_cancel_reports.append(report)
        return report


class UnprovenDeadManClearAdapter(FakeExecutionAdapter):
    def __init__(self, *, clear_status: IntentStatus, **kwargs: Any):
        super().__init__(**kwargs)
        self.clear_status = clear_status

    def schedule_cancel(self, *, scheduled_time_ms, intent_id, cloid) -> ExecutionReport:
        if scheduled_time_ms is not None:
            return super().schedule_cancel(
                scheduled_time_ms=scheduled_time_ms,
                intent_id=intent_id,
                cloid=cloid,
            )
        report = ExecutionReport(
            report_id=deterministic_cloid(
                "unproven-normal-dead-man-clear", self.clear_status.value, cloid
            ),
            intent_id=intent_id,
            cloid=cloid,
            status=self.clear_status,
            exchange_status=(
                "dead_man_rejected"
                if self.clear_status == IntentStatus.REJECTED
                else "transport_unknown"
            ),
            exchange_ts_ms=now_ms(),
            payload={"error": "dead-man clear outcome is not proven"},
        )
        self.scheduled_cancel_times.append(None)
        self.schedule_cancel_reports.append(report)
        return report


class ClockRollbackBeforeSignedBoundaryAdapter(FakeExecutionAdapter):
    def __init__(self, *, clock: dict[str, int]):
        super().__init__()
        self.clock = clock
        self.pre_send_check = None
        self.rollback_applied = False
        self.signed_exchange_calls = 0

    def set_pre_send_check(self, callback) -> None:
        self.pre_send_check = callback

    def reconcile(self) -> ReconcileSnapshot:
        return replace(super().reconcile(), observed_ms=self.clock["ms"])

    def _before_signed_boundary(
        self, action: str, risk_increasing: bool, *, intent_id: str, cloid: str
    ) -> ExecutionReport | None:
        if not self.rollback_applied:
            self.clock["ms"] -= 2_001
            self.rollback_applied = True
        try:
            assert self.pre_send_check is not None
            self.pre_send_check(action, risk_increasing)
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid("clock-rollback-blocked", action, cloid),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=self.clock["ms"],
                payload={"error": str(exc)},
            )
        self.signed_exchange_calls += 1
        return None

    def update_leverage(
        self, coin: str, leverage: int, is_cross: bool = True, *, risk_increasing: bool = True
    ) -> ExecutionReport:
        cloid = deterministic_cloid("clock-rollback-leverage", coin, leverage, is_cross)
        blocked = self._before_signed_boundary(
            "update_leverage",
            risk_increasing,
            intent_id=f"leverage:{coin}:{leverage}",
            cloid=cloid,
        )
        if blocked is not None:
            return blocked
        return super().update_leverage(
            coin,
            leverage,
            is_cross=is_cross,
            risk_increasing=risk_increasing,
        )

    def schedule_cancel(self, *, scheduled_time_ms, intent_id, cloid) -> ExecutionReport:
        blocked = self._before_signed_boundary(
            "schedule_cancel",
            False,
            intent_id=intent_id,
            cloid=cloid,
        )
        if blocked is not None:
            return blocked
        return super().schedule_cancel(
            scheduled_time_ms=scheduled_time_ms,
            intent_id=intent_id,
            cloid=cloid,
        )

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        blocked = self._before_signed_boundary(
            "place_intent",
            not intent.reduce_only,
            intent_id=intent.intent_id,
            cloid=intent.cloid,
        )
        if blocked is not None:
            return blocked
        return super().place_intent(intent)


class FillingRejectingDeadManClearAdapter(FillingActiveSmokeAdapter):
    def schedule_cancel(self, *, scheduled_time_ms, intent_id, cloid) -> ExecutionReport:
        if scheduled_time_ms is not None:
            return super().schedule_cancel(
                scheduled_time_ms=scheduled_time_ms,
                intent_id=intent_id,
                cloid=cloid,
            )
        report = ExecutionReport(
            report_id=deterministic_cloid("reject-active-dead-man-clear", cloid),
            intent_id=intent_id,
            cloid=cloid,
            status=IntentStatus.REJECTED,
            exchange_status="dead_man_rejected",
            exchange_ts_ms=now_ms(),
            payload={"error": "dead-man clear rejected"},
        )
        self.reports.append(report)
        self.schedule_cancel_reports.append(report)
        return report


class AmbiguousFilledActiveSmokeAdapter(FillingActiveSmokeAdapter):
    def __init__(self):
        super().__init__()
        self.raised_entry = False

    def place_limit_order(self, **kwargs) -> ExecutionReport:
        if not kwargs.get("reduce_only") and not self.raised_entry:
            self.raised_entry = True
            coin = str(kwargs["coin"])
            self.positions[coin] = Position(
                coin=coin,
                size=Decimal(kwargs["size"]),
                entry_px=Decimal(kwargs["price"]),
                updated_ms=now_ms(),
            )
            raise RuntimeError("entry response lost after exchange accepted the order")
        return super().place_limit_order(**kwargs)


class RetriedCleanupActiveSmokeAdapter(FillingActiveSmokeAdapter):
    def __init__(self):
        super().__init__()
        self.cleanup_calls = 0

    def place_limit_order(self, **kwargs) -> ExecutionReport:
        if not kwargs.get("reduce_only"):
            return super().place_limit_order(**kwargs)
        self.cleanup_calls += 1
        coin = str(kwargs["coin"])
        cloid = str(kwargs["cloid"])
        requested = Decimal(kwargs["size"])
        current = self.positions[coin]
        if self.cleanup_calls < 3:
            filled = current.size / Decimal("2")
            self.positions[coin] = replace(
                current,
                size=current.size - filled,
                updated_ms=now_ms(),
            )
            status = IntentStatus.ACKED
            exchange_status = "partial_fill"
            self.status_by_cloid[cloid] = {"status": "open"}
        else:
            filled = current.size
            self.positions.pop(coin, None)
            status = IntentStatus.FILLED
            exchange_status = "filled"
            self.status_by_cloid[cloid] = {"status": "filled"}
        self.balance -= Decimal("0.004115")
        report = ExecutionReport(
            report_id=deterministic_cloid("retried-active-cleanup", cloid, self.cleanup_calls),
            intent_id="limit:" + cloid,
            cloid=cloid,
            status=status,
            exchange_status=exchange_status,
            exchange_ts_ms=now_ms(),
            payload={
                "coin": coin,
                "side": kwargs["side"],
                "size": requested,
                "price": kwargs["price"],
                "tif": kwargs.get("tif", "Ioc"),
                "reduce_only": True,
                "expected_size": requested,
                "filled_size": filled,
            },
        )
        self.reports.append(report)
        return report


class PartialFillAdapter(FakeExecutionAdapter):
    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        report = ExecutionReport(
            report_id=deterministic_cloid("partial-report", intent.intent_id, len(self.reports)),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.ACKED,
            exchange_status="partial_fill",
            exchange_ts_ms=now_ms(),
            payload={
                "intent": intent,
                "expected_size": intent.size,
                "filled_size": intent.size / Decimal("2"),
            },
        )
        self.reports.append(report)
        self.status_by_cloid[intent.cloid] = {"status": "open", "order": {"cloid": intent.cloid}}
        return report


class AtomicHip3FakeExecutionAdapter(FakeExecutionAdapter):
    supports_atomic_hip3_dispatch = True

    def __init__(self):
        super().__init__()
        self.pre_send_check: Any = None

    def set_pre_send_check(self, callback) -> None:
        self.pre_send_check = callback

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        assert self.pre_send_check is not None
        resolved = self.pre_send_check("place_intent", True)
        assert isinstance(resolved, FollowerIntent)
        return super().place_intent(resolved)


def hip3_ioc_no_match_response(*, asset: int = 110022) -> dict[str, Any]:
    return {
        "status": "ok",
        "response": {
            "type": "order",
            "data": {
                "statuses": [
                    {
                        "error": (
                            "Order could not immediately match against any resting orders. "
                            f"asset={asset}"
                        )
                    }
                ]
            },
        },
    }


def hip3_ioc_zero_fill_status(
    intent: FollowerIntent,
    *,
    oid: int,
    variant: str = "matching",
    observed_ms: int | None = None,
) -> dict[str, Any]:
    if variant == "unknown":
        return {"status": "unknown"}
    if variant == "unknown_oid":
        return {"status": "unknownOid"}
    observed = now_ms() if observed_ms is None else observed_ms
    payload: dict[str, Any] = {
        "status": "order",
        "order": {
            "status": "iocCancelRejected",
            "statusTimestamp": observed,
            "order": {
                "coin": intent.coin,
                "side": "B" if intent.side == "buy" else "A",
                "limitPx": str(intent.price),
                "sz": str(intent.size),
                "oid": oid,
                "timestamp": observed,
                "origSz": str(intent.size),
                "cloid": intent.cloid,
                "reduceOnly": intent.reduce_only,
                "children": [],
                "tif": "Ioc",
                "orderType": "Limit",
            },
        },
    }
    order = payload["order"]["order"]
    if variant == "partial_fill":
        order["sz"] = str(intent.size / Decimal("2"))
    elif variant == "cloid_mismatch":
        order["cloid"] = "0x" + "f" * 32
    elif variant != "matching":
        raise ValueError(f"unsupported zero-fill status variant {variant!r}")
    return payload


def hip3_ioc_no_match_report(
    intent: FollowerIntent,
    *,
    attempt: int,
    report_kind: str = "hip3-ioc-race",
) -> ExecutionReport:
    response = hip3_ioc_no_match_response()
    return ExecutionReport(
        report_id=deterministic_cloid(report_kind, intent.intent_id, intent.cloid, attempt),
        intent_id=intent.intent_id,
        cloid=intent.cloid,
        status=IntentStatus.REJECTED,
        exchange_status="rejected",
        exchange_ts_ms=now_ms(),
        payload={
            "response": response,
            "expected_size": intent.size,
            "order_request": {
                "coin": intent.coin,
                "side": intent.side,
                "size": intent.size,
                "price": intent.price,
                "reduce_only": intent.reduce_only,
                "tif": "Ioc",
            },
        },
    )


class Hip3IocRaceAdapter(AtomicHip3FakeExecutionAdapter):
    def __init__(
        self,
        *,
        no_fill_attempts: int = 1,
        order_status_variant: str = "matching",
        order_status_age_ms: int = 0,
    ):
        super().__init__()
        self.no_fill_attempts = no_fill_attempts
        self.order_status_variant = order_status_variant
        self.order_status_age_ms = order_status_age_ms
        self.dispatched_intents: list[FollowerIntent] = []
        self.reconcile_calls = 0
        self.reconcile_calls_at_dispatch: list[int] = []

    def reconcile(self) -> ReconcileSnapshot:
        self.reconcile_calls += 1
        return super().reconcile()

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        assert self.pre_send_check is not None
        resolved = self.pre_send_check("place_intent", True)
        effective = resolved if isinstance(resolved, FollowerIntent) else intent
        self.dispatched_intents.append(effective)
        self.reconcile_calls_at_dispatch.append(self.reconcile_calls)
        attempt = len(self.dispatched_intents)
        if attempt <= self.no_fill_attempts:
            report = hip3_ioc_no_match_report(effective, attempt=attempt)
            self.status_by_cloid[effective.cloid] = hip3_ioc_zero_fill_status(
                effective,
                oid=110_000 + attempt,
                variant=self.order_status_variant,
                observed_ms=now_ms() - self.order_status_age_ms,
            )
            self.reports.append(report)
            return report
        forced_status = cast(IntentStatus | None, getattr(self, "forced_status", None))
        self.forced_status = IntentStatus.FILLED
        try:
            return FakeExecutionAdapter.place_intent(self, effective)
        finally:
            self.forced_status = forced_status


class Hip3CleanupIocRaceAdapter(FakeExecutionAdapter):
    def __init__(self, *, coin: str, size: Decimal):
        super().__init__(
            positions={coin: Position(coin=coin, size=size, entry_px=Decimal("100"), leverage=1)}
        )
        self.cleanup_intents: list[FollowerIntent] = []

    def place_limit_order(self, **kwargs) -> ExecutionReport:
        coin = str(kwargs["coin"])
        current = self.positions[coin]
        intent = FollowerIntent(
            intent_id="limit:" + str(kwargs["cloid"]),
            cloid=str(kwargs["cloid"]),
            action=IntentAction.REDUCE,
            coin=coin,
            side=str(kwargs["side"]),
            size=Decimal(kwargs["size"]),
            price=Decimal(kwargs["price"]),
            reduce_only=bool(kwargs["reduce_only"]),
            mode=Mode.TESTNET,
            source_event_key="bounded-cleanup",
            reason="test bounded cleanup IOC race",
            created_ms=now_ms(),
        )
        self.cleanup_intents.append(intent)
        attempt = len(self.cleanup_intents)
        if attempt == 1:
            report = hip3_ioc_no_match_report(
                intent,
                attempt=attempt,
                report_kind="hip3-cleanup-ioc-race",
            )
            self.status_by_cloid[intent.cloid] = hip3_ioc_zero_fill_status(
                intent,
                oid=120_000 + attempt,
            )
            self.reports.append(report)
            return report

        signed_delta = intent.size if intent.side == "buy" else -intent.size
        remaining = current.size + signed_delta
        if remaining == 0 or remaining * current.size < 0:
            self.positions.pop(coin, None)
        else:
            self.positions[coin] = replace(current, size=remaining, updated_ms=now_ms())
        report = ExecutionReport(
            report_id=deterministic_cloid("hip3-cleanup-fill", intent.cloid, attempt),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.FILLED,
            exchange_status="filled",
            exchange_ts_ms=now_ms(),
            payload={
                "expected_size": intent.size,
                "filled_size": intent.size,
                "order_request": {
                    "coin": intent.coin,
                    "side": intent.side,
                    "size": intent.size,
                    "price": intent.price,
                    "reduce_only": True,
                    "tif": "Ioc",
                },
            },
        )
        self.status_by_cloid[intent.cloid] = {"status": "filled"}
        self.reports.append(report)
        return report


class DustPartialFillAdapter(FakeExecutionAdapter):
    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        filled_size = intent.size - Decimal("0.0001")
        report = ExecutionReport(
            report_id=deterministic_cloid(
                "dust-partial-report", intent.intent_id, len(self.reports)
            ),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.ACKED,
            exchange_status="partial_fill",
            exchange_ts_ms=now_ms(),
            payload={
                "intent": intent,
                "expected_size": intent.size,
                "filled_size": filled_size,
            },
        )
        self.reports.append(report)
        self.status_by_cloid[intent.cloid] = {"status": "filled"}
        current = self.positions.get(
            intent.coin,
            Position(
                intent.coin,
                Decimal("0"),
                leverage=self.configured_leverage.get(intent.coin),
            ),
        )
        signed_delta = filled_size if intent.side == "buy" else -filled_size
        next_size = current.size + signed_delta
        if intent.reduce_only and current.size != 0 and next_size * current.size < 0:
            next_size = Decimal("0")
        if next_size == 0:
            self.positions.pop(intent.coin, None)
        else:
            self.positions[intent.coin] = Position(
                intent.coin,
                next_size,
                entry_px=intent.price or current.entry_px,
                leverage=self.configured_leverage.get(intent.coin, current.leverage),
                updated_ms=now_ms(),
            )
        return report


class FilledOrderAdapter(FakeExecutionAdapter):
    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        forced_status = cast(IntentStatus | None, getattr(self, "forced_status", None))
        self.forced_status = IntentStatus.FILLED
        try:
            return super().place_intent(intent)
        finally:
            self.forced_status = forced_status


class CapAwareAtomicHip3FakeExecutionAdapter(FilledOrderAdapter):
    supports_atomic_hip3_dispatch = True

    def __init__(self):
        super().__init__()
        self.pre_send_check: Any = None
        self.signed_order_calls = 0

    def set_pre_send_check(self, callback) -> None:
        self.pre_send_check = callback

    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        assert self.pre_send_check is not None
        try:
            resolved = self.pre_send_check("place_intent", True)
        except PreSendBlockedError as exc:
            return ExecutionReport(
                report_id=deterministic_cloid(
                    "cap-aware-pre-send-blocked",
                    intent.intent_id,
                    intent.cloid,
                    str(exc),
                ),
                intent_id=intent.intent_id,
                cloid=intent.cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=now_ms(),
                payload={"error": str(exc), "signed_action_performed": False},
            )
        assert isinstance(resolved, FollowerIntent)
        self.signed_order_calls += 1
        return super().place_intent(resolved)


class OverfillAdapter(FakeExecutionAdapter):
    def place_intent(self, intent: FollowerIntent) -> ExecutionReport:
        report = ExecutionReport(
            report_id=deterministic_cloid("overfill-report", intent.intent_id, len(self.reports)),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.ACKED,
            exchange_status="overfill",
            exchange_ts_ms=now_ms(),
            payload={
                "intent": intent,
                "expected_size": intent.size,
                "filled_size": intent.size + Decimal("0.001"),
            },
        )
        self.reports.append(report)
        self.status_by_cloid[intent.cloid] = {"status": "open", "order": {"cloid": intent.cloid}}
        return report


def append_desired(store, *, mode=Mode.TESTNET, btc_size=Decimal("0.005")):
    desired = DesiredState(
        state_id=deterministic_cloid("desired-test", mode.value, btc_size, now_ms()),
        source_event_key="source-before",
        mode=mode,
        positions={"BTC": Position("BTC", btc_size, entry_px=Decimal("50000"), leverage=2)}
        if btc_size
        else {},
        reason="test baseline",
        created_ms=now_ms(),
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        action_account="0xf000000000000000000000000000000000000000",
        source_network="testnet" if mode == Mode.TESTNET else "mainnet",
    )
    store.append_desired_state(desired)
    store.commit_desired_state(desired.state_id)
    return desired


def append_desired_positions(store, positions: dict[str, Position]) -> DesiredState:
    desired = DesiredState(
        state_id=deterministic_cloid("desired-positions-test", positions, now_ms()),
        source_event_key="source-before",
        mode=Mode.TESTNET,
        positions=positions,
        reason="test multi-market baseline",
        created_ms=now_ms(),
        source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c",
        action_account="0xf000000000000000000000000000000000000000",
        source_network="testnet",
    )
    assert store.append_desired_state(desired)
    store.commit_desired_state(desired.state_id)
    return desired


def bind_testnet_scope(store, *, source_wallet="0xcf7c4feb434751146a48b895e96caeb15838f92c"):
    bound, detail = store.ensure_journal_scope(
        {
            "source_wallet": source_wallet,
            "source_network": "testnet",
            "action_account": "0xf000000000000000000000000000000000000000",
            "execution_network": "testnet",
        }
    )
    assert bound, detail


def two_hip3_open_service_with_first_market_illiquid(base_config, store):
    """Build KR200-before-SKHX ordering with depth available only for SKHX."""

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(
            base_config.risk,
            allowed_symbols=("xyz:KR200", "xyz:SKHX"),
            fixed_multiplier=Decimal("0.001"),
            balance_sizing_enabled=False,
            min_order_size=Decimal("0.0001"),
        ),
        ops=replace(
            base_config.ops,
            max_new_intents_per_cycle=1,
            max_open_intents=1,
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}]}
    info.state = {"assetPositions": [], "marginSummary": {"accountValue": "1000"}}
    info.dex_states["xyz"] = {
        "assetPositions": [
            {
                "position": {
                    "coin": "KR200",
                    "szi": "-20",
                    "entryPx": "1108.06",
                    "leverage": {"type": "cross", "value": 2},
                }
            },
            {
                "position": {
                    "coin": "SKHX",
                    "szi": "-37",
                    "entryPx": "1252.544",
                    "leverage": {"type": "cross", "value": 2},
                }
            },
        ],
        "marginSummary": {"accountValue": "1000"},
    }
    info.dex_mids["xyz"] = {"xyz:KR200": "1108", "xyz:SKHX": "1252.5"}
    info.dex_meta["xyz"] = {
        "universe": [
            {"name": "KR200", "szDecimals": 4, "maxLeverage": 20},
            {"name": "SKHX", "szDecimals": 3, "maxLeverage": 20},
        ]
    }
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [
            {"oraclePx": "1108", "markPx": "1108", "midPx": "1108"},
            {"oraclePx": "1252.5", "markPx": "1252.5", "midPx": "1252.5"},
        ],
    ]
    observed = now_ms()
    info.books["xyz:KR200"] = {
        "coin": "xyz:KR200",
        "time": observed,
        "levels": [
            [{"px": "1090", "sz": "10", "n": 1}],
            [{"px": "1109", "sz": "10", "n": 1}],
        ],
    }
    info.books["xyz:SKHX"] = {
        "coin": "xyz:SKHX",
        "time": observed,
        "levels": [
            [{"px": "1252", "sz": "10", "n": 1}],
            [{"px": "1253", "sz": "10", "n": 1}],
        ],
    }
    adapter = FilledOrderAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.dex_states["xyz"]},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )
    return service, adapter


def hip3_ioc_race_service(
    base_config,
    store,
    *,
    adapter: Hip3IocRaceAdapter,
    circuit_breaker_failure_threshold: int | None = None,
):
    initial_service, _ = two_hip3_open_service_with_first_market_illiquid(base_config, store)
    info = cast(FakeInfoClient, initial_service.info_client)
    config = initial_service.config
    if circuit_breaker_failure_threshold is not None:
        config = replace(
            config,
            ops=replace(
                config.ops,
                circuit_breaker_failure_threshold=circuit_breaker_failure_threshold,
            ),
        )
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.dex_states["xyz"]},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )
    return service, info


def three_hip3_open_service_with_first_market_illiquid(base_config, store):
    """Extend the two-market fixture with a second independently liquid OPEN."""

    initial_service, adapter = two_hip3_open_service_with_first_market_illiquid(
        base_config,
        store,
    )
    info = cast(FakeInfoClient, initial_service.info_client)
    config = replace(
        initial_service.config,
        risk=replace(
            initial_service.config.risk,
            allowed_symbols=("xyz:KR200", "xyz:SKHX", "xyz:US500"),
        ),
    )
    info.dex_states["xyz"]["assetPositions"].append(
        {
            "position": {
                "coin": "US500",
                "szi": "-2",
                "entryPx": "5000",
                "leverage": {"type": "cross", "value": 2},
            }
        }
    )
    info.dex_mids["xyz"]["xyz:US500"] = "5000"
    info.dex_meta["xyz"]["universe"].append({"name": "US500", "szDecimals": 3, "maxLeverage": 20})
    info.dex_meta_and_contexts["xyz"][1].append(
        {"oraclePx": "5000", "markPx": "5000", "midPx": "5000"}
    )
    info.books["xyz:US500"] = {
        "coin": "xyz:US500",
        "time": now_ms(),
        "levels": [
            [{"px": "4999", "sz": "10", "n": 1}],
            [{"px": "5001", "sz": "10", "n": 1}],
        ],
    }
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.dex_states["xyz"]},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )
    return service, adapter


def hip3_short_cap_service(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(
            base_config.risk,
            allowed_symbols=("xyz:CAP",),
            fixed_multiplier=Decimal("1"),
            balance_sizing_enabled=False,
            max_notional_usd=Decimal("15"),
            max_gross_exposure_usd=Decimal("40"),
            min_order_size=Decimal("0.001"),
            hip3_oracle_envelope_bps=Decimal("100"),
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}]}
    info.state = {"assetPositions": [], "marginSummary": {"accountValue": "1000"}}
    info.dex_states["xyz"] = {
        "assetPositions": [
            {
                "position": {
                    "coin": "CAP",
                    "szi": "-1",
                    "entryPx": "100",
                    "leverage": {"type": "cross", "value": 1},
                }
            }
        ],
        "marginSummary": {"accountValue": "1000"},
    }
    info.dex_mids["xyz"] = {"xyz:CAP": "100"}
    info.dex_meta["xyz"] = {"universe": [{"name": "CAP", "szDecimals": 3, "maxLeverage": 20}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:CAP"] = {
        "coin": "xyz:CAP",
        "time": now_ms(),
        "levels": [
            [{"px": "100.5", "sz": "1", "n": 1}],
            [{"px": "100.6", "sz": "1", "n": 1}],
        ],
    }
    adapter = CapAwareAtomicHip3FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.dex_states["xyz"]},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )
    return service, adapter, info


def move_hip3_short_cap_market(info: FakeInfoClient, *, multi_level: bool = False) -> None:
    info.dex_meta_and_contexts["xyz"][1][0].update(
        {"oraclePx": "101", "markPx": "101", "midPx": "101"}
    )
    info.books["xyz:CAP"] = {
        "coin": "xyz:CAP",
        "time": now_ms(),
        "levels": [
            (
                [
                    {"px": "102", "sz": "0.147", "n": 1},
                    {"px": "100.5", "sz": "0.001", "n": 1},
                ]
                if multi_level
                else [{"px": "102", "sz": "1", "n": 1}]
            ),
            [{"px": "102.01", "sz": "1", "n": 1}],
        ],
    }


def append_corrupt_desired(store, *, mode=Mode.TESTNET):
    with store.lock:
        with store.conn:
            store.conn.execute(
                """
                INSERT INTO desired_states(state_id, source_event_key, mode, payload_json, created_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    deterministic_cloid("bad-desired", mode.value, now_ms()),
                    "source-before",
                    mode.value,
                    (
                        '{"positions":{"BTC":{"coin":"BTC","size":"not-a-number"}},'
                        '"source_wallet":"0xcf7c4feb434751146a48b895e96caeb15838f92c",'
                        '"action_account":"0xf000000000000000000000000000000000000000",'
                        '"source_network":"testnet"}'
                    ),
                    now_ms(),
                ),
            )
            state_id = store.conn.execute(
                "SELECT state_id FROM desired_states ORDER BY seq DESC LIMIT 1"
            ).fetchone()[0]
            store.conn.execute(
                "INSERT INTO desired_state_commits(state_id, committed_ms) VALUES (?, ?)",
                (state_id, now_ms()),
            )


def test_service_uses_configured_info_timeout_for_default_source_client(base_config, store):
    config = replace(base_config, ops=OpsConfig(info_timeout_s=Decimal("2.5")))

    service = CopyTraderService(config, store=store)

    assert getattr(service.info_client, "timeout_s") == 2.5


def test_testnet_service_observes_mainnet_source_while_execution_stays_testnet(base_config, store):
    config = replace(base_config, mode=Mode.TESTNET, source_network=SourceNetwork.MAINNET)

    service = CopyTraderService(
        config,
        store=store,
        execution_adapter=FakeExecutionAdapter(),
    )

    assert config.rest_url == TESTNET_REST
    assert getattr(service.info_client, "base_url") == MAINNET_REST
    assert service.observer.ws_url == MAINNET_WS


def test_readiness_truth_refresh_captures_source_after_paced_follower(
    base_config, store, monkeypatch
):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(account=follower)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    calls: list[str] = []

    def execution_mids() -> dict[str, Decimal]:
        calls.append("execution_mids")
        return {"BTC": Decimal("50000")}

    def follower_reconcile() -> ReconcileSnapshot:
        calls.append("follower")
        return ReconcileSnapshot(
            snapshot_id="paced-follower",
            account=follower,
            positions={},
            open_orders=[],
            observed_ms=now_ms(),
            source="fake",
        )

    def source_reconcile() -> SourceSnapshot:
        calls.append("source")
        observed = now_ms()
        return SourceSnapshot(
            positions={},
            open_orders=[],
            mids={"BTC": Decimal("50000")},
            observed_ms=observed,
            state_key="source-state",
            planning_key="source-plan",
            raw_state={},
        )

    monkeypatch.setattr(service, "load_execution_mids", execution_mids)
    monkeypatch.setattr(adapter, "reconcile", follower_reconcile)
    monkeypatch.setattr(service.observer, "reconcile_once", source_reconcile)

    result = service.refresh_readiness_truth()

    assert result["passed"] is True
    assert calls == ["execution_mids", "follower", "source"]


def test_live_readiness_truth_refresh_captures_follower_after_slower_source(
    base_config, store, monkeypatch
):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.LIVE,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            vault_address=follower,
            expected_account_mode=AccountMode.STANDARD,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=False,
        ),
    )
    adapter = FakeExecutionAdapter(account=follower)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    calls: list[str] = []

    def execution_mids() -> dict[str, Decimal]:
        calls.append("execution_mids")
        return {"BTC": Decimal("50000")}

    def source_reconcile() -> SourceSnapshot:
        calls.append("source")
        return SourceSnapshot(
            positions={},
            open_orders=[],
            mids={"BTC": Decimal("50000")},
            observed_ms=now_ms(),
            state_key="live-source-state",
            planning_key="live-source-plan",
            raw_state={},
        )

    def follower_reconcile() -> ReconcileSnapshot:
        calls.append("follower")
        return ReconcileSnapshot(
            snapshot_id="live-final-follower",
            account=follower,
            positions={},
            open_orders=[],
            observed_ms=now_ms(),
            source="fake",
        )

    monkeypatch.setattr(service, "load_execution_mids", execution_mids)
    monkeypatch.setattr(service.observer, "reconcile_once", source_reconcile)
    monkeypatch.setattr(adapter, "reconcile", follower_reconcile)
    monkeypatch.setattr(
        service,
        "preflight",
        lambda **_kwargs: PreflightReport(mode=Mode.LIVE, passed=True),
    )

    result = service.refresh_readiness_truth()

    assert result["source"] is not None
    assert result["follower"] is not None
    assert calls == ["execution_mids", "source", "follower"]


def test_readiness_uses_latest_follower_reconcile_without_recent_rows(base_config, store):
    observed = now_ms()
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    store.append_source_event(
        SourceEvent(
            idempotency_key="fresh-source",
            event_type=SourceEventType.SNAPSHOT,
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={"event_subtype": "rest_snapshot"},
        )
    )
    store.append_reconcile_snapshot(
        ReconcileSnapshot(
            snapshot_id="fresh-follower",
            account="0xf000000000000000000000000000000000000000",
            positions={},
            open_orders=[],
            observed_ms=observed,
            source="fake",
        )
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    readiness = service.readiness()

    follower_check = next(
        check for check in readiness["checks"] if check["name"] == "follower_reconciliation_fresh"
    )
    assert follower_check["passed"] is True
    assert "follower=fresh" in follower_check["detail"]


def test_dashboard_exposes_reduced_fidelity_source_dex_context(base_config, store):
    observed = now_ms()
    config = replace(
        base_config,
        source_dex_scope=SourceDexScope.DEFAULT_ONLY_ACCOUNT_EQUITY,
        risk=replace(base_config.risk, sizing_equity_cap_usd=Decimal("50")),
    )
    store.append_source_event(
        SourceEvent(
            idempotency_key="unified-source-context",
            event_type=SourceEventType.RECONCILE,
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={
                "unified_aggregate": {
                    "source_dex_scope": "default_only_account_equity",
                    "positions_scope": "default_perp_dex",
                    "account_value_basis": "total_unified_spot_usdc",
                    "fidelity": "reduced_non_default_positions_excluded",
                    "active_non_default_dexes": ["xyz"],
                }
            },
        )
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())

    context = service.dashboard(include_recent=False)["source_dex_context"]

    assert context["configured_scope"] == "default_only_account_equity"
    assert context["positions_scope"] == "default_perp_dex"
    assert context["account_value_basis"] == "total_unified_spot_usdc"
    assert context["active_non_default_dexes"] == ["xyz"]
    assert context["reduced_fidelity"] is True


def test_account_context_accepts_configured_hip3_but_rejects_unknown_dex(base_config, store):
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("BTC", "xyz:AAPL")),
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    observed = now_ms()
    store.append_reconcile_snapshot(
        ReconcileSnapshot(
            snapshot_id="configured-hip3-reconcile",
            account=follower,
            positions={"xyz:AAPL": Position("xyz:AAPL", Decimal("0.1"))},
            open_orders=[],
            observed_ms=observed,
            source="test",
            payload={
                "account_mode": "unified",
                "account_context": {
                    "detected_mode": "unified",
                    "collateral_source": "spot_usdc_unified",
                    "account_value": "100",
                    "active_non_default_dexes": ["xyz"],
                    "unsupported_non_default_dexes": [],
                },
            },
        )
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    configured = service._account_context_status()

    assert configured["status"] == "ready"
    assert configured["active_non_default_dexes"] == ["xyz"]
    assert configured["unsupported_non_default_dexes"] == []

    store.append_reconcile_snapshot(
        ReconcileSnapshot(
            snapshot_id="unknown-hip3-reconcile",
            account=follower,
            positions={"other:FOO": Position("other:FOO", Decimal("1"))},
            open_orders=[],
            observed_ms=observed + 1,
            source="test",
            payload={
                "account_mode": "unified",
                "account_context": {
                    "detected_mode": "unified",
                    "collateral_source": "spot_usdc_unified",
                    "account_value": "100",
                    "active_non_default_dexes": ["other"],
                },
            },
        )
    )

    unknown = service._account_context_status()

    assert unknown["status"] == "non_default_dex_activity"
    assert unknown["unsupported_non_default_dexes"] == ["other"]


def test_readiness_rejects_fresh_reconcile_for_wrong_follower_account(base_config, store):
    observed = now_ms()
    follower = "0xf000000000000000000000000000000000000000"
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    store.append_source_event(
        SourceEvent(
            idempotency_key="fresh-source-wrong-follower",
            event_type=SourceEventType.SNAPSHOT,
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={"event_subtype": "rest_snapshot"},
        )
    )
    store.append_reconcile_snapshot(
        ReconcileSnapshot(
            snapshot_id="fresh-wrong-follower",
            account="0xe000000000000000000000000000000000000000",
            positions={},
            open_orders=[],
            observed_ms=observed,
            source="fake",
        )
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    dashboard = service.dashboard(include_recent=False)
    readiness = service.readiness()

    follower_status = dashboard["reconciliation_status"]["follower"]
    assert follower_status["status"] == "mismatch"
    assert follower_status["expected_account"] == follower
    assert follower_status["account"] == "0xe000000000000000000000000000000000000000"
    assert "follower_mismatch" in dashboard["reconciliation_status"]["blockers"]
    follower_check = next(
        check for check in readiness["checks"] if check["name"] == "follower_reconciliation_fresh"
    )
    assert follower_check["passed"] is False
    assert "follower=mismatch" in follower_check["detail"]


def test_exchange_action_gate_refreshes_external_safe_mode_transition(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    external = SafeModeController(store)
    external.trip(SafeModeReason.WEBSOCKET_DISCONNECT, "external runner disconnected")

    decision = service._runtime_allows_exchange_action(count_rate=False)

    assert decision.ok is False
    assert decision.reason == SafeModeReason.WEBSOCKET_DISCONNECT
    assert service.safe_mode.enabled is True


def test_validation_supervisor_lease_is_a_fail_closed_new_risk_boundary(
    base_config, store, tmp_path, monkeypatch
):
    follower = "0xf000000000000000000000000000000000000000"
    peer_follower = "0xe000000000000000000000000000000000000000"
    incarnation_id = "validation-incarnation-1"
    lease_path = tmp_path / "supervisor.json"
    deadline_ms = now_ms() + 60_000
    identity = "a" * 64
    effective_config_sha256 = "c" * 64
    effective_config_set_sha256 = "d" * 64
    registry_path = tmp_path / "controller-registry.sqlite3"
    registry = ControllerRegistry(registry_path)
    claims = [
        ControllerClaim(
            follower=account,
            owner_token="opaque-owner-token",
            run_id="validation-run-1",
            state_identity_sha256=identity,
            deadline_ms=deadline_ms,
        )
        for account in (follower, peer_follower)
    ]
    assert registry.acquire_exclusive_set(
        claims,
        incarnation_id=incarnation_id,
        observed_ms=now_ms(),
        ttl_ms=30_000,
    )[0]
    registry.close()
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            validation_supervisor_lease_path=lease_path,
            validation_controller_registry_path=registry_path,
            validation_run_id="validation-run-1",
            validation_owner_token="opaque-owner-token",
            validation_supervisor_incarnation_id=incarnation_id,
            validation_follower_set=tuple(sorted((follower, peer_follower))),
            validation_state_identity_sha256=identity,
            validation_effective_config_sha256=effective_config_sha256,
            validation_effective_config_set_sha256=effective_config_set_sha256,
            validation_deadline_ms=deadline_ms,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    missing = service._runtime_allows_exchange_action(count_rate=False)
    assert missing.ok is False
    assert missing.reason == SafeModeReason.LIVE_BLOCKED
    assert "lease is unavailable" in missing.detail
    assert service._runtime_allows_exchange_action(count_rate=False, risk_reducing=True).ok is True

    observed = now_ms()
    lease_path.write_text(
        json.dumps(
            {
                "version": 1,
                "run_id": "validation-run-1",
                "owner_token": "opaque-owner-token",
                "supervisor_incarnation_id": incarnation_id,
                "state_identity_sha256": identity,
                "effective_config_sha256": effective_config_sha256,
                "effective_config_set_sha256": effective_config_set_sha256,
                "follower_account_address": follower,
                "deadline_ms": deadline_ms,
                "heartbeat_ms": observed,
                "expires_ms": observed + 15_000,
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    assert service._runtime_allows_exchange_action(count_rate=False).ok is True

    real_open = validation_guardian.Path.open
    transient_read_calls = 0

    def transient_lease_read(path, *args, **kwargs):
        nonlocal transient_read_calls
        mode = args[0] if args else kwargs.get("mode")
        if path == lease_path and mode in {"r", "rb"}:
            transient_read_calls += 1
            if transient_read_calls == 1:
                raise PermissionError(13, "Permission denied", str(lease_path))
        return real_open(path, *args, **kwargs)

    with monkeypatch.context() as injected:
        injected.setattr(validation_guardian.Path, "open", transient_lease_read)
        transient = service._runtime_allows_exchange_action(count_rate=False)

    assert transient.ok is True
    assert transient_read_calls == 2
    assert store.latest_safe_mode() is None

    denied_read_calls = 0

    def denied_lease_read(path, *args, **kwargs):
        nonlocal denied_read_calls
        mode = args[0] if args else kwargs.get("mode")
        if path == lease_path and mode in {"r", "rb"}:
            denied_read_calls += 1
            raise PermissionError(13, "Permission denied", str(lease_path))
        return real_open(path, *args, **kwargs)

    with monkeypatch.context() as injected:
        injected.setattr(validation_guardian, "_SUPERVISOR_LEASE_READ_RETRY_S", 0.025)
        injected.setattr(validation_guardian.Path, "open", denied_lease_read)
        exhausted = service._runtime_allows_exchange_action(count_rate=False)
        reduction = service._runtime_allows_exchange_action(
            count_rate=False,
            risk_reducing=True,
        )

    assert denied_read_calls >= 2
    assert exhausted.ok is False
    assert exhausted.reason == SafeModeReason.LIVE_BLOCKED
    assert "lease is unavailable" in exhausted.detail
    assert reduction.ok is True
    assert store.latest_safe_mode() is None

    payload = json.loads(lease_path.read_text(encoding="utf-8"))
    payload["effective_config_sha256"] = "e" * 64
    lease_path.write_text(json.dumps(payload), encoding="utf-8")
    mismatched_config = service._runtime_allows_exchange_action(count_rate=False)
    assert mismatched_config.ok is False
    assert "effective_config_sha256" in mismatched_config.detail

    payload["effective_config_sha256"] = effective_config_sha256
    payload["status"] = "containment"
    lease_path.write_text(json.dumps(payload), encoding="utf-8")
    containment = service._runtime_allows_exchange_action(count_rate=False)
    assert containment.ok is False
    assert "does not permit new risk" in containment.detail


def test_validation_supervisor_identity_is_rechecked_at_last_mile(base_config, store, tmp_path):
    follower = "0xf000000000000000000000000000000000000000"
    lease_path = tmp_path / "supervisor.json"
    observed = now_ms()
    deadline_ms = observed + 60_000
    effective_config_sha256 = "c" * 64
    effective_config_set_sha256 = "d" * 64
    incarnation_id = "validation-incarnation-1"
    lease_path.write_text(
        json.dumps(
            {
                "version": 1,
                "run_id": "wrong-run",
                "owner_token": "opaque-owner-token",
                "supervisor_incarnation_id": incarnation_id,
                "state_identity_sha256": "b" * 64,
                "effective_config_sha256": effective_config_sha256,
                "effective_config_set_sha256": effective_config_set_sha256,
                "follower_account_address": follower,
                "deadline_ms": deadline_ms,
                "heartbeat_ms": observed,
                "expires_ms": observed + 15_000,
                "status": "running",
            }
        ),
        encoding="utf-8",
    )
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=follower,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            validation_supervisor_lease_path=lease_path,
            validation_run_id="expected-run",
            validation_owner_token="opaque-owner-token",
            validation_supervisor_incarnation_id=incarnation_id,
            validation_state_identity_sha256="b" * 64,
            validation_effective_config_sha256=effective_config_sha256,
            validation_effective_config_set_sha256=effective_config_set_sha256,
            validation_deadline_ms=deadline_ms,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    service._active_plan_source_observed_ms = observed
    service._active_plan_follower_observed_ms = observed

    with pytest.raises(PreSendBlockedError, match="validation supervisor blocked new risk"):
        service._last_mile_pre_send_check("place_intent", True)

    assert service.safe_mode.reason == SafeModeReason.LIVE_BLOCKED


def test_frozen_market_universe_blocks_tampered_manifest_before_auth_probe(
    base_config, store, tmp_path
):
    manifest, path = _write_frozen_testnet_manifest(tmp_path, {"name": "BTC", "szDecimals": 5})
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["markets"][0]["sz_decimals"] = 4
    path.write_text(json.dumps(payload), encoding="utf-8")
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, allowed_symbols=("BTC",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            validation_market_universe_manifest_path=path,
            validation_market_universe_sha256=manifest.sha256,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FrozenCatalogInfoClient({"name": "BTC", "szDecimals": 5}),
        execution_adapter=adapter,
    )

    preflight = service.preflight(auth_probe=True)

    assert preflight.passed is False
    assert any("validation market universe is invalid" in item for item in preflight.blockers)
    assert adapter.auth_probe_reports == []
    assert service.safe_mode.reason == SafeModeReason.CONFIG_INVALID


def test_frozen_market_additions_are_diagnostic_but_precision_drift_blocks(
    base_config, store, tmp_path
):
    manifest, path = _write_frozen_testnet_manifest(tmp_path, {"name": "BTC", "szDecimals": 5})
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, allowed_symbols=("BTC",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            validation_market_universe_manifest_path=path,
            validation_market_universe_sha256=manifest.sha256,
        ),
    )
    info = FrozenCatalogInfoClient({"name": "BTC", "szDecimals": 5})
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=FakeExecutionAdapter(),
    )

    assert service._validation_market_universe_decision(force=True).ok is True
    info.catalog_meta = {
        "universe": [
            {"name": "BTC", "szDecimals": 5},
            {"name": "ETH", "szDecimals": 4},
        ]
    }
    addition = service._validation_market_universe_decision(force=True)

    assert addition.ok is True
    status = service.validation_market_universe_status()
    assert status["ready"] is True
    assert status["last_observed"]["changed"] is True
    assert status["last_observed"]["blocking_changed"] is False
    assert status["manifest_sha256"] == manifest.sha256

    info.catalog_meta = {
        "universe": [
            {"name": "BTC", "szDecimals": 4},
            {"name": "ETH", "szDecimals": 4},
        ]
    }
    precision_drift = service._validation_market_universe_decision(force=True)

    assert precision_drift.ok is False
    assert precision_drift.reason == SafeModeReason.RISK_LIMIT
    assert "precision_changes" in precision_drift.detail
    assert service._runtime_allows_exchange_action(count_rate=False, risk_reducing=True).ok is True
    assert service.validation_market_universe_status()["last_observed"]["blocking_changed"] is True


def test_frozen_delisted_market_keeps_precision_and_bounded_last_good_mid(
    base_config, store, tmp_path
):
    manifest, path = _write_frozen_testnet_manifest(
        tmp_path,
        {"name": "BTC", "szDecimals": 5},
        {"name": "OLD", "szDecimals": 2},
    )
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, allowed_symbols=("BTC", "OLD")),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            validation_market_universe_manifest_path=path,
            validation_market_universe_sha256=manifest.sha256,
        ),
    )
    info = FrozenCatalogInfoClient(
        {"name": "BTC", "szDecimals": 5},
        {"name": "OLD", "szDecimals": 2},
    )
    info.mids = {"BTC": "50000", "OLD": "10"}
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=FakeExecutionAdapter(),
    )

    assert service.load_execution_mids()["OLD"] == Decimal("10")
    info.mids.pop("OLD")
    service._execution_mids_cache_ms = 0
    mids_after_delist = service.load_execution_mids()

    assert mids_after_delist["OLD"] == Decimal("10")
    assert service.load_asset_meta()["OLD"].sz_decimals == 2
    context = service.validation_market_universe_status()["last_good_contexts"]
    old = next(item for item in context["contexts"] if item["symbol"] == "OLD")
    assert old["mid_px"] == "10"


def test_shadow_mode_observes_and_emits_intents_without_execution(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert result["intents"]
    assert store.count("source_events") == 1
    assert store.count("follower_intents") == 1
    assert store.count("execution_reports") == 0


def test_exchange_cycle_uses_source_and_follower_account_value_for_sizing(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(
            base_config.risk,
            max_notional_usd=Decimal("100000"),
            max_gross_exposure_usd=Decimal("100000"),
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(account_value=Decimal("100"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.run_once()
    dashboard = service.dashboard()

    assert result["desired_state"]["positions"]["BTC"]["size"] == "0.01"
    assert dashboard["sizing"]["mode"] == "balance_scaled"
    assert dashboard["sizing"]["source_account_value"] == "1000"
    assert dashboard["sizing"]["follower_account_value"] == "100"


def test_prepared_crash_recovers_then_rearms_exactly_one_exchange_send(
    base_config, store, monkeypatch
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    original_begin = store.begin_intent_dispatch

    def crash_before_dispatch(intent_id: str):
        if store.conn.execute(
            "SELECT 1 FROM follower_intents WHERE intent_id = ?", (intent_id,)
        ).fetchone():
            raise SystemExit("simulated crash after durable prepare")
        return original_begin(intent_id)

    monkeypatch.setattr(store, "begin_intent_dispatch", crash_before_dispatch)
    with pytest.raises(SystemExit, match="durable prepare"):
        service.run_once()
    assert store.pending_intents(Mode.TESTNET)[0]["attempt_phase"] == "prepared"
    assert not [report for report in adapter.reports if "intent" in report.payload]

    monkeypatch.setattr(store, "begin_intent_dispatch", original_begin)
    recovery = service.settle_pending_intents()
    assert recovery["settled"][0]["exchange_status"] == "recovered:never_dispatched"
    assert recovery["pending_after"] == 0
    cleared = service.manual_reconcile()
    assert cleared["safe_mode"]["cleared"] is True

    rerun = service.run_once()
    order_reports = [report for report in adapter.reports if "intent" in report.payload]

    assert len(order_reports) == 1
    assert order_reports[0].status == IntentStatus.FILLED
    assert rerun["safe_mode"]["enabled"] is False
    assert store.pending_intent_count(Mode.TESTNET) == 0

    stable = service.run_once()
    assert stable["intents"] == []
    assert len([report for report in adapter.reports if "intent" in report.payload]) == 1


def test_shadow_pending_intents_do_not_block_later_testnet_run(base_config, store):
    shadow = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    shadow_result = shadow.run_once()
    assert shadow_result["intents"]
    assert store.pending_intent_count(Mode.SHADOW) == 1

    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"].pop("leverage", None)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)
    result = service.run_once()

    assert result["intents"]
    assert any(report["status"] == "filled" for report in result["reports"])
    assert adapter.reports
    assert store.pending_intent_count(Mode.TESTNET) == 0
    assert service.safe_mode.reason == SafeModeReason.NONE


def test_dashboard_runtime_view_is_scoped_to_active_mode(base_config, store):
    shadow = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    assert shadow.run_once()["intents"]

    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=FakeExecutionAdapter()
    )
    dashboard = service.dashboard()

    assert dashboard["ops"]["pending_intent_count"] == 0
    assert dashboard["runtime_state"]["pending_intents"] == []
    assert dashboard["runtime_state"]["desired_state_count"] == 0
    assert dashboard["recent_intents"] == []
    assert store.count("follower_intents") == 1


def test_shadow_mode_reuses_planning_identity_for_identical_reconciles(
    base_config, store, monkeypatch
):
    counter = {"ticks": 0}

    def increasing_now_ms():
        counter["ticks"] += 1
        return now_ms() + counter["ticks"]

    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", increasing_now_ms)
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())

    first = service.run_once()
    second = service.run_once()

    assert store.count("source_events") == 2
    assert first["desired_state"]["state_id"] == second["desired_state"]["state_id"]
    assert first["intents"][0]["source_event_key"] == second["intents"][0]["source_event_key"]
    assert first["intents"][0]["cloid"] == second["intents"][0]["cloid"]
    assert store.count("desired_states") == 1
    assert store.count("follower_intents") == 1


def test_reentered_source_exposure_gets_fresh_planning_identity(base_config, store, monkeypatch):
    counter = {"ticks": 0}

    def increasing_now_ms():
        counter["ticks"] += 1
        return now_ms() + counter["ticks"]

    monkeypatch.setattr("hyperliquid_copytrader.observer.now_ms", increasing_now_ms)
    config = replace(base_config, mode=Mode.PAPER)
    info = FakeInfoClient()
    original_positions = list(info.state["assetPositions"])
    service = CopyTraderService(config, store=store, info_client=info)

    first = service.run_once()
    info.state["assetPositions"] = []
    flat = service.run_once()
    info.state["assetPositions"] = original_positions
    reentered = service.run_once()

    assert first["reports"][0]["status"] == "filled"
    assert flat["reports"][0]["status"] == "filled"
    assert reentered["reports"][0]["status"] == "filled"
    assert first["intents"][0]["source_event_key"] != reentered["intents"][0]["source_event_key"]
    assert first["intents"][0]["cloid"] != reentered["intents"][0]["cloid"]
    assert store.count("source_events") == 3
    assert store.count("desired_states") == 3
    assert store.count("follower_intents") == 3


def test_shadow_mode_preflight_blocker_stops_before_observation(base_config, store):
    config = replace(
        base_config,
        risk=replace(base_config.risk, slippage_bps=Decimal("10000")),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "preflight_failed"
    assert "slippage" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 0
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_malformed_env_config_stops_before_observation(monkeypatch, tmp_path, store):
    monkeypatch.setenv("HLCT_DB_PATH", str(tmp_path / "bad-env.sqlite3"))
    monkeypatch.setenv("HLCT_FIXED_MULTIPLIER", "not-a-decimal")
    config = load_config()
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "preflight_failed"
    assert "HLCT_FIXED_MULTIPLIER must be a decimal value" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 0
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_paper_mode_executes_intents_deterministically(base_config, store):
    config = replace(base_config, mode=Mode.PAPER)
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert result["reports"][0]["status"] == "filled"
    assert service.paper.positions["BTC"].size == Decimal("0.005")
    assert store.count("execution_reports") == 1


def test_paper_mode_preflight_blocker_does_not_execute_or_plan(base_config, store):
    config = replace(
        base_config,
        mode=Mode.PAPER,
        risk=replace(base_config.risk, slippage_bps=Decimal("-1")),
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "preflight_failed"
    assert service.paper.positions == {}
    assert store.count("source_events") == 0
    assert store.count("follower_intents") == 0
    assert store.count("execution_reports") == 0


def test_paper_mode_scales_when_portfolio_gross_cap_is_exceeded(base_config, store):
    risk = replace(base_config.risk, max_gross_exposure_usd=Decimal("300"))
    config = replace(base_config, mode=Mode.PAPER, risk=risk)
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
    )
    result = service.run_once()
    assert result["safe_mode"]["enabled"] is False, result["safe_mode"]
    assert result["desired_state"]["positions"]["BTC"]["size"] == "0.003"
    assert result["desired_state"]["positions"]["ETH"]["size"] == "0.05"
    assert service.paper.positions["BTC"].size == Decimal("0.00300")
    assert service.paper.positions["ETH"].size == Decimal("0.0500")
    assert store.count("desired_states") == 1


def test_cycle_guard_block_does_not_promote_desired_baseline(base_config, store):
    config = replace(
        base_config,
        mode=Mode.PAPER,
        ops=replace(base_config.ops, max_new_intents_per_cycle=1),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
    )
    result = service.run_once()
    assert len(result["intents"]) == 2
    assert service.safe_mode.reason == SafeModeReason.RISK_LIMIT
    assert {report["exchange_status"] for report in result["reports"]} == {"blocked:risk_limit"}
    assert service.paper.positions == {}
    assert store.count("desired_states") == 0


def test_shadow_mode_trips_stale_source_when_reconcile_exceeds_threshold(base_config, store):
    risk = replace(base_config.risk, stale_source_ms=1)
    config = replace(base_config, risk=risk)
    service = CopyTraderService(
        config,
        store=store,
        info_client=SlowInfoClient(delay_s=0.01),
    )
    result = service.run_once()
    assert result["safe_mode"]["reason"] == "stale_source"
    assert "source data age" in result["safe_mode"]["detail"]
    assert result["intents"] == []
    assert result["reports"] == []
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_run_once_refreshes_source_after_slow_market_preparation(monkeypatch, base_config, store):
    risk = replace(base_config.risk, stale_source_ms=100)
    service = CopyTraderService(
        replace(base_config, risk=risk),
        store=store,
        info_client=FakeInfoClient(),
    )
    reconcile_calls = 0
    real_reconcile = service.observer.reconcile_once
    real_load_asset_meta = service.load_asset_meta

    def counted_reconcile():
        nonlocal reconcile_calls
        reconcile_calls += 1
        return real_reconcile()

    def slow_load_asset_meta():
        sleep(0.06)
        return real_load_asset_meta()

    monkeypatch.setattr(service.observer, "reconcile_once", counted_reconcile)
    monkeypatch.setattr(service, "load_asset_meta", slow_load_asset_meta)

    result = service.run_once()

    assert reconcile_calls == 2
    assert result["safe_mode"]["enabled"] is False


def test_shadow_mode_missing_mid_trips_stale_source_without_desired_baseline(base_config, store):
    service = CopyTraderService(
        base_config,
        store=store,
        info_client=MissingMidInfoClient("BTC"),
    )
    result = service.run_once()
    assert result["safe_mode"]["reason"] == "stale_source"
    assert "BTC missing positive mid price" in result["safe_mode"]["detail"]
    assert result["intents"] == []
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_exchange_missing_mid_for_follower_delta_does_not_journal_flat_baseline(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = MissingMidInfoClient("BTC")
    info.state["assetPositions"] = []
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_adapter=FakeExecutionAdapter(
            positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
        ),
    )
    result = service.run_once()
    assert result["safe_mode"]["reason"] == "stale_source"
    assert "BTC" in result["safe_mode"]["detail"]
    assert "mid price" in result["safe_mode"]["detail"]
    assert store.count("desired_states") == 2
    assert store.latest_desired_positions(Mode.TESTNET, committed_only=True)["BTC"].size == Decimal(
        "0.005"
    )
    assert len(result["intents"]) == 1
    assert result["reports"][0]["exchange_status"] == "blocked:stale_source"


def test_run_once_trips_rest_lag_when_source_reconcile_fails(base_config, store):
    service = CopyTraderService(
        base_config,
        store=store,
        info_client=FailingInfoClient("clearinghouseState", "source down"),
    )
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "rest_lag"
    assert "source REST reconcile failed: source down" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 0
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_run_once_trips_rest_lag_when_asset_metadata_fails(base_config, store):
    service = CopyTraderService(
        base_config,
        store=store,
        info_client=FailingInfoClient("meta", "metadata down"),
    )
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "rest_lag"
    assert "execution market data load failed: metadata down" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0


def test_exchange_mode_blocks_stale_follower_reconcile(base_config, store):
    risk = replace(base_config.risk, stale_follower_ms=1)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=risk,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = StaleReconcileAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER
    assert result["intents"] == []
    assert result["reports"] == []
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 0
    assert store.count("follower_intents") == 0
    assert store.count("execution_reports") == 1
    assert adapter.reports == []


def test_exchange_run_once_trips_stale_follower_when_reconcile_raises(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FailingReconcileAdapter(),
    )
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "stale_follower"
    assert "follower reconcile failed: reconcile down" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 0
    assert store.runtime_lease(service._runtime_lease_name("run_once")) is None


def test_exchange_place_exception_records_pending_unknown_and_safe_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=RaisingPlaceIntentAdapter(),
    )
    result = service.run_once()
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "dead_man_scheduled"
    assert result["reports"][2]["status"] == "sent"
    assert result["reports"][2]["exchange_status"] == "transport_unknown"
    assert "place_intent raised: placement exploded" in result["reports"][2]["payload"]["error"]
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert store.pending_intent_count() == 1
    assert store.count("execution_reports") == 4
    assert store.runtime_lease(service._runtime_lease_name("run_once")) is None


def test_exchange_updates_leverage_before_risk_increasing_open(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "dead_man_scheduled"
    assert result["reports"][2]["status"] == "acked"
    assert store.count("execution_reports") == 4


def test_exchange_updates_existing_position_leverage_before_adding_exposure(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    append_desired_positions(store, {"BTC": Position("BTC", Decimal("0.001"), leverage=1)})
    adapter = FakeExecutionAdapter(
        positions={"BTC": Position("BTC", Decimal("0.001"), leverage=1)},
        forced_status=IntentStatus.FILLED,
    )
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )

    result = service.run_once()

    assert [report.exchange_status for report in adapter.reports] == [
        "leverage_updated",
        "filled",
    ]
    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert result["safe_mode"]["enabled"] is False


def test_exchange_reduces_size_before_lowering_existing_position_leverage(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, fixed_multiplier=Decimal("0.001")),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    current = Position("BTC", Decimal("0.002"), leverage=10)
    append_desired_positions(store, {"BTC": current})
    adapter = FakeExecutionAdapter(positions={"BTC": current}, forced_status=IntentStatus.FILLED)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )

    result = service.run_once()

    assert [report.exchange_status for report in adapter.reports] == [
        "filled",
        "leverage_updated",
    ]
    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert result["safe_mode"]["enabled"] is False


@pytest.mark.parametrize(
    "follower_size",
    [Decimal("0.001"), Decimal("0.00099")],
    ids=["matching-size", "below-min-notional-delta"],
)
def test_exchange_syncs_existing_position_leverage_without_size_order(
    follower_size, base_config, store
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, fixed_multiplier=Decimal("0.001")),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    append_desired_positions(store, {"BTC": Position("BTC", follower_size, leverage=1)})
    adapter = FakeExecutionAdapter(positions={"BTC": Position("BTC", follower_size, leverage=1)})
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )

    result = service.run_once()

    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert [report.exchange_status for report in adapter.reports] == ["leverage_updated"]
    assert adapter.positions["BTC"].size == follower_size
    assert result["safe_mode"]["enabled"] is False


def test_exchange_leverage_only_rejection_fails_closed_without_size_order(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, fixed_multiplier=Decimal("0.001")),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    current = Position("BTC", Decimal("0.001"), leverage=1)
    append_desired_positions(store, {"BTC": current})
    adapter = FakeExecutionAdapter(
        positions={"BTC": current}, leverage_status=IntentStatus.REJECTED
    )
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )

    result = service.run_once()

    assert adapter.leverage_updates == []
    assert [report.exchange_status for report in adapter.reports] == ["rejected"]
    assert adapter.positions["BTC"] == current
    assert result["desired_state_committed"] is False
    assert result["safe_mode"]["enabled"] is True


def test_exchange_retries_isolated_when_cross_margin_is_not_allowed(
    monkeypatch, base_config, store
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = CrossMarginRejectedAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    reconcile_calls = 0
    real_reconcile = service.observer.reconcile_once

    def counted_reconcile():
        nonlocal reconcile_calls
        reconcile_calls += 1
        return real_reconcile()

    monkeypatch.setattr(service.observer, "reconcile_once", counted_reconcile)

    result = service.run_once()

    assert reconcile_calls == 4
    assert adapter.leverage_updates == [("BTC", 2, False)]
    assert result["safe_mode"]["enabled"] is False
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][0]["payload"]["is_cross"] is False
    assert store.count("execution_reports") == 6


def test_cross_margin_fallback_source_drift_preserves_stale_source_recovery(
    monkeypatch,
    base_config,
    store,
):
    risk = replace(base_config.risk, stale_follower_ms=1_000)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=risk,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    clock = {"ms": now_ms()}
    monkeypatch.setattr(service_module, "now_ms", lambda: clock["ms"])
    adapter = CrossMarginRejectedAdapter(forced_status=IntentStatus.FILLED)
    source_info = FakeInfoClient()
    service = CopyTraderService(
        config,
        store=store,
        info_client=source_info,
        execution_adapter=adapter,
    )
    reconcile_calls = 0
    real_reconcile = service.observer.reconcile_once

    def drift_before_isolated_fallback():
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 3:
            source_info.state["assetPositions"][0]["position"]["szi"] = "1.1"
            clock["ms"] += risk.stale_follower_ms + 5_000
        return real_reconcile()

    monkeypatch.setattr(
        service.observer,
        "reconcile_once",
        drift_before_isolated_fallback,
    )

    result = service.run_once()

    assert reconcile_calls == 3
    assert adapter.leverage_updates == []
    assert [(report.exchange_status, report.payload["is_cross"]) for report in adapter.reports] == [
        ("rejected", True)
    ]
    assert result["safe_mode"]["reason"] == SafeModeReason.STALE_SOURCE.value
    assert result["execution_finalization"]["status"] == "cycle_invalidated"
    assert service._source_reaction_requires_recovery({"action": "run_once", "result": result})


def test_exchange_blocks_leverage_when_source_changes_at_dispatch_boundary(
    monkeypatch, base_config, store
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    source_info = FakeInfoClient()
    service = CopyTraderService(
        config, store=store, info_client=source_info, execution_adapter=adapter
    )
    reconcile_calls = 0
    planning_keys: list[str] = []
    real_reconcile = service.observer.reconcile_once

    def changed_source_at_dispatch():
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 2:
            source_info.state["assetPositions"][0]["position"]["szi"] = "1.1"
        snapshot = real_reconcile()
        planning_keys.append(snapshot.planning_key)
        return snapshot

    monkeypatch.setattr(service.observer, "reconcile_once", changed_source_at_dispatch)

    result = service.run_once()

    assert reconcile_calls == 2
    assert len(set(planning_keys)) == 2
    assert adapter.leverage_updates == []
    assert adapter.reports == []
    assert result["safe_mode"]["reason"] == "stale_source"
    assert "source changed before signed leverage dispatch" in result["safe_mode"]["detail"]


def test_exchange_mode_uses_execution_network_mids_for_order_price(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    source_info = FakeInfoClient()
    source_info.mids["BTC"] = "50000"
    execution_info = FakeInfoClient()
    execution_info.mids["BTC"] = "60000"
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=source_info,
        execution_info_client=execution_info,
        execution_adapter=adapter,
    )

    result = service.run_once()

    placed = [
        report.payload["intent"]
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert result["reports"][2]["status"] == "acked"
    assert placed[0].price == Decimal("60150")


def test_exchange_blocks_order_when_leverage_update_rejects(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(leverage_status=IntentStatus.REJECTED)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert adapter.leverage_updates == []
    assert len(adapter.reports) == 1
    assert result["reports"][0]["status"] == "rejected"
    assert result["reports"][0]["exchange_status"] == "rejected"
    assert result["reports"][1]["exchange_status"] == "blocked:ambiguous_exchange_response"
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_exchange_blocks_order_when_leverage_update_is_ambiguous(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = AmbiguousLeverageAdapter()
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert len(adapter.reports) == 1
    assert result["reports"][0]["exchange_status"] == "ambiguous_leverage_response"
    assert result["reports"][1]["exchange_status"] == "blocked:ambiguous_exchange_response"
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_exchange_run_once_blocks_when_journal_baseline_is_malformed(base_config, store):
    append_corrupt_desired(store)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    result = service.run_once()
    assert result["intents"] == []
    assert result["reports"] == []
    assert result["safe_mode"]["reason"] == "startup_reconcile"
    assert "journal baseline rebuild failed" in result["safe_mode"]["detail"]
    assert store.count("source_events") == 1
    assert store.count("desired_states") == 1
    assert store.count("follower_intents") == 0
    assert store.runtime_lease(service._runtime_lease_name("run_once")) is None


def test_manual_reconcile_does_not_clear_safe_mode_with_stale_follower_snapshot(base_config, store):
    risk = replace(base_config.risk, stale_follower_ms=1)
    config = replace(base_config, mode=Mode.TESTNET, risk=risk)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=StaleReconcileAdapter(),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    service.manual_reconcile()
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER


def test_manual_reconcile_trips_stale_follower_when_reconcile_raises(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FailingReconcileAdapter(),
    )
    result = service.manual_reconcile()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER
    assert "manual follower reconcile failed: reconcile down" in service.safe_mode.detail
    assert store.runtime_lease(service._runtime_lease_name("manual_reconcile")) is None


def test_manual_reconcile_refuses_malformed_journal_baseline(base_config, store):
    append_corrupt_desired(store)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.manual_reconcile()
    assert result["safe_mode"]["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.STARTUP_RECONCILE
    assert "journal baseline rebuild failed" in service.safe_mode.detail
    assert store.runtime_lease(service._runtime_lease_name("manual_reconcile")) is None


def test_exchange_mode_blocks_unknown_existing_follower_position(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(positions={"BTC": Position("BTC", Decimal("0.001"))})
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "before any journaled desired state" in service.safe_mode.detail
    assert {report["exchange_status"] for report in result["reports"]} == {
        "blocked:manual_intervention"
    }
    assert adapter.reports == []


def test_exchange_mode_does_not_trust_another_accounts_desired_baseline(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    other_account = "0xf111111111111111111111111111111111111111"
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=other_account,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(
        account=other_account,
        positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)},
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.run_once()

    assert result["safe_mode"]["reason"] == "testnet_blocked"
    assert "journal scope mismatch" in result["safe_mode"]["detail"]
    assert adapter.reports == []


def test_exchange_mode_allows_follower_position_matching_latest_desired(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)})
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert result["intents"] == []
    assert not service.safe_mode.enabled
    assert adapter.reports == []


def test_exchange_mode_blocks_matching_size_with_wrong_leverage(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(positions={"BTC": Position("BTC", Decimal("0.005"), leverage=5)})
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "BTC expected leverage 2 actual 5" in service.safe_mode.detail
    assert result["intents"] == []
    assert result["reports"] == []
    assert adapter.reports == []


def test_exchange_mode_blocks_unknown_uncloided_open_order(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(
        open_orders=[OpenOrder("BTC", "buy", Decimal("0.01"), Decimal("50000"))]
    )
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert {report["exchange_status"] for report in result["reports"]} == {
        "blocked:manual_intervention"
    }
    assert adapter.reports == []


def test_manual_reconcile_does_not_clear_safe_mode_with_position_mismatch(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(positions={}),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    service.manual_reconcile()
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert "BTC expected 0.005 actual 0" in service.safe_mode.detail


def test_exchange_resume_clears_only_after_fresh_matching_reconcile(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(
            positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
        ),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.resume("operator reviewed exchange truth")
    assert result["cleared"] is True
    assert not service.safe_mode.enabled
    assert result["safe_mode"]["reason"] == "none"


def test_exchange_resume_refuses_unresolved_pending_intents(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    pending = FollowerIntent(
        intent_id="pending-resume",
        cloid="0x44444444444444444444444444444444",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="old",
        reason="old unresolved intent",
        created_ms=now_ms(),
    )
    store.append_intent(pending)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(
            positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
        ),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.resume()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert "must settle" in service.safe_mode.detail


def test_exchange_resume_refuses_stale_reconcile(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    risk = replace(base_config.risk, stale_follower_ms=1)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=risk,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=StaleReconcileAdapter(
            positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
        ),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.resume()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER


def test_exchange_resume_trips_stale_follower_when_reconcile_raises(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FailingReconcileAdapter(),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.resume()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER
    assert "resume follower reconcile failed: reconcile down" in service.safe_mode.detail
    assert store.runtime_lease(service._runtime_lease_name("resume")) is None


def test_manual_reconcile_does_not_clear_with_pending_intents(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    pending = FollowerIntent(
        intent_id="pending-manual",
        cloid="0x55555555555555555555555555555555",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="old",
        reason="old unresolved intent",
        created_ms=now_ms(),
    )
    store.append_intent(pending)
    config = replace(base_config, mode=Mode.TESTNET)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(
            positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
        ),
    )
    service.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, "existing blocker")
    result = service.manual_reconcile()
    assert result["safe_mode"]["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL


def test_local_resume_refuses_active_kill_switch(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    config = replace(
        base_config, mode=Mode.PAPER, ops=replace(base_config.ops, kill_switch_path=kill)
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    service.safe_mode.trip(SafeModeReason.OPERATOR_KILL_SWITCH, "existing kill")
    result = service.resume("operator attempted resume")
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.OPERATOR_KILL_SWITCH
    assert str(kill) in service.safe_mode.detail


def test_local_manual_reconcile_refuses_active_kill_switch(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    config = replace(
        base_config, mode=Mode.PAPER, ops=replace(base_config.ops, kill_switch_path=kill)
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    service.safe_mode.trip(SafeModeReason.OPERATOR_KILL_SWITCH, "existing kill")
    result = service.manual_reconcile()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.OPERATOR_KILL_SWITCH
    assert str(kill) in service.safe_mode.detail


def test_manual_reconcile_does_not_clear_live_blocked_preflight(base_config, store):
    config = replace(
        base_config,
        mode=Mode.LIVE,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    service.safe_mode.trip(SafeModeReason.LIVE_BLOCKED, "existing live blocker")
    result = service.manual_reconcile()
    assert result["safe_mode"]["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.LIVE_BLOCKED
    assert "HLCT_LIVE_ENABLE=true" in service.safe_mode.detail


def test_manual_reconcile_refuses_concurrent_exchange_lease(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    store.acquire_runtime_lease(
        name=service._runtime_lease_name("run_once"),
        owner="other-runner",
        ttl_ms=10_000,
    )
    result = service.manual_reconcile()
    assert result["cleared"] is False
    assert service.safe_mode.reason == SafeModeReason.CONCURRENT_INSTANCE


def test_manual_reconcile_clears_stale_concurrent_exchange_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    service.safe_mode.trip(SafeModeReason.CONCURRENT_INSTANCE, "stale lease from previous process")

    result = service.manual_reconcile()

    assert result["safe_mode"]["cleared"] is True
    assert service.safe_mode.enabled is False
    assert store.runtime_lease(service._runtime_lease_name("manual_reconcile")) is None


def test_testnet_startup_never_auto_clears_persisted_dead_man_incident(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    detail = (
        "{'scheduled_time_ms': 123, 'response': {'status': 'err', "
        "'response': 'Cannot set scheduled cancel time until enough volume traded. "
        "Required: $1000000. Traded: $81484.85.'}}"
    )
    first = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    first.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, detail)

    second = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    assert second.safe_mode.enabled
    assert second.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert store.latest_safe_mode()["detail"] == detail


def test_live_startup_keeps_dead_man_volume_gate_safe_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.LIVE,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_wallet_address="0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
            api_private_key="0x" + "1" * 64,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=True,
        ),
    )
    detail = "Cannot set scheduled cancel time until enough volume traded."
    first = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    first.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, detail)

    second = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    assert second.safe_mode.enabled
    assert second.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_exchange_preflight_runs_signed_auth_probe_once_per_cooldown(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    first = service.preflight()
    second = service.preflight()

    assert first.passed
    assert second.passed
    assert [report.exchange_status for report in adapter.auth_probe_reports] == ["auth_probe_ok"]
    assert store.count("execution_reports") == 1
    row = store.latest_successful_auth_probe(
        intent_id=service._auth_probe_intent_id(),
        since_ms=now_ms() - config.ops.auth_probe_interval_ms,
    )
    assert row is not None


def test_exchange_preflight_rejected_auth_probe_blocks_exchange_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(auth_probe_status=IntentStatus.REJECTED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    report = service.preflight()

    assert not report.passed
    assert any("exchange auth probe failed" in blocker for blocker in report.blockers)
    assert service.safe_mode.reason == SafeModeReason.TESTNET_BLOCKED
    assert store.count("execution_reports") == 1


def test_exchange_auth_probe_cache_is_bound_to_derived_signer(base_config, store):
    account = "0xf000000000000000000000000000000000000000"
    first_config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address=account,
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    first_adapter = FakeExecutionAdapter()
    first = CopyTraderService(
        first_config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=first_adapter,
    )
    assert first.preflight().passed

    second_config = replace(
        first_config,
        exchange=replace(first_config.exchange, api_private_key="0x" + "2" * 64),
    )
    second_adapter = FakeExecutionAdapter()
    second = CopyTraderService(
        second_config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=second_adapter,
    )

    assert second.preflight().passed
    assert len(first_adapter.auth_probe_reports) == 1
    assert len(second_adapter.auth_probe_reports) == 1
    assert first._auth_probe_intent_id() != second._auth_probe_intent_id()


def test_exchange_preflight_slow_auth_probe_does_not_seed_cooldown(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            exchange_action_timeout_s=Decimal("2"),
            exchange_expires_after_ms=2_000,
        ),
    )
    # Keep clear of coarse Windows timer rounding at the exact two-second boundary.
    adapter = FakeExecutionAdapter(auth_probe_delay_s=2.25)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    first = service.preflight()
    second = service.preflight()

    assert not first.passed
    assert not second.passed
    assert len(adapter.auth_probe_reports) == 2
    rows = store.conn.execute(
        "SELECT exchange_status FROM execution_reports ORDER BY seq DESC LIMIT 2"
    ).fetchall()
    assert [row["exchange_status"] for row in rows] == ["auth_probe_timeout", "auth_probe_timeout"]
    assert (
        store.latest_successful_auth_probe(
            intent_id=service._auth_probe_intent_id(),
            since_ms=now_ms() - config.ops.auth_probe_interval_ms,
        )
        is None
    )


def test_testnet_smoke_returns_safe_payload_when_preflight_fails(base_config, store):
    config = replace(base_config, mode=Mode.TESTNET)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    result = service.testnet_smoke("BTC")
    assert result["preflight"]["passed"] is False
    assert result["place"] is None
    assert result["cancel"] is None
    assert result["reconcile"] is None
    assert result["safe_mode"]["reason"] == "testnet_blocked"
    assert store.count("follower_intents") == 0
    assert store.count("execution_reports") == 0


def test_testnet_smoke_places_cancels_and_reconciles_when_gated(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.testnet_smoke("BTC")
    assert result["place"]["status"] == "acked"
    assert result["place"]["payload"]["tif"] == "Alo"
    assert result["cancel"]["status"] == "canceled"
    assert result["dead_man"]["exchange_status"] == "dead_man_scheduled"
    assert result["dead_man_clear"]["exchange_status"] == "dead_man_cleared"
    assert result["reconcile"]["source"] == "fake"
    assert store.count("execution_reports") == 6


def test_testnet_smoke_does_not_finalize_before_dead_man_clear_is_proven(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=RejectingDeadManClearAdapter(),
    )

    result = service.testnet_smoke("BTC")

    assert result["dead_man_clear"]["status"] == "rejected"
    assert result["execution_finalization"] is None
    assert result["safe_mode"]["enabled"] is True
    assert store.count("desired_state_commits") == 0


def test_testnet_passive_smoke_requires_target_coin_flat(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(positions={"BTC": Position("BTC", Decimal("0.001"), leverage=1)})
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_smoke("BTC")

    assert result["place"] is None
    assert result["cleanup"] == []
    assert service.safe_mode.reason == SafeModeReason.MANUAL_INTERVENTION
    assert adapter.reports == []


def test_testnet_passive_smoke_cleans_an_unexpected_post_only_fill(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FillingActiveSmokeAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_smoke("BTC")

    assert result["place"]["payload"]["tif"] == "Alo"
    assert result["place"]["status"] == "filled"
    assert result["cleanup"][-1]["status"] == "filled"
    assert result["reconcile"]["positions"] == {}
    assert service.safe_mode.reason == SafeModeReason.PARTIAL_FILL
    assert store.pending_intent_count(Mode.TESTNET) == 0


def test_testnet_smoke_uses_execution_network_market_data(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_network=SourceNetwork.MAINNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    source_info = FakeInfoClient()
    source_info.mids["BTC"] = "90000"
    execution_info = FakeInfoClient()
    execution_info.mids["BTC"] = "50000"
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=source_info,
        execution_info_client=execution_info,
        execution_adapter=adapter,
    )

    result = service.testnet_smoke("BTC")

    assert result["place"]["payload"]["price"] == "25000"
    assert {call["type"] for call in execution_info.calls} >= {"allMids", "meta"}
    assert not any(call["type"] == "allMids" for call in source_info.calls)


def test_execution_market_data_is_loaded_per_configured_hip3_dex(base_config, store):
    config = replace(
        base_config,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("BTC", "kPEPE", "xyz:AAPL")),
    )
    execution_info = FakeInfoClient()
    execution_info.meta["universe"].append({"name": "kPEPE", "szDecimals": 0, "maxLeverage": 3})
    execution_info.mids["kPEPE"] = "4.25"
    execution_info.dex_meta["xyz"] = {
        "universe": [
            {"name": "xyz:AAPL", "szDecimals": 3, "maxLeverage": 20},
            {
                "name": "xyz:SP500",
                "szDecimals": 3,
                "maxLeverage": 50,
                "isDelisted": True,
            },
        ]
    }
    execution_info.dex_mids["xyz"] = {
        "xyz:AAPL": "314.9",
        "xyz:SP500": "7000",
    }
    execution_info.dex_meta_and_contexts["xyz"] = [
        execution_info.dex_meta["xyz"],
        [
            {"oraclePx": "315.5", "markPx": "315.1", "midPx": "314.9"},
            {"oraclePx": "7001", "markPx": "7000", "midPx": "7000"},
        ],
    ]
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_info_client=execution_info,
        execution_adapter=FakeExecutionAdapter(),
    )

    meta = service.load_asset_meta()
    mids = service.load_execution_mids()

    assert meta["xyz:AAPL"].sz_decimals == 3
    assert meta["kPEPE"].coin == "kPEPE"
    assert "KPEPE" not in meta
    assert "xyz:SP500" not in meta
    assert mids["xyz:AAPL"] == Decimal("315.5")
    assert mids["kPEPE"] == Decimal("4.25")
    assert {tuple(sorted(call.items())) for call in execution_info.calls} >= {
        (("type", "meta"),),
        (("dex", "xyz"), ("type", "meta")),
        (("type", "allMids"),),
        (("dex", "xyz"), ("type", "metaAndAssetCtxs")),
    }


@pytest.mark.parametrize("size", [Decimal("NaN"), Decimal("Infinity"), Decimal("0")])
def test_testnet_smoke_rejects_nonfinite_or_nonpositive_size(base_config, store, size):
    config = replace(base_config, mode=Mode.TESTNET)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )

    with pytest.raises(ValueError, match="finite and positive"):
        service.testnet_smoke("BTC", size)

    assert store.count("follower_intents") == 0


def test_testnet_smoke_enforces_kill_switch_before_signed_actions(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(base_config.ops, kill_switch_path=kill),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_smoke("BTC")

    assert result["place"]["exchange_status"] == "blocked:operator_kill_switch"
    assert result["dead_man"] is None
    assert adapter.reports == []
    assert adapter.scheduled_cancel_times == []


def test_testnet_smoke_enforces_symbol_allowlist(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(base_config.risk, allowed_symbols=("ETH",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    with pytest.raises(RuntimeError, match="configured allowlist"):
        service.testnet_smoke("BTC")

    assert service.safe_mode.reason == SafeModeReason.UNSUPPORTED_SYMBOL
    assert adapter.reports == []
    assert adapter.scheduled_cancel_times == []


def test_testnet_smoke_retries_isolated_when_cross_margin_is_not_allowed(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = CrossMarginRejectedAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_smoke("BTC")

    assert result["leverage"]["exchange_status"] == "leverage_updated"
    assert result["leverage"]["payload"]["is_cross"] is False
    assert adapter.leverage_updates == [("BTC", 1, False)]
    assert result["safe_mode"]["enabled"] is False


def test_testnet_active_smoke_enforces_effective_leverage_cap(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        risk=replace(
            base_config.risk,
            max_notional_usd=Decimal("1000"),
            max_gross_exposure_usd=Decimal("1000"),
            max_leverage=3,
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(account_value=Decimal("1"))
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    with pytest.raises(RuntimeError, match="effective leverage cap"):
        service.testnet_active_smoke("BTC")

    assert adapter.reports == []
    assert adapter.scheduled_cancel_times == []


def test_testnet_smoke_blocks_when_dead_man_schedule_fails(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(schedule_cancel_status=IntentStatus.REJECTED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.testnet_smoke("BTC")
    assert result["dead_man"]["status"] == "rejected"
    assert result["place"]["status"] == "skipped"
    assert result["cancel"] is None
    assert result["dead_man_clear"] is None
    assert result["safe_mode"]["enabled"] is True


def test_testnet_active_smoke_fills_closes_and_validates_balance_delta(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FillingActiveSmokeAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_active_smoke("BTC")

    assert result["passed"] is True
    assert result["entry"]["status"] == "filled"
    assert result["exit"]["status"] == "filled"
    assert result["after_reconcile"]["positions"] == {}
    assert result["after_reconcile"]["open_orders"] == []
    assert Decimal(result["balance_delta"]) != Decimal("0")
    assert result["safe_mode"]["enabled"] is False
    assert store.pending_intent_count(Mode.TESTNET) == 0


def test_testnet_active_smoke_blocks_hip3_before_signed_entry_when_exit_is_unsafe(
    base_config, store
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("xyz:TEST",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}]}
    info.dex_mids["xyz"] = {"xyz:TEST": "100"}
    info.dex_meta["xyz"] = {"universe": [{"name": "xyz:TEST", "szDecimals": 2, "maxLeverage": 5}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "98.1"}],
    ]
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "96", "sz": "5", "n": 1}],
            [{"px": "100.2", "sz": "5", "n": 1}],
        ],
    }
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_adapter=adapter,
    )
    result = service.testnet_active_smoke("xyz:TEST")

    assert result["passed"] is False
    assert result["market_admission"]["passed"] is False
    assert "visible sell exit depth" in " ".join(result["market_admission"]["blockers"])
    assert adapter.leverage_updates == []
    assert adapter.scheduled_cancel_times == []
    assert adapter.reports == []


def test_normal_dispatch_rechecks_hip3_depth_immediately_before_signed_send(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("xyz:TEST",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.dex_meta["xyz"] = {"universe": [{"name": "xyz:TEST", "szDecimals": 2, "maxLeverage": 5}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "99.8", "sz": "5", "n": 1}],
            [{"px": "100.2", "sz": "5", "n": 1}],
        ],
    }
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    intent = FollowerIntent(
        intent_id="hip3-last-mile-intent",
        cloid="0x" + "6" * 32,
        action=IntentAction.OPEN,
        coin="xyz:TEST",
        side="buy",
        size=Decimal("0.1"),
        price=Decimal("100.2"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="hip3-last-mile-source",
        reason="verify last-mile market revalidation",
        created_ms=now_ms(),
    )
    meta = AssetMeta("xyz:TEST", 2)
    admitted, liquidity_deferred, blockers = service._admit_hip3_open_intents(
        [intent],
        asset_meta={"xyz:TEST": meta},
    )
    assert blockers == []
    assert liquidity_deferred == []
    assert admitted[0].execution_proof["kind"] == "hip3_round_trip"

    # The exit side deteriorates after planning. The adapter callback must block before
    # the durable signed-send boundary, even though the original proof was valid.
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "96", "sz": "5", "n": 1}],
            [{"px": "100.2", "sz": "5", "n": 1}],
        ],
    }
    service._active_dispatch_intent = admitted[0]
    service._active_dispatch_asset_meta = meta
    service._active_plan_source_observed_ms = now_ms()
    service._active_plan_follower_observed_ms = now_ms()

    with pytest.raises(PreSendBlockedError, match="last-mile HIP-3 liquidity gate"):
        service._revalidate_active_hip3_dispatch(action="place_intent")

    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0
    assert adapter.reports == []


def test_last_mile_oracle_move_defers_cap_replan_without_global_safe_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(
            base_config.risk,
            allowed_symbols=("xyz:TEST",),
            max_notional_usd=Decimal("15"),
            hip3_oracle_envelope_bps=Decimal("100"),
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.dex_meta["xyz"] = {"universe": [{"name": "xyz:TEST", "szDecimals": 3, "maxLeverage": 5}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "99.8", "sz": "5", "n": 1}],
            [{"px": "100.2", "sz": "5", "n": 1}],
        ],
    }
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=FakeExecutionAdapter(),
    )
    intent = FollowerIntent(
        intent_id="hip3-cap-move-intent",
        cloid="0x" + "7" * 32,
        action=IntentAction.OPEN,
        coin="xyz:TEST",
        side="buy",
        size=Decimal("0.148"),
        price=Decimal("100.2"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="hip3-cap-move-source",
        reason="verify market-specific cap replan",
        created_ms=now_ms(),
    )
    meta = AssetMeta("xyz:TEST", 3)
    admitted, liquidity_deferred, blockers = service._admit_hip3_open_intents(
        [intent],
        asset_meta={"xyz:TEST": meta},
    )
    assert blockers == []
    assert liquidity_deferred == []

    info.dex_meta_and_contexts["xyz"][1][0].update(
        {"oraclePx": "101", "markPx": "101", "midPx": "101"}
    )
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "100.5", "sz": "5", "n": 1}],
            [{"px": "102.01", "sz": "5", "n": 1}],
        ],
    }
    service._active_dispatch_intent = admitted[0]
    service._active_dispatch_asset_meta = meta

    with pytest.raises(PreSendBlockedError, match="last-mile HIP-3 cap gate"):
        service._revalidate_active_hip3_dispatch(action="place_intent")

    deferral = service._active_dispatch_liquidity_deferral
    assert deferral is not None
    assert deferral.stage == "signed_dispatch_cap_reprice"
    assert deferral.retry_not_before_ms > now_ms()
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0


@pytest.mark.parametrize(
    ("side", "stale_price", "final_price", "target_size"),
    [
        ("buy", Decimal("99"), Decimal("100.4"), Decimal("0.1")),
        ("sell", Decimal("101"), Decimal("99.6"), Decimal("-0.1")),
    ],
)
def test_normal_dispatch_persists_fresh_hip3_ioc_price_before_dispatch(
    monkeypatch, base_config, store, side, stale_price, final_price, target_size
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(base_config.risk, allowed_symbols=("xyz:TEST",)),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.dex_meta["xyz"] = {"universe": [{"name": "xyz:TEST", "szDecimals": 2, "maxLeverage": 5}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "99.6", "sz": "5", "n": 1}],
            [{"px": "100.4", "sz": "5", "n": 1}],
        ],
    }
    adapter = AtomicHip3FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    desired = DesiredState(
        state_id="hip3-final-price-state",
        source_event_key="hip3-final-price-source",
        mode=Mode.TESTNET,
        positions={"xyz:TEST": Position("xyz:TEST", target_size, leverage=1)},
        reason="final IOC repricing",
        created_ms=now_ms(),
    )
    intent = FollowerIntent(
        intent_id="hip3-final-price-intent",
        cloid="0x" + "5" * 32,
        action=IntentAction.OPEN,
        coin="xyz:TEST",
        side=side,
        size=Decimal("0.1"),
        price=stale_price,
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key=desired.source_event_key,
        reason="refresh stale IOC limit",
        created_ms=now_ms(),
        desired_state_id=desired.state_id,
        execution_proof={"kind": "hip3_round_trip"},
    )
    assert store.prepare_execution_plan(desired, [intent])
    monkeypatch.setattr(
        service, "_refresh_active_dispatch_truth_for_fallback", lambda *a, **k: True
    )
    quote_loads = 0
    real_quote_load = service.load_hip3_round_trip_assessment

    def counted_quote_load(*args, **kwargs):
        nonlocal quote_loads
        quote_loads += 1
        return real_quote_load(*args, **kwargs)

    monkeypatch.setattr(service, "load_hip3_round_trip_assessment", counted_quote_load)
    adapter.set_pre_send_check(
        lambda action, _risk_increasing: service._revalidate_active_hip3_dispatch(action=action)
    )
    service._active_dispatch_intent = intent
    service._active_dispatch_asset_meta = AssetMeta("xyz:TEST", 2)

    report = service._execute_intent(intent)

    assert report.status == IntentStatus.ACKED
    assert quote_loads == 2
    assert adapter.reports[0].payload["intent"].price == final_price
    stored = store.intent_by_cloid(intent.cloid)
    assert stored is not None
    assert stored["attempt_phase"] == "dispatching"
    assert json.loads(stored["payload_json"])["price"] == str(final_price)


def test_testnet_active_smoke_uses_fresh_hip3_round_trip_quotes_for_entry_and_exit(
    base_config, store
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(
            base_config.risk,
            allowed_symbols=("xyz:TEST",),
            slippage_bps=Decimal("100"),
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}]}
    info.dex_mids["xyz"] = {"xyz:TEST": "100"}
    info.dex_meta["xyz"] = {"universe": [{"name": "xyz:TEST", "szDecimals": 2, "maxLeverage": 5}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "100", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:TEST"] = {
        "coin": "xyz:TEST",
        "time": now_ms(),
        "levels": [
            [{"px": "99.8", "sz": "5", "n": 1}],
            [{"px": "100.2", "sz": "5", "n": 1}],
        ],
    }
    adapter = FillingActiveSmokeAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.state},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )

    result = service.testnet_active_smoke("xyz:TEST")

    assert result["passed"] is True
    assert result["entry_price"] == "100.2"
    assert result["exit"]["payload"]["reduce_only_quote"]["limit_price"] == "99.8"
    assert result["entry"]["payload"]["round_trip_admission"]["kind"] == "hip3_round_trip"
    assert result["after_reconcile"]["positions"] == {}


def test_active_smoke_does_not_finalize_before_dead_man_clear_is_proven(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FillingRejectingDeadManClearAdapter(),
    )

    result = service.testnet_active_smoke("BTC")

    assert result["passed"] is False
    assert result["dead_man_clear"]["status"] == "rejected"
    assert result["execution_finalization"] is None
    assert result["safe_mode"]["enabled"] is True
    assert store.count("desired_state_commits") == 0


def test_testnet_active_smoke_cleans_fill_after_ambiguous_entry_response(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = AmbiguousFilledActiveSmokeAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_active_smoke("BTC")

    assert result["passed"] is False
    assert result["entry"]["status"] == "sent"
    assert result["entry"]["exchange_status"] == "transport_unknown"
    assert result["entry_cancel"]["status"] == "canceled"
    assert result["cleanup"][-1]["status"] == "filled"
    assert result["after_reconcile"]["positions"] == {}
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert store.pending_intent_count(Mode.TESTNET) == 0


def test_testnet_active_smoke_retries_bounded_partial_cleanup(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = RetriedCleanupActiveSmokeAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.testnet_active_smoke("BTC")

    assert result["passed"] is False
    assert len(result["cleanup"]) == 3
    assert adapter.cleanup_calls == 3
    assert result["after_reconcile"]["positions"] == {}
    assert result["safe_mode"]["enabled"] is False
    assert store.pending_intent_count(Mode.TESTNET) == 0


def test_testnet_smoke_market_data_failure_trips_rest_lag(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FailingInfoClient("allMids", "mids down"),
        execution_adapter=FakeExecutionAdapter(),
    )
    result = service.testnet_smoke("BTC")
    assert result["preflight"]["passed"] is True
    assert result["place"] is None
    assert result["cancel"] is None
    assert result["reconcile"] is None
    assert result["safe_mode"]["reason"] == "rest_lag"
    assert "testnet smoke market data load failed: mids down" in result["safe_mode"]["detail"]
    assert store.count("follower_intents") == 0
    assert store.count("execution_reports") == 1
    assert store.runtime_lease(service._runtime_lease_name("testnet_smoke")) is None


def test_testnet_smoke_honors_runtime_rate_limiter(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, max_exchange_actions_per_minute=3),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    service.exchange_rate_limiter.record()
    service.exchange_rate_limiter.record()
    result = service.testnet_smoke("BTC")
    assert result["place"]["status"] == "skipped"
    assert result["place"]["exchange_status"] == "blocked:rate_limit"
    assert result["cancel"] is None
    assert [report.exchange_status for report in adapter.reports] == ["leverage_updated"]
    assert service.safe_mode.reason.value == "rate_limit"


def test_testnet_smoke_honors_persistent_rate_limiter_after_restart(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, max_exchange_actions_per_minute=3),
    )
    first_adapter = FakeExecutionAdapter()
    first = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=first_adapter,
    )
    first_result = first.testnet_smoke("BTC")
    assert first_result["place"]["status"] == "acked"
    assert first_result["cancel"]["status"] == "canceled"
    store.append_execution_report(
        ExecutionReport(
            report_id=deterministic_cloid("rate-limit-seed", now_ms()),
            intent_id="rate-limit-seed",
            cloid=deterministic_cloid("rate-limit-seed-cloid", now_ms()),
            status=IntentStatus.ACKED,
            exchange_status="seeded_counted_action",
            exchange_ts_ms=now_ms(),
            payload={},
        )
    )

    second_adapter = FakeExecutionAdapter()
    second = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=second_adapter,
    )
    result = second.testnet_smoke("BTC")
    assert result["leverage"]["status"] == "skipped"
    assert result["leverage"]["exchange_status"] == "blocked:rate_limit"
    assert result["dead_man"] is None
    assert result["place"]["exchange_status"] == "blocked:rate_limit"
    assert "persistent action rate limit hit" in result["leverage"]["payload"]["detail"]
    assert result["cancel"] is None
    assert second_adapter.reports == []
    assert second.safe_mode.reason == SafeModeReason.RATE_LIMIT


def test_testnet_smoke_trips_circuit_breaker_on_rejected_place(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, circuit_breaker_failure_threshold=1),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.REJECTED)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.testnet_smoke("BTC")
    assert result["place"]["status"] == "rejected"
    assert result["cancel"]["status"] == "canceled"
    assert service.safe_mode.reason.value == "circuit_breaker"


def test_testnet_smoke_records_timeout_but_still_attempts_cancel(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            exchange_action_timeout_s=Decimal("2"),
            exchange_expires_after_ms=2_000,
        ),
    )
    # Keep enough margin above the two-second boundary for coarse/loaded Windows timers.
    adapter = FakeExecutionAdapter(delay_s=2.25)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.testnet_smoke("BTC")
    assert result["place"]["status"] == "acked"
    assert result["cancel"]["status"] == "canceled"
    assert Decimal(result["place"]["payload"]["elapsed_s"]) > Decimal("2")
    assert service.safe_mode.reason.value == "order_timeout"


def test_testnet_smoke_cancel_reject_trips_cancel_reject(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = RejectingCancelAdapter(forced_status=IntentStatus.ACKED)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )

    result = service.testnet_smoke("BTC")

    assert result["place"]["status"] == "acked"
    assert result["cancel"]["status"] == "rejected"
    assert result["safe_mode"]["enabled"] is True
    assert service.safe_mode.reason == SafeModeReason.CANCEL_REJECT
    assert "status=open" in service.safe_mode.detail


def test_testnet_smoke_place_exception_records_unknown_and_attempts_cancel(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=RaisingLimitOrderAdapter(),
    )
    result = service.testnet_smoke("BTC")
    assert result["place"]["status"] == "sent"
    assert result["place"]["exchange_status"] == "transport_unknown"
    assert "exchange action raised: limit placement exploded" in result["place"]["payload"]["error"]
    assert result["cancel"]["status"] == "canceled"
    assert result["reconcile"]["source"] == "fake"
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert store.count("execution_reports") == 5
    assert store.pending_intent_count() == 0


def test_testnet_smoke_reconcile_exception_trips_stale_follower(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FailingReconcileAdapter(),
    )
    result = service.testnet_smoke("BTC")
    assert result["place"] is None
    assert result["cancel"] is None
    assert result["reconcile"] == {"error": "reconcile down"}
    assert result["dead_man_clear"] is None
    assert service.safe_mode.reason == SafeModeReason.STALE_FOLLOWER
    assert "initial follower reconcile failed: reconcile down" in service.safe_mode.detail
    assert store.count("execution_reports") == 1
    assert store.count("reconcile_snapshots") == 0


def test_live_mode_stays_blocked_by_default(base_config, store):
    config = replace(base_config, mode=Mode.LIVE)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    result = service.run_once()
    assert result["intents"] == []
    assert service.safe_mode.enabled
    assert service.safe_mode.reason.value == "live_blocked"


def test_testnet_smoke_refuses_non_testnet(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    with pytest.raises(RuntimeError):
        service.testnet_smoke("BTC")


def test_testnet_active_smoke_refuses_non_testnet(base_config, store):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    with pytest.raises(RuntimeError):
        service.testnet_active_smoke("BTC")


def test_exchange_mode_refuses_restart_with_unresolved_prior_intent(base_config, store):
    bind_testnet_scope(store)
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    pending = FollowerIntent(
        intent_id="pending-1",
        cloid="0x11111111111111111111111111111111",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="old",
        reason="old unresolved intent",
        created_ms=now_ms(),
    )
    store.append_intent(pending)
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert result["intents"] == []
    assert service.safe_mode.reason.value == "restart_mid_fill"
    assert adapter.reports == []


def test_exchange_mode_refuses_concurrent_runtime_lease_owner(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    store.acquire_runtime_lease(
        name=service._runtime_lease_name("run_once"),
        owner="other-instance",
        ttl_ms=10_000,
    )
    result = service.run_once()
    assert result["intents"] == []
    assert service.safe_mode.reason.value == "concurrent_instance"
    assert adapter.reports == []


def test_exchange_runtime_lease_is_account_wide_across_operations(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    assert service._runtime_lease_name("run_once") == service._runtime_lease_name("settle_pending")
    assert service._runtime_lease_name("run_once") == service._runtime_lease_name("testnet_smoke")


def test_exchange_lease_diagnostics_classify_clear_active_and_stale(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    assert service._exchange_lease_diagnostics()["status"] == "clear"
    store.acquire_runtime_lease(
        name=service._runtime_lease_name("run_once"),
        owner="active",
        ttl_ms=10_000,
    )
    assert service._exchange_lease_diagnostics()["status"] == "active"
    with store.lock:
        with store.conn:
            store.conn.execute(
                "UPDATE runtime_leases SET expires_ms = ? WHERE name = ?",
                (1, service._runtime_lease_name("run_once")),
            )
    assert service._exchange_lease_diagnostics()["status"] == "stale"


def test_settlement_refuses_runtime_lease_held_by_run(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    pending = FollowerIntent(
        intent_id="pending-lease",
        cloid="0x22222222222222222222222222222222",
        action=IntentAction.OPEN,
        coin="BTC",
        side="buy",
        size=Decimal("0.01"),
        price=Decimal("50000"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="old",
        reason="old unresolved intent",
        created_ms=now_ms(),
    )
    store.append_intent(pending)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=FakeExecutionAdapter(),
    )
    store.acquire_runtime_lease(
        name=service._runtime_lease_name("run_once"),
        owner="other-runner",
        ttl_ms=10_000,
    )
    result = service.settle_pending_intents()
    assert result["settled"] == []
    assert result["pending_after"] == 1
    assert service.safe_mode.reason.value == "concurrent_instance"


def test_exchange_mode_releases_runtime_lease_after_successful_run(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    service.run_once()
    assert store.runtime_lease(service._runtime_lease_name("run_once")) is None
    assert adapter.reports


def test_exchange_ack_pauses_before_second_same_cycle_intent(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert len(result["intents"]) == 2
    assert len(adapter.reports) == 2
    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "dead_man_scheduled"
    assert result["reports"][2]["status"] == "acked"
    assert result["reports"][3]["exchange_status"] == "blocked:restart_mid_fill"
    assert service.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert store.pending_intent_count() == 1


def test_exchange_partial_fill_pauses_before_second_same_cycle_intent(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = PartialFillAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert len(result["intents"]) == 2
    assert len(adapter.reports) == 2
    assert adapter.leverage_updates == [("BTC", 2, True)]
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "dead_man_scheduled"
    assert result["reports"][2]["exchange_status"] == "partial_fill"
    assert result["reports"][3]["exchange_status"] == "blocked:partial_fill"
    assert service.safe_mode.reason == SafeModeReason.PARTIAL_FILL
    assert store.pending_intent_count() == 1


def test_exchange_dust_partial_fill_is_accepted_as_terminal(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = DustPartialFillAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.run_once()

    assert result["safe_mode"]["enabled"] is False
    assert result["reports"][2]["exchange_status"] == "partial_fill"
    assert result["reports"][3]["exchange_status"] == "dust_residual_accepted"
    assert result["reports"][-1]["exchange_status"] == "dead_man_cleared"
    terminal_reports = store.execution_reports_for_cloid(result["reports"][2]["cloid"])
    assert terminal_reports[0]["status"] == "filled"
    assert terminal_reports[0]["exchange_status"] == "dust_residual_accepted"
    assert store.pending_intent_count() == 0


@pytest.mark.parametrize("clear_status", [IntentStatus.REJECTED, IntentStatus.SENT])
def test_normal_cycle_retains_dead_man_and_refuses_finalization_when_clear_is_unproven(
    base_config, store, clear_status
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = UnprovenDeadManClearAdapter(
        clear_status=clear_status,
        forced_status=IntentStatus.FILLED,
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service._run_once_with_lease(PreflightReport(mode=Mode.TESTNET, passed=True))

    scheduled_deadline = next(
        value for value in adapter.scheduled_cancel_times if value is not None
    )
    assert adapter.scheduled_cancel_times[-1] is None
    assert service._active_dead_man_deadline_ms == scheduled_deadline
    assert result["desired_state_committed"] is False
    assert result["execution_finalization"]["status"] == "dead_man_clear_unproven"
    assert store.count("desired_state_commits") == 0
    assert service.safe_mode.enabled is True


@pytest.mark.parametrize("mode", [Mode.TESTNET, Mode.LIVE])
def test_clock_rollback_between_reconcile_and_dispatch_never_crosses_signed_boundary(
    base_config, store, monkeypatch, mode
):
    exchange = ExchangeConfig(
        follower_account_address="0xf000000000000000000000000000000000000000",
        api_wallet_address="0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
        api_private_key="0x" + "1" * 64,
        testnet_enable=True,
        live_enable=mode == Mode.LIVE,
        confirm_mainnet_live=mode == Mode.LIVE,
        live_copy_enable=mode == Mode.LIVE,
    )
    config = replace(base_config, mode=mode, exchange=exchange)
    clock = {"ms": now_ms()}
    adapter = ClockRollbackBeforeSignedBoundaryAdapter(clock=clock)
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    source_snapshot = replace(service.observer.reconcile_once(), observed_ms=clock["ms"])
    monkeypatch.setattr(service.observer, "reconcile_once", lambda: source_snapshot)
    monkeypatch.setattr(service_module, "now_ms", lambda: clock["ms"])

    result = service._run_once_with_lease(PreflightReport(mode=mode, passed=True))

    assert adapter.rollback_applied is True
    assert adapter.signed_exchange_calls == 0
    assert adapter.schedule_cancel_reports == []
    assert service.safe_mode.reason == SafeModeReason.CLOCK_SKEW
    assert "future relative to the local clock" in service.safe_mode.detail
    assert result["desired_state_committed"] is False


def test_future_source_and_follower_freshness_trip_clock_skew(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    current = 10_000
    monkeypatch.setattr(service_module, "now_ms", lambda: current)

    assert service._check_source_freshness(current + 1_001) is False
    assert service.safe_mode.reason == SafeModeReason.CLOCK_SKEW

    service.safe_mode.clear("test second observation type")
    assert service._check_follower_freshness(current + 1_001) is False
    assert service.safe_mode.reason == SafeModeReason.CLOCK_SKEW


def test_monotonic_elapsed_time_can_expire_frozen_wall_clock_truth(base_config, store, monkeypatch):
    risk = replace(base_config.risk, stale_source_ms=1_000)
    service = CopyTraderService(
        replace(base_config, risk=risk),
        store=store,
        info_client=FakeInfoClient(),
    )
    wall_ms = 10_000
    monotonic_s = {"value": 1.0}
    monkeypatch.setattr(service_module, "now_ms", lambda: wall_ms)
    monkeypatch.setattr(service_module, "monotonic", lambda: monotonic_s["value"])

    assert service._check_source_freshness(wall_ms) is True
    monotonic_s["value"] += 1.01

    assert service._check_source_freshness(wall_ms) is False
    assert service.safe_mode.reason == SafeModeReason.STALE_SOURCE


def test_automatic_reduction_rejects_future_follower_truth(base_config, store, monkeypatch):
    service = CopyTraderService(base_config, store=store, info_client=FakeInfoClient())
    current = 10_000
    monkeypatch.setattr(service_module, "now_ms", lambda: current)
    service.safe_mode.trip(SafeModeReason.STALE_SOURCE, "source refresh pending")
    service._active_plan_follower_observed_ms = current + 1_001

    assert service._safe_mode_allows_automatic_reduction() is False
    assert service.safe_mode.reason == SafeModeReason.CLOCK_SKEW


def test_nonfinite_exchange_report_numbers_trip_ambiguous_safe_mode(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    report = ExecutionReport(
        report_id="nonfinite-report",
        intent_id="nonfinite-intent",
        cloid="0x11111111111111111111111111111111",
        status=IntentStatus.ACKED,
        exchange_status="partial_fill",
        exchange_ts_ms=now_ms(),
        payload={"expected_size": "0.01", "filled_size": "NaN"},
    )

    service._handle_non_terminal_exchange_ack(report)

    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert "filled_size is not a finite decimal" in service.safe_mode.detail
    assert (
        CopyTraderService._account_value_from_clearinghouse_state(
            {"marginSummary": {"accountValue": "Infinity"}}
        )
        is None
    )


def test_exchange_overfill_pauses_before_second_same_cycle_intent(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = OverfillAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert len(result["intents"]) == 2
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "dead_man_scheduled"
    assert result["reports"][2]["exchange_status"] == "overfill"
    assert result["reports"][3]["exchange_status"] == "blocked:ambiguous_exchange_response"
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
    assert "greater than expected" in service.safe_mode.detail
    assert store.pending_intent_count() == 1


def test_testnet_smoke_releases_runtime_lease_after_validation_exception(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    with pytest.raises(RuntimeError):
        service.testnet_smoke("DOGE")
    assert store.runtime_lease(service._runtime_lease_name("testnet_smoke")) is None


def test_hip3_liquidity_admission_precedes_single_new_intent_capacity(base_config, store):
    service, adapter = two_hip3_open_service_with_first_market_illiquid(base_config, store)
    started_ms = now_ms()

    result = service.run_once()

    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert [item["coin"] for item in result["intents"]] == ["xyz:SKHX"], {
        "source_positions": result.get("source_positions"),
        "desired_state": result.get("desired_state"),
        "liquidity_deferred_intents": result.get("liquidity_deferred_intents"),
        "deferred_intents": result.get("deferred_intents"),
    }
    assert result["deferred_intents"] == []
    assert len(result["liquidity_deferred_intents"]) == 1
    liquidity_deferred = result["liquidity_deferred_intents"][0]
    assert liquidity_deferred["intent"]["coin"] == "xyz:KR200"
    assert liquidity_deferred["intent"]["action"] == IntentAction.OPEN.value
    assert "visible sell entry depth" in " ".join(liquidity_deferred["blockers"])
    assert started_ms + 59_000 <= liquidity_deferred["retry_not_before_ms"]
    assert liquidity_deferred["retry_not_before_ms"] <= now_ms() + 61_000
    assert {row["coin"] for row in store.recent("follower_intents", 10)} == {"xyz:SKHX"}
    placed_coins = {
        report.payload["intent"].coin
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    }
    assert placed_coins == {"xyz:SKHX"}
    assert store.pending_intent_count(Mode.TESTNET) == 0


def test_deferred_open_drain_stops_without_starving_liquid_later_symbol(base_config, store):
    service, adapter = two_hip3_open_service_with_first_market_illiquid(base_config, store)

    result, drain = service._run_once_until_deferred_opens_drained()

    assert drain["status"] == "drained_with_liquidity_deferrals"
    assert drain["cycle_count"] == 1
    assert drain["cycles"][0]["active_open_coins"] == ["xyz:SKHX"]
    assert drain["cycles"][0]["deferred_open_coins"] == []
    assert drain["cycles"][0]["liquidity_deferred_open_coins"] == ["xyz:KR200"]
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert [
        report.payload["intent"].coin
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ] == ["xyz:SKHX"]


def test_bounded_drain_executes_all_liquid_opens_around_planning_liquidity_deferral(
    base_config, store
):
    service, adapter = three_hip3_open_service_with_first_market_illiquid(
        base_config,
        store,
    )
    assert service.config.ops.max_new_intents_per_cycle == 1

    result, drain = service._run_once_until_deferred_opens_drained()

    assert drain["status"] == "drained_with_liquidity_deferrals"
    assert drain["cycle_count"] == 2
    assert drain["liquidity_cooldown_coins"] == ["xyz:KR200"]
    assert drain["cycles"][0]["active_open_coins"] == ["xyz:SKHX"]
    assert drain["cycles"][0]["deferred_open_coins"] == ["xyz:US500"]
    assert drain["cycles"][0]["liquidity_deferred_open_coins"] == ["xyz:KR200"]
    assert drain["cycles"][1]["active_open_coins"] == ["xyz:US500"]
    assert drain["cycles"][1]["deferred_open_coins"] == []
    assert drain["cycles"][1]["liquidity_deferred_open_coins"] == ["xyz:KR200"]
    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert [
        report.payload["intent"].coin
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ] == ["xyz:SKHX", "xyz:US500"]
    assert service._active_drain_liquidity_cooldown_coins == set()


def test_bounded_drain_advances_capacity_deferred_open_after_last_mile_liquidity_deferral(
    base_config, store, monkeypatch
):
    service, adapter = two_hip3_open_service_with_first_market_illiquid(base_config, store)
    assert service.config.ops.max_new_intents_per_cycle == 1
    info = cast(FakeInfoClient, service.info_client)
    info.books["xyz:KR200"]["levels"][0][0]["px"] = "1107"
    load_assessment = service.load_hip3_round_trip_assessment
    kr200_assessment_calls = 0

    def lose_kr200_depth_after_planning(coin, **kwargs):
        nonlocal kr200_assessment_calls
        assessment = load_assessment(coin, **kwargs)
        if coin == "xyz:KR200":
            kr200_assessment_calls += 1
            if kr200_assessment_calls == 1:
                info.books["xyz:KR200"]["levels"][0][0]["px"] = "1090"
        return assessment

    monkeypatch.setattr(
        service,
        "load_hip3_round_trip_assessment",
        lose_kr200_depth_after_planning,
    )

    result, drain = service._run_once_until_deferred_opens_drained()

    assert drain["status"] == "drained_with_liquidity_deferrals"
    assert drain["cycle_count"] == 2
    assert drain["liquidity_cooldown_coins"] == ["xyz:KR200"]
    assert drain["cycles"][0]["active_open_coins"] == ["xyz:KR200"]
    assert drain["cycles"][0]["deferred_open_coins"] == ["xyz:SKHX"]
    assert drain["cycles"][0]["liquidity_deferred_open_coins"] == ["xyz:KR200"]
    assert drain["cycles"][1]["active_open_coins"] == ["xyz:SKHX"]
    assert drain["cycles"][1]["deferred_open_coins"] == []
    assert drain["cycles"][1]["liquidity_deferred_open_coins"] == ["xyz:KR200"]
    assert kr200_assessment_calls == 2
    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert [
        report.payload["intent"].coin
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ] == ["xyz:SKHX"]
    assert service._active_drain_liquidity_cooldown_coins == set()


def test_hip3_ioc_exact_zero_fill_is_a_signed_typed_deferral_with_fresh_reconcile(
    base_config,
    store,
):
    adapter = Hip3IocRaceAdapter()
    service, _info = hip3_ioc_race_service(base_config, store, adapter=adapter)
    started_ms = now_ms()

    result = service.run_once()

    report = next(
        item for item in result["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    assert report["status"] == IntentStatus.REJECTED.value
    assert report["payload"]["signed_action_performed"] is True
    assert report["payload"]["proven_zero_fill"] is True
    assert Decimal(report["payload"]["filled_size"]) == 0
    assert report["payload"]["requires_post_action_reconcile"] is True
    proof = report["payload"]["zero_fill_proof"]
    assert proof["kind"] == "hip3_ioc_zero_fill_v1"
    assert proof["proof_id"] == report["payload"]["zero_fill_proof_id"]
    assert proof["cloid"] == report["cloid"]
    assert proof["coin"] == "xyz:SKHX"
    assert proof["size"] == report["payload"]["expected_size"]
    assert (
        proof["order_status"]["order"]["order"]["origSz"]
        == proof["order_status"]["order"]["order"]["sz"]
    )

    deferral = report["payload"]["liquidity_deferral"]
    assert deferral["stage"] == "signed_ioc_zero_fill_open"
    assert deferral["intent"]["coin"] == "xyz:SKHX"
    assert started_ms + 59_000 <= deferral["retry_not_before_ms"]
    assert deferral["retry_not_before_ms"] <= now_ms() + 61_000
    assert any(
        item["stage"] == "signed_ioc_zero_fill_open" and item["intent"]["coin"] == "xyz:SKHX"
        for item in result["liquidity_deferred_intents"]
    )

    assert result["post_action_reconcile"] is not None
    assert result["post_action_reconcile"]["positions"] == {}
    assert result["post_action_reconcile"]["open_orders"] == []
    assert adapter.reconcile_calls > adapter.reconcile_calls_at_dispatch[-1]
    assert adapter.positions == {}
    assert adapter.open_orders == []
    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert service.circuit_breaker.consecutive_failures == 0
    assert service.circuit_breaker.opened_ms is None
    assert store.consecutive_exchange_failure_stats()["consecutive_failures"] == 0
    assert store.count("safe_mode_transitions") == 0
    assert store.pending_intent_count(Mode.TESTNET) == 0
    assert store.latest_hip3_ioc_zero_fill_proof(report["cloid"]) == proof["proof_id"]


def test_hip3_ioc_exact_no_match_with_unknown_oid_is_a_typed_deferral(
    base_config,
    store,
    monkeypatch,
):
    monkeypatch.setattr(
        service_module,
        "HIP3_IOC_ZERO_FILL_CONFIRMATION_DELAYS_S",
        (0.0, 0.0, 0.0),
    )
    adapter = Hip3IocRaceAdapter(order_status_variant="unknown_oid")
    service, _info = hip3_ioc_race_service(
        base_config,
        store,
        adapter=adapter,
        circuit_breaker_failure_threshold=1,
    )

    result = service.run_once()

    report = next(
        item for item in result["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    proof = report["payload"]["zero_fill_proof"]
    assert proof["kind"] == "hip3_ioc_zero_fill_v1"
    assert proof["evidence_source"] == "synchronous_ioc_no_match_rejection"
    assert proof["source_report_id"]
    assert proof["proof_id"] == report["payload"]["zero_fill_proof_id"]
    assert proof["cloid"] == report["cloid"]
    assert len(proof["unknown_oid_confirmations"]) == 3
    assert all(
        confirmation["payload"] == {"status": "unknownOid"}
        for confirmation in proof["unknown_oid_confirmations"]
    )
    assert report["payload"]["signed_action_performed"] is True
    assert report["payload"]["proven_zero_fill"] is True
    assert Decimal(report["payload"]["filled_size"]) == 0
    assert report["payload"]["requires_post_action_reconcile"] is True
    assert result["post_action_reconcile"] is not None
    assert result["post_action_reconcile"]["positions"] == {}
    assert result["post_action_reconcile"]["open_orders"] == []
    assert result["safe_mode"]["enabled"] is False
    assert service.circuit_breaker.consecutive_failures == 0
    assert store.consecutive_exchange_failure_stats()["consecutive_failures"] == 0
    assert store.count("safe_mode_transitions") == 0
    assert store.latest_hip3_ioc_zero_fill_proof(report["cloid"]) == proof["proof_id"]

    stored_row = store.execution_reports_for_cloid(report["cloid"])[0]
    assert store.execution_report_is_proven_hip3_ioc_zero_fill(stored_row) is True
    tampered_payload = json.loads(stored_row["payload_json"])
    tampered_payload["payload"]["zero_fill_proof"]["unknown_oid_confirmations"][0]["payload"] = {
        "status": "unknown"
    }
    tampered_row = {**stored_row, "payload_json": json.dumps(tampered_payload)}
    assert store.execution_report_is_proven_hip3_ioc_zero_fill(tampered_row) is False


def test_hip3_ioc_noncanonical_no_match_response_remains_fatal(
    base_config,
    store,
    monkeypatch,
):
    adapter = Hip3IocRaceAdapter(order_status_variant="unknown_oid")
    original_place_intent = adapter.place_intent

    def place_with_noncanonical_error(intent):
        report = original_place_intent(intent)
        payload = dict(report.payload)
        response = json.loads(json.dumps(payload["response"]))
        response["response"]["data"]["statuses"][0]["error"] += " unexpected"
        return replace(report, payload={**payload, "response": response})

    monkeypatch.setattr(adapter, "place_intent", place_with_noncanonical_error)
    service, _info = hip3_ioc_race_service(
        base_config,
        store,
        adapter=adapter,
        circuit_breaker_failure_threshold=1,
    )

    result = service.run_once()

    rejected = next(
        item
        for item in result["reports"]
        if item["status"] == IntentStatus.REJECTED.value
        and item["intent_id"] == adapter.dispatched_intents[0].intent_id
    )
    assert rejected["exchange_status"] == "rejected"
    assert rejected["payload"].get("proven_zero_fill") is not True
    assert rejected["payload"].get("zero_fill_confirmation_attempts") is None
    assert not any(
        item["stage"] == "signed_ioc_zero_fill_open"
        for item in result["liquidity_deferred_intents"]
    )
    assert result["safe_mode"]["enabled"] is True
    assert result["safe_mode"]["reason"] == SafeModeReason.CIRCUIT_BREAKER.value
    assert service.circuit_breaker.consecutive_failures == 1


def test_hip3_ioc_signed_boundary_reprice_still_normalizes_exact_zero_fill(
    base_config,
    store,
    monkeypatch,
):
    adapter = Hip3IocRaceAdapter()
    service, info = hip3_ioc_race_service(base_config, store, adapter=adapter)
    load_assessment = service.load_hip3_round_trip_assessment
    skhx_assessments = 0

    def move_entry_bid_at_signed_boundary(coin, **kwargs):
        nonlocal skhx_assessments
        if coin == "xyz:SKHX":
            skhx_assessments += 1
            if skhx_assessments == 3:
                info.books["xyz:SKHX"] = {
                    "coin": "xyz:SKHX",
                    "time": now_ms(),
                    "levels": [
                        [{"px": "1248", "sz": "10", "n": 1}],
                        [{"px": "1253", "sz": "10", "n": 1}],
                    ],
                }
        return load_assessment(coin, **kwargs)

    monkeypatch.setattr(
        service,
        "load_hip3_round_trip_assessment",
        move_entry_bid_at_signed_boundary,
    )

    result = service.run_once()

    assert skhx_assessments == 3
    planned_price = Decimal(result["intents"][0]["price"])
    signed_intent = adapter.dispatched_intents[0]
    assert planned_price == Decimal("1252")
    assert signed_intent.price == Decimal("1248")
    assert signed_intent.price != planned_price
    report = next(
        item for item in result["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    assert report["status"] == IntentStatus.REJECTED.value
    assert Decimal(report["payload"]["order_request"]["price"]) == signed_intent.price
    assert Decimal(report["payload"]["zero_fill_proof"]["price"]) == signed_intent.price
    assert report["payload"]["proven_zero_fill"] is True
    assert report["payload"]["liquidity_deferral"]["stage"] == "signed_ioc_zero_fill_open"
    assert result["safe_mode"]["enabled"] is False
    assert service.circuit_breaker.consecutive_failures == 0
    assert store.count("safe_mode_transitions") == 0


@pytest.mark.parametrize("order_status_variant", ["unknown", "partial_fill", "cloid_mismatch"])
def test_hip3_ioc_zero_fill_without_exact_terminal_proof_remains_fatal(
    base_config,
    store,
    monkeypatch,
    order_status_variant,
):
    monkeypatch.setattr(service_module, "HIP3_IOC_ZERO_FILL_CONFIRMATION_DELAYS_S", (0.0,))
    adapter = Hip3IocRaceAdapter(order_status_variant=order_status_variant)
    service, _info = hip3_ioc_race_service(
        base_config,
        store,
        adapter=adapter,
        circuit_breaker_failure_threshold=1,
    )

    result = service.run_once()

    rejected = next(
        item
        for item in result["reports"]
        if item["status"] == IntentStatus.REJECTED.value
        and item["intent_id"] == adapter.dispatched_intents[0].intent_id
    )
    assert rejected["exchange_status"] == "rejected"
    assert rejected["payload"].get("proven_zero_fill") is not True
    assert rejected["payload"].get("signed_action_performed") is not True
    assert rejected["payload"]["zero_fill_confirmation_attempts"]
    assert not any(
        item["stage"] == "signed_ioc_zero_fill_open"
        for item in result["liquidity_deferred_intents"]
    )
    assert result["safe_mode"]["enabled"] is True
    assert result["safe_mode"]["reason"] == SafeModeReason.CIRCUIT_BREAKER.value
    assert service.circuit_breaker.consecutive_failures == 1
    assert service.circuit_breaker.opened_ms is not None
    assert store.consecutive_exchange_failure_stats()["consecutive_failures"] == 1
    assert store.count("safe_mode_transitions") >= 1


def test_hip3_ioc_old_matching_order_status_before_current_dispatch_remains_fatal(
    base_config,
    store,
):
    adapter = Hip3IocRaceAdapter(order_status_age_ms=10_000)
    service, _info = hip3_ioc_race_service(
        base_config,
        store,
        adapter=adapter,
        circuit_breaker_failure_threshold=1,
    )

    result = service.run_once()

    intent = adapter.dispatched_intents[0]
    durable = store.intent_by_cloid(intent.cloid)
    assert durable is not None
    old_status = adapter.status_by_cloid[intent.cloid]
    old_order = old_status["order"]["order"]
    assert int(old_order["timestamp"]) < int(durable["attempt_updated_ms"]) - 1_000
    rejected = next(
        item
        for item in result["reports"]
        if item["status"] == IntentStatus.REJECTED.value and item["intent_id"] == intent.intent_id
    )
    assert rejected["exchange_status"] == "rejected"
    assert rejected["payload"].get("proven_zero_fill") is not True
    assert rejected["payload"]["zero_fill_confirmation_attempts"][-1]["matched"] is False
    assert not any(
        item["stage"] == "signed_ioc_zero_fill_open"
        for item in result["liquidity_deferred_intents"]
    )
    assert result["safe_mode"]["reason"] == SafeModeReason.CIRCUIT_BREAKER.value
    assert service.circuit_breaker.consecutive_failures == 1
    assert store.consecutive_exchange_failure_stats()["consecutive_failures"] == 1


def test_native_ioc_no_match_rejection_never_uses_hip3_zero_fill_exception(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(base_config.ops, circuit_breaker_failure_threshold=1),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = Hip3IocRaceAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    result = service.run_once()

    rejected = next(
        item
        for item in result["reports"]
        if item["status"] == IntentStatus.REJECTED.value
        and item["intent_id"] == adapter.dispatched_intents[0].intent_id
    )
    assert adapter.dispatched_intents[0].coin == "BTC"
    assert rejected["exchange_status"] == "rejected"
    assert rejected["payload"].get("proven_zero_fill") is not True
    assert rejected["payload"].get("zero_fill_confirmation_attempts") is None
    assert result["liquidity_deferred_intents"] == []
    assert result["safe_mode"]["enabled"] is True
    assert result["safe_mode"]["reason"] == SafeModeReason.CIRCUIT_BREAKER.value
    assert service.circuit_breaker.consecutive_failures == 1
    assert store.consecutive_exchange_failure_stats()["consecutive_failures"] == 1


@pytest.mark.parametrize(
    ("action", "reduce_only"),
    [
        (IntentAction.OPEN, True),
        (IntentAction.REDUCE, False),
        (IntentAction.CLOSE, False),
    ],
)
def test_impossible_hip3_ioc_action_reduce_only_pairs_never_neutralize(
    base_config,
    store,
    monkeypatch,
    action,
    reduce_only,
):
    adapter = Hip3IocRaceAdapter()
    service, _info = hip3_ioc_race_service(
        base_config,
        store,
        adapter=adapter,
        circuit_breaker_failure_threshold=1,
    )
    cloid = deterministic_cloid("impossible-hip3-ioc", action.value, reduce_only)
    intent = FollowerIntent(
        intent_id=deterministic_cloid("impossible-hip3-intent", cloid),
        cloid=cloid,
        action=action,
        coin="xyz:SKHX",
        side="sell",
        size=Decimal("0.012"),
        price=Decimal("1252"),
        reduce_only=reduce_only,
        mode=Mode.TESTNET,
        source_event_key="impossible-action-reduce-only-pair",
        reason="prove impossible IOC semantics remain fatal",
        created_ms=now_ms(),
    )
    report = hip3_ioc_no_match_report(intent, attempt=1, report_kind="impossible-hip3-ioc")
    service._active_dispatch_attempt_started_ms = now_ms()

    def unexpected_order_status(_cloid):
        raise AssertionError(
            "an impossible action/reduce_only pair must not query for neutral proof"
        )

    monkeypatch.setattr(adapter, "order_status", unexpected_order_status)

    normalized, deferral = service._normalize_proven_hip3_ioc_zero_fill(
        intent,
        report,
        stage="signed_ioc_zero_fill_open",
        paced_retry=True,
    )

    assert service._is_retryable_hip3_ioc_intent(intent) is False
    assert normalized == report
    assert deferral is None
    assert normalized.status == IntentStatus.REJECTED
    assert normalized.exchange_status == "rejected"
    assert normalized.payload.get("proven_zero_fill") is not True
    assert service._is_proven_hip3_ioc_zero_fill_report(normalized) is False
    service._record_runtime_result(normalized)
    assert service.safe_mode.reason == SafeModeReason.CIRCUIT_BREAKER
    assert service.circuit_breaker.consecutive_failures == 1


def test_unchanged_hip3_source_retries_proven_zero_fill_with_fresh_cloid_and_can_fill(
    base_config,
    store,
):
    adapter = Hip3IocRaceAdapter(no_fill_attempts=1)
    service, _info = hip3_ioc_race_service(base_config, store, adapter=adapter)

    first = service.run_once()
    first_report = next(
        item for item in first["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    first_proof_id = first_report["payload"]["zero_fill_proof_id"]
    second = service.run_once()

    assert {coin: position["size"] for coin, position in first["source_positions"].items()} == {
        coin: position["size"] for coin, position in second["source_positions"].items()
    }
    assert len(adapter.dispatched_intents) == 2
    first_intent, second_intent = adapter.dispatched_intents
    assert first_intent.coin == second_intent.coin == "xyz:SKHX"
    assert first_intent.side == second_intent.side == "sell"
    assert first_intent.size == second_intent.size
    assert first_intent.cloid != second_intent.cloid
    retry_identity = second["intents"][0]["execution_proof"]["post_send_retry_identity"]
    assert retry_identity == {
        "base_cloid": first_intent.cloid,
        "attempt_cloid": second_intent.cloid,
        "predecessor_zero_fill_proof_id": first_proof_id,
    }
    filled = next(
        item
        for item in second["reports"]
        if item["status"] == IntentStatus.FILLED.value and item["cloid"] == second_intent.cloid
    )
    assert filled["exchange_status"] == IntentStatus.FILLED.value
    assert adapter.positions["xyz:SKHX"].size == -second_intent.size
    assert adapter.open_orders == []
    assert second["safe_mode"]["enabled"] is False
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert service.circuit_breaker.consecutive_failures == 0
    assert store.count("safe_mode_transitions") == 0


def test_two_consecutive_hip3_zero_fills_chain_fresh_cloids_from_stable_base_then_fill(
    base_config,
    store,
):
    adapter = Hip3IocRaceAdapter(no_fill_attempts=2)
    service, _info = hip3_ioc_race_service(base_config, store, adapter=adapter)

    first = service.run_once()
    first_report = next(
        item for item in first["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    second = service.run_once()
    second_report = next(
        item for item in second["reports"] if item["exchange_status"] == "hip3_ioc_no_fill_deferred"
    )
    third = service.run_once()

    assert len(adapter.dispatched_intents) == 3
    first_intent, second_intent, third_intent = adapter.dispatched_intents
    c0, c1, c2 = (
        first_intent.cloid,
        second_intent.cloid,
        third_intent.cloid,
    )
    assert len({c0, c1, c2}) == 3
    proof0 = first_report["payload"]["zero_fill_proof_id"]
    proof1 = second_report["payload"]["zero_fill_proof_id"]
    assert proof0 != proof1
    assert c1 == deterministic_cloid("hip3-ioc-zero-fill-retry", c0, proof0)
    assert c2 == deterministic_cloid("hip3-ioc-zero-fill-retry", c0, proof1)
    assert first_report["payload"]["post_send_retry_identity"] == {
        "base_cloid": c0,
        "attempt_cloid": c0,
        "predecessor_zero_fill_proof_id": None,
    }
    assert second_report["payload"]["post_send_retry_identity"] == {
        "base_cloid": c0,
        "attempt_cloid": c1,
        "predecessor_zero_fill_proof_id": proof0,
    }
    assert third_intent.execution_proof["post_send_retry_identity"] == {
        "base_cloid": c0,
        "attempt_cloid": c2,
        "predecessor_zero_fill_proof_id": proof1,
    }
    assert store.latest_hip3_ioc_zero_fill_proof(c0) == proof1

    filled = next(
        item
        for item in third["reports"]
        if item["status"] == IntentStatus.FILLED.value and item["cloid"] == c2
    )
    assert filled["exchange_status"] == IntentStatus.FILLED.value
    journaled = [row for row in store.recent("follower_intents", 20) if row["coin"] == "xyz:SKHX"]
    assert len(journaled) == 3
    assert {row["cloid"] for row in journaled} == {c0, c1, c2}
    assert all(row["attempt_phase"] == "terminal" for row in journaled)
    assert all(len(store.execution_reports_for_cloid(cloid)) == 1 for cloid in (c0, c1, c2))
    assert store.pending_intent_count(Mode.TESTNET) == 0
    assert adapter.positions["xyz:SKHX"].size == -third_intent.size
    assert adapter.open_orders == []
    assert third["safe_mode"]["enabled"] is False
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert service.circuit_breaker.consecutive_failures == 0
    assert service.circuit_breaker.opened_ms is None
    assert store.count("safe_mode_transitions") == 0


def test_watchdog_first_hip3_zero_fill_uses_terminal_timestamp_after_durable_dispatch(
    base_config,
    store,
):
    initial_service, _ = two_hip3_open_service_with_first_market_illiquid(base_config, store)
    config = replace(
        initial_service.config,
        ops=replace(
            initial_service.config.ops,
            dead_man_policy=DeadManPolicy.WATCHDOG_FALLBACK,
        ),
    )
    cloid = deterministic_cloid("watchdog-first-hip3-zero-fill")
    created_ms = now_ms()
    identity = {
        "base_cloid": cloid,
        "attempt_cloid": cloid,
        "predecessor_zero_fill_proof_id": None,
    }
    desired = DesiredState(
        state_id=deterministic_cloid("watchdog-first-hip3-plan", cloid),
        source_event_key="watchdog-first-hip3",
        mode=Mode.TESTNET,
        positions={"xyz:SKHX": Position("xyz:SKHX", Decimal("-0.012"), leverage=1)},
        reason="prove watchdog terminal lookup honors durable dispatch time",
        created_ms=created_ms,
        source_wallet=config.source_wallet,
        action_account=config.exchange.follower_account_address,
        source_network="testnet",
    )
    intent = FollowerIntent(
        intent_id=deterministic_cloid("watchdog-first-hip3-intent", cloid),
        cloid=cloid,
        action=IntentAction.OPEN,
        coin="xyz:SKHX",
        side="sell",
        size=Decimal("0.012"),
        price=Decimal("1252"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key=desired.source_event_key,
        reason="watchdog observes IOC terminal state before worker response handling",
        created_ms=created_ms,
        desired_state_id=desired.state_id,
        execution_proof={"post_send_retry_identity": identity},
    )
    assert store.prepare_execution_plan(desired, [intent])
    assert store.begin_intent_dispatch(intent.intent_id)
    durable = store.intent_by_cloid(cloid)
    assert durable is not None
    dispatch_boundary_ms = int(durable["attempt_updated_ms"])
    terminal_ms = dispatch_boundary_ms + 1
    adapter = FakeExecutionAdapter()
    adapter.status_by_cloid[cloid] = hip3_ioc_zero_fill_status(
        intent,
        oid=130_001,
        observed_ms=terminal_ms,
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=cast(FakeInfoClient, initial_service.info_client),
        execution_info_client=cast(FakeInfoClient, initial_service.execution_info_client),
        execution_adapter=adapter,
    )
    config.ops.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
    config.ops.kill_switch_path.write_text("contain", encoding="utf-8")

    result = service.containment_watchdog_once()

    assert result["pending_before"] == 1
    assert len(result["settled"]) == 1
    settled = result["settled"][0]
    assert settled["status"] == IntentStatus.REJECTED.value
    assert settled["exchange_status"] == "watchdog_settled:hip3_ioc_no_fill"
    assert settled["payload"]["watchdog"] is True
    assert settled["payload"]["signed_action_performed"] is True
    assert settled["payload"]["proven_zero_fill"] is True
    proof = settled["payload"]["zero_fill_proof"]
    assert proof["order_timestamp"] == terminal_ms
    assert proof["status_timestamp"] == terminal_ms
    assert proof["order_timestamp"] > dispatch_boundary_ms
    assert settled["payload"]["post_send_retry_identity"] == identity
    assert store.latest_hip3_ioc_zero_fill_proof(cloid) == proof["proof_id"]
    assert store.pending_intent_count(Mode.TESTNET) == 0
    assert result["cancellations"] == []
    assert result["errors"] == []
    assert store.count("safe_mode_transitions") == 0


def test_bounded_hip3_cleanup_retries_proven_zero_fill_then_flattens_without_incident(
    base_config,
    store,
):
    initial_service, _initial_adapter, info = hip3_short_cap_service(base_config, store)
    coin = "xyz:CAP"
    adapter = Hip3CleanupIocRaceAdapter(coin=coin, size=Decimal("-0.1"))
    service = CopyTraderService(
        initial_service.config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )

    cleanup = service._run_passive_canary_cleanup(
        coin=coin,
        entry_cloid="0x" + "9" * 32,
        cancel_entry=False,
        max_cleanup_size=Decimal("0.1"),
        baseline_positions={},
        asset_meta=AssetMeta(coin, 3, max_leverage=20),
        fallback_mid=Decimal("100"),
        operation="test bounded HIP-3 cleanup",
    )

    assert cleanup["flat"] is True
    assert len(cleanup["cleanup_reports"]) == 2
    no_fill, filled = cleanup["cleanup_reports"]
    assert no_fill.status == IntentStatus.REJECTED
    assert no_fill.exchange_status == "hip3_ioc_no_fill_cleanup_retry"
    assert no_fill.payload["signed_action_performed"] is True
    assert no_fill.payload["proven_zero_fill"] is True
    assert no_fill.payload["requires_post_action_reconcile"] is True
    assert filled.status == IntentStatus.FILLED
    assert len(adapter.cleanup_intents) == 2
    assert adapter.cleanup_intents[0].cloid != adapter.cleanup_intents[1].cloid
    assert all(intent.reduce_only for intent in adapter.cleanup_intents)
    assert adapter.positions == {}
    assert adapter.open_orders == []
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert service.circuit_breaker.consecutive_failures == 0
    assert service.circuit_breaker.opened_ms is None
    assert store.count("safe_mode_transitions") == 0


@pytest.mark.parametrize(
    ("source_size", "expected_side", "expected_entry_price"),
    [
        (Decimal("1"), "buy", Decimal("102.01")),
        (Decimal("-1"), "sell", Decimal("99.99")),
    ],
)
def test_hip3_service_reserves_oracle_envelope_below_final_notional_cap(
    base_config,
    store,
    source_size,
    expected_side,
    expected_entry_price,
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        source_dex_scope=SourceDexScope.ALL_CONFIGURED_MARKETS,
        risk=replace(
            base_config.risk,
            allowed_symbols=("xyz:CAP",),
            fixed_multiplier=Decimal("1"),
            balance_sizing_enabled=False,
            max_notional_usd=Decimal("15"),
            max_gross_exposure_usd=Decimal("40"),
            min_order_size=Decimal("0.001"),
            hip3_oracle_envelope_bps=Decimal("100"),
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            expected_account_mode=AccountMode.UNIFIED,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.user_abstraction = "unifiedAccount"
    info.spot_state = {"balances": [{"coin": "USDC", "token": 0, "total": "1000", "hold": "0"}]}
    info.state = {"assetPositions": [], "marginSummary": {"accountValue": "1000"}}
    info.dex_states["xyz"] = {
        "assetPositions": [
            {
                "position": {
                    "coin": "CAP",
                    "szi": str(source_size),
                    "entryPx": "100",
                    "leverage": {"type": "cross", "value": 1},
                }
            }
        ],
        "marginSummary": {"accountValue": "1000"},
    }
    info.dex_mids["xyz"] = {"xyz:CAP": "100"}
    info.dex_meta["xyz"] = {"universe": [{"name": "CAP", "szDecimals": 3, "maxLeverage": 20}]}
    info.dex_meta_and_contexts["xyz"] = [
        info.dex_meta["xyz"],
        [{"oraclePx": "101", "markPx": "100", "midPx": "100"}],
    ]
    info.books["xyz:CAP"] = {
        "coin": "xyz:CAP",
        "time": now_ms(),
        "levels": [
            [{"px": "99.99", "sz": "1", "n": 1}],
            [{"px": "102.01", "sz": "1", "n": 1}],
        ],
    }
    adapter = FilledOrderAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=info,
        execution_info_client=info,
        execution_adapter=adapter,
    )
    service.observer._unified_state_provider = lambda: UnifiedAccountSnapshot(
        account=config.source_wallet,
        clearinghouse_states={"": info.state, "xyz": info.dex_states["xyz"]},
        observed_ms=now_ms(),
        received_ms=now_ms(),
    )

    result = service.run_once()

    assert info.dex_mids["xyz"]["xyz:CAP"] == "100"
    assert service.load_execution_mids()["xyz:CAP"] == Decimal("101")
    assert result["preflight"]["passed"] is True
    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert result["liquidity_deferred_intents"] == []
    assert result["deferred_intents"] == []
    assert len(result["intents"]) == 1
    planned = result["intents"][0]
    assert planned["side"] == expected_side
    assert Decimal(planned["size"]) == Decimal("0.147")
    assert Decimal(planned["price"]) == expected_entry_price
    assert Decimal(result["desired_state"]["positions"]["xyz:CAP"]["size"]) == (
        (Decimal("15") / Decimal("102.01")).copy_sign(source_size)
    )
    placed = [
        report
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert len(placed) == 1
    assert placed[0].status == IntentStatus.FILLED
    signed_intent = cast(FollowerIntent, placed[0].payload["intent"])
    assert signed_intent.side == expected_side
    assert signed_intent.size == Decimal("0.147")
    assert signed_intent.price == expected_entry_price
    assert signed_intent.execution_proof["oracle_px"] == Decimal("101")
    assert signed_intent.execution_proof["entry_limit"] == expected_entry_price
    assert abs(signed_intent.size * signed_intent.price) <= config.risk.max_notional_usd
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0


@pytest.mark.parametrize(
    ("move_on_assessment", "expected_stage"),
    [
        (1, "planning_cap_reprice"),
        (2, "final_cap_reprice"),
        (3, "signed_dispatch_cap_reprice"),
    ],
)
def test_hip3_short_oracle_move_defers_at_each_cap_boundary_without_signing_order(
    base_config,
    store,
    monkeypatch,
    move_on_assessment,
    expected_stage,
):
    service, adapter, info = hip3_short_cap_service(base_config, store)
    load_assessment = service.load_hip3_round_trip_assessment
    assessment_calls = 0

    def move_before_selected_assessment(coin, **kwargs):
        nonlocal assessment_calls
        assessment_calls += 1
        if assessment_calls == move_on_assessment:
            move_hip3_short_cap_market(info)
        return load_assessment(coin, **kwargs)

    monkeypatch.setattr(
        service,
        "load_hip3_round_trip_assessment",
        move_before_selected_assessment,
    )

    result = service.run_once()

    assert assessment_calls == move_on_assessment
    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert len(result["liquidity_deferred_intents"]) == 1
    deferred = result["liquidity_deferred_intents"][0]
    assert deferred["stage"] == expected_stage
    assert deferred["intent"]["coin"] == "xyz:CAP"
    assert deferred["intent"]["side"] == "sell"
    assert Decimal(deferred["intent"]["size"]) == Decimal("0.148")
    assert "cap-safe size" in " ".join(deferred["blockers"])
    assert deferred["retry_not_before_ms"] > now_ms()
    assert adapter.signed_order_calls == 0
    assert not any(
        isinstance(report.payload.get("intent"), FollowerIntent) for report in adapter.reports
    )
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0


def test_hip3_short_multilevel_bid_notional_defers_even_when_sell_limit_looks_safe(
    base_config, store
):
    service, adapter, info = hip3_short_cap_service(base_config, store)
    move_hip3_short_cap_market(info, multi_level=True)
    intent = FollowerIntent(
        intent_id="hip3-short-multilevel-cap",
        cloid="0x" + "8" * 32,
        action=IntentAction.OPEN,
        coin="xyz:CAP",
        side="sell",
        size=Decimal("0.148"),
        price=Decimal("100.5"),
        reduce_only=False,
        mode=Mode.TESTNET,
        source_event_key="hip3-short-multilevel-cap-source",
        reason="aggregate consumed bid notional must remain below the cap",
        created_ms=now_ms(),
    )
    meta = AssetMeta("xyz:CAP", 3, max_leverage=20)

    assessment = service.load_hip3_round_trip_assessment(
        "xyz:CAP",
        opening_side="sell",
        requested_size=intent.size,
        asset_meta=meta,
    )
    admitted, liquidity_deferred, blockers = service._admit_hip3_open_intents(
        [intent],
        asset_meta={"xyz:CAP": meta},
    )

    assert assessment.quote is not None
    assert assessment.quote.entry_limit == Decimal("100.5")
    assert assessment.quote.entry_best_px == Decimal("102")
    assert assessment.quote.entry_notional_bound_px == Decimal("102.01")
    assert intent.size * assessment.quote.entry_limit == Decimal("14.8740")
    visible_fill_notional = Decimal("0.147") * Decimal("102") + Decimal("0.001") * Decimal("100.5")
    assert visible_fill_notional == Decimal("15.0945")
    assert intent.size * assessment.quote.entry_notional_bound_px == Decimal("15.09748")
    assert intent.size * assessment.quote.entry_notional_bound_px >= visible_fill_notional
    assert admitted == []
    assert blockers == []
    assert len(liquidity_deferred) == 1
    assert liquidity_deferred[0].stage == "planning_cap_reprice"
    assert "15.09748" in " ".join(liquidity_deferred[0].blockers)
    assert "cap-safe size" in " ".join(liquidity_deferred[0].blockers)
    assert adapter.signed_order_calls == 0
    assert adapter.reports == []
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0


def test_hip3_stable_bounded_short_still_crosses_all_cap_boundaries(base_config, store):
    service, adapter, _info = hip3_short_cap_service(base_config, store)

    result = service.run_once()

    assert result["safe_mode"]["enabled"] is False
    assert result["safe_mode"]["reason"] == SafeModeReason.NONE.value
    assert result["liquidity_deferred_intents"] == []
    assert result["deferred_intents"] == []
    assert len(result["intents"]) == 1
    assert result["intents"][0]["side"] == "sell"
    assert Decimal(result["intents"][0]["size"]) == Decimal("0.148")
    assert Decimal(result["intents"][0]["price"]) == Decimal("100.5")
    assert adapter.signed_order_calls == 1
    placed = [
        report
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert len(placed) == 1
    assert placed[0].status == IntentStatus.FILLED
    signed_intent = cast(FollowerIntent, placed[0].payload["intent"])
    assert signed_intent.side == "sell"
    assert signed_intent.size == Decimal("0.148")
    assert signed_intent.price == Decimal("100.5")
    assert signed_intent.size * signed_intent.price == Decimal("14.8740")
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert store.count("safe_mode_transitions") == 0


def test_exchange_cycle_executes_two_closes_with_one_new_risk_slot(base_config, store):
    follower_positions = {
        "BTC": Position("BTC", Decimal("-0.005"), entry_px=Decimal("50000"), leverage=2),
        "ETH": Position("ETH", Decimal("-0.05"), entry_px=Decimal("3000"), leverage=2),
    }
    append_desired_positions(store, follower_positions)
    info = add_eth_position(FakeInfoClient())
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(
            base_config.ops,
            max_new_intents_per_cycle=1,
            max_open_intents=1,
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FilledOrderAdapter(positions=dict(follower_positions))
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert result["safe_mode"]["enabled"] is False
    assert [item["action"] for item in result["intents"]] == ["close", "close"]
    assert all(item["reduce_only"] for item in result["intents"])
    assert [item["action"] for item in result["deferred_intents"]] == ["open", "open"]
    placed = [
        report.payload["intent"].action
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert placed == [IntentAction.CLOSE, IntentAction.CLOSE]
    assert adapter.positions == {}
    assert result["desired_state_committed"] is True


def test_exchange_cycle_prioritizes_reduction_and_bounds_new_exposure(base_config, store):
    follower_positions = {
        "SOL": Position("SOL", Decimal("0.5"), entry_px=Decimal("150"), leverage=2),
    }
    append_desired_positions(store, follower_positions)
    info = add_eth_position(FakeInfoClient())
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(
            base_config.ops,
            max_new_intents_per_cycle=1,
            max_open_intents=1,
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FilledOrderAdapter(positions=dict(follower_positions))
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert result["safe_mode"]["enabled"] is False
    assert [(item["action"], item["coin"]) for item in result["intents"]] == [
        ("reduce", "SOL"),
        ("open", "BTC"),
    ]
    assert [(item["action"], item["coin"]) for item in result["deferred_intents"]] == [
        ("open", "ETH")
    ]
    placed = [
        report.payload["intent"].action
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert placed == [IntentAction.REDUCE, IntentAction.OPEN]
    assert set(adapter.positions) == {"BTC"}
    assert set(result["desired_state"]["positions"]) == {"BTC"}
    assert result["desired_state_committed"] is True


def test_source_reaction_drains_two_opens_one_at_a_time(base_config, store):
    info = add_eth_position(FakeInfoClient())
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(
            base_config.ops,
            max_new_intents_per_cycle=1,
            max_open_intents=1,
        ),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FilledOrderAdapter()
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)
    observed = now_ms()
    event = SourceEvent(
        idempotency_key="two-market-source-fill",
        event_type=SourceEventType.FILL,
        exchange_ts_ms=observed,
        observed_ts_ms=observed,
        payload={"event_subtype": "fill"},
    )

    reaction = service.react_to_source_event(event)

    assert reaction["action"] == "run_once"
    assert reaction["deferred_open_drain"]["status"] == "drained"
    assert reaction["deferred_open_drain"]["cycle_count"] == 2
    assert reaction["deferred_open_drain"]["cycles"][0]["active_open_coins"] == ["BTC"]
    assert reaction["deferred_open_drain"]["cycles"][0]["deferred_open_coins"] == ["ETH"]
    assert reaction["deferred_open_drain"]["cycles"][1]["active_open_coins"] == ["ETH"]
    assert reaction["deferred_open_drain"]["cycles"][1]["deferred_open_coins"] == []
    assert reaction["result"]["deferred_intents"] == []
    assert reaction["result"]["desired_state_committed"] is False
    assert reaction["result"]["execution_finalization"]["status"] == ("actual_checkpoint_committed")
    assert reaction["result"]["execution_finalization"]["committed_target"] is False
    assert reaction["result"]["desired_state"]["positions"]["ETH"]["size"] == (
        "0.08333333333333333333333333333"
    )
    assert reaction["result"]["reconciled_checkpoint"]["positions"]["ETH"]["size"] == "0.0833"
    assert set(adapter.positions) == {"BTC", "ETH"}
    placed_coins = [
        report.payload["intent"].coin
        for report in adapter.reports
        if isinstance(report.payload.get("intent"), FollowerIntent)
    ]
    assert placed_coins == ["BTC", "ETH"]


def test_exchange_mode_stages_close_before_reopen(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"]["szi"] = "-1.0"
    adapter = FilledOrderAdapter(positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)})
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)
    close_result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert len(close_result["intents"]) == 1
    assert close_result["intents"][0]["action"] == "close"
    assert close_result["intents"][0]["reduce_only"] is True
    assert len(close_result["deferred_intents"]) == 1
    assert close_result["deferred_intents"][0]["action"] == "open"
    assert close_result["deferred_intents"][0]["reduce_only"] is False
    assert close_result["desired_state"]["positions"] == {}
    assert len(adapter.reports) == 1
    assert store.count("desired_states") == 2

    adapter.positions = {}
    reopen_result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.NONE
    assert len(reopen_result["intents"]) == 1
    assert reopen_result["intents"][0]["action"] == "open"
    assert reopen_result["intents"][0]["reduce_only"] is False
    assert reopen_result["deferred_intents"] == []
    assert Decimal(reopen_result["desired_state"]["positions"]["BTC"]["size"]) < 0
    assert len(adapter.reports) == 3
    assert store.count("desired_states") == 3


def test_exchange_reversal_dust_residual_requires_operator_reconcile(base_config, store):
    append_desired(store, btc_size=Decimal("0.005"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"]["szi"] = "-1.0"
    adapter = DustPartialFillAdapter(
        positions={"BTC": Position("BTC", Decimal("0.005"), leverage=2)}
    )
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    close_result = service.run_once()
    assert close_result["safe_mode"]["enabled"] is False
    assert close_result["reports"][2]["exchange_status"] == "dust_residual_accepted"
    assert close_result["deferred_intents"][0]["action"] == "open"

    adapter.positions = {
        "BTC": Position(
            "BTC",
            Decimal("0.0001"),
            entry_px=Decimal("50000"),
            leverage=2,
        )
    }
    residual_result = service.run_once()
    assert residual_result["safe_mode"]["reason"] == "partial_fill"
    assert "cannot be flattened automatically" in residual_result["safe_mode"]["detail"]
    assert residual_result["reports"][1]["exchange_status"] == "partial_fill"
    assert residual_result["reports"][2]["exchange_status"] == "ioc_unfilled"
    assert residual_result["reports"][2]["status"] == "canceled"
    assert residual_result["deferred_intents"][0]["action"] == "open"
    assert len(adapter.reports) == 2
    assert store.pending_intent_count() == 0

    uncleared = service.manual_reconcile()
    assert uncleared["safe_mode"]["cleared"] is False
    assert uncleared["safe_mode"]["safe_mode"]["reason"] == "partial_fill"
    assert "exact follower positions" in uncleared["safe_mode"]["safe_mode"]["detail"]

    adapter.positions = {}
    cleared = service.manual_reconcile()
    assert cleared["safe_mode"]["cleared"] is True
    assert cleared["safe_mode"]["safe_mode"]["enabled"] is False


def test_kill_switch_blocks_paper_execution(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    config = replace(
        base_config, mode=Mode.PAPER, ops=replace(base_config.ops, kill_switch_path=kill)
    )
    service = CopyTraderService(config, store=store, info_client=FakeInfoClient())
    result = service.run_once()
    assert service.safe_mode.reason.value == "operator_kill_switch"
    assert result["reports"][0]["exchange_status"] == "blocked:operator_kill_switch"
    assert service.paper.positions == {}
    assert store.count("desired_states") == 0


def test_kill_switch_does_not_promote_exchange_desired_baseline(base_config, store, tmp_path):
    kill = tmp_path / "KILL_SWITCH"
    kill.write_text("stop", encoding="utf-8")
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        ops=replace(base_config.ops, kill_switch_path=kill),
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.OPERATOR_KILL_SWITCH
    assert result["reports"][0]["exchange_status"] == "blocked:operator_kill_switch"
    assert adapter.reports == []
    assert store.count("desired_states") == 2
    assert store.latest_desired_positions(Mode.TESTNET, committed_only=True) == {}


def test_exchange_runtime_rate_limiter_blocks_burst(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, max_exchange_actions_per_minute=3),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=adapter,
    )
    service.exchange_rate_limiter.record()
    service.exchange_rate_limiter.record()
    result = service.run_once()
    assert len(result["intents"]) == 2
    assert len(adapter.reports) == 1
    assert service.safe_mode.reason.value == "rate_limit"
    assert any(report["exchange_status"] == "blocked:rate_limit" for report in result["reports"])


def test_exchange_order_arms_and_clears_dead_man_cancel(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"].pop("leverage", None)
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.FILLED)
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert [report["exchange_status"] for report in result["reports"]] == [
        "leverage_updated",
        "dead_man_scheduled",
        "filled",
        "dead_man_cleared",
    ]
    assert len(adapter.reports) == 2
    assert adapter.scheduled_cancel_times[0] >= now_ms() + config.ops.dead_man_cancel_ms - 1000
    assert adapter.scheduled_cancel_times[-1] is None
    signed_actions = store.recent("signed_action_attempts", 10)
    assert {row["action"] for row in signed_actions} == {
        "update_leverage_cross",
        "dead_man_schedule",
        "dead_man_clear",
    }
    assert {row["attempt_phase"] for row in signed_actions} == {"terminal"}


def test_dead_man_crash_after_prepare_blocks_restart_without_sending(
    base_config, store, monkeypatch
):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    def crash_before_signer(attempt_id: str) -> bool:
        raise SystemExit(f"simulated crash before signer for {attempt_id}")

    monkeypatch.setattr(store, "begin_signed_action_dispatch", crash_before_signer)
    with pytest.raises(SystemExit, match="before signer"):
        service._schedule_dead_man_cancel(
            scheduled_time_ms=now_ms() + config.ops.dead_man_cancel_ms,
            operation="crash-test",
            count_rate=False,
        )

    unresolved = store.unresolved_signed_action_attempts(
        Mode.TESTNET,
        account=config.exchange.follower_account_address,
        network="testnet",
    )
    assert [(row["action"], row["attempt_phase"]) for row in unresolved] == [
        ("dead_man_schedule", "prepared")
    ]
    assert adapter.schedule_cancel_reports == []

    restart_adapter = FakeExecutionAdapter()
    restarted = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=restart_adapter,
    )
    result = restarted.run_once()
    assert result["startup_recovery"]["requires_operator_review"] is True
    assert result["startup_recovery"]["signed_action_attempts"][0]["attempt_phase"] == "prepared"
    assert restarted.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert restart_adapter.reports == []
    assert restart_adapter.schedule_cancel_reports == []


def test_leverage_crash_after_send_blocks_restart_without_retry(base_config, store, monkeypatch):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )

    def crash_after_send(attempt_id: str, report: ExecutionReport) -> bool:
        assert report.exchange_status == "leverage_updated"
        raise SystemExit(f"simulated crash after send for {attempt_id}")

    monkeypatch.setattr(store, "finish_signed_action_attempt", crash_after_send)
    with pytest.raises(SystemExit, match="after send"):
        service._set_canary_leverage("BTC", operation="crash-test")

    assert adapter.leverage_updates == [("BTC", 1, True)]
    unresolved = store.unresolved_signed_action_attempts(
        Mode.TESTNET,
        account=config.exchange.follower_account_address,
        network="testnet",
    )
    assert [(row["action"], row["attempt_phase"]) for row in unresolved] == [
        ("update_leverage_cross", "dispatching")
    ]

    restart_adapter = FakeExecutionAdapter()
    restarted = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=restart_adapter,
    )
    result = restarted.run_once()
    assert result["startup_recovery"]["requires_operator_review"] is True
    assert result["startup_recovery"]["signed_action_attempts"][0]["attempt_phase"] == "dispatching"
    assert restarted.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
    assert restart_adapter.leverage_updates == []
    settlement = restarted.settle_pending_intents()
    assert settlement["requires_operator_review"] is True
    assert settlement["signed_action_attempts"][0]["attempt_phase"] == "dispatching"
    manual = restarted.manual_reconcile()
    assert manual["safe_mode"]["cleared"] is False
    assert manual["safe_mode"]["safe_mode"]["reason"] == "restart_mid_fill"


def test_non_order_pre_send_block_is_terminal_never_unknown(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
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
    cloid = deterministic_cloid("pre-send-blocked-signed-action", now_ms())

    def blocked_before_send() -> ExecutionReport:
        raise PreSendBlockedError("last-mile truth expired before signer dispatch")

    report = service._timed_exchange_action(
        intent_id="leverage:pre-send-blocked:BTC:1",
        cloid=cloid,
        count_rate=False,
        signed_action_kind="update_leverage_cross",
        signed_action_payload={"coin": "BTC", "leverage": 1},
        action=blocked_before_send,
    )

    assert report.status == IntentStatus.SKIPPED
    assert report.exchange_status == "pre_send_blocked"
    row = store.recent("signed_action_attempts", 1)[0]
    assert row["attempt_phase"] == "terminal"
    assert store.unresolved_signed_action_attempt_count(Mode.TESTNET) == 0


def test_testnet_order_blocks_when_dead_man_schedule_fails(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"].pop("leverage", None)
    adapter = FakeExecutionAdapter(
        forced_status=IntentStatus.FILLED,
        schedule_cancel_status=IntentStatus.REJECTED,
    )
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "rejected"
    assert result["reports"][2]["exchange_status"].startswith("blocked:")
    assert [report.exchange_status for report in adapter.reports] == ["leverage_updated"]
    assert service.safe_mode.enabled


def test_testnet_order_allows_known_schedule_cancel_volume_rejection(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"].pop("leverage", None)
    adapter = VolumeRejectedScheduleAdapter(
        forced_status=IntentStatus.FILLED,
        schedule_cancel_status=IntentStatus.REJECTED,
    )
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert [report["exchange_status"] for report in result["reports"]] == [
        "leverage_updated",
        "rejected",
        "filled",
    ]
    assert len(adapter.reports) == 2
    assert service.safe_mode.reason == SafeModeReason.NONE


def test_live_order_blocks_when_dead_man_schedule_fails(base_config, store):
    config = replace(
        base_config,
        mode=Mode.LIVE,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_wallet_address="0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
            api_private_key="0x" + "1" * 64,
            live_enable=True,
            confirm_mainnet_live=True,
            live_copy_enable=True,
        ),
    )
    info = FakeInfoClient()
    info.state["assetPositions"][0]["position"].pop("leverage", None)
    adapter = FakeExecutionAdapter(schedule_cancel_status=IntentStatus.REJECTED)
    service = CopyTraderService(config, store=store, info_client=info, execution_adapter=adapter)

    result = service.run_once()

    assert [report.exchange_status for report in adapter.reports] == ["leverage_updated"]
    assert result["reports"][0]["exchange_status"] == "leverage_updated"
    assert result["reports"][1]["exchange_status"] == "rejected"
    assert result["reports"][2]["exchange_status"].startswith("blocked:")
    assert service.safe_mode.reason == SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE


def test_exchange_persistent_rate_limiter_blocks_after_restart(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, max_exchange_actions_per_minute=3),
    )
    first_info = FakeInfoClient()
    first_info.state["assetPositions"][0]["position"].pop("leverage", None)
    first = CopyTraderService(
        config,
        store=store,
        info_client=first_info,
        execution_adapter=FakeExecutionAdapter(forced_status=IntentStatus.FILLED),
    )
    first_result = first.run_once()
    assert any(report["status"] == "filled" for report in first_result["reports"])
    assert first_result["desired_state_committed"] is True
    store.append_execution_report(
        ExecutionReport(
            report_id=deterministic_cloid("persistent-rate-limit-seed", now_ms()),
            intent_id="persistent-rate-limit-seed",
            cloid=deterministic_cloid("persistent-rate-limit-seed-cloid", now_ms()),
            status=IntentStatus.ACKED,
            exchange_status="seeded_counted_action",
            exchange_ts_ms=now_ms(),
            payload={},
        )
    )

    second_adapter = FakeExecutionAdapter(
        positions={"BTC": Position("BTC", Decimal("0.005"), leverage=1)}
    )
    second = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=second_adapter,
    )
    result = second.run_once()
    assert second.safe_mode.reason == SafeModeReason.RATE_LIMIT
    assert any(
        "persistent action rate limit hit" in report["payload"]["detail"]
        for report in result["reports"]
        if report["exchange_status"] == "blocked:rate_limit"
    )
    assert second_adapter.reports == []


def test_exchange_circuit_breaker_blocks_after_reject(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, circuit_breaker_failure_threshold=1),
    )
    adapter = FakeExecutionAdapter(forced_status=IntentStatus.REJECTED)
    service = CopyTraderService(
        config,
        store=store,
        info_client=add_eth_position(FakeInfoClient()),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert len(adapter.reports) == 2
    assert service.safe_mode.reason.value == "circuit_breaker"
    assert any(
        report["exchange_status"] == "blocked:circuit_breaker" for report in result["reports"]
    )


def test_exchange_persistent_circuit_breaker_blocks_after_restart(base_config, store):
    bind_testnet_scope(store)
    store.append_execution_report(
        ExecutionReport(
            report_id="prior-reject",
            intent_id="prior-intent",
            cloid="0x66666666666666666666666666666666",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=now_ms(),
            payload={"error": "prior reject"},
        )
    )
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, circuit_breaker_failure_threshold=1),
    )
    adapter = FakeExecutionAdapter()
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert service.safe_mode.reason == SafeModeReason.CIRCUIT_BREAKER
    assert service.circuit_breaker.opened_ms is not None
    assert any(
        "circuit breaker open" in report["payload"]["detail"]
        for report in result["reports"]
        if report["exchange_status"] == "blocked:circuit_breaker"
    )
    assert adapter.reports == []


def test_exchange_circuit_breaker_seeds_failures_from_journal_after_restart(base_config, store):
    bind_testnet_scope(store)
    store.append_execution_report(
        ExecutionReport(
            report_id="prior-reject-seed",
            intent_id="prior-intent-seed",
            cloid="0x77777777777777777777777777777777",
            status=IntentStatus.REJECTED,
            exchange_status="rejected",
            exchange_ts_ms=now_ms(),
            payload={"error": "prior reject"},
        )
    )
    append_desired(store, btc_size=Decimal("0.001"))
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(base_config.ops, circuit_breaker_failure_threshold=2),
    )
    adapter = FakeExecutionAdapter(
        forced_status=IntentStatus.REJECTED,
        positions={"BTC": Position("BTC", Decimal("0.001"), leverage=2)},
    )
    service = CopyTraderService(
        config,
        store=store,
        info_client=FakeInfoClient(),
        execution_adapter=adapter,
    )
    result = service.run_once()
    assert len(adapter.reports) == 1
    assert any(report["status"] == "rejected" for report in result["reports"])
    assert service.safe_mode.reason == SafeModeReason.CIRCUIT_BREAKER


def test_exchange_slow_action_trips_timeout(base_config, store):
    config = replace(
        base_config,
        mode=Mode.TESTNET,
        exchange=ExchangeConfig(
            follower_account_address="0xf000000000000000000000000000000000000000",
            api_private_key="0x" + "1" * 64,
            testnet_enable=True,
        ),
        ops=replace(
            base_config.ops,
            exchange_action_timeout_s=Decimal("2"),
            exchange_expires_after_ms=2_000,
        ),
    )
    adapter = FakeExecutionAdapter(delay_s=2.25)
    service = CopyTraderService(
        config, store=store, info_client=FakeInfoClient(), execution_adapter=adapter
    )
    result = service.run_once()
    assert result["reports"][0]["status"] == "acked"
    assert service.safe_mode.reason.value == "order_timeout"
