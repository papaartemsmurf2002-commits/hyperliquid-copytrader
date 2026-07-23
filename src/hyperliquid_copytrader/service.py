from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from threading import RLock
from time import monotonic, sleep
from typing import Any
from uuid import uuid4

from .cloid import deterministic_cloid
from .config import AppConfig, DeadManPolicy
from .copy_engine import AssetMeta, CopyEngine, CopyResult
from .exchange.hyperliquid import (
    ExecutionAdapter,
    HyperliquidExecutionAdapter,
    PreSendBlockedError,
    classify_order_status,
)
from .guard import ExecutionGuard, increases_exposure, pending_exposure_increasing_count
from .incidents import incident_guidance
from .liquidity import (
    MarketLiquiditySnapshot,
    ReduceOnlyQuote,
    RoundTripAssessment,
    RoundTripQuote,
    assess_round_trip_quote,
    build_reduce_only_quote,
    parse_market_liquidity_snapshot,
)
from .market_catalog import FrozenMarketContextProvider, resolve_public_market_universe
from .order_preflight import HYPERLIQUID_PERP_MIN_NOTIONAL_USD
from .mainnet_canary import (
    MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT,
    MAINNET_CANARY_ACKNOWLEDGEMENT,
    MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD,
    MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD,
    build_mainnet_canary_profile,
)
from .markets import (
    FrozenMarketUniverseManifest,
    MarketIdentityError,
    canonical_market_symbol,
    compare_market_universes,
    market_dex,
    qualify_market_symbol,
)
from .models import (
    DesiredState,
    ExecutionAttemptPhase,
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
    parse_decimal,
    to_jsonable,
)
from .observer import (
    FillBackfillReport,
    HyperliquidInfoClient,
    InfoClient,
    SourceObserver,
    SourceGapBackfillReport,
    SourceSnapshot,
    SourceWebsocketMessageError,
)
from .ops import prometheus_metrics, readiness_snapshot
from .paper import PaperAccount
from .persistence import JournalIntegrityError, SQLiteStore
from .preflight import (
    PreflightReport,
    active_subaccount_assignment_status,
    build_preflight_report,
)
from .precision import aggressive_ioc_price, quantize_price, quantize_size
from .runtime import CircuitBreaker, RuntimeDecision, SlidingWindowRateLimiter
from .runtime_lock import (
    AccountRuntimeFileLock,
    RuntimeFileLockBusy,
    RuntimeFileLockError,
    account_runtime_lock_path,
    generation_fence_lock_path,
    signer_runtime_lock_path,
)
from .safety import ConsistencyShield, SafeModeController
from .security import redact_secrets
from .validation_guardian import ControllerClaim, ControllerRegistry, read_supervisor_lease


MAX_FUTURE_OBSERVATION_MS = 1_000
HIP3_LIQUIDITY_RETRY_MS = 60_000
HIP3_IOC_NO_MATCH_ERROR = "Order could not immediately match against any resting orders."
HIP3_IOC_ZERO_FILL_PROOF_KIND = "hip3_ioc_zero_fill_v1"
HIP3_IOC_SYNC_NO_MATCH_EVIDENCE_SOURCE = "synchronous_ioc_no_match_rejection"
HIP3_IOC_UNKNOWN_OID_STATUS = "unknownOid"
HIP3_IOC_UNKNOWN_OID_CONFIRMATION_COUNT = 3
HIP3_IOC_ZERO_FILL_EXCHANGE_STATUS = "hip3_ioc_no_fill_deferred"
HIP3_IOC_ZERO_FILL_CLEANUP_STATUS = "hip3_ioc_no_fill_cleanup_retry"
HIP3_IOC_ZERO_FILL_NEUTRAL_STATUSES = frozenset(
    {
        HIP3_IOC_ZERO_FILL_EXCHANGE_STATUS,
        HIP3_IOC_ZERO_FILL_CLEANUP_STATUS,
        "settled:hip3_ioc_no_fill",
        "watchdog_settled:hip3_ioc_no_fill",
    }
)
HIP3_IOC_ZERO_FILL_CONFIRMATION_DELAYS_S = (0.0, 0.1, 0.25)
HIP3_PLANNING_LIQUIDITY_DEFERRAL_STAGES = frozenset(
    {
        "planning_admission",
        "planning_cap_reprice",
        "bounded_drain_cooldown",
    }
)
VALIDATION_SUPERVISOR_CONTAINMENT_DETAIL_PREFIX = "two-account supervisor containment: "
VALIDATION_SUPERVISOR_LEASE_CONTAINMENT_BLOCK = (
    "validation supervisor blocked new risk: lease status 'containment' does not permit new risk"
)


@dataclass(frozen=True)
class Hip3LiquidityDeferral:
    """A proven zero-exposure-change decision for a temporarily untradeable HIP-3 IOC."""

    intent: FollowerIntent
    blockers: tuple[str, ...]
    retry_not_before_ms: int
    stage: str


def _prefixed_backfill_warnings(prefix: str, report: FillBackfillReport) -> list[str]:
    return [f"{prefix}: {warning}" for warning in report.warnings]


def _backfill_report_int(report: dict[str, Any], key: str) -> int:
    try:
        return int(report.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def intent_outcome(
    intent_status: str,
    *,
    latest_report_status: str,
    latest_exchange_status: str,
    proven_liquidity_deferral: bool = False,
) -> str:
    if proven_liquidity_deferral:
        return "deferred"
    if latest_exchange_status.startswith("blocked:"):
        return "blocked"
    if latest_exchange_status == "skipped" or intent_status == IntentStatus.SKIPPED.value:
        return "skipped"
    if latest_report_status:
        return latest_report_status
    return intent_status


def _source_event_has_status(event: SourceEvent, expected: str) -> bool:
    expected = expected.strip().lower()
    return any(status == expected for status in _source_event_statuses(event))


def _source_event_statuses(event: SourceEvent) -> list[str]:
    raw_statuses = event.payload.get("statuses")
    if isinstance(raw_statuses, list):
        statuses = raw_statuses
    elif raw_statuses in (None, ""):
        subtype = str(event.payload.get("event_subtype") or "")
        _, _, raw_subtype_statuses = subtype.partition(":")
        statuses = raw_subtype_statuses.split(",") if raw_subtype_statuses else []
    else:
        statuses = [raw_statuses]
    return [str(status).strip().lower() for status in statuses if str(status).strip()]


def _source_event_has_rejected_order_status(event: SourceEvent) -> bool:
    return any(
        status == "rejected" or status.endswith("rejected")
        for status in _source_event_statuses(event)
    )


def _source_event_has_triggered_order_status(event: SourceEvent) -> bool:
    return any(status == "triggered" for status in _source_event_statuses(event))


def _source_event_has_terminal_twap_status(event: SourceEvent) -> bool:
    subtype = str(event.payload.get("event_subtype") or "").lower()
    if not subtype.startswith("twap_history"):
        return False
    return any(
        status in {"finished", "terminated", "error"} for status in _source_event_statuses(event)
    )


def _source_event_has_margin_transfer_ledger_type(event: SourceEvent) -> bool:
    raw_types = event.payload.get("ledger_types")
    if isinstance(raw_types, list):
        ledger_types = raw_types
    elif raw_types in (None, ""):
        subtype = str(event.payload.get("event_subtype") or "")
        _, _, raw_subtype_types = subtype.partition(":")
        ledger_types = raw_subtype_types.split(",") if raw_subtype_types else []
    else:
        ledger_types = [raw_types]
    return any(str(item).strip().lower() == "accountclasstransfer" for item in ledger_types)


class CopyTraderService:
    ORDINARY_FILL_SUBTYPES = ("fill", "fill_snapshot", "fill_backfill")
    TWAP_SLICE_FILL_SUBTYPES = (
        "twap_slice_fill",
        "twap_slice_fill_snapshot",
        "twap_slice_fill_backfill",
    )

    def __init__(
        self,
        config: AppConfig,
        *,
        store: SQLiteStore | None = None,
        info_client: InfoClient | None = None,
        execution_info_client: InfoClient | None = None,
        execution_adapter: ExecutionAdapter | None = None,
        paper_account: PaperAccount | None = None,
        execution_enabled: bool = True,
    ):
        self.config = config
        self.execution_enabled = execution_enabled
        self.instance_id = uuid4().hex
        self.store = store or SQLiteStore(config.db_path)
        self._journal_scope_error = ""
        if config.mode in {Mode.TESTNET, Mode.LIVE}:
            scope_ok, scope_detail = self.store.ensure_journal_scope(
                {
                    "source_wallet": config.source_wallet,
                    "source_network": config.resolved_source_network.value,
                    "action_account": (
                        config.exchange.vault_address or config.exchange.follower_account_address
                    ),
                    "execution_network": ("testnet" if config.mode == Mode.TESTNET else "mainnet"),
                }
            )
            if not scope_ok:
                self._journal_scope_error = scope_detail
        self.info_client = info_client or HyperliquidInfoClient(
            config.source_rest_url,
            timeout_s=float(config.ops.info_timeout_s),
        )
        self.execution_info_client = execution_info_client or (
            self.info_client
            if config.rest_url == config.source_rest_url
            else HyperliquidInfoClient(
                config.rest_url,
                timeout_s=float(config.ops.info_timeout_s),
            )
        )
        self.safe_mode = SafeModeController(self.store)
        self.shield = ConsistencyShield(self.safe_mode, rapid_flip_ms=config.risk.rapid_flip_ms)
        self._market_universe_lock = RLock()
        self._frozen_market_universe: FrozenMarketUniverseManifest | None = None
        self._frozen_market_contexts: FrozenMarketContextProvider | None = None
        self._validation_market_universe_error = ""
        self._validation_market_universe_last_check_ms = 0
        self._validation_market_universe_last_decision = RuntimeDecision(
            True, SafeModeReason.NONE, ""
        )
        self._validation_market_universe_last_observed: dict[str, Any] | None = None
        self._execution_mids_cache: dict[str, Decimal] = {}
        self._execution_mids_cache_ms = 0
        self._load_frozen_market_universe()
        self.observer = SourceObserver(
            source_wallet=config.source_wallet,
            info_client=self.info_client,
            store=self.store,
            ws_url=config.source_ws_url,
            shield=self.shield,
            active_asset_symbols=config.risk.allowed_symbols,
            websocket_idle_timeout_ms=config.ops.source_websocket_idle_timeout_ms,
            websocket_heartbeat_timeout_ms=config.ops.source_websocket_heartbeat_timeout_ms,
            rest_url=config.source_rest_url,
            info_timeout_s=float(config.ops.info_timeout_s),
            stale_after_ms=config.risk.stale_source_ms,
            reconnect_attempts=config.ops.source_websocket_reconnect_attempts,
            reconnect_backoff_ms=config.ops.source_websocket_reconnect_backoff_ms,
            source_dex_scope=config.source_dex_scope,
            market_mids_provider=(
                self.load_execution_mids
                if config.source_rest_url == config.rest_url
                and config.mode in {Mode.TESTNET, Mode.LIVE}
                else None
            ),
        )
        self.paper = paper_account or PaperAccount()
        self.exchange_rate_limiter = SlidingWindowRateLimiter(
            max_events=config.ops.max_exchange_actions_per_minute
        )
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=config.ops.circuit_breaker_failure_threshold,
            cooldown_ms=config.ops.circuit_breaker_cooldown_ms,
        )
        self._security_audit_lock = RLock()
        self._security_audit_cache: dict[str, Any] | None = None
        self._security_audit_cache_ms = 0
        self._runtime_file_lock_guard = RLock()
        self._runtime_file_locks: dict[str, tuple[AccountRuntimeFileLock, ...]] = {}
        self._runner_started_ms = now_ms()
        self._runner_cycle_count = 0
        self._runner_last_cycle_ms: int | None = None
        self._source_follow_startup_sync: dict[str, Any] = {
            "ready": False,
            "stage": "not_started",
            "detail": "source follower startup has not begun",
        }
        self._last_source_copy_signal_by_subtype: dict[str, str] = {}
        self._last_source_account_value: Decimal | None = None
        self._last_follower_account_value: Decimal | None = None
        self._active_plan_source_observed_ms = 0
        self._active_plan_follower_observed_ms = 0
        self._source_observation_context: tuple[int, float] | None = None
        self._follower_observation_context: tuple[int, float] | None = None
        self._active_dead_man_deadline_ms: int | None = None
        self._testnet_dead_man_degraded = False
        self._watchdog_dead_man_degraded = False
        self._watchdog_containment_active = False
        self._dead_man_eligibility_cache: dict[str, Any] | None = None
        self._active_dispatch_intent: FollowerIntent | None = None
        self._active_dispatch_asset_meta: AssetMeta | None = None
        self._active_dispatch_round_trip_quote: RoundTripQuote | None = None
        self._active_dispatch_liquidity_deferral: Hip3LiquidityDeferral | None = None
        self._active_dispatch_attempt_started_ms: int | None = None
        self._active_drain_liquidity_cooldown_coins: set[str] = set()
        self._last_sizing: dict[str, Any] = self._initial_sizing_status()
        if config.mode in {Mode.TESTNET, Mode.LIVE}:
            self._seed_circuit_breaker_from_journal()
        self.execution_adapter = execution_adapter if self.execution_enabled else None
        if (
            self.execution_enabled
            and self.execution_adapter is None
            and config.mode in {Mode.TESTNET, Mode.LIVE}
        ):
            self.execution_adapter = HyperliquidExecutionAdapter(
                config,
                pre_send_check=self._last_mile_pre_send_check,
                signed_action_guard=self._signed_action_guard,
            )
        elif self.execution_adapter is not None:
            set_pre_send_check = getattr(self.execution_adapter, "set_pre_send_check", None)
            if callable(set_pre_send_check):
                set_pre_send_check(self._last_mile_pre_send_check)
            set_signed_action_guard = getattr(
                self.execution_adapter, "set_signed_action_guard", None
            )
            if callable(set_signed_action_guard):
                set_signed_action_guard(self._signed_action_guard)

    @staticmethod
    def _is_testnet_dead_man_volume_rejection(detail: str) -> bool:
        return "cannot set scheduled cancel time until enough volume traded" in str(detail).lower()

    def _seed_circuit_breaker_from_journal(self) -> None:
        stats = self.store.consecutive_exchange_failure_stats()
        failures = stats["consecutive_failures"]
        if failures <= 0:
            return
        if failures >= self.config.ops.circuit_breaker_failure_threshold:
            latest = stats["latest_failure_ms"] or now_ms()
            if now_ms() - latest >= self.config.ops.circuit_breaker_cooldown_ms:
                return
            self.circuit_breaker.consecutive_failures = failures
            self.circuit_breaker.opened_ms = latest
            return
        self.circuit_breaker.consecutive_failures = failures

    def preflight(self, *, auth_probe: bool = True) -> PreflightReport:
        client = self.execution_adapter if self.config.mode in {Mode.TESTNET, Mode.LIVE} else None
        report = build_preflight_report(self.config, client=client)
        if self._validation_market_universe_error:
            report = self._preflight_with_blocker(
                report,
                "validation market universe is invalid: " + self._validation_market_universe_error,
            )
        elif (
            self.config.ops.validation_market_universe_manifest_path is not None
            and self._frozen_market_universe is None
        ):
            report = self._preflight_with_blocker(
                report,
                "validation market universe is configured but not loaded",
            )
        if self._journal_scope_error:
            report = self._preflight_with_blocker(
                report,
                f"journal scope mismatch: {self._journal_scope_error}",
            )
        if self.config.mode in {Mode.TESTNET, Mode.LIVE}:
            unresolved_signed_actions = self.store.unresolved_signed_action_attempt_count(
                self.config.mode,
                account=self._effective_action_account(),
                network=("mainnet" if self.config.mode == Mode.LIVE else "testnet"),
            )
            if unresolved_signed_actions:
                report = self._preflight_with_blocker(
                    report,
                    f"{unresolved_signed_actions} unresolved non-order signed actions require "
                    "explicit operator review before authentication or new risk",
                )
            unresolved_plans = self.store.unresolved_desired_state_count(
                mode=self.config.mode,
                source_wallet=self.config.source_wallet,
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            if unresolved_plans:
                report = self._preflight_with_blocker(
                    report,
                    f"{unresolved_plans} unresolved desired execution plans require audited "
                    "follower reconciliation before new risk",
                )
        if report.passed and auth_probe:
            report = self._preflight_auth_probe(report)
        if not report.passed:
            if self.safe_mode.enabled and self.safe_mode.reason in {
                SafeModeReason.CONFIG_INVALID,
                SafeModeReason.CONCURRENT_INSTANCE,
                SafeModeReason.OPERATOR_KILL_SWITCH,
                SafeModeReason.RESTART_MID_FILL,
            }:
                return report
            reason = (
                SafeModeReason.LIVE_BLOCKED
                if self.config.mode == Mode.LIVE
                else SafeModeReason.PREFLIGHT_FAILED
            )
            if self.config.mode == Mode.TESTNET:
                reason = SafeModeReason.TESTNET_BLOCKED
            self.safe_mode.trip(reason, "; ".join(report.blockers))
        return report

    def _preflight_auth_probe(self, report: PreflightReport) -> PreflightReport:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return report
        if self.execution_adapter is None:
            return self._preflight_with_blocker(
                report, "exchange auth probe requires execution adapter"
            )
        if self._recent_auth_probe_ok():
            return report
        if not self._acquire_exchange_lease("auth_probe"):
            return self._preflight_with_blocker(
                report,
                f"exchange auth probe could not acquire runtime lease: {self.safe_mode.detail}",
            )
        try:
            intent_id = self._auth_probe_intent_id()
            cloid = deterministic_cloid("auth-probe", intent_id, now_ms())
            started = monotonic()
            try:
                probe = self.execution_adapter.auth_probe(intent_id=intent_id, cloid=cloid)
            except Exception as exc:
                elapsed_s = Decimal(str(round(monotonic() - started, 6)))
                probe = self._exception_exchange_report(
                    intent_id=intent_id,
                    cloid=cloid,
                    detail=f"auth_probe raised: {exc}",
                    elapsed_s=elapsed_s,
                )
            elapsed_s = Decimal(str(round(monotonic() - started, 6)))
            payload = dict(probe.payload)
            payload["elapsed_s"] = elapsed_s
            probe = replace(probe, payload=payload)
            if elapsed_s > self.config.ops.exchange_action_timeout_s:
                probe = replace(
                    probe,
                    status=IntentStatus.REJECTED,
                    exchange_status="auth_probe_timeout",
                    payload={
                        **payload,
                        "original_status": probe.status.value,
                        "original_exchange_status": probe.exchange_status,
                    },
                )
                self.store.append_execution_report(probe)
                return self._preflight_with_blocker(
                    report,
                    (
                        f"exchange auth probe took {elapsed_s}s > "
                        f"{self.config.ops.exchange_action_timeout_s}s"
                    ),
                )
            self.store.append_execution_report(probe)
            if probe.status == IntentStatus.ACKED and probe.exchange_status == "auth_probe_ok":
                return report
            detail = f"exchange auth probe failed: {probe.exchange_status}"
            if isinstance(probe.payload, dict):
                error = probe.payload.get("error") or probe.payload.get("detail")
                if error:
                    detail = f"{detail}: {error}"
            return self._preflight_with_blocker(report, detail)
        finally:
            self._release_exchange_lease("auth_probe")

    def _recent_auth_probe_ok(self) -> bool:
        since_ms = now_ms() - self.config.ops.auth_probe_interval_ms
        return (
            self.store.latest_successful_auth_probe(
                intent_id=self._auth_probe_intent_id(),
                since_ms=since_ms,
            )
            is not None
        )

    def _auth_probe_intent_id(self) -> str:
        network = "mainnet" if self.config.mode == Mode.LIVE else "testnet"
        follower = self.config.exchange.follower_account_address.strip().lower()
        action = (
            (self.config.exchange.vault_address or self.config.exchange.follower_account_address)
            .strip()
            .lower()
        )
        signer = self._runtime_signer_address()
        return f"auth-probe:v2:{network}:{follower}:{action}:{signer}"

    @staticmethod
    def _preflight_with_blocker(report: PreflightReport, blocker: str) -> PreflightReport:
        return replace(report, passed=False, blockers=[*report.blockers, blocker])

    def _load_frozen_market_universe(self) -> None:
        ops = self.config.ops
        path = ops.validation_market_universe_manifest_path
        expected_sha = ops.validation_market_universe_sha256
        if path is None and not expected_sha:
            return
        try:
            if path is None or not expected_sha:
                raise MarketIdentityError("manifest path and SHA-256 must be configured together")
            stat = path.stat()
            if stat.st_size <= 0 or stat.st_size > 2_000_000:
                raise MarketIdentityError("persisted market universe file size is invalid")
            payload = json.loads(path.read_text(encoding="utf-8"))
            manifest = FrozenMarketUniverseManifest.from_payload(payload)
            if manifest.sha256 != expected_sha:
                raise MarketIdentityError(
                    "persisted market universe SHA-256 does not match runtime configuration"
                )
            expected_network = "mainnet" if self.config.mode == Mode.LIVE else "testnet"
            if manifest.network != expected_network:
                raise MarketIdentityError(
                    f"persisted market universe network {manifest.network} does not match "
                    f"execution network {expected_network}"
                )
            configured = tuple(
                canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols
            )
            if len(configured) != len(set(configured)) or set(configured) != set(manifest.symbols):
                raise MarketIdentityError(
                    "runtime allowed symbols must equal the complete frozen market universe"
                )
        except (
            OSError,
            UnicodeError,
            json.JSONDecodeError,
            MarketIdentityError,
            ValueError,
        ) as exc:
            self._validation_market_universe_error = str(exc)
            self.safe_mode.trip(
                SafeModeReason.CONFIG_INVALID,
                f"validation market universe is invalid: {exc}",
            )
            return

        self._frozen_market_universe = manifest
        self._frozen_market_contexts = FrozenMarketContextProvider(manifest)
        self._validation_market_universe_last_check_ms = now_ms()
        self._validation_market_universe_last_observed = {
            "source": "persisted_launch_manifest",
            "manifest": manifest.to_payload(),
            "changed": False,
        }

    def _validation_market_universe_decision(self, *, force: bool = False) -> RuntimeDecision:
        ops = self.config.ops
        if (
            ops.validation_market_universe_manifest_path is None
            and not ops.validation_market_universe_sha256
        ):
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        if self._validation_market_universe_error:
            return RuntimeDecision(
                False,
                SafeModeReason.CONFIG_INVALID,
                "validation market universe is invalid: " + self._validation_market_universe_error,
            )
        launch = self._frozen_market_universe
        if launch is None:
            return RuntimeDecision(
                False,
                SafeModeReason.CONFIG_INVALID,
                "validation market universe was not loaded",
            )
        observed_ms = now_ms()
        with self._market_universe_lock:
            age_ms = observed_ms - self._validation_market_universe_last_check_ms
            if (
                not force
                and self._validation_market_universe_last_check_ms > 0
                and 0 <= age_ms < ops.validation_market_universe_refresh_ms
            ):
                return self._validation_market_universe_last_decision
            try:
                current = resolve_public_market_universe(
                    self.execution_info_client,
                    network=launch.network,
                    observed_ms=observed_ms,
                )
                drift = compare_market_universes(launch, current)
            except (OSError, RuntimeError, MarketIdentityError, ValueError) as exc:
                decision = self._blocked_validation_market_universe(
                    f"unsigned catalog refresh failed: {exc}"
                )
                status: dict[str, Any] = {
                    "source": "unsigned_refresh",
                    "checked_ms": observed_ms,
                    "changed": None,
                    "error": str(exc),
                }
            else:
                blocking_changed = bool(
                    drift.expected_network != drift.observed_network
                    or drift.removed_dexes
                    or drift.removed_symbols
                    or drift.precision_changes
                )
                status = {
                    "source": "unsigned_refresh",
                    "checked_ms": observed_ms,
                    "observed_manifest": current.to_payload(),
                    "drift": drift.to_payload(),
                    "changed": drift.changed,
                    "blocking_changed": blocking_changed,
                }
                decision = (
                    self._blocked_validation_market_universe(
                        "frozen catalog drift detected: "
                        + json.dumps(drift.to_payload(), sort_keys=True, separators=(",", ":"))
                    )
                    if blocking_changed
                    else RuntimeDecision(True, SafeModeReason.NONE, "")
                )
            self._validation_market_universe_last_check_ms = observed_ms
            self._validation_market_universe_last_decision = decision
            self._validation_market_universe_last_observed = status
            return decision

    @staticmethod
    def _blocked_validation_market_universe(detail: str) -> RuntimeDecision:
        # RISK_LIMIT intentionally permits reduce-only/cancel containment while blocking every
        # exposure increase.  Catalog failure is not a reason to strand an existing position.
        return RuntimeDecision(
            False,
            SafeModeReason.RISK_LIMIT,
            f"validation frozen market universe blocked new risk: {detail}",
        )

    def validation_market_universe_status(self) -> dict[str, Any]:
        manifest = self._frozen_market_universe
        decision = self._validation_market_universe_last_decision
        return {
            "configured": self.config.ops.validation_market_universe_manifest_path is not None,
            "ready": bool(manifest) and not self._validation_market_universe_error,
            "manifest_sha256": manifest.sha256 if manifest is not None else "",
            "market_count": len(manifest.symbols) if manifest is not None else 0,
            "dex_count": len(manifest.dexes) if manifest is not None else 0,
            "last_check_ms": self._validation_market_universe_last_check_ms,
            "decision": {
                "ok": decision.ok,
                "reason": decision.reason.value,
                "detail": decision.detail,
            },
            "last_observed": self._validation_market_universe_last_observed,
            "error": self._validation_market_universe_error,
            "last_good_contexts": (
                self._frozen_market_contexts.to_payload()
                if self._frozen_market_contexts is not None
                else None
            ),
        }

    def load_asset_meta(self) -> dict[str, AssetMeta]:
        if self._frozen_market_universe is not None:
            return {
                market.symbol: AssetMeta(
                    coin=market.symbol,
                    sz_decimals=market.sz_decimals,
                    # The two-account validation is pinned to 1x. Frozen sizing precision is
                    # authoritative; current exchange leverage is still revalidated by the
                    # signed adapter and account preflight.
                    max_leverage=None,
                )
                for market in self._frozen_market_universe.markets
            }
        result: dict[str, AssetMeta] = {}
        allowed = {canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols}
        for dex in self._configured_execution_dexes():
            request: dict[str, Any] = {"type": "meta"}
            if dex:
                request["dex"] = dex
            payload = self.execution_info_client.info(request)
            universe = payload.get("universe", []) if isinstance(payload, dict) else []
            for item in universe:
                if not isinstance(item, dict) or item.get("isDelisted") is True:
                    continue
                raw_name = item.get("name", "")
                if not str(raw_name).strip():
                    continue
                candidate = str(raw_name).strip()
                if dex and ":" not in candidate:
                    candidate = f"{dex}:{candidate}"
                try:
                    candidate = canonical_market_symbol(candidate)
                except ValueError:
                    continue
                if candidate not in allowed:
                    continue
                coin = qualify_market_symbol(dex, raw_name)
                result[coin] = AssetMeta(
                    coin=coin,
                    sz_decimals=int(item.get("szDecimals", 0)),
                    max_leverage=(int(item["maxLeverage"]) if item.get("maxLeverage") else None),
                )
        return result

    def load_execution_mids(self) -> dict[str, Decimal]:
        observed_ms = now_ms()
        cache_age_ms = observed_ms - self._execution_mids_cache_ms
        cache_ttl_ms = min(max(self.config.risk.stale_source_ms // 2, 250), 2_000)
        if self._execution_mids_cache and 0 <= cache_age_ms < cache_ttl_ms:
            return dict(self._execution_mids_cache)
        mids: dict[str, Decimal] = {}
        allowed = {canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols}
        for dex in self._configured_execution_dexes():
            if dex:
                payload = self.execution_info_client.info({"type": "metaAndAssetCtxs", "dex": dex})
                if not (
                    isinstance(payload, list)
                    and len(payload) >= 2
                    and isinstance(payload[0], dict)
                    and isinstance(payload[1], list)
                ):
                    if self._frozen_market_universe is not None:
                        raise ValueError(
                            f"metaAndAssetCtxs response for DEX {dex} must contain "
                            "metadata and contexts"
                        )
                    continue
                universe = payload[0].get("universe")
                contexts = payload[1]
                if not isinstance(universe, list):
                    if self._frozen_market_universe is not None:
                        raise ValueError(f"metaAndAssetCtxs response for DEX {dex} has no universe")
                    continue
                for index, item in enumerate(universe):
                    if (
                        not isinstance(item, dict)
                        or item.get("isDelisted") is True
                        or index >= len(contexts)
                        or not isinstance(contexts[index], dict)
                    ):
                        continue
                    raw_name = item.get("name", "")
                    candidate = str(raw_name).strip()
                    if not candidate:
                        continue
                    if ":" not in candidate:
                        candidate = f"{dex}:{candidate}"
                    try:
                        candidate = canonical_market_symbol(candidate)
                    except ValueError:
                        continue
                    if candidate not in allowed:
                        continue
                    context = contexts[index]
                    oracle_px = parse_decimal(context.get("oraclePx"))
                    if oracle_px is None or not oracle_px.is_finite() or oracle_px <= 0:
                        continue
                    symbol = qualify_market_symbol(dex, raw_name)
                    mids[symbol] = oracle_px
                    mark_px = parse_decimal(context.get("markPx"))
                    mid_px = parse_decimal(context.get("midPx"))
                    self._observe_frozen_market_context(
                        symbol,
                        observed_ms=observed_ms,
                        mark_px=(
                            mark_px
                            if mark_px is not None and mark_px.is_finite() and mark_px > 0
                            else None
                        ),
                        mid_px=(
                            mid_px
                            if mid_px is not None and mid_px.is_finite() and mid_px > 0
                            else None
                        ),
                    )
                continue
            request: dict[str, Any] = {"type": "allMids"}
            payload = self.execution_info_client.info(request)
            if not isinstance(payload, dict):
                if self._frozen_market_universe is not None:
                    raise ValueError(
                        f"allMids response for DEX {dex or '<default>'} must be an object"
                    )
                continue
            for coin, value in payload.items():
                candidate = str(coin).strip()
                if dex and ":" not in candidate:
                    candidate = f"{dex}:{candidate}"
                try:
                    candidate = canonical_market_symbol(candidate)
                except ValueError:
                    continue
                if candidate not in allowed:
                    continue
                price = parse_decimal(value)
                if price is not None and price > 0:
                    symbol = qualify_market_symbol(dex, coin)
                    mids[symbol] = price
                    self._observe_frozen_market_context(
                        symbol,
                        observed_ms=observed_ms,
                        mid_px=price,
                    )
        if self._frozen_market_universe is not None and self._frozen_market_contexts is not None:
            for symbol in self._frozen_market_universe.symbols:
                if symbol in mids:
                    continue
                try:
                    reduction = self._frozen_market_contexts.reduction_context(
                        symbol,
                        now_ms=observed_ms,
                        max_age_ms=max(self.config.risk.stale_source_ms, 300_000),
                    )
                except MarketIdentityError:
                    continue
                mids[symbol] = reduction.reference_px
        self._execution_mids_cache = dict(mids)
        self._execution_mids_cache_ms = observed_ms
        return mids

    def _observe_frozen_market_context(
        self,
        symbol: str,
        *,
        observed_ms: int,
        mark_px: Decimal | None = None,
        mid_px: Decimal | None = None,
    ) -> None:
        provider = self._frozen_market_contexts
        manifest = self._frozen_market_universe
        if provider is None or manifest is None or symbol not in set(manifest.symbols):
            return
        previous = provider.context(symbol)
        latest_ms = max(
            previous.mark_observed_ms if previous and previous.mark_observed_ms else 0,
            previous.mid_observed_ms if previous and previous.mid_observed_ms else 0,
        )
        provider.observe(
            symbol,
            observed_ms=max(observed_ms, latest_ms + 1),
            mark_px=mark_px,
            mid_px=mid_px,
        )

    def _configured_execution_dexes(self) -> tuple[str, ...]:
        if self._frozen_market_universe is not None:
            return self._frozen_market_universe.dexes
        dexes = {market_dex(symbol) for symbol in self.config.risk.allowed_symbols}
        dexes.add("")
        return tuple(sorted(dexes, key=lambda item: (item != "", item)))

    def load_market_liquidity_snapshot(self, coin: str) -> MarketLiquiditySnapshot:
        market = canonical_market_symbol(coin)
        dex = market_dex(market)
        meta_request: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            meta_request["dex"] = dex
        meta_and_contexts = self.execution_info_client.info(meta_request)
        l2_book = self.execution_info_client.info({"type": "l2Book", "coin": market})
        snapshot = parse_market_liquidity_snapshot(
            market,
            meta_and_contexts=meta_and_contexts,
            l2_book=l2_book,
            observed_ms=now_ms(),
        )
        if snapshot.mark_px is not None or snapshot.mid_px is not None:
            self._observe_frozen_market_context(
                market,
                observed_ms=snapshot.observed_ms,
                mark_px=snapshot.mark_px,
                mid_px=snapshot.mid_px,
            )
        return snapshot

    def load_hip3_round_trip_quote(
        self,
        coin: str,
        *,
        opening_side: str,
        requested_size: Decimal,
        asset_meta: AssetMeta,
    ) -> tuple[RoundTripQuote | None, list[str]]:
        assessment = self.load_hip3_round_trip_assessment(
            coin,
            opening_side=opening_side,
            requested_size=requested_size,
            asset_meta=asset_meta,
        )
        return assessment.quote, list(assessment.blockers)

    def load_hip3_round_trip_assessment(
        self,
        coin: str,
        *,
        opening_side: str,
        requested_size: Decimal,
        asset_meta: AssetMeta,
    ) -> RoundTripAssessment:
        market = canonical_market_symbol(coin)
        if not market_dex(market):
            raise ValueError(f"{market} is not a HIP-3 market")
        snapshot = self.load_market_liquidity_snapshot(market)
        return assess_round_trip_quote(
            snapshot,
            opening_side=opening_side,
            requested_size=requested_size,
            oracle_envelope_bps=self.config.risk.hip3_oracle_envelope_bps,
            max_age_ms=self.config.risk.stale_source_ms,
            sz_decimals=asset_meta.sz_decimals,
            current_ms=now_ms(),
        )

    def _admit_hip3_open_intents(
        self,
        intents: list[FollowerIntent],
        *,
        asset_meta: dict[str, AssetMeta],
    ) -> tuple[list[FollowerIntent], list[Hip3LiquidityDeferral], list[str]]:
        admitted: list[FollowerIntent] = []
        liquidity_deferred: list[Hip3LiquidityDeferral] = []
        blockers: list[str] = []
        for intent in intents:
            coin = canonical_market_symbol(intent.coin)
            if intent.action != IntentAction.OPEN or intent.reduce_only or not market_dex(coin):
                admitted.append(intent)
                continue
            if coin in self._active_drain_liquidity_cooldown_coins:
                liquidity_deferred.append(
                    self._hip3_liquidity_deferral(
                        intent,
                        [
                            f"{coin} is deferred for the remainder of this bounded drain after "
                            "a same-cycle liquidity shortfall"
                        ],
                        stage="bounded_drain_cooldown",
                    )
                )
                continue
            meta = asset_meta.get(coin)
            if meta is None:
                blockers.append(f"{coin} missing metadata for HIP-3 round-trip admission")
                admitted.append(intent)
                continue
            try:
                assessment = self.load_hip3_round_trip_assessment(
                    coin,
                    opening_side=intent.side,
                    requested_size=intent.size,
                    asset_meta=meta,
                )
            except Exception as exc:
                blockers.append(f"{coin} HIP-3 round-trip admission failed: {exc}")
                admitted.append(intent)
                continue
            if assessment.quote is None and assessment.retryable_liquidity:
                liquidity_deferred.append(
                    self._hip3_liquidity_deferral(
                        intent,
                        assessment.blockers,
                        stage="planning_admission",
                    )
                )
                continue
            if assessment.quote is None:
                blockers.extend(assessment.blockers)
                admitted.append(intent)
                continue
            quote = assessment.quote
            quoted_notional = self._hip3_open_notional_bound(intent, quote)
            if quoted_notional > self.config.risk.max_notional_usd:
                liquidity_deferred.append(
                    self._hip3_liquidity_deferral(
                        intent,
                        [
                            f"{coin} fresh oracle-envelope IOC notional {quoted_notional} "
                            f"exceeds per-order cap {self.config.risk.max_notional_usd}; "
                            "current truth must be replanned at a cap-safe size"
                        ],
                        stage="planning_cap_reprice",
                    )
                )
                continue
            admitted.append(
                replace(
                    intent,
                    price=quote.entry_limit,
                    execution_proof=quote.to_payload(),
                )
            )
        return admitted, liquidity_deferred, blockers

    @staticmethod
    def _hip3_open_notional_bound(
        intent: FollowerIntent,
        quote: RoundTripQuote,
    ) -> Decimal:
        """Conservatively bound current visible HIP-3 opening fill notional."""

        return abs(intent.size) * quote.entry_notional_bound_px

    @staticmethod
    def _hip3_liquidity_deferral(
        intent: FollowerIntent,
        blockers: tuple[str, ...] | list[str],
        *,
        stage: str,
    ) -> Hip3LiquidityDeferral:
        return Hip3LiquidityDeferral(
            intent=intent,
            blockers=tuple(str(item) for item in blockers if str(item)),
            retry_not_before_ms=now_ms() + HIP3_LIQUIDITY_RETRY_MS,
            stage=stage,
        )

    def _bind_hip3_ioc_retry_identities(
        self,
        intents: list[FollowerIntent],
    ) -> list[FollowerIntent]:
        """Mint a fresh deterministic CLOID after a signed, proven zero-fill IOC."""

        bound: list[FollowerIntent] = []
        for intent in intents:
            if not self._is_retryable_hip3_ioc_intent(intent):
                bound.append(intent)
                continue
            base_cloid = intent.cloid.lower()
            predecessor = self.store.latest_hip3_ioc_zero_fill_proof(base_cloid)
            attempt_cloid = (
                deterministic_cloid(
                    "hip3-ioc-zero-fill-retry",
                    base_cloid,
                    predecessor,
                )
                if predecessor
                else base_cloid
            )
            identity = {
                "base_cloid": base_cloid,
                "attempt_cloid": attempt_cloid,
                "predecessor_zero_fill_proof_id": predecessor,
            }
            bound.append(
                replace(
                    intent,
                    cloid=attempt_cloid,
                    intent_id=(
                        deterministic_cloid("intent", attempt_cloid)
                        if attempt_cloid != base_cloid
                        else intent.intent_id
                    ),
                    execution_proof={
                        **dict(intent.execution_proof),
                        "post_send_retry_identity": identity,
                    },
                )
            )
        return bound

    @staticmethod
    def _is_retryable_hip3_ioc_intent(intent: FollowerIntent) -> bool:
        return bool(
            market_dex(intent.coin)
            and intent.size > 0
            and intent.price is not None
            and (
                (intent.action == IntentAction.OPEN and not intent.reduce_only)
                or (
                    intent.action in {IntentAction.REDUCE, IntentAction.CLOSE}
                    and intent.reduce_only
                )
            )
        )

    @staticmethod
    def _hip3_ioc_retry_identity(intent: FollowerIntent) -> dict[str, Any]:
        raw = intent.execution_proof.get("post_send_retry_identity")
        if isinstance(raw, dict):
            base_cloid = str(raw.get("base_cloid") or "").strip().lower()
            attempt_cloid = str(raw.get("attempt_cloid") or "").strip().lower()
            if base_cloid and attempt_cloid == intent.cloid.lower():
                return dict(raw)
        return {
            "base_cloid": intent.cloid.lower(),
            "attempt_cloid": intent.cloid.lower(),
            "predecessor_zero_fill_proof_id": None,
        }

    @staticmethod
    def _hip3_ioc_no_match_rejection_matches(
        intent: FollowerIntent,
        report: ExecutionReport,
    ) -> bool:
        if (
            report.status != IntentStatus.REJECTED
            or report.cloid.lower() != intent.cloid.lower()
            or not CopyTraderService._is_retryable_hip3_ioc_intent(intent)
        ):
            return False
        payload = report.payload
        if not isinstance(payload, dict):
            return False
        request = payload.get("order_request")
        response = payload.get("response")
        if not isinstance(request, dict) or not isinstance(response, dict):
            return False
        try:
            request_coin = canonical_market_symbol(str(request.get("coin") or ""))
            request_size = parse_decimal(request.get("size"))
            request_price = parse_decimal(request.get("price"))
            expected_size = parse_decimal(payload.get("expected_size"))
        except (ArithmeticError, MarketIdentityError, TypeError, ValueError):
            return False
        if (
            request_coin != canonical_market_symbol(intent.coin)
            or str(request.get("side") or "").lower() != intent.side.lower()
            or request_size != intent.size
            or expected_size != intent.size
            or request_price != intent.price
            or request.get("reduce_only") is not intent.reduce_only
            or str(request.get("tif") or "").lower() != "ioc"
            or str(response.get("status") or "").lower() != "ok"
        ):
            return False
        inner = response.get("response")
        if not isinstance(inner, dict) or str(inner.get("type") or "").lower() != "order":
            return False
        data = inner.get("data")
        statuses = data.get("statuses") if isinstance(data, dict) else None
        if not isinstance(statuses, list) or len(statuses) != 1:
            return False
        status = statuses[0]
        if not isinstance(status, dict) or set(status) != {"error"}:
            return False
        error = status.get("error")
        if not isinstance(error, str):
            return False
        if error == HIP3_IOC_NO_MATCH_ERROR:
            return True
        prefix = HIP3_IOC_NO_MATCH_ERROR + " asset="
        return error.startswith(prefix) and error[len(prefix) :].isdecimal()

    @staticmethod
    def _hip3_ioc_zero_fill_order_status_proof(
        intent: FollowerIntent,
        status_payload: Any,
        *,
        attempt_not_before_ms: int | None = None,
        proof_observed_ms: int | None = None,
    ) -> dict[str, Any] | None:
        if (
            not CopyTraderService._is_retryable_hip3_ioc_intent(intent)
            or not isinstance(status_payload, dict)
            or str(status_payload.get("status") or "").lower() != "order"
        ):
            return None
        wrapper = status_payload.get("order")
        if (
            not isinstance(wrapper, dict)
            or str(wrapper.get("status") or "").lower() != "ioccancelrejected"
        ):
            return None
        order = wrapper.get("order")
        if not isinstance(order, dict) or order.get("children") != []:
            return None
        try:
            coin = canonical_market_symbol(str(order.get("coin") or ""))
            original_size = parse_decimal(order.get("origSz"))
            remaining_size = parse_decimal(order.get("sz"))
            limit_price = parse_decimal(order.get("limitPx"))
            raw_oid = order.get("oid")
            raw_order_timestamp = order.get("timestamp")
            raw_status_timestamp = wrapper.get("statusTimestamp")
            if raw_oid is None or raw_order_timestamp is None or raw_status_timestamp is None:
                return None
            oid = int(str(raw_oid))
            order_timestamp = int(str(raw_order_timestamp))
            status_timestamp = int(str(raw_status_timestamp))
        except (ArithmeticError, MarketIdentityError, TypeError, ValueError):
            return None
        expected_side = "B" if intent.side.lower() == "buy" else "A"
        if (
            coin != canonical_market_symbol(intent.coin)
            or str(order.get("cloid") or "").lower() != intent.cloid.lower()
            or str(order.get("side") or "").upper() != expected_side
            or str(order.get("tif") or "").lower() != "ioc"
            or str(order.get("orderType") or "").lower() != "limit"
            or order.get("reduceOnly") is not intent.reduce_only
            or original_size != intent.size
            or remaining_size != original_size
            or limit_price != intent.price
            or oid <= 0
            or order_timestamp <= 0
            or status_timestamp < order_timestamp
            or (
                attempt_not_before_ms is not None
                and order_timestamp < attempt_not_before_ms - MAX_FUTURE_OBSERVATION_MS
            )
            or status_timestamp
            > (proof_observed_ms if proof_observed_ms is not None else now_ms())
            + MAX_FUTURE_OBSERVATION_MS
        ):
            return None
        proof_id = deterministic_cloid(
            "hip3-ioc-zero-fill-proof",
            intent.cloid.lower(),
            oid,
            status_timestamp,
        )
        return {
            "kind": HIP3_IOC_ZERO_FILL_PROOF_KIND,
            "proof_id": proof_id,
            "cloid": intent.cloid.lower(),
            "coin": coin,
            "side": intent.side.lower(),
            "size": intent.size,
            "price": intent.price,
            "reduce_only": intent.reduce_only,
            "oid": oid,
            "order_timestamp": order_timestamp,
            "status_timestamp": status_timestamp,
            "order_status": status_payload,
        }

    def _confirm_hip3_ioc_zero_fill(
        self,
        intent: FollowerIntent,
        *,
        attempt_not_before_ms: int,
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        attempts: list[dict[str, Any]] = []
        for row in self.store.execution_reports_for_cloid(intent.cloid):
            stored = _json_object(row.get("payload_json"))
            payload = stored.get("payload")
            status_payload = payload.get("order_status") if isinstance(payload, dict) else None
            if status_payload is None and isinstance(payload, dict):
                stored_proof = payload.get("zero_fill_proof")
                if isinstance(stored_proof, dict):
                    status_payload = stored_proof.get("order_status")
            if status_payload is None:
                continue
            proof = self._hip3_ioc_zero_fill_order_status_proof(
                intent,
                status_payload,
                attempt_not_before_ms=attempt_not_before_ms,
            )
            attempts.append(
                {
                    "source": "durable_execution_report",
                    "report_id": str(row.get("report_id") or ""),
                    "matched": proof is not None,
                }
            )
            if proof is not None:
                return proof, attempts
        if self.execution_adapter is None:
            attempts.append({"source": "order_status", "error": "execution adapter unavailable"})
            return None, attempts
        for delay_s in HIP3_IOC_ZERO_FILL_CONFIRMATION_DELAYS_S:
            if delay_s > 0:
                sleep(delay_s)
            try:
                status_payload = self.execution_adapter.order_status(intent.cloid)
            except Exception as exc:
                attempts.append(
                    {
                        "source": "order_status",
                        "queried_cloid": intent.cloid.lower(),
                        "observed_ms": now_ms(),
                        "error": str(exc),
                    }
                )
                continue
            status_observed_ms = now_ms()
            proof = self._hip3_ioc_zero_fill_order_status_proof(
                intent,
                status_payload,
                attempt_not_before_ms=attempt_not_before_ms,
                proof_observed_ms=status_observed_ms,
            )
            attempts.append(
                {
                    "source": "order_status",
                    "queried_cloid": intent.cloid.lower(),
                    "observed_ms": status_observed_ms,
                    "payload": status_payload,
                    "matched": proof is not None,
                }
            )
            if proof is not None:
                return proof, attempts
            classified, _ = classify_order_status(status_payload)
            if classified is not None:
                break
        return None, attempts

    @staticmethod
    def _hip3_ioc_synchronous_no_match_proof(
        intent: FollowerIntent,
        report: ExecutionReport,
        attempts: list[dict[str, Any]],
        *,
        attempt_not_before_ms: int,
        stage: str,
    ) -> dict[str, Any] | None:
        """Prove a rejected IOC never became an order when every lookup says unknownOid."""

        if (
            not CopyTraderService._hip3_ioc_no_match_rejection_matches(intent, report)
            or len(attempts) != HIP3_IOC_UNKNOWN_OID_CONFIRMATION_COUNT
        ):
            return None
        confirmations: list[dict[str, Any]] = []
        previous_observed_ms = 0
        for attempt in attempts:
            payload = attempt.get("payload")
            observed_ms = attempt.get("observed_ms")
            if (
                attempt.get("source") != "order_status"
                or attempt.get("queried_cloid") != intent.cloid.lower()
                or attempt.get("matched") is not False
                or type(observed_ms) is not int
                or observed_ms < attempt_not_before_ms - MAX_FUTURE_OBSERVATION_MS
                or observed_ms < previous_observed_ms
                or not isinstance(payload, dict)
                or set(payload) != {"status"}
                or payload.get("status") != HIP3_IOC_UNKNOWN_OID_STATUS
            ):
                return None
            confirmations.append(
                {
                    "queried_cloid": intent.cloid.lower(),
                    "observed_ms": observed_ms,
                    "payload": dict(payload),
                }
            )
            previous_observed_ms = observed_ms
        try:
            response_observed_ms = int(report.exchange_ts_ms)
        except (TypeError, ValueError):
            return None
        proof_observed_ms = confirmations[-1]["observed_ms"]
        if (
            attempt_not_before_ms <= 0
            or response_observed_ms <= 0
            or response_observed_ms < attempt_not_before_ms - MAX_FUTURE_OBSERVATION_MS
            or proof_observed_ms < response_observed_ms - MAX_FUTURE_OBSERVATION_MS
        ):
            return None
        request = to_jsonable(dict(report.payload["order_request"]))
        response = to_jsonable(dict(report.payload["response"]))
        confirmations = to_jsonable(confirmations)
        if (
            not isinstance(request, dict)
            or not isinstance(response, dict)
            or not isinstance(confirmations, list)
        ):
            return None
        proof_id = deterministic_cloid(
            "hip3-ioc-sync-no-match-proof-v1",
            report.report_id,
            intent.cloid.lower(),
            attempt_not_before_ms,
            response_observed_ms,
            stage,
            request,
            response,
            confirmations,
        )
        return {
            "kind": HIP3_IOC_ZERO_FILL_PROOF_KIND,
            "evidence_source": HIP3_IOC_SYNC_NO_MATCH_EVIDENCE_SOURCE,
            "proof_id": proof_id,
            "source_report_id": report.report_id,
            "stage": stage,
            "cloid": intent.cloid.lower(),
            "coin": canonical_market_symbol(intent.coin),
            "side": intent.side.lower(),
            "size": intent.size,
            "price": intent.price,
            "reduce_only": intent.reduce_only,
            "attempt_not_before_ms": attempt_not_before_ms,
            "response_observed_ms": response_observed_ms,
            "proof_observed_ms": proof_observed_ms,
            "order_request": request,
            "response": response,
            "unknown_oid_confirmations": confirmations,
        }

    def _normalize_proven_hip3_ioc_zero_fill(
        self,
        intent: FollowerIntent,
        report: ExecutionReport,
        *,
        stage: str,
        paced_retry: bool,
    ) -> tuple[ExecutionReport, Hip3LiquidityDeferral | None]:
        if not self._hip3_ioc_no_match_rejection_matches(intent, report):
            return report, None
        attempt_not_before_ms = self._active_dispatch_attempt_started_ms or 0
        if attempt_not_before_ms <= 0:
            attempt_row = self.store.intent_by_cloid(intent.cloid)
            try:
                raw_attempt_not_before_ms = (
                    attempt_row.get("attempt_updated_ms") if attempt_row is not None else 0
                )
                attempt_not_before_ms = int(str(raw_attempt_not_before_ms or 0))
            except (TypeError, ValueError):
                attempt_not_before_ms = 0
        if attempt_not_before_ms <= 0:
            return (
                replace(
                    report,
                    payload={
                        **dict(report.payload),
                        "zero_fill_confirmation_attempts": [
                            {
                                "source": "durable_attempt_boundary",
                                "error": "missing signed dispatch timestamp",
                            }
                        ],
                    },
                ),
                None,
            )
        proof, attempts = self._confirm_hip3_ioc_zero_fill(
            intent,
            attempt_not_before_ms=attempt_not_before_ms,
        )
        if proof is None:
            proof = self._hip3_ioc_synchronous_no_match_proof(
                intent,
                report,
                attempts,
                attempt_not_before_ms=attempt_not_before_ms,
                stage=stage,
            )
        if proof is None:
            return (
                replace(
                    report,
                    payload={
                        **dict(report.payload),
                        "zero_fill_confirmation_attempts": attempts,
                    },
                ),
                None,
            )
        retry_identity = self._hip3_ioc_retry_identity(intent)
        blocker = (
            f"{canonical_market_symbol(intent.coin)} IOC reached the exchange but matched zero "
            "resting liquidity; retry from fresh market truth"
        )
        deferral = (
            self._hip3_liquidity_deferral(intent, [blocker], stage=stage) if paced_retry else None
        )
        exchange_status = (
            HIP3_IOC_ZERO_FILL_EXCHANGE_STATUS if paced_retry else HIP3_IOC_ZERO_FILL_CLEANUP_STATUS
        )
        payload = {
            **dict(report.payload),
            "error": blocker,
            "original_exchange_status": report.exchange_status,
            "signed_action_performed": True,
            "proven_zero_fill": True,
            "filled_size": Decimal("0"),
            "requires_post_action_reconcile": True,
            "post_send_retry_identity": retry_identity,
            "zero_fill_proof_id": proof["proof_id"],
            "zero_fill_proof": proof,
            "zero_fill_confirmation_attempts": attempts,
        }
        if deferral is not None:
            payload["liquidity_deferral"] = to_jsonable(deferral)
        return (
            replace(
                report,
                report_id=deterministic_cloid(
                    "hip3-ioc-zero-fill-report",
                    report.report_id,
                    proof["proof_id"],
                    stage,
                ),
                exchange_status=exchange_status,
                payload=payload,
            ),
            deferral,
        )

    @staticmethod
    def _is_proven_hip3_ioc_zero_fill_report(report: ExecutionReport) -> bool:
        if (
            report.status != IntentStatus.REJECTED
            or report.exchange_status not in HIP3_IOC_ZERO_FILL_NEUTRAL_STATUSES
            or not isinstance(report.payload, dict)
            or report.payload.get("signed_action_performed") is not True
            or report.payload.get("proven_zero_fill") is not True
        ):
            return False
        proof = report.payload.get("zero_fill_proof")
        proof_id = report.payload.get("zero_fill_proof_id")
        try:
            filled_size = parse_decimal(report.payload.get("filled_size"))
        except (ArithmeticError, TypeError, ValueError):
            return False
        return (
            isinstance(proof, dict)
            and proof.get("kind") == HIP3_IOC_ZERO_FILL_PROOF_KIND
            and isinstance(proof_id, str)
            and proof.get("proof_id") == proof_id
            and filled_size == 0
        )

    @staticmethod
    def _intent_from_journal_row(row: dict[str, Any]) -> FollowerIntent | None:
        payload = _json_object(row.get("payload_json"))
        try:
            raw_price = payload.get("price")
            raw_reduce_only = payload.get("reduce_only")
            execution_proof = payload.get("execution_proof")
            side = str(payload.get("side") or "").strip().lower()
            if not isinstance(raw_reduce_only, bool) or side not in {"buy", "sell"}:
                return None
            return FollowerIntent(
                intent_id=str(payload.get("intent_id") or row.get("intent_id") or ""),
                cloid=str(payload.get("cloid") or row.get("cloid") or ""),
                action=IntentAction(str(payload.get("action") or row.get("action") or "")),
                coin=str(payload.get("coin") or row.get("coin") or ""),
                side=side,
                size=parse_decimal(payload.get("size")),
                price=(parse_decimal(raw_price) if raw_price not in {None, ""} else None),
                reduce_only=raw_reduce_only,
                mode=Mode(str(payload.get("mode") or row.get("mode") or "")),
                source_event_key=str(payload.get("source_event_key") or ""),
                reason=str(payload.get("reason") or "journaled IOC"),
                created_ms=int(payload.get("created_ms") or row.get("created_ms") or 0),
                desired_state_id=str(
                    payload.get("desired_state_id") or row.get("desired_state_id") or ""
                ),
                status=IntentStatus(str(payload.get("status") or row.get("status") or "pending")),
                execution_proof=(execution_proof if isinstance(execution_proof, dict) else {}),
            )
        except (ArithmeticError, TypeError, ValueError):
            return None

    def _settled_hip3_ioc_zero_fill_payload(
        self,
        row: dict[str, Any],
        status_payload: Any,
    ) -> dict[str, Any] | None:
        intent = self._intent_from_journal_row(row)
        if intent is None:
            return None
        try:
            attempt_not_before_ms = int(row.get("attempt_updated_ms") or 0)
        except (TypeError, ValueError):
            return None
        if attempt_not_before_ms <= 0:
            return None
        proof = self._hip3_ioc_zero_fill_order_status_proof(
            intent,
            status_payload,
            attempt_not_before_ms=attempt_not_before_ms,
        )
        if proof is None:
            return None
        return {
            "order_status": status_payload,
            "signed_action_performed": True,
            "proven_zero_fill": True,
            "filled_size": Decimal("0"),
            "requires_post_action_reconcile": True,
            "post_send_retry_identity": self._hip3_ioc_retry_identity(intent),
            "zero_fill_proof_id": proof["proof_id"],
            "zero_fill_proof": proof,
        }

    def _config_revision_id(self) -> str:
        payload = to_jsonable(redact_secrets(asdict(self.config)))
        return deterministic_cloid("config", self.config.mode.value, payload)

    def source_follow_startup_sync_status(self) -> dict[str, Any]:
        """Return a copy of the startup-adoption barrier state for CLI heartbeats."""

        return dict(self._source_follow_startup_sync)

    def _source_follow_startup_sync_evaluation(
        self,
        result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Evaluate the RUNNING barrier from current durable and exchange-cycle truth."""

        self.safe_mode.refresh_from_store()
        unfinished = self.store.unfinished_source_reaction_count(
            source_wallet=self.config.source_wallet
        )
        retry_counts = self.store.source_reaction_retry_counts(
            source_wallet=self.config.source_wallet,
            retry_due_ms=now_ms(),
        )
        liquidity_deferred = (
            self._hip3_liquidity_deferred_intents(result) if isinstance(result, dict) else []
        )
        liquidity_retry = (
            self._hip3_liquidity_retry_payload(result) if isinstance(result, dict) else None
        )
        liquidity_retry_durable = bool(
            not liquidity_deferred
            or (
                liquidity_retry is not None
                and (
                    retry_counts["hip3_liquidity_waiting"] + retry_counts["hip3_liquidity_due"] > 0
                )
            )
        )
        cycle_complete = bool(
            isinstance(result, dict)
            and self._execution_cycle_completed(result)
            and not self._deferred_exposure_increasing_intents(result)
        )
        fully_synced = bool(
            isinstance(result, dict)
            and self._source_reaction_run_completed(result)
            and unfinished == 0
        )
        supervisor = self._validation_supervisor_decision()
        ready = bool(
            not self.safe_mode.enabled
            and supervisor.ok
            and retry_counts["other_blocking_unfinished"] == 0
            and liquidity_retry_durable
            and cycle_complete
        )
        return {
            "ready": ready,
            "unfinished_source_reactions": unfinished,
            "source_reaction_retry_counts": retry_counts,
            "startup_cycle_complete": cycle_complete,
            "startup_fully_synced": fully_synced,
            "startup_liquidity_retry_durable": liquidity_retry_durable,
            "safe_mode": self._safe_mode_status(),
            "validation_supervisor": to_jsonable(supervisor),
        }

    def _startup_stale_source_recovery_allowed(self) -> bool:
        """Allow the live startup path, and only that path, to replan stale truth."""

        self.safe_mode.refresh_from_store()
        return bool(
            self.config.mode == Mode.LIVE
            and self._source_follow_startup_sync.get("ready") is not True
            and self.safe_mode.enabled
            and self.safe_mode.reason == SafeModeReason.STALE_SOURCE
        )

    def _maybe_promote_source_follow_startup_sync(
        self,
        result: dict[str, Any] | None,
        deferred_open_drain: dict[str, Any] | None,
        *,
        trigger: str,
        recovery: dict[str, Any] | None = None,
        typed_retry_obligation_proven: bool = False,
    ) -> bool:
        """Monotonically promote a blocked startup after a fresh completed cycle."""

        if self._source_follow_startup_sync.get("ready") is True or not isinstance(result, dict):
            return False

        liquidity_obligation = self._source_follow_startup_sync.get("startup_liquidity_obligation")
        liquidity_deferred = self._hip3_liquidity_deferred_intents(result)
        if liquidity_deferred and self._hip3_liquidity_retry_payload(result) is None:
            return False
        retry_counts = self.store.source_reaction_retry_counts(
            source_wallet=self.config.source_wallet,
            retry_due_ms=now_ms(),
        )
        durable_typed_retry_exists = bool(
            typed_retry_obligation_proven
            or retry_counts["hip3_liquidity_waiting"]
            or retry_counts["hip3_liquidity_due"]
        )
        if liquidity_deferred and not durable_typed_retry_exists:
            liquidity_obligation = self._ensure_startup_liquidity_retry_obligation(
                result,
                deferred_open_drain,
            )

        evaluation = self._source_follow_startup_sync_evaluation(result)
        if evaluation["ready"] is not True:
            return False

        fully_synced = bool(evaluation["startup_fully_synced"])
        stage = "ready" if fully_synced else "ready_with_liquidity_deferrals"
        detail = (
            "startup recovery and bounded current-exposure adoption completed"
            if fully_synced
            else (
                "startup recovery is operational; HIP-3 markets without usable depth "
                "remain on paced retry"
            )
        )
        candidate = {
            **self._source_follow_startup_sync,
            **evaluation,
            "ready": True,
            "stage": stage,
            "detail": detail,
            "startup_liquidity_obligation": liquidity_obligation,
            "adoption_result": result,
            "deferred_open_drain": deferred_open_drain,
            "promotion_trigger": trigger,
        }
        if recovery is not None:
            candidate["startup_recovery"] = recovery

        self.store.append_control_audit(
            control="source_follow_startup_sync",
            status="ready",
            detail=detail,
            payload={
                "source_wallet": self.config.source_wallet.lower(),
                "action_account": self._effective_action_account().lower(),
                "promotion_trigger": trigger,
                "unfinished_source_reactions": evaluation["unfinished_source_reactions"],
                "source_reaction_retry_counts": evaluation["source_reaction_retry_counts"],
                "startup_cycle_complete": evaluation["startup_cycle_complete"],
                "startup_fully_synced": fully_synced,
                "startup_liquidity_retry_durable": evaluation["startup_liquidity_retry_durable"],
                "startup_liquidity_obligation": liquidity_obligation,
                "safe_mode": evaluation["safe_mode"],
                "validation_supervisor": evaluation["validation_supervisor"],
            },
        )
        self._source_follow_startup_sync = candidate
        return True

    def record_runner_heartbeat(
        self,
        *,
        status: str,
        detail: str = "",
        ttl_ms: int = 15_000,
        cycle_completed: bool = False,
        role: str = "polling_worker",
    ) -> dict[str, Any]:
        if ttl_ms <= 0 and status != "stopped":
            raise ValueError("runner heartbeat ttl_ms must be positive")
        observed = now_ms()
        if cycle_completed:
            self._runner_cycle_count += 1
            self._runner_last_cycle_ms = observed
        self.store.upsert_runner_heartbeat(
            instance_id=self.instance_id,
            role=role,
            mode=self.config.mode.value,
            source_wallet=self.config.source_wallet.lower(),
            action_account=self._effective_action_account() or "local-paper-shadow",
            config_revision_id=self._config_revision_id(),
            status=status,
            detail=detail[:300],
            started_ms=self._runner_started_ms,
            heartbeat_ms=observed,
            expires_ms=observed if status == "stopped" else observed + ttl_ms,
            cycle_count=self._runner_cycle_count,
            last_cycle_ms=self._runner_last_cycle_ms,
        )
        if role == "containment_watchdog":
            return self.containment_watchdog_status()
        return self.runner_status()

    def runner_status(self) -> dict[str, Any]:
        observed = now_ms()
        expected_revision = self._config_revision_id()
        row = self.store.latest_runner_heartbeat(
            mode=self.config.mode.value,
            source_wallet=self.config.source_wallet.lower(),
            action_account=self._effective_action_account() or "local-paper-shadow",
            exclude_role="containment_watchdog",
        )
        if row is None:
            return {
                "online": False,
                "status": "not_running",
                "detail": "no matching runner heartbeat",
                "heartbeat_age_ms": None,
                "expires_ms": None,
                "instance_id": "",
                "owner_instance": False,
                "config_revision_id": expected_revision,
                "config_matches": False,
                "cycle_count": 0,
                "last_cycle_ms": None,
            }
        heartbeat_ms = int(row.get("heartbeat_ms") or 0)
        expires_ms = int(row.get("expires_ms") or 0)
        status = str(row.get("status") or "unknown")
        config_matches = str(row.get("config_revision_id") or "") == expected_revision
        return {
            "online": expires_ms > observed and status != "stopped",
            "status": status,
            "detail": str(row.get("detail") or ""),
            "heartbeat_age_ms": max(0, observed - heartbeat_ms) if heartbeat_ms else None,
            "expires_ms": expires_ms or None,
            "instance_id": str(row.get("instance_id") or ""),
            "owner_instance": str(row.get("instance_id") or "") == self.instance_id,
            "config_revision_id": str(row.get("config_revision_id") or ""),
            "config_matches": config_matches,
            "role": str(row.get("role") or ""),
            "cycle_count": int(row.get("cycle_count") or 0),
            "last_cycle_ms": row.get("last_cycle_ms"),
        }

    def containment_watchdog_status(self) -> dict[str, Any]:
        observed = now_ms()
        expected_revision = self._config_revision_id()
        row = self.store.latest_runner_heartbeat(
            mode=self.config.mode.value,
            source_wallet=self.config.source_wallet.lower(),
            action_account=self._effective_action_account() or "local-paper-shadow",
            role="containment_watchdog",
        )
        if row is None:
            return {
                "ready": False,
                "status": "not_running",
                "detail": "no matching containment watchdog heartbeat",
                "heartbeat_age_ms": None,
                "expires_ms": None,
                "instance_id": "",
                "config_matches": False,
            }
        heartbeat_ms = int(row.get("heartbeat_ms") or 0)
        expires_ms = int(row.get("expires_ms") or 0)
        status = str(row.get("status") or "unknown")
        config_matches = str(row.get("config_revision_id") or "") == expected_revision
        heartbeat_age_ms = max(0, observed - heartbeat_ms) if heartbeat_ms else None
        ready = (
            status in {"ready", "running"}
            and expires_ms > observed
            and config_matches
            and heartbeat_age_ms is not None
            and heartbeat_age_ms <= self.config.ops.containment_watchdog_ttl_ms
        )
        return {
            "ready": ready,
            "status": status,
            "detail": str(row.get("detail") or ""),
            "heartbeat_age_ms": heartbeat_age_ms,
            "expires_ms": expires_ms or None,
            "instance_id": str(row.get("instance_id") or ""),
            "config_matches": config_matches,
            "cycle_count": int(row.get("cycle_count") or 0),
            "last_cycle_ms": row.get("last_cycle_ms"),
        }

    def run_once(self, *, recovery_containment_only: bool = False) -> dict[str, Any]:
        self.safe_mode.refresh_from_store()
        if self.config.mode == Mode.LIVE and not self.config.exchange.live_copy_enable:
            detail = (
                "generic live copy runner is disabled; keep HLCT_LIVE_COPY_ENABLE=false during "
                "the isolated mainnet canary phase"
            )
            self.safe_mode.trip(SafeModeReason.LIVE_BLOCKED, detail)
            return {
                "preflight": to_jsonable(self.preflight(auth_probe=False)),
                **self._safe_mode_payload(cleared=False),
                "intents": [],
                "reports": [],
            }
        config_revision = to_jsonable(redact_secrets(asdict(self.config)))
        self.store.append_config_revision(
            deterministic_cloid("config", self.config.mode.value, config_revision),
            config_revision,
        )
        if self.config.mode in {Mode.TESTNET, Mode.LIVE} and not self._journal_scope_error:
            signed_action_recovery = self.store.unresolved_signed_action_attempts(
                self.config.mode,
                account=self._effective_action_account(),
                network=("mainnet" if self.config.mode == Mode.LIVE else "testnet"),
            )
            if signed_action_recovery:
                detail = (
                    "durable non-order signed-action recovery requires explicit operator review; "
                    "no signed mutation was retried"
                )
                self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
                return {
                    "preflight": to_jsonable(self.preflight(auth_probe=False)),
                    "startup_recovery": {
                        "requires_operator_review": True,
                        "signed_action_attempts": to_jsonable(signed_action_recovery),
                        "pending_follower_intents": self.store.pending_intent_count(
                            self.config.mode
                        ),
                    },
                    **self._safe_mode_payload(cleared=False),
                    "intents": [],
                    "reports": [],
                }
            pending_recovery = self.store.pending_intent_count(self.config.mode)
            unresolved_recovery = self.store.unresolved_desired_state_count(
                mode=self.config.mode,
                source_wallet=self.config.source_wallet,
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            if pending_recovery or unresolved_recovery:
                recovery = self.settle_pending_intents()
                recovery_preflight = self.preflight(auth_probe=False)
                return {
                    "preflight": to_jsonable(recovery_preflight),
                    "startup_recovery": recovery,
                    **self._safe_mode_payload(cleared=False),
                    "intents": [],
                    "reports": [],
                }
        preflight = self.preflight()
        if not preflight.passed:
            return {
                "preflight": to_jsonable(preflight),
                **self._safe_mode_payload(cleared=False),
                "intents": [],
                "reports": [],
            }

        if not self._acquire_exchange_lease("run_once"):
            return {
                "preflight": to_jsonable(preflight),
                "safe_mode": {
                    "enabled": self.safe_mode.enabled,
                    "reason": self.safe_mode.reason.value,
                    "detail": self.safe_mode.detail,
                },
                "intents": [],
            }

        try:
            return self._run_once_with_lease(
                preflight,
                recovery_containment_only=recovery_containment_only,
            )
        finally:
            self._reset_signed_action_context()
            self._release_exchange_lease("run_once")

    async def observe_source_websocket(
        self, stop_after_messages: int | None = None
    ) -> dict[str, Any]:
        await self.observer.observe_websocket(stop_after_messages=stop_after_messages)
        return {
            "source_events": self.store.count("source_events"),
            "safe_mode": {
                "enabled": self.safe_mode.enabled,
                "reason": self.safe_mode.reason.value,
                "detail": self.safe_mode.detail,
            },
        }

    def backfill_source_fills(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> dict[str, Any]:
        end = end_time_ms if end_time_ms is not None else now_ms()
        if start_time_ms is None:
            fill_start = self._source_backfill_start(self.ORDINARY_FILL_SUBTYPES, end)
            twap_start = self._source_backfill_start(self.TWAP_SLICE_FILL_SUBTYPES, end)
        else:
            fill_start = start_time_ms
            twap_start = start_time_ms
        fill_report = self.observer.backfill_fills_by_time(
            start_time_ms=fill_start,
            end_time_ms=end,
            max_pages=self.config.ops.source_fill_backfill_max_pages,
        )
        twap_report = self.observer.backfill_twap_slice_fills_by_time(
            start_time_ms=twap_start,
            end_time_ms=end,
            max_pages=self.config.ops.source_fill_backfill_max_pages,
        )
        report = SourceGapBackfillReport(
            start_time_ms=min(fill_report.start_time_ms, twap_report.start_time_ms),
            end_time_ms=end,
            pages=fill_report.pages + twap_report.pages,
            fetched=fill_report.fetched + twap_report.fetched,
            inserted=fill_report.inserted + twap_report.inserted,
            duplicates=fill_report.duplicates + twap_report.duplicates,
            warnings=[
                *_prefixed_backfill_warnings("fills", fill_report),
                *_prefixed_backfill_warnings("twap_slice_fills", twap_report),
            ],
            fills=fill_report,
            twap_slice_fills=twap_report,
        )
        return to_jsonable(report)

    def _source_backfill_start(self, subtypes: tuple[str, ...], end_time_ms: int) -> int:
        latest_ts = self.store.latest_source_event_ts_by_subtypes(
            subtypes,
            source_wallet=self.config.source_wallet,
        )
        if latest_ts > 0:
            return max(0, latest_ts - self.config.ops.source_fill_backfill_overlap_ms)
        return max(0, end_time_ms - self.config.ops.source_fill_backfill_lookback_ms)

    async def follow_source_websocket(
        self, stop_after_messages: int | None = None
    ) -> dict[str, Any]:
        self._source_follow_startup_sync = {
            "ready": False,
            "stage": "startup_backfill",
            "detail": "startup backfill and reaction adoption are in progress",
        }
        if self.config.mode == Mode.LIVE and not self.config.exchange.live_copy_enable:
            detail = (
                "generic live WebSocket follower is disabled; keep HLCT_LIVE_COPY_ENABLE=false "
                "during the isolated mainnet canary phase"
            )
            self.safe_mode.trip(SafeModeReason.LIVE_BLOCKED, detail)
            return {
                "source_events": self.store.count("source_events"),
                "reaction_queue_size": self.config.ops.source_reaction_queue_size,
                "startup_backfill": None,
                "disconnect_backfill": None,
                "disconnect_recoveries": [],
                "websocket_error": "",
                "stats": {},
                "reactions": [],
                "backfill_reactions": [],
                **self._safe_mode_payload(cleared=False),
            }
        queue: asyncio.Queue[tuple[SourceEvent, bool]] = asyncio.Queue(
            maxsize=self.config.ops.source_reaction_queue_size
        )
        reaction_lock = asyncio.Lock()
        stats: dict[str, int] = {
            "observed_events": 0,
            "duplicate_events": 0,
            "ignored_events": 0,
            "scheduled_reactions": 0,
            "completed_reactions": 0,
            "failed_reactions": 0,
            "skipped_reactions": 0,
            "validation_runs": 0,
            "coalesced_reactions": 0,
            "queue_overflows": 0,
            "websocket_attempts": 0,
            "websocket_errors": 0,
            "websocket_reconnects": 0,
            "stale_source_recoveries": 0,
            "liquidity_retry_wakeups": 0,
        }
        reactions: list[dict[str, Any]] = []
        backfill_reactions: list[dict[str, Any]] = []
        try:
            startup_backfill = await asyncio.to_thread(self.backfill_source_fills)
        except Exception as exc:
            detail = f"source websocket startup backfill failed: {exc}"
            if not self.safe_mode.enabled:
                self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
            return {
                "source_events": self.store.count("source_events"),
                "reaction_queue_size": self.config.ops.source_reaction_queue_size,
                "startup_backfill": {"error": str(exc)},
                "disconnect_backfill": None,
                "disconnect_recoveries": [],
                "websocket_error": "",
                "stats": stats,
                "reactions": reactions,
                "backfill_reactions": backfill_reactions,
                "safe_mode": {
                    "enabled": self.safe_mode.enabled,
                    "reason": self.safe_mode.reason.value,
                    "detail": self.safe_mode.detail,
                },
            }
        startup_backfill_reaction = await asyncio.to_thread(
            self._validate_recovered_backfill,
            startup_backfill,
            stage="startup_backfill",
        )
        if startup_backfill_reaction is not None:
            backfill_reactions.append(startup_backfill_reaction)
        startup_adoption_result: dict[str, Any] | None = None
        startup_deferred_open_drain: dict[str, Any] | None = None
        startup_recovery: dict[str, Any] | None = None
        try:
            startup_adoption_result, startup_deferred_open_drain = await asyncio.to_thread(
                self._run_once_until_deferred_opens_drained
            )
        except Exception as exc:
            detail = f"bounded startup current-exposure adoption failed: {exc}"
            if not self.safe_mode.enabled:
                self.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, detail)
            startup_adoption_result = {"error": str(exc)}
        self.safe_mode.refresh_from_store()
        if self._startup_stale_source_recovery_allowed():
            async with reaction_lock:
                startup_recovery = await asyncio.to_thread(
                    self._recover_source_websocket_gap,
                    allow_auto_clear=True,
                    allow_bounded_startup_adoption=True,
                )
            resume = startup_recovery.get("auto_resume")
            recovered_cycle = startup_recovery.get("containment_cycle")
            recovered_drain = startup_recovery.get("deferred_open_drain")
            if (
                isinstance(resume, dict)
                and resume.get("cleared") is True
                and isinstance(recovered_cycle, dict)
            ):
                startup_adoption_result = recovered_cycle
                startup_deferred_open_drain = (
                    recovered_drain if isinstance(recovered_drain, dict) else None
                )
        startup_liquidity_obligation = (
            self._ensure_startup_liquidity_retry_obligation(
                startup_adoption_result,
                startup_deferred_open_drain,
            )
            if isinstance(startup_adoption_result, dict) and not self.safe_mode.enabled
            else None
        )
        startup_evaluation = self._source_follow_startup_sync_evaluation(startup_adoption_result)
        startup_unfinished = int(startup_evaluation["unfinished_source_reactions"])
        startup_retry_counts = startup_evaluation["source_reaction_retry_counts"]
        startup_cycle_complete = bool(startup_evaluation["startup_cycle_complete"])
        startup_fully_synced = bool(startup_evaluation["startup_fully_synced"])
        startup_ready = bool(startup_evaluation["ready"])
        drain_status = (
            str(startup_deferred_open_drain.get("status") or "")
            if isinstance(startup_deferred_open_drain, dict)
            else "missing"
        )
        drain_cycle_count = (
            int(startup_deferred_open_drain.get("cycle_count") or 0)
            if isinstance(startup_deferred_open_drain, dict)
            else 0
        )
        self.store.append_control_audit(
            control="source_follow_startup_sync",
            status="ready" if startup_ready else "blocked",
            detail=(
                (
                    "startup adoption is operational with paced market-specific "
                    "HIP-3 liquidity deferrals"
                    if not startup_fully_synced
                    else "startup backfill and bounded current-exposure adoption completed"
                )
                if startup_ready
                else "startup backfill/adoption did not cross the RUNNING barrier"
            ),
            payload={
                "source_wallet": self.config.source_wallet.lower(),
                "action_account": self._effective_action_account().lower(),
                "backfill_inserted": (
                    int(startup_backfill.get("inserted") or 0)
                    if isinstance(startup_backfill, dict)
                    else 0
                ),
                "backfill_pending_reactions": (
                    int(startup_backfill_reaction.get("pending_reactions") or 0)
                    if isinstance(startup_backfill_reaction, dict)
                    else 0
                ),
                "unfinished_source_reactions": startup_unfinished,
                "source_reaction_retry_counts": startup_retry_counts,
                "startup_cycle_complete": startup_cycle_complete,
                "startup_fully_synced": startup_fully_synced,
                "startup_liquidity_retry_durable": startup_evaluation[
                    "startup_liquidity_retry_durable"
                ],
                "startup_liquidity_obligation": startup_liquidity_obligation,
                "deferred_open_drain_status": drain_status,
                "deferred_open_drain_cycle_count": drain_cycle_count,
                "safe_mode": self._safe_mode_status(),
                "validation_supervisor": startup_evaluation["validation_supervisor"],
                "startup_recovery": startup_recovery,
            },
        )
        self._source_follow_startup_sync = {
            "ready": startup_ready,
            "stage": (
                "ready"
                if startup_ready and startup_fully_synced
                else "ready_with_liquidity_deferrals"
                if startup_ready
                else "blocked"
            ),
            "detail": (
                (
                    "startup adoption is operational; HIP-3 markets without usable "
                    "depth remain on paced retry"
                    if not startup_fully_synced
                    else "startup backfill and bounded current-exposure adoption completed"
                )
                if startup_ready
                else "startup backfill/adoption left safe mode or unfinished reactions"
            ),
            "unfinished_source_reactions": startup_unfinished,
            "source_reaction_retry_counts": startup_retry_counts,
            "startup_cycle_complete": startup_cycle_complete,
            "startup_fully_synced": startup_fully_synced,
            "startup_liquidity_retry_durable": startup_evaluation[
                "startup_liquidity_retry_durable"
            ],
            "startup_liquidity_obligation": startup_liquidity_obligation,
            "adoption_result": startup_adoption_result,
            "deferred_open_drain": startup_deferred_open_drain,
            "safe_mode": self._safe_mode_status(),
            "validation_supervisor": startup_evaluation["validation_supervisor"],
            "startup_recovery": startup_recovery,
            "backfill_validation": startup_backfill_reaction,
        }
        disconnect_backfill: dict[str, Any] | None = None
        disconnect_recoveries: list[dict[str, Any]] = []
        websocket_error = ""

        async def on_event(event: SourceEvent, inserted: bool) -> None:
            stats["observed_events"] += 1
            if not inserted:
                stats["duplicate_events"] += 1
                if self.store.source_reaction_status(event.idempotency_key) in {
                    "completed",
                    "ignored",
                    None,
                }:
                    return
            if not self._source_event_triggers_copy_validation(event):
                stats["ignored_events"] += 1
                self.store.finish_source_reactions(
                    [event.idempotency_key],
                    status="ignored",
                    outcome={
                        "reason": "event does not change copy exposure",
                        "skip_class": "informational_event",
                    },
                )
                return
            self.safe_mode.refresh_from_store()
            startup_stale_recovery = self._startup_stale_source_recovery_allowed()
            if self.safe_mode.enabled and not startup_stale_recovery:
                stats["skipped_reactions"] += 1
                skipped = self._safe_mode_reaction_skip([event])
                self.store.finish_source_reactions(
                    [event.idempotency_key],
                    status="blocked",
                    outcome=skipped,
                )
                reactions.append(skipped)
                return
            # A stale source detected during bounded startup must enter the serialized
            # recovery worker. Marking it blocked here made the startup barrier one-way.
            signal_changed = (
                True if startup_stale_recovery else self._source_copy_signal_changed(event)
            )
            retry_counts = self.store.source_reaction_retry_counts(
                source_wallet=self.config.source_wallet,
                retry_due_ms=now_ms(),
            )
            blocked_backlog = self.store.blocked_source_reaction_count(
                source_wallet=self.config.source_wallet,
            )
            claimable_blocked_backlog = max(
                0,
                blocked_backlog - retry_counts["hip3_liquidity_waiting"],
            )
            if not signal_changed and claimable_blocked_backlog == 0:
                stats["ignored_events"] += 1
                waiting_for_liquidity = retry_counts["hip3_liquidity_waiting"] > 0
                self.store.finish_source_reactions(
                    [event.idempotency_key],
                    status="ignored",
                    outcome={
                        "reason": (
                            "copy-relevant source state is unchanged and the HIP-3 "
                            "liquidity retry is not due"
                            if waiting_for_liquidity
                            else "copy-relevant source state is unchanged"
                        ),
                        "skip_class": (
                            "hip3_liquidity_retry_not_due"
                            if waiting_for_liquidity
                            else "current_truth_already_converged"
                        ),
                    },
                )
                return
            try:
                queue.put_nowait((event, signal_changed))
            except asyncio.QueueFull:
                detail = (
                    "source websocket reaction queue overflowed; "
                    "REST reconcile is required before trusting the stream"
                )
                stats["queue_overflows"] += 1
                stats["failed_reactions"] += 1
                self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
                failure = {
                    "source_event_key": event.idempotency_key,
                    "source_event_keys": [event.idempotency_key],
                    "event_type": event.event_type.value,
                    "event_subtype": self._source_event_subtype(event),
                    "action": "failed",
                    "failure_reason": "queue_overflow",
                    "batched_event_count": 1,
                    "source_event_age_ms": self._source_event_age_ms(event),
                    "error": detail,
                    "safe_mode": self._safe_mode_status(),
                }
                self.store.finish_source_reactions(
                    [event.idempotency_key],
                    status="failed",
                    outcome=failure,
                )
                reactions.append(failure)
                return
            stats["scheduled_reactions"] += 1

        async def worker() -> None:
            while True:
                event, coalesce_waiting = await queue.get()
                queued_items = [(event, coalesce_waiting)]
                queued_batch = [event]
                batch = list(queued_batch)
                actionable: list[SourceEvent] = []
                try:
                    while True:
                        try:
                            queued_items.append(queue.get_nowait())
                        except asyncio.QueueEmpty:
                            break
                    queued_batch = [item for item, _ in queued_items]
                    coalesce_waiting = any(flag for _, flag in queued_items)
                    retry_due_ms = None if coalesce_waiting else now_ms()
                    pending = self.store.pending_source_reaction_events(
                        source_wallet=self.config.source_wallet,
                        retry_due_ms=retry_due_ms,
                    )
                    batch_by_key = {
                        item.idempotency_key: item for item in [*pending, *queued_batch]
                    }
                    batch = list(batch_by_key.values())
                    claimed_keys = set(
                        self.store.claim_source_reaction_keys(
                            [item.idempotency_key for item in batch],
                            include_processing=False,
                            retry_due_ms=retry_due_ms,
                        )
                    )
                    batch = [item for item in batch if item.idempotency_key in claimed_keys]
                    if not batch:
                        continue
                    actionable = [
                        item for item in batch if self._source_event_triggers_copy_validation(item)
                    ]
                    ignored = [
                        item
                        for item in batch
                        if not self._source_event_triggers_copy_validation(item)
                    ]
                    if ignored:
                        self.store.finish_source_reactions(
                            [item.idempotency_key for item in ignored],
                            status="ignored",
                            outcome={
                                "reason": "event does not change copy exposure",
                                "skip_class": "informational_event",
                            },
                        )
                    stats["coalesced_reactions"] += max(0, len(batch) - 1)
                    stale_recovery: dict[str, Any] | None = None
                    async with reaction_lock:
                        reaction = await asyncio.to_thread(self.react_to_source_events, batch)
                        if self._source_reaction_requires_recovery(reaction):
                            bounded_startup_recovery = (
                                self._source_follow_startup_sync.get("ready") is not True
                            )
                            stale_recovery = await asyncio.to_thread(
                                self._recover_source_websocket_gap,
                                allow_auto_clear=True,
                                allow_bounded_startup_adoption=bounded_startup_recovery,
                            )
                    if stale_recovery is not None:
                        stale_recovery["trigger"] = "stale_source_reaction"
                        disconnect_recoveries.append(stale_recovery)
                        stats["stale_source_recoveries"] += 1
                        validation = stale_recovery.get("backfill_validation")
                        if isinstance(validation, dict):
                            backfill_reactions.append(validation)
                        resume = stale_recovery.get("auto_resume")
                        cycle = stale_recovery.get("containment_cycle")
                        if (
                            isinstance(resume, dict)
                            and resume.get("cleared") is True
                            and isinstance(cycle, dict)
                        ):
                            reaction = self._recovered_source_reaction(
                                actionable,
                                stale_recovery,
                            )
                except Exception as exc:  # pragma: no cover - defensive runtime path
                    stats["failed_reactions"] += len(batch)
                    keys = [item.idempotency_key for item in batch]
                    detail = f"source websocket reaction failed for {keys}: {exc}"
                    self.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, detail)
                    failure = {
                        "source_event_key": event.idempotency_key,
                        "source_event_keys": keys,
                        "event_type": event.event_type.value,
                        "event_types": [item.event_type.value for item in batch],
                        "event_subtype": self._source_event_subtype(event),
                        "event_subtypes": [self._source_event_subtype(item) for item in batch],
                        "batched_event_count": len(batch),
                        "source_event_age_ms": max(
                            (self._source_event_age_ms(item) for item in batch),
                            default=0,
                        ),
                        "action": "failed",
                        "failure_reason": "reaction_exception",
                        "error": str(exc),
                        "safe_mode": self._safe_mode_status(),
                    }
                    self.store.finish_source_reactions(
                        keys,
                        status="failed",
                        outcome=failure,
                    )
                    reactions.append(failure)
                else:
                    try:
                        promotion_result: dict[str, Any] | None = None
                        promotion_drain: dict[str, Any] | None = None
                        typed_retry_obligation_proven = False
                        if reaction.get("action") == "run_once":
                            stats["validation_runs"] += 1
                            result = reaction.get("result")
                            completed = isinstance(
                                result, dict
                            ) and self._source_reaction_run_completed(result)
                            if completed and isinstance(result, dict):
                                finished_count = self._finish_completed_source_reaction_batch(
                                    actionable,
                                    outcome=reaction,
                                    result=result,
                                )
                            else:
                                finished_count = self.store.finish_source_reactions(
                                    [item.idempotency_key for item in actionable],
                                    status="blocked",
                                    outcome=reaction,
                                )
                            typed_retry_obligation_proven = bool(
                                finished_count > 0
                                and isinstance(reaction.get("retry"), dict)
                                and reaction["retry"].get("class") == "hip3_liquidity"
                                and type(reaction["retry"].get("retry_not_before_ms")) is int
                            )
                            if isinstance(result, dict):
                                promotion_result = result
                            raw_drain = reaction.get("deferred_open_drain")
                            if isinstance(raw_drain, dict):
                                promotion_drain = raw_drain
                        elif reaction.get("action") == "skipped":
                            stats["skipped_reactions"] += len(batch)
                            self.store.finish_source_reactions(
                                [item.idempotency_key for item in actionable],
                                status="blocked",
                                outcome=reaction,
                            )
                        elif reaction.get("action") == "ignored":
                            self.store.finish_source_reactions(
                                [item.idempotency_key for item in batch],
                                status="ignored",
                                outcome=reaction,
                            )
                        if promotion_result is not None:
                            self._maybe_promote_source_follow_startup_sync(
                                promotion_result,
                                promotion_drain,
                                trigger=(
                                    "stale_source_recovery"
                                    if stale_recovery is not None
                                    else "source_reaction"
                                ),
                                recovery=stale_recovery,
                                typed_retry_obligation_proven=typed_retry_obligation_proven,
                            )
                    except Exception as exc:  # pragma: no cover - defensive journal path
                        stats["failed_reactions"] += len(batch)
                        keys = [item.idempotency_key for item in batch]
                        detail = f"source reaction finalization failed for {keys}: {exc}"
                        with contextlib.suppress(Exception):
                            self.safe_mode.trip(
                                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                                detail,
                            )
                        failure = {
                            "source_event_key": event.idempotency_key,
                            "source_event_keys": keys,
                            "event_type": event.event_type.value,
                            "event_types": [item.event_type.value for item in batch],
                            "event_subtype": self._source_event_subtype(event),
                            "event_subtypes": [self._source_event_subtype(item) for item in batch],
                            "batched_event_count": len(batch),
                            "action": "failed",
                            "failure_reason": "reaction_finalization_exception",
                            "error": str(exc),
                            "safe_mode": {
                                "enabled": self.safe_mode.enabled,
                                "reason": self.safe_mode.reason.value,
                                "detail": self.safe_mode.detail,
                            },
                        }
                        with contextlib.suppress(Exception):
                            self.store.finish_source_reactions(
                                keys,
                                status="failed",
                                outcome=failure,
                            )
                        reactions.append(failure)
                    else:
                        stats["completed_reactions"] += len(batch)
                        reactions.append(reaction)
                finally:
                    for _ in queued_items:
                        queue.task_done()

        async def liquidity_retry_waker() -> None:
            """Wake the reaction worker when a durable market-liquidity retry is due."""

            while True:
                current_ms = now_ms()
                due = await asyncio.to_thread(
                    self.store.due_hip3_liquidity_reaction_events,
                    source_wallet=self.config.source_wallet,
                    retry_due_ms=current_ms,
                    limit=1,
                )
                if due:
                    try:
                        queue.put_nowait((due[0], False))
                    except asyncio.QueueFull:
                        # Live source events retain the bounded queue. A durable retry can
                        # wait briefly because its journal obligation remains intact.
                        await asyncio.sleep(0.1)
                    else:
                        stats["liquidity_retry_wakeups"] += 1
                        await asyncio.sleep(0)
                    continue
                next_retry_ms = await asyncio.to_thread(
                    self.store.next_hip3_liquidity_retry_ms,
                    source_wallet=self.config.source_wallet,
                )
                delay_s = (
                    5.0
                    if next_retry_ms is None
                    else min(5.0, max(0.05, (next_retry_ms - current_ms) / 1000))
                )
                await asyncio.sleep(delay_s)

        worker_task = asyncio.create_task(worker())
        retry_waker_task = asyncio.create_task(liquidity_retry_waker())
        try:
            reconnects_used = 0
            try:
                while True:
                    if stop_after_messages is not None:
                        remaining = stop_after_messages - stats["observed_events"]
                        if remaining <= 0:
                            break
                    else:
                        remaining = None
                    stats["websocket_attempts"] += 1
                    await self.observer.observe_websocket(
                        stop_after_messages=remaining,
                        on_event=on_event,
                    )
                    break
            except Exception as exc:
                while True:
                    stats["websocket_errors"] += 1
                    websocket_error = str(exc)
                    message_gap = isinstance(exc, SourceWebsocketMessageError)
                    can_retry = (
                        not message_gap
                        and reconnects_used < self.config.ops.source_websocket_reconnect_attempts
                    )
                    if not self.safe_mode.enabled:
                        if message_gap:
                            self.safe_mode.trip(
                                SafeModeReason.MISSED_EVENT_GAP,
                                f"source websocket message could not be trusted: {exc}",
                            )
                        else:
                            self.shield.websocket_disconnect(
                                f"source websocket disconnected: {exc}"
                            )
                    async with reaction_lock:
                        bounded_startup_recovery = (
                            self._source_follow_startup_sync.get("ready") is not True
                        )
                        recovery = await asyncio.to_thread(
                            self._recover_source_websocket_gap,
                            allow_auto_clear=can_retry,
                            allow_bounded_startup_adoption=bounded_startup_recovery,
                        )
                    disconnect_recoveries.append(recovery)
                    if bounded_startup_recovery:
                        resume = recovery.get("auto_resume")
                        recovered_cycle = recovery.get("containment_cycle")
                        recovered_drain = recovery.get("deferred_open_drain")
                        if (
                            isinstance(resume, dict)
                            and resume.get("cleared") is True
                            and isinstance(recovered_cycle, dict)
                        ):
                            self._maybe_promote_source_follow_startup_sync(
                                recovered_cycle,
                                recovered_drain if isinstance(recovered_drain, dict) else None,
                                trigger="websocket_reconnect_recovery",
                                recovery=recovery,
                            )
                    backfill = recovery.get("backfill") if isinstance(recovery, dict) else None
                    disconnect_backfill = backfill if isinstance(backfill, dict) else None
                    if not can_retry:
                        break
                    reconnects_used += 1
                    stats["websocket_reconnects"] += 1
                    backoff_ms = self.config.ops.source_websocket_reconnect_backoff_ms
                    if backoff_ms > 0:
                        await asyncio.sleep((backoff_ms * (2 ** (reconnects_used - 1))) / 1000)
                    try:
                        while True:
                            if stop_after_messages is not None:
                                remaining = stop_after_messages - stats["observed_events"]
                                if remaining <= 0:
                                    break
                            else:
                                remaining = None
                            attempt_start_events = stats["observed_events"]
                            stats["websocket_attempts"] += 1
                            await self.observer.observe_websocket(
                                stop_after_messages=remaining,
                                on_event=on_event,
                            )
                            break
                        break
                    except Exception as retry_exc:
                        # The reconnect limit protects against consecutive connection failures,
                        # not normal server-side expiry across an otherwise healthy long-lived
                        # process. Any trusted message proves the replacement connection worked
                        # and earns a fresh bounded retry budget for its eventual disconnect.
                        if stats["observed_events"] > attempt_start_events:
                            reconnects_used = 0
                        exc = retry_exc
                        continue
            retry_waker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await retry_waker_task
            await queue.join()
        finally:
            retry_waker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await retry_waker_task
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

        return {
            "source_events": self.store.count("source_events"),
            "reaction_queue_size": self.config.ops.source_reaction_queue_size,
            "startup_backfill": startup_backfill,
            "startup_recovery": startup_recovery,
            "disconnect_backfill": disconnect_backfill,
            "disconnect_recoveries": disconnect_recoveries,
            "websocket_error": websocket_error,
            "stats": stats,
            "reactions": reactions,
            "backfill_reactions": backfill_reactions,
            "safe_mode": {
                "enabled": self.safe_mode.enabled,
                "reason": self.safe_mode.reason.value,
                "detail": self.safe_mode.detail,
            },
        }

    def _recover_source_websocket_gap(
        self,
        *,
        allow_auto_clear: bool,
        allow_bounded_startup_adoption: bool = False,
    ) -> dict[str, Any]:
        self.safe_mode.refresh_from_store()
        trigger_safe_mode_revision = self.safe_mode.revision
        trigger_safe_mode_reason = self.safe_mode.reason
        recovery: dict[str, Any] = {
            "backfill": None,
            "backfill_validation": None,
            "reaction_scope": None,
            "source_reconcile": None,
            "follower_reconcile": None,
            "containment_cycle": None,
            "auto_resume": None,
            "auto_resume_skipped": "",
            "trigger_safe_mode": {
                "reason": trigger_safe_mode_reason.value,
                "revision": trigger_safe_mode_revision,
            },
            "bounded_startup_adoption": allow_bounded_startup_adoption,
        }
        backfill_ok = False
        source_ok = False
        source_snapshot: SourceSnapshot | None = None
        try:
            backfill = self.backfill_source_fills()
        except Exception as exc:  # pragma: no cover - defensive runtime path
            detail = f"source websocket disconnect backfill failed: {exc}"
            self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
            recovery["backfill"] = {"error": str(exc)}
        else:
            recovery["backfill"] = backfill
            backfill_ok = not bool(backfill.get("warnings")) and "error" not in backfill
            if backfill_ok:
                recovery["reaction_scope"] = self._freeze_source_reaction_scope()
        try:
            source_snapshot = self.observer.reconcile_once()
        except Exception as exc:  # pragma: no cover - defensive runtime path
            detail = f"source websocket disconnect source reconcile failed: {exc}"
            self.safe_mode.trip(SafeModeReason.REST_LAG, detail)
            recovery["source_reconcile"] = {"error": str(exc)}
        else:
            source_ok = self._check_source_freshness(source_snapshot.observed_ms)
            recovery["source_reconcile"] = {
                "observed_ms": source_snapshot.observed_ms,
                "state_key": source_snapshot.state_key,
                "planning_key": source_snapshot.planning_key,
                "positions": sorted(source_snapshot.positions),
                "open_orders": len(source_snapshot.open_orders),
                "fresh": source_ok,
            }
        if not allow_auto_clear:
            recovery["auto_resume_skipped"] = "no reconnect attempt remains"
            return recovery
        recoverable_reasons = {
            SafeModeReason.WEBSOCKET_DISCONNECT,
            SafeModeReason.STALE_SOURCE,
        }
        if trigger_safe_mode_reason not in recoverable_reasons:
            recovery["auto_resume_skipped"] = (
                f"trigger safe mode reason is {trigger_safe_mode_reason.value}"
            )
            return recovery
        if self.safe_mode.reason not in recoverable_reasons:
            recovery["auto_resume_skipped"] = f"safe mode reason is {self.safe_mode.reason.value}"
            return recovery
        if not backfill_ok or not source_ok:
            recovery["auto_resume_skipped"] = (
                "gap backfill or source reconcile did not complete cleanly"
            )
            return recovery
        reaction_scope = recovery.get("reaction_scope")
        if not isinstance(reaction_scope, dict) or not reaction_scope.get("complete"):
            detail = "source reaction obligations could not be frozen before recovery"
            self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
            recovery["auto_resume_skipped"] = detail
            return recovery
        assert source_snapshot is not None
        if self.config.mode == Mode.LIVE:
            live_recovery = self._recover_live_source_websocket_gap(
                backfill=backfill,
                source_snapshot=source_snapshot,
                reaction_scope=reaction_scope,
                expected_safe_mode_revision=trigger_safe_mode_revision,
                allow_bounded_startup_adoption=allow_bounded_startup_adoption,
            )
            recovery.update(live_recovery)
            return recovery
        if self.safe_mode.reason != SafeModeReason.WEBSOCKET_DISCONNECT:
            recovery["auto_resume_skipped"] = f"safe mode reason is {self.safe_mode.reason.value}"
            return recovery
        resume = self.manual_reconcile()
        recovery["auto_resume"] = {
            "cleared": not self.safe_mode.enabled,
            "safe_mode": self._safe_mode_status(),
            "result": resume,
        }
        if not self.safe_mode.enabled:
            recovery["backfill_validation"] = self._validate_recovered_backfill(
                backfill,
                stage="disconnect_backfill",
                reaction_scope=reaction_scope,
            )
        return recovery

    def _recover_live_source_websocket_gap(
        self,
        *,
        backfill: dict[str, Any],
        source_snapshot: SourceSnapshot,
        reaction_scope: dict[str, Any],
        expected_safe_mode_revision: int,
        allow_bounded_startup_adoption: bool = False,
    ) -> dict[str, Any]:
        """Recover exact live truth, with a narrow bounded-startup adoption capability."""

        payload: dict[str, Any] = {
            "backfill_validation": None,
            "follower_reconcile": None,
            "containment_cycle": None,
            "auto_resume": None,
            "auto_resume_skipped": "",
            "recovery_mode": "unproven",
            "bounded_startup_adoption": allow_bounded_startup_adoption,
            "deferred_open_drain": None,
            "circuit_breaker": None,
            "containment_watchdog": None,
        }
        if self.execution_adapter is None:
            payload["auto_resume_skipped"] = "live recovery requires an execution adapter"
            return payload
        if not self._acquire_exchange_lease("source_websocket_recovery"):
            payload["auto_resume_skipped"] = "live recovery could not acquire the follower lease"
            return payload
        try:
            try:
                follower_snapshot = self.execution_adapter.reconcile()
            except Exception as exc:  # pragma: no cover - defensive runtime path
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"source websocket recovery follower reconcile failed: {exc}",
                )
                payload["follower_reconcile"] = {"error": str(exc)}
                payload["auto_resume_skipped"] = "fresh follower truth could not be proven"
                return payload
            self.store.append_reconcile_snapshot(follower_snapshot)
            exposed = {
                canonical_market_symbol(coin): position
                for coin, position in follower_snapshot.positions.items()
                if position.size != 0
            }
            payload["follower_reconcile"] = {
                "snapshot_id": follower_snapshot.snapshot_id,
                "account": follower_snapshot.account,
                "observed_ms": follower_snapshot.observed_ms,
                "positions": sorted(exposed),
                "open_orders": len(follower_snapshot.open_orders),
            }
            expected_account = self._effective_action_account().strip().lower()
            actual_account = follower_snapshot.account.strip().lower()
            if not expected_account or actual_account != expected_account:
                detail = (
                    "source websocket recovery follower account mismatch: "
                    f"expected {expected_account or '<unset>'}, got {actual_account or '<unset>'}"
                )
                self.safe_mode.trip(SafeModeReason.ACCOUNT_NOT_CONFIGURED, detail)
                payload["auto_resume_skipped"] = "follower account scope could not be proven"
                return payload
            was_exposed = bool(exposed)
            payload["recovery_mode"] = (
                "startup_bounded_adoption"
                if allow_bounded_startup_adoption
                else "exposed_containment"
                if was_exposed
                else "flat_bounded_resume"
            )
            allowed = {
                canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols
            }
            outside_scope = sorted(set(exposed) - allowed)
            if outside_scope:
                detail = (
                    "source websocket recovery found follower exposure outside the configured "
                    f"allowlist: {', '.join(outside_scope)}"
                )
                self.safe_mode.trip(SafeModeReason.MANUAL_INTERVENTION, detail)
                payload["auto_resume_skipped"] = "follower symbol scope could not be proven"
                return payload
            if follower_snapshot.open_orders and (
                not was_exposed or allow_bounded_startup_adoption
            ):
                detail = (
                    "source websocket startup recovery found follower open orders; exact "
                    "bounded-adoption admission could not be proven"
                    if allow_bounded_startup_adoption
                    else (
                        "source websocket flat recovery found follower open orders; exact flat "
                        "admission could not be proven"
                    )
                )
                self.safe_mode.trip(SafeModeReason.MANUAL_INTERVENTION, detail)
                payload["auto_resume_skipped"] = (
                    "startup follower order-free truth could not be proven"
                    if allow_bounded_startup_adoption
                    else "exact flat follower truth could not be proven"
                )
                return payload
            if not self._check_source_freshness(source_snapshot.observed_ms):
                payload["auto_resume_skipped"] = "fresh source truth expired during recovery"
                return payload
            if not self._check_follower_freshness(follower_snapshot.observed_ms):
                payload["auto_resume_skipped"] = "fresh follower truth expired during recovery"
                return payload
            if allow_bounded_startup_adoption:
                supervisor = self._validation_supervisor_decision()
                payload["validation_supervisor"] = to_jsonable(supervisor)
                if not supervisor.ok:
                    payload["auto_resume_skipped"] = (
                        "bounded startup adoption lacks current supervisor authority: "
                        f"{supervisor.detail}"
                    )
                    return payload
            circuit = self.circuit_breaker.check()
            persistent_circuit = self._persistent_circuit_breaker_decision()
            payload["circuit_breaker"] = {
                "runtime": to_jsonable(circuit),
                "persistent": to_jsonable(persistent_circuit),
            }
            if not circuit.ok or not persistent_circuit.ok:
                decision = circuit if not circuit.ok else persistent_circuit
                self.safe_mode.trip(decision.reason, decision.detail)
                payload["auto_resume_skipped"] = (
                    "circuit breaker did not permit unattended recovery"
                )
                return payload
            if self.config.ops.dead_man_policy == DeadManPolicy.WATCHDOG_FALLBACK:
                watchdog = self.containment_watchdog_status()
                payload["containment_watchdog"] = watchdog
                if not watchdog.get("ready"):
                    detail = (
                        "source websocket recovery requires a ready independent containment "
                        "watchdog under watchdog_fallback policy"
                    )
                    self.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, detail)
                    payload["auto_resume_skipped"] = (
                        "independent containment watchdog did not pass recovery admission"
                    )
                    return payload
            cleared = self._clear_after_exchange_reconcile(
                follower_snapshot,
                (
                    "unattended websocket recovery reconciled exact exposed-follower truth"
                    if was_exposed
                    else "unattended websocket recovery reconciled exact flat follower truth"
                ),
                clearance_auth_probe=False,
                expected_safe_mode_revision=expected_safe_mode_revision,
            )
            if not cleared:
                payload["auto_resume_skipped"] = (
                    "exact follower truth did not satisfy safe-mode clearance"
                )
                return payload
            report = self.preflight(auth_probe=False)
            if not report.passed:
                payload["auto_resume_skipped"] = (
                    "live recovery preflight failed after follower truth reconciliation"
                )
                return payload
            next_preflight: PreflightReport | None = report

            def recovery_cycle_runner() -> dict[str, Any]:
                nonlocal next_preflight
                if allow_bounded_startup_adoption:
                    supervisor = self._validation_supervisor_decision()
                    if not supervisor.ok:
                        return self._blocked_cycle_payload(
                            next_preflight or self.preflight(auth_probe=False),
                            supervisor.reason,
                            supervisor.detail,
                        )
                cycle_preflight = next_preflight or self.preflight(auth_probe=False)
                next_preflight = None
                if not cycle_preflight.passed:
                    return {
                        "preflight": to_jsonable(cycle_preflight),
                        "safe_mode": self._safe_mode_status(),
                        "intents": [],
                        "reports": [],
                    }
                try:
                    return self._run_once_with_lease(
                        cycle_preflight,
                        recovery_containment_only=(
                            was_exposed and not allow_bounded_startup_adoption
                        ),
                    )
                finally:
                    self._reset_signed_action_context()

            if allow_bounded_startup_adoption:
                cycle, deferred_open_drain = self._run_once_until_deferred_opens_drained(
                    cycle_runner=recovery_cycle_runner
                )
                payload["deferred_open_drain"] = deferred_open_drain
            else:
                cycle = recovery_cycle_runner()
            payload["containment_cycle"] = cycle
            payload["backfill_validation"] = self._validate_recovered_backfill(
                backfill,
                stage="disconnect_backfill",
                validation_result=cycle,
                reaction_scope=reaction_scope,
            )
            payload["auto_resume"] = {
                "cleared": not self.safe_mode.enabled,
                "safe_mode": self._safe_mode_status(),
                "result": cycle,
            }
            if self.safe_mode.enabled:
                payload["auto_resume_skipped"] = (
                    "containment validation did not prove a safe completed cycle"
                )
            return payload
        finally:
            self._reset_signed_action_context()
            self._release_exchange_lease("source_websocket_recovery")
            status = "recovered" if not self.safe_mode.enabled else "blocked"
            self.store.append_control_audit(
                control="source_websocket_unattended_recovery",
                status=status,
                detail=(
                    "live follower websocket recovery completed"
                    if status == "recovered"
                    else "live follower websocket recovery remained fail-closed"
                ),
                payload={
                    "source_wallet": self.config.source_wallet.lower(),
                    "action_account": self._effective_action_account().lower(),
                    "allowed_symbols": sorted(
                        canonical_market_symbol(symbol)
                        for symbol in self.config.risk.allowed_symbols
                    ),
                    "source_state_key": source_snapshot.state_key,
                    "source_planning_key": source_snapshot.planning_key,
                    "recovery_mode": payload.get("recovery_mode", "unproven"),
                    "safe_mode": self._safe_mode_status(),
                },
            )

    def _freeze_source_reaction_scope(self) -> dict[str, Any]:
        """Freeze the exact reaction obligations covered by the next truth cycle."""

        reaction_cutoff = self.store.source_reaction_high_watermark(
            source_wallet=self.config.source_wallet,
        )
        if reaction_cutoff is None:
            return {
                "complete": True,
                "through_reaction_rowid": None,
                "source_event_keys": [],
            }
        pending_count = self.store.unfinished_source_reaction_count(
            source_wallet=self.config.source_wallet,
            through_reaction_rowid=reaction_cutoff,
        )
        pending = self.store.pending_source_reaction_events(
            source_wallet=self.config.source_wallet,
            through_reaction_rowid=reaction_cutoff,
            limit=max(1, pending_count),
        )
        source_event_keys = [event.idempotency_key for event in pending]
        return {
            "complete": len(source_event_keys) == pending_count,
            "through_reaction_rowid": reaction_cutoff,
            "source_event_keys": source_event_keys,
        }

    def _validate_recovered_backfill(
        self,
        backfill: dict[str, Any],
        *,
        stage: str,
        validation_result: dict[str, Any] | None = None,
        reaction_scope: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        inserted = _backfill_report_int(backfill, "inserted")
        scope = self._freeze_source_reaction_scope() if reaction_scope is None else reaction_scope
        reaction_cutoff = scope.get("through_reaction_rowid")
        frozen_keys = tuple(
            dict.fromkeys(str(key) for key in scope.get("source_event_keys", []) if str(key))
        )
        if not scope.get("complete"):
            detail = f"{stage} reaction obligations could not be frozen completely"
            self.safe_mode.trip(SafeModeReason.MISSED_EVENT_GAP, detail)
            return {
                "stage": stage,
                "action": "failed",
                "inserted": inserted,
                "error": detail,
                "safe_mode": self._safe_mode_status(),
            }
        if not isinstance(reaction_cutoff, int) or reaction_cutoff <= 0 or not frozen_keys:
            return None
        pending_count = self.store.unfinished_source_reaction_count(
            source_wallet=self.config.source_wallet,
            through_reaction_rowid=reaction_cutoff,
        )
        legacy_count = self.store.legacy_source_reaction_count(
            source_wallet=self.config.source_wallet,
            through_reaction_rowid=reaction_cutoff,
        )
        pending = self.store.pending_source_reaction_events(
            source_wallet=self.config.source_wallet,
            through_reaction_rowid=reaction_cutoff,
            limit=max(1, len(frozen_keys)),
        )
        frozen_key_set = set(frozen_keys)
        pending = [event for event in pending if event.idempotency_key in frozen_key_set]
        pending_count = len(pending)
        if pending_count <= 0:
            return None
        ignored = [
            event for event in pending if not self._source_event_triggers_copy_validation(event)
        ]
        if ignored and legacy_count == 0:
            self.store.finish_source_reactions(
                [event.idempotency_key for event in ignored],
                status="ignored",
                outcome={
                    "stage": stage,
                    "reason": "event does not change copy exposure",
                    "skip_class": "informational_event",
                },
            )
        actionable = [
            event for event in pending if self._source_event_triggers_copy_validation(event)
        ]
        if not actionable and legacy_count == 0:
            return {
                "stage": stage,
                "action": "ignored",
                "inserted": inserted,
                "pending_reactions": pending_count,
                "ignored_reactions": len(ignored),
            }
        if legacy_count:
            actionable = pending
        actionable_keys = [event.idempotency_key for event in actionable]
        warnings = backfill.get("warnings") if isinstance(backfill, dict) else None
        if warnings:
            self.store.finish_source_reactions(
                actionable_keys,
                status="failed",
                outcome={"stage": stage, "warnings": warnings},
            )
            return {
                "stage": stage,
                "action": "skipped",
                "inserted": inserted,
                "pending_reactions": pending_count,
                "detail": "backfill warnings require manual reconcile before validation",
                "warnings": warnings,
            }
        self.safe_mode.refresh_from_store()
        if self.safe_mode.enabled:
            self.store.finish_source_reactions(
                actionable_keys,
                status="blocked",
                outcome={"stage": stage, "safe_mode": self._safe_mode_status()},
            )
            return {
                "stage": stage,
                "action": "skipped",
                "inserted": inserted,
                "pending_reactions": pending_count,
                "detail": "safe mode is active; recovered backfill validation is blocked",
                "safe_mode": self._safe_mode_status(),
            }
        claimed_keys = self.store.claim_source_reaction_keys(
            actionable_keys,
            include_processing=True,
            retry_due_ms=now_ms(),
        )
        actionable_keys = list(claimed_keys)
        if not actionable_keys:
            return {
                "stage": stage,
                "action": "ignored",
                "inserted": inserted,
                "pending_reactions": pending_count,
                "detail": "reaction obligations changed before validation claim",
            }
        deferred_open_drain: dict[str, Any] | None = None
        try:
            if validation_result is None:
                result, deferred_open_drain = self._run_once_until_deferred_opens_drained()
            else:
                result = validation_result
        except Exception as exc:  # pragma: no cover - defensive runtime path
            detail = f"{stage} validation failed after recovered source fills: {exc}"
            self.store.finish_source_reactions(
                actionable_keys,
                status="failed",
                outcome={"stage": stage, "error": str(exc)},
            )
            self.safe_mode.trip(SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE, detail)
            return {
                "stage": stage,
                "action": "failed",
                "inserted": inserted,
                "error": str(exc),
                "safe_mode": self._safe_mode_status(),
            }
        completed = self._source_reaction_run_completed(result)
        retry = self._hip3_liquidity_retry_payload(result)
        if completed:
            disposition = self._completed_source_reaction_disposition(result)
            actionable_key_set = set(actionable_keys)
            self._finish_completed_source_reaction_batch(
                [event for event in pending if event.idempotency_key in actionable_key_set],
                outcome={
                    "stage": stage,
                    "action": "run_once",
                    "source_event_key": actionable_keys[-1],
                    "source_event_keys": actionable_keys,
                    "result": result,
                    "deferred_open_drain": deferred_open_drain,
                },
                result=result,
            )
        else:
            self.store.finish_source_reactions(
                actionable_keys,
                status="blocked",
                outcome={
                    "stage": stage,
                    "result": result,
                    "deferred_open_drain": deferred_open_drain,
                    **({"retry": retry} if retry is not None else {}),
                },
            )
        if not completed:
            return {
                "stage": stage,
                "action": "skipped",
                "inserted": inserted,
                "pending_reactions": pending_count,
                "detail": "current-truth validation did not reach a safe completed cycle",
                "result": result,
                "deferred_open_drain": deferred_open_drain,
                **({"retry": retry} if retry is not None else {}),
            }
        return {
            "stage": stage,
            "action": "run_once",
            **disposition,
            "inserted": inserted,
            "pending_reactions": pending_count,
            "result": result,
            "deferred_open_drain": deferred_open_drain,
        }

    def _execution_cycle_completed(self, result: dict[str, Any]) -> bool:
        if result.get("startup_recovery") is not None:
            return False
        preflight = result.get("preflight")
        if not isinstance(preflight, dict) or preflight.get("passed") is not True:
            return False
        safe_mode = result.get("safe_mode")
        if not isinstance(safe_mode, dict) or safe_mode.get("enabled") is not False:
            return False
        if result.get("desired_state_committed") is True:
            return True
        finalization = result.get("execution_finalization")
        if (
            isinstance(finalization, dict)
            and finalization.get("status") == "actual_checkpoint_committed"
            and isinstance(finalization.get("checkpoint"), dict)
            and self._hip3_liquidity_deferred_intents(result)
        ):
            # A last-mile depth disappearance is a conclusive no-send. Fresh
            # follower truth was checkpointed even though the original target
            # remains pending for a later market-specific retry.
            return True
        if self._verified_all_noop_current_truth_result(result):
            return True
        if self._actual_checkpoint_completed_with_terminal_plan(result):
            # Fresh, order-free follower truth may differ from the source target
            # only by deltas the planner proved too small to dispatch. Treat the
            # reaction as complete only when every concrete action reached its
            # action-specific terminal success; the actual checkpoint remains
            # committed and the unattainable target is not promoted.
            return True
        desired_state = result.get("desired_state")
        if not isinstance(desired_state, dict):
            return False
        state_id = desired_state.get("state_id")
        return isinstance(state_id, str) and self.store.desired_state_is_committed(state_id)

    @classmethod
    def _verified_all_noop_current_truth_result(cls, result: dict[str, Any]) -> bool:
        desired_state = result.get("desired_state")
        intents = result.get("intents")
        reports = result.get("reports")
        finalization = result.get("execution_finalization")
        proof = result.get("all_noop_verification")
        if (
            result.get("desired_state_committed") is not False
            or result.get("deferred_intents") != []
            or not isinstance(desired_state, dict)
            or not isinstance(intents, list)
            or not intents
            or reports != []
            or not isinstance(finalization, dict)
            or finalization.get("status") != "committed_baseline_noop_verified"
            or finalization.get("committed_target") is not False
            or not isinstance(proof, dict)
            or proof.get("kind") != "all_noop_current_truth_v1"
            or proof.get("source_fresh") is not True
            or proof.get("follower_fresh") is not True
            or proof.get("open_order_count") != 0
            or proof.get("pending_intent_count") != 0
            or proof.get("unresolved_signed_action_attempt_count") != 0
            or proof.get("committed_checkpoint_match") is not True
            or proof.get("leverage_mutation_required") is not False
            or type(proof.get("verified_ms")) is not int
            or int(proof["verified_ms"]) <= 0
            or int(proof["verified_ms"]) > now_ms() + MAX_FUTURE_OBSERVATION_MS
        ):
            return False

        state_id = desired_state.get("state_id")
        checkpoint = finalization.get("checkpoint")
        if (
            not isinstance(state_id, str)
            or not state_id
            or finalization.get("target_state_id") != state_id
            or not isinstance(checkpoint, dict)
        ):
            return False

        liquidity_deferrals = cls._pending_planning_hip3_liquidity_deferrals(
            result.get("liquidity_deferred_intents"),
            desired_state=desired_state,
            observed_ms=int(proof["verified_ms"]),
        )
        if liquidity_deferrals is None:
            return False
        retry_deadline = (
            min(int(item["retry_not_before_ms"]) for item in liquidity_deferrals)
            if liquidity_deferrals
            else None
        )
        if (
            proof.get("hip3_liquidity_deferral_count") != len(liquidity_deferrals)
            or proof.get("hip3_liquidity_retry_not_before_ms") != retry_deadline
            or (
                bool(liquidity_deferrals) != (cls._hip3_liquidity_retry_payload(result) is not None)
            )
        ):
            return False

        noop_coins: set[str] = set()
        noop_intent_ids: set[str] = set()
        noop_cloids: set[str] = set()
        for intent in intents:
            if not isinstance(intent, dict):
                return False
            try:
                noop_coin = canonical_market_symbol(str(intent.get("coin") or ""))
                noop_size = parse_decimal(intent.get("size"))
            except (ArithmeticError, TypeError, ValueError):
                return False
            if (
                intent.get("action") != IntentAction.NOOP.value
                or intent.get("status") != IntentStatus.SKIPPED.value
                or intent.get("side") != "none"
                or noop_size != 0
                or intent.get("price") is not None
                or intent.get("reduce_only") is not False
                or not str(intent.get("reason") or "").startswith("delta below min size/notional:")
            ):
                return False
            intent_id = intent.get("intent_id")
            cloid = intent.get("cloid")
            if (
                not isinstance(intent_id, str)
                or not intent_id
                or not isinstance(cloid, str)
                or not cloid
                or intent_id in noop_intent_ids
                or cloid in noop_cloids
            ):
                return False
            noop_coins.add(noop_coin)
            noop_intent_ids.add(intent_id)
            noop_cloids.add(cloid)

        if any(
            item["intent"]["intent_id"] in noop_intent_ids
            or item["intent"]["cloid"] in noop_cloids
            or item["intent"]["coin"] in noop_coins
            for item in liquidity_deferrals
        ):
            return False

        differing_coins = cls._desired_checkpoint_differing_coins(
            desired_state.get("positions"),
            checkpoint.get("positions"),
        )
        if not differing_coins:
            return False
        return differing_coins.issubset(noop_coins)

    @classmethod
    def _actual_checkpoint_completed_with_terminal_plan(
        cls,
        result: dict[str, Any],
    ) -> bool:
        finalization = result.get("execution_finalization")
        desired_state = result.get("desired_state")
        intents = result.get("intents")
        reports = result.get("reports")
        if (
            result.get("desired_state_committed") is not False
            or not isinstance(finalization, dict)
            or finalization.get("status") != "actual_checkpoint_committed"
            or finalization.get("committed_target") is not False
            or not isinstance(desired_state, dict)
            or not isinstance(intents, list)
            or not intents
            or not isinstance(reports, list)
        ):
            return False

        state_id = desired_state.get("state_id")
        checkpoint = finalization.get("checkpoint")
        if (
            not isinstance(state_id, str)
            or not state_id
            or finalization.get("target_state_id") != state_id
            or not isinstance(checkpoint, dict)
        ):
            return False

        noop_coins: set[str] = set()
        for intent in intents:
            if not isinstance(intent, dict):
                return False
            intent_id = intent.get("intent_id")
            cloid = intent.get("cloid")
            action = str(intent.get("action") or "")
            if not isinstance(intent_id, str) or not intent_id:
                return False
            if not isinstance(cloid, str) or not cloid:
                return False
            matching_reports = [
                report
                for report in reports
                if isinstance(report, dict)
                and report.get("intent_id") == intent_id
                and report.get("cloid") == cloid
            ]
            statuses = [str(report.get("status") or "") for report in matching_reports]
            terminal = matching_reports[-1] if matching_reports else None

            if action in {
                IntentAction.OPEN.value,
                IntentAction.REDUCE.value,
                IntentAction.CLOSE.value,
            }:
                if (
                    terminal is None
                    or any(
                        status not in {IntentStatus.ACKED.value, IntentStatus.FILLED.value}
                        for status in statuses
                    )
                    or terminal.get("status") != IntentStatus.FILLED.value
                ):
                    return False
                try:
                    canonical_market_symbol(str(intent.get("coin") or ""))
                except ValueError:
                    return False
                continue
            if action == IntentAction.CANCEL.value:
                if (
                    terminal is None
                    or any(
                        status not in {IntentStatus.ACKED.value, IntentStatus.CANCELED.value}
                        for status in statuses
                    )
                    or terminal.get("status") != IntentStatus.CANCELED.value
                ):
                    return False
                try:
                    canonical_market_symbol(str(intent.get("coin") or ""))
                except ValueError:
                    return False
                continue
            if action != IntentAction.NOOP.value:
                return False

            try:
                noop_size = parse_decimal(intent.get("size"))
                noop_coin = canonical_market_symbol(str(intent.get("coin") or ""))
            except (ArithmeticError, TypeError, ValueError):
                return False
            if (
                intent.get("status") != IntentStatus.SKIPPED.value
                or intent.get("side") != "none"
                or noop_size != 0
                or intent.get("price") is not None
                or intent.get("reduce_only") is not False
                or not str(intent.get("reason") or "").startswith("delta below min size/notional:")
                or any(status != IntentStatus.SKIPPED.value for status in statuses)
                or any(report.get("exchange_status") != "skipped" for report in matching_reports)
            ):
                return False
            noop_coins.add(noop_coin)

        differing_coins = cls._desired_checkpoint_differing_coins(
            desired_state.get("positions"),
            checkpoint.get("positions"),
        )
        if not differing_coins:
            return False
        quantized_action_coins = cls._action_quantization_residual_coins(
            desired_state.get("positions"),
            checkpoint.get("positions"),
            intents,
        )
        return differing_coins.issubset(noop_coins | quantized_action_coins)

    @staticmethod
    def _action_quantization_residual_coins(
        desired_positions: Any,
        checkpoint_positions: Any,
        intents: list[Any],
    ) -> set[str]:
        """Identify exact desired targets reached to within one dispatched venue lot.

        Desired state intentionally retains sub-lot tracking debt.  A terminal filled
        order therefore proves the executable projection, not byte-for-byte equality
        with the unquantized target.  This helper accepts only a same-direction residual
        strictly smaller than the lot implied by the persisted wire intent; larger or
        opposite-side discrepancies remain incomplete.
        """

        def sizes(payload: Any) -> dict[str, tuple[Decimal, int | None]] | None:
            if not isinstance(payload, dict):
                return None
            result: dict[str, tuple[Decimal, int | None]] = {}
            for raw_coin, raw_position in payload.items():
                if not isinstance(raw_position, dict):
                    return None
                try:
                    coin = canonical_market_symbol(str(raw_position.get("coin") or raw_coin))
                    size = parse_decimal(raw_position.get("size"))
                    leverage_raw = raw_position.get("leverage")
                    leverage = int(leverage_raw) if leverage_raw is not None else None
                except (ArithmeticError, TypeError, ValueError):
                    return None
                if coin in result:
                    return None
                result[coin] = (size, leverage)
            return result

        desired = sizes(desired_positions)
        actual = sizes(checkpoint_positions)
        if desired is None or actual is None:
            return set()
        lots: dict[str, Decimal] = {}
        for intent in intents:
            if not isinstance(intent, dict) or intent.get("action") not in {
                IntentAction.OPEN.value,
                IntentAction.REDUCE.value,
                IntentAction.CLOSE.value,
            }:
                continue
            try:
                coin = canonical_market_symbol(str(intent.get("coin") or ""))
                size = abs(parse_decimal(intent.get("size")))
            except (ArithmeticError, TypeError, ValueError):
                continue
            if size <= 0:
                continue
            exponent = size.as_tuple().exponent
            if not size.is_finite() or not isinstance(exponent, int):
                continue
            lot = Decimal(1).scaleb(exponent)
            prior = lots.get(coin)
            lots[coin] = lot if prior is None else min(prior, lot)

        accepted: set[str] = set()
        for coin, lot in lots.items():
            desired_size, desired_leverage = desired.get(coin, (Decimal("0"), None))
            actual_size, actual_leverage = actual.get(coin, (Decimal("0"), None))
            if (
                desired_leverage is not None
                and actual_size != 0
                and actual_leverage != desired_leverage
            ):
                continue
            if desired_size != 0 and actual_size != 0 and (desired_size > 0) != (actual_size > 0):
                continue
            if abs(desired_size - actual_size) < lot:
                accepted.add(coin)
        return accepted

    @staticmethod
    def _desired_checkpoint_differing_coins(
        desired_positions: Any,
        checkpoint_positions: Any,
    ) -> set[str] | None:
        def normalized_positions(payload: Any) -> dict[str, tuple[Decimal, int | None]] | None:
            if not isinstance(payload, dict):
                return None
            normalized: dict[str, tuple[Decimal, int | None]] = {}
            for raw_coin, raw_position in payload.items():
                if not isinstance(raw_position, dict):
                    return None
                try:
                    coin = canonical_market_symbol(str(raw_position.get("coin") or raw_coin))
                    size = parse_decimal(raw_position.get("size"))
                    raw_leverage = raw_position.get("leverage")
                    leverage = int(raw_leverage) if raw_leverage is not None else None
                except (ArithmeticError, TypeError, ValueError):
                    return None
                if coin in normalized:
                    return None
                if size != 0:
                    normalized[coin] = (size, leverage)
            return normalized

        desired = normalized_positions(desired_positions)
        checkpoint = normalized_positions(checkpoint_positions)
        if desired is None or checkpoint is None:
            return None
        differing: set[str] = set()
        for coin in set(desired) | set(checkpoint):
            target = desired.get(coin)
            actual = checkpoint.get(coin)
            if target is None or actual is None or target[0] != actual[0]:
                differing.add(coin)
                continue
            if target[1] is not None and target[1] != actual[1]:
                differing.add(coin)
        return differing

    @staticmethod
    def _pending_planning_hip3_liquidity_deferrals(
        raw_deferrals: Any,
        *,
        desired_state: dict[str, Any],
        observed_ms: int,
    ) -> list[dict[str, Any]] | None:
        """Validate the only paced deferrals allowed beside a no-send plan."""

        if not isinstance(raw_deferrals, list):
            return None
        expected_state_id = desired_state.get("state_id")
        expected_source_event_key = desired_state.get("source_event_key")
        expected_mode = desired_state.get("mode")
        if (
            not isinstance(expected_state_id, str)
            or not expected_state_id
            or not isinstance(expected_source_event_key, str)
            or not expected_source_event_key
            or expected_mode not in {Mode.TESTNET.value, Mode.LIVE.value}
        ):
            return None

        validated: list[dict[str, Any]] = []
        seen_intent_ids: set[str] = set()
        seen_cloids: set[str] = set()
        seen_coins: set[str] = set()
        for item in raw_deferrals:
            if not isinstance(item, dict):
                return None
            intent = item.get("intent")
            blockers = item.get("blockers")
            deadline = item.get("retry_not_before_ms")
            stage = item.get("stage")
            if (
                not isinstance(intent, dict)
                or not isinstance(blockers, list)
                or not blockers
                or any(not isinstance(blocker, str) or not blocker.strip() for blocker in blockers)
                or type(deadline) is not int
                or deadline <= observed_ms
                or deadline > observed_ms + HIP3_LIQUIDITY_RETRY_MS
                or stage not in HIP3_PLANNING_LIQUIDITY_DEFERRAL_STAGES
            ):
                return None

            raw_coin = intent.get("coin")
            try:
                coin = canonical_market_symbol(str(raw_coin or ""))
                size = parse_decimal(intent.get("size"))
                price = parse_decimal(intent.get("price"))
            except (ArithmeticError, TypeError, ValueError):
                return None
            intent_id = intent.get("intent_id")
            cloid = intent.get("cloid")
            if (
                raw_coin != coin
                or not market_dex(coin)
                or not isinstance(intent_id, str)
                or not intent_id
                or not isinstance(cloid, str)
                or not cloid
                or intent_id in seen_intent_ids
                or cloid in seen_cloids
                or coin in seen_coins
                or intent.get("action") != IntentAction.OPEN.value
                or intent.get("reduce_only") is not False
                or intent.get("status") != IntentStatus.PENDING.value
                or intent.get("side") not in {"buy", "sell"}
                or not size.is_finite()
                or size <= 0
                or not price.is_finite()
                or price <= 0
                or intent.get("mode") != expected_mode
                or intent.get("desired_state_id") != expected_state_id
                or intent.get("source_event_key") != expected_source_event_key
            ):
                return None
            seen_intent_ids.add(intent_id)
            seen_cloids.add(cloid)
            seen_coins.add(coin)
            validated.append(item)
        return validated

    def _verified_all_noop_current_truth_cycle(
        self,
        *,
        preflight: PreflightReport,
        source_positions: dict[str, Position],
        source_observed_ms: int,
        result: CopyResult,
        follower_positions: dict[str, Position],
        follower_open_orders: list[OpenOrder],
        follower_observed_ms: int,
        manual_ok: bool,
        deferred_intents: list[FollowerIntent],
        liquidity_deferred_intents: list[Hip3LiquidityDeferral],
    ) -> dict[str, Any] | None:
        """Consume a strict all-NOOP plan without creating replayable attempts."""

        if (
            self.config.mode not in {Mode.TESTNET, Mode.LIVE}
            or not manual_ok
            or self.safe_mode.enabled
            or result.blockers
            or deferred_intents
            or follower_open_orders
            or not result.intents
        ):
            return None

        noop_coins: set[str] = set()
        noop_intent_ids: set[str] = set()
        noop_cloids: set[str] = set()
        for intent in result.intents:
            if (
                intent.action != IntentAction.NOOP
                or intent.status != IntentStatus.SKIPPED
                or intent.side != "none"
                or intent.size != 0
                or intent.price is not None
                or intent.reduce_only
                or not intent.reason.startswith("delta below min size/notional:")
                or intent.execution_proof
            ):
                return None
            try:
                noop_coins.add(canonical_market_symbol(intent.coin))
            except ValueError:
                return None
            if intent.intent_id in noop_intent_ids or intent.cloid in noop_cloids:
                return None
            noop_intent_ids.add(intent.intent_id)
            noop_cloids.add(intent.cloid)

        if any(not isinstance(item, Hip3LiquidityDeferral) for item in liquidity_deferred_intents):
            return None
        verification_ms = now_ms()
        serialized_liquidity_deferrals = to_jsonable(liquidity_deferred_intents)
        serialized_desired_state = to_jsonable(result.desired_state)
        validated_liquidity_deferrals = self._pending_planning_hip3_liquidity_deferrals(
            serialized_liquidity_deferrals,
            desired_state=serialized_desired_state,
            observed_ms=verification_ms,
        )
        if validated_liquidity_deferrals is None or any(
            item["intent"]["intent_id"] in noop_intent_ids
            or item["intent"]["cloid"] in noop_cloids
            or item["intent"]["coin"] in noop_coins
            for item in validated_liquidity_deferrals
        ):
            return None

        source_fresh = self._check_source_freshness(source_observed_ms)
        follower_fresh = self._check_follower_freshness(follower_observed_ms)
        if not source_fresh or not follower_fresh or self.safe_mode.enabled:
            return None

        pending_intent_count = self.store.pending_intent_count(self.config.mode)
        unresolved_signed_action_attempt_count = self.store.unresolved_signed_action_attempt_count(
            self.config.mode,
            account=self._effective_action_account(),
            network=("mainnet" if self.config.mode == Mode.LIVE else "testnet"),
        )
        if pending_intent_count or unresolved_signed_action_attempt_count:
            return None

        committed_positions = self.store.latest_desired_positions(
            self.config.mode,
            source_wallet=self.config.source_wallet,
            action_account=self._effective_action_account() or "local-paper-shadow",
            source_network=self.config.resolved_source_network.value,
            committed_only=True,
        )
        if committed_positions is None or not self._positions_match_exact(
            committed_positions,
            follower_positions,
        ):
            return None

        differing_coins = self._desired_checkpoint_differing_coins(
            to_jsonable(result.desired_state.positions),
            to_jsonable(follower_positions),
        )
        if not differing_coins or not differing_coins.issubset(noop_coins):
            return None

        try:
            follower_by_coin = {
                canonical_market_symbol(coin): position
                for coin, position in follower_positions.items()
                if position.size != 0
            }
            leverage_mutation_required = any(
                target.size != 0
                and (current := follower_by_coin.get(canonical_market_symbol(coin))) is not None
                and current.size != 0
                and target.leverage is not None
                and target.leverage != current.leverage
                for coin, target in result.desired_state.positions.items()
            )
        except ValueError:
            return None
        if leverage_mutation_required:
            return None

        proof = {
            "kind": "all_noop_current_truth_v1",
            "verified_ms": verification_ms,
            "source_fresh": source_fresh,
            "follower_fresh": follower_fresh,
            "open_order_count": len(follower_open_orders),
            "pending_intent_count": pending_intent_count,
            "unresolved_signed_action_attempt_count": (unresolved_signed_action_attempt_count),
            "committed_checkpoint_match": True,
            "leverage_mutation_required": leverage_mutation_required,
            "noop_intent_ids": [intent.intent_id for intent in result.intents],
            "hip3_liquidity_deferral_count": len(validated_liquidity_deferrals),
            "hip3_liquidity_retry_not_before_ms": (
                min(int(item["retry_not_before_ms"]) for item in validated_liquidity_deferrals)
                if validated_liquidity_deferrals
                else None
            ),
        }
        self.store.append_control_audit(
            control="all_noop_current_truth",
            status="verified_no_send",
            detail=(
                "fresh follower truth still matches the committed checkpoint; "
                + (
                    "all remaining target deltas are below the dispatch minimum and "
                    f"{len(validated_liquidity_deferrals)} HIP-3 liquidity deferral(s) "
                    "remain on paced retry"
                    if validated_liquidity_deferrals
                    else "all remaining target deltas are below the dispatch minimum"
                )
            ),
            payload={
                "target_state_id": result.desired_state.state_id,
                "source_event_key": result.desired_state.source_event_key,
                **proof,
            },
        )
        return {
            "preflight": to_jsonable(preflight),
            "source_positions": to_jsonable(source_positions),
            "desired_state": to_jsonable(result.desired_state),
            "desired_state_committed": False,
            "reconciled_checkpoint": None,
            "intents": to_jsonable(result.intents),
            "deferred_intents": [],
            "liquidity_deferred_intents": serialized_liquidity_deferrals,
            "reports": [],
            "post_action_reconcile": {
                "positions": to_jsonable(follower_positions),
                "open_orders": [],
                "observed_ms": follower_observed_ms,
                "source": "fresh pre-action follower truth; no signed action required",
            },
            "execution_finalization": {
                "status": "committed_baseline_noop_verified",
                "target_state_id": result.desired_state.state_id,
                "committed_target": False,
                "checkpoint": {"positions": to_jsonable(follower_positions)},
            },
            "all_noop_verification": proof,
            "safe_mode": self._safe_mode_status(),
        }

    def _source_reaction_run_completed(self, result: dict[str, Any]) -> bool:
        return (
            self._execution_cycle_completed(result)
            and not self._deferred_exposure_increasing_intents(result)
            and not self._hip3_liquidity_deferred_intents(result)
        )

    @classmethod
    def _hip3_liquidity_retry_payload(
        cls,
        result: dict[str, Any],
    ) -> dict[str, Any] | None:
        rows = cls._hip3_liquidity_deferred_intents(result)
        if not rows:
            return None
        deadlines: list[int] = []
        coins: list[str] = []
        for item in rows:
            try:
                deadline = int(item.get("retry_not_before_ms") or 0)
            except (TypeError, ValueError):
                continue
            if deadline > 0:
                deadlines.append(deadline)
            intent = item.get("intent")
            coin = str(intent.get("coin") or "") if isinstance(intent, dict) else ""
            if coin and coin not in coins:
                coins.append(coin)
        if not deadlines:
            return None
        return {
            "class": "hip3_liquidity",
            "disposition": "deferred",
            "retry_not_before_ms": min(deadlines),
            "retry_interval_ms": HIP3_LIQUIDITY_RETRY_MS,
            "coins": sorted(coins),
            "deferral_count": len(rows),
        }

    def _ensure_startup_liquidity_retry_obligation(
        self,
        result: dict[str, Any],
        deferred_open_drain: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Persist a paced retry even before the websocket emits its first event."""

        retry = self._hip3_liquidity_retry_payload(result)
        if retry is None:
            return None
        desired = result.get("desired_state")
        source_signal = (
            str(desired.get("source_event_key") or "") if isinstance(desired, dict) else ""
        )
        observed = now_ms()
        existing = [
            event
            for event in self.store.pending_source_reaction_events(
                source_wallet=self.config.source_wallet
            )
            if self._source_event_subtype(event) == "internal_hip3_liquidity_retry"
            and event.payload.get("internal_retry_obligation") is True
        ]
        event_key = (
            existing[0].idempotency_key
            if existing
            else deterministic_cloid(
                "startup-hip3-liquidity-retry",
                self.config.source_wallet.lower(),
                source_signal,
                retry["coins"],
                retry["retry_not_before_ms"],
            )
        )
        event = SourceEvent(
            idempotency_key=event_key,
            event_type=SourceEventType.POSITION,
            source_wallet=self.config.source_wallet.lower(),
            exchange_ts_ms=observed,
            observed_ts_ms=observed,
            payload={
                "event_subtype": "internal_hip3_liquidity_retry",
                "event_count": 1,
                "copy_signal_key": source_signal or event_key,
                "internal_retry_obligation": True,
            },
        )
        inserted = (
            self.store.append_source_event(event, reaction_required=True) if not existing else False
        )
        superseded_keys = [item.idempotency_key for item in existing[1:]]
        if superseded_keys:
            self.store.finish_source_reactions(
                superseded_keys,
                status="ignored",
                outcome={
                    "reason": "superseded by the current startup HIP-3 liquidity retry",
                    "skip_class": "duplicate_internal_liquidity_retry",
                    "superseded_by": event_key,
                },
            )
        updated = self.store.finish_source_reactions(
            [event_key],
            status="blocked",
            outcome={
                "source_event_key": event_key,
                "action": "run_once",
                "detail": "startup HIP-3 liquidity is deferred for a fresh-book retry",
                "result": result,
                "deferred_open_drain": deferred_open_drain,
                "retry": retry,
            },
        )
        return {
            "source_event_key": event_key,
            "inserted": inserted,
            "outcome_updated": updated == 1,
            "reused_existing": bool(existing),
            "superseded_duplicate_count": len(superseded_keys),
            "retry": retry,
        }

    @staticmethod
    def _completed_source_reaction_disposition(result: dict[str, Any]) -> dict[str, Any]:
        """Describe a completed reaction without overstating a no-op as a copy.

        A completed planning cycle proves that current truth was handled safely. It does not by
        itself prove that a follower action was copied. ``copied`` therefore requires a concrete
        non-NOOP intent and a matching positive execution report; otherwise the completed reaction
        is an explicit, reasoned no-op/skip.
        """

        raw_intents = result.get("intents")
        intents = raw_intents if isinstance(raw_intents, list) else []
        concrete_by_intent_id: dict[str, dict[str, Any]] = {}
        concrete_by_cloid: dict[str, dict[str, Any]] = {}
        planner_reasons: list[str] = []
        for item in intents:
            if not isinstance(item, dict):
                continue
            reason = str(item.get("reason") or "").strip()
            if reason and reason not in planner_reasons:
                planner_reasons.append(reason)
            action = str(item.get("action") or "").strip().lower()
            if action not in {
                IntentAction.OPEN.value,
                IntentAction.REDUCE.value,
                IntentAction.CLOSE.value,
            }:
                continue
            reduce_only = item.get("reduce_only") is True
            if (action == IntentAction.OPEN.value and reduce_only) or (
                action in {IntentAction.REDUCE.value, IntentAction.CLOSE.value} and not reduce_only
            ):
                continue
            try:
                size = parse_decimal(item.get("size"))
            except (TypeError, ValueError):
                continue
            if size is None or not size.is_finite() or size <= 0:
                continue
            intent_id = str(item.get("intent_id") or "").strip()
            cloid = str(item.get("cloid") or "").strip().lower()
            if intent_id:
                concrete_by_intent_id[intent_id] = item
            if cloid:
                concrete_by_cloid[cloid] = item

        raw_reports = result.get("reports")
        reports = raw_reports if isinstance(raw_reports, list) else []
        execution_evidence: list[dict[str, Any]] = []
        for report in reports:
            if not isinstance(report, dict):
                continue
            status = str(report.get("status") or "").strip().lower()
            exchange_status = str(report.get("exchange_status") or "").strip().lower()
            # SENT/ACKED can still become rejected, canceled, or never filled. A completed
            # reaction may be called copied only when the durable report proves a positive fill.
            if status != IntentStatus.FILLED.value:
                continue
            raw_filled_size = report.get("filled_size")
            report_payload = report.get("payload")
            if raw_filled_size is None and isinstance(report_payload, dict):
                raw_filled_size = report_payload.get("filled_size")
            try:
                filled_size = parse_decimal(raw_filled_size)
            except (TypeError, ValueError):
                continue
            if filled_size is None or not filled_size.is_finite() or filled_size <= 0:
                continue
            intent_id = str(report.get("intent_id") or "").strip()
            cloid = str(report.get("cloid") or "").strip().lower()
            intent = concrete_by_intent_id.get(intent_id) or concrete_by_cloid.get(cloid)
            if intent is None:
                continue
            evidence = {
                "intent_id": str(intent.get("intent_id") or intent_id),
                "cloid": str(intent.get("cloid") or cloid),
                "action": str(intent.get("action") or ""),
                "coin": str(intent.get("coin") or ""),
                "side": str(intent.get("side") or ""),
                "reduce_only": intent.get("reduce_only") is True,
                "execution_status": status,
                "exchange_status": exchange_status,
                "filled_size": str(filled_size),
                "report_id": str(report.get("report_id") or ""),
            }
            if evidence not in execution_evidence:
                execution_evidence.append(evidence)

        if execution_evidence:
            return {
                "disposition": "copied",
                "reason": (
                    f"{len(execution_evidence)} concrete follower action(s) have matching "
                    "positive execution reports"
                ),
                "execution_evidence": execution_evidence,
            }

        desired_state = result.get("desired_state")
        if isinstance(desired_state, dict):
            reason = str(desired_state.get("reason") or "").strip()
            if reason and reason not in planner_reasons:
                planner_reasons.append(reason)
        planner_reason = "; ".join(planner_reasons[:3])
        reason = "current-truth validation completed with no concrete follower action required"
        if planner_reason:
            reason += f": {planner_reason}"
        return {
            "disposition": "skipped",
            "reason": reason,
            "execution_evidence": [],
            "skip_class": "current_truth_already_converged",
        }

    @staticmethod
    def _source_fill_coin_directions(event: SourceEvent) -> set[tuple[str, str]]:
        """Return exact coin/direction pairs from one durable source fill event."""

        if event.event_type != SourceEventType.FILL:
            return set()
        pairs: set[tuple[str, str]] = set()

        def walk(value: Any) -> None:
            if isinstance(value, list):
                for item in value:
                    walk(item)
                return
            if not isinstance(value, dict):
                return
            raw_coin = value.get("coin")
            raw_direction = value.get("dir") or value.get("direction")
            if isinstance(raw_coin, str) and isinstance(raw_direction, str):
                try:
                    coin = canonical_market_symbol(raw_coin)
                except (MarketIdentityError, ValueError):
                    pass
                else:
                    direction = " ".join(raw_direction.strip().lower().split())
                    if direction:
                        pairs.add((coin, direction))
            for child in value.values():
                walk(child)

        walk(event.payload)
        return pairs

    @staticmethod
    def _execution_source_direction(evidence: dict[str, Any]) -> str:
        action = str(evidence.get("action") or "").strip().lower()
        side = str(evidence.get("side") or "").strip().lower()
        if action == IntentAction.OPEN.value:
            return "open long" if side == "buy" else "open short" if side == "sell" else ""
        if action in {IntentAction.REDUCE.value, IntentAction.CLOSE.value}:
            return "close long" if side == "sell" else "close short" if side == "buy" else ""
        return ""

    def _source_event_key_for_execution(
        self,
        events: list[SourceEvent],
        evidence: dict[str, Any],
        *,
        fallback_key: str,
    ) -> str:
        try:
            coin = canonical_market_symbol(str(evidence.get("coin") or ""))
        except (MarketIdentityError, ValueError):
            return fallback_key
        direction = self._execution_source_direction(evidence)
        if not direction:
            return fallback_key
        matches = [
            event
            for event in events
            if (coin, direction) in self._source_fill_coin_directions(event)
        ]
        if not matches:
            return fallback_key
        return max(
            matches,
            key=lambda event: (event.exchange_ts_ms, event.observed_ts_ms),
        ).idempotency_key

    def _finish_completed_source_reaction_batch(
        self,
        events: list[SourceEvent],
        *,
        outcome: dict[str, Any],
        result: dict[str, Any],
    ) -> int:
        """Persist one verified disposition per event in a coalesced current-truth cycle."""

        keys = list(dict.fromkeys(event.idempotency_key for event in events))
        if not keys:
            raise ValueError("completed source reaction batch has no durable event keys")
        disposition = self._completed_source_reaction_disposition(result)
        completed_outcome = {**outcome, **disposition}
        if disposition["disposition"] != "copied":
            return self.store.finish_source_reactions(
                keys,
                status="completed",
                outcome=completed_outcome,
            )

        fallback_key = str(outcome.get("source_event_key") or "")
        if fallback_key not in keys:
            fallback_key = keys[-1]
        evidence_by_key: dict[str, list[dict[str, Any]]] = {}
        for evidence in disposition["execution_evidence"]:
            source_key = self._source_event_key_for_execution(
                events,
                evidence,
                fallback_key=fallback_key,
            )
            evidence_by_key.setdefault(source_key, []).append(evidence)

        finished = 0
        for source_key, execution_evidence in evidence_by_key.items():
            finished += self.store.finish_source_reactions(
                [source_key],
                status="completed",
                outcome={
                    **completed_outcome,
                    "reason": (
                        f"{len(execution_evidence)} concrete follower action(s) have matching "
                        "positive execution reports"
                    ),
                    "execution_evidence": execution_evidence,
                },
            )

        copied_keys = set(evidence_by_key)
        coalesced_keys = [key for key in keys if key not in copied_keys]
        if coalesced_keys:
            finished += self.store.finish_source_reactions(
                coalesced_keys,
                status="completed",
                outcome={
                    **outcome,
                    "disposition": "skipped",
                    "reason": (
                        "source event was coalesced into the same fresh current-truth cycle; "
                        "no separate follower action was required"
                    ),
                    "execution_evidence": [],
                    "skip_class": "current_truth_already_converged",
                    "coalesced_into_source_event_keys": sorted(copied_keys),
                },
            )
        return finished

    @staticmethod
    def _deferred_exposure_increasing_intents(
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        deferred = result.get("deferred_intents")
        if not isinstance(deferred, list):
            return []
        opens: list[dict[str, Any]] = []
        for item in deferred:
            if not isinstance(item, dict):
                continue
            if item.get("action") != IntentAction.OPEN.value or bool(item.get("reduce_only")):
                continue
            try:
                size = parse_decimal(item.get("size"))
            except (TypeError, ValueError):
                # A malformed deferred OPEN must keep the source reaction incomplete.
                opens.append(item)
                continue
            if size is None or not size.is_finite() or size > 0:
                opens.append(item)
        return opens

    @staticmethod
    def _hip3_liquidity_deferred_intents(
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        raw = result.get("liquidity_deferred_intents")
        rows = raw if isinstance(raw, list) else []
        deferred: list[dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            intent = item.get("intent")
            if not isinstance(intent, dict):
                continue
            if intent.get("action") not in {
                IntentAction.OPEN.value,
                IntentAction.REDUCE.value,
                IntentAction.CLOSE.value,
            }:
                continue
            try:
                size = parse_decimal(intent.get("size"))
            except (ArithmeticError, TypeError, ValueError):
                continue
            if size <= 0:
                continue
            deferred.append(item)
        return deferred

    @classmethod
    def _liquidity_deferred_exposure_increasing_intents(
        cls,
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        return [
            item
            for item in cls._hip3_liquidity_deferred_intents(result)
            if isinstance(item.get("intent"), dict)
            and item["intent"].get("action") == IntentAction.OPEN.value
            and not bool(item["intent"].get("reduce_only"))
        ]

    @classmethod
    def _deferred_open_drain_cycle_summary(
        cls,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        intents = result.get("intents")
        intent_rows = (
            [item for item in intents if isinstance(item, dict)]
            if isinstance(intents, list)
            else []
        )
        deferred = cls._deferred_exposure_increasing_intents(result)
        liquidity_deferred = cls._hip3_liquidity_deferred_intents(result)
        safe_mode = result.get("safe_mode")
        return {
            "desired_state_id": (
                result.get("desired_state", {}).get("state_id")
                if isinstance(result.get("desired_state"), dict)
                else ""
            ),
            "desired_state_committed": result.get("desired_state_committed") is True,
            "active_reduction_coins": [
                str(item.get("coin") or "")
                for item in intent_rows
                if item.get("action") in {IntentAction.CLOSE.value, IntentAction.REDUCE.value}
                and bool(item.get("reduce_only"))
            ],
            "active_open_coins": [
                str(item.get("coin") or "")
                for item in intent_rows
                if item.get("action") == IntentAction.OPEN.value
                and not bool(item.get("reduce_only"))
            ],
            "deferred_open_coins": [str(item.get("coin") or "") for item in deferred],
            "liquidity_deferred_open_coins": [
                str(item.get("intent", {}).get("coin") or "")
                for item in liquidity_deferred
                if item.get("intent", {}).get("action") == IntentAction.OPEN.value
                and not bool(item.get("intent", {}).get("reduce_only"))
            ],
            "safe_mode": safe_mode if isinstance(safe_mode, dict) else {},
        }

    def _run_once_until_deferred_opens_drained(
        self,
        *,
        cycle_runner: Callable[[], dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Run bounded exchange cycles until staged OPEN/add work is safely drained."""

        run_cycle = cycle_runner or self.run_once
        previous_cooldown = self._active_drain_liquidity_cooldown_coins
        self._active_drain_liquidity_cooldown_coins = set()
        cycle_summaries: list[dict[str, Any]] = []
        seen_deferred: set[tuple[tuple[str, str, str, str], ...]] = set()
        max_cycles = max(1, len(self.config.risk.allowed_symbols) + 2)
        status = "drained"
        try:
            result = run_cycle()
            while True:
                cycle_summaries.append(self._deferred_open_drain_cycle_summary(result))
                deferred = self._deferred_exposure_increasing_intents(result)
                liquidity_deferred = self._hip3_liquidity_deferred_intents(result)
                self._active_drain_liquidity_cooldown_coins.update(
                    canonical_market_symbol(str(item.get("intent", {}).get("coin") or ""))
                    for item in liquidity_deferred
                    if isinstance(item.get("intent"), dict)
                    and item.get("intent", {}).get("action") == IntentAction.OPEN.value
                    and not bool(item.get("intent", {}).get("reduce_only"))
                    and str(item.get("intent", {}).get("coin") or "")
                )
                if not deferred:
                    status = "drained_with_liquidity_deferrals" if liquidity_deferred else "drained"
                    break
                if not self._execution_cycle_completed(result):
                    status = "cycle_incomplete"
                    break
                if len(cycle_summaries) >= max_cycles:
                    detail = "deferred exposure-increasing intent drain exceeded its bounded cycle budget"
                    self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                    result = {**result, "safe_mode": self._safe_mode_status()}
                    status = "cycle_budget_exhausted"
                    break

                fingerprint = tuple(
                    sorted(
                        (
                            str(item.get("intent_id") or ""),
                            str(item.get("coin") or ""),
                            str(item.get("side") or ""),
                            str(item.get("size") or ""),
                        )
                        for item in deferred
                    )
                )
                if fingerprint in seen_deferred:
                    detail = "deferred exposure-increasing intent drain made no progress"
                    self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                    result = {**result, "safe_mode": self._safe_mode_status()}
                    status = "stalled"
                    break
                seen_deferred.add(fingerprint)

                self.safe_mode.refresh_from_store()
                if self.safe_mode.enabled:
                    status = "safe_mode"
                    break
                if self.store.pending_intent_count(self.config.mode) > 0:
                    status = "pending_intent"
                    break
                result = run_cycle()
        finally:
            cooldown_coins = sorted(self._active_drain_liquidity_cooldown_coins)
            self._active_drain_liquidity_cooldown_coins = previous_cooldown

        return result, {
            "status": status,
            "cycle_count": len(cycle_summaries),
            "additional_cycle_count": max(0, len(cycle_summaries) - 1),
            "max_cycle_count": max_cycles,
            "cycles": cycle_summaries,
            "liquidity_cooldown_coins": cooldown_coins,
        }

    @staticmethod
    def _source_reaction_requires_recovery(reaction: dict[str, Any]) -> bool:
        action = reaction.get("action")
        if action == "run_once":
            result = reaction.get("result")
            safe_mode = result.get("safe_mode") if isinstance(result, dict) else None
        elif action == "skipped":
            safe_mode = reaction.get("safe_mode")
        else:
            return False
        return (
            isinstance(safe_mode, dict)
            and safe_mode.get("enabled") is True
            and safe_mode.get("reason") == SafeModeReason.STALE_SOURCE.value
        )

    def _recovered_source_reaction(
        self,
        events: list[SourceEvent],
        recovery: dict[str, Any],
    ) -> dict[str, Any]:
        """Build a fresh reaction envelope without retaining stale skip/retry metadata."""

        cycle = recovery.get("containment_cycle")
        if not isinstance(cycle, dict):
            raise ValueError("recovered source reaction requires a cycle result")
        deferred_open_drain = recovery.get("deferred_open_drain")
        payload: dict[str, Any] = {
            "source_event_key": events[-1].idempotency_key if events else "",
            "source_event_keys": [event.idempotency_key for event in events],
            "event_types": [event.event_type.value for event in events],
            "event_subtypes": [self._source_event_subtype(event) for event in events],
            "action": "run_once",
            "batched_event_count": len(events),
            "source_event_age_ms": max(
                (self._source_event_age_ms(event) for event in events),
                default=0,
            ),
            "result": cycle,
            "deferred_open_drain": (
                deferred_open_drain if isinstance(deferred_open_drain, dict) else None
            ),
            "unattended_recovery": recovery,
        }
        retry = self._hip3_liquidity_retry_payload(cycle)
        if retry is not None:
            payload["retry"] = retry
        return payload

    def react_to_source_event(self, event: SourceEvent) -> dict[str, Any]:
        if not self._source_event_triggers_copy_validation(event):
            return {
                "source_event_key": event.idempotency_key,
                "event_type": event.event_type.value,
                "event_subtype": self._source_event_subtype(event),
                "action": "ignored",
                "detail": "source event does not change copy exposure",
            }
        self.safe_mode.refresh_from_store()
        if self.safe_mode.enabled:
            return self._safe_mode_reaction_skip([event])
        started = monotonic()
        result, deferred_open_drain = self._run_once_until_deferred_opens_drained()
        retry = self._hip3_liquidity_retry_payload(result)
        payload = {
            "source_event_key": event.idempotency_key,
            "event_type": event.event_type.value,
            "event_subtype": self._source_event_subtype(event),
            "action": "run_once",
            "batched_event_count": 1,
            "source_event_age_ms": self._source_event_age_ms(event),
            "elapsed_s": round(monotonic() - started, 6),
            "result": result,
            "deferred_open_drain": deferred_open_drain,
        }
        if retry is not None:
            payload["retry"] = retry
        return payload

    def react_to_source_events(self, events: list[SourceEvent]) -> dict[str, Any]:
        actionable = [
            event for event in events if self._source_event_triggers_copy_validation(event)
        ]
        if not actionable:
            return {
                "source_event_key": events[0].idempotency_key if events else "",
                "source_event_keys": [event.idempotency_key for event in events],
                "event_types": [event.event_type.value for event in events],
                "event_subtypes": [self._source_event_subtype(event) for event in events],
                "action": "ignored",
                "batched_event_count": len(events),
                "detail": "source events do not change copy exposure",
            }
        self.safe_mode.refresh_from_store()
        if self.safe_mode.enabled:
            return self._safe_mode_reaction_skip(actionable)
        if len(actionable) == 1 and len(events) == 1:
            return self.react_to_source_event(actionable[0])
        started = monotonic()
        result, deferred_open_drain = self._run_once_until_deferred_opens_drained()
        ages = [self._source_event_age_ms(event) for event in actionable]
        retry = self._hip3_liquidity_retry_payload(result)
        payload = {
            "source_event_key": actionable[-1].idempotency_key,
            "source_event_keys": [event.idempotency_key for event in actionable],
            "event_types": [event.event_type.value for event in actionable],
            "event_subtypes": [self._source_event_subtype(event) for event in actionable],
            "action": "run_once",
            "batched_event_count": len(actionable),
            "source_event_age_ms": max(ages, default=0),
            "elapsed_s": round(monotonic() - started, 6),
            "result": result,
            "deferred_open_drain": deferred_open_drain,
        }
        if retry is not None:
            payload["retry"] = retry
        return payload

    def _safe_mode_reaction_skip(self, events: list[SourceEvent]) -> dict[str, Any]:
        return {
            "source_event_key": events[-1].idempotency_key if events else "",
            "source_event_keys": [event.idempotency_key for event in events],
            "event_types": [event.event_type.value for event in events],
            "event_subtypes": [self._source_event_subtype(event) for event in events],
            "action": "skipped",
            "batched_event_count": len(events),
            "source_event_age_ms": max(
                (self._source_event_age_ms(event) for event in events),
                default=0,
            ),
            "detail": "safe mode is active; source event validation requires reconcile",
            "safe_mode": self._safe_mode_status(),
        }

    @staticmethod
    def _source_event_subtype(event: SourceEvent) -> str:
        return str(event.payload.get("event_subtype") or event.event_type.value)

    @staticmethod
    def _source_event_age_ms(event: SourceEvent) -> int:
        if event.observed_ts_ms <= 0:
            return 0
        return max(0, now_ms() - event.observed_ts_ms)

    def _source_copy_signal_changed(self, event: SourceEvent) -> bool:
        signal_key = event.payload.get("copy_signal_key")
        if not isinstance(signal_key, str) or not signal_key:
            return True
        subtype = self._source_event_subtype(event)
        previous = self._last_source_copy_signal_by_subtype.get(subtype)
        if previous == signal_key:
            return False
        self._last_source_copy_signal_by_subtype[subtype] = signal_key
        return True

    @staticmethod
    def _source_event_triggers_copy_validation(event: SourceEvent) -> bool:
        raw_event_count = event.payload.get("event_count")
        try:
            event_count = 1
            if raw_event_count not in (None, ""):
                event_count = int(str(raw_event_count))
        except (TypeError, ValueError):
            event_count = 1
        if event.event_type == SourceEventType.POSITION:
            return True
        if event_count == 0:
            return False
        if event.event_type in {
            SourceEventType.FILL,
            SourceEventType.LEVERAGE,
        }:
            return True
        if event.event_type == SourceEventType.CANCEL:
            return True
        if event.event_type == SourceEventType.OPEN_ORDER and (
            _source_event_has_status(event, "filled")
            or _source_event_has_rejected_order_status(event)
            or _source_event_has_triggered_order_status(event)
        ):
            return True
        if (
            event.event_type == SourceEventType.SNAPSHOT
            and _source_event_has_margin_transfer_ledger_type(event)
        ):
            return True
        if event.event_type == SourceEventType.SNAPSHOT and _source_event_has_terminal_twap_status(
            event
        ):
            return True
        return False

    def _run_once_with_lease(
        self,
        preflight: PreflightReport,
        *,
        recovery_containment_only: bool = False,
    ) -> dict[str, Any]:
        if self.config.mode in {Mode.TESTNET, Mode.LIVE}:
            pending = self.store.pending_intents(self.config.mode)
            if pending:
                detail = (
                    f"{len(pending)} unresolved prior intents require reconcile before new risk"
                )
                self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
                return {
                    "preflight": to_jsonable(preflight),
                    "safe_mode": {
                        "enabled": self.safe_mode.enabled,
                        "reason": self.safe_mode.reason.value,
                        "detail": self.safe_mode.detail,
                    },
                    "intents": [],
                }
        try:
            snapshot = self.observer.reconcile_once()
        except Exception as exc:
            return self._blocked_cycle_payload(
                preflight,
                SafeModeReason.REST_LAG,
                f"source REST reconcile failed: {exc}",
            )
        self._last_source_account_value = self._source_snapshot_account_value(snapshot)
        if not self._check_source_freshness(snapshot.observed_ms):
            return {
                "preflight": to_jsonable(preflight),
                "source_positions": to_jsonable(snapshot.positions),
                **self._safe_mode_payload(cleared=False),
                "intents": [],
                "reports": [],
            }
        try:
            asset_meta = self.load_asset_meta()
            execution_mids = (
                self.load_execution_mids()
                if self.config.mode in {Mode.TESTNET, Mode.LIVE}
                else snapshot.mids
            )
        except Exception as exc:
            return self._blocked_cycle_payload(
                preflight,
                SafeModeReason.REST_LAG,
                f"execution market data load failed: {exc}",
            )
        try:
            follower_positions, follower_open_orders, follower_observed_ms = (
                self._current_follower_truth()
            )
        except Exception as exc:
            return self._blocked_cycle_payload(
                preflight,
                SafeModeReason.STALE_FOLLOWER,
                f"follower reconcile failed: {exc}",
            )
        refresh_threshold_ms = max(self.config.risk.stale_source_ms // 2, 1)
        if max(0, now_ms() - snapshot.observed_ms) >= refresh_threshold_ms:
            try:
                snapshot = self.observer.reconcile_once()
            except Exception as exc:
                return self._blocked_cycle_payload(
                    preflight,
                    SafeModeReason.REST_LAG,
                    f"source pre-dispatch refresh failed: {exc}",
                )
            self._last_source_account_value = self._source_snapshot_account_value(snapshot)
            if not self._check_source_freshness(snapshot.observed_ms):
                return {
                    "preflight": to_jsonable(preflight),
                    "source_positions": to_jsonable(snapshot.positions),
                    **self._safe_mode_payload(cleared=False),
                    "intents": [],
                    "reports": [],
                }
        follower_fresh = self._check_follower_freshness(follower_observed_ms)
        if not follower_fresh:
            return {
                "preflight": to_jsonable(preflight),
                "source_positions": to_jsonable(snapshot.positions),
                **self._safe_mode_payload(cleared=False),
                "intents": [],
                "reports": [],
            }
        self._active_plan_source_observed_ms = snapshot.observed_ms
        self._active_plan_follower_observed_ms = follower_observed_ms
        manual_ok = True
        try:
            manual_ok = self._check_manual_intervention(
                follower_positions,
                follower_open_orders,
                position_mid_prices=execution_mids,
            )
        except JournalIntegrityError as exc:
            return self._blocked_cycle_payload(
                preflight,
                SafeModeReason.STARTUP_RECONCILE,
                f"journal baseline rebuild failed: {exc}",
            )
        engine = CopyEngine(
            self.config.risk,
            self.config.mode,
            follower_account=self._effective_action_account(),
        )
        source_event_key = snapshot.planning_key
        result = engine.plan(
            source_event_key=source_event_key,
            source_positions=snapshot.positions,
            follower_positions=follower_positions,
            asset_meta=asset_meta,
            mids=execution_mids,
            source_account_value=self._last_source_account_value,
            follower_account_value=self._last_follower_account_value,
        )
        action_account = self._effective_action_account() or "local-paper-shadow"
        source_network = self.config.resolved_source_network.value
        result = replace(
            result,
            desired_state=replace(
                result.desired_state,
                state_id=deterministic_cloid(
                    "scoped-desired",
                    result.desired_state.state_id,
                    self.config.source_wallet.lower(),
                    source_network,
                    action_account,
                ),
                source_wallet=self.config.source_wallet.lower(),
                action_account=action_account,
                source_network=source_network,
            ),
        )
        if self.config.mode in {Mode.TESTNET, Mode.LIVE}:
            result = replace(
                result,
                desired_state=replace(
                    result.desired_state,
                    positions={
                        coin: (
                            position
                            if position.leverage is not None
                            else replace(position, leverage=1)
                        )
                        for coin, position in result.desired_state.positions.items()
                    },
                ),
            )
        self._last_sizing = dict(result.sizing)
        deferred_reversal_intents: list[FollowerIntent] = []
        deferred_open_intents: list[FollowerIntent] = []
        liquidity_deferred_intents: list[Hip3LiquidityDeferral] = []
        if self.config.mode in {Mode.TESTNET, Mode.LIVE} and manual_ok:
            active_intents, deferred_reversal_intents, flip_detail = self._stage_exchange_reversals(
                result.intents
            )
            if flip_detail:
                self.safe_mode.trip(SafeModeReason.RAPID_FLIP, flip_detail)
            else:
                if deferred_reversal_intents:
                    result = replace(
                        result,
                        desired_state=self._staged_reversal_desired_state(
                            result.desired_state,
                            deferred_reversal_intents,
                        ),
                        intents=active_intents,
                    )
                (
                    admitted_intents,
                    liquidity_deferred_intents,
                    hip3_blockers,
                ) = self._admit_hip3_open_intents(
                    result.intents,
                    asset_meta=asset_meta,
                )
                result = replace(
                    result,
                    intents=admitted_intents,
                    blockers=[*result.blockers, *hip3_blockers],
                )
                if liquidity_deferred_intents:
                    result = replace(
                        result,
                        desired_state=self._staged_deferred_open_desired_state(
                            result.desired_state,
                            [item.intent for item in liquidity_deferred_intents],
                            follower_positions=follower_positions,
                            deferral_class="hip3_liquidity",
                        ),
                    )
                active_intents, deferred_open_intents = self._stage_exposure_increasing_batch(
                    result.intents
                )
                if deferred_open_intents:
                    result = replace(
                        result,
                        desired_state=self._staged_deferred_open_desired_state(
                            result.desired_state,
                            deferred_open_intents,
                            follower_positions=follower_positions,
                        ),
                        intents=active_intents,
                    )
        staged_reversal_coins = {
            canonical_market_symbol(intent.coin) for intent in deferred_reversal_intents
        }
        deferred_intents = [*deferred_reversal_intents, *deferred_open_intents]
        intent_priority = {
            IntentAction.CANCEL: 0,
            IntentAction.CLOSE: 1,
            IntentAction.REDUCE: 2,
            IntentAction.NOOP: 3,
            IntentAction.OPEN: 4,
        }
        result = replace(
            result,
            intents=sorted(
                result.intents,
                key=lambda item: (
                    intent_priority[item.action],
                    canonical_market_symbol(item.coin),
                ),
            ),
        )
        if self.config.mode in {Mode.TESTNET, Mode.LIVE}:
            result = replace(
                result,
                intents=self._bind_hip3_ioc_retry_identities(result.intents),
            )
            result = replace(
                result,
                desired_state=replace(
                    result.desired_state,
                    state_id=deterministic_cloid(
                        "durable-execution-plan",
                        result.desired_state.state_id,
                        follower_positions,
                        follower_open_orders,
                        [
                            (
                                intent.intent_id,
                                intent.action.value,
                                intent.coin,
                                intent.size,
                            )
                            for intent in result.intents
                        ],
                    ),
                ),
            )
        result = replace(
            result,
            intents=[
                replace(intent, desired_state_id=result.desired_state.state_id)
                for intent in result.intents
            ],
        )
        deferred_intents = [
            replace(intent, desired_state_id=result.desired_state.state_id)
            for intent in deferred_intents
        ]
        liquidity_deferred_intents = [
            replace(
                item,
                intent=replace(
                    item.intent,
                    desired_state_id=result.desired_state.state_id,
                ),
            )
            for item in liquidity_deferred_intents
        ]
        if recovery_containment_only:
            containment_blockers = self._recovery_containment_blockers(
                desired_positions=result.desired_state.positions,
                follower_positions=follower_positions,
                intents=result.intents,
            )
            if containment_blockers:
                detail = (
                    "unattended source recovery remained fail-closed because current truth "
                    "would increase follower risk: " + "; ".join(containment_blockers)
                )
                self.safe_mode.trip(SafeModeReason.WEBSOCKET_DISCONNECT, detail)
                return {
                    "preflight": to_jsonable(preflight),
                    "source_positions": to_jsonable(snapshot.positions),
                    "desired_state": to_jsonable(result.desired_state),
                    "desired_state_committed": False,
                    "intents": to_jsonable(result.intents),
                    "deferred_intents": to_jsonable(deferred_intents),
                    "liquidity_deferred_intents": to_jsonable(liquidity_deferred_intents),
                    "reports": [],
                    "recovery_containment_blocked": True,
                    "recovery_containment_blockers": containment_blockers,
                    "safe_mode": self._safe_mode_status(),
                }
        for blocker in result.blockers:
            if "allowlist" in blocker or "metadata" in blocker:
                self.safe_mode.trip(SafeModeReason.UNSUPPORTED_SYMBOL, blocker)
            elif "mid price" in blocker or "mid" in blocker:
                self.safe_mode.trip(SafeModeReason.STALE_SOURCE, blocker)
            elif (
                "exceeds cap" in blocker
                or "leverage" in blocker
                or "round-trip" in blocker
                or "visible" in blocker
                or "oracle envelope" in blocker
            ):
                self.safe_mode.trip(SafeModeReason.RISK_LIMIT, blocker)
            else:
                self.safe_mode.trip(SafeModeReason.CONFIG_INVALID, blocker)

        guard = ExecutionGuard(
            risk=self.config.risk,
            ops=self.config.ops,
            store=self.store,
            asset_meta=asset_meta,
            mids=execution_mids,
            mode=self.config.mode,
        )
        cycle_decision = guard.check_cycle(result.intents)
        if not cycle_decision.ok:
            self.safe_mode.trip(cycle_decision.reason, cycle_decision.detail)

        verified_noop_cycle = self._verified_all_noop_current_truth_cycle(
            preflight=preflight,
            source_positions=snapshot.positions,
            source_observed_ms=snapshot.observed_ms,
            result=result,
            follower_positions=follower_positions,
            follower_open_orders=follower_open_orders,
            follower_observed_ms=follower_observed_ms,
            manual_ok=manual_ok,
            deferred_intents=deferred_intents,
            liquidity_deferred_intents=liquidity_deferred_intents,
        )
        if verified_noop_cycle is not None:
            return verified_noop_cycle

        plan_persisted = self.config.mode in {Mode.TESTNET, Mode.LIVE} or (
            not result.blockers and not self.safe_mode.enabled
        )
        plan_prepared = False
        if plan_persisted:
            plan_prepared = self.store.prepare_execution_plan(
                result.desired_state,
                result.intents,
            )
            if not plan_prepared and result.intents and self.config.mode != Mode.SHADOW:
                self.safe_mode.trip(
                    SafeModeReason.DUPLICATE_INTENT,
                    "execution plan identity already exists; no member was dispatched",
                )

        reports = []
        exchange_state_changed = False
        leverage_sync_ok = True
        armed_dead_man: ExecutionReport | None = None
        satisfied_intent_ids = {
            intent.intent_id for intent in result.intents if intent.action == IntentAction.NOOP
        }
        projected_positions = dict(follower_positions)
        pre_dispatch_guard_decisions = {
            intent.intent_id: guard.check_intent(
                intent,
                projected_positions=projected_positions,
            )
            for intent in result.intents
        }
        leverage_before_open_coins = {
            canonical_market_symbol(intent.coin)
            for intent in result.intents
            if increases_exposure(intent)
            and intent.status != IntentStatus.SKIPPED
            and pre_dispatch_guard_decisions[intent.intent_id].ok
        }
        dispatchable_size_order_coins = {
            canonical_market_symbol(intent.coin)
            for intent in result.intents
            if intent.action in {IntentAction.OPEN, IntentAction.REDUCE, IntentAction.CLOSE}
            and intent.status != IntentStatus.SKIPPED
            and pre_dispatch_guard_decisions[intent.intent_id].ok
        }
        deferred_leverage_targets: dict[str, int] = {}
        if (
            plan_persisted
            and self.config.mode in {Mode.TESTNET, Mode.LIVE}
            and not recovery_containment_only
        ):
            for coin, target in sorted(result.desired_state.positions.items()):
                current = projected_positions.get(coin)
                if (
                    target.size == 0
                    or current is None
                    or current.size == 0
                    or current.leverage == target.leverage
                ):
                    continue
                canonical_coin = canonical_market_symbol(coin)
                leverage_increase = int(target.leverage or 1) > int(current.leverage or 1)
                if canonical_coin in leverage_before_open_coins:
                    continue
                if canonical_coin in dispatchable_size_order_coins:
                    deferred_leverage_targets[coin] = int(target.leverage or 1)
                    continue
                observed = now_ms()
                leverage_intent = FollowerIntent(
                    intent_id=deterministic_cloid(
                        "leverage-sync-intent", result.desired_state.state_id, coin
                    ),
                    cloid=deterministic_cloid(
                        "leverage-sync-cloid", result.desired_state.state_id, coin
                    ),
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="buy" if target.size > 0 else "sell",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    mode=self.config.mode,
                    source_event_key=result.desired_state.source_event_key,
                    reason="synchronize exchange leverage without changing position size",
                    created_ms=observed,
                    desired_state_id=result.desired_state.state_id,
                    status=IntentStatus.PENDING,
                )
                leverage_report = self._ensure_exchange_leverage_for_intent(
                    leverage_intent,
                    desired_state=result.desired_state,
                    follower_positions=projected_positions,
                    allow_existing_increase=leverage_increase,
                )
                if leverage_report is None:
                    leverage_sync_ok = False
                    continue
                reports.append(leverage_report)
                leverage_update_ok = leverage_report.status == IntentStatus.ACKED
                leverage_sync_ok = leverage_sync_ok and leverage_update_ok
                exchange_state_changed = exchange_state_changed or leverage_report.status in {
                    IntentStatus.SENT,
                    IntentStatus.ACKED,
                }
        for intent in result.intents:
            if plan_persisted:
                if not plan_prepared:
                    continue
            else:
                inserted = self.store.append_intent(intent)
                if not inserted and self.config.mode != Mode.SHADOW:
                    detail = "intent cloid already exists in journal"
                    self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                    report = self._blocked_report(
                        intent,
                        SafeModeReason.DUPLICATE_INTENT,
                        detail,
                    )
                    self.store.append_execution_report(report)
                    reports.append(report)
                    continue

            risk_reducing_intent = intent.reduce_only and intent.action in {
                IntentAction.CLOSE,
                IntentAction.REDUCE,
            }
            safe_reduction_allowed = (
                risk_reducing_intent and self._safe_mode_allows_automatic_reduction()
            )
            if (
                self.safe_mode.enabled
                and intent.action != IntentAction.NOOP
                and not safe_reduction_allowed
            ):
                if self.config.mode != Mode.SHADOW:
                    report = self._blocked_report(
                        intent, self.safe_mode.reason, self.safe_mode.detail
                    )
                    self.store.append_execution_report(report)
                    reports.append(report)
                continue

            guard_decision = guard.check_intent(intent, projected_positions=projected_positions)
            if not guard_decision.ok:
                blocked_reason = guard_decision.reason
                blocked_detail = guard_decision.detail
                if (
                    guard_decision.terminal_skip
                    and intent.action == IntentAction.CLOSE
                    and canonical_market_symbol(intent.coin) in staged_reversal_coins
                ):
                    blocked_reason = SafeModeReason.PARTIAL_FILL
                    blocked_detail = (
                        f"{canonical_market_symbol(intent.coin)} reversal cannot reopen because the residual "
                        f"position cannot be flattened automatically: {guard_decision.detail}"
                    )
                    self.safe_mode.trip(blocked_reason, blocked_detail)
                elif not guard_decision.terminal_skip:
                    self.safe_mode.trip(blocked_reason, blocked_detail)
                if self.config.mode != Mode.SHADOW:
                    report = self._blocked_report(
                        intent,
                        blocked_reason,
                        blocked_detail,
                    )
                    self.store.append_execution_report(report)
                    reports.append(report)
                continue

            leverage_report = (
                None
                if recovery_containment_only
                or canonical_market_symbol(intent.coin) in deferred_leverage_targets
                else self._ensure_exchange_leverage_for_intent(
                    intent,
                    desired_state=result.desired_state,
                    follower_positions=projected_positions,
                    allow_existing_increase=(
                        canonical_market_symbol(intent.coin) in leverage_before_open_coins
                    ),
                )
            )
            if leverage_report is not None:
                reports.append(leverage_report)
                leverage_update_ok = leverage_report.status == IntentStatus.ACKED
                leverage_sync_ok = leverage_sync_ok and leverage_update_ok
                if leverage_update_ok:
                    exchange_state_changed = True
                if self.safe_mode.enabled:
                    if self.config.mode != Mode.SHADOW:
                        report = self._blocked_report(
                            intent, self.safe_mode.reason, self.safe_mode.detail
                        )
                        self.store.append_execution_report(report)
                        reports.append(report)
                    continue
            if intent.action == IntentAction.NOOP:
                noop_report = ExecutionReport(
                    report_id=deterministic_cloid(
                        "noop-terminal-report", intent.intent_id, intent.cloid
                    ),
                    intent_id=intent.intent_id,
                    cloid=intent.cloid,
                    status=IntentStatus.SKIPPED,
                    exchange_status="skipped",
                    exchange_ts_ms=now_ms(),
                    payload={
                        "reason": intent.reason,
                        "signed_action_performed": False,
                    },
                )
                if plan_persisted:
                    self.store.append_execution_report(noop_report)
                reports.append(noop_report)
                continue
            dead_man: ExecutionReport | None = None
            if self.config.mode in {Mode.TESTNET, Mode.LIVE} and intent.action in {
                IntentAction.OPEN,
                IntentAction.REDUCE,
                IntentAction.CLOSE,
            }:
                dead_man = self._schedule_dead_man_cancel(
                    scheduled_time_ms=now_ms() + self.config.ops.dead_man_cancel_ms,
                    operation="run_once",
                    count_rate=True,
                )
                if dead_man is not None:
                    self.store.append_execution_report(dead_man)
                    reports.append(dead_man)
                    if dead_man.status == IntentStatus.ACKED:
                        armed_dead_man = dead_man
                if (
                    self.safe_mode.enabled and not risk_reducing_intent
                ) or self._dead_man_blocks_execution(dead_man):
                    report = self._blocked_report(
                        intent, self.safe_mode.reason, self.safe_mode.detail
                    )
                    self.store.append_execution_report(report)
                    reports.append(report)
                    continue
            runtime_decision = self._runtime_allows_execution(intent)
            if not runtime_decision.ok:
                self.safe_mode.trip(runtime_decision.reason, runtime_decision.detail)
                if self.config.mode != Mode.SHADOW:
                    report = self._blocked_report(
                        intent,
                        runtime_decision.reason,
                        runtime_decision.detail,
                    )
                    self.store.append_execution_report(report)
                    reports.append(report)
                continue
            self._active_dispatch_intent = intent
            self._active_dispatch_asset_meta = asset_meta.get(canonical_market_symbol(intent.coin))
            self._active_dispatch_round_trip_quote = None
            self._active_dispatch_liquidity_deferral = None
            self._active_dispatch_attempt_started_ms = None
            effective_intent = intent
            dispatch_liquidity_deferral: Hip3LiquidityDeferral | None = None
            try:
                report = self._execute_intent(intent)
                effective_intent = self._active_dispatch_intent or intent
                if report is not None and self._active_dispatch_round_trip_quote is not None:
                    report = replace(
                        report,
                        payload={
                            **report.payload,
                            "round_trip_pre_send": (
                                self._active_dispatch_round_trip_quote.to_payload()
                            ),
                        },
                    )
            finally:
                dispatch_liquidity_deferral = self._active_dispatch_liquidity_deferral
                self._active_dispatch_intent = None
                self._active_dispatch_asset_meta = None
                self._active_dispatch_round_trip_quote = None
                self._active_dispatch_liquidity_deferral = None
                self._active_dispatch_attempt_started_ms = None
            if report is not None:
                if dispatch_liquidity_deferral is not None:
                    report = replace(
                        report,
                        payload={
                            **report.payload,
                            "liquidity_deferral": to_jsonable(dispatch_liquidity_deferral),
                        },
                    )
                    if all(
                        item.intent.intent_id != dispatch_liquidity_deferral.intent.intent_id
                        for item in liquidity_deferred_intents
                    ):
                        liquidity_deferred_intents.append(dispatch_liquidity_deferral)
                self.store.append_execution_report(report)
                reports.append(report)
                self._record_runtime_result(report)
                if report.status in {
                    IntentStatus.SENT,
                    IntentStatus.ACKED,
                    IntentStatus.FILLED,
                }:
                    exchange_state_changed = True
                elif self._execution_report_decimal(
                    report, "filled_size", default=Decimal("0")
                ) not in {None, Decimal("0")}:
                    exchange_state_changed = True
                elif report.payload.get("requires_post_action_reconcile") is True:
                    exchange_state_changed = True
                effective_report = self._handle_non_terminal_exchange_ack(
                    report,
                    effective_intent,
                    require_full_fill=(
                        canonical_market_symbol(effective_intent.coin) in staged_reversal_coins
                    ),
                )
                if effective_report.report_id != report.report_id:
                    reports.append(effective_report)
                report = effective_report
                if report.status == IntentStatus.FILLED:
                    exchange_state_changed = True
                    satisfied_intent_ids.add(intent.intent_id)
                    projection_intent = effective_intent
                    if report.exchange_status == "dust_residual_accepted":
                        filled_size = self._execution_report_decimal(
                            report,
                            "filled_size",
                            default=effective_intent.size,
                        )
                        if filled_size is None:
                            continue
                        projection_intent = replace(effective_intent, size=filled_size)
                    guard.apply_projection(projection_intent, projected_positions)
        post_action_reconcile: ReconcileSnapshot | dict[str, str] | None = None
        post_action_verified = not exchange_state_changed
        if (
            exchange_state_changed
            and self.config.mode in {Mode.TESTNET, Mode.LIVE}
            and self.execution_adapter is not None
        ):
            try:
                post_action_reconcile = self.execution_adapter.reconcile()
            except Exception as exc:
                post_action_reconcile = {"error": str(exc)}
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"post-action follower reconcile failed: {exc}",
                )
            else:
                self.store.append_reconcile_snapshot(post_action_reconcile)
                self._active_plan_follower_observed_ms = post_action_reconcile.observed_ms
                if self._check_follower_freshness(post_action_reconcile.observed_ms):
                    expected_open_orders = {
                        order.cloid.lower(): order for order in follower_open_orders if order.cloid
                    }
                    verification = self.shield.manual_intervention(
                        projected_positions,
                        post_action_reconcile.positions,
                        set(expected_open_orders),
                        post_action_reconcile.open_orders,
                        expected_open_orders=expected_open_orders,
                        position_size_tolerance=Decimal("0"),
                    )
                    post_action_verified = verification.ok
        if (
            not recovery_containment_only
            and deferred_leverage_targets
            and isinstance(post_action_reconcile, ReconcileSnapshot)
            and not post_action_reconcile.open_orders
            and all(
                row["attempt_phase"] == ExecutionAttemptPhase.TERMINAL.value
                for row in self.store.execution_plan_intents(result.desired_state.state_id)
            )
        ):
            for coin in sorted(deferred_leverage_targets):
                actual = post_action_reconcile.positions.get(coin)
                leverage_target = result.desired_state.positions.get(coin)
                if actual is None or actual.size == 0 or leverage_target is None:
                    continue
                leverage_intent = FollowerIntent(
                    intent_id=deterministic_cloid(
                        "post-fill-leverage-intent", result.desired_state.state_id, coin
                    ),
                    cloid=deterministic_cloid(
                        "post-fill-leverage-cloid", result.desired_state.state_id, coin
                    ),
                    action=IntentAction.NOOP,
                    coin=coin,
                    side="buy" if actual.size > 0 else "sell",
                    size=Decimal("0"),
                    price=None,
                    reduce_only=False,
                    mode=self.config.mode,
                    source_event_key=result.desired_state.source_event_key,
                    reason="post-fill leverage increase after terminal size execution",
                    created_ms=now_ms(),
                    desired_state_id=result.desired_state.state_id,
                )
                leverage_report = self._ensure_exchange_leverage_for_intent(
                    leverage_intent,
                    desired_state=result.desired_state,
                    follower_positions=post_action_reconcile.positions,
                    allow_existing_increase=True,
                )
                if leverage_report is not None:
                    reports.append(leverage_report)
                    leverage_sync_ok = leverage_sync_ok and (
                        leverage_report.status == IntentStatus.ACKED
                    )
            assert self.execution_adapter is not None
            try:
                leverage_reconcile = self.execution_adapter.reconcile()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"post-leverage follower reconcile failed: {exc}",
                )
            else:
                self.store.append_reconcile_snapshot(leverage_reconcile)
                post_action_reconcile = leverage_reconcile
                self._active_plan_follower_observed_ms = leverage_reconcile.observed_ms
                post_action_verified = self._check_follower_freshness(
                    leverage_reconcile.observed_ms
                )
        dead_man_clear: ExecutionReport | None = None
        if post_action_verified and not self.safe_mode.enabled and armed_dead_man is not None:
            dead_man_clear = self._schedule_dead_man_cancel(
                scheduled_time_ms=None,
                operation="run_once",
                count_rate=False,
            )
            if dead_man_clear is not None:
                self.store.append_execution_report(dead_man_clear)
                reports.append(dead_man_clear)
        dead_man_clear_proven = armed_dead_man is None or (
            dead_man_clear is not None
            and dead_man_clear.status == IntentStatus.ACKED
            and dead_man_clear.exchange_status
            in {"dead_man_cleared", "watchdog_containment_released"}
        )
        if not dead_man_clear_proven and not self.safe_mode.enabled:
            self.safe_mode.trip(
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                "normal execution cycle cannot finalize because the armed dead-man "
                "schedule was not proven cleared",
            )
        reconciled_checkpoint: DesiredState | None = None
        desired_state_committed = False
        execution_finalization: dict[str, Any] | None = None
        if (
            self.config.mode in {Mode.TESTNET, Mode.LIVE}
            and plan_persisted
            and plan_prepared
            and manual_ok
        ):
            if not dead_man_clear_proven:
                execution_finalization = {
                    "status": "dead_man_clear_unproven",
                    "target_state_id": result.desired_state.state_id,
                    "committed_target": False,
                    "checkpoint": None,
                }
            elif not leverage_sync_ok or self.safe_mode.reason == SafeModeReason.STALE_SOURCE:
                execution_finalization = {
                    "status": "cycle_invalidated",
                    "target_state_id": result.desired_state.state_id,
                    "committed_target": False,
                    "checkpoint": None,
                }
            else:
                final_snapshot = (
                    post_action_reconcile
                    if isinstance(post_action_reconcile, ReconcileSnapshot)
                    else ReconcileSnapshot(
                        snapshot_id=deterministic_cloid(
                            "pre-action-finalization",
                            result.desired_state.state_id,
                            follower_observed_ms,
                        ),
                        account=self._effective_action_account(),
                        positions=follower_positions,
                        open_orders=follower_open_orders,
                        observed_ms=follower_observed_ms,
                        source="fresh pre-action follower truth",
                    )
                )
                execution_finalization = self._finalize_execution_truth(
                    final_snapshot,
                    trigger="normal execution cycle",
                    keep_incident_safe=False,
                )
                desired_state_committed = bool(
                    execution_finalization["committed_target"]
                    and execution_finalization["target_state_id"] == result.desired_state.state_id
                )
                reconciled_checkpoint = execution_finalization["checkpoint"]
        elif plan_persisted and self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            required_intent_ids = {
                intent.intent_id for intent in result.intents if intent.action != IntentAction.NOOP
            }
            desired_state_committed = (
                plan_prepared
                and leverage_sync_ok
                and post_action_verified
                and (
                    self.config.mode == Mode.SHADOW
                    or required_intent_ids.issubset(satisfied_intent_ids)
                )
            )
            if desired_state_committed:
                self.store.commit_desired_state(result.desired_state.state_id)
        payload = {
            "preflight": to_jsonable(preflight),
            "source_positions": to_jsonable(snapshot.positions),
            "desired_state": to_jsonable(result.desired_state),
            "desired_state_committed": desired_state_committed,
            "reconciled_checkpoint": to_jsonable(reconciled_checkpoint),
            "intents": to_jsonable(result.intents),
            "deferred_intents": to_jsonable(deferred_intents),
            "liquidity_deferred_intents": to_jsonable(liquidity_deferred_intents),
            "reports": to_jsonable(reports),
            "post_action_reconcile": to_jsonable(post_action_reconcile),
            "execution_finalization": to_jsonable(execution_finalization),
            "safe_mode": {
                "enabled": self.safe_mode.enabled,
                "reason": self.safe_mode.reason.value,
                "detail": self.safe_mode.detail,
            },
        }
        return payload

    def _acquire_exchange_lease(self, operation: str) -> bool:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return True
        name = self._runtime_lease_name(operation)
        owner = self._runtime_lease_owner(operation)
        file_locks: list[AccountRuntimeFileLock] = []
        try:
            for path in self._runtime_file_lock_paths():
                file_lock = AccountRuntimeFileLock(path)
                file_lock.acquire()
                file_locks.append(file_lock)
        except RuntimeFileLockBusy as exc:
            for held_lock in reversed(file_locks):
                held_lock.release()
            detail = f"another instance owns the action-account runtime lock: {exc}"
            self.safe_mode.trip(SafeModeReason.CONCURRENT_INSTANCE, detail)
            return False
        except RuntimeFileLockError as exc:
            for held_lock in reversed(file_locks):
                held_lock.release()
            detail = f"action-account runtime lock is unavailable: {exc}"
            self.safe_mode.trip(SafeModeReason.CONFIG_INVALID, detail)
            return False

        try:
            acquired = self.store.acquire_runtime_lease(
                name=name,
                owner=owner,
                ttl_ms=self.config.ops.runtime_lease_ttl_ms,
            )
        except Exception as exc:
            for file_lock in reversed(file_locks):
                file_lock.release()
            detail = f"SQLite runtime lease could not be acquired: {exc}"
            self.safe_mode.trip(SafeModeReason.CONFIG_INVALID, detail)
            return False
        if not acquired:
            for file_lock in reversed(file_locks):
                file_lock.release()
            detail = f"another instance owns runtime lease {name}"
            self.safe_mode.trip(SafeModeReason.CONCURRENT_INSTANCE, detail)
            return False
        with self._runtime_file_lock_guard:
            self._runtime_file_locks[operation] = tuple(file_locks)
        return True

    def _release_exchange_lease(self, operation: str) -> None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return
        with self._runtime_file_lock_guard:
            file_locks = self._runtime_file_locks.pop(operation, ())
        try:
            self.store.release_runtime_lease(
                name=self._runtime_lease_name(operation),
                owner=self._runtime_lease_owner(operation),
            )
        finally:
            for file_lock in reversed(file_locks):
                file_lock.release()

    def _runtime_lease_name(self, operation: str) -> str:
        account = self.config.exchange.follower_account_address or "unconfigured"
        return f"exchange:{self.config.mode.value}:{self.config.source_wallet}:{account}"

    def _runtime_lease_owner(self, operation: str) -> str:
        return f"{self.instance_id}:{operation}"

    def _runtime_file_lock_path(self):
        network = "mainnet" if self.config.mode == Mode.LIVE else self.config.mode.value
        action_account = (
            self.config.exchange.vault_address
            or self.config.exchange.follower_account_address
            or "unconfigured"
        )
        return account_runtime_lock_path(
            self.config.ops.runtime_lock_dir,
            network=network,
            action_account=action_account,
        )

    def _runtime_signer_address(self) -> str:
        private_key = self.config.exchange.api_private_key
        if private_key:
            try:
                from eth_account import Account

                return str(Account.from_key(private_key).address).lower()
            except Exception:
                return ""
        return self.config.exchange.api_wallet_address.strip().lower()

    def _runtime_signer_file_lock_path(self):
        signer_address = self._runtime_signer_address()
        if not signer_address:
            return None
        network = "mainnet" if self.config.mode == Mode.LIVE else self.config.mode.value
        return signer_runtime_lock_path(
            self.config.ops.runtime_lock_dir,
            network=network,
            signer_address=signer_address,
        )

    def _runtime_generation_fence_file_lock_path(self):
        """Return the account-level generation fence used by the supervisor.

        The ordinary account runtime lock separates two follower processes that use the
        same action account, while the signer lock serializes nonces for one API wallet.
        Neither identity fences an old API wallet from a replacement supervisor using a
        different signer.  Bounded two-account validation therefore adds this account-level
        fence and couples it to the exact controller-registry generation.
        """

        if self.config.ops.validation_controller_registry_path is None:
            return None
        action_account = self._effective_action_account()
        if not action_account:
            return None
        network = "mainnet" if self.config.mode == Mode.LIVE else self.config.mode.value
        return generation_fence_lock_path(
            self.config.ops.runtime_lock_dir,
            network=network,
            action_account=action_account,
        )

    def _runtime_file_lock_paths(self):
        return (self._runtime_file_lock_path(),)

    @contextlib.contextmanager
    def _signed_action_guard(self, action: str):
        signer_path = self._runtime_signer_file_lock_path()
        generation_path = self._runtime_generation_fence_file_lock_path()
        if (
            self.config.ops.validation_controller_registry_path is not None
            and generation_path is None
        ):
            raise PreSendBlockedError(
                f"{action} cannot derive the bounded-validation generation fence identity"
            )

        generation_lock = (
            AccountRuntimeFileLock(generation_path) if generation_path is not None else None
        )
        signer_lock = AccountRuntimeFileLock(signer_path) if signer_path is not None else None
        deadline = monotonic() + float(self.config.ops.exchange_action_timeout_s)

        def acquire(lock: AccountRuntimeFileLock, *, label: str) -> None:
            while True:
                try:
                    lock.acquire()
                    return
                except RuntimeFileLockBusy as exc:
                    if monotonic() < deadline:
                        sleep(0.02)
                        continue
                    if generation_lock is not None:
                        generation = self._validation_controller_registry_decision()
                        if not generation.ok:
                            # A replacement generation owns the account.  The stale process
                            # must become inert without changing its durable safe-mode state.
                            raise PreSendBlockedError(
                                f"{action} blocked by generation fence: {generation.detail}"
                            ) from exc
                    detail = f"{action} timed out waiting for the {label}: {exc}"
                    self.safe_mode.trip(SafeModeReason.CONCURRENT_INSTANCE, detail)
                    raise PreSendBlockedError(detail) from exc
                except RuntimeFileLockError as exc:
                    detail = f"{action} {label} is unavailable: {exc}"
                    self.safe_mode.trip(SafeModeReason.CONFIG_INVALID, detail)
                    raise PreSendBlockedError(detail) from exc

        try:
            if generation_lock is not None:
                acquire(generation_lock, label="account generation fence")
                generation = self._validation_controller_registry_decision(
                    minimum_remaining_ms=max(
                        int(self.config.ops.exchange_action_timeout_s * Decimal("1000")) + 1_000,
                        1_000,
                    )
                )
                if not generation.ok:
                    # This is deliberately not a safe-mode trip: a stale process observing a
                    # successful takeover is expected to quiesce without writing old state.
                    raise PreSendBlockedError(
                        f"{action} blocked by generation fence: {generation.detail}"
                    )
            if signer_lock is None:
                detail = f"{action} cannot derive signer identity for nonce serialization"
                self.safe_mode.trip(SafeModeReason.CONFIG_INVALID, detail)
                raise PreSendBlockedError(detail)
            acquire(signer_lock, label="signer-global nonce lock")
            yield
        finally:
            if signer_lock is not None:
                signer_lock.release()
            if generation_lock is not None:
                generation_lock.release()

    def _current_follower_truth(self) -> tuple[dict[str, Position], list[OpenOrder], int]:
        observed = now_ms()
        if self.config.mode == Mode.PAPER:
            self._last_follower_account_value = None
            return dict(self.paper.positions), [], observed
        if self.config.mode in {Mode.TESTNET, Mode.LIVE} and self.execution_adapter is not None:
            snapshot = self.execution_adapter.reconcile()
            self.store.append_reconcile_snapshot(snapshot)
            self._last_follower_account_value = self._reconcile_account_value(snapshot)
            return snapshot.positions, snapshot.open_orders, snapshot.observed_ms
        self._last_follower_account_value = None
        return {}, [], observed

    def _observation_age_ms(self, label: str, observed_ms: int) -> int | None:
        current_ms = now_ms()
        future_ms = observed_ms - current_ms
        if future_ms > MAX_FUTURE_OBSERVATION_MS:
            detail = (
                f"{label} observation is {future_ms}ms in the future relative to the local "
                f"clock (maximum tolerated {MAX_FUTURE_OBSERVATION_MS}ms); correct clock "
                "rollback/skew before any signed action"
            )
            self.safe_mode.trip(SafeModeReason.CLOCK_SKEW, detail)
            return None

        wall_age_ms = max(current_ms - observed_ms, 0)
        context = (
            self._source_observation_context
            if label == "source"
            else self._follower_observation_context
        )
        if context is None or context[0] != observed_ms:
            return wall_age_ms
        monotonic_age_ms = max(int((monotonic() - context[1]) * 1_000), 0)
        return max(wall_age_ms, monotonic_age_ms)

    def _remember_fresh_observation(self, label: str, observed_ms: int) -> None:
        context = (observed_ms, monotonic())
        if label == "source":
            if self._source_observation_context is None or (
                self._source_observation_context[0] != observed_ms
            ):
                self._source_observation_context = context
            return
        if self._follower_observation_context is None or (
            self._follower_observation_context[0] != observed_ms
        ):
            self._follower_observation_context = context

    def _check_source_freshness(self, observed_ms: int) -> bool:
        age_ms = self._observation_age_ms("source", observed_ms)
        if age_ms is None:
            return False
        fresh = self.shield.stale_source(age_ms, self.config.risk.stale_source_ms).ok
        if fresh:
            self._remember_fresh_observation("source", observed_ms)
        return fresh

    def _check_follower_freshness(self, observed_ms: int) -> bool:
        age_ms = self._observation_age_ms("follower", observed_ms)
        if age_ms is None:
            return False
        fresh = self.shield.stale_follower(age_ms, self.config.risk.stale_follower_ms).ok
        if fresh:
            self._remember_fresh_observation("follower", observed_ms)
        return fresh

    def _check_manual_intervention(
        self,
        follower_positions: dict[str, Position],
        follower_open_orders: list[OpenOrder],
        *,
        allow_dust_tolerance: bool = True,
        position_mid_prices: dict[str, Decimal] | None = None,
    ) -> bool:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return True
        expected_positions = self.store.latest_desired_positions(
            self.config.mode,
            source_wallet=self.config.source_wallet,
            action_account=self._effective_action_account() or "local-paper-shadow",
            source_network=self.config.resolved_source_network.value,
            committed_only=True,
        )
        pending_rows = self.store.pending_intents(self.config.mode)
        expected_open_cloids = {row["cloid"].lower() for row in pending_rows}
        expected_open_orders = self._expected_open_orders_from_pending(pending_rows)
        if expected_positions is None:
            if follower_positions or follower_open_orders:
                detail = (
                    "follower has exchange exposure before any journaled desired state; "
                    "manual reconcile and operator review required"
                )
                self.safe_mode.trip(SafeModeReason.MANUAL_INTERVENTION, detail)
                return False
            expected_positions = {}
        return self.shield.manual_intervention(
            expected_positions=expected_positions,
            actual_positions=follower_positions,
            expected_open_cloids=expected_open_cloids,
            actual_open_orders=follower_open_orders,
            expected_open_orders=expected_open_orders,
            position_size_tolerance=(
                self.config.risk.min_order_size if allow_dust_tolerance else Decimal("0")
            ),
            position_notional_tolerance_usd=(
                HYPERLIQUID_PERP_MIN_NOTIONAL_USD if allow_dust_tolerance else None
            ),
            position_mid_prices=position_mid_prices,
        ).ok

    def _expected_open_orders_from_pending(
        self, pending_rows: list[dict[str, Any]]
    ) -> dict[str, OpenOrder]:
        expected: dict[str, OpenOrder] = {}
        for row in pending_rows:
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError as exc:
                raise JournalIntegrityError(
                    f"pending intent {row['intent_id']} payload is not valid JSON"
                ) from exc
            if not isinstance(payload, dict):
                raise JournalIntegrityError(
                    f"pending intent {row['intent_id']} payload is malformed"
                )
            try:
                cloid = str(payload.get("cloid") or row["cloid"]).lower()
                expected[cloid] = OpenOrder(
                    coin=canonical_market_symbol(payload.get("coin") or row["coin"]),
                    side=str(payload["side"]).lower(),
                    size=parse_decimal(payload["size"]),
                    price=parse_decimal(payload["price"])
                    if payload.get("price") is not None
                    else None,
                    cloid=cloid,
                    reduce_only=bool(payload.get("reduce_only", False)),
                    updated_ms=int(payload.get("created_ms") or row["created_ms"] or 0),
                )
            except Exception as exc:
                raise JournalIntegrityError(
                    f"pending intent {row['intent_id']} cannot be rebuilt: {exc}"
                ) from exc
        return expected

    def _execute_intent(self, intent: FollowerIntent):
        if self.config.mode == Mode.SHADOW:
            return None
        if self.config.mode == Mode.PAPER:
            return self.paper.apply(intent)
        if self.config.mode in {Mode.TESTNET, Mode.LIVE} and self.execution_adapter is not None:
            decision = self._runtime_allows_exchange_action(
                count_rate=False,
                risk_reducing=intent.reduce_only,
            )
            if not decision.ok:
                return self._blocked_exchange_report(
                    intent_id=intent.intent_id,
                    cloid=intent.cloid,
                    reason=decision.reason,
                    detail=decision.detail,
                )
            if not intent.reduce_only and not self._refresh_active_dispatch_truth_for_fallback(
                intent.coin,
                expected_source_event_key=intent.source_event_key,
            ):
                reason = (
                    self.safe_mode.reason if self.safe_mode.enabled else SafeModeReason.STALE_SOURCE
                )
                detail = (
                    self.safe_mode.detail
                    if self.safe_mode.enabled
                    else f"{intent.coin} order dispatch truth refresh failed"
                )
                return self._blocked_exchange_report(
                    intent_id=intent.intent_id,
                    cloid=intent.cloid,
                    reason=reason,
                    detail=detail,
                )
            hip3_risk_increasing = (
                not intent.reduce_only
                and intent.action == IntentAction.OPEN
                and market_dex(intent.coin)
            )
            atomic_hip3_dispatch = bool(
                hip3_risk_increasing
                and getattr(
                    self.execution_adapter,
                    "supports_atomic_hip3_dispatch",
                    False,
                )
            )
            if hip3_risk_increasing:
                asset_meta = self._active_dispatch_asset_meta
                if asset_meta is None:
                    detail = f"{intent.coin} metadata is unavailable for final IOC repricing"
                    self.safe_mode.trip(SafeModeReason.UNSUPPORTED_SYMBOL, detail)
                    return self._blocked_exchange_report(
                        intent_id=intent.intent_id,
                        cloid=intent.cloid,
                        reason=SafeModeReason.UNSUPPORTED_SYMBOL,
                        detail=detail,
                    )
                try:
                    assessment = self.load_hip3_round_trip_assessment(
                        intent.coin,
                        opening_side=intent.side,
                        requested_size=intent.size,
                        asset_meta=asset_meta,
                    )
                except Exception as exc:
                    assessment = None
                    blockers = [f"{intent.coin} final HIP-3 market check failed: {exc}"]
                else:
                    assert assessment is not None
                    blockers = list(assessment.blockers)
                if assessment is not None and assessment.retryable_liquidity:
                    deferral = self._hip3_liquidity_deferral(
                        intent,
                        assessment.blockers,
                        stage="final_reprice",
                    )
                    self._active_dispatch_liquidity_deferral = deferral
                    return self._hip3_liquidity_deferred_report(intent, deferral)
                quote = assessment.quote if assessment is not None else None
                if quote is None or blockers:
                    detail = "final HIP-3 IOC repricing blocked: " + "; ".join(blockers)
                    self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                    return self._blocked_exchange_report(
                        intent_id=intent.intent_id,
                        cloid=intent.cloid,
                        reason=SafeModeReason.RISK_LIMIT,
                        detail=detail,
                    )
                final_notional = self._hip3_open_notional_bound(intent, quote)
                if final_notional > self.config.risk.max_notional_usd:
                    deferral = self._hip3_liquidity_deferral(
                        intent,
                        [
                            f"final HIP-3 IOC notional {final_notional} exceeds per-order "
                            f"cap {self.config.risk.max_notional_usd}; current truth must "
                            "be replanned at a cap-safe size"
                        ],
                        stage="final_cap_reprice",
                    )
                    self._active_dispatch_liquidity_deferral = deferral
                    return self._hip3_liquidity_deferred_report(intent, deferral)
                intent = replace(
                    intent,
                    price=quote.entry_limit,
                    execution_proof={
                        **quote.to_payload(),
                        "post_send_retry_identity": self._hip3_ioc_retry_identity(intent),
                    },
                )
                if not self.store.refresh_prepared_hip3_intent(intent):
                    detail = (
                        f"{intent.intent_id} is no longer a matching PREPARED HIP-3 "
                        "attempt; refusing repriced dispatch"
                    )
                    self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                    return self._blocked_exchange_report(
                        intent_id=intent.intent_id,
                        cloid=intent.cloid,
                        reason=SafeModeReason.DUPLICATE_INTENT,
                        detail=detail,
                    )
                self._active_dispatch_intent = intent
                self._active_dispatch_round_trip_quote = quote
            if not atomic_hip3_dispatch:
                dispatch_started = self.store.begin_intent_dispatch(intent.intent_id)
                if dispatch_started is not True:
                    detail = (
                        f"{intent.intent_id} is not a durable PREPARED attempt; "
                        "refusing duplicate or unjournaled dispatch"
                    )
                    self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                    return self._blocked_exchange_report(
                        intent_id=intent.intent_id,
                        cloid=intent.cloid,
                        reason=SafeModeReason.DUPLICATE_INTENT,
                        detail=detail,
                    )
                dispatch_row = self.store.intent_by_cloid(intent.cloid)
                if dispatch_row is not None:
                    try:
                        self._active_dispatch_attempt_started_ms = int(
                            dispatch_row.get("attempt_updated_ms") or 0
                        )
                    except (TypeError, ValueError):
                        self._active_dispatch_attempt_started_ms = None
            started = monotonic()
            try:
                report = self.execution_adapter.place_intent(intent)
            except Exception as exc:
                elapsed_s = Decimal(str(round(monotonic() - started, 6)))
                return self._exception_exchange_report(
                    intent_id=intent.intent_id,
                    cloid=intent.cloid,
                    detail=f"place_intent raised: {exc}",
                    elapsed_s=elapsed_s,
                )
            elapsed_s = Decimal(str(round(monotonic() - started, 6)))
            payload = dict(report.payload)
            payload["elapsed_s"] = elapsed_s
            report = replace(report, payload=payload)
            effective_sent_intent = self._active_dispatch_intent or intent
            report, zero_fill_deferral = self._normalize_proven_hip3_ioc_zero_fill(
                effective_sent_intent,
                report,
                stage=(
                    "signed_ioc_zero_fill_open"
                    if effective_sent_intent.action == IntentAction.OPEN
                    and not effective_sent_intent.reduce_only
                    else "signed_ioc_zero_fill_reduce_only"
                ),
                paced_retry=True,
            )
            if zero_fill_deferral is not None:
                self._active_dispatch_liquidity_deferral = zero_fill_deferral
            return report
        return None

    def _runtime_allows_execution(self, intent: FollowerIntent) -> RuntimeDecision:
        if intent.action == IntentAction.NOOP:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        return self._runtime_allows_exchange_action(
            count_rate=True,
            risk_reducing=intent.reduce_only,
        )

    def _runtime_allows_exchange_action(
        self,
        *,
        count_rate: bool,
        risk_reducing: bool = False,
    ) -> RuntimeDecision:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        self.safe_mode.refresh_from_store()
        kill_switch = self._kill_switch_path()
        if kill_switch.exists() and not (risk_reducing and self._watchdog_containment_active):
            return RuntimeDecision(
                False,
                SafeModeReason.OPERATOR_KILL_SWITCH,
                f"kill switch file exists: {kill_switch}",
            )
        # The bounded multi-account validation has an independent, atomically
        # refreshed supervisor lease.  It is deliberately evaluated on every
        # risk-increasing path (including the adapter's last-mile callback)
        # instead of only at process startup.  A dead parent, reboot, expired
        # window, or identity mismatch therefore cannot leave an orphaned
        # follower able to add exposure.  Reduce-only containment remains
        # available below this boundary.
        if not risk_reducing:
            supervisor = self._validation_supervisor_decision()
            if not supervisor.ok:
                return supervisor
            market_universe = self._validation_market_universe_decision()
            if not market_universe.ok:
                return market_universe
        if self.safe_mode.enabled and not risk_reducing:
            reason = (
                self.safe_mode.reason
                if self.safe_mode.reason != SafeModeReason.NONE
                else SafeModeReason.PREFLIGHT_FAILED
            )
            return RuntimeDecision(
                False,
                reason,
                self.safe_mode.detail or "safe mode is active",
            )
        if risk_reducing:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        circuit = self.circuit_breaker.check()
        if not circuit.ok:
            return circuit
        persistent_circuit = self._persistent_circuit_breaker_decision()
        if not persistent_circuit.ok:
            return persistent_circuit
        if not count_rate:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        persistent_rate = self._persistent_exchange_rate_decision()
        if not persistent_rate.ok:
            return persistent_rate
        rate = self.exchange_rate_limiter.check()
        if not rate.ok:
            return rate
        self.exchange_rate_limiter.record()
        return rate

    def _validation_supervisor_decision(self) -> RuntimeDecision:
        """Prove the optional bounded-validation supervisor lease is current.

        The lease is an external safety boundary, not a liveness hint.  Any
        malformed or mismatched value fails closed.  The method intentionally
        returns a decision without clearing or mutating durable safe mode so a
        caller can use it from both planning and the final pre-send gate.
        """

        ops = self.config.ops
        path = ops.validation_supervisor_lease_path
        if path is None:
            return RuntimeDecision(True, SafeModeReason.NONE, "")

        action_account = self._effective_action_account() or ""
        expected_identity: dict[str, str] = {
            "run_id": ops.validation_run_id,
            "owner_token": ops.validation_owner_token,
            "supervisor_incarnation_id": ops.validation_supervisor_incarnation_id,
            "state_identity_sha256": ops.validation_state_identity_sha256,
            "effective_config_sha256": ops.validation_effective_config_sha256,
            "effective_config_set_sha256": ops.validation_effective_config_set_sha256,
            "follower_account_address": action_account.lower(),
        }
        if (
            not expected_identity["run_id"]
            or not expected_identity["owner_token"]
            or not expected_identity["supervisor_incarnation_id"]
            or not expected_identity["state_identity_sha256"]
            or not expected_identity["effective_config_sha256"]
            or not expected_identity["effective_config_set_sha256"]
            or not expected_identity["follower_account_address"]
            or ops.validation_deadline_ms <= 0
        ):
            return self._blocked_validation_supervisor("runtime guard configuration is incomplete")
        try:
            payload = read_supervisor_lease(path)
        except (OSError, UnicodeError, ValueError) as exc:
            return self._blocked_validation_supervisor(f"lease is unavailable or invalid: {exc}")

        for field in (
            "run_id",
            "owner_token",
            "supervisor_incarnation_id",
            "state_identity_sha256",
            "effective_config_sha256",
            "effective_config_set_sha256",
            "follower_account_address",
        ):
            actual = payload.get(field)
            if field in {
                "state_identity_sha256",
                "effective_config_sha256",
                "effective_config_set_sha256",
                "follower_account_address",
            } and isinstance(actual, str):
                actual = actual.lower()
            if actual != expected_identity[field]:
                return self._blocked_validation_supervisor(
                    f"lease identity field {field} does not match this runtime"
                )
        integer_fields: dict[str, int] = {}
        for field in ("deadline_ms", "heartbeat_ms", "expires_ms"):
            value = payload.get(field)
            if type(value) is not int or value <= 0:
                return self._blocked_validation_supervisor(
                    f"lease field {field} must be a positive integer"
                )
            integer_fields[field] = value
        if integer_fields["deadline_ms"] != ops.validation_deadline_ms:
            return self._blocked_validation_supervisor(
                "lease immutable deadline does not match this runtime"
            )
        if integer_fields["expires_ms"] < integer_fields["heartbeat_ms"]:
            return self._blocked_validation_supervisor("lease expiry precedes its heartbeat")
        status = payload.get("status")
        if status not in {"starting", "ready", "running"}:
            return self._blocked_validation_supervisor(
                f"lease status {status!r} does not permit new risk"
            )
        observed = now_ms()
        if integer_fields["heartbeat_ms"] - observed > MAX_FUTURE_OBSERVATION_MS:
            return self._blocked_validation_supervisor(
                "lease heartbeat is too far in the future; correct local clock skew"
            )
        if observed >= integer_fields["expires_ms"]:
            return self._blocked_validation_supervisor("lease heartbeat expired")
        if observed >= integer_fields["deadline_ms"]:
            return self._blocked_validation_supervisor("validation deadline elapsed")
        registry = self._validation_controller_registry_decision(observed_ms=observed)
        if not registry.ok:
            return registry
        return RuntimeDecision(True, SafeModeReason.NONE, "")

    def _validation_controller_claim(self) -> ControllerClaim | None:
        follower = (self._effective_action_account() or "").lower()
        for claim in self._validation_controller_claims():
            if claim.follower == follower:
                return claim
        return None

    def _validation_controller_claims(self) -> tuple[ControllerClaim, ...]:
        ops = self.config.ops
        follower = (self._effective_action_account() or "").lower()
        followers = tuple(sorted(address.lower() for address in ops.validation_follower_set))
        if (
            ops.validation_controller_registry_path is None
            or not follower
            or len(followers) != 2
            or len(set(followers)) != 2
            or follower not in followers
            or not ops.validation_owner_token
            or not ops.validation_run_id
            or not ops.validation_supervisor_incarnation_id
            or not ops.validation_state_identity_sha256
            or ops.validation_deadline_ms <= 0
        ):
            return ()
        return tuple(
            ControllerClaim(
                follower=address,
                owner_token=ops.validation_owner_token,
                run_id=ops.validation_run_id,
                state_identity_sha256=ops.validation_state_identity_sha256,
                deadline_ms=ops.validation_deadline_ms,
            )
            for address in followers
        )

    def _validation_controller_registry_decision(
        self,
        *,
        observed_ms: int | None = None,
        minimum_remaining_ms: int = 0,
    ) -> RuntimeDecision:
        path = self.config.ops.validation_controller_registry_path
        if path is None:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        claims = self._validation_controller_claims()
        claim = self._validation_controller_claim()
        if claim is None or len(claims) != 2:
            return self._blocked_validation_supervisor("controller registry identity is incomplete")
        if not path.is_file():
            return self._blocked_validation_supervisor("controller registry is unavailable")
        registry: ControllerRegistry | None = None
        try:
            registry = ControllerRegistry(path)
            leases, exclusive = registry.exclusive_snapshot(
                expected_claim.follower for expected_claim in claims
            )
        except (OSError, sqlite3.Error, ValueError) as exc:
            return self._blocked_validation_supervisor(
                f"controller registry cannot be verified: {exc}"
            )
        finally:
            if registry is not None:
                registry.close()
        if exclusive is None:
            return self._blocked_validation_supervisor(
                "controller registry has no exact-two supervisor generation"
            )
        exclusive_expected = {
            "incarnation_id": self.config.ops.validation_supervisor_incarnation_id,
            "owner_token": claim.owner_token,
            "run_id": claim.run_id,
            "state_identity_sha256": claim.state_identity_sha256,
            "deadline_ms": claim.deadline_ms,
        }
        for field, value in exclusive_expected.items():
            actual = exclusive.get(field)
            if field == "state_identity_sha256" and isinstance(actual, str):
                actual = actual.lower()
            if actual != value:
                return self._blocked_validation_supervisor(
                    f"exclusive controller field {field} does not match this generation"
                )
        try:
            exclusive_followers = json.loads(str(exclusive.get("follower_set_json") or ""))
        except json.JSONDecodeError:
            exclusive_followers = None
        expected_followers = sorted(item.follower for item in claims)
        if (
            not isinstance(exclusive_followers, list)
            or sorted(str(item).lower() for item in exclusive_followers) != expected_followers
        ):
            return self._blocked_validation_supervisor(
                "exclusive controller follower set does not match this generation"
            )
        observed = observed_ms if observed_ms is not None else now_ms()
        required_through_ms = observed + max(0, minimum_remaining_ms)
        allowed_statuses = {"starting", "ready", "running", "guardian", "containment"}
        exclusive_status = exclusive.get("status")
        if exclusive_status not in allowed_statuses:
            return self._blocked_validation_supervisor("exclusive controller status is invalid")
        for expected_claim in claims:
            lease = leases.get(expected_claim.follower)
            if lease is None:
                return self._blocked_validation_supervisor(
                    f"controller registry has no claim for exact-set follower "
                    f"{expected_claim.follower}"
                )
            expected = {
                "follower": expected_claim.follower.lower(),
                "owner_token": expected_claim.owner_token,
                "run_id": expected_claim.run_id,
                "state_identity_sha256": expected_claim.state_identity_sha256,
                "deadline_ms": expected_claim.deadline_ms,
            }
            for field, value in expected.items():
                actual = lease.get(field)
                if field in {"follower", "state_identity_sha256"} and isinstance(actual, str):
                    actual = actual.lower()
                if actual != value:
                    return self._blocked_validation_supervisor(
                        f"controller registry field {field} does not match exact-set "
                        f"follower {expected_claim.follower}"
                    )
            if lease.get("status") != exclusive_status:
                return self._blocked_validation_supervisor(
                    "controller registry follower status does not match the exclusive generation"
                )
            expires_ms = lease.get("expires_ms")
            if type(expires_ms) is not int or expires_ms <= required_through_ms:
                return self._blocked_validation_supervisor(
                    f"controller registry ownership for {expected_claim.follower} is expired "
                    "or too close to expiry"
                )
        exclusive_expires_ms = exclusive.get("expires_ms")
        if type(exclusive_expires_ms) is not int or exclusive_expires_ms <= required_through_ms:
            return self._blocked_validation_supervisor(
                "exclusive controller ownership is expired or too close to expiry"
            )
        return RuntimeDecision(True, SafeModeReason.NONE, "")

    def _renew_validation_controller_registry(self, *, status: str) -> bool:
        path = self.config.ops.validation_controller_registry_path
        if path is None:
            return True
        claims = self._validation_controller_claims()
        if len(claims) != 2 or not path.is_file():
            return False
        registry: ControllerRegistry | None = None
        try:
            registry = ControllerRegistry(path)
            ttl_ms = max(
                self.config.ops.runtime_lease_ttl_ms,
                int(self.config.ops.exchange_action_timeout_s * 1000) + 5_000,
            )
            return registry.renew_exclusive_set(
                claims,
                incarnation_id=self.config.ops.validation_supervisor_incarnation_id,
                observed_ms=now_ms(),
                ttl_ms=ttl_ms,
                status=status,
            )
        except (OSError, sqlite3.Error, ValueError):
            return False
        finally:
            if registry is not None:
                registry.close()

    def containment_watchdog_authority(self) -> dict[str, Any]:
        """CAS-renew this watchdog's exact supervisor generation before local mutation.

        A missing supervisor lease means this generation must contain exposure; it does not
        mean that an older generation may act.  Controller-registry renewal is therefore the
        first operation of every validation watchdog cycle and cleanly distinguishes an
        orphaned current generation from a superseded stale process.
        """

        configured = self.config.ops.validation_supervisor_lease_path is not None
        if not configured:
            return {
                "configured": False,
                "authoritative": True,
                "controller_registry_renewed": True,
                "detail": "bounded-validation generation fencing is not configured",
            }
        renewed = self._renew_validation_controller_registry(status="guardian")
        return {
            "configured": True,
            "authoritative": renewed,
            "controller_registry_renewed": renewed,
            "detail": (
                "exact-two controller generation renewed"
                if renewed
                else (
                    "guardian generation is superseded or controller ownership cannot be "
                    "proven; stale process is hard-quiesced"
                )
            ),
        }

    def validation_generation_authority(self) -> dict[str, Any]:
        """Read the exact-two generation identity without renewing or mutating it."""

        configured = self.config.ops.validation_controller_registry_path is not None
        if not configured:
            return {
                "configured": False,
                "authoritative": True,
                "detail": "bounded-validation generation fencing is not configured",
            }
        decision = self._validation_controller_registry_decision()
        return {
            "configured": True,
            "authoritative": decision.ok,
            "detail": (
                "exact-two controller generation is current" if decision.ok else decision.detail
            ),
        }

    def _hard_quiesced_watchdog_result(
        self,
        *,
        observed_ms: int,
        authority: dict[str, Any],
    ) -> dict[str, Any]:
        """Return a stable, side-effect-free result for a superseded watchdog."""

        return to_jsonable(
            {
                "observed_ms": observed_ms,
                "pending_before": 0,
                "watched": [],
                "cancellations": [],
                "settled": [],
                "errors": [],
                "validation_supervisor": {
                    "configured": bool(authority.get("configured")),
                    "allows_new_risk": False,
                    "detail": str(authority.get("detail") or "generation is superseded"),
                    "containment_active": False,
                    "controller_registry_renewed": False,
                    "hard_quiesced": True,
                },
                "validation_market_universe": None,
                "validation_cleanup": [],
                "unowned_orders": [],
                "positions": None,
                "open_orders": None,
                "kill_switch_active": self._kill_switch_path().exists(),
                "hard_quiesced": True,
                "terminate": True,
            }
        )

    @staticmethod
    def _blocked_validation_supervisor(detail: str) -> RuntimeDecision:
        return RuntimeDecision(
            False,
            SafeModeReason.LIVE_BLOCKED,
            f"validation supervisor blocked new risk: {detail}",
        )

    def _kill_switch_path(self):
        return ExecutionGuard(
            risk=self.config.risk,
            ops=self.config.ops,
            store=self.store,
            asset_meta={},
            mids={},
            mode=self.config.mode,
        ).kill_switch_path()

    def _last_mile_pre_send_check(
        self, action: str, risk_increasing: bool
    ) -> FollowerIntent | None:
        kill_switch = self._kill_switch_path()
        if kill_switch.exists() and not (not risk_increasing and self._watchdog_containment_active):
            detail = f"{action} blocked by last-mile gate: kill switch file exists: {kill_switch}"
            self.safe_mode.trip(SafeModeReason.OPERATOR_KILL_SWITCH, detail)
            raise PreSendBlockedError(detail)

        # A noop authorization probe cannot mutate orders, positions, or leverage. It must
        # remain usable while safe mode is active so the clearance gate can prove that the
        # configured signer is still authorized. The lease, signer-global nonce lock,
        # REST throttle, signed deadline, and kill switch remain enforced around the call.
        if action == "auth_probe":
            return None

        if not risk_increasing:
            self.safe_mode.refresh_from_store()
            if (
                self.safe_mode.enabled
                and not self._watchdog_containment_active
                and not self._safe_mode_allows_automatic_reduction()
            ):
                detail = (
                    f"{action} blocked by last-mile gate: safe-mode reason "
                    f"{self.safe_mode.reason.value} does not permit automatic reduction"
                )
                raise PreSendBlockedError(detail)
        if risk_increasing:
            decision = self._runtime_allows_exchange_action(count_rate=False)
            if not decision.ok:
                self.safe_mode.trip(decision.reason, decision.detail)
                raise PreSendBlockedError(f"{action} blocked by last-mile gate: {decision.detail}")
        observed = now_ms()
        # Explicit containment actions do not depend on the source target. Copy-policy
        # reductions (`place_intent`) and leverage changes still do, so they retain both
        # freshness gates. Every signed order mutation still requires fresh follower truth.
        source_dependent = risk_increasing or action in {"place_intent", "update_leverage"}
        freshness_checks = []
        if source_dependent:
            freshness_checks.append(
                (
                    "source",
                    self._active_plan_source_observed_ms,
                    self.config.risk.stale_source_ms,
                    SafeModeReason.STALE_SOURCE,
                )
            )
        freshness_checks.append(
            (
                "follower",
                self._active_plan_follower_observed_ms,
                self.config.risk.stale_follower_ms,
                SafeModeReason.STALE_FOLLOWER,
            )
        )
        for label, observed_ms, threshold_ms, reason in freshness_checks:
            age_ms = (
                self._observation_age_ms(label, observed_ms)
                if observed_ms > 0
                else threshold_ms + 1
            )
            if age_ms is None:
                detail = (
                    f"{action} blocked by last-mile gate: "
                    f"{self.safe_mode.detail or f'{label} observation is invalid'}"
                )
                raise PreSendBlockedError(detail)
            if age_ms > threshold_ms:
                detail = (
                    f"{action} blocked by last-mile gate: {label} planning truth is "
                    f"{age_ms}ms old (limit {threshold_ms}ms)"
                )
                self.safe_mode.trip(reason, detail)
                raise PreSendBlockedError(detail)
        if risk_increasing and action.startswith("place_") and self._watchdog_dead_man_degraded:
            watchdog = self.containment_watchdog_status()
            if not watchdog.get("ready"):
                detail = (
                    f"{action} blocked by last-mile gate: independent containment watchdog "
                    "heartbeat is not ready"
                )
                self.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, detail)
                raise PreSendBlockedError(detail)
        if risk_increasing and action.startswith("place_") and not self._testnet_dead_man_degraded:
            deadline = self._active_dead_man_deadline_ms
            minimum_remaining_ms = max(
                int(self.config.ops.exchange_action_timeout_s * Decimal("1000")),
                1_000,
            )
            remaining_ms = (deadline - observed) if deadline is not None else -1
            if remaining_ms <= minimum_remaining_ms:
                detail = (
                    f"{action} blocked by last-mile gate: dead-man protection has "
                    f"{remaining_ms}ms remaining; requires more than {minimum_remaining_ms}ms"
                )
                self.safe_mode.trip(SafeModeReason.ORDER_TIMEOUT, detail)
                raise PreSendBlockedError(detail)
        if risk_increasing and action == "place_intent":
            return self._revalidate_active_hip3_dispatch(action=action)
        return None

    def _revalidate_active_hip3_dispatch(self, *, action: str) -> FollowerIntent | None:
        intent = self._active_dispatch_intent
        asset_meta = self._active_dispatch_asset_meta
        if (
            intent is None
            or intent.action != IntentAction.OPEN
            or intent.reduce_only
            or not market_dex(intent.coin)
        ):
            return None
        if asset_meta is None:
            detail = f"{action} blocked by last-mile gate: {intent.coin} metadata is unavailable"
            self.safe_mode.trip(SafeModeReason.UNSUPPORTED_SYMBOL, detail)
            raise PreSendBlockedError(detail)
        try:
            assessment = self.load_hip3_round_trip_assessment(
                intent.coin,
                opening_side=intent.side,
                requested_size=intent.size,
                asset_meta=asset_meta,
            )
        except Exception as exc:
            assessment = None
            blockers = [f"{intent.coin} HIP-3 pre-send market check failed: {exc}"]
        else:
            assert assessment is not None
            blockers = list(assessment.blockers)
        if assessment is not None and assessment.retryable_liquidity:
            deferral = self._hip3_liquidity_deferral(
                intent,
                assessment.blockers,
                stage="signed_dispatch_boundary",
            )
            self._active_dispatch_liquidity_deferral = deferral
            detail = f"{action} deferred by last-mile HIP-3 liquidity gate: " + "; ".join(
                deferral.blockers
            )
            raise PreSendBlockedError(detail)
        quote = assessment.quote if assessment is not None else None
        if quote is not None:
            observed = now_ms()
            if (
                quote.coin != canonical_market_symbol(intent.coin)
                or quote.opening_side != intent.side
                or quote.requested_size != intent.size
            ):
                blockers.append(f"{intent.coin} final quote identity does not match the intent")
            if observed - quote.observed_ms > self.config.risk.stale_source_ms:
                blockers.append(f"{intent.coin} final quote is stale at signed dispatch")
            price = quote.entry_limit
            distance = (
                quote.oracle_px * self.config.risk.hip3_oracle_envelope_bps / Decimal("10000")
            )
            lower = quote.oracle_px - distance
            upper = quote.oracle_px + distance
            if price is None or not lower <= price <= upper:
                blockers.append(
                    f"{intent.coin} persisted entry limit moved outside the fresh oracle envelope"
                )
            elif intent.side == "buy" and price < quote.entry_worst_px:
                blockers.append(
                    f"{intent.coin} buy limit {price} no longer crosses fresh entry depth "
                    f"through {quote.entry_worst_px}"
                )
            elif intent.side == "sell" and price > quote.entry_worst_px:
                blockers.append(
                    f"{intent.coin} sell limit {price} no longer crosses fresh entry depth "
                    f"through {quote.entry_worst_px}"
                )
        if quote is None or blockers:
            detail = f"{action} blocked by last-mile HIP-3 gate: " + "; ".join(blockers)
            self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
            raise PreSendBlockedError(detail)
        final_notional = self._hip3_open_notional_bound(intent, quote)
        if final_notional > self.config.risk.max_notional_usd:
            deferral = self._hip3_liquidity_deferral(
                intent,
                [
                    f"{action} final HIP-3 IOC notional {final_notional} exceeds per-order "
                    f"cap {self.config.risk.max_notional_usd}; current truth must be "
                    "replanned at a cap-safe size"
                ],
                stage="signed_dispatch_cap_reprice",
            )
            self._active_dispatch_liquidity_deferral = deferral
            detail = f"{action} deferred by last-mile HIP-3 cap gate: " + "; ".join(
                deferral.blockers
            )
            raise PreSendBlockedError(detail)
        resolved = replace(
            intent,
            price=quote.entry_limit,
            execution_proof={
                **quote.to_payload(),
                "post_send_retry_identity": self._hip3_ioc_retry_identity(intent),
            },
        )
        if not self.store.freeze_prepared_hip3_dispatch(resolved):
            detail = (
                f"{action} blocked by last-mile HIP-3 gate: prepared intent "
                "identity or phase changed before atomic dispatch freeze"
            )
            self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
            raise PreSendBlockedError(detail)
        dispatch_row = self.store.intent_by_cloid(resolved.cloid)
        if dispatch_row is not None:
            try:
                self._active_dispatch_attempt_started_ms = int(
                    dispatch_row.get("attempt_updated_ms") or 0
                )
            except (TypeError, ValueError):
                self._active_dispatch_attempt_started_ms = None
        self._active_dispatch_intent = resolved
        self._active_dispatch_round_trip_quote = quote
        return resolved

    def _safe_mode_allows_automatic_reduction(self) -> bool:
        self.safe_mode.refresh_from_store()
        if not self.safe_mode.enabled:
            return True
        allowed_reasons = {
            SafeModeReason.PARTIAL_FILL,
            SafeModeReason.RESTART_MID_FILL,
            SafeModeReason.CANCEL_REJECT,
            SafeModeReason.ORDER_TIMEOUT,
            SafeModeReason.REST_LAG,
            SafeModeReason.STALE_SOURCE,
            SafeModeReason.RATE_LIMIT,
            SafeModeReason.PRECISION_ERROR,
            SafeModeReason.MARGIN_ERROR,
            SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
            SafeModeReason.RISK_LIMIT,
            SafeModeReason.DUPLICATE_INTENT,
            SafeModeReason.CIRCUIT_BREAKER,
        }
        if self.safe_mode.reason not in allowed_reasons:
            return False
        observed = self._active_plan_follower_observed_ms
        if observed <= 0:
            return False
        age_ms = self._observation_age_ms("follower", observed)
        return age_ms is not None and age_ms <= self.config.risk.stale_follower_ms

    def _reset_signed_action_context(self) -> None:
        self._active_plan_source_observed_ms = 0
        self._active_plan_follower_observed_ms = 0
        self._source_observation_context = None
        self._follower_observation_context = None
        self._active_dead_man_deadline_ms = None
        self._testnet_dead_man_degraded = False
        self._watchdog_dead_man_degraded = False
        self._watchdog_containment_active = False
        self._dead_man_eligibility_cache = None
        self._active_dispatch_intent = None
        self._active_dispatch_asset_meta = None
        self._active_dispatch_round_trip_quote = None
        self._active_dispatch_liquidity_deferral = None
        self._active_dispatch_attempt_started_ms = None

    def _recovery_containment_blockers(
        self,
        *,
        desired_positions: dict[str, Position],
        follower_positions: dict[str, Position],
        intents: list[FollowerIntent],
    ) -> list[str]:
        """Return any way a disconnect-recovery cycle could add follower risk."""

        allowed = {canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols}
        actual = {
            canonical_market_symbol(coin): position
            for coin, position in follower_positions.items()
            if position.size != 0
        }
        desired = {
            canonical_market_symbol(coin): position
            for coin, position in desired_positions.items()
            if position.size != 0
        }
        blockers: list[str] = []
        for coin in sorted(set(actual) - allowed):
            blockers.append(f"actual follower symbol {coin} is outside the configured allowlist")
        for coin in sorted(set(desired) - allowed):
            blockers.append(f"desired follower symbol {coin} is outside the configured allowlist")
        for coin, target in sorted(desired.items()):
            current = actual.get(coin)
            if current is None or current.size == 0:
                blockers.append(f"{coin} would open new exposure")
                continue
            if current.size * target.size < 0:
                blockers.append(f"{coin} would reverse follower direction")
                continue
            if abs(target.size) > abs(current.size):
                blockers.append(
                    f"{coin} target size {target.size} exceeds current size {current.size}"
                )
            if target.leverage is not None:
                if current.leverage is None:
                    blockers.append(f"{coin} current leverage is unproven")
                elif target.leverage > current.leverage:
                    blockers.append(
                        f"{coin} target leverage {target.leverage} exceeds current leverage "
                        f"{current.leverage}"
                    )
        for intent in intents:
            coin = canonical_market_symbol(intent.coin)
            if coin not in allowed:
                blockers.append(f"intent symbol {coin} is outside the configured allowlist")
            if intent.action == IntentAction.OPEN and intent.size > 0:
                blockers.append(f"{coin} recovery intent would open or add exposure")
            elif (
                intent.action in {IntentAction.CLOSE, IntentAction.REDUCE}
                and not intent.reduce_only
            ):
                blockers.append(f"{coin} recovery reduction is not reduce-only")
        return list(dict.fromkeys(blockers))

    @staticmethod
    def _positions_match_exact(
        expected_positions: dict[str, Position],
        actual_positions: dict[str, Position],
    ) -> bool:
        expected = {
            canonical_market_symbol(coin): position
            for coin, position in expected_positions.items()
            if position.size != 0
        }
        actual = {
            canonical_market_symbol(coin): position
            for coin, position in actual_positions.items()
            if position.size != 0
        }
        if set(expected) != set(actual):
            return False
        return all(
            actual[coin].size == position.size
            and (position.leverage is None or actual[coin].leverage == position.leverage)
            for coin, position in expected.items()
        )

    def _finalize_execution_truth(
        self,
        snapshot: ReconcileSnapshot,
        *,
        trigger: str,
        keep_incident_safe: bool,
    ) -> dict[str, Any]:
        """Commit only truth proven by a fresh, flat-order reconciliation."""

        result: dict[str, Any] = {
            "status": "not_applicable",
            "target_state_id": "",
            "committed_target": False,
            "checkpoint": None,
        }
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return result
        if not self._check_follower_freshness(snapshot.observed_ms):
            result["status"] = "stale_reconcile"
            return result
        if snapshot.open_orders:
            result["status"] = "open_orders_remain"
            return result
        pending = self.store.pending_intents(self.config.mode)
        if pending:
            result["status"] = "attempts_unresolved"
            result["pending_intent_ids"] = [row["intent_id"] for row in pending]
            return result

        unresolved = self.store.unresolved_desired_states(
            mode=self.config.mode,
            source_wallet=self.config.source_wallet,
            action_account=self._effective_action_account(),
            source_network=self.config.resolved_source_network.value,
        )
        candidate = unresolved[-1] if unresolved else None
        if candidate is not None:
            result["target_state_id"] = candidate.state_id
            if self._positions_match_exact(candidate.positions, snapshot.positions):
                self.store.commit_desired_state(candidate.state_id)
                result["status"] = "target_committed"
                result["committed_target"] = True
            else:
                result["status"] = "actual_checkpoint_committed"
        else:
            committed = self.store.latest_desired_positions(
                self.config.mode,
                source_wallet=self.config.source_wallet,
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
                committed_only=True,
            )
            if committed is not None and self._positions_match_exact(committed, snapshot.positions):
                result["status"] = "committed_baseline_verified"
            else:
                result["status"] = "actual_checkpoint_committed"

        if result["status"] == "actual_checkpoint_committed":
            checkpoint_created_ms = now_ms()
            checkpoint_positions = {
                canonical_market_symbol(coin): position
                for coin, position in snapshot.positions.items()
                if position.size != 0
            }
            checkpoint = DesiredState(
                state_id=deterministic_cloid(
                    "verified-actual-checkpoint",
                    candidate.state_id if candidate is not None else "legacy-unlinked",
                    snapshot.snapshot_id,
                    checkpoint_positions,
                ),
                source_event_key=(
                    candidate.source_event_key
                    if candidate is not None
                    else f"execution-recovery:{trigger}"
                ),
                mode=self.config.mode,
                positions=checkpoint_positions,
                reason=(
                    f"fresh follower truth checkpoint after {trigger}; "
                    "the planned target was not promoted"
                ),
                created_ms=checkpoint_created_ms,
                source_wallet=self.config.source_wallet.lower(),
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            if not self.store.append_desired_state(checkpoint):
                existing = self.store.desired_state(checkpoint.state_id)
                if existing is None or not self._positions_match_exact(
                    existing.positions, checkpoint.positions
                ):
                    raise JournalIntegrityError(
                        "recovery checkpoint identity conflicts with journal truth"
                    )
            self.store.commit_desired_state(checkpoint.state_id)
            result["checkpoint"] = checkpoint

        if keep_incident_safe and not self.safe_mode.enabled:
            self.safe_mode.trip(
                SafeModeReason.RESTART_MID_FILL,
                f"{trigger} finalized durable journal truth; operator review is still required",
            )
        if keep_incident_safe or result["status"] == "actual_checkpoint_committed":
            self.store.append_control_audit(
                control="execution_plan_finalization",
                status=str(result["status"]),
                detail=f"durable execution truth finalized after {trigger}",
                payload={
                    "trigger": trigger,
                    "snapshot_id": snapshot.snapshot_id,
                    "target_state_id": result["target_state_id"],
                    "checkpoint_state_id": (
                        result["checkpoint"].state_id if result["checkpoint"] else ""
                    ),
                    "incident_kept_safe": self.safe_mode.enabled,
                },
            )
        return result

    def _persistent_exchange_rate_decision(self) -> RuntimeDecision:
        observed = now_ms()
        window_ms = self.exchange_rate_limiter.window_ms
        stats = self.store.recent_counted_exchange_action_stats(observed - window_ms)
        count = stats["count"]
        if count < self.config.ops.max_exchange_actions_per_minute:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        oldest = stats["oldest_ms"] or observed
        retry_ms = max(0, window_ms - (observed - oldest))
        return RuntimeDecision(
            ok=False,
            reason=SafeModeReason.RATE_LIMIT,
            detail=(
                f"persistent action rate limit hit from journal; "
                f"{count} actions in {window_ms}ms, retry after {retry_ms}ms"
            ),
        )

    def _persistent_circuit_breaker_decision(self) -> RuntimeDecision:
        stats = self.store.consecutive_exchange_failure_stats()
        failures = stats["consecutive_failures"]
        if failures < self.config.ops.circuit_breaker_failure_threshold:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        latest = stats["latest_failure_ms"] or now_ms()
        elapsed = now_ms() - latest
        cooldown = self.config.ops.circuit_breaker_cooldown_ms
        if elapsed >= cooldown:
            return RuntimeDecision(True, SafeModeReason.NONE, "")
        return RuntimeDecision(
            ok=False,
            reason=SafeModeReason.CIRCUIT_BREAKER,
            detail=(
                f"persistent circuit breaker open after {failures} consecutive journaled "
                f"exchange failures; {cooldown - elapsed}ms cooldown remaining"
            ),
        )

    def _record_runtime_result(self, report: ExecutionReport) -> None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return
        if self._is_proven_hip3_ioc_zero_fill_report(report):
            return
        is_dead_man = str(report.intent_id).startswith("dead-man-")
        if (
            is_dead_man
            and self.config.mode == Mode.TESTNET
            and self._is_testnet_dead_man_volume_rejection(str(report.payload))
        ):
            return
        elapsed_s = self._execution_report_decimal(report, "elapsed_s", default=Decimal("0"))
        if elapsed_s is not None and elapsed_s > self.config.ops.exchange_action_timeout_s:
            self.safe_mode.trip(
                SafeModeReason.ORDER_TIMEOUT,
                f"{report.cloid} exchange action took {elapsed_s}s > {self.config.ops.exchange_action_timeout_s}s",
            )
        if report.status == IntentStatus.REJECTED:
            decision = self.circuit_breaker.record_failure()
            self.shield.exchange_error(str(report.payload))
            if not decision.ok:
                self.safe_mode.trip(decision.reason, decision.detail)
        elif report.status == IntentStatus.SENT:
            self.safe_mode.trip(
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                f"{report.cloid} signed action outcome is unknown: {report.exchange_status}",
            )
        elif is_dead_man:
            return
        elif report.status in {IntentStatus.FILLED, IntentStatus.ACKED, IntentStatus.CANCELED}:
            self.circuit_breaker.record_success()

    def _execution_report_decimal(
        self,
        report: ExecutionReport,
        key: str,
        *,
        default: Decimal | None = None,
    ) -> Decimal | None:
        raw = report.payload.get(key) if isinstance(report.payload, dict) else None
        if raw is None or raw == "":
            return default
        try:
            return parse_decimal(raw)
        except (ArithmeticError, TypeError, ValueError):
            self.safe_mode.trip(
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                f"{report.cloid} exchange report field {key} is not a finite decimal",
            )
            return None

    def _handle_non_terminal_exchange_ack(
        self,
        report: ExecutionReport,
        intent: FollowerIntent | None = None,
        *,
        require_full_fill: bool = False,
    ) -> ExecutionReport:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return report
        if report.status != IntentStatus.ACKED or self.safe_mode.enabled:
            return report
        if report.exchange_status == "partial_fill":
            expected = self._execution_report_decimal(report, "expected_size")
            filled = self._execution_report_decimal(report, "filled_size")
            if expected is None or filled is None:
                return report
            residual = expected - filled
            residual_notional = (
                residual * intent.price
                if intent is not None and intent.price is not None and residual >= 0
                else None
            )
            if filled == 0 and require_full_fill:
                coin = canonical_market_symbol(intent.coin) if intent is not None else "position"
                detail = (
                    f"{coin} reversal cannot reopen because the residual position cannot be "
                    f"flattened automatically: {report.cloid} filled 0 of {expected}"
                )
                self.safe_mode.trip(SafeModeReason.PARTIAL_FILL, detail)
                terminal = replace(
                    report,
                    report_id=deterministic_cloid(
                        "reversal-ioc-unfilled",
                        report.intent_id,
                        report.cloid,
                        expected,
                    ),
                    status=IntentStatus.CANCELED,
                    exchange_status="ioc_unfilled",
                    payload={
                        **dict(report.payload),
                        "residual_size": residual,
                        "residual_notional": residual_notional,
                        "requires_operator_reconcile": True,
                    },
                )
                self.store.append_execution_report(terminal)
                return terminal
            if (
                filled > 0
                and residual >= 0
                and (
                    residual <= self.config.risk.min_order_size
                    or (
                        residual_notional is not None
                        and residual_notional < HYPERLIQUID_PERP_MIN_NOTIONAL_USD
                    )
                )
            ):
                accepted = replace(
                    report,
                    report_id=deterministic_cloid(
                        "dust-partial-accepted",
                        report.intent_id,
                        report.cloid,
                        expected,
                        filled,
                    ),
                    status=IntentStatus.FILLED,
                    exchange_status="dust_residual_accepted",
                    payload={
                        **dict(report.payload),
                        "residual_size": residual,
                        "residual_notional": residual_notional,
                        "min_order_size": self.config.risk.min_order_size,
                        "min_notional_usd": HYPERLIQUID_PERP_MIN_NOTIONAL_USD,
                    },
                )
                self.store.append_execution_report(accepted)
                return accepted
            self.shield.partial_fill(expected, filled, report.cloid)
            return report
        if report.exchange_status == "overfill":
            expected = self._execution_report_decimal(report, "expected_size")
            filled = self._execution_report_decimal(report, "filled_size")
            if expected is None or filled is None:
                return report
            self.safe_mode.trip(
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                f"{report.cloid} reported filled size {filled} greater than expected {expected}; "
                "reconcile follower truth before new risk",
            )
            return report
        self.safe_mode.trip(
            SafeModeReason.RESTART_MID_FILL,
            f"{report.cloid} acknowledged but not terminal; settle pending or reconcile before new risk",
        )
        return report

    def _handle_cancel_report(self, report: ExecutionReport) -> None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return
        if report.status == IntentStatus.CANCELED:
            return
        if (
            self.safe_mode.enabled
            and self.safe_mode.reason != SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE
        ):
            return
        order_status = None
        if self.execution_adapter is not None:
            try:
                payload = self.execution_adapter.order_status(report.cloid)
            except Exception as exc:
                order_status = f"lookup_failed:{exc}"
            else:
                _, order_status = classify_order_status(payload)
        self.shield.cancel_reject(report.cloid, order_status=order_status)

    def _blocked_report(
        self, intent: FollowerIntent, reason: SafeModeReason, detail: str
    ) -> ExecutionReport:
        report = self._blocked_exchange_report(
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            reason=reason,
            detail=detail,
        )
        if intent.execution_proof:
            report = replace(
                report,
                payload={**report.payload, "execution_proof": intent.execution_proof},
            )
        return report

    def _blocked_exchange_report(
        self, *, intent_id: str, cloid: str, reason: SafeModeReason, detail: str
    ) -> ExecutionReport:
        return ExecutionReport(
            report_id=deterministic_cloid("blocked-report", intent_id, cloid, reason.value, detail),
            intent_id=intent_id,
            cloid=cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="blocked:" + reason.value,
            exchange_ts_ms=now_ms(),
            payload={"reason": reason.value, "detail": detail},
        )

    @staticmethod
    def _hip3_liquidity_deferred_report(
        intent: FollowerIntent,
        deferral: Hip3LiquidityDeferral,
    ) -> ExecutionReport:
        detail = "HIP-3 liquidity deferred before signed send: " + "; ".join(deferral.blockers)
        return ExecutionReport(
            report_id=deterministic_cloid(
                "hip3-liquidity-deferred",
                intent.intent_id,
                intent.cloid,
                deferral.retry_not_before_ms,
            ),
            intent_id=intent.intent_id,
            cloid=intent.cloid,
            status=IntentStatus.SKIPPED,
            exchange_status="pre_send_blocked",
            exchange_ts_ms=now_ms(),
            payload={
                "error": detail,
                "liquidity_deferral": to_jsonable(deferral),
                "signed_action_performed": False,
            },
        )

    def _exception_exchange_report(
        self,
        *,
        intent_id: str,
        cloid: str,
        detail: str,
        elapsed_s: Decimal,
    ) -> ExecutionReport:
        return ExecutionReport(
            report_id=deterministic_cloid("exception-report", intent_id, cloid, detail, elapsed_s),
            intent_id=intent_id,
            cloid=cloid,
            status=IntentStatus.SENT,
            exchange_status="transport_unknown",
            exchange_ts_ms=now_ms(),
            payload={"error": detail, "elapsed_s": elapsed_s},
        )

    def _timed_exchange_action(
        self,
        *,
        intent_id: str,
        cloid: str,
        count_rate: bool,
        risk_reducing: bool = False,
        record_runtime: bool = True,
        signed_action_kind: str | None = None,
        signed_action_payload: dict[str, Any] | None = None,
        action,
    ) -> ExecutionReport:
        decision = self._runtime_allows_exchange_action(
            count_rate=count_rate,
            risk_reducing=risk_reducing,
        )
        if not decision.ok:
            self.safe_mode.trip(decision.reason, decision.detail)
            return self._blocked_exchange_report(
                intent_id=intent_id,
                cloid=cloid,
                reason=decision.reason,
                detail=decision.detail,
            )
        signed_attempt_id: str | None = None
        if signed_action_kind is not None:
            signed_attempt_id = cloid
            account = self._effective_action_account()
            network = "mainnet" if self.config.mode == Mode.LIVE else "testnet"
            prepared = self.store.prepare_signed_action_attempt(
                attempt_id=signed_attempt_id,
                intent_id=intent_id,
                cloid=cloid,
                action=signed_action_kind,
                mode=self.config.mode,
                account=account,
                network=network,
                payload=signed_action_payload or {},
            )
            if not prepared:
                unresolved = self.store.unresolved_signed_action_attempts(
                    self.config.mode,
                    account=account,
                    network=network,
                )
                reason = (
                    SafeModeReason.RESTART_MID_FILL
                    if unresolved
                    else SafeModeReason.DUPLICATE_INTENT
                )
                detail = (
                    "durable signed-action recovery requires explicit operator review; "
                    "refusing to retry or overlap a non-order mutation"
                    if unresolved
                    else f"signed-action attempt {signed_attempt_id} already exists; refusing retry"
                )
                self.safe_mode.trip(reason, detail)
                return self._blocked_exchange_report(
                    intent_id=intent_id,
                    cloid=cloid,
                    reason=reason,
                    detail=detail,
                )
            if not self.store.begin_signed_action_dispatch(signed_attempt_id):
                detail = (
                    f"signed-action attempt {signed_attempt_id} is not durably PREPARED; "
                    "refusing dispatch"
                )
                self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                return self._blocked_exchange_report(
                    intent_id=intent_id,
                    cloid=cloid,
                    reason=SafeModeReason.DUPLICATE_INTENT,
                    detail=detail,
                )

        dispatch_started = self.store.begin_intent_dispatch(intent_id)
        if dispatch_started is False:
            detail = f"{intent_id} is not a durable PREPARED attempt; refusing duplicate dispatch"
            self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
            return self._blocked_exchange_report(
                intent_id=intent_id,
                cloid=cloid,
                reason=SafeModeReason.DUPLICATE_INTENT,
                detail=detail,
            )
        started = monotonic()
        try:
            report = action()
        except PreSendBlockedError as exc:
            elapsed_s = Decimal(str(round(monotonic() - started, 6)))
            report = ExecutionReport(
                report_id=deterministic_cloid(
                    "pre-send-blocked-report", intent_id, cloid, str(exc), elapsed_s
                ),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.SKIPPED,
                exchange_status="pre_send_blocked",
                exchange_ts_ms=now_ms(),
                payload={"error": str(exc), "elapsed_s": elapsed_s},
            )
            if signed_attempt_id is not None and not self.store.finish_signed_action_attempt(
                signed_attempt_id, report
            ):
                raise JournalIntegrityError(
                    f"signed-action attempt {signed_attempt_id} could not persist its outcome"
                )
            return report
        except Exception as exc:
            elapsed_s = Decimal(str(round(monotonic() - started, 6)))
            report = self._exception_exchange_report(
                intent_id=intent_id,
                cloid=cloid,
                detail=f"exchange action raised: {exc}",
                elapsed_s=elapsed_s,
            )
            if signed_attempt_id is not None and not self.store.finish_signed_action_attempt(
                signed_attempt_id, report
            ):
                raise JournalIntegrityError(
                    f"signed-action attempt {signed_attempt_id} could not persist its outcome"
                )
            if record_runtime:
                self._record_runtime_result(report)
            return report
        elapsed_s = Decimal(str(round(monotonic() - started, 6)))
        payload = dict(report.payload)
        payload["elapsed_s"] = elapsed_s
        report = replace(report, payload=payload)
        if signed_attempt_id is not None and not self.store.finish_signed_action_attempt(
            signed_attempt_id, report
        ):
            raise JournalIntegrityError(
                f"signed-action attempt {signed_attempt_id} could not persist its outcome"
            )
        if record_runtime:
            self._record_runtime_result(report)
        return report

    def _schedule_dead_man_cancel(
        self,
        *,
        scheduled_time_ms: int | None,
        operation: str,
        count_rate: bool,
    ) -> ExecutionReport | None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return None
        if self.execution_adapter is None:
            return None
        action_name = "clear" if scheduled_time_ms is None else "schedule"
        account = self.config.exchange.follower_account_address.lower()
        intent_id = f"dead-man-{action_name}:{self.config.mode.value}:{account}"
        cloid = deterministic_cloid(
            "dead-man",
            action_name,
            self.config.mode.value,
            account,
            operation,
            scheduled_time_ms or "none",
            now_ms(),
        )
        if scheduled_time_ms is None and self._watchdog_dead_man_degraded:
            self._active_dead_man_deadline_ms = None
            self._watchdog_dead_man_degraded = False
            return ExecutionReport(
                report_id=deterministic_cloid("watchdog-containment-release", cloid),
                intent_id=intent_id,
                cloid=cloid,
                status=IntentStatus.ACKED,
                exchange_status="watchdog_containment_released",
                exchange_ts_ms=now_ms(),
                payload={
                    "protection": "independent_containment_watchdog",
                    "signed_action_performed": False,
                },
            )
        if self.config.mode == Mode.LIVE and scheduled_time_ms is not None:
            provider = getattr(self.execution_adapter, "dead_man_eligibility", None)
            if callable(provider):
                try:
                    eligibility = to_jsonable(provider())
                except Exception:
                    eligibility = None
                self._dead_man_eligibility_cache = eligibility
                if isinstance(eligibility, dict) and not eligibility.get("eligible"):
                    watchdog = self.containment_watchdog_status()
                    if (
                        self.config.ops.dead_man_policy == DeadManPolicy.WATCHDOG_FALLBACK
                        and watchdog.get("ready")
                    ):
                        self._active_dead_man_deadline_ms = scheduled_time_ms
                        self._watchdog_dead_man_degraded = True
                        return ExecutionReport(
                            report_id=deterministic_cloid("watchdog-containment-arm", cloid),
                            intent_id=intent_id,
                            cloid=cloid,
                            status=IntentStatus.ACKED,
                            exchange_status="watchdog_containment_armed",
                            exchange_ts_ms=now_ms(),
                            payload={
                                "scheduled_time_ms": scheduled_time_ms,
                                "protection": "independent_containment_watchdog",
                                "watchdog": watchdog,
                                "dead_man_eligibility": eligibility,
                                "signed_action_performed": False,
                            },
                        )
                    return ExecutionReport(
                        report_id=deterministic_cloid("dead-man-volume-block", cloid),
                        intent_id=intent_id,
                        cloid=cloid,
                        status=IntentStatus.REJECTED,
                        exchange_status="dead_man_volume_ineligible",
                        exchange_ts_ms=now_ms(),
                        payload={
                            "dead_man_eligibility": eligibility,
                            "watchdog": watchdog,
                            "signed_action_performed": False,
                        },
                    )
        report = self._timed_exchange_action(
            intent_id=intent_id,
            cloid=cloid,
            count_rate=count_rate,
            risk_reducing=scheduled_time_ms is not None,
            signed_action_kind=f"dead_man_{action_name}",
            signed_action_payload={
                "scheduled_time_ms": scheduled_time_ms,
                "operation": operation,
            },
            action=lambda: self.execution_adapter.schedule_cancel(
                scheduled_time_ms=scheduled_time_ms,
                intent_id=intent_id,
                cloid=cloid,
            ),
        )
        if scheduled_time_ms is None:
            if report.status == IntentStatus.ACKED and report.exchange_status == "dead_man_cleared":
                self._active_dead_man_deadline_ms = None
                self._testnet_dead_man_degraded = False
                self._watchdog_dead_man_degraded = False
        elif report.status == IntentStatus.ACKED:
            self._active_dead_man_deadline_ms = scheduled_time_ms
            self._testnet_dead_man_degraded = False
            self._watchdog_dead_man_degraded = False
        elif self.config.mode == Mode.TESTNET and self._is_testnet_dead_man_volume_rejection(
            str(report.payload)
        ):
            report = replace(
                report,
                payload={
                    **dict(report.payload),
                    "testnet_dead_man_volume_rejection": True,
                },
            )
            self._active_dead_man_deadline_ms = None
            self._testnet_dead_man_degraded = True
        else:
            self._active_dead_man_deadline_ms = None
            self._testnet_dead_man_degraded = False
            self._watchdog_dead_man_degraded = False
        return report

    def _dead_man_blocks_execution(self, report: ExecutionReport | None) -> bool:
        if report is None or report.status == IntentStatus.ACKED:
            return False
        if self.config.mode == Mode.TESTNET and self._is_testnet_dead_man_volume_rejection(
            str(report.payload)
        ):
            return False
        if not self.safe_mode.enabled:
            self.safe_mode.trip(
                SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                "dead-man protection was not acknowledged; new risk is blocked",
            )
        return True

    def _ensure_exchange_leverage_for_intent(
        self,
        intent: FollowerIntent,
        *,
        desired_state: DesiredState,
        follower_positions: dict[str, Position],
        allow_existing_increase: bool = False,
    ) -> ExecutionReport | None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return None
        if self.execution_adapter is None or self.safe_mode.enabled:
            return None
        if intent.action not in {IntentAction.OPEN, IntentAction.NOOP}:
            return None
        if intent.action == IntentAction.OPEN and intent.status == IntentStatus.SKIPPED:
            return None
        target = desired_state.positions.get(intent.coin)
        if target is None or target.size == 0:
            return None
        leverage = int(target.leverage or 1)
        current = follower_positions.get(intent.coin)
        if current is not None and current.leverage == leverage:
            return None
        if (
            current is not None
            and current.size != 0
            and leverage > int(current.leverage or 1)
            and not allow_existing_increase
        ):
            return None
        cloid = deterministic_cloid(
            "leverage", desired_state.state_id, intent.coin, leverage, now_ms()
        )
        if not self._refresh_active_dispatch_truth_for_fallback(
            intent.coin,
            expected_source_event_key=intent.source_event_key,
        ):
            reason = (
                self.safe_mode.reason if self.safe_mode.enabled else SafeModeReason.STALE_SOURCE
            )
            detail = (
                self.safe_mode.detail
                if self.safe_mode.enabled
                else f"{intent.coin} dispatch truth refresh failed"
            )
            report = self._blocked_exchange_report(
                intent_id=f"leverage:{desired_state.state_id}:{intent.coin}:{leverage}",
                cloid=cloid,
                reason=reason,
                detail=detail,
            )
            self.store.append_execution_report(report)
            return report
        report = self._timed_exchange_action(
            intent_id=f"leverage:{desired_state.state_id}:{intent.coin}:{leverage}",
            cloid=cloid,
            count_rate=True,
            record_runtime=False,
            signed_action_kind="update_leverage_cross",
            signed_action_payload={
                "coin": intent.coin,
                "leverage": leverage,
                "is_cross": True,
                "desired_state_id": desired_state.state_id,
            },
            action=lambda coin=intent.coin, leverage=leverage: (
                self.execution_adapter.update_leverage(
                    coin,
                    leverage,
                    is_cross=True,
                )
            ),
        )
        if self._cross_margin_not_allowed(report):
            report = replace(
                report,
                payload={
                    **dict(report.payload),
                    "expected_cross_margin_fallback": True,
                },
            )
            isolated_cloid = deterministic_cloid(
                "leverage",
                desired_state.state_id,
                intent.coin,
                leverage,
                "isolated",
                now_ms(),
            )
            if not self._refresh_active_dispatch_truth_for_fallback(
                intent.coin,
                expected_source_event_key=intent.source_event_key,
            ):
                reason = (
                    self.safe_mode.reason if self.safe_mode.enabled else SafeModeReason.STALE_SOURCE
                )
                detail = (
                    self.safe_mode.detail
                    if self.safe_mode.enabled
                    else f"{intent.coin} isolated-margin fallback refresh failed"
                )
                isolated_report = self._blocked_exchange_report(
                    intent_id=(
                        f"leverage:{desired_state.state_id}:{intent.coin}:{leverage}:isolated"
                    ),
                    cloid=isolated_cloid,
                    reason=reason,
                    detail=detail,
                )
            else:
                isolated_report = self._timed_exchange_action(
                    intent_id=f"leverage:{desired_state.state_id}:{intent.coin}:{leverage}:isolated",
                    cloid=isolated_cloid,
                    count_rate=True,
                    signed_action_kind="update_leverage_isolated",
                    signed_action_payload={
                        "coin": intent.coin,
                        "leverage": leverage,
                        "is_cross": False,
                        "desired_state_id": desired_state.state_id,
                    },
                    action=lambda coin=intent.coin, leverage=leverage: (
                        self.execution_adapter.update_leverage(
                            coin,
                            leverage,
                            is_cross=False,
                        )
                    ),
                )
            self.store.append_execution_report(report)
            self.store.append_execution_report(isolated_report)
            if isolated_report.status != IntentStatus.ACKED:
                self._record_runtime_result(isolated_report)
            report = isolated_report
        else:
            self.store.append_execution_report(report)
            self._record_runtime_result(report)
        if report.status == IntentStatus.ACKED:
            follower_positions[intent.coin] = Position(
                coin=intent.coin,
                size=current.size if current is not None else Decimal("0"),
                entry_px=current.entry_px if current is not None else None,
                leverage=leverage,
                updated_ms=now_ms(),
            )
        return report

    def _refresh_active_dispatch_truth_for_fallback(
        self,
        coin: str,
        *,
        expected_source_event_key: str = "",
    ) -> bool:
        """Refresh both sides at a signed-action boundary and bind the source plan."""

        try:
            _, _, follower_observed_ms = self._current_follower_truth()
            # Follower truth can require several catalog-scoped reads. Refresh the
            # source last so its short freshness window is not consumed by them.
            source = self.observer.reconcile_once()
        except Exception as exc:
            self.safe_mode.trip(
                SafeModeReason.REST_LAG,
                f"{coin} isolated-margin fallback truth refresh failed: {exc}",
            )
            return False
        if expected_source_event_key and source.planning_key != expected_source_event_key:
            self.safe_mode.trip(
                SafeModeReason.STALE_SOURCE,
                (
                    f"{coin} source changed before signed leverage dispatch; "
                    "the intent must be replanned"
                ),
            )
            return False
        self._last_source_account_value = self._source_snapshot_account_value(source)
        self._active_plan_source_observed_ms = source.observed_ms
        self._active_plan_follower_observed_ms = follower_observed_ms
        return True

    @staticmethod
    def _cross_margin_not_allowed(report: ExecutionReport) -> bool:
        if report.status != IntentStatus.REJECTED:
            return False
        return "cross margin is not allowed" in str(report.payload).lower()

    def _stage_exposure_increasing_batch(
        self,
        intents: list[FollowerIntent],
    ) -> tuple[list[FollowerIntent], list[FollowerIntent]]:
        """Admit a deterministic bounded OPEN/add batch without delaying reductions."""

        opening_intents = sorted(
            (intent for intent in intents if increases_exposure(intent)),
            key=lambda intent: (
                canonical_market_symbol(intent.coin),
                intent.intent_id,
                intent.cloid,
            ),
        )
        if not opening_intents:
            return list(intents), []
        pending_open_count = pending_exposure_increasing_count(
            self.store,
            self.config.mode,
        )
        available_pending_capacity = max(
            0,
            self.config.ops.max_open_intents - pending_open_count,
        )
        admission_count = min(
            self.config.ops.max_new_intents_per_cycle,
            available_pending_capacity,
        )
        admitted_ids = {intent.intent_id for intent in opening_intents[:admission_count]}
        active = [
            intent
            for intent in intents
            if not increases_exposure(intent) or intent.intent_id in admitted_ids
        ]
        deferred = [intent for intent in opening_intents if intent.intent_id not in admitted_ids]
        return active, deferred

    @staticmethod
    def _staged_deferred_open_desired_state(
        desired_state: DesiredState,
        deferred_intents: list[FollowerIntent],
        *,
        follower_positions: dict[str, Position],
        deferral_class: str = "capacity",
    ) -> DesiredState:
        """Build the exact interim target reached by the admitted OPEN/add batch."""

        if deferral_class not in {"capacity", "hip3_liquidity"}:
            raise ValueError(f"unsupported OPEN deferral class: {deferral_class}")

        staged_positions = dict(desired_state.positions)
        canonical_follower_positions = {
            canonical_market_symbol(position.coin or coin): replace(
                position,
                coin=canonical_market_symbol(position.coin or coin),
            )
            for coin, position in follower_positions.items()
        }
        deferred_coins = sorted(
            {canonical_market_symbol(intent.coin) for intent in deferred_intents}
        )
        for coin in deferred_coins:
            current = canonical_follower_positions.get(coin)
            if current is None or current.size == 0:
                staged_positions.pop(coin, None)
            else:
                staged_positions[coin] = current
        deferred_signature = [
            (
                intent.intent_id,
                canonical_market_symbol(intent.coin),
                intent.side,
                intent.size,
            )
            for intent in sorted(
                deferred_intents,
                key=lambda item: (
                    canonical_market_symbol(item.coin),
                    item.intent_id,
                ),
            )
        ]
        return replace(
            desired_state,
            state_id=deterministic_cloid(
                (
                    "staged-hip3-liquidity"
                    if deferral_class == "hip3_liquidity"
                    else "staged-open-batch"
                ),
                desired_state.state_id,
                deferred_signature,
                staged_positions,
            ),
            positions=staged_positions,
            reason=(
                (
                    f"{desired_state.reason}; deferred HIP-3 exposure for "
                    f"{', '.join(deferred_coins)} until fresh two-sided oracle-bounded "
                    "depth can cover entry and exit"
                )
                if deferral_class == "hip3_liquidity"
                else (
                    f"{desired_state.reason}; deferred exposure-increasing intents for "
                    f"{', '.join(deferred_coins)} to the next bounded execution cycle"
                )
            ),
        )

    @staticmethod
    def _stage_exchange_reversals(
        intents: list[FollowerIntent],
    ) -> tuple[list[FollowerIntent], list[FollowerIntent], str]:
        by_coin: dict[str, list[FollowerIntent]] = {}
        for intent in intents:
            if intent.action == IntentAction.NOOP:
                continue
            by_coin.setdefault(intent.coin, []).append(intent)
        deferred_ids: set[str] = set()
        unsafe_coins: list[str] = []
        for coin, coin_intents in by_coin.items():
            if len(coin_intents) <= 1:
                continue
            if (
                len(coin_intents) == 2
                and coin_intents[0].action == IntentAction.CLOSE
                and coin_intents[0].reduce_only
                and coin_intents[1].action == IntentAction.OPEN
                and not coin_intents[1].reduce_only
            ):
                deferred_ids.add(coin_intents[1].intent_id)
                continue
            unsafe_coins.append(coin)
        if unsafe_coins:
            coins = ", ".join(sorted(unsafe_coins))
            return (
                intents,
                [],
                f"unsupported same-cycle exchange intent sequence for {coins}; "
                "close must reconcile before reopen",
            )
        active = [intent for intent in intents if intent.intent_id not in deferred_ids]
        deferred = [intent for intent in intents if intent.intent_id in deferred_ids]
        return active, deferred, ""

    @staticmethod
    def _staged_reversal_desired_state(
        desired_state: DesiredState,
        deferred_intents: list[FollowerIntent],
    ) -> DesiredState:
        staged_positions = dict(desired_state.positions)
        flipped_coins = sorted({intent.coin for intent in deferred_intents})
        for coin in flipped_coins:
            staged_positions.pop(coin, None)
        return replace(
            desired_state,
            state_id=deterministic_cloid(
                "staged-reversal-close",
                desired_state.state_id,
                flipped_coins,
            ),
            positions=staged_positions,
            reason=(
                f"{desired_state.reason}; staged exchange reversal close for "
                f"{', '.join(flipped_coins)}; reopen only after flat reconcile"
            ),
        )

    def pause(self, reason: str = "operator pause") -> dict[str, Any]:
        transition = self.safe_mode.trip(SafeModeReason.PREFLIGHT_FAILED, reason)
        return to_jsonable(transition)

    def resume(self, detail: str = "operator resume") -> dict[str, Any]:
        self.safe_mode.refresh_from_store()
        expected_revision = self.safe_mode.revision
        report, clearance = self._clearance_gate()
        if clearance is not None:
            return clearance
        if self.config.mode in {Mode.TESTNET, Mode.LIVE}:
            return self._resume_exchange_mode(detail, report, expected_revision)
        transition = self.safe_mode.clear_if_revision(expected_revision, detail)
        if transition is None:
            return self._safe_mode_payload(cleared=False)
        return to_jsonable(transition)

    def _resume_exchange_mode(
        self,
        detail: str,
        report: PreflightReport,
        expected_safe_mode_revision: int,
    ) -> dict[str, Any]:
        if self.execution_adapter is None:
            self.safe_mode.trip(
                SafeModeReason.ACCOUNT_NOT_CONFIGURED,
                "cannot resume exchange mode without execution adapter",
            )
            return self._safe_mode_payload(cleared=False)
        if not self._acquire_exchange_lease("resume"):
            return self._safe_mode_payload(cleared=False)
        try:
            if not self._check_no_pending_for_clear():
                return {
                    **self._safe_mode_payload(cleared=False),
                    "preflight": to_jsonable(report),
                }
            try:
                snapshot = self.execution_adapter.reconcile()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"resume follower reconcile failed: {exc}",
                )
                return {
                    **self._safe_mode_payload(cleared=False),
                    "preflight": to_jsonable(report),
                }
            self.store.append_reconcile_snapshot(snapshot)
            cleared = self._clear_after_exchange_reconcile(
                snapshot,
                detail,
                clearance_checked=True,
                expected_safe_mode_revision=expected_safe_mode_revision,
            )
            return {
                **self._safe_mode_payload(cleared=cleared),
                "preflight": to_jsonable(report),
                "snapshot": to_jsonable(snapshot),
            }
        finally:
            self._release_exchange_lease("resume")

    def manual_reconcile(self) -> dict[str, Any]:
        self.safe_mode.refresh_from_store()
        expected_safe_mode_revision = self.safe_mode.revision
        if self.execution_adapter is not None:
            if not self._acquire_exchange_lease("manual_reconcile"):
                return self._safe_mode_payload(cleared=False)
            try:
                try:
                    snapshot = self.execution_adapter.reconcile()
                except Exception as exc:
                    self.safe_mode.trip(
                        SafeModeReason.STALE_FOLLOWER,
                        f"manual follower reconcile failed: {exc}",
                    )
                    return self._safe_mode_payload(cleared=False)
                self.store.append_reconcile_snapshot(snapshot)
                cleared = self._clear_after_exchange_reconcile(
                    snapshot,
                    "manual reconcile completed",
                    clearance_auth_probe=False,
                    expected_safe_mode_revision=expected_safe_mode_revision,
                )
                payload = to_jsonable(snapshot)
                payload["safe_mode"] = self._safe_mode_payload(cleared=cleared)
                return payload
            finally:
                self._release_exchange_lease("manual_reconcile")
        _, clearance = self._clearance_gate()
        if clearance is not None:
            return clearance
        local_snapshot = {
            "paper_positions": self.paper.positions,
            "shadow": self.config.mode == Mode.SHADOW,
            "observed_ms": now_ms(),
        }
        self.safe_mode.clear_if_revision(
            expected_safe_mode_revision,
            "manual local reconcile completed",
        )
        return to_jsonable(local_snapshot)

    def refresh_readiness_truth(self) -> dict[str, Any]:
        """Refresh source and follower journals without planning or placing an order."""
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            raise RuntimeError("readiness truth refresh is only required in exchange modes")
        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")
        preflight = self.preflight(auth_probe=False)
        if not preflight.passed:
            return {
                "passed": False,
                "preflight": to_jsonable(preflight),
                "source": None,
                "follower": None,
                "readiness": self.readiness(),
            }
        if not self._acquire_exchange_lease("refresh_readiness_truth"):
            return {
                "passed": False,
                "preflight": to_jsonable(preflight),
                "source": None,
                "follower": None,
                "readiness": self.readiness(),
            }
        payload: dict[str, Any]
        try:
            try:
                mids = self.load_execution_mids()
                if self.config.mode == Mode.LIVE:
                    # Mainnet source reconciliation can be the slower half of this pair. Capture
                    # follower truth last so admission and the containment watchdog agree on the
                    # account state immediately before any signed canary path.
                    source = self.observer.reconcile_once()
                    follower = self.execution_adapter.reconcile()
                else:
                    # Shared execution-side pacing can queue testnet account probes for several
                    # seconds. Capture the mainnet source last so it cannot expire behind that
                    # separate testnet follower budget.
                    follower = self.execution_adapter.reconcile()
                    source = self.observer.reconcile_once()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.REST_LAG,
                    f"readiness truth refresh failed: {exc}",
                )
                payload = {
                    "passed": False,
                    "preflight": to_jsonable(preflight),
                    "source": None,
                    "follower": None,
                    "error": str(exc),
                }
            else:
                self.store.append_reconcile_snapshot(follower)
                source_fresh = self._check_source_freshness(source.observed_ms)
                follower_fresh = self._check_follower_freshness(follower.observed_ms)
                matched = False
                if source_fresh and follower_fresh:
                    try:
                        matched = self._check_manual_intervention(
                            follower.positions,
                            follower.open_orders,
                            position_mid_prices=mids,
                        )
                    except JournalIntegrityError as exc:
                        self.safe_mode.trip(
                            SafeModeReason.STARTUP_RECONCILE,
                            f"journal baseline rebuild failed: {exc}",
                        )
                payload = {
                    "passed": source_fresh and follower_fresh and matched,
                    "preflight": to_jsonable(preflight),
                    "source": {
                        "observed_ms": source.observed_ms,
                        "positions": sorted(source.positions),
                    },
                    "follower": {
                        "observed_ms": follower.observed_ms,
                        "positions": sorted(follower.positions),
                        "open_orders": len(follower.open_orders),
                        "account_mode": follower.payload.get("account_mode", "unknown"),
                        "account_value": follower.payload.get("account_value"),
                        "collateral_source": (
                            follower.payload.get("account_context", {}).get("collateral_source")
                            if isinstance(follower.payload.get("account_context"), dict)
                            else "unknown"
                        ),
                    },
                }
        finally:
            self._release_exchange_lease("refresh_readiness_truth")
        payload["readiness"] = self.readiness()
        return payload

    def _clearance_gate(
        self, *, auth_probe: bool = True
    ) -> tuple[PreflightReport, dict[str, Any] | None]:
        report = self.preflight(auth_probe=auth_probe)
        if not report.passed:
            return report, {
                **self._safe_mode_payload(cleared=False),
                "preflight": to_jsonable(report),
            }
        kill_switch = ExecutionGuard(
            risk=self.config.risk,
            ops=self.config.ops,
            store=self.store,
            asset_meta={},
            mids={},
            mode=self.config.mode,
        ).kill_switch_path()
        if kill_switch.exists():
            self.safe_mode.trip(
                SafeModeReason.OPERATOR_KILL_SWITCH,
                f"kill switch file exists: {kill_switch}",
            )
            return report, {
                **self._safe_mode_payload(cleared=False),
                "preflight": to_jsonable(report),
            }
        return report, None

    def _clear_after_exchange_reconcile(
        self,
        snapshot,
        clear_detail: str,
        *,
        clearance_checked: bool = False,
        clearance_auth_probe: bool = True,
        expected_safe_mode_revision: int | None = None,
    ) -> bool:
        self.safe_mode.refresh_from_store()
        expected_revision = (
            self.safe_mode.revision
            if expected_safe_mode_revision is None
            else expected_safe_mode_revision
        )
        if not self._check_no_pending_for_clear():
            return False
        fresh = self._check_follower_freshness(snapshot.observed_ms)
        matched = False
        if fresh:
            clearance_reason = self.safe_mode.reason
            require_exact_positions = clearance_reason == SafeModeReason.PARTIAL_FILL
            if require_exact_positions:
                matched = (
                    not any(position.size != 0 for position in snapshot.positions.values())
                    and not snapshot.open_orders
                )
                if matched:
                    checkpoint_created_ms = now_ms()
                    checkpoint = DesiredState(
                        state_id=deterministic_cloid(
                            "manual-flat-checkpoint",
                            self.config.mode.value,
                            self._effective_action_account(),
                            checkpoint_created_ms,
                        ),
                        source_event_key="manual-partial-fill-reconcile",
                        mode=self.config.mode,
                        positions={},
                        reason="operator reconciled partial-fill incident to exact flat truth",
                        created_ms=checkpoint_created_ms,
                        source_wallet=self.config.source_wallet.lower(),
                        action_account=self._effective_action_account(),
                        source_network=self.config.resolved_source_network.value,
                    )
                    self.store.append_desired_state(checkpoint)
                    self.store.commit_desired_state(checkpoint.state_id)
            else:
                try:
                    position_mid_prices = self.load_execution_mids()
                    matched = self._check_manual_intervention(
                        snapshot.positions,
                        snapshot.open_orders,
                        allow_dust_tolerance=True,
                        position_mid_prices=position_mid_prices,
                    )
                except JournalIntegrityError as exc:
                    self.safe_mode.trip(
                        SafeModeReason.STARTUP_RECONCILE,
                        f"journal baseline rebuild failed: {exc}",
                    )
                except Exception as exc:
                    self.safe_mode.trip(
                        SafeModeReason.REST_LAG,
                        f"manual reconcile market data load failed: {exc}",
                    )
            if not matched and require_exact_positions:
                self.safe_mode.trip(
                    SafeModeReason.PARTIAL_FILL,
                    "partial-fill clearance requires exact follower positions; "
                    "a residual position is still present",
                )
        if fresh and matched:
            # A terminally rejected/cleaned execution plan may be newer than the last committed
            # target even though fresh follower truth matches that committed baseline. Finalize
            # the actual checkpoint before preflight, otherwise the unresolved-plan gate makes
            # safe-mode clearance circular and impossible.
            self._finalize_execution_truth(
                snapshot,
                trigger="manual reconcile clearance",
                keep_incident_safe=False,
            )
            if not clearance_checked:
                _, clearance = self._clearance_gate(auth_probe=clearance_auth_probe)
                if clearance is not None:
                    return False
            return self.safe_mode.clear_if_revision(expected_revision, clear_detail) is not None
        return False

    def _check_no_pending_for_clear(self) -> bool:
        pending_count = self.store.pending_intent_count(self.config.mode)
        signed_action_count = self.store.unresolved_signed_action_attempt_count(
            self.config.mode,
            account=self._effective_action_account(),
            network=("mainnet" if self.config.mode == Mode.LIVE else "testnet"),
        )
        if pending_count == 0 and signed_action_count == 0:
            return True
        if pending_count and signed_action_count:
            detail = (
                f"{pending_count} unresolved pending intents must settle and "
                f"{signed_action_count} unresolved non-order signed actions must be reviewed "
                "before safe mode can clear"
            )
        elif pending_count:
            detail = (
                f"{pending_count} unresolved pending intents must settle before safe mode can clear"
            )
        else:
            detail = (
                f"{signed_action_count} unresolved non-order signed actions must be explicitly "
                "reviewed before safe mode can clear"
            )
        self.safe_mode.trip(
            SafeModeReason.RESTART_MID_FILL,
            detail,
        )
        return False

    def _safe_mode_payload(self, *, cleared: bool) -> dict[str, Any]:
        return {
            "cleared": cleared,
            "safe_mode": self._safe_mode_status(),
        }

    def _safe_mode_status(self) -> dict[str, Any]:
        self.safe_mode.refresh_from_store()
        return {
            "enabled": self.safe_mode.enabled,
            "reason": self.safe_mode.reason.value,
            "detail": self.safe_mode.detail,
            "revision": self.safe_mode.revision,
            "incident": incident_guidance(self.safe_mode.reason, enabled=self.safe_mode.enabled),
        }

    def _blocked_cycle_payload(
        self,
        preflight: PreflightReport,
        reason: SafeModeReason,
        detail: str,
    ) -> dict[str, Any]:
        self.safe_mode.trip(reason, detail)
        return {
            "preflight": to_jsonable(preflight),
            "safe_mode": self._safe_mode_status(),
            "intents": [],
            "reports": [],
        }

    def settle_pending_intents(self, limit: int = 100) -> dict[str, Any]:
        pending = self.store.pending_intents(self.config.mode)[:limit]
        signed_action_pending = self.store.unresolved_signed_action_attempts(
            self.config.mode,
            account=self._effective_action_account(),
            network=("mainnet" if self.config.mode == Mode.LIVE else "testnet"),
        )
        settled: list[ExecutionReport] = []
        still_open: list[dict[str, Any]] = []
        ambiguous: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        unresolved_plans = (
            self.store.unresolved_desired_state_count(
                mode=self.config.mode,
                source_wallet=self.config.source_wallet,
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            if self.config.mode in {Mode.TESTNET, Mode.LIVE}
            else 0
        )

        if signed_action_pending:
            detail = (
                "non-order signed-action settlement requires explicit operator review; "
                "automatic retry is forbidden"
            )
            self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
            return {
                "pending_before": len(pending),
                "settled": [],
                "still_open": [],
                "ambiguous": [
                    {
                        "attempt_id": row["attempt_id"],
                        "action": row["action"],
                        "attempt_phase": row["attempt_phase"],
                        "detail": detail,
                    }
                    for row in signed_action_pending
                ],
                "errors": [],
                "pending_after": self.store.pending_intent_count(self.config.mode),
                "signed_action_attempts": to_jsonable(signed_action_pending),
                "requires_operator_review": True,
                "finalization": None,
            }

        if not pending and not unresolved_plans:
            return {
                "pending_before": 0,
                "settled": [],
                "still_open": [],
                "ambiguous": [],
                "errors": [],
                "pending_after": 0,
                "finalization": None,
            }

        if not self._acquire_exchange_lease("settle_pending"):
            return {
                "pending_before": len(pending),
                "settled": [],
                "still_open": [],
                "ambiguous": [{"detail": self.safe_mode.detail}],
                "errors": [],
                "pending_after": self.store.pending_intent_count(self.config.mode),
                "finalization": None,
            }

        try:
            if not self.safe_mode.enabled:
                self.safe_mode.trip(
                    SafeModeReason.RESTART_MID_FILL,
                    "durable execution recovery requires operator review",
                )
            status_lookup_failed = False
            for row in pending:
                cloid = row["cloid"]
                phase = str(row.get("attempt_phase") or "")
                if phase == ExecutionAttemptPhase.PREPARED.value:
                    report = ExecutionReport(
                        report_id=deterministic_cloid(
                            "never-dispatched-recovery", row["intent_id"], cloid
                        ),
                        intent_id=row["intent_id"],
                        cloid=cloid,
                        status=IntentStatus.SKIPPED,
                        exchange_status="recovered:never_dispatched",
                        exchange_ts_ms=now_ms(),
                        payload={
                            "durable_attempt_phase": phase,
                            "proof": "PREPARED was committed before mutation and never advanced",
                        },
                    )
                    self.store.append_execution_report(report)
                    settled.append(report)
                    continue
                if phase not in {
                    ExecutionAttemptPhase.DISPATCHING.value,
                    ExecutionAttemptPhase.UNKNOWN.value,
                    ExecutionAttemptPhase.LEGACY_UNRESOLVED.value,
                }:
                    ambiguous.append(
                        {
                            "intent_id": row["intent_id"],
                            "cloid": cloid,
                            "status": f"invalid durable attempt phase {phase!r}",
                        }
                    )
                    continue
                if self.execution_adapter is None:
                    errors.append(
                        {
                            "cloid": cloid,
                            "error": "exchange adapter is required for dispatched/legacy lookup",
                        }
                    )
                    status_lookup_failed = True
                    continue
                try:
                    payload = self.execution_adapter.order_status(cloid)
                except Exception as exc:  # pragma: no cover - defensive runtime path
                    errors.append({"cloid": cloid, "error": str(exc)})
                    status_lookup_failed = True
                    continue
                status, exchange_status = classify_order_status(payload)
                if status in {IntentStatus.FILLED, IntentStatus.CANCELED, IntentStatus.REJECTED}:
                    report_payload: dict[str, Any] = {"order_status": payload}
                    if status == IntentStatus.REJECTED:
                        zero_fill_payload = self._settled_hip3_ioc_zero_fill_payload(row, payload)
                        if zero_fill_payload is not None:
                            exchange_status = "hip3_ioc_no_fill"
                            report_payload = zero_fill_payload
                    report = ExecutionReport(
                        report_id=deterministic_cloid(
                            "settle-report", row["intent_id"], cloid, payload
                        ),
                        intent_id=row["intent_id"],
                        cloid=cloid,
                        status=status,
                        exchange_status="settled:" + exchange_status,
                        exchange_ts_ms=now_ms(),
                        payload=report_payload,
                    )
                    self.store.append_execution_report(report)
                    settled.append(report)
                    self._handle_settled_terminal_report(report, str(row["action"]))
                elif status == IntentStatus.ACKED:
                    still_open.append(
                        {"intent_id": row["intent_id"], "cloid": cloid, "status": exchange_status}
                    )
                else:
                    ambiguous.append(
                        {"intent_id": row["intent_id"], "cloid": cloid, "status": exchange_status}
                    )

            finalization: dict[str, Any] | None = None
            if self.execution_adapter is not None and (settled or unresolved_plans):
                try:
                    snapshot = self.execution_adapter.reconcile()
                except Exception as exc:
                    detail = f"follower reconcile after settlement failed: {exc}"
                    errors.append({"operation": "reconcile", "error": str(exc)})
                    self.safe_mode.trip(SafeModeReason.STALE_FOLLOWER, detail)
                else:
                    self.store.append_reconcile_snapshot(snapshot)
                    if self._check_follower_freshness(snapshot.observed_ms):
                        try:
                            settlement_mids = self.load_execution_mids()
                        except Exception as exc:
                            self.safe_mode.trip(
                                SafeModeReason.REST_LAG,
                                f"settlement market data load failed: {exc}",
                            )
                        else:
                            self._check_manual_intervention(
                                snapshot.positions,
                                snapshot.open_orders,
                                position_mid_prices=settlement_mids,
                            )
                    finalization = self._finalize_execution_truth(
                        snapshot,
                        trigger="restart settlement",
                        keep_incident_safe=True,
                    )
            if still_open or ambiguous or status_lookup_failed:
                detail = (
                    f"pending settlement incomplete: open={len(still_open)} "
                    f"ambiguous={len(ambiguous)} errors={len(errors)}"
                )
                if not self.safe_mode.enabled:
                    self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
            return {
                "pending_before": len(pending),
                "settled": to_jsonable(settled),
                "still_open": still_open,
                "ambiguous": ambiguous,
                "errors": errors,
                "pending_after": self.store.pending_intent_count(self.config.mode),
                "finalization": to_jsonable(finalization),
                "safe_mode": {
                    "enabled": self.safe_mode.enabled,
                    "reason": self.safe_mode.reason.value,
                    "detail": self.safe_mode.detail,
                },
            }
        finally:
            self._release_exchange_lease("settle_pending")

    def _handle_settled_terminal_report(self, report: ExecutionReport, intent_action: str) -> None:
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            return
        if report.status == IntentStatus.REJECTED:
            self._record_runtime_result(report)
            return
        if report.status == IntentStatus.CANCELED and intent_action != IntentAction.CANCEL.value:
            self.safe_mode.trip(
                SafeModeReason.RESTART_MID_FILL,
                f"{report.cloid} settled canceled; reconcile follower truth before new risk",
            )

    def _set_canary_leverage(self, coin: str, *, operation: str) -> ExecutionReport:
        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")
        leverage = 1
        report = self._timed_exchange_action(
            intent_id=f"leverage:{operation}:{coin}:{leverage}",
            cloid=deterministic_cloid("leverage", operation, coin, leverage, now_ms()),
            count_rate=True,
            record_runtime=False,
            signed_action_kind="update_leverage_cross",
            signed_action_payload={
                "coin": coin,
                "leverage": leverage,
                "is_cross": True,
                "operation": operation,
            },
            action=lambda: self.execution_adapter.update_leverage(
                coin,
                leverage,
                is_cross=True,
            ),
        )
        if self._cross_margin_not_allowed(report):
            report = replace(
                report,
                payload={
                    **dict(report.payload),
                    "expected_cross_margin_fallback": True,
                },
            )
            self.store.append_execution_report(report)
            self._refresh_active_dispatch_truth_for_fallback(coin)
            isolated_report = self._timed_exchange_action(
                intent_id=f"leverage:{operation}:{coin}:{leverage}:isolated",
                cloid=deterministic_cloid(
                    "leverage", operation, coin, leverage, "isolated", now_ms()
                ),
                count_rate=True,
                signed_action_kind="update_leverage_isolated",
                signed_action_payload={
                    "coin": coin,
                    "leverage": leverage,
                    "is_cross": False,
                    "operation": operation,
                },
                action=lambda: self.execution_adapter.update_leverage(
                    coin,
                    leverage,
                    is_cross=False,
                ),
            )
            self.store.append_execution_report(isolated_report)
            report = isolated_report
        else:
            self.store.append_execution_report(report)
            self._record_runtime_result(report)
        if report.status != IntentStatus.ACKED and not self.safe_mode.enabled:
            self.safe_mode.trip(
                SafeModeReason.MARGIN_ERROR,
                f"{operation} could not enforce {coin} leverage {leverage}: "
                f"{report.exchange_status}",
            )
        return report

    def containment_watchdog_once(
        self,
        *,
        authority: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Contain orphaned orders and exposure after a supervisor failure.

        The normal watchdog remains idle unless a bot-owned order reaches its
        cancellation deadline.  A configured validation supervisor lease is a
        stronger boundary: once it is missing, stale, malformed, in
        containment, or past the immutable run deadline, this independent
        process cancels every observable bot-owned order and makes bounded
        reduce-only attempts to flatten each follower position.
        """

        if self.config.mode not in {Mode.TESTNET, Mode.LIVE}:
            raise RuntimeError("containment watchdog only runs in exchange modes")
        if self.config.ops.dead_man_policy != DeadManPolicy.WATCHDOG_FALLBACK:
            raise RuntimeError("containment watchdog requires dead_man_policy=watchdog_fallback")
        if self.execution_adapter is None:
            raise RuntimeError("containment watchdog requires an exchange adapter")

        observed = now_ms()
        authority = authority or self.containment_watchdog_authority()
        if bool(authority.get("configured")) and not bool(authority.get("authoritative")):
            # Do not even read or write the follower journal before this return.  A stale
            # generation must have zero safe-mode, heartbeat, reconcile, or exchange effects.
            return self._hard_quiesced_watchdog_result(
                observed_ms=observed,
                authority=authority,
            )

        pending = self.store.pending_intents(self.config.mode)
        kill_switch_active = self._kill_switch_path().exists()
        validation_guard_configured = self.config.ops.validation_supervisor_lease_path is not None
        controller_registry_renewed = bool(authority.get("controller_registry_renewed", True))
        supervisor = (
            self._validation_supervisor_decision()
            if controller_registry_renewed
            else self._blocked_validation_supervisor(
                "guardian could not renew controller ownership"
            )
        )
        validation_containment = validation_guard_configured and not supervisor.ok
        if validation_containment:
            self.safe_mode.refresh_from_store()
            preserve_supervisor_containment = bool(
                self.safe_mode.enabled
                and self.safe_mode.reason == SafeModeReason.RESTART_MID_FILL
                and self.safe_mode.detail.startswith(
                    VALIDATION_SUPERVISOR_CONTAINMENT_DETAIL_PREFIX
                )
                and supervisor.reason == SafeModeReason.LIVE_BLOCKED
                and supervisor.detail == VALIDATION_SUPERVISOR_LEASE_CONTAINMENT_BLOCK
            )
            if not preserve_supervisor_containment and (
                not self.safe_mode.enabled
                or self.safe_mode.reason != supervisor.reason
                or self.safe_mode.detail != supervisor.detail
            ):
                self.safe_mode.trip(supervisor.reason, supervisor.detail)
        watched: list[dict[str, Any]] = []
        cancellations: list[ExecutionReport] = []
        settled: list[ExecutionReport] = []
        errors: list[dict[str, Any]] = []
        validation_cleanup: list[dict[str, Any]] = []
        unowned_orders: list[dict[str, Any]] = []
        final_snapshot: ReconcileSnapshot | None = None

        for row in pending:
            intent_id = str(row.get("intent_id") or "")
            cloid = str(row.get("cloid") or "")
            coin = canonical_market_symbol(str(row.get("coin") or ""))
            phase = str(row.get("attempt_phase") or "")
            created_ms = int(row.get("created_ms") or observed)
            deadline_ms = created_ms + self.config.ops.dead_man_cancel_ms
            if not intent_id or not cloid or not coin:
                errors.append({"intent_id": intent_id, "error": "pending intent is malformed"})
                continue
            if phase == ExecutionAttemptPhase.PREPARED.value:
                watched.append(
                    {
                        "intent_id": intent_id,
                        "cloid": cloid,
                        "status": "prepared_not_dispatched",
                        "deadline_ms": deadline_ms,
                    }
                )
                continue
            due = observed >= deadline_ms
            should_cancel = kill_switch_active or due or validation_containment
            if not should_cancel:
                watched.append(
                    {
                        "intent_id": intent_id,
                        "cloid": cloid,
                        "status": "dispatching_within_containment_deadline",
                        "deadline_ms": deadline_ms,
                        "due": False,
                        "kill_switch_active": False,
                    }
                )
                continue
            try:
                status_payload = self.execution_adapter.order_status(cloid)
            except Exception as exc:
                errors.append({"intent_id": intent_id, "cloid": cloid, "error": str(exc)})
                continue
            status, exchange_status = classify_order_status(status_payload)
            if status in {IntentStatus.FILLED, IntentStatus.CANCELED, IntentStatus.REJECTED}:
                report_payload: dict[str, Any] = {
                    "order_status": status_payload,
                    "watchdog": True,
                }
                if status == IntentStatus.REJECTED:
                    zero_fill_payload = self._settled_hip3_ioc_zero_fill_payload(
                        row,
                        status_payload,
                    )
                    if zero_fill_payload is not None:
                        exchange_status = "hip3_ioc_no_fill"
                        report_payload = {**zero_fill_payload, "watchdog": True}
                report = ExecutionReport(
                    report_id=deterministic_cloid(
                        "watchdog-settle", intent_id, cloid, status_payload
                    ),
                    intent_id=intent_id,
                    cloid=cloid,
                    status=status,
                    exchange_status="watchdog_settled:" + exchange_status,
                    exchange_ts_ms=observed,
                    payload=report_payload,
                )
                self.store.append_execution_report(report)
                settled.append(report)
                continue

            watched.append(
                {
                    "intent_id": intent_id,
                    "cloid": cloid,
                    "status": exchange_status,
                    "deadline_ms": deadline_ms,
                    "due": due,
                    "kill_switch_active": kill_switch_active,
                }
            )
            if not should_cancel:
                continue
            try:
                pre_cancel_snapshot = self.execution_adapter.reconcile()
                self.store.append_reconcile_snapshot(pre_cancel_snapshot)
                self._active_plan_follower_observed_ms = pre_cancel_snapshot.observed_ms
            except Exception as exc:
                errors.append(
                    {
                        "intent_id": intent_id,
                        "cloid": cloid,
                        "error": f"pre-cancel reconcile failed: {exc}",
                    }
                )
                continue
            self._watchdog_containment_active = True
            try:
                cancel_report = self._timed_exchange_action(
                    intent_id="watchdog-cancel:" + cloid,
                    cloid=cloid,
                    count_rate=False,
                    risk_reducing=True,
                    action=lambda coin=coin, cloid=cloid: self.execution_adapter.cancel_by_cloid(
                        coin, cloid
                    ),
                )
            finally:
                self._watchdog_containment_active = False
            self.store.append_execution_report(cancel_report)
            cancellations.append(cancel_report)

        if cancellations:
            final_snapshot = self.execution_adapter.reconcile()
            self.store.append_reconcile_snapshot(final_snapshot)
            self._active_plan_follower_observed_ms = final_snapshot.observed_ms

        if validation_containment:
            self._watchdog_containment_active = True
            try:
                try:
                    final_snapshot = self.execution_adapter.reconcile()
                    self.store.append_reconcile_snapshot(final_snapshot)
                    self._active_plan_follower_observed_ms = final_snapshot.observed_ms
                except Exception as exc:
                    errors.append({"operation": "validation_guardian_reconcile", "error": str(exc)})
                    final_snapshot = None

                # Pending rows are not the complete ownership record after an
                # acknowledgement race.  Match actual orders only against the
                # durable follower-intent journal; orders without an exact
                # journaled cloid are reported and never canceled.
                owned_cloids = {
                    str(row.get("cloid") or "").lower()
                    for row in self.store.recent("follower_intents", limit=10_000)
                    if row.get("cloid")
                }
                if final_snapshot is not None:
                    already_canceled = {report.cloid.lower() for report in cancellations}
                    for order in final_snapshot.open_orders:
                        cloid = str(order.cloid or "").lower()
                        if not cloid or cloid not in owned_cloids:
                            unowned_orders.append(
                                {
                                    "coin": order.coin,
                                    "cloid": cloid or None,
                                    "reason": "not bot-owned",
                                }
                            )
                            continue
                        if cloid in already_canceled:
                            continue
                        try:
                            report = self._timed_exchange_action(
                                intent_id="validation-guardian-cancel:" + cloid,
                                cloid=cloid,
                                count_rate=False,
                                risk_reducing=True,
                                action=lambda order=order, cloid=cloid: (
                                    self.execution_adapter.cancel_by_cloid(order.coin, cloid)
                                ),
                            )
                        except Exception as exc:
                            errors.append(
                                {
                                    "operation": "validation_guardian_cancel",
                                    "coin": order.coin,
                                    "cloid": cloid,
                                    "error": str(exc),
                                }
                            )
                            continue
                        self.store.append_execution_report(report)
                        cancellations.append(report)

                    try:
                        final_snapshot = self.execution_adapter.reconcile()
                        self.store.append_reconcile_snapshot(final_snapshot)
                        self._active_plan_follower_observed_ms = final_snapshot.observed_ms
                    except Exception as exc:
                        errors.append(
                            {
                                "operation": "validation_guardian_post_cancel_reconcile",
                                "error": str(exc),
                            }
                        )
                        final_snapshot = None

                if final_snapshot is not None and final_snapshot.positions:
                    try:
                        asset_meta = self.load_asset_meta()
                        mids = self.load_execution_mids()
                    except Exception as exc:
                        errors.append(
                            {
                                "operation": "validation_guardian_market_data",
                                "error": str(exc),
                            }
                        )
                    else:
                        frozen_symbols = {
                            canonical_market_symbol(symbol)
                            for symbol in self.config.risk.allowed_symbols
                        }
                        for raw_coin, position in sorted(final_snapshot.positions.items()):
                            coin = canonical_market_symbol(raw_coin)
                            if position.size == 0:
                                continue
                            if coin not in frozen_symbols:
                                errors.append(
                                    {
                                        "operation": "validation_guardian_flatten",
                                        "coin": coin,
                                        "error": (
                                            "position is outside the frozen validation scope; "
                                            "guardian will not touch potentially user-owned exposure"
                                        ),
                                    }
                                )
                                continue
                            if coin not in asset_meta or coin not in mids:
                                errors.append(
                                    {
                                        "operation": "validation_guardian_flatten",
                                        "coin": coin,
                                        "error": "market metadata or mid is unavailable",
                                    }
                                )
                                continue
                            baseline_positions = {
                                other_coin: other_position
                                for other_coin, other_position in final_snapshot.positions.items()
                                if canonical_market_symbol(other_coin) != coin
                            }
                            cleanup = self._run_passive_canary_cleanup(
                                coin=coin,
                                entry_cloid=deterministic_cloid(
                                    "validation-guardian-boundary",
                                    self._effective_action_account(),
                                    coin,
                                ),
                                cancel_entry=False,
                                max_cleanup_size=abs(position.size),
                                baseline_positions=baseline_positions,
                                asset_meta=asset_meta[coin],
                                fallback_mid=mids[coin],
                                operation="validation-supervisor-containment",
                            )
                            validation_cleanup.append(to_jsonable({"coin": coin, **cleanup}))
                            try:
                                final_snapshot = self.execution_adapter.reconcile()
                                self.store.append_reconcile_snapshot(final_snapshot)
                                self._active_plan_follower_observed_ms = final_snapshot.observed_ms
                            except Exception as exc:
                                errors.append(
                                    {
                                        "operation": "validation_guardian_final_reconcile",
                                        "coin": coin,
                                        "error": str(exc),
                                    }
                                )
                                final_snapshot = None
                                break
            finally:
                self._watchdog_containment_active = False
        terminal_flat = bool(
            validation_containment
            and final_snapshot is not None
            and not final_snapshot.open_orders
            and all(position.size == 0 for position in final_snapshot.positions.values())
        )
        return to_jsonable(
            {
                "observed_ms": observed,
                "pending_before": len(pending),
                "watched": watched,
                "cancellations": cancellations,
                "settled": settled,
                "errors": errors,
                "validation_supervisor": {
                    "configured": validation_guard_configured,
                    "allows_new_risk": supervisor.ok,
                    "detail": supervisor.detail,
                    "containment_active": validation_containment,
                    "controller_registry_renewed": controller_registry_renewed,
                    "hard_quiesced": False,
                },
                "validation_market_universe": self.validation_market_universe_status(),
                "validation_cleanup": validation_cleanup,
                "unowned_orders": unowned_orders,
                "positions": final_snapshot.positions if final_snapshot is not None else None,
                "open_orders": final_snapshot.open_orders if final_snapshot is not None else None,
                "kill_switch_active": kill_switch_active,
                "hard_quiesced": False,
                "terminate": terminal_flat,
            }
        )

    def mainnet_canary_readiness(
        self,
        coin: str = "BTC",
        *,
        allow_completed_passive: bool = False,
    ) -> dict[str, Any]:
        """Run public/read-only checks for the isolated first-mainnet canary."""

        profile = build_mainnet_canary_profile(self.config, coin=coin)
        preflight = self.preflight(auth_probe=False)
        refresh: dict[str, Any] | None = None
        if profile["passed"] and preflight.passed:
            refresh = self.refresh_readiness_truth()
        blockers = [*profile["blockers"], *preflight.blockers]
        passive_proof = self._mainnet_passive_canary_proof()
        if passive_proof["passed"] and not allow_completed_passive:
            blockers.append(
                "passive mainnet canary already passed for this journal; replay is refused"
            )
        dead_man_eligibility: dict[str, Any] | None = None
        watchdog_protection: dict[str, Any] | None = None
        if profile["passed"] and preflight.passed:
            provider = getattr(self.execution_adapter, "dead_man_eligibility", None)
            if not callable(provider):
                blockers.append(
                    "mainnet canary adapter cannot prove exchange dead-man volume eligibility"
                )
            else:
                try:
                    dead_man_eligibility = to_jsonable(provider())
                except Exception as exc:
                    blockers.append(f"mainnet canary dead-man eligibility query failed: {exc}")
                else:
                    if not dead_man_eligibility.get("eligible"):
                        if self.config.ops.dead_man_policy == DeadManPolicy.WATCHDOG_FALLBACK:
                            watchdog_protection = self.containment_watchdog_status()
                            if not watchdog_protection.get("ready"):
                                blockers.append(
                                    "exchange dead-man is volume-gated and the independent "
                                    "containment watchdog is not ready"
                                )
                        else:
                            blockers.append(
                                "exchange dead-man requires $1,000,000 cumulative volume on the "
                                "action account; current volume is "
                                f"${dead_man_eligibility.get('cumulative_volume_usd', 'unavailable')}"
                            )
        unresolved_signed_actions = self.store.unresolved_signed_action_attempts(
            Mode.LIVE,
            account=self._effective_action_account(),
            network="mainnet",
        )
        if unresolved_signed_actions:
            blockers.append(
                f"{len(unresolved_signed_actions)} unresolved non-order signed actions require "
                "explicit operator review"
            )
        if self._kill_switch_path().exists():
            blockers.append(f"kill switch file exists: {self._kill_switch_path()}")
        self.safe_mode.refresh_from_store()
        if self.safe_mode.enabled:
            blockers.append(
                f"safe mode is active: {self.safe_mode.reason.value}: {self.safe_mode.detail}"
            )
        if refresh is not None and not refresh.get("passed"):
            blockers.append(
                str(refresh.get("error") or "read-only source/follower truth refresh did not pass")
            )
        follower = refresh.get("follower") if isinstance(refresh, dict) else None
        if isinstance(follower, dict):
            if follower.get("positions"):
                blockers.append("mainnet canary follower must be flat")
            if int(follower.get("open_orders") or 0) != 0:
                blockers.append("mainnet canary follower must have no open orders")
            try:
                account_value = parse_decimal(follower.get("account_value"))
            except (ArithmeticError, TypeError, ValueError):
                blockers.append("mainnet canary follower account value is unavailable")
            else:
                if not (
                    MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD
                    <= account_value
                    <= MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD
                ):
                    blockers.append(
                        "mainnet canary follower account value must be between "
                        f"${MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD} and "
                        f"${MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD}"
                    )
        return to_jsonable(
            {
                "scope": "single_account_passive_mainnet_canary",
                "candidate": not blockers,
                "signed_actions_performed": False,
                "profile": profile,
                "preflight": preflight,
                "truth_refresh": refresh,
                "dead_man_eligibility": dead_man_eligibility,
                "watchdog_protection": watchdog_protection,
                "unresolved_signed_actions": unresolved_signed_actions,
                "passive_canary_proof": passive_proof,
                "blockers": blockers,
                "next_command": None,
            }
        )

    def mainnet_passive_canary(
        self,
        coin: str = "BTC",
        size: Decimal = Decimal("0.0001"),
        *,
        acknowledgement: str,
        expected_account: str,
    ) -> dict[str, Any]:
        """Place and cancel exactly one bounded passive mainnet order after explicit consent."""

        profile = build_mainnet_canary_profile(self.config, coin=coin)
        if acknowledgement != MAINNET_CANARY_ACKNOWLEDGEMENT:
            raise ValueError(
                "mainnet canary acknowledgement must exactly match "
                f"{MAINNET_CANARY_ACKNOWLEDGEMENT}"
            )
        if expected_account.strip().lower() != self._effective_action_account().lower():
            raise ValueError(
                "mainnet canary --account must exactly match the configured action account"
            )
        if not profile["passed"]:
            return to_jsonable(
                {
                    "scope": "single_account_passive_mainnet_canary",
                    "passed": False,
                    "profile": profile,
                    "signed_actions_performed": False,
                    "place": None,
                    "cancel": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        passive_proof = self._mainnet_passive_canary_proof()
        if passive_proof["passed"]:
            return to_jsonable(
                {
                    "scope": "single_account_passive_mainnet_canary",
                    "passed": False,
                    "profile": profile,
                    "signed_actions_performed": False,
                    "place": None,
                    "cancel": None,
                    "passive_canary_proof": passive_proof,
                    "blockers": [
                        "passive mainnet canary already passed for this journal; replay is refused"
                    ],
                    **self._safe_mode_payload(cleared=False),
                }
            )
        result = self._passive_exchange_canary(
            coin=coin,
            size=size,
            required_mode=Mode.LIVE,
            operation_slug="mainnet-passive-canary",
            operation_label="mainnet passive canary",
        )
        result["scope"] = "single_account_passive_mainnet_canary"
        result["profile"] = profile
        result.setdefault("passed", False)
        return result

    def testnet_smoke(self, coin: str = "BTC", size: Decimal = Decimal("0.0001")) -> dict[str, Any]:
        return self._passive_exchange_canary(
            coin=coin,
            size=size,
            required_mode=Mode.TESTNET,
            operation_slug="testnet-smoke",
            operation_label="testnet smoke",
        )

    def _passive_exchange_canary(
        self,
        *,
        coin: str,
        size: Decimal,
        required_mode: Mode,
        operation_slug: str,
        operation_label: str,
    ) -> dict[str, Any]:
        if self.config.mode != required_mode:
            raise RuntimeError(f"{operation_label} only runs in {required_mode.value} mode")
        if not size.is_finite() or size <= 0:
            raise ValueError(f"{operation_label} size must be finite and positive")
        pending = self.store.pending_intents(required_mode)
        if pending:
            detail = f"{operation_label} blocked by {len(pending)} unresolved prior intents"
            self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
            return to_jsonable(
                {
                    "preflight": self.preflight(auth_probe=False),
                    "dead_man": None,
                    "dead_man_clear": None,
                    "place": None,
                    "cancel": None,
                    "reconcile": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        network = "mainnet" if required_mode == Mode.LIVE else "testnet"
        unresolved_signed_actions = self.store.unresolved_signed_action_attempts(
            required_mode,
            account=self._effective_action_account(),
            network=network,
        )
        if unresolved_signed_actions:
            detail = (
                f"{operation_label} blocked by {len(unresolved_signed_actions)} unresolved "
                "non-order signed actions; explicit operator review is required"
            )
            self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
            return to_jsonable(
                {
                    "preflight": self.preflight(auth_probe=False),
                    "unresolved_signed_actions": unresolved_signed_actions,
                    "dead_man": None,
                    "dead_man_clear": None,
                    "place": None,
                    "cancel": None,
                    "reconcile": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        report = self.preflight()
        if not report.passed:
            return to_jsonable(
                {
                    "preflight": report,
                    "dead_man": None,
                    "dead_man_clear": None,
                    "place": None,
                    "cancel": None,
                    "reconcile": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")
        if not self._acquire_exchange_lease(operation_slug):
            raise RuntimeError(self.safe_mode.detail)
        try:
            try:
                mids = self.load_execution_mids()
                meta = self.load_asset_meta()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.REST_LAG,
                    f"{operation_label} market data load failed: {exc}",
                )
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "reconcile": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            try:
                before_reconcile = self.execution_adapter.reconcile()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"{operation_label} initial follower reconcile failed: {exc}",
                )
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "reconcile": {"error": str(exc)},
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            self.store.append_reconcile_snapshot(before_reconcile)
            if not self._check_follower_freshness(before_reconcile.observed_ms):
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            source_reconcile = None
            if required_mode == Mode.LIVE:
                try:
                    source_reconcile = self.observer.reconcile_once()
                except Exception as exc:
                    self.safe_mode.trip(
                        SafeModeReason.REST_LAG,
                        f"{operation_label} source reconcile failed: {exc}",
                    )
                    return to_jsonable(
                        {
                            "preflight": report,
                            "source_reconcile": {"error": str(exc)},
                            "dead_man": None,
                            "dead_man_clear": None,
                            "place": None,
                            "cancel": None,
                            "reconcile": before_reconcile,
                            **self._safe_mode_payload(cleared=False),
                        }
                    )
                if not self._check_source_freshness(source_reconcile.observed_ms):
                    return to_jsonable(
                        {
                            "preflight": report,
                            "source_reconcile": source_reconcile,
                            "dead_man": None,
                            "dead_man_clear": None,
                            "place": None,
                            "cancel": None,
                            "reconcile": before_reconcile,
                            **self._safe_mode_payload(cleared=False),
                        }
                    )
            coin = canonical_market_symbol(coin)
            if coin not in {
                canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols
            }:
                self.safe_mode.trip(
                    SafeModeReason.UNSUPPORTED_SYMBOL,
                    f"{coin} is not in HLCT_ALLOWED_SYMBOLS",
                )
                raise RuntimeError(f"{coin} is not in the configured allowlist")
            if before_reconcile.open_orders:
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation_label} requires no existing follower open orders",
                )
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            if required_mode == Mode.LIVE and any(
                position.size != 0 for position in before_reconcile.positions.values()
            ):
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation_label} requires the entire follower account to be flat",
                )
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "cleanup": [],
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            existing_coin_position = before_reconcile.positions.get(coin)
            if existing_coin_position is not None and existing_coin_position.size != 0:
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation_label} requires {coin} to be flat before placing a passive order",
                )
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": None,
                        "cancel": None,
                        "cleanup": [],
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            self._active_plan_source_observed_ms = (
                source_reconcile.observed_ms if source_reconcile is not None else now_ms()
            )
            self._active_plan_follower_observed_ms = before_reconcile.observed_ms
            if coin not in mids or coin not in meta:
                raise RuntimeError(f"{coin} is missing from {required_mode.value} mids or metadata")
            passive_factor = Decimal("0.9") if required_mode == Mode.LIVE else Decimal("0.5")
            target_limit_notional = Decimal("12") if required_mode == Mode.LIVE else Decimal("11")
            passive_price = quantize_price(
                mids[coin] * passive_factor,
                meta[coin].sz_decimals,
            )
            smoke_size = quantize_size(
                max(size, target_limit_notional / passive_price), meta[coin].sz_decimals
            )
            if smoke_size * passive_price < target_limit_notional:
                step = Decimal(1).scaleb(-meta[coin].sz_decimals)
                smoke_size += step
            risk_notional = smoke_size * mids[coin]
            signed_order_notional = smoke_size * passive_price
            if signed_order_notional > self.config.risk.max_notional_usd:
                raise RuntimeError(
                    f"{operation_label} signed order value would exceed max notional cap"
                )
            if risk_notional > self.config.risk.max_notional_usd:
                raise RuntimeError(f"{operation_label} size would exceed max notional cap")
            if risk_notional > self.config.risk.max_gross_exposure_usd:
                raise RuntimeError(f"{operation_label} size would exceed max gross exposure cap")
            account_value = self._reconcile_account_value(before_reconcile)
            if account_value is None or account_value <= 0:
                raise RuntimeError(f"{operation_label} requires a positive follower account value")
            if required_mode == Mode.LIVE and not (
                MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD
                <= account_value
                <= MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD
            ):
                raise RuntimeError(
                    f"{operation_label} follower account value must remain between "
                    f"${MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD} and "
                    f"${MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD}"
                )
            if risk_notional > account_value * self.config.risk.max_leverage:
                raise RuntimeError(f"{operation_label} size would exceed effective leverage cap")
            cloid = deterministic_cloid(operation_slug, coin, size, now_ms())
            smoke_plan = DesiredState(
                state_id=deterministic_cloid(f"{operation_slug}-plan", cloid),
                source_event_key=operation_slug,
                mode=required_mode,
                positions={
                    symbol: position
                    for symbol, position in before_reconcile.positions.items()
                    if position.size != 0
                },
                reason="passive smoke must finish at its fresh pre-test follower truth",
                created_ms=now_ms(),
                source_wallet=self.config.source_wallet.lower(),
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            intent = FollowerIntent(
                intent_id=deterministic_cloid(f"{operation_slug}-intent", cloid),
                cloid=cloid,
                action=IntentAction.OPEN,
                coin=coin,
                side="buy",
                size=smoke_size,
                price=passive_price,
                reduce_only=False,
                mode=required_mode,
                source_event_key=operation_slug,
                reason=f"operator {operation_label}",
                created_ms=now_ms(),
                desired_state_id=smoke_plan.state_id,
                status=IntentStatus.PENDING,
            )
            if not self.store.prepare_execution_plan(smoke_plan, [intent]):
                detail = f"{operation_label} plan identity already exists"
                self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                raise RuntimeError(detail)
            guard = ExecutionGuard(
                risk=self.config.risk,
                ops=self.config.ops,
                store=self.store,
                asset_meta=meta,
                mids=mids,
                mode=self.config.mode,
            )
            guard_decision = guard.check_cycle([intent])
            if guard_decision.ok:
                guard_decision = guard.check_intent(
                    intent,
                    projected_positions=dict(before_reconcile.positions),
                )
            if not guard_decision.ok:
                self.safe_mode.trip(guard_decision.reason, guard_decision.detail)
                blocked = self._blocked_report(
                    intent,
                    guard_decision.reason,
                    guard_decision.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "preflight": report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": blocked,
                        "cancel": None,
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            leverage_report = self._set_canary_leverage(coin, operation=operation_slug)
            if leverage_report.status != IntentStatus.ACKED:
                blocked = self._blocked_report(
                    intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "preflight": report,
                        "leverage": leverage_report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "place": blocked,
                        "cancel": None,
                        "reconcile": before_reconcile,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            dead_man = self._schedule_dead_man_cancel(
                scheduled_time_ms=now_ms() + self.config.ops.dead_man_cancel_ms,
                operation=operation_slug,
                count_rate=True,
            )
            if dead_man is not None:
                self.store.append_execution_report(dead_man)
            if self.safe_mode.enabled or self._dead_man_blocks_execution(dead_man):
                place = self._blocked_report(intent, self.safe_mode.reason, self.safe_mode.detail)
                self.store.append_execution_report(place)
                return to_jsonable(
                    {
                        "dead_man": dead_man,
                        "dead_man_clear": None,
                        "place": place,
                        "cancel": None,
                        "reconcile": None,
                        "safe_mode": {
                            "enabled": self.safe_mode.enabled,
                            "reason": self.safe_mode.reason.value,
                            "detail": self.safe_mode.detail,
                        },
                    }
                )
            place = self._timed_exchange_action(
                intent_id=intent.intent_id,
                cloid=cloid,
                count_rate=True,
                action=lambda: self.execution_adapter.place_limit_order(
                    coin=coin,
                    side="buy",
                    size=smoke_size,
                    price=passive_price,
                    cloid=cloid,
                    reduce_only=False,
                    tif="Alo",
                ),
            )
            self.store.append_execution_report(place)
            if place.status == IntentStatus.FILLED:
                self.safe_mode.trip(
                    SafeModeReason.PARTIAL_FILL,
                    f"passive post-only {operation_label} unexpectedly filled; bounded cleanup started",
                )
            cleanup = self._run_passive_canary_cleanup(
                coin=coin,
                entry_cloid=cloid,
                cancel_entry=place.status != IntentStatus.SKIPPED,
                max_cleanup_size=smoke_size,
                baseline_positions=dict(before_reconcile.positions),
                asset_meta=meta[coin],
                fallback_mid=mids[coin],
                operation=f"passive {operation_label}",
            )
            cancel_reports = cleanup["cancel_reports"]
            cleanup_reports = cleanup["cleanup_reports"]
            cancel = cancel_reports[0] if cancel_reports else None
            reconcile = cleanup["reconcile"]
            smoke_finalization: dict[str, Any] | None = None
            dead_man_clear = None
            if isinstance(reconcile, ReconcileSnapshot):
                if not self.safe_mode.enabled and self._check_follower_freshness(
                    reconcile.observed_ms
                ):
                    self._check_manual_intervention(
                        reconcile.positions,
                        reconcile.open_orders,
                        position_mid_prices=mids,
                    )
            if (
                cleanup["flat"]
                and not self.safe_mode.enabled
                and dead_man is not None
                and dead_man.status == IntentStatus.ACKED
            ):
                dead_man_clear = self._schedule_dead_man_cancel(
                    scheduled_time_ms=None,
                    operation=operation_slug,
                    count_rate=False,
                )
                if dead_man_clear is not None:
                    self.store.append_execution_report(dead_man_clear)
                if (
                    dead_man_clear is None
                    or dead_man_clear.status != IntentStatus.ACKED
                    or dead_man_clear.exchange_status
                    not in {"dead_man_cleared", "watchdog_containment_released"}
                ) and not self.safe_mode.enabled:
                    self.safe_mode.trip(
                        SafeModeReason.CANCEL_REJECT,
                        f"passive {operation_label} could not prove the dead-man schedule was cleared",
                    )
            if (
                isinstance(reconcile, ReconcileSnapshot)
                and cleanup["flat"]
                and not self.safe_mode.enabled
            ):
                smoke_finalization = self._finalize_execution_truth(
                    reconcile,
                    trigger=f"passive {operation_label}",
                    keep_incident_safe=False,
                )
            return to_jsonable(
                {
                    "passed": (
                        not self.safe_mode.enabled
                        and place.status == IntentStatus.ACKED
                        and cancel is not None
                        and cancel.status == IntentStatus.CANCELED
                        and not cleanup_reports
                        and cleanup["flat"]
                        and dead_man_clear is not None
                        and dead_man_clear.status == IntentStatus.ACKED
                        and dead_man_clear.exchange_status
                        in {"dead_man_cleared", "watchdog_containment_released"}
                        and smoke_finalization is not None
                    ),
                    "coin": coin,
                    "source_reconcile": source_reconcile,
                    "size": smoke_size,
                    "passive_price": passive_price,
                    "signed_order_notional": signed_order_notional,
                    "risk_notional": risk_notional,
                    "leverage": leverage_report,
                    "dead_man": dead_man,
                    "dead_man_clear": dead_man_clear,
                    "protection_mode": (
                        "independent_containment_watchdog"
                        if dead_man is not None
                        and dead_man.exchange_status == "watchdog_containment_armed"
                        else "exchange_schedule_cancel"
                    ),
                    "place": place,
                    "cancel": cancel,
                    "cancel_attempts": cancel_reports,
                    "cleanup": cleanup_reports,
                    "reconcile": reconcile,
                    "execution_finalization": smoke_finalization,
                    "safe_mode": {
                        "enabled": self.safe_mode.enabled,
                        "reason": self.safe_mode.reason.value,
                        "detail": self.safe_mode.detail,
                    },
                }
            )
        finally:
            self._reset_signed_action_context()
            self._release_exchange_lease(operation_slug)

    def testnet_active_smoke(
        self, coin: str = "BTC", size: Decimal = Decimal("0.0001")
    ) -> dict[str, Any]:
        return self._active_round_trip_canary(
            coin=coin,
            size=size,
            required_mode=Mode.TESTNET,
            operation_slug="testnet_active_smoke",
            operation_label="testnet active smoke",
        )

    def mainnet_active_canary_readiness(self, coin: str = "BTC") -> dict[str, Any]:
        """Read-only active-canary admission layered on the proven passive canary."""

        readiness = self.mainnet_canary_readiness(coin, allow_completed_passive=True)
        passive_proof = self._mainnet_passive_canary_proof()
        active_proof = self._mainnet_active_canary_proof()
        blockers = list(readiness.get("blockers") or [])
        blockers.extend(passive_proof["blockers"])
        if active_proof["passed"]:
            blockers.append(
                "active mainnet canary already passed for this journal; replay is refused"
            )
        return to_jsonable(
            {
                "scope": "single_account_active_mainnet_canary",
                "candidate": not blockers,
                "signed_actions_performed": False,
                "next_command": None,
                "passive_canary_proof": passive_proof,
                "active_canary_proof": active_proof,
                "base_readiness": readiness,
                "blockers": blockers,
                "acknowledgement": MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT,
                "maximum_signed_orders": {
                    "opening_ioc": 1,
                    "reduce_only_close_ioc": 3,
                },
            }
        )

    def mainnet_active_canary(
        self,
        coin: str = "BTC",
        size: Decimal = Decimal("0.0001"),
        *,
        acknowledgement: str,
        expected_account: str,
    ) -> dict[str, Any]:
        """Run one acknowledged bounded mainnet entry and reduce-only flatten lifecycle."""

        if acknowledgement != MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT:
            raise RuntimeError(
                "mainnet active canary requires exact acknowledgement: "
                f"{MAINNET_ACTIVE_CANARY_ACKNOWLEDGEMENT}"
            )
        if self.config.mode != Mode.LIVE:
            raise RuntimeError("mainnet active canary only runs in live mode")
        action_account = self._effective_action_account()
        if not action_account or expected_account.strip().lower() != action_account.lower():
            raise RuntimeError(
                "mainnet active canary --account must exactly match the configured follower account"
            )
        readiness = self.mainnet_active_canary_readiness(coin)
        if not readiness["candidate"]:
            return to_jsonable(
                {
                    "passed": False,
                    "scope": "single_account_active_mainnet_canary",
                    "readiness": readiness,
                    "entry": None,
                    "exit": None,
                }
            )
        result = self._active_round_trip_canary(
            coin=coin,
            size=size,
            required_mode=Mode.LIVE,
            operation_slug="mainnet_active_canary",
            operation_label="mainnet active canary",
        )
        return to_jsonable(
            {
                **result,
                "scope": "single_account_active_mainnet_canary",
                "readiness": readiness,
                "protection_mode": (
                    "independent_containment_watchdog"
                    if (result.get("dead_man") or {}).get("exchange_status")
                    == "watchdog_containment_armed"
                    else "exchange_schedule_cancel"
                ),
            }
        )

    def _mainnet_passive_canary_proof(self) -> dict[str, Any]:
        reports = self.store.recent("execution_reports", limit=500)
        cancels = {
            str(row.get("cloid") or "").lower(): row
            for row in reports
            if str(row.get("status") or "") == IntentStatus.CANCELED.value
        }
        places = [
            row
            for row in reports
            if str(row.get("intent_id") or "").startswith("limit:")
            and str(row.get("status") or "") == IntentStatus.ACKED.value
            and str(row.get("exchange_status") or "") == "resting"
            and str(row.get("cloid") or "").lower() in cancels
        ]
        blockers: list[str] = []
        if not places:
            blockers.append(
                "active mainnet canary requires a prior journaled resting/canceled passive canary"
            )
        latest = (
            max(places, key=lambda row: int(row.get("exchange_ts_ms") or 0)) if places else None
        )
        cancel = cancels.get(str(latest.get("cloid") or "").lower()) if latest is not None else None

        def proof_summary(row: dict[str, Any] | None) -> dict[str, Any] | None:
            if row is None:
                return None
            return {
                "report_id": row.get("report_id"),
                "intent_id": row.get("intent_id"),
                "cloid": row.get("cloid"),
                "status": row.get("status"),
                "exchange_status": row.get("exchange_status"),
                "created_ms": row.get("created_ms"),
            }

        return {
            "passed": not blockers,
            "place": proof_summary(latest),
            "cancel": proof_summary(cancel),
            "blockers": blockers,
        }

    def _mainnet_active_canary_proof(self) -> dict[str, Any]:
        intents = self.store.recent("follower_intents", limit=500)
        reports = self.store.recent("execution_reports", limit=500)
        filled_by_cloid = {
            str(row.get("cloid") or "").lower(): row
            for row in reports
            if str(row.get("status") or "") == IntentStatus.FILLED.value
            and str(row.get("exchange_status") or "") == "filled"
            and str(row.get("intent_id") or "").startswith("limit:")
        }
        entries = [
            row
            for row in intents
            if str(row.get("source_event_key") or "") == "mainnet_active_canary"
            and str(row.get("action") or "") == IntentAction.OPEN.value
            and str(row.get("cloid") or "").lower() in filled_by_cloid
        ]
        exits = [
            row
            for row in intents
            if str(row.get("source_event_key") or "") == "mainnet active canary"
            and str(row.get("action") or "") == IntentAction.REDUCE.value
            and str(row.get("cloid") or "").lower() in filled_by_cloid
        ]
        entry = max(entries, key=lambda row: int(row.get("created_ms") or 0)) if entries else None
        exit_intent = max(exits, key=lambda row: int(row.get("created_ms") or 0)) if exits else None
        passed = (
            entry is not None
            and exit_intent is not None
            and int(exit_intent.get("created_ms") or 0) >= int(entry.get("created_ms") or 0)
        )

        def proof_summary(intent: dict[str, Any] | None) -> dict[str, Any] | None:
            if intent is None:
                return None
            cloid = str(intent.get("cloid") or "").lower()
            report = filled_by_cloid.get(cloid) or {}
            return {
                "intent_id": intent.get("intent_id"),
                "cloid": intent.get("cloid"),
                "action": intent.get("action"),
                "created_ms": intent.get("created_ms"),
                "report_id": report.get("report_id"),
                "status": report.get("status"),
                "exchange_status": report.get("exchange_status"),
            }

        return {
            "passed": passed,
            "entry": proof_summary(entry),
            "exit": proof_summary(exit_intent),
            "blockers": ([] if passed else ["no completed active mainnet round trip is journaled"]),
        }

    def _active_round_trip_canary(
        self,
        *,
        coin: str,
        size: Decimal,
        required_mode: Mode,
        operation_slug: str,
        operation_label: str,
    ) -> dict[str, Any]:
        if self.config.mode != required_mode:
            raise RuntimeError(f"{operation_label} only runs in {required_mode.value} mode")
        if not size.is_finite() or size <= 0:
            raise ValueError(f"{operation_label} size must be finite and positive")
        pending = self.store.pending_intents(required_mode)
        if pending:
            detail = f"{operation_label} blocked by {len(pending)} unresolved prior intents"
            self.safe_mode.trip(SafeModeReason.RESTART_MID_FILL, detail)
            return to_jsonable(
                {
                    "passed": False,
                    "preflight": self.preflight(auth_probe=False),
                    "before_reconcile": None,
                    "dead_man": None,
                    "dead_man_clear": None,
                    "entry": None,
                    "exit": None,
                    "after_reconcile": None,
                    "balance_before": None,
                    "balance_after": None,
                    "balance_delta": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        report = self.preflight()
        if not report.passed:
            return to_jsonable(
                {
                    "passed": False,
                    "preflight": report,
                    "before_reconcile": None,
                    "dead_man": None,
                    "dead_man_clear": None,
                    "entry": None,
                    "exit": None,
                    "after_reconcile": None,
                    "balance_before": None,
                    "balance_after": None,
                    "balance_delta": None,
                    **self._safe_mode_payload(cleared=False),
                }
            )
        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")
        if not self._acquire_exchange_lease(operation_slug):
            raise RuntimeError(self.safe_mode.detail)
        try:
            try:
                before_reconcile = self.execution_adapter.reconcile()
                self.store.append_reconcile_snapshot(before_reconcile)
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"{operation_label} initial follower reconcile failed: {exc}",
                )
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": {"error": str(exc)},
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": None,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": None,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            balance_before = self._reconcile_account_value(before_reconcile)
            if not self._check_follower_freshness(before_reconcile.observed_ms):
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": None,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            if before_reconcile.positions or before_reconcile.open_orders:
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation_label} requires a flat follower with no open orders",
                )
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": None,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            try:
                mids = self.load_execution_mids()
                meta = self.load_asset_meta()
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.REST_LAG,
                    f"{operation_label} market data load failed: {exc}",
                )
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": None,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            coin = canonical_market_symbol(coin)
            if coin not in mids or coin not in meta:
                raise RuntimeError(f"{coin} is missing from execution mids or metadata")
            if coin not in {
                canonical_market_symbol(symbol) for symbol in self.config.risk.allowed_symbols
            }:
                self.safe_mode.trip(
                    SafeModeReason.UNSUPPORTED_SYMBOL,
                    f"{coin} is not in HLCT_ALLOWED_SYMBOLS",
                )
                raise RuntimeError(f"{coin} is not in the configured allowlist")
            self._active_plan_source_observed_ms = now_ms()
            self._active_plan_follower_observed_ms = before_reconcile.observed_ms
            entry_price = aggressive_ioc_price(
                mids[coin],
                is_buy=True,
                slippage_bps=self.config.risk.slippage_bps,
                sz_decimals=meta[coin].sz_decimals,
            )
            smoke_size = quantize_size(
                max(size, Decimal("11") / entry_price), meta[coin].sz_decimals
            )
            if smoke_size * entry_price < Decimal("10"):
                step = Decimal(1).scaleb(-meta[coin].sz_decimals)
                smoke_size += step
            round_trip_quote: RoundTripQuote | None = None
            if market_dex(coin):
                try:
                    round_trip_quote, market_blockers = self.load_hip3_round_trip_quote(
                        coin,
                        opening_side="buy",
                        requested_size=smoke_size,
                        asset_meta=meta[coin],
                    )
                except Exception as exc:
                    market_blockers = [f"{coin} HIP-3 round-trip admission failed: {exc}"]
                if round_trip_quote is None:
                    detail = "; ".join(market_blockers)
                    self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                    return to_jsonable(
                        {
                            "passed": False,
                            "preflight": report,
                            "coin": coin,
                            "size": smoke_size,
                            "market_admission": {"passed": False, "blockers": market_blockers},
                            "before_reconcile": before_reconcile,
                            "dead_man": None,
                            "dead_man_clear": None,
                            "entry": None,
                            "exit": None,
                            "after_reconcile": before_reconcile,
                            "balance_before": balance_before,
                            "balance_after": balance_before,
                            "balance_delta": Decimal("0"),
                            **self._safe_mode_payload(cleared=False),
                        }
                    )
                entry_price = round_trip_quote.entry_limit
            notional = smoke_size * entry_price
            if notional > self.config.risk.max_notional_usd:
                raise RuntimeError(f"{operation_label} size would exceed max notional cap")
            if notional > self.config.risk.max_gross_exposure_usd:
                raise RuntimeError(f"{operation_label} size would exceed max gross exposure cap")
            if balance_before is None or balance_before <= 0:
                raise RuntimeError(f"{operation_label} requires a positive follower account value")
            if required_mode == Mode.LIVE and not (
                MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD
                <= balance_before
                <= MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD
            ):
                raise RuntimeError(
                    f"{operation_label} follower account value must remain between "
                    f"${MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD} and "
                    f"${MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD}"
                )
            if notional > balance_before * self.config.risk.max_leverage:
                raise RuntimeError(f"{operation_label} size would exceed effective leverage cap")

            entry_cloid = deterministic_cloid(f"{operation_slug}-entry", coin, size, now_ms())
            entry_plan = DesiredState(
                state_id=deterministic_cloid(f"{operation_slug}-entry-plan", entry_cloid),
                source_event_key=operation_slug,
                mode=required_mode,
                positions={
                    coin: Position(
                        coin=coin,
                        size=smoke_size,
                        entry_px=entry_price,
                        leverage=1,
                        updated_ms=now_ms(),
                    )
                },
                reason=f"{operation_label} bounded entry checkpoint target",
                created_ms=now_ms(),
                source_wallet=self.config.source_wallet.lower(),
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            entry_intent = self._active_smoke_intent(
                cloid=entry_cloid,
                coin=coin,
                side="buy",
                size=smoke_size,
                price=entry_price,
                reduce_only=False,
                reason=f"operator {operation_label} entry",
                desired_state_id=entry_plan.state_id,
                mode=required_mode,
                source_event_key=operation_slug,
                execution_proof=(
                    round_trip_quote.to_payload() if round_trip_quote is not None else {}
                ),
            )
            if not self.store.prepare_execution_plan(entry_plan, [entry_intent]):
                detail = f"{operation_label} entry plan identity already exists"
                self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                raise RuntimeError(detail)
            guard = ExecutionGuard(
                risk=self.config.risk,
                ops=self.config.ops,
                store=self.store,
                asset_meta=meta,
                mids=mids,
                mode=self.config.mode,
            )
            guard_decision = guard.check_cycle([entry_intent])
            if guard_decision.ok:
                guard_decision = guard.check_intent(
                    entry_intent,
                    projected_positions=dict(before_reconcile.positions),
                )
            if not guard_decision.ok:
                self.safe_mode.trip(guard_decision.reason, guard_decision.detail)
                blocked = self._blocked_report(
                    entry_intent,
                    guard_decision.reason,
                    guard_decision.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            try:
                control_source, control_reconcile = self._refresh_active_smoke_truth(
                    operation=f"{operation_label} control refresh"
                )
            except Exception as exc:
                if not self.safe_mode.enabled:
                    refresh_reason = (
                        SafeModeReason.STALE_SOURCE
                        if "source" in str(exc).lower()
                        else SafeModeReason.STALE_FOLLOWER
                    )
                    self.safe_mode.trip(refresh_reason, str(exc))
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "control_source": None,
                        "control_reconcile": {"error": str(exc)},
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            leverage_report = self._set_canary_leverage(coin, operation=operation_slug)
            if leverage_report.status != IntentStatus.ACKED:
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "leverage": leverage_report,
                        "dead_man": None,
                        "dead_man_clear": None,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )

            dead_man = self._schedule_dead_man_cancel(
                scheduled_time_ms=now_ms() + self.config.ops.dead_man_cancel_ms,
                operation=operation_slug,
                count_rate=True,
            )
            if dead_man is not None:
                self.store.append_execution_report(dead_man)
            entry = None
            exit_report = None
            after_reconcile: ReconcileSnapshot | dict[str, str] | None = None
            dead_man_clear = None
            active_smoke_finalization: dict[str, Any] | None = None
            if self.safe_mode.enabled or self._dead_man_blocks_execution(dead_man):
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "dead_man": dead_man,
                        "dead_man_clear": None,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )

            try:
                pre_entry_source, pre_entry_reconcile = self._refresh_active_smoke_truth(
                    operation=f"{operation_label} pre-entry refresh"
                )
            except Exception as exc:
                if not self.safe_mode.enabled:
                    refresh_reason = (
                        SafeModeReason.STALE_SOURCE
                        if "source" in str(exc).lower()
                        else SafeModeReason.STALE_FOLLOWER
                    )
                    self.safe_mode.trip(
                        refresh_reason,
                        f"{operation_label} pre-entry reconcile failed: {exc}",
                    )
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                dead_man_clear = None
                if dead_man is not None and dead_man.status == IntentStatus.ACKED:
                    dead_man_clear = self._schedule_dead_man_cancel(
                        scheduled_time_ms=None,
                        operation=f"{operation_slug}_pre_entry_abort",
                        count_rate=False,
                    )
                    if dead_man_clear is not None:
                        self.store.append_execution_report(dead_man_clear)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "pre_entry_source": None,
                        "pre_entry_reconcile": {"error": str(exc)},
                        "dead_man": dead_man,
                        "dead_man_clear": dead_man_clear,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": None,
                        "balance_before": balance_before,
                        "balance_after": None,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )
            if self.safe_mode.enabled:
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "pre_entry_source": pre_entry_source,
                        "pre_entry_reconcile": pre_entry_reconcile,
                        "dead_man": dead_man,
                        "dead_man_clear": None,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": pre_entry_reconcile,
                        "balance_before": balance_before,
                        "balance_after": self._reconcile_account_value(pre_entry_reconcile),
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )

            pre_entry_balance = self._reconcile_account_value(pre_entry_reconcile)
            if required_mode == Mode.LIVE and (
                pre_entry_balance is None
                or not (
                    MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD
                    <= pre_entry_balance
                    <= MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD
                )
            ):
                detail = (
                    f"{operation_label} pre-send follower account value must remain between "
                    f"${MAINNET_CANARY_MIN_ACCOUNT_VALUE_USD} and "
                    f"${MAINNET_CANARY_MAX_ACCOUNT_VALUE_USD}"
                )
                self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                blocked = self._blocked_report(
                    entry_intent,
                    self.safe_mode.reason,
                    self.safe_mode.detail,
                )
                self.store.append_execution_report(blocked)
                if dead_man is not None and dead_man.status == IntentStatus.ACKED:
                    dead_man_clear = self._schedule_dead_man_cancel(
                        scheduled_time_ms=None,
                        operation=f"{operation_slug}_pre_send_balance_block",
                        count_rate=False,
                    )
                    if dead_man_clear is not None:
                        self.store.append_execution_report(dead_man_clear)
                return to_jsonable(
                    {
                        "passed": False,
                        "preflight": report,
                        "before_reconcile": before_reconcile,
                        "pre_entry_source": pre_entry_source,
                        "pre_entry_reconcile": pre_entry_reconcile,
                        "dead_man": dead_man,
                        "dead_man_clear": dead_man_clear,
                        "entry": blocked,
                        "exit": None,
                        "after_reconcile": pre_entry_reconcile,
                        "balance_before": balance_before,
                        "balance_after": pre_entry_balance,
                        "balance_delta": None,
                        **self._safe_mode_payload(cleared=False),
                    }
                )

            pre_send_round_trip_quote = round_trip_quote
            if round_trip_quote is not None:
                try:
                    pre_send_round_trip_quote, market_blockers = self.load_hip3_round_trip_quote(
                        coin,
                        opening_side="buy",
                        requested_size=smoke_size,
                        asset_meta=meta[coin],
                    )
                except Exception as exc:
                    market_blockers = [f"{coin} HIP-3 pre-send market check failed: {exc}"]
                    pre_send_round_trip_quote = None
                if pre_send_round_trip_quote is not None:
                    envelope_distance = (
                        pre_send_round_trip_quote.oracle_px
                        * self.config.risk.hip3_oracle_envelope_bps
                        / Decimal("10000")
                    )
                    lower = pre_send_round_trip_quote.oracle_px - envelope_distance
                    upper = pre_send_round_trip_quote.oracle_px + envelope_distance
                    if pre_send_round_trip_quote.entry_limit > entry_price:
                        market_blockers.append(
                            f"{coin} entry depth moved beyond the persisted limit {entry_price}"
                        )
                    if not lower <= entry_price <= upper:
                        market_blockers.append(
                            f"{coin} persisted entry limit moved outside the fresh oracle envelope"
                        )
                if pre_send_round_trip_quote is None or market_blockers:
                    detail = "; ".join(market_blockers)
                    self.safe_mode.trip(SafeModeReason.RISK_LIMIT, detail)
                    blocked = self._blocked_report(
                        entry_intent,
                        self.safe_mode.reason,
                        self.safe_mode.detail,
                    )
                    self.store.append_execution_report(blocked)
                    if dead_man is not None and dead_man.status == IntentStatus.ACKED:
                        dead_man_clear = self._schedule_dead_man_cancel(
                            scheduled_time_ms=None,
                            operation=f"{operation_slug}_pre_send_block",
                            count_rate=False,
                        )
                        if dead_man_clear is not None:
                            self.store.append_execution_report(dead_man_clear)
                    return to_jsonable(
                        {
                            "passed": False,
                            "preflight": report,
                            "coin": coin,
                            "size": smoke_size,
                            "market_admission": {
                                "passed": False,
                                "initial": round_trip_quote,
                                "pre_send": pre_send_round_trip_quote,
                                "blockers": market_blockers,
                            },
                            "before_reconcile": before_reconcile,
                            "pre_entry_source": pre_entry_source,
                            "pre_entry_reconcile": pre_entry_reconcile,
                            "dead_man": dead_man,
                            "dead_man_clear": dead_man_clear,
                            "entry": blocked,
                            "exit": None,
                            "after_reconcile": pre_entry_reconcile,
                            "balance_before": balance_before,
                            "balance_after": self._reconcile_account_value(pre_entry_reconcile),
                            "balance_delta": Decimal("0"),
                            **self._safe_mode_payload(cleared=False),
                        }
                    )

            entry = self._timed_exchange_action(
                intent_id=entry_intent.intent_id,
                cloid=entry_cloid,
                count_rate=True,
                action=lambda: self.execution_adapter.place_limit_order(
                    coin=coin,
                    side="buy",
                    size=smoke_size,
                    price=entry_price,
                    cloid=entry_cloid,
                    reduce_only=False,
                    tif="Ioc",
                ),
            )
            if round_trip_quote is not None and pre_send_round_trip_quote is not None:
                entry = replace(
                    entry,
                    payload={
                        **entry.payload,
                        "round_trip_admission": round_trip_quote.to_payload(),
                        "round_trip_pre_send": pre_send_round_trip_quote.to_payload(),
                    },
                )
            self.store.append_execution_report(entry)
            if entry.status == IntentStatus.ACKED and not self.safe_mode.enabled:
                self.safe_mode.trip(
                    SafeModeReason.RESTART_MID_FILL,
                    f"{operation_label} entry was non-terminal; cancel and cleanup started",
                )
            cleanup = self._run_passive_canary_cleanup(
                coin=coin,
                entry_cloid=entry_cloid,
                cancel_entry=entry.status in {IntentStatus.SENT, IntentStatus.ACKED},
                max_cleanup_size=smoke_size,
                baseline_positions={},
                asset_meta=meta[coin],
                fallback_mid=mids[coin],
                operation=operation_label,
            )
            entry_cancel_reports = cleanup["cancel_reports"]
            cleanup_reports = cleanup["cleanup_reports"]
            exit_report = cleanup_reports[-1] if cleanup_reports else None
            after_reconcile = cleanup["reconcile"]
            if entry.status != IntentStatus.FILLED and not self.safe_mode.enabled:
                self.safe_mode.trip(
                    SafeModeReason.RESTART_MID_FILL,
                    f"{operation_label} entry did not fill completely: {entry.exchange_status}",
                )
            if (
                entry.status == IntentStatus.FILLED
                and (exit_report is None or exit_report.status != IntentStatus.FILLED)
                and not self.safe_mode.enabled
            ):
                status = "missing" if exit_report is None else exit_report.exchange_status
                self.safe_mode.trip(
                    SafeModeReason.AMBIGUOUS_EXCHANGE_RESPONSE,
                    f"{operation_label} exit did not fill completely: {status}",
                )
            if (
                cleanup["flat"]
                and not self.safe_mode.enabled
                and dead_man is not None
                and dead_man.status == IntentStatus.ACKED
            ):
                dead_man_clear = self._schedule_dead_man_cancel(
                    scheduled_time_ms=None,
                    operation=operation_slug,
                    count_rate=False,
                )
                if dead_man_clear is not None:
                    self.store.append_execution_report(dead_man_clear)
                if (
                    dead_man_clear is None
                    or dead_man_clear.status != IntentStatus.ACKED
                    or dead_man_clear.exchange_status
                    not in {"dead_man_cleared", "watchdog_containment_released"}
                ) and not self.safe_mode.enabled:
                    self.safe_mode.trip(
                        SafeModeReason.CANCEL_REJECT,
                        f"{operation_label} could not prove containment was released",
                    )
            if (
                isinstance(after_reconcile, ReconcileSnapshot)
                and cleanup["flat"]
                and not self.safe_mode.enabled
            ):
                active_smoke_finalization = self._finalize_execution_truth(
                    after_reconcile,
                    trigger=operation_label,
                    keep_incident_safe=False,
                )
            balance_after = (
                self._reconcile_account_value(after_reconcile)
                if not isinstance(after_reconcile, dict)
                else None
            )
            balance_delta = (
                balance_after - balance_before
                if balance_before is not None and balance_after is not None
                else None
            )
            passed = (
                entry.status == IntentStatus.FILLED
                and len(cleanup_reports) == 1
                and cleanup_reports[0].status == IntentStatus.FILLED
                and not self.safe_mode.enabled
                and not isinstance(after_reconcile, dict)
                and cleanup["flat"]
                and balance_delta is not None
                and balance_delta != 0
            )
            return to_jsonable(
                {
                    "passed": passed,
                    "preflight": report,
                    "coin": coin,
                    "size": smoke_size,
                    "entry_price": entry_price,
                    "notional": notional,
                    "market_admission": (
                        {
                            "passed": True,
                            "initial": round_trip_quote,
                            "pre_send": pre_send_round_trip_quote,
                        }
                        if round_trip_quote is not None
                        else None
                    ),
                    "before_reconcile": before_reconcile,
                    "leverage": leverage_report,
                    "dead_man": dead_man,
                    "dead_man_clear": dead_man_clear,
                    "entry": entry,
                    "entry_cancel": (entry_cancel_reports[0] if entry_cancel_reports else None),
                    "exit": exit_report,
                    "cleanup": cleanup_reports,
                    "after_reconcile": after_reconcile,
                    "balance_before": balance_before,
                    "balance_after": balance_after,
                    "balance_delta": balance_delta,
                    "execution_finalization": active_smoke_finalization,
                    "safe_mode": {
                        "enabled": self.safe_mode.enabled,
                        "reason": self.safe_mode.reason.value,
                        "detail": self.safe_mode.detail,
                    },
                }
            )
        finally:
            self._reset_signed_action_context()
            self._release_exchange_lease(operation_slug)

    def _active_smoke_intent(
        self,
        *,
        cloid: str,
        coin: str,
        side: str,
        size: Decimal,
        price: Decimal,
        reduce_only: bool,
        reason: str,
        desired_state_id: str,
        mode: Mode,
        source_event_key: str,
        execution_proof: dict[str, Any] | None = None,
    ) -> FollowerIntent:
        return FollowerIntent(
            intent_id=deterministic_cloid(f"{source_event_key}-intent", cloid),
            cloid=cloid,
            action=IntentAction.REDUCE if reduce_only else IntentAction.OPEN,
            coin=coin,
            side=side,
            size=size,
            price=price,
            reduce_only=reduce_only,
            mode=mode,
            source_event_key=source_event_key,
            reason=reason,
            created_ms=now_ms(),
            desired_state_id=desired_state_id,
            status=IntentStatus.PENDING,
            execution_proof=dict(execution_proof or {}),
        )

    def _refresh_active_smoke_truth(self, *, operation: str) -> tuple[Any, ReconcileSnapshot]:
        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")
        try:
            source_snapshot = self.observer.reconcile_once()
        except Exception as exc:
            raise RuntimeError(f"{operation} source refresh failed: {exc}") from exc
        if not self._check_source_freshness(source_snapshot.observed_ms):
            raise RuntimeError(f"{operation} source truth is stale immediately after refresh")
        self._active_plan_source_observed_ms = source_snapshot.observed_ms
        try:
            follower_snapshot = self.execution_adapter.reconcile()
        except Exception as exc:
            raise RuntimeError(f"{operation} follower refresh failed: {exc}") from exc
        self.store.append_reconcile_snapshot(follower_snapshot)
        if not self._check_follower_freshness(follower_snapshot.observed_ms):
            raise RuntimeError(f"{operation} follower truth is stale immediately after refresh")
        self._active_plan_follower_observed_ms = follower_snapshot.observed_ms
        if follower_snapshot.positions or follower_snapshot.open_orders:
            self.safe_mode.trip(
                SafeModeReason.MANUAL_INTERVENTION,
                f"{operation} requires a flat follower with no open orders",
            )
            raise RuntimeError(self.safe_mode.detail)
        return source_snapshot, follower_snapshot

    def _run_passive_canary_cleanup(
        self,
        *,
        coin: str,
        entry_cloid: str,
        cancel_entry: bool,
        max_cleanup_size: Decimal,
        baseline_positions: dict[str, Position],
        asset_meta: AssetMeta,
        fallback_mid: Decimal,
        operation: str,
    ) -> dict[str, Any]:
        """Cancel bot-owned smoke orders and make bounded reduce-only cleanup attempts.

        The caller proves that the target coin was flat before the test. This helper will
        therefore close at most the test's requested size, will not touch any unrelated
        position or order, and will stop after three cleanup sends. Every attempt starts
        from a fresh follower reconcile and a fresh mid when the read side is available.
        """

        if self.execution_adapter is None:
            raise RuntimeError("execution adapter is not configured")

        cancel_reports: list[ExecutionReport] = []
        cleanup_reports: list[ExecutionReport] = []
        managed_cloids = {entry_cloid.lower()}
        pending_cancels = [entry_cloid] if cancel_entry else []
        cancel_attempts = 0
        cleanup_attempts = 0
        latest: ReconcileSnapshot | dict[str, str] | None = None
        baseline_other = {
            canonical_market_symbol(symbol): position
            for symbol, position in baseline_positions.items()
            if canonical_market_symbol(symbol) != coin and position.size != 0
        }

        # Three cancels plus three reduce-only attempts and one final observation fit in
        # seven rounds. The extra round is defensive and still leaves a hard finite bound.
        for _round in range(8):
            while pending_cancels and cancel_attempts < 6:
                cancel_cloid = pending_cancels.pop(0)
                cancel_attempts += 1
                cancel_report = self._timed_exchange_action(
                    intent_id="cancel:" + cancel_cloid,
                    cloid=cancel_cloid,
                    count_rate=False,
                    risk_reducing=True,
                    action=lambda cancel_cloid=cancel_cloid: self.execution_adapter.cancel_by_cloid(
                        coin, cancel_cloid
                    ),
                )
                self.store.append_execution_report(cancel_report)
                cancel_reports.append(cancel_report)
                self._handle_cancel_report(cancel_report)
                if cancel_report.status == IntentStatus.SKIPPED:
                    break

            try:
                snapshot = self.execution_adapter.reconcile()
            except Exception as exc:
                latest = {"error": str(exc)}
                self.safe_mode.trip(
                    SafeModeReason.STALE_FOLLOWER,
                    f"{operation} cleanup follower reconcile failed: {exc}",
                )
                break
            self.store.append_reconcile_snapshot(snapshot)
            latest = snapshot
            self._active_plan_follower_observed_ms = snapshot.observed_ms
            if not self._check_follower_freshness(snapshot.observed_ms):
                break

            actual_other = {
                canonical_market_symbol(symbol): position
                for symbol, position in snapshot.positions.items()
                if canonical_market_symbol(symbol) != coin and position.size != 0
            }
            if not self._positions_match_exact(baseline_other, actual_other):
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation} observed unrelated position drift during cleanup",
                )
                break

            managed_open = []
            unrelated_open = []
            for order in snapshot.open_orders:
                order_cloid = (order.cloid or "").lower()
                if order_cloid and order_cloid in managed_cloids:
                    managed_open.append(order)
                else:
                    unrelated_open.append(order)
            if unrelated_open:
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation} observed an unrelated open order during cleanup",
                )
                break
            if managed_open:
                if self._kill_switch_path().exists():
                    break
                if cancel_attempts >= 6:
                    unresolved_cloid = managed_open[0].cloid or entry_cloid
                    try:
                        status_payload = self.execution_adapter.order_status(unresolved_cloid)
                    except Exception as exc:
                        order_status = f"lookup_failed:{exc}"
                    else:
                        _, order_status = classify_order_status(status_payload)
                    self.shield.cancel_reject(
                        unresolved_cloid,
                        order_status=order_status,
                    )
                    break
                pending_cancels.extend(
                    order.cloid
                    for order in managed_open
                    if order.cloid and order.cloid not in pending_cancels
                )
                continue

            position = snapshot.positions.get(coin)
            actual_size = position.size if position is not None else Decimal("0")
            if actual_size == 0:
                return {
                    "cancel_reports": cancel_reports,
                    "cleanup_reports": cleanup_reports,
                    "reconcile": snapshot,
                    "flat": True,
                }
            if abs(actual_size) > max_cleanup_size:
                self.safe_mode.trip(
                    SafeModeReason.MANUAL_INTERVENTION,
                    f"{operation} position {actual_size} exceeds the bounded smoke size "
                    f"{max_cleanup_size}; automatic cleanup refused",
                )
                break
            if cleanup_attempts >= 3:
                self.safe_mode.trip(
                    SafeModeReason.PARTIAL_FILL,
                    f"{operation} exhausted 3 bounded cleanup attempts with residual "
                    f"{coin} position {actual_size}",
                )
                break

            try:
                cleanup_mid = self.load_execution_mids()[coin]
            except Exception as exc:
                self.safe_mode.trip(
                    SafeModeReason.REST_LAG,
                    f"{operation} cleanup mid refresh failed: {exc}; using bounded fallback",
                )
                cleanup_mid = fallback_mid
            is_buy = actual_size < 0
            cleanup_size = quantize_size(abs(actual_size), asset_meta.sz_decimals)
            if cleanup_size <= 0:
                self.safe_mode.trip(
                    SafeModeReason.PARTIAL_FILL,
                    f"{operation} residual {coin} position is below executable precision",
                )
                break
            reduce_only_quote: ReduceOnlyQuote | None = None
            if market_dex(coin):
                try:
                    liquidity_snapshot = self.load_market_liquidity_snapshot(coin)
                    reduce_only_quote, cleanup_blockers = build_reduce_only_quote(
                        liquidity_snapshot,
                        side="buy" if is_buy else "sell",
                        requested_size=cleanup_size,
                        oracle_envelope_bps=self.config.risk.hip3_oracle_envelope_bps,
                        max_age_ms=self.config.risk.stale_source_ms,
                        sz_decimals=asset_meta.sz_decimals,
                        current_ms=now_ms(),
                    )
                except Exception as exc:
                    cleanup_blockers = [f"{coin} HIP-3 cleanup market check failed: {exc}"]
                if reduce_only_quote is None:
                    self.safe_mode.trip(
                        SafeModeReason.PARTIAL_FILL,
                        f"{operation} cannot safely flatten HIP-3 residual: "
                        + "; ".join(cleanup_blockers),
                    )
                    break
                cleanup_price = reduce_only_quote.limit_price
            else:
                cleanup_price = aggressive_ioc_price(
                    cleanup_mid,
                    is_buy=is_buy,
                    slippage_bps=self.config.risk.close_slippage_bps,
                    sz_decimals=asset_meta.sz_decimals,
                )
            cleanup_attempts += 1
            cleanup_cloid = deterministic_cloid(
                "passive-canary-cleanup",
                self.config.mode.value,
                operation,
                entry_cloid,
                cleanup_attempts,
                snapshot.snapshot_id,
            )
            cleanup_plan = DesiredState(
                state_id=deterministic_cloid("passive-canary-cleanup-plan", cleanup_cloid),
                source_event_key=operation,
                mode=self.config.mode,
                positions=dict(baseline_positions),
                reason=f"{operation} bounded reduce-only cleanup target",
                created_ms=now_ms(),
                source_wallet=self.config.source_wallet.lower(),
                action_account=self._effective_action_account(),
                source_network=self.config.resolved_source_network.value,
            )
            cleanup_intent = FollowerIntent(
                intent_id=deterministic_cloid("passive-canary-cleanup-intent", cleanup_cloid),
                cloid=cleanup_cloid,
                action=IntentAction.REDUCE,
                coin=coin,
                side="buy" if is_buy else "sell",
                size=cleanup_size,
                price=cleanup_price,
                reduce_only=True,
                mode=self.config.mode,
                source_event_key=operation,
                reason=f"{operation} bounded reduce-only cleanup attempt",
                created_ms=now_ms(),
                desired_state_id=cleanup_plan.state_id,
                status=IntentStatus.PENDING,
                execution_proof=(
                    reduce_only_quote.to_payload() if reduce_only_quote is not None else {}
                ),
            )
            if not self.store.prepare_execution_plan(cleanup_plan, [cleanup_intent]):
                detail = f"{operation} cleanup plan identity already exists"
                self.safe_mode.trip(SafeModeReason.DUPLICATE_INTENT, detail)
                cleanup_report = self._blocked_report(
                    cleanup_intent,
                    SafeModeReason.DUPLICATE_INTENT,
                    detail,
                )
            else:
                cleanup_report = self._timed_exchange_action(
                    intent_id=cleanup_intent.intent_id,
                    cloid=cleanup_cloid,
                    count_rate=False,
                    risk_reducing=True,
                    record_runtime=False,
                    action=lambda cleanup_intent=cleanup_intent: (
                        self.execution_adapter.place_limit_order(
                            coin=cleanup_intent.coin,
                            side=cleanup_intent.side,
                            size=cleanup_intent.size,
                            price=cleanup_intent.price or Decimal("0"),
                            cloid=cleanup_intent.cloid,
                            reduce_only=True,
                            tif="Ioc",
                        )
                    ),
                )
                if reduce_only_quote is not None:
                    cleanup_report = replace(
                        cleanup_report,
                        payload={
                            **cleanup_report.payload,
                            "reduce_only_quote": reduce_only_quote.to_payload(),
                        },
                    )
                cleanup_report, _ = self._normalize_proven_hip3_ioc_zero_fill(
                    cleanup_intent,
                    cleanup_report,
                    stage="bounded_cleanup_zero_fill",
                    paced_retry=False,
                )
                self._record_runtime_result(cleanup_report)
            self.store.append_execution_report(cleanup_report)
            cleanup_reports.append(cleanup_report)
            self._handle_non_terminal_exchange_ack(cleanup_report, cleanup_intent)
            managed_cloids.add(cleanup_cloid.lower())
            if cleanup_report.status in {IntentStatus.SENT, IntentStatus.ACKED}:
                pending_cancels.append(cleanup_cloid)

        return {
            "cancel_reports": cancel_reports,
            "cleanup_reports": cleanup_reports,
            "reconcile": latest,
            "flat": False,
        }

    def _report_filled_size(self, report: ExecutionReport) -> Decimal:
        if report.status != IntentStatus.FILLED:
            return self._execution_report_decimal(
                report,
                "filled_size",
                default=Decimal("0"),
            ) or Decimal("0")
        payload_size = (
            report.payload.get("filled_size") if isinstance(report.payload, dict) else None
        )
        expected_size = (
            report.payload.get("expected_size") if isinstance(report.payload, dict) else None
        )
        if payload_size is not None and payload_size != "":
            return self._execution_report_decimal(report, "filled_size") or Decimal("0")
        if expected_size is not None and expected_size != "":
            return self._execution_report_decimal(report, "expected_size") or Decimal("0")
        return Decimal("0")

    @staticmethod
    def _active_smoke_flat(snapshot: Any) -> bool:
        return not bool(getattr(snapshot, "positions", None)) and not bool(
            getattr(snapshot, "open_orders", None)
        )

    @staticmethod
    def _reconcile_account_value(snapshot: Any) -> Decimal | None:
        payload = getattr(snapshot, "payload", None)
        if not isinstance(payload, dict):
            return None
        raw_value = payload.get("account_value")
        if raw_value is None and isinstance(payload.get("account_context"), dict):
            raw_value = payload["account_context"].get("account_value")
        if raw_value is not None:
            try:
                return parse_decimal(raw_value)
            except (ArithmeticError, TypeError, ValueError):
                return None
        return CopyTraderService._account_value_from_clearinghouse_state(
            payload.get("clearinghouseState")
        )

    @staticmethod
    def _source_snapshot_account_value(snapshot: Any) -> Decimal | None:
        raw_state = getattr(snapshot, "raw_state", None)
        if not isinstance(raw_state, dict):
            return None
        if raw_state.get("accountValue") is not None:
            try:
                return parse_decimal(raw_state["accountValue"])
            except (ArithmeticError, TypeError, ValueError):
                return None
        return CopyTraderService._account_value_from_clearinghouse_state(
            raw_state.get("clearinghouseState")
        )

    @staticmethod
    def _account_value_from_clearinghouse_state(state: Any) -> Decimal | None:
        if not isinstance(state, dict):
            return None
        for key in ("marginSummary", "crossMarginSummary"):
            summary = state.get(key)
            if isinstance(summary, dict) and summary.get("accountValue") is not None:
                try:
                    return parse_decimal(summary["accountValue"])
                except (ArithmeticError, TypeError, ValueError):
                    return None
        return None

    def _initial_sizing_status(self) -> dict[str, Any]:
        return {
            "mode": "not_calculated",
            "fixed_multiplier": self.config.risk.fixed_multiplier,
            "equity_ratio": self.config.risk.equity_ratio,
            "balance_sizing_enabled": self.config.risk.balance_sizing_enabled,
            "source_account_value": None,
            "follower_account_value": None,
            "sizing_equity_cap_usd": self.config.risk.sizing_equity_cap_usd,
            "sizing_equity_usd": None,
            "raw_balance_scale": None,
            "balance_scale": None,
            "max_balance_scale": self.config.risk.max_balance_scale,
            "effective_scale": self.config.risk.equity_ratio or self.config.risk.fixed_multiplier,
            "entry_slippage_bps": self.config.risk.slippage_bps,
            "close_slippage_bps": self.config.risk.close_slippage_bps,
            "slippage_policy": {
                "entry": {
                    "applies_to": "risk-increasing OPEN intents",
                    "bound_bps": self.config.risk.slippage_bps,
                    "guard": "entry price must stay within midpoint +/- bound",
                },
                "reduce_only": {
                    "applies_to": "REDUCE and CLOSE intents with reduce_only=true",
                    "bound_bps": self.config.risk.close_slippage_bps,
                    "guard": "price must stay within midpoint +/- bound and exposure must shrink",
                },
            },
            "detail": "waiting for a copy cycle",
        }

    def _sizing_status(self) -> dict[str, Any]:
        status = {**self._initial_sizing_status(), **self._last_sizing}
        status["source_account_value"] = self._last_source_account_value or status.get(
            "source_account_value"
        )
        status["follower_account_value"] = self._last_follower_account_value or status.get(
            "follower_account_value"
        )
        status["entry_slippage_bps"] = self.config.risk.slippage_bps
        status["close_slippage_bps"] = self.config.risk.close_slippage_bps
        status["slippage_policy"] = self._initial_sizing_status()["slippage_policy"]
        return status

    def _connection_integrity(self, source_health: dict[str, Any]) -> dict[str, Any]:
        threshold = self.config.ops.connection_siren_after_ms
        age_ms = source_health.get("latest_age_ms")
        event_count = int(source_health.get("event_count") or 0)
        if age_ms is None:
            status = "waiting_for_source"
            siren = False
            detail = "no source events have been journaled yet"
        elif age_ms > threshold:
            status = "lost"
            siren = True
            detail = f"latest source event is older than {threshold}ms"
        else:
            status = "ok"
            siren = False
            detail = "source event freshness is within the siren threshold"
        return {
            "status": status,
            "siren": siren,
            "latest_age_ms": age_ms,
            "threshold_ms": threshold,
            "event_count": event_count,
            "required_action": (
                "Check network/source websocket immediately; consider external flattening if exposed."
                if siren
                else "Monitor source stream and copy loop."
            ),
            "detail": detail,
        }

    def _effective_action_account(self) -> str:
        return (
            self.config.exchange.vault_address
            or self.config.exchange.follower_account_address
            or ""
        ).lower()

    def _subaccount_monitoring(self, connection_integrity: dict[str, Any]) -> list[dict[str, Any]]:
        follower = self.config.exchange.follower_account_address or "local paper/shadow"
        action_account = (
            self.config.exchange.vault_address or self.config.exchange.follower_account_address
        )
        primary = [
            {
                "slot": "primary",
                "subaccount": follower,
                "assigned_source": self.config.source_wallet,
                "mode": self.config.mode.value,
                "status": "active"
                if self.config.mode in {Mode.TESTNET, Mode.LIVE}
                else "simulated",
                "connection": connection_integrity["status"],
                "pending_intents": self.store.pending_intent_count(self.config.mode),
                "subaccount_verified": False,
                "operator_verified_at": "",
                "verification": "runtime account",
                "note": "manual source-to-subaccount assignment",
            },
        ]
        configured = [
            {
                "slot": assignment.slot,
                "subaccount": assignment.subaccount,
                "assigned_source": assignment.source_wallet,
                "mode": assignment.mode,
                "status": (
                    "enabled"
                    if assignment.enabled and assignment.subaccount_verified
                    else "blocked:unverified"
                    if assignment.enabled
                    else "preloaded"
                ),
                "connection": (
                    connection_integrity["status"]
                    if (
                        assignment.subaccount == action_account
                        and assignment.source_wallet == self.config.source_wallet
                    )
                    else "not_connected"
                ),
                "pending_intents": (
                    self.store.pending_intent_count(self.config.mode)
                    if assignment.subaccount == action_account
                    else 0
                ),
                "subaccount_verified": assignment.subaccount_verified,
                "operator_verified_at": assignment.operator_verified_at,
                "verification": (
                    f"verified {assignment.operator_verified_at}"
                    if assignment.subaccount_verified and assignment.operator_verified_at
                    else "verified"
                    if assignment.subaccount_verified
                    else "unverified"
                ),
                "note": assignment.note,
            }
            for assignment in self.config.subaccount_assignments
        ]
        reserves = [
            {
                "slot": "reserve-1",
                "subaccount": "unassigned",
                "assigned_source": "unassigned",
                "mode": "planned",
                "status": "placeholder",
                "connection": "not_connected",
                "pending_intents": 0,
                "note": "future simultaneous copytrading slot",
            },
            {
                "slot": "reserve-2",
                "subaccount": "unassigned",
                "assigned_source": "unassigned",
                "mode": "planned",
                "status": "placeholder",
                "connection": "not_connected",
                "pending_intents": 0,
                "note": "future manual account assignment slot",
            },
        ]
        return [*primary, *configured, *reserves]

    def _recent_intent_rows(self, limit: int) -> list[dict[str, Any]]:
        rows = self.store.recent_intents(self.config.mode, limit)
        return [self._dashboard_intent_row(row) for row in rows]

    def _dashboard_intent_row(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = _json_object(row.get("payload_json"))
        cloid = str(row.get("cloid") or "")
        reports = self.store.execution_reports_for_cloid(cloid) if cloid else []
        latest_report = reports[0] if reports else {}
        report_payload = _json_object(latest_report.get("payload_json"))
        report_detail_payload = _json_object(report_payload.get("payload"))
        latest_exchange_status = str(latest_report.get("exchange_status") or "")
        latest_report_status = str(latest_report.get("status") or "")
        latest_report_detail = str(
            report_detail_payload.get("detail")
            or report_detail_payload.get("reason")
            or report_payload.get("detail")
            or report_payload.get("reason")
            or ""
        )
        return {
            **self._select_fields(
                row,
                (
                    "seq",
                    "intent_id",
                    "cloid",
                    "source_event_key",
                    "action",
                    "coin",
                    "mode",
                    "status",
                    "created_ms",
                ),
            ),
            "reason": str(payload.get("reason") or ""),
            "side": str(payload.get("side") or ""),
            "size": str(payload.get("size") or ""),
            "price": str(payload.get("price") or ""),
            "reduce_only": payload.get("reduce_only") is True,
            "latest_report_status": latest_report_status,
            "latest_exchange_status": latest_exchange_status,
            "latest_report_detail": latest_report_detail,
            "outcome": intent_outcome(
                str(row.get("status") or ""),
                latest_report_status=latest_report_status,
                latest_exchange_status=latest_exchange_status,
                proven_liquidity_deferral=(
                    bool(latest_report)
                    and self.store.execution_report_is_proven_hip3_ioc_zero_fill(latest_report)
                ),
            ),
        }

    @staticmethod
    def _select_fields(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
        return {field: row.get(field) for field in fields}

    @staticmethod
    def _bounded_payload_text(value: Any, *, limit: int = 12_000) -> str:
        text = str(value or "")
        if len(text) <= limit:
            return text
        return text[:limit] + f"... [truncated {len(text) - limit} characters]"

    def _latest_mainnet_canary_evidence(self, phase: str) -> dict[str, Any] | None:
        evidence_dir = self.config.db_path.parent / "evidence"
        try:
            candidates = sorted(
                evidence_dir.glob(f"*-{phase}-canary.json"),
                key=lambda path: path.stat().st_mtime_ns,
                reverse=True,
            )
        except OSError:
            return None
        for path in candidates:
            try:
                if path.stat().st_size > 2_000_000:
                    continue
                payload = json.loads(path.read_text(encoding="utf-8-sig"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            final = (
                payload.get("after_reconcile") if phase == "active" else payload.get("reconcile")
            )
            final = final if isinstance(final, dict) else {}
            final_payload = final.get("payload")
            final_payload = final_payload if isinstance(final_payload, dict) else {}
            account_context = final_payload.get("account_context")
            account_context = account_context if isinstance(account_context, dict) else {}
            entry = payload.get("entry") if phase == "active" else payload.get("place")
            exit_report = payload.get("exit") if phase == "active" else payload.get("cancel")
            entry = entry if isinstance(entry, dict) else {}
            exit_report = exit_report if isinstance(exit_report, dict) else {}
            return {
                "phase": phase,
                "file": path.name,
                "passed": payload.get("passed") is True,
                "coin": str(payload.get("coin") or ""),
                "size": payload.get("size"),
                "notional": payload.get("notional", payload.get("signed_order_notional")),
                "balance_before": payload.get("balance_before"),
                "balance_after": payload.get("balance_after", account_context.get("account_value")),
                "balance_delta": payload.get("balance_delta"),
                "protection_mode": str(payload.get("protection_mode") or ""),
                "entry_cloid": str(entry.get("cloid") or ""),
                "entry_status": str(entry.get("exchange_status") or entry.get("status") or ""),
                "exit_cloid": str(exit_report.get("cloid") or ""),
                "exit_status": str(
                    exit_report.get("exchange_status") or exit_report.get("status") or ""
                ),
                "flat": not bool(final.get("positions")) and not bool(final.get("open_orders")),
                "observed_ms": final.get("observed_ms"),
            }
        return None

    def _mainnet_validation_summary(self, *, include_journal_proof: bool = True) -> dict[str, Any]:
        passive = self._latest_mainnet_canary_evidence("passive")
        active = self._latest_mainnet_canary_evidence("active")
        passive_proof = self._mainnet_passive_canary_proof() if include_journal_proof else None
        active_proof = self._mainnet_active_canary_proof() if include_journal_proof else None
        active_passed = bool(active and active.get("passed") and active.get("flat"))
        passive_passed = bool(passive and passive.get("passed") and passive.get("flat"))
        status = (
            "active_round_trip_passed"
            if active_passed and (active_proof is None or active_proof["passed"])
            else "passive_canary_passed"
            if passive_passed and (passive_proof is None or passive_proof["passed"])
            else "not_proven"
        )
        return {
            "status": status,
            "passive": passive,
            "active": active,
            "passive_journal_proof": (
                passive_proof["passed"] if passive_proof is not None else None
            ),
            "active_journal_proof": active_proof["passed"] if active_proof is not None else None,
            "operator_lock_armed": self._kill_switch_path().exists(),
            "next_action": (
                "Manually review the active entry/exit on Hyperliquid; no retry is permitted."
                if status == "active_round_trip_passed"
                else "Use the continuous launch preview and WebSocket startup preflight; "
                "this legacy summary does not authorize execution."
            ),
        }

    @staticmethod
    def _bounded_json_file(path, *, limit: int = 4_000_000) -> dict[str, Any] | None:
        try:
            if path.stat().st_size > limit:
                return None
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _mainnet_follow_validation_summary(self) -> dict[str, Any]:
        runs_dir = self.config.db_path.parent.parent / "runs"
        report = self._bounded_json_file(runs_dir / "mainnet-acc7-current-report.json") or {}
        try:
            run_dirs = sorted(
                runs_dir.glob("mainnet-acc7-*"),
                key=lambda path: path.stat().st_mtime_ns,
                reverse=True,
            )
        except OSError:
            run_dirs = []
        progress: dict[str, Any] = {}
        for run_dir in run_dirs:
            manifest = self._bounded_json_file(run_dir / "manifest.json")
            if manifest is None:
                continue
            diagnostics_path = run_dir / "diagnostics.jsonl"
            try:
                if diagnostics_path.stat().st_size > 4_000_000:
                    continue
                lines = [
                    line
                    for line in diagnostics_path.read_text(encoding="utf-8-sig").splitlines()
                    if line.strip()
                ]
                latest = json.loads(lines[-1]) if lines else {}
            except (OSError, UnicodeError, json.JSONDecodeError):
                latest = {}
                lines = []
            if not isinstance(latest, dict):
                latest = {}
            heartbeat = self._bounded_json_file(run_dir / "supervisor-heartbeat.json") or {}
            follower = latest.get("follower")
            follower = follower if isinstance(follower, dict) else {}
            source = latest.get("source")
            source = source if isinstance(source, dict) else {}
            runner = latest.get("runner")
            runner = runner if isinstance(runner, dict) else {}
            watchdog = latest.get("watchdog")
            watchdog = watchdog if isinstance(watchdog, dict) else {}
            progress = {
                "run": run_dir.name,
                "active": not (run_dir / "summary.json").exists(),
                "deadline_at": str(manifest.get("deadline_at") or ""),
                "snapshot_count": len(lines),
                "latest_observed_at": str(latest.get("observed_at") or ""),
                "healthy": latest.get("healthy") is True,
                "runner_online": runner.get("online") is True,
                "watchdog_ready": watchdog.get("ready") is True,
                "watchdog_detail": str(watchdog.get("detail") or ""),
                "safe_mode": bool((latest.get("safe_mode") or {}).get("enabled"))
                if isinstance(latest.get("safe_mode"), dict)
                else None,
                "pending_intents": latest.get("pending_intents"),
                "source_positions": len(source.get("positions") or []),
                "follower_positions": len(follower.get("positions") or []),
                "open_orders": follower.get("open_orders"),
                "account_value": follower.get("account_value"),
                "heartbeat_outcome": str(heartbeat.get("outcome") or ""),
            }
            break
        verdict = str(report.get("verdict") or ("in_progress" if progress else "not_started"))
        requirements = report.get("requirements")
        requirements = requirements if isinstance(requirements, dict) else {}
        report_diagnostics = report.get("diagnostics")
        report_diagnostics = report_diagnostics if isinstance(report_diagnostics, dict) else {}
        journal = report.get("journal")
        journal = journal if isinstance(journal, dict) else {}
        window = report.get("window")
        window = window if isinstance(window, dict) else {}
        if verdict == "in_progress":
            next_action = (
                "Keep acc7 supervised until the fixed deadline; a real filled open and "
                "reduce-only close are still required for lifecycle proof."
            )
        elif verdict == "acc7_validation_passed":
            next_action = (
                "Review the final acc7 evidence before any separately authorized fleet test."
            )
        elif verdict == "inconclusive_no_copied_trade":
            next_action = "Repeat only with a separately reviewed active source; no fleet launch proof exists."
        else:
            next_action = "Review the failed requirement and keep the fleet disabled."
        return {
            "verdict": verdict,
            "generated_at": str(report.get("generated_at") or ""),
            "acc7_gate_passed": report.get("acc7_gate_passed") is True,
            "fleet_launch_ready": report.get("fleet_launch_ready") is True,
            "fleet_boundary": str(report.get("fleet_boundary") or ""),
            "window": window,
            "diagnostics": {
                "snapshot_count": report_diagnostics.get("snapshot_count"),
                "maximum_snapshot_gap_seconds": report_diagnostics.get(
                    "maximum_snapshot_gap_seconds"
                ),
                "all_healthy": report_diagnostics.get("all_healthy"),
            },
            "journal": {
                "source_fill_events": journal.get("source_fill_events", 0),
                "copy_intent_count": len(journal.get("copy_intents") or []),
                "filled_open_observed": journal.get("filled_open_observed") is True,
                "filled_reduce_only_close_observed": (
                    journal.get("filled_reduce_only_close_observed") is True
                ),
                "safe_mode_incident_count": len(journal.get("safe_mode_incidents") or []),
            },
            "requirements": requirements,
            "progress": progress,
            "next_action": next_action,
        }

    def dashboard(
        self,
        *,
        include_recent: bool = True,
        security_cached: bool = True,
    ) -> dict[str, Any]:
        kill_switch = ExecutionGuard(
            risk=self.config.risk,
            ops=self.config.ops,
            store=self.store,
            asset_meta={},
            mids={},
            mode=self.config.mode,
        ).kill_switch_path()
        lease_diag = self._exchange_lease_diagnostics()
        persistent_rate_stats = self.store.recent_counted_exchange_action_stats(
            now_ms() - self.exchange_rate_limiter.window_ms
        )
        persistent_failure_stats = self.store.consecutive_exchange_failure_stats()
        persistent_circuit_open = (
            not self._persistent_circuit_breaker_decision().ok
            if self.config.mode in {Mode.TESTNET, Mode.LIVE}
            else False
        )
        recent_source_events: list[dict[str, Any]] = []
        recent_intents: list[dict[str, Any]] = []
        recent_reports: list[dict[str, Any]] = []
        recent_reconciles: list[dict[str, Any]] = []
        recent_safe_mode: list[dict[str, Any]] = []
        recent_control_audit: list[dict[str, Any]] = []
        reconcile_account = self._effective_action_account()
        if include_recent:
            recent_source_events = self._recent_source_event_rows(20)
            recent_intents = self._recent_intent_rows(20)
            recent_reports = [
                self._select_fields(
                    row,
                    (
                        "seq",
                        "report_id",
                        "intent_id",
                        "cloid",
                        "status",
                        "exchange_status",
                        "created_ms",
                    ),
                )
                for row in self.store.recent("execution_reports", 20)
            ]
            recent_reconciles = (
                self.store.recent_reconcile_snapshots(account=reconcile_account, limit=20)
                if self.config.mode in {Mode.TESTNET, Mode.LIVE} and reconcile_account
                else self.store.recent("reconcile_snapshots", 20)
            )
            recent_safe_mode = [
                self._select_fields(
                    row,
                    ("seq", "transition_id", "enabled", "reason", "detail", "created_ms"),
                )
                for row in self.store.recent("safe_mode_transitions", 10)
            ]
            recent_control_audit = [
                self._select_fields(
                    row,
                    ("seq", "control", "status", "detail", "created_ms"),
                )
                for row in self.store.recent("control_audit", 20)
            ]
        runtime_state = self._runtime_state(include_recent=include_recent)
        source_health = self._source_health(runtime_state, recent_source_events)
        source_dex_context = self._source_dex_context()
        connection_integrity = self._connection_integrity(source_health)
        reconciliation_status = self._reconciliation_status(
            source_health=source_health,
            recent_reconciles=recent_reconciles,
            include_recent=include_recent,
        )
        recent_reconciles = [
            {
                **self._select_fields(
                    row,
                    ("seq", "snapshot_id", "account", "source", "observed_ms", "created_ms"),
                ),
                "payload_json": self._bounded_payload_text(row.get("payload_json")),
            }
            for row in recent_reconciles
        ]
        local_preflight = to_jsonable(build_preflight_report(self.config))
        active_assignment = to_jsonable(active_subaccount_assignment_status(self.config))
        account_context = self._account_context_status()
        return {
            "mode": self.config.mode.value,
            "source_wallet": self.config.source_wallet,
            "preflight": {
                **local_preflight,
                "scope": "local_config",
                "signed_account_probe": False,
            },
            "safe_mode": self._safe_mode_status(),
            "risk": to_jsonable(asdict(self.config.risk)),
            "paper_positions": to_jsonable(self.paper.positions),
            "recent_source_events": recent_source_events,
            "recent_intents": recent_intents,
            "recent_reports": recent_reports,
            "recent_reconciles": recent_reconciles,
            "recent_safe_mode": recent_safe_mode,
            "recent_control_audit": recent_control_audit,
            "mainnet_validation": self._mainnet_validation_summary(
                include_journal_proof=include_recent
            ),
            "mainnet_follow_validation": self._mainnet_follow_validation_summary(),
            "runtime_state": runtime_state,
            "runner": self.runner_status(),
            "containment_watchdog": self.containment_watchdog_status(),
            "validation_market_universe": self.validation_market_universe_status(),
            "source_open_orders_cache": self.observer.open_order_cache_status(),
            "source_health": source_health,
            "source_dex_context": source_dex_context,
            "connection_integrity": connection_integrity,
            "reconciliation_status": reconciliation_status,
            "sizing": to_jsonable(self._sizing_status()),
            "account_context": account_context,
            "active_subaccount_assignment": active_assignment,
            "subaccount_monitoring": to_jsonable(self._subaccount_monitoring(connection_integrity)),
            "follower_account": self.config.exchange.follower_account_address
            or "local paper/shadow",
            "ops": {
                "kill_switch_path": str(kill_switch),
                "kill_switch_active": kill_switch.exists(),
                "pending_intent_count": self.store.pending_intent_count(self.config.mode),
                "pending_source_reaction_count": self.store.unfinished_source_reaction_count(
                    source_wallet=self.config.source_wallet
                ),
                "max_new_intents_per_cycle": self.config.ops.max_new_intents_per_cycle,
                "max_open_intents": self.config.ops.max_open_intents,
                "max_exchange_actions_per_minute": self.config.ops.max_exchange_actions_per_minute,
                "exchange_action_timeout_s": to_jsonable(self.config.ops.exchange_action_timeout_s),
                "exchange_expires_after_ms": self.config.ops.exchange_expires_after_ms,
                "dead_man_cancel_ms": self.config.ops.dead_man_cancel_ms,
                "dead_man_policy": self.config.ops.dead_man_policy.value,
                "containment_watchdog_ttl_ms": (self.config.ops.containment_watchdog_ttl_ms),
                "auth_probe_interval_ms": self.config.ops.auth_probe_interval_ms,
                "info_timeout_s": to_jsonable(self.config.ops.info_timeout_s),
                "gui_token_configured": bool(self.config.ops.gui_token),
                "dashboard_control_max_per_minute": (
                    self.config.ops.dashboard_control_max_per_minute
                ),
                "runtime_lease_ttl_ms": self.config.ops.runtime_lease_ttl_ms,
                "dashboard_security_audit_ttl_ms": (
                    self.config.ops.dashboard_security_audit_ttl_ms
                ),
                "source_reaction_queue_size": self.config.ops.source_reaction_queue_size,
                "source_websocket_idle_timeout_ms": (
                    self.config.ops.source_websocket_idle_timeout_ms
                ),
                "source_websocket_heartbeat_timeout_ms": (
                    self.config.ops.source_websocket_heartbeat_timeout_ms
                ),
                "source_websocket_reconnect_attempts": (
                    self.config.ops.source_websocket_reconnect_attempts
                ),
                "source_websocket_reconnect_backoff_ms": (
                    self.config.ops.source_websocket_reconnect_backoff_ms
                ),
                "source_fill_backfill_lookback_ms": (
                    self.config.ops.source_fill_backfill_lookback_ms
                ),
                "source_fill_backfill_overlap_ms": self.config.ops.source_fill_backfill_overlap_ms,
                "source_fill_backfill_max_pages": self.config.ops.source_fill_backfill_max_pages,
                "connection_siren_after_ms": self.config.ops.connection_siren_after_ms,
            },
            "runtime": {
                "rate_limiter_events": len(self.exchange_rate_limiter.events),
                "persistent_rate_limiter_events": persistent_rate_stats["count"],
                "circuit_breaker_failures": self.circuit_breaker.consecutive_failures,
                "circuit_breaker_open": (
                    self.circuit_breaker.opened_ms is not None or persistent_circuit_open
                ),
                "persistent_circuit_breaker_failures": persistent_failure_stats[
                    "consecutive_failures"
                ],
                "exchange_lease": lease_diag["lease"],
                "exchange_lease_status": lease_diag["status"],
                "exchange_lease_ms_remaining": lease_diag["ms_remaining"],
            },
            "security": self.cached_security_audit() if security_cached else self.security_audit(),
        }

    def _account_context_status(self) -> dict[str, Any]:
        expected = self.config.exchange.expected_account_mode.value
        account = self._effective_action_account()
        if self.config.mode not in {Mode.TESTNET, Mode.LIVE} or not account:
            return {
                "required": False,
                "expected_mode": expected,
                "detected_mode": "not_required",
                "status": "not_required",
                "collateral_source": "local",
                "account_value": None,
                "spot_usdc_total": None,
                "spot_usdc_hold": None,
                "active_non_default_dexes": [],
                "unsupported_non_default_dexes": [],
            }
        latest = self.store.latest_reconcile_snapshot(account)
        if latest is None:
            return {
                "required": True,
                "expected_mode": expected,
                "detected_mode": "unknown",
                "status": "missing",
                "collateral_source": "unknown",
                "account_value": None,
                "spot_usdc_total": None,
                "spot_usdc_hold": None,
                "active_non_default_dexes": [],
                "unsupported_non_default_dexes": [],
            }
        snapshot = _json_object(latest.get("payload_json"))
        raw_payload = snapshot.get("payload")
        raw_payload = raw_payload if isinstance(raw_payload, dict) else {}
        context = raw_payload.get("account_context")
        context = context if isinstance(context, dict) else {}
        detected = str(context.get("detected_mode") or raw_payload.get("account_mode") or "unknown")
        active = context.get("active_non_default_dexes")
        active = [str(item) for item in active] if isinstance(active, list) else []
        configured_dexes = sorted(
            {
                market_dex(symbol)
                for symbol in self.config.risk.allowed_symbols
                if market_dex(symbol)
            }
        )
        unsupported = context.get("unsupported_non_default_dexes")
        if isinstance(unsupported, list):
            unsupported = [str(item) for item in unsupported]
        elif self.config.source_dex_scope.value == "all_configured_markets":
            unsupported = [dex for dex in active if dex not in configured_dexes]
        else:
            unsupported = list(active)
        mode_matches = expected == "auto" or detected == expected
        account_value = context.get("account_value", raw_payload.get("account_value"))
        positive_value = False
        try:
            positive_value = parse_decimal(account_value) > 0
        except (ArithmeticError, TypeError, ValueError):
            pass
        status = "ready"
        if detected not in {"standard", "unified"}:
            status = "unknown"
        elif (
            self.config.source_dex_scope.value == "all_configured_markets" and detected != "unified"
        ):
            status = "market_scope_requires_unified"
        elif not mode_matches:
            status = "mismatch"
        elif unsupported:
            status = "non_default_dex_activity"
        elif not positive_value:
            status = "unfunded"
        return {
            "required": True,
            "expected_mode": expected,
            "detected_mode": detected,
            "status": status,
            "mode_matches": mode_matches,
            "collateral_source": str(context.get("collateral_source") or "unknown"),
            "account_value": account_value,
            "spot_usdc_total": context.get("spot_usdc_total"),
            "spot_usdc_hold": context.get("spot_usdc_hold"),
            "aggregate_observed_ms": context.get("aggregate_observed_ms"),
            "aggregate_dex_count": int(context.get("aggregate_dex_count") or 0),
            "active_non_default_dexes": active,
            "configured_perp_dexes": configured_dexes,
            "unsupported_non_default_dexes": unsupported,
        }

    def _reconciliation_status(
        self,
        *,
        source_health: dict[str, Any],
        recent_reconciles: list[dict[str, Any]],
        include_recent: bool,
    ) -> dict[str, Any]:
        expected_account = self._effective_action_account()
        if not include_recent:
            latest_reconcile = self.store.latest_reconcile_snapshot(
                expected_account if self.config.mode in {Mode.TESTNET, Mode.LIVE} else None
            )
            recent_reconciles = [latest_reconcile] if latest_reconcile is not None else []
        source_age_ms = source_health.get("latest_age_ms")
        source_threshold_ms = self.config.risk.stale_source_ms
        if source_age_ms is None:
            source_status = "missing"
        elif int(source_age_ms) <= source_threshold_ms:
            source_status = "fresh"
        else:
            source_status = "stale"
        follower = self._follower_reconciliation_status(
            recent_reconciles=recent_reconciles,
            include_recent=include_recent,
            expected_account=expected_account,
        )
        blockers: list[str] = []
        if source_status == "missing":
            blockers.append("source_missing")
        elif source_status == "stale":
            blockers.append("source_stale")
        if follower["required"] and follower["status"] in {
            "missing",
            "stale",
            "not_loaded",
            "mismatch",
        }:
            blockers.append(f"follower_{follower['status']}")
        return {
            "source": {
                "status": source_status,
                "latest_age_ms": source_age_ms,
                "threshold_ms": source_threshold_ms,
                "latest_observed_ts_ms": source_health.get("latest_observed_ts_ms"),
                "latest_type": source_health.get("latest_type") or "",
                "latest_subtype": source_health.get("latest_subtype") or "",
                "latest_key": source_health.get("latest_key") or "",
            },
            "follower": follower,
            "blockers": blockers,
            "ready_for_planning": not blockers,
        }

    def _follower_reconciliation_status(
        self,
        *,
        recent_reconciles: list[dict[str, Any]],
        include_recent: bool,
        expected_account: str,
    ) -> dict[str, Any]:
        required = self.config.mode in {Mode.TESTNET, Mode.LIVE}
        threshold_ms = self.config.risk.stale_follower_ms
        account_mismatch = False
        if required and not recent_reconciles:
            latest_any = self.store.latest_reconcile_snapshot()
            observed_account = str((latest_any or {}).get("account") or "").lower()
            if latest_any is not None and observed_account != expected_account.lower():
                recent_reconciles = [latest_any]
                account_mismatch = True
        if not recent_reconciles:
            status = "not_required"
            if required:
                status = "missing" if include_recent else "not_loaded"
            return {
                "required": required,
                "status": status,
                "latest_age_ms": None,
                "threshold_ms": threshold_ms,
                "latest_observed_ms": None,
                "account": expected_account or "local paper/shadow",
                "expected_account": expected_account or "local paper/shadow",
                "source": "",
                "positions": 0,
                "open_orders": 0,
            }
        latest = recent_reconciles[0]
        observed_account = str(latest.get("account") or "")
        if required and observed_account.lower() != expected_account.lower():
            account_mismatch = True
        observed_ms = self._int_field(latest.get("observed_ms"))
        latest_age_ms = max(now_ms() - observed_ms, 0) if observed_ms else None
        if account_mismatch:
            status = "mismatch"
        elif latest_age_ms is None:
            status = "missing"
        elif latest_age_ms <= threshold_ms:
            status = "fresh"
        else:
            status = "stale"
        payload = _json_object(latest.get("payload_json"))
        positions = payload.get("positions")
        open_orders = payload.get("open_orders")
        raw_payload = payload.get("payload")
        raw_payload = raw_payload if isinstance(raw_payload, dict) else {}
        account_context = raw_payload.get("account_context")
        account_context = account_context if isinstance(account_context, dict) else {}
        return {
            "required": required,
            "status": status,
            "latest_age_ms": latest_age_ms,
            "threshold_ms": threshold_ms,
            "latest_observed_ms": observed_ms or None,
            "account": observed_account,
            "expected_account": expected_account or "local paper/shadow",
            "source": str(latest.get("source") or ""),
            "positions": len(positions) if isinstance(positions, dict) else 0,
            "open_orders": len(open_orders) if isinstance(open_orders, list) else 0,
            "account_mode": str(
                account_context.get("detected_mode") or raw_payload.get("account_mode") or "unknown"
            ),
            "collateral_source": str(account_context.get("collateral_source") or "unknown"),
        }

    def _source_health(
        self,
        runtime_state: dict[str, Any],
        recent_source_events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        latest = recent_source_events[0] if recent_source_events else None
        if latest is None:
            latest_rows = runtime_state.get("latest_source_events") or []
            latest = latest_rows[0] if latest_rows else None
        if latest is None:
            latest = runtime_state.get("latest_source_event")
        if latest is None:
            return {
                "event_count": int(runtime_state.get("source_event_count") or 0),
                "latest_age_ms": None,
                "latest_exchange_ts_ms": None,
                "latest_observed_ts_ms": None,
                "latest_type": "",
                "latest_subtype": "",
                "latest_symbols": "",
                "latest_timestamp_source": "",
                "latest_key": "",
            }
        payload = self._source_event_payload(latest.get("payload_json"))
        observed_ts_ms = self._int_field(latest.get("observed_ts_ms"))
        exchange_ts_ms = self._int_field(latest.get("exchange_ts_ms"))
        latest_age_ms = max(now_ms() - observed_ts_ms, 0) if observed_ts_ms else None
        return {
            "event_count": int(runtime_state.get("source_event_count") or 0),
            "latest_age_ms": latest_age_ms,
            "latest_exchange_ts_ms": exchange_ts_ms or None,
            "latest_observed_ts_ms": observed_ts_ms or None,
            "latest_type": str(latest.get("event_type") or ""),
            "latest_subtype": str(
                payload.get("event_subtype")
                or latest.get("event_subtype")
                or latest.get("event_type")
                or ""
            ),
            "latest_symbols": self._format_event_values(
                payload.get("coins") or latest.get("event_symbols")
            ),
            "latest_timestamp_source": str(payload.get("timestamp_source") or "exchange"),
            "latest_key": str(latest.get("idempotency_key") or ""),
        }

    def _source_dex_context(self) -> dict[str, Any]:
        configured_scope = self.config.source_dex_scope.value
        context: dict[str, Any] = {
            "configured_scope": configured_scope,
            "observed_scope": "not_observed",
            "positions_scope": "unknown",
            "account_value_basis": "unknown",
            "fidelity": "not_observed",
            "active_non_default_dexes": [],
            "configured_perp_dexes": sorted(
                {
                    market_dex(symbol)
                    for symbol in self.config.risk.allowed_symbols
                    if market_dex(symbol)
                }
            ),
            "reduced_fidelity": False,
        }
        rows = self.store.recent_source_events(
            source_wallet=self.config.source_wallet,
            limit=100,
        )
        for row in rows:
            payload = self._source_event_payload(row.get("payload_json"))
            aggregate = payload.get("unified_aggregate")
            if not isinstance(aggregate, dict):
                continue
            active = aggregate.get("active_non_default_dexes")
            active = [str(item) for item in active] if isinstance(active, list) else []
            fidelity = str(aggregate.get("fidelity") or "unknown")
            return {
                "configured_scope": configured_scope,
                "observed_scope": str(aggregate.get("source_dex_scope") or "strict"),
                "positions_scope": str(aggregate.get("positions_scope") or "default_perp_dex"),
                "account_value_basis": str(
                    aggregate.get("account_value_basis") or "total_unified_spot_usdc"
                ),
                "fidelity": fidelity,
                "active_non_default_dexes": active,
                "configured_perp_dexes": sorted(
                    {
                        market_dex(symbol)
                        for symbol in self.config.risk.allowed_symbols
                        if market_dex(symbol)
                    }
                ),
                "reduced_fidelity": fidelity == "reduced_non_default_positions_excluded",
            }
        return context

    def _runtime_state(self, *, include_recent: bool) -> dict[str, Any]:
        if include_recent:
            state = self.store.rebuild_runtime_state(
                self.config.mode,
                source_wallet=self.config.source_wallet,
            )
            state["latest_source_events"] = []
            latest_safe_mode = state.get("latest_safe_mode")
            if isinstance(latest_safe_mode, dict):
                state["latest_safe_mode"] = self._select_fields(
                    latest_safe_mode,
                    ("seq", "transition_id", "enabled", "reason", "detail", "created_ms"),
                )
            return state
        latest_safe_mode = self.store.latest_safe_mode()
        return {
            "source_event_count": self.store.count_source_events(self.config.source_wallet),
            "pending_source_reaction_count": self.store.unfinished_source_reaction_count(
                source_wallet=self.config.source_wallet
            ),
            "desired_state_count": self.store.desired_state_count(self.config.mode),
            "pending_intents": [],
            "latest_safe_mode": (
                self._select_fields(
                    latest_safe_mode,
                    ("seq", "transition_id", "enabled", "reason", "detail", "created_ms"),
                )
                if latest_safe_mode is not None
                else None
            ),
            "latest_source_events": [],
            "latest_source_event": self.store.latest_source_event(self.config.source_wallet),
        }

    def _recent_source_event_rows(self, limit: int) -> list[dict[str, Any]]:
        rows = self.store.recent_source_events(
            source_wallet=self.config.source_wallet,
            limit=limit,
        )
        enriched: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            payload = self._source_event_payload(item.get("payload_json"))
            item["event_subtype"] = str(
                payload.get("event_subtype") or item.get("event_type") or ""
            )
            item["event_symbols"] = self._format_event_values(payload.get("coins"))
            item["event_statuses"] = self._format_event_values(payload.get("statuses"))
            item["event_dexs"] = self._format_event_values(payload.get("dexs"))
            item["event_leverage"] = self._format_event_values(payload.get("leverage"))
            item["event_available_to_trade"] = self._format_event_values(
                payload.get("available_to_trade")
            )
            item["event_count"] = payload.get("event_count", "")
            item["timestamp_source"] = str(payload.get("timestamp_source") or "exchange")
            item.pop("payload_json", None)
            enriched.append(item)
        return enriched

    @staticmethod
    def _source_event_payload(payload_json: Any) -> dict[str, Any]:
        if not isinstance(payload_json, str):
            return {}
        try:
            event = json.loads(payload_json)
        except json.JSONDecodeError:
            return {}
        payload = event.get("payload") if isinstance(event, dict) else None
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _format_event_values(values: Any) -> str:
        if isinstance(values, list):
            return ", ".join(str(value) for value in values)
        if values is None or values == "":
            return ""
        return str(values)

    @staticmethod
    def _int_field(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def readiness(self) -> dict[str, Any]:
        return readiness_snapshot(self.dashboard(include_recent=False))

    def metrics_text(self) -> str:
        return prometheus_metrics(self.dashboard(include_recent=False))

    def _exchange_lease_diagnostics(self) -> dict[str, Any]:
        lease = self.store.runtime_lease(self._runtime_lease_name("run_once"))
        file_lock_path = str(self._runtime_file_lock_path())
        signer_file_lock = self._runtime_signer_file_lock_path()
        signer_file_lock_path = str(signer_file_lock) if signer_file_lock is not None else ""
        if lease is None:
            return {
                "lease": None,
                "status": "clear",
                "ms_remaining": 0,
                "file_lock_path": file_lock_path,
                "signer_file_lock_path": signer_file_lock_path,
            }
        remaining = int(lease["expires_ms"]) - now_ms()
        if remaining <= 0:
            return {
                "lease": lease,
                "status": "stale",
                "ms_remaining": 0,
                "file_lock_path": file_lock_path,
                "signer_file_lock_path": signer_file_lock_path,
            }
        return {
            "lease": lease,
            "status": "active",
            "ms_remaining": remaining,
            "file_lock_path": file_lock_path,
            "signer_file_lock_path": signer_file_lock_path,
        }

    def security_audit(self) -> dict[str, Any]:
        configured_secret_occurrences = self.store.find_text_occurrences(
            [
                self.config.exchange.api_private_key,
                self.config.ops.gui_token,
            ]
        )
        sensitive_value_findings = self.store.sensitive_value_findings()
        return {
            "passed": not configured_secret_occurrences and not sensitive_value_findings,
            "configured_secret_occurrences": configured_secret_occurrences,
            "sensitive_value_findings": sensitive_value_findings,
        }

    def cached_security_audit(self) -> dict[str, Any]:
        observed = now_ms()
        ttl_ms = self.config.ops.dashboard_security_audit_ttl_ms
        with self._security_audit_lock:
            if self._security_audit_cache is not None:
                age_ms = observed - self._security_audit_cache_ms
                if age_ms <= ttl_ms:
                    return self._security_audit_with_cache_meta(
                        self._security_audit_cache,
                        cached=True,
                        age_ms=age_ms,
                    )
        audit = self.security_audit()
        cached_at = now_ms()
        with self._security_audit_lock:
            self._security_audit_cache = dict(audit)
            self._security_audit_cache_ms = cached_at
        return self._security_audit_with_cache_meta(audit, cached=False, age_ms=0)

    def invalidate_security_audit_cache(self) -> None:
        with self._security_audit_lock:
            self._security_audit_cache = None
            self._security_audit_cache_ms = 0

    def _security_audit_with_cache_meta(
        self,
        audit: dict[str, Any],
        *,
        cached: bool,
        age_ms: int,
    ) -> dict[str, Any]:
        return {
            **audit,
            "cached": cached,
            "cache_age_ms": max(age_ms, 0),
            "cache_ttl_ms": self.config.ops.dashboard_security_audit_ttl_ms,
        }


def infer_manual_intervention(
    expected_positions: dict[str, Position],
    actual_positions: dict[str, Position],
    expected_open_cloids: set[str],
    actual_open_orders: list[OpenOrder],
    service: CopyTraderService,
) -> bool:
    result = service.shield.manual_intervention(
        expected_positions=expected_positions,
        actual_positions=actual_positions,
        expected_open_cloids=expected_open_cloids,
        actual_open_orders=actual_open_orders,
    )
    return not result.ok
