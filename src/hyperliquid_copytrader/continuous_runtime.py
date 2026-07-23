from __future__ import annotations

import asyncio
import json
import sqlite3
from collections import deque
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any, Callable, Iterable, Mapping, Protocol

from .account_stream import AccountStream, FillRecord, source_subscription_specs
from .continuous_config import BoundContinuousPlan, BoundContinuousSlot
from .continuous_executor import ContinuousSignerLane, ExecutionAttempt
from .desired_engine import (
    ActionDecision,
    DesiredPortfolio,
    NextAction,
    build_desired_portfolio,
    choose_next_action,
)
from .market_catalog import CatalogRevision, MarketReadiness
from .market_stream import (
    ExecutableIoc,
    MarketSnapshot,
    MarketStream,
    MarketStreamError,
    MarketSubscriptionChange,
    executable_ioc,
)
from .markets import canonical_market_symbol, market_dex
from .models import Position
from .order_preflight import (
    HyperliquidPerpRules,
    preflight_hyperliquid_perp_order,
)
from .precision import aggressive_ioc_price, quantize_size
from .runtime_lock import (
    AccountRuntimeFileLock,
    account_runtime_lock_path,
    default_runtime_lock_dir,
    signer_runtime_lock_path,
)
from .ws_actions import PostOutcome, WsPostMux


TRANSIENT_REJECTION_RETRY_MS = 5_000


class RuntimeState(str, Enum):
    RUNNING = "RUNNING"
    PAUSE_ENTRIES = "PAUSE_ENTRIES"
    RECOVERING = "RECOVERING"
    OPERATOR_STOP = "OPERATOR_STOP"


@dataclass(frozen=True, slots=True)
class FollowerTruth:
    positions: Mapping[str, Position]
    equity: Decimal
    observed_ms: int
    dex_observed_ms: Mapping[str, int] = field(default_factory=dict)
    available_collateral: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Dispatch:
    slot: str | None
    state: RuntimeState | None
    reason: str
    market_change: MarketSubscriptionChange = MarketSubscriptionChange((), ())
    attempt: ExecutionAttempt | None = None
    action: NextAction | None = None
    source_frame_accepted: bool = False


class FollowerInfoHook(Protocol):
    async def __call__(
        self, *, slot: BoundContinuousSlot, mux: WsPostMux, epoch: int, now_ms: int
    ) -> FollowerTruth: ...


class GapRepairHook(Protocol):
    async def __call__(
        self,
        *,
        slot: BoundContinuousSlot,
        before: Mapping[str, Decimal],
        after: Mapping[str, Decimal],
    ) -> tuple[FillRecord, ...]: ...


@dataclass(frozen=True, slots=True)
class _Trigger:
    price: Decimal
    time_ms: int
    accepted_ms: int


@dataclass(frozen=True, slots=True)
class _RejectedTarget:
    desired_id: str
    follower_fingerprint: tuple[str, str, str, str]
    market_identity_sha256: str
    reason: str


@dataclass(frozen=True, slots=True)
class _LiquidityRetry:
    desired_id: str
    source_revision: int
    not_before_ms: int
    after_observed_ms: int
    liquidity_fingerprint: tuple[str, ...]
    retry_count: int
    require_market_observation: bool
    observe_context: bool
    reason: str


@dataclass(slots=True)
class _Slot:
    bound: BoundContinuousSlot
    account: AccountStream
    lane: ContinuousSignerLane
    identity: Mapping[str, str]
    state: RuntimeState = RuntimeState.RECOVERING
    reason: str = "startup reconciliation required"
    source_ready: bool = False
    ever_baselined: bool = False
    expected_source: dict[str, Decimal] | None = None
    unattributed: dict[str, Decimal] = field(default_factory=dict)
    attributable: dict[str, Decimal] = field(default_factory=dict)
    pending_catalog_markets: set[str] = field(default_factory=set)
    applied_fills: dict[tuple[str, str, str], None] = field(default_factory=dict)
    triggers: dict[str, _Trigger] = field(default_factory=dict)
    source_revisions: dict[str, int] = field(default_factory=dict)
    follower: FollowerTruth | None = None
    restored_follower_positions: dict[str, Decimal] | None = None
    initial_follower_nonflat: bool = False
    operator_paused: bool = False
    fail_close_reason: str | None = None
    startup_blocker: str | None = None
    action_times: deque[int] = field(default_factory=deque)
    liquidity_retries: dict[str, _LiquidityRetry] = field(default_factory=dict)
    rejected_targets: dict[str, _RejectedTarget] = field(default_factory=dict)
    truth_revision: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    drive_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True, slots=True)
class _PreparedIoc:
    action: NextAction
    snapshot: MarketSnapshot
    executable: ExecutableIoc
    required_epoch: int
    gate_clock: int
    evidence: Mapping[str, Any]
    retry_count: int
    source_revision: int


class _StateStore:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS continuous_state ("
            "slot TEXT PRIMARY KEY, version INTEGER NOT NULL, payload TEXT NOT NULL)"
        )

    def load(self, slot: str) -> Mapping[str, Any] | None:
        row = self.db.execute(
            "SELECT version,payload FROM continuous_state WHERE slot=?", (slot,)
        ).fetchone()
        if row is None:
            return None
        if int(row[0]) != 1:
            raise ValueError(f"unsupported continuous state version for {slot}")
        value = json.loads(str(row[1]))
        if not isinstance(value, Mapping):
            raise ValueError(f"malformed continuous state for {slot}")
        return value

    def save(self, slot: _Slot) -> None:
        follower = slot.follower
        payload = {
            "identity": dict(slot.identity),
            "state": slot.state.value,
            # Persist only the last source state that completed attribution.
            # A reconnect snapshot is not trusted merely because the parser
            # accepted it; gap repair may still reject that snapshot.
            "source": _wire_decimals(slot.expected_source or {}),
            "unattributed": _wire_decimals(slot.unattributed),
            "attributable": _wire_decimals(slot.attributable),
            "pending_catalog_markets": sorted(slot.pending_catalog_markets),
            "fills": [list(item) for item in tuple(slot.applied_fills)[-512:]],
            "triggers": {
                market: {
                    "price": str(value.price),
                    "time_ms": value.time_ms,
                    "accepted_ms": value.accepted_ms,
                }
                for market, value in sorted(slot.triggers.items())
            },
            "source_revisions": dict(sorted(slot.source_revisions.items())),
            "initial_follower_nonflat": slot.initial_follower_nonflat,
            "fail_close_reason": slot.fail_close_reason,
            "follower": None
            if follower is None
            else {
                "positions": _wire_decimals(_sizes(follower.positions)),
                "equity": str(follower.equity),
                "observed_ms": follower.observed_ms,
            },
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "INSERT INTO continuous_state(slot,version,payload) VALUES(?,1,?) "
                "ON CONFLICT(slot) DO UPDATE SET version=1,payload=excluded.payload",
                (slot.bound.config.slot, encoded),
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def upgrade_identity(
        self,
        slot: str,
        payload: Mapping[str, Any],
        identity: Mapping[str, Any],
    ) -> None:
        """Replace only checkpoint identity after a proven additive schema migration."""

        upgraded = dict(payload)
        upgraded["identity"] = dict(identity)
        encoded = json.dumps(upgraded, sort_keys=True, separators=(",", ":"))
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE continuous_state SET payload=? WHERE slot=? AND version=1",
                (encoded, slot),
            )
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self.db.close()


class ContinuousRuntime:
    """One shared leader stream, one active-market reducer and one WS action mux."""

    def __init__(
        self,
        *,
        plan: BoundContinuousPlan,
        catalog: CatalogRevision,
        lanes: Mapping[str, ContinuousSignerLane],
        mux: WsPostMux,
        follower_info: FollowerInfoHook,
        preflight_vaults: Mapping[str, str | None],
        preflight_source_modes: Mapping[str, str],
        state_path: Path | str,
        preflight_follower_dexes: Mapping[str, Iterable[str]] | None = None,
        execution_enabled: bool = False,
        lock_dir: Path | str | None = None,
        gap_repair: GapRepairHook | None = None,
        max_tracking_bps: Decimal = Decimal("50"),
        reduction_slippage_bps: Decimal = Decimal("100"),
        emergency_slippage_bps: Decimal = Decimal("300"),
        max_source_fill_age_ms: int = 5_000,
        source_age_ms: int = 30_000,
        market_age_ms: int = 45_000,
        follower_age_ms: int = 90_000,
        monotonic_ms: Callable[[], int] | None = None,
        action_clock_ms: Callable[[], int] | None = None,
    ) -> None:
        ids = {item.config.slot for item in plan.slots}
        if (
            not 1 <= len(ids) <= 10
            or ids != set(lanes)
            or ids != set(preflight_vaults)
            or ids != set(preflight_source_modes)
        ):
            raise ValueError("plan, signer lanes and preflight proofs must match 1-10 slots")
        if min(max_source_fill_age_ms, source_age_ms, market_age_ms, follower_age_ms) <= 0:
            raise ValueError("freshness limits must be positive")
        for value in (
            max_tracking_bps,
            reduction_slippage_bps,
            emergency_slippage_bps,
        ):
            if not value.is_finite() or not Decimal("0") <= value < Decimal("10000"):
                raise ValueError("slippage limits must be finite and below 10000 bps")
        self.plan, self.mux, self.follower_info, self.gap_repair = (
            plan,
            mux,
            follower_info,
            gap_repair,
        )
        self.execution_enabled = execution_enabled
        self.max_tracking_bps = max_tracking_bps
        self.reduction_slippage_bps = reduction_slippage_bps
        self.emergency_slippage_bps = emergency_slippage_bps
        self.max_source_fill_age_ms = max_source_fill_age_ms
        self.source_age_ms, self.market_age_ms, self.follower_age_ms = (
            source_age_ms,
            market_age_ms,
            follower_age_ms,
        )
        self._clock = monotonic_ms or (lambda: monotonic_ns() // 1_000_000)
        self._action_clock = action_clock_ms or (lambda: time_ns() // 1_000_000)
        self._store = _StateStore(Path(state_path).resolve())
        self._catalog = catalog
        self._market = MarketStream(catalog=catalog)
        self._market_lock, self._exposure_lock = asyncio.Lock(), asyncio.Lock()
        self._catalog_refresh_request = asyncio.Event()
        self._reservations: dict[str, dict[str, Decimal]] = {}
        self._slots: dict[str, _Slot] = {}
        self._source_index: dict[str, str] = {}
        self._source_open_epoch: int | None = None
        self._prewarm_markets: set[str] = set()
        self._allowed_markets: dict[str, tuple[str, ...]] = {}
        self._jit_order_audited: dict[str, set[str]] = {}
        self._locks: list[AccountRuntimeFileLock] = []
        audited_dexes = preflight_follower_dexes or {}
        unknown_audit_slots = set(audited_dexes) - ids
        if unknown_audit_slots:
            raise ValueError("preflight follower DEX proofs contain unknown slots")
        for bound in plan.slots:
            lane, slot_id = lanes[bound.config.slot], bound.config.slot
            allowed_markets = self._effective_allowed_markets(bound, catalog)
            self._allowed_markets[slot_id] = allowed_markets
            proven_dexes = {str(dex) for dex in audited_dexes.get(slot_id, ())}
            if not proven_dexes.issubset(set(catalog.wire_dexes)):
                raise ValueError(f"slot {slot_id} preflight DEX proof is outside the catalog")
            self._jit_order_audited[slot_id] = proven_dexes | {
                market_dex(market) for market in bound.config.allowed_markets
            }
            source_mode = str(preflight_source_modes[slot_id]).strip().lower()
            if source_mode not in {"standard", "unified"}:
                raise ValueError(f"slot {slot_id} source mode proof is invalid")
            if bound.expected_account_mode.strip().lower() != "unified":
                raise ValueError(f"slot {slot_id} follower must be preflight-proven Unified")
            source_equity_basis = (
                "standard_sum_relevant_dex_account_value"
                if source_mode == "standard"
                else "unified_spot_token0_usdc_total"
            )
            expected_vault = (
                bound.config.follower_account_address
                if (bound.config.follower_account_address != bound.global_account_address)
                else None
            )
            proven = preflight_vaults[slot_id]
            proven = None if proven is None else proven.lower()
            lane_vault = None if lane.vault_address is None else lane.vault_address.lower()
            if lane.follower_account.lower() != bound.config.follower_account_address:
                raise ValueError(f"slot {slot_id} signer lane follower mismatch")
            if lane.api_wallet_address.lower() != bound.api_wallet_address:
                raise ValueError(f"slot {slot_id} signer lane wallet mismatch")
            if proven != expected_vault or lane_vault != expected_vault:
                raise ValueError(f"slot {slot_id} signing-vault proof mismatch")
            identity = {
                "plan_sha256": plan.plan.sha256,
                "runtime_id": plan.plan.runtime_id,
                "network": plan.plan.network,
                "source": bound.config.source_address,
                "source_account_mode": source_mode,
                "source_equity_basis": source_equity_basis,
                "follower": bound.config.follower_account_address,
                "api_wallet": bound.api_wallet_address,
                "market_policy": (
                    "all_active_token0_perps"
                    if bound.dynamic_market_eligibility
                    else "explicit_allowlist"
                ),
                "catalog_markets_sha256": (
                    "dynamic"
                    if bound.dynamic_market_eligibility
                    else _catalog_markets_sha256(catalog, bound.config.allowed_markets)
                ),
                "denied_markets_sha256": sha256(
                    json.dumps(
                        sorted(bound.denied_markets),
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            }
            slot = _Slot(
                bound,
                AccountStream(
                    source=bound.config.source_address,
                    follower=bound.config.follower_account_address,
                    source_account_mode=source_mode,
                    source_markets=allowed_markets or None,
                    follower_markets=allowed_markets or None,
                    accept_source_markets_outside_scope=bound.dynamic_market_eligibility,
                ),
                lane,
                identity,
            )
            catalog_issues = []
            for market in allowed_markets:
                spec = catalog.market(market)
                if spec is None:
                    catalog_issues.append(f"{market} is absent from the pinned catalog")
                elif spec.is_delisted or spec.readiness in {
                    MarketReadiness.HALTED,
                    MarketReadiness.DELISTED,
                    MarketReadiness.UNTRUSTED,
                }:
                    catalog_issues.append(f"{market} is not runtime-ready ({spec.readiness.value})")
                else:
                    self._prewarm_markets.add(market)
            if catalog_issues:
                slot.startup_blocker = "allowed market validation failed: " + "; ".join(
                    catalog_issues
                )
                self._block(slot, slot.startup_blocker)
            stored_payload = self._store.load(slot_id)
            identity_upgraded = self._restore(slot, stored_payload)
            if identity_upgraded:
                assert stored_payload is not None
                self._store.upgrade_identity(slot_id, stored_payload, slot.identity)
            slot.action_times.extend(
                lane.journal.recent_send_attempts(
                    follower_account=lane.follower_account,
                    api_wallet=lane.api_wallet_address,
                    after_ms=max(0, self._action_clock() - 60_000),
                )
            )
            self._slots[slot_id] = slot
            if bound.config.source_address in self._source_index:
                raise ValueError("leader accounts must be unique on the shared source stream")
            self._source_index[bound.config.source_address] = slot_id
        source_subscription_count = sum(
            len(
                source_subscription_specs(
                    slot.bound.config.source_address,
                    account_mode=slot.account.source_account_mode,
                )
            )
            for slot in self._slots.values()
        )
        market_subscription_count = 3 * len(self._prewarm_markets)
        if source_subscription_count + market_subscription_count > 1_000:
            raise ValueError("continuous runtime exceeds the 1000 WebSocket subscription limit")
        self._market.set_active_markets(self._prewarm_markets)
        base = Path(lock_dir or default_runtime_lock_dir()).resolve()
        paths = {
            account_runtime_lock_path(
                base,
                network=plan.plan.network,
                action_account=s.bound.config.follower_account_address,
            )
            for s in self._slots.values()
        } | {
            signer_runtime_lock_path(
                base, network=plan.plan.network, signer_address=s.bound.api_wallet_address
            )
            for s in self._slots.values()
        }
        try:
            for path in sorted(paths, key=lambda item: str(item).casefold()):
                lock = AccountRuntimeFileLock(path)
                lock.acquire()
                self._locks.append(lock)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        for lock in reversed(getattr(self, "_locks", [])):
            lock.release()
        if hasattr(self, "_locks"):
            self._locks.clear()
        store = getattr(self, "_store", None)
        if store is not None:
            store.close()
            self._store = None  # type: ignore[assignment]

    def __enter__(self) -> ContinuousRuntime:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @property
    def source_subscriptions(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            spec
            for slot in self._slots.values()
            for spec in source_subscription_specs(
                slot.bound.config.source_address,
                account_mode=slot.account.source_account_mode,
            )
        )

    @property
    def market_subscriptions(self) -> tuple[dict[str, str], ...]:
        return self._market.subscription_specs

    @property
    def catalog(self) -> CatalogRevision:
        return self._catalog

    @property
    def catalog_retention_markets(self) -> frozenset[str]:
        markets: set[str] = set()
        for slot in self._slots.values():
            markets.update(slot.attributable)
            if slot.follower is not None:
                markets.update(slot.follower.positions)
            markets.update(slot.lane.unresolved_signed_remaining())
        return frozenset(markets)

    @property
    def catalog_refresh_requested(self) -> bool:
        return self._catalog_refresh_request.is_set()

    async def wait_for_catalog_refresh_request(self) -> None:
        await self._catalog_refresh_request.wait()

    def clear_catalog_refresh_request(self) -> None:
        self._catalog_refresh_request.clear()

    async def apply_catalog(self, catalog: CatalogRevision) -> MarketSubscriptionChange:
        """Adopt a periodic catalog revision without triggering follower-wide audits."""

        if catalog.network != self._catalog.network or catalog.sequence <= self._catalog.sequence:
            raise ValueError("catalog refresh network/sequence is invalid")
        allowed_by_slot = {
            slot_id: self._effective_allowed_markets(slot.bound, catalog)
            for slot_id, slot in self._slots.items()
        }
        prewarm = {market for allowed in allowed_by_slot.values() for market in allowed}
        source_subscription_count = sum(
            len(
                source_subscription_specs(
                    slot.bound.config.source_address,
                    account_mode=slot.account.source_account_mode,
                )
            )
            for slot in self._slots.values()
        )
        if source_subscription_count + 3 * len(prewarm) > 1_000:
            raise ValueError("refreshed catalog exceeds the 1000 WebSocket subscription limit")
        before = set(self._market.active_markets)
        changed_slot_ids: set[str] = set()
        async with self._market_lock:
            self._market.replace_catalog(catalog)
            self._catalog = catalog
            replace_catalog = getattr(self.follower_info, "replace_catalog", None)
            if callable(replace_catalog):
                replace_catalog(catalog)
            self._prewarm_markets = prewarm
            for slot_id, slot in self._slots.items():
                allowed = allowed_by_slot[slot_id]
                self._allowed_markets[slot_id] = allowed
                slot.account.set_market_scope(
                    source_markets=allowed or None,
                    follower_markets=allowed or None,
                )
                for market in tuple(slot.pending_catalog_markets):
                    spec = catalog.market(market)
                    if spec is None:
                        continue
                    slot.pending_catalog_markets.discard(market)
                    changed_slot_ids.add(slot_id)
                    if market in allowed:
                        continue
                    size = slot.attributable.pop(market, Decimal(0))
                    _put(
                        slot.unattributed, market, slot.unattributed.get(market, Decimal(0)) + size
                    )
                    slot.triggers.pop(market, None)
            self._market.set_active_markets(self._prewarm_markets)
        for slot_id in sorted(changed_slot_ids):
            self._store.save(self._slots[slot_id])
        await self._activate()
        return self._change(before)

    @property
    def slot_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._slots))

    def source_frame_status(self, slot_id: str, *, received_ms: int) -> tuple[bool, bool]:
        slot = self._slots[slot_id]
        accepted = slot.account.source.last_connection_activity_ms == received_ms
        return accepted, slot.source_ready

    def source_slot_id(self, source_address: str) -> str | None:
        return self._source_index.get(str(source_address).strip().lower())

    def status(self, slot_id: str) -> tuple[RuntimeState, str]:
        slot = self._slots[slot_id]
        return slot.state, slot.reason

    def operational_status(self, slot_id: str, *, now_ms: int) -> dict[str, Any]:
        """Return canonical account state without driving or mutating execution."""

        slot = self._slots[slot_id]
        follower = slot.follower
        unresolved = slot.lane.unresolved_signed_remaining()
        actual = {} if follower is None else _sizes(follower.positions)
        projected = dict(actual)
        for market, size in unresolved.items():
            _put(projected, market, projected.get(market, Decimal("0")) + size)
        latest_market = ""
        latest_event_ms = 0
        if slot.triggers:
            latest_market, trigger = max(
                slot.triggers.items(), key=lambda item: (item[1].time_ms, item[0])
            )
            latest_event_ms = trigger.time_ms
        desired_payload: dict[str, str] | None = None
        try:
            source = slot.account.source
            source_equity = (
                source.perp_equity_total
                if slot.account.source_account_mode == "standard"
                else None
                if source.collateral is None
                else source.collateral.total
            )
            if follower is not None and source_equity is not None and source_equity > 0:
                attributable = self._attributable_positions(slot)
                mids: dict[str, Decimal] = {}
                for market in attributable:
                    snapshot = self._market.fresh_snapshot(
                        market, now_ms=now_ms, max_age_ms=self.market_age_ms
                    )
                    if snapshot is None:
                        raise ValueError("market snapshot unavailable")
                    mids[market] = snapshot.mark_px
                desired = build_desired_portfolio(
                    self._effective_config(slot),
                    source_positions=attributable,
                    source_equity=source_equity,
                    follower_equity=follower.equity,
                    mids=mids,
                )
                desired_payload = _wire_decimals(_sizes(desired.positions))
        except (TypeError, ValueError):
            desired_payload = None
        current_market = latest_market or next(
            iter(sorted(set(projected) | set(slot.attributable))), ""
        )
        return {
            "slot": slot_id,
            "enabled": slot.bound.config.enabled,
            "source_address": slot.bound.config.source_address,
            "follower_account_address": slot.bound.config.follower_account_address,
            "credential_profile_id": slot.bound.config.credential_profile_id,
            "state": slot.state.value,
            "reason": slot.reason,
            "latest_leader_event_ms": latest_event_ms,
            "latest_market": latest_market,
            "latest_leader_market": latest_market,
            "market": current_market,
            "source_last_activity_ms": slot.account.source.last_connection_activity_ms,
            "last_successful_sync_ms": 0 if follower is None else follower.observed_ms,
            "follower_equity_usd": None if follower is None else str(follower.equity),
            "spot_spendable_usdc": (
                None
                if follower is None or follower.available_collateral is None
                else str(follower.available_collateral)
            ),
            "source_positions": _wire_decimals(slot.attributable),
            "catalog_pending_markets": sorted(slot.pending_catalog_markets),
            "desired_positions": desired_payload,
            "desired_position": (
                None
                if desired_payload is None or not current_market
                else desired_payload.get(current_market)
            ),
            "actual_positions": _wire_decimals(actual),
            "actual_position": None
            if not current_market
            else _wire_decimals(actual).get(current_market),
            "projected_positions": _wire_decimals(projected),
            "unresolved_actions": _wire_decimals(unresolved),
            "data_stale": not self.source_is_ready(slot_id, now_ms=now_ms)
            or follower is None
            or not 0 <= now_ms - follower.observed_ms <= self.follower_age_ms,
        }

    @property
    def fail_close_slots(self) -> tuple[str, ...]:
        return tuple(
            sorted(slot_id for slot_id, slot in self._slots.items() if slot.fail_close_reason)
        )

    def source_is_ready(self, slot_id: str, *, now_ms: int) -> bool:
        slot = self._slots[slot_id]
        return slot.source_ready and slot.account.source.is_fresh(
            now_ms=now_ms, max_age_ms=self.source_age_ms
        )

    def request_fail_close(self, slot_ids: Iterable[str], *, reason: str) -> None:
        if not reason.strip():
            raise ValueError("fail-close reason cannot be empty")
        for slot_id in slot_ids:
            slot = self._slots[slot_id]
            if slot.state is RuntimeState.OPERATOR_STOP:
                continue
            slot.fail_close_reason = slot.fail_close_reason or reason.strip()
            slot.operator_paused = True
            slot.state = RuntimeState.PAUSE_ENTRIES
            slot.reason = f"fail-close: {slot.fail_close_reason}"
            self._store.save(slot)

    def operator_rearm(self, slot_ids: Iterable[str]) -> None:
        """Re-arm a completed flat closeout without reopening the old target.

        Crash recovery without a fail-close latch keeps durable attribution.  An
        explicit re-arm is different: the prior run intentionally flattened the
        follower, so its old desired portfolio is no longer trading authority.
        The current source checkpoint becomes observation-only baseline and only
        later accepted fills may create new follower exposure.
        """

        if not self.execution_enabled:
            raise RuntimeError("operator re-arm requires execution to be armed")
        for slot_id in slot_ids:
            slot = self._slots[slot_id]
            if slot.follower is None or slot.follower.positions:
                raise RuntimeError(f"operator re-arm requires authoritative flat {slot_id}")
            if slot.lane.unresolved_signed_remaining():
                raise RuntimeError(f"operator re-arm requires terminal actions for {slot_id}")
            slot.unattributed = dict(slot.expected_source or _source_sizes(slot))
            slot.attributable.clear()
            slot.triggers.clear()
            slot.pending_catalog_markets.clear()
            slot.rejected_targets.clear()
            slot.liquidity_retries.clear()
            slot.initial_follower_nonflat = False
            slot.fail_close_reason = None
            slot.operator_paused = False
            slot.state = RuntimeState.RECOVERING
            slot.reason = "operator re-arm awaiting live reconciliation"
            self._store.save(slot)

    def follower_is_flat(
        self,
        slot_id: str,
        *,
        observed_at_least_ms: int | None = None,
    ) -> bool:
        follower = self._slots[slot_id].follower
        return bool(
            follower is not None
            and not follower.positions
            and (observed_at_least_ms is None or follower.observed_ms >= observed_at_least_ms)
        )

    async def drive_fail_close(self, slot_id: str, *, now_ms: int) -> Dispatch:
        slot, before = self._slots[slot_id], set(self._market.active_markets)
        attempt, action, reason = await self._drive_serialized(
            slot, now_ms, None, force_flatten=True
        )
        return Dispatch(slot_id, slot.state, reason, self._change(before), attempt, action)

    def set_operator_state(self, slot_id: str, state: RuntimeState, *, now_ms: int) -> None:
        slot = self._slots[slot_id]
        if state is RuntimeState.OPERATOR_STOP:
            slot.state, slot.reason = state, "operator stop"
        elif state is RuntimeState.PAUSE_ENTRIES:
            slot.operator_paused, slot.state, slot.reason = True, state, "operator pause"
        elif state is RuntimeState.RUNNING and slot.state is not RuntimeState.OPERATOR_STOP:
            slot.operator_paused = False
            self._ready(slot, now_ms)
        else:
            raise ValueError("operator may only stop, pause entries, or request resume")
        self._store.save(slot)

    def begin_source_connection(self, *, received_ms: int = 0) -> int:
        epochs = set()
        for slot in self._slots.values():
            if slot.ever_baselined and slot.account.source.baseline_complete:
                slot.expected_source = _source_sizes(slot)
            slot.source_ready = False
            self._block(slot, "source connection is rebuilding")
            epochs.add(slot.account.begin_connection(received_ms=received_ms))
        if len(epochs) != 1:
            raise RuntimeError("source reducer epochs diverged")
        epoch = epochs.pop()
        self._source_open_epoch = epoch
        return epoch

    def begin_market_connection(self, *, received_ms: int = 0) -> int:
        epoch = self._market.begin_connection(received_ms=received_ms)
        for slot in self._slots.values():
            if self._needed(slot):
                self._block(slot, "market connection is rebuilding")
        return epoch

    def note_source_activity(self, *, epoch: int, received_ms: int) -> None:
        for slot in self._slots.values():
            slot.account.note_connection_activity(epoch=epoch, received_ms=received_ms)

    def note_market_activity(self, *, epoch: int, received_ms: int) -> None:
        self._market.note_connection_activity(epoch=epoch, received_ms=received_ms)

    def connection_gap(self, kind: str, *, epoch: int, reason: str) -> None:
        if kind == "source":
            if epoch == self._source_open_epoch:
                self._source_open_epoch = None
            for slot in self._slots.values():
                if epoch == slot.account.connection_epoch:
                    slot.source_ready = False
                    self._block(slot, f"source gap: {reason}")
        elif kind == "market" and epoch == self._market.connection_epoch:
            for slot in self._slots.values():
                if self._needed(slot):
                    self._block(slot, f"market gap: {reason}")
        else:
            raise ValueError("connection kind must be source or market")

    async def apply_source(
        self,
        message: Mapping[str, Any],
        *,
        epoch: int,
        received_ms: int,
        received_mono_ns: int | None = None,
        drive: bool = True,
    ) -> Dispatch:
        if str(message.get("channel") or "") not in {
            "allDexsClearinghouseState",
            "spotState",
            "userFills",
            "userTwapSliceFills",
        }:
            return Dispatch(None, None, "ignored non-account source frame")
        data = message.get("data")
        user = (
            str(data.get("user") or data.get("userAddress") or "").lower()
            if isinstance(data, Mapping)
            else ""
        )
        slot_id = self._source_index.get(user)
        if slot_id is None:
            return Dispatch(None, None, "ignored unconfigured source frame")
        slot, before = self._slots[slot_id], set(self._market.active_markets)
        accepted = False
        async with slot.lock:
            if epoch != self._source_open_epoch:
                return Dispatch(
                    slot_id,
                    slot.state,
                    "source frame rejected for closed connection epoch",
                    source_frame_accepted=False,
                )
            try:
                update = slot.account.apply(message, epoch=epoch, received_ms=received_ms)
                accepted = True
                if slot.account.source.baseline_complete and not slot.source_ready:
                    await self._baseline(slot, now_ms=received_ms)
                elif update.new_fills and slot.source_ready:
                    fresh = tuple(
                        fill for fill in update.new_fills if fill.identity not in slot.applied_fills
                    )
                    self._attribute(slot, fresh, recovered_at_ms=received_ms)
                    slot.expected_source = _source_sizes(slot)
                    self._store.save(slot)  # durable before any signed action
                elif update.new_fills:
                    self._block(slot, "live source fill arrived before trusted baseline")
            except Exception as exc:
                self._block(slot, f"source frame failed: {type(exc).__name__}: {exc}")
                return Dispatch(slot_id, slot.state, slot.reason, source_frame_accepted=accepted)
            reason = (
                slot.reason
                if slot.state is RuntimeState.RECOVERING
                else "source frame accepted; drive pending"
            )
        await self._activate()
        if drive:
            attempt, action, reason = await self._drive_serialized(
                slot, received_ms, received_mono_ns
            )
        else:
            attempt, action = None, None
        return Dispatch(
            slot_id,
            slot.state,
            reason,
            self._change(before),
            attempt,
            action,
            source_frame_accepted=accepted,
        )

    async def apply_market(
        self,
        message: Mapping[str, Any],
        *,
        epoch: int,
        received_ms: int,
        drive: bool = True,
    ) -> tuple[Dispatch, ...]:
        data = message.get("data")
        try:
            market = canonical_market_symbol(
                data.get("coin") if isinstance(data, Mapping) else None
            )
        except (TypeError, ValueError):
            return (Dispatch(None, None, "ignored malformed market frame"),)
        affected = [slot for slot in self._slots.values() if market in self._needed(slot)]
        async with self._market_lock:
            try:
                snapshot = self._market.apply(message, epoch=epoch, received_ms=received_ms)
            except MarketStreamError as exc:
                for slot in affected:
                    self._block(slot, f"market {market} failed: {exc}")
                return tuple(Dispatch(s.bound.config.slot, s.state, s.reason) for s in affected)
        if snapshot is None:
            return tuple(
                Dispatch(s.bound.config.slot, s.state, f"{market} snapshot incomplete")
                for s in affected
            )
        if not drive:
            return tuple(
                Dispatch(s.bound.config.slot, s.state, "market frame accepted; drive pending")
                for s in affected
            )

        async def drive_one(slot: _Slot) -> Dispatch:
            attempt, action, reason = await self._drive_serialized(slot, received_ms, None)
            return Dispatch(
                slot.bound.config.slot, slot.state, reason, attempt=attempt, action=action
            )

        return tuple(await asyncio.gather(*(drive_one(slot) for slot in affected)))

    async def reconcile_follower(
        self, slot_id: str, *, now_ms: int, drive: bool = True
    ) -> Dispatch:
        slot, before = self._slots[slot_id], set(self._market.active_markets)
        async with slot.lock:
            if slot.state is RuntimeState.OPERATOR_STOP:
                return Dispatch(slot_id, slot.state, slot.reason)
            # A refresh which overlaps a submitted IOC is ambiguous: exchange
            # truth may describe either side of that fill.  Folding it before
            # the action response can count the same fill twice, while folding
            # it afterwards can overwrite the response.  Let the action finish;
            # the regular reconciliation loop will immediately try again.
            if self._reservations.get(slot_id):
                return Dispatch(
                    slot_id,
                    slot.state,
                    "follower refresh deferred during in-flight action",
                )
            try:
                epoch = self.mux.capture_epoch()
                slot.lane.recover_provably_unsent()
                recovery = slot.lane.journal.recovery_actions(
                    follower_account=slot.lane.follower_account,
                    api_wallet=slot.lane.api_wallet_address,
                )
                revision = slot.truth_revision
            except Exception as exc:
                self._block(slot, f"follower WS reconciliation failed: {type(exc).__name__}: {exc}")
                return Dispatch(slot_id, slot.state, slot.reason)

        try:
            for record in recovery:
                await slot.lane.resolve_by_cloid(
                    record.cloid,
                    mux=self.mux,
                    required_epoch=epoch,
                )
            if recovery:
                async with slot.lock:
                    if slot.truth_revision != revision:
                        return Dispatch(
                            slot_id,
                            slot.state,
                            "stale follower recovery result discarded",
                        )
                    slot.truth_revision += 1
                    revision = slot.truth_revision
            truth = await self.follower_info(
                slot=self._effective_bound(slot),
                mux=self.mux,
                epoch=epoch,
                now_ms=now_ms,
            )
        except Exception as exc:
            async with slot.lock:
                self._block(
                    slot,
                    f"follower WS reconciliation failed: {type(exc).__name__}: {exc}",
                )
                return Dispatch(slot_id, slot.state, slot.reason)

        async with slot.lock:
            try:
                current_epoch = self.mux.capture_epoch()
            except Exception as exc:
                self._block(
                    slot,
                    f"follower WS reconciliation failed: {type(exc).__name__}: {exc}",
                )
                return Dispatch(slot_id, slot.state, slot.reason)
            if current_epoch != epoch:
                self._block(slot, "follower WS reconciliation crossed a connection epoch")
                return Dispatch(slot_id, slot.state, slot.reason)
            if slot.truth_revision != revision:
                return Dispatch(
                    slot_id,
                    slot.state,
                    "stale follower truth discarded after concurrent action",
                )
            first_truth = slot.follower is None
            try:
                follower = self._truth(slot, truth, now_ms)
            except Exception as exc:
                self._block(
                    slot,
                    f"follower WS reconciliation failed: {type(exc).__name__}: {exc}",
                )
                return Dispatch(slot_id, slot.state, slot.reason)
            slot.follower = follower
            slot.truth_revision += 1
            if first_truth and slot.follower.positions and not slot.attributable:
                observed = _sizes(slot.follower.positions)
                if slot.restored_follower_positions != observed:
                    slot.initial_follower_nonflat = True
            elif slot.initial_follower_nonflat and not slot.follower.positions:
                slot.initial_follower_nonflat = False
            if first_truth:
                slot.restored_follower_positions = None
            self._store.save(slot)
            reason = (
                slot.reason
                if slot.state is RuntimeState.RECOVERING
                else "follower truth refreshed; drive pending"
            )
        await self._activate()
        if drive:
            attempt, action, reason = await self._drive_serialized(slot, now_ms, None)
        else:
            attempt, action = None, None
        return Dispatch(slot_id, slot.state, reason, self._change(before), attempt, action)

    async def drive_slot(
        self, slot_id: str, *, now_ms: int, received_mono_ns: int | None = None
    ) -> Dispatch:
        slot, before = self._slots[slot_id], set(self._market.active_markets)
        attempt, action, reason = await self._drive_serialized(slot, now_ms, received_mono_ns)
        return Dispatch(slot_id, slot.state, reason, self._change(before), attempt, action)

    async def _drive_serialized(
        self,
        slot: _Slot,
        now_ms: int,
        received_mono_ns: int | None,
        *,
        force_flatten: bool = False,
    ) -> tuple[ExecutionAttempt | None, NextAction | None, str]:
        # One action per slot remains unresolved at a time, while source and
        # follower truth continue to advance under the shorter state lock.
        queued_clock = self._clock()
        async with slot.drive_lock:
            now_ms += max(0, self._clock() - queued_clock)
            return await self._safe_drive(
                slot,
                now_ms,
                received_mono_ns,
                force_flatten=force_flatten,
            )

    async def _safe_drive(
        self,
        slot: _Slot,
        now_ms: int,
        received_mono_ns: int | None,
        *,
        force_flatten: bool = False,
    ) -> tuple[ExecutionAttempt | None, NextAction | None, str]:
        try:
            refresh_clock = self._clock()
            await self._ensure_needed_dex_truth(slot, now_ms)
            now_ms += max(0, self._clock() - refresh_clock)
            return await self._drive(slot, now_ms, received_mono_ns, force_flatten=force_flatten)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with slot.lock:
                self._block(slot, f"slot drive failed: {type(exc).__name__}: {exc}")
                self._store.save(slot)
                return None, None, slot.reason

    async def _baseline(self, slot: _Slot, *, now_ms: int) -> None:
        current = _source_sizes(slot)
        if not slot.ever_baselined:
            slot.unattributed, slot.attributable = dict(current), {}
            slot.ever_baselined, slot.source_ready = True, True
        elif _nz(slot.expected_source or {}) == _nz(current):
            slot.source_ready = True
        elif self.gap_repair is None:
            self._block(slot, "source changed across gap and no repair hook is configured")
            return
        else:
            fills = await self.gap_repair(
                slot=slot.bound, before=slot.expected_source or {}, after=current
            )
            working = dict(slot.expected_source or {})
            for fill in fills:
                if fill.start_position != working.get(fill.market, Decimal(0)):
                    raise ValueError("gap repair fill chain does not start at checkpoint truth")
                working[fill.market] = fill.start_position + fill.signed_size
            if _nz(working) != _nz(current):
                raise ValueError("gap repair fill chain does not reach reconnect truth")
            self._attribute(slot, fills, recovered_at_ms=now_ms)
            if not self._attribution_matches(slot):
                raise ValueError("gap repair did not converge to the source baseline")
            slot.source_ready = True
        slot.expected_source = current
        self._store.save(slot)

    async def _drive(
        self,
        slot: _Slot,
        now_ms: int,
        received_mono_ns: int | None,
        *,
        force_flatten: bool = False,
    ) -> tuple[ExecutionAttempt | None, NextAction | None, str]:
        async with slot.lock:
            planned = await self._plan_drive(slot, now_ms, force_flatten=force_flatten)
        if not isinstance(planned, _PreparedIoc):
            return planned

        action = planned.action
        slot_id = slot.bound.config.slot
        try:
            attempt = await slot.lane.execute_ioc(
                action=action,
                asset_id=planned.snapshot.asset_id,
                limit_px=planned.executable.limit_px,
                mux=self.mux,
                required_epoch=planned.required_epoch,
                received_mono_ns=received_mono_ns,
            )
        except asyncio.CancelledError:
            async with slot.lock:
                async with self._exposure_lock:
                    self._reservations.pop(slot_id, None)
                    self._block(slot, "IOC task was cancelled; reconciliation is required")
                    self._store.save(slot)
            raise
        except Exception as exc:
            async with slot.lock:
                async with self._exposure_lock:
                    self._reservations.pop(slot_id, None)
                    self._block(slot, f"IOC failed: {type(exc).__name__}: {exc}")
                    self._store.save(slot)
                return None, action, slot.reason

        async with slot.lock:
            async with self._exposure_lock:
                self._reservations.pop(slot_id, None)
                attempted_ms = getattr(attempt.record, "send_attempted_ms", None)
                if attempted_ms is None and attempt.result.outcome is not PostOutcome.NOT_SENT:
                    attempted_ms = self._action_clock()
                if attempted_ms is not None:
                    slot.action_times.append(int(attempted_ms))
                attempt = replace(attempt, execution_context=planned.evidence)
                self._fold(slot, action, attempt, planned)
                # A follower refresh that began while the exchange awaited may
                # describe pre-fill truth and must not overwrite this result.
                slot.truth_revision += 1
                self._store.save(slot)
        await self._activate()
        async with slot.lock:
            self._ready(slot, now_ms + max(0, self._clock() - planned.gate_clock))
        return attempt, action, f"IOC {attempt.result.outcome.value}"

    async def _plan_drive(
        self,
        slot: _Slot,
        now_ms: int,
        *,
        force_flatten: bool,
    ) -> _PreparedIoc | tuple[ExecutionAttempt | None, NextAction | None, str]:
        if force_flatten:
            if slot.state is RuntimeState.OPERATOR_STOP:
                return None, None, slot.reason
            follower = slot.follower
            if follower is None or not 0 <= now_ms - follower.observed_ms <= self.follower_age_ms:
                self._block(slot, "fail-close requires fresh follower truth")
                return None, None, slot.reason
            unresolved = slot.lane.unresolved_signed_remaining()
            if unresolved:
                self._block(slot, "fail-close is resolving an earlier action")
                return None, None, slot.reason
            action = self._forced_close_action(slot)
            if action is None:
                slot.state = RuntimeState.PAUSE_ENTRIES
                slot.reason = f"fail-close complete: {slot.fail_close_reason or 'requested'}"
                self._store.save(slot)
                return None, None, slot.reason
        else:
            self._ready(slot, now_ms)
            if slot.state in {RuntimeState.RECOVERING, RuntimeState.OPERATOR_STOP}:
                return None, None, slot.reason
            action = None
        follower, source = slot.follower, slot.account.source
        assert follower is not None
        unresolved = slot.lane.unresolved_signed_remaining()
        desired: DesiredPortfolio | None = None
        decision: ActionDecision | None = None
        source_equity: Decimal | None = None
        action = action or self._provable_reduction(slot, unresolved, now_ms)
        if action is None:
            if slot.account.source_account_mode == "standard":
                source_equity = source.perp_equity_total
            elif source.collateral is not None:
                source_equity = source.collateral.total
            else:
                self._block(slot, "Unified source collateral truth is unavailable")
                return None, None, slot.reason
            if not source_equity.is_finite() or source_equity <= 0:
                self._block(slot, "source sizing equity is not positive and finite")
                return None, None, slot.reason
            markets, mids, decimals = self._needed(slot), {}, {}
            for market in markets:
                snapshot = self._market.fresh_snapshot(
                    market, now_ms=now_ms, max_age_ms=self.market_age_ms
                )
                if snapshot is None:
                    self._block(slot, f"market {market} is not executable")
                    return None, None, slot.reason
                mids[market], decimals[market] = snapshot.mark_px, snapshot.sz_decimals
            desired = build_desired_portfolio(
                self._effective_config(slot),
                source_positions=self._attributable_positions(slot),
                source_equity=source_equity,
                follower_equity=follower.equity,
                mids=mids,
            )
            decision = choose_next_action(
                self._effective_config(slot),
                desired,
                follower_positions=follower.positions,
                unresolved_signed_remaining=unresolved,
                mids=mids,
                size_decimals=decimals,
                market_rules={market: self._order_rules(market) for market in markets},
            )
            if decision.action is None:
                if decision.blocker and "unresolved attempted action" in decision.blocker:
                    self._block(slot, decision.blocker)
                return None, None, decision.blocker or "desired and follower state agree"
            action = decision.action
        confirmed = follower.positions.get(action.market, Position(action.market, Decimal(0))).size
        projected_before = confirmed + unresolved.get(action.market, Decimal(0))
        increasing = abs(projected_before + action.signed_size) > abs(projected_before)
        execution_class = (
            "emergency_reduction"
            if force_flatten
            else "entry"
            if increasing
            else "normal_reduction"
        )
        rejected = slot.rejected_targets.get(action.market)
        if rejected is not None:
            unchanged_evidence = (
                rejected.desired_id == action.desired_id
                and rejected.follower_fingerprint
                == _rejection_follower_fingerprint(follower, action.market)
                and rejected.market_identity_sha256
                == _catalog_markets_sha256(self._catalog, (action.market,))
            )
            if unchanged_evidence:
                return (
                    None,
                    action,
                    f"unchanged target remains blocked after rejection: {rejected.reason}",
                )
            slot.rejected_targets.pop(action.market, None)
        if slot.state is RuntimeState.PAUSE_ENTRIES and increasing:
            return None, action, slot.reason
        self._prune_rate(slot)
        entry_limit = slot.bound.config.action_limit_per_minute
        reduction_limit = entry_limit + max(2, entry_limit // 2)
        if not force_flatten and len(slot.action_times) >= (
            entry_limit if increasing else reduction_limit
        ):
            slot.state, slot.reason = RuntimeState.PAUSE_ENTRIES, "slot action limit reached"
            return None, action, slot.reason
        gate_clock = self._clock()
        async with self._exposure_lock:
            gate_now_ms = now_ms + max(0, self._clock() - gate_clock)
            snapshot = self._market.fresh_snapshot(
                action.market, now_ms=gate_now_ms, max_age_ms=self.market_age_ms
            )
            if snapshot is None:
                self._block(slot, f"market {action.market} became stale before send")
                return None, action, slot.reason
            source_revision = slot.source_revisions.get(action.market, 0)
            retry = slot.liquidity_retries.get(action.market)
            if retry is not None and (
                retry.desired_id != action.desired_id
                or retry.source_revision != source_revision
            ):
                # A response from an older in-flight IOC must never throttle a
                # newer source revision, even when quantization yields the same
                # desired position.
                slot.liquidity_retries.pop(action.market, None)
                retry = None
            if (
                retry is not None
                and not retry.require_market_observation
                and gate_now_ms < retry.not_before_ms
            ):
                return None, action, f"waiting after {retry.reason}"
            retry_count = 0

            requested_size = action.size
            trigger = slot.triggers.get(action.market) if increasing else None
            if increasing and trigger is None:
                slot.state = RuntimeState.PAUSE_ENTRIES
                slot.reason = "entry has no accepted post-baseline source fill"
                return None, action, slot.reason
            if execution_class == "entry":
                assert trigger is not None
                hard_limit = aggressive_ioc_price(
                    trigger.price,
                    is_buy=action.side == "buy",
                    slippage_bps=self.max_tracking_bps,
                    sz_decimals=snapshot.sz_decimals,
                )
                bbo_buffer: Decimal | None = None
            else:
                hard_limit = None
                bbo_buffer = (
                    self.emergency_slippage_bps
                    if execution_class == "emergency_reduction"
                    else self.reduction_slippage_bps
                )
            executable = executable_ioc(
                snapshot,
                is_buy=action.side == "buy",
                requested_size=action.size,
                max_slippage_bps=bbo_buffer,
                hard_limit_px=hard_limit,
            )
            if (
                executable
                and executable.size * executable.limit_px > slot.bound.config.max_order_notional_usd
            ):
                cap = quantize_size(
                    slot.bound.config.max_order_notional_usd / executable.limit_px,
                    snapshot.sz_decimals,
                )
                if cap > 0:
                    executable = executable_ioc(
                        snapshot,
                        is_buy=action.side == "buy",
                        requested_size=min(action.size, abs(cap)),
                        max_slippage_bps=bbo_buffer,
                        hard_limit_px=hard_limit,
                    )
                else:
                    executable = None
            if executable is None:
                if increasing:
                    slot.state = RuntimeState.PAUSE_ENTRIES
                    slot.reason = (
                        "latest target is waiting: current BBO is outside the "
                        "source-fill price cap or has no executable liquidity"
                    )
                    return None, action, slot.reason
                self._block(slot, "reduction has no executable liquidity inside its price envelope")
                return None, action, slot.reason
            if retry is not None and retry.require_market_observation:
                observed_ms = (
                    snapshot.context_received_ms
                    if retry.observe_context
                    else max(snapshot.bbo_received_ms, snapshot.book_received_ms)
                )
                if observed_ms <= retry.after_observed_ms:
                    return None, action, f"waiting for post-result market data after {retry.reason}"
                current_fingerprint = _liquidity_fingerprint(
                    snapshot,
                    side=action.side,
                    limit_px=executable.limit_px,
                    include_context=retry.observe_context,
                )
                if (
                    current_fingerprint == retry.liquidity_fingerprint
                    and gate_now_ms < retry.not_before_ms
                ):
                    return None, action, f"waiting before unchanged-liquidity retry after {retry.reason}"
            if retry is not None:
                retry_count = retry.retry_count
                slot.liquidity_retries.pop(action.market, None)
            actual_leverage = follower.positions.get(
                action.market, Position(action.market, Decimal("0"))
            ).leverage
            eligibility = preflight_hyperliquid_perp_order(
                rules=self._order_rules(action.market),
                requested_quantity=executable.size,
                price=executable.limit_px,
                side=action.side,
                max_order_notional_usd=slot.bound.config.max_order_notional_usd,
                reduce_only=action.reduce_only,
                current_position_size=projected_before,
                # Position truth exposes the follower's actual exchange leverage
                # only while that market is open. Never substitute the leader's
                # leverage or Spot spendable balance for follower margin state.
                leverage=actual_leverage,
                available_collateral_usd=None,
            )
            if not eligibility.placeable:
                slot.state = RuntimeState.PAUSE_ENTRIES
                slot.reason = f"order preflight rejected: {eligibility.reason}"
                return None, action, slot.reason
            action = replace(action, size=executable.size)
            if not self._locks or not all(lock.acquired for lock in self._locks):
                self._block(slot, "exclusive account/signer ownership was lost")
                return None, action, slot.reason
            blocker = self._exposure(slot, action, executable.limit_px, gate_now_ms)
            if blocker:
                slot.state, slot.reason = RuntimeState.PAUSE_ENTRIES, blocker
                return None, action, blocker
            if not self.execution_enabled:
                return None, action, "execution disarmed"
            self._reservations[slot.bound.config.slot] = {action.market: action.signed_size}
            required_epoch = self.mux.capture_epoch()
            slot.truth_revision += 1
            best_bid = snapshot.best_bid_or_none
            best_ask = snapshot.best_ask_or_none
            spread_bps = None
            if best_bid is not None and best_ask is not None:
                midpoint = (best_bid + best_ask) / Decimal("2")
                spread_bps = (best_ask - best_bid) / midpoint * Decimal("10000")
            evidence: dict[str, Any] = {
                "price_policy": "latest-bbo-leader-cap-v1",
                "execution_class": execution_class,
                "side": action.side,
                "reduce_only": action.reduce_only,
                "target_requested_size": str(requested_size),
                "submitted_size": str(executable.size),
                "visible_size": str(executable.visible_size),
                "visible_fraction": str(executable.visible_size / executable.size),
                "limit_px": str(executable.limit_px),
                "estimated_visible_vwap": str(executable.estimated_vwap),
                "estimated_visible_notional": str(executable.estimated_notional),
                "mark_px": str(snapshot.mark_px),
                "oracle_px": str(snapshot.oracle_px),
                "best_bid": None if best_bid is None else str(best_bid),
                "best_ask": None if best_ask is None else str(best_ask),
                "spread_bps": None if spread_bps is None else str(spread_bps),
                "bbo_time_ms": snapshot.bbo_time_ms,
                "bbo_age_ms": max(0, gate_now_ms - snapshot.bbo_received_ms),
                "l2_book_time_ms": snapshot.book_time_ms,
                "l2_book_age_ms": max(0, gate_now_ms - snapshot.book_received_ms),
                "liquidity_retry_count": retry_count,
                "source_revision": source_revision,
                "confirmed_follower_size": str(confirmed),
                "projected_follower_size_before": str(projected_before),
                "planned_signed_delta": str(action.signed_size),
                "projected_follower_size_after": str(projected_before + action.signed_size),
            }
            if desired is not None:
                source_position = slot.attributable.get(action.market, Decimal("0"))
                evidence.update(
                    {
                        "source_equity_usd": str(desired.source_equity),
                        "source_equity_basis": slot.identity["source_equity_basis"],
                        "follower_equity_usd": str(desired.follower_equity),
                        "follower_equity_basis": "unified_spot_token0_usdc_total",
                        "copy_multiplier": str(slot.bound.config.multiplier),
                        "sizing_scale": str(desired.scale),
                        "sizing_gross_scale": str(desired.gross_scale),
                        "source_position_size": str(source_position),
                        "raw_scaled_target_size": str(source_position * desired.scale),
                        "desired_target_size": str(
                            desired.positions.get(
                                action.market, Position(action.market, Decimal("0"))
                            ).size
                        ),
                        "desired_gross_notional_usd": str(desired.gross_notional_usd),
                        "slot_gross_cap_usd": str(slot.bound.config.max_gross_exposure_usd),
                        "slot_order_cap_usd": str(slot.bound.config.max_order_notional_usd),
                        "selection_skipped_blockers": (
                            [] if decision is None else list(decision.skipped_blockers)
                        ),
                    }
                )
            if trigger is not None:
                evidence.update(
                    {
                        "leader_trigger_px": str(trigger.price),
                        "leader_trigger_time_ms": trigger.time_ms,
                        "leader_trigger_age_ms": max(0, gate_now_ms - trigger.time_ms),
                        "leader_trigger_admission_age_ms": max(
                            0, trigger.accepted_ms - trigger.time_ms
                        ),
                        "accepted_target_wait_ms": max(0, gate_now_ms - trigger.accepted_ms),
                        "leader_price_cap_bps": str(self.max_tracking_bps),
                    }
                )
        return _PreparedIoc(
            action,
            snapshot,
            executable,
            required_epoch,
            gate_clock,
            evidence,
            retry_count,
            source_revision,
        )

    def _forced_close_action(self, slot: _Slot) -> NextAction | None:
        follower = slot.follower
        assert follower is not None
        unresolved = slot.lane.unresolved_signed_remaining()
        for market, position in sorted(follower.positions.items()):
            pending = unresolved.get(market, Decimal(0))
            projected = position.size + pending
            if not projected or pending:
                continue
            identity = json.dumps(
                ["fail-close-v1", slot.bound.config.slot, market, str(projected)],
                separators=(",", ":"),
            ).encode()
            return NextAction(
                desired_id=sha256(identity).hexdigest(),
                market=market,
                side="sell" if projected > 0 else "buy",
                size=abs(projected),
                reduce_only=True,
                reason=f"fail-close: {slot.fail_close_reason or 'requested'}",
            )
        return None

    def _exposure(self, slot: _Slot, action: NextAction, price: Decimal, now_ms: int) -> str | None:
        slot_id = slot.bound.config.slot
        follower = slot.follower
        assert follower is not None
        current = _sizes(follower.positions)
        current_pending = self._reservations.get(slot_id) or slot.lane.unresolved_signed_remaining()
        for market, value in current_pending.items():
            current[market] = current.get(market, Decimal(0)) + value
        before = current.get(action.market, Decimal(0))
        after = before + action.signed_size
        if abs(after) <= abs(before):
            return None

        bases: dict[str, dict[str, Decimal]] = {}
        sizes: dict[str, dict[str, Decimal]] = {}
        for other_id, other in self._slots.items():
            if (
                other.follower is None
                or not 0 <= now_ms - other.follower.observed_ms <= self.follower_age_ms
            ):
                return "combined exposure is unknown or stale for another slot"
            base = _sizes(other.follower.positions)
            values = dict(base)
            pending = self._reservations.get(other_id)
            unresolved = pending or other.lane.unresolved_signed_remaining()
            for market, value in unresolved.items():
                values[market] = values.get(market, Decimal(0)) + value
            bases[other_id], sizes[other_id] = _nz(base), _nz(values)
        sizes[slot_id][action.market] = after
        if len(set(bases[slot_id]) | set(sizes[slot_id])) > slot.bound.config.max_open_positions:
            return "projected position count exceeds the slot cap"
        gross: dict[str, Decimal] = {}
        for other_id, positions in sizes.items():
            total = Decimal(0)
            for market in set(bases[other_id]) | set(positions):
                if other_id == slot_id and market == action.market:
                    mark = price
                else:
                    snapshot = self._market.fresh_snapshot(
                        market, now_ms=now_ms, max_age_ms=self.market_age_ms
                    )
                    if snapshot is None:
                        return f"combined exposure lacks fresh {market} price"
                    mark = snapshot.mark_px
                total += (
                    max(
                        abs(bases[other_id].get(market, Decimal(0))),
                        abs(positions.get(market, Decimal(0))),
                    )
                    * mark
                )
            gross[other_id] = total
        cap = min(
            slot.bound.config.max_gross_exposure_usd,
            follower.equity * slot.bound.config.max_leverage,
        )
        if gross[slot_id] > cap:
            return "projected slot gross exceeds collateral/config cap"
        if sum(gross.values(), Decimal(0)) > self.plan.plan.max_combined_gross_usd:
            return "projected combined gross exceeds the plan cap"
        return None

    def _provable_reduction(
        self, slot: _Slot, unresolved: Mapping[str, Decimal], now_ms: int
    ) -> NextAction | None:
        follower = slot.follower
        assert follower is not None
        candidates: list[tuple[str, Decimal, Decimal]] = []
        for market, position in sorted(follower.positions.items()):
            pending = unresolved.get(market, Decimal(0))
            projected = position.size + pending
            source = slot.attributable.get(market, Decimal(0))
            if not projected or pending or (source and not _opposite(source, projected)):
                continue
            candidates.append((market, projected, source))
        if not candidates:
            return None
        fresh = next(
            (
                item
                for item in candidates
                if self._market.fresh_snapshot(
                    item[0], now_ms=now_ms, max_age_ms=self.market_age_ms
                )
                is not None
            ),
            candidates[0],
        )
        market, projected, source = fresh
        identity = json.dumps(
            ["provable-close-v1", slot.bound.config.slot, market, str(projected), str(source)],
            separators=(",", ":"),
        ).encode()
        return NextAction(
            desired_id=sha256(identity).hexdigest(),
            market=market,
            side="sell" if projected > 0 else "buy",
            size=abs(projected),
            reduce_only=True,
            reason="close confirmed exposure before any opposite-side replan",
        )

    def _attribute(
        self,
        slot: _Slot,
        fills: tuple[FillRecord, ...],
        *,
        recovered_at_ms: int | None = None,
    ) -> None:
        # Attribution is a transaction.  A duplicate/missing row can make an
        # otherwise plausible recovery chain disagree with source truth; never
        # leave half of that failed chain in durable runtime state.
        unattributed = dict(slot.unattributed)
        attributable = dict(slot.attributable)
        applied_fills = dict(slot.applied_fills)
        triggers = dict(slot.triggers)
        source_revisions = dict(slot.source_revisions)
        pending_catalog_markets = set(slot.pending_catalog_markets)
        denied_markets = set(slot.bound.denied_markets)
        request_catalog_refresh = False
        changed: set[str] = set()
        for fill in fills:
            if fill.identity in applied_fills:
                continue
            market, delta = fill.market, fill.signed_size
            old, copied = (
                unattributed.get(market, Decimal(0)),
                attributable.get(market, Decimal(0)),
            )
            if copied and _opposite(delta, copied):
                amount = min(abs(delta), abs(copied)).copy_sign(delta)
                copied, delta = copied + amount, delta - amount
            if delta and old and _opposite(delta, old):
                amount = min(abs(delta), abs(old)).copy_sign(delta)
                old, delta = old + amount, delta - amount
            fresh_recovery = (
                recovered_at_ms is None
                or 0 <= recovered_at_ms - fill.time_ms <= self.max_source_fill_age_ms
            )
            denied = slot.bound.dynamic_market_eligibility and market in denied_markets
            if fresh_recovery and not denied:
                copied += delta
            else:
                # A gap-recovered increase is observation, not a current signal.
                # Keep it permanently outside follower attribution. Reductions
                # above still consume copied exposure so follower risk can close.
                old += delta
            _put(unattributed, market, old)
            _put(attributable, market, copied)
            applied_fills[fill.identity] = None
            source_revisions[market] = source_revisions.get(market, 0) + 1
            if len(applied_fills) > 4_096:
                applied_fills.pop(next(iter(applied_fills)))
            if fresh_recovery and not denied:
                accepted_ms = recovered_at_ms if recovered_at_ms is not None else fill.received_ms
                triggers[market] = _Trigger(fill.price, fill.time_ms, accepted_ms)
                if slot.bound.dynamic_market_eligibility and self._catalog.market(market) is None:
                    pending_catalog_markets.add(market)
                    request_catalog_refresh = True
            changed.add(market)
        if changed:
            current = _source_sizes(slot)
            if any(
                unattributed.get(market, Decimal(0)) + attributable.get(market, Decimal(0))
                != current.get(market, Decimal(0))
                for market in changed
            ):
                raise ValueError("post-baseline fill attribution does not match source truth")
            slot.unattributed = unattributed
            slot.attributable = attributable
            slot.applied_fills = applied_fills
            slot.triggers = triggers
            slot.source_revisions = source_revisions
            slot.pending_catalog_markets = pending_catalog_markets
            if request_catalog_refresh:
                self._catalog_refresh_request.set()
            for market in changed:
                slot.liquidity_retries.pop(market, None)

    def _attribution_matches(self, slot: _Slot, markets: set[str] | None = None) -> bool:
        current = _source_sizes(slot)
        check = markets or set(current) | set(slot.unattributed) | set(slot.attributable)
        return all(
            slot.unattributed.get(m, Decimal(0)) + slot.attributable.get(m, Decimal(0))
            == current.get(m, Decimal(0))
            for m in check
        )

    def _fold(
        self,
        slot: _Slot,
        action: NextAction,
        attempt: ExecutionAttempt,
        planned: _PreparedIoc,
    ) -> None:
        outcome = attempt.result.outcome
        if outcome is PostOutcome.UNKNOWN:
            self._block(slot, "IOC outcome is unknown")
            return
        lowered = attempt.result.reason.lower()
        same_source_revision = (
            slot.source_revisions.get(action.market, 0) == planned.source_revision
        )
        terminal_observed_ms = self._action_clock()
        retry_observation_floor_ms = max(
            terminal_observed_ms,
            planned.snapshot.bbo_received_ms,
            planned.snapshot.book_received_ms,
        )
        market_retry_observation_floor_ms = max(
            retry_observation_floor_ms,
            planned.snapshot.context_received_ms,
        )
        next_retry_count = planned.retry_count + 1
        liquidity_fingerprint = _liquidity_fingerprint(
            planned.snapshot, side=action.side, limit_px=planned.executable.limit_px
        )
        market_state_fingerprint = _liquidity_fingerprint(
            planned.snapshot,
            side=action.side,
            limit_px=planned.executable.limit_px,
            include_context=True,
        )
        no_liquidity = outcome is PostOutcome.CANCELLED or (
            outcome is PostOutcome.REJECTED
            and any(
                fragment in lowered
                for fragment in (
                    "could not immediately match against any resting orders",
                    "no liquidity",
                    "ioc cancel",
                )
            )
        )
        if no_liquidity:
            if not same_source_revision:
                return
            slot.liquidity_retries[action.market] = _LiquidityRetry(
                desired_id=action.desired_id,
                source_revision=planned.source_revision,
                not_before_ms=(
                    terminal_observed_ms + _liquidity_retry_delay_ms(next_retry_count)
                ),
                after_observed_ms=retry_observation_floor_ms,
                liquidity_fingerprint=liquidity_fingerprint,
                retry_count=next_retry_count,
                require_market_observation=True,
                observe_context=False,
                reason="terminal IOC no-fill",
            )
            return
        if outcome is PostOutcome.REJECTED:
            reason = attempt.result.reason
            detail = lowered.partition(":")[2].strip() if ":" in lowered else lowered
            transient = any(
                fragment in detail
                for fragment in (
                    "rate limit",
                    "temporar",
                    "overload",
                    "unavailable",
                    "try again",
                    "timeout",
                )
            )
            venue_market_state = any(
                fragment in detail
                for fragment in (
                    "positionincreaseatopeninterestcap",
                    "positionflipatopeninterestcap",
                    "tooaggressiveatopeninterestcap",
                    "openinterestincrease",
                    "order would increase open interest while open interest is capped",
                    "order rejected due to price more aggressive than oracle while at open interest cap",
                    "order would increase open interest too quickly",
                    "order price too far from oracle",
                )
            )
            if transient and same_source_revision:
                slot.liquidity_retries[action.market] = _LiquidityRetry(
                    desired_id=action.desired_id,
                    source_revision=planned.source_revision,
                    not_before_ms=terminal_observed_ms + TRANSIENT_REJECTION_RETRY_MS,
                    after_observed_ms=retry_observation_floor_ms,
                    liquidity_fingerprint=liquidity_fingerprint,
                    retry_count=next_retry_count,
                    require_market_observation=False,
                    observe_context=False,
                    reason="transient exchange rejection",
                )
            elif venue_market_state and same_source_revision:
                slot.liquidity_retries[action.market] = _LiquidityRetry(
                    desired_id=action.desired_id,
                    source_revision=planned.source_revision,
                    not_before_ms=(
                        terminal_observed_ms + _liquidity_retry_delay_ms(next_retry_count)
                    ),
                    after_observed_ms=market_retry_observation_floor_ms,
                    liquidity_fingerprint=market_state_fingerprint,
                    retry_count=next_retry_count,
                    require_market_observation=True,
                    observe_context=True,
                    reason="venue market-state rejection",
                )
            elif same_source_revision:
                assert slot.follower is not None
                slot.rejected_targets[action.market] = _RejectedTarget(
                    action.desired_id,
                    _rejection_follower_fingerprint(slot.follower, action.market),
                    _catalog_markets_sha256(self._catalog, (action.market,)),
                    reason,
                )
            return
        if outcome not in {PostOutcome.FILLED, PostOutcome.PARTIALLY_FILLED}:
            return
        slot.liquidity_retries.pop(action.market, None)
        slot.rejected_targets.pop(action.market, None)
        assert slot.follower is not None
        positions = dict(slot.follower.positions)
        prior = positions.get(action.market, Position(action.market, Decimal(0)))
        signed = attempt.result.filled_size if action.side == "buy" else -attempt.result.filled_size
        size = prior.size + signed
        if action.reduce_only and _opposite(size, prior.size):
            self._block(slot, "reduce-only fill crossed confirmed follower truth")
            return
        if size:
            positions[action.market] = replace(prior, coin=action.market, size=size)
        else:
            positions.pop(action.market, None)
        slot.follower = replace(slot.follower, positions=positions)
        if outcome is PostOutcome.PARTIALLY_FILLED and same_source_revision:
            slot.liquidity_retries[action.market] = _LiquidityRetry(
                desired_id=action.desired_id,
                source_revision=planned.source_revision,
                not_before_ms=(
                    terminal_observed_ms + _liquidity_retry_delay_ms(next_retry_count)
                ),
                after_observed_ms=retry_observation_floor_ms,
                liquidity_fingerprint=liquidity_fingerprint,
                retry_count=next_retry_count,
                require_market_observation=True,
                observe_context=False,
                reason="terminal IOC partial fill",
            )

    async def _activate(self) -> MarketSubscriptionChange:
        markets = set(self._prewarm_markets)
        for slot in self._slots.values():
            needed = self._needed(slot)
            try:
                for market in needed:
                    self._market.catalog_market(market)
            except (TypeError, ValueError) as exc:
                self._block(slot, f"market activation failed: {exc}")
                continue
            markets.update(needed)
        async with self._market_lock:
            return self._market.set_active_markets(markets)

    def _needed(self, slot: _Slot) -> set[str]:
        allowed = set(self._allowed(slot))
        values = dict(slot.attributable)
        if slot.follower:
            values.update({m: p.size for m, p in slot.follower.positions.items()})
        values.update(slot.lane.unresolved_signed_remaining())
        follower_markets = set() if slot.follower is None else set(slot.follower.positions)
        unresolved_markets = set(slot.lane.unresolved_signed_remaining())
        return {
            m
            for m, value in values.items()
            if value and (m in allowed or m in follower_markets or m in unresolved_markets)
        }

    def _attributable_positions(self, slot: _Slot) -> dict[str, Position]:
        allowed = set(self._allowed(slot))
        result = {}
        for market, size in slot.attributable.items():
            if market in slot.pending_catalog_markets:
                continue
            if not size or (allowed and market not in allowed):
                continue
            source = slot.account.source.positions.get(market, Position(market, size))
            result[market] = replace(source, size=size)
        return result

    def _truth(self, slot: _Slot, truth: FollowerTruth, now_ms: int) -> FollowerTruth:
        if not truth.equity.is_finite() or truth.equity <= 0 or not 0 < truth.observed_ms <= now_ms:
            raise ValueError("follower WS truth has invalid equity/time")
        allowed, positions = set(self._allowed(slot)), {}
        for market, position in truth.positions.items():
            name = canonical_market_symbol(market)
            if not position.size.is_finite():
                raise ValueError(f"follower position {name} is non-finite")
            managed_tombstone = (
                slot.bound.dynamic_market_eligibility
                and (spec := self._catalog.market(name)) is not None
                and (spec.is_delisted or spec.removal_tombstone)
            )
            if position.size and allowed and name not in allowed and not managed_tombstone:
                raise ValueError(f"follower has unmanaged {name} exposure")
            if position.size:
                positions[name] = replace(position, coin=name)
        return FollowerTruth(
            positions,
            truth.equity,
            truth.observed_ms,
            truth.dex_observed_ms,
            truth.available_collateral,
        )

    async def _ensure_needed_dex_truth(self, slot: _Slot, now_ms: int) -> None:
        refresh = getattr(self.follower_info, "refresh_dex", None)
        if not callable(refresh) or slot.follower is None:
            return
        needed = {market_dex(market) for market in self._needed(slot)}
        stale = [
            dex
            for dex in sorted(needed, key=lambda item: (item != "", item))
            if dex not in self._jit_order_audited[slot.bound.config.slot]
            or not 0
            <= now_ms - int(slot.follower.dex_observed_ms.get(dex, 0))
            <= self.follower_age_ms
        ]
        for dex in stale:
            async with slot.lock:
                if slot.follower is None:
                    return
                revision = slot.truth_revision
                epoch = self.mux.capture_epoch()
            parsed = await refresh(
                slot=self._effective_bound(slot),
                dex=dex,
                mux=self.mux,
                epoch=epoch,
                now_ms=now_ms,
                audit_open_orders=(dex not in self._jit_order_audited[slot.bound.config.slot]),
            )
            async with slot.lock:
                if self.mux.capture_epoch() != epoch or slot.truth_revision != revision:
                    continue
                positions = {
                    market: position
                    for market, position in slot.follower.positions.items()
                    if market_dex(market) != dex
                }
                positions.update(parsed)
                observed = dict(slot.follower.dex_observed_ms)
                observed[dex] = now_ms
                slot.follower = replace(
                    slot.follower,
                    positions=dict(sorted(positions.items())),
                    dex_observed_ms=observed,
                )
                slot.truth_revision += 1
                self._jit_order_audited[slot.bound.config.slot].add(dex)

    def _allowed(self, slot: _Slot) -> tuple[str, ...]:
        return self._allowed_markets[slot.bound.config.slot]

    def _effective_config(self, slot: _Slot):
        return replace(slot.bound.config, allowed_markets=self._allowed(slot))

    def _effective_bound(self, slot: _Slot) -> BoundContinuousSlot:
        return replace(slot.bound, config=self._effective_config(slot))

    def _order_rules(self, market: str) -> HyperliquidPerpRules:
        spec = self._catalog.market(market)
        if spec is None:
            raise ValueError(f"market {market} has no catalog order rules")
        return HyperliquidPerpRules(
            market=spec.symbol,
            sz_decimals=spec.sz_decimals,
            max_leverage=spec.max_leverage,
            margin_mode=getattr(spec, "margin_mode", "unknown"),
        )

    @staticmethod
    def _effective_allowed_markets(
        bound: BoundContinuousSlot, catalog: CatalogRevision
    ) -> tuple[str, ...]:
        if not bound.dynamic_market_eligibility:
            return bound.config.allowed_markets
        denied = set(bound.denied_markets)
        return tuple(
            sorted(
                market.symbol
                for market in catalog.markets
                if market.symbol not in denied
                and not market.is_delisted
                and not market.removal_tombstone
                and market.collateral_token == 0
                and market.readiness not in {MarketReadiness.DELISTED, MarketReadiness.UNTRUSTED}
            )
        )

    def _ready(self, slot: _Slot, now_ms: int) -> None:
        if slot.state is RuntimeState.OPERATOR_STOP:
            return
        if slot.startup_blocker:
            return self._block(slot, slot.startup_blocker)
        if slot.fail_close_reason:
            slot.state = RuntimeState.PAUSE_ENTRIES
            slot.reason = f"fail-close: {slot.fail_close_reason}"
            return
        follower = slot.follower
        if not slot.source_ready or not slot.account.source.is_fresh(
            now_ms=now_ms, max_age_ms=self.source_age_ms
        ):
            return self._block(slot, "source truth is unavailable or stale")
        if follower is None or not 0 <= now_ms - follower.observed_ms <= self.follower_age_ms:
            return self._block(slot, "follower WS truth is unavailable or stale")
        if slot.initial_follower_nonflat:
            return self._block(slot, "startup follower exposure has no runtime attribution")
        if slot.lane.unresolved_signed_remaining():
            return self._block(slot, "signer lane has unresolved action outcome")
        self._prune_rate(slot)
        if (
            slot.operator_paused
            or len(slot.action_times) >= slot.bound.config.action_limit_per_minute
        ):
            slot.state = RuntimeState.PAUSE_ENTRIES
            slot.reason = "operator pause" if slot.operator_paused else "slot action limit reached"
        else:
            slot.state, slot.reason = RuntimeState.RUNNING, "ready"

    def _prune_rate(self, slot: _Slot) -> None:
        cutoff = self._action_clock() - 60_000
        while slot.action_times and slot.action_times[0] <= cutoff:
            slot.action_times.popleft()

    @staticmethod
    def _block(slot: _Slot, reason: str) -> None:
        if slot.state is not RuntimeState.OPERATOR_STOP:
            slot.state, slot.reason = RuntimeState.RECOVERING, reason

    def _change(self, before: set[str]) -> MarketSubscriptionChange:
        after = set(self._market.active_markets)
        return MarketSubscriptionChange(
            tuple(sorted(after - before)), tuple(sorted(before - after))
        )

    def _restore(self, slot: _Slot, payload: Mapping[str, Any] | None) -> bool:
        if payload is None:
            return False
        stored_identity = payload.get("identity")
        identity_upgraded = False
        if stored_identity != slot.identity:
            legacy_identity = {
                key: value
                for key, value in slot.identity.items()
                if key != "denied_markets_sha256"
            }
            if not isinstance(stored_identity, Mapping) or dict(stored_identity) != legacy_identity:
                raise ValueError(
                    f"continuous state identity mismatch for {slot.bound.config.slot}"
                )
            identity_upgraded = True
        slot.ever_baselined = True
        slot.expected_source = _read_decimals(payload.get("source"))
        slot.unattributed = _read_decimals(payload.get("unattributed"))
        slot.attributable = _read_decimals(payload.get("attributable"))
        raw_pending = payload.get("pending_catalog_markets", [])
        if not isinstance(raw_pending, list) or any(
            not isinstance(item, str) for item in raw_pending
        ):
            raise ValueError(f"malformed pending catalog state for {slot.bound.config.slot}")
        pending = {canonical_market_symbol(item) for item in raw_pending}
        if slot.bound.dynamic_market_eligibility:
            # The runtime catalog is applied before its file is atomically replaced. If the
            # process dies in that narrow window, reconstruct discovery intent from the
            # durable source state rather than silently waiting for another leader fill.
            pending.update(
                market
                for market, size in slot.attributable.items()
                if size and self._catalog.market(market) is None
            )
        slot.initial_follower_nonflat = payload.get("initial_follower_nonflat") is True
        fail_close_reason = payload.get("fail_close_reason")
        if isinstance(fail_close_reason, str) and fail_close_reason.strip():
            slot.fail_close_reason = fail_close_reason.strip()
            slot.operator_paused = True
        follower = payload.get("follower")
        if isinstance(follower, Mapping):
            slot.restored_follower_positions = _read_decimals(follower.get("positions"))
        slot.applied_fills = {
            (str(item[0]), str(item[1]), str(item[2])): None
            for item in payload.get("fills", [])
            if isinstance(item, list) and len(item) == 3
        }
        triggers = payload.get("triggers")
        if isinstance(triggers, Mapping):
            for market, value in triggers.items():
                if isinstance(value, Mapping):
                    slot.triggers[str(market)] = _Trigger(
                        Decimal(str(value["price"])),
                        int(value["time_ms"]),
                        int(value.get("accepted_ms", value["time_ms"])),
                    )
        revisions = payload.get("source_revisions")
        if isinstance(revisions, Mapping):
            slot.source_revisions = {
                str(market): max(0, int(revision))
                for market, revision in revisions.items()
            }
        allowed = set(self._allowed(slot))
        for market in pending:
            spec = self._catalog.market(market)
            if spec is None:
                if slot.attributable.get(market, Decimal(0)):
                    slot.pending_catalog_markets.add(market)
                    self._catalog_refresh_request.set()
                continue
            if market in allowed:
                continue
            size = slot.attributable.pop(market, Decimal(0))
            _put(slot.unattributed, market, slot.unattributed.get(market, Decimal(0)) + size)
            slot.triggers.pop(market, None)
        stored_state = str(payload.get("state") or "")
        if stored_state == RuntimeState.OPERATOR_STOP.value:
            slot.state, slot.reason = RuntimeState.OPERATOR_STOP, "restored operator stop"
        elif stored_state == RuntimeState.PAUSE_ENTRIES.value:
            slot.operator_paused = True
        return identity_upgraded


def _sizes(positions: Mapping[str, Position]) -> dict[str, Decimal]:
    return {canonical_market_symbol(m): p.size for m, p in positions.items() if p.size}


def _source_sizes(slot: _Slot) -> dict[str, Decimal]:
    return _sizes(slot.account.source.positions)


def _wire_decimals(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {market: str(value) for market, value in values.items() if value}


def _read_decimals(value: Any) -> dict[str, Decimal]:
    return {str(k): Decimal(str(v)) for k, v in value.items()} if isinstance(value, Mapping) else {}


def _nz(values: Mapping[str, Decimal]) -> dict[str, Decimal]:
    return {market: value for market, value in values.items() if value}


def _opposite(left: Decimal, right: Decimal) -> bool:
    return bool(left and right and (left > 0) != (right > 0))


def _put(values: dict[str, Decimal], market: str, value: Decimal) -> None:
    if value:
        values[market] = value
    else:
        values.pop(market, None)


def _rejection_follower_fingerprint(
    follower: FollowerTruth,
    market: str,
) -> tuple[str, str, str, str]:
    position = follower.positions.get(market, Position(market, Decimal("0")))
    portfolio = [
        {
            "market": name,
            "size": str(value.size),
            "leverage": "" if value.leverage is None else str(value.leverage),
        }
        for name, value in sorted(follower.positions.items())
    ]
    portfolio_sha256 = sha256(
        json.dumps(portfolio, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return (
        str(position.size),
        "" if position.leverage is None else str(position.leverage),
        str(follower.equity),
        portfolio_sha256,
    )


def _catalog_markets_sha256(catalog: CatalogRevision, markets: tuple[str, ...]) -> str:
    identities = []
    for market in sorted(markets):
        spec = catalog.market(market)
        if spec is None:
            identities.append({"symbol": market, "missing": True})
            continue
        identities.append(
            {
                "symbol": spec.symbol,
                "dex": spec.dex,
                "asset_id": spec.asset_id,
                "dex_index": spec.dex_index,
                "universe_index": spec.universe_index,
                "sz_decimals": spec.sz_decimals,
                "max_leverage": spec.max_leverage,
                "margin_mode": spec.margin_mode,
                "collateral_token": spec.collateral_token,
                "margin_table_id": spec.margin_table_id,
            }
        )
    encoded = json.dumps(identities, sort_keys=True, separators=(",", ":")).encode()
    return sha256(encoded).hexdigest()


def _liquidity_fingerprint(
    snapshot: MarketSnapshot,
    *,
    side: str,
    limit_px: Decimal,
    include_context: bool = False,
) -> tuple[str, ...]:
    """Describe executable liquidity without trusting periodic timestamps.

    Unchanged periodic L2 snapshots must not unlock another IOC.  Price/size
    changes at any level inside the hard limit do, and an identical-looking
    book may retry only after a post-result observation plus bounded backoff.
    """

    levels = snapshot.asks if side == "buy" else snapshot.bids
    eligible = (
        level
        for level in levels
        if (level.price <= limit_px if side == "buy" else level.price >= limit_px)
    )
    context = (
        (str(snapshot.oracle_px), str(snapshot.mark_px))
        if include_context
        else ()
    )
    return (
        side,
        str(limit_px),
        *context,
        *(f"{level.price}:{level.size}" for level in eligible),
    )


def _liquidity_retry_delay_ms(retry_count: int) -> int:
    if retry_count <= 0:
        raise ValueError("liquidity retry count must be positive")
    return (1_000, 2_000, 5_000)[min(retry_count - 1, 2)]


__all__ = [
    "ContinuousRuntime",
    "Dispatch",
    "FollowerInfoHook",
    "FollowerTruth",
    "GapRepairHook",
    "RuntimeState",
]
