from __future__ import annotations

import asyncio
import json
import threading
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from time import monotonic, monotonic_ns, sleep, time_ns
from typing import Any, Callable, Mapping, Protocol

from .account_state import (
    AccountStateBook,
    FollowerAccountRevision,
    StateProvenance,
    fetch_all_dex_clearinghouse_states,
)
from .journal_writer import JournalWriter
from .models import OpenOrder, Position, parse_decimal
from .observer import HyperliquidInfoClient, parse_clearinghouse_positions, parse_open_orders
from .rest_budget import (
    RestBudgetCoordinator,
    RestBudgetPipeClient,
    RestGrant,
    RestPriority,
    authoritative_rest_weight,
)
from .unified_account import parse_all_dexs_message


def info_weight(request_type: str) -> int:
    return authoritative_rest_weight(f"info:{request_type}")


def info_priority(request_type: str) -> RestPriority:
    if request_type in {"orderStatus", "historicalOrders"}:
        return RestPriority.AMBIGUITY_CONTAINMENT
    if request_type in {"clearinghouseState", "spotClearinghouseState"}:
        return RestPriority.AFFECTED_FOLLOWER
    if request_type in {"userFillsByTime", "userTwapSliceFillsByTime"}:
        return RestPriority.GAP_REPAIR
    if request_type in {"perpDexs", "allPerpMetas", "meta", "metaAndAssetCtxs"}:
        return RestPriority.CATALOG
    return RestPriority.BROAD_AUDIT


def launch_info_priority(request_type: str) -> RestPriority:
    """Keep read-only launch work out of execution and containment priority pools."""

    if request_type in {"perpDexs", "allPerpMetas", "meta", "metaAndAssetCtxs"}:
        return RestPriority.CATALOG
    return RestPriority.BROAD_AUDIT


class GrantProvider(Protocol):
    def request_grant(self, **kwargs: Any) -> RestGrant: ...


class RestAdmissionClosed(RuntimeError):
    def __init__(self, priority: RestPriority) -> None:
        self.priority = priority
        super().__init__(f"REST priority {int(priority)} is closed during containment")


class RestRetrySessionExpired(TimeoutError):
    pass


@dataclass(slots=True)
class RestRetrySession:
    """Keep one terminal observation on the exact denied request.

    Waiting for the first grant does not age exchange truth.  Once the first
    successful response arrives, every later response in the session must fit
    inside one bounded coherence window.
    """

    wait_deadline_mono: float
    response_window_s: float = 60.0
    first_response_mono: float | None = None
    first_response_wall_ms: int = 0
    last_response_wall_ms: int = 0

    @property
    def deadline_mono(self) -> float:
        if self.first_response_mono is None:
            return self.wait_deadline_mono
        return self.first_response_mono + self.response_window_s

    def observe_response(self) -> None:
        observed_mono = monotonic()
        observed_wall_ms = time_ns() // 1_000_000
        if observed_mono > self.deadline_mono:
            raise RestRetrySessionExpired("terminal REST response window expired")
        if self.first_response_mono is None:
            self.first_response_mono = observed_mono
            self.first_response_wall_ms = observed_wall_ms
        self.last_response_wall_ms = observed_wall_ms


class BudgetedInfoClient:
    """Every HTTP info request obtains a durable grant before issuing bytes."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout_s: float,
        grants: RestBudgetCoordinator | RestBudgetPipeClient,
        sender: str = "fleet-runtime",
        sender_epoch: int = 1,
        priority_resolver: Callable[[str], RestPriority] = info_priority,
        wait_for_budget: bool = False,
        budget_deadline_mono: float | None = None,
        budget_waiter: Callable[[float], None] | None = None,
        priority_admission: Callable[[RestPriority], bool] | None = None,
        initial_message_id: int = 0,
    ) -> None:
        if initial_message_id < 0:
            raise ValueError("initial REST message ID must be nonnegative")
        self.raw = HyperliquidInfoClient(base_url, timeout_s)
        # Disable the legacy independent throttle. The durable coordinator is authoritative.
        self.raw.rest_throttle_enabled = False
        self.grants = grants
        self.sender = sender
        self.sender_epoch = sender_epoch
        self.priority_resolver = priority_resolver
        self.wait_for_budget = wait_for_budget
        self.budget_deadline_mono = budget_deadline_mono
        self.budget_waiter = budget_waiter or sleep
        self.priority_admission = priority_admission
        self._message_id = initial_message_id
        self._lock = threading.Lock()
        self._inflight_by_priority: dict[RestPriority, int] = {}
        self._ordinary_admission_closed = False

    @staticmethod
    def _is_ordinary(priority: RestPriority) -> bool:
        return priority != RestPriority.AMBIGUITY_CONTAINMENT

    def close_ordinary_admission(self) -> int:
        """Atomically close priorities 1-4 and return their in-flight count."""

        with self._lock:
            self._ordinary_admission_closed = True
            return sum(
                count
                for priority, count in self._inflight_by_priority.items()
                if self._is_ordinary(priority)
            )

    def ordinary_inflight_count(self) -> int:
        with self._lock:
            return sum(
                count
                for priority, count in self._inflight_by_priority.items()
                if self._is_ordinary(priority)
            )

    def _admitted_locked(self, priority: RestPriority) -> bool:
        return not (
            (self._ordinary_admission_closed and self._is_ordinary(priority))
            or (self.priority_admission is not None and not self.priority_admission(priority))
        )

    def _assert_admitted(self, priority: RestPriority) -> None:
        with self._lock:
            admitted = self._admitted_locked(priority)
        if not admitted:
            raise RestAdmissionClosed(priority)

    def _begin_request(self, priority: RestPriority) -> None:
        with self._lock:
            if not self._admitted_locked(priority):
                raise RestAdmissionClosed(priority)
            self._inflight_by_priority[priority] = self._inflight_by_priority.get(priority, 0) + 1

    def info(
        self,
        payload: dict[str, Any],
        *,
        priority: RestPriority | None = None,
        retry_session: RestRetrySession | None = None,
    ) -> Any:
        request_type = str(payload.get("type") or "unknown")
        weight = info_weight(request_type)
        selected_priority = self.priority_resolver(request_type) if priority is None else priority
        # The runtime switches from bounded startup waiting to fail-closed
        # steady-state admission only after accepting the supervisor's active
        # control. A request already in flight must retain the startup deadline
        # and heartbeat-capable waiter through its terminal grant.
        wait_for_budget = self.wait_for_budget
        budget_deadline_mono = self.budget_deadline_mono
        budget_waiter = self.budget_waiter
        while True:
            self._assert_admitted(selected_priority)
            retry_deadline = (
                retry_session.deadline_mono if retry_session is not None else budget_deadline_mono
            )
            if (
                retry_deadline is not None
                and (wait_for_budget or retry_session is not None)
                and monotonic() >= retry_deadline
            ):
                if retry_session is not None:
                    raise RestRetrySessionExpired("terminal REST retry session expired")
                raise TimeoutError("REST discovery exceeded its shared startup deadline")
            if isinstance(self.grants, RestBudgetCoordinator):
                with self._lock:
                    self._message_id += 1
                    message_id = self._message_id
                grant = self.grants.request_grant(
                    sender=self.sender,
                    sender_epoch=self.sender_epoch,
                    message_id=message_id,
                    priority=selected_priority,
                    endpoint=f"info:{request_type}",
                    weight=weight,
                )
            else:
                grant = self.grants.request_grant(
                    priority=selected_priority,
                    endpoint=f"info:{request_type}",
                    weight=weight,
                )
            if grant.granted:
                break
            retryable = grant.reason in {
                "rolling_weight_budget_exhausted",
                "monotonic_clock_epoch_quarantine",
            }
            wait_s = max(0.001, grant.retry_after_ms / 1_000)
            waiting = wait_for_budget or retry_session is not None
            if not waiting or not retryable or retry_deadline is None:
                raise RestBudgetDenied(grant)
            if monotonic() + wait_s > retry_deadline:
                if retry_session is not None:
                    raise RestRetrySessionExpired("terminal REST retry session expired")
                raise RestBudgetDenied(grant)
            budget_waiter(wait_s)
        if retry_session is not None and monotonic() > retry_session.deadline_mono:
            raise RestRetrySessionExpired("terminal REST retry session expired after grant")
        self._begin_request(selected_priority)
        try:
            response = self.raw.info(payload)
        finally:
            with self._lock:
                remaining = self._inflight_by_priority[selected_priority] - 1
                if remaining:
                    self._inflight_by_priority[selected_priority] = remaining
                else:
                    self._inflight_by_priority.pop(selected_priority, None)
        if retry_session is not None:
            retry_session.observe_response()
        return response


class RestBudgetDenied(RuntimeError):
    def __init__(self, grant: RestGrant):
        self.grant = grant
        super().__init__(
            f"REST budget denied {grant.endpoint}; retry_after_ms={grant.retry_after_ms}"
        )


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    follower: str
    revision: FollowerAccountRevision
    triggers: tuple[str, ...]
    divergence: tuple[str, ...]
    external_activity: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class OrderStatusResolution:
    resolved: bool
    terminal: bool
    state: str
    cumulative_filled_abs: Decimal
    oid: int | None
    raw: Any


class Reconciler:
    """Background-only follower truth and targeted ambiguity owner."""

    def __init__(
        self,
        *,
        client: BudgetedInfoClient,
        state_book: AccountStateBook,
        freshness_ms: int,
        all_dexes: tuple[str, ...],
        journal: JournalWriter | None = None,
        monitor_nonfunding_ledger: bool = False,
    ) -> None:
        if freshness_ms <= 0:
            raise ValueError("reconciler freshness must be positive")
        self.client = client
        self.state_book = state_book
        self.freshness_ms = freshness_ms
        self.all_dexes = all_dexes
        self.journal = journal
        self.monitor_nonfunding_ledger = monitor_nonfunding_ledger
        self._locks: dict[str, asyncio.Lock] = {}
        self._follower_dexes: dict[str, set[str]] = {}

    def update_all_dexes(self, dexes: tuple[str, ...]) -> None:
        # A DEX disappearing from the current catalog does not prove its
        # follower orders are gone.  Retain every DEX queried during this run
        # until terminal truth is order-free.
        normalized = tuple(sorted({str(dex) for dex in dexes} | set(self.all_dexes) | {""}))
        self.all_dexes = normalized
        for known in self._follower_dexes.values():
            known.update(normalized)

    async def refresh_follower(
        self,
        follower: str,
        *,
        trigger: str,
        full_audit: bool,
        catalog_revision: str,
        affected_dexes: tuple[str, ...] | None = None,
        priority_override: RestPriority | None = None,
        retry_session: RestRetrySession | None = None,
        include_nonfunding_ledger: bool | None = None,
        publish: bool = True,
    ) -> ReconcileResult:
        account = follower.lower()
        if self.monitor_nonfunding_ledger and publish:
            raise ValueError(
                "monitored ledger reconciliation must be classified before publication"
            )
        lock = self._locks.setdefault(account, asyncio.Lock())
        async with lock:
            ledger_requested = bool(
                full_audit
                and self.monitor_nonfunding_ledger
                and include_nonfunding_ledger is not False
            )
            if include_nonfunding_ledger is True and not (
                full_audit and self.monitor_nonfunding_ledger
            ):
                raise ValueError("non-funding ledger refresh requires a monitored full audit")
            received = time_ns() // 1_000_000
            prior = self.state_book.follower(account)
            states: dict[str, dict[str, Any]] = {}
            known_dexes = self._follower_dexes.setdefault(account, set(self.all_dexes) | {""})
            known_dexes.update(self.all_dexes)
            if full_audit:
                priority = (
                    priority_override if priority_override is not None else RestPriority.BROAD_AUDIT
                )
                aggregate = await asyncio.to_thread(
                    fetch_all_dex_clearinghouse_states,
                    lambda payload: self.client.info(
                        payload,
                        priority=priority,
                        retry_session=retry_session,
                    ),
                    user=account,
                    dexes=sorted(known_dexes),
                )
                parsed = parse_all_dexs_message(
                    {"channel": "allDexsClearinghouseState", "data": aggregate},
                    expected_account=account,
                    received_ms=received,
                )
                states.update(parsed.clearinghouse_states)
                for dex, state in parsed.clearinghouse_states.items():
                    if parse_clearinghouse_positions(state, received, dex=dex):
                        known_dexes.add(dex)
            requested_dexes = (
                set(known_dexes)
                if full_audit or affected_dexes is None
                else set(affected_dexes) | {""}
            )
            for dex in sorted(requested_dexes):
                if dex in states and full_audit:
                    continue
                request: dict[str, Any] = {
                    "type": "clearinghouseState",
                    "user": account,
                }
                if dex:
                    request["dex"] = dex
                state = await asyncio.to_thread(
                    self.client.info,
                    request,
                    priority=(
                        priority_override
                        if priority_override is not None
                        else (
                            RestPriority.AFFECTED_FOLLOWER
                            if not full_audit
                            else RestPriority.BROAD_AUDIT
                        )
                    ),
                    retry_session=retry_session,
                )
                if not isinstance(state, dict):
                    raise ValueError("follower clearinghouseState is malformed")
                states[dex] = state
            spot_state = await asyncio.to_thread(
                self.client.info,
                {"type": "spotClearinghouseState", "user": account},
                priority=(
                    priority_override
                    if priority_override is not None
                    else (
                        RestPriority.AFFECTED_FOLLOWER
                        if not full_audit
                        else RestPriority.BROAD_AUDIT
                    )
                ),
                retry_session=retry_session,
            )
            if not isinstance(spot_state, Mapping):
                raise ValueError("follower spotClearinghouseState is malformed")
            # Freshness starts when the complete requested position/account
            # response set has arrived, never when the network work began.
            received = time_ns() // 1_000_000
            positions: dict[str, Position] = {}
            exchange_times: list[int] = []
            for dex, state in states.items():
                positions.update(parse_clearinghouse_positions(state, received, dex=dex))
                raw_time = state.get("time")
                if raw_time is not None:
                    try:
                        exchange_times.append(int(str(raw_time)))
                    except ValueError:
                        pass
            raw_spot_time = spot_state.get("time")
            if raw_spot_time is not None:
                try:
                    exchange_times.append(int(str(raw_spot_time)))
                except ValueError:
                    pass
            open_orders: list[OpenOrder] = []
            if full_audit:
                for dex in sorted(known_dexes):
                    request = {"type": "openOrders", "user": account}
                    if dex:
                        request["dex"] = dex
                    raw_orders = await asyncio.to_thread(
                        self.client.info,
                        request,
                        priority=(
                            priority_override
                            if priority_override is not None
                            else RestPriority.BROAD_AUDIT
                        ),
                        retry_session=retry_session,
                    )
                    open_orders.extend(parse_open_orders(raw_orders, received, dex=dex))
            external_activity: tuple[Mapping[str, Any], ...] = ()
            ledger_checkpoint = {} if prior is None else dict(prior.nonfunding_ledger_checkpoint)
            if ledger_requested:
                durable_cursor = ledger_checkpoint.get("cursor_ms")
                durable_seen = ledger_checkpoint.get("seen_identities")
                first_ledger_baseline = not ledger_checkpoint
                prior_cursor = (
                    durable_cursor if isinstance(durable_cursor, int) else max(0, received - 60_000)
                )
                raw_ledger = await asyncio.to_thread(
                    self.client.info,
                    {
                        "type": "userNonFundingLedgerUpdates",
                        "user": account,
                        "startTime": max(0, prior_cursor - 60_000),
                        "endTime": received,
                    },
                    priority=(
                        priority_override
                        if priority_override is not None
                        else RestPriority.BROAD_AUDIT
                    ),
                    retry_session=retry_session,
                )
                if not isinstance(raw_ledger, list):
                    raise ValueError("follower non-funding ledger response is malformed")
                seen = (
                    {str(identity) for identity in durable_seen}
                    if isinstance(durable_seen, (list, tuple))
                    else set()
                )
                newly_observed: list[Mapping[str, Any]] = []
                retained_identities: set[str] = set()
                for row in raw_ledger:
                    if not isinstance(row, Mapping):
                        raise ValueError("follower non-funding ledger row is malformed")
                    raw_time = row.get("time")
                    delta = row.get("delta")
                    if not isinstance(raw_time, int) or not isinstance(delta, Mapping):
                        raise ValueError("follower non-funding ledger identity is malformed")
                    identity = sha256(
                        json.dumps(row, sort_keys=True, separators=(",", ":")).encode("utf-8")
                    ).hexdigest()
                    if identity in seen:
                        if raw_time >= received - 60_000:
                            retained_identities.add(identity)
                        continue
                    seen.add(identity)
                    if raw_time >= received - 60_000:
                        retained_identities.add(identity)
                    if not first_ledger_baseline:
                        newly_observed.append(dict(row))
                ledger_checkpoint = {
                    "cursor_ms": received,
                    "seen_identities": tuple(sorted(retained_identities)),
                }
                external_activity = tuple(newly_observed)
            reconciled = time_ns() // 1_000_000
            projected = positions if prior is None else dict(prior.projected_positions)
            inflight = {} if prior is None else dict(prior.inflight_by_cloid)
            divergence = _position_divergence(projected, positions)
            if divergence and not inflight:
                projected = positions
            account_value, available_margin = _unified_spot_usdc_collateral(spot_state)
            revision = FollowerAccountRevision(
                follower_address=account,
                revision=1 if prior is None else prior.revision + 1,
                confirmed_positions=positions,
                projected_positions=projected,
                open_orders=tuple(
                    open_orders if full_audit else (() if prior is None else prior.open_orders)
                ),
                inflight_by_cloid=inflight,
                account_value=account_value,
                available_margin=available_margin,
                account_mode="unified",
                exchange_ts_ms=min(exchange_times) if exchange_times else received,
                receive_wall_ms=received,
                reconcile_wall_ms=reconciled,
                fresh_until_ms=received + self.freshness_ms,
                confidence="full_audit" if full_audit else "affected_follower",
                provenance=StateProvenance.FULL_AUDIT
                if full_audit
                else StateProvenance.TARGETED_REST,
                catalog_revision=catalog_revision,
                durable_checkpoint=0 if prior is None else prior.durable_checkpoint,
                leverage_blocks=({} if prior is None else dict(prior.leverage_blocks)),
                nonfunding_ledger_checkpoint=ledger_checkpoint,
            )
            if publish and self.journal is not None:
                await self.journal.submit(
                    "append_state_revision",
                    kind="follower",
                    revision_id=f"follower:{account}:{revision.revision}",
                    owner=account,
                    revision=revision.revision,
                    catalog_revision=catalog_revision,
                    observed_wall_ms=received,
                    observed_mono_ns=monotonic_ns(),
                    provenance=revision.provenance.value,
                    payload={
                        "follower": account,
                        "revision": revision.revision,
                        "trigger": trigger,
                        "full_audit": full_audit,
                        "confirmed_positions": {
                            market: {
                                "size": str(position.size),
                                "entry_px": (
                                    None if position.entry_px is None else str(position.entry_px)
                                ),
                                "leverage": position.leverage,
                                "updated_ms": position.updated_ms,
                            }
                            for market, position in revision.confirmed_positions.items()
                        },
                        "projected_positions": {
                            market: {
                                "size": str(position.size),
                                "entry_px": (
                                    None if position.entry_px is None else str(position.entry_px)
                                ),
                                "leverage": position.leverage,
                                "updated_ms": position.updated_ms,
                            }
                            for market, position in revision.projected_positions.items()
                        },
                        "account_value": str(revision.account_value),
                        "available_margin": str(revision.available_margin),
                        "account_mode": revision.account_mode,
                        "leverage_blocks": revision.leverage_blocks,
                        "nonfunding_ledger_checkpoint": (revision.nonfunding_ledger_checkpoint),
                        "open_orders": [
                            {
                                "coin": order.coin,
                                "side": order.side,
                                "size": str(order.size),
                                "oid": order.oid,
                                "cloid": order.cloid,
                                "reduce_only": order.reduce_only,
                            }
                            for order in revision.open_orders
                        ],
                        "divergence": divergence,
                    },
                )
            if publish:
                self.state_book.publish_follower(revision)
            return ReconcileResult(
                account,
                revision,
                (trigger,),
                divergence,
                external_activity,
            )

    async def reconcile_cloid(
        self,
        *,
        follower: str,
        cloid: str,
        expected_size: Decimal,
    ) -> OrderStatusResolution:
        return await self.reconcile_order_identity(
            follower=follower,
            order_identity=cloid,
            expected_size=expected_size,
        )

    async def reconcile_order_identity(
        self,
        *,
        follower: str,
        order_identity: str | int,
        expected_size: Decimal,
    ) -> OrderStatusResolution:
        raw = await asyncio.to_thread(
            self.client.info,
            {
                "type": "orderStatus",
                "user": follower.lower(),
                "oid": order_identity,
            },
            priority=RestPriority.AMBIGUITY_CONTAINMENT,
        )
        return classify_order_status(raw, expected_size=expected_size)


def classify_order_status(payload: Any, *, expected_size: Decimal) -> OrderStatusResolution:
    if not expected_size.is_finite() or expected_size <= 0:
        raise ValueError("expected order size must be finite and positive")
    statuses: list[str] = []
    quantities: list[Decimal] = []
    oid: int | None = None
    original_sizes: list[Decimal] = []
    remaining_sizes: list[Decimal] = []

    def decimal_value(value: Any) -> Decimal | None:
        try:
            parsed = Decimal(str(value))
        except Exception:
            return None
        return abs(parsed) if parsed.is_finite() and parsed >= 0 else None

    def visit(value: Any) -> None:
        nonlocal oid
        if isinstance(value, Mapping):
            status = value.get("status")
            if isinstance(status, str):
                statuses.append(status)
            for key in ("totalSz", "filledSz", "filledSize", "executedSz"):
                if value.get(key) is not None:
                    parsed = decimal_value(value[key])
                    if parsed is not None:
                        quantities.append(parsed)
            if value.get("oid") is not None and oid is None:
                try:
                    oid = int(value["oid"])
                except Exception:
                    pass
            for key in ("origSz", "originalSz"):
                if value.get(key) is not None:
                    parsed = decimal_value(value[key])
                    if parsed is not None:
                        original_sizes.append(parsed)
            if value.get("sz") is not None:
                parsed = decimal_value(value["sz"])
                if parsed is not None:
                    remaining_sizes.append(parsed)
            for nested in value.values():
                if isinstance(nested, (Mapping, list, tuple)):
                    visit(nested)
        elif isinstance(value, (list, tuple)):
            for nested in value:
                visit(nested)

    visit(payload)
    normalized = [
        "".join(character for character in status.casefold() if character.isalnum())
        for status in statuses
    ]
    cumulative = max(quantities, default=Decimal("0"))
    if original_sizes and remaining_sizes:
        cumulative = max(
            cumulative,
            max(Decimal("0"), max(original_sizes) - min(remaining_sizes)),
        )
    if cumulative > expected_size:
        return OrderStatusResolution(
            False,
            False,
            "reported_fill_exceeds_submitted_size",
            cumulative,
            oid,
            payload,
        )
    terminal_states = {
        "filled": "filled",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "rejected": "rejected",
        "expired": "expired",
        "margincanceled": "cancelled",
        "margincancelled": "cancelled",
        "openinterestcapcanceled": "cancelled",
        "openinterestcapcancelled": "cancelled",
        "selftradecanceled": "cancelled",
        "selftradecancelled": "cancelled",
    }
    for status in normalized:
        state = terminal_states.get(status)
        if state is None and (
            status == "scheduledcancel"
            or status.endswith("canceled")
            or status.endswith("cancelled")
        ):
            state = "cancelled"
        elif state is None and status.endswith("rejected"):
            state = "rejected"
        if state is None:
            continue
        if state == "filled" and cumulative == 0:
            cumulative = expected_size
        return OrderStatusResolution(
            True,
            True,
            state,
            cumulative,
            oid,
            payload,
        )
    for status in normalized:
        if status in {"partiallyfilled", "partialfill"}:
            return OrderStatusResolution(
                True,
                False,
                "partially_filled",
                cumulative,
                oid,
                payload,
            )
        if status in {"open", "resting", "triggered", "pending"}:
            return OrderStatusResolution(
                True,
                False,
                "resting",
                cumulative,
                oid,
                payload,
            )
    return OrderStatusResolution(
        False,
        False,
        "unknown_transport_outcome",
        cumulative,
        oid,
        payload,
    )


def _position_divergence(
    projected: Mapping[str, Position], confirmed: Mapping[str, Position]
) -> tuple[str, ...]:
    symbols = set(projected) | set(confirmed)
    return tuple(
        sorted(
            symbol
            for symbol in symbols
            if (
                projected.get(symbol, Position(symbol, Decimal("0"))).size
                != confirmed.get(symbol, Position(symbol, Decimal("0"))).size
                or projected.get(symbol, Position(symbol, Decimal("0"))).size != 0
                and confirmed.get(symbol, Position(symbol, Decimal("0"))).size != 0
                and projected.get(symbol, Position(symbol, Decimal("0"))).leverage
                != confirmed.get(symbol, Position(symbol, Decimal("0"))).leverage
            )
        )
    )


def _unified_spot_usdc_collateral(spot_state: Mapping[str, Any]) -> tuple[Decimal, Decimal]:
    """Return the collateral basis used by Hyperliquid unified accounts.

    Unified-account per-DEX margin summaries can legitimately report zero even
    while the account is funded.  Token-0 Spot USDC is the shared collateral
    source, so using a perp summary here silently disables every follower.
    """

    balances = spot_state.get("balances")
    if not isinstance(balances, list):
        raise ValueError("unified follower spot balances are missing")
    matches = [
        row
        for row in balances
        if isinstance(row, Mapping)
        and str(row.get("coin") or "").upper() == "USDC"
        and str(row.get("token") if row.get("token") is not None else "0") == "0"
    ]
    if len(matches) != 1:
        raise ValueError("unified follower requires exactly one token-0 USDC balance")
    try:
        total = parse_decimal(matches[0].get("total"))
        hold = parse_decimal(matches[0].get("hold"))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError("unified follower token-0 USDC total/hold is malformed") from exc
    if not total.is_finite() or total < 0:
        raise ValueError("unified follower token-0 USDC total must be finite and non-negative")
    if not hold.is_finite() or hold < 0 or hold > total:
        raise ValueError("unified follower token-0 USDC hold is invalid")
    return total, total - hold
