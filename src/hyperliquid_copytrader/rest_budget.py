from __future__ import annotations

import hmac
import json
import os
import sqlite3
import threading
from dataclasses import asdict, dataclass
from enum import IntEnum
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, time_ns
from typing import Any, Mapping


class RestPriority(IntEnum):
    AMBIGUITY_CONTAINMENT = 0
    AFFECTED_FOLLOWER = 1
    GAP_REPAIR = 2
    CATALOG = 3
    BROAD_AUDIT = 4
    ANALYTICS = 5


REST_BUDGET_WINDOW_MS = 60_000
REST_CLOCK_EPOCH_QUARANTINE_MS = REST_BUDGET_WINDOW_MS


_INFO_ENDPOINT_WEIGHTS: dict[str, int] = {
    "l2Book": 2,
    "allMids": 2,
    "clearinghouseState": 2,
    "spotClearinghouseState": 2,
    "orderStatus": 2,
    "userRole": 60,
    "userAbstraction": 20,
    "userDexAbstraction": 20,
    "vaultDetails": 20,
    "extraAgents": 20,
    "subAccounts": 20,
    "userRateLimit": 20,
    # Retain the historical reservation in the frozen coordinator policy even
    # though production HTTP callers use per-DEX clearinghouseState requests.
    # Removing it would invalidate the durable prelaunch ledger policy hash.
    "allDexsClearinghouseState": 20,
    "openOrders": 20,
    # Response-weighted history calls reserve their documented 2,000-row
    # maximum (20 request + 100 response units).  A caller crash therefore
    # cannot leave unaccounted venue weight in the shared rolling ledger.
    "historicalOrders": 120,
    "userFillsByTime": 120,
    "userTwapSliceFillsByTime": 120,
    "userNonFundingLedgerUpdates": 20,
    "perpDexs": 20,
    "allPerpMetas": 20,
    "meta": 20,
    "metaAndAssetCtxs": 20,
    "spotMetaAndAssetCtxs": 20,
}
_RUNTIME_SENDERS = {"fleet-runtime"}
_GUARDIAN_SENDERS = {
    "containment-guardian",
    "containment-guardian-info",
    "containment-guardian-info-takeover",
    "containment-guardian-action-takeover",
}
_SUPERVISOR_SENDERS = {"fleet-supervisor-network-end"}
_LAUNCH_SENDERS = {
    "context-feed-measurement",
    "fast-execution-benchmark",
    "ui-launch-preview",
    "fleet-supervisor-preview-reservation",
    "fleet-supervisor-launch",
    "fleet-supervisor-launch-network",
}


def authoritative_rest_weight(endpoint: str) -> int:
    if endpoint.startswith("info:"):
        request_type = endpoint.removeprefix("info:")
        if request_type not in _INFO_ENDPOINT_WEIGHTS:
            raise ValueError(f"REST endpoint policy has no weight for {endpoint}")
        return _INFO_ENDPOINT_WEIGHTS[request_type]
    if endpoint.startswith("exchange:"):
        action = endpoint.removeprefix("exchange:")
        if action not in {"cancel", "cancelByCloid", "order", "scheduleCancel"}:
            raise ValueError(f"REST endpoint policy rejects {endpoint}")
        return 1
    if endpoint == "network-profile:api.hyperliquid.xyz":
        # Three official allMids info requests at weight two each.
        return 6
    raise ValueError(f"REST endpoint policy rejects {endpoint}")


def _validate_sender_policy(
    *, sender: str, priority: RestPriority, endpoint: str, weight: int
) -> None:
    expected_weight = authoritative_rest_weight(endpoint)
    if weight != expected_weight:
        raise ValueError(
            f"REST endpoint weight mismatch for {endpoint}: {weight} != {expected_weight}"
        )
    if sender in _RUNTIME_SENDERS:
        if priority is RestPriority.ANALYTICS or endpoint.startswith("exchange:"):
            raise ValueError("fleet runtime REST role exceeded its admission policy")
        return
    if sender in _GUARDIAN_SENDERS:
        if priority is not RestPriority.AMBIGUITY_CONTAINMENT:
            raise ValueError("containment REST role may use only containment priority")
        return
    if sender in _SUPERVISOR_SENDERS:
        if (
            priority is not RestPriority.ANALYTICS
            or endpoint != "network-profile:api.hyperliquid.xyz"
        ):
            raise ValueError("supervisor REST role exceeded its terminal measurement policy")
        return
    if sender in _LAUNCH_SENDERS:
        if priority not in {
            RestPriority.CATALOG,
            RestPriority.BROAD_AUDIT,
            RestPriority.ANALYTICS,
        } or endpoint.startswith("exchange:"):
            raise ValueError("launch REST role exceeded its read-only admission policy")
        return
    raise ValueError(f"REST sender is not present in the frozen role policy: {sender}")


def _rest_policy_sha256() -> str:
    payload = {
        "endpoint_weights": _INFO_ENDPOINT_WEIGHTS,
        "runtime_senders": sorted(_RUNTIME_SENDERS),
        "guardian_senders": sorted(_GUARDIAN_SENDERS),
        "supervisor_senders": sorted(_SUPERVISOR_SENDERS),
        "launch_senders": sorted(_LAUNCH_SENDERS),
        "version": 6,
    }
    return sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def rest_policy_sha256() -> str:
    """Return the exact immutable policy identity used by the coordinator."""

    return _rest_policy_sha256()


def validate_rest_request_policy(
    *, sender: str, priority: RestPriority, endpoint: str, weight: int
) -> None:
    """Apply the coordinator's authoritative endpoint and sender policy."""

    _validate_sender_policy(
        sender=sender,
        priority=priority,
        endpoint=endpoint,
        weight=weight,
    )


@dataclass(frozen=True, slots=True)
class RestGrant:
    grant_id: str
    granted: bool
    generation: str
    coordinator_epoch: int
    sender: str
    sender_epoch: int
    message_id: int
    priority: RestPriority
    endpoint: str
    weight: int
    pool: str
    granted_wall_ms: int
    retry_after_ms: int
    reason: str


@dataclass(frozen=True, slots=True)
class RestLoadModel:
    fleet_slots: int
    audited_dexes: int | None = None
    cheap_follower_weight: int = 2
    cheap_follower_queries_per_cycle: int | None = None
    cheap_follower_period_ms: int = 5_000
    # HTTP account truth and open orders are both read once per audited DEX.
    full_dex_discovery_weight: int = 2
    full_open_order_weight: int = 20
    full_follower_period_ms: int = 60_000
    nonfunding_ledger_weight: int = 20
    nonfunding_ledger_period_ms: int = 600_000
    catalog_refresh_weight: int = 60
    catalog_period_ms: int = 60_000

    def per_minute(self) -> dict[str, int]:
        if self.fleet_slots < 1:
            raise ValueError("REST load model requires at least one slot")
        cheap_cycles = (60_000 + self.cheap_follower_period_ms - 1) // self.cheap_follower_period_ms
        catalog_cycles = (60_000 + self.catalog_period_ms - 1) // self.catalog_period_ms
        ledger_queries = (
            self.fleet_slots * 60_000 + self.nonfunding_ledger_period_ms - 1
        ) // self.nonfunding_ledger_period_ms
        audited_dexes = self.fleet_slots if self.audited_dexes is None else self.audited_dexes
        cheap_queries = (
            self.fleet_slots
            if self.cheap_follower_queries_per_cycle is None
            else self.cheap_follower_queries_per_cycle
        )
        if audited_dexes < self.fleet_slots or cheap_queries < self.fleet_slots:
            raise ValueError("REST load model must cover every follower and at least one DEX")
        components = {
            "affected_follower": cheap_queries * cheap_cycles * self.cheap_follower_weight,
            "full_follower_dex_discovery": (
                audited_dexes * 60_000 + self.full_follower_period_ms - 1
            )
            // self.full_follower_period_ms
            * self.full_dex_discovery_weight,
            "full_open_order_audit": (audited_dexes * 60_000 + self.full_follower_period_ms - 1)
            // self.full_follower_period_ms
            * self.full_open_order_weight,
            "nonfunding_ledger_audit": ledger_queries * self.nonfunding_ledger_weight,
            "catalog": catalog_cycles * self.catalog_refresh_weight,
        }
        components["total"] = sum(components.values())
        return components

    def validate(self, *, ordinary_budget: int, reserve_budget: int) -> dict[str, Any]:
        components = self.per_minute()
        blockers: list[str] = []
        if components["total"] > ordinary_budget:
            blockers.append(
                f"configured fleet requires {components['total']} ordinary REST weight/min, "
                f"above frozen budget {ordinary_budget}"
            )
        if reserve_budget <= 0:
            blockers.append("emergency REST reserve must be positive")
        return {
            "components": components,
            "ordinary_budget": ordinary_budget,
            "reserve_budget": reserve_budget,
            "ordinary_headroom": ordinary_budget - components["total"],
            "blockers": blockers,
            "passed": not blockers,
        }


@dataclass(frozen=True, slots=True)
class RestTakeoverProof:
    exact_prior_process_exited: bool
    exit_attestation_sha256: str
    prior_grants_retained: bool

    @property
    def complete(self) -> bool:
        return (
            self.exact_prior_process_exited
            and len(self.exit_attestation_sha256) == 64
            and self.exit_attestation_sha256 == self.exit_attestation_sha256.lower()
            and all(character in "0123456789abcdef" for character in self.exit_attestation_sha256)
            and self.prior_grants_retained
        )


class RestBudgetCoordinator:
    """Durable rolling REST grant ledger with fenced epoch ownership."""

    def __init__(
        self,
        path: Path | str,
        *,
        generation: str,
        ordinary_weight: int = 720,
        reserve_weight: int = 480,
        process_identity: str,
        epoch: int = 1,
        claim_initial: bool = True,
        priority_coalesce_ms: int = 5,
        max_pending_requests: int = 1_024,
        critical_pending_reserve: int = 64,
        sender_reorder_window: int = 64,
    ) -> None:
        if not generation or not process_identity or epoch < 1:
            raise ValueError("REST coordinator identity is incomplete")
        if ordinary_weight <= 0 or reserve_weight <= 0:
            raise ValueError("REST coordinator budgets must be positive")
        if ordinary_weight + reserve_weight > 1_200:
            raise ValueError("REST coordinator budgets exceed the frozen IP allowance")
        if not 0 <= priority_coalesce_ms <= 100:
            raise ValueError("REST priority coalescing must be between 0 and 100ms")
        if (
            max_pending_requests < 2
            or not 0 < critical_pending_reserve < max_pending_requests
            or sender_reorder_window < 1
        ):
            raise ValueError("REST durable queue bounds are invalid")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.generation = generation
        self.ordinary_weight = ordinary_weight
        self.reserve_weight = reserve_weight
        self.process_identity = process_identity
        self.epoch = epoch
        self.priority_coalesce_ms = priority_coalesce_ms
        self.max_pending_requests = max_pending_requests
        self.critical_pending_reserve = critical_pending_reserve
        self.sender_reorder_window = sender_reorder_window
        self.policy_sha256 = _rest_policy_sha256()
        self.clock_epoch = 1
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._initialize()
        if claim_initial:
            self._claim_initial_epoch()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def owner_snapshot(self) -> dict[str, Any] | None:
        """Return the durable coordinator owner without changing its lease or epoch."""

        with self._lock:
            row = self._conn.execute("SELECT * FROM coordinator_owner WHERE singleton=1").fetchone()
        return None if row is None else dict(row)

    def sender_message_highwater(self, *, sender: str, sender_epoch: int) -> int:
        """Return the durable message high-water mark for one fenced sender epoch."""

        if not sender or sender_epoch < 1:
            raise ValueError("REST sender identity is incomplete")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT coalesce(max(message_id), 0) AS highwater
                FROM (
                  SELECT message_id FROM rest_ipc_acks
                  WHERE generation=? AND sender=? AND sender_epoch=?
                  UNION ALL
                  SELECT message_id FROM rest_requests
                  WHERE generation=? AND sender=? AND sender_epoch=?
                )
                """,
                (
                    self.generation,
                    sender,
                    sender_epoch,
                    self.generation,
                    sender,
                    sender_epoch,
                ),
            ).fetchone()
        return int(row["highwater"] or 0)

    def recent_grant_snapshot(
        self,
        *,
        now_wall_ms: int | None = None,
        now_mono_ms: int | None = None,
    ) -> dict[str, Any]:
        """Snapshot every durable grant still inside the shared rolling window."""

        now = time_ns() // 1_000_000 if now_wall_ms is None else int(now_wall_ms)
        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else int(now_mono_ms)
        with self._lock:
            rows = self._recent_grant_rows_locked(now_mono_ms=now_mono)
        payload: dict[str, Any] = {
            "generation": self.generation,
            "clock_epoch": self.clock_epoch,
            "captured_wall_ms": now,
            "captured_mono_ms": now_mono,
            "window_ms": REST_BUDGET_WINDOW_MS,
            "grants": rows,
        }
        payload["snapshot_sha256"] = sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return payload

    def import_recent_grant_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        source_generation: str,
        now_wall_ms: int | None = None,
        now_mono_ms: int | None = None,
    ) -> dict[str, Any]:
        """Carry live preview spend into a run ledger without resetting its age."""

        body = dict(snapshot)
        snapshot_sha256 = str(body.pop("snapshot_sha256", ""))
        if (
            snapshot_sha256
            != sha256(
                json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
        ):
            raise ValueError("REST grant import snapshot hash is invalid")
        rows = body.get("grants")
        if (
            body.get("generation") != source_generation
            or int(body.get("clock_epoch") or 0) < 1
            or int(body.get("window_ms") or 0) != REST_BUDGET_WINDOW_MS
            or not isinstance(rows, list)
        ):
            raise ValueError("REST grant import snapshot identity is invalid")
        now = time_ns() // 1_000_000 if now_wall_ms is None else int(now_wall_ms)
        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else int(now_mono_ms)
        captured_mono = int(body.get("captured_mono_ms") or 0)
        captured_wall = int(body.get("captured_wall_ms") or 0)
        if (
            captured_wall <= 0
            or captured_mono <= 0
            or captured_mono > now_mono
            or self._clock_quarantine_remaining(now_mono_ms=now_mono) > 0
        ):
            raise ValueError("REST grant import snapshot clock identity is invalid")
        validated: list[dict[str, Any]] = []
        seen: set[str] = set()
        for index, raw in enumerate(rows, start=1):
            if not isinstance(raw, Mapping):
                raise ValueError("REST grant import row is malformed")
            row = dict(raw)
            source_grant_id = str(row.get("grant_id") or "")
            raw_priority = row.get("priority")
            if not isinstance(raw_priority, (int, str)):
                raise ValueError("REST grant import priority is malformed")
            priority = RestPriority(int(raw_priority))
            endpoint = str(row.get("endpoint") or "")
            weight = int(row.get("weight") or 0)
            sender = str(row.get("sender") or "")
            granted_mono = int(row.get("granted_mono_ms") or 0)
            granted_wall = int(row.get("granted_wall_ms") or 0)
            if (
                not source_grant_id
                or source_grant_id in seen
                or row.get("generation") != source_generation
                or int(row.get("clock_epoch") or 0) != int(body["clock_epoch"])
                or row.get("pool") != "ordinary"
                or granted_wall <= 0
                or granted_mono <= 0
                or granted_mono > captured_mono
            ):
                raise ValueError("REST grant import row identity is invalid")
            _validate_sender_policy(
                sender=sender,
                priority=priority,
                endpoint=endpoint,
                weight=weight,
            )
            seen.add(source_grant_id)
            if now_mono - granted_mono >= REST_BUDGET_WINDOW_MS:
                continue
            validated.append(
                {
                    "source_grant_id": source_grant_id,
                    "message_id": index,
                    "priority": priority,
                    "endpoint": endpoint,
                    "weight": weight,
                    "granted_wall_ms": granted_wall,
                    "granted_mono_ms": granted_mono,
                }
            )
        if sum(int(row["weight"]) for row in validated) > self.ordinary_weight:
            raise ValueError("REST grant import exceeds the run ordinary budget")
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                prior = self._conn.execute(
                    "SELECT * FROM rest_grant_imports WHERE source_generation=?",
                    (source_generation,),
                ).fetchone()
                if prior is not None:
                    if str(prior["snapshot_sha256"]) != snapshot_sha256:
                        raise RuntimeError("REST grant import source was replayed differently")
                    self._conn.commit()
                    return dict(prior)
                for row in validated:
                    grant_id = (
                        "rest-carryover-"
                        + sha256(
                            (
                                f"{self.generation}|{self.clock_epoch}|{row['source_grant_id']}"
                            ).encode("utf-8")
                        ).hexdigest()[:32]
                    )
                    self._conn.execute(
                        """
                        INSERT INTO rest_grants(
                          grant_id,generation,coordinator_epoch,sender,sender_epoch,
                          message_id,priority,endpoint,weight,pool,granted_wall_ms,
                          granted_mono_ms,clock_epoch
                        ) VALUES (?,?,?,'fleet-supervisor-preview-reservation',1,?,?,?,?,'ordinary',?,?,?)
                        """,
                        (
                            grant_id,
                            self.generation,
                            self.epoch,
                            row["message_id"],
                            int(row["priority"]),
                            row["endpoint"],
                            row["weight"],
                            row["granted_wall_ms"],
                            row["granted_mono_ms"],
                            self.clock_epoch,
                        ),
                    )
                    request_payload = {
                        "generation": self.generation,
                        "sender": "fleet-supervisor-preview-reservation",
                        "sender_epoch": 1,
                        "message_id": row["message_id"],
                        "priority": int(row["priority"]),
                        "endpoint": row["endpoint"],
                        "weight": row["weight"],
                    }
                    request_sha256 = sha256(
                        json.dumps(
                            request_payload,
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest()
                    grant = RestGrant(
                        grant_id=grant_id,
                        granted=True,
                        generation=self.generation,
                        coordinator_epoch=self.epoch,
                        sender="fleet-supervisor-preview-reservation",
                        sender_epoch=1,
                        message_id=int(row["message_id"]),
                        priority=RestPriority(int(row["priority"])),
                        endpoint=str(row["endpoint"]),
                        weight=int(row["weight"]),
                        pool="ordinary",
                        granted_wall_ms=int(row["granted_wall_ms"]),
                        retry_after_ms=0,
                        reason="imported_preview_reservation",
                    )
                    response = asdict(grant)
                    response["priority"] = int(grant.priority)
                    response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
                    self._conn.execute(
                        """
                        INSERT INTO rest_requests(
                          generation,sender,sender_epoch,message_id,priority,
                          endpoint,weight,request_sha256,requested_wall_ms,
                          requested_mono_ms,status,response_json,decided_wall_ms
                        ) VALUES (?,'fleet-supervisor-preview-reservation',1,?,?,?,?,?,?,?,
                                  'decided',?,?)
                        """,
                        (
                            self.generation,
                            row["message_id"],
                            int(row["priority"]),
                            row["endpoint"],
                            row["weight"],
                            request_sha256,
                            row["granted_wall_ms"],
                            row["granted_mono_ms"],
                            response_json,
                            now,
                        ),
                    )
                    self._conn.execute(
                        """
                        INSERT INTO rest_ipc_acks(
                          generation,sender,sender_epoch,message_id,
                          request_sha256,response_json,created_wall_ms
                        ) VALUES (?,'fleet-supervisor-preview-reservation',1,?,?,?,?)
                        """,
                        (
                            self.generation,
                            row["message_id"],
                            request_sha256,
                            response_json,
                            now,
                        ),
                    )
                self._conn.execute(
                    """
                    INSERT INTO rest_grant_imports(
                      source_generation,snapshot_sha256,source_clock_epoch,
                      captured_wall_ms,captured_mono_ms,source_grant_count,
                      imported_live_grant_count,imported_weight,imported_clock_epoch,
                      imported_wall_ms
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        source_generation,
                        snapshot_sha256,
                        int(body["clock_epoch"]),
                        captured_wall,
                        captured_mono,
                        len(rows),
                        len(validated),
                        sum(int(row["weight"]) for row in validated),
                        self.clock_epoch,
                        now,
                    ),
                )
                result = self._conn.execute(
                    "SELECT * FROM rest_grant_imports WHERE source_generation=?",
                    (source_generation,),
                ).fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        assert result is not None
        return dict(result)

    def egress_fence(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM rest_egress_fence WHERE singleton=1").fetchone()
        return None if row is None else dict(row)

    def stale_egress_fence_release_status(
        self, *, now_mono_ms: int | None = None
    ) -> dict[str, Any]:
        """Prove whether an ownerless fence has drained across one clock epoch."""

        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else int(now_mono_ms)
        with self._lock:
            fence = self._conn.execute(
                "SELECT * FROM rest_egress_fence WHERE singleton=1"
            ).fetchone()
            epoch = self._clock_epoch_row_locked()
        if fence is None:
            return {"ready": True, "wait_ms": 0, "reason": "no_fence"}
        fence_epoch = int(fence["clock_epoch"])
        if fence_epoch == self.clock_epoch:
            ready_at = int(fence["frozen_mono_ms"]) + REST_BUDGET_WINDOW_MS
            reason = "same_clock_epoch_window"
        else:
            ready_at = int(epoch["quarantine_until_mono_ms"])
            reason = "prior_clock_epoch_quarantine"
        wait_ms = max(0, ready_at - now_mono)
        return {
            "ready": wait_ms == 0,
            "wait_ms": wait_ms,
            "reason": reason,
            "clock_epoch": self.clock_epoch,
            "fence_clock_epoch": fence_epoch,
        }

    def freeze_egress(
        self,
        *,
        preview_sha256: str,
        expected_snapshot: Mapping[str, Any],
        now_wall_ms: int | None = None,
        now_mono_ms: int | None = None,
    ) -> dict[str, Any]:
        """Fence the host-wide prelaunch ledger after validating its token-bound snapshot."""

        if len(preview_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in preview_sha256
        ):
            raise ValueError("REST egress fence preview hash is invalid")
        expected = dict(expected_snapshot)
        expected_hash = str(expected.pop("snapshot_sha256", ""))
        actual_hash = sha256(
            json.dumps(expected, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        if (
            expected_hash != actual_hash
            or expected.get("generation") != self.generation
            or int(expected.get("window_ms") or 0) != REST_BUDGET_WINDOW_MS
            or not isinstance(expected.get("grants"), list)
        ):
            raise ValueError("REST egress fence snapshot is invalid")
        expected_ids = {
            str(row.get("grant_id") or "") for row in expected["grants"] if isinstance(row, Mapping)
        }
        if not expected_ids or "" in expected_ids or len(expected_ids) != len(expected["grants"]):
            raise ValueError("REST egress fence snapshot grant identities are invalid")
        now = time_ns() // 1_000_000 if now_wall_ms is None else int(now_wall_ms)
        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else int(now_mono_ms)
        captured_mono_ms = int(expected.get("captured_mono_ms") or 0)
        snapshot_clock_epoch = int(expected.get("clock_epoch") or 0)
        if (
            snapshot_clock_epoch != self.clock_epoch
            or captured_mono_ms <= 0
            or captured_mono_ms > now_mono
            or any(
                not isinstance(row, Mapping)
                or int(row.get("clock_epoch") or 0) != snapshot_clock_epoch
                or int(row.get("granted_mono_ms") or 0) <= 0
                or int(row.get("granted_mono_ms") or 0) > captured_mono_ms
                for row in expected["grants"]
            )
        ):
            raise ValueError("REST egress fence snapshot clock epoch is invalid")
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT * FROM rest_egress_fence WHERE singleton=1"
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing["generation"]) == self.generation
                        and str(existing["preview_sha256"]) == preview_sha256
                    ):
                        self._conn.commit()
                        return dict(existing)
                    raise RuntimeError("host-wide REST egress is already launch-fenced")
                pending = int(
                    self._conn.execute(
                        "SELECT count(*) FROM rest_requests "
                        "WHERE generation=? AND status='pending'",
                        (self.generation,),
                    ).fetchone()[0]
                )
                if pending:
                    raise RuntimeError("host-wide REST egress has pending requests")
                current_ids = {
                    str(row["grant_id"])
                    for row in self._recent_grant_rows_locked(now_mono_ms=now_mono)
                }
                unexpected = sorted(current_ids - expected_ids)
                if unexpected:
                    raise RuntimeError(
                        "host-wide REST grants changed after preview; request a fresh preview"
                    )
                self._conn.execute(
                    "INSERT INTO rest_egress_fence("
                    "singleton,generation,preview_sha256,process_identity,"
                    "frozen_wall_ms,frozen_mono_ms,clock_epoch) VALUES(1,?,?,?,?,?,?)",
                    (
                        self.generation,
                        preview_sha256,
                        self.process_identity,
                        now,
                        now_mono,
                        self.clock_epoch,
                    ),
                )
                row = self._conn.execute(
                    "SELECT * FROM rest_egress_fence WHERE singleton=1"
                ).fetchone()
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        assert row is not None
        return dict(row)

    def release_egress_fence(self, *, preview_sha256: str) -> bool:
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT preview_sha256 FROM rest_egress_fence WHERE singleton=1"
            ).fetchone()
            if row is None:
                return False
            if str(row["preview_sha256"]) != preview_sha256:
                raise RuntimeError("REST egress fence preview identity mismatch")
            self._conn.execute("DELETE FROM rest_egress_fence WHERE singleton=1")
        return True

    def _recent_grant_rows_locked(self, *, now_mono_ms: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self._conn.execute(
                "SELECT seq,grant_id,generation,coordinator_epoch,sender,sender_epoch,"
                "message_id,priority,endpoint,weight,pool,granted_wall_ms,granted_mono_ms,"
                "clock_epoch FROM rest_grants WHERE generation=? AND clock_epoch=? "
                "AND granted_mono_ms>? ORDER BY seq",
                (
                    self.generation,
                    self.clock_epoch,
                    now_mono_ms - REST_BUDGET_WINDOW_MS,
                ),
            ).fetchall()
        ]

    def _initialize(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS coordinator_owner (
                  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                  generation TEXT NOT NULL,
                  epoch INTEGER NOT NULL,
                  process_identity TEXT NOT NULL,
                  ordinary_weight INTEGER NOT NULL,
                  reserve_weight INTEGER NOT NULL,
                  policy_sha256 TEXT NOT NULL,
                  clock_epoch INTEGER NOT NULL DEFAULT 1,
                  last_wall_ms INTEGER NOT NULL,
                  last_mono_ms INTEGER NOT NULL,
                  updated_wall_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rest_grants (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  grant_id TEXT NOT NULL UNIQUE,
                  generation TEXT NOT NULL,
                  coordinator_epoch INTEGER NOT NULL,
                  sender TEXT NOT NULL,
                  sender_epoch INTEGER NOT NULL,
                  message_id INTEGER NOT NULL,
                  priority INTEGER NOT NULL,
                  endpoint TEXT NOT NULL,
                  weight INTEGER NOT NULL,
                  pool TEXT NOT NULL,
                  granted_wall_ms INTEGER NOT NULL,
                  granted_mono_ms INTEGER NOT NULL,
                  clock_epoch INTEGER NOT NULL DEFAULT 1,
                  UNIQUE(generation, sender, sender_epoch, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_rest_grants_window
                  ON rest_grants(generation, pool, granted_mono_ms);
                CREATE INDEX IF NOT EXISTS idx_rest_grants_clock_window
                  ON rest_grants(generation, clock_epoch, pool, granted_mono_ms);
                CREATE TABLE IF NOT EXISTS rest_requests (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  generation TEXT NOT NULL,
                  sender TEXT NOT NULL,
                  sender_epoch INTEGER NOT NULL,
                  message_id INTEGER NOT NULL,
                  priority INTEGER NOT NULL,
                  endpoint TEXT NOT NULL,
                  weight INTEGER NOT NULL,
                  request_sha256 TEXT NOT NULL,
                  requested_wall_ms INTEGER NOT NULL,
                  requested_mono_ms INTEGER NOT NULL,
                  status TEXT NOT NULL CHECK(status IN ('pending','decided')),
                  response_json TEXT NOT NULL DEFAULT '',
                  decided_wall_ms INTEGER NOT NULL DEFAULT 0,
                  UNIQUE(generation, sender, sender_epoch, message_id)
                );
                CREATE INDEX IF NOT EXISTS idx_rest_requests_pending
                  ON rest_requests(generation, status, priority, seq);
                CREATE TABLE IF NOT EXISTS rest_ipc_acks (
                  generation TEXT NOT NULL,
                  sender TEXT NOT NULL,
                  sender_epoch INTEGER NOT NULL,
                  message_id INTEGER NOT NULL,
                  request_sha256 TEXT NOT NULL,
                  response_json TEXT NOT NULL,
                  created_wall_ms INTEGER NOT NULL,
                  PRIMARY KEY(generation, sender, sender_epoch, message_id)
                );
                CREATE TABLE IF NOT EXISTS coordinator_takeovers (
                  transition_id TEXT PRIMARY KEY,
                  generation TEXT NOT NULL,
                  from_epoch INTEGER NOT NULL,
                  to_epoch INTEGER NOT NULL,
                  from_process_identity TEXT NOT NULL,
                  to_process_identity TEXT NOT NULL,
                  proof_json TEXT NOT NULL,
                  created_wall_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rest_egress_fence (
                  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                  generation TEXT NOT NULL,
                  preview_sha256 TEXT NOT NULL,
                  process_identity TEXT NOT NULL,
                  frozen_wall_ms INTEGER NOT NULL,
                  frozen_mono_ms INTEGER NOT NULL,
                  clock_epoch INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS coordinator_clock_epochs (
                  generation TEXT NOT NULL,
                  clock_epoch INTEGER NOT NULL,
                  started_wall_ms INTEGER NOT NULL,
                  started_mono_ms INTEGER NOT NULL,
                  quarantine_until_mono_ms INTEGER NOT NULL,
                  prior_clock_epoch INTEGER NOT NULL,
                  prior_last_wall_ms INTEGER NOT NULL,
                  prior_last_mono_ms INTEGER NOT NULL,
                  reason TEXT NOT NULL,
                  PRIMARY KEY(generation, clock_epoch)
                );
                CREATE TABLE IF NOT EXISTS rest_grant_imports (
                  source_generation TEXT PRIMARY KEY,
                  snapshot_sha256 TEXT NOT NULL,
                  source_clock_epoch INTEGER NOT NULL,
                  captured_wall_ms INTEGER NOT NULL,
                  captured_mono_ms INTEGER NOT NULL,
                  source_grant_count INTEGER NOT NULL,
                  imported_live_grant_count INTEGER NOT NULL,
                  imported_weight INTEGER NOT NULL,
                  imported_clock_epoch INTEGER NOT NULL,
                  imported_wall_ms INTEGER NOT NULL
                );
                """
            )
            owner_columns = {
                str(row[1]) for row in self._conn.execute("PRAGMA table_info(coordinator_owner)")
            }
            if "last_mono_ms" not in owner_columns:
                self._conn.execute(
                    "ALTER TABLE coordinator_owner ADD COLUMN last_mono_ms INTEGER NOT NULL DEFAULT 0"
                )
            if "ordinary_weight" not in owner_columns:
                self._conn.execute(
                    "ALTER TABLE coordinator_owner ADD COLUMN ordinary_weight INTEGER NOT NULL DEFAULT 0"
                )
            if "reserve_weight" not in owner_columns:
                self._conn.execute(
                    "ALTER TABLE coordinator_owner ADD COLUMN reserve_weight INTEGER NOT NULL DEFAULT 0"
                )
            if "policy_sha256" not in owner_columns:
                self._conn.execute(
                    "ALTER TABLE coordinator_owner ADD COLUMN policy_sha256 TEXT NOT NULL DEFAULT ''"
                )
            if "clock_epoch" not in owner_columns:
                self._conn.execute(
                    "ALTER TABLE coordinator_owner ADD COLUMN clock_epoch INTEGER NOT NULL DEFAULT 1"
                )
            grant_columns = {
                str(row[1]) for row in self._conn.execute("PRAGMA table_info(rest_grants)")
            }
            if "granted_mono_ms" not in grant_columns:
                self._conn.execute(
                    "ALTER TABLE rest_grants ADD COLUMN granted_mono_ms INTEGER NOT NULL DEFAULT 0"
                )
            if "clock_epoch" not in grant_columns:
                self._conn.execute(
                    "ALTER TABLE rest_grants ADD COLUMN clock_epoch INTEGER NOT NULL DEFAULT 1"
                )
            fence_columns = {
                str(row[1]) for row in self._conn.execute("PRAGMA table_info(rest_egress_fence)")
            }
            if "clock_epoch" not in fence_columns:
                self._conn.execute(
                    "ALTER TABLE rest_egress_fence ADD COLUMN clock_epoch INTEGER NOT NULL DEFAULT 1"
                )

    @staticmethod
    def _clock_epoch_changed(
        *, last_wall_ms: int, last_mono_ms: int, now_wall_ms: int, now_mono_ms: int
    ) -> bool:
        if last_wall_ms <= 0 or last_mono_ms <= 0:
            return False
        wall_delta = now_wall_ms - last_wall_ms
        mono_delta = now_mono_ms - last_mono_ms
        return mono_delta < 0 or abs(wall_delta - mono_delta) > 2_000

    def _insert_clock_epoch_locked(
        self,
        *,
        clock_epoch: int,
        now_wall_ms: int,
        now_mono_ms: int,
        quarantine_ms: int,
        prior_clock_epoch: int,
        prior_last_wall_ms: int,
        prior_last_mono_ms: int,
        reason: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO coordinator_clock_epochs(
              generation, clock_epoch, started_wall_ms, started_mono_ms,
              quarantine_until_mono_ms, prior_clock_epoch,
              prior_last_wall_ms, prior_last_mono_ms, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.generation,
                clock_epoch,
                now_wall_ms,
                now_mono_ms,
                now_mono_ms + quarantine_ms,
                prior_clock_epoch,
                prior_last_wall_ms,
                prior_last_mono_ms,
                reason,
            ),
        )

    def _begin_new_clock_epoch_locked(
        self,
        *,
        owner: Mapping[str, Any],
        now_wall_ms: int,
        now_mono_ms: int,
        reason: str,
    ) -> int:
        prior_clock_epoch = int(owner["clock_epoch"])
        clock_epoch = prior_clock_epoch + 1
        self._insert_clock_epoch_locked(
            clock_epoch=clock_epoch,
            now_wall_ms=now_wall_ms,
            now_mono_ms=now_mono_ms,
            quarantine_ms=REST_CLOCK_EPOCH_QUARANTINE_MS,
            prior_clock_epoch=prior_clock_epoch,
            prior_last_wall_ms=int(owner["last_wall_ms"]),
            prior_last_mono_ms=int(owner["last_mono_ms"]),
            reason=reason,
        )
        self._conn.execute(
            """
            UPDATE coordinator_owner
            SET clock_epoch=?, last_wall_ms=?, last_mono_ms=?, updated_wall_ms=?
            WHERE singleton=1
            """,
            (clock_epoch, now_wall_ms, now_mono_ms, now_wall_ms),
        )
        self.clock_epoch = clock_epoch
        return clock_epoch

    def _clock_epoch_row_locked(self) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT * FROM coordinator_clock_epochs WHERE generation=? AND clock_epoch=?",
            (self.generation, self.clock_epoch),
        ).fetchone()
        if row is None:
            raise RuntimeError("REST coordinator clock epoch evidence is missing")
        return row

    def _durable_mono_highwater_locked(self) -> int:
        values = [
            int(
                self._conn.execute(
                    "SELECT coalesce(max(last_mono_ms),0) FROM coordinator_owner"
                ).fetchone()[0]
            ),
            int(
                self._conn.execute(
                    "SELECT coalesce(max(granted_mono_ms),0) FROM rest_grants"
                ).fetchone()[0]
            ),
            int(
                self._conn.execute(
                    "SELECT coalesce(max(requested_mono_ms),0) FROM rest_requests"
                ).fetchone()[0]
            ),
            int(
                self._conn.execute(
                    "SELECT coalesce(max(frozen_mono_ms),0) FROM rest_egress_fence"
                ).fetchone()[0]
            ),
            int(
                self._conn.execute(
                    "SELECT coalesce(max(started_mono_ms),0) FROM coordinator_clock_epochs"
                ).fetchone()[0]
            ),
        ]
        return max(values)

    def _clock_quarantine_wait_locked(self, *, now_mono_ms: int) -> int:
        epoch = self._clock_epoch_row_locked()
        return max(0, int(epoch["quarantine_until_mono_ms"]) - now_mono_ms)

    def _deny_pending_for_clock_epoch_locked(self, *, now_wall_ms: int, now_mono_ms: int) -> None:
        pending = self._conn.execute(
            "SELECT * FROM rest_requests WHERE generation=? AND status='pending' ORDER BY seq",
            (self.generation,),
        ).fetchall()
        for row in pending:
            grant = self._denial(
                sender=str(row["sender"]),
                sender_epoch=int(row["sender_epoch"]),
                message_id=int(row["message_id"]),
                priority=RestPriority(int(row["priority"])),
                endpoint=str(row["endpoint"]),
                weight=int(row["weight"]),
                now=now_wall_ms,
                now_mono=now_mono_ms,
                reason="monotonic_clock_epoch_requires_exact_request_replay",
            )
            response = asdict(grant)
            response["priority"] = int(grant.priority)
            response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
            self._conn.execute(
                """
                INSERT INTO rest_ipc_acks(
                  generation,sender,sender_epoch,message_id,
                  request_sha256,response_json,created_wall_ms
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    self.generation,
                    row["sender"],
                    row["sender_epoch"],
                    row["message_id"],
                    row["request_sha256"],
                    response_json,
                    now_wall_ms,
                ),
            )
            self._conn.execute(
                "UPDATE rest_requests SET status='decided',response_json=?,"
                "decided_wall_ms=? WHERE seq=? AND status='pending'",
                (response_json, now_wall_ms, row["seq"]),
            )

    def _claim_initial_epoch(self) -> None:
        now = time_ns() // 1_000_000
        now_mono = monotonic_ns() // 1_000_000
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT * FROM coordinator_owner WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        """
                        INSERT INTO coordinator_owner(
                          singleton, generation, epoch, process_identity,
                          ordinary_weight, reserve_weight, policy_sha256,
                          clock_epoch, last_wall_ms, last_mono_ms, updated_wall_ms
                        ) VALUES (1, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                        """,
                        (
                            self.generation,
                            self.epoch,
                            self.process_identity,
                            self.ordinary_weight,
                            self.reserve_weight,
                            self.policy_sha256,
                            now,
                            now_mono,
                            now,
                        ),
                    )
                    self.clock_epoch = 1
                    self._insert_clock_epoch_locked(
                        clock_epoch=1,
                        now_wall_ms=now,
                        now_mono_ms=now_mono,
                        quarantine_ms=0,
                        prior_clock_epoch=0,
                        prior_last_wall_ms=0,
                        prior_last_mono_ms=0,
                        reason="initial_process_clock_epoch",
                    )
                elif (
                    row["generation"] != self.generation
                    or int(row["epoch"]) != self.epoch
                    or row["process_identity"] != self.process_identity
                    or int(row["ordinary_weight"]) != self.ordinary_weight
                    or int(row["reserve_weight"]) != self.reserve_weight
                    or row["policy_sha256"] != self.policy_sha256
                ):
                    raise RuntimeError(
                        "REST coordinator ledger owner or frozen policy does not match"
                    )
                else:
                    self.clock_epoch = int(row["clock_epoch"])
                    epoch_row = self._conn.execute(
                        "SELECT 1 FROM coordinator_clock_epochs "
                        "WHERE generation=? AND clock_epoch=?",
                        (self.generation, self.clock_epoch),
                    ).fetchone()
                    if epoch_row is None:
                        self._insert_clock_epoch_locked(
                            clock_epoch=self.clock_epoch,
                            now_wall_ms=int(row["last_wall_ms"]),
                            now_mono_ms=int(row["last_mono_ms"]),
                            quarantine_ms=0,
                            prior_clock_epoch=max(0, self.clock_epoch - 1),
                            prior_last_wall_ms=0,
                            prior_last_mono_ms=0,
                            reason="legacy_clock_epoch_adopted",
                        )
                    if (
                        now_mono < self._durable_mono_highwater_locked()
                        or self._clock_epoch_changed(
                            last_wall_ms=int(row["last_wall_ms"]),
                            last_mono_ms=int(row["last_mono_ms"]),
                            now_wall_ms=now,
                            now_mono_ms=now_mono,
                        )
                    ):
                        self._begin_new_clock_epoch_locked(
                            owner=row,
                            now_wall_ms=now,
                            now_mono_ms=now_mono,
                            reason="durable_wall_monotonic_epoch_change",
                        )
                        self._deny_pending_for_clock_epoch_locked(
                            now_wall_ms=now,
                            now_mono_ms=now_mono,
                        )
                    else:
                        self._conn.execute(
                            "UPDATE coordinator_owner SET last_wall_ms=?,last_mono_ms=?,"
                            "updated_wall_ms=? WHERE singleton=1",
                            (now, now_mono, now),
                        )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def request_grant(
        self,
        *,
        sender: str,
        sender_epoch: int,
        message_id: int,
        priority: RestPriority,
        endpoint: str,
        weight: int,
        now_wall_ms: int | None = None,
        now_mono_ms: int | None = None,
    ) -> RestGrant:
        if not sender or sender_epoch < 1 or message_id < 1 or not endpoint or weight < 1:
            raise ValueError("REST grant request fields are invalid")
        priority = RestPriority(priority)
        _validate_sender_policy(sender=sender, priority=priority, endpoint=endpoint, weight=weight)
        now = time_ns() // 1_000_000 if now_wall_ms is None else int(now_wall_ms)
        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else int(now_mono_ms)
        canonical_request = {
            "generation": self.generation,
            "sender": sender,
            "sender_epoch": sender_epoch,
            "message_id": message_id,
            "priority": int(priority),
            "endpoint": endpoint,
            "weight": weight,
        }
        request_hash = sha256(
            json.dumps(canonical_request, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        queued = False
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                egress_fence = self._conn.execute(
                    "SELECT generation FROM rest_egress_fence WHERE singleton=1"
                ).fetchone()
                if egress_fence is not None:
                    if str(egress_fence["generation"]) != self.generation:
                        raise RuntimeError("REST egress fence generation mismatch")
                    grant = self._denial(
                        sender=sender,
                        sender_epoch=sender_epoch,
                        message_id=message_id,
                        priority=priority,
                        endpoint=endpoint,
                        weight=weight,
                        now=now,
                        now_mono=now_mono,
                        reason="host_wide_launch_egress_fenced",
                    )
                    self._conn.commit()
                    return grant
                prior = self._conn.execute(
                    """
                    SELECT request_sha256, response_json FROM rest_ipc_acks
                    WHERE generation = ? AND sender = ? AND sender_epoch = ? AND message_id = ?
                    """,
                    (self.generation, sender, sender_epoch, message_id),
                ).fetchone()
                if prior is not None:
                    if prior["request_sha256"] != request_hash:
                        raise RuntimeError(
                            "REST IPC message ID was replayed with different content"
                        )
                    payload = json.loads(prior["response_json"])
                    self._conn.commit()
                    payload["priority"] = RestPriority(payload["priority"])
                    return RestGrant(**payload)
                pending = self._conn.execute(
                    """
                    SELECT request_sha256, status, response_json FROM rest_requests
                    WHERE generation = ? AND sender = ? AND sender_epoch = ? AND message_id = ?
                    """,
                    (self.generation, sender, sender_epoch, message_id),
                ).fetchone()
                if pending is not None:
                    if pending["request_sha256"] != request_hash:
                        raise RuntimeError(
                            "REST IPC message ID was replayed with different content"
                        )
                    if pending["status"] == "decided" and pending["response_json"]:
                        payload = json.loads(pending["response_json"])
                        self._conn.commit()
                        payload["priority"] = RestPriority(payload["priority"])
                        return RestGrant(**payload)
                    self._conn.commit()
                else:
                    sender_fence = self._conn.execute(
                        """
                        SELECT sender_epoch, max(message_id) AS last_message_id
                        FROM (
                          SELECT sender_epoch, message_id FROM rest_ipc_acks
                          WHERE generation=? AND sender=?
                          UNION ALL
                          SELECT sender_epoch, message_id FROM rest_requests
                          WHERE generation=? AND sender=?
                        )
                        GROUP BY sender_epoch
                        """,
                        (self.generation, sender, self.generation, sender),
                    ).fetchall()
                    if sender_fence:
                        if (
                            len(sender_fence) != 1
                            or int(sender_fence[0]["sender_epoch"]) != sender_epoch
                        ):
                            raise RuntimeError("REST sender epoch fence failed")
                        highwater = int(sender_fence[0]["last_message_id"])
                        if message_id < highwater - self.sender_reorder_window:
                            raise RuntimeError(
                                "REST sender message ID exceeded the bounded reorder window"
                            )
                    owner = self._conn.execute(
                        "SELECT * FROM coordinator_owner WHERE singleton = 1"
                    ).fetchone()
                    if owner is None or (
                        owner["generation"] != self.generation
                        or int(owner["epoch"]) != self.epoch
                        or owner["process_identity"] != self.process_identity
                        or int(owner["ordinary_weight"]) != self.ordinary_weight
                        or int(owner["reserve_weight"]) != self.reserve_weight
                        or owner["policy_sha256"] != self.policy_sha256
                        or int(owner["clock_epoch"]) != self.clock_epoch
                    ):
                        raise RuntimeError("REST coordinator epoch fence failed")
                    counts = self._conn.execute(
                        """
                        SELECT count(*) AS total,
                          sum(CASE WHEN priority=0 THEN 1 ELSE 0 END) AS critical
                        FROM rest_requests
                        WHERE generation=? AND status='pending'
                        """,
                        (self.generation,),
                    ).fetchone()
                    pending_total = int(counts["total"] or 0)
                    noncritical_capacity = self.max_pending_requests - self.critical_pending_reserve
                    queue_full = pending_total >= self.max_pending_requests or (
                        priority is not RestPriority.AMBIGUITY_CONTAINMENT
                        and pending_total >= noncritical_capacity
                    )
                    if queue_full:
                        grant = self._denial(
                            sender=sender,
                            sender_epoch=sender_epoch,
                            message_id=message_id,
                            priority=priority,
                            endpoint=endpoint,
                            weight=weight,
                            now=now,
                            now_mono=now_mono,
                            reason="durable_priority_queue_capacity_exhausted",
                        )
                        response = asdict(grant)
                        response["priority"] = int(grant.priority)
                        response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
                        self._conn.execute(
                            """
                            INSERT INTO rest_requests(
                              generation, sender, sender_epoch, message_id, priority,
                              endpoint, weight, request_sha256, requested_wall_ms,
                              requested_mono_ms, status, response_json, decided_wall_ms
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'decided', ?, ?)
                            """,
                            (
                                self.generation,
                                sender,
                                sender_epoch,
                                message_id,
                                int(priority),
                                endpoint,
                                weight,
                                request_hash,
                                now,
                                now_mono,
                                response_json,
                                now,
                            ),
                        )
                        self._conn.execute(
                            """
                            INSERT INTO rest_ipc_acks(
                              generation, sender, sender_epoch, message_id,
                              request_sha256, response_json, created_wall_ms
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                self.generation,
                                sender,
                                sender_epoch,
                                message_id,
                                request_hash,
                                response_json,
                                now,
                            ),
                        )
                        self._conn.commit()
                        return grant
                    self._conn.execute(
                        """
                        INSERT INTO rest_requests(
                          generation, sender, sender_epoch, message_id, priority,
                          endpoint, weight, request_sha256, requested_wall_ms,
                          requested_mono_ms, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                        """,
                        (
                            self.generation,
                            sender,
                            sender_epoch,
                            message_id,
                            int(priority),
                            endpoint,
                            weight,
                            request_hash,
                            now,
                            now_mono,
                        ),
                    )
                    self._conn.commit()
                    queued = True
            except Exception:
                self._conn.rollback()
                raise

        if queued and self.priority_coalesce_ms:
            threading.Event().wait(self.priority_coalesce_ms / 1_000)
        self._drain_pending_requests()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT request_sha256, response_json FROM rest_ipc_acks
                WHERE generation = ? AND sender = ? AND sender_epoch = ? AND message_id = ?
                """,
                (self.generation, sender, sender_epoch, message_id),
            ).fetchone()
        if row is None:
            raise RuntimeError("REST durable request was not decided")
        if row["request_sha256"] != request_hash:
            raise RuntimeError("REST IPC acknowledgement content fence failed")
        payload = json.loads(row["response_json"])
        payload["priority"] = RestPriority(payload["priority"])
        return RestGrant(**payload)

    def _drain_pending_requests(self) -> None:
        """Decide one durable batch in priority order under a single owner fence."""

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                owner = self._conn.execute(
                    "SELECT * FROM coordinator_owner WHERE singleton = 1"
                ).fetchone()
                if owner is None or (
                    owner["generation"] != self.generation
                    or int(owner["epoch"]) != self.epoch
                    or owner["process_identity"] != self.process_identity
                    or int(owner["ordinary_weight"]) != self.ordinary_weight
                    or int(owner["reserve_weight"]) != self.reserve_weight
                    or owner["policy_sha256"] != self.policy_sha256
                    or int(owner["clock_epoch"]) != self.clock_epoch
                ):
                    raise RuntimeError("REST coordinator epoch fence failed")
                pending = self._conn.execute(
                    """
                    SELECT * FROM rest_requests
                    WHERE generation=? AND status='pending'
                    ORDER BY priority ASC, seq ASC
                    """,
                    (self.generation,),
                ).fetchall()
                if not pending:
                    self._conn.commit()
                    return
                now = max(int(row["requested_wall_ms"]) for row in pending)
                now_mono = max(int(row["requested_mono_ms"]) for row in pending)
                last_wall = int(owner["last_wall_ms"])
                last_mono = int(owner["last_mono_ms"])
                wall_delta = now - last_wall
                mono_delta = now_mono - last_mono
                clock_jump = last_mono > 0 and (
                    mono_delta < 0 or abs(wall_delta - mono_delta) > 2_000
                )
                if clock_jump:
                    self._begin_new_clock_epoch_locked(
                        owner=owner,
                        now_wall_ms=now,
                        now_mono_ms=now_mono,
                        reason="runtime_wall_monotonic_epoch_change",
                    )
                quarantine_wait_ms = self._clock_quarantine_wait_locked(now_mono_ms=now_mono)
                for row in pending:
                    priority = RestPriority(int(row["priority"]))
                    grant = (
                        self._denial(
                            sender=str(row["sender"]),
                            sender_epoch=int(row["sender_epoch"]),
                            message_id=int(row["message_id"]),
                            priority=priority,
                            endpoint=str(row["endpoint"]),
                            weight=int(row["weight"]),
                            now=now,
                            now_mono=now_mono,
                            reason="monotonic_clock_epoch_quarantine",
                        )
                        if clock_jump or quarantine_wait_ms > 0
                        else self._allocate_locked(
                            sender=str(row["sender"]),
                            sender_epoch=int(row["sender_epoch"]),
                            message_id=int(row["message_id"]),
                            priority=priority,
                            endpoint=str(row["endpoint"]),
                            weight=int(row["weight"]),
                            now=now,
                            now_mono=now_mono,
                        )
                    )
                    response = asdict(grant)
                    response["priority"] = int(grant.priority)
                    response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
                    self._conn.execute(
                        """
                        INSERT INTO rest_ipc_acks(
                          generation, sender, sender_epoch, message_id,
                          request_sha256, response_json, created_wall_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.generation,
                            row["sender"],
                            row["sender_epoch"],
                            row["message_id"],
                            row["request_sha256"],
                            response_json,
                            now,
                        ),
                    )
                    self._conn.execute(
                        """
                        UPDATE rest_requests
                        SET status='decided', response_json=?, decided_wall_ms=?
                        WHERE seq=? AND status='pending'
                        """,
                        (response_json, now, row["seq"]),
                    )
                self._conn.execute(
                    """
                    UPDATE coordinator_owner
                    SET last_wall_ms=?, last_mono_ms=?, updated_wall_ms=?
                    WHERE singleton=1
                    """,
                    (now, now_mono, now),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _allocate_locked(
        self,
        *,
        sender: str,
        sender_epoch: int,
        message_id: int,
        priority: RestPriority,
        endpoint: str,
        weight: int,
        now: int,
        now_mono: int,
    ) -> RestGrant:
        cutoff = now_mono - REST_BUDGET_WINDOW_MS
        used = {
            row["pool"]: int(row["used"] or 0)
            for row in self._conn.execute(
                """
                SELECT pool, sum(weight) AS used FROM rest_grants
                WHERE generation = ? AND clock_epoch=? AND granted_mono_ms > ?
                GROUP BY pool
                """,
                (self.generation, self.clock_epoch, cutoff),
            ).fetchall()
        }
        ordinary_left = self.ordinary_weight - used.get("ordinary", 0)
        reserve_left = self.reserve_weight - used.get("reserve", 0)
        reserve_first = priority in {
            RestPriority.AMBIGUITY_CONTAINMENT,
            RestPriority.GAP_REPAIR,
        }
        reserve_eligible = reserve_first or priority is RestPriority.AFFECTED_FOLLOWER
        if reserve_first and weight <= reserve_left:
            pool = "reserve"
        elif weight <= ordinary_left:
            pool = "ordinary"
        elif reserve_eligible and weight <= reserve_left:
            pool = "reserve"
        else:
            return self._denial(
                sender=sender,
                sender_epoch=sender_epoch,
                message_id=message_id,
                priority=priority,
                endpoint=endpoint,
                weight=weight,
                now=now,
                now_mono=now_mono,
                reason="rolling_weight_budget_exhausted",
            )
        identity = (
            f"{self.generation}|{self.epoch}|{self.clock_epoch}|{sender}|"
            f"{sender_epoch}|{message_id}|{endpoint}|{weight}"
        )
        grant_id = "rest-grant-" + sha256(identity.encode("utf-8")).hexdigest()[:32]
        self._conn.execute(
            """
            INSERT INTO rest_grants(
              grant_id, generation, coordinator_epoch, sender, sender_epoch,
              message_id, priority, endpoint, weight, pool, granted_wall_ms
              , granted_mono_ms, clock_epoch
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                grant_id,
                self.generation,
                self.epoch,
                sender,
                sender_epoch,
                message_id,
                int(priority),
                endpoint,
                weight,
                pool,
                now,
                now_mono,
                self.clock_epoch,
            ),
        )
        return RestGrant(
            grant_id=grant_id,
            granted=True,
            generation=self.generation,
            coordinator_epoch=self.epoch,
            sender=sender,
            sender_epoch=sender_epoch,
            message_id=message_id,
            priority=priority,
            endpoint=endpoint,
            weight=weight,
            pool=pool,
            granted_wall_ms=now,
            retry_after_ms=0,
            reason="granted",
        )

    def _denial(
        self,
        *,
        sender: str,
        sender_epoch: int,
        message_id: int,
        priority: RestPriority,
        endpoint: str,
        weight: int,
        now: int,
        now_mono: int,
        reason: str,
    ) -> RestGrant:
        oldest = self._conn.execute(
            """
            SELECT min(granted_mono_ms) FROM rest_grants
            WHERE generation = ? AND clock_epoch=? AND granted_mono_ms > ?
            """,
            (
                self.generation,
                self.clock_epoch,
                now_mono - REST_BUDGET_WINDOW_MS,
            ),
        ).fetchone()[0]
        quarantine_wait_ms = self._clock_quarantine_wait_locked(now_mono_ms=now_mono)
        retry = max(
            quarantine_wait_ms,
            (
                1_000
                if oldest is None
                else min(
                    REST_BUDGET_WINDOW_MS + 1,
                    max(1, int(oldest) + REST_BUDGET_WINDOW_MS + 1 - now_mono),
                )
            ),
        )
        identity = f"deny|{self.generation}|{sender}|{sender_epoch}|{message_id}|{reason}"
        return RestGrant(
            grant_id="rest-denial-" + sha256(identity.encode("utf-8")).hexdigest()[:32],
            granted=False,
            generation=self.generation,
            coordinator_epoch=self.epoch,
            sender=sender,
            sender_epoch=sender_epoch,
            message_id=message_id,
            priority=priority,
            endpoint=endpoint,
            weight=weight,
            pool="none",
            granted_wall_ms=now,
            retry_after_ms=retry,
            reason=reason,
        )

    def takeover(
        self,
        *,
        expected_epoch: int,
        new_epoch: int,
        prior_process_identity: str,
        new_process_identity: str,
        proof: RestTakeoverProof,
    ) -> None:
        if not proof.complete:
            raise RuntimeError("REST coordinator takeover proof is incomplete")
        if new_epoch != expected_epoch + 1:
            raise ValueError("REST coordinator epoch must advance by exactly one")
        now = time_ns() // 1_000_000
        now_mono = monotonic_ns() // 1_000_000
        transition_id = (
            "rest-takeover-"
            + sha256(
                f"{self.generation}|{expected_epoch}|{new_epoch}|{new_process_identity}".encode(
                    "utf-8"
                )
            ).hexdigest()[:32]
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                owner = self._conn.execute(
                    "SELECT * FROM coordinator_owner WHERE singleton=1"
                ).fetchone()
                if owner is None or (
                    owner["generation"] != self.generation
                    or int(owner["ordinary_weight"]) != self.ordinary_weight
                    or int(owner["reserve_weight"]) != self.reserve_weight
                    or owner["policy_sha256"] != self.policy_sha256
                ):
                    raise RuntimeError("REST takeover frozen-policy fence failed")
                self.clock_epoch = int(owner["clock_epoch"])
                if (
                    self._conn.execute(
                        "SELECT 1 FROM coordinator_clock_epochs "
                        "WHERE generation=? AND clock_epoch=?",
                        (self.generation, self.clock_epoch),
                    ).fetchone()
                    is None
                ):
                    self._insert_clock_epoch_locked(
                        clock_epoch=self.clock_epoch,
                        now_wall_ms=int(owner["last_wall_ms"]),
                        now_mono_ms=int(owner["last_mono_ms"]),
                        quarantine_ms=0,
                        prior_clock_epoch=max(0, self.clock_epoch - 1),
                        prior_last_wall_ms=0,
                        prior_last_mono_ms=0,
                        reason="legacy_clock_epoch_adopted_during_takeover",
                    )
                if now_mono < self._durable_mono_highwater_locked() or self._clock_epoch_changed(
                    last_wall_ms=int(owner["last_wall_ms"]),
                    last_mono_ms=int(owner["last_mono_ms"]),
                    now_wall_ms=now,
                    now_mono_ms=now_mono,
                ):
                    self._begin_new_clock_epoch_locked(
                        owner=owner,
                        now_wall_ms=now,
                        now_mono_ms=now_mono,
                        reason="takeover_wall_monotonic_epoch_change",
                    )
                    owner = self._conn.execute(
                        "SELECT * FROM coordinator_owner WHERE singleton=1"
                    ).fetchone()
                    assert owner is not None
                prior_grant_count = int(
                    self._conn.execute(
                        "SELECT count(*) FROM rest_grants WHERE generation=?",
                        (self.generation,),
                    ).fetchone()[0]
                )
                cur = self._conn.execute(
                    """
                    UPDATE coordinator_owner
                    SET epoch = ?, process_identity = ?, last_wall_ms=?,
                        last_mono_ms=?, updated_wall_ms = ?
                    WHERE singleton = 1 AND generation = ? AND epoch = ?
                      AND process_identity = ? AND ordinary_weight = ?
                      AND reserve_weight = ? AND policy_sha256 = ?
                    """,
                    (
                        new_epoch,
                        new_process_identity,
                        now,
                        now_mono,
                        now,
                        self.generation,
                        expected_epoch,
                        prior_process_identity,
                        self.ordinary_weight,
                        self.reserve_weight,
                        self.policy_sha256,
                    ),
                )
                if cur.rowcount != 1:
                    raise RuntimeError("REST coordinator takeover CAS failed")
                self._conn.execute(
                    """
                    INSERT INTO coordinator_takeovers(
                      transition_id, generation, from_epoch, to_epoch,
                      from_process_identity, to_process_identity, proof_json, created_wall_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        transition_id,
                        self.generation,
                        expected_epoch,
                        new_epoch,
                        prior_process_identity,
                        new_process_identity,
                        json.dumps(asdict(proof), sort_keys=True, separators=(",", ":")),
                        now,
                    ),
                )
                pending = self._conn.execute(
                    """
                    SELECT * FROM rest_requests
                    WHERE generation=? AND status='pending'
                    ORDER BY seq
                    """,
                    (self.generation,),
                ).fetchall()
                for row in pending:
                    grant = self._denial(
                        sender=str(row["sender"]),
                        sender_epoch=int(row["sender_epoch"]),
                        message_id=int(row["message_id"]),
                        priority=RestPriority(int(row["priority"])),
                        endpoint=str(row["endpoint"]),
                        weight=int(row["weight"]),
                        now=now,
                        now_mono=now_mono,
                        reason="coordinator_takeover_requires_exact_request_replay",
                    )
                    response = asdict(grant)
                    response["priority"] = int(grant.priority)
                    response_json = json.dumps(response, sort_keys=True, separators=(",", ":"))
                    self._conn.execute(
                        """
                        INSERT INTO rest_ipc_acks(
                          generation, sender, sender_epoch, message_id,
                          request_sha256, response_json, created_wall_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            self.generation,
                            row["sender"],
                            row["sender_epoch"],
                            row["message_id"],
                            row["request_sha256"],
                            response_json,
                            now,
                        ),
                    )
                    self._conn.execute(
                        """
                        UPDATE rest_requests
                        SET status='decided', response_json=?, decided_wall_ms=?
                        WHERE seq=? AND status='pending'
                        """,
                        (response_json, now, row["seq"]),
                    )
                retained_grant_count = int(
                    self._conn.execute(
                        "SELECT count(*) FROM rest_grants WHERE generation=?",
                        (self.generation,),
                    ).fetchone()[0]
                )
                if retained_grant_count != prior_grant_count:
                    raise RuntimeError("REST takeover did not retain the prior grant ledger")
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        self.epoch = new_epoch
        self.process_identity = new_process_identity

    def usage(
        self,
        *,
        now_wall_ms: int | None = None,
        now_mono_ms: int | None = None,
    ) -> dict[str, int]:
        del now_wall_ms
        now_mono = monotonic_ns() // 1_000_000 if now_mono_ms is None else now_mono_ms
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT pool, coalesce(sum(weight), 0) AS used FROM rest_grants
                WHERE generation = ? AND clock_epoch=? AND granted_mono_ms > ?
                GROUP BY pool
                """,
                (
                    self.generation,
                    self.clock_epoch,
                    now_mono - REST_BUDGET_WINDOW_MS,
                ),
            ).fetchall()
            pending = self._conn.execute(
                """
                SELECT count(*) AS total,
                  sum(CASE WHEN priority=0 THEN 1 ELSE 0 END) AS critical
                FROM rest_requests WHERE generation=? AND status='pending'
                """,
                (self.generation,),
            ).fetchone()
        used = {str(row["pool"]): int(row["used"]) for row in rows}
        return {
            "ordinary_used": used.get("ordinary", 0),
            "ordinary_remaining": self.ordinary_weight - used.get("ordinary", 0),
            "reserve_used": used.get("reserve", 0),
            "reserve_remaining": self.reserve_weight - used.get("reserve", 0),
            "pending_requests": int(pending["total"] or 0),
            "pending_critical": int(pending["critical"] or 0),
            "clock_epoch": self.clock_epoch,
            "clock_quarantine_remaining_ms": self._clock_quarantine_remaining(
                now_mono_ms=int(now_mono)
            ),
        }

    def _clock_quarantine_remaining(self, *, now_mono_ms: int) -> int:
        with self._lock:
            return self._clock_quarantine_wait_locked(now_mono_ms=now_mono_ms)


class RestBudgetPipeServer:
    """Authenticated Windows named-pipe facade over a coordinator."""

    def __init__(
        self,
        coordinator: RestBudgetCoordinator,
        *,
        pipe_name: str,
        auth_secret: bytes,
    ) -> None:
        if os.name != "nt":
            raise RuntimeError("REST budget named pipes are supported only on Windows")
        if not pipe_name or len(auth_secret) < 32:
            raise ValueError("named-pipe identity and 256-bit auth secret are required")
        self.coordinator = coordinator
        self.address = rf"\\.\pipe\{pipe_name}"
        self.auth_secret = auth_secret
        self._stop = threading.Event()
        self._worker_slots = threading.BoundedSemaphore(32)
        self._workers: set[threading.Thread] = set()
        self._workers_lock = threading.Lock()

    def stop(self) -> None:
        self._stop.set()

    def serve(self) -> None:
        from multiprocessing.connection import Listener

        with Listener(self.address, family="AF_PIPE", authkey=self.auth_secret) as listener:
            while not self._stop.is_set():
                connection = listener.accept()
                if not self._worker_slots.acquire(blocking=False):
                    try:
                        connection.send({"ok": False, "error": "REST pipe worker limit reached"})
                    finally:
                        connection.close()
                    continue
                worker = threading.Thread(
                    target=self._serve_connection,
                    args=(connection,),
                    name="rest-budget-pipe-client",
                    daemon=True,
                )
                with self._workers_lock:
                    self._workers.add(worker)
                worker.start()
        with self._workers_lock:
            workers = list(self._workers)
        for worker in workers:
            worker.join(timeout=3)

    def _serve_connection(self, connection: Any) -> None:
        try:
            if not connection.poll(2):
                raise TimeoutError("REST pipe client did not send a bounded request")
            request = connection.recv()
            response = self._handle(request)
            connection.send(response)
        except Exception as exc:
            try:
                connection.send({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
            except Exception:
                pass
        finally:
            connection.close()
            self._worker_slots.release()
            with self._workers_lock:
                self._workers.discard(threading.current_thread())

    def _handle(self, request: Any) -> dict[str, Any]:
        if not isinstance(request, Mapping):
            raise ValueError("REST pipe request must be an object")
        if request.get("generation") != self.coordinator.generation:
            raise RuntimeError("REST pipe generation fence failed")
        grant = self.coordinator.request_grant(
            sender=str(request["sender"]),
            sender_epoch=int(request["sender_epoch"]),
            message_id=int(request["message_id"]),
            priority=RestPriority(int(request["priority"])),
            endpoint=str(request["endpoint"]),
            weight=int(request["weight"]),
        )
        payload = asdict(grant)
        payload["priority"] = int(grant.priority)
        return {"ok": True, "grant": payload}


class RestBudgetTransportError(RuntimeError):
    """The fenced coordinator transport failed before a grant was returned."""


class RestBudgetPipeClient:
    def __init__(
        self,
        *,
        pipe_name: str,
        auth_secret: bytes,
        generation: str,
        sender: str,
        sender_epoch: int,
        message_state_path: Path | str | None = None,
        request_timeout_s: float = 5.0,
    ) -> None:
        if os.name != "nt":
            raise RuntimeError("REST budget named pipes are supported only on Windows")
        if (
            not pipe_name
            or len(auth_secret) < 32
            or not generation
            or not sender
            or request_timeout_s <= 0
        ):
            raise ValueError("REST pipe client identity is incomplete")
        self.address = rf"\\.\pipe\{pipe_name}"
        self.auth_secret = auth_secret
        self.generation = generation
        self.sender = sender
        self.sender_epoch = sender_epoch
        self.request_timeout_s = request_timeout_s
        self._message_id = 0
        self._lock = threading.Lock()
        self._state: sqlite3.Connection | None = None
        if message_state_path is not None:
            state_path = Path(message_state_path).resolve()
            state_path.parent.mkdir(parents=True, exist_ok=True)
            self._state = sqlite3.connect(state_path, check_same_thread=False)
            with self._state:
                self._state.execute("PRAGMA journal_mode=WAL")
                self._state.execute("PRAGMA synchronous=FULL")
                self._state.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sender_message_ids(
                      sender TEXT NOT NULL,
                      sender_epoch INTEGER NOT NULL,
                      next_message_id INTEGER NOT NULL,
                      PRIMARY KEY(sender, sender_epoch)
                    )
                    """
                )

    def request_grant(
        self,
        *,
        priority: RestPriority,
        endpoint: str,
        weight: int,
    ) -> RestGrant:
        from multiprocessing.connection import Client, wait

        with self._lock:
            if self._state is None:
                self._message_id += 1
                message_id = self._message_id
            else:
                self._state.execute("BEGIN IMMEDIATE")
                try:
                    row = self._state.execute(
                        """
                        SELECT next_message_id FROM sender_message_ids
                        WHERE sender=? AND sender_epoch=?
                        """,
                        (self.sender, self.sender_epoch),
                    ).fetchone()
                    message_id = 1 if row is None else int(row[0])
                    self._state.execute(
                        """
                        INSERT INTO sender_message_ids(sender, sender_epoch, next_message_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT(sender, sender_epoch) DO UPDATE SET
                          next_message_id=excluded.next_message_id
                        """,
                        (self.sender, self.sender_epoch, message_id + 1),
                    )
                    self._state.commit()
                except Exception:
                    self._state.rollback()
                    raise
        request = {
            "generation": self.generation,
            "sender": self.sender,
            "sender_epoch": self.sender_epoch,
            "message_id": message_id,
            "priority": int(priority),
            "endpoint": endpoint,
            "weight": weight,
        }
        try:
            connection = Client(self.address, family="AF_PIPE", authkey=self.auth_secret)
            try:
                connection.send(request)
                if not wait([connection], timeout=self.request_timeout_s):
                    raise TimeoutError("REST grant pipe response timed out")
                response = connection.recv()
            finally:
                connection.close()
        except (OSError, EOFError, TimeoutError, ConnectionError) as exc:
            raise RestBudgetTransportError(
                f"REST grant pipe transport failed: {type(exc).__name__}: {exc}"
            ) from exc
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            raise RuntimeError(f"REST grant pipe failed: {response}")
        raw = response.get("grant")
        if not isinstance(raw, Mapping):
            raise RuntimeError("REST grant pipe returned malformed response")
        payload = dict(raw)
        payload["priority"] = RestPriority(int(payload["priority"]))
        return RestGrant(**payload)


def derive_pipe_auth_secret(master_secret: bytes, generation: str) -> bytes:
    if len(master_secret) < 32 or not generation:
        raise ValueError("pipe auth derivation requires a 256-bit secret and generation")
    return hmac.new(master_secret, generation.encode("utf-8"), sha256).digest()
