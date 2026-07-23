from __future__ import annotations

import json
import sqlite3
from hashlib import sha256
from threading import RLock
from pathlib import Path
from typing import Any, Iterable, Mapping

from .account_state import StateProvenance
from .cloid import deterministic_cloid, validate_cloid
from .markets import canonical_market_symbol, market_dex
from .models import (
    DesiredState,
    ExecutionAttemptPhase,
    ExecutionReport,
    FollowerIntent,
    Mode,
    Position,
    ReconcileSnapshot,
    SafeModeTransition,
    SourceEvent,
    SourceEventType,
    now_ms,
    parse_decimal,
    to_jsonable,
)
from .security import is_sensitive_key, redact_secrets


SCHEMA_VERSION = 10
UNRESOLVED_ATTEMPT_PHASES = (
    ExecutionAttemptPhase.PREPARED.value,
    ExecutionAttemptPhase.DISPATCHING.value,
    ExecutionAttemptPhase.UNKNOWN.value,
    ExecutionAttemptPhase.LEGACY_UNRESOLVED.value,
)
TERMINAL_REPORT_STATUSES = ("filled", "canceled", "rejected", "skipped")
SOURCE_REACTION_OPEN_STATUSES = (
    "pending",
    "processing",
    "blocked",
    "failed",
    "legacy_unverified",
)
SOURCE_REACTION_FINAL_STATUSES = ("completed", "ignored")
SOURCE_REACTION_CLAIMABLE_STATUSES = tuple(
    status for status in SOURCE_REACTION_OPEN_STATUSES if status != "processing"
)
HIP3_LIQUIDITY_RETRY_CLASS = "hip3_liquidity"
HIP3_IOC_ZERO_FILL_PROOF_KIND = "hip3_ioc_zero_fill_v1"
HIP3_IOC_NO_MATCH_ERROR = "Order could not immediately match against any resting orders."
HIP3_IOC_SYNC_NO_MATCH_EVIDENCE_SOURCE = "synchronous_ioc_no_match_rejection"
HIP3_IOC_UNKNOWN_OID_STATUS = "unknownOid"
HIP3_IOC_UNKNOWN_OID_CONFIRMATION_COUNT = 3
MAX_FUTURE_OBSERVATION_MS = 1_000
HIP3_IOC_ZERO_FILL_NEUTRAL_STATUSES = frozenset(
    {
        "hip3_ioc_no_fill_deferred",
        "hip3_ioc_no_fill_cleanup_retry",
        "settled:hip3_ioc_no_fill",
        "watchdog_settled:hip3_ioc_no_fill",
    }
)


def _stored_execution_report(row: dict[str, Any]) -> dict[str, Any] | None:
    raw = row.get("payload_json")
    if not isinstance(raw, str):
        return None
    try:
        stored = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(stored, dict):
        return None
    return stored


def _stored_execution_payload(row: dict[str, Any]) -> dict[str, Any] | None:
    stored = _stored_execution_report(row)
    if stored is None:
        return None
    payload = stored.get("payload")
    return payload if isinstance(payload, dict) else None


def _canonical_hip3_ioc_no_match_error(response: Any) -> str | None:
    if not isinstance(response, dict) or str(response.get("status") or "").lower() != "ok":
        return None
    inner = response.get("response")
    if not isinstance(inner, dict) or str(inner.get("type") or "").lower() != "order":
        return None
    data = inner.get("data")
    statuses = data.get("statuses") if isinstance(data, dict) else None
    if not isinstance(statuses, list) or len(statuses) != 1:
        return None
    status = statuses[0]
    if not isinstance(status, dict) or set(status) != {"error"}:
        return None
    error = status.get("error")
    if not isinstance(error, str):
        return None
    if error == HIP3_IOC_NO_MATCH_ERROR:
        return error
    prefix = HIP3_IOC_NO_MATCH_ERROR + " asset="
    return error if error.startswith(prefix) and error[len(prefix) :].isdecimal() else None


def _strict_persisted_hip3_ioc_synchronous_no_match(
    row: dict[str, Any],
    payload: dict[str, Any],
    proof: dict[str, Any],
    retry_identity: dict[str, Any],
    top_level_proof_id: str,
) -> dict[str, Any] | None:
    """Validate the exact rejected-before-OID evidence used by live HIP-3 IOCs."""

    stored_report = _stored_execution_report(row)
    if stored_report is None:
        return None
    try:
        cloid = validate_cloid(str(row.get("cloid") or ""))
        proof_id = validate_cloid(top_level_proof_id)
        nested_proof_id = validate_cloid(str(proof.get("proof_id") or ""))
        proof_cloid = validate_cloid(str(proof.get("cloid") or ""))
        base_cloid = validate_cloid(str(retry_identity.get("base_cloid") or ""))
        attempt_cloid = validate_cloid(str(retry_identity.get("attempt_cloid") or ""))
        predecessor_raw = retry_identity.get("predecessor_zero_fill_proof_id")
        predecessor = (
            validate_cloid(str(predecessor_raw)) if predecessor_raw not in {None, ""} else None
        )
        filled_size = parse_decimal(payload.get("filled_size"))
        size = parse_decimal(proof.get("size"))
        price = parse_decimal(proof.get("price"))
        proof_coin = canonical_market_symbol(str(proof.get("coin") or ""))
        attempt_not_before_ms = int(str(proof.get("attempt_not_before_ms")))
        response_observed_ms = int(str(proof.get("response_observed_ms")))
        proof_observed_ms = int(str(proof.get("proof_observed_ms")))
        report_exchange_ts_ms = int(str(stored_report.get("exchange_ts_ms")))
    except (ArithmeticError, TypeError, ValueError):
        return None
    expected_attempt_cloid = (
        deterministic_cloid(
            "hip3-ioc-zero-fill-retry",
            base_cloid,
            predecessor,
        )
        if predecessor is not None
        else base_cloid
    )
    source_report_id = str(proof.get("source_report_id") or "")
    stage = str(proof.get("stage") or "")
    stored_report_id = str(stored_report.get("report_id") or "")
    side = str(proof.get("side") or "").strip().lower()
    reduce_only = proof.get("reduce_only")
    expected_stored_report_id = deterministic_cloid(
        "hip3-ioc-zero-fill-report",
        source_report_id,
        proof_id,
        stage,
    )
    if (
        filled_size != 0
        or size <= 0
        or price <= 0
        or not market_dex(proof_coin)
        or proof_id != nested_proof_id
        or proof_cloid != cloid
        or attempt_cloid != cloid
        or expected_attempt_cloid != cloid
        or side not in {"buy", "sell"}
        or not isinstance(reduce_only, bool)
        or payload.get("requires_post_action_reconcile") is not True
        or attempt_not_before_ms <= 0
        or response_observed_ms <= 0
        or response_observed_ms < attempt_not_before_ms - MAX_FUTURE_OBSERVATION_MS
        or proof_observed_ms < response_observed_ms - MAX_FUTURE_OBSERVATION_MS
        or not source_report_id
        or not stage
        or stored_report_id != expected_stored_report_id
        or response_observed_ms != report_exchange_ts_ms
    ):
        return None
    request = proof.get("order_request")
    response = proof.get("response")
    confirmations = proof.get("unknown_oid_confirmations")
    if (
        not isinstance(request, dict)
        or not isinstance(response, dict)
        or payload.get("order_request") != request
        or payload.get("response") != response
        or _canonical_hip3_ioc_no_match_error(response) is None
        or not isinstance(confirmations, list)
        or len(confirmations) != HIP3_IOC_UNKNOWN_OID_CONFIRMATION_COUNT
    ):
        return None
    try:
        request_coin = canonical_market_symbol(str(request.get("coin") or ""))
        request_size = parse_decimal(request.get("size"))
        request_price = parse_decimal(request.get("price"))
        expected_size = parse_decimal(payload.get("expected_size"))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if (
        request_coin != proof_coin
        or str(request.get("side") or "").lower() != side
        or request_size != size
        or expected_size != size
        or request_price != price
        or request.get("reduce_only") is not reduce_only
        or str(request.get("tif") or "").lower() != "ioc"
    ):
        return None
    previous_observed_ms = 0
    for confirmation in confirmations:
        if not isinstance(confirmation, dict) or set(confirmation) != {
            "queried_cloid",
            "observed_ms",
            "payload",
        }:
            return None
        try:
            queried_cloid = validate_cloid(str(confirmation.get("queried_cloid") or ""))
            observed_ms = int(str(confirmation.get("observed_ms")))
        except (TypeError, ValueError):
            return None
        status_payload = confirmation.get("payload")
        if (
            queried_cloid != cloid
            or observed_ms < attempt_not_before_ms - MAX_FUTURE_OBSERVATION_MS
            or observed_ms < previous_observed_ms
            or observed_ms > proof_observed_ms
            or not isinstance(status_payload, dict)
            or set(status_payload) != {"status"}
            or status_payload.get("status") != HIP3_IOC_UNKNOWN_OID_STATUS
        ):
            return None
        previous_observed_ms = observed_ms
    if previous_observed_ms != proof_observed_ms:
        return None
    expected_proof_id = deterministic_cloid(
        "hip3-ioc-sync-no-match-proof-v1",
        source_report_id,
        cloid,
        attempt_not_before_ms,
        response_observed_ms,
        stage,
        request,
        response,
        confirmations,
    )
    if expected_proof_id != proof_id:
        return None
    return {
        "base_cloid": base_cloid,
        "attempt_cloid": attempt_cloid,
        "predecessor_zero_fill_proof_id": predecessor,
        "proof_id": proof_id,
    }


def _strict_persisted_hip3_ioc_zero_fill(
    row: dict[str, Any],
) -> dict[str, Any] | None:
    """Validate a durable zero-fill proof before it can relax runtime controls."""

    if (
        str(row.get("status") or "").lower() != "rejected"
        or str(row.get("exchange_status") or "") not in HIP3_IOC_ZERO_FILL_NEUTRAL_STATUSES
    ):
        return None
    payload = _stored_execution_payload(row)
    if (
        payload is None
        or payload.get("signed_action_performed") is not True
        or payload.get("proven_zero_fill") is not True
        or "filled_size" not in payload
    ):
        return None
    proof = payload.get("zero_fill_proof")
    retry_identity = payload.get("post_send_retry_identity")
    top_level_proof_id = payload.get("zero_fill_proof_id")
    if (
        not isinstance(proof, dict)
        or not isinstance(retry_identity, dict)
        or proof.get("kind") != HIP3_IOC_ZERO_FILL_PROOF_KIND
        or not isinstance(top_level_proof_id, str)
    ):
        return None
    evidence_source = proof.get("evidence_source")
    if evidence_source == HIP3_IOC_SYNC_NO_MATCH_EVIDENCE_SOURCE:
        return _strict_persisted_hip3_ioc_synchronous_no_match(
            row,
            payload,
            proof,
            retry_identity,
            top_level_proof_id,
        )
    if evidence_source is not None:
        return None
    try:
        cloid = validate_cloid(str(row.get("cloid") or ""))
        proof_id = validate_cloid(top_level_proof_id)
        nested_proof_id = validate_cloid(str(proof.get("proof_id") or ""))
        proof_cloid = validate_cloid(str(proof.get("cloid") or ""))
        base_cloid = validate_cloid(str(retry_identity.get("base_cloid") or ""))
        attempt_cloid = validate_cloid(str(retry_identity.get("attempt_cloid") or ""))
        predecessor_raw = retry_identity.get("predecessor_zero_fill_proof_id")
        predecessor = (
            validate_cloid(str(predecessor_raw)) if predecessor_raw not in {None, ""} else None
        )
        filled_size = parse_decimal(payload.get("filled_size"))
        size = parse_decimal(proof.get("size"))
        price = parse_decimal(proof.get("price"))
        oid = int(str(proof.get("oid")))
        order_timestamp = int(str(proof.get("order_timestamp")))
        status_timestamp = int(str(proof.get("status_timestamp")))
        proof_coin = canonical_market_symbol(str(proof.get("coin") or ""))
    except (ArithmeticError, TypeError, ValueError):
        return None
    expected_attempt_cloid = (
        deterministic_cloid(
            "hip3-ioc-zero-fill-retry",
            base_cloid,
            predecessor,
        )
        if predecessor is not None
        else base_cloid
    )
    if (
        filled_size != 0
        or size <= 0
        or price <= 0
        or oid <= 0
        or order_timestamp <= 0
        or status_timestamp < order_timestamp
        or proof_id != nested_proof_id
        or proof_cloid != cloid
        or attempt_cloid != cloid
        or expected_attempt_cloid != cloid
        or deterministic_cloid(
            "hip3-ioc-zero-fill-proof",
            cloid,
            oid,
            status_timestamp,
        )
        != proof_id
    ):
        return None
    side = str(proof.get("side") or "").strip().lower()
    reduce_only = proof.get("reduce_only")
    if side not in {"buy", "sell"} or not isinstance(reduce_only, bool):
        return None
    status_payload = proof.get("order_status")
    if (
        not isinstance(status_payload, dict)
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
        order_coin = canonical_market_symbol(str(order.get("coin") or ""))
        original_size = parse_decimal(order.get("origSz"))
        remaining_size = parse_decimal(order.get("sz"))
        limit_price = parse_decimal(order.get("limitPx"))
        order_oid = int(str(order.get("oid")))
        order_created_ms = int(str(order.get("timestamp")))
        order_status_ms = int(str(wrapper.get("statusTimestamp")))
    except (ArithmeticError, TypeError, ValueError):
        return None
    if (
        order_coin != proof_coin
        or str(order.get("cloid") or "").lower() != cloid
        or str(order.get("side") or "").upper() != ("B" if side == "buy" else "A")
        or str(order.get("tif") or "").lower() != "ioc"
        or str(order.get("orderType") or "").lower() != "limit"
        or order.get("reduceOnly") is not reduce_only
        or original_size != size
        or remaining_size != original_size
        or limit_price != price
        or order_oid != oid
        or order_created_ms != order_timestamp
        or order_status_ms != status_timestamp
    ):
        return None
    return {
        "proof_id": proof_id,
        "base_cloid": base_cloid,
        "attempt_cloid": attempt_cloid,
    }


def _source_reaction_due_expression(prefix: str = "") -> str:
    """Return the SQL predicate for optionally paced reaction claims.

    Only a blocked reaction with a valid typed HIP-3 retry deadline may wait.
    Missing, malformed, or unrelated outcomes retain the historical immediately
    claimable behavior.
    """

    return f"""
        CASE
          WHEN {prefix}status = 'blocked'
           AND json_valid({prefix}outcome_json)
          THEN CASE
            WHEN json_extract({prefix}outcome_json, '$.retry.class') = ?
             AND json_type(
                   {prefix}outcome_json,
                   '$.retry.retry_not_before_ms'
                 ) = 'integer'
            THEN CAST(
                   json_extract(
                     {prefix}outcome_json,
                     '$.retry.retry_not_before_ms'
                   ) AS INTEGER
                 ) <= ?
            ELSE 1
          END
          ELSE 1
        END = 1
    """


def _report_is_proven_no_send(*, status: str, exchange_status: str, action: str) -> bool:
    return status == "skipped" and (
        exchange_status == "recovered:never_dispatched"
        or exchange_status == "pre_send_blocked"
        or exchange_status.startswith("blocked:")
        or (action == "noop" and exchange_status == "skipped")
    )


class JournalIntegrityError(RuntimeError):
    pass


class SignedDispatchExpired(JournalIntegrityError):
    """A provably unsent signature lost its required transaction-time margin."""

    pass


class SQLiteStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.initialize()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def initialize(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            # Every acknowledgement on the execution journal is a durability
            # boundary.  Set this explicitly instead of relying on SQLite's
            # connection default so a future environment change cannot weaken
            # durable-before-send semantics.
            cur.execute("PRAGMA synchronous=FULL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA busy_timeout=5000")
            prior_schema_version = self._read_schema_version(cur)
            self._check_schema_version(prior_schema_version)
            cur.executescript(
                """
            CREATE TABLE IF NOT EXISTS schema_meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS source_events (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              idempotency_key TEXT NOT NULL UNIQUE,
              event_type TEXT NOT NULL,
              exchange_ts_ms INTEGER NOT NULL,
              observed_ts_ms INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_source_events_wallet_seq
              ON source_events(lower(json_extract(payload_json, '$.source_wallet')), seq DESC)
              WHERE json_valid(payload_json);

            CREATE TABLE IF NOT EXISTS source_event_reactions (
              source_event_key TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              outcome_json TEXT NOT NULL DEFAULT '{}',
              result_id TEXT NOT NULL DEFAULT '',
              created_ms INTEGER NOT NULL,
              updated_ms INTEGER NOT NULL,
              FOREIGN KEY(source_event_key) REFERENCES source_events(idempotency_key)
            );

            CREATE INDEX IF NOT EXISTS idx_source_event_reactions_status_updated
              ON source_event_reactions(status, updated_ms);

            CREATE TABLE IF NOT EXISTS desired_states (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              state_id TEXT NOT NULL UNIQUE,
              source_event_key TEXT NOT NULL,
              mode TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS desired_state_commits (
              state_id TEXT PRIMARY KEY,
              committed_ms INTEGER NOT NULL,
              FOREIGN KEY(state_id) REFERENCES desired_states(state_id)
            );

            CREATE INDEX IF NOT EXISTS idx_desired_states_scope_seq
              ON desired_states(
                mode,
                lower(json_extract(payload_json, '$.source_wallet')),
                lower(json_extract(payload_json, '$.action_account')),
                json_extract(payload_json, '$.source_network'),
                seq DESC
              )
              WHERE json_valid(payload_json);

            CREATE TABLE IF NOT EXISTS follower_intents (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              intent_id TEXT NOT NULL UNIQUE,
              cloid TEXT NOT NULL UNIQUE,
              desired_state_id TEXT NOT NULL DEFAULT '',
              source_event_key TEXT NOT NULL,
              action TEXT NOT NULL,
              coin TEXT NOT NULL,
              mode TEXT NOT NULL,
              status TEXT NOT NULL,
              attempt_phase TEXT NOT NULL DEFAULT 'legacy_unresolved',
              attempt_updated_ms INTEGER NOT NULL DEFAULT 0,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS execution_reports (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              report_id TEXT NOT NULL UNIQUE,
              intent_id TEXT NOT NULL,
              cloid TEXT NOT NULL,
              status TEXT NOT NULL,
              exchange_status TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signed_action_attempts (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              attempt_id TEXT NOT NULL UNIQUE,
              intent_id TEXT NOT NULL,
              cloid TEXT NOT NULL UNIQUE,
              action TEXT NOT NULL,
              mode TEXT NOT NULL,
              account TEXT NOT NULL,
              network TEXT NOT NULL,
              attempt_phase TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              result_json TEXT NOT NULL DEFAULT '{}',
              created_ms INTEGER NOT NULL,
              updated_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signed_action_attempts_scope_phase
              ON signed_action_attempts(mode, lower(account), network, attempt_phase, seq);

            CREATE TABLE IF NOT EXISTS reconcile_snapshots (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              snapshot_id TEXT NOT NULL UNIQUE,
              account TEXT NOT NULL,
              source TEXT NOT NULL,
              observed_ms INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_reconcile_snapshots_account_seq
              ON reconcile_snapshots(lower(account), seq DESC);

            CREATE TABLE IF NOT EXISTS safe_mode_transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              transition_id TEXT NOT NULL UNIQUE,
              enabled INTEGER NOT NULL,
              reason TEXT NOT NULL,
              detail TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS config_revisions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              revision_id TEXT NOT NULL UNIQUE,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_leases (
              name TEXT PRIMARY KEY,
              owner TEXT NOT NULL,
              expires_ms INTEGER NOT NULL,
              updated_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runner_heartbeats (
              instance_id TEXT PRIMARY KEY,
              role TEXT NOT NULL,
              mode TEXT NOT NULL,
              source_wallet TEXT NOT NULL,
              action_account TEXT NOT NULL,
              config_revision_id TEXT NOT NULL,
              status TEXT NOT NULL,
              detail TEXT NOT NULL,
              started_ms INTEGER NOT NULL,
              heartbeat_ms INTEGER NOT NULL,
              expires_ms INTEGER NOT NULL,
              cycle_count INTEGER NOT NULL,
              last_cycle_ms INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_runner_heartbeats_scope
              ON runner_heartbeats(mode, source_wallet, action_account, heartbeat_ms DESC);

            CREATE TABLE IF NOT EXISTS control_audit (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              control TEXT NOT NULL,
              status TEXT NOT NULL,
              detail TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reaction_results (
              result_id TEXT PRIMARY KEY,
              disposition TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stream_partitions (
              partition_key TEXT PRIMARY KEY,
              stream_state TEXT NOT NULL,
              ingress_cursor INTEGER NOT NULL DEFAULT 0,
              applied_cursor INTEGER NOT NULL DEFAULT 0,
              last_frame_wall_ms INTEGER NOT NULL DEFAULT 0,
              last_valid_event_wall_ms INTEGER NOT NULL DEFAULT 0,
              last_durable_checkpoint_wall_ms INTEGER NOT NULL DEFAULT 0,
              gap_detail TEXT NOT NULL DEFAULT '',
              generation TEXT NOT NULL DEFAULT '',
              updated_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS stream_state_transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              partition_key TEXT NOT NULL,
              generation TEXT NOT NULL,
              from_state TEXT NOT NULL,
              to_state TEXT NOT NULL,
              gap_detail TEXT NOT NULL DEFAULT '',
              wall_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_stream_state_transitions_partition
              ON stream_state_transitions(partition_key, wall_ms, seq);

            CREATE TABLE IF NOT EXISTS runtime_events (
              ingress_seq INTEGER PRIMARY KEY AUTOINCREMENT,
              event_key TEXT NOT NULL UNIQUE,
              partition_key TEXT NOT NULL,
              event_class TEXT NOT NULL,
              exchange_ts_ms INTEGER NOT NULL,
              receive_wall_ms INTEGER NOT NULL,
              receive_mono_ns INTEGER NOT NULL,
              seed_snapshot INTEGER NOT NULL DEFAULT 0 CHECK(seed_snapshot IN (0, 1)),
              stream_state TEXT NOT NULL DEFAULT 'SNAPSHOT',
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              FOREIGN KEY(partition_key) REFERENCES stream_partitions(partition_key)
            );

            CREATE INDEX IF NOT EXISTS idx_runtime_events_partition_ingress
              ON runtime_events(partition_key, ingress_seq);

            CREATE TABLE IF NOT EXISTS source_state_revisions (
              revision_id TEXT PRIMARY KEY,
              partition_key TEXT NOT NULL,
              source_wallet TEXT NOT NULL,
              revision INTEGER NOT NULL,
              catalog_revision TEXT NOT NULL,
              checkpoint INTEGER NOT NULL,
              observed_wall_ms INTEGER NOT NULL,
              observed_mono_ns INTEGER NOT NULL,
              provenance TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              UNIQUE(partition_key, revision)
            );

            CREATE TABLE IF NOT EXISTS follower_state_revisions (
              revision_id TEXT PRIMARY KEY,
              follower_account TEXT NOT NULL,
              revision INTEGER NOT NULL,
              catalog_revision TEXT NOT NULL,
              observed_wall_ms INTEGER NOT NULL,
              observed_mono_ns INTEGER NOT NULL,
              provenance TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              UNIQUE(follower_account, revision)
            );

            CREATE INDEX IF NOT EXISTS idx_follower_state_revisions_account_revision
              ON follower_state_revisions(lower(follower_account), revision DESC);

            CREATE TABLE IF NOT EXISTS deferred_deltas (
              delta_id TEXT PRIMARY KEY,
              generation TEXT NOT NULL,
              follower_account TEXT NOT NULL,
              canonical_market TEXT NOT NULL,
              direction TEXT NOT NULL,
              state TEXT NOT NULL,
              desired_revision INTEGER NOT NULL,
              source_revision INTEGER NOT NULL,
              follower_revision INTEGER NOT NULL,
              catalog_revision TEXT NOT NULL,
              book_revision INTEGER NOT NULL,
              source_basis_kind TEXT NOT NULL,
              source_basis_px TEXT,
              desired_qty TEXT NOT NULL,
              projected_qty TEXT NOT NULL,
              inflight_qty TEXT NOT NULL,
              remaining_qty TEXT NOT NULL,
              suppression_watermark TEXT NOT NULL DEFAULT '',
              first_blocked_wall_ms INTEGER NOT NULL,
              deadline_wall_ms INTEGER NOT NULL,
              deadline_mono_ns INTEGER NOT NULL,
              cloid TEXT NOT NULL DEFAULT '',
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              updated_ms INTEGER NOT NULL,
              UNIQUE(generation, follower_account, canonical_market)
            );

            CREATE INDEX IF NOT EXISTS idx_deferred_deltas_state_deadline
              ON deferred_deltas(state, deadline_wall_ms);

            CREATE TABLE IF NOT EXISTS deferred_delta_transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              transition_id TEXT NOT NULL UNIQUE,
              delta_id TEXT NOT NULL,
              from_state TEXT NOT NULL,
              to_state TEXT NOT NULL,
              cause TEXT NOT NULL,
              desired_revision INTEGER NOT NULL,
              source_revision INTEGER NOT NULL,
              follower_revision INTEGER NOT NULL,
              catalog_revision TEXT NOT NULL,
              book_revision INTEGER NOT NULL,
              wall_ms INTEGER NOT NULL,
              mono_ns INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              FOREIGN KEY(delta_id) REFERENCES deferred_deltas(delta_id)
            );

            CREATE INDEX IF NOT EXISTS idx_deferred_transitions_delta_seq
              ON deferred_delta_transitions(delta_id, seq);

            CREATE TABLE IF NOT EXISTS deferred_delta_contributions (
              contribution_id TEXT PRIMARY KEY,
              delta_id TEXT NOT NULL,
              source_event_key TEXT NOT NULL,
              follower_equivalent_qty TEXT NOT NULL,
              source_fill_px TEXT,
              source_basis_kind TEXT NOT NULL,
              exchange_ts_ms INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              FOREIGN KEY(delta_id) REFERENCES deferred_deltas(delta_id)
            );

            CREATE TABLE IF NOT EXISTS catalog_revisions (
              revision_id TEXT PRIMARY KEY,
              policy_version TEXT NOT NULL,
              snapshot_sha256 TEXT NOT NULL,
              dex_bracket_before_sha256 TEXT NOT NULL,
              dex_bracket_after_sha256 TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              accepted_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS catalog_diffs (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              diff_id TEXT NOT NULL UNIQUE,
              from_revision_id TEXT NOT NULL,
              to_revision_id TEXT NOT NULL,
              change_class TEXT NOT NULL,
              canonical_market TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              FOREIGN KEY(to_revision_id) REFERENCES catalog_revisions(revision_id)
            );

            CREATE TABLE IF NOT EXISTS catalog_asset_history (
              asset_id INTEGER PRIMARY KEY,
              canonical_market TEXT NOT NULL,
              first_revision_id TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              FOREIGN KEY(first_revision_id) REFERENCES catalog_revisions(revision_id)
            );

            CREATE TABLE IF NOT EXISTS catalog_market_transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              transition_id TEXT NOT NULL UNIQUE,
              revision_id TEXT NOT NULL,
              canonical_market TEXT NOT NULL,
              transition_class TEXT NOT NULL,
              from_readiness TEXT NOT NULL,
              to_readiness TEXT NOT NULL,
              observed_ms INTEGER NOT NULL,
              frame_sha256 TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              FOREIGN KEY(revision_id) REFERENCES catalog_revisions(revision_id)
            );

            CREATE INDEX IF NOT EXISTS idx_catalog_market_transitions_revision_market
              ON catalog_market_transitions(revision_id, canonical_market, seq);

            CREATE TABLE IF NOT EXISTS action_states (
              intent_id TEXT PRIMARY KEY,
              cloid TEXT NOT NULL UNIQUE,
              generation TEXT NOT NULL,
              follower_account TEXT NOT NULL,
              canonical_market TEXT NOT NULL,
              action_shard INTEGER NOT NULL,
              signer_epoch INTEGER NOT NULL,
              nonce INTEGER,
              request_id INTEGER,
              state TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL,
              updated_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_action_states_generation_follower_created
              ON action_states(generation, lower(follower_account), created_ms);

            CREATE TABLE IF NOT EXISTS action_state_transitions (
              seq INTEGER PRIMARY KEY AUTOINCREMENT,
              transition_id TEXT NOT NULL UNIQUE,
              intent_id TEXT NOT NULL,
              from_state TEXT NOT NULL,
              to_state TEXT NOT NULL,
              cause TEXT NOT NULL,
              wall_ms INTEGER NOT NULL,
              mono_ns INTEGER NOT NULL,
              payload_json TEXT NOT NULL,
              FOREIGN KEY(intent_id) REFERENCES action_states(intent_id)
            );

            CREATE INDEX IF NOT EXISTS idx_action_transitions_intent_seq
              ON action_state_transitions(intent_id, seq);

            CREATE TABLE IF NOT EXISTS stage_timings (
              timing_id TEXT PRIMARY KEY,
              generation TEXT NOT NULL,
              source_shard INTEGER NOT NULL,
              slot_id TEXT NOT NULL,
              intent_id TEXT NOT NULL DEFAULT '',
              event_key TEXT NOT NULL DEFAULT '',
              stage TEXT NOT NULL,
              wall_ms INTEGER NOT NULL,
              mono_ns INTEGER NOT NULL,
              duration_ns INTEGER,
              excluded_reason TEXT NOT NULL DEFAULT '',
              payload_json TEXT NOT NULL,
              created_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_stage_timings_generation_stage
              ON stage_timings(generation, stage, mono_ns);

            CREATE TABLE IF NOT EXISTS signer_epochs (
              follower_account TEXT PRIMARY KEY,
              generation TEXT NOT NULL,
              signer_epoch INTEGER NOT NULL,
              transport_epoch INTEGER NOT NULL,
              rest_epoch INTEGER NOT NULL,
              next_nonce INTEGER NOT NULL,
              signed_unsent_count INTEGER NOT NULL DEFAULT 0,
              updated_ms INTEGER NOT NULL
            );
            """
            )
            self._migrate_execution_attempt_columns(cur, prior_schema_version)
            self._migrate_source_event_reactions(cur, prior_schema_version)
            self._migrate_fast_execution_columns(cur)
            self._migrate_catalog_snapshot_index(cur)
            self._migrate_runtime_event_replay_fields(cur)
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_follower_intents_plan_phase
                ON follower_intents(desired_state_id, attempt_phase, seq)
                """
            )
            cur.execute(
                "INSERT OR REPLACE INTO schema_meta(key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )
            cur.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.conn.commit()

    @staticmethod
    def _column_names(cur: sqlite3.Cursor, table: str) -> set[str]:
        return {str(row[1]) for row in cur.execute(f"PRAGMA table_info({table})").fetchall()}

    @classmethod
    def _migrate_execution_attempt_columns(
        cls,
        cur: sqlite3.Cursor,
        prior_schema_version: int,
    ) -> None:
        """Add attempt metadata without guessing whether a legacy order was sent.

        A terminal report is durable proof that an old attempt is finished. Every
        other pre-v4 row remains ``legacy_unresolved`` and must be looked up at the
        exchange before it can be finalized.
        """

        columns = cls._column_names(cur, "follower_intents")
        if "desired_state_id" not in columns:
            cur.execute(
                "ALTER TABLE follower_intents ADD COLUMN desired_state_id TEXT NOT NULL DEFAULT ''"
            )
        if "attempt_phase" not in columns:
            cur.execute(
                "ALTER TABLE follower_intents "
                "ADD COLUMN attempt_phase TEXT NOT NULL DEFAULT 'legacy_unresolved'"
            )
        if "attempt_updated_ms" not in columns:
            cur.execute(
                "ALTER TABLE follower_intents "
                "ADD COLUMN attempt_updated_ms INTEGER NOT NULL DEFAULT 0"
            )
        if prior_schema_version >= 4:
            return
        cur.execute(
            """
            UPDATE follower_intents
            SET desired_state_id = coalesce(
                  nullif(json_extract(payload_json, '$.desired_state_id'), ''),
                  ''
                )
            WHERE json_valid(payload_json)
            """
        )
        terminal_placeholders = ",".join("?" for _ in TERMINAL_REPORT_STATUSES)
        cur.execute(
            f"""
            UPDATE follower_intents
            SET attempt_phase = ?,
                attempt_updated_ms = coalesce(
                  (SELECT max(created_ms) FROM execution_reports
                   WHERE execution_reports.cloid = follower_intents.cloid
                     AND execution_reports.status IN ({terminal_placeholders})),
                  created_ms
                )
            WHERE EXISTS (
              SELECT 1 FROM execution_reports
              WHERE execution_reports.cloid = follower_intents.cloid
                AND execution_reports.status IN ({terminal_placeholders})
            )
            """,
            (
                ExecutionAttemptPhase.TERMINAL.value,
                *TERMINAL_REPORT_STATUSES,
                *TERMINAL_REPORT_STATUSES,
            ),
        )

    @classmethod
    def _migrate_source_event_reactions(
        cls,
        cur: sqlite3.Cursor,
        prior_schema_version: int,
    ) -> None:
        """Require one current-truth validation for every pre-v5 source journal.

        Older journals cannot prove whether a persisted websocket/backfill event reached
        the in-memory reaction worker. Marking them legacy-unverified avoids guessing;
        the follower loop coalesces them into one fresh validation before subscribing.
        """

        if prior_schema_version >= 5:
            return
        cur.execute(
            """
            INSERT OR IGNORE INTO source_event_reactions(
              source_event_key, status, attempt_count, outcome_json, created_ms, updated_ms
            )
            SELECT idempotency_key, 'legacy_unverified', 0, '{}', created_ms, created_ms
            FROM source_events
            """
        )

    @classmethod
    def _migrate_fast_execution_columns(cls, cur: sqlite3.Cursor) -> None:
        columns = cls._column_names(cur, "source_event_reactions")
        if "result_id" not in columns:
            cur.execute(
                "ALTER TABLE source_event_reactions ADD COLUMN result_id TEXT NOT NULL DEFAULT ''"
            )

    @classmethod
    def _migrate_catalog_snapshot_index(cls, cur: sqlite3.Cursor) -> None:
        unique_snapshot = False
        for row in cur.execute("PRAGMA index_list(catalog_revisions)").fetchall():
            if not int(row[2]):
                continue
            columns = tuple(
                str(item[2]) for item in cur.execute(f"PRAGMA index_info({row[1]})").fetchall()
            )
            if columns == ("snapshot_sha256",):
                unique_snapshot = True
                break
        if not unique_snapshot:
            return
        count = int(cur.execute("SELECT count(*) FROM catalog_revisions").fetchone()[0])
        if count:
            raise JournalIntegrityError(
                "legacy catalog snapshot uniqueness cannot preserve pending adoption; "
                "start a fresh run generation with the v10 journal schema"
            )
        cur.execute("DROP TABLE catalog_revisions")
        cur.execute(
            """
            CREATE TABLE catalog_revisions (
              revision_id TEXT PRIMARY KEY,
              policy_version TEXT NOT NULL,
              snapshot_sha256 TEXT NOT NULL,
              dex_bracket_before_sha256 TEXT NOT NULL,
              dex_bracket_after_sha256 TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              accepted_ms INTEGER NOT NULL
            )
            """
        )

    @classmethod
    def _migrate_runtime_event_replay_fields(cls, cur: sqlite3.Cursor) -> None:
        columns = cls._column_names(cur, "runtime_events")
        if "seed_snapshot" not in columns:
            # Legacy rows did not retain the seed bit.  Treating them as seeds
            # is the conservative migration: startup adopts their state and a
            # later LIVE wake can establish durable debt without replaying a
            # potentially old event as a direct action.
            cur.execute(
                "ALTER TABLE runtime_events "
                "ADD COLUMN seed_snapshot INTEGER NOT NULL DEFAULT 1 "
                "CHECK(seed_snapshot IN (0, 1))"
            )
        if "stream_state" not in columns:
            cur.execute(
                "ALTER TABLE runtime_events "
                "ADD COLUMN stream_state TEXT NOT NULL DEFAULT 'SNAPSHOT'"
            )

    @staticmethod
    def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
        row = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        return row is not None

    @classmethod
    def _read_schema_version(cls, cur: sqlite3.Cursor) -> int:
        user_version = int(cur.execute("PRAGMA user_version").fetchone()[0] or 0)
        meta_version = 0
        if cls._table_exists(cur, "schema_meta"):
            row = cur.execute(
                "SELECT value FROM schema_meta WHERE key = ?",
                ("schema_version",),
            ).fetchone()
            if row is not None:
                try:
                    meta_version = int(row["value"])
                except (TypeError, ValueError) as exc:
                    raise JournalIntegrityError(
                        "SQLite schema_meta.schema_version is invalid; "
                        "back up the DB before repairing or migrating it"
                    ) from exc
        return max(user_version, meta_version)

    @staticmethod
    def _check_schema_version(version: int) -> None:
        if version > SCHEMA_VERSION:
            raise JournalIntegrityError(
                f"SQLite schema version {version} is newer than supported {SCHEMA_VERSION}; "
                "back up the DB and use a matching application version"
            )

    def _insert(
        self,
        sql: str,
        params: Iterable[Any],
        *,
        ignore_duplicate: bool = False,
    ) -> bool:
        try:
            with self.lock:
                with self.conn:
                    self.conn.execute(sql, tuple(params))
            return True
        except sqlite3.IntegrityError:
            if ignore_duplicate:
                return False
            raise

    @staticmethod
    def _json(payload: Any) -> str:
        return json.dumps(
            to_jsonable(redact_secrets(payload)), sort_keys=True, separators=(",", ":")
        )

    @classmethod
    def _stored_payload_matches(cls, stored_json: str, incoming: Any) -> bool:
        """Compare an immutable plan payload while ignoring its observation timestamp."""

        try:
            stored = json.loads(stored_json)
            expected = json.loads(cls._json(incoming))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(stored, dict) or not isinstance(expected, dict):
            return False
        stored.pop("created_ms", None)
        expected.pop("created_ms", None)
        if isinstance(incoming, DesiredState):
            for payload in (stored, expected):
                positions = payload.get("positions")
                if not isinstance(positions, dict):
                    return False
                for position in positions.values():
                    if not isinstance(position, dict):
                        return False
                    position.pop("updated_ms", None)
        return stored == expected

    @classmethod
    def _stored_intent_payload_matches_for_rearm(
        cls,
        stored_json: str,
        incoming: FollowerIntent,
    ) -> bool:
        """Match retry identity while allowing only a refreshed HIP-3 market proof.

        A deterministic plan that was durably stopped before signing may be retried with
        a newer HIP-3 book observation and its corresponding entry limit. Every semantic
        identity and sizing field remains immutable, and native-perp prices cannot change.
        """

        try:
            stored = json.loads(stored_json)
            expected = json.loads(cls._json(incoming))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        if not isinstance(stored, dict) or not isinstance(expected, dict):
            return False
        for payload in (stored, expected):
            payload.pop("created_ms", None)
        if stored == expected:
            return True
        stored_proof = stored.get("execution_proof")
        expected_proof = expected.get("execution_proof")
        if not (
            isinstance(stored_proof, dict)
            and isinstance(expected_proof, dict)
            and stored_proof.get("kind") == "hip3_round_trip"
            and expected_proof.get("kind") == "hip3_round_trip"
        ):
            return False
        for payload in (stored, expected):
            payload.pop("execution_proof", None)
            payload.pop("price", None)
        return stored == expected

    def append_source_event(
        self,
        event: SourceEvent,
        *,
        reaction_required: bool = False,
    ) -> bool:
        """Persist source truth and its reaction obligation in one transaction.

        Websocket/backfill callers set ``reaction_required``. A duplicate still
        ensures the outbox row exists, closing the crash window between an old
        event insert and reaction scheduling without replaying completed rows.
        """

        created = now_ms()
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO source_events(
                      idempotency_key, event_type, exchange_ts_ms, observed_ts_ms,
                      payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.idempotency_key,
                        event.event_type.value,
                        event.exchange_ts_ms,
                        event.observed_ts_ms,
                        self._json(event),
                        created,
                    ),
                )
                inserted = cur.rowcount > 0
                if reaction_required:
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO source_event_reactions(
                          source_event_key, status, attempt_count, outcome_json,
                          created_ms, updated_ms
                        ) VALUES (?, 'pending', 0, '{}', ?, ?)
                        """,
                        (event.idempotency_key, created, created),
                    )
                self.conn.commit()
                return inserted
            except Exception:
                self.conn.rollback()
                raise

    def append_desired_state(self, state: DesiredState) -> bool:
        return self._insert(
            """
            INSERT INTO desired_states(state_id, source_event_key, mode, payload_json, created_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                state.state_id,
                state.source_event_key,
                state.mode.value,
                self._json(state),
                state.created_ms,
            ),
            ignore_duplicate=True,
        )

    def prepare_execution_plan(
        self,
        state: DesiredState,
        intents: Iterable[FollowerIntent],
    ) -> bool:
        """Atomically persist a target and every never-dispatched attempt.

        Returning ``False`` means at least one durable identity already exists.
        The caller must not dispatch any member of that plan in that case.
        """

        plan_intents = tuple(intents)
        if any(intent.desired_state_id != state.state_id for intent in plan_intents):
            raise JournalIntegrityError(
                "every prepared follower intent must reference its desired state"
            )
        intent_ids = {intent.intent_id for intent in plan_intents}
        cloids = {intent.cloid for intent in plan_intents}
        if len(intent_ids) != len(plan_intents) or len(cloids) != len(plan_intents):
            raise JournalIntegrityError("execution plan contains duplicate intent identities")

        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                existing_state = self.conn.execute(
                    "SELECT 1 FROM desired_states WHERE state_id = ?",
                    (state.state_id,),
                ).fetchone()
                existing_intent = None
                if plan_intents:
                    id_placeholders = ",".join("?" for _ in plan_intents)
                    cloid_placeholders = ",".join("?" for _ in plan_intents)
                    existing_intent = self.conn.execute(
                        f"""
                        SELECT 1 FROM follower_intents
                        WHERE intent_id IN ({id_placeholders})
                           OR cloid IN ({cloid_placeholders})
                        LIMIT 1
                        """,
                        (*intent_ids, *cloids),
                    ).fetchone()
                if existing_state is not None or existing_intent is not None:
                    if existing_state is not None and self._rearm_proven_unsent_plan(
                        state,
                        plan_intents,
                    ):
                        self.conn.commit()
                        return True
                    self.conn.rollback()
                    return False
                self.conn.execute(
                    """
                    INSERT INTO desired_states(
                      state_id, source_event_key, mode, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        state.state_id,
                        state.source_event_key,
                        state.mode.value,
                        self._json(state),
                        state.created_ms,
                    ),
                )
                for intent in plan_intents:
                    self.conn.execute(
                        """
                        INSERT INTO follower_intents(
                          intent_id, cloid, desired_state_id, source_event_key,
                          action, coin, mode, status, attempt_phase,
                          attempt_updated_ms, payload_json, created_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            intent.intent_id,
                            intent.cloid,
                            state.state_id,
                            intent.source_event_key,
                            intent.action.value,
                            intent.coin,
                            intent.mode.value,
                            intent.status.value,
                            ExecutionAttemptPhase.PREPARED.value,
                            intent.created_ms,
                            self._json(intent),
                            intent.created_ms,
                        ),
                    )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def _rearm_proven_unsent_plan(
        self,
        state: DesiredState,
        plan_intents: tuple[FollowerIntent, ...],
    ) -> bool:
        """Reopen an identical terminal plan only when every send was provably blocked.

        Reusing the deterministic CLOID is safe only when durable reports prove that
        it never crossed the signed-send boundary. Any sent/acked/filled/canceled/
        rejected or otherwise unknown evidence keeps the identity conflict closed.
        The caller holds ``BEGIN IMMEDIATE`` and commits on success.
        """

        if not plan_intents:
            return False
        stored_state = self.conn.execute(
            "SELECT source_event_key, mode, payload_json FROM desired_states WHERE state_id = ?",
            (state.state_id,),
        ).fetchone()
        if (
            stored_state is None
            or str(stored_state["source_event_key"]) != state.source_event_key
            or str(stored_state["mode"]) != state.mode.value
            or not self._stored_payload_matches(str(stored_state["payload_json"]), state)
        ):
            return False
        stored_rows = self.conn.execute(
            """
            SELECT intent_id, cloid, action, coin, mode, attempt_phase, payload_json
            FROM follower_intents
            WHERE desired_state_id = ?
            ORDER BY seq
            """,
            (state.state_id,),
        ).fetchall()
        incoming_by_id = {intent.intent_id: intent for intent in plan_intents}
        if len(stored_rows) != len(plan_intents) or len(incoming_by_id) != len(plan_intents):
            return False
        for row in stored_rows:
            intent = incoming_by_id.get(str(row["intent_id"]))
            if (
                intent is None
                or str(row["cloid"]) != intent.cloid
                or str(row["action"]) != intent.action.value
                or str(row["coin"]) != intent.coin
                or str(row["mode"]) != intent.mode.value
                or str(row["attempt_phase"]) != ExecutionAttemptPhase.TERMINAL.value
                or not self._stored_intent_payload_matches_for_rearm(
                    str(row["payload_json"]), intent
                )
            ):
                return False
            reports = self.conn.execute(
                """
                SELECT status, exchange_status
                FROM execution_reports
                WHERE cloid = ?
                ORDER BY seq
                """,
                (intent.cloid,),
            ).fetchall()
            if not reports:
                return False
            for report in reports:
                report_status = str(report["status"])
                exchange_status = str(report["exchange_status"])
                if not _report_is_proven_no_send(
                    status=report_status,
                    exchange_status=exchange_status,
                    action=intent.action.value,
                ):
                    return False
        observed = now_ms()
        intent_ids = tuple(incoming_by_id)
        placeholders = ",".join("?" for _ in intent_ids)
        for intent in plan_intents:
            cur = self.conn.execute(
                """
                UPDATE follower_intents
                SET payload_json = ?
                WHERE desired_state_id = ?
                  AND intent_id = ?
                  AND attempt_phase = ?
                """,
                (
                    self._json(intent),
                    state.state_id,
                    intent.intent_id,
                    ExecutionAttemptPhase.TERMINAL.value,
                ),
            )
            if cur.rowcount != 1:
                raise JournalIntegrityError("proven-unsent payload refresh was not atomic")
        cur = self.conn.execute(
            f"""
            UPDATE follower_intents
            SET attempt_phase = ?, attempt_updated_ms = ?
            WHERE desired_state_id = ?
              AND intent_id IN ({placeholders})
              AND attempt_phase = ?
            """,
            (
                ExecutionAttemptPhase.PREPARED.value,
                observed,
                state.state_id,
                *intent_ids,
                ExecutionAttemptPhase.TERMINAL.value,
            ),
        )
        if cur.rowcount != len(plan_intents):
            raise JournalIntegrityError("proven-unsent plan rearm was not atomic")
        self.conn.execute(
            """
            INSERT INTO control_audit(control, status, detail, payload_json, created_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "rearm_execution_plan",
                "success",
                "rearmed deterministic plan after durable no-send proof",
                self._json(
                    {
                        "state_id": state.state_id,
                        "intent_ids": list(intent_ids),
                        "proof": "all prior reports were terminal skipped before signed send",
                    }
                ),
                observed,
            ),
        )
        return True

    def commit_desired_state(self, state_id: str) -> None:
        with self.lock:
            with self.conn:
                exists = self.conn.execute(
                    "SELECT 1 FROM desired_states WHERE state_id = ?",
                    (state_id,),
                ).fetchone()
                if exists is None:
                    raise JournalIntegrityError(f"cannot commit missing desired state {state_id}")
                self.conn.execute(
                    "INSERT OR IGNORE INTO desired_state_commits(state_id, committed_ms) "
                    "VALUES (?, ?)",
                    (state_id, now_ms()),
                )

    def desired_state_is_committed(self, state_id: str) -> bool:
        if not state_id:
            return False
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM desired_state_commits WHERE state_id = ? LIMIT 1",
                (state_id,),
            ).fetchone()
        return row is not None

    def unresolved_desired_state_count(
        self,
        *,
        mode: Mode,
        source_wallet: str,
        action_account: str,
        source_network: str,
    ) -> int:
        scope_params = (
            mode.value,
            source_wallet.lower(),
            action_account.lower(),
            source_network,
        )
        with self.lock:
            row = self.conn.execute(
                """
                WITH scoped AS (
                  SELECT seq, state_id
                  FROM desired_states
                  WHERE mode = ?
                    AND json_valid(payload_json)
                    AND lower(json_extract(payload_json, '$.source_wallet')) = ?
                    AND lower(json_extract(payload_json, '$.action_account')) = ?
                    AND json_extract(payload_json, '$.source_network') = ?
                ), latest_commit AS (
                  SELECT max(scoped.seq) AS seq
                  FROM scoped
                  JOIN desired_state_commits AS committed
                    ON committed.state_id = scoped.state_id
                )
                SELECT count(*)
                FROM scoped
                WHERE scoped.seq > coalesce((SELECT seq FROM latest_commit), 0)
                """,
                scope_params,
            ).fetchone()
        return int(row[0])

    def unresolved_desired_states(
        self,
        *,
        mode: Mode,
        source_wallet: str,
        action_account: str,
        source_network: str,
    ) -> list[DesiredState]:
        with self.lock:
            rows = self.conn.execute(
                """
                WITH scoped AS (
                  SELECT *
                  FROM desired_states
                  WHERE mode = ?
                    AND json_valid(payload_json)
                    AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                    AND lower(json_extract(payload_json, '$.action_account')) = lower(?)
                    AND json_extract(payload_json, '$.source_network') = ?
                ), latest_commit AS (
                  SELECT max(scoped.seq) AS seq
                  FROM scoped
                  JOIN desired_state_commits AS committed
                    ON committed.state_id = scoped.state_id
                )
                SELECT scoped.*
                FROM scoped
                WHERE scoped.seq > coalesce((SELECT seq FROM latest_commit), 0)
                ORDER BY scoped.seq
                """,
                (mode.value, source_wallet, action_account, source_network),
            ).fetchall()
        return [self._desired_state_from_row(row) for row in rows]

    def desired_state(self, state_id: str) -> DesiredState | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM desired_states WHERE state_id = ?",
                (state_id,),
            ).fetchone()
        return None if row is None else self._desired_state_from_row(row)

    @staticmethod
    def _desired_state_from_row(row: sqlite3.Row) -> DesiredState:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise JournalIntegrityError(
                f"desired state {row['state_id']} payload is not valid JSON"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("positions"), dict):
            raise JournalIntegrityError(
                f"desired state {row['state_id']} positions payload is malformed"
            )
        positions: dict[str, Position] = {}
        for coin, item in payload["positions"].items():
            if not isinstance(item, dict):
                raise JournalIntegrityError(
                    f"desired state {row['state_id']} position {coin} is malformed"
                )
            try:
                symbol = canonical_market_symbol(item.get("coin") or coin)
                size = parse_decimal(item.get("size"))
                if size == 0:
                    continue
                entry_px = item.get("entry_px")
                leverage = item.get("leverage")
                positions[symbol] = Position(
                    coin=symbol,
                    size=size,
                    entry_px=(parse_decimal(entry_px) if entry_px not in {None, ""} else None),
                    leverage=(int(str(leverage)) if leverage not in {None, ""} else None),
                    updated_ms=int(str(item.get("updated_ms") or 0)),
                )
            except Exception as exc:
                raise JournalIntegrityError(
                    f"desired state {row['state_id']} position {coin} cannot be rebuilt: {exc}"
                ) from exc
        try:
            return DesiredState(
                state_id=str(payload.get("state_id") or row["state_id"]),
                source_event_key=str(payload.get("source_event_key") or row["source_event_key"]),
                mode=Mode(str(payload.get("mode") or row["mode"])),
                positions=positions,
                reason=str(payload.get("reason") or "legacy desired state"),
                created_ms=int(payload.get("created_ms") or row["created_ms"] or 0),
                source_wallet=str(payload.get("source_wallet") or "").lower(),
                action_account=str(payload.get("action_account") or "").lower(),
                source_network=str(payload.get("source_network") or ""),
            )
        except Exception as exc:
            raise JournalIntegrityError(
                f"desired state {row['state_id']} cannot be rebuilt: {exc}"
            ) from exc

    def append_intent(self, intent: FollowerIntent) -> bool:
        phase = (
            ExecutionAttemptPhase.PREPARED
            if intent.desired_state_id
            else ExecutionAttemptPhase.LEGACY_UNRESOLVED
        )
        return self._insert(
            """
            INSERT INTO follower_intents(
              intent_id, cloid, desired_state_id, source_event_key, action, coin,
              mode, status, attempt_phase, attempt_updated_ms, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                intent.intent_id,
                intent.cloid,
                intent.desired_state_id,
                intent.source_event_key,
                intent.action.value,
                intent.coin,
                intent.mode.value,
                intent.status.value,
                phase.value,
                intent.created_ms,
                self._json(intent),
                intent.created_ms,
            ),
            ignore_duplicate=True,
        )

    def append_execution_report(self, report: ExecutionReport) -> bool:
        created = now_ms()
        phase = (
            ExecutionAttemptPhase.TERMINAL
            if report.status.value in TERMINAL_REPORT_STATUSES
            else ExecutionAttemptPhase.UNKNOWN
        )
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO execution_reports(
                      report_id, intent_id, cloid, status, exchange_status,
                      payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report.report_id,
                        report.intent_id,
                        report.cloid,
                        report.status.value,
                        report.exchange_status,
                        self._json(report),
                        created,
                    ),
                )
                if cur.rowcount == 0:
                    self.conn.rollback()
                    return False
                if phase == ExecutionAttemptPhase.TERMINAL:
                    self.conn.execute(
                        """
                        UPDATE follower_intents
                        SET attempt_phase = ?, attempt_updated_ms = ?
                        WHERE cloid = ? AND attempt_phase != ?
                        """,
                        (
                            phase.value,
                            created,
                            report.cloid,
                            ExecutionAttemptPhase.TERMINAL.value,
                        ),
                    )
                else:
                    self.conn.execute(
                        """
                        UPDATE follower_intents
                        SET attempt_phase = ?, attempt_updated_ms = ?
                        WHERE intent_id = ? AND cloid = ? AND attempt_phase != ?
                        """,
                        (
                            phase.value,
                            created,
                            report.intent_id,
                            report.cloid,
                            ExecutionAttemptPhase.TERMINAL.value,
                        ),
                    )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def begin_intent_dispatch(self, intent_id: str) -> bool | None:
        """Persist the ambiguity boundary immediately before a signed send.

        ``None`` means the ID is not a follower intent (for example a dead-man
        operation). ``False`` means an existing attempt was not PREPARED and the
        caller must not send it again.
        """

        observed = now_ms()
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                row = self.conn.execute(
                    "SELECT attempt_phase FROM follower_intents WHERE intent_id = ?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    self.conn.commit()
                    return None
                if str(row["attempt_phase"]) != ExecutionAttemptPhase.PREPARED.value:
                    self.conn.rollback()
                    return False
                self.conn.execute(
                    """
                    UPDATE follower_intents
                    SET attempt_phase = ?, attempt_updated_ms = ?
                    WHERE intent_id = ? AND attempt_phase = ?
                    """,
                    (
                        ExecutionAttemptPhase.DISPATCHING.value,
                        observed,
                        intent_id,
                        ExecutionAttemptPhase.PREPARED.value,
                    ),
                )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def refresh_prepared_hip3_intent(self, intent: FollowerIntent) -> bool:
        """CAS-refresh only the price/proof of a never-dispatched HIP-3 intent."""

        return self._refresh_hip3_intent_phase(intent, ExecutionAttemptPhase.PREPARED)

    def freeze_prepared_hip3_dispatch(self, intent: FollowerIntent) -> bool:
        """Atomically persist the exact HIP-3 request and enter DISPATCHING.

        A DISPATCHING payload is immutable because the signed send may already have
        happened.  This transition is therefore the one durable ambiguity boundary.
        """

        observed = now_ms()
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                row = self.conn.execute(
                    "SELECT cloid, attempt_phase, payload_json FROM follower_intents "
                    "WHERE intent_id = ?",
                    (intent.intent_id,),
                ).fetchone()
                if (
                    row is None
                    or str(row["cloid"]) != intent.cloid
                    or str(row["attempt_phase"]) != ExecutionAttemptPhase.PREPARED.value
                    or not self._stored_intent_payload_matches_for_rearm(
                        str(row["payload_json"]), intent
                    )
                ):
                    self.conn.rollback()
                    return False
                cur = self.conn.execute(
                    "UPDATE follower_intents "
                    "SET payload_json = ?, attempt_phase = ?, attempt_updated_ms = ? "
                    "WHERE intent_id = ? AND cloid = ? AND attempt_phase = ?",
                    (
                        self._json(intent),
                        ExecutionAttemptPhase.DISPATCHING.value,
                        observed,
                        intent.intent_id,
                        intent.cloid,
                        ExecutionAttemptPhase.PREPARED.value,
                    ),
                )
                if cur.rowcount != 1:
                    self.conn.rollback()
                    return False
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def _refresh_hip3_intent_phase(
        self, intent: FollowerIntent, phase: ExecutionAttemptPhase
    ) -> bool:

        observed = now_ms()
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                row = self.conn.execute(
                    "SELECT cloid, attempt_phase, payload_json FROM follower_intents "
                    "WHERE intent_id = ?",
                    (intent.intent_id,),
                ).fetchone()
                if (
                    row is None
                    or str(row["cloid"]) != intent.cloid
                    or str(row["attempt_phase"]) != phase.value
                    or not self._stored_intent_payload_matches_for_rearm(
                        str(row["payload_json"]), intent
                    )
                ):
                    self.conn.rollback()
                    return False
                self.conn.execute(
                    "UPDATE follower_intents SET payload_json = ?, attempt_updated_ms = ? "
                    "WHERE intent_id = ? AND cloid = ? AND attempt_phase = ?",
                    (
                        self._json(intent),
                        observed,
                        intent.intent_id,
                        intent.cloid,
                        phase.value,
                    ),
                )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def prepare_signed_action_attempt(
        self,
        *,
        attempt_id: str,
        intent_id: str,
        cloid: str,
        action: str,
        mode: Mode,
        account: str,
        network: str,
        payload: dict[str, Any],
    ) -> bool:
        """Durably prepare a non-order signed mutation without permitting overlap.

        Only one unresolved mutation may exist for an execution account. A duplicate
        attempt ID or any older unresolved attempt returns ``False`` so callers fail
        closed instead of guessing whether a prior signed request reached the exchange.
        """

        created = now_ms()
        normalized_account = account.strip().lower()
        normalized_network = network.strip().lower()
        placeholders = ",".join("?" for _ in UNRESOLVED_ATTEMPT_PHASES)
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                unresolved = self.conn.execute(
                    f"""
                    SELECT attempt_id FROM signed_action_attempts
                    WHERE mode = ?
                      AND lower(account) = ?
                      AND network = ?
                      AND attempt_phase IN ({placeholders})
                    ORDER BY seq LIMIT 1
                    """,
                    (
                        mode.value,
                        normalized_account,
                        normalized_network,
                        *UNRESOLVED_ATTEMPT_PHASES,
                    ),
                ).fetchone()
                if unresolved is not None:
                    self.conn.rollback()
                    return False
                try:
                    self.conn.execute(
                        """
                        INSERT INTO signed_action_attempts(
                          attempt_id, intent_id, cloid, action, mode, account, network,
                          attempt_phase, payload_json, result_json, created_ms, updated_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
                        """,
                        (
                            attempt_id,
                            intent_id,
                            cloid,
                            action,
                            mode.value,
                            normalized_account,
                            normalized_network,
                            ExecutionAttemptPhase.PREPARED.value,
                            self._json(payload),
                            created,
                            created,
                        ),
                    )
                except sqlite3.IntegrityError:
                    self.conn.rollback()
                    return False
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def begin_signed_action_dispatch(self, attempt_id: str) -> bool:
        """Persist the ambiguity boundary immediately before entering the signer."""

        observed = now_ms()
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.execute(
                    """
                    UPDATE signed_action_attempts
                    SET attempt_phase = ?, updated_ms = ?
                    WHERE attempt_id = ? AND attempt_phase = ?
                    """,
                    (
                        ExecutionAttemptPhase.DISPATCHING.value,
                        observed,
                        attempt_id,
                        ExecutionAttemptPhase.PREPARED.value,
                    ),
                )
                if cur.rowcount != 1:
                    self.conn.rollback()
                    return False
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def finish_signed_action_attempt(
        self,
        attempt_id: str,
        report: ExecutionReport,
    ) -> bool:
        """Record a conclusive result or preserve an ambiguous transport outcome."""

        observed = now_ms()
        phase = (
            ExecutionAttemptPhase.UNKNOWN
            if report.status.value == "sent"
            else ExecutionAttemptPhase.TERMINAL
        )
        result = {
            "report_id": report.report_id,
            "intent_id": report.intent_id,
            "cloid": report.cloid,
            "status": report.status.value,
            "exchange_status": report.exchange_status,
            "exchange_ts_ms": report.exchange_ts_ms,
            "payload": report.payload,
        }
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                cur = self.conn.execute(
                    """
                    UPDATE signed_action_attempts
                    SET attempt_phase = ?, result_json = ?, updated_ms = ?
                    WHERE attempt_id = ? AND attempt_phase = ?
                    """,
                    (
                        phase.value,
                        self._json(result),
                        observed,
                        attempt_id,
                        ExecutionAttemptPhase.DISPATCHING.value,
                    ),
                )
                if cur.rowcount != 1:
                    self.conn.rollback()
                    return False
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def unresolved_signed_action_attempts(
        self,
        mode: Mode | None = None,
        *,
        account: str | None = None,
        network: str | None = None,
    ) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in UNRESOLVED_ATTEMPT_PHASES)
        clauses = [f"attempt_phase IN ({placeholders})"]
        params: list[Any] = [*UNRESOLVED_ATTEMPT_PHASES]
        if mode is not None:
            clauses.append("mode = ?")
            params.append(mode.value)
        if account is not None:
            clauses.append("lower(account) = lower(?)")
            params.append(account)
        if network is not None:
            clauses.append("network = ?")
            params.append(network.lower())
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM signed_action_attempts WHERE "
                + " AND ".join(clauses)
                + " ORDER BY seq",
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def unresolved_signed_action_attempt_count(
        self,
        mode: Mode | None = None,
        *,
        account: str | None = None,
        network: str | None = None,
    ) -> int:
        return len(
            self.unresolved_signed_action_attempts(
                mode,
                account=account,
                network=network,
            )
        )

    def append_reconcile_snapshot(self, snapshot: ReconcileSnapshot) -> bool:
        return self._insert(
            """
            INSERT INTO reconcile_snapshots(
              snapshot_id, account, source, observed_ms, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.snapshot_id,
                snapshot.account,
                snapshot.source,
                snapshot.observed_ms,
                self._json(snapshot),
                now_ms(),
            ),
            ignore_duplicate=True,
        )

    def append_safe_mode(self, transition: SafeModeTransition) -> bool:
        return self._insert(
            """
            INSERT INTO safe_mode_transitions(
              transition_id, enabled, reason, detail, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                transition.transition_id,
                1 if transition.enabled else 0,
                transition.reason.value,
                transition.detail,
                self._json(transition),
                transition.created_ms,
            ),
            ignore_duplicate=True,
        )

    def append_safe_mode_if_revision(
        self,
        transition: SafeModeTransition,
        *,
        expected_seq: int,
    ) -> bool:
        """Append a safe-mode transition only if no newer transition won the race."""

        params = (
            transition.transition_id,
            1 if transition.enabled else 0,
            transition.reason.value,
            transition.detail,
            self._json(transition),
            transition.created_ms,
        )
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                row = self.conn.execute(
                    "SELECT seq FROM safe_mode_transitions ORDER BY seq DESC LIMIT 1"
                ).fetchone()
                current_seq = int(row["seq"]) if row is not None else 0
                if current_seq != expected_seq:
                    self.conn.rollback()
                    return False
                self.conn.execute(
                    """
                    INSERT INTO safe_mode_transitions(
                      transition_id, enabled, reason, detail, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
                self.conn.commit()
                return True
            except Exception:
                self.conn.rollback()
                raise

    def append_config_revision(self, revision_id: str, payload: dict[str, Any]) -> bool:
        return self._insert(
            "INSERT INTO config_revisions(revision_id, payload_json, created_ms) VALUES (?, ?, ?)",
            (revision_id, self._json(payload), now_ms()),
            ignore_duplicate=True,
        )

    def append_control_audit(
        self,
        *,
        control: str,
        status: str,
        detail: str,
        payload: dict[str, Any],
        created_ms: int | None = None,
    ) -> bool:
        observed = created_ms or now_ms()
        return self._insert(
            """
            INSERT INTO control_audit(control, status, detail, payload_json, created_ms)
            VALUES (?, ?, ?, ?, ?)
            """,
            (control, status, detail, self._json(payload), observed),
        )

    def schema_version(self) -> int:
        with self.lock:
            return self._read_schema_version(self.conn.cursor())

    def ensure_journal_scope(self, scope: dict[str, str]) -> tuple[bool, str]:
        normalized = {
            "source_wallet": str(scope.get("source_wallet") or "").lower(),
            "source_network": str(scope.get("source_network") or "").lower(),
            "action_account": str(scope.get("action_account") or "").lower(),
            "execution_network": str(scope.get("execution_network") or "").lower(),
        }
        if not all(normalized.values()):
            return False, (
                "journal scope requires source wallet/network, action account, "
                "and execution network"
            )
        encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT value FROM schema_meta WHERE key = ?",
                    ("journal_scope_v1",),
                ).fetchone()
                if row is not None:
                    existing = str(row["value"])
                    if existing == encoded:
                        self.conn.commit()
                        return True, "journal scope matches"
                    self.conn.rollback()
                    return False, "database is bound to a different source/account/network scope"

                compatible, detail = self._legacy_journal_scope_compatible(normalized)
                if not compatible:
                    self.conn.rollback()
                    return False, detail
                self.conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES (?, ?)",
                    ("journal_scope_v1", encoded),
                )
                self.conn.commit()
                return True, "journal scope bound"
            except Exception:
                self.conn.rollback()
                raise

    def _legacy_journal_scope_compatible(self, scope: dict[str, str]) -> tuple[bool, str]:
        desired_rows = self.conn.execute(
            "SELECT mode, payload_json FROM desired_states WHERE mode IN ('testnet', 'live')"
        ).fetchall()
        for row in desired_rows:
            expected_mode = "testnet" if scope["execution_network"] == "testnet" else "live"
            if str(row["mode"]) != expected_mode:
                return False, "legacy exchange desired state uses another execution network"
            try:
                payload = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                return False, "legacy exchange desired state is malformed; use a new DB"
            if not isinstance(payload, dict):
                return False, "legacy exchange desired state is unscoped; use a new DB"
            if (
                str(payload.get("source_wallet") or "").lower() != scope["source_wallet"]
                or str(payload.get("action_account") or "").lower() != scope["action_account"]
                or str(payload.get("source_network") or "").lower() != scope["source_network"]
            ):
                return False, "legacy exchange desired state belongs to another scope; use a new DB"

        reconcile_accounts = {
            str(row["account"] or "").lower()
            for row in self.conn.execute(
                "SELECT DISTINCT account FROM reconcile_snapshots"
            ).fetchall()
        }
        if reconcile_accounts and reconcile_accounts != {scope["action_account"]}:
            return False, "legacy follower reconciles belong to another account; use a new DB"

        exchange_intents = int(
            self.conn.execute(
                "SELECT count(*) FROM follower_intents WHERE mode IN ('testnet', 'live')"
            ).fetchone()[0]
        )
        exchange_reports = int(
            self.conn.execute("SELECT count(*) FROM execution_reports").fetchone()[0]
        )
        safe_transitions = int(
            self.conn.execute("SELECT count(*) FROM safe_mode_transitions").fetchone()[0]
        )
        if (exchange_intents or exchange_reports or safe_transitions) and not desired_rows:
            return False, "legacy exchange journal has no scoped desired baseline; use a new DB"
        return True, "legacy journal is compatible"

    def checkpoint_wal(self, mode: str = "PASSIVE") -> dict[str, int]:
        normalized = mode.strip().upper()
        if normalized not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("checkpoint mode must be PASSIVE, FULL, RESTART, or TRUNCATE")
        with self.lock:
            row = self.conn.execute(f"PRAGMA wal_checkpoint({normalized})").fetchone()
        return {"busy": int(row[0]), "log": int(row[1]), "checkpointed": int(row[2])}

    def backup_to(self, destination: Path | str) -> Path:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self.lock:
            backup_conn = sqlite3.connect(target)
            try:
                self.conn.backup(backup_conn)
            finally:
                backup_conn.close()
        return target

    def recent(self, table: str, limit: int = 50) -> list[dict[str, Any]]:
        allowed = {
            "source_events",
            "desired_states",
            "follower_intents",
            "execution_reports",
            "signed_action_attempts",
            "reconcile_snapshots",
            "safe_mode_transitions",
            "config_revisions",
            "control_audit",
        }
        if table not in allowed:
            raise ValueError(f"unsupported journal table: {table}")
        with self.lock:
            rows = self.conn.execute(
                f"SELECT * FROM {table} ORDER BY seq DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def count(self, table: str) -> int:
        with self.lock:
            return int(self.conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])

    def latest_source_event(self, source_wallet: str | None = None) -> dict[str, Any] | None:
        where = ""
        params: tuple[Any, ...] = ()
        if source_wallet:
            where = (
                "WHERE json_valid(payload_json) "
                "AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)"
            )
            params = (source_wallet,)
        with self.lock:
            row = self.conn.execute(
                f"SELECT * FROM source_events {where} ORDER BY seq DESC LIMIT 1",
                params,
            ).fetchone()
        return None if row is None else dict(row)

    def count_source_events(self, source_wallet: str | None = None) -> int:
        if not source_wallet:
            return self.count("source_events")
        with self.lock:
            row = self.conn.execute(
                """
                SELECT count(*) FROM source_events
                WHERE json_valid(payload_json)
                  AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                """,
                (source_wallet,),
            ).fetchone()
        return int(row[0])

    def recent_source_events(
        self,
        *,
        source_wallet: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        if not source_wallet:
            return self.recent("source_events", limit)
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM source_events
                WHERE json_valid(payload_json)
                  AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                ORDER BY seq DESC LIMIT ?
                """,
                (source_wallet, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_source_planning_identity(
        self,
        *,
        source_wallet: str,
    ) -> tuple[str, str] | None:
        """Return the newest durable REST planning identity for one source wallet."""

        with self.lock:
            row = self.conn.execute(
                """
                SELECT
                  json_extract(payload_json, '$.payload.planning_exposure_key'),
                  json_extract(payload_json, '$.payload.planning_key')
                FROM source_events
                WHERE event_type = 'reconcile'
                  AND json_valid(payload_json)
                  AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                  AND json_type(
                        payload_json,
                        '$.payload.planning_exposure_key'
                      ) = 'text'
                  AND json_type(payload_json, '$.payload.planning_key') = 'text'
                  AND trim(json_extract(
                        payload_json,
                        '$.payload.planning_exposure_key'
                      )) != ''
                  AND trim(json_extract(payload_json, '$.payload.planning_key')) != ''
                ORDER BY seq DESC
                LIMIT 1
                """,
                (source_wallet,),
            ).fetchone()
        if row is None:
            return None
        return str(row[0]), str(row[1])

    def source_reaction_status(self, source_event_key: str) -> str | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT status FROM source_event_reactions WHERE source_event_key = ?",
                (source_event_key,),
            ).fetchone()
        return None if row is None else str(row["status"])

    def source_reaction_high_watermark(self, *, source_wallet: str) -> int | None:
        placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT max(reactions.rowid)
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status IN ({placeholders})
                  AND json_valid(events.payload_json)
                  AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)
                """,
                (*SOURCE_REACTION_OPEN_STATUSES, source_wallet),
            ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def unfinished_source_reaction_count(
        self,
        *,
        source_wallet: str | None = None,
        through_reaction_rowid: int | None = None,
    ) -> int:
        placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        params: list[Any] = [*SOURCE_REACTION_OPEN_STATUSES]
        cutoff_filter = ""
        if through_reaction_rowid is not None:
            cutoff_filter = "AND reactions.rowid <= ?"
            params.append(int(through_reaction_rowid))
        wallet_filter = ""
        if source_wallet:
            wallet_filter = (
                "AND json_valid(events.payload_json) "
                "AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)"
            )
            params.append(source_wallet)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT count(*)
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status IN ({placeholders})
                  {cutoff_filter}
                  {wallet_filter}
                """,
                tuple(params),
            ).fetchone()
        return int(row[0])

    def legacy_source_reaction_count(
        self,
        *,
        source_wallet: str | None = None,
        through_reaction_rowid: int | None = None,
    ) -> int:
        params: list[Any] = []
        cutoff_filter = ""
        if through_reaction_rowid is not None:
            cutoff_filter = "AND reactions.rowid <= ?"
            params.append(int(through_reaction_rowid))
        wallet_filter = ""
        if source_wallet:
            wallet_filter = (
                "AND json_valid(events.payload_json) "
                "AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)"
            )
            params.append(source_wallet)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT count(*)
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status = 'legacy_unverified'
                  {cutoff_filter}
                  {wallet_filter}
                """,
                tuple(params),
            ).fetchone()
        return int(row[0])

    def blocked_source_reaction_count(self, *, source_wallet: str | None = None) -> int:
        """Count source reactions intentionally held while safe mode blocked validation."""

        params: list[Any] = []
        wallet_filter = ""
        if source_wallet:
            wallet_filter = (
                "AND json_valid(events.payload_json) "
                "AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)"
            )
            params.append(source_wallet)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT count(*)
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status = 'blocked'
                  {wallet_filter}
                """,
                tuple(params),
            ).fetchone()
        return int(row[0])

    def source_reaction_retry_counts(
        self,
        *,
        source_wallet: str,
        retry_due_ms: int,
    ) -> dict[str, int]:
        """Partition open reactions into paced HIP-3 and other blocking work.

        A paced reaction must be blocked and carry both the exact retry class and
        an integer retry deadline. Malformed typed outcomes remain in
        ``other_blocking_unfinished`` so startup cannot silently ignore them.
        """

        placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        with self.lock:
            row = self.conn.execute(
                f"""
                WITH scoped AS (
                  SELECT
                    reactions.status AS status,
                    CASE
                      WHEN reactions.status = 'blocked'
                       AND json_valid(reactions.outcome_json)
                       AND json_extract(
                             reactions.outcome_json,
                             '$.retry.class'
                           ) = '{HIP3_LIQUIDITY_RETRY_CLASS}'
                       AND json_type(
                             reactions.outcome_json,
                             '$.retry.retry_not_before_ms'
                           ) = 'integer'
                      THEN 1
                      ELSE 0
                    END AS is_hip3_liquidity,
                    CASE
                      WHEN json_valid(reactions.outcome_json)
                      THEN CASE
                        WHEN json_type(
                               reactions.outcome_json,
                               '$.retry.retry_not_before_ms'
                             ) = 'integer'
                        THEN CAST(
                               json_extract(
                                 reactions.outcome_json,
                                 '$.retry.retry_not_before_ms'
                               ) AS INTEGER
                             )
                        ELSE NULL
                      END
                      ELSE NULL
                    END AS retry_not_before_ms
                  FROM source_event_reactions AS reactions
                  JOIN source_events AS events
                    ON events.idempotency_key = reactions.source_event_key
                  WHERE reactions.status IN ({placeholders})
                    AND json_valid(events.payload_json)
                    AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)
                )
                SELECT
                  coalesce(sum(
                    CASE
                      WHEN is_hip3_liquidity = 1 AND retry_not_before_ms > ? THEN 1
                      ELSE 0
                    END
                  ), 0) AS hip3_liquidity_waiting,
                  coalesce(sum(
                    CASE
                      WHEN is_hip3_liquidity = 1 AND retry_not_before_ms <= ? THEN 1
                      ELSE 0
                    END
                  ), 0) AS hip3_liquidity_due,
                  coalesce(sum(
                    CASE WHEN is_hip3_liquidity = 0 THEN 1 ELSE 0 END
                  ), 0) AS other_blocking_unfinished
                FROM scoped
                """,
                (
                    *SOURCE_REACTION_OPEN_STATUSES,
                    source_wallet,
                    int(retry_due_ms),
                    int(retry_due_ms),
                ),
            ).fetchone()
        assert row is not None
        return {
            "hip3_liquidity_waiting": int(row["hip3_liquidity_waiting"]),
            "hip3_liquidity_due": int(row["hip3_liquidity_due"]),
            "other_blocking_unfinished": int(row["other_blocking_unfinished"]),
        }

    def next_hip3_liquidity_retry_ms(self, *, source_wallet: str) -> int | None:
        """Return the earliest durable typed HIP-3 retry deadline for one wallet."""

        with self.lock:
            row = self.conn.execute(
                """
                SELECT min(CAST(
                  json_extract(
                    reactions.outcome_json,
                    '$.retry.retry_not_before_ms'
                  ) AS INTEGER
                ))
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status = 'blocked'
                  AND json_valid(reactions.outcome_json)
                  AND json_extract(reactions.outcome_json, '$.retry.class') = ?
                  AND json_type(
                        reactions.outcome_json,
                        '$.retry.retry_not_before_ms'
                      ) = 'integer'
                  AND json_valid(events.payload_json)
                  AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)
                """,
                (HIP3_LIQUIDITY_RETRY_CLASS, source_wallet),
            ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    def due_hip3_liquidity_reaction_events(
        self,
        *,
        source_wallet: str,
        retry_due_ms: int,
        limit: int = 10_000,
    ) -> list[SourceEvent]:
        """Return only due, typed HIP-3 liquidity reactions for worker wakeup."""

        with self.lock:
            rows = self.conn.execute(
                """
                SELECT events.*
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status = 'blocked'
                  AND json_valid(reactions.outcome_json)
                  AND json_extract(reactions.outcome_json, '$.retry.class') = ?
                  AND json_type(
                        reactions.outcome_json,
                        '$.retry.retry_not_before_ms'
                      ) = 'integer'
                  AND CAST(
                        json_extract(
                          reactions.outcome_json,
                          '$.retry.retry_not_before_ms'
                        ) AS INTEGER
                      ) <= ?
                  AND json_valid(events.payload_json)
                  AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)
                ORDER BY events.seq
                LIMIT ?
                """,
                (
                    HIP3_LIQUIDITY_RETRY_CLASS,
                    int(retry_due_ms),
                    source_wallet,
                    max(1, int(limit)),
                ),
            ).fetchall()
        return [self._source_event_from_row(row) for row in rows]

    def pending_source_reaction_events(
        self,
        *,
        source_wallet: str | None = None,
        limit: int = 10_000,
        through_reaction_rowid: int | None = None,
        retry_due_ms: int | None = None,
    ) -> list[SourceEvent]:
        placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        params: list[Any] = [*SOURCE_REACTION_OPEN_STATUSES]
        retry_due_filter = ""
        if retry_due_ms is not None:
            retry_due_filter = "AND " + _source_reaction_due_expression("reactions.")
            params.extend((HIP3_LIQUIDITY_RETRY_CLASS, int(retry_due_ms)))
        cutoff_filter = ""
        if through_reaction_rowid is not None:
            cutoff_filter = "AND reactions.rowid <= ?"
            params.append(int(through_reaction_rowid))
        wallet_filter = ""
        if source_wallet:
            wallet_filter = (
                "AND json_valid(events.payload_json) "
                "AND lower(json_extract(events.payload_json, '$.source_wallet')) = lower(?)"
            )
            params.append(source_wallet)
        params.append(max(1, int(limit)))
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT events.*
                FROM source_event_reactions AS reactions
                JOIN source_events AS events
                  ON events.idempotency_key = reactions.source_event_key
                WHERE reactions.status IN ({placeholders})
                  {retry_due_filter}
                  {cutoff_filter}
                  {wallet_filter}
                ORDER BY events.seq
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [self._source_event_from_row(row) for row in rows]

    def claim_source_reaction_keys(
        self,
        source_event_keys: Iterable[str],
        *,
        include_processing: bool = False,
        retry_due_ms: int | None = None,
    ) -> tuple[str, ...]:
        keys = tuple(dict.fromkeys(str(key) for key in source_event_keys if str(key)))
        if not keys:
            return ()
        claimable_statuses = (
            SOURCE_REACTION_OPEN_STATUSES
            if include_processing
            else SOURCE_REACTION_CLAIMABLE_STATUSES
        )
        key_placeholders = ",".join("?" for _ in keys)
        status_placeholders = ",".join("?" for _ in claimable_statuses)
        select_retry_filter = ""
        select_retry_params: tuple[Any, ...] = ()
        update_retry_filter = ""
        update_retry_params: tuple[Any, ...] = ()
        if retry_due_ms is not None:
            retry_params = (HIP3_LIQUIDITY_RETRY_CLASS, int(retry_due_ms))
            select_retry_filter = "AND " + _source_reaction_due_expression()
            select_retry_params = retry_params
            update_retry_filter = select_retry_filter
            update_retry_params = retry_params
        updated = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self.conn.execute(
                    f"""
                    SELECT source_event_key
                    FROM source_event_reactions
                    WHERE source_event_key IN ({key_placeholders})
                      AND status IN ({status_placeholders})
                      {select_retry_filter}
                    """,
                    (*keys, *claimable_statuses, *select_retry_params),
                ).fetchall()
                matched = {str(row["source_event_key"]) for row in rows}
                claimed = tuple(key for key in keys if key in matched)
                if not claimed:
                    self.conn.commit()
                    return ()
                claimed_placeholders = ",".join("?" for _ in claimed)
                cur = self.conn.execute(
                    f"""
                    UPDATE source_event_reactions
                    SET status = 'processing',
                        attempt_count = attempt_count + 1,
                        updated_ms = ?
                    WHERE source_event_key IN ({claimed_placeholders})
                      AND status IN ({status_placeholders})
                      {update_retry_filter}
                    """,
                    (
                        updated,
                        *claimed,
                        *claimable_statuses,
                        *update_retry_params,
                    ),
                )
                if cur.rowcount != len(claimed):
                    raise JournalIntegrityError("source reaction claim was not atomic")
                self.conn.commit()
                return claimed
            except Exception:
                self.conn.rollback()
                raise

    def claim_source_reactions(self, source_event_keys: Iterable[str]) -> int:
        return len(self.claim_source_reaction_keys(source_event_keys, include_processing=True))

    def finish_source_reactions(
        self,
        source_event_keys: Iterable[str],
        *,
        status: str,
        outcome: Any,
    ) -> int:
        allowed = {*SOURCE_REACTION_OPEN_STATUSES, *SOURCE_REACTION_FINAL_STATUSES}
        if status not in allowed:
            raise ValueError(f"unsupported source reaction status: {status}")
        keys = tuple(dict.fromkeys(str(key) for key in source_event_keys if str(key)))
        if not keys:
            return 0
        key_placeholders = ",".join("?" for _ in keys)
        status_placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    f"""
                    UPDATE source_event_reactions
                    SET status = ?, outcome_json = ?, updated_ms = ?
                    WHERE source_event_key IN ({key_placeholders})
                      AND status IN ({status_placeholders})
                    """,
                    (
                        status,
                        self._json(outcome),
                        now_ms(),
                        *keys,
                        *SOURCE_REACTION_OPEN_STATUSES,
                    ),
                )
        return int(cur.rowcount)

    def finish_all_open_source_reactions(
        self,
        *,
        source_wallet: str,
        status: str,
        outcome: Any,
        through_reaction_rowid: int,
    ) -> int:
        if status not in SOURCE_REACTION_FINAL_STATUSES:
            raise ValueError("bulk source reaction completion requires a final status")
        if through_reaction_rowid <= 0:
            raise ValueError("bulk source reaction completion requires a positive cutoff")
        status_placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    f"""
                    UPDATE source_event_reactions
                    SET status = ?, outcome_json = ?, updated_ms = ?
                    WHERE rowid <= ?
                      AND status IN ({status_placeholders})
                      AND source_event_key IN (
                        SELECT idempotency_key
                        FROM source_events
                        WHERE json_valid(payload_json)
                          AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                      )
                    """,
                    (
                        status,
                        self._json(outcome),
                        now_ms(),
                        through_reaction_rowid,
                        *SOURCE_REACTION_OPEN_STATUSES,
                        source_wallet,
                    ),
                )
        return int(cur.rowcount)

    def _append_runtime_event_in_transaction(
        self,
        *,
        event_key: str,
        partition_key: str,
        event_class: str,
        exchange_ts_ms: int,
        receive_wall_ms: int,
        receive_mono_ns: int,
        payload: Any,
        stream_state: str,
        generation: str,
        seed_snapshot: bool = False,
    ) -> tuple[int, bool]:
        """Durably append ingress and advance only the ingress cursor.

        The applied cursor is intentionally untouched.  A duplicate event returns the
        existing sequence so reconnect overlap can converge without inventing work.
        """

        if not event_key or not partition_key or not event_class:
            raise ValueError("runtime event identity fields must be non-empty")
        created = now_ms()
        with self.lock:
            if not self.conn.in_transaction:
                raise JournalIntegrityError("runtime event helper requires an active transaction")
            try:
                self.conn.execute(
                    """
                    INSERT INTO stream_partitions(
                      partition_key, stream_state, generation, updated_ms
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(partition_key) DO UPDATE SET
                      stream_state = excluded.stream_state,
                      generation = CASE
                        WHEN stream_partitions.generation = '' THEN excluded.generation
                        ELSE stream_partitions.generation
                      END,
                      last_frame_wall_ms = max(
                        stream_partitions.last_frame_wall_ms, excluded.updated_ms
                      ),
                      updated_ms = excluded.updated_ms
                    """,
                    (partition_key, stream_state, generation, created),
                )
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO runtime_events(
                      event_key, partition_key, event_class, exchange_ts_ms,
                      receive_wall_ms, receive_mono_ns, seed_snapshot, stream_state,
                      payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_key,
                        partition_key,
                        event_class,
                        int(exchange_ts_ms),
                        int(receive_wall_ms),
                        int(receive_mono_ns),
                        int(bool(seed_snapshot)),
                        stream_state,
                        self._json(payload),
                        created,
                    ),
                )
                inserted = cur.rowcount == 1
                row = self.conn.execute(
                    "SELECT ingress_seq FROM runtime_events WHERE event_key = ?",
                    (event_key,),
                ).fetchone()
                if row is None:
                    raise JournalIntegrityError("runtime event insert was not readable")
                ingress_seq = int(row["ingress_seq"])
                if inserted:
                    self.conn.execute(
                        """
                        UPDATE stream_partitions
                        SET ingress_cursor = max(ingress_cursor, ?),
                            last_valid_event_wall_ms = max(last_valid_event_wall_ms, ?),
                            updated_ms = ?
                        WHERE partition_key = ?
                        """,
                        (ingress_seq, int(receive_wall_ms), created, partition_key),
                    )
                return ingress_seq, inserted
            except Exception:
                raise

    def append_runtime_events(
        self,
        *,
        events: Iterable[Mapping[str, Any]],
    ) -> list[tuple[int, bool]]:
        """Append consecutive ingress commands in one ordered atomic transaction."""

        items = tuple(dict(event) for event in events)
        if not items:
            return []
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                results = [self._append_runtime_event_in_transaction(**item) for item in items]
                self.conn.commit()
                return results
            except BaseException:
                self.conn.rollback()
                raise

    def append_runtime_event(
        self,
        *,
        event_key: str,
        partition_key: str,
        event_class: str,
        exchange_ts_ms: int,
        receive_wall_ms: int,
        receive_mono_ns: int,
        payload: Any,
        stream_state: str,
        generation: str,
        seed_snapshot: bool = False,
    ) -> tuple[int, bool]:
        return self.append_runtime_events(
            events=(
                {
                    "event_key": event_key,
                    "partition_key": partition_key,
                    "event_class": event_class,
                    "exchange_ts_ms": exchange_ts_ms,
                    "receive_wall_ms": receive_wall_ms,
                    "receive_mono_ns": receive_mono_ns,
                    "payload": payload,
                    "stream_state": stream_state,
                    "generation": generation,
                    "seed_snapshot": seed_snapshot,
                },
            )
        )[0]

    def set_stream_partition_state(
        self,
        *,
        partition_key: str,
        stream_state: str,
        generation: str,
        gap_detail: str = "",
        last_frame_wall_ms: int | None = None,
    ) -> None:
        updated = now_ms()
        with self.lock:
            with self.conn:
                prior = self.conn.execute(
                    "SELECT stream_state, gap_detail FROM stream_partitions WHERE partition_key=?",
                    (partition_key,),
                ).fetchone()
                self.conn.execute(
                    """
                    INSERT INTO stream_partitions(
                      partition_key, stream_state, gap_detail, generation,
                      last_frame_wall_ms, updated_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(partition_key) DO UPDATE SET
                      stream_state = excluded.stream_state,
                      gap_detail = excluded.gap_detail,
                      generation = CASE
                        WHEN stream_partitions.generation = '' THEN excluded.generation
                        ELSE stream_partitions.generation
                      END,
                      last_frame_wall_ms = max(
                        stream_partitions.last_frame_wall_ms,
                        excluded.last_frame_wall_ms
                      ),
                      updated_ms = excluded.updated_ms
                    """,
                    (
                        partition_key,
                        stream_state,
                        gap_detail,
                        generation,
                        int(last_frame_wall_ms or 0),
                        updated,
                    ),
                )
                prior_state = "" if prior is None else str(prior["stream_state"])
                prior_gap = "" if prior is None else str(prior["gap_detail"])
                if prior_state != stream_state or prior_gap != gap_detail:
                    self.conn.execute(
                        "INSERT INTO stream_state_transitions("
                        "partition_key, generation, from_state, to_state, gap_detail, wall_ms"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            partition_key,
                            generation,
                            prior_state,
                            stream_state,
                            gap_detail,
                            updated,
                        ),
                    )

    def unapplied_runtime_events(
        self, *, partition_key: str, limit: int = 10_000
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 10_000:
            raise ValueError("runtime replay limit must be between 1 and 10000")
        with self.lock:
            partition = self.conn.execute(
                "SELECT applied_cursor FROM stream_partitions WHERE partition_key = ?",
                (partition_key,),
            ).fetchone()
            if partition is None:
                return []
            rows = self.conn.execute(
                """
                SELECT * FROM runtime_events
                WHERE partition_key = ? AND ingress_seq > ?
                ORDER BY ingress_seq LIMIT ?
                """,
                (partition_key, int(partition["applied_cursor"]), limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def applied_event_checkpoint(
        self, *, partition_key: str, event_classes: tuple[str, ...]
    ) -> dict[str, Any] | None:
        if not partition_key or not event_classes:
            raise ValueError("applied event checkpoint requires partition and event classes")
        placeholders = ",".join("?" for _ in event_classes)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT event_key, event_class, exchange_ts_ms, ingress_seq
                FROM runtime_events
                WHERE partition_key=?
                  AND event_class IN ({placeholders})
                  AND ingress_seq <= coalesce((
                    SELECT applied_cursor FROM stream_partitions WHERE partition_key=?
                  ), 0)
                ORDER BY exchange_ts_ms DESC, ingress_seq DESC
                LIMIT 1
                """,
                (partition_key, *event_classes, partition_key),
            ).fetchone()
        return None if row is None else dict(row)

    def commit_runtime_disposition(
        self,
        *,
        partition_key: str,
        through_ingress_seq: int,
        result_id: str,
        disposition: str,
        result_payload: Any,
        source_event_keys: Iterable[str] = (),
        source_reaction_status: str = "completed",
    ) -> None:
        """Atomically store one compact result and advance a partition checkpoint."""

        if through_ingress_seq <= 0:
            raise ValueError("through_ingress_seq must be positive")
        if not result_id or not disposition:
            raise ValueError("result identity and disposition must be non-empty")
        if source_reaction_status not in SOURCE_REACTION_FINAL_STATUSES:
            raise ValueError("runtime disposition requires a final reaction status")
        keys = tuple(dict.fromkeys(str(key) for key in source_event_keys if str(key)))
        updated = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                partition = self.conn.execute(
                    """
                    SELECT ingress_cursor, applied_cursor
                    FROM stream_partitions WHERE partition_key = ?
                    """,
                    (partition_key,),
                ).fetchone()
                if partition is None:
                    raise JournalIntegrityError("unknown stream partition")
                if through_ingress_seq > int(partition["ingress_cursor"]):
                    raise JournalIntegrityError("applied cursor cannot exceed ingress cursor")
                if through_ingress_seq < int(partition["applied_cursor"]):
                    raise JournalIntegrityError("applied cursor cannot move backwards")
                self.conn.execute(
                    """
                    INSERT INTO reaction_results(
                      result_id, disposition, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (result_id, disposition, self._json(result_payload), updated),
                )
                if keys:
                    placeholders = ",".join("?" for _ in keys)
                    status_placeholders = ",".join("?" for _ in SOURCE_REACTION_OPEN_STATUSES)
                    cur = self.conn.execute(
                        f"""
                        UPDATE source_event_reactions
                        SET status = ?, result_id = ?, outcome_json = ?, updated_ms = ?
                        WHERE source_event_key IN ({placeholders})
                          AND status IN ({status_placeholders})
                        """,
                        (
                            source_reaction_status,
                            result_id,
                            self._json({"result_id": result_id}),
                            updated,
                            *keys,
                            *SOURCE_REACTION_OPEN_STATUSES,
                        ),
                    )
                    if cur.rowcount != len(keys):
                        raise JournalIntegrityError(
                            "compact source reaction completion was not atomic"
                        )
                self.conn.execute(
                    """
                    UPDATE stream_partitions
                    SET applied_cursor = ?,
                        last_durable_checkpoint_wall_ms = ?,
                        updated_ms = ?
                    WHERE partition_key = ?
                    """,
                    (through_ingress_seq, updated, updated, partition_key),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def _commit_fast_reaction_in_transaction(
        self,
        *,
        partition_key: str,
        through_ingress_seq: int,
        source_revision: dict[str, Any],
        desired_state: dict[str, Any],
        result_id: str,
        disposition: str,
        disposition_payload: Any,
        deferred_mutations: Iterable[dict[str, Any]] = (),
        prepared_actions: Iterable[dict[str, Any]] = (),
        action_rate_limit: int = 12,
    ) -> None:
        """Commit the complete source reaction before advancing its applied cursor.

        Every exposure-increasing action, including immediately executable
        direct work, is coupled to its authoritative tracking row.  Reductions
        may couple to the terminalization of prior entry debt.  The overlap
        checks below enforce those two exact state-machine edges.
        """

        updated = now_ms()
        required_source = {
            "revision_id",
            "source_wallet",
            "revision",
            "catalog_revision",
            "checkpoint",
            "observed_wall_ms",
            "observed_mono_ns",
            "provenance",
        }
        required_desired = {"state_id", "source_event_key", "mode"}
        if required_source.difference(source_revision) or required_desired.difference(
            desired_state
        ):
            raise ValueError("fast reaction revision payload is incomplete")
        deferred_items = tuple(deferred_mutations)
        action_items = tuple(prepared_actions)
        if action_rate_limit < 1:
            raise ValueError("fast action-rate limit must be positive")
        deferred_keys = {
            (
                str(item["record"]["follower_account"]).lower(),
                str(item["record"]["canonical_market"]),
            )
            for item in deferred_items
        }
        action_keys = {
            (
                str(item["follower_account"]).lower(),
                str(item["canonical_market"]),
            )
            for item in action_items
        }
        if len(deferred_keys) != len(deferred_items) or len(action_keys) != len(action_items):
            raise ValueError("fast reaction contains duplicate follower/market work")
        overlap = deferred_keys & action_keys
        for key in overlap:
            mutation = next(
                item
                for item in deferred_items
                if (
                    str(item["record"]["follower_account"]).lower(),
                    str(item["record"]["canonical_market"]),
                )
                == key
            )
            action = next(
                item
                for item in action_items
                if (
                    str(item["follower_account"]).lower(),
                    str(item["canonical_market"]),
                )
                == key
            )
            coupled_release = (
                mutation["to_state"] == "released_pending_action"
                and action.get("source_path") in {"aggregate", "direct"}
                and mutation["record"].get("cloid") == action.get("cloid")
            )
            terminal_then_reduction = (
                str(mutation["to_state"]).startswith("terminal_")
                and action.get("kind") == "order"
                and action.get("reduce_only") is True
            )
            leverage_prerequisite = action.get("kind") == "updateLeverage" and mutation[
                "to_state"
            ] in {
                "blocked_stale_state",
                "blocked_rate",
                "blocked_catalog",
                "blocked_risk",
                "active_deferred",
            }
            if not (coupled_release or terminal_then_reduction or leverage_prerequisite):
                raise ValueError(
                    "fast reaction cannot persist direct and deferred work for one delta"
                )
        with self.lock:
            if not self.conn.in_transaction:
                raise JournalIntegrityError("fast reaction helper requires an active transaction")
            try:
                partition = self.conn.execute(
                    "SELECT ingress_cursor, applied_cursor FROM stream_partitions WHERE partition_key = ?",
                    (partition_key,),
                ).fetchone()
                if partition is None:
                    raise JournalIntegrityError("fast reaction references an unknown partition")
                if (
                    not int(partition["applied_cursor"])
                    <= through_ingress_seq
                    <= int(partition["ingress_cursor"])
                ):
                    raise JournalIntegrityError("fast reaction cursor is outside durable ingress")
                next_event = self.conn.execute(
                    """
                    SELECT min(ingress_seq) AS ingress_seq FROM runtime_events
                    WHERE partition_key=? AND ingress_seq>?
                    """,
                    (partition_key, int(partition["applied_cursor"])),
                ).fetchone()
                if (
                    next_event is None
                    or next_event["ingress_seq"] is None
                    or through_ingress_seq != int(next_event["ingress_seq"])
                ):
                    raise JournalIntegrityError(
                        "fast reaction must advance exactly one contiguous partition event"
                    )
                event = self.conn.execute(
                    """
                    SELECT 1 FROM runtime_events
                    WHERE partition_key = ? AND ingress_seq = ?
                    """,
                    (partition_key, through_ingress_seq),
                ).fetchone()
                if event is None:
                    raise JournalIntegrityError(
                        "fast reaction cursor does not identify a durable partition event"
                    )
                action_counts: dict[tuple[str, str], int] = {}
                for action in action_items:
                    key = (
                        str(action["generation"]),
                        str(action["follower_account"]).lower(),
                    )
                    action_counts[key] = action_counts.get(key, 0) + 1
                for (generation, follower), incoming_count in action_counts.items():
                    existing_count = int(
                        self.conn.execute(
                            """
                            SELECT count(*) FROM action_states
                            WHERE generation=? AND lower(follower_account)=?
                              AND created_ms>?
                            """,
                            (generation, follower, updated - 60_000),
                        ).fetchone()[0]
                    )
                    if existing_count + incoming_count > action_rate_limit:
                        raise JournalIntegrityError(
                            "durable per-follower action-rate ceiling would be exceeded"
                        )
                self.conn.execute(
                    """
                    INSERT INTO source_state_revisions(
                      revision_id, partition_key, source_wallet, revision,
                      catalog_revision, checkpoint, observed_wall_ms, observed_mono_ns,
                      provenance, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source_revision["revision_id"],
                        partition_key,
                        source_revision["source_wallet"],
                        int(source_revision["revision"]),
                        source_revision["catalog_revision"],
                        int(source_revision["checkpoint"]),
                        int(source_revision["observed_wall_ms"]),
                        int(source_revision["observed_mono_ns"]),
                        source_revision["provenance"],
                        self._json(source_revision),
                        updated,
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO desired_states(
                      state_id, source_event_key, mode, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        desired_state["state_id"],
                        desired_state["source_event_key"],
                        desired_state["mode"],
                        self._json(desired_state),
                        updated,
                    ),
                )
                self.conn.execute(
                    "INSERT INTO desired_state_commits(state_id, committed_ms) VALUES (?, ?)",
                    (desired_state["state_id"], updated),
                )
                self.conn.execute(
                    """
                    INSERT INTO reaction_results(
                      result_id, disposition, payload_json, created_ms
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (result_id, disposition, self._json(disposition_payload), updated),
                )
                for mutation in deferred_items:
                    record = mutation["record"]
                    from_state = str(mutation.get("from_state") or "")
                    to_state = str(mutation["to_state"])
                    current = self.conn.execute(
                        """
                        SELECT delta_id, state FROM deferred_deltas
                        WHERE generation=? AND lower(follower_account)=lower(?)
                          AND canonical_market=?
                        """,
                        (
                            record["generation"],
                            record["follower_account"],
                            record["canonical_market"],
                        ),
                    ).fetchone()
                    if current is None:
                        if from_state:
                            raise JournalIntegrityError(
                                "atomic deferred transition source row is missing"
                            )
                    elif (
                        str(current["delta_id"]) != str(record["delta_id"])
                        or str(current["state"]) != from_state
                    ):
                        raise JournalIntegrityError(
                            "atomic deferred transition source identity changed"
                        )
                    created = int(record.get("created_ms") or mutation["wall_ms"])
                    self.conn.execute(
                        """
                        INSERT INTO deferred_deltas(
                          delta_id, generation, follower_account, canonical_market,
                          direction, state, desired_revision, source_revision,
                          follower_revision, catalog_revision, book_revision,
                          source_basis_kind, source_basis_px, desired_qty, projected_qty,
                          inflight_qty, remaining_qty, suppression_watermark,
                          first_blocked_wall_ms, deadline_wall_ms, deadline_mono_ns,
                          cloid, payload_json, created_ms, updated_ms
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(generation, follower_account, canonical_market) DO UPDATE SET
                          delta_id=excluded.delta_id, direction=excluded.direction,
                          state=excluded.state, desired_revision=excluded.desired_revision,
                          source_revision=excluded.source_revision,
                          follower_revision=excluded.follower_revision,
                          catalog_revision=excluded.catalog_revision,
                          book_revision=excluded.book_revision,
                          source_basis_kind=excluded.source_basis_kind,
                          source_basis_px=excluded.source_basis_px,
                          desired_qty=excluded.desired_qty,
                          projected_qty=excluded.projected_qty,
                          inflight_qty=excluded.inflight_qty,
                          remaining_qty=excluded.remaining_qty,
                          suppression_watermark=excluded.suppression_watermark,
                          first_blocked_wall_ms=excluded.first_blocked_wall_ms,
                          deadline_wall_ms=excluded.deadline_wall_ms,
                          deadline_mono_ns=excluded.deadline_mono_ns,
                          cloid=excluded.cloid, payload_json=excluded.payload_json,
                          updated_ms=excluded.updated_ms
                        """,
                        (
                            record["delta_id"],
                            record["generation"],
                            record["follower_account"],
                            record["canonical_market"],
                            record["direction"],
                            to_state,
                            int(record["desired_revision"]),
                            int(record["source_revision"]),
                            int(record["follower_revision"]),
                            record["catalog_revision"],
                            int(record["book_revision"]),
                            record["source_basis_kind"],
                            None
                            if record.get("source_basis_px") is None
                            else str(record["source_basis_px"]),
                            str(record["desired_qty"]),
                            str(record["projected_qty"]),
                            str(record["inflight_qty"]),
                            str(record["remaining_qty"]),
                            str(record.get("suppression_watermark") or ""),
                            int(record["first_blocked_wall_ms"]),
                            int(record["deadline_wall_ms"]),
                            int(record["deadline_mono_ns"]),
                            str(record.get("cloid") or ""),
                            self._json(record),
                            created,
                            int(mutation["wall_ms"]),
                        ),
                    )
                    self.conn.execute(
                        """
                        INSERT INTO deferred_delta_transitions(
                          transition_id, delta_id, from_state, to_state, cause,
                          desired_revision, source_revision, follower_revision,
                          catalog_revision, book_revision, wall_ms, mono_ns, payload_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            mutation["transition_id"],
                            record["delta_id"],
                            from_state,
                            to_state,
                            mutation["cause"],
                            int(record["desired_revision"]),
                            int(record["source_revision"]),
                            int(record["follower_revision"]),
                            record["catalog_revision"],
                            int(record["book_revision"]),
                            int(mutation["wall_ms"]),
                            int(mutation["mono_ns"]),
                            self._json(mutation.get("transition_payload") or {}),
                        ),
                    )
                    for contribution in mutation.get("contributions", ()):
                        self.conn.execute(
                            """
                            INSERT INTO deferred_delta_contributions(
                              contribution_id, delta_id, source_event_key,
                              follower_equivalent_qty, source_fill_px,
                              source_basis_kind, exchange_ts_ms, payload_json, created_ms
                            ) VALUES (?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                contribution["contribution_id"],
                                record["delta_id"],
                                contribution["source_event_key"],
                                str(contribution["follower_equivalent_qty"]),
                                None
                                if contribution.get("source_fill_px") is None
                                else str(contribution["source_fill_px"]),
                                contribution["source_basis_kind"],
                                int(contribution["exchange_ts_ms"]),
                                self._json(contribution),
                                updated,
                            ),
                        )
                for action in action_items:
                    if (
                        self.conn.execute(
                            "SELECT 1 FROM action_states WHERE intent_id=?",
                            (action["intent_id"],),
                        ).fetchone()
                        is not None
                    ):
                        raise JournalIntegrityError("atomic prepared intent already exists")
                    self.conn.execute(
                        """
                        INSERT INTO action_states(
                          intent_id, cloid, generation, follower_account,
                          canonical_market, action_shard, signer_epoch, nonce,
                          request_id, state, payload_json, created_ms, updated_ms
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            action["intent_id"],
                            action["cloid"],
                            action["generation"],
                            action["follower_account"],
                            action["canonical_market"],
                            int(action["action_shard"]),
                            int(action["signer_epoch"]),
                            action.get("nonce"),
                            action.get("request_id"),
                            "prepared",
                            self._json(action),
                            int(action.get("created_ms") or updated),
                            updated,
                        ),
                    )
                    transition_id = (
                        "action-transition-"
                        + sha256(
                            f"{action['intent_id']}||prepared|{updated}|atomic_fast_reaction".encode(
                                "utf-8"
                            )
                        ).hexdigest()[:32]
                    )
                    self.conn.execute(
                        """
                        INSERT INTO action_state_transitions(
                          transition_id, intent_id, from_state, to_state, cause,
                          wall_ms, mono_ns, payload_json
                        ) VALUES (?, ?, '', 'prepared', 'atomic_fast_reaction', ?, ?, ?)
                        """,
                        (
                            transition_id,
                            action["intent_id"],
                            updated,
                            int(source_revision["observed_mono_ns"]),
                            self._json({"source_event_key": desired_state["source_event_key"]}),
                        ),
                    )
                self.conn.execute(
                    """
                    UPDATE stream_partitions
                    SET applied_cursor = ?, last_durable_checkpoint_wall_ms = ?, updated_ms = ?
                    WHERE partition_key = ?
                    """,
                    (through_ingress_seq, updated, updated, partition_key),
                )
            except Exception:
                raise

    def commit_fast_reactions(
        self,
        *,
        reactions: Iterable[Mapping[str, Any]],
    ) -> None:
        """Commit consecutive fast reactions in input order as one transaction."""

        items = tuple(dict(reaction) for reaction in reactions)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._commit_fast_reaction_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def commit_fast_reaction(
        self,
        *,
        partition_key: str,
        through_ingress_seq: int,
        source_revision: dict[str, Any],
        desired_state: dict[str, Any],
        result_id: str,
        disposition: str,
        disposition_payload: Any,
        deferred_mutations: Iterable[dict[str, Any]] = (),
        prepared_actions: Iterable[dict[str, Any]] = (),
        action_rate_limit: int = 12,
    ) -> None:
        self.commit_fast_reactions(
            reactions=(
                {
                    "partition_key": partition_key,
                    "through_ingress_seq": through_ingress_seq,
                    "source_revision": source_revision,
                    "desired_state": desired_state,
                    "result_id": result_id,
                    "disposition": disposition,
                    "disposition_payload": disposition_payload,
                    "deferred_mutations": deferred_mutations,
                    "prepared_actions": prepared_actions,
                    "action_rate_limit": action_rate_limit,
                },
            )
        )

    def _validate_fast_reaction_head_identity(
        self,
        *,
        prepared_actions: tuple[dict[str, Any], ...],
        head_intent_id: str,
        follower_revision: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Fence the one action allowed to advance with a fast reaction.

        All reaction actions are durably PREPARED by the base transaction.  Only
        the sorted head may acquire a follower in-flight revision and advance to
        COMMITTED; later actions must remain independently rejectable.
        """

        if not self.conn.in_transaction:
            raise JournalIntegrityError("fast reaction head helper requires an active transaction")
        try:

            def canonical_key(action: Mapping[str, Any]) -> tuple[int, int, str, str]:
                kind = str(action["kind"])
                reduce_only = action.get("reduce_only")
                if not isinstance(reduce_only, bool):
                    raise TypeError("reduce_only must be boolean")
                if kind == "cancel":
                    priority = 0
                elif kind == "order" and reduce_only:
                    priority = 1
                elif kind == "updateLeverage":
                    priority = 2
                elif kind == "order":
                    priority = 3
                else:
                    raise ValueError(f"unsupported fast reaction action kind {kind!r}")
                revisions = action["revisions"]
                if not isinstance(revisions, Mapping):
                    raise TypeError("revisions must be a mapping")
                return (
                    priority,
                    int(revisions["desired_revision"]),
                    str(action["canonical_market"]),
                    str(action["intent_id"]),
                )

            canonical_actions = tuple(sorted(prepared_actions, key=canonical_key))
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalIntegrityError("fast reaction prepared action order is invalid") from exc
        if canonical_actions != prepared_actions:
            raise JournalIntegrityError(
                "fast reaction prepared actions are not in canonical priority order"
            )
        matches = [
            dict(action)
            for action in prepared_actions
            if str(action.get("intent_id") or "") == head_intent_id
        ]
        if len(matches) != 1 or not prepared_actions or matches[0] != prepared_actions[0]:
            raise JournalIntegrityError(
                "fast reaction head must be the unique first prepared action"
            )
        action = matches[0]
        revisions = action.get("revisions")
        if not isinstance(revisions, Mapping):
            raise JournalIntegrityError("fast reaction head revision identity is missing")
        try:
            planned_revision = int(revisions["follower_revision"])
            next_revision = int(follower_revision["revision"])
            payload = follower_revision["payload"]
        except (KeyError, TypeError, ValueError) as exc:
            raise JournalIntegrityError("fast reaction follower revision is incomplete") from exc
        if not isinstance(payload, Mapping):
            raise JournalIntegrityError("fast reaction follower payload is invalid")
        follower = str(action["follower_account"])
        if (
            str(follower_revision.get("kind") or "") != "follower"
            or str(follower_revision.get("owner") or "").lower() != follower.lower()
            or str(payload.get("follower") or "").lower() != follower.lower()
            or next_revision != planned_revision + 1
            or str(follower_revision.get("catalog_revision") or "")
            != str(revisions.get("catalog_revision") or "")
            or str(follower_revision.get("revision_id") or "").lower()
            != f"follower:{follower}:{next_revision}".lower()
        ):
            raise JournalIntegrityError("fast reaction follower revision identity changed")
        prior = self.conn.execute(
            """
            SELECT revision, catalog_revision, payload_json
            FROM follower_state_revisions
            WHERE lower(follower_account)=lower(?)
            ORDER BY revision DESC LIMIT 1
            """,
            (follower,),
        ).fetchone()
        if (
            prior is None
            or int(prior["revision"]) != planned_revision
            or str(prior["catalog_revision"])
            != str(follower_revision.get("catalog_revision") or "")
        ):
            raise JournalIntegrityError("fast reaction follower base revision changed")
        try:
            prior_payload = json.loads(str(prior["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise JournalIntegrityError("fast reaction follower base payload is invalid") from exc
        inflight = payload.get("inflight_by_cloid")
        cloid = str(action["cloid"]).lower()
        entry = inflight.get(cloid) if isinstance(inflight, Mapping) else None
        prior_mapping = dict(prior_payload) if isinstance(prior_payload, Mapping) else {}
        prior_inflight = prior_mapping.get("inflight_by_cloid", {})
        if (
            not isinstance(prior_payload, Mapping)
            or not isinstance(inflight, Mapping)
            or not isinstance(entry, Mapping)
            or not isinstance(prior_inflight, Mapping)
        ):
            raise JournalIntegrityError("fast reaction follower in-flight identity changed")
        candidate_inflight = dict(inflight) if isinstance(inflight, Mapping) else {}
        candidate_inflight.pop(cloid, None)
        prior_identity = dict(prior_mapping)
        candidate_identity = dict(payload)
        for identity in (prior_identity, candidate_identity):
            for mutable_field in ("revision", "cause", "inflight_by_cloid", "detail"):
                identity.pop(mutable_field, None)
        try:
            observed_wall_ms = int(follower_revision["observed_wall_ms"])
            payload_revision = int(payload["revision"])
            planned_entry_revision = int(entry.get("planned_follower_revision", -1))
            truth_checkpoint = int(entry.get("truth_checkpoint", -1))
            updated_ms = int(entry.get("updated_ms", -1))
            kind = str(action["kind"])
            reduce_only = action["reduce_only"]
            if not isinstance(reduce_only, bool):
                raise TypeError("reduce_only must be boolean")
            expected_size = parse_decimal(action.get("expected_size"))
            wire_action = action["action"]
            if not isinstance(wire_action, Mapping):
                raise TypeError("action payload must be a mapping")
            if kind == "order":
                orders = wire_action.get("orders")
                if not isinstance(orders, list) or len(orders) != 1:
                    raise TypeError("order action must contain exactly one order")
                order = orders[0]
                if not isinstance(order, Mapping) or not isinstance(order.get("b"), bool):
                    raise TypeError("order side must be boolean")
                if expected_size <= 0:
                    raise ValueError("order expected size must be positive")
                expected_signed_qty = expected_size if order["b"] else -expected_size
                expected_target_leverage = None
                expected_is_cross = None
            elif kind == "updateLeverage":
                if expected_size != 0:
                    raise ValueError("leverage action expected size must be zero")
                target_leverage = wire_action["leverage"]
                is_cross = wire_action["isCross"]
                if (
                    isinstance(target_leverage, bool)
                    or not isinstance(target_leverage, int)
                    or target_leverage < 1
                    or not isinstance(is_cross, bool)
                ):
                    raise TypeError("leverage action identity is invalid")
                expected_signed_qty = parse_decimal("0")
                expected_target_leverage = target_leverage
                expected_is_cross = is_cross
            else:
                raise ValueError("only order and leverage heads may own follower in-flight state")
            original_signed_qty = parse_decimal(entry.get("original_signed_qty"))
            remaining_signed_qty = parse_decimal(entry.get("remaining_signed_qty"))
            legacy_signed_qty = parse_decimal(entry.get("signed_qty"))
            cumulative_filled_qty = parse_decimal(entry.get("cumulative_filled_qty"))
        except Exception as exc:
            raise JournalIntegrityError(
                "fast reaction follower in-flight entry is invalid"
            ) from exc
        expected_detail = {"intent_id": head_intent_id, "cloid": cloid}
        if (
            self._json(candidate_inflight) != self._json(prior_inflight)
            or self._json(candidate_identity) != self._json(prior_identity)
            or str(follower_revision.get("provenance") or "")
            != StateProvenance.COMMITTED_ACTION.value
            or str(entry.get("cloid") or "").lower() != cloid
            or str(entry.get("intent_id") or "") != head_intent_id
            or str(entry.get("market") or "") != str(action["canonical_market"])
            or str(entry.get("state") or "") != "committed_to_journal"
            or planned_entry_revision != planned_revision
            or truth_checkpoint != int(prior_payload.get("durable_checkpoint", -2))
            or payload_revision != next_revision
            or str(payload.get("cause") or "") != "action_inflight_reserved"
            or self._json(payload.get("detail") or {}) != self._json(expected_detail)
            or str(entry.get("action_kind") or "") != kind
            or original_signed_qty != expected_signed_qty
            or remaining_signed_qty != expected_signed_qty
            or legacy_signed_qty != expected_signed_qty
            or cumulative_filled_qty != 0
            or entry.get("target_leverage") != expected_target_leverage
            or entry.get("is_cross") is not expected_is_cross
            or entry.get("reduce_only") is not reduce_only
            or updated_ms != observed_wall_ms
        ):
            raise JournalIntegrityError("fast reaction follower in-flight identity changed")
        return action

    def _commit_fast_reaction_head_in_transaction(
        self,
        *,
        partition_key: str,
        through_ingress_seq: int,
        source_revision: dict[str, Any],
        desired_state: dict[str, Any],
        result_id: str,
        disposition: str,
        disposition_payload: Any,
        follower_revision: Mapping[str, Any],
        head_intent_id: str,
        head_wall_ms: int,
        head_mono_ns: int,
        deferred_mutations: Iterable[dict[str, Any]] = (),
        prepared_actions: Iterable[dict[str, Any]] = (),
        action_rate_limit: int = 12,
        dispatch: Mapping[str, Any] | None = None,
        terminal_rejection: Mapping[str, Any] | None = None,
    ) -> None:
        """Commit one complete reaction and its head action in one FULL transaction."""

        action_items = tuple(dict(action) for action in prepared_actions)
        if head_wall_ms < 1 or head_mono_ns < 0:
            raise ValueError("fast reaction head timestamps are invalid")
        head = self._validate_fast_reaction_head_identity(
            prepared_actions=action_items,
            head_intent_id=head_intent_id,
            follower_revision=follower_revision,
        )
        self._commit_fast_reaction_in_transaction(
            partition_key=partition_key,
            through_ingress_seq=through_ingress_seq,
            source_revision=source_revision,
            desired_state=desired_state,
            result_id=result_id,
            disposition=disposition,
            disposition_payload=disposition_payload,
            deferred_mutations=deferred_mutations,
            prepared_actions=action_items,
            action_rate_limit=action_rate_limit,
        )
        self._append_state_revision_in_transaction(**dict(follower_revision))
        committed_transition_id = (
            "action-transition-"
            + sha256(
                f"{head_intent_id}|prepared|committed_to_journal|atomic_fast_reaction_head".encode(
                    "utf-8"
                )
            ).hexdigest()[:32]
        )
        self._transition_action_state_in_transaction(
            action=head,
            transition_id=committed_transition_id,
            from_state="prepared",
            to_state="committed_to_journal",
            cause="atomic_fast_reaction_head_committed",
            wall_ms=head_wall_ms,
            mono_ns=head_mono_ns,
            payload={"source_event_key": desired_state["source_event_key"]},
            action_rate_limit=action_rate_limit,
        )
        if terminal_rejection is not None:
            if dispatch is not None:
                raise JournalIntegrityError(
                    "fast reaction head cannot be both dispatched and terminally rejected"
                )
            rejection_cause = str(terminal_rejection.get("cause") or "")
            rejection_payload = terminal_rejection.get("payload")
            blockers = (
                rejection_payload.get("blockers")
                if isinstance(rejection_payload, Mapping)
                else None
            )
            gate = rejection_payload.get("gate") if isinstance(rejection_payload, Mapping) else None
            if (
                rejection_cause != "last_mile_blocked"
                or not isinstance(rejection_payload, Mapping)
                or not isinstance(blockers, list)
                or not blockers
                or any(not isinstance(blocker, str) or not blocker for blocker in blockers)
                or not isinstance(gate, Mapping)
            ):
                raise JournalIntegrityError("fast reaction terminal rejection is invalid")
            rejected_transition_id = (
                "action-transition-"
                + sha256(
                    f"{head_intent_id}|committed_to_journal|rejected|{rejection_cause}".encode(
                        "utf-8"
                    )
                ).hexdigest()[:32]
            )
            self._transition_action_state_in_transaction(
                action=head,
                transition_id=rejected_transition_id,
                from_state="committed_to_journal",
                to_state="rejected",
                cause=rejection_cause,
                wall_ms=head_wall_ms,
                mono_ns=head_mono_ns,
                payload=dict(rejection_payload),
                action_rate_limit=action_rate_limit,
            )
            return
        if dispatch is None:
            return
        try:
            nonce = dispatch["nonce"]
            request_id = dispatch["request_id"]
            signed_payload = dispatch["signed_payload"]
            dispatch_action = dispatch["action"]
            minimum_remaining_ms = dispatch["minimum_remaining_ms"]
        except KeyError as exc:
            raise JournalIntegrityError("fast reaction dispatch identity is incomplete") from exc
        expected_action = dict(head)
        candidate_action = dict(dispatch_action) if isinstance(dispatch_action, Mapping) else {}
        for candidate in (expected_action, candidate_action):
            candidate["nonce"] = None
            candidate["request_id"] = None
            candidate.pop("signed_payload", None)
        signature = signed_payload.get("signature") if isinstance(signed_payload, Mapping) else None
        expires_after = (
            signed_payload.get("expiresAfter") if isinstance(signed_payload, Mapping) else None
        )
        deadline_wall_ms = head.get("deadline_wall_ms")
        if (
            isinstance(nonce, bool)
            or not isinstance(nonce, int)
            or nonce < 1
            or isinstance(request_id, bool)
            or not isinstance(request_id, int)
            or request_id < 1
            or isinstance(minimum_remaining_ms, bool)
            or not isinstance(minimum_remaining_ms, int)
            or minimum_remaining_ms < 250
            or not isinstance(signed_payload, Mapping)
            or not isinstance(dispatch_action, Mapping)
            or self._json(candidate_action) != self._json(expected_action)
            or signed_payload.get("action") != head.get("action")
            or signed_payload.get("nonce") != nonce
            or not isinstance(signature, Mapping)
            or set(signature) != {"r", "s", "v"}
            or isinstance(expires_after, bool)
            or not isinstance(expires_after, int)
            or (deadline_wall_ms is not None and expires_after > int(deadline_wall_ms))
        ):
            raise JournalIntegrityError("fast reaction signed dispatch identity changed")
        if expires_after <= now_ms() + minimum_remaining_ms:
            raise SignedDispatchExpired(
                "fast reaction signed dispatch has insufficient expiry margin"
            )
        signed_action = dict(dispatch_action)
        signed_action["nonce"] = nonce
        signed_action["request_id"] = None
        signed_action["signed_payload"] = dict(signed_payload)
        signed_transition_id = (
            "action-transition-"
            + sha256(
                f"{head_intent_id}|committed_to_journal|signed|{nonce}".encode("utf-8")
            ).hexdigest()[:32]
        )
        self._commit_signed_action_in_transaction(
            action=signed_action,
            transition_id=signed_transition_id,
            nonce=nonce,
            signed_payload=dict(signed_payload),
            wall_ms=head_wall_ms,
            mono_ns=head_mono_ns,
        )
        sent_action = dict(signed_action)
        sent_action["request_id"] = request_id
        sent_transition_id = (
            "action-transition-"
            + sha256(f"{head_intent_id}|signed|sent|{request_id}".encode("utf-8")).hexdigest()[:32]
        )
        sent_payload = dict(dispatch.get("payload") or {})
        if "request_id" in sent_payload and sent_payload["request_id"] != request_id:
            raise JournalIntegrityError("fast reaction transport payload request identity changed")
        sent_payload["request_id"] = request_id
        self._commit_transport_attempt_in_transaction(
            action=sent_action,
            transition_id=sent_transition_id,
            cause=str(dispatch.get("cause") or "atomic_fast_reaction_transport_committed"),
            wall_ms=head_wall_ms,
            mono_ns=head_mono_ns,
            minimum_remaining_ms=minimum_remaining_ms,
            payload=sent_payload,
        )

    def commit_fast_reaction_heads(
        self,
        *,
        reactions: Iterable[Mapping[str, Any]],
    ) -> None:
        """Commit consecutive reaction/head pairs in input order as one transaction."""

        items = tuple(dict(reaction) for reaction in reactions)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._commit_fast_reaction_head_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def commit_fast_reaction_head(self, **reaction: Any) -> None:
        self.commit_fast_reaction_heads(reactions=(reaction,))

    def _append_state_revision_in_transaction(
        self,
        *,
        kind: str,
        revision_id: str,
        owner: str,
        revision: int,
        catalog_revision: str,
        observed_wall_ms: int,
        observed_mono_ns: int,
        provenance: str,
        payload: Any,
        partition_key: str = "",
        checkpoint: int = 0,
    ) -> None:
        if not self.conn.in_transaction:
            raise JournalIntegrityError("state revision helper requires an active transaction")
        params: tuple[Any, ...]
        if kind == "source":
            sql = """
                INSERT INTO source_state_revisions(
                  revision_id, partition_key, source_wallet, revision,
                  catalog_revision, checkpoint, observed_wall_ms, observed_mono_ns,
                  provenance, payload_json, created_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                revision_id,
                partition_key,
                owner,
                int(revision),
                catalog_revision,
                int(checkpoint),
                int(observed_wall_ms),
                int(observed_mono_ns),
                provenance,
                self._json(payload),
                now_ms(),
            )
        elif kind == "follower":
            sql = """
                INSERT INTO follower_state_revisions(
                  revision_id, follower_account, revision, catalog_revision,
                  observed_wall_ms, observed_mono_ns, provenance, payload_json, created_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = (
                revision_id,
                owner,
                int(revision),
                catalog_revision,
                int(observed_wall_ms),
                int(observed_mono_ns),
                provenance,
                self._json(payload),
                now_ms(),
            )
        else:
            raise ValueError("state revision kind must be source or follower")
        self.conn.execute(sql, params)

    def append_state_revisions(
        self,
        *,
        revisions: Iterable[Mapping[str, Any]],
    ) -> None:
        """Append consecutive state revisions in input order as one transaction."""

        items = tuple(dict(revision) for revision in revisions)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._append_state_revision_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def append_state_revision(
        self,
        *,
        kind: str,
        revision_id: str,
        owner: str,
        revision: int,
        catalog_revision: str,
        observed_wall_ms: int,
        observed_mono_ns: int,
        provenance: str,
        payload: Any,
        partition_key: str = "",
        checkpoint: int = 0,
    ) -> None:
        self.append_state_revisions(
            revisions=(
                {
                    "kind": kind,
                    "revision_id": revision_id,
                    "owner": owner,
                    "revision": revision,
                    "catalog_revision": catalog_revision,
                    "observed_wall_ms": observed_wall_ms,
                    "observed_mono_ns": observed_mono_ns,
                    "provenance": provenance,
                    "payload": payload,
                    "partition_key": partition_key,
                    "checkpoint": checkpoint,
                },
            )
        )

    def upsert_deferred_delta(
        self,
        *,
        record: dict[str, Any],
        transition_id: str,
        from_state: str,
        to_state: str,
        cause: str,
        transition_wall_ms: int,
        transition_mono_ns: int,
        transition_payload: Any = None,
    ) -> None:
        required = {
            "delta_id",
            "generation",
            "follower_account",
            "canonical_market",
            "direction",
            "desired_revision",
            "source_revision",
            "follower_revision",
            "catalog_revision",
            "book_revision",
            "source_basis_kind",
            "desired_qty",
            "projected_qty",
            "inflight_qty",
            "remaining_qty",
            "first_blocked_wall_ms",
            "deadline_wall_ms",
            "deadline_mono_ns",
        }
        missing = required.difference(record)
        if missing:
            raise ValueError(f"deferred delta missing fields: {sorted(missing)}")
        created = int(record.get("created_ms") or transition_wall_ms)
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    """
                    INSERT INTO deferred_deltas(
                      delta_id, generation, follower_account, canonical_market,
                      direction, state, desired_revision, source_revision,
                      follower_revision, catalog_revision, book_revision,
                      source_basis_kind, source_basis_px, desired_qty, projected_qty,
                      inflight_qty, remaining_qty, suppression_watermark,
                      first_blocked_wall_ms, deadline_wall_ms, deadline_mono_ns,
                      cloid, payload_json, created_ms, updated_ms
                    ) VALUES (
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    ON CONFLICT(generation, follower_account, canonical_market) DO UPDATE SET
                      delta_id = excluded.delta_id,
                      direction = excluded.direction,
                      state = excluded.state,
                      desired_revision = excluded.desired_revision,
                      source_revision = excluded.source_revision,
                      follower_revision = excluded.follower_revision,
                      catalog_revision = excluded.catalog_revision,
                      book_revision = excluded.book_revision,
                      source_basis_kind = excluded.source_basis_kind,
                      source_basis_px = excluded.source_basis_px,
                      desired_qty = excluded.desired_qty,
                      projected_qty = excluded.projected_qty,
                      inflight_qty = excluded.inflight_qty,
                      remaining_qty = excluded.remaining_qty,
                      suppression_watermark = excluded.suppression_watermark,
                      first_blocked_wall_ms = excluded.first_blocked_wall_ms,
                      deadline_wall_ms = excluded.deadline_wall_ms,
                      deadline_mono_ns = excluded.deadline_mono_ns,
                      cloid = excluded.cloid,
                      payload_json = excluded.payload_json,
                      updated_ms = excluded.updated_ms
                    """,
                    (
                        record["delta_id"],
                        record["generation"],
                        record["follower_account"],
                        record["canonical_market"],
                        record["direction"],
                        to_state,
                        int(record["desired_revision"]),
                        int(record["source_revision"]),
                        int(record["follower_revision"]),
                        record["catalog_revision"],
                        int(record["book_revision"]),
                        record["source_basis_kind"],
                        None
                        if record.get("source_basis_px") is None
                        else str(record["source_basis_px"]),
                        str(record["desired_qty"]),
                        str(record["projected_qty"]),
                        str(record["inflight_qty"]),
                        str(record["remaining_qty"]),
                        str(record.get("suppression_watermark") or ""),
                        int(record["first_blocked_wall_ms"]),
                        int(record["deadline_wall_ms"]),
                        int(record["deadline_mono_ns"]),
                        str(record.get("cloid") or ""),
                        self._json(record),
                        created,
                        int(transition_wall_ms),
                    ),
                )
                self.conn.execute(
                    """
                    INSERT INTO deferred_delta_transitions(
                      transition_id, delta_id, from_state, to_state, cause,
                      desired_revision, source_revision, follower_revision,
                      catalog_revision, book_revision, wall_ms, mono_ns, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        transition_id,
                        record["delta_id"],
                        from_state,
                        to_state,
                        cause,
                        int(record["desired_revision"]),
                        int(record["source_revision"]),
                        int(record["follower_revision"]),
                        record["catalog_revision"],
                        int(record["book_revision"]),
                        int(transition_wall_ms),
                        int(transition_mono_ns),
                        self._json(transition_payload or {}),
                    ),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def append_deferred_contribution(
        self,
        *,
        contribution_id: str,
        delta_id: str,
        source_event_key: str,
        follower_equivalent_qty: Any,
        source_fill_px: Any | None,
        source_basis_kind: str,
        exchange_ts_ms: int,
        payload: Any,
    ) -> None:
        self._insert(
            """
            INSERT INTO deferred_delta_contributions(
              contribution_id, delta_id, source_event_key, follower_equivalent_qty,
              source_fill_px, source_basis_kind, exchange_ts_ms, payload_json, created_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contribution_id,
                delta_id,
                source_event_key,
                str(follower_equivalent_qty),
                None if source_fill_px is None else str(source_fill_px),
                source_basis_kind,
                int(exchange_ts_ms),
                self._json(payload),
                now_ms(),
            ),
            ignore_duplicate=True,
        )

    def append_catalog_revision(
        self,
        *,
        revision_id: str,
        policy_version: str,
        snapshot_sha256: str,
        dex_bracket_before_sha256: str,
        dex_bracket_after_sha256: str,
        payload: Any,
        diffs: Iterable[dict[str, Any]] = (),
    ) -> None:
        accepted = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    """
                    INSERT INTO catalog_revisions(
                      revision_id, policy_version, snapshot_sha256,
                      dex_bracket_before_sha256, dex_bracket_after_sha256,
                      payload_json, accepted_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        revision_id,
                        policy_version,
                        snapshot_sha256,
                        dex_bracket_before_sha256,
                        dex_bracket_after_sha256,
                        self._json(payload),
                        accepted,
                    ),
                )
                raw_markets = payload.get("markets") if isinstance(payload, dict) else None
                if not isinstance(raw_markets, list):
                    raise JournalIntegrityError("catalog revision payload has no market array")
                for market in raw_markets:
                    if not isinstance(market, dict):
                        raise JournalIntegrityError("catalog revision market is malformed")
                    identities = [(market.get("asset_id"), market.get("symbol"))]
                    if market.get("pending_asset_id") is not None:
                        identities.append((market.get("pending_asset_id"), market.get("symbol")))
                    for asset_id, symbol in identities:
                        if asset_id is None or symbol is None:
                            raise JournalIntegrityError(
                                "catalog revision market identity is incomplete"
                            )
                        resolved_asset_id = int(asset_id)
                        prior = self.conn.execute(
                            "SELECT canonical_market FROM catalog_asset_history WHERE asset_id=?",
                            (resolved_asset_id,),
                        ).fetchone()
                        if prior is not None and str(prior[0]) != str(symbol):
                            if str(market.get("readiness")) != "UNTRUSTED":
                                raise JournalIntegrityError(
                                    f"catalog asset ID {asset_id} was reused by {symbol}"
                                )
                            # The first owner remains authoritative.  The
                            # conflicting UNTRUSTED candidate is still durably
                            # recorded in the revision/diff as market-local
                            # incident evidence.
                            continue
                        self.conn.execute(
                            """
                            INSERT OR IGNORE INTO catalog_asset_history(
                              asset_id, canonical_market, first_revision_id, created_ms
                            ) VALUES (?, ?, ?, ?)
                            """,
                            (resolved_asset_id, str(symbol), revision_id, accepted),
                        )
                for diff in diffs:
                    self.conn.execute(
                        """
                        INSERT INTO catalog_diffs(
                          diff_id, from_revision_id, to_revision_id, change_class,
                          canonical_market, payload_json, created_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            diff["diff_id"],
                            str(diff.get("from_revision_id") or ""),
                            revision_id,
                            diff["change_class"],
                            diff["canonical_market"],
                            self._json(diff),
                            accepted,
                        ),
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def append_catalog_market_transitions(self, *, transitions: Iterable[dict[str, Any]]) -> None:
        rows = tuple(transitions)
        if not rows:
            return
        created = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                for transition in rows:
                    self.conn.execute(
                        """
                        INSERT INTO catalog_market_transitions(
                          transition_id, revision_id, canonical_market,
                          transition_class, from_readiness, to_readiness,
                          observed_ms, frame_sha256, payload_json, created_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            transition["transition_id"],
                            transition["revision_id"],
                            transition["canonical_market"],
                            transition["transition_class"],
                            transition["from_readiness"],
                            transition["to_readiness"],
                            int(transition["observed_ms"]),
                            transition["frame_sha256"],
                            self._json(transition),
                            created,
                        ),
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def _transition_action_state_in_transaction(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        from_state: str,
        to_state: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        payload: Any = None,
        action_rate_limit: int = 12,
    ) -> None:
        if action_rate_limit < 1:
            raise ValueError("action-rate limit must be positive")
        if not self.conn.in_transaction:
            raise JournalIntegrityError("action transition helper requires an active transaction")
        prior = self.conn.execute(
            "SELECT * FROM action_states WHERE intent_id = ?",
            (action["intent_id"],),
        ).fetchone()
        immutable = (
            "cloid",
            "generation",
            "follower_account",
            "canonical_market",
            "action_shard",
            "signer_epoch",
        )
        if prior is None:
            if from_state:
                raise JournalIntegrityError("action transition source row is missing")
            recent = int(
                self.conn.execute(
                    """
                    SELECT count(*) FROM action_states
                    WHERE generation=? AND lower(follower_account)=?
                      AND created_ms>?
                    """,
                    (
                        action["generation"],
                        str(action["follower_account"]).lower(),
                        int(action.get("created_ms") or wall_ms) - 60_000,
                    ),
                ).fetchone()[0]
            )
            if recent >= action_rate_limit:
                raise JournalIntegrityError(
                    "durable per-follower action-rate ceiling would be exceeded"
                )
        else:
            if not from_state or str(prior["state"]) != from_state:
                raise JournalIntegrityError("action transition source state changed")
            for field in immutable:
                expected = str(action[field]).lower()
                observed = str(prior[field]).lower()
                if observed != expected:
                    raise JournalIntegrityError(f"action immutable identity changed: {field}")
        stored_action = dict(action)
        if prior is not None:
            try:
                prior_payload = json.loads(str(prior["payload_json"]))
            except Exception:
                prior_payload = {}
            if (
                "signed_payload" not in stored_action
                and isinstance(prior_payload, dict)
                and "signed_payload" in prior_payload
            ):
                stored_action["signed_payload"] = prior_payload["signed_payload"]
        self.conn.execute(
            """
            INSERT INTO action_states(
              intent_id, cloid, generation, follower_account, canonical_market,
              action_shard, signer_epoch, nonce, request_id, state,
              payload_json, created_ms, updated_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(intent_id) DO UPDATE SET
              nonce = coalesce(excluded.nonce, action_states.nonce),
              request_id = coalesce(excluded.request_id, action_states.request_id),
              state = excluded.state,
              payload_json = excluded.payload_json,
              updated_ms = excluded.updated_ms
            """,
            (
                action["intent_id"],
                action["cloid"],
                action["generation"],
                action["follower_account"],
                action["canonical_market"],
                int(action["action_shard"]),
                int(action["signer_epoch"]),
                action.get("nonce"),
                action.get("request_id"),
                to_state,
                self._json(stored_action),
                int(action.get("created_ms") or wall_ms),
                int(wall_ms),
            ),
        )
        self.conn.execute(
            """
            INSERT INTO action_state_transitions(
              transition_id, intent_id, from_state, to_state, cause,
              wall_ms, mono_ns, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transition_id,
                action["intent_id"],
                from_state,
                to_state,
                cause,
                int(wall_ms),
                int(mono_ns),
                self._json(payload or {}),
            ),
        )

    def transition_action_states(
        self,
        *,
        transitions: Iterable[Mapping[str, Any]],
    ) -> None:
        """Apply consecutive action transitions in input order as one transaction."""

        items = tuple(dict(transition) for transition in transitions)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._transition_action_state_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def transition_action_state(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        from_state: str,
        to_state: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        payload: Any = None,
        action_rate_limit: int = 12,
    ) -> None:
        self.transition_action_states(
            transitions=(
                {
                    "action": action,
                    "transition_id": transition_id,
                    "from_state": from_state,
                    "to_state": to_state,
                    "cause": cause,
                    "wall_ms": wall_ms,
                    "mono_ns": mono_ns,
                    "payload": payload,
                    "action_rate_limit": action_rate_limit,
                },
            )
        )

    def record_stage_timing(
        self,
        *,
        timing_id: str,
        generation: str,
        source_shard: int,
        slot_id: str,
        stage: str,
        wall_ms: int,
        mono_ns: int,
        duration_ns: int | None = None,
        intent_id: str = "",
        event_key: str = "",
        excluded_reason: str = "",
        payload: Any = None,
    ) -> None:
        self.record_stage_timings(
            timings=(
                {
                    "timing_id": timing_id,
                    "generation": generation,
                    "source_shard": source_shard,
                    "slot_id": slot_id,
                    "stage": stage,
                    "wall_ms": wall_ms,
                    "mono_ns": mono_ns,
                    "duration_ns": duration_ns,
                    "intent_id": intent_id,
                    "event_key": event_key,
                    "excluded_reason": excluded_reason,
                    "payload": payload,
                },
            )
        )

    def record_stage_timings(self, *, timings: Iterable[Mapping[str, Any]]) -> None:
        rows = [
            (
                str(timing["timing_id"]),
                str(timing["generation"]),
                int(timing["source_shard"]),
                str(timing["slot_id"]),
                str(timing.get("intent_id") or ""),
                str(timing.get("event_key") or ""),
                str(timing["stage"]),
                int(timing["wall_ms"]),
                int(timing["mono_ns"]),
                (None if timing.get("duration_ns") is None else int(timing["duration_ns"])),
                str(timing.get("excluded_reason") or ""),
                self._json(timing.get("payload") or {}),
                now_ms(),
            )
            for timing in timings
        ]
        if not rows:
            return
        with self.lock:
            with self.conn:
                self.conn.executemany(
                    """
                    INSERT OR IGNORE INTO stage_timings(
                      timing_id, generation, source_shard, slot_id, intent_id, event_key,
                      stage, wall_ms, mono_ns, duration_ns, excluded_reason,
                      payload_json, created_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def initialize_signer_epoch(
        self,
        *,
        follower_account: str,
        generation: str,
        signer_epoch: int,
        transport_epoch: int,
        rest_epoch: int,
        next_nonce: int,
    ) -> None:
        if min(signer_epoch, transport_epoch, rest_epoch, next_nonce) < 1:
            raise ValueError("signer epoch and nonce fields must be positive")
        updated = now_ms()
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT * FROM signer_epochs WHERE lower(follower_account) = lower(?)",
                    (follower_account,),
                ).fetchone()
                if row is None:
                    self.conn.execute(
                        """
                        INSERT INTO signer_epochs(
                          follower_account, generation, signer_epoch, transport_epoch,
                          rest_epoch, next_nonce, signed_unsent_count, updated_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
                        """,
                        (
                            follower_account.lower(),
                            generation,
                            signer_epoch,
                            transport_epoch,
                            rest_epoch,
                            next_nonce,
                            updated,
                        ),
                    )
                elif (
                    row["generation"] != generation
                    or int(row["signer_epoch"]) != signer_epoch
                    or int(row["transport_epoch"]) != transport_epoch
                    or int(row["rest_epoch"]) != rest_epoch
                ):
                    raise JournalIntegrityError("signer epoch is owned by another generation")
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def allocate_signer_nonce(
        self,
        *,
        follower_account: str,
        generation: str,
        signer_epoch: int,
        wall_ms: int,
    ) -> int:
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    """
                    SELECT next_nonce FROM signer_epochs
                    WHERE lower(follower_account) = lower(?)
                      AND generation = ? AND signer_epoch = ?
                    """,
                    (follower_account, generation, signer_epoch),
                ).fetchone()
                if row is None:
                    raise JournalIntegrityError("signer nonce allocation failed its epoch fence")
                nonce = max(int(row["next_nonce"]), int(wall_ms))
                cur = self.conn.execute(
                    """
                    UPDATE signer_epochs SET next_nonce = ?, updated_ms = ?
                    WHERE lower(follower_account) = lower(?)
                      AND generation = ? AND signer_epoch = ? AND next_nonce <= ?
                    """,
                    (
                        nonce + 1,
                        int(wall_ms),
                        follower_account,
                        generation,
                        signer_epoch,
                        nonce,
                    ),
                )
                if cur.rowcount != 1:
                    raise JournalIntegrityError("signer nonce allocation CAS failed")
                self.conn.commit()
                return nonce
            except Exception:
                self.conn.rollback()
                raise

    def peek_signer_nonces(
        self,
        *,
        requests: Iterable[Mapping[str, Any]],
    ) -> list[int]:
        items = tuple(dict(request) for request in requests)
        if not items:
            return []
        values: list[int] = []
        with self.lock:
            for item in items:
                row = self.conn.execute(
                    """
                    SELECT next_nonce FROM signer_epochs
                    WHERE lower(follower_account) = lower(?)
                      AND generation = ? AND signer_epoch = ?
                    """,
                    (
                        item["follower_account"],
                        item["generation"],
                        int(item["signer_epoch"]),
                    ),
                ).fetchone()
                if row is None:
                    raise JournalIntegrityError("signer nonce preview failed its epoch fence")
                values.append(max(int(row["next_nonce"]), int(item["wall_ms"])))
        return values

    def peek_signer_nonce(
        self,
        *,
        follower_account: str,
        generation: str,
        signer_epoch: int,
        wall_ms: int,
    ) -> int:
        return self.peek_signer_nonces(
            requests=(
                {
                    "follower_account": follower_account,
                    "generation": generation,
                    "signer_epoch": signer_epoch,
                    "wall_ms": wall_ms,
                },
            )
        )[0]

    def _commit_signed_action_in_transaction(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        nonce: int,
        signed_payload: Any,
        wall_ms: int,
        mono_ns: int,
    ) -> None:
        """Atomically reserve a nonce, count signed-unsent work, and persist SIGNED."""

        follower_account = str(action["follower_account"])
        generation = str(action["generation"])
        signer_epoch = int(action["signer_epoch"])
        signed_action = dict(action)
        signed_action["nonce"] = int(nonce)
        signed_action["signed_payload"] = signed_payload
        with self.lock:
            if not self.conn.in_transaction:
                raise JournalIntegrityError("signed action helper requires an active transaction")
            try:
                owner = self.conn.execute(
                    """
                    SELECT next_nonce, signed_unsent_count FROM signer_epochs
                    WHERE lower(follower_account) = lower(?)
                      AND generation = ? AND signer_epoch = ?
                    """,
                    (follower_account, generation, signer_epoch),
                ).fetchone()
                if owner is None or nonce < int(owner["next_nonce"]):
                    raise JournalIntegrityError("signed action lost its signer nonce fence")
                prior = self.conn.execute(
                    "SELECT * FROM action_states WHERE intent_id = ?",
                    (action["intent_id"],),
                ).fetchone()
                if prior is None or str(prior["state"]) != "committed_to_journal":
                    raise JournalIntegrityError("signed action is not durably committed")
                if (
                    str(prior["generation"]) != generation
                    or str(prior["follower_account"]).lower() != follower_account.lower()
                    or int(prior["signer_epoch"]) != signer_epoch
                ):
                    raise JournalIntegrityError("signed action immutable epoch identity changed")
                signer_cur = self.conn.execute(
                    """
                    UPDATE signer_epochs
                    SET next_nonce = ?, signed_unsent_count = ?, updated_ms = ?
                    WHERE lower(follower_account) = lower(?)
                      AND generation = ? AND signer_epoch = ? AND next_nonce <= ?
                    """,
                    (
                        nonce + 1,
                        int(owner["signed_unsent_count"]) + 1,
                        wall_ms,
                        follower_account,
                        generation,
                        signer_epoch,
                        nonce,
                    ),
                )
                if signer_cur.rowcount != 1:
                    raise JournalIntegrityError("signed action signer nonce CAS failed")
                action_cur = self.conn.execute(
                    """
                    UPDATE action_states
                    SET nonce=?, state='signed', payload_json=?, updated_ms=?
                    WHERE intent_id=? AND state='committed_to_journal'
                    """,
                    (
                        nonce,
                        self._json(signed_action),
                        wall_ms,
                        action["intent_id"],
                    ),
                )
                if action_cur.rowcount != 1:
                    raise JournalIntegrityError("signed action state CAS failed")
                self.conn.execute(
                    """
                    INSERT INTO action_state_transitions(
                      transition_id, intent_id, from_state, to_state, cause,
                      wall_ms, mono_ns, payload_json
                    ) VALUES (?, ?, 'committed_to_journal', 'signed',
                              'signed_by_epoch_owner', ?, ?, ?)
                    """,
                    (
                        transition_id,
                        action["intent_id"],
                        wall_ms,
                        mono_ns,
                        self._json({"signed_payload": signed_payload}),
                    ),
                )
            except Exception:
                raise

    def commit_signed_actions(
        self,
        *,
        actions: Iterable[Mapping[str, Any]],
    ) -> None:
        """Commit consecutive signed actions in input order as one transaction."""

        items = tuple(dict(action) for action in actions)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._commit_signed_action_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def commit_signed_action(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        nonce: int,
        signed_payload: Any,
        wall_ms: int,
        mono_ns: int,
    ) -> None:
        self.commit_signed_actions(
            actions=(
                {
                    "action": action,
                    "transition_id": transition_id,
                    "nonce": nonce,
                    "signed_payload": signed_payload,
                    "wall_ms": wall_ms,
                    "mono_ns": mono_ns,
                },
            )
        )

    def _adjust_signed_unsent_in_transaction(
        self,
        *,
        follower_account: str,
        generation: str,
        signer_epoch: int,
        delta: int,
    ) -> int:
        if delta not in {-1, 1}:
            raise ValueError("signed-unsent adjustment must be -1 or 1")
        if not self.conn.in_transaction:
            raise JournalIntegrityError("signed-unsent helper requires an active transaction")
        updated = now_ms()
        row = self.conn.execute(
            """
            SELECT signed_unsent_count FROM signer_epochs
            WHERE lower(follower_account) = lower(?)
              AND generation = ? AND signer_epoch = ?
            """,
            (follower_account, generation, signer_epoch),
        ).fetchone()
        if row is None:
            raise JournalIntegrityError("signed-unsent adjustment failed its epoch fence")
        value = int(row["signed_unsent_count"]) + delta
        if value < 0:
            raise JournalIntegrityError("signed-unsent count cannot be negative")
        cur = self.conn.execute(
            """
            UPDATE signer_epochs SET signed_unsent_count = ?, updated_ms = ?
            WHERE lower(follower_account) = lower(?)
              AND generation = ? AND signer_epoch = ?
              AND signed_unsent_count = ?
            """,
            (
                value,
                updated,
                follower_account,
                generation,
                signer_epoch,
                int(row["signed_unsent_count"]),
            ),
        )
        if cur.rowcount != 1:
            raise JournalIntegrityError("signed-unsent adjustment CAS failed")
        return value

    def adjust_signed_unsents(
        self,
        *,
        adjustments: Iterable[Mapping[str, Any]],
    ) -> list[int]:
        items = tuple(dict(adjustment) for adjustment in adjustments)
        if not items:
            return []
        values: list[int] = []
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    values.append(self._adjust_signed_unsent_in_transaction(**item))
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise
        return values

    def adjust_signed_unsent(
        self,
        *,
        follower_account: str,
        generation: str,
        signer_epoch: int,
        delta: int,
    ) -> int:
        return self.adjust_signed_unsents(
            adjustments=(
                {
                    "follower_account": follower_account,
                    "generation": generation,
                    "signer_epoch": signer_epoch,
                    "delta": delta,
                },
            )
        )[0]

    def _commit_transport_attempt_in_transaction(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        minimum_remaining_ms: int,
        payload: Any = None,
    ) -> None:
        """Atomically transfer one durable SIGNED action to transport ownership."""

        self._validate_signed_release_identity(action=action, require_request_id=True)
        signed_payload = action.get("signed_payload")
        expires_after = (
            signed_payload.get("expiresAfter") if isinstance(signed_payload, Mapping) else None
        )
        deadline_wall_ms = action.get("deadline_wall_ms")
        if (
            isinstance(minimum_remaining_ms, bool)
            or not isinstance(minimum_remaining_ms, int)
            or minimum_remaining_ms < 250
            or isinstance(expires_after, bool)
            or not isinstance(expires_after, int)
            or isinstance(deadline_wall_ms, bool)
            or not isinstance(deadline_wall_ms, int)
            or min(expires_after, deadline_wall_ms) <= now_ms() + minimum_remaining_ms
        ):
            raise SignedDispatchExpired("signed transport attempt has insufficient expiry margin")
        self._transition_action_state_in_transaction(
            action=action,
            transition_id=transition_id,
            from_state="signed",
            to_state="sent",
            cause=cause,
            wall_ms=wall_ms,
            mono_ns=mono_ns,
            payload=payload,
        )
        self._adjust_signed_unsent_in_transaction(
            follower_account=str(action["follower_account"]),
            generation=str(action["generation"]),
            signer_epoch=int(action["signer_epoch"]),
            delta=-1,
        )

    def _validate_signed_release_identity(
        self,
        *,
        action: Mapping[str, Any],
        require_request_id: bool,
    ) -> None:
        """Fence the nonce, signature, and request identity at SIGNED ownership release."""

        if not self.conn.in_transaction:
            raise JournalIntegrityError("signed release helper requires an active transaction")
        prior = self.conn.execute(
            "SELECT nonce, request_id, state, payload_json FROM action_states WHERE intent_id=?",
            (action["intent_id"],),
        ).fetchone()
        if prior is None or str(prior["state"]) != "signed":
            raise JournalIntegrityError("signed release source action is not durably SIGNED")
        action_nonce = action.get("nonce")
        if (
            isinstance(action_nonce, bool)
            or not isinstance(action_nonce, int)
            or action_nonce < 1
            or prior["nonce"] is None
            or int(prior["nonce"]) != action_nonce
        ):
            raise JournalIntegrityError("signed release nonce identity changed")
        try:
            prior_payload = json.loads(str(prior["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise JournalIntegrityError("durable signed action payload is invalid") from exc
        durable_signed = (
            prior_payload.get("signed_payload") if isinstance(prior_payload, dict) else None
        )
        candidate_signed = action.get("signed_payload")
        if not isinstance(durable_signed, dict) or not isinstance(candidate_signed, Mapping):
            raise JournalIntegrityError("signed release payload is missing")
        signed_nonce = candidate_signed.get("nonce")
        if (
            isinstance(signed_nonce, bool)
            or not isinstance(signed_nonce, int)
            or signed_nonce != action_nonce
            or self._json(durable_signed) != self._json(candidate_signed)
        ):
            raise JournalIntegrityError("signed release payload identity changed")
        request_id = action.get("request_id")
        if require_request_id:
            if (
                isinstance(request_id, bool)
                or not isinstance(request_id, int)
                or request_id < 1
                or prior["request_id"] is not None
            ):
                raise JournalIntegrityError("transport request identity is invalid")
        elif request_id is not None or prior["request_id"] is not None:
            raise JournalIntegrityError("unsent signed expiry cannot own a request id")

    def commit_transport_attempts(
        self,
        *,
        attempts: Iterable[Mapping[str, Any]],
    ) -> None:
        """Commit consecutive pre-send ownership transfers in one transaction."""

        items = tuple(dict(attempt) for attempt in attempts)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._commit_transport_attempt_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def commit_transport_attempt(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        minimum_remaining_ms: int,
        payload: Any = None,
    ) -> None:
        self.commit_transport_attempts(
            attempts=(
                {
                    "action": action,
                    "transition_id": transition_id,
                    "cause": cause,
                    "wall_ms": wall_ms,
                    "mono_ns": mono_ns,
                    "minimum_remaining_ms": minimum_remaining_ms,
                    "payload": payload,
                },
            )
        )

    def _commit_signed_expiry_in_transaction(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        payload: Any = None,
    ) -> None:
        """Atomically expire a provably unsent SIGNED action and release its count."""

        self._validate_signed_release_identity(action=action, require_request_id=False)
        self._transition_action_state_in_transaction(
            action=action,
            transition_id=transition_id,
            from_state="signed",
            to_state="expired",
            cause=cause,
            wall_ms=wall_ms,
            mono_ns=mono_ns,
            payload=payload,
        )
        self._adjust_signed_unsent_in_transaction(
            follower_account=str(action["follower_account"]),
            generation=str(action["generation"]),
            signer_epoch=int(action["signer_epoch"]),
            delta=-1,
        )

    def commit_signed_expiries(
        self,
        *,
        expiries: Iterable[Mapping[str, Any]],
    ) -> None:
        """Commit consecutive provably-unsent signed expiries in one transaction."""

        items = tuple(dict(expiry) for expiry in expiries)
        if not items:
            return
        with self.lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                for item in items:
                    self._commit_signed_expiry_in_transaction(**item)
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def commit_signed_expiry(
        self,
        *,
        action: dict[str, Any],
        transition_id: str,
        cause: str,
        wall_ms: int,
        mono_ns: int,
        payload: Any = None,
    ) -> None:
        self.commit_signed_expiries(
            expiries=(
                {
                    "action": action,
                    "transition_id": transition_id,
                    "cause": cause,
                    "wall_ms": wall_ms,
                    "mono_ns": mono_ns,
                    "payload": payload,
                },
            )
        )

    def takeover_signer_epochs(
        self,
        *,
        follower_account: str,
        generation: str,
        expected_signer_epoch: int,
        new_signer_epoch: int,
        expected_transport_epoch: int,
        new_transport_epoch: int,
        expected_rest_epoch: int,
        new_rest_epoch: int,
        proof: dict[str, Any],
    ) -> None:
        required_proofs = (
            "exact_prior_process_exited",
            "external_exit_attestation",
            "signed_unsent_zero",
            "prior_actions_resolved",
        )
        if not all(proof.get(field) for field in required_proofs):
            raise JournalIntegrityError("signer takeover proof is incomplete")
        if (
            new_signer_epoch != expected_signer_epoch + 1
            or new_transport_epoch != expected_transport_epoch + 1
            or new_rest_epoch != expected_rest_epoch + 1
        ):
            raise ValueError("takeover epochs must advance by exactly one")
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    """
                    UPDATE signer_epochs
                    SET signer_epoch = ?, transport_epoch = ?, rest_epoch = ?, updated_ms = ?
                    WHERE lower(follower_account) = lower(?) AND generation = ?
                      AND signer_epoch = ? AND transport_epoch = ? AND rest_epoch = ?
                      AND signed_unsent_count = 0
                      AND NOT EXISTS (
                        SELECT 1 FROM action_states
                        WHERE lower(action_states.follower_account) = lower(signer_epochs.follower_account)
                          AND action_states.generation = signer_epochs.generation
                          AND action_states.state IN (
                            'prepared', 'committed_to_journal', 'signed', 'sent',
                            'accepted', 'resting', 'partially_filled',
                            'unknown_transport_outcome'
                          )
                      )
                    """,
                    (
                        new_signer_epoch,
                        new_transport_epoch,
                        new_rest_epoch,
                        now_ms(),
                        follower_account,
                        generation,
                        expected_signer_epoch,
                        expected_transport_epoch,
                        expected_rest_epoch,
                    ),
                )
                if cur.rowcount != 1:
                    raise JournalIntegrityError("signer/transport/REST takeover CAS failed")

    def unresolved_fast_work(self) -> dict[str, int]:
        active_delta_states = (
            "active_deferred",
            "blocked_missing_basis",
            "blocked_stale_state",
            "blocked_catalog",
            "blocked_rate",
            "blocked_risk",
            "released_pending_action",
        )
        action_states = (
            "prepared",
            "committed_to_journal",
            "signed",
            "sent",
            "accepted",
            "resting",
            "partially_filled",
            "unknown_transport_outcome",
        )
        with self.lock:
            deltas = int(
                self.conn.execute(
                    f"SELECT count(*) FROM deferred_deltas WHERE state IN ({','.join('?' for _ in active_delta_states)})",
                    active_delta_states,
                ).fetchone()[0]
            )
            actions = int(
                self.conn.execute(
                    f"SELECT count(*) FROM action_states WHERE state IN ({','.join('?' for _ in action_states)})",
                    action_states,
                ).fetchone()[0]
            )
            signed_unsent = int(
                self.conn.execute(
                    "SELECT coalesce(sum(signed_unsent_count),0) FROM signer_epochs"
                ).fetchone()[0]
            )
        return {
            "deferred_deltas": deltas,
            "actions": actions,
            "signed_unsent": signed_unsent,
        }

    def fast_runtime_recovery(self, *, generation: str) -> dict[str, Any]:
        """Return a bounded, single-snapshot recovery image for one run generation."""

        if not generation:
            raise ValueError("runtime recovery requires a generation")
        self.conn.execute("BEGIN")
        try:
            partitions = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT * FROM stream_partitions
                    WHERE generation=? ORDER BY partition_key
                    """,
                    (generation,),
                ).fetchall()
            ]
            events: list[dict[str, Any]] = []
            for partition in partitions:
                rows = self.conn.execute(
                    """
                    SELECT * FROM runtime_events
                    WHERE partition_key=? AND ingress_seq>?
                    ORDER BY ingress_seq LIMIT 10001
                    """,
                    (partition["partition_key"], int(partition["applied_cursor"])),
                ).fetchall()
                if len(rows) > 10_000:
                    raise JournalIntegrityError(
                        f"runtime recovery backlog exceeds 10000 for {partition['partition_key']}"
                    )
                events.extend(dict(row) for row in rows)
            deltas = [
                dict(row)
                for row in self.conn.execute(
                    "SELECT * FROM deferred_deltas WHERE generation=? ORDER BY follower_account, canonical_market",
                    (generation,),
                ).fetchall()
            ]
            delta_ids = [str(row["delta_id"]) for row in deltas]
            contributions: list[dict[str, Any]] = []
            if delta_ids:
                placeholders = ",".join("?" for _ in delta_ids)
                contributions = [
                    dict(row)
                    for row in self.conn.execute(
                        f"""
                        SELECT c.* FROM deferred_delta_contributions c
                        JOIN deferred_deltas d ON d.delta_id=c.delta_id
                        WHERE c.delta_id IN ({placeholders})
                        AND COALESCE(json_extract(c.payload_json,'$.basis_epoch'),'')=
                            COALESCE(json_extract(d.payload_json,'$.basis_epoch'),'')
                        ORDER BY c.delta_id, c.exchange_ts_ms, c.contribution_id
                        """,
                        delta_ids,
                    ).fetchall()
                ]
            actions = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT * FROM action_states
                    WHERE generation=? AND state IN (
                      'prepared','committed_to_journal','signed','sent','accepted',
                      'resting','partially_filled','unknown_transport_outcome'
                    ) ORDER BY created_ms, intent_id
                    """,
                    (generation,),
                ).fetchall()
            ]
            rate_reservations = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT intent_id, follower_account, created_ms
                    FROM action_states
                    WHERE generation=? AND created_ms>?
                    ORDER BY created_ms, intent_id
                    """,
                    (generation, now_ms() - 60_000),
                ).fetchall()
            ]
            signer_epochs = [
                dict(row)
                for row in self.conn.execute(
                    "SELECT * FROM signer_epochs WHERE generation=? ORDER BY follower_account",
                    (generation,),
                ).fetchall()
            ]
            source_state_revisions: list[dict[str, Any]] = []
            for partition in partitions:
                row = self.conn.execute(
                    """
                    SELECT * FROM source_state_revisions
                    WHERE partition_key=? ORDER BY revision DESC LIMIT 1
                    """,
                    (partition["partition_key"],),
                ).fetchone()
                if row is not None:
                    source_state_revisions.append(dict(row))
            follower_state_revisions = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT revisions.*
                    FROM follower_state_revisions AS revisions
                    JOIN (
                      SELECT lower(follower_account) AS owner, max(revision) AS revision
                      FROM follower_state_revisions
                      GROUP BY lower(follower_account)
                    ) AS latest
                      ON lower(revisions.follower_account)=latest.owner
                     AND revisions.revision=latest.revision
                    ORDER BY lower(revisions.follower_account)
                    """
                ).fetchall()
            ]
            desired_state_revisions: list[dict[str, Any]] = []
            latest_desired: dict[str, dict[str, Any]] = {}
            for row in self.conn.execute(
                "SELECT * FROM desired_states WHERE mode='live' ORDER BY seq"
            ).fetchall():
                item = dict(row)
                payload = json.loads(str(item["payload_json"]))
                action_account = str(payload.get("action_account") or "").lower()
                if action_account:
                    latest_desired[action_account] = item
            desired_state_revisions.extend(latest_desired[key] for key in sorted(latest_desired))
            catalog_row = self.conn.execute(
                "SELECT payload_json FROM catalog_revisions ORDER BY accepted_ms DESC, rowid DESC LIMIT 1"
            ).fetchone()
            catalog_asset_history = [
                dict(row)
                for row in self.conn.execute(
                    "SELECT asset_id, canonical_market, first_revision_id FROM catalog_asset_history ORDER BY asset_id"
                ).fetchall()
            ]
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return {
            "partitions": partitions,
            "events": events,
            "deferred_deltas": deltas,
            "deferred_contributions": contributions,
            "unresolved_actions": actions,
            "action_rate_reservations": rate_reservations,
            "signer_epochs": signer_epochs,
            "source_state_revisions": source_state_revisions,
            "follower_state_revisions": follower_state_revisions,
            "desired_state_revisions": desired_state_revisions,
            "catalog_revision": (
                None if catalog_row is None else json.loads(str(catalog_row["payload_json"]))
            ),
            "catalog_asset_history": catalog_asset_history,
        }

    def source_reaction_rows(self) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM source_event_reactions ORDER BY created_ms, source_event_key"
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _source_event_from_row(row: sqlite3.Row) -> SourceEvent:
        try:
            payload = json.loads(row["payload_json"])
            event_payload = payload.get("payload") if isinstance(payload, dict) else None
            if not isinstance(payload, dict) or not isinstance(event_payload, dict):
                raise ValueError("source event payload is not an object")
            return SourceEvent(
                idempotency_key=str(payload["idempotency_key"]),
                event_type=SourceEventType(str(payload["event_type"])),
                source_wallet=str(payload["source_wallet"]),
                exchange_ts_ms=int(payload["exchange_ts_ms"]),
                observed_ts_ms=int(payload["observed_ts_ms"]),
                payload=event_payload,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise JournalIntegrityError(
                f"source event seq {row['seq']} cannot be rebuilt: {exc}"
            ) from exc

    def latest_reconcile_snapshot(self, account: str | None = None) -> dict[str, Any] | None:
        where = ""
        params: tuple[Any, ...] = ()
        if account:
            where = "WHERE lower(account) = lower(?)"
            params = (account,)
        with self.lock:
            row = self.conn.execute(
                f"SELECT * FROM reconcile_snapshots {where} ORDER BY seq DESC LIMIT 1",
                params,
            ).fetchone()
        return None if row is None else dict(row)

    def recent_reconcile_snapshots(self, *, account: str, limit: int = 20) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM reconcile_snapshots
                WHERE lower(account) = lower(?)
                ORDER BY seq DESC LIMIT ?
                """,
                (account, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def has_source_event(self, idempotency_key: str) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM source_events WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
        return row is not None

    def latest_source_event_ts(
        self,
        event_types: Iterable[str] | None = None,
        *,
        source_wallet: str | None = None,
    ) -> int:
        clauses: list[str] = []
        params: tuple[Any, ...] = ()
        if event_types is not None:
            values = tuple(event_types)
            if not values:
                return 0
            placeholders = ",".join("?" for _ in values)
            clauses.append(f"event_type IN ({placeholders})")
            params = values
        if source_wallet:
            clauses.extend(
                (
                    "json_valid(payload_json)",
                    "lower(json_extract(payload_json, '$.source_wallet')) = lower(?)",
                )
            )
            params = (*params, source_wallet)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self.lock:
            row = self.conn.execute(
                f"SELECT max(exchange_ts_ms) FROM source_events {where}",
                params,
            ).fetchone()
        return int(row[0] or 0)

    def latest_source_event_ts_by_subtypes(
        self,
        subtypes: Iterable[str],
        *,
        source_wallet: str | None = None,
    ) -> int:
        values = set(subtypes)
        if not values:
            return 0
        with self.lock:
            if source_wallet:
                rows = self.conn.execute(
                    """
                    SELECT exchange_ts_ms, payload_json FROM source_events
                    WHERE event_type = ? AND json_valid(payload_json)
                      AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                    """,
                    ("fill", source_wallet),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT exchange_ts_ms, payload_json FROM source_events WHERE event_type = ?",
                    ("fill",),
                ).fetchall()
        latest = 0
        for row in rows:
            try:
                event = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            payload = event.get("payload") if isinstance(event, dict) else None
            if not isinstance(payload, dict):
                continue
            if payload.get("event_subtype") in values:
                latest = max(latest, int(row["exchange_ts_ms"] or 0))
        return latest

    def latest_safe_mode(self) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM safe_mode_transitions ORDER BY seq DESC LIMIT 1"
            ).fetchone()
        return None if row is None else dict(row)

    def latest_desired_positions(
        self,
        mode: Mode,
        *,
        source_wallet: str | None = None,
        action_account: str | None = None,
        source_network: str | None = None,
        committed_only: bool = False,
    ) -> dict[str, Position] | None:
        with self.lock:
            from_clause = (
                "desired_states JOIN desired_state_commits AS committed "
                "ON committed.state_id = desired_states.state_id"
                if committed_only
                else "desired_states"
            )
            if (
                source_wallet is not None
                or action_account is not None
                or source_network is not None
            ):
                if not source_wallet or not action_account or not source_network:
                    raise ValueError("complete desired-state scope is required")
                row = self.conn.execute(
                    f"""
                    SELECT desired_states.seq, desired_states.payload_json FROM {from_clause}
                    WHERE mode = ?
                      AND json_valid(payload_json)
                      AND lower(json_extract(payload_json, '$.source_wallet')) = lower(?)
                      AND lower(json_extract(payload_json, '$.action_account')) = lower(?)
                      AND json_extract(payload_json, '$.source_network') = ?
                    ORDER BY desired_states.seq DESC LIMIT 1
                    """,
                    (mode.value, source_wallet, action_account, source_network),
                ).fetchone()
            else:
                row = self.conn.execute(
                    f"""
                    SELECT desired_states.seq, desired_states.payload_json FROM {from_clause}
                    WHERE mode = ?
                    ORDER BY desired_states.seq DESC LIMIT 1
                    """,
                    (mode.value,),
                ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError as exc:
            raise JournalIntegrityError(
                f"desired state seq {row['seq']} payload is not valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise JournalIntegrityError(f"desired state seq {row['seq']} payload is not an object")
        if "positions" not in payload or not isinstance(payload["positions"], dict):
            raise JournalIntegrityError(
                f"desired state seq {row['seq']} positions payload is malformed"
            )
        raw_positions = payload["positions"]
        positions: dict[str, Position] = {}
        for coin, item in raw_positions.items():
            if not isinstance(item, dict):
                raise JournalIntegrityError(
                    f"desired state seq {row['seq']} position {coin} is malformed"
                )
            try:
                symbol = canonical_market_symbol(item.get("coin") or coin)
                size = parse_decimal(item.get("size"))
                if size == 0:
                    continue
                entry_px = item.get("entry_px")
                leverage = item.get("leverage")
                updated_ms = item.get("updated_ms") or 0
                positions[symbol] = Position(
                    coin=symbol,
                    size=size,
                    entry_px=parse_decimal(entry_px) if entry_px not in {None, ""} else None,
                    leverage=int(str(leverage)) if leverage not in {None, ""} else None,
                    updated_ms=int(str(updated_ms)),
                )
            except Exception as exc:
                raise JournalIntegrityError(
                    f"desired state seq {row['seq']} position {coin} cannot be rebuilt: {exc}"
                ) from exc
        return positions

    def rebuild_runtime_state(
        self,
        mode: Mode | None = None,
        *,
        source_wallet: str | None = None,
    ) -> dict[str, Any]:
        return {
            "source_event_count": self.count_source_events(source_wallet),
            "pending_source_reaction_count": self.unfinished_source_reaction_count(
                source_wallet=source_wallet
            ),
            "desired_state_count": self.desired_state_count(mode),
            "pending_intents": self.pending_intents(mode),
            "latest_safe_mode": self.latest_safe_mode(),
            "latest_source_events": self.recent_source_events(
                source_wallet=source_wallet,
                limit=10,
            ),
        }

    def desired_state_count(self, mode: Mode | None = None) -> int:
        if mode is None:
            return self.count("desired_states")
        with self.lock:
            row = self.conn.execute(
                "SELECT count(*) FROM desired_states WHERE mode = ?",
                (mode.value,),
            ).fetchone()
        return int(row[0])

    def recent_intents(self, mode: Mode | None = None, limit: int = 50) -> list[dict[str, Any]]:
        if mode is None:
            return self.recent("follower_intents", limit)
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM follower_intents
                WHERE mode = ?
                ORDER BY seq DESC LIMIT ?
                """,
                (mode.value, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def execution_plan_intents(self, state_id: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM follower_intents
                WHERE desired_state_id = ?
                ORDER BY seq
                """,
                (state_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def intent_by_cloid(self, cloid: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM follower_intents WHERE cloid = ? ORDER BY seq DESC LIMIT 1",
                (cloid,),
            ).fetchone()
        return None if row is None else dict(row)

    def execution_reports_for_cloid(self, cloid: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM execution_reports WHERE cloid = ? ORDER BY seq DESC",
                (cloid,),
            ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def execution_report_is_proven_hip3_ioc_zero_fill(row: dict[str, Any]) -> bool:
        return _strict_persisted_hip3_ioc_zero_fill(row) is not None

    def latest_hip3_ioc_zero_fill_proof(self, base_cloid: str) -> str | None:
        """Return the latest strict signed zero-fill proof for a planner-base CLOID."""

        try:
            canonical_base_cloid = validate_cloid(base_cloid)
        except ValueError:
            return None
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT status, exchange_status, cloid, payload_json
                FROM execution_reports
                WHERE status = 'rejected'
                  AND json_valid(payload_json)
                  AND lower(
                    json_extract(
                      payload_json,
                      '$.payload.post_send_retry_identity.base_cloid'
                    )
                  ) = lower(?)
                ORDER BY seq DESC
                """,
                (canonical_base_cloid,),
            )
            for stored_row in rows:
                validated = _strict_persisted_hip3_ioc_zero_fill(dict(stored_row))
                if validated is not None and validated["base_cloid"] == canonical_base_cloid:
                    return str(validated["proof_id"])
        return None

    def has_execution_report_for_cloid(self, cloid: str) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM execution_reports WHERE cloid = ? LIMIT 1",
                (cloid,),
            ).fetchone()
        return row is not None

    def has_dispatch_evidence_for_cloid(self, cloid: str) -> bool:
        """Return false only for a currently rearmed, durably proven no-send attempt."""

        with self.lock:
            intent = self.conn.execute(
                "SELECT action, attempt_phase FROM follower_intents WHERE cloid = ?",
                (cloid,),
            ).fetchone()
            reports = self.conn.execute(
                "SELECT status, exchange_status FROM execution_reports WHERE cloid = ? ORDER BY seq",
                (cloid,),
            ).fetchall()
        if not reports:
            return False
        if intent is None or str(intent["attempt_phase"]) != ExecutionAttemptPhase.PREPARED.value:
            return True
        action = str(intent["action"])
        return any(
            not _report_is_proven_no_send(
                status=str(report["status"]),
                exchange_status=str(report["exchange_status"]),
                action=action,
            )
            for report in reports
        )

    def recent_counted_exchange_action_stats(self, since_ms: int) -> dict[str, int]:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT count(*) AS count, min(created_ms) AS oldest_ms
                FROM execution_reports
                WHERE created_ms >= ?
                  AND exchange_status NOT LIKE 'blocked:%'
                  AND exchange_status NOT LIKE 'settled:%'
                  AND exchange_status NOT LIKE 'watchdog_settled:%'
                  AND exchange_status NOT LIKE 'paper_%'
                  AND exchange_status != 'skipped'
                  AND intent_id NOT LIKE 'auth-probe:%'
                  AND intent_id NOT LIKE 'cancel:%'
                  AND intent_id NOT LIKE 'dead-man-%'
                """,
                (since_ms,),
            ).fetchone()
        return {
            "count": int(row["count"] or 0),
            "oldest_ms": int(row["oldest_ms"] or 0),
        }

    def latest_successful_auth_probe(
        self,
        *,
        mode: Mode | None = None,
        account: str = "",
        intent_id: str = "",
        since_ms: int,
    ) -> dict[str, Any] | None:
        if not intent_id:
            if mode is None or not account:
                raise ValueError("mode/account or an explicit auth-probe intent_id is required")
            intent_id = f"auth-probe:{mode.value}:{account.lower()}"
        with self.lock:
            row = self.conn.execute(
                """
                SELECT *
                FROM execution_reports
                WHERE intent_id = ?
                  AND status = 'acked'
                  AND exchange_status = 'auth_probe_ok'
                  AND created_ms >= ?
                ORDER BY seq DESC
                LIMIT 1
                """,
                (intent_id, since_ms),
            ).fetchone()
        return None if row is None else dict(row)

    def consecutive_exchange_failure_stats(self, limit: int = 100) -> dict[str, int]:
        failures = 0
        latest_failure_ms = 0
        examined = 0
        seen_terminal_dispositions: set[tuple[str, str]] = set()
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT intent_id, cloid, status, exchange_status, payload_json, created_ms
                FROM execution_reports
                WHERE exchange_status NOT LIKE 'blocked:%'
                  AND exchange_status NOT LIKE 'paper_%'
                  AND exchange_status != 'skipped'
                  AND intent_id NOT LIKE 'auth-probe:%'
                  AND NOT (
                    intent_id LIKE 'dead-man-%:testnet:%'
                    AND (
                      coalesce(
                        json_extract(
                          payload_json,
                          '$.payload.testnet_dead_man_volume_rejection'
                        ),
                        0
                      ) = 1
                      OR lower(payload_json) LIKE
                        '%cannot set scheduled cancel time until enough volume traded%'
                    )
                  )
                  AND NOT (
                    intent_id LIKE 'leverage:%'
                    AND (
                      coalesce(
                        json_extract(payload_json, '$.payload.expected_cross_margin_fallback'),
                        0
                      ) = 1
                      OR lower(payload_json) LIKE
                        '%cross margin is not allowed for this asset%'
                    )
                  )
                ORDER BY seq DESC
                """,
            )
            for row in rows:
                stored_row = dict(row)
                status = str(row["status"])
                intent_id = str(row["intent_id"] or "")
                cloid = str(row["cloid"] or "").lower()
                disposition = (cloid, status)
                if (
                    cloid
                    and status in TERMINAL_REPORT_STATUSES
                    and disposition in seen_terminal_dispositions
                ):
                    continue
                if cloid and status in TERMINAL_REPORT_STATUSES:
                    seen_terminal_dispositions.add(disposition)
                if _strict_persisted_hip3_ioc_zero_fill(stored_row) is not None:
                    continue
                if status == "rejected":
                    failures += 1
                    examined += 1
                    if latest_failure_ms == 0:
                        latest_failure_ms = int(row["created_ms"] or 0)
                    if examined >= max(1, limit):
                        break
                    continue
                if intent_id.startswith("dead-man-"):
                    continue
                if status in {"filled", "acked", "canceled"}:
                    break
                break
        return {"consecutive_failures": failures, "latest_failure_ms": latest_failure_ms}

    def pending_intents(self, mode: Mode | None = None) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in UNRESOLVED_ATTEMPT_PHASES)
        mode_filter = "AND mode = ?" if mode is not None else ""
        params: list[Any] = [*UNRESOLVED_ATTEMPT_PHASES]
        if mode is not None:
            params.append(mode.value)
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT * FROM follower_intents
                WHERE attempt_phase IN ({placeholders})
                  {mode_filter}
                ORDER BY seq
                """,
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_intent_count(self, mode: Mode | None = None) -> int:
        placeholders = ",".join("?" for _ in UNRESOLVED_ATTEMPT_PHASES)
        mode_filter = "AND mode = ?" if mode is not None else ""
        params: list[Any] = [*UNRESOLVED_ATTEMPT_PHASES]
        if mode is not None:
            params.append(mode.value)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT count(*) FROM follower_intents
                WHERE attempt_phase IN ({placeholders})
                  {mode_filter}
                """,
                tuple(params),
            ).fetchone()
        return int(row[0])

    def sensitive_value_findings(self) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        for table in (
            "source_events",
            "desired_states",
            "follower_intents",
            "execution_reports",
            "signed_action_attempts",
            "reconcile_snapshots",
            "safe_mode_transitions",
            "config_revisions",
            "control_audit",
        ):
            with self.lock:
                rows = self.conn.execute(
                    f"SELECT seq, payload_json FROM {table} WHERE payload_json IS NOT NULL"
                ).fetchall()
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except json.JSONDecodeError:
                    findings.append({"table": table, "seq": row["seq"], "path": "payload_json"})
                    continue
                findings.extend(
                    {
                        "table": table,
                        "seq": row["seq"],
                        "path": path,
                    }
                    for path in _find_unredacted_sensitive_values(payload)
                )
        with self.lock:
            reaction_rows = self.conn.execute(
                "SELECT source_event_key, outcome_json FROM source_event_reactions"
            ).fetchall()
        for row in reaction_rows:
            try:
                payload = json.loads(row["outcome_json"])
            except json.JSONDecodeError:
                findings.append(
                    {
                        "table": "source_event_reactions",
                        "seq": row["source_event_key"],
                        "path": "outcome_json",
                    }
                )
                continue
            findings.extend(
                {
                    "table": "source_event_reactions",
                    "seq": row["source_event_key"],
                    "path": path,
                }
                for path in _find_unredacted_sensitive_values(payload)
            )
        with self.lock:
            signed_result_rows = self.conn.execute(
                "SELECT seq, result_json FROM signed_action_attempts"
            ).fetchall()
        for row in signed_result_rows:
            try:
                payload = json.loads(row["result_json"])
            except json.JSONDecodeError:
                findings.append(
                    {
                        "table": "signed_action_attempts",
                        "seq": row["seq"],
                        "path": "result_json",
                    }
                )
                continue
            findings.extend(
                {
                    "table": "signed_action_attempts",
                    "seq": row["seq"],
                    "path": path,
                }
                for path in _find_unredacted_sensitive_values(payload)
            )
        return findings

    def find_text_occurrences(self, values: Iterable[str]) -> list[dict[str, Any]]:
        needles = [value for value in values if value]
        if not needles:
            return []
        findings: list[dict[str, Any]] = []
        searchable_columns = {
            "source_events": ("payload_json",),
            "desired_states": ("payload_json",),
            "follower_intents": ("payload_json", "cloid", "source_event_key"),
            "execution_reports": ("payload_json", "cloid", "exchange_status"),
            "signed_action_attempts": (
                "payload_json",
                "result_json",
                "cloid",
                "action",
                "account",
            ),
            "reconcile_snapshots": ("payload_json", "account"),
            "safe_mode_transitions": ("payload_json", "detail"),
            "config_revisions": ("payload_json", "revision_id"),
            "control_audit": ("payload_json", "detail", "control", "status"),
        }
        for table, columns in searchable_columns.items():
            with self.lock:
                rows = self.conn.execute(f"SELECT * FROM {table}").fetchall()
            for row in rows:
                row_dict = dict(row)
                for column in columns:
                    haystack = str(row_dict.get(column) or "")
                    if any(needle in haystack for needle in needles):
                        findings.append({"table": table, "seq": row_dict["seq"], "column": column})
        with self.lock:
            reaction_rows = self.conn.execute(
                "SELECT source_event_key, outcome_json FROM source_event_reactions"
            ).fetchall()
        for row in reaction_rows:
            haystack = str(row["outcome_json"] or "")
            if any(needle in haystack for needle in needles):
                findings.append(
                    {
                        "table": "source_event_reactions",
                        "seq": row["source_event_key"],
                        "column": "outcome_json",
                    }
                )
        return findings

    def acquire_runtime_lease(
        self,
        *,
        name: str,
        owner: str,
        ttl_ms: int,
        observed_ms: int | None = None,
    ) -> bool:
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be positive")
        observed = observed_ms or now_ms()
        expires = observed + ttl_ms
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    """
                    INSERT INTO runtime_leases(name, owner, expires_ms, updated_ms)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(name) DO UPDATE SET
                      owner = excluded.owner,
                      expires_ms = excluded.expires_ms,
                      updated_ms = excluded.updated_ms
                    WHERE runtime_leases.owner = excluded.owner
                       OR runtime_leases.expires_ms <= ?
                    """,
                    (name, owner, expires, observed, observed),
                )
        return cur.rowcount > 0

    def release_runtime_lease(self, *, name: str, owner: str) -> bool:
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    "DELETE FROM runtime_leases WHERE name = ? AND owner = ?",
                    (name, owner),
                )
        return cur.rowcount > 0

    def runtime_lease(self, name: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM runtime_leases WHERE name = ?",
                (name,),
            ).fetchone()
        return None if row is None else dict(row)

    def upsert_runner_heartbeat(
        self,
        *,
        instance_id: str,
        role: str,
        mode: str,
        source_wallet: str,
        action_account: str,
        config_revision_id: str,
        status: str,
        detail: str,
        started_ms: int,
        heartbeat_ms: int,
        expires_ms: int,
        cycle_count: int,
        last_cycle_ms: int | None,
    ) -> None:
        with self.lock:
            with self.conn:
                self.conn.execute(
                    "DELETE FROM runner_heartbeats WHERE expires_ms < ?",
                    (heartbeat_ms - 7 * 86_400_000,),
                )
                self.conn.execute(
                    """
                    INSERT INTO runner_heartbeats(
                      instance_id, role, mode, source_wallet, action_account,
                      config_revision_id, status, detail, started_ms, heartbeat_ms,
                      expires_ms, cycle_count, last_cycle_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(instance_id) DO UPDATE SET
                      role = excluded.role,
                      mode = excluded.mode,
                      source_wallet = excluded.source_wallet,
                      action_account = excluded.action_account,
                      config_revision_id = excluded.config_revision_id,
                      status = excluded.status,
                      detail = excluded.detail,
                      heartbeat_ms = excluded.heartbeat_ms,
                      expires_ms = excluded.expires_ms,
                      cycle_count = excluded.cycle_count,
                      last_cycle_ms = excluded.last_cycle_ms
                    """,
                    (
                        instance_id,
                        role,
                        mode,
                        source_wallet,
                        action_account,
                        config_revision_id,
                        status,
                        detail,
                        started_ms,
                        heartbeat_ms,
                        expires_ms,
                        cycle_count,
                        last_cycle_ms,
                    ),
                )

    def latest_runner_heartbeat(
        self,
        *,
        mode: str,
        source_wallet: str,
        action_account: str,
        role: str | None = None,
        exclude_role: str | None = None,
    ) -> dict[str, Any] | None:
        if role is not None and exclude_role is not None:
            raise ValueError("runner heartbeat role and exclude_role are mutually exclusive")
        role_clause = "" if role is None else "AND role = ?"
        if exclude_role is not None:
            role_clause = "AND role != ?"
        params: tuple[str, ...] = (mode, source_wallet, action_account)
        if role is not None:
            params = (*params, role)
        elif exclude_role is not None:
            params = (*params, exclude_role)
        with self.lock:
            row = self.conn.execute(
                f"""
                SELECT * FROM runner_heartbeats
                WHERE mode = ?
                  AND lower(source_wallet) = lower(?)
                  AND lower(action_account) = lower(?)
                  {role_clause}
                ORDER BY heartbeat_ms DESC LIMIT 1
                """,
                params,
            ).fetchone()
        return None if row is None else dict(row)


def _find_unredacted_sensitive_values(value: Any, path: str = "$") -> list[str]:
    findings: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            child_path = f"{path}.{key}"
            if is_sensitive_key(str(key)):
                if item not in {"", None, "<redacted>"}:
                    findings.append(child_path)
            else:
                findings.extend(_find_unredacted_sensitive_values(item, child_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            findings.extend(_find_unredacted_sensitive_values(item, f"{path}[{idx}]"))
    return findings
