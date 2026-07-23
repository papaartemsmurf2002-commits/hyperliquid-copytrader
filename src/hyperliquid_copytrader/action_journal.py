from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from threading import RLock
from time import time_ns
from typing import Iterator

from .cloid import validate_cloid


SCHEMA_VERSION = 1


class ActionState(str, Enum):
    PREPARED = "PREPARED"
    SEND_ATTEMPTED = "SEND_ATTEMPTED"
    NOT_SENT = "NOT_SENT"
    UNKNOWN = "UNKNOWN"
    REJECTED = "REJECTED"
    RESTING = "RESTING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"


TERMINAL_STATES = frozenset(
    {
        ActionState.NOT_SENT,
        ActionState.REJECTED,
        ActionState.PARTIALLY_FILLED,
        ActionState.FILLED,
        ActionState.CANCELED,
    }
)
RECOVERY_STATES = frozenset(
    {
        ActionState.PREPARED,
        ActionState.SEND_ATTEMPTED,
        ActionState.UNKNOWN,
        ActionState.RESTING,
    }
)
AMBIGUOUS_STATES = frozenset({ActionState.SEND_ATTEMPTED, ActionState.UNKNOWN})


@dataclass(frozen=True, slots=True)
class ActionRecord:
    cloid: str
    follower_account: str
    api_wallet: str
    desired_id: str
    market: str
    attempt_no: int
    nonce: int
    requested_size: Decimal
    cumulative_filled_size: Decimal
    action_json: str
    signed_payload_json: str
    expires_after_ms: int
    request_id: str
    state: ActionState
    outcome_detail: str | None
    created_ms: int
    updated_ms: int
    send_attempted_ms: int | None
    terminal_ms: int | None

    @property
    def terminal(self) -> bool:
        # continuous-v1 submits IOC orders only.  A partial IOC fill is final:
        # its unfilled remainder was canceled by the venue.
        return self.state in TERMINAL_STATES

    @property
    def recovery_required(self) -> bool:
        return self.state in RECOVERY_STATES

    @property
    def ambiguous(self) -> bool:
        return self.state in AMBIGUOUS_STATES

    @property
    def provably_not_sent(self) -> bool:
        return self.state in {ActionState.PREPARED, ActionState.NOT_SENT}

    @property
    def remaining_size(self) -> Decimal:
        return self.requested_size - self.cumulative_filled_size


class ActionJournal:
    """Small durable nonce and IOC action journal for continuous-v1.

    The caller owns one execution lock per follower/API-wallet lane.  SQLite's
    ``BEGIN IMMEDIATE`` additionally serializes nonce allocation across
    processes.  ``mark_send_attempted`` must be committed immediately before
    writing the already-journaled ``signed_payload_json`` to the socket.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._conn = sqlite3.connect(
            self.path,
            timeout=10,
            isolation_level=None,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> ActionJournal:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def reserve_nonce(
        self,
        *,
        follower_account: str,
        api_wallet: str,
        wall_ms: int | None = None,
    ) -> int:
        """Durably reserve the next nonce in one signer lane.

        Clock rollback cannot reuse a nonce.  A crash after this call may leave
        a harmless gap, but a reserved nonce is never handed out again.
        """

        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        candidate = _positive_int(_now_ms() if wall_ms is None else wall_ms, "wall_ms")
        with self._transaction():
            row = self._conn.execute(
                """
                SELECT last_nonce
                FROM signer_nonce
                WHERE follower_account=? AND api_wallet=?
                """,
                (follower, wallet),
            ).fetchone()
            nonce = max(candidate, (int(row["last_nonce"]) + 1) if row else candidate)
            self._conn.execute(
                """
                INSERT INTO signer_nonce(follower_account, api_wallet, last_nonce, updated_ms)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(follower_account, api_wallet) DO UPDATE SET
                  last_nonce=excluded.last_nonce,
                  updated_ms=excluded.updated_ms
                """,
                (follower, wallet, nonce, _now_ms()),
            )
        return nonce

    def last_nonce(self, *, follower_account: str, api_wallet: str) -> int | None:
        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT last_nonce
                FROM signer_nonce
                WHERE follower_account=? AND api_wallet=?
                """,
                (follower, wallet),
            ).fetchone()
        return None if row is None else int(row["last_nonce"])

    def next_attempt_no(
        self,
        *,
        follower_account: str,
        api_wallet: str,
        desired_id: str,
        market: str,
    ) -> int:
        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        desired = _nonempty(desired_id, "desired_id")
        canonical_market = _nonempty(market, "market")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COALESCE(MAX(attempt_no), 0) AS last_attempt
                FROM actions
                WHERE follower_account=? AND api_wallet=?
                  AND desired_id=? AND market=?
                """,
                (follower, wallet, desired, canonical_market),
            ).fetchone()
        return int(row["last_attempt"]) + 1

    def prepare_action(
        self,
        *,
        follower_account: str,
        api_wallet: str,
        desired_id: str,
        market: str,
        attempt_no: int,
        cloid: str,
        nonce: int,
        requested_size: Decimal | str | int,
        action_json: str,
        signed_payload_json: str,
        expires_after_ms: int,
        request_id: str,
        created_ms: int | None = None,
    ) -> ActionRecord:
        """Persist the exact signed wire payload before any send is attempted."""

        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        desired = _nonempty(desired_id, "desired_id")
        canonical_market = _nonempty(market, "market")
        normalized_cloid = validate_cloid(cloid)
        attempt = _positive_int(attempt_no, "attempt_no")
        reserved_nonce = _positive_int(nonce, "nonce")
        size = _positive_decimal(requested_size, "requested_size")
        exact_action = _json_object(action_json, "action_json")
        exact_payload = _json_object(signed_payload_json, "signed_payload_json")
        request = _nonempty(request_id, "request_id")
        created = _positive_int(_now_ms() if created_ms is None else created_ms, "created_ms")
        expires = _positive_int(expires_after_ms, "expires_after_ms")
        if expires <= created:
            raise ValueError("expires_after_ms must be later than created_ms")

        with self._transaction():
            nonce_row = self._conn.execute(
                """
                SELECT last_nonce
                FROM signer_nonce
                WHERE follower_account=? AND api_wallet=?
                """,
                (follower, wallet),
            ).fetchone()
            if nonce_row is None or reserved_nonce != int(nonce_row["last_nonce"]):
                raise ValueError("nonce is not the latest durable reservation for this signer lane")
            attempt_row = self._conn.execute(
                """
                SELECT COALESCE(MAX(attempt_no), 0) AS last_attempt
                FROM actions
                WHERE follower_account=? AND api_wallet=?
                  AND desired_id=? AND market=?
                """,
                (follower, wallet, desired, canonical_market),
            ).fetchone()
            expected_attempt = int(attempt_row["last_attempt"]) + 1
            if attempt != expected_attempt:
                raise ValueError(
                    f"attempt_no must be the next durable attempt ({expected_attempt}), got {attempt}"
                )
            try:
                self._conn.execute(
                    """
                    INSERT INTO actions(
                      cloid, follower_account, api_wallet, desired_id, market,
                      attempt_no, nonce, requested_size, cumulative_filled_size,
                      action_json, signed_payload_json, expires_after_ms, request_id,
                      state, outcome_detail, created_ms, updated_ms,
                      send_attempted_ms, terminal_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '0', ?, ?, ?, ?, ?, NULL, ?, ?, NULL, NULL)
                    """,
                    (
                        normalized_cloid,
                        follower,
                        wallet,
                        desired,
                        canonical_market,
                        attempt,
                        reserved_nonce,
                        _decimal_wire(size),
                        exact_action,
                        exact_payload,
                        expires,
                        request,
                        ActionState.PREPARED.value,
                        created,
                        created,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError(
                    "CLOID, signer nonce, or durable attempt is already journaled"
                ) from exc
            return self._require_action_locked(normalized_cloid)

    def get_action(self, cloid: str) -> ActionRecord | None:
        normalized_cloid = validate_cloid(cloid)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM actions WHERE cloid=?",
                (normalized_cloid,),
            ).fetchone()
        return None if row is None else _record(row)

    def get_owned_action(
        self,
        cloid: str,
        *,
        follower_account: str,
        api_wallet: str,
    ) -> ActionRecord | None:
        """Resolve ownership by exact CLOID and exact signer lane only."""

        normalized_cloid = validate_cloid(cloid)
        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM actions
                WHERE cloid=? AND follower_account=? AND api_wallet=?
                """,
                (normalized_cloid, follower, wallet),
            ).fetchone()
        return None if row is None else _record(row)

    def owns_cloid(
        self,
        cloid: str,
        *,
        follower_account: str,
        api_wallet: str,
    ) -> bool:
        return (
            self.get_owned_action(
                cloid,
                follower_account=follower_account,
                api_wallet=api_wallet,
            )
            is not None
        )

    def mark_send_attempted(
        self,
        cloid: str,
        *,
        request_id: str | int | None = None,
        observed_ms: int | None = None,
    ) -> ActionRecord:
        """Commit the ambiguity boundary immediately before ``socket.send``.

        Calling this twice raises: a caller must reconcile the first attempt,
        never use a second call as permission to resend the payload.
        """

        normalized_cloid = validate_cloid(cloid)
        observed = _positive_int(
            _now_ms() if observed_ms is None else observed_ms,
            "observed_ms",
        )
        correlated_request_id = (
            None if request_id is None else _nonempty(str(request_id), "request_id")
        )
        with self._transaction():
            current = self._require_action_locked(normalized_cloid)
            if current.state is not ActionState.PREPARED:
                raise ValueError(
                    f"send may be attempted only from PREPARED, got {current.state.value}"
                )
            self._conn.execute(
                """
                UPDATE actions
                SET state=?, request_id=COALESCE(?, request_id),
                    updated_ms=?, send_attempted_ms=?
                WHERE cloid=?
                """,
                (
                    ActionState.SEND_ATTEMPTED.value,
                    correlated_request_id,
                    observed,
                    observed,
                    normalized_cloid,
                ),
            )
            return self._require_action_locked(normalized_cloid)

    def mark_not_sent(self, cloid: str, *, observed_ms: int | None = None) -> ActionRecord:
        """Terminalize a PREPARED row whose socket send provably never began."""

        normalized_cloid = validate_cloid(cloid)
        observed = _positive_int(
            _now_ms() if observed_ms is None else observed_ms,
            "observed_ms",
        )
        with self._transaction():
            current = self._require_action_locked(normalized_cloid)
            if current.state is ActionState.NOT_SENT:
                return current
            if current.state is not ActionState.PREPARED:
                raise ValueError(f"NOT_SENT is valid only from PREPARED, got {current.state.value}")
            self._conn.execute(
                """
                UPDATE actions
                SET state=?, updated_ms=?, terminal_ms=?
                WHERE cloid=?
                """,
                (ActionState.NOT_SENT.value, observed, observed, normalized_cloid),
            )
            return self._require_action_locked(normalized_cloid)

    def mark_unknown(
        self,
        cloid: str,
        *,
        detail: str | None = None,
        observed_ms: int | None = None,
    ) -> ActionRecord:
        return self.record_outcome(
            cloid,
            state=ActionState.UNKNOWN,
            detail=detail,
            observed_ms=observed_ms,
        )

    def record_outcome(
        self,
        cloid: str,
        *,
        state: ActionState | str,
        cumulative_filled_size: Decimal | str | int | None = None,
        detail: str | None = None,
        observed_ms: int | None = None,
    ) -> ActionRecord:
        """Record a classified venue outcome for an attempted action.

        ``PARTIALLY_FILLED`` is terminal because this journal is IOC-only.
        Fill quantities are cumulative, so replaying the same observation is a
        no-op and a decreasing observation is rejected.
        """

        normalized_cloid = validate_cloid(cloid)
        target = state if isinstance(state, ActionState) else ActionState(state)
        if target not in {
            ActionState.UNKNOWN,
            ActionState.REJECTED,
            ActionState.RESTING,
            ActionState.PARTIALLY_FILLED,
            ActionState.FILLED,
            ActionState.CANCELED,
        }:
            raise ValueError(f"{target.value} is not a venue outcome")
        observed = _positive_int(
            _now_ms() if observed_ms is None else observed_ms,
            "observed_ms",
        )
        normalized_detail = None if detail is None else detail.strip() or None
        with self._transaction():
            current = self._require_action_locked(normalized_cloid)
            cumulative = (
                current.cumulative_filled_size
                if cumulative_filled_size is None
                else _nonnegative_decimal(cumulative_filled_size, "cumulative_filled_size")
            )
            self._validate_cumulative(current, cumulative)
            self._validate_outcome(current, target, cumulative)
            if (
                target is current.state
                and cumulative == current.cumulative_filled_size
                and normalized_detail == current.outcome_detail
            ):
                return current
            terminal_ms = current.terminal_ms
            if target in TERMINAL_STATES and terminal_ms is None:
                terminal_ms = observed
            self._conn.execute(
                """
                UPDATE actions
                SET state=?, cumulative_filled_size=?, outcome_detail=?,
                    updated_ms=?, terminal_ms=?
                WHERE cloid=?
                """,
                (
                    target.value,
                    _decimal_wire(cumulative),
                    normalized_detail,
                    observed,
                    terminal_ms,
                    normalized_cloid,
                ),
            )
            return self._require_action_locked(normalized_cloid)

    def record_cumulative_fill(
        self,
        cloid: str,
        cumulative_filled_size: Decimal | str | int,
        *,
        observed_ms: int | None = None,
    ) -> ActionRecord:
        """Apply one idempotent cumulative user-fill observation by CLOID.

        A full fill is definitive immediately.  A partial fill keeps an
        attempted/unknown/resting action unresolved until the IOC response is
        classified.  If a late fill contradicts REJECTED/CANCELED, fill truth
        wins and the action becomes terminal PARTIALLY_FILLED.
        """

        normalized_cloid = validate_cloid(cloid)
        cumulative = _nonnegative_decimal(cumulative_filled_size, "cumulative_filled_size")
        observed = _positive_int(
            _now_ms() if observed_ms is None else observed_ms,
            "observed_ms",
        )
        with self._transaction():
            current = self._require_action_locked(normalized_cloid)
            self._validate_cumulative(current, cumulative)
            if cumulative == current.cumulative_filled_size:
                return current
            if current.send_attempted_ms is None:
                raise ValueError("a provably unsent action cannot have a fill")

            target = current.state
            terminal_ms = current.terminal_ms
            if cumulative == current.requested_size:
                target = ActionState.FILLED
                terminal_ms = terminal_ms or observed
            elif current.state in {
                ActionState.REJECTED,
                ActionState.CANCELED,
                ActionState.PARTIALLY_FILLED,
            }:
                target = ActionState.PARTIALLY_FILLED
                terminal_ms = terminal_ms or observed

            self._conn.execute(
                """
                UPDATE actions
                SET state=?, cumulative_filled_size=?, updated_ms=?, terminal_ms=?
                WHERE cloid=?
                """,
                (
                    target.value,
                    _decimal_wire(cumulative),
                    observed,
                    terminal_ms,
                    normalized_cloid,
                ),
            )
            return self._require_action_locked(normalized_cloid)

    def recovery_actions(
        self,
        *,
        follower_account: str | None = None,
        api_wallet: str | None = None,
    ) -> tuple[ActionRecord, ...]:
        """Return provably-unsent, ambiguous, and still-resting actions."""

        if (follower_account is None) != (api_wallet is None):
            raise ValueError("follower_account and api_wallet must be filtered together")
        params: list[object] = [state.value for state in sorted(RECOVERY_STATES, key=str)]
        placeholders = ",".join("?" for _ in params)
        query = f"SELECT * FROM actions WHERE state IN ({placeholders})"  # noqa: S608
        if follower_account is not None and api_wallet is not None:
            query += " AND follower_account=? AND api_wallet=?"
            params.extend(
                [
                    _identity(follower_account, "follower_account"),
                    _identity(api_wallet, "api_wallet"),
                ]
            )
        query += " ORDER BY created_ms, cloid"
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return tuple(_record(row) for row in rows)

    def recent_send_attempts(
        self,
        *,
        follower_account: str,
        api_wallet: str,
        after_ms: int,
    ) -> tuple[int, ...]:
        """Return exact durable send-boundary times after one rolling-window cutoff."""

        follower = _identity(follower_account, "follower_account")
        wallet = _identity(api_wallet, "api_wallet")
        if isinstance(after_ms, bool) or not isinstance(after_ms, int) or after_ms < 0:
            raise ValueError("after_ms must be a non-negative integer")
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT send_attempted_ms
                FROM actions
                WHERE follower_account=? AND api_wallet=?
                  AND send_attempted_ms IS NOT NULL AND send_attempted_ms>?
                ORDER BY send_attempted_ms, cloid
                """,
                (follower, wallet, after_ms),
            ).fetchall()
        return tuple(int(row["send_attempted_ms"]) for row in rows)

    def _initialize_schema(self) -> None:
        with self._lock:
            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if version not in {0, SCHEMA_VERSION}:
                raise RuntimeError(
                    f"unsupported action journal schema {version}; expected {SCHEMA_VERSION}"
                )
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS signer_nonce(
                  follower_account TEXT NOT NULL,
                  api_wallet TEXT NOT NULL,
                  last_nonce INTEGER NOT NULL CHECK(last_nonce > 0),
                  updated_ms INTEGER NOT NULL CHECK(updated_ms > 0),
                  PRIMARY KEY(follower_account, api_wallet)
                );

                CREATE TABLE IF NOT EXISTS actions(
                  cloid TEXT PRIMARY KEY,
                  follower_account TEXT NOT NULL,
                  api_wallet TEXT NOT NULL,
                  desired_id TEXT NOT NULL,
                  market TEXT NOT NULL,
                  attempt_no INTEGER NOT NULL CHECK(attempt_no > 0),
                  nonce INTEGER NOT NULL CHECK(nonce > 0),
                  requested_size TEXT NOT NULL,
                  cumulative_filled_size TEXT NOT NULL DEFAULT '0',
                  action_json TEXT NOT NULL,
                  signed_payload_json TEXT NOT NULL,
                  expires_after_ms INTEGER NOT NULL CHECK(expires_after_ms > 0),
                  request_id TEXT NOT NULL,
                  state TEXT NOT NULL CHECK(state IN (
                    'PREPARED', 'SEND_ATTEMPTED', 'NOT_SENT', 'UNKNOWN',
                    'REJECTED', 'RESTING', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED'
                  )),
                  outcome_detail TEXT,
                  created_ms INTEGER NOT NULL CHECK(created_ms > 0),
                  updated_ms INTEGER NOT NULL CHECK(updated_ms > 0),
                  send_attempted_ms INTEGER,
                  terminal_ms INTEGER,
                  FOREIGN KEY(follower_account, api_wallet)
                    REFERENCES signer_nonce(follower_account, api_wallet),
                  UNIQUE(follower_account, api_wallet, nonce),
                  UNIQUE(follower_account, api_wallet, desired_id, market, attempt_no)
                );

                CREATE INDEX IF NOT EXISTS idx_actions_lane_recovery
                  ON actions(follower_account, api_wallet, state, created_ms);
                CREATE INDEX IF NOT EXISTS idx_actions_desired_attempt
                  ON actions(follower_account, api_wallet, desired_id, market, attempt_no DESC);
                """
            )
            self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def _require_action_locked(self, cloid: str) -> ActionRecord:
        row = self._conn.execute("SELECT * FROM actions WHERE cloid=?", (cloid,)).fetchone()
        if row is None:
            raise KeyError(f"unknown action CLOID: {cloid}")
        return _record(row)

    @staticmethod
    def _validate_cumulative(current: ActionRecord, cumulative: Decimal) -> None:
        if cumulative < current.cumulative_filled_size:
            raise ValueError("cumulative fill cannot decrease")
        if cumulative > current.requested_size:
            raise ValueError("cumulative fill cannot exceed requested size")

    @staticmethod
    def _validate_outcome(
        current: ActionRecord,
        target: ActionState,
        cumulative: Decimal,
    ) -> None:
        allowed = {
            ActionState.SEND_ATTEMPTED: {
                ActionState.UNKNOWN,
                ActionState.REJECTED,
                ActionState.RESTING,
                ActionState.PARTIALLY_FILLED,
                ActionState.FILLED,
                ActionState.CANCELED,
            },
            ActionState.UNKNOWN: {
                ActionState.UNKNOWN,
                ActionState.REJECTED,
                ActionState.RESTING,
                ActionState.PARTIALLY_FILLED,
                ActionState.FILLED,
                ActionState.CANCELED,
            },
            ActionState.RESTING: {
                ActionState.UNKNOWN,
                ActionState.RESTING,
                ActionState.PARTIALLY_FILLED,
                ActionState.FILLED,
                ActionState.CANCELED,
            },
            ActionState.PARTIALLY_FILLED: {
                ActionState.PARTIALLY_FILLED,
                ActionState.FILLED,
            },
            ActionState.REJECTED: {ActionState.REJECTED},
            ActionState.FILLED: {ActionState.FILLED},
            ActionState.CANCELED: {ActionState.CANCELED},
        }
        if target not in allowed.get(current.state, set()):
            raise ValueError(f"invalid action transition {current.state.value} -> {target.value}")
        if target is ActionState.FILLED and cumulative != current.requested_size:
            raise ValueError("FILLED requires cumulative fill equal to requested size")
        if target is ActionState.PARTIALLY_FILLED and not (
            Decimal("0") < cumulative < current.requested_size
        ):
            raise ValueError("PARTIALLY_FILLED requires a positive incomplete cumulative fill")
        if target in {ActionState.REJECTED, ActionState.CANCELED} and cumulative != 0:
            raise ValueError(f"{target.value} with a fill must be PARTIALLY_FILLED")
        if target in {ActionState.UNKNOWN, ActionState.RESTING} and (
            cumulative == current.requested_size
        ):
            raise ValueError(f"{target.value} cannot retain a complete fill")


def _record(row: sqlite3.Row) -> ActionRecord:
    return ActionRecord(
        cloid=str(row["cloid"]),
        follower_account=str(row["follower_account"]),
        api_wallet=str(row["api_wallet"]),
        desired_id=str(row["desired_id"]),
        market=str(row["market"]),
        attempt_no=int(row["attempt_no"]),
        nonce=int(row["nonce"]),
        requested_size=Decimal(str(row["requested_size"])),
        cumulative_filled_size=Decimal(str(row["cumulative_filled_size"])),
        action_json=str(row["action_json"]),
        signed_payload_json=str(row["signed_payload_json"]),
        expires_after_ms=int(row["expires_after_ms"]),
        request_id=str(row["request_id"]),
        state=ActionState(str(row["state"])),
        outcome_detail=None if row["outcome_detail"] is None else str(row["outcome_detail"]),
        created_ms=int(row["created_ms"]),
        updated_ms=int(row["updated_ms"]),
        send_attempted_ms=(
            None if row["send_attempted_ms"] is None else int(row["send_attempted_ms"])
        ),
        terminal_ms=None if row["terminal_ms"] is None else int(row["terminal_ms"]),
    )


def _identity(value: str, field: str) -> str:
    return _nonempty(value, field).lower()


def _nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _positive_int(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _nonnegative_decimal(value: Decimal | str | int, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{field} must be finite and non-negative")
    return parsed


def _positive_decimal(value: Decimal | str | int, field: str) -> Decimal:
    parsed = _nonnegative_decimal(value, field)
    if parsed == 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def _decimal_wire(value: Decimal) -> str:
    return format(value, "f")


def _json_object(value: str, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty JSON string")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field} must contain valid JSON") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"{field} must contain a JSON object")
    return value


def _now_ms() -> int:
    return time_ns() // 1_000_000
