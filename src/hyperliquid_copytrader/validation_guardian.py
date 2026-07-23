from __future__ import annotations

import errno
import json
import os
import sqlite3
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


REGISTRY_VERSION = 1
SUPERVISOR_LEASE_VERSION = 1
_TRANSIENT_WINDOWS_FILE_ERRORS = frozenset({5, 32, 33})
_PRIVATE_JSON_REPLACE_RETRY_S = 2.0
_SUPERVISOR_LEASE_MAX_BYTES = 65_536
_SUPERVISOR_LEASE_READ_RETRY_S = 0.35


@dataclass(frozen=True)
class ControllerClaim:
    follower: str
    owner_token: str
    run_id: str
    state_identity_sha256: str
    deadline_ms: int


def atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
    """Durably replace a user-private JSON control file.

    The unique sibling temporary avoids two controllers sharing a predictable
    ``.tmp`` name.  The file is flushed before replacement and best-effort
    restricted to the current OS user; the surrounding LOCALAPPDATA directory
    remains the primary Windows access boundary.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        deadline = time.monotonic() + _PRIVATE_JSON_REPLACE_RETRY_S
        backoff_s = 0.02
        while True:
            try:
                os.replace(temporary, path)
                break
            except OSError as exc:
                if (
                    getattr(exc, "winerror", None) not in _TRANSIENT_WINDOWS_FILE_ERRORS
                    or time.monotonic() >= deadline
                ):
                    raise
                time.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, 0.25)
        with suppress(OSError):
            os.chmod(path, 0o600)
        with suppress(OSError):
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        with suppress(OSError):
            temporary.unlink()


def atomic_supervisor_lease(path: Path, payload: dict[str, Any]) -> None:
    atomic_private_json(path, payload)


def read_supervisor_lease(path: Path) -> dict[str, Any]:
    deadline = time.monotonic() + _SUPERVISOR_LEASE_READ_RETRY_S
    backoff_s = 0.01
    while True:
        try:
            with path.open("rb") as handle:
                raw = handle.read(_SUPERVISOR_LEASE_MAX_BYTES + 1)
            break
        except OSError as exc:
            transient = (
                isinstance(exc, PermissionError)
                or exc.errno == errno.EACCES
                or getattr(exc, "winerror", None) in _TRANSIENT_WINDOWS_FILE_ERRORS
            )
            remaining_s = deadline - time.monotonic()
            if not transient or remaining_s <= 0:
                raise
            time.sleep(min(backoff_s, remaining_s))
            backoff_s = min(backoff_s * 2, 0.1)

    if not raw or len(raw) > _SUPERVISOR_LEASE_MAX_BYTES:
        raise ValueError("supervisor lease file size is invalid")
    value = json.loads(raw.decode("utf-8"))
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != SUPERVISOR_LEASE_VERSION
    ):
        raise ValueError("supervisor lease is missing or has an unsupported version")
    return value


class ControllerRegistry:
    """Atomic cross-run ownership for live follower controllers."""

    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS controller_leases(
              follower TEXT PRIMARY KEY,
              owner_token TEXT NOT NULL,
              run_id TEXT NOT NULL,
              state_identity_sha256 TEXT NOT NULL,
              deadline_ms INTEGER NOT NULL,
              heartbeat_ms INTEGER NOT NULL,
              expires_ms INTEGER NOT NULL,
              status TEXT NOT NULL,
              registry_version INTEGER NOT NULL
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS supervisor_exclusive_leases(
              singleton_id TEXT PRIMARY KEY,
              incarnation_id TEXT NOT NULL UNIQUE,
              owner_token TEXT NOT NULL,
              run_id TEXT NOT NULL,
              state_identity_sha256 TEXT NOT NULL,
              deadline_ms INTEGER NOT NULL,
              follower_set_json TEXT NOT NULL,
              heartbeat_ms INTEGER NOT NULL,
              expires_ms INTEGER NOT NULL,
              status TEXT NOT NULL,
              registry_version INTEGER NOT NULL
            )
            """
        )
        with suppress(OSError):
            os.chmod(self.path, 0o600)

    def close(self) -> None:
        self.conn.close()

    @staticmethod
    def _normalized(claims: Iterable[ControllerClaim]) -> list[ControllerClaim]:
        normalized = [
            ControllerClaim(
                follower=claim.follower.strip().lower(),
                owner_token=claim.owner_token,
                run_id=claim.run_id,
                state_identity_sha256=claim.state_identity_sha256,
                deadline_ms=int(claim.deadline_ms),
            )
            for claim in claims
        ]
        followers = [claim.follower for claim in normalized]
        if not normalized or len(followers) != len(set(followers)):
            raise ValueError("controller claims must contain unique followers")
        return normalized

    @classmethod
    def _exclusive_claims(cls, claims: Iterable[ControllerClaim]) -> list[ControllerClaim]:
        items = cls._normalized(claims)
        if len(items) != 2:
            raise ValueError("exclusive validation ownership requires exactly two followers")
        identities = {
            (
                claim.owner_token,
                claim.run_id,
                claim.state_identity_sha256,
                claim.deadline_ms,
            )
            for claim in items
        }
        if len(identities) != 1:
            raise ValueError(
                "exclusive validation claims must share one owner/run/state/deadline identity"
            )
        identity = next(iter(identities))
        if not all(identity[:3]) or identity[3] <= 0:
            raise ValueError("exclusive validation identity fields must be non-empty")
        return items

    def acquire_many(
        self,
        claims: Iterable[ControllerClaim],
        *,
        observed_ms: int,
        ttl_ms: int,
        status: str = "starting",
    ) -> tuple[bool, list[dict[str, Any]]]:
        items = self._normalized(claims)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            collisions: list[dict[str, Any]] = []
            exclusive = self.conn.execute(
                "SELECT * FROM supervisor_exclusive_leases "
                "WHERE singleton_id='mainnet-two-account' AND expires_ms>?",
                (observed_ms,),
            ).fetchone()
            if exclusive is not None:
                self.conn.execute("ROLLBACK")
                return False, [dict(exclusive)]
            for claim in items:
                row = self.conn.execute(
                    "SELECT * FROM controller_leases WHERE follower=?",
                    (claim.follower,),
                ).fetchone()
                if row is None or int(row["expires_ms"]) <= observed_ms:
                    continue
                same_owner = (
                    str(row["owner_token"]) == claim.owner_token
                    and str(row["run_id"]) == claim.run_id
                    and str(row["state_identity_sha256"]) == claim.state_identity_sha256
                    and int(row["deadline_ms"]) == claim.deadline_ms
                )
                if not same_owner:
                    collisions.append(dict(row))
            if collisions:
                self.conn.execute("ROLLBACK")
                return False, collisions
            for claim in items:
                self.conn.execute(
                    """
                    INSERT INTO controller_leases(
                      follower, owner_token, run_id, state_identity_sha256,
                      deadline_ms, heartbeat_ms, expires_ms, status, registry_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(follower) DO UPDATE SET
                      owner_token=excluded.owner_token,
                      run_id=excluded.run_id,
                      state_identity_sha256=excluded.state_identity_sha256,
                      deadline_ms=excluded.deadline_ms,
                      heartbeat_ms=excluded.heartbeat_ms,
                      expires_ms=excluded.expires_ms,
                      status=excluded.status,
                      registry_version=excluded.registry_version
                    """,
                    (
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                        observed_ms,
                        observed_ms + ttl_ms,
                        status,
                        REGISTRY_VERSION,
                    ),
                )
            self.conn.execute("COMMIT")
            return True, []
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def acquire_exclusive_set(
        self,
        claims: Iterable[ControllerClaim],
        *,
        incarnation_id: str,
        observed_ms: int,
        ttl_ms: int,
        previous_owner_token: str = "",
        status: str = "starting",
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Atomically own the singleton host lease and the exact follower set."""

        items = self._exclusive_claims(claims)
        expected_followers = sorted(claim.follower for claim in items)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "DELETE FROM supervisor_exclusive_leases WHERE expires_ms<=?", (observed_ms,)
            )
            self.conn.execute("DELETE FROM controller_leases WHERE expires_ms<=?", (observed_ms,))
            collisions: list[dict[str, Any]] = []
            exclusive = self.conn.execute(
                "SELECT * FROM supervisor_exclusive_leases WHERE singleton_id='mainnet-two-account'"
            ).fetchone()
            if exclusive is not None:
                collisions.append(dict(exclusive))
            active_rows = self.conn.execute(
                "SELECT * FROM controller_leases WHERE expires_ms>? ORDER BY follower",
                (observed_ms,),
            ).fetchall()
            expected_by_follower = {claim.follower: claim for claim in items}
            for row in active_rows:
                claim = expected_by_follower.get(str(row["follower"]))
                recoverable_previous = bool(
                    claim is not None
                    and previous_owner_token
                    and str(row["owner_token"]) == previous_owner_token
                    and str(row["run_id"]) == claim.run_id
                    and str(row["state_identity_sha256"]) == claim.state_identity_sha256
                    and int(row["deadline_ms"]) == claim.deadline_ms
                )
                if not recoverable_previous:
                    collisions.append(dict(row))
            if collisions:
                self.conn.execute("ROLLBACK")
                return False, collisions
            for claim in items:
                self.conn.execute(
                    """
                    INSERT INTO controller_leases(
                      follower, owner_token, run_id, state_identity_sha256,
                      deadline_ms, heartbeat_ms, expires_ms, status, registry_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(follower) DO UPDATE SET
                      owner_token=excluded.owner_token,
                      run_id=excluded.run_id,
                      state_identity_sha256=excluded.state_identity_sha256,
                      deadline_ms=excluded.deadline_ms,
                      heartbeat_ms=excluded.heartbeat_ms,
                      expires_ms=excluded.expires_ms,
                      status=excluded.status,
                      registry_version=excluded.registry_version
                    """,
                    (
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                        observed_ms,
                        observed_ms + ttl_ms,
                        status,
                        REGISTRY_VERSION,
                    ),
                )
            first = items[0]
            self.conn.execute(
                """
                INSERT INTO supervisor_exclusive_leases(
                  singleton_id, incarnation_id, owner_token, run_id,
                  state_identity_sha256, deadline_ms, follower_set_json,
                  heartbeat_ms, expires_ms, status, registry_version
                ) VALUES ('mainnet-two-account', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    incarnation_id,
                    first.owner_token,
                    first.run_id,
                    first.state_identity_sha256,
                    first.deadline_ms,
                    json.dumps(expected_followers, separators=(",", ":")),
                    observed_ms,
                    observed_ms + ttl_ms,
                    status,
                    REGISTRY_VERSION,
                ),
            )
            self.conn.execute("COMMIT")
            return True, []
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def renew_exclusive_set(
        self,
        claims: Iterable[ControllerClaim],
        *,
        incarnation_id: str,
        observed_ms: int,
        ttl_ms: int,
        status: str,
    ) -> bool:
        items = self._exclusive_claims(claims)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            first = items[0]
            cursor = self.conn.execute(
                """
                UPDATE supervisor_exclusive_leases
                SET heartbeat_ms=?, expires_ms=?, status=?
                WHERE singleton_id='mainnet-two-account' AND incarnation_id=?
                  AND owner_token=? AND run_id=? AND state_identity_sha256=?
                  AND deadline_ms=?
                """,
                (
                    observed_ms,
                    observed_ms + ttl_ms,
                    status,
                    incarnation_id,
                    first.owner_token,
                    first.run_id,
                    first.state_identity_sha256,
                    first.deadline_ms,
                ),
            )
            if cursor.rowcount != 1:
                self.conn.execute("ROLLBACK")
                return False
            for claim in items:
                cursor = self.conn.execute(
                    """
                    UPDATE controller_leases SET heartbeat_ms=?, expires_ms=?, status=?
                    WHERE follower=? AND owner_token=? AND run_id=?
                      AND state_identity_sha256=? AND deadline_ms=?
                    """,
                    (
                        observed_ms,
                        observed_ms + ttl_ms,
                        status,
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                    ),
                )
                if cursor.rowcount != 1:
                    self.conn.execute("ROLLBACK")
                    return False
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def release_exclusive_set(
        self,
        claims: Iterable[ControllerClaim],
        *,
        incarnation_id: str,
    ) -> bool:
        items = self._exclusive_claims(claims)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            first = items[0]
            cursor = self.conn.execute(
                """
                DELETE FROM supervisor_exclusive_leases
                WHERE singleton_id='mainnet-two-account' AND incarnation_id=?
                  AND owner_token=? AND run_id=? AND state_identity_sha256=?
                  AND deadline_ms=?
                """,
                (
                    incarnation_id,
                    first.owner_token,
                    first.run_id,
                    first.state_identity_sha256,
                    first.deadline_ms,
                ),
            )
            if cursor.rowcount != 1:
                self.conn.execute("ROLLBACK")
                return False
            for claim in items:
                cursor = self.conn.execute(
                    """
                    DELETE FROM controller_leases
                    WHERE follower=? AND owner_token=? AND run_id=?
                      AND state_identity_sha256=? AND deadline_ms=?
                    """,
                    (
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                    ),
                )
                if cursor.rowcount != 1:
                    self.conn.execute("ROLLBACK")
                    return False
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def exclusive_lease(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM supervisor_exclusive_leases WHERE singleton_id='mainnet-two-account'"
        ).fetchone()
        return None if row is None else dict(row)

    def exclusive_snapshot(
        self, followers: Iterable[str]
    ) -> tuple[dict[str, dict[str, Any] | None], dict[str, Any] | None]:
        """Read the exact-set leases and singleton from one SQLite snapshot."""

        normalized = tuple(item.strip().lower() for item in followers)
        self.conn.execute("BEGIN")
        try:
            leases = {follower: self.lease(follower) for follower in normalized}
            exclusive = self.exclusive_lease()
            self.conn.execute("COMMIT")
            return leases, exclusive
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def renew_many(
        self,
        claims: Iterable[ControllerClaim],
        *,
        observed_ms: int,
        ttl_ms: int,
        status: str,
    ) -> bool:
        items = self._normalized(claims)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for claim in items:
                cursor = self.conn.execute(
                    """
                    UPDATE controller_leases
                    SET heartbeat_ms=?, expires_ms=?, status=?
                    WHERE follower=? AND owner_token=? AND run_id=?
                      AND state_identity_sha256=? AND deadline_ms=?
                    """,
                    (
                        observed_ms,
                        observed_ms + ttl_ms,
                        status,
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                    ),
                )
                if cursor.rowcount != 1:
                    self.conn.execute("ROLLBACK")
                    return False
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def release_many(self, claims: Iterable[ControllerClaim]) -> bool:
        items = self._normalized(claims)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for claim in items:
                cursor = self.conn.execute(
                    """
                    DELETE FROM controller_leases
                    WHERE follower=? AND owner_token=? AND run_id=?
                      AND state_identity_sha256=? AND deadline_ms=?
                    """,
                    (
                        claim.follower,
                        claim.owner_token,
                        claim.run_id,
                        claim.state_identity_sha256,
                        claim.deadline_ms,
                    ),
                )
                if cursor.rowcount != 1:
                    self.conn.execute("ROLLBACK")
                    return False
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def lease(self, follower: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT * FROM controller_leases WHERE follower=?",
            (follower.strip().lower(),),
        ).fetchone()
        return None if row is None else dict(row)
