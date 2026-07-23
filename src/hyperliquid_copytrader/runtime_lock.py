from __future__ import annotations

import errno
import hashlib
import importlib
import os
from pathlib import Path
from threading import Lock


class RuntimeFileLockError(RuntimeError):
    """Raised when the account-global runtime lock cannot be used safely."""


class RuntimeFileLockBusy(RuntimeFileLockError):
    """Raised when another local process or thread owns the runtime lock."""


_PROCESS_LOCK_GUARD = Lock()
_PROCESS_LOCKS: set[str] = set()


def default_runtime_lock_dir() -> Path:
    """Return a stable, user-local directory shared by every checkout and run directory."""

    local_app_data = os.getenv("LOCALAPPDATA", "").strip()
    if local_app_data:
        return Path(local_app_data) / "hyperliquid-copytrader" / "runtime-locks"
    return Path.home() / ".hyperliquid-copytrader" / "runtime-locks"


def account_runtime_lock_path(
    lock_dir: Path | str,
    *,
    network: str,
    action_account: str,
) -> Path:
    normalized_network = (network or "unknown").strip().lower()
    normalized_account = (action_account or "unconfigured").strip().lower()
    identity = f"{normalized_network}:{normalized_account}"
    digest = hashlib.blake2s(identity.encode("utf-8"), digest_size=16).hexdigest()
    return Path(lock_dir) / f"{normalized_network}-{digest}.lock"


def signer_runtime_lock_path(
    lock_dir: Path | str,
    *,
    network: str,
    signer_address: str,
) -> Path:
    normalized_network = (network or "unknown").strip().lower()
    normalized_signer = (signer_address or "unconfigured").strip().lower()
    identity = f"{normalized_network}:signer:{normalized_signer}"
    digest = hashlib.blake2s(identity.encode("utf-8"), digest_size=16).hexdigest()
    return Path(lock_dir) / f"{normalized_network}-signer-{digest}.lock"


def generation_fence_lock_path(
    lock_dir: Path | str,
    *,
    network: str,
    action_account: str,
) -> Path:
    """Return the cross-signer ownership fence for one action account.

    Unlike the signer nonce lock, this identity is shared by every API wallet that can act
    for the account. Validation supervisors take the same fence around ownership handoff;
    children hold it from their final generation check through the signed send.
    """

    normalized_network = (network or "unknown").strip().lower()
    normalized_account = (action_account or "unconfigured").strip().lower()
    identity = f"{normalized_network}:generation-fence:{normalized_account}"
    digest = hashlib.blake2s(identity.encode("utf-8"), digest_size=16).hexdigest()
    return Path(lock_dir) / f"{normalized_network}-generation-{digest}.lock"


class AccountRuntimeFileLock:
    """Crash-safe, non-blocking OS lock for one execution network/account pair.

    The lock file is intentionally retained after release. Ownership is determined only by
    the operating-system byte-range lock, so a process crash cannot leave a stale logical lock.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._fd: int | None = None
        self._registry_key = _registry_key(self.path)

    @property
    def acquired(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        if self._fd is not None:
            raise RuntimeFileLockError(f"runtime file lock is already held: {self.path}")

        with _PROCESS_LOCK_GUARD:
            if self._registry_key in _PROCESS_LOCKS:
                raise RuntimeFileLockBusy(f"runtime file lock is held in this process: {self.path}")
            _PROCESS_LOCKS.add(self._registry_key)

        fd: int | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            flags = os.O_CREAT | os.O_RDWR
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(self.path, flags, 0o600)
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
                os.fsync(fd)
            os.lseek(fd, 0, os.SEEK_SET)
            _acquire_os_lock(fd, self.path)
            self._fd = fd
        except Exception:
            if fd is not None:
                os.close(fd)
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCKS.discard(self._registry_key)
            raise

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        try:
            _release_os_lock(fd)
        finally:
            os.close(fd)
            with _PROCESS_LOCK_GUARD:
                _PROCESS_LOCKS.discard(self._registry_key)

    def __enter__(self) -> AccountRuntimeFileLock:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()


def _registry_key(path: Path) -> str:
    resolved = str(path.expanduser().resolve())
    return resolved.casefold() if os.name == "nt" else resolved


def _acquire_os_lock(fd: int, path: Path) -> None:
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK, errno.EPERM} or getattr(
                exc, "winerror", None
            ) in {33, 36, 158}:
                raise RuntimeFileLockBusy(
                    f"runtime file lock is held by another process: {path}"
                ) from exc
            raise RuntimeFileLockError(
                f"could not acquire runtime file lock {path}: {exc}"
            ) from exc
        return

    fcntl = importlib.import_module("fcntl")

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EAGAIN}:
            raise RuntimeFileLockBusy(
                f"runtime file lock is held by another process: {path}"
            ) from exc
        raise RuntimeFileLockError(f"could not acquire runtime file lock {path}: {exc}") from exc


def _release_os_lock(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        return

    fcntl = importlib.import_module("fcntl")

    fcntl.flock(fd, fcntl.LOCK_UN)
