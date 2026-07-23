from __future__ import annotations

import asyncio
import ctypes
import json
import os
import re
import subprocess
from contextlib import suppress
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, sleep, time_ns
from typing import IO, Any, Sequence


if os.name == "nt":
    from ctypes import wintypes


PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
PROCESS_TERMINATE = 0x0001
SYNCHRONIZE = 0x00100000
STILL_ACTIVE = 259
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
CREATE_NO_WINDOW = 0x08000000
CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008
ERROR_INVALID_PARAMETER = 87
DRIVE_FIXED = 3
_TRANSIENT_WINDOWS_REPLACE_ERRORS = frozenset({5, 32, 33})
_ATOMIC_JSON_REPLACE_DELAYS_S = (0.005, 0.010, 0.020, 0.040, 0.080, 0.100)


def default_event_loop_identity() -> str:
    """Return the default loop implementation without deprecated policy APIs."""

    loop = asyncio.new_event_loop()
    try:
        return type(loop).__name__
    finally:
        loop.close()


def _kernel32() -> ctypes.WinDLL:
    if os.name != "nt":
        raise RuntimeError("Windows process APIs require Windows")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.GetProcessTimes.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.QueryFullProcessImageNameW.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.GetExitCodeProcess.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.GetVolumePathNameW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
    ]
    kernel32.GetVolumePathNameW.restype = wintypes.BOOL
    kernel32.GetVolumeInformationW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
        wintypes.DWORD,
    ]
    kernel32.GetVolumeInformationW.restype = wintypes.BOOL
    kernel32.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
    kernel32.GetDriveTypeW.restype = wintypes.UINT
    kernel32.SetThreadExecutionState.argtypes = [wintypes.DWORD]
    kernel32.SetThreadExecutionState.restype = wintypes.DWORD
    return kernel32


@dataclass(frozen=True, slots=True)
class WindowsProcessIdentity:
    pid: int
    creation_filetime: int
    image_path: str

    @property
    def canonical(self) -> str:
        material = f"{self.pid}|{self.creation_filetime}|{self.image_path.lower()}"
        return f"winproc:{self.pid}:{sha256(material.encode('utf-8')).hexdigest()[:24]}"


class ExactWindowsProcessHandle:
    """Retained SYNCHRONIZE handle bound to one creation identity."""

    def __init__(self, identity: WindowsProcessIdentity) -> None:
        if os.name != "nt":
            raise RuntimeError("exact Windows process handles require Windows")
        self.identity = identity
        self._kernel32 = _kernel32()
        self._handle = self._kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
            False,
            identity.pid,
        )
        if not self._handle:
            error = ctypes.get_last_error()
            if error == ERROR_INVALID_PARAMETER:
                raise ProcessLookupError(identity.pid)
            raise OSError(error, f"OpenProcess handle failed for PID {identity.pid}")
        try:
            observed = inspect_process_identity(identity.pid)
            if observed != identity:
                raise ProcessLookupError(
                    f"PID {identity.pid} no longer names the recorded creation identity"
                )
        except BaseException:
            self.close()
            raise

    def poll_exit_code(self) -> int | None:
        if not self._handle:
            raise RuntimeError("exact process handle is closed")
        result = self._kernel32.WaitForSingleObject(self._handle, 0)
        if result == 0x102:
            return None
        if result != 0:
            raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
        code = wintypes.DWORD()
        if not self._kernel32.GetExitCodeProcess(self._handle, ctypes.byref(code)):
            raise OSError(ctypes.get_last_error(), "GetExitCodeProcess failed")
        if int(code.value) == STILL_ACTIVE:
            raise RuntimeError("signaled process handle still reports STILL_ACTIVE")
        return int(code.value)

    def wait(self, timeout_ms: int) -> int | None:
        if timeout_ms < 0:
            raise ValueError("process wait timeout must be non-negative")
        if not self._handle:
            raise RuntimeError("exact process handle is closed")
        result = self._kernel32.WaitForSingleObject(self._handle, timeout_ms)
        if result == 0x102:
            return None
        if result != 0:
            raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
        return self.poll_exit_code()

    def close(self) -> None:
        handle = self._handle
        if handle:
            self._kernel32.CloseHandle(handle)
            self._handle = None

    def __enter__(self) -> ExactWindowsProcessHandle:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def inspect_process_identity(pid: int) -> WindowsProcessIdentity:
    if os.name != "nt":
        raise RuntimeError("exact Windows process identity requires Windows")
    if pid <= 0:
        raise ValueError("process ID must be positive")
    kernel32 = _kernel32()
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, False, pid)
    if not handle:
        error = ctypes.get_last_error()
        if error == ERROR_INVALID_PARAMETER:
            raise ProcessLookupError(pid)
        raise OSError(error, f"OpenProcess failed for PID {pid}")
    try:
        # A retained SYNCHRONIZE handle keeps the process object (and, on
        # Windows, sometimes its PID) queryable after exit.  Image-path lookup
        # on that signalled object fails with ERROR_GEN_FAILURE, which must mean
        # "this exact process exited", not a lifecycle-monitor exception.
        wait_result = kernel32.WaitForSingleObject(handle, 0)
        if wait_result == 0:
            raise ProcessLookupError(pid)
        if wait_result != 0x102:
            raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
        creation = wintypes.FILETIME()
        exit_time = wintypes.FILETIME()
        kernel = wintypes.FILETIME()
        user = wintypes.FILETIME()
        if not kernel32.GetProcessTimes(
            handle,
            ctypes.byref(creation),
            ctypes.byref(exit_time),
            ctypes.byref(kernel),
            ctypes.byref(user),
        ):
            raise OSError(ctypes.get_last_error(), "GetProcessTimes failed")
        capacity = wintypes.DWORD(32768)
        buffer = ctypes.create_unicode_buffer(capacity.value)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(capacity)):
            raise OSError(ctypes.get_last_error(), "QueryFullProcessImageNameW failed")
        filetime = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
        return WindowsProcessIdentity(
            pid=pid,
            creation_filetime=filetime,
            image_path=buffer.value,
        )
    finally:
        kernel32.CloseHandle(handle)


def inspect_spawned_process_identity(
    process: subprocess.Popen[Any], *, expected_image_path: Path | str
) -> WindowsProcessIdentity:
    """Read a just-spawned process identity from Popen's retained process handle.

    Unlike a PID re-open, the retained CreateProcess handle remains bound to the
    same process object even when a very short-lived child exits before identity
    capture.  The known executable path is used only when Windows no longer
    permits image-path lookup on an already-signalled object.
    """

    if os.name != "nt":
        raise RuntimeError("exact Windows spawned-process identity requires Windows")
    if process.pid <= 0:
        raise ValueError("spawned process ID must be positive")
    handle = getattr(process, "_handle", None)
    if not handle:
        raise RuntimeError("spawned process does not retain a Windows process handle")
    kernel32 = _kernel32()
    wait_result = kernel32.WaitForSingleObject(handle, 0)
    if wait_result not in {0, 0x102}:
        raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
    creation = wintypes.FILETIME()
    exit_time = wintypes.FILETIME()
    kernel = wintypes.FILETIME()
    user = wintypes.FILETIME()
    if not kernel32.GetProcessTimes(
        handle,
        ctypes.byref(creation),
        ctypes.byref(exit_time),
        ctypes.byref(kernel),
        ctypes.byref(user),
    ):
        raise OSError(ctypes.get_last_error(), "GetProcessTimes failed")
    capacity = wintypes.DWORD(32768)
    buffer = ctypes.create_unicode_buffer(capacity.value)
    if kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(capacity)):
        image_path = buffer.value
    elif wait_result == 0:
        image_path = str(Path(expected_image_path).resolve())
    else:
        raise OSError(ctypes.get_last_error(), "QueryFullProcessImageNameW failed")
    filetime = (int(creation.dwHighDateTime) << 32) | int(creation.dwLowDateTime)
    return WindowsProcessIdentity(
        pid=int(process.pid), creation_filetime=filetime, image_path=image_path
    )


def exact_process_is_alive(identity: WindowsProcessIdentity) -> bool:
    try:
        observed = inspect_process_identity(identity.pid)
    except ProcessLookupError:
        return False
    return observed == identity


def wait_exact_process_exit(identity: WindowsProcessIdentity, timeout_ms: int) -> bool:
    if os.name != "nt":
        raise RuntimeError("exact Windows process waiting requires Windows")
    if timeout_ms < 0:
        raise ValueError("process wait timeout must be non-negative")
    kernel32 = _kernel32()
    handle = kernel32.OpenProcess(
        PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE, False, identity.pid
    )
    if not handle:
        error = ctypes.get_last_error()
        if error == ERROR_INVALID_PARAMETER:
            return True
        raise OSError(error, f"OpenProcess wait failed for PID {identity.pid}")
    try:
        try:
            current = inspect_process_identity(identity.pid)
        except ProcessLookupError:
            return True
        if current != identity:
            return True
        result = kernel32.WaitForSingleObject(handle, timeout_ms)
        if result == 0:
            return True
        if result == 0x102:
            return False
        raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
    finally:
        kernel32.CloseHandle(handle)


def terminate_exact_process(
    identity: WindowsProcessIdentity,
    *,
    exit_code: int = 0xF17E,
    wait_timeout_ms: int = 30_000,
) -> bool:
    """Hard-fence one exact Windows process instance and prove its handle signalled."""

    if os.name != "nt":
        raise RuntimeError("exact Windows process termination requires Windows")
    if wait_timeout_ms < 0:
        raise ValueError("process wait timeout must be non-negative")
    kernel32 = _kernel32()
    handle = kernel32.OpenProcess(
        PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | SYNCHRONIZE,
        False,
        identity.pid,
    )
    if not handle:
        error = ctypes.get_last_error()
        if error == ERROR_INVALID_PARAMETER:
            return True
        raise OSError(error, f"OpenProcess terminate failed for PID {identity.pid}")
    try:
        try:
            current = inspect_process_identity(identity.pid)
        except ProcessLookupError:
            return True
        if current != identity:
            return True
        if not kernel32.TerminateProcess(handle, exit_code):
            error = ctypes.get_last_error()
            if not exact_process_is_alive(identity):
                return True
            raise OSError(error, "TerminateProcess failed")
        result = kernel32.WaitForSingleObject(handle, wait_timeout_ms)
        if result == 0:
            return True
        if result == 0x102:
            return False
        raise OSError(ctypes.get_last_error(), "WaitForSingleObject failed")
    finally:
        kernel32.CloseHandle(handle)


class SleepInhibitor:
    """Supervisor-owned Windows system-sleep inhibition."""

    def __init__(self) -> None:
        self._active = False

    def acquire(self) -> None:
        if os.name != "nt":
            raise RuntimeError("sleep inhibition requires Windows")
        result = _kernel32().SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)
        if result == 0:
            raise OSError(ctypes.get_last_error(), "SetThreadExecutionState failed")
        self._active = True

    def release(self) -> None:
        if not self._active:
            return
        result = _kernel32().SetThreadExecutionState(ES_CONTINUOUS)
        if result == 0:
            raise OSError(ctypes.get_last_error(), "sleep inhibition release failed")
        self._active = False

    def __enter__(self) -> SleepInhibitor:
        self.acquire()
        return self

    def __exit__(self, *_: object) -> None:
        self.release()


@dataclass(frozen=True, slots=True)
class ClockSample:
    sampled_wall_ms: int
    sampled_mono_ns: int
    source: str
    offsets_ms: tuple[float, ...]
    max_abs_offset_ms: float
    w32time_status: str
    raw_sha256: str


@dataclass(frozen=True, slots=True)
class ClockHealth:
    healthy: bool
    max_skew_ms: int
    max_jump_ms: int
    measured_offset_ms: float | None
    observed_jump_ms: float | None
    reasons: tuple[str, ...]
    sample: ClockSample | None


_OFFSET_RE = re.compile(r"([+-]?\d+(?:[.,]\d+)?)s(?:\s|$)", re.IGNORECASE)


def sample_windows_clock(
    *, server: str = "time.windows.com", samples: int = 3, timeout_s: int = 15
) -> ClockSample:
    if os.name != "nt":
        raise RuntimeError("trusted clock sampling requires Windows")
    if samples < 1:
        raise ValueError("clock sample count must be positive")
    command = [
        "w32tm",
        "/stripchart",
        f"/computer:{server}",
        f"/samples:{samples}",
        "/dataonly",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_s,
        check=False,
        creationflags=CREATE_NO_WINDOW,
    )
    raw = (completed.stdout or "") + "\n" + (completed.stderr or "")
    if completed.returncode != 0:
        raise RuntimeError(
            f"w32tm trusted clock sample failed with exit {completed.returncode}: {raw.strip()}"
        )
    offsets = tuple(
        float(match.group(1).replace(",", ".")) * 1000 for match in _OFFSET_RE.finditer(raw)
    )
    if len(offsets) < samples:
        raise RuntimeError("w32tm output did not contain all requested offset samples")
    query = subprocess.run(
        ["sc.exe", "query", "W32Time"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=5,
        check=False,
        creationflags=CREATE_NO_WINDOW,
    )
    status = ((query.stdout or "") + " " + (query.stderr or "")).strip()
    return ClockSample(
        sampled_wall_ms=time_ns() // 1_000_000,
        sampled_mono_ns=monotonic_ns(),
        source=server,
        offsets_ms=offsets,
        max_abs_offset_ms=max(abs(value) for value in offsets),
        w32time_status="RUNNING" if "RUNNING" in status.upper() else status[:500],
        raw_sha256=sha256(raw.encode("utf-8")).hexdigest(),
    )


class ClockHealthMonitor:
    def __init__(self, *, max_skew_ms: int, max_jump_ms: int) -> None:
        if max_skew_ms <= 0 or max_jump_ms <= 0:
            raise ValueError("clock health limits must be positive")
        self.max_skew_ms = max_skew_ms
        self.max_jump_ms = max_jump_ms
        self._sample: ClockSample | None = None
        self._anchor_wall_ms: int | None = None
        self._anchor_mono_ns: int | None = None
        self._last_jump_ms: float | None = None
        self._latched_reasons: set[str] = set()

    def accept_trusted_sample(self, sample: ClockSample) -> ClockHealth:
        self._sample = sample
        self._anchor_wall_ms = sample.sampled_wall_ms
        self._anchor_mono_ns = sample.sampled_mono_ns
        self._last_jump_ms = 0.0
        self._latched_reasons.clear()
        if sample.max_abs_offset_ms > self.max_skew_ms:
            self._latched_reasons.add("trusted_clock_skew_exceeded")
        return self.health()

    def observe(self, *, wall_ms: int | None = None, mono_ns: int | None = None) -> ClockHealth:
        wall = time_ns() // 1_000_000 if wall_ms is None else wall_ms
        mono = monotonic_ns() if mono_ns is None else mono_ns
        if self._anchor_wall_ms is None or self._anchor_mono_ns is None:
            self._anchor_wall_ms = wall
            self._anchor_mono_ns = mono
            self._last_jump_ms = 0.0
        else:
            wall_elapsed = wall - self._anchor_wall_ms
            mono_elapsed = (mono - self._anchor_mono_ns) / 1_000_000
            self._last_jump_ms = wall_elapsed - mono_elapsed
            if abs(self._last_jump_ms) > self.max_jump_ms:
                self._latched_reasons.add("wall_monotonic_jump_exceeded")
        return self.health()

    def health(self) -> ClockHealth:
        reasons: list[str] = sorted(self._latched_reasons)
        measured = None if self._sample is None else self._sample.max_abs_offset_ms
        if self._sample is None:
            if "no_trusted_clock_sample" not in reasons:
                reasons.append("no_trusted_clock_sample")
        elif measured is not None and measured > self.max_skew_ms:
            if "trusted_clock_skew_exceeded" not in reasons:
                reasons.append("trusted_clock_skew_exceeded")
        if self._last_jump_ms is not None and abs(self._last_jump_ms) > self.max_jump_ms:
            if "wall_monotonic_jump_exceeded" not in reasons:
                reasons.append("wall_monotonic_jump_exceeded")
        return ClockHealth(
            healthy=not reasons,
            max_skew_ms=self.max_skew_ms,
            max_jump_ms=self.max_jump_ms,
            measured_offset_ms=measured,
            observed_jump_ms=self._last_jump_ms,
            reasons=tuple(reasons),
            sample=self._sample,
        )


@dataclass(frozen=True, slots=True)
class RuntimeStorageCheck:
    path: str
    filesystem: str
    volume_root: str
    local_non_cloud_ntfs: bool
    reasons: tuple[str, ...]
    drive_type: int = 0
    atomic_replace_proven: bool = False


def verify_local_ntfs_runtime(path: Path | str) -> RuntimeStorageCheck:
    target = Path(path).expanduser().resolve()
    reasons: list[str] = []
    if os.name != "nt":
        reasons.append("runtime host is not Windows")
        return RuntimeStorageCheck(str(target), "", "", False, tuple(reasons))
    target.mkdir(parents=True, exist_ok=True)
    kernel32 = _kernel32()
    volume_buffer = ctypes.create_unicode_buffer(32768)
    if not kernel32.GetVolumePathNameW(str(target), volume_buffer, len(volume_buffer)):
        raise OSError(ctypes.get_last_error(), "GetVolumePathNameW failed")
    volume_root = volume_buffer.value
    fs_buffer = ctypes.create_unicode_buffer(256)
    if not kernel32.GetVolumeInformationW(
        volume_root,
        None,
        0,
        None,
        None,
        None,
        fs_buffer,
        len(fs_buffer),
    ):
        raise OSError(ctypes.get_last_error(), "GetVolumeInformationW failed")
    filesystem = fs_buffer.value.upper()
    drive_type = int(kernel32.GetDriveTypeW(volume_root))
    if filesystem != "NTFS":
        reasons.append(f"runtime filesystem is {filesystem}, not NTFS")
    if drive_type != DRIVE_FIXED:
        reasons.append(f"runtime volume drive type is {drive_type}, not fixed local storage")
    target_text = str(target).lower()
    one_drive_roots = {
        str(Path(value).expanduser().resolve()).lower()
        for key, value in os.environ.items()
        if key.upper().startswith("ONEDRIVE") and value.strip()
    }
    if any(
        target_text == root or target_text.startswith(root + os.sep) for root in one_drive_roots
    ):
        reasons.append("runtime path is under a OneDrive root")
    if "onedrive" in {part.lower() for part in target.parts}:
        reasons.append("runtime path contains a OneDrive directory")
    probe_a = target / f".runtime-probe-{os.getpid()}.a"
    probe_b = target / f".runtime-probe-{os.getpid()}.b"
    atomic_replace_proven = False
    try:
        with probe_a.open("xb") as handle:
            handle.write(b"a")
            handle.flush()
            os.fsync(handle.fileno())
        with probe_b.open("xb") as handle:
            handle.write(b"b")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(probe_a, probe_b)
        atomic_replace_proven = probe_b.read_bytes() == b"a"
        if not atomic_replace_proven:
            reasons.append("runtime atomic replace verification returned unexpected content")
    except OSError as exc:
        reasons.append(f"runtime atomic replace probe failed: {exc}")
    finally:
        for probe in (probe_a, probe_b):
            try:
                probe.unlink()
            except FileNotFoundError:
                pass
    return RuntimeStorageCheck(
        path=str(target),
        filesystem=filesystem,
        volume_root=volume_root,
        local_non_cloud_ntfs=not reasons,
        reasons=tuple(reasons),
        drive_type=drive_type,
        atomic_replace_proven=atomic_replace_proven,
    )


def spawn_hidden_detached(
    argv: Sequence[str],
    *,
    cwd: Path | str,
    stdout: IO[bytes] | int,
    stderr: IO[bytes] | int,
    env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes]:
    if os.name != "nt":
        raise RuntimeError("detached fleet processes require native Windows")
    if not argv:
        raise ValueError("detached process argv must be non-empty")
    return subprocess.Popen(
        list(argv),
        cwd=str(Path(cwd).resolve()),
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        env=env,
        close_fds=True,
        creationflags=CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
    )


def atomic_json_write(path: Path | str, payload: object) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    temporary = target.with_name(f".{target.name}.{os.getpid()}.{os.urandom(8).hex()}.tmp")
    # Exclusive creation makes the cleanup ownership unambiguous.  Write and
    # fsync once; only the Windows replace operation is safe to retry when a
    # reader, indexer, or antivirus briefly denies delete sharing on target.
    handle = temporary.open("xb")
    try:
        with handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(len(_ATOMIC_JSON_REPLACE_DELAYS_S) + 1):
            try:
                os.replace(temporary, target)
                break
            except OSError as exc:
                if getattr(
                    exc, "winerror", None
                ) not in _TRANSIENT_WINDOWS_REPLACE_ERRORS or attempt >= len(
                    _ATOMIC_JSON_REPLACE_DELAYS_S
                ):
                    raise
                sleep(_ATOMIC_JSON_REPLACE_DELAYS_S[attempt])
    finally:
        with suppress(OSError):
            temporary.unlink()


def process_identity_payload(identity: WindowsProcessIdentity) -> dict[str, object]:
    return {**asdict(identity), "canonical": identity.canonical}
