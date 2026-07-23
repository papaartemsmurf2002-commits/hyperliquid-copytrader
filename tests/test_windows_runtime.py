from __future__ import annotations

import json
from pathlib import Path

import pytest

from hyperliquid_copytrader import windows_runtime


class _WindowsReplaceError(PermissionError):
    def __init__(self, winerror: int) -> None:
        super().__init__(f"replace failed with WinError {winerror}")
        self.winerror = winerror


def _owned_temps(target: Path) -> list[Path]:
    return list(target.parent.glob(f".{target.name}.*.tmp"))


def test_atomic_json_write_retries_only_transient_windows_replace_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "heartbeat.json"
    target.write_text("old", encoding="utf-8")
    real_replace = windows_runtime.os.replace
    sources: list[Path] = []
    sleeps: list[float] = []
    failures = iter((5, 32, 33))

    def flaky_replace(source: str | bytes | Path, destination: str | bytes | Path) -> None:
        sources.append(Path(source.decode() if isinstance(source, bytes) else source))
        try:
            raise _WindowsReplaceError(next(failures))
        except StopIteration:
            real_replace(source, destination)

    monkeypatch.setattr(windows_runtime.os, "replace", flaky_replace)
    monkeypatch.setattr(windows_runtime, "sleep", sleeps.append)

    windows_runtime.atomic_json_write(target, {"z": 1, "a": True})

    assert target.read_bytes() == json.dumps(
        {"z": 1, "a": True}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert len(sources) == 4
    assert len(set(sources)) == 1
    assert sleeps == list(windows_runtime._ATOMIC_JSON_REPLACE_DELAYS_S[:3])
    assert _owned_temps(target) == []


def test_atomic_json_write_bounds_persistent_transient_replace_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "control.json"
    target.write_text("old", encoding="utf-8")
    sources: list[Path] = []
    sleeps: list[float] = []

    def always_denied(source: str | bytes | Path, _destination: object) -> None:
        sources.append(Path(source.decode() if isinstance(source, bytes) else source))
        raise _WindowsReplaceError(5)

    monkeypatch.setattr(windows_runtime.os, "replace", always_denied)
    monkeypatch.setattr(windows_runtime, "sleep", sleeps.append)

    with pytest.raises(PermissionError, match="WinError 5"):
        windows_runtime.atomic_json_write(target, {"new": True})

    assert len(sources) == len(windows_runtime._ATOMIC_JSON_REPLACE_DELAYS_S) + 1
    assert len(set(sources)) == 1
    assert sleeps == list(windows_runtime._ATOMIC_JSON_REPLACE_DELAYS_S)
    assert target.read_text(encoding="utf-8") == "old"
    assert _owned_temps(target) == []


@pytest.mark.parametrize("error", [_WindowsReplaceError(87), PermissionError("denied")])
def test_atomic_json_write_does_not_retry_other_replace_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error: OSError
) -> None:
    target = tmp_path / "evidence.json"
    target.write_text("old", encoding="utf-8")
    attempts = 0
    sleeps: list[float] = []

    def fail_once(_source: object, _destination: object) -> None:
        nonlocal attempts
        attempts += 1
        raise error

    monkeypatch.setattr(windows_runtime.os, "replace", fail_once)
    monkeypatch.setattr(windows_runtime, "sleep", sleeps.append)

    with pytest.raises(type(error)):
        windows_runtime.atomic_json_write(target, {"new": True})

    assert attempts == 1
    assert sleeps == []
    assert target.read_text(encoding="utf-8") == "old"
    assert _owned_temps(target) == []


def test_atomic_json_write_uses_a_distinct_owned_temp_for_each_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = tmp_path / "state.json"
    real_replace = windows_runtime.os.replace
    sources: list[Path] = []

    def capture_replace(source: str | bytes | Path, destination: str | bytes | Path) -> None:
        sources.append(Path(source.decode() if isinstance(source, bytes) else source))
        real_replace(source, destination)

    monkeypatch.setattr(windows_runtime.os, "replace", capture_replace)

    windows_runtime.atomic_json_write(target, {"seq": 1})
    windows_runtime.atomic_json_write(target, {"seq": 2})

    assert len(sources) == 2
    assert sources[0] != sources[1]
    assert json.loads(target.read_text(encoding="utf-8")) == {"seq": 2}
    assert _owned_temps(target) == []
