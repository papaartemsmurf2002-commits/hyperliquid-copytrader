from __future__ import annotations

from io import BytesIO
from pathlib import Path
import tarfile
import zipfile

import pytest

from scripts.verify_release_artifacts import verify_distributions


TOP_LEVEL = (".env.example", "LICENSE", "MANIFEST.in", "README.md", "pyproject.toml")


def _repository(root: Path) -> dict[str, bytes]:
    files = {
        ".env.example": b"MODE=test\n",
        "LICENSE": b"MIT\n",
        "MANIFEST.in": b"include LICENSE\n",
        "README.md": b"# example\n",
        "pyproject.toml": b"[project]\nname='example'\n",
        "docs/guide.md": b"guide\n",
        "requirements/ci-lock.txt": b"",
        "scripts/run.py": b"print('run')\n",
        "tests/test_sample.py": b"def test_sample(): pass\n",
        "src/hyperliquid_copytrader/__init__.py": b"VALUE = 1\n",
    }
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return files


def _write_sdist(path: Path, files: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for name, content in sorted(files.items()):
            info = tarfile.TarInfo(f"example-1.0/{name}")
            info.size = len(content)
            archive.addfile(info, BytesIO(content))
        for name, content in {
            "PKG-INFO": b"Metadata-Version: 2.4\n",
            "src/example.egg-info/SOURCES.txt": b"generated\n",
        }.items():
            info = tarfile.TarInfo(f"example-1.0/{name}")
            info.size = len(content)
            archive.addfile(info, BytesIO(content))


def _write_wheel(
    path: Path,
    files: dict[str, bytes],
    *,
    extra_runtime: tuple[str, bytes] | None = None,
) -> None:
    runtime = {
        name.removeprefix("src/"): content
        for name, content in files.items()
        if name.startswith("src/hyperliquid_copytrader/")
    }
    if extra_runtime is not None:
        runtime[extra_runtime[0]] = extra_runtime[1]
    metadata = (
        b"Metadata-Version: 2.4\n"
        b"Name: example\n"
        b"Version: 1.0\n"
        b"License-Expression: MIT\n"
        b"License-File: LICENSE\n\n"
    )
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in runtime.items():
            archive.writestr(name, content)
        archive.writestr("example-1.0.dist-info/METADATA", metadata)
        archive.writestr("example-1.0.dist-info/licenses/LICENSE", files["LICENSE"])


def _artifact_pair(tmp_path: Path) -> tuple[Path, Path, Path, dict[str, bytes]]:
    repository = tmp_path / "repo"
    dist = tmp_path / "dist"
    dist.mkdir()
    files = _repository(repository)
    sdist = dist / "example-1.0.tar.gz"
    wheel = dist / "example-1.0-py3-none-any.whl"
    _write_sdist(sdist, files)
    _write_wheel(wheel, files)
    return repository, sdist, wheel, files


def test_release_verifier_accepts_exact_repository_bytes(tmp_path: Path) -> None:
    repository, _sdist, _wheel, _files = _artifact_pair(tmp_path)

    sdist_count, wheel_count = verify_distributions(tmp_path / "dist", repository)

    assert sdist_count > len(TOP_LEVEL)
    assert wheel_count == 3


@pytest.mark.parametrize("artifact", ["sdist", "wheel"])
def test_release_verifier_rejects_same_name_with_stale_bytes(tmp_path: Path, artifact: str) -> None:
    repository, sdist, wheel, files = _artifact_pair(tmp_path)
    stale = dict(files)
    stale["src/hyperliquid_copytrader/__init__.py"] = b"VALUE = 0\n"
    if artifact == "sdist":
        _write_sdist(sdist, stale)
    else:
        _write_wheel(wheel, stale)

    with pytest.raises(ValueError, match="stale or mismatched"):
        verify_distributions(tmp_path / "dist", repository)


def test_release_verifier_rejects_missing_sdist_source(tmp_path: Path) -> None:
    repository, sdist, _wheel, files = _artifact_pair(tmp_path)
    missing = dict(files)
    missing.pop("src/hyperliquid_copytrader/__init__.py")
    _write_sdist(sdist, missing)

    with pytest.raises(ValueError, match="missing source files"):
        verify_distributions(tmp_path / "dist", repository)


def test_release_verifier_rejects_unexpected_stale_wheel_module(tmp_path: Path) -> None:
    repository, _sdist, wheel, files = _artifact_pair(tmp_path)
    _write_wheel(
        wheel,
        files,
        extra_runtime=("hyperliquid_copytrader/deleted.py", b"STALE = True\n"),
    )

    with pytest.raises(ValueError, match="unexpected runtime files"):
        verify_distributions(tmp_path / "dist", repository)
