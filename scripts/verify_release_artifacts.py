"""Verify the intentional contents and exact bytes of built distributions."""

from __future__ import annotations

import argparse
from email.parser import BytesParser
from email.policy import default
from pathlib import Path, PurePosixPath
import tarfile
import zipfile


CONTROLLED_SDIST_ROOTS = (
    "docs/",
    "requirements/",
    "scripts/",
    "tests/",
    "src/hyperliquid_copytrader/",
)


def _repository_files(root: Path, relative_dir: str) -> dict[str, bytes]:
    base = root / relative_dir
    if not base.is_dir():
        return {}
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in base.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    }


def _sdist_entries(path: Path) -> dict[str, bytes]:
    with tarfile.open(path, "r:gz") as archive:
        members = [member for member in archive.getmembers() if member.isfile()]
        roots = {member.name.split("/", 1)[0] for member in members if "/" in member.name}
        if len(roots) != 1:
            raise ValueError(f"{path.name}: expected one source-distribution root, found {roots}")
        prefix = f"{next(iter(roots))}/"
        entries: dict[str, bytes] = {}
        for member in members:
            if not member.name.startswith(prefix):
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"{path.name}: could not read {member.name}")
            entries[member.name.removeprefix(prefix)] = extracted.read()
        return entries


def _wheel_entries(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist() if not name.endswith("/")}


def _has_bytecode(entries: dict[str, bytes]) -> bool:
    return any(
        "__pycache__" in PurePosixPath(name).parts or name.endswith((".pyc", ".pyo"))
        for name in entries
    )


def _require_exact_bytes(
    *, artifact_name: str, actual: dict[str, bytes], expected: dict[str, bytes], label: str
) -> None:
    missing = sorted(set(expected) - set(actual))
    if missing:
        raise ValueError(f"{artifact_name}: missing {label} files: {missing}")
    stale = sorted(name for name, content in expected.items() if actual[name] != content)
    if stale:
        raise ValueError(f"{artifact_name}: stale or mismatched {label} bytes: {stale}")


def verify_distributions(dist_dir: Path, repository_root: Path) -> tuple[int, int]:
    sdists = sorted(dist_dir.glob("*.tar.gz"))
    wheels = sorted(dist_dir.glob("*.whl"))
    if len(sdists) != 1 or len(wheels) != 1:
        raise ValueError(
            f"expected exactly one sdist and one wheel in {dist_dir}; "
            f"found {len(sdists)} sdist(s) and {len(wheels)} wheel(s)"
        )

    expected_sdist: dict[str, bytes] = {}
    for filename in (".env.example", "LICENSE", "MANIFEST.in", "README.md", "pyproject.toml"):
        path = repository_root / filename
        if not path.is_file():
            raise ValueError(f"repository is missing required release file: {filename}")
        expected_sdist[filename] = path.read_bytes()
    for relative_dir in (
        "docs",
        "requirements",
        "scripts",
        "tests",
        "src/hyperliquid_copytrader",
    ):
        expected_sdist.update(_repository_files(repository_root, relative_dir))

    sdist_entries = _sdist_entries(sdists[0])
    _require_exact_bytes(
        artifact_name=sdists[0].name,
        actual=sdist_entries,
        expected=expected_sdist,
        label="source",
    )
    unexpected_controlled = sorted(
        name
        for name in set(sdist_entries) - set(expected_sdist)
        if name.startswith(CONTROLLED_SDIST_ROOTS)
    )
    if unexpected_controlled:
        raise ValueError(
            f"{sdists[0].name}: contains unexpected repository-controlled files: "
            f"{unexpected_controlled}"
        )
    if _has_bytecode(sdist_entries):
        raise ValueError(f"{sdists[0].name}: contains cache or bytecode files")

    wheel_entries = _wheel_entries(wheels[0])
    expected_runtime = {
        name.removeprefix("src/"): content
        for name, content in _repository_files(
            repository_root, "src/hyperliquid_copytrader"
        ).items()
    }
    _require_exact_bytes(
        artifact_name=wheels[0].name,
        actual=wheel_entries,
        expected=expected_runtime,
        label="runtime",
    )
    actual_runtime = {name for name in wheel_entries if name.startswith("hyperliquid_copytrader/")}
    unexpected_runtime = sorted(actual_runtime - set(expected_runtime))
    if unexpected_runtime:
        raise ValueError(
            f"{wheels[0].name}: contains unexpected runtime files: {unexpected_runtime}"
        )
    if any(name.startswith(("docs/", "scripts/")) for name in wheel_entries):
        raise ValueError(f"{wheels[0].name}: contains repository-level docs or scripts")
    if _has_bytecode(wheel_entries):
        raise ValueError(f"{wheels[0].name}: contains cache or bytecode files")

    license_entries = [
        name for name in wheel_entries if name.endswith(".dist-info/licenses/LICENSE")
    ]
    metadata_entries = [name for name in wheel_entries if name.endswith(".dist-info/METADATA")]
    if len(license_entries) != 1 or len(metadata_entries) != 1:
        raise ValueError(f"{wheels[0].name}: expected one packaged license and one metadata file")
    metadata = BytesParser(policy=default).parsebytes(wheel_entries[metadata_entries[0]])
    if metadata.get("License-Expression") != "MIT":
        raise ValueError(f"{wheels[0].name}: metadata does not declare MIT")
    if "LICENSE" not in metadata.get_all("License-File", []):
        raise ValueError(f"{wheels[0].name}: metadata does not reference LICENSE")

    return len(sdist_entries), len(wheel_entries)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist_dir", nargs="?", default="dist", help="built artifact directory")
    args = parser.parse_args()
    repository_root = Path(__file__).resolve().parents[1]
    dist_dir = Path(str(args.dist_dir)).resolve()
    sdist_count, wheel_count = verify_distributions(dist_dir, repository_root)
    print(
        "release artifacts verified: "
        f"sdist_files={sdist_count} wheel_files={wheel_count} exact_bytes=true license=MIT"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
