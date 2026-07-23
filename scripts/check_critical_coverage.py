from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


# These are per-file line/branch floors, not a substitute for the repository-wide
# threshold. They prevent broad legacy coverage from hiding an untested live
# stream, action journal, execution, reconciliation, or runtime implementation.
CRITICAL_COVERAGE_FLOORS: dict[str, tuple[float, float]] = {
    "src/hyperliquid_copytrader/account_stream.py": (70.0, 50.0),
    "src/hyperliquid_copytrader/action_journal.py": (70.0, 50.0),
    "src/hyperliquid_copytrader/continuous_executor.py": (60.0, 40.0),
    "src/hyperliquid_copytrader/continuous_network.py": (50.0, 30.0),
    "src/hyperliquid_copytrader/continuous_preflight.py": (60.0, 40.0),
    "src/hyperliquid_copytrader/continuous_runtime.py": (60.0, 40.0),
    "src/hyperliquid_copytrader/fleet_config.py": (75.0, 55.0),
    "src/hyperliquid_copytrader/market_catalog.py": (70.0, 50.0),
    "src/hyperliquid_copytrader/rest_budget.py": (60.0, 45.0),
    "src/hyperliquid_copytrader/ws_actions.py": (60.0, 40.0),
}


def _normalized_files(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    files = payload.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("coverage JSON has no files mapping")
    normalized: dict[str, Mapping[str, Any]] = {}
    for raw_path, value in files.items():
        if isinstance(raw_path, str) and isinstance(value, Mapping):
            normalized[raw_path.replace("\\", "/")] = value
    return normalized


def _percentage(*, covered: object, total: object, label: str) -> float:
    if not isinstance(covered, int) or not isinstance(total, int) or total <= 0:
        raise ValueError(f"{label} counts are missing or invalid")
    if covered < 0 or covered > total:
        raise ValueError(f"{label} counts are inconsistent")
    return covered * 100.0 / total


def critical_coverage_failures(
    payload: Mapping[str, Any],
    *,
    floors: Mapping[str, tuple[float, float]] = CRITICAL_COVERAGE_FLOORS,
) -> list[str]:
    meta = payload.get("meta")
    if not isinstance(meta, Mapping) or meta.get("branch_coverage") is not True:
        return ["coverage JSON was not produced with branch coverage enabled"]
    try:
        files = _normalized_files(payload)
    except ValueError as exc:
        return [str(exc)]
    failures: list[str] = []
    for path, (line_floor, branch_floor) in sorted(floors.items()):
        entry = files.get(path.replace("\\", "/"))
        if entry is None:
            failures.append(f"{path}: missing from coverage JSON")
            continue
        summary = entry.get("summary")
        if not isinstance(summary, Mapping):
            failures.append(f"{path}: missing coverage summary")
            continue
        try:
            line_percent = _percentage(
                covered=summary.get("covered_lines"),
                total=summary.get("num_statements"),
                label="line coverage",
            )
            branch_percent = _percentage(
                covered=summary.get("covered_branches"),
                total=summary.get("num_branches"),
                label="branch coverage",
            )
        except ValueError as exc:
            failures.append(f"{path}: {exc}")
            continue
        if line_percent + 1e-9 < line_floor:
            failures.append(f"{path}: line coverage {line_percent:.2f}% is below {line_floor:.2f}%")
        if branch_percent + 1e-9 < branch_floor:
            failures.append(
                f"{path}: branch coverage {branch_percent:.2f}% is below {branch_floor:.2f}%"
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Enforce per-file line and branch coverage on fleet-critical modules."
    )
    parser.add_argument("coverage_json", nargs="?", default="coverage.json")
    args = parser.parse_args()
    path = Path(args.coverage_json)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"critical coverage check failed: cannot read {path}: {exc}")
        return 2
    if not isinstance(payload, Mapping):
        print("critical coverage check failed: coverage JSON root is not an object")
        return 2
    failures = critical_coverage_failures(payload)
    if failures:
        print("critical coverage check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print(f"critical coverage check passed for {len(CRITICAL_COVERAGE_FLOORS)} modules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
