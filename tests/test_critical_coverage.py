from __future__ import annotations

from typing import Any

from scripts.check_critical_coverage import critical_coverage_failures


FLOORS = {"src/hyperliquid_copytrader/critical.py": (80.0, 60.0)}


def _payload(*, path: str = "src/hyperliquid_copytrader/critical.py") -> dict[str, Any]:
    return {
        "meta": {"branch_coverage": True},
        "files": {
            path: {
                "summary": {
                    "covered_lines": 8,
                    "num_statements": 10,
                    "covered_branches": 3,
                    "num_branches": 5,
                }
            }
        },
    }


def test_critical_coverage_accepts_exact_line_and_branch_floors() -> None:
    assert critical_coverage_failures(_payload(), floors=FLOORS) == []


def test_critical_coverage_normalizes_windows_paths() -> None:
    payload = _payload(path=r"src\hyperliquid_copytrader\critical.py")
    assert critical_coverage_failures(payload, floors=FLOORS) == []


def test_critical_coverage_rejects_missing_file() -> None:
    failures = critical_coverage_failures(
        {"meta": {"branch_coverage": True}, "files": {}},
        floors=FLOORS,
    )
    assert failures == ["src/hyperliquid_copytrader/critical.py: missing from coverage JSON"]


def test_critical_coverage_rejects_below_floor() -> None:
    payload = _payload()
    summary = payload["files"]["src/hyperliquid_copytrader/critical.py"]["summary"]
    summary["covered_lines"] = 7
    summary["covered_branches"] = 2

    failures = critical_coverage_failures(payload, floors=FLOORS)

    assert "line coverage 70.00% is below 80.00%" in failures[0]
    assert "branch coverage 40.00% is below 60.00%" in failures[1]


def test_critical_coverage_requires_branch_measurement() -> None:
    payload = _payload()
    payload["meta"]["branch_coverage"] = False

    assert critical_coverage_failures(payload, floors=FLOORS) == [
        "coverage JSON was not produced with branch coverage enabled"
    ]


def test_critical_coverage_rejects_invalid_counts() -> None:
    payload = _payload()
    payload["files"]["src/hyperliquid_copytrader/critical.py"]["summary"]["num_branches"] = 0

    assert critical_coverage_failures(payload, floors=FLOORS) == [
        "src/hyperliquid_copytrader/critical.py: branch coverage counts are missing or invalid"
    ]
