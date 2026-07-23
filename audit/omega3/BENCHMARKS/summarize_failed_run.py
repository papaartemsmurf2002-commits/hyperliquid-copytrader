"""Derive compact percentile evidence from a preserved failed benchmark database."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _nearest_rank(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percentile * len(ordered)) - 1))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--failure-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="hlct-omega3-sqlite-") as temporary:
        with zipfile.ZipFile(args.archive) as archive:
            names = archive.namelist()
            if names != ["execution.sqlite3"]:
                raise RuntimeError(f"unexpected benchmark archive members: {names}")
            archive.extract(names[0], temporary)
        database = Path(temporary) / names[0]
        connection = sqlite3.connect(database)
        try:
            table_counts = {
                name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                for (name,) in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
                if name != "sqlite_sequence"
            }
            grouped: dict[tuple[str, str], list[float]] = defaultdict(list)
            for stage, excluded_reason, duration_ns in connection.execute(
                "SELECT stage, excluded_reason, duration_ns FROM stage_timings"
            ):
                grouped[(str(stage), str(excluded_reason or ""))].append(
                    int(duration_ns) / 1_000_000
                )
        finally:
            connection.close()

        distributions = {}
        for (stage, excluded_reason), values in sorted(grouped.items()):
            key = stage + (f" [{excluded_reason}]" if excluded_reason else " [direct]")
            distributions[key] = {
                "count": len(values),
                "p50_ms": _nearest_rank(values, 0.50),
                "p95_ms": _nearest_rank(values, 0.95),
                "p99_ms": _nearest_rank(values, 0.99),
                "max_ms": max(values),
            }

        payload = {
            "schema_version": 1,
            "scope": "partial failed full-workload run; native Windows local compute only",
            "release_gate_evidence": False,
            "network_and_exchange_excluded": True,
            "operator_credentials_read": False,
            "exchange_actions_sent": False,
            "workload_requested": {
                "full_fleet_slots": 10,
                "direct_cycles_per_slot": 500,
                "aggregation_cases_per_slot": 100,
                "raw_frames": 100000,
            },
            "terminal_failure": "aggregate scheduler release bound exceeded",
            "scheduler_bound_ms": 100,
            "percentile_method": "nearest_rank",
            "archive_sha256": _sha256(args.archive),
            "failure_log_sha256": _sha256(args.failure_log),
            "database_sha256": _sha256(database),
            "table_counts_at_failure": table_counts,
            "stage_distributions_at_failure": distributions,
        }
        args.output.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
