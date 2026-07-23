"""Run the production-path compute benchmark without network or operator credentials.

This is audit evidence, not a release-gate benchmark. It uses the production actors,
journal, queues, deterministic signers, and loopback action transport, while replacing
the prelaunch network/QoS observation with a declared fixture. The result therefore
measures local native-Windows compute and queue behavior only.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
import traceback
from hashlib import sha256
from pathlib import Path
from time import monotonic_ns, time_ns

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "src"))

from hyperliquid_copytrader import performance_benchmark  # noqa: E402
from hyperliquid_copytrader.fleet_config import FleetPlan  # noqa: E402
from hyperliquid_copytrader.performance_benchmark import (  # noqa: E402
    run_production_path_benchmark,
)
from hyperliquid_copytrader.windows_runtime import ClockSample  # noqa: E402
from tests.test_performance_benchmark import _context_measurement, _slot  # noqa: E402


class _OfflineInfoGrant:
    def grant(self, **_: object) -> None:
        return None


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def _main() -> None:
    artifact_dir = Path(__file__).resolve().parent
    repo_root = artifact_dir.parents[2]
    local_root = Path(os.environ.get("LOCALAPPDATA") or tempfile.gettempdir())
    runtime_root = local_root / (
        "hlct-omega3-offline-benchmark-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    )
    runtime_root.mkdir(parents=True, exist_ok=False)

    plan = FleetPlan(
        version=3,
        environment="mainnet",
        purpose="full_fleet_12h",
        policy_version="fleet-fast-execution-v1",
        intended_fleet_complete=True,
        slots=tuple(_slot(index, index % 2) for index in range(1, 11)),
        sha256="a" * 64,
        path=artifact_dir / "synthetic-full-fleet-plan.json",
    )
    context_path = artifact_dir / "offline-context-fixture.json"
    context_payload = _context_measurement(plan)
    context_path.write_text(
        json.dumps(context_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    performance_benchmark.durable_prelaunch_info_client = lambda **_: _OfflineInfoGrant()
    performance_benchmark.measure_network_profile = lambda: {
        "active_qos_policy_present": True,
        "active_qos_policy_sha256": "b" * 64,
        "frozen_throttled_byte_rate_bps": 1_000,
        "audit_fixture": True,
    }

    output_path = artifact_dir / "native-windows-offline-production-path.json"
    started_mono_ns = monotonic_ns()
    try:
        result = await run_production_path_benchmark(
            repo_root=repo_root,
            runtime_root=runtime_root,
            output_path=output_path,
            scope="fleet",
            launch_plan=plan,
            provisioning={
                "passed": True,
                "blockers": [],
                "scope": "fleet",
                "selected_plan_sha256": plan.sha256,
                "audit_fixture": True,
            },
            context_measurement_path=context_path,
            clock_sample=ClockSample(
                sampled_wall_ms=time_ns() // 1_000_000,
                sampled_mono_ns=monotonic_ns(),
                source="omega3-offline-zero-offset-fixture",
                offsets_ms=(0.0,),
                max_abs_offset_ms=0.0,
                w32time_status="fixture-not-measured",
                raw_sha256="0" * 64,
            ),
        )
    except Exception:
        failed_at = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        failure_path = artifact_dir / (
            "native-windows-offline-production-path.failure-" + failed_at + ".txt"
        )
        failure_path.write_text(
            "scope=native Windows local compute only; network and exchange excluded\n"
            "release_gate_evidence=false\n"
            "operator_credentials_read=false\n"
            "exchange_actions_sent=false\n"
            f"runtime_root={runtime_root}\n"
            f"elapsed_ms={(monotonic_ns() - started_mono_ns) / 1_000_000:.3f}\n\n"
            + traceback.format_exc(),
            encoding="utf-8",
        )
        raise

    detailed_source = Path(str(result["detailed_evidence"]["execution_sqlite"]))
    detailed_copy = artifact_dir / "native-windows-offline-production-path.sqlite3"
    shutil.copy2(detailed_source, detailed_copy)
    copied_sha256 = _file_sha256(detailed_copy)
    if copied_sha256 != result["detailed_evidence"]["execution_sqlite_sha256"]:
        raise RuntimeError("copied detailed benchmark database hash mismatch")

    metadata = {
        "schema_version": 1,
        "scope": "native Windows local compute only; network and exchange excluded",
        "release_gate_evidence": False,
        "operator_credentials_read": False,
        "exchange_actions_sent": False,
        "runtime_root": str(runtime_root),
        "result": output_path.name,
        "result_sha256": _file_sha256(output_path),
        "detailed_sqlite": detailed_copy.name,
        "detailed_sqlite_sha256": copied_sha256,
        "context_fixture": context_path.name,
        "context_fixture_sha256": _file_sha256(context_path),
        "passed_internal_compute_gates": result.get("passed") is True,
    }
    metadata_path = artifact_dir / "native-windows-offline-production-path.metadata.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(metadata, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(_main())
