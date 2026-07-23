# Native-Windows performance evidence

Status: historical negative evidence for the retired FleetRuntime path. The referenced runner imports modules removed by the continuous-runtime rebuild and is not a current or runnable gate.

## Scope

The retired audit runner invoked `run_production_path_benchmark` with the frozen production minimums:

- 10 slots;
- 500 direct cycles per slot;
- 100 aggregation cases per slot;
- 100,000 raw frames;
- real source/market queues, SQLite journal, slot actors, last-mile gate, deterministic signing preparation, deadline scheduler, and loopback action transport.

It uses `%LOCALAPPDATA%` on NTFS because the repository itself is under OneDrive. Operator credentials are not read. Network, exchange, and real QoS/clock observation are replaced with explicit fixtures, so this is local compute evidence only and is not a release-gate benchmark.

## Results

Both attempts failed at the unchanged `scheduler_bound_ms=100` invariant:

- attempt 1: same `aggregate scheduler release bound exceeded` terminal error after 93.6 seconds (console evidence; runner had not yet gained automatic failure capture);
- attempt 2: same error after 60,476.499 ms; raw trace and partial SQLite preserved.

At the second failure the detailed database contained 4,819 runtime events, 4,811 desired states/reaction results, 872 action states, 4,356 action transitions, and 30,148 stage timings. It had not completed the requested workload, so these distributions describe the partial run only.

Selected nearest-rank distributions from the preserved partial run:

| Stage | n | p50 ms | p95 ms | p99 ms | max ms |
|---|---:|---:|---:|---:|---:|
| local receive -> durable ingress commit, direct | 3,919 | 19.837 | 34.342 | 42.517 | 76.177 |
| local receive -> desired state, direct | 4,791 | 27.032 | 45.887 | 55.366 | 100.024 |
| local receive -> applied cursor commit, direct | 4,791 | 35.965 | 60.585 | 70.157 | 109.383 |
| local receive -> signing start, direct | 434 | 78.266 | 101.894 | 107.220 | 135.957 |
| local receive -> loopback signed transport start, direct | 434 | 103.731 | 128.761 | 136.234 | 154.729 |
| loopback transport start -> send end, direct | 434 | 0.007 | 0.009 | 0.012 | 0.180 |
| loopback transport start -> committed response, direct | 434 | 37.159 | 51.574 | 56.386 | 64.550 |

Interpretation: the transport boundary itself is negligible; local queueing, desired-state work, signing, and durable response commits dominate. The run fails before it can produce a valid complete-result payload. There is no honest before/after comparison because no reproducible pre-edit benchmark artifact existed.

## Raw and reproducibility artifacts

- `run_offline_production_path.py`: exact runner.
- `offline-context-fixture.json`: declared synthetic context/QoS fixture.
- `native-windows-offline-production-path.failure-20260717T014858Z.txt`: raw second failure.
- `native-windows-offline-production-path.failed-run.sqlite3.zip`: compressed raw SQLite at failure; archive SHA-256 `04e4a98411cbbd95a69122a8b354ca69e8127a13fc4b48fcd2604e7ec11990bd`; inner database SHA-256 `38a8684f0e4ed584d77d104661e961630d3232fc64f082a0dfebd15db1b6b130`.
- `summarize_failed_run.py`: deterministic nearest-rank extractor.
- `native-windows-offline-production-path.failed-run.summary.json`: all stage distributions and table counts.

## Platform verdict

Native Windows remains the required platform and no evidence justifies Linux, WSL, a VM, extra egress, or a local node. This archived run shows that the retired FleetRuntime build failed its representative local-compute gate; it does not describe or block the current continuous runtime. Current promotion boundaries are recorded in [READINESS.md](../READINESS.md) and [FINAL_REPORT.md](../FINAL_REPORT.md).
