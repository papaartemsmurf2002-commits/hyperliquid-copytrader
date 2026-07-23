# Validation

## Static and focused validation

- Full test collection: clean after legacy deletion.
- Continuous + CLI + GUI focused set: 216 passed.
- Explicit continuous engine/journal/WS set: 120 passed.
- Continuous keyword set: 103 passed.
- Preserved catalog/REST/reconciliation/signing set: 72 passed.
- Retained mainnet-canary analytics/readiness tests: 28 passed.
- Ruff lint: clean.
- Ruff format: applied to `src`, `scripts`, and `tests`.
- Mypy source/scripts: clean.
- Mypy tests: clean after six test-only annotation corrections.
- Source/scripts/tests compile: clean.
- Final strict coverage run: 1,370 passed in 77.21 seconds.
- Aggregate branch-aware coverage: 75%; the current ten-module continuous critical set passed its per-file floors.
- Dependency check: consistent; pinned lock audit found no known vulnerabilities.
- Wheel and sdist build: passed.
- Twine metadata check: passed for both artifacts.
- Release-content verifier: passed (`sdist_files=178`, `wheel_files=65`, exact bytes, MIT license).
- Installed-wheel import: passed for `ContinuousRuntime` and `run_continuous_fleet`; installed CLI help exposes none of the deleted legacy action commands.

The first broad run exposed nine closure failures: six stale tests still required deleted handoff/canary artifacts, one contradicted the intentional removal of credential-path export, and three ran in a project venv missing the pinned `coincurve==19.0.1`. Obsolete assertions were removed instead of restoring dead architecture, the CLI marker now points to `docs/ARCHITECTURE.md`, and the declared dependency was installed. A final console smoke then exposed five tests protecting removed polling/follow/smoke commands; those commands and tests were deleted. The final strict run passed all 1,370 collected tests.

## Real mainnet monitor runs

Run 1: `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-ui\monitor-soak-20260718T225606`

- preflight passed; two slots; 20 WS info posts;
- three HTTP catalog calls, weight 60;
- 56 source frames; two follower refreshes;
- max receive-to-reduce 4.7194 ms;
- zero action/reconnect/recovery-HTTP/drop events.

Run 2: `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-ui\monitor-soak-restart-20260718T225933`

- reused the same durable engine directory;
- both `acc1` and `acc7` ended `RUNNING / ready` before graceful stop;
- 20 WS info posts; three HTTP catalog calls/weight 60;
- 52 source frames; two follower refreshes;
- max receive-to-reduce 4.6668 ms;
- zero actions/reconnects/recovery HTTP/backlog discard/metric drops.

Both per-slot monitor journals contain zero action rows.

## Bounded mainnet actions

Artifact: `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-v1\proof-20260718T190008705Z`

- Four WS IOC actions: four `FILLED`.
- HTTP/REST actions: zero.
- Unresolved journal rows: zero.
- Final independent exchange truth: both followers flat and order-free.
- One entry/close freshness defect was exposed, stopped without blind retry, then recovered by an exact provenance-bound reduce-only action.

## Commands for final broad validation

```powershell
python -m pytest --strict-config --strict-markers
python -m ruff check .
python -m ruff format --check src scripts tests
python -m mypy --check-untyped-defs src scripts
python -m mypy --check-untyped-defs tests
python -m compileall -q src scripts tests
python -m build --no-isolation
python -m twine check dist\*
python scripts\verify_release_artifacts.py dist
```

Green results prove integration consistency only. They do not replace the missing natural leader-event or ten-slot experiment.
