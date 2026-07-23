# Hyperliquid Copytrader

Native-Windows, local-only Hyperliquid copytrader with a browser UI, address analytics, and a continuous WebSocket-first execution engine for up to ten leader/follower pairs.

## Current verdict

- Ready for monitor/shadow fleet testing and bounded two-account launch testing.
- The two follower API-wallet lanes have completed four bounded mainnet WebSocket IOC actions and ended flat/order-free.
- The real two-account continuous runner completed two read-only mainnet soaks, including a restart with the same durable engine state.
- A fresh natural leader fill has not yet traversed the complete leader-event-to-follower-order path. Full ten-slot live readiness and profitability are therefore not claimed.

The prior FleetRuntime/supervisor/guardian stack and its synthetic launch gates were removed. The only browser launch surface is the continuous runtime.

## Active architecture

| Work | Normal transport |
|---|---|
| Ten leader user streams | One WebSocket source connection |
| Public market context and books | One WebSocket market connection |
| Follower orders and order status | One action-priority WebSocket POST connection |
| Follower positions/open orders | WebSocket POST reconciliation |
| Catalog bootstrap | Exactly three HTTP `/info` calls at startup |
| Gap recovery | WebSocket POST first; optional HTTP fallback only when explicitly enabled |
| Historical analytics | Separate read-only HTTP/archive work, outside execution |

HTTP `/exchange` is not used by the continuous engine. Consumer VPNs, multi-IP routing, WSL, a VM, and a local Hyperliquid node are not part of the production design.

The execution core is in `continuous_runtime.py`, `continuous_network.py`, `account_stream.py`, `market_stream.py`, `action_journal.py`, `ws_actions.py`, and `continuous_executor.py`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Install

```powershell
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Runtime state must be on local NTFS under `%LOCALAPPDATA%`, not in this OneDrive checkout. Private keys stay in ignored profile files under `.secrets` and are opened only by an explicitly armed child process.

## Monitor-only run

```powershell
$repo = (Resolve-Path .).Path
$plan = (Resolve-Path .\.secrets\mainnet-two-account-continuous-proof.json).Path
$root = Join-Path $env:LOCALAPPDATA "HyperliquidCopytrader\runtime\manual-monitor"
$run = Join-Path $root ("run-" + (Get-Date -Format "yyyyMMddTHHmmss"))
$engine = Join-Path $root "engine"

python .\scripts\run_continuous_fleet.py `
  --repo-root $repo `
  --plan $plan `
  --state-dir $run `
  --engine-state-dir $engine `
  --duration 120
```

With no `--arm`, the runner does not open private keys and cannot submit an order. Do not add `--enable-rest-recovery-fallback` during a normal soak.

## Browser control

Expose one explicit continuous plan on the loopback-only browser UI:

```powershell
hl-copytrader serve --repo-root . `
  --continuous-plan .\.secrets\mainnet-ten-account-continuous.json
```

The UI derives a stable durable engine directory from the plan's runtime identity.
Do not pin `--continuous-engine-state-dir` for an editable fleet plan; an offline
plan update intentionally receives a new identity and therefore a new journal.

Open `http://127.0.0.1:8080/` for the full-fleet controller and `http://127.0.0.1:8080/analytics` for the restored analytics, operations, setup, and evidence dashboard.

The browser is bound to numeric loopback and accepts only a local peer. No login is required on this local-only surface. Preview is read-only. Arming requires the exact acknowledgement shown by the continuous controller. See [docs/RUNBOOK.md](docs/RUNBOOK.md).

## Evidence

- [Final audit](audit/omega3/FINAL_REPORT.md)
- [External source comparison](audit/omega3/EXTERNAL_BASELINE.md)
- [Two-account mainnet WebSocket proof](audit/omega3/live/TWO_ACCOUNT_MAINNET_WS_PROOF.md)
- [Two-account monitor/restart soak](audit/omega3/live/TWO_ACCOUNT_MONITOR_SOAK.md)
- [Readiness boundary](audit/omega3/READINESS.md)

No test count, benchmark, or document is treated as readiness by itself. Exchange truth, durable journals, real runtime artifacts, and explicit proof boundaries take priority.
