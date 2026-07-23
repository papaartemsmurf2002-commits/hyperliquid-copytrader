# Deployment

## Supported production host

- Native Windows on the operator's local PC.
- Repository may remain in OneDrive.
- Runtime databases, locks, metrics, and status must live on local NTFS under `%LOCALAPPDATA%\HyperliquidCopytrader\runtime`.
- One process owns each follower account and API-wallet signer globally.

WSL, Linux, a VM, containers, a local node, VPN routing, and extra egress are not justified by current evidence and are not supported production requirements.

## Install

```powershell
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Credential profiles contain public role metadata plus one absolute private-key file path. Use a distinct API wallet for every concurrently operated follower. Never use a follower/master private key as the normal signer.

## Plan

Start from [examples/continuous_plan.example.json](examples/continuous_plan.example.json). Replace every placeholder with the exact leader, follower, credential profile, and cap intended for that slot. Market eligibility comes from each credential profile (`all_active_markets` plus its explicit denylist), so a continuously running fleet adopts compatible new markets without editing or restarting the plan. The global catalog refresh runs every five minutes and averages 12 public-info REST weight per minute. A fresh leader fill for an unknown perp requests an immediate refresh, coalesced to at most once per minute; the fill remains non-executable until identity, collateral, precision, source equity, follower truth, and live book data validate. All leader, follower, and API-wallet identities must be disjoint where their roles require it.

## First launch

1. Run monitor-only for at least one full follower refresh.
2. Confirm both slot states are `RUNNING` and `ready`.
3. Confirm `startup_http_requests=3`, `startup_http_weight=60`, `normal_rest_enabled=false`, `recovery_rest_enabled=false`, and all drop counters are zero.
4. Stop and repeat using the same engine directory to prove restart reuse.
5. Only then expose the same plan through the loopback UI for an explicitly acknowledged bounded launch.

Do not arm a new ten-slot plan merely because a two-slot plan passed.
