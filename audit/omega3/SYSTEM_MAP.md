# Active system map

## Entry points

- Browser: `hl-copytrader serve --continuous-plan <plan>` creates only `ContinuousLaunchController`.
- Headless: `scripts/run_continuous_fleet.py` calls `run_continuous_fleet`.
- Monitor is the default. Only the exact arm acknowledgement enables signing.

## Startup

1. Load and validate one explicit plan.
2. Read public credential-profile metadata; key contents are checked only when armed.
3. Perform exactly three HTTP catalog reads.
4. Open one temporary WebSocket POST preflight connection and verify source/follower roles, account modes, signer authorization, positions, open orders, and collateral.
5. Create the durable engine identity only after validation succeeds.
6. Open one action journal per signer, one runtime state database, one source-gap database, and global account/signer locks.
7. Start source, market, and action/info WebSocket sessions.

## Live flow

```text
leader WS frame
  -> per-slot queue
  -> AccountStream validation/dedup/continuity
  -> durable source attribution
  -> desired proportional follower target
  -> caps, pending exposure, freshness, market depth
  -> durable CLOID/nonce/signed payload/send boundary
  -> action-priority WebSocket POST IOC
  -> response or UNKNOWN
  -> follower WebSocket POST reconciliation
  -> durable runtime/journal truth
```

Follower reconciliation performs network I/O outside the slot lock. A revision check discards a stale refresh if an action changed truth while the refresh was in flight.

## Transport ownership

- Source socket: one to ten unique leader users.
- Market socket: public context and L2 for configured markets.
- Action/info socket: actions, order status, positions, open orders, and gap repair.
- HTTP: three startup catalog calls; optional explicit recovery fallback only.

## Failure behavior

- Unknown action outcome: block conflicting work and resolve by CLOID.
- Stale/gap-recovered source increase: observation only, never a fresh copy signal.
- Stale source reduction: consumes previously attributable copied exposure.
- Unexplained follower exposure: block new risk.
- Socket flapping: all three sockets share a 24-attempt/minute admission window.
- Shutdown: stop the process; never guess that flattening is safe.

## Removed path

FleetRuntime, FleetSupervisor, FleetAuthority, guardian, attestor, launch-control, REST coordinator, execution gateway, deferred delta framework, and synthetic benchmark/context gates were deleted. There is no active compatibility route to them.
