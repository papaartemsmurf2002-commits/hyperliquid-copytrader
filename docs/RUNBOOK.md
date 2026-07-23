# Runbook

## Before every run

1. Confirm no other copytrader/continuous process owns the follower or signer.
2. Inspect current exchange truth for follower positions and open orders.
3. Use the intended explicit network and plan file.
4. Use a stable engine directory and a new generation/run directory.
5. Start monitor-only. Never bypass a failed preflight by editing state or increasing a budget.

## Monitor-only

Use `scripts/run_continuous_fleet.py` without `--arm`. A healthy run has:

- `arm_requested=false` and `execution_enabled=false`;
- exactly three startup HTTP requests, weight 60;
- preflight transport `websocket_post` with no blockers;
- every slot `RUNNING` / `ready`;
- source frames and follower refreshes present;
- zero action-journal rows;
- zero `socket_reconnect`, `recovery_http`, overflow, backlog-discard, and metric-drop events.

Repeat once with the same engine directory. A changed plan SHA for an existing engine directory is rejected; use an explicit reviewed migration rather than overwriting identity.

## Arm through the UI

Run `hl-copytrader serve --repo-root . --continuous-plan <plan>`. The UI derives stable durable engine state from the plan runtime identity. Reserve `--continuous-engine-state-dir` for advanced recovery of that exact unchanged plan; do not pin it for an editable fleet plan. The UI is restricted to numeric loopback Host and peer, so the local-only surface does not require a login. Preview is read-only. Start only after checking plan SHA, public role mapping, market eligibility, caps, account modes, signer authorization, and current follower truth.

The UI supplies the exact arm and operator-rearm acknowledgement to the child. If the stable engine contains an interrupted fail-close, the runner first reaches fresh authoritative flatness, then clears that latch and resumes entries. The UI process itself remains credential-free.

## Stop

Continuous Stop requests graceful process shutdown. It does not automatically flatten positions. Automatic flatten-on-error was removed because it can turn uncertain state into a second wrong action.

After any abnormal stop:

1. Do not blind-restart or resubmit an uncertain order.
2. Read the per-slot action journal.
3. Resolve any `SEND_ATTEMPTED`, `UNKNOWN`, or `RESTING` CLOID against exchange truth.
4. Re-read positions and open orders across the catalog.
5. Resume only when the journal and exchange agree.

The provenance-bound proof recovery script is for its recorded proof incident only; it is not a generic flatten command.

## Promotion boundary

Two-slot monitor/restart and bounded action proofs permit bounded fleet testing. Full live promotion still requires one naturally occurring fresh leader fill through the complete runtime and a separate ten-slot monitor soak with real distinct profiles.
