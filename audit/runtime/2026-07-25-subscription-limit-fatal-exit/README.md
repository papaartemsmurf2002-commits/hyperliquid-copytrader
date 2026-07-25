# Continuous-run audit: subscription-limit fatal exit

This directory preserves the latest authoritative continuous-run evidence available on 2026-07-25. It records a real production failure and its separately authorized recovery. It is not a production-readiness certificate.

The detailed, code-linked implementation sequence is in
[`IMPLEMENTATION_HANDOFF.md`](IMPLEMENTATION_HANDOFF.md).

The two raw `metrics.jsonl` streams are committed as lossless gzip-compressed tar archives. Compression is required because the failed-run stream is 137 MB uncompressed, above GitHub's normal 100 MB per-file limit. Mutable SQLite databases and account configuration remain local. `evidence-manifest.json` records the sizes and SHA-256 hashes of both the original streams and their archives.

## Raw metrics

- `failed-run-metrics.jsonl.tar.gz`
  - Original bytes: 137,776,790
  - Original SHA-256: `d641e0c298dcc1c068e265150f4b97e3be7666f9f31f9f881d3f97c314a58a5d`
  - Archive bytes: 14,207,250
  - Archive SHA-256: `0106a9b82a074a825e0e85aa2632d6e233c6c4a6566d7ddaf83a47749769b9d9`
- `recovery-run-metrics.jsonl.tar.gz`
  - Original bytes: 50,113
  - Original SHA-256: `fe3d0637f9d7e786857f5bdb429b6ac3249968e7239febbed04503f573026053`
  - Archive bytes: 6,810
  - Archive SHA-256: `1369838c5f074bdabddee2bd6d597cb76db525a8d32ef59b1dda934bb51df99c`

Each archive contains one file named `metrics.jsonl`. Extract with:

```powershell
tar -xzf failed-run-metrics.jsonl.tar.gz
```

Before publication, both streams were scanned for sensitive field names, known API-token signatures, PEM private-key blocks, 64-hex private-key-like values, and `.secrets` paths. No matches were found. Decompression was then verified to reproduce the original byte counts and SHA-256 hashes above.

## Failed generation

- Generation: `continuous-1784746437424-619587a1`
- Started: 2026-07-22 18:53:57 UTC
- Fatal exit: 2026-07-25 00:14:06 UTC
- Direct error type: `FatalContinuousNetworkError`
- Exchange response: `Cannot subscribe to more than 1000 channels.`
- Terminal process state: runner stopped, execution disabled, but followers were not flat.
- Metrics dropped: zero.

The market socket disconnected and attempted to reconnect. Hyperliquid rejected the replacement market subscription set because it exceeded the 1,000-channel ceiling. The local evidence proves the rejection and fatal exit, but it cannot prove whether the server temporarily counted the prior connection, another process shared the IP allowance, or both.

The run periodically tracked 468 markets. Maintaining several permanent channels for every eligible market left insufficient reconnect-overlap headroom. The implementation therefore demonstrated steady-state feasibility, not continuous reconnect feasibility.

## Exposure after the crash

The fatal network exception exited through the general error path without performing authoritative reduce-only closeout. A later read-only account audit found 14 open positions:

- acc3: BTC
- acc4: HYPE, KAITO, XPL
- acc5: xyz:BRENTOIL, xyz:SP500, xyz:SPCX
- acc6: FARTCOIN
- acc7: xyz:GOOGL, xyz:SKHX, xyz:KR200, xyz:EWY
- acc10: BTC, KAITO

Acc1, acc2, acc8, and acc9 were flat. The positions remained open for approximately seven hours until an authorized recovery was performed.

## Authorized recovery

- Recovery generation: `continuous-1784963558188-649d95b0`
- Started: 2026-07-25 07:12:38 UTC
- Finished: 2026-07-25 07:14:57 UTC
- A graceful stop request was latched immediately after launch.
- Startup preflight verified signer ownership, no resting follower orders, and the non-flat inventory.
- The production reduce-only closeout path flattened all positions.
- Fifteen filled reduce-only actions were required because SPCX needed a second partial-reduction action.
- Final status: stopped, execution disabled, followers flat, error null.
- Both recovery Python processes terminated.

All ten accounts were queried across their configured DEXes after shutdown and were authoritatively flat. The durable action journals contained 747 terminal actions, zero duplicate CLOIDs, and zero `PREPARED`, `SEND_ATTEMPTED`, `UNKNOWN`, `RESTING`, `SUBMITTED`, or `OPEN` actions.

## Earlier operator intervention retained by this run

On 2026-07-24, acc6 contained a WLD action stuck in `UNKNOWN`. The IOC had expired, repeated `orderStatus` checks returned `unknownOid`, no matching WLD fill existed, and authoritative follower state contained no WLD position. With explicit operator authorization, the durable row was transitioned to `CANCELED`. Acc6 then completed four reduce-only convergence fills without retrying WLD.

This intervention preserved the evidence run, but it also proves the generation was not unattended. See `operator-intervention.md`.

## Material defects

1. Fatal source, market, or action network errors can exit without authoritative reduce-only closeout.
2. Subscription feasibility lacks reconnect-overlap headroom. Continuous operation needs a bounded dynamic market working set rather than permanent feeds for every eligible market.
3. Ambiguous entry actions correctly block duplicate risk, but an unrelated ambiguity can indefinitely block independently provable exposure reduction.
4. Completed closeout raced with the restored fail-close loop and emitted a second `fail_close_started` event.
5. Final status retained a stale action-backlog value even though tasks were dead, exchange truth was flat, and all journals were terminal.
6. Direct fail-close metrics lack the CLOID, market, size, price, and outcome detail available for ordinary action attempts.

## Required behavior before unattended production

- A fatal transport path must disable new risk and invoke bounded authoritative reduce-only convergence before process exit when exchange truth is available.
- Reconnect subscription planning must reserve explicit headroom and avoid multiplying channels across the complete market catalog.
- Unknown actions must remain reconciled under their original CLOID, while a separate conservative path permits only independently provable, monotonic reduce-only convergence.
- Closeout must be single-owner and idempotent.
- Terminal status must report zero live backlog after tasks have drained.
- Fail-close actions must have the same durable diagnostic identity and outcome detail as normal actions.

## Verdict

The recovery was successful and terminally safe. The failed generation is valuable real-world evidence, but it is a **NO-GO for unattended production** until the defects above are corrected and validated through the direct runtime path.
