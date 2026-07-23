# Omega-3 final report

Date: 2026-07-18

## Executive verdict

**Ready for bounded fleet testing, not verified for unattended full ten-slot mainnet use. Confidence: high for this boundary.**

The prior project was materially overbuilt and operationally ineffective. Eight mainnet pilot generations were attempted on 2026-07-18: five failed before activation; three activated but produced zero actions. The latest old run processed 5,144 desired-state observations and 521 revisions while producing zero intents/actions, yet made 7,692 REST requests. A Unified-account accounting bug wrote follower equity as zero, and the supervisor/guardian/gate stack obscured the root cause.

The old live path was replaced, not patched. Sixty-four thousand three hundred twenty-four lines of obsolete production/test/script code were removed, including FleetRuntime, supervisor, guardian, authority, attestor, launch-control, alternate execution gateway, synthetic benchmarks, and legacy mainnet REST mutation commands. The active continuous execution core is about 7,800 lines across reducers, runtime, network, journal, signer, config, and runner.

The replacement has positive real evidence:

- four bounded mainnet WebSocket IOC actions across two followers, zero HTTP actions, all terminal `FILLED`, zero unresolved journal rows, and independent final flat/order-free truth;
- two real monitor-only mainnet runs using the same durable engine directory, both reaching `RUNNING/ready`, with three HTTP catalog calls/weight 60, 20 WS preflight posts, zero ongoing REST, zero actions, zero reconnects, zero recovery HTTP, and zero drops;
- receive-to-source-reducer maxima of 4.7194 ms and 4.6668 ms;
- focused behavioral, restart, ambiguity, lock, queue, reconnect, and UI validation.

The remaining missing proof is one naturally occurring fresh leader fill through the complete continuous path and a real ten-profile monitor soak. Four action samples do not establish p95/p99, trading consistency, tracking error, or profitability.

## Verified material findings and decisions

### F-01: Unified follower equity was computed incorrectly

Severity: High. The old path summed per-DEX margin summaries for a Unified follower and wrote zero account value even though Spot token-0 USDC existed. This blocked all intents while superficial gates kept retrying. The continuous preflight/reconciliation now uses Unified Spot token-0 USDC for equity and all relevant DEX position truth.

### F-02: REST was doing live-system work it did not need to do

Severity: High. The two-slot old runtime repeatedly polled follower clearinghouse/open-order state and replayed source history, consuming hundreds of weighted units per minute. The new steady path uses WS subscriptions and WS POST. HTTP is three catalog calls at startup plus an explicit disabled-by-default recovery fallback.

### F-03: Safety machinery could prevent the safe reduction

Severity: High. Entry book freshness was incorrectly reused after an approximately 981 ms acknowledgement, blocking the proof's reduce-only close. Legacy guardian flattening could also be stopped by unrelated book/depth/rate gates. The proof incident was recovered with one provenance-bound reduce-only WS action. Current closes retain strict entry freshness but have a bounded asynchronous recovery-book window; generic automatic flatten was removed.

### F-04: The architecture multiplied failure surfaces

Severity: High. Multiple supervisors, guardians, authorities, rest-budget processes, benchmarks, and launch evidence stages did not make a working trade path. They made state and ownership harder to reason about. The active design uses three sockets, per-slot reducers, one journal per signer, one runtime state store, and OS locks.

### F-05: Reconnect loops could self-rate-limit

Severity: Medium. Three independent 5-second reconnect loops could collectively exceed the venue's 30 new connections/minute limit during accept/drop flapping. They now share one 24/minute limiter.

### F-06: Browser/CLI retained obsolete alternate live routes

Severity: Medium. Old fleet launch, polling runner, source follower, settlement/smoke, console worker, watchdog, canary, benchmark, and credential-export surfaces survived after the engine replacement. They were removed. Browser control exposes only the continuous controller; monitor status is keyless.

### F-07: Candidate ranking remains analytically unproven

Severity: Medium. The analytics UI is useful for bounded exploration, but history completeness, global universe, independent accounting, held-out predictiveness, and copyability after execution costs remain unproven. No global top-100 or SOTA claim is allowed.

## External comparison

Source-level comparison covered the official Python SDK/node/examples, direct Hyperliquid copytraders, NautilusTrader, typed connectors, wallet/PnL analyzers, historical stores, and closed-product documentation. Useful narrow patterns were adapted: official signing/schema authority, actor ownership, deterministic desired exposure, status-race handling, and independent analytics watermarks.

Whole frameworks, blind HTTP retries, random non-durable CLOIDs, VPN/multi-IP workarounds, Linux migration, local-node deployment, and unlicensed code reuse were rejected. The local durable CLOID/UNKNOWN model is stronger than several inspected direct copytraders on that narrow property; overall superiority is not established. See [EXTERNAL_BASELINE.md](EXTERNAL_BASELINE.md).

## Test verdict

Tests were used as regression instruments, not as the definition of correctness. The useful suite covers money-moving invariants, crash boundaries, ambiguity, reconnect overlap, source attribution, slow reconciliation, global locks, monitor-no-key behavior, and active UI routing. Dedicated tests for the deleted architecture were removed with it.

The strongest evidence is the agreement between code tracing, independent protocol review, durable journals, exchange truth, real monitor artifacts, and focused tests. A proposed extra journal-checkpoint mechanism was deliberately rejected after the reviewer falsified the alleged crash path by tracing the actual persistence/lock ordering.

Final integration validation passed 1,370 tests plus branch-aware coverage, Ruff, formatting, mypy for production and tests, bytecode compilation, dependency audit, package build, metadata/content verification, and an installed-wheel import. These results establish internal consistency only; the live evidence above remains the readiness authority.

## Performance verdict

- Continuous monitor source receive -> reducer: max 4.7194 ms and 4.6668 ms in two runs.
- Bounded mainnet action call -> socket send: 7.7723-15.2501 ms, four samples.
- Socket send -> exchange response: 858.2787-980.5405 ms, four samples.

These stages are not directly additive across different experiments. No defensible p95/p99 or full leader-event-to-order result exists yet. Old synthetic FleetRuntime benchmark numbers do not describe the replacement and are not release gates.

## Platform decision

Native Windows is sufficient for the current workload. Runtime state is on local NTFS under `%LOCALAPPDATA%`. There is no demonstrated blocker or material end-to-end gain that justifies WSL/Linux/VM/container/node/VPN complexity.

## Final readiness

- Analysis/UI: acceptable with stated data limits.
- Deterministic core/restart: verified locally.
- Two-account monitor/restart: passed on mainnet.
- Two-account bounded WS action lane: passed with one exposed-and-recovered close-window defect.
- Bounded fleet testing: GO.
- Full ten-slot unattended mainnet: not yet verified.
- Profitability/predictive ranking/SOTA: not established.

The exact next steps are intentionally small: ten-profile monitor/restart, one natural leader fill through the full runtime, then an independent analytics/copyability oracle.
