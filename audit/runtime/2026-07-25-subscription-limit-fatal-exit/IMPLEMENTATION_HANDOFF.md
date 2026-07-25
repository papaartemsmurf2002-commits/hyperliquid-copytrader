# Implementation handoff: continuous-run incident and product hardening

## Purpose

This is the implementation handoff for the 53.3-hour ARMED generation
`continuous-1784746437424-619587a1` and its separately authorized recovery
generation `continuous-1784963558188-649d95b0`.

The objective is not another architecture audit or another generic soak. The
next agent should fix the direct failures demonstrated by this run, preserve
the mechanisms that worked, and validate each repair at the production
boundary.

The current verdict is:

> **NO-GO for unattended ARMED operation.**

The product executed and reconciled hundreds of actions correctly, but it also:

- exhausted the venue WebSocket subscription ceiling during a market reconnect;
- terminated without flattening 14 live follower positions;
- required an operator to edit one ambiguous action journal row before acc6
  could resume reductions;
- repeatedly sent a margin-impossible SKHX entry 114 times;
- briefly ran two competing closeout paths during recovery;
- emitted ten startup `AssertionError` slot failures during that closeout;
- left incomplete and misleading terminal diagnostics.

The authorized recovery eventually ended authoritatively flat with all durable
actions terminal. That proves the close machinery can work under favorable
conditions. It does not erase the production failures.

## Evidence and proof boundaries

### Sources inspected

The conclusions below were reconstructed from:

- the failed-run raw `metrics.jsonl` stream;
- the recovery-run raw `metrics.jsonl` stream;
- failed and recovery `status.json`, `stdout.log`, and `stderr.log`;
- ten durable SQLite action journals;
- post-run authoritative account-position checks recorded by the recovery
  report;
- the operator's SKHX screenshot showing the live follower position at 1×
  cross leverage;
- current production code paths, rather than only the existing incident prose;
- current Hyperliquid documentation;
- the current NautilusTrader Hyperliquid integration documentation.

The raw streams and their hashes are in this directory. The mutable journals
remain local; `journal-summary.json` preserves their terminal aggregation.

### Dataset shape and validation

| Evidence | Grain | Rows/items | Integrity result |
|---|---|---:|---|
| Failed metrics | One emitted runtime metric | 1,076,967 | Zero JSON parse failures |
| Recovery metrics | One emitted runtime metric | 320 | Zero JSON parse failures |
| Durable journals | One durable action lifecycle | 747 | Zero duplicate CLOIDs; zero nonterminal rows after recovery |
| Post-recovery account audit | One follower account / configured DEX | 10 accounts | All authoritatively flat |

The failed metrics cover 53.314 hours. They contain 732 current-run action
attempts. The 747 journal actions are not the same grain: the journal total
also contains prior durable actions and the recovery closeout actions.

No metric-sink loss was reported. The 246,637 `monitoring_drop` records refer
to a separate bounded UI/status dispatch queue, not loss of the raw metric
stream or action journal.

### Important remaining uncertainty

- Local evidence proves the server's `Cannot subscribe to more than 1000
  channels` response. It cannot prove whether the old market socket was still
  counted, another local process shared the IP allowance, or both.
- The 1× SKHX exchange leverage is supported by the operator screenshot and
  the repeated `PerpMargin` responses. The action metric did not retain a
  complete leverage/collateral snapshot, so the exact available-margin
  calculation cannot be replayed from metrics alone.
- Metrics record BBO and L2 ages but do not retain every raw market frame.
- Price-deviation evidence is present for entries but absent for ordinary and
  emergency reductions.
- Analytics-page requests are not individually represented in this run's
  metrics. They were not the cause of this failure, but their future budget
  isolation still needs a direct test.

## Run scorecard

### What worked

| Area | Evidence | Assessment |
|---|---|---|
| Leader ingestion | 797,728 source reductions; 797,711 accepted | Strong under sustained load |
| Source queue | p50 8.23 ms, p95 36.68 ms, p99 64.02 ms, max 216.41 ms | Bounded |
| Source receive to reduction | p50 11.44 ms, p95 40.17 ms, p99 67.49 ms, max 293.71 ms | Bounded |
| Socket recovery before the terminal incident | 23 action, 25 market, and 23 source reconnects completed | Ordinary reconnect logic was effective |
| Action identity | Zero duplicate CLOIDs across 747 durable actions | Strong |
| Ordinary local source-to-send | 698 measured; p50 17.51 ms, p95 343.37 ms, p99 948.93 ms, max 1,055.90 ms | Single-flight tail is bounded to about one exchange-response cycle |
| Exchange response | p50 956.14 ms, p95 1,073.97 ms, p99 1,156.97 ms, max 1,548.76 ms | Consistent with the known committed-response path |
| IOC fill policy | 7 no-match rejections out of 732 attempts | Generally effective |
| Dynamic catalog refresh | 633 periodic refreshes; catalog grew from 466 to 468 markets; 2 subscriptions added | New-market discovery exists |
| Sizing | 679 formula-bearing attempts reproduced the proportional formula to Decimal serialization precision | Correct for observed standard/unified cases and approximately $100 balances |
| Recovery closeout | 15 filled reduce-only actions; all ten accounts flat | Capable, but not single-owner or crash-safe yet |

### What failed or was materially undesirable

| Severity | Failure | Observed consequence |
|---|---|---|
| Critical | Fatal market subscription error terminated the runner without closeout | 14 live positions remained open for about seven hours |
| High | Full eligible catalog was permanently prewarmed with three feeds per market | Reconnect lacked safe subscription overlap headroom |
| High | One WLD `UNKNOWN` blocked the complete acc6 signer lane | About 735 minute-level refresh cycles, roughly 12.25 hours, remained blocked until manual SQLite terminalization |
| High | Actual follower leverage/margin was not provisioned or enforced | 114 repeated SKHX `Insufficient margin` entry rejects |
| High | Fail-close is not a single exclusive state-machine owner | Ten startup assertion failures, four normal actor reductions during closeout, and a second `fail_close_started` |
| Medium | Minimum-notional preflight was too optimistic at the boundary | Four venue `MinTradeNtl` rejects despite local preflight |
| Medium | UI/status dispatch queue dropped 246,637 intermediate events | Operator-visible transient state could be missed; metric volume was inflated |
| Medium | Terminal status reused stale operational fields | Final status reported action backlog 6 despite dead tasks, terminal journals, and flat truth |
| Medium | Fail-close metrics are coarse | CLOID, market, size, limit, fill, and detailed result were missing from 12 closeout metrics |
| Low/Medium | Four action-mux futures ended with “Future exception was never retrieved” | Async cleanup is incomplete and obscures disconnect diagnostics |
| Observation | 124 entries were sent more than one minute after their original leader fill; maximum age 4.88 hours | This may be valid persistent-target behavior, but its economic policy is not explicit |
| Observation | L2 age was much worse than BBO age | L2 visible-liquidity claims are less current than the actual price anchor |

## Finding 1 — subscription accounting and reconnect design are wrong

### Direct failure

The final market reconnect received:

```text
Cannot subscribe to more than 1000 channels.
```

The preceding 25 market reconnects recovered. The final market gap was followed
by the next connection epoch about two seconds later, which makes overlap with
the previous socket a plausible cause. It is still an inference, not a
server-side proof.

Across the whole run there were 72 recorded connection gaps and 71 completed
reconnects:

- action: 23 gaps, 23 reconnects;
- market: 26 gaps, 25 reconnects;
- source: 23 gaps, 23 reconnects.

The unmatched final market gap is the fatal incident. This means reconnecting
in general is not broken; reconnecting a very large subscription set without a
capacity envelope is broken.

### Concrete code contradiction

The production market stream creates **three** subscriptions per active market:

- `activeAssetCtx`;
- `l2Book`;
- `bbo`.

See
[`market_stream.py::_subscription_specs`](../../../src/hyperliquid_copytrader/market_stream.py#L444).

The planning model in
[`stream_gateway.py::active_market_subscription_capacity`](../../../src/hyperliquid_copytrader/stream_gateway.py#L1062)
and
[`fleet_config.py::_websocket_load_model`](../../../src/hyperliquid_copytrader/fleet_config.py#L1143)
budgets **two** market
subscriptions and relies on an unrelated legacy source-subscription list. The
configured `maximum_active_markets=420` therefore does not describe the
continuous runtime that actually ran.

The runtime's launch check is closer to reality because it uses:

```text
actual source specs + 3 × prewarmed markets
```

but it checks only one steady-state connection. It has no old-plus-new
reconnect envelope.

### Why “dynamic markets” still caused the crash

Dynamic catalog discovery itself works and must be preserved. The failure is
that dynamic eligibility currently means:

```text
every eligible catalog market
→ added to _prewarm_markets
→ permanently subscribed
```

[`ContinuousRuntime._activate()`](../../../src/hyperliquid_copytrader/continuous_runtime.py#L1900)
always starts from the complete prewarm set. At the end of the
run the eligible set was approximately 277 markets, so market data alone
required about 831 subscriptions. Ten leader accounts add roughly 3–4
user-specific subscriptions each in the current continuous path.

That steady state fits below 1,000. A briefly overlapping replacement market
socket does not.

### Required repair

Do **not** restore frozen ticker lists. Separate these concepts:

1. **Eligible catalog** — every currently supported standard/HIP-3 market.
2. **Active working set** — only markets that currently require executable
   price/depth state.

The working set must contain:

- nonzero attributable leader exposure;
- nonzero authoritative follower exposure;
- unresolved or in-flight actions;
- pending current targets or liquidity retries;
- active fail-close/recovery fences;
- a short TTL for recently active leader markets.

A fill for a newly listed or previously inactive market arrives through the
leader `userFills` stream. That event should:

1. resolve the market from the latest catalog;
2. request an immediate catalog refresh if the symbol is new;
3. add the market's required feeds to the working set;
4. wait for a fresh executable snapshot;
5. then drive the current target.

Evict a market only after leader exposure, follower exposure, target,
reservation, unresolved action, and recovery fence are all zero for a grace
period.

Capacity must be computed from the exact generated subscription specifications,
not copied constants. Validate:

```text
fixed leader subscriptions
+ old working-set subscriptions
+ replacement working-set subscriptions
+ control headroom
<= 1000
```

With three feeds per market, 30–40 leader subscriptions, and explicit control
headroom, a conservative overlap-safe market cap around 140 is ample for this
product. Derive the exact cap in code; do not hardcode 140 as a second source of
truth.

Optionally evaluate `allDexsAssetCtxs` as one aggregate context feed plus JIT
BBO/L2 subscriptions. NautilusTrader uses aggregate contexts. Adopt that only
if symbol/index alignment and new-listing behavior are proven simpler than
per-market context feeds.

### Validation

- Build a 468-market catalog with a 35-market working set.
- Add a never-before-active HIP-3 leader fill during the run; prove JIT
  activation and execution without restart.
- Simulate old and replacement market sockets coexisting.
- Assert the exact generated subscription union stays below the venue limit.
- Reconnect repeatedly at the maximum supported working set.
- Prove eviction never removes a market with exposure, target, unresolved
  action, or closeout need.
- Treat a subscription-cap error as a degraded/rebuild event before declaring
  the entire process fatal.

## Finding 2 — fatal transport failure can strand live exposure

### Direct failure

When the market subscription error was classified as
`FatalContinuousNetworkError`,
[`ContinuousNetworkDriver.run()`](../../../src/hyperliquid_copytrader/continuous_network.py#L693)
waited for the
first completed task, raised the failure, canceled every source, market,
action, reconciliation, and actor task, and returned control to the runner.

[`continuous_runner.py`](../../../src/hyperliquid_copytrader/continuous_runner.py#L812)
wrote an error status and closed local resources. It did
not invoke `close_out_all()` in the generic exception path.

This left 14 positions:

- acc3: BTC;
- acc4: HYPE, KAITO, XPL;
- acc5: xyz:BRENTOIL, xyz:SP500, xyz:SPCX;
- acc6: FARTCOIN;
- acc7: xyz:GOOGL, xyz:SKHX, xyz:KR200, xyz:EWY;
- acc10: BTC, KAITO.

### Required repair

Transport health must not be the same state as trading authority.

On a material source, market, or action failure:

1. atomically disable all new risk;
2. enter one visible `EMERGENCY_CLOSING` state;
3. retain or re-establish the action/info path independently of the failed
   market socket;
4. fetch fresh authoritative follower positions;
5. activate only the markets required by current exposure;
6. perform bounded, monotonic reduce-only convergence;
7. reconcile after every result;
8. exit only when flat and terminal.

If authoritative closeout cannot proceed, keep the process alive in
`EMERGENCY_BLOCKED`, expose the exact blocker in the UI, and alarm. Do not
silently terminate with positions open.

This is not authorization to auto-relaunch or auto-arm after a crash. New risk
must remain off until an operator explicitly re-arms.

### Validation

Inject each of these while followers are nonflat:

- fatal market subscription error;
- source socket terminal failure;
- action socket disconnect;
- market data stale during closeout;
- info reconciliation temporarily unavailable.

For every case prove either:

- authoritative flatness and terminal journals before exit; or
- a live, visible, non-armed `EMERGENCY_BLOCKED` process with no new risk.

## Finding 3 — an unrelated UNKNOWN can block all risk reduction

### Direct failure

Acc6 produced one WLD entry with an ambiguous send result after a WebSocket
disconnect. The durable CLOID correctly remained `UNKNOWN`; blind retry did
not occur.

Repeated `orderStatus` checks returned `unknownOid`, no WLD fill was found, and
authoritative follower truth showed no WLD position. Nevertheless:

- [`ContinuousSignerLane.execute_ioc()`](../../../src/hyperliquid_copytrader/continuous_executor.py#L134)
  refused every subsequent action while
  any unresolved action existed;
- `_ready()` blocked the slot;
- fail-close also refused to act while any unresolved action existed.

The slot remained blocked for about 735 minute-level follower refreshes. The
operator eventually authorized an external SQLite transition from `UNKNOWN`
to `CANCELED`, after which acc6 completed four reductions.

The manual edit preserved the experiment. It is not a production recovery
mechanism.

### Required repair

Keep the original CLOID unresolved and never blindly retry it. Add a separate
emergency-reduction admission rule which may bypass an unrelated ambiguity only
when all of the following hold:

- authoritative follower positions are fresh;
- current leader/source truth calls for less absolute exposure;
- the action is `reduce_only`;
- its size is no greater than the current authoritative absolute position;
- it moves monotonically toward zero;
- it cannot reverse, increase, or open exposure;
- worst-case late fill of the unresolved action has been modeled;
- signer/nonce serialization remains exclusive;
- the bypass records the unresolved CLOID and the evidence used.

The old CLOID continues normal resolution. If it later fills, reconciliation
must calculate another monotonic reduce-only correction. A late ambiguous fill
must never trigger an offsetting risk increase.

### Validation

- Unknown entry on market A plus real exposure on markets B/C: B/C can close,
  A is not retried.
- Unknown entry on market A plus exposure on A: only a worst-case-safe
  reduction is permitted.
- The unknown later fills after the reduction: fresh reconciliation converges
  without reversal.
- Contradictory or stale truth blocks the bypass.
- Restart retains the unresolved CLOID and the reduction evidence.

## Finding 4 — follower leverage and margin policy are not actually enforced

### Direct failure

Of 125 rejected actions:

- 114 were acc7 `xyz:SKHX` entries rejected for insufficient margin;
- 7 were IOC no-match/no-liquidity;
- 4 were minimum-notional failures.

Thus 91.2% of all rejects came from one margin-policy defect.

The follower already held approximately 0.075 SKHX at 1× cross leverage. The
copy target asked for roughly 0.118, so the bot repeatedly attempted to add
about 0.042–0.044 SKHX, approximately $55 at the observed price. With about
$99 account equity and the existing position consuming almost all 1× margin,
the increase was impossible.

### Concrete code defect

The desired engine caps the leader's leverage metadata by the slot policy, but
it does not configure follower exchange leverage.

The
[`continuous runtime preflight`](../../../src/hyperliquid_copytrader/continuous_runtime.py#L1414):

- reads actual leverage only when that market already has an open position;
- passes `available_collateral_usd=None` to preflight;
- uses `follower.equity × slot.max_leverage` as a local gross ceiling.

Therefore a configured `max_leverage=50` acts as if capacity exists even when
the exchange account is actually at 1×. It is a risk ceiling, not proof of
exchange configuration.

### Required product policy

Implement one explicit policy, not an accidental mixture:

#### Recommended policy for faithful copying

Provision follower leverage outside the IOC hot path:

1. when a market enters the JIT working set before its first risk increase,
   read its actual follower leverage and margin mode;
2. choose `desired_leverage = min(slot policy, venue market maximum)`;
3. submit the official `updateLeverage` action with the correct cross/isolated
   mode;
4. serialize it through the same signer/nonce owner;
5. record a durable setup action;
6. verify the new value from fresh account state;
7. only then allow the entry.

Cache the verified configuration by follower, market, catalog identity, and
account mode. Revalidate on restart, catalog change, or explicit operator
change. Reductions must never wait for leverage provisioning.

For markets that cannot use the requested cross mode, either provision the
supported isolated mode explicitly or mark them unsupported with a clear
reason.

If the product owner instead chooses “never mutate leverage,” then entries must
be sized to actual exchange leverage and available margin. That is safer but
less faithful. Do not silently switch between these policies.

### Stop rejection hammering

One deterministic `PerpMargin` rejection should latch a margin blocker keyed to
the actual dependencies:

- desired target/revision;
- current follower position;
- verified leverage/margin mode;
- available collateral/margin state;
- market identity.

Do not include harmless mark/equity jitter that turns the same impossible
target into a “new” retry. Retry only after one of those dependencies changes.

### Validation

- Flat market at 1×, venue max 10×, policy 10×: provision 10×, verify, then
  submit one entry.
- Existing 1× position requiring more margin: no entry before verified setup.
- `updateLeverage` unknown outcome: no entry retry; reductions remain available.
- Unsupported margin mode: fail closed with one stable reason.
- Repeated identical target: one rejection/block, not 114 actions.
- Restart: verified leverage is rechecked before risk increase.

Hyperliquid documents `updateLeverage` as a separate signed action. This is
also consistent with NautilusTrader's principle that leverage configuration is
explicit rather than inferred from an order.

## Finding 5 — closeout has competing owners and an invalid startup state

### Direct recovery evidence

The recovery run emitted:

- 2 `fail_close_started`;
- 1 `fail_close_complete`;
- 12 coarse `fail_close_attempt`;
- 4 ordinary `action_attempt` reductions during closeout;
- 10 follower-refresh reasons equal to `slot drive failed: AssertionError:`.

The first closeout completed at wall time `1784963696803`. Two milliseconds
later the restored-fail-close loop emitted a second start.

### Exact assertion hole

When `fail_close_reason` exists,
[`_ready()`](../../../src/hyperliquid_copytrader/continuous_runtime.py#L2050)
sets:

```text
state = PAUSE_ENTRIES
```

and returns.

The ordinary `_plan_drive()` path exits only for:

```text
RECOVERING or OPERATOR_STOP
```

It then asserts `follower is not None`. If fail-close is latched before the
first follower snapshot, `PAUSE_ENTRIES` allows execution to reach that
assertion with no follower truth. That happened across all ten slots.

### Competing authority

While the explicit closeout loop was flattening, normal slot actors remained
allowed to perform reductions. Four such actions ran. One SPCX action reduced
toward the leader-derived desired target rather than directly to flat, after
which fail-close had to continue flattening.

Normal convergence and fail-close must not independently own the same account
at the same time.

### Required repair

- Introduce one idempotent closeout operation/epoch.
- Every trigger—operator stop, duration, restored latch, source failure, fatal
  transport, status failure—must join the same task.
- Once closeout begins, suppress ordinary slot actors for selected slots.
- Reconciliation can continue, but only the closeout owner may submit actions.
- Fail-close before initial follower truth must wait for fresh truth, never
  assert.
- Complete and clear durable latch ownership before releasing the closeout
  operation.
- A second caller after completion returns the completed result without
  emitting another start.

### Validation

- Latch fail-close before the first follower snapshot for all ten slots.
- Trigger operator stop and restored-latch recovery simultaneously.
- Trigger a source event during closeout.
- Prove one closeout ID, one start, one complete, zero ordinary copy actions,
  no assertion, and authoritative terminal flatness.

## Finding 6 — minimum-notional policy is correct in principle but weak at the boundary

### Direct evidence

The venue returned four `MinTradeNtl` errors:

- three acc9 `xyz:CXMT` normal reductions;
- one acc5 `xyz:BRENTOIL` entry.

The current official Hyperliquid contract states that ordinary perp orders
must have a minimum value of $10. This run confirms that the $10 rule is real
for risk increases and partial reductions.

The current local policy also permits an exact full reduce-only close below
$10. Preserve that narrow exception only while it remains verified.

### What not to do

- Do not change the ordinary minimum to $0.60–$1.10.
- Do not enlarge a faithful small follower target to $10.
- Do not distort proportional sizing to make an order pass.

Small targets should remain residual and accumulate naturally until the
leader-derived delta is executable.

### Why local preflight still missed four orders

Preflight calculates notional using the submitted aggressive limit price. Near
$10, the venue can use an effective reference that makes a locally calculated
$10.01–$10.06 order fall below its threshold after precision/reference
differences.

### Required repair

- Use a conservative minimum-notional reference for risk increases and partial
  reductions, based on current executable/mark/oracle behavior established by
  the official contract and bounded probe evidence.
- Require at least one quantity step of headroom above the venue floor, rather
  than admitting an order at an exact floating boundary.
- Record:
  - rounded size and price;
  - reference price;
  - calculated notional;
  - floor and headroom;
  - exact-full-close classification.

### Validation

- $9.99 ordinary entry is locally blocked.
- Exact full reduce-only close below $10 remains possible.
- Partial reduce-only below $10 is blocked and retained as residual.
- Boundary values around $10 survive price/size normalization without venue
  rejection.
- Increasing follower funding scales the target linearly; it never causes
  minimum-size upscaling.

## Finding 7 — IOC/BBO policy mostly worked; improve evidence, not complexity

### Measured result

Failed-run action outcomes:

| Outcome | Count |
|---|---:|
| Filled | 602 |
| Partially filled | 4 |
| Rejected | 125 |
| Unknown | 1 |

Only seven attempts were rejected for IOC no-match/no-liquidity:

- 4 SKHX;
- 2 CL;
- 1 SHAZ.

Only three of 732 attempts were marked as a liquidity retry. This is not
evidence that the product needs chase orders, GTC lifecycle management, or
pervasive full-depth execution.

The current behavior already matches the useful NautilusTrader pattern:

- an aggressive IOC limit;
- anchored to current BBO;
- bounded by configurable slippage;
- normalized to venue precision;
- partial/no-fill outcomes preserved and reconciled.

### Price evidence

For 341 filled entries with leader-price evidence:

- median absolute leader-to-follower fill deviation: 1.12 bps;
- p95: 34.26 bps;
- p99: 49.61 bps;
- maximum: 77.19 bps.

BBO age at send:

- p50: 55.5 ms;
- p95: 430.95 ms;
- p99: 1,433.05 ms;
- max: 3,462 ms.

L2 age at send:

- p50: 2,578.5 ms;
- p95: 5,033.95 ms;
- p99: 5,262.52 ms;
- max: 5,477 ms.

The actual price anchor is generally much fresher than the L2 visible-size
evidence. Do not claim current executable depth from a multi-second-old book.

### Required improvement

- Keep BBO-first aggressive IOC.
- Keep an entry leader-price cap; do not widen entries until everything fills.
- Allow a separately configured, wider envelope for ordinary reductions and a
  still wider bounded envelope for emergency reductions.
- After a partial/no-fill, permit at most a bounded remaining-size retry after
  a genuinely new BBO/book observation.
- Record market-data age used for visible-size admission.
- If L2 is stale, treat visible depth as unknown rather than current.
- Add price-deviation metrics for normal and emergency reductions.
- Report fill ratio, notional completion, slippage, and eventual
  desired-versus-authoritative tracking.

Full L2 walking should be added only if post-repair evidence shows material
missed notional despite fresh books. Chase/GTC orders should remain out unless
their lifecycle benefit is measured to exceed stale-target and cancellation
risk.

## Finding 8 — single-flight execution should remain

The run does not support adding per-market concurrency inside one follower
account.

For 698 actions with source-to-send timing:

- 694, or 99.43%, were at or below one second;
- only four exceeded one second;
- the maximum was 1.056 seconds.

That maximum is approximately one exchange-response cycle, not an unbounded
multi-market queue. The existing design also avoids nonce, margin reservation,
and late-result complexity.

Keep one unresolved action per follower slot. Reconsider per-market concurrency
only if future metrics prove:

- more than about 1% of eligible cross-market actions wait behind another
  market;
- the wait causes measurable fill/tracking loss;
- a bounded concurrency prototype improves economics without increasing p95
  latency, ambiguous outcomes, or margin conflicts.

## Finding 9 — persistent delayed targets need an explicit economic contract

Entries were admitted from fresh fills, but some remained current while waiting
for BBO/price-cap conditions:

- median leader-trigger age at send: 620.5 ms;
- p95: 403.6 seconds;
- p99: 2,002.2 seconds;
- maximum: 17,565.2 seconds, about 4.88 hours;
- 124 of 464 entries, 26.7%, were older than one minute.

This is not automatically stale execution. A copytrader follows target
positions, and a target can remain valid while the leader still holds it.
Delayed execution can be economically better than permanently missing the
position.

The contract must become explicit:

- a persistent target is valid only while the latest attributable leader state
  still requires it;
- any close, reversal, or newer source revision supersedes it immediately;
- source truth must remain fresh;
- waiting reason and duration must remain visible;
- delayed fill quality must be reported separately from immediate copying;
- a market/leader-specific maximum economic horizon may be added only after
  replay evidence, not as an arbitrary global expiry.

Add tests proving a four-hour-old original fill cannot execute if the leader
closed meanwhile, but can execute if the latest authoritative target still
requires the position and the configured policy permits it.

## Finding 10 — monitoring and terminal diagnostics are not trustworthy enough

### Monitoring queue

[`_publish()`](../../../src/hyperliquid_copytrader/continuous_network.py#L1331)
uses a 256-item FIFO. When full, it drops the oldest event and
emits one `monitoring_drop` metric for every drop. The run produced 246,637
such metrics—22.9% of the complete metrics stream.

Core source processing, actions, and journals continued. This is not evidence
of lost trading events. It is evidence that UI/status telemetry is modeled as
a high-volume event queue when the UI only needs current per-slot state plus
critical transitions.

Replace it with:

- coalesced latest state per slot/connection;
- lossless critical action/error/closeout events;
- one aggregate coalescing/drop counter per status interval.

Do not merely enlarge the queue.

### Final status

The runner merges `last_status` into the terminal document after tasks are
canceled. It does not recompute operational backlog after drain. The recovery
therefore reported action backlog 6 despite:

- dead tasks;
- all journals terminal;
- authoritative flat follower truth.

Terminal status must be recomputed after task cancellation and journal close.

### Action and closeout metrics

All signed actions, including fail-close, should use one canonical
`action_attempt` schema:

- intent class: copy, reconciliation, or fail-close;
- closeout ID where applicable;
- CLOID;
- slot and market;
- requested/submitted/filled size;
- limit and price evidence;
- send/response timing;
- terminal state and detailed venue outcome.

A `fail_close_attempt` event may reference that action but must not replace its
identity.

### Async future cleanup

The failed run's stderr contains four
`Future exception was never retrieved` warnings from action WebSocket
disconnects. When a canceled `_post()` owns a future which already has a
connection exception, explicitly consume or await that terminal result before
discarding it. Also avoid canceling the complete action/recovery subsystem
merely because the market socket failed.

## Finding 11 — REST/info budgeting was not the failure

The run used the intended WebSocket-first design:

- `normal_rest_enabled=false`;
- approximately 50 follower WS info posts/minute;
- approximately 100 logical info weight/minute;
- three HTTP catalog requests every five minutes;
- 1,899 periodic catalog HTTP requests total;
- 37,980 catalog weight total;
- about 11.87 catalog weight/minute averaged across the run;
- no recorded REST-budget denial or rate-limit failure.

This is what the earlier REST cleanup accomplished:

- ordinary follower truth and order resolution moved to WebSocket POST;
- leader and market state remained on subscriptions;
- HTTP stayed primarily at startup, periodic metadata/catalog refresh, and
  bounded recovery fallback;
- expensive analytics/history work was kept out of the copy hot path.

Do not raise the budget. Preserve strict priority:

1. unknown-order resolution and emergency reduction;
2. authoritative reconciliation;
3. action submission/cancel;
4. ordinary follower refresh;
5. catalog refresh;
6. analytics/history.

The address analyzer should use a separate bounded low-priority limiter, cache,
and cancellation. It must yield before the emergency reserve. A browser page
refresh must never consume the capacity required to resolve or close exposure.
Add a direct integration test with deliberately heavy analytics traffic and
prove critical info requests still meet their deadlines.

## Sizing verdict

The proportional sizing observed in this run is correct:

```text
leader signed position
× follower eligible equity / leader eligible equity
× 0.75 multiplier
× gross exposure scale
```

Across 679 formula-bearing actions:

- every multiplier was 0.75;
- every gross scale was 1;
- the largest recomputation difference was only Decimal serialization noise;
- standard source equity used the relevant perp-DEX account-value sum;
- unified source equity used token-0 collateral;
- follower equity used unified token-0 USDC;
- follower equity around $100 scaled targets normally.

Do not rewrite the sizing formula because of this incident. Top-ups from $50
to $100 and later values naturally scale targets linearly.

Add only the missing provenance:

- source equity observed timestamp and age;
- follower equity observed timestamp and age;
- account mode and DEX/collateral domain;
- actual follower leverage/margin mode;
- each cap before/after value.

The margin/leverage defect is separate from sizing.

## Startup existing-position behavior

The current baseline path intentionally places pre-existing leader positions
into `unattributed` state and only attributes later accepted fills. Existing
follower exposure without runtime attribution blocks new entries.

Preserve this behavior. Add a direct restart test proving:

- a leader position already open before startup is not copied merely because
  it exists;
- a later fill modifies attribution correctly;
- restored durable attribution resumes only after authoritative
  reconciliation;
- an operator re-arm after a deliberate flat close creates a new observation
  baseline rather than reopening the old target.

No journal evidence from this run indicates a startup “copy every existing
leader position” bug.

## Named account/market outcomes

These cases answer prior operator observations from the same generation:

- **acc6 CASHCAT:** 35 action attempts, all filled—10 entries and 25 normal
  reductions. The BBO/leader-cap policy materially worked here.
- **acc6 LIT:** 2 actions, both filled—one entry and one reduction.
- **acc7 GOOGL:** 1 entry, filled.
- **acc7 KR200:** 9 actions, all filled—7 entries and 2 reductions.
- **acc7 SKHX:** 128 attempts—8 filled, 2 partially filled, 4 no-match rejects,
  and 114 insufficient-margin rejects. This is primarily a leverage/margin
  configuration failure, not an IOC-liquidity failure.
- **acc6 WLD:** 2 filled actions plus 1 ambiguous entry. The ambiguity blocked
  the slot until operator intervention.
- **acc3 BONK:** no follower action appears in the current-run action metrics.
  The archived metrics do not retain enough per-market blocked-target detail to
  prove whether it was subminimum, unattributed, superseded, or otherwise
  ineligible. Future metrics must preserve the explicit no-action reason.

## Mechanisms to preserve

Do not replace these without contradictory evidence:

- durable CLOID identity and terminal journal state machine;
- no blind retry after an ambiguous signed action;
- one signer lane per follower account;
- one unresolved action per slot;
- source truth continuing while an IOC awaits exchange response;
- latest-target coalescing rather than one order per intermediate revision;
- BBO-first aggressive IOC with leader-price cap;
- separate entry, normal-reduction, and emergency-reduction price envelopes;
- exact Decimal proportional sizing;
- dynamic market discovery;
- WebSocket-first hot path and bounded HTTP use;
- startup baseline that does not copy pre-existing leader positions;
- follower reconciliation after action results and reconnects.

## Changes specifically rejected by this evidence

- Do not freeze ticker lists.
- Do not subscribe permanently to every eligible market.
- Do not add consumer VPN or multi-IP sharding.
- Do not add per-market action concurrency yet.
- Do not switch to chase/GTC orders by default.
- Do not widen entry slippage until fill rate reaches 100%.
- Do not hardcode a sub-$10 ordinary perp minimum.
- Do not upscale small faithful targets to the venue minimum.
- Do not increase REST budgets to hide poor prioritization.
- Do not add a local node or VPS as a response to this incident.
- Do not declare an `unknownOid` action canceled automatically without
  authoritative evidence.

## Implementation order

### P0 — prevent another exposed fatal exit

1. Replace permanent full-catalog market subscriptions with the JIT working set.
2. Derive subscription capacity from exact generated specs and reconnect
   overlap.
3. Add the fatal-to-emergency-close state machine.
4. Make closeout single-owner and suppress ordinary actors.
5. Fix fail-close-before-follower-truth state handling.

### P1 — restore correct trading admission

6. Implement explicit verified leverage provisioning, or explicitly choose the
   lower-fidelity actual-margin sizing policy.
7. Add stable margin-rejection latching.
8. Add conservative minimum-notional headroom without distorting sizing.
9. Add UNKNOWN-safe monotonic reductions.

### P2 — make the product diagnosable

10. Unify action/fail-close metrics.
11. Coalesce monitoring state instead of dropping a FIFO.
12. Recompute terminal status.
13. Fix action-mux future cleanup.
14. Add equity age, leverage, reduction slippage, and no-action reason evidence.

### P3 — measure economics after correctness

15. Re-evaluate delayed persistent targets by leader/market style.
16. Measure completion ratio, missed notional, price deviation, and tracking
    error.
17. Consider deeper L2 sizing only if fresh-depth evidence shows a material
    economic benefit.

## Required validation before another unattended run

### Targeted direct-path tests

- 468-market catalog, JIT activation, eviction, and reconnect overlap.
- New HIP-3 market introduced while the process is running.
- Fatal market/source/action transport with nonflat followers.
- Closeout requested before the first follower snapshot.
- Simultaneous operator stop and restored fail-close latch.
- UNKNOWN entry plus unrelated reductions and a late fill.
- Actual 1× follower market with policy/venue maximum 10×.
- Repeated unchanged margin-impossible target.
- $10 boundary, exact subminimum full close, and residual accumulation.
- Partial IOC followed by a new-liquidity retry.
- Analytics load competing with emergency info traffic.
- Final status after all tasks terminate.

### Broad validation

- Full repository tests.
- Ruff and mypy over production and tests.
- Deterministic replay of the WLD ambiguity and SKHX rejection storm.
- Fault-injected production driver run with no exchange actions.
- Monitor-only ten-slot startup and repeated socket reconnects.

### Live gate

Do not use another generic 12-hour duration as the primary acceptance unit.
After the direct defects pass deterministic and monitor-only validation, use a
small-capital supervised canary with explicit current authorization.

The canary must demonstrate:

- at least one dynamically activated previously inactive market;
- at least one market reconnect with overlap-safe capacity;
- one verified leverage setup before entry;
- one reduction while an unrelated ambiguity is injected or reproduced safely;
- one planned single-owner closeout;
- zero duplicate CLOIDs;
- zero blind retries;
- zero preventable margin/minimum/precision rejects;
- terminal flatness and dead signer process.

Duration alone is not sufficient.

## UI/operator requirements resulting from this run

The UI should expose, without becoming a hot-path dependency:

- eligible catalog count versus active working-set count;
- current subscription count, reconnect reserve, and pending changes;
- actual versus desired follower leverage and margin mode;
- available margin or “unknown”;
- current target, target age, and waiting reason;
- ambiguous CLOIDs and which reductions remain permitted;
- closeout owner/ID and progress;
- emergency state and exact blocker;
- latest terminal action details;
- analytics budget usage separately from critical execution capacity.

An error process with open exposure must remain visibly `EMERGENCY_BLOCKED`, not
look like an ordinary stopped website.

## Process guidance for the next agent

1. Start from the raw evidence and the first incorrect state transition.
2. Change the smallest coherent ownership boundary that fixes the root cause.
3. Keep dynamic eligibility and faithful target sizing.
4. Do not introduce a new supervisor, queue, or fallback unless it replaces the
   failed mechanism rather than stacking beside it.
5. Make every recovery path prove both:
   - it blocks new risk when truth is uncertain;
   - it still closes independently provable exposure.
6. Treat tests as regression evidence, not as the definition of correctness.
7. Do not launch another long ARMED run until the P0/P1 direct-path gates pass.
8. If a fix fails, repair the first reproduced production-path failure; do not
   restart a whole-repository redesign.

## External contract references

Retrieval checked on 2026-07-25:

- [Hyperliquid rate limits and WebSocket ceilings](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
- [Hyperliquid WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Hyperliquid WebSocket reconnect guidance](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Hyperliquid error responses](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses)
- [Hyperliquid exchange endpoint and updateLeverage](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
- [Hyperliquid orderStatus by CLOID](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [NautilusTrader Hyperliquid integration](https://nautilustrader.io/docs/latest/integrations/hyperliquid/)

## Completion definition

This incident is closed only when:

- subscription planning survives realistic reconnect overlap;
- a fatal transport cannot silently strand follower exposure;
- UNKNOWN never permits duplicate risk and never indefinitely blocks provable
  reductions;
- follower leverage/margin is explicit and verified;
- closeout has one owner and no startup assertion hole;
- deterministic margin and minimum-notional rejections do not loop;
- final status and action metrics describe the truth;
- all relevant validation passes on the production path.

Until then, the successful recovery is evidence of recoverability with
operator help—not unattended production readiness.
