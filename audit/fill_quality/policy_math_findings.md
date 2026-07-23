# Execution-policy quantitative findings

This note isolates the execution-policy decision from the wider runtime audit. It
uses the durable `continuous-ten-mainnet-v2` and `v3` action journals, the
bounded public leader/follower fill extracts in this directory, a five-minute
read-only L2/trade sample, and a separate 89-second read-only comparison of the
`bbo`, `l2Book`, and `activeAssetCtx` WebSocket feeds.

## Decision objective

For desired follower position `D(t)` and actual position `P(t)`, the relevant
loss is not slippage alone:

```text
execution loss
= filled quantity * adverse leader-to-follower price move
+ fees
+ lambda_target * integral(abs(D(t) - P(t))) dt
+ lambda_wrong * integral(unwanted/wrong-side exposure) dt
+ lambda_action * submitted action count
+ lambda_unknown * unresolved-action duration
```

The coefficients depend on the leader's alpha/holding horizon and cannot be
estimated honestly from this small sample. Therefore policy selection should be
lexicographic:

1. Never duplicate an action or trade through unresolved exchange truth.
2. Never increase risk beyond a hard leader-relative economic price boundary.
3. Within those constraints, minimize target-error area and unwanted-exposure
   duration.
4. Then minimize fees and action count.

This prevents a superficially low-slippage policy from winning merely because
it often does not fill.

## Durable-history evidence

- 272 actions: 192 filled, one partial fill, 79 rejected.
- Of the 193 actions with any fill, 192 filled completely. The sole partial was
  a CASHCAT entry that filled 5 of 148 requested units. Thus the observed
  failure mode was overwhelmingly binary marketability, not routine depth
  exhaustion, at the current canary size.
- 233 distinct desired-action groups. Entry groups eventually filled 92/130
  (70.8%); reduction groups filled 100/103 (97.1%).
- 49 actions were explicit IOC no-liquidity outcomes. The largest clusters were
  CASHCAT (29), `xyz:CXMT` (9), and PUMP (8). Another 25 were server-response
  failures and must not be attributed to pricing.
- Seventeen desired groups had multiple attempts: 56 actions in total, or 39
  attempts beyond the first (14.3% of all actions). Only seven of those 17
  groups eventually filled; ten never filled. Nineteen rejected attempts
  preceded the seven eventual fills.
- A terminal action occupied its slot for 945 ms at the median; a rejected
  action occupied it for 923 ms. The 39 extra attempts therefore consumed
  roughly 36 seconds of aggregate slot-lane time, before counting the economic
  cost of remaining off target.
- Among 78 filled entry actions that could be matched heuristically to a
  same-side leader fill in the preceding ten seconds, adverse follower price
  was 0.81 bps at p50, 17.28 bps at p90, and 24.11 bps at p95. Three fills
  exceeded 50 bps; all three were CASHCAT actions created 6.77-9.92 seconds
  after the matched leader fill. A 50 bps leader-relative cap would have
  rejected these stale tail executions while retaining the remaining 75/78
  matched fills.
- Excluding server failures, matched entry fill rate rose sharply with the
  order's adverse price headroom relative to the matched leader fill:

| Headroom | N | Observed fill rate |
|---:|---:|---:|
| <= 0 bps | 17 | 5.9% |
| 0-10 bps | 7 | 28.6% |
| 10-20 bps | 15 | 80.0% |
| 20-25 bps | 25 | 84.0% |
| 25-35 bps | 33 | 93.9% |
| 35-50 bps | 7 | 100% |
| > 50 bps | 4 | 100% |

This is observational rather than randomized evidence, but it directly shows
that a limit which lands behind the live offer/bid is the principal IOC failure
mode in the thin-market clusters.

Wilson 95% intervals are wide because the sample is small: 84.0% at 20-25 bps
has an interval of about 65.3-93.6%, while 93.9% at 25-35 bps has an interval of
about 80.4-98.3%. The monotone pattern is decision evidence, not a precise fill-
rate forecast.

## Live market evidence

Five minutes produced 57 L2 snapshots per market. Median L2 snapshot cadence
was about 5.38 seconds. A separate simultaneous channel sample showed:

| Feed/market | Median event interval |
|---|---:|
| `activeAssetCtx`, all five markets | about 1.014 s |
| `l2Book`, all five markets | about 5.39 s |
| `bbo`, BTC | 109 ms |
| `bbo`, CASHCAT | 278 ms |
| `bbo`, `xyz:CXMT` | 299 ms |
| `bbo`, `xyz:KR200` | 885 ms |
| `bbo`, `xyz:CL` | 126 ms |

`bbo` is event-driven and can remain silent when the quote does not change, so
age alone is not proof that its state is stale. Connection/subscription health
and gap/reconnect state are authoritative; the periodic L2 refresh can detect a
missed state transition.

Median spread was 0.15 bps for BTC, 33.52 bps for CASHCAT, 13.24 bps for
`xyz:KR200`, 17.90 bps for `xyz:CXMT`, and 0.47 bps for `xyz:CL`. CASHCAT's
maximum observed spread was 130.95 bps. Consequently, mark/mid + 25 bps leaves
only about eight bps beyond the median CASHCAT ask and can be behind the ask
entirely during wide-spread states.

For a $12 order, a static replay of the captured books (which cannot model
sub-snapshot motion) made the current mid +/- 25 bps plus visible-depth chunking
fully executable in only 84.8% of CASHCAT states and 97.0% overall. A BBO-
anchored 25 bps IOC was executable in all captured static states. This result is
directional, not a forward fill-rate forecast.

Even these thin books usually had ample size for the current canary:

| Market | p10 top-of-book notional | p10 notional within 10 bps of BBO |
|---|---:|---:|
| CASHCAT | $16.5 | $112.1 |
| `xyz:KR200` | $31.5 | $417.0 |
| `xyz:CXMT` | $67.3 | $1,785.7 |
| `xyz:CL` | $100.0 | $421,413 |
| BTC | $2,438.5 | $2,399,553 |

The median durable requested notional was $12.20. Full depth-walking is useful
as an impact estimate and becomes more important when capital grows, but it is
not the current primary failure mechanism.

Across the coarse 5.38-second L2 intervals, p95 adverse BBO movement was 24.27
bps for CASHCAT, 5.97 bps for `xyz:CXMT`, 4.40 bps for `xyz:KR200`, 3.04 bps for
`xyz:CL`, and 1.81 bps for BTC. CASHCAT had an 85.54 bps maximum outlier. This
supports a 50 bps entry boundary as a useful initial economic cap, not as a
guarantee or a permanently optimal constant.

## Policy decision

### Risk-increasing entry

Use the latest-target model and a fresh event-driven BBO. Let the latest
accepted same-side leader fill define a hard adverse boundary (initially the
existing 50 bps tracking policy). If the current opposite BBO is already beyond
that boundary, retain the target and wait for a new market/leader state rather
than submit or discard it.

If the current book is acceptable, an IOC limit at the hard boundary weakly
dominates a narrower IOC limit: in every book state where both orders fill,
they receive the same best resting prices; in states where only the wider order
fills, every accepted price is still inside the explicit economic boundary.
The limit is a ceiling/floor, not the expected fill price. L2 should validate
expected VWAP/impact and expose uncertainty, not silently shrink a valid order
into a subminimum child.

This is preferable to NautilusTrader's generic BBO + 50 bps default for this
copying problem. BBO + 50 bps can chase 50 bps beyond a BBO which was already
materially worse than the leader. The leader-relative boundary preserves the
economic contract.

### Reduction, reversal, and emergency exit

- A normal pure reduction should be reduce-only and use a wider independent
  BBO-relative boundary (an initial 100 bps is defensible). Leader entry price
  is not the correct cap because remaining unwanted exposure has its own cost.
- A reversal should close the wrong side first under the reduction policy,
  reconcile, and only then open the new side under the entry policy.
- An emergency flatten may use a still wider bounded envelope (for example 300
  bps), but must remain reduce-only and operator/risk-policy explicit.

These are separate economic contracts and should not share mark +/- 25 bps.

### Retry and residual behavior

After a terminal IOC no-fill or partial fill, fold the real fill once, recompute
the residual from the newest desired and actual state, and attempt again only
on a newer source revision or meaningful post-result liquidity evidence. An
unchanged periodic L2 timestamp is not evidence. Identical-looking liquidity
may retry only after a post-result observation and bounded 1/2/5-second
backoff. Preserve the original CLOID until terminal truth is known; a new
terminal retry gets a new CLOID.

The prior five-second-only retry was too slow when the BBO became executable
sooner, while timestamp-only hot retries on an unchanged book wasted actions
and serialized the slot. Meaningful-state retry with bounded fallback handles
both cases and prevents an older in-flight response from throttling a newer
source revision.

### Order size and depth

If the intended order itself passes the venue minimum, submit the intended
bounded IOC size and allow the venue to return a partial fill. Do not reduce the
submitted quantity solely to the stale visible L2 amount; that can convert a
valid order into a locally rejected subminimum child. Reserve risk for the full
submitted amount and replan the terminal residual.

A one-sided book remains valid venue state. If the executable side required for
a risk reduction exists, the absent opposite side must not block that
reduce-only action; an entry still waits when its required side is absent.

### Maker/chase/TWAP

Do not use maker-first, UI-style chase, or native TWAP as the default copy path.
The recorded median taker fee was 4.5 bps, only about $0.0055 on the median
$12.20 action. Saving that fee does not compensate for another queue/block
cycle, queue-position uncertainty, adverse selection, and increased
target-error area when the source signal is already 0.5-2.5 seconds old. These
mechanisms may be useful later for large, persistent, slow-moving targets, but
the available data cannot establish that regime.

## Evidence limits

- Leader-to-action matching is nearest same-market/same-side matching within ten
  seconds, not a stored causal trigger identity.
- The live sample is only five minutes and L2 updates are coarse. It cannot
  estimate a stable market-specific optimum or claim a future fill rate.
- No order-queue data exists, so maker/chase performance cannot be simulated
  fairly.
- Desired-position history is not complete enough to calculate exact target-
  error area; the action/retry counts and durations are proxies.
- The 50/100/300 bps values are defensible initial boundaries. They should be
  recalibrated from post-change action-linked BBO, expected-VWAP, fill, and
  residual metrics rather than optimized against this small sample.
