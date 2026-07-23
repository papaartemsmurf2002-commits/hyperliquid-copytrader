# Historical data and candidate coverage

## Address-analysis inputs

| Input | Coverage/limit | Honesty behavior |
|---|---|---|
| `userFillsByTime` | Maximum 2,000 rows per response and only the 10,000 most recent fills are available; local `max_pages` is also bounded | Reports page count, truncation, first/last fill and partial window volume/PnL provenance |
| Portfolio window histories | Venue-provided named windows; may not align exactly with the requested arbitrary start/end | Records upstream scope and coverage start/end; does not relabel all-time as exact arbitrary-window truth |
| Clearinghouse state | Current positions/account value, not a historical ledger | Current account value source/confidence/fallback is explicit |
| Account-value history | Sparse observations carried forward only after a real baseline | No future backfill; carried points marked; residual unavailable until next observation |
| Funding/non-funding ledger | Not joined into the current compact address report | Residual is explicitly not a confirmed transfer; independent accounting oracle remains absent |
| HIP-3 universe | Dynamic `perpDexs`/metadata | No fixed DEX count or asset-ID list; immutable revision per run |

## Metric provenance

- Closed-fill net PnL, fee, volume, win/loss and trade drawdown derive from compacted fills and inherit fill truncation limits.
- Portfolio/account-value metrics use upstream history and expose scope/partial flags.
- `account_value_residual_usd = account-value change - compacted closed-fill net PnL` is ambiguous and may contain unrealized PnL, funding, fees, transfers, and other effects. `residual_cashflow_confirmed=false` is invariant.
- Daily buckets use one right-closed convention; no observation is emitted before the first value at/before the represented time.

## Candidate-ranking boundary

There is no frozen complete historical leader universe, honest “top 100” naming contract, independent ledger oracle, cost/slippage/fill simulation, held-out target, rank-stability study, or post-selection copyability result sufficient for a predictive-ranking claim. The product currently supports bounded address inspection, not verified leader selection or alpha prediction.

Required before such a claim:

1. freeze universe inclusion/exclusion and coverage dates;
2. persist source/funding/ledger/fill watermarks and truncation;
3. define realizable follower fills, fees, latency, caps and missing-market treatment;
4. separate training/selection/held-out periods with no future leakage;
5. publish rank stability, uncertainty, losing cases and tracking-error distribution.
