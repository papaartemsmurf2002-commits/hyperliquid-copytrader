# Two-account mainnet WebSocket action proof

Date: 2026-07-18

## Verdict

The bounded `acc7` / `acc1` signer and order-lifecycle proof completed. Both followers were independently verified flat and order-free across the catalog, and the stable journal contains four terminal `FILLED` actions with zero unresolved rows.

This proves the bounded WebSocket POST mutation and reconciliation lane. It does not by itself prove the continuous leader-event-to-order path, ten-slot capacity, tracking error, or profitability.

## Scope

- Runtime/plan: `continuous-v1-proof`, mainnet.
- Raw plan SHA-256: `93bc2917bac3799fd1bfbd7a48d04e8f93c6cb9bd358aa5ae973c67e2ed48be4`.
- Bound-plan SHA-256: `05ffa0b55573523f611a478b8234d4cd2d9d1d38c86ecbcfc9ffffb2003769f9`.
- Maximum order: USD 12.
- Maximum follower gross: USD 15.
- Maximum combined gross: USD 30.
- One market per follower.
- Mutation transport: signed WebSocket POST IOC only.
- HTTP `/exchange` action count: zero.
- No transfer, withdrawal, approval, leverage-change, or unrelated account capability.

## Actions and latency

| Slot | Market | Phase | Reduce-only | Filled size | Notional USD | Action-call to socket-send ms | Socket-send to response ms | Result |
|---|---|---|---:|---:|---:|---:|---:|---|
| acc7 | xyz:EWY | entry | no | 0.069 | 11.16144 | 12.9566 | 980.5405 | FILLED |
| acc7 | xyz:EWY | recovery close | yes | 0.069 | 11.10762 | 15.2501 | 956.3964 | FILLED |
| acc1 | BTC | entry | no | 0.00018 | 11.65104 | 13.9181 | 974.8452 | FILLED |
| acc1 | BTC | close | yes | 0.00018 | 11.59092 | 7.7723 | 858.2787 | FILLED |

Four observations are not enough for p95/p99. `Action-call to socket-send` is not leader-event-to-order latency.

## Incident and recovery

The first `acc7` entry filled. Its close was then blocked because the same one-second book-age rule was applied after an approximately 981 ms action acknowledgement. Terminal WebSocket truth showed exactly `xyz:EWY +0.069` and no open orders. The runner stopped without retrying or flattening blindly.

Recovery was separate and provenance-bound. It required the original plan/catalog/manifest, the single durable filled entry, zero unresolved journal rows, exact all-DEX position equality, and both account/signer locks. It sent one exact reduce-only WS IOC for 0.069 EWY and re-read exchange truth.

The current proof/runtime logic keeps strict freshness for entries and gives a provable reduce-only close a bounded asynchronous recovery-book window. The `acc1` continuation then completed entry and close successfully.

## Terminal truth

- `acc7`: zero nonzero positions; zero open orders.
- `acc1`: zero nonzero positions; zero open orders.
- Journal: four `FILLED`; zero `PREPARED`, `SEND_ATTEMPTED`, `UNKNOWN`, or `RESTING`.
- No profitability claim is made from the small round trips.

## Raw artifacts

- Proof and recovery: `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-v1\proof-20260718T190008705Z`
- Passed `acc1` continuation: `continuation-acc1-1784402358198`
- Stable journal: `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-v1\mainnet-proof-actions.sqlite3`
- Redacted preflight: `audit/omega3/live/mainnet-two-account-preflight-20260718.json`

## Remaining boundary

The real continuous runner has separately proven shared-reader isolation, local gap/restart behavior, all-DEX follower reconciliation, and two live monitor/restart soaks. The remaining action-path proof is one naturally occurring fresh leader fill through source reduction, sizing, reservation, WebSocket action, and terminal reconciliation.
