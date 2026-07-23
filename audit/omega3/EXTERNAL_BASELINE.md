# External source-level baseline

Inspected through 2026-07-18. No third-party code was executed or copied. `NO LICENSE` means no reuse permission was found; concepts can still be independently reimplemented. Closed-product claims are not correctness or latency evidence.

## Primary execution/recovery comparators

| Source / exact revision | License | Inspected evidence | Decision |
|---|---|---|---|
| [Official Python SDK 0.24.0](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/tree/2fdb18f9517675ea03695a0962bd19eece9c83f0), `2fdb18f9517675ea03695a0962bd19eece9c83f0`, 2026-06-04 | MIT | `exchange.py`, `utils/signing.py`, `websocket_manager.py`, `utils/types.py` | **Adopt** official signing/action, `vaultAddress`/`expiresAfter`, dynamic HIP-3 and precision semantics. Local durable cloid/UNKNOWN/WS-post transport is **already stronger**; SDK still posts actions by HTTP. |
| [Official node](https://github.com/hyperliquid-dex/node/tree/91f3ca8745d07a72b0c40c62a9d17d1a9771bdf5), `91f3ca8745d07a72b0c40c62a9d17d1a9771bdf5`, 2026-07-01 | Apache-2.0 | README and output/local-state surfaces | **Adapt** integrity/state-ownership concepts; **reject** local-node deployment absent measured need. |
| [Official illustrative order book server](https://github.com/hyperliquid-dex/order_book_server/tree/8b4f237904f683aca2dba21a07d87e831ead2a97), `8b4f237904f683aca2dba21a07d87e831ead2a97`, 2026-07-15 | No license | README; WebSocket server/listeners/order-book state. Exits on node inactivity or reconstructed/snapshot mismatch. | **Adapt** fail-fast invariant only; **reject** deployment/reuse. README says illustrative/non-core/no warranty. |
| [MaxIsOntoSomething/Hyperliquid_Copy_Trader](https://github.com/MaxIsOntoSomething/Hyperliquid_Copy_Trader/tree/78858611fd418bab5eb0e558ecd1145b3d66bc12), `78858611fd418bab5eb0e558ecd1145b3d66bc12`, 2026-06-03 | MIT | Direct Python copy-trader comparator: `main.py`, `copy_engine/monitor.py`, `copy_engine/executor.py`, `hyperliquid/websocket.py`. It sends HTTP `/exchange` actions with no CLOID, deliberately skips close/reduce fills, and has no durable action journal, gap backfill, or restart reconciliation. | **Reject** reuse/execution claims; this direct comparator is **weaker** on identity, close handling, ambiguity containment, durability, and recovery. Adapt only basic proportional-sizing and WS-resubscribe concepts. |
| [NautilusTrader develop](https://github.com/nautechsystems/nautilus_trader/tree/3ffa0ca4bea7876a7f78f8799fd73426059a097a/crates/adapters/hyperliquid), `3ffa0ca4bea7876a7f78f8799fd73426059a097a`, 2026-07-18; release v1.230.0 is `8160730c7c550480b0a439fb11086a4c4de15f0b` | LGPL-3.0 | Hyperliquid `websocket/client.rs`, `websocket/post.rs`, `data.rs`, `execution.rs`: WS POST, heartbeat/reconnect, targeted resubscribe, pre-send cloid, ambiguous transport, cloid-before-oid query | **Adapt** ownership/reconnect/status-race patterns; **reject** framework integration. Local product-specific durable pre-send journal, signer actor, and explicit UNKNOWN containment remain stricter for this deployment. |
| [hyper-trader](https://github.com/0xtitan6/hyper-trader/tree/6d84db0a4c5c55cdbe0f8e96c9fee9b321390978), `6d84db0a4c5c55cdbe0f8e96c9fee9b321390978`, 2026-07-05 | No license | `journal.py`, `mirror.py`, `ws_health.py`, `connection.py`, alerts/tests. Useful seen-tid/backfill/submit-lock ideas; tid-only identity and submit/journal gap are weak. | **Adapt** concepts only; local composite fill identity/durability/restart reconciliation is **already stronger**. |
| [origamidottech trading bot](https://github.com/origamidottech/hyperliquid-trading-bot/tree/1b1357c711c61158ca092a92448fa5918e114708), `1b1357c711c61158ca092a92448fa5918e114708`, 2026-07-07 | No license | `orderExecutor.ts`, `fillProcessor.ts`, `bot.ts`: live order blindly retried three times; snapshots skipped/no backfill | **Reject** execution/reuse; **adopt as negative regression seed** for timeout-filled-then-retry duplicate and snapshot loss. |
| [Gajesh copytrading-agent](https://github.com/Gajesh2007/copytrading-agent/tree/5422835ea77acdd8ef2867814b38f6c921ec2c40), `5422835ea77acdd8ef2867814b38f6c921ec2c40`, 2025-12-25 | No license | Reconciler/executor/subscriptions/client/domain: useful desired state, but one sync flag and random build-time cloid without persistence/orderStatus recovery | **Adapt** desired-state simplicity; local implementation is **already stronger**. |

## Analytics/UI/data comparators

| Source / exact revision | License | Inspected evidence | Decision |
|---|---|---|---|
| [Trading Strategy DuckDB trade history](https://github.com/tradingstrategy-ai/web3-ethereum-defi/blob/1c0c0b6797eba716f85b4d40925a00a7263e1e77/eth_defi/hyperliquid/trade_history_db.py), `1c0c0b6797eba716f85b4d40925a00a7263e1e77`, 2026-07-16 | MIT | Separate fills/funding/ledger tables, per-kind watermarks/checkpoints, truncation disclosure | **Adapt** analytics schema/checkpoint ideas; keep execution journal separate. |
| [Flowsurface](https://github.com/flowsurface-rs/flowsurface/tree/9d2d8eb7f2e673b117379b2309fae4b551b5c769), `9d2d8eb7f2e673b117379b2309fae4b551b5c769`, 2026-07-12; release v0.8.9 `0e97d60f0894a853548cb7a551bc1d6bfc36391c` | GPL-3.0 | README/Cargo/exchange/UI/data tree | **Adapt** visualization concepts only; **reject** code/integration and execution inference. |
| [Hypertrack project](https://github.com/ssanin82/hyperliquid-copy-trader/tree/f3aa9153e05312ce28f18e5f14137d89af51d24e), `f3aa9153e05312ce28f18e5f14137d89af51d24e`, 2026-01-24 | MIT | Recorder/model/frontend and BBO/candle/L2/trade recorders | **Adapt** recorder/JSONL ideas; **reject** as execution comparator. |
| [Hypelens](https://github.com/Bortlesboat/hypelens/tree/31f7ca8bb6c2e31c0a15a3b5085d762e47decc6c), `31f7ca8bb6c2e31c0a15a3b5085d762e47decc6c`, 2026-05-11 | MIT | Analytics tree | **Adapt** inspectable analytics patterns; **reject** as execution truth. |
| [hyperliquid-tracker](https://github.com/mgalihpp/hyperliquid-tracker/tree/11e11446523ad7a5ceea24f75911d7f8b2eb36b7), `11e11446523ad7a5ceea24f75911d7f8b2eb36b7`, 2026-03-20 | No license | Tracker source/README | Reference only; **reject** reuse/execution evidence. |
| [HyperliquidWalletAnalyzer](https://github.com/PotluckProtocol/HyperliquidWalletAnalyzer/tree/25eaf78cfd58806e880079645edecc0cf392548f), `25eaf78cfd58806e880079645edecc0cf392548f`, 2026-01-28 | No license | Wallet-analysis source/README | Reference only; **reject** reuse/execution evidence. |
| [Official stats-web fork](https://github.com/hyperliquid-dex/hyperliquid-stats-web/tree/78cd9c2473da3c7d9872af28c77d85f0342b8c76), `78cd9c2473da3c7d9872af28c77d85f0342b8c76`, 2024-12-02 | MIT | Calculation/UI tree | **Adapt** display ideas only; **reject** as current contract/execution authority. |

## Transport/SDK cross-checks

| Source / exact revision | License | Decision |
|---|---|---|
| [Official Rust SDK](https://github.com/hyperliquid-dex/hyperliquid-rust-sdk/tree/aac75585daf12d0a3761126cc7da7a5e035b5853), `aac75585daf12d0a3761126cc7da7a5e035b5853`, 2025-10-21 | MIT | **Reject** migration without measured Rust need; stale against current docs. |
| [hypersdk](https://github.com/infinitefield/hypersdk/tree/a7dc347fea8e16bb60e3d250dee839b6151c9eed), `a7dc347fea8e16bb60e3d250dee839b6151c9eed`, 2026-07-15, observed tag v0.2.14 not proven at HEAD | MPL-2.0 | **Adapt** typed contract-test ideas if Rust is ever justified; **reject** migration now. |
| [Official TS examples](https://github.com/hyperliquid-dex/ts-examples/tree/1ba2704bead1f23c8790e1dd8ad5205af6d8f43e), `1ba2704bead1f23c8790e1dd8ad5205af6d8f43e`, 2025-02-11 | No license | Payload reference only; **reject** reuse/current authority. |
| [nktkas/hyperliquid](https://github.com/nktkas/hyperliquid/tree/0a386109e17b0cb5e01349d9ebbbdebcf9cf22e3), `0a386109e17b0cb5e01349d9ebbbdebcf9cf22e3`, 2026-07-15 | MIT | **Adapt** typed HTTP/WS schema tests; **reject** JS/TS migration. |
| [nomeida/hyperliquid](https://github.com/nomeida/hyperliquid/tree/eb21422ab6ae9b466bcf862f58f380e632be939f), `eb21422ab6ae9b466bcf862f58f380e632be939f`, commit 2025-11-05 | No license | Reference only; **reject** reuse/current authority. |
| [HyperLiquid.Net](https://github.com/JKorf/HyperLiquid.Net/tree/e4301681d932fedd96046cdb293a5778c324c2fb), `e4301681d932fedd96046cdb293a5778c324c2fb`, 2026-07-13 | MIT | **Adapt** rate-limit/Windows transport patterns if measured useful; **reject** .NET migration. |

## Closed/documentation-only products

| Product | Inspectable boundary | Decision |
|---|---|---|
| [Hyperdash exposure copying](https://docs.hyperdash.info/strategic-copytrading-hyperdash/how-exposure-based-copytrading-works) | Product docs only; no public execution SHA/license | **Adapt** desired-exposure/reweight/manual-override semantics; **reject** performance/correctness claims. |
| [pvp.trade docs](https://github.com/pvptrade/docs/tree/f786073e9c1b6e55514c02db670f5810a1a7542d), `f786073e9c1b6e55514c02db670f5810a1a7542d`, 2025-05-04 | Docs repo, no license; no public engine | **Adapt** UX/group-copy ideas; **reject** execution evidence/reuse. |
| [Hydromancer docs](https://docs.hydromancer.xyz/hydromancer-better-hyperliquid-apis/websocket/userfills) | Closed server; userFills/session replay docs | **Adapt** replay/live overlap, cursor/gap ideas; **reject** latency/dependency claims. Official semantics override its apparent `scheduledCancel` classification. |
| [Dwellir L1 gateway](https://www.dwellir.com/docs/hyperliquid/grpc) | Paid closed server; 24h bounded retention docs | **Adapt** timestamp/block resumption concept; **reject** default dependency. |
| AlgoInfraTech/Hyperliquid-copy-trading-bot | Repository returned 404 on 2026-07-17 | **Reject/unavailable**; no old claims retained. |

## Synthesis

The useful external ideas are narrow: official signing/schema authority; actor ownership; reconnect/status-race tests; desired-exposure semantics; fail-fast invariants; and separate analytics watermarks. None supplies a reusable proof for this product's one-signer-per-follower durability, UNKNOWN containment, ten-user capacity edge, or native-Windows full-fleet performance. Migration to another language/framework, a local node, or a closed gateway is rejected without measured benefit.
