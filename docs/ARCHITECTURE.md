# Architecture

## Product contract

One local Windows operator selects up to ten leaders and maps each to one follower subaccount/API wallet. The engine copies proportional target exposure, not raw leader leverage and not every leader order instruction. Each slot has explicit market, order, gross-exposure, leverage, open-position, and action-rate caps.

## Runtime topology

```text
leader user streams ----> source socket ----> per-slot source reducer
                                                   |
public book/context ----> market socket -----------+----> target/risk decision
                                                   |
follower truth <------ action/info WS POST <-------+----> durable IOC journal
                                                   |
follower IOC order ---- action/info WS POST <-------+
```

There are three steady sockets:

1. Source socket: all user-specific subscriptions for one to ten unique leaders.
2. Market socket: public context and L2 subscriptions for configured markets.
3. Action/info socket: action-priority WebSocket POST requests, follower reconciliation, and order-status resolution.

The unique-user subscription allowance is used only by leaders. Follower account queries are request/response posts and do not consume those slots.

## State ownership

- `AccountStream` validates snapshots, deduplicates fills, enforces position continuity, and never performs network I/O.
- `MarketStream` owns current catalog/book/context truth.
- `ContinuousRuntime` owns desired, attributable, follower, pending, and operator state for each slot.
- `ContinuousSignerLane` owns exactly one follower/API-wallet signer and nonce lane.
- `ActionJournal` durably records CLOID, nonce, signed payload, send boundary, cumulative fill, and terminal/unknown state.
- `ContinuousNetworkDriver` owns socket lifecycle, per-slot queues, reconnects, source actors, and follower refresh scheduling.

Desired leader state, pending actions, and authoritative follower state are distinct. A timeout after the send boundary is `UNKNOWN`; it is resolved by CLOID before any replacement action.

## Transport rules

- Normal leader detection: WebSocket only.
- Normal follower order/cancel/status/account queries: WebSocket POST only.
- Startup HTTP: exactly three public catalog reads, total documented weight 60.
- Historical/gap HTTP fallback: disabled by default and available only through an explicit recovery option.
- No continuous HTTP polling loop and no HTTP action path.

One shared reconnect-admission window caps all three runtime sockets at 24 new attempts per minute, leaving headroom below the venue's documented 30/minute IP limit.

## Recovery

Source attribution is persisted before an action can be signed. The slot lock spans action execution through follower fold/state persistence. On restart, the engine restores source attribution, runtime state, journals, and gap cursors, then obtains authoritative follower truth before allowing new risk.

Unexplained existing follower exposure blocks new risk. Exact durable known exposure can resume. Reduce-only work is prioritized, but shutdown never guesses that flattening is universally safe.

## Removed architecture

The former FleetRuntime, supervisor, guardian, attestor, launch authority, REST coordinator, synthetic context benchmark, and alternate execution gateway were deleted. They are not compatibility paths and are not part of the shipped runtime.
