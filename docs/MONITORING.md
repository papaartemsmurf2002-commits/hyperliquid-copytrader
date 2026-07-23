# Monitoring

Each generation writes `status.json` and `metrics.jsonl`. Durable engine state and action journals live in the stable engine directory.

## Status fields

- `status`: `starting`, `running`, `stopped`, `cancelled`, or `error`.
- `arm_requested`: whether the exact arm token was supplied.
- `execution_enabled`: false until an armed runner owns the engine; false again after stop.
- `startup_http_requests`: must be 3.
- `startup_http_weight`: must be 60.
- `normal_rest_enabled`: must be false.
- `recovery_rest_enabled`: normally false.
- `slots.<id>.state/reason`: operational slot state and concrete blocker.
- `monitoring_dropped`, `source_backlog_discarded`, `metrics_dropped`: must remain zero.

## Metrics that matter

- `startup_complete`: transport/budget summary.
- `source_reduce`: queue delay, receive-to-reduce latency, and acceptance.
- `follower_refresh`: authoritative follower refresh timing and result.
- `socket_connect_attempt`: shared connection-attempt accounting.
- `socket_connect_throttle`: reconnect budget protection activated.
- `socket_reconnect`: an actual reconnect attempt after a socket failure.
- `recovery_http`: explicit HTTP recovery fallback was used.
- `monitoring_drop`: status/event observation overflow.

`connection_gap` emitted at graceful socket detach is not itself a reconnect. Use `socket_reconnect` plus status/epoch evidence to diagnose an actual failure.

## Known two-account baseline

Two 60-second mainnet monitor runs produced 56 and 52 source frames, two follower refreshes each, zero actions/reconnects/recovery HTTP/drops, and maximum receive-to-reduce latency of 4.7194 ms and 4.6668 ms. These are reducer measurements, not leader-event-to-order or exchange acknowledgement latency.
