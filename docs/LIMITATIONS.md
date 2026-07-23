# Limitations

- No naturally occurring fresh leader fill has yet traversed source receive, reduction, sizing, reservation, WebSocket order submission, acknowledgement, and terminal reconciliation in one continuous run.
- Four bounded mainnet IOC samples are too few for p95/p99 or consistency claims. Local decision-to-send was 7.77-15.25 ms; exchange response was 858-981 ms.
- The real monitor/restart proof covers two slots for about 60 seconds each, not ten slots for hours.
- Ten leaders exactly consume the documented unique-user subscription allowance; candidate switching has no temporary eleventh-user overlap.
- Optional HTTP gap fallback has not been exercised in the healthy soaks and remains disabled by default.
- Ranking is bounded by the explicitly observed address universe/history. Predictive superiority and follower profitability are not established.
- Windows is the only production target tested. This is intentional; no measured blocker justifies another platform.
