# Hyperliquid contract matrix

Verified against official documentation/source during the audit. The runtime must recheck drift before a future release.

| Contract | Runtime decision |
|---|---|
| 10 unique users across user-specific subscriptions per IP | Use all slots for leaders; follower info uses WS POST, not subscriptions. |
| 10 WebSocket connections, 1000 subscriptions | Three steady sockets; validate total source + market subscriptions before start. |
| 30 new WebSocket connections/minute per IP | One shared runtime limiter at 24/minute, leaving startup headroom. |
| 2000 outbound WS messages/minute, 100 inflight posts | Project follower reconciliation before start; action writes have priority over info writes. |
| HTTP info budget 1200 weight/minute | Normal continuous loop does not use it. Startup catalog is three calls/weight 60. |
| WS POST supports `info` and signed `action` requests | Use it for follower truth, order status, and IOC actions. |
| Nonce is tracked per signer | One API wallet/journal/lock per follower lane. |
| Query address is account/subaccount, not API wallet | Bind query identity separately from signing identity. |
| API wallets can operate subaccounts/vaults when authorized | Preflight exact role, owner, action principal, and signing vault. |
| Order response can be ambiguous or item-level rejected | Durable CLOID/send boundary; classify each result; resolve unknown before retry. |
| `userFillsByTime` is bounded | Use only bounded gap/history recovery; never treat it as an unlimited analytics source. |
| Market precision/tick/lot rules are dynamic | Build a current catalog and quantize at the final action edge. |

Official references: Hyperliquid WebSocket subscriptions, WebSocket post requests, rate limits, info endpoint, nonce/signing documentation, and current official SDK source.
