# Two-account continuous mainnet monitor soak

Date: 2026-07-18

Two read-only runs used the same stable engine directory. Neither supplied the arm token, opened signer-key content, enabled HTTP recovery, or created an action row.

| Metric | First run | Restart run |
|---|---:|---:|
| HTTP catalog calls / weight | 3 / 60 | 3 / 60 |
| WS startup info posts | 20 | 20 |
| Source frames reduced | 56 | 52 |
| Follower refreshes | 2 | 2 |
| Max queue delay ms | 4.5678 | 4.5254 |
| Max receive-to-reduce ms | 4.7194 | 4.6668 |
| Actions | 0 | 0 |
| Socket reconnects | 0 | 0 |
| Recovery HTTP calls | 0 | 0 |
| Monitoring/backlog/metric drops | 0 | 0 |

The restart run ended with both slots `RUNNING / ready` before graceful stop and reused:

`C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-ui\engines\mainnet\continuous-v1-proof`

Run artifacts:

- `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-ui\monitor-soak-20260718T225606`
- `C:\Users\papaa\AppData\Local\HyperliquidCopytrader\runtime\continuous-ui\monitor-soak-restart-20260718T225933`

Boundary: these runs prove real source/follower connectivity, reducer latency, stable-state reuse, and no normal REST loop. They do not contain a fresh leader trade or an armed follower action.
