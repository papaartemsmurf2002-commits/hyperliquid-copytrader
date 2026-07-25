# Authorized operator intervention

## 2026-07-24 — acc6 WLD UNKNOWN terminalization

- CLOID: `0x55e5c961632b1caa9f5c2ae1b5f609a2`
- The durable action was externally transitioned from `UNKNOWN` to `CANCELED`.
- Evidence: the IOC had expired, repeated `orderStatus` returned `unknownOid`, no matching WLD fill existed, and authoritative follower state contained no WLD position.
- Purpose: preserve the long-running generation while retaining this incident as real recovery-failure evidence.
- Result: acc6 resumed and completed four reduce-only convergence fills for EIGEN, HYPE, JTO, and ZRO without retrying WLD.

## Unresolved safety requirement

An ambiguous or disconnected entry must block duplicate and risk-increasing actions, but it must not indefinitely block an independently provable position reduction after the leader reduces or closes exposure.

The required emergency-reduction path must:

- use fresh authoritative follower positions and fresh leader/source truth;
- permit only reduce-only actions that monotonically decrease absolute follower exposure;
- never reverse, increase, or open exposure;
- remain available while an unrelated CLOID is unresolved;
- account conservatively for the maximum possible fill of the unresolved action;
- preserve market, signer, margin, and rate-limit safety;
- record every decision;
- stop if truth is contradictory or insufficient;
- reconcile the ambiguous CLOID independently until terminal.

The generation under audit did not satisfy this requirement and therefore cannot be classified as unattended.
