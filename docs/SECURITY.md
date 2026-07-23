# Security

- UI binds to numeric literal loopback, accepts only a loopback peer, and requires exact same-origin evidence for continuous mutations. Local-only launch does not require a login; setting `HLCT_GUI_TOKEN` optionally restores HTTP Basic authentication.
- Browser/frontend never receives a private key, signature, signed payload, or signer-key path.
- Monitor-only binding validates public profile metadata but does not open key content.
- An armed child opens only its profile-bound API-wallet key files.
- One global OS lock owns each follower and API-wallet signer across all engine directories.
- Source, follower, and signer roles are checked for collisions and exact authorization.
- Every IOC has a durable CLOID, signer nonce, signed payload, and send boundary before socket write.
- A lost acknowledgement becomes `UNKNOWN`; no blind retry is permitted.
- The continuous action capability is limited to order actions needed by the copy engine. Transfer, withdrawal, approval, and unrelated account actions are absent.
- Mainnet HTTP canary, watchdog, guardian, and alternate fleet launch commands were removed.

Do not place secrets in `.env`, browser storage, URLs, logs, audit files, or the repository. The local backup created for this audit includes the complete project scope and must be protected accordingly.
