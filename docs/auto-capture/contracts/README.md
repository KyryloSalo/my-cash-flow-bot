# Auto-capture shared contracts

A0 accepts normalized, structured events only. Clients must parse notification text locally and send no raw notification text, OTP, PAN, or provider payload.

- `event.schema.json` is the wire contract.
- `fixtures/` contains safe examples for tests and client implementations.
- Ingest is idempotent by `(connection, event_id)` and rejects changed retries as conflicts.
- Ingest creates inbox capture events only; it never writes the ledger. Ledger writes reuse the existing draft/confirm flow.
