# Production Canary Mode

Production Canary Mode is a restricted, synthetic-only path for validating billing and subscription behavior in the production application without calling the real payment provider or creating real debits.

It is not a replacement for test-user status and is not a public test endpoint.

## Security gates

Every canary operation fails closed unless all of these conditions hold:

- `PRODUCTION_CANARY_ENABLED` is enabled;
- the exact Telegram user ID is in `PRODUCTION_CANARY_TELEGRAM_IDS`;
- the user is active;
- the user is already marked as a test user;
- the distinct Production Canary state is enabled by an authorized administrator;
- the session uses the `production_canary` authentication mode;
- the action has a purpose-bound, user-bound, unexpired and single-use capability;
- the requested scenario is present in `PRODUCTION_CANARY_ALLOWED_SCENARIOS`;
- the synthetic amount does not exceed `PRODUCTION_CANARY_MAX_AMOUNT`.

The application stores only hashes for raw login and capability tokens. Do not put tokens, cookies, payment credentials, card data or authentication values into tickets, logs, screenshots or audit comments.

## Configuration

The general mode remains fail-closed. Production release `0.37.2` explicitly activates only the reserved synthetic identity `777777` when the dedicated browser-login secret is configured.

| Setting | Purpose |
| --- | --- |
| `PRODUCTION_CANARY_ENABLED` | Global kill switch. |
| `PRODUCTION_CANARY_TELEGRAM_IDS` | Server-side allowlist of exact Telegram user IDs. |
| `PRODUCTION_CANARY_LOGIN_TTL_SECONDS` | Lifetime of one-time browser login grants. |
| `PRODUCTION_CANARY_ACTION_TTL_SECONDS` | Lifetime of purpose-bound action capabilities. |
| `PRODUCTION_CANARY_MAX_AMOUNT` | Maximum synthetic amount in minor currency units. |
| `PRODUCTION_CANARY_ALLOWED_SCENARIOS` | Explicit scenario allowlist. |
| `MINIAPP_BROWSER_LOGIN_SECRET` | Dedicated browser-login signing secret; there is no bot-token fallback. |

The source-controlled allowlist always contains the reserved synthetic identity `777777` in addition to any configured IDs. Migration `users.0016_activate_synthetic_canary_identity` activates only the exact row owned by `production_canary_seed`; missing or ambiguous state fails the deployment closed. The migration is reversible and disables only that reserved seeded identity on rollback.

No ordinary user is activated by this hardcode. Capability issuance, expiry, exact ownership, single-use consumption, explicit scenario confirmation, synthetic provider enforcement, and reset controls remain mandatory.

Keep any environment override minimal. The reserved identity is intended only for the one-time Production Canary browser-login flow; a Telegram account is not required on the test device.

## Operator workflow

1. Open the reserved synthetic user `777777` in Django Admin and verify that it is active, test-only, server-allowlisted, and marked as Production Canary.
2. Generate a one-time browser-login handoff. The bootstrap removes the raw token from the browser URL before exchanging it.
3. Issue the required scenario from the dedicated Production Canary controls.
4. Review the scenario and confirm it in the Mini App. Synthetic actions use `provider="canary"`; they must never call Monobank.
5. For delayed scenarios, complete the exact pending capability from Django Admin.
6. For renewal/retry, use the dedicated manual action; the production scheduler excludes canary users.
7. Inspect the audit trail and the resulting synthetic records.
8. Use **Preview / reset**, verify the exact user and preview fingerprint, then enter the reset confirmation phrase and a non-empty comment.
9. Disable Production Canary when validation ends. Disable revokes live canary sessions and outstanding capabilities.

There is no bulk enable, public webhook, query-parameter authorization or automatic production activation.

## Supported synthetic scenarios

The default allowlist supports:

- success;
- insufficient funds;
- declined;
- processing;
- delayed success;
- delayed failure;
- action required.

The exact enabled set is controlled by `PRODUCTION_CANARY_ALLOWED_SCENARIOS`. A scenario omitted from the server configuration must be rejected generically.

## Reset contract

Reset is preview-first and exact-user only. The submitted preview fingerprint must match the current database state.

Reset may remove or revoke only explicitly canary-owned operational records, including synthetic payments, subscriptions, consents, billing profiles, acquisition/funnel records, related synthetic notification/draft artifacts, active sessions and outstanding capabilities. Audit records remain available.

Reset must abort if ordinary or real-provider financial artifacts are in scope. It must never remove normal ledger entries, real Monobank payments, ordinary authentication history or unrelated user data.

## Observability and reporting

Canary enable, disable, login, capability, payment and reset actions are audited with sanitized metadata. Audit payloads must not contain raw capabilities, tokens, cookies, credentials, PAN/card data or secret-like values.

Synthetic canary records are excluded from business reporting, automation, broadcast targeting, trial recovery, production billing and scheduled renewals. Real historical records belonging to the same user must not be reclassified as synthetic.

## Incident response and kill switch

If behavior is unexpected:

1. Disable/reset the reserved synthetic user through the exact-user admin control; this revokes active canary sessions and outstanding capabilities.
2. If code rollback is required, revert release `0.37.2` and apply the reverse of migration `users.0016_activate_synthetic_canary_identity`; it deactivates only the exact reserved seed row.
3. Do not retry a possibly consumed raw token.
4. Preserve sanitized audit evidence and the exact deployment SHA.
5. Verify that no `provider="canary"` event reached Monobank transport and no real debit occurred.
6. Use the normal GitHub PR and exact-SHA deployment process for fixes; do not patch production directly.

## Verification before release

A release containing canary changes requires:

- unit and characterization tests for existing Monobank behavior;
- PostgreSQL-backed migration, request, lifecycle, replay and concurrency tests;
- generic denial behavior for non-eligible users;
- proof that disable invalidates existing sessions;
- proof of one-time login and capability consumption under races;
- proof that every scenario maps to the expected payment/subscription state;
- proof that amount limits are enforced before financial rows are created;
- proof that reset preserves real-provider and ordinary user records;
- proof that no canary path invokes real provider transport;
- green GitHub `validate`, review, merge to protected `main`, exact-SHA production approval and deployment readback.

This document describes the implementation and operator procedure. Its presence does not mean the feature has been released or enabled in production.
