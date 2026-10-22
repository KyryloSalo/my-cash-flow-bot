# Paid checkout return to authenticated PWA

Status: production runtime `9163a883822ce9def5acd884d653f948c56d5f91`, deployed 2026-10-22; release 0.44.0. This document describes runtime behavior, not proof of live paid acceptance.

## User path

A newly created Monobank bind/recovery checkout can carry a server-issued checkout-return browser capability. After its exact payment is PAID, the localized return page offers **Відкрити застосунок / Open app**, not a Telegram-only primary action. The user explicitly submits the same-origin form. On success the browser owns a normal authenticated session and reaches exact `/app/`. The anonymous browser needs no existing Telegram/PWA session. Old checkout tokens without the capability, and ineligible/expired returns, keep their safe fallback; visiting a return page does not mint a fresh capability.

## HTTP contract and guards

- `POST /billing/mono/open-app`, protected by Django CSRF. Form field: `return_token` (the signed checkout return value). GET is refused and cannot authenticate or consume a capability.
- The outer return, embedded capability, checkout owner, expected bind/recovery payment kind, Monobank provider, exact checkout binding and PAID status must agree. No polling/authentication of a newer unrelated checkout is allowed.
- Only purpose `checkout_return` is accepted here; ordinary browser-login and production-canary capabilities are not interchangeable. The signed capability expires and can be consumed once.
- An existing session for another user is refused without switching identity or consuming the checkout capability. Same-user replay stays the same user; a used capability cannot authenticate a different fresh browser.
- Failed, unpaid, tampered, expired and reused capability attempts must not create a login. Success redirects to `/app/`; failure statuses and fallback are deliberate guards, not permission to retry with a different user.
- On capability-bearing return pages `strict-origin` permits the protected same-origin POST without disclosing the capability-bearing query in Referer. Other return pages retain `no-referrer`. CSRF is not disabled.

## Verification boundary

Isolated local Chromium acceptance covers uk bind/en recovery, exact authenticated `/app/` plus profile identity, same-user replay, cross-user isolation, used-token denial in a fresh browser, invalid/unpaid/failed/expired denial, fixture cleanup and financial-state preservation around owned fixtures. The unchanged runtime also passed 244 regression tests and exact-main GitHub validation.

Production run `39742966975` has successful exact-SHA revision/readiness readback and verified encrypted off-host backup. Eight fresh anonymous production Chromium checks cover the PWA shell, anonymous profile, GET/CSRF rejection and no session creation. They do **not** prove a real paid production Monobank checkout, authenticated production identity or native device handoff. Those live acceptance checks remain unverified. No provider call or financial write was requested by public probes; production financial-row-count invariance was not independently measured.
