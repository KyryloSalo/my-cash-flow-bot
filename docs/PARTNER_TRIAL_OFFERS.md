# Partner trial offers

## Configuration

In CRM → Partner links, create a link with an explicit, unique **code/hash** and **trial days**. Codes use `^[a-z0-9][a-z0-9_-]{2,63}$`; leave trial days empty for attribution only. Any positive whole number up to the representable safety limit is supported, including **51** and **137**; zero and negative values are rejected. Published codes remain immutable to avoid breaking shared links.

The public URL is `/app/r/<code>`. The landing destination and direct PWA destination both preserve trusted acquisition attribution.

## Registration and access

- A valid click freezes the configured offer on the server-side acquisition session. Changing the link's days later affects subsequent clicks, not the offer already shown to that visitor.
- Landing, registration and onboarding display the frozen offer; a new registration receives a matching trial subscription with an exact `expires_at - started_at` duration.
- No payment, card binding or automatic renewal is required or initiated. Partner trials do not use the separate fixed 90-day course bonus.
- One immutable claim is recorded per new user. Repeat authentication and existing users do not receive another grant; stronger paid/manual/lifetime access is not downgraded.
- Deactivation withdraws an unclaimed offer. Missing, unknown, inactive or expired attribution falls back to ordinary registration without a partner grant.
- Grant persistence and identity registration share one transaction. A temporary grant failure rolls both back and returns `partner_offer_unavailable` with HTTP 503 so registration can be retried safely.

## Manual acceptance

1. Create a fresh link with a manual code and **51** trial days.
2. Open it in a private browser session; check the 51-day copy before signing in.
3. Register a genuinely new Telegram identity; check 51 days in onboarding and the access record.
4. Re-authenticate: no second subscription/claim should appear.
5. Repeat with another count (for example 137); organic/inactive links must not grant a partner trial.

Release status is recorded separately in the exact-SHA deployment evidence; this specification alone is not a production-deployment claim.
