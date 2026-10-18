# Partner trial offers

## Configuration

In the existing CRM → Partner links, create a link with an explicit, unique **code/hash** and **trial days**. Codes use `^[a-z0-9][a-z0-9_-]{2,63}$`; leave trial days empty for attribution only. Positive whole numbers from **1 to 365000** are supported, including **51**, **137** and the mandatory acceptance value **7889**; zero, negative and out-of-range values are rejected. Published codes remain immutable to avoid breaking shared links.

The public URL is `/app/r/<code>`. The landing destination and direct PWA destination both preserve trusted acquisition attribution.

Use that generated route to resolve a trusted partner offer. A manually constructed `/app/?referral_code=<code>` query is attribution data alone and is not a substitute for resolving the partner link. Quiz result CTAs wait for funnel bootstrap and preserve the resolved server-owned partner state.

## Registration and access

- A valid click freezes the configured offer on the server-side acquisition session. Changing the link's days later affects subsequent clicks, not the offer already shown to that visitor.
- Landing, registration and onboarding display the frozen offer; a new registration receives a matching trial subscription with an exact `expires_at - started_at` duration.
- No payment, card binding or automatic renewal is required or initiated. Partner trials do not use the separate fixed 90-day course bonus.
- One immutable claim is recorded per new user. Repeat authentication and existing users do not receive another grant; stronger paid/manual/lifetime access is not downgraded.
- Deactivation withdraws an unclaimed offer. Missing, unknown, inactive or expired attribution falls back to ordinary registration without a partner grant.
- Grant persistence and identity registration share one transaction. A temporary grant failure rolls both back and returns `partner_offer_unavailable` with HTTP 503 so registration can be retried safely.

## Manual acceptance

1. In the existing production admin, create a fresh link with a manual code and exactly **7889** trial days.
2. Open its generated URL in a private browser session; check literal **7889** before signing in and preserve attribution through all landing and quiz CTAs.
3. Register a genuinely new identity through real Telegram authentication; verify the durable claim/subscription duration and PWA/onboarding both remain exactly **7889** days. Local or synthetic registration alone does not satisfy this production step.
4. Re-authenticate: no second subscription/claim should appear.
5. Repeat with another count (for example 137); organic/inactive links must not grant a partner trial.

Release status is recorded separately in the exact-SHA deployment evidence; this specification alone is not a production-deployment claim.

The `0.40.1` artifact is deployed on exact SHA `b80a05536f2a61cc0884257117ce4a89a2bc4bd8`, but full `PARTNER-01` delivery remains **INCOMPLETE** because production real-browser `7889 → 7889 → 7889` acceptance is blocked. See `releases/0.40.1-partner-cta-attribution.md` for authoritative activation/readback and the browser-session blocker.
