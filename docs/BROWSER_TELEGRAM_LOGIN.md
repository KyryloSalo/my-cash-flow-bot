# Browser login through Telegram confirmation

Status: deployed to production on 2026-10-21 at runtime SHA `581efb313ddfc40ea0fa529d545e8643b409ed27` through approved production run `39624076224`; deployment readback passed. Actual native Telegram-to-browser acceptance remains unverified.

## User flow

The bot sends a public HTTPS URL such as `https://vydno.capital/app/login/`.
The URL does not contain a Telegram user ID or an authentication token and does not log anyone in by itself.
The browser entry page starts the existing Telegram OIDC authorization at `/app/auth/telegram/start`.
The user confirms their own Telegram identity; only the verified callback establishes the PWA browser session.

The bot cabinet, onboarding, operator entry and install reminder no longer require internal browser-link issuance.
Neither these links nor the browser entry page require `MINIAPP_BROWSER_LOGIN_SECRET`, a new signing key or a new `.env` file.
The existing Telegram OIDC client configuration is still required; this change does not bypass Telegram identity verification.

## Browser handoff and destinations

- Android Telegram: reuse the Chrome intent handoff before creating OIDC state, so state and callback cookies belong to the external browser.
- iOS embedded browsers: retain the existing Safari instructions.
- Ordinary browser: click **Увійти через Telegram**.
- `?install=1` returns to `/app/?install=1` after confirmation.
- `?operator=1` returns to `/app/operator/`; this flag never grants operator permissions.
- Combined mode returns to `/app/operator/?install=1`.
- Arbitrary `next`, Telegram identity and authentication parameters on `/app/login/` are ignored.

OIDC state, PKCE, signed ID-token verification, one-use token consumption and session identity binding remain in the existing OIDC implementation.
The login page itself does not create an authenticated session or write financial rows.
Previously issued `/app/browser-login/<token>/` and `/app/api/browser-login/<token>/` routes remain compatible; ordinary entry no longer uses them.

## Verification boundaries

Run bot entry regressions and `python ops/telegram_login_acceptance.py`.
The isolated acceptance uses a real Django server, HTTPS proxy, PostgreSQL and Chromium with a **synthetic Telegram provider**, never production identity or financial data.
Synthetic-provider success verifies the application callback path, not real Telegram confirmation or native Telegram-to-Chrome behavior.
Production readback of the entry page and Telegram redirect is likewise not proof of a completed user login.
Actual confirmation by the intended Telegram user remains a separate acceptance step.

Release `0.43.3` also decodes gzip-compressed Telegram JWKS and token responses before JSON parsing. Signature, claim, state, PKCE and replay checks are unchanged. Two real HTTP/gzip/RSA regressions and both isolated Chromium runs with 35 Django regressions passed. A read-only probe of the deployed module loaded four signing keys from the live Telegram provider at `2026-10-21T15:28:49.268171+00:00`; this is not an authenticated production login.

The owner reported successful desktop link generation and Telegram-confirmed login before this deployment. The desktop/mobile difference and the operational trigger of the gzip behavior are not established. An existing browser session can survive a failed repeat callback; this is a code-path observation, not a diagnosis of the owner's device.

## Historical failure

Git commit `0f093c03aba99022cca67c894e098431cf131f1f` removed the bot's local-signing fallback after internal issuance failed.
The production issuance endpoint subsequently observed in this incident failed with `Browser login secret not configured.`
This proves why the bot stopped producing a link when server issuance failed; it does not establish that every old locally signed link could successfully authenticate or when server configuration first became incomplete.
