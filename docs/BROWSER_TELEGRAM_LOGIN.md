# Browser login through Telegram confirmation

Status: candidate implementation; production activation requires the protected GitHub delivery route and exact-SHA deployment evidence.

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

## Historical failure

Git commit `0f093c03aba99022cca67c894e098431cf131f1f` removed the bot's local-signing fallback after internal issuance failed.
The production issuance endpoint subsequently observed in this incident failed with `Browser login secret not configured.`
This proves why the bot stopped producing a link when server issuance failed; it does not establish that every old locally signed link could successfully authenticate or when server configuration first became incomplete.
