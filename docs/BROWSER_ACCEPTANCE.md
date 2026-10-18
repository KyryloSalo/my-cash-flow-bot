# Local PWA browser acceptance

## Commands

From the repository root:

```text
python ops/browser_acceptance_smoke.py --preflight
python ops/browser_acceptance_smoke.py --server --log C:/Users/User/AppData/Local/hermes/rollbacks/task-inspector/20261018-delivery-loop/pwa-browser.log
```

The first command launches the installed headless Playwright Chromium. The second starts a uniquely named, disposable `postgres:16-alpine` container with a newly declared synthetic credential, maps it to a dynamic loopback port, runs Django migrations, starts Django on a dynamically selected loopback port, and exercises the real `/app/` route. `DATABASE_URL` is removed before the explicit synthetic URL is installed; no `.env` file is read, no pre-existing database/container is reused or modified, and cleanup runs on success or failure.

## Acceptance assertions

The browser gate requires the visible shipped PWA shell, exactly one manifest link, the dev-authenticated dashboard, onboarding shell, Ukrainian navigation, successful `manifest.webmanifest` and `sw.js` responses, and zero browser console errors. It performs no financial draft, confirmation, payment, Telegram, provider, or production action. A pass is only local route acceptance; it is not provider or financial acceptance.

## Runtime requirements

- Python 3.11+ with the repository's `admin_service/requirements.txt` installed.
- Playwright Python package and an installed Chromium browser (`python -m playwright install chromium`).
- Docker Desktop running and able to pull/run `postgres:16-alpine`.
- Loopback networking and permission to create/remove the uniquely named smoke container.

The script does not change `.github/workflows/deploy.yml`, production settings, or deployment state. Keep the generated transcript under `C:/Users/User/AppData/Local/hermes/rollbacks/task-inspector/20261018-delivery-loop/` and publish its SHA-256 with the task evidence.
