# My Cash Flow Bot — Repository Rules

## Product and code

- The PWA is the primary product interface. Telegram provides identity, notifications, and result delivery; it is not a replacement for the PWA.
- The project is DB-first/Postgres. Historical Google Sheets-first documentation is not current architecture.
- Main bot entry path: `bot/app.py` -> `bot/bot_main.py`, unless current code proves otherwise.
- Inspect only the smallest relevant set of files. Do not scan generated folders, caches, virtual environments, staticfiles, local SQLite databases, old archives, or credentials.
- Never read or expose `.env`, `.env.*`, `*.pem`, `*.key`, tokens, passwords, connection strings, or recovery codes.

## Financial invariants

- Every financial write requires a confirmation step before the database mutation.
- Transfers are not expenses or income.
- Cross-currency transfers require an exchange rate.
- Base currency is UAH unless the user explicitly selects another currency.
- Do not add “start without categories” to onboarding.
- Prefer services and tests for financial logic; do not patch large handlers opportunistically.

## Mandatory task entrypoint

Before implementing a feature or bugfix, run `python delivery.py start --session-id <actual-session-id>` with the complete agreed request, one `--requirement` per observable outcome, explicit `--kind`, `--target` and `--acceptance-command`. Then run `python delivery.py preflight --session-id <actual-session-id>` before edits. On resume, read `status` instead of recreating the task. See `docs/DELIVERY_WORKFLOW.md` for complete commands.

- Preserve the original request; append every agreed clarification and additional requirement through the CLI. Never replace the contract with a smaller implementation plan.
- Reproduce an explicit bugfix and record the actual RED evidence before changing its cause. After two disproved diagnoses or the same blocker twice, reassess from evidence rather than retrying blindly.
- After source changes, snapshot the final candidate and repeat its preflight. Execute the named user-scenario acceptance, not only unit tests; PWA tasks need browser acceptance. Record actual sanitized logs, exact commands, exit codes and explicit per-requirement acceptance links.
- Obtain independent review of the original requirements and exact candidate for substantial changes. Run `python delivery.py readiness --session-id <actual-session-id>` and the `delivery-verification` receipt gate before claiming completion. A green local gate is neither a merged PR nor production activation.
- `.hermes/delivery/` is local task state, never a release input. Do not commit requests, receipts or model transcripts.
- From the usual outer project folder, the same checked-in `delivery.py` launcher finds the `code/` repository. Keep it and these rules synchronized from reviewed `main`; do not reset unrelated unfinished work to update delivery tools.

## Mandatory GitHub delivery contract

GitHub is the canonical control plane for every production-bound change. This is a project invariant and must be followed in every new task and chat.

1. Fetch the repository and compare the task/local work with current `origin/main` and the verified production SHA.
2. Branch from current `origin/main`. Preserve useful local work, but integrate it thematically; never merge a recovery snapshot wholesale.
3. Use Conventional Commits: `type(scope): description`.
4. Open a pull request. Required checks and review must pass before merge.
5. Merge only through protected `main`; do not force-push, rewrite history, or bypass branch protection.
6. Let `.github/workflows/deploy.yml` validate the exact full SHA and publish the immutable `validated-<SHA>` artifact.
7. Runtime changes reaching `main` are not automatically released. Start `.github/workflows/production.yml` manually for the current full `main` SHA and obtain `production` Environment approval.
8. Production promotion must use the validated exact-SHA artifact, pinned SSH host verification, encrypted off-host backup with a verified receipt, existing hardened rollout scripts, and exact-SHA readback.
9. Do not claim release success from HTTP status alone. Verify the GitHub run, deployment evidence, backup receipt, and production readback.
10. After a successful production deploy, bump `VERSION`, append `docs/RELEASE_NOTES.md`, and update affected canonical documentation through a follow-up PR. Never document a release that did not happen.

Documentation-only changes still use PR/CI but do not require a production redeploy unless they change the shipped artifact or runtime behavior.

Never deploy a local working tree, recovery branch, mutable archive, or unvalidated SHA directly to production. Direct server edits or archive uploads are emergency-only actions requiring explicit approval and must be ported back through a reviewed PR.

## Canonical deployment files

- `README_DEPLOY.md` — operator runbook.
- `.github/workflows/deploy.yml` — validation and immutable release artifact.
- `.github/workflows/production.yml` — approved exact-SHA production promotion.
- `ops/deploy.ps1` — hardened deployment entrypoint.
- `ops/known_hosts.production` — pinned SSH host keys.
- `ops/backup_database.sh` and `ops/backup_receipt.py` — encrypted backup and receipt verification.
- `deploy_v0_on_vps.sh` — server rollout, readiness, and rollback mechanism.

Do not weaken host verification, protected-branch checks, Environment approval, artifact/checksum validation, backup verification, or production readback to work around a failure.
