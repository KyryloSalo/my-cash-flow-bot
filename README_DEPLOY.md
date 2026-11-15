# Production deployment

Production is deployed from GitHub, not from a locally assembled `code_v0.zip`.

Canonical repository:

```text
https://github.com/KyryloSalo/my-cash-flow-bot
```

## Required release path

1. Merge reviewed changes into protected `main` through a pull request.
2. Wait for `.github/workflows/deploy.yml` to validate the exact 40-character commit SHA and publish `validated-<SHA>` artifacts.
3. Start `.github/workflows/production.yml` from `main` with:
   - `release_sha`: the current full `main` SHA;
   - `confirm`: `deploy-production`.
4. Approve the protected `production` Environment request.
5. Wait for the workflow to finish and verify its deployment evidence artifact.

The production Environment:

- accepts deployments only from `main`;
- requires reviewer approval;
- disables administrator bypass;
- stores deployment credentials and the backup encryption passphrase as GitHub secrets.

Do not copy secret values into commands, issues, pull requests, release notes, or this file.

## Safety guarantees

The production workflow fails closed unless all of the following are true:

- the requested SHA is the current protected `main` SHA;
- the matching push validation run succeeded;
- the release tag and validation-run identity match;
- release and image checksums pass;
- SSH uses the pinned host key from `ops/known_hosts.production`;
- an encrypted PostgreSQL backup is uploaded off-host and its receipt checksum is verified;
- the existing `ops/deploy.ps1` and `deploy_v0_on_vps.sh` rollout succeeds;
- database migration checks, Django deploy checks, container health, and readiness smoke checks pass;
- `/opt/my-cash-flow-bot/current-release` reads back the requested exact SHA.

Transfers remain transfers, financial writes retain their confirmation gate, and deployment does not change ledger semantics.

## Verification evidence

For each production run, retain and inspect:

- the successful validation run ID;
- the successful production run ID;
- `deployment-evidence/deployment.json`;
- `backup-evidence/backup-receipt.json`;
- the promoted `validated-<SHA>` GitHub Release;
- public HTTP readback for the site and Mini App.

A successful HTTP response alone is not deployment proof. The authoritative proof is the exact-SHA deployment artifact with `readback: passed`.

For the restricted synthetic billing validation workflow, security gates, kill switch, reset contract, and operator checklist, see `docs/PRODUCTION_CANARY_MODE.md`. That mode remains disabled unless its independent production configuration and per-user gates are all enabled.

## Rollback

Do not initiate rollback only because an external page probe fails. First inspect the failed GitHub Actions run and production evidence.

The hardened server rollout maintains release directories and the `current` symlink. Its rollback restores the previous application release without attempting a database downgrade. Any rollback is an incident action and requires an explicit decision after the failure reason is known.

## Manual emergency access

Direct archive upload, direct edits under `/opt/my-cash-flow-bot`, disabling SSH host verification, and bypassing backup/evidence checks are not normal release procedures.

Use manual server access only for an explicitly approved incident. Any emergency production hotfix must be ported back through a reviewed GitHub pull request before the next release so GitHub remains the source of truth.

## Current verified release

As of November 15, 2026, production runtime is `00bbcce7f35ea0c5042915eb70daf5503dd82d29` (PR #89), documented as PATCH `0.45.4` after canonical documentation synchronization. Exact-main validation `43740158393` and approved production run `43740828643` passed, including encrypted off-host backup, deployment/readback and promotion at `2026-11-15T17:29:21.742598+00:00`.

See `docs/releases/0.45.4-billing-lifecycle-recovery.md` for scope, guarded read-only findings and limitations. Source/template hashes matched the exact runtime Git revision; all six app containers were healthy/running, and bounded beat/worker logs showed three reconciliation sends and three successful executions with no traceback. Live new payment/end-user retry and eligible-claim activation remain unverified. The companion seven-file canonical replacement manifest is pending review, CI and docs PR merge before application. This docs change does not deploy a new runtime or waive historical acceptance gaps.

### Historical release 0.45.1

As of October 23, 2026:

- version: `0.45.1` (registration administrator-notification PATCH);
- deployed runtime SHA: `b3e22861ec827bbfa885832b22189063317dbfcb`;
- exact-main validation run: `39951518916`;
- approved production run: `39954037678`;
- deployment readback: `passed` at `2026-10-23T12:37:02.622959+00:00`;
- release promotion and separate public readiness smoke: passed.

See `docs/releases/0.45.1-registration-admin-notifications.md` for scope and
acceptance limits. Real new-user production registration and recipient-side
Telegram delivery have not been verified. This documentation sync does not
change the deployed runtime SHA or require another production rollout.
