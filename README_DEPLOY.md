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

## Rollback

Do not initiate rollback only because an external page probe fails. First inspect the failed GitHub Actions run and production evidence.

The hardened server rollout maintains release directories and the `current` symlink. Its rollback restores the previous application release without attempting a database downgrade. Any rollback is an incident action and requires an explicit decision after the failure reason is known.

## Manual emergency access

Direct archive upload, direct edits under `/opt/my-cash-flow-bot`, disabling SSH host verification, and bypassing backup/evidence checks are not normal release procedures.

Use manual server access only for an explicitly approved incident. Any emergency production hotfix must be ported back through a reviewed GitHub pull request before the next release so GitHub remains the source of truth.

## Current verified release

As of October 13, 2026:

- version: `0.35.11`;
- deployed SHA: `e2ff3eef59e91f77523a96fe11a2b940ca797049`;
- validation run: `38431439054`;
- production run: `38432260261`;
- deployment readback: `passed`.
