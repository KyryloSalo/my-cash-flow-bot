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

### TLS renewal and independent monitoring

The ACME webroot is `/opt/my-cash-flow-bot/shared/acme-webroot`, mounted
read-only into nginx at `/var/www/certbot`. Never replace it with a path under
`current` or a release directory. Deployment verifies a unique, exact-body
HTTP challenge on every configured certificate domain before promotion.

The reviewed release installs host-owned controls in `shared/tls-ops` and
systemd units via `ops/install_tls_ops.py`. The existing `certbot.timer`
invokes a persistent renewal wrapper under the deployment lock. Its deploy
hook validates nginx, reloads without restarting the application, verifies
public TLS against local TLS, and checks that renewed domains actually changed
certificates. A weekly staging dry-run exercises the same hook without
requiring certificate replacement. Failed renewal/dry-run units trigger a
retryable notification using the existing bot runtime configuration.

`vydno-public-monitor.timer` checks public HTTPS every five minutes. The
separate GitHub `tls-monitor` workflow checks independently of the VPS and
maintains one incident issue. A separately scheduled external Telegram watcher
can reuse configured Hermes access without copying credentials; its machine
must be awake and online. GitHub schedules may be delayed, so they are not a
strict five-minute delivery SLA. Threshold transitions at 21/14/7 days and
recoveries are deduplicated; failed notification attempts must not acknowledge
the state transition.

After deployment run these on the host, recording real outputs:

```sh
python3 /opt/my-cash-flow-bot/current/ops/tls_acceptance.py --production
systemctl start vydno-tls-dry-run.service
systemctl show vydno-tls-dry-run.service -p Result -p ExecMainStatus
systemctl list-timers certbot.timer vydno-tls-dry-run.timer vydno-public-monitor.timer
printf '%s' 'Vydno TLS notification delivery test' | bash /opt/my-cash-flow-bot/shared/tls-ops/notify_ops.sh
```

The installer does not prove activation or notification delivery; those require
the explicit production readbacks above. Do not disable TLS validation or read
private keys to diagnose a renewal failure.

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
