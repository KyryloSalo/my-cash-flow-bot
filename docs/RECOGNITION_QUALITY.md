# Recognition quality capture

## Status and access

This is an unreleased candidate. Production capture and scheduled weekly analysis must remain disabled until reviewed PR/CI, approved exact-SHA rollout and runtime readback. There is no public media or export endpoint.

Use an authenticated private-server operator session: `python manage.py enable_recognition_quality` previews the current tester count; `--apply` freezes all current users for seven days. Repeated apply while active neither adds users nor extends expiry; future users remain excluded. `--status` emits bounded operational metadata; `--disable` is idempotent. Activation fails closed without private storage, enabled admin notifications, administrator IDs and bot access. Presence checks do not prove persistent mounts or live delivery. Confirmation is not audited ground truth; cancellation is not automatically an error. No automatic fine-tuning is performed.

## Private disk quota

- Configure `RECOGNITION_QUALITY_STORAGE_ROOT` as an absolute, dedicated, persistent private directory, shared by every API/worker replica using this database. Do not use static/media directories, a public web-server mount, an ephemeral container layer or a symlink. Without this directory, media capture is skipped without stopping recognition; text/metadata collection can continue.
- The hard ceiling is **1 GiB (1,073,741,824 bytes)** of actual files in the private directory, including raw audio/photo, private review exports and orphans. This is disk usage, not a RAM allowance, DB row count or physical server-space reservation. Database metadata/text and filesystem overhead are outside this file quota. Provision free server disk and the persistent mount separately during approved rollout; compose does not allocate production storage automatically.
- The config defaults to 1 GiB and may lower the limit for testing; it cannot raise the hard ceiling. Each media file is additionally bounded to 10 MiB. Postgres stores the opaque file key and byte count, not raw media blobs.
- PostgreSQL transaction advisory locking serializes the scan/check/write across workers and cohort-config changes. The scan counts actual file lengths, including orphaned files, rather than trusting only DB accounting. No blob collection is loaded into RAM to count usage.
- Files have random names, no user IDs/extensions, and POSIX mode `0600`; the root is `0700`. Deployment must verify actual filesystem/web-server access restrictions (Windows requires equivalent ACLs).
- When the next file would exceed quota it is not saved. Recognition continues, no extra inference is requested, and existing financial confirmation gates are unchanged. Storage or notification failures do not create financial writes or turn telemetry into a request failure.

## Telegram administrator notifications

The existing admin-notification Celery task sends a warning at **90%** and a full/insufficient-space alert at the limit or when the next file cannot fit. `ADMIN_NOTIFICATIONS_ENABLED`, configured `ADMIN_TELEGRAM_IDS`, bot access and a working Celery broker/worker are prerequisites; no new credentials are embedded in this feature.

Messages contain only the event, used/quota bytes, timestamp and operational status. They never include raw photo/audio/text, user identity, provider results or export contents. Warning and full events are each rate-limited for 24 hours across workers/cohort renewals. A durable DB outbox survives broker enqueue failure; beat retries queued/failed deliveries every five minutes independently of new captures. Delivery is at-least-once, not exactly-once. Successful queueing is not proof of Telegram delivery: inspect notification-delivery status and perform an approved synthetic admin-channel smoke after rollout.

## Retention and deletion

Celery beat schedules `miniapp.tasks.purge_expired_recognition_captures_task` hourly. With beat and workers running, expired-cohort captures and captures older than the configured retention (at most seven days) are removed. Files are unlinked only after DB deletion commits; an aborted deletion preserves them. Old crash/rollback orphans are also swept and count against quota until removed. Filesystem removal failures are best-effort and continue to consume quota until a later orphan sweep succeeds. The private directory must remain available to API and cleanup workers.

`hard_delete_user` deletes recognition metadata and schedules media removal after DB commit. ORM capture deletion also schedules media deletion. Both paths invalidate cached review exports after commit; private exports otherwise expire after one hour. Rollback preserves files. Filesystem failures remain an operational cleanup issue, not proof of deletion.

Migration `0022_recognition_private_disk` refuses to drop a nonempty legacy DB-media field. If legacy raw blobs exist, stop and arrange reviewed private migration; do not bypass the guard or discard captured data.

## Analysis/export release gates

The export service requires authenticated protected access, bounds rows to 1000 and excludes the explicit raw-text/media fields by default. Provider/normalized/correction fields can themselves contain sensitive text, so **all exports are private**, even without `include_raw`. Explicit raw export reads private files and bounds aggregate media bytes to 20 MiB; it must not be put into logs, Telegram, GitHub Actions or broad data exports.

The internal service requires an active Django superuser and additionally checks the shared token only when configured. On the authenticated private server, `python manage.py recognition_quality_export --requester-id <active-superuser-pk> --limit 100` writes one bounded random `*.export.json` file under the private root with mode `0600`; stdout prints filename/count/has_more only. `--metadata-only` excludes explicit raw fields. Exports count against the same quota and expire after one hour or capture/account deletion. This local operator CLI must never become an unauthenticated HTTP wrapper or run in GitHub Actions. Never send raw samples via Telegram or broad exports. Production private analyst access remains unverified. Weekly AI analysis stays paused until that access and target-environment acceptance pass. `unreviewed` is not accuracy; no model training or production activation is claimed.
