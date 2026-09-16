# Runtime schema ownership and clean PostgreSQL setup

`statements.py` is the shared, core-only SQL extracted from the active
`bot_main.init_db`, **not** the deprecated `bot/migrations/*.sql` branch.

## Ownership

- This package owns the 16 runtime tables listed in `CORE_TABLES`, including
  unmanaged Django mirrors and runtime-only family/reminder/debt tables.
- It creates no Django-managed table. Django migrations own bot settings,
  feedback (including its implicit tags through table), support, polls, billing,
  admin state, and Mini App managed tables.
- Bootstrap changes schema only. It does not seed categories/settings, repair
  debts, recalculate balances, change account types, send messages, load bot
  configuration, or call an external service. Those existing runtime data
  operations remain with their runtime owner.
- Preserve the inspected core constraints. In particular `users.last_seen_at`,
  `family_members.role`, and AI draft source/media identity are required by the
  real schema. Test fixtures must supply them; do not loosen DDL for a test.

## Clean database / existing migrated database

From `code/admin_service`, with an explicitly configured **local or approved**
PostgreSQL `DATABASE_URL` and normal Django settings:

```sh
python manage.py migrate --noinput
python manage.py migrate --check
```

`users.apps.UsersConfig` registers a `pre_migrate` receiver. It initializes the
core tables in a transaction **before any Django migration runs**, so the first
managed foreign key can safely reference `users`/`accounts`. App import merely
registers the receiver and never accesses a database. PostgreSQL advisory
transaction locks serialize concurrent core initializers. Django migrations
must still run in one dedicated init job, not concurrently in serving workers.

There is deliberately no new predecessor or `run_before` edge on an existing
migration: adding one could invalidate already-applied production history.
Direct `MigrationExecutor` callers bypass management-command signals and must
call `common.runtime_schema.ensure_runtime_schema(using)` first.

### Historical bot-first + fake-initial installations

For an installation with its historical initial migrations already recorded,
run the **same normal `migrate --noinput` command**. New
`feedback.0002_repair_feedback_tags` creates only a missing through table using
the historical Django model. That supplies its primary key, unique pair,
indexes, and both foreign keys. If it already exists, no rows/table are replaced.
The repair is idempotent and its reverse operation is intentionally non-destructive.

`bot_settings.0007_runtime_sql_defaults` transfers the actual old bot SQL
DEFAULT expressions into the Django-owned shared tables. Without those defaults,
a clean Django-created table can migrate successfully yet reject the bot's raw
INSERTs. Mini App install-nudge defaults already belong to `miniapp.0003`.

Do not use blanket `--fake` or `--fake-initial` as a normal bootstrap/upgrade
strategy. An unrecorded bot-only installation needs explicit reviewed adoption
of its exact existing schema/history. Unknown drift or orphaned data is not
silently accepted by this implementation. The legacy audit uses `--fake-initial`
only to reproduce the old defect in a synthetic database, not as the fix.

## Test database runner

Run against a disposable PostgreSQL instance with a role permitted to create
and drop **test** databases. Do not point this command at a production service.
Django creates the separate `test_<DATABASE_URL database name>` database.

```sh
python manage.py test common.tests_schema_batch2 \
  --testrunner=common.test_runner.RuntimeSchemaTestRunner --parallel=1 --noinput
python manage.py test \
  --testrunner=common.test_runner.RuntimeSchemaTestRunner --parallel=1 --noinput
```

The runner uses normal migrations and the same core bootstrap. During tests only,
it adds the explicit runtime table/sequence inventory to Django flush/reset
introspection, including tables with no ORM model. It never flips `managed`,
skips migrations, or copies a prepopulated application database. The patch is
restored on exit. `TEST.NAME` equal to the application DB is rejected, and parallel
worker mode is explicitly unsupported rather than offering unreliable isolation.
SQLite-only unit suites are not evidence of PostgreSQL schema parity.

## Bot and image integration contract

- Make this package importable in both images (e.g. code-root build context,
  `COPY runtime_schema /app/runtime_schema`). Local Django checkout has a
  narrowly scoped sibling-package loader.
- In bot `init_db`, inside `pool.acquire()`, call
  `await runtime_schema.bootstrap_async(conn)` before the retained data backfills.
  Remove its old literal DDL, including every managed-table CREATE. Do not remove
  runtime seeding, accounting repair, pool registration, or job scheduling.
- Compose order: healthy PostgreSQL -> **one completed schema init job**
  (`migrate --noinput && migrate --check`) -> bot/API/admin/worker/beat.
  Remove migrations/fake flags from ordinary serving startup.
- This package does not claim Compose verification; test the actual images and
  startup ordering separately on a Docker-capable environment.

Future core changes must be explicit, idempotent schema upgrades in the shared
package. Core bootstrap is not a financial data migration mechanism. Future
changes to managed shared-table SQL defaults belong to new Django migrations.
