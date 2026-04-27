from __future__ import annotations

import asyncio
import glob
import logging
import os
from pathlib import Path

import asyncpg

logger = logging.getLogger("mcf.migrate")


async def ensure_schema_migrations(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          id TEXT PRIMARY KEY,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )


def _read_migration_files(migrations_dir: str) -> list[Path]:
    pattern = os.path.join(migrations_dir, "*.sql")
    files = [Path(p) for p in glob.glob(pattern)]
    files.sort(key=lambda p: p.name)
    return files


async def run_migrations(pool: asyncpg.Pool, migrations_dir: str) -> None:
    files = _read_migration_files(migrations_dir)
    if not files:
        logger.warning("No migrations found in %s", migrations_dir)
        return

    async with pool.acquire() as conn:
        await ensure_schema_migrations(conn)
        applied = {r["id"] for r in await conn.fetch("SELECT id FROM schema_migrations ORDER BY applied_at ASC")}

    for f in files:
        if f.name in applied:
            continue

        sql = f.read_text(encoding="utf-8")
        logger.info("Applying migration %s", f.name)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute("INSERT INTO schema_migrations (id) VALUES ($1)", f.name)


async def wait_for_db_and_migrate(dsn: str, migrations_dir: str) -> asyncpg.Pool:
    last_err: Exception | None = None
    for attempt in range(30):
        try:
            pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
            await run_migrations(pool, migrations_dir)
            return pool
        except Exception as exc:
            last_err = exc
            wait_s = 1 + attempt * 0.2
            logger.warning("DB/migrate failed (attempt %s/30): %s; retry in %.1fs", attempt + 1, exc, wait_s)
            await asyncio.sleep(wait_s)
    raise RuntimeError(f"DB/migrate failed after retries: {last_err}")
