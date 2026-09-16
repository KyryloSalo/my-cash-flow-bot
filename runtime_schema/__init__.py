"""Shared PostgreSQL runtime schema, with no Django/Telegram imports or I/O on import."""
from .statements import CORE_STATEMENTS, CORE_TABLES

__all__ = ["BOOTSTRAP_LOCK_SQL", "CORE_STATEMENTS", "CORE_TABLES", "bootstrap", "bootstrap_async"]

# Stable application-scoped lock; both adapters use the same transaction lock.
BOOTSTRAP_LOCK_SQL = "SELECT pg_advisory_xact_lock(19481021, 2)"


def bootstrap(execute):
    """Apply core-only DDL using a cursor.execute inside the caller's transaction.

    Deliberately excludes seeds, financial recalculation and managed tables.
    The lock prevents concurrent first boots racing PostgreSQL catalog creation.
    """
    execute(BOOTSTRAP_LOCK_SQL)
    for statement in CORE_STATEMENTS:
        execute(statement)


async def bootstrap_async(connection):
    """Asyncpg adapter; commit all DDL together or roll it all back."""
    async with connection.transaction():
        await connection.execute(BOOTSTRAP_LOCK_SQL)
        for statement in CORE_STATEMENTS:
            await connection.execute(statement)
