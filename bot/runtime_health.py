"""Container dependency probe; NOT a Telegram polling/delivery assertion."""
import asyncio
import os
import asyncpg


async def check():
    conn = await asyncpg.connect(os.environ["DATABASE_URL"], timeout=3, command_timeout=3)
    try:
        assert await conn.fetchval("SELECT 1") == 1
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(check())
    except Exception:
        raise SystemExit(1)
