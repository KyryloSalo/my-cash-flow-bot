import os
import asyncio
import logging

import asyncpg
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mcf-bot")


async def init_db(application: Application) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is missing. Put it into /opt/my-cash-flow-bot/.env")

    last_err: Exception | None = None
    for attempt in range(30):
        try:
            pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)

            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        tg_user_id BIGINT PRIMARY KEY,
                        first_name TEXT,
                        username TEXT,
                        lang TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                )

            application.bot_data["db_pool"] = pool
            logger.info("DB connected")
            return
        except Exception as exc:
            last_err = exc
            wait_s = 1 + attempt * 0.2
            logger.warning("DB connect failed (attempt %s/30): %s; retry in %.1fs", attempt + 1, exc, wait_s)
            await asyncio.sleep(wait_s)

    raise RuntimeError(f"DB connect failed after retries: {last_err}")


async def shutdown_db(application: Application) -> None:
    pool = application.bot_data.get("db_pool")
    if pool is not None:
        await pool.close()
        logger.info("DB pool closed")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    pool: asyncpg.Pool = context.application.bot_data["db_pool"]

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (tg_user_id, first_name, username, lang, last_seen_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (tg_user_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                username = EXCLUDED.username,
                lang = EXCLUDED.lang,
                last_seen_at = now();
            """,
            user.id,
            user.first_name,
            user.username,
            user.language_code,
        )

    await update.message.reply_text("Привіт! DB-first режим ✅")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.text:
        await update.message.reply_text(update.message.text)


def main() -> None:
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise SystemExit("BOT_TOKEN is missing. Put it into /opt/my-cash-flow-bot/.env")

    app = (
        Application.builder()
        .token(token)
        .post_init(init_db)
        .post_shutdown(shutdown_db)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    logger.info("Bot starting (long polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
