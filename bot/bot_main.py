from __future__ import annotations

import asyncio
import logging
from datetime import datetime

import asyncpg
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

import config
from keyboards import kb_home
from parsing import parse_tx
from stt import transcribe_ogg_bytes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mcf-bot")


async def _connect_pool(dsn: str) -> asyncpg.Pool:
    last_err: Exception | None = None
    for attempt in range(30):
        try:
            return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        except Exception as exc:
            last_err = exc
            wait_s = 1 + attempt * 0.2
            logger.warning("DB connect failed (attempt %s/30): %s; retry in %.1fs", attempt + 1, exc, wait_s)
            await asyncio.sleep(wait_s)
    raise RuntimeError(f"DB connect failed after retries: {last_err}")


async def init_db(application: Application) -> None:
    if not config.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing. Put it into /opt/my-cash-flow-bot/.env")

    pool = await _connect_pool(config.DATABASE_URL)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              tg_user_id BIGINT PRIMARY KEY,
              first_name TEXT,
              username TEXT,
              lang TEXT,
              base_currency TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
              id BIGSERIAL PRIMARY KEY,
              tg_user_id BIGINT NOT NULL REFERENCES users (tg_user_id) ON DELETE CASCADE,
              date DATE NOT NULL,
              type TEXT NOT NULL,
              amount NUMERIC NOT NULL,
              currency TEXT NOT NULL,
              comment TEXT,
              source TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )

    application.bot_data["db_pool"] = pool
    logger.info("DB connected")


async def shutdown_db(application: Application) -> None:
    pool: asyncpg.Pool | None = application.bot_data.get("db_pool")
    if pool is not None:
        await pool.close()
        logger.info("DB pool closed")


def _pool(context: ContextTypes.DEFAULT_TYPE) -> asyncpg.Pool:
    return context.application.bot_data["db_pool"]


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    async with _pool(context).acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (tg_user_id, first_name, username, lang, base_currency, last_seen_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'UAH'), now())
            ON CONFLICT (tg_user_id) DO UPDATE SET
              first_name = EXCLUDED.first_name,
              username = EXCLUDED.username,
              lang = COALESCE(EXCLUDED.lang, users.lang),
              base_currency = COALESCE(users.base_currency, EXCLUDED.base_currency),
              last_seen_at = now();
            """,
            user.id,
            user.first_name,
            user.username,
            user.language_code,
            "UAH",
        )

    await update.message.reply_text(
        "Привіт! Надішли транзакцію текстом або голосом (≤20с). Напр: `продукти 1000`",
        reply_markup=kb_home(),
        parse_mode="Markdown",
    )


async def home_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()
    try:
        _, action, value = q.data.split(":", 2)
    except ValueError:
        return
    if action == "add":
        context.user_data["add_mode"] = value
        await q.message.reply_text(
            f"Ок. Режим: `{value}`. Напиши транзакцію текстом або голосом.",
            parse_mode="Markdown",
        )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow("SELECT base_currency FROM users WHERE tg_user_id=$1", user.id)
        if not u:
            await update.message.reply_text("Спочатку /start")
            return
        base_currency = (u.get("base_currency") or "UAH").strip() or "UAH"

    mode = context.user_data.get("add_mode") or "expense"
    draft = parse_tx(
        update.message.text,
        default_date=datetime.now().date(),
        default_currency=base_currency,
        default_type=mode,
    )

    if draft.amount is None or draft.amount <= 0:
        await update.message.reply_text("Не бачу суму. Приклад: `продукти 1000`", parse_mode="Markdown")
        return

    async with _pool(context).acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transactions (tg_user_id, date, type, amount, currency, comment, source)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            user.id,
            draft.date,
            draft.type,
            float(draft.amount),
            draft.currency,
            draft.comment,
            "text",
        )

    await update.message.reply_text("Збережено ✅", reply_markup=kb_home())


async def voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.voice:
        return

    if update.message.voice.duration and update.message.voice.duration > 20:
        await update.message.reply_text("Голосове має бути ≤20с. Спробуй коротше або надішли текстом.")
        return

    if not config.OPENAI_API_KEY:
        await update.message.reply_text("Голосові поки не налаштовані (OPENAI_API_KEY відсутній).")
        return

    await update.message.chat.send_action(ChatAction.TYPING)
    f = await update.message.voice.get_file()
    ogg = await f.download_as_bytearray()

    try:
        res = await transcribe_ogg_bytes(bytes(ogg))
    except Exception as exc:
        await update.message.reply_text(f"STT помилка: {exc}")
        return

    await update.message.reply_text(f"Розпізнав: {res.text}")

    original_text = getattr(update.message, "text", None)
    update.message.text = res.text  # type: ignore[assignment]
    try:
        await text_message(update, context)
    finally:
        update.message.text = original_text  # type: ignore[assignment]


def build_app() -> Application:
    if not config.BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Put it into /opt/my-cash-flow-bot/.env")

    app = (
        Application.builder()
        .token(config.BOT_TOKEN)
        .post_init(init_db)
        .post_shutdown(shutdown_db)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(home_callback, pattern=r"^home:"))
    app.add_handler(MessageHandler(filters.VOICE, voice_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    return app


def main() -> None:
    app = build_app()
    logger.info("Bot starting (long polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
