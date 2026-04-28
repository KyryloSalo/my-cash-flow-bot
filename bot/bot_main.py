from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import asyncpg
from telegram import Update
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

import config
from keyboards import (
    kb_currency,
    kb_home,
    kb_language,
    kb_onb_account_bank,
    kb_onb_account_currency,
    kb_onb_account_last4,
    kb_onb_accounts_more_done,
    kb_onb_start_date,
)
from parsing import parse_tx
from stt import transcribe_ogg_bytes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mcf-bot")

(
    LANG,
    BASE_CURRENCY,
    BASE_CURRENCY_TEXT,
    START_DATE,
    ACC_BANK,
    ACC_BANK_TEXT,
    ACC_LAST4_CHOICE,
    ACC_LAST4_TEXT,
    ACC_CURRENCY,
    ACC_CURRENCY_TEXT,
    ACC_BALANCE,
    ACC_MORE_DONE,
) = range(12)


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
              start_date DATE,
              onboarding_completed BOOLEAN NOT NULL DEFAULT false,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        # Backward-compatible schema upgrades
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_date DATE")
        await conn.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_completed BOOLEAN NOT NULL DEFAULT false"
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
              id BIGSERIAL PRIMARY KEY,
              tg_user_id BIGINT NOT NULL REFERENCES users (tg_user_id) ON DELETE CASCADE,
              label TEXT NOT NULL,
              currency TEXT NOT NULL,
              starting_balance NUMERIC NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
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


def _parse_date_ddmmyyyy(text: str) -> date | None:
    t = (text or "").strip().lower()
    if not t:
        return None
    if t in {"сьогодні", "сегодня", "today"}:
        return datetime.now().date()
    if t in {"вчора", "вчера", "yesterday"}:
        return (datetime.now().date()).fromordinal(datetime.now().date().toordinal() - 1)

    try:
        return datetime.strptime(t, "%d.%m.%Y").date()
    except ValueError:
        return None


def _parse_decimal(text: str) -> Decimal | None:
    t = (text or "").strip().replace(" ", "")
    if not t:
        return None
    # allow comma decimal separator
    t = t.replace(",", ".")
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


async def _show_home(update: Update) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "Привіт! Надішли транзакцію текстом або голосом (≤20с). Напр: `продукти 1000`",
        reply_markup=kb_home(),
        parse_mode="Markdown",
    )


def _reset_onboarding(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("onb", None)
    context.user_data.pop("onb_waiting_date", None)


async def start_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not update.message:
        return ConversationHandler.END

    _reset_onboarding(context)

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

        u = await conn.fetchrow(
            "SELECT onboarding_completed FROM users WHERE tg_user_id=$1",
            user.id,
        )
        accounts_cnt = await conn.fetchval(
            "SELECT count(1) FROM accounts WHERE tg_user_id=$1",
            user.id,
        )

    if u and bool(u.get("onboarding_completed")) and accounts_cnt and int(accounts_cnt) > 0:
        await _show_home(update)
        return ConversationHandler.END

    await update.message.reply_text(
        "Привіт! Давай швидко налаштуємо все під тебе. Обери мову:",
        reply_markup=kb_language(),
    )
    return LANG


async def onb_lang(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return LANG
    await q.answer()

    try:
        _, _, lang = q.data.split(":", 2)
    except Exception:
        return LANG

    context.user_data["onb"] = {"lang": lang}
    await q.message.reply_text("Базова валюта:", reply_markup=kb_currency())
    return BASE_CURRENCY


async def onb_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return BASE_CURRENCY
    await q.answer()

    try:
        _, _, cur = q.data.split(":", 2)
    except Exception:
        return BASE_CURRENCY

    if cur == "OTHER":
        await q.message.reply_text("Введи валюту (напр. UAH, USD, EUR):")
        return BASE_CURRENCY_TEXT

    context.user_data.setdefault("onb", {})["base_currency"] = cur
    await q.message.reply_text("Стартова дата обліку:", reply_markup=kb_onb_start_date())
    return START_DATE


async def onb_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return BASE_CURRENCY_TEXT

    cur = update.message.text.strip().upper()
    if len(cur) != 3 or not cur.isalpha():
        await update.message.reply_text("Потрібно 3 літери, напр. UAH / USD / EUR. Спробуй ще раз:")
        return BASE_CURRENCY_TEXT

    context.user_data.setdefault("onb", {})["base_currency"] = cur
    await update.message.reply_text("Стартова дата обліку:", reply_markup=kb_onb_start_date())
    return START_DATE


async def onb_start_date_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return START_DATE
    await q.answer()

    if q.data == "onb:date:today":
        context.user_data.setdefault("onb", {})["start_date"] = datetime.now().date().isoformat()
        return await _start_accounts_step(q.message, context)

    if q.data == "onb:date:pick":
        context.user_data["onb_waiting_date"] = True
        await q.message.reply_text("Введи дату у форматі dd.mm.yyyy (напр. 27.04.2026):")
        return START_DATE

    return START_DATE


async def onb_start_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not context.user_data.get("onb_waiting_date"):
        return START_DATE
    if not update.message or not update.message.text:
        return START_DATE

    d = _parse_date_ddmmyyyy(update.message.text)
    if not d:
        await update.message.reply_text("Не зрозумів дату. Приклад: 27.04.2026. Спробуй ще раз:")
        return START_DATE

    context.user_data.pop("onb_waiting_date", None)
    context.user_data.setdefault("onb", {})["start_date"] = d.isoformat()
    return await _start_accounts_step(update.message, context)


async def _start_accounts_step(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    onb = context.user_data.setdefault("onb", {})
    onb.setdefault("accounts", [])
    onb["current_account"] = {}
    n = len(onb["accounts"]) + 1
    await message.reply_text(f"Додай рахунок №{n}:", reply_markup=kb_onb_account_bank())
    return ACC_BANK


async def onb_account_bank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_BANK
    await q.answer()

    parts = q.data.split(":")
    if len(parts) != 4:
        return ACC_BANK

    bank = parts[-1]
    if bank == "other":
        await q.message.reply_text("Напиши назву банку/рахунку (напр. Revolut або Картка):")
        return ACC_BANK_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["bank"] = bank

    if bank == "cash":
        context.user_data["onb"]["current_account"]["last4"] = None
        await q.message.reply_text("Валюта рахунку:", reply_markup=kb_onb_account_currency())
        return ACC_CURRENCY

    await q.message.reply_text("Останні 4 цифри картки (опційно):", reply_markup=kb_onb_account_last4())
    return ACC_LAST4_CHOICE


async def onb_account_bank_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BANK_TEXT

    label = update.message.text.strip()
    if not label:
        await update.message.reply_text("Напиши назву (не порожньо), будь ласка:")
        return ACC_BANK_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["bank_label"] = label
    await update.message.reply_text("Останні 4 цифри картки (опційно):", reply_markup=kb_onb_account_last4())
    return ACC_LAST4_CHOICE


async def onb_account_last4_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_LAST4_CHOICE
    await q.answer()

    if q.data == "onb:acct:last4:skip":
        context.user_data.setdefault("onb", {}).setdefault("current_account", {})["last4"] = None
        await q.message.reply_text("Валюта рахунку:", reply_markup=kb_onb_account_currency())
        return ACC_CURRENCY

    if q.data == "onb:acct:last4:enter":
        await q.message.reply_text("Введи 4 цифри (напр. 1234):")
        return ACC_LAST4_TEXT

    return ACC_LAST4_CHOICE


async def onb_account_last4_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_LAST4_TEXT

    t = update.message.text.strip()
    if not (len(t) == 4 and t.isdigit()):
        await update.message.reply_text("Потрібно рівно 4 цифри. Спробуй ще раз:")
        return ACC_LAST4_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["last4"] = t
    await update.message.reply_text("Валюта рахунку:", reply_markup=kb_onb_account_currency())
    return ACC_CURRENCY


async def onb_account_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_CURRENCY
    await q.answer()

    parts = q.data.split(":")
    if len(parts) != 4:
        return ACC_CURRENCY

    cur = parts[-1]
    if cur == "OTHER":
        await q.message.reply_text("Введи валюту рахунку (напр. UAH, USD, EUR):")
        return ACC_CURRENCY_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["currency"] = cur
    await q.message.reply_text("Баланс рахунку (можна з мінусом):")
    return ACC_BALANCE


async def onb_account_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_CURRENCY_TEXT

    cur = update.message.text.strip().upper()
    if len(cur) != 3 or not cur.isalpha():
        await update.message.reply_text("Потрібно 3 літери, напр. UAH / USD / EUR. Спробуй ще раз:")
        return ACC_CURRENCY_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["currency"] = cur
    await update.message.reply_text("Баланс рахунку (можна з мінусом):")
    return ACC_BALANCE


async def onb_account_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return ACC_BALANCE

    bal = _parse_decimal(update.message.text)
    if bal is None:
        await update.message.reply_text("Не схоже на число. Приклад: 1500 або -200.50. Спробуй ще раз:")
        return ACC_BALANCE

    onb = context.user_data.setdefault("onb", {})
    cur_acc = onb.get("current_account") or {}
    currency = (cur_acc.get("currency") or onb.get("base_currency") or "UAH").strip().upper() or "UAH"

    bank = cur_acc.get("bank")
    bank_label = cur_acc.get("bank_label")
    last4 = cur_acc.get("last4")

    if bank == "mono":
        base_label = "Monobank"
    elif bank == "privat":
        base_label = "ПриватБанк"
    elif bank == "cash":
        base_label = "Готівка"
    else:
        base_label = bank_label or "Рахунок"

    label = base_label
    if last4:
        label = f"{label} •{last4}"

    async with _pool(context).acquire() as conn:
        await conn.execute(
            """
            INSERT INTO accounts (tg_user_id, label, currency, starting_balance)
            VALUES ($1, $2, $3, $4)
            """,
            user.id,
            label,
            currency,
            str(bal),
        )

    onb.setdefault("accounts", []).append({"label": label, "currency": currency, "starting_balance": str(bal)})
    onb["current_account"] = {}

    cnt = len(onb.get("accounts") or [])
    await update.message.reply_text(
        f"✅ Додано рахунок ({cnt}/5): {label} ({currency}), баланс {bal}",
        reply_markup=kb_onb_accounts_more_done(),
    )
    return ACC_MORE_DONE


async def onb_accounts_more_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_MORE_DONE
    await q.answer()

    user = update.effective_user
    if not user:
        return ConversationHandler.END

    if q.data == "onb:acct:more":
        onb = context.user_data.setdefault("onb", {})
        if len(onb.get("accounts") or []) >= 5:
            await q.message.reply_text("Ліміт 5 рахунків. Натисни «Готово».", reply_markup=kb_onb_accounts_more_done())
            return ACC_MORE_DONE
        return await _start_accounts_step(q.message, context)

    if q.data == "onb:acct:done":
        onb = context.user_data.get("onb", {})
        lang = onb.get("lang")
        base_currency = (onb.get("base_currency") or "UAH").strip().upper() or "UAH"
        start_date_iso = onb.get("start_date")
        start_date_v = date.fromisoformat(start_date_iso) if start_date_iso else None

        async with _pool(context).acquire() as conn:
            await conn.execute(
                """
                UPDATE users
                SET lang = COALESCE($2, lang),
                    base_currency = COALESCE($3, base_currency),
                    start_date = COALESCE($4, start_date),
                    onboarding_completed = true,
                    last_seen_at = now()
                WHERE tg_user_id=$1
                """,
                user.id,
                lang,
                base_currency,
                start_date_v,
            )

        _reset_onboarding(context)
        await q.message.reply_text(
            "Готово ✅ Тепер можна додавати витрати/доходи текстом або голосом.",
            reply_markup=kb_home(),
        )
        return ConversationHandler.END

    return ACC_MORE_DONE


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
        u = await conn.fetchrow(
            "SELECT base_currency, onboarding_completed FROM users WHERE tg_user_id=$1",
            user.id,
        )
        if not u or not bool(u.get("onboarding_completed")):
            await update.message.reply_text("Спочатку пройди /start (онбординг).")
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

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow(
            "SELECT onboarding_completed FROM users WHERE tg_user_id=$1",
            user.id,
        )
        if not u or not bool(u.get("onboarding_completed")):
            await update.message.reply_text("Спочатку пройди /start (онбординг).")
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
        Application.builder().token(config.BOT_TOKEN).post_init(init_db).post_shutdown(shutdown_db).build()
    )

    onboarding = ConversationHandler(
        entry_points=[CommandHandler("start", start_entry)],
        states={
            LANG: [CallbackQueryHandler(onb_lang, pattern=r"^onb:lang:")],
            BASE_CURRENCY: [CallbackQueryHandler(onb_currency, pattern=r"^onb:cur:")],
            BASE_CURRENCY_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_currency_text)],
            START_DATE: [
                CallbackQueryHandler(onb_start_date_choice, pattern=r"^onb:date:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_start_date_text),
            ],
            ACC_BANK: [CallbackQueryHandler(onb_account_bank, pattern=r"^onb:acct:bank:")],
            ACC_BANK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_bank_text)],
            ACC_LAST4_CHOICE: [CallbackQueryHandler(onb_account_last4_choice, pattern=r"^onb:acct:last4:")],
            ACC_LAST4_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_last4_text)],
            ACC_CURRENCY: [CallbackQueryHandler(onb_account_currency, pattern=r"^onb:acct:cur:")],
            ACC_CURRENCY_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_currency_text)],
            ACC_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_balance)],
            ACC_MORE_DONE: [CallbackQueryHandler(onb_accounts_more_done, pattern=r"^onb:acct:(more|done)$")],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    app.add_handler(onboarding)
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
