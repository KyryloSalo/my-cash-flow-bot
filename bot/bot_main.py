from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
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
    kb_debts_menu,
    kb_home,
    kb_language,
    kb_onb_account_bank,
    kb_onb_account_last4,
    kb_onb_accounts_more_done,
    kb_onb_confirm,
    kb_onb_edit_accounts,
    kb_onb_start_date,
    kb_pick_account,
    kb_pick_category,
    kb_reports_menu,
)
from parsing import parse_amount, parse_tx
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
    ONB_CONFIRM,
    ONB_EDIT_ACCOUNTS,
) = range(14)


DEFAULT_EXPENSE_CATEGORIES = [
    "Продукти",
    "Кафе/ресторани",
    "Транспорт",
    "Таксі",
    "Побут",
    "Здоровʼя",
    "Одяг",
    "Підписки",
    "Інше",
]
DEFAULT_INCOME_CATEGORIES = [
    "Зарплата",
    "Фріланс",
    "Подарунок",
    "Кешбек",
    "Інше",
]


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
            CREATE TABLE IF NOT EXISTS categories (
              id BIGSERIAL PRIMARY KEY,
              tg_user_id BIGINT NOT NULL REFERENCES users (tg_user_id) ON DELETE CASCADE,
              kind TEXT NOT NULL,
              name TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_unique ON categories (tg_user_id, kind, name)"
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
        await conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS account_id BIGINT")
        await conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category_id BIGINT")
        await conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS flow_kind TEXT NOT NULL DEFAULT 'normal'")
        await conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS counterparty TEXT")
        await conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS debt_action TEXT")

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
    t = t.replace(",", ".")
    try:
        return Decimal(t)
    except InvalidOperation:
        return None


def _is_onboarding_in_progress(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get("onb")) and not bool(context.user_data.get("onb_finished"))


async def _ensure_default_categories(conn: asyncpg.Connection, tg_user_id: int) -> None:
    for kind, names in (
        ("expense", DEFAULT_EXPENSE_CATEGORIES),
        ("income", DEFAULT_INCOME_CATEGORIES),
    ):
        for name in names:
            await conn.execute(
                "INSERT INTO categories (tg_user_id, kind, name) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                tg_user_id,
                kind,
                name,
            )


async def _get_accounts(conn: asyncpg.Connection, tg_user_id: int) -> list[tuple[int, str]]:
    rows = await conn.fetch(
        "SELECT id, label, currency FROM accounts WHERE tg_user_id=$1 ORDER BY id ASC",
        tg_user_id,
    )
    out: list[tuple[int, str]] = []
    for r in rows:
        label = str(r["label"])
        cur = str(r["currency"])
        out.append((int(r["id"]), f"{label} ({cur})"))
    return out


async def _get_categories(conn: asyncpg.Connection, tg_user_id: int, kind: str) -> list[tuple[int, str]]:
    rows = await conn.fetch(
        "SELECT id, name FROM categories WHERE tg_user_id=$1 AND kind=$2 ORDER BY name ASC",
        tg_user_id,
        kind,
    )
    return [(int(r["id"]), str(r["name"])) for r in rows]


async def _show_home(update: Update) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "Головне меню. Обери дію нижче.",
        reply_markup=kb_home(),
    )


def _reset_onboarding(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("onb", None)
    context.user_data.pop("onb_waiting_date", None)
    context.user_data.pop("onb_finished", None)


def _reset_tx_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("tx_flow", None)


def _reset_debt_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("debt_flow", None)


async def start_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not update.message:
        return ConversationHandler.END

    _reset_onboarding(context)
    _reset_tx_flow(context)
    _reset_debt_flow(context)

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
        "Привіт! Це My Cash Flow Bot.\n\nСпочатку налаштуємо базові речі (2–3 хв): мову, валюту, стартову дату та твої рахунки.\n\nОбери мову:",
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
    await q.message.reply_text(
        "Базова валюта — у ній бот показуватиме звіти за замовчуванням. Обери валюту:",
        reply_markup=kb_currency("onb:cur"),
    )
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
        await q.message.reply_text("Введи валюту 3 літерами (напр. UAH, USD, EUR):")
        return BASE_CURRENCY_TEXT

    context.user_data.setdefault("onb", {})["base_currency"] = cur
    await q.message.reply_text(
        "Стартова дата обліку — з якого дня рахувати звіти.\n\nОбери варіант:",
        reply_markup=kb_onb_start_date(),
    )
    return START_DATE


async def onb_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return BASE_CURRENCY_TEXT

    cur = update.message.text.strip().upper()
    if len(cur) != 3 or not cur.isalpha():
        await update.message.reply_text("Потрібно 3 літери, напр. UAH / USD / EUR. Спробуй ще раз:")
        return BASE_CURRENCY_TEXT

    context.user_data.setdefault("onb", {})["base_currency"] = cur
    await update.message.reply_text(
        "Стартова дата обліку — з якого дня рахувати звіти.\n\nОбери варіант:",
        reply_markup=kb_onb_start_date(),
    )
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
    await message.reply_text(
        f"Крок 4/6: Рахунки (1–5)\n\nДодай рахунок №{n}. Обери банк або «Готівка».",
        reply_markup=kb_onb_account_bank(),
    )
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
        await q.message.reply_text("Валюта рахунку:", reply_markup=kb_currency("onb:acct:cur"))
        return ACC_CURRENCY

    await q.message.reply_text(
        "Останні 4 цифри картки — опційно.\n\nНатисни кнопку або просто введи 4 цифри:",
        reply_markup=kb_onb_account_last4(),
    )
    return ACC_LAST4_CHOICE


async def onb_account_bank_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BANK_TEXT

    label = update.message.text.strip()
    if not label:
        await update.message.reply_text("Напиши назву (не порожньо), будь ласка:")
        return ACC_BANK_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["bank_label"] = label
    await update.message.reply_text(
        "Останні 4 цифри картки — опційно.\n\nНатисни кнопку або просто введи 4 цифри:",
        reply_markup=kb_onb_account_last4(),
    )
    return ACC_LAST4_CHOICE


async def onb_account_last4_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_LAST4_CHOICE
    await q.answer()

    if q.data == "onb:acct:last4:skip":
        context.user_data.setdefault("onb", {}).setdefault("current_account", {})["last4"] = None
        await q.message.reply_text("Валюта рахунку:", reply_markup=kb_currency("onb:acct:cur"))
        return ACC_CURRENCY

    if q.data == "onb:acct:last4:enter":
        await q.message.reply_text("Введи 4 цифри (напр. 1234):")
        return ACC_LAST4_TEXT

    return ACC_LAST4_CHOICE


async def onb_account_last4_text_direct(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # User typed something while we were waiting for a button.
    if not update.message or not update.message.text:
        return ACC_LAST4_CHOICE

    t = update.message.text.strip()
    if len(t) == 4 and t.isdigit():
        context.user_data.setdefault("onb", {}).setdefault("current_account", {})["last4"] = t
        await update.message.reply_text("Валюта рахунку:", reply_markup=kb_currency("onb:acct:cur"))
        return ACC_CURRENCY

    await update.message.reply_text(
        "Я чекаю 4 цифри (напр. 1234) або натисни «Пропустити». Спробуй ще раз:",
        reply_markup=kb_onb_account_last4(),
    )
    return ACC_LAST4_CHOICE


async def onb_account_last4_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_LAST4_TEXT

    t = update.message.text.strip()
    if not (len(t) == 4 and t.isdigit()):
        await update.message.reply_text("Потрібно рівно 4 цифри. Приклад: 1234. Спробуй ще раз:")
        return ACC_LAST4_TEXT

    context.user_data.setdefault("onb", {}).setdefault("current_account", {})["last4"] = t
    await update.message.reply_text("Валюта рахунку:", reply_markup=kb_currency("onb:acct:cur"))
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
        await q.message.reply_text("Введи валюту рахунку 3 літерами (напр. UAH, USD, EUR):")
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
            "INSERT INTO accounts (tg_user_id, label, currency, starting_balance) VALUES ($1, $2, $3, $4)",
            user.id,
            label,
            currency,
            str(bal),
        )
        await _ensure_default_categories(conn, user.id)

    onb.setdefault("accounts", []).append({"label": label, "currency": currency, "starting_balance": str(bal)})
    onb["current_account"] = {}

    cnt = len(onb.get("accounts") or [])
    await update.message.reply_text(
        f"✅ Додано рахунок ({cnt}/5): {label} ({currency}), баланс {bal}",
        reply_markup=kb_onb_accounts_more_done(),
    )
    return ACC_MORE_DONE


async def _render_onboarding_summary(conn: asyncpg.Connection, tg_user_id: int) -> str:
    u = await conn.fetchrow(
        "SELECT lang, base_currency, start_date FROM users WHERE tg_user_id=$1",
        tg_user_id,
    )
    accounts = await conn.fetch(
        "SELECT label, currency, starting_balance FROM accounts WHERE tg_user_id=$1 ORDER BY id ASC",
        tg_user_id,
    )
    lang = (u.get("lang") if u else None) or "—"
    base_currency = (u.get("base_currency") if u else None) or "UAH"
    start_date = (u.get("start_date") if u else None)
    start_s = start_date.isoformat() if start_date else "—"

    lines = [
        "Перевір налаштування:",
        f"• Мова: {lang}",
        f"• Базова валюта: {base_currency}",
        f"• Стартова дата: {start_s}",
        "",
        "Рахунки:",
    ]
    if not accounts:
        lines.append("• (немає) — додай хоча б 1 рахунок")
    else:
        for a in accounts:
            lines.append(f"• {a['label']} ({a['currency']}), баланс {a['starting_balance']}")
    return "\n".join(lines)


async def onb_accounts_more_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_MORE_DONE
    await q.answer()

    if q.data == "onb:acct:more":
        onb = context.user_data.setdefault("onb", {})
        if len(onb.get("accounts") or []) >= 5:
            await q.message.reply_text(
                "Ліміт 5 рахунків. Натисни «Готово».",
                reply_markup=kb_onb_accounts_more_done(),
            )
            return ACC_MORE_DONE
        return await _start_accounts_step(q.message, context)

    if q.data == "onb:acct:done":
        user = update.effective_user
        if not user:
            return ConversationHandler.END

        async with _pool(context).acquire() as conn:
            onb = context.user_data.get("onb", {})
            lang = onb.get("lang")
            base_currency = (onb.get("base_currency") or "UAH").strip().upper() or "UAH"
            start_date_iso = onb.get("start_date")
            start_date_v = date.fromisoformat(start_date_iso) if start_date_iso else None

            await conn.execute(
                """
                UPDATE users
                SET lang = COALESCE($2, lang),
                    base_currency = COALESCE($3, base_currency),
                    start_date = COALESCE($4, start_date),
                    last_seen_at = now()
                WHERE tg_user_id=$1
                """,
                user.id,
                lang,
                base_currency,
                start_date_v,
            )

            summary = await _render_onboarding_summary(conn, user.id)

        await q.message.reply_text(
            summary,
            reply_markup=kb_onb_confirm(),
        )
        return ONB_CONFIRM

    return ACC_MORE_DONE


async def onb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CONFIRM
    await q.answer()

    user = update.effective_user
    if not user:
        return ConversationHandler.END

    if q.data == "onb:confirm:restart":
        async with _pool(context).acquire() as conn:
            await conn.execute("DELETE FROM accounts WHERE tg_user_id=$1", user.id)
        _reset_onboarding(context)
        await q.message.reply_text(
            "Ок, починаємо спочатку. Обери мову:",
            reply_markup=kb_language(),
        )
        return LANG

    if q.data == "onb:confirm:edit":
        async with _pool(context).acquire() as conn:
            accounts = await _get_accounts(conn, user.id)
        await q.message.reply_text(
            "Редагування рахунків: можеш видалити помилковий і додати заново.",
            reply_markup=kb_onb_edit_accounts(accounts),
        )
        return ONB_EDIT_ACCOUNTS

    if q.data == "onb:confirm:ok":
        async with _pool(context).acquire() as conn:
            cnt = await conn.fetchval("SELECT count(1) FROM accounts WHERE tg_user_id=$1", user.id)
            if not cnt or int(cnt) <= 0:
                await q.message.reply_text(
                    "Потрібно додати хоча б 1 рахунок.",
                    reply_markup=kb_onb_confirm(),
                )
                return ONB_CONFIRM
            await conn.execute(
                "UPDATE users SET onboarding_completed=true, last_seen_at=now() WHERE tg_user_id=$1",
                user.id,
            )

        context.user_data["onb_finished"] = True
        _reset_onboarding(context)
        await q.message.reply_text(
            "Готово ✅\n\nТепер: натисни «Витрата» або «Дохід» і обери рахунок.",
            reply_markup=kb_home(),
        )
        return ConversationHandler.END

    return ONB_CONFIRM


async def onb_edit_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_EDIT_ACCOUNTS
    await q.answer()

    user = update.effective_user
    if not user:
        return ConversationHandler.END

    if q.data == "onb:edit:back":
        async with _pool(context).acquire() as conn:
            summary = await _render_onboarding_summary(conn, user.id)
        await q.message.reply_text(summary, reply_markup=kb_onb_confirm())
        return ONB_CONFIRM

    if q.data == "onb:edit:add":
        return await _start_accounts_step(q.message, context)

    if q.data.startswith("onb:edit:del:"):
        try:
            account_id = int(q.data.rsplit(":", 1)[-1])
        except ValueError:
            return ONB_EDIT_ACCOUNTS
        async with _pool(context).acquire() as conn:
            await conn.execute(
                "DELETE FROM accounts WHERE tg_user_id=$1 AND id=$2",
                user.id,
                account_id,
            )
            accounts = await _get_accounts(conn, user.id)
        await q.message.reply_text("Ок, видалив.", reply_markup=kb_onb_edit_accounts(accounts))
        return ONB_EDIT_ACCOUNTS

    return ONB_EDIT_ACCOUNTS


async def home_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    try:
        _, action, value = q.data.split(":", 2)
    except ValueError:
        return

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow("SELECT onboarding_completed FROM users WHERE tg_user_id=$1", user.id)
        if not u or not bool(u.get("onboarding_completed")):
            await q.message.reply_text("Спочатку пройди /start (онбординг).")
            return

        if action == "add" and value in {"expense", "income"}:
            accounts = await _get_accounts(conn, user.id)
            if not accounts:
                await q.message.reply_text("Немає рахунків. Запусти /start і додай хоча б один рахунок.")
                return
            _reset_tx_flow(context)
            context.user_data["tx_flow"] = {"kind": value}
            await q.message.reply_text(
                "Крок 1/3: Обери рахунок:",
                reply_markup=kb_pick_account(accounts, value),
            )
            return

        if action == "cmd" and value == "reports":
            await q.message.reply_text("Звіти: обери період", reply_markup=kb_reports_menu())
            return

        if action == "cmd" and value == "debts":
            txt = await _debts_report_text(conn, user.id)
            await q.message.reply_text(txt, reply_markup=kb_debts_menu())
            return

        if action == "cmd" and value == "categories":
            await _ensure_default_categories(conn, user.id)
            exp = await _get_categories(conn, user.id, "expense")
            inc = await _get_categories(conn, user.id, "income")
            msg = ["Категорії:", "", "Витрати:"]
            msg += [f"• {name}" for _, name in exp[:30]]
            msg += ["", "Доходи:"]
            msg += [f"• {name}" for _, name in inc[:30]]
            msg.append("\n(Редагування категорій додамо наступним кроком.)")
            await q.message.reply_text("\n".join(msg))
            return

        await _show_home(update)


async def pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if not context.user_data.get("tx_flow"):
        await q.message.reply_text("Почни з «Витрата» або «Дохід».", reply_markup=kb_home())
        return

    parts = q.data.split(":")
    if len(parts) < 3:
        return

    async with _pool(context).acquire() as conn:
        await _ensure_default_categories(conn, user.id)

        if q.data == "pick:acct:back":
            _reset_tx_flow(context)
            await q.message.reply_text("Ок.", reply_markup=kb_home())
            return

        if parts[0] == "pick" and parts[1] == "acct" and len(parts) == 5:
            kind = parts[2]
            account_id = int(parts[3])
            context.user_data["tx_flow"]["account_id"] = account_id
            categories = await _get_categories(conn, user.id, kind)
            await q.message.reply_text(
                "Крок 2/3: Обери категорію:",
                reply_markup=kb_pick_category(categories, kind),
            )
            return

        if q.data == "pick:cat:back":
            kind = context.user_data["tx_flow"]["kind"]
            accounts = await _get_accounts(conn, user.id)
            await q.message.reply_text("Крок 1/3: Обери рахунок:", reply_markup=kb_pick_account(accounts, kind))
            return

        if parts[0] == "pick" and parts[1] == "cat" and len(parts) == 5:
            kind = parts[2]
            category_id = int(parts[3])
            context.user_data["tx_flow"]["category_id"] = category_id
            context.user_data["tx_flow"]["await_amount"] = True
            await q.message.reply_text(
                "Крок 3/3: Напиши суму (можна з коментарем).\nПриклад: `продукти 1000`\n\nАбо надиктуй голосом.",
                parse_mode="Markdown",
            )
            return


def _today_range() -> tuple[date, date]:
    start = datetime.now().date()
    return start, start + timedelta(days=1)


def _range_for_key(key: str) -> tuple[date, date, str]:
    today = datetime.now().date()
    if key == "today":
        s, e = _today_range()
        return s, e, "Сьогодні"
    if key == "7d":
        s = today - timedelta(days=6)
        return s, today + timedelta(days=1), "Останні 7 днів"
    if key == "30d":
        s = today - timedelta(days=29)
        return s, today + timedelta(days=1), "Останні 30 днів"
    if key == "3m":
        s = today - timedelta(days=90)
        return s, today + timedelta(days=1), "Останні 3 місяці"
    if key == "6m":
        s = today - timedelta(days=180)
        return s, today + timedelta(days=1), "Останні 6 місяців"
    if key == "month":
        s = date(today.year, today.month, 1)
        if today.month == 12:
            e = date(today.year + 1, 1, 1)
        else:
            e = date(today.year, today.month + 1, 1)
        return s, e, "Поточний місяць"
    return today, today + timedelta(days=1), "Сьогодні"


async def reports_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if q.data == "reports:back":
        await q.message.reply_text("Ок.", reply_markup=kb_home())
        return

    if not q.data.startswith("reports:range:"):
        return

    key = q.data.split(":")[-1]
    start_d, end_d, title = _range_for_key(key)

    async with _pool(context).acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT type, sum(amount) AS total
            FROM transactions
            WHERE tg_user_id=$1 AND date >= $2 AND date < $3 AND flow_kind='normal'
            GROUP BY type
            """,
            user.id,
            start_d,
            end_d,
        )

    totals = {r["type"]: float(r["total"] or 0) for r in rows}
    exp = totals.get("expense", 0.0)
    inc = totals.get("income", 0.0)

    await q.message.reply_text(
        f"Звіт: {title}\n\nВитрати: {exp:.2f}\nДоходи: {inc:.2f}",
        reply_markup=kb_reports_menu(),
    )


async def _debts_report_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    rows = await conn.fetch(
        """
        SELECT counterparty, currency,
               sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
               sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
        FROM transactions
        WHERE tg_user_id=$1 AND flow_kind='debt'
        GROUP BY counterparty, currency
        """,
        tg_user_id,
    )

    owed_to_me: list[str] = []
    i_owe: list[str] = []
    for r in rows:
        cp = (r["counterparty"] or "—").strip() or "—"
        cur = (r["currency"] or "").strip() or ""
        o = float(r["owed_to_me"] or 0)
        i = float(r["i_owe"] or 0)
        if o > 0:
            owed_to_me.append(f"• {cp}: {o:.2f} {cur}")
        if i > 0:
            i_owe.append(f"• {cp}: {i:.2f} {cur}")

    text = ["Борги (залишок):", ""]
    text.append("Мені винні:" if owed_to_me else "Мені винні: —")
    text += owed_to_me[:10]
    text.append("")
    text.append("Я винен:" if i_owe else "Я винен: —")
    text += i_owe[:10]
    return "\n".join(text)


async def debts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if q.data == "debts:back":
        _reset_debt_flow(context)
        await q.message.reply_text("Ок.", reply_markup=kb_home())
        return

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow("SELECT base_currency, onboarding_completed FROM users WHERE tg_user_id=$1", user.id)
        if not u or not bool(u.get("onboarding_completed")):
            await q.message.reply_text("Спочатку пройди /start (онбординг).")
            return
        base_currency = (u.get("base_currency") or "UAH").strip().upper() or "UAH"

    if q.data.startswith("debts:add:"):
        direction = q.data.split(":")[-1]
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "base_currency": base_currency, "repay": False}
        await q.message.reply_text("Імʼя контрагента (напр. Сергій):")
        return

    if q.data.startswith("debts:repay:"):
        direction = q.data.split(":")[-1]
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "base_currency": base_currency, "repay": True}
        await q.message.reply_text("Імʼя контрагента (кому/хто повертає):")
        return


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    if _is_onboarding_in_progress(context):
        await update.message.reply_text("Зараз триває онбординг. Пройди кроки вище або /start щоб почати заново.")
        return

    if context.user_data.get("debt_flow"):
        await _debt_flow_text(update, context)
        return

    tx_flow = context.user_data.get("tx_flow")
    if tx_flow and tx_flow.get("await_amount"):
        await _tx_amount_text(update, context)
        return

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow(
            "SELECT base_currency, onboarding_completed FROM users WHERE tg_user_id=$1",
            user.id,
        )
        if not u or not bool(u.get("onboarding_completed")):
            await update.message.reply_text("Спочатку пройди /start (онбординг).")
            return

    await update.message.reply_text("Натисни «Витрата» або «Дохід», обери рахунок і категорію — тоді введи суму.")


async def _tx_amount_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    tx_flow = context.user_data.get("tx_flow") or {}
    kind = tx_flow.get("kind")
    account_id = tx_flow.get("account_id")
    category_id = tx_flow.get("category_id")

    amount = parse_amount(update.message.text)
    if amount is None or amount <= 0:
        await update.message.reply_text("Не бачу суму. Приклад: `продукти 1000`", parse_mode="Markdown")
        return

    async with _pool(context).acquire() as conn:
        u = await conn.fetchrow("SELECT base_currency FROM users WHERE tg_user_id=$1", user.id)
        base_currency = (u.get("base_currency") if u else None) or "UAH"

        await conn.execute(
            """
            INSERT INTO transactions (tg_user_id, date, type, amount, currency, comment, source, account_id, category_id, flow_kind)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'normal')
            """,
            user.id,
            datetime.now().date(),
            kind,
            float(amount),
            base_currency,
            (update.message.text or "").strip()[:500] or None,
            "text",
            int(account_id) if account_id is not None else None,
            int(category_id) if category_id is not None else None,
        )

    _reset_tx_flow(context)
    await update.message.reply_text("Збережено ✅", reply_markup=kb_home())


async def _debt_flow_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    flow = context.user_data.get("debt_flow") or {}
    step = flow.get("step")

    if step == "name":
        name = update.message.text.strip()[:80]
        if not name:
            await update.message.reply_text("Напиши імʼя (не порожньо), будь ласка:")
            return
        flow["name"] = name
        flow["step"] = "amount"
        context.user_data["debt_flow"] = flow
        await update.message.reply_text("Сума (число, можна з комою/крапкою):")
        return

    if step == "amount":
        amount = parse_amount(update.message.text)
        if amount is None or amount <= 0:
            await update.message.reply_text("Не схоже на суму. Приклад: 500 або 1200.50. Спробуй ще раз:")
            return

        direction = flow.get("direction")
        repay = bool(flow.get("repay"))
        base_currency = (flow.get("base_currency") or "UAH").strip().upper() or "UAH"
        counterparty = flow.get("name")

        if direction == "owed_to_me":
            debt_action = "lend_repaid" if repay else "lend"
        else:
            debt_action = "borrow_repaid" if repay else "borrow"

        async with _pool(context).acquire() as conn:
            await conn.execute(
                """
                INSERT INTO transactions (tg_user_id, date, type, amount, currency, comment, source, flow_kind, counterparty, debt_action)
                VALUES ($1, $2, 'transfer', $3, $4, $5, $6, 'debt', $7, $8)
                """,
                user.id,
                datetime.now().date(),
                float(amount),
                base_currency,
                None,
                "text",
                counterparty,
                debt_action,
            )
            txt = await _debts_report_text(conn, user.id)

        _reset_debt_flow(context)
        await update.message.reply_text("Готово ✅")
        await update.message.reply_text(txt, reply_markup=kb_debts_menu())
        return


async def voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.voice:
        return

    if _is_onboarding_in_progress(context):
        await update.message.reply_text("Зараз триває онбординг. Спочатку заверши його.")
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

    app = Application.builder().token(config.BOT_TOKEN).post_init(init_db).post_shutdown(shutdown_db).build()

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
            ACC_LAST4_CHOICE: [
                CallbackQueryHandler(onb_account_last4_choice, pattern=r"^onb:acct:last4:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_last4_text_direct),
            ],
            ACC_LAST4_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_last4_text)],
            ACC_CURRENCY: [CallbackQueryHandler(onb_account_currency, pattern=r"^onb:acct:cur:")],
            ACC_CURRENCY_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_currency_text)],
            ACC_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_balance)],
            ACC_MORE_DONE: [CallbackQueryHandler(onb_accounts_more_done, pattern=r"^onb:acct:(more|done)$")],
            ONB_CONFIRM: [CallbackQueryHandler(onb_confirm, pattern=r"^onb:confirm:")],
            ONB_EDIT_ACCOUNTS: [CallbackQueryHandler(onb_edit_accounts, pattern=r"^onb:edit:")],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    app.add_handler(onboarding)

    app.add_handler(CallbackQueryHandler(pick_callback, pattern=r"^pick:"))
    app.add_handler(CallbackQueryHandler(reports_callback, pattern=r"^reports:"))
    app.add_handler(CallbackQueryHandler(debts_callback, pattern=r"^debts:"))

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
