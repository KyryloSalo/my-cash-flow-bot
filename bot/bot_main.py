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
    kb_onb_categories,
    kb_onb_confirm,
    kb_onb_edit_accounts,
    kb_onb_start_date,
    kb_pick_account,
    kb_pick_category,
    kb_reports_menu,
)
from parsing import parse_amount
from stt import transcribe_ogg_bytes

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mcf-bot")

CURRENT_ONBOARDING_VERSION = 2

(
    LANG,
    BASE_CURRENCY,
    START_DATE,
    ACC_BANK,
    ACC_BANK_TEXT,
    ACC_LAST4_CHOICE,
    ACC_LAST4_TEXT,
    ACC_CURRENCY,
    ACC_BALANCE,
    ACC_MORE_DONE,
    ONB_CATS,
    ONB_CONFIRM,
    ONB_EDIT_ACCOUNTS,
) = range(13)

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
MIN_EXPENSE_CATEGORIES = DEFAULT_EXPENSE_CATEGORIES[:5]
MIN_INCOME_CATEGORIES = DEFAULT_INCOME_CATEGORIES[:3]


async def _connect_pool(dsn: str) -> asyncpg.Pool:
    last_error: Exception | None = None
    for attempt in range(30):
        try:
            return await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
        except Exception as exc:
            last_error = exc
            wait_seconds = 1 + attempt * 0.2
            logger.warning("DB connect failed (%s/30): %s; retry in %.1fs", attempt + 1, exc, wait_seconds)
            await asyncio.sleep(wait_seconds)
    raise RuntimeError(f"DB connect failed after retries: {last_error}")


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
              onboarding_version INT NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_date DATE")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_completed BOOLEAN NOT NULL DEFAULT false")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_version INT NOT NULL DEFAULT 0")

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
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_unique ON categories (tg_user_id, kind, name)")

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
              account_id BIGINT,
              category_id BIGINT,
              flow_kind TEXT NOT NULL DEFAULT 'normal',
              counterparty TEXT,
              debt_action TEXT,
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
    pool = application.bot_data.get("db_pool")
    if pool is not None:
        await pool.close()
        logger.info("DB pool closed")


def _pool(context: ContextTypes.DEFAULT_TYPE) -> asyncpg.Pool:
    return context.application.bot_data["db_pool"]


def _parse_date_ddmmyyyy(text: str) -> date | None:
    value = (text or "").strip().lower()
    if not value:
        return None
    if value in {"сьогодні", "сегодня", "today"}:
        return datetime.now().date()
    if value in {"вчора", "вчера", "yesterday"}:
        return datetime.now().date() - timedelta(days=1)
    try:
        return datetime.strptime(value, "%d.%m.%Y").date()
    except ValueError:
        return None


def _parse_decimal(text: str) -> Decimal | None:
    value = (text or "").strip().replace(" ", "").replace(",", ".")
    if not value:
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def _reset_onboarding(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("onb", None)


def _reset_tx_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("tx_flow", None)


def _reset_debt_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("debt_flow", None)


def _reset_runtime_flows(context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_tx_flow(context)
    _reset_debt_flow(context)


def _onboarding_active(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get("onb"))


async def _get_user(conn: asyncpg.Connection, tg_user_id: int) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM users WHERE tg_user_id=$1", tg_user_id)


async def _get_accounts(conn: asyncpg.Connection, tg_user_id: int) -> list[tuple[int, str]]:
    rows = await conn.fetch(
        "SELECT id, label, currency FROM accounts WHERE tg_user_id=$1 ORDER BY id ASC",
        tg_user_id,
    )
    return [(int(row["id"]), f"{row['label']} ({row['currency']})") for row in rows]


async def _get_categories(conn: asyncpg.Connection, tg_user_id: int, kind: str) -> list[tuple[int, str]]:
    rows = await conn.fetch(
        "SELECT id, name FROM categories WHERE tg_user_id=$1 AND kind=$2 ORDER BY name ASC",
        tg_user_id,
        kind,
    )
    return [(int(row["id"]), str(row["name"])) for row in rows]


async def _user_ready(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    user = await _get_user(conn, tg_user_id)
    if not user:
        return False
    accounts_count = await conn.fetchval("SELECT count(1) FROM accounts WHERE tg_user_id=$1", tg_user_id)
    return bool(
        user.get("onboarding_completed")
        and int(user.get("onboarding_version") or 0) >= CURRENT_ONBOARDING_VERSION
        and user.get("start_date")
        and user.get("base_currency")
        and accounts_count
        and int(accounts_count) > 0
    )


async def _seed_categories(conn: asyncpg.Connection, tg_user_id: int, choice: str) -> None:
    await conn.execute("DELETE FROM categories WHERE tg_user_id=$1", tg_user_id)
    if choice == "empty":
        return

    if choice == "minimal":
        expense_names = MIN_EXPENSE_CATEGORIES
        income_names = MIN_INCOME_CATEGORIES
    else:
        expense_names = DEFAULT_EXPENSE_CATEGORIES
        income_names = DEFAULT_INCOME_CATEGORIES

    for kind, names in (("expense", expense_names), ("income", income_names)):
        for name in names:
            await conn.execute(
                "INSERT INTO categories (tg_user_id, kind, name) VALUES ($1, $2, $3)",
                tg_user_id,
                kind,
                name,
            )


async def _ensure_default_categories(conn: asyncpg.Connection, tg_user_id: int) -> None:
    expense_count = await conn.fetchval("SELECT count(1) FROM categories WHERE tg_user_id=$1 AND kind='expense'", tg_user_id)
    income_count = await conn.fetchval("SELECT count(1) FROM categories WHERE tg_user_id=$1 AND kind='income'", tg_user_id)
    if expense_count and int(expense_count) > 0 and income_count and int(income_count) > 0:
        return
    await _seed_categories(conn, tg_user_id, "standard")


async def _reply_home(message, text: str = "🏠 Головне меню. Обери дію нижче.") -> None:
    await message.reply_text(text, reply_markup=kb_home())


async def _reply_and_return_home(message, text: str) -> None:
    await message.reply_text(text)
    await _reply_home(message)


async def _show_home(update: Update) -> None:
    if update.message:
        await _reply_home(update.message)
    elif update.callback_query:
        await _reply_home(update.callback_query.message)


def _format_decimal(value: Decimal | float | int | None) -> str:
    if value is None:
        return "0"
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    normalized = decimal_value.quantize(Decimal("0.01"))
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


async def _render_accounts_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    accounts = await conn.fetch(
        "SELECT label, currency, starting_balance FROM accounts WHERE tg_user_id=$1 ORDER BY id ASC",
        tg_user_id,
    )
    if not accounts:
        return "Рахунків поки немає. Додай хоча б один рахунок."
    lines = ["Поточні рахунки:"]
    for row in accounts:
        lines.append(f"- {row['label']} ({row['currency']}), баланс {_format_decimal(row['starting_balance'])}")
    return "\n".join(lines)


async def _render_onboarding_summary(conn: asyncpg.Connection, tg_user_id: int, cats_choice: str) -> str:
    user = await _get_user(conn, tg_user_id)
    accounts_text = await _render_accounts_text(conn, tg_user_id)
    cats_label = {
        "standard": "Стандарт UA",
        "minimal": "Мінімальний",
        "empty": "Порожньо",
    }.get(cats_choice, "Стандарт UA")
    start_date = user.get("start_date") if user else None
    return "\n".join(
        [
            "Крок 6/6: Підтвердження.",
            "",
            "Перевір налаштування:",
            f"- Мова: {(user.get('lang') if user else None) or '—'}",
            f"- Базова валюта: {(user.get('base_currency') if user else None) or '—'}",
            f"- Стартова дата: {start_date.isoformat() if start_date else '—'}",
            f"- Пакет категорій: {cats_label}",
            "",
            accounts_text,
            "",
            "Якщо все ок — підтверди. Якщо ні, відкрий редагування рахунків або почни спочатку.",
        ]
    )


async def _show_onboarding_summary(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    cats_choice = context.user_data.get("onb", {}).get("cats_choice", "standard")
    async with _pool(context).acquire() as conn:
        summary = await _render_onboarding_summary(conn, tg_user_id, cats_choice)
    await message.reply_text(summary, reply_markup=kb_onb_confirm())
    return ONB_CONFIRM


async def _start_accounts_step(message, context: ContextTypes.DEFAULT_TYPE, force_add: bool = False) -> int:
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        accounts = await _get_accounts(conn, tg_user_id)

    if accounts and not force_add:
        lines = [
            "Крок 4/6: Рахунки.",
            "",
            "Ось рахунки, які вже налаштовані. Можеш додати ще один або завершити цей крок.",
            "",
        ]
        lines.extend(f"- {label}" for _, label in accounts)
        await message.reply_text("\n".join(lines), reply_markup=kb_onb_accounts_more_done())
        return ACC_MORE_DONE

    next_number = len(accounts) + 1
    context.user_data.setdefault("onb", {})["current_account"] = {}
    await message.reply_text(
        f"Крок 4/6: Додай рахунок №{next_number}.\n\nОбери банк або «Готівка».",
        reply_markup=kb_onb_account_bank(),
    )
    return ACC_BANK


async def start_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not update.message:
        return ConversationHandler.END

    context.user_data["tg_user_id"] = user.id
    _reset_onboarding(context)
    _reset_runtime_flows(context)

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
        ready = await _user_ready(conn, user.id)

    if ready:
        await _show_home(update)
        return ConversationHandler.END

    context.user_data["onb"] = {}
    await update.message.reply_text(
        "Привіт! Це My Cash Flow Bot.\n\n"
        "Зараз швидко пройдемо стартове налаштування: мова, базова валюта, стартова дата, рахунки та пакет категорій.\n\n"
        "Крок 1/6: Обери мову.",
        reply_markup=kb_language(),
    )
    return LANG


async def onb_lang(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return LANG
    await q.answer()
    lang = q.data.rsplit(":", 1)[-1]
    context.user_data.setdefault("onb", {})["lang"] = lang
    await q.message.reply_text(
        "Крок 2/6: Базова валюта.\n\nУ цій валюті бот показуватиме звіти та підказки за замовчуванням.",
        reply_markup=kb_currency("onb:cur"),
    )
    return BASE_CURRENCY


async def _save_base_currency(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    currency = value.strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        await message.reply_text("Потрібно 3 літери, напр. UAH / USD / EUR. Спробуй ще раз:")
        return BASE_CURRENCY
    context.user_data.setdefault("onb", {})["base_currency"] = currency
    await message.reply_text(
        "Крок 3/6: Стартова дата.\n\n"
        "З якого дня рахувати звіти? Можеш натиснути кнопку або ввести дату у форматі `dd.mm.yyyy`.",
        reply_markup=kb_onb_start_date(),
        parse_mode="Markdown",
    )
    return START_DATE


async def onb_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return BASE_CURRENCY
    await q.answer()
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text("Введи валюту 3 літерами, напр. UAH, USD, EUR:")
        return BASE_CURRENCY
    return await _save_base_currency(q.message, context, value)


async def onb_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return BASE_CURRENCY
    return await _save_base_currency(update.message, context, update.message.text)


async def _save_start_date(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    parsed = _parse_date_ddmmyyyy(value)
    if not parsed:
        await message.reply_text("Не зрозумів дату. Приклад: 27.04.2026 або «сьогодні». Спробуй ще раз:")
        return START_DATE

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    context.user_data.setdefault("onb", {})["start_date"] = parsed.isoformat()

    async with _pool(context).acquire() as conn:
        await conn.execute(
            "UPDATE users SET lang=$2, base_currency=$3, start_date=$4, onboarding_completed=false, onboarding_version=0 WHERE tg_user_id=$1",
            tg_user_id,
            context.user_data["onb"].get("lang"),
            context.user_data["onb"].get("base_currency"),
            parsed,
        )

    return await _start_accounts_step(message, context)


async def onb_start_date_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return START_DATE
    await q.answer()
    value = q.data.rsplit(":", 1)[-1]
    if value == "pick":
        await q.message.reply_text("Введи дату у форматі `dd.mm.yyyy`, напр. `27.04.2026`.", parse_mode="Markdown")
        return START_DATE
    return await _save_start_date(q.message, context, "сьогодні")


async def onb_start_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return START_DATE
    return await _save_start_date(update.message, context, update.message.text)


async def onb_account_bank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_BANK
    await q.answer()

    bank_key = q.data.rsplit(":", 1)[-1]
    label_map = {
        "mono": "Monobank",
        "privat": "ПриватБанк",
        "cash": "Готівка",
    }

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["bank_key"] = bank_key

    if bank_key == "other":
        await q.message.reply_text("Введи назву банку або рахунку текстом, напр. `Райф`.", parse_mode="Markdown")
        return ACC_BANK_TEXT

    current["label_base"] = label_map[bank_key]
    if bank_key == "cash":
        current["last4"] = None
        await q.message.reply_text(
            "Це готівка, тому номер картки не потрібен.\n\nТепер обери валюту цього рахунку.",
            reply_markup=kb_currency("onb:acct:cur"),
        )
        return ACC_CURRENCY

    await q.message.reply_text(
        "Останні 4 цифри картки — опційно.\n\n"
        "Якщо хочеш відрізняти картки між собою, введи 4 цифри. Якщо ні — пропусти цей крок.",
        reply_markup=kb_onb_account_last4(),
    )
    return ACC_LAST4_CHOICE


async def onb_account_bank_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BANK_TEXT

    label = update.message.text.strip()[:40]
    if len(label) < 2:
        await update.message.reply_text("Назва занадто коротка. Напиши щось на кшталт `Райф` або `Wise`.", parse_mode="Markdown")
        return ACC_BANK_TEXT

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["label_base"] = label
    await update.message.reply_text(
        "Останні 4 цифри картки — опційно.\n\n"
        "Можеш ввести 4 цифри для зручності або пропустити крок.",
        reply_markup=kb_onb_account_last4(),
    )
    return ACC_LAST4_CHOICE


async def _go_to_account_currency(message, context: ContextTypes.DEFAULT_TYPE, last4: str | None) -> int:
    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["last4"] = last4
    await message.reply_text(
        "Добре. Тепер обери валюту цього рахунку.\n\n"
        "Якщо валюти немає в кнопках, введи її 3 літерами текстом.",
        reply_markup=kb_currency("onb:acct:cur"),
    )
    return ACC_CURRENCY


async def onb_account_last4_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_LAST4_CHOICE
    await q.answer()

    choice = q.data.rsplit(":", 1)[-1]
    if choice == "skip":
        return await _go_to_account_currency(q.message, context, None)

    await q.message.reply_text(
        "Введи рівно 4 цифри, напр. `1234`.\n\n"
        "Якщо передумав — натисни «Пропустити».",
        parse_mode="Markdown",
    )
    return ACC_LAST4_TEXT


async def _save_last4(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    last4 = (value or "").strip()
    if len(last4) != 4 or not last4.isdigit():
        await message.reply_text(
            "⚠️ Тут потрібні рівно 4 цифри, напр. `1234`.\n"
            "Якщо картка без цифр або вони не потрібні — натисни «Пропустити».",
            parse_mode="Markdown",
        )
        return ACC_LAST4_TEXT
    return await _go_to_account_currency(message, context, last4)


async def onb_account_last4_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_LAST4_TEXT
    return await _save_last4(update.message, context, update.message.text)


async def onb_account_last4_text_direct(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_LAST4_CHOICE
    return await _save_last4(update.message, context, update.message.text)


async def _save_account_currency(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    currency = value.strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        await message.reply_text("Потрібно 3 літери, напр. UAH / USD / EUR. Спробуй ще раз:")
        return ACC_CURRENCY

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["currency"] = currency
    await message.reply_text(
        "Тепер введи стартовий баланс цього рахунку.\n\n"
        "Приклади: `0`, `5000`, `-100`.",
        parse_mode="Markdown",
    )
    return ACC_BALANCE


async def onb_account_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_CURRENCY
    await q.answer()
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text("Введи валюту 3 літерами, напр. `PLN` або `GBP`.", parse_mode="Markdown")
        return ACC_CURRENCY
    return await _save_account_currency(q.message, context, value)


async def onb_account_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_CURRENCY
    return await _save_account_currency(update.message, context, update.message.text)


async def onb_account_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BALANCE

    balance = _parse_decimal(update.message.text)
    if balance is None:
        await update.message.reply_text("Не бачу коректну суму. Приклад: `5000` або `-100`.", parse_mode="Markdown")
        return ACC_BALANCE

    tg_user_id = context.user_data.get("tg_user_id")
    current = context.user_data.setdefault("onb", {}).get("current_account") or {}
    if not tg_user_id or not current.get("label_base") or not current.get("currency"):
        await update.message.reply_text("Щось збилось у кроці рахунку. Натисни /start і повтори onboarding.")
        return ConversationHandler.END

    label = current["label_base"]
    if current.get("last4"):
        label = f"{label} •{current['last4']}"

    async with _pool(context).acquire() as conn:
        await conn.execute(
            "INSERT INTO accounts (tg_user_id, label, currency, starting_balance) VALUES ($1, $2, $3, $4)",
            tg_user_id,
            label,
            current["currency"],
            balance,
        )
        accounts = await _get_accounts(conn, tg_user_id)

    context.user_data["onb"]["current_account"] = {}
    await update.message.reply_text(
        f"✅ Додано рахунок ({len(accounts)}/5): {label} ({current['currency']}), баланс {_format_decimal(balance)}",
        reply_markup=kb_onb_accounts_more_done(),
    )
    return ACC_MORE_DONE


async def onb_accounts_more_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_MORE_DONE
    await q.answer()

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        accounts = await _get_accounts(conn, tg_user_id)

    action = q.data.rsplit(":", 1)[-1]
    if action == "more":
        if len(accounts) >= 5:
            await q.message.reply_text("У MVP можна додати до 5 рахунків. Якщо все готово — натисни «Готово».")
            return ACC_MORE_DONE
        return await _start_accounts_step(q.message, context, force_add=True)

    if not accounts:
        await q.message.reply_text("Потрібен хоча б один рахунок, щоб продовжити.")
        return await _start_accounts_step(q.message, context, force_add=True)

    await q.message.reply_text(
        "Крок 5/6: Категорії.\n\n"
        "Обери стартовий пакет категорій. Його можна буде змінити пізніше.",
        reply_markup=kb_onb_categories(),
    )
    return ONB_CATS


async def onb_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CATS
    await q.answer()

    choice = q.data.rsplit(":", 1)[-1]
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    context.user_data.setdefault("onb", {})["cats_choice"] = choice
    async with _pool(context).acquire() as conn:
        await _seed_categories(conn, tg_user_id, choice)

    return await _show_onboarding_summary(q.message, context)


async def onb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CONFIRM
    await q.answer()

    action = q.data.rsplit(":", 1)[-1]
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    if action == "ok":
        async with _pool(context).acquire() as conn:
            await conn.execute(
                "UPDATE users SET onboarding_completed=true, onboarding_version=$2 WHERE tg_user_id=$1",
                tg_user_id,
                CURRENT_ONBOARDING_VERSION,
            )
            await _ensure_default_categories(conn, tg_user_id)
        _reset_onboarding(context)
        await q.message.reply_text("✅ Готово. Онбординг завершено.")
        await _reply_home(q.message, "🏠 Тепер можна додавати витрати/доходи текстом або голосом.")
        return ConversationHandler.END

    if action == "edit":
        async with _pool(context).acquire() as conn:
            accounts = await _get_accounts(conn, tg_user_id)
        await q.message.reply_text(
            "Редагування рахунків.\n\nМожеш видалити зайве, додати новий рахунок або повернутись до підтвердження.",
            reply_markup=kb_onb_edit_accounts(accounts),
        )
        return ONB_EDIT_ACCOUNTS

    async with _pool(context).acquire() as conn:
        await conn.execute("DELETE FROM accounts WHERE tg_user_id=$1", tg_user_id)
        await conn.execute("DELETE FROM categories WHERE tg_user_id=$1", tg_user_id)
        await conn.execute(
            "UPDATE users SET onboarding_completed=false, onboarding_version=0, start_date=NULL WHERE tg_user_id=$1",
            tg_user_id,
        )

    _reset_onboarding(context)
    context.user_data["onb"] = {}
    await q.message.reply_text("🔄 Починаємо onboarding спочатку.\n\nКрок 1/6: Обери мову.", reply_markup=kb_language())
    return LANG


async def onb_edit_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_EDIT_ACCOUNTS
    await q.answer()

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text("Не зміг визначити користувача. Запусти /start ще раз.")
        return ConversationHandler.END

    action = q.data.split(":")
    async with _pool(context).acquire() as conn:
        if action[2] == "back":
            return await _show_onboarding_summary(q.message, context)

        if action[2] == "add":
            accounts = await _get_accounts(conn, tg_user_id)
            if len(accounts) >= 5:
                await q.message.reply_text("У MVP можна додати до 5 рахунків.")
                return ONB_EDIT_ACCOUNTS
            return await _start_accounts_step(q.message, context, force_add=True)

        if action[2] == "del" and len(action) == 4:
            await conn.execute("DELETE FROM accounts WHERE tg_user_id=$1 AND id=$2", tg_user_id, int(action[3]))
            accounts = await _get_accounts(conn, tg_user_id)
            if not accounts:
                await q.message.reply_text("Рахунків не залишилось. Додай хоча б один новий.")
                return await _start_accounts_step(q.message, context, force_add=True)
            await q.message.reply_text("Оновив список рахунків.", reply_markup=kb_onb_edit_accounts(accounts))
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

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await q.message.reply_text("Спочатку пройди /start (онбординг).")
            return

        parts = q.data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        value = parts[2] if len(parts) > 2 else ""

        if action == "add" and value in {"expense", "income"}:
            _reset_debt_flow(context)
            accounts = await _get_accounts(conn, user.id)
            await _ensure_default_categories(conn, user.id)
            if not accounts:
                await _reply_and_return_home(q.message, "⚠️ Спочатку додай хоча б один рахунок у налаштуваннях.")
                return
            context.user_data["tx_flow"] = {"kind": value}
            await q.message.reply_text(
                f"Крок 1/3: Обери рахунок для {'витрати' if value == 'expense' else 'доходу'}.",
                reply_markup=kb_pick_account(accounts, value),
            )
            return

        if action == "add" and value == "transfer":
            _reset_runtime_flows(context)
            await _reply_and_return_home(q.message, "🔁 Перекази ще в роботі.")
            return

        if action == "cmd" and value == "reports":
            _reset_runtime_flows(context)
            await q.message.reply_text("📊 Звіти: обери період.", reply_markup=kb_reports_menu())
            return

        if action == "cmd" and value == "categories":
            _reset_runtime_flows(context)
            expense = await _get_categories(conn, user.id, "expense")
            income = await _get_categories(conn, user.id, "income")
            lines = ["📂 Категорії", "", "Витрати:"]
            lines += [f"- {name}" for _, name in expense] or ["- немає"]
            lines += ["", "Доходи:"]
            lines += [f"- {name}" for _, name in income] or ["- немає"]
            await q.message.reply_text("\n".join(lines))
            await _reply_home(q.message)
            return

        if action == "cmd" and value == "debts":
            _reset_tx_flow(context)
            await q.message.reply_text(await _debts_report_text(conn, user.id), reply_markup=kb_debts_menu())
            return

        if action == "cmd" and value in {"family", "settings", "export"}:
            _reset_runtime_flows(context)
            placeholders = {
                "family": "👨‍👩‍👧 Розділ «Сімʼя» ще в роботі.",
                "settings": "⚙️ Розділ «Налаштування» ще в роботі.",
                "export": "📄 Експорт CSV ще в роботі.",
            }
            await _reply_and_return_home(q.message, placeholders[value])
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

    tx_flow = context.user_data.get("tx_flow")
    if not tx_flow:
        await q.message.reply_text("Почни з кнопки «Витрата» або «Дохід».", reply_markup=kb_home())
        return

    if q.data == "pick:acct:back":
        _reset_tx_flow(context)
        await _reply_home(q.message)
        return

    async with _pool(context).acquire() as conn:
        if q.data == "pick:cat:back":
            accounts = await _get_accounts(conn, user.id)
            await q.message.reply_text("Крок 1/3: Обери рахунок.", reply_markup=kb_pick_account(accounts, tx_flow["kind"]))
            return

        parts = q.data.split(":")
        if parts[:2] == ["pick", "acct"] and len(parts) == 4:
            tx_flow["account_id"] = int(parts[3])
            categories = await _get_categories(conn, user.id, tx_flow["kind"])
            await q.message.reply_text("Крок 2/3: Обери категорію.", reply_markup=kb_pick_category(categories, tx_flow["kind"]))
            return

        if parts[:2] == ["pick", "cat"] and len(parts) == 4:
            tx_flow["category_id"] = int(parts[3])
            tx_flow["await_amount"] = True
            await q.message.reply_text(
                "Крок 3/3: Напиши суму транзакції та, за бажанням, короткий коментар.\n"
                "Приклад: `Продукти 500`\n\n"
                "Або надішли голосове.",
                parse_mode="Markdown",
            )


async def _save_tx_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    amount = parse_amount(raw_text)
    if amount is None or amount <= 0:
        await update.message.reply_text("Не бачу суму. Приклад: `Продукти 500`", parse_mode="Markdown")
        return

    tx_flow = context.user_data.get("tx_flow") or {}
    if not tx_flow.get("account_id") or not tx_flow.get("category_id") or tx_flow.get("kind") not in {"expense", "income"}:
        _reset_tx_flow(context)
        await _reply_and_return_home(update.message, "⚠️ Схоже, flow транзакції збився. Почни ще раз з меню.")
        return

    async with _pool(context).acquire() as conn:
        db_user = await _get_user(conn, user.id)
        currency = (db_user.get("base_currency") if db_user else None) or "UAH"
        await conn.execute(
            """
            INSERT INTO transactions (tg_user_id, date, type, amount, currency, comment, source, account_id, category_id, flow_kind)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'normal')
            """,
            user.id,
            datetime.now().date(),
            tx_flow["kind"],
            float(amount),
            currency,
            raw_text.strip()[:500] or None,
            "text",
            tx_flow["account_id"],
            tx_flow["category_id"],
        )

    _reset_tx_flow(context)
    await update.message.reply_text("✅ Транзакцію збережено.")
    await _reply_home(update.message)


async def _debts_report_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    rows = await conn.fetch(
        """
        SELECT counterparty, currency,
               sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
               sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
        FROM transactions
        WHERE tg_user_id=$1 AND flow_kind='debt'
        GROUP BY counterparty, currency
        ORDER BY counterparty ASC
        """,
        tg_user_id,
    )

    owed_to_me: list[str] = []
    i_owe: list[str] = []
    for row in rows:
        counterparty = (row["counterparty"] or "—").strip() or "—"
        currency = (row["currency"] or "").strip()
        owed = Decimal(str(row["owed_to_me"] or 0))
        owe = Decimal(str(row["i_owe"] or 0))
        if owed > 0:
            owed_to_me.append(f"- {counterparty}: {_format_decimal(owed)} {currency}")
        if owe > 0:
            i_owe.append(f"- {counterparty}: {_format_decimal(owe)} {currency}")

    lines = ["🤝 Борги", "", "Мені винні:"]
    lines += owed_to_me or ["- немає"]
    lines += ["", "Я винен:"]
    lines += i_owe or ["- немає"]
    return "\n".join(lines)


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
        await _reply_home(q.message)
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await q.message.reply_text("Спочатку пройди /start (онбординг).")
            return
        db_user = await _get_user(conn, user.id)
        base_currency = (db_user.get("base_currency") if db_user else None) or "UAH"

    _reset_tx_flow(context)
    if q.data.startswith("debts:add:"):
        direction = q.data.rsplit(":", 1)[-1]
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "repay": False, "currency": base_currency}
        await q.message.reply_text("Введи імʼя людини або назву контрагента:")
        return

    if q.data.startswith("debts:repay:"):
        direction = q.data.rsplit(":", 1)[-1]
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "repay": True, "currency": base_currency}
        await q.message.reply_text("Введи імʼя людини або назву контрагента:")


async def _handle_debt_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    flow = context.user_data.get("debt_flow") or {}
    if flow.get("step") == "name":
        name = update.message.text.strip()[:80]
        if not name:
            await update.message.reply_text("Імʼя не може бути порожнім. Спробуй ще раз:")
            return
        flow["name"] = name
        flow["step"] = "amount"
        context.user_data["debt_flow"] = flow
        await update.message.reply_text("Введи суму боргу або погашення:")
        return

    amount = parse_amount(update.message.text)
    if amount is None or amount <= 0:
        await update.message.reply_text("Не схоже на суму. Приклад: `500` або `1200.50`.", parse_mode="Markdown")
        return

    if flow.get("direction") == "owed_to_me":
        debt_action = "lend_repaid" if flow.get("repay") else "lend"
    else:
        debt_action = "borrow_repaid" if flow.get("repay") else "borrow"

    async with _pool(context).acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transactions (tg_user_id, date, type, amount, currency, comment, source, flow_kind, counterparty, debt_action)
            VALUES ($1, $2, 'transfer', $3, $4, $5, $6, 'debt', $7, $8)
            """,
            user.id,
            datetime.now().date(),
            float(amount),
            flow.get("currency") or "UAH",
            None,
            "text",
            flow.get("name"),
            debt_action,
        )
        report = await _debts_report_text(conn, user.id)

    _reset_debt_flow(context)
    await update.message.reply_text("✅ Борг збережено.")
    await update.message.reply_text(report, reply_markup=kb_debts_menu())


async def reports_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if q.data == "reports:back":
        await _reply_home(q.message)
        return

    key = q.data.rsplit(":", 1)[-1]
    today = datetime.now().date()
    if key == "today":
        start_date, end_date, title = today, today + timedelta(days=1), "Сьогодні"
    elif key == "7d":
        start_date, end_date, title = today - timedelta(days=6), today + timedelta(days=1), "Останні 7 днів"
    elif key == "30d":
        start_date, end_date, title = today - timedelta(days=29), today + timedelta(days=1), "Останні 30 днів"
    elif key == "3m":
        start_date, end_date, title = today - timedelta(days=90), today + timedelta(days=1), "Останні 3 місяці"
    elif key == "6m":
        start_date, end_date, title = today - timedelta(days=180), today + timedelta(days=1), "Останні 6 місяців"
    else:
        start_date = date(today.year, today.month, 1)
        end_date = date(today.year + (today.month // 12), (today.month % 12) + 1, 1)
        title = "Поточний місяць"

    async with _pool(context).acquire() as conn:
        db_user = await _get_user(conn, user.id)
        user_start = db_user.get("start_date") if db_user else None
        effective_start = max(start_date, user_start) if user_start else start_date
        rows = await conn.fetch(
            """
            SELECT type, sum(amount) AS total
            FROM transactions
            WHERE tg_user_id=$1
              AND date >= $2
              AND date < $3
              AND flow_kind='normal'
              AND account_id IS NOT NULL
              AND category_id IS NOT NULL
            GROUP BY type
            """,
            user.id,
            effective_start,
            end_date,
        )

    totals = {row["type"]: Decimal(str(row["total"] or 0)) for row in rows}
    await q.message.reply_text(
        f"📊 Звіт: {title}\n\n"
        f"Витрати: {_format_decimal(totals.get('expense'))}\n"
        f"Доходи: {_format_decimal(totals.get('income'))}",
        reply_markup=kb_reports_menu(),
    )


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    if context.user_data.get("tx_flow", {}).get("await_amount"):
        await _save_tx_amount(update, context, update.message.text)
        return

    if context.user_data.get("debt_flow"):
        await _handle_debt_text(update, context)
        return

    if _onboarding_active(context):
        await update.message.reply_text("Зараз активний онбординг. Заверши крок вище або запусти /start заново.")
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await update.message.reply_text("Спочатку пройди /start (онбординг).")
            return

    await _reply_home(update.message, "Щоб додати нову операцію, натисни «Витрата» або «Дохід».")


async def voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.voice:
        return

    if _onboarding_active(context):
        await update.message.reply_text("Зараз активний онбординг. Спочатку заверши його.")
        return

    if update.message.voice.duration and update.message.voice.duration > 20:
        await update.message.reply_text("Голосове має бути коротше 20 секунд.")
        return

    if not context.user_data.get("tx_flow", {}).get("await_amount"):
        await update.message.reply_text(
            "Спочатку натисни «Витрата» або «Дохід», обери рахунок і категорію, а потім можеш надиктувати суму голосом."
        )
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await update.message.reply_text("Спочатку пройди /start (онбординг).")
            return

    if not config.OPENAI_API_KEY:
        await update.message.reply_text("Голосовий ввід ще не налаштовано: відсутній OPENAI_API_KEY.")
        return

    await update.message.chat.send_action(ChatAction.TYPING)
    telegram_file = await update.message.voice.get_file()
    ogg_bytes = await telegram_file.download_as_bytearray()

    try:
        result = await transcribe_ogg_bytes(bytes(ogg_bytes))
    except Exception as exc:
        await update.message.reply_text(f"STT помилка: {exc}")
        return

    await update.message.reply_text(f"Розпізнав: {result.text}")
    await _save_tx_amount(update, context, result.text)


def build_app() -> Application:
    if not config.BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Put it into /opt/my-cash-flow-bot/.env")

    app = Application.builder().token(config.BOT_TOKEN).post_init(init_db).post_shutdown(shutdown_db).build()

    onboarding = ConversationHandler(
        entry_points=[CommandHandler("start", start_entry)],
        states={
            LANG: [CallbackQueryHandler(onb_lang, pattern=r"^onb:lang:")],
            BASE_CURRENCY: [
                CallbackQueryHandler(onb_currency_callback, pattern=r"^onb:cur:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_currency_text),
            ],
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
            ACC_CURRENCY: [
                CallbackQueryHandler(onb_account_currency_callback, pattern=r"^onb:acct:cur:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_currency_text),
            ],
            ACC_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_balance)],
            ACC_MORE_DONE: [CallbackQueryHandler(onb_accounts_more_done, pattern=r"^onb:acct:(more|done)$")],
            ONB_CATS: [CallbackQueryHandler(onb_categories, pattern=r"^onb:cats:")],
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
