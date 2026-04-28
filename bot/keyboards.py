from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def kb_language() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Українська", callback_data="onb:lang:uk"),
                InlineKeyboardButton("Русский", callback_data="onb:lang:ru"),
            ]
        ]
    )


def kb_currency() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("UAH", callback_data="onb:cur:UAH"),
                InlineKeyboardButton("USD", callback_data="onb:cur:USD"),
                InlineKeyboardButton("EUR", callback_data="onb:cur:EUR"),
            ],
            [InlineKeyboardButton("Інша…", callback_data="onb:cur:OTHER")],
        ]
    )


def kb_onb_start_date() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сьогодні", callback_data="onb:date:today"),
                InlineKeyboardButton("Вибрати дату", callback_data="onb:date:pick"),
            ]
        ]
    )


def kb_onb_account_bank() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Monobank", callback_data="onb:acct:bank:mono"),
                InlineKeyboardButton("ПриватБанк", callback_data="onb:acct:bank:privat"),
            ],
            [
                InlineKeyboardButton("Готівка", callback_data="onb:acct:bank:cash"),
                InlineKeyboardButton("Інший банк…", callback_data="onb:acct:bank:other"),
            ],
        ]
    )


def kb_onb_account_last4() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Ввести 4 цифри", callback_data="onb:acct:last4:enter"),
                InlineKeyboardButton("Пропустити", callback_data="onb:acct:last4:skip"),
            ]
        ]
    )


def kb_onb_account_currency() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("UAH", callback_data="onb:acct:cur:UAH"),
                InlineKeyboardButton("USD", callback_data="onb:acct:cur:USD"),
                InlineKeyboardButton("EUR", callback_data="onb:acct:cur:EUR"),
            ],
            [InlineKeyboardButton("Інша…", callback_data="onb:acct:cur:OTHER")],
        ]
    )


def kb_onb_accounts_more_done() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Додати ще", callback_data="onb:acct:more"),
                InlineKeyboardButton("Готово", callback_data="onb:acct:done"),
            ]
        ]
    )


def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Витрата", callback_data="home:add:expense"),
                InlineKeyboardButton("➕ Дохід", callback_data="home:add:income"),
            ],
            [InlineKeyboardButton("🔁 Переказ", callback_data="home:add:transfer")],
            [
                InlineKeyboardButton("📊 Звіт за місяць", callback_data="home:cmd:month"),
                InlineKeyboardButton("📂 Категорії", callback_data="home:cmd:categories"),
            ],
            [
                InlineKeyboardButton("🤝 Борги", callback_data="home:cmd:debts"),
                InlineKeyboardButton("👪 Сімʼя", callback_data="home:cmd:family"),
            ],
            [InlineKeyboardButton("⚙️ Налаштування", callback_data="home:cmd:settings")],
            [InlineKeyboardButton("📄 Експорт (CSV)", callback_data="home:cmd:export")],
        ]
    )


def kb_confirm(prefix: str = "tx") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Зберегти", callback_data=f"{prefix}:confirm:save"),
                InlineKeyboardButton("✏️ Змінити", callback_data=f"{prefix}:confirm:edit"),
                InlineKeyboardButton("❌ Скасувати", callback_data=f"{prefix}:confirm:cancel"),
            ]
        ]
    )


def kb_debts_start() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Додати «мені винні»", callback_data="onb:debt:owed_to_me"),
                InlineKeyboardButton("Додати «я винен»", callback_data="onb:debt:i_owe"),
            ],
            [InlineKeyboardButton("Готово", callback_data="onb:debt:done")],
        ]
    )
