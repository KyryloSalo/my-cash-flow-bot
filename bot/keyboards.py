from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def _rows2(buttons: list[InlineKeyboardButton]) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for b in buttons:
        row.append(b)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def kb_language() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Українська", callback_data="onb:lang:uk"),
                InlineKeyboardButton("Русский", callback_data="onb:lang:ru"),
            ]
        ]
    )


def kb_currency(prefix: str = "onb:cur") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("UAH", callback_data=f"{prefix}:UAH"),
                InlineKeyboardButton("USD", callback_data=f"{prefix}:USD"),
                InlineKeyboardButton("EUR", callback_data=f"{prefix}:EUR"),
            ],
            [InlineKeyboardButton("Інша…", callback_data=f"{prefix}:OTHER")],
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


def kb_onb_accounts_more_done() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Додати ще", callback_data="onb:acct:more"),
                InlineKeyboardButton("Готово", callback_data="onb:acct:done"),
            ]
        ]
    )


def kb_onb_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Підтвердити", callback_data="onb:confirm:ok"),
                InlineKeyboardButton("✏️ Редагувати", callback_data="onb:confirm:edit"),
            ],
            [InlineKeyboardButton("🔄 Почати спочатку", callback_data="onb:confirm:restart")],
        ]
    )


def kb_onb_edit_accounts(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(f"🗑️ {label}", callback_data=f"onb:edit:del:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append(
        [
            InlineKeyboardButton("➕ Додати рахунок", callback_data="onb:edit:add"),
            InlineKeyboardButton("Назад", callback_data="onb:edit:back"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Витрата", callback_data="home:add:expense"),
                InlineKeyboardButton("➕ Дохід", callback_data="home:add:income"),
            ],
            [InlineKeyboardButton("🔁 Переказ", callback_data="home:add:transfer")],
            [
                InlineKeyboardButton("📊 Звіти", callback_data="home:cmd:reports"),
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


def kb_reports_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сьогодні", callback_data="reports:range:today"),
                InlineKeyboardButton("7 днів", callback_data="reports:range:7d"),
            ],
            [
                InlineKeyboardButton("Місяць", callback_data="reports:range:month"),
                InlineKeyboardButton("Останні 30 днів", callback_data="reports:range:30d"),
            ],
            [
                InlineKeyboardButton("3 місяці", callback_data="reports:range:3m"),
                InlineKeyboardButton("6 місяців", callback_data="reports:range:6m"),
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="reports:back")],
        ]
    )


def kb_pick_account(accounts: list[tuple[int, str]], kind: str) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"pick:acct:{kind}:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="pick:acct:back")])
    return InlineKeyboardMarkup(rows)


def kb_pick_category(categories: list[tuple[int, str]], kind: str) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(name, callback_data=f"pick:cat:{kind}:{category_id}") for category_id, name in categories]
    rows = _rows2(buttons)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="pick:cat:back")])
    return InlineKeyboardMarkup(rows)


def kb_debts_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ Мені винні", callback_data="debts:add:owed_to_me"),
                InlineKeyboardButton("➕ Я винен", callback_data="debts:add:i_owe"),
            ],
            [
                InlineKeyboardButton("✅ Погашення (мені)", callback_data="debts:repay:owed_to_me"),
                InlineKeyboardButton("✅ Погашення (я)", callback_data="debts:repay:i_owe"),
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="debts:back")],
        ]
    )
