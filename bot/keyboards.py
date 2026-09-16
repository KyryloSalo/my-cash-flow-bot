from __future__ import annotations

import unicodedata

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
try:
    from telegram import WebAppInfo
except ImportError:  # pragma: no cover - compatibility for older telegram versions/stubs
    WebAppInfo = None

import config
from bot_i18n import current_locale, t

HOME_MENU_MAIN_CALLBACK = "menu:main"
HOME_MENU_DASHBOARD_CALLBACK = "menu:dashboard"
HOME_MENU_OPERATIONS_CALLBACK = "menu:operations"
HOME_MENU_REPORTS_CALLBACK = "menu:reports"
HOME_MENU_FINANCES_CALLBACK = "menu:finances"
HOME_MENU_MORE_CALLBACK = "menu:more"

_WIDE_FILL = "⠀"
_WIDE_MIN_WIDTH = 0


def _locale(locale: str | None = None) -> str:
    return current_locale(locale)


def _t(uk_text: str, en_text: str, locale: str | None = None) -> str:
    return t(uk_text, en_text, _locale(locale))


def _dashboard_button_text(locale: str | None = None) -> str:
    return _t("📊 Дашборд", "📊 Dashboard", locale)


def _operations_button_text(locale: str | None = None) -> str:
    return _t("🧾 Операції", "🧾 Operations", locale)


def _reports_button_text(locale: str | None = None) -> str:
    return _t("📈 Звіти", "📈 Reports", locale)


def _finances_button_text(locale: str | None = None) -> str:
    return _t("💼 Фінанси", "💼 Finances", locale)


def _more_button_text(locale: str | None = None) -> str:
    return _t("⋯ Ще", "⋯ More", locale)


def _expense_button_text(locale: str | None = None) -> str:
    return _t("➖ Витрата", "➖ Expense", locale)


def _income_button_text(locale: str | None = None) -> str:
    return _t("➕ Дохід", "➕ Income", locale)


def _transfer_button_text(locale: str | None = None) -> str:
    return _t("🔄 Переказ", "🔄 Transfer", locale)


def _accounts_button_text(locale: str | None = None) -> str:
    return _t("💼 Рахунки", "💼 Accounts", locale)


def _savings_button_text(locale: str | None = None) -> str:
    return _t("🐿 Заощадження", "🐿 Savings", locale)


def _debts_button_text(locale: str | None = None) -> str:
    return _t("🤝 Борги", "🤝 Debts", locale)


def _view_reports_button_text(locale: str | None = None) -> str:
    return _t("📊 Звіти", "📊 Reports", locale)


def _export_home_button_text(locale: str | None = None) -> str:
    return _t("📥 Експорт", "📥 Export", locale)


def _categories_button_text(locale: str | None = None) -> str:
    return _t("🗂 Категорії", "🗂 Categories", locale)


def _family_button_text(locale: str | None = None) -> str:
    return _t("👨‍👩‍👧‍👦 Сім'я", "👨‍👩‍👧‍👦 Family", locale)


def _settings_button_text(locale: str | None = None) -> str:
    return _t("⚙️ Налашт.", "⚙️ Settings", locale)


def _help_button_text(locale: str | None = None) -> str:
    return _t("🆘 Допомога", "🆘 Help", locale)


def _is_regional_indicator(char: str) -> bool:
    codepoint = ord(char)
    return 0x1F1E6 <= codepoint <= 0x1F1FF


def _cluster_width(cluster: str) -> int:
    visible = [
        char
        for char in cluster
        if char != "\u200d" and char != "\ufe0f" and not unicodedata.combining(char)
    ]
    if not visible:
        return 0
    if len(visible) == 2 and all(_is_regional_indicator(char) for char in visible):
        return 2
    if "\u200d" in cluster or any(unicodedata.category(char) == "So" for char in visible):
        return 2
    return sum(2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1 for char in visible)


def _display_width(text: str) -> int:
    width = 0
    cluster = ""
    regional_count = 0
    prev_joiner = False

    for char in str(text or ""):
        if not cluster:
            cluster = char
            regional_count = 1 if _is_regional_indicator(char) else 0
            prev_joiner = char == "\u200d"
            continue

        if prev_joiner or char == "\u200d" or char == "\ufe0f" or unicodedata.combining(char):
            cluster += char
            prev_joiner = char == "\u200d"
            regional_count = 0
            continue

        if regional_count == 1 and _is_regional_indicator(char):
            cluster += char
            regional_count = 0
            prev_joiner = False
            continue

        width += _cluster_width(cluster)
        cluster = char
        regional_count = 1 if _is_regional_indicator(char) else 0
        prev_joiner = char == "\u200d"

    if cluster:
        width += _cluster_width(cluster)
    return width


_WIDE_MIN_WIDTH = max(
    _display_width(label)
    for label in (
        _accounts_button_text("uk"),
        _accounts_button_text("en"),
        _savings_button_text("uk"),
        _savings_button_text("en"),
        _debts_button_text("uk"),
        _debts_button_text("en"),
        "⬅️ Назад",
        "⬅️ Back",
    )
)


def _wide_text(text: str, *, min_width: int | None = None) -> str:
    label = str(text or "").strip()
    target_width = _WIDE_MIN_WIDTH if min_width is None else min_width
    missing = max(0, target_width - _display_width(label))
    left = missing // 2
    right = missing - left
    return f"{_WIDE_FILL * left}{label}{_WIDE_FILL * right}"


def _home_btn() -> InlineKeyboardButton:
    return InlineKeyboardButton(_wide_text(_t("🏠 Головне меню", "🏠 Home")), callback_data=HOME_MENU_MAIN_CALLBACK)


def _back_btn(callback_data: str = HOME_MENU_MAIN_CALLBACK) -> InlineKeyboardButton:
    return InlineKeyboardButton(_wide_text(_t("⬅️ Назад", "⬅️ Back")), callback_data=callback_data)


def _cancel_btn(callback_data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(_wide_text(_t("❌ Скасувати", "❌ Cancel")), callback_data=callback_data)


def _wide_callback_btn(text: str, callback_data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(_wide_text(text), callback_data=callback_data)


def _wide_url_btn(text: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(_wide_text(text), url=url)


def _miniapp_url(value: str | None = None) -> str:
    return str(value or config.MINIAPP_URL or "").strip()


def _miniapp_btn(text: str | None = None, *, url: str | None = None) -> InlineKeyboardButton | None:
    safe_url = _miniapp_url(url)
    if not safe_url:
        return None
    button_text = text or _dashboard_button_text()
    if WebAppInfo is not None:
        try:
            return InlineKeyboardButton(button_text, web_app=WebAppInfo(url=safe_url))
        except TypeError:
            pass
    return InlineKeyboardButton(button_text, url=safe_url)


def _dashboard_btn() -> InlineKeyboardButton:
    return InlineKeyboardButton(
        _wide_text(_dashboard_button_text()),
        callback_data=HOME_MENU_DASHBOARD_CALLBACK,
    )


def kb_dashboard_menu_entry() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[_dashboard_btn()]])


def kb_dashboard_launch_menu(*, url: str | None = None, include_home_button: bool = False) -> InlineKeyboardMarkup:
    rows = [[_miniapp_btn(_wide_text(_dashboard_button_text()), url=url) or _dashboard_btn()]]
    if include_home_button:
        rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def _rows2(buttons: list[InlineKeyboardButton]) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for button in buttons:
        row.append(button)
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def _rows1(buttons: list[InlineKeyboardButton]) -> list[list[InlineKeyboardButton]]:
    return [[button] for button in buttons]


def _nav_rows(
    back_callback: str | None = None,
    *,
    include_home: bool = False,
) -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    if back_callback:
        rows.append([_back_btn(back_callback)])
    if include_home and back_callback != HOME_MENU_MAIN_CALLBACK:
        rows.append([_home_btn()])
    return rows


def _home_menu_footer() -> list[list[InlineKeyboardButton]]:
    return _nav_rows(HOME_MENU_MAIN_CALLBACK)


def kb_language() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🇺🇦 Українська", callback_data="onb:lang:uk"),
                InlineKeyboardButton("🇬🇧 English", callback_data="onb:lang:en"),
            ],
            [InlineKeyboardButton(_t("Як ми працюємо з даними", "How we use your data"), callback_data="onb:privacy:open")],
        ]
    )


def kb_onb_privacy(policy_url: str | None = None) -> InlineKeyboardMarkup:
    policy_button = (
        InlineKeyboardButton(_t("Відкрити політику конфіденційності", "Open privacy policy"), url=policy_url)
        if policy_url
        else InlineKeyboardButton(_t("Відкрити політику конфіденційності", "Open privacy policy"), callback_data="onb:privacy:policy")
    )
    return InlineKeyboardMarkup(
        [
            [policy_button],
            [InlineKeyboardButton(_t("Назад до налаштування", "Back to setup"), callback_data="onb:privacy:back")],
        ]
    )


def kb_currency(
    prefix: str = "onb:cur",
    *,
    include_other: bool = True,
    currencies: tuple[str, ...] = ("UAH", "USD", "EUR", "TRY", "USDT"),
) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(currency, callback_data=f"{prefix}:{currency}") for currency in currencies]
    rows = _rows2(buttons)
    if include_other:
        rows.append([InlineKeyboardButton(_t("🌍 Інша…", "🌍 Other…"), callback_data=f"{prefix}:OTHER")])
    return InlineKeyboardMarkup(rows)


def kb_onb_start_date() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(_t("📅 Сьогодні", "📅 Today"), callback_data="onb:date:today"),
            InlineKeyboardButton(_t("✍️ Ввести дату", "✍️ Enter date"), callback_data="onb:date:pick"),
        ]]
    )


def kb_onb_account_bank() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🏦 Monobank", callback_data="onb:acct:bank:mono"),
                InlineKeyboardButton(_t("🏦 ПриватБанк", "🏦 PrivatBank"), callback_data="onb:acct:bank:privat"),
            ],
            [
                InlineKeyboardButton(_t("💵 Готівка", "💵 Cash"), callback_data="onb:acct:bank:cash"),
                InlineKeyboardButton(_t("🏛️ Інший банк…", "🏛️ Other bank…"), callback_data="onb:acct:bank:other"),
            ],
        ]
    )


def kb_onb_account_last4() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(_t("🔢 Ввести 4 цифри", "🔢 Enter last 4 digits"), callback_data="onb:acct:last4:enter"),
            InlineKeyboardButton(_t("⏭️ Пропустити", "⏭️ Skip"), callback_data="onb:acct:last4:skip"),
        ]]
    )


def kb_onb_accounts_more_done() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton(_t("➕ Додати ще рахунок", "➕ Add another account"), callback_data="onb:acct:more"),
            InlineKeyboardButton(_t("✅ Готово", "✅ Done"), callback_data="onb:acct:done"),
        ]]
    )


def kb_onb_categories() -> InlineKeyboardMarkup:
    return onboarding_categories_mode_keyboard()


def onboarding_categories_mode_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Стандартні категорії", callback_data="ob_cat_mode:standard")],
            [InlineKeyboardButton("✏️ Налаштувати витрати вручну", callback_data="ob_cat_mode:custom")],
            [_back_btn("ob_cat:back")],
        ]
    )


def onboarding_custom_categories_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➖ Категорії витрат", callback_data="ob_cat:expense"),
            ],
            [InlineKeyboardButton("➕ Додати категорію витрат", callback_data="ob_cat:add")],
            [InlineKeyboardButton("✅ Готово", callback_data="ob_cat:done")],
            [_back_btn("ob_cat:back")],
        ]
    )


def onboarding_add_category_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➖ Витрата", callback_data="ob_cat_add:expense"),
            ],
            [_back_btn("ob_cat:back")],
            [_cancel_btn("ob_cat:back")],
        ]
    )


def _checkbox_prefix(selected: bool) -> str:
    return "✅" if selected else "⬜"


def onboarding_expense_categories_checkbox_keyboard(
    selected_ids: list[dict],
    custom_categories: list[str],
) -> InlineKeyboardMarkup:
    buttons: list[InlineKeyboardButton] = []
    for t in selected_ids:
        template_id = int(t.get("id"))
        name = str(t.get("name") or "")
        is_system = bool(t.get("is_system"))
        selected = bool(t.get("selected"))
        label = f"{_checkbox_prefix(selected)} {name}"
        if is_system:
            label = f"✅ {name}"
            buttons.append(InlineKeyboardButton(label, callback_data="ob_cat:noop"))
        else:
            buttons.append(InlineKeyboardButton(label, callback_data=f"ob_cat_toggle:expense:{template_id}"))

    for name in custom_categories or []:
        n = (name or "").strip()
        if n:
            buttons.append(InlineKeyboardButton(f"✅ {n}", callback_data="ob_cat:noop"))

    rows = _rows2(buttons)
    rows.append([InlineKeyboardButton("➕ Додати свою категорію витрат", callback_data="ob_cat_add:expense")])
    rows.append([_back_btn("ob_cat:back")])
    return InlineKeyboardMarkup(rows)


def onboarding_income_categories_checkbox_keyboard(
    selected_ids: list[dict],
    custom_categories: list[str],
) -> InlineKeyboardMarkup:
    buttons: list[InlineKeyboardButton] = []
    for t in selected_ids:
        template_id = int(t.get("id"))
        name = str(t.get("name") or "")
        is_system = bool(t.get("is_system"))
        selected = bool(t.get("selected"))
        label = f"{_checkbox_prefix(selected)} {name}"
        if is_system:
            label = f"✅ {name}"
            buttons.append(InlineKeyboardButton(label, callback_data="ob_cat:noop"))
        else:
            buttons.append(InlineKeyboardButton(label, callback_data=f"ob_cat_toggle:income:{template_id}"))

    for name in custom_categories or []:
        n = (name or "").strip()
        if n:
            buttons.append(InlineKeyboardButton(f"✅ {n}", callback_data="ob_cat:noop"))

    rows = _rows2(buttons)
    rows.append([_back_btn("ob_cat:back")])
    return InlineKeyboardMarkup(rows)


def onboarding_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(_t("✅ Підтвердити", "✅ Confirm"), callback_data="onb:confirm:ok"),
                InlineKeyboardButton(_t("✏️ Змінити категорії", "✏️ Edit categories"), callback_data="onb:confirm:edit_categories"),
            ],
            [InlineKeyboardButton(_t("✏️ Змінити рахунки", "✏️ Edit accounts"), callback_data="onb:confirm:edit")],
            [InlineKeyboardButton(_t("🔄 Почати спочатку", "🔄 Start over"), callback_data="onb:confirm:restart")],
        ]
    )


def kb_onb_confirm() -> InlineKeyboardMarkup:
    return onboarding_confirm_keyboard()


def categories_home_keyboard() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("➖ Витрати", "categories:list:expense"),
            _wide_callback_btn("➕ Доходи", "categories:list:income"),
            _wide_callback_btn("➕ Додати", "categories:add"),
            _wide_callback_btn("✏️ Редагувати", "categories:edit"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def categories_list_keyboard(type: str, categories: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(name, callback_data=f"categories:open:{category_id}") for category_id, name in categories]
    rows = _rows2(buttons)
    rows.append([InlineKeyboardButton("➕ Додати категорію", callback_data=f"categories:add:{type}")])
    rows.append([_back_btn("categories:start")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def category_edit_keyboard(category: dict) -> InlineKeyboardMarkup:
    category_id = int(category.get("id"))
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("✏️ Перейменувати", callback_data=f"categories:rename:{category_id}"),
            InlineKeyboardButton("Ключові слова", callback_data=f"categories:aliases:{category_id}"),
        ]
    ]
    if bool(category.get("is_active", True)) and not bool(category.get("is_system", False)):
        rows.append([InlineKeyboardButton("👁 Приховати", callback_data=f"categories:archive:{category_id}")])
    if not bool(category.get("is_active", True)) and not bool(category.get("is_system", False)):
        rows.append([InlineKeyboardButton("♻️ Відновити", callback_data=f"categories:restore:{category_id}")])
    rows.append([_back_btn(f"categories:list:{category.get('type','expense')}")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def hidden_categories_keyboard(categories: list[tuple[int, str, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(f"♻️ {name}", callback_data=f"categories:restore:{category_id}") for category_id, name, _type in categories]
    rows = _rows2(buttons)
    rows.append([_back_btn("categories:start")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_onb_edit_accounts(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(f"🗃 {label}", callback_data=f"onb:edit:archive:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append(
        [
            InlineKeyboardButton(_t("➕ Додати рахунок", "➕ Add account"), callback_data="onb:edit:add"),
            _back_btn("onb:edit:back"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def kb_home(*, url: str | None = None) -> InlineKeyboardMarkup:
    dashboard_button = _miniapp_btn(_wide_text(_dashboard_button_text()), url=url) or _dashboard_btn()
    return InlineKeyboardMarkup(
        _rows1(
            [
                dashboard_button,
                _wide_callback_btn(_operations_button_text(), HOME_MENU_OPERATIONS_CALLBACK),
                _wide_callback_btn(_reports_button_text(), HOME_MENU_REPORTS_CALLBACK),
                _wide_callback_btn(_finances_button_text(), HOME_MENU_FINANCES_CALLBACK),
                _wide_callback_btn(_more_button_text(), HOME_MENU_MORE_CALLBACK),
            ]
        )
    )


def kb_home_operations_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_expense_button_text(), "expense:start"),
            _wide_callback_btn(_income_button_text(), "income:start"),
            _wide_callback_btn(_transfer_button_text(), "transfer:start"),
            _wide_callback_btn(_t("🕘 Останні операції", "🕘 Recent transactions"), "txvoid:recent"),
        ]
    )
    rows.extend(_home_menu_footer())
    return InlineKeyboardMarkup(rows)


def kb_home_finances_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_accounts_button_text(), "accounts:list"),
            _wide_callback_btn(_savings_button_text(), "saving:overview"),
            _wide_callback_btn(_debts_button_text(), "debts:start"),
        ]
    )
    rows.extend(_home_menu_footer())
    return InlineKeyboardMarkup(rows)


def kb_home_reports_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_view_reports_button_text(), "reports:start"),
            _wide_callback_btn(_export_home_button_text(), "export:start"),
        ]
    )
    rows.extend(_home_menu_footer())
    return InlineKeyboardMarkup(rows)


def kb_home_more_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_categories_button_text(), "categories:start"),
            _wide_callback_btn(_family_button_text(), "family:start"),
            _wide_callback_btn(_settings_button_text(), "settings:start"),
            _wide_callback_btn(_help_button_text(), "help:home"),
        ]
    )
    rows.extend(_home_menu_footer())
    return InlineKeyboardMarkup(rows)


def kb_paywall_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [_wide_callback_btn(_t("Доступ і оплата", "Access and billing"), "settings:billing")],
            [_wide_callback_btn(_help_button_text(), "help:home")],
        ]
    )


def kb_debt_only_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [_wide_callback_btn(_debts_button_text(), "debts:start")],
            [_wide_callback_btn(_t("Доступ і оплата", "Access and billing"), "settings:billing")],
            [_wide_callback_btn(_help_button_text(), "help:home")],
        ]
    )


def kb_help_home(topics: list[tuple[str, str]], *, back_callback: str = "help:back") -> InlineKeyboardMarkup:
    rows = [[_wide_callback_btn(title, f"help:topic:{topic_id}")] for topic_id, title in topics]
    rows.append([_wide_callback_btn(_t("Написати в підтримку", "Contact support"), "help:support")])
    rows.append([_back_btn(back_callback)])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_help_topic(topic_id: str, questions: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    rows = [[_wide_callback_btn(title, f"help:q:{topic_id}:{question_id}")] for question_id, title in questions]
    rows.append([_wide_callback_btn(_t("Всі теми допомоги", "All help topics"), "help:home")])
    rows.append([_wide_callback_btn(_t("Написати в підтримку", "Contact support"), "help:support")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_help_answer(topic_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [_wide_callback_btn(_t("Назад до теми", "Back to topic"), f"help:topic:{topic_id}")],
            [_wide_callback_btn(_t("Всі теми допомоги", "All help topics"), "help:home")],
            [_wide_callback_btn(_t("Написати в підтримку", "Contact support"), "help:support")],
            [_home_btn()],
        ]
    )


def kb_help_support(support_url: str | None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if support_url:
        rows.append([_wide_url_btn(_t("Написати в підтримку", "Contact support"), support_url)])
    rows.append([_wide_callback_btn(_t("Всі теми допомоги", "All help topics"), "help:home")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_accounts_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("➕ Додати", "➕ Add"), "accounts:add"),
            _wide_callback_btn(_t("✏️ Керувати", "✏️ Manage"), "accounts:manage"),
            _wide_callback_btn(_t("🔄 Оновити", "🔄 Refresh"), "accounts:list"),
        ]
    )
    rows.extend(_nav_rows(include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_accounts_manage_keyboard(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"accounts:open:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.extend(_nav_rows("accounts:list", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_account_detail_keyboard(account_id: int) -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("✏️ Змінити назву", "✏️ Rename"), f"accounts:rename:{account_id}"),
            _wide_callback_btn(_t("💰 Змінити баланс", "💰 Update balance"), f"accounts:balance:{account_id}"),
            _wide_callback_btn(_t("🔁 Змінити тип рахунку", "🔁 Change account type"), f"accounts:type:{account_id}"),
            _wide_callback_btn(_t("🗃 Архівувати", "🗃 Archive"), f"accounts:archive:{account_id}"),
        ]
    )
    rows.extend(_nav_rows("accounts:manage", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_account_archive_confirm_keyboard(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Архівувати", "✅ Archive"), callback_data=f"accounts:archive:confirm:{account_id}")],
            [_cancel_btn(f"accounts:open:{account_id}")],
        ]
    )


def kb_reports_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("📅 Сьогодні", "📅 Today"), "reports:today"),
            _wide_callback_btn(_t("🗓 7 днів", "🗓 7 days"), "reports:7d"),
            _wide_callback_btn(_t("📆 Місяць", "📆 Month"), "reports:month"),
            _wide_callback_btn(_t("🗓 30 днів", "🗓 30 days"), "reports:30d"),
            _wide_callback_btn(_t("📈 3 місяці", "📈 3 months"), "reports:3m"),
            _wide_callback_btn(_t("📉 6 місяців", "📉 6 months"), "reports:6m"),
            _wide_callback_btn(_t("🗂 Рік", "🗂 Year"), "reports:year"),
            _wide_callback_btn(_t("🗓 12 місяців", "🗓 12 months"), "reports:12m"),
            _wide_callback_btn(_t("🧾 Весь час", "🧾 All time"), "reports:all"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_export_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("📅 Сьогодні", "📅 Today"), "export:today"),
            _wide_callback_btn(_t("🗓 7 днів", "🗓 7 days"), "export:7d"),
            _wide_callback_btn(_t("📆 Місяць", "📆 Month"), "export:month"),
            _wide_callback_btn(_t("🗓 30 днів", "🗓 30 days"), "export:30d"),
            _wide_callback_btn(_t("📈 3 місяці", "📈 3 months"), "export:3m"),
            _wide_callback_btn(_t("📉 6 місяців", "📉 6 months"), "export:6m"),
            _wide_callback_btn(_t("🗂 Рік", "🗂 Year"), "export:year"),
            _wide_callback_btn(_t("🗓 12 місяців", "🗓 12 months"), "export:12m"),
            _wide_callback_btn(_t("🧾 Весь час", "🧾 All time"), "export:all"),
        ]
    )
    rows.extend(_nav_rows(include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_family_empty() -> InlineKeyboardMarkup:
    rows = _rows1([_wide_callback_btn(_t("✅ Створити родину", "✅ Create family"), "family:create")])
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_family_owner_menu(*, has_active_invite: bool, active_members_count: int) -> InlineKeyboardMarkup:
    rows = [
        [_wide_callback_btn(_t("👥 Учасники", "👥 Members"), "family:members")],
    ]
    if active_members_count < 4:
        rows.append([_wide_callback_btn(_t("🔗 Запросити", "🔗 Invite"), "family:invite:create")])
    if has_active_invite:
        rows.append([_wide_callback_btn(_t("❌ Відкликати", "❌ Revoke invite"), "family:invite:revoke")])
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_family_member_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("👥 Учасники", "👥 Members"), "family:members"),
            _wide_callback_btn(_t("🚪 Покинути родину", "🚪 Leave family"), "family:leave:confirm"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_family_members(owner_mode: bool, members: list[tuple[int, str, str, bool]]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for user_id, label, role, is_active in members:
        if not is_active:
            continue
        if owner_mode and role != "owner":
            rows.append([InlineKeyboardButton(f"❌ {label}", callback_data=f"family:member:remove:{user_id}")])
        else:
            rows.append([InlineKeyboardButton(label, callback_data="family:noop")])
    rows.extend(_nav_rows("family:start", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_family_confirm_leave() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Так, покинути", "✅ Yes, leave"), callback_data="family:leave")],
            [_cancel_btn("family:start")],
        ]
    )


def kb_family_confirm_remove(member_user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Видалити", "✅ Remove"), callback_data=f"family:member:remove:confirm:{member_user_id}")],
            [_cancel_btn("family:members")],
        ]
    )


def kb_family_invite_accept(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Приєднатися", "✅ Join"), callback_data=f"family:invite:accept:{token}")],
            [_cancel_btn("menu:main")],
        ]
    )


def kb_pick_account(accounts: list[tuple[int, str]], kind: str) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"{kind}:account:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_currency_resolution(currency: str, *, callback_prefix: str) -> InlineKeyboardMarkup:
    normalized_currency = str(currency or "UAH").strip().upper() or "UAH"
    rows = _rows1(
        [
            _wide_callback_btn(
                _t(f"Створити рахунок {normalized_currency}", f"Create {normalized_currency} account"),
                f"{callback_prefix}:create",
            ),
            _wide_callback_btn(
                _t("Автоматична конвертація за курсом НБУ", "Auto-convert at NBU rate"),
                f"{callback_prefix}:auto",
            ),
            _wide_callback_btn(
                _t("Конвертувати вручну", "Convert manually"),
                f"{callback_prefix}:manual",
            ),
            _wide_callback_btn(_t("Головне меню", "Home"), HOME_MENU_MAIN_CALLBACK),
        ]
    )
    return InlineKeyboardMarkup(rows)


def kb_pick_category(categories: list[tuple[int, str]], kind: str) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(name, callback_data=f"{kind}:category:{category_id}") for category_id, name in categories]
    rows = _rows2(buttons)
    rows.extend(_nav_rows(f"{kind}:account:back", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_tx_confirm(kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Зберегти", "✅ Save"), callback_data=f"{kind}:confirm")],
            [
                InlineKeyboardButton(_t("✏️ Змінити суму", "✏️ Edit amount"), callback_data=f"{kind}:edit:amount"),
                InlineKeyboardButton(_t("🗂 Змінити категорію", "🗂 Edit category"), callback_data=f"{kind}:edit:category"),
            ],
            [InlineKeyboardButton(_t("💼 Змінити рахунок", "💼 Edit account"), callback_data=f"{kind}:edit:account")],
            [_cancel_btn(f"{kind}:cancel")],
        ]
    )


def kb_debts_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("Мені винні", "They owe me"), "debt:list:receivable"),
            _wide_callback_btn(_t("Я винен", "I owe"), "debt:list:payable"),
            _wide_callback_btn(_t("➕ Додати борг", "➕ Add debt"), "debt:add"),
            _wide_callback_btn(_t("💸 Повернення", "💸 Repayment"), "debt:repay"),
            _wide_callback_btn(_t("Історія", "History"), "debt:history"),
        ]
    )
    rows.extend(_nav_rows(include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_debt_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(_t("✅ Підтвердити", "✅ Confirm"), callback_data="debt:confirm")],
            [InlineKeyboardButton(_t("✏️ Змінити", "✏️ Edit"), callback_data="debt:edit")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_invite_prompt(counterparty_name: str) -> InlineKeyboardMarkup:
    safe_name = (counterparty_name or "").strip() or "боржника"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"Створити посилання для {safe_name}", callback_data="debt:invite:create")],
            [InlineKeyboardButton("Без нагадувань", callback_data="debt:invite:skip")],
        ]
    )


def kb_debt_direction_choice() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Я дав у борг", callback_data="debt:add:receivable")],
            [InlineKeyboardButton("Я взяв у борг", callback_data="debt:add:payable")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_repay_choice() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Мені повернули", callback_data="debt:repay:in")],
            [InlineKeyboardButton("Я повернув", callback_data="debt:repay:out")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_fx_choice() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Вкажу суму", callback_data="debt:fx:amount")],
            [InlineKeyboardButton("Вкажу курс", callback_data="debt:fx:rate")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_close_confirm(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Закрити", callback_data=f"debt:close:{debt_id}")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_delete_confirm(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Видалити", callback_data=f"debt:delete:{debt_id}:confirm")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_overlimit(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Зарахувати тільки залишок і закрити борг", callback_data=f"debt:overlimit:{debt_id}:cap")],
            [InlineKeyboardButton("Змінити суму", callback_data=f"debt:overlimit:{debt_id}:change")],
            [_cancel_btn("debt:cancel")],
        ]
    )


def kb_debt_detail(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Додати повернення", callback_data=f"debt:repay:{debt_id}")],
            [InlineKeyboardButton("Закрити вручну", callback_data=f"debt:close:{debt_id}")],
            [InlineKeyboardButton("Редагувати", callback_data=f"debt:edit:{debt_id}")],
            [InlineKeyboardButton("Видалити", callback_data=f"debt:delete:{debt_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="debt:back")],
        ]
    )


def kb_debt_invite_skip_actions(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("До боргу", callback_data=f"debt:view:{debt_id}")],
            [InlineKeyboardButton("До меню", callback_data="menu:main")],
        ]
    )


def kb_debt_invite_ready_actions(debt_id: int, share_url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📤 Поділитися повідомленням", url=share_url)],
            [InlineKeyboardButton("До боргу", callback_data=f"debt:view:{debt_id}")],
            [InlineKeyboardButton("До меню", callback_data="menu:main")],
        ]
    )


def kb_debt_observer_detail(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⬅️ Назад", callback_data="debt:list:payable")],
        ]
    )


def kb_debt_invite_decision(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Так, підтверджую", callback_data=f"debt:invite:confirm:{token}")],
            [InlineKeyboardButton("Це помилка", callback_data=f"debt:invite:reject:{token}")],
        ]
    )


def kb_debt_edit_fields(debt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Контрагент", callback_data=f"debt:edit:{debt_id}:counterparty")],
            [InlineKeyboardButton("Сума", callback_data=f"debt:edit:{debt_id}:amount")],
            [InlineKeyboardButton("Валюта", callback_data=f"debt:edit:{debt_id}:currency")],
            [InlineKeyboardButton("Строк", callback_data=f"debt:edit:{debt_id}:due_date")],
            [InlineKeyboardButton("Коментар", callback_data=f"debt:edit:{debt_id}:comment")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"debt:view:{debt_id}")],
        ]
    )


def kb_ai_tx_confirm() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Підтвердити", callback_data="ai:tx:ok"),
                InlineKeyboardButton("✏️ Змінити суму", callback_data="ai:edit:amount"),
            ],
            [
                InlineKeyboardButton("🔁 Змінити тип", callback_data="ai:edit:type"),
                InlineKeyboardButton("Змінити рахунок", callback_data="ai:edit:account"),
            ],
            [
                InlineKeyboardButton("Змінити категорію", callback_data="ai:edit:category"),
                InlineKeyboardButton("📝 Змінити опис", callback_data="ai:edit:comment"),
            ],
            [
                InlineKeyboardButton("📅 Змінити дату", callback_data="ai:edit:date"),
                InlineKeyboardButton("❌ Скасувати", callback_data="ai:tx:cancel"),
            ],
            [_home_btn()],
        ]
    )


def kb_ai_tx_edit() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✏️ Змінити суму", callback_data="ai:edit:amount"),
                InlineKeyboardButton("🔁 Змінити тип", callback_data="ai:edit:type"),
            ],
            [
                InlineKeyboardButton("Змінити рахунок", callback_data="ai:edit:account"),
                InlineKeyboardButton("Змінити категорію", callback_data="ai:edit:category"),
            ],
            [
                InlineKeyboardButton("📝 Змінити опис", callback_data="ai:edit:comment"),
                InlineKeyboardButton("📅 Змінити дату", callback_data="ai:edit:date"),
            ],
            *_nav_rows("ai:tx:ok", include_home=True),
        ]
    )


def kb_ai_pick_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➖ Витрата", callback_data="ai:pick:type:expense"),
                InlineKeyboardButton("➕ Дохід", callback_data="ai:pick:type:income"),
            ],
            *_nav_rows("ai:edit:back", include_home=True),
        ]
    )


def kb_ai_pick_account(
    accounts: list[tuple[int, str]],
    *,
    callback_prefix: str = "ai:pick:acct",
    back_callback: str = "ai:edit:back",
) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"{callback_prefix}:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.extend(_nav_rows(back_callback, include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_ai_tx_confirm_prefixed(callback_prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Підтвердити", callback_data=f"{callback_prefix}:tx:ok"),
                InlineKeyboardButton("✏️ Змінити суму", callback_data=f"{callback_prefix}:edit:amount"),
            ],
            [
                InlineKeyboardButton("🔃 Змінити тип", callback_data=f"{callback_prefix}:edit:type"),
                InlineKeyboardButton("Змінити рахунок", callback_data=f"{callback_prefix}:edit:account"),
            ],
            [
                InlineKeyboardButton("Змінити категорію", callback_data=f"{callback_prefix}:edit:category"),
                InlineKeyboardButton("📝 Змінити опис", callback_data=f"{callback_prefix}:edit:comment"),
            ],
            [
                InlineKeyboardButton("📅 Змінити дату", callback_data=f"{callback_prefix}:edit:date"),
                InlineKeyboardButton("❌ Скасувати", callback_data=f"{callback_prefix}:tx:cancel"),
            ],
            [_home_btn()],
        ]
    )


def kb_ai_tx_edit_prefixed(callback_prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✏️ Змінити суму", callback_data=f"{callback_prefix}:edit:amount"),
                InlineKeyboardButton("🔃 Змінити тип", callback_data=f"{callback_prefix}:edit:type"),
            ],
            [
                InlineKeyboardButton("Змінити рахунок", callback_data=f"{callback_prefix}:edit:account"),
                InlineKeyboardButton("Змінити категорію", callback_data=f"{callback_prefix}:edit:category"),
            ],
            [
                InlineKeyboardButton("📝 Змінити опис", callback_data=f"{callback_prefix}:edit:comment"),
                InlineKeyboardButton("📅 Змінити дату", callback_data=f"{callback_prefix}:edit:date"),
            ],
            *_nav_rows(f"{callback_prefix}:tx:ok", include_home=True),
        ]
    )


def kb_ai_pick_type_prefixed(
    callback_prefix: str,
    *,
    back_callback: str,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➖ Витрата", callback_data=f"{callback_prefix}:expense"),
                InlineKeyboardButton("➕ Дохід", callback_data=f"{callback_prefix}:income"),
            ],
            *_nav_rows(back_callback, include_home=True),
        ]
    )


def kb_ai_pick_category(
    categories: list[tuple[int, str]],
    *,
    callback_prefix: str = "ai:pick:cat",
    back_callback: str = "ai:edit:back",
) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(name, callback_data=f"{callback_prefix}:{category_id}") for category_id, name in categories]
    rows = _rows2(buttons)
    rows.extend(_nav_rows(back_callback, include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_ai_batch_confirm(item_count: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("✅ Підтвердити все", callback_data="ai:batch:ok"),
            InlineKeyboardButton("Змінити рахунок", callback_data="ai:batch:account"),
        ],
        [
            InlineKeyboardButton("📅 Змінити дату всім", callback_data="ai:batch:date"),
            InlineKeyboardButton("❌ Скасувати", callback_data="ai:batch:cancel"),
        ],
    ]
    for index in range(max(0, item_count)):
        rows.append([InlineKeyboardButton(f"✏️ Позиція {index + 1}", callback_data=f"ai:batch:item:{index}")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_ai_batch_item_edit(index: int, *, can_remove: bool = True) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("✏️ Змінити суму", callback_data=f"ai:batch:item:{index}:amount"),
            InlineKeyboardButton("Змінити категорію", callback_data=f"ai:batch:item:{index}:category"),
        ],
        [
            InlineKeyboardButton("📝 Змінити опис", callback_data=f"ai:batch:item:{index}:comment"),
            InlineKeyboardButton("📅 Змінити дату", callback_data=f"ai:batch:item:{index}:date"),
        ],
    ]
    if can_remove:
        rows.append([InlineKeyboardButton("🗑 Видалити позицію", callback_data=f"ai:batch:item:{index}:remove")])
    rows.extend(_nav_rows("ai:batch:back", include_home=True))
    return InlineKeyboardMarkup(rows)


def _kb_settings_menu_legacy() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("💳 Рахунки", "accounts:list"),
            _wide_callback_btn("🦔 Заощадження", "saving:overview"),
            _wide_callback_btn("💳 Оплата", "settings:billing"),
            _wide_callback_btn("💱 Валюта", "settings:currency"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)
    rows = _rows1(
        [
            _wide_callback_btn("💳 Рахунки", "accounts:list"),
            _wide_callback_btn("🐿️ Заощадження та інвестиції", "saving:overview"),
            _wide_callback_btn("Доступ і оплата", "settings:billing"),
            _wide_callback_btn("💱 Базова валюта", "settings:currency"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_settings_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn(_t("💼 Рахунки", "💼 Accounts"), "accounts:list"),
            _wide_callback_btn(_t("🦔 Заощадження", "🦔 Savings"), "saving:overview"),
            _wide_callback_btn(_t("🌙 Вечірні нагадування", "🌙 Evening reminders"), "settings:expense_reminders"),
            _wide_callback_btn(_t("💳 Оплата", "💳 Billing"), "settings:billing"),
            _wide_callback_btn(_t("💱 Валюта", "💱 Currency"), "settings:currency"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)
    rows = _rows1(
        [
            _wide_callback_btn("💼 Рахунки", "accounts:list"),
            _wide_callback_btn("🐿️ Заощадження та інвестиції", "saving:overview"),
            _wide_callback_btn("🌙 Вечірні нагадування", "settings:expense_reminders"),
            _wide_callback_btn("Доступ і оплата", "settings:billing"),
            _wide_callback_btn("💱 Базова валюта", "settings:currency"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_expense_reminder_settings(mode: str, reminder_hour: int) -> InlineKeyboardMarkup:
    current_mode = str(mode or "daily")
    current_hour = int(reminder_hour or 21)

    def _mode_label(value: str, label: str) -> str:
        return f"• {label}" if current_mode == value else label

    def _hour_label(hour: int) -> str:
        label = f"{hour:02d}:00"
        return f"• {label}" if current_hour == hour else label

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(_mode_label("daily", "Щодня"), callback_data="settings:expense_reminders:mode:daily"),
                InlineKeyboardButton(_mode_label("weekdays", "Будні"), callback_data="settings:expense_reminders:mode:weekdays"),
            ],
            [InlineKeyboardButton(_mode_label("off", "Вимкнено"), callback_data="settings:expense_reminders:mode:off")],
            [
                InlineKeyboardButton(_hour_label(19), callback_data="settings:expense_reminders:hour:19"),
                InlineKeyboardButton(_hour_label(20), callback_data="settings:expense_reminders:hour:20"),
            ],
            [
                InlineKeyboardButton(_hour_label(21), callback_data="settings:expense_reminders:hour:21"),
                InlineKeyboardButton(_hour_label(22), callback_data="settings:expense_reminders:hour:22"),
            ],
            *_nav_rows("settings:start", include_home=True),
        ]
    )


def kb_expense_reminder_no_expenses(day_iso: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Сьогодні без витрат", callback_data=f"expense:reminder:no_expenses:{day_iso}")]]
    )


def kb_billing_menu(
    *,
    profile_exists: bool,
    has_card: bool,
    auto_renew_enabled: bool,
    trial_eligible: bool = True,
    trial_days: int = 30,
    action_url: str | None = None,
    allow_retry: bool = False,
    bind_callback: str = "settings:billing:bind",
    rebind_callback: str = "settings:billing:rebind",
    bind_button_text: str | None = None,
    rebind_button_text: str | None = None,
    retry_button_text: str = "Оплатити зараз",
    retry_first: bool = False,
    show_action_url_button: bool = True,
    back_callback: str = "settings:start",
    include_home_button: bool = True,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if not has_card:
        if action_url:
            rows.append([_wide_url_btn("Продовжити в Monobank", action_url)])
        else:
            rows.append(
                [
                    _wide_callback_btn(
                        bind_button_text
                        or (f"Почати {int(trial_days or 30)} днів (1 грн)" if trial_eligible else "Прив'язати картку"),
                        bind_callback,
                    )
                ]
            )
    else:
        if allow_retry and retry_first:
            rows.append([_wide_callback_btn(retry_button_text, "settings:billing:retry")])
        rows.append([_wide_callback_btn(rebind_button_text or "Змінити картку", rebind_callback)])
        if auto_renew_enabled:
            rows.append([_wide_callback_btn("Вимкнути автопродовження", "settings:billing:disable")])
    if allow_retry and (not has_card or not retry_first):
        rows.append([_wide_callback_btn(retry_button_text, "settings:billing:retry")])
    if action_url and has_card and show_action_url_button:
        rows.append([_wide_url_btn("Підтвердити в Monobank", action_url)])
    rows.extend(_nav_rows(back_callback, include_home=include_home_button))
    return InlineKeyboardMarkup(rows)


def kb_settings_accounts_menu(*, can_transfer: bool) -> InlineKeyboardMarkup:
    rows = [[_wide_callback_btn("➕ Додати", "accounts:add")]]
    rows.append([_wide_callback_btn("✏️ Керувати", "accounts:manage")])
    if can_transfer:
        rows.append([_wide_callback_btn("🔄 Переказ", "transfer:start")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)
    rows = [[_wide_callback_btn("➕ Додати рахунок", "accounts:add")]]
    rows.append([_wide_callback_btn("✏️ Керувати", "accounts:manage")])
    if can_transfer:
        rows.append([_wide_callback_btn("🔄 Переказ між рахунками", "transfer:start")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_account_add_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Звичайний", "accounts:add:type:main"),
            _wide_callback_btn("Кредитка", "accounts:add:type:credit"),
            _wide_callback_btn("Накопичення", "accounts:add:type:savings"),
            _wide_callback_btn("Готівка", "accounts:add:type:cash"),
            _wide_callback_btn("Депозит", "accounts:add:type:deposit"),
            _wide_callback_btn("Інвестиції", "accounts:add:type:investment"),
        ]
    )
    rows.extend(_nav_rows("accounts:list", include_home=True))
    return InlineKeyboardMarkup(rows)
    rows = _rows1(
        [
            _wide_callback_btn("Звичайний рахунок", "accounts:add:type:main"),
            _wide_callback_btn("Кредитка", "accounts:add:type:credit"),
            _wide_callback_btn("Накопичення", "accounts:add:type:savings"),
            _wide_callback_btn("Готівка", "accounts:add:type:cash"),
            _wide_callback_btn("Депозит", "accounts:add:type:deposit"),
            _wide_callback_btn("Інвестиційний рахунок", "accounts:add:type:investment"),
        ]
    )
    rows.extend(_nav_rows("accounts:list", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_account_type(
    prefix: str = "accounts:create:type",
    *,
    cancel_callback: str = "accounts:create:cancel",
    include_credit: bool = False,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Звичайний рахунок", callback_data=f"{prefix}:main"),
            InlineKeyboardButton("Готівка", callback_data=f"{prefix}:cash"),
        ],
        [
            InlineKeyboardButton("Накопичення", callback_data=f"{prefix}:savings"),
            InlineKeyboardButton("Депозит", callback_data=f"{prefix}:deposit"),
        ],
        [
            InlineKeyboardButton("Інвестиційний рахунок", callback_data=f"{prefix}:investment"),
            InlineKeyboardButton("Інше", callback_data=f"{prefix}:other"),
        ],
    ]
    if include_credit:
        rows.insert(
            1,
            [InlineKeyboardButton("Кредитка", callback_data=f"{prefix}:credit")],
        )
    rows.append([_cancel_btn(cancel_callback)])
    return InlineKeyboardMarkup(rows)


def kb_account_type_change(account_id: int) -> InlineKeyboardMarkup:
    return kb_account_type(prefix=f"accounts:type:set:{account_id}", cancel_callback=f"accounts:open:{account_id}")


def kb_account_create_confirm(
    *,
    create_callback: str = "settings:acct:confirm:create",
    reset_callback: str = "settings:acct:confirm:reset",
    cancel_callback: str = "settings:acct:cancel",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Створити", callback_data=create_callback)],
            [InlineKeyboardButton("✏️ Змінити", callback_data=reset_callback)],
            [_cancel_btn(cancel_callback)],
        ]
    )


def kb_inline_cancel(callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[_cancel_btn(callback_data)]])


def kb_onb_account_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Звичайний рахунок", callback_data="onb:acct:type:main")],
            [InlineKeyboardButton("Кредитка", callback_data="onb:acct:type:credit")],
        ]
    )


def kb_saving_income_prompt(
    *,
    primary_label: str,
    cancel_transaction_id: int | None = None,
) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(primary_label, callback_data="saving:plan:primary")]]
    rows.append([InlineKeyboardButton("Інша сума", callback_data="saving:plan:other")])
    rows.append(
        [
            InlineKeyboardButton("Не зараз", callback_data="saving:not_now"),
            InlineKeyboardButton("Більше не питати", callback_data="saving:disable_prompt"),
        ]
    )
    if cancel_transaction_id:
        rows.append([InlineKeyboardButton("↩️ Скасувати цю транзакцію", callback_data=f"txvoid:pick:{cancel_transaction_id}")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_saving_target_accounts(accounts: list[tuple[int, str]], *, include_create: bool = True, prefix: str = "saving:target") -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"{prefix}:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    if include_create:
        rows.append([InlineKeyboardButton("Створити накопичення", callback_data="saving:create_account")])
    rows.append([_cancel_btn("saving:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_saving_plan_actions(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Я переказав(ла), записати в боті", callback_data=f"saving:task:confirm:{task_id}")],
            [InlineKeyboardButton("Нагадати пізніше", callback_data=f"saving:task:remind:{task_id}")],
            [InlineKeyboardButton("Змінити суму", callback_data=f"saving:task:change:{task_id}")],
            [InlineKeyboardButton("Змінити рахунок", callback_data=f"saving:task:target:{task_id}")],
            [InlineKeyboardButton("Скасувати план", callback_data=f"saving:task:cancel:{task_id}")],
            [_home_btn()],
        ]
    )


def kb_saving_reminder_actions(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Я переказав(ла), записати", callback_data=f"saving:task:confirm:{task_id}")],
            [InlineKeyboardButton("Нагадати ще раз", callback_data=f"saving:task:remind:{task_id}")],
            [InlineKeyboardButton("Скасувати план", callback_data=f"saving:task:cancel:{task_id}")],
        ]
    )


def kb_saving_setup_prompt(*, cancel_transaction_id: int | None = None) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Створити накопичення", callback_data="saving:create_account")],
        [
            InlineKeyboardButton("Не зараз", callback_data="saving:not_now"),
            InlineKeyboardButton("Більше не питати", callback_data="saving:disable_prompt"),
        ],
    ]
    if cancel_transaction_id:
        rows.append([InlineKeyboardButton("↩️ Скасувати цю транзакцію", callback_data=f"txvoid:pick:{cancel_transaction_id}")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def kb_transaction_saved(transaction_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ Скасувати цю транзакцію", callback_data=f"txvoid:pick:{transaction_id}")],
            [_home_btn()],
        ]
    )


def kb_recent_transactions(items: list[dict]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for item in items[:10]:
        category = item.get("category") if isinstance(item.get("category"), dict) else {}
        amount = item.get("amount") if isinstance(item.get("amount"), dict) else {}
        label = f"{item.get('date', '—')} · {amount.get('display', amount.get('value', '—'))} · {category.get('name', 'Без категорії')}"
        rows.append([InlineKeyboardButton(label[:64], callback_data=f"txvoid:pick:{int(item['id'])}")])
    rows.extend(_nav_rows(HOME_MENU_OPERATIONS_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_transaction_void_confirm(transaction_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Так, скасувати транзакцію", callback_data=f"txvoid:confirm:{transaction_id}")],
            [InlineKeyboardButton("Ні, залишити", callback_data="txvoid:leave")],
        ]
    )


def kb_account_choice(accounts: list[tuple[int, str]], *, prefix: str, cancel_callback: str) -> InlineKeyboardMarkup:
    rows = _rows2([InlineKeyboardButton(label, callback_data=f"{prefix}:{account_id}") for account_id, label in accounts])
    rows.append([_cancel_btn(cancel_callback)])
    return InlineKeyboardMarkup(rows)


def kb_savings_overview_empty() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Додати рахунок", "saving:add_account"),
            _wide_callback_btn("Налаштування", "saving:settings:open"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_savings_add_menu() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Накопичення", "accounts:add:type:savings"),
            _wide_callback_btn("Депозит", "accounts:add:type:deposit"),
            _wide_callback_btn("Інвестрахунок", "accounts:add:type:investment"),
        ]
    )
    rows.extend(_nav_rows("saving:overview", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_savings_overview() -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Поповнити", "saving:topup"),
            _wide_callback_btn("Вивести", "saving:withdraw"),
            _wide_callback_btn("Мої рахунки", "saving:list"),
            _wide_callback_btn("Додати рахунок", "saving:add_account"),
            _wide_callback_btn("Налаштування", "saving:settings:open"),
        ]
    )
    rows.extend(_nav_rows(HOME_MENU_MAIN_CALLBACK))
    return InlineKeyboardMarkup(rows)


def kb_saving_confirm(prefix: str = "saving:confirm") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Так, записати переказ", callback_data=f"{prefix}:ok")],
            [InlineKeyboardButton("Ще не переказав(ла), нагадати", callback_data=f"{prefix}:remind")],
            [InlineKeyboardButton("Скасувати", callback_data=f"{prefix}:cancel")],
        ]
    )


def kb_saving_task_confirm(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Так, записати переказ", callback_data=f"saving:task:confirm:ok:{task_id}")],
            [InlineKeyboardButton("Ще не переказав(ла), нагадати", callback_data=f"saving:task:remind:{task_id}")],
            [InlineKeyboardButton("Скасувати", callback_data=f"saving:task:cancel:{task_id}")],
        ]
    )


def kb_saving_reminder_choice(task_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Через 1 годину", callback_data=f"saving:task:remind:1h:{task_id}"),
                InlineKeyboardButton("Сьогодні ввечері", callback_data=f"saving:task:remind:evening:{task_id}"),
            ],
            [
                InlineKeyboardButton("Завтра", callback_data=f"saving:task:remind:tomorrow:{task_id}"),
                InlineKeyboardButton("Обрати час", callback_data=f"saving:task:remind:custom:{task_id}"),
            ],
            [InlineKeyboardButton("Не нагадувати", callback_data=f"saving:task:remind:off:{task_id}")],
            [_cancel_btn(f"saving:task:confirm:{task_id}")],
        ]
    )


def kb_saving_settings_menu(
    *,
    ask_after_income_enabled: bool,
    ask_only_for_salary: bool,
) -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Вимкнути" if ask_after_income_enabled else "Увімкнути", "saving:settings:toggle_prompt"),
            _wide_callback_btn("Змінити відсоток", "saving:settings:percent"),
            _wide_callback_btn("Рахунок за зам.", "saving:settings:target"),
            _wide_callback_btn("Після зарплати", "saving:settings:toggle_salary_only"),
            _wide_callback_btn("Мінімальна сума", "saving:settings:min_income"),
        ]
    )
    rows.extend(_nav_rows("saving:overview", include_home=True))
    return InlineKeyboardMarkup(rows)
    rows = _rows1(
        [
            _wide_callback_btn("Вимкнути" if ask_after_income_enabled else "Увімкнути", "saving:settings:toggle_prompt"),
            _wide_callback_btn("Змінити відсоток", "saving:settings:percent"),
            _wide_callback_btn("Обрати рахунок за замовчуванням", "saving:settings:target"),
            _wide_callback_btn("Питати тільки після зарплати", "saving:settings:toggle_salary_only"),
            _wide_callback_btn("Мінімальна сума", "saving:settings:min_income"),
        ]
    )
    rows.extend(_nav_rows("saving:overview", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_saving_settings_target_accounts(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"saving:settings:target:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append([InlineKeyboardButton("Без рахунку за замовчуванням", callback_data="saving:settings:target:none")])
    rows.append([_cancel_btn("saving:settings:open")])
    return InlineKeyboardMarkup(rows)


def kb_savings_accounts_list(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = _rows2([InlineKeyboardButton(label, callback_data=f"saving:detail:{account_id}") for account_id, label in accounts])
    rows.extend(_nav_rows("saving:overview", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_savings_detail(account_id: int) -> InlineKeyboardMarkup:
    rows = _rows1(
        [
            _wide_callback_btn("Поповнити", f"saving:topup:target:{account_id}"),
            _wide_callback_btn("Вивести", f"saving:withdraw:source:{account_id}"),
            _wide_callback_btn("Історія", f"saving:history:{account_id}"),
            _wide_callback_btn("Змінити ціль", f"saving:edit_goal:{account_id}"),
            _wide_callback_btn("Редагувати назву", f"saving:edit_name:{account_id}"),
        ]
    )
    rows.extend(_nav_rows("saving:list", include_home=True))
    return InlineKeyboardMarkup(rows)


def kb_savings_history(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(_nav_rows(f"saving:detail:{account_id}", include_home=True))


def kb_savings_transfer_confirm(
    *,
    confirm_label: str,
    confirm_callback: str,
    edit_amount_callback: str,
    cancel_callback: str,
    change_target_callback: str | None = None,
) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(confirm_label, callback_data=confirm_callback)]]
    rows.append([InlineKeyboardButton("Змінити суму", callback_data=edit_amount_callback)])
    if change_target_callback:
        rows.append([InlineKeyboardButton("Змінити накопичення", callback_data=change_target_callback)])
    rows.append([InlineKeyboardButton("Скасувати" if cancel_callback != "saving:not_now" else "Не зараз", callback_data=cancel_callback)])
    return InlineKeyboardMarkup(rows)


def kb_transfer_pick_from(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"transfer:source:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append([_back_btn("menu:main"), _cancel_btn("transfer:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_transfer_pick_to(accounts: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    buttons = [InlineKeyboardButton(label, callback_data=f"transfer:target:{account_id}") for account_id, label in accounts]
    rows = _rows2(buttons)
    rows.append([_back_btn("transfer:back"), _cancel_btn("transfer:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_transfer_confirm(*, allow_rate_edit: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("✅ Підтвердити", callback_data="transfer:confirm")],
        [InlineKeyboardButton("✏️ Змінити суму", callback_data="transfer:edit:amount")],
    ]
    if allow_rate_edit:
        rows.append([InlineKeyboardButton("💱 Змінити курс", callback_data="transfer:edit:rate")])
    rows.append([InlineKeyboardButton("🔁 Змінити рахунки", callback_data="transfer:edit:accounts")])
    rows.append([_cancel_btn("transfer:cancel")])
    return InlineKeyboardMarkup(rows)
