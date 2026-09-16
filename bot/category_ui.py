from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_i18n import t


def _home_btn() -> InlineKeyboardButton:
    return InlineKeyboardButton(t("🏠 Головне меню", "🏠 Home"), callback_data="menu:main")


def _back_btn(callback_data: str = "menu:main") -> InlineKeyboardButton:
    return InlineKeyboardButton(t("⬅️ Назад", "⬅️ Back"), callback_data=callback_data)


def _cancel_btn(callback_data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(t("❌ Скасувати", "❌ Cancel"), callback_data=callback_data)


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


def categories_home_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Категорії витрат", "Expense categories"), callback_data="categories:list:expense")],
            [InlineKeyboardButton(t("Категорії доходів", "Income categories"), callback_data="categories:list:income")],
            [InlineKeyboardButton(t("Додати категорію доходів", "Add income category"), callback_data="categories:add:type:income")],
            [InlineKeyboardButton(t("Редагувати доходи", "Edit income categories"), callback_data="categories:edit:type:income")],
            [_back_btn("categories:back:settings")],
            [_home_btn()],
        ]
    )


def categories_list_keyboard(type: str) -> InlineKeyboardMarkup:
    if type == "expense":
        return InlineKeyboardMarkup([[_back_btn("categories:menu"), _home_btn()]])
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Додати категорію доходів", "Add income category"), callback_data="categories:add:type:income")],
            [InlineKeyboardButton(t("Редагувати", "Edit"), callback_data="categories:edit:type:income")],
            [_back_btn("categories:menu"), _home_btn()],
        ]
    )


def categories_edit_type_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Категорії доходів", "Income categories"), callback_data="categories:edit:type:income")],
            [_back_btn("categories:back:menu")],
        ]
    )


def categories_edit_list_keyboard(
    type: str,
    categories: list[tuple[int, str]],
    *,
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(name, callback_data=f"categories:edit:item:{int(category_id)}")
        for category_id, name in categories
    ]
    rows = _rows2(buttons)
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"categories:edit:page:{type}:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data=f"categories:edit:page:{type}:{page}"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"categories:edit:page:{type}:{page + 1}"))
        rows.append(nav)
    rows.append([_back_btn("categories:edit:start")])
    rows.append([_home_btn()])
    return InlineKeyboardMarkup(rows)


def categories_empty_edit_keyboard(type: str) -> InlineKeyboardMarkup:
    if type == "expense":
        return InlineKeyboardMarkup([[_back_btn("categories:menu")], [_home_btn()]])
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Додати категорію доходів", "Add income category"), callback_data="categories:add:type:income")],
            [InlineKeyboardButton(t("Відновити стандартні", "Restore defaults"), callback_data="categories:restore_defaults:type:income")],
            [_back_btn("categories:edit:type:income")],
        ]
    )


def categories_unavailable_keyboard(type: str | None = None) -> InlineKeyboardMarkup:
    if type == "expense":
        callback_data = "categories:list:expense"
    elif type == "income":
        callback_data = "categories:edit:type:income"
    else:
        callback_data = "categories:menu"
    return InlineKeyboardMarkup([[InlineKeyboardButton(t("До списку категорій", "Back to categories"), callback_data=callback_data)]])


def category_edit_keyboard(category: dict) -> InlineKeyboardMarkup:
    category_id = int(category.get("id"))
    category_type = str(category.get("type", "expense"))
    if category_type == "expense":
        return InlineKeyboardMarkup([[_back_btn("categories:list:expense")], [_home_btn()]])
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Перейменувати", "Rename"), callback_data=f"categories:rename:start:{category_id}")],
            [InlineKeyboardButton(t("Видалити", "Delete"), callback_data=f"categories:delete:start:{category_id}")],
            [_back_btn(f"categories:edit:type:{category_type}")],
        ]
    )


def categories_delete_confirm_keyboard(category_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Так, видалити", "Yes, delete"), callback_data=f"categories:delete:confirm:{int(category_id)}")],
            [_cancel_btn(f"categories:edit:item:{int(category_id)}")],
        ]
    )


def categories_restore_defaults_keyboard(confirm_callback: str, cancel_callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t("Так, відновити", "Yes, restore"), callback_data=confirm_callback)],
            [_cancel_btn(cancel_callback)],
        ]
    )


def hidden_categories_keyboard(categories: list[tuple[int, str, str]]) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton(
            t("Відновити: {name}", "Restore: {name}").format(name=name),
            callback_data=f"categories:restore:{category_id}",
        )
        for category_id, name, _type in categories
    ]
    rows = _rows2(buttons)
    rows.append([_back_btn("categories:menu"), _home_btn()])
    return InlineKeyboardMarkup(rows)
