from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import logging
from pathlib import Path
import re
import secrets
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import asyncpg
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
try:
    from telegram import BotCommand, MenuButtonCommands, WebAppInfo
except ImportError:
    @dataclass(frozen=True)
    class BotCommand:
        command: str
        description: str

    @dataclass(frozen=True)
    class MenuButtonCommands:
        pass

    @dataclass(frozen=True)
    class MenuButtonWebApp:
        text: str
        web_app: Any

    WebAppInfo = None
else:
    try:
        from telegram import MenuButtonWebApp
    except ImportError:
        @dataclass(frozen=True)
        class MenuButtonWebApp:
            text: str
            web_app: Any
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    Defaults,
    ConversationHandler,
    MessageHandler,
    filters,
)

if TYPE_CHECKING:
    from category_services import Category
    from telegram import Message

import config
try:
    from runtime_schema import bootstrap_async
except ModuleNotFoundError as exc:
    if exc.name != "runtime_schema":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from runtime_schema import bootstrap_async
from confirmation_guard import (
    FINANCIAL_CONFIRMATION_STALE_TEXT,
    bind_financial_preview,
    consume_financial_confirmation,
    is_current_confirmation,
    validated_confirmation_action,
)
from admin_integrations import (
    consume_pending_admin_reset,
    create_feedback_from_poll,
    create_support_case,
    format_error_notification,
    format_new_user_notification,
    format_onboarding_notification,
    get_pending_admin_reset,
    get_bot_setting,
    is_user_banned,
    is_registration_open,
    log_bot_event,
    log_admin_notification,
    notify_admins,
    save_poll_response,
    should_block_for_maintenance,
    sync_onboarding_debug_state,
)
from billing_client import (
    BillingAPIError,
    cancel_autorenew,
    get_access_state,
    get_billing_state,
    init_bind_session,
    init_recovery_payment_session,
    retry_renewal,
    sync_pending_bind_status,
)
from transaction_cancellation_client import (
    TransactionCancellationAPIError,
    build_transaction_void_draft,
    confirm_transaction_void,
    list_recent_transactions,
)
from trial_recovery import (
    begin_free_text as begin_trial_recovery_free_text,
    consume_free_text as consume_trial_recovery_free_text,
    decline_contact as decline_trial_recovery_contact,
    format_owner_notification as format_trial_recovery_owner_notification,
    get_recipient_owner_snapshot as get_trial_recovery_owner_snapshot,
    opt_out as opt_out_trial_recovery,
    owner_reply_allowed,
    reason_label as trial_recovery_reason_label,
    record_reason as record_trial_recovery_reason,
    request_contact as request_trial_recovery_contact,
)
from help_content import get_faq_catalog, get_faq_question, get_faq_topic
from bot_i18n import choose_locale, current_locale as current_ui_locale, normalize_locale as normalize_ui_locale, set_current_locale, t as locale_text
from keyboards import (
    HOME_MENU_DASHBOARD_CALLBACK,
    HOME_MENU_FINANCES_CALLBACK,
    HOME_MENU_MORE_CALLBACK,
    HOME_MENU_OPERATIONS_CALLBACK,
    HOME_MENU_REPORTS_CALLBACK,
    kb_ai_batch_confirm,
    kb_ai_batch_item_edit,
    kb_ai_pick_account,
    kb_ai_pick_category,
    kb_ai_pick_type,
    kb_ai_tx_confirm,
    kb_ai_tx_edit,
    kb_ai_pick_type_prefixed,
    kb_ai_tx_confirm_prefixed,
    kb_ai_tx_edit_prefixed,
    kb_account_add_menu,
    kb_account_archive_confirm_keyboard,
    kb_account_choice,
    kb_account_detail_keyboard,
    kb_account_type_change,
    kb_accounts_manage_keyboard,
    kb_accounts_menu,
    kb_billing_menu,
    categories_home_keyboard,
    categories_list_keyboard,
    category_edit_keyboard,
    hidden_categories_keyboard,
    kb_currency,
    kb_currency_resolution,
    kb_dashboard_launch_menu,
    kb_debts_menu,
    kb_debt_close_confirm,
    kb_debt_delete_confirm,
    kb_debt_detail,
    kb_debt_direction_choice,
    kb_debt_edit_fields,
    kb_debt_fx_choice,
    kb_help_answer,
    kb_help_home,
    kb_help_support,
    kb_help_topic,
    kb_expense_reminder_no_expenses,
    kb_expense_reminder_settings,
    kb_home_finances_menu,
    kb_home_more_menu,
    kb_home_operations_menu,
    kb_home_reports_menu,
    kb_home as _kb_home_base,
    kb_inline_cancel,
    kb_language,
    kb_onb_privacy,
    kb_account_create_confirm,
    kb_account_type,
    kb_onb_account_type,
    kb_onb_account_bank,
    kb_onb_account_last4,
    kb_onb_accounts_more_done,
    onboarding_add_category_type_keyboard,
    onboarding_categories_mode_keyboard,
    onboarding_confirm_keyboard,
    onboarding_custom_categories_main_keyboard,
    onboarding_expense_categories_checkbox_keyboard,
    onboarding_income_categories_checkbox_keyboard,
    kb_onb_edit_accounts,
    kb_onb_start_date,
    kb_pick_account,
    kb_pick_category,
    kb_export_menu,
    kb_family_confirm_leave,
    kb_family_confirm_remove,
    kb_family_empty,
    kb_family_invite_accept,
    kb_family_member_menu,
    kb_family_members,
    kb_family_owner_menu,
    kb_reports_menu,
    kb_saving_confirm,
    kb_saving_income_prompt,
    kb_saving_plan_actions,
    kb_saving_reminder_actions,
    kb_saving_reminder_choice,
    kb_saving_setup_prompt,
    kb_saving_settings_menu,
    kb_saving_settings_target_accounts,
    kb_saving_target_accounts,
    kb_saving_task_confirm,
    kb_savings_add_menu,
    kb_savings_accounts_list,
    kb_savings_detail,
    kb_savings_history,
    kb_savings_overview,
    kb_savings_overview_empty,
    kb_savings_transfer_confirm,
    kb_settings_accounts_menu,
    kb_settings_menu,
    kb_tx_confirm,
    kb_debt_confirm,
    kb_debt_invite_decision,
    kb_debt_invite_prompt,
    kb_debt_invite_ready_actions,
    kb_debt_invite_skip_actions,
    kb_debt_only_home,
    kb_debt_observer_detail,
    kb_debt_overlimit,
    kb_debt_repay_choice,
    kb_recent_transactions,
    kb_transaction_saved,
    kb_transaction_void_confirm,
    kb_transfer_confirm,
    kb_transfer_pick_from,
    kb_transfer_pick_to,
    kb_paywall_home,
)
from category_ui import (
    _back_btn,
    _cancel_btn,
    _home_btn,
    categories_delete_confirm_keyboard,
    categories_edit_list_keyboard,
    categories_edit_type_keyboard,
    categories_empty_edit_keyboard,
    categories_home_keyboard,
    categories_list_keyboard,
    categories_restore_defaults_keyboard,
    categories_unavailable_keyboard,
    category_edit_keyboard,
    hidden_categories_keyboard,
)
from finance import (
    CREDIT_ACCOUNT_TYPE,
    SUPPORTED_ACCOUNT_CURRENCIES,
    SAVINGS_ACCOUNT_TYPES,
    account_type_label,
    build_transfer_rate_prompt,
    calculate_transfer_amount,
    default_non_negative_account_type,
    format_decimal as format_decimal_value,
    escape_html,
    format_account_line,
    format_account_name,
    format_exchange_rate,
    format_money,
    is_valid_currency_code,
    normalize_account_name,
    normalize_account_type,
    normalize_currency,
    parse_decimal_amount,
    parse_decimal_rate,
    quantize_money,
    quantize_rate,
)
from fx_rates import get_latest_rates
from expense_category_catalog import (
    DEFAULT_EXPENSE_CATEGORIES as CATALOG_DEFAULT_EXPENSE_CATEGORIES,
    DEFAULT_EXPENSE_CATEGORY_BY_SLUG as CATALOG_DEFAULT_EXPENSE_CATEGORY_BY_SLUG,
    DEFAULT_EXPENSE_FALLBACK_SLUG as CATALOG_DEFAULT_EXPENSE_FALLBACK_SLUG,
    DEFAULT_EXPENSE_CATEGORY_SLUGS as CATALOG_DEFAULT_EXPENSE_CATEGORY_SLUGS,
)
from normalization_service import match_expense_normalization
from parsing import parse_amount, parse_currency_info, parse_message, parse_message_batch
from stt import transcribe_ogg_bytes
from category_services import (
    CategoryConflictError,
    CategoryService,
    CategoryTemplateService,
    CategoryUnavailableError,
    CategoryValidationError,
    normalize_category_name,
)
from account_service import AccountService
from report_service import ReportService, split_report_text
from transfer_service import SUCCESS_TRANSFER_STATUSES, TransferService
from transaction_service import SUCCESS_TRANSACTION_STATUSES, TransactionService
from debt_service import DebtService
from push_client import dispatch_push_event
from expense_reminder_service import (
    DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
    DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
    DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE,
    ExpenseReminderService,
    SUPPORTED_DAILY_EXPENSE_REMINDER_HOURS,
)
from family_service import FamilyService, generate_family_invite_token
from finance_scope import get_current_finance_scope
from savings_service import SavingsService, is_savings_account_type
from ai_transaction_draft_service import AiTransactionDraft, AiTransactionDraftService
from vision_tx import (
    AccountOption as VisionAccountOption,
    CategoryOption as VisionCategoryOption,
    OpenAIVisionConfigError,
    OpenAIVisionError,
    OpenAIVisionInvalidResponseError,
    ScreenshotAnalysis,
    analyze_screenshot,
    is_screenshot_currency_explicit,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mcf-bot")


def _ui_text(uk_text: str, en_text: str, locale: str | None = None) -> str:
    return locale_text(uk_text, en_text, locale)


def _user_locale_from_value(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return normalize_ui_locale(raw, default="")


def _user_locale(user: Any | None = None) -> str | None:
    if user is None:
        return None
    if isinstance(user, dict):
        return _user_locale_from_value(user.get("lang"))
    return _user_locale_from_value(getattr(user, "lang", None))


async def _load_stored_locale(conn: asyncpg.Connection, tg_user_id: int | None) -> str | None:
    if not tg_user_id:
        return None
    try:
        return _user_locale_from_value(await conn.fetchval("SELECT lang FROM users WHERE tg_user_id=$1", tg_user_id))
    except Exception:
        return None


async def _activate_locale(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    tg_user_id: int | None = None,
    conn: asyncpg.Connection | None = None,
    user: Any | None = None,
    telegram_locale: str | None = None,
) -> str:
    onboarding_locale = _user_locale_from_value((context.user_data.get("onb") or {}).get("lang"))
    cached_locale = _user_locale_from_value(context.user_data.get("locale"))
    user_locale = _user_locale(user)
    stored_locale = None
    if not cached_locale and not onboarding_locale and not user_locale and tg_user_id and conn is not None:
        stored_locale = await _load_stored_locale(conn, tg_user_id)
    locale = choose_locale(onboarding_locale, cached_locale, user_locale, stored_locale, telegram_locale)
    context.user_data["locale"] = locale
    return set_current_locale(locale)


async def _set_locale_for_user(
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    fallback: str | None = None,
) -> str:
    stored_locale = await _load_stored_locale(conn, tg_user_id)
    return set_current_locale(choose_locale(stored_locale, fallback))


def _current_context_locale(context: ContextTypes.DEFAULT_TYPE, telegram_locale: str | None = None) -> str:
    locale = choose_locale(
        _user_locale_from_value((context.user_data.get("onb") or {}).get("lang")),
        _user_locale_from_value(context.user_data.get("locale")),
        telegram_locale,
    )
    context.user_data["locale"] = locale
    return set_current_locale(locale)


def _home_menu_text(locale: str | None = None) -> str:
    return _ui_text(
        "<b>🏠 Головне меню</b>\n\n🎙 Надиктуйте витрату або дохід\n📥 Або надішліть скріншот",
        "<b>🏠 Home</b>\n\n🎙 Dictate an expense or income\n📥 Or send a screenshot",
        locale,
    )


def _miniapp_unavailable_text(locale: str | None = None) -> str:
    return _ui_text(
        "Кабінет ще не налаштований. Спробуйте трохи пізніше.",
        "The dashboard is not configured yet. Please try again later.",
        locale,
    )


def _user_not_found_text(locale: str | None = None) -> str:
    return _ui_text(
        "Не вдалося знайти ваш профіль. Натисніть /start і пройдіть налаштування ще раз.",
        "We couldn't find your profile. Tap /start and complete setup again.",
        locale,
    )


def _onboarding_required_text(locale: str | None = None) -> str:
    return _ui_text(
        "Спершу завершіть коротке налаштування. Натисніть /start, і я проведу вас по кроках.",
        "Please finish the quick setup first. Tap /start and I'll guide you through it.",
        locale,
    )


def _access_restricted_text(locale: str | None = None) -> str:
    return _ui_text(
        "Доступ тимчасово обмежено. Напишіть у підтримку, і ми перевіримо це вручну.",
        "Access is temporarily restricted. Contact support and we'll check it manually.",
        locale,
    )


def _onboarding_in_progress_text(locale: str | None = None) -> str:
    return _ui_text(
        "Зараз триває налаштування. Завершіть поточний крок або натисніть /start, щоб почати заново.",
        "Setup is still in progress. Finish the current step or tap /start to begin again.",
        locale,
    )


def _onboarding_voice_blocked_text(locale: str | None = None) -> str:
    return _ui_text(
        "Зараз триває налаштування. Завершіть його, і після цього можна буде надсилати голосові повідомлення.",
        "Setup is still in progress. Finish it first, then you'll be able to send voice messages.",
        locale,
    )


def _onboarding_screenshot_blocked_text(locale: str | None = None) -> str:
    return _ui_text(
        "Зараз триває налаштування. Завершіть його, і після цього можна буде надсилати скріншоти.",
        "Setup is still in progress. Finish it first, then you'll be able to send screenshots.",
        locale,
    )


def _debt_action_inactive_text(locale: str | None = None) -> str:
    return _ui_text(
        "Ця дія вже неактивна. Поверніться до меню боргів і спробуйте ще раз.",
        "This action is no longer active. Go back to the debts menu and try again.",
        locale,
    )


def _categories_action_inactive_text(locale: str | None = None) -> str:
    return _ui_text(
        "Ця дія вже неактивна. Поверніться до категорій і спробуйте ще раз.",
        "This action is no longer active. Go back to categories and try again.",
        locale,
    )


def _debt_invite_borrower_link_text(locale: str | None = None) -> str:
    return _ui_text(
        "Це посилання для людини, яка має підтвердити нагадування про борг. Надішліть його їй.",
        "This link is for the person who should confirm the debt reminder. Please send it to them.",
        locale,
    )


def _onboarding_privacy_text(locale: str | None = None) -> str:
    return _ui_text(
        (
            "Як ми працюємо з вашими даними\n\n"
            "Ваші фінансові записи потрібні для роботи бота: показати баланс, побудувати звіти, знайти витрати і порахувати статистику.\n\n"
            "Ми не продаємо ваші дані і не використовуємо їх для реклами.\n\n"
            "Для голосових повідомлень і скріншотів можуть використовуватись зовнішні AI-сервіси. "
            "Передаємо тільки вміст повідомлення або зображення, потрібний для розпізнавання операції.\n\n"
            "Детальні умови доступні в політиці конфіденційності."
        ),
        (
            "How we use your data\n\n"
            "Your finance records are needed for the bot to work: to show balances, build reports, find expenses, and calculate stats.\n\n"
            "We do not sell your data and do not use it for advertising.\n\n"
            "Voice messages and screenshots may use external AI services. "
            "We only send the message or image content needed to recognize the transaction.\n\n"
            "Full details are available in the privacy policy."
        ),
        locale,
    )


def _privacy_policy_missing_text(locale: str | None = None) -> str:
    return _ui_text(
        "Посилання на політику конфіденційності ще не налаштоване. Поверніться до налаштування або спробуйте пізніше.",
        "The privacy policy link is not configured yet. Go back to setup or try again later.",
        locale,
    )


def _localized_bot_commands(locale: str, *, include_miniapp: bool) -> list[BotCommand]:
    commands = [
        BotCommand("start", _ui_text("Запустити бота", "Start the bot", locale)),
        BotCommand("menu", _ui_text("Відкрити головне меню", "Open the main menu", locale)),
        BotCommand("help", _ui_text("Відкрити допомогу", "Open help", locale)),
        BotCommand("settings", _ui_text("Відкрити налаштування", "Open settings", locale)),
    ]
    if include_miniapp:
        commands.append(BotCommand("cabinet", _ui_text("Відкрити кабінет", "Open dashboard", locale)))
    return commands


HOME_MENU_TEXT = _home_menu_text()
MINIAPP_UNAVAILABLE_TEXT = _miniapp_unavailable_text()
USER_NOT_FOUND_TEXT = _user_not_found_text()
ONBOARDING_REQUIRED_TEXT = _onboarding_required_text()
ACCESS_RESTRICTED_TEXT = _access_restricted_text()
ONBOARDING_PRIVACY_TEXT = _onboarding_privacy_text()
PRIVACY_POLICY_MISSING_TEXT = _privacy_policy_missing_text()
BOT_COMMANDS = _localized_bot_commands("uk", include_miniapp=False)
MINIAPP_BOT_COMMAND = _localized_bot_commands("uk", include_miniapp=True)[-1]

FULL_ACCESS_REQUIRED_NOTICE = (
    "<b>\u0426\u0435\u0439 \u0440\u043e\u0437\u0434\u0456\u043b \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0438\u0439 \u0431\u0435\u0437 \u043f\u043e\u0432\u043d\u043e\u0433\u043e \u0434\u043e\u0441\u0442\u0443\u043f\u0443.</b>\n\n"
    "\u041e\u0444\u043e\u0440\u043c\u0456\u0442\u044c \u043f\u0456\u0434\u043f\u0438\u0441\u043a\u0443 \u0430\u0431\u043e \u0441\u043a\u043e\u0440\u0438\u0441\u0442\u0430\u0439\u0442\u0435\u0441\u044c \u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0438\u043c\u0438 \u0434\u043b\u044f \u0446\u044c\u043e\u0433\u043e \u0430\u043a\u0430\u0443\u043d\u0442\u0430 \u0441\u0446\u0435\u043d\u0430\u0440\u0456\u044f\u043c\u0438."
)

CURRENT_ONBOARDING_VERSION = 3
ONBOARDING_ACCOUNT_LIMIT = 15
ONBOARDING_IN_PROGRESS_TEXT = _onboarding_in_progress_text()
ONBOARDING_VOICE_BLOCKED_TEXT = _onboarding_voice_blocked_text()
ONBOARDING_SCREENSHOT_BLOCKED_TEXT = _onboarding_screenshot_blocked_text()
DEBT_ACTION_INACTIVE_TEXT = _debt_action_inactive_text()
CATEGORIES_ACTION_INACTIVE_TEXT = _categories_action_inactive_text()
DEBT_INVITE_BORROWER_LINK_TEXT = _debt_invite_borrower_link_text()
OPTIONAL_CARD_LAST4_TEXT = (
    "Останні 4 цифри картки потрібні тільки для зручності.\n\n"
    "Можете ввести їх або пропустити цей крок."
)
ACCOUNT_NOT_FOUND_REFRESH_TEXT = "Рахунок не знайдено. Оновіть список і спробуйте ще раз."
AI_BATCH_INACTIVE_TEXT = "Ця чернетка вже неактивна. Надішліть повідомлення ще раз, і я підготую нову."

BOT_COPY_DEFAULTS: dict[str, str] = {
    "onboarding_welcome_text": (
        "Привіт! Це vydno.capital.\n\n"
        "Допоможу вести витрати, доходи, рахунки, борги та заощадження в Telegram.\n\n"
        "Спочатку швидко налаштуємо бота під вас: мова, валюта, стартова дата, рахунки.\n\n"
        "Це займе 1-2 хвилини.\n\n"
        "Крок 1 з 5: оберіть мову."
    ),
    "onboarding_restart_notice_text": (
        "Починаємо налаштування з початку.\n\n"
        "Поточні операції залишаться без змін. За потреби рахунки можна буде відредагувати на кроці підтвердження."
    ),
    "onboarding_currency_step_text": (
        "Крок 2 з 5: базова валюта.\n\n"
        "У цій валюті бот показуватиме звіти та підказки за замовчуванням."
    ),
    "onboarding_start_date_step_text": (
        "Крок 3 з 5: стартова дата.\n\n"
        "З якого дня рахувати звіти? Можете натиснути кнопку або ввести дату у форматі `dd.mm.yyyy`."
    ),
    "onboarding_categories_step_text": "",
    "onboarding_categories_custom_text": "",
    "support_intro_text": (
        "<b>🆘 Допомога</b>\n\n"
        "Опишіть проблему одним повідомленням.\n\n"
        "Адміністратор побачить ваше звернення і зможе відповісти."
    ),
    "settings_intro_text": "<b>⚙️ Налаштування</b>\nОберіть, що хочете змінити:",
    "reports_intro_text": "<b>📊 Звіти</b>\nОберіть період:",
    "export_intro_text": "<b>📤 Експорт Excel</b>\n\nОберіть період, за який потрібно сформувати файл.",
    "categories_intro_text": "Категорії допомагають групувати витрати й доходи.\n\nОберіть, що хочете зробити:",
    "family_disabled_text": "<b>👨‍👩‍👧 Сімейний доступ</b>\n\nФункцію тимчасово вимкнено.",
    "family_empty_text": (
        "<b>👨‍👩‍👧 Сімейний доступ</b>\n\n"
        "У вас ще немає сімейного бюджету.\n\n"
        "Створіть родину, щоб вести фінанси разом з партнером або членами сім’ї."
    ),
    "accounts_empty_text": (
        "<b>💼 Рахунки</b>\n\n"
        "У вас ще немає жодного рахунку.\n\n"
        "Щоб почати облік, додайте перший рахунок."
    ),
    "savings_empty_text": (
        "<b>🐿️ Заощадження та інвестиції</b>\n\n"
        "Поки тут порожньо.\n\n"
        "Створіть перший рахунок накопичень: накопичення, депозит або інвестрахунок, де ви реально тримаєте відкладені гроші."
    ),
    "savings_missing_account_text": "Рахунок накопичень не знайдено.",
    "savings_need_first_account_text": "Спочатку додайте рахунок накопичень.",
    "hidden_categories_empty_text": "Прихованих категорій немає.",
    "saving_reminder_text": (
        "Котику, нагадую про сейв.\n\n"
        "План був такий:\n"
        "{amount_text} у \"{target_label}\".\n\n"
        "Якщо вже переказав(ла), запишемо це в боті.\n"
        "Якщо ні, зараз гарний момент зробити переказ."
    ),
    "saving_after_income_text": (
        "Дохід збережено: +{amount_text}.\n\n"
        "{percent_text} від цього доходу: {saving_amount_text}.\n\n"
        "Можна одразу відкласти частину в накопичення."
    ),
    "saving_setup_text": (
        "Дохід збережено: +{amount_text}.\n\n"
        "{percent_text} від цього доходу: {saving_amount_text}.\n\n"
        "Можна одразу відкласти частину в накопичення.\n\n"
        "Щоб відкладати гроші, створіть перше накопичення: банку, сейф, окрему картку або інше місце для відкладених грошей."
    ),
    "saving_plan_text": (
        "План є.\n\n"
        "Переказати:\n"
        "{amount_text}\n\n"
        "Звідки:\n"
        "{source_label}\n\n"
        "Куди:\n"
        "{target_label}\n\n"
        "Тепер зробіть реальний переказ у банку. Коли гроші вже переказані, натисніть \"Я переказав(ла)\"."
    ),
    "saving_topup_confirm_text": (
        "Поповнити накопичення?\n\n"
        "Звідки:\n"
        "{source_label}\n\n"
        "Куди:\n"
        "{target_label}\n\n"
        "Сума:\n"
        "{amount_text}\n\n"
        "Перевірте: гроші вже реально переказані або відкладені.\n"
        "Це внутрішній переказ, він не потрапить у витрати."
    ),
    "saving_post_income_confirm_text": (
        "Відкласти в накопичення?\n\n"
        "Звідки:\n"
        "{source_label}\n\n"
        "Куди:\n"
        "{target_label}\n\n"
        "Сума:\n"
        "{amount_text}\n\n"
        "Спочатку зробіть реальний переказ у банку або відкладіть гроші фізично.\n"
        "Коли гроші вже переїхали, натисніть кнопку нижче."
    ),
    "debt_invite_post_create_text": (
        "✅ Борг збережено.\n\n"
        "Хто дав: ви\n"
        "Хто отримав: {borrower_name}\n"
        "Сума боргу: {initial_amount}\n"
        "Повернуто: {paid_amount}\n"
        "Залишок: {remaining_amount}\n"
        "Коментар: {comment}\n\n"
        "Хочете створити посилання для {borrower_name}, щоб бот раз на місяць автоматично нагадував про залишок боргу?"
    ),
    "debt_invite_owner_message_text": (
        "Посилання для {borrower_name} готове.\n\n"
        "Надішліть боржнику повідомлення нижче.\n\n"
        "------\n\n"
        "Привіт! Я зафіксував(ла) у боті борг.\n\n"
        "Хто дав: {owner_name}\n"
        "Хто отримав: {borrower_name}\n"
        "Сума боргу: {initial_amount}\n"
        "Коментар: {comment}\n\n"
        "Підтвердіть, будь ласка, що це ваш Telegram-акаунт для цього боргу.\n\n"
        "Після підтвердження бот буде раз на місяць автоматично нагадувати вам про залишок.\n\n"
        "Важливо: ви не будете керувати боргом у боті. Повернення коштів відмічає тільки {owner_name}.\n\n"
        "{link}"
    ),
    "debt_invite_share_message_text": (
        "Привіт! Я зафіксував(ла) у боті борг.\n\n"
        "Хто дав: {owner_name}\n"
        "Хто отримав: {borrower_name}\n"
        "Сума боргу: {initial_amount}\n"
        "Коментар: {comment}\n\n"
        "Підтвердіть, будь ласка, що це ваш Telegram-акаунт для цього боргу.\n\n"
        "Після підтвердження бот буде раз на місяць автоматично нагадувати вам про залишок.\n\n"
        "Важливо: ви не будете керувати боргом у боті. Повернення коштів відмічає тільки {owner_name}.\n\n"
        "{link}"
    ),
    "debt_invite_claim_text": (
        "Користувач {lender_name} вказав, що дав вам у борг:\n\n"
        "Сума: {initial_amount}\n"
        "Боржник у записі: {counterparty_name}\n"
        "Коментар: {comment}\n\n"
        "Після підтвердження бот буде раз на місяць надсилати вам автоматичне нагадування про залишок боргу.\n\n"
        "Важливо: цим боргом керує тільки {lender_name}. Ви не зможете редагувати суму, закривати борг або відмічати повернення в боті.\n\n"
        "Підтверджуєте, що це ваш Telegram-акаунт для цього боргу?"
    ),
    "debt_monthly_reminder_text": (
        "Дружнє нагадування про борг.\n\n"
        "{lender_name} дав(ла) вам у борг: {initial_amount}\n"
        "Повернуто за записом {lender_name}: {paid_amount}\n"
        "Залишок: {remaining_amount}\n\n"
        "Це автоматичне щомісячне нагадування. Якщо ви вже повернули кошти, попросіть {lender_name} відмітити це в боті."
    ),
    "daily_expense_reminder_text_1": (
        "Ого, сьогодні поки 0 грн витрат 😄\n"
        "Або ти реально економний, або просто ще не вніс день.\n\n"
        "Якщо щось було, відкрий головний екран або просто надішли мені голос, текст чи скрін з банку. Я рознесу все по категоріях."
    ),
    "daily_expense_reminder_text_2": (
        "День майже закритий, а витрат за сьогодні ще немає 👀\n"
        "Перевіримо: це був день без покупок чи просто забув внести?\n\n"
        "Можеш написати в чат: \"кава 80, таксі 230, продукти 600\", або скинути скрін з банку."
    ),
    "daily_expense_reminder_text_3": (
        "Фінансовий чекпоінт дня 🧾\n"
        "Сьогодні в мене ще немає твоїх витрат.\n\n"
        "Додай їх зараз, поки пам'ятаєш. Можна через головний екран, голосом, текстом або скріншотом."
    ),
    "daily_expense_reminder_text_4": (
        "Поки що виглядає так, ніби ти сьогодні нічого не витрачав 😎\n"
        "Якщо це правда, можна зафіксувати день без витрат.\n\n"
        "Якщо покупки були, просто надішли їх мені будь-яким зручним способом."
    ),
    "daily_expense_reminder_text_5": (
        "Маленька вечірня звичка 💸\n"
        "Витрати за сьогодні ще не внесені.\n\n"
        "Краще додати зараз, ніж завтра згадувати \"а куди поділись гроші\". Напиши, надиктуй або скинь скрін з банку, я все підготую."
    ),
    "daily_expense_reminder_7day_text": (
        "Бачу, вечірні нагадування не дуже заходять 😅\n"
        "Можу нагадувати рідше або не чіпати тебе щодня. Налаштування можна змінити на головному екрані."
    ),
    "daily_expense_no_expenses_confirm_text": (
        "Готово, день закритий ✅\n"
        "Витрат за сьогодні не було."
    ),
    "daily_expense_reminder_timezone": DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE,
}

BOT_COPY_DEFAULTS_EN: dict[str, str] = {
    "onboarding_welcome_text": (
        "Hi! This is vydno.capital.\n\n"
        "I will help you track expenses, income, accounts, debts, and savings in Telegram.\n\n"
        "First, let's quickly set up the bot for you: language, currency, start date, and accounts.\n\n"
        "This will take 1-2 minutes.\n\n"
        "Step 1 of 5: choose a language."
    ),
    "onboarding_restart_notice_text": (
        "Let's restart setup from the beginning.\n\n"
        "Your existing transactions will stay unchanged. If needed, you'll be able to edit accounts again at the confirmation step."
    ),
    "onboarding_currency_step_text": (
        "Step 2 of 5: base currency.\n\n"
        "The bot will use this currency for reports and default hints."
    ),
    "onboarding_start_date_step_text": (
        "Step 3 of 5: start date.\n\n"
        "From which date should reports begin? You can tap a button or enter a date in `dd.mm.yyyy` format."
    ),
    "support_intro_text": (
        "<b>🆘 Support</b>\n\n"
        "Describe the problem in one message.\n\n"
        "An admin will see your request and will be able to reply."
    ),
    "settings_intro_text": "<b>⚙️ Settings</b>\nChoose what you want to change:",
    "reports_intro_text": "<b>📊 Reports</b>\nChoose a period:",
    "export_intro_text": "<b>📤 Excel Export</b>\n\nChoose the period for the file.",
    "categories_intro_text": "Categories help group expenses and income.\n\nChoose what you want to do:",
    "family_disabled_text": "<b>👨‍👩‍👧 Family Access</b>\n\nThis feature is temporarily disabled.",
    "family_empty_text": (
        "<b>👨‍👩‍👧 Family Access</b>\n\n"
        "You don't have a family budget yet.\n\n"
        "Create one to manage finances together with your partner or family members."
    ),
    "accounts_empty_text": (
        "<b>💼 Accounts</b>\n\n"
        "You don't have any accounts yet.\n\n"
        "Add your first account to start tracking."
    ),
    "savings_empty_text": (
        "<b>🐿️ Savings & Investments</b>\n\n"
        "Nothing here yet.\n\n"
        "Create your first savings account: savings, deposit, or investment account where you actually keep reserved money."
    ),
    "savings_missing_account_text": "Savings account not found.",
    "savings_need_first_account_text": "Add a savings account first.",
    "hidden_categories_empty_text": "There are no hidden categories.",
}


class _CopyFormatDict(dict[str, object]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _format_copy_text(template: str, **kwargs: object) -> str:
    try:
        return template.format_map(_CopyFormatDict(kwargs))
    except Exception as exc:
        logger.warning("Failed to format bot copy template: %s", exc)
        return template


async def _get_bot_copy(
    conn: asyncpg.Connection,
    key: str,
    *,
    locale: str | None = None,
    **kwargs: object,
) -> str:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    if resolved_locale == "en":
        template = BOT_COPY_DEFAULTS_EN.get(key, BOT_COPY_DEFAULTS.get(key, ""))
    else:
        default = BOT_COPY_DEFAULTS.get(key, "")
        template = str(await get_bot_setting(conn, key, default))
    try:
        return _format_copy_text(str(template), **kwargs)
    except Exception as exc:
        logger.warning("Failed to format bot copy %s: %s", key, exc)
        return str(template)


async def _get_bot_copy_from_context(context: ContextTypes.DEFAULT_TYPE, key: str, **kwargs: object) -> str:
    async with _pool(context).acquire() as conn:
        return await _get_bot_copy(conn, key, locale=_current_context_locale(context), **kwargs)


def _daily_expense_reminder_mode_label(mode: str) -> str:
    normalized = str(mode or DEFAULT_DAILY_EXPENSE_REMINDER_MODE).strip().lower()
    if normalized == "weekdays":
        return "Будні"
    if normalized == "off":
        return "Вимкнено"
    return "Щодня"


async def _get_daily_expense_reminder_zone(conn: asyncpg.Connection) -> tuple[ZoneInfo, str]:
    raw_name = str(
        await get_bot_setting(
            conn,
            "daily_expense_reminder_timezone",
            BOT_COPY_DEFAULTS.get("daily_expense_reminder_timezone", DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE),
        )
        or DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE
    ).strip()
    try:
        return ZoneInfo(raw_name), raw_name
    except Exception:
        logger.warning(
            "Unavailable or invalid daily expense reminder timezone %r, fallback to %s",
            raw_name,
            DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE,
        )
        try:
            return ZoneInfo(DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE), DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE
        except Exception:
            fallback_zone = datetime.now().astimezone().tzinfo
            if fallback_zone is None:
                fallback_zone = ZoneInfo("UTC")
            logger.warning("ZoneInfo data unavailable, fallback to local timezone object for daily expense reminders")
            return fallback_zone, DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE


def _daily_expense_reminder_settings_text(*, mode: str, reminder_hour: int, timezone_name: str) -> str:
    return (
        "<b>🌙 Вечірні нагадування</b>\n\n"
        "Надсилаю одне дружнє нагадування, якщо за день ще немає жодної витрати типу expense.\n\n"
        f"Режим: <b>{escape_html(_daily_expense_reminder_mode_label(mode))}</b>\n"
        f"Час: <b>{int(reminder_hour):02d}:00</b>\n"
        f"Часова зона: <code>{escape_html(timezone_name)}</code>"
    )


async def _show_daily_expense_reminder_settings(message, conn: asyncpg.Connection, tg_user_id: int) -> None:
    settings = await ExpenseReminderService(conn).get_or_create_settings(tg_user_id)
    timezone_name = (await _get_daily_expense_reminder_zone(conn))[1]
    mode = str(settings.get("mode") or DEFAULT_DAILY_EXPENSE_REMINDER_MODE)
    reminder_hour = int(settings.get("reminder_hour") or DEFAULT_DAILY_EXPENSE_REMINDER_HOUR)
    await message.reply_text(
        _daily_expense_reminder_settings_text(
            mode=mode,
            reminder_hour=reminder_hour,
            timezone_name=timezone_name,
        ),
        reply_markup=kb_expense_reminder_settings(mode, reminder_hour),
        parse_mode=ParseMode.HTML,
    )


async def _mark_daily_expense_recorded_from_result(conn: asyncpg.Connection, tg_user_id: int, transaction_result) -> None:
    if transaction_result is None:
        return
    if str(getattr(transaction_result, "status", "") or "") not in SUCCESS_TRANSACTION_STATUSES:
        return
    if str(getattr(transaction_result, "kind", "") or "") != "expense":
        return
    tx_day = getattr(transaction_result, "transaction_date", None)
    if not isinstance(tx_day, date):
        return
    await ExpenseReminderService(conn).mark_expense_recorded(tg_user_id, tx_day)


(
    LANG,
    BASE_CURRENCY,
    START_DATE,
    ACC_BANK,
    ACC_BANK_TEXT,
    ACC_ACCOUNT_TYPE,
    ACC_LAST4_CHOICE,
    ACC_LAST4_TEXT,
    ACC_CURRENCY,
    ACC_BALANCE,
    ACC_MORE_DONE,
    ONB_CONFIRM,
    ONB_EDIT_ACCOUNTS,
) = range(13)

SYSTEM_EXPENSE_CATEGORY_NAME = "Інше"
SYSTEM_INCOME_CATEGORY_NAME = "Інший дохід"
AI_SCREENSHOT_SOURCE = "screenshot"
BILLING_TX_DRAFT_SOURCE = "billing_subscription"
BILLING_TX_CATEGORY_SLUG = "subscriptions_services"
BILLING_TX_INACTIVE_TEXT = "Ця чернетка вже неактивна."
AI_SCREENSHOT_MAX_BYTES = 10 * 1024 * 1024
AI_SCREENSHOT_DRAFT_SCHEMA_VERSION = 4
AI_SCREENSHOT_ALLOWED_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
    "image/gif",
}

SYSTEM_EXPENSE_CATEGORY_NAME = str(
    CATALOG_DEFAULT_EXPENSE_CATEGORY_BY_SLUG[CATALOG_DEFAULT_EXPENSE_FALLBACK_SLUG]["name"]
)


@dataclass(frozen=True)
class _IncomingImage:
    telegram_file_id: str
    telegram_file_unique_id: str
    mime_type: str
    file_size: int
    source: str

DEFAULT_EXPENSE_CATEGORIES = [
    "Продукти",
    "Кафе/ресторани",
    "Транспорт",
    "Таксі",
    "Побут",
    "Здоровʼя",
    "Одяг",
    "Підписки",
    SYSTEM_EXPENSE_CATEGORY_NAME,
]
DEFAULT_INCOME_CATEGORIES = [
    "Зарплата",
    "Фріланс",
    "Подарунок",
    "Кешбек",
    SYSTEM_INCOME_CATEGORY_NAME,
]
MIN_EXPENSE_CATEGORIES = DEFAULT_EXPENSE_CATEGORIES[:5]
MIN_INCOME_CATEGORIES = DEFAULT_INCOME_CATEGORIES[:3]
DEFAULT_EXPENSE_CATEGORIES = CATALOG_DEFAULT_EXPENSE_CATEGORIES
TX_FLOW_STEP_CONFIRMATION = "confirmation"
TX_FLOW_STEP_COMMITTING = "committing"


def _is_transaction_commit_success(status: str | None) -> bool:
    return str(status or "") in SUCCESS_TRANSACTION_STATUSES


def _is_transfer_commit_success(status: str | None) -> bool:
    return str(status or "") in SUCCESS_TRANSFER_STATUSES


def _credit_limit_required_text(label: str, projected_balance: Decimal, currency: str) -> str:
    return "\n".join(
        [
            "<b>⚠️ Рахунок перейде в кредитний режим</b>",
            "",
            f"<b>Рахунок:</b> {escape_html(label)}",
            f"<b>Після операції:</b> {format_money(projected_balance, currency)}",
            f"<b>Мінімальний ліміт:</b> {format_money(abs(projected_balance), currency)}",
            "",
            "Введіть кредитний ліміт для цього рахунку. Після цього я одразу завершу операцію.",
        ]
    )


def _credit_limit_exceeded_text(label: str, projected_balance: Decimal, credit_limit: Decimal | None, currency: str) -> str:
    limit_text = format_money(credit_limit, currency) if credit_limit is not None else "не вказано"
    return "\n".join(
        [
            "<b>❌ Перевищено кредитний ліміт</b>",
            "",
            f"<b>Рахунок:</b> {escape_html(label)}",
            f"<b>Після операції було б:</b> {format_money(projected_balance, currency)}",
            f"<b>Ліміт:</b> {limit_text}",
            "",
            "Зменште суму або збільште кредитний ліміт у налаштуваннях рахунку.",
        ]
    )


def _account_transition_notice(
    *,
    status: str | None,
    account_label: str,
    account_type: str | None,
    balance: Decimal | None,
    currency: str,
) -> str:
    normalized_status = str(status or "")
    if normalized_status == "account_switched_to_credit":
        balance_text = format_money(balance, currency) if balance is not None else f"0 {currency}"
        return (
            "\n\n<b>⚠️ Рахунок переведено в кредитний режим</b>\n"
            f"{escape_html(account_label)} тепер у мінусі: <b>{balance_text}</b>."
        )
    if normalized_status == "account_restored_from_credit":
        type_label = account_type_label(str(account_type or default_non_negative_account_type("main")))
        balance_text = format_money(balance, currency) if balance is not None else f"0 {currency}"
        return (
            "\n\n<b>✅ Рахунок вийшов з кредитного режиму</b>\n"
            f"{escape_html(account_label)} знову рахується як <b>{escape_html(type_label)}</b>. Поточний баланс: <b>{balance_text}</b>."
        )
    return ""


def _transfer_transition_notice(result) -> str:
    if result is None:
        return ""
    status = str(getattr(result, "status", "") or "")
    if status == "account_switched_to_credit" and getattr(result, "source_account", None) is not None:
        account = result.source_account
        return _account_transition_notice(
            status=status,
            account_label=str(account.get("label") or "—"),
            account_type=str(result.source_new_account_type or account.get("account_type") or "credit"),
            balance=result.source_new_balance,
            currency=normalize_currency(str(account.get("currency") or "UAH")),
        )
    if status != "account_restored_from_credit":
        return ""

    source_restored = (
        normalize_account_type(str(getattr(result, "source_previous_account_type", "") or ""))
        == CREDIT_ACCOUNT_TYPE
        and normalize_account_type(str(getattr(result, "source_new_account_type", "") or "")) != CREDIT_ACCOUNT_TYPE
    )
    if source_restored and getattr(result, "source_account", None) is not None:
        account = result.source_account
        return _account_transition_notice(
            status=status,
            account_label=str(account.get("label") or "—"),
            account_type=str(result.source_new_account_type or account.get("account_type") or "main"),
            balance=result.source_new_balance,
            currency=normalize_currency(str(account.get("currency") or "UAH")),
        )

    target_restored = (
        normalize_account_type(str(getattr(result, "target_previous_account_type", "") or ""))
        == CREDIT_ACCOUNT_TYPE
        and normalize_account_type(str(getattr(result, "target_new_account_type", "") or "")) != CREDIT_ACCOUNT_TYPE
    )
    if target_restored and getattr(result, "target_account", None) is not None:
        account = result.target_account
        return _account_transition_notice(
            status=status,
            account_label=str(account.get("label") or "—"),
            account_type=str(result.target_new_account_type or account.get("account_type") or "main"),
            balance=result.target_new_balance,
            currency=normalize_currency(str(account.get("currency") or "UAH")),
        )
    return ""

DEBT_FLOW_STEP_CHOOSING_DIRECTION = "choosing_direction"
DEBT_FLOW_STEP_ENTERING_COUNTERPARTY = "entering_counterparty"
DEBT_FLOW_STEP_ENTERING_AMOUNT = "entering_amount"
DEBT_FLOW_STEP_CHOOSING_CURRENCY = "choosing_currency"
DEBT_FLOW_STEP_CHOOSING_ACCOUNT = "choosing_account"
DEBT_FLOW_STEP_CHOOSING_FX_MODE = "choosing_fx_mode"
DEBT_FLOW_STEP_ENTERING_FX_VALUE = "entering_fx_value"
DEBT_FLOW_STEP_ENTERING_DUE_DATE = "entering_due_date"
DEBT_FLOW_STEP_ENTERING_COMMENT = "entering_comment"
DEBT_FLOW_STEP_CONFIRMING_DEBT = "confirming_debt"
DEBT_FLOW_STEP_CHOOSING_REPAYMENT_TYPE = "choosing_repayment_type"
DEBT_FLOW_STEP_CHOOSING_DEBT_FOR_REPAYMENT = "choosing_debt_for_repayment"
DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT = "entering_repayment_amount"
DEBT_FLOW_STEP_CHOOSING_REPAYMENT_ACCOUNT = "choosing_repayment_account"
DEBT_FLOW_STEP_CONFIRMING_REPAYMENT = "confirming_repayment"
DEBT_FLOW_STEP_EDIT_SELECT = "edit_select"
DEBT_FLOW_STEP_EDIT_VALUE = "edit_value"
DEBT_FLOW_STEP_CONFIRM_DELETE = "confirm_delete"
DEBT_FLOW_STEP_CONFIRMING_OVER_LIMIT = "confirming_over_limit"


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
        await bootstrap_async(conn)
        await conn.execute(
            """
            UPDATE accounts
            SET non_negative_account_type = CASE
              WHEN account_type = 'credit' THEN COALESCE(NULLIF(non_negative_account_type, ''), 'main')
              ELSE COALESCE(NULLIF(non_negative_account_type, ''), account_type, 'main')
            END
            WHERE non_negative_account_type IS NULL OR non_negative_account_type = ''
            """
        )
        await conn.execute(
            """
            UPDATE accounts
            SET account_type = CASE
              WHEN balance < 0 THEN 'credit'
              ELSE COALESCE(NULLIF(non_negative_account_type, ''), account_type, 'main')
            END
            """
        )
        await conn.execute("UPDATE categories SET user_id=tg_user_id WHERE user_id IS NULL")
        await conn.execute("UPDATE categories SET type=kind WHERE type IS NULL")
        await conn.execute(
            "UPDATE categories SET created_by_user_id=COALESCE(created_by_user_id, user_id, tg_user_id) WHERE created_by_user_id IS NULL"
        )
        await conn.execute(
            "UPDATE transactions SET created_by_user_id=COALESCE(created_by_user_id, tg_user_id) WHERE created_by_user_id IS NULL"
        )
        await conn.execute(
            """
            UPDATE debts AS d
            SET family_id = t.family_id,
                updated_at = now()
            FROM transactions AS t
            WHERE t.debt_id = d.id
              AND t.family_id IS NOT NULL
              AND d.family_id IS NULL
            """
        )
        await conn.execute(
            """
            UPDATE debt_payments AS dp
            SET family_id = d.family_id
            FROM debts AS d
            WHERE d.id = dp.debt_id
              AND d.family_id IS NOT NULL
              AND dp.family_id IS NULL
            """
        )
        repaired_debts = await DebtService(conn).repair_legacy_debts()
        if repaired_debts:
            logger.info("Legacy debt repair migrated %s debt groups", repaired_debts)
        await _recalculate_account_balances(conn)
        await conn.execute(
            """
            INSERT INTO bot_settings (key, value, description, value_type)
            VALUES
              ('base_currency', 'UAH', 'Default bot currency', 'string'),
              ('trial_duration_days', '7', 'Default trial duration', 'int'),
              ('registration_enabled', 'true', 'Allow new users to register', 'bool'),
              ('maintenance_mode', 'false', 'Enable maintenance mode', 'bool'),
              ('maintenance_message', 'Бот тимчасово на технічних роботах. Спробуйте пізніше.', 'Maintenance message', 'string'),
              ('support_contact_text', 'Опиши, що сталося, одним повідомленням.', 'Support prompt', 'string'),
              ('daily_expense_reminder_timezone', 'Europe/Kyiv', 'Shared timezone for evening expense reminders', 'string')
            ON CONFLICT (key) DO NOTHING
            """
        )
        await _seed_category_templates(conn)

    application.bot_data["db_pool"] = pool
    if application.job_queue is not None:
        application.job_queue.run_repeating(_send_due_saving_reminders, interval=600, first=60, name="saving-reminders")
        application.job_queue.run_repeating(_send_due_debt_reminders, interval=3600, first=120, name="debt-reminders")
        application.job_queue.run_repeating(_send_daily_expense_reminders, interval=600, first=180, name="daily-expense-reminders")
        application.job_queue.run_repeating(_send_pwa_install_reminders, interval=600, first=240, name="pwa-install-reminders")
    logger.info("DB connected")


async def _seed_category_templates(conn: asyncpg.Connection) -> None:
    expense_names = list(DEFAULT_EXPENSE_CATEGORIES)
    # Ensure system categories exist in templates even if lists change.
    if SYSTEM_EXPENSE_CATEGORY_NAME not in expense_names:
        expense_names.append(SYSTEM_EXPENSE_CATEGORY_NAME)

    for idx, name in enumerate(expense_names):
        is_system = name == SYSTEM_EXPENSE_CATEGORY_NAME
        await conn.execute(
            """
            INSERT INTO category_templates (type, name, aliases, sort_order, is_system, is_active, updated_at)
            VALUES ('expense', $1, $2, $3, $4, true, now())
            ON CONFLICT (type, name) DO UPDATE SET
              sort_order = EXCLUDED.sort_order,
              is_system = EXCLUDED.is_system,
              is_active = true,
              updated_at = now();
            """,
            name,
            [],
            idx,
            is_system,
        )

    await CategoryTemplateService(conn).syncDefaultIncomeTemplates()


async def _register_bot_commands(application: Application) -> None:
    bot = getattr(application, "bot", None)
    if bot is None or not hasattr(bot, "set_my_commands"):
        return
    try:
        include_miniapp = bool(_miniapp_url())
        await bot.set_my_commands(_localized_bot_commands("uk", include_miniapp=include_miniapp))
        try:
            await bot.set_my_commands(_localized_bot_commands("uk", include_miniapp=include_miniapp), language_code="uk")
            await bot.set_my_commands(_localized_bot_commands("en", include_miniapp=include_miniapp), language_code="en")
        except TypeError:
            logger.warning("Telegram bot client does not support locale-specific commands")
    except Exception:
        logger.exception("Failed to register Telegram bot commands")


async def _register_chat_menu_button(application: Application) -> None:
    bot = getattr(application, "bot", None)
    if bot is None or not hasattr(bot, "set_chat_menu_button"):
        return
    try:
        url = _miniapp_url()
        operator_url = _miniapp_operator_url()
        if operator_url and WebAppInfo is not None:
            for tg_user_id in getattr(config, "MINIAPP_OPERATOR_TELEGRAM_IDS", []):
                try:
                    operator_button = MenuButtonWebApp(text="Vydno Admin", web_app=WebAppInfo(url=operator_url))
                    await bot.set_chat_menu_button(chat_id=int(tg_user_id), menu_button=operator_button)
                except Exception:
                    logger.exception("Failed to register operator Mini App menu for %s", tg_user_id)
        if url and WebAppInfo is not None:
            try:
                menu_button = MenuButtonWebApp(text="Vydno.Capital", web_app=WebAppInfo(url=url))
            except TypeError:
                menu_button = MenuButtonCommands()
        else:
            menu_button = MenuButtonCommands()
        await bot.set_chat_menu_button(menu_button=menu_button)
    except Exception:
        logger.exception("Failed to register Telegram chat menu button")


async def post_init(application: Application) -> None:
    await init_db(application)
    await _register_bot_commands(application)
    await _register_chat_menu_button(application)


async def shutdown_db(application: Application) -> None:
    pool = application.bot_data.get("db_pool")
    if pool is not None:
        await pool.close()
        logger.info("DB pool closed")


def _pool(context: ContextTypes.DEFAULT_TYPE) -> asyncpg.Pool:
    return context.application.bot_data["db_pool"]


async def _send_due_saving_reminders(job_context) -> None:
    pool = job_context.application.bot_data.get("db_pool")
    if pool is None:
        return
    async with pool.acquire() as conn:
        due_tasks = await SavingsService(conn).list_due_reminders(datetime.now(), limit=50)
        for task in due_tasks:
            text = await _get_bot_copy(
                conn,
                "saving_reminder_text",
                amount_text=format_money(Decimal(str(task["amount"] or 0)), str(task["currency"] or "UAH")),
                target_label=escape_html(str(task["target_label"] or "—")),
            )
            reminder_key = task.get("remind_at") or task.get("created_at") or datetime.now()
            pushed = await dispatch_push_event(
                user_id=int(task["tg_user_id"]),
                event_type="saving_due",
                idempotency_key=f"saving:{int(task['id'])}:{reminder_key.isoformat()}",
                context={"saving_task_id": int(task["id"])},
            )
            if not pushed:
                await job_context.bot.send_message(
                    chat_id=int(task["tg_user_id"]),
                    text=text,
                    reply_markup=kb_saving_reminder_actions(int(task["id"])),
                    parse_mode=ParseMode.HTML,
                )
            await SavingsService(conn).mark_task_reminded(int(task["id"]))


async def _send_daily_expense_reminders(job_context) -> None:
    pool = job_context.application.bot_data.get("db_pool")
    if pool is None:
        return
    async with pool.acquire() as conn:
        reminder_zone, _timezone_name = await _get_daily_expense_reminder_zone(conn)
        now_local = datetime.now(reminder_zone)
        due_users = await ExpenseReminderService(conn).list_due_reminders(
            now_local,
            min_onboarding_version=CURRENT_ONBOARDING_VERSION,
            limit=50,
        )
        for candidate in due_users:
            text = await _get_bot_copy(conn, candidate.copy_key)
            try:
                pushed = await dispatch_push_event(
                    user_id=int(candidate.tg_user_id),
                    event_type="daily_expense_missing",
                    idempotency_key=f"daily-expense:{candidate.day.isoformat()}",
                    context={"day": candidate.day.isoformat()},
                )
                if not pushed:
                    await job_context.bot.send_message(
                        chat_id=int(candidate.tg_user_id),
                        text=text,
                        reply_markup=kb_expense_reminder_no_expenses(candidate.day.isoformat()),
                        parse_mode=ParseMode.HTML,
                    )
            except Exception as exc:
                logger.warning("Failed to send daily expense reminder to %s: %s", candidate.tg_user_id, exc)
                continue
            await ExpenseReminderService(conn).mark_reminder_sent(
                int(candidate.tg_user_id),
                candidate.day,
                copy_key=candidate.copy_key,
            )


async def _send_pwa_install_reminders(job_context) -> None:
    pool = job_context.application.bot_data.get("db_pool")
    if pool is None:
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT s.id, s.tg_user_id, s.telegram_reminder_count, u.lang
            FROM miniapp_install_nudge_states s
            JOIN users u ON u.tg_user_id = s.tg_user_id
            WHERE s.installed_at IS NULL
              AND s.telegram_reminder_count < 2
              AND s.next_telegram_reminder_at IS NOT NULL
              AND s.next_telegram_reminder_at <= now()
              AND u.onboarding_completed = TRUE
              AND u.onboarding_version >= $1
            ORDER BY s.next_telegram_reminder_at, s.id
            LIMIT 50
            """,
            CURRENT_ONBOARDING_VERSION,
        )
        for row in rows:
            user_id = int(row["tg_user_id"])
            reminder_count = int(row["telegram_reminder_count"] or 0)
            is_english = str(row["lang"] or "").strip().lower().startswith("en")
            browser_login_url = _miniapp_browser_login_url(user_id)
            if browser_login_url:
                browser_login_url = f"{browser_login_url}?install=1"
            if reminder_count == 0:
                text = (
                    "<b>Add Vydno to your Home Screen</b>\n\nOpen the secure link in Safari or Chrome and install the app. Then your finance dashboard is always one tap away."
                    if is_english
                    else "<b>Додайте Vydno на головний екран</b>\n\nВідкрийте захищене посилання в Safari або Chrome та встановіть додаток. Тоді фінансовий кабінет завжди буде в одному тапі."
                )
            else:
                text = (
                    "<b>Final reminder about the Vydno app</b>\n\nAdd it to your Home Screen now so you don't have to search for the Telegram chat later."
                    if is_english
                    else "<b>Останнє нагадування про додаток Vydno</b>\n\nДодайте його на головний екран зараз, щоб потім не шукати чат у Telegram."
                )
            reply_markup = None
            if browser_login_url:
                reply_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("📲 Install Vydno" if is_english else "📲 Встановити Vydno", url=browser_login_url)]]
                )
            try:
                await job_context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML,
                )
            except Exception as exc:
                logger.warning("Failed to send PWA install reminder to %s: %s", user_id, exc)
                await conn.execute(
                    """
                    UPDATE miniapp_install_nudge_states
                    SET next_telegram_reminder_at = now() + interval '12 hours', updated_at = now()
                    WHERE id = $1 AND installed_at IS NULL
                    """,
                    int(row["id"]),
                )
                continue
            await conn.execute(
                """
                UPDATE miniapp_install_nudge_states
                SET telegram_reminder_count = telegram_reminder_count + 1,
                    last_telegram_reminder_at = now(),
                    next_telegram_reminder_at = CASE
                      WHEN telegram_reminder_count + 1 < 2 THEN now() + interval '3 days'
                      ELSE NULL
                    END,
                    updated_at = now()
                WHERE id = $1 AND installed_at IS NULL
                """,
                int(row["id"]),
            )


def _display_user_name(first_name: object | None, username: object | None, fallback: str = "Користувач") -> str:
    first = str(first_name or "").strip()
    if first:
        return first
    username_text = str(username or "").strip().lstrip("@")
    if username_text:
        return f"@{username_text}"
    return fallback


def _debt_comment_text(value: object | None) -> str:
    text = str(value or "").strip()
    return text or "—"


def _build_debt_invite_link(bot_username: str, token: str) -> str:
    return f"https://t.me/{bot_username}?start=debt_{token}"


def _build_telegram_share_url(text: str) -> str:
    return f"https://t.me/share/url?text={quote(text, safe='')}"


async def _build_debt_invite_post_create_text(conn: asyncpg.Connection, debt: asyncpg.Record | dict, *, html: bool = True) -> str:
    borrower_name = _debt_comment_text(debt.get("counterparty_name"))
    comment = _debt_comment_text(debt.get("comment"))
    currency = str(debt.get("currency") or "UAH")
    if html:
        borrower_name = escape_html(borrower_name)
        comment = escape_html(comment)
    return await _get_bot_copy(
        conn,
        "debt_invite_post_create_text",
        borrower_name=borrower_name,
        initial_amount=format_money(Decimal(str(debt.get("initial_amount") or 0)), currency),
        paid_amount=format_money(Decimal(str(debt.get("paid_amount") or 0)), currency),
        remaining_amount=format_money(Decimal(str(debt.get("remaining_amount") or 0)), currency),
        comment=comment,
    )


async def _build_debt_invite_owner_message(
    conn: asyncpg.Connection,
    owner_name: str,
    debt: asyncpg.Record | dict,
    link: str,
    *,
    html: bool = True,
) -> str:
    borrower_name = _debt_comment_text(debt.get("counterparty_name"))
    comment = _debt_comment_text(debt.get("comment"))
    safe_owner_name = escape_html(owner_name) if html else owner_name
    safe_borrower_name = escape_html(borrower_name) if html else borrower_name
    safe_comment = escape_html(comment) if html else comment
    safe_link = escape_html(link) if html else link
    currency = str(debt.get("currency") or "UAH")
    return await _get_bot_copy(
        conn,
        "debt_invite_owner_message_text",
        owner_name=safe_owner_name,
        borrower_name=safe_borrower_name,
        initial_amount=format_money(Decimal(str(debt.get("initial_amount") or 0)), currency),
        comment=safe_comment,
        link=safe_link,
    )


async def _build_debt_invite_share_message(conn: asyncpg.Connection, owner_name: str, debt: asyncpg.Record | dict, link: str) -> str:
    borrower_name = _debt_comment_text(debt.get("counterparty_name"))
    comment = _debt_comment_text(debt.get("comment"))
    currency = str(debt.get("currency") or "UAH")
    return await _get_bot_copy(
        conn,
        "debt_invite_share_message_text",
        owner_name=owner_name,
        borrower_name=borrower_name,
        initial_amount=format_money(Decimal(str(debt.get("initial_amount") or 0)), currency),
        comment=comment,
        link=link,
    )


def _generate_debt_invite_token() -> str:
    return secrets.token_urlsafe(18)


def _build_family_invite_link(bot_username: str, token: str) -> str:
    return f"https://t.me/{bot_username}?start=family_invite_{token}"


def _family_member_label(member: asyncpg.Record) -> str:
    role = str(member.get("role") or "member")
    name = _display_user_name(member.get("first_name"), member.get("username"))
    return f"👑 {name}" if role == "owner" else f"• {name}"


def _extract_start_payload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    args = list(getattr(context, "args", []) or [])
    if args:
        return str(args[0] or "").strip()
    message = update.message
    if message and message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            return parts[1].strip()
    return ""


def _marketing_start_payload_state(start_payload: str) -> dict[str, str] | None:
    payload = str(start_payload or "").strip()
    if not payload:
        return None
    if payload.startswith("promo_"):
        promo_code = payload.removeprefix("promo_").strip().upper() or payload
        return {
            "access_source": "promo",
            "pending_start_payload": payload,
            "source": "promo",
            "referral_code": promo_code,
        }
    if payload == "course" or payload.startswith("course_"):
        referral_code = payload.removeprefix("course_").strip() if payload.startswith("course_") else ""
        return {
            "access_source": "promo",
            "pending_start_payload": payload,
            "source": "course",
            "referral_code": referral_code or "course",
        }
    if payload == "treads":
        return {
            "access_source": "promo",
            "pending_start_payload": payload,
            "source": "treads",
            "referral_code": "treads",
        }
    return None


async def _resolve_bot_username(context: ContextTypes.DEFAULT_TYPE) -> str | None:
    bot = getattr(context, "bot", None) or getattr(context.application, "bot", None)
    username = getattr(bot, "username", None)
    if username:
        return str(username)
    if bot is None or not hasattr(bot, "get_me"):
        return None
    me = await bot.get_me()
    return str(getattr(me, "username", "") or "") or None


async def _show_family_members_menu(message, user_id: int, conn: asyncpg.Connection) -> None:
    service = FamilyService(conn)
    scope = await service.get_scope(user_id)
    members = await service.list_family_members(user_id)
    if not scope.is_family:
        await message.reply_text(
            await _get_bot_copy(conn, "family_empty_text"),
            reply_markup=kb_family_empty(),
        )
        return
    items = [
        (
            int(member["user_id"]),
            _family_member_label(member),
            str(member["role"] or "member"),
            str(member["status"] or "") == "active",
        )
        for member in members
    ]
    await message.reply_text(
        "<b>👥 Учасники родини</b>\n\nОберіть учасника для дії." if scope.is_owner else "<b>👥 Учасники родини</b>",
        reply_markup=kb_family_members(scope.is_owner, items),
    )


async def _show_family_menu(message, context: ContextTypes.DEFAULT_TYPE, user_id: int, conn: asyncpg.Connection) -> None:
    if not config.FAMILY_ACCESS_ENABLED:
        await message.reply_text(
            await _get_bot_copy(conn, "family_disabled_text"),
            reply_markup=_kb_home_only(),
        )
        return

    service = FamilyService(conn)
    scope = await service.get_scope(user_id)
    if not scope.is_family:
        await message.reply_text(
            await _get_bot_copy(conn, "family_empty_text"),
            reply_markup=kb_family_empty(),
        )
        return

    family = await service.get_family_overview(user_id)
    members = await service.list_family_members(user_id)
    active_members_count = int(family["active_members_count"] or 0) if family is not None else 0
    member_lines = [_family_member_label(member) for member in members if str(member["status"] or "") == "active"]
    if scope.is_owner:
        invites = await service.list_active_invites(user_id)
        await message.reply_text(
            "\n".join(
                [
                    f"<b>👨‍👩‍👧 {escape_html(str(family['name'] or 'Сімейний бюджет'))}</b>",
                    "",
                    f"<b>Учасників:</b> {active_members_count}/4",
                    "<b>Склад:</b>",
                    *member_lines,
                ]
            ),
            reply_markup=kb_family_owner_menu(has_active_invite=bool(invites), active_members_count=active_members_count),
        )
        return

    await message.reply_text(
        "\n".join(
            [
                f"<b>👨‍👩‍👧 {escape_html(str(family['name'] or 'Сімейний бюджет'))}</b>",
                "",
                f"<b>Учасників:</b> {active_members_count}/4",
                "<b>Склад:</b>",
                *member_lines,
            ]
        ),
        reply_markup=kb_family_member_menu(),
    )


async def _handle_family_invite_start(update: Update, context: ContextTypes.DEFAULT_TYPE, token: str) -> int:
    message = update.message
    user = update.effective_user
    if not message or not user:
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        await _present_family_invite_prompt(
            message,
            conn,
            user.id,
            token,
            remember_pending=True,
            silent_invalid=False,
        )
    return ConversationHandler.END


async def _send_due_debt_reminders(job_context) -> None:
    pool = job_context.application.bot_data.get("db_pool")
    if pool is None:
        return

    async with pool.acquire() as conn:
        service = DebtService(conn)
        due_debts = await service.list_due_debt_reminders(datetime.now(), limit=50)
        for debt in due_debts:
            lender_name = _display_user_name(debt.get("lender_first_name"), debt.get("lender_username"))
            lender_name_html = escape_html(lender_name)
            currency = str(debt["currency"] or "UAH")
            text = await _get_bot_copy(
                conn,
                "debt_monthly_reminder_text",
                lender_name=lender_name_html,
                initial_amount=format_money(Decimal(str(debt["initial_amount"] or 0)), currency),
                paid_amount=format_money(Decimal(str(debt["paid_amount"] or 0)), currency),
                remaining_amount=format_money(Decimal(str(debt["remaining_amount"] or 0)), currency),
                comment=escape_html(_debt_comment_text(debt.get("comment"))),
            )
            sent_at = datetime.now()
            try:
                reminder_key = debt.get("next_reminder_at") or sent_at
                pushed = await dispatch_push_event(
                    user_id=int(debt["borrower_user_id"]),
                    event_type="debt_monthly",
                    idempotency_key=f"debt-monthly:{int(debt['id'])}:{reminder_key.isoformat()}",
                    context={"debt_id": int(debt["id"])},
                )
                if not pushed:
                    await job_context.bot.send_message(
                        chat_id=int(debt["borrower_user_id"]),
                        text=text,
                        parse_mode=ParseMode.HTML,
                    )
            except Exception as exc:
                logger.exception("Debt reminder delivery failed for debt_id=%s", debt["id"])
                await service.mark_debt_reminder_failed(int(debt["id"]), str(exc))
                continue
            await service.mark_debt_reminder_sent(int(debt["id"]), sent_at)


def _parse_date_ddmmyyyy(text: str) -> date | None:
    value = (text or "").strip().lower()
    if not value:
        return None
    if value in {"сьогодні", "сегодня", "today"}:
        return datetime.now().date()
    if value in {"вчора", "вчера", "yesterday"}:
        return datetime.now().date() - timedelta(days=1)
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d", "%d.%m", "%d/%m/%Y", "%d/%m/%y", "%d/%m"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt in {"%d.%m", "%d/%m"}:
                parsed = parsed.replace(year=datetime.now().year)
            return parsed.date()
        except ValueError:
            continue
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


def _reset_ai_tx_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("ai_tx_flow", None)


def _reset_ai_batch_tx_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("ai_batch_tx_flow", None)


def _reset_currency_resolution_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("currency_resolution_flow", None)


def _reset_transfer_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("transfer_flow", None)


def _reset_saving_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("saving_flow", None)


def _reset_saving_settings_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("saving_settings_flow", None)


def _reset_settings_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("settings_flow", None)


def _reset_debt_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("debt_flow", None)


def _reset_categories_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("categories_flow", None)


def _reset_accounts_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("accounts_flow", None)


class CategoryEditStates:
    WAITING_FOR_ADD_NAME = "waiting_for_add_name"
    WAITING_FOR_RENAME_NAME = "waiting_for_rename_name"
    WAITING_FOR_ALIASES = "waiting_for_aliases"


CATEGORY_EDIT_PAGE_SIZE = 8


def _normalize_category_name_input(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\u00A0", " ")).strip()


def _category_name_error(name: str) -> str | None:
    normalized = _normalize_category_name_input(name)
    if not normalized:
        return "Назва має містити від 2 до 40 символів."
    if len(normalized) < 2 or len(normalized) > 40:
        return "Назва має містити від 2 до 40 символів."
    if re.search(r"[\x00-\x1f\x7f]", normalized):
        return "Назва містить недопустимі символи. Введіть іншу назву."
    return None


def _category_type_label(type_: str) -> str:
    return "витрат" if type_ == "expense" else "доходів"


def _category_type_title(type_: str) -> str:
    return "витрати" if type_ == "expense" else "доходи"


def _category_type_accusative(type_: str) -> str:
    return "категорію витрат" if type_ == "expense" else "категорію доходів"


def _category_row_label(category: Category) -> str:
    if category.is_system:
        return "стандартна"
    if str(category.source or "").lower() == "template":
        return "стандартна"
    if category.is_active:
        return "кастомна"
    return "архівована"


def _category_mark(category: Category) -> str:
    if category.is_system:
        return "⭐"
    if not category.is_active:
        return "⏸"
    return "👤"


def _category_list_text(type_: str, categories: list[Category]) -> str:
    lines = [f"Категорії {_category_type_label(type_)}:"]
    lines.append("")
    if categories:
        for index, category in enumerate(categories, 1):
            lines.append(f"{index}. {escape_html(category.name)} [{_category_row_label(category)}]")
    else:
        lines.append("<i>Немає активних категорій.</i>")
    lines.append("")
    lines.append("Можете додати нову або відредагувати існуючу.")
    return "\n".join(lines)


def _category_edit_list_text(type_: str) -> str:
    return f"Оберіть {_category_type_accusative(type_)}, яку хочете змінити:"


def _category_empty_text(type_: str) -> str:
    return f"У тебе немає активних категорій {_category_type_label(type_)}."


def _categories_mine_text(expense: list[Category], income: list[Category]) -> str:
    lines = ["Мої категорії:", ""]
    rows: list[str] = []
    for category in expense + income:
        if category.source == "custom" and category.is_active:
            rows.append(f"• {escape_html(category.name)} ({_category_type_title(category.type)})")
    if rows:
        lines.extend(rows)
    else:
        lines.append("<i>Поки немає власних категорій.</i>")
    return "\n".join(lines)


def _category_detail_text(category: Category) -> str:
    return (
        f"Категорія: {escape_html(category.name)}\n"
        f"Тип: {_category_type_title(category.type)}\n\n"
        "Що хочете зробити?"
    )


def _category_missing_text() -> str:
    return "Категорію не знайдено або вона вже недоступна."


def _category_rename_prompt(category: Category) -> str:
    return f'Введіть нову назву для категорії "{escape_html(category.name)}".'


def _category_delete_confirm_text(category: Category) -> str:
    return (
        f'Категорія "{escape_html(category.name)}" зникне з нових операцій.\n\n'
        "Старі операції залишаться без змін.\n\n"
        "Видалити категорію?"
    )


def _paginate_items(items: list, page: int, *, page_size: int = CATEGORY_EDIT_PAGE_SIZE) -> tuple[list, int, int]:
    if page_size <= 0:
        page_size = CATEGORY_EDIT_PAGE_SIZE
    total_pages = max(1, (len(items) + page_size - 1) // page_size)
    safe_page = min(max(page, 0), total_pages - 1)
    start = safe_page * page_size
    end = start + page_size
    return items[start:end], safe_page, total_pages


EDIT_START_CALLBACKS = {
    "categories:edit",
    "categories:edit:start",
    "categories:edit_menu",
    "categories:edit:menu",
}


def _normalize_categories_callback(data: str) -> str:
    if data in EDIT_START_CALLBACKS:
        return "categories:edit:start"
    if data == "categories:add":
        return "categories:add:start"
    if data in {"categories:add:expense", "categories:add:income"}:
        return f"categories:add:type:{data.rsplit(':', 1)[-1]}"
    if data.startswith("categories:edit:list:"):
        return data.replace("categories:edit:list:", "categories:edit:type:", 1)
    if data.startswith("categories:rename:") and not data.startswith("categories:rename:start:"):
        return data.replace("categories:rename:", "categories:rename:start:", 1)
    if data.startswith("categories:delete_confirm:"):
        return data.replace("categories:delete_confirm:", "categories:delete:confirm:", 1)
    if data.startswith("categories:delete:") and not data.startswith("categories:delete:start:") and not data.startswith("categories:delete:confirm:"):
        return data.replace("categories:delete:", "categories:delete:start:", 1)
    if data.startswith("categories:archive:confirm:"):
        return data.replace("categories:archive:confirm:", "categories:delete:confirm:", 1)
    if data.startswith("categories:archive:"):
        return data.replace("categories:archive:", "categories:delete:start:", 1)
    return data


def _category_dispatch_action(data: str) -> str:
    if data in EDIT_START_CALLBACKS or data == "categories:edit:start":
        return "show_categories_edit_type_picker"
    if data.startswith("categories:edit:type:") or data.startswith("categories:edit:page:"):
        return "show_categories_edit_list"
    if data.startswith("categories:edit:item:") or data.startswith("categories:open:"):
        return "show_category_card"
    if data.startswith("categories:rename:start:"):
        return "start_category_rename"
    if data.startswith("categories:delete:start:"):
        return "start_category_delete"
    if data.startswith("categories:delete:confirm:"):
        return "confirm_category_delete"
    if data.startswith("categories:add:"):
        return "categories_add_flow"
    return "categories_dispatch"


def _log_categories_callback_received(q, user_id: int, raw_data: str, normalized_data: str) -> None:
    message = getattr(q, "message", None)
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", "")
    message_id = getattr(message, "message_id", "")
    logger.info(
        "categories callback received user_id=%s chat_id=%s message_id=%s raw_data=%s normalized_data=%s dispatch_action=%s",
        user_id,
        chat_id,
        message_id,
        raw_data,
        normalized_data,
        _category_dispatch_action(normalized_data),
    )


async def _reply_category_edit_list(message, svc: CategoryService, user_id: int, type_: str, *, page: int = 0, notice: str | None = None) -> None:
    categories = await svc.getUserCategories(user_id, type_)
    if not categories:
        text = _category_empty_text(type_)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=categories_empty_edit_keyboard(type_))
        return
    page_items, safe_page, total_pages = _paginate_items(categories, page)
    text = _category_edit_list_text(type_)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(
        text,
        reply_markup=categories_edit_list_keyboard(
            type_,
            [(category.id, category.name) for category in page_items],
            page=safe_page,
            total_pages=total_pages,
        ),
    )


def _log_category_event(action: str, user_id: int, callback_data: str, *, state: str = "", category_id: int | None = None) -> None:
    logger.info(
        "category action=%s user_id=%s callback_data=%s state=%s category_id=%s",
        action,
        user_id,
        callback_data,
        state,
        category_id,
    )


def _reset_support_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("support_flow", None)


def _reset_poll_text_flow(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("poll_text_flow", None)


def _reset_runtime_flows(context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_tx_flow(context)
    _reset_ai_tx_flow(context)
    _reset_ai_batch_tx_flow(context)
    _reset_currency_resolution_flow(context)
    _reset_transfer_flow(context)
    _reset_saving_flow(context)
    _reset_saving_settings_flow(context)
    _reset_settings_flow(context)
    _reset_debt_flow(context)
    _reset_categories_flow(context)
    _reset_accounts_flow(context)
    _reset_support_flow(context)
    _reset_poll_text_flow(context)
    context.user_data.pop("transaction_void_draft", None)
    context.user_data.pop("transaction_void_idempotency_key", None)


def _onboarding_active(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get("onb"))


def _serialized_onboarding_payload(context: ContextTypes.DEFAULT_TYPE) -> dict:
    payload = context.user_data.get("onb") or {}
    try:
        return json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    except TypeError:
        return {}


async def _sync_onboarding_debug(
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    step: str,
    last_user_input: str | None = None,
    last_bot_response: str | None = None,
    last_parse_error: str | None = None,
) -> None:
    async with _pool(context).acquire() as conn:
        await sync_onboarding_debug_state(
            conn,
            tg_user_id,
            current_fsm_state=step,
            onboarding_payload=_serialized_onboarding_payload(context),
            last_user_input=last_user_input,
            last_bot_response=last_bot_response,
            last_parse_error=last_parse_error,
            last_event_type="onboarding_step_completed",
        )


async def _ensure_onboarding_context_loaded(
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
) -> None:
    context.user_data["tg_user_id"] = tg_user_id
    if context.user_data.get("onb"):
        return
    payload: dict[str, Any] = {}
    async with _pool(context).acquire() as conn:
        try:
            row = await conn.fetchrow(
                "SELECT onboarding_payload FROM user_admin_states WHERE telegram_user_id = $1",
                tg_user_id,
            )
        except Exception:
            row = None
    raw_payload = None
    if row is not None:
        try:
            raw_payload = row["onboarding_payload"]
        except Exception:
            try:
                raw_payload = row.get("onboarding_payload")
            except Exception:
                raw_payload = None
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            raw_payload = None
    if isinstance(raw_payload, dict):
        payload = raw_payload
    context.user_data["onb"] = payload


def _onboarding_category_selection(onb_payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = onb_payload or {}
    return {
        "category_mode": str(payload.get("category_mode") or "default"),
        "selected_expense_template_ids": list(payload.get("selected_expense_template_ids") or []),
        "selected_income_template_ids": list(payload.get("selected_income_template_ids") or []),
        "custom_expense_categories": list(payload.get("custom_expense_categories") or []),
        "custom_income_categories": list(payload.get("custom_income_categories") or []),
    }


async def _consume_pending_admin_reset_if_needed(
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
) -> dict[str, str] | None:
    async with _pool(context).acquire() as conn:
        pending_reset = await get_pending_admin_reset(conn, tg_user_id)
        if not pending_reset:
            return None
        _reset_onboarding(context)
        _reset_runtime_flows(context)
        context.user_data.pop("force_onboarding", None)
        await consume_pending_admin_reset(conn, tg_user_id, mode=pending_reset["mode"])
        await sync_onboarding_debug_state(
            conn,
            tg_user_id,
            current_fsm_state="",
            onboarding_payload={},
            last_bot_response=f"Admin reset consumed ({pending_reset['mode']})",
            last_event_type="onboarding_reset_by_admin",
        )
        return pending_reset


async def _get_user(conn: asyncpg.Connection, tg_user_id: int) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM users WHERE tg_user_id=$1", tg_user_id)


def _default_currency_from_user_row(user_row: asyncpg.Record | dict | None) -> str:
    if not user_row:
        return "UAH"
    if isinstance(user_row, dict):
        value = user_row.get("base_currency")
    else:
        value = user_row["base_currency"] if "base_currency" in user_row else None
    return normalize_currency(str(value or "UAH")) or "UAH"


def _normalized_account_currency(account: asyncpg.Record | dict | None, fallback: str = "UAH") -> str:
    if not account:
        return normalize_currency(fallback) or "UAH"
    if isinstance(account, dict):
        value = account.get("currency")
    else:
        value = account["currency"] if "currency" in account else None
    return normalize_currency(str(value or fallback)) or "UAH"


def _currency_account_mismatch_text(*, account_label: str, tx_currency: str, account_currency: str) -> str:
    return (
        "<b>⚠️ Валюта не збігається з рахунком</b>\n\n"
        f"<b>У повідомленні:</b> {escape_html(tx_currency)}\n"
        f"<b>Рахунок {escape_html(account_label or '—')}:</b> {escape_html(account_currency)}\n\n"
        "Оберіть рахунок у потрібній валюті або введіть суму без валюти, якщо хочете записати операцію у валюті цього рахунку."
    )


def _batch_currency_account_mismatch_text(*, account_currency: str, item_indexes: list[int]) -> str:
    positions = ", ".join(str(index) for index in item_indexes)
    return (
        "<b>⚠️ Валюта пакета не збігається з рахунком</b>\n\n"
        f"<b>Рахунок:</b> {escape_html(account_currency)}\n"
        f"<b>Позиції з іншою валютою:</b> {escape_html(positions)}\n\n"
        "Оберіть рахунок у потрібній валюті або виправте суми без явної валюти для цих позицій."
    )


def _currency_resolution_text(currency: str) -> str:
    normalized_currency = normalize_currency(currency) or "UAH"
    return (
        f"Ви вказали <b>{escape_html(normalized_currency)}</b>, "
        f"але активного <b>{escape_html(normalized_currency)}-рахунку</b> немає."
    )


def _currency_resolution_target_prompt(source_currency: str) -> str:
    normalized_currency = normalize_currency(source_currency) or "UAH"
    return (
        "<b>Оберіть рахунок для конвертації</b>\n\n"
        f"У яку валюту конвертувати <b>{escape_html(normalized_currency)}</b>?"
    )


def _currency_resolution_rate_prompt(source_currency: str, target_currency: str) -> str:
    return "<b>Введіть курс для конвертації</b>\n\n" + build_transfer_rate_prompt(source_currency, target_currency)


def _currency_resolution_rate_source_label(rate_source: str) -> str:
    normalized = str(rate_source or "").strip().lower()
    if normalized == "nbu":
        return "НБУ"
    if normalized == "manual":
        return "вручну"
    return normalized or "вказаним курсом"


def _currency_conversion_notice(
    *,
    source_amount: Decimal,
    source_currency: str,
    target_amount: Decimal,
    target_currency: str,
    fx_rate: Decimal,
    rate_source: str,
) -> str:
    return "\n".join(
        [
            "<b>Конвертацію підготовлено</b>",
            "",
            f"<b>Було:</b> {format_money(source_amount, source_currency)}",
            f"<b>Стане:</b> {format_money(target_amount, target_currency)}",
            f"<b>Курс ({escape_html(_currency_resolution_rate_source_label(rate_source))}):</b> {format_exchange_rate(source_currency, target_currency, fx_rate)}",
        ]
    )


def _batch_currency_conversion_notice(
    *,
    converted_items_count: int,
    source_currency: str,
    target_currency: str,
    fx_rate: Decimal,
    rate_source: str,
) -> str:
    items_label = "позицію" if converted_items_count == 1 else "позиції"
    return "\n".join(
        [
            "<b>Конвертацію підготовлено</b>",
            "",
            f"Сконвертовано {converted_items_count} {items_label} з <b>{escape_html(source_currency)}</b> у <b>{escape_html(target_currency)}</b>.",
            f"<b>Курс ({escape_html(_currency_resolution_rate_source_label(rate_source))}):</b> {format_exchange_rate(source_currency, target_currency, fx_rate)}",
        ]
    )


def _build_currency_resolution_flow(
    *,
    mode: str,
    callback_prefix: str,
    source_currency: str,
    target_account_id: int | None = None,
    target_account_label: str | None = None,
    target_currency: str | None = None,
    original_amount: Decimal | None = None,
    original_comment: str | None = None,
) -> dict[str, object]:
    flow: dict[str, object] = {
        "mode": mode,
        "step": "menu",
        "callback_prefix": callback_prefix,
        "source_currency": normalize_currency(source_currency) or "UAH",
        "pending_action": None,
    }
    if target_account_id is not None:
        flow["target_account_id"] = int(target_account_id)
    if target_account_label:
        flow["target_account_label"] = str(target_account_label)
    if target_currency:
        flow["target_currency"] = normalize_currency(target_currency) or "UAH"
    if isinstance(original_amount, Decimal):
        flow["original_amount"] = original_amount
    if original_comment is not None:
        flow["original_comment"] = str(original_comment)
    return flow


def _set_currency_resolution_target(resolution_flow: dict, account: asyncpg.Record | dict) -> dict:
    resolution_flow["target_account_id"] = int(account["id"])
    resolution_flow["target_account_label"] = str(account["label"])
    resolution_flow["target_currency"] = _normalized_account_currency(account)
    return resolution_flow


def _has_active_account_in_currency(accounts_full: list[asyncpg.Record] | list[dict], currency: str) -> bool:
    normalized_currency = normalize_currency(currency) or "UAH"
    return any(_normalized_account_currency(account) == normalized_currency for account in accounts_full)


def _single_active_account_target(accounts_full: list[asyncpg.Record] | list[dict], source_currency: str) -> dict | None:
    if len(accounts_full) != 1:
        return None
    account = accounts_full[0]
    if _normalized_account_currency(account) == (normalize_currency(source_currency) or "UAH"):
        return None
    return {
        "id": int(account["id"]),
        "label": str(account["label"]),
        "currency": _normalized_account_currency(account),
    }


def _ai_batch_explicit_currencies(items: list[dict]) -> set[str]:
    currencies: set[str] = set()
    for item in items:
        if not bool(item.get("currency_explicit")):
            continue
        currencies.add(normalize_currency(str(item.get("currency") or "UAH")) or "UAH")
    return currencies


async def _show_currency_resolution_menu(message, resolution_flow: dict, *, notice: str | None = None) -> None:
    text = _currency_resolution_text(str(resolution_flow.get("source_currency") or "UAH"))
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(
        text,
        reply_markup=kb_currency_resolution(
            str(resolution_flow.get("source_currency") or "UAH"),
            callback_prefix=str(resolution_flow.get("callback_prefix") or "ai:currency"),
        ),
    )


async def _show_currency_resolution_target_picker(
    message,
    resolution_flow: dict,
    accounts_rows: list[tuple[int, str]],
    *,
    notice: str | None = None,
) -> None:
    if not accounts_rows:
        await _show_currency_resolution_menu(
            message,
            resolution_flow,
            notice=notice or "<b>Немає активного рахунку для конвертації.</b>\n\nСпочатку створіть рахунок.",
        )
        return
    text = _currency_resolution_target_prompt(str(resolution_flow.get("source_currency") or "UAH"))
    if notice:
        text = f"{notice}\n\n{text}"
    callback_prefix = str(resolution_flow.get("callback_prefix") or "ai:currency")
    await message.reply_text(
        text,
        reply_markup=kb_ai_pick_account(
            accounts_rows,
            callback_prefix=f"{callback_prefix}:pick:acct",
            back_callback=f"{callback_prefix}:back",
        ),
    )


async def _resolve_nbu_rate(source_currency: str, target_currency: str) -> Decimal:
    normalized_source = normalize_currency(source_currency) or "UAH"
    normalized_target = normalize_currency(target_currency) or normalized_source
    if normalized_source == normalized_target:
        return Decimal("1")
    rates = await get_latest_rates("UAH")
    if normalized_source == "UAH":
        fx_rate = rates.rates[normalized_target]
    elif normalized_target == "UAH":
        fx_rate = rates.rates[normalized_source]
    else:
        fx_rate = rates.rates[normalized_source] / rates.rates[normalized_target]
    return quantize_rate(Decimal(str(fx_rate)))


def _start_tx_currency_resolution(
    context: ContextTypes.DEFAULT_TYPE,
    tx_flow: dict,
    *,
    source_currency: str,
    amount: Decimal,
    comment: str | None,
) -> dict[str, object]:
    kind = str(tx_flow.get("kind") or "expense")
    resolution_flow = _build_currency_resolution_flow(
        mode="tx",
        callback_prefix=f"{kind}:currency",
        source_currency=source_currency,
        target_account_id=int(tx_flow.get("account_id") or 0) or None,
        target_account_label=str(tx_flow.get("account_label") or "") or None,
        target_currency=str(tx_flow.get("currency") or "UAH"),
        original_amount=amount,
        original_comment=comment,
    )
    tx_flow["step"] = "currency_resolution"
    tx_flow.pop("await_amount", None)
    context.user_data["tx_flow"] = tx_flow
    context.user_data["currency_resolution_flow"] = resolution_flow
    return resolution_flow


def _start_ai_tx_currency_resolution(
    context: ContextTypes.DEFAULT_TYPE,
    flow: dict,
    *,
    source_currency: str,
    target_account: asyncpg.Record | dict | None = None,
    fallback_target: dict | None = None,
) -> dict[str, object]:
    callback_prefix = _ai_tx_callback_prefix(flow)
    resolution_flow = _build_currency_resolution_flow(
        mode="ai_tx",
        callback_prefix="ai:currency" if callback_prefix == "ai" else f"{callback_prefix}:currency",
        source_currency=source_currency,
    )
    if target_account is not None:
        _set_currency_resolution_target(resolution_flow, target_account)
    elif fallback_target is not None:
        resolution_flow["target_account_id"] = int(fallback_target["id"])
        resolution_flow["target_account_label"] = str(fallback_target["label"])
        resolution_flow["target_currency"] = normalize_currency(str(fallback_target["currency"])) or "UAH"
    flow.pop("await_amount", None)
    flow.pop("await_field", None)
    context.user_data["ai_tx_flow"] = flow
    context.user_data["currency_resolution_flow"] = resolution_flow
    return resolution_flow


def _start_ai_batch_currency_resolution(
    context: ContextTypes.DEFAULT_TYPE,
    flow: dict,
    *,
    source_currency: str,
    target_account: asyncpg.Record | dict | None = None,
    fallback_target: dict | None = None,
) -> dict[str, object]:
    resolution_flow = _build_currency_resolution_flow(
        mode="ai_batch",
        callback_prefix="ai:batch:currency",
        source_currency=source_currency,
    )
    if target_account is not None:
        _set_currency_resolution_target(resolution_flow, target_account)
    elif fallback_target is not None:
        resolution_flow["target_account_id"] = int(fallback_target["id"])
        resolution_flow["target_account_label"] = str(fallback_target["label"])
        resolution_flow["target_currency"] = normalize_currency(str(fallback_target["currency"])) or "UAH"
    flow.pop("await_field", None)
    flow.pop("edit_item_index", None)
    context.user_data["ai_batch_tx_flow"] = flow
    context.user_data["currency_resolution_flow"] = resolution_flow
    return resolution_flow


async def _start_currency_resolution_account_create(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    resolution_flow: dict,
) -> None:
    context.user_data["settings_flow"] = _new_account_create_flow(
        preferred_account_type="main",
        preferred_currency=str(resolution_flow.get("source_currency") or "UAH"),
        origin="currency_resolution",
        cancel_callback="settings:acct:cancel",
        include_goal_fields=False,
    )
    await message.reply_text(
        _account_create_intro_text("main", include_goal_fields=False),
        reply_markup=kb_inline_cancel("settings:acct:cancel"),
    )


async def _resume_currency_resolution_with_created_account(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    account: asyncpg.Record | dict,
    *,
    notice: str | None = None,
) -> None:
    resolution_flow = context.user_data.get("currency_resolution_flow") or {}
    mode = str(resolution_flow.get("mode") or "")
    account_id = int(account["id"])
    account_label = str(account["label"])
    account_currency = _normalized_account_currency(account)
    _reset_settings_flow(context)

    if mode == "tx":
        tx_flow = context.user_data.get("tx_flow") or {}
        tx_flow["account_id"] = account_id
        tx_flow["account_label"] = account_label
        tx_flow["currency"] = account_currency
        original_amount = resolution_flow.get("original_amount")
        if isinstance(original_amount, Decimal):
            tx_flow["amount"] = original_amount
            tx_flow["comment"] = (str(resolution_flow.get("original_comment") or "")[:500] or None)
            tx_flow["step"] = TX_FLOW_STEP_CONFIRMATION
            tx_flow.pop("await_amount", None)
            context.user_data["tx_flow"] = tx_flow
            _reset_currency_resolution_flow(context)
            text = _tx_confirm_text(tx_flow)
            if notice:
                text = f"{notice}\n\n{text}"
            await message.reply_text(text, reply_markup=_kb_tx_confirm(tx_flow))
            return
        tx_flow["step"] = "await_amount"
        tx_flow["await_amount"] = True
        context.user_data["tx_flow"] = tx_flow
        _reset_currency_resolution_flow(context)
        prompt = _tx_amount_prompt_text(
            str(tx_flow.get("kind") or "expense"),
            account_label,
            str(tx_flow.get("category_label") or "—"),
        )
        if notice:
            prompt = f"{notice}\n\n{prompt}"
        await message.reply_text(
            prompt,
            reply_markup=kb_inline_cancel(f"{str(tx_flow.get('kind') or 'expense')}:cancel"),
        )
        return

    if mode == "ai_tx":
        flow = context.user_data.get("ai_tx_flow") or {}
        tx = _ai_flow_tx(flow)
        tx["account_id"] = account_id
        tx["currency"] = account_currency
        tx["currency_explicit"] = True
        accounts_map = flow.get("accounts")
        if isinstance(accounts_map, dict):
            accounts_map[account_id] = account_label
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, tg_user_id, flow)
        _reset_currency_resolution_flow(context)
        text = _ai_tx_card_text(flow)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=_ai_tx_confirm_markup(flow))
        return

    if mode == "ai_batch":
        flow = context.user_data.get("ai_batch_tx_flow") or {}
        items = _ai_batch_flow_items(flow)
        flow["account_id"] = account_id
        accounts_map = flow.get("accounts")
        if isinstance(accounts_map, dict):
            accounts_map[account_id] = account_label
        for item in items:
            if not bool(item.get("currency_explicit")):
                item["currency"] = account_currency
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, tg_user_id, flow)
        _reset_currency_resolution_flow(context)
        text = _ai_batch_tx_card_text(flow)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return

    _reset_currency_resolution_flow(context)
    await _reply_home(message, notice or "Рахунок створено.")


async def _apply_currency_resolution_conversion(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    fx_rate: Decimal,
    rate_source: str,
) -> None:
    resolution_flow = context.user_data.get("currency_resolution_flow") or {}
    mode = str(resolution_flow.get("mode") or "")
    source_currency = normalize_currency(str(resolution_flow.get("source_currency") or "UAH")) or "UAH"
    target_account_id = int(resolution_flow.get("target_account_id") or 0)
    target_currency = normalize_currency(str(resolution_flow.get("target_currency") or "")) or ""
    target_label = str(resolution_flow.get("target_account_label") or "")
    if not target_account_id or not target_currency:
        await _show_currency_resolution_menu(
            message,
            resolution_flow,
            notice="<b>Спочатку оберіть рахунок для конвертації.</b>",
        )
        return

    if mode == "tx":
        tx_flow = context.user_data.get("tx_flow") or {}
        original_amount = resolution_flow.get("original_amount")
        if not isinstance(original_amount, Decimal):
            await _show_currency_resolution_menu(
                message,
                resolution_flow,
                notice="<b>Не вдалося підготувати конвертацію.</b>",
            )
            return
        converted_amount = calculate_transfer_amount(source_currency, target_currency, original_amount, fx_rate).target_amount
        tx_flow["account_id"] = target_account_id
        tx_flow["account_label"] = target_label or str(tx_flow.get("account_label") or "—")
        tx_flow["currency"] = target_currency
        tx_flow["amount"] = converted_amount
        tx_flow["comment"] = (str(resolution_flow.get("original_comment") or "")[:500] or None)
        tx_flow["step"] = TX_FLOW_STEP_CONFIRMATION
        tx_flow.pop("await_amount", None)
        context.user_data["tx_flow"] = tx_flow
        _reset_currency_resolution_flow(context)
        notice_text = _currency_conversion_notice(
            source_amount=original_amount,
            source_currency=source_currency,
            target_amount=converted_amount,
            target_currency=target_currency,
            fx_rate=fx_rate,
            rate_source=rate_source,
        )
        await message.reply_text(
            f"{notice_text}\n\n{_tx_confirm_text(tx_flow)}",
            reply_markup=_kb_tx_confirm(tx_flow),
        )
        return

    if mode == "ai_tx":
        flow = context.user_data.get("ai_tx_flow") or {}
        tx = _ai_flow_tx(flow)
        original_amount = tx.get("amount")
        if not isinstance(original_amount, Decimal):
            await _show_currency_resolution_menu(
                message,
                resolution_flow,
                notice="<b>Не вдалося підготувати конвертацію.</b>",
            )
            return
        converted_amount = calculate_transfer_amount(source_currency, target_currency, original_amount, fx_rate).target_amount
        tx["account_id"] = target_account_id
        tx["amount"] = converted_amount
        tx["currency"] = target_currency
        tx["currency_explicit"] = True
        accounts_map = flow.get("accounts")
        if isinstance(accounts_map, dict):
            accounts_map[target_account_id] = target_label
        flow.pop("await_amount", None)
        flow.pop("await_field", None)
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, tg_user_id, flow)
        _reset_currency_resolution_flow(context)
        notice_text = _currency_conversion_notice(
            source_amount=original_amount,
            source_currency=source_currency,
            target_amount=converted_amount,
            target_currency=target_currency,
            fx_rate=fx_rate,
            rate_source=rate_source,
        )
        await message.reply_text(
            f"{notice_text}\n\n{_ai_tx_card_text(flow)}",
            reply_markup=_ai_tx_confirm_markup(flow),
        )
        return

    if mode == "ai_batch":
        flow = context.user_data.get("ai_batch_tx_flow") or {}
        items = _ai_batch_flow_items(flow)
        converted_items_count = 0
        for item in items:
            if not bool(item.get("currency_explicit")):
                item["currency"] = target_currency
                continue
            item_currency = normalize_currency(str(item.get("currency") or source_currency)) or source_currency
            if item_currency != source_currency:
                continue
            try:
                original_amount = quantize_money(Decimal(str(item.get("amount") or 0)))
            except (InvalidOperation, ValueError):
                continue
            if original_amount <= 0:
                continue
            item["amount"] = calculate_transfer_amount(source_currency, target_currency, original_amount, fx_rate).target_amount
            item["currency"] = target_currency
            converted_items_count += 1
        flow["account_id"] = target_account_id
        accounts_map = flow.get("accounts")
        if isinstance(accounts_map, dict):
            accounts_map[target_account_id] = target_label
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, tg_user_id, flow)
        _reset_currency_resolution_flow(context)
        notice_text = _batch_currency_conversion_notice(
            converted_items_count=max(converted_items_count, 1),
            source_currency=source_currency,
            target_currency=target_currency,
            fx_rate=fx_rate,
            rate_source=rate_source,
        )
        await message.reply_text(
            f"{notice_text}\n\n{_ai_batch_tx_card_text(flow)}",
            reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))),
        )
        return

    _reset_currency_resolution_flow(context)
    await _reply_home(message, "Не вдалося завершити конвертацію.")


async def _handle_currency_resolution_text(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    resolution_flow: dict,
) -> bool:
    user = update.effective_user
    if not user or not update.message or not resolution_flow:
        return False
    if str(resolution_flow.get("step") or "") != "await_manual_rate":
        return False
    rate = parse_decimal_rate(update.message.text.strip())
    callback_prefix = str(resolution_flow.get("callback_prefix") or "ai:currency")
    if rate is None:
        await update.message.reply_text(
            "Не вдалося розпізнати курс. Наприклад: 40 або 39.85",
            reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
        )
        return True
    await _apply_currency_resolution_conversion(
        update.message,
        context,
        user.id,
        fx_rate=rate,
        rate_source="manual",
    )
    return True


async def _get_finance_scope(conn: asyncpg.Connection, tg_user_id: int):
    return await get_current_finance_scope(conn, tg_user_id)


async def _recalculate_account_balances(conn: asyncpg.Connection, tg_user_id: int | None = None) -> None:
    await AccountService(conn).recalculate_account_balances(tg_user_id)


async def _get_accounts(conn: asyncpg.Connection, tg_user_id: int) -> list[tuple[int, str]]:
    return await AccountService(conn).get_accounts(tg_user_id)


async def _get_accounts_full(conn: asyncpg.Connection, tg_user_id: int) -> list[asyncpg.Record]:
    return await AccountService(conn).get_accounts_full(tg_user_id)


async def _get_active_account_by_id(
    conn: asyncpg.Connection,
    tg_user_id: int,
    account_id: int,
    *,
    for_update: bool = False,
) -> asyncpg.Record | None:
    return await AccountService(conn).get_active_account_by_id(tg_user_id, account_id, for_update=for_update)


async def _get_categories(conn: asyncpg.Connection, tg_user_id: int, kind: str) -> list[tuple[int, str]]:
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        rows = await conn.fetch(
            """
            SELECT id, name
            FROM categories
            WHERE family_id=$1 AND type=$2 AND is_active=true
            ORDER BY is_system DESC, sort_order ASC, name ASC
            """,
            int(scope.family_id),
            kind,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name
            FROM categories
            WHERE user_id=$1 AND family_id IS NULL AND type=$2 AND is_active=true
            ORDER BY is_system DESC, sort_order ASC, name ASC
            """,
            tg_user_id,
            kind,
        )
    return [(int(row["id"]), str(row["name"])) for row in rows]


async def _get_categories_full(conn: asyncpg.Connection, tg_user_id: int, kind: str) -> list[tuple[int, str, list[str]]]:
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        rows = await conn.fetch(
            """
            SELECT id, name, aliases
            FROM categories
            WHERE family_id=$1 AND type=$2 AND is_active=true
            ORDER BY is_system DESC, sort_order ASC, name ASC
            """,
            int(scope.family_id),
            kind,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name, aliases
            FROM categories
            WHERE user_id=$1 AND family_id IS NULL AND type=$2 AND is_active=true
            ORDER BY is_system DESC, sort_order ASC, name ASC
            """,
            tg_user_id,
            kind,
        )
    return [(int(row["id"]), str(row["name"]), list(row["aliases"] or [])) for row in rows]


async def _user_ready(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    user = await _get_user(conn, tg_user_id)
    if not user:
        return False
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        accounts_count = await conn.fetchval("SELECT count(1) FROM accounts WHERE family_id=$1 AND is_active=true", int(scope.family_id))
    else:
        accounts_count = await conn.fetchval(
            "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true",
            tg_user_id,
        )
    return bool(
        user.get("onboarding_completed")
        and int(user.get("onboarding_version") or 0) >= CURRENT_ONBOARDING_VERSION
        and user.get("start_date")
        and user.get("base_currency")
        and accounts_count
        and int(accounts_count) > 0
    )


async def _ensure_default_categories(conn: asyncpg.Connection, tg_user_id: int) -> None:
    svc = CategoryService(conn)
    await svc.ensureDefaultCategories(tg_user_id)


async def _get_active_templates_for_onboarding(conn: asyncpg.Connection, type_: str) -> list[dict]:
    _ = (conn, type_)
    return []


async def _onb_select_all_templates(conn: asyncpg.Connection, context: ContextTypes.DEFAULT_TYPE) -> None:
    _ = (conn, context)


def _onb_norm_name(name: str) -> str:
    return (name or "").strip()


def _onb_is_digits_only(name: str) -> bool:
    t = re.sub(r"[\s.,-]+", "", (name or "").strip())
    return bool(t) and t.isdigit()


def _onb_conflicts_with_system(name: str) -> bool:
    n = _onb_norm_name(name)
    return n in {SYSTEM_EXPENSE_CATEGORY_NAME, SYSTEM_INCOME_CATEGORY_NAME}


def _onb_has_name_conflict(existing: list[str], candidate: str) -> bool:
    c = _onb_norm_name(candidate).lower()
    if not c:
        return True
    return any((_onb_norm_name(x).lower() == c) for x in (existing or []))


def _language_label(code: str | None) -> str:
    normalized = str(code or "").strip().lower()
    if normalized == "uk":
        return "Українська"
    if normalized == "en":
        return "English"
    return str(code or "—")


def _normalize_onboarding_categories_callback(data: str) -> str:
    return data


async def _show_onb_categories_mode(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _show_onboarding_summary(message, context)


async def _show_onb_custom_main(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await _show_onboarding_summary(message, context)


async def _show_onb_custom_list(message, context: ContextTypes.DEFAULT_TYPE, type_: str) -> int:
    _ = type_
    return await _show_onboarding_summary(message, context)


async def _reply_home(
    message,
    text: str | None = None,
) -> None:
    await message.reply_text(text or _home_menu_text(current_ui_locale()), reply_markup=kb_home())


async def _reply_and_return_home(message, text: str) -> None:
    await message.reply_text(text)
    await _reply_home(message)


async def _show_home(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message or (update.callback_query.message if update.callback_query else None)
    if not user or message is None:
        return
    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        await _show_access_surface(message, conn, user.id)


async def _show_settings_entry(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
) -> None:
    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=tg_user_id, conn=conn)
        maintenance_message = await should_block_for_maintenance(conn, tg_user_id)
        if maintenance_message:
            await message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, tg_user_id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if not await _user_ready(conn, tg_user_id) and access_scope != "debt_only":
            await message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(access_scope):
            await _show_access_locked(
                message,
                conn,
                tg_user_id,
                notice=FULL_ACCESS_REQUIRED_NOTICE,
            )
            return
        _reset_runtime_flows(context)
        await _show_settings_menu(message, conn)


async def _start_support_flow(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["support_flow"] = {"await_text": True}
    await message.reply_text(
        await _get_bot_copy_from_context(context, "support_intro_text"),
        reply_markup=_kb_home_only(),
    )


def _help_support_url() -> str:
    return str(config.SUPPORT_CONTACT_URL or "").strip()


HELP_TOPIC_TITLES_EN = {
    "getting_started": "Getting Started",
    "balances_accounts": "Balances and Accounts",
    "expenses_income": "Expenses and Income",
    "transfers": "Transfers",
    "categories": "Categories",
    "debts": "Debts",
    "savings": "Savings",
    "investments": "Investments",
    "family": "Family",
    "reports": "Reports",
    "miniapp": "Mini App",
    "export": "Data Export",
    "billing_access": "Billing and Access",
    "data_privacy": "Data and Privacy",
    "issues": "Bot Issues",
}


def _help_topic_titles(locale: str | None = None) -> list[tuple[str, str]]:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    catalog = get_faq_catalog(resolved_locale)
    return [(topic.id, topic.title) for topic in catalog.topics]


async def _show_help_home(message, *, locale: str | None = None) -> None:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    catalog = get_faq_catalog(resolved_locale)
    await message.reply_text(
        f"<b>{escape_html(catalog.home_title)}</b>\n\n{escape_html(catalog.home_intro)}",
        reply_markup=kb_help_home(_help_topic_titles(resolved_locale)),
        parse_mode=ParseMode.HTML,
    )


async def _show_help_topic(message, topic_id: str, *, locale: str | None = None) -> None:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    topic = get_faq_topic(topic_id, locale=resolved_locale)
    if topic is None:
        await message.reply_text(
            locale_text(
                "<b>Допомога</b>\n\nНе вдалося знайти цей розділ. Оберіть тему ще раз.",
                "<b>Help</b>\n\nCouldn't find this section. Choose a topic again.",
                resolved_locale,
            ),
            reply_markup=kb_help_home(_help_topic_titles(resolved_locale)),
            parse_mode=ParseMode.HTML,
        )
        return
    await message.reply_text(
        f"<b>{escape_html(topic.title)}</b>\n\n"
        + locale_text("Оберіть питання нижче.", "Choose a question below.", resolved_locale),
        reply_markup=kb_help_topic(topic.id, [(question.id, question.title) for question in topic.questions]),
        parse_mode=ParseMode.HTML,
    )


async def _show_help_answer(message, topic_id: str, question_id: str, *, locale: str | None = None) -> None:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    match = get_faq_question(topic_id, question_id, locale=resolved_locale)
    if match is None:
        await message.reply_text(
            locale_text(
                "<b>Допомога</b>\n\nНе вдалося знайти цю відповідь. Оберіть тему ще раз.",
                "<b>Help</b>\n\nCouldn't find this answer. Choose a topic again.",
                resolved_locale,
            ),
            reply_markup=kb_help_home(_help_topic_titles(resolved_locale)),
            parse_mode=ParseMode.HTML,
        )
        return
    _topic, question = match
    await message.reply_text(
        f"<b>{escape_html(question.title)}</b>\n\n{escape_html(question.answer)}",
        reply_markup=kb_help_answer(topic_id),
        parse_mode=ParseMode.HTML,
    )


async def _show_help_support(message, *, locale: str | None = None) -> None:
    resolved_locale = normalize_ui_locale(locale or current_ui_locale())
    catalog = get_faq_catalog(resolved_locale)
    support_title = catalog.support_title
    support_intro = catalog.support_intro
    support_missing_url_text = catalog.support_missing_url_text
    if resolved_locale == "en":
        support_title = "Support"
        support_intro = (
            "Describe what is not working or what question you have.\n\n"
            "To help us respond faster, include:\n"
            "• which bot section you used;\n"
            "• what you tapped;\n"
            "• what should have happened;\n"
            "• what happened instead;\n"
            "• a screenshot if relevant."
        )
        support_missing_url_text = "The support link is not configured yet. Please try again later or contact us another way."
    text = f"<b>{escape_html(support_title)}</b>\n\n{escape_html(support_intro)}"
    support_url = _help_support_url()
    if not support_url:
        text = f"{text}\n\n{escape_html(support_missing_url_text)}"
    await message.reply_text(
        text,
        reply_markup=kb_help_support(support_url or None),
        parse_mode=ParseMode.HTML,
    )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    if user:
        await _consume_pending_admin_reset_if_needed(context, user.id)
        pool = getattr(getattr(context, "application", None), "bot_data", {}).get("db_pool")
        if pool is not None:
            async with pool.acquire() as conn:
                await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        else:
            _current_context_locale(context, getattr(user, "language_code", None))
    _reset_runtime_flows(context)
    await _show_home(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    if user:
        await _consume_pending_admin_reset_if_needed(context, user.id)
        pool = getattr(getattr(context, "application", None), "bot_data", {}).get("db_pool")
        if pool is not None:
            async with pool.acquire() as conn:
                await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        else:
            _current_context_locale(context, getattr(user, "language_code", None))
    _reset_runtime_flows(context)
    await _show_help_home(update.message, locale=_current_context_locale(context))


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)
    await _show_settings_entry(update.message, context, user.id)


def _miniapp_url() -> str:
    url = str(config.MINIAPP_URL or "").strip()
    if not url:
        return ""
    parts = urlsplit(url)
    path = parts.path or "/app/"
    if path.rstrip("/") in {"/app", "/app-v2", "/app-shell", "/app-shell-2"}:
        path = "/app/"
    query_items = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "v"]
    cache_bust = str(getattr(config, "MINIAPP_CACHE_BUST_VERSION", "") or "").strip()
    if cache_bust:
        query_items.append(("v", cache_bust))
    return urlunsplit((parts.scheme, parts.netloc, path, urlencode(query_items), parts.fragment))


def _miniapp_operator_url() -> str:
    url = _miniapp_url()
    if not url:
        return ""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "/app/operator/", parts.query, ""))


def _is_miniapp_operator(tg_user_id: int | None) -> bool:
    try:
        user_id = int(tg_user_id or 0)
    except (TypeError, ValueError):
        return False
    return user_id in {int(value) for value in getattr(config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [])}


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _miniapp_browser_login_secret() -> bytes:
    secret = str(getattr(config, "MINIAPP_BROWSER_LOGIN_SECRET", "") or config.BOT_TOKEN or "").strip()
    if not secret:
        return b""
    return hmac.new(b"MiniAppBrowserLogin", secret.encode("utf-8"), hashlib.sha256).digest()


def _miniapp_browser_login_token(tg_user_id: int, *, now: int | None = None, nonce: str | None = None) -> str:
    try:
        user_id = int(tg_user_id)
    except (TypeError, ValueError):
        return ""
    if user_id <= 0:
        return ""
    secret = _miniapp_browser_login_secret()
    if not secret:
        return ""

    issued_at = int(time.time() if now is None else now)
    ttl_seconds = max(60, min(int(getattr(config, "MINIAPP_BROWSER_LOGIN_TTL_SECONDS", 15 * 60) or 15 * 60), 60 * 60))
    payload = {
        "v": 1,
        "tg_user_id": user_id,
        "iat": issued_at,
        "exp": issued_at + ttl_seconds,
        "nonce": nonce or secrets.token_urlsafe(18),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(secret, payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(signature)}"


def _miniapp_browser_login_url(tg_user_id: int, *, operator: bool = False) -> str:
    token = _miniapp_browser_login_token(tg_user_id)
    if not token:
        return ""
    base_url = _miniapp_url()
    if not base_url:
        return ""
    parts = urlsplit(base_url)
    query = "operator=1" if operator else ""
    return urlunsplit((parts.scheme, parts.netloc, f"/app/browser-login/{quote(token, safe='')}/", query, ""))


def kb_home() -> InlineKeyboardMarkup:
    return _kb_home_base(url=_miniapp_url())


def _miniapp_launch_markup(
    *,
    include_home_button: bool = False,
    browser_login_url: str | None = None,
    app_url: str | None = None,
) -> InlineKeyboardMarkup | None:
    url = str(app_url or _miniapp_url()).strip()
    if not url:
        return None
    markup = kb_dashboard_launch_menu(url=url, include_home_button=include_home_button)
    safe_browser_login_url = str(browser_login_url or "").strip()
    if not safe_browser_login_url:
        return markup
    rows = _inline_keyboard_rows(markup)
    rows.insert(
        0,
        [
            InlineKeyboardButton(
                locale_text("🌐 Увійти через Safari / Chrome", "🌐 Sign in with Safari / Chrome", current_ui_locale()),
                url=safe_browser_login_url,
            )
        ],
    )
    return InlineKeyboardMarkup(rows)


async def _show_operator_miniapp_entry(message, tg_user_id: int) -> bool:
    if not _is_miniapp_operator(tg_user_id):
        return False
    launch_markup = _miniapp_launch_markup(
        include_home_button=True,
        browser_login_url=_miniapp_browser_login_url(tg_user_id, operator=True),
        app_url=_miniapp_operator_url(),
    )
    if launch_markup is None:
        return False
    await message.reply_text(
        "<b>Vydno Admin Hub</b>\n\n"
        "Короткий операційний огляд, пошук користувачів і швидкі переходи до повної CRM. "
        "Небезпечні дії залишаються тільки у повній адмінці.",
        reply_markup=launch_markup,
    )
    return True


def _inline_keyboard_rows(markup: InlineKeyboardMarkup | None) -> list[list[InlineKeyboardButton]]:
    if markup is None:
        return []
    inline_keyboard = getattr(markup, "inline_keyboard", None)
    if inline_keyboard is not None:
        return [list(row) for row in inline_keyboard]
    args = getattr(markup, "args", ())
    if args:
        rows = args[0]
        return [list(row) for row in rows]
    return []


def _with_miniapp_launch(
    markup: InlineKeyboardMarkup | None,
    *,
    prepend: bool = True,
) -> InlineKeyboardMarkup | None:
    launch_markup = _miniapp_launch_markup()
    if launch_markup is None:
        return markup
    launch_rows = _inline_keyboard_rows(launch_markup)
    if not launch_rows:
        return markup
    base_rows = _inline_keyboard_rows(markup)
    rows = launch_rows + base_rows if prepend else base_rows + launch_rows
    return InlineKeyboardMarkup(rows)


async def _show_miniapp_onboarding_entry(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
) -> bool:
    """Open Mini App setup without applying the post-onboarding paywall."""
    await _set_locale_for_user(conn, tg_user_id)
    if await _show_operator_miniapp_entry(message, tg_user_id):
        return True
    launch_markup = _miniapp_launch_markup(
        include_home_button=True,
        browser_login_url=_miniapp_browser_login_url(tg_user_id),
    )
    if launch_markup is None:
        return False
    await message.reply_text(
        locale_text(
            "<b>Продовжіть налаштування у Vydno.Capital.</b>\n\n"
            "Оберіть мову, валюту та додайте перший рахунок у застосунку. "
            "Кнопка нижче містить щойно створене актуальне посилання.\n\n"
            "Для окремого вебдодатка відкрийте його через Safari / Chrome. Оплату запропонуємо лише після завершення налаштування.",
            "<b>Continue setup in Vydno.Capital.</b>\n\n"
            "Choose the language and currency, then add your first account in the app. "
            "The button below contains a freshly generated login link.\n\n"
            "For the standalone web app, continue with Safari / Chrome. Payment is offered only after setup is complete.",
            current_ui_locale(),
        ),
        reply_markup=launch_markup,
    )
    return True


async def _show_miniapp_entry(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    notice: str | None = None,
) -> None:
    await _set_locale_for_user(conn, tg_user_id)
    if await _show_operator_miniapp_entry(message, tg_user_id):
        return
    browser_login_url = _miniapp_browser_login_url(tg_user_id)
    launch_markup = _miniapp_launch_markup(
        include_home_button=True,
        browser_login_url=browser_login_url,
    )
    if launch_markup is None:
        await message.reply_text(_miniapp_unavailable_text(current_ui_locale()))
        return

    access_state = await _get_resolved_access_state(conn, tg_user_id)
    access_state, _sync_result = await _maybe_sync_pending_bind_access_state(conn, tg_user_id, access_state)
    access_scope = str(access_state.get("access_scope") or "paywall")
    billing_state = dict(access_state.get("billing_state") or {})
    access_mode = str(billing_state.get("access_mode") or "open")

    if access_scope == "debt_only":
        await _show_debt_only_surface(
            message,
            conn,
            tg_user_id,
            notice=notice
            or locale_text(
                "<b>Кабінет зараз недоступний для режиму лише з боргами.</b>",
                "<b>The dashboard is not available in debt-only mode.</b>",
                current_ui_locale(),
            ),
        )
        return

    lines: list[str] = []
    if notice:
        lines.extend([notice, ""])
    lines.extend(
        [
            "<b>Vydno.Capital</b>",
            "",
            locale_text(
                "Відкрий свій фінансовий кабінет у Mini App всередині Telegram.",
                "Open your finance dashboard in the Mini App inside Telegram.",
                current_ui_locale(),
            ),
        ]
    )
    if browser_login_url:
        lines.extend(
            [
                "",
                locale_text(
                    "Для іконки на телефоні використай веб-вхід один раз у Safari/Chrome; далі сесія збережеться.",
                    "For the phone Home Screen app, use the browser login once in Safari/Chrome; the session will stay saved.",
                    current_ui_locale(),
                ),
            ]
        )
    if not _access_scope_has_full_home(access_scope) and access_mode != "read_only":
        lines.extend(
            [
                "",
                locale_text(
                    "Завершення налаштування, пробний період і оплата продовжаться вже у застосунку.",
                    "Setup, trial activation, and payment continue inside the app.",
                    current_ui_locale(),
                ),
            ]
        )
    if access_mode == "read_only" and not _access_scope_has_full_home(access_scope):
        grace_until = _format_billing_datetime(billing_state.get("grace_expires_at"))
        if grace_until != "—":
            lines.extend(
                [
                    "",
                    locale_text(
                        f"Зараз доступ лише для перегляду до {grace_until}.",
                        f"Read-only access is active until {grace_until}.",
                        current_ui_locale(),
                    ),
                ]
            )
        else:
            lines.extend(["", locale_text("Зараз доступ лише для перегляду.", "Read-only access is active.", current_ui_locale())])
    elif billing_state.get("soft_grace_active"):
        lines.extend(
            [
                "",
                locale_text(
                    "Оплату варто оновити, але кабінет поки ще доступний.",
                    "Your payment should be updated, but the dashboard is still available for now.",
                    current_ui_locale(),
                ),
            ]
        )

    await message.reply_text(
        "\n".join(lines),
        reply_markup=launch_markup,
        parse_mode=ParseMode.HTML,
    )


async def cabinet_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user
    if message is None or user is None:
        return

    await _consume_pending_admin_reset_if_needed(context, user.id)
    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        if await is_user_banned(conn, user.id):
            await message.reply_text(_access_restricted_text(current_ui_locale()))
            return
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if not await _user_ready(conn, user.id) and access_scope != "debt_only":
            if not await _show_miniapp_onboarding_entry(message, conn, user.id):
                await message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        _reset_runtime_flows(context)
        await _show_miniapp_entry(message, conn, user.id)


def _format_decimal(value: Decimal | float | int | None) -> str:
    return format_decimal_value(value, places=2)


def _account_list_item(row: asyncpg.Record, index: int) -> str:
    return f"{index}. {escape_html(row['label'])} — <b>{format_money(row['balance'], row['currency'])}</b>"


def _format_fx_snapshot_date(value: object) -> str | None:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%d.%m.%Y")
    text = str(value).strip()
    return text or None


async def _get_fx_snapshot_for_base(base_currency: str, currencies: set[str]) -> tuple[object | None, str | None]:
    foreign_currencies = {currency for currency in currencies if currency and currency != base_currency}
    if not foreign_currencies:
        return None, None
    try:
        return await get_latest_rates(base_currency), None
    except Exception:
        return None, "Не вдалося оновити приблизні курси зараз."


def _convert_amount_to_base(
    amount: Decimal | float | int | None,
    currency: str,
    base_currency: str,
    snapshot: object | None,
) -> tuple[Decimal | None, Decimal | None]:
    source_currency = normalize_currency(currency)
    amount_value = Decimal(str(amount or 0))
    if source_currency == base_currency:
        return quantize_money(amount_value), None
    if snapshot is None:
        return None, None
    rates = getattr(snapshot, "rates", None) or {}
    rate = rates.get(source_currency)
    if rate is None:
        return None, None
    rate_value = Decimal(str(rate))
    return quantize_money(amount_value * rate_value), rate_value


def _format_base_total(amount: Decimal, base_currency: str, *, approximate: bool) -> str:
    prefix = "≈ " if approximate else ""
    return f"{prefix}<b>{format_money(quantize_money(amount), base_currency)}</b>"


def _render_base_total_line(
    label: str,
    amount: Decimal | None,
    base_currency: str,
    *,
    approximate: bool,
) -> str:
    if amount is None:
        return f"<b>{escape_html(label)}:</b> тимчасово недоступно у {escape_html(base_currency)}"
    return f"<b>{escape_html(label)}:</b> {_format_base_total(amount, base_currency, approximate=approximate)}"


async def _fetch_debt_summary_rows(conn: asyncpg.Connection, tg_user_id: int) -> list[asyncpg.Record]:
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        return await conn.fetch(
            """
            SELECT counterparty, currency,
                   sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
                   sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
            FROM transactions
            WHERE family_id=$1 AND flow_kind='debt'
              AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
            GROUP BY counterparty, currency
            ORDER BY counterparty ASC
            """,
            int(scope.family_id),
        )
    return await conn.fetch(
        """
        SELECT counterparty, currency,
               sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
               sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
        FROM transactions
        WHERE tg_user_id=$1 AND family_id IS NULL AND flow_kind='debt'
          AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
        GROUP BY counterparty, currency
        ORDER BY counterparty ASC
        """,
        tg_user_id,
    )


async def _build_debt_summary_in_base(
    debt_rows: list[asyncpg.Record],
    base_currency: str,
    snapshot: object | None,
) -> tuple[dict[str, Decimal | None], dict[str, Decimal], dict[str, bool]]:
    totals: dict[str, Decimal | None] = {
        "i_owe": Decimal("0"),
        "owed_to_me": Decimal("0"),
    }
    rates_used: dict[str, Decimal] = {}
    approximate_flags = {"i_owe": False, "owed_to_me": False}

    for row in debt_rows:
        currency = normalize_currency(str(row["currency"] or base_currency))
        for key in ("i_owe", "owed_to_me"):
            raw_amount = max(Decimal(str(row[key] or 0)), Decimal("0"))
            if raw_amount <= 0:
                continue
            converted, rate = _convert_amount_to_base(raw_amount, currency, base_currency, snapshot)
            if converted is None:
                totals[key] = None
                continue
            if totals[key] is None:
                continue
            totals[key] += converted
            if currency != base_currency:
                approximate_flags[key] = True
                if rate is not None:
                    rates_used[currency] = rate

    if totals["i_owe"] is not None:
        totals["i_owe"] = quantize_money(totals["i_owe"])
    if totals["owed_to_me"] is not None:
        totals["owed_to_me"] = quantize_money(totals["owed_to_me"])
    return totals, rates_used, approximate_flags


def _append_fx_footer(lines: list[str], rates_used: dict[str, Decimal], snapshot: object | None, base_currency: str, fx_error: str | None) -> None:
    if rates_used:
        lines += ["", "<b>Курси:</b>"]
        for currency in sorted(rates_used):
            lines.append(f"1 {escape_html(currency)} = {format_decimal_value(rates_used[currency], places=4)} {escape_html(base_currency)}")
        snapshot_date = _format_fx_snapshot_date(getattr(snapshot, "date", None))
        if snapshot_date:
            lines.append(f"<i>Дата курсу: {snapshot_date}</i>")
    elif fx_error:
        lines += ["", fx_error]


def _transfer_account_option(row: asyncpg.Record) -> tuple[int, str]:
    return int(row["id"]), f"{format_account_name(row)}"


async def _render_accounts_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    await _recalculate_account_balances(conn, tg_user_id)
    user = await _get_user(conn, tg_user_id)
    base_currency = normalize_currency(str((user.get("base_currency") if user else None) or "UAH"))
    accounts = await _get_accounts_full(conn, tg_user_id)
    if not accounts:
        return await _get_bot_copy(conn, "accounts_empty_text")
    grouped: dict[str, list[asyncpg.Record]] = {
        key: []
        for key in ("main", "cash", "savings", "deposit", "investment", "credit", "other")
    }
    for row in accounts:
        raw_account_type = row["account_type"] if "account_type" in row else "other"
        grouped.setdefault(normalize_account_type(str(raw_account_type or "other")), []).append(row)

    section_titles = {
        "main": "Звичайні рахунки",
        "cash": "Готівка",
        "savings": "Накопичення",
        "deposit": "Депозити",
        "investment": "Інвестиції",
        "credit": "Кредитки",
        "other": "Інше",
    }
    lines = ["<b>💼 Рахунки</b>"]
    debt_rows = await _fetch_debt_summary_rows(conn, tg_user_id)
    snapshot, fx_error = await _get_fx_snapshot_for_base(
        base_currency,
        {
            *(normalize_currency(str(row["currency"] or base_currency)) for row in accounts),
            *(normalize_currency(str(row["currency"] or base_currency)) for row in debt_rows),
        },
    )
    debt_totals, debt_rates_used, debt_approximate_flags = await _build_debt_summary_in_base(debt_rows, base_currency, snapshot)
    overall_total = Decimal("0")
    overall_can_compute = True
    overall_has_foreign = False
    rates_used: dict[str, Decimal] = {}
    index = 1
    for key in ("main", "cash", "savings", "deposit", "investment", "credit", "other"):
        items = grouped.get(key) or []
        if not items:
            continue
        lines += ["", f"<b>{section_titles[key]}:</b>"]
        section_total = Decimal("0")
        section_can_compute = True
        section_has_foreign = False
        for row in items:
            currency = normalize_currency(str(row["currency"] or base_currency))
            balance = Decimal(str(row["balance"] or 0))
            _, rate = _convert_amount_to_base(balance, currency, base_currency, snapshot)
            lines.append(f"{index}. {format_account_line(row, base_currency=base_currency, rate=rate)}")
            converted, converted_rate = _convert_amount_to_base(balance, currency, base_currency, snapshot)
            if converted is None:
                section_can_compute = False
                if key != CREDIT_ACCOUNT_TYPE:
                    overall_can_compute = False
            else:
                section_total += converted
                if key != CREDIT_ACCOUNT_TYPE and overall_can_compute:
                    overall_total += converted
            if currency != base_currency:
                section_has_foreign = True
                if key != CREDIT_ACCOUNT_TYPE:
                    overall_has_foreign = True
                if converted_rate is not None:
                    rates_used[currency] = converted_rate
            index += 1
        lines.append(
            _render_base_total_line(
                "Разом по типу",
                quantize_money(section_total) if section_can_compute else None,
                base_currency,
                approximate=section_has_foreign,
            )
        )

    lines += [
        "",
        _render_base_total_line(
            "Загалом без кредиток",
            quantize_money(overall_total) if overall_can_compute else None,
            base_currency,
            approximate=overall_has_foreign,
        ),
        "",
        "<b>Коротко про борги:</b>",
        _render_base_total_line(
            "Винен Я",
            debt_totals["i_owe"],
            base_currency,
            approximate=debt_approximate_flags["i_owe"],
        ),
        _render_base_total_line(
            "Винні Мені",
            debt_totals["owed_to_me"],
            base_currency,
            approximate=debt_approximate_flags["owed_to_me"],
        ),
    ]
    rates_used.update(debt_rates_used)
    _append_fx_footer(lines, rates_used, snapshot, base_currency, fx_error)
    return "\n".join(lines)


async def _render_savings_overview_text(conn: asyncpg.Connection, tg_user_id: int) -> tuple[str, bool]:
    await _recalculate_account_balances(conn, tg_user_id)
    accounts = await _get_accounts_full(conn, tg_user_id)
    grouped: dict[str, list[asyncpg.Record]] = {key: [] for key in ("savings", "deposit", "investment")}
    totals_by_currency: dict[str, Decimal] = {}
    has_assets = False

    for row in accounts:
        account_type = normalize_account_type(str(row["account_type"] or "other"))
        if account_type not in grouped:
            continue
        grouped[account_type].append(row)
        has_assets = True
        currency = normalize_currency(str(row["currency"] or "UAH"))
        totals_by_currency[currency] = totals_by_currency.get(currency, Decimal("0")) + Decimal(str(row["balance"] or 0))

    if not has_assets:
        return await _get_bot_copy(conn, "savings_empty_text"), False

    section_titles = {
        "savings": "Накопичення",
        "deposit": "Депозити",
        "investment": "Інвестиції",
    }
    lines = ["<b>🐿️ Заощадження та інвестиції</b>"]
    for key in ("savings", "deposit", "investment"):
        items = grouped.get(key) or []
        lines += ["", f"<b>{section_titles[key]}:</b>"]
        if not items:
            lines.append("Немає")
            continue
        for row in items:
            lines.extend(_savings_overview_account_lines(row))

    if totals_by_currency:
        lines += ["", f"<b>Разом у накопиченнях та інвестиціях:</b> {_format_currency_totals(totals_by_currency)}", "", "Що робимо?"]
    return "\n".join(lines), True


async def _render_balances_overview_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    await _recalculate_account_balances(conn, tg_user_id)
    user = await _get_user(conn, tg_user_id)
    base_currency = normalize_currency(str((user.get("base_currency") if user else None) or "UAH"))
    accounts = await _get_accounts_full(conn, tg_user_id)
    if not accounts:
        return (
            "<b>💼 Рахунки</b>\n\n"
            "У вас ще немає жодного рахунку.\n\n"
            "Щоб почати облік, додайте перший рахунок."
        )

    lines = ["<b>💼 Рахунки</b>", "", f"Базова валюта: <b>{escape_html(base_currency)}</b>", "", "<b>Список рахунків:</b>"]
    total = Decimal("0")
    has_foreign_accounts = False
    can_compute_total = True
    rates_used: dict[str, Decimal] = {}
    fx_error: str | None = None
    snapshot = None

    other_currencies = {
        normalize_currency(str(row["currency"]))
        for row in accounts
        if normalize_currency(str(row["currency"])) != base_currency
    }
    if other_currencies:
        try:
            snapshot = await get_latest_rates(base_currency)
        except Exception:
            fx_error = "Не вдалося оновити приблизні курси зараз."

    for index, row in enumerate(accounts, start=1):
        currency = normalize_currency(str(row["currency"]))
        balance = Decimal(str(row["balance"] or 0))
        rate = snapshot.rates.get(currency) if snapshot else None
        line = f"{index}. {format_account_line(row, base_currency=base_currency, rate=rate)}"
        if currency == base_currency:
            total += balance
        else:
            has_foreign_accounts = True
            if rate is not None:
                converted = quantize_money(balance * rate)
                total += converted
                rates_used[currency] = rate
            else:
                can_compute_total = False
        lines.append(line)

    if can_compute_total:
        lines += ["", f"<b>Разом:</b> ≈ <b>{format_money(quantize_money(total), base_currency)}</b>"]
    else:
        lines += ["", f"<b>Разом:</b> тимчасово недоступно у {escape_html(base_currency)}"]
    if has_foreign_accounts:
        if rates_used:
            lines += ["", "<b>Курси:</b>"]
            for currency in sorted(rates_used):
                lines.append(f"1 {escape_html(currency)} = {format_decimal_value(rates_used[currency], places=4)} {escape_html(base_currency)}")
            snapshot_date = _format_fx_snapshot_date(getattr(snapshot, "date", None))
            if snapshot_date:
                lines.append(f"<i>Дата курсу: {snapshot_date}</i>")
        elif fx_error:
            lines += ["", fx_error]
    return "\n".join(lines)


async def _show_settings_menu(message, conn: asyncpg.Connection) -> None:
    await message.reply_text(
        await _get_bot_copy(conn, "settings_intro_text"),
        reply_markup=_with_miniapp_launch(kb_settings_menu()),
    )


def _format_billing_datetime(value: datetime | None) -> str:
    if value is None:
        return "—"
    return value.astimezone().strftime("%d.%m.%Y %H:%M")


def _billing_subscription_label(status: str) -> str:
    mapping = {
        "trial": "Пробний період",
        "active": "Активна",
        "paid": "Активна",
        "expired": "Очікує оплату",
        "cancelled": "Скасована",
        "manual": "Активна вручну",
        "lifetime": "Lifetime",
    }
    return mapping.get(status, "—")


def _billing_access_label(access_mode: str) -> str:
    mapping = {
        "open": "Повний доступ",
        "full": "Повний доступ",
        "read_only": "Лише читання",
        "blocked": "Потрібна оплата",
    }
    return mapping.get(access_mode, "Повний доступ")


def _billing_retry_allowed(state: dict[str, Any]) -> bool:
    if not state.get("profile_exists") or not state.get("has_card"):
        return False
    if state.get("last_action_url"):
        return True
    if state.get("access_mode") in {"read_only", "blocked"}:
        return True
    if not state.get("auto_renew_enabled"):
        return False
    return str(state.get("last_charge_status") or "") in {"failed", "rejected", "requires_action"}


def _billing_prompt_after_onboarding(state: dict[str, Any]) -> bool:
    access_mode = str(state.get("access_mode") or "open")
    if access_mode != "open":
        return False
    if state.get("has_card"):
        return False
    return not bool(state.get("subscription_status"))


def _billing_has_subscription_history(state: dict[str, Any]) -> bool:
    return bool(
        state.get("subscription_status")
        or state.get("expires_at")
        or state.get("next_charge_at")
        or state.get("grace_expires_at")
    )


def _billing_recovery_checkout_needed(*, access_scope: str, state: dict[str, Any]) -> bool:
    if access_scope != "paywall":
        return False
    if not _billing_has_subscription_history(state):
        return False
    return str(state.get("access_mode") or "") in {"read_only", "blocked"}


def _billing_trial_days(value: Any, default: int = 30) -> int:
    try:
        days = int(value)
    except (TypeError, ValueError):
        days = default
    return days if days > 0 else default


def _billing_trial_offer_lines(trial_days: int) -> list[str]:
    return [
        "<b>Почніть пробний період</b>",
        "",
        f"Прив'яжіть картку, щоб увімкнути {trial_days} днів повного доступу.",
        "",
        "Сьогодні спишемо 1 грн для перевірки картки.",
        f"499 грн/міс — лише після {trial_days} днів.",
    ]


def _billing_bind_button_label(*, trial_granted: bool) -> str:
    return "Активувати пробний період" if trial_granted else "Відкрити Monobank"


def _billing_bind_confirmation_text(*, trial_granted: bool, trial_days: int) -> str:
    lines = [
        "<b>Підтвердьте в Monobank</b>",
        "",
    ]
    if trial_granted and trial_days > 0:
        lines.extend(
            [
                f"Щоб увімкнути {trial_days} днів пробного доступу, підтвердьте прив'язку картки.",
                "",
                "Сьогодні спишемо 1 грн для перевірки картки.",
                f"499 грн/міс — лише після {trial_days} днів.",
            ]
        )
    else:
        lines.extend(
            [
                "Щоб оновити картку, підтвердьте прив'язку в Monobank.",
                "",
                "Сьогодні спишемо 1 грн для перевірки картки.",
                "Новий пробний період не нараховується.",
                "",
                "Якщо доступ уже зупинився, після прив'язки натисніть «Оплатити зараз».",
            ]
        )
    return "\n".join(lines)


def _billing_recovery_confirmation_text() -> str:
    return "\n".join(
        [
            "<b>Підтвердьте оплату в Monobank</b>",
            "",
            "Зараз спишемо 499 грн за місячний доступ.",
            "Після успішної оплати доступ відновиться одразу.",
            "Картку, яку підтвердите в Monobank, збережемо для наступних щомісячних списань.",
        ]
    )


def _billing_onboarding_autobind_text(*, trial_days: int) -> str:
    return "\n".join(
        [
            "🎉 <b>Вітаємо! Все налаштовано</b>",
            "",
            "Тепер можна користуватись vydno.capital і бачити фінанси в одному місці: витрати, доходи, рахунки, борги та заощадження.",
            "",
            f"Повний доступ можна спробувати {trial_days} днів без оплати.",
            "",
            "Щоб активувати пробний період, прив'яжіть картку:",
            "• 1 грн зараз: перевірка картки",
            f"• {trial_days} днів: без оплати",
            "• після цього: 499 грн/міс",
            "",
            "Оплата почнеться тільки після пробного періоду.",
        ]
    )


def _access_scope_has_full_home(access_scope: str) -> bool:
    return access_scope in {"personal_full", "family_full"}


def _access_scope_allows_debts(access_scope: str) -> bool:
    return access_scope in {"personal_full", "family_full", "debt_only"}


def _pending_promo_code(access_state: dict[str, Any]) -> str:
    return str(access_state.get("promo_offer_code") or "")


def _billing_soft_grace_notice(state: dict[str, Any]) -> str:
    if not bool(state.get("soft_grace_active")):
        return ""
    grace_until = _format_billing_datetime(state.get("grace_expires_at"))
    return (
        "<b>Автосписання не пройшло.</b>\n\n"
        f"Ми залишили повний доступ до {grace_until}. Поповніть картку або оновіть спосіб оплати, "
        "щоб доступ не зупинився."
    )


def _billing_pending_bind_sync_needed(state: dict[str, Any]) -> bool:
    return (
        bool(state.get("profile_exists"))
        and not bool(state.get("has_card"))
        and bool(str(state.get("last_action_url") or "").strip())
        and not bool(str(state.get("subscription_status") or "").strip())
    )


def _monobank_status_is_success(value: Any) -> bool:
    return str(value or "").strip().lower() in {"success", "paid"}


async def _maybe_sync_pending_bind_access_state(
    conn: asyncpg.Connection,
    tg_user_id: int,
    access_state: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    billing_state = dict(access_state.get("billing_state") or {})
    if not _billing_pending_bind_sync_needed(billing_state):
        return access_state, None
    try:
        sync_result = await sync_pending_bind_status(tg_user_id=tg_user_id)
    except BillingAPIError:
        return access_state, None
    refreshed_access_state = await _get_resolved_access_state(conn, tg_user_id)
    return refreshed_access_state, sync_result


async def _sync_access_scope_state(
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    access_scope: str,
    access_source: str,
    pending_start_payload: str | None = None,
    source: str | None = None,
    referral_code: str | None = None,
    clear_pending_start_payload: bool = False,
) -> None:
    if pending_start_payload is not None:
        await conn.execute(
            """
            INSERT INTO user_admin_states (
              telegram_user_id,
              status,
              subscription_status,
              access_scope,
              access_source,
              source,
              referral_code,
              pending_start_payload,
              timezone,
              is_blocked,
              can_receive_messages,
              blocked_bot,
              admin_comment,
              is_test_user,
              test_user_notes,
              current_fsm_state,
              onboarding_payload,
              last_user_input,
              last_bot_response,
              last_parse_error,
              pending_admin_reset_mode,
              created_at,
              updated_at
            )
            VALUES (
              $1,
              'active',
              'none',
              $2,
              $3,
              COALESCE($5, ''),
              COALESCE($6, ''),
              $4,
              '',
              false,
              true,
              false,
              '',
              false,
              '',
              '',
              '{}'::jsonb,
              '',
              '',
              '',
              '',
              now(),
              now()
            )
            ON CONFLICT (telegram_user_id) DO UPDATE SET
              access_scope=EXCLUDED.access_scope,
              access_source=EXCLUDED.access_source,
              source=COALESCE($5, user_admin_states.source),
              referral_code=COALESCE($6, user_admin_states.referral_code),
              pending_start_payload=EXCLUDED.pending_start_payload,
              updated_at=now()
            """,
            tg_user_id,
            access_scope,
            access_source,
            pending_start_payload,
            source,
            referral_code,
        )
        return
    if clear_pending_start_payload:
        await conn.execute(
            """
            INSERT INTO user_admin_states (
              telegram_user_id,
              status,
              subscription_status,
              access_scope,
              access_source,
              source,
              referral_code,
              pending_start_payload,
              timezone,
              is_blocked,
              can_receive_messages,
              blocked_bot,
              admin_comment,
              is_test_user,
              test_user_notes,
              current_fsm_state,
              onboarding_payload,
              last_user_input,
              last_bot_response,
              last_parse_error,
              pending_admin_reset_mode,
              created_at,
              updated_at
            )
            VALUES (
              $1,
              'active',
              'none',
              $2,
              $3,
              '',
              '',
              '',
              '',
              false,
              true,
              false,
              '',
              false,
              '',
              '',
              '{}'::jsonb,
              '',
              '',
              '',
              '',
              now(),
              now()
            )
            ON CONFLICT (telegram_user_id) DO UPDATE SET
              access_scope=EXCLUDED.access_scope,
              access_source=EXCLUDED.access_source,
              pending_start_payload='',
              updated_at=now()
            """,
            tg_user_id,
            access_scope,
            access_source,
        )
        return
    await conn.execute(
        """
        INSERT INTO user_admin_states (
          telegram_user_id,
          status,
          subscription_status,
          access_scope,
          access_source,
          source,
          referral_code,
          pending_start_payload,
          timezone,
          is_blocked,
          can_receive_messages,
          blocked_bot,
          admin_comment,
          is_test_user,
          test_user_notes,
          current_fsm_state,
          onboarding_payload,
          last_user_input,
          last_bot_response,
          last_parse_error,
          pending_admin_reset_mode,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          'active',
          'none',
          $2,
          $3,
          '',
          '',
          '',
          '',
          false,
          true,
          false,
          '',
          false,
          '',
          '',
          '{}'::jsonb,
          '',
          '',
          '',
          '',
          now(),
          now()
        )
        ON CONFLICT (telegram_user_id) DO UPDATE SET
          access_scope=EXCLUDED.access_scope,
          access_source=EXCLUDED.access_source,
          updated_at=now()
        """,
        tg_user_id,
        access_scope,
        access_source,
    )


async def _clear_pending_start_payload_for_state(
    conn: asyncpg.Connection,
    tg_user_id: int,
    access_state: dict[str, Any],
) -> None:
    await _sync_access_scope_state(
        conn,
        tg_user_id,
        access_scope=str(access_state.get("access_scope") or "paywall"),
        access_source=str(access_state.get("access_source") or "billing"),
        clear_pending_start_payload=True,
    )
    access_state["pending_start_payload"] = ""


async def _get_resolved_access_state(conn: asyncpg.Connection, tg_user_id: int) -> dict[str, Any]:
    access_state = await get_access_state(conn, tg_user_id)
    await _sync_access_scope_state(
        conn,
        tg_user_id,
        access_scope=str(access_state.get("access_scope") or "paywall"),
        access_source=str(access_state.get("access_source") or "billing"),
    )
    return access_state


async def _present_family_invite_prompt(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    token: str,
    *,
    remember_pending: bool,
    silent_invalid: bool,
) -> bool:
    locale = current_ui_locale()
    if not config.FAMILY_ACCESS_ENABLED:
        if not silent_invalid:
            await message.reply_text(_ui_text("Сімейний доступ зараз вимкнено.", "Family access is currently disabled.", locale))
        return not silent_invalid

    service = FamilyService(conn)
    invite = await service.get_family_invite(token)
    if invite is None or str(invite["status"] or "") != "active":
        if not silent_invalid:
            await message.reply_text(_ui_text("Це сімейне запрошення вже неактивне.", "This family invite is no longer active.", locale))
        return not silent_invalid
    expires_at = invite["expires_at"]
    if expires_at is not None and expires_at <= datetime.now(expires_at.tzinfo):
        if not silent_invalid:
            await message.reply_text(_ui_text("Термін дії цього запрошення вже минув.", "This invite has already expired.", locale))
        return not silent_invalid
    if int(invite["used_count"] or 0) >= int(invite["max_uses"] or 1):
        if not silent_invalid:
            await message.reply_text(_ui_text("Це сімейне запрошення вже використане.", "This family invite has already been used.", locale))
        return not silent_invalid
    if int(invite["owner_user_id"] or 0) == tg_user_id:
        if not silent_invalid:
            await message.reply_text(_ui_text("Ви вже власник цього сімейного бюджету.", "You already own this family budget.", locale))
        return not silent_invalid
    scope = await service.get_scope(tg_user_id)
    if scope.is_family:
        if not silent_invalid:
            await message.reply_text(_ui_text("Ви вже перебуваєте в іншій родині.", "You are already part of another family space.", locale))
        return not silent_invalid
    if int(invite["active_members_count"] or 0) >= 4:
        if not silent_invalid:
            await message.reply_text(_ui_text("У цій родині вже немає вільних місць.", "There are no free slots left in this family space.", locale))
        return not silent_invalid

    if remember_pending:
        await _sync_access_scope_state(
            conn,
            tg_user_id,
            access_scope="paywall",
            access_source="family",
            pending_start_payload=f"family_invite_{token}",
        )
    owner_name = _display_user_name(invite.get("owner_first_name"), invite.get("owner_username"))
    await message.reply_text(
        "\n".join(
            [
                _ui_text(
                    f"<b>Запрошення до сімейного бюджету {escape_html(str(invite['family_name'] or 'Сімейний бюджет'))}</b>",
                    f"<b>Invitation to family budget {escape_html(str(invite['family_name'] or 'Family Budget'))}</b>",
                    locale,
                ),
                "",
                _ui_text(f"<b>Запросив:</b> {escape_html(owner_name)}", f"<b>Invited by:</b> {escape_html(owner_name)}", locale),
                _ui_text(
                    f"<b>Учасників зараз:</b> {int(invite['active_members_count'] or 0)}/4",
                    f"<b>Members now:</b> {int(invite['active_members_count'] or 0)}/4",
                    locale,
                ),
                "",
                _ui_text(
                    "Після підтвердження ви приєднаєтесь до спільного фінансового простору.",
                    "After confirmation, you will join the shared finance space.",
                    locale,
                ),
            ]
        ),
        reply_markup=kb_family_invite_accept(token),
    )
    return True


async def _present_debt_invite_prompt(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    token: str,
    *,
    remember_pending: bool,
    silent_invalid: bool,
) -> bool:
    locale = current_ui_locale()
    service = DebtService(conn)
    invite = await service.get_debt_invite(token)
    if invite is None:
        if not silent_invalid:
            await message.reply_text(_ui_text("Це посилання вже неактивне.", "This link is no longer active.", locale))
        return not silent_invalid
    if int(invite["lender_user_id"] or 0) == tg_user_id:
        if not silent_invalid:
            await message.reply_text(_debt_invite_borrower_link_text(locale))
        return not silent_invalid
    if str(invite["invite_status"] or "") != "pending":
        if not silent_invalid:
            await message.reply_text(_ui_text("Це посилання вже неактивне.", "This link is no longer active.", locale))
        return not silent_invalid
    expires_at = invite["expires_at"]
    if expires_at is not None and expires_at <= datetime.now():
        if not silent_invalid:
            await message.reply_text(_ui_text("Це посилання вже неактивне.", "This link is no longer active.", locale))
        return not silent_invalid
    if str(invite["debt_status"] or "") in {"closed", "cancelled"} or Decimal(str(invite["remaining_amount"] or 0)) <= 0:
        if not silent_invalid:
            await message.reply_text(_ui_text("Це посилання вже неактивне.", "This link is no longer active.", locale))
        return not silent_invalid

    if remember_pending:
        await _sync_access_scope_state(
            conn,
            tg_user_id,
            access_scope="debt_only",
            access_source="debt",
            pending_start_payload=f"debt_{token}",
        )

    lender_name = _display_user_name(invite.get("lender_first_name"), invite.get("lender_username"))
    lender_name_html = escape_html(lender_name)
    currency = str(invite["currency"] or "UAH")
    text = await _get_bot_copy(
        conn,
        "debt_invite_claim_text",
        lender_name=lender_name_html,
        initial_amount=format_money(Decimal(str(invite["initial_amount"] or 0)), currency),
        counterparty_name=escape_html(_debt_comment_text(invite.get("counterparty_name"))),
        comment=escape_html(_debt_comment_text(invite.get("comment"))),
    )
    await message.reply_text(text, reply_markup=kb_debt_invite_decision(token))
    return True


async def _maybe_resume_pending_start_payload(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    access_state: dict[str, Any],
) -> bool:
    pending_payload = str(access_state.get("pending_start_payload") or "")
    if not pending_payload:
        return False

    handled = False
    if pending_payload.startswith("family_invite_"):
        handled = await _present_family_invite_prompt(
            message,
            conn,
            tg_user_id,
            pending_payload.removeprefix("family_invite_"),
            remember_pending=False,
            silent_invalid=True,
        )
    elif pending_payload.startswith("debt_"):
        handled = await _present_debt_invite_prompt(
            message,
            conn,
            tg_user_id,
            pending_payload.removeprefix("debt_"),
            remember_pending=False,
            silent_invalid=True,
        )
    else:
        return False

    if handled:
        return True
    await _clear_pending_start_payload_for_state(conn, tg_user_id, access_state)
    return False


async def _show_debt_only_surface(message, conn: asyncpg.Connection, tg_user_id: int, *, notice: str | None = None) -> None:
    await _set_locale_for_user(conn, tg_user_id)
    access_state = await _get_resolved_access_state(conn, tg_user_id)
    billing_state = access_state["billing_state"]
    locale = current_ui_locale()
    lines = [
        _ui_text("<b>🤝 Доступ лише до боргів</b>", "<b>🤝 Debts-only access</b>", locale),
        "",
        _ui_text(
            "Тут можна працювати з боргами, але повний функціонал бота відкривається після підписки.",
            "You can work with debts here, but the full bot experience opens after access is activated.",
            locale,
        ),
    ]
    if _pending_promo_code(access_state) and int(access_state.get("promo_trial_days") or 0) > 0:
        lines.extend(
            [
                "",
                _ui_text(
                    f"Повний доступ можна спробувати без оплати протягом {int(access_state['promo_trial_days'])} днів. Для запуску пробного періоду потрібна прив'язка картки через Monobank.",
                    f"You can try full access free for {int(access_state['promo_trial_days'])} days. To start the trial, link a card through Monobank.",
                    locale,
                ),
            ]
        )
    elif not billing_state.get("has_card"):
        lines.extend(
            [
                "",
                _ui_text(
                    "Повний доступ можна спробувати без оплати протягом 30 днів. Для запуску пробного періоду потрібна прив'язка картки через Monobank.",
                    "You can try full access free for 30 days. To start the trial, link a card through Monobank.",
                    locale,
                ),
            ]
        )
    text = "\n".join(lines)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_debt_only_home())


async def _show_paywall_surface(message, conn: asyncpg.Connection, tg_user_id: int, *, notice: str | None = None) -> None:
    await _set_locale_for_user(conn, tg_user_id)
    access_state = await _get_resolved_access_state(conn, tg_user_id)
    trial_days = _billing_trial_days(access_state.get("promo_trial_days") or 30)
    locale = current_ui_locale()
    lines = [
        _ui_text("<b>🔒 Доступ до бота закрито</b>", "<b>🔒 Bot access is locked</b>", locale),
        "",
        _ui_text(
            f"Прив'яжіть картку, щоб увімкнути {trial_days} днів повного доступу.",
            f"Link a card to activate {trial_days} days of full access.",
            locale,
        ),
        "",
        _ui_text("Сьогодні спишемо 1 грн для перевірки картки.", "Today we charge 1 UAH to verify the card.", locale),
        _ui_text(
            f"499 грн/міс — лише після {trial_days} днів.",
            f"499 UAH/month only after {trial_days} days.",
            locale,
        ),
    ]
    text = "\n".join(lines)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_paywall_home())


async def _show_access_surface(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    notice: str | None = None,
) -> None:
    await _set_locale_for_user(conn, tg_user_id)
    access_state = await _get_resolved_access_state(conn, tg_user_id)
    access_state, sync_result = await _maybe_sync_pending_bind_access_state(conn, tg_user_id, access_state)
    resolved_access_scope = str(access_state.get("access_scope") or "")
    if await _maybe_resume_pending_start_payload(message, conn, tg_user_id, access_state):
        return
    access_scope = str(access_state.get("access_scope") or "paywall")
    if _access_scope_has_full_home(access_scope):
        if sync_result and _monobank_status_is_success(sync_result.get("monobank_status")) and not notice:
            notice = locale_text(
                "<b>Оплату підтверджено. Доступ відкрито.</b>",
                "<b>Payment confirmed. Access is now active.</b>",
                current_ui_locale(),
            )
        home_notice = notice or _home_menu_text(current_ui_locale())
        soft_grace_notice = _billing_soft_grace_notice(dict(access_state.get("billing_state") or {}))
        if soft_grace_notice:
            home_notice = f"{soft_grace_notice}\n\n{home_notice}"
        await _reply_home(message, home_notice)
        return
    if access_scope == "debt_only":
        await _show_debt_only_surface(message, conn, tg_user_id, notice=notice)
        return
    await _show_paywall_surface(message, conn, tg_user_id, notice=notice)


async def _show_access_locked(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    notice: str,
    allow_debt_only: bool = False,
) -> bool:
    access_state = await _get_resolved_access_state(conn, tg_user_id)
    access_scope = str(access_state.get("access_scope") or "paywall")
    if _access_scope_has_full_home(access_scope):
        return False
    if access_scope == "debt_only" and allow_debt_only:
        return False
    if access_scope == "debt_only":
        await _show_debt_only_surface(message, conn, tg_user_id, notice=notice)
        return True
    await _show_billing_menu(message, conn, tg_user_id, notice=notice, access_state=access_state)
    return True


async def _show_billing_menu(
    message,
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    notice: str | None = None,
    access_state: dict[str, Any] | None = None,
) -> None:
    access_state = access_state or await _get_resolved_access_state(conn, tg_user_id)
    original_access_scope = str(access_state.get("access_scope") or "")
    access_state, sync_result = await _maybe_sync_pending_bind_access_state(conn, tg_user_id, access_state)
    resolved_access_scope = str(access_state.get("access_scope") or "")
    if (
        not _access_scope_has_full_home(original_access_scope)
        and _access_scope_has_full_home(resolved_access_scope)
        and sync_result
        and _monobank_status_is_success(sync_result.get("monobank_status"))
    ):
        success_notice = notice
        if not success_notice:
            success_notice = "<b>Оплату підтверджено. Доступ відкрито.</b>"
        await _show_access_surface(message, conn, tg_user_id, notice=success_notice)
        return
    state = dict(access_state.get("billing_state") or await get_billing_state(conn, tg_user_id))
    access_scope = str(access_state.get("access_scope") or "paywall")
    promo_trial_days = int(access_state.get("promo_trial_days") or 0)
    has_subscription_history = _billing_has_subscription_history(state)
    personal_full_without_card = access_scope == "personal_full" and not bool(state.get("has_card"))
    family_full_without_card = access_scope == "family_full" and not bool(state.get("has_card"))
    trial_eligible = not has_subscription_history
    trial_days = _billing_trial_days(promo_trial_days or state.get("trial_days") or 30)
    access_label_text = _billing_access_label(str(state.get("access_mode") or "open"))
    is_trial_offer_screen = access_scope == "paywall" and trial_eligible
    is_recovery_offer_screen = _billing_recovery_checkout_needed(access_scope=access_scope, state=state)
    is_recovery_card_screen = is_recovery_offer_screen and bool(state.get("has_card"))
    if access_scope == "paywall":
        access_label_text = "Потрібна оплата"
    elif access_scope == "debt_only":
        access_label_text = "Лише модуль боргів"
    if is_trial_offer_screen:
        lines = _billing_trial_offer_lines(trial_days)
    else:
        lines = [
            "<b>Доступ і оплата</b>",
            "",
            "Цей розділ входить у повний доступ.",
            f"Доступ: <b>{escape_html(access_label_text)}</b>",
        ]
    if access_scope == "family_full":
        lines.extend(["", "Ваш доступ надано через сімейний простір. Особиста картка для користування сімейним контуром не потрібна."])
    elif access_scope == "debt_only":
        lines.extend(["", "Зараз для цього акаунта відкрито лише розділ боргів. Щоб відкрити весь бот, оформіть персональну підписку."])
    elif is_trial_offer_screen:
        if state.get("last_action_url"):
            lines.extend(["", "Якщо Monobank уже відкритий, просто підтвердьте платіж у застосунку."])
    elif is_recovery_offer_screen:
        lines.extend(
            [
                "",
                "Щоб відновити доступ, відкрийте оплату в Monobank.",
                "",
                "Зараз буде списано 499 грн за місячний доступ.",
                "Картку, яку підтвердите в Monobank, збережемо для наступних щомісячних списань.",
            ]
        )
    else:
        lines.extend(
            [
                "",
                "Прив'яжіть або оновіть картку для майбутніх списань.",
                "",
                "1 грн зараз: перевірка картки.",
                "Пробний період повторно не нараховується.",
                "Якщо доступ уже зупинився, після прив'язки натисніть «Оплатити зараз».",
            ]
        )
    if personal_full_without_card:
        lines = [
            "<b>Доступ і оплата</b>",
            "",
            "Цей розділ входить у повний доступ.",
            f"Доступ: <b>{escape_html(access_label_text)}</b>",
            "",
            "Поточний доступ уже активний.",
            "Картку можна прив'язати або оновити пізніше для майбутніх списань.",
            "Без прив'язаної картки автопродовження не спрацює.",
        ]
    if state.get("subscription_status"):
        lines.append(f"Статус підписки: <b>{escape_html(_billing_subscription_label(str(state['subscription_status'])))}</b>")
    if state.get("trial_days") and not is_recovery_offer_screen:
        lines.append(f"Пробний період: {int(state['trial_days'])} днів")
    if not is_trial_offer_screen:
        if state.get("has_card"):
            lines.append(f"Картка: <b>{escape_html(str(state.get('masked_pan') or '—'))}</b>")
        elif family_full_without_card:
            lines.append("Особиста картка не прив'язана.")
        elif personal_full_without_card:
            lines.append("Картка не прив'язана.")
        else:
            lines.append("Картка ще не прив'язана.")
        if state.get("has_card"):
            auto_renew_text = "увімкнено" if state.get("auto_renew_enabled") else "вимкнено"
        elif family_full_without_card:
            auto_renew_text = "не використовується для цього доступу"
        elif personal_full_without_card:
            auto_renew_text = "недоступне без картки"
        else:
            auto_renew_text = "ще не активовано"
        lines.append(f"Автопродовження: <b>{auto_renew_text}</b>")
    if state.get("expires_at"):
        lines.append(f"Доступ до: <b>{_format_billing_datetime(state['expires_at'])}</b>")
    if state.get("next_charge_at"):
        lines.append(f"Наступне списання: <b>{_format_billing_datetime(state['next_charge_at'])}</b>")
    if state.get("grace_expires_at"):
        lines.append(f"Grace до: <b>{_format_billing_datetime(state['grace_expires_at'])}</b>")
    soft_grace_notice = _billing_soft_grace_notice(state)
    if soft_grace_notice:
        lines.extend(["", soft_grace_notice])
    if state.get("last_failure_reason"):
        lines.extend(["", f"Остання помилка: {escape_html(str(state['last_failure_reason']))}"])
    if state.get("last_action_url"):
        if state.get("has_card"):
            lines.extend(["", "Monobank очікує додаткове підтвердження оплати."])
        elif is_recovery_offer_screen:
            lines.extend(
                [
                    "",
                    "У Monobank вже відкрита сесія оплати.",
                    "Зараз буде списано 499 грн за місячний доступ.",
                    "Після успішної оплати цю картку збережемо для наступних щомісячних списань.",
                ]
            )
        elif not is_trial_offer_screen:
            lines.extend(["", "У Monobank вже відкрита сесія оновлення картки.", "Зараз буде списано 1 грн для перевірки картки.", "Новий пробний період не нараховується."])
    text = "\n".join(lines)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(
        text,
        reply_markup=kb_billing_menu(
            profile_exists=bool(state.get("profile_exists")),
            has_card=bool(state.get("has_card")),
            auto_renew_enabled=bool(state.get("auto_renew_enabled")),
            trial_eligible=trial_eligible,
            trial_days=trial_days,
            action_url=str(state.get("last_action_url") or "") or None,
            allow_retry=_billing_retry_allowed(state),
            bind_callback="settings:billing:bind",
            rebind_callback="settings:billing:rebind",
            bind_button_text="Оплатити 499 грн" if is_recovery_offer_screen and not state.get("has_card") else None,
            rebind_button_text="Змінити спосіб оплати і оплатити" if is_recovery_card_screen else None,
            retry_first=is_recovery_card_screen,
            show_action_url_button=not is_recovery_card_screen,
            back_callback="home:show" if access_scope in {"paywall", "debt_only"} else "settings:start",
            include_home_button=access_scope not in {"paywall", "debt_only"},
        ),
    )


async def _billing_write_blocked(message, conn: asyncpg.Connection, tg_user_id: int) -> bool:
    return await _show_access_locked(
        message,
        conn,
        tg_user_id,
        notice=(
            "<b>Доступ до цієї дії зараз закритий.</b>\n\n"
            "Щоб створювати або змінювати фінансові записи, потрібен повний доступ."
        ),
    )


async def _show_accounts_settings(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    notice: str | None = None,
    conn: asyncpg.Connection | None = None,
) -> None:
    _reset_settings_flow(context)
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            text = await _render_accounts_text(own_conn, tg_user_id)
            accounts = await _get_accounts_full(own_conn, tg_user_id)
    else:
        text = await _render_accounts_text(conn, tg_user_id)
        accounts = await _get_accounts_full(conn, tg_user_id)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_accounts_menu() if accounts else _kb_accounts_empty_menu())


def _new_account_create_flow(
    *,
    preferred_account_type: str | None = None,
    preferred_currency: str | None = None,
    origin: str = "settings",
    cancel_callback: str = "settings:acct:cancel",
    include_goal_fields: bool = False,
) -> dict[str, object]:
    flow: dict[str, object] = {"mode": "account_create", "step": "waiting_for_account_name", "account": {}}
    if preferred_account_type:
        flow["preferred_account_type"] = normalize_account_type(preferred_account_type)
        flow["account"]["account_type"] = normalize_account_type(preferred_account_type)
    if preferred_currency:
        flow["preferred_currency"] = normalize_currency(preferred_currency) or "UAH"
        flow["account"]["currency"] = normalize_currency(preferred_currency) or "UAH"
    flow["origin"] = origin
    flow["cancel_callback"] = cancel_callback
    flow["include_goal_fields"] = include_goal_fields
    return flow


def _account_create_data(context: ContextTypes.DEFAULT_TYPE) -> dict:
    settings_flow = context.user_data.setdefault("settings_flow", {})
    return settings_flow.setdefault("account", {})


def _currency_code_validation_text() -> str:
    return (
        locale_text(
            "Введіть код валюти великими латинськими літерами від 3 до 10 символів.\n\n"
            "Приклади: <code>UAH</code>, <code>USD</code>, <code>USDT</code>, <code>BTC</code>.",
            "Enter a currency code in uppercase Latin letters, from 3 to 10 characters.\n\n"
            "Examples: <code>UAH</code>, <code>USD</code>, <code>USDT</code>, <code>BTC</code>.",
            current_ui_locale(),
        )
    )


async def _ask_account_create_initial_balance(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    settings_flow = context.user_data.get("settings_flow") or {}
    account_data = _account_create_data(context)
    account_type = normalize_account_type(str(account_data.get("account_type") or settings_flow.get("preferred_account_type") or "other"))
    account_data["account_type"] = account_type
    settings_flow["step"] = "waiting_for_account_initial_balance"
    context.user_data["settings_flow"] = settings_flow
    balance_prompt = "Введіть поточний баланс рахунку.\n\nЯкщо зараз на ньому 0, просто введіть 0."
    if account_type == CREDIT_ACCOUNT_TYPE:
        balance_prompt = (
            "Введіть поточний баланс кредитки.\n\n"
            "Можна вводити 0, плюс або мінус.\n"
            "Наприклад: <code>-3500</code>, <code>0</code>, <code>250</code>."
        )
    await message.reply_text(
        balance_prompt,
        reply_markup=kb_inline_cancel(str(settings_flow.get("cancel_callback") or "settings:acct:cancel")),
    )


def _account_create_summary(account_data: dict) -> str:
    currency = normalize_currency(str(account_data.get("currency") or "UAH"))
    balance = account_data.get("initial_balance") or Decimal("0")
    account_type = normalize_account_type(str(account_data.get("account_type") or "other"))
    lines = [
        "<b>Підтвердіть створення рахунку</b>",
        "",
        f"<b>Назва:</b> {escape_html(account_data.get('name') or '—')}",
        f"<b>Валюта:</b> {escape_html(currency)}",
        f"<b>Тип:</b> {escape_html(account_type_label(account_type))}",
        f"<b>Початковий баланс:</b> {format_money(balance, currency)}",
    ]
    credit_limit = account_data.get("credit_limit")
    if isinstance(credit_limit, Decimal):
        lines.append(f"<b>Кредитний ліміт:</b> {format_money(credit_limit, currency)}")
    monthly_interest_rate = account_data.get("monthly_interest_rate")
    if isinstance(monthly_interest_rate, Decimal):
        lines.append(f"<b>% на місяць:</b> {format_decimal_value(monthly_interest_rate, places=4)}%")
    goal_amount = account_data.get("goal_amount")
    if isinstance(goal_amount, Decimal) and goal_amount > 0:
        lines.append(f"<b>Цільова сума:</b> {format_money(goal_amount, currency)}")
    goal_date = account_data.get("goal_date")
    if isinstance(goal_date, date):
        lines.append(f"<b>Дата цілі:</b> {goal_date.strftime('%d.%m.%Y')}")
    return "\n".join(lines)


def _saving_target_options(accounts: list[asyncpg.Record]) -> list[tuple[int, str]]:
    return [
        (int(row["id"]), format_account_name(row))
        for row in accounts
        if is_savings_account_type(str(row["account_type"] or "other"))
    ]


def _goal_progress_percent(balance: Decimal, goal_amount: Decimal | None) -> Decimal | None:
    if goal_amount is None:
        return None
    normalized_goal = Decimal(str(goal_amount or 0))
    if normalized_goal <= 0:
        return None
    return quantize_money(balance / normalized_goal * Decimal("100"))


def _goal_progress_lines(account: asyncpg.Record | dict, *, indent: str = "") -> list[str]:
    goal_amount = account["goal_amount"] if "goal_amount" in account else None
    progress = _goal_progress_percent(Decimal(str(account["balance"] or 0)), Decimal(str(goal_amount)) if goal_amount is not None else None)
    if progress is None:
        return []
    currency = normalize_currency(str(account["currency"] or "UAH"))
    goal_date = account["goal_date"] if "goal_date" in account else None
    goal_tail = f", до {goal_date.strftime('%d.%m.%Y')}" if isinstance(goal_date, date) else ""
    lines = [
        f"{indent}Ціль: {format_money(goal_amount, currency)}{goal_tail}",
        f"{indent}Прогрес: {format_decimal_value(progress, places=2)}%",
    ]
    if progress >= Decimal("100"):
        lines.append(f"{indent}Ціль виконана. Можна відкладати далі.")
    return lines


def _savings_target_balance_line(account: asyncpg.Record | dict) -> str:
    balance = Decimal(str(account["balance"] or 0))
    currency = normalize_currency(str(account["currency"] or "UAH"))
    goal_amount = account["goal_amount"] if "goal_amount" in account else None
    if goal_amount is None or Decimal(str(goal_amount or 0)) <= 0:
        return f"{escape_html(str(account['label'] or '—'))}: {format_money(balance, currency)}"
    return f"{escape_html(str(account['label'] or '—'))}: {format_money(balance, currency)} / {format_money(goal_amount, currency)}"


def _savings_overview_account_lines(account: asyncpg.Record | dict) -> list[str]:
    lines = [f"• {_savings_target_balance_line(account)}"]
    lines.extend(_goal_progress_lines(account, indent="  "))
    return lines


def _format_currency_totals(totals_by_currency: dict[str, Decimal]) -> str:
    if len(totals_by_currency) == 1:
        currency, total = next(iter(totals_by_currency.items()))
        return format_money(quantize_money(total), currency)
    return "; ".join(
        format_money(quantize_money(totals_by_currency[currency]), currency)
        for currency in sorted(totals_by_currency)
    )


def _saving_settings_text(settings: asyncpg.Record, default_target_label: str | None) -> str:
    percent = Decimal(str(settings["default_percent"] or 10))
    min_income_amount = settings["min_income_amount"]
    return "\n".join(
        [
            "<b>⚙️ Налаштування відкладень</b>",
            "",
            f"<b>Пропонувати після доходу:</b> {'так' if settings['ask_after_income'] else 'ні'}",
            f"<b>Основний відсоток:</b> {format_decimal_value(percent, places=2)}%",
            f"<b>Накопичення за замовчуванням:</b> {escape_html(default_target_label or '—')}",
            f"<b>Лише після зарплати:</b> {'так' if settings['ask_only_for_salary'] else 'ні'}",
            f"<b>Мінімальна сума доходу:</b> {format_money(min_income_amount, 'UAH') if min_income_amount is not None else 'без обмеження'}",
        ]
    )


async def _is_salary_category(conn: asyncpg.Connection, tg_user_id: int, category_id: int) -> bool:
    if category_id <= 0:
        return False
    category = await CategoryService(conn).getCategoryById(tg_user_id, category_id)
    if category is None or category.type != "income":
        return False
    if category.template_id is not None:
        template = await CategoryTemplateService(conn).getTemplateById(int(category.template_id))
        if template is not None and "зарплат" in normalize_category_name(template.name).casefold():
            return True
    normalized_name = normalize_category_name(category.name).casefold()
    return normalized_name in {"зарплата", "salary"}


async def _saving_after_income_text(conn: asyncpg.Connection, amount: Decimal, currency: str, percent: Decimal) -> str:
    return await _get_bot_copy(
        conn,
        "saving_after_income_text",
        amount_text=format_money(amount, currency),
        percent_text=f"{format_decimal_value(percent, places=2)}%",
        saving_amount_text=format_money(quantize_money(amount * percent / Decimal('100')), currency),
    )


def _income_saved_text(*, kind: str, amount: Decimal, currency: str, category_label: str, account_label: str, comment: str | None) -> str:
    return (
        f"<b>✅ {'Витрату' if kind == 'expense' else 'Дохід'} збережено</b>\n\n"
        f"<b>Сума:</b> {format_money(amount, currency)}\n"
        f"<b>Категорія:</b> {escape_html(category_label)}\n"
        f"<b>Рахунок:</b> {escape_html(account_label)}\n"
        f"<b>Коментар:</b> {f'<i>{escape_html(comment)}</i>' if comment else '<i>—</i>'}"
    )


async def _saving_setup_text(conn: asyncpg.Connection, amount: Decimal, currency: str, percent: Decimal) -> str:
    return await _get_bot_copy(
        conn,
        "saving_setup_text",
        amount_text=format_money(amount, currency),
        percent_text=f"{format_decimal_value(percent, places=2)}%",
        saving_amount_text=format_money(quantize_money(amount * percent / Decimal('100')), currency),
    )


async def _saving_plan_text(
    conn: asyncpg.Connection,
    *,
    amount: Decimal,
    currency: str,
    source_label: str,
    target_label: str,
) -> str:
    return await _get_bot_copy(
        conn,
        "saving_plan_text",
        amount_text=format_money(amount, currency),
        source_label=escape_html(source_label),
        target_label=escape_html(target_label),
    )


def _saving_confirm_text(*, amount: Decimal, source_label: str, target_label: str, source_currency: str, target_currency: str, fx_rate: Decimal | None = None) -> str:
    debit_text = format_money(amount, source_currency)
    credit_amount = amount if source_currency == target_currency else (
        quantize_money(amount / fx_rate) if fx_rate and source_currency == "UAH" and target_currency != "UAH" else
        quantize_money(amount * fx_rate) if fx_rate else amount
    )
    credit_text = format_money(credit_amount, target_currency)
    lines = [
        "Точно переказав(ла)?",
        "",
        "Я зараз зміню баланси:",
        f"{escape_html(source_label)}: -{debit_text}",
        f"{escape_html(target_label)}: +{credit_text}",
        "",
        "Це внутрішній переказ, він не потрапить у витрати.",
    ]
    if fx_rate is not None and source_currency != target_currency:
        lines.extend(["", f"Курс: {format_exchange_rate(source_currency, target_currency, fx_rate)}"])
    return "\n".join(lines)


def _saving_success_text(
    *,
    source_label: str,
    source_balance: Decimal,
    source_currency: str,
    target_label: str,
    target_balance: Decimal,
    target_currency: str,
    target_goal_amount: Decimal | None = None,
) -> str:
    target_account = {
        "label": target_label,
        "balance": target_balance,
        "currency": target_currency,
        "goal_amount": target_goal_amount,
    }
    lines = [
        "Готово.",
        "",
        f"{escape_html(source_label)}: {format_money(source_balance, source_currency)}",
        _savings_target_balance_line(target_account),
    ]
    progress_lines = _goal_progress_lines(target_account)
    if progress_lines:
        lines += ["", *progress_lines]
    return "\n".join(lines)


def _saving_quick_remind_at(choice: str, now: datetime) -> datetime | None:
    if choice == "1h":
        return now + timedelta(hours=1)
    if choice == "evening":
        evening = now.replace(hour=20, minute=0, second=0, microsecond=0)
        return evening if evening > now else now + timedelta(hours=1)
    if choice == "tomorrow":
        tomorrow = now + timedelta(days=1)
        return tomorrow.replace(hour=9, minute=0, second=0, microsecond=0)
    return None


def _parse_reminder_datetime_text(text: str, now: datetime) -> datetime | None:
    value = (text or "").strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%d.%m %H:%M", "%H:%M"):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        if fmt == "%d.%m %H:%M":
            parsed = parsed.replace(year=now.year)
        if fmt == "%H:%M":
            parsed = now.replace(hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0)
            if parsed <= now:
                parsed = parsed + timedelta(days=1)
        if parsed <= now:
            return None
        return parsed
    return None


async def _show_saving_settings(message, context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, *, conn: asyncpg.Connection | None = None, notice: str | None = None) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_saving_settings(message, context, tg_user_id, conn=own_conn, notice=notice)
            return
    settings = await SavingsService(conn).get_or_create_settings(tg_user_id)
    default_target_label = None
    default_target_id = settings["default_target_account_id"]
    if default_target_id:
        target = await _get_active_account_by_id(conn, tg_user_id, int(default_target_id))
        if target is not None:
            default_target_label = str(target["label"])
    text = _saving_settings_text(settings, default_target_label)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(
        text,
        reply_markup=kb_saving_settings_menu(
            ask_after_income_enabled=bool(settings["ask_after_income"]),
            ask_only_for_salary=bool(settings["ask_only_for_salary"]),
        ),
    )


async def _show_savings_overview(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_savings_overview(message, context, tg_user_id, conn=own_conn, notice=notice)
            return
    text, has_assets = await _render_savings_overview_text(conn, tg_user_id)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_savings_overview() if has_assets else kb_savings_overview_empty())


def _saving_account_picker_label(account: asyncpg.Record | dict) -> str:
    return f"{str(account['label'])} — {format_money(account['balance'], str(account['currency'] or 'UAH'))}"


def _saving_target_payload(account: asyncpg.Record | dict) -> dict[str, object]:
    goal_amount_raw = account["goal_amount"] if "goal_amount" in account else None
    goal_amount = Decimal(str(goal_amount_raw)) if goal_amount_raw is not None and Decimal(str(goal_amount_raw or 0)) > 0 else None
    goal_date = account["goal_date"] if "goal_date" in account else None
    return {
        "target_account_id": int(account["id"]),
        "target_label": str(account["label"]),
        "target_currency": normalize_currency(str(account["currency"] or "UAH")),
        "target_balance": Decimal(str(account["balance"] or 0)),
        "target_goal_amount": goal_amount,
        "goal_amount": goal_amount,
        "goal_date": goal_date,
    }


def _saving_source_payload(account: asyncpg.Record | dict, prefix: str = "source") -> dict[str, object]:
    return {
        f"{prefix}_account_id": int(account["id"]),
        f"{prefix}_label": str(account["label"]),
        f"{prefix}_currency": normalize_currency(str(account["currency"] or "UAH")),
        f"{prefix}_balance": Decimal(str(account["balance"] or 0)),
    }


def _saving_transfer_credit_amount(flow: dict) -> Decimal:
    amount = Decimal(str(flow.get("amount") or 0))
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    fx_rate = flow.get("fx_rate")
    if source_currency == target_currency:
        return amount
    if not isinstance(fx_rate, Decimal):
        return amount
    if source_currency == "UAH" and target_currency != "UAH":
        return quantize_money(amount / fx_rate)
    return quantize_money(amount * fx_rate)


async def _saving_topup_confirm_text(conn: asyncpg.Connection, flow: dict) -> str:
    lines = [
        await _get_bot_copy(
            conn,
            "saving_topup_confirm_text",
            source_label=escape_html(str(flow.get("source_label") or "—")),
            target_label=escape_html(str(flow.get("target_label") or "—")),
            amount_text=format_money(Decimal(str(flow.get("amount") or 0)), str(flow.get("source_currency") or "UAH")),
        )
    ]
    if flow.get("fx_rate") is not None and flow.get("source_currency") != flow.get("target_currency"):
        lines.extend(["", f"Курс: {format_exchange_rate(str(flow['source_currency']), str(flow['target_currency']), flow['fx_rate'])}"])
    return "\n".join(lines)


async def _saving_post_income_confirm_text(conn: asyncpg.Connection, flow: dict) -> str:
    lines = [
        await _get_bot_copy(
            conn,
            "saving_post_income_confirm_text",
            source_label=escape_html(str(flow.get("source_label") or "—")),
            target_label=escape_html(str(flow.get("target_label") or "—")),
            amount_text=format_money(Decimal(str(flow.get("amount") or 0)), str(flow.get("source_currency") or "UAH")),
        )
    ]
    if flow.get("fx_rate") is not None and flow.get("source_currency") != flow.get("target_currency"):
        lines.extend(["", f"Курс: {format_exchange_rate(str(flow['source_currency']), str(flow['target_currency']), flow['fx_rate'])}"])
    return "\n".join(lines)


def _saving_withdraw_confirm_text(flow: dict) -> str:
    lines = [
        "Записати виведення?",
        "",
        "Звідки:",
        escape_html(str(flow.get("source_label") or "—")),
        "",
        "Куди:",
        escape_html(str(flow.get("target_label") or "—")),
        "",
        "Сума:",
        format_money(Decimal(str(flow.get("amount") or 0)), str(flow.get("source_currency") or "UAH")),
        "",
        "Це внутрішній переказ, він не потрапить у доходи.",
    ]
    if flow.get("fx_rate") is not None and flow.get("source_currency") != flow.get("target_currency"):
        lines.extend(["", f"Курс: {format_exchange_rate(str(flow['source_currency']), str(flow['target_currency']), flow['fx_rate'])}"])
    return "\n".join(lines)


def _saving_detail_text(account: asyncpg.Record, history_exists: bool) -> str:
    lines = [
        f"<b>{escape_html(str(account['label'] or '—'))}</b>",
        "",
        f"Тип: {escape_html(account_type_label(str(account['account_type'] or 'other')))}",
        f"Баланс: {format_money(account['balance'], str(account['currency'] or 'UAH'))}",
    ]
    goal_lines = _goal_progress_lines(account)
    if goal_lines:
        lines.append(f"Ціль: {format_money(account['goal_amount'], str(account['currency'] or 'UAH'))}")
        if account.get("goal_date") is not None:
            lines.append(f"Дата цілі: {account['goal_date'].strftime('%d.%m.%Y')}")
        lines.extend([line for line in goal_lines if not line.startswith("Ціль:")])
    lines += ["", "Операцій поки немає." if not history_exists else "Є збережені операції по цьому рахунку."]
    return "\n".join(lines)


async def _show_saving_income_prompt(message, context: ContextTypes.DEFAULT_TYPE, flow: dict, *, setup_mode: bool = False) -> None:
    amount = Decimal(str(flow.get("income_amount") or 0))
    currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    primary_percent = Decimal(str(flow.get("primary_percent") or 10))
    primary_amount = quantize_money(amount * primary_percent / Decimal("100"))
    flow["primary_amount"] = primary_amount
    context.user_data["saving_flow"] = flow
    async with _pool(context).acquire() as conn:
        if setup_mode:
            await message.reply_text(
                await _saving_setup_text(conn, amount, currency, primary_percent),
                reply_markup=kb_saving_setup_prompt(
                    cancel_transaction_id=int(flow.get("income_transaction_id") or 0) or None,
                ),
            )
            return
        text = await _saving_after_income_text(conn, amount, currency, primary_percent)
    await message.reply_text(
        text,
        reply_markup=kb_saving_income_prompt(
            primary_label=f"Відкласти {format_money(primary_amount, currency)}",
            cancel_transaction_id=int(flow.get("income_transaction_id") or 0) or None,
        ),
    )


async def _maybe_prompt_saving_after_income(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    transaction_result,
    category_id: int,
) -> bool:
    if not _is_transaction_commit_success(transaction_result.status) or transaction_result.kind != "income":
        return False
    async with _pool(context).acquire() as conn:
        service = SavingsService(conn)
        settings = await service.get_or_create_settings(tg_user_id)
        if settings is None:
            return False
        if not bool(settings["enabled"]) or not bool(settings["ask_after_income"]):
            return False
        if bool(settings["ask_only_for_salary"]) and not await _is_salary_category(conn, tg_user_id, category_id):
            return False
        min_income_amount = settings["min_income_amount"]
        if min_income_amount is not None and Decimal(str(transaction_result.amount or 0)) < Decimal(str(min_income_amount)):
            return False
        targets = _saving_target_options(await service.get_active_target_accounts(tg_user_id))

        amount = Decimal(str(transaction_result.amount or 0))
        primary_percent = Decimal(str(settings["default_percent"] or 10))
        flow = {
            "mode": "post_income_prompt",
            "source_account_id": int(transaction_result.account["id"]),
            "source_label": str(transaction_result.account["label"]),
            "source_currency": normalize_currency(str(transaction_result.currency or "UAH")),
            "source_balance": Decimal(str(transaction_result.new_balance or 0)),
            "income_amount": amount,
            "income_transaction_id": getattr(transaction_result, "transaction_id", None),
            "primary_percent": primary_percent,
            "primary_amount": quantize_money(amount * primary_percent / Decimal("100")),
            "default_target_account_id": settings["default_target_account_id"],
        }
        if not targets:
            await _show_saving_income_prompt(message, context, flow, setup_mode=True)
            return True
        await _show_saving_income_prompt(message, context, flow)
        return True


async def _get_savings_like_accounts(conn: asyncpg.Connection, tg_user_id: int) -> list[asyncpg.Record]:
    accounts = await _get_accounts_full(conn, tg_user_id)
    return [row for row in accounts if normalize_account_type(str(row["account_type"] or "other")) in SAVINGS_ACCOUNT_TYPES]


async def _show_savings_list(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_savings_list(message, context, tg_user_id, conn=own_conn, notice=notice)
            return
    savings_accounts = await _get_savings_like_accounts(conn, tg_user_id)
    lines = ["<b>Мої рахунки накопичень</b>"]
    if not savings_accounts:
        lines += ["", "Поки немає жодного рахунку накопичень."]
        text = "\n".join(lines)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=kb_savings_overview_empty())
        return
    buttons: list[tuple[int, str]] = []
    for index, account in enumerate(savings_accounts, start=1):
        account_type = account_type_label(str(account["account_type"] or "other"))
        lines += ["", f"{index}. {escape_html(str(account['label'] or '—'))}", f"   {escape_html(account_type)} · {format_money(account['balance'], str(account['currency'] or 'UAH'))}"]
        goal_lines = _goal_progress_lines(account)
        if account["goal_amount"] is not None and Decimal(str(account["goal_amount"] or 0)) > 0:
            lines[-1] = (
                f"   {escape_html(account_type)} · "
                f"{format_money(account['balance'], str(account['currency'] or 'UAH'))} / "
                f"{format_money(account['goal_amount'], str(account['currency'] or 'UAH'))}"
            )
            lines.extend(f"   {line}" for line in goal_lines if line)
        buttons.append((int(account["id"]), str(account["label"])))
    text = "\n".join(lines)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_savings_accounts_list(buttons))


async def _savings_history_rows(conn: asyncpg.Connection, tg_user_id: int, account_id: int) -> list[asyncpg.Record]:
    scope = await get_current_finance_scope(conn, tg_user_id)
    if scope.is_family:
        return await conn.fetch(
            """
            SELECT date, amount, currency, to_amount, to_currency, from_account_id, to_account_id, transfer_subtype
            FROM transactions
            WHERE family_id=$1
              AND flow_kind='transfer'
              AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
              AND transfer_subtype IN ('savings_transfer', 'savings_withdraw')
              AND (from_account_id=$2 OR to_account_id=$2)
            ORDER BY date DESC, id DESC
            LIMIT 10
            """,
            int(scope.family_id),
            account_id,
        )
    return await conn.fetch(
        """
        SELECT date, amount, currency, to_amount, to_currency, from_account_id, to_account_id, transfer_subtype
        FROM transactions
        WHERE tg_user_id=$1
          AND family_id IS NULL
          AND flow_kind='transfer'
          AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
          AND transfer_subtype IN ('savings_transfer', 'savings_withdraw')
          AND (from_account_id=$2 OR to_account_id=$2)
        ORDER BY date DESC, id DESC
        LIMIT 10
        """,
        tg_user_id,
        account_id,
    )


async def _show_savings_detail(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    account_id: int,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_savings_detail(message, context, tg_user_id, account_id, conn=own_conn, notice=notice)
            return
    account = await _get_active_account_by_id(conn, tg_user_id, account_id)
    if account is None or not is_savings_account_type(str(account["account_type"] or "other")):
        await message.reply_text(await _get_bot_copy(conn, "savings_missing_account_text"), reply_markup=kb_savings_overview())
        return
    history_rows = await _savings_history_rows(conn, tg_user_id, account_id)
    text = _saving_detail_text(account, bool(history_rows))
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(text, reply_markup=kb_savings_detail(account_id))


async def _show_savings_history(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    account_id: int,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_savings_history(message, context, tg_user_id, account_id, conn=own_conn)
            return
    account = await _get_active_account_by_id(conn, tg_user_id, account_id)
    if account is None or not is_savings_account_type(str(account["account_type"] or "other")):
        await message.reply_text(await _get_bot_copy(conn, "savings_missing_account_text"), reply_markup=kb_savings_overview())
        return
    rows = await _savings_history_rows(conn, tg_user_id, account_id)
    lines = [f"<b>Історія: {escape_html(str(account['label'] or '—'))}</b>"]
    if not rows:
        lines += ["", "Операцій поки немає."]
    else:
        for row in rows:
            direction = "Поповнення" if int(row["to_account_id"] or 0) == account_id else "Виведення"
            money = format_money(row["to_amount"] if direction == "Поповнення" else row["amount"], row["to_currency"] if direction == "Поповнення" else row["currency"])
            lines.append(f"• {row['date'].strftime('%d.%m.%Y')}: {direction} {money}")
    await message.reply_text("\n".join(lines), reply_markup=kb_savings_history(account_id))


async def _show_manual_savings_target_picker(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_manual_savings_target_picker(message, context, tg_user_id, flow, conn=own_conn, notice=notice)
            return
    savings_accounts = await _get_savings_like_accounts(conn, tg_user_id)
    if not savings_accounts:
        await message.reply_text(await _get_bot_copy(conn, "savings_need_first_account_text"), reply_markup=kb_savings_overview_empty())
        return
    options = [(int(row["id"]), _saving_account_picker_label(row)) for row in savings_accounts]
    context.user_data["saving_flow"] = flow
    prompt = (
        _ui_text("Куди відкласти?", "Where should I set it aside?", current_ui_locale())
        if str(flow.get("mode") or "") == "post_income_prompt"
        else _ui_text("Куди поповнюємо?", "Which savings account should we top up?", current_ui_locale())
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=kb_account_choice(options, prefix="saving:topup:target", cancel_callback="saving:cancel"))


async def _show_topup_source_picker(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_topup_source_picker(message, context, tg_user_id, flow, conn=own_conn)
            return
    accounts = await _get_accounts_full(conn, tg_user_id)
    target_account_id = int(flow.get("target_account_id") or 0)
    options = [
        (int(row["id"]), _saving_account_picker_label(row))
        for row in accounts
        if int(row["id"]) != target_account_id and bool(row["is_active"])
    ]
    if not options:
        await message.reply_text(
            _ui_text(
                "Немає доступного рахунку, з якого можна списати гроші.",
                "There is no available account to take money from.",
                current_ui_locale(),
            ),
            reply_markup=kb_savings_overview(),
        )
        return
    context.user_data["saving_flow"] = flow
    await message.reply_text(
        _ui_text("Звідки списати гроші?", "Which account should we take the money from?", current_ui_locale()),
        reply_markup=kb_account_choice(options, prefix="saving:topup:source", cancel_callback="saving:cancel"),
    )


async def _show_withdraw_source_picker(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict | None = None,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_withdraw_source_picker(message, context, tg_user_id, flow, conn=own_conn)
            return
    savings_accounts = await _get_savings_like_accounts(conn, tg_user_id)
    if not savings_accounts:
        await message.reply_text(
            _ui_text(
                "Поки немає рахунків накопичень, з яких можна вивести гроші.",
                "There are no savings accounts to withdraw from yet.",
                current_ui_locale(),
            ),
            reply_markup=kb_savings_overview(),
        )
        return
    options = [(int(row["id"]), _saving_account_picker_label(row)) for row in savings_accounts]
    context.user_data["saving_flow"] = flow or {"mode": "manual_withdraw"}
    await message.reply_text(
        _ui_text("Звідки вивести?", "Which savings account should we withdraw from?", current_ui_locale()),
        reply_markup=kb_account_choice(options, prefix="saving:withdraw:source", cancel_callback="saving:cancel"),
    )


async def _show_withdraw_target_picker(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_withdraw_target_picker(message, context, tg_user_id, flow, conn=own_conn)
            return
    accounts = await _get_accounts_full(conn, tg_user_id)
    source_account_id = int(flow.get("source_account_id") or 0)
    options = [
        (int(row["id"]), _saving_account_picker_label(row))
        for row in accounts
        if int(row["id"]) != source_account_id and normalize_account_type(str(row["account_type"] or "other")) in {"main", "cash", "other"}
    ]
    if not options:
        await message.reply_text(
            _ui_text(
                "Немає рахунку, куди можна зарахувати гроші.",
                "There is no account available to receive the money.",
                current_ui_locale(),
            ),
            reply_markup=kb_savings_overview(),
        )
        return
    context.user_data["saving_flow"] = flow
    await message.reply_text(
        _ui_text("Куди зарахувати?", "Which account should receive the money?", current_ui_locale()),
        reply_markup=kb_account_choice(options, prefix="saving:withdraw:target", cancel_callback="saving:cancel"),
    )


async def _show_saving_transfer_confirmation(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    flow: dict,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    context.user_data["saving_flow"] = flow
    mode = str(flow.get("mode") or "")
    if conn is None and mode != "manual_withdraw":
        async with _pool(context).acquire() as own_conn:
            await _show_saving_transfer_confirmation(message, context, flow, conn=own_conn)
        return
    if mode == "manual_withdraw":
        text = _saving_withdraw_confirm_text(flow)
        reply_markup = kb_savings_transfer_confirm(
            confirm_label="Так, записати",
            confirm_callback="saving:withdraw:confirm:ok",
            edit_amount_callback="saving:withdraw:confirm:amount",
            cancel_callback="saving:cancel",
        )
    elif mode == "post_income_prompt":
        text = await _saving_post_income_confirm_text(conn, flow)
        reply_markup = kb_savings_transfer_confirm(
            confirm_label="Переказ зроблено, записати",
            confirm_callback="saving:topup:confirm:ok",
            edit_amount_callback="saving:topup:confirm:amount",
            cancel_callback="saving:not_now",
            change_target_callback="saving:topup:confirm:target",
        )
    else:
        text = await _saving_topup_confirm_text(conn, flow)
        reply_markup = kb_savings_transfer_confirm(
            confirm_label="Переказ зроблено, записати",
            confirm_callback="saving:topup:confirm:ok",
            edit_amount_callback="saving:topup:confirm:amount",
            cancel_callback="saving:cancel",
        )
    await message.reply_text(text, reply_markup=bind_financial_preview(flow, reply_markup))


async def _execute_saving_flow_transfer(
    conn: asyncpg.Connection,
    tg_user_id: int,
    flow: dict,
) -> tuple[object, Decimal, Decimal]:
    service = SavingsService(conn)
    mode = str(flow.get("mode") or "")
    subtype = "savings_withdraw" if mode == "manual_withdraw" else "savings_transfer"
    result = await service.record_completed_transfer(
        tg_user_id,
        source_account_id=int(flow["source_account_id"]),
        target_account_id=int(flow["target_account_id"]),
        source_currency=normalize_currency(str(flow["source_currency"] or "UAH")),
        target_currency=normalize_currency(str(flow["target_currency"] or "UAH")),
        amount=Decimal(str(flow["amount"] or 0)),
        fx_rate=flow.get("fx_rate"),
        rate_source=str(flow.get("rate_source") or "") or None,
        transfer_subtype=subtype,
        comment="saving withdraw" if subtype == "savings_withdraw" else "saving transfer",
    )
    source_balance = quantize_money((result.source_balance or Decimal("0")) - Decimal(str(flow["amount"] or 0)))
    target_balance = quantize_money(Decimal(str(flow.get("target_balance") or 0)) + Decimal(str(result.target_amount or 0)))
    return result, source_balance, target_balance

async def _show_saving_target_picker(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_saving_target_picker(message, context, tg_user_id, flow, conn=own_conn, notice=notice)
            return
    options = _saving_target_options(await SavingsService(conn).get_active_target_accounts(tg_user_id))
    if not options:
        await message.reply_text(
            _ui_text(
                "Спочатку створіть рахунок для накопичень, депозит або інвестиційний рахунок.",
                "First create a savings, deposit, or investment account.",
                current_ui_locale(),
            ),
            reply_markup=kb_saving_setup_prompt(),
        )
        return
    context.user_data["saving_flow"] = flow
    prompt = _ui_text("Куди відкладаємо?", "Which savings target should we use?", current_ui_locale())
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=kb_saving_target_accounts(options))


def _saving_default_target_id(flow: dict, options: list[tuple[int, str]]) -> int | None:
    option_ids = {account_id for account_id, _label in options}
    default_target_id = flow.get("default_target_account_id")
    if default_target_id is not None:
        try:
            normalized_id = int(default_target_id)
        except (TypeError, ValueError):
            normalized_id = 0
        if normalized_id in option_ids:
            return normalized_id
    if len(options) == 1:
        return options[0][0]
    return None


def _should_preserve_saving_flow_for_account_create(flow: dict | None) -> bool:
    mode = str((flow or {}).get("mode") or "")
    return mode in {"post_income_prompt", "manual_topup", "create_plan"}


async def _show_existing_account_picker_for_saving(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_existing_account_picker_for_saving(message, context, tg_user_id, conn=own_conn)
            return
    saving_flow = context.user_data.get("saving_flow") or {}
    source_account_id = int(saving_flow.get("source_account_id") or 0)
    accounts = await _get_accounts_full(conn, tg_user_id)
    options = [
        (int(row["id"]), format_account_name(row))
        for row in accounts
        if int(row["id"]) != source_account_id and not is_savings_account_type(str(row["account_type"] or "other"))
    ]
    if not options:
        await message.reply_text(
            _ui_text(
                "Немає іншого рахунку, який можна перевести в накопичення. Створіть новий рахунок для накопичень.",
                "There is no other account you can convert into savings. Create a new savings account.",
                current_ui_locale(),
            ),
            reply_markup=kb_saving_setup_prompt(),
        )
        return
    await message.reply_text(
        _ui_text(
            "Оберіть рахунок, який хочете використовувати для накопичень:",
            "Choose the account you want to use for savings:",
            current_ui_locale(),
        ),
        reply_markup=kb_account_choice(options, prefix="saving:existing", cancel_callback="saving:cancel"),
    )


async def _create_pending_saving_plan(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    target_account: asyncpg.Record,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _create_pending_saving_plan(message, context, tg_user_id, flow, target_account, conn=own_conn)
            return
    amount = flow.get("amount")
    if not isinstance(amount, Decimal):
        await message.reply_text("Спочатку вкажіть суму.")
        return
    task = await SavingsService(conn).create_pending_task(
        tg_user_id,
        income_transaction_id=flow.get("income_transaction_id"),
        source_account_id=int(flow["source_account_id"]),
        target_account_id=int(target_account["id"]),
        amount=amount,
        currency=normalize_currency(str(flow.get("currency") or flow.get("source_currency") or "UAH")),
        percent_from_income=flow.get("percent_from_income"),
        income_amount=flow.get("income_amount"),
    )
    _reset_saving_flow(context)
    await message.reply_text(
        await _saving_plan_text(
            conn,
            amount=amount,
            currency=normalize_currency(str(flow.get("currency") or "UAH")),
            source_label=str(flow.get("source_label") or "—"),
            target_label=str(target_account["label"] or "—"),
        ),
        reply_markup=kb_saving_plan_actions(task.id),
    )


def _account_create_title(account_type: str) -> str:
    labels = {
        "main": "💼 Новий звичайний рахунок",
        "cash": "💵 Нова готівка",
        "savings": "🐿️ Нове накопичення",
        "deposit": "🏦 Новий депозит",
        "investment": "📈 Новий інвестиційний рахунок",
        "credit": "💳 Нова кредитка",
        "other": "➕ Новий рахунок",
    }
    return labels.get(normalize_account_type(account_type), "➕ Новий рахунок")


def _account_create_intro_text(account_type: str, *, include_goal_fields: bool) -> str:
    title = _account_create_title(account_type)
    lines = [f"<b>{title}</b>", "Крок <b>1 з 3</b>. Введіть назву рахунку.", ""]
    normalized_type = normalize_account_type(account_type)
    if normalized_type == "savings":
        lines.extend(
            [
                "Наприклад:",
                "<code>Банка \"Подушка\"</code>",
                "<code>Накопичення на відпустку</code>",
                "<code>Сейф USD</code>",
            ]
        )
    elif normalized_type == CREDIT_ACCOUNT_TYPE:
        lines.extend(
            [
                "Наприклад:",
                "<code>Monobank кредитка</code>",
                "<code>Visa Credit</code>",
                "<code>Кредитка USD</code>",
                "",
                "<i>Далі я попрошу поточний баланс, кредитний ліміт і % на місяць.</i>",
            ]
        )
    else:
        lines.extend(
            [
                "Наприклад:",
                "<code>Monobank</code>",
                "<code>Готівка</code>",
                "<code>Карта ФОП</code>",
            ]
        )
    if include_goal_fields:
        lines.extend(["", "<i>Далі можна одразу додати цільову суму і дату, якщо вони вже відомі.</i>"])
    return "\n".join(lines)


async def _show_saving_rate_step(message, flow: dict, notice: str | None = None) -> None:
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    mode = str(flow.get("mode") or "")
    title = "💱 Виведення з накопичення" if mode == "manual_withdraw" else "💱 Переказ у накопичення"
    prompt = "\n".join(
        [
            f"<b>{title}</b>",
            "Введіть курс для підтвердження переказу.",
            "",
            f"<b>Звідки:</b> {escape_html(str(flow.get('source_label') or '—'))} ({escape_html(source_currency)})",
            f"<b>Куди:</b> {escape_html(str(flow.get('target_label') or '—'))} ({escape_html(target_currency)})",
            "",
            build_transfer_rate_prompt(source_currency, target_currency),
        ]
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=kb_inline_cancel("saving:cancel"))


async def _start_account_create_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    account_type: str = "main",
    preferred_currency: str | None = None,
    include_goal_fields: bool = False,
    origin: str = "settings",
    cancel_callback: str = "settings:acct:cancel",
    preserve_saving_flow: bool = False,
) -> None:
    saved_saving_flow = context.user_data.get("saving_flow") if preserve_saving_flow else None
    _reset_runtime_flows(context)
    if preserve_saving_flow and saved_saving_flow:
        context.user_data["saving_flow"] = saved_saving_flow
    normalized_type = normalize_account_type(account_type)
    context.user_data["settings_flow"] = _new_account_create_flow(
        preferred_account_type=normalized_type,
        preferred_currency=preferred_currency,
        origin=origin,
        cancel_callback=cancel_callback,
        include_goal_fields=include_goal_fields,
    )
    await message.reply_text(
        _account_create_intro_text(normalized_type, include_goal_fields=include_goal_fields),
        reply_markup=kb_inline_cancel(cancel_callback),
    )


async def _start_savings_account_create_flow(message, context: ContextTypes.DEFAULT_TYPE) -> None:
    _reset_settings_flow(context)
    _reset_saving_settings_flow(context)
    await _start_account_create_flow(
        message,
        context,
        account_type="savings",
        include_goal_fields=True,
        origin="saving",
        cancel_callback="saving:cancel",
        preserve_saving_flow=True,
    )


async def _render_onboarding_summary(conn: asyncpg.Connection, tg_user_id: int, onb_state: dict) -> str:
    user = await _get_user(conn, tg_user_id)
    accounts_text = await _render_accounts_text(conn, tg_user_id)
    _ = onb_state
    start_date = user.get("start_date") if user else None
    return "\n".join(
        [
            "РљСЂРѕРє 5 Р· 5: РїС–РґС‚РІРµСЂРґР¶РµРЅРЅСЏ.",
            "",
            "РџРµСЂРµРІС–СЂС‚Рµ РЅР°Р»Р°С€С‚СѓРІР°РЅРЅСЏ:",
            f"- РњРѕРІР°: {_language_label(user.get('lang') if user else None)}",
            f"- Р‘Р°Р·РѕРІР° РІР°Р»СЋС‚Р°: {(user.get('base_currency') if user else None) or 'вЂ”'}",
            f"- РЎС‚Р°СЂС‚РѕРІР° РґР°С‚Р°: {start_date.isoformat() if start_date else 'вЂ”'}",
            "",
            accounts_text,
            "",
            "РњРё РІР¶Рµ РїС–РґРіРѕС‚СѓРІР°Р»Рё СЃС‚Р°РЅРґР°СЂС‚РЅС– РєР°С‚РµРіРѕСЂС–С— РІРёС‚СЂР°С‚. Р‘РѕС‚ СЃР°Рј СЂРѕР·РєР»Р°РґРµ РІРёС‚СЂР°С‚Рё, Р° С‚Рё Р·РјРѕР¶РµС€ РІРёРїСЂР°РІРёС‚Рё РєР°С‚РµРіРѕСЂС–СЋ РїС–СЃР»СЏ РґРѕРґР°РІР°РЅРЅСЏ.",
            "",
            "РЇРєС‰Рѕ РІСЃРµ РіР°СЂР°Р·Рґ, РїС–РґС‚РІРµСЂРґСЊС‚Рµ. РЇРєС‰Рѕ РЅС–, РјРѕР¶РµС‚Рµ РїРѕРІРµСЂРЅСѓС‚РёСЃСЏ С– РІС–РґСЂРµРґР°РіСѓРІР°С‚Рё СЂР°С…СѓРЅРєРё.",
        ]
    )



async def _show_onboarding_summary(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        summary = await _render_onboarding_summary(conn, tg_user_id, context.user_data.get("onb", {}))
    await message.reply_text(summary, reply_markup=onboarding_confirm_keyboard())
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/confirm",
        last_bot_response="Крок 5 з 5: підтвердження.",
    )
    return ONB_CONFIRM


async def _start_accounts_step(message, context: ContextTypes.DEFAULT_TYPE, force_add: bool = False) -> int:
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        accounts = await _get_accounts(conn, tg_user_id)

    if accounts and not force_add:
        lines = [
            "Крок 4 з 5: рахунки.",
            "",
            "Ось рахунки, які вже налаштовані. Можете додати ще один або завершити цей крок.",
            "",
        ]
        lines.extend(f"- {label}" for _, label in accounts)
        await message.reply_text("\n".join(lines), reply_markup=kb_onb_accounts_more_done())
        await _sync_onboarding_debug(
            context,
            tg_user_id,
            step="onboarding/add_accounts",
            last_bot_response="Крок 4 з 5: рахунки.",
        )
        return ACC_MORE_DONE

    next_number = len(accounts) + 1
    context.user_data.setdefault("onb", {})["current_account"] = {}
    await message.reply_text(
        f"Крок 4 з 5: додайте рахунок №{next_number}.\n\nОберіть банк або готівку.",
        reply_markup=kb_onb_account_bank(),
    )
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/add_accounts",
        last_bot_response=f"Крок 4 з 5: додайте рахунок №{next_number}.",
    )
    return ACC_BANK


async def _handle_debt_invite_start(update: Update, context: ContextTypes.DEFAULT_TYPE, token: str) -> int:
    message = update.message
    user = update.effective_user
    if not message or not user:
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        await _present_debt_invite_prompt(
            message,
            conn,
            user.id,
            token,
            remember_pending=True,
            silent_invalid=False,
        )
    return ConversationHandler.END


async def _begin_onboarding_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection,
    source: str,
    raw_input: str,
    parsed_result: dict[str, Any],
) -> int:
    context.user_data["onb"] = {}
    set_current_locale(_current_context_locale(context))
    await log_bot_event(
        conn,
        tg_user_id,
        "onboarding_started",
        source=source,
        raw_input=raw_input,
        parsed_result=parsed_result,
    )
    welcome_text = await _get_bot_copy(conn, "onboarding_welcome_text", locale=current_ui_locale())
    await message.reply_text(welcome_text, reply_markup=kb_language())
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/select_language",
        last_bot_response="Крок 1 з 5: оберіть мову.",
    )
    return LANG


async def start_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not user or not update.message:
        return ConversationHandler.END

    context.user_data["tg_user_id"] = user.id
    _current_context_locale(context, getattr(user, "language_code", None))
    await _consume_pending_admin_reset_if_needed(context, user.id)
    _reset_onboarding(context)
    _reset_runtime_flows(context)
    start_payload = _extract_start_payload(update, context)

    is_new_user = False
    maintenance_message = None
    registration_open = True
    access_state: dict[str, Any] | None = None
    async with _pool(context).acquire() as conn:
        if await is_user_banned(conn, user.id):
            await update.message.reply_text(_access_restricted_text(current_ui_locale()))
            return ConversationHandler.END
        telegram_locale = normalize_ui_locale(getattr(user, "language_code", None))
        is_new_user = not bool(await conn.fetchval("SELECT 1 FROM users WHERE tg_user_id=$1", user.id))
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        registration_open = await is_registration_open(conn)
        await conn.execute(
            """
            INSERT INTO users (tg_user_id, first_name, last_name, username, lang, base_currency, last_seen_at)
            VALUES ($1, $2, $3, $4, $5, COALESCE($6, 'UAH'), now())
            ON CONFLICT (tg_user_id) DO UPDATE SET
              first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
              username = EXCLUDED.username,
              lang = COALESCE(EXCLUDED.lang, users.lang),
              base_currency = COALESCE(users.base_currency, EXCLUDED.base_currency),
              last_seen_at = now();
            """,
            user.id,
            user.first_name,
            user.last_name,
            user.username,
            telegram_locale,
            "UAH",
        )
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=telegram_locale)
        await log_bot_event(
            conn,
            user.id,
            "start",
            source="command",
            raw_input="/start",
            parsed_result={"language_code": user.language_code or ""},
        )
        if is_new_user:
            await log_bot_event(
                conn,
                user.id,
                "new_user_registered",
                source="command",
                raw_input="/start",
                parsed_result={"username": user.username or "", "first_name": user.first_name or ""},
            )
        marketing_start_state = _marketing_start_payload_state(start_payload)
        if marketing_start_state:
            await _sync_access_scope_state(
                conn,
                user.id,
                access_scope="paywall",
                access_source=str(marketing_start_state["access_source"] or "promo"),
                pending_start_payload=str(marketing_start_state["pending_start_payload"] or start_payload),
                source=str(marketing_start_state["source"] or ""),
                referral_code=str(marketing_start_state["referral_code"] or ""),
            )
        ready = await _user_ready(conn, user.id)
        access_state = await _get_resolved_access_state(conn, user.id)

    if maintenance_message:
        await update.message.reply_text(maintenance_message)
        return ConversationHandler.END
    if start_payload.startswith("family_invite_"):
        return await _handle_family_invite_start(update, context, start_payload.removeprefix("family_invite_"))
    if start_payload.startswith("debt_"):
        return await _handle_debt_invite_start(update, context, start_payload.removeprefix("debt_"))
    if str((access_state or {}).get("pending_start_payload") or ""):
        async with _pool(context).acquire() as conn:
            if await _maybe_resume_pending_start_payload(update.message, conn, user.id, access_state or {}):
                return ConversationHandler.END
    if is_new_user and not registration_open:
        await update.message.reply_text(
            locale_text(
                "Реєстрацію тимчасово призупинено. Спробуйте ще раз пізніше.",
                "Registration is temporarily paused. Please try again later.",
                current_ui_locale(),
            )
        )
        return ConversationHandler.END

    force_onboarding = bool(context.user_data.pop("force_onboarding", False))
    if ready and not force_onboarding:
        async with _pool(context).acquire() as conn:
            access_scope = str((access_state or {}).get("access_scope") or "paywall")
            if access_scope == "debt_only":
                await _show_debt_only_surface(update.message, conn, user.id)
            elif access_scope == "paywall" or start_payload == "app_login":
                await _show_miniapp_entry(update.message, conn, user.id)
            else:
                await _show_access_surface(update.message, conn, user.id)
        return ConversationHandler.END
    if not force_onboarding and str((access_state or {}).get("access_scope") or "") == "debt_only":
        async with _pool(context).acquire() as conn:
            await _show_debt_only_surface(
                update.message,
                conn,
                user.id,
                notice=(
                    locale_text(
                        "<b>Для цього акаунта відкрито лише розділ боргів.</b>\n\n"
                        "Можете працювати з боргами без персональної підписки.",
                        "<b>Only the debts section is open for this account.</b>\n\n"
                        "You can work with debts without a personal subscription.",
                        current_ui_locale(),
                    )
                ),
            )
        return ConversationHandler.END

    if is_new_user:
        await notify_admins(
            context.application.bot,
            format_new_user_notification(
                tg_user_id=user.id,
                username=user.username,
                first_name=user.first_name,
            ),
        )

    if not force_onboarding:
        async with _pool(context).acquire() as conn:
            if await _show_miniapp_onboarding_entry(update.message, conn, user.id):
                return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        return await _begin_onboarding_flow(
            update.message,
            context,
            user.id,
            conn=conn,
            source="onboarding",
            raw_input="/start",
            parsed_result={"version": CURRENT_ONBOARDING_VERSION},
        )

async def restart_onboarding_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["force_onboarding"] = True
    if update.message:
        await update.message.reply_text(await _get_bot_copy_from_context(context, "onboarding_restart_notice_text"))
    return await start_entry(update, context)


async def onb_lang(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return LANG
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    lang = normalize_ui_locale(q.data.rsplit(":", 1)[-1])
    context.user_data.setdefault("onb", {})["lang"] = lang
    context.user_data["locale"] = lang
    set_current_locale(lang)
    await q.message.reply_text(
        await _get_bot_copy_from_context(context, "onboarding_currency_step_text"),
        reply_markup=kb_currency("onb:cur"),
    )
    tg_user_id = context.user_data.get("tg_user_id")
    if tg_user_id:
        await _sync_onboarding_debug(
            context,
            tg_user_id,
            step="onboarding/select_currency",
            last_user_input=lang,
            last_bot_response="Крок 2 з 5: базова валюта.",
        )
    return BASE_CURRENCY


async def onb_privacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return LANG
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    if q.data == "onb:privacy:back":
        async with _pool(context).acquire() as conn:
            welcome_text = await _get_bot_copy(conn, "onboarding_welcome_text", locale=_current_context_locale(context))
        await q.message.reply_text(welcome_text, reply_markup=kb_language())
        return LANG

    if q.data == "onb:privacy:policy" and not config.PRIVACY_POLICY_URL:
        await q.message.reply_text(_privacy_policy_missing_text(_current_context_locale(context)), reply_markup=kb_onb_privacy())
        return LANG

    await q.message.reply_text(
        _onboarding_privacy_text(_current_context_locale(context)),
        reply_markup=kb_onb_privacy(config.PRIVACY_POLICY_URL),
    )
    return LANG


async def _save_base_currency(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    currency = normalize_currency(value)
    if not is_valid_currency_code(currency):
        await message.reply_text(_currency_code_validation_text())
        return BASE_CURRENCY
    context.user_data.setdefault("onb", {})["base_currency"] = currency
    await message.reply_text(
            await _get_bot_copy_from_context(context, "onboarding_start_date_step_text"),
            reply_markup=kb_onb_start_date(),
        )
    tg_user_id = context.user_data.get("tg_user_id")
    if tg_user_id:
        await _sync_onboarding_debug(
                context,
                tg_user_id,
                step="onboarding/select_start_date",
                last_user_input=currency,
                last_bot_response="РљСЂРѕРє 3 Р· 6: СЃС‚Р°СЂС‚РѕРІР° РґР°С‚Р°.",
            )
    return START_DATE


async def onb_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return BASE_CURRENCY
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text(_currency_code_validation_text())
        return BASE_CURRENCY
    return await _save_base_currency(q.message, context, value)


async def onb_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return BASE_CURRENCY
    return await _save_base_currency(update.message, context, update.message.text)


async def _save_start_date(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    parsed = _parse_date_ddmmyyyy(value)
    if not parsed:
        await message.reply_text(
            locale_text(
                "Не вдалося розпізнати дату. Спробуйте формат 27.04.2026 або напишіть 'сьогодні'.",
                "Couldn't recognize the date. Try the format 27.04.2026 or type 'today'.",
                _current_context_locale(context),
            )
        )
        return START_DATE

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text(_user_not_found_text(_current_context_locale(context)))
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
        await sync_onboarding_debug_state(
            conn,
            tg_user_id,
            current_fsm_state="onboarding/add_accounts",
            onboarding_payload=_serialized_onboarding_payload(context),
            last_user_input=value,
            last_bot_response="Крок 4 з 5: рахунки.",
        )

    return await _start_accounts_step(message, context)


async def onb_start_date_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return START_DATE
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    value = q.data.rsplit(":", 1)[-1]
    if value == "pick":
        await q.message.reply_text(
            locale_text(
                "Введіть дату у форматі <code>dd.mm.yyyy</code>, наприклад <code>27.04.2026</code>.",
                "Enter a date in <code>dd.mm.yyyy</code> format, for example <code>27.04.2026</code>.",
                _current_context_locale(context),
            )
        )
        return START_DATE
    return await _save_start_date(q.message, context, locale_text("сьогодні", "today", _current_context_locale(context)))


async def onb_start_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return START_DATE
    return await _save_start_date(update.message, context, update.message.text)


async def onb_account_bank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_BANK
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    bank_key = q.data.rsplit(":", 1)[-1]
    label_map = {
        "mono": "Monobank",
        "privat": "ПриватБанк",
        "cash": "Готівка",
    }

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["bank_key"] = bank_key

    if bank_key == "other":
        await q.message.reply_text("Введіть назву банку або рахунку текстом, наприклад <code>Райф</code>.")
        return ACC_BANK_TEXT

    current["label_base"] = label_map[bank_key]
    if bank_key == "cash":
        current["requested_account_type"] = "cash"
        current["last4"] = None
        await q.message.reply_text(
            "Це готівка, тому номер картки не потрібен.\n\nТепер оберіть валюту цього рахунку.",
            reply_markup=kb_currency("onb:acct:cur"),
        )
        return ACC_CURRENCY

    await q.message.reply_text("Оберіть тип рахунку:", reply_markup=kb_onb_account_type())
    return ACC_ACCOUNT_TYPE


async def onb_account_bank_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BANK_TEXT

    label = update.message.text.strip()[:40]
    if len(label) < 2:
        await update.message.reply_text("Назва занадто коротка. Введіть щось на кшталт <code>Райф</code> або <code>Wise</code>.")
        return ACC_BANK_TEXT

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["label_base"] = label
    await update.message.reply_text("Оберіть тип рахунку:", reply_markup=kb_onb_account_type())
    return ACC_ACCOUNT_TYPE


async def onb_account_type_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_ACCOUNT_TYPE
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    choice = normalize_account_type(q.data.rsplit(":", 1)[-1])
    if choice not in {"main", CREDIT_ACCOUNT_TYPE}:
        await q.message.reply_text("Оберіть тип рахунку кнопкою нижче.", reply_markup=kb_onb_account_type())
        return ACC_ACCOUNT_TYPE

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["requested_account_type"] = choice
    await q.message.reply_text(OPTIONAL_CARD_LAST4_TEXT, reply_markup=kb_onb_account_last4())
    return ACC_LAST4_CHOICE


async def _go_to_account_currency(message, context: ContextTypes.DEFAULT_TYPE, last4: str | None) -> int:
    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["last4"] = last4
    await message.reply_text(
        "Добре. Тепер оберіть валюту цього рахунку.\n\n"
        "Якщо валюти немає в кнопках, введіть її 3 літерами текстом.",
        reply_markup=kb_currency("onb:acct:cur"),
    )
    return ACC_CURRENCY


async def onb_account_last4_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_LAST4_CHOICE
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    choice = q.data.rsplit(":", 1)[-1]
    if choice == "skip":
        return await _go_to_account_currency(q.message, context, None)

    await q.message.reply_text(
        "Введіть рівно 4 цифри, наприклад <code>1234</code>.\n\n"
        "Якщо передумали, натисніть «Пропустити».",
    )
    return ACC_LAST4_TEXT


async def _save_last4(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    last4 = (value or "").strip()
    if len(last4) != 4 or not last4.isdigit():
        await message.reply_text(
            "⚠️ Тут потрібні рівно 4 цифри, напр. <code>1234</code>.\n"
            "Якщо картка без цифр або вони не потрібні, натисніть «Пропустити».",
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
    currency = normalize_currency(value)
    if not is_valid_currency_code(currency):
        await message.reply_text(_currency_code_validation_text())
        return ACC_CURRENCY
    if not is_valid_currency_code(currency):
        await message.reply_text(_currency_code_validation_text())
        return ACC_CURRENCY
    if len(currency) != 3 or not currency.isalpha():
        await message.reply_text("Введіть 3 літери валюти, наприклад UAH, USD або EUR.")
        return ACC_CURRENCY

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["currency"] = currency
    requested_account_type = normalize_account_type(str(current.get("requested_account_type") or "main"))
    balance_prompt = (
        "РўРµРїРµСЂ РІРІРµРґС–С‚СЊ СЃС‚Р°СЂС‚РѕРІРёР№ Р±Р°Р»Р°РЅСЃ С†СЊРѕРіРѕ СЂР°С…СѓРЅРєСѓ.\n\n"
        "РџСЂРёРєР»Р°РґРё: <code>0</code>, <code>5000</code>."
    )
    if requested_account_type == CREDIT_ACCOUNT_TYPE:
        balance_prompt = (
            "Введіть стартовий баланс кредитки.\n\n"
            "Можна вводити 0, плюс або мінус.\n"
            "Приклади: <code>-100</code>, <code>0</code>, <code>500</code>."
        )
    await message.reply_text(
        "Тепер введіть стартовий баланс цього рахунку.\n\n"
        "Приклади: <code>0</code>, <code>5000</code>, <code>-100</code>.",
    )
    tg_user_id = context.user_data.get("tg_user_id")
    if tg_user_id:
        await _sync_onboarding_debug(
            context,
            tg_user_id,
            step="onboarding/add_accounts",
            last_user_input=currency,
            last_bot_response="Введіть стартовий баланс рахунку.",
        )
    return ACC_BALANCE


async def onb_account_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_CURRENCY
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text("Введіть валюту 3 літерами, наприклад <code>PLN</code> або <code>GBP</code>.")
        return ACC_CURRENCY
    if value == "OTHER":
        await q.message.reply_text(_currency_code_validation_text())
        return ACC_CURRENCY
    return await _save_account_currency(q.message, context, value)


async def onb_account_currency_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_CURRENCY
    return await _save_account_currency(update.message, context, update.message.text)


async def _finish_onboarding_account_create(message, context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, current: dict, balance: Decimal) -> int:
    label = current["label_base"]
    if current.get("last4"):
        label = f"{label} •{current['last4']}"
    requested_account_type = normalize_account_type(str(current.get("requested_account_type") or "main"))

    async with _pool(context).acquire() as conn:
        await AccountService(conn).create_account(
            tg_user_id,
            label=label,
            currency=current["currency"],
            account_type=requested_account_type,
            starting_balance=balance,
            credit_limit=current.get("credit_limit"),
            monthly_interest_rate=current.get("monthly_interest_rate"),
            non_negative_account_type="main" if requested_account_type == CREDIT_ACCOUNT_TYPE else requested_account_type,
        )
        accounts = await _get_accounts(conn, tg_user_id)

    context.user_data["onb"]["current_account"] = {}
    await message.reply_text(
        f"✅ Додано рахунок ({len(accounts)}/{ONBOARDING_ACCOUNT_LIMIT}): {label} ({current['currency']}), баланс {_format_decimal(balance)}",
        reply_markup=kb_onb_accounts_more_done(),
    )
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/add_accounts",
        last_user_input=str(balance),
        last_bot_response="Рахунок додано.",
    )
    return ACC_MORE_DONE


async def onb_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return BASE_CURRENCY
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text(_currency_code_validation_text())
        return BASE_CURRENCY
    return await _save_base_currency(q.message, context, value)


async def _go_to_account_currency(message, context: ContextTypes.DEFAULT_TYPE, last4: str | None) -> int:
    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["last4"] = last4
    await message.reply_text(
        "Добре. Тепер оберіть валюту цього рахунку.\n\n"
        "Якщо валюти немає в кнопках, введіть код валюти великими латинськими літерами від 3 до 10 символів.",
        reply_markup=kb_currency("onb:acct:cur"),
    )
    return ACC_CURRENCY


async def _save_account_currency(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    currency = normalize_currency(value)
    if not is_valid_currency_code(currency):
        await message.reply_text(_currency_code_validation_text())
        return ACC_CURRENCY

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["currency"] = currency
    requested_account_type = normalize_account_type(str(current.get("requested_account_type") or "main"))
    balance_prompt = (
        "Тепер введіть стартовий баланс цього рахунку.\n\n"
        "Приклади: <code>0</code>, <code>5000</code>."
    )
    if requested_account_type == CREDIT_ACCOUNT_TYPE:
        balance_prompt = (
            "Введіть стартовий баланс кредитки.\n\n"
            "Можна вводити 0, плюс або мінус.\n"
            "Приклади: <code>-100</code>, <code>0</code>, <code>500</code>."
        )
    await message.reply_text(balance_prompt)
    tg_user_id = context.user_data.get("tg_user_id")
    if tg_user_id:
        await _sync_onboarding_debug(
            context,
            tg_user_id,
            step="onboarding/add_accounts",
            last_user_input=currency,
            last_bot_response="Введіть стартовий баланс рахунку.",
        )
    return ACC_BALANCE


async def onb_account_currency_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_CURRENCY
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    value = q.data.rsplit(":", 1)[-1]
    if value == "OTHER":
        await q.message.reply_text(_currency_code_validation_text())
        return ACC_CURRENCY
    return await _save_account_currency(q.message, context, value)


async def onb_account_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BALANCE

    balance = _parse_decimal(update.message.text)
    if balance is None:
        await update.message.reply_text("Не бачу коректну суму. Приклад: <code>5000</code> або <code>-100</code>.")
        return ACC_BALANCE

    tg_user_id = context.user_data.get("tg_user_id")
    current = context.user_data.setdefault("onb", {}).get("current_account") or {}
    if not tg_user_id or not current.get("label_base") or not current.get("currency"):
        await update.message.reply_text("Щось пішло не так на кроці з рахунком. Натисніть /start і спробуйте ще раз.")
        return ConversationHandler.END

    label = current["label_base"]
    if current.get("last4"):
        label = f"{label} •{current['last4']}"
    account_type = "cash" if str(current.get("bank_key") or "") == "cash" else "main"

    async with _pool(context).acquire() as conn:
        await AccountService(conn).create_account(
            tg_user_id,
            label=label,
            currency=current["currency"],
            account_type=account_type,
            starting_balance=balance,
        )
        accounts = await _get_accounts(conn, tg_user_id)

    context.user_data["onb"]["current_account"] = {}
    await update.message.reply_text(
        f"✅ Додано рахунок ({len(accounts)}/{ONBOARDING_ACCOUNT_LIMIT}): {label} ({current['currency']}), баланс {_format_decimal(balance)}",
        reply_markup=kb_onb_accounts_more_done(),
    )
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/add_accounts",
        last_user_input=update.message.text,
        last_bot_response="Рахунок додано.",
    )
    return ACC_MORE_DONE


async def _save_account_currency(message, context: ContextTypes.DEFAULT_TYPE, value: str) -> int:
    currency = normalize_currency(value)
    if len(currency) != 3 or not currency.isalpha():
        await message.reply_text("Введіть 3 літери валюти, наприклад UAH, USD або EUR.")
        return ACC_CURRENCY

    current = context.user_data.setdefault("onb", {}).setdefault("current_account", {})
    current["currency"] = currency
    requested_account_type = normalize_account_type(str(current.get("requested_account_type") or "main"))
    balance_prompt = (
        "Тепер введіть стартовий баланс цього рахунку.\n\n"
        "Приклади: <code>0</code>, <code>5000</code>."
    )
    if requested_account_type == CREDIT_ACCOUNT_TYPE:
        balance_prompt = (
            "Введіть стартовий баланс кредитки.\n\n"
            "Можна вводити 0, плюс або мінус.\n"
            "Приклади: <code>-100</code>, <code>0</code>, <code>500</code>."
        )
    await message.reply_text(balance_prompt)
    tg_user_id = context.user_data.get("tg_user_id")
    if tg_user_id:
        await _sync_onboarding_debug(
            context,
            tg_user_id,
            step="onboarding/add_accounts",
            last_user_input=currency,
            last_bot_response="Введіть стартовий баланс рахунку.",
        )
    return ACC_BALANCE


async def onb_account_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.message.text:
        return ACC_BALANCE

    tg_user_id = context.user_data.get("tg_user_id")
    current = context.user_data.setdefault("onb", {}).get("current_account") or {}
    if not tg_user_id or not current.get("label_base") or not current.get("currency"):
        await update.message.reply_text("Щось пішло не так на кроці з рахунком. Натисніть /start і спробуйте ще раз.")
        return ConversationHandler.END

    requested_account_type = normalize_account_type(str(current.get("requested_account_type") or "main"))
    credit_step = str(current.get("credit_step") or "")

    if credit_step == "await_limit":
        credit_limit = parse_decimal_amount(update.message.text, allow_negative=False)
        if credit_limit is None or credit_limit <= 0:
            await update.message.reply_text("Введіть кредитний ліміт числом більше 0.")
            return ACC_BALANCE
        current["credit_limit"] = credit_limit
        current["credit_step"] = "await_rate"
        await update.message.reply_text("Введіть % нарахування на місяць. Наприклад: <code>3.5</code>.")
        return ACC_BALANCE

    if credit_step == "await_rate":
        monthly_interest_rate = parse_decimal_rate(update.message.text)
        if monthly_interest_rate is None:
            await update.message.reply_text("Введіть відсоток числом більше 0. Наприклад: <code>3.5</code>.")
            return ACC_BALANCE
        current["monthly_interest_rate"] = monthly_interest_rate
        current.pop("credit_step", None)
        balance = current.get("starting_balance")
        if not isinstance(balance, Decimal):
            await update.message.reply_text("Не вдалося завершити створення кредитки. Спробуйте ще раз.")
            return ACC_BALANCE
        return await _finish_onboarding_account_create(update.message, context, tg_user_id, current, balance)

    balance = parse_decimal_amount(update.message.text, allow_negative=(requested_account_type == CREDIT_ACCOUNT_TYPE))
    if balance is None:
        await update.message.reply_text("Не бачу коректну суму. Приклад: <code>5000</code> або <code>-100</code>.")
        return ACC_BALANCE
    if requested_account_type != CREDIT_ACCOUNT_TYPE and balance < 0:
        await update.message.reply_text("Для звичайного рахунку баланс не може бути від’ємним. Якщо це кредитка, оберіть тип «Кредитка».")
        return ACC_BALANCE

    if requested_account_type == CREDIT_ACCOUNT_TYPE:
        current["starting_balance"] = balance
        current["credit_step"] = "await_limit"
        await update.message.reply_text("Введіть кредитний ліміт цієї картки.")
        return ACC_BALANCE

    return await _finish_onboarding_account_create(update.message, context, tg_user_id, current, balance)


async def onb_accounts_more_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_MORE_DONE
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        accounts = await _get_accounts(conn, tg_user_id)

    action = q.data.rsplit(":", 1)[-1]
    if action == "more":
        if len(accounts) >= ONBOARDING_ACCOUNT_LIMIT:
            await q.message.reply_text(
                f"На одного користувача можна додати до {ONBOARDING_ACCOUNT_LIMIT} рахунків. Якщо все готово — натисніть «Готово»."
            )
            return ACC_MORE_DONE
        return await _start_accounts_step(q.message, context, force_add=True)

    if not accounts:
        await q.message.reply_text("Потрібен хоча б один рахунок, щоб продовжити.")
        return await _start_accounts_step(q.message, context, force_add=True)

    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/confirm",
        last_user_input=action,
        last_bot_response="Перехід до підтвердження після кроку рахунків.",
    )
    return await _show_onboarding_summary(q.message, context)


async def onb_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CONFIRM
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    return await _show_onboarding_summary(q.message, context)


async def onb_categories_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return ONB_CONFIRM
    return await _show_onboarding_summary(update.message, context)


async def onb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CONFIRM
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    action = q.data.rsplit(":", 1)[-1]
    tg_user_id = context.user_data.get("tg_user_id") or (update.effective_user.id if update.effective_user else None)
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END
    await _ensure_onboarding_context_loaded(context, int(tg_user_id))

    if action == "ok":
        next_access_state: dict[str, Any] | None = None
        async with _pool(context).acquire() as conn:
            await CategoryService(conn).createCategoriesFromOnboardingSelection(
                int(tg_user_id),
                _onboarding_category_selection(context.user_data.get("onb")),
            )
            await conn.execute(
                "UPDATE users SET onboarding_completed=true, onboarding_version=$2 WHERE tg_user_id=$1",
                tg_user_id,
                CURRENT_ONBOARDING_VERSION,
            )
            await log_bot_event(
                conn,
                tg_user_id,
                "onboarding_completed",
                source="onboarding",
                parsed_result={"version": CURRENT_ONBOARDING_VERSION},
            )
            await sync_onboarding_debug_state(
                conn,
                tg_user_id,
                current_fsm_state="",
                onboarding_payload={},
                last_bot_response="Налаштування завершено.",
                last_event_type="onboarding_completed",
            )
            next_access_state = await _get_resolved_access_state(conn, int(tg_user_id))
        _reset_onboarding(context)
        if str((next_access_state or {}).get("access_scope") or "paywall") == "paywall":
            async with _pool(context).acquire() as conn:
                requested_trial_days = _billing_trial_days(int((next_access_state or {}).get("promo_trial_days") or 30))
                try:
                    result = await init_bind_session(
                        tg_user_id=int(tg_user_id),
                        trial_days=requested_trial_days,
                        mode="bind",
                        promo_code=_pending_promo_code(next_access_state or {}),
                    )
                except BillingAPIError as exc:
                    await _show_billing_menu(
                        q.message,
                        conn,
                        int(tg_user_id),
                        notice=f"Налаштування завершено.\n\nMonobank bind не стартував: {escape_html(str(exc))}",
                        access_state=next_access_state,
                    )
                else:
                    effective_trial_days = _billing_trial_days(int(result.get("trial_days") or requested_trial_days or 30))
                    await q.message.reply_text(
                        _billing_onboarding_autobind_text(trial_days=effective_trial_days),
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton(_billing_bind_button_label(trial_granted=True), url=str(result.get("page_url") or ""))],
                                [_back_btn("home:show")],
                            ]
                        ),
                    )
        elif str((next_access_state or {}).get("access_scope") or "") == "debt_only":
            await q.message.reply_text("Готово, базове налаштування завершено.")
            async with _pool(context).acquire() as conn:
                await _show_debt_only_surface(
                    q.message,
                    conn,
                    int(tg_user_id),
                    notice=(
                        "<b>Налаштування завершено.</b>\n\n"
                        "Для цього акаунта зараз відкрито лише розділ боргів."
                    ),
                )
        else:
            await q.message.reply_text("Готово, базове налаштування завершено.\n\nМожна додавати витрати, доходи та дивитися статистику.")
            await _reply_home(q.message, "🏠 Тепер можна додавати витрати/доходи текстом або голосом.")
        await notify_admins(
            context.application.bot,
            format_onboarding_notification(
                tg_user_id=tg_user_id,
                username=getattr(update.effective_user, "username", None) if update.effective_user else None,
                first_name=getattr(update.effective_user, "first_name", None) if update.effective_user else None,
            ),
            exclude_chat_ids={int(tg_user_id)},
        )
        return ConversationHandler.END

    if action == "edit":
        async with _pool(context).acquire() as conn:
            accounts = await _get_accounts(conn, tg_user_id)
        await q.message.reply_text(
            "Редагування рахунків.\n\nМожете видалити зайве, додати новий рахунок або повернутися до підтвердження.",
            reply_markup=kb_onb_edit_accounts(accounts),
        )
        return ONB_EDIT_ACCOUNTS

    return await _restart_onboarding_from_confirm_callback(
        context,
        tg_user_id=int(tg_user_id),
        message=q.message,
        last_user_input=action,
    )


async def _restart_onboarding_from_confirm_callback(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    tg_user_id: int,
    message,
    last_user_input: str,
) -> int:
    async with _pool(context).acquire() as conn:
        await conn.execute("UPDATE accounts SET is_active=false WHERE tg_user_id=$1", tg_user_id)
        await conn.execute("UPDATE categories SET is_active=false WHERE tg_user_id=$1", tg_user_id)
        await conn.execute(
            "UPDATE users SET onboarding_completed=false, onboarding_version=0, start_date=NULL WHERE tg_user_id=$1",
            tg_user_id,
        )
        await sync_onboarding_debug_state(
            conn,
            tg_user_id,
            current_fsm_state="onboarding/select_language",
            onboarding_payload={},
            last_user_input=last_user_input,
            last_bot_response="Налаштування перезапущено з кроку 1.",
            last_event_type="onboarding_failed",
        )

    _reset_onboarding(context)
    context.user_data["onb"] = {}
    context.user_data["tg_user_id"] = int(tg_user_id)
    await message.reply_text("Починаємо налаштування з початку.\n\nКрок 1 з 5: оберіть мову.", reply_markup=kb_language())
    return LANG


async def _render_onboarding_summary(conn: asyncpg.Connection, tg_user_id: int, onb_state: dict) -> str:
    _ = onb_state
    user = await _get_user(conn, tg_user_id)
    accounts_text = await _render_accounts_text(conn, tg_user_id)
    start_date = user.get("start_date") if user else None
    return "\n".join(
        [
            "Крок 5 з 5: підтвердження.",
            "",
            "Перевірте налаштування:",
            f"- Мова: {_language_label(user.get('lang') if user else None)}",
            f"- Базова валюта: {(user.get('base_currency') if user else None) or '—'}",
            f"- Стартова дата: {start_date.isoformat() if start_date else '—'}",
            "",
            accounts_text,
            "",
            "Ми вже підготували стандартні категорії витрат. Бот сам розкладе витрати, а ти зможеш виправити категорію після додавання.",
            "",
            "Якщо все гаразд, підтвердьте. Якщо ні, можете повернутися і відредагувати рахунки.",
        ]
    )


async def _seed_category_templates(conn: asyncpg.Connection) -> None:
    await CategoryTemplateService(conn).syncDefaultExpenseTemplates()

    await CategoryTemplateService(conn).syncDefaultIncomeTemplates()


def onboarding_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Підтвердити", callback_data="onb:confirm:ok")],
            [InlineKeyboardButton("✏️ Змінити рахунки", callback_data="onb:confirm:edit")],
            [InlineKeyboardButton("🔄 Почати спочатку", callback_data="onb:confirm:restart")],
        ]
    )


async def _show_onboarding_summary(message, context: ContextTypes.DEFAULT_TYPE) -> int:
    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        summary = await _render_onboarding_summary(conn, tg_user_id, context.user_data.get("onb", {}))
    await message.reply_text(summary, reply_markup=onboarding_confirm_keyboard())
    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/confirm",
        last_bot_response="Крок 5 з 5: підтвердження.",
    )
    return ONB_CONFIRM


async def onb_accounts_more_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ACC_MORE_DONE
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    tg_user_id = context.user_data.get("tg_user_id")
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END

    async with _pool(context).acquire() as conn:
        accounts = await _get_accounts(conn, tg_user_id)

    action = q.data.rsplit(":", 1)[-1]
    if action == "more":
        if len(accounts) >= ONBOARDING_ACCOUNT_LIMIT:
            await q.message.reply_text(
                f"На одного користувача можна додати до {ONBOARDING_ACCOUNT_LIMIT} рахунків. Якщо все готово — натисніть «Готово»."
            )
            return ACC_MORE_DONE
        return await _start_accounts_step(q.message, context, force_add=True)

    if not accounts:
        await q.message.reply_text("Потрібен хоча б один рахунок, щоб продовжити.")
        return await _start_accounts_step(q.message, context, force_add=True)

    await _sync_onboarding_debug(
        context,
        tg_user_id,
        step="onboarding/confirm",
        last_user_input=action,
        last_bot_response="Перехід до підтвердження після кроку рахунків.",
    )
    return await _show_onboarding_summary(q.message, context)


async def onb_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_CONFIRM
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    action = q.data.rsplit(":", 1)[-1]
    tg_user_id = context.user_data.get("tg_user_id") or (update.effective_user.id if update.effective_user else None)
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END
    await _ensure_onboarding_context_loaded(context, int(tg_user_id))

    if action == "ok":
        next_access_state: dict[str, Any] | None = None
        async with _pool(context).acquire() as conn:
            await CategoryService(conn).createCategoriesFromOnboardingSelection(
                int(tg_user_id),
                _onboarding_category_selection(context.user_data.get("onb")),
            )
            await conn.execute(
                "UPDATE users SET onboarding_completed=true, onboarding_version=$2 WHERE tg_user_id=$1",
                tg_user_id,
                CURRENT_ONBOARDING_VERSION,
            )
            await log_bot_event(
                conn,
                tg_user_id,
                "onboarding_completed",
                source="onboarding",
                parsed_result={"version": CURRENT_ONBOARDING_VERSION},
            )
            await sync_onboarding_debug_state(
                conn,
                tg_user_id,
                current_fsm_state="",
                onboarding_payload={},
                last_bot_response="Налаштування завершено.",
                last_event_type="onboarding_completed",
            )
            next_access_state = await _get_resolved_access_state(conn, int(tg_user_id))
        _reset_onboarding(context)
        if str((next_access_state or {}).get("access_scope") or "paywall") == "paywall":
            async with _pool(context).acquire() as conn:
                requested_trial_days = _billing_trial_days(int((next_access_state or {}).get("promo_trial_days") or 30))
                try:
                    result = await init_bind_session(
                        tg_user_id=int(tg_user_id),
                        trial_days=requested_trial_days,
                        mode="bind",
                        promo_code=_pending_promo_code(next_access_state or {}),
                    )
                except BillingAPIError as exc:
                    await _show_billing_menu(
                        q.message,
                        conn,
                        int(tg_user_id),
                        notice=f"Налаштування завершено.\n\nMonobank bind не стартував: {escape_html(str(exc))}",
                        access_state=next_access_state,
                    )
                else:
                    effective_trial_days = _billing_trial_days(int(result.get("trial_days") or requested_trial_days or 30))
                    await q.message.reply_text(
                        _billing_onboarding_autobind_text(trial_days=effective_trial_days),
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [InlineKeyboardButton(_billing_bind_button_label(trial_granted=True), url=str(result.get("page_url") or ""))],
                                [_back_btn("home:show")],
                            ]
                        ),
                    )
        elif str((next_access_state or {}).get("access_scope") or "") == "debt_only":
            await q.message.reply_text("Готово, базове налаштування завершено.")
            async with _pool(context).acquire() as conn:
                await _show_debt_only_surface(
                    q.message,
                    conn,
                    int(tg_user_id),
                    notice=(
                        "<b>Налаштування завершено.</b>\n\n"
                        "Для цього акаунта зараз відкрито лише розділ боргів."
                    ),
                )
        else:
            await q.message.reply_text(
                "Готово, базове налаштування завершено.\n\n"
                "Ми вже підготували стандартні категорії витрат. Бот сам розкладе витрати, а ти зможеш виправити категорію після додавання."
            )
            await _reply_home(q.message, "🏠 Тепер можна додавати витрати/доходи текстом або голосом.")
        await notify_admins(
            context.application.bot,
            format_onboarding_notification(
                tg_user_id=tg_user_id,
                username=getattr(update.effective_user, "username", None) if update.effective_user else None,
                first_name=getattr(update.effective_user, "first_name", None) if update.effective_user else None,
            ),
            exclude_chat_ids={int(tg_user_id)},
        )
        return ConversationHandler.END

    if action == "edit":
        async with _pool(context).acquire() as conn:
            accounts = await _get_accounts(conn, tg_user_id)
        await q.message.reply_text(
            "Редагування рахунків.\n\nМожете видалити зайве, додати новий рахунок або повернутися до підтвердження.",
            reply_markup=kb_onb_edit_accounts(accounts),
        )
        return ONB_EDIT_ACCOUNTS

    return await _restart_onboarding_from_confirm_callback(
        context,
        tg_user_id=int(tg_user_id),
        message=q.message,
        last_user_input=action,
    )


async def restart_onboarding_callback_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return LANG
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)
    tg_user_id = context.user_data.get("tg_user_id") or (update.effective_user.id if update.effective_user else None)
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END
    await _ensure_onboarding_context_loaded(context, int(tg_user_id))
    return await _restart_onboarding_from_confirm_callback(
        context,
        tg_user_id=int(tg_user_id),
        message=q.message,
        last_user_input="restart",
    )


async def onb_edit_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q:
        return ONB_EDIT_ACCOUNTS
    await q.answer()
    if update.effective_user:
        await _consume_pending_admin_reset_if_needed(context, update.effective_user.id)

    tg_user_id = context.user_data.get("tg_user_id") or (update.effective_user.id if update.effective_user else None)
    if not tg_user_id:
        await q.message.reply_text(USER_NOT_FOUND_TEXT)
        return ConversationHandler.END
    await _ensure_onboarding_context_loaded(context, int(tg_user_id))

    action = q.data.split(":")
    async with _pool(context).acquire() as conn:
        if action[2] == "back":
            return await _show_onboarding_summary(q.message, context)

        if action[2] == "add":
            accounts = await _get_accounts(conn, tg_user_id)
            if len(accounts) >= ONBOARDING_ACCOUNT_LIMIT:
                await q.message.reply_text(f"На одного користувача можна додати до {ONBOARDING_ACCOUNT_LIMIT} рахунків.")
                return ONB_EDIT_ACCOUNTS
            return await _start_accounts_step(q.message, context, force_add=True)

        if action[2] == "archive" and len(action) == 4:
            await conn.execute(
                "UPDATE accounts SET is_active=false WHERE tg_user_id=$1 AND id=$2",
                tg_user_id,
                int(action[3]),
            )
            accounts = await _get_accounts(conn, tg_user_id)
            if not accounts:
                await q.message.reply_text("Рахунків не залишилось. Додайте хоча б один новий.")
                return await _start_accounts_step(q.message, context, force_add=True)
            await q.message.reply_text("Оновив список рахунків.", reply_markup=kb_onb_edit_accounts(accounts))
            return ONB_EDIT_ACCOUNTS

    return ONB_EDIT_ACCOUNTS


async def onboarding_callback_fallback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    data = q.data or ""
    if data.startswith("onb:confirm:"):
        await onb_confirm(update, context)
        return
    if data.startswith("onb:edit:"):
        await onb_edit_accounts(update, context)
        return
    current_account = ((context.user_data.get("onb") or {}).get("current_account") or {})
    if data.startswith("onb:acct:last4:") and current_account.get("label_base"):
        await onb_account_last4_choice(update, context)


async def home_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    if str(q.data or "").startswith("accounts:balance:confirm"):
        await _handle_account_balance_confirmation(q, context, user.id)
        return

    parts = q.data.split(":")
    action = parts[0] if parts else ""
    subaction = parts[1] if len(parts) > 1 else ""
    value = parts[2] if len(parts) > 2 else ""

    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        user_ready = await _user_ready(conn, user.id)
        if q.data == "billing:continue":
            _reset_runtime_flows(context)
            if not user_ready:
                if not await _show_miniapp_onboarding_entry(q.message, conn, user.id):
                    await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
                return
            if _access_scope_has_full_home(access_scope):
                await _show_access_surface(q.message, conn, user.id)
                return
            if access_scope == "debt_only":
                await _show_debt_only_surface(q.message, conn, user.id)
                return
            await _show_billing_menu(q.message, conn, user.id, access_state=access_state)
            return
        if q.data in {"menu:main", "home:show"}:
            _reset_runtime_flows(context)
            if not user_ready and access_scope != "debt_only":
                if not await _show_miniapp_onboarding_entry(q.message, conn, user.id):
                    await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            elif access_scope == "paywall":
                await _show_miniapp_entry(q.message, conn, user.id)
            else:
                await _show_access_surface(q.message, conn, user.id)
            return
        if q.data == HOME_MENU_DASHBOARD_CALLBACK:
            _reset_runtime_flows(context)
            if not user_ready and access_scope != "debt_only":
                if not await _show_miniapp_onboarding_entry(q.message, conn, user.id):
                    await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
                return
            await _show_miniapp_entry(q.message, conn, user.id)
            return
        if not user_ready and access_scope != "debt_only":
            if not await _show_miniapp_onboarding_entry(q.message, conn, user.id):
                await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(access_scope) and q.data != "settings:billing":
            if access_scope == "debt_only" and q.data == "debts:start":
                pass
            else:
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice=(
                        "<b>Цей розділ недоступний без повного доступу.</b>\n\n"
                        "Оформіть підписку або скористайтесь доступними для цього акаунта сценаріями."
                    ),
                    allow_debt_only=False,
                ):
                    return

        if q.data == "txvoid:recent":
            _reset_runtime_flows(context)
            try:
                recent = await list_recent_transactions(actor_tg_user_id=user.id, limit=10)
            except TransactionCancellationAPIError:
                await q.message.reply_text(_transaction_void_unavailable_text(), reply_markup=kb_home_operations_menu())
                return
            items = recent.get("items") if isinstance(recent.get("items"), list) else []
            if not items:
                await q.message.reply_text(
                    "<b>Останні операції</b>\n\nПоки немає доходів або витрат, які можна скасувати.",
                    reply_markup=kb_home_operations_menu(),
                )
                return
            await q.message.reply_text(
                "<b>Останні операції</b>\n\nОберіть транзакцію, яку хочете переглянути:",
                reply_markup=kb_recent_transactions(items),
            )
            return

        if action == "txvoid" and subaction == "pick":
            try:
                transaction_id = int(value)
                response = await build_transaction_void_draft(
                    actor_tg_user_id=user.id,
                    transaction_id=transaction_id,
                )
                draft = response.get("draft") if isinstance(response.get("draft"), dict) else {}
                if not draft:
                    raise TransactionCancellationAPIError("Cancellation draft is missing")
            except (TypeError, ValueError):
                await q.message.reply_text("Не вдалося визначити транзакцію.", reply_markup=kb_home_operations_menu())
                return
            except TransactionCancellationAPIError as exc:
                if exc.code == "already_cancelled":
                    await q.message.reply_text("Цю транзакцію вже скасовано.", reply_markup=kb_home_operations_menu())
                elif exc.status in {403, 404, 409}:
                    await q.message.reply_text(escape_html(str(exc)), reply_markup=kb_home_operations_menu())
                else:
                    await q.message.reply_text(_transaction_void_unavailable_text(), reply_markup=kb_transaction_saved(transaction_id))
                return
            context.user_data["transaction_void_draft"] = draft
            context.user_data["transaction_void_idempotency_key"] = secrets.token_urlsafe(24)
            await q.message.reply_text(
                _transaction_void_card(draft),
                reply_markup=kb_transaction_void_confirm(transaction_id),
            )
            return

        if action == "txvoid" and subaction == "confirm":
            try:
                transaction_id = int(value)
            except (TypeError, ValueError):
                await q.message.reply_text("Не вдалося визначити транзакцію.", reply_markup=kb_home_operations_menu())
                return
            draft = context.user_data.get("transaction_void_draft")
            if not isinstance(draft, dict) or int(draft.get("transaction_id") or 0) != transaction_id:
                try:
                    draft_response = await build_transaction_void_draft(
                        actor_tg_user_id=user.id,
                        transaction_id=transaction_id,
                    )
                    draft = draft_response.get("draft") if isinstance(draft_response.get("draft"), dict) else {}
                    context.user_data["transaction_void_draft"] = draft
                    context.user_data["transaction_void_idempotency_key"] = secrets.token_urlsafe(24)
                except TransactionCancellationAPIError as exc:
                    if exc.code == "already_cancelled":
                        await q.message.reply_text("Цю транзакцію вже скасовано.", reply_markup=kb_home_operations_menu())
                    else:
                        await q.message.reply_text(_transaction_void_unavailable_text(), reply_markup=kb_transaction_void_confirm(transaction_id))
                    return
            idempotency_key = str(context.user_data.get("transaction_void_idempotency_key") or secrets.token_urlsafe(24))
            context.user_data["transaction_void_idempotency_key"] = idempotency_key
            try:
                response = await confirm_transaction_void(
                    actor_tg_user_id=user.id,
                    draft=draft,
                    idempotency_key=idempotency_key,
                )
            except TransactionCancellationAPIError as exc:
                if exc.code == "already_cancelled":
                    await q.message.reply_text("Цю транзакцію вже скасовано.", reply_markup=kb_home_operations_menu())
                elif exc.status in {403, 404, 409}:
                    await q.message.reply_text(escape_html(str(exc)), reply_markup=kb_home_operations_menu())
                else:
                    await q.message.reply_text(_transaction_void_unavailable_text(), reply_markup=kb_transaction_void_confirm(transaction_id))
                return
            result = response.get("result") if isinstance(response.get("result"), dict) else {}
            new_balance = result.get("new_balance") if isinstance(result.get("new_balance"), dict) else {}
            context.user_data.pop("transaction_void_draft", None)
            context.user_data.pop("transaction_void_idempotency_key", None)
            if str(result.get("status") or "") == "already_cancelled":
                text = "Цю транзакцію вже скасовано."
            else:
                text = (
                    "Транзакцію скасовано. Новий баланс рахунку: "
                    f"<b>{escape_html(str(new_balance.get('display') or new_balance.get('value') or '—'))}</b>."
                )
            await q.message.reply_text(text, reply_markup=kb_home_operations_menu())
            return

        if q.data == "txvoid:leave":
            context.user_data.pop("transaction_void_draft", None)
            context.user_data.pop("transaction_void_idempotency_key", None)
            await q.message.reply_text("Транзакцію залишено без змін.", reply_markup=kb_home_operations_menu())
            return

        if q.data == HOME_MENU_OPERATIONS_CALLBACK:
            _reset_runtime_flows(context)
            await q.message.reply_text(
                locale_text("<b>🧾 Операції</b>", "<b>🧾 Operations</b>", current_ui_locale()),
                reply_markup=kb_home_operations_menu(),
            )
            return

        if q.data == HOME_MENU_FINANCES_CALLBACK:
            _reset_runtime_flows(context)
            await q.message.reply_text(
                locale_text("<b>💼 Фінанси</b>", "<b>💼 Finances</b>", current_ui_locale()),
                reply_markup=kb_home_finances_menu(),
            )
            return

        if q.data == HOME_MENU_REPORTS_CALLBACK:
            _reset_runtime_flows(context)
            await q.message.reply_text(
                locale_text("<b>📈 Звіти</b>", "<b>📈 Reports</b>", current_ui_locale()),
                reply_markup=kb_home_reports_menu(),
            )
            return

        if q.data == HOME_MENU_MORE_CALLBACK:
            _reset_runtime_flows(context)
            await q.message.reply_text(
                locale_text("<b>⋯ Ще</b>", "<b>⋯ More</b>", current_ui_locale()),
                reply_markup=kb_home_more_menu(),
            )
            return

        if q.data == "accounts:list":
            _reset_runtime_flows(context)
            await _recalculate_account_balances(conn, user.id)
            accounts = await _get_accounts_full(conn, user.id)
            await q.message.reply_text(
                await _render_accounts_text(conn, user.id),
                reply_markup=kb_accounts_menu() if accounts else _kb_accounts_empty_menu(),
            )
            return

        if q.data == "accounts:add":
            _reset_runtime_flows(context)
            await q.message.reply_text(
                "<b>➕ Додати рахунок</b>\n\nОберіть тип рахунку:",
                reply_markup=kb_account_add_menu(),
            )
            return

        if q.data.startswith("accounts:add:type:"):
            account_type = normalize_account_type(q.data.rsplit(":", 1)[-1])
            saving_flow = context.user_data.get("saving_flow") or {}
            preserve_saving_flow = _should_preserve_saving_flow_for_account_create(saving_flow)
            origin = "saving" if saving_flow else "settings"
            cancel_callback = "saving:cancel" if saving_flow else "settings:acct:cancel"
            await _start_account_create_flow(
                q.message,
                context,
                account_type=account_type,
                include_goal_fields=(account_type in SAVINGS_ACCOUNT_TYPES),
                origin=origin,
                cancel_callback=cancel_callback,
                preserve_saving_flow=preserve_saving_flow,
            )
            return

        if q.data == "accounts:manage":
            _reset_runtime_flows(context)
            accounts = await _get_accounts(conn, user.id)
            if not accounts:
                await q.message.reply_text(
                    "<b>✏️ Керування рахунками</b>\n\nУ вас ще немає жодного активного рахунку.",
                    reply_markup=_kb_accounts_empty_menu(),
                )
                return
            await q.message.reply_text(
                "<b>✏️ Керування рахунками</b>\n\nОберіть рахунок, який хочете змінити.",
                reply_markup=kb_accounts_manage_keyboard(accounts),
            )
            return

        if q.data.startswith("accounts:open:"):
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            detail_lines = [
                f"<b>💼 {escape_html(str(account['label']))}</b>",
                "",
                f"<b>Валюта:</b> {escape_html(str(account['currency']))}",
                f"<b>Тип:</b> {escape_html(account_type_label(str(account['account_type'] or 'other')))}",
                f"<b>Баланс:</b> {format_money(account['balance'], str(account['currency']))}",
            ]
            credit_limit = account.get("credit_limit")
            monthly_interest_rate = account.get("monthly_interest_rate")
            non_negative_type = str(account.get("non_negative_account_type") or "main")
            if credit_limit is not None or monthly_interest_rate is not None or normalize_account_type(str(account["account_type"] or "other")) == CREDIT_ACCOUNT_TYPE:
                detail_lines.append(f"<b>Повертається в тип:</b> {escape_html(account_type_label(non_negative_type))}")
            if credit_limit is not None:
                detail_lines.append(f"<b>Кредитний ліміт:</b> {format_money(credit_limit, str(account['currency']))}")
            if monthly_interest_rate is not None:
                detail_lines.append(f"<b>% на місяць:</b> {format_decimal_value(monthly_interest_rate, places=4)}%")
            detail_lines.extend(["", "Оберіть дію:"])
            await q.message.reply_text("\n".join(detail_lines), reply_markup=kb_account_detail_keyboard(account_id))
            return
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            await q.message.reply_text(
                "<b>💼 {name}</b>\n\n<b>Валюта:</b> {currency}\n<b>Тип:</b> {account_type}\n<b>Баланс:</b> {balance}\n\nОберіть дію:".format(
                    name=escape_html(str(account["label"])),
                    currency=escape_html(str(account["currency"])),
                    account_type=escape_html(account_type_label(str(account["account_type"] or "other"))),
                    balance=format_money(account["balance"], str(account["currency"])),
                ),
                reply_markup=kb_account_detail_keyboard(account_id),
            )
            return

        if q.data.startswith("accounts:type:") and not q.data.startswith("accounts:type:set:"):
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            await q.message.reply_text(
                "<b>🔁 Змінити тип рахунку</b>\n\n"
                f"Поточний тип: <b>{escape_html(account_type_label(str(account['account_type'] or 'other')))}</b>\n\n"
                "Оберіть новий тип:",
                reply_markup=kb_account_type_change(account_id),
            )
            return

        if q.data.startswith("accounts:type:set:"):
            try:
                account_id = int(parts[3])
                new_type = normalize_account_type(parts[4])
            except (IndexError, ValueError):
                await q.message.reply_text("Не вдалося змінити тип рахунку.", reply_markup=kb_accounts_menu())
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            await AccountService(conn).update_account_type(user.id, account_id, new_type)
            await q.message.reply_text(
                f"Готово. Тип рахунку <b>{escape_html(str(account['label']))}</b> змінено на <b>{escape_html(account_type_label(new_type))}</b>.",
                reply_markup=kb_account_detail_keyboard(account_id),
            )
            return

        if q.data.startswith("accounts:rename:"):
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            context.user_data["accounts_flow"] = {"step": "rename", "account_id": account_id}
            await q.message.reply_text(
                "<b>✏️ Змінити назву рахунку</b>\n\nВведіть нову назву рахунку.",
                reply_markup=kb_inline_cancel("accounts:cancel"),
            )
            return

        if q.data.startswith("accounts:balance:"):
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            context.user_data["accounts_flow"] = {"step": "balance", "account_id": account_id}
            await q.message.reply_text(
                "<b>💰 Змінити баланс рахунку</b>\n\nВведіть поточний баланс рахунку. Можна вказати число зі знаком <code>-</code>, якщо рахунок зараз у мінусі.",
                reply_markup=kb_inline_cancel("accounts:cancel"),
            )
            return
            await q.message.reply_text(
                "<b>💰 Змінити баланс рахунку</b>\n\nВведіть поточний баланс рахунку.",
                reply_markup=kb_inline_cancel("accounts:cancel"),
            )
            return

        if q.data.startswith("accounts:archive:confirm:"):
            try:
                account_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            account_service = AccountService(conn)
            account = await account_service.get_active_account_by_id(user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            await account_service.archive_account(user.id, account_id)
            await account_service.recalculate_account_balances(user.id)
            await q.message.reply_text(
                f"<b>🗃 Архівовано</b>\n\n{escape_html(str(account['label']))} більше не показуватиметься у списках для нових операцій.",
                reply_markup=kb_accounts_menu(),
            )
            return

        if q.data.startswith("accounts:archive:"):
            account_id = int(value or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.", reply_markup=kb_accounts_menu())
                return
            await q.message.reply_text(
                f"<b>🗃 Архівувати рахунок?</b>\n\n"
                f"Рахунок <b>{escape_html(str(account['label']))}</b> більше не буде показуватись у списках для нових операцій.\n\n"
                "Історія операцій збережеться.",
                reply_markup=kb_account_archive_confirm_keyboard(account_id),
            )
            return

        if q.data in {"expense:start", "income:start"}:
            if await _billing_write_blocked(q.message, conn, user.id):
                return
            _reset_debt_flow(context)
            accounts = await _get_accounts(conn, user.id)
            await _ensure_default_categories(conn, user.id)
            if not accounts:
                await _reply_and_return_home(q.message, "<b>❌ Рахунок не вибрано</b>\n\nСпочатку додайте хоча б один рахунок.")
                return
            kind = "expense" if q.data.startswith("expense:") else "income"
            context.user_data["tx_flow"] = {"kind": kind, "step": "choose_account"}
            await q.message.reply_text(
                f"<b>{'➖ Нова витрата' if kind == 'expense' else '➕ Новий дохід'}</b>\n\n"
                "Крок <b>1 з 3</b>: оберіть рахунок.",
                reply_markup=kb_pick_account(accounts, kind),
            )
            return
        if q.data == "transfer:start":
            if await _billing_write_blocked(q.message, conn, user.id):
                return
            _reset_runtime_flows(context)
            await _start_transfer_flow(q.message, context, user.id, conn=conn)
            return

        if q.data in {"reports:start", "reports:today", "reports:7d", "reports:month", "reports:30d", "reports:3m", "reports:6m"}:
            _reset_runtime_flows(context)
            await q.message.reply_text(
                await _get_bot_copy(conn, "reports_intro_text"),
                reply_markup=_with_miniapp_launch(kb_reports_menu()),
            )
            return

        if q.data == "categories:start":
            _reset_runtime_flows(context)
            await _ensure_default_categories(conn, user.id)
            await q.message.reply_text(
                await _get_bot_copy(conn, "categories_intro_text"),
                reply_markup=categories_home_keyboard(),
            )
            return

        if q.data == "debts:start":
            _reset_tx_flow(context)
            await q.message.reply_text(await _debts_report_text(conn, user.id), reply_markup=kb_debts_menu())
            return
        if q.data == "settings:start":
            _reset_runtime_flows(context)
            await _show_settings_menu(q.message, conn)
            return

        if q.data == "family:start":
            _reset_runtime_flows(context)
            await _show_family_menu(q.message, context, user.id, conn)
            return

        if q.data == "export:start":
            _reset_runtime_flows(context)
            await q.message.reply_text(
                await _get_bot_copy(conn, "export_intro_text"),
                reply_markup=_with_miniapp_launch(kb_export_menu()),
            )
            return
    await _show_home(update, context)


async def _get_categories_full(conn: asyncpg.Connection, tg_user_id: int, kind: str) -> list[tuple[int, str, list[str]]]:
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        rows = await conn.fetch(
            """
            SELECT id, name, slug, aliases
            FROM categories
            WHERE family_id=$1 AND type=$2 AND is_active=true
            ORDER BY sort_order ASC, name ASC
            """,
            int(scope.family_id),
            kind,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name, slug, aliases
            FROM categories
            WHERE user_id=$1 AND family_id IS NULL AND type=$2 AND is_active=true
            ORDER BY sort_order ASC, name ASC
            """,
            tg_user_id,
            kind,
        )

    result: list[tuple[int, str, list[str]]] = []
    for row in rows:
        slug = str(row["slug"] or "").strip().lower()
        aliases = _merge_category_lookup_aliases(kind, slug, list(row["aliases"] or []))
        result.append((int(row["id"]), str(row["name"]), aliases))
    return result


def _find_other_category_id(categories_rows: list[tuple[int, str, list[str]]]) -> int | None:
    for category_id, name, aliases in categories_rows:
        normalized_name = str(name or "").strip().lower()
        normalized_aliases = {str(alias or "").strip().lower() for alias in aliases or []}
        if normalized_name == "інше" or "other" in normalized_aliases:
            return int(category_id)
    return None


_LEGACY_CATEGORY_KEY_ALIASES = {
    "products": "groceries",
    "cafe": "cafes_restaurants_delivery",
    "utilities_internet": "telecom_internet",
}

_NON_EXPENSE_CATEGORY_KEY_HINTS = {
    "salary": ["salary", "зарплат"],
    "client_payment": ["client_payment", "client", "payment", "клієнт", "клиент", "оплата"],
}


def _normalize_category_lookup_value(value: str | None) -> str:
    return str(value or "").strip().lower()


def _merge_category_lookup_aliases(kind: str, slug: str | None, aliases: list[str] | None) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()

    def _append(values: list[str] | tuple[str, ...] | None) -> None:
        for raw_value in values or []:
            value = str(raw_value or "").strip()
            normalized = _normalize_category_lookup_value(value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(value)

    normalized_slug = _normalize_category_lookup_value(slug)
    if normalized_slug:
        _append([normalized_slug])
    _append(list(aliases or []))
    if kind == "expense" and normalized_slug:
        definition = CATALOG_DEFAULT_EXPENSE_CATEGORY_BY_SLUG.get(normalized_slug)
        if definition is not None:
            _append(list(definition.get("aliases") or []))
    return merged


def _canonical_category_lookup_key(category_key: str | None) -> str | None:
    normalized_key = _normalize_category_lookup_value(category_key)
    if not normalized_key:
        return None
    return _LEGACY_CATEGORY_KEY_ALIASES.get(normalized_key, normalized_key)


def _category_lookup_hints(category_key: str | None) -> list[str]:
    normalized_key = _canonical_category_lookup_key(category_key)
    if normalized_key is None:
        return []

    hints: list[str] = [normalized_key]
    definition = CATALOG_DEFAULT_EXPENSE_CATEGORY_BY_SLUG.get(normalized_key)
    if definition is not None:
        hints.append(str(definition.get("name") or ""))
        hints.extend(str(alias or "") for alias in definition.get("aliases") or [])
    else:
        hints.extend(_NON_EXPENSE_CATEGORY_KEY_HINTS.get(normalized_key) or [])

    normalized_hints: list[str] = []
    for hint in hints:
        value = _normalize_category_lookup_value(hint)
        if value and value not in normalized_hints:
            normalized_hints.append(value)
    return normalized_hints


def _category_row_tokens(name: str, aliases: list[str]) -> set[str]:
    tokens = {_normalize_category_lookup_value(name)}
    tokens.update(_normalize_category_lookup_value(alias) for alias in aliases or [])
    tokens.discard("")
    return tokens


def _pick_category_id_by_key(
    category_key: str | None,
    categories_rows: list[tuple[int, str, list[str]]],
    *,
    fallback_to_other: bool = False,
) -> int | None:
    normalized_key = _normalize_category_lookup_value(category_key)
    canonical_key = _canonical_category_lookup_key(category_key)
    if canonical_key is None:
        return _find_other_category_id(categories_rows) if fallback_to_other else None

    exact_keys = [canonical_key]
    if normalized_key and normalized_key != canonical_key:
        exact_keys.append(normalized_key)

    for category_id, name, aliases in categories_rows:
        row_tokens = _category_row_tokens(str(name or ""), list(aliases or []))
        if any(exact_key in row_tokens for exact_key in exact_keys):
            return int(category_id)

    hints = _category_lookup_hints(canonical_key)
    best_id: int | None = None
    best_score = 0
    for category_id, name, aliases in categories_rows:
        candidates = [str(name or "")] + [str(alias or "") for alias in aliases or []]
        hay = " | ".join(_normalize_category_lookup_value(candidate) for candidate in candidates if candidate).strip()
        for hint in hints:
            if hint and hint in hay:
                score = len(hint)
                if score > best_score:
                    best_score = score
                    best_id = int(category_id)
    if best_id is not None:
        return best_id
    return _find_other_category_id(categories_rows) if fallback_to_other else None


def _pick_category_id_by_text(
    text: str,
    categories_rows: list[tuple[int, str, list[str]]],
    *,
    fallback_to_other: bool = False,
) -> int | None:
    normalized_text = (text or "").strip().lower()
    if normalized_text:
        best_id: int | None = None
        best_len = 0
        for category_id, name, aliases in categories_rows:
            for token in [str(name or "")] + [str(a or "") for a in aliases or []]:
                value = token.strip().lower()
                if not value:
                    continue
                if value in normalized_text and len(value) > best_len:
                    best_len = len(value)
                    best_id = int(category_id)
        if best_id is not None:
            return best_id
    return _find_other_category_id(categories_rows) if fallback_to_other else None


def _pick_category_id_by_hint(category_hint: str | None, categories_rows: list[tuple[int, str, list[str]]]) -> int | None:
    if not category_hint:
        return None
    return _pick_category_id_by_key(category_hint, categories_rows) or _pick_category_id_by_text(
        category_hint,
        categories_rows,
    )


def _pick_category_id_by_normalization(
    texts: list[str | None],
    categories_rows: list[tuple[int, str, list[str]]],
) -> int | None:
    joined_text = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if not joined_text:
        return None
    normalized_match = match_expense_normalization(joined_text)
    if normalized_match is None:
        return None
    return _pick_category_id_by_key(normalized_match.matched_slug, categories_rows)


def _category_list_text(type_: str, categories: list[Category]) -> str:
    lines = [f"Категорії {_category_type_label(type_)}:", ""]
    if categories:
        for index, category in enumerate(categories, 1):
            lines.append(f"{index}. {escape_html(category.name)} [{_category_row_label(category)}]")
    else:
        lines.append("<i>Немає активних категорій.</i>")
    lines.append("")
    if type_ == "expense":
        lines.append("Це стандартний фіксований набір витрат. Змінювати його в налаштуваннях не потрібно.")
    else:
        lines.append("Можете додати нову або відредагувати існуючу.")
    return "\n".join(lines)


def _category_edit_list_text(type_: str) -> str:
    if type_ == "expense":
        return "Стандартні категорії витрат уже підготовлені. Тут вони доступні лише для перегляду."
    return f"Оберіть {_category_type_accusative(type_)}, яку хочете змінити:"


def _category_empty_text(type_: str) -> str:
    if type_ == "expense":
        return "Стандартні категорії витрат тимчасово недоступні."
    return f"У тебе немає активних категорій {_category_type_label(type_)}."


def _category_detail_text(category: Category) -> str:
    if category.type == "expense":
        return (
            f"Категорія: {escape_html(category.name)}\n"
            f"Тип: {_category_type_title(category.type)}\n\n"
            "Це стандартна категорія витрат. Вона фіксована для всіх користувачів, але категорію конкретної транзакції можна змінити вручну."
        )
    return (
        f"Категорія: {escape_html(category.name)}\n"
        f"Тип: {_category_type_title(category.type)}\n\n"
        "Що хочете зробити?"
    )


async def _reply_category_edit_list(message, svc: CategoryService, user_id: int, type_: str, *, page: int = 0, notice: str | None = None) -> None:
    categories = await svc.getUserCategories(user_id, type_)
    if type_ == "expense":
        text = _category_list_text(type_, categories)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=categories_list_keyboard(type_))
        return
    if not categories:
        text = _category_empty_text(type_)
        if notice:
            text = f"{notice}\n\n{text}"
        await message.reply_text(text, reply_markup=categories_empty_edit_keyboard(type_))
        return
    page_items, safe_page, total_pages = _paginate_items(categories, page)
    text = _category_edit_list_text(type_)
    if notice:
        text = f"{notice}\n\n{text}"
    await message.reply_text(
        text,
        reply_markup=categories_edit_list_keyboard(
            type_,
            [(category.id, category.name) for category in page_items],
            page=safe_page,
            total_pages=total_pages,
        ),
    )


async def categories_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user or not q.message:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        if not await _user_ready(conn, user.id):
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Категорії доступні лише з повним доступом.</b>",
            ):
                return

        categories_write_prefixes = (
            "categories:add",
            "categories:rename",
            "categories:archive",
            "categories:restore",
            "categories:aliases",
            "categories:restore_defaults",
            "categories:edit:",
        )
        if any((q.data or "").startswith(prefix) for prefix in categories_write_prefixes):
            if await _billing_write_blocked(q.message, conn, user.id):
                return

        await _ensure_default_categories(conn, user.id)
        svc = CategoryService(conn)
        raw_data = q.data or ""
        data = _normalize_categories_callback(raw_data)
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""
        state = str((context.user_data.get("categories_flow") or {}).get("await") or "")
        category_id = int(parts[-1]) if len(parts) > 2 and parts[-1].isdigit() else None
        _log_categories_callback_received(q, user.id, raw_data, data)
        _log_category_event("callback", user.id, raw_data, state=state, category_id=category_id)

        if data in {"categories:menu", "categories:home"}:
            _reset_categories_flow(context)
            await q.message.reply_text(
                await _get_bot_copy(conn, "categories_intro_text"),
                reply_markup=categories_home_keyboard(),
            )
            return

        if data == "categories:back:settings":
            _reset_categories_flow(context)
            await _show_settings_menu(q.message, conn)
            return

        if data == "categories:back:menu":
            _reset_categories_flow(context)
            await q.message.reply_text(
                await _get_bot_copy(conn, "categories_intro_text"),
                reply_markup=categories_home_keyboard(),
            )
            return

        if data == "categories:restore_defaults" or data.startswith("categories:restore_defaults:type:"):
            _reset_categories_flow(context)
            restore_type = parts[-1] if data.startswith("categories:restore_defaults:type:") else ""
            confirm_callback = "categories:restore_defaults:confirm"
            cancel_callback = "categories:menu"
            if restore_type in {"expense", "income"}:
                confirm_callback = f"categories:restore_defaults:confirm:type:{restore_type}"
                cancel_callback = f"categories:edit:type:{restore_type}"
            await q.message.reply_text(
                "Відновити стандартні категорії?\n\nВаші власні категорії залишаться без змін.",
                reply_markup=categories_restore_defaults_keyboard(confirm_callback, cancel_callback),
            )
            return

        if data == "categories:restore_defaults:confirm" or data.startswith("categories:restore_defaults:confirm:type:"):
            restore_type = parts[-1] if data.startswith("categories:restore_defaults:confirm:type:") else ""
            if restore_type == "expense":
                _reset_categories_flow(context)
                await q.message.reply_text(
                    "Стандартні категорії витрат уже зафіксовані для всіх користувачів.",
                    reply_markup=categories_list_keyboard("expense"),
                )
                return
            await svc.restoreDefaultCategories(user.id)
            _reset_categories_flow(context)
            restore_type = parts[-1] if data.startswith("categories:restore_defaults:confirm:type:") else ""
            if restore_type in {"expense", "income"}:
                await _reply_category_edit_list(
                    q.message,
                    svc,
                    user.id,
                    restore_type,
                    notice="Стандартні категорії відновлено. Ваші власні категорії залишились без змін.",
                )
            else:
                await q.message.reply_text(
                    "Стандартні категорії відновлено. Ваші власні категорії залишились без змін.",
                    reply_markup=categories_home_keyboard(),
                )
            return

        if data == "categories:add:start":
            _reset_categories_flow(context)
            await q.message.reply_text(
                "Додати категорію\n\nСпочатку оберіть тип:",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton("Категорія витрат", callback_data="categories:add:type:expense"),
                            InlineKeyboardButton("Категорія доходів", callback_data="categories:add:type:income"),
                        ],
                        [_back_btn("categories:menu")],
                        [_cancel_btn("categories:back:settings")],
                    ]
                ),
            )
            return

        if data.startswith("categories:add:type:"):
            type_ = parts[-1]
            if type_ not in {"expense", "income"}:
                await q.message.reply_text("Не зрозумів тип категорії.", reply_markup=categories_home_keyboard())
                return
            if type_ == "expense":
                await q.message.reply_text(
                    "Категорії витрат уже підготовлені. Додатково створювати їх у налаштуваннях не потрібно.",
                    reply_markup=categories_list_keyboard("expense"),
                )
                return
            context.user_data["categories_flow"] = {"await": CategoryEditStates.WAITING_FOR_ADD_NAME, "type": type_}
            await q.message.reply_text(
                f"Введіть назву нової категорії {_category_type_label(type_)}.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [_back_btn("categories:add:start")],
                        [_cancel_btn("categories:menu")],
                    ]
                ),
            )
            return

        if data in EDIT_START_CALLBACKS or data == "categories:edit:start":
            _reset_categories_flow(context)
            await q.message.reply_text(
                "Що редагуємо?",
                reply_markup=categories_edit_type_keyboard(),
            )
            return

        if data.startswith("categories:edit:type:") or data.startswith("categories:edit:page:"):
            _reset_categories_flow(context)
            if data.startswith("categories:edit:page:"):
                type_ = parts[-2]
                page = int(parts[-1]) if parts[-1].isdigit() else 0
            else:
                type_ = parts[-1]
                page = 0
            if type_ not in {"expense", "income"}:
                await q.message.reply_text("Не зрозумів тип категорій.", reply_markup=categories_home_keyboard())
                return
            await _reply_category_edit_list(q.message, svc, user.id, type_, page=page)
            return

        if data.startswith("categories:list:") and data != "categories:list:mine":
            _reset_categories_flow(context)
            type_ = parts[-1]
            if type_ not in {"expense", "income"}:
                await q.message.reply_text("Не зрозумів тип категорій.", reply_markup=categories_home_keyboard())
                return
            categories = await svc.getUserCategories(user.id, type_)
            await q.message.reply_text(
                _category_list_text(type_, categories),
                reply_markup=categories_list_keyboard(type_),
            )
            return

        if data == "categories:list:mine":
            _reset_categories_flow(context)
            expense = await svc.getUserCategories(user.id, "expense")
            income = await svc.getUserCategories(user.id, "income")
            await q.message.reply_text(
                _categories_mine_text(expense, income),
                reply_markup=categories_home_keyboard(),
            )
            return

        if data.startswith("categories:edit:item:") or data.startswith("categories:open:"):
            _reset_categories_flow(context)
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category or not category.is_active:
                await q.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(category.type if category else None))
                return
            await q.message.reply_text(
                _category_detail_text(category),
                reply_markup=category_edit_keyboard(
                    {"id": category.id, "type": category.type, "is_active": category.is_active, "is_system": category.is_system}
                ),
            )
            return

        if data.startswith("categories:rename:start:"):
            _reset_categories_flow(context)
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category or not category.is_active:
                await q.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(category.type if category else None))
                return
            if category.type == "expense":
                await q.message.reply_text(
                    "Стандартні категорії витрат не редагуються в налаштуваннях.",
                    reply_markup=category_edit_keyboard({"id": category.id, "type": category.type}),
                )
                return
            context.user_data["categories_flow"] = {
                "await": CategoryEditStates.WAITING_FOR_RENAME_NAME,
                "category_id": category_id,
                "category_type": category.type,
                "type": category.type,
                "old_name": category.name,
            }
            await q.message.reply_text(
                _category_rename_prompt(category),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [_back_btn(f"categories:edit:item:{category_id}")],
                        [_cancel_btn(f"categories:edit:item:{category_id}")],
                    ]
                ),
            )
            return

        if data.startswith("categories:delete:start:"):
            _reset_categories_flow(context)
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category or not category.is_active:
                await q.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(category.type if category else None))
                return
            if category.type == "expense":
                await q.message.reply_text(
                    "Стандартні категорії витрат не видаляються в налаштуваннях.",
                    reply_markup=category_edit_keyboard({"id": category.id, "type": category.type}),
                )
                return
            await q.message.reply_text(
                _category_delete_confirm_text(category),
                reply_markup=categories_delete_confirm_keyboard(category_id),
            )
            return

        if data.startswith("categories:delete:confirm:"):
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category or not category.is_active:
                await q.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(category.type if category else None))
                return
            if category.type == "expense":
                await q.message.reply_text(
                    "Стандартні категорії витрат не видаляються в налаштуваннях.",
                    reply_markup=category_edit_keyboard({"id": category.id, "type": category.type}),
                )
                return
            await svc.archiveCategory(user.id, category_id)
            _reset_categories_flow(context)
            await _reply_category_edit_list(
                q.message,
                svc,
                user.id,
                category.type,
                notice="Категорію видалено з нових операцій. Старі операції залишились без змін.",
            )
            return

        if data.startswith("categories:restore:"):
            _reset_categories_flow(context)
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category:
                await q.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard())
                return
            await svc.restoreCategory(user.id, category_id)
            _reset_categories_flow(context)
            await _reply_category_edit_list(q.message, svc, user.id, category.type, notice="Категорію відновлено.")
            return

        if data == "categories:aliases":
            _reset_categories_flow(context)
            await q.message.reply_text("Ключові слова поки можна змінювати тільки в розширених налаштуваннях.")
            return

        if data.startswith("categories:aliases:"):
            _reset_categories_flow(context)
            category_id = int(parts[-1])
            category = await svc.getCategoryById(user.id, category_id)
            if not category:
                await q.message.reply_text("Категорію не знайдено.", reply_markup=categories_home_keyboard())
                return
            context.user_data["categories_flow"] = {"await": CategoryEditStates.WAITING_FOR_ALIASES, "category_id": category_id, "type": category.type}
            await q.message.reply_text(
                "Введіть слова для розпізнавання через кому (наприклад: кава, coffee, starbucks).",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [_back_btn(f"categories:edit:item:{category_id}")],
                        [_cancel_btn("categories:edit:start")],
                    ]
                ),
            )
            return

        if data == "categories:hidden":
            _reset_categories_flow(context)
            scope = await _get_finance_scope(conn, user.id)
            if scope.is_family:
                rows = await conn.fetch(
                    """
                    SELECT id, name, type
                    FROM categories
                    WHERE family_id=$1 AND is_active=false AND is_system=false
                    ORDER BY deleted_at DESC NULLS LAST, name ASC
                    """,
                    int(scope.family_id),
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT id, name, type
                    FROM categories
                    WHERE user_id=$1 AND family_id IS NULL AND is_active=false AND is_system=false
                    ORDER BY deleted_at DESC NULLS LAST, name ASC
                    """,
                    user.id,
                )
            items = [(int(row["id"]), str(row["name"]), str(row["type"])) for row in rows]
            if not items:
                await q.message.reply_text(await _get_bot_copy(conn, "hidden_categories_empty_text"), reply_markup=categories_home_keyboard())
                return
            await q.message.reply_text("Приховані категорії", reply_markup=hidden_categories_keyboard(items))
            return

        _log_category_event("unknown", user.id, data, state=state)
        await q.message.reply_text("Не зрозумів дію. Спробуйте ще раз через меню.", reply_markup=categories_home_keyboard())


def _kb_home_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Головне меню", callback_data="home:show")]])


def _kb_transfer_confirm(*, allow_rate_edit: bool, flow: dict | None = None) -> InlineKeyboardMarkup:
    markup = kb_transfer_confirm(allow_rate_edit=allow_rate_edit)
    return bind_financial_preview(flow, markup) if flow is not None else markup


def _kb_tx_confirm(flow: dict) -> InlineKeyboardMarkup:
    return bind_financial_preview(flow, kb_tx_confirm(str(flow.get("kind") or "expense")))


def _kb_accounts_empty_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Додати рахунок", callback_data="accounts:add")],
            [InlineKeyboardButton("🏠 Головне меню", callback_data="menu:main")],
        ]
    )


async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return

        if q.data in {"help:start", "help:home"}:
            _reset_runtime_flows(context)
            await _show_help_home(q.message, locale=current_ui_locale())
            return

        if q.data == "help:back":
            _reset_runtime_flows(context)
            await _show_access_surface(q.message, conn, user.id)
            return

        if q.data == "help:support":
            await _show_help_support(q.message, locale=current_ui_locale())
            return

        if q.data.startswith("help:topic:"):
            await _show_help_topic(q.message, q.data.removeprefix("help:topic:"), locale=current_ui_locale())
            return

        if q.data.startswith("help:q:"):
            parts = q.data.split(":", 3)
            if len(parts) == 4:
                await _show_help_answer(q.message, parts[2], parts[3], locale=current_ui_locale())
                return

    await _show_help_home(q.message, locale=current_ui_locale())


async def expense_reminder_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    parts = str(q.data or "").split(":")
    if parts[:3] != ["expense", "reminder", "no_expenses"] or len(parts) != 4:
        return
    try:
        reminder_day = date.fromisoformat(parts[3])
    except ValueError:
        await q.message.reply_text("Не вдалося розпізнати дату цього нагадування.")
        return

    async with _pool(context).acquire() as conn:
        reminder_zone, _timezone_name = await _get_daily_expense_reminder_zone(conn)
        today_local = datetime.now(reminder_zone).date()
        if reminder_day != today_local:
            await q.message.reply_text("Це нагадування вже неактуальне.", reply_markup=kb_home())
            return

        result = await ExpenseReminderService(conn).confirm_no_expenses_day(user.id, reminder_day)
        if result == "expense_recorded":
            await q.message.reply_text("За цей день уже є витрата, тому день без витрат не фіксую.")
            return
        await q.message.reply_text(
            await _get_bot_copy(conn, "daily_expense_no_expenses_confirm_text"),
            reply_markup=kb_home(),
            parse_mode=ParseMode.HTML,
        )


async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        await _activate_locale(context, tg_user_id=user.id, conn=conn, telegram_locale=getattr(user, "language_code", None))
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        ready_ok = await _user_ready(conn, user.id)
        if q.data == "settings:back":
            _reset_runtime_flows(context)
            await _show_access_surface(q.message, conn, user.id)
            return
        if not ready_ok and access_scope != "debt_only":
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return

        if q.data == "settings:accounts:back":
            _reset_runtime_flows(context)
            await _show_access_surface(q.message, conn, user.id)
            return

        if q.data == "settings:accounts":
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Розділ рахунків доступний лише з повним доступом.</b>",
                ):
                    return
            await _show_accounts_settings(q.message, context, user.id, conn=conn)
            return

        if q.data == "settings:savings":
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Розділ заощаджень доступний лише з повним доступом.</b>",
                ):
                    return
            await _show_savings_overview(q.message, context, user.id, conn=conn)
            return

        if q.data == "settings:expense_reminders":
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Вечірні нагадування доступні лише з повним доступом.</b>",
                ):
                    return
            await _show_daily_expense_reminder_settings(q.message, conn, user.id)
            return

        if q.data.startswith("settings:expense_reminders:mode:"):
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Вечірні нагадування доступні лише з повним доступом.</b>",
                ):
                    return
            mode = q.data.rsplit(":", 1)[-1]
            await ExpenseReminderService(conn).update_settings(user.id, mode=mode)
            await _show_daily_expense_reminder_settings(q.message, conn, user.id)
            return

        if q.data.startswith("settings:expense_reminders:hour:"):
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Вечірні нагадування доступні лише з повним доступом.</b>",
                ):
                    return
            try:
                reminder_hour = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                await _show_daily_expense_reminder_settings(q.message, conn, user.id)
                return
            if reminder_hour not in SUPPORTED_DAILY_EXPENSE_REMINDER_HOURS:
                await _show_daily_expense_reminder_settings(q.message, conn, user.id)
                return
            await ExpenseReminderService(conn).update_settings(user.id, reminder_hour=reminder_hour)
            await _show_daily_expense_reminder_settings(q.message, conn, user.id)
            return

        if q.data == "settings:billing":
            await _show_billing_menu(q.message, conn, user.id, access_state=access_state)
            return

        if q.data in {"settings:billing:bind", "settings:billing:rebind"} or q.data.startswith("settings:billing:bind:") or q.data.startswith("settings:billing:rebind:"):
            billing_state = access_state["billing_state"]
            if _billing_recovery_checkout_needed(access_scope=access_scope, state=billing_state):
                try:
                    result = await init_recovery_payment_session(tg_user_id=user.id)
                except BillingAPIError as exc:
                    await _show_billing_menu(
                        q.message,
                        conn,
                        user.id,
                        notice=f"Monobank оплата не стартувала: {escape_html(str(exc))}",
                        access_state=access_state,
                    )
                    return
                await q.message.reply_text(
                    _billing_recovery_confirmation_text(),
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("Відкрити Monobank", url=str(result.get("page_url") or ""))],
                            [_back_btn("settings:billing")] if access_scope in {"paywall", "debt_only"} else [_back_btn("settings:billing"), _home_btn()],
                        ]
                    ),
                )
                return
            mode = "rebind" if ":rebind" in q.data and access_state["billing_state"].get("has_card") else "bind"
            requested_trial_days = promo_trial_days = int(access_state.get("promo_trial_days") or 0)
            trial_days = promo_trial_days or 30
            if q.data.endswith(":90"):
                requested_trial_days = 90
                if not promo_trial_days:
                    trial_days = 90
            elif q.data.endswith(":30"):
                requested_trial_days = 30
                if not promo_trial_days:
                    trial_days = 30
            try:
                result = await init_bind_session(
                    tg_user_id=user.id,
                    trial_days=trial_days,
                    mode=mode,
                    promo_code=_pending_promo_code(access_state),
                )
            except BillingAPIError as exc:
                await _show_billing_menu(q.message, conn, user.id, notice=f"Monobank bind не стартував: {escape_html(str(exc))}", access_state=access_state)
                return
            effective_trial_days = int(result.get("trial_days") or trial_days or requested_trial_days or 30)
            trial_granted = bool(result.get("trial_granted"))
            if "trial_granted" not in result:
                trial_granted = mode == "bind" and effective_trial_days > 0
            text = _billing_bind_confirmation_text(
                trial_granted=trial_granted,
                trial_days=_billing_trial_days(effective_trial_days),
            )
            await q.message.reply_text(
                text,
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton(_billing_bind_button_label(trial_granted=trial_granted), url=str(result.get("page_url") or ""))],
                        [_back_btn("settings:billing")] if access_scope in {"paywall", "debt_only"} else [_back_btn("settings:billing"), _home_btn()],
                    ]
                ),
            )
            return

        if q.data == "settings:billing:disable":
            try:
                await cancel_autorenew(tg_user_id=user.id)
            except BillingAPIError as exc:
                await _show_billing_menu(q.message, conn, user.id, notice=f"Не вдалося вимкнути автопродовження: {escape_html(str(exc))}", access_state=access_state)
                return
            await _show_billing_menu(q.message, conn, user.id, notice="Автопродовження вимкнено.", access_state=await _get_resolved_access_state(conn, user.id))
            return

        if q.data.startswith("settings:billing:retry:confirm:"):
            token = q.data.rsplit(":", 1)[-1]
            pending = context.user_data.get("billing_retry_intent") or {}
            expected_key = str(pending.get("intent_key") or "")
            expected_token = str(pending.get("token") or "")
            if (
                int(pending.get("tg_user_id") or 0) != int(user.id)
                or not expected_key
                or not expected_token
                or not hmac.compare_digest(expected_token, token)
            ):
                await q.message.reply_text(
                    "Це підтвердження застаріло. Відкрийте оплату та сформуйте нове.",
                    reply_markup=InlineKeyboardMarkup([[_back_btn("settings:billing")]]),
                )
                return
            if pending.get("inflight"):
                await q.message.reply_text("Повторна оплата вже обробляється. Зачекайте кілька секунд.")
                return
            pending["inflight"] = True
            context.user_data["billing_retry_intent"] = pending
            try:
                result = await retry_renewal(tg_user_id=user.id, intent_key=expected_key)
            except BillingAPIError as exc:
                pending["inflight"] = False
                context.user_data["billing_retry_intent"] = pending
                await _show_billing_menu(
                    q.message,
                    conn,
                    user.id,
                    notice=f"Не вдалося запустити повторну оплату: {escape_html(str(exc))}",
                    access_state=access_state,
                )
                return
            context.user_data.pop("billing_retry_intent", None)
            action_url = str(result.get("action_url") or "")
            if action_url:
                await q.message.reply_text(
                    "<b>Потрібне підтвердження в Monobank</b>\n\nВідкрийте сторінку оплати та завершіть 3DS-перевірку.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("Підтвердити в Monobank", url=action_url)], [_back_btn("settings:billing")]]
                    ),
                )
                return
            await _show_billing_menu(
                q.message,
                conn,
                user.id,
                notice="Повторну оплату запущено. Якщо Monobank прийме її одразу, статус оновиться автоматично.",
                access_state=await _get_resolved_access_state(conn, user.id),
            )
            return

        if q.data == "settings:billing:retry":
            billing_state = access_state["billing_state"]
            if not billing_state.get("has_card"):
                await _show_billing_menu(
                    q.message,
                    conn,
                    user.id,
                    notice="Спочатку прив'яжіть картку через Monobank.",
                    access_state=access_state,
                )
                return
            if _billing_recovery_checkout_needed(access_scope=access_scope, state=billing_state):
                action_url = str(billing_state.get("last_action_url") or "")
                if action_url:
                    await q.message.reply_text(
                        "<b>Потрібне підтвердження в Monobank</b>\n\nВідкрийте сторінку оплати та завершіть підтвердження.",
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("Підтвердити в Monobank", url=action_url)], [_back_btn("settings:billing")]]
                        ),
                    )
                    return
            token = secrets.token_urlsafe(12)
            context.user_data["billing_retry_intent"] = {
                "tg_user_id": int(user.id),
                "token": token,
                "intent_key": f"billing_retry_{int(user.id)}_{token}",
            }
            await q.message.reply_text(
                "<b>Підтвердьте повторне списання</b>\n\nMonobank отримає один ідемпотентний запит на продовження підписки.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("Підтвердити списання", callback_data=f"settings:billing:retry:confirm:{token}")],
                        [_back_btn("settings:billing")],
                    ]
                ),
            )
            return

        if q.data == "saving:settings:open":
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Налаштування заощаджень доступні лише з повним доступом.</b>",
                ):
                    return
            await _show_saving_settings(q.message, context, user.id, conn=conn)
            return

        if q.data == "settings:accounts:add":
            if await _billing_write_blocked(q.message, conn, user.id):
                return
            await q.message.reply_text(
                "<b>➕ Додати рахунок</b>\n\nОберіть тип рахунку:",
                reply_markup=kb_account_add_menu(),
            )
            return

        if q.data == "settings:accounts:transfer":
            if not _access_scope_has_full_home(access_scope):
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Перекази між рахунками доступні лише з повним доступом.</b>",
                ):
                    return
            await _start_transfer_flow(q.message, context, user.id, conn=conn)
            return

        settings_flow = context.user_data.get("settings_flow") or {}
        if q.data == "settings:acct:cancel":
            origin = str(settings_flow.get("origin") or "settings")
            if origin == "currency_resolution":
                _reset_settings_flow(context)
                resolution_flow = context.user_data.get("currency_resolution_flow") or {}
                await _show_currency_resolution_menu(
                    q.message,
                    resolution_flow,
                    notice="Створення рахунку скасовано.",
                )
            elif origin == "saving":
                await _show_savings_overview(q.message, context, user.id, conn=conn, notice="Створення рахунку скасовано.")
            else:
                await _show_accounts_settings(q.message, context, user.id, notice="Створення рахунку скасовано.", conn=conn)
            return

        if q.data.startswith("settings:acct:cur:"):
            if settings_flow.get("mode") != "account_create":
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            currency = normalize_currency(q.data.rsplit(":", 1)[-1])
            if currency == "OTHER":
                await q.message.reply_text(
                    _currency_code_validation_text(),
                    reply_markup=kb_currency(
                        prefix="settings:acct:cur",
                        include_other=True,
                        currencies=SUPPORTED_ACCOUNT_CURRENCIES,
                    ),
                )
                return
                await q.message.reply_text(
                    "Оберіть валюту зі списку.",
                    reply_markup=kb_currency(
                        prefix="settings:acct:cur",
                        include_other=True,
                        currencies=SUPPORTED_ACCOUNT_CURRENCIES,
                    ),
                )
                return
            if not is_valid_currency_code(currency):
                await q.message.reply_text(
                    _currency_code_validation_text(),
                    reply_markup=kb_currency(
                        prefix="settings:acct:cur",
                        include_other=True,
                        currencies=SUPPORTED_ACCOUNT_CURRENCIES,
                    ),
                )
                return
            _account_create_data(context)["currency"] = currency
            await _ask_account_create_initial_balance(q.message, context)
            return
            preferred_account_type = normalize_account_type(str(settings_flow.get("preferred_account_type") or "other"))
            _account_create_data(context)["account_type"] = preferred_account_type
            settings_flow["step"] = "waiting_for_account_initial_balance"
            context.user_data["settings_flow"] = settings_flow
            balance_prompt = "Введіть поточний баланс рахунку.\n\nЯкщо зараз на ньому 0, просто введіть 0."
            if preferred_account_type == CREDIT_ACCOUNT_TYPE:
                balance_prompt = (
                    "Введіть поточний баланс кредитки.\n\n"
                    "Можна вводити 0, плюс або мінус.\n"
                    "Наприклад: <code>-3500</code>, <code>0</code>, <code>250</code>."
                )
            await q.message.reply_text(
                balance_prompt,
                reply_markup=kb_inline_cancel(str(settings_flow.get("cancel_callback") or "settings:acct:cancel")),
            )
            return

        if q.data.startswith("settings:acct:type:"):
            if settings_flow.get("mode") != "account_create":
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            account_type = normalize_account_type(q.data.rsplit(":", 1)[-1])
            _account_create_data(context)["account_type"] = account_type
            settings_flow["step"] = "waiting_for_account_initial_balance"
            context.user_data["settings_flow"] = settings_flow
            balance_prompt = "Введіть початковий баланс рахунку.\n\nЯкщо баланс 0, просто введіть 0."
            if account_type == CREDIT_ACCOUNT_TYPE:
                balance_prompt = (
                    "Введіть початковий баланс кредитки.\n\n"
                    "Можна вводити 0, плюс або мінус.\n"
                    "Наприклад: <code>-3500</code>, <code>0</code>, <code>250</code>."
                )
            await q.message.reply_text(
                balance_prompt,
                reply_markup=kb_inline_cancel("settings:acct:cancel"),
            )
            return

        if q.data == "settings:acct:confirm:reset":
            preferred_account_type = normalize_account_type(str(settings_flow.get("preferred_account_type") or "main"))
            preserve_saving_flow = _should_preserve_saving_flow_for_account_create(context.user_data.get("saving_flow"))
            if str(settings_flow.get("origin") or "settings") == "currency_resolution":
                await _start_currency_resolution_account_create(
                    q.message,
                    context,
                    context.user_data.get("currency_resolution_flow") or {},
                )
                return
            await _start_account_create_flow(
                q.message,
                context,
                account_type=preferred_account_type,
                preferred_currency=str(settings_flow.get("preferred_currency") or "") or None,
                include_goal_fields=bool(settings_flow.get("include_goal_fields")),
                origin=str(settings_flow.get("origin") or "settings"),
                cancel_callback=str(settings_flow.get("cancel_callback") or "settings:acct:cancel"),
                preserve_saving_flow=preserve_saving_flow,
            )
            return

        if q.data == "settings:acct:confirm:create":
            if settings_flow.get("mode") != "account_create":
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            if await _billing_write_blocked(q.message, conn, user.id):
                return
            account_data = settings_flow.get("account") or {}
            name = normalize_account_name(str(account_data.get("name") or ""))
            currency = normalize_currency(str(account_data.get("currency") or ""))
            account_type = normalize_account_type(str(account_data.get("account_type") or "other"))
            initial_balance = account_data.get("initial_balance")
            credit_limit = account_data.get("credit_limit")
            monthly_interest_rate = account_data.get("monthly_interest_rate")
            if not name or not is_valid_currency_code(currency) or not isinstance(initial_balance, Decimal):
                origin = str(settings_flow.get("origin") or "settings")
                if origin == "saving":
                    await _start_savings_account_create_flow(q.message, context)
                elif origin == "currency_resolution":
                    await _start_currency_resolution_account_create(
                        q.message,
                        context,
                        context.user_data.get("currency_resolution_flow") or {},
                    )
                else:
                    await _start_account_create_flow(
                        q.message,
                        context,
                        preferred_currency=str(settings_flow.get("preferred_currency") or "") or None,
                    )
                return
            if account_type == CREDIT_ACCOUNT_TYPE and (
                not isinstance(credit_limit, Decimal)
                or credit_limit <= 0
                or not isinstance(monthly_interest_rate, Decimal)
                or monthly_interest_rate <= 0
            ):
                if str(settings_flow.get("origin") or "settings") == "currency_resolution":
                    await _start_currency_resolution_account_create(
                        q.message,
                        context,
                        context.user_data.get("currency_resolution_flow") or {},
                    )
                else:
                    await _start_account_create_flow(
                        q.message,
                        context,
                        account_type=CREDIT_ACCOUNT_TYPE,
                        preferred_currency=str(settings_flow.get("preferred_currency") or "") or None,
                        include_goal_fields=False,
                        origin=str(settings_flow.get("origin") or "settings"),
                        cancel_callback=str(settings_flow.get("cancel_callback") or "settings:acct:cancel"),
                        preserve_saving_flow=False,
                    )
                return
            existing_accounts = await _get_accounts_full(conn, user.id)
            if _onb_has_name_conflict([str(row["label"]) for row in existing_accounts], name):
                context.user_data["settings_flow"] = _new_account_create_flow(
                    preferred_account_type=account_type,
                    preferred_currency=currency or str(settings_flow.get("preferred_currency") or "") or None,
                    origin=str(settings_flow.get("origin") or "settings"),
                    cancel_callback=str(settings_flow.get("cancel_callback") or "settings:acct:cancel"),
                    include_goal_fields=bool(settings_flow.get("include_goal_fields")),
                )
                await q.message.reply_text(
                    "Рахунок з такою назвою вже існує.\nВведіть іншу назву рахунку.",
                    reply_markup=kb_inline_cancel(str(settings_flow.get("cancel_callback") or "settings:acct:cancel")),
                )
                return
            created_account_id = await AccountService(conn).create_account(
                user.id,
                label=name,
                currency=currency,
                account_type=account_type,
                starting_balance=initial_balance,
                goal_name=account_data.get("goal_name"),
                goal_amount=account_data.get("goal_amount"),
                goal_date=account_data.get("goal_date"),
                credit_limit=credit_limit,
                monthly_interest_rate=monthly_interest_rate,
                non_negative_account_type="main" if account_type == CREDIT_ACCOUNT_TYPE else account_type,
            )
            if str(settings_flow.get("origin") or "settings") == "currency_resolution":
                created_account = await _get_active_account_by_id(conn, user.id, created_account_id)
                if created_account is not None:
                    await _resume_currency_resolution_with_created_account(
                        q.message,
                        context,
                        user.id,
                        created_account,
                        notice=f'Рахунок "{name}" створено.',
                    )
                    return
                _reset_settings_flow(context)
                await _show_currency_resolution_menu(
                    q.message,
                    context.user_data.get("currency_resolution_flow") or {},
                    notice=f'Рахунок "{name}" створено, але не вдалося відкрити його для продовження операції.',
                )
                return
            if str(settings_flow.get("origin") or "settings") == "saving":
                saving_flow = context.user_data.get("saving_flow") or {}
                mode = str(saving_flow.get("mode") or "")
                if mode in {"post_income_prompt", "manual_topup", "create_plan"}:
                    created_account = await _get_active_account_by_id(conn, user.id, created_account_id)
                    if created_account is not None and is_savings_account_type(str(created_account["account_type"] or "other")):
                        if mode == "create_plan":
                            await _create_pending_saving_plan(q.message, context, user.id, saving_flow, created_account, conn=conn)
                            return
                        saving_flow.update(_saving_target_payload(created_account))
                        if mode == "manual_topup":
                            await _show_topup_source_picker(q.message, context, user.id, saving_flow, conn=conn)
                            return
                        if mode == "post_income_prompt":
                            if "amount" not in saving_flow and isinstance(saving_flow.get("primary_amount"), Decimal):
                                saving_flow["amount"] = saving_flow["primary_amount"]
                            if not isinstance(saving_flow.get("amount"), Decimal) or Decimal(str(saving_flow.get("amount") or 0)) <= 0:
                                _reset_saving_flow(context)
                                await _show_savings_overview(
                                    q.message,
                                    context,
                                    user.id,
                                    conn=conn,
                                    notice=f'Рахунок "{name}" створено.',
                                )
                                return
                            if saving_flow["target_currency"] != normalize_currency(str(saving_flow.get("source_currency") or "UAH")):
                                saving_flow["step"] = "await_fx_rate"
                                context.user_data["saving_flow"] = saving_flow
                                await _show_saving_rate_step(q.message, saving_flow, notice=f'Накопичення "{name}" створено.')
                                return
                            await _show_saving_transfer_confirmation(q.message, context, saving_flow, conn=conn)
                            return
                _reset_saving_flow(context)
                await _show_savings_overview(
                    q.message,
                    context,
                    user.id,
                    conn=conn,
                    notice=f'Рахунок "{name}" створено.',
                )
                return
            await _show_accounts_settings(
                q.message,
                context,
                user.id,
                notice=f'Рахунок "{name}" створено.',
                conn=conn,
            )
            return

        if q.data == "settings:currency":
            context.user_data["settings_flow"] = {"await_currency_text": False}
            await q.message.reply_text("Оберіть базову валюту:", reply_markup=kb_currency(prefix="settings:cur"))
            return

        if q.data.startswith("settings:cur:"):
            cur = normalize_currency(q.data.rsplit(":", 1)[-1])
            if cur == "OTHER":
                context.user_data["settings_flow"] = {"await_currency_text": True}
                await q.message.reply_text(_currency_code_validation_text(), reply_markup=_kb_home_only())
                return
                await q.message.reply_text("Введіть валюту (наприклад: UAH або USD):", reply_markup=_kb_home_only())
                return
            if not is_valid_currency_code(cur):
                await q.message.reply_text(_currency_code_validation_text(), reply_markup=kb_currency(prefix="settings:cur"))
                return
            if not cur:
                await q.message.reply_text("Не зрозумів валюту. Спробуйте ще раз.", reply_markup=kb_currency(prefix="settings:cur"))
                return
            await conn.execute("UPDATE users SET base_currency=$2 WHERE tg_user_id=$1", user.id, cur)
            _reset_settings_flow(context)
            await q.message.reply_text(f"✅ Базову валюту оновлено: {cur}", reply_markup=kb_settings_menu())
            return


_TRIAL_RECOVERY_OWNER_REPLY_KEY = "trial_recovery_owner_reply"


def _trial_recovery_owner_ids() -> set[int]:
    operator_ids = {
        int(value)
        for value in getattr(config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [])
        if str(value).isdigit()
    }
    if operator_ids:
        return operator_ids
    return {
        int(value)
        for value in getattr(config, "ADMIN_TELEGRAM_IDS", [])
        if str(value).isdigit()
    }


def _trial_recovery_profile_url(snapshot: dict[str, Any]) -> str:
    username = str(snapshot.get("username") or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        return f"https://t.me/{username}"
    return f"tg://user?id={int(snapshot.get('tg_user_id') or 0)}"


def _trial_recovery_owner_notification_keyboard(
    snapshot: dict[str, Any],
    *,
    allow_reply: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if allow_reply:
        rows.append(
            [
                InlineKeyboardButton(
                    "✍️ Відповісти в боті",
                    callback_data=f"trialrec:a:{int(snapshot['recipient_id'])}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("👤 Відкрити профіль", url=_trial_recovery_profile_url(snapshot))])
    return InlineKeyboardMarkup(rows)


async def _notify_trial_recovery_owners(
    context: ContextTypes.DEFAULT_TYPE,
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
    event: str,
    telegram_user: Any,
    reason: str | None = None,
    free_text: str | None = None,
    case_id: int | None = None,
) -> None:
    try:
        snapshot = await get_trial_recovery_owner_snapshot(conn, recipient_id=recipient_id)
    except Exception as exc:  # pragma: no cover - delivery must never break the user's response
        logger.warning("Could not load recovery owner snapshot for %s: %s", recipient_id, exc)
        snapshot = None
    if snapshot is None:
        snapshot = {
            "recipient_id": recipient_id,
            "tg_user_id": int(getattr(telegram_user, "id", 0) or 0),
            "first_name": getattr(telegram_user, "first_name", None),
            "last_name": getattr(telegram_user, "last_name", None),
            "username": getattr(telegram_user, "username", None),
            "lang": getattr(telegram_user, "language_code", None),
            "reason": reason or "",
            "free_text": free_text or "",
            "recovery_status": "opted_out" if event == "opt_out" else "responded",
            "support_case_id": case_id,
        }
    allow_reply = event != "opt_out" and owner_reply_allowed(snapshot)
    message_text = format_trial_recovery_owner_notification(
        snapshot,
        event=event,
        reason=reason,
        free_text=free_text,
        case_id=case_id,
        timezone_name=str(getattr(config, "TZ", "Europe/Istanbul") or "Europe/Istanbul"),
        allow_reply=allow_reply,
    )
    event_type = {
        "reason": "trial_recovery_response",
        "details": "trial_recovery_details",
        "contact": "trial_recovery_contact_requested",
        "opt_out": "trial_recovery_opt_out",
    }.get(event, "trial_recovery_signal")
    try:
        await log_admin_notification(
            conn,
            event_type,
            {
                "telegram_user_id": int(snapshot.get("tg_user_id") or 0),
                "recipient_id": recipient_id,
                "support_case_id": case_id or snapshot.get("support_case_id"),
                "reason": reason or snapshot.get("reason") or "",
            },
        )
    except Exception as exc:  # pragma: no cover - notification logging is best effort
        logger.warning("Could not log recovery owner notification for %s: %s", recipient_id, exc)
    await notify_admins(
        context.application.bot,
        message_text,
        chat_ids=_trial_recovery_owner_ids(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=_trial_recovery_owner_notification_keyboard(snapshot, allow_reply=allow_reply),
    )


async def _start_trial_recovery_owner_reply(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
) -> None:
    snapshot = await get_trial_recovery_owner_snapshot(conn, recipient_id=recipient_id)
    if snapshot is None:
        await message.reply_text("Не знайшов цього recovery-запису. Оновіть сторінку кампанії та спробуйте ще раз.")
        return
    if not owner_reply_allowed(snapshot):
        await message.reply_text("Користувач відмовився від контакту або зараз не може отримати повідомлення від бота.")
        return
    target_id = int(snapshot["tg_user_id"])
    username = str(snapshot.get("username") or "").strip()
    target_label = f"@{username}" if username else str(target_id)
    context.user_data[_TRIAL_RECOVERY_OWNER_REPLY_KEY] = {
        "recipient_id": recipient_id,
        "telegram_user_id": target_id,
        "lang": str(snapshot.get("lang") or "uk"),
        "target_label": target_label,
    }
    cancel_keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Скасувати", callback_data=f"trialrec:x:{recipient_id}")]]
    )
    await message.reply_text(
        f"✍️ Відповідь для {target_label} (ID {target_id})\n\n"
        "Надішліть наступним повідомленням текст відповіді. Він одразу піде користувачу від імені бота.",
        reply_markup=cancel_keyboard,
    )


async def _consume_trial_recovery_owner_reply(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> bool:
    state = context.user_data.get(_TRIAL_RECOVERY_OWNER_REPLY_KEY)
    user = update.effective_user
    message = update.message
    if not state or not user or not message or message.text is None:
        return False
    if int(user.id) not in _trial_recovery_owner_ids():
        context.user_data.pop(_TRIAL_RECOVERY_OWNER_REPLY_KEY, None)
        return False
    clean_text = str(message.text or "").strip()
    if not clean_text:
        await message.reply_text("Відповідь не може бути порожньою. Напишіть текст або натисніть «Скасувати».")
        return True
    if len(clean_text) > 3500:
        await message.reply_text("Відповідь задовга. Скоротіть її до 3500 символів і надішліть ще раз.")
        return True

    recipient_id = int(state["recipient_id"])
    async with _pool(context).acquire() as conn:
        snapshot = await get_trial_recovery_owner_snapshot(conn, recipient_id=recipient_id)
        if snapshot is None:
            context.user_data.pop(_TRIAL_RECOVERY_OWNER_REPLY_KEY, None)
            await message.reply_text("Recovery-запис уже недоступний. Відповідь не надіслано.")
            return True
        if not owner_reply_allowed(snapshot):
            context.user_data.pop(_TRIAL_RECOVERY_OWNER_REPLY_KEY, None)
            await message.reply_text("Користувач відмовився від контакту або зараз не може отримати повідомлення від бота.")
            return True

        target_id = int(snapshot["tg_user_id"])
        locale = "en" if str(snapshot.get("lang") or "").lower().startswith("en") else "uk"
        outgoing_text = (
            "💬 Personal reply from the founder of vydno.capital\n\n"
            if locale == "en"
            else "💬 Особиста відповідь від засновника vydno.capital\n\n"
        ) + clean_text
        user_reply_keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "↩️ Reply" if locale == "en" else "↩️ Відповісти",
                        callback_data=f"trialrec:t:{recipient_id}",
                    )
                ]
            ]
        )
        try:
            sent_message = await context.application.bot.send_message(
                chat_id=target_id,
                text=outgoing_text,
                reply_markup=user_reply_keyboard,
            )
        except Exception as exc:  # pragma: no cover - Telegram errors depend on live chat state
            logger.warning("Recovery owner reply failed for %s: %s", target_id, exc)
            await message.reply_text(
                "Не вдалося доставити відповідь. Текст залишився в режимі відповіді — спробуйте ще раз або натисніть «Скасувати»."
            )
            return True
        try:
            await log_admin_notification(
                conn,
                "trial_recovery_owner_reply_sent",
                {
                    "telegram_user_id": target_id,
                    "recipient_id": recipient_id,
                    "telegram_message_id": getattr(sent_message, "message_id", None),
                    "owner_telegram_user_id": int(user.id),
                },
            )
        except Exception as exc:  # pragma: no cover - sent replies must not be retried because audit logging failed
            logger.warning("Could not log sent recovery owner reply for %s: %s", target_id, exc)

    context.user_data.pop(_TRIAL_RECOVERY_OWNER_REPLY_KEY, None)
    target_label = str(state.get("target_label") or target_id)
    await message.reply_text(f"✅ Відповідь надіслано {target_label} (ID {target_id}).")
    return True


def _trial_recovery_followup_keyboard(recipient_id: int, locale: str) -> InlineKeyboardMarkup:
    is_en = str(locale or "").startswith("en")
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Yes, message me" if is_en else "Так, напишіть мені", callback_data=f"trialrec:c:{recipient_id}")],
            [InlineKeyboardButton("Add details" if is_en else "Додати деталі", callback_data=f"trialrec:t:{recipient_id}")],
            [InlineKeyboardButton("No, thanks" if is_en else "Ні, дякую", callback_data=f"trialrec:n:{recipient_id}")],
            [InlineKeyboardButton("Activate 90 days" if is_en else "Активувати 90 днів", callback_data="settings:billing:bind:90")],
        ]
    )


async def trial_recovery_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    user = update.effective_user
    if not q or not user or not q.data:
        return
    await q.answer()
    parts = q.data.split(":")
    if len(parts) < 3:
        return
    action = parts[1]
    try:
        recipient_id = int(parts[2])
    except (TypeError, ValueError):
        return

    if action in {"a", "x"} and int(user.id) not in _trial_recovery_owner_ids():
        await q.message.reply_text("Ця дія доступна лише власнику бота.")
        return

    async with _pool(context).acquire() as conn:
        if action == "x":
            context.user_data.pop(_TRIAL_RECOVERY_OWNER_REPLY_KEY, None)
            await q.message.reply_text("Режим відповіді скасовано.")
            return

        if action == "a":
            await _start_trial_recovery_owner_reply(
                q.message,
                context,
                conn,
                recipient_id=recipient_id,
            )
            return

        locale = await _activate_locale(
            context,
            tg_user_id=user.id,
            conn=conn,
            telegram_locale=getattr(user, "language_code", None),
        )
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return

        if action == "r" and len(parts) >= 4:
            result = await record_trial_recovery_reason(
                conn,
                recipient_id=recipient_id,
                tg_user_id=user.id,
                reason=parts[3],
            )
            if result is None:
                await q.message.reply_text("This answer is no longer active." if locale == "en" else "Цю відповідь уже не можна змінити.")
                return
            label = trial_recovery_reason_label(str(result["reason"]), locale)
            if result.get("notification_required"):
                await _notify_trial_recovery_owners(
                    context,
                    conn,
                    recipient_id=recipient_id,
                    event="reason",
                    telegram_user=user,
                    reason=str(result["reason"]),
                )
            text = (
                f"Thank you — I saved your answer: “{label}”. Would you like me to message you personally?"
                if locale == "en"
                else f"Дякую — відповідь «{label}» збережено. Хочете, щоб я написав вам особисто?"
            )
            await q.message.reply_text(text, reply_markup=_trial_recovery_followup_keyboard(recipient_id, locale))
            return

        if action == "t":
            started = await begin_trial_recovery_free_text(conn, recipient_id=recipient_id, tg_user_id=user.id)
            if started:
                text = (
                    "Send one text message with the details within 24 hours."
                    if locale == "en"
                    else "Напишіть деталі одним текстовим повідомленням протягом 24 годин."
                )
                await q.message.reply_text(text)
            return

        if action == "c":
            result = await request_trial_recovery_contact(conn, recipient_id=recipient_id, tg_user_id=user.id)
            if result is None:
                await q.message.reply_text("This request is no longer active." if locale == "en" else "Цей запит уже неактивний.")
                return
            case_id = int(result["support_case_id"])
            if result.get("created"):
                await _notify_trial_recovery_owners(
                    context,
                    conn,
                    recipient_id=recipient_id,
                    event="contact",
                    telegram_user=user,
                    reason=str(result["reason"]),
                    free_text=str(result["free_text"] or ""),
                    case_id=case_id,
                )
            await q.message.reply_text(
                ("Thank you. Your request is saved as case #" if locale == "en" else "Дякую. Запит збережено як звернення №")
                + str(case_id)
                + (". I’ll message you here." if locale == "en" else ". Я напишу вам тут."),
            )
            return

        if action == "n":
            await decline_trial_recovery_contact(conn, recipient_id=recipient_id, tg_user_id=user.id)
            await q.message.reply_text("Understood, thank you." if locale == "en" else "Зрозумів, дякую.")
            return

        if action == "o":
            changed = await opt_out_trial_recovery(conn, recipient_id=recipient_id, tg_user_id=user.id)
            if changed:
                await _notify_trial_recovery_owners(
                    context,
                    conn,
                    recipient_id=recipient_id,
                    event="opt_out",
                    telegram_user=user,
                )
            await q.message.reply_text(
                "Done. I won’t message you about the trial again." if locale == "en" else "Готово. Більше не писатиму вам про trial."
            )
            return


async def poll_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q or not update.effective_user:
        return
    await q.answer()

    user = update.effective_user
    await _consume_pending_admin_reset_if_needed(context, user.id)
    parts = q.data.split(":")
    if len(parts) < 3:
        return

    async with _pool(context).acquire() as conn:
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return

        if parts[1] == "text":
            campaign_id = int(parts[2])
            context.user_data["poll_text_flow"] = {"campaign_id": campaign_id}
            await q.message.reply_text("Напишіть відповідь одним повідомленням.", reply_markup=_kb_home_only())
            return

        if parts[1] == "answer" and len(parts) >= 4:
            campaign_id = int(parts[2])
            answer = ":".join(parts[3:])
            rating_value = int(answer) if answer.isdigit() else None
            await save_poll_response(
                conn,
                campaign_id=campaign_id,
                tg_user_id=user.id,
                answer=answer,
                rating_value=rating_value,
            )
            if rating_value is not None:
                threshold = await get_bot_setting(conn, "poll_low_rating_threshold", 3)
                threshold_int = int(threshold) if str(threshold).isdigit() else 3
                if rating_value <= threshold_int:
                    await create_feedback_from_poll(
                        conn,
                        tg_user_id=user.id,
                        category="ux",
                        text=f"Low rating from poll #{campaign_id}",
                        rating=rating_value,
                    )
            await q.message.reply_text("Дякую! Відповідь збережено.", reply_markup=_kb_home_only())
            return


async def _handle_account_create_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    settings_flow = context.user_data.get("settings_flow") or {}
    if settings_flow.get("mode") != "account_create":
        return

    step = str(settings_flow.get("step") or "")
    text = update.message.text.strip()
    cancel_callback = str(settings_flow.get("cancel_callback") or "settings:acct:cancel")

    if step == "waiting_for_account_name":
        name = normalize_account_name(text)
        if not name:
            await update.message.reply_text(
                "Назва рахунку не може бути порожньою. Спробуйте ще раз.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if len(name) < 2:
            await update.message.reply_text(
                "Назва рахунку має містити мінімум 2 символи. Спробуйте ще раз.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if len(name) > 50:
            await update.message.reply_text(
                "Назва рахунку має містити максимум 50 символів. Спробуйте ще раз.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if _onb_is_digits_only(name):
            await update.message.reply_text(
                "Назва рахунку не може складатися тільки з цифр. Спробуйте ще раз.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return

        async with _pool(context).acquire() as conn:
            if not await _user_ready(conn, user.id):
                _reset_settings_flow(context)
                await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
                return
            existing_accounts = await _get_accounts_full(conn, user.id)
        if _onb_has_name_conflict([str(row["label"]) for row in existing_accounts], name):
            await update.message.reply_text(
                "Рахунок з такою назвою вже існує. Введіть іншу назву.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return

        _account_create_data(context)["name"] = name
        preferred_currency = normalize_currency(str(settings_flow.get("preferred_currency") or ""))
        if preferred_currency and is_valid_currency_code(preferred_currency):
            _account_create_data(context)["currency"] = preferred_currency
            await _ask_account_create_initial_balance(update.message, context)
            return
        settings_flow["step"] = "waiting_for_account_currency"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            "Оберіть валюту рахунку:",
            reply_markup=kb_currency(
                prefix="settings:acct:cur",
                include_other=True,
                currencies=SUPPORTED_ACCOUNT_CURRENCIES,
            ),
        )
        return

    if step == "waiting_for_account_initial_balance":
        account_type = normalize_account_type(str(_account_create_data(context).get("account_type") or "other"))
        balance = parse_decimal_amount(text, allow_negative=(account_type == CREDIT_ACCOUNT_TYPE))
        if balance is None:
            await update.message.reply_text(
                "Не вдалося розпізнати суму.\n\nВведіть число, наприклад:\n1000\n1000.50\n1000,50",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if account_type != CREDIT_ACCOUNT_TYPE and balance < 0:
            await update.message.reply_text(
                "Для звичайного рахунку баланс не може бути від’ємним. Якщо це кредитка, створіть її як окремий тип рахунку.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        _account_create_data(context)["initial_balance"] = balance
        if account_type == CREDIT_ACCOUNT_TYPE:
            settings_flow["step"] = "waiting_for_account_credit_limit"
            context.user_data["settings_flow"] = settings_flow
            await update.message.reply_text(
                "Введіть кредитний ліміт.\n\nНаприклад: <code>20000</code>",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if bool(settings_flow.get("include_goal_fields")):
            settings_flow["step"] = "waiting_for_account_goal_amount"
            context.user_data["settings_flow"] = settings_flow
            await update.message.reply_text(
                "Якщо хочете, введіть цільову суму для цього рахунку.\n\n"
                "Якщо цілі поки немає, введіть 0.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        settings_flow["step"] = "waiting_for_account_confirmation"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            _account_create_summary(_account_create_data(context)),
            reply_markup=kb_account_create_confirm(cancel_callback=cancel_callback),
        )
        return

    if step == "waiting_for_account_credit_limit":
        credit_limit = parse_decimal_amount(text, allow_negative=False)
        if credit_limit is None or credit_limit <= 0:
            await update.message.reply_text(
                "Введіть кредитний ліміт числом більше 0.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        _account_create_data(context)["credit_limit"] = credit_limit
        settings_flow["step"] = "waiting_for_account_monthly_interest_rate"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            "Введіть % нарахування на місяць.\n\nНаприклад: <code>3.5</code>",
            reply_markup=kb_inline_cancel(cancel_callback),
        )
        return

    if step == "waiting_for_account_monthly_interest_rate":
        monthly_interest_rate = parse_decimal_rate(text)
        if monthly_interest_rate is None:
            await update.message.reply_text(
                "Введіть відсоток числом більше 0. Наприклад: <code>3.5</code>.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        _account_create_data(context)["monthly_interest_rate"] = monthly_interest_rate
        settings_flow["step"] = "waiting_for_account_confirmation"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            _account_create_summary(_account_create_data(context)),
            reply_markup=kb_account_create_confirm(cancel_callback=cancel_callback),
        )
        return

    if step == "waiting_for_account_currency":
        currency = normalize_currency(text)
        if not is_valid_currency_code(currency):
            await update.message.reply_text(
                _currency_code_validation_text(),
                reply_markup=kb_currency(
                    prefix="settings:acct:cur",
                    include_other=True,
                    currencies=SUPPORTED_ACCOUNT_CURRENCIES,
                ),
            )
            return
        _account_create_data(context)["currency"] = currency
        await _ask_account_create_initial_balance(update.message, context)
        return
        await update.message.reply_text(
            "Оберіть валюту рахунку кнопками нижче.",
            reply_markup=kb_currency(
                prefix="settings:acct:cur",
                include_other=True,
                currencies=SUPPORTED_ACCOUNT_CURRENCIES,
            ),
        )
        return

    if step == "waiting_for_account_type":
        await update.message.reply_text(
            "Оберіть тип рахунку кнопками нижче.",
            reply_markup=kb_account_type(prefix="settings:acct:type", cancel_callback=cancel_callback, include_credit=True),
        )
        return

    if step == "waiting_for_account_goal_amount":
        goal_amount = parse_decimal_amount(text, allow_negative=False)
        if goal_amount is None:
            await update.message.reply_text(
                "Введіть цільову суму числом або 0, якщо поки без цілі.",
                reply_markup=kb_inline_cancel(cancel_callback),
            )
            return
        if goal_amount <= 0:
            _account_create_data(context)["goal_amount"] = None
            _account_create_data(context)["goal_date"] = None
            settings_flow["step"] = "waiting_for_account_confirmation"
            context.user_data["settings_flow"] = settings_flow
            await update.message.reply_text(
                _account_create_summary(_account_create_data(context)),
                reply_markup=kb_account_create_confirm(cancel_callback=cancel_callback),
            )
            return
        _account_create_data(context)["goal_amount"] = goal_amount
        settings_flow["step"] = "waiting_for_account_goal_date"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            "Введіть дату цілі у форматі ДД.ММ.РРРР.\n\nЯкщо дати поки немає, введіть 0.",
            reply_markup=kb_inline_cancel(cancel_callback),
        )
        return

    if step == "waiting_for_account_goal_date":
        if text == "0":
            _account_create_data(context)["goal_date"] = None
        else:
            goal_date = _parse_date_ddmmyyyy(text)
            if goal_date is None:
                await update.message.reply_text(
                    "Не вдалося розпізнати дату. Використайте формат ДД.ММ.РРРР або введіть 0.",
                    reply_markup=kb_inline_cancel(cancel_callback),
                )
                return
            _account_create_data(context)["goal_date"] = goal_date
        settings_flow["step"] = "waiting_for_account_confirmation"
        context.user_data["settings_flow"] = settings_flow
        await update.message.reply_text(
            _account_create_summary(_account_create_data(context)),
            reply_markup=kb_account_create_confirm(cancel_callback=cancel_callback),
        )
        return

    await update.message.reply_text(
        _account_create_summary(_account_create_data(context)),
        reply_markup=kb_account_create_confirm(cancel_callback=cancel_callback),
    )


async def _handle_account_rename_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    accounts_flow = context.user_data.get("accounts_flow") or {}
    if accounts_flow.get("step") != "rename":
        return

    text = normalize_account_name(update.message.text)
    if not text:
        await update.message.reply_text("Назва рахунку не може бути порожньою. Спробуйте ще раз.")
        return
    if len(text) < 2:
        await update.message.reply_text("Назва рахунку має містити мінімум 2 символи. Спробуйте ще раз.")
        return
    if len(text) > 50:
        await update.message.reply_text("Назва рахунку має містити максимум 50 символів. Спробуйте ще раз.")
        return
    if _onb_is_digits_only(text):
        await update.message.reply_text("Назва рахунку не може складатися тільки з цифр. Спробуйте ще раз.")
        return

    account_id = int(accounts_flow.get("account_id") or 0)
    if account_id <= 0:
        context.user_data.pop("accounts_flow", None)
        await update.message.reply_text("Не вдалося знайти рахунок для перейменування.")
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            context.user_data.pop("accounts_flow", None)
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return

        account_service = AccountService(conn)
        account = await account_service.get_active_account_by_id(user.id, account_id)
        if not account:
            context.user_data.pop("accounts_flow", None)
            await update.message.reply_text("Не вдалося знайти рахунок для перейменування.")
            return

        existing_accounts = await _get_accounts_full(conn, user.id)
        other_names = [str(row["label"]) for row in existing_accounts if int(row["id"]) != account_id]
        if _onb_has_name_conflict(other_names, text):
            await update.message.reply_text("Рахунок з такою назвою вже існує. Введіть іншу назву.")
            return

        await account_service.rename_account(user.id, account_id, text)

        context.user_data.pop("accounts_flow", None)
        await _show_accounts_settings(
            update.message,
            context,
            user.id,
            notice=f"✅ Рахунок перейменовано: {escape_html(text)}",
            conn=conn,
        )


async def _handle_account_balance_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    accounts_flow = context.user_data.get("accounts_flow") or {}
    if accounts_flow.get("step") != "balance":
        return

    text = update.message.text.strip()
    if not text:
        await update.message.reply_text("Баланс не може бути порожнім. Спробуйте ще раз.")
        return
    target_balance = parse_decimal_amount(text, allow_negative=True)
    if target_balance is None:
        await update.message.reply_text(
            "Не вдалося розпізнати суму.\n\nВведіть число, наприклад:\n1000\n1000.50\n1000,50",
            reply_markup=kb_inline_cancel("accounts:cancel"),
        )
        return

    account_id = int(accounts_flow.get("account_id") or 0)
    if account_id <= 0:
        context.user_data.pop("accounts_flow", None)
        await update.message.reply_text("Не вдалося знайти рахунок для корекції балансу.")
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            context.user_data.pop("accounts_flow", None)
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return

        async with conn.transaction():
            account_service = AccountService(conn)
            account = await account_service.get_active_account_by_id(user.id, account_id, for_update=True)
            if account is None:
                context.user_data.pop("accounts_flow", None)
                await update.message.reply_text("Не вдалося знайти рахунок для корекції балансу.")
                return

            current_balance = quantize_money(Decimal(str(account["balance"] or 0)))
            currency = normalize_currency(str(account["currency"] or "UAH"))
            if target_balance == current_balance:
                context.user_data.pop("accounts_flow", None)
                await _show_accounts_settings(
                    update.message,
                    context,
                    user.id,
                    notice="Баланс уже відповідає введеній сумі. Корекція не потрібна.",
                    conn=conn,
                )
                return

        flow = {
            "step": "balance_confirm", "account_id": account_id,
            "current_balance": current_balance, "target_balance": target_balance,
            "currency": currency,
        }
        context.user_data["accounts_flow"] = flow
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Підтвердити корекцію", callback_data="accounts:balance:confirm")],
            [InlineKeyboardButton("❌ Скасувати", callback_data="accounts:cancel")],
        ])
        await update.message.reply_text(
            "<b>Перевірте корекцію балансу</b>\n\n"
            f"<b>Було:</b> {format_money(current_balance, currency)}\n"
            f"<b>Стало:</b> {format_money(target_balance, currency)}\n"
            f"<b>Корекція:</b> {format_money(target_balance - current_balance, currency)}",
            reply_markup=bind_financial_preview(flow, markup),
        )


async def _handle_account_balance_confirmation(q, context: ContextTypes.DEFAULT_TYPE, tg_user_id: int) -> None:
    flow = context.user_data.get("accounts_flow") or {}
    if flow.get("step") != "balance_confirm" or not is_current_confirmation(flow, q.data):
        await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
        return
    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, tg_user_id) or await _billing_write_blocked(q.message, conn, tg_user_id):
            return
        if not consume_financial_confirmation(flow, q.data):
            await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
            return
        try:
            delta, tx_amount = await AccountService(conn).create_balance_correction(
                tg_user_id, int(flow["account_id"]), flow["current_balance"],
                flow["target_balance"], flow["currency"],
            )
        except ValueError:
            context.user_data.pop("accounts_flow", None)
            await q.message.reply_text("Баланс або рахунок змінився. Введіть баланс повторно й перевірте нову корекцію.", reply_markup=kb_home())
            return
        context.user_data.pop("accounts_flow", None)
        await _show_accounts_settings(
            q.message, context, tg_user_id, conn=conn,
            notice="<b>✅ Баланс скориговано</b>\n\n"
            f"<b>Було:</b> {format_money(flow['current_balance'], flow['currency'])}\n"
            f"<b>Стало:</b> {format_money(flow['target_balance'], flow['currency'])}\n"
            f"<b>Корекція:</b> {'+' if delta > 0 else '-'}{format_money(tx_amount, flow['currency'])}",
        )


def _transfer_flow_source_balance(flow: dict) -> Decimal:
    return flow.get("source_balance") if isinstance(flow.get("source_balance"), Decimal) else Decimal(str(flow.get("source_balance") or 0))


def _insufficient_funds_text(label: str, available: Decimal, attempted: Decimal, currency: str) -> str:
    return "\n".join(
        [
            "<b>❌ Недостатньо коштів</b>",
            "",
            f"<b>Доступно:</b> {format_money(available, currency)}",
            f"<b>Ви намагаєтесь списати:</b> {format_money(attempted, currency)}",
            "",
            f"Введіть суму ще раз для рахунку <b>{escape_html(label)}</b>.",
        ]
    )


def _kb_transfer_rate_choice() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📈 Використати курс бота", callback_data="transfer:rate:auto"),
                InlineKeyboardButton("✍️ Ввести свій курс", callback_data="transfer:rate:manual"),
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="transfer:back"), InlineKeyboardButton("❌ Скасувати", callback_data="transfer:cancel")],
        ]
    )


async def _show_transfer_source_step(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_transfer_source_step(message, context, tg_user_id, conn=own_conn, notice=notice)
            return
    await _recalculate_account_balances(conn, tg_user_id)
    accounts = await _get_accounts_full(conn, tg_user_id)
    if len(accounts) < 2:
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice=notice or "<b>⚠️ Для переказу потрібно мінімум 2 рахунки.</b>\n\nСпочатку додайте ще один рахунок.",
            conn=conn,
        )
        return
    context.user_data["transfer_flow"] = {"step": "choose_source_account"}
    prompt = "<b>🔄 Переказ між рахунками</b>\nКрок <b>1 з 4</b>. Оберіть рахунок, з якого списати кошти."
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(
        prompt,
        reply_markup=kb_transfer_pick_from([_transfer_account_option(row) for row in accounts]),
    )


async def _show_transfer_target_step(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    source_account_id: int,
    *,
    conn: asyncpg.Connection | None = None,
    notice: str | None = None,
) -> None:
    if conn is None:
        async with _pool(context).acquire() as own_conn:
            await _show_transfer_target_step(
                message,
                context,
                tg_user_id,
                source_account_id,
                conn=own_conn,
                notice=notice,
            )
            return
    await _recalculate_account_balances(conn, tg_user_id)
    accounts = await _get_accounts_full(conn, tg_user_id)
    by_id = {int(row["id"]): row for row in accounts}
    source = by_id.get(source_account_id)
    if source is None:
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice="<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.",
            conn=conn,
        )
        return
    targets = [row for row in accounts if int(row["id"]) != source_account_id]
    if not targets:
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice="<b>⚠️ Для переказу потрібно мінімум 2 рахунки.</b>\n\nСпочатку додайте ще один рахунок.",
            conn=conn,
        )
        return
    context.user_data["transfer_flow"] = {
        "step": "choose_target_account",
        "source_account_id": int(source["id"]),
        "source_label": str(source["label"]),
        "source_currency": normalize_currency(str(source["currency"])),
        "source_balance": Decimal(str(source["balance"] or 0)),
    }
    prompt = (
        "<b>🔄 Переказ між рахунками</b>\n"
        "Крок <b>2 з 4</b>. Оберіть рахунок, на який зарахувати кошти.\n\n"
        f"<b>З рахунку:</b> {escape_html(str(source['label']))}"
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(
        prompt,
        reply_markup=kb_transfer_pick_to([_transfer_account_option(row) for row in targets]),
    )


async def _show_transfer_amount_step(message, context: ContextTypes.DEFAULT_TYPE, flow: dict, notice: str | None = None) -> None:
    label = str(flow.get("source_label") or "—")
    currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    available = _transfer_flow_source_balance(flow)
    prompt = "\n".join(
        [
            "<b>🔄 Переказ між рахунками</b>",
            "Крок <b>3 з 4</b>. Введіть суму переказу.",
            "",
            f"<b>З рахунку:</b> {escape_html(label)}",
            f"<b>Доступно:</b> {format_money(available, currency)}",
        ]
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=kb_inline_cancel("transfer:cancel"))


async def _show_transfer_rate_step(message, flow: dict, notice: str | None = None) -> None:
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    prompt = (
        "<b>🔄 Переказ між рахунками</b>\n"
        "Крок <b>3 з 5</b>. Введіть курс вручну.\n\n"
        f"<b>З рахунку:</b> {escape_html(str(flow.get('source_label') or '—'))} ({escape_html(source_currency)})\n"
        f"<b>На рахунок:</b> {escape_html(str(flow.get('target_label') or '—'))} ({escape_html(target_currency)})\n\n"
        f"{build_transfer_rate_prompt(source_currency, target_currency)}"
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=kb_inline_cancel("transfer:cancel"))


async def _show_transfer_rate_choice_step(message, flow: dict, notice: str | None = None) -> None:
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    prompt = (
        "<b>🔄 Переказ між рахунками</b>\n"
        "Крок <b>3 з 5</b>. Рахунки мають різні валюти.\n\n"
        f"<b>З рахунку:</b> {escape_html(str(flow.get('source_label') or '—'))} ({escape_html(source_currency)})\n"
        f"<b>На рахунок:</b> {escape_html(str(flow.get('target_label') or '—'))} ({escape_html(target_currency)})\n\n"
        "Оберіть, який курс використати."
    )
    if notice:
        prompt = f"{notice}\n\n{prompt}"
    await message.reply_text(prompt, reply_markup=_kb_transfer_rate_choice())


def _prepare_transfer_preview(flow: dict) -> dict:
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    source_amount = flow.get("source_amount")
    if not isinstance(source_amount, Decimal):
        raise ValueError("source_amount is missing")
    fx_rate = flow.get("fx_rate")
    if fx_rate is not None and not isinstance(fx_rate, Decimal):
        raise ValueError("fx_rate is invalid")
    target_balance = flow.get("target_balance")
    if not isinstance(target_balance, Decimal):
        target_balance = Decimal(str(target_balance or 0))
    preview = TransferService.prepare_preview(
        source_currency=source_currency,
        target_currency=target_currency,
        source_amount=source_amount,
        source_balance=_transfer_flow_source_balance(flow),
        target_balance=target_balance,
        fx_rate=fx_rate,
        rate_source=str(flow.get("rate_source") or "") or None,
    )
    flow["target_amount"] = preview.target_amount
    flow["rate_text"] = preview.rate_text
    flow["preview_source_balance"] = preview.preview_source_balance
    flow["preview_target_balance"] = preview.preview_target_balance
    flow["step"] = "confirm_transfer"
    flow["rate_source"] = preview.rate_source
    return flow


async def _complete_transfer_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection,
    credit_limit_override: Decimal | None = None,
) -> None:
    try:
        source_account_id = int(flow["source_account_id"])
        target_account_id = int(flow["target_account_id"])
        source_currency = normalize_currency(str(flow["source_currency"]))
        target_currency = normalize_currency(str(flow["target_currency"]))
        source_amount = flow["source_amount"]
        if not isinstance(source_amount, Decimal):
            raise ValueError("source_amount is missing")
        fx_rate = flow.get("fx_rate")
        if fx_rate is not None and not isinstance(fx_rate, Decimal):
            raise ValueError("fx_rate is invalid")
    except Exception:
        _reset_transfer_flow(context)
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice="<b>❌ Не вдалося завершити переказ</b>\n\nСпробуйте ще раз.",
            conn=conn,
        )
        return

    transfer_result = await TransferService(conn).execute_transfer(
        tg_user_id,
        source_account_id=source_account_id,
        target_account_id=target_account_id,
        source_currency=source_currency,
        target_currency=target_currency,
        source_amount=source_amount,
        fx_rate=fx_rate,
        rate_source=str(flow.get("rate_source") or "") or None,
        credit_limit_override=credit_limit_override,
    )
    if transfer_result.status == "accounts_missing":
        _reset_transfer_flow(context)
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice="<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.",
            conn=conn,
        )
        return
    if transfer_result.status == "same_account":
        await _show_transfer_target_step(
            message,
            context,
            tg_user_id,
            source_account_id,
            conn=conn,
            notice="<b>❌ Неможливо обрати той самий рахунок</b>\n\nОберіть інший рахунок.",
        )
        return
    if transfer_result.status == "credit_limit_required":
        flow["source_balance"] = transfer_result.source_previous_balance or transfer_result.source_balance or _transfer_flow_source_balance(flow)
        flow["projected_balance"] = transfer_result.source_new_balance
        flow["step"] = "await_credit_limit"
        context.user_data["transfer_flow"] = flow
        account_label = str(
            (transfer_result.source_account or {}).get("label")
            or flow.get("source_label")
            or "—"
        )
        await message.reply_text(
            _credit_limit_required_text(
                account_label,
                transfer_result.source_new_balance or Decimal("0"),
                source_currency,
            ),
            reply_markup=kb_inline_cancel("transfer:cancel"),
        )
        return
    if transfer_result.status == "credit_limit_exceeded":
        flow["source_balance"] = transfer_result.source_previous_balance or transfer_result.source_balance or _transfer_flow_source_balance(flow)
        account_label = str(
            (transfer_result.source_account or {}).get("label")
            or flow.get("source_label")
            or "—"
        )
        notice = _credit_limit_exceeded_text(
            account_label,
            transfer_result.source_new_balance or Decimal("0"),
            transfer_result.source_credit_limit,
            source_currency,
        )
        if credit_limit_override is not None or str(flow.get("step") or "") == "await_credit_limit":
            flow["step"] = "await_credit_limit"
            context.user_data["transfer_flow"] = flow
            await message.reply_text(
                f"{notice}\n\nВведіть більший кредитний ліміт або скасуйте операцію.",
                reply_markup=kb_inline_cancel("transfer:cancel"),
            )
            return
        flow["step"] = "confirm_transfer"
        context.user_data["transfer_flow"] = flow
        allow_rate_edit = source_currency != target_currency
        await message.reply_text(
            notice,
            reply_markup=_kb_transfer_confirm(allow_rate_edit=allow_rate_edit, flow=flow),
        )
        return
    if not _is_transfer_commit_success(transfer_result.status):
        _reset_transfer_flow(context)
        await _show_accounts_settings(
            message,
            context,
            tg_user_id,
            notice="<b>❌ Не вдалося записати переказ</b>\n\nСпробуйте ще раз.",
            conn=conn,
        )
        return

    source_account = transfer_result.source_account
    target_account = transfer_result.target_account
    target_amount = transfer_result.target_amount
    await log_bot_event(
        conn,
        tg_user_id,
        "transaction_created",
        source="transfer",
        parsed_result={
            "type": "transfer",
            "amount": float(source_amount),
            "currency": source_currency,
            "to_amount": float(target_amount),
            "to_currency": target_currency,
            "rate_source": transfer_result.rate_source,
        },
    )

    success_text = (
        "<b>✅ Переказ виконано</b>\n\n"
        f"<b>З рахунку:</b> {escape_html(str(source_account['label']))}\n"
        f"<b>На рахунок:</b> {escape_html(str(target_account['label']))}\n\n"
        f"<b>Списано:</b> {format_money(source_amount, source_currency)}\n"
        f"<b>Зараховано:</b> {format_money(target_amount, target_currency)}"
    )
    success_text += _transfer_transition_notice(transfer_result)

    _reset_transfer_flow(context)
    await message.reply_text(success_text, reply_markup=kb_home())


async def _start_transfer_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    conn: asyncpg.Connection | None = None,
) -> None:
    _reset_transfer_flow(context)
    await _show_transfer_source_step(message, context, tg_user_id, conn=conn)


async def transfer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        if not await _user_ready(conn, user.id):
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Перекази доступні лише з повним доступом.</b>",
            ):
                return
        if q.data not in {"transfer:cancel", "transfer:back"}:
            if await _billing_write_blocked(q.message, conn, user.id):
                return

        flow = context.user_data.get("transfer_flow") or {}
        step = str(flow.get("step") or "")

        if q.data == "transfer:cancel":
            _reset_transfer_flow(context)
            await _show_accounts_settings(q.message, context, user.id, notice="Переказ скасовано.", conn=conn)
            return

        if q.data == "transfer:back":
            if step == "choose_target_account":
                await _show_transfer_source_step(q.message, context, user.id, conn=conn)
                return
            if step == "enter_source_amount":
                source_account_id = int(flow.get("source_account_id") or 0)
                await _show_transfer_target_step(q.message, context, user.id, source_account_id, conn=conn)
                return
            if step == "enter_exchange_rate":
                source_account_id = int(flow.get("source_account_id") or 0)
                await _show_transfer_target_step(q.message, context, user.id, source_account_id, conn=conn)
                return
            if step == "await_credit_limit":
                flow["step"] = "confirm_transfer"
                context.user_data["transfer_flow"] = flow
                allow_rate_edit = normalize_currency(str(flow.get("source_currency") or "UAH")) != normalize_currency(
                    str(flow.get("target_currency") or "UAH")
                )
                await q.message.reply_text(
                    _transfer_confirm_text(flow),
                    reply_markup=_kb_transfer_confirm(allow_rate_edit=allow_rate_edit, flow=flow),
                )
                return
            _reset_transfer_flow(context)
            await _show_accounts_settings(q.message, context, user.id, conn=conn)
            return

        if q.data.startswith("transfer:source:"):
            try:
                source_account_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            await _show_transfer_target_step(q.message, context, user.id, source_account_id, conn=conn)
            return

        if q.data.startswith("transfer:target:"):
            try:
                target_account_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            source_account_id = int(flow.get("source_account_id") or 0)
            if not source_account_id:
                await _show_transfer_source_step(q.message, context, user.id, conn=conn)
                return
            if source_account_id == target_account_id:
                await _show_transfer_target_step(
                    q.message,
                    context,
                    user.id,
                    source_account_id,
                    conn=conn,
                    notice="<b>❌ Неможливо обрати той самий рахунок</b>\n\nОберіть інший рахунок.",
                )
                return
            source_account = await _get_active_account_by_id(conn, user.id, source_account_id)
            target_account = await _get_active_account_by_id(conn, user.id, target_account_id)
            if source_account is None or target_account is None:
                await _show_accounts_settings(
                    q.message,
                    context,
                    user.id,
                    notice="<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.",
                    conn=conn,
                )
                return
            flow = {
                "step": "enter_source_amount",
                "source_account_id": int(source_account["id"]),
                "source_label": str(source_account["label"]),
                "source_currency": normalize_currency(str(source_account["currency"])),
                "source_balance": Decimal(str(source_account["balance"] or 0)),
                "target_account_id": int(target_account["id"]),
                "target_label": str(target_account["label"]),
                "target_currency": normalize_currency(str(target_account["currency"])),
                "target_balance": Decimal(str(target_account["balance"] or 0)),
            }
            context.user_data["transfer_flow"] = flow
            if flow["source_currency"] == flow["target_currency"]:
                await _show_transfer_amount_step(q.message, context, flow)
            else:
                flow["step"] = "enter_exchange_rate"
                context.user_data["transfer_flow"] = flow
                await _show_transfer_rate_step(q.message, flow)
            return

        if q.data == "transfer:rate:auto":
            if not flow:
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            # Legacy keyboards must obey the current manual-rate contract too.
            flow.pop("fx_rate", None)
            flow.pop("rate_source", None)
            flow["step"] = "enter_exchange_rate"
            context.user_data["transfer_flow"] = flow
            await _show_transfer_rate_step(q.message, flow)
            return

        if q.data == "transfer:rate:manual":
            if not flow:
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            flow["step"] = "enter_exchange_rate"
            context.user_data["transfer_flow"] = flow
            await _show_transfer_rate_step(q.message, flow)
            return

        if q.data == "transfer:edit:amount":
            if not flow:
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            flow["step"] = "enter_source_amount"
            context.user_data["transfer_flow"] = flow
            await _show_transfer_amount_step(q.message, context, flow)
            return

        if q.data == "transfer:edit:rate":
            if not flow:
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            flow["step"] = "enter_exchange_rate"
            context.user_data["transfer_flow"] = flow
            await _show_transfer_rate_step(q.message, flow)
            return

        if q.data == "transfer:edit:accounts":
            source_account_id = int(flow.get("source_account_id") or 0)
            if source_account_id:
                await _show_transfer_target_step(q.message, context, user.id, source_account_id, conn=conn)
            else:
                await _show_transfer_source_step(q.message, context, user.id, conn=conn)
            return

        if q.data == "transfer:confirm" or q.data.startswith("transfer:confirm:pv:"):
            if not flow:
                await _show_accounts_settings(q.message, context, user.id, conn=conn)
                return
            if not consume_financial_confirmation(flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            await _complete_transfer_flow(q.message, context, user.id, flow, conn=conn)
            return
            try:
                source_account_id = int(flow["source_account_id"])
                target_account_id = int(flow["target_account_id"])
                source_currency = normalize_currency(str(flow["source_currency"]))
                target_currency = normalize_currency(str(flow["target_currency"]))
                source_amount = flow["source_amount"]
                if not isinstance(source_amount, Decimal):
                    raise ValueError("source_amount is missing")
                fx_rate = flow.get("fx_rate")
                if fx_rate is not None and not isinstance(fx_rate, Decimal):
                    raise ValueError("fx_rate is invalid")
            except Exception:
                _reset_transfer_flow(context)
                await _show_accounts_settings(
                    q.message,
                    context,
                    user.id,
                    notice="<b>❌ Не вдалося завершити переказ</b>\n\nСпробуйте ще раз.",
                    conn=conn,
                )
                return

            transfer_result = await TransferService(conn).execute_transfer(
                user.id,
                source_account_id=source_account_id,
                target_account_id=target_account_id,
                source_currency=source_currency,
                target_currency=target_currency,
                source_amount=source_amount,
                fx_rate=fx_rate,
                rate_source=str(flow.get("rate_source") or "") or None,
            )
            if transfer_result.status == "accounts_missing":
                _reset_transfer_flow(context)
                await _show_accounts_settings(
                    q.message,
                    context,
                    user.id,
                    notice="<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.",
                    conn=conn,
                )
                return
            if transfer_result.status == "same_account":
                await _show_transfer_target_step(
                    q.message,
                    context,
                    user.id,
                    source_account_id,
                    conn=conn,
                    notice="<b>❌ Неможливо обрати той самий рахунок</b>\n\nОберіть інший рахунок.",
                )
                return
            if transfer_result.status == "insufficient_funds":
                flow["source_balance"] = transfer_result.source_balance
                flow["step"] = "enter_source_amount"
                context.user_data["transfer_flow"] = flow
                await _show_transfer_amount_step(
                    q.message,
                    context,
                    flow,
                    notice=_insufficient_funds_text(
                        str(transfer_result.source_account["label"]),
                        transfer_result.source_balance,
                        source_amount,
                        source_currency,
                    ),
                )
                return
            source_account = transfer_result.source_account
            target_account = transfer_result.target_account
            target_amount = transfer_result.target_amount
            await log_bot_event(
                conn,
                user.id,
                "transaction_created",
                source="transfer",
                parsed_result={
                    "type": "transfer",
                    "amount": float(source_amount),
                    "currency": source_currency,
                    "to_amount": float(target_amount),
                    "to_currency": target_currency,
                    "rate_source": transfer_result.rate_source,
                },
            )

            _reset_transfer_flow(context)
            await q.message.reply_text(
                "<b>✅ Переказ виконано</b>\n\n"
                f"<b>З рахунку:</b> {escape_html(str(source_account['label']))}\n"
                f"<b>На рахунок:</b> {escape_html(str(target_account['label']))}\n\n"
                f"<b>Списано:</b> {format_money(source_amount, source_currency)}\n"
                f"<b>Зараховано:</b> {format_money(target_amount, target_currency)}",
                reply_markup=kb_home(),
            )
            return


async def saving_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = validated_confirmation_action(context.user_data.get("saving_flow") or {}, q.data)
    if data is None:
        await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
        return

    user = update.effective_user
    if not user:
        return

    parts = data.split(":")
    async with _pool(context).acquire() as conn:
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await q.message.reply_text(maintenance_message)
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        if not await _user_ready(conn, user.id):
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Заощадження та інвестиції доступні лише з повним доступом.</b>",
            ):
                return

        saving_write_prefixes = (
            "saving:create_account",
            "saving:add_account",
            "saving:topup",
            "saving:withdraw",
            "saving:edit:",
            "saving:plan:",
            "saving:confirm",
            "saving:task:confirm",
            "saving:settings:toggle_prompt",
            "saving:settings:percent",
            "saving:settings:target",
            "saving:settings:toggle_salary_only",
            "saving:settings:min_income",
            "saving:existing:confirm",
        )
        if any((q.data or "").startswith(prefix) for prefix in saving_write_prefixes):
            if await _billing_write_blocked(q.message, conn, user.id):
                return

        saving_service = SavingsService(conn)

        if q.data == "saving:overview":
            _reset_saving_flow(context)
            _reset_saving_settings_flow(context)
            await _show_savings_overview(q.message, context, user.id, conn=conn)
            return

        if q.data == "saving:settings:open":
            await _show_saving_settings(q.message, context, user.id, conn=conn)
            return

        if q.data == "saving:cancel":
            _reset_saving_flow(context)
            _reset_saving_settings_flow(context)
            await _reply_home(q.message)
            return

        if q.data == "saving:create_account":
            await _start_savings_account_create_flow(q.message, context)
            return

        if q.data == "saving:add_account":
            _reset_saving_flow(context)
            context.user_data["saving_flow"] = {"mode": "add_account"}
            await q.message.reply_text(
                "<b>➕ Додати рахунок накопичень</b>\n\nОберіть тип рахунку:",
                reply_markup=kb_savings_add_menu(),
            )
            return

        if q.data == "saving:list":
            await _show_savings_list(q.message, context, user.id, conn=conn)
            return

        if parts[:2] == ["saving", "detail"]:
            try:
                account_id = int(parts[2])
            except (IndexError, ValueError):
                return
            await _show_savings_detail(q.message, context, user.id, account_id, conn=conn)
            return

        if parts[:2] == ["saving", "history"]:
            try:
                account_id = int(parts[2])
            except (IndexError, ValueError):
                return
            await _show_savings_history(q.message, context, user.id, account_id, conn=conn)
            return

        if q.data == "saving:topup":
            await _show_manual_savings_target_picker(q.message, context, user.id, {"mode": "manual_topup"}, conn=conn)
            return

        if parts[:3] == ["saving", "topup", "target"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None or normalize_account_type(str(account["account_type"] or "other")) != "savings":
                await q.message.reply_text("Оберіть накопичення зі списку.")
                return
            flow = context.user_data.get("saving_flow") or {"mode": "manual_topup"}
            flow.update(_saving_target_payload(account))
            if flow.get("mode") == "post_income_prompt" and flow.get("source_account_id"):
                if flow["target_currency"] != normalize_currency(str(flow.get("source_currency") or "UAH")):
                    flow["step"] = "await_fx_rate"
                    context.user_data["saving_flow"] = flow
                    await _show_saving_rate_step(q.message, flow)
                    return
                await _show_saving_transfer_confirmation(q.message, context, flow, conn=conn)
                return
            await _show_topup_source_picker(q.message, context, user.id, flow, conn=conn)
            return

        if parts[:3] == ["saving", "topup", "source"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            flow = context.user_data.get("saving_flow") or {}
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(ACCOUNT_NOT_FOUND_REFRESH_TEXT)
                return
            if int(account["id"]) == int(flow.get("target_account_id") or 0):
                await q.message.reply_text("Рахунок списання має відрізнятися від накопичення.")
                return
            flow.update(_saving_source_payload(account))
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Яку суму поповнення записати?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if q.data == "saving:topup:confirm:amount":
            flow = context.user_data.get("saving_flow") or {}
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Яку суму поповнення записати?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if q.data == "saving:topup:confirm:target":
            flow = context.user_data.get("saving_flow") or {}
            await _show_manual_savings_target_picker(q.message, context, user.id, flow, conn=conn)
            return

        if q.data == "saving:withdraw":
            await _show_withdraw_source_picker(q.message, context, user.id, conn=conn)
            return

        if parts[:3] == ["saving", "withdraw", "source"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None or normalize_account_type(str(account["account_type"] or "other")) != "savings":
                await q.message.reply_text("Оберіть накопичення зі списку.")
                return
            flow = context.user_data.get("saving_flow") or {"mode": "manual_withdraw"}
            flow.update(_saving_source_payload(account))
            await _show_withdraw_target_picker(q.message, context, user.id, flow, conn=conn)
            return

        if parts[:3] == ["saving", "withdraw", "target"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            flow = context.user_data.get("saving_flow") or {}
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(ACCOUNT_NOT_FOUND_REFRESH_TEXT)
                return
            if int(account["id"]) == int(flow.get("source_account_id") or 0):
                await q.message.reply_text("Рахунок зарахування має відрізнятися від накопичення.")
                return
            flow.update(_saving_target_payload(account))
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Яку суму виведення записати?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if q.data == "saving:withdraw:confirm:amount":
            flow = context.user_data.get("saving_flow") or {}
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Яку суму виведення записати?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if parts[:3] == ["saving", "edit", "name"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            context.user_data["saving_flow"] = {"mode": "edit_name", "step": "await_name", "account_id": account_id}
            await q.message.reply_text("Введіть нову назву накопичення.", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if parts[:3] == ["saving", "edit", "goal"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            context.user_data["saving_flow"] = {"mode": "edit_goal", "step": "await_goal_amount", "account_id": account_id}
            await q.message.reply_text(
                "Введіть цільову суму числом або 0, якщо хочете прибрати ціль.",
                reply_markup=kb_inline_cancel("saving:cancel"),
            )
            return

        if q.data == "saving:not_now":
            _reset_saving_flow(context)
            await _reply_home(q.message, "Добре, поки без відкладення.")
            return

        if q.data == "saving:disable_prompt":
            await saving_service.update_settings(user.id, ask_after_income=False)
            _reset_saving_flow(context)
            await q.message.reply_text(
                "Готово. Після доходів більше не пропонуватиму відкладення. Це можна змінити в налаштуваннях.",
                reply_markup=kb_home(),
            )
            return

        if q.data == "saving:choose_existing":
            await _show_existing_account_picker_for_saving(q.message, context, user.id, conn=conn)
            return

        if parts[:2] == ["saving", "existing"] and len(parts) == 3:
            try:
                account_id = int(parts[2])
            except ValueError:
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(ACCOUNT_NOT_FOUND_REFRESH_TEXT, reply_markup=kb_saving_setup_prompt())
                return
            context.user_data["saving_flow"] = {
                **(context.user_data.get("saving_flow") or {}),
                "mode": "confirm_existing_account_as_savings",
                "existing_account_id": account_id,
            }
            await q.message.reply_text(
                "Зробити цей рахунок рахунком для накопичень?\n\n"
                f"<b>{escape_html(str(account['label']))}</b>\n"
                f"Поточний тип: <b>{escape_html(account_type_label(str(account['account_type'] or 'other')))}</b>",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [InlineKeyboardButton("Так, зробити рахунком для накопичень", callback_data=f"saving:existing:confirm:{account_id}")],
                        [_cancel_btn("saving:cancel")],
                    ]
                ),
            )
            return

        if parts[:3] == ["saving", "existing", "confirm"]:
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(ACCOUNT_NOT_FOUND_REFRESH_TEXT, reply_markup=kb_saving_setup_prompt())
                return
            await AccountService(conn).update_account_type(user.id, account_id, "savings")
            flow = context.user_data.get("saving_flow") or {}
            flow.pop("mode", None)
            flow.pop("existing_account_id", None)
            context.user_data["saving_flow"] = flow
            await _show_saving_income_prompt(q.message, context, flow, setup_mode=False)
            return

        if q.data == "saving:settings:toggle_prompt":
            settings = await saving_service.get_or_create_settings(user.id)
            await saving_service.update_settings(
                user.id,
                ask_after_income=not bool(settings["ask_after_income"]),
                enabled=True,
            )
            await _show_saving_settings(q.message, context, user.id, conn=conn, notice="Налаштування оновлено.")
            return

        if q.data == "saving:settings:percent":
            context.user_data["saving_settings_flow"] = {"step": "await_default_percent"}
            await q.message.reply_text(
                "Який відсоток доходу пропонувати відкладати?\n\nНаприклад:\n10\n15\n20",
                reply_markup=kb_inline_cancel("saving:settings:open"),
            )
            return

        if q.data == "saving:settings:target":
            targets = _saving_target_options(await saving_service.get_active_target_accounts(user.id))
            if not targets:
                await q.message.reply_text(
                    "Немає жодного рахунку накопичень.",
                    reply_markup=kb_saving_settings_target_accounts([]),
                )
                return
            await q.message.reply_text("Оберіть рахунок за замовчуванням:", reply_markup=kb_saving_settings_target_accounts(targets))
            return

        if q.data == "saving:settings:toggle_salary_only":
            settings = await saving_service.get_or_create_settings(user.id)
            await saving_service.update_settings(user.id, ask_only_for_salary=not bool(settings["ask_only_for_salary"]))
            await _show_saving_settings(q.message, context, user.id, conn=conn, notice="Налаштування оновлено.")
            return

        if q.data == "saving:settings:min_income":
            context.user_data["saving_settings_flow"] = {"step": "await_min_income_amount"}
            await q.message.reply_text(
                "Введіть мінімальну суму доходу або 0, щоб прибрати обмеження.",
                reply_markup=kb_inline_cancel("saving:settings:open"),
            )
            return

        if parts[:3] == ["saving", "settings", "target"]:
            if parts[3] == "none":
                await saving_service.update_settings(user.id, default_target_account_id=None)
                await _show_saving_settings(q.message, context, user.id, conn=conn, notice="Рахунок за замовчуванням очищено.")
                return
            try:
                account_id = int(parts[3])
            except (IndexError, ValueError):
                return
            target = await _get_active_account_by_id(conn, user.id, account_id)
            if target is None or not is_savings_account_type(str(target["account_type"] or "other")):
                await q.message.reply_text("Оберіть рахунок накопичень.")
                return
            await saving_service.update_settings(user.id, default_target_account_id=account_id)
            await _show_saving_settings(q.message, context, user.id, conn=conn, notice=f"Рахунок за замовчуванням: {escape_html(str(target['label']))}.")
            return

        flow = context.user_data.get("saving_flow") or {}

        if q.data == "saving:plan:primary":
            amount = flow.get("primary_amount")
            if not isinstance(amount, Decimal):
                await _reply_home(q.message)
                return
            flow.update(
                {
                    "mode": "post_income_prompt",
                    "amount": amount,
                }
            )
            targets = _saving_target_options(await saving_service.get_active_target_accounts(user.id))
            default_target_id = _saving_default_target_id(flow, targets)
            if default_target_id is not None:
                target_account = await _get_active_account_by_id(conn, user.id, default_target_id)
                if target_account is not None:
                    flow.update(_saving_target_payload(target_account))
                    if flow["target_currency"] != normalize_currency(str(flow.get("source_currency") or "UAH")):
                        flow["step"] = "await_fx_rate"
                        context.user_data["saving_flow"] = flow
                        await _show_saving_rate_step(q.message, flow)
                        return
                    await _show_saving_transfer_confirmation(q.message, context, flow, conn=conn)
                    return
            await _show_manual_savings_target_picker(q.message, context, user.id, flow, conn=conn)
            return

        if q.data == "saving:plan:secondary":
            amount = flow.get("secondary_amount")
            percent = flow.get("secondary_percent")
            if not isinstance(amount, Decimal) or not isinstance(percent, Decimal):
                await _reply_home(q.message)
                return
            flow.update(
                {
                    "mode": "create_plan",
                    "amount": amount,
                    "currency": normalize_currency(str(flow.get("source_currency") or "UAH")),
                    "percent_from_income": percent,
                }
            )
            targets = _saving_target_options(await saving_service.get_active_target_accounts(user.id))
            default_target_id = _saving_default_target_id(flow, targets)
            if default_target_id is not None:
                target_account = await _get_active_account_by_id(conn, user.id, default_target_id)
                if target_account is not None:
                    await _create_pending_saving_plan(q.message, context, user.id, flow, target_account, conn=conn)
                    return
            await _show_saving_target_picker(q.message, context, user.id, flow, conn=conn)
            return

        if q.data == "saving:plan:other":
            flow["mode"] = "post_income_prompt"
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Яку суму відкласти?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if q.data == "saving:already":
            flow["mode"] = "already_transferred"
            flow["step"] = "await_amount"
            context.user_data["saving_flow"] = flow
            await q.message.reply_text("Скільки вже переказав(ла)?", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if parts[:2] == ["saving", "target"]:
            try:
                target_account_id = int(parts[2])
            except (IndexError, ValueError):
                return
            if not flow:
                await _reply_home(q.message)
                return
            target_account = await _get_active_account_by_id(conn, user.id, target_account_id)
            if target_account is None or not is_savings_account_type(str(target_account["account_type"] or "other")):
                await q.message.reply_text("Оберіть рахунок для накопичень, депозит або інвестиційний рахунок.")
                return
            flow["target_account_id"] = int(target_account["id"])
            flow["target_label"] = str(target_account["label"])
            flow["target_currency"] = normalize_currency(str(target_account["currency"] or flow.get("source_currency") or "UAH"))
            flow["target_balance"] = Decimal(str(target_account["balance"] or 0))
            if flow.get("mode") == "create_plan":
                await _create_pending_saving_plan(q.message, context, user.id, flow, target_account, conn=conn)
                return
            if flow.get("mode") == "change_task_target":
                updated_task = await saving_service.update_task_target(
                    user.id,
                    int(flow.get("pending_task_id") or 0),
                    int(target_account["id"]),
                )
                _reset_saving_flow(context)
                if updated_task is None:
                    await q.message.reply_text("План уже неактивний.", reply_markup=kb_home())
                    return
                await q.message.reply_text(
                    await _saving_plan_text(
                        conn,
                        amount=Decimal(str(updated_task["amount"] or 0)),
                        currency=normalize_currency(str(updated_task["currency"] or "UAH")),
                        source_label=str(updated_task["source_label"]),
                        target_label=str(updated_task["target_label"]),
                    ),
                    reply_markup=kb_saving_plan_actions(int(updated_task["id"])),
                )
                return
            if flow.get("mode") in {"already_transferred", "task_confirm"}:
                if flow["target_currency"] != normalize_currency(str(flow.get("source_currency") or "UAH")):
                    flow["step"] = "await_fx_rate"
                    context.user_data["saving_flow"] = flow
                    await _show_saving_rate_step(q.message, flow)
                    return
                flow["step"] = "confirm"
                context.user_data["saving_flow"] = flow
                await q.message.reply_text(
                    _saving_confirm_text(
                        amount=Decimal(str(flow.get("amount") or 0)),
                        source_label=str(flow.get("source_label") or "—"),
                        target_label=str(flow.get("target_label") or "—"),
                        source_currency=normalize_currency(str(flow.get("source_currency") or "UAH")),
                        target_currency=normalize_currency(str(flow.get("target_currency") or "UAH")),
                    ),
                    reply_markup=bind_financial_preview(flow, kb_saving_confirm()),
                )
                return

        if parts[:3] == ["saving", "task", "confirm"] and len(parts) == 4:
            try:
                task_id = int(parts[3])
            except ValueError:
                return
            task = await saving_service.get_task(user.id, task_id)
            if task is None:
                await q.message.reply_text("План не знайдено або вже завершено.", reply_markup=kb_home())
                return
            if normalize_currency(str(task["source_currency"] or "UAH")) != normalize_currency(str(task["target_currency"] or "UAH")):
                context.user_data["saving_flow"] = {
                    "mode": "task_confirm",
                    "step": "await_fx_rate",
                    "pending_task_id": task_id,
                    "source_account_id": int(task["source_account_id"]),
                    "source_label": str(task["source_label"]),
                    "source_currency": normalize_currency(str(task["source_currency"] or "UAH")),
                    "target_account_id": int(task["target_account_id"]),
                    "target_label": str(task["target_label"]),
                    "target_currency": normalize_currency(str(task["target_currency"] or "UAH")),
                    "target_balance": Decimal(str(task["target_balance"] or 0)),
                    "amount": Decimal(str(task["amount"] or 0)),
                }
                await _show_saving_rate_step(q.message, context.user_data["saving_flow"])
                return
            flow = {
                "mode": "task_confirm", "step": "confirm", "pending_task_id": task_id,
                "source_account_id": int(task["source_account_id"]),
                "target_account_id": int(task["target_account_id"]),
                "source_currency": normalize_currency(str(task["source_currency"] or "UAH")),
                "target_currency": normalize_currency(str(task["target_currency"] or "UAH")),
                "amount": Decimal(str(task["amount"] or 0)),
            }
            context.user_data["saving_flow"] = flow
            await q.message.reply_text(
                _saving_confirm_text(
                    amount=Decimal(str(task["amount"] or 0)),
                    source_label=str(task["source_label"]),
                    target_label=str(task["target_label"]),
                    source_currency=normalize_currency(str(task["source_currency"] or "UAH")),
                    target_currency=normalize_currency(str(task["target_currency"] or "UAH")),
                ),
                reply_markup=bind_financial_preview(flow, kb_saving_task_confirm(task_id)),
            )
            return

        if parts[:4] == ["saving", "task", "confirm", "ok"]:
            try:
                task_id = int(parts[4])
            except (IndexError, ValueError):
                return
            flow = context.user_data.get("saving_flow") or {}
            if not consume_financial_confirmation(flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            result = await saving_service.confirm_pending_task(
                user.id, task_id, fx_rate=None, rate_source=None,
                expected_task=flow,
            )
            _reset_saving_flow(context)
            if result.status == "stale_preview":
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT, reply_markup=kb_saving_plan_actions(task_id))
                return
            if result.status == "task_missing":
                await q.message.reply_text("План не знайдено.", reply_markup=kb_home())
                return
            if result.status == "task_not_pending":
                await q.message.reply_text("Цей план уже неактивний.", reply_markup=kb_home())
                return
            if result.status == "insufficient_funds":
                task = result.task
                await q.message.reply_text(
                    _insufficient_funds_text(
                        str(task["source_label"] if task is not None else "рахунок"),
                        result.transfer_result.source_balance if result.transfer_result else Decimal("0"),
                        Decimal(str(task["amount"] if task is not None else 0)),
                        normalize_currency(str(task["source_currency"] if task is not None else "UAH")),
                    ),
                    reply_markup=kb_saving_plan_actions(task_id),
                )
                return
            if result.status != "completed" or result.transfer_result is None or result.task is None:
                await q.message.reply_text("Не вдалося записати переказ. Спробуйте ще раз.", reply_markup=kb_home())
                return
            source_balance = quantize_money(result.transfer_result.source_balance - Decimal(str(result.task["amount"] or 0)))
            target_balance = quantize_money(Decimal(str(result.task["target_balance"] or 0)) + Decimal(str(result.transfer_result.target_amount or 0)))
            await q.message.reply_text(
                _saving_success_text(
                    source_label=str(result.task["source_label"]),
                    source_balance=source_balance,
                    source_currency=normalize_currency(str(result.task["source_currency"] or "UAH")),
                    target_label=str(result.task["target_label"]),
                    target_balance=target_balance,
                    target_currency=normalize_currency(str(result.task["target_currency"] or "UAH")),
                ),
                reply_markup=kb_home(),
            )
            return

        if parts[:3] == ["saving", "task", "change"]:
            try:
                task_id = int(parts[3])
            except (IndexError, ValueError):
                return
            task = await saving_service.get_task(user.id, task_id)
            if task is None or str(task["status"] or "") != "pending":
                await q.message.reply_text("План уже неактивний.", reply_markup=kb_home())
                return
            context.user_data["saving_flow"] = {
                "mode": "change_task_amount",
                "step": "await_amount",
                "pending_task_id": task_id,
                "source_label": str(task["source_label"]),
                "source_currency": normalize_currency(str(task["source_currency"] or "UAH")),
                "target_label": str(task["target_label"]),
                "target_currency": normalize_currency(str(task["target_currency"] or "UAH")),
            }
            await q.message.reply_text("Введіть нову суму для плану:", reply_markup=kb_inline_cancel("saving:cancel"))
            return

        if parts[:3] == ["saving", "task", "target"]:
            try:
                task_id = int(parts[3])
            except (IndexError, ValueError):
                return
            task = await saving_service.get_task(user.id, task_id)
            if task is None or str(task["status"] or "") != "pending":
                await q.message.reply_text("План уже неактивний.", reply_markup=kb_home())
                return
            context.user_data["saving_flow"] = {
                "mode": "change_task_target",
                "pending_task_id": task_id,
                "source_account_id": int(task["source_account_id"]),
                "source_label": str(task["source_label"]),
                "source_currency": normalize_currency(str(task["source_currency"] or "UAH")),
                "amount": Decimal(str(task["amount"] or 0)),
                "currency": normalize_currency(str(task["currency"] or "UAH")),
            }
            await _show_saving_target_picker(q.message, context, user.id, context.user_data["saving_flow"], conn=conn, notice="Оберіть новий рахунок для плану.")
            return

        if parts[:3] == ["saving", "task", "cancel"]:
            try:
                task_id = int(parts[3])
            except (IndexError, ValueError):
                return
            await saving_service.cancel_task(user.id, task_id)
            _reset_saving_flow(context)
            await q.message.reply_text("План скасовано.", reply_markup=kb_home())
            return

        if parts[:3] == ["saving", "task", "remind"] and len(parts) == 4:
            try:
                task_id = int(parts[3])
            except ValueError:
                return
            await q.message.reply_text("Коли нагадати?", reply_markup=kb_saving_reminder_choice(task_id))
            return

        if parts[:3] == ["saving", "task", "remind"] and len(parts) == 5:
            choice = parts[3]
            try:
                task_id = int(parts[4])
            except ValueError:
                return
            if choice == "custom":
                context.user_data["saving_flow"] = {"mode": "custom_reminder", "step": "await_custom_reminder", "pending_task_id": task_id}
                await q.message.reply_text(
                    "Введіть час нагадування у форматі `YYYY-MM-DD HH:MM`, `DD.MM HH:MM` або просто `HH:MM`.",
                    reply_markup=kb_inline_cancel("saving:cancel"),
                )
                return
            remind_at = _saving_quick_remind_at(choice, datetime.now())
            await saving_service.set_task_reminder(user.id, task_id, remind_at)
            notice = "Нагадування вимкнено." if remind_at is None else f"Нагадаю {remind_at.strftime('%d.%m %H:%M')}."
            await q.message.reply_text(notice, reply_markup=kb_saving_plan_actions(task_id))
            return

        if data in {"saving:topup:confirm:ok", "saving:withdraw:confirm:ok"}:
            flow = context.user_data.get("saving_flow") or {}
            if not flow:
                await _reply_home(q.message)
                return
            if not consume_financial_confirmation(flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            result, source_balance, target_balance = await _execute_saving_flow_transfer(conn, user.id, flow)
            if getattr(result, "status", None) == "insufficient_funds":
                await q.message.reply_text(
                    _insufficient_funds_text(
                        str(flow.get("source_label") or "рахунок"),
                        getattr(result, "source_balance", Decimal("0")),
                        Decimal(str(flow.get("amount") or 0)),
                        normalize_currency(str(flow.get("source_currency") or "UAH")),
                    ),
                    reply_markup=kb_home(),
                )
                _reset_saving_flow(context)
                return
            if not _is_transfer_commit_success(getattr(result, "status", None)):
                await q.message.reply_text("Не вдалося записати переказ.", reply_markup=kb_home())
                _reset_saving_flow(context)
                return
            _reset_saving_flow(context)
            await q.message.reply_text(
                _saving_success_text(
                    source_label=str(flow["source_label"]),
                    source_balance=source_balance,
                    source_currency=normalize_currency(str(flow["source_currency"] or "UAH")),
                    target_label=str(flow["target_label"]),
                    target_balance=target_balance,
                    target_currency=normalize_currency(str(flow["target_currency"] or "UAH")),
                    target_goal_amount=flow.get("target_goal_amount"),
                ),
                reply_markup=kb_home(),
            )
            return

        if data == "saving:confirm:ok":
            flow = context.user_data.get("saving_flow") or {}
            if not consume_financial_confirmation(flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            if flow.get("mode") == "already_transferred":
                result = await saving_service.record_completed_transfer(
                    user.id,
                    source_account_id=int(flow["source_account_id"]),
                    target_account_id=int(flow["target_account_id"]),
                    source_currency=normalize_currency(str(flow["source_currency"] or "UAH")),
                    target_currency=normalize_currency(str(flow["target_currency"] or "UAH")),
                    amount=Decimal(str(flow["amount"] or 0)),
                    fx_rate=flow.get("fx_rate"),
                    rate_source=str(flow.get("rate_source") or "") or None,
                )
                if result.status == "insufficient_funds":
                    await q.message.reply_text(
                        _insufficient_funds_text(
                            str(result.source_account["label"]),
                            result.source_balance,
                            Decimal(str(flow["amount"])),
                            normalize_currency(str(flow["source_currency"] or "UAH")),
                        ),
                        reply_markup=kb_home(),
                    )
                    _reset_saving_flow(context)
                    return
                if not _is_transfer_commit_success(result.status) or result.target_account is None:
                    await q.message.reply_text("Не вдалося записати переказ.", reply_markup=kb_home())
                    _reset_saving_flow(context)
                    return
                source_balance = quantize_money(result.source_balance - Decimal(str(flow["amount"] or 0)))
                target_balance = quantize_money(Decimal(str(result.target_account["balance"] or 0)) + Decimal(str(result.target_amount or 0)))
                _reset_saving_flow(context)
                await q.message.reply_text(
                    _saving_success_text(
                        source_label=str(flow["source_label"]),
                        source_balance=source_balance,
                        source_currency=normalize_currency(str(flow["source_currency"] or "UAH")),
                        target_label=str(flow["target_label"]),
                        target_balance=target_balance,
                        target_currency=normalize_currency(str(flow["target_currency"] or "UAH")),
                    ),
                    reply_markup=kb_home(),
                )
                return

            if flow.get("mode") == "task_confirm":
                task_id = int(flow.get("pending_task_id") or 0)
                result = await saving_service.confirm_pending_task(
                    user.id,
                    task_id,
                    fx_rate=flow.get("fx_rate"),
                    rate_source=str(flow.get("rate_source") or "manual") or "manual",
                    expected_task=flow,
                )
                if result.status == "stale_preview":
                    _reset_saving_flow(context)
                    await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT, reply_markup=kb_saving_plan_actions(task_id))
                    return
                if result.status == "insufficient_funds":
                    task = result.task
                    await q.message.reply_text(
                        _insufficient_funds_text(
                            str(task["source_label"] if task is not None else "рахунок"),
                            result.transfer_result.source_balance if result.transfer_result else Decimal("0"),
                            Decimal(str(task["amount"] if task is not None else 0)),
                            normalize_currency(str(task["source_currency"] if task is not None else "UAH")),
                        ),
                        reply_markup=kb_saving_plan_actions(task_id),
                    )
                    _reset_saving_flow(context)
                    return
                if result.status != "completed" or result.transfer_result is None or result.task is None:
                    await q.message.reply_text("Не вдалося записати переказ.", reply_markup=kb_home())
                    _reset_saving_flow(context)
                    return
                source_balance = quantize_money(result.transfer_result.source_balance - Decimal(str(result.task["amount"] or 0)))
                target_balance = quantize_money(Decimal(str(flow.get("target_balance") or 0)) + Decimal(str(result.transfer_result.target_amount or 0)))
                _reset_saving_flow(context)
                await q.message.reply_text(
                    _saving_success_text(
                        source_label=str(result.task["source_label"]),
                        source_balance=source_balance,
                        source_currency=normalize_currency(str(result.task["source_currency"] or "UAH")),
                        target_label=str(result.task["target_label"]),
                        target_balance=target_balance,
                        target_currency=normalize_currency(str(result.task["target_currency"] or "UAH")),
                    ),
                    reply_markup=kb_home(),
                )
                return

        if q.data == "saving:confirm:remind":
            flow = context.user_data.get("saving_flow") or {}
            task_id = int(flow.get("pending_task_id") or 0)
            if task_id > 0:
                await q.message.reply_text("Коли нагадати?", reply_markup=kb_saving_reminder_choice(task_id))
                return
            await q.message.reply_text("Щоб нагадати пізніше, спочатку створіть план відкладення.", reply_markup=kb_home())
            _reset_saving_flow(context)
            return

        if q.data == "saving:confirm:cancel":
            _reset_saving_flow(context)
            await _reply_home(q.message)
            return


def _transaction_void_card(draft: dict[str, Any]) -> str:
    item = draft.get("transaction") if isinstance(draft.get("transaction"), dict) else {}
    account = item.get("account") if isinstance(item.get("account"), dict) else {}
    category = item.get("category") if isinstance(item.get("category"), dict) else {}
    amount = item.get("amount") if isinstance(item.get("amount"), dict) else {}
    current_balance = draft.get("current_balance") if isinstance(draft.get("current_balance"), dict) else {}
    balance_after = draft.get("balance_after") if isinstance(draft.get("balance_after"), dict) else {}
    kind_label = "Дохід" if str(item.get("kind") or "") == "income" else "Витрата"
    comment = str(item.get("comment") or "").strip() or "—"
    lines = [
        "<b>Скасувати транзакцію?</b>",
        "",
        f"Тип: <b>{kind_label}</b>",
        f"Сума: <b>{escape_html(str(amount.get('display') or amount.get('value') or '—'))}</b>",
        f"Рахунок: {escape_html(str(account.get('label') or '—'))}",
        f"Категорія: {escape_html(str(category.get('name') or 'Без категорії'))}",
        f"Дата: {escape_html(str(item.get('date') or '—'))}",
        f"Коментар: {escape_html(comment)}",
        "",
        f"Поточний баланс: <b>{escape_html(str(current_balance.get('display') or current_balance.get('value') or '—'))}</b>",
        f"Баланс після скасування: <b>{escape_html(str(balance_after.get('display') or balance_after.get('value') or '—'))}</b>",
        "",
        "Запис зникне з історії та звітів, а його вплив на баланс буде повністю повернуто.",
    ]
    warning = draft.get("warning") if isinstance(draft.get("warning"), dict) else {}
    if warning.get("message"):
        lines.extend(["", f"⚠️ {escape_html(str(warning['message']))}"])
    return "\n".join(lines)


def _transaction_void_unavailable_text() -> str:
    return (
        "<b>Не вдалося зв’язатися із сервісом транзакцій.</b>\n\n"
        "Транзакція не змінена. Спробуйте ще раз за кілька секунд."
    )


async def pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user or not q.message:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    tx_flow = context.user_data.get("tx_flow") or {}
    parts = q.data.split(":")
    if not tx_flow:
        async with _pool(context).acquire() as conn:
            if q.data in {"expense:cancel", "income:cancel", "pick:acct:back", "pick:cat:back"}:
                await _show_access_surface(q.message, conn, user.id)
            elif q.data in {"expense:confirm", "income:confirm"}:
                await _show_access_surface(q.message, conn, user.id, notice="Цю операцію вже завершено. Створіть нову.")
            else:
                await _show_access_surface(q.message, conn, user.id, notice="Ця дія вже неактивна.")
        return

    kind = str(tx_flow.get("kind") or parts[0] or "")
    if kind not in {"expense", "income"} and parts[0] in {"expense", "income"}:
        kind = parts[0]

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Створення витрат і доходів доступне лише з повним доступом.</b>",
            ):
                return
        if q.data not in {f"{kind}:cancel", "pick:acct:back", f"{kind}:account:back", "pick:cat:back"}:
            if await _billing_write_blocked(q.message, conn, user.id):
                return

        if q.data in {f"{kind}:cancel", "pick:acct:back"}:
            _reset_tx_flow(context)
            await _show_access_surface(q.message, conn, user.id)
            return

        if q.data in {f"{kind}:account:back", "pick:cat:back"}:
            accounts = await _get_accounts(conn, user.id)
            await q.message.reply_text(
                f"<b>{'➖ Нова витрата' if kind == 'expense' else '➕ Новий дохід'}</b>\n\n"
                "Крок <b>1 з 3</b>: оберіть рахунок.",
                reply_markup=kb_pick_account(accounts, kind),
            )
            return

        if len(parts) >= 3 and parts[1] == "currency":
            resolution_flow = context.user_data.get("currency_resolution_flow") or {}
            if str(resolution_flow.get("mode") or "") != "tx":
                await _show_access_surface(q.message, conn, user.id, notice="Ця дія вже неактивна.")
                return
            action = parts[2]
            callback_prefix = str(resolution_flow.get("callback_prefix") or f"{kind}:currency")
            if action == "back":
                await _show_currency_resolution_menu(q.message, resolution_flow)
                return
            if action == "create":
                await _start_currency_resolution_account_create(q.message, context, resolution_flow)
                return
            if action in {"auto", "manual"}:
                if not int(resolution_flow.get("target_account_id") or 0):
                    accounts_rows = await _get_accounts(conn, user.id)
                    resolution_flow["pending_action"] = action
                    context.user_data["currency_resolution_flow"] = resolution_flow
                    await _show_currency_resolution_target_picker(q.message, resolution_flow, accounts_rows)
                    return
                if action == "auto":
                    try:
                        fx_rate = await _resolve_nbu_rate(
                            str(resolution_flow.get("source_currency") or "UAH"),
                            str(resolution_flow.get("target_currency") or "UAH"),
                        )
                    except Exception:
                        await _show_currency_resolution_menu(
                            q.message,
                            resolution_flow,
                            notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                        )
                        return
                    await _apply_currency_resolution_conversion(
                        q.message,
                        context,
                        user.id,
                        fx_rate=fx_rate,
                        rate_source="nbu",
                    )
                    return
                resolution_flow["pending_action"] = "manual"
                resolution_flow["step"] = "await_manual_rate"
                context.user_data["currency_resolution_flow"] = resolution_flow
                await q.message.reply_text(
                    _currency_resolution_rate_prompt(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    ),
                    reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
                )
                return
            if action == "pick" and len(parts) == 5 and parts[3] == "acct":
                try:
                    account_id = int(parts[4])
                except ValueError:
                    return
                account = await _get_active_account_by_id(conn, user.id, account_id)
                if account is None:
                    await _show_currency_resolution_menu(
                        q.message,
                        resolution_flow,
                        notice="<b>Рахунок не знайдено.</b>",
                    )
                    return
                _set_currency_resolution_target(resolution_flow, account)
                context.user_data["currency_resolution_flow"] = resolution_flow
                if str(resolution_flow.get("pending_action") or "") == "auto":
                    try:
                        fx_rate = await _resolve_nbu_rate(
                            str(resolution_flow.get("source_currency") or "UAH"),
                            str(resolution_flow.get("target_currency") or "UAH"),
                        )
                    except Exception:
                        await _show_currency_resolution_menu(
                            q.message,
                            resolution_flow,
                            notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                        )
                        return
                    await _apply_currency_resolution_conversion(
                        q.message,
                        context,
                        user.id,
                        fx_rate=fx_rate,
                        rate_source="nbu",
                    )
                    return
                resolution_flow["pending_action"] = "manual"
                resolution_flow["step"] = "await_manual_rate"
                context.user_data["currency_resolution_flow"] = resolution_flow
                await q.message.reply_text(
                    _currency_resolution_rate_prompt(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    ),
                    reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
                )
                return

        if q.data == f"{kind}:edit:amount":
            tx_flow["step"] = "await_amount"
            tx_flow["await_amount"] = True
            context.user_data["tx_flow"] = tx_flow
            await q.message.reply_text(
                _tx_amount_prompt_text(
                    kind,
                    str(tx_flow.get("account_label") or "—"),
                    str(tx_flow.get("category_label") or "—"),
                ),
                reply_markup=kb_inline_cancel(f"{kind}:cancel"),
            )
            return

        if q.data == f"{kind}:edit:account":
            accounts = await _get_accounts(conn, user.id)
            await q.message.reply_text(
                f"<b>{'➖ Нова витрата' if kind == 'expense' else '➕ Новий дохід'}</b>\n"
                "Крок <b>1 з 3</b>: оберіть рахунок.",
                reply_markup=kb_pick_account(accounts, kind),
            )
            return

        if q.data == f"{kind}:edit:category":
            account_id = int(tx_flow.get("account_id") or 0)
            categories = await _get_categories(conn, user.id, kind)
            if account_id:
                await q.message.reply_text(
                    f"<b>{'➖ Нова витрата' if kind == 'expense' else '➕ Новий дохід'}</b>\n\n"
                    "Крок <b>2 з 3</b>: оберіть категорію.",
                    reply_markup=kb_pick_category(categories, kind),
                )
            return

        if parts[:2] == [kind, "confirm"]:
            if not is_current_confirmation(tx_flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            if str(tx_flow.get("step") or "") == TX_FLOW_STEP_COMMITTING:
                await q.message.reply_text(
                    "Операцію вже обробляю. Зачекайте кілька секунд.",
                    reply_markup=kb_home(),
                )
                return
            if str(tx_flow.get("step") or "") != TX_FLOW_STEP_CONFIRMATION:
                await q.message.reply_text(
                    "Спочатку введіть суму та дочекайтеся екрана підтвердження.",
                    reply_markup=kb_home(),
                )
                return

            account_id = int(tx_flow.get("account_id") or 0)
            category_id = int(tx_flow.get("category_id") or 0)
            amount = tx_flow.get("amount")
            category_label = str(tx_flow.get("category_label") or "—")
            category_snapshot_label = str(tx_flow.get("category_label") or "")
            currency = normalize_currency(str(tx_flow.get("currency") or "UAH"))
            if not account_id or not category_id or not isinstance(amount, Decimal):
                _reset_tx_flow(context)
                await _reply_and_return_home(q.message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
                return

            if not consume_financial_confirmation(tx_flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            tx_flow["step"] = TX_FLOW_STEP_COMMITTING
            context.user_data["tx_flow"] = tx_flow
            await _commit_tx_flow(q.message, context, user.id, tx_flow, conn=conn)
            return
            tx_comment = str(tx_flow.get("comment") or "").strip()[:500] or None
            transaction_result = await TransactionService(conn).commit_normal_transaction(
                user.id,
                transaction_date=datetime.now().date(),
                kind=kind,
                amount=amount,
                currency=currency,
                account_id=account_id,
                category_id=category_id,
                category_label=category_snapshot_label,
                comment=tx_comment,
                source="text",
            )
            if transaction_result.status == "account_missing":
                _reset_tx_flow(context)
                await _reply_and_return_home(q.message, "<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.")
                return
            if transaction_result.status != "completed":
                _reset_tx_flow(context)
                await _reply_and_return_home(q.message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
                return
            account = transaction_result.account
            if account is None:
                _reset_tx_flow(context)
                await _reply_and_return_home(q.message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
                return
            await log_bot_event(
                conn,
                user.id,
                "transaction_created",
                source="text",
                raw_input=str(tx_comment or ""),
                parsed_result={"type": kind, "amount": float(amount), "currency": currency},
            )
            await _mark_daily_expense_recorded_from_result(conn, user.id, transaction_result)

            _reset_tx_flow(context)
            prompted = await _maybe_prompt_saving_after_income(
                q.message,
                context,
                user.id,
                transaction_result=transaction_result,
                category_id=category_id,
            )
            if not prompted:
                await q.message.reply_text(
                    _income_saved_text(
                        kind=kind,
                        amount=amount,
                        currency=currency,
                        category_label=category_label,
                        account_label=str(account["label"]),
                        comment=tx_comment,
                    ),
                    reply_markup=kb_home(),
                )
            return

        if q.data.startswith(f"{kind}:account:") and len(parts) == 3:
            try:
                account_id = int(parts[2])
            except ValueError:
                return
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.")
                return
            tx_flow["account_id"] = account_id
            tx_flow["account_label"] = str(account["label"])
            tx_flow["currency"] = normalize_currency(str(account["currency"] or "UAH"))
            context.user_data["tx_flow"] = tx_flow
            categories = await _get_categories(conn, user.id, kind)
            await q.message.reply_text(
                f"<b>{'➖ Нова витрата' if kind == 'expense' else '➕ Новий дохід'}</b>\n\n"
                "Крок <b>2 з 3</b>: оберіть категорію.",
                reply_markup=kb_pick_category(categories, kind),
            )
            return

        if q.data.startswith(f"{kind}:category:") and len(parts) == 3:
            try:
                category_id = int(parts[2])
            except ValueError:
                return
            category_rows = await _get_categories_full(conn, user.id, kind)
            category_name = None
            for row_id, row_name, _aliases in category_rows:
                if row_id == category_id:
                    category_name = row_name
                    break
            if category_name is None:
                await q.message.reply_text("<b>❌ Категорію не вибрано</b>\n\nСпочатку оберіть категорію.")
                return
            tx_flow["category_id"] = category_id
            tx_flow["category_label"] = category_name
            tx_flow["step"] = "await_amount"
            tx_flow["await_amount"] = True
            context.user_data["tx_flow"] = tx_flow
            await q.message.reply_text(
                _tx_amount_prompt_text(
                    kind,
                    str(tx_flow.get("account_label") or "—"),
                    category_name,
                ),
                reply_markup=kb_inline_cancel(f"{kind}:cancel"),
            )
            return


async def _commit_tx_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    tx_flow: dict,
    *,
    conn: asyncpg.Connection,
    credit_limit_override: Decimal | None = None,
) -> None:
    kind = str(tx_flow.get("kind") or "expense")
    try:
        account_id = int(tx_flow.get("account_id") or 0)
        category_id = int(tx_flow.get("category_id") or 0)
        amount = tx_flow.get("amount")
        category_label = str(tx_flow.get("category_label") or "—")
        category_snapshot_label = str(tx_flow.get("category_label") or "")
        currency = normalize_currency(str(tx_flow.get("currency") or "UAH"))
        if not account_id or not category_id or not isinstance(amount, Decimal):
            raise ValueError("tx flow is incomplete")
    except Exception:
        _reset_tx_flow(context)
        await _reply_and_return_home(message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
        return

    tx_comment = str(tx_flow.get("comment") or "").strip()[:500] or None
    transaction_result = await TransactionService(conn).commit_normal_transaction(
        tg_user_id,
        transaction_date=datetime.now().date(),
        kind=kind,
        amount=amount,
        currency=currency,
        account_id=account_id,
        category_id=category_id,
        category_label=category_snapshot_label,
        comment=tx_comment,
        source="text",
        credit_limit_override=credit_limit_override,
    )
    if transaction_result.status == "account_missing":
        _reset_tx_flow(context)
        await _reply_and_return_home(message, "<b>Рахунок не знайдено</b>\n\nСписок міг змінитися. Оновіть рахунки й спробуйте ще раз.")
        return
    if transaction_result.status == "currency_account_mismatch":
        account = transaction_result.account or {}
        resolution_flow = _start_tx_currency_resolution(
            context,
            tx_flow,
            source_currency=normalize_currency(str(transaction_result.currency or currency)) or currency,
            amount=amount,
            comment=tx_comment,
        )
        if account:
            _set_currency_resolution_target(resolution_flow, account)
            context.user_data["currency_resolution_flow"] = resolution_flow
        await _show_currency_resolution_menu(message, resolution_flow)
        return
    if transaction_result.status == "credit_limit_required":
        tx_flow["step"] = "await_credit_limit"
        context.user_data["tx_flow"] = tx_flow
        account_label = str((transaction_result.account or {}).get("label") or tx_flow.get("account_label") or "—")
        await message.reply_text(
            _credit_limit_required_text(
                account_label,
                transaction_result.projected_balance or Decimal("0"),
                currency,
            ),
            reply_markup=kb_inline_cancel(f"{kind}:cancel"),
        )
        return
    if transaction_result.status == "credit_limit_exceeded":
        account_label = str((transaction_result.account or {}).get("label") or tx_flow.get("account_label") or "—")
        notice = _credit_limit_exceeded_text(
            account_label,
            transaction_result.projected_balance or Decimal("0"),
            transaction_result.credit_limit,
            currency,
        )
        if credit_limit_override is not None or str(tx_flow.get("step") or "") == "await_credit_limit":
            tx_flow["step"] = "await_credit_limit"
            context.user_data["tx_flow"] = tx_flow
            await message.reply_text(
                f"{notice}\n\nВведіть більший кредитний ліміт або скасуйте операцію.",
                reply_markup=kb_inline_cancel(f"{kind}:cancel"),
            )
            return
        tx_flow["step"] = TX_FLOW_STEP_CONFIRMATION
        context.user_data["tx_flow"] = tx_flow
        await message.reply_text(notice, reply_markup=_kb_tx_confirm(tx_flow))
        return
    if not _is_transaction_commit_success(transaction_result.status):
        _reset_tx_flow(context)
        await _reply_and_return_home(message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
        return

    account = transaction_result.account
    if account is None:
        _reset_tx_flow(context)
        await _reply_and_return_home(message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
        return

    await log_bot_event(
        conn,
        tg_user_id,
        "transaction_created",
        source="text",
        raw_input=str(tx_comment or ""),
        parsed_result={"type": kind, "amount": float(amount), "currency": currency},
    )
    await _mark_daily_expense_recorded_from_result(conn, tg_user_id, transaction_result)

    _reset_tx_flow(context)
    prompted = await _maybe_prompt_saving_after_income(
        message,
        context,
        tg_user_id,
        transaction_result=transaction_result,
        category_id=category_id,
    )
    if prompted:
        return

    saved_text = _income_saved_text(
        kind=kind,
        amount=amount,
        currency=currency,
        category_label=category_label,
        account_label=str(account["label"]),
        comment=tx_comment,
    )
    saved_text += _account_transition_notice(
        status=transaction_result.status,
        account_label=str(account["label"]),
        account_type=transaction_result.new_account_type,
        balance=transaction_result.new_balance,
        currency=currency,
    )
    reply_markup = (
        kb_transaction_saved(transaction_result.transaction_id)
        if transaction_result.transaction_id
        else kb_home()
    )
    await message.reply_text(saved_text, reply_markup=reply_markup)


async def _save_tx_amount(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str) -> None:
    user = update.effective_user
    if not user or not update.message:
        return

    amount = parse_decimal_amount(raw_text, allow_negative=False)
    if amount is None:
        parsed_amount = parse_amount(raw_text)
        if parsed_amount is not None:
            amount = quantize_money(Decimal(str(parsed_amount)))
    if amount is None or amount <= 0:
        await update.message.reply_text(
            "<b>❌ Некоректна сума</b>\n\nВведіть число.\n\nНаприклад:\n<code>500</code>\n<code>500,50</code>\n<code>500.50</code>"
        )
        return

    tx_flow = context.user_data.get("tx_flow") or {}
    if not tx_flow.get("account_id") or not tx_flow.get("category_id") or tx_flow.get("kind") not in {"expense", "income"}:
        _reset_tx_flow(context)
        await _reply_and_return_home(update.message, "<b>❌ Не вибрано рахунок</b>\n\nСпочатку оберіть рахунок.")
        return

    account_currency = normalize_currency(str(tx_flow.get("currency") or "UAH")) or "UAH"
    parsed_currency, currency_explicit = parse_currency_info(raw_text, default_currency=account_currency)
    comment = (raw_text or "").strip()
    if comment:
        match = re.search(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)", comment)
        if match:
            comment = (comment[: match.start()] + comment[match.end() :]).strip(" -–—:\t")
    if currency_explicit and parsed_currency != account_currency:
        resolution_flow = _start_tx_currency_resolution(
            context,
            tx_flow,
            source_currency=parsed_currency,
            amount=amount,
            comment=comment,
        )
        await _show_currency_resolution_menu(update.message, resolution_flow)
        return

    tx_flow["amount"] = amount
    tx_flow["currency"] = parsed_currency if currency_explicit else account_currency
    tx_flow["comment"] = comment
    tx_flow["step"] = TX_FLOW_STEP_CONFIRMATION
    tx_flow.pop("await_amount", None)
    context.user_data["tx_flow"] = tx_flow
    await update.message.reply_text(_tx_confirm_text(tx_flow), reply_markup=_kb_tx_confirm(tx_flow))


async def _handle_tx_credit_limit_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    tx_flow = context.user_data.get("tx_flow") or {}
    if str(tx_flow.get("step") or "") != "await_credit_limit":
        return

    credit_limit = parse_decimal_amount(update.message.text.strip(), allow_negative=False)
    if credit_limit is None or credit_limit <= 0:
        await update.message.reply_text(
            "Введіть кредитний ліміт числом, наприклад <code>5000</code> або <code>12500.50</code>.",
            reply_markup=kb_inline_cancel(f"{str(tx_flow.get('kind') or 'expense')}:cancel"),
        )
        return

    async with _pool(context).acquire() as conn:
        await _commit_tx_flow(
            update.message,
            context,
            user.id,
            tx_flow,
            conn=conn,
            credit_limit_override=credit_limit,
        )


def _ai_tx_card_text(flow: dict) -> str:
    tx = flow.get("tx") or {}
    accounts = flow.get("accounts") or {}
    categories = flow.get("categories") or {}

    tx_type = str(tx.get("type") or "").strip().lower()
    type_label = {"expense": "Витрата", "income": "Дохід"}.get(tx_type, "—")
    date_text = escape_html(str(tx.get("date") or "—"))
    amount_text = tx.get("amount")
    currency = normalize_currency(str(tx.get("currency") or "UAH"))

    account_id = tx.get("account_id")
    category_id = tx.get("category_id")
    account = accounts.get(int(account_id), "—") if account_id else "—"
    category = categories.get(int(category_id), "—") if category_id else "—"

    comment = (tx.get("comment") or "").strip()
    confidence = flow.get("confidence")
    amount_line = f"— {escape_html(currency)}"
    if amount_text is not None:
        try:
            amount_line = format_money(quantize_money(Decimal(str(amount_text))), currency)
        except (InvalidOperation, ValueError):
            amount_line = f"{escape_html(str(amount_text))} {escape_html(currency)}"

    lines = [
        "<b>Перевірте чернетку</b>",
        "",
        f"<b>📅 Дата:</b> {date_text}",
        f"<b>🏷 Тип:</b> {escape_html(type_label)}",
        f"<b>💰 Сума:</b> {amount_line}",
        f"<b>🏦 Рахунок:</b> {escape_html(str(account))}",
        f"<b>📂 Категорія:</b> {escape_html(str(category))}",
        f"<b>📝 Опис:</b> {f'<i>{escape_html(comment)}</i>' if comment else '<i>—</i>'}",
    ]
    if confidence is not None:
        try:
            lines.append(f"<b>🔎 Впевненість:</b> {max(0.0, min(float(confidence), 1.0)):.0%}")
        except (TypeError, ValueError):
            pass
    return "\n".join(lines)


def _ai_batch_flow_items(flow: dict) -> list[dict]:
    items = flow.get("items")
    if isinstance(items, list):
        return items
    items = []
    flow["items"] = items
    return items


def _ai_batch_kind(flow: dict) -> str:
    items = _ai_batch_flow_items(flow)
    if not items:
        return ""
    return str(items[0].get("type") or "").strip().lower()


def _ai_batch_item_kind(item: dict) -> str:
    return str(item.get("type") or "").strip().lower()


def _ai_batch_account_label(flow: dict) -> str:
    account_id = flow.get("account_id")
    accounts = flow.get("accounts") or {}
    if account_id:
        try:
            return str(accounts.get(int(account_id)) or "—")
        except (TypeError, ValueError):
            return "—"
    return "—"


def _ai_batch_date_summary(items: list[dict]) -> str:
    dates = sorted({str(item.get("date") or "").strip() for item in items if str(item.get("date") or "").strip()})
    if not dates:
        return "—"
    if len(dates) == 1:
        return escape_html(dates[0])
    return f"{escape_html(dates[0])} — {escape_html(dates[-1])}"


def _ai_batch_tx_card_text(flow: dict) -> str:
    items = _ai_batch_flow_items(flow)
    kinds = {_ai_batch_item_kind(item) for item in items if _ai_batch_item_kind(item)}
    kind = _ai_batch_kind(flow)
    type_label = {"expense": "Витрата", "income": "Дохід"}.get(kind, "Змішаний")
    if len(kinds) > 1:
        type_label = "Змішаний"
    total_amount = Decimal("0")
    currency = "UAH"
    for item in items:
        currency = normalize_currency(str(item.get("currency") or currency)) or currency
        try:
            total_amount += quantize_money(Decimal(str(item.get("amount") or 0)))
        except (InvalidOperation, ValueError):
            pass

    lines = [
        "<b>Перевірте список операцій</b>",
        "",
        f"<b>📅 Період:</b> {_ai_batch_date_summary(items)}",
        f"<b>🏷 Тип:</b> {escape_html(type_label)}",
        f"<b>🏦 Рахунок:</b> {escape_html(_ai_batch_account_label(flow))}",
        f"<b>🧾 Позицій:</b> {len(items)}",
        f"<b>💰 Разом:</b> {format_money(total_amount, currency)}",
        "",
    ]
    skipped_items_count = int(flow.get("skipped_items_count") or 0)
    if skipped_items_count > 0:
        lines.insert(-1, f"<b>⚠️ Пропущено рядків:</b> {skipped_items_count}")
    categories = flow.get("categories") or {}
    for index, item in enumerate(items, 1):
        amount_line = f"— {escape_html(currency)}"
        try:
            amount_line = format_money(quantize_money(Decimal(str(item.get('amount') or 0))), normalize_currency(str(item.get("currency") or currency)) or currency)
        except (InvalidOperation, ValueError):
            pass
        category_id = item.get("category_id")
        category_label = "—"
        if category_id:
            try:
                category_label = str(categories.get(int(category_id)) or "—")
            except (TypeError, ValueError):
                category_label = "—"
        suffix = ""
        if not category_id:
            suffix = " <i>(оберіть категорію)</i>"
        item_date = escape_html(str(item.get("date") or "—"))
        lines.append(f"<b>{index}.</b> {item_date} — {amount_line} — {escape_html(category_label)}{suffix}")
        comment = str(item.get("comment") or "").strip()
        if comment:
            lines.append(f"<i>{escape_html(comment)}</i>")
    return "\n".join(lines)


def _ai_batch_item_text(flow: dict, index: int) -> str:
    items = _ai_batch_flow_items(flow)
    if index < 0 or index >= len(items):
        return "Позицію не знайдено."
    item = items[index]
    currency = normalize_currency(str(item.get("currency") or "UAH")) or "UAH"
    amount_line = f"— {escape_html(currency)}"
    try:
        amount_line = format_money(quantize_money(Decimal(str(item.get("amount") or 0))), currency)
    except (InvalidOperation, ValueError):
        pass
    categories = flow.get("categories") or {}
    category_id = item.get("category_id")
    category_label = "—"
    if category_id:
        try:
            category_label = str(categories.get(int(category_id)) or "—")
        except (TypeError, ValueError):
            category_label = "—"
    comment = str(item.get("comment") or "").strip()
    type_label = {"expense": "Витрата", "income": "Дохід"}.get(_ai_batch_item_kind(item), "—")
    return "\n".join(
        [
            f"<b>Позиція {index + 1}</b>",
            "",
            f"<b>📅 Дата:</b> {escape_html(str(item.get('date') or '—'))}",
            f"<b>🏷 Тип:</b> {escape_html(type_label)}",
            f"<b>💰 Сума:</b> {amount_line}",
            f"<b>📂 Категорія:</b> {escape_html(category_label)}",
            f"<b>📝 Опис:</b> {f'<i>{escape_html(comment)}</i>' if comment else '<i>—</i>'}",
            "",
            "Що змінити?",
        ]
    )


def _build_ai_batch_tx_flow_state(
    *,
    items: list[dict],
    account_id: int | None,
    accounts_map: dict[int, str],
    categories_map: dict[int, str],
    origin: str,
    raw_text: str,
    confidence: float | None = None,
    draft_id: int | None = None,
    draft_metadata: dict | None = None,
    skipped_items_count: int = 0,
) -> dict:
    normalized_items: list[dict] = []
    for item in items:
        normalized_items.append(
            {
                "date": _normalize_ai_tx_date(item.get("date")),
                "type": str(item.get("type") or "").strip().lower(),
                "amount": item.get("amount"),
                "currency": normalize_currency(str(item.get("currency") or "UAH")) or "UAH",
                "currency_explicit": bool(item.get("currency_explicit")),
                "category_id": item.get("category_id"),
                "comment": (str(item.get("comment") or "").strip()[:500] or None),
                "source": str(item.get("source") or origin),
            }
        )
    flow = {
        "items": normalized_items,
        "account_id": account_id,
        "accounts": accounts_map,
        "categories": categories_map,
        "origin": origin,
        "raw_text": (raw_text or "").strip()[:500],
    }
    if confidence is not None:
        flow["confidence"] = confidence
    if draft_id is not None:
        flow["draft_id"] = draft_id
        flow["draft_source"] = AI_SCREENSHOT_SOURCE
    if draft_metadata:
        flow["draft_metadata"] = dict(draft_metadata)
    if skipped_items_count > 0:
        flow["skipped_items_count"] = int(skipped_items_count)
    return flow


def _ai_batch_saved_text(flow: dict, *, account_label: str) -> str:
    items = _ai_batch_flow_items(flow)
    currency = "UAH"
    total_amount = Decimal("0")
    for item in items:
        currency = normalize_currency(str(item.get("currency") or currency)) or currency
        try:
            total_amount += quantize_money(Decimal(str(item.get("amount") or 0)))
        except (InvalidOperation, ValueError):
            pass
    return "\n".join(
        [
            f"✅ Операцій збережено: {len(items)}.",
            f"💰 Разом: {format_money(total_amount, currency)}",
            f"🏦 Рахунок: {escape_html(account_label)}",
        ]
    )


def _ai_flow_tx(flow: dict) -> dict:
    return flow.setdefault("tx", {})


def _ai_tx_draft_source(flow: dict) -> str:
    return str(flow.get("draft_source") or "").strip()


def _ai_tx_has_persisted_draft(flow: dict) -> bool:
    return bool(flow.get("draft_id")) and bool(_ai_tx_draft_source(flow))


def _ai_tx_is_screenshot_flow(flow: dict) -> bool:
    return _ai_tx_has_persisted_draft(flow) and _ai_tx_draft_source(flow) == AI_SCREENSHOT_SOURCE


def _ai_tx_is_billing_flow(flow: dict) -> bool:
    return _ai_tx_has_persisted_draft(flow) and _ai_tx_draft_source(flow) == BILLING_TX_DRAFT_SOURCE


def _ai_batch_is_screenshot_flow(flow: dict) -> bool:
    return bool(flow.get("draft_id")) and str(flow.get("draft_source") or "") == AI_SCREENSHOT_SOURCE


def _billing_tx_callback_prefix(draft_id: int) -> str:
    return f"btx:{int(draft_id)}"


def _ai_tx_callback_prefix(flow: dict) -> str:
    if _ai_tx_is_billing_flow(flow):
        draft_id = int(flow.get("draft_id") or 0)
        if draft_id > 0:
            return _billing_tx_callback_prefix(draft_id)
    return "ai"


def _ai_tx_confirm_markup(flow: dict) -> InlineKeyboardMarkup:
    callback_prefix = _ai_tx_callback_prefix(flow)
    if callback_prefix == "ai":
        return bind_financial_preview(flow, kb_ai_tx_confirm())
    return bind_financial_preview(flow, kb_ai_tx_confirm_prefixed(callback_prefix))


def _ai_tx_edit_markup(flow: dict) -> InlineKeyboardMarkup:
    callback_prefix = _ai_tx_callback_prefix(flow)
    if callback_prefix == "ai":
        return kb_ai_tx_edit()
    return kb_ai_tx_edit_prefixed(callback_prefix)


def _ai_tx_pick_type_markup(flow: dict) -> InlineKeyboardMarkup:
    callback_prefix = _ai_tx_callback_prefix(flow)
    if callback_prefix == "ai":
        return kb_ai_pick_type()
    return kb_ai_pick_type_prefixed(callback_prefix, back_callback=f"{callback_prefix}:edit:back")


def _ai_tx_pick_account_markup(flow: dict, accounts_rows: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    callback_prefix = _ai_tx_callback_prefix(flow)
    if callback_prefix == "ai":
        return kb_ai_pick_account(accounts_rows)
    return kb_ai_pick_account(
        accounts_rows,
        callback_prefix=f"{callback_prefix}:pick:acct",
        back_callback=f"{callback_prefix}:edit:back",
    )


def _ai_tx_pick_category_markup(flow: dict, categories_rows: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    callback_prefix = _ai_tx_callback_prefix(flow)
    if callback_prefix == "ai":
        return kb_ai_pick_category(categories_rows)
    return kb_ai_pick_category(
        categories_rows,
        callback_prefix=f"{callback_prefix}:pick:cat",
        back_callback=f"{callback_prefix}:edit:back",
    )


def _billing_tx_inactive_text(draft: AiTransactionDraft | None) -> str:
    if draft is None:
        return BILLING_TX_INACTIVE_TEXT
    status = str(getattr(draft, "status", "") or "").strip().lower()
    if status == "completed":
        return "Цю оплату вже записано."
    if status == "cancelled":
        return "Цю чернетку вже скасовано."
    return BILLING_TX_INACTIVE_TEXT


def _normalize_ai_tx_date(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    return text or datetime.now().date().isoformat()


def _build_ai_tx_flow_state(
    *,
    tx: dict,
    accounts_map: dict[int, str],
    categories_map: dict[int, str],
    origin: str,
    confidence: float | None = None,
    draft_id: int | None = None,
    draft_source: str | None = None,
    draft_metadata: dict | None = None,
) -> dict:
    normalized_tx = {
        "date": _normalize_ai_tx_date(tx.get("date")),
        "type": str(tx.get("type") or "").strip().lower(),
        "amount": tx.get("amount"),
        "currency": normalize_currency(str(tx.get("currency") or "UAH")),
        "currency_explicit": bool(tx.get("currency_explicit")),
        "account_id": tx.get("account_id"),
        "category_id": tx.get("category_id"),
        "comment": (str(tx.get("comment") or "").strip()[:500] or None),
        "source": str(tx.get("source") or origin),
    }
    flow = {
        "tx": normalized_tx,
        "accounts": accounts_map,
        "categories": categories_map,
        "origin": origin,
    }
    if confidence is not None:
        flow["confidence"] = confidence
    if draft_id is not None:
        flow["draft_id"] = draft_id
        flow["draft_source"] = str(draft_source or AI_SCREENSHOT_SOURCE)
    if draft_metadata:
        flow["draft_metadata"] = dict(draft_metadata)
    return flow


def _is_amount_only_text(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if parse_amount(t) is None:
        return False
    # only digits/separators/spaces
    return bool(re.fullmatch(r"[\d\s.,]+", t))


def _pick_account_id_by_key(account_key: str | None, accounts_rows: list[tuple[int, str]]) -> int | None:
    if not account_key:
        return None
    key = account_key.strip().lower()
    hints: list[str]
    if key == "monobank":
        hints = ["моно", "mono", "monobank"]
    elif key == "privatbank":
        hints = ["приват", "privat"]
    elif key == "raiffeisen":
        hints = ["райф", "raif", "райфф"]
    elif key == "cash":
        hints = ["готівка", "cash", "кеш", "налич"]
    else:
        return None

    for account_id, label in accounts_rows:
        lbl = (label or "").lower()
        if any(h in lbl for h in hints):
            return int(account_id)
    return None


def _pick_account_id_by_hint(
    account_hint: str | None,
    bank_name: str | None,
    card_last4: str | None,
    accounts_rows: list[tuple[int, str]],
) -> int | None:
    for key in (account_hint, bank_name):
        picked = _pick_account_id_by_key(key, accounts_rows)
        if picked is not None:
            return picked

    textual_hints = [str(value or "").strip().lower() for value in (account_hint, bank_name) if str(value or "").strip()]
    digits_hint = re.sub(r"\D+", "", str(card_last4 or ""))[-4:]
    best_id: int | None = None
    best_len = 0
    for account_id, label in accounts_rows:
        label_text = str(label or "")
        lower_label = label_text.lower()
        if digits_hint and digits_hint in re.sub(r"\D+", "", label_text):
            return int(account_id)
        for hint in textual_hints:
            if hint and (hint in lower_label or lower_label in hint):
                if len(hint) > best_len:
                    best_len = len(hint)
                    best_id = int(account_id)
    return best_id


def _pick_account_id_by_currency(accounts_full: list[Any], currency: str | None) -> int | None:
    return _pick_account_id_by_currency_filtered(accounts_full, currency, allow_cash_accounts=True)


def _pick_account_id_by_currency_filtered(
    accounts_full: list[Any],
    currency: str | None,
    *,
    allow_cash_accounts: bool,
) -> int | None:
    normalized_currency = normalize_currency(str(currency or "").strip())
    if not normalized_currency:
        return None
    matches: list[int] = []
    non_savings_matches: list[int] = []
    for account in accounts_full:
        account_currency = normalize_currency(str(account.get("currency") or "").strip())
        if account_currency == normalized_currency:
            try:
                account_id = int(account["id"])
            except (KeyError, TypeError, ValueError):
                continue
            account_type = normalize_account_type(str(account.get("account_type") or ""))
            if not allow_cash_accounts and account_type == "cash":
                continue
            matches.append(account_id)
            if not is_savings_account_type(str(account.get("account_type") or "")):
                non_savings_matches.append(account_id)
    if len(non_savings_matches) == 1:
        return non_savings_matches[0]
    if len(matches) == 1:
        return matches[0]
    return None


async def _pick_recent_account_id_by_currency(conn: asyncpg.Connection, tg_user_id: int, currency: str | None) -> int | None:
    normalized_currency = normalize_currency(str(currency or "").strip())
    if not normalized_currency:
        return None
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        row = await conn.fetchrow(
            """
            SELECT t.account_id
            FROM transactions t
            JOIN accounts a
              ON a.id = t.account_id
             AND a.family_id = t.family_id
             AND a.is_active = true
            WHERE t.family_id=$1
              AND t.flow_kind='normal'
              AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
              AND t.account_id IS NOT NULL
              AND a.currency=$2
            ORDER BY t.date DESC, t.id DESC
            LIMIT 1
            """,
            int(scope.family_id),
            normalized_currency,
        )
    else:
        row = await conn.fetchrow(
            """
            SELECT t.account_id
            FROM transactions t
            JOIN accounts a
              ON a.id = t.account_id
             AND a.tg_user_id = t.tg_user_id
             AND a.family_id IS NULL
             AND a.is_active = true
            WHERE t.tg_user_id=$1
              AND t.family_id IS NULL
              AND t.flow_kind='normal'
              AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
              AND t.account_id IS NOT NULL
              AND a.currency=$2
            ORDER BY t.date DESC, t.id DESC
            LIMIT 1
            """,
            tg_user_id,
            normalized_currency,
        )
    try:
        return int(row["account_id"]) if row and row.get("account_id") is not None else None
    except (TypeError, ValueError, KeyError):
        return None


def _pick_account_id_for_ai_flow(
    *,
    account_hint: str | None,
    bank_name: str | None,
    card_last4: str | None,
    currency: str | None,
    accounts_rows: list[tuple[int, str]],
    accounts_full: list[Any],
    allow_single_account_fallback: bool = True,
    allow_currency_match: bool = True,
    allow_cash_currency_match: bool = True,
) -> int | None:
    picked = _pick_account_id_by_hint(account_hint, bank_name, card_last4, accounts_rows)
    if picked is not None:
        return picked
    if allow_currency_match:
        picked = _pick_account_id_by_currency_filtered(
            accounts_full,
            currency,
            allow_cash_accounts=allow_cash_currency_match,
        )
        if picked is not None:
            return picked
    if allow_single_account_fallback and len(accounts_rows) == 1:
        return int(accounts_rows[0][0])
    return None


def _screenshot_currency_is_explicit(analysis: ScreenshotAnalysis) -> bool:
    return is_screenshot_currency_explicit(analysis)


def _pick_recent_account_id_from_samples(
    samples: list[dict[str, Any]],
    *,
    accounts_full_by_id: dict[int, Any] | None = None,
    allow_cash_accounts: bool = True,
) -> int | None:
    fallback_account_id: int | None = None
    for sample in samples:
        try:
            account_id = sample.get("account_id")
        except AttributeError:
            continue
        try:
            normalized_account_id = int(account_id) if account_id is not None else None
        except (TypeError, ValueError):
            continue
        if normalized_account_id is None:
            continue
        if accounts_full_by_id is None:
            return normalized_account_id
        account = accounts_full_by_id.get(normalized_account_id)
        account_type = normalize_account_type(str((account or {}).get("account_type") or ""))
        if account_type == "cash" and not allow_cash_accounts:
            if fallback_account_id is None:
                fallback_account_id = normalized_account_id
            continue
        return normalized_account_id
    return fallback_account_id if allow_cash_accounts else None


_AI_MATCH_STOPWORDS = {
    "pokupka",
    "purchase",
    "payment",
    "oplata",
    "oplatau",
    "transaction",
    "transakciya",
    "tranzakciya",
    "card",
    "kartka",
    "karta",
    "rahunok",
    "schet",
    "рахунок",
    "картка",
    "карта",
    "оплата",
    "покупка",
    "транзакція",
    "транзакцiя",
    "комісія",
    "комиссия",
}


def _normalize_ai_match_text(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[_/|]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:200]


def _ai_match_tokens(value: str | None) -> list[str]:
    tokens: list[str] = []
    for token in _normalize_ai_match_text(value).split():
        if len(token) < 3:
            continue
        if token in _AI_MATCH_STOPWORDS:
            continue
        if token.isdigit() and len(token) < 4:
            continue
        tokens.append(token)
    return tokens


def _ai_text_similarity_score(left: str | None, right: str | None) -> int:
    left_norm = _normalize_ai_match_text(left)
    right_norm = _normalize_ai_match_text(right)
    if not left_norm or not right_norm:
        return 0
    if left_norm == right_norm:
        return 1000

    shorter, longer = (left_norm, right_norm) if len(left_norm) <= len(right_norm) else (right_norm, left_norm)
    if len(shorter) >= 6 and shorter in longer:
        return 800 + len(shorter)

    left_tokens = set(_ai_match_tokens(left_norm))
    right_tokens = set(_ai_match_tokens(right_norm))
    overlap = left_tokens & right_tokens
    if not overlap:
        return 0

    score = sum(len(token) for token in overlap)
    if len(overlap) >= 2:
        score += 10
    if left_tokens and overlap == left_tokens:
        score += 10
    return score


async def _load_recent_categorized_transactions(
    conn: asyncpg.Connection,
    tg_user_id: int,
    kind: str,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    normalized_kind = str(kind or "").strip().lower()
    if normalized_kind not in {"expense", "income"}:
        return []
    scope = await _get_finance_scope(conn, tg_user_id)
    if scope.is_family:
        rows = await conn.fetch(
            """
            SELECT category_id, account_id, comment
            FROM transactions
            WHERE family_id=$1
              AND flow_kind='normal'
              AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
              AND type=$2
              AND category_id IS NOT NULL
              AND comment IS NOT NULL
              AND btrim(comment) <> ''
            ORDER BY date DESC, id DESC
            LIMIT $3
            """,
            int(scope.family_id),
            normalized_kind,
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT category_id, account_id, comment
            FROM transactions
            WHERE tg_user_id=$1
              AND family_id IS NULL
              AND flow_kind='normal'
              AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
              AND type=$2
              AND category_id IS NOT NULL
              AND comment IS NOT NULL
              AND btrim(comment) <> ''
            ORDER BY date DESC, id DESC
            LIMIT $3
            """,
            tg_user_id,
            normalized_kind,
            limit,
        )
    samples: list[dict[str, Any]] = []
    for row in rows:
        try:
            category_id = int(row["category_id"])
        except (TypeError, ValueError, KeyError):
            continue
        comment = str(row.get("comment") or "").strip()
        if not comment:
            continue
        account_id = row.get("account_id")
        try:
            account_id = int(account_id) if account_id is not None else None
        except (TypeError, ValueError):
            account_id = None
        samples.append({"category_id": category_id, "account_id": account_id, "comment": comment})
    return samples


def _pick_category_id_by_recent_samples(
    texts: list[str | None],
    samples: list[dict[str, Any]],
    *,
    account_id: int | None = None,
) -> int | None:
    candidate_texts = [text for text in texts if _normalize_ai_match_text(text)]
    if not candidate_texts or not samples:
        return None

    scores: dict[int, int] = {}
    for sample in samples:
        sample_comment = str(sample.get("comment") or "")
        score = max((_ai_text_similarity_score(text, sample_comment) for text in candidate_texts), default=0)
        if score <= 0:
            continue
        sample_account_id = sample.get("account_id")
        if account_id is not None and sample_account_id == account_id:
            score += 12
        category_id = sample.get("category_id")
        try:
            category_id = int(category_id)
        except (TypeError, ValueError):
            continue
        current = scores.get(category_id, 0)
        scores[category_id] = max(current, score) + min(score, 15)

    if not scores:
        return None

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    best_id, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0
    if best_score >= 850:
        return best_id
    if best_score < 18:
        return None
    if second_score and best_score < second_score + 6:
        return None
    return best_id


def _vision_account_options(accounts_rows: list[tuple[int, str]]) -> list[VisionAccountOption]:
    return [VisionAccountOption(id=int(account_id), label=str(label)) for account_id, label in accounts_rows]


def _vision_category_options(kind: str, categories_rows: list[tuple[int, str, list[str]]]) -> list[VisionCategoryOption]:
    return [
        VisionCategoryOption(id=int(category_id), kind=kind, name=str(name), aliases=list(aliases or []))
        for category_id, name, aliases in categories_rows
    ]


def _ai_draft_metadata_from_flow(flow: dict) -> dict:
    metadata = dict(flow.get("draft_metadata") or {})
    metadata["origin"] = str(flow.get("origin") or "")
    metadata["source"] = str(_ai_flow_tx(flow).get("source") or "")
    metadata["currency_explicit"] = bool(_ai_flow_tx(flow).get("currency_explicit"))
    if _ai_tx_draft_source(flow) == AI_SCREENSHOT_SOURCE:
        metadata["screenshot_draft_schema_version"] = AI_SCREENSHOT_DRAFT_SCHEMA_VERSION
    return metadata


def _serialize_ai_batch_item_for_metadata(item: dict) -> dict:
    amount_value = None
    try:
        amount = quantize_money(Decimal(str(item.get("amount") or 0)))
    except (InvalidOperation, ValueError):
        amount = Decimal("0")
    if amount > 0:
        amount_value = str(amount)
    category_id = item.get("category_id")
    try:
        category_id = int(category_id) if category_id is not None else None
    except (TypeError, ValueError):
        category_id = None
    return {
        "date": _normalize_ai_tx_date(item.get("date")),
        "type": _ai_batch_item_kind(item) or "expense",
        "amount": amount_value,
        "currency": normalize_currency(str(item.get("currency") or "UAH")) or "UAH",
        "currency_explicit": bool(item.get("currency_explicit")),
        "category_id": category_id,
        "comment": (str(item.get("comment") or "").strip()[:500] or None),
        "source": str(item.get("source") or AI_SCREENSHOT_SOURCE),
    }


def _ai_batch_draft_tx(flow: dict) -> dict:
    items = _ai_batch_flow_items(flow)
    if not items:
        return {
            "date": datetime.now().date().isoformat(),
            "type": _ai_batch_kind(flow) or "expense",
            "amount": None,
            "currency": "UAH",
            "account_id": flow.get("account_id"),
            "category_id": None,
            "comment": None,
        }
    tx = dict(_serialize_ai_batch_item_for_metadata(items[0]))
    tx["account_id"] = flow.get("account_id")
    return tx


def _ai_batch_draft_metadata_from_flow(flow: dict) -> dict:
    metadata = dict(flow.get("draft_metadata") or {})
    metadata["origin"] = str(flow.get("origin") or "")
    metadata["source"] = AI_SCREENSHOT_SOURCE
    metadata["mode"] = "statement_expenses"
    metadata["screenshot_draft_schema_version"] = AI_SCREENSHOT_DRAFT_SCHEMA_VERSION
    metadata["statement_items"] = [_serialize_ai_batch_item_for_metadata(item) for item in _ai_batch_flow_items(flow)]
    metadata["statement_skipped_items_count"] = int(flow.get("skipped_items_count") or 0)
    return metadata


def _screenshot_draft_schema_version(metadata: Any) -> int:
    if not isinstance(metadata, dict):
        return 0
    try:
        return int(metadata.get("screenshot_draft_schema_version") or 0)
    except (TypeError, ValueError):
        return 0


def _should_reanalyze_pending_screenshot_draft(draft: AiTransactionDraft | None) -> bool:
    if draft is None or draft.source != AI_SCREENSHOT_SOURCE or draft.status != "pending":
        return False
    return _screenshot_draft_schema_version(draft.metadata) < AI_SCREENSHOT_DRAFT_SCHEMA_VERSION


def _restore_screenshot_statement_items(raw_items: Any, *, dominant_currency: str | None) -> list[dict]:
    if not isinstance(raw_items, list):
        return []
    normalized_dominant_currency = normalize_currency(str(dominant_currency or "").strip())
    restored_items: list[dict] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        if "currency_explicit" not in item:
            if normalized_dominant_currency:
                item["currency"] = normalized_dominant_currency
            item["currency_explicit"] = True
        elif not bool(item.get("currency_explicit")):
            if normalized_dominant_currency:
                item["currency"] = normalized_dominant_currency
        restored_items.append(item)
    return restored_items


def _build_ai_batch_tx_flow_from_draft(
    draft: AiTransactionDraft,
    *,
    accounts_map: dict[int, str],
    categories_map: dict[int, str],
) -> dict | None:
    metadata = dict(draft.metadata or {})
    if str(metadata.get("mode") or "").strip().lower() != "statement_expenses":
        return None
    raw_items = metadata.get("statement_items")
    if not isinstance(raw_items, list) or not raw_items:
        return None
    restored_items = _restore_screenshot_statement_items(
        raw_items,
        dominant_currency=metadata.get("dominant_currency"),
    )
    if not restored_items:
        return None
    return _build_ai_batch_tx_flow_state(
        items=restored_items,
        account_id=draft.account_id,
        accounts_map=accounts_map,
        categories_map=categories_map,
        origin=AI_SCREENSHOT_SOURCE,
        raw_text=f"screenshot:{draft.telegram_file_unique_id}",
        confidence=draft.confidence,
        draft_id=draft.id,
        draft_metadata=metadata,
        skipped_items_count=int(metadata.get("statement_skipped_items_count") or 0),
    )


def _build_ai_tx_flow_from_draft(
    draft: AiTransactionDraft,
    *,
    accounts_map: dict[int, str],
    categories_map: dict[int, str],
) -> dict:
    metadata = dict(draft.metadata or {})
    origin = str(metadata.get("origin") or draft.source or AI_SCREENSHOT_SOURCE)
    tx_source = str(metadata.get("source") or draft.source or origin)
    currency_explicit = bool(metadata.get("currency_explicit"))
    dominant_currency = normalize_currency(str(metadata.get("dominant_currency") or "").strip()) or None
    if draft.source == AI_SCREENSHOT_SOURCE and "currency_explicit" not in metadata:
        currency_explicit = True
    tx = {
        "date": draft.transaction_date.isoformat() if draft.transaction_date else datetime.now().date().isoformat(),
        "type": draft.tx_type or "",
        "amount": draft.amount,
        "currency": dominant_currency or draft.currency or "UAH",
        "currency_explicit": currency_explicit,
        "account_id": draft.account_id,
        "category_id": draft.category_id,
        "comment": draft.comment,
        "source": tx_source,
    }
    return _build_ai_tx_flow_state(
        tx=tx,
        accounts_map=accounts_map,
        categories_map=categories_map,
        origin=origin,
        confidence=draft.confidence,
        draft_id=draft.id,
        draft_source=draft.source,
        draft_metadata=metadata,
    )


async def _restore_existing_pending_screenshot_draft_preview(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    draft: AiTransactionDraft,
    *,
    accounts_map: dict[int, str],
    categories_map: dict[int, str],
) -> bool:
    restored_batch_flow = _build_ai_batch_tx_flow_from_draft(
        draft,
        accounts_map=accounts_map,
        categories_map=categories_map,
    )
    if restored_batch_flow is not None:
        context.user_data["ai_batch_tx_flow"] = restored_batch_flow
        await message.reply_text("Цей скріншот уже є в чернетках. Показую знайдені витрати.")
        await message.reply_text(
            _ai_batch_tx_card_text(restored_batch_flow),
            reply_markup=bind_financial_preview(restored_batch_flow, kb_ai_batch_confirm(len(_ai_batch_flow_items(restored_batch_flow)))),
        )
        return True
    restored_flow = _build_ai_tx_flow_from_draft(
        draft,
        accounts_map=accounts_map,
        categories_map=categories_map,
    )
    context.user_data["ai_tx_flow"] = restored_flow
    await message.reply_text("Цей скріншот уже є в чернетках. Показую підготовлену операцію.")
    await message.reply_text(_ai_tx_card_text(restored_flow), reply_markup=_ai_tx_confirm_markup(restored_flow))
    return True


async def _sync_ai_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_tx_has_persisted_draft(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).update_draft(
            draft_id,
            tg_user_id,
            tx=_ai_flow_tx(flow),
            confidence=flow.get("confidence"),
            metadata=_ai_draft_metadata_from_flow(flow),
        )


async def _find_existing_transaction_by_source(conn: asyncpg.Connection, tg_user_id: int, source: str) -> int | None:
    normalized_source = str(source or "").strip()
    if not normalized_source:
        return None
    row = await conn.fetchrow(
        """
        SELECT id
        FROM transactions
        WHERE tg_user_id=$1
          AND flow_kind='normal'
          AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
          AND source=$2
        ORDER BY id DESC
        LIMIT 1
        """,
        int(tg_user_id),
        normalized_source,
    )
    try:
        return int(row["id"]) if row and row.get("id") is not None else None
    except (TypeError, ValueError, KeyError):
        return None


async def _restore_billing_tx_flow(
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    draft_id: int,
) -> tuple[dict | None, str | None]:
    async with _pool(context).acquire() as conn:
        draft_service = AiTransactionDraftService(conn)
        draft = await draft_service.get_by_id(draft_id, tg_user_id)
        if draft is None or draft.source != BILLING_TX_DRAFT_SOURCE:
            return None, BILLING_TX_INACTIVE_TEXT
        if draft.status != "pending":
            return None, _billing_tx_inactive_text(draft)

        catalog = await _load_ai_catalog(conn, tg_user_id)
        metadata = dict(draft.metadata or {})
        tx_source = str(metadata.get("source") or "").strip()
        if tx_source:
            existing_tx_id = await _find_existing_transaction_by_source(conn, tg_user_id, f"ai_{tx_source}")
            if existing_tx_id is not None:
                await draft_service.mark_completed(draft.id, tg_user_id)
                return None, _billing_tx_inactive_text(
                    AiTransactionDraft(
                        id=draft.id,
                        tg_user_id=draft.tg_user_id,
                        source=draft.source,
                        telegram_file_unique_id=draft.telegram_file_unique_id,
                        telegram_file_id=draft.telegram_file_id,
                        telegram_message_id=draft.telegram_message_id,
                        status="completed",
                        transaction_date=draft.transaction_date,
                        tx_type=draft.tx_type,
                        amount=draft.amount,
                        currency=draft.currency,
                        account_id=draft.account_id,
                        category_id=draft.category_id,
                        comment=draft.comment,
                        confidence=draft.confidence,
                        metadata=draft.metadata,
                        created_at=draft.created_at,
                        updated_at=draft.updated_at,
                        confirmed_at=draft.confirmed_at,
                        cancelled_at=draft.cancelled_at,
                    )
                )

        flow = _build_ai_tx_flow_from_draft(
            draft,
            accounts_map=catalog["accounts_map"],
            categories_map=catalog["categories_map"],
        )
        tx = _ai_flow_tx(flow)
        changed = False

        account_id = tx.get("account_id")
        try:
            account_id = int(account_id) if account_id is not None else None
        except (TypeError, ValueError):
            account_id = None
        if account_id is not None and account_id not in (catalog.get("accounts_map") or {}):
            account_id = None
            tx["account_id"] = None
            changed = True
        if account_id is None:
            account_id = _pick_account_id_for_ai_flow(
                account_hint=None,
                bank_name=None,
                card_last4=None,
                currency=tx.get("currency"),
                accounts_rows=catalog["accounts_rows"],
                accounts_full=catalog["accounts_full"],
            )
            if account_id is None:
                account_id = await _pick_recent_account_id_by_currency(conn, tg_user_id, tx.get("currency"))
            if account_id is not None:
                tx["account_id"] = int(account_id)
                changed = True

        category_id = tx.get("category_id")
        try:
            category_id = int(category_id) if category_id is not None else None
        except (TypeError, ValueError):
            category_id = None
        if category_id is not None and category_id not in (catalog.get("categories_map") or {}):
            category_id = None
            tx["category_id"] = None
            changed = True
        if category_id is None:
            category = await CategoryService(conn).getCategoryBySlug(tg_user_id, "expense", BILLING_TX_CATEGORY_SLUG)
            if category is not None:
                tx["category_id"] = int(category.id)
                flow["categories"][int(category.id)] = str(category.name)
                changed = True

        if changed:
            await draft_service.update_draft(
                draft.id,
                tg_user_id,
                tx=tx,
                confidence=flow.get("confidence"),
                metadata=_ai_draft_metadata_from_flow(flow),
            )
        return flow, None


async def _sync_ai_batch_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_batch_is_screenshot_flow(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).update_draft(
            draft_id,
            tg_user_id,
            tx=_ai_batch_draft_tx(flow),
            confidence=flow.get("confidence"),
            metadata=_ai_batch_draft_metadata_from_flow(flow),
        )


async def _commit_ai_tx_flow(
    message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    flow: dict,
    *,
    conn: asyncpg.Connection,
    credit_limit_override: Decimal | None = None,
) -> None:
    tx = _ai_flow_tx(flow)
    try:
        tx_type = str(tx.get("type") or "").strip().lower()
        if tx_type not in {"expense", "income"}:
            raise ValueError("unsupported ai tx type")
        tx_date = date.fromisoformat(str(tx.get("date")))
        amount = quantize_money(Decimal(str(tx.get("amount") or 0)))
        account_id = int(tx.get("account_id"))
        category_id = int(tx.get("category_id"))
        category_label = str((flow.get("categories") or {}).get(category_id) or "")
        comment = (str(tx.get("comment") or "")[:500] or None) if tx.get("comment") is not None else None
        source = str(tx.get("source") or "ai")
    except Exception:
        _reset_ai_tx_flow(context)
        await _reply_and_return_home(message, "⚠️ Не зміг зберегти: чернетка пошкоджена. Спробуйте ще раз.")
        return

    if amount <= 0:
        flow["await_field"] = "amount"
        flow["await_amount"] = True
        context.user_data["ai_tx_flow"] = flow
        await message.reply_text(
            "Не вдалося розпізнати суму. Введіть її числом, напр. <code>500</code> або <code>500 таксі</code>.",
            reply_markup=_kb_home_only(),
        )
        return

    account = await _get_active_account_by_id(conn, tg_user_id, account_id)
    if account is None:
        logger.warning(
            "ai_tx_flow precheck_account_missing tg_user_id=%s account_id=%s tx_type=%s amount=%s draft_id=%s origin=%s",
            tg_user_id,
            account_id,
            tx_type,
            amount,
            flow.get("draft_id"),
            flow.get("origin"),
        )
        _reset_ai_tx_flow(context)
        await _reply_and_return_home(
            message,
            "Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
        )
        return

    currency = normalize_currency(str(tx.get("currency") or account["currency"] or "UAH")) or "UAH"
    if _ai_tx_is_billing_flow(flow):
        existing_tx_id = await _find_existing_transaction_by_source(conn, tg_user_id, f"ai_{source}")
        if existing_tx_id is not None:
            await _complete_ai_draft_flow(context, tg_user_id, flow)
            _reset_ai_tx_flow(context)
            await message.reply_text("Цю оплату вже записано.", reply_markup=kb_home())
            return
    logger.info(
        "ai_tx_flow commit_start tg_user_id=%s account_id=%s tx_type=%s amount=%s currency=%s balance=%s account_type=%s credit_limit=%s draft_id=%s origin=%s credit_limit_override=%s",
        tg_user_id,
        account_id,
        tx_type,
        amount,
        currency,
        account.get("balance"),
        account.get("account_type"),
        account.get("credit_limit"),
        flow.get("draft_id"),
        flow.get("origin"),
        credit_limit_override,
    )
    draft_id = int(flow["draft_id"]) if _ai_tx_has_persisted_draft(flow) else None
    try:
        async with AiTransactionDraftService(conn).atomic_confirmation(draft_id, tg_user_id) as guard:
            tx_result = await TransactionService(conn).commit_normal_transaction(
                tg_user_id,
                transaction_date=tx_date,
                kind=tx_type,
                amount=amount,
                currency=currency,
                account_id=account_id,
                category_id=category_id,
                category_label=category_label,
                comment=comment,
                source=f"ai_{source}",
                credit_limit_override=credit_limit_override,
            )
            guard.succeeded = _is_transaction_commit_success(tx_result.status)
    except ValueError as exc:
        if str(exc) != "draft_not_pending":
            raise
        _reset_ai_tx_flow(context)
        await message.reply_text("Цю чернетку вже завершено або скасовано. Повторну операцію не створюю.", reply_markup=kb_home())
        return
    logger.info(
        "ai_tx_flow commit_result tg_user_id=%s account_id=%s tx_type=%s amount=%s status=%s previous_balance=%s projected_balance=%s new_balance=%s previous_account_type=%s new_account_type=%s credit_limit=%s draft_id=%s origin=%s credit_limit_override=%s",
        tg_user_id,
        account_id,
        tx_type,
        amount,
        tx_result.status,
        tx_result.previous_balance,
        tx_result.projected_balance,
        tx_result.new_balance,
        tx_result.previous_account_type,
        tx_result.new_account_type,
        tx_result.credit_limit,
        flow.get("draft_id"),
        flow.get("origin"),
        credit_limit_override,
    )
    if tx_result.status == "account_missing":
        _reset_ai_tx_flow(context)
        await _reply_and_return_home(
            message,
            "Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
        )
        return
    if tx_result.status == "currency_account_mismatch":
        resolution_flow = _start_ai_tx_currency_resolution(
            context,
            flow,
            source_currency=currency,
            target_account=(tx_result.account or account),
        )
        await _sync_ai_draft_flow(context, tg_user_id, flow)
        await _show_currency_resolution_menu(message, resolution_flow)
        return
    if tx_result.status == "credit_limit_required":
        flow.pop("await_amount", None)
        flow["await_field"] = "credit_limit"
        context.user_data["ai_tx_flow"] = flow
        account_label = str((tx_result.account or {}).get("label") or account.get("label") or "—")
        await message.reply_text(
            _credit_limit_required_text(
                account_label,
                tx_result.projected_balance or Decimal("0"),
                currency,
            ),
            reply_markup=_kb_home_only(),
        )
        return
    if tx_result.status == "credit_limit_exceeded":
        account_label = str((tx_result.account or {}).get("label") or account.get("label") or "—")
        notice = _credit_limit_exceeded_text(
            account_label,
            tx_result.projected_balance or Decimal("0"),
            tx_result.credit_limit,
            currency,
        )
        if credit_limit_override is not None or str(flow.get("await_field") or "") == "credit_limit":
            flow.pop("await_amount", None)
            flow["await_field"] = "credit_limit"
            context.user_data["ai_tx_flow"] = flow
            await message.reply_text(
                f"{notice}\n\nВведіть більший кредитний ліміт числом.",
                reply_markup=_kb_home_only(),
            )
            return
        flow.pop("await_amount", None)
        flow.pop("await_field", None)
        context.user_data["ai_tx_flow"] = flow
        await message.reply_text(notice, reply_markup=_ai_tx_confirm_markup(flow))
        return
    if not _is_transaction_commit_success(tx_result.status):
        _reset_ai_tx_flow(context)
        await _reply_and_return_home(message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
        return

    committed_account = tx_result.account
    if committed_account is None:
        _reset_ai_tx_flow(context)
        await _reply_and_return_home(message, "<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.")
        return

    await log_bot_event(
        conn,
        tg_user_id,
        "transaction_created",
        source=f"ai_{source}",
        raw_input=comment or "",
        parsed_result={"type": tx_type, "amount": float(amount), "currency": currency},
    )
    await _mark_daily_expense_recorded_from_result(conn, tg_user_id, tx_result)

    _reset_ai_tx_flow(context)
    prompted = await _maybe_prompt_saving_after_income(
        message,
        context,
        tg_user_id,
        transaction_result=tx_result,
        category_id=category_id,
    )
    if prompted:
        return

    saved_text = _income_saved_text(
        kind=tx_type,
        amount=amount,
        currency=currency,
        category_label=category_label,
        account_label=str(committed_account["label"]),
        comment=comment,
    )
    saved_text = saved_text.replace("✅ Дохід збережено", "✅ Операцію збережено", 1)
    saved_text = saved_text.replace("✅ Витрату збережено", "✅ Операцію збережено", 1)
    saved_text += _account_transition_notice(
        status=tx_result.status,
        account_label=str(committed_account["label"]),
        account_type=tx_result.new_account_type,
        balance=tx_result.new_balance,
        currency=currency,
    )
    reply_markup = kb_transaction_saved(tx_result.transaction_id) if tx_result.transaction_id else kb_home()
    await message.reply_text(saved_text, reply_markup=reply_markup)


async def _cancel_ai_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_tx_has_persisted_draft(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).mark_cancelled(draft_id, tg_user_id)


async def _cancel_ai_batch_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_batch_is_screenshot_flow(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).mark_cancelled(draft_id, tg_user_id)


async def _complete_ai_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_tx_has_persisted_draft(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).mark_completed(draft_id, tg_user_id)


async def _complete_ai_batch_draft_flow(context: ContextTypes.DEFAULT_TYPE, tg_user_id: int, flow: dict) -> None:
    if not _ai_batch_is_screenshot_flow(flow):
        return
    draft_id = int(flow.get("draft_id") or 0)
    if draft_id <= 0:
        return
    async with _pool(context).acquire() as conn:
        await AiTransactionDraftService(conn).mark_completed(draft_id, tg_user_id)


def _extract_incoming_image(message) -> _IncomingImage | None:
    if not message:
        return None
    document = getattr(message, "document", None)
    if document and getattr(document, "file_id", None):
        mime_type = str(getattr(document, "mime_type", "") or "").strip().lower()
        if mime_type == "image/jpg":
            mime_type = "image/jpeg"
        return _IncomingImage(
            telegram_file_id=str(document.file_id),
            telegram_file_unique_id=str(document.file_unique_id),
            mime_type=mime_type,
            file_size=int(getattr(document, "file_size", 0) or 0),
            source="document",
        )
    photos = list(getattr(message, "photo", []) or [])
    if photos:
        photo = photos[-1]
        return _IncomingImage(
            telegram_file_id=str(photo.file_id),
            telegram_file_unique_id=str(photo.file_unique_id),
            mime_type="image/jpeg",
            file_size=int(getattr(photo, "file_size", 0) or 0),
            source="photo",
        )
    return None


def _is_supported_screenshot_mime(mime_type: str) -> bool:
    normalized = str(mime_type or "").strip().lower()
    if normalized == "image/jpg":
        normalized = "image/jpeg"
    return normalized in AI_SCREENSHOT_ALLOWED_MIME_TYPES


async def _load_ai_catalog(conn: asyncpg.Connection, tg_user_id: int) -> dict[str, Any]:
    accounts_rows = await _get_accounts(conn, tg_user_id)
    accounts_full = await _get_accounts_full(conn, tg_user_id)
    await _ensure_default_categories(conn, tg_user_id)
    expense_full = await _get_categories_full(conn, tg_user_id, "expense")
    income_full = await _get_categories_full(conn, tg_user_id, "income")
    expense_rows = [(c_id, name) for c_id, name, _ in expense_full]
    income_rows = [(c_id, name) for c_id, name, _ in income_full]
    return {
        "accounts_rows": accounts_rows,
        "accounts_full": accounts_full,
        "expense_full": expense_full,
        "income_full": income_full,
        "expense_rows": expense_rows,
        "income_rows": income_rows,
        "accounts_map": {int(a_id): label for a_id, label in accounts_rows},
        "categories_map": {int(c_id): name for c_id, name in (expense_rows + income_rows)},
    }


async def _start_transfer_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> bool:
    user = update.effective_user
    if not user or not update.message:
        return False

    if _is_amount_only_text(text):
        return False

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return True
        base_currency = _default_currency_from_user_row(await _get_user(conn, user.id))
        parsed = parse_message(text, today=datetime.now().date(), default_currency=base_currency)
        if not parsed.is_candidate_tx or parsed.intent != "transfer":
            return False
        await _recalculate_account_balances(conn, user.id)
        accounts_full = await _get_accounts_full(conn, user.id)
        if len(accounts_full) < 2:
            await _show_accounts_settings(
                update.message,
                context,
                user.id,
                notice="Для переказу потрібно мінімум 2 рахунки.\n\nСпочатку додайте ще один рахунок.",
                conn=conn,
            )
            return True

        accounts_rows = [(int(a["id"]), str(a["label"])) for a in accounts_full]
        by_id = {int(a["id"]): a for a in accounts_full}

        source_account_id = _pick_account_id_by_key((parsed.from_account_id or parsed.account_id), accounts_rows)
        target_account_id = _pick_account_id_by_key(parsed.to_account_id, accounts_rows)
        if source_account_id and target_account_id and source_account_id == target_account_id:
            target_account_id = None

        if not source_account_id:
            await _show_transfer_source_step(update.message, context, user.id, conn=conn)
            return True

        if not target_account_id:
            await _show_transfer_target_step(update.message, context, user.id, source_account_id, conn=conn)
            return True

        source_account = by_id.get(source_account_id)
        target_account = by_id.get(target_account_id)
        if source_account is None or target_account is None:
            await _show_accounts_settings(
                update.message,
                context,
                user.id,
                notice="Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
                conn=conn,
            )
            return True

    flow: dict = {
        "step": "enter_source_amount",
        "source_account_id": int(source_account["id"]),
        "source_label": str(source_account["label"]),
        "source_currency": normalize_currency(str(source_account["currency"])),
        "source_balance": Decimal(str(source_account["balance"] or 0)),
        "target_account_id": int(target_account["id"]),
        "target_label": str(target_account["label"]),
        "target_currency": normalize_currency(str(target_account["currency"])),
        "target_balance": Decimal(str(target_account["balance"] or 0)),
    }

    if parsed.amount is None:
        context.user_data["transfer_flow"] = flow
        await _show_transfer_amount_step(update.message, context, flow)
        return True

    source_amount = quantize_money(Decimal(str(parsed.amount)))
    if source_amount <= 0:
        context.user_data["transfer_flow"] = flow
        await _show_transfer_amount_step(
            update.message,
            context,
            flow,
            notice="Не вдалося розпізнати суму.\n\nВведіть число, наприклад:\n1000\n1000.50\n1000,50",
        )
        return True
    flow["source_amount"] = source_amount
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    if source_currency == target_currency:
        flow = _prepare_transfer_preview(flow)
        context.user_data["transfer_flow"] = flow
        await update.message.reply_text(
            _transfer_confirm_text(flow),
            reply_markup=_kb_transfer_confirm(allow_rate_edit=False, flow=flow),
        )
        return True

    flow["step"] = "enter_exchange_rate"
    context.user_data["transfer_flow"] = flow
    await _show_transfer_rate_step(update.message, flow)
    return True


async def _start_ai_tx_flow(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, *, origin: str = "text") -> bool:
    user = update.effective_user
    if not user or not update.message:
        return False

    if _is_amount_only_text(text):
        return False

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return True

        base_currency = _default_currency_from_user_row(await _get_user(conn, user.id))
        catalog = await _load_ai_catalog(conn, user.id)
        accounts_rows = catalog["accounts_rows"]
        accounts_full = list(catalog.get("accounts_full") or [])
        accounts_full_by_id = {int(row["id"]): row for row in accounts_full}
        categories_map = catalog["categories_map"]
        expense_full = catalog["expense_full"]
        income_full = catalog["income_full"]
        expense_rows = catalog["expense_rows"]
        income_rows = catalog["income_rows"]

    if not accounts_rows or not expense_rows or not income_rows:
        return False

    today = datetime.now().date()
    batch_candidates = parse_message_batch(text, today=today, default_currency=base_currency)
    if batch_candidates:
        whole_message = parse_message(text, today=today, default_currency=base_currency)
        kind = str(batch_candidates[0].intent or "").strip().lower()
        picked_full = income_full if kind == "income" else expense_full
        shared_account_id = _pick_account_id_for_ai_flow(
            account_hint=whole_message.account_id,
            bank_name=None,
            card_last4=None,
            currency=whole_message.currency,
            accounts_rows=accounts_rows,
            accounts_full=accounts_full,
            allow_single_account_fallback=not bool(getattr(whole_message, "currency_explicit", False)),
        )
        batch_items: list[dict] = []
        for candidate in batch_candidates:
            category_id = _pick_category_id_by_key(candidate.category_id, picked_full)
            if category_id is None:
                category_id = _pick_category_id_by_text(
                    candidate.comment or candidate.raw_text,
                    picked_full,
                    fallback_to_other=True,
                )
            batch_items.append(
                {
                    "date": (candidate.date.isoformat() if candidate.date else today.isoformat()),
                    "type": kind,
                    "amount": candidate.amount,
                    "currency": candidate.currency,
                    "currency_explicit": bool(candidate.currency_explicit),
                    "category_id": category_id,
                    "comment": (candidate.comment or candidate.raw_text[:500] or None),
                    "source": "rules",
                }
            )
        batch_flow = _build_ai_batch_tx_flow_state(
            items=batch_items,
            account_id=shared_account_id,
            accounts_map=catalog["accounts_map"],
            categories_map=catalog["categories_map"],
            origin=origin,
            raw_text=text,
        )
        if shared_account_id and shared_account_id in accounts_full_by_id:
            account_currency = _normalized_account_currency(accounts_full_by_id[shared_account_id], fallback=base_currency)
            for item in batch_flow.get("items", []):
                if not bool(item.get("currency_explicit")):
                    item["currency"] = account_currency
        context.user_data["ai_batch_tx_flow"] = batch_flow
        await update.message.reply_text(
            _ai_batch_tx_card_text(context.user_data["ai_batch_tx_flow"]),
            reply_markup=bind_financial_preview(batch_flow, kb_ai_batch_confirm(len(batch_items))),
        )
        return True

    parsed = parse_message(text, today=today, default_currency=base_currency)
    if not parsed.is_candidate_tx or parsed.intent not in {"expense", "income"}:
        return False

    kind = parsed.intent
    picked_full = income_full if kind == "income" else expense_full
    category_id = _pick_category_id_by_key(parsed.category_id, picked_full)
    if category_id is None:
        category_id = _pick_category_id_by_text(text, picked_full, fallback_to_other=True)
    account_id = _pick_account_id_for_ai_flow(
        account_hint=parsed.account_id,
        bank_name=None,
        card_last4=None,
        currency=parsed.currency,
        accounts_rows=accounts_rows,
        accounts_full=accounts_full,
        allow_single_account_fallback=not bool(parsed.currency_explicit),
    )

    flow_currency = parsed.currency
    if account_id and account_id in accounts_full_by_id and not parsed.currency_explicit:
        flow_currency = _normalized_account_currency(accounts_full_by_id[account_id], fallback=base_currency)

    context.user_data["ai_tx_flow"] = _build_ai_tx_flow_state(
        tx={
            "date": (parsed.date.isoformat() if parsed.date else datetime.now().date().isoformat()),
            "type": kind,
            "amount": parsed.amount,
            "currency": flow_currency,
            "currency_explicit": bool(parsed.currency_explicit),
            "account_id": account_id,
            "category_id": category_id,
            "comment": (parsed.comment or (text.strip()[:500] or None)),
            "source": "rules",
        },
        accounts_map=catalog["accounts_map"],
        categories_map=catalog["categories_map"],
        origin=origin,
        confidence=getattr(parsed, "confidence", None),
    )

    flow = context.user_data["ai_tx_flow"]
    if parsed.amount is None:
        flow["await_amount"] = True
    tx = flow.get("tx") or {}
    if flow.get("await_amount"):
        type_label = "витрата" if kind == "expense" else "дохід"
        cat_name = categories_map.get(int(category_id), None) if category_id else None
        parts: list[str] = [f"Розпізнав: {type_label}."]
        if cat_name:
            parts.append(f"Категорія: «{cat_name}».")
        if tx.get("comment"):
            parts.append(f"Коментар: «{tx.get('comment')}».")
        parts.append("Вкажи суму числом (наприклад: `200`).")
        await update.message.reply_text("\n".join(parts), parse_mode=ParseMode.HTML, reply_markup=_kb_home_only())
        return True

    if not tx.get("account_id"):
        await update.message.reply_text(
            "Я не зрозумів рахунок. Оберіть, будь ласка:",
            reply_markup=_ai_tx_pick_account_markup(flow, accounts_rows),
        )
        return True
    if not tx.get("category_id"):
        cats = income_rows if str(tx.get("type") or "expense") == "income" else expense_rows
        await update.message.reply_text(
            "Я не знайшов категорію. Оберіть, будь ласка:",
            reply_markup=_ai_tx_pick_category_markup(flow, cats),
        )
        return True

    await update.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
    return True


async def _handle_ai_batch_tx_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, flow: dict) -> bool:
    user = update.effective_user
    if not user or not update.message or not flow:
        return False

    await_field = str(flow.get("await_field") or "").strip().lower()
    if not await_field:
        return False

    items = _ai_batch_flow_items(flow)
    raw_text = (update.message.text or "").strip()

    if await_field == "date":
        parsed_date = _parse_date_ddmmyyyy(raw_text)
        if parsed_date is None:
            parsed = parse_message(raw_text, today=datetime.now().date(), default_currency="UAH")
            parsed_date = parsed.date
        if parsed_date is None:
            await update.message.reply_text(
                "Не зміг розпізнати дату. Приклади: <code>2026-05-03</code>, <code>03.05.2026</code>, <code>сьогодні</code>, <code>вчора</code>.",
                reply_markup=_kb_home_only(),
            )
            return True
        edit_item_index = flow.get("edit_item_index")
        if edit_item_index is None:
            for item in items:
                item["date"] = parsed_date.isoformat()
        else:
            try:
                item_index = int(edit_item_index)
            except (TypeError, ValueError):
                _reset_ai_batch_tx_flow(context)
                await _reply_and_return_home(update.message, "Чернетка пакета пошкоджена. Спробуйте ще раз.")
                return True
            if item_index < 0 or item_index >= len(items):
                _reset_ai_batch_tx_flow(context)
                await _reply_and_return_home(update.message, "Позицію не знайдено. Спробуйте ще раз.")
                return True
            items[item_index]["date"] = parsed_date.isoformat()
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return True

    try:
        item_index = int(flow.get("edit_item_index"))
    except (TypeError, ValueError):
        _reset_ai_batch_tx_flow(context)
        await _reply_and_return_home(update.message, "Чернетка пакета пошкоджена. Спробуйте ще раз.")
        return True

    if item_index < 0 or item_index >= len(items):
        _reset_ai_batch_tx_flow(context)
        await _reply_and_return_home(update.message, "Позицію не знайдено. Спробуйте ще раз.")
        return True

    item = items[item_index]
    if await_field == "amount":
        amount = parse_decimal_amount(raw_text, allow_negative=False)
        if amount is None:
            parsed_amount = parse_amount(raw_text)
            if parsed_amount is not None:
                amount = quantize_money(Decimal(str(parsed_amount)))
        if amount is None or amount <= 0:
            await update.message.reply_text(
                "Не бачу суму. Введіть число, напр. <code>500</code> або <code>500.50</code>.",
                reply_markup=_kb_home_only(),
            )
            return True

        default_currency = normalize_currency(str(item.get("currency") or "UAH")) or "UAH"
        parsed_currency, currency_explicit = parse_currency_info(raw_text, default_currency=default_currency)
        if not currency_explicit and flow.get("account_id"):
            try:
                async with _pool(context).acquire() as conn:
                    account = await _get_active_account_by_id(conn, user.id, int(flow.get("account_id") or 0))
            except Exception:
                account = None
            if account is not None:
                parsed_currency = _normalized_account_currency(account, fallback=default_currency)

        comment = raw_text
        match = re.search(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)", comment)
        if match:
            comment = (comment[: match.start()] + comment[match.end() :]).strip(" -–—:\t")
        item["amount"] = amount
        item["currency"] = parsed_currency
        item["currency_explicit"] = currency_explicit
        if comment:
            item["comment"] = comment[:500]
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return True

    if await_field == "comment":
        item["comment"] = None if raw_text in {"-", "—"} else (raw_text[:500] or None)
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return True

    return False


async def _handle_ai_batch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, flow: dict, parts: list[str]) -> None:
    q = update.callback_query
    user = update.effective_user
    if not q or not user:
        return

    items = _ai_batch_flow_items(flow)

    if len(parts) == 3 and parts[2] == "back":
        await q.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return

    if len(parts) == 3 and parts[2] == "cancel":
        await _cancel_ai_batch_draft_flow(context, user.id, flow)
        _reset_ai_batch_tx_flow(context)
        await _reply_home(q.message, "Скасував підготовку операцій.")
        return

    if len(parts) >= 4 and parts[:3] == ["ai", "batch", "currency"]:
        resolution_flow = context.user_data.get("currency_resolution_flow") or {}
        if str(resolution_flow.get("mode") or "") != "ai_batch":
            await q.message.reply_text(AI_BATCH_INACTIVE_TEXT)
            return
        action = parts[3]
        callback_prefix = str(resolution_flow.get("callback_prefix") or "ai:batch:currency")
        if action == "back":
            await _show_currency_resolution_menu(q.message, resolution_flow)
            return
        if action == "create":
            await _start_currency_resolution_account_create(q.message, context, resolution_flow)
            return
        if action in {"auto", "manual"}:
            if not int(resolution_flow.get("target_account_id") or 0):
                async with _pool(context).acquire() as conn:
                    accounts_rows = await _get_accounts(conn, user.id)
                resolution_flow["pending_action"] = action
                context.user_data["currency_resolution_flow"] = resolution_flow
                await _show_currency_resolution_target_picker(q.message, resolution_flow, accounts_rows)
                return
            if action == "auto":
                try:
                    fx_rate = await _resolve_nbu_rate(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    )
                except Exception:
                    await _show_currency_resolution_menu(
                        q.message,
                        resolution_flow,
                        notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                    )
                    return
                await _apply_currency_resolution_conversion(
                    q.message,
                    context,
                    user.id,
                    fx_rate=fx_rate,
                    rate_source="nbu",
                )
                return
            resolution_flow["pending_action"] = "manual"
            resolution_flow["step"] = "await_manual_rate"
            context.user_data["currency_resolution_flow"] = resolution_flow
            await q.message.reply_text(
                _currency_resolution_rate_prompt(
                    str(resolution_flow.get("source_currency") or "UAH"),
                    str(resolution_flow.get("target_currency") or "UAH"),
                ),
                reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
            )
            return
        if action == "pick" and len(parts) == 6 and parts[4] == "acct":
            try:
                account_id = int(parts[5])
            except ValueError:
                return
            async with _pool(context).acquire() as conn:
                account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await _show_currency_resolution_menu(
                    q.message,
                    resolution_flow,
                    notice="<b>Рахунок не знайдено.</b>",
                )
                return
            _set_currency_resolution_target(resolution_flow, account)
            context.user_data["currency_resolution_flow"] = resolution_flow
            if str(resolution_flow.get("pending_action") or "") == "auto":
                try:
                    fx_rate = await _resolve_nbu_rate(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    )
                except Exception:
                    await _show_currency_resolution_menu(
                        q.message,
                        resolution_flow,
                        notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                    )
                    return
                await _apply_currency_resolution_conversion(
                    q.message,
                    context,
                    user.id,
                    fx_rate=fx_rate,
                    rate_source="nbu",
                )
                return
            resolution_flow["pending_action"] = "manual"
            resolution_flow["step"] = "await_manual_rate"
            context.user_data["currency_resolution_flow"] = resolution_flow
            await q.message.reply_text(
                _currency_resolution_rate_prompt(
                    str(resolution_flow.get("source_currency") or "UAH"),
                    str(resolution_flow.get("target_currency") or "UAH"),
                ),
                reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
            )
            return

    if len(parts) == 3 and parts[2] == "account":
        async with _pool(context).acquire() as conn:
            accounts_rows = await _get_accounts(conn, user.id)
        await q.message.reply_text(
            "Оберіть рахунок для всіх позицій:",
            reply_markup=kb_ai_pick_account(accounts_rows, callback_prefix="ai:batch:pick:acct", back_callback="ai:batch:back"),
        )
        return

    if len(parts) == 3 and parts[2] == "date":
        flow["await_field"] = "date"
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await q.message.reply_text(
            "Введіть дату для всіх позицій, напр. <code>2026-05-03</code>, <code>03.05.2026</code>, <code>сьогодні</code> або <code>вчора</code>.",
            reply_markup=_kb_home_only(),
        )
        return

    if len(parts) == 3 and parts[2] == "ok":
        if not items:
            _reset_ai_batch_tx_flow(context)
            await _reply_home(q.message, "У пакеті не залишилось позицій.")
            return

        account_id = flow.get("account_id")
        if not account_id:
            async with _pool(context).acquire() as conn:
                accounts_full = await _get_accounts_full(conn, user.id)
                accounts_rows = await _get_accounts(conn, user.id)
            explicit_currencies = _ai_batch_explicit_currencies(items)
            if len(explicit_currencies) == 1:
                [source_currency] = list(explicit_currencies)
                if not _has_active_account_in_currency(accounts_full, source_currency):
                    resolution_flow = _start_ai_batch_currency_resolution(
                        context,
                        flow,
                        source_currency=source_currency,
                        fallback_target=_single_active_account_target(accounts_full, source_currency),
                    )
                    await _sync_ai_batch_draft_flow(context, user.id, flow)
                    await _show_currency_resolution_menu(q.message, resolution_flow)
                    return
            await q.message.reply_text(
                "Оберіть рахунок для всіх позицій:",
                reply_markup=kb_ai_pick_account(accounts_rows, callback_prefix="ai:batch:pick:acct", back_callback="ai:batch:back"),
            )
            return

        for index, item in enumerate(items):
            if not item.get("category_id"):
                await q.message.reply_text(
                    f"У позиції {index + 1} не вистачає категорії.",
                    reply_markup=kb_ai_batch_item_edit(index, can_remove=len(items) > 1),
                )
                return
            try:
                amount = quantize_money(Decimal(str(item.get("amount") or 0)))
            except (InvalidOperation, ValueError):
                amount = Decimal("0")
            if amount <= 0:
                flow["await_field"] = "amount"
                flow["edit_item_index"] = index
                context.user_data["ai_batch_tx_flow"] = flow
                await q.message.reply_text(
                    f"У позиції {index + 1} не вистачає коректної суми. Введіть її числом.",
                    reply_markup=_kb_home_only(),
                )
                return

        if not consume_financial_confirmation(flow, q.data):
            await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
            return
        try:
            async with _pool(context).acquire() as conn:
                account = await _get_active_account_by_id(conn, user.id, int(account_id))
                if account is None:
                    _reset_ai_batch_tx_flow(context)
                    await _reply_and_return_home(
                        q.message,
                        "Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
                    )
                    return

                currency = _normalized_account_currency(account)
                mismatch_indexes: list[int] = []
                for index, item in enumerate(items, start=1):
                    if not bool(item.get("currency_explicit")):
                        item["currency"] = currency
                        continue
                    item_currency = normalize_currency(str(item.get("currency") or currency)) or currency
                    if item_currency != currency:
                        mismatch_indexes.append(index)
                if mismatch_indexes:
                    explicit_currencies = _ai_batch_explicit_currencies(items)
                    context.user_data["ai_batch_tx_flow"] = flow
                    await _sync_ai_batch_draft_flow(context, user.id, flow)
                    if len(explicit_currencies) == 1:
                        [source_currency] = list(explicit_currencies)
                        resolution_flow = _start_ai_batch_currency_resolution(
                            context,
                            flow,
                            source_currency=source_currency,
                            target_account=account,
                        )
                        await q.message.reply_text(
                            _currency_resolution_text(source_currency),
                            reply_markup=kb_currency_resolution(
                                source_currency,
                                callback_prefix=str(resolution_flow.get("callback_prefix") or "ai:batch:currency"),
                            ),
                        )
                        return
                    await q.message.reply_text(
                        _batch_currency_account_mismatch_text(
                            account_currency=currency,
                            item_indexes=mismatch_indexes,
                        ),
                        reply_markup=kb_ai_pick_account(
                            await _get_accounts(conn, user.id),
                            callback_prefix="ai:batch:pick:acct",
                            back_callback="ai:batch:back",
                        ),
                    )
                    return
                service = TransactionService(conn)
                tx_results = []
                draft_id = int(flow["draft_id"]) if _ai_batch_is_screenshot_flow(flow) else None
                async with AiTransactionDraftService(conn).atomic_confirmation(draft_id, user.id) as guard:
                    for item in items:
                        tx_date = date.fromisoformat(str(item.get("date") or datetime.now().date().isoformat()))
                        amount = quantize_money(Decimal(str(item.get("amount") or 0)))
                        item_currency = normalize_currency(str(item.get("currency") or currency)) or currency
                        category_id = int(item.get("category_id"))
                        category_label = str((flow.get("categories") or {}).get(category_id) or "")
                        comment = (str(item.get("comment") or "")[:500] or None)
                        tx_result = await service.commit_normal_transaction(
                            user.id,
                            transaction_date=tx_date,
                            kind=str(item.get("type") or ""),
                            amount=amount,
                            currency=item_currency,
                            account_id=int(account["id"]),
                            category_id=category_id,
                            category_label=category_label,
                            comment=comment,
                            source=f"ai_{str(item.get('source') or flow.get('origin') or 'batch')}",
                        )
                        if not _is_transaction_commit_success(tx_result.status):
                            raise RuntimeError("batch_commit_failed")
                        tx_results.append(tx_result)
                    guard.succeeded = True

                total_amount = 0.0
                for result in tx_results:
                    if result.amount is not None:
                        total_amount += float(result.amount)
                await log_bot_event(
                    conn,
                    user.id,
                    "transaction_batch_created",
                    source=f"ai_{str(flow.get('origin') or 'batch')}",
                    raw_input=str(flow.get("raw_text") or ""),
                    parsed_result={
                        "count": len(tx_results),
                        "type": _ai_batch_kind(flow),
                        "amount_total": total_amount,
                        "currency": currency,
                    },
                )
                for tx_result in tx_results:
                    await _mark_daily_expense_recorded_from_result(conn, user.id, tx_result)
        except ValueError as exc:
            _reset_ai_batch_tx_flow(context)
            if str(exc) == "draft_not_pending":
                await _reply_and_return_home(
                    q.message,
                    "Цю чернетку вже завершено або скасовано. Повторну операцію не створюю.",
                )
                return
            await _reply_and_return_home(q.message, "⚠️ Не вдалося зберегти список операцій. Спробуйте ще раз.")
            return
        except Exception:
            _reset_ai_batch_tx_flow(context)
            await _reply_and_return_home(q.message, "⚠️ Не вдалося зберегти список операцій. Спробуйте ще раз.")
            return

        saved_text = _ai_batch_saved_text(flow, account_label=str(account["label"]))
        _reset_ai_batch_tx_flow(context)
        await q.message.reply_text(saved_text, reply_markup=kb_home())
        return

    if parts[:4] == ["ai", "batch", "pick", "acct"] and len(parts) == 5:
        try:
            account_id = int(parts[4])
        except ValueError:
            return
        async with _pool(context).acquire() as conn:
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(
                    "Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
                    reply_markup=kb_home(),
                )
                return
            accounts_rows = await _get_accounts(conn, user.id)
        flow["account_id"] = account_id
        account_currency = _normalized_account_currency(account)
        mismatch_indexes: list[int] = []
        for index, item in enumerate(items, start=1):
            if not bool(item.get("currency_explicit")):
                item["currency"] = account_currency
                continue
            item_currency = normalize_currency(str(item.get("currency") or account_currency)) or account_currency
            if item_currency != account_currency:
                mismatch_indexes.append(index)
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, user.id, flow)
        if mismatch_indexes:
            explicit_currencies = _ai_batch_explicit_currencies(items)
            if len(explicit_currencies) == 1:
                [source_currency] = list(explicit_currencies)
                resolution_flow = _start_ai_batch_currency_resolution(
                    context,
                    flow,
                    source_currency=source_currency,
                    target_account=account,
                )
                await q.message.reply_text(
                    _currency_resolution_text(source_currency),
                    reply_markup=kb_currency_resolution(
                        source_currency,
                        callback_prefix=str(resolution_flow.get("callback_prefix") or "ai:batch:currency"),
                    ),
                )
                return
            await q.message.reply_text(
                _batch_currency_account_mismatch_text(
                    account_currency=account_currency,
                    item_indexes=mismatch_indexes,
                ),
                reply_markup=kb_ai_pick_account(
                    accounts_rows,
                    callback_prefix="ai:batch:pick:acct",
                    back_callback="ai:batch:back",
                ),
            )
            return
        await q.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return

    if parts[:4] == ["ai", "batch", "pick", "cat"] and len(parts) == 6:
        try:
            item_index = int(parts[4])
            category_id = int(parts[5])
        except ValueError:
            return
        if item_index < 0 or item_index >= len(items):
            return
        items[item_index]["category_id"] = category_id
        flow.pop("await_field", None)
        flow.pop("edit_item_index", None)
        context.user_data["ai_batch_tx_flow"] = flow
        await _sync_ai_batch_draft_flow(context, user.id, flow)
        await q.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
        return

    if parts[:3] == ["ai", "batch", "item"] and len(parts) == 4:
        try:
            item_index = int(parts[3])
        except ValueError:
            return
        if item_index < 0 or item_index >= len(items):
            return
        await q.message.reply_text(
            _ai_batch_item_text(flow, item_index),
            reply_markup=kb_ai_batch_item_edit(item_index, can_remove=len(items) > 1),
        )
        return

    if parts[:3] == ["ai", "batch", "item"] and len(parts) == 5:
        try:
            item_index = int(parts[3])
        except ValueError:
            return
        if item_index < 0 or item_index >= len(items):
            return
        action = parts[4]
        if action == "amount":
            flow["await_field"] = "amount"
            flow["edit_item_index"] = item_index
            context.user_data["ai_batch_tx_flow"] = flow
            await q.message.reply_text(
                f"Введіть нову суму для позиції {item_index + 1}. Можна разом з коротким описом.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "comment":
            flow["await_field"] = "comment"
            flow["edit_item_index"] = item_index
            context.user_data["ai_batch_tx_flow"] = flow
            await q.message.reply_text(
                f"Введіть новий опис для позиції {item_index + 1}. Надішліть <code>-</code>, якщо хочете очистити опис.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "date":
            flow["await_field"] = "date"
            flow["edit_item_index"] = item_index
            context.user_data["ai_batch_tx_flow"] = flow
            await q.message.reply_text(
                f"Введіть нову дату для позиції {item_index + 1}. Приклади: <code>2026-05-03</code>, <code>03.05.2026</code>, <code>сьогодні</code>.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "category":
            async with _pool(context).acquire() as conn:
                categories_rows = await _get_categories(conn, user.id, _ai_batch_item_kind(items[item_index]) or _ai_batch_kind(flow) or "expense")
            await q.message.reply_text(
                f"Оберіть категорію для позиції {item_index + 1}:",
                reply_markup=kb_ai_pick_category(
                    categories_rows,
                    callback_prefix=f"ai:batch:pick:cat:{item_index}",
                    back_callback=f"ai:batch:item:{item_index}",
                ),
            )
            return
        if action == "remove":
            if len(items) <= 1:
                await q.message.reply_text("Останню позицію не можна видалити. Скасуй пакет повністю або підтвердь його.")
                return
            del items[item_index]
            flow.pop("await_field", None)
            flow.pop("edit_item_index", None)
            context.user_data["ai_batch_tx_flow"] = flow
            await _sync_ai_batch_draft_flow(context, user.id, flow)
            await q.message.reply_text(_ai_batch_tx_card_text(flow), reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(items))))
            return


async def ai_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, *, callback_data: str | None = None) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    raw_data = callback_data if callback_data is not None else q.data
    flow_key = "ai_batch_tx_flow" if str(raw_data or "").startswith("ai:batch:") else "ai_tx_flow"
    data = validated_confirmation_action(context.user_data.get(flow_key) or {}, raw_data)
    if data is None:
        await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
        return

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>AI-обробка операцій доступна лише з повним доступом.</b>",
            ):
                return

    parts = data.split(":")
    if parts[:2] == ["ai", "batch"]:
        batch_flow = context.user_data.get("ai_batch_tx_flow")
        if not batch_flow:
            await q.message.reply_text(AI_BATCH_INACTIVE_TEXT)
            return
        await _handle_ai_batch_callback(update, context, batch_flow, parts)
        return

    flow = context.user_data.get("ai_tx_flow")
    if not flow:
        async with _pool(context).acquire() as conn:
            await _show_access_surface(q.message, conn, user.id, notice="Схоже, чернетка вже не активна.")
        return

    if parts[:2] == ["ai", "currency"] and len(parts) >= 3:
        resolution_flow = context.user_data.get("currency_resolution_flow") or {}
        if str(resolution_flow.get("mode") or "") != "ai_tx":
            await q.message.reply_text(BILLING_TX_INACTIVE_TEXT if _ai_tx_is_billing_flow(flow) else "Ця дія вже неактивна.")
            return
        action = parts[2]
        callback_prefix = str(resolution_flow.get("callback_prefix") or "ai:currency")
        if action == "back":
            await _show_currency_resolution_menu(q.message, resolution_flow)
            return
        if action == "create":
            await _start_currency_resolution_account_create(q.message, context, resolution_flow)
            return
        if action in {"auto", "manual"}:
            if not int(resolution_flow.get("target_account_id") or 0):
                async with _pool(context).acquire() as conn:
                    accounts_rows = await _get_accounts(conn, user.id)
                resolution_flow["pending_action"] = action
                context.user_data["currency_resolution_flow"] = resolution_flow
                await _show_currency_resolution_target_picker(q.message, resolution_flow, accounts_rows)
                return
            if action == "auto":
                try:
                    fx_rate = await _resolve_nbu_rate(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    )
                except Exception:
                    await _show_currency_resolution_menu(
                        q.message,
                        resolution_flow,
                        notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                    )
                    return
                await _apply_currency_resolution_conversion(
                    q.message,
                    context,
                    user.id,
                    fx_rate=fx_rate,
                    rate_source="nbu",
                )
                return
            resolution_flow["pending_action"] = "manual"
            resolution_flow["step"] = "await_manual_rate"
            context.user_data["currency_resolution_flow"] = resolution_flow
            await q.message.reply_text(
                _currency_resolution_rate_prompt(
                    str(resolution_flow.get("source_currency") or "UAH"),
                    str(resolution_flow.get("target_currency") or "UAH"),
                ),
                reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
            )
            return
        if action == "pick" and len(parts) == 5 and parts[3] == "acct":
            try:
                account_id = int(parts[4])
            except ValueError:
                return
            async with _pool(context).acquire() as conn:
                account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await _show_currency_resolution_menu(
                    q.message,
                    resolution_flow,
                    notice="<b>Рахунок не знайдено.</b>",
                )
                return
            _set_currency_resolution_target(resolution_flow, account)
            context.user_data["currency_resolution_flow"] = resolution_flow
            if str(resolution_flow.get("pending_action") or "") == "auto":
                try:
                    fx_rate = await _resolve_nbu_rate(
                        str(resolution_flow.get("source_currency") or "UAH"),
                        str(resolution_flow.get("target_currency") or "UAH"),
                    )
                except Exception:
                    await _show_currency_resolution_menu(
                        q.message,
                        resolution_flow,
                        notice="<b>Не вдалося отримати курс НБУ.</b>\n\nМожете спробувати ручну конвертацію.",
                    )
                    return
                await _apply_currency_resolution_conversion(
                    q.message,
                    context,
                    user.id,
                    fx_rate=fx_rate,
                    rate_source="nbu",
                )
                return
            resolution_flow["pending_action"] = "manual"
            resolution_flow["step"] = "await_manual_rate"
            context.user_data["currency_resolution_flow"] = resolution_flow
            await q.message.reply_text(
                _currency_resolution_rate_prompt(
                    str(resolution_flow.get("source_currency") or "UAH"),
                    str(resolution_flow.get("target_currency") or "UAH"),
                ),
                reply_markup=kb_inline_cancel(f"{callback_prefix}:back"),
            )
            return

    if parts[:2] == ["ai", "tx"] and len(parts) == 3:
        action = parts[2]
        if action == "cancel":
            await _cancel_ai_draft_flow(context, user.id, flow)
            _reset_ai_tx_flow(context)
            async with _pool(context).acquire() as conn:
                await _show_access_surface(q.message, conn, user.id, notice="Скасував.")
            return

        if action == "edit":
            await q.message.reply_text("Що змінити?", reply_markup=_ai_tx_edit_markup(flow))
            return

        if action == "ok":
            tx = _ai_flow_tx(flow)
            tx_type = str(tx.get("type") or "").strip().lower()
            if tx_type not in {"expense", "income"}:
                await q.message.reply_text("Оберіть тип операції:", reply_markup=_ai_tx_pick_type_markup(flow))
                return
            if not tx.get("account_id"):
                async with _pool(context).acquire() as conn:
                    accounts_full = await _get_accounts_full(conn, user.id)
                    accounts_rows = await _get_accounts(conn, user.id)
                tx_currency = normalize_currency(str(tx.get("currency") or "UAH")) or "UAH"
                if bool(tx.get("currency_explicit")) and not _has_active_account_in_currency(accounts_full, tx_currency):
                    resolution_flow = _start_ai_tx_currency_resolution(
                        context,
                        flow,
                        source_currency=tx_currency,
                        fallback_target=_single_active_account_target(accounts_full, tx_currency),
                    )
                    await _sync_ai_draft_flow(context, user.id, flow)
                    await _show_currency_resolution_menu(q.message, resolution_flow)
                    return
                await q.message.reply_text("Оберіть рахунок:", reply_markup=_ai_tx_pick_account_markup(flow, accounts_rows))
                return
            if not tx.get("category_id"):
                async with _pool(context).acquire() as conn:
                    cats = await _get_categories(conn, user.id, tx_type)
                await q.message.reply_text("Оберіть категорію:", reply_markup=_ai_tx_pick_category_markup(flow, cats))
                return
            if not consume_financial_confirmation(flow, raw_data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            async with _pool(context).acquire() as conn:
                await _commit_ai_tx_flow(q.message, context, user.id, flow, conn=conn)
            return

    if parts[:2] == ["ai", "edit"] and len(parts) == 3:
        action = parts[2]
        if action == "back":
            await q.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
            return
        if action == "amount":
            flow["await_field"] = "amount"
            flow["await_amount"] = True
            context.user_data["ai_tx_flow"] = flow
            await q.message.reply_text(
                "Введіть суму (можна з коментарем), напр. <code>500</code> або <code>500 аптека</code>.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "comment":
            flow.pop("await_amount", None)
            flow["await_field"] = "comment"
            context.user_data["ai_tx_flow"] = flow
            await q.message.reply_text(
                "Введіть новий опис одним повідомленням. Надішліть <code>-</code>, якщо хочете очистити опис.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "date":
            flow.pop("await_amount", None)
            flow["await_field"] = "date"
            context.user_data["ai_tx_flow"] = flow
            await q.message.reply_text(
                "Введіть нову дату, напр. <code>2026-05-03</code>, <code>03.05.2026</code>, <code>сьогодні</code> або <code>вчора</code>.",
                reply_markup=_kb_home_only(),
            )
            return
        if action == "type":
            await q.message.reply_text("Оберіть тип операції:", reply_markup=_ai_tx_pick_type_markup(flow))
            return

        async with _pool(context).acquire() as conn:
            accounts_rows = await _get_accounts(conn, user.id)
            expense_rows = await _get_categories(conn, user.id, "expense")
            income_rows = await _get_categories(conn, user.id, "income")

        tx = flow.get("tx") or {}
        tx_type = str(tx.get("type") or "expense")

        if action == "account":
            await q.message.reply_text("Оберіть рахунок:", reply_markup=_ai_tx_pick_account_markup(flow, accounts_rows))
            return
        if action == "category":
            cats = income_rows if tx_type == "income" else expense_rows
            await q.message.reply_text("Оберіть категорію:", reply_markup=_ai_tx_pick_category_markup(flow, cats))
            return

    if parts[:3] == ["ai", "pick", "acct"] and len(parts) == 4:
        try:
            account_id = int(parts[3])
        except ValueError:
            return
        async with _pool(context).acquire() as conn:
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text(
                    "Рахунок не знайдено або він більше не активний.\nОновіть список рахунків і спробуйте ще раз.",
                    reply_markup=kb_home(),
                )
                return
            accounts_rows = await _get_accounts(conn, user.id)
        tx = _ai_flow_tx(flow)
        tx["account_id"] = account_id
        account_currency = _normalized_account_currency(account)
        tx_currency = normalize_currency(str(tx.get("currency") or account_currency)) or account_currency
        if not bool(tx.get("currency_explicit")):
            tx["currency"] = account_currency
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        if bool(tx.get("currency_explicit")) and tx_currency != account_currency:
            resolution_flow = _start_ai_tx_currency_resolution(
                context,
                flow,
                source_currency=tx_currency,
                target_account=account,
            )
            await q.message.reply_text(
                _currency_resolution_text(tx_currency),
                reply_markup=kb_currency_resolution(
                    tx_currency,
                    callback_prefix=str(resolution_flow.get("callback_prefix") or "ai:currency"),
                ),
            )
            return
        await q.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
        return

    if parts[:3] == ["ai", "pick", "cat"] and len(parts) == 4:
        try:
            category_id = int(parts[3])
        except ValueError:
            return
        _ai_flow_tx(flow)["category_id"] = category_id
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        await q.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
        return

    if parts[:3] == ["ai", "pick", "type"] and len(parts) == 4:
        tx_type = str(parts[3] or "").strip().lower()
        if tx_type not in {"expense", "income"}:
            return
        tx = _ai_flow_tx(flow)
        tx["type"] = tx_type
        tx["category_id"] = None
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        async with _pool(context).acquire() as conn:
            cats = await _get_categories(conn, user.id, tx_type)
        await q.message.reply_text("Оберіть категорію для нового типу:", reply_markup=_ai_tx_pick_category_markup(flow, cats))
        return


async def billing_tx_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return

    parts = (q.data or "").split(":")
    if len(parts) < 4 or parts[0] != "btx":
        await q.answer()
        return
    try:
        draft_id = int(parts[1])
    except ValueError:
        await q.answer()
        return

    flow = context.user_data.get("ai_tx_flow")
    if not (_ai_tx_is_billing_flow(flow or {}) and int((flow or {}).get("draft_id") or 0) == draft_id):
        user = update.effective_user
        if not user:
            await q.answer()
            return
        restored_flow, inactive_text = await _restore_billing_tx_flow(context, user.id, draft_id)
        if restored_flow is None:
            await q.answer()
            await q.message.reply_text(inactive_text or BILLING_TX_INACTIVE_TEXT)
            return
        flow = restored_flow
        context.user_data["ai_tx_flow"] = flow

    await ai_callback(update, context, callback_data="ai:" + ":".join(parts[2:]))


async def _handle_ai_tx_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE, flow: dict) -> bool:
    user = update.effective_user
    if not user or not update.message:
        return False

    await_field = str(flow.get("await_field") or ("amount" if flow.get("await_amount") else "")).strip().lower()
    if not await_field:
        return False

    tx = _ai_flow_tx(flow)
    raw_text = (update.message.text or "").strip()

    if await_field == "amount":
        amount = parse_decimal_amount(raw_text, allow_negative=False)
        if amount is None:
            parsed_amount = parse_amount(raw_text)
            if parsed_amount is not None:
                amount = quantize_money(Decimal(str(parsed_amount)))
        if amount is None or amount <= 0:
            await update.message.reply_text(
                "Не бачу суму. Введіть число, напр. <code>500</code> або <code>500.50</code>.",
                reply_markup=_kb_home_only(),
            )
            return True

        comment = raw_text
        if comment:
            match = re.search(r"(?P<amount>-?\d+(?:[\s.,]\d{3})*(?:[.,]\d+)?)", comment)
            if match:
                comment = (comment[: match.start()] + comment[match.end() :]).strip(" -–—:\t")
        default_currency = normalize_currency(str(tx.get("currency") or "UAH")) or "UAH"
        parsed_currency, currency_explicit = parse_currency_info(raw_text, default_currency=default_currency)
        if not currency_explicit and tx.get("account_id"):
            try:
                async with _pool(context).acquire() as conn:
                    account = await _get_active_account_by_id(conn, user.id, int(tx.get("account_id") or 0))
            except Exception:
                account = None
            if account is not None:
                parsed_currency = _normalized_account_currency(account, fallback=default_currency)
        tx["amount"] = amount
        tx["currency"] = parsed_currency
        tx["currency_explicit"] = currency_explicit
        if comment:
            tx["comment"] = comment[:500]
        flow.pop("await_amount", None)
        flow.pop("await_field", None)
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
        return True

    if await_field == "comment":
        tx["comment"] = None if raw_text in {"-", "—"} else (raw_text[:500] or None)
        flow.pop("await_amount", None)
        flow.pop("await_field", None)
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
        return True

    if await_field == "date":
        parsed_date = _parse_date_ddmmyyyy(raw_text)
        if parsed_date is None:
            parsed = parse_message(raw_text, today=datetime.now().date(), default_currency=str(tx.get("currency") or "UAH"))
            parsed_date = parsed.date
        if parsed_date is None:
            await update.message.reply_text(
                "Не зміг розпізнати дату. Приклади: <code>2026-05-03</code>, <code>03.05.2026</code>, <code>сьогодні</code>, <code>вчора</code>.",
                reply_markup=_kb_home_only(),
            )
            return True
        tx["date"] = parsed_date.isoformat()
        flow.pop("await_amount", None)
        flow.pop("await_field", None)
        context.user_data["ai_tx_flow"] = flow
        await _sync_ai_draft_flow(context, user.id, flow)
        await update.message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))
        return True

    if await_field == "credit_limit":
        credit_limit = parse_decimal_amount(raw_text, allow_negative=False)
        if credit_limit is None or credit_limit <= 0:
            await update.message.reply_text(
                "Введіть кредитний ліміт числом, наприклад <code>5000</code> або <code>12500.50</code>.",
                reply_markup=_kb_home_only(),
            )
            return True
        async with _pool(context).acquire() as conn:
            await _commit_ai_tx_flow(
                update.message,
                context,
                user.id,
                flow,
                conn=conn,
                credit_limit_override=credit_limit,
            )
        return True

    return False


async def _debts_report_text(conn: asyncpg.Connection, tg_user_id: int) -> str:
    return await DebtService(conn).build_debt_summary_text(tg_user_id)


def _debt_confirm_text(flow: dict) -> str:
    direction = str(flow.get("direction") or "owed_to_me")
    name = escape_html(str(flow.get("name") or "—"))
    amount = flow.get("amount")
    currency = normalize_currency(str(flow.get("currency") or "UAH"))
    return "\n".join(
        [
            "<b>Підтвердіть борг</b>",
            "",
            f"<b>Тип:</b> {'Мені винні' if direction == 'owed_to_me' else 'Я винен'}",
            f"<b>Хто:</b> {name}" if direction == "owed_to_me" else f"<b>Кому:</b> {name}",
            f"<b>Сума:</b> {format_money(amount, currency)}",
            f"<b>Коментар:</b> <i>{escape_html(str(flow.get('comment') or '—'))}</i>",
        ]
    )


def _debt_repay_kind(direction: str, repay: bool) -> str:
    if direction == "owed_to_me":
        return "lend_repaid" if repay else "lend"
    return "borrow_repaid" if repay else "borrow"


def _debt_direction_from_repay_type(repay_type: str) -> str:
    return "receivable" if repay_type == "in" else "payable"


def _debt_direction_label(direction: str) -> str:
    return "Мені винні" if direction == "receivable" else "Я винен"


def _debt_status_label(status: str | None) -> str:
    return {
        "active": "активний",
        "partially_paid": "частково погашено",
        "closed": "закрито",
        "cancelled": "скасовано",
        "needs_review": "потребує перевірки",
    }.get((status or "").strip().lower(), (status or "active"))


def _parse_optional_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text or text.lower() in {"-", "—", "skip", "пропустити"}:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _debt_accounts_keyboard(accounts: list[asyncpg.Record], back_callback: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account in accounts:
        rows.append(
            [
                InlineKeyboardButton(
                    f"{escape_html(str(account['label']))} ({normalize_currency(str(account['currency'] or 'UAH'))})",
                    callback_data=f"debt:account:{int(account['id'])}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=back_callback)])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="debt:cancel")])
    return InlineKeyboardMarkup(rows)


def _debt_debt_list_keyboard(debts: list[asyncpg.Record], *, prefix: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for debt in debts:
        label = f"{escape_html(str(debt['counterparty_name'] or '—'))} · {format_money(Decimal(str(debt['remaining_amount'] or 0)), str(debt['currency'] or 'UAH'))}"
        rows.append([InlineKeyboardButton(label, callback_data=f"{prefix}:{int(debt['id'])}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="debt:back")])
    return InlineKeyboardMarkup(rows)


def _mixed_payable_debt_list_keyboard(
    owned_debts: list[asyncpg.Record],
    observed_debts: list[asyncpg.Record],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for debt in owned_debts:
        label = f"{escape_html(str(debt['counterparty_name'] or '—'))} · {format_money(Decimal(str(debt['remaining_amount'] or 0)), str(debt['currency'] or 'UAH'))}"
        rows.append([InlineKeyboardButton(label, callback_data=f"debt:view:{int(debt['id'])}")])
    for debt in observed_debts:
        lender_name = _display_user_name(debt.get("lender_first_name"), debt.get("lender_username"))
        label = f"{escape_html(lender_name)} · {format_money(Decimal(str(debt['remaining_amount'] or 0)), str(debt['currency'] or 'UAH'))}"
        rows.append([InlineKeyboardButton(label, callback_data=f"debt:observer:view:{int(debt['id'])}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="debt:back")])
    return InlineKeyboardMarkup(rows)


def _debt_confirm_text(flow: dict) -> str:
    direction = str(flow.get("direction") or "receivable")
    counterparty = escape_html(str(flow.get("counterparty_name") or "—"))
    debt_currency = normalize_currency(str(flow.get("debt_currency") or "UAH"))
    debt_amount = flow.get("debt_amount")
    account_label = escape_html(str(flow.get("account_label") or "—"))
    account_currency = normalize_currency(str(flow.get("account_currency") or debt_currency))
    comment = escape_html(str(flow.get("comment") or "—"))
    due_date = flow.get("due_date")
    due_date_text = due_date.strftime("%d.%m.%Y") if isinstance(due_date, date) else "—"
    mode = str(flow.get("mode") or "create")
    if mode == "repay":
        title = "Повернення боргу"
        amount_label = "Сума повернення"
    else:
        title = "Підтвердіть борг"
        amount_label = "Сума боргу"
    lines = [
        f"<b>{title}</b>",
        "",
        f"<b>Тип:</b> {'Я дав у борг' if direction == 'receivable' else 'Я взяв у борг'}" if mode == "create" else f"<b>Тип:</b> {'Мені повернули' if direction == 'receivable' else 'Я повернув'}",
        f"<b>Кому:</b> {counterparty}" if mode == "create" or direction == "receivable" else f"<b>У кого:</b> {counterparty}",
        f"<b>{amount_label}:</b> {format_money(debt_amount, debt_currency)}",
        f"<b>Рахунок:</b> {account_label} ({account_currency})",
        f"<b>Строк:</b> {due_date_text}",
        f"<b>Коментар:</b> <i>{comment}</i>",
    ]
    if flow.get("fx_mode") == "amount" and flow.get("account_amount") is not None:
        lines.insert(-2, f"<b>Сума списання/зарахування:</b> {format_money(flow['account_amount'], account_currency)}")
    if flow.get("fx_mode") == "rate" and flow.get("exchange_rate") is not None:
        lines.insert(-2, f"<b>Курс:</b> {format_exchange_rate(debt_currency, account_currency, Decimal(str(flow['exchange_rate'])))}")
    lines.append("")
    if mode == "create":
        lines.append(
            f"Після підтвердження з рахунку буде {'списано' if direction == 'receivable' else 'зараховано'} "
            f"{format_money(flow.get('account_amount') or debt_amount, account_currency)}."
        )
    else:
        lines.append(
            f"Після підтвердження на рахунок буде {'зараховано' if direction == 'receivable' else 'списано'} "
            f"{format_money(flow.get('account_amount') or debt_amount, account_currency)}."
        )
    return "\n".join(lines)


async def _show_debt_accounts(
    q_message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    *,
    back_callback: str,
) -> None:
    async with _pool(context).acquire() as conn:
        accounts = await AccountService(conn).get_accounts_full(tg_user_id)
    await q_message.reply_text("Оберіть рахунок:", reply_markup=_debt_accounts_keyboard(accounts, back_callback))


async def _build_payable_debt_list_payload(
    service: DebtService,
    tg_user_id: int,
) -> tuple[str, list[asyncpg.Record], list[asyncpg.Record]]:
    owned_debts = await service.list_active_payable_debts(tg_user_id)
    observed_debts = await service.list_observed_payable_debts(tg_user_id)
    lines = ["<b>Я винен:</b>", ""]
    if not owned_debts and not observed_debts:
        lines.append("<i>Немає активних боргів.</i>")
        return "\n".join(lines), owned_debts, observed_debts

    index = 1
    for debt in owned_debts:
        currency = str(debt["currency"] or "UAH")
        lines += [
            f"{index}. {escape_html(str(debt['counterparty_name'] or '—'))}",
            f"Борг: {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
            f"Повернув: {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
            f"Залишилось: {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
            "",
        ]
        index += 1
    for debt in observed_debts:
        lender_name = _display_user_name(debt.get("lender_first_name"), debt.get("lender_username"))
        currency = str(debt["currency"] or "UAH")
        lines += [
            f"{index}. {escape_html(lender_name)}",
            f"Борг: {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
            f"Повернуто: {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
            f"Залишок: {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
            f"Керує боргом: {escape_html(lender_name)}",
            "",
        ]
        index += 1
    return "\n".join(lines).strip(), owned_debts, observed_debts


async def _show_debt_list(
    q_message,
    context: ContextTypes.DEFAULT_TYPE,
    tg_user_id: int,
    direction: str,
) -> None:
    async with _pool(context).acquire() as conn:
        service = DebtService(conn)
        if direction == "receivable":
            debts = await service.list_active_receivable_debts(tg_user_id)
            text = await service.build_debt_list_text(tg_user_id, direction)
            reply_markup = _debt_debt_list_keyboard(debts, prefix="debt:view")
        else:
            text, owned_debts, observed_debts = await _build_payable_debt_list_payload(service, tg_user_id)
            reply_markup = _mixed_payable_debt_list_keyboard(owned_debts, observed_debts)
    await q_message.reply_text(text, reply_markup=reply_markup)


async def _send_optional_bot_message(bot, chat_id: int, text: str) -> None:
    try:
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
    except Exception:
        logger.exception("Could not send debt notification to chat_id=%s", chat_id)


async def _notify_borrower_about_repayment(bot, owner_name: str, debt: asyncpg.Record) -> None:
    borrower_user_id = debt["borrower_user_id"]
    if borrower_user_id is None or debt["borrower_confirmed_at"] is None:
        return

    currency = str(debt["currency"] or "UAH")
    owner_name_html = escape_html(owner_name)
    if str(debt["status"] or "") == "closed" or Decimal(str(debt["remaining_amount"] or 0)) <= 0:
        text = "\n".join(
            [
                f"{owner_name_html} відмітив(ла), що борг повністю повернуто.",
                "",
                f"Борг: {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
                f"Коментар: {escape_html(_debt_comment_text(debt.get('comment')))}",
                "",
                "Нагадування вимкнені.",
            ]
        )
    else:
        text = "\n".join(
            [
                f"{owner_name_html} відмітив(ла) часткове повернення боргу.",
                "",
                f"Борг: {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
                f"Повернуто: {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
                f"Залишок: {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
                "",
                f"Коментар: {escape_html(_debt_comment_text(debt.get('comment')))}",
                "",
                "Наступне автоматичне нагадування буде тільки про залишок.",
            ]
        )
    await _send_optional_bot_message(bot, int(borrower_user_id), text)


async def _notify_owner_about_invite_resolution(
    bot,
    *,
    owner_user_id: int,
    borrower_name: str,
    confirmed: bool,
) -> None:
    borrower_name_html = escape_html(borrower_name)
    text = (
        f"{borrower_name_html} підтвердив(ла) свій Telegram-акаунт для цього боргу."
        if confirmed
        else f"{borrower_name_html} позначив(ла), що це посилання надійшло помилково."
    )
    await _send_optional_bot_message(bot, owner_user_id, text)


async def debt_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    data = validated_confirmation_action(context.user_data.get("debt_flow") or {}, q.data)
    if data is None:
        await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
        return

    user = update.effective_user
    if not user:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    if q.data.startswith("debt:invite:confirm:") or q.data.startswith("debt:invite:reject:"):
        token = q.data.rsplit(":", 1)[-1]
        async with _pool(context).acquire() as conn:
            service = DebtService(conn)
            if q.data.startswith("debt:invite:confirm:"):
                result = await service.confirm_debt_invite(token, borrower_user_id=user.id)
            else:
                result = await service.reject_debt_invite(token, borrower_user_id=user.id)
            if result.status in {"invite_owner_blocked", "invite_missing", "invite_not_pending", "debt_locked", "invite_conflict", "completed"}:
                access_state = await _get_resolved_access_state(conn, user.id)
                await _clear_pending_start_payload_for_state(conn, user.id, access_state)

        if result.status == "invite_owner_blocked":
            await q.message.reply_text(_debt_invite_borrower_link_text(current_ui_locale()))
            return
        if result.status in {"invite_missing", "invite_not_pending", "debt_locked"}:
            await q.message.reply_text("Це посилання вже неактивне.")
            return
        if result.status == "invite_conflict":
            await q.message.reply_text("Цей борг уже підтверджено для іншого Telegram-акаунта.")
            return
        if result.status != "completed" or result.debt is None:
            await q.message.reply_text("Не вдалося обробити це посилання.")
            return

        borrower_name = _display_user_name(user.first_name, user.username)
        owner_user_id = int(result.debt["tg_user_id"])
        if q.data.startswith("debt:invite:confirm:"):
            await q.message.reply_text(
                "Підтвердження збережено. Бот буде раз на місяць надсилати вам нагадування тільки про залишок боргу."
            )
            await _notify_owner_about_invite_resolution(
                context.application.bot,
                owner_user_id=owner_user_id,
                borrower_name=borrower_name,
                confirmed=True,
            )
            return

        await q.message.reply_text("Позначив(ла) це як помилку. Нагадування для цього боргу не будуть увімкнені.")
        await _notify_owner_about_invite_resolution(
            context.application.bot,
            owner_user_id=owner_user_id,
            borrower_name=borrower_name,
            confirmed=False,
        )
        return

    if q.data in {"debt:back", "debt:cancel"}:
        _reset_debt_flow(context)
        async with _pool(context).acquire() as conn:
            await _show_access_surface(q.message, conn, user.id)
        return

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if not await _user_ready(conn, user.id) and access_scope != "debt_only":
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_allows_debts(access_scope):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Розділ боргів для цього акаунта недоступний.</b>",
                allow_debt_only=True,
            ):
                return
        if q.data.startswith("debts:"):
            _reset_debt_flow(context)
            await q.message.reply_text(
                _ui_text(
                    "Ця картка боргу застаріла. Відкрийте актуальний розділ боргів нижче.",
                    "This debt card is outdated. Open the current debts screen below.",
                    current_ui_locale(),
                ),
                reply_markup=kb_debts_menu(),
            )
            return
        debt_write_prefixes = (
            "debt:add",
            "debt:repay",
            "debt:confirm",
            "debt:edit",
            "debt:delete",
            "debt:close",
            "debt:invite:create",
            "debt:invite:skip",
        )
        if any((q.data or "").startswith(prefix) for prefix in debt_write_prefixes):
            if access_scope not in {"personal_full", "family_full", "debt_only"}:
                if await _show_access_locked(
                    q.message,
                    conn,
                    user.id,
                    notice="<b>Для роботи з боргами потрібен щонайменше доступ до розділу боргів.</b>",
                    allow_debt_only=True,
                ):
                    return
            elif access_scope not in {"personal_full", "family_full"} and q.data.startswith("debt:invite:create"):
                await q.message.reply_text("Створити посилання для нагадувань може тільки власник боргу.")
                return
        service = DebtService(conn)
        user_row = await _get_user(conn, user.id)
        base_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
        owner_name = _display_user_name((user_row or {}).get("first_name") or user.first_name, (user_row or {}).get("username") or user.username)

        if q.data in {"debt:invite:create", "debt:invite:skip"}:
            flow = context.user_data.get("debt_flow") or {}
            if flow.get("mode") != "invite_prompt":
                await q.message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
                return
            debt_id = int(flow.get("debt_id") or 0)
            if not debt_id:
                _reset_debt_flow(context)
                await q.message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
                return
            debt = await service.get_debt(user.id, debt_id)
            if debt is None:
                _reset_debt_flow(context)
                await q.message.reply_text("Борг не знайдено.", reply_markup=kb_debts_menu())
                return
            if q.data == "debt:invite:skip":
                _reset_debt_flow(context)
                await q.message.reply_text(
                    "\n".join(
                        [
                            "Ок, борг залишився без автоматичних нагадувань для боржника.",
                            "",
                            "Ви можете вести його як звичайний борг у розділі «Мені винні».",
                        ]
                    ),
                    reply_markup=kb_debt_invite_skip_actions(debt_id),
                )
                return

            bot_username = await _resolve_bot_username(context)
            if not bot_username:
                await q.message.reply_text("Не вдалося підготувати посилання для бота. Спробуйте трохи пізніше.")
                return
            result = await service.create_debt_invite(
                user.id,
                debt_id,
                token=_generate_debt_invite_token(),
                created_by_user_id=user.id,
            )
            if result.status != "completed" or result.invite is None:
                await q.message.reply_text("Не вдалося створити посилання для цього боргу.", reply_markup=kb_debt_detail(debt_id))
                return
            link = _build_debt_invite_link(bot_username, str(result.invite["token"]))
            share_text = await _build_debt_invite_share_message(conn, owner_name, debt, link)
            _reset_debt_flow(context)
            await q.message.reply_text(
                await _build_debt_invite_owner_message(conn, owner_name, debt, link),
                reply_markup=kb_debt_invite_ready_actions(debt_id, _build_telegram_share_url(share_text)),
            )
            return

        if q.data == "debt:add":
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                "mode": "create",
                "step": DEBT_FLOW_STEP_CHOOSING_DIRECTION,
                "base_currency": base_currency,
            }
            await q.message.reply_text("<b>Що сталося?</b>", reply_markup=kb_debt_direction_choice())
            return

        if q.data.startswith("debt:add:"):
            direction = q.data.rsplit(":", 1)[-1]
            if direction not in {"receivable", "payable"}:
                return
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                "mode": "create",
                "direction": direction,
                "step": DEBT_FLOW_STEP_ENTERING_COUNTERPARTY,
                "base_currency": base_currency,
            }
            prompt = "Кому / у кого?" if direction == "receivable" else "Кому / у кого?"
            await q.message.reply_text(prompt, reply_markup=kb_inline_cancel("debt:cancel"))
            return

        if q.data == "debt:repay":
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                "mode": "repay",
                "step": DEBT_FLOW_STEP_CHOOSING_REPAYMENT_TYPE,
                "base_currency": base_currency,
            }
            await q.message.reply_text("<b>Який це тип повернення?</b>", reply_markup=kb_debt_repay_choice())
            return

        if q.data in {"debt:repay:in", "debt:repay:out"}:
            repay_type = q.data.rsplit(":", 1)[-1]
            direction = _debt_direction_from_repay_type(repay_type)
            debts = (
                await service.list_active_receivable_debts(user.id)
                if direction == "receivable"
                else await service.list_active_payable_debts(user.id)
            )
            if not debts:
                await q.message.reply_text("Активних боргів для цього типу немає.", reply_markup=kb_debts_menu())
                return
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                "mode": "repay",
                "repay_type": repay_type,
                "direction": direction,
                "step": DEBT_FLOW_STEP_CHOOSING_DEBT_FOR_REPAYMENT,
                "base_currency": base_currency,
            }
            await q.message.reply_text(
                "Оберіть борг для погашення:",
                reply_markup=_debt_debt_list_keyboard(debts, prefix="debt:repay"),
            )
            return

        if q.data.startswith("debt:list:"):
            direction = q.data.rsplit(":", 1)[-1]
            if direction not in {"receivable", "payable"}:
                return
            await _show_debt_list(q.message, context, user.id, direction)
            return

        if q.data.startswith("debt:view:"):
            try:
                debt_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            detail = await service.build_debt_detail_text(user.id, debt_id)
            await q.message.reply_text(detail, reply_markup=kb_debt_detail(debt_id))
            return

        if q.data.startswith("debt:observer:view:"):
            try:
                debt_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            detail = await service.build_observed_debt_detail_text(user.id, debt_id)
            await q.message.reply_text(detail, reply_markup=kb_debt_observer_detail(debt_id))
            return

        if q.data.startswith("debt:close:"):
            try:
                debt_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            result = await service.close_debt(user.id, debt_id)
            if result.status != "completed" or result.debt is None:
                await q.message.reply_text("Не вдалося закрити борг.", reply_markup=kb_debts_menu())
                return
            await _notify_borrower_about_repayment(context.application.bot, owner_name, result.debt)
            await q.message.reply_text(await service.build_debt_detail_text(user.id, debt_id), reply_markup=kb_debt_detail(debt_id))
            return

        if q.data == "debt:history":
            await q.message.reply_text(await service.build_debt_history_text(user.id), reply_markup=kb_debts_menu())
            return

        if q.data.startswith("debt:edit:"):
            parts = q.data.split(":")
            if len(parts) == 3:
                try:
                    debt_id = int(parts[2])
                except ValueError:
                    return
                _reset_debt_flow(context)
                context.user_data["debt_flow"] = {
                    "mode": "edit",
                    "debt_id": debt_id,
                    "step": DEBT_FLOW_STEP_EDIT_SELECT,
                }
                await q.message.reply_text("Що змінити?", reply_markup=kb_debt_edit_fields(debt_id))
                return
            if len(parts) == 4:
                try:
                    debt_id = int(parts[2])
                except ValueError:
                    return
                field = parts[3]
                debt = await service.get_debt(user.id, debt_id)
                if debt is None:
                    await q.message.reply_text("Борг не знайдено.")
                    return
                context.user_data["debt_flow"] = {
                    "mode": "edit",
                    "debt_id": debt_id,
                    "field": field,
                    "step": DEBT_FLOW_STEP_EDIT_VALUE,
                }
                prompts = {
                    "counterparty": "Введіть нове ім'я:",
                    "amount": "Введіть нову суму:",
                    "currency": "Введіть нову валюту:",
                    "due_date": "Введіть нову дату або '-' щоб скасувати строк:",
                    "comment": "Введіть новий коментар або '-' щоб очистити:",
                }
                await q.message.reply_text(prompts.get(field, "Введіть нове значення:"), reply_markup=kb_inline_cancel("debt:cancel"))
                return

        if q.data.startswith("debt:delete:"):
            parts = data.split(":")
            try:
                debt_id = int(parts[2])
            except (IndexError, ValueError):
                return
            if len(parts) == 4 and parts[3] == "confirm":
                if not consume_financial_confirmation(context.user_data.get("debt_flow") or {}, q.data):
                    await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                    return
                result = await service.delete_debt(user.id, debt_id)
                if result.status != "completed":
                    await q.message.reply_text("Не вдалося видалити борг.", reply_markup=kb_debts_menu())
                    return
                await q.message.reply_text("Борг видалено.", reply_markup=kb_debts_menu())
                return
            if len(parts) == 3:
                flow = {"mode": "delete", "debt_id": debt_id}
                context.user_data["debt_flow"] = flow
                await q.message.reply_text(
                    "Видалити цей борг?\n\nПов'язані операції також буде видалено, а баланс рахунку буде перераховано.",
                    reply_markup=bind_financial_preview(flow, kb_debt_delete_confirm(debt_id)),
                )
                return

        if q.data.startswith("debt:account:"):
            try:
                account_id = int(q.data.rsplit(":", 1)[-1])
            except ValueError:
                return
            flow = context.user_data.get("debt_flow") or {}
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                await q.message.reply_text("Рахунок не знайдено або він неактивний.")
                return
            flow["account_id"] = account_id
            flow["account_label"] = str(account["label"])
            flow["account_currency"] = normalize_currency(str(account["currency"] or "UAH")) or "UAH"
            if flow.get("mode") == "repay":
                debt = await service.get_debt(user.id, int(flow.get("debt_id") or 0))
                if debt is None:
                    await q.message.reply_text("Борг не знайдено.")
                    return
                flow["debt_currency"] = normalize_currency(str(debt["currency"] or "UAH")) or "UAH"
                flow["debt_amount"] = quantize_money(Decimal(str(flow.get("debt_amount") or debt["remaining_amount"] or 0)))
            context.user_data["debt_flow"] = flow
            if flow.get("account_currency") != flow.get("debt_currency"):
                flow["step"] = DEBT_FLOW_STEP_CHOOSING_FX_MODE
                context.user_data["debt_flow"] = flow
                await q.message.reply_text(
                    "Валюти різні. Оберіть, що ввести:",
                    reply_markup=kb_debt_fx_choice(),
                )
                return
            flow["step"] = DEBT_FLOW_STEP_ENTERING_DUE_DATE if flow.get("mode") == "create" else DEBT_FLOW_STEP_CONFIRMING_REPAYMENT
            context.user_data["debt_flow"] = flow
            if flow.get("mode") == "create":
                await q.message.reply_text("Строк повернення? Введіть дату або натисніть '-', щоб пропустити:", reply_markup=kb_inline_cancel("debt:cancel"))
            else:
                await q.message.reply_text(_debt_confirm_text(flow), reply_markup=bind_financial_preview(flow, kb_debt_confirm()))
            return

        if q.data.startswith("debt:currency:"):
            currency = q.data.rsplit(":", 1)[-1]
            flow = context.user_data.get("debt_flow") or {}
            if not flow:
                await q.message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
                return
            if currency == "OTHER":
                flow["step"] = "entering_currency_text"
                context.user_data["debt_flow"] = flow
                await q.message.reply_text("Введіть валюту текстом (наприклад UAH, USD, EUR):", reply_markup=kb_inline_cancel("debt:cancel"))
                return
            flow["debt_currency"] = normalize_currency(currency) or "UAH"
            flow["step"] = DEBT_FLOW_STEP_CHOOSING_ACCOUNT
            context.user_data["debt_flow"] = flow
            await _show_debt_accounts(q.message, context, user.id, back_callback="debt:cancel")
            return

        if q.data.startswith("debt:overlimit:"):
            parts = q.data.split(":")
            if len(parts) != 4:
                return
            action = parts[3]
            flow = context.user_data.get("debt_flow") or {}
            if action == "cap":
                remaining = quantize_money(Decimal(str(flow.get("remaining_amount") or flow.get("debt_amount") or 0)))
                flow["debt_amount"] = remaining
                flow["step"] = DEBT_FLOW_STEP_CHOOSING_REPAYMENT_ACCOUNT
                context.user_data["debt_flow"] = flow
                await _show_debt_accounts(q.message, context, user.id, back_callback="debt:cancel")
                return
            if action == "change":
                flow["step"] = DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT
                context.user_data["debt_flow"] = flow
                await q.message.reply_text("Скільки повернули?", reply_markup=kb_inline_cancel("debt:cancel"))
                return

        if q.data in {"debt:fx:amount", "debt:fx:rate"}:
            flow = context.user_data.get("debt_flow") or {}
            if not flow:
                await q.message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
                return
            flow["fx_mode"] = "amount" if q.data.endswith("amount") else "rate"
            flow["step"] = DEBT_FLOW_STEP_ENTERING_FX_VALUE
            context.user_data["debt_flow"] = flow
            if flow["fx_mode"] == "amount":
                await q.message.reply_text("Введіть суму списання/зарахування в валюті рахунку:", reply_markup=kb_inline_cancel("debt:cancel"))
            else:
                await q.message.reply_text(build_transfer_rate_prompt(str(flow.get("debt_currency") or "UAH"), str(flow.get("account_currency") or "UAH")), reply_markup=kb_inline_cancel("debt:cancel"))
            return

        if data == "debt:confirm":
            flow = context.user_data.get("debt_flow") or {}
            if not flow or flow.get("mode") not in {"create", "repay"}:
                await q.message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
                return
            debt_amount = flow.get("debt_amount")
            if not isinstance(debt_amount, Decimal):
                await q.message.reply_text("Не вистачає суми.", reply_markup=kb_debts_menu())
                return
            debt_currency = normalize_currency(str(flow.get("debt_currency") or "UAH"))
            account_id = int(flow.get("account_id") or 0)
            if not account_id:
                await q.message.reply_text("Не обрано рахунок.", reply_markup=kb_debts_menu())
                return
            if not consume_financial_confirmation(flow, q.data):
                await q.message.reply_text(FINANCIAL_CONFIRMATION_STALE_TEXT)
                return
            if flow.get("mode") == "create":
                result = await service.create_debt(
                    user.id,
                    counterparty_name=str(flow.get("counterparty_name") or "").strip(),
                    direction=str(flow.get("direction") or "receivable"),
                    debt_amount=debt_amount,
                    debt_currency=debt_currency,
                    account_id=account_id,
                    comment=str(flow.get("comment") or "").strip() or None,
                    due_date=flow.get("due_date"),
                    account_amount=flow.get("account_amount"),
                    exchange_rate=flow.get("exchange_rate"),
                )
            else:
                result = await service.record_repayment(
                    user.id,
                    debt_id=int(flow.get("debt_id") or 0),
                    account_id=account_id,
                    payment_amount=debt_amount,
                    payment_currency=debt_currency,
                    comment=str(flow.get("comment") or "").strip() or None,
                    payment_date=flow.get("payment_date"),
                    account_amount=flow.get("account_amount"),
                    exchange_rate=flow.get("exchange_rate"),
                    allow_partial=False,
                )
            if result.status == "over_limit":
                flow.pop("account_amount", None)
                flow.pop("exchange_rate", None)
                flow.pop("_confirmation_preview", None)
                flow["remaining_amount"] = result.required_amount
                flow["step"] = DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT
                context.user_data["debt_flow"] = flow
                await q.message.reply_text(
                    "Залишок боргу змінився: "
                    f"{format_money(result.required_amount, result.required_currency or debt_currency)}. "
                    "Введіть суму погашення повторно та підтвердіть новий розрахунок.",
                    reply_markup=kb_inline_cancel("debt:cancel"),
                )
                return
            if result.status == "credit_limit_exceeded":
                await q.message.reply_text(
                    "Операція перевищує кредитний ліміт рахунку. Нічого не записано. Зменште суму або оберіть інший рахунок.",
                    reply_markup=kb_debts_menu(),
                )
                return
            if result.status != "completed" or result.debt is None:
                await q.message.reply_text("Не вдалося зберегти борг.", reply_markup=kb_debts_menu())
                return
            created_direction = str(flow.get("direction") or result.debt["direction"] or "")
            if flow.get("mode") == "create" and created_direction == "receivable":
                context.user_data["debt_flow"] = {
                    "mode": "invite_prompt",
                    "debt_id": int(result.debt["id"]),
                    "counterparty_name": str(result.debt["counterparty_name"] or ""),
                }
                await q.message.reply_text(
                    await _build_debt_invite_post_create_text(conn, result.debt),
                    reply_markup=kb_debt_invite_prompt(str(result.debt["counterparty_name"] or "боржника")),
                )
                return

            _reset_debt_flow(context)
            await q.message.reply_text(
                await service.build_debt_detail_text(user.id, int(result.debt["id"])),
                reply_markup=kb_debt_detail(int(result.debt["id"])),
            )
            if flow.get("mode") == "repay":
                await _notify_borrower_about_repayment(context.application.bot, owner_name, result.debt)
            return

        if q.data.startswith("debt:repay:") and q.data.split(":")[-1].isdigit():
            debt_id = int(q.data.split(":")[-1])
            debt = await service.get_debt(user.id, debt_id)
            if debt is None:
                await q.message.reply_text("Борг не знайдено.")
                return
            flow = context.user_data.get("debt_flow") or {}
            flow.update(
                {
                    "mode": "repay",
                    "debt_id": debt_id,
                    "direction": str(debt["direction"] or "receivable"),
                    "counterparty_name": str(debt["counterparty_name"] or ""),
                    "debt_currency": normalize_currency(str(debt["currency"] or "UAH")) or "UAH",
                    "debt_amount": quantize_money(Decimal(str(debt["remaining_amount"] or 0))),
                    "step": DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT,
                }
            )
            context.user_data["debt_flow"] = flow
            await q.message.reply_text("Скільки повернули?", reply_markup=kb_inline_cancel("debt:cancel"))
            return

        if q.data.startswith("debt:delete:") and q.data.endswith(":confirm"):
            return

        await q.message.reply_text("Невідома дія.", reply_markup=kb_debts_menu())


async def debts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if q.data in {"debts:back", "debts:cancel"}:
        _reset_debt_flow(context)
        async with _pool(context).acquire() as conn:
            await _show_access_surface(q.message, conn, user.id)
        return

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if not await _user_ready(conn, user.id) and access_scope != "debt_only":
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_allows_debts(access_scope):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Розділ боргів для цього акаунта недоступний.</b>",
                allow_debt_only=True,
            ):
                return
        db_user = await _get_user(conn, user.id)
        base_currency = (db_user.get("base_currency") if db_user else None) or "UAH"

    if q.data == "debts:history":
        await q.message.reply_text(
            "<b>📄 Історія боргів</b>\n\nОкремий журнал боргів ще в розробці.\nПоки що використовуй поточний список боргів.",
            reply_markup=kb_debts_menu(),
        )
        return

    if q.data == "debts:repay":
        await q.message.reply_text(
            "<b>🤝 Борги</b>\n\nОберіть, що хочете додати:",
            reply_markup=kb_debts_menu(),
        )
        return

    if q.data.startswith("debts:add:"):
        direction = q.data.rsplit(":", 1)[-1]
        _reset_tx_flow(context)
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "repay": False, "currency": base_currency}
        prompt = "<b>➕ Новий борг</b>\n\nХто вам винен?" if direction == "owed_to_me" else "<b>➕ Новий борг</b>\n\nКому ви винні?"
        await q.message.reply_text(prompt, reply_markup=kb_inline_cancel("debts:cancel"))
        return

    if q.data.startswith("debts:repay:"):
        direction = q.data.rsplit(":", 1)[-1]
        _reset_tx_flow(context)
        context.user_data["debt_flow"] = {"step": "name", "direction": direction, "repay": True, "currency": base_currency}
        prompt = "<b>➕ Новий борг</b>\n\nХто вам винен?" if direction == "owed_to_me" else "<b>➕ Новий борг</b>\n\nКому ви винні?"
        await q.message.reply_text(prompt, reply_markup=kb_inline_cancel("debts:cancel"))
        return

    if q.data == "debts:confirm":
        flow = context.user_data.get("debt_flow") or {}
        name = str(flow.get("name") or "").strip()
        amount = flow.get("amount")
        if not name or not isinstance(amount, Decimal):
            _reset_debt_flow(context)
            await q.message.reply_text("<b>❌ Не вдалося виконати дію</b>\n\nСпробуйте ще раз.", reply_markup=kb_debts_menu())
            return
        debt_action = _debt_repay_kind(str(flow.get("direction") or "owed_to_me"), bool(flow.get("repay")))
        async with conn.transaction():
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
                name,
                debt_action,
            )
            await log_bot_event(
                conn,
                user.id,
                "transaction_created",
                source="debt",
                parsed_result={"type": "transfer", "amount": float(amount), "currency": flow.get("currency") or "UAH", "debt_action": debt_action},
            )
            report = await _debts_report_text(conn, user.id)
        _reset_debt_flow(context)
        await q.message.reply_text(
            f"<b>✅ Борг {'збережено' if not bool(flow.get('repay')) else 'погашено'}</b>\n\n"
            f"<b>Хто:</b> {escape_html(name)}\n"
            f"<b>Сума:</b> {format_money(amount, flow.get('currency') or 'UAH')}",
            reply_markup=kb_home(),
        )
        await q.message.reply_text(report, reply_markup=kb_debts_menu())
        return

    if q.data == "debts:edit":
        flow = context.user_data.get("debt_flow") or {}
        if flow.get("step") == "confirm":
            flow["step"] = "amount"
            context.user_data["debt_flow"] = flow
            await q.message.reply_text("Введіть суму ще раз:", reply_markup=kb_inline_cancel("debts:cancel"))
            return
        await q.message.reply_text("Введіть дані ще раз:", reply_markup=kb_inline_cancel("debts:cancel"))
        return


async def _handle_debt_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    flow = context.user_data.get("debt_flow") or {}
    if flow.get("step") == "name":
        name = escape_html(update.message.text.strip()[:80])
        if not name:
            await update.message.reply_text("Імʼя не може бути порожнім. Спробуйте ще раз:")
            return
        flow["name"] = name
        flow["step"] = "amount"
        context.user_data["debt_flow"] = flow
        prompt = "<b>➕ Новий борг</b>\n\nСкільки вам винен <b>{}</b>?".format(name) if flow.get("direction") == "owed_to_me" else "<b>➕ Новий борг</b>\n\nСкільки ви винні <b>{}</b>?".format(name)
        await update.message.reply_text(prompt, reply_markup=kb_inline_cancel("debts:cancel"))
        return

    amount = parse_amount(update.message.text)
    if amount is None or amount <= 0:
        await update.message.reply_text(
            "<b>❌ Некоректна сума</b>\n\nВведіть число.\n\nНаприклад:\n<code>500</code>\n<code>500,50</code>\n<code>500.50</code>",
            reply_markup=kb_inline_cancel("debts:cancel"),
        )
        return

    flow["amount"] = amount
    flow["comment"] = ""
    flow["step"] = "confirm"
    context.user_data["debt_flow"] = flow
    await update.message.reply_text(_debt_confirm_text(flow), reply_markup=bind_financial_preview(flow, kb_debt_confirm()))


async def family_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    if q.data == "family:noop":
        return

    if q.data.startswith("family:invite:accept:"):
        token = q.data.removeprefix("family:invite:accept:")
        async with _pool(context).acquire() as conn:
            result = await FamilyService(conn).accept_family_invite(token, user_id=user.id)
            if result.status in {
                "invite_missing",
                "invite_not_active",
                "invite_expired",
                "invite_used",
                "invite_self_blocked",
                "already_in_family",
                "family_full",
            }:
                access_state = await _get_resolved_access_state(conn, user.id)
                await _clear_pending_start_payload_for_state(conn, user.id, access_state)
        if result.status in {"invite_missing", "invite_not_active"}:
            await q.message.reply_text("Це сімейне запрошення вже неактивне.")
            return
        if result.status == "invite_expired":
            await q.message.reply_text("Термін дії цього запрошення вже минув.")
            return
        if result.status == "invite_used":
            await q.message.reply_text("Це сімейне запрошення вже використане.")
            return
        if result.status == "invite_self_blocked":
            await q.message.reply_text("Ви вже власник цього сімейного бюджету.")
            return
        if result.status == "already_in_family":
            await q.message.reply_text("Ви вже перебуваєте в іншій родині.")
            return
        if result.status == "family_full":
            await q.message.reply_text("У цій родині вже немає вільних місць.")
            return
        if result.status != "completed" or result.family is None:
            await q.message.reply_text("Не вдалося приєднатися до родини.")
            return

        owner_user_id = int(result.family["owner_user_id"] or 0)
        member_name = _display_user_name(user.first_name, user.username)
        if owner_user_id:
            await _send_optional_bot_message(context.application.bot, owner_user_id, f"{member_name} приєднався(лась) до родини.")
        async with _pool(context).acquire() as conn:
            await _sync_access_scope_state(conn, user.id, access_scope="family_full", access_source="family", clear_pending_start_payload=True)
            await _show_access_surface(
                q.message,
                conn,
                user.id,
                notice=f"✅ Ви приєдналися до родини {escape_html(str(result.family['name'] or 'Сімейний бюджет'))}.",
            )
        return

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if not await _user_ready(conn, user.id) and access_scope != "debt_only":
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(access_scope):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Сімейний простір доступний лише з повним доступом або через активне сімейне запрошення.</b>",
            ):
                return
        service = FamilyService(conn)

        if q.data == "family:start":
            await _show_family_menu(q.message, context, user.id, conn)
            return

        if q.data == "family:create":
            family_name = f"Сімейний бюджет {_display_user_name(user.first_name, user.username, fallback='')}".strip()
            result = await service.create_family(user.id, name=family_name)
            if result.status == "already_in_family":
                await q.message.reply_text("Ви вже перебуваєте в родині.")
                return
            await _show_family_menu(q.message, context, user.id, conn)
            return

        if q.data == "family:members":
            await _show_family_members_menu(q.message, user.id, conn)
            return

        if q.data == "family:invite:create":
            bot_username = await _resolve_bot_username(context)
            if not bot_username:
                await q.message.reply_text("Не вдалося підготувати посилання. Спробуйте трохи пізніше.")
                return
            token = generate_family_invite_token()
            result = await service.create_family_invite(user.id, token=token)
            if result.status == "family_full":
                await q.message.reply_text("У родині вже максимум учасників.")
                return
            if result.status in {"not_in_family", "not_owner"}:
                await q.message.reply_text("Тільки власник сімейного бюджету може створювати запрошення.")
                return
            link = _build_family_invite_link(bot_username, token)
            await q.message.reply_text(
                "\n".join(
                    [
                        "Ваше посилання-запрошення:",
                        "",
                        link,
                        "",
                        "Посилання діє 7 днів і може бути використане 1 раз.",
                    ]
                ),
                reply_markup=kb_family_owner_menu(has_active_invite=True, active_members_count=int(result.family["active_members_count"] or 0) if result.family else 1),
            )
            return

        if q.data == "family:invite:revoke":
            invites = await service.list_active_invites(user.id)
            if not invites:
                await q.message.reply_text("Активних запрошень зараз немає.")
                return
            await service.revoke_family_invite(user.id, int(invites[0]["id"]))
            await q.message.reply_text("Запрошення відкликано.")
            await _show_family_menu(q.message, context, user.id, conn)
            return

        if q.data == "family:leave:confirm":
            await q.message.reply_text("Після виходу ви втратите доступ до сімейного бюджету.", reply_markup=kb_family_confirm_leave())
            return

        if q.data == "family:leave":
            result = await service.leave_family(user.id)
            if result.status == "owner_blocked":
                await q.message.reply_text("Власник сімейного бюджету не може покинути його в цій версії.")
                return
            if result.status != "completed" or result.family is None:
                await q.message.reply_text("Не вдалося покинути родину.")
                return
            owner_user_id = int(result.family["owner_user_id"] or 0)
            member_name = _display_user_name(user.first_name, user.username)
            if owner_user_id:
                await _send_optional_bot_message(context.application.bot, owner_user_id, f"{member_name} покинув(ла) родину.")
            access_state = await _get_resolved_access_state(conn, user.id)
            next_scope = str(access_state.get("access_scope") or "paywall")
            if next_scope == "family_full":
                next_scope = "paywall"
            await _sync_access_scope_state(conn, user.id, access_scope=next_scope, access_source=str(access_state.get("access_source") or "billing"))
            await _show_access_surface(q.message, conn, user.id, notice="Ви покинули родину.")
            return

        if q.data.startswith("family:member:remove:confirm:"):
            member_user_id = int(q.data.rsplit(":", 1)[-1])
            result = await service.remove_family_member(user.id, member_user_id)
            if result.status == "member_owner_blocked":
                await q.message.reply_text("Власника сімейного бюджету не можна видалити.")
                return
            if result.status != "completed":
                await q.message.reply_text("Не вдалося видалити учасника.")
                return
            removed_name = None
            for member in await service.list_family_members(user.id):
                if int(member["user_id"]) == member_user_id:
                    removed_name = _display_user_name(member.get("first_name"), member.get("username"))
                    break
            await _send_optional_bot_message(context.application.bot, member_user_id, "Owner видалив вас із родини.")
            await q.message.reply_text(f"Учасника {escape_html(removed_name or str(member_user_id))} видалено.")
            await _show_family_members_menu(q.message, user.id, conn)
            return

        if q.data.startswith("family:member:remove:"):
            member_user_id = int(q.data.rsplit(":", 1)[-1])
            await q.message.reply_text("Видалити цього учасника з родини?", reply_markup=kb_family_confirm_remove(member_user_id))
            return

        await _show_family_menu(q.message, context, user.id, conn)
        return


def _resolve_report_period_range(
    key: str,
    *,
    today: date,
    user_start: date | None,
    export_style: bool,
) -> tuple[date, date, str]:
    month_start = date(today.year, today.month, 1)
    next_month_start = date(today.year + (today.month // 12), (today.month % 12) + 1, 1)
    end_date = today + timedelta(days=1)
    all_time_start = user_start or date(2000, 1, 1)

    if key == "today":
        return today, end_date, "Сьогодні" if export_style else "сьогодні"
    if key == "7d":
        return today - timedelta(days=6), end_date, "7 днів" if export_style else "останні 7 днів"
    if key == "30d":
        return today - timedelta(days=29), end_date, "Останні 30 днів" if export_style else "останні 30 днів"
    if key == "3m":
        return today - timedelta(days=90), end_date, "3 місяці" if export_style else "останні 3 місяці"
    if key == "6m":
        return today - timedelta(days=180), end_date, "6 місяців" if export_style else "останні 6 місяців"
    if key == "year":
        return date(today.year, 1, 1), end_date, "Рік" if export_style else "цей рік"
    if key == "12m":
        return today - timedelta(days=364), end_date, "12 місяців" if export_style else "останні 12 місяців"
    if key == "all":
        return all_time_start, end_date, "Весь час" if export_style else "весь час"
    return month_start, next_month_start, "Місяць" if export_style else "цей місяць"


async def reports_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        if not await _user_ready(conn, user.id):
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Звіти доступні лише з повним доступом.</b>",
            ):
                return
        if q.data == "reports:start" or q.data == "reports:back":
            await q.message.reply_text(
                "<b>📊 Звіти</b>\nОберіть період:",
                reply_markup=_with_miniapp_launch(kb_reports_menu()),
            )
            return

        key = q.data.rsplit(":", 1)[-1]
        member_filter_user_id: int | None = None
        member_filter_label: str | None = None
        if q.data.startswith("reports:filter:"):
            parts = q.data.split(":")
            if len(parts) < 4:
                await q.message.reply_text(
                    "Не вдалося застосувати фільтр.",
                    reply_markup=_with_miniapp_launch(kb_reports_menu()),
                )
                return
            key = parts[2]
            filter_kind = parts[3]
            if filter_kind == "mine":
                member_filter_user_id = user.id
                member_filter_label = "Тільки мої операції"
            elif filter_kind == "all":
                member_filter_label = "Всі учасники"
            elif filter_kind == "user" and len(parts) >= 5:
                member_filter_user_id = int(parts[4])
            else:
                await q.message.reply_text(
                    "Не вдалося застосувати фільтр.",
                    reply_markup=_with_miniapp_launch(kb_reports_menu()),
                )
                return
        scope = await _get_finance_scope(conn, user.id)
        if scope.is_family and not q.data.startswith("reports:filter:"):
            members = await FamilyService(conn).list_family_members(user.id)
            filter_rows = [
                [InlineKeyboardButton("Всі учасники", callback_data=f"reports:filter:{key}:all")],
                [InlineKeyboardButton("Тільки мої операції", callback_data=f"reports:filter:{key}:mine")],
            ]
            for member in members:
                member_user_id = int(member["user_id"])
                label = _display_user_name(member.get("first_name"), member.get("username"))
                filter_rows.append([InlineKeyboardButton(label, callback_data=f"reports:filter:{key}:user:{member_user_id}")])
            filter_rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="reports:start")])
            await q.message.reply_text(
                "<b>Фільтр звіту</b>\nОберіть, чиї операції показати:",
                reply_markup=_with_miniapp_launch(InlineKeyboardMarkup(filter_rows), prepend=False),
            )
            return
        if scope.is_family and member_filter_user_id is not None and member_filter_label is None:
            members = await FamilyService(conn).list_family_members(user.id)
            selected = next((member for member in members if int(member["user_id"]) == int(member_filter_user_id)), None)
            member_filter_label = _display_user_name(selected.get("first_name"), selected.get("username")) if selected else "Окремий учасник"
        db_user = await _get_user(conn, user.id)
        user_start = db_user.get("start_date") if db_user else None
        start_date, end_date, title = _resolve_report_period_range(key, today=datetime.now().date(), user_start=user_start, export_style=False)
        effective_start = max(start_date, user_start) if user_start else start_date
        report_text = await ReportService(conn).build_normal_report_text(
            user.id,
            effective_start,
            end_date,
            title,
            member_filter_user_id=member_filter_user_id,
            member_filter_label=member_filter_label,
        )

    if report_text is None:
        await q.message.reply_text(
            f"<b>Звіт за {escape_html(title)}</b>\n\nЗа цей період операцій ще немає.",
            reply_markup=_with_miniapp_launch(kb_reports_menu()),
        )
        return

    chunks = split_report_text(report_text)
    for index, chunk in enumerate(chunks):
        await q.message.reply_text(
            chunk, parse_mode="HTML",
            reply_markup=_with_miniapp_launch(kb_reports_menu()) if index == len(chunks) - 1 else None,
        )


async def export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user = update.effective_user
    if not user:
        return

    async with _pool(context).acquire() as conn:
        access_state = await _get_resolved_access_state(conn, user.id)
        if not await _user_ready(conn, user.id):
            await q.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        if not _access_scope_has_full_home(str(access_state.get("access_scope") or "paywall")):
            if await _show_access_locked(
                q.message,
                conn,
                user.id,
                notice="<b>Експорт Excel доступний лише з повним доступом.</b>",
            ):
                return
        if q.data in {"export:start", "export:back"}:
            await q.message.reply_text(
                "<b>📤 Експорт Excel</b>\n\nОберіть період, за який потрібно сформувати файл.",
                reply_markup=_with_miniapp_launch(kb_export_menu()),
            )
            return

        key = q.data.rsplit(":", 1)[-1]
        db_user = await _get_user(conn, user.id)
        user_start = db_user.get("start_date") if db_user else None
        start_date, end_date, title = _resolve_report_period_range(key, today=datetime.now().date(), user_start=user_start, export_style=True)
        report_result = await ReportService(conn).build_export_xlsx(user.id, start_date, end_date, title)

    if report_result is None:
        await q.message.reply_text(
            "<b>Експорт Excel</b>\n\nЗа цей період немає операцій для експорту.",
            reply_markup=_with_miniapp_launch(kb_export_menu()),
        )
        return

    await q.message.reply_document(
        document=report_result.content,
        filename=f"my-cash-flow-{key}.xlsx",
        caption=(
            "<b>Excel-файл готовий</b>\n\n"
            f"Період: <b>{escape_html(title)}</b>\n"
            f"Операцій: <b>{report_result.row_count}</b>"
        ),
    )
    await q.message.reply_text(
        report_result.summary_message,
        reply_markup=_with_miniapp_launch(InlineKeyboardMarkup(
            [[InlineKeyboardButton("📤 Експорт ще раз", callback_data="export:start")], [InlineKeyboardButton("🏠 Головне меню", callback_data="menu:main")]]
        ), prepend=False),
    )


def _transfer_confirm_text(flow: dict) -> str:
    source_label = str(flow.get("source_label") or "—")
    target_label = str(flow.get("target_label") or "—")
    source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
    target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
    source_amount = flow.get("source_amount")
    target_amount = flow.get("target_amount")
    preview_source_balance = flow.get("preview_source_balance")
    preview_target_balance = flow.get("preview_target_balance")
    rate_text = str(flow.get("rate_text") or "").strip()

    lines = [
        "<b>Підтвердіть переказ</b>",
        "",
        f"<b>З рахунку:</b> {escape_html(source_label)}",
        f"<b>На рахунок:</b> {escape_html(target_label)}",
        "",
        f"<b>Списати:</b> {format_money(source_amount, source_currency)}" if source_amount is not None else f"<b>Списати:</b> — {escape_html(source_currency)}",
    ]
    if source_currency != target_currency and rate_text:
        lines.append(f"<b>Курс:</b> {rate_text}")
    lines.extend(
        [
            f"<b>Зарахувати:</b> {format_money(target_amount, target_currency)}" if target_amount is not None else f"<b>Зарахувати:</b> — {escape_html(target_currency)}",
            "",
            "<b>Баланси після переказу:</b>",
            f"{escape_html(source_label)} — <b>{format_money(preview_source_balance, source_currency)}</b>",
            f"{escape_html(target_label)} — <b>{format_money(preview_target_balance, target_currency)}</b>",
        ]
    )
    return "\n".join(lines)


def _tx_confirm_text(flow: dict) -> str:
    kind = str(flow.get("kind") or "expense")
    account_label = escape_html(str(flow.get("account_label") or "—"))
    category_label = escape_html(str(flow.get("category_label") or "—"))
    amount = flow.get("amount")
    currency = normalize_currency(str(flow.get("currency") or "UAH"))
    comment = escape_html(str(flow.get("comment") or "").strip())
    title = "Підтвердіть витрату" if kind == "expense" else "Підтвердіть дохід"
    lines = [
        f"<b>{title}</b>",
        "",
        f"<b>Сума:</b> {format_money(amount, currency)}",
        f"<b>Рахунок:</b> {account_label}",
        f"<b>Категорія:</b> {category_label}",
        f"<b>Коментар:</b> {f'<i>{comment}</i>' if comment else '<i>—</i>'}",
    ]
    return "\n".join(lines)


def _tx_amount_prompt_text(kind: str, account_label: str, category_label: str) -> str:
    title = "➖ Нова витрата" if kind == "expense" else "➕ Новий дохід"
    examples = (
        "<code>500 зарплата</code>\n<code>500 фріланс</code>\n<code>500 повернення боргу</code>"
        if kind == "income"
        else "<code>500 продукти</code>\n<code>500 кафе</code>\n<code>500 таксі</code>"
    )
    return "\n".join(
        [
            f"<b>{title}</b>",
            "",
            "Крок <b>3 з 3</b>: введіть суму.",
            "",
            f"<b>Рахунок:</b> {escape_html(str(account_label or '—'))}",
            f"<b>Категорія:</b> {escape_html(str(category_label or '—'))}",
            "",
            "Приклади:",
            "<code>500</code>",
            examples,
            "",
            "Можна також надіслати коротке голосове.",
        ]
    )


async def _handle_transfer_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    flow = context.user_data.get("transfer_flow") or {}
    step = flow.get("step")
    if step not in {"enter_source_amount", "enter_exchange_rate", "await_credit_limit"}:
        return

    text = update.message.text.strip()
    if step == "await_credit_limit":
        credit_limit = parse_decimal_amount(text, allow_negative=False)
        if credit_limit is None or credit_limit <= 0:
            await update.message.reply_text(
                "Введіть кредитний ліміт числом, наприклад <code>5000</code> або <code>12500.50</code>.",
                reply_markup=kb_inline_cancel("transfer:cancel"),
            )
            return
        async with _pool(context).acquire() as conn:
            await _complete_transfer_flow(
                update.message,
                context,
                user.id,
                flow,
                conn=conn,
                credit_limit_override=credit_limit,
            )
        return

    if step == "enter_source_amount":
        amount = parse_decimal_amount(text, allow_negative=False)
        if amount is None or amount <= 0:
            await update.message.reply_text(
                "<b>❌ Некоректна сума</b>\n\nВведіть число.\n\nНаприклад:\n<code>1000</code>\n<code>1000.50</code>\n<code>1000,50</code>",
                reply_markup=kb_inline_cancel("transfer:cancel"),
            )
            return

        source_balance = _transfer_flow_source_balance(flow)
        source_currency = normalize_currency(str(flow.get("source_currency") or "UAH"))
        target_currency = normalize_currency(str(flow.get("target_currency") or source_currency))
        flow["source_amount"] = amount
        if source_currency == target_currency:
            flow.pop("fx_rate", None)
            flow.pop("rate_source", None)
            flow.pop("target_amount", None)
            flow.pop("rate_text", None)
            flow.pop("preview_source_balance", None)
            flow.pop("preview_target_balance", None)
            flow = _prepare_transfer_preview(flow)
            context.user_data["transfer_flow"] = flow
            await update.message.reply_text(
                _transfer_confirm_text(flow),
                reply_markup=_kb_transfer_confirm(allow_rate_edit=False, flow=flow),
            )
            return

        if flow.get("fx_rate") is None:
            flow["step"] = "enter_exchange_rate"
            context.user_data["transfer_flow"] = flow
            await _show_transfer_rate_step(
                update.message,
                flow,
                notice="<b>вќЊ РџРѕС‚СЂС–Р±РµРЅ РєСѓСЂСЃ РѕР±РјС–РЅСѓ</b>\n\nР’РІРµРґС–С‚СЊ РєСѓСЂСЃ РІСЂСѓС‡РЅСѓ, С‰РѕР± РїСЂРѕРґРѕРІР¶РёС‚Рё РїРµСЂРµРєР°Р·.",
            )
            return
            await _show_transfer_rate_choice_step(
                update.message,
                flow,
                notice="<b>❌ Не вдалося отримати курс</b>\n\nОберіть курс бота або введіть свій вручну.",
            )
            return

        flow.pop("target_amount", None)
        flow.pop("rate_text", None)
        flow.pop("preview_source_balance", None)
        flow.pop("preview_target_balance", None)
        flow["step"] = "confirm_transfer"
        context.user_data["transfer_flow"] = flow
        flow = _prepare_transfer_preview(flow)
        context.user_data["transfer_flow"] = flow
        await update.message.reply_text(
            _transfer_confirm_text(flow),
            reply_markup=_kb_transfer_confirm(allow_rate_edit=True, flow=flow),
        )
        return

    rate = parse_decimal_rate(text)
    if rate is None:
        await update.message.reply_text(
            "<b>❌ Не вдалося отримати курс</b>\n\nМожете ввести курс вручну.\n\nНаприклад:\n<code>40</code>\n<code>39.85</code>",
            reply_markup=kb_inline_cancel("transfer:cancel"),
        )
        return
    flow["fx_rate"] = rate
    flow["rate_source"] = "manual"
    flow["step"] = "enter_source_amount"
    context.user_data["transfer_flow"] = flow
    await _show_transfer_amount_step(update.message, context, flow)


async def _handle_saving_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return

    saving_settings_flow = context.user_data.get("saving_settings_flow") or {}
    if saving_settings_flow.get("step") == "await_default_percent":
        percent = parse_decimal_amount(update.message.text, allow_negative=False)
        if percent is None or percent <= 0 or percent > Decimal("100"):
            await update.message.reply_text("Введіть відсоток числом від 0 до 100.", reply_markup=kb_inline_cancel("saving:settings:open"))
            return
        async with _pool(context).acquire() as conn:
            await SavingsService(conn).update_settings(update.effective_user.id, default_percent=percent, ask_after_income=True, enabled=True)
            _reset_saving_settings_flow(context)
            await _show_saving_settings(update.message, context, update.effective_user.id, conn=conn, notice="Відсоток оновлено.")
        return

    if saving_settings_flow.get("step") == "await_min_income_amount":
        amount = parse_decimal_amount(update.message.text, allow_negative=False)
        if amount is None:
            await update.message.reply_text("Введіть суму числом або 0, щоб прибрати обмеження.", reply_markup=kb_inline_cancel("saving:settings:open"))
            return
        async with _pool(context).acquire() as conn:
            await SavingsService(conn).update_settings(
                update.effective_user.id,
                min_income_amount=None if amount <= 0 else amount,
                ask_after_income=True,
                enabled=True,
            )
            _reset_saving_settings_flow(context)
            await _show_saving_settings(update.message, context, update.effective_user.id, conn=conn, notice="Мінімальну суму оновлено.")
        return

    flow = context.user_data.get("saving_flow") or {}
    step = str(flow.get("step") or "")
    if step not in {"await_amount", "await_fx_rate", "await_custom_reminder", "await_name", "await_goal_amount", "await_goal_date"}:
        return

    text = update.message.text.strip()
    if step == "await_name":
        new_name = normalize_account_name(text)
        if len(new_name) < 2:
            await update.message.reply_text("Назва має містити мінімум 2 символи.", reply_markup=kb_inline_cancel("saving:cancel"))
            return
        async with _pool(context).acquire() as conn:
            account_id = int(flow.get("account_id") or 0)
            account = await _get_active_account_by_id(conn, user.id, account_id)
            if account is None:
                _reset_saving_flow(context)
                await update.message.reply_text("Накопичення не знайдено.", reply_markup=kb_home())
                return
            accounts = await _get_accounts_full(conn, user.id)
            other_names = [str(row["label"]) for row in accounts if int(row["id"]) != account_id]
            if _onb_has_name_conflict(other_names, new_name):
                await update.message.reply_text("Накопичення з такою назвою вже існує. Введіть іншу назву.", reply_markup=kb_inline_cancel("saving:cancel"))
                return
            await AccountService(conn).rename_account(user.id, account_id, new_name)
            _reset_saving_flow(context)
            await _show_savings_detail(update.message, context, user.id, account_id, conn=conn, notice="Назву накопичення оновлено.")
        return

    if step == "await_goal_amount":
        goal_amount = parse_decimal_amount(text, allow_negative=False)
        if goal_amount is None:
            await update.message.reply_text("Введіть цільову суму числом або 0, якщо хочете прибрати ціль.", reply_markup=kb_inline_cancel("saving:cancel"))
            return
        if goal_amount <= 0:
            async with _pool(context).acquire() as conn:
                account_id = int(flow.get("account_id") or 0)
                await AccountService(conn).update_goal(user.id, account_id, goal_amount=None, goal_date=None)
                _reset_saving_flow(context)
                await _show_savings_detail(update.message, context, user.id, account_id, conn=conn, notice="Ціль прибрано.")
            return
        flow["goal_amount"] = goal_amount
        flow["step"] = "await_goal_date"
        context.user_data["saving_flow"] = flow
        await update.message.reply_text("Введіть дату цілі у форматі ДД.ММ.РРРР або 0, якщо дата не потрібна.", reply_markup=kb_inline_cancel("saving:cancel"))
        return

    if step == "await_goal_date":
        goal_date = None if text == "0" else _parse_date_ddmmyyyy(text)
        if text != "0" and goal_date is None:
            await update.message.reply_text("Не вдалося розпізнати дату. Використайте ДД.ММ.РРРР або 0.", reply_markup=kb_inline_cancel("saving:cancel"))
            return
        async with _pool(context).acquire() as conn:
            account_id = int(flow.get("account_id") or 0)
            await AccountService(conn).update_goal(user.id, account_id, goal_amount=flow.get("goal_amount"), goal_date=goal_date)
            _reset_saving_flow(context)
            await _show_savings_detail(update.message, context, user.id, account_id, conn=conn, notice="Ціль оновлено.")
        return

    if step == "await_amount":
        amount = parse_decimal_amount(text, allow_negative=False)
        if amount is None or amount <= 0:
            await update.message.reply_text("Введіть суму числом, наприклад 1000 або 1000.50.", reply_markup=kb_inline_cancel("saving:cancel"))
            return
        if flow.get("mode") == "change_task_amount":
            async with _pool(context).acquire() as conn:
                updated_task = await SavingsService(conn).update_task_amount(user.id, int(flow.get("pending_task_id") or 0), amount)
            _reset_saving_flow(context)
            if updated_task is None:
                await update.message.reply_text("План уже неактивний.", reply_markup=kb_home())
                return
            await update.message.reply_text(
                await _saving_plan_text(
                    conn,
                    amount=Decimal(str(updated_task["amount"] or 0)),
                    currency=normalize_currency(str(updated_task["currency"] or "UAH")),
                    source_label=str(updated_task["source_label"]),
                    target_label=str(updated_task["target_label"]),
                ),
                reply_markup=kb_saving_plan_actions(int(updated_task["id"])),
            )
            return
        flow["amount"] = amount
        flow["currency"] = normalize_currency(str(flow.get("source_currency") or "UAH"))
        flow.pop("step", None)
        context.user_data["saving_flow"] = flow
        if flow.get("mode") == "create_plan":
            async with _pool(context).acquire() as conn:
                targets = _saving_target_options(await SavingsService(conn).get_active_target_accounts(user.id))
                default_target_id = _saving_default_target_id(flow, targets)
                if default_target_id is not None:
                    target_account = await _get_active_account_by_id(conn, user.id, default_target_id)
                    if target_account is not None:
                        await _create_pending_saving_plan(update.message, context, user.id, flow, target_account, conn=conn)
                        return
        if flow.get("mode") == "post_income_prompt" and not flow.get("target_account_id"):
            async with _pool(context).acquire() as conn:
                targets = _saving_target_options(await SavingsService(conn).get_active_target_accounts(user.id))
                default_target_id = _saving_default_target_id(flow, targets)
                if default_target_id is not None:
                    target_account = await _get_active_account_by_id(conn, user.id, default_target_id)
                    if target_account is not None:
                        flow.update(_saving_target_payload(target_account))
                        if flow["target_currency"] != normalize_currency(str(flow.get("source_currency") or "UAH")):
                            flow["step"] = "await_fx_rate"
                            context.user_data["saving_flow"] = flow
                            await _show_saving_rate_step(update.message, flow)
                            return
                        await _show_saving_transfer_confirmation(update.message, context, flow, conn=conn)
                        return
                await _show_manual_savings_target_picker(update.message, context, user.id, flow, conn=conn)
                return
        if flow.get("mode") in {"manual_topup", "post_income_prompt", "manual_withdraw"} and flow.get("source_account_id") and flow.get("target_account_id"):
            if normalize_currency(str(flow.get("source_currency") or "UAH")) != normalize_currency(str(flow.get("target_currency") or "UAH")):
                flow["step"] = "await_fx_rate"
                context.user_data["saving_flow"] = flow
                await _show_saving_rate_step(update.message, flow)
                return
            await _show_saving_transfer_confirmation(update.message, context, flow)
            return
        if flow.get("mode") == "manual_topup":
            await _show_topup_source_picker(update.message, context, user.id, flow)
            return
        if flow.get("mode") == "manual_withdraw":
            await _show_withdraw_target_picker(update.message, context, user.id, flow)
            return
        await _show_saving_target_picker(update.message, context, user.id, flow)
        return

    if step == "await_fx_rate":
        rate = parse_decimal_rate(text)
        if rate is None:
            await update.message.reply_text("Не вдалося розпізнати курс. Наприклад: 40 або 39.85", reply_markup=kb_inline_cancel("saving:cancel"))
            return
        flow["fx_rate"] = rate
        flow["rate_source"] = "manual"
        flow["step"] = "confirm"
        context.user_data["saving_flow"] = flow
        if flow.get("mode") in {"manual_topup", "manual_withdraw", "post_income_prompt"}:
            await _show_saving_transfer_confirmation(update.message, context, flow)
            return
        await update.message.reply_text(
            _saving_confirm_text(
                amount=Decimal(str(flow.get("amount") or 0)),
                source_label=str(flow.get("source_label") or "—"),
                target_label=str(flow.get("target_label") or "—"),
                source_currency=normalize_currency(str(flow.get("source_currency") or "UAH")),
                target_currency=normalize_currency(str(flow.get("target_currency") or "UAH")),
                fx_rate=rate,
            ),
            reply_markup=bind_financial_preview(flow, kb_saving_confirm()),
        )
        return

    remind_at = _parse_reminder_datetime_text(text, datetime.now())
    if remind_at is None:
        await update.message.reply_text(
            "Не вдалося розпізнати час. Використайте `YYYY-MM-DD HH:MM`, `DD.MM HH:MM` або `HH:MM`.",
            reply_markup=kb_inline_cancel("saving:cancel"),
        )
        return
    task_id = int(flow.get("pending_task_id") or 0)
    async with _pool(context).acquire() as conn:
        await SavingsService(conn).set_task_reminder(user.id, task_id, remind_at)
    _reset_saving_flow(context)
    await update.message.reply_text(f"Нагадаю {remind_at.strftime('%d.%m %H:%M')}.", reply_markup=kb_saving_plan_actions(task_id))


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.text:
        return
    if await _consume_trial_recovery_owner_reply(update, context):
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)
    access_state: dict[str, Any] | None = None
    access_scope = "paywall"
    trial_recovery_feedback: dict[str, Any] | None = None

    async with _pool(context).acquire() as conn:
        if await is_user_banned(conn, user.id):
            await update.message.reply_text(_access_restricted_text(current_ui_locale()))
            return
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await update.message.reply_text(maintenance_message)
            return
        await log_bot_event(
            conn,
            user.id,
            "text_received",
            source="text",
            raw_input=update.message.text,
        )
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str((access_state or {}).get("access_scope") or "paywall")
        trial_recovery_feedback = await consume_trial_recovery_free_text(
            conn,
            tg_user_id=user.id,
            text=update.message.text,
        )
        if trial_recovery_feedback is not None:
            await _notify_trial_recovery_owners(
                context,
                conn,
                recipient_id=int(trial_recovery_feedback["id"]),
                event="details",
                telegram_user=user,
                reason=str(trial_recovery_feedback["reason"]),
                free_text=str(trial_recovery_feedback["free_text"]),
                case_id=(
                    int(trial_recovery_feedback["support_case_id"])
                    if trial_recovery_feedback.get("support_case_id")
                    else None
                ),
            )

    if trial_recovery_feedback is not None:
        locale = _current_context_locale(context, getattr(user, "language_code", None))
        await update.message.reply_text(
            (
                "Thank you — I saved the details. Would you like me to message you personally?"
                if locale == "en"
                else "Дякую — деталі збережено. Хочете, щоб я написав вам особисто?"
            ),
            reply_markup=_trial_recovery_followup_keyboard(int(trial_recovery_feedback["id"]), locale),
        )
        return

    support_flow = context.user_data.get("support_flow") or {}
    if support_flow.get("await_text"):
        text = update.message.text.strip()
        if len(text) < 4:
            await update.message.reply_text("Опиши проблему трохи детальніше, будь ласка.", reply_markup=_kb_home_only())
            return
        async with _pool(context).acquire() as conn:
            case_id = await create_support_case(conn, user.id, text)
            await log_admin_notification(conn, "support_case_new", {"support_case_id": case_id, "telegram_user_id": user.id})
        _reset_support_flow(context)
        await notify_admins(
            context.application.bot,
            (
                f"Новий support case #{case_id}\n\n"
                f"Username: @{user.username}" if user.username else f"Новий support case #{case_id}\n\nUsername: -"
            )
            + f"\nTelegram ID: {user.id}\nТекст: {text[:250]}",
        )
        await update.message.reply_text("Дякую. Кейс створено, адміністратор отримає повідомлення.", reply_markup=_kb_home_only())
        return

    poll_text_flow = context.user_data.get("poll_text_flow") or {}
    if poll_text_flow.get("campaign_id"):
        campaign_id = int(poll_text_flow["campaign_id"])
        async with _pool(context).acquire() as conn:
            await save_poll_response(
                conn,
                campaign_id=campaign_id,
                tg_user_id=user.id,
                answer="text",
                text_answer=update.message.text.strip(),
            )
            await create_feedback_from_poll(
                conn,
                tg_user_id=user.id,
                category="feature_request",
                text=update.message.text.strip(),
                rating=None,
            )
        _reset_poll_text_flow(context)
        await update.message.reply_text("Дякую! Текстову відповідь збережено.", reply_markup=kb_home())
        return

    write_flow_active = any(
        [
            bool(context.user_data.get("tx_flow")),
            bool(context.user_data.get("ai_tx_flow")),
            bool(context.user_data.get("ai_batch_tx_flow")),
            bool(context.user_data.get("currency_resolution_flow")),
            bool(context.user_data.get("saving_flow")),
            bool(context.user_data.get("saving_settings_flow")),
            bool(context.user_data.get("debt_flow")),
            bool(context.user_data.get("categories_flow")),
            bool(context.user_data.get("accounts_flow")),
            str((context.user_data.get("settings_flow") or {}).get("mode") or "") == "account_create",
        ]
    )
    if write_flow_active:
        async with _pool(context).acquire() as conn:
            if access_scope == "debt_only" and context.user_data.get("debt_flow"):
                pass
            elif await _billing_write_blocked(update.message, conn, user.id):
                return

    accounts_flow = context.user_data.get("accounts_flow") or {}
    if accounts_flow.get("step") == "rename":
        await _handle_account_rename_text(update, context)
        return
    if accounts_flow.get("step") == "balance":
        await _handle_account_balance_text(update, context)
        return

    categories_flow = context.user_data.get("categories_flow") or {}
    if categories_flow:
        await update.message.chat.send_action(ChatAction.TYPING)
        text = update.message.text or ""
        action = str(categories_flow.get("await") or "")
        type_ = str(categories_flow.get("category_type") or categories_flow.get("type") or "")
        category_id = int(categories_flow.get("category_id") or 0)
        if type_ not in {"expense", "income"}:
            _reset_categories_flow(context)
            await update.message.reply_text(_categories_action_inactive_text(current_ui_locale()), reply_markup=categories_home_keyboard())
            return
        if type_ == "expense":
            _reset_categories_flow(context)
            await update.message.reply_text(
                "Стандартні категорії витрат не редагуються в налаштуваннях.",
                reply_markup=categories_list_keyboard("expense"),
            )
            return

        async with _pool(context).acquire() as conn:
            if not await _user_ready(conn, user.id):
                _reset_categories_flow(context)
                await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
                return

            svc = CategoryService(conn)
            name_error = _category_name_error(text)
            if action in {CategoryEditStates.WAITING_FOR_ADD_NAME, CategoryEditStates.WAITING_FOR_RENAME_NAME} and name_error:
                cancel_callback = "categories:menu"
                if action == CategoryEditStates.WAITING_FOR_RENAME_NAME and category_id:
                    cancel_callback = f"categories:edit:item:{category_id}"
                await update.message.reply_text(name_error, reply_markup=InlineKeyboardMarkup([[_cancel_btn(cancel_callback)]]))
                return

            normalized_name = _normalize_category_name_input(text)
            if action == CategoryEditStates.WAITING_FOR_ADD_NAME:
                existing = await svc.findCategoryByName(user.id, type_, normalized_name, includeInactive=True)
                if existing:
                    await update.message.reply_text("Така категорія вже існує. Введіть іншу назву або поверніться назад.")
                    return
                try:
                    await svc.createCategory(user.id, type_, normalized_name, aliases=[], source="custom")
                except (CategoryConflictError, CategoryValidationError):
                    await update.message.reply_text("Така категорія вже існує. Введіть іншу назву або поверніться назад.")
                    return
                _reset_categories_flow(context)
                await update.message.reply_text(
                    f"Категорію створено: {escape_html(normalized_name)}",
                    reply_markup=categories_list_keyboard(type_),
                )
                return

            if action == CategoryEditStates.WAITING_FOR_RENAME_NAME:
                category = await svc.getCategoryById(user.id, category_id)
                if not category or not category.is_active:
                    _reset_categories_flow(context)
                    await update.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(type_))
                    return
                if _normalize_category_name_input(category.name) == normalized_name:
                    await update.message.reply_text("Це вже поточна назва категорії. Введіть іншу назву або скасуй дію.")
                    return
                try:
                    old_name = str(categories_flow.get("old_name") or category.name)
                    await svc.renameCategory(user.id, category_id, normalized_name)
                except CategoryConflictError:
                    await update.message.reply_text("Категорія з такою назвою вже існує. Введіть іншу назву.")
                    return
                except CategoryValidationError:
                    await update.message.reply_text("Назва має містити від 2 до 40 символів.")
                    return
                except CategoryUnavailableError:
                    _reset_categories_flow(context)
                    await update.message.reply_text(_category_missing_text(), reply_markup=categories_unavailable_keyboard(type_))
                    return
                _reset_categories_flow(context)
                await _reply_category_edit_list(
                    update.message,
                    svc,
                    user.id,
                    type_,
                    notice=(
                        "Категорію перейменовано:\n"
                        f"{escape_html(old_name)} -> {escape_html(normalized_name)}"
                    ),
                )
                return

            if action == CategoryEditStates.WAITING_FOR_ALIASES:
                aliases = [alias.strip() for alias in text.split(",") if alias.strip()]
                await svc.addCategoryAliases(user.id, category_id, aliases)
                _reset_categories_flow(context)
                category = await svc.getCategoryById(user.id, category_id)
                if category:
                    await update.message.reply_text(
                        f"Ключові слова оновлено.\nПоточні слова: {', '.join(category.aliases) if category.aliases else '—'}",
                        reply_markup=category_edit_keyboard(
                            {"id": category.id, "type": category.type, "is_active": category.is_active, "is_system": category.is_system}
                        ),
                    )
                else:
                    await update.message.reply_text("Ключові слова оновлено.", reply_markup=categories_home_keyboard())
                return

        _reset_categories_flow(context)
        await update.message.reply_text("Зараз не очікую назву категорії. Оберіть дію кнопками вище.", reply_markup=categories_home_keyboard())
        return

    settings_flow = context.user_data.get("settings_flow") or {}
    if settings_flow.get("mode") == "account_create":
        await _handle_account_create_text(update, context)
        return

    if await _handle_currency_resolution_text(update, context, context.user_data.get("currency_resolution_flow") or {}):
        return

    if context.user_data.get("saving_settings_flow") or context.user_data.get("saving_flow"):
        await _handle_saving_text(update, context)
        return

    if settings_flow.get("await_currency_text"):
        cur = normalize_currency(update.message.text)
        if not is_valid_currency_code(cur):
            await update.message.reply_text(_currency_code_validation_text(), reply_markup=_kb_home_only())
            return
        if not cur.isalpha() or len(cur) < 3:
            await update.message.reply_text("Не схоже на валюту. Приклад: <code>UAH</code> або <code>USD</code>.", reply_markup=_kb_home_only())
            return
        async with _pool(context).acquire() as conn:
            await conn.execute("UPDATE users SET base_currency=$2 WHERE tg_user_id=$1", user.id, cur)
        _reset_settings_flow(context)
        await update.message.reply_text(f"✅ Базову валюту оновлено: {cur}", reply_markup=kb_settings_menu())
        return

    transfer_flow = context.user_data.get("transfer_flow") or {}
    if transfer_flow:
        step = transfer_flow.get("step")
        if step in {"enter_source_amount", "enter_exchange_rate", "await_credit_limit"}:
            await _handle_transfer_text(update, context)
            return
        if step == "confirm_transfer":
            allow_rate_edit = normalize_currency(str(transfer_flow.get("source_currency") or "UAH")) != normalize_currency(
                str(transfer_flow.get("target_currency") or "UAH")
            )
            await update.message.reply_text(
                _transfer_confirm_text(transfer_flow),
                reply_markup=_kb_transfer_confirm(allow_rate_edit=allow_rate_edit, flow=transfer_flow),
            )
            return
        if step in {"choose_source_account", "choose_target_account"}:
            await update.message.reply_text("Спочатку оберіть варіант кнопкою.", reply_markup=kb_inline_cancel("transfer:cancel"))
            return

    ai_batch_flow = context.user_data.get("ai_batch_tx_flow") or {}
    if await _handle_ai_batch_tx_text_input(update, context, ai_batch_flow):
        return

    ai_flow = context.user_data.get("ai_tx_flow") or {}
    if await _handle_ai_tx_text_input(update, context, ai_flow):
        return

    if context.user_data.get("tx_flow", {}).get("await_amount"):
        await _save_tx_amount(update, context, update.message.text)
        return

    tx_flow = context.user_data.get("tx_flow") or {}
    tx_step = str(tx_flow.get("step") or "")
    if tx_step == "await_credit_limit":
        await _handle_tx_credit_limit_text(update, context)
        return
    if tx_step == TX_FLOW_STEP_CONFIRMATION:
        await update.message.reply_text(
            "Щоб завершити операцію, натисніть «✅ Підтвердити» або «❌ Скасувати».",
            reply_markup=_kb_tx_confirm(tx_flow),
        )
        return

    if tx_step == TX_FLOW_STEP_COMMITTING:
        await update.message.reply_text("Операцію вже обробляю. Зачекайте кілька секунд.")
        return

    tx_flow_state = tx_flow
    if tx_flow_state and tx_flow_state.get("step") in {"choose_account", "choose_category"}:
        await update.message.reply_text("Спочатку оберіть варіант кнопкою.", reply_markup=kb_inline_cancel(f"{str(tx_flow_state.get('kind') or 'expense')}:cancel"))
        return

    if context.user_data.get("debt_flow"):
        await _handle_debt_text(update, context)
        return

    if _onboarding_active(context):
        await _sync_onboarding_debug(
            context,
            user.id,
            step="onboarding/in_progress",
            last_user_input=update.message.text,
            last_parse_error="User sent generic text while onboarding was still active.",
        )
        await update.message.reply_text(_onboarding_in_progress_text(current_ui_locale()))
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id) and access_scope != "debt_only":
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return

    if await _start_debt_from_text(update, context, update.message.text):
        return
    if access_scope == "debt_only":
        async with _pool(context).acquire() as conn:
            await _show_debt_only_surface(
                update.message,
                conn,
                user.id,
                notice="<b>Для цього акаунта доступний лише розділ боргів.</b>",
            )
        return
    if access_scope == "paywall":
        async with _pool(context).acquire() as conn:
            await _show_billing_menu(
                update.message,
                conn,
                user.id,
                notice="Щоб додавати записи, спочатку прив'яжіть картку.",
                access_state=access_state,
            )
        return
    if await _start_transfer_from_text(update, context, update.message.text):
        return
    if await _start_ai_tx_flow(update, context, update.message.text, origin="text"):
        return

    async with _pool(context).acquire() as conn:
        await log_bot_event(
            conn,
            user.id,
            "parse_error",
            source="text",
            raw_input=update.message.text,
            success=False,
            error_message="Could not match text input to a known operation",
        )

    await _reply_home(update.message, "Щоб додати нову операцію, відкрийте «🧾 Операції» і виберіть «Витрата» або «Дохід».")


async def voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or not update.message or not update.message.voice:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        if await is_user_banned(conn, user.id):
            await update.message.reply_text(_access_restricted_text(current_ui_locale()))
            return
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await update.message.reply_text(maintenance_message)
            return
        await log_bot_event(
            conn,
            user.id,
            "voice_received",
            source="voice",
            raw_input=f"duration={update.message.voice.duration or 0}",
            parsed_result={"duration": update.message.voice.duration or 0},
        )

    if _onboarding_active(context):
        await _sync_onboarding_debug(
            context,
            user.id,
            step="onboarding/in_progress",
            last_user_input=f"voice:{update.message.voice.duration or 0}",
            last_parse_error="User sent voice while onboarding was still active.",
        )
        await update.message.reply_text(_onboarding_voice_blocked_text(current_ui_locale()))
        return

    if update.message.voice.duration and update.message.voice.duration > 20:
        await update.message.reply_text("Голосове повідомлення має бути до 20 секунд. Спробуйте коротше.")
        return

    if context.user_data.get("debt_flow"):
        await update.message.reply_text("Голосом борги поки не додаються. Додайте борг текстом або через кнопки.")
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await update.message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if access_scope == "debt_only":
            await _show_debt_only_surface(update.message, conn, user.id, notice="<b>Голосовий ввід недоступний у розділі боргів.</b>")
            return
        if access_scope != "personal_full" and access_scope != "family_full":
            await _show_billing_menu(
                update.message,
                conn,
                user.id,
                notice="<b>Голосовий ввід доступний лише з повним доступом.</b>",
                access_state=access_state,
            )
            return

    if not config.OPENAI_API_KEY:
        await update.message.reply_text("Голосове розпізнавання зараз недоступне. Додайте операцію текстом або через меню.")
        return

    await update.message.chat.send_action(ChatAction.TYPING)
    telegram_file = await update.message.voice.get_file()
    ogg_bytes = await telegram_file.download_as_bytearray()

    try:
        result = await transcribe_ogg_bytes(bytes(ogg_bytes))
    except Exception as exc:
        async with _pool(context).acquire() as conn:
            await log_bot_event(
                conn,
                user.id,
                "bot_error",
                source="voice",
                success=False,
                error_message=str(exc),
            )
        await update.message.reply_text("Не вдалося розпізнати голосове повідомлення. Спробуйте ще раз коротше або додайте операцію через меню.")
        await notify_admins(
            context.application.bot,
            format_error_notification(event_type="bot_error", tg_user_id=user.id, error_message=str(exc)),
        )
        return

    await update.message.reply_text(f"Розпізнав: {result.text}")
    if parse_message(result.text, today=datetime.now().date(), default_currency="UAH").intent == "debt":
        await update.message.reply_text("Голосом борги поки не додаються. Додайте борг текстом або через кнопки.")
        return
    if context.user_data.get("tx_flow", {}).get("await_amount"):
        await _save_tx_amount(update, context, result.text)
        return

    if await _start_transfer_from_text(update, context, result.text):
        return
    if await _start_ai_tx_flow(update, context, result.text, origin="voice"):
        return

    async with _pool(context).acquire() as conn:
        await log_bot_event(
            conn,
            user.id,
            "parse_error",
            source="voice",
            raw_input=result.text,
            parsed_result={"transcript": result.text},
            success=False,
            error_message="Could not match voice input to a known operation",
        )
    await notify_admins(
        context.application.bot,
        format_error_notification(
            event_type="parse_error",
            tg_user_id=user.id,
            error_message="Could not match voice input to a known operation",
        ),
    )

    await update.message.reply_text(
        "Не вдалося впевнено розпізнати операцію з голосу.\n\nСкажіть коротко: сума, категорія, рахунок. Або додайте через меню."
    )
    await _reply_home(update.message)


async def photo_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    image = _extract_incoming_image(message)
    if not user or not message or image is None:
        return
    await _consume_pending_admin_reset_if_needed(context, user.id)

    async with _pool(context).acquire() as conn:
        if await is_user_banned(conn, user.id):
            await message.reply_text(_access_restricted_text(current_ui_locale()))
            return
        maintenance_message = await should_block_for_maintenance(conn, user.id)
        if maintenance_message:
            await message.reply_text(maintenance_message)
            return
        await log_bot_event(
            conn,
            user.id,
            "screenshot_received",
            source=AI_SCREENSHOT_SOURCE,
            raw_input=f"{image.source}:{image.telegram_file_unique_id}",
            parsed_result={"mime_type": image.mime_type, "file_size": image.file_size},
        )

    if _onboarding_active(context):
        await _sync_onboarding_debug(
            context,
            user.id,
            step="onboarding/in_progress",
            last_user_input=f"screenshot:{image.source}",
            last_parse_error="User sent screenshot while onboarding was still active.",
        )
        await message.reply_text(_onboarding_screenshot_blocked_text(current_ui_locale()))
        return

    if context.user_data.get("debt_flow"):
        await message.reply_text("Борги через скріншот поки не додаються. Додайте борг текстом або через кнопки.")
        return

    if not _is_supported_screenshot_mime(image.mime_type):
        await message.reply_text("Підтримуються зображення JPG, PNG, WEBP або GIF.")
        return
    if image.file_size and image.file_size > AI_SCREENSHOT_MAX_BYTES:
        await message.reply_text("Скріншот завеликий. Надішліть файл до 10 МБ.")
        return

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            await message.reply_text(_onboarding_required_text(current_ui_locale()))
            return
        access_state = await _get_resolved_access_state(conn, user.id)
        access_scope = str(access_state.get("access_scope") or "paywall")
        if access_scope == "debt_only":
            await _show_debt_only_surface(message, conn, user.id, notice="<b>Обробка скріншотів недоступна в розділі боргів.</b>")
            return
        if access_scope != "personal_full" and access_scope != "family_full":
            await _show_billing_menu(
                message,
                conn,
                user.id,
                notice="<b>Обробка скріншотів доступна лише з повним доступом.</b>",
                access_state=access_state,
            )
            return
        catalog = await _load_ai_catalog(conn, user.id)
        existing_draft = await AiTransactionDraftService(conn).get_by_media_key(
            user.id,
            source=AI_SCREENSHOT_SOURCE,
            telegram_file_unique_id=image.telegram_file_unique_id,
        )
        user_row = await _get_user(conn, user.id)

    draft_to_refresh: AiTransactionDraft | None = None
    if existing_draft is not None:
        if existing_draft.status == "completed":
            await _reply_home(message, "Цей скріншот уже був підтверджений. Повторну операцію не створюю.")
            return
        if existing_draft.status == "cancelled":
            await _reply_home(message, "Цей скріншот уже був скасований. Для нового запису надішліть інший файл.")
            return
        if _should_reanalyze_pending_screenshot_draft(existing_draft):
            draft_to_refresh = existing_draft
        else:
            restored = await _restore_existing_pending_screenshot_draft_preview(
                message,
                context,
                existing_draft,
                accounts_map=catalog["accounts_map"],
                categories_map=catalog["categories_map"],
            )
            if restored:
                return

    if not config.OPENAI_API_KEY:
        await message.reply_text("Розпізнавання скріншотів зараз недоступне. Додайте операцію текстом або через меню.")
        return

    await message.chat.send_action(ChatAction.TYPING)
    telegram_file = await (message.document.get_file() if getattr(message, "document", None) else message.photo[-1].get_file())
    image_bytes = bytes(await telegram_file.download_as_bytearray())
    if len(image_bytes) > AI_SCREENSHOT_MAX_BYTES:
        await message.reply_text("Скріншот завеликий. Надішліть файл до 10 МБ.")
        return

    default_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
    try:
        analysis = await analyze_screenshot(
            image_bytes,
            mime_type=image.mime_type,
            today=datetime.now().date(),
            default_currency=default_currency,
            accounts=_vision_account_options(catalog["accounts_rows"]),
            expense_categories=_vision_category_options("expense", catalog["expense_full"]),
            income_categories=_vision_category_options("income", catalog["income_full"]),
        )
    except OpenAIVisionConfigError as exc:
        await message.reply_text("Не вдалося розпізнати скріншот. Спробуйте інший скріншот або додайте операцію текстом.")
        return
    except (OpenAIVisionError, OpenAIVisionInvalidResponseError) as exc:
        async with _pool(context).acquire() as conn:
            await log_bot_event(
                conn,
                user.id,
                "bot_error",
                source=AI_SCREENSHOT_SOURCE,
                success=False,
                error_message=str(exc),
            )
        await message.reply_text("Не вдалося розібрати скріншот. Спробуйте інший або введіть операцію текстом.")
        await notify_admins(
            context.application.bot,
            format_error_notification(event_type="bot_error", tg_user_id=user.id, error_message=str(exc)),
        )
        return

    currency_explicit = _screenshot_currency_is_explicit(analysis)
    accounts_full = catalog.get("accounts_full") or []
    accounts_full_by_id: dict[int, Any] = {}
    for account in accounts_full:
        try:
            accounts_full_by_id[int(account["id"])] = account
        except (KeyError, TypeError, ValueError):
            continue
    account_id = _pick_account_id_for_ai_flow(
        account_hint=analysis.account_hint,
        bank_name=analysis.bank_name,
        card_last4=analysis.card_last4,
        currency=analysis.dominant_currency or analysis.currency,
        accounts_rows=catalog["accounts_rows"],
        accounts_full=accounts_full,
        allow_single_account_fallback=not currency_explicit,
        allow_currency_match=currency_explicit,
        allow_cash_currency_match=False,
    )

    if analysis.mode == "statement_expenses":
        batch_items: list[dict] = []
        recent_expense_samples: list[dict[str, Any]] = []
        async with _pool(context).acquire() as conn:
            recent_expense_samples = await _load_recent_categorized_transactions(conn, user.id, "expense")
        if account_id is None and not currency_explicit:
            account_id = _pick_recent_account_id_from_samples(
                recent_expense_samples,
                accounts_full_by_id=accounts_full_by_id,
                allow_cash_accounts=False,
            )
        dominant_currency = analysis.dominant_currency or analysis.currency
        if not currency_explicit and account_id in accounts_full_by_id:
            dominant_currency = _normalized_account_currency(accounts_full_by_id[account_id], fallback=dominant_currency)
        for item in analysis.statement_items:
            category_id = _pick_category_id_by_normalization([item.category_hint, item.comment], catalog["expense_full"])
            if category_id is None:
                category_id = _pick_category_id_by_hint(item.category_hint, catalog["expense_full"])
            if category_id is None and item.comment:
                category_id = _pick_category_id_by_text(item.comment, catalog["expense_full"])
            if category_id is None:
                category_id = _pick_category_id_by_recent_samples(
                    [item.category_hint, item.comment],
                    recent_expense_samples,
                    account_id=account_id,
                )
            batch_items.append(
                {
                    "date": item.transaction_date.isoformat(),
                    "type": "expense",
                    "amount": item.amount,
                    "currency": item.currency if currency_explicit else dominant_currency,
                    "currency_explicit": currency_explicit,
                    "category_id": category_id,
                    "comment": item.comment,
                    "source": AI_SCREENSHOT_SOURCE,
                }
            )

        if not batch_items:
            await message.reply_text("Не вдалося надійно знайти витрати на скріншоті. Спробуйте інший скріншот або додайте операцію текстом.")
            return

        draft_metadata = {
            "mode": "statement_expenses",
            "model": analysis.model,
            "detail": analysis.detail,
            "used_fallback": analysis.used_fallback,
            "screenshot_draft_schema_version": AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
            "bank_name": analysis.bank_name or "",
            "card_last4": analysis.card_last4 or "",
            "account_hint": analysis.account_hint or "",
            "category_hint": analysis.category_hint or "",
            "dominant_currency": dominant_currency,
            "currency_evidence_kind": analysis.currency_evidence_kind or "",
            "currency_evidence_text": analysis.currency_evidence_text or "",
            "currency_context_text": analysis.currency_context_text or "",
            "currency_resolution": analysis.currency_resolution or "",
            "currency_explicit": currency_explicit,
            "statement_items": [_serialize_ai_batch_item_for_metadata(item) for item in batch_items],
            "statement_skipped_items_count": int(analysis.skipped_items_count or 0),
            "mime_type": image.mime_type,
            "file_size": image.file_size or len(image_bytes),
            "file_source": image.source,
        }
        if analysis.raw_response is not None:
            draft_metadata["raw_response"] = analysis.raw_response

        draft_tx_payload = dict(batch_items[0])
        draft_tx_payload["account_id"] = account_id

        async with _pool(context).acquire() as conn:
            draft_service = AiTransactionDraftService(conn)
            if draft_to_refresh is not None:
                draft = await draft_service.update_draft(
                    draft_to_refresh.id,
                    user.id,
                    tx=draft_tx_payload,
                    confidence=analysis.confidence,
                    metadata=draft_metadata,
                )
                created = False
                if draft is None:
                    raise RuntimeError("AI screenshot draft refresh failed")
            else:
                draft, created = await draft_service.create_or_get(
                    user.id,
                    source=AI_SCREENSHOT_SOURCE,
                    telegram_file_unique_id=image.telegram_file_unique_id,
                    telegram_file_id=image.telegram_file_id,
                    telegram_message_id=getattr(message, "message_id", None),
                    tx=draft_tx_payload,
                    confidence=analysis.confidence,
                    metadata=draft_metadata,
                )
            await log_bot_event(
                conn,
                user.id,
                "screenshot_draft_created",
                source=AI_SCREENSHOT_SOURCE,
                raw_input=f"{image.source}:{image.telegram_file_unique_id}",
                parsed_result={
                    "confidence": analysis.confidence,
                    "mode": "statement_expenses",
                    "items": len(batch_items),
                    "skipped_items_count": int(analysis.skipped_items_count or 0),
                    "used_fallback": analysis.used_fallback,
                    "dominant_currency": analysis.dominant_currency or analysis.currency,
                    "currency_evidence_kind": analysis.currency_evidence_kind,
                    "currency_evidence_text": analysis.currency_evidence_text,
                    "currency_context_text": analysis.currency_context_text,
                    "currency_resolution": analysis.currency_resolution,
                },
            )

        if not created:
            if draft.status == "completed":
                await _reply_home(message, "Цей скріншот уже був підтверджений. Повторну операцію не створюю.")
                return
            if draft.status == "cancelled":
                await _reply_home(message, "Цей скріншот уже був скасований. Для нового запису надішліть інший файл.")
                return
            flow = _build_ai_batch_tx_flow_from_draft(
                draft,
                accounts_map=catalog["accounts_map"],
                categories_map=catalog["categories_map"],
            )
            if flow is None:
                await message.reply_text("Чернетка скріншота пошкоджена. Надішліть скріншот ще раз.")
                return
        else:
            flow = _build_ai_batch_tx_flow_state(
                items=batch_items,
                account_id=account_id,
                accounts_map=catalog["accounts_map"],
                categories_map=catalog["categories_map"],
                origin=AI_SCREENSHOT_SOURCE,
                raw_text=f"screenshot:{image.telegram_file_unique_id}",
                confidence=analysis.confidence,
                draft_id=draft.id,
                draft_metadata=draft_metadata,
                skipped_items_count=int(analysis.skipped_items_count or 0),
            )

        context.user_data["ai_batch_tx_flow"] = flow
        await message.reply_text("Знайшов витрати зі скріншота. Перевірте дані перед збереженням.")
        await message.reply_text(
            _ai_batch_tx_card_text(flow),
            reply_markup=bind_financial_preview(flow, kb_ai_batch_confirm(len(_ai_batch_flow_items(flow)))),
        )
        return

    if analysis.mode == "unsupported":
        await message.reply_text("Не вдалося надійно розібрати скріншот. Спробуйте інший або додайте операцію текстом.")
        return

    tx_type = analysis.type or ""
    if account_id is None and not currency_explicit and tx_type in {"expense", "income"}:
        async with _pool(context).acquire() as conn:
            recent_samples = await _load_recent_categorized_transactions(conn, user.id, tx_type)
        account_id = _pick_recent_account_id_from_samples(
            recent_samples,
            accounts_full_by_id=accounts_full_by_id,
            allow_cash_accounts=False,
        )
    category_full = catalog["income_full"] if tx_type == "income" else catalog["expense_full"] if tx_type == "expense" else []
    category_id = None
    if tx_type == "expense":
        category_id = _pick_category_id_by_normalization([analysis.category_hint, analysis.comment], category_full)
    if category_id is None:
        category_id = _pick_category_id_by_hint(analysis.category_hint, category_full)
    if category_id is None and analysis.comment:
        category_id = _pick_category_id_by_text(analysis.comment, category_full)
    if category_id is None and tx_type in {"expense", "income"}:
        async with _pool(context).acquire() as conn:
            recent_samples = await _load_recent_categorized_transactions(conn, user.id, tx_type)
        category_id = _pick_category_id_by_recent_samples(
            [analysis.category_hint, analysis.comment],
            recent_samples,
            account_id=account_id,
        )
    tx_currency = analysis.currency
    dominant_currency = analysis.dominant_currency or analysis.currency
    if not currency_explicit and account_id in accounts_full_by_id:
        tx_currency = _normalized_account_currency(accounts_full_by_id[account_id], fallback=tx_currency)
        dominant_currency = tx_currency

    tx_payload = {
        "date": analysis.transaction_date.isoformat(),
        "type": tx_type,
        "amount": analysis.amount,
        "currency": tx_currency,
        "currency_explicit": currency_explicit,
        "account_id": account_id,
        "category_id": category_id,
        "comment": analysis.comment,
        "source": AI_SCREENSHOT_SOURCE,
    }
    draft_metadata = {
        "mode": analysis.mode,
        "model": analysis.model,
        "detail": analysis.detail,
        "used_fallback": analysis.used_fallback,
        "screenshot_draft_schema_version": AI_SCREENSHOT_DRAFT_SCHEMA_VERSION,
        "bank_name": analysis.bank_name or "",
        "card_last4": analysis.card_last4 or "",
        "account_hint": analysis.account_hint or "",
        "category_hint": analysis.category_hint or "",
        "dominant_currency": dominant_currency,
        "currency_evidence_kind": analysis.currency_evidence_kind or "",
        "currency_evidence_text": analysis.currency_evidence_text or "",
        "currency_context_text": analysis.currency_context_text or "",
        "currency_resolution": analysis.currency_resolution or "",
        "currency_explicit": currency_explicit,
        "mime_type": image.mime_type,
        "file_size": image.file_size or len(image_bytes),
        "file_source": image.source,
    }
    if analysis.raw_response is not None:
        draft_metadata["raw_response"] = analysis.raw_response

    async with _pool(context).acquire() as conn:
        draft_service = AiTransactionDraftService(conn)
        if draft_to_refresh is not None:
            draft = await draft_service.update_draft(
                draft_to_refresh.id,
                user.id,
                tx=tx_payload,
                confidence=analysis.confidence,
                metadata=draft_metadata,
            )
            created = False
            if draft is None:
                raise RuntimeError("AI screenshot draft refresh failed")
        else:
            draft, created = await draft_service.create_or_get(
                user.id,
                source=AI_SCREENSHOT_SOURCE,
                telegram_file_unique_id=image.telegram_file_unique_id,
                telegram_file_id=image.telegram_file_id,
                telegram_message_id=getattr(message, "message_id", None),
                tx=tx_payload,
                confidence=analysis.confidence,
                metadata=draft_metadata,
            )
        await log_bot_event(
            conn,
            user.id,
            "screenshot_draft_created",
            source=AI_SCREENSHOT_SOURCE,
            raw_input=f"{image.source}:{image.telegram_file_unique_id}",
            parsed_result={
                "confidence": analysis.confidence,
                "type": tx_type,
                "has_amount": analysis.amount is not None,
                "used_fallback": analysis.used_fallback,
                "currency": analysis.currency,
                "dominant_currency": analysis.dominant_currency or analysis.currency,
                "currency_evidence_kind": analysis.currency_evidence_kind,
                "currency_evidence_text": analysis.currency_evidence_text,
                "currency_context_text": analysis.currency_context_text,
                "currency_resolution": analysis.currency_resolution,
            },
        )

    if not created:
        if draft.status == "completed":
            await _reply_home(message, "Цей скріншот уже був підтверджений. Повторну операцію не створюю.")
            return
        if draft.status == "cancelled":
            await _reply_home(message, "Цей скріншот уже був скасований. Для нового запису надішліть інший файл.")
            return
        flow = _build_ai_tx_flow_from_draft(
            draft,
            accounts_map=catalog["accounts_map"],
            categories_map=catalog["categories_map"],
        )
    else:
        flow = _build_ai_tx_flow_state(
            tx=tx_payload,
            accounts_map=catalog["accounts_map"],
            categories_map=catalog["categories_map"],
            origin=AI_SCREENSHOT_SOURCE,
            confidence=analysis.confidence,
            draft_id=draft.id,
            draft_metadata=draft_metadata,
        )
    context.user_data["ai_tx_flow"] = flow
    await message.reply_text("Чернетку зі скріншоту підготовлено. Перевірте дані перед збереженням.")
    await message.reply_text(_ai_tx_card_text(flow), reply_markup=_ai_tx_confirm_markup(flow))


async def _start_debt_from_text(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_text: str) -> bool:
    user = update.effective_user
    if not user or not raw_text:
        return False

    if context.user_data.get("debt_flow"):
        return False

    async with _pool(context).acquire() as conn:
        if not await _user_ready(conn, user.id):
            return False
        user_row = await _get_user(conn, user.id)
        service = DebtService(conn)
        base_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
        parsed = parse_message(raw_text, today=datetime.now().date(), default_currency=base_currency)
        if parsed.intent != "debt" or parsed.amount is None or not parsed.person_id:
            return False

        debt_amount = quantize_money(Decimal(str(parsed.amount)))
        debt_currency = normalize_currency(parsed.currency or base_currency) or "UAH"
        base_flow = {
            "source": "text",
            "counterparty_name": parsed.person_id,
            "debt_amount": debt_amount,
            "debt_currency": debt_currency,
            "raw_text": raw_text[:500],
        }

        if parsed.debt_action is None:
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                **base_flow,
                "mode": "create",
                "step": DEBT_FLOW_STEP_CHOOSING_DIRECTION,
            }
            await update.message.reply_text(
                "Я не до кінця зрозумів, що це за борг.\n\nОбери:",
                reply_markup=kb_debt_direction_choice(),
            )
            return True

        if parsed.debt_action in {"lend", "borrow"}:
            direction = "receivable" if parsed.debt_action == "lend" else "payable"
            _reset_debt_flow(context)
            context.user_data["debt_flow"] = {
                **base_flow,
                "mode": "create",
                "direction": direction,
                "step": DEBT_FLOW_STEP_CHOOSING_ACCOUNT,
            }
            await _show_debt_accounts(update.message, context, user.id, back_callback="debt:cancel")
            return True

        repay_direction = "receivable" if parsed.debt_action == "receive_repayment" else "payable"
        debts = await (
            service.list_active_receivable_debts(user.id)
            if repay_direction == "receivable"
            else service.list_active_payable_debts(user.id)
        )
        matched = [
            debt
            for debt in debts
            if parsed.person_id.lower() in str(debt["counterparty_name"] or "").lower()
        ]
        _reset_debt_flow(context)
        if len(matched) == 1:
            debt = matched[0]
            context.user_data["debt_flow"] = {
                **base_flow,
                "mode": "repay",
                "direction": repay_direction,
                "repay_type": "in" if repay_direction == "receivable" else "out",
                "debt_id": int(debt["id"]),
                "debt_amount": debt_amount,
                "debt_currency": normalize_currency(str(debt["currency"] or debt_currency)) or debt_currency,
                "step": DEBT_FLOW_STEP_CHOOSING_REPAYMENT_ACCOUNT,
            }
            await _show_debt_accounts(update.message, context, user.id, back_callback="debt:cancel")
            return True

        context.user_data["debt_flow"] = {
            **base_flow,
            "mode": "repay",
            "direction": repay_direction,
            "repay_type": "in" if repay_direction == "receivable" else "out",
            "step": DEBT_FLOW_STEP_CHOOSING_DEBT_FOR_REPAYMENT,
        }
        await update.message.reply_text(
            "Оберіть борг для погашення:",
            reply_markup=_debt_debt_list_keyboard(debts, prefix="debt:repay"),
        )
        return True


async def _handle_debt_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if not user or not message or not message.text:
        return

    flow = context.user_data.get("debt_flow") or {}
    step = str(flow.get("step") or "")
    text = message.text.strip()

    if step == DEBT_FLOW_STEP_ENTERING_COUNTERPARTY:
        counterparty = text[:120].strip()
        if not counterparty:
            await message.reply_text("Імʼя не може бути порожнім. Спробуйте ще раз:")
            return
        flow["counterparty_name"] = counterparty
        flow["step"] = DEBT_FLOW_STEP_ENTERING_AMOUNT
        context.user_data["debt_flow"] = flow
        await message.reply_text("Сума боргу?", reply_markup=kb_inline_cancel("debt:cancel"))
        return

    if step == DEBT_FLOW_STEP_ENTERING_AMOUNT:
        amount = parse_decimal_amount(text)
        if amount is None or amount <= 0:
            await message.reply_text("Введіть суму числом, наприклад <code>1000</code>.", reply_markup=kb_inline_cancel("debt:cancel"))
            return
        flow["debt_amount"] = quantize_money(amount)
        flow["step"] = DEBT_FLOW_STEP_CHOOSING_CURRENCY
        context.user_data["debt_flow"] = flow
        await message.reply_text("Валюта?", reply_markup=kb_currency(prefix="debt:currency", include_other=True))
        return

    if step == DEBT_FLOW_STEP_ENTERING_FX_VALUE:
        debt_amount = flow.get("debt_amount")
        if not isinstance(debt_amount, Decimal):
            await message.reply_text(_debt_action_inactive_text(current_ui_locale()), reply_markup=kb_debts_menu())
            _reset_debt_flow(context)
            return
        debt_currency = normalize_currency(str(flow.get("debt_currency") or "UAH")) or "UAH"
        account_currency = normalize_currency(str(flow.get("account_currency") or debt_currency)) or debt_currency
        fx_mode = str(flow.get("fx_mode") or "rate")
        if fx_mode == "amount":
            account_amount = parse_decimal_amount(text)
            if account_amount is None or account_amount <= 0:
                await message.reply_text("Введіть суму в валюті рахунку.", reply_markup=kb_inline_cancel("debt:cancel"))
                return
            flow["account_amount"] = quantize_money(account_amount)
            if debt_amount > 0:
                flow["exchange_rate"] = (flow["account_amount"] / debt_amount).quantize(Decimal("0.0001"))
        else:
            rate = parse_decimal_rate(text)
            if rate is None or rate <= 0:
                await message.reply_text("Введіть коректний курс.", reply_markup=kb_inline_cancel("debt:cancel"))
                return
            flow["exchange_rate"] = rate
            flow["account_amount"] = calculate_transfer_amount(debt_currency, account_currency, debt_amount, rate).target_amount
        if flow.get("mode") == "create":
            flow["step"] = DEBT_FLOW_STEP_ENTERING_DUE_DATE
            context.user_data["debt_flow"] = flow
            await message.reply_text("Строк повернення? Введіть дату або натисніть '-', щоб пропустити:", reply_markup=kb_inline_cancel("debt:cancel"))
            return
        flow["step"] = DEBT_FLOW_STEP_CONFIRMING_REPAYMENT
        context.user_data["debt_flow"] = flow
        await message.reply_text(_debt_confirm_text(flow), reply_markup=bind_financial_preview(flow, kb_debt_confirm()))
        return

    if step == "entering_currency_text":
        currency = normalize_currency(text) or "UAH"
        flow["debt_currency"] = currency
        flow["step"] = DEBT_FLOW_STEP_CHOOSING_ACCOUNT
        context.user_data["debt_flow"] = flow
        await _show_debt_accounts(message, context, user.id, back_callback="debt:cancel")
        return

    if step == DEBT_FLOW_STEP_ENTERING_DUE_DATE:
        flow["due_date"] = _parse_optional_date(text)
        flow["step"] = DEBT_FLOW_STEP_ENTERING_COMMENT
        context.user_data["debt_flow"] = flow
        await message.reply_text("Коментар? Можна пропустити.", reply_markup=kb_inline_cancel("debt:cancel"))
        return

    if step == DEBT_FLOW_STEP_ENTERING_COMMENT:
        flow["comment"] = text if text and text not in {"-", "—"} else ""
        flow["step"] = DEBT_FLOW_STEP_CONFIRMING_DEBT
        context.user_data["debt_flow"] = flow
        await message.reply_text(_debt_confirm_text(flow), reply_markup=bind_financial_preview(flow, kb_debt_confirm()))
        return

    if step == DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT:
        amount = parse_decimal_amount(text)
        if amount is None or amount <= 0:
            await message.reply_text("Введіть суму числом, наприклад <code>300</code>.", reply_markup=kb_inline_cancel("debt:cancel"))
            return
        async with _pool(context).acquire() as conn:
            debt = await DebtService(conn).get_debt(user.id, int(flow.get("debt_id") or 0))
        if debt is None:
            await message.reply_text("Борг не знайдено.", reply_markup=kb_debts_menu())
            _reset_debt_flow(context)
            return
        remaining = quantize_money(Decimal(str(debt["remaining_amount"] or 0)))
        if amount > remaining:
            flow["requested_amount"] = quantize_money(amount)
            flow["remaining_amount"] = remaining
            flow["step"] = DEBT_FLOW_STEP_CONFIRMING_OVER_LIMIT
            context.user_data["debt_flow"] = flow
            await message.reply_text(
                f"Залишок боргу: {format_money(remaining, str(debt['currency'] or 'UAH'))}.\n"
                f"Ви ввели {format_money(amount, str(debt['currency'] or 'UAH'))}.\n\nЩо зробити?",
                reply_markup=kb_debt_overlimit(int(debt["id"])),
            )
            return
        flow["debt_amount"] = quantize_money(amount)
        flow["step"] = DEBT_FLOW_STEP_CHOOSING_REPAYMENT_ACCOUNT
        context.user_data["debt_flow"] = flow
        await _show_debt_accounts(message, context, user.id, back_callback="debt:cancel")
        return

    if step == DEBT_FLOW_STEP_EDIT_VALUE:
        debt_id = int(flow.get("debt_id") or 0)
        field = str(flow.get("field") or "")
        kwargs: dict[str, object] = {"debt_id": debt_id}
        if field == "counterparty":
            kwargs["counterparty_name"] = text[:120]
        elif field == "amount":
            amount = parse_decimal_amount(text)
            if amount is None or amount <= 0:
                await message.reply_text("Введіть суму числом.", reply_markup=kb_inline_cancel("debt:cancel"))
                return
            kwargs["initial_amount"] = quantize_money(amount)
        elif field == "currency":
            kwargs["currency"] = normalize_currency(text)
        elif field == "due_date":
            kwargs["due_date"] = _parse_optional_date(text)
        elif field == "comment":
            kwargs["comment"] = text if text not in {"-", "—"} else ""
        else:
            await message.reply_text("Невідоме поле.", reply_markup=kb_debt_detail(debt_id))
            return
        async with _pool(context).acquire() as conn:
            result = await DebtService(conn).update_debt(user.id, **kwargs)  # type: ignore[arg-type]
            if result.status != "completed" or result.debt is None:
                await message.reply_text("Не вдалося оновити борг.", reply_markup=kb_debt_detail(debt_id))
                return
        _reset_debt_flow(context)
        await message.reply_text("Борг оновлено.", reply_markup=kb_debt_detail(debt_id))
        return

    await message.reply_text("Оберіть дію кнопкою.", reply_markup=kb_debts_menu())


def build_app() -> Application:
    if not config.BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Put it into /opt/my-cash-flow-bot/.env")

    app = (
        Application.builder()
        .token(config.BOT_TOKEN)
        .defaults(Defaults(parse_mode=ParseMode.HTML))
        .post_init(post_init)
        .post_shutdown(shutdown_db)
        .build()
    )

    onboarding = ConversationHandler(
        entry_points=[
            CommandHandler("start", start_entry),
            CommandHandler("onboarding", restart_onboarding_entry),
            CallbackQueryHandler(restart_onboarding_callback_entry, pattern=r"^onb:confirm:restart$"),
        ],
        states={
            LANG: [
                CallbackQueryHandler(onb_lang, pattern=r"^onb:lang:"),
                CallbackQueryHandler(onb_privacy, pattern=r"^onb:privacy:"),
            ],
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
            ACC_ACCOUNT_TYPE: [CallbackQueryHandler(onb_account_type_choice, pattern=r"^onb:acct:type:")],
            ACC_LAST4_CHOICE: [
                CallbackQueryHandler(onb_account_last4_choice, pattern=r"^onb:acct:last4:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_last4_text_direct),
            ],
            ACC_LAST4_TEXT: [
                CallbackQueryHandler(onb_account_last4_choice, pattern=r"^onb:acct:last4:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_last4_text),
            ],
            ACC_CURRENCY: [
                CallbackQueryHandler(onb_account_currency_callback, pattern=r"^onb:acct:cur:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_currency_text),
            ],
            ACC_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, onb_account_balance)],
            ACC_MORE_DONE: [CallbackQueryHandler(onb_accounts_more_done, pattern=r"^onb:acct:(more|done)$")],
            ONB_CONFIRM: [CallbackQueryHandler(onb_confirm, pattern=r"^onb:confirm:")],
            ONB_EDIT_ACCOUNTS: [CallbackQueryHandler(onb_edit_accounts, pattern=r"^onb:edit:")],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    app.add_handler(onboarding)
    app.add_handler(CommandHandler("menu", menu_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("cabinet", cabinet_command))
    app.add_handler(CallbackQueryHandler(billing_tx_callback, pattern=r"^btx:"))
    app.add_handler(CallbackQueryHandler(ai_callback, pattern=r"^ai:"))
    app.add_handler(CallbackQueryHandler(onb_privacy, pattern=r"^onb:privacy:"))
    app.add_handler(CallbackQueryHandler(onboarding_callback_fallback, pattern=r"^onb:(confirm:|edit:|acct:last4:)"))
    app.add_handler(CallbackQueryHandler(help_callback, pattern=r"^help:"))
    app.add_handler(CallbackQueryHandler(home_callback, pattern=r"^(billing:continue$|menu:|home:|accounts:|expense:start$|income:start$|transfer:start$|reports:start$|categories:start$|debts:start$|settings:start$|family:start$|export:start$|txvoid:)"))
    app.add_handler(CallbackQueryHandler(family_callback, pattern=r"^family:"))
    app.add_handler(CallbackQueryHandler(saving_callback, pattern=r"^saving:"))
    app.add_handler(CallbackQueryHandler(transfer_callback, pattern=r"^transfer:"))
    app.add_handler(CallbackQueryHandler(reports_callback, pattern=r"^reports:"))
    app.add_handler(CallbackQueryHandler(export_callback, pattern=r"^export:"))
    app.add_handler(CallbackQueryHandler(debt_callback, pattern=r"^debt:"))
    app.add_handler(CallbackQueryHandler(debt_callback, pattern=r"^debts:"))
    app.add_handler(CallbackQueryHandler(trial_recovery_callback, pattern=r"^trialrec:"))
    app.add_handler(CallbackQueryHandler(settings_callback, pattern=r"^settings:"))
    app.add_handler(CallbackQueryHandler(expense_reminder_callback, pattern=r"^expense:reminder:"))
    app.add_handler(CallbackQueryHandler(categories_callback, pattern=r"^categories:"))
    app.add_handler(CallbackQueryHandler(pick_callback, pattern=r"^(expense:(account|category|edit|confirm|cancel)|income:(account|category|edit|confirm|cancel)|pick:)"))
    app.add_handler(CallbackQueryHandler(poll_callback, pattern=r"^poll:"))
    app.add_handler(MessageHandler(filters.VOICE, voice_message))
    app.add_handler(MessageHandler(filters.PHOTO, photo_message))
    app.add_handler(MessageHandler(filters.Document.IMAGE, photo_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    return app


def main() -> None:
    logger.info("BOT BUILD: auto-credit-without-limit v0.6.26 candidate loaded")
    app = build_app()
    logger.info("Bot starting (long polling)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()






