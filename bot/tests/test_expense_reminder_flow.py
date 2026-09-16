from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Pool = object
    asyncpg_stub.Record = object
    asyncpg_stub.create_pool = AsyncMock()
    sys.modules["asyncpg"] = asyncpg_stub

if "telegram" not in sys.modules:
    telegram_stub = types.ModuleType("telegram")

    class _InlineKeyboardButton:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.text = args[0] if args else kwargs.get("text", "")
            self.callback_data = kwargs.get("callback_data")

    class _InlineKeyboardMarkup:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.inline_keyboard = args[0] if args else kwargs.get("inline_keyboard", [])

    class _Update:
        pass

    telegram_stub.Update = _Update
    telegram_stub.InlineKeyboardButton = _InlineKeyboardButton
    telegram_stub.InlineKeyboardMarkup = _InlineKeyboardMarkup
    sys.modules["telegram"] = telegram_stub

    constants_stub = types.ModuleType("telegram.constants")
    constants_stub.ChatAction = types.SimpleNamespace(TYPING="typing")
    constants_stub.ParseMode = types.SimpleNamespace(HTML="HTML")
    sys.modules["telegram.constants"] = constants_stub

    ext_stub = types.ModuleType("telegram.ext")

    class _Stub:
        pass

    ext_stub.Application = _Stub
    ext_stub.CallbackQueryHandler = _Stub
    ext_stub.CommandHandler = _Stub
    ext_stub.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext_stub.Defaults = _Stub
    ext_stub.ConversationHandler = _Stub
    ext_stub.MessageHandler = _Stub
    ext_stub.filters = types.SimpleNamespace(TEXT=object(), COMMAND=object(), VOICE=object())
    sys.modules["telegram.ext"] = ext_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class _AsyncClient:
        pass

    httpx_stub.AsyncClient = _AsyncClient
    sys.modules["httpx"] = httpx_stub

import bot_main  # noqa: E402


class DummyMessage:
    def __init__(self) -> None:
        self.replies: list[dict] = []
        self.chat = SimpleNamespace(send_action=AsyncMock())

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})


class DummyQuery:
    def __init__(self, data: str, message: DummyMessage) -> None:
        self.data = data
        self.message = message
        self.answered = False

    async def answer(self) -> None:
        self.answered = True


class DummyReminderConn:
    def __init__(self) -> None:
        self.reminder_settings: dict[int, dict] = {}
        self.day_statuses: dict[tuple[int, date], dict] = {}
        self.transactions: list[dict] = []

    async def execute(self, query: str, *args) -> None:
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO daily_expense_reminder_settings"):
            tg_user_id = int(args[0])
            self.reminder_settings.setdefault(
                tg_user_id,
                {"tg_user_id": tg_user_id, "mode": str(args[1]), "reminder_hour": int(args[2])},
            )
            return
        if normalized.startswith("UPDATE daily_expense_reminder_settings SET"):
            tg_user_id = int(args[0])
            current = self.reminder_settings.setdefault(
                tg_user_id,
                {"tg_user_id": tg_user_id, "mode": "daily", "reminder_hour": 21},
            )
            if "mode=$2" in normalized:
                current["mode"] = str(args[1])
            if "reminder_hour=$2" in normalized:
                current["reminder_hour"] = int(args[1])
            if "mode=$2, reminder_hour=$3" in normalized:
                current["mode"] = str(args[1])
                current["reminder_hour"] = int(args[2])
            return
        if normalized.startswith("INSERT INTO daily_expense_day_statuses"):
            tg_user_id = int(args[0])
            day_value = args[1]
            key = (tg_user_id, day_value)
            current = self.day_statuses.setdefault(
                key,
                {
                    "tg_user_id": tg_user_id,
                    "day": day_value,
                    "reminder_sent_at": None,
                    "sent_copy_key": None,
                    "no_expenses_confirmed_at": None,
                    "expense_recorded_at": None,
                },
            )
            if "no_expenses_confirmed_at" in normalized:
                current["no_expenses_confirmed_at"] = True
            if "expense_recorded_at" in normalized:
                current["expense_recorded_at"] = True
                current["no_expenses_confirmed_at"] = None
            return

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT * FROM daily_expense_reminder_settings WHERE tg_user_id=$1"):
            current = self.reminder_settings.get(int(args[0]))
            return dict(current) if current is not None else None
        if normalized.startswith("SELECT * FROM daily_expense_day_statuses WHERE tg_user_id=$1 AND day=$2"):
            return self.day_statuses.get((int(args[0]), args[1]))
        if "FROM family_members fm" in normalized and "JOIN families f" in normalized:
            return None
        if "FROM transactions" in normalized and "flow_kind='normal'" in normalized and "type='expense'" in normalized:
            tg_user_id = int(args[0])
            tx_day = args[1]
            for tx in self.transactions:
                if (
                    int(tx.get("created_by_user_id") or tx.get("tg_user_id") or 0) == tg_user_id
                    and tx.get("family_id") is None
                    and tx.get("date") == tx_day
                    and tx.get("flow_kind") == "normal"
                    and tx.get("type") == "expense"
                    and tx.get("deleted_at") is None
                    and not bool(tx.get("is_deleted"))
                ):
                    return {"exists": 1}
            return None
        return None

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return False
        return None


class DummyAcquire:
    def __init__(self, conn: DummyReminderConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> DummyReminderConn:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyPool:
    def __init__(self, conn: DummyReminderConn) -> None:
        self._conn = conn

    def acquire(self) -> DummyAcquire:
        return DummyAcquire(self._conn)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 5, 14, 21, 5, tzinfo=tz)


class ExpenseReminderFlowTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _button_labels(reply_markup) -> list[str]:
        labels: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                labels.append(button.args[0] if getattr(button, "args", None) else "")
        return labels

    def _make_context(self):
        return SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

    def _make_update(self, callback_data: str):
        message = DummyMessage()
        user = SimpleNamespace(id=123, username="tester")
        query = DummyQuery(callback_data, message)
        return SimpleNamespace(effective_user=user, callback_query=query, message=message), message, query

    async def test_settings_menu_contains_evening_reminders_button(self) -> None:
        labels = self._button_labels(bot_main.kb_settings_menu())
        self.assertIn("🌙 Вечірні нагадування", labels)

    async def test_settings_callback_updates_mode_and_hour(self) -> None:
        conn = DummyReminderConn()
        context = self._make_context()
        update, message, query = self._make_update("settings:expense_reminders")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_get_resolved_access_state", new=AsyncMock(return_value={"access_scope": "personal_full"})),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.settings_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("Вечірні нагадування", message.replies[-1]["text"])
        self.assertEqual(conn.reminder_settings[123]["mode"], "daily")
        self.assertEqual(conn.reminder_settings[123]["reminder_hour"], 21)

        mode_update, mode_message, _mode_query = self._make_update("settings:expense_reminders:mode:weekdays")
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_get_resolved_access_state", new=AsyncMock(return_value={"access_scope": "personal_full"})),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.settings_callback(mode_update, context)

        self.assertEqual(conn.reminder_settings[123]["mode"], "weekdays")
        self.assertIn("Будні", mode_message.replies[-1]["text"])

        hour_update, hour_message, _hour_query = self._make_update("settings:expense_reminders:hour:19")
        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_get_resolved_access_state", new=AsyncMock(return_value={"access_scope": "personal_full"})),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.settings_callback(hour_update, context)

        self.assertEqual(conn.reminder_settings[123]["reminder_hour"], 19)
        self.assertIn("19:00", hour_message.replies[-1]["text"])

    async def test_no_expenses_callback_marks_day_and_is_idempotent(self) -> None:
        conn = DummyReminderConn()
        context = self._make_context()
        update, message, query = self._make_update("expense:reminder:no_expenses:2026-05-14")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "datetime", FixedDateTime),
        ):
            await bot_main.expense_reminder_callback(update, context)
            await bot_main.expense_reminder_callback(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(len(conn.day_statuses), 1)
        status = conn.day_statuses[(123, date(2026, 5, 14))]
        self.assertTrue(status["no_expenses_confirmed_at"])
        self.assertIn("Готово, день закритий", message.replies[-1]["text"])

        self.assertEqual(
            self._button_labels(message.replies[-1]["reply_markup"]),
            self._button_labels(bot_main.kb_home()),
        )

    async def test_no_expenses_callback_rejects_stale_day(self) -> None:
        conn = DummyReminderConn()
        context = self._make_context()
        update, message, query = self._make_update("expense:reminder:no_expenses:2026-05-13")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "datetime", FixedDateTime),
        ):
            await bot_main.expense_reminder_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("неактуальне", message.replies[-1]["text"])
        self.assertEqual(
            self._button_labels(message.replies[-1]["reply_markup"]),
            self._button_labels(bot_main.kb_home()),
        )

    async def test_no_expenses_callback_rejects_malformed_day(self) -> None:
        conn = DummyReminderConn()
        context = self._make_context()
        update, message, query = self._make_update("expense:reminder:no_expenses:not-a-date")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
        ):
            await bot_main.expense_reminder_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("Не вдалося розпізнати дату", message.replies[-1]["text"])

    async def test_no_expenses_callback_rejects_when_expense_already_exists(self) -> None:
        conn = DummyReminderConn()
        conn.transactions.append(
            {"tg_user_id": 123, "family_id": None, "date": date(2026, 5, 14), "flow_kind": "normal", "type": "expense", "deleted_at": None}
        )
        context = self._make_context()
        update, message, query = self._make_update("expense:reminder:no_expenses:2026-05-14")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_get_daily_expense_reminder_zone", new=AsyncMock(return_value=(timezone.utc, "Europe/Kyiv"))),
            patch.object(bot_main, "datetime", FixedDateTime),
        ):
            await bot_main.expense_reminder_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("уже є витрата", message.replies[-1]["text"])
        self.assertTrue(conn.day_statuses[(123, date(2026, 5, 14))]["expense_recorded_at"])
