from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Pool = object
    asyncpg_stub.Record = object
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
    ext_stub.Application = object
    ext_stub.CallbackQueryHandler = object
    ext_stub.CommandHandler = object
    ext_stub.ContextTypes = types.SimpleNamespace(DEFAULT_TYPE=object)
    ext_stub.Defaults = object
    ext_stub.ConversationHandler = object
    ext_stub.MessageHandler = object
    ext_stub.filters = types.SimpleNamespace(TEXT=object(), COMMAND=object(), VOICE=object())
    sys.modules["telegram.ext"] = ext_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")
    httpx_stub.AsyncClient = object
    sys.modules["httpx"] = httpx_stub

import bot_main  # noqa: E402


class DummyMessage:
    def __init__(self) -> None:
        self.replies: list[dict] = []
        self.chat = SimpleNamespace(send_action=AsyncMock())

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})


class DummyConn:
    def __init__(self, *, accounts: list[dict], history_rows: list[dict] | None = None) -> None:
        self.accounts = [dict(row) for row in accounts]
        self.history_rows = history_rows or []

    async def execute(self, query: str, *args) -> None:
        return None

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM family_members" in normalized and "JOIN families" in normalized:
            return None
        if "FROM accounts" in normalized and "id=$2" in normalized:
            account_id = int(args[1])
            for row in self.accounts:
                if int(row["id"]) == account_id and row.get("is_active", True):
                    return dict(row)
        return None

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM accounts" in normalized:
            return [dict(row) for row in self.accounts if row.get("is_active", True)]
        if "FROM transactions" in normalized and "transfer_subtype IN ('savings_transfer', 'savings_withdraw')" in normalized:
            return list(self.history_rows)
        return []


class SavingsOverviewTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _button_labels(reply_markup) -> list[str]:
        labels: list[str] = []
        rows = reply_markup.args[0] if reply_markup and getattr(reply_markup, "args", None) else []
        for row in rows:
            for button in row:
                labels.append((button.args[0] if getattr(button, "args", None) else "").strip(" \u2800"))
        return labels

    @staticmethod
    def _context():
        return SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

    async def test_overview_empty_shows_create_accumulation_button(self) -> None:
        conn = DummyConn(accounts=[])
        message = DummyMessage()

        text, has_assets = await bot_main._render_savings_overview_text(conn, 123)
        await bot_main._show_savings_overview(message, self._context(), 123, conn=conn)

        self.assertFalse(has_assets)
        self.assertIn("Створіть перший рахунок накопичень", text)
        labels = self._button_labels(message.replies[0]["reply_markup"])
        self.assertIn("Додати рахунок", labels)

    async def test_overview_active_shows_topup_and_hides_plan_first_action(self) -> None:
        conn = DummyConn(
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "На подорож",
                    "currency": "UAH",
                    "account_type": "savings",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "goal_amount": Decimal("10000"),
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ]
        )
        message = DummyMessage()

        text, has_assets = await bot_main._render_savings_overview_text(conn, 123)
        await bot_main._show_savings_overview(message, self._context(), 123, conn=conn)

        self.assertTrue(has_assets)
        self.assertIn("Накопичення:", text)
        self.assertIn("Депозити:", text)
        self.assertIn("Інвестиції:", text)
        self.assertNotIn("Заплановано відкласти", text)
        labels = self._button_labels(message.replies[0]["reply_markup"])
        self.assertIn("Поповнити", labels)
        self.assertIn("Мої рахунки", labels)
        self.assertIn("Додати рахунок", labels)
        self.assertNotIn("Депозити", labels)
        self.assertNotIn("Інвестиції", labels)
        self.assertNotIn("Запланувати відкладення", labels)

    async def test_savings_list_includes_deposit_and_investment_accounts(self) -> None:
        conn = DummyConn(
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Подушка",
                    "currency": "UAH",
                    "account_type": "savings",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("5000"),
                    "goal_amount": None,
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                },
                {
                    "id": 2,
                    "tg_user_id": 123,
                    "label": "Річний депозит",
                    "currency": "UAH",
                    "account_type": "deposit",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("12000"),
                    "goal_amount": None,
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 2),
                },
                {
                    "id": 3,
                    "tg_user_id": 123,
                    "label": "BTC",
                    "currency": "USD",
                    "account_type": "investment",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("300"),
                    "goal_amount": None,
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 3),
                },
            ]
        )
        message = DummyMessage()

        await bot_main._show_savings_list(message, self._context(), 123, conn=conn)

        reply = message.replies[0]
        self.assertIn("Мої рахунки накопичень", reply["text"])
        self.assertIn("Подушка", reply["text"])
        self.assertIn("Річний депозит", reply["text"])
        self.assertIn("BTC", reply["text"])
        self.assertIn("Депозит", reply["text"])
        self.assertIn("Інвестиційний рахунок", reply["text"])

    async def test_savings_detail_accepts_deposit_account(self) -> None:
        conn = DummyConn(
            accounts=[
                {
                    "id": 2,
                    "tg_user_id": 123,
                    "label": "Річний депозит",
                    "currency": "UAH",
                    "account_type": "deposit",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("12000"),
                    "goal_amount": Decimal("15000"),
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 2),
                }
            ]
        )
        message = DummyMessage()

        await bot_main._show_savings_detail(message, self._context(), 123, 2, conn=conn)

        reply = message.replies[0]
        self.assertIn("Річний депозит", reply["text"])
        self.assertIn("Тип: Депозит", reply["text"])
        self.assertNotIn("не знайдено", reply["text"])

    async def test_progress_can_exceed_100_percent(self) -> None:
        conn = DummyConn(
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "На подорож",
                    "currency": "UAH",
                    "account_type": "savings",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("12000"),
                    "goal_amount": Decimal("10000"),
                    "goal_date": None,
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ]
        )

        text, _ = await bot_main._render_savings_overview_text(conn, 123)

        self.assertIn("120", text)
        self.assertIn("Ціль виконана. Можна відкладати далі.", text)


if __name__ == "__main__":
    unittest.main()
