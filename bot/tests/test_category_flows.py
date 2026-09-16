from __future__ import annotations

import os
import sys
import types
import unittest
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
    ext_stub.ConversationHandler = types.SimpleNamespace(END=-1)
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
from category_ui import category_edit_keyboard  # noqa: E402
from test_category_services import MemoryConn  # noqa: E402


class DummyMessage:
    def __init__(self, text: str = "") -> None:
        self.text = text
        self.replies: list[dict] = []
        self.chat = SimpleNamespace(id=777, send_action=AsyncMock())
        self.message_id = 555

    async def reply_text(self, text: str, reply_markup=None, **kwargs) -> None:
        self.replies.append({"text": text, "reply_markup": reply_markup, "kwargs": kwargs})


class DummyQuery:
    def __init__(self, data: str, message: DummyMessage) -> None:
        self.data = data
        self.message = message
        self.answered = False

    async def answer(self) -> None:
        self.answered = True


class DummyPool:
    def __init__(self, conn) -> None:
        self._conn = conn

    def acquire(self):
        return DummyAcquire(self._conn)


class DummyAcquire:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class CategoryFlowTests(unittest.IsolatedAsyncioTestCase):
    def _make_context(self):
        return SimpleNamespace(application=SimpleNamespace(bot_data={}, bot=AsyncMock()), user_data={})

    def _make_update(self, data: str, *, user_id: int = 111):
        message = DummyMessage()
        query = DummyQuery(data, message)
        return SimpleNamespace(effective_user=SimpleNamespace(id=user_id, username="tester"), callback_query=query, message=message), message, query

    def _flatten_callbacks(self, markup) -> list[str]:
        if markup is None:
            return []
        rows = markup.args[0] if getattr(markup, "args", None) else markup.kwargs.get("inline_keyboard", [])
        callbacks: list[str] = []
        for row in rows:
            for button in row:
                callback = getattr(button, "kwargs", {}).get("callback_data")
                if callback:
                    callbacks.append(callback)
        return callbacks

    def test_onboarding_confirm_keyboard_has_no_category_edit_button(self) -> None:
        callbacks = self._flatten_callbacks(bot_main.onboarding_confirm_keyboard())
        self.assertIn("onb:confirm:ok", callbacks)
        self.assertIn("onb:confirm:edit", callbacks)
        self.assertNotIn("onb:confirm:edit_categories", callbacks)

    def test_expense_category_card_is_view_only(self) -> None:
        callbacks = self._flatten_callbacks(category_edit_keyboard({"id": 1, "type": "expense"}))
        self.assertEqual(callbacks, ["categories:list:expense", "menu:main"])

    def test_categories_home_keyboard_targets_income_management_only(self) -> None:
        callbacks = self._flatten_callbacks(bot_main.categories_home_keyboard())
        self.assertIn("categories:list:expense", callbacks)
        self.assertIn("categories:list:income", callbacks)
        self.assertIn("categories:add:type:income", callbacks)
        self.assertIn("categories:edit:type:income", callbacks)
        self.assertNotIn("categories:add:start", callbacks)
        self.assertNotIn("categories:edit:start", callbacks)

    async def test_onb_accounts_done_skips_category_step(self) -> None:
        context = self._make_context()
        context.user_data["tg_user_id"] = 111
        update, message, query = self._make_update("onb:acct:done")

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_pool", return_value=DummyPool(MemoryConn())),
            patch.object(bot_main, "_get_accounts", new=AsyncMock(return_value=[(1, "Monobank")])),
            patch.object(bot_main, "_show_onboarding_summary", new=AsyncMock(return_value=bot_main.ONB_CONFIRM)) as summary_mock,
            patch.object(bot_main, "_sync_onboarding_debug", new=AsyncMock()),
        ):
            state = await bot_main.onb_accounts_more_done(update, context)

        self.assertTrue(query.answered)
        self.assertEqual(state, bot_main.ONB_CONFIRM)
        summary_mock.assert_awaited_once()
        self.assertEqual(message.replies, [])

    async def test_categories_add_expense_callback_is_blocked(self) -> None:
        context = self._make_context()
        update, message, query = self._make_update("categories:add:type:expense")

        with (
            patch.object(bot_main, "_pool", return_value=DummyPool(MemoryConn())),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_get_resolved_access_state", new=AsyncMock(return_value={"access_scope": "personal_full", "access_source": "billing"})),
            patch.object(bot_main, "_billing_write_blocked", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_ensure_default_categories", new=AsyncMock()),
        ):
            await bot_main.categories_callback(update, context)

        self.assertTrue(query.answered)
        self.assertIn("не потрібно", message.replies[-1]["text"])

    def test_onboarding_state_registration_no_longer_contains_category_step(self) -> None:
        with open(bot_main.__file__, "r", encoding="utf-8") as fh:
            source = fh.read()
        self.assertNotIn("ONB_CATS: [", source)
        self.assertNotIn('CallbackQueryHandler(onb_categories, pattern=r"^(ob_cat_mode:', source)


if __name__ == "__main__":
    unittest.main()
