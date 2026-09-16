from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402

bot_main = harness.bot_main


class HelpFlowTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _keyboard_rows(markup):
        inline_keyboard = getattr(markup, "inline_keyboard", None)
        if inline_keyboard is not None:
            return inline_keyboard
        return getattr(markup, "args", [[]])[0]

    @classmethod
    def _button_labels(cls, markup) -> list[str]:
        labels: list[str] = []
        for row in cls._keyboard_rows(markup):
            for button in row:
                raw = button.args[0] if getattr(button, "args", None) else ""
                labels.append(str(raw).replace("\u2800", "").strip())
        return labels

    @staticmethod
    def _button_callback(button):
        kwargs = getattr(button, "kwargs", None)
        if kwargs is not None:
            return kwargs.get("callback_data")
        return getattr(button, "callback_data", None)

    @staticmethod
    def _button_url(button):
        kwargs = getattr(button, "kwargs", None)
        if kwargs is not None:
            return kwargs.get("url")
        return getattr(button, "url", None)

    @classmethod
    def _flatten_callbacks(cls, markup) -> list[str]:
        callbacks: list[str] = []
        for row in cls._keyboard_rows(markup):
            for button in row:
                callback = cls._button_callback(button)
                if callback:
                    callbacks.append(str(callback))
        return callbacks

    def _make_context(self, conn: harness.LedgerConn | None = None):
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={}),
            user_data={},
        )
        if conn is not None:
            context.application.bot_data["db_pool"] = harness.DummyPool(conn)
        return context

    async def test_help_command_shows_faq_home_and_resets_runtime_flows(self) -> None:
        context = self._make_context()
        context.user_data["support_flow"] = {"await_text": True}
        context.user_data["tx_flow"] = {"kind": "expense"}
        message = harness.DummyMessage(text="/help")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=message,
            callback_query=None,
        )

        with patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()) as reset_mock:
            await bot_main.help_command(update, context)

        reset_mock.assert_awaited_once_with(context, 123)
        self.assertNotIn("support_flow", context.user_data)
        self.assertNotIn("tx_flow", context.user_data)

        catalog = bot_main.get_faq_catalog("uk")
        reply = message.replies[-1]
        self.assertIn(catalog.home_title, reply["text"])
        self.assertIn(catalog.home_intro.splitlines()[0], reply["text"])
        callbacks = self._flatten_callbacks(reply["reply_markup"])
        self.assertIn("help:support", callbacks)
        self.assertIn("help:back", callbacks)
        self.assertIn("menu:main", callbacks)

    async def test_help_command_uses_english_catalog_for_english_locale(self) -> None:
        context = self._make_context()
        context.user_data["locale"] = "en"
        message = harness.DummyMessage(text="/help")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            message=message,
            callback_query=None,
        )

        with patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()):
            await bot_main.help_command(update, context)

        catalog = bot_main.get_faq_catalog("en")
        reply = message.replies[-1]
        self.assertIn(catalog.home_title, reply["text"])
        self.assertIn(catalog.home_intro.splitlines()[0], reply["text"])
        self.assertEqual(
            self._button_labels(reply["reply_markup"])[-3:],
            ["Contact support", "⬅️ Back", "🏠 Home"],
        )

    async def test_help_callback_home_is_available_for_paywall_user_without_onboarding_check(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
            billing_state={"access_mode": "blocked", "profile_exists": False, "has_card": False, "subscription_status": ""},
        )
        context = self._make_context(conn)
        context.user_data["support_flow"] = {"await_text": True}
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:home", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=False)) as user_ready_mock,
        ):
            await bot_main.help_callback(update, context)

        self.assertTrue(query.answered)
        user_ready_mock.assert_not_awaited()
        self.assertNotIn("support_flow", context.user_data)
        self.assertIn(bot_main.get_faq_catalog("uk").home_title, message.replies[-1]["text"])

    async def test_help_topic_screen_lists_questions_and_navigation(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:topic:getting_started", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
        ):
            await bot_main.help_callback(update, context)

        catalog = bot_main.get_faq_catalog("uk")
        topic = bot_main.get_faq_topic("getting_started", locale="uk")
        self.assertIsNotNone(topic)
        labels = self._button_labels(message.replies[-1]["reply_markup"])
        self.assertIn(topic.questions[0].title, labels)
        callbacks = self._flatten_callbacks(message.replies[-1]["reply_markup"])
        self.assertIn("help:home", callbacks)
        self.assertIn("help:support", callbacks)
        self.assertIn("menu:main", callbacks)
        self.assertIn(topic.title, message.replies[-1]["text"])

    async def test_help_answer_screen_shows_answer_and_back_to_topic(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:q:getting_started:what_is_vydno", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
        ):
            await bot_main.help_callback(update, context)

        _topic, question = bot_main.get_faq_question("getting_started", "what_is_vydno", locale="uk")
        self.assertIn(question.title, message.replies[-1]["text"])
        self.assertIn(question.answer.splitlines()[0], message.replies[-1]["text"])
        callbacks = self._flatten_callbacks(message.replies[-1]["reply_markup"])
        self.assertIn("help:topic:getting_started", callbacks)
        self.assertIn("help:home", callbacks)
        self.assertIn("help:support", callbacks)
        self.assertIn("menu:main", callbacks)

    async def test_help_answer_screen_shows_english_question_and_answer(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        context.user_data["locale"] = "en"
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:q:getting_started:what_is_vydno", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
        ):
            await bot_main.help_callback(update, context)

        _topic, question = bot_main.get_faq_question("getting_started", "what_is_vydno", locale="en")
        self.assertIn(question.title, message.replies[-1]["text"])
        self.assertIn("Telegram bot and Mini App for personal finance tracking", message.replies[-1]["text"])
        self.assertEqual(
            self._button_labels(message.replies[-1]["reply_markup"]),
            ["Back to topic", "All help topics", "Contact support", "🏠 Home"],
        )

    async def test_help_support_screen_uses_configured_url(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:support", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main.config, "SUPPORT_CONTACT_URL", "https://t.me/Askills_Support"),
        ):
            await bot_main.help_callback(update, context)

        rows = self._keyboard_rows(message.replies[-1]["reply_markup"])
        self.assertEqual(self._button_url(rows[0][0]), "https://t.me/Askills_Support")

    async def test_help_support_screen_without_url_shows_fallback_text(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:support", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main.config, "SUPPORT_CONTACT_URL", None),
        ):
            await bot_main.help_callback(update, context)

        callbacks = self._flatten_callbacks(message.replies[-1]["reply_markup"])
        self.assertIn("help:home", callbacks)
        self.assertIn("menu:main", callbacks)
        self.assertIn(bot_main.get_faq_catalog("uk").support_missing_url_text.splitlines()[0], message.replies[-1]["text"])

    async def test_help_back_uses_access_surface_for_ready_user(self) -> None:
        conn = harness.LedgerConn(tg_user_id=123, accounts=[])
        context = self._make_context(conn)
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:back", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_show_access_surface", new=AsyncMock()) as access_mock,
        ):
            await bot_main.help_callback(update, context)

        access_mock.assert_awaited_once_with(message, conn, 123)

    async def test_help_back_uses_access_surface_for_paywall_user_without_onboarding_check(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        context = self._make_context(conn)
        context.user_data["support_flow"] = {"await_text": True}
        message = harness.DummyMessage()
        query = harness.DummyQuery("help:back", message)
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123, username="tester"), callback_query=query, message=None)

        with (
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=False)) as user_ready_mock,
            patch.object(bot_main, "_show_access_surface", new=AsyncMock()) as access_mock,
        ):
            await bot_main.help_callback(update, context)

        self.assertTrue(query.answered)
        user_ready_mock.assert_not_awaited()
        self.assertNotIn("support_flow", context.user_data)
        access_mock.assert_awaited_once_with(message, conn, 123)

    def test_home_keyboards_use_new_help_namespace(self) -> None:
        for markup in (bot_main.kb_home_more_menu(), bot_main.kb_paywall_home(), bot_main.kb_debt_only_home()):
            callbacks = [
                self._button_callback(button)
                for row in self._keyboard_rows(markup)
                for button in row
                if self._button_callback(button) == "help:home"
            ]
            self.assertEqual(callbacks, ["help:home"])


if __name__ == "__main__":
    unittest.main()
