from __future__ import annotations

import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

import admin_integrations  # noqa: E402


class AdminIntegrationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        admin_integrations._KNOWN_TABLES.clear()

    async def test_notify_admins_skips_excluded_chat_ids(self) -> None:
        original_enabled = admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED
        original_ids = admin_integrations.config.ADMIN_TELEGRAM_IDS
        bot = SimpleNamespace(send_message=AsyncMock())
        try:
            admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED = True
            admin_integrations.config.ADMIN_TELEGRAM_IDS = [111, 222]

            await admin_integrations.notify_admins(bot, "hello", exclude_chat_ids={111})
        finally:
            admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED = original_enabled
            admin_integrations.config.ADMIN_TELEGRAM_IDS = original_ids

        bot.send_message.assert_awaited_once_with(chat_id=222, text="hello")

    async def test_notify_admins_supports_explicit_owner_ids_and_message_controls(self) -> None:
        original_enabled = admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED
        original_ids = admin_integrations.config.ADMIN_TELEGRAM_IDS
        bot = SimpleNamespace(send_message=AsyncMock())
        reply_markup = object()
        try:
            admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED = True
            admin_integrations.config.ADMIN_TELEGRAM_IDS = []

            await admin_integrations.notify_admins(
                bot,
                "owner alert",
                chat_ids=[7884326049, 7884326049],
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        finally:
            admin_integrations.config.ADMIN_NOTIFICATIONS_ENABLED = original_enabled
            admin_integrations.config.ADMIN_TELEGRAM_IDS = original_ids

        bot.send_message.assert_awaited_once_with(
            chat_id=7884326049,
            text="owner alert",
            parse_mode="HTML",
            reply_markup=reply_markup,
        )

    async def test_log_bot_event_insert_includes_legacy_admin_defaults(self) -> None:
        class DummyConn:
            def __init__(self) -> None:
                self.queries: list[str] = []

            async def fetchval(self, query: str, *args):
                return True

            async def execute(self, query: str, *args) -> None:
                self.queries.append(" ".join(query.split()))

        conn = DummyConn()

        await admin_integrations.log_bot_event(conn, 123, "text_received", raw_input="hello")

        user_queries = [query for query in conn.queries if "INSERT INTO user_admin_states" in query]
        self.assertEqual(len(user_queries), 1)
        query = user_queries[0]
        self.assertIn("access_scope", query)
        self.assertIn("current_fsm_state", query)
        self.assertIn("onboarding_payload", query)
        self.assertIn("pending_admin_reset_mode", query)

    async def test_sync_onboarding_debug_state_insert_includes_access_defaults(self) -> None:
        class DummyConn:
            def __init__(self) -> None:
                self.queries: list[str] = []

            async def fetchval(self, query: str, *args):
                return True

            async def execute(self, query: str, *args) -> None:
                self.queries.append(" ".join(query.split()))

        conn = DummyConn()

        await admin_integrations.sync_onboarding_debug_state(
            conn,
            123,
            current_fsm_state="onboarding:currency",
            onboarding_payload={"step": "currency"},
        )

        user_queries = [query for query in conn.queries if "INSERT INTO user_admin_states" in query]
        self.assertEqual(len(user_queries), 1)
        query = user_queries[0]
        self.assertIn("access_scope", query)
        self.assertIn("pending_start_payload", query)
        self.assertIn("is_test_user", query)


if __name__ == "__main__":
    unittest.main()
