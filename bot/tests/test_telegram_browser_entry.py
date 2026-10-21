from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402
import test_miniapp_bot_entry as entry_tests  # noqa: E402

bot_main = harness.bot_main


class TelegramBrowserEntryTests(unittest.IsolatedAsyncioTestCase):
    async def test_cabinet_delivers_identity_free_login_without_issuance_or_signing_secret(self):
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
            billing_state={"access_mode": "full"},
        )
        message = harness.DummyMessage()
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/?source=bot&v=old"),
            patch.object(bot_main.config, "MINIAPP_BROWSER_LOGIN_SECRET", ""),
            patch.object(bot_main.config, "BOT_TOKEN", ""),
            patch.object(bot_main, "_server_issued_browser_login_url", new=AsyncMock(side_effect=AssertionError("Magic-link issuance must not run"))) as issuer,
            patch.object(bot_main, "_set_locale_for_user", new=AsyncMock()),
            patch.object(bot_main, "_maybe_sync_pending_bind_access_state", new=AsyncMock(side_effect=lambda conn, tg_user_id, state: (state, None))),
        ):
            await bot_main._show_miniapp_entry(message, conn, 123)
        issuer.assert_not_awaited()
        rows = entry_tests.MiniAppBotEntryTests._keyboard_rows(message.replies[-1]["reply_markup"])
        urls = [entry_tests.MiniAppBotEntryTests._button_url(button) for row in rows for button in row]
        self.assertIn("https://vydno.capital/app/login/", urls)
        self.assertNotIn("Не вдалося", message.replies[-1]["text"])

    def test_login_url_preserves_only_safe_mode_flags(self):
        with patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/?tg_user_id=999&next=https://example.com#token"):
            self.assertEqual(bot_main._miniapp_telegram_login_url(), "https://vydno.capital/app/login/")
            self.assertEqual(bot_main._miniapp_telegram_login_url(operator=True), "https://vydno.capital/app/login/?operator=1")
            self.assertEqual(bot_main._miniapp_telegram_login_url(install=True), "https://vydno.capital/app/login/?install=1")

    async def test_install_reminder_sends_public_login_without_issuance(self):
        conn = SimpleNamespace(
            fetch=AsyncMock(return_value=[{"id": 1, "tg_user_id": 123, "telegram_reminder_count": 0, "lang": "uk"}]),
            execute=AsyncMock(),
        )
        sender = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={"db_pool": harness.DummyPool(conn)}),
            bot=sender,
        )
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_BROWSER_LOGIN_SECRET", ""),
            patch.object(bot_main, "_server_issued_browser_login_url", new=AsyncMock(side_effect=AssertionError("No issuance"))) as issuer,
        ):
            await bot_main._send_pwa_install_reminders(context)
        issuer.assert_not_awaited()
        sender.send_message.assert_awaited_once()
        markup = sender.send_message.await_args.kwargs["reply_markup"]
        button = entry_tests.MiniAppBotEntryTests._keyboard_rows(markup)[0][0]
        self.assertEqual(entry_tests.MiniAppBotEntryTests._button_url(button), "https://vydno.capital/app/login/?install=1")

    def test_missing_or_insecure_app_origin_does_not_create_a_login_link(self):
        for url in ("", "http://vydno.capital/app/", "javascript:alert(1)", "https:///app/"):
            with self.subTest(url=url), patch.object(bot_main.config, "MINIAPP_URL", url):
                self.assertEqual(bot_main._miniapp_telegram_login_url(), "")


if __name__ == "__main__":
    unittest.main()
