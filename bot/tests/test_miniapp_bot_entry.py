from __future__ import annotations

import os
import sys
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlsplit
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402

bot_main = harness.bot_main


class MiniAppBotEntryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _keyboard_rows(markup):
        inline_keyboard = getattr(markup, "inline_keyboard", None)
        if inline_keyboard is not None:
            return inline_keyboard
        return markup.args[0]

    @staticmethod
    def _is_launch_button(button) -> bool:
        kwargs = getattr(button, "kwargs", None)
        if kwargs is not None:
            return "web_app" in kwargs or "url" in kwargs
        return bool(getattr(button, "web_app", None) or getattr(button, "url", None))

    @staticmethod
    def _button_url(button) -> str:
        kwargs = getattr(button, "kwargs", None)
        if kwargs is not None:
            value = kwargs.get("url")
            return str(value or "")
        return str(getattr(button, "url", "") or "")

    async def test_chat_menu_registers_operator_override_and_keeps_default_app(self) -> None:
        bot = SimpleNamespace(set_chat_menu_button=AsyncMock())
        application = SimpleNamespace(bot=bot)

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
        ):
            await bot_main._register_chat_menu_button(application)

        operator_calls = [call for call in bot.set_chat_menu_button.await_args_list if call.kwargs.get("chat_id")]
        default_calls = [call for call in bot.set_chat_menu_button.await_args_list if "chat_id" not in call.kwargs]
        self.assertEqual(len(operator_calls), 1)
        self.assertEqual(operator_calls[0].kwargs["chat_id"], 7884326049)
        self.assertIn("/app/operator/", operator_calls[0].kwargs["menu_button"].web_app.url)
        self.assertEqual(len(default_calls), 1)
        self.assertIn("/app/", default_calls[0].kwargs["menu_button"].web_app.url)

    async def test_pwa_install_reminder_sends_browser_login_and_marks_delivery(self) -> None:
        class ReminderConn:
            def __init__(self):
                self.executions = []

            async def fetch(self, _query, onboarding_version):
                self.onboarding_version = onboarding_version
                return [{"id": 7, "tg_user_id": 123, "telegram_reminder_count": 0, "lang": "uk"}]

            async def execute(self, query, *args):
                self.executions.append((query, args))

        conn = ReminderConn()
        bot = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(
            application=SimpleNamespace(bot_data={"db_pool": harness.DummyPool(conn)}),
            bot=bot,
        )

        with patch.object(bot_main, "_miniapp_browser_login_url", return_value="https://vydno.capital/app/browser-login/token/"):
            await bot_main._send_pwa_install_reminders(context)

        bot.send_message.assert_awaited_once()
        kwargs = bot.send_message.await_args.kwargs
        self.assertEqual(kwargs["chat_id"], 123)
        self.assertIn("головний екран", kwargs["text"])
        button = self._keyboard_rows(kwargs["reply_markup"])[0][0]
        self.assertEqual(self._button_url(button), "https://vydno.capital/app/browser-login/token/?install=1")
        self.assertTrue(any("telegram_reminder_count = telegram_reminder_count + 1" in query for query, _args in conn.executions))

    async def test_show_miniapp_entry_returns_launch_button_for_full_access(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
            billing_state={"access_mode": "full"},
        )
        message = harness.DummyMessage()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(
                bot_main,
                "_maybe_sync_pending_bind_access_state",
                new=AsyncMock(side_effect=lambda conn, tg_user_id, state: (state, None)),
            ),
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
            patch.object(bot_main, "_show_debt_only_surface", new=AsyncMock()) as debt_only_mock,
        ):
            await bot_main._show_miniapp_entry(message, conn, 123)

        billing_mock.assert_not_awaited()
        debt_only_mock.assert_not_awaited()
        self.assertTrue(message.replies)
        reply_markup = message.replies[-1]["reply_markup"]
        rows = self._keyboard_rows(reply_markup)
        first_button = rows[0][0]
        self.assertTrue(self._is_launch_button(first_button))
        browser_login_url = self._button_url(first_button)
        self.assertIn("/app/browser-login/", browser_login_url)
        self.assertNotIn("tg_user_id=", browser_login_url)

    async def test_show_miniapp_onboarding_entry_bypasses_paywall_and_launches_app(self) -> None:
        message = harness.DummyMessage()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(bot_main, "_set_locale_for_user", new=AsyncMock()),
        ):
            shown = await bot_main._show_miniapp_onboarding_entry(message, object(), 123)

        self.assertTrue(shown)
        self.assertTrue(message.replies)
        reply_markup = message.replies[-1]["reply_markup"]
        first_button = self._keyboard_rows(reply_markup)[0][0]
        self.assertTrue(self._is_launch_button(first_button))
        self.assertIn("/app/browser-login/", self._button_url(first_button))
        self.assertIn("актуальне посилання", message.replies[-1]["text"])

    async def test_show_miniapp_entry_allows_read_only_grace_access(self) -> None:
        now = datetime.now(UTC)
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
            billing_state={
                "subscription_status": "expired",
                "expires_at": now - timedelta(days=1),
                "grace_expires_at": now + timedelta(days=7),
                "last_failure_reason": "card expired",
            },
        )
        message = harness.DummyMessage()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(
                bot_main,
                "_maybe_sync_pending_bind_access_state",
                new=AsyncMock(side_effect=lambda conn, tg_user_id, state: (state, None)),
            ),
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
            patch.object(bot_main, "_show_debt_only_surface", new=AsyncMock()) as debt_only_mock,
        ):
            await bot_main._show_miniapp_entry(message, conn, 123)

        billing_mock.assert_not_awaited()
        debt_only_mock.assert_not_awaited()
        self.assertIn("лише для перегляду", message.replies[-1]["text"])

    async def test_show_miniapp_entry_routes_paywall_users_into_app_billing_flow(self) -> None:
        now = datetime.now(UTC)
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
            billing_state={
                "subscription_status": "expired",
                "expires_at": now - timedelta(days=10),
                "grace_expires_at": now - timedelta(days=1),
                "last_failure_reason": "expired card",
            },
        )
        message = harness.DummyMessage()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(
                bot_main,
                "_maybe_sync_pending_bind_access_state",
                new=AsyncMock(side_effect=lambda conn, tg_user_id, state: (state, None)),
            ),
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
            patch.object(bot_main, "_show_debt_only_surface", new=AsyncMock()) as debt_only_mock,
        ):
            await bot_main._show_miniapp_entry(message, conn, 123)

        billing_mock.assert_not_awaited()
        debt_only_mock.assert_not_awaited()
        self.assertTrue(message.replies)
        self.assertIn("оплата продовжаться вже у застосунку", message.replies[-1]["text"])
        self.assertIn("/app/browser-login/", self._button_url(self._keyboard_rows(message.replies[-1]["reply_markup"])[0][0]))

    async def test_home_callback_incomplete_user_refreshes_app_link_instead_of_showing_paywall(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
            billing_state={"access_mode": "open", "subscription_status": "", "has_card": False},
        )
        message = harness.DummyMessage()
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester", language_code="uk"),
            callback_query=harness.DummyQuery("home:show", message),
            message=message,
        )
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        with (
            patch.object(bot_main, "_pool", return_value=harness.DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_show_miniapp_onboarding_entry", new=AsyncMock(return_value=True)) as onboarding_mock,
            patch.object(bot_main, "_show_access_surface", new=AsyncMock()) as access_mock,
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
        ):
            await bot_main.home_callback(update, context)

        onboarding_mock.assert_awaited_once_with(message, conn, 123)
        access_mock.assert_not_awaited()
        billing_mock.assert_not_awaited()

    async def test_start_entry_ready_paywall_user_gets_fresh_app_entry_instead_of_bot_billing(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
            billing_state={"access_mode": "open", "subscription_status": "", "has_card": False},
        )
        message = harness.DummyMessage(text="/start app_login")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(
                id=123,
                username="tester",
                first_name="Tester",
                last_name=None,
                language_code="uk",
            ),
            message=message,
        )
        context = SimpleNamespace(application=SimpleNamespace(bot=object(), bot_data={}), user_data={}, args=["app_login"])

        with (
            patch.object(bot_main, "_pool", return_value=harness.DummyPool(conn)),
            patch.object(bot_main, "_consume_pending_admin_reset_if_needed", new=AsyncMock()),
            patch.object(bot_main, "is_user_banned", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(bot_main, "is_registration_open", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
            patch.object(bot_main, "log_bot_event", new=AsyncMock()),
            patch.object(bot_main, "_show_miniapp_entry", new=AsyncMock()) as app_entry_mock,
            patch.object(bot_main, "_show_billing_menu", new=AsyncMock()) as billing_mock,
            patch.object(bot_main.ConversationHandler, "END", -1, create=True),
        ):
            state = await bot_main.start_entry(update, context)

        self.assertEqual(state, -1)
        app_entry_mock.assert_awaited_once_with(message, conn, 123)
        billing_mock.assert_not_awaited()

    async def test_reply_home_prepends_miniapp_launch_button(self) -> None:
        message = harness.DummyMessage()

        with patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"):
            await bot_main._reply_home(message, "home")

        reply_markup = message.replies[-1]["reply_markup"]
        first_button = self._keyboard_rows(reply_markup)[0][0]
        self.assertTrue(self._is_launch_button(first_button))

    def test_home_keyboard_uses_launch_button_when_miniapp_url_present(self) -> None:
        with patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"):
            reply_markup = bot_main.kb_home()

        first_button = self._keyboard_rows(reply_markup)[0][0]
        self.assertTrue(self._is_launch_button(first_button))

    async def test_show_settings_menu_includes_miniapp_launch_button(self) -> None:
        message = harness.DummyMessage()
        conn = object()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main, "_get_bot_copy", new=AsyncMock(return_value="settings")),
        ):
            await bot_main._show_settings_menu(message, conn)

        reply_markup = message.replies[-1]["reply_markup"]
        first_button = self._keyboard_rows(reply_markup)[0][0]
        self.assertTrue(self._is_launch_button(first_button))

    def test_with_miniapp_launch_can_append_to_reports_keyboard(self) -> None:
        with patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"):
            reply_markup = bot_main._with_miniapp_launch(bot_main.kb_reports_menu(), prepend=False)

        rows = self._keyboard_rows(reply_markup)
        last_button = rows[-1][0]
        self.assertTrue(self._is_launch_button(last_button))

    def test_with_miniapp_launch_can_prepend_to_export_keyboard(self) -> None:
        with patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"):
            reply_markup = bot_main._with_miniapp_launch(bot_main.kb_export_menu())

        rows = self._keyboard_rows(reply_markup)
        first_button = rows[0][0]
        self.assertTrue(self._is_launch_button(first_button))

    def test_miniapp_url_normalizes_to_single_app_route(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_CACHE_BUST_VERSION", "20260525-miniapp-period-summary-bars1"),
        ):
            resolved = bot_main._miniapp_url()

        parts = urlsplit(resolved)
        self.assertEqual(parts.path, "/app/")
        params = parse_qs(parts.query)
        self.assertEqual(params.get("v"), ["20260525-miniapp-period-summary-bars1"])

    def test_miniapp_url_appends_cache_bust_version(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_CACHE_BUST_VERSION", "20260525-miniapp-period-summary-bars1"),
        ):
            resolved = bot_main._miniapp_url()

        parts = urlsplit(resolved)
        params = parse_qs(parts.query)
        self.assertEqual(parts.path, "/app/")
        self.assertEqual(params.get("v"), ["20260525-miniapp-period-summary-bars1"])

    def test_miniapp_url_preserves_existing_query_params_without_stale_v(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app-shell/?source=bot&v=stale"),
            patch.object(bot_main.config, "MINIAPP_CACHE_BUST_VERSION", "20260525-miniapp-period-summary-bars1"),
        ):
            resolved = bot_main._miniapp_url()

        parts = urlsplit(resolved)
        params = parse_qs(parts.query)
        self.assertEqual(parts.path, "/app/")
        self.assertEqual(params.get("source"), ["bot"])
        self.assertEqual(params.get("v"), ["20260525-miniapp-period-summary-bars1"])

    def test_operator_url_uses_dedicated_app_route_for_allowlisted_admin(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
        ):
            self.assertTrue(bot_main._is_miniapp_operator(7884326049))
            self.assertFalse(bot_main._is_miniapp_operator(123))
            self.assertEqual(urlsplit(bot_main._miniapp_operator_url()).path, "/app/operator/")

    async def test_operator_entry_bypasses_finance_access_and_opens_admin_hub(self) -> None:
        message = harness.DummyMessage()

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(bot_main, "_set_locale_for_user", new=AsyncMock()),
            patch.object(bot_main, "kb_dashboard_launch_menu", wraps=bot_main.kb_dashboard_launch_menu) as menu_builder,
        ):
            await bot_main._show_miniapp_entry(message, object(), 7884326049)

        self.assertIn("Vydno Admin Hub", message.replies[-1]["text"])
        self.assertIn("/app/operator/", menu_builder.call_args.kwargs["url"])
        rows = self._keyboard_rows(message.replies[-1]["reply_markup"])
        urls = []
        for row in rows:
            for button in row:
                url = self._button_url(button)
                web_app = getattr(button, "web_app", None)
                if web_app is not None:
                    url = str(getattr(web_app, "url", "") or url)
                urls.append(url)
        self.assertTrue(any("operator=1" in url for url in urls))

    def test_miniapp_browser_login_url_uses_signed_token_path_without_query_identity(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/?source=bot&v=stale"),
            patch.object(bot_main.config, "MINIAPP_CACHE_BUST_VERSION", "20260525-miniapp-period-summary-bars1"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(bot_main.config, "MINIAPP_BROWSER_LOGIN_SECRET", None),
            patch.object(bot_main.config, "MINIAPP_BROWSER_LOGIN_TTL_SECONDS", 900),
        ):
            resolved = bot_main._miniapp_browser_login_url(123)

        parts = urlsplit(resolved)
        self.assertEqual(parts.scheme, "https")
        self.assertEqual(parts.netloc, "vydno.capital")
        self.assertTrue(parts.path.startswith("/app/browser-login/"))
        self.assertEqual(parts.query, "")
        token = unquote(parts.path.removeprefix("/app/browser-login/").removesuffix("/"))
        self.assertIn(".", token)
        self.assertNotIn("tg_user_id=", resolved)

    def test_operator_browser_login_url_keeps_identity_signed_and_sets_operator_target(self) -> None:
        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main.config, "BOT_TOKEN", "123456:test-miniapp-token"),
            patch.object(bot_main.config, "MINIAPP_BROWSER_LOGIN_SECRET", None),
        ):
            resolved = bot_main._miniapp_browser_login_url(7884326049, operator=True)

        self.assertEqual(parse_qs(urlsplit(resolved).query), {"operator": ["1"]})
        self.assertNotIn("tg_user_id=", resolved)

    async def test_export_callback_success_appends_miniapp_launch_button(self) -> None:
        today = bot_main.datetime.now().date()
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "starting_balance": Decimal("0"),
                    "balance": Decimal("0"),
                    "is_active": True,
                    "created_at": datetime(2026, 1, 1),
                }
            ],
            transactions=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "date": today,
                    "type": "income",
                    "amount": Decimal("100"),
                    "currency": "UAH",
                    "account_id": 1,
                    "category_id": 11,
                    "category_name_snapshot": "Salary",
                    "flow_kind": "normal",
                    "comment": "Salary",
                }
            ],
        )
        message = harness.DummyMessage()
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123, username="tester"),
            callback_query=harness.DummyQuery("export:today", message),
            message=message,
        )
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        with (
            patch.object(bot_main.config, "MINIAPP_URL", "https://vydno.capital/app/"),
            patch.object(bot_main, "_pool", return_value=harness.DummyPool(conn)),
            patch.object(bot_main, "_user_ready", new=AsyncMock(return_value=True)),
        ):
            await bot_main.export_callback(update, context)

        reply_markup = message.replies[-1]["reply_markup"]
        last_button = self._keyboard_rows(reply_markup)[-1][0]
        self.assertTrue(self._is_launch_button(last_button))


if __name__ == "__main__":
    unittest.main()
