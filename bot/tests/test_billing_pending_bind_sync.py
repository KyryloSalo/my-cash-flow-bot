from __future__ import annotations

import os
import sys
import unittest
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402

bot_main = harness.bot_main


class PendingBindSyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_show_billing_menu_syncs_pending_bind_success_and_opens_home(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "profile_status": "pending",
                "has_card": False,
                "masked_pan": "",
                "auto_renew_enabled": False,
                "last_action_url": "https://pay.monobank.ua/test-bind",
                "last_failure_reason": "",
                "subscription_status": "",
                "trial_days": 0,
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "billing", "pending_start_payload": ""},
        )
        message = harness.DummyMessage()

        async def sync_side_effect(*, tg_user_id: int):
            self.assertEqual(tg_user_id, 123)
            expiry = datetime(2030, 1, 1, tzinfo=UTC)
            conn.billing_state.update(
                {
                    "profile_status": "active",
                    "has_card": True,
                    "masked_pan": "444455******1111",
                    "auto_renew_enabled": True,
                    "last_action_url": "",
                    "subscription_status": "trial",
                    "trial_days": 30,
                    "expires_at": expiry,
                    "next_charge_at": expiry,
                    "access_mode": "full",
                }
            )
            conn.access_state.update({"access_scope": "personal_full", "access_source": "billing"})
            return {"updated": True, "monobank_status": "success"}

        with (
            patch.object(bot_main, "sync_pending_bind_status", new=AsyncMock(side_effect=sync_side_effect)) as sync_mock,
            patch.object(bot_main, "_maybe_resume_pending_start_payload", new=AsyncMock(return_value=False)),
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as reply_home_mock,
        ):
            await bot_main._show_billing_menu(message, conn, 123)

        sync_mock.assert_awaited_once()
        reply_home_mock.assert_awaited_once()
        self.assertEqual(message.replies, [])
        self.assertIn("Оплату підтверджено", reply_home_mock.await_args.args[1])
 
    async def test_show_billing_menu_for_full_access_user_stays_on_billing_screen(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": True,
                "profile_status": "active",
                "has_card": True,
                "masked_pan": "444455******1111",
                "auto_renew_enabled": True,
                "last_action_url": "",
                "last_failure_reason": "",
                "subscription_status": "trial",
                "trial_days": 30,
                "access_mode": "full",
            },
            access_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
        )
        message = harness.DummyMessage()

        with (
            patch.object(bot_main, "sync_pending_bind_status", new=AsyncMock()) as sync_mock,
            patch.object(bot_main, "_reply_home", new=AsyncMock()) as reply_home_mock,
        ):
            await bot_main._show_billing_menu(message, conn, 123)

        sync_mock.assert_not_awaited()
        reply_home_mock.assert_not_awaited()
        self.assertTrue(message.replies)
        self.assertIn("Доступ і оплата", message.replies[-1]["text"])
    async def test_show_billing_menu_uses_ninety_day_bind_button_for_course_payload(self) -> None:
        conn = harness.LedgerConn(
            tg_user_id=123,
            accounts=[],
            billing_state={
                "profile_exists": False,
                "has_card": False,
                "masked_pan": "",
                "profile_status": "",
                "auto_renew_enabled": False,
                "last_charge_status": "",
                "last_failure_reason": "",
                "last_action_url": "",
                "subscription_status": "",
                "expires_at": None,
                "next_charge_at": None,
                "grace_expires_at": None,
                "trial_days": 0,
                "access_mode": "open",
            },
            access_state={"access_scope": "paywall", "access_source": "promo", "pending_start_payload": "course"},
        )
        message = harness.DummyMessage()

        await bot_main._show_billing_menu(message, conn, 123)

        self.assertTrue(message.replies)
        labels = harness.AccountLifecycleRegressionTests._button_labels(message.replies[-1]["reply_markup"])
        self.assertIn("Почати 90 днів (1 грн)", labels)


if __name__ == "__main__":
    unittest.main()
