from __future__ import annotations

import os
import sys
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

import test_account_lifecycle_regression as harness  # noqa: E402

import trial_recovery  # noqa: E402


bot_main = harness.bot_main


def recovery_snapshot(**overrides):
    now = datetime(2026, 8, 28, 12, 30, tzinfo=UTC)
    snapshot = {
        "recipient_id": 17,
        "campaign_id": 2,
        "tg_user_id": 123456,
        "first_name": "Олена",
        "last_name": "Тестова",
        "username": "olena_test",
        "lang": "uk",
        "base_currency": "UAH",
        "onboarding_completed": True,
        "onboarding_version": 3,
        "user_created_at": now,
        "last_seen_at": now,
        "user_status": "active",
        "subscription_status": "none",
        "access_scope": "paywall",
        "access_source": "billing",
        "can_receive_messages": True,
        "blocked_bot": False,
        "recovery_status": "responded",
        "recovery_source": "backfill",
        "reason": "price",
        "free_text": "499 грн дорого",
        "first_sent_at": now,
        "last_sent_at": now,
        "sent_count": 1,
        "responded_at": now,
        "contact_requested_at": None,
        "opted_out_at": None,
        "support_case_id": None,
    }
    snapshot.update(overrides)
    return snapshot


class TrialRecoveryOwnerFlowTests(unittest.IsolatedAsyncioTestCase):
    async def test_reason_callback_wires_new_response_to_owner_notification(self) -> None:
        conn = object()
        message = harness.DummyMessage()
        query = harness.DummyQuery("trialrec:r:17:price", message)
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=123456, username="olena_test", language_code="uk"),
            callback_query=query,
        )
        context = SimpleNamespace(
            application=SimpleNamespace(bot=SimpleNamespace(), bot_data={"db_pool": harness.DummyPool(conn)}),
            user_data={},
        )

        with (
            patch.object(bot_main, "_activate_locale", new=AsyncMock(return_value="uk")),
            patch.object(bot_main, "should_block_for_maintenance", new=AsyncMock(return_value=None)),
            patch.object(
                bot_main,
                "record_trial_recovery_reason",
                new=AsyncMock(return_value={"id": 17, "reason": "price", "notification_required": True}),
            ),
            patch.object(bot_main, "_notify_trial_recovery_owners", new=AsyncMock()) as notify_mock,
        ):
            await bot_main.trial_recovery_callback(update, context)

        notify_mock.assert_awaited_once()
        self.assertEqual(notify_mock.await_args.kwargs["event"], "reason")
        self.assertEqual(notify_mock.await_args.kwargs["recipient_id"], 17)
        self.assertIn("499 грн", message.replies[-1]["text"])

    async def test_owner_reply_callback_rejects_non_operator_before_database_access(self) -> None:
        message = harness.DummyMessage()
        query = harness.DummyQuery("trialrec:a:17", message)
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=555, username="not_owner"),
            callback_query=query,
        )
        context = SimpleNamespace(application=SimpleNamespace(bot_data={}), user_data={})

        with patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]):
            await bot_main.trial_recovery_callback(update, context)

        self.assertIn("лише власнику", message.replies[-1]["text"])

    async def test_reason_result_marks_only_first_response_for_notification(self) -> None:
        class Transaction:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        conn = SimpleNamespace(transaction=lambda: Transaction(), execute=AsyncMock())
        base_row = {
            "status": "scheduled",
            "reason": "",
            "free_text": "",
            "feedback_item_id": None,
            "responded_at": None,
        }
        with (
            patch.object(trial_recovery, "_locked_recipient", new=AsyncMock(return_value=base_row)),
            patch.object(trial_recovery, "_upsert_feedback", new=AsyncMock(return_value=91)),
        ):
            first_result = await trial_recovery.record_reason(
                conn,
                recipient_id=17,
                tg_user_id=123456,
                reason="price",
            )

        repeated_row = dict(base_row, status="responded", reason="price", responded_at=datetime.now(UTC))
        with (
            patch.object(trial_recovery, "_locked_recipient", new=AsyncMock(return_value=repeated_row)),
            patch.object(trial_recovery, "_upsert_feedback", new=AsyncMock(return_value=91)),
        ):
            repeated_result = await trial_recovery.record_reason(
                conn,
                recipient_id=17,
                tg_user_id=123456,
                reason="price",
            )

        self.assertTrue(first_result["notification_required"])
        self.assertFalse(repeated_result["notification_required"])

    async def test_contact_request_can_open_followup_text_without_losing_status(self) -> None:
        class Transaction:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        conn = SimpleNamespace(transaction=lambda: Transaction(), execute=AsyncMock())
        row = {"status": "contact_requested", "opted_out_at": None}

        with patch.object(trial_recovery, "_locked_recipient", new=AsyncMock(return_value=row)):
            started = await trial_recovery.begin_free_text(
                conn,
                recipient_id=17,
                tg_user_id=123456,
            )

        self.assertTrue(started)
        update_sql = conn.execute.await_args.args[0]
        self.assertIn("contact_requested_at IS NOT NULL THEN 'contact_requested'", update_sql)

    def test_owner_notification_is_detailed_and_escapes_user_content(self) -> None:
        snapshot = recovery_snapshot(
            first_name="<Олена>",
            free_text="Дорого & незрозуміло <script>",
        )

        text = trial_recovery.format_owner_notification(
            snapshot,
            event="details",
            timezone_name="Europe/Istanbul",
        )

        self.assertIn("Нове уточнення", text)
        self.assertIn("499 грн потім дорого", text)
        self.assertIn("&lt;Олена&gt;", text)
        self.assertIn("Дорого &amp; незрозуміло &lt;script&gt;", text)
        self.assertIn("Telegram ID:</b> <code>123456</code>", text)
        self.assertIn("Онбординг:</b> завершено (v3)", text)
        self.assertIn("Підписка / доступ:</b> немає / paywall", text)
        self.assertIn("28.08.2026 15:30", text)
        self.assertNotIn("<script>", text)

    async def test_owner_notification_goes_to_operator_with_reply_button(self) -> None:
        bot = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(application=SimpleNamespace(bot=bot), user_data={})
        snapshot = recovery_snapshot()

        with (
            patch.object(bot_main, "get_trial_recovery_owner_snapshot", new=AsyncMock(return_value=snapshot)),
            patch.object(bot_main, "log_admin_notification", new=AsyncMock()),
            patch.object(bot_main, "notify_admins", new=AsyncMock()) as notify_mock,
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
        ):
            await bot_main._notify_trial_recovery_owners(
                context,
                object(),
                recipient_id=17,
                event="reason",
                telegram_user=SimpleNamespace(id=123456),
                reason="price",
            )

        notify_mock.assert_awaited_once()
        kwargs = notify_mock.await_args.kwargs
        self.assertEqual(kwargs["chat_ids"], {7884326049})
        self.assertEqual(kwargs["parse_mode"], "HTML")
        markup = kwargs["reply_markup"]
        button = markup.args[0][0][0]
        self.assertEqual(button.kwargs["callback_data"], "trialrec:a:17")

    async def test_owner_text_is_sent_to_target_and_reply_mode_is_cleared(self) -> None:
        conn = object()
        bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=901)))
        context = SimpleNamespace(
            application=SimpleNamespace(bot=bot, bot_data={"db_pool": harness.DummyPool(conn)}),
            user_data={
                bot_main._TRIAL_RECOVERY_OWNER_REPLY_KEY: {
                    "recipient_id": 17,
                    "telegram_user_id": 123456,
                    "lang": "uk",
                    "target_label": "@olena_test",
                }
            },
        )
        message = harness.DummyMessage("Дякую за чесну відповідь — хочу розібратись детальніше.")
        update = SimpleNamespace(
            effective_user=SimpleNamespace(id=7884326049),
            message=message,
        )

        with (
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
            patch.object(bot_main, "get_trial_recovery_owner_snapshot", new=AsyncMock(return_value=recovery_snapshot())),
            patch.object(bot_main, "log_admin_notification", new=AsyncMock()) as log_mock,
        ):
            consumed = await bot_main._consume_trial_recovery_owner_reply(update, context)

        self.assertTrue(consumed)
        bot.send_message.assert_awaited_once()
        send_kwargs = bot.send_message.await_args.kwargs
        self.assertEqual(send_kwargs["chat_id"], 123456)
        self.assertIn("Особиста відповідь від засновника", send_kwargs["text"])
        reply_button = send_kwargs["reply_markup"].args[0][0][0]
        self.assertEqual(reply_button.kwargs["callback_data"], "trialrec:t:17")
        self.assertNotIn(bot_main._TRIAL_RECOVERY_OWNER_REPLY_KEY, context.user_data)
        self.assertIn("Відповідь надіслано", message.replies[-1]["text"])
        log_mock.assert_awaited_once()

    async def test_owner_reply_is_blocked_after_user_opts_out(self) -> None:
        conn = object()
        bot = SimpleNamespace(send_message=AsyncMock())
        context = SimpleNamespace(
            application=SimpleNamespace(bot=bot, bot_data={"db_pool": harness.DummyPool(conn)}),
            user_data={
                bot_main._TRIAL_RECOVERY_OWNER_REPLY_KEY: {
                    "recipient_id": 17,
                    "telegram_user_id": 123456,
                    "lang": "uk",
                    "target_label": "@olena_test",
                }
            },
        )
        message = harness.DummyMessage("Напишемо ще раз")
        update = SimpleNamespace(effective_user=SimpleNamespace(id=7884326049), message=message)

        with (
            patch.object(bot_main.config, "MINIAPP_OPERATOR_TELEGRAM_IDS", [7884326049]),
            patch.object(
                bot_main,
                "get_trial_recovery_owner_snapshot",
                new=AsyncMock(return_value=recovery_snapshot(recovery_status="opted_out")),
            ),
        ):
            consumed = await bot_main._consume_trial_recovery_owner_reply(update, context)

        self.assertTrue(consumed)
        bot.send_message.assert_not_awaited()
        self.assertNotIn(bot_main._TRIAL_RECOVERY_OWNER_REPLY_KEY, context.user_data)
        self.assertIn("відмовився", message.replies[-1]["text"])


if __name__ == "__main__":
    unittest.main()
