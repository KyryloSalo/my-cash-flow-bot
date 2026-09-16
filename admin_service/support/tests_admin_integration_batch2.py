"""Support replies have at-most-once attempts, including unknown outcomes."""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from support.models import SupportCase, SupportMessage
from support.tasks import send_support_reply_task
from subscriptions.models import TrialRecoveryCampaign, TrialRecoveryRecipient
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="synthetic-test-token")
class SupportAtMostOnceTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create(username="synthetic-support-admin", is_staff=True)
        self.user = TelegramUser.objects.create(
            tg_user_id=82001,
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
        )
        UserAdminState.objects.create(
            telegram_user=self.user,
            status=UserAdminState.Status.ACTIVE,
            can_receive_messages=True,
        )
        self.case = SupportCase.objects.create(user=self.user, assigned_admin=self.admin)
        self.message = SupportMessage.objects.create(
            case=self.case,
            sender_type=SupportMessage.SenderType.ADMIN,
            sender_admin=self.admin,
            text="Synthetic reply, never transmitted",
        )
        self.send = self.enterContext(patch("support.tasks.send_support_reply", return_value={"ok": True, "result": {"message_id": 321}}))

    def test_repeat_task_cannot_send_same_support_message_twice(self):
        send_support_reply_task(self.message.pk)
        send_support_reply_task(self.message.pk)
        self.assertEqual(self.send.call_count, 1)
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, 321)

    def test_missing_provider_receipt_stays_uncertain_and_is_not_retried(self):
        for payload in ({}, {"result": {}}, {"result": {"message_id": None}}, {"result": {"message_id": 0}}):
            with self.subTest(payload=payload):
                message = SupportMessage.objects.create(
                    case=self.case,
                    sender_type=SupportMessage.SenderType.ADMIN,
                    sender_admin=self.admin,
                    text="Synthetic",
                )
                self.send.return_value = payload
                self.send.reset_mock()
                send_support_reply_task(message.pk)
                send_support_reply_task(message.pk)
                self.assertEqual(self.send.call_count, 1)
                message.refresh_from_db()
                self.assertEqual(message.telegram_message_id, -1)

    def test_transport_failure_is_durable_uncertainty_not_a_raw_error(self):
        from bot_events.models import BotEvent
        self.send.side_effect = TimeoutError("synthetic private transport payload")
        send_support_reply_task(self.message.pk)
        send_support_reply_task(self.message.pk)
        self.assertEqual(self.send.call_count, 1)
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, -1)
        event = BotEvent.objects.get(source="support_reply")
        self.assertEqual(event.parsed_result["delivery_status"], "uncertain")
        self.assertEqual(event.raw_input, "")
        self.assertNotIn("private transport", event.error_message)

    def test_provider_rejection_and_blocking_are_not_uncertain_or_retryable(self):
        from common.telegram import TelegramSendError
        from users.models import UserAdminState
        for blocked, expected in ((False, -2), (True, -3)):
            with self.subTest(blocked=blocked):
                message = SupportMessage.objects.create(
                    case=self.case,
                    sender_type=SupportMessage.SenderType.ADMIN,
                    sender_admin=self.admin,
                    text="Synthetic",
                )
                self.send.side_effect = TelegramSendError("rejected", blocked=blocked, payload='{"ok": false, "error_code": 400}')
                self.send.reset_mock()
                send_support_reply_task(message.pk)
                send_support_reply_task(message.pk)
                self.assertEqual(self.send.call_count, 1)
                message.refresh_from_db()
                self.assertEqual(message.telegram_message_id, expected)
        self.assertTrue(UserAdminState.objects.get(telegram_user=self.user).blocked_bot)

    def test_crash_after_provider_acceptance_preserves_claim(self):
        with patch.object(SupportMessage, "save", side_effect=RuntimeError("synthetic post-send DB failure")):
            with self.assertRaises(RuntimeError):
                send_support_reply_task(self.message.pk)
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, -1)
        send_support_reply_task(self.message.pk)
        self.assertEqual(self.send.call_count, 1)

    def test_user_messages_are_never_sent(self):
        self.message.sender_type = "user"
        self.message.save(update_fields=["sender_type"])
        send_support_reply_task(self.message.pk)
        self.send.assert_not_called()

    def test_recovery_opt_out_is_rechecked_before_support_transport(self):
        campaign = TrialRecoveryCampaign.objects.create(name="Synthetic recovery")
        TrialRecoveryRecipient.objects.create(
            campaign=campaign,
            user=self.user,
            status=TrialRecoveryRecipient.Status.OPTED_OUT,
            support_case_id=self.case.pk,
            personal_contact_declined_at=timezone.now(),
            opted_out_at=timezone.now(),
        )

        send_support_reply_task(self.message.pk)

        self.send.assert_not_called()
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, -3)
