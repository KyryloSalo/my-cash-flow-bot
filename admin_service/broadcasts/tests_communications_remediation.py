"""ADMIN-004..008 regressions. All Telegram and queue calls are stubbed."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.sessions.serializers import JSONSerializer
from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient, Segment
from broadcasts.targets import users_for_target, users_for_segment_slug
from broadcasts.tasks import send_broadcast_task, send_manual_message_task
from broadcasts.wizard import remember_broadcast_test, broadcast_test_matches
from polls.models import PollCampaign, PollRecipient
from polls.tasks import send_poll_campaign_task
from common.telegram import TelegramSendError
from users.models import Tag, TelegramUser, UserAdminState, UserTag


class Session(dict):
    modified = False


def queued_poll_args(campaign_id):
    with patch("celery.app.task.Task.apply_async", side_effect=lambda args=None, kwargs=None, **options: (args or (), kwargs or {})):
        return json.loads(json.dumps(send_poll_campaign_task.delay(campaign_id)))


def run_confirmed_poll(campaign_id):
    args, kwargs = queued_poll_args(campaign_id)
    return send_poll_campaign_task(*args, **kwargs)


class BroadcastFingerprintRegressionTests(SimpleTestCase):
    def test_test_confirmation_survives_json_session_roundtrip(self):
        for manual_users in ([], [101]):
            with self.subTest(manual_users=manual_users):
                broadcast = Broadcast(pk=1, title="Synthetic", message_text="Stub only", manual_users=manual_users)
                request = SimpleNamespace(user=SimpleNamespace(pk=99), session=Session())
                with patch("broadcasts.wizard.recipient_preview_data", return_value={"count": 8}):
                    remember_broadcast_test(request, broadcast)
                    serializer = JSONSerializer()
                    request.session = Session(serializer.loads(serializer.dumps(request.session)))
                    self.assertTrue(broadcast_test_matches(request, broadcast))

    def test_image_change_invalidates_test_confirmation(self):
        broadcast = Broadcast(pk=1, title="Synthetic", message_text="Stub only")
        request = SimpleNamespace(user=SimpleNamespace(pk=99), session=Session())
        with patch("broadcasts.wizard.recipient_preview_data", return_value={"count": 8}):
            remember_broadcast_test(request, broadcast)
            broadcast.image_url = "https://example.invalid/changed.png"
            self.assertFalse(broadcast_test_matches(request, broadcast))


@override_settings(TELEGRAM_BOT_TOKEN="synthetic-test-token")
class CommunicationsDatabaseTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create(username="synthetic-admin", is_staff=True)
        self.users = [
            TelegramUser.objects.create(
                tg_user_id=i,
                lang="uk",
                base_currency="UAH",
                onboarding_completed=True,
                onboarding_version=3,
                created_at=timezone.now(),
            )
            for i in (101, 102, 103)
        ]
        for user in self.users:
            UserAdminState.objects.create(
                telegram_user=user,
                status=UserAdminState.Status.ACTIVE,
                can_receive_messages=True,
                blocked_bot=False,
            )
        self.send = self.enterContext(patch("broadcasts.tasks.send_telegram_message", return_value={"result": {"message_id": 1}}))
        self.poll_send = self.enterContext(patch("polls.tasks.send_telegram_message", return_value={"result": {"message_id": 1}}))
        self.notify = self.enterContext(patch("admin_notifications.tasks.send_admin_notification_task.delay"))
        self.enterContext(patch("broadcasts.tasks.time.sleep"))

    def broadcast(self, **kwargs):
        values = {
            "status": Broadcast.Status.SCHEDULED,
            "target_type": Broadcast.TargetType.MANUAL_USERS,
            "manual_users": [user.tg_user_id for user in self.users],
        }
        values.update(kwargs)
        broadcast = Broadcast.objects.create(
            title="Synthetic",
            message_text="Stub only",
            created_by=self.admin,
            **values,
        )
        send_broadcast_task(broadcast.pk, dry_run=True)
        broadcast.refresh_from_db()
        return broadcast

    def poll(self, **kwargs):
        values = {
            "target_type": PollCampaign.TargetType.MANUAL_USERS,
            "manual_users": [user.tg_user_id for user in self.users],
        }
        values.update(kwargs)
        return PollCampaign.objects.create(
            title="Synthetic",
            question="Stub only?",
            created_by=self.admin,
            **values,
        )


class TerminalCampaignRegressionTests(CommunicationsDatabaseTests):
    def test_terminal_broadcast_never_reactivated(self):
        for status in (Broadcast.Status.CANCELLED, Broadcast.Status.COMPLETED, Broadcast.Status.SENT, Broadcast.Status.FAILED):
            with self.subTest(status=status):
                broadcast = self.broadcast(status=status)
                send_broadcast_task(broadcast.pk)
                broadcast.refresh_from_db()
                self.assertEqual(broadcast.status, status)
        self.send.assert_not_called()

    def test_mid_send_cancel_preserves_terminal_status_and_skipped_count(self):
        broadcast = self.broadcast()
        def cancel(**kwargs):
            Broadcast.objects.filter(pk=broadcast.pk).update(status=Broadcast.Status.CANCELLED)
            return {"result": {"message_id": 1}}
        self.send.side_effect = cancel
        send_broadcast_task(broadcast.pk)
        broadcast.refresh_from_db()
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(broadcast.status, Broadcast.Status.CANCELLED)
        self.assertEqual(broadcast.sent_count, 1)
        self.assertEqual(broadcast.skipped_count, 2)
        self.assertEqual(broadcast.recipients.filter(status=BroadcastRecipient.Status.SKIPPED).count(), 2)

    def test_terminal_poll_never_reactivated(self):
        for status in (PollCampaign.Status.COMPLETED, PollCampaign.Status.CANCELLED):
            campaign = self.poll(status=status)
            run_confirmed_poll(campaign.pk)
            campaign.refresh_from_db()
            self.assertEqual(campaign.status, status)
        self.poll_send.assert_not_called()

    def test_poll_completion_after_first_delivery_stops_remaining_sends(self):
        campaign = self.poll()
        def complete(**kwargs):
            PollCampaign.objects.filter(pk=campaign.pk).update(status=PollCampaign.Status.COMPLETED)
            return {"result": {"message_id": 1}}
        self.poll_send.side_effect = complete
        run_confirmed_poll(campaign.pk)
        campaign.refresh_from_db()
        self.assertEqual(self.poll_send.call_count, 1)
        self.assertEqual(campaign.status, PollCampaign.Status.COMPLETED)


class DeliveryClaimRegressionTests(CommunicationsDatabaseTests):
    def test_broadcast_never_retries_uncertain_or_failed_recipients(self):
        broadcast = self.broadcast()
        for user, status in zip(self.users, ("uncertain", "failed", "sent")):
            BroadcastRecipient.objects.create(broadcast=broadcast, user=user, status=status)
        send_broadcast_task(broadcast.pk)
        self.send.assert_not_called()
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.sent_count, 1)
        self.assertEqual(broadcast.failed_count, 1)
        self.assertEqual(broadcast.uncertain_count, 1)

    def test_transport_failure_is_uncertain_not_retryable(self):
        broadcast = self.broadcast(target_type="manual_users", manual_users=[101])
        self.send.side_effect = TelegramSendError("synthetic connection lost after write")
        send_broadcast_task(broadcast.pk)
        recipient = broadcast.recipients.get()
        self.assertEqual(recipient.status, "uncertain")
        self.assertIn("automatic retry disabled", recipient.error_message)
        Broadcast.objects.filter(pk=broadcast.pk).update(status=Broadcast.Status.SCHEDULED)
        send_broadcast_task(broadcast.pk)
        self.assertEqual(self.send.call_count, 1)

    def test_success_then_database_failure_does_not_resend(self):
        broadcast = self.broadcast(target_type="manual_users", manual_users=[101])
        # Create first, so only result persistence is fault-injected.
        BroadcastRecipient.objects.create(broadcast=broadcast, user=self.users[0])
        with patch.object(BroadcastRecipient, "save", side_effect=RuntimeError("synthetic post-send persistence failure")):
            with self.assertRaises(RuntimeError):
                send_broadcast_task(broadcast.pk)
        Broadcast.objects.filter(pk=broadcast.pk).update(status=Broadcast.Status.SCHEDULED)
        send_broadcast_task(broadcast.pk)
        self.assertEqual(self.send.call_count, 1)
        self.assertEqual(broadcast.recipients.get().status, "uncertain")

    def test_manual_message_replay_sends_once(self):
        log = AdminMessageLog.objects.create(
            telegram_user=self.users[0],
            admin_user=self.admin,
            target_chat_id=101,
            message_text="Stub",
        )
        send_manual_message_task(log.pk)
        send_manual_message_task(log.pk)
        self.assertEqual(self.send.call_count, 1)
        log.refresh_from_db()
        self.assertEqual(log.error_message, "")

    def test_successful_poll_clears_uncertain_claim_message(self):
        campaign = self.poll(target_type="manual_users", manual_users=[101])
        run_confirmed_poll(campaign.pk)
        recipient = campaign.recipients.get()
        self.assertEqual(recipient.status, PollRecipient.Status.SENT)
        self.assertEqual(recipient.error_message, "")

    def test_poll_does_not_retry_uncertain_recipient(self):
        campaign = self.poll(target_type="manual_users", manual_users=[101])
        PollRecipient.objects.create(campaign=campaign, user=self.users[0], status="uncertain")
        run_confirmed_poll(campaign.pk)
        self.poll_send.assert_not_called()

    def test_poll_transport_failure_stays_uncertain(self):
        campaign = self.poll(target_type="manual_users", manual_users=[101])
        self.poll_send.side_effect = TelegramSendError("synthetic connection lost")
        run_confirmed_poll(campaign.pk)
        self.assertEqual(campaign.recipients.get().status, "uncertain")
        self.assertIn("uncertain=1", self.notify.call_args.args[1])
        self.assertIn("failed=0", self.notify.call_args.args[1])


class ActualBlockedTargetRegressionTests(CommunicationsDatabaseTests):
    def test_blocked_admin_test_does_not_mark_customer_blocked(self):
        log = AdminMessageLog.objects.create(
            telegram_user=self.users[0],
            admin_user=self.admin,
            target_chat_id=101,
            message_text="Stub",
        )
        self.send.side_effect = TelegramSendError("synthetic admin blocked", blocked=True)
        with patch("broadcasts.tasks._resolve_admin_chat_id", return_value=999):
            send_manual_message_task(log.pk, is_test=True)
        self.assertEqual(self.send.call_args.kwargs["chat_id"], 999)
        self.assertFalse(UserAdminState.objects.filter(blocked_bot=True).exists())
        log.refresh_from_db()
        self.assertEqual(log.target_chat_id, 999)

    def test_real_delivery_marks_only_actual_recipient_blocked(self):
        log = AdminMessageLog.objects.create(
            telegram_user=self.users[0],
            admin_user=self.admin,
            target_chat_id=101,
            message_text="Stub",
        )
        self.send.side_effect = TelegramSendError("synthetic recipient blocked", blocked=True)
        send_manual_message_task(log.pk)
        self.assertEqual(list(UserAdminState.objects.filter(blocked_bot=True).values_list("telegram_user_id", flat=True)), [101])

    def test_mismatched_chat_does_not_mark_associated_customer(self):
        log = AdminMessageLog.objects.create(
            telegram_user=self.users[0],
            admin_user=self.admin,
            target_chat_id=999,
            message_text="Stub",
        )
        self.send.side_effect = TelegramSendError("synthetic other chat blocked", blocked=True)
        send_manual_message_task(log.pk)
        self.assertFalse(UserAdminState.objects.filter(blocked_bot=True).exists())
