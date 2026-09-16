"""Real DB compare-and-set concurrency; Telegram and Celery broker are stubbed.
SQLite executes these locally; also run with the PostgreSQL test backend before release.
"""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connections
from django.test import TransactionTestCase, override_settings
from django.utils import timezone

from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient
from broadcasts.tasks import claim_delivery, send_broadcast_task, send_manual_message_task
from broadcasts.tests_communications_remediation import queued_poll_args
from polls.models import PollCampaign
from polls.tasks import send_poll_campaign_task
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="synthetic-test-token")
class AtomicDeliveryConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create(username="synthetic-concurrency", is_staff=True)
        self.user = TelegramUser.objects.create(
            tg_user_id=701,
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
        self.enterContext(patch("admin_notifications.tasks.send_admin_notification_task.delay"))
        self.enterContext(patch("broadcasts.tasks.time.sleep"))

    def tearDown(self):
        self.user.delete()  # Runtime-owned users is deliberately unmanaged.
        super().tearDown()

    def race(self, call):
        barrier = Barrier(2)
        def worker():
            try:
                barrier.wait(timeout=10)
                return call()
            finally:
                connections.close_all()
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker) for _ in range(2)]
            return [future.result(timeout=20) for future in futures]

    def test_recipient_database_claim_has_exactly_one_winner(self):
        broadcast = Broadcast.objects.create(title="Synthetic", message_text="Stub", created_by=self.admin, status="sending")
        recipient = BroadcastRecipient.objects.create(broadcast=broadcast, user=self.user)
        winners = self.race(lambda: claim_delivery(BroadcastRecipient.objects.filter(pk=recipient.pk, status="pending", broadcast__status="sending")))
        self.assertEqual(sorted(winners), [False, True])
        recipient.refresh_from_db()
        self.assertEqual(recipient.status, "uncertain")

    def test_two_broadcast_workers_send_once(self):
        broadcast = Broadcast.objects.create(
            title="Synthetic",
            message_text="Stub",
            created_by=self.admin,
            status="scheduled",
            target_type=Broadcast.TargetType.MANUAL_USERS,
            manual_users=[self.user.tg_user_id],
        )
        send_broadcast_task(broadcast.pk, dry_run=True)
        with patch("broadcasts.tasks.send_telegram_message", return_value={"result": {"message_id": 1}}) as send:
            self.race(lambda: send_broadcast_task(broadcast.pk))
        self.assertEqual(send.call_count, 1)
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.sent_count, 1)

    def test_two_manual_workers_send_once(self):
        log = AdminMessageLog.objects.create(
            telegram_user=self.user,
            admin_user=self.admin,
            target_chat_id=701,
            message_text="Stub",
        )
        with patch("broadcasts.tasks.send_telegram_message", return_value={"result": {"message_id": 1}}) as send:
            self.race(lambda: send_manual_message_task(log.pk))
        self.assertEqual(send.call_count, 1)

    def test_two_poll_workers_send_once(self):
        campaign = PollCampaign.objects.create(
            title="Synthetic",
            question="Stub?",
            created_by=self.admin,
            target_type=PollCampaign.TargetType.MANUAL_USERS,
            manual_users=[self.user.tg_user_id],
        )
        args, kwargs = queued_poll_args(campaign.pk)
        with patch("polls.tasks.send_telegram_message", return_value={"result": {"message_id": 1}}) as send:
            self.race(lambda: send_poll_campaign_task(*args, **kwargs))
        self.assertEqual(send.call_count, 1)
