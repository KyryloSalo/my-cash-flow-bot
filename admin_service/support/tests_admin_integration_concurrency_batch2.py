"""Independent PostgreSQL connections prove durable pre-I/O support claims."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connections, transaction
from django.db.models.query import QuerySet
from django.test import TransactionTestCase, override_settings
from django.utils import timezone

from support.models import SupportCase, SupportMessage
from support.tasks import send_support_reply_task
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="synthetic-test-token")
class SupportDurableConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create(username="synthetic-support-race-admin", is_staff=True)
        self.user = TelegramUser.objects.create(
            tg_user_id=82002,
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
            text="Synthetic race",
        )

    def tearDown(self):
        self.user.delete()
        super().tearDown()

    def test_two_workers_with_same_unclaimed_snapshot_send_once(self):
        self.assertEqual(connections["default"].vendor, "postgresql", "Run this proof against PostgreSQL")
        barrier = Barrier(2)
        original_get = QuerySet.get
        pids = []

        def synchronized_get(queryset, *args, **kwargs):
            row = original_get(queryset, *args, **kwargs)
            if queryset.model is SupportMessage:
                self.assertIsNone(row.telegram_message_id)
                barrier.wait(timeout=10)
            return row

        def worker():
            try:
                with connections["default"].cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    pids.append(cursor.fetchone()[0])
                send_support_reply_task(self.message.pk)
            finally:
                connections.close_all()

        with patch.object(QuerySet, "get", synchronized_get), patch("support.tasks.send_support_reply", return_value={"ok": True, "result": {"message_id": 321}}) as send:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = [pool.submit(worker) for _ in range(2)]
                for result in results:
                    result.result(timeout=20)
        self.assertEqual(len(set(pids)), 2)
        self.assertEqual(send.call_count, 1)
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, 321)
        print("SUPPORT_PG_RACE", {"independent_connections": len(set(pids)), "attempts": send.call_count})

    def test_claim_is_committed_to_other_connection_before_send_and_survives_crash(self):
        def inspect_claim():
            try:
                return SupportMessage.objects.get(pk=self.message.pk).telegram_message_id
            finally:
                connections.close_all()

        def crash_after_acceptance(**kwargs):
            with ThreadPoolExecutor(max_workers=1) as pool:
                self.assertEqual(pool.submit(inspect_claim).result(timeout=10), -1)
            raise SystemExit("synthetic worker death after accepted send")

        with patch("support.tasks.send_support_reply", side_effect=crash_after_acceptance) as send:
            with self.assertRaises(SystemExit):
                send_support_reply_task(self.message.pk)
            send_support_reply_task(self.message.pk)
        self.assertEqual(send.call_count, 1)
        self.message.refresh_from_db()
        self.assertEqual(self.message.telegram_message_id, -1)

    def test_outer_transaction_is_rejected_before_io(self):
        with patch("support.tasks.send_support_reply") as send:
            with transaction.atomic():
                with self.assertRaises(RuntimeError):
                    send_support_reply_task(self.message.pk)
            send.assert_not_called()
        self.message.refresh_from_db()
        self.assertIsNone(self.message.telegram_message_id)
