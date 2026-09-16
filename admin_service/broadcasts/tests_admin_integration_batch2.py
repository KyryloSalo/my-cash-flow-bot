"""Admin integration regressions; synthetic ORM fixtures, no provider/broker."""
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.messages.storage.fallback import FallbackStorage
from django.contrib.sessions.backends.signed_cookies import SessionStore
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from broadcasts.admin import BroadcastAdmin
from broadcasts.models import AdminMessageLog, Broadcast
from broadcasts.tasks import send_broadcast_task, send_manual_message_task
from common.admin_site import admin_site
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="synthetic-test-token")
class DeliveredBroadcastTestApprovalTests(TestCase):
    def setUp(self):
        self.admin = get_user_model().objects.create(username="synthetic-admin-integration", is_staff=True, is_superuser=True)
        self.user = TelegramUser.objects.create(
            tg_user_id=81001,
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
        self.broadcast = Broadcast.objects.create(title="Synthetic only", message_text="Never transmitted", created_by=self.admin)
        send_broadcast_task(self.broadcast.pk, dry_run=True)
        self.broadcast.refresh_from_db()
        self.model_admin = BroadcastAdmin(Broadcast, admin_site)
        self.queue = self.enterContext(patch("broadcasts.admin.send_manual_message_task.delay"))
        self.launch = self.enterContext(patch("broadcasts.admin.send_broadcast_task.delay"))
        self.enterContext(patch.object(self.model_admin, "_resolve_admin_test_chat_id", return_value=999))
        self.enterContext(patch.object(admin_site, "each_context", return_value={}))
        self.session = SessionStore()

    def request(self, data=None):
        # Exercise Django's real signing + JSON save/load between requests.
        self.session.save()
        self.session = SessionStore(session_key=self.session.session_key)
        request = RequestFactory().post("/synthetic/", data or {}) if data is not None else RequestFactory().get("/synthetic/")
        request.user = self.admin
        request.session = self.session
        request._messages = FallbackStorage(request)
        return request

    def queue_test(self):
        self.model_admin.send_test_view(self.request({}), self.broadcast.pk)
        return AdminMessageLog.objects.latest("pk")

    def ready(self):
        response = self.model_admin.wizard_view(self.request(), self.broadcast.pk, 3)
        return response.context_data["test_ready"]

    def test_enqueue_is_not_approval_but_delivered_receipt_survives_session_reload(self):
        log = self.queue_test()
        self.assertFalse(self.ready(), "Queue acceptance is not successful delivery")
        self.model_admin.send_view(self.request({}), self.broadcast.pk)
        self.launch.assert_not_called()
        self.model_admin.wizard_view(self.request({"action": "send"}), self.broadcast.pk, 4)
        self.launch.assert_not_called()
        with patch("broadcasts.tasks.send_telegram_message", return_value={"ok": True, "result": {"message_id": 123}}):
            send_manual_message_task(log.pk)
        self.assertTrue(self.ready())
        self.model_admin.send_view(self.request({}), self.broadcast.pk)
        self.launch.assert_called_once_with(self.broadcast.pk, dry_run=False)

    def test_failed_uncertain_or_unconfirmed_receipt_cannot_approve(self):
        log = self.queue_test()
        for status in ("failed", "blocked", "skipped", "uncertain", "sent"):
            with self.subTest(status=status):
                AdminMessageLog.objects.filter(pk=log.pk).update(status=status, sent_at=None)
                self.assertFalse(self.ready())

    def test_changed_image_audience_or_receipt_identity_invalidates_approval(self):
        log = self.queue_test()
        AdminMessageLog.objects.filter(pk=log.pk).update(status="sent", sent_at=timezone.now())
        self.assertTrue(self.ready())
        AdminMessageLog.objects.filter(pk=log.pk).update(target_chat_id=998)
        self.assertFalse(self.ready())
        AdminMessageLog.objects.filter(pk=log.pk).update(target_chat_id=999)
        Broadcast.objects.filter(pk=self.broadcast.pk).update(image_url="https://example.invalid/changed.png")
        self.assertFalse(self.ready())
        Broadcast.objects.filter(pk=self.broadcast.pk).update(image_url="")
        TelegramUser.objects.create(tg_user_id=81002, created_at=timezone.now())
        self.assertFalse(self.ready())

    def test_two_stale_launch_requests_cannot_enqueue_twice(self):
        log = self.queue_test()
        AdminMessageLog.objects.filter(pk=log.pk).update(status="sent", sent_at=timezone.now())
        snapshots = [Broadcast.objects.get(pk=self.broadcast.pk) for _ in range(2)]
        with patch("broadcasts.admin.get_object_or_404", side_effect=snapshots):
            self.model_admin.send_view(self.request({}), self.broadcast.pk)
            self.model_admin.wizard_view(self.request({"action": "send"}), self.broadcast.pk, 4)
        self.assertEqual(self.launch.call_count, 1)

    def test_wizard_shows_uncertain_count_and_no_retry_status(self):
        from broadcasts.models import BroadcastRecipient
        Broadcast.objects.filter(pk=self.broadcast.pk).update(status="failed", uncertain_count=1)
        BroadcastRecipient.objects.create(broadcast=self.broadcast, user=self.user, status="uncertain")
        response = self.model_admin.wizard_view(self.request(), self.broadcast.pk, 4)
        self.assertIn("невідомо: 1", response.context_data["status_label"])
        html = response.rendered_content
        self.assertIn("без автоматичного повтору", html)
