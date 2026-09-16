from __future__ import annotations

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from support.models import SupportCase, SupportMessage
from support.tasks import send_support_reply_task
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="test-token")
class SupportFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=3001,
            first_name="Support",
            username="support_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        UserAdminState.objects.create(
            telegram_user=self.telegram_user,
            status=UserAdminState.Status.ACTIVE,
            can_receive_messages=True,
        )

    def test_support_case_and_admin_reply(self):
        case = SupportCase.objects.create(user=self.telegram_user, subject="Payment issue")
        message = SupportMessage.objects.create(case=case, sender_type=SupportMessage.SenderType.ADMIN, sender_admin=self.admin_user, text="Reply text")
        with patch("support.tasks.send_support_reply", return_value={"result": {"message_id": 777}}):
            send_support_reply_task(message.id)
        message.refresh_from_db()
        self.assertEqual(message.telegram_message_id, 777)
