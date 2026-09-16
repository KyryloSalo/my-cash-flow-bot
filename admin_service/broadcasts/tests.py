from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from broadcasts.admin import BroadcastAdmin
from broadcasts.models import Broadcast, BroadcastRecipient, Segment
from broadcasts.tasks import send_broadcast_task
from common.admin_site import admin_site
from common.test_helpers import ensure_telegram_user_table
from users.models import TelegramUser, UserAdminState


class BroadcastAdminPreviewEscapingTests(SimpleTestCase):
    def setUp(self):
        self.broadcast_admin = BroadcastAdmin(Broadcast, admin_site)

    def test_broadcast_admin_previews_escape_user_supplied_html(self):
        broadcast = SimpleNamespace(pk=1)
        recipient_preview = {
            "count": 1,
            "users": [
                SimpleNamespace(
                    tg_user_id=2001,
                    username="<img src=x onerror=alert('user')>",
                    full_name="<script>alert('name')</script>",
                )
            ],
        }
        delivery_rows = [
            SimpleNamespace(
                user_id=2001,
                status=BroadcastRecipient.Status.FAILED,
                sent_at="2026-05-11 12:00",
                error_message="<script>alert('delivery')</script>",
            )
        ]

        with (
            patch("broadcasts.admin.recipient_preview_data", return_value=recipient_preview),
            patch("broadcasts.admin.delivery_preview_rows", return_value=delivery_rows),
        ):
            recipient_html = str(self.broadcast_admin.recipient_preview(broadcast))
            delivery_html = str(self.broadcast_admin.delivery_log_preview(broadcast))

        self.assertNotIn("<script>alert('name')</script>", recipient_html)
        self.assertNotIn("<img src=x onerror=alert('user')>", recipient_html)
        self.assertIn("&lt;script&gt;alert(&#x27;name&#x27;)&lt;/script&gt;", recipient_html)
        self.assertIn("&lt;img src=x onerror=alert(&#x27;user&#x27;)&gt;", recipient_html)
        self.assertNotIn("<script>alert('delivery')</script>", delivery_html)
        self.assertIn("&lt;script&gt;alert(&#x27;delivery&#x27;)&lt;/script&gt;", delivery_html)


class BroadcastFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        self.user = TelegramUser.objects.create(
            tg_user_id=2001,
            first_name="Broadcast",
            username="broadcast_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.segment, _ = Segment.objects.update_or_create(
            slug="all_users",
            defaults={
                "name": "All Users",
                "type": Segment.Type.SYSTEM,
                "description": "All users",
                "is_active": True,
                "is_system": True,
            },
        )
        self.test_segment, _ = Segment.objects.update_or_create(
            slug="test_users",
            defaults={
                "name": "Тестові користувачі",
                "type": Segment.Type.SYSTEM,
                "description": "Користувачі з позначкою тестового або додані до allowlist для QA",
                "is_active": True,
                "is_system": True,
            },
        )

    def test_broadcast_dry_run_collects_recipients(self):
        broadcast = Broadcast.objects.create(
            title="Promo",
            message_text="Hello",
            created_by=self.admin_user,
            target_type=Broadcast.TargetType.SEGMENT,
            target_segment=self.segment,
        )
        send_broadcast_task(broadcast.id, dry_run=True)
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.total_recipients, 1)
        self.assertEqual(broadcast.last_dry_run_preview["count"], 1)
        self.assertEqual(broadcast.last_dry_run_preview["users"][0]["telegram_id"], self.user.tg_user_id)

    def test_test_users_segment_targets_only_marked_users(self):
        TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Regular",
            username="regular_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        UserAdminState.objects.update_or_create(telegram_user_id=self.user.tg_user_id, defaults={"is_test_user": True})
        broadcast = Broadcast.objects.create(
            title="QA",
            message_text="Test only",
            created_by=self.admin_user,
            target_type=Broadcast.TargetType.SEGMENT,
            target_segment=self.test_segment,
        )
        send_broadcast_task(broadcast.id, dry_run=True)
        broadcast.refresh_from_db()
        self.assertEqual(broadcast.total_recipients, 1)
        self.assertEqual(broadcast.last_dry_run_preview["users"][0]["telegram_id"], self.user.tg_user_id)
