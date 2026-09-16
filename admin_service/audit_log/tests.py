from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase
from django.utils import timezone

from audit_log.models import AdminAuditLog
from common.audit import create_audit_log, serialize_instance
from common.test_helpers import ensure_telegram_user_table
from support.models import SupportCase
from users.models import TelegramUser


class AuditLogTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def test_create_audit_log_records_action(self):
        admin_user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        request = RequestFactory().post("/admin/test/")
        request.user = admin_user
        request.META["REMOTE_ADDR"] = "127.0.0.1"
        create_audit_log(
            request=request,
            admin_user=admin_user,
            action="unit test action",
            object_type="test_object",
            object_id="1",
            target_user_id=123,
            mode="safe",
            reason="because",
            before={"a": 1},
            after={"a": 2},
        )
        log = AdminAuditLog.objects.get(action="unit test action", object_type="test_object")
        self.assertEqual(log.target_user_id, 123)
        self.assertEqual(log.mode, "safe")
        self.assertEqual(log.reason, "because")

    def test_serialize_instance_converts_foreign_keys_to_json_safe_values(self):
        admin_user = get_user_model().objects.create_superuser("admin2", "admin2@example.com", "pass12345")
        telegram_user = TelegramUser.objects.create(
            tg_user_id=4001,
            first_name="Audit",
            username="audit_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=1,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        case = SupportCase.objects.create(user=telegram_user, subject="Need help", assigned_admin=admin_user)

        payload = serialize_instance(case)
        self.assertEqual(payload["user"], telegram_user.tg_user_id)
        self.assertEqual(payload["assigned_admin"], admin_user.pk)

        create_audit_log(
            admin_user=admin_user,
            action="serialize fk",
            object_type="support_case",
            object_id=case.pk,
            after=payload,
        )

        log = AdminAuditLog.objects.get(action="serialize fk", object_type="support_case")
        self.assertEqual(log.after["user"], telegram_user.tg_user_id)
        self.assertEqual(log.after["assigned_admin"], admin_user.pk)
