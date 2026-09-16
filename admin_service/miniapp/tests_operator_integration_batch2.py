"""Shared operator KPI semantics and real-user attention scope."""
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from bot_events.models import BotEvent
from dashboard import services as dashboard_services
from miniapp.operator_hub import build_operator_payload
from subscriptions.models import Payment, Subscription
from support.models import SupportCase
from users.models import TelegramUser, UserAdminState


@override_settings(ADMIN_TEST_TELEGRAM_IDS=[83005])
class OperatorMetricsIntegrationTests(TestCase):
    def setUp(self):
        self.now = timezone.now()
        self.users = [TelegramUser.objects.create(tg_user_id=i, created_at=self.now, last_seen_at=self.now) for i in range(83001, 83006)]
        for index, user in enumerate(self.users):
            UserAdminState.objects.create(telegram_user=user, is_test_user=index in (2, 3), subscription_status="paid")
            if index >= 2:
                Subscription.objects.create(user=user, status="manual")
        self.operator = SimpleNamespace(tg_user_id=89999, full_name="Synthetic operator")
        self.enterContext(patch("miniapp.operator_hub._operator_users", return_value=[]))
        self.enterContext(patch("django.utils.timezone.now", return_value=self.now))

    def test_operator_uses_canonical_real_user_metrics_not_paid_projection(self):
        expected = dashboard_services.build_current_user_metrics(include_test_users=False)
        with patch("dashboard.services.build_current_user_metrics", wraps=dashboard_services.build_current_user_metrics) as shared:
            payload = build_operator_payload(self.operator)
        shared.assert_called_once_with(include_test_users=False)
        self.assertEqual(payload["metrics"], expected["metrics"])
        self.assertEqual(payload["metrics"][0]["value"], 2)
        self.assertEqual(payload["metrics"][2]["value"], 0)
        for key in ("scope_label", "retrieved_at", "include_test_users", "degraded"):
            self.assertEqual(payload[key], expected[key])

    def test_attention_excludes_flagged_and_allowlisted_tests_and_unattributed_errors(self):
        for user in self.users:
            Payment.objects.create(user=user, status="failed", amount="1.00", currency="UAH")
            BotEvent.objects.create(user=user, success=False)
            SupportCase.objects.create(user=user)
        BotEvent.objects.create(user=None, success=False)
        payload = build_operator_payload(self.operator)
        values = {item["label"]: item["value"] for item in payload["attention"]}
        self.assertEqual(values["Проблемні платежі"], 2)
        self.assertEqual(values["Помилки бота"], 2)
        self.assertEqual(values["Відкриті звернення"], 2)

    def test_unavailable_attention_is_null_and_not_healthy(self):
        from django.db.models.query import QuerySet
        original_count = QuerySet.count
        def unavailable(queryset):
            if queryset.model is SupportCase:
                raise RuntimeError("synthetic source unavailable")
            return original_count(queryset)
        with patch.object(QuerySet, "count", unavailable):
            payload = build_operator_payload(self.operator)
        support = next(item for item in payload["attention"] if item["label"] == "Відкриті звернення")
        self.assertIsNone(support["value"])
        self.assertEqual(support["tone"], "unknown")
        self.assertFalse(support["available"])
        self.assertTrue(payload["degraded"])
        self.assertEqual(payload["metrics"][0]["value"], 2)

    def test_uncertain_delivery_counters_use_durable_records_and_real_user_scope(self):
        from django.contrib.auth import get_user_model
        from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient
        from polls.models import PollCampaign, PollRecipient
        from support.models import SupportMessage
        admin = get_user_model().objects.create(username="synthetic-receipts")
        broadcast = Broadcast.objects.create(title="Synthetic", message_text="Stub", created_by=admin)
        poll = PollCampaign.objects.create(title="Synthetic", question="Stub", created_by=admin)
        for user in self.users:
            BroadcastRecipient.objects.create(broadcast=broadcast, user=user, status="uncertain")
            AdminMessageLog.objects.create(telegram_user=user, target_chat_id=user.pk, message_text="Stub", status="uncertain")
            PollRecipient.objects.create(campaign=poll, user=user, status="uncertain")
            case = SupportCase.objects.create(user=user)
            SupportMessage.objects.create(case=case, sender_type="admin", text="Stub", telegram_message_id=-1)
        payload = build_operator_payload(self.operator)
        values = {item.get("id"): item["value"] for item in payload["attention"]}
        for key in ("broadcast_uncertain", "manual_uncertain", "poll_uncertain", "support_uncertain"):
            self.assertEqual(values.get(key), 2, key)

    def test_nullable_snapshot_values_are_preserved(self):
        snapshot = dashboard_services.build_current_user_metrics(include_test_users=False)
        snapshot["metrics"][0].update(value=None, available=False, reason="Synthetic unavailable")
        snapshot["degraded"] = True
        with patch("dashboard.services.build_current_user_metrics", return_value=snapshot):
            payload = build_operator_payload(self.operator)
        self.assertIsNone(payload["metrics"][0]["value"])
        self.assertTrue(payload["degraded"])
