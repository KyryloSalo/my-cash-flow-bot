"""Slice 8 contract tests; database-backed cases require the unavailable PostgreSQL host."""
import ast
import os
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from subscriptions.canary import confirm_canary_renewal
from subscriptions.models import Payment, Plan, Subscription
from users.canary import issue_canary_capability
from users.models import CanaryCapability, TelegramUser, UserAdminState

ROOT = Path(__file__).resolve().parent


class CanaryRenewalSlice8Contracts(unittest.TestCase):
    def test_manual_canary_renewal_service_exists(self):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
        os.environ.setdefault("DJANGO_DEBUG", "true")
        os.environ.setdefault("DJANGO_SECRET_KEY", "slice8-test")
        import django
        django.setup()
        from subscriptions.canary import confirm_canary_renewal
        self.assertTrue(callable(confirm_canary_renewal))

    def test_admin_confirmation_route_is_wired(self):
        source = (ROOT.parent / "users" / "admin.py").read_text(encoding="utf-8")
        self.assertIn("production-canary-renew", source)
        self.assertIn("confirm_canary_renewal", source)

    def test_scheduler_excludes_canary_users_before_transport(self):
        source = (ROOT / "billing.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        text = ast.unparse(tree)
        self.assertIn("is_production_canary", text)
        self.assertIn("Production Canary users are isolated", text)
        self.assertIn("user__billing_profile__provider", text)


@override_settings(
    PRODUCTION_CANARY_ENABLED=True,
    PRODUCTION_CANARY_TELEGRAM_IDS=[92008],
    PRODUCTION_CANARY_ALLOWED_SCENARIOS=["success", "declined", "processing"],
    PRODUCTION_CANARY_MAX_AMOUNT=1000,
)
class CanaryRenewalBehaviorTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin = get_user_model().objects.create_superuser(
            username="canary-renew-admin",
            email="canary-renew@example.invalid",
            password="unused-test-password",
        )
        self.user = TelegramUser.objects.create(tg_user_id=92008, first_name="Renew")
        UserAdminState.objects.create(
            telegram_user=self.user,
            is_test_user=True,
            is_production_canary=True,
            status="active",
        )
        self.plan = Plan.objects.create(
            name="Renew plan",
            slug="renew-plan",
            price=Decimal("1.00"),
            currency="UAH",
            duration_days=30,
        )
        self.expires_at = timezone.now() + timedelta(days=2)
        self.subscription = Subscription.objects.create(
            user=self.user,
            plan="renew-plan",
            plan_ref=self.plan,
            status=Subscription.Status.ACTIVE,
            provider="canary",
            amount=Decimal("1.00"),
            currency="UAH",
            expires_at=self.expires_at,
            canary_owned=True,
        )

    def _issue(self, scenario):
        return issue_canary_capability(
            user=self.user,
            purpose=CanaryCapability.Purpose.BILLING_RENEWAL,
            issued_by=self.admin,
            scenario=scenario,
            metadata={"subscription_id": self.subscription.pk},
        )

    def test_success_renewal_finalizes_paid_and_replay_is_idempotent(self):
        capability, token = self._issue(CanaryCapability.Scenario.SUCCESS)

        first = confirm_canary_renewal(user=self.user, capability_token=token, confirmed=True)
        second = confirm_canary_renewal(user=self.user, capability_token=token, confirmed=True)

        self.assertEqual(first["payment_id"], second["payment_id"])
        payment = Payment.objects.get(pk=first["payment_id"])
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(payment.provider, "canary")
        self.subscription.refresh_from_db()
        self.assertGreater(self.subscription.expires_at, self.expires_at)
        capability.refresh_from_db()
        self.assertEqual(capability.status, CanaryCapability.Status.COMPLETED)

    def test_failed_renewal_never_extends_subscription(self):
        capability, token = self._issue(CanaryCapability.Scenario.DECLINED)

        result = confirm_canary_renewal(user=self.user, capability_token=token, confirmed=True)

        self.assertEqual(
            Payment.objects.get(pk=result["payment_id"]).status,
            Payment.Status.FAILED,
        )
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, self.expires_at)
        capability.refresh_from_db()
        self.assertEqual(capability.status, CanaryCapability.Status.FAILED)


if __name__ == "__main__":
    unittest.main()
