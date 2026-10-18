"""Real PostgreSQL/admin-action/API regressions for cardless explicit grants."""
from datetime import timedelta

from django.test import TestCase, override_settings
from django.utils import timezone

from miniapp import auth
from miniapp.services import resolve_access
from subscriptions.models import BillingProfile, Payment, Subscription
from subscriptions.services import apply_subscription_change
from transactions.models import Transaction
from users.models import TelegramUser, UserAdminState


@override_settings(SECURE_SSL_REDIRECT=False)
class AdminCardAccessIntegrationTests(TestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=998801, first_name="Synthetic access regression")
        session = self.client.session
        session[auth.SESSION_USER_ID_KEY] = self.user.tg_user_id
        session.save()

    def assert_api_access(self, expected_status):
        for path in ("/app/api/bootstrap", "/app/api/flow", "/app/api/history"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, expected_status)

    def test_admin_activation_works_without_card_and_does_not_create_payments(self):
        for action in ("activate", "manual", "lifetime", "trial"):
            with self.subTest(action=action):
                subscription = apply_subscription_change(user=self.user, action=action, days=30, admin_user=None)
                access = resolve_access(self.user)
                self.assertEqual(subscription.source, Subscription.Source.ADMIN)
                self.assertEqual(access["access_scope"], "personal_full")
                self.assertEqual(access["mode"], "active")
                self.assertFalse(access["has_card"])
                self.assertIsNone(access["recommended_action"])
                self.assert_api_access(200)
        self.assertFalse(BillingProfile.objects.filter(user=self.user).exists())
        self.assertFalse(Payment.objects.filter(user=self.user).exists())
        self.assertFalse(Transaction.objects.filter(tg_user=self.user).exists())

    def test_pending_or_failed_binding_does_not_invalidate_manual_activation(self):
        apply_subscription_change(user=self.user, action="activate", days=30, admin_user=None)
        profile = BillingProfile.objects.create(user=self.user, status="pending", last_charge_status="created")
        for status in ("pending", "failed", "blocked"):
            with self.subTest(status=status):
                profile.status = status
                profile.save(update_fields=["status"])
                self.assertEqual(resolve_access(self.user)["mode"], "active")
                self.assert_api_access(200)
        profile.refresh_from_db()
        self.assertEqual(profile.card_token, "")
        self.assertFalse(Payment.objects.filter(user=self.user).exists())

    def test_expiry_revocation_and_admin_block_remain_authoritative(self):
        subscription = apply_subscription_change(user=self.user, action="activate", days=30, admin_user=None)
        subscription.expires_at = timezone.now() - timedelta(seconds=1)
        subscription.save(update_fields=["expires_at"])
        self.assert_api_access(403)
        apply_subscription_change(user=self.user, action="cancel", admin_user=None)
        self.assert_api_access(403)
        apply_subscription_change(user=self.user, action="lifetime", admin_user=None)
        UserAdminState.objects.filter(telegram_user=self.user).update(is_blocked=True)
        self.assert_api_access(403)

    def test_unbound_self_service_trial_cannot_use_stale_admin_projection(self):
        subscription = apply_subscription_change(user=self.user, action="activate", days=30, admin_user=None)
        subscription.source = Subscription.Source.PROMO
        subscription.provider = "monobank"
        subscription.status = Subscription.Status.TRIAL
        subscription.save(update_fields=["source", "provider", "status"])
        self.assertEqual(resolve_access(self.user)["access_scope"], "paywall")
        self.assert_api_access(403)
