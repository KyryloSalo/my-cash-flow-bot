from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase
from django.utils import timezone

from miniapp import services


class CardAccessTests(SimpleTestCase):
    def resolve(self, *, status="trial", card=False, source="promo", grace=False, payment_id=""):
        now = timezone.now()
        subscription = SimpleNamespace(
            status=status, source=source, expires_at=now + timedelta(days=-1 if grace else 90),
            grace_expires_at=now + timedelta(days=1) if grace else None,
            trial_days=90, next_charge_at=None, payment_id=payment_id,
        )
        profile = SimpleNamespace(
            card_token="synthetic-bound-card" if card else "", masked_pan="", status="active",
            auto_renew_enabled=False, last_charge_status="", last_failure_reason="", last_action_url="",
        )
        user = SimpleNamespace(tg_user_id=999001, lang="en")
        with (
            patch.object(services.UserAdminState.objects, "filter") as states,
            patch.object(services.Subscription.objects, "filter") as subscriptions,
            patch.object(services.BillingProfile.objects, "filter") as profiles,
            patch.object(services, "_resolve_finance_scope", return_value=SimpleNamespace(is_family=False)),
            patch.object(services, "activated_course_bonus", return_value=None),
            patch.object(services, "partner_offer_for_user", return_value={"trial_days": 90}),
        ):
            states.return_value.first.return_value = None
            subscriptions.return_value.order_by.return_value.first.return_value = subscription
            profiles.return_value.first.return_value = profile
            return services.resolve_access(user)

    def test_cardless_partner_trial_is_blocked_despite_future_expiry(self):
        access = self.resolve()
        self.assertEqual(access["mode"], "blocked")
        self.assertEqual(access["access_scope"], "paywall")
        self.assertEqual(access["recommended_action"], "start_trial")

    def test_activated_trial_keeps_access_without_saved_card(self):
        self.assertEqual(self.resolve(source="payment")["mode"], "active")

    def test_activated_promo_trial_keeps_access_without_saved_card(self):
        self.assertEqual(self.resolve(source="promo", payment_id="synthetic-confirmed-bind")["mode"], "active")

    def test_bound_trial_remains_active(self):
        self.assertEqual(self.resolve(card=True)["mode"], "active")

    def test_paid_entitlement_does_not_require_saved_card(self):
        for status in ("active", "paid"):
            with self.subTest(status=status):
                self.assertEqual(self.resolve(status=status, source="payment")["mode"], "active")
                self.assertEqual(self.resolve(status=status, source="payment", card=True)["mode"], "active")

    def test_explicit_admin_grants_do_not_require_a_card(self):
        for status, source in (("trial", "admin"), ("active", "admin"), ("paid", "admin"), ("manual", "payment"), ("lifetime", "admin")):
            with self.subTest(status=status, source=source):
                access = self.resolve(status=status, source=source)
                self.assertEqual(access["mode"], "active")
                self.assertEqual(access["access_scope"], "personal_full")
                self.assertFalse(access["has_card"])
                self.assertIsNone(access["recommended_action"])

    def test_expired_or_cancelled_admin_grant_does_not_bypass_paywall(self):
        for status in ("expired", "cancelled"):
            with self.subTest(status=status):
                self.assertEqual(self.resolve(status=status, source="admin")["mode"], "blocked")

    def test_cardless_trial_cannot_use_read_only_grace(self):
        self.assertEqual(self.resolve(grace=True)["mode"], "blocked")

    def test_expired_paid_entitlement_stays_blocked_without_card(self):
        self.assertEqual(self.resolve(status="expired", source="payment")["mode"], "blocked")
