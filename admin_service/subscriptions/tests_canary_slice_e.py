from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings
from common.test_helpers import ensure_telegram_user_table
from subscriptions.canary import (
    complete_delayed_canary,
    confirm_canary_bind,
    get_canary_bind_preview,
    init_canary_bind,
)
from subscriptions.models import BillingConsent, Payment, Plan, Subscription
from users.models import TelegramUser, UserAdminState


@override_settings(PRODUCTION_CANARY_ENABLED=True, PRODUCTION_CANARY_TELEGRAM_IDS=[93001],
                   PRODUCTION_CANARY_ALLOWED_SCENARIOS=["insufficient_funds", "declined", "processing", "delayed_success", "delayed_failure", "action_required"])
class CanarySliceETests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass(); ensure_telegram_user_table()

    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=93001, first_name="Canary", base_currency="UAH")
        UserAdminState.objects.create(telegram_user=self.user, is_test_user=True, is_production_canary=True, status="active")
        self.plan = Plan.objects.create(name="Solo", slug="solo", price=Decimal("1.00"), currency="USD", duration_days=30)
        self.consent = BillingConsent.objects.create(user=self.user, offer_id="solo", offer_version="v1", bind_amount_minor=100, currency="USD", trial_days=30, renewal_amount_minor=49900, renewal_period_days=30, terms_version="t", privacy_version="p", copy_locale="en", payload_hash="b"*64, idempotency_key="00000000-0000-0000-0000-000000000002")

    def _run(self, scenario):
        init = init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan, scenario=scenario)
        review_digest = get_canary_bind_preview(
            user=self.user,
            capability_token=init.capability_token,
        )["review_digest"]
        with patch("subscriptions.monobank.create_invoice") as call:
            result = confirm_canary_bind(
                user=self.user,
                capability_token=init.capability_token,
                confirmed=True,
                review_digest=review_digest,
            )
        self.assertFalse(call.called)
        return result

    def test_terminal_failures_do_not_grant_access(self):
        for scenario in ("insufficient_funds", "declined"):
            with self.subTest(scenario=scenario):
                self._run(scenario)
                p = Payment.objects.order_by("-pk").first(); p.refresh_from_db()
                self.assertEqual(p.status, Payment.Status.FAILED)
                self.assertFalse(Subscription.objects.filter(user=self.user).exists())

    def test_processing_action_required_and_delayed_are_not_paid(self):
        for scenario in ("processing", "action_required", "delayed_success", "delayed_failure"):
            with self.subTest(scenario=scenario):
                self._run(scenario)
                p = Payment.objects.order_by("-pk").first(); p.refresh_from_db()
                self.assertNotEqual(p.status, Payment.Status.PAID)
                self.assertFalse(Subscription.objects.filter(user=self.user).exists())

    def test_delayed_completion_requires_explicit_admin_action_and_is_idempotent(self):
        init = init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan, scenario="delayed_success")
        review_digest = get_canary_bind_preview(
            user=self.user,
            capability_token=init.capability_token,
        )["review_digest"]
        confirm_canary_bind(
            user=self.user,
            capability_token=init.capability_token,
            confirmed=True,
            review_digest=review_digest,
        )
        with self.assertRaises(ValueError):
            complete_delayed_canary(user=self.user, capability_token=init.capability_token, confirmed=False)
        first = complete_delayed_canary(user=self.user, capability_token=init.capability_token, confirmed=True)
        second = complete_delayed_canary(user=self.user, capability_token=init.capability_token, confirmed=True)
        self.assertEqual(first.payment_id, second.payment_id)
        self.assertEqual(Payment.objects.get(pk=first.payment_id).status, Payment.Status.PAID)
