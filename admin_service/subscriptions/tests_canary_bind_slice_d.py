from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from subscriptions.models import BillingConsent, Payment, Plan
from subscriptions.canary import (
    CanaryBindError,
    confirm_canary_bind,
    get_canary_bind_preview,
    init_canary_bind,
)
from users.models import TelegramUser, UserAdminState


@override_settings(
    PRODUCTION_CANARY_ENABLED=True,
    PRODUCTION_CANARY_TELEGRAM_IDS=[92001],
    PRODUCTION_CANARY_ALLOWED_SCENARIOS=["success"],
    PRODUCTION_CANARY_ACTION_TTL_SECONDS=300,
)
class CanaryBindSliceDTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=92001, first_name="Canary", base_currency="UAH")
        UserAdminState.objects.create(telegram_user=self.user, is_test_user=True, is_production_canary=True, status="active")
        self.plan = Plan.objects.create(name="Solo", slug="solo", price=Decimal("1.00"), currency="USD", duration_days=30)
        self.consent = BillingConsent.objects.create(
            user=self.user, offer_id="solo", offer_version="v1", bind_amount_minor=100,
            currency="USD", trial_days=30, renewal_amount_minor=49900, renewal_period_days=30,
            terms_version="terms-v1", privacy_version="privacy-v1", copy_locale="en",
            payload_hash="a" * 64, idempotency_key="00000000-0000-0000-0000-000000000001",
        )

    def test_init_requires_consent_and_returns_canary_checkout(self):
        result = init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan)
        payment = Payment.objects.get(pk=result.payment_id)
        self.assertEqual(payment.provider, "canary")
        self.assertEqual(payment.status, Payment.Status.PENDING)
        self.assertEqual(result.page_url, f"/app/canary/checkout/#token={result.capability_token}")

    def test_confirm_finalizes_once_with_synthetic_values_and_replay_is_idempotent(self):
        init = init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan)
        review_digest = get_canary_bind_preview(
            user=self.user,
            capability_token=init.capability_token,
        )["review_digest"]
        with patch("subscriptions.monobank.create_invoice") as create_invoice:
            first = confirm_canary_bind(
                user=self.user,
                capability_token=init.capability_token,
                confirmed=True,
                review_digest=review_digest,
            )
        self.assertFalse(create_invoice.called)
        second = confirm_canary_bind(
            user=self.user,
            capability_token=init.capability_token,
            confirmed=True,
            review_digest=review_digest,
        )
        self.assertEqual(first.payment_id, second.payment_id)
        payment = Payment.objects.get(pk=first.payment_id)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertTrue(str(payment.raw_payload).find("pan") == -1)

    def test_exact_user_and_explicit_confirmation_are_required(self):
        init = init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan)
        with self.assertRaises(CanaryBindError):
            confirm_canary_bind(user=self.user, capability_token=init.capability_token, confirmed=False)
        other = TelegramUser.objects.create(tg_user_id=92002, first_name="Other")
        with self.assertRaises(CanaryBindError):
            confirm_canary_bind(user=other, capability_token=init.capability_token, confirmed=True)

    @override_settings(PRODUCTION_CANARY_MAX_AMOUNT=50)
    def test_bind_rejects_over_limit_before_any_payment_write(self):
        before = Payment.objects.count()

        with self.assertRaises(CanaryBindError):
            init_canary_bind(user=self.user, consent_id=self.consent.pk, plan=self.plan)

        self.assertEqual(Payment.objects.count(), before)
        self.consent.refresh_from_db()
        self.assertFalse(self.consent.canary_owned)
