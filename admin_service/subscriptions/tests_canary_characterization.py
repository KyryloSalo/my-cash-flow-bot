from datetime import timedelta
from decimal import Decimal
import uuid
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from subscriptions.billing import (
    ProviderPaymentEvent,
    finalize_payment_event,
    parse_monobank_payment_event,
    process_monobank_event,
)
from subscriptions.models import BillingConsent, BillingProfile, Payment, Plan, Subscription, SubscriptionEvent
from users.models import TelegramUser, UserAdminState


@override_settings(TELEGRAM_BOT_TOKEN="test-token")
class MonobankFinalizationCharacterizationTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.user = TelegramUser.objects.create(
            tg_user_id=81001,
            first_name="Characterization",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.plan = Plan.objects.create(
            name="Solo", slug="solo", price=Decimal("10.00"), currency="USD", duration_days=30
        )

    def _payment(self, *, kind, invoice_id, status=Payment.Status.PENDING, subscription=None, raw_payload=None):
        return Payment.objects.create(
            user=self.user,
            plan=self.plan if kind == Payment.Kind.BIND else None,
            subscription=subscription,
            provider="monobank",
            provider_payment_id=invoice_id,
            amount=Decimal("1.00" if kind == Payment.Kind.BIND else "499.00"),
            currency="UAH",
            status=status,
            kind=kind,
            raw_payload=raw_payload or {},
        )

    def test_monobank_parser_returns_provider_neutral_event_without_leaking_raw_mutability(self):
        payload = {
            "invoiceId": "char-normalized",
            "status": "success",
            "amount": 49900,
            "ccy": 980,
            "modifiedDate": 1_715_280_200_000,
            "walletData": {"cardToken": "tok-parser", "maskedPan": "444455******1111"},
        }

        event = parse_monobank_payment_event(payload)

        self.assertIsInstance(event, ProviderPaymentEvent)
        self.assertEqual(event.provider, "monobank")
        self.assertEqual(event.provider_payment_id, "char-normalized")
        self.assertEqual(event.status, Payment.Status.PAID)
        self.assertEqual(event.amount, 49900)
        self.assertEqual(event.currency, "UAH")
        self.assertEqual(event.card_token, "tok-parser")
        self.assertEqual(event.masked_pan, "444455******1111")
        self.assertEqual(event.raw_payload, payload)
        self.assertIsNot(event.raw_payload, payload)

    def test_provider_neutral_finalizer_preserves_bind_success_behavior(self):
        payment = self._payment(kind=Payment.Kind.BIND, invoice_id="char-neutral-finalizer")
        event = parse_monobank_payment_event({
            "invoiceId": payment.provider_payment_id,
            "status": "success",
            "modifiedDate": 1_715_280_200_000,
            "walletData": {"cardToken": "tok-neutral", "maskedPan": "444455******1111"},
        })

        processed, updated = finalize_payment_event(event)

        self.assertTrue(updated)
        self.assertEqual(processed.pk, payment.pk)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(BillingProfile.objects.get(user=self.user).card_token, "tok-neutral")

    def test_bind_success_finalizes_consent_profile_trial_and_event(self):
        consent = BillingConsent.objects.create(
            user=self.user,
            idempotency_key=uuid.uuid4(),
            offer_id="solo",
            offer_version="v1",
            bind_amount_minor=100,
            currency="UAH",
            trial_days=30,
            renewal_amount_minor=49900,
            renewal_period_days=30,
            terms_version="v1",
            privacy_version="v1",
            copy_locale="uk",
            payload_hash="hash-bind",
            accepted_at=timezone.now(),
        )
        payment = self._payment(
            kind=Payment.Kind.BIND,
            invoice_id="char-bind-success",
            raw_payload={
                "bind_context": {
                    "mode": "bind",
                    "trial_days": 30,
                    "trial_granted": True,
                }
            },
        )
        consent.payment = payment
        consent.save(update_fields=["payment"])

        processed, updated = process_monobank_event({
            "invoiceId": payment.provider_payment_id,
            "status": "success",
            "modifiedDate": 1_715_280_200_000,
            "walletData": {"cardToken": "tok-characterization", "maskedPan": "444455******1111"},
        })

        self.assertTrue(updated)
        self.assertEqual(processed.pk, payment.pk)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(BillingProfile.objects.get(user=self.user).card_token, "tok-characterization")
        self.assertEqual(Subscription.objects.get(user=self.user).status, Subscription.Status.TRIAL)
        self.assertEqual(SubscriptionEvent.objects.filter(user=self.user).count(), 1)
        consent.refresh_from_db()
        self.assertIsNotNone(consent.first_renewal_at)

    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    def test_bind_failure_is_terminal_and_notifies_once(self, send_mock):
        payment = self._payment(kind=Payment.Kind.BIND, invoice_id="char-bind-failure")
        payload = {
            "invoiceId": payment.provider_payment_id,
            "status": "failure",
            "modifiedDate": 1_715_280_200_000,
            "failureReason": "declined",
        }

        _, first_updated = process_monobank_event(payload)
        _, second_updated = process_monobank_event({**payload, "modifiedDate": 1_715_280_300_000})

        self.assertTrue(first_updated)
        self.assertTrue(second_updated)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.FAILED)
        self.assertEqual(send_mock.call_count, 1)
        self.assertFalse(Subscription.objects.filter(user=self.user).exists())

    def test_renewal_success_and_failure_preserve_current_state_rules(self):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.user,
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=28),
            expires_at=now + timedelta(days=2),
            next_charge_at=now + timedelta(days=2),
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user=self.user, provider="monobank", wallet_id="mono-char", card_token="tok-renew",
            masked_pan="444455******1111", status=BillingProfile.Status.ACTIVE, auto_renew_enabled=True,
        )
        success = self._payment(kind=Payment.Kind.RENEWAL, invoice_id="char-renew-success", subscription=subscription,
                                raw_payload={"charge_context": {"kind": Payment.Kind.RENEWAL, "wallet_id": "mono-char"}})
        process_monobank_event({"invoiceId": success.provider_payment_id, "status": "success", "modifiedDate": 1_715_280_200_000})
        success.refresh_from_db()
        subscription.refresh_from_db()
        self.assertEqual(success.status, Payment.Status.PAID)
        self.assertGreater(subscription.expires_at, now + timedelta(days=2))

        failed = self._payment(kind=Payment.Kind.RENEWAL, invoice_id="char-renew-failure", subscription=subscription,
                               raw_payload={"charge_context": {"kind": Payment.Kind.RENEWAL, "wallet_id": "mono-char"}})
        process_monobank_event({"invoiceId": failed.provider_payment_id, "status": "failure", "modifiedDate": 1_715_280_300_000,
                                "failureReason": "declined"})
        failed.refresh_from_db()
        self.assertEqual(failed.status, Payment.Status.FAILED)

    def test_duplicate_and_out_of_order_events_do_not_regress_paid_payment_or_token(self):
        payment = self._payment(kind=Payment.Kind.BIND, invoice_id="char-ordering")
        success = {"invoiceId": payment.provider_payment_id, "status": "success", "modifiedDate": 1_715_280_200_000,
                   "walletData": {"cardToken": "tok-first", "maskedPan": "444455******1111"}}
        process_monobank_event(success)
        _, duplicate = process_monobank_event(success)
        _, older = process_monobank_event({**success, "status": "failure", "modifiedDate": 1_715_280_100_000,
                                            "walletData": {"cardToken": "tok-second"}})
        payment.refresh_from_db()
        self.assertFalse(duplicate)
        self.assertFalse(older)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(BillingProfile.objects.get(user=self.user).card_token, "tok-first")

    def test_unknown_and_processing_are_non_finalizing_and_payload_is_sanitized(self):
        unknown, unknown_updated = process_monobank_event({"invoiceId": "missing-characterization", "status": "success"})
        self.assertIsNone(unknown)
        self.assertFalse(unknown_updated)

        payment = self._payment(kind=Payment.Kind.BIND, invoice_id="char-processing")
        processed, updated = process_monobank_event({
            "invoiceId": payment.provider_payment_id,
            "status": "processing",
            "modifiedDate": 1_715_280_200_000,
            "walletData": {"cardToken": "tok-secret", "pan": "4444550011112222"},
        })
        self.assertTrue(updated)
        self.assertEqual(processed.status, Payment.Status.PENDING)
        payment.refresh_from_db()
        webhook = payment.raw_payload["webhook_payload"]
        self.assertNotEqual(webhook["walletData"]["cardToken"], "tok-secret")
        self.assertNotEqual(webhook["walletData"]["pan"], "4444550011112222")
        self.assertFalse(Subscription.objects.filter(user=self.user).exists())
