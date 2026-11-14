"""Database regressions for partner consent; no provider/card integration is used.

Discovered by the canonical ``python manage.py test`` command. Run with the
normal PostgreSQL test database; all financial persistence uses the real ORM.
"""
from datetime import timedelta
from decimal import Decimal
import json
import uuid
from unittest.mock import patch

from django.test import RequestFactory, TestCase, override_settings
from django.utils import timezone

from miniapp.funnel import FunnelValidationError
from miniapp.funnel_views import billing_consent
from miniapp.models import AcquisitionSession, FunnelEvent, PartnerLink
from miniapp.partner_offers import claim_partner_offer
from subscriptions.consent import (
    accept_billing_consent,
    attach_consent_payment,
    finalize_paid_consent,
)
from subscriptions.models import BillingConsent, BillingProfile, Payment
from users.models import TelegramUser


@override_settings(MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900,
                   MONO_RENEWAL_PERIOD_DAYS=30)
class PartnerBillingConsentTests(TestCase):
    def setUp(self):
        # Fail closed if a future change attempts to reach a payment provider.
        provider = patch("subscriptions.billing.create_invoice",
                         side_effect=AssertionError("provider traffic is forbidden"))
        provider.start()
        self.addCleanup(provider.stop)
        self.user = TelegramUser.objects.create(tg_user_id=84251001,
                                                first_name="Synthetic partner")
        self.link = PartnerLink.objects.create(name="Synthetic partner", code="consent-regression",
                                               trial_days=51)
        self.acquisition = AcquisitionSession.objects.create(
            user=self.user, first_partner_link=self.link, last_partner_link=self.link,
            expires_at=timezone.now() + timedelta(days=1),
        )
        self.claim = claim_partner_offer(user=self.user, acquisition=self.acquisition)
        self.offer_id = f"partner_link:{self.link.pk}"
        self.key = str(uuid.uuid4())

    def accept(self):
        return accept_billing_consent(
            user=self.user, acquisition=self.acquisition, idempotency_key=self.key,
            trial_days=30, copy_locale="uk",
        )

    def payment(self):
        return Payment.objects.create(user=self.user, amount=Decimal("1.00"), currency="UAH",
                                      kind=Payment.Kind.BIND, status=Payment.Status.PAID,
                                      provider="synthetic-local-only")

    def assert_event(self, name, key):
        events = FunnelEvent.objects.filter(acquisition_session=self.acquisition, event_name=name)
        self.assertEqual(events.count(), 1)
        event = events.get()
        self.assertEqual(event.offer_variant, self.offer_id)
        self.assertEqual(event.idempotency_key, key)
        self.assertEqual(event.acquisition_session.user_id, self.user.pk)

    def test_consent_freezes_claim_terms_and_replays_without_financial_side_effects(self):
        self.assertEqual(self.claim.offer_id, self.offer_id)
        # Editing the source link must not change the offer the user claimed.
        PartnerLink.objects.filter(pk=self.link.pk).update(trial_days=9, code="edited-partner")
        consent, created = self.accept()
        replay, created_again = self.accept()
        consent.refresh_from_db()
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(replay.pk, consent.pk)
        self.assertEqual(replay.payload_hash, consent.payload_hash)
        self.assertEqual(consent.offer_id, self.offer_id)
        self.assertEqual(consent.trial_days, 51)
        self.assertEqual(consent.bind_amount_minor, 100)
        self.assertEqual(consent.currency, "UAH")
        self.assertEqual(consent.renewal_amount_minor, 49900)
        self.assertEqual(consent.renewal_period_days, 30)
        self.assertEqual(consent.renewal_anchor, "disabled")
        self.assertIsNone(consent.first_renewal_at)
        self.assertEqual(BillingConsent.objects.filter(user=self.user).count(), 1)
        self.assertFalse(Payment.objects.exists())
        self.assertFalse(BillingProfile.objects.exists())
        self.assert_event("billing_consent_accepted", f"consent:{consent.pk}")

    def test_invoice_attachment_persists_once_with_exact_offer_id(self):
        consent, _ = self.accept()
        payment = self.payment()
        attach_consent_payment(consent=consent, payment=payment)
        attach_consent_payment(consent=consent, payment=payment)
        consent.refresh_from_db()
        self.assertEqual(consent.payment_id, payment.pk)
        self.assertEqual(consent.offer_id, self.offer_id)
        self.assertEqual(consent.trial_days, 51)
        self.assert_event("bind_invoice_created", f"bind_invoice:{payment.pk}")

    def test_paid_finalization_deduplicates_and_preserves_first_renewal_timestamp(self):
        consent, _ = self.accept()
        payment = self.payment()
        attach_consent_payment(consent=consent, payment=payment)
        first_renewal_at = timezone.now() + timedelta(days=51)
        self.assertEqual(finalize_paid_consent(payment=payment, first_renewal_at=first_renewal_at), 1)
        self.assertEqual(finalize_paid_consent(payment=payment,
                         first_renewal_at=first_renewal_at + timedelta(days=1)), 0)
        self.assertEqual(finalize_paid_consent(payment=payment, first_renewal_at=None), 0)
        consent.refresh_from_db()
        self.assertEqual(consent.first_renewal_at, first_renewal_at)
        self.assertEqual(consent.offer_id, self.offer_id)
        self.assertEqual(consent.trial_days, 51)
        self.assert_event("billing_consent_accepted", f"consent:{consent.pk}")
        self.assert_event("bind_invoice_created", f"bind_invoice:{payment.pk}")
        self.assert_event("payment_success", f"payment:{payment.pk}:success")
        self.assertEqual(FunnelEvent.objects.filter(acquisition_session=self.acquisition).count(), 3)

    def test_attachment_rejects_replacement_payment_without_changing_persisted_binding(self):
        consent, _ = self.accept()
        payment = self.payment()
        attach_consent_payment(consent=consent, payment=payment)
        replacement = self.payment()
        with self.assertRaises(FunnelValidationError) as raised:
            attach_consent_payment(consent=consent, payment=replacement)
        self.assertEqual(raised.exception.code, "consent_payment_conflict")
        consent.refresh_from_db()
        self.assertEqual(consent.payment_id, payment.pk)
        self.assert_event("bind_invoice_created", f"bind_invoice:{payment.pk}")

    def test_explicit_confirmation_is_required_before_any_consent_write(self):
        for accepted in (False, None, "true", 1):
            with self.subTest(accepted=accepted):
                request = RequestFactory().post(
                    "/app/api/funnel/billing-consent",
                    data=json.dumps({"accepted": accepted, "idempotency_key": self.key}),
                    content_type="application/json",
                )
                response = billing_consent(request)
                self.assertEqual(response.status_code, 400)
                self.assertEqual(json.loads(response.content)["error"]["code"], "consent_required")
        self.assertFalse(BillingConsent.objects.exists())
        self.assertFalse(FunnelEvent.objects.exists())
        self.assertFalse(Payment.objects.exists())
        self.assertFalse(BillingProfile.objects.exists())
