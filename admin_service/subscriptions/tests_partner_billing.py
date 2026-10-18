from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings

from common.test_helpers import ensure_telegram_user_table
from miniapp.models import AcquisitionSession, PartnerLink, PartnerTrialClaim
from miniapp.partner_offers import claim_partner_offer
from subscriptions import billing
from subscriptions.models import BillingProfile, Payment, Subscription
from users.models import TelegramUser


NOW = datetime(2030, 1, 15, 12, tzinfo=UTC)


@override_settings(TELEGRAM_BOT_TOKEN="", MONO_RENEWAL_AMOUNT=49900, MONO_RENEWAL_PERIOD_DAYS=30)
class PartnerTrialBillingTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        clock = patch("subscriptions.billing.timezone.now", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)
        notify = patch("subscriptions.billing.send_bind_activation_notification")
        notify.start()
        self.addCleanup(notify.stop)
        self.user = TelegramUser.objects.create(tg_user_id=90123001, created_at=NOW, lang="uk")
        self.link = PartnerLink.objects.create(name="Partner", code="partner-90", trial_days=90, utm_source="test")
        self.acquisition = AcquisitionSession.objects.create(user=self.user, first_partner_link=self.link, last_partner_link=self.link, expires_at=NOW + timedelta(days=1))

    def reserve(self, days=90):
        self.link.trial_days = days
        self.link.save(update_fields=["trial_days"])
        return claim_partner_offer(user=self.user, acquisition=self.acquisition)

    def bind(self, invoice="bind-1", *, token="card-token", status="success", mode="bind", trial_granted=True, trial_days=30, modified=NOW):
        Payment.objects.create(user=self.user, provider="monobank", kind=Payment.Kind.BIND, status=Payment.Status.PENDING, amount=Decimal("0"), currency="UAH", provider_payment_id=invoice, raw_payload={"bind_context": {"mode": mode, "trial_granted": trial_granted, "trial_days": trial_days}})
        payload = {"invoiceId": invoice, "status": status, "modifiedDate": modified.isoformat()}
        if token is not None:
            payload["walletData"] = {"cardToken": token, "maskedPan": "444455******1111"}
        billing.process_monobank_event(payload)

    def test_reservation_creates_no_subscription_and_freezes_exact_duration(self):
        claim = self.reserve(90)
        self.assertEqual(claim.trial_days, 90)
        self.assertFalse(Subscription.objects.exists())
        self.assertIsNone(claim.activated_at)

    def test_confirmed_card_grants_exact_90_days_and_partner_auto_renew_off(self):
        claim = self.reserve(90)
        self.bind(token="token-90")
        subscription = Subscription.objects.get(user=self.user)
        self.assertEqual(subscription.trial_days, 90)
        self.assertEqual(subscription.expires_at, NOW + timedelta(days=90))
        self.assertFalse(subscription.auto_renew)
        claim.refresh_from_db()
        self.assertIsNotNone(claim.activated_at)
        self.assertEqual(BillingProfile.objects.get(user=self.user).card_token, "token-90")

    def test_confirmed_card_preserves_exact_7889_days(self):
        self.reserve(7889)
        self.bind(token="token-7889")
        subscription = Subscription.objects.get(user=self.user)
        self.assertEqual(subscription.trial_days, 7889)
        self.assertEqual(subscription.expires_at, NOW + timedelta(days=7889))

    def test_missing_token_failed_pending_and_stale_callbacks_do_not_grant(self):
        for status, token, modified in (("success", None, NOW), ("failure", "token-fail", NOW), ("processing", "token-pending", NOW)):
            with self.subTest(status=status, token=token):
                self.reserve(90)
                if modified < NOW:
                    Payment.objects.create(user=self.user, provider="monobank", kind=Payment.Kind.BIND, status=Payment.Status.PENDING, amount=Decimal("0"), currency="UAH", provider_payment_id="newer-bind", raw_payload={"bind_context": {"mode": "bind", "trial_granted": True, "trial_days": 90}})
                self.bind(invoice=f"{status}-{token}", token=token, status=status, modified=modified)
                self.assertFalse(Subscription.objects.filter(user=self.user).exists())
                Subscription.objects.all().delete()
                PartnerTrialClaim.objects.all().delete()
                Payment.objects.all().delete()

    def test_stale_callback_cannot_reset_a_newer_verified_trial(self):
        self.reserve(90)
        self.bind(invoice="current", token="current")
        current = Subscription.objects.get(user=self.user)
        self.bind(invoice="stale", token="stale", modified=NOW - timedelta(days=1))
        current.refresh_from_db()
        self.assertEqual(current.trial_days, 90)
        self.assertEqual(current.expires_at, NOW + timedelta(days=90))

    def test_duplicate_and_rebind_cannot_reset_activated_trial(self):
        self.reserve(90)
        self.bind(token="first")
        original = Subscription.objects.get(user=self.user)
        self.bind(invoice="second", token="second", mode="rebind", trial_granted=False, trial_days=7889)
        current = Subscription.objects.get(user=self.user)
        self.assertEqual(current.expires_at, original.expires_at)
        self.assertEqual(current.trial_days, 90)

    def test_legacy_cardless_partner_trial_is_reset_only_after_verified_binding(self):
        self.reserve(90)
        legacy = Subscription.objects.create(user=self.user, provider="monobank", status=Subscription.Status.TRIAL, source=Subscription.Source.PROMO, trial_days=30, started_at=NOW - timedelta(days=30), expires_at=NOW)
        self.bind(token="verified")
        legacy.refresh_from_db()
        self.assertEqual(legacy.trial_days, 90)
        self.assertEqual(legacy.expires_at, NOW + timedelta(days=90))

    def test_paid_and_manual_entitlements_are_untouched(self):
        for status, source in ((Subscription.Status.PAID, Subscription.Source.PAYMENT), (Subscription.Status.MANUAL, Subscription.Source.ADMIN)):
            with self.subTest(status=status):
                subscription = Subscription.objects.create(user=self.user, provider="monobank", status=status, source=source, trial_days=0, started_at=NOW, expires_at=NOW + timedelta(days=365), auto_renew=False)
                self.reserve(90)
                self.bind(invoice=f"{status}-bind", token=f"{status}-token")
                subscription.refresh_from_db()
                self.assertEqual(subscription.status, status)
                self.assertEqual(subscription.expires_at, NOW + timedelta(days=365))
                Subscription.objects.all().delete()
                PartnerTrialClaim.objects.all().delete()
