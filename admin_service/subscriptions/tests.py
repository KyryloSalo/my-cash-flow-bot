from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from subscriptions.billing import (
    build_bind_return_token,
    build_bind_invoice,
    build_recovery_invoice,
    cancel_auto_renew,
    get_latest_refundable_monobank_payment,
    parse_bind_return_token,
    process_monobank_event,
    reconcile_pending_monobank_binds,
    reconcile_pending_monobank_charges,
    retry_monobank_charge,
    refund_latest_monobank_payment,
    run_due_monobank_charges,
    sync_pending_charge_status,
    sync_pending_bind_status,
)
from subscriptions.models import BillingProfile, Payment, Plan, PromoOffer, PromoOfferClaim, Subscription
from subscriptions.monobank import MonobankAPIError, verify_webhook_signature
from subscriptions.payloads import REDACTED, sanitize_monobank_payload
from subscriptions.services import apply_subscription_change, confirm_payment
from users.models import TelegramUser, UserAdminState


class MonobankPayloadSanitizationTests(SimpleTestCase):
    def test_sanitize_monobank_payload_redacts_nested_card_token_and_pan(self):
        payload = {
            "walletData": {
                "cardToken": "tok-live",
                "maskedPan": "444455******1111",
                "pan": "4444550011112222",
            },
            "items": [{"cardToken": "nested-token"}],
        }

        sanitized = sanitize_monobank_payload(payload)

        self.assertEqual(sanitized["walletData"]["cardToken"], REDACTED)
        self.assertEqual(sanitized["walletData"]["maskedPan"], "444455******1111")
        self.assertEqual(sanitized["walletData"]["pan"], REDACTED)
        self.assertEqual(sanitized["items"][0]["cardToken"], REDACTED)


class MonobankWebhookSignatureVerificationTests(SimpleTestCase):
    EXAMPLE_PUBKEY = (
        "LS0tLS1CRUdJTiBQVUJMSUMgS0VZLS0tLS0KTUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJemowREFRY0RRZ0FFQUc1LzZ3NnZu"
        "bGJZb0ZmRHlYWE4vS29CbVVjTgo3NWJSUWg4MFBhaEdldnJoanFCQnI3OXNSS0JSbnpHODFUZVQ5OEFOakU1c0R3RmZ5"
        "Znhub0ZJcmZBPT0KLS0tLS1FTkQgUFVCTElDIEtFWS0tLS0tCg=="
    )
    EXAMPLE_SIGNATURE = "MEUCIQC/mVKhi8FKoayul2Mim3E2oaIOCNJk5dEXxTqbkeJSOQIgOM0hsW0qcP2H8iXy1aQYpmY0SJWEaWur7nQXlKDCFxA="
    EXAMPLE_BODY = b"""{
  "invoiceId": "p2_9ZgpZVsl3",
  "status": "created",
  "failureReason": "string",
  "amount": 4200,
  "ccy": 980,
  "finalAmount": 4200,
  "createdDate": "2019-08-24T14:15:22Z",
  "modifiedDate": "2019-08-24T14:15:22Z",
  "reference": "84d0070ee4e44667b31371d8f8813947",
  "cancelList": [
    {
      "status": "processing",
      "amount": 4200,
      "ccy": 980,
      "createdDate": "2019-08-24T14:15:22Z",
      "modifiedDate": "2019-08-24T14:15:22Z",
      "approvalCode": "662476",
      "rrn": "060189181768",
      "extRef": "635ace02599849e981b2cd7a65f417fe"
    }
  ]
}"""

    def tearDown(self):
        from subscriptions.monobank import _cached_webhook_verify_key

        _cached_webhook_verify_key.cache_clear()

    @patch("subscriptions.monobank.fetch_pubkey")
    def test_verify_webhook_signature_accepts_official_monobank_example(self, mock_fetch_pubkey):
        mock_fetch_pubkey.return_value = self.EXAMPLE_PUBKEY

        self.assertTrue(verify_webhook_signature(body=self.EXAMPLE_BODY, signature=self.EXAMPLE_SIGNATURE))

    @patch("subscriptions.monobank._get_webhook_verify_key")
    @patch("subscriptions.monobank._verify_signature_with_key")
    def test_verify_webhook_signature_retries_once_with_refreshed_key(self, mock_verify_with_key, mock_get_webhook_verify_key):
        first_key = object()
        second_key = object()
        mock_get_webhook_verify_key.side_effect = [first_key, second_key]
        mock_verify_with_key.side_effect = [False, True]

        result = verify_webhook_signature(body=b"{}", signature="signed")

        self.assertTrue(result)
        self.assertEqual(mock_get_webhook_verify_key.call_args_list[0].kwargs, {})
        self.assertEqual(mock_get_webhook_verify_key.call_args_list[1].kwargs, {"refresh": True})

    @patch("subscriptions.views.sync_pending_charge_status")
    def helper_monobank_return_pending_recovery_syncs_charge_status_without_bind_push(self, mock_sync_pending_charge_status):
        payment = self._recovery_payment(invoice_id="recovery-processing-1")
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="recovery")
        mock_sync_pending_charge_status.return_value = {
            "updated": False,
            "monobank_status": "processing",
            "payment_id": payment.pk,
        }

        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Оплата обробляється")
        mock_sync_pending_charge_status.assert_called_once_with(user_id=self.telegram_user.tg_user_id)
        send_mock.assert_not_called()


@override_settings(
    TELEGRAM_BOT_TOKEN="",
    MONO_RENEWAL_PERIOD_DAYS=30,
    MONO_GRACE_DAYS=30,
    MONO_SOFT_GRACE_DAYS=1,
)
class SubscriptionFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=1001,
            first_name="Test",
            username="tester",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.plan = Plan.objects.create(name="Solo", slug="solo", price=Decimal("10.00"), currency="USD", duration_days=30)

    def test_apply_subscription_change_creates_trial_then_extend(self):
        subscription = apply_subscription_change(
            user=self.telegram_user,
            action="trial",
            admin_user=self.admin_user,
            plan_slug=self.plan.slug,
            plan_ref=self.plan,
            days=7,
        )
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertIsNotNone(subscription.expires_at)

        extended = apply_subscription_change(
            user=self.telegram_user,
            action="extend",
            admin_user=self.admin_user,
            plan_slug=self.plan.slug,
            plan_ref=self.plan,
            days=30,
        )
        self.assertEqual(extended.status, Subscription.Status.ACTIVE)
        self.assertGreater(extended.expires_at, subscription.expires_at)
        state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.PAID)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)
        self.assertEqual(state.access_source, "billing")

    def test_apply_subscription_change_manual_grant_then_expire_updates_access_scope(self):
        subscription = apply_subscription_change(
            user=self.telegram_user,
            action="manual",
            admin_user=self.admin_user,
            plan_slug=self.plan.slug,
            plan_ref=self.plan,
            days=30,
            amount=Decimal("10.00"),
            currency="USD",
        )
        self.assertEqual(subscription.status, Subscription.Status.MANUAL)
        state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.PAID)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)
        self.assertEqual(state.access_source, "billing")

        expired = apply_subscription_change(
            user=self.telegram_user,
            action="expire",
            admin_user=self.admin_user,
        )
        self.assertEqual(expired.status, Subscription.Status.EXPIRED)
        state.refresh_from_db()
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.EXPIRED)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PAYWALL)
        self.assertEqual(state.access_source, "billing")

    def test_confirm_payment_creates_or_extends_subscription(self):
        payment = Payment.objects.create(
            user=self.telegram_user,
            plan=self.plan,
            provider="manual",
            amount=Decimal("10.00"),
            currency="USD",
            status=Payment.Status.PENDING,
        )
        subscription = confirm_payment(payment=payment, admin_user=self.admin_user)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.MANUAL_CONFIRMED)
        self.assertEqual(subscription.plan_ref, self.plan)
        self.assertEqual(subscription.user_id, self.telegram_user.tg_user_id)

    def test_confirm_payment_tolerates_missing_related_plan(self):
        class FakePayment:
            def __init__(self, *, user, plan_id, amount, currency, status, pk):
                self.user = user
                self.plan_id = plan_id
                self.provider = "manual"
                self.amount = amount
                self.currency = currency
                self.status = status
                self.subscription = None
                self.subscription_id = None
                self.paid_at = None
                self.confirmed_by = None
                self.pk = pk
                self.saved_update_fields = None

            def save(self, *, update_fields):
                self.saved_update_fields = list(update_fields)

        payment = FakePayment(
            user=self.telegram_user,
            plan_id=self.plan.pk,
            amount=Decimal("10.00"),
            currency="USD",
            status=Payment.Status.PENDING,
            pk=999,
        )
        subscription = confirm_payment(payment=payment, admin_user=self.admin_user)

        self.assertEqual(payment.status, Payment.Status.MANUAL_CONFIRMED)
        self.assertEqual(payment.saved_update_fields, ["subscription", "status", "paid_at", "confirmed_by", "updated_at"])
        self.assertEqual(subscription.plan_ref, self.plan)
        self.assertEqual(subscription.user_id, self.telegram_user.tg_user_id)

    def _bind_payment(
        self,
        *,
        invoice_id: str,
        trial_days: int,
        mode: str = "bind",
        trial_granted: bool = True,
        promo_code: str = "",
        promo_offer_id: int | None = None,
    ) -> Payment:
        return Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id=invoice_id,
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.BIND,
            raw_payload={
                "bind_context": {
                    "trial_days": trial_days,
                    "mode": mode,
                    "trial_granted": trial_granted,
                    "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                    "promo_code": promo_code,
                    "promo_offer_id": promo_offer_id,
                }
            },
        )

    def _recovery_payment(
        self,
        *,
        invoice_id: str,
        subscription: Subscription | None = None,
        status: str = Payment.Status.PENDING,
        page_url: str = "https://mono.test/recovery",
    ) -> Payment:
        return Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id=invoice_id,
            amount=Decimal("499.00"),
            currency="UAH",
            status=status,
            kind=Payment.Kind.RETRY,
            paid_at=timezone.now() if status == Payment.Status.PAID else None,
            raw_payload={
                "charge_context": {
                    "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                    "kind": Payment.Kind.RETRY,
                },
                "recovery_context": {
                    "save_card": True,
                    "replace_card": True,
                    "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                    "return_token": build_bind_return_token(
                        user_id=self.telegram_user.tg_user_id,
                        flow="recovery",
                    ),
                    "page_url": page_url,
                },
            },
        )

    def test_monobank_bind_success_activates_30_day_trial(self):
        payment = self._bind_payment(invoice_id="bind-30", trial_days=30)

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-30",
                "status": "success",
                "modifiedDate": 1_715_280_000_000,
                "walletData": {"cardToken": "tok-30", "maskedPan": "444455******1111"},
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        payment.refresh_from_db()
        subscription = Subscription.objects.get(user_id=self.telegram_user.tg_user_id)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(subscription.trial_days, 30)
        self.assertIsNotNone(subscription.next_charge_at)
        self.assertEqual(subscription.next_charge_at, subscription.expires_at)
        self.assertEqual(profile.card_token, "tok-30")
        self.assertEqual(payment.raw_payload["webhook_payload"]["walletData"]["cardToken"], REDACTED)
        self.assertTrue(profile.auto_renew_enabled)

    def test_monobank_bind_success_activates_90_day_trial(self):
        payment = self._bind_payment(invoice_id="bind-90", trial_days=90)

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-90",
                "status": "success",
                "modifiedDate": 1_715_280_100_000,
                "walletData": {"cardToken": "tok-90", "maskedPan": "555566******2222"},
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        payment.refresh_from_db()
        subscription = Subscription.objects.get(user_id=self.telegram_user.tg_user_id)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(subscription.trial_days, 90)
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertGreater(subscription.expires_at, timezone.now() + timedelta(days=89))
        self.assertEqual(profile.card_token, "tok-90")

    @patch("subscriptions.billing.create_invoice")
    def test_build_bind_invoice_applies_active_promo_offer(self, mock_create_invoice):
        PromoOffer.objects.create(
            code="PROMO90",
            label="Promo 90",
            trial_days=90,
            source="campaign",
            is_active=True,
        )
        mock_create_invoice.return_value = {"invoiceId": "promo-bind-1", "pageUrl": "https://mono.test/promo"}

        result = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind", promo_code="promo90")

        payment = Payment.objects.get(provider_payment_id="promo-bind-1")
        claim = PromoOfferClaim.objects.get(user=self.telegram_user)
        self.assertEqual(result["trial_days"], 90)
        self.assertTrue(result["trial_granted"])
        self.assertEqual(result["promo_code"], "PROMO90")
        self.assertEqual(payment.raw_payload["bind_context"]["promo_code"], "PROMO90")
        self.assertEqual(payment.raw_payload["bind_context"]["promo_offer_id"], claim.offer_id)
        self.assertEqual(claim.status, PromoOfferClaim.Status.PENDING)

    @patch("subscriptions.billing.create_invoice")
    def test_build_bind_invoice_for_existing_subscription_switches_to_rebind_without_new_trial(self, mock_create_invoice):
        now = timezone.now()
        Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=35),
            expires_at=now - timedelta(days=5),
            trial_days=30,
        )
        mock_create_invoice.return_value = {"invoiceId": "bind-rebind-1", "pageUrl": "https://mono.test/rebind"}

        result = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind")

        payment = Payment.objects.get(provider_payment_id="bind-rebind-1")
        self.assertEqual(result["mode"], "rebind")
        self.assertEqual(result["trial_days"], 0)
        self.assertFalse(result["trial_granted"])
        self.assertEqual(payment.raw_payload["bind_context"]["mode"], "rebind")
        self.assertEqual(payment.raw_payload["bind_context"]["trial_days"], 0)
        self.assertFalse(payment.raw_payload["bind_context"]["trial_granted"])

    @patch("subscriptions.billing.create_invoice")
    def test_build_bind_invoice_appends_signed_return_token(self, mock_create_invoice):
        mock_create_invoice.return_value = {"invoiceId": "bind-return-1", "pageUrl": "https://mono.test/return"}

        build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind")

        redirect_url = mock_create_invoice.call_args.args[0]["redirectUrl"]
        parsed = urlparse(redirect_url)
        return_token = parse_qs(parsed.query).get("return_token", [""])[0]
        payment = Payment.objects.get(provider_payment_id="bind-return-1")
        payload = parse_bind_return_token(return_token)

        self.assertTrue(return_token)
        self.assertEqual(parsed.path, "/billing/mono/return")
        self.assertEqual(payload["telegram_user_id"], self.telegram_user.tg_user_id)
        self.assertEqual(payload["flow"], "bind")
        self.assertEqual(payment.raw_payload["bind_context"]["return_token"], return_token)

    @patch("subscriptions.billing.create_invoice")
    def test_build_bind_invoice_reuses_existing_pending_checkout(self, mock_create_invoice):
        mock_create_invoice.return_value = {"invoiceId": "bind-reuse-1", "pageUrl": "https://mono.test/reuse"}

        first = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind")
        second = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=90, mode="bind")

        self.assertFalse(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(second["payment_id"], first["payment_id"])
        self.assertEqual(second["invoice_id"], "bind-reuse-1")
        self.assertEqual(second["page_url"], "https://mono.test/reuse")
        self.assertEqual(second["trial_days"], 30)
        self.assertEqual(Payment.objects.filter(user=self.telegram_user, kind=Payment.Kind.BIND).count(), 1)
        mock_create_invoice.assert_called_once()
        self.assertEqual(mock_create_invoice.call_args.args[0]["validity"], 3600)

    @override_settings(MONO_BIND_INVOICE_VALIDITY_SECONDS=300)
    @patch("subscriptions.billing.create_invoice")
    def test_build_bind_invoice_creates_new_checkout_after_previous_expired(self, mock_create_invoice):
        mock_create_invoice.side_effect = [
            {"invoiceId": "bind-expired-1", "pageUrl": "https://mono.test/expired"},
            {"invoiceId": "bind-fresh-2", "pageUrl": "https://mono.test/fresh"},
        ]
        first = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind")
        Payment.objects.filter(pk=first["payment_id"]).update(created_at=timezone.now() - timedelta(minutes=6))

        second = build_bind_invoice(user_id=self.telegram_user.tg_user_id, trial_days=30, mode="bind")

        self.assertFalse(second["reused"])
        self.assertEqual(second["invoice_id"], "bind-fresh-2")
        self.assertEqual(Payment.objects.filter(user=self.telegram_user, kind=Payment.Kind.BIND).count(), 2)
        self.assertEqual(mock_create_invoice.call_count, 2)

    @patch("subscriptions.billing.create_invoice")
    def test_build_recovery_invoice_creates_pending_retry_checkout_with_save_card_data(self, mock_create_invoice):
        now = timezone.now()
        Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=35),
            expires_at=now - timedelta(days=5),
            trial_days=30,
            auto_renew=False,
        )
        mock_create_invoice.return_value = {"invoiceId": "recovery-1", "pageUrl": "https://mono.test/recovery"}

        result = build_recovery_invoice(user_id=self.telegram_user.tg_user_id)

        payment = Payment.objects.get(provider_payment_id="recovery-1")
        profile = BillingProfile.objects.get(user=self.telegram_user)
        create_payload = mock_create_invoice.call_args.args[0]
        redirect_url = create_payload["redirectUrl"]
        parsed = urlparse(redirect_url)
        return_token = parse_qs(parsed.query).get("return_token", [""])[0]
        token_payload = parse_bind_return_token(return_token)

        self.assertEqual(
            result,
            {
                "payment_id": payment.pk,
                "invoice_id": "recovery-1",
                "page_url": "https://mono.test/recovery",
            },
        )
        self.assertEqual(payment.kind, Payment.Kind.RETRY)
        self.assertEqual(payment.status, Payment.Status.PENDING)
        self.assertEqual(payment.amount, Decimal("499.00"))
        self.assertEqual(parsed.path, "/billing/mono/return")
        self.assertEqual(token_payload["telegram_user_id"], self.telegram_user.tg_user_id)
        self.assertEqual(token_payload["flow"], "recovery")
        self.assertTrue(create_payload["saveCardData"]["saveCard"])
        self.assertEqual(create_payload["saveCardData"]["walletId"], profile.wallet_id)
        self.assertTrue(payment.raw_payload["recovery_context"]["save_card"])
        self.assertTrue(payment.raw_payload["recovery_context"]["replace_card"])
        self.assertEqual(payment.raw_payload["recovery_context"]["return_token"], return_token)
        self.assertEqual(payment.raw_payload["recovery_context"]["page_url"], "https://mono.test/recovery")
        self.assertEqual(profile.status, BillingProfile.Status.PENDING)
        self.assertEqual(profile.last_action_url, "https://mono.test/recovery")

    @patch("subscriptions.billing.create_invoice")
    def test_build_recovery_invoice_reuses_existing_pending_checkout(self, mock_create_invoice):
        payment = self._recovery_payment(
            invoice_id="recovery-pending-1",
            page_url="https://mono.test/recovery-pending",
        )

        result = build_recovery_invoice(user_id=self.telegram_user.tg_user_id)

        self.assertEqual(
            result,
            {
                "payment_id": payment.pk,
                "invoice_id": "recovery-pending-1",
                "page_url": "https://mono.test/recovery-pending",
            },
        )
        mock_create_invoice.assert_not_called()

    def test_monobank_bind_success_consumes_promo_claim_and_marks_promo_source(self):
        offer = PromoOffer.objects.create(
            code="PROMO90",
            label="Promo 90",
            trial_days=90,
            source="campaign",
            is_active=True,
        )
        payment = Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="promo-bind-90",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.BIND,
            raw_payload={
                "bind_context": {
                    "trial_days": 90,
                    "mode": "bind",
                    "trial_granted": True,
                    "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                    "promo_code": "PROMO90",
                    "promo_offer_id": offer.pk,
                }
            },
        )
        PromoOfferClaim.objects.create(offer=offer, user=self.telegram_user, status=PromoOfferClaim.Status.PENDING)

        process_monobank_event(
            {
                "invoiceId": "promo-bind-90",
                "status": "success",
                "modifiedDate": 1_715_280_150_000,
                "walletData": {"cardToken": "tok-promo-90", "maskedPan": "555566******9999"},
            }
        )

        payment.refresh_from_db()
        subscription = Subscription.objects.get(user_id=self.telegram_user.tg_user_id)
        claim = PromoOfferClaim.objects.get(offer=offer, user=self.telegram_user)
        self.assertEqual(subscription.trial_days, 90)
        self.assertEqual(subscription.source, Subscription.Source.PROMO)
        self.assertEqual(subscription.promo_offer_id, offer.pk)
        self.assertEqual(claim.status, PromoOfferClaim.Status.CONSUMED)
        self.assertEqual(claim.bind_payment_id, payment.pk)
        self.assertEqual(claim.subscription_id, subscription.pk)

    def test_monobank_recovery_success_refreshes_expired_subscription_and_replaces_card(self):
        expires_at = timezone.now() - timedelta(days=5)
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=expires_at - timedelta(days=30),
            expires_at=expires_at,
            grace_expires_at=timezone.now() + timedelta(hours=6),
            trial_days=30,
            auto_renew=False,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="tok-old",
            masked_pan="444455******1111",
            status=BillingProfile.Status.FAILED,
            auto_renew_enabled=False,
            last_failure_reason="insufficient funds",
        )
        payment = self._recovery_payment(invoice_id="recovery-paid-1", subscription=subscription)

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "recovery-paid-1",
                "status": "success",
                "modifiedDate": 1_715_280_175_000,
                "walletData": {"cardToken": "tok-recovery", "maskedPan": "444455******7777"},
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        payment.refresh_from_db()
        subscription.refresh_from_db()
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        admin_state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        self.assertEqual(Subscription.objects.filter(user_id=self.telegram_user.tg_user_id).count(), 1)
        self.assertEqual(payment.subscription_id, subscription.pk)
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertEqual(subscription.trial_days, 30)
        self.assertGreater(subscription.expires_at, timezone.now() + timedelta(days=29))
        self.assertIsNotNone(subscription.next_charge_at)
        self.assertIsNone(subscription.grace_expires_at)
        self.assertTrue(subscription.auto_renew)
        self.assertEqual(profile.card_token, "tok-recovery")
        self.assertEqual(profile.masked_pan, "444455******7777")
        self.assertEqual(profile.status, BillingProfile.Status.ACTIVE)
        self.assertTrue(profile.auto_renew_enabled)
        self.assertEqual(profile.last_action_url, "")
        self.assertEqual(admin_state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)

    @patch("subscriptions.billing.fetch_invoice_status")
    def test_monobank_recovery_success_refetches_missing_card_token_from_invoice_status(self, mock_fetch_invoice_status):
        expires_at = timezone.now() - timedelta(days=5)
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=expires_at - timedelta(days=30),
            expires_at=expires_at,
            grace_expires_at=timezone.now() + timedelta(hours=4),
            trial_days=30,
            auto_renew=False,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="tok-old",
            masked_pan="444455******1111",
            status=BillingProfile.Status.FAILED,
            auto_renew_enabled=False,
        )
        payment = self._recovery_payment(invoice_id="recovery-refetch-1", subscription=subscription)
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "recovery-refetch-1",
            "status": "success",
            "modifiedDate": "2026-05-10T22:06:28Z",
            "walletData": {"cardToken": "tok-refetched"},
            "paymentInfo": {"maskedPan": "51693600******77"},
        }

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "recovery-refetch-1",
                "status": "success",
                "modifiedDate": 1_715_280_176_000,
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        mock_fetch_invoice_status.assert_called_once_with("recovery-refetch-1")
        payment.refresh_from_db()
        subscription.refresh_from_db()
        profile = BillingProfile.objects.get(user=self.telegram_user)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(profile.card_token, "tok-refetched")
        self.assertEqual(profile.masked_pan, "51693600******77")
        self.assertEqual(payment.raw_payload["invoice_status_payload"]["walletData"]["cardToken"], REDACTED)
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertIsNone(subscription.grace_expires_at)
        self.assertTrue(profile.auto_renew_enabled)

    @patch("subscriptions.billing.fetch_wallet_cards")
    @patch("subscriptions.billing.fetch_invoice_status")
    def test_monobank_bind_success_recovers_missing_card_token_from_wallet(self, mock_fetch_invoice_status, mock_fetch_wallet_cards):
        payment = self._bind_payment(invoice_id="bind-wallet-fallback", trial_days=30)
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "bind-wallet-fallback",
            "status": "success",
            "walletData": {"walletId": f"mono-user-{self.telegram_user.tg_user_id}", "status": "new"},
            "paymentInfo": {"maskedPan": "51693600******77"},
        }
        mock_fetch_wallet_cards.return_value = {
            "wallet": [
                {"cardToken": "tok-wallet-fallback", "maskedPan": "51693600******77", "country": "804"},
                {"cardToken": "tok-other-card", "maskedPan": "444455******11", "country": "804"},
            ]
        }

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-wallet-fallback",
                "status": "success",
                "modifiedDate": 1_715_280_177_000,
                "walletData": {"walletId": f"mono-user-{self.telegram_user.tg_user_id}", "status": "new"},
                "paymentInfo": {"maskedPan": "51693600******77"},
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        mock_fetch_invoice_status.assert_called_once_with("bind-wallet-fallback")
        mock_fetch_wallet_cards.assert_called_once_with(f"mono-user-{self.telegram_user.tg_user_id}")
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        payment.refresh_from_db()
        self.assertEqual(profile.card_token, "tok-wallet-fallback")
        self.assertEqual(profile.masked_pan, "51693600******77")
        self.assertEqual(profile.status, BillingProfile.Status.ACTIVE)
        self.assertTrue(profile.auto_renew_enabled)
        self.assertEqual(payment.raw_payload["wallet_cards_payload"]["wallet"][0]["cardToken"], REDACTED)

    @patch("subscriptions.billing.fetch_invoice_status")
    def test_monobank_recovery_paid_without_card_token_preserves_access_but_disables_auto_renew(
        self,
        mock_fetch_invoice_status,
    ):
        expires_at = timezone.now() - timedelta(days=5)
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=expires_at - timedelta(days=30),
            expires_at=expires_at,
            grace_expires_at=timezone.now() + timedelta(hours=4),
            trial_days=30,
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="tok-old",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        payment = self._recovery_payment(invoice_id="recovery-missing-token-1", subscription=subscription)
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "recovery-missing-token-1",
            "status": "success",
            "modifiedDate": "2026-05-13T22:13:33Z",
        }

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "recovery-missing-token-1",
                "status": "success",
                "modifiedDate": 1_715_280_177_000,
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        mock_fetch_invoice_status.assert_called_once_with("recovery-missing-token-1")
        payment.refresh_from_db()
        subscription.refresh_from_db()
        profile = BillingProfile.objects.get(user=self.telegram_user)
        admin_state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertGreater(subscription.expires_at, timezone.now() + timedelta(days=29))
        self.assertIsNone(subscription.grace_expires_at)
        self.assertFalse(subscription.auto_renew)
        self.assertIsNone(subscription.next_charge_at)
        self.assertEqual(profile.status, BillingProfile.Status.MISSING_TOKEN)
        self.assertEqual(profile.card_token, "")
        self.assertFalse(profile.auto_renew_enabled)
        self.assertEqual(profile.last_failure_reason, "Monobank did not return cardToken.")
        self.assertEqual(payment.raw_payload["invoice_status_payload"]["status"], "success")
        self.assertEqual(admin_state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)

    def test_monobank_recovery_non_paid_updates_keep_previous_card_token(self):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=35),
            expires_at=now - timedelta(days=5),
            grace_expires_at=now + timedelta(hours=4),
            trial_days=30,
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="tok-old",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        failed_payment = self._recovery_payment(invoice_id="recovery-failed-1", subscription=subscription)

        processed_failed, failed_updated = process_monobank_event(
            {
                "invoiceId": "recovery-failed-1",
                "status": "failure",
                "modifiedDate": 1_715_280_178_000,
                "failureReason": "declined",
            }
        )

        self.assertTrue(failed_updated)
        self.assertEqual(processed_failed.pk, failed_payment.pk)
        profile = BillingProfile.objects.get(user=self.telegram_user)
        self.assertEqual(profile.card_token, "tok-old")
        self.assertEqual(profile.status, BillingProfile.Status.FAILED)

        pending_payment = self._recovery_payment(invoice_id="recovery-pending-1", subscription=subscription)
        processed_pending, pending_updated = process_monobank_event(
            {
                "invoiceId": "recovery-pending-1",
                "status": "processing",
                "modifiedDate": 1_715_280_179_000,
                "tdsUrl": "https://mono.test/3ds",
            }
        )

        self.assertTrue(pending_updated)
        self.assertEqual(processed_pending.pk, pending_payment.pk)
        profile.refresh_from_db()
        self.assertEqual(profile.card_token, "tok-old")
        self.assertEqual(profile.status, BillingProfile.Status.ACTION_REQUIRED)
        self.assertEqual(profile.last_action_url, "https://mono.test/3ds")

    @override_settings(TELEGRAM_BOT_TOKEN="test-token")
    @patch("subscriptions.billing._create_or_refresh_billing_expense_draft", return_value=77)
    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    def test_paid_renewal_sends_billing_confirm_draft_prompt(self, send_mock, draft_mock):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            plan="solo",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=28),
            expires_at=now + timedelta(days=2),
            next_charge_at=now + timedelta(days=2),
            trial_days=0,
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="tok-renew",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="renew-prompt-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RENEWAL,
            raw_payload={
                "charge_context": {
                    "kind": Payment.Kind.RENEWAL,
                    "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                }
            },
        )

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "renew-prompt-1",
                "status": "success",
                "modifiedDate": 1_715_280_210_000,
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        draft_mock.assert_called_once_with(payment)
        send_mock.assert_called_once()
        kwargs = send_mock.call_args.kwargs
        self.assertEqual(kwargs["chat_id"], self.telegram_user.tg_user_id)
        self.assertEqual(kwargs["parse_mode"], "HTML")
        self.assertIn("Оплата підписки vydno.capital - 499 грн", kwargs["text"])
        self.assertIn("<b>Перевірте чернетку</b>", kwargs["text"])
        self.assertEqual(kwargs["buttons"][0][0]["callback_data"], "btx:77:tx:ok")
        self.assertEqual(kwargs["buttons"][1][1]["callback_data"], "btx:77:edit:account")
        payment.refresh_from_db()
        self.assertIn("charge_paid_sent_at", payment.raw_payload["bot_notifications"])

    def test_monobank_webhook_is_idempotent_by_modified_date(self):
        self._bind_payment(invoice_id="bind-dup", trial_days=30)
        process_monobank_event(
            {
                "invoiceId": "bind-dup",
                "status": "success",
                "modifiedDate": 1_715_280_200_000,
                "walletData": {"cardToken": "tok-dup", "maskedPan": "444400******1111"},
            }
        )

        payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-dup",
                "status": "failure",
                "modifiedDate": 1_715_280_100_000,
                "walletData": {"cardToken": "tok-other"},
            }
        )

        self.assertFalse(updated)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(profile.card_token, "tok-dup")

    def test_monobank_webhook_allows_status_change_with_same_modified_date(self):
        payment = self._bind_payment(invoice_id="bind-same-modified", trial_days=90)
        payment.provider_modified_at = datetime.fromtimestamp(1_715_280_200, tz=UTC)
        payment.save(update_fields=["provider_modified_at", "updated_at"])

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-same-modified",
                "status": "success",
                "modifiedDate": 1_715_280_200_000,
                "walletData": {"cardToken": "tok-same-modified", "maskedPan": "444411******1118"},
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertIsNotNone(payment.paid_at)
        subscription = Subscription.objects.get(user_id=self.telegram_user.tg_user_id)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(subscription.trial_days, 90)
        self.assertEqual(profile.card_token, "tok-same-modified")

    @override_settings(TELEGRAM_BOT_TOKEN="test-token")
    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    def test_monobank_bind_failure_notification_is_sent_once(self, send_mock):
        payment = self._bind_payment(invoice_id="bind-fail-once", trial_days=30)

        process_monobank_event(
            {
                "invoiceId": "bind-fail-once",
                "status": "failure",
                "modifiedDate": 1_715_280_200_000,
                "failureReason": "declined",
            }
        )
        process_monobank_event(
            {
                "invoiceId": "bind-fail-once",
                "status": "failure",
                "modifiedDate": 1_715_280_300_000,
                "failureReason": "declined",
            }
        )

        send_mock.assert_called_once()
        payment.refresh_from_db()
        self.assertTrue(payment.raw_payload["bot_notifications"]["bind_failed_sent_at"])

    @patch("subscriptions.billing.fetch_invoice_status")
    def test_sync_pending_bind_status_reconciles_successful_invoice_status(self, mock_fetch_invoice_status):
        payment = self._bind_payment(invoice_id="bind-sync", trial_days=30)
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "bind-sync",
            "status": "success",
            "modifiedDate": "2026-05-10T22:06:28Z",
            "walletData": {"cardToken": "tok-sync"},
            "paymentInfo": {"maskedPan": "51693600******03"},
        }

        result = sync_pending_bind_status(user_id=self.telegram_user.tg_user_id)

        self.assertTrue(result["updated"])
        self.assertEqual(result["monobank_status"], "success")
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)
        subscription = Subscription.objects.get(user_id=self.telegram_user.tg_user_id)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(subscription.trial_days, 30)
        self.assertEqual(profile.masked_pan, "51693600******03")
        self.assertEqual(profile.card_token, "tok-sync")
        self.assertEqual(payment.raw_payload["webhook_payload"]["walletData"]["cardToken"], REDACTED)

    @patch("subscriptions.billing.fetch_invoice_status")
    def test_sync_pending_bind_status_checks_older_pending_invoice_if_newer_one_is_still_created(self, mock_fetch_invoice_status):
        older_success = self._bind_payment(invoice_id="bind-success-older", trial_days=30)
        newer_created = self._bind_payment(invoice_id="bind-created-newer", trial_days=30)

        def fetch_side_effect(invoice_id: str) -> dict:
            if invoice_id == "bind-created-newer":
                return {
                    "invoiceId": invoice_id,
                    "status": "created",
                }
            return {
                "invoiceId": invoice_id,
                "status": "success",
                "modifiedDate": "2026-05-10T22:06:28Z",
                "walletData": {"cardToken": "tok-older"},
                "paymentInfo": {"maskedPan": "51693600******77"},
            }

        mock_fetch_invoice_status.side_effect = fetch_side_effect

        result = sync_pending_bind_status(user_id=self.telegram_user.tg_user_id)

        self.assertTrue(result["updated"])
        self.assertEqual(result["invoice_id"], "bind-success-older")
        older_success.refresh_from_db()
        newer_created.refresh_from_db()
        self.assertEqual(older_success.status, Payment.Status.PAID)
        self.assertEqual(newer_created.status, Payment.Status.PENDING)
        profile = BillingProfile.objects.get(user_id=self.telegram_user.tg_user_id)
        self.assertEqual(profile.card_token, "tok-older")

    @override_settings(TELEGRAM_BOT_TOKEN="test-token")
    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    @patch("subscriptions.billing.fetch_invoice_status")
    def test_sync_pending_charge_status_reconciles_failed_invoice_status_and_notifies(
        self,
        mock_fetch_invoice_status,
        send_mock,
    ):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=60),
            expires_at=now,
            next_charge_at=now,
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
            last_charge_status="pending",
        )
        payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="retry-sync-fail-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RETRY,
            raw_payload={
                "bot_notifications": {"charge_processing_sent_at": timezone.now().isoformat()},
            },
        )
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "retry-sync-fail-1",
            "status": "failure",
            "modifiedDate": "2026-05-13T22:13:33Z",
            "failureReason": "На картці недостатньо коштів для завершення покупки",
        }

        result = sync_pending_charge_status(user_id=self.telegram_user.tg_user_id)

        self.assertTrue(result["updated"])
        self.assertEqual(result["monobank_status"], "failure")
        payment.refresh_from_db()
        subscription.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.FAILED)
        self.assertTrue(payment.raw_payload["bot_notifications"]["charge_failed_sent_at"])
        self.assertEqual(subscription.status, Subscription.Status.EXPIRED)
        self.assertIsNone(subscription.next_charge_at)
        send_mock.assert_called_once()
        self.assertIn("Автосписання не пройшло", send_mock.call_args.kwargs["text"])

    @patch("subscriptions.billing.sync_pending_charge_status")
    def test_reconcile_pending_monobank_charges_checks_unique_users(self, mock_sync_pending_charge_status):
        second_user = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Second",
            username="second",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="pending-a-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RETRY,
        )
        Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="pending-a-2",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RENEWAL,
        )
        Payment.objects.create(
            user=second_user,
            provider="monobank",
            provider_payment_id="pending-b-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RETRY,
        )
        mock_sync_pending_charge_status.side_effect = [
            {"payment_id": 1, "invoice_id": "pending-a-1", "monobank_status": "failure", "updated": True},
            {"payment_id": 2, "invoice_id": "pending-b-1", "monobank_status": "processing", "updated": False},
        ]

        result = reconcile_pending_monobank_charges(limit=10)

        self.assertEqual(result, {"checked": 2, "updated": 1, "still_pending": 1, "failed": 0})
        self.assertEqual(
            [call.kwargs["user_id"] for call in mock_sync_pending_charge_status.call_args_list],
            [self.telegram_user.tg_user_id, second_user.tg_user_id],
        )

    @patch("subscriptions.billing.sync_pending_bind_status")
    def test_reconcile_pending_monobank_binds_checks_unique_users(self, mock_sync_pending_bind_status):
        second_user = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Second",
            username="second",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="bind-a-1",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.BIND,
        )
        Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="bind-a-2",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.BIND,
        )
        Payment.objects.create(
            user=second_user,
            provider="monobank",
            provider_payment_id="bind-b-1",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.BIND,
        )
        Payment.objects.create(
            user=second_user,
            provider="monobank",
            provider_payment_id="renewal-b-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RENEWAL,
        )
        mock_sync_pending_bind_status.side_effect = [
            {"payment_id": 1, "invoice_id": "bind-a-1", "monobank_status": "success", "updated": True},
            {"payment_id": 2, "invoice_id": "bind-b-1", "monobank_status": "processing", "updated": False},
        ]

        result = reconcile_pending_monobank_binds(limit=10)

        self.assertEqual(result, {"checked": 2, "updated": 1, "still_pending": 1, "failed": 0})
        self.assertEqual(
            [call.kwargs["user_id"] for call in mock_sync_pending_bind_status.call_args_list],
            [self.telegram_user.tg_user_id, second_user.tg_user_id],
        )

    @patch("subscriptions.billing.charge_wallet_payment")
    def test_due_renewal_success_extends_subscription(self, mock_charge_wallet_payment):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=30),
            expires_at=now,
            next_charge_at=now - timedelta(minutes=5),
            auto_renew=True,
            trial_days=30,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        mock_charge_wallet_payment.return_value = {
            "invoiceId": "renew-1",
            "status": "success",
            "modifiedDate": 1_715_280_300_000,
        }

        result = run_due_monobank_charges()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        subscription.refresh_from_db()
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertGreater(subscription.expires_at, now + timedelta(days=29))
        renewal_payment = Payment.objects.get(provider_payment_id="renew-1")
        self.assertEqual(renewal_payment.kind, Payment.Kind.RENEWAL)
        self.assertEqual(renewal_payment.status, Payment.Status.PAID)

    @patch("subscriptions.billing.charge_wallet_payment")
    def test_retry_monobank_charge_blocks_when_pending_charge_exists(self, mock_charge_wallet_payment):
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        Payment.objects.create(
            user=self.telegram_user,
            provider="monobank",
            provider_payment_id="retry-pending-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RETRY,
        )

        with self.assertRaisesMessage(ValueError, "pending"):
            retry_monobank_charge(
                user_id=self.telegram_user.tg_user_id,
                intent_key="test_pending_retry",
            )

        mock_charge_wallet_payment.assert_not_called()

    @patch("subscriptions.billing.charge_wallet_payment")
    def test_retry_monobank_charge_allows_manual_retry_when_auto_renew_is_disabled(self, mock_charge_wallet_payment):
        now = timezone.now()
        Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=60),
            expires_at=now - timedelta(days=1),
            next_charge_at=None,
            grace_expires_at=now + timedelta(hours=8),
            auto_renew=False,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.FAILED,
            auto_renew_enabled=False,
        )
        mock_charge_wallet_payment.return_value = {
            "invoiceId": "retry-manual-disabled-1",
            "status": "success",
            "modifiedDate": 1_715_280_451_000,
        }

        result = retry_monobank_charge(
            user_id=self.telegram_user.tg_user_id,
            intent_key="test_completed_retry",
        )

        self.assertEqual(result["status"], Payment.Status.PAID)
        payment = Payment.objects.get(provider_payment_id="retry-manual-disabled-1")
        profile = BillingProfile.objects.get(user=self.telegram_user)
        subscription = Subscription.objects.get(user=self.telegram_user)
        self.assertEqual(payment.kind, Payment.Kind.RETRY)
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertTrue(profile.auto_renew_enabled)
        mock_charge_wallet_payment.assert_called_once()

    @override_settings(TELEGRAM_BOT_TOKEN="test-token")
    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    @patch("subscriptions.billing.charge_wallet_payment")
    def test_retry_monobank_charge_processing_response_updates_state_and_notifies(
        self,
        mock_charge_wallet_payment,
        send_mock,
    ):
        now = timezone.now()
        Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=30),
            expires_at=now - timedelta(seconds=1),
            next_charge_at=now - timedelta(seconds=1),
            auto_renew=True,
            trial_days=30,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
            last_charge_status="paid",
        )
        mock_charge_wallet_payment.return_value = {
            "invoiceId": "retry-processing-1",
            "status": "processing",
            "modifiedDate": 1_715_280_450_000,
        }

        result = retry_monobank_charge(
            user_id=self.telegram_user.tg_user_id,
            intent_key="test_processing_retry",
        )

        self.assertEqual(result["status"], Payment.Status.PENDING)
        payment = Payment.objects.get(provider_payment_id="retry-processing-1")
        profile = BillingProfile.objects.get(user=self.telegram_user)
        sent_text = send_mock.call_args.kwargs["text"]
        self.assertEqual(payment.status, Payment.Status.PENDING)
        self.assertTrue(payment.raw_payload["bot_notifications"]["charge_processing_sent_at"])
        self.assertEqual(profile.last_charge_status, Payment.Status.PENDING)
        self.assertEqual(profile.status, BillingProfile.Status.ACTIVE)
        send_mock.assert_called_once()
        self.assertIn("Оплата обробляється", sent_text)
        self.assertNotIn("Р", sent_text)

    @override_settings(TELEGRAM_BOT_TOKEN="test-token")
    @patch("subscriptions.billing.send_telegram_message", return_value={"ok": True})
    @patch("subscriptions.billing.charge_wallet_payment")
    def test_retry_monobank_charge_failed_response_moves_to_grace_and_notifies(
        self,
        mock_charge_wallet_payment,
        send_mock,
    ):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=60),
            expires_at=now,
            next_charge_at=now,
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        mock_charge_wallet_payment.return_value = {
            "invoiceId": "retry-fail-1",
            "status": "failure",
            "modifiedDate": 1_715_280_460_000,
            "failureReason": "На картці недостатньо коштів для завершення покупки",
        }

        result = retry_monobank_charge(
            user_id=self.telegram_user.tg_user_id,
            intent_key="test_failed_retry",
        )

        self.assertEqual(result["status"], Payment.Status.FAILED)
        payment = Payment.objects.get(provider_payment_id="retry-fail-1")
        profile = BillingProfile.objects.get(user=self.telegram_user)
        subscription.refresh_from_db()
        sent_text = send_mock.call_args.kwargs["text"]
        self.assertEqual(payment.status, Payment.Status.FAILED)
        self.assertTrue(payment.raw_payload["bot_notifications"]["charge_failed_sent_at"])
        self.assertEqual(profile.last_charge_status, Payment.Status.FAILED)
        self.assertEqual(subscription.status, Subscription.Status.EXPIRED)
        self.assertIsNotNone(subscription.grace_expires_at)
        self.assertLess(subscription.grace_expires_at, now + timedelta(days=2))
        send_mock.assert_called_once()
        self.assertIn("Автосписання не пройшло", sent_text)
        self.assertNotIn("Р", sent_text)

    def test_renewal_failure_moves_subscription_to_grace(self):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=60),
            expires_at=now,
            next_charge_at=now,
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
        )
        Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="renew-fail",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RENEWAL,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )

        process_monobank_event(
            {
                "invoiceId": "renew-fail",
                "status": "failure",
                "modifiedDate": 1_715_280_400_000,
                "failureReason": "declined",
            }
        )

        subscription.refresh_from_db()
        self.assertEqual(subscription.status, Subscription.Status.EXPIRED)
        self.assertIsNotNone(subscription.grace_expires_at)
        self.assertIsNone(subscription.next_charge_at)
        self.assertLess(subscription.grace_expires_at, now + timedelta(days=2))

    def test_renewal_technical_failure_keeps_long_grace_window(self):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=60),
            expires_at=now,
            next_charge_at=now,
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
        )
        Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="renew-tech-fail",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PENDING,
            kind=Payment.Kind.RENEWAL,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )

        process_monobank_event(
            {
                "invoiceId": "renew-tech-fail",
                "status": "failure",
                "modifiedDate": 1_715_280_410_000,
                "failureReason": "processor_error",
            }
        )

        subscription.refresh_from_db()
        self.assertEqual(subscription.status, Subscription.Status.EXPIRED)
        self.assertIsNotNone(subscription.grace_expires_at)
        self.assertGreater(subscription.grace_expires_at, now + timedelta(days=20))

    @patch("subscriptions.billing.delete_wallet_card", return_value={})
    def test_cancel_auto_renew_disables_future_charges(self, mock_delete_wallet_card):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=15),
            expires_at=now + timedelta(days=15),
            next_charge_at=now + timedelta(days=15),
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="renew-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )

        profile = cancel_auto_renew(user_id=self.telegram_user.tg_user_id)

        subscription.refresh_from_db()
        admin_state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        mock_delete_wallet_card.assert_called_once_with("renew-token")
        self.assertFalse(profile.auto_renew_enabled)
        self.assertEqual(profile.card_token, "")
        self.assertEqual(subscription.next_charge_at, None)
        self.assertFalse(subscription.auto_renew)
        self.assertEqual(admin_state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)

    @patch("subscriptions.billing.delete_wallet_card", return_value={})
    @patch("subscriptions.billing.cancel_invoice", return_value={"status": "processing"})
    def test_refund_latest_monobank_payment_requests_refund_and_disables_token(self, mock_cancel_invoice, mock_delete_wallet_card):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=55),
            expires_at=now + timedelta(days=35),
            next_charge_at=now + timedelta(days=35),
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
            payment_id="renew-live-1",
        )
        profile = BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="refund-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="renew-live-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.RENEWAL,
            paid_at=now,
            raw_payload={
                "charge_context": {
                    "kind": Payment.Kind.RENEWAL,
                    "wallet_id": profile.wallet_id,
                    "subscription_snapshot_before_charge": {
                        "status": Subscription.Status.ACTIVE,
                        "provider": "monobank",
                        "source": Subscription.Source.PAYMENT,
                        "amount": "499.00",
                        "currency": "UAH",
                        "started_at": (now - timedelta(days=55)).isoformat(),
                        "expires_at": (now + timedelta(days=5)).isoformat(),
                        "next_charge_at": (now + timedelta(days=5)).isoformat(),
                        "grace_expires_at": "",
                        "trial_days": 0,
                        "auto_renew": True,
                        "payment_id": "previous-paid-1",
                        "comment": "before paid renewal",
                    },
                },
            },
        )

        result = refund_latest_monobank_payment(
            user_id=self.telegram_user.tg_user_id,
            reason="support refund",
            admin_user=self.admin_user,
        )

        payment.refresh_from_db()
        subscription.refresh_from_db()
        profile.refresh_from_db()
        admin_state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        mock_cancel_invoice.assert_called_once()
        self.assertEqual(mock_cancel_invoice.call_args.args[0]["invoiceId"], "renew-live-1")
        self.assertEqual(mock_cancel_invoice.call_args.args[0]["amount"], 49900)
        mock_delete_wallet_card.assert_called_once_with("refund-token")
        self.assertEqual(result["refund_status"], "processing")
        self.assertEqual(payment.raw_payload["refund_request"]["reason"], "support refund")
        self.assertEqual(payment.raw_payload["refund_request"]["requested_by"], self.admin_user.username)
        self.assertFalse(profile.auto_renew_enabled)
        self.assertEqual(profile.card_token, "")
        self.assertFalse(subscription.auto_renew)
        self.assertIsNone(subscription.next_charge_at)
        self.assertEqual(admin_state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)

    def test_refunded_webhook_restores_subscription_snapshot_and_disables_autorenew(self):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now - timedelta(days=55),
            expires_at=now + timedelta(days=35),
            next_charge_at=now + timedelta(days=35),
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
            payment_id="renew-refund-1",
        )
        profile = BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="refund-webhook-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="renew-refund-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.RENEWAL,
            paid_at=now,
            raw_payload={
                "charge_context": {
                    "kind": Payment.Kind.RENEWAL,
                    "wallet_id": profile.wallet_id,
                    "subscription_snapshot_before_charge": {
                        "status": Subscription.Status.ACTIVE,
                        "provider": "monobank",
                        "source": Subscription.Source.PAYMENT,
                        "amount": "499.00",
                        "currency": "UAH",
                        "started_at": (now - timedelta(days=55)).isoformat(),
                        "expires_at": (now + timedelta(days=5)).isoformat(),
                        "next_charge_at": (now + timedelta(days=5)).isoformat(),
                        "grace_expires_at": "",
                        "trial_days": 0,
                        "auto_renew": True,
                        "payment_id": "previous-paid-1",
                        "comment": "before paid renewal",
                    },
                },
                "refund_request": {"status": "processing"},
            },
        )

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "renew-refund-1",
                "status": "reversed",
                "modifiedDate": 1_715_280_500_000,
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, payment.pk)
        payment.refresh_from_db()
        subscription.refresh_from_db()
        profile.refresh_from_db()
        admin_state = UserAdminState.objects.get(telegram_user_id=self.telegram_user.tg_user_id)
        self.assertEqual(payment.status, Payment.Status.REFUNDED)
        self.assertEqual(payment.raw_payload["refund_request"]["status"], "success")
        self.assertEqual(subscription.status, Subscription.Status.ACTIVE)
        self.assertGreater(subscription.expires_at, now)
        self.assertEqual(subscription.payment_id, "previous-paid-1")
        self.assertFalse(subscription.auto_renew)
        self.assertIsNone(subscription.next_charge_at)
        self.assertFalse(profile.auto_renew_enabled)
        self.assertEqual(profile.card_token, "")
        self.assertEqual(profile.status, BillingProfile.Status.CANCELLED)
        self.assertEqual(admin_state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)

    @patch("subscriptions.billing.delete_wallet_card")
    @patch("subscriptions.billing.cancel_invoice", return_value={"status": "processing"})
    @patch("subscriptions.billing._send_user_message", return_value=True)
    def test_duplicate_bind_refund_preserves_subscription_card_and_autorenew(
        self,
        mock_send_user_message,
        mock_cancel_invoice,
        mock_delete_wallet_card,
    ):
        now = timezone.now()
        subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now,
            expires_at=now + timedelta(days=30),
            next_charge_at=now + timedelta(days=30),
            trial_days=30,
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
            payment_id="bind-duplicate-2",
        )
        profile = BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="keep-this-token",
            masked_pan="444455******1111",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        first = self._bind_payment(invoice_id="bind-original-1", trial_days=30)
        duplicate = self._bind_payment(
            invoice_id="bind-duplicate-2",
            trial_days=90,
            mode="rebind",
            trial_granted=False,
        )
        Payment.objects.filter(pk=first.pk).update(
            subscription=subscription,
            status=Payment.Status.PAID,
            paid_at=now - timedelta(minutes=2),
        )
        Payment.objects.filter(pk=duplicate.pk).update(
            subscription=subscription,
            status=Payment.Status.PAID,
            paid_at=now,
        )

        self.assertEqual(get_latest_refundable_monobank_payment(user_id=self.telegram_user.tg_user_id).pk, duplicate.pk)
        result = refund_latest_monobank_payment(
            user_id=self.telegram_user.tg_user_id,
            reason="duplicate 1 UAH bind",
            admin_user=self.admin_user,
        )

        self.assertTrue(result["preserved_billing_state"])
        self.assertEqual(result["refund_kind"], "duplicate_bind")
        self.assertFalse(result["token_delete_attempted"])
        mock_delete_wallet_card.assert_not_called()
        self.assertEqual(mock_cancel_invoice.call_args.args[0]["invoiceId"], "bind-duplicate-2")
        self.assertEqual(mock_cancel_invoice.call_args.args[0]["amount"], 100)

        processed_payment, updated = process_monobank_event(
            {
                "invoiceId": "bind-duplicate-2",
                "status": "reversed",
                "modifiedDate": 1_715_280_600_000,
            }
        )

        self.assertTrue(updated)
        self.assertEqual(processed_payment.pk, duplicate.pk)
        duplicate.refresh_from_db()
        subscription.refresh_from_db()
        profile.refresh_from_db()
        self.assertEqual(duplicate.status, Payment.Status.REFUNDED)
        self.assertEqual(duplicate.raw_payload["refund_request"]["status"], "success")
        self.assertEqual(subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(subscription.payment_id, "bind-original-1")
        self.assertTrue(subscription.auto_renew)
        self.assertIsNotNone(subscription.next_charge_at)
        self.assertEqual(profile.card_token, "keep-this-token")
        self.assertTrue(profile.auto_renew_enabled)
        self.assertEqual(profile.status, BillingProfile.Status.ACTIVE)
        mock_send_user_message.assert_called_once()


@override_settings(
    TELEGRAM_BOT_TOKEN="",
)
class BillingProfileAdminTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("billing-admin", "billing-admin@example.com", "pass12345")
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Billing",
            username="billingtester",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now() - timedelta(days=3),
            expires_at=timezone.now() + timedelta(days=27),
            next_charge_at=timezone.now() + timedelta(days=27),
            auto_renew=True,
            trial_days=30,
            source=Subscription.Source.PAYMENT,
        )
        self.profile = BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="retry-token",
            masked_pan="444455******1212",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
            last_charge_status="paid",
        )

    def test_change_page_shows_force_charge_button(self):
        self.client.force_login(self.admin_user)

        response = self.client.get(reverse("admin:subscriptions_billingprofile_change", args=[self.profile.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("admin:subscriptions_billingprofile_force_charge", args=[self.profile.pk]))

    @patch("subscriptions.admin.create_audit_log")
    @patch("subscriptions.admin.retry_monobank_charge")
    def test_force_charge_view_uses_retry_charge_path(self, mock_retry_monobank_charge, mock_create_audit_log):
        payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=self.subscription,
            provider="monobank",
            provider_payment_id="retry-admin-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.RETRY,
        )
        mock_retry_monobank_charge.return_value = {
            "payment_id": payment.pk,
            "invoice_id": "retry-admin-1",
            "status": Payment.Status.PAID,
            "action_url": "",
        }
        self.client.force_login(self.admin_user)

        response = self.client.post(
            reverse("admin:subscriptions_billingprofile_force_charge", args=[self.profile.pk]),
            {"confirm": "on", "reason": "billing smoke", "intent_key": "admin-billing-smoke-intent"},
        )

        self.assertRedirects(
            response,
            reverse("admin:subscriptions_billingprofile_change", args=[self.profile.pk]),
            fetch_redirect_response=False,
        )
        mock_retry_monobank_charge.assert_called_once_with(
            user_id=self.telegram_user.tg_user_id,
            intent_key="admin-billing-smoke-intent",
        )
        mock_create_audit_log.assert_called_once()
        self.assertEqual(mock_create_audit_log.call_args.kwargs["mode"], "force_charge_now")
        self.assertEqual(mock_create_audit_log.call_args.kwargs["reason"], "billing smoke")


@override_settings(
    TELEGRAM_BOT_TOKEN="",
)
class TelegramUserRefundAdminTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("crm-admin", "crm-admin@example.com", "pass12345")
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=3003,
            first_name="CRM",
            username="crmtester",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.subscription = Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.ACTIVE,
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now() - timedelta(days=10),
            expires_at=timezone.now() + timedelta(days=20),
            next_charge_at=timezone.now() + timedelta(days=20),
            auto_renew=True,
            source=Subscription.Source.PAYMENT,
            payment_id="crm-renew-1",
        )
        BillingProfile.objects.create(
            user=self.telegram_user,
            provider="monobank",
            wallet_id=f"mono-user-{self.telegram_user.tg_user_id}",
            card_token="crm-token",
            masked_pan="444455******1212",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
            last_charge_status="paid",
        )
        self.payment = Payment.objects.create(
            user=self.telegram_user,
            subscription=self.subscription,
            provider="monobank",
            provider_payment_id="crm-renew-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.RENEWAL,
            paid_at=timezone.now(),
            raw_payload={"charge_context": {"kind": Payment.Kind.RENEWAL, "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}"}},
        )

    def test_user_change_page_shows_refund_button_for_latest_paid_charge(self):
        self.client.force_login(self.admin_user)

        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.telegram_user.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("admin:users_telegramuser_refund_latest_payment", args=[self.telegram_user.pk]))
        self.assertEqual(get_latest_refundable_monobank_payment(user_id=self.telegram_user.tg_user_id).pk, self.payment.pk)

    def test_user_change_page_offers_safe_duplicate_bind_refund(self):
        self.payment.status = Payment.Status.REFUNDED
        self.payment.save(update_fields=["status", "updated_at"])
        now = timezone.now()
        bind_context = {
            "trial_days": 30,
            "mode": "bind",
            "trial_granted": True,
            "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
            "promo_code": "",
            "promo_offer_id": None,
        }
        Payment.objects.create(
            user=self.telegram_user,
            subscription=self.subscription,
            provider="monobank",
            provider_payment_id="crm-bind-original",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.BIND,
            paid_at=now - timedelta(minutes=2),
            raw_payload={"bind_context": bind_context},
        )
        duplicate = Payment.objects.create(
            user=self.telegram_user,
            subscription=self.subscription,
            provider="monobank",
            provider_payment_id="crm-bind-duplicate",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.BIND,
            paid_at=now,
            raw_payload={"bind_context": bind_context},
        )
        self.client.force_login(self.admin_user)

        user_response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.telegram_user.pk]))
        refund_response = self.client.get(
            reverse("admin:users_telegramuser_refund_latest_payment", args=[self.telegram_user.pk])
        )

        self.assertEqual(get_latest_refundable_monobank_payment(user_id=self.telegram_user.tg_user_id).pk, duplicate.pk)
        self.assertContains(user_response, "Повернути зайву 1 грн")
        self.assertContains(refund_response, "Пробний доступ, збережена картка й автопродовження залишаться без змін")

    @patch("users.admin.create_audit_log")
    @patch("users.admin.refund_latest_monobank_payment")
    def test_refund_latest_payment_view_uses_billing_service(self, mock_refund_latest_monobank_payment, mock_create_audit_log):
        mock_refund_latest_monobank_payment.return_value = {
            "payment_id": self.payment.pk,
            "invoice_id": "crm-renew-1",
            "refund_status": "processing",
            "token_delete_attempted": True,
            "token_delete_ok": True,
            "token_delete_error": "",
        }
        self.client.force_login(self.admin_user)

        response = self.client.post(
            reverse("admin:users_telegramuser_refund_latest_payment", args=[self.telegram_user.pk]),
            {"confirm": "on", "reason": "crm support refund"},
        )

        self.assertRedirects(
            response,
            reverse("admin:users_telegramuser_change", args=[self.telegram_user.pk]),
            fetch_redirect_response=False,
        )
        mock_refund_latest_monobank_payment.assert_called_once_with(
            user_id=self.telegram_user.tg_user_id,
            reason="crm support refund",
            admin_user=self.admin_user,
        )
        mock_create_audit_log.assert_called_once()
        self.assertEqual(mock_create_audit_log.call_args.kwargs["mode"], "refund_latest_payment")


@override_settings(
    TELEGRAM_BOT_TOKEN="test-bot",
)
class MonobankReturnViewTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=4004,
            first_name="Return",
            username="returntester",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

    def _bind_payment(
        self,
        *,
        invoice_id: str,
        status: str = Payment.Status.PENDING,
        trial_granted: bool = True,
        notifications: dict | None = None,
        subscription: Subscription | None = None,
    ) -> Payment:
        raw_payload = {
            "bind_context": {
                "trial_days": 30,
                "mode": "bind",
                "trial_granted": trial_granted,
                "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
            }
        }
        if notifications:
            raw_payload["bot_notifications"] = notifications
        return Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id=invoice_id,
            amount=Decimal("1.00"),
            currency="UAH",
            status=status,
            kind=Payment.Kind.BIND,
            paid_at=timezone.now() if status == Payment.Status.PAID else None,
            raw_payload=raw_payload,
        )

    def _recovery_payment(
        self,
        *,
        invoice_id: str,
        status: str = Payment.Status.PENDING,
        notifications: dict | None = None,
        subscription: Subscription | None = None,
    ) -> Payment:
        raw_payload = {
            "recovery_context": {
                "save_card": True,
                "replace_card": True,
                "wallet_id": f"mono-user-{self.telegram_user.tg_user_id}",
                "return_token": build_bind_return_token(
                    user_id=self.telegram_user.tg_user_id,
                    flow="recovery",
                ),
                "page_url": "https://mono.test/recovery",
            }
        }
        if notifications:
            raw_payload["bot_notifications"] = notifications
        return Payment.objects.create(
            user=self.telegram_user,
            subscription=subscription,
            provider="monobank",
            provider_payment_id=invoice_id,
            amount=Decimal("499.00"),
            currency="UAH",
            status=status,
            kind=Payment.Kind.RETRY,
            paid_at=timezone.now() if status == Payment.Status.PAID else None,
            raw_payload=raw_payload,
        )

    def _trial_subscription(self) -> Subscription:
        now = timezone.now()
        return Subscription.objects.create(
            user=self.telegram_user,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="monobank",
            source=Subscription.Source.PAYMENT,
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=now,
            expires_at=now + timedelta(days=30),
            next_charge_at=now + timedelta(days=30),
            auto_renew=True,
            trial_days=30,
        )

    def test_monobank_return_without_token_shows_fallback_without_push(self):
        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Поверніться в Telegram")
        self.assertContains(response, "Відкрити бота")
        send_mock.assert_not_called()

    def test_monobank_return_with_invalid_token_shows_fallback_without_push(self):
        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"), {"return_token": "broken-token"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Поверніться в Telegram")
        send_mock.assert_not_called()

    def test_monobank_return_pending_bind_sends_processing_message_once(self):
        payment = self._bind_payment(invoice_id="bind-processing-1")
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="bind")

        with (
            patch("subscriptions.views.sync_pending_bind_status", return_value={"updated": False, "monobank_status": "processing", "payment_id": payment.pk}),
            patch("subscriptions.billing.send_telegram_message", return_value={"ok": True}) as send_mock,
        ):
            first_response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
            second_response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        payment.refresh_from_db()
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertContains(first_response, "Оплата обробляється")
        self.assertTrue(payment.raw_payload["bot_notifications"]["processing_sent_at"])
        send_mock.assert_called_once()

    @patch("subscriptions.billing.fetch_invoice_status")
    def test_monobank_return_sync_success_renders_success_without_duplicate_activation(self, mock_fetch_invoice_status):
        payment = self._bind_payment(invoice_id="bind-sync-success")
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="bind")
        mock_fetch_invoice_status.return_value = {
            "invoiceId": "bind-sync-success",
            "status": "success",
            "modifiedDate": "2026-05-10T22:06:28Z",
            "walletData": {"cardToken": "tok-sync"},
            "paymentInfo": {"maskedPan": "51693600******03"},
        }

        with patch("subscriptions.billing.send_telegram_message", return_value={"ok": True}) as send_mock:
            first_response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
            second_response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        payment.refresh_from_db()
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertContains(first_response, "Доступ активовано")
        self.assertEqual(payment.status, Payment.Status.PAID)
        self.assertTrue(payment.raw_payload["bot_notifications"]["activation_sent_at"])
        send_mock.assert_called_once()

    def test_monobank_return_paid_bind_with_activation_flag_does_not_send_duplicate_success(self):
        subscription = self._trial_subscription()
        self._bind_payment(
            invoice_id="bind-paid-1",
            status=Payment.Status.PAID,
            subscription=subscription,
            notifications={"activation_sent_at": timezone.now().isoformat()},
        )
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="bind")

        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Доступ активовано")
        send_mock.assert_not_called()

    def test_monobank_return_failed_bind_does_not_send_processing_message(self):
        self._bind_payment(invoice_id="bind-failed-1", status=Payment.Status.FAILED)
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="bind")

        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Не вдалося підтвердити оплату")
        send_mock.assert_not_called()

    @patch("subscriptions.views.sync_pending_charge_status")
    def test_monobank_return_pending_recovery_syncs_charge_status_without_bind_push(self, mock_sync_pending_charge_status):
        payment = self._recovery_payment(invoice_id="recovery-processing-1")
        token = build_bind_return_token(user_id=self.telegram_user.tg_user_id, flow="recovery")
        mock_sync_pending_charge_status.return_value = {
            "updated": False,
            "monobank_status": "processing",
            "payment_id": payment.pk,
        }

        with patch("subscriptions.billing.send_telegram_message") as send_mock:
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Оплата обробляється")
        mock_sync_pending_charge_status.assert_called_once_with(user_id=self.telegram_user.tg_user_id)
        send_mock.assert_not_called()


@override_settings(
    TELEGRAM_BOT_TOKEN="",
    MONO_WEBHOOK_VERIFY_SIGNATURES=True,
)
class MonobankWebhookViewTests(SimpleTestCase):
    def test_monobank_webhook_rejects_missing_or_invalid_signature(self):
        body = b'{"invoiceId":"bind-30"}'

        with patch("subscriptions.views.verify_webhook_signature", return_value=False) as mock_verify:
            response = self.client.post(
                reverse("billing-mono-webhook"),
                data=body,
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"ok": False, "error": "Invalid Monobank signature."})
        mock_verify.assert_called_once_with(body=body, signature=None)

    def test_monobank_webhook_processes_signed_request(self):
        body = b'{"invoiceId":"bind-30"}'

        with (
            patch("subscriptions.views.verify_webhook_signature", return_value=True) as mock_verify,
            patch("subscriptions.views.process_monobank_event", return_value=(None, False)) as mock_process,
        ):
            response = self.client.post(
                reverse("billing-mono-webhook"),
                data=body,
                content_type="application/json",
                HTTP_X_SIGN="signed-payload",
            )

        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"ok": True, "updated": False, "payment_id": None})
        mock_verify.assert_called_once_with(body=body, signature="signed-payload")
        mock_process.assert_called_once_with({"invoiceId": "bind-30"})

    def test_monobank_webhook_returns_503_when_signature_verifier_is_unavailable(self):
        body = b'{"invoiceId":"bind-30"}'

        with (
            patch("subscriptions.views.verify_webhook_signature", side_effect=MonobankAPIError("pubkey lookup failed")) as mock_verify,
            patch("subscriptions.views.process_monobank_event") as mock_process,
        ):
            response = self.client.post(
                reverse("billing-mono-webhook"),
                data=body,
                content_type="application/json",
                HTTP_X_SIGN="signed-payload",
            )

        self.assertEqual(response.status_code, 503)
        self.assertJSONEqual(response.content, {"ok": False, "error": "pubkey lookup failed"})
        mock_verify.assert_called_once_with(body=body, signature="signed-payload")
        mock_process.assert_not_called()


@override_settings(
    TELEGRAM_BOT_TOKEN="",
    BILLING_INTERNAL_TOKEN="billing-secret",
)
class MonobankInternalAuthViewTests(SimpleTestCase):
    def test_internal_billing_endpoint_rejects_invalid_token(self):
        with patch("subscriptions.views.build_bind_invoice") as mock_build:
            response = self.client.post(
                reverse("billing-mono-init-bind"),
                data=b'{"telegram_user_id":1001,"trial_days":30,"mode":"bind"}',
                content_type="application/json",
                HTTP_X_INTERNAL_TOKEN="wrong-secret",
            )

        self.assertEqual(response.status_code, 401)
        self.assertJSONEqual(response.content, {"ok": False, "error": "Unauthorized."})
        mock_build.assert_not_called()

    @override_settings(BILLING_INTERNAL_TOKEN="")
    def test_internal_billing_endpoint_returns_503_when_token_not_configured(self):
        with patch("subscriptions.views.build_bind_invoice") as mock_build:
            response = self.client.post(
                reverse("billing-mono-init-bind"),
                data=b'{"telegram_user_id":1001,"trial_days":30,"mode":"bind"}',
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 503)
        self.assertJSONEqual(response.content, {"ok": False, "error": "BILLING_INTERNAL_TOKEN not configured."})
        mock_build.assert_not_called()

    def test_internal_billing_endpoint_accepts_valid_token(self):
        with patch(
            "subscriptions.views.build_bind_invoice",
            return_value={"invoice_id": "bind-1", "page_url": "https://mono.test/bind"},
        ) as mock_build:
            response = self.client.post(
                reverse("billing-mono-init-bind"),
                data=b'{"telegram_user_id":1001,"trial_days":90,"mode":"bind","promo_code":"PROMO90"}',
                content_type="application/json",
                HTTP_X_INTERNAL_TOKEN="billing-secret",
            )

        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(
            response.content,
            {"ok": True, "invoice_id": "bind-1", "page_url": "https://mono.test/bind"},
        )
        mock_build.assert_called_once_with(user_id=1001, trial_days=90, mode="bind", promo_code="PROMO90")

    def test_internal_recovery_endpoint_accepts_valid_token(self):
        with patch(
            "subscriptions.views.build_recovery_invoice",
            return_value={
                "payment_id": 7,
                "invoice_id": "recovery-1",
                "page_url": "https://mono.test/recovery",
            },
        ) as mock_build:
            response = self.client.post(
                reverse("billing-mono-init-recovery"),
                data=b'{"telegram_user_id":1001}',
                content_type="application/json",
                HTTP_X_INTERNAL_TOKEN="billing-secret",
            )

        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(
            response.content,
            {"ok": True, "payment_id": 7, "invoice_id": "recovery-1", "page_url": "https://mono.test/recovery"},
        )
        mock_build.assert_called_once_with(user_id=1001)
