"""Actual separate PostgreSQL connections exercise bind user-lock serialization."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from django.db import close_old_connections, connection, connections
from django.test import TransactionTestCase, override_settings
from django.utils import timezone

from miniapp.models import PartnerLink, PartnerTrialClaim
from subscriptions import billing
from subscriptions.models import Payment, Subscription, SubscriptionEvent
from users.models import TelegramUser


@override_settings(TELEGRAM_BOT_TOKEN="", MONO_RENEWAL_AMOUNT=49900)
class BindConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=84562001)
        link = PartnerLink.objects.create(name="Concurrency", code="concurrency", trial_days=51)
        self.claim = PartnerTrialClaim.objects.create(
            user=self.user, partner_link=link, offer_id=f"partner_link:{link.pk}",
            code=link.code, name=link.name, trial_days=51,
        )

    def concurrent(self, action):
        self.assertEqual(connection.vendor, "postgresql", "Real PostgreSQL locking is required")
        barrier = Barrier(2)

        def worker():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout TO '15s'")
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]
                barrier.wait(timeout=15)
                return pid, action()
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(worker) for _ in range(2)]
            results = [future.result(timeout=45) for future in futures]
        self.assertNotEqual(results[0][0], results[1][0])
        return [result[1] for result in results]

    def paid_pending(self):
        return Payment.objects.create(
            user=self.user, provider="monobank", provider_payment_id="synthetic-concurrent",
            amount="1.00", currency="UAH", kind=Payment.Kind.BIND, status=Payment.Status.PAID,
            paid_at=timezone.now(), raw_payload={
                "bind_context": {"trial_days":51, "mode":"bind", "trial_granted":True,
                                 "partner_claim_id":self.claim.pk},
                "applied_effects":{"paid":"recorded"},
            },
        )

    def test_concurrent_checkout_reuses_one_provider_invoice(self):
        with patch("subscriptions.billing.create_invoice", return_value={
            "invoiceId":"synthetic-one", "pageUrl":"http://127.0.0.1/synthetic-checkout",
        }) as provider:
            results = self.concurrent(lambda: billing.build_bind_invoice(user_id=self.user.pk, trial_days=51))
        self.assertEqual(provider.call_count, 1)
        self.assertEqual(Payment.objects.filter(user=self.user).count(), 1)
        self.assertEqual(results[0]["payment_id"], results[1]["payment_id"])
        self.assertEqual(sorted(result["reused"] for result in results), [False, True])

    def test_concurrent_paid_pending_checkout_never_creates_invoice(self):
        self.paid_pending()

        def checkout():
            try:
                billing.build_bind_invoice(user_id=self.user.pk, trial_days=51)
            except ValueError as exc:
                return str(exc)
            self.fail("Paid pending checkout unexpectedly accepted")

        with patch("subscriptions.billing.create_invoice") as provider:
            results = self.concurrent(checkout)
        self.assertEqual(results, ["bind_activation_pending", "bind_activation_pending"])
        provider.assert_not_called()
        self.assertEqual(Payment.objects.filter(user=self.user).count(), 1)

    def test_concurrent_late_token_activates_once(self):
        payment = self.paid_pending()
        event = {"invoiceId":payment.provider_payment_id, "status":"success", "amount":100,
                 "ccy":980, "modifiedDate":"2026-11-14T08:00:00Z"}
        with patch("subscriptions.billing._resolve_paid_save_card_payload", return_value=("synthetic",{})), patch(
            "subscriptions.billing.fetch_wallet_cards", return_value={}
        ), patch("subscriptions.billing.fetch_invoice_status", return_value=event):
            self.concurrent(lambda: billing.process_monobank_event(event)[1])
        self.claim.refresh_from_db()
        self.assertIsNotNone(self.claim.activated_at)
        subscription = Subscription.objects.get(user=self.user)
        self.assertEqual(subscription.trial_days, 51)
        self.assertEqual(subscription.payment_id, payment.provider_payment_id)
        self.assertEqual(SubscriptionEvent.objects.filter(user=self.user,event_type="mono_trial_activated").count(), 1)
