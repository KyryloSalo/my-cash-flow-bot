"""Real PostgreSQL locking checks. Explicitly skipped on SQLite."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from unittest.mock import patch

from django.db import connections
from django.test import TransactionTestCase, skipUnlessDBFeature

from common.test_helpers import ensure_telegram_user_table
from subscriptions import billing, tests_billing_batch1_regressions as fixtures
from subscriptions.models import Payment, SubscriptionEvent


@skipUnlessDBFeature('has_select_for_update')
class BillingBatch1PostgresConcurrencyTests(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    setUp = fixtures.BillingBatch1Regressions.setUp
    payment = fixtures.BillingBatch1Regressions.payment
    event = fixtures.BillingBatch1Regressions.event

    def tearDown(self):
        # users is unmanaged and Django flush intentionally does not own it.
        self.user.delete()
        super().tearDown()

    def worker(self, callback):
        connection = connections['default']
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '5s'")
                cursor.execute("SET statement_timeout = '15s'")
            return callback()
        finally:
            connection.close()

    def test_bill002_parallel_webhook_and_sync_apply_one_economic_effect(self):
        payment = self.payment()
        barrier = Barrier(2)
        find = billing._find_payment

        def both_read_pending(invoice_id):
            row = find(invoice_id)
            barrier.wait(timeout=5)
            return row

        with patch('subscriptions.billing._find_payment', side_effect=both_read_pending):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(self.worker, lambda: self.event(payment)) for _ in range(2)]
                for future in futures:
                    future.result(timeout=20)
        self.assertEqual(SubscriptionEvent.objects.filter(event_type='mono_renewal_paid').count(), 1)
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, fixtures.NOW + fixtures.timedelta(days=30))

    def test_bill003_stale_beat_after_other_connection_paid_never_dispatches(self):
        completed = Event()
        selected = (self.subscription.pk, self.subscription.next_charge_at)

        def first():
            result = billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RETRY)
            completed.set()
            return result['status']

        def second():
            self.assertTrue(completed.wait(timeout=10))
            with self.assertRaises(billing.BillingChargeSkipped):
                billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL,
                                           expected_subscription_id=selected[0], expected_next_charge_at=selected[1])

        with patch('subscriptions.billing.charge_wallet_payment', return_value={
            'invoiceId': 'offline-concurrent', 'status': 'success', 'modifiedDate': fixtures.NOW.isoformat(),
        }) as provider:
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(self.worker, first)
                b = pool.submit(self.worker, second)
                self.assertEqual(a.result(timeout=20), Payment.Status.PAID)
                b.result(timeout=20)
            provider.assert_called_once()
        self.assertEqual(Payment.objects.count(), 1)
