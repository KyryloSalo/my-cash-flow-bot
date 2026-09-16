"""Synthetic I/O; real billing code and ORM regressions for BILL-006..015."""
import json
from datetime import timedelta
from unittest.mock import patch
from subscriptions.tests_billing_batch1_regressions import BillingBatch1Regressions, NOW
from subscriptions.models import Payment, Subscription, SubscriptionEvent
from subscriptions import billing


class BillingBatch2Regressions(BillingBatch1Regressions):
    def test_bill003_retry_requires_nonempty_intent_at_every_entry(self):
        from django.test import RequestFactory, override_settings
        from subscriptions.admin import ForceChargeNowForm
        from subscriptions.views import monobank_retry_renew

        request = RequestFactory().post(
            "/internal/billing/mono/retry-renew",
            data=json.dumps({"telegram_user_id": self.user.pk}),
            content_type="application/json",
            HTTP_X_INTERNAL_TOKEN="offline-internal",
        )
        with override_settings(BILLING_INTERNAL_TOKEN="offline-internal"), patch(
            "subscriptions.billing.charge_wallet_payment"
        ) as provider:
            response = monobank_retry_renew(request)
            with self.assertRaisesRegex(ValueError, "intent"):
                billing.retry_monobank_charge(user_id=self.user.pk)
        self.assertEqual(response.status_code, 400)
        self.assertTrue(ForceChargeNowForm.base_fields["intent_key"].required)
        provider.assert_not_called()

    def test_bill003_retry_request_replay_returns_same_payment(self):
        import json
        from django.test import RequestFactory, override_settings
        from subscriptions.views import monobank_retry_renew
        request = lambda: RequestFactory().post('/internal/billing/mono/retry-renew',
            data=json.dumps({'telegram_user_id':self.user.pk, 'intent_key':'synthetic-retry-001'}),
            content_type='application/json', HTTP_X_INTERNAL_TOKEN='offline-internal')
        with override_settings(BILLING_INTERNAL_TOKEN='offline-internal'), patch(
            'subscriptions.billing.charge_wallet_payment', side_effect=[
                {'invoiceId':'offline-retry-001','status':'success'},
                {'invoiceId':'offline-retry-002','status':'success'}]) as provider:
            first = monobank_retry_renew(request())
            second = monobank_retry_renew(request())
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(json.loads(first.content)['payment_id'], json.loads(second.content)['payment_id'])
        provider.assert_called_once()

    def test_bill006_unexpired_retry_never_dispatches(self):
        self.subscription.expires_at = NOW + timedelta(days=20)
        self.subscription.save()
        with patch('subscriptions.billing.charge_wallet_payment', return_value={'invoiceId':'early','status':'success'}) as provider:
            with self.assertRaises(billing.BillingChargeSkipped):
                billing.retry_monobank_charge(user_id=self.user.pk, intent_key="test-unexpired-retry")
        provider.assert_not_called()

    def test_bill007_late_success_cannot_restore_cancelled_mandate(self):
        for kind, save_card in [(Payment.Kind.RENEWAL, False), (Payment.Kind.RETRY, True), (Payment.Kind.BIND, True)]:
            with self.subTest(kind=kind):
                payment = self.payment('late-' + kind, kind=kind, raw_payload={
                    'recovery_context': {'save_card':True} if kind == Payment.Kind.RETRY else {},
                    'bind_context': {'mode':'rebind', 'trial_granted':False},
                })
                with patch('subscriptions.billing.delete_wallet_card', return_value={}):
                    billing.cancel_auto_renew(user_id=self.user.pk)
                self.event(payment, walletData={'cardToken':'late-synthetic-token'} if save_card else {})
                self.profile.refresh_from_db()
                self.subscription.refresh_from_db()
                self.assertFalse(self.profile.auto_renew_enabled)
                self.assertFalse(self.profile.card_token)
                self.assertFalse(self.subscription.auto_renew)
                self.assertIsNone(self.subscription.next_charge_at)

    def test_bill007_cancel_between_intent_and_dispatch_prevents_debit(self):
        create = billing._create_charge_attempt
        def cancel_after_create(**kwargs):
            result = create(**kwargs)
            with patch('subscriptions.billing.delete_wallet_card', return_value={}):
                billing.cancel_auto_renew(user_id=self.user.pk)
            return result
        with patch('subscriptions.billing._create_charge_attempt', side_effect=cancel_after_create), patch(
            'subscriptions.billing.charge_wallet_payment', return_value={'invoiceId':'cancel-race','status':'success'}) as provider:
            billing.run_due_monobank_charges()
        provider.assert_not_called()

    def test_bill007_new_explicit_bind_after_cancel_is_new_consent(self):
        with patch('subscriptions.billing.create_invoice', side_effect=[
            {'invoiceId':'bind-before-cancel','pageUrl':'https://example.invalid/old'},
            {'invoiceId':'bind-after-cancel','pageUrl':'https://example.invalid/new'}]):
            first = billing.build_bind_invoice(user_id=self.user.pk, trial_days=30)
            with patch('subscriptions.billing.delete_wallet_card', return_value={}):
                billing.cancel_auto_renew(user_id=self.user.pk)
            second = billing.build_bind_invoice(user_id=self.user.pk, trial_days=30)
        self.assertNotEqual(first['payment_id'], second['payment_id'])
        self.event(Payment.objects.get(pk=second['payment_id']), walletData={'cardToken':'explicit-synthetic'})
        self.profile.refresh_from_db()
        self.assertTrue(self.profile.auto_renew_enabled)
        self.assertEqual(self.profile.card_token, 'explicit-synthetic')

    def test_bill008_old_refund_replays_only_its_entitlement(self):
        old = self.payment('refund-A', raw_payload={'charge_context':{'subscription_snapshot_before_charge':billing._subscription_snapshot(self.subscription)}})
        self.event(old)
        self.subscription.refresh_from_db()
        newer = self.payment('refund-B', raw_payload={'charge_context':{'subscription_snapshot_before_charge':billing._subscription_snapshot(self.subscription)}})
        with patch('subscriptions.billing.timezone.now', return_value=NOW + timedelta(days=20)):
            self.event(newer, modified_at=NOW + timedelta(days=20))
        self.event(old, 'reversed', modified_at=NOW + timedelta(days=21))
        self.subscription.refresh_from_db()
        self.profile.refresh_from_db()
        self.assertEqual(self.subscription.payment_id, 'refund-B')
        self.assertEqual(self.subscription.expires_at, NOW + timedelta(days=50))
        self.assertTrue(self.profile.card_token)
        self.event(old, 'reversed', modified_at=NOW + timedelta(days=22))
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, NOW + timedelta(days=50))

    def test_bill008_refund_does_not_erase_manual_lifetime_grant(self):
        old = self.payment('refund-admin')
        self.event(old)
        self.subscription.refresh_from_db()
        self.subscription.status = Subscription.Status.LIFETIME
        self.subscription.source = Subscription.Source.ADMIN
        self.subscription.expires_at = None
        self.subscription.save()
        self.event(old, 'reversed', modified_at=NOW + timedelta(days=1))
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.LIFETIME)
        self.assertIsNone(self.subscription.expires_at)

    def test_bill009_scheduler_reconciles_paid_refund_without_webhook(self):
        from subscriptions.tasks import reconcile_pending_monobank_charges_task
        payment = self.payment('refund-lost-webhook')
        self.event(payment)
        payment.refresh_from_db()
        payment.raw_payload['refund_request'] = {'status':'processing','requested_at':NOW.isoformat()}
        payment.save()
        with patch('subscriptions.billing.fetch_invoice_status', return_value={
            'invoiceId':payment.provider_payment_id, 'status':'reversed','modifiedDate':(NOW + timedelta(minutes=1)).isoformat()}) as fetch:
            reconcile_pending_monobank_charges_task()
        fetch.assert_called_once_with(payment.provider_payment_id)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.REFUNDED)

    def test_bill009_refund_timeout_keeps_durable_unknown_request(self):
        payment = self.payment('refund-timeout')
        self.event(payment)
        with patch('subscriptions.billing.cancel_invoice', side_effect=TimeoutError('synthetic timeout')), patch(
            'subscriptions.billing.delete_wallet_card', return_value={}):
            try:
                billing.refund_latest_monobank_payment(user_id=self.user.pk)
            except (TimeoutError, billing.MonobankAPIError):
                pass
        payment.refresh_from_db()
        self.assertEqual(payment.raw_payload.get('refund_request', {}).get('status'), 'unknown')
        self.assertIsNone(billing.get_latest_refundable_monobank_payment(user_id=self.user.pk))

    def test_bill010_old_bind_failure_keeps_new_card(self):
        old = self.payment('bind-old', kind=Payment.Kind.BIND)
        new = self.payment('bind-new', kind=Payment.Kind.BIND, raw_payload={'bind_context':{'mode':'rebind'}})
        self.event(new, walletData={'cardToken':'new-synthetic-token'})
        self.event(old, 'failure', modified_at=NOW + timedelta(minutes=1))
        self.profile.refresh_from_db()
        self.assertEqual(self.profile.card_token, 'new-synthetic-token')
        self.assertEqual(self.profile.status, 'active')
        self.assertTrue(self.profile.auto_renew_enabled)

    def test_bill006_decline_preserves_unexpired_access(self):
        expiry = NOW + timedelta(days=20)
        self.subscription.expires_at = expiry
        self.subscription.status = Subscription.Status.ACTIVE
        self.subscription.save()
        payment = self.payment()
        self.event(payment, 'failure', failureReason='technical error')
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, expiry)
        self.assertEqual(self.subscription.status, Subscription.Status.ACTIVE)
        self.assertTrue(billing._subscription_grants_full_access(self.subscription))

    def test_failure_and_rejected_callbacks_apply_failure_effect_once(self):
        payment = self.payment("failure-class-once")
        with patch("subscriptions.billing._send_payment_notification_once") as notify:
            self.event(payment, "failure", modified_at=NOW)
            self.event(payment, "rejected", modified_at=NOW + timedelta(minutes=1))
            self.event(payment, "failure", modified_at=NOW + timedelta(minutes=2))

        self.assertEqual(notify.call_count, 1)
        self.assertEqual(
            SubscriptionEvent.objects.filter(
                subscription=self.subscription,
                event_type="mono_renewal_failed",
            ).count(),
            1,
        )

    def test_obsolete_failed_charge_cannot_apply_grace_or_message(self):
        old = self.payment("obsolete-renewal")
        self.payment("newer-bind", kind=Payment.Kind.BIND)
        before = (
            self.subscription.status,
            self.subscription.expires_at,
            self.subscription.grace_expires_at,
        )
        with patch("subscriptions.billing._send_payment_notification_once") as notify:
            self.event(old, "failure", modified_at=NOW + timedelta(minutes=1))
        self.subscription.refresh_from_db()
        self.assertEqual(
            (self.subscription.status, self.subscription.expires_at, self.subscription.grace_expires_at),
            before,
        )
        self.assertFalse(
            SubscriptionEvent.objects.filter(
                subscription=self.subscription,
                event_type="mono_renewal_failed",
                payload__payment_id="obsolete-renewal",
            ).exists()
        )
        notify.assert_not_called()


def load_tests(loader, tests, pattern):
    import unittest
    return unittest.TestSuite(BillingBatch2Regressions(name) for name in BillingBatch2Regressions.__dict__ if name.startswith('test_'))
