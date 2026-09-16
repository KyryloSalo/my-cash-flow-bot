"""BILL-002..005: actual billing source and ORM, provider/notify seams only."""
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase, override_settings

from common.test_helpers import ensure_telegram_user_table
from subscriptions import billing
from subscriptions.models import BillingProfile, Payment, Subscription, SubscriptionEvent
from users.models import TelegramUser, UserAdminState

NOW = datetime(2030, 1, 15, 12, tzinfo=UTC)


@override_settings(
    TELEGRAM_BOT_TOKEN='', MONO_RENEWAL_AMOUNT=49900, MONO_RENEWAL_PERIOD_DAYS=30,
    MONO_SOFT_GRACE_DAYS=1, MONO_GRACE_DAYS=30,
)
class BillingBatch1Regressions(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        for name, result in [('_send_billing_expense_prompt_once', True), ('_send_user_message', False),
                             ('_emit_pwa_event', 0), ('send_bind_activation_notification', False)]:
            stub = patch('subscriptions.billing.' + name, return_value=result)
            setattr(self, name, stub.start())
            self.addCleanup(stub.stop)
        clock = patch('subscriptions.billing.timezone.now', return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)
        self.user = TelegramUser.objects.create(tg_user_id=90123001, created_at=NOW, lang='uk')
        self.subscription = Subscription.objects.create(
            user=self.user, provider='monobank', status=Subscription.Status.TRIAL,
            source=Subscription.Source.PAYMENT, amount=Decimal('499.00'), currency='UAH',
            started_at=NOW - timedelta(days=30), expires_at=NOW, next_charge_at=NOW,
            auto_renew=True, trial_days=30,
        )
        self.profile = BillingProfile.objects.create(
            user=self.user, wallet_id='offline-wallet', card_token='offline-synthetic-token',
            status=BillingProfile.Status.ACTIVE, auto_renew_enabled=True,
        )
        UserAdminState.objects.create(telegram_user=self.user, access_scope='personal_full')

    def payment(self, invoice='offline-invoice', **kwargs):
        values = dict(user=self.user, subscription=self.subscription, provider='monobank',
                      provider_payment_id=invoice, amount=Decimal('499.00'), currency='UAH',
                      kind=Payment.Kind.RENEWAL, status=Payment.Status.PENDING)
        values.update(kwargs)
        return Payment.objects.create(**values)

    def event(self, payment, status='success', modified_at=NOW, **extra):
        return billing.process_monobank_event(dict(
            invoiceId=payment.provider_payment_id, status=status,
            modifiedDate=modified_at.isoformat() if modified_at is not None else None, **extra,
        ))

    def test_bill002_repeated_paid_updates_extend_once(self):
        payment = self.payment()
        self.event(payment)
        self.subscription.refresh_from_db()
        expiry = self.subscription.expires_at
        for stamp in (NOW + timedelta(seconds=1), None):
            self.event(payment, modified_at=stamp)
            self.subscription.refresh_from_db()
            self.assertEqual(self.subscription.expires_at, expiry)
        self.assertEqual(SubscriptionEvent.objects.filter(event_type='mono_renewal_paid').count(), 1)

    def test_bill002_paid_cannot_regress_to_processing_or_failure(self):
        payment = self.payment()
        self.event(payment)
        for status in ('processing', 'failure', 'rejected'):
            with self.subTest(status=status):
                self.event(payment, status=status)
                payment.refresh_from_db()
                self.assertEqual(payment.status, Payment.Status.PAID)

    def test_bill002_same_timestamp_pending_can_become_paid(self):
        payment = self.payment(provider_modified_at=NOW)
        _, updated = self.event(payment)
        self.assertTrue(updated)
        payment.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.PAID)

    def test_bill002_refund_cannot_reactivate_paid_invoice(self):
        payment = self.payment()
        self.event(payment)
        self.event(payment, 'reversed', NOW + timedelta(seconds=1))
        self.subscription.refresh_from_db()
        expiry = self.subscription.expires_at
        self.event(payment, 'success', NOW + timedelta(seconds=2))
        payment.refresh_from_db()
        self.subscription.refresh_from_db()
        self.assertEqual(payment.status, Payment.Status.REFUNDED)
        self.assertEqual(self.subscription.expires_at, expiry)

    def test_bill002_repeated_refund_has_one_effect(self):
        payment = self.payment()
        self.event(payment)
        self.event(payment, 'reversed', NOW + timedelta(seconds=1))
        self.subscription.refresh_from_db()
        expiry = self.subscription.expires_at
        self.event(payment, 'reversed', NOW + timedelta(seconds=2))
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, expiry)
        self.assertEqual(SubscriptionEvent.objects.filter(event_type='mono_charge_refunded').count(), 1)

    def test_bill002_second_precreated_bind_does_not_restart_trial(self):
        self.subscription.delete()
        first = self.payment('offline-bind-1', subscription=None, kind=Payment.Kind.BIND,
                             raw_payload={'bind_context': {'trial_granted': True, 'trial_days': 30, 'mode': 'bind'}})
        second = self.payment('offline-bind-2', subscription=None, kind=Payment.Kind.BIND,
                              raw_payload={'bind_context': {'trial_granted': True, 'trial_days': 90, 'mode': 'bind'}})
        self.event(first, walletData={'cardToken': 'offline-token-1'})
        original = Subscription.objects.get(user=self.user)
        with patch('subscriptions.billing.timezone.now', return_value=NOW + timedelta(days=1)):
            self.event(second, modified_at=NOW + timedelta(days=1), walletData={'cardToken': 'offline-token-2'})
        current = Subscription.objects.get(user=self.user)
        self.assertEqual(current.expires_at, original.expires_at)
        self.assertEqual(current.started_at, original.started_at)
        self.assertEqual(current.trial_days, original.trial_days)

    def test_bill002_late_card_token_reconciles_without_restarting_trial(self):
        self.subscription.delete()
        payment = self.payment('offline-bind-token', subscription=None, kind=Payment.Kind.BIND,
                               raw_payload={'bind_context': {'trial_granted': True, 'trial_days': 30, 'mode': 'bind'}})
        with patch('subscriptions.billing.fetch_invoice_status', return_value={'status': 'success'}):
            self.event(payment)
        subscription = Subscription.objects.get(user=self.user)
        expiry = subscription.expires_at
        self.profile.refresh_from_db()
        self.assertEqual(self.profile.status, BillingProfile.Status.MISSING_TOKEN)
        self.event(payment, walletData={'cardToken': 'offline-late-token'})
        self.profile.refresh_from_db()
        subscription.refresh_from_db()
        self.assertEqual(self.profile.card_token, 'offline-late-token')
        self.assertEqual(self.profile.status, BillingProfile.Status.ACTIVE)
        self.assertEqual(subscription.expires_at, expiry)

    def test_bill003_future_period_is_not_charged_from_stale_due_selection(self):
        self.subscription.next_charge_at = NOW + timedelta(days=30)
        self.subscription.expires_at = self.subscription.next_charge_at
        self.subscription.save()
        with patch('subscriptions.billing.charge_wallet_payment') as provider:
            with self.assertRaises(ValueError):
                billing._create_charge_attempt(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        provider.assert_not_called()
        self.assertFalse(Payment.objects.exists())

    def test_bill003_cancelled_subscription_is_not_charged(self):
        self.subscription.auto_renew = False
        self.subscription.save()
        with self.assertRaises(ValueError):
            billing._create_charge_attempt(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        self.assertFalse(Payment.objects.exists())

    def test_bill003_attempt_is_bound_to_subscription_period_and_reference(self):
        _, payment = billing._create_charge_attempt(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        self.assertEqual(payment.subscription_id, self.subscription.pk)
        context = payment.raw_payload['charge_context']
        self.assertEqual(context['period_end'], NOW.isoformat())
        self.assertTrue(context['reference'])

    def test_bill003_second_due_tick_after_success_does_not_charge(self):
        with patch('subscriptions.billing.charge_wallet_payment', return_value={
            'invoiceId': 'offline-due-paid', 'status': 'success', 'modifiedDate': NOW.isoformat(),
        }) as provider:
            self.assertEqual(billing.run_due_monobank_charges()['processed'], 1)
            self.assertEqual(billing.run_due_monobank_charges()['processed'], 0)
            provider.assert_called_once()

    def test_bill004_transport_exception_is_reconcilable_not_failed_or_retried(self):
        from urllib.error import URLError
        for error in (TimeoutError('offline'), URLError('offline'), billing.MonobankAPIError('offline')):
            with self.subTest(error=type(error).__name__):
                with patch('subscriptions.billing.charge_wallet_payment', side_effect=error) as provider:
                    with self.assertRaises(Exception):
                        billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
                    payment = Payment.objects.get()
                    self.assertEqual(payment.status, Payment.Status.PENDING)
                    self.assertEqual(payment.raw_payload.get('charge_transport', {}).get('state'), 'unknown')
                    self.assertTrue(payment.raw_payload['charge_context']['reference'])
                    with self.assertRaises(ValueError):
                        billing.retry_monobank_charge(user_id=self.user.pk, intent_key=f"test-{type(error).__name__}-retry")
                    provider.assert_called_once()
                with patch('subscriptions.billing.fetch_invoice_status') as fetch:
                    result = billing.reconcile_pending_monobank_charges()
                fetch.assert_not_called()
                self.assertEqual(result.get('requires_reconciliation'), 1)
                payment.delete()

    def test_bill004_missing_invoice_response_never_grants_paid_effect(self):
        with patch('subscriptions.billing.charge_wallet_payment', return_value={'status': 'success'}):
            with self.assertRaises(billing.MonobankAPIError):
                billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, NOW)
        self.assertEqual(Payment.objects.get().status, Payment.Status.PENDING)

    def test_bill004_early_webhook_correlates_persisted_reference_before_timeout(self):
        def provider(payload):
            billing.process_monobank_event({
                'invoiceId': 'offline-early', 'status': 'success', 'modifiedDate': NOW.isoformat(),
                'reference': payload['merchantPaymInfo']['reference'], 'amount': 49900, 'ccy': 980,
            })
            raise TimeoutError('accepted before disconnect')
        with patch('subscriptions.billing.charge_wallet_payment', side_effect=provider):
            result = billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        self.assertEqual(result['status'], Payment.Status.PAID)
        self.assertEqual(Payment.objects.get().provider_payment_id, 'offline-early')
        self.assertEqual(SubscriptionEvent.objects.filter(event_type='mono_renewal_paid').count(), 1)

    def test_bill004_operator_reconciliation_checks_identity_then_applies_known_invoice(self):
        from types import SimpleNamespace
        resolver = getattr(billing, 'resolve_unknown_monobank_charge', None)
        self.assertTrue(callable(resolver), 'A controlled operator reconciliation path is required')
        _, payment = billing._create_charge_attempt(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        operator = SimpleNamespace(is_authenticated=True, is_staff=True, pk=99, has_perm=lambda _: True)
        payload = {'invoiceId': 'offline-reconciled', 'reference': 'wrong', 'amount': 49900, 'ccy': 980,
                   'status': 'success', 'modifiedDate': NOW.isoformat()}
        with patch('subscriptions.billing.fetch_invoice_status', return_value=payload):
            with self.assertRaises(ValueError):
                resolver(payment_id=payment.pk, invoice_id='offline-reconciled', admin_user=operator, reason='provider evidence')
        payment.refresh_from_db()
        self.assertIsNone(payment.provider_payment_id)
        payload['reference'] = payment.raw_payload['charge_context']['reference']
        with patch('subscriptions.billing.fetch_invoice_status', return_value=payload), patch('subscriptions.billing.charge_wallet_payment') as charge:
            result = resolver(payment_id=payment.pk, invoice_id='offline-reconciled', admin_user=operator, reason='provider evidence')
        charge.assert_not_called()
        self.assertEqual(result['status'], Payment.Status.PAID)

    def test_bill004_only_explicit_operator_no_charge_evidence_releases_unknown(self):
        from types import SimpleNamespace
        resolver = getattr(billing, 'resolve_unknown_monobank_charge', None)
        self.assertTrue(callable(resolver), 'A controlled operator reconciliation path is required')
        _, payment = billing._create_charge_attempt(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        operator = SimpleNamespace(is_authenticated=True, is_staff=True, pk=99, has_perm=lambda _: True)
        with self.assertRaises(ValueError):
            resolver(payment_id=payment.pk, admin_user=operator, reason='')
        with self.assertRaises(PermissionError):
            resolver(payment_id=payment.pk, admin_user=None, reason='verified', confirmed_no_charge=True)
        with patch('subscriptions.billing.charge_wallet_payment') as charge:
            result = resolver(payment_id=payment.pk, admin_user=operator, reason='provider confirmed no debit', confirmed_no_charge=True)
            billing.run_due_monobank_charges()
        charge.assert_not_called()
        self.assertEqual(result['status'], Payment.Status.REJECTED)
        payment.refresh_from_db()
        self.assertEqual(payment.raw_payload['operator_reconciliation']['admin_user_id'], 99)

    def test_bill005_pending_3ds_survives_next_scheduler_tick(self):
        with patch('subscriptions.billing.charge_wallet_payment', return_value={
            'invoiceId': 'offline-3ds', 'status': 'processing', 'tdsUrl': 'https://example.invalid/3ds',
        }) as provider:
            billing.run_due_monobank_charges()
            result = billing.run_due_monobank_charges()
        self.assertEqual(result['failed'], 0)
        self.assertEqual(result['skipped'], 1)
        provider.assert_called_once()
        self.subscription.refresh_from_db()
        self.profile.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.TRIAL)
        self.assertEqual(self.subscription.next_charge_at, NOW)
        self.assertEqual(self.profile.status, BillingProfile.Status.ACTION_REQUIRED)
        self.assertEqual(self.profile.last_action_url, 'https://example.invalid/3ds')
        self.assertEqual(Payment.objects.get().status, Payment.Status.PENDING)
        self.assertFalse(SubscriptionEvent.objects.filter(event_type='mono_renewal_failed').exists())
        self.assertFalse(any('Автосписання не пройшло' in str(call) for call in self._send_user_message.call_args_list))

    def test_bill005_processing_charge_survives_next_scheduler_tick(self):
        with patch('subscriptions.billing.charge_wallet_payment', return_value={
            'invoiceId': 'offline-processing', 'status': 'processing',
        }) as provider:
            billing.run_due_monobank_charges()
            result = billing.run_due_monobank_charges()
        self.assertEqual(result['failed'], 0)
        provider.assert_called_once()
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.TRIAL)
        self.profile.refresh_from_db()
        self.assertEqual(self.profile.last_charge_status, Payment.Status.PENDING)

    def test_bill005_unknown_scheduler_result_is_not_a_decline(self):
        with patch('subscriptions.billing.charge_wallet_payment', side_effect=TimeoutError('offline')) as provider:
            first = billing.run_due_monobank_charges()
            second = billing.run_due_monobank_charges()
        self.assertEqual(first['failed'], 0)
        self.assertEqual(first['requires_reconciliation'], 1)
        self.assertEqual(second['failed'], 0)
        provider.assert_called_once()
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.TRIAL)
        self.assertIsNone(self.subscription.grace_expires_at)
        self._send_user_message.assert_not_called()

    def test_bill005_missing_card_does_not_fake_provider_failure(self):
        self.profile.card_token = ''
        self.profile.save()
        result = billing.run_due_monobank_charges()
        self.assertEqual(result['failed'], 0)
        self.assertEqual(result['skipped'], 1)
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.TRIAL)
        self._send_user_message.assert_not_called()

    def test_bill005_final_provider_decline_still_applies_grace(self):
        payment = self.payment()
        self.event(payment, 'failure', failureReason='declined')
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.status, Subscription.Status.EXPIRED)
        self.assertIsNone(self.subscription.next_charge_at)
        self.assertIsNotNone(self.subscription.grace_expires_at)

    def test_bill004_invoice_survives_local_effect_rollback(self):
        with patch('subscriptions.billing.charge_wallet_payment', return_value={
            'invoiceId': 'offline-local-error', 'status': 'success',
        }), patch('subscriptions.billing._extend_paid_subscription', side_effect=RuntimeError('local effect failed')):
            with self.assertRaises(RuntimeError):
                billing._run_single_charge(user_id=self.user.pk, payment_kind=Payment.Kind.RENEWAL)
        payment = Payment.objects.get()
        self.assertEqual(payment.provider_payment_id, 'offline-local-error')
        self.assertEqual(payment.status, Payment.Status.PENDING)
        self.assertFalse(payment.raw_payload.get('applied_effects'))

    def test_bill004_orphan_does_not_starve_known_invoice_reconciliation(self):
        orphan = self.payment(invoice=None)
        known = self.payment('offline-known')
        with patch('subscriptions.billing.fetch_invoice_status', return_value={
            'invoiceId': known.provider_payment_id, 'status': 'success',
        }) as fetch:
            result = billing.sync_pending_charge_status(user_id=self.user.pk)
        self.assertTrue(result['updated'])
        fetch.assert_called_once_with(known.provider_payment_id)
        orphan.refresh_from_db()
        self.assertEqual(orphan.status, Payment.Status.PENDING)

    def test_bill002_legacy_recorded_paid_does_not_regrant_after_old_downgrade(self):
        payment = self.payment(paid_at=NOW)
        self.subscription.expires_at = NOW + timedelta(days=30)
        self.subscription.save()
        self.event(payment)
        self.subscription.refresh_from_db()
        self.assertEqual(self.subscription.expires_at, NOW + timedelta(days=30))
