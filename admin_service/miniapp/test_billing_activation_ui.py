from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, RequestFactory
from django.utils import timezone
from miniapp.services import resolve_access
from subscriptions.models import BillingProfile, Payment, Subscription
from miniapp.models import PartnerTrialClaim
from subscriptions.views import monobank_return
from users.models import TelegramUser


class BillingActivationProjectionTests(TestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=788881, first_name='Synthetic', lang='en')

    def payment(self, status=Payment.Status.PAID):
        return Payment.objects.create(user=self.user, provider='monobank', amount='1.00', currency='UAH', kind='bind', status=status)

    def test_paid_without_entitlement_is_not_another_trial_offer(self):
        self.payment()
        access = resolve_access(self.user)
        self.assertEqual(access['activation_state'], 'paid_activation_pending')
        self.assertEqual(access['recommended_action'], 'check_activation')
        self.assertEqual(access['mode'], 'blocked')
        self.assertIn('Do not pay again', access['entry_message'])

    def test_pending_checkout_is_not_another_trial_offer(self):
        self.payment(Payment.Status.PENDING)
        self.assertEqual(resolve_access(self.user)['activation_state'], 'payment_pending')

    def test_unpaid_checkout_continues_existing_invoice_without_new_consent(self):
        BillingProfile.objects.create(user=self.user, status='pending', last_action_url='https://example.invalid/existing-invoice')
        for status in (Payment.Status.CREATED, Payment.Status.PENDING, Payment.Status.PROCESSING):
            with self.subTest(status=status):
                Payment.objects.filter(user=self.user).delete()
                self.payment(status)
                access = resolve_access(self.user)
                self.assertEqual(access['activation_state'], 'payment_pending')
                self.assertEqual(access['recommended_action'], 'continue_payment')
                self.assertEqual(access['last_action_url'], 'https://example.invalid/existing-invoice')
                self.assertEqual(Payment.objects.filter(user=self.user).count(), 1)

    def test_pending_without_invoice_url_is_check_only(self):
        self.payment(Payment.Status.PENDING)
        self.assertEqual(resolve_access(self.user)['recommended_action'], 'check_activation')

    def test_paid_with_stale_checkout_url_never_recommends_payment(self):
        BillingProfile.objects.create(user=self.user, status='pending', last_action_url='https://example.invalid/paid-invoice')
        self.payment()
        access = resolve_access(self.user)
        self.assertEqual(access['activation_state'], 'paid_activation_pending')
        self.assertEqual(access['recommended_action'], 'check_activation')

    def test_failed_invoice_allows_legitimate_new_binding(self):
        self.payment(Payment.Status.FAILED)
        access = resolve_access(self.user)
        self.assertEqual(access['activation_state'], 'not_started')
        self.assertEqual(access['recommended_action'], 'start_trial')

    def test_active_entitlement_preserves_conscious_rebind(self):
        self.payment()
        Subscription.objects.create(user=self.user, status='active', source='payment', expires_at=timezone.now()+timedelta(days=30))
        access = resolve_access(self.user)
        self.assertEqual(access['mode'], 'active')
        self.assertEqual(access['activation_state'], 'active')

    def test_paid_return_does_not_claim_activation(self):
        self.payment()
        request = RequestFactory().get('/billing/mono/return/', {'return_token':'synthetic', 'lang':'en'})
        with patch('subscriptions.views.parse_bind_return_token', return_value={'telegram_user_id':self.user.pk, 'flow':'bind', 'lang':'en'}), patch('subscriptions.views.sync_pending_bind_status', return_value={}), patch('subscriptions.views._can_offer_app_login', return_value=False):
            response = monobank_return(request)
        self.assertContains(response, 'Payment received')
        self.assertNotContains(response, 'Access activated')
        self.assertContains(response, 'Do not pay again')

    def test_paid_return_reconciles_before_deciding_activation(self):
        self.payment()
        request = RequestFactory().get('/billing/mono/return/', {'return_token':'synthetic', 'lang':'en'})
        def reconcile(**kwargs):
            Subscription.objects.create(user=self.user, status='active', source='payment', expires_at=timezone.now()+timedelta(days=30))
            return {'payment_id': Payment.objects.get(user=self.user).pk}
        with patch('subscriptions.views.parse_bind_return_token', return_value={'telegram_user_id':self.user.pk, 'flow':'bind', 'lang':'en'}), patch('subscriptions.views.sync_pending_bind_status', side_effect=reconcile) as sync, patch('subscriptions.views._can_offer_app_login', return_value=False):
            response = monobank_return(request)
        sync.assert_called_once_with(user_id=self.user.pk)
        self.assertContains(response, 'Access activated')

    def test_later_token_rebind_does_not_hide_unactivated_paid_claim(self):
        self.payment()
        PartnerTrialClaim.objects.create(user=self.user, offer_id='partner_link:synthetic', code='synthetic', name='Synthetic', trial_days=90)
        BillingProfile.objects.create(user=self.user, status='active', card_token='synthetic-local-only', auto_renew_enabled=True)
        self.payment()
        access = resolve_access(self.user)
        self.assertTrue(access['has_card'])
        self.assertEqual(access['activation_state'], 'paid_activation_pending')
        self.assertEqual(access['recommended_action'], 'check_activation')

    def test_manual_grant_overrides_unactivated_paid_claim(self):
        self.payment()
        PartnerTrialClaim.objects.create(user=self.user, offer_id='partner_link:synthetic', code='synthetic', name='Synthetic', trial_days=90)
        Subscription.objects.create(user=self.user, status='manual', source='admin', expires_at=timezone.now()+timedelta(days=30))
        access = resolve_access(self.user)
        self.assertEqual(access['activation_state'], 'active')
        self.assertNotEqual(access['recommended_action'], 'check_activation')
