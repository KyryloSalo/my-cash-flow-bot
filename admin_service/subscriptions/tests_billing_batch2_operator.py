"""Guarded operator preview-confirm uses real resolver and ORM."""
from unittest.mock import patch
from django.contrib.auth import get_user_model
from django.contrib.messages.storage.fallback import FallbackStorage
from django.test import RequestFactory
from subscriptions.tests_billing_batch1_regressions import BillingBatch1Regressions
from subscriptions.models import Payment

class BillingBatch2Operator(BillingBatch1Regressions):
    def test_bill004_operator_preview_confirm_is_bound_and_one_shot(self):
        from subscriptions.admin import PaymentAdmin
        from common.admin_site import admin_site
        controller = PaymentAdmin(Payment, admin_site)
        self.assertTrue(callable(getattr(controller,'resolve_unknown_view',None)), 'operator confirmation route missing')
        payment=self.payment(None, raw_payload={'charge_context':{'reference':'synthetic-unknown'}})
        operator=get_user_model().objects.create(username='synthetic-operator',is_staff=True,is_superuser=True)
        def request(data):
            req=RequestFactory().post('/admin/subscriptions/payment/unknown/',data)
            req.user=operator
            req.session={}
            req._messages=FallbackStorage(req)
            return req
        data={'reason':'Synthetic provider statement confirms no debit','confirmed_no_charge':'on'}
        with patch.object(admin_site,'each_context',return_value={}):
            preview=controller.resolve_unknown_view(request(data), str(payment.pk))
            payment.refresh_from_db()
            self.assertEqual(payment.status,'pending')
            token=preview.context_data['form'].initial['preview_token']
            confirm={**data,'preview_token':token,'confirm':'on'}
            changed=controller.resolve_unknown_view(request(confirm),str(payment.pk))
            self.assertEqual(changed.status_code,302)
            replay=controller.resolve_unknown_view(request(confirm),str(payment.pk))
        payment.refresh_from_db()
        self.assertEqual(payment.status,'rejected')
        self.assertEqual(replay.status_code,409)
        self.assertTrue(payment.raw_payload['operator_reconciliation']['confirmed_no_charge'])

    def test_bill003_operator_retry_confirmation_reuses_stable_intent(self):
        from subscriptions.admin import BillingProfileAdmin
        from common.admin_site import admin_site
        controller = BillingProfileAdmin(type(self.profile), admin_site)
        operator = get_user_model().objects.create(username='synthetic-retry-operator', is_staff=True, is_superuser=True)

        def request(data=None):
            factory = RequestFactory()
            req = factory.post('/admin/subscriptions/billingprofile/force-charge/', data) if data is not None else factory.get('/admin/subscriptions/billingprofile/force-charge/')
            req.user = operator
            req.session = {}
            req._messages = FallbackStorage(req)
            return req

        payment = self.payment('synthetic-admin-retry')
        result = {'payment_id': payment.pk, 'invoice_id': payment.provider_payment_id, 'status': payment.status, 'action_url': ''}
        with patch.object(admin_site, 'each_context', return_value={}):
            preview = controller.force_charge_view(request(), str(self.profile.pk))
            intent_key = preview.context_data['form'].initial['intent_key']
            payload = {'confirm': 'on', 'reason': 'synthetic confirmed retry', 'intent_key': intent_key}
            with patch('subscriptions.admin.retry_monobank_charge', return_value=result) as retry:
                controller.force_charge_view(request(payload), str(self.profile.pk))
                controller.force_charge_view(request(payload), str(self.profile.pk))
        self.assertEqual(retry.call_count, 2)
        self.assertEqual(retry.call_args_list[0].kwargs['intent_key'], intent_key)
        self.assertEqual(retry.call_args_list[1].kwargs['intent_key'], intent_key)


def load_tests(loader, tests, pattern):
    import unittest
    return unittest.TestSuite(BillingBatch2Operator(name) for name in BillingBatch2Operator.__dict__ if name.startswith('test_'))
