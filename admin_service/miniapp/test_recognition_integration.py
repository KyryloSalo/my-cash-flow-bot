from types import SimpleNamespace as NS
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory

from .tests_unit import SimpleTestCase
from . import views, services
from .recognition_review import issue_review


class RecognitionIntegrationTests(SimpleTestCase):
    def test_batch_payload_reports_only_authoritative_acknowledgement(self):
        for value in (None, False, "true", True):
            with self.subTest(value=value):
                payload = views._ai_image_batch_payload({"batch_id": "test-batch", "review_acknowledged": value})
                self.assertIs(payload["review_acknowledged"], value is True)

    def allowed_review(self):
        suggestion = dict(self.values, source='receipt', needs_review_bool=True,
                          field_findings=['transaction_date_unknown'])
        issued = issue_review(self.binding, suggestion, attempt_id=self.attempt_id)
        payload = dict(self.payload, field_findings=['transaction_date_unknown'])
        payload['recognition_review'] = dict(token=issued['token'], review_id=issued['review_id'],
            acknowledgements=[i['id'] for i in issued['issues']], values=self.values)
        class Session(dict):
            modified = False
        request = NS(session=Session(recognition_review_active={
            'attempt_id': self.attempt_id, 'source_fingerprint': issued['source_fingerprint'],
            'review_nonce': issued['review_nonce'], 'consumed': False}))
        return payload, request

    def setUp(self):
        self.user = NS(tg_user_id=123)
        self.binding = {'actor_user_id': 123, 'scope_type': 'personal', 'family_id': None}
        self.values = dict(kind='expense', amount='12.50', currency='UAH', account_id='7', category_id='8', transaction_date='2026-10-01')
        self.suggestion = dict(self.values, source='receipt', needs_review_bool=True, field_findings=['status_conflict'])
        self.attempt_id = 'attempt-1'
        self.issued = issue_review(self.binding, self.suggestion, attempt_id=self.attempt_id)
        self.values['receipt_resolution'] = 'items'
        self.values['receipt_items'] = [{'name': 'Item', 'amount': '12.50'}]
        self.payload = dict(self.values, source='receipt', needs_review_bool=True, field_findings=['status_conflict'], receipt_items=self.values['receipt_items'], receipt_resolution='items', recognition_attempt_id=self.attempt_id)
        self.payload['recognition_review'] = dict(token=self.issued['token'], review_id=self.issued['review_id'], acknowledgements=[i['id'] for i in self.issued['issues']], values=self.values)

    def test_review_keeps_blocking_status_conflict_fail_closed(self):
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            with self.assertRaises(Exception):
                views._resolve_recognition_review(self.user, self.payload)

    def test_review_resolves_only_signed_reviewable_findings(self):
        payload, request = self.allowed_review()
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            resolved = views._resolve_recognition_review(self.user, payload, request=request)
        self.assertFalse(resolved['needs_review_bool'])
        self.assertEqual(resolved['field_findings'], [])
        self.assertTrue(resolved['recognition_audit']['resolved_issues'])

    def test_live_request_requires_active_server_context(self):
        payload, request = self.allowed_review()
        request.session.clear()
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            with self.assertRaises(services.MiniAppTransactionError):
                views._resolve_recognition_review(self.user, payload, request=request)

    def test_resolution_does_not_consume_before_financial_validation(self):
        payload, request = self.allowed_review()
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            views._resolve_recognition_review(self.user, payload, request=request)
        self.assertFalse(request.session['recognition_review_active']['consumed'])

    def test_new_tab_review_invalidates_old_snapshot_even_for_same_attempt(self):
        old_payload, request = self.allowed_review()
        new_payload, new_request = self.allowed_review()
        request.session['recognition_review_active'] = new_request.session['recognition_review_active']
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            with self.assertRaises(services.MiniAppTransactionError) as cm:
                views._resolve_recognition_review(self.user, old_payload, request=request)
            self.assertEqual(cm.exception.code, 'recognition_review_required')
            resolved = views._resolve_recognition_review(self.user, new_payload, request=request)
        self.assertIn('recognition_audit', resolved)
        self.assertFalse(request.session['recognition_review_active']['consumed'])

    def test_consumed_tab_review_cannot_create_another_draft(self):
        payload, request = self.allowed_review()
        request.session['recognition_review_active']['consumed'] = True
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            with self.assertRaises(services.MiniAppTransactionError) as cm:
                views._resolve_recognition_review(self.user, payload, request=request)
        self.assertEqual(cm.exception.code, 'recognition_review_required')

    def test_malformed_provider_finding_returns_controlled_api_error(self):
        request = RequestFactory().post('/app/api/ai-image/parse', {
            'image': SimpleUploadedFile('synthetic.png', b'synthetic', content_type='image/png'),
        })
        request.session = {}
        suggestion = dict(self.values, mode='transaction', field_findings=[{'code': []}])
        user = NS(tg_user_id=123, base_currency='UAH')
        with patch.object(views, '_authorized_write_user_or_blocked', return_value=(user, None)), \
             patch.object(views, 'detect_ai_image_mime', return_value='image/png'), \
             patch.object(views, 'normalize_ai_image_upload', return_value=(b'synthetic', 'image/png')), \
             patch.object(views, '_recognition_capture'), \
             patch.object(views, '_recognition_update'), \
             patch.object(views, 'build_ai_image_catalog', return_value={}), \
             patch.object(views, 'request_ai_image_parse', return_value={}), \
             patch.object(views, 'build_ai_image_suggestion', return_value=suggestion), \
             patch.object(views, '_identity_scope_binding', return_value=self.binding):
            response = views.ai_image_parse(request)
        self.assertEqual(response.status_code, 400)
        self.assertIn(b'invalid_suggestion', response.content)
        self.assertNotIn('recognition_review_active', request.session)

    def test_unsigned_added_finding_is_not_silently_cleared(self):
        payload, request = self.allowed_review()
        payload['field_findings'].append('pending')
        with patch.object(views, '_identity_scope_binding', return_value=self.binding):
            with self.assertRaises(services.MiniAppTransactionError):
                views._resolve_recognition_review(self.user, payload, request=request)

    def test_review_cannot_silently_use_today_or_account_currency(self):
        for field in ('transaction_date', 'currency'):
            payload = dict(self.payload, **{field: ''})
            payload['recognition_review'] = dict(self.payload['recognition_review'], values=dict(self.values, **{field: ''}))
            with self.subTest(field=field), patch.object(views, '_identity_scope_binding', return_value=self.binding):
                with self.assertRaises(Exception):
                    views._resolve_recognition_review(self.user, payload)
