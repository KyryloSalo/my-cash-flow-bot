from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch
from .tests_unit import SimpleTestCase
from . import services as s


class ReceiptPersistenceTests(SimpleTestCase):
    def test_screenshot_audit_survives_both_draft_and_commit(self):
        user = NS(tg_user_id=123)
        audit = {'version': 1, 'source': 'screenshot', 'fields': ['amount', 'currency'],
                 'resolved_issues': [{'id': 'i0', 'code': 'status_unknown', 'field': ''}]}
        payload = dict(kind='expense', amount='12.50', account_id=7, category_id=8,
                       currency='UAH', transaction_date='2026-10-01', source='screenshot',
                       recognition_audit=audit)
        account = NS(id=7, currency='UAH', label='Account', balance=Decimal('100'),
                     account_type='main', non_negative_account_type='main', credit_limit=None, save=MagicMock())
        with patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_active_scoped_account', return_value=account), patch.object(s, '_active_scoped_category', return_value=NS(id=8, name='Category')), patch.object(s, '_account_payload', return_value={}), patch.object(s, '_category_payload', return_value={}):
            draft = s.build_transaction_draft(user, payload, draft_id='screenshot-preview')
            self.assertEqual(draft.get('recognition_audit'), audit)
            with patch.object(s, '_resolve_finance_scope', return_value=NS(is_family=False)), patch.object(s.db_transaction, 'atomic', return_value=nullcontext()), patch.object(s.Transaction.objects, 'create', return_value=NS(id=1, created_at=None)) as create, patch.object(s, 'enqueue_transaction_created'):
                result = s.commit_transaction_draft(user, draft)
        self.assertEqual(create.call_args.kwargs['metadata']['recognition_review'], audit)
        self.assertEqual(result['transaction']['metadata']['recognition_review'], audit)

    def test_receipt_draft_preserves_provenance_and_review_metadata(self):
        user = NS(tg_user_id=123)
        payload = dict(kind='expense', amount='12.50', account_id=7, category_id=8,
                       currency='UAH', transaction_date='2026-10-01', source='receipt',
                       receipt_items=[{'name': 'Item', 'amount': '12.50'}],
                       needs_review=[], needs_review_bool=False, field_findings=[], receipt_total='12.50')
        with patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_active_scoped_account', return_value=NS(id=7, currency='UAH', label='Account')), patch.object(s, '_active_scoped_category', return_value=NS(id=8, name='Category')), patch.object(s, '_account_payload', return_value={}), patch.object(s, '_category_payload', return_value={}):
            draft = s.build_transaction_draft(user, payload, draft_id='receipt-preview')
        self.assertEqual(draft['source'], 'receipt')
        self.assertEqual(draft['receipt_metadata'], {
            'source': 'receipt', 'needs_review': [], 'needs_review_bool': False,
            'field_findings': [], 'receipt_total': '12.50',
            'receipt_items': [{'description': 'Item', 'line_total': '12.50', 'status': 'included'}],
            'arithmetic': {'state': 'matched', 'resolution': 'items', 'computed_total': '12.50',
                           'supplied_total': '12.50', 'transaction_amount': '12.50', 'delta': '0.00', 'findings': []},
        })

    def test_receipt_confirm_preserves_provenance_and_review_metadata(self):
        user = NS(tg_user_id=123)
        account = NS(id=7, currency='UAH', balance=Decimal('100'), account_type='main', non_negative_account_type='main', credit_limit=None, save=MagicMock())
        provenance = {'source': 'receipt', 'needs_review': [], 'needs_review_bool': False, 'field_findings': [], 'receipt_total': '12.50'}
        draft = dict(kind='expense', amount_value='12.50', account_id=7, category_id=8, currency='UAH', transaction_date='2026-10-01', source='receipt', receipt_review=[{'name': 'Item', 'amount': '12.50'}], receipt_metadata=provenance)
        with patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_resolve_finance_scope', return_value=NS(is_family=False)), patch.object(s.db_transaction, 'atomic', return_value=nullcontext()), patch.object(s, '_active_scoped_account', return_value=account), patch.object(s, '_active_scoped_category', return_value=NS(id=8, name='Category')), patch.object(s.Transaction.objects, 'create', return_value=NS(id=1, created_at=None)) as create, patch.object(s, 'enqueue_transaction_created'), patch.object(s, '_account_payload', return_value={}), patch.object(s, '_category_payload', return_value={}):
            result = s.commit_transaction_draft(user, draft)
        create.assert_called_once()
        self.assertEqual(create.call_args.kwargs['source'], 'receipt')
        self.assertEqual(create.call_args.kwargs['metadata']['receipt_metadata'], provenance)
        self.assertEqual(result['transaction']['metadata'], create.call_args.kwargs['metadata'])

    def test_receipt_confirm_rejects_missing_or_warning_metadata_before_db_access(self):
        draft = dict(kind='expense', amount_value='12.50', account_id=7, category_id=8,
                     currency='UAH', transaction_date='2026-10-01', source='receipt')
        for metadata in (None, {}, {'source': 'receipt'},
                         {'source': 'receipt', 'needs_review': [], 'needs_review_bool': True, 'field_findings': []}):
            with self.subTest(metadata=metadata), patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_resolve_finance_scope') as scope:
                with self.assertRaises(s.MiniAppTransactionError) as error:
                    s.commit_transaction_draft(NS(tg_user_id=123), dict(draft, receipt_metadata=metadata))
                self.assertEqual(error.exception.code, 'receipt_requires_review')
                scope.assert_not_called()

    def test_runtime_schema_owns_metadata_upgrade(self):
        from runtime_schema import CORE_STATEMENTS
        self.assertIn("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb", CORE_STATEMENTS)

    def test_confirm_creates_one_transaction_with_review_metadata(self):
        user = NS(tg_user_id=123)
        self.enterContext(patch.object(s, '_category_payload', return_value={}))
        account = NS(id=7, currency='UAH', balance=Decimal('100'), account_type='main', non_negative_account_type='main', credit_limit=None, save=MagicMock())
        draft = dict(kind='expense', amount_value='12.50', account_id=7, category_id=8, currency='UAH', transaction_date='2026-10-01', receipt_review=[{'name':'Item', 'amount':'12.50'}])
        with patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_resolve_finance_scope', return_value=NS(is_family=False)), patch.object(s.db_transaction, 'atomic', return_value=nullcontext()), patch.object(s, '_active_scoped_account', return_value=account), patch.object(s, '_active_scoped_category', return_value=NS(id=8, name='Category')), patch.object(s.Transaction.objects, 'create', return_value=NS(id=1, created_at=None)) as create, patch.object(s, 'enqueue_transaction_created'), patch.object(s, '_account_payload', return_value={}):
            result = s.commit_transaction_draft(user, draft)
        create.assert_called_once()
        expected = {'receipt_review':[{'description':'Item', 'status':'included', 'line_total':'12.50'}]}
        self.assertEqual(create.call_args.kwargs['metadata'], expected)
        self.assertEqual(result['transaction']['metadata'], expected)
