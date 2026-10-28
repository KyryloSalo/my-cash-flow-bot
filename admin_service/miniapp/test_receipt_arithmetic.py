"""Server recomputes receipt evidence; no provider or personal images involved."""
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import patch

from .tests_unit import SimpleTestCase
from . import services as s


class ReceiptArithmeticTests(SimpleTestCase):
    def setUp(self):
        self.user = NS(tg_user_id=123)
        for name, value in (
            ('locale_for_user', 'uk'),
            ('_active_scoped_account', NS(id=7, currency='UAH', label='Account')),
            ('_active_scoped_category', NS(id=8, name='Category')),
            ('_account_payload', {}), ('_category_payload', {}),
        ):
            self.enterContext(patch.object(s, name, return_value=value))
        self.payload = dict(kind='expense', amount='12.50', account_id=7, category_id=8,
                            currency='UAH', transaction_date='2026-10-01', source='receipt',
                            receipt_items=[{'name': 'Item', 'amount': '12.50'}],
                            receipt_total='12.50', receipt_resolution='items')

    def draft(self, **updates):
        return s.build_transaction_draft(self.user, dict(self.payload, **updates), draft_id='preview')

    def test_recomputes_and_persists_normalized_items_and_total(self):
        result = self.draft()
        evidence = result['receipt_metadata']
        self.assertEqual(evidence['receipt_items'], result['receipt_review'])
        self.assertEqual(evidence['arithmetic'], {
            'state': 'matched', 'resolution': 'items', 'computed_total': '12.50',
            'supplied_total': '12.50', 'transaction_amount': '12.50', 'delta': '0.00',
            'findings': [],
        })

    def test_final_mismatched_items_cannot_be_approved_by_warning_flags(self):
        with self.assertRaises(s.MiniAppTransactionError) as error:
            self.draft(receipt_items=[{'name': 'Item', 'amount': '10'}],
                       needs_review=False, field_findings=[])
        self.assertEqual(error.exception.code, 'receipt_arithmetic_requires_review')

    def test_l1_only_is_explicit_and_retains_unresolved_evidence(self):
        result = self.draft(receipt_items=[{'name': 'Item', 'amount': '10'}],
                            receipt_resolution='l1_only')
        self.assertEqual(result['receipt_review'], [])
        evidence = result['receipt_metadata']
        self.assertEqual(evidence['arithmetic']['state'], 'l1_only')
        self.assertEqual(evidence['arithmetic']['delta'], '-2.50')
        self.assertIn('items_total_mismatch', evidence['arithmetic']['findings'])
        self.assertEqual(evidence['receipt_items'][0]['line_total'], '10.00')

    def test_missing_line_total_is_not_a_zero_or_an_invented_line(self):
        with self.assertRaises(s.MiniAppTransactionError) as error:
            self.draft(receipt_items=[{'name': 'Item', 'quantity': '1'}])
        self.assertEqual(error.exception.code, 'receipt_arithmetic_requires_review')

    def test_empty_optional_items_do_not_block_l1(self):
        result = self.draft(receipt_items=[])
        self.assertEqual(result['receipt_metadata']['arithmetic']['state'], 'not_available')

    def test_explicit_l1_only_does_not_hide_transaction_total_mismatch(self):
        with self.assertRaises(s.MiniAppTransactionError) as error:
            self.draft(amount='13', receipt_resolution='l1_only')
        self.assertEqual(error.exception.code, 'receipt_total_amount_mismatch')
