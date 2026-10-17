import unittest
from datetime import date
from decimal import Decimal
from photo_pipeline import Evidence, ExtractionPass, reconcile_extractions
from photo_shadow import _reconcile_statement_passes

class StatementPassTests(unittest.TestCase):
    def row(self, comment='Merchant', amount='10'):
        return {'transaction_date':date(2026,10,1),'amount':Decimal(amount),'currency':'UAH','comment':comment,'type':'expense','confidence':0.9}
    def extraction(self,source,rows,skipped=0):
        return ExtractionPass(source=source,raw_evidence={'statement_items':rows,'skipped_items_count':skipped},mode='statement_expenses')
    def reconcile(self,passes):
        return _reconcile_statement_passes(passes,reconcile_extractions(passes))
    def test_crop_omission_does_not_reject_two_agreeing_full_passes(self):
        rows=[self.row(),self.row(amount='20')]
        result=self.reconcile((self.extraction('full-a',rows),self.extraction('full-b',list(reversed(rows))),self.extraction('crop-0',rows[:1])))
        self.assertTrue(result.accepted)
        self.assertEqual(len(result.statement_items),2)
    def test_merchant_disagreement_is_explicit_review(self):
        result=self.reconcile((self.extraction('full-a',[self.row()]),self.extraction('full-b',[self.row('Different OCR')])) )
        self.assertTrue(result.accepted)
        self.assertTrue(result.requires_review)
        self.assertIn('statement_description_conflict',result.findings)
    def test_financial_disagreement_rejects(self):
        result=self.reconcile((self.extraction('full-a',[self.row()]),self.extraction('full-b',[self.row(amount='20')])))
        self.assertFalse(result.accepted)
    def test_duplicate_multiplicity_is_not_collapsed(self):
        result=self.reconcile((self.extraction('full-a',[self.row(),self.row()]),self.extraction('full-b',[self.row()])))
        self.assertFalse(result.accepted)
    def test_crop_cannot_replace_missing_second_full_pass(self):
        result=self.reconcile((self.extraction('full-a',[self.row()]),self.extraction('crop-0',[self.row()])))
        self.assertFalse(result.accepted)
    def test_layout_conflict_preserves_history_only_as_unconfirmed_review(self):
        review = {'row_index': 0, 'reason': 'missing_status_evidence',
                  'transaction_date': date(2026,10,1), 'amount': Decimal('10.00'),
                  'currency': 'UAH', 'comment': 'Shop'}
        passes = (ExtractionPass('full-a', raw_evidence={'statement_items': [],
                     'statement_review_rows': [review]}, mode='statement_expenses'),
                  ExtractionPass('full-b', mode='single_tx'))
        result = self.reconcile(passes)
        self.assertFalse(result.accepted)
        self.assertEqual(result.statement_items, ())
        self.assertEqual(len(result.statement_review_rows), 1)
        self.assertIn('statement_layout_conflict', result.findings)
    def test_skipped_rows_require_review(self):
        result=self.reconcile((self.extraction('full-a',[self.row()],1),self.extraction('full-b',[self.row()],1)))
        self.assertTrue(result.requires_review)

    def test_review_only_statement_is_structured_and_not_accepted(self):
        review = ({'row_index': 0, 'reason': 'missing_status_evidence', 'transaction_date': date(2026,10,1),
                   'amount': Decimal('10.00'), 'currency': 'UAH', 'comment': 'Shop'},)
        passes = (ExtractionPass('full-a', raw_evidence={'statement_items': [], 'statement_review_rows': review, 'skipped_items_count': 1}, mode='statement_expenses'),
                  ExtractionPass('full-b', raw_evidence={'statement_items': [], 'statement_review_rows': review, 'skipped_items_count': 1}, mode='statement_expenses'))
        result = self.reconcile(passes)
        self.assertFalse(result.accepted)
        self.assertTrue(result.requires_review)
        self.assertEqual(len(result.statement_review_rows), 1)
        self.assertIn('statement_review_only', result.findings)

    def test_mixed_statement_keeps_safe_rows_and_review_rows(self):
        safe = self.row()
        review = {'row_index': 1, 'reason': 'missing_status_evidence', 'transaction_date': date(2026,10,2),
                  'amount': Decimal('20.00'), 'currency': 'UAH', 'comment': 'Unknown'}
        passes = (ExtractionPass('full-a', raw_evidence={'statement_items':[safe], 'statement_review_rows':[review], 'skipped_items_count':1}, mode='statement_expenses'),
                  ExtractionPass('full-b', raw_evidence={'statement_items':[safe], 'statement_review_rows':[review], 'skipped_items_count':1}, mode='statement_expenses'))
        result = self.reconcile(passes)
        self.assertTrue(result.accepted)
        self.assertTrue(result.requires_review)
        self.assertEqual(len(result.statement_items), 1)
        self.assertEqual(len(result.statement_review_rows), 1)

    def test_cross_pass_review_rows_are_deduplicated(self):
        review = {'row_index': 0, 'reason': 'missing_status_evidence', 'transaction_date': date(2026,10,1), 'amount': Decimal('10.00'), 'currency': 'UAH', 'comment': 'Shop'}
        passes = (ExtractionPass('full-a', raw_evidence={'statement_items':[self.row()], 'statement_review_rows':[review]}, mode='statement_expenses'), ExtractionPass('full-b', raw_evidence={'statement_items':[self.row()], 'statement_review_rows':[review]}, mode='statement_expenses'))
        result = self.reconcile(passes)
        self.assertEqual(len(result.statement_review_rows), 1)

    def test_review_only_preserves_rows_visible_in_either_full_pass(self):
        first = {'row_index': 0, 'reason': 'missing_status_evidence', 'transaction_date': date(2026,10,1), 'amount': Decimal('10.00'), 'currency': 'UAH', 'comment': 'Shop'}
        second = {**first, 'row_index': 1, 'amount': Decimal('20.00')}
        passes = (
            ExtractionPass('full-a', raw_evidence={'statement_items': [], 'statement_review_rows': [first], 'skipped_items_count': 1}, mode='statement_expenses'),
            ExtractionPass('full-b', raw_evidence={'statement_items': [], 'statement_review_rows': [first, second], 'skipped_items_count': 2}, mode='statement_expenses'),
        )
        result = self.reconcile(passes)
        self.assertFalse(result.accepted)
        self.assertEqual(len(result.statement_review_rows), 2)
        self.assertEqual(result.statement_items, ())

    def test_statement_mode_does_not_route_generic_evidence_through_single_reconciler(self):
        rows = [self.row()]
        generic = (Evidence('a', 'status', 'Успішно', 'completed'),)
        passes = (ExtractionPass('full-a', generic, raw_evidence={'statement_items': rows}, mode='statement_expenses'),
                  ExtractionPass('full-b', generic, raw_evidence={'statement_items': rows}, mode='statement_expenses'))
        result = self.reconcile(passes)
        self.assertTrue(result.accepted)
        self.assertEqual(result.statement_items[0]['amount'], Decimal('10'))
