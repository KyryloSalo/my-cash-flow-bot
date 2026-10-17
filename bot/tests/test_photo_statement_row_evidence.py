import json
import unittest
from photo_shadow import _parse_pass, _reconcile_statement_passes, _prompt
from photo_pipeline import reconcile_extractions

class StatementRowEvidenceTests(unittest.TestCase):
    def test_prompt_retains_visible_single_fields_when_other_fields_are_ambiguous(self):
        prompt = _prompt()
        self.assertNotIn('unsupported or ambiguous single-transaction evidence, return empty evidence', prompt)
        self.assertIn('For a recognizable single transaction, return mode=single_tx even when a core field is missing', prompt)
        self.assertIn('preserve visible fields and omit only missing or ambiguous fields', prompt)

    def test_history_prompt_keeps_document_classification_separate_from_row_eligibility(self):
        prompt = _prompt()
        self.assertIn('Repeated transaction rows with date headings MUST use statement_expenses', prompt)
        self.assertIn('Missing status/type labels do not make a history unsupported', prompt)
        self.assertIn('use unknown and leave the corresponding raw field empty', prompt)

    def test_prompt_separates_single_and_history_response_containers(self):
        prompt = _prompt()
        self.assertIn('For single_tx and receipt, statement_items MUST be []', prompt)
        self.assertIn('put all visible core fields in evidence, never in statement_items', prompt)
        self.assertIn('A transaction history remains statement_expenses even if no row is eligible', prompt)

    def test_prompt_does_not_instruct_provider_to_omit_ineligible_rows(self):
        prompt = _prompt()
        self.assertNotIn('Exclude income, refunds, failed/declined rows', prompt)
        self.assertIn('Do not omit income, refunds, failed/declined rows', prompt)

    def row(self, **changes):
        row = dict(transaction_date='2026-10-01', amount='10.00', currency='UAH', type='expense', status='completed', comment='Shop', confidence=.9, raw_date='01.10.2026', raw_amount='-10.00 UAH', raw_currency='UAH', raw_status='Оплачено', raw_type='Оплата', classification='expense', review_reason='')
        row.update(changes)
        return row

    def parse(self, rows, source='full-a'):
        return _parse_pass(source, json.dumps(dict(mode='statement_expenses', evidence=[], statement_items=rows, skipped_items_count=0)))

    def test_raw_amount_must_support_claim(self):
        p = self.parse([self.row(raw_amount='-20.00 UAH')])
        self.assertEqual(p.raw_evidence['statement_items'], [])
        self.assertEqual(p.raw_evidence['statement_review_rows'][0]['reason'], 'amount_evidence_mismatch')

    def test_missing_date_has_visible_review_not_today(self):
        p = self.parse([self.row(transaction_date='', raw_date='')])
        self.assertEqual(p.raw_evidence['statement_items'], [])
        self.assertEqual(p.raw_evidence['skipped_items_count'], 1)
        self.assertEqual(len(p.raw_evidence['statement_review_rows']), 1)

    def test_excluded_rows_retain_reason(self):
        p = self.parse([self.row(type='income', raw_type='Дохід'), self.row(status='declined', raw_status='Відхилено')])
        self.assertEqual(p.raw_evidence['skipped_items_count'], 2)
        self.assertEqual(len(p.raw_evidence['statement_review_rows']), 2)

    def test_excluded_row_recovers_visible_magnitude_without_promoting_income(self):
        from decimal import Decimal
        p = self.parse([self.row(type='income', status='unknown', amount='', raw_amount='+12.30 UAH')])
        self.assertEqual(p.raw_evidence['statement_items'], [])
        review = p.raw_evidence['statement_review_rows'][0]
        self.assertEqual(review['amount'], Decimal('12.30'))
        self.assertEqual(review['direction'], 'income')

    def test_review_amount_conflict_displays_visible_value_not_provider_claim(self):
        from decimal import Decimal
        p = self.parse([self.row(amount='999.00', raw_amount='-10.00 UAH')])
        self.assertEqual(p.raw_evidence['statement_items'], [])
        review = p.raw_evidence['statement_review_rows'][0]
        self.assertEqual(review['amount'], Decimal('10.00'))
        self.assertEqual(review['reason'], 'amount_evidence_mismatch')
        self.assertEqual(review['direction'], 'expense')

    def test_non_numeric_statement_metadata_is_reviewed_not_crashed(self):
        p = self.parse([self.row(confidence="not-a-number")])
        self.assertEqual(p.raw_evidence["statement_items"][0]["confidence"], 0.0)
        p = self.parse([self.row()], source="full-b")
        self.assertEqual(p.raw_evidence["skipped_items_count"], 0)
    def test_missing_raw_type_not_accepted(self):
        p = self.parse([self.row(raw_type='')])
        self.assertEqual(p.raw_evidence['statement_items'], [])

    def test_unresolved_row_is_preserved_for_review(self):
        from decimal import Decimal
        p = self.parse([self.row(raw_status='', raw_type='')])
        review = p.raw_evidence['statement_review_rows']
        self.assertEqual(len(review), 1)
        self.assertEqual(review[0]['transaction_date'].isoformat(), '2026-10-01')
        self.assertEqual(review[0]['amount'], Decimal('10.00'))
        self.assertEqual(review[0]['reason'], 'status_type_or_currency_evidence_mismatch')

    def test_duplicates_retained_and_exclusions_surfaced(self):
        rows = [self.row(), self.row(), self.row(type='transfer')]
        passes = (self.parse(rows), self.parse(rows, 'full-b'))
        result = _reconcile_statement_passes(passes, reconcile_extractions(passes))
        self.assertTrue(result.accepted)
        self.assertTrue(result.requires_review)
        self.assertEqual(len(result.statement_items), 2)
        self.assertEqual(len(result.raw_evidence[0]['statement_review_rows']), 1)

    def test_full_pass_amount_disagreement_fails_closed(self):
        passes = (self.parse([self.row()]), self.parse([self.row(amount='20.00', raw_amount='-20.00 UAH')], 'full-b'))
        result = _reconcile_statement_passes(passes, reconcile_extractions(passes))
        self.assertFalse(result.accepted)

    def test_review_rows_are_not_duplicated_by_description_or_reason_variants(self):
        first = self.row(type='unknown', status='unknown', comment='Shop')
        second = self.row(raw_type='', raw_status='', comment='SHOP')
        passes = (self.parse([first, first]), self.parse([second, second], 'full-b'))
        result = _reconcile_statement_passes(passes, reconcile_extractions(passes))
        self.assertFalse(result.accepted)
        self.assertEqual(len(result.statement_items), 0)
        self.assertEqual(len(result.statement_review_rows), 2)
        self.assertEqual([row['row_index'] for row in result.statement_review_rows], [0, 1])

if __name__ == '__main__':
    unittest.main()
