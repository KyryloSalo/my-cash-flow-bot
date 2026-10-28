import json
import unittest
from decimal import Decimal

from photo_pipeline import ExtractionPass, reconcile_extractions
from photo_shadow import _parse_pass, _reconcile_statement_passes


class StatementReviewAlignmentTests(unittest.TestCase):
    def reconcile(self, passes):
        return _reconcile_statement_passes(passes, reconcile_extractions(passes))

    def test_header_offset_does_not_duplicate_review_operations(self):
        def operation(amount, raw_date, status=''):
            return {'transaction_date': None, 'type': 'unknown', 'status': 'unknown',
                    'raw_date': raw_date, 'raw_amount': amount + ' EUR', 'raw_currency': 'EUR',
                    'raw_status': status, 'raw_type': '', 'classification': 'ambiguous'}

        rows = [operation('-43.09', '18 вересня'), operation('-10.00', '2 вересня'),
                operation('53.50', '2 вересня'), operation('-43.09', '2 вересня', 'Недостатньо коштів'),
                operation('-17.51', '28 серпня'), operation('-24.96', '26 серпня'),
                operation('76.04', '23 серпня')]
        header = {**operation('10.37', '23 вересня'), 'comment': 'Top visible balance/amount, not a transaction row',
                  'review_reason': 'Top visible amount appears to be a balance/header amount, not a transaction row.'}
        passes = tuple(_parse_pass(source, json.dumps({'mode': 'statement_expenses', 'statement_items': body}))
                       for source, body in [('full-a', rows), ('full-b', [header, *rows])])
        result = self.reconcile(passes)
        self.assertFalse(result.accepted)
        self.assertEqual([row['amount'] for row in result.statement_review_rows],
                         list(map(Decimal, ['43.09', '10.00', '53.50', '43.09', '17.51', '24.96', '76.04'])))
        self.assertEqual(len(passes[1].raw_evidence['non_transaction_rows']), 1)

    def test_alignment_preserves_real_same_value_attempts_and_omitted_rows(self):
        def review(index, amount):
            return {'row_index': index, 'reason': 'unknown_date', 'transaction_date': None,
                    'amount': Decimal(amount), 'currency': 'EUR', 'direction': 'expense',
                    'raw_row': {'raw_date': '29 вересня', 'raw_amount': '-' + amount + ' EUR'}}
        first = [review(0, '25.62'), review(1, '10.00'), review(2, '25.62')]
        second = [review(4, '25.62'), review(5, '25.62')]
        passes = tuple(ExtractionPass(source, mode='statement_expenses',
                       raw_evidence={'statement_items': [], 'statement_review_rows': rows})
                       for source, rows in [('full-a', first), ('full-b', second)])
        result = self.reconcile(passes)
        self.assertEqual([row['amount'] for row in result.statement_review_rows],
                         list(map(Decimal, ['25.62', '10.00', '25.62'])))

    def test_unknown_rows_are_not_collapsed_without_literal_identity(self):
        row = {'row_index': 0, 'amount': None, 'currency': None, 'transaction_date': None,
               'reason': 'unknown', 'raw_row': {}}
        passes = tuple(ExtractionPass(source, mode='statement_expenses',
                       raw_evidence={'statement_items': [], 'statement_review_rows': [row]})
                       for source in ['full-a', 'full-b'])
        self.assertEqual(len(self.reconcile(passes).statement_review_rows), 2)

    def test_undated_literal_money_at_the_same_physical_position_is_not_duplicated(self):
        body = [{'transaction_date': '', 'type': 'unknown', 'status': 'unknown',
                 'raw_date': '', 'raw_amount': '-' + amount + ' ₴', 'raw_currency': '₴',
                 'classification': 'ambiguous'} for amount in ['1043.66', '330.54', '612.12']]
        passes = tuple(_parse_pass(source, json.dumps({'mode': 'statement_expenses', 'statement_items': body}))
                       for source in ['full-a', 'full-b'])
        self.assertEqual([row['amount'] for row in self.reconcile(passes).statement_review_rows],
                         list(map(Decimal, ['1043.66', '330.54', '612.12'])))

    def test_header_with_no_literal_amount_and_no_transaction_label_is_not_preserved(self):
        row = {'transaction_date': '', 'type': 'unknown', 'status': 'unknown',
               'raw_amount': '', 'raw_date': '23 вересня', 'raw_currency': 'EUR',
               'classification': 'ambiguous', 'review_reason':
               'Visible date heading with a top balance/amount but no explicit transaction row or payment label; treat as ambiguous history item only.'}
        parsed = _parse_pass('full-a', json.dumps({'mode': 'statement_expenses', 'statement_items': [row]}))
        self.assertEqual(parsed.raw_evidence['statement_review_rows'], [])

    def test_same_ordered_inventory_is_not_duplicated_when_one_pass_omits_dates(self):
        first = [{'transaction_date': None, 'raw_date': '', 'raw_amount': amount + ' UAH',
                  'raw_currency': 'UAH', 'type': 'unknown', 'status': 'unknown',
                  'classification': 'ambiguous'}
                 for amount in ['-20000.00', '-1700.00', '-2000.00', '-2000.00',
                                '-11900.00', '-112000.00', '149500.00', '-18880.00', '18880.00']]
        second = [{**row, 'raw_date': '28 вересня'} for row in first]
        passes = tuple(_parse_pass(source, json.dumps({'mode': 'statement_expenses', 'statement_items': body}))
                       for source, body in [('full-a', first), ('full-b', second)])
        result = self.reconcile(passes)
        self.assertEqual(len(result.statement_review_rows), 9)
        self.assertEqual(sum(row['amount'] == Decimal('2000.00') for row in result.statement_review_rows), 2)
        self.assertTrue(all(row['transaction_date'] is None for row in result.statement_review_rows))
        self.assertEqual(result.statement_review_rows[0]['alternative_raw_rows'][0]['raw_date'], '28 вересня')

    def test_balance_named_merchant_is_not_discarded(self):
        row = {'transaction_date': None, 'type': 'unknown', 'status': 'unknown',
               'raw_amount': '-10 EUR', 'raw_date': '2 вересня', 'raw_currency': 'EUR',
               'comment': 'Balance coffee', 'classification': 'ambiguous'}
        parsed = _parse_pass('full-a', json.dumps({'mode': 'statement_expenses', 'statement_items': [row]}))
        self.assertEqual(len(parsed.raw_evidence['statement_review_rows']), 1)


if __name__ == '__main__':
    unittest.main()
