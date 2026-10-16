import unittest
from dataclasses import replace
from photo_pipeline import Evidence, ExtractionPass, parse_raw_amount, reconcile_extractions


def passes(**overrides):
    base = {'amount': ('1 234,50', '1234.50'), 'currency': ('UAH', 'UAH'),
            'status': ('Оплачено', 'completed'), 'type': ('оплата', 'expense')}
    base.update(overrides)
    return tuple(ExtractionPass(source, tuple(Evidence(source + '-' + field, field, raw, value)
                 for field, (raw, value) in base.items())) for source in ('full-a', 'full-b'))


class PhotoEvidenceSafetyTests(unittest.TestCase):
    def test_single_transaction_merchants_must_agree(self):
        first, second = passes()
        result = reconcile_extractions((replace(first, merchant='First'), replace(second, merchant='Other')))
        self.assertTrue(result.accepted)
        self.assertIn('merchant_conflict', result.findings)
        self.assertIsNone(result.merchant)
        self.assertTrue(result.requires_review)

    def test_full_document_layout_disagreement_is_never_accepted(self):
        first, second = passes()
        result = reconcile_extractions((replace(first, mode='receipt'), second))
        self.assertFalse(result.accepted)
        self.assertIn('document_layout_conflict', result.findings)

    def test_independent_full_dates_must_agree(self):
        from datetime import date
        first, second = passes()
        for other in (date(2026, 10, 2), None):
            with self.subTest(other=other):
                result = reconcile_extractions((replace(first, transaction_date=date(2026, 10, 1)),
                                                replace(second, transaction_date=other)))
                self.assertFalse(result.accepted)
                self.assertIn('transaction_date_conflict', result.findings)

    def test_receipt_metadata_must_agree_across_full_passes(self):
        from decimal import Decimal
        first, second = (replace(p, mode='receipt', receipt_total=Decimal('1234.50'), merchant='Shop',
                                 line_items=({'name':'Item', 'amount':Decimal('1234.50')},)) for p in passes())
        for changes, finding in (({'receipt_total':Decimal('1234.00')}, 'receipt_total_conflict'),
                                 ({'line_items':()}, 'receipt_items_conflict'),
                                 ({'merchant':'Other'}, 'receipt_merchant_conflict')):
            with self.subTest(finding=finding):
                result = reconcile_extractions((first, replace(second, **changes)))
                self.assertFalse(result.accepted)
                self.assertIn(finding, result.findings)

    def test_unsupported_or_history_mode_never_becomes_single_transaction(self):
        first, second = passes()
        for mode in ('unsupported', 'statement_expenses'):
            with self.subTest(mode=mode):
                result = reconcile_extractions((replace(first, mode=mode), replace(second, mode=mode)))
                self.assertFalse(result.accepted)
                self.assertIn('non_single_transaction_layout', result.findings)

    def test_claimed_values_must_match_raw_evidence(self):
        for field, evidence in [('amount', ('60 000', '54000')), ('currency', ('UAH', 'EUR')),
                                ('status', ('failed', 'completed')), ('type', ('transfer', 'expense'))]:
            with self.subTest(field=field):
                self.assertFalse(reconcile_extractions(passes(**{field: evidence})).accepted)

    def test_ambiguous_or_malformed_grouping_is_not_guessed(self):
        for raw in ('1.234', '1,234', '12 34', '1 23 456', '1 2345', 'NaN', 'Infinity', '-50', '0', '1.2345'):
            with self.subTest(raw=raw):
                self.assertIsNone(parse_raw_amount(raw))

    def test_missing_values_in_second_pass_cannot_borrow_first_evidence(self):
        first, second = passes()
        for field in ('amount', 'currency', 'status', 'type'):
            with self.subTest(field=field):
                incomplete = replace(second, evidence=tuple(replace(e, value='') if e.field == field else e for e in second.evidence))
                self.assertFalse(reconcile_extractions((first, incomplete)).accepted)

    def test_ambiguous_dollar_symbol_is_not_usd_evidence(self):
        self.assertFalse(reconcile_extractions(passes(currency=('$', 'USD'))).accepted)

    def test_visible_signed_amount_never_overrides_currency_or_direction(self):
        for raw in ('−43,69 EUR', '+43,69 UAH', '--43,69 UAH', '43,69 balance 43,69 UAH'):
            with self.subTest(raw=raw):
                self.assertFalse(reconcile_extractions(passes(amount=(raw, '43.69'))).accepted)
        self.assertFalse(reconcile_extractions(passes(amount=('−43,69 UAH', '1234.50'))).accepted)

    def test_explicit_payment_action_is_evidence_not_card_direction(self):
        self.assertTrue(reconcile_extractions(passes(type=('Розділити оплату', 'expense'))).accepted)
        self.assertFalse(reconcile_extractions(passes(type=('З картки', 'expense'))).accepted)

    def test_empty_or_duplicate_evidence_identifiers_rejected(self):
        first, second = passes()
        for identifier in ('', first.evidence[0].id):
            with self.subTest(identifier=identifier):
                changed = replace(second, evidence=tuple(replace(e, id=identifier) for e in second.evidence))
                self.assertFalse(reconcile_extractions((first, changed)).accepted)

    def test_positive_income_can_be_classified_without_becoming_expense(self):
        result = reconcile_extractions(passes(type=('дохід', 'income')))
        self.assertTrue(result.accepted)
        self.assertEqual(result.transaction_type, 'income')

    def test_same_visible_evidence_can_consensus_across_pass_local_ids(self):
        first, second = passes()
        second = replace(second, evidence=tuple(replace(item, id='independent-' + item.field) for item in second.evidence))
        result = reconcile_extractions((first, second))
        self.assertTrue(result.accepted)

    def test_missing_field_is_review_not_borrowed_or_draft(self):
        first, second = passes()
        second = replace(second, evidence=tuple(item for item in second.evidence if item.field != 'currency'))
        result = reconcile_extractions((first, second))
        self.assertFalse(result.accepted)
        self.assertTrue(result.requires_review)
        self.assertIn('currency_unknown', result.findings)

    def test_different_regions_for_same_field_conflict_even_when_values_match(self):
        first, second = passes()
        second = replace(second, evidence=tuple(replace(item, region='other-region') if item.field == 'amount' else item for item in second.evidence))
        result = reconcile_extractions((first, second))
        self.assertFalse(result.accepted)
        self.assertIn('amount_conflict', result.findings)
