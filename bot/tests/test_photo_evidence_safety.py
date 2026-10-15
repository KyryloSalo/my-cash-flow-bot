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
