"""Regression cases use synthetic evidence, never personal bank screenshots."""
from types import SimpleNamespace as NS
from unittest.mock import patch

from .tests_unit import SimpleTestCase
from . import services as s


class ImageNormalizationTests(SimpleTestCase):
    def setUp(self):
        self.user = NS(tg_user_id=123, base_currency="UAH", lang="uk")
        self.enterContext(patch.object(s, "_pick_ai_image_account", return_value=None))
        self.enterContext(patch.object(s, "_pick_ai_image_category", return_value=None))

    def suggestion(self, **fields):
        return s.build_ai_image_suggestion(self.user, dict(mode="single_tx", type="expense", **fields))

    def test_missing_date_is_unknown_not_today(self):
        result = self.suggestion(amount="100", currency="UAH", currency_explicit=True)
        self.assertIsNone(result["transaction_date"])
        self.assertIn("transaction_date", result["missing_fields"])

    def test_unknown_currency_is_not_the_users_base_currency(self):
        result = self.suggestion(amount="100", transaction_date="2026-10-01")
        self.assertEqual(result["currency"], "")
        self.assertIn("currency", result["missing_fields"])
        s._pick_ai_image_account.assert_not_called()

    def test_independent_receipt_candidates_prefill_review_without_accepting_evidence(self):
        result = self.suggestion(source="receipt", needs_review=True, field_candidates={
            "amount": "100.00", "currency": "UAH", "transaction_date": "2026-10-01", "comment": "Synthetic shop",
        })
        self.assertEqual(result["amount"], "100.00")
        self.assertEqual(result["currency"], "UAH")
        self.assertEqual(result["transaction_date"], "2026-10-01")
        self.assertTrue(result["needs_review_bool"])
        self.assertEqual(result["field_candidates"]["amount"], "100.00")

    def test_screenshot_candidates_with_unknown_direction_open_explicit_manual_review(self):
        result = s.build_ai_image_suggestion(self.user, {
            'mode': 'single_tx', 'type': None, 'source': 'screenshot', 'needs_review': True,
            'field_findings': ['status_unknown', 'type_unknown', 'screenshot_manual_review'],
            'field_candidates': {'amount': '100.00', 'currency': 'UAH', 'comment': 'Synthetic shop'},
        })
        self.assertEqual(result['amount'], '100.00')
        self.assertEqual(result['currency'], 'UAH')
        self.assertIsNone(result['transaction_date'])
        self.assertTrue(result['needs_review_bool'])
        self.assertIn('kind', result['missing_fields'])
        self.assertIn('type_unknown', result['field_findings'])


    def test_locale_money_is_parsed_without_float(self):
        for raw in ("1 234,56", "1\u00a0234,56", "1,234.56", "1.234,56"):
            with self.subTest(raw=raw):
                self.assertEqual(self.suggestion(amount=raw)["amount"], "1234.56")

    def test_ambiguous_or_nonfinite_money_requires_input(self):
        for raw in ("1,234", "1.234", "NaN", "Infinity", "-10", "0", "9e100000", "12,34,56"):
            with self.subTest(raw=raw):
                result = self.suggestion(amount=raw)
                self.assertIsNone(result["amount"])
                self.assertIn("amount", result["missing_fields"])


class StatementEvidenceHandoffTests(SimpleTestCase):
    setUp = ImageNormalizationTests.setUp

    def test_blank_row_currency_is_not_filled_from_document(self):
        result = s.build_ai_image_batch(self.user, {
            "mode": "statement_expenses", "source": "statement_transactions", "currency_explicit": True,
            "currency": "UAH", "dominant_currency": "UAH",
            "statement_items": [{"amount": "25.00", "currency": "", "transaction_date": "2026-10-01",
                                 "field_findings": ["currency_conflict"], "needs_review": True}],
        }, batch_id="synthetic-batch")
        suggestion = result["items"][0]["suggestion"]
        self.assertEqual(suggestion["currency"], "")
        self.assertIn("currency", suggestion["missing_fields"])
        self.assertIn("currency_conflict", suggestion["field_findings"])
        s._pick_ai_image_account.assert_not_called()

    def test_statement_with_only_excluded_rows_still_opens_review_batch(self):
        result = s.build_ai_image_suggestion(self.user, {
            "mode": "statement_expenses", "source": "statement_transactions", "statement_items": [],
            "statement_review_rows": [{"included": False, "exclude_reason": "transfer", "amount": "25.00"}],
        })
        self.assertEqual(result["mode"], "batch")
        self.assertEqual(result["item_count"], 0)


class ImageCategoryMappingTests(SimpleTestCase):
    def test_explicit_bank_category_list_suggests_only_unique_scoped_alias(self):
        cafe = NS(id=7, slug='cafe', name='Кафе', aliases=[], kind='expense', type='expense')
        bar = NS(id=8, slug='bar', name='Бари', aliases=[], kind='expense', type='expense')
        def query(categories):
            return NS(filter=lambda **_: NS(order_by=lambda *_: categories))
        with patch.object(s, '_scoped_categories_qs', return_value=query([cafe])):
            self.assertEqual(s._pick_ai_image_category(NS(id=1), kind='expense', category_hint='Ресторани, кафе, бари'), cafe)
        with patch.object(s, '_scoped_categories_qs', return_value=query([cafe, bar])):
            self.assertIsNone(s._pick_ai_image_category(NS(id=1), kind='expense', category_hint='Ресторани, кафе, бари'))
