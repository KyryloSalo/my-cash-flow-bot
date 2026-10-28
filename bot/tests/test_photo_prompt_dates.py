import json
import sys
import unittest
from datetime import date
from decimal import Decimal

sys.path.insert(0, "bot")

import photo_pipeline
import photo_shadow


class PhotoPromptDateTests(unittest.TestCase):
    def test_prompt_never_invents_missing_year(self):
        prompt = photo_shadow._prompt(today=date(2026, 10, 27))
        self.assertIn("never invent", prompt.lower())
        self.assertIn("transaction_date empty", prompt.lower())
        self.assertIn("preserve the literal raw date", prompt.lower())
        self.assertIn("reference date cannot fill", prompt.lower())
        self.assertNotIn("most recent occurrence", prompt.lower())

    def test_pipeline_prompt_version_invalidates_cached_inference(self):
        self.assertEqual(photo_pipeline.PipelineConfig().prompt_version, "photo-evidence-v7")

    def test_single_transaction_schema_can_preserve_literal_date(self):
        schema = photo_shadow._schema()
        self.assertEqual(schema["properties"].get("raw_date"), {"type": ["string", "null"]})
        self.assertIn("raw_date", schema["required"])
        self.assertIn("top-level raw_date", photo_shadow._prompt().lower())

    def test_yearless_literal_date_survives_extraction_for_review(self):
        extraction = photo_shadow._parse_pass("full-1", json.dumps({
            "mode": "single_tx", "evidence": [], "statement_items": [],
            "skipped_items_count": 0, "transaction_date": None, "raw_date": "25 жовтня",
        }))
        self.assertIsNone(extraction.transaction_date)
        self.assertEqual(extraction.raw_evidence.get("raw_date"), "25 жовтня")

    def test_single_claimed_date_requires_complete_literal_evidence(self):
        for literal in ("25 жовтня", "", "25.10.2026, 25:01", "24.10.2026"):
            with self.subTest(literal=literal):
                extraction = photo_shadow._parse_pass("full-1", json.dumps({
                    "mode": "single_tx", "evidence": [], "statement_items": [],
                    "transaction_date": "2026-10-25", "raw_date": literal,
                }))
                self.assertIsNone(extraction.transaction_date)

    def test_statement_claimed_year_is_never_accepted_or_shown_without_literal_year(self):
        for status in ("completed", "unknown"):
            with self.subTest(status=status):
                extraction = photo_shadow._parse_pass("full-1", json.dumps({
                    "mode": "statement_expenses", "evidence": [], "statement_items": [{
                        "transaction_date": "2026-10-25", "raw_date": "25 жовтня",
                        "type": "expense", "status": status, "raw_type": "Оплата",
                        "raw_status": "Оплачено", "amount": "12.34", "raw_amount": "-12.34 UAH",
                        "currency": "UAH", "raw_currency": "UAH", "classification": "expense",
                    }],
                }))
                self.assertEqual(extraction.raw_evidence["statement_items"], [])
                row = extraction.raw_evidence["statement_review_rows"][0]
                self.assertIsNone(row["transaction_date"])
                self.assertEqual(row["amount"], Decimal("12.34"))
                self.assertEqual(row["raw_row"]["raw_date"], "25 жовтня")

    def test_review_never_displays_unprinted_amount_or_default_currency(self):
        extraction = photo_shadow._parse_pass("full-1", json.dumps({
            "mode": "statement_expenses", "evidence": [], "statement_items": [{
                "transaction_date": "", "raw_date": "", "type": "unknown", "status": "unknown",
                "amount": "12.34", "raw_amount": "", "currency": "UAH", "raw_currency": "",
            }],
        }))
        row = extraction.raw_evidence["statement_review_rows"][0]
        self.assertIsNone(row["amount"])
        self.assertIsNone(row["currency"])

    def test_visible_en_dash_amount_is_expense(self):
        amount, currency, direction = photo_pipeline._visible_amount("–12.34 USD")
        self.assertEqual(amount, Decimal("12.34"))
        self.assertEqual(currency, "USD")
        self.assertEqual(direction, "expense")

    def test_visible_plus_amount_preserves_positive_income_direction(self):
        amount, currency, direction = photo_pipeline._visible_amount("+12.34 USD")
        self.assertEqual(amount, Decimal("12.34"))
        self.assertEqual(currency, "USD")
        self.assertEqual(direction, "income")


if __name__ == "__main__":
    unittest.main()
