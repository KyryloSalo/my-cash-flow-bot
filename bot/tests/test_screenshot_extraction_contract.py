from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from vision_tx import _parse_response, _prompt, _schema  # noqa: E402


class ScreenshotExtractionContractTests(unittest.TestCase):
    def test_schema_describes_exhaustive_rows_and_explicit_missing_evidence(self) -> None:
        schema = _schema()
        item_description = schema["properties"]["items"]["description"].lower()
        skipped_description = schema["properties"]["skipped_rows_count"]["description"].lower()
        item = schema["properties"]["items"]["items"]

        self.assertIn("every visible transaction row", item_description)
        self.assertIn("before deterministic filtering", item_description)
        self.assertIn("non-transaction", skipped_description)
        self.assertIn("income", skipped_description)
        self.assertIn("transfer", skipped_description)
        self.assertIn("not skipped", skipped_description)
        self.assertIn("amount_text", item["required"])
        self.assertIn("currency_text", item["required"])
        self.assertIn("status", item["required"])
        self.assertIn("operation_kind", item["required"])

    def test_prompt_requires_all_mixed_status_rows_and_no_guessed_facts(self) -> None:
        prompt = _prompt(today=date(2026, 10, 27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]).lower()
        self.assertIn("every visible transaction row", prompt)
        self.assertIn("before deterministic filtering", prompt)
        self.assertIn("missing", prompt)
        self.assertIn("same booked amount", prompt)
        self.assertIn("income", prompt)
        self.assertIn("refund", prompt)
        self.assertIn("declined", prompt)
        self.assertIn("transfer", prompt)

    def test_parser_retains_eight_mixed_status_rows_as_included_or_review(self) -> None:
        statuses = ["posted", "posted", "pending", "declined", "posted", "posted", "unknown", "posted"]
        kinds = ["expense", "income", "transfer", "refund", "expense", "expense", "unknown", "expense"]
        rows = [
            {
                "transaction_date": "2026-10-01", "amount_text": f"{index + 1},00", "date_text": "2026-10-01",
                "currency_text": "₴", "status": status, "operation_kind": kind,
                "type": "expense", "amount": index + 1, "currency": "UAH",
                "category_hint": "", "comment": f"row {index + 1}", "confidence": 0.9,
            }
            for index, (status, kind) in enumerate(zip(statuses, kinds))
        ]
        body = {
            "mode_candidate": "statement_expenses", "amount_text": None, "date_text": None,
            "currency_text": "UAH", "status": "unknown", "operation_kind": "unknown",
            "transaction_date": "", "type": "", "amount": None, "currency": "UAH",
            "dominant_currency": "UAH", "currency_evidence_kind": "code", "currency_evidence_text": "UAH",
            "currency_context_text": "UAH", "account_hint": "", "category_hint": "", "comment": "statement",
            "confidence": 0.9, "bank_name": "", "card_last4": "", "skipped_rows_count": 0,
            "items": rows, "receipt_date": "", "receipt_merchant": "", "receipt_total": None,
            "receipt_items": [], "field_findings": [], "needs_review": True, "arithmetic_warnings": [],
        }
        result = _parse_response({"output_text": json.dumps(body)}, today=date(2026, 10, 27), default_currency="UAH", model="test", detail="low", used_fallback=False)
        self.assertEqual(len(result.statement_items) + len(result.statement_review_rows), 8)
        self.assertEqual(len(result.statement_review_rows), 4)
        self.assertEqual(result.skipped_items_count, 4)
    def test_prompt_requires_literal_evidence_and_no_synthesized_year(self) -> None:
        prompt = _prompt(today=date(2026, 10, 27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]).lower()
        self.assertIn("literal verbatim visible source fragments", prompt)
        self.assertIn("never synthesize a year from today", prompt)
        self.assertIn("incomplete or absent", prompt)

    def test_incomplete_visible_date_stays_literal_and_transaction_date_unknown(self) -> None:
        body = {
            "mode_candidate": "statement_expenses", "amount_text": None, "date_text": None,
            "currency_text": "UAH", "status": "posted", "operation_kind": "expense",
            "transaction_date": "2026-10-06", "type": "expense", "amount": 12,
            "currency": "UAH", "dominant_currency": "UAH", "currency_evidence_kind": "code",
            "currency_evidence_text": "UAH", "currency_context_text": "UAH", "account_hint": "",
            "category_hint": "", "comment": "statement", "confidence": 0.9, "bank_name": "",
            "card_last4": "", "skipped_rows_count": 0,
            "items": [{"transaction_date": "2026-10-06", "date_text": "Вт, 6 жовтня",
                       "amount_text": "12,00", "amount": 12, "currency_text": "₴", "currency": "UAH",
                       "status": "posted", "operation_kind": "expense", "type": "expense",
                       "comment": "Cafe", "category_hint": "", "confidence": 0.9}],
            "receipt_date": "", "receipt_merchant": "", "receipt_total": None, "receipt_items": [],
            "field_findings": [], "needs_review": True, "arithmetic_warnings": [],
        }
        result = _parse_response({"output_text": json.dumps(body)}, today=date(2026, 10, 27), default_currency="UAH", model="test", detail="low", used_fallback=False)
        self.assertIsNone(result.statement_items[0].transaction_date)
        self.assertEqual(result.statement_items[0].field_evidence["date_text"], "Вт, 6 жовтня")


if __name__ == "__main__":
    unittest.main()
