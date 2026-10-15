import json
import unittest
from datetime import date
from decimal import Decimal

from photo_shadow import _parse_pass, _schema
from photo_pipeline import reconcile_extractions


class ReceiptProviderTests(unittest.TestCase):
    def response(self, rows=None):
        payload = {
            "evidence": [
                {"id": field, "field": field, "raw_text": raw, "value": value, "region": None}
                for field, raw, value in (
                    ("amount", "10.00", "10"), ("currency", "UAH", "UAH"),
                    ("status", "paid", "completed"), ("type", "purchase", "expense"),
                )
            ],
            "transaction_date": "2026-10-14", "merchant": "Cafe",
            "receipt_total": "10.00", "receipt_items": rows if rows is not None else [{"name": "Coffee", "amount_raw": "10.00"}],
        }
        return json.dumps(payload)

    def test_actual_active_schema_includes_receipt_fields(self):
        schema = _schema()
        self.assertEqual(set(schema["properties"]), set(schema["required"]))
        self.assertIn("receipt_items", schema["properties"])

    def test_provider_receipt_fields_survive_reconciliation(self):
        passes = tuple(_parse_pass(source, self.response()) for source in ("full-a", "full-b"))
        result = reconcile_extractions(passes)
        self.assertTrue(result.accepted, result.findings)
        self.assertEqual(result.transaction_date, date(2026, 10, 14))
        self.assertEqual(result.merchant, "Cafe")
        self.assertEqual(result.receipt_total, Decimal("10.00"))
        self.assertEqual(result.line_items[0]["amount"], Decimal("10.00"))

    def test_malformed_optional_items_preserve_valid_total(self):
        passes = tuple(_parse_pass(source, self.response([{"name": "Coffee", "amount_raw": "unclear"}])) for source in ("full-a", "full-b"))
        result = reconcile_extractions(passes)
        self.assertTrue(result.accepted, result.findings)
        self.assertEqual(result.amount, Decimal("10.00"))
        self.assertEqual(result.line_items, ())
