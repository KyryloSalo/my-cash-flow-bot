import json
import unittest
from datetime import date
from decimal import Decimal

from photo_shadow import _parse_pass, _schema, _prompt, _payload
from unittest.mock import patch
from photo_pipeline import reconcile_extractions


class ReceiptProviderTests(unittest.TestCase):
    def test_provider_budget_supports_multiline_statement_schema(self):
        with patch('photo_shadow.config.OPENAI_MAX_OUTPUT_TOKENS', 600):
            payload = _payload(image_bytes=b'image', mime_type='image/jpeg', model='test', detail='high')
        self.assertGreaterEqual(payload['max_output_tokens'], 4096)

    def test_prompt_uses_requested_date_context_for_yearless_bank_dates(self):
        prompt = _prompt(today=date(2026, 10, 16))
        self.assertIn("2026-10-16", prompt)
        self.assertIn("transaction_date", prompt)
        self.assertIn("currency.raw_text", prompt)

    def test_prompt_specifies_normalized_values_and_primary_transaction_role(self):
        prompt = _prompt()
        for instruction in ('positive decimal magnitude', 'completed', 'expense', 'Розділити оплату', 'original currency', 'region=null'):
            self.assertIn(instruction, prompt)

    def response(self, rows=None):
        payload = {
            "mode": "receipt",
            "evidence": [
                {"id": field, "field": field, "raw_text": raw, "value": value, "region": None}
                for field, raw, value in (
                    ("amount", "10.00", "10"), ("currency", "UAH", "UAH"),
                    ("status", "paid", "completed"), ("type", "purchase", "expense"),
                )
            ],
            "transaction_date": "2026-10-14", "raw_date": "14.10.2026", "merchant": "Cafe",
            "receipt_total": "10.00", "receipt_items": rows if rows is not None else [{"name": "Coffee", "amount_raw": "10.00"}],
        }
        return json.dumps(payload)

    def test_actual_active_schema_includes_receipt_fields(self):
        schema = _schema()
        self.assertEqual(set(schema["properties"]), set(schema["required"]))
        self.assertIn("receipt_items", schema["properties"])

    def test_visible_category_hint_is_retained_only_when_full_passes_agree(self):
        self.assertIn('category_hint', _schema()['properties'])
        payload = json.loads(self.response())
        payload['category_hint'] = 'Food'
        first = _parse_pass('full-a', json.dumps(payload))
        second = _parse_pass('full-b', json.dumps(payload))
        result = reconcile_extractions((first, second))
        self.assertEqual(result.trace['field_candidates']['category_hint'], 'Food')
        payload['category_hint'] = 'Transport'
        conflict = reconcile_extractions((first, _parse_pass('full-b', json.dumps(payload))))
        self.assertNotIn('category_hint', conflict.trace['field_candidates'])

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
