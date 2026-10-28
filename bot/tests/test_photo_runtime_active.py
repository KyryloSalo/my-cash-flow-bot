import sys
import types
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import vision_tx
from photo_pipeline import ReconciliationResult


class ActivePhotoRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_date_is_not_replaced_with_today(self):
        evidence = ReconciliationResult(
            accepted=True, requires_review=False, amount=Decimal("12.34"), currency="UAH",
            status="completed", transaction_type="expense", evidence_ids=("a",), findings=(), trace={},
        )
        with patch.object(vision_tx.config, "PHOTO_PIPELINE_ENABLED", True), \
             patch.dict(sys.modules, {"photo_shadow": types.SimpleNamespace(analyze_photo=AsyncMock(return_value=evidence))}):
            with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                await vision_tx.analyze_screenshot(
                    b"image", mime_type="image/png", today=date(2026, 10, 15), default_currency="UAH",
                    accounts=[], expense_categories=[], income_categories=[])

    async def test_enabled_pipeline_replaces_legacy_with_evidence_backed_result(self):
        evidence = ReconciliationResult(
            accepted=True, requires_review=False, amount=Decimal("12.34"), currency="UAH",
            status="completed", transaction_type="expense", evidence_ids=("a",), findings=(),
            trace={"stage": "reconciliation"}, transaction_date=date(2026, 10, 14), merchant="Cafe",
        )
        legacy = object()
        adapter = AsyncMock(return_value=evidence)
        with patch.object(vision_tx, "_analyze_screenshot_legacy", AsyncMock(return_value=legacy)), \
             patch.object(vision_tx.config, "PHOTO_PIPELINE_ENABLED", True), \
             patch.dict(sys.modules, {"photo_shadow": types.SimpleNamespace(analyze_photo=adapter)}):
            result = await vision_tx.analyze_screenshot(
                b"image", mime_type="image/png", today=date(2026, 10, 15), default_currency="UAH",
                accounts=[], expense_categories=[], income_categories=[])
        self.assertEqual(result.amount, Decimal("12.34"))
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(result.type, "expense")
        self.assertEqual(result.comment, "Cafe")
        adapter.assert_awaited_once()

    async def test_enabled_pipeline_fails_closed_on_conflict(self):
        evidence = ReconciliationResult(
            accepted=False, requires_review=True, amount=None, currency=None, status=None,
            transaction_type=None, evidence_ids=(), findings=("amount_conflict",), trace={},
        )
        with patch.object(vision_tx, "_analyze_screenshot_legacy", AsyncMock()), \
             patch.object(vision_tx.config, "PHOTO_PIPELINE_ENABLED", True), \
             patch.dict(sys.modules, {"photo_shadow": types.SimpleNamespace(analyze_photo=AsyncMock(return_value=evidence))}):
            with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                await vision_tx.analyze_screenshot(
                    b"image", mime_type="image/png", today=date(2026, 10, 15), default_currency="UAH",
                    accounts=[], expense_categories=[], income_categories=[])

    async def test_screenshot_review_preserves_supported_field_candidates_and_conflicts(self):
        evidence = ReconciliationResult(
            accepted=False, requires_review=True, amount=Decimal("12.34"), currency="UAH",
            status=None, transaction_type=None, evidence_ids=(),
            findings=("status_unknown", "type_conflict"),
            trace={"screenshot_review_only": True, "unobserved_fields": ["status"],
                   "field_candidates": {"amount": "12.34", "currency": "UAH",
                                        "transaction_date": "2026-10-14",
                                        "comment": "Cafe", "category_hint": "Food"}},
            transaction_date=date(2026, 10, 14), merchant="Cafe")
        with patch.object(vision_tx.config, "PHOTO_PIPELINE_ENABLED", True), \
             patch.dict(sys.modules, {"photo_shadow": types.SimpleNamespace(analyze_photo=AsyncMock(return_value=evidence))}):
            result = await vision_tx.analyze_screenshot(
                b"image", mime_type="image/png", today=date(2026, 10, 15), default_currency="UAH",
                accounts=[], expense_categories=[], income_categories=[])
        self.assertEqual(result.amount, Decimal("12.34"))
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(result.transaction_date, date(2026, 10, 14))
        self.assertEqual(result.comment, "Cafe")
        self.assertEqual(result.category_hint, "Food")
        self.assertIsNone(result.type)
        self.assertTrue(result.needs_review)
        self.assertEqual(result.field_findings, ("status_unknown", "type_conflict", "screenshot_manual_review"))
        self.assertEqual(result.raw_response["field_candidates"]["amount"], "12.34")

    async def test_enabled_pipeline_returns_review_only_statement_to_caller(self):
        evidence = ReconciliationResult(
            accepted=False, requires_review=True, amount=None, currency='UAH', status=None,
            transaction_type=None, evidence_ids=(), findings=('statement_review_only',), trace={},
            statement_review_rows=({'row_index': 0, 'reason': 'missing_status_evidence',
                                    'transaction_date': date(2026, 10, 1), 'amount': Decimal('12.34'),
                                    'currency': 'UAH', 'comment': 'Shop'},), skipped_items_count=1)
        with patch.object(vision_tx.config, 'PHOTO_PIPELINE_ENABLED', True), \
             patch.dict(sys.modules, {'photo_shadow': types.SimpleNamespace(analyze_photo=AsyncMock(return_value=evidence))}):
            result = await vision_tx.analyze_screenshot(
                b'image', mime_type='image/png', today=date(2026, 10, 15), default_currency='UAH',
                accounts=[], expense_categories=[], income_categories=[])
        self.assertEqual(result.mode, 'statement_expenses')
        self.assertTrue(result.needs_review)
        self.assertEqual(len(result.statement_review_rows), 1)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertEqual(payload['statement_review_rows'][0]['transaction_date'], '2026-10-01')
        self.assertEqual(payload['statement_review_rows'][0]['amount'], '12.34')


if __name__ == "__main__":
    unittest.main()
