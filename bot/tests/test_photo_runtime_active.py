import sys
import types
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import vision_tx
from photo_pipeline import ReconciliationResult


class ActivePhotoRuntimeTests(unittest.IsolatedAsyncioTestCase):
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


if __name__ == "__main__":
    unittest.main()
