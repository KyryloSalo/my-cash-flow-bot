import sys
import types
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import vision_tx
from photo_pipeline import Evidence, ExtractionPass, reconcile_extractions


def receipt_pass(source, *, mode='receipt', status='', kind=''):
    return ExtractionPass(
        source=source, mode=mode,
        evidence=tuple(Evidence(source + '-' + field, field, value, value)
                       for field, value in [('amount', 'unreadable'), ('currency', '$'),
                                            ('status', status), ('type', kind)]),
        transaction_date=date(2026, 10, 1), receipt_total=Decimal('12.34'), merchant='Shop',
    )


class ReceiptManualReviewTests(unittest.IsolatedAsyncioTestCase):
    async def analyze(self, evidence):
        with patch.object(vision_tx.config, 'PHOTO_PIPELINE_ENABLED', True), \
             patch.dict(sys.modules, {'photo_shadow': types.SimpleNamespace(analyze_photo=AsyncMock(return_value=evidence))}):
            return await vision_tx.analyze_screenshot(
                b'image', mime_type='image/png', today=date(2026, 10, 16), default_currency='UAH',
                accounts=[], expense_categories=[], income_categories=[])

    async def test_rejected_single_transaction_with_merchant_never_bypasses_guardrails(self):
        evidence = reconcile_extractions(tuple(receipt_pass(s, mode='single_tx') for s in ('full-a', 'full-b')))
        evidence = replace(evidence, amount=Decimal('12.34'))
        with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
            await self.analyze(evidence)

    async def test_unanimous_receipt_opens_blank_manual_review_without_guessed_money_or_date(self):
        evidence = reconcile_extractions(tuple(receipt_pass(s) for s in ('full-a', 'full-b')))
        self.assertFalse(evidence.accepted)
        result = await self.analyze(evidence)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertTrue(payload['needs_review'])
        self.assertEqual(payload['source'], 'receipt')
        for field in ('amount', 'currency', 'transaction_date', 'receipt_total'):
            self.assertIsNone(payload[field], field)
        self.assertFalse(payload['currency_explicit'])
        self.assertIn('receipt_manual_review', payload['field_findings'])

    async def test_mixed_layout_and_crop_only_never_open_receipt_fallback(self):
        pairs = [
            (receipt_pass('full-a'), receipt_pass('full-b', mode='single_tx')),
            (receipt_pass('crop-0'), receipt_pass('crop-1')),
        ]
        for passes in pairs:
            with self.subTest(sources=[p.source for p in passes]):
                with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                    await self.analyze(reconcile_extractions(passes))

    async def test_explicit_excluded_receipts_are_not_expense_suggestions(self):
        for status, kind in [('failed', ''), ('refunded', ''), ('pending', ''), ('', 'income'),
                             ('', 'transfer'), ('', 'own account'), ('', 'refund')]:
            with self.subTest(status=status, kind=kind):
                passes = tuple(receipt_pass(s, status=status, kind=kind) for s in ('full-a', 'full-b'))
                with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                    await self.analyze(reconcile_extractions(passes))

    async def test_receipt_date_disagreement_is_never_replaced_with_today(self):
        first, second = tuple(receipt_pass(s) for s in ('full-a', 'full-b'))
        second = replace(second, transaction_date=date(2026, 10, 2))
        result = await self.analyze(reconcile_extractions((first, second)))
        self.assertIsNone(result.transaction_date)
        self.assertIsNone(result.receipt_date)


if __name__ == '__main__':
    unittest.main()
