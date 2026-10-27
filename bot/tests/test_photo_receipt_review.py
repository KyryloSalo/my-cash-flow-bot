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

    async def test_single_merchant_conflict_keeps_verified_financial_fields_for_review(self):
        from test_photo_evidence_safety import passes
        first, second = passes()
        evidence = reconcile_extractions((
            replace(first, merchant='First', transaction_date=date(2026, 10, 1)),
            replace(second, merchant='Other', transaction_date=date(2026, 10, 1)),
        ))
        self.assertFalse(evidence.accepted)
        self.assertEqual(evidence.findings, ('merchant_conflict',))
        result = await self.analyze(evidence)
        self.assertTrue(result.needs_review)
        self.assertEqual(result.amount, Decimal('1234.50'))
        self.assertEqual(result.currency, 'UAH')
        self.assertTrue(vision_tx.serialize_miniapp_screenshot_analysis(result)['currency_explicit'])
        self.assertEqual(result.transaction_date, date(2026, 10, 1))
        self.assertEqual(result.type, 'expense')
        self.assertFalse(result.comment)
        self.assertIn('merchant_conflict', result.field_findings)

    async def test_accepted_single_and_receipt_keep_verified_currency_explicit(self):
        from test_photo_evidence_safety import passes
        for mode in ('single_tx', 'receipt'):
            with self.subTest(mode=mode):
                evidence = reconcile_extractions(tuple(
                    replace(p, mode=mode, merchant='Shop', transaction_date=date(2026, 10, 1))
                    for p in passes()
                ))
                self.assertTrue(evidence.accepted)
                payload = vision_tx.serialize_miniapp_screenshot_analysis(await self.analyze(evidence))
                self.assertEqual(payload['currency'], 'UAH')
                self.assertTrue(payload['currency_explicit'])

    async def test_accepted_statement_keeps_verified_currency_explicit(self):
        from test_photo_statement_row_evidence import StatementRowEvidenceTests
        from photo_shadow import _reconcile_statement_passes
        fixture = StatementRowEvidenceTests()
        full_passes = tuple(fixture.parse([fixture.row()], source) for source in ('full-a', 'full-b'))
        evidence = _reconcile_statement_passes(full_passes, reconcile_extractions(full_passes))
        self.assertTrue(evidence.accepted)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(await self.analyze(evidence))
        self.assertEqual(payload['currency'], 'UAH')
        self.assertTrue(payload['currency_explicit'])

    async def test_merchant_conflict_never_hides_financial_conflicts(self):
        from test_photo_evidence_safety import passes
        first, second = passes()
        second = replace(second, evidence=tuple(
            replace(item, raw_text='999.00', value='999.00') if item.field == 'amount' else item
            for item in second.evidence
        ))
        evidence = reconcile_extractions((replace(first, merchant='First'), replace(second, merchant='Other')))
        self.assertIn('amount_conflict', evidence.findings)
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

    async def test_crop_layout_disagreement_does_not_block_full_receipt_manual_review(self):
        for crop_mode in ('single_tx', 'unsupported'):
            with self.subTest(crop_mode=crop_mode):
                passes = (*tuple(receipt_pass(s) for s in ('full-a', 'full-b')),
                          receipt_pass('crop-0', mode=crop_mode))
                evidence = reconcile_extractions(passes)
                self.assertFalse(evidence.accepted)
                result = await self.analyze(evidence)
                payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
                self.assertTrue(payload['needs_review'])
                self.assertEqual(payload['source'], 'receipt')
                self.assertIn('receipt_crop_layout_conflict', payload['field_findings'])
                for field in ('amount', 'currency', 'transaction_date', 'receipt_total'):
                    self.assertIsNone(payload[field], field)
                self.assertFalse(payload['currency_explicit'])

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

    async def test_excluded_receipts_with_valid_money_never_bypass_reconciliation(self):
        for status, kind in [('declined', 'expense'), ('refunded', 'expense'),
                             ('pending', 'expense'), ('completed', 'income'),
                             ('completed', 'transfer'), ('completed', 'own account'),
                             ('completed', 'refund')]:
            with self.subTest(status=status, kind=kind):
                passes = tuple(replace(receipt_pass(source), evidence=tuple(
                    Evidence(source + '-' + field, field, raw, value)
                    for field, raw, value in [
                        ('amount', '12.34', '12.34'), ('currency', 'UAH', 'UAH'),
                        ('status', status, status),
                        ('type', kind, 'own_account_transfer' if kind == 'own account' else kind),
                    ])) for source in ('full-a', 'full-b'))
                evidence = reconcile_extractions(passes)
                self.assertFalse(evidence.accepted)
                self.assertIn('excluded_receipt', evidence.findings)
                self.assertFalse(evidence.trace['receipt_review_only'])
                with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                    await self.analyze(evidence)

    async def test_receipt_date_disagreement_is_never_replaced_with_today(self):
        first, second = tuple(receipt_pass(s) for s in ('full-a', 'full-b'))
        second = replace(second, transaction_date=date(2026, 10, 2))
        result = await self.analyze(reconcile_extractions((first, second)))
        self.assertIsNone(result.transaction_date)
        self.assertIsNone(result.receipt_date)

    async def test_screenshot_missing_date_status_and_direction_keeps_review_candidates_only(self):
        full = tuple(ExtractionPass(source, mode='single_tx', transaction_date=None, merchant='Shop',
                     evidence=(Evidence(source+'-amount', 'amount', '-1 234,50 ₴', '1234.50'),
                               Evidence(source+'-currency', 'currency', '₴', 'UAH')))
                     for source in ('full-a', 'full-b'))
        evidence = reconcile_extractions(full)
        self.assertFalse(evidence.accepted)
        result = await self.analyze(evidence)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertEqual(payload['mode'], 'single_tx')
        self.assertEqual(payload['source'], 'screenshot')
        # Agreeing evidence pre-fills the editable draft; missing date/type and
        # review findings still prevent this from being a confirmed transaction.
        self.assertEqual(payload['amount'], '1234.50')
        self.assertEqual(payload['currency'], 'UAH')
        for field in ('transaction_date', 'type'):
            self.assertIsNone(payload[field], field)
        self.assertEqual(payload['field_candidates'], {'amount': '1234.50', 'currency': 'UAH', 'comment': 'Shop'})
        self.assertTrue(payload['needs_review'])
        self.assertIn('status_unknown', payload['field_findings'])
        self.assertIn('type_unknown', payload['field_findings'])
        self.assertNotIn('status_conflict', payload['field_findings'])
        self.assertNotIn('type_conflict', payload['field_findings'])

    async def test_explicit_adverse_screenshot_status_never_opens_candidate_fallback(self):
        from test_photo_evidence_safety import passes
        for raw, value in [('failed', 'declined'), ('pending', 'pending'), ('refund', 'refunded')]:
            with self.subTest(status=raw):
                full = passes(status=(raw, value))
                with self.assertRaises(vision_tx.OpenAIVisionInvalidResponseError):
                    await self.analyze(reconcile_extractions(full))

    async def test_receipt_review_preserves_agreeing_independent_fields_as_candidates(self):
        from test_photo_evidence_safety import passes
        first, second = passes()
        evidence = reconcile_extractions(tuple(
            replace(p, mode='receipt', merchant='Shop', transaction_date=date(2026, 10, 1),
                    receipt_total=Decimal('1234.50'), line_items=({'name': 'Item', 'amount': Decimal('1234.50')},))
            for p in (first, second)
        ))
        evidence = replace(evidence, accepted=False, requires_review=True,
                           findings=('receipt_items_conflict',),
                           trace={**evidence.trace, 'receipt_review_only': True})
        result = await self.analyze(evidence)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertIsNone(payload['amount'])
        self.assertIsNone(payload['currency'])
        self.assertIsNone(payload['transaction_date'])
        self.assertEqual(payload['field_candidates'], {
            'amount': '1234.50', 'currency': 'UAH', 'transaction_date': '2026-10-01',
            'comment': 'Shop', 'items': [{'name': 'Item', 'amount': '1234.50'}],
        })

    async def test_receipt_review_missing_date_does_not_default_or_expose_date_candidate(self):
        evidence = reconcile_extractions(tuple(replace(receipt_pass(s), transaction_date=None)
                                                for s in ('full-a', 'full-b')))
        result = await self.analyze(evidence)
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertIsNone(payload['transaction_date'])
        self.assertNotIn('transaction_date', payload['field_candidates'])

    async def test_receipt_review_disputed_amount_is_not_exposed_as_candidate_or_accepted_amount(self):
        first, second = tuple(replace(receipt_pass(s), evidence=tuple(
            replace(item, raw_text='12.34', value='12.34') if item.field == 'amount' else item
            for item in receipt_pass(s).evidence)) for s in ('full-a', 'full-b'))
        second = replace(second, evidence=tuple(replace(item, raw_text='13.34', value='13.34')
                                                 if item.field == 'amount' else item for item in second.evidence))
        result = await self.analyze(reconcile_extractions((first, second)))
        payload = vision_tx.serialize_miniapp_screenshot_analysis(result)
        self.assertIsNone(payload['amount'])
        self.assertNotIn('amount', payload['field_candidates'])


if __name__ == '__main__':
    unittest.main()
