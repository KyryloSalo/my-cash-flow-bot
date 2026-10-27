"""Sanitized regressions from real-image failures; no provider or database calls."""
import json
import asyncio
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import vision_tx
from vision_tx import _normalize_amount, _parse_response, serialize_miniapp_screenshot_analysis, analyze_screenshot


class EvidenceNormalizationTests(unittest.TestCase):
    def test_iso_timestamp_evidence_preserves_date_without_ignoring_conflicts(self):
        for evidence in ("2026-10-25 22:17:36", "2026-10-25T22:17:36+03:00"):
            with self.subTest(evidence=evidence):
                _, parsed, findings = vision_tx._verified_fields(
                    {"transaction_date": "2026-10-25", "date_text": evidence},
                    today=date(2026, 10, 27),
                )
                self.assertEqual(parsed, date(2026, 10, 25))
                self.assertEqual(findings, ())
        for evidence, expected in (
            ("2026-10-24 22:17:36", "transaction_date_conflict"),
            ("2026-10-25 99:17:36", "transaction_date_unknown"),
        ):
            with self.subTest(evidence=evidence):
                _, parsed, findings = vision_tx._verified_fields(
                    {"transaction_date": "2026-10-25", "date_text": evidence},
                    today=date(2026, 10, 27),
                )
                self.assertIsNone(parsed)
                self.assertIn(expected, findings)

    def parse(self, **overrides):
        body = {"mode_candidate": "single_tx", "transaction_date": "2026-10-27",
                "type": "expense", "amount": 11200, "currency": "UAH",
                "dominant_currency": "UAH", "comment": "Merchant", "confidence": .95}
        body.update(overrides)
        return _parse_response({"output_text": json.dumps(body)}, today=date(2026, 10, 27),
                               default_currency="UAH", model="test", detail="high", used_fallback=False)

    def test_statement_row_without_currency_does_not_inherit_document_currency(self):
        for row_currency in (None, ""):
            with self.subTest(row_currency=row_currency):
                result = self.parse(mode_candidate="statement_expenses", amount=None, items=[
                    {"type": "expense", "amount": 90, "currency": row_currency,
                     "transaction_date": "2026-10-27", "comment": "Cafe"},
                ])
                row = result.statement_items[0]
                self.assertEqual(row.currency, "")
                self.assertEqual(row.amount, Decimal("90"))
                self.assertTrue(row.needs_review)
                self.assertIn("currency_unknown", row.field_findings)

    def test_document_currency_evidence_does_not_overwrite_statement_rows(self):
        result = self.parse(mode_candidate="statement_expenses", amount=None,
                            currency_evidence_kind="word", currency_evidence_text="UAH",
                            currency_context_text="Account currency UAH", items=[
            {"type": "expense", "amount": 90, "currency": "",
             "transaction_date": "2026-10-27"},
            {"type": "expense", "amount": 20, "currency": "EUR",
             "transaction_date": "2026-10-27"},
        ])
        self.assertEqual([row.currency for row in result.statement_items], ["", "EUR"])

    def test_empty_reconciliation_does_not_invent_today(self):
        from photo_pipeline import reconcile_extractions
        result = reconcile_extractions((), today=date(2026, 10, 27))
        self.assertIsNone(result.transaction_date)
        self.assertFalse(result.accepted)
        self.assertTrue(result.requires_review)

    def test_grouped_money_keeps_all_digits_without_rounding(self):
        for text in ("-112 000,00", "−112\u202f000,00", "-112,000.00", "-112.000,00"):
            with self.subTest(text=text):
                self.assertEqual(_normalize_amount(text), Decimal("112000.00"))

    def test_invalid_or_ambiguous_money_is_not_coerced(self):
        for value in (True, "NaN", "Infinity", "1e6", "1,234", "12.345", "1,2,3", "1.2345", "1 23.00"):
            with self.subTest(value=value):
                self.assertIsNone(_normalize_amount(value))

    def test_amount_disagreement_requires_manual_value_not_autofix(self):
        result = self.parse(amount_text="-112 000,00")
        self.assertIsNone(result.amount)
        self.assertIn("amount_conflict", result.field_findings)
        self.assertTrue(result.needs_review)
        self.assertEqual(result.comment, "Merchant")
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(serialize_miniapp_screenshot_analysis(result)["field_evidence"]["amount_text"], "-112 000,00")

    def test_date_without_visible_year_is_not_completed_from_today(self):
        result = self.parse(amount=90, amount_text="-90,00", date_text="2 вересня")
        self.assertIsNone(result.transaction_date)
        self.assertIn("transaction_date_unknown", result.field_findings)
        self.assertEqual(result.amount, Decimal("90"))

    def test_matching_evidence_does_not_erase_usable_fields(self):
        result = self.parse(amount=112000, amount_text="−112\u202f000,00", date_text="27.10.2026",
                            currency_text="₴", status="posted", operation_kind="expense")
        self.assertEqual(result.amount, Decimal("112000"))
        self.assertEqual(result.transaction_date, date(2026, 10, 27))
        self.assertFalse(result.needs_review)

    def test_currency_conflict_stays_local_and_requires_choice(self):
        result = self.parse(amount=318.68, currency="USD", dominant_currency="USD", currency_text="₴")
        self.assertEqual(result.currency, "")
        self.assertIn("currency_conflict", result.field_findings)
        payload = serialize_miniapp_screenshot_analysis(result)
        self.assertEqual(set(payload["currency_candidates"]), {"UAH", "USD"})
        self.assertEqual(payload["amount"], "318.68")

    def test_status_and_operation_kind_cannot_hide_behind_merchant(self):
        for evidence in ({"status": "declined"}, {"status": "pending"},
                         {"operation_kind": "transfer"}, {"operation_kind": "refund"}):
            with self.subTest(evidence=evidence):
                result = self.parse(amount=90, **evidence)
                self.assertEqual(result.mode, "unsupported")
                self.assertTrue(result.needs_review)
                self.assertIsNone(result.amount)

    def test_higher_confidence_retry_must_not_drop_statement_rows(self):
        rows = [{"type":"expense", "amount":x, "currency":"UAH", "transaction_date":"2026-10-27"} for x in (10,20,30)]
        primary = self.parse(mode_candidate="statement_expenses", amount=None, items=rows, confidence=.8)
        retry = self.parse(mode_candidate="statement_expenses", amount=None, items=rows[:2], confidence=.99)
        with patch("vision_tx.config.PHOTO_PIPELINE_ENABLED", False), patch("vision_tx._needs_fallback", return_value=True), patch("vision_tx._request_analysis", new=AsyncMock(side_effect=[primary,retry])):
            result = asyncio.run(analyze_screenshot(b"synthetic", mime_type="image/png", today=date(2026,10,27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]))
        self.assertEqual(len(result.statement_items), 3)
        self.assertIn("provider_attempt_conflict", result.field_findings)
        self.assertTrue(result.needs_review)

    def test_currency_disagreement_retry_preserves_the_fuller_statement(self):
        rows = [{"type": "expense", "amount": x, "currency": "UAH",
                 "transaction_date": "2026-10-27"} for x in (10, 20, 30)]
        primary = self.parse(mode_candidate="statement_expenses", amount=None,
                             items=rows[:1], confidence=.8)
        retry = self.parse(mode_candidate="statement_expenses", amount=None,
                           currency="EUR", dominant_currency="EUR",
                           items=[{**row, "currency": "EUR"} for row in rows], confidence=.99)
        with patch("vision_tx.config.PHOTO_PIPELINE_ENABLED", False), patch("vision_tx._needs_fallback", return_value=True), patch("vision_tx._request_analysis", new=AsyncMock(side_effect=[primary, retry])):
            result = asyncio.run(analyze_screenshot(b"synthetic", mime_type="image/png", today=date(2026, 10, 27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]))
        self.assertEqual(len(result.statement_items), 3)
        self.assertEqual(set(result.currency_candidates), {"UAH", "EUR"})
        self.assertIn("provider_attempt_conflict", result.field_findings)
        self.assertIn("currency_conflict", result.field_findings)
        self.assertTrue(all(row.needs_review for row in result.statement_items))
        self.assertTrue(all("currency_conflict" in row.field_findings for row in result.statement_items))

    def test_currency_disagreement_must_not_keep_invalid_primary_over_valid_retry(self):
        primary = self.parse(amount=None, type=None, currency="EUR", dominant_currency="EUR")
        retry = self.parse(amount=90, currency="UAH", dominant_currency="UAH")
        with patch("vision_tx.config.PHOTO_PIPELINE_ENABLED", False), patch("vision_tx._request_analysis", new=AsyncMock(side_effect=[primary, retry])):
            result = asyncio.run(analyze_screenshot(b"synthetic", mime_type="image/png", today=date(2026, 10, 27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]))
        self.assertEqual(result.amount, Decimal("90"))
        self.assertEqual(result.mode, "single_tx")
        self.assertTrue(result.needs_review)
        self.assertIn("currency_conflict", result.field_findings)

    def test_retry_cannot_supply_currency_when_primary_is_explicitly_unknown(self):
        primary = self.parse(amount=318.68, currency="", dominant_currency="",
                             currency_text=None, currency_evidence_kind="unknown",
                             currency_evidence_text="", currency_context_text="",
                             confidence=.62, needs_review=True)
        retry = self.parse(amount=318.68, currency="UAH", dominant_currency="UAH",
                           currency_text="UAH", currency_evidence_kind="word",
                           currency_evidence_text="₴", confidence=.76)
        with patch("vision_tx.config.PHOTO_PIPELINE_ENABLED", False), patch("vision_tx._needs_fallback", return_value=True), patch("vision_tx._request_analysis", new=AsyncMock(side_effect=[primary, retry])):
            result = asyncio.run(analyze_screenshot(b"synthetic", mime_type="image/png", today=date(2026, 10, 27), default_currency="UAH", accounts=[], expense_categories=[], income_categories=[]))
        self.assertEqual(result.currency, "")
        self.assertTrue(result.needs_review)
        self.assertIn("provider_attempt_conflict", result.field_findings)

    def test_unknown_status_keeps_draft_but_not_transfer(self):
        result = self.parse(status="unknown", operation_kind="expense")
        self.assertEqual(result.amount, Decimal("11200"))
        self.assertIn("status_unknown", result.field_findings)
        self.assertTrue(result.needs_review)
        self.assertEqual(self.parse(status="unknown", operation_kind="transfer").mode, "unsupported")

    def test_statement_retains_unknown_date_and_excluded_rows_for_review(self):
        result = self.parse(mode_candidate="statement_expenses", amount=None, items=[
            {"transaction_date": "2026-10-02", "date_text": "2 вересня", "type": "expense",
             "amount": -90, "amount_text": "-90,00", "currency": "UAH", "comment": "Cafe"},
            {"transaction_date": "2026-10-02", "type": "expense", "amount": -25,
             "currency": "UAH", "comment": "Provider", "status": "declined"},
            {"transaction_date": "2026-10-02", "type": "expense", "amount": 53.50,
             "amount_text": "+53,50", "currency": "UAH", "comment": "Own card", "operation_kind": "transfer"},
        ])
        self.assertEqual(len(result.statement_items), 1)
        self.assertIsNone(result.statement_items[0].transaction_date)
        self.assertEqual(result.statement_items[0].amount, Decimal("90"))
        self.assertEqual(len(result.statement_review_rows), 2)
        self.assertTrue(all(not row["included"] for row in result.statement_review_rows))
        payload = serialize_miniapp_screenshot_analysis(result)
        self.assertIn("transaction_date_unknown", payload["statement_items"][0]["field_findings"])
        self.assertEqual(payload["skipped_items_count"], 2)


if __name__ == "__main__":
    unittest.main()
