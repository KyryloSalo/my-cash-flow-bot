from __future__ import annotations

import json
import os
import sys
import types
import unittest
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.modules.setdefault("httpx", types.SimpleNamespace(AsyncClient=object, HTTPError=Exception))

from vision_tx import (  # noqa: E402
    ScreenshotAnalysis,
    StatementItemAnalysis,
    _needs_fallback,
    _parse_response,
    _prefer_currency_retry,
    _prompt,
    _should_prefer_default_for_weak_symbol_conflict,
    is_screenshot_currency_explicit,
    serialize_miniapp_screenshot_analysis,
)


class VisionTxParseTests(unittest.TestCase):
    @staticmethod
    def _payload(body: dict) -> dict:
        return {"output_text": json.dumps(body, ensure_ascii=False)}

    def test_statement_items_accept_negative_amounts_and_try_aliases(self) -> None:
        analysis = _parse_response(
            self._payload(
                {
                    "mode_candidate": "statement_expenses",
                    "transaction_date": "",
                    "type": "",
                    "amount": None,
                    "currency": "TRY",
                    "dominant_currency": "TRY",
                    "account_hint": "monobank",
                    "category_hint": "",
                    "comment": "statement",
                    "confidence": 0.83,
                    "bank_name": "monobank",
                    "card_last4": "1111",
                    "skipped_rows_count": 0,
                    "items": [
                        {
                            "transaction_date": "2026-06-01",
                            "type": "expense",
                            "amount": -78.65,
                            "currency": "₺",
                            "category_hint": "market",
                            "comment": "ALANYA",
                            "confidence": 0.94,
                        },
                        {
                            "transaction_date": "2026-06-01",
                            "type": "expense",
                            "amount": -856.25,
                            "currency": "TL",
                            "category_hint": "market",
                            "comment": "BIM",
                            "confidence": 0.91,
                        },
                    ],
                }
            ),
            today=date(2026, 6, 1),
            default_currency="UAH",
            model="test-model",
            detail="low",
            used_fallback=False,
        )

        self.assertEqual(analysis.mode, "statement_expenses")
        self.assertEqual(analysis.dominant_currency, "TRY")
        self.assertEqual(len(analysis.statement_items), 2)
        self.assertEqual(analysis.statement_items[0].amount, Decimal("78.65"))
        self.assertEqual(analysis.statement_items[0].currency, "TRY")
        self.assertEqual(analysis.statement_items[1].amount, Decimal("856.25"))
        self.assertEqual(analysis.statement_items[1].currency, "TRY")
        self.assertEqual(analysis.skipped_items_count, 0)

    def test_statement_rejected_and_own_account_rows_are_dropped_from_model_output(self) -> None:
        analysis = _parse_response(
            self._payload({
                "mode_candidate": "statement_expenses", "transaction_date": "", "type": "",
                "amount": None, "currency": "UAH", "dominant_currency": "UAH",
                "account_hint": "", "category_hint": "", "comment": "statement",
                "confidence": 0.9, "bank_name": "monobank", "card_last4": "",
                "skipped_rows_count": 1,
                "items": [
                    {"transaction_date": "2026-10-01", "type": "expense", "amount": -50,
                     "currency": "UAH", "category_hint": "coffee", "comment": "Coffee shop", "confidence": .9},
                    {"transaction_date": "2026-10-01", "type": "expense", "amount": -100,
                     "currency": "UAH", "category_hint": "", "comment": "Недостатньо коштів", "confidence": .9},
                    {"transaction_date": "2026-10-01", "type": "expense", "amount": -200,
                     "currency": "UAH", "category_hint": "", "comment": "Поповнення власної банки", "confidence": .9},
                ],
            }), today=date(2026, 10, 14), default_currency="UAH", model="test-model",
            detail="high", used_fallback=False,
        )
        self.assertEqual(len(analysis.statement_items), 1)
        self.assertEqual(analysis.statement_items[0].comment, "Coffee shop")
        self.assertEqual(analysis.skipped_items_count, 3)

    def test_statement_model_exclusion_annotations_are_dropped_from_model_output(self) -> None:
        for comment in (
            "Hetzner (rejected shown, not taken as expense)",
            "FIREFLIES.AI payment (insufficient funds; excluded)",
        ):
            analysis = _parse_response(
                self._payload({
                    "mode_candidate": "statement_expenses", "transaction_date": "", "type": "",
                    "amount": None, "currency": "EUR", "dominant_currency": "EUR",
                    "account_hint": "", "category_hint": "", "comment": "statement",
                    "confidence": 0.9, "bank_name": "monobank", "card_last4": "",
                    "skipped_rows_count": 0,
                    "items": [
                        {"transaction_date": "2026-09-18", "type": "expense", "amount": -43.09,
                         "currency": "EUR", "category_hint": "hosting", "comment": comment, "confidence": .1},
                        {"transaction_date": "2026-09-17", "type": "expense", "amount": -17.51,
                         "currency": "EUR", "category_hint": "software", "comment": "OpenAI", "confidence": .9},
                    ],
                }), today=date(2026, 10, 14), default_currency="EUR", model="test-model",
                detail="high", used_fallback=False,
            )
            self.assertEqual(analysis.mode, "single_tx")
            self.assertEqual(analysis.amount, Decimal("17.51"))
            self.assertEqual(analysis.skipped_items_count, 1)

    def test_ambiguous_transfer_and_currency_context_do_not_suppress_expense(self) -> None:
        for comment in ("Переказ на картку", "Оплата Failed Foods", "З гривневого рахунку — оплата FIREFLIES"):
            analysis = _parse_response(self._payload({"mode_candidate": "single_tx", "transaction_date": "2026-10-01", "type": "expense", "amount": -100, "currency": "UAH", "dominant_currency": "UAH", "account_hint": "", "category_hint": "", "comment": comment, "confidence": .9, "bank_name": "monobank", "card_last4": "", "skipped_rows_count": 0, "items": []}), today=date(2026, 10, 14), default_currency="UAH", model="test-model", detail="high", used_fallback=False)
            self.assertEqual(analysis.mode, "single_tx")
            self.assertEqual(analysis.amount, Decimal("100"))

    def test_failed_status_requires_status_context_not_merchant_substring(self) -> None:
        for comment in ("Payment failed", "Платіж failed", "Payment was declined"):
            analysis = _parse_response(self._payload({"mode_candidate": "single_tx", "transaction_date": "2026-10-01", "type": "expense", "amount": -100, "currency": "UAH", "dominant_currency": "UAH", "account_hint": "", "category_hint": "", "comment": comment, "confidence": .9, "bank_name": "monobank", "card_last4": "", "skipped_rows_count": 0, "items": []}), today=date(2026, 10, 14), default_currency="UAH", model="test-model", detail="high", used_fallback=False)
            self.assertEqual(analysis.mode, "unsupported")
        merchant = _parse_response(self._payload({"mode_candidate": "single_tx", "transaction_date": "2026-10-01", "type": "expense", "amount": -100, "currency": "UAH", "dominant_currency": "UAH", "account_hint": "", "category_hint": "", "comment": "Failed Foods", "confidence": .9, "bank_name": "monobank", "card_last4": "", "skipped_rows_count": 0, "items": []}), today=date(2026, 10, 14), default_currency="UAH", model="test-model", detail="high", used_fallback=False)
        self.assertEqual(merchant.mode, "single_tx")

        excluded_merchant = _parse_response(
            self._payload({
                "mode_candidate": "single_tx", "transaction_date": "2026-10-01", "type": "expense",
                "amount": -100, "currency": "UAH", "dominant_currency": "UAH", "account_hint": "",
                "category_hint": "restaurant", "comment": "Excluded Cafe", "confidence": .9,
                "bank_name": "monobank", "card_last4": "", "skipped_rows_count": 0, "items": [],
            }), today=date(2026, 10, 14), default_currency="UAH", model="test-model",
            detail="high", used_fallback=False,
        )
        self.assertEqual(excluded_merchant.mode, "single_tx")

    def test_single_rejected_or_explicit_own_account_row_is_unsupported(self) -> None:
        for comment in (
            "Платіж відхилено: Недостатньо коштів",
            "Переказ на свою картку",
            "Зняття готівки в банкоматі",
            "Cash withdrawal at ATM",
        ):
            analysis = _parse_response(
                self._payload({
                    "mode_candidate": "single_tx", "transaction_date": "2026-10-01", "type": "expense",
                    "amount": -100, "currency": "UAH", "dominant_currency": "UAH",
                    "account_hint": "", "category_hint": "", "comment": comment,
                    "confidence": .9, "bank_name": "monobank", "card_last4": "",
                    "skipped_rows_count": 0, "items": [],
                }), today=date(2026, 10, 14), default_currency="UAH", model="test-model",
                detail="high", used_fallback=False,
            )
            self.assertEqual(analysis.mode, "unsupported")
            self.assertIsNone(analysis.amount)

    def test_single_tx_accepts_negative_amount_and_currency_symbol(self) -> None:
        analysis = _parse_response(
            self._payload(
                {
                    "mode_candidate": "single_tx",
                    "transaction_date": "2026-06-01",
                    "type": "expense",
                    "amount": -1628.97,
                    "currency": "₺",
                    "dominant_currency": "₺",
                    "account_hint": "monobank",
                    "category_hint": "pharmacy",
                    "comment": "VITAMIN ECZANESI",
                    "confidence": 0.88,
                    "bank_name": "monobank",
                    "card_last4": "1111",
                    "skipped_rows_count": 0,
                    "items": [],
                }
            ),
            today=date(2026, 6, 1),
            default_currency="UAH",
            model="test-model",
            detail="high",
            used_fallback=True,
        )

        self.assertEqual(analysis.mode, "single_tx")
        self.assertEqual(analysis.type, "expense")
        self.assertEqual(analysis.amount, Decimal("1628.97"))
        self.assertEqual(analysis.currency, "TRY")
        self.assertEqual(analysis.dominant_currency, "TRY")

    def test_miniapp_serialization_keeps_normalized_fields_without_raw_response(self) -> None:
        analysis = ScreenshotAnalysis(
            transaction_date=date(2026, 7, 14),
            type="expense",
            amount=Decimal("250.00"),
            currency="UAH",
            account_hint="monobank",
            category_hint="taxi",
            comment="Taxi",
            confidence=0.91,
            bank_name="monobank",
            card_last4="1234",
            model="test-model",
            detail="high",
            used_fallback=False,
            raw_response={"sensitive": "do-not-send"},
            currency_evidence_kind="symbol",
            currency_evidence_text="₴",
            currency_resolution="weak_symbol_default",
        )

        payload = serialize_miniapp_screenshot_analysis(analysis)

        self.assertEqual(payload["amount"], "250.00")
        self.assertEqual(payload["transaction_date"], "2026-07-14")
        self.assertFalse(payload["currency_explicit"])
        self.assertNotIn("raw_response", payload)
        self.assertFalse(is_screenshot_currency_explicit(analysis))

    def test_statement_items_infer_dominant_currency_from_rows_when_top_level_missing(self) -> None:
        analysis = _parse_response(
            self._payload(
                {
                    "mode_candidate": "statement_expenses",
                    "transaction_date": "",
                    "type": "",
                    "amount": None,
                    "currency": "",
                    "dominant_currency": "",
                    "account_hint": "monobank",
                    "category_hint": "",
                    "comment": "statement",
                    "confidence": 0.82,
                    "bank_name": "monobank",
                    "card_last4": "1111",
                    "skipped_rows_count": 0,
                    "items": [
                        {
                            "transaction_date": "2026-06-01",
                            "type": "expense",
                            "amount": -78.65,
                            "currency": "",
                            "category_hint": "market",
                            "comment": "ALANYA",
                            "confidence": 0.94,
                        },
                        {
                            "transaction_date": "2026-06-01",
                            "type": "expense",
                            "amount": -856.25,
                            "currency": "TL",
                            "category_hint": "groceries",
                            "comment": "BIM",
                            "confidence": 0.91,
                        },
                        {
                            "transaction_date": "2026-06-01",
                            "type": "expense",
                            "amount": -1628.97,
                            "currency": "\u20ba",
                            "category_hint": "pharmacy",
                            "comment": "VITAMIN ECZANESI",
                            "confidence": 0.90,
                        },
                    ],
                }
            ),
            today=date(2026, 6, 1),
            default_currency="UAH",
            model="test-model",
            detail="low",
            used_fallback=False,
        )

        self.assertEqual(analysis.mode, "statement_expenses")
        self.assertEqual(analysis.dominant_currency, "TRY")
        self.assertEqual(len(analysis.statement_items), 3)
        self.assertEqual(analysis.statement_items[0].currency, "TRY")
        self.assertEqual(analysis.statement_items[1].currency, "TRY")
        self.assertEqual(analysis.statement_items[2].currency, "TRY")

    def test_prompt_explicitly_mentions_turkish_lira_sign(self) -> None:
        prompt = _prompt(
            today=date(2026, 6, 1),
            default_currency="UAH",
            accounts=[],
            expense_categories=[],
            income_categories=[],
        )

        self.assertIn("U+20B4", prompt)
        self.assertIn("U+20BA", prompt)
        self.assertIn("U+20AC", prompt)
        self.assertIn("currency_evidence_kind", prompt)
        self.assertIn("currency_evidence_text", prompt)
        self.assertIn("currency_context_text", prompt)

    def test_currency_evidence_symbol_overrides_conflicting_eur_parse(self) -> None:
        analysis = _parse_response(
            self._payload(
                {
                    "mode_candidate": "statement_expenses",
                    "transaction_date": "",
                    "type": "",
                    "amount": None,
                    "currency": "EUR",
                    "dominant_currency": "EUR",
                    "currency_evidence_kind": "symbol",
                    "currency_evidence_text": "₴",
                    "account_hint": "",
                    "category_hint": "",
                    "comment": "statement",
                    "confidence": 0.89,
                    "bank_name": "",
                    "card_last4": "",
                    "skipped_rows_count": 0,
                    "items": [
                        {
                            "transaction_date": "2026-06-02",
                            "type": "expense",
                            "amount": -78.65,
                            "currency": "EUR",
                            "category_hint": "other",
                            "comment": "ALANYA",
                            "confidence": 0.91,
                        },
                        {
                            "transaction_date": "2026-06-02",
                            "type": "expense",
                            "amount": -856.25,
                            "currency": "EUR",
                            "category_hint": "groceries",
                            "comment": "BiM",
                            "confidence": 0.90,
                        },
                    ],
                }
            ),
            today=date(2026, 6, 2),
            default_currency="UAH",
            model="test-model",
            detail="high",
            used_fallback=True,
        )

        self.assertEqual(analysis.currency, "UAH")
        self.assertEqual(analysis.dominant_currency, "UAH")
        self.assertEqual(analysis.currency_evidence_kind, "symbol")
        self.assertEqual(analysis.currency_evidence_text, "₴")
        self.assertEqual(analysis.currency_resolution, "evidence_override")
        self.assertTrue(all(item.currency == "UAH" for item in analysis.statement_items))

    def test_context_text_overrides_symbol_only_try_misread_to_uah(self) -> None:
        analysis = _parse_response(
            self._payload(
                {
                    "mode_candidate": "statement_expenses",
                    "transaction_date": "",
                    "type": "",
                    "amount": None,
                    "currency": "TRY",
                    "dominant_currency": "TRY",
                    "currency_evidence_kind": "symbol",
                    "currency_evidence_text": "₺",
                    "currency_context_text": "З гривневого рахунку ФОП",
                    "account_hint": "monobank",
                    "category_hint": "",
                    "comment": "statement",
                    "confidence": 0.86,
                    "bank_name": "monobank",
                    "card_last4": "",
                    "skipped_rows_count": 2,
                    "items": [
                        {
                            "transaction_date": "2026-06-03",
                            "type": "expense",
                            "amount": -252.88,
                            "currency": "TRY",
                            "category_hint": "groceries",
                            "comment": "OZDE KASAP",
                            "confidence": 0.91,
                        },
                        {
                            "transaction_date": "2026-06-03",
                            "type": "expense",
                            "amount": -164.85,
                            "currency": "TRY",
                            "category_hint": "groceries",
                            "comment": "TARIK DONDURMA",
                            "confidence": 0.90,
                        },
                    ],
                }
            ),
            today=date(2026, 6, 3),
            default_currency="UAH",
            model="test-model",
            detail="original",
            used_fallback=True,
        )

        self.assertEqual(analysis.currency, "UAH")
        self.assertEqual(analysis.dominant_currency, "UAH")
        self.assertEqual(analysis.currency_context_text, "З гривневого рахунку ФОП")
        self.assertEqual(analysis.currency_resolution, "context_text_override")
        self.assertTrue(all(item.currency == "UAH" for item in analysis.statement_items))

    def test_needs_fallback_when_ambiguous_currency_differs_from_default(self) -> None:
        result = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=None,
            currency="EUR",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.93,
            bank_name=None,
            card_last4=None,
            model="test-model",
            detail="high",
            used_fallback=False,
            mode="statement_expenses",
            dominant_currency="EUR",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("856.25"),
                    currency="EUR",
                    category_hint=None,
                    comment="BIM",
                    confidence=0.91,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("1628.97"),
                    currency="EUR",
                    category_hint=None,
                    comment="VITAMIN ECZANESI",
                    confidence=0.90,
                ),
            ),
            skipped_items_count=0,
        )

        self.assertTrue(_needs_fallback(result, default_currency="UAH"))

    def test_weak_symbol_only_conflict_prefers_default_currency(self) -> None:
        result = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 2),
            type="expense",
            amount=Decimal("4696.73"),
            currency="EUR",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.84,
            bank_name=None,
            card_last4=None,
            model="test-model",
            detail="original",
            used_fallback=True,
            mode="statement_expenses",
            dominant_currency="EUR",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 2),
                    type="expense",
                    amount=Decimal("78.65"),
                    currency="EUR",
                    category_hint=None,
                    comment="ALANYA",
                    confidence=0.90,
                ),
            ),
            skipped_items_count=0,
            currency_evidence_kind="symbol",
            currency_evidence_text="€",
        )

        self.assertTrue(_should_prefer_default_for_weak_symbol_conflict(result, default_currency="UAH"))

    def test_prefer_currency_retry_when_retry_matches_default_currency(self) -> None:
        primary = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=Decimal("856.25"),
            currency="EUR",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.93,
            bank_name=None,
            card_last4=None,
            model="test-model",
            detail="high",
            used_fallback=False,
            mode="statement_expenses",
            dominant_currency="EUR",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("856.25"),
                    currency="EUR",
                    category_hint=None,
                    comment="BIM",
                    confidence=0.91,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("1628.97"),
                    currency="EUR",
                    category_hint=None,
                    comment="VITAMIN ECZANESI",
                    confidence=0.90,
                ),
            ),
            skipped_items_count=0,
        )
        retry = ScreenshotAnalysis(
            transaction_date=date(2026, 6, 1),
            type="expense",
            amount=Decimal("856.25"),
            currency="UAH",
            account_hint=None,
            category_hint=None,
            comment="statement",
            confidence=0.88,
            bank_name=None,
            card_last4=None,
            model="test-model",
            detail="original",
            used_fallback=True,
            mode="statement_expenses",
            dominant_currency="UAH",
            statement_items=(
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("856.25"),
                    currency="UAH",
                    category_hint=None,
                    comment="BIM",
                    confidence=0.89,
                ),
                StatementItemAnalysis(
                    transaction_date=date(2026, 6, 1),
                    type="expense",
                    amount=Decimal("1628.97"),
                    currency="UAH",
                    category_hint=None,
                    comment="VITAMIN ECZANESI",
                    confidence=0.88,
                ),
            ),
            skipped_items_count=0,
        )

        self.assertTrue(_prefer_currency_retry(primary, retry, default_currency="UAH"))


    def test_mocked_provider_receipt_roundtrip_through_serializer(self) -> None:
        analysis = _parse_response(
            self._payload({
                "mode_candidate": "unsupported", "transaction_date": "", "type": "", "amount": None,
                "currency": "EUR", "dominant_currency": "EUR", "currency_evidence_kind": "code",
                "currency_evidence_text": "EUR", "currency_context_text": "", "account_hint": "",
                "category_hint": "", "comment": "receipt", "confidence": 0.96, "bank_name": "",
                "card_last4": "", "skipped_rows_count": 0, "items": [],
                "receipt_date": "2026-10-14", "receipt_merchant": "Corner Market", "receipt_total": 12.50,
                "receipt_items": [{"name": "Coffee", "quantity": 2, "unit_price": 3.25, "total": 6.50,
                                   "currency": "EUR", "evidence": "Coffee 2 x 3.25", "confidence": 0.94},
                                  {"name": "Sandwich", "quantity": 1, "unit_price": 6.00, "total": 6.00,
                                   "currency": "EUR", "evidence": "Sandwich 6.00", "confidence": 0.91}],
                "field_findings": ["Date and total are clearly visible."], "needs_review": False,
                "arithmetic_warnings": [],
            }), today=date(2026, 10, 15), default_currency="UAH", model="test-model",
            detail="high", used_fallback=False,
        )
        payload = serialize_miniapp_screenshot_analysis(analysis)
        self.assertEqual(payload["receipt_merchant"], "Corner Market")
        self.assertEqual(payload["receipt_total"], "12.50")
        self.assertEqual(payload["receipt_items"][0]["name"], "Coffee")
        self.assertEqual(payload["receipt_items"][0]["evidence"], "Coffee 2 x 3.25")
        self.assertFalse(payload["needs_review"])

    def test_malformed_optional_l2_preserves_l1_and_requests_review(self) -> None:
        analysis = _parse_response(
            self._payload({
                "mode_candidate": "unsupported", "transaction_date": "", "type": "", "amount": None,
                "currency": "UAH", "dominant_currency": "UAH", "currency_evidence_kind": "code",
                "currency_evidence_text": "UAH", "currency_context_text": "", "account_hint": "",
                "category_hint": "", "comment": "receipt", "confidence": 0.9, "bank_name": "",
                "card_last4": "", "skipped_rows_count": 0, "items": [],
                "receipt_date": "2026-10-14", "receipt_merchant": "Market", "receipt_total": 20,
                "receipt_items": [{"name": "Valid", "quantity": 1, "unit_price": 20, "total": 20,
                                   "currency": "UAH", "evidence": "Valid 20.00", "confidence": 0.9},
                                  {"name": "", "quantity": None, "unit_price": None, "total": None,
                                   "currency": "UAH", "evidence": "", "confidence": 0}],
                "field_findings": [], "needs_review": False, "arithmetic_warnings": [],
            }), today=date(2026, 10, 15), default_currency="UAH", model="test-model",
            detail="high", used_fallback=False,
        )
        self.assertEqual(analysis.receipt_total, Decimal("20"))
        self.assertEqual(len(analysis.receipt_items), 1)
        self.assertTrue(analysis.needs_review)
        self.assertTrue(any("malformed" in warning for warning in analysis.arithmetic_warnings))


if __name__ == "__main__":
    unittest.main()
