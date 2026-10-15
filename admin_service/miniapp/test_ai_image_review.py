from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from .services import build_ai_image_batch, build_ai_image_suggestion


class AiImageReviewTests(SimpleTestCase):
    def test_single_suggestion_preserves_optional_runtime_review_metadata(self) -> None:
        account = SimpleNamespace(id=7)
        category = SimpleNamespace(id=8)
        with patch("miniapp.services._pick_ai_image_account", return_value=account), patch(
            "miniapp.services._pick_ai_image_category", return_value=category
        ):
            suggestion = build_ai_image_suggestion(
                SimpleNamespace(base_currency="UAH"),
                {
                    "mode": "single_tx",
                    "type": "expense",
                    "amount": "12.50",
                    "currency": "UAH",
                    "transaction_date": "2026-10-15",
                    "field_findings": [{"field": "amount", "message": "blurry"}],
                    "needs_review": ["amount"],
                    "arithmetic_warnings": ["total does not match items"],
                },
            )
        self.assertEqual(suggestion["field_findings"][0]["field"], "amount")
        self.assertEqual(suggestion["needs_review"], ["amount"])
        self.assertEqual(suggestion["arithmetic_warnings"], ["total does not match items"])

    def test_batch_keeps_review_metadata_on_editable_items(self) -> None:
        with patch("miniapp.services._pick_ai_image_account", return_value=SimpleNamespace(id=7)), patch(
            "miniapp.services._pick_ai_image_category", return_value=SimpleNamespace(id=8)
        ):
            batch = build_ai_image_batch(
                SimpleNamespace(base_currency="UAH"),
                {
                    "mode": "statement_expenses",
                    "currency": "UAH",
                    "statement_items": [{"amount": "12.50", "transaction_date": "2026-10-15"}],
                    "source": "statement_transactions",
                    "needs_review": ["amount"],
                    "arithmetic_warnings": ["sum mismatch"],
                },
                batch_id="batch-1",
            )
        item = batch["items"][0]["suggestion"]
        self.assertEqual(item["needs_review"], ["amount"])
        self.assertEqual(item["arithmetic_warnings"], ["sum mismatch"])

    def test_template_has_local_original_preview_and_explicit_review_surface(self) -> None:
        template = Path(__file__).with_name("templates") / "miniapp" / "index.html"
        content = template.read_text(encoding="utf-8")
        self.assertIn('id="aiImagePreview"', content)
        self.assertIn("renderAiImageFindings", content)
        self.assertIn("transaction-confirm", content)

    def test_receipt_items_and_total_stay_on_one_transaction(self) -> None:
        with patch("miniapp.services._pick_ai_image_account", return_value=SimpleNamespace(id=7)), patch(
            "miniapp.services._pick_ai_image_category", return_value=SimpleNamespace(id=8)
        ):
            suggestion = build_ai_image_suggestion(
                SimpleNamespace(base_currency="UAH"),
                {"mode": "single_tx", "type": "expense", "amount": "12.50", "receipt_items": [{"name": "Coffee", "amount": "12.50"}], "receipt_total": "12.50", "source": "receipt"},
            )
        self.assertEqual(suggestion["receipt_total"], "12.50")
        self.assertEqual(suggestion["receipt_items"][0]["name"], "Coffee")
        self.assertEqual(suggestion["source"], "receipt")
        self.assertNotEqual(suggestion["mode"], "batch")

    def test_malformed_receipt_l2_falls_back_to_single_transaction(self) -> None:
        with patch("miniapp.services._pick_ai_image_account", return_value=SimpleNamespace(id=7)), patch(
            "miniapp.services._pick_ai_image_category", return_value=SimpleNamespace(id=8)
        ):
            suggestion = build_ai_image_suggestion(
                SimpleNamespace(base_currency="UAH"),
                {"mode": "single_tx", "type": "expense", "amount": "12.50", "receipt_items": ["malformed"], "receipt_total": "12.50", "needs_review": True},
            )
        self.assertEqual(suggestion["mode"], "transaction")
        self.assertEqual(suggestion["amount"], "12.50")
        self.assertTrue(suggestion["needs_review_bool"])

    def test_receipt_items_are_not_used_for_statement_batch(self) -> None:
        batch = build_ai_image_batch(
            SimpleNamespace(base_currency="UAH"),
            {"mode": "statement_expenses", "source": "statement_transactions", "statement_items": [{"amount": "12.50"}], "receipt_items": [{"name": "Coffee", "amount": "12.50"}]},
            batch_id="batch-1",
        )
        self.assertEqual(batch["items"], [])

    def test_statement_mode_without_items_falls_back_to_single_transaction(self) -> None:
        with patch("miniapp.services._pick_ai_image_account", return_value=SimpleNamespace(id=7)), patch(
            "miniapp.services._pick_ai_image_category", return_value=SimpleNamespace(id=8)
        ):
            suggestion = build_ai_image_suggestion(
                SimpleNamespace(base_currency="UAH"),
                {"mode": "statement_expenses", "type": "expense", "amount": "12.50", "currency": "UAH"},
            )
        self.assertEqual(suggestion["mode"], "transaction")
        self.assertEqual(suggestion["amount"], "12.50")

    def test_template_contains_editable_l2_controls_and_blob_cleanup(self) -> None:
        template = Path(__file__).with_name("templates") / "miniapp" / "index.html"
        content = template.read_text(encoding="utf-8")
        self.assertIn("ai-image-batch-name", content)
        self.assertIn("ai-image-batch-amount", content)
        self.assertIn("revokeObjectURL", content)
        self.assertIn("receipt_total", content)
