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

    def test_review_only_rows_survive_batch_and_payload(self) -> None:
        batch = build_ai_image_batch(SimpleNamespace(base_currency="UAH"), {"source":"statement_transactions", "statement_review_rows":[{"row_index":0,"reason":"missing_status_evidence","amount":"12.50"}]}, batch_id="batch-1")
        self.assertEqual(len(batch["review_rows"]), 1)

    def test_mixed_batch_keeps_items_and_review_rows(self) -> None:
        with patch("miniapp.services._pick_ai_image_account", return_value=SimpleNamespace(id=7)), patch(
            "miniapp.services._pick_ai_image_category", return_value=SimpleNamespace(id=8)
        ):
            batch = build_ai_image_batch(SimpleNamespace(base_currency="UAH"), {"source":"statement_transactions", "statement_items":[{"amount":"12.50","currency":"UAH"}], "statement_review_rows":[{"row_index":1,"reason":"ambiguous"}]}, batch_id="batch-1")
        self.assertEqual(len(batch["review_rows"]), 1)
        self.assertEqual(len(batch["items"]), 1)

    def test_cross_pass_review_rows_are_deduplicated(self) -> None:
        review = {"row_index":0,"reason":"missing_status_evidence","transaction_date":"2026-10-01","amount":"12.50","currency":"UAH","comment":"Shop"}
        batch = build_ai_image_batch(SimpleNamespace(base_currency="UAH"), {"source":"statement_transactions", "statement_review_rows":[review]}, batch_id="batch-1")
        self.assertEqual(batch["review_rows"][0]["row_index"], 0)

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

    def test_receipt_review_canonicalizes_recognized_item_fields_and_clears_on_terminal_paths(self) -> None:
        template = Path(__file__).with_name("templates") / "miniapp" / "index.html"
        content = template.read_text(encoding="utf-8")
        self.assertIn("function canonicalReceiptItem(item)", content)
        self.assertIn("source.name != null ? source.name : source.description", content)
        self.assertIn("source.amount != null ? source.amount : (source.total != null ? source.total : source.line_total)", content)
        self.assertIn('item.name = input.value', content)
        self.assertIn('item[field[0]] = control.value', content)
        self.assertIn("function clearReceiptItems()", content)
        self.assertGreaterEqual(content.count("clearReceiptItems();"), 3)
        self.assertIn("transactionState.receiptItems = Array.isArray(suggestion.receipt_items) ? suggestion.receipt_items.map(canonicalReceiptItem) : [];", content)
        confirm_block = content[content.index("function confirmTransactionDraft"):content.index("function cancelTransactionDraft")]
        self.assertIn("clearReceiptItems();", confirm_block)
        self.assertNotIn("clearReceiptItems();", confirm_block[confirm_block.index(".catch(function (errorPayload)"):])
        self.assertIn("ai-image-batch-name", content)
        self.assertIn("ai-image-batch-amount", content)
        self.assertIn("revokeObjectURL", content)
        self.assertIn("receipt_total", content)
