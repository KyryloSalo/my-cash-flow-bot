from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import date  # noqa: E402

from parsing import build_miniapp_text_parse_payload  # noqa: E402


class MiniAppTextParseContractTests(unittest.TestCase):
    def test_text_parse_uses_canonical_bot_parser_contract(self) -> None:
        payload = build_miniapp_text_parse_payload(text="Таксі 250 грн", today=date(2026, 7, 14), default_currency="USD")

        self.assertTrue(payload["is_candidate_tx"])
        self.assertEqual(payload["intent"], "expense")
        self.assertEqual(payload["amount"], "250.00")
        self.assertEqual(payload["currency"], "UAH")
        self.assertTrue(payload["currency_explicit"])
        self.assertEqual(payload["category_slug"], "taxi")
        self.assertIn("account_id", payload["missing_fields"])

    def test_text_parse_rejects_empty_and_oversized_input(self) -> None:
        with self.assertRaises(ValueError):
            build_miniapp_text_parse_payload(text="", today=date(2026, 7, 14), default_currency="UAH")

        with self.assertRaises(ValueError):
            build_miniapp_text_parse_payload(text="x" * 501, today=date(2026, 7, 14), default_currency="UAH")

    def test_text_parse_keeps_explicit_transfer_out_of_normal_transactions(self) -> None:
        payload = build_miniapp_text_parse_payload(
            text="Переказати 10 грн з ПриватБанк •3882 на ПриватБанк •4956",
            today=date(2026, 7, 14),
            default_currency="UAH",
        )

        self.assertTrue(payload["is_candidate_tx"])
        self.assertEqual(payload["intent"], "transfer")
        self.assertIsNone(payload["category_slug"])
