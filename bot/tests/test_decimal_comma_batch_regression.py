from __future__ import annotations

from datetime import date
from decimal import Decimal
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from parsing import parse_message, parse_message_batch, split_message_into_tx_segments  # noqa: E402


class DecimalCommaBatchRegressionTests(unittest.TestCase):
    today = date(2026, 9, 14)

    def test_decimal_comma_is_one_operation_not_a_batch(self) -> None:
        text = "кава 10,50"
        self.assertEqual(split_message_into_tx_segments(text), [text])
        self.assertEqual(parse_message_batch(text, today=self.today), [])
        self.assertEqual(Decimal(str(parse_message(text, today=self.today).amount)), Decimal("10.50"))

    def test_batch_preserves_decimal_commas_in_each_amount(self) -> None:
        for separator in (", ", "; ", "\n"):
            with self.subTest(separator=separator):
                text = "кава 10,50" + separator + "таксі 20,75"
                items = parse_message_batch(text, today=self.today)
                self.assertEqual([Decimal(str(item.amount)) for item in items], [Decimal("10.50"), Decimal("20.75")])
                self.assertEqual(sum(Decimal(str(item.amount)) for item in items), Decimal("31.25"))

    def test_grouped_amount_and_period_decimal_do_not_change_separators(self) -> None:
        self.assertEqual(split_message_into_tx_segments("кава 1 234,50; таксі 20.75"), ["кава 1 234,50", "таксі 20.75"])
        self.assertEqual(split_message_into_tx_segments("кава 10,,, таксі 20"), ["кава 10", "таксі 20"])


if __name__ == "__main__":
    unittest.main()
