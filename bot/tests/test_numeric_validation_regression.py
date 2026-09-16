from __future__ import annotations

import os
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from finance import parse_decimal_amount, parse_decimal_rate  # noqa: E402


class NumericValidationRegressionTests(unittest.TestCase):
    def test_amount_rejects_non_finite_and_unstorable_values(self) -> None:
        for value in ("NaN", "sNaN", "Infinity", "-Infinity", "1e100", "10000000000000000", "9999999999999999.995"):
            for allow_negative in (False, True):
                with self.subTest(value=value, allow_negative=allow_negative):
                    try:
                        result = parse_decimal_amount(value, allow_negative=allow_negative)
                    except Exception as exc:
                        self.fail(f"Invalid input escaped validation: {type(exc).__name__}")
                    self.assertIsNone(result)

    def test_amount_preserves_zero_signed_balance_and_last_storable_cent(self) -> None:
        self.assertEqual(parse_decimal_amount("0"), Decimal("0.00"))
        self.assertEqual(parse_decimal_amount("-10,50", allow_negative=True), Decimal("-10.50"))
        self.assertIsNone(parse_decimal_amount("-10,50"))
        self.assertEqual(parse_decimal_amount("9999999999999999.99"), Decimal("9999999999999999.99"))

    def test_rate_rejects_non_finite_overflow_and_rounded_zero(self) -> None:
        for value in ("NaN", "sNaN", "Infinity", "-Infinity", "1e100", "0", "-2", "0.00001", "1e-1000"):
            with self.subTest(value=value):
                try:
                    result = parse_decimal_rate(value)
                except Exception as exc:
                    self.fail(f"Invalid rate escaped validation: {type(exc).__name__}")
                self.assertIsNone(result)

    def test_rate_preserves_small_positive_and_decimal_comma(self) -> None:
        self.assertEqual(parse_decimal_rate("0.0001"), Decimal("0.0001"))
        self.assertEqual(parse_decimal_rate("40,12345"), Decimal("40.1235"))


if __name__ == "__main__":
    unittest.main()
