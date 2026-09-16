from __future__ import annotations

import os
import sys
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from finance import (  # noqa: E402
    calculate_transfer_amount,
    escape_html,
    format_account_line,
    format_account_name,
    format_exchange_rate,
    format_money,
    is_valid_currency_code,
    parse_decimal_amount,
    parse_decimal_rate,
)


class FinanceTests(unittest.TestCase):
    def test_format_money(self) -> None:
        self.assertEqual(format_money(1000, "UAH"), "1 000 UAH")
        self.assertEqual(format_money(Decimal("192.62"), "USD"), "192.62 USD")
        self.assertEqual(format_money(0, "TRY"), "0 TRY")

    def test_format_account_helpers(self) -> None:
        account = {"label": 'Monobank <4444>', "currency": "USD", "balance": Decimal("192.62")}
        self.assertEqual(format_account_name(account), "Monobank &lt;4444&gt; (USD)")
        self.assertEqual(
            format_account_line(account, base_currency="UAH", rate=Decimal("43.9129")),
            "Monobank &lt;4444&gt; — <b>192.62 USD</b> (≈ <b>8 458.5 UAH</b>)",
        )

    def test_format_exchange_rate(self) -> None:
        self.assertEqual(format_exchange_rate("UAH", "USD", Decimal("43.9129")), "1 USD = <b>43.9129 UAH</b>")
        self.assertEqual(format_exchange_rate("USD", "UAH", Decimal("43.9129")), "1 USD = <b>43.9129 UAH</b>")

    def test_escape_html(self) -> None:
        self.assertEqual(escape_html('<b>Monobank</b> & "card"'), "&lt;b&gt;Monobank&lt;/b&gt; &amp; \"card\"")

    def test_parse_decimal_amount_accepts_comma(self) -> None:
        self.assertEqual(parse_decimal_amount("1000,50"), Decimal("1000.50"))

    def test_parse_decimal_rate_quantizes_to_four_places(self) -> None:
        self.assertEqual(parse_decimal_rate("39,85456"), Decimal("39.8546"))

    def test_same_currency_transfer_keeps_amount(self) -> None:
        calculation = calculate_transfer_amount("UAH", "UAH", Decimal("2000"))
        self.assertEqual(calculation.target_amount, Decimal("2000.00"))
        self.assertIsNone(calculation.rate_text)

    def test_uah_to_usd_transfer_divides_by_rate(self) -> None:
        calculation = calculate_transfer_amount("UAH", "USD", Decimal("4000"), Decimal("40"))
        self.assertEqual(calculation.target_amount, Decimal("100.00"))
        self.assertEqual(calculation.rate_text, "1 USD = <b>40 UAH</b>")

    def test_usd_to_uah_transfer_multiplies_by_rate(self) -> None:
        calculation = calculate_transfer_amount("USD", "UAH", Decimal("50"), Decimal("40"))
        self.assertEqual(calculation.target_amount, Decimal("2000.00"))
        self.assertEqual(calculation.rate_text, "1 USD = <b>40 UAH</b>")

    def test_usd_to_eur_transfer_multiplies_by_rate(self) -> None:
        calculation = calculate_transfer_amount("USD", "EUR", Decimal("100"), Decimal("0.92"))
        self.assertEqual(calculation.target_amount, Decimal("92.00"))
        self.assertEqual(calculation.rate_text, "1 USD = <b>0.92 EUR</b>")

    def test_transfer_requires_positive_rate_for_cross_currency(self) -> None:
        with self.assertRaises(ValueError):
            calculate_transfer_amount("UAH", "USD", Decimal("100"))

        with self.assertRaises(ValueError):
            calculate_transfer_amount("UAH", "USD", Decimal("100"), Decimal("0"))

        with self.assertRaises(ValueError):
            calculate_transfer_amount("USD", "UAH", Decimal("100"), Decimal("-1"))

    def test_transfer_amount_rounds_to_money_precision(self) -> None:
        calculation = calculate_transfer_amount("UAH", "USD", Decimal("100"), Decimal("6"))
        self.assertEqual(calculation.target_amount, Decimal("16.67"))
        self.assertEqual(calculation.rate_text, "1 USD = <b>6 UAH</b>")

    def test_currency_code_validation_accepts_supported_and_custom_crypto_codes(self) -> None:
        self.assertTrue(is_valid_currency_code("UAH"))
        self.assertTrue(is_valid_currency_code("USDT"))
        self.assertTrue(is_valid_currency_code("BTC"))

    def test_currency_code_validation_rejects_invalid_shapes(self) -> None:
        self.assertFalse(is_valid_currency_code("US"))
        self.assertFalse(is_valid_currency_code("USDT1"))
        self.assertFalse(is_valid_currency_code("usd-coin"))


if __name__ == "__main__":
    unittest.main()
