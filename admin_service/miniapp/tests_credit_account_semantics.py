"""Credit balances are signed net amounts; available input is opt-in."""
from types import SimpleNamespace
from unittest.mock import patch
from django.test import SimpleTestCase
from miniapp import services

class CreditAccountSemanticsTests(SimpleTestCase):
    def draft(self, **values):
        payload = dict(action="create", label="Credit", currency="UAH", account_type="credit", starting_balance="100000", credit_limit="300000", balance_input_mode="available_credit")
        payload.update(values)
        with patch.object(services, "_ensure_unique_account_label"):
            return services.build_account_draft(SimpleNamespace(lang="en", base_currency="UAH"), payload, draft_id="credit-test")

    def test_available_100000_of_limit_300000_is_net_debt_200000(self):
        self.assertEqual(self.draft()["account"]["starting_balance"], "-200000.00")

    def test_legacy_and_explicit_net_values_are_not_reinterpreted(self):
        for mode in (None, "net"):
            for value in ("100000", "-200000", "0"):
                with self.subTest(mode=mode, value=value):
                    self.assertEqual(self.draft(balance_input_mode=mode, starting_balance=value)["account"]["starting_balance"], f"{__import__('decimal').Decimal(value):.2f}")

    def test_available_boundaries_and_validation(self):
        for available, net in (("0", "-300000.00"), ("300000", "0.00"), ("310000", "10000.00")):
            self.assertEqual(self.draft(starting_balance=available)["account"]["starting_balance"], net)
        for values in ({"credit_limit": "0"}, {"starting_balance": "-1"}, {"account_type": "cash"}, {"balance_input_mode": "typo"}):
            with self.subTest(values=values), self.assertRaises(services.MiniAppTransactionError):
                self.draft(**values)

    def test_interest_forecast_uses_debt_not_limit_and_preserves_rate_precision(self):
        from decimal import Decimal
        account = SimpleNamespace(id=1, label="Credit", currency="UAH", balance=Decimal("-200000"), credit_limit=Decimal("300000"), monthly_interest_rate=Decimal("3.5123"), account_type="credit")
        with patch.object(services, "_scoped_accounts_qs") as scope:
            scope.return_value.filter.return_value.filter.return_value.order_by.return_value.__getitem__.return_value = [account]
            item = services.build_credit_cards_preview(SimpleNamespace(base_currency="UAH", lang="en"))["items"][0]
        self.assertEqual(item["monthly_interest_rate"], "3.5123")
        self.assertEqual(item["monthly_interest_forecast"]["value"], "7024.60")
        for balance in ("0", "100000"):
            account.balance = Decimal(balance)
            with patch.object(services, "_scoped_accounts_qs") as scope:
                scope.return_value.filter.return_value.filter.return_value.order_by.return_value.__getitem__.return_value = [account]
                item = services.build_credit_cards_preview(SimpleNamespace(base_currency="UAH", lang="en"))["items"][0]
            self.assertEqual(item["monthly_interest_forecast"]["value"], "0.00")

    def test_correction_uses_available_input_only_when_explicit(self):
        account = SimpleNamespace(id=7, account_type="credit", credit_limit=__import__('decimal').Decimal("300000"), balance=__import__('decimal').Decimal("100000"), currency="UAH", label="Legacy credit")
        with patch.object(services, "_active_scoped_account", return_value=account), patch.object(services, "_account_payload", return_value={}):
            draft = services.build_account_draft(SimpleNamespace(lang="en"), dict(action="balance_correction", account_id=7, target_balance="100000", balance_input_mode="available_credit"), draft_id="correction")
        self.assertEqual(draft["target_balance"], "-200000.00")
        self.assertEqual(draft["delta"]["value"], "-300000.00")

