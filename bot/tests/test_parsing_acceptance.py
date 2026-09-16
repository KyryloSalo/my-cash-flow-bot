from __future__ import annotations

import unittest
from datetime import date

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from parsing import parse_message, parse_message_batch  # noqa: E402


TODAY = date(2026, 4, 30)


class ParsingAcceptanceTests(unittest.TestCase):
    def test_expense_no_amount(self) -> None:
        msg = parse_message("Купив хліб", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertIsNone(msg.amount)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "groceries")
        self.assertEqual(msg.comment, "Купив хліб")

    def test_expense_with_amount(self) -> None:
        msg = parse_message("Хліб 200", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 200.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "groceries")
        self.assertEqual(msg.comment, "хліб")

    def test_signed_negative_shorthand_is_treated_as_expense(self) -> None:
        msg = parse_message("- 150 \u0433\u0440\u043d - \u043f\u0440\u043e\u0434\u0443\u043a\u0442\u0438", today=TODAY, default_currency="USD")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 150.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.category_id, "groceries")
        self.assertEqual(msg.comment, "\u043f\u0440\u043e\u0434\u0443\u043a\u0442\u0438")

    def test_signed_positive_shorthand_is_treated_as_income(self) -> None:
        msg = parse_message("+500 \u0437\u0430\u0440\u043f\u043b\u0430\u0442\u0430", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 500.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "salary")
        self.assertEqual(msg.comment, "\u0437\u0430\u0440\u043f\u043b\u0430\u0442\u0430")

    def test_expense_with_amount_and_account(self) -> None:
        msg = parse_message("Кава 80 моно", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 80.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.account_id, "monobank")
        self.assertEqual(msg.category_id, "cafes_restaurants_delivery")
        self.assertEqual(msg.comment, "кава")

    def test_taxi(self) -> None:
        msg = parse_message("Таксі 250", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 250.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "taxi")
        self.assertEqual(msg.comment, "таксі")

    def test_internet_payment(self) -> None:
        msg = parse_message("Оплатив інтернет 300", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 300.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "telecom_internet")
        self.assertEqual(msg.comment, "Оплатив інтернет")

    def test_income_salary(self) -> None:
        msg = parse_message("Прийшла зарплата 40000", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 40000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "salary")
        self.assertEqual(msg.comment, "Прийшла зарплата")

    def test_income_salary_voice_ru_words_and_account_inflection(self) -> None:
        msg = parse_message(
            "Прийшла зарплата пять тисяч гривен на карту Монобанка.",
            today=TODAY,
            default_currency="UAH",
        )
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 5000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.account_id, "monobank")
        self.assertEqual(msg.category_id, "salary")

    def test_income_salary_voice_ru_words_euro(self) -> None:
        msg = parse_message(
            "Пришла зарплата десять евро",
            today=TODAY,
            default_currency="UAH",
        )
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 10.0)
        self.assertEqual(msg.currency, "EUR")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.category_id, "salary")

    def test_income_client_voice_ru_words_dollars(self) -> None:
        msg = parse_message(
            "Клиент оплатил пятьдесят долларов",
            today=TODAY,
            default_currency="UAH",
        )
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 50.0)
        self.assertEqual(msg.currency, "USD")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.category_id, "client_payment")

    def test_income_client_usd(self) -> None:
        msg = parse_message("Клієнт оплатив 500 доларів", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 500.0)
        self.assertEqual(msg.currency, "USD")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.category_id, "client_payment")

    def test_income_salary_usdt(self) -> None:
        msg = parse_message("Зайшло 500 USDT зарплата", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 500.0)
        self.assertEqual(msg.currency, "USDT")
        self.assertTrue(msg.currency_explicit)

    def test_expense_try_word_form_lir_maps_to_try(self) -> None:
        msg = parse_message("10 лір молоко", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 10.0)
        self.assertEqual(msg.currency, "TRY")
        self.assertTrue(msg.currency_explicit)
        self.assertEqual(msg.comment, "молоко")

    def test_implicit_currency_uses_default_without_explicit_flag(self) -> None:
        msg = parse_message("Зарплата 500", today=TODAY, default_currency="USDT")
        self.assertEqual(msg.intent, "income")
        self.assertEqual(msg.amount, 500.0)
        self.assertEqual(msg.currency, "USDT")
        self.assertFalse(msg.currency_explicit)

    def test_transfer_full(self) -> None:
        msg = parse_message("Перекинув 5000 з моно на приват", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "transfer")
        self.assertEqual(msg.amount, 5000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.from_account_id, "monobank")
        self.assertEqual(msg.to_account_id, "privatbank")

    def test_transfer_infinitive_is_not_classified_as_expense(self) -> None:
        msg = parse_message(
            "Переказати 10 грн з ПриватБанк •3882 на ПриватБанк •4956",
            today=TODAY,
            default_currency="UAH",
        )

        self.assertEqual(msg.intent, "transfer")
        self.assertTrue(msg.is_candidate_tx)
        self.assertIsNone(msg.category_id)

    def test_debt_lend(self) -> None:
        msg = parse_message("Дав Саші 1000 в борг", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "lend")
        self.assertEqual(msg.amount, 1000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.person_id, "Саша")
        self.assertNotIn("debt_action", msg.missing_fields)

    def test_debt_borrow(self) -> None:
        msg = parse_message("Взяв у Саші 5000 в борг", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "borrow")
        self.assertEqual(msg.amount, 5000.0)
        self.assertEqual(msg.person_id, "Саша")
        self.assertNotIn("debt_action", msg.missing_fields)

    def test_debt_repayment_receive(self) -> None:
        msg = parse_message("Саша повернув 1000", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "receive_repayment")
        self.assertEqual(msg.amount, 1000.0)
        self.assertEqual(msg.person_id, "Саша")

    def test_debt_repayment_make(self) -> None:
        msg = parse_message("Повернув Саші 2000", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "make_repayment")
        self.assertEqual(msg.amount, 2000.0)
        self.assertEqual(msg.person_id, "Саша")

    def test_yesterday_products(self) -> None:
        msg = parse_message("Вчора продукти 850", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.amount, 850.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.category_id, "groceries")
        self.assertEqual(msg.date, date(2026, 4, 29))

    def test_debt_lend_plain_text(self) -> None:
        msg = parse_message("Дав Саші 1000 грн у борг", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "lend")
        self.assertEqual(msg.amount, 1000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.person_id, "Саша")

    def test_debt_borrow_plain_text(self) -> None:
        msg = parse_message("Позичив у Іри 5000 грн", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "borrow")
        self.assertEqual(msg.amount, 5000.0)
        self.assertEqual(msg.currency, "UAH")
        self.assertEqual(msg.person_id, "Іри")

    def test_debt_receive_repayment_plain_text(self) -> None:
        msg = parse_message("Саша повернув 400 грн", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "receive_repayment")
        self.assertEqual(msg.amount, 400.0)
        self.assertEqual(msg.person_id, "Саша")

    def test_debt_make_repayment_plain_text(self) -> None:
        msg = parse_message("Повернув Ірі 2000 грн", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "debt")
        self.assertEqual(msg.debt_action, "make_repayment")
        self.assertEqual(msg.amount, 2000.0)
        self.assertEqual(msg.person_id, "Іра")


    def test_batch_expense_message_splits_into_multiple_items(self) -> None:
        items = parse_message_batch(
            "Двісті гривень таксі, сто гривень кава, п'ятсот гривень продукти в АТБ",
            today=TODAY,
            default_currency="USD",
        )
        self.assertEqual(len(items), 3)
        self.assertEqual([item.intent for item in items], ["expense", "expense", "expense"])
        self.assertEqual([item.amount for item in items], [200.0, 100.0, 500.0])
        self.assertEqual([item.currency for item in items], ["UAH", "UAH", "UAH"])
        self.assertTrue(all(item.currency_explicit for item in items))
        self.assertEqual([item.category_id for item in items], ["taxi", "cafes_restaurants_delivery", "groceries"])

    def test_batch_expense_message_with_russian_griven_keeps_explicit_uah(self) -> None:
        items = parse_message_batch(
            "Две тысячи гривен оплата врачу, 1628 гривен витамины и аптека, 856 гривен продукты, 78 гривен продукты",
            today=TODAY,
            default_currency="USD",
        )
        self.assertEqual(len(items), 4)
        self.assertEqual([item.currency for item in items], ["UAH", "UAH", "UAH", "UAH"])
        self.assertTrue(all(item.currency_explicit for item in items))


    def test_rent_phrase_maps_to_rent(self) -> None:
        msg = parse_message("оренда квартири 15000", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "rent")

    def test_utilities_phrase_maps_to_utilities(self) -> None:
        msg = parse_message("оплата світла 900", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "utilities")

    def test_kommunalka_phrase_maps_to_utilities(self) -> None:
        msg = parse_message("комуналка 100 грн", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "utilities")

    def test_turkcell_maps_to_telecom(self) -> None:
        msg = parse_message("Turkcell 450", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "telecom_internet")

    def test_netflix_maps_to_subscriptions(self) -> None:
        msg = parse_message("Netflix 12 usd", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "subscriptions_services")

    def test_apteka_maps_to_health(self) -> None:
        msg = parse_message("аптека 380", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "health")

    def test_bolt_taxi_maps_to_taxi(self) -> None:
        msg = parse_message("Bolt taxi 220", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "taxi")

    def test_glovo_silpo_routes_to_groceries(self) -> None:
        msg = parse_message("Glovo Silpo 450", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "groceries")

    def test_glovo_sushi_routes_to_cafes(self) -> None:
        msg = parse_message("Glovo sushi 390", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "cafes_restaurants_delivery")

    def test_glovo_apteka_routes_to_health(self) -> None:
        msg = parse_message("Glovo apteka 280", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "health")

    def test_apple_merchant_routes_to_subscriptions(self) -> None:
        msg = parse_message("Apple 12 usd", today=TODAY, default_currency="UAH")
        self.assertEqual(msg.intent, "expense")
        self.assertEqual(msg.category_id, "subscriptions_services")


if __name__ == "__main__":
    unittest.main()
