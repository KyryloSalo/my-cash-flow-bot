from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from parsing import build_miniapp_text_parse_payload, parse_amount, parse_message


TODAY = date(2026, 10, 14)


def test_voice_kopecks_are_decimal_not_additive_words() -> None:
    # Parser normalization regression; this does not exercise speech-to-text/model OCR.
    assert parse_amount("шесть тысяч семьсот семьдесят пять гривен тридцать пять копеек") == 6775.35


def test_invalid_kopecks_fail_closed() -> None:
    assert parse_amount("сто гривен сто копеек") is None


def test_numeric_kopecks_are_decimal_not_dropped() -> None:
    assert parse_amount("6 775 гривен 35 копеек") == 6775.35


def test_voice_category_aliases_cover_phone_headphones_and_household_chemicals() -> None:
    assert parse_message("телефону 150 грн", today=TODAY).category_id == "telecom_internet"
    assert parse_message("наушники 227 гривен", today=TODAY).category_id == "other"
    assert parse_message("побутовая химия 30 евро", today=TODAY).category_id == "home_household"


def test_decimal_comma_is_not_a_multi_operation() -> None:
    parsed = build_miniapp_text_parse_payload(
        text="Кава 12,50 грн",
        today=TODAY,
        default_currency="UAH",
    )
    assert parsed["amount"] == "12.50"


def test_colloquial_compound_amount_requires_clarification() -> None:
    parsed = parse_message("Коммуналка семь триста", today=TODAY, default_currency="UAH")
    assert parsed.amount is None
    assert "amount" in parsed.missing_fields


def test_second_colloquial_compound_amount_requires_clarification() -> None:
    parsed = parse_message("Аренда квартиры двенадцать пятьсот", today=TODAY, default_currency="UAH")
    assert parsed.amount is None
    assert "amount" in parsed.missing_fields


def test_curly_apostrophe_compound_amount_requires_clarification() -> None:
    parsed = parse_message("Аренда квартиры дванадцять п’ятсот", today=TODAY, default_currency="UAH")
    assert parsed.amount is None
    assert "amount" in parsed.missing_fields


def test_multi_operation_voice_is_rejected_as_one_draft() -> None:
    try:
        build_miniapp_text_parse_payload(
            text="Нож 150 гривен, такси 430 гривен",
            today=TODAY,
            default_currency="UAH",
        )
    except ValueError as exc:
        assert "окремо" in str(exc).lower() or "separately" in str(exc).lower()
    else:
        raise AssertionError("multi-operation input must not become one draft")


def test_explicit_currency_amounts_remain_exact() -> None:
    for text, expected in (("9216,67 грн", Decimal("9216.67")), ("3299,95 грн", Decimal("3299.95")), ("9444,92 грн", Decimal("9444.92"))):
        parsed = parse_message(text, today=TODAY, default_currency="EUR")
        assert Decimal(str(parsed.amount)).quantize(Decimal("0.01")) == expected
        assert parsed.currency == "UAH"
