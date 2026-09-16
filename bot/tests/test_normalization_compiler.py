from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from normalization_compiler import compile_rows_by_sheet  # noqa: E402


class NormalizationCompilerTests(unittest.TestCase):
    def test_compile_rows_tracks_routing_conflicts_and_unmapped_categories(self) -> None:
        rows = {
            "Merchants_177": [
                {
                    "merchant_norm": "Bolt",
                    "merchant_type": "taxi",
                    "default_category": "Транспорт",
                },
                {
                    "merchant_norm": "Silpo",
                    "merchant_type": "grocery",
                    "default_category": "Продукти",
                },
            ],
            "Merchant_aliases": [
                {
                    "alias_raw": "bolt",
                    "normalize_to": "Bolt",
                    "entity_type": "merchant",
                    "main_category": "Транспорт",
                    "subcategory_or_type": "taxi",
                    "priority": 95,
                },
                {
                    "alias_raw": "silpo",
                    "normalize_to": "Silpo",
                    "entity_type": "merchant",
                    "main_category": "Продукти",
                    "subcategory_or_type": "grocery",
                    "priority": 95,
                },
            ],
            "FoodService_merchants": [
                {
                    "merchant_norm": "Glovo",
                    "merchant_type": "food_delivery_app",
                    "default_category": "Кафе та ресторани",
                    "subcategory": "Доставка їжі",
                }
            ],
            "FoodService_aliases": [
                {
                    "alias_raw": "glovo",
                    "normalize_to": "Glovo",
                    "entity_type": "merchant",
                    "main_category": "Кафе та ресторани",
                    "subcategory_or_type": "Доставка їжі",
                    "priority": 98,
                }
            ],
            "Categories": [
                {
                    "keyword_or_phrase": "bolt",
                    "normalize_to_category": "Транспорт",
                    "category_group": "Транспорт",
                    "priority": 70,
                },
                {
                    "keyword_or_phrase": "coursera",
                    "normalize_to_category": "Освіта",
                    "category_group": "Освіта",
                    "priority": 70,
                },
            ],
            "Stopwords_noise": [{"noise_word": "apple pay"}],
            "Conflicts": [{"alias_raw": "bolt"}],
            "Regex_rules": [
                {
                    "regex_pattern": r"(?i)\bкомуналка\b",
                    "normalize_to": "Комунальні платежі",
                    "entity_type": "category",
                    "main_category_or_subcategory": "Комунальні платежі",
                }
            ],
        }

        runtime, report = compile_rows_by_sheet(rows, source_path="test.xlsx")

        exact_aliases = {item["alias"]: item for item in runtime["exact_aliases"]}
        category_keywords = {item["alias"]: item for item in runtime["category_keywords"]}
        unmapped = {item["category"]: item for item in report["unmapped_categories"]}

        self.assertIn("bolt", exact_aliases)
        self.assertNotIn("bolt", category_keywords)
        self.assertIn("glovo", runtime["routing"]["carrier_aliases"])
        self.assertIn("apple pay", runtime["stopwords"])
        self.assertEqual(runtime["regex_rules"][0]["matched_slug"], "utilities")
        self.assertIn("Освіта", unmapped)

    def test_compile_rows_prefers_higher_priority_duplicate_alias(self) -> None:
        rows = {
            "Subscriptions": [
                {
                    "service_norm": "ChatGPT",
                    "default_category": "Підписки та сервіси",
                    "service_type": "ai",
                }
            ],
            "Subscription_aliases": [
                {
                    "alias_raw": "chatgpt",
                    "normalize_to": "ChatGPT",
                    "entity_type": "subscription",
                    "main_category": "Підписки та сервіси",
                    "subcategory_or_type": "ai",
                    "priority": 80,
                },
                {
                    "alias_raw": "chatgpt",
                    "normalize_to": "ChatGPT",
                    "entity_type": "subscription",
                    "main_category": "Підписки та сервіси",
                    "subcategory_or_type": "ai",
                    "priority": 98,
                },
            ],
        }

        runtime, report = compile_rows_by_sheet(rows, source_path="test.xlsx")

        entry = next(item for item in runtime["exact_aliases"] if item["alias"] == "chatgpt")
        self.assertEqual(entry["priority"], 98)
        self.assertTrue(report["duplicate_alias_resolutions"])

    def test_compile_rows_maps_safe_rollups_into_existing_top_level_slugs(self) -> None:
        rows = {
            "Categories": [
                {
                    "keyword_or_phrase": "donation",
                    "normalize_to_category": "Благодійність",
                    "category_group": "Благодійність",
                    "priority": 70,
                },
                {
                    "keyword_or_phrase": "gift",
                    "normalize_to_category": "Подарунки",
                    "category_group": "Подарунки",
                    "priority": 70,
                },
                {
                    "keyword_or_phrase": "cinema",
                    "normalize_to_category": "Кіно та розваги",
                    "category_group": "Кіно та розваги",
                    "priority": 70,
                },
                {
                    "keyword_or_phrase": "misc",
                    "normalize_to_category": "Інше",
                    "category_group": "Інше",
                    "priority": 70,
                },
            ],
            "Regex_rules": [
                {
                    "regex_pattern": r"(?i)\bpizza\b",
                    "normalize_to": "Піца",
                    "entity_type": "category",
                    "main_category_or_subcategory": "Піца",
                },
                {
                    "regex_pattern": r"(?i)\bglovo\s+market\b",
                    "normalize_to": "Доставка продуктів",
                    "entity_type": "category",
                    "main_category_or_subcategory": "Доставка продуктів",
                },
            ],
        }

        runtime, report = compile_rows_by_sheet(rows, source_path="test.xlsx")

        category_keywords = {item["alias"]: item for item in runtime["category_keywords"]}
        regex_rules = {item["pattern"]: item for item in runtime["regex_rules"]}
        unmapped_names = {item["category"] for item in report["unmapped_categories"]}

        self.assertEqual(category_keywords["donation"]["matched_slug"], "gifts_donations")
        self.assertEqual(category_keywords["gift"]["matched_slug"], "gifts_donations")
        self.assertEqual(category_keywords["cinema"]["matched_slug"], "entertainment_leisure")
        self.assertEqual(category_keywords["misc"]["matched_slug"], "other")
        self.assertEqual(regex_rules[r"(?i)\bpizza\b"]["matched_slug"], "cafes_restaurants_delivery")
        self.assertEqual(regex_rules[r"(?i)\bglovo\s+market\b"]["matched_slug"], "groceries")
        self.assertTrue(
            {"Благодійність", "Подарунки", "Кіно та розваги", "Інше", "Піца", "Доставка продуктів"}.isdisjoint(
                unmapped_names
            )
        )


if __name__ == "__main__":
    unittest.main()
