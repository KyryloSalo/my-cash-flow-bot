from __future__ import annotations

from django.test import SimpleTestCase

from categories.default_catalog import (
    DEFAULT_EXPENSE_CATEGORIES,
    DEFAULT_EXPENSE_FALLBACK_SLUG,
    DEFAULT_EXPENSE_CATEGORY_SLUGS,
    get_default_expense_categories,
)


class DefaultExpenseCatalogTests(SimpleTestCase):
    def test_catalog_contains_required_fallback_slug(self) -> None:
        self.assertIn(DEFAULT_EXPENSE_FALLBACK_SLUG, DEFAULT_EXPENSE_CATEGORY_SLUGS)
        self.assertEqual(DEFAULT_EXPENSE_CATEGORIES[0]["slug"], DEFAULT_EXPENSE_FALLBACK_SLUG)

    def test_get_default_expense_categories_returns_deep_copy(self) -> None:
        categories = get_default_expense_categories()

        categories[0]["name"] = "mutated"

        self.assertNotEqual(categories[0]["name"], DEFAULT_EXPENSE_CATEGORIES[0]["name"])
