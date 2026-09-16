import unittest

from runtime_schema import CORE_STATEMENTS


class CategoryScopeSchemaTests(unittest.TestCase):
    def test_category_uniqueness_is_scoped_to_personal_or_family_finance(self):
        sql = "\n".join(CORE_STATEMENTS)
        self.assertIn("DROP INDEX IF EXISTS idx_categories_unique", sql)
        self.assertIn("ux_categories_personal_active", sql)
        self.assertIn("WHERE family_id IS NULL AND is_active", sql)
        self.assertIn("ux_categories_family_active", sql)
        self.assertIn("WHERE family_id IS NOT NULL AND is_active", sql)


if __name__ == "__main__":
    unittest.main()
