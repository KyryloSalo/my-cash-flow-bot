"""The bot delegates core DDL to the shared, versioned schema bootstrap."""
from __future__ import annotations
import ast
from pathlib import Path
import unittest


class BotSchemaIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.path = Path(__file__).resolve().parents[1] / "bot_main.py"
        cls.tree = ast.parse(cls.path.read_text(encoding="utf-8-sig"))
        cls.node = next(n for n in cls.tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "init_db")

    def test_init_uses_shared_bootstrap_before_runtime_repairs(self):
        calls = [
            n for n in ast.walk(self.node)
            if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
        ]
        names = []
        for item in calls:
            fn = item.value.func
            if isinstance(fn, ast.Name):
                names.append(fn.id)
            elif isinstance(fn, ast.Attribute):
                names.append(fn.attr)
        self.assertIn("bootstrap_async", names)
        self.assertIn("repair_legacy_debts", names)
        self.assertIn("_recalculate_account_balances", names)
        self.assertIn("_seed_category_templates", names)
        self.assertLess(names.index("bootstrap_async"), names.index("repair_legacy_debts"))

    def test_init_contains_no_literal_schema_ddl(self):
        forbidden = ("CREATE TABLE", "ALTER TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX")
        literals = [n.value.upper() for n in ast.walk(self.node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        offenders = [value for value in literals if any(token in value for token in forbidden)]
        self.assertEqual(offenders, [])

    def test_runtime_schema_import_is_present(self):
        imports = [
            n for n in ast.walk(self.tree)
            if isinstance(n, ast.ImportFrom) and n.module == "runtime_schema"
        ]
        self.assertTrue(any(any(alias.name == "bootstrap_async" for alias in item.names) for item in imports))


if __name__ == "__main__":
    unittest.main()
