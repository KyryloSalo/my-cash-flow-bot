"""Cross-owner billing integrations must not bypass durable guards."""
from __future__ import annotations
import ast
from pathlib import Path
import unittest

from billing_client import BillingAPIError, retry_renewal

BOT = Path(__file__).resolve().parents[1]


def function(tree: ast.AST, name: str):
    return next(node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)


class BillingIntegrationGuardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.main_tree = ast.parse((BOT / "bot_main.py").read_text(encoding="utf-8-sig"))
        cls.client_tree = ast.parse((BOT / "billing_client.py").read_text(encoding="utf-8-sig"))
        cls.main_source = (BOT / "bot_main.py").read_text(encoding="utf-8-sig")

    def test_recovery_reply_uses_fresh_suppression_guard_at_all_three_gates(self):
        for name in (
            "_notify_trial_recovery_owners",
            "_start_trial_recovery_owner_reply",
            "_consume_trial_recovery_owner_reply",
        ):
            names = {node.id for node in ast.walk(function(self.main_tree, name)) if isinstance(node, ast.Name)}
            self.assertIn("owner_reply_allowed", names, name)

    def test_retry_client_forwards_required_stable_intent(self):
        target = function(self.client_tree, "retry_renewal")
        keyword_names = {arg.arg for arg in target.args.kwonlyargs}
        self.assertIn("intent_key", keyword_names)
        constants = {node.value for node in ast.walk(target) if isinstance(node, ast.Constant)}
        self.assertIn("intent_key", constants)

    def test_bot_retry_has_preview_confirm_and_forwards_bound_intent(self):
        self.assertIn('settings:billing:retry:confirm:', self.main_source)
        calls = [
            node for node in ast.walk(function(self.main_tree, "settings_callback"))
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "retry_renewal"
        ]
        self.assertTrue(calls)
        self.assertTrue(all(any(keyword.arg == "intent_key" for keyword in call.keywords) for call in calls))


class BillingClientValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_rejects_empty_intent_as_billing_api_error(self):
        with self.assertRaises(BillingAPIError):
            await retry_renewal(tg_user_id=1, intent_key=" ")


if __name__ == "__main__":
    unittest.main()
