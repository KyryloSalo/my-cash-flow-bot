"""Run actual entrypoints: stale financial callbacks stop before DB access."""
from __future__ import annotations
import ast
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confirmation_guard import FINANCIAL_CONFIRMATION_STALE_TEXT, validated_confirmation_action


class SiblingConfirmationRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_ai_batch_completed_draft_has_explicit_replay_message(self):
        source = (Path(__file__).resolve().parents[1] / "bot_main.py").read_text(encoding="utf-8-sig")
        start = source.index("async def _handle_ai_batch_callback")
        end = source.index("\n\nasync def ai_callback", start)
        handler = source[start:end]
        self.assertIn('str(exc) == "draft_not_pending"', handler)
        self.assertIn("вже завершено", handler)

    async def test_account_menu_never_reparses_bot_card_as_new_user_transaction(self):
        source = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        node = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "home_callback")
        ai_start = AsyncMock(return_value=True)
        namespace = {
            "_consume_pending_admin_reset_if_needed": AsyncMock(),
            "_start_ai_tx_flow": ai_start, "parse_amount": lambda text: 1000,
            "_pool": Mock(side_effect=RuntimeError("account_route_reached")),
        }
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), node], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
        query = SimpleNamespace(data="accounts:balance:1", answer=AsyncMock(), message=SimpleNamespace(text="Баланс 1000 UAH"))
        update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=42))
        with self.assertRaisesRegex(RuntimeError, "account_route_reached"):
            await namespace["home_callback"](update, SimpleNamespace(user_data={}))
        ai_start.assert_not_awaited()

    async def test_unsigned_sibling_confirmations_are_rejected_before_any_io(self):
        source = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        nodes = {node.name: node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)}
        for handler, key, action in (
            ("saving_callback", "saving_flow", "saving:topup:confirm:ok"),
            ("saving_callback", "saving_flow", "saving:confirm:ok"),
            ("saving_callback", "saving_flow", "saving:task:confirm:ok:1"),
            ("debt_callback", "debt_flow", "debt:confirm"),
            ("ai_callback", "ai_tx_flow", "ai:tx:ok"),
            ("ai_callback", "ai_batch_tx_flow", "ai:batch:ok"),
        ):
            with self.subTest(handler=handler, action=action):
                flow = {"amount": "500", "mode": "create"}
                context = SimpleNamespace(user_data={key: deepcopy(flow)})
                query = SimpleNamespace(data=action, answer=AsyncMock(), message=SimpleNamespace(reply_text=AsyncMock()))
                update = SimpleNamespace(callback_query=query, effective_user=SimpleNamespace(id=900001))
                pool = Mock(side_effect=AssertionError("Stale confirmation reached the database"))
                namespace = {
                    "validated_confirmation_action": validated_confirmation_action,
                    "FINANCIAL_CONFIRMATION_STALE_TEXT": FINANCIAL_CONFIRMATION_STALE_TEXT,
                    "_pool": pool,
                    "_consume_pending_admin_reset_if_needed": AsyncMock(),
                }
                code = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), nodes[handler]], type_ignores=[])
                exec(compile(ast.fix_missing_locations(code), str(source), "exec"), namespace)
                await namespace[handler](update, context)
                pool.assert_not_called()
                query.message.reply_text.assert_awaited_once_with(FINANCIAL_CONFIRMATION_STALE_TEXT)
                self.assertEqual(context.user_data[key], flow)


if __name__ == "__main__":
    unittest.main()
