from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import os
import sys
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from confirmation_guard import bind_financial_preview, consume_financial_confirmation, is_current_confirmation  # noqa: E402
from finance import normalize_currency  # noqa: E402
from telegram import InlineKeyboardButton, InlineKeyboardMarkup  # noqa: E402


class StaleCallbackRegressionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(cls.source.read_text(encoding="utf-8-sig"))
        cls.nodes = {node.name: node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name in {"pick_callback", "transfer_callback"}}
        cls.constants = {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id in {"TX_FLOW_STEP_CONFIRMATION", "TX_FLOW_STEP_COMMITTING"}:
                        cls.constants[target.id] = ast.literal_eval(node.value)

    def make_case(self, kind, data_kind):
        @asynccontextmanager
        async def acquire():
            yield object()

        if kind == "expense":
            key, handler, action = "tx_flow", "pick_callback", "expense:confirm"
            flow = {"kind": "expense", "step": self.constants["TX_FLOW_STEP_CONFIRMATION"], "amount": Decimal("500"), "account_id": 1, "category_id": 7, "currency": "UAH"}
        else:
            key, handler, action = "transfer_flow", "transfer_callback", "transfer:confirm"
            flow = {"step": "confirm_transfer", "source_account_id": 1, "target_account_id": 2, "source_currency": "UAH", "target_currency": "UAH", "source_amount": Decimal("500")}
        context = SimpleNamespace(user_data={key: flow})
        old_flow = deepcopy(flow)
        old_flow["amount" if kind == "expense" else "source_amount"] = Decimal("50")
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Confirm", callback_data=action)]])
        old_data = bind_financial_preview(old_flow, keyboard).inline_keyboard[0][0].callback_data
        current_data = bind_financial_preview(flow, keyboard).inline_keyboard[0][0].callback_data
        data = {"legacy": action, "old": old_data, "current": current_data}[data_kind]
        query = SimpleNamespace(data=data, message=SimpleNamespace(reply_text=AsyncMock()), answer=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), callback_query=query)
        writes = []

        async def commit(message, ctx, user_id, actual_flow, **kwargs):
            writes.append(deepcopy(actual_flow))
            ctx.user_data.pop(key, None)

        namespace = {
            **self.constants, "normalize_currency": normalize_currency, "Decimal": Decimal,
            "is_current_confirmation": is_current_confirmation,
            "consume_financial_confirmation": consume_financial_confirmation,
            "FINANCIAL_CONFIRMATION_STALE_TEXT": "Це підтвердження застаріло. Відкрийте актуальний перегляд операції.",
            "_pool": lambda ctx: SimpleNamespace(acquire=acquire),
            "_consume_pending_admin_reset_if_needed": AsyncMock(),
            "should_block_for_maintenance": AsyncMock(return_value=None),
            "_user_ready": AsyncMock(return_value=True),
            "_get_resolved_access_state": AsyncMock(return_value={"access_scope": "personal_full"}),
            "_access_scope_has_full_home": lambda scope: True,
            "_billing_write_blocked": AsyncMock(return_value=False),
            "_show_access_surface": AsyncMock(), "_show_accounts_settings": AsyncMock(),
            "_commit_tx_flow": commit, "_complete_transfer_flow": commit,
        }
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), self.nodes[handler]], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(self.source), "exec"), namespace)
        return namespace[handler], update, context, writes, key

    async def test_legacy_confirm_never_writes_the_newer_active_draft(self):
        for kind in ("expense", "transfer"):
            with self.subTest(kind=kind):
                handler, update, context, writes, key = self.make_case(kind, "legacy")
                before = deepcopy(context.user_data[key])
                await handler(update, context)
                self.assertEqual(writes, [])
                self.assertEqual(context.user_data[key], before)

    async def test_old_bound_preview_cannot_confirm_a_new_draft(self):
        for kind in ("expense", "transfer"):
            with self.subTest(kind=kind):
                handler, update, context, writes, key = self.make_case(kind, "old")
                await handler(update, context)
                self.assertEqual(writes, [])
                self.assertIn(key, context.user_data)
                self.assertTrue(update.callback_query.message.reply_text.await_count)

    async def test_current_bound_confirmation_writes_once_and_repeat_is_safe(self):
        for kind in ("expense", "transfer"):
            with self.subTest(kind=kind):
                handler, update, context, writes, key = self.make_case(kind, "current")
                await handler(update, context)
                self.assertEqual(len(writes), 1)
                self.assertNotIn(key, context.user_data)
                await handler(update, context)
                self.assertEqual(len(writes), 1)


if __name__ == "__main__":
    unittest.main()
