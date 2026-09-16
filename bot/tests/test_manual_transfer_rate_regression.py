from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock


class ManualTransferRateRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_auto_button_cannot_supply_an_unconfirmed_exchange_rate(self) -> None:
        @asynccontextmanager
        async def acquire():
            yield object()

        flow = {
            "step": "enter_exchange_rate", "source_account_id": 1, "target_account_id": 2,
            "source_currency": "USD", "target_currency": "EUR", "fx_rate": "0.92", "rate_source": "bot",
        }
        context = SimpleNamespace(user_data={"transfer_flow": flow})
        query = SimpleNamespace(data="transfer:rate:auto", message=SimpleNamespace(reply_text=AsyncMock()), answer=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=123), callback_query=query)
        namespace = {
            "_pool": lambda context: SimpleNamespace(acquire=acquire),
            "_consume_pending_admin_reset_if_needed": AsyncMock(),
            "should_block_for_maintenance": AsyncMock(return_value=None),
            "_get_resolved_access_state": AsyncMock(return_value={"access_scope": "personal_full"}),
            "_user_ready": AsyncMock(return_value=True),
            "_access_scope_has_full_home": lambda scope: True,
            "_billing_write_blocked": AsyncMock(return_value=False),
            "_resolve_nbu_rate": AsyncMock(return_value="0.95"),
            "_show_transfer_amount_step": AsyncMock(),
            "_show_transfer_rate_step": AsyncMock(),
            "format_exchange_rate": lambda *args: "synthetic rate",
        }
        source = Path(__file__).resolve().parents[1] / "bot_main.py"
        nodes = [node for node in ast.parse(source.read_text(encoding="utf-8-sig")).body if isinstance(node, ast.AsyncFunctionDef) and node.name == "transfer_callback"]
        self.assertEqual(len(nodes), 1)
        module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), nodes[0]], type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
        await namespace["transfer_callback"](update, context)
        namespace["_resolve_nbu_rate"].assert_not_awaited()
        namespace["_show_transfer_amount_step"].assert_not_awaited()
        namespace["_show_transfer_rate_step"].assert_awaited_once()
        self.assertEqual(flow["step"], "enter_exchange_rate")
        self.assertNotIn("fx_rate", flow)
        self.assertNotIn("rate_source", flow)


if __name__ == "__main__":
    unittest.main()
