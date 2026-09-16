from __future__ import annotations

import ast
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confirmation_guard import bind_financial_preview
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


class SavingsConfirmationRegressionTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source_path = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(cls.source_path.read_text(encoding="utf-8-sig"))
        cls.function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_show_saving_transfer_confirmation")

    def make_runtime(self):
        connection = SimpleNamespace(active=False, execute=AsyncMock())
        acquisitions = []

        @asynccontextmanager
        async def acquire():
            acquisitions.append(connection)
            connection.active = True
            try:
                yield connection
            finally:
                connection.active = False

        async def render(conn, flow):
            self.assertTrue(conn.active, "Copy lookup must use a live scoped connection")
            return "Reviewed " + flow["mode"]

        namespace = {
            "_pool": lambda context: SimpleNamespace(acquire=acquire),
            "_saving_topup_confirm_text": AsyncMock(side_effect=render),
            "_saving_post_income_confirm_text": AsyncMock(side_effect=render),
            "_saving_withdraw_confirm_text": lambda flow: "Reviewed withdrawal",
            "kb_savings_transfer_confirm": lambda **kwargs: InlineKeyboardMarkup([
                [InlineKeyboardButton("Confirm", callback_data=kwargs["confirm_callback"])],
                [InlineKeyboardButton("Cancel", callback_data=kwargs["cancel_callback"])],
            ]),
            "bind_financial_preview": bind_financial_preview,
        }
        code = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), self.function], type_ignores=[])
        exec(compile(ast.fix_missing_locations(code), str(self.source_path), "exec"), namespace)
        return namespace, connection, acquisitions

    async def test_topup_previews_acquire_and_release_their_copy_connection(self) -> None:
        for mode in ("manual_topup", "post_income_prompt"):
            with self.subTest(mode=mode):
                ns, connection, acquisitions = self.make_runtime()
                message = SimpleNamespace(reply_text=AsyncMock())
                context = SimpleNamespace(user_data={})
                try:
                    await ns["_show_saving_transfer_confirmation"](message, context, {"mode": mode})
                except Exception as exc:
                    self.fail(f"Preview failed instead of showing confirmation: {type(exc).__name__}: {exc}")
                self.assertEqual(len(acquisitions), 1)
                self.assertFalse(connection.active)
                message.reply_text.assert_awaited_once()
                self.assertEqual(message.reply_text.await_args.args[0], "Reviewed " + mode)
                self.assertTrue(message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data.startswith("saving:topup:confirm:ok:pv:"))
                connection.execute.assert_not_awaited()

    async def test_supplied_connection_is_reused_not_released_or_reacquired(self) -> None:
        ns, connection, acquisitions = self.make_runtime()
        connection.active = True
        message = SimpleNamespace(reply_text=AsyncMock())
        try:
            await ns["_show_saving_transfer_confirmation"](message, SimpleNamespace(user_data={}), {"mode": "manual_topup"}, conn=connection)
        except Exception as exc:
            self.fail(f"Preview failed with caller-owned connection: {type(exc).__name__}: {exc}")
        self.assertEqual(acquisitions, [])
        self.assertTrue(connection.active)
        message.reply_text.assert_awaited_once()

    async def test_withdraw_preview_needs_no_database_connection(self) -> None:
        ns, connection, acquisitions = self.make_runtime()
        message = SimpleNamespace(reply_text=AsyncMock())
        await ns["_show_saving_transfer_confirmation"](message, SimpleNamespace(user_data={}), {"mode": "manual_withdraw"})
        self.assertEqual(acquisitions, [])
        self.assertTrue(message.reply_text.await_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data.startswith("saving:withdraw:confirm:ok:pv:"))
        connection.execute.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
