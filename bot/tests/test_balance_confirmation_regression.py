from __future__ import annotations
import ast
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import AsyncMock
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from finance import parse_decimal_amount, quantize_money, normalize_currency, format_money
from confirmation_guard import bind_financial_preview, consume_financial_confirmation, is_current_confirmation, FINANCIAL_CONFIRMATION_STALE_TEXT
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


class BalancePreviewTests(unittest.IsolatedAsyncioTestCase):
    def setup_case(self):
        @asynccontextmanager
        async def transaction():
            yield
        conn = SimpleNamespace(transaction=transaction)
        @asynccontextmanager
        async def acquire():
            yield conn
        service = SimpleNamespace(get_active_account_by_id=AsyncMock(return_value={"id": 1, "label": "Test", "balance": Decimal("1000"), "currency": "UAH"}), create_balance_correction=AsyncMock(return_value=(Decimal("200"), Decimal("200"))))
        namespace = dict(Decimal=Decimal, parse_decimal_amount=parse_decimal_amount, quantize_money=quantize_money, normalize_currency=normalize_currency, format_money=format_money, InlineKeyboardButton=InlineKeyboardButton, InlineKeyboardMarkup=InlineKeyboardMarkup, bind_financial_preview=bind_financial_preview, consume_financial_confirmation=consume_financial_confirmation, is_current_confirmation=is_current_confirmation, FINANCIAL_CONFIRMATION_STALE_TEXT=FINANCIAL_CONFIRMATION_STALE_TEXT, AccountService=lambda c: service, _pool=lambda c: SimpleNamespace(acquire=acquire), _user_ready=AsyncMock(return_value=True), _billing_write_blocked=AsyncMock(return_value=False), _show_accounts_settings=AsyncMock(), kb_home=lambda: None)
        source = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        nodes = [n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name in {"_handle_account_balance_text", "_handle_account_balance_confirmation"}]
        module = ast.Module(body=[ast.ImportFrom(module="__future__",names=[ast.alias(name="annotations")],level=0),*nodes],type_ignores=[])
        exec(compile(ast.fix_missing_locations(module), str(source), "exec"), namespace)
        context = SimpleNamespace(user_data={"accounts_flow": {"step": "balance", "account_id": 1}})
        update = SimpleNamespace(effective_user=SimpleNamespace(id=900001), message=SimpleNamespace(text="1200", reply_text=AsyncMock()))
        return namespace, service, context, update

    async def test_typing_balance_renders_preview_without_writing(self):
        ns, service, context, update = self.setup_case()
        await ns["_handle_account_balance_text"](update, context)
        service.create_balance_correction.assert_not_awaited()
        flow = context.user_data["accounts_flow"]
        markup = update.message.reply_text.call_args.kwargs["reply_markup"]
        data = markup.inline_keyboard[0][0].callback_data
        self.assertTrue(is_current_confirmation(flow, data))
        self.assertEqual(flow["current_balance"], Decimal("1000"))
        self.assertEqual(flow["target_balance"], Decimal("1200"))

    async def test_only_preview_button_can_write_and_duplicate_is_rejected(self):
        ns, service, context, update = self.setup_case()
        self.assertIn("_handle_account_balance_confirmation", ns)
        await ns["_handle_account_balance_text"](update, context)
        data = update.message.reply_text.call_args.kwargs["reply_markup"].inline_keyboard[0][0].callback_data
        query = SimpleNamespace(data="accounts:balance:confirm", message=update.message)
        await ns["_handle_account_balance_confirmation"](query, context, 900001)
        service.create_balance_correction.assert_not_awaited()
        query.data = data
        await ns["_handle_account_balance_confirmation"](query, context, 900001)
        await ns["_handle_account_balance_confirmation"](query, context, 900001)
        service.create_balance_correction.assert_awaited_once_with(900001, 1, Decimal("1000"), Decimal("1200"), "UAH")

if __name__ == "__main__":
    unittest.main()
