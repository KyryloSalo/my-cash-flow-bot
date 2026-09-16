from __future__ import annotations
import ast
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock
from confirmation_guard import bind_financial_preview, consume_financial_confirmation
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


class DebtConfirmationRejectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_stale_or_credit_limited_repayment_requires_new_input(self):
        source = Path(__file__).resolve().parents[1] / "bot_main.py"
        tree = ast.parse(source.read_text(encoding="utf-8-sig"))
        callback = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "debt_callback")
        branch = next(n for n in ast.walk(callback) if isinstance(n, ast.If) and ast.unparse(n.test) == "data == 'debt:confirm'")
        for status in ("over_limit", "credit_limit_exceeded"):
            with self.subTest(status=status):
                flow = {"mode": "repay", "debt_id": 5, "debt_amount": Decimal("100"), "debt_currency": "USD", "account_id": 1, "account_amount": Decimal("4000"), "exchange_rate": Decimal("40")}
                markup = bind_financial_preview(
                    flow,
                    InlineKeyboardMarkup([[InlineKeyboardButton("ok", callback_data="debt:confirm")]]),
                )
                result = SimpleNamespace(status=status, debt=None, required_amount=Decimal("20"), required_currency="USD")
                service = SimpleNamespace(record_repayment=AsyncMock(return_value=result))
                context = SimpleNamespace(user_data={"debt_flow": flow})
                q = SimpleNamespace(data=markup.inline_keyboard[0][0].callback_data, message=SimpleNamespace(reply_text=AsyncMock()))
                namespace = dict(data="debt:confirm", context=context, q=q, user=SimpleNamespace(id=42), service=service, Decimal=Decimal, consume_financial_confirmation=consume_financial_confirmation, normalize_currency=lambda s:s, kb_debts_menu=lambda:None, kb_inline_cancel=lambda x:None, format_money=lambda a,c:f"{a} {c}", DEBT_FLOW_STEP_ENTERING_REPAYMENT_AMOUNT="entering_repayment_amount")
                function = ast.parse("async def run():\n    pass\n").body[0]
                function.body = [branch]
                exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])), str(source), "exec"), namespace)
                await namespace["run"]()
                self.assertEqual(service.record_repayment.await_count, 1)
                self.assertFalse(service.record_repayment.call_args.kwargs.get("allow_partial", True))
                if status == "over_limit":
                    self.assertNotIn("account_amount", flow)
                    self.assertNotIn("exchange_rate", flow)
                    self.assertEqual(flow["step"], "entering_repayment_amount")
                else:
                    self.assertIn("ліміт", q.message.reply_text.call_args.args[0])

if __name__ == "__main__":
    unittest.main()
