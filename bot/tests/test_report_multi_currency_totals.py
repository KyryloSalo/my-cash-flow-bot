from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")
    httpx_stub.AsyncClient = object
    sys.modules["httpx"] = httpx_stub

from report_service import ReportService, map_export_row  # noqa: E402


class DummyConn:
    def __init__(
        self,
        *,
        user_row: dict,
        accounts: list[dict],
        transactions: list[dict],
        debts: list[dict] | None = None,
        pending_tasks: list[dict] | None = None,
    ) -> None:
        self.user_row = dict(user_row)
        self.accounts = [dict(row) for row in accounts]
        self.transactions = [dict(row) for row in transactions]
        self.debts = [dict(row) for row in (debts or [])]
        self.pending_tasks = [dict(row) for row in (pending_tasks or [])]

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM users" in normalized:
            tg_user_id = int(args[0])
            if int(self.user_row["tg_user_id"]) == tg_user_id:
                return dict(self.user_row)
        return None

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM accounts" in normalized:
            tg_user_id = int(args[0])
            return [
                dict(row)
                for row in self.accounts
                if int(row.get("created_by_user_id") or row.get("tg_user_id") or 0) == tg_user_id
            ]
        if "FROM debts" in normalized:
            tg_user_id = int(args[0])
            return [dict(row) for row in self.debts if int(row.get("tg_user_id") or 0) == tg_user_id]
        if "FROM pending_saving_tasks" in normalized:
            tg_user_id = int(args[0])
            return [
                dict(row)
                for row in self.pending_tasks
                if int(row.get("tg_user_id") or 0) == tg_user_id and str(row.get("status") or "") == "pending"
            ]
        if "FROM transactions t" in normalized and "t.flow_kind='debt'" in normalized:
            tg_user_id = int(args[0])
            return [
                dict(row)
                for row in self.transactions
                if int(row.get("created_by_user_id") or row.get("tg_user_id") or 0) == tg_user_id
                and str(row.get("flow_kind") or "") == "debt"
            ]
        if "FROM transactions t" in normalized and "t.date >=" in normalized:
            tg_user_id = int(args[0])
            start_date = args[1]
            end_date = args[2]
            return [
                dict(row)
                for row in self.transactions
                if int(row.get("created_by_user_id") or row.get("tg_user_id") or 0) == tg_user_id
                and row.get("date") is not None
                and start_date <= row["date"] < end_date
            ]
        return []


class ReportMultiCurrencyTotalsTests(unittest.IsolatedAsyncioTestCase):
    def test_map_export_row_uses_other_for_uncategorized_expense(self) -> None:
        row = {
            "date": date(2026, 5, 1),
            "type": "expense",
            "flow_kind": "normal",
            "created_by_user_id": 123,
            "tg_user_id": 123,
            "author_name": "User",
            "account_id": 1,
            "category_id": None,
            "category_name_snapshot": None,
            "amount": Decimal("45"),
            "currency": "UAH",
            "comment": "Unknown merchant",
        }

        export_row = map_export_row(row)

        self.assertEqual(export_row["category"], "Інше")

    async def test_build_export_csv_keeps_uncategorized_expense_in_other_bucket(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[],
            transactions=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "date": date(2026, 5, 1),
                    "type": "income",
                    "amount": Decimal("200"),
                    "currency": "UAH",
                    "account_id": 1,
                    "category_id": 11,
                    "category_name_snapshot": "Salary",
                    "flow_kind": "normal",
                    "comment": "Salary",
                    "author_name": "User",
                },
                {
                    "id": 2,
                    "tg_user_id": 123,
                    "date": date(2026, 5, 2),
                    "type": "expense",
                    "amount": Decimal("45"),
                    "currency": "UAH",
                    "account_id": 1,
                    "category_id": None,
                    "category_name_snapshot": None,
                    "flow_kind": "normal",
                    "comment": "Unknown merchant",
                    "author_name": "User",
                },
            ],
        )

        content, row_count = await ReportService(conn).build_export_csv(123, date(2026, 5, 1), date(2026, 5, 31))

        self.assertEqual(row_count, 2)
        self.assertIsNotNone(content)
        assert content is not None
        csv_text = content.decode("utf-8-sig")
        self.assertIn("Інше", csv_text)
        self.assertIn("45", csv_text)

    async def test_build_export_xlsx_uses_fx_fallback_for_summary_and_current_totals(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("500"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "TRY cash", "currency": "TRY", "account_type": "cash", "balance": Decimal("200"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("7600"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "UAH income"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "income", "amount": Decimal("1000"), "currency": "TRY", "account_id": 2, "category_id": 12, "category_name_snapshot": "Bonus", "flow_kind": "normal", "comment": "TRY income"},
            ],
        )

        async def fake_get_latest_rates(base_currency: str):
            self.assertEqual(base_currency, "UAH")
            return SimpleNamespace(rates={"UAH": Decimal("1"), "TRY": Decimal("1.2")})

        with patch("report_service.get_latest_rates", side_effect=fake_get_latest_rates):
            result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.total_income, "7 600 UAH\n1 000 TRY\n~ 8 800 UAH")
        self.assertEqual(result.summary.money_left, "7 600 UAH\n1 000 TRY\n~ 8 800 UAH")
        self.assertEqual(result.summary.current_available_total, "500 UAH\n200 TRY\n~ 740 UAH")
        self.assertIn("~ 8 800 UAH", result.summary_message)

    async def test_build_normal_report_text_uses_fx_fallback_for_multi_currency_balance(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("7600"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "UAH income", "author_name": "User"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "income", "amount": Decimal("1000"), "currency": "TRY", "account_id": 2, "category_id": 12, "category_name_snapshot": "Bonus", "flow_kind": "normal", "comment": "TRY income", "author_name": "User"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "expense", "amount": Decimal("400"), "currency": "UAH", "account_id": 1, "category_id": 13, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Food", "author_name": "User"},
            ],
        )

        async def fake_get_latest_rates(base_currency: str):
            self.assertEqual(base_currency, "UAH")
            return SimpleNamespace(rates={"UAH": Decimal("1"), "TRY": Decimal("1.2")})

        with patch("report_service.get_latest_rates", side_effect=fake_get_latest_rates):
            report_text = await ReportService(conn).build_normal_report_text(123, date(2026, 5, 1), date(2026, 5, 31), "Місяць")

        self.assertIsNotNone(report_text)
        assert report_text is not None
        self.assertIn("7 600 UAH\n1 000 TRY\n~ 8 800 UAH", report_text)
        self.assertIn("7 200 UAH\n1 000 TRY\n~ 8 400 UAH", report_text)

    async def test_build_normal_report_text_keeps_uncategorized_expense_in_other_bucket(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[],
            transactions=[
                {
                    "id": 1,
                    "tg_user_id": 123,
                    "date": date(2026, 5, 1),
                    "type": "income",
                    "amount": Decimal("200"),
                    "currency": "UAH",
                    "account_id": 1,
                    "category_id": 11,
                    "category_name_snapshot": "Salary",
                    "flow_kind": "normal",
                    "comment": "Salary",
                    "author_name": "User",
                },
                {
                    "id": 2,
                    "tg_user_id": 123,
                    "date": date(2026, 5, 2),
                    "type": "expense",
                    "amount": Decimal("45"),
                    "currency": "UAH",
                    "account_id": 1,
                    "category_id": None,
                    "category_name_snapshot": None,
                    "flow_kind": "normal",
                    "comment": "Unknown merchant",
                    "author_name": "User",
                },
            ],
        )

        report_text = await ReportService(conn).build_normal_report_text(
            123,
            date(2026, 5, 1),
            date(2026, 5, 31),
            "Місяць",
        )

        self.assertIsNotNone(report_text)
        assert report_text is not None
        self.assertIn("Інше", report_text)
        self.assertIn("45 UAH", report_text)

    async def test_build_export_xlsx_supports_usdt_base_currency_for_summary_current_totals_and_chart_points(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "USDT"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("4000"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "USDT wallet", "currency": "USDT", "account_type": "cash", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("4000"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "UAH income"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "income", "amount": Decimal("50"), "currency": "USDT", "account_id": 2, "category_id": 12, "category_name_snapshot": "Freelance", "flow_kind": "normal", "comment": "USDT income"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "expense", "amount": Decimal("2000"), "currency": "UAH", "account_id": 1, "category_id": 13, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "UAH expense"},
            ],
        )

        async def fake_get_latest_rates(base_currency: str):
            self.assertEqual(base_currency, "USDT")
            return SimpleNamespace(rates={"USDT": Decimal("1"), "USD": Decimal("1"), "UAH": Decimal("0.025")})

        with patch("report_service.get_latest_rates", side_effect=fake_get_latest_rates):
            result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.total_income, "50 USDT\n4 000 UAH\n~ 150 USDT")
        self.assertEqual(result.summary.total_expenses, "2 000 UAH\n~ 50 USDT")
        self.assertEqual(result.summary.money_left, "50 USDT\n2 000 UAH\n~ 100 USDT")
        self.assertEqual(result.summary.current_available_total, "100 USDT\n4 000 UAH\n~ 200 USDT")
        self.assertEqual(result.summary.expense_chart_points, (("Food • 100.0%", Decimal("50.00")),))
        self.assertEqual(
            result.summary.income_chart_points,
            (("Salary • 66.7%", Decimal("100.00")), ("Freelance • 33.3%", Decimal("50.00"))),
        )


if __name__ == "__main__":
    unittest.main()
