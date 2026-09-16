from __future__ import annotations

import io
import os
import sys
import types
import unittest
import xml.etree.ElementTree as ET
import zipfile
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

from report_service import ReportService  # noqa: E402


XLSX_NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


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
            scope_id = int(args[0])
            if "family_id=$1" in normalized:
                return [dict(row) for row in self.debts if int(row.get("family_id") or 0) == scope_id]
            return [dict(row) for row in self.debts if int(row.get("tg_user_id") or 0) == scope_id]
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


class ReportXlsxExportTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _zip_text(blob: bytes, path: str) -> str:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            return workbook.read(path).decode("utf-8")

    @staticmethod
    def _worksheet_count(blob: bytes) -> int:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            return len([name for name in workbook.namelist() if name.startswith("xl/worksheets/sheet") and name.endswith(".xml")])

    @staticmethod
    def _sheet_names(blob: bytes) -> list[str]:
        workbook_xml = ReportXlsxExportTests._zip_text(blob, "xl/workbook.xml")
        root = ET.fromstring(workbook_xml)
        return [node.attrib["name"] for node in root.findall(".//main:sheets/main:sheet", XLSX_NS)]

    @staticmethod
    def _sheet_xml(blob: bytes, sheet_index: int) -> str:
        return ReportXlsxExportTests._zip_text(blob, f"xl/worksheets/sheet{sheet_index}.xml")

    @staticmethod
    def _shared_strings(blob: bytes) -> list[str]:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            try:
                shared_xml = workbook.read("xl/sharedStrings.xml").decode("utf-8")
            except KeyError:
                return []
        root = ET.fromstring(shared_xml)
        return ["".join(node.itertext()) for node in root.findall(".//main:si", XLSX_NS)]

    @staticmethod
    def _sheet_cells(blob: bytes, sheet_index: int) -> dict[str, str]:
        root = ET.fromstring(ReportXlsxExportTests._sheet_xml(blob, sheet_index))
        shared_strings = ReportXlsxExportTests._shared_strings(blob)
        values: dict[str, str] = {}
        for cell in root.findall(".//main:sheetData/main:row/main:c", XLSX_NS):
            ref = str(cell.attrib.get("r") or "")
            if not ref:
                continue
            cell_type = cell.attrib.get("t")
            if cell_type == "s":
                value_node = cell.find("main:v", XLSX_NS)
                if value_node is not None and value_node.text is not None:
                    values[ref] = shared_strings[int(value_node.text)]
                continue
            if cell_type == "inlineStr":
                inline_node = cell.find("main:is", XLSX_NS)
                values[ref] = "".join(inline_node.itertext()) if inline_node is not None else ""
                continue
            value_node = cell.find("main:v", XLSX_NS)
            if value_node is not None and value_node.text is not None:
                values[ref] = value_node.text
        return values

    @staticmethod
    def _all_xml_text(blob: bytes) -> str:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            return "\n".join(
                workbook.read(name).decode("utf-8", errors="ignore")
                for name in workbook.namelist()
                if name.endswith(".xml")
            )

    @staticmethod
    def _all_chart_xml_text(blob: bytes) -> str:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            return "\n".join(
                workbook.read(name).decode("utf-8", errors="ignore")
                for name in workbook.namelist()
                if name.startswith("xl/charts/chart") and name.endswith(".xml")
            )

    @staticmethod
    def _chart_count(blob: bytes) -> int:
        with zipfile.ZipFile(io.BytesIO(blob)) as workbook:
            return len(
                [
                    name
                    for name in workbook.namelist()
                    if name.startswith("xl/charts/chart") and name.endswith(".xml")
                ]
            )

    async def test_build_export_xlsx_returns_dashboard_and_transactions_sheets(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("1500"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "Подушка", "currency": "UAH", "account_type": "savings", "balance": Decimal("300"), "is_active": True, "goal_name": "Подушка", "goal_amount": Decimal("1000"), "goal_date": date(2026, 12, 31)},
                {"id": 3, "tg_user_id": 123, "label": "Broker", "currency": "UAH", "account_type": "investment", "balance": Decimal("450"), "is_active": True, "goal_name": "ETF", "goal_amount": Decimal("2000"), "goal_date": None},
                {"id": 4, "tg_user_id": 123, "label": "Cash", "currency": "UAH", "account_type": "cash", "balance": Decimal("120"), "is_active": True},
                {"id": 9, "tg_user_id": 999, "label": "Other", "currency": "UAH", "account_type": "main", "balance": Decimal("999"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("1000"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "May salary"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "expense", "amount": Decimal("400"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Groceries"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "transfer", "amount": Decimal("100"), "to_amount": Decimal("100"), "currency": "UAH", "to_currency": "UAH", "from_account_id": 1, "to_account_id": 2, "flow_kind": "transfer", "transfer_subtype": "savings_transfer", "comment": "Top up savings"},
                {"id": 4, "tg_user_id": 123, "date": date(2026, 5, 4), "type": "transfer", "amount": Decimal("50"), "to_amount": Decimal("50"), "currency": "UAH", "to_currency": "UAH", "from_account_id": 1, "to_account_id": 3, "flow_kind": "transfer", "transfer_subtype": "savings_transfer", "comment": "Buy ETF"},
                {"id": 5, "tg_user_id": 123, "date": date(2026, 5, 5), "type": "transfer", "amount": Decimal("200"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Bank", "debt_action": "borrow", "comment": "Borrowed", "debt_id": 77},
                {"id": 6, "tg_user_id": 123, "date": date(2026, 5, 6), "type": "transfer", "amount": Decimal("80"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Bank", "debt_action": "borrow_repaid", "comment": "Paid debt", "debt_id": 77},
                {"id": 7, "tg_user_id": 123, "date": date(2026, 5, 7), "type": "transfer", "amount": Decimal("70"), "to_amount": Decimal("70"), "currency": "UAH", "to_currency": "UAH", "from_account_id": 1, "to_account_id": 4, "flow_kind": "transfer", "comment": "Move cash"},
                {"id": 8, "tg_user_id": 999, "date": date(2026, 5, 8), "type": "income", "amount": Decimal("9999"), "currency": "UAH", "account_id": 9, "category_id": 99, "category_name_snapshot": "Other", "flow_kind": "normal", "comment": "Other user income"},
            ],
            debts=[
                {"id": 77, "tg_user_id": 123, "counterparty_name": "Bank", "direction": "payable", "initial_amount": Decimal("200"), "paid_amount": Decimal("80"), "remaining_amount": Decimal("120"), "currency": "UAH", "account_id": 1, "status": "partially_paid", "due_date": date(2026, 6, 1), "comment": "Main bank debt"},
                {"id": 88, "tg_user_id": 123, "counterparty_name": "Alice", "direction": "receivable", "initial_amount": Decimal("500"), "paid_amount": Decimal("0"), "remaining_amount": Decimal("500"), "currency": "UAH", "account_id": 1, "status": "active", "due_date": None, "comment": "Friend debt"},
            ],
            pending_tasks=[
                {"id": 1, "tg_user_id": 123, "status": "pending", "amount": Decimal("150"), "currency": "UAH", "source_label": "Mono", "target_label": "Подушка"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.row_count, 7)
        self.assertEqual(result.summary.total_income, "1 000 UAH")
        self.assertEqual(result.summary.total_expenses, "400 UAH")
        self.assertEqual(result.summary.savings_amount, "100 UAH")
        self.assertEqual(result.summary.investments_amount, "50 UAH")
        self.assertEqual(result.summary.debt_payments_amount, "80 UAH")
        self.assertEqual(result.summary.money_left, "370 UAH")
        self.assertEqual(result.summary.expense_percentage, "40.0%")
        self.assertEqual(result.summary.savings_percentage, "10.0%")
        self.assertEqual(result.summary.investments_percentage, "5.0%")
        self.assertEqual(result.summary.debt_payments_percentage, "8.0%")
        self.assertEqual(result.summary.current_available_total, "1 620 UAH")
        self.assertEqual(result.summary.current_savings_total, "300 UAH")
        self.assertEqual(result.summary.current_investments_total, "450 UAH")
        self.assertEqual(result.summary.current_receivable_total, "500 UAH")
        self.assertEqual(result.summary.current_debts_total, "120 UAH")
        self.assertEqual(result.summary.debt_balance_total, "380 UAH")
        self.assertEqual(result.summary.planned_savings_total, "1 / 150 UAH")
        self.assertEqual(result.summary.top_expense_category, "Food (400 UAH)")
        self.assertIn("Food - 400 UAH", result.summary.largest_expense)
        self.assertTrue(any("Bank" in line for line in result.summary.debt_snapshot_lines))
        self.assertTrue(any("Подушка" in line for line in result.summary.goal_snapshot_lines))
        self.assertIn("Дохід: 1 000 UAH", result.summary_message)

        self.assertEqual(self._worksheet_count(result.content), 2)
        self.assertEqual(self._sheet_names(result.content), ["Мій звіт", "Транзакції"])
        self.assertGreaterEqual(self._chart_count(result.content), 4)

        workbook_text = self._all_xml_text(result.content)
        dashboard_xml = self._sheet_xml(result.content, 1)
        transactions_xml = self._sheet_xml(result.content, 2)

        for dashboard_label in (
            "Підсумок",
            "Аналітика",
            "Топ витрат",
            "Борги",
            "Цілі та плани",
            "Витрати по категоріях",
            "Доходи по категоріях",
            "Заощадження по рахунках",
            "Боргове навантаження",
        ):
            self.assertIn(dashboard_label, workbook_text)

        for header in (
            "Дата",
            "Місяць",
            "Тип",
            "Хто / категорія",
            "Що сталося",
            "Рахунок / маршрут",
            "Сума",
            "Валюта",
            "У базовій валюті",
            "Статус / контекст",
            "Нотатка",
        ):
            self.assertIn(header, workbook_text)

        for label in ("Дохід", "Витрата", "Заощадження", "Інвестиція", "Борг", "Переказ"):
            self.assertIn(label, workbook_text)

        self.assertIn("Bank", workbook_text)
        self.assertIn("Alice", workbook_text)
        self.assertIn("частково погашений", workbook_text)
        self.assertNotIn("Other user income", workbook_text)
        self.assertIn('hidden="1"', dashboard_xml)
        self.assertNotIn('plotVisOnly val="1"', workbook_text)
        self.assertIn('state="frozen"', transactions_xml)

    async def test_build_export_xlsx_formats_multi_currency_totals_as_separate_lines_with_approximate_base_total(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("500"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "TRY", "currency": "TRY", "account_type": "cash", "balance": Decimal("200"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("17600"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "UAH income"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "income", "amount": Decimal("1000"), "currency": "TRY", "to_amount": Decimal("1200"), "to_currency": "UAH", "account_id": 2, "category_id": 12, "category_name_snapshot": "Bonus", "flow_kind": "normal", "comment": "TRY income"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.total_income, "17 600 UAH\n1 000 TRY\n~ 18 800 UAH")

        workbook_text = self._all_xml_text(result.content)
        self.assertIn("17 600 UAH", workbook_text)
        self.assertIn("1 000 TRY", workbook_text)
        self.assertIn("~ 18 800 UAH", workbook_text)
        self.assertNotIn("17 600 UAH + 1 000 TRY", workbook_text)

    async def test_build_export_xlsx_places_transactions_on_second_sheet_and_freezes_only_title_and_header(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("100"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        dashboard_xml = self._sheet_xml(result.content, 1)
        transactions_xml = self._sheet_xml(result.content, 2)
        self.assertNotIn('state="frozen"', dashboard_xml)
        self.assertIn('state="frozen"', transactions_xml)
        self.assertIn('ySplit="2"', transactions_xml)
        self.assertIn('topLeftCell="A3"', transactions_xml)

    async def test_build_export_xlsx_dashboard_contains_new_infographic_sections_and_legacy_debt_fallback(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("1000"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "Подушка", "currency": "UAH", "account_type": "savings", "balance": Decimal("300"), "is_active": True, "goal_amount": Decimal("1000")},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("7600"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "expense", "amount": Decimal("2100"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Підписки", "flow_kind": "normal", "comment": "Subscriptions"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "transfer", "amount": Decimal("1111"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Олег", "debt_action": "lend", "comment": "Lent"},
                {"id": 4, "tg_user_id": 123, "date": date(2026, 5, 4), "type": "transfer", "amount": Decimal("500"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Олег", "debt_action": "lend_repaid", "comment": "Repaid to me"},
                {"id": 5, "tg_user_id": 123, "date": date(2026, 5, 5), "type": "transfer", "amount": Decimal("500"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Іра", "debt_action": "borrow", "comment": "Borrowed"},
            ],
            debts=[],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.current_receivable_total, "611 UAH")
        self.assertEqual(result.summary.current_debts_total, "500 UAH")
        self.assertEqual(result.summary.debt_balance_total, "111 UAH")

        workbook_text = self._all_xml_text(result.content)
        for label in ("Витрати по категоріях", "Доходи по категоріях", "Заощадження по рахунках", "Боргове навантаження"):
            self.assertIn(label, workbook_text)
        self.assertIn("Мені винні — 611 UAH", workbook_text)
        self.assertIn("Я винен — 500 UAH", workbook_text)

        self.assertIn("Підписки • 100.0%", workbook_text)
        self.assertIn("Мені винні • 8.0%", workbook_text)

    async def test_build_export_xlsx_returns_none_for_empty_user(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[],
            transactions=[],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Порожньо")
        self.assertIsNone(result)

    async def test_build_export_xlsx_places_auto_filter_on_transactions_sheet(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("100"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        transactions_xml = self._sheet_xml(result.content, 2)
        self.assertIn("<tableParts", transactions_xml)

    async def test_build_export_xlsx_sanitizes_invalid_xml_characters(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "expense", "amount": Decimal("40"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Bad\x0bcomment"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        workbook_text = self._all_xml_text(result.content)
        dashboard_xml = self._sheet_xml(result.content, 1)
        transactions_xml = self._sheet_xml(result.content, 2)
        self.assertNotIn("\x0b", workbook_text)
        ET.fromstring(dashboard_xml)
        ET.fromstring(transactions_xml)

    async def test_build_export_xlsx_mixed_export_flow_keeps_credit_payoff_as_transfer_and_excludes_credit_accounts_from_available_total(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Main", "currency": "UAH", "account_type": "main", "balance": Decimal("2000"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "Emergency", "currency": "UAH", "account_type": "savings", "balance": Decimal("300"), "is_active": True},
                {"id": 3, "tg_user_id": 123, "label": "Credit Card", "currency": "UAH", "account_type": "credit", "balance": Decimal("-150"), "is_active": True},
                {"id": 4, "tg_user_id": 123, "label": "Cash", "currency": "UAH", "account_type": "cash", "balance": Decimal("50"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("1200"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "expense", "amount": Decimal("200"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Groceries"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "transfer", "amount": Decimal("100"), "to_amount": Decimal("100"), "currency": "UAH", "to_currency": "UAH", "from_account_id": 1, "to_account_id": 2, "flow_kind": "transfer", "transfer_subtype": "savings_transfer", "comment": "Emergency top up"},
                {"id": 4, "tg_user_id": 123, "date": date(2026, 5, 4), "type": "income", "amount": Decimal("25"), "currency": "UAH", "account_id": 1, "category_id": None, "category_name_snapshot": None, "flow_kind": "adjustment", "source": "balance_correction", "comment": "Корекція балансу до 2000 UAH"},
                {"id": 5, "tg_user_id": 123, "date": date(2026, 5, 5), "type": "transfer", "amount": Decimal("75"), "currency": "UAH", "flow_kind": "debt", "counterparty": "Bank", "debt_action": "borrow_repaid", "comment": "Debt payment", "debt_id": 44},
                {"id": 6, "tg_user_id": 123, "date": date(2026, 5, 6), "type": "transfer", "amount": Decimal("150"), "to_amount": Decimal("150"), "currency": "UAH", "to_currency": "UAH", "from_account_id": 1, "to_account_id": 3, "flow_kind": "transfer", "transfer_subtype": "credit_payment", "comment": "Card payoff"},
            ],
            debts=[
                {"id": 44, "tg_user_id": 123, "counterparty_name": "Bank", "direction": "payable", "initial_amount": Decimal("500"), "paid_amount": Decimal("75"), "remaining_amount": Decimal("425"), "currency": "UAH", "account_id": 3, "status": "partially_paid"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.row_count, 6)
        self.assertEqual(result.summary.current_available_total, "2 050 UAH")
        self.assertEqual(result.summary.current_savings_total, "300 UAH")
        self.assertEqual(result.summary.debt_payments_amount, "75 UAH")
        self.assertEqual(result.summary.money_left, "825 UAH")

        workbook_text = self._all_xml_text(result.content)
        self.assertIn("Погашення кредитки", workbook_text)
        self.assertIn("Корекція балансу", workbook_text)
        self.assertIn("Credit Card", workbook_text)
        self.assertIn("Bank", workbook_text)
        self.assertNotIn("1 900 UAH", result.summary.current_available_total)

    async def test_build_export_xlsx_cross_currency_transfer_preserves_manual_fx_context_and_base_amount(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("900"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "USDT Wallet", "currency": "USDT", "account_type": "cash", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("5000"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "transfer", "amount": Decimal("4100"), "to_amount": Decimal("100"), "currency": "UAH", "to_currency": "USDT", "fx_rate": Decimal("41"), "fx_rate_text": "41", "fx_rate_source": "manual", "from_account_id": 1, "to_account_id": 2, "flow_kind": "transfer", "comment": "Buy USDT"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "expense", "amount": Decimal("200"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Food"},
            ],
        )

        async def fake_get_latest_rates(base_currency: str):
            self.assertEqual(base_currency, "UAH")
            return SimpleNamespace(rates={"UAH": Decimal("1"), "USDT": Decimal("41")})

        with patch("report_service.get_latest_rates", side_effect=fake_get_latest_rates):
            result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.current_available_total, "900 UAH\n100 USDT\n~ 5 000 UAH")

        cells = self._sheet_cells(result.content, 2)
        self.assertEqual(cells["F4"], "Mono -> USDT Wallet")
        self.assertEqual(cells["G4"], "4100")
        self.assertEqual(cells["H4"], "UAH")
        self.assertEqual(cells["I4"], "4100")
        self.assertEqual(cells["J4"], "Курс 41")

    async def test_build_export_xlsx_unsupported_custom_currency_stays_raw_without_silent_conversion(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "UAH"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "BTC Wallet", "currency": "BTC", "account_type": "main", "balance": Decimal("1"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("1"), "currency": "BTC", "account_id": 1, "category_id": 11, "category_name_snapshot": "Mining", "flow_kind": "normal", "comment": "BTC income"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "expense", "amount": Decimal("10"), "currency": "UAH", "account_id": 1, "category_id": 12, "category_name_snapshot": "Fee", "flow_kind": "normal", "comment": "Fee"},
            ],
        )

        result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.summary.total_income, "1 BTC")
        self.assertEqual(result.summary.current_available_total, "1 BTC")
        self.assertEqual(result.summary.money_left, "-10 UAH\n1 BTC")
        self.assertNotIn("~ ", result.summary.total_income)
        self.assertNotIn("~ ", result.summary.current_available_total)
        self.assertNotIn("~ ", result.summary.money_left)

    async def test_build_export_xlsx_uses_usdt_base_currency_in_dashboard_chart_sources(self) -> None:
        conn = DummyConn(
            user_row={"tg_user_id": 123, "lang": "uk", "base_currency": "USDT"},
            accounts=[
                {"id": 1, "tg_user_id": 123, "label": "Mono", "currency": "UAH", "account_type": "main", "balance": Decimal("4000"), "is_active": True},
                {"id": 2, "tg_user_id": 123, "label": "USDT Wallet", "currency": "USDT", "account_type": "cash", "balance": Decimal("100"), "is_active": True},
            ],
            transactions=[
                {"id": 1, "tg_user_id": 123, "date": date(2026, 5, 1), "type": "income", "amount": Decimal("4000"), "currency": "UAH", "account_id": 1, "category_id": 11, "category_name_snapshot": "Salary", "flow_kind": "normal", "comment": "Salary"},
                {"id": 2, "tg_user_id": 123, "date": date(2026, 5, 2), "type": "income", "amount": Decimal("50"), "currency": "USDT", "account_id": 2, "category_id": 12, "category_name_snapshot": "Freelance", "flow_kind": "normal", "comment": "Freelance"},
                {"id": 3, "tg_user_id": 123, "date": date(2026, 5, 3), "type": "expense", "amount": Decimal("2000"), "currency": "UAH", "account_id": 1, "category_id": 13, "category_name_snapshot": "Food", "flow_kind": "normal", "comment": "Food"},
            ],
        )

        async def fake_get_latest_rates(base_currency: str):
            self.assertEqual(base_currency, "USDT")
            return SimpleNamespace(rates={"USDT": Decimal("1"), "USD": Decimal("1"), "UAH": Decimal("0.025")})

        with patch("report_service.get_latest_rates", side_effect=fake_get_latest_rates):
            result = await ReportService(conn).build_export_xlsx(123, date(2026, 5, 1), date(2026, 5, 31), "Тест")

        self.assertIsNotNone(result)
        assert result is not None
        dashboard_cells = self._sheet_cells(result.content, 1)
        self.assertEqual(dashboard_cells["N1"], "USDT")
        chart_xml = self._all_chart_xml_text(result.content)
        self.assertIn("USDT", chart_xml)


if __name__ == "__main__":
    unittest.main()
