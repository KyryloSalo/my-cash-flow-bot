"""FIN-007/011/012 regressions: real services, synthetic in-memory SQL only."""
from __future__ import annotations

import io
from pathlib import Path
import re
import sqlite3
import sys
import unittest
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import report_service
from report_service import ReportService
from savings_service import SavingsService
from account_service import AccountService
from finance import format_money


START = date(2026, 9, 1)
END = date(2026, 10, 1)
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


class ReportingBatch1Conn:
    """Execute service SELECTs rather than return rows for guessed query shapes.

    SQLite is only a predicate/join test seam, not a PostgreSQL concurrency proof.
    Decimal amounts are stored as TEXT; only cast/parameter spelling is adapted.
    """

    def __init__(self):
        self.db = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        self.db.row_factory = sqlite3.Row
        self.queries = []
        self.db.executescript("""
            CREATE TABLE users (tg_user_id INTEGER PRIMARY KEY, lang TEXT,
                base_currency TEXT, start_date DATE, first_name TEXT, username TEXT);
            CREATE TABLE families (id INTEGER PRIMARY KEY, status TEXT);
            CREATE TABLE family_members (id INTEGER PRIMARY KEY, user_id INTEGER,
                family_id INTEGER, role TEXT, status TEXT, joined_at TIMESTAMP);
            CREATE TABLE accounts (id INTEGER PRIMARY KEY, tg_user_id INTEGER,
                family_id INTEGER, created_by_user_id INTEGER, label TEXT, currency TEXT,
                account_type TEXT, starting_balance TEXT, balance TEXT, is_active BOOLEAN,
                created_at TIMESTAMP, updated_at TIMESTAMP, goal_name TEXT, goal_amount TEXT,
                goal_date DATE, credit_limit TEXT, monthly_interest_rate TEXT,
                non_negative_account_type TEXT);
            CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE transactions (id INTEGER PRIMARY KEY, tg_user_id INTEGER,
                created_by_user_id INTEGER, family_id INTEGER, date DATE, type TEXT,
                flow_kind TEXT, amount TEXT, currency TEXT, category_id INTEGER,
                category_name_snapshot TEXT, account_id INTEGER, from_account_id INTEGER,
                to_account_id INTEGER, to_amount TEXT, to_currency TEXT, fx_rate TEXT,
                transfer_subtype TEXT, comment TEXT, source TEXT, debt_action TEXT,
                counterparty TEXT, debt_id INTEGER, is_deleted BOOLEAN DEFAULT 0,
                deleted_at TIMESTAMP);
            CREATE TABLE debts (id INTEGER PRIMARY KEY, tg_user_id INTEGER, family_id INTEGER,
                counterparty_name TEXT, direction TEXT, initial_amount TEXT, paid_amount TEXT,
                remaining_amount TEXT, currency TEXT, account_id INTEGER, status TEXT,
                due_date DATE, comment TEXT, borrower_user_id INTEGER, borrower_confirmed_at TIMESTAMP,
                reminder_enabled BOOLEAN, next_reminder_at TIMESTAMP, created_at TIMESTAMP);
            CREATE TABLE pending_saving_tasks (id INTEGER PRIMARY KEY, tg_user_id INTEGER,
                source_account_id INTEGER, target_account_id INTEGER, amount TEXT,
                currency TEXT, status TEXT, created_at TIMESTAMP);
        """)
        self.insert("users", tg_user_id=123, lang="uk", base_currency="UAH", start_date=START)

    def insert(self, table, **values):
        # Identifiers here are test constants, never untrusted input.
        converted = [str(v) if isinstance(v, Decimal) else v for v in values.values()]
        self.db.execute(
            f"INSERT INTO {table} ({', '.join(values)}) VALUES ({', '.join('?' for _ in values)})",
            converted,
        )

    def account(self, account_id, *, user_id=123, family_id=None, **fields):
        row = dict(id=account_id, tg_user_id=user_id, family_id=family_id,
                   created_by_user_id=user_id, label=f"Account {account_id}", currency="UAH",
                   account_type="main", starting_balance=Decimal("0"), balance=Decimal("10"),
                   is_active=True, created_at=datetime(2026, 1, 1), non_negative_account_type="main")
        row.update(fields)
        self.insert("accounts", **row)

    def transaction(self, transaction_id, **fields):
        row = dict(id=transaction_id, tg_user_id=123, created_by_user_id=123, family_id=None,
                   date=START, type="income", flow_kind="normal", amount=Decimal("1"),
                   currency="UAH", account_id=1, category_name_snapshot="Synthetic income",
                   comment=f"History {transaction_id}", is_deleted=False)
        row.update(fields)
        self.insert("transactions", **row)

    def membership(self, *, family_id=99, status="active", family_status="active", role="member"):
        self.insert("families", id=family_id, status=family_status)
        self.insert("family_members", id=family_id, user_id=123, family_id=family_id,
                    role=role, status=status, joined_at=datetime(2026, 1, 1))

    async def fetch(self, query, *args):
        self.queries.append((query, args))
        sql = re.sub(r"\$(\d+)", r"?\1", query).replace("::text", "")
        return [dict(row) for row in self.db.execute(sql, args).fetchall()]

    async def fetchrow(self, query, *args):
        rows = await self.fetch(query, *args)
        return rows[0] if rows else None

    def close(self):
        self.db.close()


def inspect_workbook(content):
    """Read every XML part, including hidden chart source cells and cached data."""
    with zipfile.ZipFile(io.BytesIO(content)) as book:
        if book.testzip() is not None:
            raise AssertionError("Broken XLSX ZIP")
        parts = {name: book.read(name).decode("utf-8") for name in book.namelist()
                 if name.endswith(".xml")}
    for payload in parts.values():
        ET.fromstring(payload)
    root = ET.fromstring(parts["xl/workbook.xml"])
    sheets = [node.attrib["name"] for node in root.findall("s:sheets/s:sheet", NS)]
    return sheets, "\n".join(parts.values())


class ReportingBatch1ScopeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn = ReportingBatch1Conn()
        self.addCleanup(self.conn.close)
        self.fx = patch.object(report_service, "get_latest_rates", side_effect=AssertionError("Offline tests"))
        self.fx.start()
        self.addCleanup(self.fx.stop)

    async def test_revoked_family_snapshot_is_absent_but_authored_history_survives(self):
        c = self.conn
        c.membership(status="removed")
        c.account(1, label="Authorized personal", balance=Decimal("100.25"))
        c.account(2, family_id=99, label="REVOKED LIVE ACCOUNT", balance=Decimal("543210"))
        c.account(3, family_id=99, label="REVOKED LIVE SAVINGS", account_type="savings",
                  balance=Decimal("654321"), goal_name="REVOKED LIVE GOAL", goal_amount=Decimal("765432"))
        c.transaction(1, amount=Decimal("12.50"), comment="Authorized personal history")
        c.transaction(2, family_id=99, account_id=2, amount=Decimal("2.25"), comment="Authored family history")
        c.transaction(3, family_id=99, account_id=2, tg_user_id=456, created_by_user_id=456,
                      amount=Decimal("9"), comment="Not my history")
        c.transaction(4, account_id=1, amount=Decimal("99"), is_deleted=True, comment="Cancelled history")

        result = await ReportService(c).build_export_xlsx(123, START, END, "Synthetic revoked scope")

        self.assertIsNotNone(result)
        sheets, xml = inspect_workbook(result.content)
        self.assertEqual(sheets, ["Мій звіт", "Транзакції"])
        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.summary.total_income, "14.75 UAH")
        self.assertIn("Authored family history", xml)
        self.assertIn("Authorized personal history", xml)
        self.assertNotIn("Not my history", xml)
        self.assertNotIn("Cancelled history", xml)
        self.assertEqual(result.summary.current_available_total, "100.25 UAH")
        self.assertEqual(result.summary.current_savings_total, "0")
        for forbidden in ("REVOKED LIVE", "543210", "543 210", "654321", "654 321", "765432", "765 432"):
            self.assertNotIn(forbidden, xml)

    async def test_active_family_snapshot_uses_current_family_not_account_creator(self):
        c = self.conn
        c.membership()
        c.account(1, label="PRIVATE OUTSIDE CURRENT SCOPE", balance=Decimal("10"))
        c.account(2, family_id=99, balance=Decimal("20"))
        c.account(3, user_id=456, family_id=99, label="Authorized shared goal",
                  account_type="savings", balance=Decimal("30"), goal_amount=Decimal("100"))
        c.account(4, family_id=98, label="OTHER FAMILY SNAPSHOT", balance=Decimal("40"))
        c.transaction(1, family_id=99, account_id=2)

        result = await ReportService(c).build_export_xlsx(123, START, END, "Active family")

        _, xml = inspect_workbook(result.content)
        self.assertEqual(result.summary.current_available_total, "20 UAH")
        self.assertEqual(result.summary.current_savings_total, "30 UAH")
        self.assertIn("Authorized shared goal", xml)
        self.assertNotIn("PRIVATE OUTSIDE CURRENT SCOPE", xml)
        self.assertNotIn("OTHER FAMILY SNAPSHOT", xml)

    async def test_pending_plan_keeps_authored_fields_without_revoked_account_metadata(self):
        c = self.conn
        c.membership(status="left")
        c.account(1, label="Authorized source")
        c.account(2, family_id=99, label="REVOKED TARGET LABEL", account_type="savings",
                  goal_name="REVOKED TARGET GOAL", goal_amount=Decimal("56789"))
        c.transaction(1)
        c.insert("pending_saving_tasks", id=1, tg_user_id=123, source_account_id=1,
                 target_account_id=2, amount=Decimal("5.25"), currency="UAH",
                 status="pending", created_at=datetime(2026, 9, 1))

        tasks = await ReportService(c).fetch_personal_pending_saving_tasks(123)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(Decimal(tasks[0]["amount"]), Decimal("5.25"))
        self.assertEqual(tasks[0]["source_label"], "Authorized source")
        self.assertIsNone(tasks[0]["target_label"])
        self.assertIsNone(tasks[0]["goal_name"])
        self.assertIsNone(tasks[0]["goal_amount"])
        result = await ReportService(c).build_export_xlsx(123, START, END, "Pending plan")
        _, xml = inspect_workbook(result.content)
        self.assertEqual(result.summary.planned_savings_total, "1 / 5.25 UAH")
        self.assertNotIn("REVOKED TARGET", xml)

    async def test_repeated_category_aggregates_once_in_runtime_report(self):
        for transaction_id in range(200):
            self.conn.transaction(transaction_id, type="expense", amount=Decimal("50"),
                                  category_name_snapshot="Food and groceries")

        text = await ReportService(self.conn).build_normal_report_text(123, START, END, "Month")

        self.assertEqual(text.count("Food and groceries"), 1)
        self.assertIn("• Food and groceries — <b>10 000 UAH</b>", text)
        self.assertIn("<b>Витрати:</b> 10 000 UAH", text)
        self.assertIn("<b>Баланс періоду:</b> -10 000 UAH", text)
        self.assertLessEqual(len(text.encode("utf-16-le")) // 2, 4096)

    async def test_large_report_splits_on_lines_without_losing_categories_or_html(self):
        for transaction_id in range(250):
            self.conn.transaction(transaction_id, type="expense", amount=Decimal("0.25"),
                                  category_name_snapshot=f"Category {transaction_id:03d} <food & drink> 🥑")
        text = await ReportService(self.conn).build_normal_report_text(123, START, END, "Month <test>")
        splitter = getattr(report_service, "split_report_text", None)
        self.assertTrue(callable(splitter), "Report service must expose a bounded HTML split API")

        chunks = splitter(text)

        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-16-le")) // 2, 4096)
            ET.fromstring(f"<root>{chunk}</root>")
        self.assertEqual("".join(chunks), text)
        self.assertTrue(all(chunk.endswith("\n") for chunk in chunks[:-1]))
        self.assertEqual("".join(chunks).count("• Category"), 250)

    async def test_single_oversized_fields_split_with_balanced_tags_and_whole_entities(self):
        self.conn.membership()
        long_label = "🥑 <food & drink> ' \" " * 500
        self.conn.db.execute("UPDATE users SET first_name=? WHERE tg_user_id=123", (long_label,))
        self.conn.transaction(1, family_id=99, type="expense", amount=Decimal("1.25"),
                              category_name_snapshot=long_label)
        text = await ReportService(self.conn).build_normal_report_text(
            123, START, END, long_label + "\n" + long_label, member_filter_label=long_label)

        chunks = report_service.split_report_text(text, max_length=512)

        visible_parts = []
        for chunk in chunks:
            self.assertLessEqual(len(chunk.encode("utf-16-le")) // 2, 512)
            visible_parts.extend(ET.fromstring(f"<root>{chunk}</root>").itertext())
        original_visible = "".join(ET.fromstring(f"<root>{text}</root>").itertext())
        self.assertEqual("".join(visible_parts), original_visible)

    async def test_savings_targets_match_authorized_family_accounts_not_creator(self):
        c = self.conn
        c.membership()
        c.account(1, account_type="savings", label="Private target")
        c.account(2, user_id=456, family_id=99, account_type="savings", label="Shared savings")
        c.account(3, user_id=456, family_id=99, account_type="deposit", label="Shared deposit")
        c.account(4, user_id=456, family_id=99, account_type="investment", label="Shared investment")
        c.account(5, family_id=99, account_type="main", label="Not a savings target")
        c.account(6, family_id=99, account_type="savings", is_active=False, label="Archived")
        c.account(7, family_id=98, account_type="savings", label="Previous family target")

        targets = await SavingsService(c).get_active_target_accounts(123)

        self.assertEqual([row["id"] for row in targets], [2, 3, 4])

    async def test_savings_target_scope_is_rechecked_after_revocation(self):
        c = self.conn
        c.membership()
        c.account(1, account_type="savings")
        c.account(2, family_id=99, account_type="savings")
        c.account(3, family_id=99, user_id=456, account_type="savings")
        c.account(4, user_id=456, account_type="savings")
        c.account(5, account_type="savings", is_active=False)
        savings = SavingsService(c)
        accounts = AccountService(c)
        self.assertEqual([r["id"] for r in await savings.get_active_target_accounts(123)], [2, 3])
        self.assertIsNotNone(await accounts.get_active_account_by_id(123, 3))
        c.db.execute("UPDATE family_members SET status='removed'")

        self.assertEqual([r["id"] for r in await savings.get_active_target_accounts(123)], [1])
        self.assertIsNone(await accounts.get_active_account_by_id(123, 2))
        self.assertIsNone(await accounts.get_active_account_by_id(123, 3))
        self.assertIsNotNone(await accounts.get_active_account_by_id(123, 1))

    async def test_inactive_family_does_not_grant_current_snapshots_or_targets(self):
        c = self.conn
        c.membership(family_status="inactive", role="owner")
        c.account(1, account_type="savings")
        c.account(2, family_id=99, account_type="savings")
        self.assertEqual([r["id"] for r in await SavingsService(c).get_active_target_accounts(123)], [1])
        self.assertEqual([r["id"] for r in await ReportService(c).fetch_personal_export_accounts(123)], [1])

    async def test_category_totals_keep_currency_type_member_and_active_row_boundaries(self):
        c = self.conn
        c.membership()
        c.insert("categories", id=7, name="Shared <category>")
        cases = {
            ("expense", "UAH"): [Decimal("0.10"), Decimal("0.20")],
            ("expense", "USD"): [Decimal("1.10"), Decimal("1.20")],
            ("income", "UAH"): [Decimal("7.01"), Decimal("2.09")],
            ("income", "USD"): [Decimal("4.01"), Decimal("1.99")],
        }
        tx_id = 0
        for (kind, currency), amounts in cases.items():
            for amount in amounts:
                tx_id += 1
                c.transaction(tx_id, family_id=99, type=kind, currency=currency.lower(), amount=amount,
                              category_id=7, category_name_snapshot="Old category name")
        ignored = [dict(flow_kind=kind) for kind in ("transfer", "debt", "adjustment")]
        ignored += [dict(is_deleted=True), dict(deleted_at=datetime(2026, 9, 2)),
                    dict(created_by_user_id=456, tg_user_id=456), dict(date=END), dict(family_id=98)]
        for fields in ignored:
            tx_id += 1
            row = dict(family_id=99, type="expense", amount=Decimal("99999"), category_name_snapshot="Excluded row")
            row.update(fields)
            c.transaction(tx_id, **row)
        rates = SimpleNamespace(rates={"UAH": Decimal("1"), "USD": Decimal("40")})
        with patch.object(report_service, "get_latest_rates", return_value=rates):
            text = await ReportService(c).build_normal_report_text(
                123, START, END, "Scope", member_filter_user_id=123, member_filter_label="Mine")

        self.assertEqual(text.count("• Shared &lt;category&gt;"), 4)
        self.assertNotIn("Old category name", text)
        self.assertNotIn("Excluded row", text)
        expense_section = text.split("<b>Витрати за категоріями:</b>")[1].split("<b>Доходи за категоріями:</b>")[0]
        income_section = text.split("<b>Доходи за категоріями:</b>")[1].split("<b>За учасниками:</b>")[0]
        for (kind, currency), amounts in cases.items():
            section = expense_section if kind == "expense" else income_section
            self.assertIn(f"<b>{format_money(sum(amounts, Decimal('0')), currency)}</b>", section)

    def test_split_api_rejects_unsafe_size_limits(self):
        for limit in (-1, 0, 31, 4097, 1.5, "4096", None, True):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                report_service.split_report_text("Small report", max_length=limit)

    def test_split_api_empty_short_and_exact_boundary_html(self):
        self.assertEqual(report_service.split_report_text(""), [])
        self.assertEqual(report_service.split_report_text("<b>Short &amp; safe 🥑</b>"), ["<b>Short &amp; safe 🥑</b>"])
        for length in range(1, 100):
            text = "<b>" + "a" * length + "\n" + "b" * length + "</b>\n<i>End &amp; 🥑</i>"
            for limit in (32, 64):
                with self.subTest(length=length, limit=limit):
                    chunks = report_service.split_report_text(text, max_length=limit)
                    visible = []
                    for chunk in chunks:
                        self.assertLessEqual(len(chunk.encode("utf-16-le")) // 2, limit)
                        value = "".join(ET.fromstring(f"<root>{chunk}</root>").itertext())
                        self.assertTrue(value.strip(), "Telegram rejects whitespace-only messages")
                        visible.append(value)
                    self.assertEqual("".join(visible), "".join(ET.fromstring(f"<root>{text}</root>").itertext()))


if __name__ == "__main__":
    unittest.main()
