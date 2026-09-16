from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from savings_service import SavingsService  # noqa: E402


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self, *, accounts_by_id: dict[int, dict]) -> None:
        self.accounts_by_id = {account_id: dict(row) for account_id, row in accounts_by_id.items()}
        for account in self.accounts_by_id.values():
            account.setdefault("account_type", "main")
            account.setdefault("non_negative_account_type", account["account_type"])
            account.setdefault("credit_limit", None)
            account.setdefault("monthly_interest_rate", None)
        self.pending_tasks: dict[int, dict] = {}
        self.next_task_id = 1
        self.transactions: list[dict] = []
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []

    def transaction(self) -> DummyTx:
        return DummyTx()

    def _build_task_row(self, task_id: int) -> dict | None:
        task = self.pending_tasks.get(task_id)
        if task is None:
            return None
        source = self.accounts_by_id[int(task["source_account_id"])]
        target = self.accounts_by_id[int(task["target_account_id"])]
        return {
            **task,
            "source_label": source["label"],
            "source_currency": source["currency"],
            "target_label": target["label"],
            "target_currency": target["currency"],
            "target_balance": target["balance"],
        }

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO pending_saving_tasks"):
            task_id = self.next_task_id
            self.next_task_id += 1
            self.pending_tasks[task_id] = {
                "id": task_id,
                "tg_user_id": int(args[0]),
                "income_transaction_id": args[1],
                "source_account_id": int(args[2]),
                "target_account_id": int(args[3]),
                "amount": Decimal(str(args[4])),
                "currency": str(args[5]),
                "percent_from_income": args[6],
                "income_amount": args[7],
                "status": "pending",
                "remind_at": None,
                "reminded_at": None,
                "created_at": datetime.now(),
                "completed_at": None,
                "cancelled_at": None,
            }
            return {
                "id": task_id,
                "amount": self.pending_tasks[task_id]["amount"],
                "currency": self.pending_tasks[task_id]["currency"],
            }
        if "FROM pending_saving_tasks pst" in normalized:
            return self._build_task_row(int(args[1]))
        return None

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM accounts" in normalized and "id = ANY($2::bigint[])" in normalized:
            account_ids = [int(value) for value in args[1]]
            return [
                dict(self.accounts_by_id[account_id])
                for account_id in sorted(account_ids)
                if account_id in self.accounts_by_id and self.accounts_by_id[account_id].get("is_active", True)
            ]
        return []

    async def execute(self, query: str, *args) -> None:
        self.execute_calls.append((query, args))
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE accounts SET balance=$3"):
            account_id = int(args[1])
            account = self.accounts_by_id[account_id]
            account["balance"] = Decimal(str(args[2]))
            if len(args) > 3:
                account["account_type"] = args[3]
                if "credit_limit=$5" in normalized:
                    account["credit_limit"] = args[4]
                    account["non_negative_account_type"] = args[5]
                else:
                    account["non_negative_account_type"] = args[4]
            return
        if normalized.startswith("UPDATE pending_saving_tasks SET status='completed'"):
            task = self.pending_tasks.get(int(args[1]))
            if task is not None:
                task["status"] = "completed"
                task["completed_at"] = datetime.now()
                task["remind_at"] = None
            return
        if normalized.startswith("UPDATE pending_saving_tasks SET status='cancelled'"):
            task = self.pending_tasks.get(int(args[1]))
            if task is not None:
                task["status"] = "cancelled"
                task["cancelled_at"] = datetime.now()
                task["remind_at"] = None
            return
        if normalized.startswith("UPDATE pending_saving_tasks SET amount=$3"):
            task = self.pending_tasks.get(int(args[1]))
            if task is not None and task["status"] == "pending":
                task["amount"] = Decimal(str(args[2]))
            return
        if normalized.startswith("UPDATE pending_saving_tasks SET remind_at=$3, reminded_at=NULL"):
            task = self.pending_tasks.get(int(args[1]))
            if task is not None and task["status"] == "pending":
                task["remind_at"] = args[2]
                task["reminded_at"] = None
            return
        if "INSERT INTO transactions" in normalized:
            self.transactions.append(
                {
                    "tg_user_id": int(args[0]),
                    "type": "transfer",
                    "amount": Decimal(str(args[3])),
                    "currency": str(args[4]),
                    "to_amount": Decimal(str(args[5])),
                    "to_currency": str(args[6]),
                    "comment": args[10],
                    "source": str(args[11]),
                    "account_id": int(args[12]),
                    "from_account_id": int(args[13]),
                    "to_account_id": int(args[14]),
                    "flow_kind": "transfer",
                    "transfer_subtype": args[15],
                }
            )


class SavingsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_pending_task_does_not_change_balances(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Main", "currency": "UAH", "balance": Decimal("1000.00"), "is_active": True},
                2: {"id": 2, "label": "Podushka", "currency": "UAH", "balance": Decimal("200.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)

        result = await service.create_pending_task(
            123,
            income_transaction_id=None,
            source_account_id=1,
            target_account_id=2,
            amount=Decimal("100"),
            currency="uah",
            percent_from_income=Decimal("10"),
            income_amount=Decimal("1000"),
        )

        self.assertEqual(result.id, 1)
        self.assertEqual(result.amount, Decimal("100.00"))
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(conn.accounts_by_id[1]["balance"], Decimal("1000.00"))
        self.assertEqual(conn.accounts_by_id[2]["balance"], Decimal("200.00"))
        self.assertEqual(conn.pending_tasks[1]["status"], "pending")
        self.assertEqual(conn.transactions, [])

    async def test_confirm_pending_task_moves_money_and_marks_completed(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Main", "currency": "UAH", "balance": Decimal("1000.00"), "is_active": True},
                2: {"id": 2, "label": "Podushka", "currency": "UAH", "balance": Decimal("200.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)
        await service.create_pending_task(
            123,
            income_transaction_id=None,
            source_account_id=1,
            target_account_id=2,
            amount=Decimal("100"),
            currency="UAH",
            percent_from_income=Decimal("10"),
            income_amount=Decimal("1000"),
        )

        result = await service.confirm_pending_task(123, 1, fx_rate=None, rate_source=None)

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts_by_id[1]["balance"], Decimal("900.00"))
        self.assertEqual(conn.accounts_by_id[2]["balance"], Decimal("300.00"))
        self.assertEqual(conn.pending_tasks[1]["status"], "completed")
        self.assertEqual(len(conn.transactions), 1)
        self.assertEqual(conn.transactions[0]["type"], "transfer")
        self.assertEqual(conn.transactions[0]["flow_kind"], "transfer")
        self.assertEqual(conn.transactions[0]["transfer_subtype"], "savings_transfer")

    async def test_record_completed_transfer_moves_money_without_pending_task(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Main", "currency": "UAH", "balance": Decimal("1000.00"), "is_active": True},
                2: {"id": 2, "label": "На подорож", "currency": "UAH", "balance": Decimal("0.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)

        result = await service.record_completed_transfer(
            123,
            source_account_id=1,
            target_account_id=2,
            source_currency="UAH",
            target_currency="UAH",
            amount=Decimal("500"),
            fx_rate=None,
            rate_source=None,
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts_by_id[1]["balance"], Decimal("500.00"))
        self.assertEqual(conn.accounts_by_id[2]["balance"], Decimal("500.00"))
        self.assertEqual(len(conn.pending_tasks), 0)
        self.assertEqual(len(conn.transactions), 1)
        self.assertEqual(conn.transactions[0]["type"], "transfer")
        self.assertEqual(conn.transactions[0]["transfer_subtype"], "savings_transfer")

    async def test_record_completed_withdraw_uses_savings_withdraw_subtype(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "На подорож", "currency": "UAH", "balance": Decimal("600.00"), "is_active": True},
                2: {"id": 2, "label": "Monobank +4444", "currency": "UAH", "balance": Decimal("100.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)

        result = await service.record_completed_transfer(
            123,
            source_account_id=1,
            target_account_id=2,
            source_currency="UAH",
            target_currency="UAH",
            amount=Decimal("250"),
            fx_rate=None,
            rate_source=None,
            transfer_subtype="savings_withdraw",
            comment="saving withdraw",
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts_by_id[1]["balance"], Decimal("350.00"))
        self.assertEqual(conn.accounts_by_id[2]["balance"], Decimal("350.00"))
        self.assertEqual(len(conn.transactions), 1)
        self.assertEqual(conn.transactions[0]["transfer_subtype"], "savings_withdraw")
        self.assertEqual(conn.transactions[0]["type"], "transfer")

    async def test_confirm_pending_task_with_insufficient_funds_keeps_task_pending(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Main", "currency": "UAH", "balance": Decimal("50.00"), "is_active": True},
                2: {"id": 2, "label": "Podushka", "currency": "UAH", "balance": Decimal("200.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)
        await service.create_pending_task(
            123,
            income_transaction_id=None,
            source_account_id=1,
            target_account_id=2,
            amount=Decimal("100"),
            currency="UAH",
            percent_from_income=Decimal("10"),
            income_amount=Decimal("1000"),
        )

        result = await service.confirm_pending_task(123, 1, fx_rate=None, rate_source=None)

        self.assertEqual(result.status, "credit_limit_required")
        self.assertEqual(conn.accounts_by_id[1]["balance"], Decimal("50.00"))
        self.assertEqual(conn.accounts_by_id[2]["balance"], Decimal("200.00"))
        self.assertEqual(conn.pending_tasks[1]["status"], "pending")
        self.assertEqual(conn.transactions, [])

    async def test_cancel_task_marks_pending_task_cancelled(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                1: {"id": 1, "label": "Main", "currency": "UAH", "balance": Decimal("1000.00"), "is_active": True},
                2: {"id": 2, "label": "Podushka", "currency": "UAH", "balance": Decimal("200.00"), "is_active": True},
            }
        )
        service = SavingsService(conn)
        await service.create_pending_task(
            123,
            income_transaction_id=None,
            source_account_id=1,
            target_account_id=2,
            amount=Decimal("100"),
            currency="UAH",
            percent_from_income=Decimal("10"),
            income_amount=Decimal("1000"),
        )

        cancelled_task = await service.cancel_task(123, 1)

        self.assertIsNotNone(cancelled_task)
        self.assertEqual(conn.pending_tasks[1]["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
