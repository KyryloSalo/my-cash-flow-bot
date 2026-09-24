from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date, datetime
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from transaction_service import TransactionService  # noqa: E402


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConn:
    def __init__(self, *, accounts_by_id: dict[int, dict]) -> None:
        self.accounts_by_id = accounts_by_id
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.transactions: list[dict] = []

    def transaction(self) -> DummyTx:
        return DummyTx()

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        account_id = int(args[1]) if len(args) > 1 else None
        if account_id is None:
            return None
        account = self.accounts_by_id.get(account_id)
        return dict(account) if account is not None else None

    async def execute(self, query: str, *args) -> None:
        self.execute_calls.append((query, args))
        normalized = " ".join(query.split())
        if "UPDATE accounts SET balance=$3, account_type=$4, credit_limit=$5, non_negative_account_type=$6, updated_at=now()" in normalized:
            account_id = int(args[1])
            balance = Decimal(str(args[2]))
            account = self.accounts_by_id.get(account_id)
            if account is not None:
                account["balance"] = balance
                account["account_type"] = str(args[3])
                account["credit_limit"] = args[4]
                account["non_negative_account_type"] = str(args[5])
                account["updated_at"] = datetime.now()
            return
        if "INSERT INTO transactions" in normalized:
            if "family_id, created_by_user_id, updated_by_user_id" in normalized:
                date_arg_index = 2
                type_arg_index = 3
                amount_arg_index = 4
                currency_arg_index = 5
                comment_arg_index = 6
                source_arg_index = 7
                account_id_arg_index = 8
                category_id_arg_index = 9
                category_label_arg_index = 10
                family_id = args[1]
                created_by_user_id = int(args[0])
            else:
                date_arg_index = 1
                type_arg_index = 2
                amount_arg_index = 3
                currency_arg_index = 4
                comment_arg_index = 5
                source_arg_index = 6
                account_id_arg_index = 7
                category_id_arg_index = 8
                category_label_arg_index = 9
                family_id = None
                created_by_user_id = int(args[0])
            self.transactions.append(
                {
                    "tg_user_id": int(args[0]),
                    "family_id": family_id,
                    "created_by_user_id": created_by_user_id,
                    "date": args[date_arg_index],
                    "type": str(args[type_arg_index]),
                    "amount": Decimal(str(args[amount_arg_index])),
                    "currency": str(args[currency_arg_index]),
                    "comment": args[comment_arg_index],
                    "source": str(args[source_arg_index]),
                    "account_id": int(args[account_id_arg_index]),
                    "category_id": int(args[category_id_arg_index]),
                    "category_label_arg": args[category_label_arg_index],
                    "flow_kind": "normal",
                    "query": query,
                }
            )

    async def fetchval(self, query: str, *args):
        if "INSERT INTO transactions" in " ".join(query.split()):
            await self.execute(query, *args)
            return len(self.transactions)
        return None


class TransactionServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_commit_normal_income_inserts_row_and_increases_balance(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                11: {
                    "id": 11,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 20),
            kind="income",
            amount=Decimal("25"),
            currency="uah",
            account_id=11,
            category_id=22,
            category_label="Зарплата",
            comment="зарплата",
            source="text",
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.kind, "income")
        self.assertEqual(result.amount, Decimal("25.00"))
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(result.account["label"], "Main")
        self.assertEqual(conn.accounts_by_id[11]["balance"], Decimal("125.00"))
        self.assertEqual(len(conn.execute_calls), 3)
        insert_query, insert_args = conn.execute_calls[0]
        self.assertIn("INSERT INTO transactions", insert_query)
        self.assertIn("flow_kind", insert_query)
        self.assertIn("category_name_snapshot", insert_query)
        self.assertEqual(insert_args[-1], "Зарплата")
        self.assertEqual(conn.transactions[0]["flow_kind"], "normal")
        self.assertEqual(result.transaction_id, 1)
        self.assertEqual(conn.transactions[0]["category_label_arg"], "Зарплата")
        outbox_query, outbox_args = conn.execute_calls[1]
        self.assertIn("INSERT INTO gamification_event_outbox", outbox_query)
        self.assertEqual(outbox_args[0], 1)
        self.assertEqual(outbox_args[1], 123)
        self.assertEqual(outbox_args[4], "text")

    async def test_commit_normal_expense_inserts_row_and_decreases_balance(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                12: {
                    "id": 12,
                    "tg_user_id": 123,
                    "label": "Cash",
                    "currency": "UAH",
                    "account_type": "cash",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "cash",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 21),
            kind="expense",
            amount=Decimal("40"),
            currency="UAH",
            account_id=12,
            category_id=33,
            category_label="Кафе",
            comment="обід",
            source="text",
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.kind, "expense")
        self.assertEqual(result.amount, Decimal("40.00"))
        self.assertEqual(result.currency, "UAH")
        self.assertEqual(conn.accounts_by_id[12]["balance"], Decimal("60.00"))
        self.assertEqual(len(conn.execute_calls), 3)
        self.assertEqual(conn.transactions[0]["flow_kind"], "normal")
        self.assertEqual(conn.transactions[0]["type"], "expense")
        self.assertEqual(conn.transactions[0]["category_label_arg"], "Кафе")

    async def test_commit_normal_transaction_rejects_currency_account_mismatch(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                15: {
                    "id": 15,
                    "tg_user_id": 123,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 21),
            kind="income",
            amount=Decimal("40"),
            currency="USDT",
            account_id=15,
            category_id=33,
            category_label="Зарплата",
            comment="зарплата",
            source="text",
        )

        self.assertEqual(result.status, "currency_account_mismatch")
        self.assertEqual(result.currency, "USDT")
        self.assertEqual(result.account["label"], "Main")
        self.assertEqual(conn.accounts_by_id[15]["balance"], Decimal("100"))
        self.assertEqual(conn.execute_calls, [])
        self.assertEqual(conn.transactions, [])


    async def test_expense_entering_negative_balance_auto_switches_to_credit_without_limit(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                13: {
                    "id": 13,
                    "tg_user_id": 123,
                    "label": "Mono",
                    "currency": "UAH",
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 22),
            kind="expense",
            amount=Decimal("150"),
            currency="UAH",
            account_id=13,
            category_id=44,
            category_label="Техніка",
            comment="покупка",
            source="text",
        )

        self.assertEqual(result.status, "account_switched_to_credit")
        self.assertEqual(result.previous_balance, Decimal("100"))
        self.assertEqual(result.projected_balance, Decimal("-50.00"))
        self.assertEqual(result.new_balance, Decimal("-50.00"))
        self.assertIsNone(result.credit_limit)
        self.assertEqual(conn.accounts_by_id[13]["balance"], Decimal("-50.00"))
        self.assertEqual(conn.accounts_by_id[13]["account_type"], "credit")
        self.assertEqual(len(conn.execute_calls), 3)

    async def test_expense_can_switch_account_to_credit_with_limit_override(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                14: {
                    "id": 14,
                    "tg_user_id": 123,
                    "label": "Mono",
                    "currency": "UAH",
                    "account_type": "main",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 22),
            kind="expense",
            amount=Decimal("150"),
            currency="UAH",
            account_id=14,
            category_id=44,
            category_label="Техніка",
            comment="покупка",
            source="text",
            credit_limit_override=Decimal("1000"),
        )

        self.assertEqual(result.status, "account_switched_to_credit")
        self.assertEqual(result.new_balance, Decimal("-50.00"))
        self.assertEqual(result.new_account_type, "credit")
        self.assertEqual(conn.accounts_by_id[14]["account_type"], "credit")
        self.assertEqual(conn.accounts_by_id[14]["credit_limit"], Decimal("1000.00"))

    async def test_expense_respects_existing_credit_limit_when_present(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                16: {
                    "id": 16,
                    "tg_user_id": 123,
                    "label": "Mono",
                    "currency": "UAH",
                    "account_type": "main",
                    "credit_limit": Decimal("300"),
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("100"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 22),
            kind="expense",
            amount=Decimal("450"),
            currency="UAH",
            account_id=16,
            category_id=44,
            category_label="РўРµС…РЅС–РєР°",
            comment="РїРѕРєСѓРїРєР°",
            source="text",
        )

        self.assertEqual(result.status, "credit_limit_exceeded")
        self.assertEqual(result.projected_balance, Decimal("-350.00"))
        self.assertEqual(result.credit_limit, Decimal("300.00"))
        self.assertEqual(conn.execute_calls, [])

    async def test_expense_on_credit_account_without_limit_can_go_more_negative(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                17: {
                    "id": 17,
                    "tg_user_id": 123,
                    "label": "Mono",
                    "currency": "UAH",
                    "account_type": "credit",
                    "credit_limit": None,
                    "monthly_interest_rate": None,
                    "non_negative_account_type": "main",
                    "balance": Decimal("-50"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 22),
            kind="expense",
            amount=Decimal("25"),
            currency="UAH",
            account_id=17,
            category_id=44,
            category_label="РўРµС…РЅС–РєР°",
            comment="РїРѕРєСѓРїРєР°",
            source="text",
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.projected_balance, Decimal("-75.00"))
        self.assertEqual(result.new_account_type, "credit")
        self.assertEqual(conn.accounts_by_id[17]["balance"], Decimal("-75.00"))
        self.assertEqual(conn.accounts_by_id[17]["account_type"], "credit")

    async def test_income_can_restore_credit_account_to_non_negative_type(self) -> None:
        conn = DummyConn(
            accounts_by_id={
                15: {
                    "id": 15,
                    "tg_user_id": 123,
                    "label": "Mono",
                    "currency": "UAH",
                    "account_type": "credit",
                    "credit_limit": Decimal("1000"),
                    "monthly_interest_rate": Decimal("3.5"),
                    "non_negative_account_type": "main",
                    "balance": Decimal("-20"),
                }
            }
        )
        service = TransactionService(conn)

        result = await service.commit_normal_transaction(
            123,
            transaction_date=date(2026, 1, 23),
            kind="income",
            amount=Decimal("30"),
            currency="UAH",
            account_id=15,
            category_id=55,
            category_label="Повернення",
            comment="поповнення",
            source="text",
        )

        self.assertEqual(result.status, "account_restored_from_credit")
        self.assertEqual(result.new_balance, Decimal("10.00"))
        self.assertEqual(result.new_account_type, "main")
        self.assertEqual(conn.accounts_by_id[15]["account_type"], "main")


if __name__ == "__main__":
    unittest.main()
