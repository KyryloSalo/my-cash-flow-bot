from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from debt_service import DebtService  # noqa: E402


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DebtConn:
    def __init__(self, *, tg_user_id: int, accounts: list[dict], base_currency: str = "UAH") -> None:
        self.tg_user_id = tg_user_id
        self.family_id: int | None = None
        self.family_role = "owner"
        self.user = {
            "tg_user_id": tg_user_id,
            "base_currency": base_currency,
            "start_date": date(2026, 1, 1),
            "first_name": "Owner",
            "username": "owner",
        }
        self.users: dict[int, dict] = {tg_user_id: dict(self.user)}
        self.accounts = {int(row["id"]): dict(row) for row in accounts}
        self.debts: dict[int, dict] = {}
        self.debt_payments: dict[int, dict] = {}
        self.debt_invites: dict[int, dict] = {}
        self.transactions: dict[int, dict] = {}
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self._next_debt_id = 1
        self._next_payment_id = 1
        self._next_invite_id = 1
        self._next_tx_id = 1

    def transaction(self) -> DummyTx:
        return DummyTx()

    def _clone(self, row: dict | None) -> dict | None:
        return dict(row) if row is not None else None

    def _account(self, account_id: int) -> dict | None:
        account = self.accounts.get(account_id)
        if not account or not account.get("is_active", True):
            return None
        if self.family_id is not None:
            if account.get("family_id") == self.family_id:
                return account
            return None
        if account.get("tg_user_id") == self.tg_user_id and account.get("family_id") is None:
            return account
        return None

    def _user_row(self, user_id: int) -> dict | None:
        user = self.users.get(user_id)
        return self._clone(user)

    def _debt(self, debt_id: int) -> dict | None:
        debt = self.debts.get(debt_id)
        if not debt:
            return None
        if self.family_id is not None:
            if debt.get("family_id") == self.family_id:
                return debt
            return None
        if debt.get("tg_user_id") == self.tg_user_id and debt.get("family_id") is None:
            return debt
        return None

    def _invite_join_by_token(self, token: str) -> dict | None:
        invite = next((row for row in self.debt_invites.values() if row.get("token") == token), None)
        if invite is None:
            return None
        debt = self.debts.get(int(invite["debt_id"]))
        if debt is None:
            return None
        lender = self.users.get(int(debt["tg_user_id"]), {})
        return {
            "invite_id": int(invite["id"]),
            "debt_id": int(invite["debt_id"]),
            "token": str(invite["token"]),
            "invite_status": str(invite["status"]),
            "created_by_user_id": int(invite["created_by_user_id"]),
            "used_by_user_id": invite.get("used_by_user_id"),
            "invite_created_at": invite["created_at"],
            "invite_used_at": invite.get("used_at"),
            "expires_at": invite.get("expires_at"),
            "lender_user_id": int(debt["tg_user_id"]),
            "counterparty_name": debt.get("counterparty_name"),
            "direction": debt.get("direction"),
            "initial_amount": debt.get("initial_amount"),
            "paid_amount": debt.get("paid_amount"),
            "remaining_amount": debt.get("remaining_amount"),
            "currency": debt.get("currency"),
            "comment": debt.get("comment"),
            "debt_status": debt.get("status"),
            "borrower_user_id": debt.get("borrower_user_id"),
            "borrower_confirmed_at": debt.get("borrower_confirmed_at"),
            "reminder_enabled": debt.get("reminder_enabled"),
            "reminder_frequency": debt.get("reminder_frequency"),
            "next_reminder_at": debt.get("next_reminder_at"),
            "last_reminder_at": debt.get("last_reminder_at"),
            "lender_first_name": lender.get("first_name"),
            "lender_username": lender.get("username"),
        }

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "count(1)" in normalized and "FROM accounts" in normalized:
            return sum(1 for row in self.accounts.values() if row.get("tg_user_id") == self.tg_user_id)
        return None

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM family_members fm" in normalized and "JOIN families f" in normalized:
            if self.family_id is None:
                return None
            return {"family_id": self.family_id, "role": self.family_role}
        if "FROM users" in normalized and "WHERE tg_user_id=$1" in normalized:
            return self._user_row(int(args[0]))
        if "FROM accounts" in normalized and "WHERE family_id=$1 AND id=$2" in normalized:
            account_id = int(args[1])
            return self._clone(self._account(account_id))
        if "FROM accounts" in normalized and "WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2" in normalized:
            account_id = int(args[1])
            return self._clone(self._account(account_id))
        if "FROM accounts" in normalized and "WHERE tg_user_id=$1 AND id=$2" in normalized:
            account_id = int(args[1])
            return self._clone(self._account(account_id))
        if "FROM debts" in normalized and "WHERE family_id=$1 AND id=$2" in normalized:
            debt_id = int(args[1])
            return self._clone(self._debt(debt_id))
        if "FROM debts" in normalized and "WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2" in normalized:
            debt_id = int(args[1])
            return self._clone(self._debt(debt_id))
        if "FROM debts" in normalized and "WHERE tg_user_id=$1 AND id=$2" in normalized:
            debt_id = int(args[1])
            return self._clone(self._debt(debt_id))
        if "FROM debt_invites di" in normalized and "WHERE di.token=$1" in normalized:
            return self._clone(self._invite_join_by_token(str(args[0])))
        if "INSERT INTO debts" in normalized:
            debt_id = self._next_debt_id
            self._next_debt_id += 1
            debt = {
                "id": debt_id,
                "tg_user_id": int(args[0]),
                "family_id": args[1],
                "counterparty_name": str(args[2]),
                "direction": str(args[3]),
                "initial_amount": Decimal(str(args[4])),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal(str(args[4])),
                "currency": str(args[5]),
                "account_id": int(args[6]),
                "status": "active",
                "due_date": args[7],
                "comment": args[8],
                "created_at": datetime(2026, 5, 3, 9, 0, 0),
                "updated_at": datetime(2026, 5, 3, 9, 0, 0),
                "closed_at": None,
                "borrower_user_id": None,
                "borrower_confirmed_at": None,
                "reminder_enabled": False,
                "reminder_frequency": None,
                "next_reminder_at": None,
                "last_reminder_at": None,
                "reminder_delivery_error": None,
            }
            self.debts[debt_id] = debt
            return self._clone(debt)
        if "INSERT INTO debt_invites" in normalized:
            invite_id = self._next_invite_id
            self._next_invite_id += 1
            invite = {
                "id": invite_id,
                "debt_id": int(args[0]),
                "token": str(args[1]),
                "status": "pending",
                "created_by_user_id": int(args[2]),
                "used_by_user_id": None,
                "created_at": datetime(2026, 5, 3, 9, 0, 0),
                "used_at": None,
                "expires_at": args[3],
            }
            self.debt_invites[invite_id] = invite
            return self._clone(invite)
        if "INSERT INTO debt_payments" in normalized:
            payment_id = self._next_payment_id
            self._next_payment_id += 1
            payment = {
                "id": payment_id,
                "tg_user_id": int(args[0]),
                "family_id": args[1],
                "debt_id": int(args[2]),
                "amount": Decimal(str(args[3])),
                "currency": str(args[4]),
                "account_id": int(args[5]),
                "payment_date": args[6],
                "comment": args[7],
                "created_at": datetime(2026, 5, 3, 9, 0, 0),
            }
            self.debt_payments[payment_id] = payment
            return self._clone(payment)
        if "INSERT INTO transactions" in normalized:
            tx_id = self._next_tx_id
            self._next_tx_id += 1
            tx: dict[str, object] = {
                "id": tx_id,
                "tg_user_id": int(args[0]),
                "family_id": args[1],
                "created_by_user_id": int(args[0]),
                "updated_by_user_id": int(args[0]),
                "date": args[2],
                "type": str(args[3]),
                "amount": Decimal(str(args[4])),
                "currency": str(args[5]),
                "comment": args[6],
                "source": str(args[7]),
                "account_id": int(args[8]),
                "category_id": None,
                "category_name_snapshot": None,
                "flow_kind": "debt",
                "counterparty": str(args[9]),
                "debt_id": int(args[10]),
                "created_at": datetime(2026, 5, 3, 9, 0, 0),
            }
            if len(args) >= 15:
                tx["debt_payment_id"] = int(args[11])
                tx["original_amount"] = Decimal(str(args[12]))
                tx["original_currency"] = str(args[13])
                tx["exchange_rate"] = args[14]
            else:
                tx["original_amount"] = Decimal(str(args[11]))
                tx["original_currency"] = str(args[12])
                tx["exchange_rate"] = args[13]
            self.transactions[tx_id] = tx
            return self._clone(tx)
        if "UPDATE debts" in normalized and "SET paid_amount=$3" in normalized:
            debt_id = int(args[1])
            debt = self._debt(debt_id)
            if debt is None:
                return None
            debt["paid_amount"] = Decimal(str(args[2]))
            debt["remaining_amount"] = Decimal(str(args[3]))
            debt["status"] = str(args[4])
            debt["closed_at"] = datetime(2026, 5, 3, 9, 0, 0) if debt["status"] == "closed" else None
            if debt["status"] == "closed":
                debt["reminder_enabled"] = False
                debt["next_reminder_at"] = None
                debt["reminder_delivery_error"] = None
            debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return self._clone(debt)
        if "UPDATE debts" in normalized and "SET borrower_user_id=$3" in normalized:
            debt_id = int(args[1])
            debt = self._debt(debt_id)
            if debt is None:
                return None
            debt["borrower_user_id"] = int(args[2])
            debt["borrower_confirmed_at"] = args[3]
            debt["reminder_enabled"] = True
            debt["reminder_frequency"] = "monthly"
            debt["next_reminder_at"] = args[4]
            debt["last_reminder_at"] = None
            debt["reminder_delivery_error"] = None
            debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return self._clone(debt)
        if "UPDATE debts" in normalized and "SET counterparty_name=$3" in normalized:
            debt_id = int(args[1])
            debt = self._debt(debt_id)
            if debt is None:
                return None
            debt["counterparty_name"] = str(args[2])
            debt["initial_amount"] = Decimal(str(args[3]))
            debt["remaining_amount"] = Decimal(str(args[4]))
            debt["currency"] = str(args[5])
            debt["due_date"] = args[6]
            debt["comment"] = args[7]
            debt["status"] = str(args[8])
            debt["closed_at"] = args[9]
            debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return self._clone(debt)
        if "UPDATE debt_invites" in normalized and "SET status='used'" in normalized:
            invite = self.debt_invites.get(int(args[0]))
            if invite is None:
                return None
            invite["status"] = "used"
            invite["used_by_user_id"] = int(args[1])
            invite["used_at"] = args[2]
            return self._clone(invite)
        if "UPDATE debt_invites" in normalized and "SET status='rejected'" in normalized:
            invite = self.debt_invites.get(int(args[0]))
            if invite is None:
                return None
            invite["status"] = "rejected"
            return self._clone(invite)
        if "UPDATE debt_invites" in normalized and "SET status='expired'" in normalized:
            invite = self.debt_invites.get(int(args[0]))
            if invite is None:
                return None
            invite["status"] = "expired"
            return self._clone(invite)
        return None

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM debts" in normalized and "FROM debts d" not in normalized and "LEFT JOIN debt_payments" not in normalized:
            if self.family_id is not None:
                rows = [self._clone(row) for row in self.debts.values() if row.get("family_id") == self.family_id]
            else:
                rows = [self._clone(row) for row in self.debts.values() if row.get("tg_user_id") == self.tg_user_id and row.get("family_id") is None]
            if "direction=$2" in normalized and len(args) > 1:
                rows = [row for row in rows if row and row.get("direction") == args[1]]
            if "status = ANY($3)" in normalized and len(args) > 2:
                allowed = {str(value) for value in args[2]}
                rows = [row for row in rows if row and row.get("status") in allowed]
            elif "status = ANY($2)" in normalized and len(args) > 1:
                allowed = {str(value) for value in args[1]}
                rows = [row for row in rows if row and row.get("status") in allowed]
            if "remaining_amount > 0" in normalized:
                rows = [row for row in rows if row and Decimal(str(row.get("remaining_amount") or 0)) > 0]
            rows.sort(key=lambda row: (row["created_at"], row["id"]))
            return rows
        if "FROM debt_payments" in normalized and "LEFT JOIN" not in normalized:
            rows = [
                self._clone(row)
                for row in self.debt_payments.values()
                if (
                    (self.family_id is not None and row.get("family_id") == self.family_id and row.get("debt_id") == int(args[1]))
                    or (
                        self.family_id is None
                        and row.get("tg_user_id") == self.tg_user_id
                        and row.get("family_id") is None
                        and row.get("debt_id") == int(args[1])
                    )
                )
            ]
            rows.sort(key=lambda row: (row["payment_date"], row["id"]))
            return rows
        if "SELECT id, type, amount" in normalized and "FROM transactions" in normalized:
            debt_id = int(args[1])
            rows = [
                {"id": tx["id"], "type": tx["type"], "amount": tx["amount"], "account_id": tx["account_id"], "currency": tx["currency"]}
                for tx in self.transactions.values()
                if (
                    (self.family_id is not None and tx.get("family_id") == self.family_id and tx.get("debt_id") == debt_id)
                    or (
                        self.family_id is None
                        and tx.get("tg_user_id") == self.tg_user_id
                        and tx.get("family_id") is None
                        and tx.get("debt_id") == debt_id
                    )
                )
            ]
            rows.sort(key=lambda row: row["id"])
            return rows
        if "SELECT id" in normalized and "FROM debt_payments" in normalized:
            debt_id = int(args[1])
            rows = [
                {"id": payment["id"]}
                for payment in self.debt_payments.values()
                if (
                    (self.family_id is not None and payment.get("family_id") == self.family_id and payment.get("debt_id") == debt_id)
                    or (
                        self.family_id is None
                        and payment.get("tg_user_id") == self.tg_user_id
                        and payment.get("family_id") is None
                        and payment.get("debt_id") == debt_id
                    )
                )
            ]
            rows.sort(key=lambda row: row["id"])
            return rows
        if "FROM debts d" in normalized and "d.borrower_user_id=$1" in normalized:
            rows = []
            for row in self.debts.values():
                if row.get("borrower_user_id") != int(args[0]):
                    continue
                if row.get("direction") != "receivable":
                    continue
                if row.get("status") not in set(args[1]):
                    continue
                if Decimal(str(row.get("remaining_amount") or 0)) <= 0:
                    continue
                lender = self.users.get(int(row["tg_user_id"]), {})
                rows.append(
                    {
                        **dict(row),
                        "lender_first_name": lender.get("first_name"),
                        "lender_username": lender.get("username"),
                    }
                )
            rows.sort(key=lambda row: (row["created_at"], row["id"]))
            return rows
        if "WHERE d.reminder_enabled=true" in normalized and "FROM debts d" in normalized:
            rows = []
            now = args[0]
            for row in self.debts.values():
                if not row.get("reminder_enabled"):
                    continue
                if row.get("borrower_user_id") is None or row.get("borrower_confirmed_at") is None:
                    continue
                if Decimal(str(row.get("remaining_amount") or 0)) <= 0:
                    continue
                if row.get("status") in {"closed", "cancelled"}:
                    continue
                next_reminder = row.get("next_reminder_at")
                if next_reminder is None or next_reminder > now:
                    continue
                lender = self.users.get(int(row["tg_user_id"]), {})
                rows.append(
                    {
                        **dict(row),
                        "lender_first_name": lender.get("first_name"),
                        "lender_username": lender.get("username"),
                    }
                )
            rows.sort(key=lambda row: (row["next_reminder_at"], row["id"]))
            return rows[: int(args[1])]
        if "SELECT *" in normalized and "FROM transactions" in normalized and "flow_kind='debt'" in normalized:
            if self.family_id is not None:
                return [self._clone(row) for row in self.transactions.values() if row.get("family_id") == self.family_id]
            return [self._clone(row) for row in self.transactions.values() if row.get("tg_user_id") == self.tg_user_id and row.get("family_id") is None]
        return []

    async def execute(self, query: str, *args) -> None:
        self.execute_calls.append((query, args))
        normalized = " ".join(query.split())
        if "UPDATE accounts SET balance=$3, updated_at=now()" in normalized:
            account_id = int(args[1])
            account = self._account(account_id)
            if account is not None:
                account["balance"] = Decimal(str(args[2]))
                account["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        if "UPDATE debts SET status='cancelled'" in normalized:
            debt = self._debt(int(args[1]))
            if debt is not None:
                debt["status"] = "cancelled"
                debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        if "UPDATE debts SET status='closed'" in normalized:
            debt = self._debt(int(args[1]))
            if debt is not None:
                debt["status"] = "closed"
                debt["closed_at"] = debt["closed_at"] or datetime(2026, 5, 3, 9, 0, 0)
                debt["reminder_enabled"] = False
                debt["next_reminder_at"] = None
                debt["reminder_delivery_error"] = None
                debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        if "UPDATE debts SET last_reminder_at=$2" in normalized:
            debt = self.debts.get(int(args[0]))
            if debt is not None:
                debt["last_reminder_at"] = args[1]
                debt["next_reminder_at"] = args[2]
                debt["reminder_delivery_error"] = None
                debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        if "UPDATE debts SET reminder_delivery_error=$2" in normalized:
            debt = self.debts.get(int(args[0]))
            if debt is not None:
                debt["reminder_delivery_error"] = args[1]
                debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        if "DELETE FROM debt_payments" in normalized:
            self.debt_payments.pop(int(args[0]), None)
            return
        if "DELETE FROM transactions" in normalized:
            self.transactions.pop(int(args[0]), None)
            return
        if "DELETE FROM debts" in normalized:
            debt_id = int(args[1])
            self.debts.pop(debt_id, None)
            return
        if "UPDATE debt_invites SET status='cancelled'" in normalized:
            debt_id = int(args[0])
            exclude_id = int(args[1]) if len(args) > 1 else None
            for invite in self.debt_invites.values():
                if int(invite["debt_id"]) != debt_id or invite.get("status") != "pending":
                    continue
                if exclude_id is not None and int(invite["id"]) == exclude_id:
                    continue
                invite["status"] = "cancelled"
            return
        if "UPDATE debts SET status='cancelled', updated_at=now()" in normalized:
            debt = self._debt(int(args[1]))
            if debt is not None:
                debt["status"] = "cancelled"
                debt["updated_at"] = datetime(2026, 5, 3, 9, 0, 0)
            return
        return


class DebtServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.account_id = 1
        self.user_id = 42
        self.account = {
            "id": self.account_id,
            "tg_user_id": self.user_id,
            "label": "Main",
            "currency": "UAH",
            "account_type": "card",
            "starting_balance": Decimal("5000"),
            "balance": Decimal("5000"),
            "is_active": True,
            "created_at": datetime(2026, 5, 1, 12, 0, 0),
            "updated_at": datetime(2026, 5, 1, 12, 0, 0),
        }

    def _service(self) -> tuple[DebtConn, DebtService]:
        conn = DebtConn(tg_user_id=self.user_id, accounts=[self.account])
        return conn, DebtService(conn)

    def assert_debt_invariants(self, debt: dict) -> None:
        initial = Decimal(str(debt["initial_amount"] or 0))
        paid = Decimal(str(debt["paid_amount"] or 0))
        remaining = Decimal(str(debt["remaining_amount"] or 0))
        status = str(debt["status"] or "")

        self.assertGreaterEqual(initial, Decimal("0"))
        self.assertGreaterEqual(paid, Decimal("0"))
        self.assertGreaterEqual(remaining, Decimal("0"))
        if status not in {"cancelled", "needs_review"}:
            self.assertEqual(initial, paid + remaining)
        if status == "closed":
            self.assertEqual(remaining, Decimal("0"))
        if status == "active":
            self.assertEqual(paid, Decimal("0"))
            self.assertGreater(remaining, Decimal("0"))
        if status == "partially_paid":
            self.assertGreater(paid, Decimal("0"))
            self.assertGreater(remaining, Decimal("0"))

    async def test_create_receivable_debt_decreases_balance(self) -> None:
        conn, service = self._service()

        result = await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
            comment="Без коментаря",
            due_date=date(2026, 5, 10),
        )

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("4000.00"))
        self.assertEqual(len(conn.debts), 1)
        debt = next(iter(conn.debts.values()))
        self.assertEqual(debt["direction"], "receivable")
        self.assertEqual(debt["initial_amount"], Decimal("1000.00"))
        self.assertEqual(debt["remaining_amount"], Decimal("1000.00"))
        self.assert_debt_invariants(debt)
        self.assertEqual(len(conn.transactions), 1)
        tx = next(iter(conn.transactions.values()))
        self.assertEqual(tx["type"], "debt_given")
        self.assertEqual(tx["flow_kind"], "debt")

    async def test_create_payable_debt_has_positive_amounts(self) -> None:
        conn, service = self._service()

        result = await service.create_debt(
            self.user_id,
            counterparty_name="Іра",
            direction="payable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        self.assertEqual(result.status, "completed")
        debt = next(iter(conn.debts.values()))
        self.assertEqual(debt["direction"], "payable")
        self.assertEqual(debt["initial_amount"], Decimal("1000.00"))
        self.assertEqual(debt["paid_amount"], Decimal("0"))
        self.assertEqual(debt["remaining_amount"], Decimal("1000.00"))
        self.assertEqual(debt["status"], "active")
        self.assert_debt_invariants(debt)

    async def test_receivable_debt_appears_in_active_receivables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "active",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_receivable_debts(self.user_id)

        self.assertEqual(len(debts), 1)
        self.assertEqual(debts[0]["counterparty_name"], "Олег")
        self.assertEqual(debts[0]["remaining_amount"], Decimal("1000.00"))

    async def test_payable_debt_appears_in_active_payables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Іра",
                "direction": "payable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "active",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_payable_debts(self.user_id)

        self.assertEqual(len(debts), 1)
        self.assertEqual(debts[0]["counterparty_name"], "Іра")

    async def test_receivable_debt_does_not_appear_in_active_payables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "active",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_payable_debts(self.user_id)

        self.assertEqual(debts, [])

    async def test_payable_debt_does_not_appear_in_active_receivables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Іра",
                "direction": "payable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "active",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_receivable_debts(self.user_id)

        self.assertEqual(debts, [])

    async def test_partially_paid_receivable_appears_in_active_receivables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("400.00"),
                "remaining_amount": Decimal("600.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "partially_paid",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_receivable_debts(self.user_id)

        self.assertEqual(len(debts), 1)
        self.assertEqual(debts[0]["remaining_amount"], Decimal("600.00"))

    async def test_closed_receivable_does_not_appear_in_active_receivables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("1000.00"),
                "remaining_amount": Decimal("0.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "closed",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": datetime(2026, 5, 1, 16, 0, 0),
            }
        }

        debts = await service.list_active_receivable_debts(self.user_id)

        self.assertEqual(debts, [])

    async def test_cancelled_receivable_does_not_appear_in_active_receivables(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "cancelled",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 0, 0),
                "updated_at": datetime(2026, 5, 1, 16, 0, 0),
                "closed_at": None,
            }
        }

        debts = await service.list_active_receivable_debts(self.user_id)

        self.assertEqual(debts, [])

    async def test_negative_amounts_are_rejected_on_runtime_creation(self) -> None:
        conn, service = self._service()

        result = await service.create_debt(
            self.user_id,
            counterparty_name="Олег",
            direction="receivable",
            debt_amount=Decimal("-500"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        self.assertEqual(result.status, "invalid_payload")
        self.assertEqual(conn.debts, {})
        self.assertEqual(conn.transactions, {})
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("5000"))

    async def test_partial_repayment_updates_balances_and_status(self) -> None:
        conn, service = self._service()
        await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        debt_id = next(iter(conn.debts))
        result = await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("400"),
            payment_currency="UAH",
            comment="Part",
        )

        debt = conn.debts[debt_id]
        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("4400.00"))
        self.assertEqual(debt["paid_amount"], Decimal("400.00"))
        self.assertEqual(debt["remaining_amount"], Decimal("600.00"))
        self.assertEqual(debt["status"], "partially_paid")
        self.assert_debt_invariants(debt)
        self.assertEqual(len(conn.debt_payments), 1)
        self.assertEqual(next(iter(conn.transactions.values()))["type"], "debt_given")
        self.assertEqual(sorted(tx["type"] for tx in conn.transactions.values()), ["debt_given", "debt_repayment_in"])

    async def test_full_repayment_closes_debt_and_sets_closed_at(self) -> None:
        conn, service = self._service()
        await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        debt_id = next(iter(conn.debts))
        result = await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("1000"),
            payment_currency="UAH",
        )

        debt = conn.debts[debt_id]
        self.assertEqual(result.status, "completed")
        self.assertEqual(debt["remaining_amount"], Decimal("0.00"))
        self.assertEqual(debt["status"], "closed")
        self.assertIsNotNone(debt["closed_at"])
        self.assert_debt_invariants(debt)

    async def test_payable_debt_increases_balance_and_repayment_out_decreases_it(self) -> None:
        conn, service = self._service()
        create_result = await service.create_debt(
            self.user_id,
            counterparty_name="Іра",
            direction="payable",
            debt_amount=Decimal("5000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        debt_id = int(create_result.debt["id"])
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("10000.00"))
        self.assertEqual(conn.transactions[1]["type"], "debt_received")

        repay_result = await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("2000"),
            payment_currency="UAH",
        )

        debt = conn.debts[debt_id]
        self.assertEqual(repay_result.status, "completed")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("8000.00"))
        self.assertEqual(debt["paid_amount"], Decimal("2000.00"))
        self.assertEqual(debt["remaining_amount"], Decimal("3000.00"))
        self.assertEqual(conn.transactions[2]["type"], "debt_repayment_out")
        self.assert_debt_invariants(debt)

    async def test_over_limit_repayment_can_be_capped(self) -> None:
        conn, service = self._service()
        await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("600"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        debt_id = next(iter(conn.debts))

        result = await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("800"),
            payment_currency="UAH",
            allow_partial=False,
        )

        self.assertEqual(result.status, "over_limit")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("4400.00"))
        self.assertEqual(conn.debts[debt_id]["paid_amount"], Decimal("0"))
        self.assertEqual(conn.debts[debt_id]["remaining_amount"], Decimal("600.00"))

        capped = await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("600"),
            payment_currency="UAH",
        )
        self.assertEqual(capped.status, "completed")
        self.assertEqual(conn.debts[debt_id]["status"], "closed")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("5000.00"))

    async def test_cross_currency_debt_records_original_amount_and_rate(self) -> None:
        conn = DebtConn(
            tg_user_id=self.user_id,
            accounts=[
                {
                    "id": self.account_id,
                    "tg_user_id": self.user_id,
                    "label": "Main",
                    "currency": "UAH",
                    "account_type": "card",
                    "starting_balance": Decimal("20000"),
                    "balance": Decimal("20000"),
                    "is_active": True,
                    "created_at": datetime(2026, 5, 1, 12, 0, 0),
                    "updated_at": datetime(2026, 5, 1, 12, 0, 0),
                }
            ],
        )
        service = DebtService(conn)

        result = await service.create_debt(
            self.user_id,
            counterparty_name="Olga",
            direction="receivable",
            debt_amount=Decimal("100"),
            debt_currency="USD",
            account_id=self.account_id,
            account_amount=Decimal("3700"),
            exchange_rate=Decimal("37"),
        )

        debt = result.debt
        tx = result.transaction
        self.assertEqual(result.status, "completed")
        self.assertEqual(debt["currency"], "USD")
        self.assertEqual(debt["initial_amount"], Decimal("100.00"))
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("16300.00"))
        self.assertEqual(tx["currency"], "UAH")
        self.assertEqual(tx["original_amount"], Decimal("100.00"))
        self.assertEqual(tx["original_currency"], "USD")
        self.assertEqual(tx["exchange_rate"], Decimal("37"))

    async def test_delete_debt_rolls_back_related_transactions(self) -> None:
        conn, service = self._service()
        await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        debt_id = next(iter(conn.debts))
        await service.record_repayment(
            self.user_id,
            debt_id=debt_id,
            account_id=self.account_id,
            payment_amount=Decimal("400"),
            payment_currency="UAH",
        )

        result = await service.delete_debt(self.user_id, debt_id)

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("5000.00"))
        self.assertEqual(conn.debts, {})
        self.assertEqual(conn.debt_payments, {})
        self.assertEqual(conn.transactions, {})

    async def test_cancel_debt_keeps_record_but_marks_cancelled(self) -> None:
        conn, service = self._service()
        await service.create_debt(
            self.user_id,
            counterparty_name="Саша",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        debt_id = next(iter(conn.debts))

        result = await service.cancel_debt(self.user_id, debt_id)

        self.assertEqual(result.status, "completed")
        self.assertEqual(conn.debts[debt_id]["status"], "cancelled")
        self.assertEqual(conn.accounts[self.account_id]["balance"], Decimal("4000.00"))


    async def test_negative_receivable_rows_are_hidden_from_active_lists(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1500.00"),
                "paid_amount": Decimal("500.00"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "partially_paid",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 34, 37),
                "updated_at": datetime(2026, 5, 1, 16, 34, 37),
                "closed_at": None,
            },
            2: {
                "id": 2,
                "tg_user_id": self.user_id,
                "counterparty_name": "Максим",
                "direction": "receivable",
                "initial_amount": Decimal("500.00"),
                "paid_amount": Decimal("500.00"),
                "remaining_amount": Decimal("0.00"),
                "currency": "UAH",
                "account_id": None,
                "status": "needs_review",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 35, 13),
                "updated_at": datetime(2026, 5, 1, 16, 35, 13),
                "closed_at": None,
            },
        }

        summary = await service.build_debt_summary_text(self.user_id)
        listing = await service.build_debt_list_text(self.user_id, "receivable")

        self.assertIn("Олег", listing)
        self.assertIn("1 000", summary)
        self.assertIn("1 000", listing)
        self.assertNotIn("-500", summary)
        self.assertNotIn("Максим", listing)

    async def test_summary_uses_debt_table_not_raw_transactions_for_active_debts(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1000.00"),
                "paid_amount": Decimal("0"),
                "remaining_amount": Decimal("1000.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "active",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 34, 37),
                "updated_at": datetime(2026, 5, 1, 16, 34, 37),
                "closed_at": None,
            }
        }
        conn.transactions = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty": "Legacy",
                "debt_action": "borrow",
                "amount": Decimal("9999.00"),
                "currency": "UAH",
                "flow_kind": "debt",
            }
        }

        summary = await service.build_debt_summary_text(self.user_id)

        self.assertIn("1 000", summary)
        self.assertNotIn("Legacy", summary)
        self.assertNotIn("9 999", summary)

    async def test_closed_debt_rows_block_legacy_fallback(self) -> None:
        conn, service = self._service()
        conn.debts = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty_name": "Олег",
                "direction": "receivable",
                "initial_amount": Decimal("1500.00"),
                "paid_amount": Decimal("1500.00"),
                "remaining_amount": Decimal("0.00"),
                "currency": "UAH",
                "account_id": self.account_id,
                "status": "closed",
                "due_date": None,
                "comment": None,
                "created_at": datetime(2026, 5, 1, 16, 34, 37),
                "updated_at": datetime(2026, 5, 3, 10, 24, 49),
                "closed_at": datetime(2026, 5, 3, 10, 24, 49),
            }
        }
        conn.transactions = {
            1: {
                "id": 1,
                "tg_user_id": self.user_id,
                "counterparty": "Олег",
                "debt_action": "lend",
                "amount": Decimal("1000.00"),
                "currency": "UAH",
                "flow_kind": "debt",
            }
        }

        summary = await service.build_debt_summary_text(self.user_id)

        self.assertNotIn("Олег", summary)
        self.assertNotIn("1000", summary)
        self.assertNotIn("Legacy", summary)


    async def test_create_debt_invite_for_receivable_debt(self) -> None:
        conn, service = self._service()
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Олег",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )

        result = await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-1",
            created_by_user_id=self.user_id,
        )

        self.assertEqual(result.status, "completed")
        self.assertIsNotNone(result.invite)
        self.assertEqual(result.invite["status"], "pending")
        self.assertEqual(result.invite["token"], "invite-token-1")

    async def test_confirm_debt_invite_sets_borrower_and_blocks_reuse(self) -> None:
        conn, service = self._service()
        conn.users[99] = {"tg_user_id": 99, "first_name": "Borrower", "username": "borrower"}
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Олег",
            direction="receivable",
            debt_amount=Decimal("1000"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-2",
            created_by_user_id=self.user_id,
        )

        result = await service.confirm_debt_invite("invite-token-2", borrower_user_id=99)
        repeated = await service.confirm_debt_invite("invite-token-2", borrower_user_id=99)

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.debt["borrower_user_id"], 99)
        self.assertIsNotNone(result.debt["borrower_confirmed_at"])
        self.assertTrue(result.debt["reminder_enabled"])
        self.assertEqual(result.debt["reminder_frequency"], "monthly")
        self.assertIsNotNone(result.debt["next_reminder_at"])
        self.assertEqual(result.invite["status"], "used")
        self.assertEqual(result.invite["used_by_user_id"], 99)
        self.assertEqual(repeated.status, "invite_not_pending")

    async def test_get_debt_invite_for_update_locks_only_invite_and_debt_tables(self) -> None:
        conn, service = self._service()
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Микола",
            direction="receivable",
            debt_amount=Decimal("5"),
            debt_currency="UAH",
            account_id=1,
            comment="Тест",
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-lock",
            created_by_user_id=self.user_id,
        )

        await service.get_debt_invite("invite-token-lock", for_update=True)

        query, _args = conn.fetchrow_calls[-1]
        self.assertIn("FOR UPDATE OF di, d", " ".join(query.split()))

    async def test_confirmed_receivable_debt_texts_show_borrower_confirmation(self) -> None:
        conn, service = self._service()
        conn.users[99] = {"tg_user_id": 99, "first_name": "Borrower", "username": "borrower"}
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Микола",
            direction="receivable",
            debt_amount=Decimal("5"),
            debt_currency="UAH",
            account_id=1,
            comment="Тест",
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-status",
            created_by_user_id=self.user_id,
        )
        await service.confirm_debt_invite("invite-token-status", borrower_user_id=99)

        detail_text = await service.build_debt_detail_text(self.user_id, int(debt_result.debt["id"]))
        list_text = await service.build_debt_list_text(self.user_id, "receivable")

        self.assertIn("Підтвердження боржника:</b> підтверджено", detail_text)
        self.assertIn("Нагадування боржнику:</b> увімкнені", detail_text)
        self.assertIn("Підтвердження: підтверджено", list_text)
        self.assertIn("Нагадування: увімкнені", list_text)

    async def test_reject_debt_invite_keeps_reminders_disabled(self) -> None:
        conn, service = self._service()
        conn.users[77] = {"tg_user_id": 77, "first_name": "Guest", "username": "guest"}
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Іра",
            direction="receivable",
            debt_amount=Decimal("500"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-3",
            created_by_user_id=self.user_id,
        )

        result = await service.reject_debt_invite("invite-token-3", borrower_user_id=77)

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.invite["status"], "rejected")
        self.assertIsNone(result.debt["borrower_user_id"])
        self.assertFalse(bool(result.debt.get("reminder_enabled")))

    async def test_list_observed_payable_debts_returns_confirmed_receivable_debts(self) -> None:
        conn, service = self._service()
        conn.users[101] = {"tg_user_id": 101, "first_name": "Олег", "username": "oleh"}
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Олег",
            direction="receivable",
            debt_amount=Decimal("800"),
            debt_currency="UAH",
            account_id=self.account_id,
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-4",
            created_by_user_id=self.user_id,
        )
        await service.confirm_debt_invite("invite-token-4", borrower_user_id=101)

        observed = await service.list_observed_payable_debts(101)

        self.assertEqual(len(observed), 1)
        self.assertEqual(observed[0]["id"], int(debt_result.debt["id"]))
        self.assertEqual(observed[0]["lender_first_name"], "Owner")

    async def test_due_debt_reminders_are_found_and_marked(self) -> None:
        conn, service = self._service()
        conn.users[102] = {"tg_user_id": 102, "first_name": "Borrower", "username": "borrower"}
        debt_result = await service.create_debt(
            self.user_id,
            counterparty_name="Borrower",
            direction="receivable",
            debt_amount=Decimal("300"),
            debt_currency="UAH",
            account_id=self.account_id,
            comment="Тест",
        )
        await service.create_debt_invite(
            self.user_id,
            int(debt_result.debt["id"]),
            token="invite-token-5",
            created_by_user_id=self.user_id,
        )
        await service.confirm_debt_invite("invite-token-5", borrower_user_id=102)
        conn.debts[int(debt_result.debt["id"])]["next_reminder_at"] = datetime(2026, 5, 1, 9, 0, 0)

        due = await service.list_due_debt_reminders(datetime(2026, 5, 3, 9, 0, 0))
        await service.mark_debt_reminder_sent(int(debt_result.debt["id"]), datetime(2026, 5, 3, 9, 0, 0))
        await service.mark_debt_reminder_failed(int(debt_result.debt["id"]), "delivery failed")

        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["borrower_user_id"], 102)
        self.assertEqual(conn.debts[int(debt_result.debt["id"])]["reminder_delivery_error"], "delivery failed")


if __name__ == "__main__":
    unittest.main()
