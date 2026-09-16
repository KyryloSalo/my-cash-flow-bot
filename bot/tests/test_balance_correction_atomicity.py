from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from account_service import AccountService  # noqa: E402


class CorrectionConnection:
    """Strict transaction seam; the real PostgreSQL gate is separate."""

    def __init__(self, *, family_id=None, fail_update=False):
        self.account = {"id": 1, "tg_user_id": 123, "family_id": family_id, "balance": Decimal("1000"),
                        "currency": "UAH", "account_type": "main", "non_negative_account_type": "main", "is_active": True}
        self.rows = []
        self.depth = 0
        self.fail_update = fail_update
        self.events = []

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy((self.account, self.rows))
        self.depth += 1
        self.events.append("begin")
        try:
            yield
        except Exception:
            self.account, self.rows = before
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.depth -= 1

    async def fetchrow(self, query, *args):
        if "FROM family_members" in query:
            return None if self.account["family_id"] is None else {"family_id": self.account["family_id"], "role": "member"}
        if "FROM accounts" in query:
            assert self.depth > 0 and "FOR UPDATE" in query, "correction must lock its account within its own transaction"
            self.events.append("lock")
            return deepcopy(self.account)
        raise AssertionError(f"Unexpected correction query: {query}")

    async def execute(self, query, *args):
        assert self.depth > 0, "financial writes must occur inside the correction transaction"
        if "INSERT INTO transactions" in query:
            self.rows.append({"user_id": args[0], "family_id": args[1], "kind": args[3], "amount": args[4], "account_id": args[8]})
            self.events.append("ledger")
            return "INSERT 0 1"
        if "UPDATE accounts" in query:
            if self.fail_update:
                raise RuntimeError("injected balance update failure")
            self.account["balance"] = args[2]
            self.account["account_type"] = args[3]
            self.account["non_negative_account_type"] = args[4]
            self.events.append("balance")
            return "UPDATE 1"
        raise AssertionError(f"Unexpected correction write: {query}")


class BalanceCorrectionAtomicityTests(unittest.IsolatedAsyncioTestCase):
    async def test_correction_commits_ledger_and_materialized_balance_together(self):
        conn = CorrectionConnection()
        result = await AccountService(conn).create_balance_correction(123, 1, Decimal("1000"), Decimal("1200"), "UAH")
        self.assertEqual(result, (Decimal("200"), Decimal("200")))
        self.assertEqual(conn.account["balance"], Decimal("1200"))
        self.assertEqual(conn.events, ["begin", "lock", "ledger", "balance", "commit"])
        self.assertEqual(len(conn.rows), 1)

    async def test_failure_after_ledger_insert_rolls_back_every_financial_effect(self):
        conn = CorrectionConnection(fail_update=True)
        with self.assertRaisesRegex(RuntimeError, "injected balance update failure"):
            await AccountService(conn).create_balance_correction(123, 1, Decimal("1000"), Decimal("1200"), "UAH")
        self.assertEqual(conn.rows, [])
        self.assertEqual(conn.account["balance"], Decimal("1000"))
        self.assertEqual(conn.events[-1], "rollback")

    async def test_changed_preview_or_currency_is_rejected_without_adjustment(self):
        for expected_balance, currency in ((Decimal("900"), "UAH"), (Decimal("1000"), "USD")):
            with self.subTest(balance=expected_balance, currency=currency):
                conn = CorrectionConnection()
                with self.assertRaises(ValueError):
                    await AccountService(conn).create_balance_correction(123, 1, expected_balance, Decimal("1200"), currency)
                self.assertEqual(conn.rows, [])
                self.assertEqual(conn.account["balance"], Decimal("1000"))

    async def test_family_correction_preserves_scope_and_updates_dynamic_credit_type(self):
        conn = CorrectionConnection(family_id=77)
        await AccountService(conn).create_balance_correction(123, 1, Decimal("1000"), Decimal("-100"), "UAH")
        self.assertEqual(conn.rows[0]["family_id"], 77)
        self.assertEqual(conn.account["balance"], Decimal("-100"))
        self.assertEqual(conn.account["account_type"], "credit")
        self.assertEqual(conn.account["non_negative_account_type"], "main")

    async def test_unchanged_balance_is_a_locked_noop(self):
        conn = CorrectionConnection()
        self.assertEqual(await AccountService(conn).create_balance_correction(123, 1, Decimal("1000"), Decimal("1000"), "UAH"), (Decimal("0"), Decimal("0")))
        self.assertEqual(conn.rows, [])
        self.assertEqual(conn.events, ["begin", "lock", "commit"])


if __name__ == "__main__":
    unittest.main()
