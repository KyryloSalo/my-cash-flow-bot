"""Real PostgreSQL acceptance for draft atomicity and balance concurrency.

Uses only explicit loopback /debt_fin_batch1 and random per-test schemas.
"""
from __future__ import annotations
import ast
import asyncio
import os
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_debt_fin_batch1_postgres_regression as pgfixture
from account_service import AccountService
from ai_transaction_draft_service import AiTransactionDraftService
from transaction_service import TransactionService, SUCCESS_TRANSACTION_STATUSES


@unittest.skipUnless(pgfixture.DSN, "Set explicit disposable VYDNO_DEBT_TEST_DSN for real PostgreSQL tests")
class FinancialPostgresAcceptanceTests(unittest.IsolatedAsyncioTestCase):
    account = pgfixture.DebtFinBatch1PostgresTests.account
    extra_connection = pgfixture.DebtFinBatch1PostgresTests.extra_connection
    asyncTearDown = pgfixture.DebtFinBatch1PostgresTests.asyncTearDown

    async def asyncSetUp(self):
        await pgfixture.DebtFinBatch1PostgresTests.asyncSetUp(self)
        tree = ast.parse((Path(__file__).resolve().parents[1] / "bot_main.py").read_text(encoding="utf-8-sig"))
        for node in sorted(ast.walk(tree), key=lambda n: getattr(n, "lineno", 0)):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                sql = node.value.strip()
                if sql.startswith("CREATE TABLE IF NOT EXISTS ai_transaction_drafts ("):
                    await self.conn.execute(sql)
        self.account_id = await self.account("1000")
        self.draft_id = await self.conn.fetchval("""
            INSERT INTO ai_transaction_drafts(tg_user_id, source, telegram_file_unique_id,
                status, transaction_date, tx_type, amount, currency, account_id, category_id)
            VALUES (42, 'synthetic', 'synthetic-file', 'pending', CURRENT_DATE, 'expense', 100, 'UAH', $1, 1)
            RETURNING id
        """, self.account_id)

    async def expense(self, conn, amount="100"):
        result = await TransactionService(conn).commit_normal_transaction(
            42, transaction_date=date.today(), kind="expense", amount=Decimal(amount),
            currency="UAH", account_id=self.account_id, category_id=1, category_label="Synthetic",
            comment="Synthetic isolated test", source="test",
        )
        self.assertIn(result.status, SUCCESS_TRANSACTION_STATUSES)
        return result

    async def test_completion_failure_rolls_back_real_ledger_and_balance(self):
        service = AiTransactionDraftService(self.conn)
        async def broken(*args):
            raise RuntimeError("injected completion failure")
        service.mark_completed = broken
        with self.assertRaisesRegex(RuntimeError, "injected completion failure"):
            async with service.atomic_confirmation(self.draft_id, 42) as guard:
                await self.expense(self.conn)
                guard.succeeded = True
        self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions"), 0)
        self.assertEqual(await self.conn.fetchval("SELECT balance FROM accounts WHERE id=$1", self.account_id), Decimal("1000"))
        self.assertEqual(await self.conn.fetchval("SELECT status FROM ai_transaction_drafts WHERE id=$1", self.draft_id), "pending")
        async with AiTransactionDraftService(self.conn).atomic_confirmation(self.draft_id, 42) as guard:
            await self.expense(self.conn)
            guard.succeeded = True
        self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions"), 1)
        self.assertEqual(await self.conn.fetchval("SELECT balance FROM accounts WHERE id=$1", self.account_id), Decimal("900"))

    async def test_concurrent_single_and_batch_confirmations_write_exactly_one_set(self):
        for size in (1, 2):
            with self.subTest(batch_size=size):
                await self.conn.execute("UPDATE ai_transaction_drafts SET status='pending' WHERE id=$1", self.draft_id)
                before = await self.conn.fetchval("SELECT count(*) FROM transactions")
                second = await self.extra_connection()
                locked = asyncio.Event()
                release = asyncio.Event()
                async def first():
                    async with AiTransactionDraftService(self.conn).atomic_confirmation(self.draft_id, 42) as guard:
                        locked.set()
                        await release.wait()
                        for _ in range(size):
                            await self.expense(self.conn, "10")
                        guard.succeeded = True
                async def duplicate():
                    with self.assertRaisesRegex(ValueError, "draft_not_pending"):
                        async with AiTransactionDraftService(second).atomic_confirmation(self.draft_id, 42):
                            self.fail("duplicate writer entered locked completed draft")
                one = asyncio.create_task(first())
                await asyncio.wait_for(locked.wait(), 5)
                two = asyncio.create_task(duplicate())
                try:
                    observer = await self.extra_connection()
                    for _ in range(100):
                        blockers = await observer.fetchval("SELECT cardinality(pg_blocking_pids($1))", second.get_server_pid())
                        if blockers:
                            break
                        await asyncio.sleep(0.02)
                    self.assertTrue(blockers, "Second writer must actually wait on PostgreSQL lock")
                finally:
                    release.set()
                    await asyncio.wait_for(asyncio.gather(one, two), 10)
                self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions"), before + size)

    async def test_next_writer_observes_committed_balance_correction(self):
        await AccountService(self.conn).create_balance_correction(42, self.account_id, Decimal("1000"), Decimal("1200"), "UAH")
        other = await self.extra_connection()
        result = await self.expense(other)
        self.assertEqual(result.previous_balance, Decimal("1200"))
        self.assertEqual(result.new_balance, Decimal("1100"))
        self.assertEqual(await self.conn.fetchval("SELECT balance FROM accounts WHERE id=$1", self.account_id), Decimal("1100"))
        self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions"), 2)

if __name__ == "__main__":
    unittest.main()
