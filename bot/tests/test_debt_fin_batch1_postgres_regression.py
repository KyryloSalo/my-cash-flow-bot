"""FIN-001/002/005 against disposable PostgreSQL, never an application DB.

Run with VYDNO_DEBT_TEST_DSN=postgresql://...@127.0.0.1:PORT/debt_fin_batch1.
Each test owns a random schema and drops only that schema. No bot startup or
configuration imports: the financial table DDL comes from the shared schema.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
import unittest
from decimal import Decimal
from urllib.parse import urlparse
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import asyncpg
from debt_service import DebtService
from runtime_schema import CORE_STATEMENTS

D = Decimal
DSN = os.environ.get("VYDNO_DEBT_TEST_DSN", "")


def financial_schema_sql() -> list[str]:
    """Use the shared runtime-schema source used by bot and Django."""
    statements = []
    for table in (
        "accounts",
        "transactions",
        "debts",
        "debt_payments",
        "debt_invites",
        "ai_transaction_drafts",
        "ai_transaction_draft_items",
    ):
        for statement in CORE_STATEMENTS:
            sql = statement.strip()
            if sql.startswith(f"CREATE TABLE IF NOT EXISTS {table} (") or sql.startswith(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
            ):
                statements.append(sql)
    return statements


FINANCIAL_SCHEMA_SQL = financial_schema_sql() if DSN else []


@unittest.skipUnless(DSN, "Set explicit disposable VYDNO_DEBT_TEST_DSN for real PostgreSQL tests")
class DebtFinBatch1PostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        parsed = urlparse(DSN)
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or parsed.path != "/debt_fin_batch1":
            raise RuntimeError("Refusing non-local or non-disposable debt test database")
        self.schema = "debt_fin_" + uuid4().hex
        self.conn = await asyncpg.connect(DSN, timeout=5, command_timeout=10)
        await self.conn.execute(f'CREATE SCHEMA "{self.schema}"')
        await self.conn.execute(f'SET search_path TO "{self.schema}"')
        self.extra_connections = []
        await self.conn.execute("""
            CREATE TABLE users (tg_user_id BIGINT PRIMARY KEY);
            CREATE TABLE families (id BIGINT PRIMARY KEY, status TEXT NOT NULL);
            CREATE TABLE family_members (
                id BIGSERIAL PRIMARY KEY, family_id BIGINT REFERENCES families(id),
                user_id BIGINT REFERENCES users(tg_user_id), role TEXT, status TEXT,
                joined_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            INSERT INTO users VALUES (42), (43), (99);
            INSERT INTO families VALUES (7, 'active');
        """)
        for sql in FINANCIAL_SCHEMA_SQL:
            await self.conn.execute(sql)
        self.service = DebtService(self.conn)

    async def asyncTearDown(self):
        for conn in self.extra_connections:
            await conn.close()
        await self.conn.execute('SET search_path TO pg_catalog')
        await self.conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        await self.conn.close()

    async def extra_connection(self):
        conn = await asyncpg.connect(DSN, timeout=5, command_timeout=10)
        await conn.execute(f'SET search_path TO "{self.schema}"')
        self.extra_connections.append(conn)
        return conn

    async def family(self):
        await self.conn.execute("""
            INSERT INTO family_members (family_id, user_id, role, status)
            VALUES (7, 42, 'owner', 'active'), (7, 43, 'member', 'active')
        """)

    async def account(self, balance="10000", currency="UAH", *, family_id=None,
                      account_type="main", limit=None, restore="main", owner=42):
        return await self.conn.fetchval("""
            INSERT INTO accounts (tg_user_id, family_id, label, currency, account_type,
                starting_balance, balance, credit_limit, non_negative_account_type, is_active)
            VALUES ($1, $2, 'synthetic', $3, $4, $5, $5, $6, $7, true) RETURNING id
        """, owner, family_id, currency, account_type, D(balance), D(limit) if limit is not None else None, restore)

    async def snapshot(self):
        return {
            table: [dict(row) for row in await self.conn.fetch(f"SELECT * FROM {table} ORDER BY id")]
            for table in ("accounts", "debts", "debt_payments", "transactions", "debt_invites")
        }

    async def debt(self, account_id, *, direction="payable", amount="100", currency="USD", **kwargs):
        result = await self.service.create_debt(
            42, counterparty_name="Synthetic", direction=direction, debt_amount=D(amount),
            debt_currency=currency, account_id=account_id, **kwargs,
        )
        self.assertEqual(result.status, "completed")
        return int(result.debt["id"])

    async def test_fin001_stale_fx_confirmation_has_no_writes(self):
        account_id = await self.account()
        for fx_mode in ("explicit_account_amount", "rate_only"):
            for partial in (None, True, False):
                with self.subTest(fx_mode=fx_mode, legacy_allow_partial=partial):
                    debt_id = await self.debt(account_id, exchange_rate=D("40"))
                    earlier = await self.service.record_repayment(
                        42, debt_id=debt_id, account_id=account_id,
                        payment_amount=D("80"), payment_currency="USD", exchange_rate=D("40"),
                    )
                    self.assertEqual(earlier.status, "completed")
                    before = await self.snapshot()
                    kwargs = {"exchange_rate": D("40")}
                    if fx_mode == "explicit_account_amount":
                        kwargs["account_amount"] = D("4000")
                    if partial is not None:
                        kwargs["allow_partial"] = partial
                    result = await self.service.record_repayment(
                        42, debt_id=debt_id, account_id=account_id,
                        payment_amount=D("100"), payment_currency="USD", **kwargs,
                    )
                    self.assertEqual(result.status, "over_limit")
                    self.assertEqual(result.required_amount, D("20.00"))
                    self.assertEqual(result.required_currency, "USD")
                    self.assertEqual(await self.snapshot(), before)
                    revised = await self.service.record_repayment(
                        42, debt_id=debt_id, account_id=account_id, payment_amount=D("20"),
                        payment_currency="USD", account_amount=D("800"), exchange_rate=D("40"),
                    )
                    self.assertEqual(revised.status, "completed")
                    self.assertEqual(revised.payment["amount"], D("20.00"))
                    self.assertEqual(revised.transaction["amount"], D("800.00"))
                    self.assertEqual(revised.transaction["original_amount"], D("20.00"))
                    self.assertEqual(revised.transaction["exchange_rate"], D("40"))
                    self.assertEqual(revised.debt["remaining_amount"], D("0.00"))

    async def test_fin002_delete_reverses_each_actual_account(self):
        for family_mode, currency, archived in ((False, "UAH", False), (False, "USD", False),
                                                (True, "USD", True)):
            with self.subTest(family=family_mode, repayment_currency=currency, archived=archived):
                if family_mode:
                    await self.family()
                family_id = 7 if family_mode else None
                origin = await self.account("1000", family_id=family_id)
                receiving = await self.account("0", currency, family_id=family_id)
                debt_id = await self.debt(origin, direction="receivable", amount="1000", currency="UAH")
                result = await self.service.record_repayment(
                    43 if family_mode else 42, debt_id=debt_id, account_id=receiving,
                    payment_amount=D("400"), payment_currency="UAH",
                    **({"exchange_rate": D("40")} if currency == "USD" else {}),
                )
                self.assertEqual(result.status, "completed")
                if archived:
                    await self.conn.execute("UPDATE accounts SET is_active=false WHERE id=ANY($1::bigint[])", [origin, receiving])
                deleted = await self.service.delete_debt(43 if family_mode else 42, debt_id)
                self.assertEqual(deleted.status, "completed")
                balances = dict(await self.conn.fetch("SELECT id, balance FROM accounts WHERE id=ANY($1::bigint[])", [origin, receiving]))
                self.assertEqual(balances, {origin: D("1000.00"), receiving: D("0.00")})
                for table in ("debts", "debt_payments", "transactions"):
                    key = "id" if table == "debts" else "debt_id"
                    self.assertEqual(await self.conn.fetchval(f"SELECT count(*) FROM {table} WHERE {key}=$1", debt_id), 0)
                before_repeat = await self.snapshot()
                self.assertEqual((await self.service.delete_debt(43 if family_mode else 42, debt_id)).status, "debt_missing")
                self.assertEqual(await self.snapshot(), before_repeat)

    async def test_fin002_missing_repayment_account_rejects_entire_deletion(self):
        origin = await self.account("1000")
        receiving = await self.account("0")
        debt_id = await self.debt(origin, direction="receivable", amount="1000", currency="UAH")
        await self.service.record_repayment(42, debt_id=debt_id, account_id=receiving,
                                            payment_amount=D("400"), payment_currency="UAH")
        await self.conn.execute("DELETE FROM accounts WHERE id=$1", receiving)
        before = await self.snapshot()
        result = await self.service.delete_debt(42, debt_id)
        self.assertEqual(result.status, "account_missing")
        self.assertEqual(await self.snapshot(), before)

    async def test_fin002_accountless_legacy_debt_can_be_deleted_without_balance_change(self):
        origin = await self.account("1000")
        debt_id = await self.debt(origin, direction="receivable", amount="100", currency="UAH")
        await self.conn.execute("UPDATE accounts SET balance=1000 WHERE id=$1", origin)
        await self.conn.execute("UPDATE debts SET account_id=NULL WHERE id=$1", debt_id)
        await self.conn.execute("UPDATE transactions SET account_id=NULL WHERE debt_id=$1", debt_id)

        result = await self.service.delete_debt(42, debt_id)

        self.assertEqual(result.status, "completed")
        self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM debts WHERE id=$1", debt_id), 0)
        self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions WHERE debt_id=$1", debt_id), 0)
        self.assertEqual(await self.conn.fetchval("SELECT balance FROM accounts WHERE id=$1", origin), D("1000.00"))

    async def test_fin002_currency_mismatch_rejects_entire_deletion(self):
        origin = await self.account("1000")
        debt_id = await self.debt(origin, direction="receivable", amount="1000", currency="UAH")
        await self.conn.execute("UPDATE transactions SET currency='USD' WHERE debt_id=$1", debt_id)
        before = await self.snapshot()
        result = await self.service.delete_debt(42, debt_id)
        self.assertEqual(result.status, "invalid_payload")
        self.assertEqual(await self.snapshot(), before)

    async def test_fin002_deletion_locks_accounts_in_id_order(self):
        receiving = await self.account("0")  # lowest id; deletion must wait here first
        origin = await self.account("1000")
        debt_id = await self.debt(origin, direction="receivable", amount="1000", currency="UAH")
        await self.service.record_repayment(42, debt_id=debt_id, account_id=receiving,
                                            payment_amount=D("400"), payment_currency="UAH")
        holder = await self.extra_connection()
        worker = await self.extra_connection()
        task = None
        try:
            async with holder.transaction():
                await holder.fetchrow("SELECT id FROM accounts WHERE id=$1 FOR UPDATE", receiving)
                task = asyncio.create_task(DebtService(worker).delete_debt(42, debt_id))
                for _ in range(200):
                    blocked = await self.conn.fetchval(
                        "SELECT cardinality(pg_blocking_pids($1)) > 0", worker.get_server_pid()
                    )
                    if blocked or task.done():
                        break
                    await asyncio.sleep(0.01)
                self.assertFalse(task.done(), "Deletion did not lock the repayment account")
                self.assertTrue(blocked, "Did not observe PostgreSQL blocking on the lowest account")
                async with self.conn.transaction():
                    await self.conn.fetchrow("SELECT id FROM accounts WHERE id=$1 FOR UPDATE NOWAIT", origin)
            self.assertEqual((await asyncio.wait_for(task, 5)).status, "completed")
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_fin005_explicit_credit_limit_blocks_debt_debits_without_writes(self):
        for family_mode in (False, True):
            if family_mode:
                await self.family()
            for action in ("lend", "repay_payable"):
                for limit in ("50", "0"):
                    with self.subTest(family=family_mode, action=action, limit=limit):
                        family_id = 7 if family_mode else None
                        payer = await self.account("100", family_id=family_id, limit=limit)
                        if action == "repay_payable":
                            origin = await self.account("0", family_id=family_id)
                            debt_id = await self.debt(origin, amount="200", currency="UAH")
                        before = await self.snapshot()
                        if action == "lend":
                            result = await self.service.create_debt(
                                43 if family_mode else 42, counterparty_name="Synthetic", direction="receivable",
                                debt_amount=D("200"), debt_currency="UAH", account_id=payer,
                            )
                        else:
                            result = await self.service.record_repayment(
                                43 if family_mode else 42, debt_id=debt_id, account_id=payer,
                                payment_amount=D("200"), payment_currency="UAH",
                            )
                        self.assertEqual(result.status, "credit_limit_exceeded")
                        self.assertEqual(await self.snapshot(), before)

    async def test_fin005_permitted_debits_enter_credit_mode_atomically(self):
        for family_mode in (False, True):
            if family_mode:
                await self.family()
            for action in ("lend", "repay_payable"):
                for limit in (None, "50"):
                    with self.subTest(family=family_mode, action=action, limit=limit):
                        family_id = 7 if family_mode else None
                        payer = await self.account("100", family_id=family_id, account_type="savings", restore="savings", limit=limit)
                        if action == "lend":
                            result = await self.service.create_debt(
                                43 if family_mode else 42, counterparty_name="Synthetic", direction="receivable",
                                debt_amount=D("150"), debt_currency="UAH", account_id=payer,
                            )
                        else:
                            origin = await self.account("0", family_id=family_id)
                            debt_id = await self.debt(origin, amount="150", currency="UAH")
                            result = await self.service.record_repayment(
                                43 if family_mode else 42, debt_id=debt_id, account_id=payer,
                                payment_amount=D("150"), payment_currency="UAH",
                            )
                        self.assertEqual(result.status, "completed")
                        row = await self.conn.fetchrow("SELECT * FROM accounts WHERE id=$1", payer)
                        self.assertEqual(row["balance"], D("-50.00"))
                        self.assertEqual(row["account_type"], "credit")
                        self.assertEqual(row["non_negative_account_type"], "savings")
                        self.assertEqual(row["credit_limit"], D(limit) if limit is not None else None)
                        self.assertEqual(result.transaction["flow_kind"], "debt")
                        self.assertNotIn(result.transaction["type"], {"income", "expense"})

    async def test_fin005_incoming_debt_money_restores_non_negative_type(self):
        for family_mode in (False, True):
            if family_mode:
                await self.family()
            for action in ("borrow", "repay_receivable"):
                for amount in ("50", "75"):
                    with self.subTest(family=family_mode, action=action, amount=amount):
                        family_id = 7 if family_mode else None
                        receiving = await self.account("-50", family_id=family_id, account_type="credit", restore="savings", limit="50")
                        if action == "borrow":
                            result = await self.service.create_debt(
                                43 if family_mode else 42, counterparty_name="Synthetic", direction="payable",
                                debt_amount=D(amount), debt_currency="UAH", account_id=receiving,
                            )
                        else:
                            origin = await self.account("100", family_id=family_id)
                            debt_id = await self.debt(origin, direction="receivable", amount=amount, currency="UAH")
                            result = await self.service.record_repayment(
                                43 if family_mode else 42, debt_id=debt_id, account_id=receiving,
                                payment_amount=D(amount), payment_currency="UAH",
                            )
                        self.assertEqual(result.status, "completed")
                        row = await self.conn.fetchrow("SELECT * FROM accounts WHERE id=$1", receiving)
                        self.assertEqual(row["balance"], D(amount) - D("50"))
                        self.assertEqual(row["account_type"], "savings")
                        self.assertEqual(row["non_negative_account_type"], "savings")
                        self.assertEqual(row["credit_limit"], D("50"))

    async def test_fin005_deletion_updates_credit_mode_and_enforces_limit(self):
        account_id = await self.account("100", account_type="savings", restore="savings", limit="50")
        debt_id = await self.debt(account_id, direction="receivable", amount="150", currency="UAH")
        result = await self.service.delete_debt(42, debt_id)
        self.assertEqual(result.status, "completed")
        row = await self.conn.fetchrow("SELECT * FROM accounts WHERE id=$1", account_id)
        self.assertEqual((row["balance"], row["account_type"]), (D("100"), "savings"))
        debt_id = await self.debt(account_id, amount="200", currency="UAH")
        # Unrelated spending cannot let debt deletion break the credit-limit invariant.
        await self.conn.execute("UPDATE accounts SET balance=0 WHERE id=$1", account_id)
        before = await self.snapshot()
        result = await self.service.delete_debt(42, debt_id)
        self.assertEqual(result.status, "credit_limit_exceeded")
        self.assertEqual(await self.snapshot(), before)

    async def test_fin005_deletion_reversal_cannot_exceed_credit_limit(self):
        account_id = await self.account("0", limit="100")
        debt_id = await self.debt(account_id, direction="payable", amount="100", currency="UAH")
        await self.conn.execute("UPDATE accounts SET balance=-50 WHERE id=$1", account_id)
        before = await self.snapshot()

        result = await self.service.delete_debt(42, debt_id)

        self.assertEqual(result.status, "credit_limit_exceeded")
        self.assertEqual(await self.snapshot(), before)

    async def test_fin005_account_write_failure_rolls_back_ledger_and_debt(self):
        payer = await self.account("100")
        origin = await self.account("0")
        debt_id = await self.debt(origin, amount="200", currency="UAH")
        await self.conn.execute("""
            CREATE FUNCTION reject_account_write() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'synthetic account write failure'; END $$;
            CREATE TRIGGER debt_test_write_failure BEFORE UPDATE ON accounts
            FOR EACH ROW EXECUTE FUNCTION reject_account_write();
        """)
        for action in ("create", "repay", "delete"):
            with self.subTest(action=action):
                before = await self.snapshot()
                with self.assertRaisesRegex(asyncpg.RaiseError, "synthetic account write failure"):
                    if action == "create":
                        await self.service.create_debt(42, counterparty_name="Synthetic", direction="receivable",
                                                       debt_amount=D("150"), debt_currency="UAH", account_id=payer)
                    elif action == "repay":
                        await self.service.record_repayment(42, debt_id=debt_id, account_id=payer,
                                                            payment_amount=D("150"), payment_currency="UAH")
                    else:
                        await self.service.delete_debt(42, debt_id)
                self.assertEqual(await self.snapshot(), before)


    async def wait_for_lock(self, worker, task):
        for _ in range(200):
            if await self.conn.fetchval("SELECT cardinality(pg_blocking_pids($1)) > 0", worker.get_server_pid()):
                return
            self.assertFalse(task.done(), "Mutation completed without waiting for its row lock")
            await asyncio.sleep(0.01)
        self.fail("PostgreSQL did not expose the expected blocked writer")

    async def test_fin001_family_confirmation_rechecks_remaining_after_real_lock_wait(self):
        await self.family()
        account_id = await self.account(family_id=7)
        debt_id = await self.debt(account_id, exchange_rate=D("40"))
        earlier = await self.extra_connection()
        stale = await self.extra_connection()
        task = None
        try:
            async with earlier.transaction():
                await earlier.fetchrow("SELECT id FROM debts WHERE id=$1 FOR UPDATE", debt_id)
                task = asyncio.create_task(DebtService(stale).record_repayment(
                    43, debt_id=debt_id, account_id=account_id, payment_amount=D("100"),
                    payment_currency="USD", account_amount=D("4000"), exchange_rate=D("40"),
                ))
                await self.wait_for_lock(stale, task)
                result = await DebtService(earlier).record_repayment(
                    42, debt_id=debt_id, account_id=account_id, payment_amount=D("80"),
                    payment_currency="USD", exchange_rate=D("40"),
                )
                self.assertEqual(result.status, "completed")
            result = await asyncio.wait_for(task, 5)
            self.assertEqual(result.status, "over_limit")
            self.assertEqual(result.required_amount, D("20"))
            self.assertEqual(await self.conn.fetchval("SELECT balance FROM accounts WHERE id=$1", account_id), D("10800"))
            self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM debt_payments WHERE debt_id=$1", debt_id), 1)
            self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions WHERE debt_id=$1", debt_id), 2)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_fin005_credit_limit_is_checked_after_real_account_lock_wait(self):
        account_id = await self.account("100", limit="1000")
        holder = await self.extra_connection()
        worker = await self.extra_connection()
        task = None
        try:
            async with holder.transaction():
                await holder.fetchrow("SELECT id FROM accounts WHERE id=$1 FOR UPDATE", account_id)
                task = asyncio.create_task(DebtService(worker).create_debt(
                    42, counterparty_name="Synthetic", direction="receivable", debt_amount=D("200"),
                    debt_currency="UAH", account_id=account_id,
                ))
                await self.wait_for_lock(worker, task)
                await holder.execute("UPDATE accounts SET credit_limit=50 WHERE id=$1", account_id)
            self.assertEqual((await asyncio.wait_for(task, 5)).status, "credit_limit_exceeded")
            row = await self.conn.fetchrow("SELECT * FROM accounts WHERE id=$1", account_id)
            self.assertEqual((row["balance"], row["account_type"], row["credit_limit"]), (D("100"), "main", D("50")))
            self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM transactions"), 0)
            self.assertEqual(await self.conn.fetchval("SELECT count(*) FROM debts"), 0)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_fin002_second_account_failure_rolls_back_first_reversal(self):
        origin = await self.account("1000")
        receiving = await self.account("0")
        debt_id = await self.debt(origin, direction="receivable", amount="1000", currency="UAH")
        await self.service.record_repayment(42, debt_id=debt_id, account_id=receiving,
                                            payment_amount=D("400"), payment_currency="UAH")
        await self.conn.execute("""
            CREATE FUNCTION reject_second_reversal() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD.id::text = TG_ARGV[0] THEN RAISE EXCEPTION 'synthetic second reversal failure'; END IF;
                RETURN NEW;
            END $$;
        """)
        await self.conn.execute(f"""
            CREATE TRIGGER debt_test_second_failure BEFORE UPDATE ON accounts
            FOR EACH ROW EXECUTE FUNCTION reject_second_reversal('{receiving}');
        """)
        before = await self.snapshot()
        with self.assertRaisesRegex(asyncpg.RaiseError, "synthetic second reversal failure"):
            await self.service.delete_debt(42, debt_id)
        self.assertEqual(await self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
