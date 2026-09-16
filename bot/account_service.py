from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import asyncpg

from finance_scope import FinanceScope, get_current_finance_scope
from finance import (
    default_non_negative_account_type,
    format_money,
    normalize_account_type,
    normalize_currency,
    parse_decimal_amount,
    quantize_money,
    quantize_rate,
    resolve_runtime_account_type,
)

ACCOUNT_SELECT_FIELDS = """
id, tg_user_id, family_id, created_by_user_id, label, currency, account_type,
starting_balance, balance, is_active, created_at, updated_at,
goal_name, goal_amount, goal_date,
credit_limit, monthly_interest_rate, non_negative_account_type
"""


class AccountService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def _scope(self, tg_user_id: int) -> FinanceScope:
        return await get_current_finance_scope(self.conn, tg_user_id)

    async def create_account(
        self,
        tg_user_id: int,
        *,
        label: str,
        currency: str,
        account_type: str,
        starting_balance: Decimal,
        goal_name: str | None = None,
        goal_amount: Decimal | None = None,
        goal_date=None,
        credit_limit: Decimal | None = None,
        monthly_interest_rate: Decimal | None = None,
        non_negative_account_type: str | None = None,
    ) -> int:
        scope = await self._scope(tg_user_id)
        normalized_starting_balance = quantize_money(Decimal(str(starting_balance)))
        normalized_account_type = normalize_account_type(account_type)
        fallback_account_type = default_non_negative_account_type(non_negative_account_type or normalized_account_type)
        runtime_account_type = resolve_runtime_account_type(
            balance=normalized_starting_balance,
            non_negative_account_type=fallback_account_type,
            current_account_type=normalized_account_type,
        )
        normalized_credit_limit = None
        if credit_limit is not None:
            normalized_credit_limit = quantize_money(Decimal(str(credit_limit)))
        normalized_monthly_interest_rate = None
        if monthly_interest_rate is not None:
            normalized_monthly_interest_rate = quantize_rate(Decimal(str(monthly_interest_rate)))
        row = await self.conn.fetchrow(
            """
            INSERT INTO accounts (
              tg_user_id, family_id, created_by_user_id,
              label, currency, account_type,
              starting_balance, balance, is_active, updated_at,
              goal_name, goal_amount, goal_date,
              credit_limit, monthly_interest_rate, non_negative_account_type
            )
            VALUES ($1, $2, $1, $3, $4, $5, $6, $6, true, now(), $7, $8, $9, $10, $11, $12)
            RETURNING id
            """,
            tg_user_id,
            scope.family_id,
            label,
            currency,
            runtime_account_type,
            normalized_starting_balance,
            goal_name,
            goal_amount,
            goal_date,
            normalized_credit_limit,
            normalized_monthly_interest_rate,
            fallback_account_type,
        )
        return int(row["id"])

    async def get_accounts(self, tg_user_id: int) -> list[tuple[int, str]]:
        rows = await self.get_accounts_full(tg_user_id)
        return [(int(row["id"]), f"{row['label']} ({row['currency']})") for row in rows]

    async def get_accounts_full(self, tg_user_id: int) -> list[asyncpg.Record]:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            return await self.conn.fetch(
                """
                SELECT
                    """
                + ACCOUNT_SELECT_FIELDS
                + """
                FROM accounts
                WHERE family_id=$1 AND is_active=true
                ORDER BY created_at ASC, id ASC
                """,
                int(scope.family_id),
            )
        return await self.conn.fetch(
            """
            SELECT
                """
            + ACCOUNT_SELECT_FIELDS
            + """
            FROM accounts
            WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true
            ORDER BY created_at ASC, id ASC
            """,
            tg_user_id,
        )

    async def get_active_account_by_id(
        self,
        tg_user_id: int,
        account_id: int,
        *,
        for_update: bool = False,
    ) -> asyncpg.Record | None:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            sql = (
                """
                SELECT
                    """
                + ACCOUNT_SELECT_FIELDS
                + """
                FROM accounts
                WHERE family_id=$1 AND id=$2 AND is_active=true
                """
                + (" FOR UPDATE" if for_update else "")
            )
            return await self.conn.fetchrow(sql, int(scope.family_id), account_id)
        sql = (
            """
            SELECT
                """
            + ACCOUNT_SELECT_FIELDS
            + """
            FROM accounts
            WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2 AND is_active=true
            """
            + (" FOR UPDATE" if for_update else "")
        )
        return await self.conn.fetchrow(sql, tg_user_id, account_id)

    async def rename_account(self, tg_user_id: int, account_id: int, label: str) -> None:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET label=$3, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                account_id,
                label,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET label=$3, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            tg_user_id,
            account_id,
            label,
        )

    async def update_goal(
        self,
        tg_user_id: int,
        account_id: int,
        *,
        goal_amount: Decimal | None,
        goal_date,
    ) -> None:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET goal_amount=$3, goal_date=$4, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                account_id,
                goal_amount,
                goal_date,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET goal_amount=$3, goal_date=$4, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            tg_user_id,
            account_id,
            goal_amount,
            goal_date,
        )

    async def update_account_type(
        self,
        tg_user_id: int,
        account_id: int,
        account_type: str,
        *,
        non_negative_account_type: str | None = None,
    ) -> None:
        scope = await self._scope(tg_user_id)
        normalized_account_type = normalize_account_type(account_type)
        normalized_non_negative_type = default_non_negative_account_type(non_negative_account_type or normalized_account_type)
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET account_type=$3, updated_at=now(), non_negative_account_type=$4 WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                account_id,
                normalized_account_type,
                normalized_non_negative_type,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET account_type=$3, updated_at=now(), non_negative_account_type=$4 WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            tg_user_id,
            account_id,
            normalized_account_type,
            normalized_non_negative_type,
        )

    async def update_credit_settings(
        self,
        tg_user_id: int,
        account_id: int,
        *,
        credit_limit: Decimal | None,
        monthly_interest_rate: Decimal | None,
    ) -> None:
        scope = await self._scope(tg_user_id)
        normalized_credit_limit = None if credit_limit is None else quantize_money(Decimal(str(credit_limit)))
        normalized_monthly_interest_rate = None if monthly_interest_rate is None else quantize_rate(Decimal(str(monthly_interest_rate)))
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET credit_limit=$3, monthly_interest_rate=$4, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                account_id,
                normalized_credit_limit,
                normalized_monthly_interest_rate,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET credit_limit=$3, monthly_interest_rate=$4, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            tg_user_id,
            account_id,
            normalized_credit_limit,
            normalized_monthly_interest_rate,
        )

    async def archive_account(self, tg_user_id: int, account_id: int) -> None:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET is_active=false, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                account_id,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET is_active=false, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            tg_user_id,
            account_id,
        )

    async def create_balance_correction(
        self,
        tg_user_id: int,
        account_id: int,
        current_balance: Decimal,
        target_balance: Decimal,
        currency: str,
    ) -> tuple[Decimal, Decimal]:
        expected_balance = parse_decimal_amount(str(current_balance), allow_negative=True)
        target_balance = parse_decimal_amount(str(target_balance), allow_negative=True)
        if expected_balance is None or target_balance is None:
            raise ValueError("invalid_balance")
        currency = normalize_currency(currency)
        async with self.conn.transaction():
            account = await self.get_active_account_by_id(tg_user_id, account_id, for_update=True)
            if account is None:
                raise ValueError("account_missing")
            locked_balance = quantize_money(Decimal(str(account["balance"] or 0)))
            if locked_balance != expected_balance or normalize_currency(str(account["currency"])) != currency:
                raise ValueError("stale_balance_preview")
            delta = quantize_money(target_balance - locked_balance)
            if delta == 0:
                return delta, Decimal("0")
            # Even two valid endpoint balances can produce an oversized ledger delta.
            tx_amount = parse_decimal_amount(str(abs(delta)))
            if tx_amount is None:
                raise ValueError("invalid_balance_delta")
            tx_type = "income" if delta > 0 else "expense"
            tx_comment = (
                f"Корекція балансу до {format_money(target_balance, currency)} "
                f"(було {format_money(locked_balance, currency)})"
            )
            family_id = account["family_id"]
            await self.conn.execute(
                """
                INSERT INTO transactions (
                  tg_user_id, family_id, created_by_user_id, date, type, amount, currency,
                  comment, source,
                  account_id, category_id, category_name_snapshot,
                  flow_kind
                )
                VALUES (
                  $1, $2, $1, $3, $4, $5, $6,
                  $7, $8,
                  $9, NULL, NULL,
                  'adjustment'
                )
                """,
                tg_user_id, family_id, datetime.now().date(), tx_type, tx_amount,
                currency, tx_comment, "balance_correction", account_id,
            )
            non_negative_type = default_non_negative_account_type(
                str(account["non_negative_account_type"] or account["account_type"] or "main")
            )
            runtime_type = resolve_runtime_account_type(
                balance=target_balance,
                non_negative_account_type=non_negative_type,
                current_account_type=str(account["account_type"] or "main"),
            )
            scope_filter = "family_id=$1" if family_id is not None else "tg_user_id=$1 AND family_id IS NULL"
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, account_type=$4, non_negative_account_type=$5, updated_at=now() "
                f"WHERE {scope_filter} AND id=$2",
                family_id if family_id is not None else tg_user_id,
                account_id, target_balance, runtime_type, non_negative_type,
            )
            return delta, tx_amount

    async def recalculate_account_balances(self, tg_user_id: int | None = None) -> None:
        params: list[object] = []
        user_filter = ""
        tx_filter = ""
        if tg_user_id is not None:
            scope = await self._scope(tg_user_id)
            if scope.is_family:
                user_filter = "WHERE a.family_id=$1"
                tx_filter = "AND t.family_id=$1"
                params.append(int(scope.family_id))
            else:
                user_filter = "WHERE a.tg_user_id=$1 AND a.family_id IS NULL"
                tx_filter = "AND t.tg_user_id=$1 AND t.family_id IS NULL"
                params.append(tg_user_id)
        await self.conn.execute(
            f"""
            WITH ledger AS (
              SELECT
                a.id,
                COALESCE(
                  NULLIF(a.non_negative_account_type, ''),
                  CASE
                    WHEN a.account_type = 'credit' THEN 'main'
                    ELSE a.account_type
                  END,
                  'main'
                ) AS effective_non_negative_account_type,
                COALESCE(a.starting_balance, 0)
                + COALESCE(SUM(CASE
                    WHEN t.account_id = a.id AND t.type = 'income' THEN t.amount
                    ELSE 0
                  END), 0)
                - COALESCE(SUM(CASE
                    WHEN t.account_id = a.id AND t.type = 'expense' THEN t.amount
                    ELSE 0
                  END), 0)
                - COALESCE(SUM(CASE
                    WHEN t.from_account_id = a.id AND t.type = 'transfer' THEN t.amount
                    ELSE 0
                  END), 0)
                + COALESCE(SUM(CASE
                    WHEN t.to_account_id = a.id AND t.type = 'transfer' THEN COALESCE(t.to_amount, t.amount)
                    ELSE 0
                  END), 0)
                + COALESCE(SUM(CASE
                    WHEN t.account_id = a.id AND t.type IN ('debt_received', 'debt_repayment_in') THEN t.amount
                    ELSE 0
                  END), 0)
                - COALESCE(SUM(CASE
                    WHEN t.account_id = a.id AND t.type IN ('debt_given', 'debt_repayment_out') THEN t.amount
                    ELSE 0
                  END), 0) AS computed_balance
              FROM accounts a
              LEFT JOIN transactions t
                ON (
                  t.account_id = a.id
                  OR t.from_account_id = a.id
                  OR t.to_account_id = a.id
                )
               AND t.deleted_at IS NULL
               AND COALESCE(t.is_deleted, false)=false
               {tx_filter}
              {user_filter}
              GROUP BY a.id, a.starting_balance, a.account_type, a.non_negative_account_type
            )
            UPDATE accounts AS a
            SET balance = ledger.computed_balance,
                account_type = CASE
                  WHEN ledger.computed_balance < 0 THEN 'credit'
                  ELSE ledger.effective_non_negative_account_type
                END,
                non_negative_account_type = ledger.effective_non_negative_account_type,
                updated_at = now()
            FROM ledger
            WHERE ledger.id = a.id
            """,
            *params,
        )
