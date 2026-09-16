from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from collections import defaultdict
from typing import Literal

import asyncpg

from finance import (
    calculate_transfer_amount,
    default_non_negative_account_type,
    escape_html,
    format_money,
    normalize_account_type,
    normalize_currency,
    quantize_money,
    resolve_runtime_account_type,
)
from finance_scope import FinanceScope, get_current_finance_scope


DebtDirection = Literal["receivable", "payable"]
DebtStatus = Literal["active", "partially_paid", "closed", "cancelled", "needs_review"]
DebtAmountKind = Literal["debt", "account"]


@dataclass(frozen=True)
class DebtOperationResult:
    status: Literal[
        "completed",
        "invalid_payload",
        "account_missing",
        "debt_missing",
        "debt_locked",
        "over_limit",
        "credit_limit_exceeded",
        "not_owner",
        "not_found",
        "invite_missing",
        "invite_not_pending",
        "invite_owner_blocked",
        "invite_conflict",
    ]
    debt: asyncpg.Record | None = None
    payment: asyncpg.Record | None = None
    transaction: asyncpg.Record | None = None
    account: asyncpg.Record | None = None
    invite: asyncpg.Record | None = None
    message: str | None = None
    required_amount: Decimal | None = None
    required_currency: str | None = None
    required_rate: Decimal | None = None


def _normalize_direction(direction: str | None) -> DebtDirection | None:
    value = (direction or "").strip().lower()
    if value in {"receivable", "payable"}:
        return value  # type: ignore[return-value]
    return None


def _normalize_status(status: str | None) -> DebtStatus | None:
    value = (status or "").strip().lower()
    if value in {"active", "partially_paid", "closed", "cancelled", "needs_review"}:
        return value  # type: ignore[return-value]
    return None


def _status_from_remaining(*, paid: Decimal, remaining: Decimal, current: str | None = None) -> DebtStatus:
    if _normalize_status(current) == "cancelled":
        return "cancelled"
    if remaining <= 0:
        return "closed"
    if paid > 0:
        return "partially_paid"
    return "active"


def _payment_sign(direction: str, transaction_type: str) -> int:
    if direction == "receivable":
        if transaction_type in {"debt_given"}:
            return -1
        if transaction_type in {"debt_repayment_in"}:
            return 1
    if direction == "payable":
        if transaction_type in {"debt_received"}:
            return 1
        if transaction_type in {"debt_repayment_out"}:
            return -1
    return 0


def _exceeds_credit_limit(account: asyncpg.Record, new_balance: Decimal) -> bool:
    credit_limit = account.get("credit_limit")
    return new_balance < 0 and credit_limit is not None and abs(new_balance) > Decimal(str(credit_limit))


def _add_one_month(value: datetime) -> datetime:
    year = value.year + (1 if value.month == 12 else 0)
    month = 1 if value.month == 12 else value.month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _person_name(first_name: object | None, username: object | None, fallback: str = "Користувач") -> str:
    first = str(first_name or "").strip()
    if first:
        return first
    username_text = str(username or "").strip().lstrip("@")
    if username_text:
        return f"@{username_text}"
    return fallback


class DebtService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def _scope(self, tg_user_id: int) -> FinanceScope:
        return await get_current_finance_scope(self.conn, tg_user_id)

    async def _get_account_for_scope(
        self,
        scope: FinanceScope,
        account_id: int,
        *,
        for_update: bool = False,
        include_archived: bool = False,
    ) -> asyncpg.Record | None:
        suffix = " FOR UPDATE" if for_update else ""
        active_clause = "" if include_archived else " AND is_active=true"
        if scope.is_family:
            return await self.conn.fetchrow(
                f"""
                SELECT id, tg_user_id, family_id, label, currency, account_type, starting_balance, balance, is_active, created_at, updated_at,
                       credit_limit, non_negative_account_type
                FROM accounts
                WHERE family_id=$1 AND id=$2{active_clause}
                {suffix}
                """,
                int(scope.family_id),
                account_id,
            )
        return await self.conn.fetchrow(
            f"""
            SELECT id, tg_user_id, family_id, label, currency, account_type, starting_balance, balance, is_active, created_at, updated_at,
                   credit_limit, non_negative_account_type
            FROM accounts
            WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2{active_clause}
            {suffix}
            """,
            int(scope.user_id),
            account_id,
        )

    async def _get_debt_for_scope(
        self,
        scope: FinanceScope,
        debt_id: int,
        *,
        for_update: bool = False,
    ) -> asyncpg.Record | None:
        suffix = " FOR UPDATE" if for_update else ""
        if scope.is_family:
            return await self.conn.fetchrow(
                f"""
                SELECT *
                FROM debts
                WHERE family_id=$1 AND id=$2
                {suffix}
                """,
                int(scope.family_id),
                debt_id,
            )
        return await self.conn.fetchrow(
            f"""
            SELECT *
            FROM debts
            WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
            {suffix}
            """,
            int(scope.user_id),
            debt_id,
        )

    async def _update_account_balance_for_scope(self, scope: FinanceScope, account: asyncpg.Record, new_balance: Decimal) -> None:
        # The caller holds this account's row lock until all ledger writes commit.
        current_type = normalize_account_type(str(account["account_type"] or "other"))
        non_negative_type = default_non_negative_account_type(
            str(account.get("non_negative_account_type") or current_type or "main")
        )
        new_type = resolve_runtime_account_type(
            balance=new_balance,
            non_negative_account_type=non_negative_type,
            current_account_type=current_type,
        )
        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, updated_at=now(), account_type=$4, credit_limit=$5, non_negative_account_type=$6 WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                int(account["id"]),
                new_balance,
                new_type,
                account.get("credit_limit"),
                non_negative_type,
            )
            return
        await self.conn.execute(
            "UPDATE accounts SET balance=$3, updated_at=now(), account_type=$4, credit_limit=$5, non_negative_account_type=$6 WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
            int(scope.user_id),
            int(account["id"]),
            new_balance,
            new_type,
            account.get("credit_limit"),
            non_negative_type,
        )

    async def repair_legacy_debts(self) -> int:
        repaired = 0
        async with self.conn.transaction():
            await self.conn.execute(
                """
                UPDATE debts
                SET initial_amount=ABS(initial_amount),
                    paid_amount=ABS(paid_amount),
                    remaining_amount=GREATEST(ABS(initial_amount) - ABS(paid_amount), 0),
                    status=CASE
                        WHEN GREATEST(ABS(initial_amount) - ABS(paid_amount), 0) = 0 THEN 'closed'
                        WHEN ABS(paid_amount) > 0 THEN 'partially_paid'
                        ELSE 'active'
                    END,
                    closed_at=CASE
                        WHEN GREATEST(ABS(initial_amount) - ABS(paid_amount), 0) = 0 THEN COALESCE(closed_at, now())
                        ELSE NULL
                    END,
                    updated_at=now()
                WHERE initial_amount < 0 OR paid_amount < 0 OR remaining_amount < 0
                """
            )

            legacy_rows = await self.conn.fetch(
                """
                SELECT id, tg_user_id, family_id, counterparty, debt_action, amount, currency, account_id, created_at
                FROM transactions
                WHERE flow_kind='debt' AND debt_id IS NULL
                  AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                ORDER BY family_id NULLS FIRST, tg_user_id, counterparty, id
                """
            )
            if not legacy_rows:
                return repaired

            grouped: dict[tuple[int | None, int, str, str], list[asyncpg.Record]] = defaultdict(list)
            for row in legacy_rows:
                action = str(row["debt_action"] or "")
                if action in {"lend", "lend_repaid"}:
                    direction = "receivable"
                elif action in {"borrow", "borrow_repaid"}:
                    direction = "payable"
                else:
                    direction = "needs_review"
                currency = normalize_currency(str(row["currency"] or "UAH")) or "UAH"
                counterparty = str(row["counterparty"] or "").strip() or "—"
                family_id = int(row["family_id"]) if row["family_id"] is not None else None
                grouped[(family_id, int(row["tg_user_id"]), counterparty, direction)].append(row)

            for (family_id, tg_user_id, counterparty, direction), rows in grouped.items():
                if family_id is not None:
                    existing_id = await self.conn.fetchval(
                        """
                        SELECT id
                        FROM debts
                        WHERE family_id=$1 AND counterparty_name=$2
                        ORDER BY created_at ASC, id ASC
                        LIMIT 1
                        """,
                        family_id,
                        counterparty,
                    )
                else:
                    existing_id = await self.conn.fetchval(
                        """
                        SELECT id
                        FROM debts
                        WHERE tg_user_id=$1 AND family_id IS NULL AND counterparty_name=$2
                        ORDER BY created_at ASC, id ASC
                        LIMIT 1
                        """,
                        tg_user_id,
                        counterparty,
                    )
                if existing_id is not None:
                    for row in rows:
                        await self._link_legacy_transaction(int(row["id"]), int(existing_id))
                    continue

                lend_total = sum(
                    quantize_money(Decimal(str(row["amount"] or 0)))
                    for row in rows
                    if str(row["debt_action"] or "") == "lend"
                )
                lend_repaid_total = sum(
                    quantize_money(Decimal(str(row["amount"] or 0)))
                    for row in rows
                    if str(row["debt_action"] or "") == "lend_repaid"
                )
                borrow_total = sum(
                    quantize_money(Decimal(str(row["amount"] or 0)))
                    for row in rows
                    if str(row["debt_action"] or "") == "borrow"
                )
                borrow_repaid_total = sum(
                    quantize_money(Decimal(str(row["amount"] or 0)))
                    for row in rows
                    if str(row["debt_action"] or "") == "borrow_repaid"
                )

                if direction == "receivable":
                    opening_total = lend_total
                    repayment_total = lend_repaid_total
                elif direction == "payable":
                    opening_total = borrow_total
                    repayment_total = borrow_repaid_total
                else:
                    opening_total = lend_total if lend_total > 0 else borrow_total
                    repayment_total = lend_repaid_total if lend_total > 0 else borrow_repaid_total

                if opening_total <= 0:
                    opening_total = repayment_total
                    status = "needs_review"
                    paid_amount = quantize_money(repayment_total)
                    remaining_amount = Decimal("0.00")
                else:
                    paid_amount = quantize_money(repayment_total)
                    remaining_amount = quantize_money(max(opening_total - paid_amount, Decimal("0")))
                    if remaining_amount <= 0:
                        status = "closed"
                    elif paid_amount > 0:
                        status = "partially_paid"
                    else:
                        status = "active"

                created_at = min((row["created_at"] for row in rows if row["created_at"] is not None), default=datetime.now())
                account_id = next((int(row["account_id"]) for row in rows if row["account_id"] is not None), None)
                if direction == "needs_review":
                    normalized_direction = "receivable" if lend_total > 0 or lend_repaid_total > 0 else "payable"
                else:
                    normalized_direction = direction

                debt = await self.conn.fetchrow(
                    """
                    INSERT INTO debts (
                      tg_user_id, family_id, counterparty_name, direction,
                      initial_amount, paid_amount, remaining_amount,
                      currency, account_id, status, due_date, comment,
                      created_at, updated_at, closed_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12, now(), $13)
                    RETURNING *
                    """,
                    tg_user_id,
                    family_id,
                    counterparty,
                    normalized_direction,
                    opening_total,
                    paid_amount,
                    remaining_amount,
                    normalize_currency(str(rows[0]["currency"] or "UAH")) or "UAH",
                    account_id,
                    status,
                    "Migrated from legacy debt transactions",
                    created_at,
                    datetime.now() if status == "closed" else None,
                )
                if debt is None:
                    continue
                repaired += 1

                for row in rows:
                    action = str(row["debt_action"] or "")
                    if action in {"lend_repaid", "borrow_repaid"}:
                        payment = await self.conn.fetchrow(
                            """
                            INSERT INTO debt_payments (
                              tg_user_id, family_id, debt_id, amount, currency, account_id,
                              payment_date, comment, created_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            RETURNING id
                            """,
                            tg_user_id,
                            family_id,
                            int(debt["id"]),
                            quantize_money(Decimal(str(row["amount"] or 0))),
                            normalize_currency(str(row["currency"] or "UAH")) or "UAH",
                            row["account_id"],
                            row["created_at"].date() if row["created_at"] is not None else datetime.now().date(),
                            "Migrated from legacy debt transactions",
                            row["created_at"] or datetime.now(),
                        )
                        await self.conn.execute(
                            "UPDATE transactions SET debt_id=$2, debt_payment_id=$3 WHERE id=$1",
                            int(row["id"]),
                            int(debt["id"]),
                            int(payment["id"]),
                        )
                    else:
                        await self.conn.execute(
                            "UPDATE transactions SET debt_id=$2 WHERE id=$1",
                            int(row["id"]),
                            int(debt["id"]),
                        )

            return repaired

    async def _link_legacy_transaction(self, tx_id: int, debt_id: int) -> None:
        await self.conn.execute("UPDATE transactions SET debt_id=$2 WHERE id=$1", tx_id, debt_id)

    async def get_debt(self, tg_user_id: int, debt_id: int, *, for_update: bool = False) -> asyncpg.Record | None:
        scope = await self._scope(tg_user_id)
        return await self._get_debt_for_scope(scope, debt_id, for_update=for_update)

    async def get_debt_invite(self, token: str, *, for_update: bool = False) -> asyncpg.Record | None:
        sql = """
            SELECT
              di.id AS invite_id,
              di.debt_id,
              di.token,
              di.status AS invite_status,
              di.created_by_user_id,
              di.used_by_user_id,
              di.created_at AS invite_created_at,
              di.used_at AS invite_used_at,
              di.expires_at,
              d.tg_user_id AS lender_user_id,
              d.counterparty_name,
              d.direction,
              d.initial_amount,
              d.paid_amount,
              d.remaining_amount,
              d.currency,
              d.comment,
              d.status AS debt_status,
              d.borrower_user_id,
              d.borrower_confirmed_at,
              d.reminder_enabled,
              d.reminder_frequency,
              d.next_reminder_at,
              d.last_reminder_at,
              lender.first_name AS lender_first_name,
              lender.username AS lender_username
            FROM debt_invites di
            JOIN debts d ON d.id = di.debt_id
            LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
            WHERE di.token=$1
        """
        if for_update:
            sql += " FOR UPDATE OF di, d"
        return await self.conn.fetchrow(sql, token)

    async def get_observed_debt(self, borrower_user_id: int, debt_id: int) -> asyncpg.Record | None:
        scope = await self._scope(borrower_user_id)
        if scope.is_family:
            return await self.conn.fetchrow(
                """
                SELECT
                  d.*,
                  lender.first_name AS lender_first_name,
                  lender.username AS lender_username
                FROM debts d
                LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
                WHERE d.borrower_user_id=$1
                  AND d.id=$2
                  AND d.family_id=$3
                  AND d.borrower_confirmed_at IS NOT NULL
                  AND d.direction='receivable'
                """,
                borrower_user_id,
                debt_id,
                int(scope.family_id),
            )
        return await self.conn.fetchrow(
            """
            SELECT
              d.*,
              lender.first_name AS lender_first_name,
              lender.username AS lender_username
            FROM debts d
            LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
            WHERE d.borrower_user_id=$1
              AND d.id=$2
              AND d.family_id IS NULL
              AND d.borrower_confirmed_at IS NOT NULL
              AND d.direction='receivable'
            """,
            borrower_user_id,
            debt_id,
        )

    async def list_debts(
        self,
        tg_user_id: int,
        *,
        direction: DebtDirection | None = None,
        statuses: tuple[DebtStatus, ...] | None = None,
        positive_remaining_only: bool = False,
    ) -> list[asyncpg.Record]:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            clauses = ["family_id=$1"]
            args: list[object] = [int(scope.family_id)]
        else:
            clauses = ["tg_user_id=$1", "family_id IS NULL"]
            args = [tg_user_id]
        if direction:
            args.append(direction)
            clauses.append(f"direction=${len(args)}")
        if statuses:
            args.append(list(statuses))
            clauses.append(f"status = ANY(${len(args)})")
        if positive_remaining_only:
            clauses.append("remaining_amount > 0")
        sql = f"""
            SELECT *
            FROM debts
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, id ASC
        """
        return await self.conn.fetch(sql, *args)

    async def list_observed_payable_debts(self, borrower_user_id: int) -> list[asyncpg.Record]:
        scope = await self._scope(borrower_user_id)
        if scope.is_family:
            return await self.conn.fetch(
                """
                SELECT
                  d.*,
                  lender.first_name AS lender_first_name,
                  lender.username AS lender_username
                FROM debts d
                LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
                WHERE d.borrower_user_id=$1
                  AND d.family_id=$2
                  AND d.borrower_confirmed_at IS NOT NULL
                  AND d.direction='receivable'
                  AND d.status = ANY($3)
                  AND d.remaining_amount > 0
                ORDER BY d.created_at ASC, d.id ASC
                """,
                borrower_user_id,
                int(scope.family_id),
                ["active", "partially_paid"],
            )
        return await self.conn.fetch(
            """
            SELECT
              d.*,
              lender.first_name AS lender_first_name,
              lender.username AS lender_username
            FROM debts d
            LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
            WHERE d.borrower_user_id=$1
              AND d.family_id IS NULL
              AND d.borrower_confirmed_at IS NOT NULL
              AND d.direction='receivable'
              AND d.status = ANY($2)
              AND d.remaining_amount > 0
            ORDER BY d.created_at ASC, d.id ASC
            """,
            borrower_user_id,
            ["active", "partially_paid"],
        )

    async def list_active_receivable_debts(self, tg_user_id: int) -> list[asyncpg.Record]:
        return await self.list_debts(
            tg_user_id,
            direction="receivable",
            statuses=("active", "partially_paid"),
            positive_remaining_only=True,
        )

    async def list_active_payable_debts(self, tg_user_id: int) -> list[asyncpg.Record]:
        return await self.list_debts(
            tg_user_id,
            direction="payable",
            statuses=("active", "partially_paid"),
            positive_remaining_only=True,
        )

    async def get_debt_summary(self, tg_user_id: int) -> dict[str, object]:
        receivable = await self.list_active_receivable_debts(tg_user_id)
        payable = await self.list_active_payable_debts(tg_user_id)
        return {
            "receivable": receivable,
            "payable": payable,
            "total_given": sum((quantize_money(Decimal(str(row["initial_amount"] or 0))) for row in receivable), Decimal("0")),
            "total_returned": sum((quantize_money(Decimal(str(row["paid_amount"] or 0))) for row in receivable), Decimal("0")),
            "total_taken": sum((quantize_money(Decimal(str(row["initial_amount"] or 0))) for row in payable), Decimal("0")),
            "total_paid": sum((quantize_money(Decimal(str(row["paid_amount"] or 0))) for row in payable), Decimal("0")),
            "remaining_owed_to_me": sum((quantize_money(Decimal(str(row["remaining_amount"] or 0))) for row in receivable), Decimal("0")),
            "remaining_i_owe": sum((quantize_money(Decimal(str(row["remaining_amount"] or 0))) for row in payable), Decimal("0")),
        }

    async def list_debt_payments(self, tg_user_id: int, debt_id: int) -> list[asyncpg.Record]:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            return await self.conn.fetch(
                """
                SELECT *
                FROM debt_payments
                WHERE family_id=$1 AND debt_id=$2
                ORDER BY payment_date ASC, id ASC
                """,
                int(scope.family_id),
                debt_id,
            )
        return await self.conn.fetch(
            """
            SELECT *
            FROM debt_payments
            WHERE tg_user_id=$1 AND family_id IS NULL AND debt_id=$2
            ORDER BY payment_date ASC, id ASC
            """,
            tg_user_id,
            debt_id,
        )

    async def create_debt_invite(
        self,
        tg_user_id: int,
        debt_id: int,
        *,
        token: str,
        created_by_user_id: int,
        expires_at: datetime | None = None,
    ) -> DebtOperationResult:
        normalized_token = str(token or "").strip()
        if not normalized_token:
            return DebtOperationResult(status="invalid_payload", message="Invite token is required")

        async with self.conn.transaction():
            debt = await self.get_debt(tg_user_id, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing", message="Debt is missing")
            if _normalize_direction(str(debt["direction"] or "")) != "receivable":
                return DebtOperationResult(status="invalid_payload", debt=debt, message="Invite is available only for receivable debts")
            if _normalize_status(str(debt["status"] or "")) in {"closed", "cancelled"}:
                return DebtOperationResult(status="debt_locked", debt=debt, message="Debt is closed or cancelled")
            if quantize_money(Decimal(str(debt["remaining_amount"] or 0))) <= 0:
                return DebtOperationResult(status="debt_locked", debt=debt, message="Debt has no remaining balance")
            if debt["borrower_user_id"] is not None and debt["borrower_confirmed_at"] is not None:
                return DebtOperationResult(status="invite_conflict", debt=debt, message="Borrower is already confirmed")

            await self._cancel_pending_invites_locked(debt_id)
            invite = await self.conn.fetchrow(
                """
                INSERT INTO debt_invites (
                  debt_id, token, status, created_by_user_id,
                  used_by_user_id, created_at, used_at, expires_at
                )
                VALUES ($1, $2, 'pending', $3, NULL, now(), NULL, $4)
                RETURNING *
                """,
                debt_id,
                normalized_token,
                created_by_user_id,
                expires_at,
            )
            return DebtOperationResult(status="completed", debt=debt, invite=invite)

    async def create_debt(
        self,
        tg_user_id: int,
        *,
        counterparty_name: str,
        direction: str,
        debt_amount: Decimal,
        debt_currency: str,
        account_id: int,
        comment: str | None = None,
        due_date: date | None = None,
        account_amount: Decimal | None = None,
        exchange_rate: Decimal | None = None,
    ) -> DebtOperationResult:
        normalized_direction = _normalize_direction(direction)
        if normalized_direction is None:
            return DebtOperationResult(status="invalid_payload", message="Invalid debt direction")

        normalized_debt_amount = quantize_money(Decimal(str(debt_amount)))
        if normalized_debt_amount <= 0:
            return DebtOperationResult(status="invalid_payload", message="Debt amount must be positive")

        normalized_debt_currency = normalize_currency(debt_currency) or "UAH"
        normalized_comment = (comment or "").strip()[:500] or None
        normalized_counterparty = (counterparty_name or "").strip()[:120]
        if not normalized_counterparty:
            return DebtOperationResult(status="invalid_payload", message="Counterparty is required")

        scope = await self._scope(tg_user_id)
        async with self.conn.transaction():
            account = await self._get_account_for_scope(scope, account_id, for_update=True)
            if account is None:
                return DebtOperationResult(status="account_missing", message="Account is missing")

            account_currency = normalize_currency(str(account["currency"] or "UAH"))
            resolved_account_amount, resolved_rate = self._resolve_cross_currency_amounts(
                normalized_debt_amount,
                normalized_debt_currency,
                account_currency,
                account_amount=account_amount,
                exchange_rate=exchange_rate,
            )
            if resolved_account_amount is None:
                return DebtOperationResult(
                    status="invalid_payload",
                    account=account,
                    message="Currency conversion details are missing",
                    required_amount=normalized_debt_amount,
                    required_currency=normalized_debt_currency,
                )

            balance = Decimal(str(account["balance"] or 0))
            if normalized_direction == "receivable":
                new_balance = quantize_money(balance - resolved_account_amount)
                tx_type = "debt_given"
            else:
                new_balance = quantize_money(balance + resolved_account_amount)
                tx_type = "debt_received"

            if normalized_direction == "receivable" and _exceeds_credit_limit(account, new_balance):
                return DebtOperationResult(status="credit_limit_exceeded", account=account, message="Debt debit exceeds the account credit limit")

            debt = await self.conn.fetchrow(
                """
                INSERT INTO debts (
                  tg_user_id, family_id, counterparty_name, direction,
                  initial_amount, paid_amount, remaining_amount,
                  currency, account_id, status, due_date, comment,
                  created_at, updated_at
                )
                VALUES (
                  $1, $2, $3, $4,
                  $5, 0, $5,
                  $6, $7, 'active', $8, $9,
                  now(), now()
                )
                RETURNING *
                """,
                tg_user_id,
                scope.family_id,
                normalized_counterparty,
                normalized_direction,
                normalized_debt_amount,
                normalized_debt_currency,
                account_id,
                due_date,
                normalized_comment,
            )
            if debt is None:
                return DebtOperationResult(status="invalid_payload", message="Could not create debt")

            tx = await self.conn.fetchrow(
                """
                INSERT INTO transactions (
                  tg_user_id, family_id, created_by_user_id, updated_by_user_id, date, type, amount, currency,
                  comment, source,
                  account_id, category_id, category_name_snapshot,
                  flow_kind, counterparty, debt_id,
                  original_amount, original_currency, exchange_rate
                )
                VALUES (
                  $1, $2, $1, $1, $3, $4, $5, $6,
                  $7, $8,
                  $9, NULL, NULL,
                  'debt', $10, $11,
                  $12, $13, $14
                )
                RETURNING *
                """,
                tg_user_id,
                scope.family_id,
                datetime.now().date(),
                tx_type,
                resolved_account_amount,
                account_currency,
                normalized_comment,
                "debt",
                account_id,
                normalized_counterparty,
                int(debt["id"]),
                normalized_debt_amount,
                normalized_debt_currency,
                resolved_rate,
            )
            await self._update_account_balance_for_scope(scope, account, new_balance)
            return DebtOperationResult(status="completed", debt=debt, transaction=tx, account=account)

    async def record_repayment(
        self,
        tg_user_id: int,
        *,
        debt_id: int,
        account_id: int,
        payment_amount: Decimal,
        payment_currency: str,
        comment: str | None = None,
        payment_date: date | None = None,
        account_amount: Decimal | None = None,
        exchange_rate: Decimal | None = None,
        allow_partial: bool = False,
    ) -> DebtOperationResult:
        normalized_payment_amount = quantize_money(Decimal(str(payment_amount)))
        if normalized_payment_amount <= 0:
            return DebtOperationResult(status="invalid_payload", message="Payment amount must be positive")

        normalized_payment_currency = normalize_currency(payment_currency) or "UAH"
        normalized_comment = (comment or "").strip()[:500] or None

        scope = await self._scope(tg_user_id)
        async with self.conn.transaction():
            debt = await self._get_debt_for_scope(scope, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing", message="Debt is missing")
            if not scope.is_family and int(debt["tg_user_id"] or 0) != tg_user_id:
                return DebtOperationResult(status="not_owner", debt=debt, message="Debt belongs to another user")

            debt_status = _normalize_status(str(debt["status"] or "")) or "active"
            if debt_status in {"closed", "cancelled"}:
                return DebtOperationResult(status="debt_locked", debt=debt, message="Debt is closed or cancelled")

            remaining = quantize_money(Decimal(str(debt["remaining_amount"] or 0)))
            if normalized_payment_amount > remaining:
                # Keep the legacy keyword callable, but never change confirmed
                # principal (or its manual FX amount) without a new confirmation.
                return DebtOperationResult(
                    status="over_limit",
                    debt=debt,
                    required_amount=remaining,
                    required_currency=normalize_currency(str(debt["currency"] or "UAH")),
                    message="Payment amount exceeds remaining debt; confirm a new payment",
                )

            account = await self._get_account_for_scope(scope, account_id, for_update=True)
            if account is None:
                return DebtOperationResult(status="account_missing", debt=debt, message="Account is missing")

            account_currency = normalize_currency(str(account["currency"] or "UAH"))
            resolved_account_amount, resolved_rate = self._resolve_cross_currency_amounts(
                normalized_payment_amount,
                normalize_currency(str(debt["currency"] or "UAH")),
                account_currency,
                account_amount=account_amount,
                exchange_rate=exchange_rate,
            )
            if resolved_account_amount is None:
                return DebtOperationResult(
                    status="invalid_payload",
                    debt=debt,
                    account=account,
                    message="Currency conversion details are missing",
                    required_amount=normalized_payment_amount,
                    required_currency=normalize_currency(str(debt["currency"] or "UAH")),
                )

            direction = _normalize_direction(str(debt["direction"] or "")) or "receivable"
            transaction_type = "debt_repayment_in" if direction == "receivable" else "debt_repayment_out"

            current_balance = Decimal(str(account["balance"] or 0))
            if direction == "receivable":
                new_balance = quantize_money(current_balance + resolved_account_amount)
            else:
                new_balance = quantize_money(current_balance - resolved_account_amount)

            if direction == "payable" and _exceeds_credit_limit(account, new_balance):
                return DebtOperationResult(status="credit_limit_exceeded", debt=debt, account=account, message="Debt debit exceeds the account credit limit")

            paid_amount = quantize_money(Decimal(str(debt["paid_amount"] or 0)) + normalized_payment_amount)
            remaining_amount = quantize_money(Decimal(str(debt["initial_amount"] or 0)) - paid_amount)
            status = _status_from_remaining(paid=paid_amount, remaining=remaining_amount, current=debt_status)

            if scope.is_family:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET paid_amount=$3,
                        remaining_amount=$4,
                        status=$5,
                        closed_at=CASE WHEN $5='closed' THEN COALESCE(closed_at, now()) ELSE NULL END,
                        reminder_enabled=CASE WHEN $5='closed' THEN false ELSE reminder_enabled END,
                        next_reminder_at=CASE WHEN $5='closed' THEN NULL ELSE next_reminder_at END,
                        reminder_delivery_error=CASE WHEN $5='closed' THEN NULL ELSE reminder_delivery_error END,
                        updated_at=now()
                    WHERE family_id=$1 AND id=$2
                    RETURNING *
                    """,
                    int(scope.family_id),
                    debt_id,
                    paid_amount,
                    remaining_amount,
                    status,
                )
            else:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET paid_amount=$3,
                        remaining_amount=$4,
                        status=$5,
                        closed_at=CASE WHEN $5='closed' THEN COALESCE(closed_at, now()) ELSE NULL END,
                        reminder_enabled=CASE WHEN $5='closed' THEN false ELSE reminder_enabled END,
                        next_reminder_at=CASE WHEN $5='closed' THEN NULL ELSE next_reminder_at END,
                        reminder_delivery_error=CASE WHEN $5='closed' THEN NULL ELSE reminder_delivery_error END,
                        updated_at=now()
                    WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
                    RETURNING *
                    """,
                    tg_user_id,
                    debt_id,
                    paid_amount,
                    remaining_amount,
                    status,
                )
            if updated_debt is None:
                return DebtOperationResult(status="debt_missing", message="Debt update failed")

            payment = await self.conn.fetchrow(
                """
                INSERT INTO debt_payments (
                  tg_user_id, family_id, debt_id, amount, currency, account_id,
                  payment_date, comment, created_at
                )
                VALUES (
                  $1, $2, $3, $4, $5, $6,
                  $7, $8, now()
                )
                RETURNING *
                """,
                tg_user_id,
                scope.family_id,
                debt_id,
                normalized_payment_amount,
                normalize_currency(str(debt["currency"] or "UAH")),
                account_id,
                payment_date or datetime.now().date(),
                normalized_comment,
            )

            tx = await self.conn.fetchrow(
                """
                INSERT INTO transactions (
                  tg_user_id, family_id, created_by_user_id, updated_by_user_id, date, type, amount, currency,
                  comment, source,
                  account_id, category_id, category_name_snapshot,
                  flow_kind, counterparty, debt_id, debt_payment_id,
                  original_amount, original_currency, exchange_rate
                )
                VALUES (
                  $1, $2, $1, $1, $3, $4, $5, $6,
                  $7, $8,
                  $9, NULL, NULL,
                  'debt', $10, $11, $12,
                  $13, $14, $15
                )
                RETURNING *
                """,
                tg_user_id,
                scope.family_id,
                payment_date or datetime.now().date(),
                transaction_type,
                resolved_account_amount,
                account_currency,
                normalized_comment,
                "debt",
                account_id,
                str(debt["counterparty_name"] or ""),
                debt_id,
                int(payment["id"]),
                normalized_payment_amount,
                normalize_currency(str(debt["currency"] or "UAH")),
                resolved_rate,
            )

            await self._update_account_balance_for_scope(scope, account, new_balance)
            if status == "closed":
                await self._cancel_pending_invites_locked(debt_id)
            return DebtOperationResult(status="completed", debt=updated_debt, payment=payment, transaction=tx, account=account)

    async def update_debt(
        self,
        tg_user_id: int,
        *,
        debt_id: int,
        counterparty_name: str | None = None,
        initial_amount: Decimal | None = None,
        currency: str | None = None,
        due_date: date | None = None,
        comment: str | None = None,
    ) -> DebtOperationResult:
        scope = await self._scope(tg_user_id)
        async with self.conn.transaction():
            debt = await self._get_debt_for_scope(scope, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing", message="Debt is missing")
            if _normalize_status(str(debt["status"] or "")) in {"closed", "cancelled"}:
                return DebtOperationResult(status="debt_locked", debt=debt, message="Debt is closed or cancelled")

            updated_counterparty = (counterparty_name or str(debt["counterparty_name"] or "")).strip()[:120]
            updated_comment = (comment if comment is not None else str(debt["comment"] or "")).strip()[:500] or None
            updated_currency = normalize_currency(currency or str(debt["currency"] or "UAH")) or "UAH"
            updated_initial = quantize_money(Decimal(str(initial_amount))) if initial_amount is not None else quantize_money(Decimal(str(debt["initial_amount"] or 0)))
            paid_amount = quantize_money(Decimal(str(debt["paid_amount"] or 0)))
            if updated_initial < paid_amount:
                return DebtOperationResult(status="invalid_payload", debt=debt, message="Initial amount is less than paid amount")

            remaining_amount = quantize_money(updated_initial - paid_amount)
            status = _status_from_remaining(paid=paid_amount, remaining=remaining_amount, current=str(debt["status"] or "active"))
            closed_at = debt["closed_at"]
            if status == "closed":
                closed_at = closed_at or datetime.now()
            elif status != "cancelled":
                closed_at = None

            if scope.is_family:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET counterparty_name=$3,
                        initial_amount=$4,
                        remaining_amount=$5,
                        currency=$6,
                        due_date=$7,
                        comment=$8,
                        status=$9,
                        closed_at=$10,
                        updated_at=now()
                    WHERE family_id=$1 AND id=$2
                    RETURNING *
                    """,
                    int(scope.family_id),
                    debt_id,
                    updated_counterparty,
                    updated_initial,
                    remaining_amount,
                    updated_currency,
                    due_date,
                    updated_comment,
                    status,
                    closed_at,
                )
            else:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET counterparty_name=$3,
                        initial_amount=$4,
                        remaining_amount=$5,
                        currency=$6,
                        due_date=$7,
                        comment=$8,
                        status=$9,
                        closed_at=$10,
                        updated_at=now()
                    WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
                    RETURNING *
                    """,
                    tg_user_id,
                    debt_id,
                    updated_counterparty,
                    updated_initial,
                    remaining_amount,
                    updated_currency,
                    due_date,
                    updated_comment,
                    status,
                    closed_at,
                )
            return DebtOperationResult(status="completed", debt=updated_debt)

    async def cancel_debt(self, tg_user_id: int, debt_id: int) -> DebtOperationResult:
        debt = await self.get_debt(tg_user_id, debt_id, for_update=False)
        if debt is None:
            return DebtOperationResult(status="debt_missing")
        if _normalize_status(str(debt["status"] or "")) == "cancelled":
            return DebtOperationResult(status="debt_locked", debt=debt)
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            await self.conn.execute(
                """
                UPDATE debts
                SET status='cancelled', updated_at=now()
                WHERE family_id=$1 AND id=$2
                """,
                int(scope.family_id),
                debt_id,
            )
        else:
            await self.conn.execute(
                """
                UPDATE debts
                SET status='cancelled', updated_at=now()
                WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
                """,
                tg_user_id,
                debt_id,
            )
        updated = await self.get_debt(tg_user_id, debt_id)
        return DebtOperationResult(status="completed", debt=updated)

    async def close_debt(self, tg_user_id: int, debt_id: int) -> DebtOperationResult:
        scope = await self._scope(tg_user_id)
        async with self.conn.transaction():
            debt = await self._get_debt_for_scope(scope, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing")
            if scope.is_family:
                await self.conn.execute(
                    """
                    UPDATE debts
                    SET status='closed',
                        closed_at=COALESCE(closed_at, now()),
                        reminder_enabled=false,
                        next_reminder_at=NULL,
                        reminder_delivery_error=NULL,
                        updated_at=now()
                    WHERE family_id=$1 AND id=$2
                    """,
                    int(scope.family_id),
                    debt_id,
                )
            else:
                await self.conn.execute(
                    """
                    UPDATE debts
                    SET status='closed',
                        closed_at=COALESCE(closed_at, now()),
                        reminder_enabled=false,
                        next_reminder_at=NULL,
                        reminder_delivery_error=NULL,
                        updated_at=now()
                    WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
                    """,
                    tg_user_id,
                    debt_id,
                )
            await self._cancel_pending_invites_locked(debt_id)
            updated = await self.get_debt(tg_user_id, debt_id)
            return DebtOperationResult(status="completed", debt=updated)

    async def delete_debt(self, tg_user_id: int, debt_id: int) -> DebtOperationResult:
        scope = await self._scope(tg_user_id)
        async with self.conn.transaction():
            debt = await self._get_debt_for_scope(scope, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing")

            if scope.is_family:
                tx_rows = await self.conn.fetch(
                    """
                    SELECT id, type, amount, account_id, currency
                    FROM transactions
                    WHERE family_id=$1 AND debt_id=$2
                      AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                    ORDER BY id ASC
                    FOR UPDATE
                    """,
                    int(scope.family_id),
                    debt_id,
                )
                payment_rows = await self.conn.fetch(
                    """
                    SELECT id
                    FROM debt_payments
                    WHERE family_id=$1 AND debt_id=$2
                    ORDER BY id ASC
                    FOR UPDATE
                    """,
                    int(scope.family_id),
                    debt_id,
                )
            else:
                tx_rows = await self.conn.fetch(
                    """
                    SELECT id, type, amount, account_id, currency
                    FROM transactions
                    WHERE tg_user_id=$1 AND family_id IS NULL AND debt_id=$2
                      AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                    ORDER BY id ASC
                    FOR UPDATE
                    """,
                    tg_user_id,
                    debt_id,
                )
                payment_rows = await self.conn.fetch(
                    """
                    SELECT id
                    FROM debt_payments
                    WHERE tg_user_id=$1 AND family_id IS NULL AND debt_id=$2
                    ORDER BY id ASC
                    FOR UPDATE
                    """,
                    tg_user_id,
                    debt_id,
                )

            balance_deltas: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
            cash_movements = []
            for tx in tx_rows:
                tx_type = str(tx["type"] or "")
                if tx_type in {"debt_given", "debt_repayment_out"}:
                    reversal_sign = 1
                elif tx_type in {"debt_received", "debt_repayment_in"}:
                    reversal_sign = -1
                else:
                    # Legacy debt-only rows never changed an account balance.
                    continue
                if tx["account_id"] is None:
                    # Historical accountless debt rows had no reversible cash
                    # movement. Delete them without inventing a balance target.
                    continue
                tx_account_id = int(tx["account_id"])
                balance_deltas[tx_account_id] += reversal_sign * quantize_money(Decimal(str(tx["amount"] or 0)))
                cash_movements.append(tx)

            # The debt lock serializes its repayments. Lock every affected account
            # in the same ID order as transfers, including archived accounts.
            accounts: dict[int, asyncpg.Record] = {}
            for account_id in sorted(balance_deltas):
                account = await self._get_account_for_scope(scope, account_id, for_update=True, include_archived=True)
                if account is None:
                    return DebtOperationResult(status="account_missing", debt=debt, message="Debt transaction account is missing")
                accounts[account_id] = account
            for tx in cash_movements:
                account = accounts[int(tx["account_id"])]
                if normalize_currency(str(tx["currency"] or "")) != normalize_currency(str(account["currency"] or "")):
                    return DebtOperationResult(status="invalid_payload", debt=debt, account=account, message="Debt transaction currency differs from its account")
            for account_id, account in accounts.items():
                new_balance = quantize_money(Decimal(str(account["balance"] or 0)) + balance_deltas[account_id])
                if _exceeds_credit_limit(account, new_balance):
                    return DebtOperationResult(
                        status="credit_limit_exceeded",
                        debt=debt,
                        account=account,
                        message="Debt deletion reversal exceeds the account credit limit",
                    )
                await self._update_account_balance_for_scope(scope, account, new_balance)

            for row in payment_rows:
                await self.conn.execute("DELETE FROM debt_payments WHERE id=$1", row["id"])
            for row in tx_rows:
                await self.conn.execute("DELETE FROM transactions WHERE id=$1", row["id"])
            if scope.is_family:
                await self.conn.execute("DELETE FROM debts WHERE family_id=$1 AND id=$2", int(scope.family_id), debt_id)
            else:
                await self.conn.execute("DELETE FROM debts WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2", tg_user_id, debt_id)

            origin_account_id = int(debt["account_id"]) if debt["account_id"] is not None else None
            return DebtOperationResult(status="completed", debt=debt, account=accounts.get(origin_account_id))

    async def build_debt_summary_text(self, tg_user_id: int) -> str:
        scope = await self._scope(tg_user_id)
        all_debts = await self.list_debts(tg_user_id)
        summary = await self.get_debt_summary(tg_user_id)
        receivable = list(summary["receivable"])  # type: ignore[list-item]
        payable = list(summary["payable"])  # type: ignore[list-item]
        if not all_debts:
            if scope.is_family:
                legacy_rows = await self.conn.fetch(
                    """
                    SELECT *
                    FROM transactions
                    WHERE family_id=$1 AND flow_kind='debt'
                      AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                    ORDER BY id ASC
                    """,
                    int(scope.family_id),
                )
            else:
                legacy_rows = await self.conn.fetch(
                    """
                    SELECT *
                    FROM transactions
                    WHERE tg_user_id=$1 AND family_id IS NULL AND flow_kind='debt'
                      AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                    ORDER BY id ASC
                    """,
                    tg_user_id,
                )
            if legacy_rows:
                grouped: dict[tuple[str, str], dict[str, Decimal]] = {}
                for row in legacy_rows:
                    counterparty = str(row["counterparty"] or "—")
                    currency = str(row["currency"] or "UAH")
                    bucket = grouped.setdefault((counterparty, currency), {"owed_to_me": Decimal("0"), "i_owe": Decimal("0")})
                    if "amount" in row:
                        amount = quantize_money(Decimal(str(row["amount"] or 0)))
                        action = str(row["debt_action"] or "")
                        if action == "lend":
                            bucket["owed_to_me"] += amount
                        elif action == "lend_repaid":
                            bucket["owed_to_me"] -= amount
                        elif action == "borrow":
                            bucket["i_owe"] += amount
                        elif action == "borrow_repaid":
                            bucket["i_owe"] -= amount
                    else:
                        bucket["owed_to_me"] += quantize_money(Decimal(str(row.get("owed_to_me") or 0)))
                        bucket["i_owe"] += quantize_money(Decimal(str(row.get("i_owe") or 0)))
                lines = ["<b>💸 Борги</b>", ""]
                for (counterparty, currency), vals in sorted(grouped.items()):
                    owed_to_me = max(quantize_money(vals["owed_to_me"]), Decimal("0"))
                    i_owe = max(quantize_money(vals["i_owe"]), Decimal("0"))
                    lines.extend(
                        [
                            f"<b>{escape_html(counterparty)}</b>",
                            f"Мені винні: {format_money(owed_to_me, currency)}",
                            f"Я винен: {format_money(i_owe, currency)}",
                            "",
                        ]
                    )
                return "\n".join(lines).strip()

        lines = [
            "<b>💸 Борги</b>",
            "",
            f"<b>Видано в борг:</b> {format_money(Decimal(str(summary['total_given'])), 'UAH')}",
            f"<b>Повернули мені:</b> {format_money(Decimal(str(summary['total_returned'])), 'UAH')}",
            f"<b>Я взяв у борг:</b> {format_money(Decimal(str(summary['total_taken'])), 'UAH')}",
            f"<b>Я повернув боргів:</b> {format_money(Decimal(str(summary['total_paid'])), 'UAH')}",
            "",
            f"<b>Мені ще винні:</b> {format_money(Decimal(str(summary['remaining_owed_to_me'])), 'UAH')}",
            f"<b>Я ще винен:</b> {format_money(Decimal(str(summary['remaining_i_owe'])), 'UAH')}",
        ]
        return "\n".join(lines)

    async def build_debt_list_text(self, tg_user_id: int, direction: DebtDirection) -> str:
        debts = await (
            self.list_active_receivable_debts(tg_user_id)
            if direction == "receivable"
            else self.list_active_payable_debts(tg_user_id)
        )
        header = "Мені винні" if direction == "receivable" else "Я винен"
        lines = [f"<b>{header}:</b>", ""]
        if not debts:
            lines.append("<i>Немає активних боргів.</i>")
            return "\n".join(lines)

        for idx, debt in enumerate(debts, start=1):
            currency = str(debt["currency"] or "UAH")
            status = str(debt["status"] or "active")
            if status == "partially_paid":
                status_label = "частково погашено"
            elif status == "closed":
                status_label = "закрито"
            elif status == "cancelled":
                status_label = "скасовано"
            elif status == "needs_review":
                status_label = "потребує перевірки"
            else:
                status_label = "активний"
            lines += [
                f"{idx}. {escape_html(debt['counterparty_name'])}",
                f"Борг: {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
                f"Повернув: {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
                f"Залишилось: {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
                f"Статус: {escape_html(status_label)}",
            ]
            if direction == "receivable":
                borrower_confirmed = debt.get("borrower_confirmed_at") is not None and debt.get("borrower_user_id") is not None
                lines.append(f"Підтвердження: {'підтверджено' if borrower_confirmed else 'не підтверджено'}")
                if borrower_confirmed:
                    reminder_label = "увімкнені" if bool(debt.get("reminder_enabled")) else "вимкнені"
                    lines.append(f"Нагадування: {reminder_label}")
            lines.append("")
        return "\n".join(lines).strip()

    async def build_debt_detail_text(self, tg_user_id: int, debt_id: int) -> str:
        debt = await self.get_debt(tg_user_id, debt_id)
        if debt is None:
            return "<b>❌ Борг не знайдено</b>"

        payments = await self.list_debt_payments(tg_user_id, debt_id)
        direction = "мені винні" if _normalize_direction(str(debt["direction"] or "")) == "receivable" else "я винен"
        status = str(debt["status"] or "active")
        status_label = {
            "active": "активний",
            "partially_paid": "частково погашено",
            "closed": "закрито",
            "cancelled": "скасовано",
            "needs_review": "потребує перевірки",
        }.get(status, status)
        currency = str(debt["currency"] or "UAH")
        lines = [
            f"<b>Борг:</b> {escape_html(debt['counterparty_name'])}",
            f"<b>Тип:</b> {escape_html(direction)}",
            f"<b>Сума:</b> {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
            f"<b>Статус:</b> {escape_html(status_label)}",
        ]
        if _normalize_direction(str(debt["direction"] or "")) == "receivable":
            borrower_confirmed = debt.get("borrower_confirmed_at") is not None and debt.get("borrower_user_id") is not None
            lines.append(f"<b>Підтвердження боржника:</b> {'підтверджено' if borrower_confirmed else 'не підтверджено'}")
            if borrower_confirmed:
                reminder_label = "увімкнені" if bool(debt.get("reminder_enabled")) else "вимкнені"
                lines.append(f"<b>Нагадування боржнику:</b> {reminder_label}")
        lines += [
            "",
            "<b>Історія:</b>",
            f"{debt['created_at'].strftime('%d.%m.%Y') if debt['created_at'] else '--'} — створено борг {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
        ]
        for payment in payments:
            payment_currency = str(payment["currency"] or currency)
            payment_date = payment["payment_date"].strftime("%d.%m.%Y") if payment["payment_date"] else "--"
            lines.append(
                f"{payment_date} — повернення {format_money(Decimal(str(payment['amount'] or 0)), payment_currency)}"
            )
        lines += [
            "",
            f"<b>Всього повернуто:</b> {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
            f"<b>Залишилось:</b> {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
        ]
        if debt.get("comment"):
            lines += ["", f"<b>Коментар:</b> <i>{escape_html(debt['comment'])}</i>"]
        return "\n".join(lines)

    async def build_observed_debt_detail_text(self, borrower_user_id: int, debt_id: int) -> str:
        debt = await self.get_observed_debt(borrower_user_id, debt_id)
        if debt is None:
            return "<b>❌ Борг не знайдено</b>"

        lender_name = _person_name(debt.get("lender_first_name"), debt.get("lender_username"))
        currency = str(debt["currency"] or "UAH")
        lines = [
            f"<b>Позичив(ла):</b> {escape_html(lender_name)}",
            f"<b>Сума:</b> {format_money(Decimal(str(debt['initial_amount'] or 0)), currency)}",
            f"<b>Повернуто:</b> {format_money(Decimal(str(debt['paid_amount'] or 0)), currency)}",
            f"<b>Залишок:</b> {format_money(Decimal(str(debt['remaining_amount'] or 0)), currency)}",
        ]
        if debt.get("comment"):
            lines.append(f"<b>Коментар:</b> <i>{escape_html(str(debt['comment']))}</i>")
        lines.extend(
            [
                "",
                f"<b>Керує боргом:</b> {escape_html(lender_name)}",
                "Це read-only запис. Змінювати суму або відмічати повернення тут не можна.",
            ]
        )
        return "\n".join(lines)

    async def build_debt_history_text(self, tg_user_id: int) -> str:
        scope = await self._scope(tg_user_id)
        if scope.is_family:
            rows = await self.conn.fetch(
                """
                SELECT d.counterparty_name, d.direction, d.currency, d.status, p.amount, p.payment_date, p.comment
                FROM debts d
                LEFT JOIN debt_payments p ON p.debt_id = d.id AND p.family_id = d.family_id
                WHERE d.family_id=$1
                ORDER BY COALESCE(p.payment_date, d.created_at::date) DESC, d.id DESC, p.id DESC
                """,
                int(scope.family_id),
            )
        else:
            rows = await self.conn.fetch(
                """
                SELECT d.counterparty_name, d.direction, d.currency, d.status, p.amount, p.payment_date, p.comment
                FROM debts d
                LEFT JOIN debt_payments p ON p.debt_id = d.id AND p.tg_user_id = d.tg_user_id AND p.family_id IS NULL
                WHERE d.tg_user_id=$1 AND d.family_id IS NULL
                ORDER BY COALESCE(p.payment_date, d.created_at::date) DESC, d.id DESC, p.id DESC
                """,
                tg_user_id,
            )
        if not rows:
            return "<b>📄 Історія боргів</b>\n\n<i>Поки що немає записів.</i>"

        lines = ["<b>📄 Історія боргів</b>", ""]
        for row in rows:
            direction = "мені винні" if str(row["direction"] or "") == "receivable" else "я винен"
            base = f"{escape_html(row['counterparty_name'])} — {escape_html(direction)}"
            if row["amount"] is None:
                lines.append(base)
                continue
            lines.append(
                f"{base}: {format_money(Decimal(str(row['amount'] or 0)), str(row['currency'] or 'UAH'))} "
                f"({row['payment_date'].strftime('%d.%m.%Y') if row['payment_date'] else '--'})"
            )
        return "\n".join(lines)

    async def confirm_debt_invite(self, token: str, *, borrower_user_id: int) -> DebtOperationResult:
        async with self.conn.transaction():
            invite = await self.get_debt_invite(token, for_update=True)
            if invite is None:
                return DebtOperationResult(status="invite_missing", message="Invite is missing")

            owner_user_id = int(invite["lender_user_id"] or 0)
            debt_id = int(invite["debt_id"] or 0)
            if owner_user_id == borrower_user_id:
                return DebtOperationResult(status="invite_owner_blocked", message="Owner cannot confirm own invite", invite=invite)

            if str(invite["invite_status"] or "") != "pending":
                return DebtOperationResult(status="invite_not_pending", message="Invite is not pending", invite=invite)

            expires_at = invite["expires_at"]
            now = datetime.now()
            if expires_at is not None and expires_at <= now:
                updated_invite = await self.conn.fetchrow(
                    """
                    UPDATE debt_invites
                    SET status='expired'
                    WHERE id=$1
                    RETURNING *
                    """,
                    int(invite["invite_id"]),
                )
                return DebtOperationResult(status="invite_not_pending", message="Invite is expired", invite=updated_invite)

            debt = await self.get_debt(owner_user_id, debt_id, for_update=True)
            if debt is None:
                return DebtOperationResult(status="debt_missing", message="Debt is missing", invite=invite)
            if _normalize_status(str(debt["status"] or "")) in {"closed", "cancelled"}:
                await self._cancel_pending_invites_locked(debt_id)
                return DebtOperationResult(status="debt_locked", debt=debt, invite=invite, message="Debt is closed or cancelled")

            existing_borrower = debt["borrower_user_id"]
            if existing_borrower is not None and int(existing_borrower) != borrower_user_id:
                return DebtOperationResult(status="invite_conflict", debt=debt, invite=invite, message="Debt already belongs to another borrower")

            next_reminder_at = _add_one_month(now)
            if debt["family_id"] is not None:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET borrower_user_id=$3,
                        borrower_confirmed_at=$4,
                        reminder_enabled=true,
                        reminder_frequency='monthly',
                        next_reminder_at=$5,
                        last_reminder_at=NULL,
                        reminder_delivery_error=NULL,
                        updated_at=now()
                    WHERE family_id=$1 AND id=$2
                    RETURNING *
                    """,
                    int(debt["family_id"]),
                    debt_id,
                    borrower_user_id,
                    now,
                    next_reminder_at,
                )
            else:
                updated_debt = await self.conn.fetchrow(
                    """
                    UPDATE debts
                    SET borrower_user_id=$3,
                        borrower_confirmed_at=$4,
                        reminder_enabled=true,
                        reminder_frequency='monthly',
                        next_reminder_at=$5,
                        last_reminder_at=NULL,
                        reminder_delivery_error=NULL,
                        updated_at=now()
                    WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2
                    RETURNING *
                    """,
                    owner_user_id,
                    debt_id,
                    borrower_user_id,
                    now,
                    next_reminder_at,
                )
            updated_invite = await self.conn.fetchrow(
                """
                UPDATE debt_invites
                SET status='used',
                    used_by_user_id=$2,
                    used_at=$3
                WHERE id=$1
                RETURNING *
                """,
                int(invite["invite_id"]),
                borrower_user_id,
                now,
            )
            await self._cancel_pending_invites_locked(debt_id, exclude_invite_id=int(invite["invite_id"]))
            return DebtOperationResult(status="completed", debt=updated_debt, invite=updated_invite)

    async def reject_debt_invite(self, token: str, *, borrower_user_id: int) -> DebtOperationResult:
        async with self.conn.transaction():
            invite = await self.get_debt_invite(token, for_update=True)
            if invite is None:
                return DebtOperationResult(status="invite_missing", message="Invite is missing")
            if int(invite["lender_user_id"] or 0) == borrower_user_id:
                return DebtOperationResult(status="invite_owner_blocked", message="Owner cannot reject own invite", invite=invite)
            if str(invite["invite_status"] or "") != "pending":
                return DebtOperationResult(status="invite_not_pending", message="Invite is not pending", invite=invite)

            updated_invite = await self.conn.fetchrow(
                """
                UPDATE debt_invites
                SET status='rejected'
                WHERE id=$1
                RETURNING *
                """,
                int(invite["invite_id"]),
            )
            debt = await self.get_debt(int(invite["lender_user_id"] or 0), int(invite["debt_id"] or 0))
            return DebtOperationResult(status="completed", debt=debt, invite=updated_invite)

    async def list_due_debt_reminders(self, now: datetime, *, limit: int = 50) -> list[asyncpg.Record]:
        return await self.conn.fetch(
            """
            SELECT
              d.*,
              lender.first_name AS lender_first_name,
              lender.username AS lender_username
            FROM debts d
            LEFT JOIN users lender ON lender.tg_user_id = d.tg_user_id
            WHERE d.reminder_enabled=true
              AND d.borrower_user_id IS NOT NULL
              AND d.borrower_confirmed_at IS NOT NULL
              AND d.remaining_amount > 0
              AND d.status NOT IN ('closed', 'cancelled')
              AND d.next_reminder_at IS NOT NULL
              AND d.next_reminder_at <= $1
            ORDER BY d.next_reminder_at ASC, d.id ASC
            LIMIT $2
            """,
            now,
            limit,
        )

    async def mark_debt_reminder_sent(self, debt_id: int, sent_at: datetime) -> None:
        await self.conn.execute(
            """
            UPDATE debts
            SET last_reminder_at=$2,
                next_reminder_at=$3,
                reminder_delivery_error=NULL,
                updated_at=now()
            WHERE id=$1
            """,
            debt_id,
            sent_at,
            _add_one_month(sent_at),
        )

    async def mark_debt_reminder_failed(self, debt_id: int, error_text: str) -> None:
        await self.conn.execute(
            """
            UPDATE debts
            SET reminder_delivery_error=$2,
                updated_at=now()
            WHERE id=$1
            """,
            debt_id,
            str(error_text or "").strip()[:500],
        )

    async def _cancel_pending_invites_locked(self, debt_id: int, *, exclude_invite_id: int | None = None) -> None:
        if exclude_invite_id is None:
            await self.conn.execute(
                """
                UPDATE debt_invites
                SET status='cancelled'
                WHERE debt_id=$1 AND status='pending'
                """,
                debt_id,
            )
            return
        await self.conn.execute(
            """
            UPDATE debt_invites
            SET status='cancelled'
            WHERE debt_id=$1 AND status='pending' AND id <> $2
            """,
            debt_id,
            exclude_invite_id,
        )

    def _resolve_cross_currency_amounts(
        self,
        debt_amount: Decimal,
        debt_currency: str,
        account_currency: str,
        *,
        account_amount: Decimal | None = None,
        exchange_rate: Decimal | None = None,
    ) -> tuple[Decimal | None, Decimal | None]:
        debt_currency = normalize_currency(debt_currency) or "UAH"
        account_currency = normalize_currency(account_currency) or "UAH"
        debt_amount = quantize_money(debt_amount)
        if debt_currency == account_currency:
            return debt_amount, None

        if account_amount is None and exchange_rate is None:
            return None, None

        if account_amount is not None:
            normalized_account_amount = quantize_money(Decimal(str(account_amount)))
            if normalized_account_amount <= 0:
                return None, None
            if exchange_rate is None:
                if debt_amount <= 0:
                    return None, None
                exchange_rate = (normalized_account_amount / debt_amount).quantize(Decimal("0.0001"))
            return normalized_account_amount, Decimal(str(exchange_rate))

        assert exchange_rate is not None
        calculation = calculate_transfer_amount(debt_currency, account_currency, debt_amount, Decimal(str(exchange_rate)))
        return calculation.target_amount, Decimal(str(exchange_rate))
