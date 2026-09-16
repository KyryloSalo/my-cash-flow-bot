from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import logging
from typing import Literal

import asyncpg

from finance_scope import get_current_finance_scope
from finance import (
    CREDIT_ACCOUNT_TYPE,
    default_non_negative_account_type,
    normalize_account_type,
    normalize_currency,
    quantize_money,
    resolve_runtime_account_type,
)

logger = logging.getLogger("mcf-bot.transaction-service")

SUCCESS_TRANSACTION_STATUSES = {"completed", "account_switched_to_credit", "account_restored_from_credit"}
TRANSACTION_ACCOUNT_FIELDS = """
id, tg_user_id, family_id, created_by_user_id, label, currency, account_type,
starting_balance, balance, is_active, created_at, updated_at,
credit_limit, monthly_interest_rate, non_negative_account_type
"""


@dataclass(frozen=True)
class NormalTransactionCommitResult:
    status: Literal[
        "completed",
        "account_missing",
        "invalid_payload",
        "currency_account_mismatch",
        "credit_limit_required",
        "credit_limit_exceeded",
        "account_switched_to_credit",
        "account_restored_from_credit",
    ]
    account: asyncpg.Record | None = None
    transaction_date: date | None = None
    kind: str | None = None
    amount: Decimal | None = None
    currency: str | None = None
    category_label: str | None = None
    comment: str | None = None
    source: str | None = None
    new_balance: Decimal | None = None
    previous_balance: Decimal | None = None
    previous_account_type: str | None = None
    new_account_type: str | None = None
    non_negative_account_type: str | None = None
    credit_limit: Decimal | None = None
    projected_balance: Decimal | None = None
    transaction_id: int | None = None


class TransactionService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def commit_normal_transaction(
        self,
        tg_user_id: int,
        *,
        transaction_date: date,
        kind: str,
        amount: Decimal,
        currency: str,
        account_id: int,
        category_id: int,
        category_label: str | None,
        comment: str | None,
        source: str,
        credit_limit_override: Decimal | None = None,
    ) -> NormalTransactionCommitResult:
        normalized_kind = (kind or "").strip().lower()
        if normalized_kind not in {"income", "expense"}:
            return NormalTransactionCommitResult(status="invalid_payload")

        normalized_amount = quantize_money(Decimal(str(amount)))
        if normalized_amount <= 0:
            return NormalTransactionCommitResult(status="invalid_payload")

        normalized_currency = normalize_currency(currency) or "UAH"
        normalized_comment = (comment or "").strip()[:500] or None
        normalized_source = (source or "").strip()[:50] or "text"
        balance_delta = normalized_amount if normalized_kind == "income" else -normalized_amount
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        normalized_credit_limit_override = None
        if credit_limit_override is not None:
            normalized_credit_limit_override = quantize_money(Decimal(str(credit_limit_override)))

        async with self.conn.transaction():
            if scope.is_family:
                account = await self.conn.fetchrow(
                    """
                    SELECT
                        """
                    + TRANSACTION_ACCOUNT_FIELDS
                    + """
                    FROM accounts
                    WHERE family_id=$1 AND id=$2 AND is_active=true
                    FOR UPDATE
                    """,
                    int(scope.family_id),
                    account_id,
                )
            else:
                account = await self.conn.fetchrow(
                    """
                    SELECT
                        """
                    + TRANSACTION_ACCOUNT_FIELDS
                    + """
                    FROM accounts
                    WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2 AND is_active=true
                    FOR UPDATE
                    """,
                    tg_user_id,
                    account_id,
                )
            if account is None:
                logger.warning(
                    "commit_normal_transaction account_missing tg_user_id=%s account_id=%s scope_is_family=%s family_id=%s kind=%s amount=%s currency=%s",
                    tg_user_id,
                    account_id,
                    bool(scope.is_family),
                    getattr(scope, "family_id", None),
                    normalized_kind,
                    normalized_amount,
                    normalized_currency,
                )
                return NormalTransactionCommitResult(status="account_missing")

            account_currency = normalize_currency(str(account["currency"] or "UAH")) or "UAH"
            if account_currency != normalized_currency:
                logger.warning(
                    "commit_normal_transaction currency_account_mismatch tg_user_id=%s account_id=%s scope_is_family=%s family_id=%s kind=%s amount=%s tx_currency=%s account_currency=%s",
                    tg_user_id,
                    account_id,
                    bool(scope.is_family),
                    getattr(scope, "family_id", None),
                    normalized_kind,
                    normalized_amount,
                    normalized_currency,
                    account_currency,
                )
                return NormalTransactionCommitResult(
                    status="currency_account_mismatch",
                    account=account,
                    transaction_date=transaction_date,
                    kind=normalized_kind,
                    amount=normalized_amount,
                    currency=normalized_currency,
                    category_label=category_label,
                    comment=normalized_comment,
                    source=normalized_source,
                )

            current_balance = Decimal(str(account["balance"] or 0))
            new_balance = quantize_money(current_balance + balance_delta)
            current_account_type = normalize_account_type(str(account["account_type"] or "other"))
            non_negative_account_type = default_non_negative_account_type(
                str(account["non_negative_account_type"] or current_account_type or "main")
            )
            current_credit_limit = account["credit_limit"]
            normalized_credit_limit = (
                quantize_money(Decimal(str(current_credit_limit)))
                if current_credit_limit is not None
                else None
            )
            if normalized_kind == "expense" and new_balance < 0:
                effective_credit_limit = normalized_credit_limit_override or normalized_credit_limit
                if effective_credit_limit is None:
                    logger.info(
                        "commit_normal_transaction auto_credit_without_limit tg_user_id=%s account_id=%s kind=%s amount=%s currency=%s current_balance=%s projected_balance=%s account_type=%s",
                        tg_user_id,
                        account_id,
                        normalized_kind,
                        normalized_amount,
                        normalized_currency,
                        current_balance,
                        new_balance,
                        current_account_type,
                    )
                elif abs(new_balance) > effective_credit_limit:
                    logger.info(
                        "commit_normal_transaction credit_limit_exceeded tg_user_id=%s account_id=%s kind=%s amount=%s currency=%s current_balance=%s projected_balance=%s credit_limit=%s override=%s account_type=%s",
                        tg_user_id,
                        account_id,
                        normalized_kind,
                        normalized_amount,
                        normalized_currency,
                        current_balance,
                        new_balance,
                        effective_credit_limit,
                        normalized_credit_limit_override,
                        current_account_type,
                    )
                    return NormalTransactionCommitResult(
                        status="credit_limit_exceeded",
                        account=account,
                        transaction_date=transaction_date,
                        kind=normalized_kind,
                        amount=normalized_amount,
                        currency=normalized_currency,
                        category_label=category_label,
                        comment=normalized_comment,
                        source=normalized_source,
                        previous_balance=current_balance,
                        projected_balance=new_balance,
                        previous_account_type=current_account_type,
                        non_negative_account_type=non_negative_account_type,
                        credit_limit=effective_credit_limit,
                    )
                else:
                    normalized_credit_limit = effective_credit_limit

            new_account_type = resolve_runtime_account_type(
                balance=new_balance,
                non_negative_account_type=non_negative_account_type,
                current_account_type=current_account_type,
            )
            if normalized_credit_limit_override is not None and normalized_credit_limit is None:
                normalized_credit_limit = normalized_credit_limit_override

            transaction_id = await self.conn.fetchval(
                """
                INSERT INTO transactions (
                  tg_user_id, family_id, created_by_user_id, updated_by_user_id,
                  date, type, amount, currency,
                  comment, source,
                  account_id, category_id, category_name_snapshot,
                  flow_kind
                )
                VALUES (
                  $1, $2, $1, $1,
                  $3, $4, $5, $6,
                  $7, $8,
                  $9, $10, $11,
                  'normal'
                )
                RETURNING id
                """,
                tg_user_id,
                scope.family_id,
                transaction_date,
                normalized_kind,
                normalized_amount,
                normalized_currency,
                normalized_comment,
                normalized_source,
                account_id,
                category_id,
                category_label,
            )
            if scope.is_family:
                await self.conn.execute(
                    "UPDATE accounts SET balance=$3, account_type=$4, credit_limit=$5, non_negative_account_type=$6, updated_at=now() WHERE family_id=$1 AND id=$2",
                    int(scope.family_id),
                    account_id,
                    new_balance,
                    new_account_type,
                    normalized_credit_limit,
                    non_negative_account_type,
                )
            else:
                await self.conn.execute(
                    "UPDATE accounts SET balance=$3, account_type=$4, credit_limit=$5, non_negative_account_type=$6, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
                    tg_user_id,
                    account_id,
                    new_balance,
                    new_account_type,
                    normalized_credit_limit,
                    non_negative_account_type,
                )

        result_status: Literal[
            "completed",
            "account_switched_to_credit",
            "account_restored_from_credit",
        ] = "completed"
        if current_account_type != CREDIT_ACCOUNT_TYPE and new_account_type == CREDIT_ACCOUNT_TYPE:
            result_status = "account_switched_to_credit"
        elif current_account_type == CREDIT_ACCOUNT_TYPE and new_account_type != CREDIT_ACCOUNT_TYPE:
            result_status = "account_restored_from_credit"

        logger.info(
            "commit_normal_transaction success status=%s transaction_id=%s tg_user_id=%s account_id=%s kind=%s amount=%s currency=%s previous_balance=%s new_balance=%s previous_account_type=%s new_account_type=%s credit_limit=%s",
            result_status,
            transaction_id,
            tg_user_id,
            account_id,
            normalized_kind,
            normalized_amount,
            normalized_currency,
            current_balance,
            new_balance,
            current_account_type,
            new_account_type,
            normalized_credit_limit,
        )

        account_snapshot = dict(account)
        account_snapshot["balance"] = new_balance
        account_snapshot["account_type"] = new_account_type
        account_snapshot["credit_limit"] = normalized_credit_limit
        account_snapshot["non_negative_account_type"] = non_negative_account_type

        return NormalTransactionCommitResult(
            status=result_status,
            account=account_snapshot,
            transaction_date=transaction_date,
            kind=normalized_kind,
            amount=normalized_amount,
            currency=normalized_currency,
            category_label=category_label,
            comment=normalized_comment,
            source=normalized_source,
            new_balance=new_balance,
            previous_balance=current_balance,
            previous_account_type=current_account_type,
            new_account_type=new_account_type,
            non_negative_account_type=non_negative_account_type,
            credit_limit=normalized_credit_limit,
            projected_balance=new_balance,
            transaction_id=int(transaction_id) if transaction_id is not None else None,
        )
