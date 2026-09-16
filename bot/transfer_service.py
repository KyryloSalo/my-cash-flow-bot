from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

import asyncpg

from finance_scope import get_current_finance_scope
from finance import (
    CREDIT_ACCOUNT_TYPE,
    calculate_transfer_amount,
    default_non_negative_account_type,
    normalize_account_type,
    normalize_currency,
    quantize_money,
    resolve_runtime_account_type,
)

SUCCESS_TRANSFER_STATUSES = {"completed", "account_switched_to_credit", "account_restored_from_credit"}
TRANSFER_ACCOUNT_FIELDS = """
id, label, currency, balance, account_type, credit_limit, monthly_interest_rate, non_negative_account_type
"""


@dataclass(frozen=True)
class TransferPreview:
    target_amount: Decimal
    rate_text: str | None
    preview_source_balance: Decimal
    preview_target_balance: Decimal
    rate_source: str


@dataclass(frozen=True)
class TransferCommitResult:
    status: Literal[
        "completed",
        "accounts_missing",
        "same_account",
        "insufficient_funds",
        "credit_limit_required",
        "credit_limit_exceeded",
        "account_switched_to_credit",
        "account_restored_from_credit",
    ]
    source_account: asyncpg.Record | None = None
    target_account: asyncpg.Record | None = None
    source_balance: Decimal | None = None
    target_amount: Decimal | None = None
    rate_text: str | None = None
    rate_source: str | None = None
    source_previous_balance: Decimal | None = None
    target_previous_balance: Decimal | None = None
    source_new_balance: Decimal | None = None
    target_new_balance: Decimal | None = None
    source_previous_account_type: str | None = None
    target_previous_account_type: str | None = None
    source_new_account_type: str | None = None
    target_new_account_type: str | None = None
    source_credit_limit: Decimal | None = None


class TransferService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    @staticmethod
    def prepare_preview(
        *,
        source_currency: str,
        target_currency: str,
        source_amount: Decimal,
        source_balance: Decimal,
        target_balance: Decimal,
        fx_rate: Decimal | None,
        rate_source: str | None,
    ) -> TransferPreview:
        normalized_source_currency = normalize_currency(source_currency)
        normalized_target_currency = normalize_currency(target_currency or source_currency)
        calculation = calculate_transfer_amount(
            normalized_source_currency,
            normalized_target_currency,
            source_amount,
            fx_rate,
        )
        effective_rate_source = (rate_source or "manual") if fx_rate is not None else (rate_source or "bot")
        return TransferPreview(
            target_amount=calculation.target_amount,
            rate_text=calculation.rate_text,
            preview_source_balance=quantize_money(source_balance - source_amount),
            preview_target_balance=quantize_money(target_balance + calculation.target_amount),
            rate_source=effective_rate_source,
        )

    async def _execute_transfer_core(
        self,
        tg_user_id: int,
        *,
        source_account_id: int,
        target_account_id: int,
        source_currency: str,
        target_currency: str,
        source_amount: Decimal,
        fx_rate: Decimal | None,
        rate_source: str | None,
        transfer_subtype: str | None,
        comment: str | None,
        transaction_date,
        credit_limit_override: Decimal | None,
    ) -> TransferCommitResult:
        normalized_source_currency = normalize_currency(source_currency)
        normalized_target_currency = normalize_currency(target_currency or source_currency)
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        if scope.is_family:
            locked_accounts = await self.conn.fetch(
                """
                SELECT
                    """
                + TRANSFER_ACCOUNT_FIELDS
                + """
                FROM accounts
                WHERE family_id=$1 AND id = ANY($2::bigint[]) AND is_active=true
                ORDER BY id
                FOR UPDATE
                """,
                int(scope.family_id),
                [source_account_id, target_account_id],
            )
        else:
            locked_accounts = await self.conn.fetch(
                """
                SELECT
                    """
                + TRANSFER_ACCOUNT_FIELDS
                + """
                FROM accounts
                WHERE tg_user_id=$1 AND family_id IS NULL AND id = ANY($2::bigint[]) AND is_active=true
                ORDER BY id
                FOR UPDATE
                """,
                tg_user_id,
                [source_account_id, target_account_id],
            )
        by_id = {int(row["id"]): row for row in locked_accounts}
        source_account = by_id.get(source_account_id)
        target_account = by_id.get(target_account_id)
        if source_account is None or target_account is None:
            return TransferCommitResult(status="accounts_missing")
        if source_account_id == target_account_id:
            return TransferCommitResult(
                status="same_account",
                source_account=source_account,
                target_account=target_account,
            )

        source_balance = Decimal(str(source_account["balance"] or 0))
        calculation = calculate_transfer_amount(
            normalized_source_currency,
            normalized_target_currency,
            source_amount,
            fx_rate,
        )
        target_amount = calculation.target_amount
        target_balance = Decimal(str(target_account["balance"] or 0))
        source_new_balance = quantize_money(source_balance - source_amount)
        target_new_balance = quantize_money(target_balance + target_amount)
        source_account_type = normalize_account_type(str(source_account["account_type"] or "other"))
        target_account_type = normalize_account_type(str(target_account["account_type"] or "other"))
        source_non_negative_account_type = default_non_negative_account_type(
            str(source_account["non_negative_account_type"] or source_account_type or "main")
        )
        target_non_negative_account_type = default_non_negative_account_type(
            str(target_account["non_negative_account_type"] or target_account_type or "main")
        )
        current_credit_limit = source_account["credit_limit"]
        normalized_source_credit_limit = (
            quantize_money(Decimal(str(current_credit_limit)))
            if current_credit_limit is not None
            else None
        )
        normalized_credit_limit_override = None
        if credit_limit_override is not None:
            normalized_credit_limit_override = quantize_money(Decimal(str(credit_limit_override)))

        if source_new_balance < 0:
            if source_account_type != CREDIT_ACCOUNT_TYPE and normalized_source_credit_limit is None and normalized_credit_limit_override is None:
                return TransferCommitResult(
                    status="credit_limit_required",
                    source_account=source_account,
                    target_account=target_account,
                    source_balance=source_balance,
                    source_previous_balance=source_balance,
                    target_previous_balance=target_balance,
                    target_amount=target_amount,
                    rate_text=calculation.rate_text,
                    rate_source=rate_source if normalized_source_currency != normalized_target_currency else None,
                    source_new_balance=source_new_balance,
                    target_new_balance=target_new_balance,
                    source_previous_account_type=source_account_type,
                    target_previous_account_type=target_account_type,
                )
            effective_source_credit_limit = normalized_credit_limit_override or normalized_source_credit_limit
            if effective_source_credit_limit is None or abs(source_new_balance) > effective_source_credit_limit:
                return TransferCommitResult(
                    status="credit_limit_exceeded",
                    source_account=source_account,
                    target_account=target_account,
                    source_balance=source_balance,
                    source_previous_balance=source_balance,
                    target_previous_balance=target_balance,
                    target_amount=target_amount,
                    rate_text=calculation.rate_text,
                    rate_source=rate_source if normalized_source_currency != normalized_target_currency else None,
                    source_new_balance=source_new_balance,
                    target_new_balance=target_new_balance,
                    source_previous_account_type=source_account_type,
                    target_previous_account_type=target_account_type,
                    source_credit_limit=effective_source_credit_limit,
                )
            normalized_source_credit_limit = effective_source_credit_limit

        source_new_account_type = resolve_runtime_account_type(
            balance=source_new_balance,
            non_negative_account_type=source_non_negative_account_type,
            current_account_type=source_account_type,
        )
        target_new_account_type = resolve_runtime_account_type(
            balance=target_new_balance,
            non_negative_account_type=target_non_negative_account_type,
            current_account_type=target_account_type,
        )

        if scope.is_family:
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, account_type=$4, credit_limit=$5, non_negative_account_type=$6, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                source_account_id,
                source_new_balance,
                source_new_account_type,
                normalized_source_credit_limit,
                source_non_negative_account_type,
            )
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, account_type=$4, non_negative_account_type=$5, updated_at=now() WHERE family_id=$1 AND id=$2",
                int(scope.family_id),
                target_account_id,
                target_new_balance,
                target_new_account_type,
                target_non_negative_account_type,
            )
        else:
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, account_type=$4, credit_limit=$5, non_negative_account_type=$6, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
                tg_user_id,
                source_account_id,
                source_new_balance,
                source_new_account_type,
                normalized_source_credit_limit,
                source_non_negative_account_type,
            )
            await self.conn.execute(
                "UPDATE accounts SET balance=$3, account_type=$4, non_negative_account_type=$5, updated_at=now() WHERE tg_user_id=$1 AND family_id IS NULL AND id=$2",
                tg_user_id,
                target_account_id,
                target_new_balance,
                target_new_account_type,
                target_non_negative_account_type,
            )
        await self.conn.execute(
            """
            INSERT INTO transactions (
              tg_user_id, family_id, created_by_user_id, updated_by_user_id, date, type,
              amount, currency,
              to_amount, to_currency, fx_rate, fx_rate_text,
              fx_rate_source,
              comment, source,
              account_id, from_account_id, to_account_id,
              flow_kind, transfer_subtype
            )
            VALUES (
              $1, $2, $1, $1, $3, 'transfer',
              $4, $5,
              $6, $7, $8, $9,
              $10, $11, $12,
              $13, $14, $15,
              'transfer', $16
            )
            """,
            tg_user_id,
            scope.family_id,
            transaction_date or datetime.now().date(),
            source_amount,
            normalized_source_currency,
            target_amount,
            normalized_target_currency,
            fx_rate,
            calculation.rate_text,
            rate_source if normalized_source_currency != normalized_target_currency else None,
            (comment or "").strip()[:500] or None,
            "transfer",
            source_account_id,
            source_account_id,
            target_account_id,
            (transfer_subtype or "").strip()[:50] or None,
        )

        result_status: Literal[
            "completed",
            "account_switched_to_credit",
            "account_restored_from_credit",
        ] = "completed"
        if source_account_type != CREDIT_ACCOUNT_TYPE and source_new_account_type == CREDIT_ACCOUNT_TYPE:
            result_status = "account_switched_to_credit"
        elif (
            source_account_type == CREDIT_ACCOUNT_TYPE
            and source_new_account_type != CREDIT_ACCOUNT_TYPE
        ) or (
            target_account_type == CREDIT_ACCOUNT_TYPE
            and target_new_account_type != CREDIT_ACCOUNT_TYPE
        ):
            result_status = "account_restored_from_credit"

        source_snapshot = dict(source_account)
        source_snapshot["balance"] = source_new_balance
        source_snapshot["account_type"] = source_new_account_type
        source_snapshot["credit_limit"] = normalized_source_credit_limit
        source_snapshot["non_negative_account_type"] = source_non_negative_account_type
        target_snapshot = dict(target_account)
        target_snapshot["balance"] = target_new_balance
        target_snapshot["account_type"] = target_new_account_type
        target_snapshot["non_negative_account_type"] = target_non_negative_account_type

        return TransferCommitResult(
            status=result_status,
            source_account=source_snapshot,
            target_account=target_snapshot,
            source_balance=source_balance,
            source_previous_balance=source_balance,
            target_previous_balance=target_balance,
            target_amount=target_amount,
            rate_text=calculation.rate_text,
            rate_source=rate_source if normalized_source_currency != normalized_target_currency else None,
            source_new_balance=source_new_balance,
            target_new_balance=target_new_balance,
            source_previous_account_type=source_account_type,
            target_previous_account_type=target_account_type,
            source_new_account_type=source_new_account_type,
            target_new_account_type=target_new_account_type,
            source_credit_limit=normalized_source_credit_limit,
        )

    async def execute_transfer(
        self,
        tg_user_id: int,
        *,
        source_account_id: int,
        target_account_id: int,
        source_currency: str,
        target_currency: str,
        source_amount: Decimal,
        fx_rate: Decimal | None,
        rate_source: str | None,
        transfer_subtype: str | None = None,
        comment: str | None = None,
        transaction_date=None,
        manage_transaction: bool = True,
        credit_limit_override: Decimal | None = None,
    ) -> TransferCommitResult:
        if manage_transaction:
            async with self.conn.transaction():
                return await self._execute_transfer_core(
                    tg_user_id,
                    source_account_id=source_account_id,
                    target_account_id=target_account_id,
                    source_currency=source_currency,
                    target_currency=target_currency,
                    source_amount=source_amount,
                    fx_rate=fx_rate,
                    rate_source=rate_source,
                    transfer_subtype=transfer_subtype,
                    comment=comment,
                    transaction_date=transaction_date,
                    credit_limit_override=credit_limit_override,
                )
        return await self._execute_transfer_core(
            tg_user_id,
            source_account_id=source_account_id,
            target_account_id=target_account_id,
            source_currency=source_currency,
            target_currency=target_currency,
            source_amount=source_amount,
            fx_rate=fx_rate,
            rate_source=rate_source,
            transfer_subtype=transfer_subtype,
            comment=comment,
            transaction_date=transaction_date,
            credit_limit_override=credit_limit_override,
        )
