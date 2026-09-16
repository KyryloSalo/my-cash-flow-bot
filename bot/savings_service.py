from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

import asyncpg

from account_service import AccountService
from finance import SAVINGS_ACCOUNT_TYPES, normalize_account_type, normalize_currency, quantize_money
from transfer_service import SUCCESS_TRANSFER_STATUSES, TransferCommitResult, TransferService


@dataclass(frozen=True)
class PendingSavingTaskCreateResult:
    id: int
    amount: Decimal
    currency: str


@dataclass(frozen=True)
class PendingSavingTaskConfirmResult:
    status: Literal[
        "completed",
        "task_missing",
        "task_not_pending",
        "stale_preview",
        "accounts_missing",
        "same_account",
        "insufficient_funds",
    ]
    task: asyncpg.Record | None = None
    transfer_result: TransferCommitResult | None = None


class SavingsService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_or_create_settings(self, tg_user_id: int) -> asyncpg.Record:
        await self.conn.execute(
            """
            INSERT INTO saving_prompt_settings (
              tg_user_id, enabled, default_percent, secondary_percent,
              ask_after_income, ask_only_for_salary, reminder_enabled,
              playful_tone_enabled, created_at, updated_at
            )
            VALUES ($1, true, 10, 5, true, false, true, true, now(), now())
            ON CONFLICT (tg_user_id) DO NOTHING
            """,
            tg_user_id,
        )
        return await self.conn.fetchrow(
            """
            SELECT *
            FROM saving_prompt_settings
            WHERE tg_user_id=$1
            """,
            tg_user_id,
        )

    async def update_settings(self, tg_user_id: int, **fields) -> asyncpg.Record:
        await self.get_or_create_settings(tg_user_id)
        allowed_fields = {
            "enabled",
            "default_percent",
            "secondary_percent",
            "default_target_account_id",
            "ask_after_income",
            "ask_only_for_salary",
            "min_income_amount",
            "reminder_enabled",
            "default_reminder_delay",
            "playful_tone_enabled",
        }
        updates = [(key, value) for key, value in fields.items() if key in allowed_fields]
        if not updates:
            return await self.get_or_create_settings(tg_user_id)

        assignments = ", ".join(f"{key}=${index + 2}" for index, (key, _value) in enumerate(updates))
        values = [tg_user_id, *[value for _key, value in updates]]
        await self.conn.execute(
            f"""
            UPDATE saving_prompt_settings
            SET {assignments}, updated_at=now()
            WHERE tg_user_id=$1
            """,
            *values,
        )
        return await self.get_or_create_settings(tg_user_id)

    async def get_active_target_accounts(self, tg_user_id: int) -> list[asyncpg.Record]:
        accounts = await AccountService(self.conn).get_accounts_full(tg_user_id)
        return [row for row in accounts if is_savings_account_type(str(row["account_type"] or ""))]

    async def create_pending_task(
        self,
        tg_user_id: int,
        *,
        income_transaction_id: int | None,
        source_account_id: int,
        target_account_id: int,
        amount: Decimal,
        currency: str,
        percent_from_income: Decimal | None,
        income_amount: Decimal | None,
    ) -> PendingSavingTaskCreateResult:
        row = await self.conn.fetchrow(
            """
            INSERT INTO pending_saving_tasks (
              tg_user_id, income_transaction_id,
              source_account_id, target_account_id,
              amount, currency,
              percent_from_income, income_amount,
              status, created_at
            )
            VALUES (
              $1, $2,
              $3, $4,
              $5, $6,
              $7, $8,
              'pending', now()
            )
            RETURNING id, amount, currency
            """,
            tg_user_id,
            income_transaction_id,
            source_account_id,
            target_account_id,
            quantize_money(Decimal(str(amount))),
            normalize_currency(currency) or "UAH",
            percent_from_income,
            income_amount,
        )
        return PendingSavingTaskCreateResult(
            id=int(row["id"]),
            amount=Decimal(str(row["amount"] or 0)),
            currency=normalize_currency(str(row["currency"] or "UAH")) or "UAH",
        )

    async def get_task(self, tg_user_id: int, task_id: int) -> asyncpg.Record | None:
        return await self.conn.fetchrow(
            """
            SELECT pst.*, src.label AS source_label, src.currency AS source_currency,
                   dst.label AS target_label, dst.currency AS target_currency,
                   dst.balance AS target_balance
            FROM pending_saving_tasks pst
            JOIN accounts src ON src.id = pst.source_account_id
            JOIN accounts dst ON dst.id = pst.target_account_id
            WHERE pst.tg_user_id=$1 AND pst.id=$2
            """,
            tg_user_id,
            task_id,
        )

    async def update_task_amount(self, tg_user_id: int, task_id: int, amount: Decimal) -> asyncpg.Record | None:
        await self.conn.execute(
            """
            UPDATE pending_saving_tasks
            SET amount=$3
            WHERE tg_user_id=$1 AND id=$2 AND status='pending'
            """,
            tg_user_id,
            task_id,
            quantize_money(Decimal(str(amount))),
        )
        return await self.get_task(tg_user_id, task_id)

    async def update_task_target(self, tg_user_id: int, task_id: int, target_account_id: int) -> asyncpg.Record | None:
        await self.conn.execute(
            """
            UPDATE pending_saving_tasks
            SET target_account_id=$3
            WHERE tg_user_id=$1 AND id=$2 AND status='pending'
            """,
            tg_user_id,
            task_id,
            target_account_id,
        )
        return await self.get_task(tg_user_id, task_id)

    async def cancel_task(self, tg_user_id: int, task_id: int) -> asyncpg.Record | None:
        await self.conn.execute(
            """
            UPDATE pending_saving_tasks
            SET status='cancelled', cancelled_at=now(), remind_at=NULL
            WHERE tg_user_id=$1 AND id=$2 AND status='pending'
            """,
            tg_user_id,
            task_id,
        )
        return await self.get_task(tg_user_id, task_id)

    async def set_task_reminder(self, tg_user_id: int, task_id: int, remind_at: datetime | None) -> asyncpg.Record | None:
        await self.conn.execute(
            """
            UPDATE pending_saving_tasks
            SET remind_at=$3, reminded_at=NULL
            WHERE tg_user_id=$1 AND id=$2 AND status='pending'
            """,
            tg_user_id,
            task_id,
            remind_at,
        )
        return await self.get_task(tg_user_id, task_id)

    async def list_due_reminders(self, now: datetime, limit: int = 50) -> list[asyncpg.Record]:
        return await self.conn.fetch(
            """
            SELECT pst.*, src.label AS source_label, src.currency AS source_currency,
                   dst.label AS target_label, dst.currency AS target_currency
            FROM pending_saving_tasks pst
            JOIN accounts src ON src.id = pst.source_account_id
            JOIN accounts dst ON dst.id = pst.target_account_id
            WHERE pst.status='pending'
              AND pst.remind_at IS NOT NULL
              AND pst.remind_at <= $1
              AND (pst.reminded_at IS NULL OR pst.reminded_at < pst.remind_at)
            ORDER BY pst.remind_at ASC, pst.id ASC
            LIMIT $2
            """,
            now,
            limit,
        )

    async def mark_task_reminded(self, task_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE pending_saving_tasks
            SET reminded_at=now()
            WHERE id=$1
            """,
            task_id,
        )

    async def confirm_pending_task(
        self,
        tg_user_id: int,
        task_id: int,
        *,
        fx_rate: Decimal | None,
        rate_source: str | None,
        expected_task: dict | None = None,
    ) -> PendingSavingTaskConfirmResult:
        async with self.conn.transaction():
            task = await self.conn.fetchrow(
                """
                SELECT pst.*,
                       src.label AS source_label,
                       src.currency AS source_currency,
                       dst.label AS target_label,
                       dst.currency AS target_currency,
                       dst.balance AS target_balance
                FROM pending_saving_tasks pst
                JOIN accounts src ON src.id = pst.source_account_id
                JOIN accounts dst ON dst.id = pst.target_account_id
                WHERE pst.tg_user_id=$1 AND pst.id=$2
                FOR UPDATE
                """,
                tg_user_id,
                task_id,
            )
            if task is None:
                return PendingSavingTaskConfirmResult(status="task_missing")
            if str(task["status"] or "") != "pending":
                return PendingSavingTaskConfirmResult(status="task_not_pending", task=task)

            if expected_task is not None and (
                Decimal(str(task["amount"])) != Decimal(str(expected_task.get("amount") or 0))
                or any(int(task[key]) != int(expected_task.get(key) or 0) for key in ("source_account_id", "target_account_id"))
                or any(normalize_currency(str(task[key])) != normalize_currency(str(expected_task.get(key) or "")) for key in ("source_currency", "target_currency"))
            ):
                return PendingSavingTaskConfirmResult(status="stale_preview", task=task)

            transfer_result = await TransferService(self.conn).execute_transfer(
                tg_user_id,
                source_account_id=int(task["source_account_id"]),
                target_account_id=int(task["target_account_id"]),
                source_currency=normalize_currency(str(task["source_currency"] or task["currency"] or "UAH")),
                target_currency=normalize_currency(str(task["target_currency"] or task["currency"] or "UAH")),
                source_amount=Decimal(str(task["amount"] or 0)),
                fx_rate=fx_rate,
                rate_source=rate_source,
                transfer_subtype="savings_transfer",
                comment="saving transfer",
                manage_transaction=False,
            )
            if transfer_result.status not in SUCCESS_TRANSFER_STATUSES:
                return PendingSavingTaskConfirmResult(
                    status=transfer_result.status,
                    task=task,
                    transfer_result=transfer_result,
                )

            await self.conn.execute(
                """
                UPDATE pending_saving_tasks
                SET status='completed', completed_at=now(), remind_at=NULL
                WHERE tg_user_id=$1 AND id=$2
                """,
                tg_user_id,
                task_id,
            )
            return PendingSavingTaskConfirmResult(
                status="completed",
                task=task,
                transfer_result=transfer_result,
            )

    async def record_completed_transfer(
        self,
        tg_user_id: int,
        *,
        source_account_id: int,
        target_account_id: int,
        source_currency: str,
        target_currency: str,
        amount: Decimal,
        fx_rate: Decimal | None,
        rate_source: str | None,
        transfer_subtype: str = "savings_transfer",
        comment: str | None = "saving transfer",
    ) -> TransferCommitResult:
        return await TransferService(self.conn).execute_transfer(
            tg_user_id,
            source_account_id=source_account_id,
            target_account_id=target_account_id,
            source_currency=source_currency,
            target_currency=target_currency,
            source_amount=quantize_money(Decimal(str(amount))),
            fx_rate=fx_rate,
            rate_source=rate_source,
            transfer_subtype=transfer_subtype,
            comment=comment,
        )


def is_savings_account_type(account_type: str) -> bool:
    return normalize_account_type(account_type) in SAVINGS_ACCOUNT_TYPES
