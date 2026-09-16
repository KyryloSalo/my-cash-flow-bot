from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

from finance import normalize_currency, quantize_money


@dataclass(frozen=True)
class AiTransactionDraft:
    id: int
    tg_user_id: int
    source: str
    telegram_file_unique_id: str
    telegram_file_id: str | None
    telegram_message_id: int | None
    status: str
    transaction_date: date | None
    tx_type: str | None
    amount: Decimal | None
    currency: str | None
    account_id: int | None
    category_id: int | None
    comment: str | None
    confidence: float | None
    metadata: dict[str, Any]
    created_at: datetime | None
    updated_at: datetime | None
    confirmed_at: datetime | None
    cancelled_at: datetime | None


def _normalize_amount(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        normalized = quantize_money(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None
    return normalized if normalized > 0 else None


def _normalize_type(value: Any) -> str | None:
    tx_type = str(value or "").strip().lower()
    return tx_type if tx_type in {"expense", "income"} else None


def _normalize_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _row_to_draft(row: asyncpg.Record | dict[str, Any] | None) -> AiTransactionDraft | None:
    if row is None:
        return None
    return AiTransactionDraft(
        id=int(row["id"]),
        tg_user_id=int(row["tg_user_id"]),
        source=str(row["source"]),
        telegram_file_unique_id=str(row["telegram_file_unique_id"]),
        telegram_file_id=(str(row["telegram_file_id"]) if row.get("telegram_file_id") else None),
        telegram_message_id=(int(row["telegram_message_id"]) if row.get("telegram_message_id") is not None else None),
        status=str(row["status"]),
        transaction_date=row.get("transaction_date"),
        tx_type=_normalize_type(row.get("tx_type")),
        amount=_normalize_amount(row.get("amount")),
        currency=(normalize_currency(str(row["currency"])) if row.get("currency") else None),
        account_id=(int(row["account_id"]) if row.get("account_id") is not None else None),
        category_id=(int(row["category_id"]) if row.get("category_id") is not None else None),
        comment=(str(row["comment"]) if row.get("comment") else None),
        confidence=(float(row["confidence"]) if row.get("confidence") is not None else None),
        metadata=_normalize_metadata(row.get("metadata")),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
        confirmed_at=row.get("confirmed_at"),
        cancelled_at=row.get("cancelled_at"),
    )


@dataclass
class DraftCommitGuard:
    succeeded: bool = False


class AiTransactionDraftService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    @asynccontextmanager
    async def atomic_confirmation(self, draft_id: int | None, tg_user_id: int):
        """Lock the persisted draft and complete it in the ledger transaction."""
        async with self.conn.transaction():
            if draft_id is not None:
                row = await self.conn.fetchrow(
                    "SELECT status FROM ai_transaction_drafts WHERE id=$1 AND tg_user_id=$2 FOR UPDATE",
                    int(draft_id), int(tg_user_id),
                )
                if row is None or str(row["status"]) != "pending":
                    raise ValueError("draft_not_pending")
            guard = DraftCommitGuard()
            yield guard
            if draft_id is not None and guard.succeeded:
                if await self.mark_completed(int(draft_id), int(tg_user_id)) is None:
                    raise RuntimeError("draft_completion_failed")

    async def get_by_id(self, draft_id: int, tg_user_id: int) -> AiTransactionDraft | None:
        row = await self.conn.fetchrow(
            """
            SELECT *
            FROM ai_transaction_drafts
            WHERE id=$1 AND tg_user_id=$2
            LIMIT 1
            """,
            int(draft_id),
            int(tg_user_id),
        )
        return _row_to_draft(row)

    async def get_by_media_key(self, tg_user_id: int, *, source: str, telegram_file_unique_id: str) -> AiTransactionDraft | None:
        row = await self.conn.fetchrow(
            """
            SELECT *
            FROM ai_transaction_drafts
            WHERE tg_user_id=$1 AND source=$2 AND telegram_file_unique_id=$3
            """,
            tg_user_id,
            source,
            telegram_file_unique_id,
        )
        return _row_to_draft(row)

    async def create_or_get(
        self,
        tg_user_id: int,
        *,
        source: str,
        telegram_file_unique_id: str,
        telegram_file_id: str | None,
        telegram_message_id: int | None,
        tx: dict[str, Any],
        confidence: float | None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[AiTransactionDraft, bool]:
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
        row = await self.conn.fetchrow(
            """
            INSERT INTO ai_transaction_drafts (
              tg_user_id,
              source,
              telegram_file_unique_id,
              telegram_file_id,
              telegram_message_id,
              status,
              transaction_date,
              tx_type,
              amount,
              currency,
              account_id,
              category_id,
              comment,
              confidence,
              metadata,
              created_at,
              updated_at
            )
            VALUES (
              $1, $2, $3, $4, $5,
              'pending',
              $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb,
              now(), now()
            )
            ON CONFLICT (tg_user_id, source, telegram_file_unique_id) DO NOTHING
            RETURNING *
            """,
            tg_user_id,
            source,
            telegram_file_unique_id,
            telegram_file_id,
            telegram_message_id,
            _normalize_date(tx.get("date")),
            _normalize_type(tx.get("type")),
            _normalize_amount(tx.get("amount")),
            normalize_currency(str(tx.get("currency") or "UAH")),
            tx.get("account_id"),
            tx.get("category_id"),
            (str(tx.get("comment") or "").strip()[:500] or None),
            confidence,
            metadata_json,
        )
        created = row is not None
        if row is None:
            row = await self.conn.fetchrow(
                """
                SELECT *
                FROM ai_transaction_drafts
                WHERE tg_user_id=$1 AND source=$2 AND telegram_file_unique_id=$3
                """,
                tg_user_id,
                source,
                telegram_file_unique_id,
            )
        draft = _row_to_draft(row)
        if draft is None:
            raise RuntimeError("AI transaction draft was not created")
        return draft, created

    async def update_draft(
        self,
        draft_id: int,
        tg_user_id: int,
        *,
        tx: dict[str, Any],
        confidence: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> AiTransactionDraft | None:
        row = await self.conn.fetchrow(
            """
            UPDATE ai_transaction_drafts
            SET transaction_date=$3,
                tx_type=$4,
                amount=$5,
                currency=$6,
                account_id=$7,
                category_id=$8,
                comment=$9,
                confidence=$10,
                metadata=COALESCE($11::jsonb, metadata),
                updated_at=now()
            WHERE id=$1 AND tg_user_id=$2 AND status='pending'
            RETURNING *
            """,
            draft_id,
            tg_user_id,
            _normalize_date(tx.get("date")),
            _normalize_type(tx.get("type")),
            _normalize_amount(tx.get("amount")),
            normalize_currency(str(tx.get("currency") or "UAH")),
            tx.get("account_id"),
            tx.get("category_id"),
            (str(tx.get("comment") or "").strip()[:500] or None),
            confidence,
            (json.dumps(metadata, ensure_ascii=False) if metadata is not None else None),
        )
        return _row_to_draft(row)

    async def mark_completed(self, draft_id: int, tg_user_id: int) -> AiTransactionDraft | None:
        row = await self.conn.fetchrow(
            """
            UPDATE ai_transaction_drafts
            SET status='completed',
                confirmed_at=now(),
                updated_at=now()
            WHERE id=$1 AND tg_user_id=$2 AND status='pending'
            RETURNING *
            """,
            draft_id,
            tg_user_id,
        )
        return _row_to_draft(row)

    async def mark_cancelled(self, draft_id: int, tg_user_id: int) -> AiTransactionDraft | None:
        row = await self.conn.fetchrow(
            """
            UPDATE ai_transaction_drafts
            SET status='cancelled',
                cancelled_at=now(),
                updated_at=now()
            WHERE id=$1 AND tg_user_id=$2 AND status='pending'
            RETURNING *
            """,
            draft_id,
            tg_user_id,
        )
        return _row_to_draft(row)
