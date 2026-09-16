from __future__ import annotations

import csv
import io
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from xml.sax.saxutils import escape as xml_escape

import asyncpg

from finance_scope import get_current_finance_scope
from fx_rates import get_latest_rates
from finance import (
    SAVINGS_ACCOUNT_TYPES,
    escape_html,
    format_decimal as format_decimal_value,
    format_money,
    is_asset_account_type,
    normalize_account_type,
    normalize_currency,
    quantize_money,
)

SYSTEM_EXPENSE_CATEGORY_NAME = "Інше"
SYSTEM_INCOME_CATEGORY_NAME = "Інший дохід"


def split_report_text(text: str, *, max_length: int = 4096) -> list[str]:
    """Split generated report HTML, preferring complete lines without truncation.

    Keep the report builders' <b>/<i> tags balanced and entities intact even for
    a single oversized label. The bound includes markup in UTF-16 code units,
    conservatively staying below Telegram's post-parsing text limit. Callers
    send each chunk as HTML and attach navigation only to the last message.
    """
    if type(max_length) is not int or not 32 <= max_length <= 4096:
        raise ValueError("max_length must be an integer between 32 and 4096")
    chunks: list[str] = []
    current: list[str] = []
    open_tags: list[str] = []
    current_units = 0
    last_line_break = None
    formatting_tags = {"<b>", "<i>", "</b>", "</i>"}

    def closing_tags(tags: list[str]) -> str:
        return "".join(tag.replace("<", "</", 1) for tag in reversed(tags))

    tokens = re.finditer(r"</?[bi]>|&(?:[a-zA-Z]+|#\d+|#x[0-9a-fA-F]+);|[\s\S]", text)
    for match in tokens:
        token = match.group()
        next_tags = open_tags
        if token in {"<b>", "<i>"}:
            next_tags = [*open_tags, token]
        elif token in {"</b>", "</i>"}:
            next_tags = open_tags[:-1]
        token_units = len(token.encode("utf-16-le")) // 2
        # Reserve room to close the currently open formatting on every chunk.
        while current_units + token_units + 4 * len(next_tags) > max_length:
            if last_line_break is not None:
                index, units, tags = last_line_break
                chunks.append("".join(current[:index]) + closing_tags(tags))
                current = [*tags, *current[index:]]
                current_units = 3 * len(tags) + current_units - units
            elif current_units > 3 * len(open_tags):
                chunks.append("".join(current) + closing_tags(open_tags))
                current = list(open_tags)
                current_units = 3 * len(open_tags)
            else:
                raise ValueError("max_length is too small for report markup")
            last_line_break = None
        current.append(token)
        current_units += token_units
        open_tags = next_tags
        if token == "\n" and any(piece.strip() for piece in current if piece not in formatting_tags):
            last_line_break = (len(current), current_units, list(open_tags))
    if current:
        chunks.append("".join(current) + closing_tags(open_tags))
    return chunks


CSV_FIELDNAMES = [
    "date",
    "type",
    "flow_kind",
    "created_by_user_id",
    "created_by",
    "account",
    "category",
    "amount",
    "currency",
    "comment",
    "source_account",
    "target_account",
    "source_amount",
    "source_currency",
    "target_amount",
    "target_currency",
    "rate",
    "counterparty",
    "debt_action",
    "original_amount",
    "original_currency",
    "exchange_rate",
    "debt_id",
    "debt_payment_id",
]


def _format_money_map(amounts: dict[str, Decimal]) -> str:
    if not amounts:
        return "0"
    return " + ".join(format_money(value, currency) for currency, value in amounts.items())


def _money_display_order(currency: str, base_currency: str) -> tuple[int, str]:
    normalized_currency = normalize_currency(currency) or currency
    normalized_base = normalize_currency(base_currency) or base_currency
    return (0 if normalized_currency == normalized_base else 1, normalized_currency)


def _format_export_money_block(
    amounts: dict[str, Decimal],
    *,
    base_currency: str,
    converted_total: Decimal | None = None,
) -> str:
    if not amounts:
        return "0"

    lines = [
        format_money(amounts[currency], currency)
        for currency in sorted(amounts, key=lambda item: _money_display_order(item, base_currency))
    ]
    if converted_total is not None:
        normalized_base = normalize_currency(base_currency) or "UAH"
        if len(amounts) > 1 or any((normalize_currency(currency) or currency) != normalized_base for currency in amounts):
            lines.append(f"~ {format_money(converted_total, normalized_base)}")
    return "\n".join(lines)


def _format_export_money_inline(
    amounts: dict[str, Decimal],
    *,
    base_currency: str,
    converted_total: Decimal | None = None,
) -> str:
    return _format_export_money_block(
        amounts,
        base_currency=base_currency,
        converted_total=converted_total,
    ).replace("\n~ ", " • ~ ").replace("\n", " / ")


def _actor_name(row: asyncpg.Record) -> str:
    return str(row.get("author_name") or row.get("first_name") or row.get("username") or "Невідомий")


def _row_category_name(row: asyncpg.Record | dict[str, object]) -> str:
    tx_type = str(row.get("type") or "").strip().lower()
    fallback = SYSTEM_INCOME_CATEGORY_NAME if tx_type == "income" else SYSTEM_EXPENSE_CATEGORY_NAME
    for key in ("category_display_name", "category_name", "category__name", "category", "category_name_snapshot"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return fallback


def _base_export_columns(row: asyncpg.Record) -> dict[str, str]:
    return {
        "date": row["date"].isoformat(),
        "type": str(row["type"] or ""),
        "flow_kind": str(row["flow_kind"] or "normal"),
        "created_by_user_id": str(row.get("created_by_user_id") or row.get("tg_user_id") or ""),
        "created_by": str(row.get("author_name") or ""),
    }


def _format_transfer_export_row(row: asyncpg.Record) -> dict[str, str]:
    base = _base_export_columns(row)
    base.update(
        {
            "type": "transfer",
            "flow_kind": "transfer",
            "account": "",
            "category": "",
            "amount": "",
            "currency": "",
            "comment": str(row["comment"] or ""),
            "source_account": str(row["from_account_id"] or ""),
            "target_account": str(row["to_account_id"] or ""),
            "source_amount": format_decimal_value(Decimal(str(row["amount"] or 0))),
            "source_currency": str(row["currency"] or ""),
            "target_amount": format_decimal_value(Decimal(str(row["to_amount"] or 0))),
            "target_currency": str(row["to_currency"] or ""),
            "rate": format_decimal_value(Decimal(str(row["fx_rate"] or 0)), places=4) if row["fx_rate"] is not None else "",
            "counterparty": "",
            "debt_action": "",
            "original_amount": "",
            "original_currency": "",
            "exchange_rate": "",
            "debt_id": "",
            "debt_payment_id": "",
        }
    )
    return base


def _format_debt_export_row(row: asyncpg.Record) -> dict[str, str]:
    base = _base_export_columns(row)
    base.update(
        {
            "type": str(row["type"] or "transfer"),
            "flow_kind": "debt",
            "account": "",
            "category": "",
            "amount": format_decimal_value(Decimal(str(row["amount"] or 0))),
            "currency": str(row["currency"] or ""),
            "comment": str(row["comment"] or ""),
            "source_account": "",
            "target_account": "",
            "source_amount": "",
            "source_currency": "",
            "target_amount": "",
            "target_currency": "",
            "rate": "",
            "counterparty": str(row["counterparty"] or ""),
            "debt_action": str(row["debt_action"] or ""),
            "original_amount": format_decimal_value(Decimal(str(row.get("original_amount") or 0))) if row.get("original_amount") is not None else "",
            "original_currency": str(row.get("original_currency") or ""),
            "exchange_rate": format_decimal_value(Decimal(str(row.get("exchange_rate") or 0)), places=4) if row.get("exchange_rate") is not None else "",
            "debt_id": str(row.get("debt_id") or ""),
            "debt_payment_id": str(row.get("debt_payment_id") or ""),
        }
    )
    return base


def _format_standard_export_row(row: asyncpg.Record, flow_kind: str) -> dict[str, str]:
    base = _base_export_columns(row)
    base.update(
        {
            "flow_kind": flow_kind,
            "account": str(row["account_id"] or ""),
            "category": _row_category_name(row),
            "amount": format_decimal_value(Decimal(str(row["amount"] or 0))),
            "currency": str(row["currency"] or ""),
            "comment": str(row["comment"] or ""),
            "source_account": "",
            "target_account": "",
            "source_amount": "",
            "source_currency": "",
            "target_amount": "",
            "target_currency": "",
            "rate": "",
            "counterparty": "",
            "debt_action": "",
            "original_amount": "",
            "original_currency": "",
            "exchange_rate": "",
            "debt_id": "",
            "debt_payment_id": "",
        }
    )
    return base


def map_export_row(row: asyncpg.Record) -> dict[str, str]:
    flow_kind = str(row["flow_kind"] or "normal")
    if flow_kind == "transfer":
        return _format_transfer_export_row(row)
    if flow_kind == "debt":
        return _format_debt_export_row(row)
    return _format_standard_export_row(row, flow_kind)


class ReportService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def build_debt_report_text(self, tg_user_id: int) -> str:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        if scope.is_family:
            rows = await self.conn.fetch(
                """
                SELECT counterparty, currency,
                       sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
                       sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
                FROM transactions
                WHERE family_id=$1 AND flow_kind='debt'
                  AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                GROUP BY counterparty, currency
                ORDER BY counterparty ASC
                """,
                int(scope.family_id),
            )
        else:
            rows = await self.conn.fetch(
                """
                SELECT counterparty, currency,
                       sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
                       sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
                FROM transactions
                WHERE tg_user_id=$1 AND family_id IS NULL AND flow_kind='debt'
                  AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                GROUP BY counterparty, currency
                ORDER BY counterparty ASC
                """,
                tg_user_id,
            )

        owed_to_me: list[str] = []
        i_owe: list[str] = []
        for row in rows:
            counterparty = escape_html((row["counterparty"] or "—").strip() or "—")
            currency = (row["currency"] or "").strip()
            owed = Decimal(str(row["owed_to_me"] or 0))
            owe = Decimal(str(row["i_owe"] or 0))
            if owed > 0:
                owed_to_me.append(f"• {counterparty} — <b>{format_money(owed, currency)}</b>")
            if owe > 0:
                i_owe.append(f"• {counterparty} — <b>{format_money(owe, currency)}</b>")

        lines = ["<b>🤝 Борги</b>", "", "<b>Мені винні:</b>"]
        lines += owed_to_me or ["<i>Немає</i>"]
        lines += ["", "<b>Я винен:</b>"]
        lines += i_owe or ["<i>Немає</i>"]
        return "\n".join(lines)

    async def build_normal_report_text(
        self,
        tg_user_id: int,
        start_date: date,
        end_date: date,
        title: str,
        *,
        member_filter_user_id: int | None = None,
        member_filter_label: str | None = None,
    ) -> str | None:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        by_member_rows: list[asyncpg.Record] = []
        operation_rows: list[asyncpg.Record] = []

        if scope.is_family:
            args: list[object] = [int(scope.family_id), start_date, end_date]
            member_clause = ""
            if member_filter_user_id is not None:
                member_clause = "AND COALESCE(t.created_by_user_id, t.tg_user_id)=$4"
                args.append(int(member_filter_user_id))

            totals_rows = await self.conn.fetch(
                f"""
                SELECT t.type, t.currency, sum(t.amount) AS total
                FROM transactions t
                WHERE t.family_id=$1
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.flow_kind='normal'
                  AND t.type IN ('expense','income')
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                  {member_clause}
                GROUP BY t.type, t.currency
                ORDER BY t.type, t.currency
                """,
                *args,
            )
            by_cat_rows = await self.conn.fetch(
                f"""
                SELECT t.type, t.currency, COALESCE(c.name, t.category_name_snapshot) AS category, sum(t.amount) AS total
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.family_id=$1
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.flow_kind='normal'
                  AND t.type IN ('expense','income')
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                  {member_clause}
                GROUP BY t.type, t.currency, COALESCE(c.name, t.category_name_snapshot)
                ORDER BY t.type, t.currency, total DESC NULLS LAST
                """,
                *args,
            )
            by_member_rows = await self.conn.fetch(
                """
                SELECT
                  t.type,
                  t.currency,
                  COALESCE(u.first_name, u.username, t.created_by_user_id::text, t.tg_user_id::text) AS author_name,
                  sum(t.amount) AS total
                FROM transactions t
                LEFT JOIN users u ON u.tg_user_id = COALESCE(t.created_by_user_id, t.tg_user_id)
                WHERE t.family_id=$1
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.flow_kind='normal'
                  AND t.type IN ('expense','income')
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                GROUP BY t.type, t.currency, author_name
                ORDER BY t.type, author_name, t.currency
                """,
                int(scope.family_id),
                start_date,
                end_date,
            )
            operation_rows = await self.conn.fetch(
                f"""
                SELECT
                  t.date,
                  t.type,
                  t.amount,
                  t.currency,
                  COALESCE(c.name, t.category_name_snapshot) AS category,
                  COALESCE(u.first_name, u.username, t.created_by_user_id::text, t.tg_user_id::text) AS author_name
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN users u ON u.tg_user_id = COALESCE(t.created_by_user_id, t.tg_user_id)
                WHERE t.family_id=$1
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.flow_kind='normal'
                  AND t.type IN ('expense','income')
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                  {member_clause}
                ORDER BY t.date DESC, t.id DESC
                LIMIT 20
                """,
                *args,
            )
        else:
            totals_rows = await self.conn.fetch(
                """
                SELECT type, currency, sum(amount) AS total
                FROM transactions
                WHERE tg_user_id=$1
                  AND family_id IS NULL
                  AND date >= $2
                  AND date < $3
                  AND flow_kind='normal'
                  AND type IN ('expense','income')
                  AND deleted_at IS NULL AND COALESCE(is_deleted, false)=false
                GROUP BY type, currency
                ORDER BY type, currency
                """,
                tg_user_id,
                start_date,
                end_date,
            )
            by_cat_rows = await self.conn.fetch(
                """
                SELECT t.type, t.currency, COALESCE(c.name, t.category_name_snapshot) AS category, sum(t.amount) AS total
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.tg_user_id=$1
                  AND t.family_id IS NULL
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.flow_kind='normal'
                  AND t.type IN ('expense','income')
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                GROUP BY t.type, t.currency, COALESCE(c.name, t.category_name_snapshot)
                ORDER BY t.type, t.currency, total DESC NULLS LAST
                """,
                tg_user_id,
                start_date,
                end_date,
            )

        if not totals_rows:
            return None

        totals_by_type: dict[str, dict[str, Decimal]] = {"expense": {}, "income": {}}
        for row in totals_rows:
            tx_type = str(row["type"])
            currency = str(row["currency"] or "").strip() or "UAH"
            totals_by_type.setdefault(tx_type, {})
            totals_by_type[tx_type][currency] = Decimal(str(row["total"] or 0))

        by_category: dict[str, dict[str, list[tuple[str, Decimal]]]] = {"expense": {}, "income": {}}
        for row in by_cat_rows:
            tx_type = str(row["type"])
            currency = str(row["currency"] or "").strip() or "UAH"
            category = _row_category_name({"type": tx_type, "category": row["category"]})
            total = Decimal(str(row["total"] or 0))
            by_category.setdefault(tx_type, {}).setdefault(currency, []).append((category, total))

        balance = sum((sum(values.values(), Decimal("0")) for values in [totals_by_type.get("income") or {}]), Decimal("0")) - sum(
            (sum(values.values(), Decimal("0")) for values in [totals_by_type.get("expense") or {}]),
            Decimal("0"),
        )

        lines: list[str] = [f"<b>📊 Звіт за {escape_html(title)}</b>"]
        if scope.is_family:
            lines += ["", f"<b>Фільтр:</b> {escape_html(member_filter_label or 'Всі учасники')}"]
        lines += [
            "",
            f"<b>Витрати:</b> {_format_money_map(totals_by_type.get('expense') or {})}",
            f"<b>Доходи:</b> {_format_money_map(totals_by_type.get('income') or {})}",
            f"<b>Баланс періоду:</b> {format_money(balance, 'UAH')}",
        ]

        expenses = by_category.get("expense") or {}
        incomes = by_category.get("income") or {}

        if expenses:
            lines += ["", "<b>Витрати за категоріями:</b>"]
            for currency, items in expenses.items():
                for category, total in items:
                    lines.append(f"• {escape_html(category)} — <b>{format_money(total, currency)}</b>")

        if incomes:
            lines += ["", "<b>Доходи за категоріями:</b>"]
            for currency, items in incomes.items():
                for category, total in items:
                    lines.append(f"• {escape_html(category)} — <b>{format_money(total, currency)}</b>")

        if scope.is_family and by_member_rows:
            lines += ["", "<b>За учасниками:</b>"]
            for row in by_member_rows:
                lines.append(
                    f"• {escape_html(_actor_name(row))} — {escape_html(str(row['type']))}: "
                    f"<b>{format_money(Decimal(str(row['total'] or 0)), str(row['currency'] or 'UAH'))}</b>"
                )

        if scope.is_family and operation_rows:
            lines += ["", "<b>Останні операції:</b>"]
            for row in operation_rows:
                sign = "+" if str(row["type"]) == "income" else "-"
                lines.append(
                    f"• {row['date'].isoformat()} {sign}{format_money(Decimal(str(row['amount'] or 0)), str(row['currency'] or 'UAH'))} — "
                    f"{escape_html(str(row['category'] or '—'))} (Додав: {escape_html(_actor_name(row))})"
                )

        return "\n".join(lines)

    async def fetch_export_rows(self, tg_user_id: int, start_date: date, end_date: date) -> list[asyncpg.Record]:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        if scope.is_family:
            return await self.conn.fetch(
                """
                SELECT
                  t.*,
                  COALESCE(c.name, t.category_name_snapshot) AS category_display_name,
                  COALESCE(u.first_name, u.username, t.created_by_user_id::text, t.tg_user_id::text) AS author_name
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN users u ON u.tg_user_id = COALESCE(t.created_by_user_id, t.tg_user_id)
                WHERE t.family_id=$1
                  AND t.date >= $2
                  AND t.date < $3
                  AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
                ORDER BY t.date ASC, t.id ASC
                """,
                int(scope.family_id),
                start_date,
                end_date,
            )
        return await self.conn.fetch(
            """
            SELECT
              t.*,
              COALESCE(c.name, t.category_name_snapshot) AS category_display_name,
              COALESCE(u.first_name, u.username, t.created_by_user_id::text, t.tg_user_id::text) AS author_name
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            LEFT JOIN users u ON u.tg_user_id = COALESCE(t.created_by_user_id, t.tg_user_id)
            WHERE t.tg_user_id=$1
              AND t.family_id IS NULL
              AND t.date >= $2
              AND t.date < $3
              AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
            ORDER BY t.date ASC, t.id ASC
            """,
            tg_user_id,
            start_date,
            end_date,
        )

    async def build_export_csv(self, tg_user_id: int, start_date: date, end_date: date) -> tuple[bytes | None, int]:
        rows = await self.fetch_export_rows(tg_user_id, start_date, end_date)
        if not rows:
            return None, 0

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(map_export_row(row))
        return buffer.getvalue().encode("utf-8-sig"), len(rows)

    async def fetch_personal_export_rows(self, tg_user_id: int, start_date: date, end_date: date) -> list[asyncpg.Record]:
        return await self.conn.fetch(
            """
            SELECT
              t.*,
              COALESCE(c.name, t.category_name_snapshot) AS category_display_name,
              COALESCE(u.first_name, u.username, t.created_by_user_id::text, t.tg_user_id::text) AS author_name
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            LEFT JOIN users u ON u.tg_user_id = COALESCE(t.created_by_user_id, t.tg_user_id)
            WHERE COALESCE(t.created_by_user_id, t.tg_user_id)=$1
              AND t.date >= $2
              AND t.date < $3
              AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
            ORDER BY t.date ASC, t.id ASC
            """,
            tg_user_id,
            start_date,
            end_date,
        )

    async def fetch_personal_debt_rows(self, tg_user_id: int) -> list[asyncpg.Record]:
        return await self.conn.fetch(
            """
            SELECT t.*
            FROM transactions t
            WHERE COALESCE(t.created_by_user_id, t.tg_user_id)=$1
              AND t.flow_kind='debt'
              AND t.deleted_at IS NULL AND COALESCE(t.is_deleted, false)=false
            ORDER BY t.date ASC, t.id ASC
            """,
            tg_user_id,
        )

    async def fetch_personal_export_accounts(self, tg_user_id: int) -> list[asyncpg.Record]:
        # Authored ledger history survives revocation; mutable snapshots do not.
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        predicate = "family_id=$1" if scope.is_family else "tg_user_id=$1 AND family_id IS NULL"
        return await self.conn.fetch(
            f"""
            SELECT id, tg_user_id, family_id, created_by_user_id, label, currency, account_type,
                   starting_balance, balance, is_active, created_at, updated_at,
                   goal_name, goal_amount, goal_date
            FROM accounts
            WHERE {predicate}
            ORDER BY created_at ASC, id ASC
            """,
            int(scope.family_id) if scope.is_family else tg_user_id,
        )

    async def fetch_personal_export_debts(self, tg_user_id: int) -> list[asyncpg.Record]:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        if scope.is_family:
            return await self.conn.fetch(
                """
                SELECT id, tg_user_id, family_id, counterparty_name, direction,
                       initial_amount, paid_amount, remaining_amount, currency,
                       account_id, status, due_date, comment, borrower_user_id,
                       borrower_confirmed_at, reminder_enabled, next_reminder_at,
                       created_at
                FROM debts
                WHERE family_id=$1
                ORDER BY created_at ASC, id ASC
                """,
                int(scope.family_id),
            )
        return await self.conn.fetch(
            """
            SELECT id, tg_user_id, family_id, counterparty_name, direction,
                   initial_amount, paid_amount, remaining_amount, currency,
                   account_id, status, due_date, comment, borrower_user_id,
                   borrower_confirmed_at, reminder_enabled, next_reminder_at,
                   created_at
            FROM debts
            WHERE tg_user_id=$1 AND family_id IS NULL
            ORDER BY created_at ASC, id ASC
            """,
            tg_user_id,
        )

    async def fetch_personal_pending_saving_tasks(self, tg_user_id: int) -> list[asyncpg.Record]:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        account_scope = (
            "{alias}.family_id=$2" if scope.is_family
            else "{alias}.tg_user_id=$1 AND {alias}.family_id IS NULL"
        )
        args = [tg_user_id, int(scope.family_id)] if scope.is_family else [tg_user_id]
        return await self.conn.fetch(
            f"""
            SELECT
              pst.*,
              src.label AS source_label,
              dst.label AS target_label,
              dst.goal_name,
              dst.goal_amount,
              dst.goal_date
            FROM pending_saving_tasks pst
            LEFT JOIN accounts src ON src.id = pst.source_account_id
              AND {account_scope.format(alias="src")}
            LEFT JOIN accounts dst ON dst.id = pst.target_account_id
              AND {account_scope.format(alias="dst")}
            WHERE pst.tg_user_id=$1
              AND pst.status='pending'
            ORDER BY pst.created_at ASC, pst.id ASC
            """,
            *args,
        )

    async def fetch_export_user_context(self, tg_user_id: int) -> asyncpg.Record | None:
        return await self.conn.fetchrow(
            """
            SELECT tg_user_id, lang, base_currency, start_date
            FROM users
            WHERE tg_user_id=$1
            """,
            tg_user_id,
        )

    async def build_export_xlsx(
        self,
        tg_user_id: int,
        start_date: date,
        end_date: date,
        title: str,
    ) -> ExportWorkbookResult | None:
        rows = await self.fetch_personal_export_rows(tg_user_id, start_date, end_date)
        if not rows:
            return None

        user_row = await self.fetch_export_user_context(tg_user_id)
        accounts = await self.fetch_personal_export_accounts(tg_user_id)
        debt_rows = await self.fetch_personal_debt_rows(tg_user_id)
        debts = await self.fetch_personal_export_debts(tg_user_id)
        pending_tasks = await self.fetch_personal_pending_saving_tasks(tg_user_id)

        base_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
        lang = str((user_row or {}).get("lang") or "").strip().lower()
        sheet_name = _sheet_name_for_lang(lang)
        summary, operation_rows = _build_export_dataset(
            rows=rows,
            debt_rows=debt_rows,
            accounts=accounts,
            debts=debts,
            pending_tasks=pending_tasks,
            base_currency=base_currency,
            start_date=start_date,
            end_date=end_date,
            title=title,
        )
        workbook_bytes = _build_export_two_sheet_workbook(sheet_name, lang, summary, operation_rows)
        return ExportWorkbookResult(
            content=workbook_bytes,
            sheet_name=sheet_name,
            row_count=len(operation_rows),
            summary=summary,
        )


@dataclass(frozen=True)
class ExportOperationRow:
    date_value: date
    month: str
    type_label: str
    subject: str
    event_label: str
    account_label: str
    amount: Decimal | None
    currency: str
    base_amount: Decimal | None
    context: str
    note: str


@dataclass(frozen=True)
class ExportSummary:
    period_label: str
    base_currency: str
    total_income: str
    total_expenses: str
    money_left: str
    expense_percentage: str
    savings_amount: str
    savings_percentage: str
    investments_amount: str
    investments_percentage: str
    debt_payments_amount: str
    debt_payments_percentage: str
    current_available_total: str
    current_savings_total: str
    current_investments_total: str
    current_receivable_total: str
    current_debts_total: str
    debt_balance_total: str
    planned_savings_total: str
    top_expense_category: str
    top_expense_lines: tuple[str, ...]
    debt_snapshot_lines: tuple[str, ...]
    goal_snapshot_lines: tuple[str, ...]
    expense_breakdown_lines: tuple[str, ...]
    income_breakdown_lines: tuple[str, ...]
    savings_account_lines: tuple[str, ...]
    debt_burden_lines: tuple[str, ...]
    expense_chart_points: tuple[tuple[str, Decimal], ...]
    income_chart_points: tuple[tuple[str, Decimal], ...]
    savings_chart_points: tuple[tuple[str, Decimal, Decimal], ...]
    debt_chart_points: tuple[tuple[str, Decimal], ...]
    largest_expense: str
    transaction_count: int
    export_date: str
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExportWorkbookResult:
    content: bytes
    sheet_name: str
    row_count: int
    summary: ExportSummary

    @property
    def summary_message(self) -> str:
        return (
            "Ваш фінансовий звіт готовий.\n\n"
            f"Дохід: {self.summary.total_income}\n"
            f"Витрати: {self.summary.total_expenses}\n"
            f"Залишилось: {self.summary.money_left}\n"
            f"Заощадження: {self.summary.savings_amount}\n"
            f"Інвестиції: {self.summary.investments_amount}\n"
            f"Погашення боргів: {self.summary.debt_payments_amount}"
        )


def _sheet_name_for_lang(lang: str) -> str:
    if lang.startswith("en"):
        return "My Report"
    if lang.startswith("ru"):
        return "Мой отчёт"
    return "Мій звіт"


def _transactions_sheet_name_for_lang(lang: str) -> str:
    if lang.startswith("en"):
        return "Transactions"
    if lang.startswith("ru"):
        return "Транзакции"
    return "Транзакції"


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    transactions_sheet_name = _transactions_sheet_name_for_lang(lang)
    dashboard_sheet_xml = _build_export_dashboard_sheet_xml(dashboard_sheet_name, summary)
    transactions_sheet_xml = _build_export_transactions_sheet_xml(operation_rows)

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        '<sheets>'
        f'<sheet name="{xml_escape(dashboard_sheet_name)}" sheetId="1" r:id="rId1"/>'
        f'<sheet name="{xml_escape(transactions_sheet_name)}" sheetId="2" r:id="rId2"/>'
        '</sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="11">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFE74C3C"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4CAF50"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF5B9BD5"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF7F8C8D"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="16">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="7" borderId="1" xfId="0" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="8" borderId="1" xfId="0" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="9" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="10" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="4" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", dashboard_sheet_xml)
        workbook.writestr("xl/worksheets/sheet2.xml", transactions_sheet_xml)
    return buffer.getvalue()


def _build_export_dashboard_sheet_xml(sheet_name: str, summary: ExportSummary) -> str:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    rows_xml.append(_row_xml(2, [_inline_cell("A2", "Підсумок", 2)], height=18))
    merges.append("A2:K2")

    current_row = 3
    for metric_index in range(0, len(metric_rows), 2):
        left_label, left_value = metric_rows[metric_index]
        if metric_index + 1 < len(metric_rows):
            right_label, right_value = metric_rows[metric_index + 1]
        else:
            right_label, right_value = "", ""
        line_count = max(
            1,
            str(left_value).count("\n") + 1,
            str(right_value).count("\n") + 1,
        )
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", left_label, 3),
                    _inline_cell(f"B{current_row}", left_value, 4),
                    _inline_cell(f"F{current_row}", right_label, 3),
                    _inline_cell(f"G{current_row}", right_value, 4),
                ],
                height=max(20, 20 + ((line_count - 1) * 14)),
            )
        )
        merges.extend(
            [
                f"B{current_row}:D{current_row}",
                f"G{current_row}:K{current_row}",
            ]
        )
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "Топ витрат", 6),
                _inline_cell(f"E{current_row}", "Борги", 6),
                _inline_cell(f"I{current_row}", "Цілі та плани", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Інфографіка", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    infographic_rows = [
        ("Витрати", _progress_bar_text(summary.expense_percentage)),
        ("Заощадження", _progress_bar_text(summary.savings_percentage)),
        ("Інвестиції", _progress_bar_text(summary.investments_percentage)),
        ("Борги", _progress_bar_text(summary.debt_payments_percentage)),
    ]
    for label, value in infographic_rows:
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", label, 3),
                    _inline_cell(f"B{current_row}", value, 4),
                ],
                height=20,
            )
        )
        merges.append(f"B{current_row}:K{current_row}")
        current_row += 1

    last_row = current_row - 1
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="16" customWidth="1"/>'
        '<col min="2" max="2" width="18" customWidth="1"/>'
        '<col min="3" max="4" width="14" customWidth="1"/>'
        '<col min="5" max="5" width="4" customWidth="1"/>'
        '<col min="6" max="6" width="18" customWidth="1"/>'
        '<col min="7" max="11" width="16" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{merge_xml}"
        '</worksheet>'
    )


def _build_export_transactions_sheet_xml(operation_rows: list[ExportOperationRow]) -> str:
    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Хто / категорія",
        "Що сталося",
        "Рахунок / маршрут",
        "Сума",
        "Валюта",
        "У базовій валюті",
        "Статус / контекст",
        "Нотатка",
    ]
    rows_xml: list[str] = []
    rows_xml.append(_row_xml(1, [_inline_cell("A1", "Транзакції", 1)], height=24))
    header_row = 2
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.subject or "", 10),
            _inline_cell(f"E{data_row}", operation.event_label or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.context or "", 10),
            _inline_cell(f"K{data_row}", operation.note or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_data_row = data_row - 1
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_data_row}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="2" topLeftCell="A3" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A3" sqref="A3"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="24" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '<col min="11" max="11" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f'<autoFilter ref="A{header_row}:K{last_data_row}"/>'
        '</worksheet>'
    )


def _progress_bar_text(percentage: str, *, width: int = 16) -> str:
    raw_value = str(percentage or "").replace("%", "").strip() or "0"
    try:
        numeric_value = Decimal(raw_value)
    except Exception:
        numeric_value = Decimal("0")
    normalized_value = min(max(numeric_value, Decimal("0")), Decimal("100"))
    filled = int((normalized_value / Decimal("100")) * width)
    return f"[{'#' * filled}{'.' * (width - filled)}] {percentage}"


def _progress_bar_cells(row_number: int, percentage: str, *, filled_style_id: int, width: int = 8) -> list[str]:
    raw_value = str(percentage or "").replace("%", "").strip() or "0"
    try:
        numeric_value = Decimal(raw_value)
    except Exception:
        numeric_value = Decimal("0")
    normalized_value = min(max(numeric_value, Decimal("0")), Decimal("100"))
    filled = int((normalized_value / Decimal("100")) * Decimal(str(width)))
    cells: list[str] = []
    for offset in range(width):
        col_ref = _col_name(2 + offset)
        style_id = filled_style_id if offset < filled else 15
        cells.append(_inline_cell(f"{col_ref}{row_number}", "", style_id))
    return cells


def _build_export_dashboard_sheet_xml(sheet_name: str, summary: ExportSummary) -> str:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    rows_xml.append(_row_xml(2, [_inline_cell("A2", "Підсумок", 2)], height=18))
    merges.append("A2:K2")

    current_row = 3
    for metric_index in range(0, len(metric_rows), 2):
        left_label, left_value = metric_rows[metric_index]
        if metric_index + 1 < len(metric_rows):
            right_label, right_value = metric_rows[metric_index + 1]
        else:
            right_label, right_value = "", ""
        line_count = max(
            1,
            str(left_value).count("\n") + 1,
            str(right_value).count("\n") + 1,
        )
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", left_label, 3),
                    _inline_cell(f"B{current_row}", left_value, 4),
                    _inline_cell(f"F{current_row}", right_label, 3),
                    _inline_cell(f"G{current_row}", right_value, 4),
                ],
                height=max(20, 20 + ((line_count - 1) * 14)),
            )
        )
        merges.extend([f"B{current_row}:D{current_row}", f"G{current_row}:K{current_row}"])
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "Топ витрат", 6),
                _inline_cell(f"E{current_row}", "Борги", 6),
                _inline_cell(f"I{current_row}", "Цілі та плани", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Інфографіка", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    infographic_rows = [
        ("Витрати", summary.expense_percentage, 11),
        ("Заощадження", summary.savings_percentage, 12),
        ("Інвестиції", summary.investments_percentage, 13),
        ("Борги", summary.debt_payments_percentage, 14),
    ]
    for label, value, filled_style_id in infographic_rows:
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", label, 3),
                    *_progress_bar_cells(current_row, value, filled_style_id=filled_style_id),
                    _inline_cell(f"J{current_row}", value, 4),
                ],
                height=20,
            )
        )
        merges.append(f"J{current_row}:K{current_row}")
        current_row += 1

    last_row = current_row - 1
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="16" customWidth="1"/>'
        '<col min="2" max="4" width="12" customWidth="1"/>'
        '<col min="5" max="5" width="4" customWidth="1"/>'
        '<col min="6" max="6" width="16" customWidth="1"/>'
        '<col min="7" max="9" width="10" customWidth="1"/>'
        '<col min="10" max="11" width="10" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{merge_xml}"
        '</worksheet>'
    )


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    transactions_sheet_name = _transactions_sheet_name_for_lang(lang)
    dashboard_sheet_xml = _build_export_dashboard_sheet_xml(dashboard_sheet_name, summary)
    transactions_sheet_xml = _build_export_transactions_sheet_xml(operation_rows)

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        '<sheets>'
        f'<sheet name="{xml_escape(dashboard_sheet_name)}" sheetId="1" r:id="rId1"/>'
        f'<sheet name="{xml_escape(transactions_sheet_name)}" sheetId="2" r:id="rId2"/>'
        '</sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="11">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFE74C3C"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4CAF50"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF5B9BD5"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF7F8C8D"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="16">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="7" borderId="1" xfId="0" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="8" borderId="1" xfId="0" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="9" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="10" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
    <xf numFmtId="0" fontId="0" fillId="4" borderId="1" xfId="0" applyFill="1" applyBorder="1"/>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", dashboard_sheet_xml)
        workbook.writestr("xl/worksheets/sheet2.xml", transactions_sheet_xml)
    return buffer.getvalue()


def _build_export_operations_first_workbook(
    sheet_name: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Хто / категорія",
        "Що сталося",
        "Рахунок / маршрут",
        "Сума",
        "Валюта",
        "У базовій валюті",
        "Статус / контекст",
        "Нотатка",
    ]
    header_row = 2
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.subject or "", 10),
            _inline_cell(f"E{data_row}", operation.event_label or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.context or "", 10),
            _inline_cell(f"K{data_row}", operation.note or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_data_row = data_row - 1
    current_row = data_row + 1
    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Підсумок", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    for label, value in metric_rows:
        line_count = max(1, str(value).count("\n") + 1)
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", label, 3),
                    _inline_cell(f"B{current_row}", value, 4),
                ],
                height=max(18, 18 + ((line_count - 1) * 14)),
            )
        )
        merges.append(f"B{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "Топ витрат", 6),
                _inline_cell(f"E{current_row}", "Борги", 6),
                _inline_cell(f"I{current_row}", "Цілі та плани", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    last_row = current_row - 1
    auto_filter_xml = f'<autoFilter ref="A{header_row}:K{last_data_row}"/>' if operation_rows else ""
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="2" topLeftCell="A3" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A3" sqref="A3"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="24" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '<col min="11" max="11" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{auto_filter_xml}"
        f"{merge_xml}"
        '</worksheet>'
    )

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        f'<sheets><sheet name="{xml_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="11">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()

_PREVIOUS_VISIBLE_SOURCE_EXPORT_TWO_SHEET_WORKBOOK = _build_export_two_sheet_workbook


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    content = _PREVIOUS_VISIBLE_SOURCE_EXPORT_TWO_SHEET_WORKBOOK(
        dashboard_sheet_name,
        lang,
        summary,
        operation_rows,
    )
    if not content:
        return content

    source_buffer = io.BytesIO(content)
    patched_buffer = io.BytesIO()
    with zipfile.ZipFile(source_buffer, "r") as source_zip, zipfile.ZipFile(
        patched_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as patched_zip:
        for info in source_zip.infolist():
            payload = source_zip.read(info.filename)
            if info.filename == "xl/worksheets/sheet1.xml":
                xml_text = payload.decode("utf-8")
                if 'hidden="1"' not in xml_text:
                    xml_text = xml_text.replace('<col min="13" max="26" ', '<col min="13" max="26" hidden="1" ')
                payload = xml_text.encode("utf-8")
            elif info.filename.startswith("xl/charts/chart") and info.filename.endswith(".xml"):
                xml_text = payload.decode("utf-8")
                xml_text = xml_text.replace('<c:plotVisOnly val="1"/>', "")
                payload = xml_text.encode("utf-8")
            patched_zip.writestr(info, payload)
    return patched_buffer.getvalue()


_PREVIOUS_DASHBOARD_FIRST_EXPORT_TWO_SHEET_WORKBOOK = _build_export_two_sheet_workbook


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    try:
        import xlsxwriter
        from xlsxwriter.utility import quote_sheetname, xl_range_abs
    except ImportError:
        return _PREVIOUS_DASHBOARD_FIRST_EXPORT_TWO_SHEET_WORKBOOK(dashboard_sheet_name, lang, summary, operation_rows)

    transactions_sheet_name = _transactions_sheet_name_for_lang(lang)
    workbook_buffer = io.BytesIO()
    workbook = xlsxwriter.Workbook(workbook_buffer, {"in_memory": True})
    base_currency = summary.base_currency

    title_fmt = workbook.add_format({"bold": True, "font_size": 18, "font_color": "white", "bg_color": "#16324F", "align": "center", "valign": "vcenter"})
    subtitle_fmt = workbook.add_format({"font_size": 11, "font_color": "#44546A", "bg_color": "#EEF3F8", "align": "center", "valign": "vcenter"})
    section_fmt = workbook.add_format({"bold": True, "font_color": "#16324F", "bg_color": "#DCEAF7", "align": "left", "valign": "vcenter"})
    panel_title_fmt = workbook.add_format({"bold": True, "font_color": "white", "bg_color": "#2F5D8A", "align": "center", "valign": "vcenter"})
    detail_title_fmt = workbook.add_format({"bold": True, "font_color": "white", "bg_color": "#4A78C2", "align": "center", "valign": "vcenter"})
    detail_body_fmt = workbook.add_format({"bg_color": "#FFFFFF", "border": 1, "border_color": "#D9E2EC", "font_color": "#1F2933", "text_wrap": True, "valign": "top"})
    detail_positive_fmt = workbook.add_format({"bg_color": "#D9F2E3", "border": 1, "border_color": "#B5DCC2", "font_color": "#174F2C", "text_wrap": True, "valign": "top"})
    detail_negative_fmt = workbook.add_format({"bg_color": "#F9D7D3", "border": 1, "border_color": "#EDB7AF", "font_color": "#7A1F14", "text_wrap": True, "valign": "top"})
    placeholder_fmt = workbook.add_format({"bg_color": "#F7FAFC", "border": 1, "border_color": "#D9E2EC", "font_color": "#7B8794", "italic": True, "align": "center", "valign": "vcenter", "text_wrap": True})
    note_fmt = workbook.add_format({"italic": True, "bg_color": "#FFF5CC", "border": 1, "border_color": "#E6D99D", "font_color": "#6B5800", "text_wrap": True, "valign": "top"})
    tx_header_fmt = workbook.add_format({"bold": True, "font_color": "white", "bg_color": "#4472C4", "align": "center", "valign": "vcenter", "border": 1, "border_color": "#D9E2EC"})
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd", "border": 1, "border_color": "#D9E2EC"})
    amount_fmt = workbook.add_format({"num_format": "#,##0.00", "border": 1, "border_color": "#D9E2EC"})
    text_cell_fmt = workbook.add_format({"border": 1, "border_color": "#D9E2EC", "valign": "top"})

    card_title_formats: dict[str, object] = {}
    card_body_formats: dict[tuple[str, str], object] = {}

    def _card_title_fmt(color: str):
        fmt = card_title_formats.get(color)
        if fmt is None:
            fmt = workbook.add_format({"bold": True, "font_color": "white", "bg_color": color, "align": "left", "valign": "vcenter", "left": 1, "right": 1, "top": 1, "border_color": "#D9E2EC"})
            card_title_formats[color] = fmt
        return fmt

    def _card_body_fmt(bg_color: str, font_color: str = "#102A43"):
        key = (bg_color, font_color)
        fmt = card_body_formats.get(key)
        if fmt is None:
            fmt = workbook.add_format({"bg_color": bg_color, "font_color": font_color, "font_size": 12, "bold": True, "text_wrap": True, "valign": "top", "left": 1, "right": 1, "bottom": 1, "border_color": "#D9E2EC"})
            card_body_formats[key] = fmt
        return fmt

    def _detail_fmt_for_text(text: str):
        if text.startswith("Мені винні"):
            return detail_positive_fmt
        if text.startswith("Я винен"):
            return detail_negative_fmt
        return detail_body_fmt

    def _normalized_lines(value: str) -> list[str]:
        lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        approx = [line for line in lines if line.startswith("~ ")]
        rest = [line for line in lines if not line.startswith("~ ")]
        return approx + rest if approx else rest

    def _card_text(value: str, extras: tuple[str, ...] = ()) -> str:
        lines = _normalized_lines(value)
        extra_lines = [f"• {extra}" for extra in extras if str(extra).strip()]
        if lines and extra_lines:
            return "\n".join(lines[:3] + [""] + extra_lines[:2])
        if lines:
            return "\n".join(lines[:5])
        if extra_lines:
            return "\n".join(extra_lines[:3])
        return "—"

    def _ref(sheet_name: str, first_row: int, first_col: int, last_row: int, last_col: int) -> str:
        return f"={quote_sheetname(sheet_name)}!{xl_range_abs(first_row, first_col, last_row, last_col)}"

    dashboard = workbook.add_worksheet(dashboard_sheet_name[:31])
    dashboard.hide_gridlines(2)
    dashboard.set_zoom(95)
    dashboard.set_default_row(20)
    dashboard.set_column("A:L", 14)

    dashboard.merge_range("A1:L1", dashboard_sheet_name, title_fmt)
    dashboard.set_row(0, 28)
    dashboard.merge_range("A2:L2", f"{summary.period_label} • Експортовано: {summary.export_date}", subtitle_fmt)
    dashboard.set_row(1, 22)
    dashboard.merge_range("A4:L4", "Підсумок", section_fmt)
    dashboard.set_row(3, 22)

    def _write_card(*, start_row: int, start_col: int, title: str, body: str, accent_color: str, body_bg: str) -> None:
        dashboard.merge_range(start_row, start_col, start_row, start_col + 3, title, _card_title_fmt(accent_color))
        dashboard.merge_range(start_row + 1, start_col, start_row + 3, start_col + 3, body, _card_body_fmt(body_bg))
        dashboard.set_row(start_row, 20)
        dashboard.set_row(start_row + 1, 30)
        dashboard.set_row(start_row + 2, 30)
        dashboard.set_row(start_row + 3, 30)

    cards = [
        (4, 0, "Дохід", _card_text(summary.total_income), "#2E8B57", "#EAF6EE"),
        (4, 4, "Витрати", _card_text(summary.total_expenses, (f"Топ: {summary.top_expense_category}",)), "#C0392B", "#FDEDEC"),
        (4, 8, "Залишок", _card_text(summary.money_left, (f"Доступно: {summary.current_available_total}", f"Транзакцій: {summary.transaction_count}")), "#1F4E78", "#EAF2F8"),
        (8, 0, "Заощадження", _card_text(summary.savings_amount, (f"Поточні: {summary.current_savings_total}", f"Інвестиції: {summary.current_investments_total}")), "#0F766E", "#E6FFFB"),
        (8, 4, "Борговий баланс", _card_text(summary.debt_balance_total, (f"Мені винні: {summary.current_receivable_total}", f"Я винен: {summary.current_debts_total}")), "#8E2C2C", "#FDEEEE"),
        (8, 8, "План / Ризик", _card_text(summary.planned_savings_total, (f"Витрати: {summary.expense_percentage}", f"Найбільша: {summary.largest_expense}")), "#7A5C00", "#FFF8E1"),
    ]
    for start_row, start_col, title, body, accent_color, body_bg in cards:
        _write_card(start_row=start_row, start_col=start_col, title=title, body=body, accent_color=accent_color, body_bg=body_bg)

    dashboard.merge_range("A13:L13", "Аналітика", section_fmt)
    dashboard.set_row(12, 22)

    hidden_row = 0

    def _write_chart_source(start_col: int, header: str, points: tuple[tuple[str, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write(hidden_row, start_col, header)
        dashboard.write(hidden_row, start_col + 1, base_currency)
        first_row = hidden_row + 1
        for offset, (label, value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _write_savings_source(start_col: int, points: tuple[tuple[str, Decimal, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write_row(hidden_row, start_col, ["Рахунок", "Факт", "Ціль"])
        first_row = hidden_row + 1
        for offset, (label, current_value, goal_value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(current_value))
            dashboard.write_number(first_row + offset, start_col + 2, float(goal_value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _write_panel(*, start_row: int, start_col: int, end_col: int, title: str, chart, has_series: bool, lines: tuple[str, ...]) -> None:
        dashboard.merge_range(start_row, start_col, start_row, end_col, title, panel_title_fmt)
        dashboard.set_row(start_row, 22)
        if has_series:
            dashboard.insert_chart(start_row + 1, start_col, chart, {"x_offset": 8, "y_offset": 6})
        else:
            dashboard.merge_range(start_row + 1, start_col, start_row + 10, end_col, "Недостатньо даних для побудови графіка", placeholder_fmt)
        for row in range(start_row + 1, start_row + 11):
            dashboard.set_row(row, 22)
        line_row = start_row + 11
        for offset, line in enumerate(lines[:5]):
            dashboard.merge_range(line_row + offset, start_col, line_row + offset, end_col, line, _detail_fmt_for_text(line))
            dashboard.set_row(line_row + offset, 22)

    expense_points = summary.expense_chart_points
    income_points = summary.income_chart_points
    savings_points = summary.savings_chart_points
    debt_points = summary.debt_chart_points

    expense_has_series = bool(expense_points)
    if expense_has_series:
        exp_first, exp_last = _write_chart_source(12, "Витрати", expense_points)
        expense_chart = workbook.add_chart({"type": "bar"})
        expense_chart.add_series({"name": "Витрати", "categories": _ref(dashboard_sheet_name, exp_first, 12, exp_last, 12), "values": _ref(dashboard_sheet_name, exp_first, 13, exp_last, 13), "fill": {"color": "#D97706"}, "border": {"none": True}, "data_labels": {"value": True, "position": "outside_end"}})
        expense_chart.show_hidden_data()
        expense_chart.set_legend({"none": True})
        expense_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        expense_chart.set_y_axis({"reverse": True})
        expense_chart.set_plotarea({"border": {"none": True}})
        expense_chart.set_chartarea({"border": {"none": True}})
        expense_chart.set_size({"width": 420, "height": 230})
        expense_chart.show_hidden_data()
    else:
        expense_chart = None

    income_has_series = bool(income_points)
    if income_has_series:
        inc_first, inc_last = _write_chart_source(15, "Доходи", income_points)
        income_chart = workbook.add_chart({"type": "bar"})
        income_chart.add_series({"name": "Доходи", "categories": _ref(dashboard_sheet_name, inc_first, 15, inc_last, 15), "values": _ref(dashboard_sheet_name, inc_first, 16, inc_last, 16), "fill": {"color": "#2E8B57"}, "border": {"none": True}, "data_labels": {"value": True, "position": "outside_end"}})
        income_chart.show_hidden_data()
        income_chart.set_legend({"none": True})
        income_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        income_chart.set_y_axis({"reverse": True})
        income_chart.set_plotarea({"border": {"none": True}})
        income_chart.set_chartarea({"border": {"none": True}})
        income_chart.set_size({"width": 420, "height": 230})
        income_chart.show_hidden_data()
    else:
        income_chart = None

    savings_has_series = bool(savings_points)
    if savings_has_series:
        sav_first, sav_last = _write_savings_source(18, savings_points)
        savings_chart = workbook.add_chart({"type": "bar"})
        savings_chart.add_series({"name": "Ціль", "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18), "values": _ref(dashboard_sheet_name, sav_first, 20, sav_last, 20), "fill": {"color": "#D0D7DE"}, "border": {"none": True}})
        savings_chart.add_series({"name": "Факт", "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18), "values": _ref(dashboard_sheet_name, sav_first, 19, sav_last, 19), "fill": {"color": "#0F766E"}, "border": {"none": True}, "data_labels": {"value": True, "position": "outside_end"}})
        savings_chart.show_hidden_data()
        savings_chart.set_legend({"position": "top"})
        savings_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        savings_chart.set_y_axis({"reverse": True})
        savings_chart.set_plotarea({"border": {"none": True}})
        savings_chart.set_chartarea({"border": {"none": True}})
        savings_chart.set_size({"width": 420, "height": 230})
        savings_chart.show_hidden_data()
    else:
        savings_chart = None

    debt_has_series = bool(debt_points)
    if debt_has_series:
        debt_first, debt_last = _write_chart_source(22, "Борг", debt_points)
        debt_chart = workbook.add_chart({"type": "column"})
        debt_chart.add_series({"name": "Борг", "categories": _ref(dashboard_sheet_name, debt_first, 22, debt_last, 22), "values": _ref(dashboard_sheet_name, debt_first, 23, debt_last, 23), "points": [{"fill": {"color": "#4CAF50"}, "border": {"none": True}} if label == "Мені винні" else {"fill": {"color": "#E74C3C"}, "border": {"none": True}} for label, _ in debt_points], "data_labels": {"value": True, "position": "outside_end"}})
        debt_chart.show_hidden_data()
        debt_chart.set_legend({"none": True})
        debt_chart.set_y_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        debt_chart.set_x_axis({"label_position": "low"})
        debt_chart.set_plotarea({"border": {"none": True}})
        debt_chart.set_chartarea({"border": {"none": True}})
        debt_chart.set_size({"width": 420, "height": 230})
        debt_chart.show_hidden_data()
    else:
        debt_chart = None

    _write_panel(start_row=13, start_col=0, end_col=5, title="Витрати по категоріях", chart=expense_chart, has_series=expense_has_series, lines=summary.expense_breakdown_lines)
    _write_panel(start_row=13, start_col=6, end_col=11, title="Доходи по категоріях", chart=income_chart, has_series=income_has_series, lines=summary.income_breakdown_lines)
    _write_panel(start_row=30, start_col=0, end_col=5, title="Заощадження по рахунках", chart=savings_chart, has_series=savings_has_series, lines=summary.savings_account_lines)
    _write_panel(start_row=30, start_col=6, end_col=11, title="Боргове навантаження", chart=debt_chart, has_series=debt_has_series, lines=summary.debt_burden_lines)

    dashboard.merge_range("A47:L47", "Деталі", section_fmt)
    dashboard.set_row(46, 22)
    dashboard.merge_range("A48:D48", "Топ витрат", detail_title_fmt)
    dashboard.merge_range("E48:H48", "Борги", detail_title_fmt)
    dashboard.merge_range("I48:L48", "Цілі та плани", detail_title_fmt)
    dashboard.merge_range("A49:D54", "\n".join(summary.top_expense_lines), detail_body_fmt)
    dashboard.merge_range("E49:H54", "\n".join(summary.debt_snapshot_lines), detail_body_fmt)
    dashboard.merge_range("I49:L54", "\n".join(summary.goal_snapshot_lines), detail_body_fmt)
    for row in range(47, 55):
        dashboard.set_row(row, 22)

    if summary.notes:
        dashboard.merge_range("A56:L57", "\n".join(summary.notes), note_fmt)
        dashboard.set_row(55, 26)
        dashboard.set_row(56, 26)

    transactions = workbook.add_worksheet(transactions_sheet_name[:31])
    transactions.freeze_panes(2, 0)
    transactions.set_zoom(100)
    transactions.set_column("A:A", 12)
    transactions.set_column("B:B", 10)
    transactions.set_column("C:C", 14)
    transactions.set_column("D:D", 18)
    transactions.set_column("E:E", 24)
    transactions.set_column("F:F", 22)
    transactions.set_column("G:G", 14)
    transactions.set_column("H:H", 10)
    transactions.set_column("I:I", 18)
    transactions.set_column("J:K", 28)
    transactions.merge_range("A1:K1", transactions_sheet_name, title_fmt)
    headers = ["Дата", "Місяць", "Тип", "Хто / категорія", "Що сталося", "Рахунок / маршрут", "Сума", "Валюта", "У базовій валюті", "Статус / контекст", "Нотатка"]

    for col, header in enumerate(headers):
        transactions.write(1, col, header, tx_header_fmt)

    from datetime import datetime as _dt

    for row_index, operation in enumerate(operation_rows, start=2):
        transactions.write_datetime(row_index, 0, _dt.combine(operation.date_value, _dt.min.time()), date_fmt)
        transactions.write(row_index, 1, operation.month, text_cell_fmt)
        transactions.write(row_index, 2, operation.type_label, text_cell_fmt)
        transactions.write(row_index, 3, operation.subject or "", text_cell_fmt)
        transactions.write(row_index, 4, operation.event_label or "", text_cell_fmt)
        transactions.write(row_index, 5, operation.account_label or "", text_cell_fmt)
        if operation.amount is not None:
            transactions.write_number(row_index, 6, float(operation.amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 6, None, text_cell_fmt)
        transactions.write(row_index, 7, operation.currency or "", text_cell_fmt)
        if operation.base_amount is not None:
            transactions.write_number(row_index, 8, float(operation.base_amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 8, None, text_cell_fmt)
        transactions.write(row_index, 9, operation.context or "", text_cell_fmt)
        transactions.write(row_index, 10, operation.note or "", text_cell_fmt)

    if operation_rows:
        transactions.add_table(
            1,
            0,
            len(operation_rows) + 1,
            len(headers) - 1,
            {"style": "Table Style Medium 2", "columns": [{"header": header} for header in headers]},
        )

    workbook.close()
    workbook_bytes = workbook_buffer.getvalue()
    source_buffer = io.BytesIO(workbook_bytes)
    patched_buffer = io.BytesIO()
    with zipfile.ZipFile(source_buffer, "r") as source_zip, zipfile.ZipFile(
        patched_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as patched_zip:
        for info in source_zip.infolist():
            payload = source_zip.read(info.filename)
            if info.filename == "xl/worksheets/sheet1.xml":
                xml_text = payload.decode("utf-8")
                xml_text = xml_text.replace(
                    '<col min="13" max="26" width="12.7109375" customWidth="1"/>',
                    '<col min="13" max="26" width="12.7109375" hidden="1" customWidth="1"/>',
                )
                payload = xml_text.encode("utf-8")
            elif info.filename.startswith("xl/charts/chart") and info.filename.endswith(".xml"):
                xml_text = payload.decode("utf-8")
                xml_text = xml_text.replace('<c:plotVisOnly val="1"/>', "")
                payload = xml_text.encode("utf-8")
            patched_zip.writestr(info, payload)
    return patched_buffer.getvalue()


_LEGACY_BUILD_EXPORT_TWO_SHEET_WORKBOOK = _build_export_two_sheet_workbook


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    try:
        import xlsxwriter
        from xlsxwriter.utility import quote_sheetname, xl_range_abs
    except ImportError:
        return _LEGACY_BUILD_EXPORT_TWO_SHEET_WORKBOOK(dashboard_sheet_name, lang, summary, operation_rows)

    transactions_sheet_name = _transactions_sheet_name_for_lang(lang)
    workbook_buffer = io.BytesIO()
    workbook = xlsxwriter.Workbook(workbook_buffer, {"in_memory": True})

    base_currency = summary.base_currency
    period_label = summary.period_label
    if " (" in period_label:
        _, _, tail = period_label.rpartition("(")
        period_title = tail.rstrip(")").strip()
    else:
        period_title = period_label

    title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 16,
            "font_color": "white",
            "bg_color": "#1F4E78",
            "align": "center",
            "valign": "vcenter",
        }
    )
    section_fmt = workbook.add_format(
        {
            "bold": True,
            "bg_color": "#D9EAF7",
            "border": 0,
            "align": "left",
            "valign": "vcenter",
        }
    )
    label_fmt = workbook.add_format(
        {
            "bold": True,
            "bg_color": "#F2F4F7",
            "border": 1,
            "text_wrap": True,
            "valign": "top",
        }
    )
    value_fmt = workbook.add_format({"border": 1, "text_wrap": True, "valign": "top"})
    note_fmt = workbook.add_format(
        {
            "italic": True,
            "bg_color": "#FFF2CC",
            "border": 1,
            "text_wrap": True,
            "valign": "top",
        }
    )
    block_title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "white",
            "bg_color": "#4472C4",
            "align": "center",
            "valign": "vcenter",
            "border": 1,
        }
    )
    block_line_fmt = workbook.add_format({"border": 1, "text_wrap": True, "valign": "top"})
    debt_positive_fmt = workbook.add_format(
        {"border": 1, "text_wrap": True, "valign": "top", "bg_color": "#4CAF50", "font_color": "black"}
    )
    debt_negative_fmt = workbook.add_format(
        {"border": 1, "text_wrap": True, "valign": "top", "bg_color": "#E74C3C", "font_color": "black"}
    )
    placeholder_fmt = workbook.add_format(
        {
            "border": 1,
            "text_wrap": True,
            "valign": "vcenter",
            "align": "center",
            "font_color": "#7F8C8D",
            "italic": True,
        }
    )
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd", "border": 1})
    amount_fmt = workbook.add_format({"num_format": "#,##0.00", "border": 1})
    text_cell_fmt = workbook.add_format({"border": 1, "valign": "top"})

    def _chart_text_fmt(text: str):
        if text.startswith("Мені винні"):
            return debt_positive_fmt
        if text.startswith("Я винен"):
            return debt_negative_fmt
        return block_line_fmt

    def _ref(sheet_name: str, first_row: int, first_col: int, last_row: int, last_col: int) -> str:
        return f"={quote_sheetname(sheet_name)}!{xl_range_abs(first_row, first_col, last_row, last_col)}"

    dashboard = workbook.add_worksheet(dashboard_sheet_name[:31])
    dashboard.hide_gridlines(2)
    dashboard.set_zoom(110)
    dashboard.set_column("A:A", 20)
    dashboard.set_column("B:D", 14)
    dashboard.set_column("E:E", 4)
    dashboard.set_column("F:F", 20)
    dashboard.set_column("G:K", 14)
    dashboard.set_column("M:Z", 12, None, {"hidden": True})

    dashboard.merge_range("A1:K1", dashboard_sheet_name, title_fmt)
    dashboard.set_row(0, 24)
    dashboard.merge_range("A2:K2", "Підсумок", section_fmt)
    dashboard.set_row(1, 20)

    metric_rows = _summary_metrics(summary)
    current_row = 2
    for index in range(0, len(metric_rows), 2):
        left_label, left_value = metric_rows[index]
        if index + 1 < len(metric_rows):
            right_label, right_value = metric_rows[index + 1]
        else:
            right_label, right_value = "", ""
        line_count = max(1, str(left_value).count("\n") + 1, str(right_value).count("\n") + 1)
        dashboard.write(current_row, 0, left_label, label_fmt)
        dashboard.merge_range(current_row, 1, current_row, 3, left_value, value_fmt)
        dashboard.write(current_row, 5, right_label, label_fmt)
        dashboard.merge_range(current_row, 6, current_row, 10, right_value, value_fmt)
        dashboard.set_row(current_row, max(22, 22 + ((line_count - 1) * 14)))
        current_row += 1

    dashboard.merge_range(current_row, 0, current_row, 3, "Топ витрат", block_title_fmt)
    dashboard.merge_range(current_row, 4, current_row, 7, "Борги", block_title_fmt)
    dashboard.merge_range(current_row, 8, current_row, 10, "Цілі та плани", block_title_fmt)
    dashboard.set_row(current_row, 22)
    current_row += 1

    dashboard.merge_range(current_row, 0, current_row, 3, "\n".join(summary.top_expense_lines), block_line_fmt)
    dashboard.merge_range(current_row, 4, current_row, 7, "\n".join(summary.debt_snapshot_lines), block_line_fmt)
    dashboard.merge_range(current_row, 8, current_row, 10, "\n".join(summary.goal_snapshot_lines), block_line_fmt)
    dashboard.set_row(
        current_row,
        max(
            72,
            22 + 16 * max(
                len(summary.top_expense_lines),
                len(summary.debt_snapshot_lines),
                len(summary.goal_snapshot_lines),
                1,
            ),
        ),
    )
    current_row += 1

    if summary.notes:
        dashboard.merge_range(current_row, 0, current_row, 10, "\n".join(summary.notes), note_fmt)
        dashboard.set_row(current_row, 38)
        current_row += 1

    dashboard.merge_range(current_row, 0, current_row, 10, "Аналітика", section_fmt)
    dashboard.set_row(current_row, 20)
    current_row += 1

    hidden_row = 0

    def _write_chart_source(start_col: int, header: str, points: tuple[tuple[str, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write(hidden_row, start_col, header)
        dashboard.write(hidden_row, start_col + 1, base_currency)
        first_row = hidden_row + 1
        for offset, (label, value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _write_savings_source(start_col: int, points: tuple[tuple[str, Decimal, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write_row(hidden_row, start_col, ["Рахунок", "Факт", "Ціль"])
        first_row = hidden_row + 1
        for offset, (label, current_value, goal_value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(current_value))
            dashboard.write_number(first_row + offset, start_col + 2, float(goal_value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _insert_chart_block(
        *,
        start_row: int,
        chart_col: int,
        line_col_start: int,
        line_col_end: int,
        title: str,
        lines: tuple[str, ...],
        chart,
        has_series: bool,
    ) -> None:
        title_end_col = line_col_end
        dashboard.merge_range(start_row, chart_col, start_row, title_end_col, title, block_title_fmt)
        dashboard.set_row(start_row, 22)
        if has_series:
            dashboard.insert_chart(start_row + 1, chart_col, chart, {"x_offset": 4, "y_offset": 4})
        else:
            dashboard.merge_range(
                start_row + 1,
                chart_col,
                start_row + 8,
                line_col_start - 1,
                "Недостатньо даних для побудови графіка",
                placeholder_fmt,
            )
        for offset, line in enumerate(lines[:8]):
            dashboard.merge_range(start_row + 1 + offset, line_col_start, start_row + 1 + offset, line_col_end, line, _chart_text_fmt(line))
            dashboard.set_row(start_row + 1 + offset, 22)

    expense_points = summary.expense_chart_points
    income_points = summary.income_chart_points
    savings_points = summary.savings_chart_points
    debt_points = summary.debt_chart_points

    expense_has_series = bool(expense_points)
    if expense_has_series:
        exp_first, exp_last = _write_chart_source(12, "Витрати", expense_points)
        expense_chart = workbook.add_chart({"type": "bar"})
        expense_chart.add_series(
            {
                "name": "Витрати",
                "categories": _ref(dashboard_sheet_name, exp_first, 12, exp_last, 12),
                "values": _ref(dashboard_sheet_name, exp_first, 13, exp_last, 13),
                "fill": {"color": "#E67E22"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        expense_chart.set_title({"name": "Витрати по категоріях"})
        expense_chart.set_legend({"none": True})
        expense_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        expense_chart.set_y_axis({"reverse": True})
        expense_chart.set_plotarea({"border": {"none": True}})
        expense_chart.set_chartarea({"border": {"none": True}})
        expense_chart.set_size({"width": 340, "height": 220})
    else:
        expense_chart = None

    income_has_series = bool(income_points)
    if income_has_series:
        inc_first, inc_last = _write_chart_source(15, "Доходи", income_points)
        income_chart = workbook.add_chart({"type": "column"})
        income_chart.add_series(
            {
                "name": "Доходи",
                "categories": _ref(dashboard_sheet_name, inc_first, 15, inc_last, 15),
                "values": _ref(dashboard_sheet_name, inc_first, 16, inc_last, 16),
                "fill": {"color": "#2E8B57"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        income_chart.set_title({"name": "Доходи по категоріях"})
        income_chart.set_legend({"none": True})
        income_chart.set_y_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        income_chart.set_x_axis({"label_position": "low"})
        income_chart.set_plotarea({"border": {"none": True}})
        income_chart.set_chartarea({"border": {"none": True}})
        income_chart.set_size({"width": 340, "height": 220})
    else:
        income_chart = None

    savings_has_series = bool(savings_points)
    if savings_has_series:
        sav_first, sav_last = _write_savings_source(18, savings_points)
        savings_chart = workbook.add_chart({"type": "column"})
        savings_chart.add_series(
            {
                "name": "Факт",
                "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18),
                "values": _ref(dashboard_sheet_name, sav_first, 19, sav_last, 19),
                "fill": {"color": "#5B9BD5"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        savings_chart.add_series(
            {
                "name": "Ціль",
                "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18),
                "values": _ref(dashboard_sheet_name, sav_first, 20, sav_last, 20),
                "fill": {"color": "#BFBFBF"},
                "border": {"none": True},
            }
        )
        savings_chart.set_title({"name": "Заощадження по рахунках"})
        savings_chart.set_y_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        savings_chart.set_x_axis({"label_position": "low"})
        savings_chart.set_legend({"position": "top"})
        savings_chart.set_plotarea({"border": {"none": True}})
        savings_chart.set_chartarea({"border": {"none": True}})
        savings_chart.set_size({"width": 340, "height": 220})
    else:
        savings_chart = None

    debt_has_series = bool(debt_points)
    if debt_has_series:
        debt_first, debt_last = _write_chart_source(22, "Борг", debt_points)
        debt_chart = workbook.add_chart({"type": "column"})
        debt_chart.add_series(
            {
                "name": "Борг",
                "categories": _ref(dashboard_sheet_name, debt_first, 22, debt_last, 22),
                "values": _ref(dashboard_sheet_name, debt_first, 23, debt_last, 23),
                "points": [
                    {"fill": {"color": "#4CAF50"}, "border": {"none": True}} if label == "Мені винні" else {"fill": {"color": "#E74C3C"}, "border": {"none": True}}
                    for label, _ in debt_points
                ],
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        debt_chart.set_title({"name": "Боргове навантаження"})
        debt_chart.set_legend({"none": True})
        debt_chart.set_y_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        debt_chart.set_x_axis({"label_position": "low"})
        debt_chart.set_plotarea({"border": {"none": True}})
        debt_chart.set_chartarea({"border": {"none": True}})
        debt_chart.set_size({"width": 340, "height": 220})
    else:
        debt_chart = None

    top_block_row = current_row
    _insert_chart_block(
        start_row=top_block_row,
        chart_col=0,
        line_col_start=4,
        line_col_end=5,
        title="Витрати по категоріях",
        lines=summary.expense_breakdown_lines,
        chart=expense_chart,
        has_series=expense_has_series,
    )
    _insert_chart_block(
        start_row=top_block_row,
        chart_col=6,
        line_col_start=9,
        line_col_end=10,
        title="Доходи по категоріях",
        lines=summary.income_breakdown_lines,
        chart=income_chart,
        has_series=income_has_series,
    )
    bottom_block_row = top_block_row + 12
    _insert_chart_block(
        start_row=bottom_block_row,
        chart_col=0,
        line_col_start=4,
        line_col_end=5,
        title="Заощадження по рахунках",
        lines=summary.savings_account_lines,
        chart=savings_chart,
        has_series=savings_has_series,
    )
    _insert_chart_block(
        start_row=bottom_block_row,
        chart_col=6,
        line_col_start=9,
        line_col_end=10,
        title="Боргове навантаження",
        lines=summary.debt_burden_lines,
        chart=debt_chart,
        has_series=debt_has_series,
    )

    transactions = workbook.add_worksheet(transactions_sheet_name[:31])
    transactions.freeze_panes(2, 0)
    transactions.set_zoom(100)
    transactions.set_column("A:A", 12)
    transactions.set_column("B:B", 10)
    transactions.set_column("C:C", 14)
    transactions.set_column("D:D", 18)
    transactions.set_column("E:E", 24)
    transactions.set_column("F:F", 22)
    transactions.set_column("G:G", 14)
    transactions.set_column("H:H", 10)
    transactions.set_column("I:I", 18)
    transactions.set_column("J:K", 28)
    transactions.merge_range("A1:K1", transactions_sheet_name, title_fmt)
    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Хто / категорія",
        "Що сталося",
        "Рахунок / маршрут",
        "Сума",
        "Валюта",
        "У базовій валюті",
        "Статус / контекст",
        "Нотатка",
    ]

    for col, header in enumerate(headers):
        transactions.write(1, col, header, block_title_fmt)

    from datetime import datetime as _dt

    for row_index, operation in enumerate(operation_rows, start=2):
        transactions.write_datetime(row_index, 0, _dt.combine(operation.date_value, _dt.min.time()), date_fmt)
        transactions.write(row_index, 1, operation.month, text_cell_fmt)
        transactions.write(row_index, 2, operation.type_label, text_cell_fmt)
        transactions.write(row_index, 3, operation.subject or "", text_cell_fmt)
        transactions.write(row_index, 4, operation.event_label or "", text_cell_fmt)
        transactions.write(row_index, 5, operation.account_label or "", text_cell_fmt)
        if operation.amount is not None:
            transactions.write_number(row_index, 6, float(operation.amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 6, None, text_cell_fmt)
        transactions.write(row_index, 7, operation.currency or "", text_cell_fmt)
        if operation.base_amount is not None:
            transactions.write_number(row_index, 8, float(operation.base_amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 8, None, text_cell_fmt)
        transactions.write(row_index, 9, operation.context or "", text_cell_fmt)
        transactions.write(row_index, 10, operation.note or "", text_cell_fmt)

    if operation_rows:
        transactions.add_table(
            1,
            0,
            len(operation_rows) + 1,
            len(headers) - 1,
            {
                "style": "Table Style Medium 2",
                "columns": [{"header": header} for header in headers],
            },
        )

    workbook.close()
    return workbook_buffer.getvalue()


def _build_export_dashboard_sheet_xml(sheet_name: str, summary: ExportSummary) -> str:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    rows_xml.append(_row_xml(2, [_inline_cell("A2", "Підсумок", 2)], height=18))
    merges.append("A2:K2")

    current_row = 3
    for metric_index in range(0, len(metric_rows), 2):
        left_label, left_value = metric_rows[metric_index]
        if metric_index + 1 < len(metric_rows):
            right_label, right_value = metric_rows[metric_index + 1]
        else:
            right_label, right_value = "", ""
        line_count = max(1, str(left_value).count("\n") + 1, str(right_value).count("\n") + 1)
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", left_label, 3),
                    _inline_cell(f"B{current_row}", left_value, 4),
                    _inline_cell(f"F{current_row}", right_label, 3),
                    _inline_cell(f"G{current_row}", right_value, 4),
                ],
                height=max(20, 20 + ((line_count - 1) * 14)),
            )
        )
        merges.extend([f"B{current_row}:D{current_row}", f"G{current_row}:K{current_row}"])
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "Топ витрат", 6),
                _inline_cell(f"E{current_row}", "Борги", 6),
                _inline_cell(f"I{current_row}", "Цілі та плани", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=64,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=36))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Інфографіка", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    def _insight_style_id(text: str) -> int:
        stripped = (text or "").strip()
        if stripped.startswith("Мені винні"):
            return 12
        if stripped.startswith("Я винен"):
            return 11
        return 4

    def _append_dual_panel_section(
        left_title: str,
        left_lines: tuple[str, ...],
        right_title: str,
        right_lines: tuple[str, ...],
    ) -> None:
        nonlocal current_row
        rows_xml.append(
            _row_xml(
                current_row,
                [
                    _inline_cell(f"A{current_row}", left_title, 6),
                    _inline_cell(f"G{current_row}", right_title, 6),
                ],
                height=20,
            )
        )
        merges.extend([f"A{current_row}:F{current_row}", f"G{current_row}:K{current_row}"])
        current_row += 1

        line_count = max(len(left_lines), len(right_lines), 1)
        for index in range(line_count):
            left_text = left_lines[index] if index < len(left_lines) else ""
            right_text = right_lines[index] if index < len(right_lines) else ""
            rows_xml.append(
                _row_xml(
                    current_row,
                    [
                        _inline_cell(f"A{current_row}", left_text, _insight_style_id(left_text)),
                        _inline_cell(f"G{current_row}", right_text, _insight_style_id(right_text)),
                    ],
                    height=22,
                )
            )
            merges.extend([f"A{current_row}:F{current_row}", f"G{current_row}:K{current_row}"])
            current_row += 1

    _append_dual_panel_section(
        "Витрати по категоріях",
        summary.expense_breakdown_lines,
        "Доходи по категоріях",
        summary.income_breakdown_lines,
    )
    _append_dual_panel_section(
        "Заощадження по рахунках",
        summary.savings_account_lines,
        "Боргове навантаження",
        summary.debt_burden_lines,
    )

    last_row = current_row - 1
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0"/></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="20" customWidth="1"/>'
        '<col min="2" max="6" width="12" customWidth="1"/>'
        '<col min="7" max="7" width="20" customWidth="1"/>'
        '<col min="8" max="11" width="12" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{merge_xml}"
        '</worksheet>'
    )


async def _load_report_fx_snapshot(base_currency: str, currencies: set[str]):
    normalized_base = normalize_currency(base_currency) or "UAH"
    normalized_currencies = {
        normalize_currency(currency) or normalized_base
        for currency in currencies
        if (currency or "").strip()
    }
    if all(currency == normalized_base for currency in normalized_currencies):
        return None
    try:
        return await get_latest_rates(normalized_base)
    except Exception:
        return None


def _convert_amount_to_base(
    amount: Decimal | None,
    currency: str,
    base_currency: str,
    fx_snapshot,
) -> Decimal | None:
    if amount is None:
        return None
    normalized_base = normalize_currency(base_currency) or "UAH"
    normalized_currency = normalize_currency(currency) or normalized_base
    if normalized_currency == normalized_base:
        return quantize_money(Decimal(str(amount)))
    if fx_snapshot is None:
        return None
    rate = fx_snapshot.rates.get(normalized_currency)
    if rate is None:
        return None
    return quantize_money(Decimal(str(amount)) * Decimal(str(rate)))


def _base_amount_for_row_with_fallback(row: asyncpg.Record, base_currency: str, fx_snapshot) -> tuple[Decimal | None, bool]:
    exact_base_amount = _base_amount_for_row(row, base_currency)
    if exact_base_amount is not None:
        return exact_base_amount, False
    amount, currency = _display_amount_and_currency(row)
    fallback_base_amount = _convert_amount_to_base(amount, currency, base_currency, fx_snapshot)
    return fallback_base_amount, fallback_base_amount is not None


def _converted_total_from_money_map(amounts: dict[str, Decimal], base_currency: str, fx_snapshot) -> Decimal | None:
    if not amounts:
        return Decimal("0")
    total = Decimal("0")
    for currency, amount in amounts.items():
        converted = _convert_amount_to_base(amount, currency, base_currency, fx_snapshot)
        if converted is None:
            return None
        total += converted
    return quantize_money(total)


def _money_map_currencies(*maps: dict[str, Decimal]) -> set[str]:
    currencies: set[str] = set()
    for money_map in maps:
        currencies.update(str(currency or "") for currency in money_map)
    return currencies


def _build_export_dataset_with_fx(
    *,
    rows: list[asyncpg.Record],
    debt_rows: list[asyncpg.Record],
    accounts: list[asyncpg.Record],
    debts: list[asyncpg.Record],
    pending_tasks: list[asyncpg.Record],
    base_currency: str,
    start_date: date,
    end_date: date,
    title: str,
    fx_snapshot,
) -> tuple[ExportSummary, list[ExportOperationRow]]:
    effective_debts = debts or _legacy_debt_snapshots_from_rows(debt_rows)
    account_map = {int(row["id"]): row for row in accounts if row.get("id") is not None}
    debt_map = {int(row["id"]): row for row in effective_debts if row.get("id") is not None}
    active_accounts = [row for row in accounts if bool(row.get("is_active", True))]

    total_income_map = _money_map()
    total_expense_map = _money_map()
    savings_map = _money_map()
    investments_map = _money_map()
    debt_payments_map = _money_map()
    current_available_map = _money_map()
    current_savings_map = _money_map()
    current_investments_map = _money_map()
    current_receivable_map = _money_map()
    current_payable_map = _money_map()

    income_base = Decimal("0")
    expense_base = Decimal("0")
    savings_base = Decimal("0")
    investments_base = Decimal("0")
    debt_payments_base = Decimal("0")

    income_has_unknown = False
    expense_has_unknown = False
    savings_has_unknown = False
    investments_has_unknown = False
    debt_payments_has_unknown = False

    base_gap_count = 0
    percent_partial = False
    fx_fallback_used = False

    income_category_totals_raw: dict[str, defaultdict[str, Decimal]] = defaultdict(_money_map)
    income_category_totals_base: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    expense_category_totals_raw: dict[str, defaultdict[str, Decimal]] = defaultdict(_money_map)
    expense_category_totals_base: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    expense_rows_for_ranking: list[tuple[Decimal, str, str]] = []
    expense_currencies: set[str] = set()
    operation_rows: list[ExportOperationRow] = []

    for account in active_accounts:
        account_type = normalize_account_type(str(account.get("account_type") or "other"))
        balance = Decimal(str(account.get("balance") or 0))
        currency = str(account.get("currency") or "")
        if account_type in {"savings", "deposit"}:
            _add_money(current_savings_map, currency, balance)
        elif account_type == "investment":
            _add_money(current_investments_map, currency, balance)
        elif is_asset_account_type(account_type):
            _add_money(current_available_map, currency, balance)

    for debt in effective_debts:
        if not _is_active_debt_snapshot(debt):
            continue
        remaining_amount = quantize_money(Decimal(str(debt.get("remaining_amount") or 0)))
        currency = str(debt.get("currency") or "")
        direction = str(debt.get("direction") or "").strip().lower()
        if direction == "receivable":
            _add_money(current_receivable_map, currency, remaining_amount)
        elif direction == "payable":
            _add_money(current_payable_map, currency, remaining_amount)

    for row in rows:
        kind = _operation_kind(row, account_map)
        amount, currency = _display_amount_and_currency(row)
        base_amount, used_fallback_fx = _base_amount_for_row_with_fallback(row, base_currency, fx_snapshot)
        fx_fallback_used = fx_fallback_used or used_fallback_fx
        operation = ExportOperationRow(
            date_value=row["date"],
            month=row["date"].strftime("%Y-%m"),
            type_label=_friendly_type_label(kind),
            subject=_display_subject(row, kind, account_map, debt_map),
            event_label=_display_event_label(row, kind),
            account_label=_display_account_label(row, kind, account_map, debt_map),
            amount=amount,
            currency=currency,
            base_amount=base_amount,
            context=_display_context(row, kind, account_map, debt_map),
            note=_display_note(row, kind, debt_map),
        )
        operation_rows.append(operation)

        flow_kind = str(row.get("flow_kind") or "normal")
        tx_type = str(row.get("type") or "")

        if flow_kind == "normal" and tx_type == "income":
            _add_money(total_income_map, currency, amount)
            category_name = operation.subject or SYSTEM_INCOME_CATEGORY_NAME
            if base_amount is not None:
                income_base += base_amount
                income_category_totals_base[category_name] += base_amount
            elif amount is not None:
                income_has_unknown = True
                base_gap_count += 1
                percent_partial = True
            _add_money(income_category_totals_raw[category_name], currency, amount)
        elif flow_kind == "normal" and tx_type == "expense":
            _add_money(total_expense_map, currency, amount)
            category_name = operation.subject or SYSTEM_EXPENSE_CATEGORY_NAME
            if base_amount is not None:
                expense_base += base_amount
                expense_category_totals_base[category_name] += base_amount
                expense_rows_for_ranking.append((base_amount, category_name, _largest_expense_text(row, operation, base_amount, base_currency)))
            elif amount is not None:
                expense_has_unknown = True
                base_gap_count += 1
                percent_partial = True
            expense_currencies.add(currency or base_currency)
            _add_money(expense_category_totals_raw[category_name], currency, amount)
        elif flow_kind == "transfer" and kind == "savings" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(savings_map, currency, amount)
            if base_amount is not None:
                savings_base += base_amount
            elif amount is not None:
                savings_has_unknown = True
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "transfer" and kind == "investment" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(investments_map, currency, amount)
            if base_amount is not None:
                investments_base += base_amount
            elif amount is not None:
                investments_has_unknown = True
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "debt" and _is_debt_payment_out(row):
            _add_money(debt_payments_map, currency, amount)
            if base_amount is not None:
                debt_payments_base += base_amount
            elif amount is not None:
                debt_payments_has_unknown = True
                base_gap_count += 1
                percent_partial = True

    total_income = _clean_money_bucket(total_income_map)
    total_expenses = _clean_money_bucket(total_expense_map)
    savings_total = _clean_money_bucket(savings_map)
    investments_total = _clean_money_bucket(investments_map)
    debt_payments_total = _clean_money_bucket(debt_payments_map)
    current_available_total = _clean_money_bucket(current_available_map)
    current_savings_total = _clean_money_bucket(current_savings_map)
    current_investments_total = _clean_money_bucket(current_investments_map)
    current_receivable_total = _clean_money_bucket(current_receivable_map)
    current_payable_total = _clean_money_bucket(current_payable_map)
    debt_balance_total = _subtract_money_maps(current_receivable_total, current_payable_total)

    money_left = _subtract_money_maps(
        total_income,
        total_expenses,
        savings_total,
        investments_total,
        debt_payments_total,
    )

    top_expense_category = _top_expense_category_text(
        expense_category_totals_raw=expense_category_totals_raw,
        expense_category_totals_base=expense_category_totals_base,
        expense_currencies=expense_currencies,
        base_currency=base_currency,
    )
    largest_expense = _largest_expense_summary(
        expense_rows_for_ranking=expense_rows_for_ranking,
        rows=rows,
        base_currency=base_currency,
    )

    expense_percentage = _format_percent(_percentage(expense_base, income_base))
    savings_percentage = _format_percent(_percentage(savings_base, income_base))
    investments_percentage = _format_percent(_percentage(investments_base, income_base))
    debt_payments_percentage = _format_percent(_percentage(debt_payments_base, income_base))
    money_left_base = income_base - expense_base - savings_base - investments_base - debt_payments_base

    current_available_base = _converted_total_from_money_map(current_available_total, base_currency, fx_snapshot)
    current_savings_base = _converted_total_from_money_map(current_savings_total, base_currency, fx_snapshot)
    current_investments_base = _converted_total_from_money_map(current_investments_total, base_currency, fx_snapshot)
    current_receivable_base = _converted_total_from_money_map(current_receivable_total, base_currency, fx_snapshot)
    current_payable_base = _converted_total_from_money_map(current_payable_total, base_currency, fx_snapshot)
    debt_balance_base = _converted_total_from_money_map(debt_balance_total, base_currency, fx_snapshot)

    notes: list[str] = []
    if base_gap_count > 0:
        notes.append(f"Сума в базовій валюті недоступна для {base_gap_count} операцій.")
    if percent_partial and income_base > 0:
        notes.append(f"Відсотки пораховано лише для сум, які можна зіставити з {base_currency}.")
    if fx_fallback_used:
        notes.append(f"Позначка ~ означає зведення в {base_currency}; для частини сум використано актуальний курс.")

    summary = ExportSummary(
        period_label=f"{start_date.isoformat()} - {(end_date - date.resolution).isoformat()} ({title})",
        base_currency=base_currency,
        total_income=_format_export_money_block(total_income, base_currency=base_currency, converted_total=None if income_has_unknown else income_base),
        total_expenses=_format_export_money_block(total_expenses, base_currency=base_currency, converted_total=None if expense_has_unknown else expense_base),
        money_left=_format_export_money_block(money_left, base_currency=base_currency, converted_total=None if any((income_has_unknown, expense_has_unknown, savings_has_unknown, investments_has_unknown, debt_payments_has_unknown)) else money_left_base),
        expense_percentage=expense_percentage,
        savings_amount=_format_export_money_block(savings_total, base_currency=base_currency, converted_total=None if savings_has_unknown else savings_base),
        savings_percentage=savings_percentage,
        investments_amount=_format_export_money_block(investments_total, base_currency=base_currency, converted_total=None if investments_has_unknown else investments_base),
        investments_percentage=investments_percentage,
        debt_payments_amount=_format_export_money_block(debt_payments_total, base_currency=base_currency, converted_total=None if debt_payments_has_unknown else debt_payments_base),
        debt_payments_percentage=debt_payments_percentage,
        current_available_total=_format_export_money_block(current_available_total, base_currency=base_currency, converted_total=current_available_base),
        current_savings_total=_format_export_money_block(current_savings_total, base_currency=base_currency, converted_total=current_savings_base),
        current_investments_total=_format_export_money_block(current_investments_total, base_currency=base_currency, converted_total=current_investments_base),
        current_receivable_total=_format_export_money_block(current_receivable_total, base_currency=base_currency, converted_total=current_receivable_base),
        current_debts_total=_format_export_money_block(current_payable_total, base_currency=base_currency, converted_total=current_payable_base),
        debt_balance_total=_format_export_money_block(debt_balance_total, base_currency=base_currency, converted_total=debt_balance_base),
        planned_savings_total=_pending_savings_total_text(pending_tasks),
        top_expense_category=top_expense_category,
        top_expense_lines=_top_expense_lines(
            expense_category_totals_raw=expense_category_totals_raw,
            expense_category_totals_base=expense_category_totals_base,
            expense_currencies=expense_currencies,
            base_currency=base_currency,
        ),
        debt_snapshot_lines=_debt_snapshot_lines(effective_debts),
        goal_snapshot_lines=_goal_snapshot_lines(accounts, pending_tasks),
        expense_breakdown_lines=_category_breakdown_lines(
            category_totals_raw=expense_category_totals_raw,
            category_totals_base=expense_category_totals_base,
            total_base=expense_base,
            base_currency=base_currency,
            empty_text="Немає витрат за період",
        ),
        income_breakdown_lines=_category_breakdown_lines(
            category_totals_raw=income_category_totals_raw,
            category_totals_base=income_category_totals_base,
            total_base=income_base,
            base_currency=base_currency,
            empty_text="Немає доходів за період",
        ),
        savings_account_lines=_savings_account_breakdown_lines(accounts),
        debt_burden_lines=_debt_burden_lines(
            current_receivable_total=current_receivable_total,
            current_payable_total=current_payable_total,
            current_receivable_base=current_receivable_base,
            current_payable_base=current_payable_base,
            income_base=income_base,
            base_currency=base_currency,
        ),
        expense_chart_points=_category_chart_points(
            category_totals_base=expense_category_totals_base,
            total_base=expense_base,
        ),
        income_chart_points=_category_chart_points(
            category_totals_base=income_category_totals_base,
            total_base=income_base,
        ),
        savings_chart_points=_savings_chart_points(
            accounts=accounts,
            base_currency=base_currency,
            fx_snapshot=fx_snapshot,
        ),
        debt_chart_points=_debt_chart_points(
            current_receivable_base=current_receivable_base,
            current_payable_base=current_payable_base,
            income_base=income_base,
        ),
        largest_expense=largest_expense,
        transaction_count=len(operation_rows),
        export_date=date.today().isoformat(),
        notes=tuple(notes),
    )
    return summary, operation_rows


def _finalize_export_chart_package(workbook_bytes: bytes) -> bytes:
    source_buffer = io.BytesIO(workbook_bytes)
    patched_buffer = io.BytesIO()
    with zipfile.ZipFile(source_buffer, "r") as source_zip, zipfile.ZipFile(
        patched_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as patched_zip:
        for info in source_zip.infolist():
            payload = source_zip.read(info.filename)
            if info.filename == "xl/worksheets/sheet1.xml":
                xml_text = payload.decode("utf-8")
                xml_text = re.sub(
                    r'<col min="13" max="26" [^>]*/>',
                    lambda match: re.sub(r'\shidden="1"', "", match.group(0), count=0).replace(
                        '<col min="13" max="26" ',
                        '<col min="13" max="26" hidden="1" ',
                        1,
                    ),
                    xml_text,
                    count=1,
                )
                payload = xml_text.encode("utf-8")
            elif info.filename.startswith("xl/charts/chart") and info.filename.endswith(".xml"):
                xml_text = payload.decode("utf-8")
                xml_text = xml_text.replace('<c:plotVisOnly val="1"/>', "")
                payload = xml_text.encode("utf-8")
            patched_zip.writestr(info, payload)
    return patched_buffer.getvalue()


async def _report_service_build_export_xlsx_v2(
    self: ReportService,
    tg_user_id: int,
    start_date: date,
    end_date: date,
    title: str,
) -> ExportWorkbookResult | None:
    rows = await self.fetch_personal_export_rows(tg_user_id, start_date, end_date)
    if not rows:
        return None

    user_row = await self.fetch_export_user_context(tg_user_id)
    accounts = await self.fetch_personal_export_accounts(tg_user_id)
    debt_rows = await self.fetch_personal_debt_rows(tg_user_id)
    debts = await self.fetch_personal_export_debts(tg_user_id)
    pending_tasks = await self.fetch_personal_pending_saving_tasks(tg_user_id)

    base_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
    lang = str((user_row or {}).get("lang") or "").strip().lower()
    sheet_name = _sheet_name_for_lang(lang)
    currencies = {
        str(row.get("currency") or "")
        for row in [*rows, *accounts, *debts, *pending_tasks]
        if row.get("currency") is not None
    }
    fx_snapshot = await _load_report_fx_snapshot(base_currency, currencies)
    summary, operation_rows = _build_export_dataset_with_fx(
        rows=rows,
        debt_rows=debt_rows,
        accounts=accounts,
        debts=debts,
        pending_tasks=pending_tasks,
        base_currency=base_currency,
        start_date=start_date,
        end_date=end_date,
        title=title,
        fx_snapshot=fx_snapshot,
    )
    workbook_bytes = _build_export_two_sheet_workbook(sheet_name, lang, summary, operation_rows)
    workbook_bytes = _finalize_export_chart_package(workbook_bytes)
    return ExportWorkbookResult(
        content=workbook_bytes,
        sheet_name=sheet_name,
        row_count=len(operation_rows),
        summary=summary,
    )


async def _report_service_build_normal_report_text_v2(
    self: ReportService,
    tg_user_id: int,
    start_date: date,
    end_date: date,
    title: str,
    *,
    member_filter_user_id: int | None = None,
    member_filter_label: str | None = None,
) -> str | None:
    scope = await get_current_finance_scope(self.conn, tg_user_id)
    user_row = await self.fetch_export_user_context(tg_user_id)
    base_currency = normalize_currency(str((user_row or {}).get("base_currency") or "UAH")) or "UAH"
    rows = await self.fetch_export_rows(tg_user_id, start_date, end_date)

    filtered_rows: list[asyncpg.Record] = []
    for row in rows:
        if str(row.get("flow_kind") or "") != "normal":
            continue
        if str(row.get("type") or "") not in {"income", "expense"}:
            continue
        if scope.is_family and member_filter_user_id is not None:
            actor_id = int(row.get("created_by_user_id") or row.get("tg_user_id") or 0)
            if actor_id != int(member_filter_user_id):
                continue
        filtered_rows.append(row)

    if not filtered_rows:
        return None

    currencies = {str(row.get("currency") or "") for row in filtered_rows}
    fx_snapshot = await _load_report_fx_snapshot(base_currency, currencies)

    income_map = _money_map()
    expense_map = _money_map()
    income_base = Decimal("0")
    expense_base = Decimal("0")
    income_has_unknown = False
    expense_has_unknown = False
    by_category: dict[str, dict[str, dict[str, Decimal]]] = {"expense": {}, "income": {}}
    by_member: dict[tuple[str, str, str], Decimal] = {}

    for row in filtered_rows:
        tx_type = str(row.get("type") or "")
        amount, currency = _display_amount_and_currency(row)
        base_amount, _ = _base_amount_for_row_with_fallback(row, base_currency, fx_snapshot)
        category = _row_category_name(row)
        _add_money((income_map if tx_type == "income" else expense_map), currency, amount)
        category_totals = by_category[tx_type].setdefault(currency, {})
        category_totals[category] = category_totals.get(category, Decimal("0")) + (amount or Decimal("0"))
        if tx_type == "income":
            if base_amount is not None:
                income_base += base_amount
            else:
                income_has_unknown = True
        else:
            if base_amount is not None:
                expense_base += base_amount
            else:
                expense_has_unknown = True
        if scope.is_family:
            key = (str(row.get("author_name") or "—"), tx_type, currency)
            by_member[key] = quantize_money(by_member.get(key, Decimal("0")) + Decimal(str(amount or 0)))

    income_totals = _clean_money_bucket(income_map)
    expense_totals = _clean_money_bucket(expense_map)
    balance_totals = _subtract_money_maps(income_totals, expense_totals)

    lines: list[str] = [f"<b>📊 Звіт за {escape_html(title)}</b>"]
    if scope.is_family:
        lines += ["", f"<b>Фільтр:</b> {escape_html(member_filter_label or 'Всі учасники')}"]
    lines += [
        "",
        f"<b>Витрати:</b> {_format_export_money_block(expense_totals, base_currency=base_currency, converted_total=None if expense_has_unknown else expense_base)}",
        f"<b>Доходи:</b> {_format_export_money_block(income_totals, base_currency=base_currency, converted_total=None if income_has_unknown else income_base)}",
        f"<b>Баланс періоду:</b> {_format_export_money_block(balance_totals, base_currency=base_currency, converted_total=None if income_has_unknown or expense_has_unknown else quantize_money(income_base - expense_base))}",
    ]

    expenses = by_category.get("expense") or {}
    incomes = by_category.get("income") or {}

    if expenses:
        lines += ["", "<b>Витрати за категоріями:</b>"]
        for currency, items in expenses.items():
            for category, total in items.items():
                lines.append(f"• {escape_html(category)} — <b>{format_money(total, currency)}</b>")

    if incomes:
        lines += ["", "<b>Доходи за категоріями:</b>"]
        for currency, items in incomes.items():
            for category, total in items.items():
                lines.append(f"• {escape_html(category)} — <b>{format_money(total, currency)}</b>")

    if scope.is_family and by_member:
        lines += ["", "<b>За учасниками:</b>"]
        for author_name, tx_type, currency in sorted(by_member):
            lines.append(
                f"• {escape_html(author_name)} — {escape_html(tx_type)}: "
                f"<b>{format_money(by_member[(author_name, tx_type, currency)], currency)}</b>"
            )

    if income_has_unknown or expense_has_unknown:
        lines += ["", f"<i>~ показується лише коли суму можна звести в {escape_html(base_currency)}.</i>"]
    return "\n".join(lines)


ReportService.build_export_xlsx = _report_service_build_export_xlsx_v2
ReportService.build_normal_report_text = _report_service_build_normal_report_text_v2


_PREVIOUS_CHART_EXPORT_TWO_SHEET_WORKBOOK = _build_export_two_sheet_workbook


def _build_export_two_sheet_workbook(
    dashboard_sheet_name: str,
    lang: str,
    summary: ExportSummary,
    operation_rows: list[ExportOperationRow],
) -> bytes:
    try:
        import xlsxwriter
        from xlsxwriter.utility import quote_sheetname, xl_range_abs
    except ImportError:
        return _PREVIOUS_CHART_EXPORT_TWO_SHEET_WORKBOOK(dashboard_sheet_name, lang, summary, operation_rows)

    transactions_sheet_name = _transactions_sheet_name_for_lang(lang)
    workbook_buffer = io.BytesIO()
    workbook = xlsxwriter.Workbook(workbook_buffer, {"in_memory": True})
    base_currency = summary.base_currency

    title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 18,
            "font_color": "white",
            "bg_color": "#16324F",
            "align": "center",
            "valign": "vcenter",
        }
    )
    subtitle_fmt = workbook.add_format(
        {
            "font_size": 11,
            "font_color": "#44546A",
            "bg_color": "#EEF3F8",
            "align": "center",
            "valign": "vcenter",
        }
    )
    section_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "#16324F",
            "bg_color": "#DCEAF7",
            "align": "left",
            "valign": "vcenter",
        }
    )
    panel_title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "white",
            "bg_color": "#2F5D8A",
            "align": "center",
            "valign": "vcenter",
        }
    )
    detail_title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "white",
            "bg_color": "#4A78C2",
            "align": "center",
            "valign": "vcenter",
        }
    )
    detail_body_fmt = workbook.add_format(
        {
            "bg_color": "#FFFFFF",
            "border": 1,
            "border_color": "#D9E2EC",
            "font_color": "#1F2933",
            "text_wrap": True,
            "valign": "top",
        }
    )
    detail_positive_fmt = workbook.add_format(
        {
            "bg_color": "#D9F2E3",
            "border": 1,
            "border_color": "#B5DCC2",
            "font_color": "#174F2C",
            "text_wrap": True,
            "valign": "top",
        }
    )
    detail_negative_fmt = workbook.add_format(
        {
            "bg_color": "#F9D7D3",
            "border": 1,
            "border_color": "#EDB7AF",
            "font_color": "#7A1F14",
            "text_wrap": True,
            "valign": "top",
        }
    )
    placeholder_fmt = workbook.add_format(
        {
            "bg_color": "#F7FAFC",
            "border": 1,
            "border_color": "#D9E2EC",
            "font_color": "#7B8794",
            "italic": True,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
        }
    )
    note_fmt = workbook.add_format(
        {
            "italic": True,
            "bg_color": "#FFF5CC",
            "border": 1,
            "border_color": "#E6D99D",
            "font_color": "#6B5800",
            "text_wrap": True,
            "valign": "top",
        }
    )
    tx_header_fmt = workbook.add_format(
        {
            "bold": True,
            "font_color": "white",
            "bg_color": "#4472C4",
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#D9E2EC",
        }
    )
    date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd", "border": 1, "border_color": "#D9E2EC"})
    amount_fmt = workbook.add_format({"num_format": "#,##0.00", "border": 1, "border_color": "#D9E2EC"})
    text_cell_fmt = workbook.add_format({"border": 1, "border_color": "#D9E2EC", "valign": "top"})

    card_title_formats: dict[str, object] = {}
    card_body_formats: dict[tuple[str, str], object] = {}

    def _card_title_fmt(color: str):
        fmt = card_title_formats.get(color)
        if fmt is None:
            fmt = workbook.add_format(
                {
                    "bold": True,
                    "font_color": "white",
                    "bg_color": color,
                    "align": "left",
                    "valign": "vcenter",
                    "left": 1,
                    "right": 1,
                    "top": 1,
                    "border_color": "#D9E2EC",
                }
            )
            card_title_formats[color] = fmt
        return fmt

    def _card_body_fmt(bg_color: str, font_color: str = "#102A43"):
        key = (bg_color, font_color)
        fmt = card_body_formats.get(key)
        if fmt is None:
            fmt = workbook.add_format(
                {
                    "bg_color": bg_color,
                    "font_color": font_color,
                    "font_size": 12,
                    "bold": True,
                    "text_wrap": True,
                    "valign": "top",
                    "left": 1,
                    "right": 1,
                    "bottom": 1,
                    "border_color": "#D9E2EC",
                }
            )
            card_body_formats[key] = fmt
        return fmt

    def _detail_fmt_for_text(text: str):
        if text.startswith("Мені винні"):
            return detail_positive_fmt
        if text.startswith("Я винен"):
            return detail_negative_fmt
        return detail_body_fmt

    def _normalized_lines(value: str) -> list[str]:
        lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
        approx = [line for line in lines if line.startswith("~ ")]
        rest = [line for line in lines if not line.startswith("~ ")]
        return approx + rest if approx else rest

    def _card_text(value: str, extras: tuple[str, ...] = ()) -> str:
        lines = _normalized_lines(value)
        lines.extend(extra for extra in extras if str(extra).strip())
        return "\n".join(lines[:5]) if lines else "—"

    def _ref(sheet_name: str, first_row: int, first_col: int, last_row: int, last_col: int) -> str:
        return f"={quote_sheetname(sheet_name)}!{xl_range_abs(first_row, first_col, last_row, last_col)}"

    dashboard = workbook.add_worksheet(dashboard_sheet_name[:31])
    dashboard.hide_gridlines(2)
    dashboard.set_zoom(95)
    dashboard.set_default_row(20)
    dashboard.set_column("A:L", 14)
    dashboard.set_column("M:Z", 12, None, {"hidden": True})

    dashboard.merge_range("A1:L1", dashboard_sheet_name, title_fmt)
    dashboard.set_row(0, 28)
    dashboard.merge_range("A2:L2", f"{summary.period_label} • Експортовано: {summary.export_date}", subtitle_fmt)
    dashboard.set_row(1, 22)
    dashboard.merge_range("A4:L4", "Підсумок", section_fmt)

    def _write_card(
        *,
        start_row: int,
        start_col: int,
        title: str,
        body: str,
        accent_color: str,
        body_bg: str,
    ) -> None:
        dashboard.merge_range(start_row, start_col, start_row, start_col + 3, title, _card_title_fmt(accent_color))
        dashboard.merge_range(start_row + 1, start_col, start_row + 3, start_col + 3, body, _card_body_fmt(body_bg))

    cards = [
        (4, 0, "Дохід", _card_text(summary.total_income), "#2E8B57", "#EAF6EE"),
        (4, 4, "Витрати", _card_text(summary.total_expenses, (f"Топ: {summary.top_expense_category}",)), "#C0392B", "#FDEDEC"),
        (4, 8, "Залишок", _card_text(summary.money_left, (f"Витрати: {summary.expense_percentage}", f"Транзакцій: {summary.transaction_count}")), "#1F4E78", "#EAF2F8"),
        (8, 0, "Заощадження", _card_text(summary.savings_amount, (f"Поточні: {summary.current_savings_total}", f"Інвестиції: {summary.current_investments_total}")), "#0F766E", "#E6FFFB"),
        (8, 4, "Борговий баланс", _card_text(summary.debt_balance_total, (f"Мені винні: {summary.current_receivable_total}", f"Я винен: {summary.current_debts_total}")), "#8E2C2C", "#FDEEEE"),
        (8, 8, "Доступно зараз", _card_text(summary.current_available_total, (f"План: {summary.planned_savings_total}", f"Найбільша витрата: {summary.largest_expense}")), "#7A5C00", "#FFF8E1"),
    ]
    for start_row, start_col, title, body, accent_color, body_bg in cards:
        _write_card(
            start_row=start_row,
            start_col=start_col,
            title=title,
            body=body,
            accent_color=accent_color,
            body_bg=body_bg,
        )

    dashboard.merge_range("A13:L13", "Аналітика", section_fmt)

    hidden_row = 0

    def _write_chart_source(start_col: int, header: str, points: tuple[tuple[str, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write(hidden_row, start_col, header)
        dashboard.write(hidden_row, start_col + 1, base_currency)
        first_row = hidden_row + 1
        for offset, (label, value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _write_savings_source(start_col: int, points: tuple[tuple[str, Decimal, Decimal], ...]) -> tuple[int, int]:
        nonlocal hidden_row
        dashboard.write_row(hidden_row, start_col, ["Рахунок", "Факт", "Ціль"])
        first_row = hidden_row + 1
        for offset, (label, current_value, goal_value) in enumerate(points):
            dashboard.write(first_row + offset, start_col, label)
            dashboard.write_number(first_row + offset, start_col + 1, float(current_value))
            dashboard.write_number(first_row + offset, start_col + 2, float(goal_value))
        last_row = first_row + max(len(points) - 1, 0)
        hidden_row = last_row + 2
        return first_row, last_row

    def _write_panel(
        *,
        start_row: int,
        start_col: int,
        end_col: int,
        title: str,
        chart,
        has_series: bool,
        lines: tuple[str, ...],
    ) -> None:
        dashboard.merge_range(start_row, start_col, start_row, end_col, title, panel_title_fmt)
        if has_series:
            dashboard.insert_chart(start_row + 1, start_col, chart, {"x_offset": 8, "y_offset": 6})
        else:
            dashboard.merge_range(
                start_row + 1,
                start_col,
                start_row + 10,
                end_col,
                "Недостатньо даних для побудови графіка",
                placeholder_fmt,
            )
        line_row = start_row + 11
        for offset, line in enumerate(lines[:5]):
            dashboard.merge_range(
                line_row + offset,
                start_col,
                line_row + offset,
                end_col,
                line,
                _detail_fmt_for_text(line),
            )

    expense_points = summary.expense_chart_points
    income_points = summary.income_chart_points
    savings_points = summary.savings_chart_points
    debt_points = summary.debt_chart_points

    expense_has_series = bool(expense_points)
    if expense_has_series:
        exp_first, exp_last = _write_chart_source(12, "Витрати", expense_points)
        expense_chart = workbook.add_chart({"type": "bar"})
        expense_chart.add_series(
            {
                "name": "Витрати",
                "categories": _ref(dashboard_sheet_name, exp_first, 12, exp_last, 12),
                "values": _ref(dashboard_sheet_name, exp_first, 13, exp_last, 13),
                "fill": {"color": "#D97706"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        expense_chart.set_legend({"none": True})
        expense_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        expense_chart.set_y_axis({"reverse": True})
        expense_chart.set_plotarea({"border": {"none": True}})
        expense_chart.set_chartarea({"border": {"none": True}})
        expense_chart.set_size({"width": 420, "height": 230})
    else:
        expense_chart = None

    income_has_series = bool(income_points)
    if income_has_series:
        inc_first, inc_last = _write_chart_source(15, "Доходи", income_points)
        income_chart = workbook.add_chart({"type": "bar"})
        income_chart.add_series(
            {
                "name": "Доходи",
                "categories": _ref(dashboard_sheet_name, inc_first, 15, inc_last, 15),
                "values": _ref(dashboard_sheet_name, inc_first, 16, inc_last, 16),
                "fill": {"color": "#2E8B57"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        income_chart.set_legend({"none": True})
        income_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        income_chart.set_y_axis({"reverse": True})
        income_chart.set_plotarea({"border": {"none": True}})
        income_chart.set_chartarea({"border": {"none": True}})
        income_chart.set_size({"width": 420, "height": 230})
    else:
        income_chart = None

    savings_has_series = bool(savings_points)
    if savings_has_series:
        sav_first, sav_last = _write_savings_source(18, savings_points)
        savings_chart = workbook.add_chart({"type": "bar"})
        savings_chart.add_series(
            {
                "name": "Ціль",
                "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18),
                "values": _ref(dashboard_sheet_name, sav_first, 20, sav_last, 20),
                "fill": {"color": "#D0D7DE"},
                "border": {"none": True},
            }
        )
        savings_chart.add_series(
            {
                "name": "Факт",
                "categories": _ref(dashboard_sheet_name, sav_first, 18, sav_last, 18),
                "values": _ref(dashboard_sheet_name, sav_first, 19, sav_last, 19),
                "fill": {"color": "#0F766E"},
                "border": {"none": True},
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        savings_chart.set_legend({"position": "top"})
        savings_chart.set_x_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        savings_chart.set_y_axis({"reverse": True})
        savings_chart.set_plotarea({"border": {"none": True}})
        savings_chart.set_chartarea({"border": {"none": True}})
        savings_chart.set_size({"width": 420, "height": 230})
    else:
        savings_chart = None

    debt_has_series = bool(debt_points)
    if debt_has_series:
        debt_first, debt_last = _write_chart_source(22, "Борг", debt_points)
        debt_chart = workbook.add_chart({"type": "column"})
        debt_chart.add_series(
            {
                "name": "Борг",
                "categories": _ref(dashboard_sheet_name, debt_first, 22, debt_last, 22),
                "values": _ref(dashboard_sheet_name, debt_first, 23, debt_last, 23),
                "points": [
                    {"fill": {"color": "#4CAF50"}, "border": {"none": True}} if label == "Мені винні" else {"fill": {"color": "#E74C3C"}, "border": {"none": True}}
                    for label, _ in debt_points
                ],
                "data_labels": {"value": True, "position": "outside_end"},
            }
        )
        debt_chart.set_legend({"none": True})
        debt_chart.set_y_axis({"name": base_currency, "major_gridlines": {"visible": False}})
        debt_chart.set_x_axis({"label_position": "low"})
        debt_chart.set_plotarea({"border": {"none": True}})
        debt_chart.set_chartarea({"border": {"none": True}})
        debt_chart.set_size({"width": 420, "height": 230})
    else:
        debt_chart = None

    _write_panel(
        start_row=13,
        start_col=0,
        end_col=5,
        title="Витрати по категоріях",
        chart=expense_chart,
        has_series=expense_has_series,
        lines=summary.expense_breakdown_lines,
    )
    _write_panel(
        start_row=13,
        start_col=6,
        end_col=11,
        title="Доходи по категоріях",
        chart=income_chart,
        has_series=income_has_series,
        lines=summary.income_breakdown_lines,
    )
    _write_panel(
        start_row=30,
        start_col=0,
        end_col=5,
        title="Заощадження по рахунках",
        chart=savings_chart,
        has_series=savings_has_series,
        lines=summary.savings_account_lines,
    )
    _write_panel(
        start_row=30,
        start_col=6,
        end_col=11,
        title="Боргове навантаження",
        chart=debt_chart,
        has_series=debt_has_series,
        lines=summary.debt_burden_lines,
    )

    dashboard.merge_range("A47:L47", "Деталі", section_fmt)
    dashboard.merge_range("A48:D48", "Топ витрат", detail_title_fmt)
    dashboard.merge_range("E48:H48", "Борги", detail_title_fmt)
    dashboard.merge_range("I48:L48", "Цілі та плани", detail_title_fmt)
    dashboard.merge_range("A49:D54", "\n".join(summary.top_expense_lines), detail_body_fmt)
    dashboard.merge_range("E49:H54", "\n".join(summary.debt_snapshot_lines), detail_body_fmt)
    dashboard.merge_range("I49:L54", "\n".join(summary.goal_snapshot_lines), detail_body_fmt)

    if summary.notes:
        dashboard.merge_range("A56:L57", "\n".join(summary.notes), note_fmt)

    transactions = workbook.add_worksheet(transactions_sheet_name[:31])
    transactions.freeze_panes(2, 0)
    transactions.set_zoom(100)
    transactions.set_column("A:A", 12)
    transactions.set_column("B:B", 10)
    transactions.set_column("C:C", 14)
    transactions.set_column("D:D", 18)
    transactions.set_column("E:E", 24)
    transactions.set_column("F:F", 22)
    transactions.set_column("G:G", 14)
    transactions.set_column("H:H", 10)
    transactions.set_column("I:I", 18)
    transactions.set_column("J:K", 28)
    transactions.merge_range("A1:K1", transactions_sheet_name, title_fmt)
    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Хто / категорія",
        "Що сталося",
        "Рахунок / маршрут",
        "Сума",
        "Валюта",
        "У базовій валюті",
        "Статус / контекст",
        "Нотатка",
    ]

    for col, header in enumerate(headers):
        transactions.write(1, col, header, tx_header_fmt)

    from datetime import datetime as _dt

    for row_index, operation in enumerate(operation_rows, start=2):
        transactions.write_datetime(row_index, 0, _dt.combine(operation.date_value, _dt.min.time()), date_fmt)
        transactions.write(row_index, 1, operation.month, text_cell_fmt)
        transactions.write(row_index, 2, operation.type_label, text_cell_fmt)
        transactions.write(row_index, 3, operation.subject or "", text_cell_fmt)
        transactions.write(row_index, 4, operation.event_label or "", text_cell_fmt)
        transactions.write(row_index, 5, operation.account_label or "", text_cell_fmt)
        if operation.amount is not None:
            transactions.write_number(row_index, 6, float(operation.amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 6, None, text_cell_fmt)
        transactions.write(row_index, 7, operation.currency or "", text_cell_fmt)
        if operation.base_amount is not None:
            transactions.write_number(row_index, 8, float(operation.base_amount), amount_fmt)
        else:
            transactions.write_blank(row_index, 8, None, text_cell_fmt)
        transactions.write(row_index, 9, operation.context or "", text_cell_fmt)
        transactions.write(row_index, 10, operation.note or "", text_cell_fmt)

    if operation_rows:
        transactions.add_table(
            1,
            0,
            len(operation_rows) + 1,
            len(headers) - 1,
            {
                "style": "Table Style Medium 2",
                "columns": [{"header": header} for header in headers],
            },
        )

    workbook.close()
    return workbook_buffer.getvalue()


def _money_map() -> defaultdict[str, Decimal]:
    return defaultdict(lambda: Decimal("0"))


def _add_money(bucket: defaultdict[str, Decimal], currency: str | None, amount: Decimal | int | float | None) -> None:
    if amount is None:
        return
    normalized_currency = normalize_currency(currency or "") or "UAH"
    bucket[normalized_currency] += quantize_money(Decimal(str(amount)))


def _clean_money_bucket(bucket: defaultdict[str, Decimal] | dict[str, Decimal]) -> dict[str, Decimal]:
    cleaned: dict[str, Decimal] = {}
    for currency, amount in bucket.items():
        normalized = quantize_money(Decimal(str(amount)))
        if normalized == 0:
            continue
        cleaned[currency] = normalized
    return cleaned


def _subtract_money_maps(*maps: dict[str, Decimal]) -> dict[str, Decimal]:
    result = _money_map()
    for index, mapping in enumerate(maps):
        sign = Decimal("1") if index == 0 else Decimal("-1")
        for currency, amount in mapping.items():
            result[currency] += quantize_money(Decimal(str(amount))) * sign
    return _clean_money_bucket(result)


def _format_percent(value: Decimal | None) -> str:
    if value is None:
        return "—"
    quantized = value.quantize(Decimal("0.1"))
    return f"{quantized}%"


def _pair_metrics(summary: ExportSummary) -> list[tuple[str, str, str, str]]:
    return [
        ("Період", summary.period_label, "Дохід", summary.total_income),
        ("Витрати", summary.total_expenses, "Залишок", summary.money_left),
        ("Витрати %", summary.expense_percentage, "Заощадження", summary.savings_amount),
        ("Заощадження %", summary.savings_percentage, "Інвестиції", summary.investments_amount),
        ("Інвестиції %", summary.investments_percentage, "Борги", summary.debt_payments_amount),
        ("Борги %", summary.debt_payments_percentage, "Поточні заощадження", summary.current_savings_total),
        ("Поточні інвестиції", summary.current_investments_total, "Поточні борги", summary.current_debts_total),
        ("Топ витрат", summary.top_expense_category, "Найбільша витрата", summary.largest_expense),
        ("Транзакцій", str(summary.transaction_count), "Експортовано", summary.export_date),
    ]


def _build_export_dataset(
    *,
    rows: list[asyncpg.Record],
    debt_rows: list[asyncpg.Record],
    accounts: list[asyncpg.Record],
    base_currency: str,
    start_date: date,
    end_date: date,
    title: str,
) -> tuple[ExportSummary, list[ExportOperationRow]]:
    account_map = {int(row["id"]): row for row in accounts if row.get("id") is not None}
    active_accounts = [row for row in accounts if bool(row.get("is_active", True))]

    total_income_map = _money_map()
    total_expense_map = _money_map()
    savings_map = _money_map()
    investments_map = _money_map()
    debt_payments_map = _money_map()
    current_savings_map = _money_map()
    current_investments_map = _money_map()
    current_debts_map = _money_map()

    income_base = Decimal("0")
    expense_base = Decimal("0")
    savings_base = Decimal("0")
    investments_base = Decimal("0")
    debt_payments_base = Decimal("0")
    income_base_known = False
    expense_base_known = False
    savings_base_known = False
    investments_base_known = False
    debt_payments_base_known = False

    base_gap_count = 0
    percent_partial = False

    expense_category_totals_raw: dict[str, defaultdict[str, Decimal]] = defaultdict(_money_map)
    expense_category_totals_base: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    expense_rows_for_ranking: list[tuple[Decimal, str, str]] = []
    expense_currencies: set[str] = set()
    operation_rows: list[ExportOperationRow] = []

    for account in active_accounts:
        account_type = normalize_account_type(str(account.get("account_type") or "other"))
        if account_type in {"savings", "deposit"}:
            _add_money(current_savings_map, str(account.get("currency") or ""), Decimal(str(account.get("balance") or 0)))
        elif account_type == "investment":
            _add_money(current_investments_map, str(account.get("currency") or ""), Decimal(str(account.get("balance") or 0)))

    for row in debt_rows:
        delta = _current_debt_delta(row)
        if delta == 0:
            continue
        _add_money(current_debts_map, str(row.get("currency") or ""), delta)

    for row in rows:
        kind = _operation_kind(row, account_map)
        amount, currency = _display_amount_and_currency(row)
        base_amount = _base_amount_for_row(row, base_currency)
        operation = ExportOperationRow(
            date_value=row["date"],
            month=row["date"].strftime("%Y-%m"),
            type_label=_friendly_type_label(kind),
            category=_display_category(row, kind, account_map),
            description=_display_description(row, kind, account_map),
            account_label=_display_account_label(row, account_map),
            amount=amount,
            currency=currency,
            base_amount=base_amount,
            comment=str(row.get("comment") or ""),
        )
        operation_rows.append(operation)

        flow_kind = str(row.get("flow_kind") or "normal")
        tx_type = str(row.get("type") or "")

        if flow_kind == "normal" and tx_type == "income":
            _add_money(total_income_map, currency, amount)
            if base_amount is not None:
                income_base += base_amount
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "normal" and tx_type == "expense":
            _add_money(total_expense_map, currency, amount)
            if base_amount is not None:
                expense_base += base_amount
                expense_category_totals_base[operation.category or SYSTEM_EXPENSE_CATEGORY_NAME] += base_amount
                expense_rows_for_ranking.append((base_amount, operation.category or SYSTEM_EXPENSE_CATEGORY_NAME, _largest_expense_text(row, operation, base_amount, base_currency)))
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
            expense_currencies.add(currency or base_currency)
            _add_money(expense_category_totals_raw[operation.category or SYSTEM_EXPENSE_CATEGORY_NAME], currency, amount)
        elif flow_kind == "transfer" and kind == "savings" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(savings_map, currency, amount)
            if base_amount is not None:
                savings_base += base_amount
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "transfer" and kind == "investment" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(investments_map, currency, amount)
            if base_amount is not None:
                investments_base += base_amount
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "debt" and _is_debt_payment_out(row):
            _add_money(debt_payments_map, currency, amount)
            if base_amount is not None:
                debt_payments_base += base_amount
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True

    total_income = _clean_money_bucket(total_income_map)
    total_expenses = _clean_money_bucket(total_expense_map)
    savings_total = _clean_money_bucket(savings_map)
    investments_total = _clean_money_bucket(investments_map)
    debt_payments_total = _clean_money_bucket(debt_payments_map)
    current_savings_total = _clean_money_bucket(current_savings_map)
    current_investments_total = _clean_money_bucket(current_investments_map)
    current_debts_total = _clean_money_bucket(current_debts_map)

    money_left = _subtract_money_maps(
        total_income,
        total_expenses,
        savings_total,
        investments_total,
        debt_payments_total,
    )

    top_expense_category = _top_expense_category_text(
        expense_category_totals_raw=expense_category_totals_raw,
        expense_category_totals_base=expense_category_totals_base,
        expense_currencies=expense_currencies,
        base_currency=base_currency,
    )
    largest_expense = _largest_expense_summary(
        expense_rows_for_ranking=expense_rows_for_ranking,
        rows=rows,
        base_currency=base_currency,
    )

    expense_percentage = _format_percent(_percentage(expense_base, income_base))
    savings_percentage = _format_percent(_percentage(savings_base, income_base))
    investments_percentage = _format_percent(_percentage(investments_base, income_base))
    debt_payments_percentage = _format_percent(_percentage(debt_payments_base, income_base))

    notes: list[str] = []
    if base_gap_count > 0:
        notes.append(f"Сума в базовій валюті недоступна для {base_gap_count} операцій.")
    if percent_partial and income_base > 0:
        notes.append(f"Відсотки пораховано лише для сум, які можна зіставити з {base_currency}.")

    summary = ExportSummary(
        period_label=f"{start_date.isoformat()} - {(end_date - date.resolution).isoformat()} ({title})",
        base_currency=base_currency,
        total_income=_format_money_map(total_income),
        total_expenses=_format_money_map(total_expenses),
        money_left=_format_money_map(money_left),
        expense_percentage=expense_percentage,
        savings_amount=_format_money_map(savings_total),
        savings_percentage=savings_percentage,
        investments_amount=_format_money_map(investments_total),
        investments_percentage=investments_percentage,
        debt_payments_amount=_format_money_map(debt_payments_total),
        debt_payments_percentage=debt_payments_percentage,
        current_savings_total=_format_money_map(current_savings_total),
        current_investments_total=_format_money_map(current_investments_total),
        current_debts_total=_format_money_map(current_debts_total),
        top_expense_category=top_expense_category,
        largest_expense=largest_expense,
        transaction_count=len(operation_rows),
        export_date=date.today().isoformat(),
        notes=tuple(notes),
    )
    return summary, operation_rows


def _operation_kind(row: asyncpg.Record, account_map: dict[int, asyncpg.Record]) -> str:
    flow_kind = str(row.get("flow_kind") or "normal")
    tx_type = str(row.get("type") or "").lower()
    if flow_kind == "debt":
        return "debt"
    if flow_kind == "transfer":
        subtype = str(row.get("transfer_subtype") or "")
        if subtype == "savings_transfer":
            target_account_id = row.get("to_account_id")
            target_account = account_map.get(int(target_account_id)) if target_account_id is not None else None
            target_type = normalize_account_type(str((target_account or {}).get("account_type") or "other"))
            if target_type == "investment":
                return "investment"
            if target_type in SAVINGS_ACCOUNT_TYPES:
                return "savings"
        return "transfer"
    if tx_type == "income":
        return "income"
    if tx_type == "expense":
        return "expense"
    return "transfer"


def _friendly_type_label(kind: str) -> str:
    labels = {
        "income": "Дохід",
        "expense": "Витрата",
        "savings": "Заощадження",
        "investment": "Інвестиція",
        "debt": "Борг",
        "transfer": "Переказ",
    }
    return labels.get(kind, "Переказ")


def _display_amount_and_currency(row: asyncpg.Record) -> tuple[Decimal | None, str]:
    amount = row.get("amount")
    currency = normalize_currency(str(row.get("currency") or "")) or "UAH"
    return (quantize_money(Decimal(str(amount))), currency) if amount is not None else (None, currency)


def _base_amount_for_row(row: asyncpg.Record, base_currency: str) -> Decimal | None:
    normalized_base = normalize_currency(base_currency) or "UAH"
    currency = normalize_currency(str(row.get("currency") or ""))
    if currency == normalized_base and row.get("amount") is not None:
        return quantize_money(Decimal(str(row["amount"])))
    to_currency = normalize_currency(str(row.get("to_currency") or ""))
    if to_currency == normalized_base and row.get("to_amount") is not None:
        return quantize_money(Decimal(str(row["to_amount"])))
    original_currency = normalize_currency(str(row.get("original_currency") or ""))
    if original_currency == normalized_base and row.get("original_amount") is not None:
        return quantize_money(Decimal(str(row["original_amount"])))
    return None


def _display_category(row: asyncpg.Record, kind: str, account_map: dict[int, asyncpg.Record]) -> str:
    if kind in {"income", "expense"}:
        return _row_category_name(row)
    if kind in {"savings", "investment"}:
        target_account = account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None
        return str((target_account or {}).get("label") or "").strip()
    if kind == "debt":
        return (str(row.get("counterparty") or "")).strip()
    return ""


def _display_description(row: asyncpg.Record, kind: str, account_map: dict[int, asyncpg.Record]) -> str:
    comment = (str(row.get("comment") or "")).strip()
    if kind in {"income", "expense"}:
        if comment:
            return comment
        return _row_category_name(row)
    if kind in {"savings", "investment", "transfer"}:
        from_label = _account_label(account_map.get(int(row["from_account_id"])) if row.get("from_account_id") is not None else None)
        to_label = _account_label(account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None)
        return f"{from_label} -> {to_label}".strip()
    if kind == "debt":
        return _debt_description(row)
    return comment


def _display_account_label(row: asyncpg.Record, account_map: dict[int, asyncpg.Record]) -> str:
    flow_kind = str(row.get("flow_kind") or "normal")
    if flow_kind == "transfer":
        from_label = _account_label(account_map.get(int(row["from_account_id"])) if row.get("from_account_id") is not None else None)
        to_label = _account_label(account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None)
        return f"{from_label} -> {to_label}".strip()
    if row.get("account_id") is not None:
        return _account_label(account_map.get(int(row["account_id"])))
    return ""


def _account_label(account: asyncpg.Record | None) -> str:
    if not account:
        return ""
    return str(account.get("label") or "").strip()


def _debt_description(row: asyncpg.Record) -> str:
    counterparty = (str(row.get("counterparty") or "")).strip()
    debt_action = str(row.get("debt_action") or "").strip().lower()
    tx_type = str(row.get("type") or "").strip().lower()
    labels = {
        "borrow": "Отримано в борг",
        "borrow_repaid": "Погашення боргу",
        "lend": "Позичено іншому",
        "lend_repaid": "Повернення позики",
        "debt_received": "Отримано в борг",
        "debt_repayment_out": "Погашення боргу",
        "debt_given": "Позичено іншому",
        "debt_repayment_in": "Повернення позики",
    }
    label = labels.get(debt_action) or labels.get(tx_type) or "Борг"
    return f"{label}: {counterparty}".strip(": ")


def _is_debt_payment_out(row: asyncpg.Record) -> bool:
    debt_action = str(row.get("debt_action") or "").strip().lower()
    tx_type = str(row.get("type") or "").strip().lower()
    return debt_action == "borrow_repaid" or tx_type == "debt_repayment_out"


def _current_debt_delta(row: asyncpg.Record) -> Decimal:
    debt_action = str(row.get("debt_action") or "").strip().lower()
    tx_type = str(row.get("type") or "").strip().lower()
    amount = quantize_money(Decimal(str(row.get("amount") or 0)))
    if debt_action == "borrow" or tx_type == "debt_received":
        return amount
    if debt_action == "borrow_repaid" or tx_type == "debt_repayment_out":
        return -amount
    return Decimal("0")


def _percentage(part: Decimal, whole: Decimal) -> Decimal | None:
    if whole <= 0:
        return None
    return (part / whole) * Decimal("100")


def _top_expense_category_text(
    *,
    expense_category_totals_raw: dict[str, defaultdict[str, Decimal]],
    expense_category_totals_base: defaultdict[str, Decimal],
    expense_currencies: set[str],
    base_currency: str,
) -> str:
    if not expense_category_totals_raw:
        return "—"
    if len(expense_currencies) == 1:
        currency = next(iter(expense_currencies))
        best_category = max(
            expense_category_totals_raw.items(),
            key=lambda item: quantize_money(item[1].get(currency, Decimal("0"))),
        )
        return f"{best_category[0]} ({format_money(best_category[1].get(currency, Decimal('0')), currency)})"
    if expense_category_totals_base:
        category, amount = max(expense_category_totals_base.items(), key=lambda item: item[1])
        return f"{category} ({format_money(amount, base_currency)})"
    return "—"


def _largest_expense_text(row: asyncpg.Record, operation: ExportOperationRow, base_amount: Decimal, base_currency: str) -> str:
    return f"{row['date'].isoformat()} - {operation.category or '—'} - {format_money(base_amount, base_currency)}"


def _largest_expense_summary(
    *,
    expense_rows_for_ranking: list[tuple[Decimal, str, str]],
    rows: list[asyncpg.Record],
    base_currency: str,
) -> str:
    if expense_rows_for_ranking:
        _amount, _category, summary = max(expense_rows_for_ranking, key=lambda item: item[0])
        return summary
    same_currency_expenses = [
        row
        for row in rows
        if str(row.get("flow_kind") or "") == "normal" and str(row.get("type") or "") == "expense" and row.get("amount") is not None
    ]
    currencies = {normalize_currency(str(row.get("currency") or "")) or base_currency for row in same_currency_expenses}
    if len(currencies) == 1 and same_currency_expenses:
        currency = next(iter(currencies))
        row = max(same_currency_expenses, key=lambda item: Decimal(str(item.get("amount") or 0)))
        category = _row_category_name(row)
        return f"{row['date'].isoformat()} - {category} - {format_money(Decimal(str(row['amount'] or 0)), currency)}"
    return "—"


def _col_name(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _excel_date(value: date) -> int:
    return (value - date(1899, 12, 30)).days


def _inline_cell(ref: str, value: str, style_id: int) -> str:
    safe_value = _xml_safe_text(value or "")
    escaped = xml_escape(safe_value).replace("\n", "&#10;")
    return f'<c r="{ref}" s="{style_id}" t="inlineStr"><is><t xml:space="preserve">{escaped}</t></is></c>'


def _number_cell(ref: str, value: Decimal | int | float, style_id: int) -> str:
    return f'<c r="{ref}" s="{style_id}"><v>{Decimal(str(value))}</v></c>'


def _xml_safe_text(value: str) -> str:
    return "".join(
        char
        for char in value
        if char in ("\t", "\n", "\r")
        or 0x20 <= ord(char) <= 0xD7FF
        or 0xE000 <= ord(char) <= 0xFFFD
        or 0x10000 <= ord(char) <= 0x10FFFF
    )


def _row_xml(row_number: int, cells: list[str], *, height: int | None = None) -> str:
    attrs = [f'r="{row_number}"']
    if height is not None:
        attrs.append(f'ht="{height}"')
        attrs.append('customHeight="1"')
    return f"<row {' '.join(attrs)}>{''.join(cells)}</row>"


def _build_single_sheet_workbook(sheet_name: str, summary: ExportSummary, operation_rows: list[ExportOperationRow]) -> bytes:
    metric_rows = _pair_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:J1")

    rows_xml.append(_row_xml(2, [_inline_cell("A2", "Підсумок", 2)], height=18))
    merges.append("A2:J2")

    current_row = 3
    for left_label, left_value, right_label, right_value in metric_rows:
        row_cells = [
            _inline_cell(f"A{current_row}", left_label, 3),
            _inline_cell(f"B{current_row}", left_value, 4),
            _inline_cell(f"F{current_row}", right_label, 3),
            _inline_cell(f"G{current_row}", right_value, 4),
        ]
        rows_xml.append(_row_xml(current_row, row_cells, height=18))
        merges.extend(
            [
                f"B{current_row}:D{current_row}",
                f"G{current_row}:J{current_row}",
            ]
        )
        current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:J{current_row}")
        current_row += 1

    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Операції", 2)], height=18))
    merges.append(f"A{current_row}:J{current_row}")
    table_section_row = current_row
    header_row = table_section_row + 1

    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Категорія",
        "Опис",
        "Рахунок / банк",
        "Сума",
        "Валюта",
        "Сума в базовій валюті",
        "Коментар / нотатка",
    ]
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.category or "", 10),
            _inline_cell(f"E{data_row}", operation.description or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.comment or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_row = data_row - 1
    auto_filter_xml = f'<autoFilter ref="A{header_row}:J{last_row}"/>' if operation_rows else ""
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:J{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        f'<pane ySplit="{header_row - 1}" topLeftCell="A{header_row}" activePane="bottomLeft" state="frozen"/>'
        f'<selection pane="bottomLeft" activeCell="A{header_row}" sqref="A{header_row}"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="26" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{auto_filter_xml}"
        f"{merge_xml}"
        '</worksheet>'
    )

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        f'<sheets><sheet name="{xml_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="11">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def _build_single_sheet_workbook(sheet_name: str, summary: ExportSummary, operation_rows: list[ExportOperationRow]) -> bytes:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    headers = [
        "\u0414\u0430\u0442\u0430",
        "\u041c\u0456\u0441\u044f\u0446\u044c",
        "\u0422\u0438\u043f",
        "\u0425\u0442\u043e / \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f",
        "\u0429\u043e \u0441\u0442\u0430\u043b\u043e\u0441\u044f",
        "\u0420\u0430\u0445\u0443\u043d\u043e\u043a / \u043c\u0430\u0440\u0448\u0440\u0443\u0442",
        "\u0421\u0443\u043c\u0430",
        "\u0412\u0430\u043b\u044e\u0442\u0430",
        "\u0423 \u0431\u0430\u0437\u043e\u0432\u0456\u0439 \u0432\u0430\u043b\u044e\u0442\u0456",
        "\u0421\u0442\u0430\u0442\u0443\u0441 / \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442",
        "\u041d\u043e\u0442\u0430\u0442\u043a\u0430",
    ]
    header_row = 2
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.subject or "", 10),
            _inline_cell(f"E{data_row}", operation.event_label or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.context or "", 10),
            _inline_cell(f"K{data_row}", operation.note or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_data_row = data_row - 1
    current_row = data_row + 1
    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\u041f\u0456\u0434\u0441\u0443\u043c\u043e\u043a", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    for label, value in metric_rows:
        line_count = max(1, str(value).count("\n") + 1)
        row_cells = [
            _inline_cell(f"A{current_row}", label, 3),
            _inline_cell(f"B{current_row}", value, 4),
        ]
        rows_xml.append(_row_xml(current_row, row_cells, height=max(18, 18 + ((line_count - 1) * 14))))
        merges.append(f"B{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\u0422\u043e\u043f \u0432\u0438\u0442\u0440\u0430\u0442", 6),
                _inline_cell(f"E{current_row}", "\u0411\u043e\u0440\u0433\u0438", 6),
                _inline_cell(f"I{current_row}", "\u0426\u0456\u043b\u0456 \u0442\u0430 \u043f\u043b\u0430\u043d\u0438", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    last_row = current_row - 1
    auto_filter_xml = f'<autoFilter ref="A{header_row}:K{last_data_row}"/>' if operation_rows else ""
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="2" topLeftCell="A3" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A3" sqref="A3"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="24" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '<col min="11" max="11" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{auto_filter_xml}"
        f"{merge_xml}"
        '</worksheet>'
    )

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        f'<sheets><sheet name="{xml_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="11">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def _build_single_sheet_workbook(sheet_name: str, summary: ExportSummary, operation_rows: list[ExportOperationRow]) -> bytes:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    headers = [
        "\u0414\u0430\u0442\u0430",
        "\u041c\u0456\u0441\u044f\u0446\u044c",
        "\u0422\u0438\u043f",
        "\u0425\u0442\u043e / \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f",
        "\u0429\u043e \u0441\u0442\u0430\u043b\u043e\u0441\u044f",
        "\u0420\u0430\u0445\u0443\u043d\u043e\u043a / \u043c\u0430\u0440\u0448\u0440\u0443\u0442",
        "\u0421\u0443\u043c\u0430",
        "\u0412\u0430\u043b\u044e\u0442\u0430",
        "\u0423 \u0431\u0430\u0437\u043e\u0432\u0456\u0439 \u0432\u0430\u043b\u044e\u0442\u0456",
        "\u0421\u0442\u0430\u0442\u0443\u0441 / \u043a\u043e\u043d\u0442\u0435\u043a\u0441\u0442",
        "\u041d\u043e\u0442\u0430\u0442\u043a\u0430",
    ]
    header_row = 2
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.subject or "", 10),
            _inline_cell(f"E{data_row}", operation.event_label or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.context or "", 10),
            _inline_cell(f"K{data_row}", operation.note or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_data_row = data_row - 1
    current_row = data_row + 1
    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\u041f\u0456\u0434\u0441\u0443\u043c\u043e\u043a", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    current_row += 1

    for label, value in metric_rows:
        line_count = max(1, str(value).count("\n") + 1)
        row_cells = [
            _inline_cell(f"A{current_row}", label, 3),
            _inline_cell(f"B{current_row}", value, 4),
        ]
        rows_xml.append(_row_xml(current_row, row_cells, height=max(18, 18 + ((line_count - 1) * 14))))
        merges.append(f"B{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\u0422\u043e\u043f \u0432\u0438\u0442\u0440\u0430\u0442", 6),
                _inline_cell(f"E{current_row}", "\u0411\u043e\u0440\u0433\u0438", 6),
                _inline_cell(f"I{current_row}", "\u0426\u0456\u043b\u0456 \u0442\u0430 \u043f\u043b\u0430\u043d\u0438", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    last_row = current_row - 1
    auto_filter_xml = f'<autoFilter ref="A{header_row}:K{last_data_row}"/>' if operation_rows else ""
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="2" topLeftCell="A3" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A3" sqref="A3"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="24" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '<col min="11" max="11" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{auto_filter_xml}"
        f"{merge_xml}"
        '</worksheet>'
    )

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        f'<sheets><sheet name="{xml_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="11">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()


def _summary_metrics(summary: ExportSummary) -> list[tuple[str, str]]:
    return [
        ("Період", summary.period_label),
        ("Дохід", summary.total_income),
        ("Витрати", summary.total_expenses),
        ("Залишок", summary.money_left),
        ("Заощадження", summary.savings_amount),
        ("Інвестиції", summary.investments_amount),
        ("Платежі по боргах", summary.debt_payments_amount),
        ("Витрати %", summary.expense_percentage),
        ("Заощадження %", summary.savings_percentage),
        ("Інвестиції %", summary.investments_percentage),
        ("Борги %", summary.debt_payments_percentage),
        ("Доступно зараз", summary.current_available_total),
        ("Поточні заощадження", summary.current_savings_total),
        ("Поточні інвестиції", summary.current_investments_total),
        ("Я винен", summary.current_debts_total),
        ("Мені винні", summary.current_receivable_total),
        ("Чистий борговий баланс", summary.debt_balance_total),
        ("Топ витрат", summary.top_expense_category),
        ("Найбільша витрата", summary.largest_expense),
        ("Заплановано відкласти", summary.planned_savings_total),
        ("Транзакцій", str(summary.transaction_count)),
        ("Експортовано", summary.export_date),
    ]


def _build_export_dataset(
    *,
    rows: list[asyncpg.Record],
    debt_rows: list[asyncpg.Record],
    accounts: list[asyncpg.Record],
    debts: list[asyncpg.Record],
    pending_tasks: list[asyncpg.Record],
    base_currency: str,
    start_date: date,
    end_date: date,
    title: str,
) -> tuple[ExportSummary, list[ExportOperationRow]]:
    account_map = {int(row["id"]): row for row in accounts if row.get("id") is not None}
    debt_map = {int(row["id"]): row for row in debts if row.get("id") is not None}
    active_accounts = [row for row in accounts if bool(row.get("is_active", True))]

    total_income_map = _money_map()
    total_expense_map = _money_map()
    savings_map = _money_map()
    investments_map = _money_map()
    debt_payments_map = _money_map()
    current_available_map = _money_map()
    current_savings_map = _money_map()
    current_investments_map = _money_map()
    current_receivable_map = _money_map()
    current_payable_map = _money_map()

    income_base = Decimal("0")
    expense_base = Decimal("0")
    savings_base = Decimal("0")
    investments_base = Decimal("0")
    debt_payments_base = Decimal("0")
    income_base_known = False
    expense_base_known = False
    savings_base_known = False
    investments_base_known = False
    debt_payments_base_known = False

    base_gap_count = 0
    percent_partial = False

    expense_category_totals_raw: dict[str, defaultdict[str, Decimal]] = defaultdict(_money_map)
    expense_category_totals_base: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    expense_rows_for_ranking: list[tuple[Decimal, str, str]] = []
    expense_currencies: set[str] = set()
    operation_rows: list[ExportOperationRow] = []

    for account in active_accounts:
        account_type = normalize_account_type(str(account.get("account_type") or "other"))
        balance = Decimal(str(account.get("balance") or 0))
        currency = str(account.get("currency") or "")
        if account_type in {"savings", "deposit"}:
            _add_money(current_savings_map, currency, balance)
        elif account_type == "investment":
            _add_money(current_investments_map, currency, balance)
        elif is_asset_account_type(account_type):
            _add_money(current_available_map, currency, balance)

    for debt in debts:
        if not _is_active_debt_snapshot(debt):
            continue
        remaining_amount = quantize_money(Decimal(str(debt.get("remaining_amount") or 0)))
        currency = str(debt.get("currency") or "")
        direction = str(debt.get("direction") or "").strip().lower()
        if direction == "receivable":
            _add_money(current_receivable_map, currency, remaining_amount)
        elif direction == "payable":
            _add_money(current_payable_map, currency, remaining_amount)

    for row in rows:
        kind = _operation_kind(row, account_map)
        amount, currency = _display_amount_and_currency(row)
        base_amount = _base_amount_for_row(row, base_currency)
        operation = ExportOperationRow(
            date_value=row["date"],
            month=row["date"].strftime("%Y-%m"),
            type_label=_friendly_type_label(kind),
            subject=_display_subject(row, kind, account_map, debt_map),
            event_label=_display_event_label(row, kind),
            account_label=_display_account_label(row, kind, account_map, debt_map),
            amount=amount,
            currency=currency,
            base_amount=base_amount,
            context=_display_context(row, kind, account_map, debt_map),
            note=_display_note(row, kind, debt_map),
        )
        operation_rows.append(operation)

        flow_kind = str(row.get("flow_kind") or "normal")
        tx_type = str(row.get("type") or "")

        if flow_kind == "normal" and tx_type == "income":
            _add_money(total_income_map, currency, amount)
            if base_amount is not None:
                income_base += base_amount
                income_base_known = True
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "normal" and tx_type == "expense":
            _add_money(total_expense_map, currency, amount)
            category_name = operation.subject or SYSTEM_EXPENSE_CATEGORY_NAME
            if base_amount is not None:
                expense_base += base_amount
                expense_base_known = True
                expense_category_totals_base[category_name] += base_amount
                expense_rows_for_ranking.append((base_amount, category_name, _largest_expense_text(row, operation, base_amount, base_currency)))
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
            expense_currencies.add(currency or base_currency)
            _add_money(expense_category_totals_raw[category_name], currency, amount)
        elif flow_kind == "transfer" and kind == "savings" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(savings_map, currency, amount)
            if base_amount is not None:
                savings_base += base_amount
                savings_base_known = True
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "transfer" and kind == "investment" and str(row.get("transfer_subtype") or "") == "savings_transfer":
            _add_money(investments_map, currency, amount)
            if base_amount is not None:
                investments_base += base_amount
                investments_base_known = True
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True
        elif flow_kind == "debt" and _is_debt_payment_out(row):
            _add_money(debt_payments_map, currency, amount)
            if base_amount is not None:
                debt_payments_base += base_amount
                debt_payments_base_known = True
            elif amount is not None:
                base_gap_count += 1
                percent_partial = True

    total_income = _clean_money_bucket(total_income_map)
    total_expenses = _clean_money_bucket(total_expense_map)
    savings_total = _clean_money_bucket(savings_map)
    investments_total = _clean_money_bucket(investments_map)
    debt_payments_total = _clean_money_bucket(debt_payments_map)
    current_available_total = _clean_money_bucket(current_available_map)
    current_savings_total = _clean_money_bucket(current_savings_map)
    current_investments_total = _clean_money_bucket(current_investments_map)
    current_receivable_total = _clean_money_bucket(current_receivable_map)
    current_payable_total = _clean_money_bucket(current_payable_map)
    debt_balance_total = _subtract_money_maps(current_receivable_total, current_payable_total)

    money_left = _subtract_money_maps(
        total_income,
        total_expenses,
        savings_total,
        investments_total,
        debt_payments_total,
    )

    top_expense_category = _top_expense_category_text(
        expense_category_totals_raw=expense_category_totals_raw,
        expense_category_totals_base=expense_category_totals_base,
        expense_currencies=expense_currencies,
        base_currency=base_currency,
    )
    largest_expense = _largest_expense_summary(
        expense_rows_for_ranking=expense_rows_for_ranking,
        rows=rows,
        base_currency=base_currency,
    )

    expense_percentage = _format_percent(_percentage(expense_base, income_base))
    savings_percentage = _format_percent(_percentage(savings_base, income_base))
    investments_percentage = _format_percent(_percentage(investments_base, income_base))
    debt_payments_percentage = _format_percent(_percentage(debt_payments_base, income_base))
    money_left_base = income_base - expense_base - savings_base - investments_base - debt_payments_base
    money_left_base_known = any(
        (
            income_base_known,
            expense_base_known,
            savings_base_known,
            investments_base_known,
            debt_payments_base_known,
        )
    )

    notes: list[str] = []
    if base_gap_count > 0:
        notes.append(f"Сума в базовій валюті недоступна для {base_gap_count} операцій.")
    if percent_partial and income_base > 0:
        notes.append(f"Відсотки пораховано лише для сум, які можна зіставити з {base_currency}.")

    summary = ExportSummary(
        period_label=f"{start_date.isoformat()} - {(end_date - date.resolution).isoformat()} ({title})",
        base_currency=base_currency,
        total_income=_format_export_money_block(
            total_income,
            base_currency=base_currency,
            converted_total=income_base if income_base_known else None,
        ),
        total_expenses=_format_export_money_block(
            total_expenses,
            base_currency=base_currency,
            converted_total=expense_base if expense_base_known else None,
        ),
        money_left=_format_export_money_block(
            money_left,
            base_currency=base_currency,
            converted_total=money_left_base if money_left_base_known else None,
        ),
        expense_percentage=expense_percentage,
        savings_amount=_format_export_money_block(
            savings_total,
            base_currency=base_currency,
            converted_total=savings_base if savings_base_known else None,
        ),
        savings_percentage=savings_percentage,
        investments_amount=_format_export_money_block(
            investments_total,
            base_currency=base_currency,
            converted_total=investments_base if investments_base_known else None,
        ),
        investments_percentage=investments_percentage,
        debt_payments_amount=_format_export_money_block(
            debt_payments_total,
            base_currency=base_currency,
            converted_total=debt_payments_base if debt_payments_base_known else None,
        ),
        debt_payments_percentage=debt_payments_percentage,
        current_available_total=_format_export_money_block(current_available_total, base_currency=base_currency),
        current_savings_total=_format_export_money_block(current_savings_total, base_currency=base_currency),
        current_investments_total=_format_export_money_block(current_investments_total, base_currency=base_currency),
        current_receivable_total=_format_export_money_block(current_receivable_total, base_currency=base_currency),
        current_debts_total=_format_export_money_block(current_payable_total, base_currency=base_currency),
        debt_balance_total=_format_export_money_block(debt_balance_total, base_currency=base_currency),
        planned_savings_total=_pending_savings_total_text(pending_tasks),
        top_expense_category=top_expense_category,
        top_expense_lines=_top_expense_lines(
            expense_category_totals_raw=expense_category_totals_raw,
            expense_category_totals_base=expense_category_totals_base,
            expense_currencies=expense_currencies,
            base_currency=base_currency,
        ),
        debt_snapshot_lines=_debt_snapshot_lines(debts),
        goal_snapshot_lines=_goal_snapshot_lines(accounts, pending_tasks),
        largest_expense=largest_expense,
        transaction_count=len(operation_rows),
        export_date=date.today().isoformat(),
        notes=tuple(notes),
    )
    return summary, operation_rows


def _display_subject(
    row: asyncpg.Record,
    kind: str,
    account_map: dict[int, asyncpg.Record],
    debt_map: dict[int, asyncpg.Record],
) -> str:
    if kind in {"income", "expense"}:
        return _row_category_name(row)
    if kind in {"savings", "investment"}:
        target_account = account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None
        return str((target_account or {}).get("label") or "").strip()
    if kind == "debt":
        debt = _matched_debt(row, debt_map)
        return (str((debt or {}).get("counterparty_name") or row.get("counterparty") or "")).strip()
    return ""


def _display_event_label(row: asyncpg.Record, kind: str) -> str:
    flow_kind = str(row.get("flow_kind") or "").strip().lower()
    source = str(row.get("source") or "").strip().lower()
    transfer_subtype = str(row.get("transfer_subtype") or "").strip().lower()
    if flow_kind == "adjustment" or source == "balance_correction":
        return "Корекція балансу"
    if flow_kind == "transfer" and transfer_subtype == "credit_payment":
        return "Погашення кредитки"
    if kind == "income":
        return "Надходження"
    if kind == "expense":
        return "Витрата"
    if kind == "savings":
        return "Поповнення накопичення"
    if kind == "investment":
        return "Інвестування"
    if kind == "transfer":
        return "Внутрішній переказ"
    if kind == "debt":
        return _debt_description(row)
    return ""


def _display_account_label(
    row: asyncpg.Record,
    kind: str,
    account_map: dict[int, asyncpg.Record],
    debt_map: dict[int, asyncpg.Record],
) -> str:
    flow_kind = str(row.get("flow_kind") or "normal")
    if flow_kind == "transfer":
        from_label = _account_label(account_map.get(int(row["from_account_id"])) if row.get("from_account_id") is not None else None)
        to_label = _account_label(account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None)
        return f"{from_label} -> {to_label}".strip()
    if kind == "debt":
        debt = _matched_debt(row, debt_map)
        if debt and debt.get("account_id") is not None:
            return _account_label(account_map.get(int(debt["account_id"])))
    if row.get("account_id") is not None:
        return _account_label(account_map.get(int(row["account_id"])))
    return ""


def _display_context(
    row: asyncpg.Record,
    kind: str,
    account_map: dict[int, asyncpg.Record],
    debt_map: dict[int, asyncpg.Record],
) -> str:
    if kind == "debt":
        return _debt_context_text(_matched_debt(row, debt_map))
    if kind in {"savings", "investment"}:
        target_account = account_map.get(int(row["to_account_id"])) if row.get("to_account_id") is not None else None
        return _goal_context_text(target_account)
    if kind == "transfer":
        rate = row.get("fx_rate")
        if rate is not None:
            return f"Курс {format_decimal_value(Decimal(str(rate)), places=4)}"
        return "Не враховується як дохід чи витрата"
    return ""


def _display_note(row: asyncpg.Record, kind: str, debt_map: dict[int, asyncpg.Record]) -> str:
    note = (str(row.get("comment") or "")).strip()
    if note.lower() == "saving transfer":
        note = ""
    if note:
        return note
    if kind == "debt":
        debt = _matched_debt(row, debt_map)
        return (str((debt or {}).get("comment") or "")).strip()
    return ""


def _debt_description(row: asyncpg.Record) -> str:
    debt_action = str(row.get("debt_action") or "").strip().lower()
    tx_type = str(row.get("type") or "").strip().lower()
    labels = {
        "borrow": "Я взяв у борг",
        "borrow_repaid": "Платіж по боргу",
        "lend": "Я позичив",
        "lend_repaid": "Мені повернули",
        "debt_received": "Я взяв у борг",
        "debt_repayment_out": "Платіж по боргу",
        "debt_given": "Я позичив",
        "debt_repayment_in": "Мені повернули",
    }
    return labels.get(debt_action) or labels.get(tx_type) or "Борг"


def _largest_expense_text(row: asyncpg.Record, operation: ExportOperationRow, base_amount: Decimal, base_currency: str) -> str:
    return f"{row['date'].isoformat()} - {operation.subject or '—'} - {format_money(base_amount, base_currency)}"


def _top_expense_lines(
    *,
    expense_category_totals_raw: dict[str, defaultdict[str, Decimal]],
    expense_category_totals_base: defaultdict[str, Decimal],
    expense_currencies: set[str],
    base_currency: str,
) -> tuple[str, ...]:
    if not expense_category_totals_raw:
        return ("Немає витрат за період",)
    if len(expense_currencies) == 1:
        currency = next(iter(expense_currencies))
        ranked = sorted(
            expense_category_totals_raw.items(),
            key=lambda item: quantize_money(item[1].get(currency, Decimal("0"))),
            reverse=True,
        )[:3]
        return tuple(f"{category} — {format_money(amounts.get(currency, Decimal('0')), currency)}" for category, amounts in ranked)
    if expense_category_totals_base:
        ranked = sorted(expense_category_totals_base.items(), key=lambda item: item[1], reverse=True)[:3]
        return tuple(f"{category} — {format_money(amount, base_currency)}" for category, amount in ranked)
    ranked_raw = sorted(
        expense_category_totals_raw.items(),
        key=lambda item: sum((abs(value) for value in item[1].values()), Decimal("0")),
        reverse=True,
    )[:3]
    return tuple(f"{category} — {_format_money_map(_clean_money_bucket(amounts))}" for category, amounts in ranked_raw)


def _is_active_debt_snapshot(row: asyncpg.Record) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in {"closed", "cancelled"}:
        return False
    return quantize_money(Decimal(str(row.get("remaining_amount") or 0))) > 0


def _matched_debt(row: asyncpg.Record, debt_map: dict[int, asyncpg.Record]) -> asyncpg.Record | None:
    debt_id = row.get("debt_id")
    if debt_id is None:
        return None
    try:
        return debt_map.get(int(debt_id))
    except (TypeError, ValueError):
        return None


def _debt_direction_label(direction: str | None) -> str:
    value = str(direction or "").strip().lower()
    if value == "receivable":
        return "мені винні"
    if value == "payable":
        return "я винен"
    return "борг"


def _debt_status_label(status: str | None) -> str:
    value = str(status or "").strip().lower()
    labels = {
        "active": "активний",
        "partially_paid": "частково погашений",
        "closed": "закритий",
        "cancelled": "скасований",
        "needs_review": "потрібна перевірка",
    }
    return labels.get(value, value or "—")


def _debt_context_text(debt: asyncpg.Record | None) -> str:
    if not debt:
        return ""
    currency = str(debt.get("currency") or "UAH")
    remaining = Decimal(str(debt.get("remaining_amount") or 0))
    parts = [
        f"Залишок {format_money(remaining, currency)}",
        _debt_status_label(str(debt.get('status') or '')),
    ]
    if debt.get("due_date") is not None:
        parts.append(f"до {debt['due_date'].isoformat()}")
    return "; ".join(part for part in parts if part)


def _goal_context_text(account: asyncpg.Record | None) -> str:
    if not account:
        return ""
    goal_name = (str(account.get("goal_name") or "")).strip()
    goal_amount_raw = account.get("goal_amount")
    goal_date = account.get("goal_date")
    if not goal_name and goal_amount_raw in (None, "", 0) and goal_date is None:
        return ""
    parts: list[str] = []
    if goal_name:
        parts.append(goal_name)
    if goal_amount_raw not in (None, "", 0):
        currency = str(account.get("currency") or "UAH")
        balance = quantize_money(Decimal(str(account.get("balance") or 0)))
        goal_amount = quantize_money(Decimal(str(goal_amount_raw or 0)))
        progress = _format_percent(_percentage(balance, goal_amount))
        parts.append(f"{format_money(balance, currency)} / {format_money(goal_amount, currency)}")
        if progress != "—":
            parts.append(progress)
    if goal_date is not None:
        parts.append(f"до {goal_date.isoformat()}")
    return "; ".join(parts)


def _debt_snapshot_lines(debts: list[asyncpg.Record]) -> tuple[str, ...]:
    active_debts = [row for row in debts if _is_active_debt_snapshot(row)]
    if not active_debts:
        return ("Активних боргів немає",)
    ranked = sorted(
        active_debts,
        key=lambda row: (
            row.get("due_date") is None,
            row.get("due_date") or date.max,
            -Decimal(str(row.get("remaining_amount") or 0)),
        ),
    )[:3]
    lines: list[str] = []
    for debt in ranked:
        counterparty = (str(debt.get("counterparty_name") or "")).strip() or "—"
        amount = format_money(Decimal(str(debt.get("remaining_amount") or 0)), str(debt.get("currency") or "UAH"))
        direction = _debt_direction_label(str(debt.get("direction") or ""))
        status = _debt_status_label(str(debt.get("status") or ""))
        due_text = f", до {debt['due_date'].isoformat()}" if debt.get("due_date") is not None else ""
        lines.append(f"{counterparty} — {direction}; {amount}; {status}{due_text}")
    return tuple(lines)


def _legacy_debt_snapshots_from_rows(debt_rows: list[asyncpg.Record]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], dict[str, Decimal]] = {}
    for row in debt_rows:
        counterparty = (str(row.get("counterparty") or "")).strip() or "—"
        currency = normalize_currency(str(row.get("currency") or "UAH")) or "UAH"
        bucket = grouped.setdefault((counterparty, currency), {"receivable": Decimal("0"), "payable": Decimal("0")})
        amount = quantize_money(Decimal(str(row.get("amount") or 0)))
        action = str(row.get("debt_action") or "").strip().lower()
        if action == "lend":
            bucket["receivable"] += amount
        elif action == "lend_repaid":
            bucket["receivable"] -= amount
        elif action == "borrow":
            bucket["payable"] += amount
        elif action == "borrow_repaid":
            bucket["payable"] -= amount

    snapshots: list[dict[str, object]] = []
    synthetic_id = 1
    for (counterparty, currency), bucket in sorted(grouped.items()):
        receivable = max(quantize_money(bucket["receivable"]), Decimal("0"))
        payable = max(quantize_money(bucket["payable"]), Decimal("0"))
        if receivable > 0:
            snapshots.append(
                {
                    "id": synthetic_id,
                    "counterparty_name": counterparty,
                    "direction": "receivable",
                    "initial_amount": receivable,
                    "paid_amount": Decimal("0"),
                    "remaining_amount": receivable,
                    "currency": currency,
                    "status": "active",
                    "due_date": None,
                    "comment": "",
                }
            )
            synthetic_id += 1
        if payable > 0:
            snapshots.append(
                {
                    "id": synthetic_id,
                    "counterparty_name": counterparty,
                    "direction": "payable",
                    "initial_amount": payable,
                    "paid_amount": Decimal("0"),
                    "remaining_amount": payable,
                    "currency": currency,
                    "status": "active",
                    "due_date": None,
                    "comment": "",
                }
            )
            synthetic_id += 1
    return snapshots


def _category_breakdown_lines(
    *,
    category_totals_raw: dict[str, defaultdict[str, Decimal]],
    category_totals_base: dict[str, Decimal],
    total_base: Decimal,
    base_currency: str,
    empty_text: str,
    limit: int = 5,
) -> tuple[str, ...]:
    if not category_totals_raw:
        return (empty_text,)

    ranked: list[tuple[Decimal, str]] = []
    for category, amounts in category_totals_raw.items():
        cleaned_amounts = _clean_money_bucket(amounts)
        converted_total = category_totals_base.get(category)
        percent = _format_percent(_percentage(converted_total, total_base)) if converted_total is not None and total_base > 0 else "—"
        display = _format_export_money_inline(
            cleaned_amounts,
            base_currency=base_currency,
            converted_total=converted_total,
        )
        suffix = f" • {percent}" if percent != "—" else ""
        sort_amount = converted_total if converted_total is not None else sum((abs(value) for value in cleaned_amounts.values()), Decimal("0"))
        ranked.append((sort_amount, f"{category} — {display}{suffix}"))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return tuple(line for _, line in ranked[:limit])


def _savings_account_breakdown_lines(accounts: list[asyncpg.Record]) -> tuple[str, ...]:
    savings_accounts = [
        row
        for row in accounts
        if bool(row.get("is_active", True))
        and normalize_account_type(str(row.get("account_type") or "")) in {"savings", "deposit"}
    ]
    if not savings_accounts:
        return ("Немає окремих рахунків заощаджень",)

    lines: list[str] = []
    for account in savings_accounts[:5]:
        label = (str(account.get("label") or "")).strip() or "Заощадження"
        currency = str(account.get("currency") or "UAH")
        balance = quantize_money(Decimal(str(account.get("balance") or 0)))
        goal_amount_raw = account.get("goal_amount")
        if goal_amount_raw not in (None, "", 0):
            goal_amount = quantize_money(Decimal(str(goal_amount_raw or 0)))
            progress = _format_percent(_percentage(balance, goal_amount))
            line = f"{label} — {format_money(balance, currency)} / {format_money(goal_amount, currency)}"
            if progress != "—":
                line += f" • {progress}"
        else:
            line = f"{label} — {format_money(balance, currency)}"
        lines.append(line)
    return tuple(lines)


def _debt_burden_lines(
    *,
    current_receivable_total: dict[str, Decimal],
    current_payable_total: dict[str, Decimal],
    current_receivable_base: Decimal | None,
    current_payable_base: Decimal | None,
    income_base: Decimal,
    base_currency: str,
) -> tuple[str, ...]:
    lines: list[str] = []
    if current_receivable_total:
        receivable_percent = _format_percent(_percentage(current_receivable_base, income_base)) if current_receivable_base is not None and income_base > 0 else "—"
        line = (
            "Мені винні — "
            + _format_export_money_inline(
                current_receivable_total,
                base_currency=base_currency,
                converted_total=current_receivable_base,
            )
        )
        if receivable_percent != "—":
            line += f" • {receivable_percent} від доходу"
        lines.append(line)
    if current_payable_total:
        payable_percent = _format_percent(_percentage(current_payable_base, income_base)) if current_payable_base is not None and income_base > 0 else "—"
        line = (
            "Я винен — "
            + _format_export_money_inline(
                current_payable_total,
                base_currency=base_currency,
                converted_total=current_payable_base,
            )
        )
        if payable_percent != "—":
            line += f" • {payable_percent} від доходу"
        lines.append(line)
    if not lines:
        return ("Боргового навантаження немає",)
    return tuple(lines)


def _category_chart_points(
    *,
    category_totals_base: dict[str, Decimal],
    total_base: Decimal,
    limit: int = 5,
) -> tuple[tuple[str, Decimal], ...]:
    ranked = [
        (str(category), quantize_money(Decimal(str(amount or 0))))
        for category, amount in category_totals_base.items()
        if quantize_money(Decimal(str(amount or 0))) > 0
    ]
    ranked.sort(key=lambda item: item[1], reverse=True)
    points: list[tuple[str, Decimal]] = []
    for category, amount in ranked[:limit]:
        percent = _format_percent(_percentage(amount, total_base)) if total_base > 0 else "—"
        label = f"{category} • {percent}" if percent != "—" else category
        points.append((label, amount))
    return tuple(points)


def _savings_chart_points(
    *,
    accounts: list[asyncpg.Record],
    base_currency: str,
    fx_snapshot,
    limit: int = 5,
) -> tuple[tuple[str, Decimal, Decimal], ...]:
    points: list[tuple[str, Decimal, Decimal]] = []
    for account in accounts:
        if not bool(account.get("is_active", True)):
            continue
        if normalize_account_type(str(account.get("account_type") or "")) not in {"savings", "deposit"}:
            continue
        currency = str(account.get("currency") or base_currency)
        balance = quantize_money(Decimal(str(account.get("balance") or 0)))
        goal_amount = quantize_money(Decimal(str(account.get("goal_amount") or 0)))
        current_base = _convert_amount_to_base(balance, currency, base_currency, fx_snapshot) or Decimal("0")
        goal_base = _convert_amount_to_base(goal_amount, currency, base_currency, fx_snapshot) or Decimal("0")
        label = (str(account.get("goal_name") or "")).strip() or (str(account.get("label") or "")).strip() or "Заощадження"
        if current_base > 0 or goal_base > 0:
            progress = _format_percent(_percentage(current_base, goal_base)) if goal_base > 0 else "—"
            chart_label = f"{label} • {progress}" if progress != "—" else label
            points.append((chart_label, quantize_money(current_base), quantize_money(goal_base)))
    points.sort(key=lambda item: item[1], reverse=True)
    return tuple(points[:limit])


def _debt_chart_points(
    *,
    current_receivable_base: Decimal | None,
    current_payable_base: Decimal | None,
    income_base: Decimal,
) -> tuple[tuple[str, Decimal], ...]:
    points: list[tuple[str, Decimal]] = []
    if current_receivable_base is not None and quantize_money(current_receivable_base) > 0:
        points.append(("Мені винні", quantize_money(current_receivable_base)))
    if current_payable_base is not None and quantize_money(current_payable_base) > 0:
        points.append(("Я винен", quantize_money(current_payable_base)))
    return tuple(points)


def _debt_chart_points(
    *,
    current_receivable_base: Decimal | None,
    current_payable_base: Decimal | None,
    income_base: Decimal,
) -> tuple[tuple[str, Decimal], ...]:
    points: list[tuple[str, Decimal]] = []
    if current_receivable_base is not None and quantize_money(current_receivable_base) > 0:
        receivable_percent = _format_percent(_percentage(current_receivable_base, income_base)) if income_base > 0 else "вЂ”"
        label = f"РњРµРЅС– РІРёРЅРЅС– вЂў {receivable_percent}" if receivable_percent != "вЂ”" else "РњРµРЅС– РІРёРЅРЅС–"
        points.append((label, quantize_money(current_receivable_base)))
    if current_payable_base is not None and quantize_money(current_payable_base) > 0:
        payable_percent = _format_percent(_percentage(current_payable_base, income_base)) if income_base > 0 else "вЂ”"
        label = f"РЇ РІРёРЅРµРЅ вЂў {payable_percent}" if payable_percent != "вЂ”" else "РЇ РІРёРЅРµРЅ"
        points.append((label, quantize_money(current_payable_base)))
    return tuple(points)


def _goal_snapshot_lines(accounts: list[asyncpg.Record], pending_tasks: list[asyncpg.Record]) -> tuple[str, ...]:
    lines: list[str] = []
    goal_accounts = [
        row
        for row in accounts
        if bool(row.get("is_active", True))
        and (
            (str(row.get("goal_name") or "")).strip()
            or row.get("goal_amount") not in (None, "", 0)
            or row.get("goal_date") is not None
        )
    ]
    goal_accounts.sort(key=lambda row: (normalize_account_type(str(row.get("account_type") or "")) != "savings", int(row.get("id") or 0)))
    for account in goal_accounts[:2]:
        label = (str(account.get("goal_name") or "")).strip() or (str(account.get("label") or "")).strip() or "Ціль"
        context = _goal_context_text(account)
        lines.append(f"{label} — {context}" if context else label)
    if pending_tasks:
        routes = [
            f"{str(task.get('source_label') or '—').strip()} -> {str(task.get('target_label') or '—').strip()}"
            for task in pending_tasks[:2]
        ]
        lines.append(f"Плани: {', '.join(routes)}")
    if not lines:
        return ("Немає активних цілей",)
    return tuple(lines[:3])


def _debt_chart_points(
    *,
    current_receivable_base: Decimal | None,
    current_payable_base: Decimal | None,
    income_base: Decimal,
) -> tuple[tuple[str, Decimal], ...]:
    points: list[tuple[str, Decimal]] = []
    if current_receivable_base is not None and quantize_money(current_receivable_base) > 0:
        receivable_percent = _format_percent(_percentage(current_receivable_base, income_base)) if income_base > 0 else "—"
        label = f"Мені винні • {receivable_percent}" if receivable_percent != "—" else "Мені винні"
        points.append((label, quantize_money(current_receivable_base)))
    if current_payable_base is not None and quantize_money(current_payable_base) > 0:
        payable_percent = _format_percent(_percentage(current_payable_base, income_base)) if income_base > 0 else "—"
        label = f"Я винен • {payable_percent}" if payable_percent != "—" else "Я винен"
        points.append((label, quantize_money(current_payable_base)))
    return tuple(points)


def _pending_savings_total_text(pending_tasks: list[asyncpg.Record]) -> str:
    if not pending_tasks:
        return "—"
    totals = _money_map()
    for task in pending_tasks:
        _add_money(totals, str(task.get("currency") or ""), Decimal(str(task.get("amount") or 0)))
    return f"{len(pending_tasks)} / {_format_money_map(_clean_money_bucket(totals))}"


def _build_single_sheet_workbook(sheet_name: str, summary: ExportSummary, operation_rows: list[ExportOperationRow]) -> bytes:
    metric_rows = _summary_metrics(summary)
    rows_xml: list[str] = []
    merges: list[str] = []

    rows_xml.append(_row_xml(1, [_inline_cell("A1", sheet_name, 1)], height=24))
    merges.append("A1:K1")

    rows_xml.append(_row_xml(2, [_inline_cell("A2", "Підсумок", 2)], height=18))
    merges.append("A2:K2")

    current_row = 3
    for label, value in metric_rows:
        line_count = max(1, str(value).count("\n") + 1)
        row_cells = [
            _inline_cell(f"A{current_row}", label, 3),
            _inline_cell(f"B{current_row}", value, 4),
        ]
        rows_xml.append(_row_xml(current_row, row_cells, height=max(18, 18 + ((line_count - 1) * 14))))
        merges.append(f"B{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "Топ витрат", 6),
                _inline_cell(f"E{current_row}", "Борги", 6),
                _inline_cell(f"I{current_row}", "Цілі та плани", 6),
            ],
            height=20,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    rows_xml.append(
        _row_xml(
            current_row,
            [
                _inline_cell(f"A{current_row}", "\n".join(summary.top_expense_lines), 4),
                _inline_cell(f"E{current_row}", "\n".join(summary.debt_snapshot_lines), 4),
                _inline_cell(f"I{current_row}", "\n".join(summary.goal_snapshot_lines), 4),
            ],
            height=58,
        )
    )
    merges.extend([f"A{current_row}:D{current_row}", f"E{current_row}:H{current_row}", f"I{current_row}:K{current_row}"])
    current_row += 1

    if summary.notes:
        rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "\n".join(summary.notes), 5)], height=34))
        merges.append(f"A{current_row}:K{current_row}")
        current_row += 1

    rows_xml.append(_row_xml(current_row, [_inline_cell(f"A{current_row}", "Операції", 2)], height=18))
    merges.append(f"A{current_row}:K{current_row}")
    header_row = current_row + 1

    headers = [
        "Дата",
        "Місяць",
        "Тип",
        "Хто / категорія",
        "Що сталося",
        "Рахунок / маршрут",
        "Сума",
        "Валюта",
        "У базовій валюті",
        "Статус / контекст",
        "Нотатка",
    ]
    header_cells = [_inline_cell(f"{_col_name(index)}{header_row}", label, 6) for index, label in enumerate(headers, start=1)]
    rows_xml.append(_row_xml(header_row, header_cells, height=22))

    data_row = header_row + 1
    for operation in operation_rows:
        row_cells = [
            _number_cell(f"A{data_row}", _excel_date(operation.date_value), 8),
            _inline_cell(f"B{data_row}", operation.month, 7),
            _inline_cell(f"C{data_row}", operation.type_label, 7),
            _inline_cell(f"D{data_row}", operation.subject or "", 10),
            _inline_cell(f"E{data_row}", operation.event_label or "", 10),
            _inline_cell(f"F{data_row}", operation.account_label or "", 10),
            (_number_cell(f"G{data_row}", operation.amount, 9) if operation.amount is not None else _inline_cell(f"G{data_row}", "", 7)),
            _inline_cell(f"H{data_row}", operation.currency or "", 7),
            (_number_cell(f"I{data_row}", operation.base_amount, 9) if operation.base_amount is not None else _inline_cell(f"I{data_row}", "", 7)),
            _inline_cell(f"J{data_row}", operation.context or "", 10),
            _inline_cell(f"K{data_row}", operation.note or "", 10),
        ]
        rows_xml.append(_row_xml(data_row, row_cells, height=18))
        data_row += 1

    last_row = data_row - 1
    auto_filter_xml = f'<autoFilter ref="A{header_row}:K{last_row}"/>' if operation_rows else ""
    merge_xml = ""
    if merges:
        merge_cells_xml = "".join(f'<mergeCell ref="{ref}"/>' for ref in merges)
        merge_xml = f'<mergeCells count="{len(merges)}">{merge_cells_xml}</mergeCells>'

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:K{last_row}"/>'
        f'<sheetViews><sheetView workbookViewId="0"><selection activeCell="A{header_row}" sqref="A{header_row}"/></sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="2" width="10" customWidth="1"/>'
        '<col min="3" max="3" width="14" customWidth="1"/>'
        '<col min="4" max="4" width="18" customWidth="1"/>'
        '<col min="5" max="5" width="24" customWidth="1"/>'
        '<col min="6" max="6" width="22" customWidth="1"/>'
        '<col min="7" max="7" width="14" customWidth="1"/>'
        '<col min="8" max="8" width="10" customWidth="1"/>'
        '<col min="9" max="9" width="18" customWidth="1"/>'
        '<col min="10" max="10" width="28" customWidth="1"/>'
        '<col min="11" max="11" width="28" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        f"{auto_filter_xml}"
        f"{merge_xml}"
        '</worksheet>'
    )

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
"""

    root_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
"""

    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="16000"/></bookViews>'
        f'<sheets><sheet name="{xml_escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="2">
    <numFmt numFmtId="164" formatCode="yyyy-mm-dd"/>
    <numFmt numFmtId="165" formatCode="#,##0.00"/>
  </numFmts>
  <fonts count="5">
    <font><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="16"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
    <font><i/><sz val="11"/><color theme="1"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/><family val="2"/></font>
  </fonts>
  <fills count="7">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFF2F4F7"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF4472C4"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="11">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="2" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="3" borderId="0" xfId="0" applyFont="1" applyFill="1" applyAlignment="1"><alignment horizontal="left" vertical="center"/></xf>
    <xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="1" xfId="0" applyFont="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="6" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyFont="1" applyFill="1" applyBorder="1" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="165" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyBorder="1" applyAlignment="1"><alignment vertical="top"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyBorder="1" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
"""

    core_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>My Cash Flow Bot</dc:creator>
  <cp:lastModifiedBy>My Cash Flow Bot</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{date.today().isoformat()}T00:00:00Z</dcterms:modified>
</cp:coreProperties>
"""

    app_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>My Cash Flow Bot</Application>
</Properties>
"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return buffer.getvalue()
