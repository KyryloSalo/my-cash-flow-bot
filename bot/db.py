from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import asyncpg

logger = logging.getLogger("mcf.db")


@dataclass(frozen=True)
class Scope:
    kind: str  # "user" | "family"
    owner_tg_user_id: int
    family_id: str | None = None


async def upsert_user(
    conn: asyncpg.Connection,
    tg_user_id: int,
    first_name: str | None,
    username: str | None,
    lang: str | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO users (tg_user_id, first_name, username, lang, last_seen_at)
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (tg_user_id) DO UPDATE SET
            first_name = EXCLUDED.first_name,
            username = EXCLUDED.username,
            lang = COALESCE(EXCLUDED.lang, users.lang),
            last_seen_at = now();
        """,
        tg_user_id,
        first_name,
        username,
        lang,
    )


async def get_user(conn: asyncpg.Connection, tg_user_id: int) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM users WHERE tg_user_id=$1", tg_user_id)


async def get_active_scope(conn: asyncpg.Connection, tg_user_id: int) -> Scope:
    u = await get_user(conn, tg_user_id)
    if u and (u.get("active_scope") == "family") and u.get("active_family_id"):
        return Scope(kind="family", owner_tg_user_id=tg_user_id, family_id=str(u["active_family_id"]))
    return Scope(kind="user", owner_tg_user_id=tg_user_id)


async def set_active_scope_user(conn: asyncpg.Connection, tg_user_id: int) -> None:
    await conn.execute(
        "UPDATE users SET active_scope='user', active_family_id=NULL WHERE tg_user_id=$1",
        tg_user_id,
    )


async def set_active_scope_family(conn: asyncpg.Connection, tg_user_id: int, family_id: str) -> None:
    await conn.execute(
        "UPDATE users SET active_scope='family', active_family_id=$2 WHERE tg_user_id=$1",
        tg_user_id,
        family_id,
    )


async def set_user_onboarding_completed(conn: asyncpg.Connection, tg_user_id: int) -> None:
    await conn.execute(
        "UPDATE users SET onboarding_completed=true WHERE tg_user_id=$1",
        tg_user_id,
    )


async def set_user_profile(
    conn: asyncpg.Connection,
    tg_user_id: int,
    lang: str | None,
    base_currency: str | None,
    start_date: date | None,
) -> None:
    await conn.execute(
        """
        UPDATE users
        SET lang = COALESCE($2, lang),
            base_currency = COALESCE($3, base_currency),
            start_date = COALESCE($4, start_date)
        WHERE tg_user_id=$1
        """,
        tg_user_id,
        lang,
        base_currency,
        start_date,
    )


async def get_or_create_default_categories(
    conn: asyncpg.Connection,
    scope: Scope,
    kind: str,
    names: list[str],
) -> None:
    if scope.kind == "family":
        existing = await conn.fetchval(
            "SELECT count(1) FROM categories WHERE scope='family' AND family_id=$1 AND kind=$2",
            scope.family_id,
            kind,
        )
    else:
        existing = await conn.fetchval(
            "SELECT count(1) FROM categories WHERE scope='user' AND owner_tg_user_id=$1 AND kind=$2",
            scope.owner_tg_user_id,
            kind,
        )
    if existing and int(existing) > 0:
        return

    for idx, name in enumerate(names):
        await conn.execute(
            """
            INSERT INTO categories (scope, owner_tg_user_id, family_id, kind, name, sort_order)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            scope.kind,
            scope.owner_tg_user_id if scope.kind == "user" else None,
            scope.family_id if scope.kind == "family" else None,
            kind,
            name,
            idx,
        )


async def list_categories(conn: asyncpg.Connection, scope: Scope, kind: str) -> list[asyncpg.Record]:
    if scope.kind == "family":
        return await conn.fetch(
            """
            SELECT * FROM categories
            WHERE scope='family' AND family_id=$1 AND kind=$2
            ORDER BY is_active DESC, sort_order ASC, name ASC
            """,
            scope.family_id,
            kind,
        )
    return await conn.fetch(
        """
        SELECT * FROM categories
        WHERE scope='user' AND owner_tg_user_id=$1 AND kind=$2
        ORDER BY is_active DESC, sort_order ASC, name ASC
        """,
        scope.owner_tg_user_id,
        kind,
    )


async def upsert_category(
    conn: asyncpg.Connection,
    scope: Scope,
    kind: str,
    name: str,
) -> None:
    name = name.strip()
    if scope.kind == "family":
        await conn.execute(
            """
            INSERT INTO categories (scope, family_id, kind, name, is_active)
            VALUES ('family', $1, $2, $3, true)
            ON CONFLICT (family_id, kind, name) DO UPDATE SET
              is_active = true;
            """,
            scope.family_id,
            kind,
            name,
        )
        return
    await conn.execute(
        """
        INSERT INTO categories (scope, owner_tg_user_id, kind, name, is_active)
        VALUES ('user', $1, $2, $3, true)
        ON CONFLICT (owner_tg_user_id, kind, name) DO UPDATE SET
          is_active = true;
        """,
        scope.owner_tg_user_id,
        kind,
        name,
    )


async def rename_category(
    conn: asyncpg.Connection,
    scope: Scope,
    kind: str,
    old_name: str,
    new_name: str,
) -> int:
    if scope.kind == "family":
        res = await conn.execute(
            """
            UPDATE categories
            SET name=$4
            WHERE scope='family' AND family_id=$1 AND kind=$2 AND name=$3
            """,
            scope.family_id,
            kind,
            old_name,
            new_name,
        )
    else:
        res = await conn.execute(
            """
            UPDATE categories
            SET name=$4
            WHERE scope='user' AND owner_tg_user_id=$1 AND kind=$2 AND name=$3
            """,
            scope.owner_tg_user_id,
            kind,
            old_name,
            new_name,
        )
    return int(res.split()[-1])


async def set_category_active(
    conn: asyncpg.Connection,
    scope: Scope,
    kind: str,
    name: str,
    is_active: bool,
) -> int:
    if scope.kind == "family":
        res = await conn.execute(
            """
            UPDATE categories
            SET is_active=$4
            WHERE scope='family' AND family_id=$1 AND kind=$2 AND name=$3
            """,
            scope.family_id,
            kind,
            name,
            is_active,
        )
    else:
        res = await conn.execute(
            """
            UPDATE categories
            SET is_active=$4
            WHERE scope='user' AND owner_tg_user_id=$1 AND kind=$2 AND name=$3
            """,
            scope.owner_tg_user_id,
            kind,
            name,
            is_active,
        )
    return int(res.split()[-1])


async def create_account(
    conn: asyncpg.Connection,
    scope: Scope,
    label: str,
    currency: str,
    starting_balance: float,
    sort_order: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO accounts (scope, owner_tg_user_id, family_id, label, currency, starting_balance, sort_order)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        scope.kind,
        scope.owner_tg_user_id if scope.kind == "user" else None,
        scope.family_id if scope.kind == "family" else None,
        label,
        currency,
        starting_balance,
        sort_order,
    )


async def list_accounts(conn: asyncpg.Connection, scope: Scope) -> list[asyncpg.Record]:
    if scope.kind == "family":
        return await conn.fetch(
            """
            SELECT * FROM accounts
            WHERE scope='family' AND family_id=$1
            ORDER BY is_active DESC, sort_order ASC, label ASC
            """,
            scope.family_id,
        )
    return await conn.fetch(
        """
        SELECT * FROM accounts
        WHERE scope='user' AND owner_tg_user_id=$1
        ORDER BY is_active DESC, sort_order ASC, label ASC
        """,
        scope.owner_tg_user_id,
    )


async def set_account_active(conn: asyncpg.Connection, scope: Scope, label: str, is_active: bool) -> int:
    if scope.kind == "family":
        res = await conn.execute(
            "UPDATE accounts SET is_active=$3 WHERE scope='family' AND family_id=$1 AND label=$2",
            scope.family_id,
            label,
            is_active,
        )
    else:
        res = await conn.execute(
            "UPDATE accounts SET is_active=$3 WHERE scope='user' AND owner_tg_user_id=$1 AND label=$2",
            scope.owner_tg_user_id,
            label,
            is_active,
        )
    return int(res.split()[-1])


async def insert_transaction(conn: asyncpg.Connection, tx: dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO transactions (
          scope, owner_tg_user_id, family_id, actor_tg_user_id,
          date, type, amount, currency,
          from_account_id, to_account_id, category_id,
          comment, source, flow_kind, counterparty, debt_action
        )
        VALUES (
          $1, $2, $3, $4,
          $5, $6, $7, $8,
          $9, $10, $11,
          $12, $13, $14, $15, $16
        )
        """,
        tx["scope"],
        tx.get("owner_tg_user_id"),
        tx.get("family_id"),
        tx["actor_tg_user_id"],
        tx["date"],
        tx["type"],
        tx["amount"],
        tx["currency"],
        tx.get("from_account_id"),
        tx.get("to_account_id"),
        tx.get("category_id"),
        tx.get("comment"),
        tx["source"],
        tx.get("flow_kind", "normal"),
        tx.get("counterparty"),
        tx.get("debt_action"),
    )


async def month_report(conn: asyncpg.Connection, scope: Scope, month_start: date, month_end: date) -> dict[str, Any]:
    if scope.kind == "family":
        rows = await conn.fetch(
            """
            SELECT type, sum(amount) AS total
            FROM transactions
            WHERE scope='family' AND family_id=$1 AND date >= $2 AND date < $3 AND flow_kind='normal'
            GROUP BY type
            """,
            scope.family_id,
            month_start,
            month_end,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT type, sum(amount) AS total
            FROM transactions
            WHERE scope='user' AND owner_tg_user_id=$1 AND date >= $2 AND date < $3 AND flow_kind='normal'
            GROUP BY type
            """,
            scope.owner_tg_user_id,
            month_start,
            month_end,
        )
    totals = {r["type"]: float(r["total"] or 0) for r in rows}
    return totals


async def category_report(
    conn: asyncpg.Connection,
    scope: Scope,
    month_start: date,
    month_end: date,
    tx_type: str = "expense",
) -> list[asyncpg.Record]:
    if scope.kind == "family":
        return await conn.fetch(
            """
            SELECT c.name AS category, sum(t.amount) AS total
            FROM transactions t
            LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.scope='family' AND t.family_id=$1
              AND t.date >= $2 AND t.date < $3
              AND t.flow_kind='normal'
              AND t.type=$4
            GROUP BY c.name
            ORDER BY total DESC NULLS LAST
            """,
            scope.family_id,
            month_start,
            month_end,
            tx_type,
        )
    return await conn.fetch(
        """
        SELECT c.name AS category, sum(t.amount) AS total
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.scope='user' AND t.owner_tg_user_id=$1
          AND t.date >= $2 AND t.date < $3
          AND t.flow_kind='normal'
          AND t.type=$4
        GROUP BY c.name
        ORDER BY total DESC NULLS LAST
        """,
        scope.owner_tg_user_id,
        month_start,
        month_end,
        tx_type,
    )


async def debts_report(conn: asyncpg.Connection, scope: Scope) -> dict[str, list[dict[str, Any]]]:
    if scope.kind == "family":
        rows = await conn.fetch(
            """
            SELECT counterparty, currency,
                   sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
                   sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
            FROM transactions
            WHERE scope='family' AND family_id=$1 AND flow_kind='debt'
            GROUP BY counterparty, currency
            """,
            scope.family_id,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT counterparty, currency,
                   sum(CASE WHEN debt_action='lend' THEN amount WHEN debt_action='lend_repaid' THEN -amount ELSE 0 END) AS owed_to_me,
                   sum(CASE WHEN debt_action='borrow' THEN amount WHEN debt_action='borrow_repaid' THEN -amount ELSE 0 END) AS i_owe
            FROM transactions
            WHERE scope='user' AND owner_tg_user_id=$1 AND flow_kind='debt'
            GROUP BY counterparty, currency
            """,
            scope.owner_tg_user_id,
        )

    owed_to_me: list[dict[str, Any]] = []
    i_owe: list[dict[str, Any]] = []
    for r in rows:
        cp = (r["counterparty"] or "").strip() or "—"
        currency = (r["currency"] or "").strip() or ""
        o = float(r["owed_to_me"] or 0)
        i = float(r["i_owe"] or 0)
        if o > 0:
            owed_to_me.append({"counterparty": cp, "amount": o, "currency": currency})
        if i > 0:
            i_owe.append({"counterparty": cp, "amount": i, "currency": currency})
    owed_to_me.sort(key=lambda x: x["amount"], reverse=True)
    i_owe.sort(key=lambda x: x["amount"], reverse=True)
    return {"owed_to_me": owed_to_me[:10], "i_owe": i_owe[:10]}
