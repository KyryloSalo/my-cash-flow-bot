from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import asyncpg


FinanceScopeType = Literal["personal", "family"]
FamilyRole = Literal["owner", "member"]


@dataclass(frozen=True)
class FinanceScope:
    type: FinanceScopeType
    user_id: int
    family_id: int | None = None
    role: FamilyRole | None = None

    @property
    def is_family(self) -> bool:
        return self.type == "family" and self.family_id is not None

    @property
    def is_owner(self) -> bool:
        return self.role == "owner"


async def get_current_finance_scope(conn: asyncpg.Connection, user_id: int) -> FinanceScope:
    membership = await conn.fetchrow(
        """
        SELECT fm.family_id, fm.role
        FROM family_members fm
        JOIN families f ON f.id = fm.family_id
        WHERE fm.user_id=$1
          AND fm.status='active'
          AND f.status='active'
        ORDER BY CASE WHEN fm.role='owner' THEN 0 ELSE 1 END, fm.joined_at ASC, fm.id ASC
        LIMIT 1
        """,
        int(user_id),
    )
    if membership is None:
        return FinanceScope(type="personal", user_id=int(user_id))
    return FinanceScope(
        type="family",
        user_id=int(user_id),
        family_id=int(membership["family_id"]),
        role=str(membership["role"] or "member"),
    )
