from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

import asyncpg

from finance_scope import FinanceScope, get_current_finance_scope

MAX_FAMILY_MEMBERS = 4
INVITE_TTL_DAYS = 7


@dataclass(frozen=True)
class FamilyActionResult:
    status: Literal[
        "completed",
        "feature_disabled",
        "already_in_family",
        "not_in_family",
        "not_owner",
        "owner_blocked",
        "invite_missing",
        "invite_not_active",
        "invite_expired",
        "invite_used",
        "invite_self_blocked",
        "family_full",
        "member_missing",
        "member_owner_blocked",
    ]
    family: asyncpg.Record | None = None
    membership: asyncpg.Record | None = None
    invite: asyncpg.Record | None = None
    removed_membership: asyncpg.Record | None = None


def generate_family_invite_token() -> str:
    return secrets.token_urlsafe(24)


def hash_family_invite_token(token: str) -> str:
    return hashlib.sha256((token or "").encode("utf-8")).hexdigest()


class FamilyService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_scope(self, user_id: int) -> FinanceScope:
        return await get_current_finance_scope(self.conn, user_id)

    async def create_family(self, owner_user_id: int, *, name: str) -> FamilyActionResult:
        scope = await self.get_scope(owner_user_id)
        if scope.is_family:
            return FamilyActionResult(status="already_in_family")

        async with self.conn.transaction():
            family = await self.conn.fetchrow(
                """
                INSERT INTO families (name, owner_user_id, status, created_at, updated_at)
                VALUES ($1, $2, 'active', now(), now())
                RETURNING *
                """,
                (name or "").strip()[:255] or "Сімейний бюджет",
                int(owner_user_id),
            )
            membership = await self.conn.fetchrow(
                """
                INSERT INTO family_members (
                  family_id, user_id, role, status, invited_by_user_id,
                  joined_at, created_at, updated_at
                )
                VALUES ($1, $2, 'owner', 'active', $2, now(), now(), now())
                RETURNING *
                """,
                int(family["id"]),
                int(owner_user_id),
            )
            await self._move_existing_owner_data_to_family(owner_user_id, int(family["id"]))

        return FamilyActionResult(status="completed", family=family, membership=membership)

    async def list_family_members(self, user_id: int) -> list[asyncpg.Record]:
        scope = await self.get_scope(user_id)
        if not scope.is_family:
            return []
        return await self.conn.fetch(
            """
            SELECT
              fm.family_id,
              fm.user_id,
              fm.role,
              fm.status,
              fm.joined_at,
              fm.left_at,
              fm.removed_at,
              u.first_name,
              u.username
            FROM family_members fm
            LEFT JOIN users u ON u.tg_user_id = fm.user_id
            WHERE fm.family_id=$1
            ORDER BY CASE WHEN fm.role='owner' THEN 0 ELSE 1 END, fm.joined_at ASC, fm.id ASC
            """,
            int(scope.family_id),
        )

    async def get_family_overview(self, user_id: int) -> asyncpg.Record | None:
        scope = await self.get_scope(user_id)
        if not scope.is_family:
            return None
        return await self.conn.fetchrow(
            """
            SELECT
              f.*,
              (
                SELECT count(1)
                FROM family_members fm
                WHERE fm.family_id=f.id AND fm.status='active'
              ) AS active_members_count
            FROM families f
            WHERE f.id=$1
            """,
            int(scope.family_id),
        )

    async def list_active_invites(self, user_id: int) -> list[asyncpg.Record]:
        scope = await self.get_scope(user_id)
        if not scope.is_family or not scope.is_owner:
            return []
        return await self.conn.fetch(
            """
            SELECT *
            FROM family_invites
            WHERE family_id=$1
              AND status='active'
              AND used_count < max_uses
              AND expires_at > now()
            ORDER BY created_at DESC, id DESC
            """,
            int(scope.family_id),
        )

    async def create_family_invite(self, owner_user_id: int, *, token: str) -> FamilyActionResult:
        scope = await self.get_scope(owner_user_id)
        if not scope.is_family:
            return FamilyActionResult(status="not_in_family")
        if not scope.is_owner:
            return FamilyActionResult(status="not_owner")

        active_members_count = int(
            await self.conn.fetchval(
                """
                SELECT count(1)
                FROM family_members
                WHERE family_id=$1 AND status='active'
                """,
                int(scope.family_id),
            )
            or 0
        )
        if active_members_count >= MAX_FAMILY_MEMBERS:
            return FamilyActionResult(status="family_full")

        async with self.conn.transaction():
            await self.conn.execute(
                """
                UPDATE family_invites
                SET status='revoked', revoked_at=now(), updated_at=now()
                WHERE family_id=$1
                  AND status='active'
                  AND used_count < max_uses
                """,
                int(scope.family_id),
            )
            invite = await self.conn.fetchrow(
                """
                INSERT INTO family_invites (
                  family_id, created_by_user_id, token_hash, status,
                  max_uses, used_count, expires_at, created_at, updated_at
                )
                VALUES ($1, $2, $3, 'active', 1, 0, $4, now(), now())
                RETURNING *
                """,
                int(scope.family_id),
                int(owner_user_id),
                hash_family_invite_token(token),
                datetime.now() + timedelta(days=INVITE_TTL_DAYS),
            )
        family = await self.get_family_overview(owner_user_id)
        return FamilyActionResult(status="completed", family=family, invite=invite)

    async def revoke_family_invite(self, owner_user_id: int, invite_id: int) -> FamilyActionResult:
        scope = await self.get_scope(owner_user_id)
        if not scope.is_family:
            return FamilyActionResult(status="not_in_family")
        if not scope.is_owner:
            return FamilyActionResult(status="not_owner")
        invite = await self.conn.fetchrow(
            """
            UPDATE family_invites
            SET status='revoked', revoked_at=now(), updated_at=now()
            WHERE id=$1 AND family_id=$2
            RETURNING *
            """,
            int(invite_id),
            int(scope.family_id),
        )
        if invite is None:
            return FamilyActionResult(status="invite_missing")
        return FamilyActionResult(status="completed", invite=invite)

    async def get_family_invite(self, token: str) -> asyncpg.Record | None:
        token_hash = hash_family_invite_token(token)
        return await self.conn.fetchrow(
            """
            SELECT
              fi.*,
              f.name AS family_name,
              f.owner_user_id,
              owner.first_name AS owner_first_name,
              owner.username AS owner_username,
              (
                SELECT count(1)
                FROM family_members fm
                WHERE fm.family_id=f.id AND fm.status='active'
              ) AS active_members_count
            FROM family_invites fi
            JOIN families f ON f.id = fi.family_id
            LEFT JOIN users owner ON owner.tg_user_id = f.owner_user_id
            WHERE fi.token_hash=$1
            LIMIT 1
            """,
            token_hash,
        )

    async def accept_family_invite(self, token: str, *, user_id: int) -> FamilyActionResult:
        scope = await self.get_scope(user_id)
        if scope.is_family:
            return FamilyActionResult(status="already_in_family")

        async with self.conn.transaction():
            invite = await self.conn.fetchrow(
                """
                SELECT
                  fi.*,
                  f.status AS family_status,
                  f.owner_user_id,
                  (
                    SELECT count(1)
                    FROM family_members fm
                    WHERE fm.family_id=f.id AND fm.status='active'
                  ) AS active_members_count
                FROM family_invites fi
                JOIN families f ON f.id = fi.family_id
                WHERE fi.token_hash=$1
                FOR UPDATE
                """,
                hash_family_invite_token(token),
            )
            if invite is None:
                return FamilyActionResult(status="invite_missing")
            if str(invite["status"] or "") != "active" or str(invite["family_status"] or "") != "active":
                return FamilyActionResult(status="invite_not_active")
            if invite["expires_at"] is not None and invite["expires_at"] <= datetime.now(invite["expires_at"].tzinfo):
                return FamilyActionResult(status="invite_expired")
            if int(invite["used_count"] or 0) >= int(invite["max_uses"] or 1):
                return FamilyActionResult(status="invite_used")
            if int(invite["owner_user_id"] or 0) == int(user_id):
                return FamilyActionResult(status="invite_self_blocked")
            if int(invite["active_members_count"] or 0) >= MAX_FAMILY_MEMBERS:
                return FamilyActionResult(status="family_full")

            existing_membership = await self.conn.fetchrow(
                """
                SELECT *
                FROM family_members
                WHERE family_id=$1 AND user_id=$2
                LIMIT 1
                """,
                int(invite["family_id"]),
                int(user_id),
            )
            if existing_membership is not None and str(existing_membership["status"] or "") == "active":
                return FamilyActionResult(status="already_in_family")

            membership = await self.conn.fetchrow(
                """
                INSERT INTO family_members (
                  family_id, user_id, role, status, invited_by_user_id,
                  joined_at, created_at, updated_at
                )
                VALUES ($1, $2, 'member', 'active', $3, now(), now(), now())
                ON CONFLICT (family_id, user_id) DO UPDATE SET
                  role='member',
                  status='active',
                  invited_by_user_id=EXCLUDED.invited_by_user_id,
                  joined_at=now(),
                  left_at=NULL,
                  removed_at=NULL,
                  updated_at=now()
                RETURNING *
                """,
                int(invite["family_id"]),
                int(user_id),
                int(invite["created_by_user_id"] or invite["owner_user_id"] or 0),
            )
            invite = await self.conn.fetchrow(
                """
                UPDATE family_invites
                SET status='used',
                    used_count=used_count + 1,
                    used_at=now(),
                    used_by_user_id=$2,
                    updated_at=now()
                WHERE id=$1
                RETURNING *
                """,
                int(invite["id"]),
                int(user_id),
            )
            await self._ensure_joining_user_profile(user_id)

        family = await self.conn.fetchrow("SELECT * FROM families WHERE id=$1", int(invite["family_id"]))
        return FamilyActionResult(status="completed", family=family, membership=membership, invite=invite)

    async def remove_family_member(self, owner_user_id: int, member_user_id: int) -> FamilyActionResult:
        scope = await self.get_scope(owner_user_id)
        if not scope.is_family:
            return FamilyActionResult(status="not_in_family")
        if not scope.is_owner:
            return FamilyActionResult(status="not_owner")
        if int(member_user_id) == int(owner_user_id):
            return FamilyActionResult(status="member_owner_blocked")

        membership = await self.conn.fetchrow(
            """
            UPDATE family_members
            SET status='removed', removed_at=now(), updated_at=now()
            WHERE family_id=$1 AND user_id=$2 AND status='active'
            RETURNING *
            """,
            int(scope.family_id),
            int(member_user_id),
        )
        if membership is None:
            return FamilyActionResult(status="member_missing")
        family = await self.get_family_overview(owner_user_id)
        return FamilyActionResult(status="completed", family=family, removed_membership=membership)

    async def leave_family(self, user_id: int) -> FamilyActionResult:
        scope = await self.get_scope(user_id)
        if not scope.is_family:
            return FamilyActionResult(status="not_in_family")
        if scope.is_owner:
            return FamilyActionResult(status="owner_blocked")

        membership = await self.conn.fetchrow(
            """
            UPDATE family_members
            SET status='left', left_at=now(), updated_at=now()
            WHERE family_id=$1 AND user_id=$2 AND status='active'
            RETURNING *
            """,
            int(scope.family_id),
            int(user_id),
        )
        if membership is None:
            return FamilyActionResult(status="member_missing")
        family = await self.conn.fetchrow("SELECT * FROM families WHERE id=$1", int(scope.family_id))
        return FamilyActionResult(status="completed", family=family, removed_membership=membership)

    async def _move_existing_owner_data_to_family(self, owner_user_id: int, family_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE accounts
            SET family_id=$2,
                created_by_user_id=COALESCE(created_by_user_id, tg_user_id),
                updated_at=now()
            WHERE tg_user_id=$1
              AND family_id IS NULL
            """,
            int(owner_user_id),
            int(family_id),
        )
        await self.conn.execute(
            """
            UPDATE categories
            SET family_id=$2,
                created_by_user_id=COALESCE(created_by_user_id, user_id, tg_user_id),
                updated_at=now()
            WHERE user_id=$1
              AND family_id IS NULL
            """,
            int(owner_user_id),
            int(family_id),
        )
        await self.conn.execute(
            """
            UPDATE transactions
            SET family_id=$2,
                created_by_user_id=COALESCE(created_by_user_id, tg_user_id)
            WHERE tg_user_id=$1
              AND family_id IS NULL
            """,
            int(owner_user_id),
            int(family_id),
        )
        await self.conn.execute(
            """
            UPDATE debts
            SET family_id=$2,
                updated_at=now()
            WHERE tg_user_id=$1
              AND family_id IS NULL
            """,
            int(owner_user_id),
            int(family_id),
        )
        await self.conn.execute(
            """
            UPDATE debt_payments
            SET family_id=$2
            WHERE tg_user_id=$1
              AND family_id IS NULL
            """,
            int(owner_user_id),
            int(family_id),
        )

    async def _ensure_joining_user_profile(self, user_id: int) -> None:
        await self.conn.execute(
            """
            UPDATE users
            SET base_currency=COALESCE(base_currency, 'UAH'),
                start_date=COALESCE(start_date, CURRENT_DATE),
                onboarding_completed=true,
                onboarding_version=GREATEST(COALESCE(onboarding_version, 0), 3),
                last_seen_at=now()
            WHERE tg_user_id=$1
            """,
            int(user_id),
        )
