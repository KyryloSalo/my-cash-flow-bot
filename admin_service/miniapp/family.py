from __future__ import annotations

import hashlib
import secrets
from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db import connection, transaction as db_transaction
from django.utils import timezone

from users.models import TelegramUser


MAX_FAMILY_MEMBERS = 4
INVITE_TTL_DAYS = 7


class MiniAppFamilyError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def _family_feature_enabled() -> bool:
    return bool(getattr(settings, "FAMILY_ACCESS_ENABLED", True))


def _family_tables_available() -> bool:
    return {"families", "family_members", "family_invites"}.issubset(connection.introspection.table_names())


def _invite_token_hash(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _serialize_datetime(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _row_dict(cursor) -> dict[str, object] | None:
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip((column[0] for column in cursor.description), row, strict=True))


def _rows_dict(cursor) -> list[dict[str, object]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _active_membership(user_id: int, *, lock: bool = False) -> dict[str, object] | None:
    if not _family_tables_available():
        return None
    suffix = " FOR UPDATE OF f" if lock else ""
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT fm.family_id, fm.role, f.name AS family_name, f.owner_user_id
            FROM family_members fm
            JOIN families f ON f.id = fm.family_id
            WHERE fm.user_id = %s
              AND fm.status = 'active'
              AND f.status = 'active'
            ORDER BY CASE WHEN fm.role = 'owner' THEN 0 ELSE 1 END, fm.joined_at ASC, fm.id ASC
            LIMIT 1{suffix}
            """,
            [int(user_id)],
        )
        return _row_dict(cursor)


def _require_family_feature() -> None:
    if not _family_feature_enabled():
        raise MiniAppFamilyError("family_feature_disabled", "Family access is temporarily unavailable.", status=503)
    if not _family_tables_available():
        raise MiniAppFamilyError("family_unavailable", "Family data is temporarily unavailable.", status=503)


def _refresh_access_projection(user_id: int) -> None:
    from miniapp.services import resolve_access
    from users.models import UserAdminState
    user = TelegramUser.objects.get(tg_user_id=user_id)
    access = resolve_access(user)
    scope = str(access["access_scope"])
    UserAdminState.objects.update_or_create(telegram_user_id=user_id, defaults={"access_scope": scope, "access_source": "family" if scope == "family_full" else "billing"})


def _serialize_member(row: dict[str, object]) -> dict[str, object]:
    display_name = str(row.get("first_name") or "").strip() or str(row.get("username") or "").strip() or "Member"
    return {
        "user_id": int(row["user_id"]),
        "name": display_name[:120],
        "username": str(row.get("username") or "").strip()[:120] or None,
        "role": str(row.get("role") or "member"),
        "status": str(row.get("status") or "inactive"),
        "joined_at": _serialize_datetime(row.get("joined_at")),
    }


def _serialize_invite(row: dict[str, object]) -> dict[str, object]:
    return {
        "id": int(row["id"]),
        "expires_at": _serialize_datetime(row.get("expires_at")),
        "created_at": _serialize_datetime(row.get("created_at")),
    }


def build_family_payload(user: TelegramUser) -> dict[str, object]:
    if not _family_feature_enabled():
        return {"enabled": False, "mode": "unavailable"}
    if not _family_tables_available():
        return {"enabled": False, "mode": "unavailable"}

    membership = _active_membership(int(user.tg_user_id))
    if membership is None:
        return {
            "enabled": True,
            "mode": "personal",
            "can_create": True,
            "can_join": True,
            "max_members": MAX_FAMILY_MEMBERS,
        }

    family_id = int(membership["family_id"])
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT f.id, f.name, f.owner_user_id, f.created_at,
                   count(fm.id) FILTER (WHERE fm.status = 'active') AS active_members_count
            FROM families f
            LEFT JOIN family_members fm ON fm.family_id = f.id
            WHERE f.id = %s AND f.status = 'active'
            GROUP BY f.id
            """,
            [family_id],
        )
        family = _row_dict(cursor)
        if family is None:
            return {"enabled": True, "mode": "personal", "can_create": True, "can_join": True, "max_members": MAX_FAMILY_MEMBERS}

        cursor.execute(
            """
            SELECT fm.user_id, fm.role, fm.status, fm.joined_at, u.first_name, u.username
            FROM family_members fm
            LEFT JOIN users u ON u.tg_user_id = fm.user_id
            WHERE fm.family_id = %s AND fm.status = 'active'
            ORDER BY CASE WHEN fm.role = 'owner' THEN 0 ELSE 1 END, fm.joined_at ASC, fm.id ASC
            """,
            [family_id],
        )
        members = _rows_dict(cursor)

        invites: list[dict[str, object]] = []
        if str(membership.get("role") or "") == "owner":
            cursor.execute(
                """
                SELECT id, expires_at, created_at
                FROM family_invites
                WHERE family_id = %s
                  AND status = 'active'
                  AND used_count < max_uses
                  AND expires_at > now()
                ORDER BY created_at DESC, id DESC
                """,
                [family_id],
            )
            invites = _rows_dict(cursor)

    return {
        "enabled": True,
        "mode": "family",
        "family": {
            "id": family_id,
            "name": str(family.get("name") or "Family budget")[:255],
            "role": str(membership.get("role") or "member"),
            "owner_user_id": int(family.get("owner_user_id") or 0),
            "active_members_count": int(family.get("active_members_count") or 0),
            "max_members": MAX_FAMILY_MEMBERS,
            "created_at": _serialize_datetime(family.get("created_at")),
        },
        "members": [_serialize_member(member) for member in members],
        "invites": [_serialize_invite(invite) for invite in invites],
    }


def create_family(user: TelegramUser, *, name: str) -> dict[str, object]:
    _require_family_feature()
    user_id = int(user.tg_user_id)
    with db_transaction.atomic():
        TelegramUser.objects.select_for_update().get(tg_user_id=user_id)
        if _active_membership(user_id, lock=True) is not None:
            raise MiniAppFamilyError("already_in_family", "You already belong to a family.", status=409)

        family_name = str(name or "").strip()[:255] or "Family budget"
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO families (name, owner_user_id, status, created_at, updated_at)
                VALUES (%s, %s, 'active', now(), now())
                RETURNING id
                """,
                [family_name, user_id],
            )
            family_id = int(_row_dict(cursor)["id"])
            cursor.execute(
                """
                INSERT INTO family_members (
                    family_id, user_id, role, status, invited_by_user_id,
                    joined_at, created_at, updated_at
                )
                VALUES (%s, %s, 'owner', 'active', %s, now(), now(), now())
                """,
                [family_id, user_id, user_id],
            )
            _move_owner_data_to_family(cursor, owner_user_id=user_id, family_id=family_id)

    return build_family_payload(user)


def _move_owner_data_to_family(cursor, *, owner_user_id: int, family_id: int) -> None:
    cursor.execute(
        """
        UPDATE accounts
        SET family_id = %s,
            created_by_user_id = COALESCE(created_by_user_id, tg_user_id),
            updated_at = now()
        WHERE tg_user_id = %s AND family_id IS NULL
        """,
        [family_id, owner_user_id],
    )
    cursor.execute(
        """
        UPDATE categories
        SET family_id = %s,
            created_by_user_id = COALESCE(created_by_user_id, user_id, tg_user_id),
            updated_at = now()
        WHERE user_id = %s AND family_id IS NULL
        """,
        [family_id, owner_user_id],
    )
    cursor.execute(
        """
        UPDATE transactions
        SET family_id = %s,
            created_by_user_id = COALESCE(created_by_user_id, tg_user_id)
        WHERE tg_user_id = %s AND family_id IS NULL
        """,
        [family_id, owner_user_id],
    )
    cursor.execute(
        """
        UPDATE debts
        SET family_id = %s, updated_at = now()
        WHERE tg_user_id = %s AND family_id IS NULL
        """,
        [family_id, owner_user_id],
    )
    cursor.execute(
        """
        UPDATE debt_payments
        SET family_id = %s
        WHERE tg_user_id = %s AND family_id IS NULL
        """,
        [family_id, owner_user_id],
    )


def create_family_invite(user: TelegramUser) -> dict[str, object]:
    _require_family_feature()
    user_id = int(user.tg_user_id)
    with db_transaction.atomic():
        TelegramUser.objects.select_for_update().get(tg_user_id=user_id)
        membership = _active_membership(user_id, lock=True)
        if membership is None:
            raise MiniAppFamilyError("not_in_family", "Create or join a family first.", status=409)
        if str(membership.get("role") or "") != "owner":
            raise MiniAppFamilyError("not_owner", "Only the family owner can create an invite.", status=403)
        family_id = int(membership["family_id"])
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(1) FROM family_members WHERE family_id = %s AND status = 'active'",
                [family_id],
            )
            if int(cursor.fetchone()[0] or 0) >= MAX_FAMILY_MEMBERS:
                raise MiniAppFamilyError("family_full", "The family already has the maximum number of members.", status=409)
            cursor.execute(
                """
                UPDATE family_invites
                SET status = 'revoked', revoked_at = now(), updated_at = now()
                WHERE family_id = %s AND status = 'active' AND used_count < max_uses
                """,
                [family_id],
            )
            token = secrets.token_urlsafe(24)
            cursor.execute(
                """
                INSERT INTO family_invites (
                    family_id, created_by_user_id, token_hash, status,
                    max_uses, used_count, expires_at, created_at, updated_at
                )
                VALUES (%s, %s, %s, 'active', 1, 0, %s, now(), now())
                RETURNING id, expires_at
                """,
                [family_id, user_id, _invite_token_hash(token), timezone.now() + timedelta(days=INVITE_TTL_DAYS)],
            )
            invite = _row_dict(cursor) or {}

    return {
        "invite": {
            "id": int(invite["id"]),
            "token": token,
            "expires_at": _serialize_datetime(invite.get("expires_at")),
        },
        "family": build_family_payload(user),
    }


def revoke_family_invite(user: TelegramUser, *, invite_id: int) -> dict[str, object]:
    _require_family_feature()
    user_id = int(user.tg_user_id)
    with db_transaction.atomic():
        TelegramUser.objects.select_for_update().get(tg_user_id=user_id)
        membership = _active_membership(user_id, lock=True)
        if membership is None:
            raise MiniAppFamilyError("not_in_family", "You are not in a family.", status=409)
        if str(membership.get("role") or "") != "owner":
            raise MiniAppFamilyError("not_owner", "Only the family owner can revoke an invite.", status=403)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE family_invites
                SET status = 'revoked', revoked_at = now(), updated_at = now()
                WHERE id = %s AND family_id = %s
                RETURNING id
                """,
                [int(invite_id), int(membership["family_id"])],
            )
            if _row_dict(cursor) is None:
                raise MiniAppFamilyError("invite_missing", "The invite is no longer available.", status=404)
    return build_family_payload(user)


def accept_family_invite(user: TelegramUser, *, token: str) -> dict[str, object]:
    from miniapp.services import _family_sponsor_has_access
    from users.models import UserAdminState
    _require_family_feature()
    user_id = int(user.tg_user_id)
    normalized_token = str(token or "").strip()
    if not normalized_token:
        raise MiniAppFamilyError("invite_missing", "Enter a valid family invite code.")

    with db_transaction.atomic():
        TelegramUser.objects.select_for_update().get(tg_user_id=user_id)
        state = UserAdminState.objects.select_for_update().filter(telegram_user_id=user_id).first()
        if state and (state.is_blocked or state.status == "banned"):
            raise MiniAppFamilyError("access_blocked", "This user is blocked.", status=403)
        if _active_membership(user_id, lock=True) is not None:
            raise MiniAppFamilyError("already_in_family", "Leave the current family before joining another one.", status=409)
        with connection.cursor() as cursor:
            # Lock family before invite, matching owner invite/revoke operations.
            cursor.execute("SELECT family_id FROM family_invites WHERE token_hash=%s", [_invite_token_hash(normalized_token)])
            target = cursor.fetchone()
            if target is None:
                raise MiniAppFamilyError("invite_missing", "The invite was not found.", status=404)
            cursor.execute("SELECT id FROM families WHERE id=%s FOR UPDATE", [target[0]])
            cursor.execute(
                """
                SELECT fi.id, fi.family_id, fi.status, fi.used_count, fi.max_uses, fi.expires_at,
                       fi.created_by_user_id, f.status AS family_status, f.owner_user_id
                FROM family_invites fi
                JOIN families f ON f.id = fi.family_id
                WHERE fi.token_hash = %s
                FOR UPDATE OF fi
                """,
                [_invite_token_hash(normalized_token)],
            )
            invite = _row_dict(cursor)
            if invite is None:
                raise MiniAppFamilyError("invite_missing", "The invite was not found.", status=404)
            if str(invite.get("status") or "") != "active" or str(invite.get("family_status") or "") != "active":
                raise MiniAppFamilyError("invite_inactive", "The invite is no longer active.", status=409)
            if invite.get("expires_at") is not None and invite["expires_at"] <= timezone.now():
                raise MiniAppFamilyError("invite_expired", "The invite has expired.", status=409)
            if int(invite.get("used_count") or 0) >= int(invite.get("max_uses") or 1):
                raise MiniAppFamilyError("invite_used", "The invite has already been used.", status=409)
            if int(invite.get("owner_user_id") or 0) == user_id:
                raise MiniAppFamilyError("invite_self_blocked", "The owner cannot accept their own invite.", status=409)
            if not _family_sponsor_has_access(int(invite["family_id"])):
                raise MiniAppFamilyError("family_sponsor_inactive", "The family owner needs active access before you can join.", status=403)
            cursor.execute(
                """
                SELECT count(1)
                FROM family_members
                WHERE family_id = %s AND status = 'active'
                """,
                [int(invite["family_id"])],
            )
            active_members_count = int(cursor.fetchone()[0] or 0)
            if active_members_count >= MAX_FAMILY_MEMBERS:
                raise MiniAppFamilyError("family_full", "The family already has the maximum number of members.", status=409)
            cursor.execute(
                """
                INSERT INTO family_members (
                    family_id, user_id, role, status, invited_by_user_id,
                    joined_at, created_at, updated_at
                )
                VALUES (%s, %s, 'member', 'active', %s, now(), now(), now())
                ON CONFLICT (family_id, user_id) DO UPDATE SET
                    role = 'member', status = 'active', invited_by_user_id = EXCLUDED.invited_by_user_id,
                    joined_at = now(), left_at = NULL, removed_at = NULL, updated_at = now()
                """,
                [int(invite["family_id"]), user_id, int(invite.get("created_by_user_id") or invite["owner_user_id"])],
            )
            cursor.execute(
                """
                UPDATE family_invites
                SET status = 'used', used_count = used_count + 1, used_at = now(), used_by_user_id = %s, updated_at = now()
                WHERE id = %s
                """,
                [user_id, int(invite["id"])],
            )
            cursor.execute(
                """
                UPDATE users
                SET base_currency = COALESCE(base_currency, 'UAH'),
                    start_date = COALESCE(start_date, CURRENT_DATE),
                    onboarding_completed = true,
                    onboarding_version = GREATEST(COALESCE(onboarding_version, 0), 3),
                    last_seen_at = now()
                WHERE tg_user_id = %s
                """,
                [user_id],
            )
        _refresh_access_projection(user_id)
        user.__dict__.pop("_miniapp_finance_scope", None)
    return build_family_payload(user)


def remove_family_member(user: TelegramUser, *, member_user_id: int) -> dict[str, object]:
    _require_family_feature()
    user_id = int(user.tg_user_id)
    if int(member_user_id) == user_id:
        raise MiniAppFamilyError("member_owner_blocked", "The owner cannot remove themselves.", status=409)
    with db_transaction.atomic():
        list(TelegramUser.objects.filter(tg_user_id__in=[user_id, member_user_id]).order_by("tg_user_id").select_for_update())
        membership = _active_membership(user_id, lock=True)
        if membership is None:
            raise MiniAppFamilyError("not_in_family", "You are not in a family.", status=409)
        if str(membership.get("role") or "") != "owner":
            raise MiniAppFamilyError("not_owner", "Only the family owner can remove a member.", status=403)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE family_members
                SET status = 'removed', removed_at = now(), updated_at = now()
                WHERE family_id = %s AND user_id = %s AND status = 'active'
                RETURNING id
                """,
                [int(membership["family_id"]), int(member_user_id)],
            )
            if _row_dict(cursor) is None:
                raise MiniAppFamilyError("member_missing", "The member is no longer active.", status=404)
        _refresh_access_projection(int(member_user_id))
    return build_family_payload(user)


def leave_family(user: TelegramUser) -> dict[str, object]:
    _require_family_feature()
    user_id = int(user.tg_user_id)
    with db_transaction.atomic():
        TelegramUser.objects.select_for_update().get(tg_user_id=user_id)
        membership = _active_membership(user_id, lock=True)
        if membership is None:
            raise MiniAppFamilyError("not_in_family", "You are not in a family.", status=409)
        if str(membership.get("role") or "") == "owner":
            raise MiniAppFamilyError("owner_blocked", "The family owner cannot leave. Remove members first or continue in Telegram.", status=409)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE family_members
                SET status = 'left', left_at = now(), updated_at = now()
                WHERE family_id = %s AND user_id = %s AND status = 'active'
                RETURNING id
                """,
                [int(membership["family_id"]), user_id],
            )
            if _row_dict(cursor) is None:
                raise MiniAppFamilyError("member_missing", "The membership is no longer active.", status=404)
        _refresh_access_projection(user_id)
        user.__dict__.pop("_miniapp_finance_scope", None)
    return build_family_payload(user)
