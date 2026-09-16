from __future__ import annotations

import os
import sys
import types
import unittest
from copy import deepcopy
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from family_service import FamilyService, generate_family_invite_token, hash_family_invite_token  # noqa: E402
from finance_scope import get_current_finance_scope  # noqa: E402


class DummyTx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FamilyConn:
    def __init__(self) -> None:
        self.users = {
            101: {"tg_user_id": 101, "first_name": "Owner", "username": "owner", "onboarding_version": 2},
            202: {"tg_user_id": 202, "first_name": "Member", "username": "member", "onboarding_version": 0},
            303: {"tg_user_id": 303, "first_name": "Third", "username": "third", "onboarding_version": 2},
            404: {"tg_user_id": 404, "first_name": "Fourth", "username": "fourth", "onboarding_version": 2},
            505: {"tg_user_id": 505, "first_name": "Fifth", "username": "fifth", "onboarding_version": 2},
        }
        self.families: dict[int, dict] = {}
        self.family_members: list[dict] = []
        self.family_invites: list[dict] = []
        self.accounts = [{"tg_user_id": 101, "family_id": None, "created_by_user_id": None}]
        self.categories = [{"user_id": 101, "tg_user_id": 101, "family_id": None, "created_by_user_id": None, "updated_at": None}]
        self.transactions = [{"tg_user_id": 101, "family_id": None, "created_by_user_id": None, "updated_at": None}]
        self.next_family_id = 1
        self.next_membership_id = 1
        self.next_invite_id = 1

    def transaction(self) -> DummyTx:
        return DummyTx()

    def _active_family_members(self, family_id: int) -> list[dict]:
        return [row for row in self.family_members if row["family_id"] == family_id and row["status"] == "active"]

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT count(1) FROM family_members" in normalized:
            return len(self._active_family_members(int(args[0])))
        return None

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM family_members fm" in normalized and "LEFT JOIN users u" in normalized:
            family_id = int(args[0])
            rows = []
            for row in self.family_members:
                if row["family_id"] != family_id:
                    continue
                user = self.users.get(int(row["user_id"]), {})
                item = deepcopy(row)
                item["first_name"] = user.get("first_name")
                item["username"] = user.get("username")
                rows.append(item)
            rows.sort(key=lambda row: (0 if row["role"] == "owner" else 1, row["id"]))
            return rows
        if "FROM family_invites" in normalized and "WHERE family_id=$1" in normalized:
            family_id = int(args[0])
            rows = [
                deepcopy(row)
                for row in self.family_invites
                if row["family_id"] == family_id
                and row["status"] == "active"
                and row["used_count"] < row["max_uses"]
                and row["expires_at"] > datetime.now()
            ]
            rows.sort(key=lambda row: (-row["id"],))
            return rows
        return []

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM family_members fm" in normalized and "JOIN families f" in normalized and "LIMIT 1" in normalized:
            user_id = int(args[0])
            for row in sorted(self.family_members, key=lambda item: (0 if item["role"] == "owner" else 1, item["id"])):
                family = self.families.get(int(row["family_id"]))
                if row["user_id"] == user_id and row["status"] == "active" and family and family["status"] == "active":
                    return {"family_id": row["family_id"], "role": row["role"]}
            return None
        if normalized.startswith("INSERT INTO families"):
            family_id = self.next_family_id
            self.next_family_id += 1
            family = {
                "id": family_id,
                "name": args[0],
                "owner_user_id": int(args[1]),
                "status": "active",
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            self.families[family_id] = family
            return deepcopy(family)
        if normalized.startswith("INSERT INTO family_members"):
            family_id = int(args[0])
            user_id = int(args[1])
            existing = next((row for row in self.family_members if row["family_id"] == family_id and row["user_id"] == user_id), None)
            if existing is not None:
                existing.update(
                    {
                        "role": "member",
                        "status": "active",
                        "invited_by_user_id": int(args[2]),
                        "left_at": None,
                        "removed_at": None,
                        "updated_at": datetime.now(),
                    }
                )
                return deepcopy(existing)
            role = "owner" if "VALUES ($1, $2, 'owner'" in normalized else "member"
            membership = {
                "id": self.next_membership_id,
                "family_id": family_id,
                "user_id": user_id,
                "role": role,
                "status": "active",
                "invited_by_user_id": int(args[2]) if len(args) > 2 else user_id,
                "joined_at": datetime.now(),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "removed_at": None,
                "left_at": None,
            }
            self.next_membership_id += 1
            self.family_members.append(membership)
            return deepcopy(membership)
        if normalized.startswith("INSERT INTO family_invites"):
            invite = {
                "id": self.next_invite_id,
                "family_id": int(args[0]),
                "created_by_user_id": int(args[1]),
                "token_hash": str(args[2]),
                "status": "active",
                "max_uses": 1,
                "used_count": 0,
                "expires_at": args[3],
                "used_at": None,
                "used_by_user_id": None,
                "revoked_at": None,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            self.next_invite_id += 1
            self.family_invites.append(invite)
            return deepcopy(invite)
        if "SELECT f.*," in normalized and "FROM families f" in normalized:
            family = deepcopy(self.families.get(int(args[0])))
            if family is None:
                return None
            family["active_members_count"] = len(self._active_family_members(int(args[0])))
            return family
        if "SELECT * FROM families WHERE id=$1" in normalized:
            family = self.families.get(int(args[0]))
            return deepcopy(family) if family else None
        if "SELECT * FROM family_members WHERE family_id=$1 AND user_id=$2" in normalized:
            for row in self.family_members:
                if row["family_id"] == int(args[0]) and row["user_id"] == int(args[1]):
                    return deepcopy(row)
            return None
        if "FROM family_invites fi" in normalized and "LEFT JOIN users owner" in normalized and "LIMIT 1" in normalized:
            token_hash = str(args[0])
            invite = next((row for row in self.family_invites if row["token_hash"] == token_hash), None)
            if invite is None:
                return None
            family = self.families.get(int(invite["family_id"]))
            owner = self.users.get(int(family["owner_user_id"]), {}) if family else {}
            row = deepcopy(invite)
            row["family_name"] = family["name"]
            row["owner_user_id"] = family["owner_user_id"]
            row["owner_first_name"] = owner.get("first_name")
            row["owner_username"] = owner.get("username")
            row["active_members_count"] = len(self._active_family_members(int(invite["family_id"])))
            return row
        if "FROM family_invites fi" in normalized and "JOIN families f ON f.id = fi.family_id" in normalized and "FOR UPDATE" in normalized:
            token_hash = str(args[0])
            invite = next((row for row in self.family_invites if row["token_hash"] == token_hash), None)
            if invite is None:
                return None
            family = self.families.get(int(invite["family_id"]))
            row = deepcopy(invite)
            row["family_status"] = family["status"]
            row["owner_user_id"] = family["owner_user_id"]
            row["active_members_count"] = len(self._active_family_members(int(invite["family_id"])))
            return row
        if normalized.startswith("UPDATE family_invites SET status='used'"):
            invite = next(row for row in self.family_invites if row["id"] == int(args[0]))
            invite["status"] = "used"
            invite["used_count"] += 1
            invite["used_at"] = datetime.now()
            invite["used_by_user_id"] = int(args[1])
            invite["updated_at"] = datetime.now()
            return deepcopy(invite)
        if normalized.startswith("UPDATE family_invites SET status='revoked'"):
            invite = next((row for row in self.family_invites if row["id"] == int(args[0]) and row["family_id"] == int(args[1])), None)
            if invite is None:
                return None
            invite["status"] = "revoked"
            invite["revoked_at"] = datetime.now()
            invite["updated_at"] = datetime.now()
            return deepcopy(invite)
        if normalized.startswith("UPDATE family_members SET status='removed'"):
            for row in self.family_members:
                if row["family_id"] == int(args[0]) and row["user_id"] == int(args[1]) and row["status"] == "active":
                    row["status"] = "removed"
                    row["removed_at"] = datetime.now()
                    row["updated_at"] = datetime.now()
                    return deepcopy(row)
            return None
        if normalized.startswith("UPDATE family_members SET status='left'"):
            for row in self.family_members:
                if row["family_id"] == int(args[0]) and row["user_id"] == int(args[1]) and row["status"] == "active":
                    row["status"] = "left"
                    row["left_at"] = datetime.now()
                    row["updated_at"] = datetime.now()
                    return deepcopy(row)
            return None
        return None

    async def execute(self, query: str, *args) -> None:
        normalized = " ".join(query.split())
        if normalized.startswith("UPDATE family_invites SET status='revoked'"):
            for row in self.family_invites:
                if row["family_id"] == int(args[0]) and row["status"] == "active" and row["used_count"] < row["max_uses"]:
                    row["status"] = "revoked"
                    row["revoked_at"] = datetime.now()
                    row["updated_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE accounts SET family_id=$2"):
            for row in self.accounts:
                if row["tg_user_id"] == int(args[0]) and row["family_id"] is None:
                    row["family_id"] = int(args[1])
                    row["created_by_user_id"] = row["tg_user_id"]
                    row["updated_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE categories SET family_id=$2"):
            for row in self.categories:
                if row["user_id"] == int(args[0]) and row["family_id"] is None:
                    row["family_id"] = int(args[1])
                    row["created_by_user_id"] = row["user_id"]
                    row["updated_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE transactions SET family_id=$2"):
            for row in self.transactions:
                if row["tg_user_id"] == int(args[0]) and row["family_id"] is None:
                    row["family_id"] = int(args[1])
                    row["created_by_user_id"] = row["tg_user_id"]
                    row["updated_at"] = datetime.now()
            return
        if normalized.startswith("UPDATE users SET base_currency=COALESCE(base_currency, 'UAH')"):
            user = self.users[int(args[0])]
            user["base_currency"] = user.get("base_currency") or "UAH"
            user["start_date"] = user.get("start_date") or datetime.now().date()
            user["onboarding_completed"] = True
            user["onboarding_version"] = max(int(user.get("onboarding_version") or 0), 3)
            user["last_seen_at"] = datetime.now()


class FamilyServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_scope_returns_personal_without_membership(self) -> None:
        conn = FamilyConn()

        scope = await get_current_finance_scope(conn, 101)

        self.assertEqual(scope.type, "personal")
        self.assertIsNone(scope.family_id)

    async def test_create_family_moves_owner_data_and_sets_owner_scope(self) -> None:
        conn = FamilyConn()
        service = FamilyService(conn)

        result = await service.create_family(101, name="Family")
        scope = await get_current_finance_scope(conn, 101)

        self.assertEqual(result.status, "completed")
        self.assertEqual(scope.type, "family")
        self.assertEqual(conn.accounts[0]["family_id"], int(result.family["id"]))
        self.assertEqual(conn.categories[0]["family_id"], int(result.family["id"]))
        self.assertEqual(conn.transactions[0]["family_id"], int(result.family["id"]))

    async def test_create_invite_and_accept_member(self) -> None:
        conn = FamilyConn()
        service = FamilyService(conn)
        family_result = await service.create_family(101, name="Family")
        token = generate_family_invite_token()

        invite_result = await service.create_family_invite(101, token=token)
        accept_result = await service.accept_family_invite(token, user_id=202)

        self.assertEqual(invite_result.status, "completed")
        self.assertEqual(accept_result.status, "completed")
        self.assertEqual(accept_result.invite["status"], "used")
        scope = await get_current_finance_scope(conn, 202)
        self.assertEqual(scope.type, "family")
        self.assertEqual(scope.family_id, int(family_result.family["id"]))
        self.assertEqual(conn.users[202]["onboarding_version"], 3)

    async def test_owner_cannot_accept_own_invite(self) -> None:
        conn = FamilyConn()
        service = FamilyService(conn)
        await service.create_family(101, name="Family")
        token = generate_family_invite_token()
        await service.create_family_invite(101, token=token)

        result = await service.accept_family_invite(token, user_id=101)

        self.assertEqual(result.status, "already_in_family")

    async def test_family_capacity_is_limited_to_four_active_members(self) -> None:
        conn = FamilyConn()
        service = FamilyService(conn)
        await service.create_family(101, name="Family")
        for member_id in (202, 303, 404):
            token = generate_family_invite_token()
            await service.create_family_invite(101, token=token)
            await service.accept_family_invite(token, user_id=member_id)

        token = generate_family_invite_token()
        invite_result = await service.create_family_invite(101, token=token)

        self.assertEqual(invite_result.status, "family_full")

    async def test_member_can_leave_but_owner_cannot(self) -> None:
        conn = FamilyConn()
        service = FamilyService(conn)
        await service.create_family(101, name="Family")
        token = generate_family_invite_token()
        await service.create_family_invite(101, token=token)
        await service.accept_family_invite(token, user_id=202)

        owner_result = await service.leave_family(101)
        member_result = await service.leave_family(202)

        self.assertEqual(owner_result.status, "owner_blocked")
        self.assertEqual(member_result.status, "completed")
        personal_scope = await get_current_finance_scope(conn, 202)
        self.assertEqual(personal_scope.type, "personal")

    async def test_hash_is_deterministic(self) -> None:
        token = "abc123"
        self.assertEqual(hash_family_invite_token(token), hash_family_invite_token(token))
        self.assertNotEqual(hash_family_invite_token(token), hash_family_invite_token("def456"))


if __name__ == "__main__":
    unittest.main()
