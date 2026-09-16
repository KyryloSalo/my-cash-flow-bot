from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import UTC, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

if "httpx" not in sys.modules:
    httpx_stub = types.ModuleType("httpx")

    class _AsyncClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def post(self, *args, **kwargs):
            raise AssertionError("HTTP client should not be used in family access tests")

    httpx_stub.AsyncClient = _AsyncClient
    sys.modules["httpx"] = httpx_stub

import billing_client  # noqa: E402


def _default_billing_state() -> dict:
    return {
        "profile_exists": False,
        "has_card": False,
        "masked_pan": "",
        "profile_status": "",
        "auto_renew_enabled": False,
        "last_charge_status": "",
        "last_failure_reason": "",
        "last_action_url": "",
        "subscription_status": "",
        "expires_at": None,
        "next_charge_at": None,
        "grace_expires_at": None,
        "trial_days": 0,
        "access_mode": "open",
        "soft_grace_active": False,
    }


def _personal_full_billing_state() -> dict:
    state = _default_billing_state()
    expiry = datetime(2030, 1, 1, tzinfo=UTC)
    state.update(
        {
            "profile_exists": True,
            "has_card": True,
            "masked_pan": "444455******1111",
            "profile_status": "active",
            "auto_renew_enabled": True,
            "subscription_status": "trial",
            "expires_at": expiry,
            "next_charge_at": expiry,
            "trial_days": 30,
            "access_mode": "full",
        }
    )
    return state


class BillingFamilyAccessConn:
    def __init__(
        self,
        *,
        finance_scope: dict | None,
        billing_states: dict[int, dict] | None = None,
        family_owner_by_id: dict[int, int] | None = None,
        admin_state: dict | None = None,
    ) -> None:
        self.finance_scope = dict(finance_scope) if finance_scope else None
        self.billing_states = {
            int(user_id): {**_default_billing_state(), **dict(state)}
            for user_id, state in (billing_states or {}).items()
        }
        self.family_owner_by_id = {int(family_id): int(user_id) for family_id, user_id in (family_owner_by_id or {}).items()}
        self.admin_state = {
            "access_scope": "",
            "access_source": "",
            "pending_start_payload": "",
        }
        if admin_state:
            self.admin_state.update(admin_state)

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT to_regclass($1) IS NOT NULL" in normalized:
            return str(args[0]) in {"billing_profiles", "subscriptions", "user_admin_states", "promo_offers"}
        raise AssertionError(f"Unexpected fetchval query: {normalized}")

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM family_members fm" in normalized and "JOIN families f" in normalized:
            return dict(self.finance_scope) if self.finance_scope else None
        if "SELECT user_id FROM family_members" in normalized and "role='owner'" in normalized:
            owner_user_id = self.family_owner_by_id.get(int(args[0]))
            return {"user_id": owner_user_id} if owner_user_id is not None else None
        if "FROM billing_profiles" in normalized and "WHERE telegram_user_id=$1" in normalized:
            state = self.billing_states.get(int(args[0]), _default_billing_state())
            if not state.get("profile_exists"):
                return None
            return {
                "status": state.get("profile_status"),
                "masked_pan": state.get("masked_pan"),
                "auto_renew_enabled": state.get("auto_renew_enabled"),
                "last_charge_status": state.get("last_charge_status"),
                "last_failure_reason": state.get("last_failure_reason"),
                "last_action_url": state.get("last_action_url"),
                "has_card": bool(state.get("has_card")),
            }
        if "FROM subscriptions" in normalized and "WHERE telegram_user_id=$1" in normalized:
            state = self.billing_states.get(int(args[0]), _default_billing_state())
            if not state.get("subscription_status"):
                return None
            return {
                "status": state.get("subscription_status"),
                "expires_at": state.get("expires_at"),
                "next_charge_at": state.get("next_charge_at"),
                "grace_expires_at": state.get("grace_expires_at"),
                "trial_days": state.get("trial_days"),
            }
        if "FROM user_admin_states" in normalized and "SELECT access_scope, access_source, pending_start_payload" in normalized:
            return dict(self.admin_state)
        if "FROM promo_offers" in normalized and "WHERE upper(code)=upper($1)" in normalized:
            return None
        raise AssertionError(f"Unexpected fetchrow query: {normalized}")


class BillingFamilyAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_billing_state_uses_boolean_has_card_flag(self) -> None:
        conn = BillingFamilyAccessConn(finance_scope=None, billing_states={288834786: _personal_full_billing_state()})

        state = await billing_client.get_billing_state(conn, 288834786)

        self.assertTrue(state["has_card"])
        self.assertEqual(state["masked_pan"], "444455******1111")

    async def test_family_owner_without_personal_subscription_is_paywalled(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope={"family_id": 3, "role": "owner"},
            billing_states={288834786: _default_billing_state()},
            family_owner_by_id={3: 288834786},
            admin_state={"access_scope": "family_full", "access_source": "family", "pending_start_payload": ""},
        )

        state = await billing_client.get_access_state(conn, 288834786)

        self.assertEqual(state["access_scope"], "paywall")
        self.assertEqual(state["access_source"], "billing")

    async def test_family_member_gets_family_full_only_when_owner_has_personal_full(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope={"family_id": 3, "role": "member"},
            billing_states={
                5684474126: _default_billing_state(),
                288834786: _personal_full_billing_state(),
            },
            family_owner_by_id={3: 288834786},
        )

        state = await billing_client.get_access_state(conn, 5684474126)

        self.assertEqual(state["access_scope"], "family_full")
        self.assertEqual(state["access_source"], "family")

    async def test_family_member_keeps_personal_full_when_owner_is_not_sponsored(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope={"family_id": 3, "role": "member"},
            billing_states={
                5684474126: _personal_full_billing_state(),
                288834786: _default_billing_state(),
            },
            family_owner_by_id={3: 288834786},
            admin_state={"access_scope": "family_full", "access_source": "family", "pending_start_payload": ""},
        )

        state = await billing_client.get_access_state(conn, 5684474126)

        self.assertEqual(state["access_scope"], "personal_full")
        self.assertEqual(state["access_source"], "billing")

    async def test_manual_subscription_without_card_still_gets_personal_full(self) -> None:
        expiry = datetime(2030, 1, 1, tzinfo=UTC)
        conn = BillingFamilyAccessConn(
            finance_scope=None,
            billing_states={
                288834786: {
                    "profile_exists": False,
                    "has_card": False,
                    "subscription_status": "manual",
                    "expires_at": expiry,
                    "next_charge_at": None,
                    "grace_expires_at": None,
                    "trial_days": 0,
                }
            },
        )

        state = await billing_client.get_access_state(conn, 288834786)

        self.assertEqual(state["access_scope"], "personal_full")
        self.assertEqual(state["access_source"], "billing")
        self.assertFalse(state["billing_state"]["has_card"])
        self.assertEqual(state["billing_state"]["access_mode"], "full")

    async def test_cached_personal_full_projection_does_not_override_expired_subscription(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope=None,
            billing_states={
                5684474126: {
                    "profile_exists": True,
                    "has_card": True,
                    "masked_pan": "51687520******93",
                    "profile_status": "active",
                    "auto_renew_enabled": False,
                    "last_charge_status": "requires_action",
                    "last_action_url": "https://pay.monobank.ua/recovery",
                    "subscription_status": "expired",
                    "access_mode": "blocked",
                }
            },
            admin_state={"access_scope": "personal_full", "access_source": "billing", "pending_start_payload": ""},
        )

        state = await billing_client.get_access_state(conn, 5684474126)

        # A cached projection is not a canonical manual/lifetime grant.
        self.assertEqual(state["access_scope"], "paywall")
        self.assertEqual(state["access_source"], "billing")
        self.assertEqual(state["billing_state"]["subscription_status"], "expired")

    async def test_course_payload_sets_ninety_day_trial_offer(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope=None,
            admin_state={"access_scope": "paywall", "access_source": "promo", "pending_start_payload": "course"},
        )

        state = await billing_client.get_access_state(conn, 288834786)

        self.assertEqual(state["access_scope"], "paywall")
        self.assertEqual(state["promo_trial_days"], 90)

    async def test_treads_payload_sets_thirty_day_trial_offer(self) -> None:
        conn = BillingFamilyAccessConn(
            finance_scope=None,
            admin_state={"access_scope": "paywall", "access_source": "promo", "pending_start_payload": "treads"},
        )

        state = await billing_client.get_access_state(conn, 288834786)

        self.assertEqual(state["access_scope"], "paywall")
        self.assertEqual(state["promo_trial_days"], 30)


if __name__ == "__main__":
    unittest.main()
