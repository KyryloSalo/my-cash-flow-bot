from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx

import config
from finance_scope import get_current_finance_scope


class BillingAPIError(Exception):
    pass


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    value = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table_name)
    return bool(value)


PERSONAL_FULL_STATUSES = {"trial", "active", "paid", "manual", "lifetime"}


def _normalize_failure_reason(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_soft_grace_reason(value: Any) -> bool:
    normalized = _normalize_failure_reason(value)
    if not normalized:
        return False
    soft_markers = (
        "insufficient",
        "not enough",
        "low balance",
        "declin",
        "do not honor",
        "недостат",
        "не вистач",
        "брак кошт",
        "коштів недостат",
        "недостач",
    )
    return any(marker in normalized for marker in soft_markers)


def _access_mode(
    *,
    subscription_status: str,
    expires_at: datetime | None,
    grace_expires_at: datetime | None,
    soft_grace_active: bool = False,
) -> str:
    now = datetime.now(UTC)
    if expires_at is None:
        if subscription_status in {"manual", "lifetime"}:
            return "full"
        return "open"
    if expires_at >= now:
        return "full"
    if soft_grace_active and grace_expires_at and grace_expires_at >= now:
        return "full"
    if grace_expires_at and grace_expires_at >= now:
        return "read_only"
    return "blocked"


def _billing_state_has_personal_full_access(billing_state: dict[str, Any]) -> bool:
    subscription_status = str(billing_state.get("subscription_status") or "")
    return (
        str(billing_state.get("access_mode") or "") == "full"
        and (subscription_status in PERSONAL_FULL_STATUSES or bool(billing_state.get("soft_grace_active")))
    )


async def get_billing_state(conn: asyncpg.Connection, tg_user_id: int) -> dict[str, Any]:
    state: dict[str, Any] = {
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

    if await _table_exists(conn, "billing_profiles"):
        profile = await conn.fetchrow(
            """
            SELECT status, masked_pan, auto_renew_enabled, last_charge_status, last_failure_reason, last_action_url,
                   CASE WHEN card_token IS NOT NULL AND card_token <> '' THEN TRUE ELSE FALSE END AS has_card
            FROM billing_profiles
            WHERE telegram_user_id=$1
            """,
            tg_user_id,
        )
        if profile:
            state.update(
                {
                    "profile_exists": True,
                    "has_card": bool(profile["has_card"]),
                    "masked_pan": str(profile["masked_pan"] or ""),
                    "profile_status": str(profile["status"] or ""),
                    "auto_renew_enabled": bool(profile["auto_renew_enabled"]),
                    "last_charge_status": str(profile["last_charge_status"] or ""),
                    "last_failure_reason": str(profile["last_failure_reason"] or ""),
                    "last_action_url": str(profile["last_action_url"] or ""),
                }
            )

    if await _table_exists(conn, "subscriptions"):
        subscription = await conn.fetchrow(
            """
            SELECT status, expires_at, next_charge_at, grace_expires_at, trial_days
            FROM subscriptions
            WHERE telegram_user_id=$1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            tg_user_id,
        )
        if subscription:
            subscription_status = str(subscription["status"] or "")
            expires_at = subscription["expires_at"]
            grace_expires_at = subscription["grace_expires_at"]
            soft_grace_active = (
                subscription_status == "expired"
                and bool(grace_expires_at)
                and _is_soft_grace_reason(state.get("last_failure_reason"))
            )
            state.update(
                {
                    "subscription_status": subscription_status,
                    "expires_at": expires_at,
                    "next_charge_at": subscription["next_charge_at"],
                    "grace_expires_at": grace_expires_at,
                    "trial_days": int(subscription["trial_days"] or 0),
                    "soft_grace_active": soft_grace_active,
                    "access_mode": _access_mode(
                        subscription_status=subscription_status,
                        expires_at=expires_at,
                        grace_expires_at=grace_expires_at,
                        soft_grace_active=soft_grace_active,
                    ),
                }
            )
    return state


async def _get_active_family_owner_user_id(conn: asyncpg.Connection, family_id: int) -> int | None:
    row = await conn.fetchrow(
        """
        SELECT user_id
        FROM family_members
        WHERE family_id=$1
          AND role='owner'
          AND status='active'
        ORDER BY joined_at ASC, id ASC
        LIMIT 1
        """,
        int(family_id),
    )
    if row is None:
        return None
    owner_user_id = row.get("user_id") if hasattr(row, "get") else row["user_id"]
    return int(owner_user_id) if owner_user_id is not None else None


async def _family_has_sponsored_full_access(conn: asyncpg.Connection, finance_scope) -> bool:
    if not finance_scope.is_family or finance_scope.family_id is None or finance_scope.is_owner:
        return False
    owner_user_id = await _get_active_family_owner_user_id(conn, int(finance_scope.family_id))
    if owner_user_id is None:
        return False
    owner_billing_state = await get_billing_state(conn, owner_user_id)
    return _billing_state_has_personal_full_access(owner_billing_state)


def _pending_payload_trial_days(pending_payload: str) -> int:
    payload = str(pending_payload or "").strip()
    if payload == "course" or payload.startswith("course_"):
        return 90
    if payload == "treads":
        return 30
    return 0


async def get_access_state(conn: asyncpg.Connection, tg_user_id: int) -> dict[str, Any]:
    billing_state = await get_billing_state(conn, tg_user_id)
    state: dict[str, Any] = {
        "access_scope": "paywall",
        "access_source": "",
        "pending_start_payload": "",
        "promo_offer_code": "",
        "promo_trial_days": 0,
        "billing_state": billing_state,
    }

    admin_state = None
    if await _table_exists(conn, "user_admin_states"):
        admin_state = await conn.fetchrow(
            """
            SELECT access_scope, access_source, pending_start_payload
            FROM user_admin_states
            WHERE telegram_user_id=$1
            """,
            tg_user_id,
        )
        if admin_state:
            state["access_source"] = str(admin_state["access_source"] or "")
            state["pending_start_payload"] = str(admin_state["pending_start_payload"] or "")

    pending_payload = str(state.get("pending_start_payload") or "")
    if pending_payload.startswith("promo_") and await _table_exists(conn, "promo_offers"):
        promo_code = pending_payload.removeprefix("promo_").strip().upper()
        promo_offer = await conn.fetchrow(
            """
            SELECT code, trial_days
            FROM promo_offers
            WHERE upper(code)=upper($1)
              AND is_active=true
              AND (starts_at IS NULL OR starts_at <= now())
              AND (ends_at IS NULL OR ends_at >= now())
            LIMIT 1
            """,
            promo_code,
        )
        if promo_offer:
            state["promo_offer_code"] = str(promo_offer["code"] or "")
            state["promo_trial_days"] = int(promo_offer["trial_days"] or 0)
    if not state["promo_trial_days"]:
        state["promo_trial_days"] = _pending_payload_trial_days(pending_payload)

    finance_scope = await get_current_finance_scope(conn, tg_user_id)
    if await _family_has_sponsored_full_access(conn, finance_scope):
        state["access_scope"] = "family_full"
        if not state["access_source"]:
            state["access_source"] = "family"
        return state
    if state["access_source"] == "family":
        state["access_source"] = ""

    personal_full = _billing_state_has_personal_full_access(billing_state)
    if personal_full:
        state["access_scope"] = "personal_full"
        if not state["access_source"]:
            state["access_source"] = "billing"
        return state

    # A cached projection has no independent expiry. Manual/lifetime grants are
    # canonical Subscription rows and are handled above, never by this fallback.
    if admin_state and str(admin_state["access_scope"] or "") == "debt_only":
        state["access_scope"] = "debt_only"
        if not state["access_source"]:
            state["access_source"] = "debt"
        return state

    if not state["access_source"]:
        state["access_source"] = "billing"
    return state


async def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not config.BILLING_INTERNAL_TOKEN:
        raise BillingAPIError("BILLING_INTERNAL_TOKEN not configured")
    headers = {"Content-Type": "application/json"}
    headers["X-Internal-Token"] = config.BILLING_INTERNAL_TOKEN
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{config.BILLING_INTERNAL_BASE_URL.rstrip('/')}{path}",
            json=payload,
            headers=headers,
        )
    data = response.json()
    if response.status_code >= 400 or not data.get("ok"):
        raise BillingAPIError(str(data.get("error") or f"HTTP {response.status_code}"))
    return data


async def init_bind_session(*, tg_user_id: int, trial_days: int, mode: str, promo_code: str = "") -> dict[str, Any]:
    return await _post_json(
        "/internal/billing/mono/init-bind",
        {"telegram_user_id": tg_user_id, "trial_days": trial_days, "mode": mode, "promo_code": promo_code},
    )


async def init_recovery_payment_session(*, tg_user_id: int) -> dict[str, Any]:
    return await _post_json(
        "/internal/billing/mono/init-recovery",
        {"telegram_user_id": tg_user_id},
    )


async def cancel_autorenew(*, tg_user_id: int) -> dict[str, Any]:
    return await _post_json(
        "/internal/billing/mono/cancel-autorenew",
        {"telegram_user_id": tg_user_id},
    )


async def retry_renewal(*, tg_user_id: int, intent_key: str) -> dict[str, Any]:
    normalized_intent = str(intent_key or "").strip()
    if not normalized_intent:
        raise BillingAPIError("Billing retry requires an intent key.")
    return await _post_json(
        "/internal/billing/mono/retry-renew",
        {"telegram_user_id": tg_user_id, "intent_key": normalized_intent},
    )


async def sync_pending_bind_status(*, tg_user_id: int) -> dict[str, Any]:
    return await _post_json(
        "/internal/billing/mono/sync-bind-status",
        {"telegram_user_id": tg_user_id},
    )
