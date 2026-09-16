from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import asyncpg

from admin_integrations import create_support_case


REASON_LABELS = {
    "uk": {
        "card": "Не хочу прив’язувати картку",
        "autorenew": "Боюся автосписання",
        "value": "Не бачу користі",
        "price": "499 грн потім дорого",
        "error": "Сталася помилка",
        "later": "Просто не на часі",
        "other": "Інше",
    },
    "en": {
        "card": "I don’t want to link a card",
        "autorenew": "I’m worried about auto-renewal",
        "value": "I don’t see the value",
        "price": "UAH 499 later is too much",
        "error": "Something went wrong",
        "later": "It’s not the right time",
        "other": "Other",
    },
}
VALID_REASONS = frozenset(REASON_LABELS["uk"])
TERMINAL_STATUSES = frozenset({"converted", "opted_out", "ineligible"})

OWNER_EVENT_LABELS = {
    "reason": ("🟠 Нова відповідь на recovery", "Відповів на recovery"),
    "details": ("💬 Нове уточнення до recovery", "Додав текстове уточнення"),
    "contact": ("🔴 Потрібна особиста відповідь", "Попросив особистий контакт"),
    "opt_out": ("⚪️ Відмова від recovery", "Попросив більше не писати про trial"),
}

RECOVERY_STATUS_LABELS = {
    "scheduled": "заплановано",
    "responded": "відповів",
    "contact_requested": "просить контакт",
    "converted": "активувався",
    "opted_out": "відмовився від recovery",
    "ineligible": "не відповідає умовам",
    "completed": "кампанію завершено",
    "delivery_failed": "помилка доставки",
}

SUBSCRIPTION_STATUS_LABELS = {
    "none": "немає",
    "trial": "trial",
    "active": "активна",
    "paid": "оплачена",
    "expired": "закінчилась",
    "cancelled": "скасована",
    "manual": "ручний доступ",
    "lifetime": "довічний доступ",
}

ACCESS_SCOPE_LABELS = {
    "personal_full": "повний особистий",
    "family_full": "повний сімейний",
    "debt_only": "лише борги",
    "paywall": "paywall",
}


def reason_label(reason: str, lang: str = "uk") -> str:
    locale = "en" if str(lang or "").lower().startswith("en") else "uk"
    return REASON_LABELS[locale].get(reason, reason or "—")


def _feedback_category(reason: str) -> str:
    if reason in {"card", "autorenew", "price"}:
        return "payment"
    if reason == "error":
        return "bug"
    if reason == "value":
        return "ux"
    return "other"


async def _locked_recipient(conn: asyncpg.Connection, recipient_id: int, tg_user_id: int):
    return await conn.fetchrow(
        """
        SELECT id, status, reason, free_text, feedback_item_id, support_case_id,
               responded_at, awaiting_text_until, contact_requested_at, personal_contact_declined_at, opted_out_at, converted_at
        FROM trial_recovery_recipients
        WHERE id=$1 AND telegram_user_id=$2
        FOR UPDATE
        """,
        recipient_id,
        tg_user_id,
    )


async def _upsert_feedback(
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
    tg_user_id: int,
    reason: str,
    free_text: str = "",
    feedback_item_id: int | None = None,
) -> int:
    label = reason_label(reason, "uk")
    text = f"Trial recovery reason: {label}"
    if free_text:
        text += f"\n\nComment: {free_text.strip()[:4000]}"
    category = _feedback_category(reason)
    if feedback_item_id:
        await conn.execute(
            """
            UPDATE feedback_items
            SET text=$2, category=$3, updated_at=now()
            WHERE id=$1
            """,
            feedback_item_id,
            text,
            category,
        )
        return int(feedback_item_id)
    feedback_id = await conn.fetchval(
        """
        INSERT INTO feedback_items
          (telegram_user_id, source, rating, text, category, status, created_at, updated_at)
        VALUES ($1, 'subscription', NULL, $2, $3, 'new', now(), now())
        RETURNING id
        """,
        tg_user_id,
        text,
        category,
    )
    await conn.execute(
        "UPDATE trial_recovery_recipients SET feedback_item_id=$2, updated_at=now() WHERE id=$1",
        recipient_id,
        feedback_id,
    )
    return int(feedback_id)


async def record_reason(
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
    tg_user_id: int,
    reason: str,
) -> dict[str, Any] | None:
    if reason not in VALID_REASONS:
        return None
    async with conn.transaction():
        row = await _locked_recipient(conn, recipient_id, tg_user_id)
        if row is None or row["status"] in TERMINAL_STATUSES or row["status"] == "contact_requested":
            return None
        selected_reason = str(row["reason"] or reason)
        is_new_response = row["responded_at"] is None
        feedback_id = await _upsert_feedback(
            conn,
            recipient_id=recipient_id,
            tg_user_id=tg_user_id,
            reason=selected_reason,
            free_text=str(row["free_text"] or ""),
            feedback_item_id=row["feedback_item_id"],
        )
        await conn.execute(
            """
            UPDATE trial_recovery_recipients
            SET status='responded', reason=$2, responded_at=COALESCE(responded_at, now()),
                next_send_at=NULL, awaiting_text_until=NULL, feedback_item_id=$3, updated_at=now()
            WHERE id=$1
            """,
            recipient_id,
            selected_reason,
            feedback_id,
        )
        return {
            "id": recipient_id,
            "reason": selected_reason,
            "feedback_item_id": feedback_id,
            "notification_required": is_new_response,
        }


async def begin_free_text(
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
    tg_user_id: int,
) -> bool:
    async with conn.transaction():
        row = await _locked_recipient(conn, recipient_id, tg_user_id)
        if row is None or row["status"] in TERMINAL_STATUSES or row["opted_out_at"]:
            return False
        await conn.execute(
            """
            UPDATE trial_recovery_recipients
            SET status=CASE WHEN contact_requested_at IS NOT NULL THEN 'contact_requested' ELSE 'responded' END,
                reason=CASE WHEN reason='' THEN 'other' ELSE reason END,
                responded_at=COALESCE(responded_at, now()), next_send_at=NULL,
                awaiting_text_until=now() + interval '24 hours', updated_at=now()
            WHERE id=$1
            """,
            recipient_id,
        )
    return True


async def consume_free_text(
    conn: asyncpg.Connection,
    *,
    tg_user_id: int,
    text: str,
) -> dict[str, Any] | None:
    clean_text = str(text or "").strip()[:4000]
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT id, status, reason, free_text, feedback_item_id, support_case_id,
                   contact_requested_at, awaiting_text_until
            FROM trial_recovery_recipients
            WHERE telegram_user_id=$1 AND awaiting_text_until IS NOT NULL
            FOR UPDATE
            """,
            tg_user_id,
        )
        if row is None:
            return None
        if row["awaiting_text_until"] is None or row["awaiting_text_until"] <= datetime.now(timezone.utc):
            await conn.execute(
                "UPDATE trial_recovery_recipients SET awaiting_text_until=NULL, updated_at=now() WHERE id=$1",
                row["id"],
            )
            return None
        reason = str(row["reason"] or "other")
        feedback_id = await _upsert_feedback(
            conn,
            recipient_id=int(row["id"]),
            tg_user_id=tg_user_id,
            reason=reason,
            free_text=clean_text,
            feedback_item_id=row["feedback_item_id"],
        )
        await conn.execute(
            """
            UPDATE trial_recovery_recipients
            SET status=CASE WHEN contact_requested_at IS NOT NULL THEN 'contact_requested' ELSE 'responded' END,
                reason=$2, free_text=$3, responded_at=COALESCE(responded_at, now()),
                awaiting_text_until=NULL, next_send_at=NULL, feedback_item_id=$4, updated_at=now()
            WHERE id=$1
            """,
            row["id"],
            reason,
            clean_text,
            feedback_id,
        )
        return {
            "id": int(row["id"]),
            "reason": reason,
            "free_text": clean_text,
            "feedback_item_id": feedback_id,
            "support_case_id": row["support_case_id"],
        }


async def request_contact(
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
    tg_user_id: int,
) -> dict[str, Any] | None:
    async with conn.transaction():
        row = await _locked_recipient(conn, recipient_id, tg_user_id)
        if row is None or row["status"] in TERMINAL_STATUSES or row["opted_out_at"]:
            return None
        if row["support_case_id"]:
            case_id = int(row["support_case_id"])
            created = False
        else:
            reason = str(row["reason"] or "other")
            free_text = str(row["free_text"] or "")
            case_text = f"Trial recovery contact request\nReason: {reason_label(reason, 'uk')}"
            if free_text:
                case_text += f"\nComment: {free_text}"
            case_id = await create_support_case(
                conn,
                tg_user_id,
                case_text,
                category="subscription",
                subject="Trial recovery: personal contact",
            )
            created = True
        await conn.execute(
            """
            UPDATE trial_recovery_recipients
            SET status='contact_requested', contact_requested_at=now(), personal_contact_declined_at=NULL,
                support_case_id=$2, awaiting_text_until=NULL, next_send_at=NULL, updated_at=now()
            WHERE id=$1
            """,
            recipient_id,
            case_id,
        )
        if row["feedback_item_id"]:
            await conn.execute(
                """
                UPDATE feedback_items
                SET support_case_id=$2, status='converted_to_case', updated_at=now()
                WHERE id=$1
                """,
                row["feedback_item_id"],
                case_id,
            )
        return {
            "id": recipient_id,
            "support_case_id": case_id,
            "created": created,
            "reason": str(row["reason"] or "other"),
            "free_text": str(row["free_text"] or ""),
        }


async def decline_contact(conn: asyncpg.Connection, *, recipient_id: int, tg_user_id: int) -> bool:
    result = await conn.execute(
        """
        UPDATE trial_recovery_recipients
        SET status=CASE WHEN status IN ('converted','opted_out','ineligible') THEN status ELSE 'responded' END,
            responded_at=COALESCE(responded_at, now()), personal_contact_declined_at=now(),
            awaiting_text_until=NULL, next_send_at=NULL, updated_at=now()
        WHERE id=$1 AND telegram_user_id=$2
        """,
        recipient_id,
        tg_user_id,
    )
    return result != "UPDATE 0"


async def opt_out(conn: asyncpg.Connection, *, recipient_id: int, tg_user_id: int) -> bool:
    result = await conn.execute(
        """
        UPDATE trial_recovery_recipients
        SET status='opted_out', opted_out_at=COALESCE(opted_out_at, now()),
            awaiting_text_until=NULL, next_send_at=NULL, updated_at=now()
        WHERE id=$1 AND telegram_user_id=$2 AND opted_out_at IS NULL
        """,
        recipient_id,
        tg_user_id,
    )
    return result != "UPDATE 0"


async def get_recipient_owner_snapshot(
    conn: asyncpg.Connection,
    *,
    recipient_id: int,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT r.id AS recipient_id,
               r.campaign_id,
               r.status AS recovery_status,
               r.source AS recovery_source,
               r.reason,
               r.free_text,
               r.timezone AS recovery_timezone,
               r.first_sent_at,
               r.last_sent_at,
               r.sent_count,
               r.responded_at,
               r.contact_requested_at,
               r.personal_contact_declined_at,
               r.opted_out_at,
               r.converted_at,
               r.support_case_id,
               u.tg_user_id,
               u.first_name,
               u.last_name,
               u.username,
               u.lang,
               u.base_currency,
               u.onboarding_completed,
               u.onboarding_version,
               u.created_at AS user_created_at,
               u.last_seen_at,
               uas.status AS user_status,
               uas.subscription_status,
               uas.access_scope,
               uas.access_source,
               uas.is_blocked,
               uas.can_receive_messages,
               uas.blocked_bot
        FROM trial_recovery_recipients r
        JOIN users u ON u.tg_user_id = r.telegram_user_id
        LEFT JOIN user_admin_states uas ON uas.telegram_user_id = u.tg_user_id
        WHERE r.id=$1
        """,
        recipient_id,
    )
    return dict(row) if row is not None else None


def owner_reply_allowed(snapshot: dict[str, Any] | None) -> bool:
    """Independent suppression facts; call on a fresh snapshot before send."""
    return bool(snapshot and not snapshot.get("opted_out_at")
        and not snapshot.get("personal_contact_declined_at")
        and snapshot.get("recovery_status") != "opted_out"
        and snapshot.get("user_status") != "banned" and not snapshot.get("is_blocked")
        and not snapshot.get("blocked_bot") and snapshot.get("can_receive_messages") is True)


def _format_owner_timestamp(value: Any, timezone_name: str) -> str:
    if not isinstance(value, datetime):
        return "—"
    try:
        target_zone = ZoneInfo(str(timezone_name or "Europe/Istanbul"))
    except ZoneInfoNotFoundError:
        target_zone = timezone(timedelta(hours=3), name="UTC+03")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(target_zone).strftime("%d.%m.%Y %H:%M")


def _owner_value(value: Any, fallback: str = "—") -> str:
    normalized = str(value or "").strip()
    return normalized or fallback


def format_owner_notification(
    snapshot: dict[str, Any],
    *,
    event: str,
    reason: str | None = None,
    free_text: str | None = None,
    case_id: int | None = None,
    timezone_name: str = "Europe/Istanbul",
    allow_reply: bool = True,
) -> str:
    allow_reply = allow_reply and owner_reply_allowed(snapshot)
    title, event_label = OWNER_EVENT_LABELS.get(event, ("🔔 Recovery-сигнал", event or "Відповідь"))
    tg_user_id = int(snapshot.get("tg_user_id") or 0)
    name_parts = [
        _owner_value(snapshot.get("first_name"), ""),
        _owner_value(snapshot.get("last_name"), ""),
    ]
    full_name = " ".join(part for part in name_parts if part) or str(tg_user_id)
    username = _owner_value(snapshot.get("username"), "")
    username_label = f"@{username}" if username else "—"
    selected_reason = str(reason if reason is not None else snapshot.get("reason") or "")
    comment = str(free_text if free_text is not None else snapshot.get("free_text") or "").strip()
    comment = comment[:1200] or "—"
    recovery_status = RECOVERY_STATUS_LABELS.get(
        str(snapshot.get("recovery_status") or ""),
        _owner_value(snapshot.get("recovery_status")),
    )
    subscription_status = SUBSCRIPTION_STATUS_LABELS.get(
        str(snapshot.get("subscription_status") or "none"),
        _owner_value(snapshot.get("subscription_status"), "немає"),
    )
    access_scope = ACCESS_SCOPE_LABELS.get(
        str(snapshot.get("access_scope") or "paywall"),
        _owner_value(snapshot.get("access_scope"), "paywall"),
    )
    onboarding_label = (
        f"завершено (v{int(snapshot.get('onboarding_version') or 0)})"
        if snapshot.get("onboarding_completed")
        else f"не завершено (v{int(snapshot.get('onboarding_version') or 0)})"
    )
    delivery_label = "можна писати"
    if snapshot.get("blocked_bot"):
        delivery_label = "бот заблокований користувачем"
    elif snapshot.get("can_receive_messages") is False:
        delivery_label = "доставка вимкнена"
    response_at = (
        snapshot.get("contact_requested_at")
        if event == "contact"
        else snapshot.get("opted_out_at")
        if event == "opt_out"
        else snapshot.get("responded_at")
    )

    lines = [
        f"<b>{escape(title)}</b>",
        "",
        f"<b>Подія:</b> {escape(event_label)}",
        f"<b>Причина:</b> {escape(reason_label(selected_reason, 'uk') if selected_reason else '—')}",
        f"<b>Коментар:</b> {escape(comment)}",
    ]
    resolved_case_id = case_id or snapshot.get("support_case_id")
    if resolved_case_id:
        lines.append(f"<b>Звернення:</b> №{int(resolved_case_id)}")
    lines.extend(
        [
            "",
            "<b>Користувач</b>",
            f'<b>Ім’я:</b> <a href="tg://user?id={tg_user_id}">{escape(full_name)}</a>',
            f"<b>Username:</b> {escape(username_label)}",
            f"<b>Telegram ID:</b> <code>{tg_user_id}</code>",
            f"<b>Мова / валюта:</b> {escape(_owner_value(snapshot.get('lang'), '—'))} / {escape(_owner_value(snapshot.get('base_currency'), '—'))}",
            f"<b>Онбординг:</b> {escape(onboarding_label)}",
            f"<b>Стан профілю:</b> {escape(_owner_value(snapshot.get('user_status'), 'active'))}",
            f"<b>Підписка / доступ:</b> {escape(subscription_status)} / {escape(access_scope)}",
            f"<b>Джерело доступу:</b> {escape(_owner_value(snapshot.get('access_source')))}",
            f"<b>Доставка:</b> {escape(delivery_label)}",
            "",
            "<b>Recovery</b>",
            f"<b>Кампанія / отримувач:</b> #{int(snapshot.get('campaign_id') or 0)} / #{int(snapshot.get('recipient_id') or 0)}",
            f"<b>Джерело:</b> {escape(_owner_value(snapshot.get('recovery_source')))}",
            f"<b>Статус:</b> {escape(recovery_status)}",
            f"<b>Надіслано повідомлень:</b> {int(snapshot.get('sent_count') or 0)}",
            f"<b>Перший recovery:</b> {_format_owner_timestamp(snapshot.get('first_sent_at'), timezone_name)}",
            f"<b>Останній recovery:</b> {_format_owner_timestamp(snapshot.get('last_sent_at'), timezone_name)}",
            f"<b>Час цієї події:</b> {_format_owner_timestamp(response_at, timezone_name)}",
            f"<b>Реєстрація:</b> {_format_owner_timestamp(snapshot.get('user_created_at'), timezone_name)}",
            f"<b>Остання активність:</b> {_format_owner_timestamp(snapshot.get('last_seen_at'), timezone_name)}",
        ]
    )
    lines.extend(
        [
            "",
            "Натисніть «Відповісти», напишіть текст — і бот одразу доставить його цьому користувачу."
            if allow_reply
            else "Відповідь вимкнена: користувач відмовився від recovery або не може отримувати повідомлення.",
        ]
    )
    return "\n".join(lines)
