from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
import json
import logging
from typing import Any

import asyncpg

import config


logger = logging.getLogger("mcf.admin")
_KNOWN_TABLES: set[str] = set()
_ONBOARDING_EVENT_TYPES = {
    "onboarding_started",
    "onboarding_step_completed",
    "onboarding_completed",
    "onboarding_failed",
    "onboarding_reset_by_admin",
}


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    if table_name in _KNOWN_TABLES:
        return True
    fetchval = getattr(conn, "fetchval", None)
    if fetchval is None:
        return False
    exists = await fetchval("SELECT to_regclass($1) IS NOT NULL", table_name)
    if exists:
        _KNOWN_TABLES.add(table_name)
    return bool(exists)


async def is_user_banned(conn: asyncpg.Connection, tg_user_id: int) -> bool:
    if not await _table_exists(conn, "user_admin_states"):
        return False
    status = await conn.fetchval(
        "SELECT status FROM user_admin_states WHERE telegram_user_id=$1",
        tg_user_id,
    )
    return str(status or "").strip().lower() == "banned"


async def log_bot_event(
    conn: asyncpg.Connection,
    user_id: int | None,
    event_type: str,
    *,
    source: str = "",
    raw_input: str = "",
    parsed_result: dict | None = None,
    success: bool = True,
    error_message: str = "",
) -> None:
    if not await _table_exists(conn, "bot_events"):
        return

    payload = json.dumps(parsed_result or {}, ensure_ascii=False)
    try:
        await conn.execute(
            """
            INSERT INTO bot_events (
              telegram_user_id, event_type, source, raw_input, parsed_result, success, error_message, created_at
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, now())
            """,
            user_id,
            event_type,
            source[:64],
            raw_input[:4000],
            payload,
            success,
            error_message[:2000],
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("log_bot_event failed for %s/%s: %s", user_id, event_type, exc)

    if user_id and await _table_exists(conn, "user_admin_states"):
        try:
            await conn.execute(
                """
                INSERT INTO user_admin_states (
                  telegram_user_id,
                  status,
                  subscription_status,
                  access_scope,
                  access_source,
                  source,
                  referral_code,
                  pending_start_payload,
                  timezone,
                  last_action_at,
                  is_blocked,
                  can_receive_messages,
                  blocked_bot,
                  admin_comment,
                  is_test_user,
                  test_user_notes,
                  current_fsm_state,
                  onboarding_payload,
                  last_user_input,
                  last_bot_response,
                  last_parse_error,
                  pending_admin_reset_mode,
                  created_at,
                  updated_at
                )
                VALUES (
                  $1,
                  'active',
                  'none',
                  'paywall',
                  '',
                  '',
                  '',
                  '',
                  '',
                  now(),
                  false,
                  true,
                  false,
                  '',
                  false,
                  '',
                  '',
                  '{}'::jsonb,
                  '',
                  '',
                  '',
                  '',
                  now(),
                  now()
                )
                ON CONFLICT (telegram_user_id) DO UPDATE SET
                  last_action_at = now(),
                  updated_at = now()
                """,
                user_id,
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("user_admin_states sync failed for %s: %s", user_id, exc)
        else:
            try:
                updates: list[str] = ["last_action_at = now()", "updated_at = now()"]
                values: list[Any] = []
                param_index = 2
                if event_type == "text_received":
                    updates.append(f"last_user_input = ${param_index}")
                    values.append(raw_input[:4000])
                    param_index += 1
                if event_type == "parse_error" and error_message:
                    updates.append(f"last_parse_error = ${param_index}")
                    values.append(error_message[:4000])
                    param_index += 1
                if event_type in _ONBOARDING_EVENT_TYPES:
                    updates.append("last_onboarding_event_at = now()")
                if event_type == "onboarding_completed":
                    updates.append("current_fsm_state = ''")
                    updates.append("onboarding_payload = '{}'::jsonb")
                await conn.execute(
                    f"""
                    UPDATE user_admin_states
                    SET {", ".join(updates)}
                    WHERE telegram_user_id = $1
                    """,
                    user_id,
                    *values,
                )
            except Exception as exc:  # pragma: no cover
                logger.warning("user_admin_states debug sync failed for %s: %s", user_id, exc)


async def sync_onboarding_debug_state(
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    current_fsm_state: str = "",
    onboarding_payload: dict | None = None,
    last_user_input: str | None = None,
    last_bot_response: str | None = None,
    last_parse_error: str | None = None,
    last_event_type: str = "",
) -> None:
    if not await _table_exists(conn, "user_admin_states"):
        return
    try:
        state, payload = current_fsm_state[:255], json.dumps(onboarding_payload or {}, ensure_ascii=False)
        await conn.execute(
            """
            INSERT INTO user_admin_states (
              telegram_user_id,
              status,
              subscription_status,
              access_scope,
              access_source,
              source,
              referral_code,
              pending_start_payload,
              timezone,
              last_action_at,
              is_blocked,
              can_receive_messages,
              blocked_bot,
              admin_comment,
              is_test_user,
              test_user_notes,
              current_fsm_state,
              onboarding_payload,
              last_user_input,
              last_bot_response,
              last_parse_error,
              last_onboarding_event_at,
              pending_admin_reset_mode,
              created_at,
              updated_at
            )
            VALUES (
              $1,
              'active',
              'none',
              'paywall',
              '',
              '',
              '',
              '',
              '',
              now(),
              false,
              true,
              false,
              '',
              false,
              '',
              $2,
              $3::jsonb,
              $4,
              $5,
              $6,
              now(),
              '',
              now(),
              now()
            )
            ON CONFLICT (telegram_user_id) DO UPDATE SET
              current_fsm_state = EXCLUDED.current_fsm_state,
              onboarding_payload = EXCLUDED.onboarding_payload,
              last_user_input = COALESCE(NULLIF(EXCLUDED.last_user_input, ''), user_admin_states.last_user_input),
              last_bot_response = COALESCE(NULLIF(EXCLUDED.last_bot_response, ''), user_admin_states.last_bot_response),
              last_parse_error = COALESCE(NULLIF(EXCLUDED.last_parse_error, ''), user_admin_states.last_parse_error),
              last_onboarding_event_at = now(),
              last_action_at = now(),
              updated_at = now()
            """,
            tg_user_id,
            state,
            payload,
            (last_user_input or "")[:4000],
            (last_bot_response or "")[:4000],
            (last_parse_error or "")[:4000],
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("sync_onboarding_debug_state failed for %s: %s", tg_user_id, exc)


async def get_pending_admin_reset(conn: asyncpg.Connection, tg_user_id: int) -> dict[str, str] | None:
    if not await _table_exists(conn, "user_admin_states"):
        return None
    row = await conn.fetchrow(
        """
        SELECT pending_admin_reset_mode, pending_admin_reset_requested_at
        FROM user_admin_states
        WHERE telegram_user_id = $1
        """,
        tg_user_id,
    )
    if not row or not row["pending_admin_reset_mode"]:
        return None
    return {
        "mode": str(row["pending_admin_reset_mode"]),
        "requested_at": str(row["pending_admin_reset_requested_at"] or ""),
    }


async def consume_pending_admin_reset(
    conn: asyncpg.Connection,
    tg_user_id: int,
    *,
    mode: str,
) -> None:
    if not await _table_exists(conn, "user_admin_states"):
        return
    try:
        await conn.execute(
            """
            UPDATE user_admin_states
            SET pending_admin_reset_mode = '',
                pending_admin_reset_requested_at = NULL,
                current_fsm_state = '',
                onboarding_payload = '{}'::jsonb,
                updated_at = now()
            WHERE telegram_user_id = $1
            """,
            tg_user_id,
        )
        await log_bot_event(
            conn,
            tg_user_id,
            "onboarding_reset_by_admin",
            source="bot_runtime",
            parsed_result={"mode": mode, "consumed": True},
            success=True,
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("consume_pending_admin_reset failed for %s: %s", tg_user_id, exc)


async def get_bot_setting(conn: asyncpg.Connection, key: str, default: Any = None) -> Any:
    if not await _table_exists(conn, "bot_settings"):
        return default
    value = await conn.fetchrow("SELECT value, value_type FROM bot_settings WHERE key=$1", key)
    if not value:
        return default
    raw = value.get("value")
    value_type = (value.get("value_type") or "string").strip().lower()
    if value_type == "bool":
        return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}
    if value_type == "int":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return default
    if value_type == "json":
        try:
            return json.loads(raw or "{}")
        except json.JSONDecodeError:
            return default
    return raw if raw not in {None, ""} else default


async def should_block_for_maintenance(conn: asyncpg.Connection, tg_user_id: int) -> str | None:
    if tg_user_id in config.ADMIN_TELEGRAM_IDS:
        return None
    enabled = bool(await get_bot_setting(conn, "maintenance_mode", False))
    if not enabled:
        return None
    return str(await get_bot_setting(conn, "maintenance_message", "Бот тимчасово на технічних роботах. Спробуй пізніше."))


async def is_registration_open(conn: asyncpg.Connection) -> bool:
    return bool(await get_bot_setting(conn, "registration_enabled", True))


async def create_support_case(
    conn: asyncpg.Connection,
    tg_user_id: int,
    text: str,
    *,
    category: str = "other",
    subject: str | None = None,
) -> int:
    if not await _table_exists(conn, "support_cases") or not await _table_exists(conn, "support_messages"):
        raise RuntimeError("support tables are missing")
    resolved_subject = (subject if subject is not None else text or "").strip()[:120]
    case_id = await conn.fetchval(
        """
        INSERT INTO support_cases (
          telegram_user_id, subject, status, category, priority, internal_notes, created_at, updated_at
        )
        VALUES ($1, $2, 'new', $3, 'normal', '', now(), now())
        RETURNING id
        """,
        tg_user_id,
        resolved_subject,
        category,
    )
    await conn.execute(
        """
        INSERT INTO support_messages (
          case_id, sender_type, text, created_at
        )
        VALUES ($1, 'user', $2, now())
        """,
        case_id,
        text[:4000],
    )
    return int(case_id)


async def log_admin_notification(conn: asyncpg.Connection, event_type: str, payload: dict | None = None) -> None:
    if not await _table_exists(conn, "admin_notification_logs"):
        return
    for admin_id in config.ADMIN_TELEGRAM_IDS:
        await conn.execute(
            """
            INSERT INTO admin_notification_logs (event_type, telegram_chat_id, payload, status, created_at)
            VALUES ($1, $2, $3::jsonb, 'queued', now())
            """,
            event_type,
            admin_id,
            json.dumps(payload or {}, ensure_ascii=False),
        )


async def save_poll_response(
    conn: asyncpg.Connection,
    *,
    campaign_id: int,
    tg_user_id: int,
    answer: str = "",
    rating_value: int | None = None,
    text_answer: str = "",
) -> None:
    if not await _table_exists(conn, "poll_responses"):
        raise RuntimeError("poll tables are missing")
    await conn.execute(
        """
        INSERT INTO poll_responses (campaign_id, telegram_user_id, answer, rating_value, text_answer, created_at)
        VALUES ($1, $2, $3, $4, $5, now())
        ON CONFLICT (campaign_id, telegram_user_id) DO UPDATE SET
          answer = EXCLUDED.answer,
          rating_value = EXCLUDED.rating_value,
          text_answer = EXCLUDED.text_answer
        """,
        campaign_id,
        tg_user_id,
        answer[:2000],
        rating_value,
        text_answer[:4000],
    )
    if await _table_exists(conn, "poll_recipients"):
        await conn.execute(
            """
            UPDATE poll_recipients
            SET status='responded', responded_at=now()
            WHERE campaign_id=$1 AND telegram_user_id=$2
            """,
            campaign_id,
            tg_user_id,
        )
    if await _table_exists(conn, "poll_campaigns"):
        await conn.execute(
            """
            UPDATE poll_campaigns
            SET response_count = (
              SELECT count(1) FROM poll_responses WHERE campaign_id=$1
            ),
                updated_at = now()
            WHERE id=$1
            """,
            campaign_id,
        )


async def create_feedback_from_poll(
    conn: asyncpg.Connection,
    *,
    tg_user_id: int,
    category: str,
    text: str,
    rating: int | None,
) -> None:
    if not await _table_exists(conn, "feedback_items"):
        return
    await conn.execute(
        """
        INSERT INTO feedback_items (telegram_user_id, source, rating, text, category, status, created_at, updated_at)
        VALUES ($1, 'poll', $2, $3, $4, 'new', now(), now())
        """,
        tg_user_id,
        rating,
        text[:4000],
        category,
    )


async def notify_admins(
    bot,
    text: str,
    *,
    exclude_chat_ids: set[int] | None = None,
    chat_ids: Iterable[int] | None = None,
    **message_kwargs: Any,
) -> None:
    if not config.ADMIN_NOTIFICATIONS_ENABLED:
        return
    configured_ids = config.ADMIN_TELEGRAM_IDS if chat_ids is None else chat_ids
    recipient_ids = list(dict.fromkeys(int(chat_id) for chat_id in configured_ids))
    if not recipient_ids:
        return
    excluded = {int(chat_id) for chat_id in (exclude_chat_ids or set())}
    for admin_id in recipient_ids:
        if int(admin_id) in excluded:
            continue
        try:
            await bot.send_message(chat_id=admin_id, text=text, **message_kwargs)
        except Exception as exc:  # pragma: no cover
            logger.warning("Admin notification failed for %s: %s", admin_id, exc)


def format_new_user_notification(*, tg_user_id: int, username: str | None, first_name: str | None) -> str:
    username_text = f"@{username}" if username else "-"
    return (
        "Новий користувач\n\n"
        f"telegram_id: {tg_user_id}\n"
        f"username: {username_text}\n"
        f"name: {first_name or '-'}\n"
        f"time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


def format_onboarding_notification(*, tg_user_id: int, username: str | None, first_name: str | None) -> str:
    username_text = f"@{username}" if username else "-"
    return (
        "Онбординг завершено\n\n"
        f"telegram_id: {tg_user_id}\n"
        f"username: {username_text}\n"
        f"name: {first_name or '-'}\n"
        f"time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )


def format_error_notification(*, event_type: str, tg_user_id: int | None, error_message: str) -> str:
    return (
        "Помилка бота\n\n"
        f"event_type: {event_type}\n"
        f"telegram_id: {tg_user_id or '-'}\n"
        f"error: {error_message[:300] or '-'}\n"
        f"time: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
