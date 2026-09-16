from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

import asyncpg

from finance_scope import FinanceScope, get_current_finance_scope

DEFAULT_DAILY_EXPENSE_REMINDER_MODE = "daily"
DEFAULT_DAILY_EXPENSE_REMINDER_HOUR = 21
DEFAULT_DAILY_EXPENSE_REMINDER_TIMEZONE = "Europe/Kyiv"
SUPPORTED_DAILY_EXPENSE_REMINDER_MODES = ("daily", "weekdays", "off")
SUPPORTED_DAILY_EXPENSE_REMINDER_HOURS = (19, 20, 21, 22)
DAILY_EXPENSE_REMINDER_SOFT_COPY_KEY = "daily_expense_reminder_text_4"
DAILY_EXPENSE_REMINDER_7DAY_COPY_KEY = "daily_expense_reminder_7day_text"


ExpenseReminderConfirmResult = Literal["confirmed", "already_confirmed", "expense_recorded"]


@dataclass(frozen=True)
class DailyExpenseReminderCandidate:
    tg_user_id: int
    day: date
    mode: str
    reminder_hour: int
    ignore_streak: int
    copy_key: str


def _normalize_reminder_mode(value: object | None) -> str:
    mode = str(value or "").strip().lower()
    if mode in SUPPORTED_DAILY_EXPENSE_REMINDER_MODES:
        return mode
    return DEFAULT_DAILY_EXPENSE_REMINDER_MODE


def _normalize_reminder_hour(value: object | None) -> int:
    try:
        hour = int(value)
    except (TypeError, ValueError):
        return DEFAULT_DAILY_EXPENSE_REMINDER_HOUR
    if hour in SUPPORTED_DAILY_EXPENSE_REMINDER_HOURS:
        return hour
    return DEFAULT_DAILY_EXPENSE_REMINDER_HOUR


class ExpenseReminderService:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_or_create_settings(self, tg_user_id: int) -> asyncpg.Record:
        await self.conn.execute(
            """
            INSERT INTO daily_expense_reminder_settings (
              tg_user_id, mode, reminder_hour, created_at, updated_at
            )
            VALUES ($1, $2, $3, now(), now())
            ON CONFLICT (tg_user_id) DO NOTHING
            """,
            tg_user_id,
            DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
            DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
        )
        return await self.conn.fetchrow(
            """
            SELECT *
            FROM daily_expense_reminder_settings
            WHERE tg_user_id=$1
            """,
            tg_user_id,
        )

    async def update_settings(self, tg_user_id: int, *, mode: object | None = None, reminder_hour: object | None = None) -> asyncpg.Record:
        await self.get_or_create_settings(tg_user_id)
        updates: list[tuple[str, object]] = []
        if mode is not None:
            updates.append(("mode", _normalize_reminder_mode(mode)))
        if reminder_hour is not None:
            updates.append(("reminder_hour", _normalize_reminder_hour(reminder_hour)))
        if not updates:
            return await self.get_or_create_settings(tg_user_id)

        assignments = ", ".join(f"{column}=${index + 2}" for index, (column, _value) in enumerate(updates))
        values = [tg_user_id, *[value for _column, value in updates]]
        await self.conn.execute(
            f"""
            UPDATE daily_expense_reminder_settings
            SET {assignments}, updated_at=now()
            WHERE tg_user_id=$1
            """,
            *values,
        )
        return await self.get_or_create_settings(tg_user_id)

    async def get_day_status(self, tg_user_id: int, day: date) -> asyncpg.Record | None:
        return await self.conn.fetchrow(
            """
            SELECT *
            FROM daily_expense_day_statuses
            WHERE tg_user_id=$1 AND day=$2
            """,
            tg_user_id,
            day,
        )

    async def mark_reminder_sent(self, tg_user_id: int, day: date, *, copy_key: str) -> None:
        await self.conn.execute(
            """
            INSERT INTO daily_expense_day_statuses (
              tg_user_id, day, reminder_sent_at, sent_copy_key, created_at, updated_at
            )
            VALUES ($1, $2, now(), $3, now(), now())
            ON CONFLICT (tg_user_id, day) DO UPDATE SET
              reminder_sent_at=COALESCE(daily_expense_day_statuses.reminder_sent_at, now()),
              sent_copy_key=EXCLUDED.sent_copy_key,
              updated_at=now()
            """,
            tg_user_id,
            day,
            str(copy_key or "").strip(),
        )

    async def confirm_no_expenses_day(self, tg_user_id: int, day: date) -> ExpenseReminderConfirmResult:
        scope = await get_current_finance_scope(self.conn, tg_user_id)
        if await self._has_normal_expense_for_day(scope, tg_user_id, day):
            await self.mark_expense_recorded(tg_user_id, day)
            return "expense_recorded"

        existing = await self.get_day_status(tg_user_id, day)
        if existing and existing.get("expense_recorded_at"):
            return "expense_recorded"
        if existing and existing.get("no_expenses_confirmed_at"):
            return "already_confirmed"

        await self.conn.execute(
            """
            INSERT INTO daily_expense_day_statuses (
              tg_user_id, day, no_expenses_confirmed_at, created_at, updated_at
            )
            VALUES ($1, $2, now(), now(), now())
            ON CONFLICT (tg_user_id, day) DO UPDATE SET
              no_expenses_confirmed_at=COALESCE(daily_expense_day_statuses.no_expenses_confirmed_at, now()),
              updated_at=now()
            """,
            tg_user_id,
            day,
        )
        return "confirmed"

    async def mark_expense_recorded(self, tg_user_id: int, day: date) -> None:
        await self.conn.execute(
            """
            INSERT INTO daily_expense_day_statuses (
              tg_user_id, day, expense_recorded_at, created_at, updated_at
            )
            VALUES ($1, $2, now(), now(), now())
            ON CONFLICT (tg_user_id, day) DO UPDATE SET
              expense_recorded_at=COALESCE(daily_expense_day_statuses.expense_recorded_at, now()),
              no_expenses_confirmed_at=NULL,
              updated_at=now()
            """,
            tg_user_id,
            day,
        )

    async def list_due_reminders(
        self,
        now_local: datetime,
        *,
        min_onboarding_version: int,
        limit: int = 50,
    ) -> list[DailyExpenseReminderCandidate]:
        reminder_day = now_local.date()
        is_weekday = now_local.weekday() < 5
        candidate_limit = max(limit * 5, 50)
        rows = await self.conn.fetch(
            """
            SELECT
              u.tg_user_id,
              COALESCE(s.mode, $6) AS mode,
              COALESCE(s.reminder_hour, $7) AS reminder_hour,
              d.reminder_sent_at,
              d.no_expenses_confirmed_at,
              d.expense_recorded_at
            FROM users u
            JOIN user_admin_states uas ON uas.telegram_user_id = u.tg_user_id
            LEFT JOIN daily_expense_reminder_settings s ON s.tg_user_id = u.tg_user_id
            LEFT JOIN daily_expense_day_statuses d
              ON d.tg_user_id = u.tg_user_id
             AND d.day = $1
            WHERE u.onboarding_completed=true
              AND u.onboarding_version >= $2
              AND u.start_date IS NOT NULL
              AND COALESCE(NULLIF(u.base_currency, ''), '') <> ''
              AND uas.access_scope IN ('personal_full', 'family_full')
              AND COALESCE(s.mode, $6) <> 'off'
              AND (COALESCE(s.mode, $6) <> 'weekdays' OR $3)
              AND COALESCE(s.reminder_hour, $7) <= $4
              AND d.reminder_sent_at IS NULL
              AND d.no_expenses_confirmed_at IS NULL
              AND d.expense_recorded_at IS NULL
            ORDER BY COALESCE(s.reminder_hour, $7) ASC, u.tg_user_id ASC
            LIMIT $5
            """,
            reminder_day,
            int(min_onboarding_version),
            is_weekday,
            int(now_local.hour),
            candidate_limit,
            DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
            DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
        )
        due: list[DailyExpenseReminderCandidate] = []
        for row in rows:
            tg_user_id = int(row["tg_user_id"])
            scope = await get_current_finance_scope(self.conn, tg_user_id)
            if not await self._scope_has_active_accounts(scope, tg_user_id):
                continue
            if await self._has_normal_expense_for_day(scope, tg_user_id, reminder_day):
                await self.mark_expense_recorded(tg_user_id, reminder_day)
                continue
            streak = await self._ignore_streak_for_next_send(scope, tg_user_id, reminder_day)
            due.append(
                DailyExpenseReminderCandidate(
                    tg_user_id=tg_user_id,
                    day=reminder_day,
                    mode=_normalize_reminder_mode(row["mode"]),
                    reminder_hour=_normalize_reminder_hour(row["reminder_hour"]),
                    ignore_streak=streak,
                    copy_key=self.pick_copy_key(tg_user_id, reminder_day, streak),
                )
            )
            if len(due) >= limit:
                break
        return due

    def pick_copy_key(self, tg_user_id: int, day: date, ignore_streak: int) -> str:
        if ignore_streak >= 7:
            return DAILY_EXPENSE_REMINDER_7DAY_COPY_KEY
        if ignore_streak >= 4:
            return DAILY_EXPENSE_REMINDER_SOFT_COPY_KEY
        index = ((int(tg_user_id) + day.toordinal()) % 5) + 1
        return f"daily_expense_reminder_text_{index}"

    async def _ignore_streak_for_next_send(self, scope: FinanceScope, tg_user_id: int, day: date) -> int:
        streak = 1
        for offset in range(1, 31):
            prev_day = day - timedelta(days=offset)
            prev_status = await self.get_day_status(tg_user_id, prev_day)
            if await self._has_normal_expense_for_day(scope, tg_user_id, prev_day):
                break
            if prev_status and prev_status.get("expense_recorded_at"):
                break
            if prev_status and prev_status.get("no_expenses_confirmed_at"):
                break
            if prev_status and prev_status.get("reminder_sent_at"):
                streak += 1
                continue
            break
        return streak

    async def _scope_has_active_accounts(self, scope: FinanceScope, tg_user_id: int) -> bool:
        if scope.is_family:
            count = await self.conn.fetchval(
                "SELECT count(1) FROM accounts WHERE family_id=$1 AND is_active=true",
                int(scope.family_id),
            )
        else:
            count = await self.conn.fetchval(
                "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true",
                tg_user_id,
            )
        try:
            return int(count or 0) > 0
        except (TypeError, ValueError):
            return False

    async def _has_normal_expense_for_day(self, scope: FinanceScope, tg_user_id: int, day: date) -> bool:
        if scope.is_family:
            row = await self.conn.fetchrow(
                """
                SELECT 1
                FROM transactions
                WHERE COALESCE(created_by_user_id, tg_user_id)=$1
                  AND (family_id=$2 OR family_id IS NULL)
                  AND date=$3
                  AND flow_kind='normal'
                  AND type='expense'
                  AND deleted_at IS NULL
                  AND COALESCE(is_deleted, false)=false
                LIMIT 1
                """,
                tg_user_id,
                int(scope.family_id),
                day,
            )
        else:
            row = await self.conn.fetchrow(
                """
                SELECT 1
                FROM transactions
                WHERE COALESCE(created_by_user_id, tg_user_id)=$1
                  AND family_id IS NULL
                  AND date=$2
                  AND flow_kind='normal'
                  AND type='expense'
                  AND deleted_at IS NULL
                  AND COALESCE(is_deleted, false)=false
                LIMIT 1
                """,
                tg_user_id,
                day,
            )
        return row is not None
