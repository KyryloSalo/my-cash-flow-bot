from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")
    asyncpg_stub.Connection = object
    asyncpg_stub.Record = object
    sys.modules["asyncpg"] = asyncpg_stub

from expense_reminder_service import (  # noqa: E402
    DAILY_EXPENSE_REMINDER_7DAY_COPY_KEY,
    DAILY_EXPENSE_REMINDER_SOFT_COPY_KEY,
    DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
    DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
    ExpenseReminderService,
)


class DummyReminderConn:
    def __init__(self) -> None:
        self.users: dict[int, dict] = {}
        self.access_scopes: dict[int, str] = {}
        self.family_memberships: dict[int, dict] = {}
        self.personal_account_counts: dict[int, int] = {}
        self.family_account_counts: dict[int, int] = {}
        self.reminder_settings: dict[int, dict] = {}
        self.day_statuses: dict[tuple[int, date], dict] = {}
        self.transactions: list[dict] = []

    async def execute(self, query: str, *args) -> None:
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO daily_expense_reminder_settings"):
            tg_user_id = int(args[0])
            self.reminder_settings.setdefault(
                tg_user_id,
                {
                    "tg_user_id": tg_user_id,
                    "mode": str(args[1]),
                    "reminder_hour": int(args[2]),
                },
            )
            return
        if normalized.startswith("UPDATE daily_expense_reminder_settings SET"):
            tg_user_id = int(args[0])
            current = self.reminder_settings.setdefault(
                tg_user_id,
                {
                    "tg_user_id": tg_user_id,
                    "mode": DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
                    "reminder_hour": DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
                },
            )
            if "mode=$2" in normalized:
                current["mode"] = str(args[1])
            if "reminder_hour=$2" in normalized:
                current["reminder_hour"] = int(args[1])
            if "mode=$2, reminder_hour=$3" in normalized:
                current["mode"] = str(args[1])
                current["reminder_hour"] = int(args[2])
            return
        if normalized.startswith("INSERT INTO daily_expense_day_statuses"):
            tg_user_id = int(args[0])
            day_value = args[1]
            key = (tg_user_id, day_value)
            current = self.day_statuses.setdefault(
                key,
                {
                    "tg_user_id": tg_user_id,
                    "day": day_value,
                    "reminder_sent_at": None,
                    "sent_copy_key": None,
                    "no_expenses_confirmed_at": None,
                    "expense_recorded_at": None,
                },
            )
            if "reminder_sent_at" in normalized:
                current["reminder_sent_at"] = True
                current["sent_copy_key"] = str(args[2])
            if "no_expenses_confirmed_at" in normalized:
                current["no_expenses_confirmed_at"] = True
            if "expense_recorded_at" in normalized:
                current["expense_recorded_at"] = True
                current["no_expenses_confirmed_at"] = None
            return

    async def fetch(self, query: str, *args):
        normalized = " ".join(query.split())
        if "FROM users u" in normalized and "JOIN user_admin_states uas" in normalized:
            reminder_day = args[0]
            min_onboarding_version = int(args[1])
            is_weekday = bool(args[2])
            current_hour = int(args[3])
            limit = int(args[4])
            rows: list[dict] = []
            for tg_user_id in sorted(self.users):
                user = self.users[tg_user_id]
                if not user.get("onboarding_completed"):
                    continue
                if int(user.get("onboarding_version") or 0) < min_onboarding_version:
                    continue
                if not user.get("start_date") or not user.get("base_currency"):
                    continue
                if self.access_scopes.get(tg_user_id) not in {"personal_full", "family_full"}:
                    continue
                settings = self.reminder_settings.get(
                    tg_user_id,
                    {
                        "mode": DEFAULT_DAILY_EXPENSE_REMINDER_MODE,
                        "reminder_hour": DEFAULT_DAILY_EXPENSE_REMINDER_HOUR,
                    },
                )
                mode = str(settings.get("mode") or DEFAULT_DAILY_EXPENSE_REMINDER_MODE)
                hour = int(settings.get("reminder_hour") or DEFAULT_DAILY_EXPENSE_REMINDER_HOUR)
                if mode == "off":
                    continue
                if mode == "weekdays" and not is_weekday:
                    continue
                if hour > current_hour:
                    continue
                status = self.day_statuses.get((tg_user_id, reminder_day), {})
                if status.get("reminder_sent_at") or status.get("no_expenses_confirmed_at") or status.get("expense_recorded_at"):
                    continue
                rows.append(
                    {
                        "tg_user_id": tg_user_id,
                        "mode": mode,
                        "reminder_hour": hour,
                        "reminder_sent_at": status.get("reminder_sent_at"),
                        "no_expenses_confirmed_at": status.get("no_expenses_confirmed_at"),
                        "expense_recorded_at": status.get("expense_recorded_at"),
                    }
                )
            return rows[:limit]
        return []

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT * FROM daily_expense_reminder_settings WHERE tg_user_id=$1"):
            tg_user_id = int(args[0])
            current = self.reminder_settings.get(tg_user_id)
            if current is None:
                return None
            return dict(current)
        if normalized.startswith("SELECT * FROM daily_expense_day_statuses WHERE tg_user_id=$1 AND day=$2"):
            return self.day_statuses.get((int(args[0]), args[1]))
        if "FROM family_members fm" in normalized and "JOIN families f" in normalized:
            membership = self.family_memberships.get(int(args[0]))
            if membership is None:
                return None
            return dict(membership)
        if "FROM transactions" in normalized and "flow_kind='normal'" in normalized and "type='expense'" in normalized:
            if "(family_id=$2 OR family_id IS NULL)" in normalized:
                tg_user_id = int(args[0])
                family_id = int(args[1])
                tx_day = args[2]
                for tx in self.transactions:
                    if (
                        int(tx.get("created_by_user_id") or tx.get("tg_user_id") or 0) == tg_user_id
                        and tx.get("family_id") in {family_id, None}
                        and tx.get("date") == tx_day
                        and tx.get("flow_kind") == "normal"
                        and tx.get("type") == "expense"
                        and tx.get("deleted_at") is None
                        and not bool(tx.get("is_deleted"))
                    ):
                        return {"exists": 1}
                return None
            tg_user_id = int(args[0])
            tx_day = args[1]
            for tx in self.transactions:
                if (
                    int(tx.get("created_by_user_id") or tx.get("tg_user_id") or 0) == tg_user_id
                    and tx.get("family_id") is None
                    and tx.get("date") == tx_day
                    and tx.get("flow_kind") == "normal"
                    and tx.get("type") == "expense"
                    and tx.get("deleted_at") is None
                    and not bool(tx.get("is_deleted"))
                ):
                    return {"exists": 1}
            return None
        return None

    async def fetchval(self, query: str, *args):
        normalized = " ".join(query.split())
        if "SELECT count(1) FROM accounts WHERE family_id=$1 AND is_active=true" in normalized:
            return self.family_account_counts.get(int(args[0]), 0)
        if "SELECT count(1) FROM accounts WHERE tg_user_id=$1 AND family_id IS NULL AND is_active=true" in normalized:
            return self.personal_account_counts.get(int(args[0]), 0)
        return None


class ExpenseReminderServiceTests(unittest.IsolatedAsyncioTestCase):
    def _build_conn(self) -> DummyReminderConn:
        conn = DummyReminderConn()
        for user_id in range(1, 13):
            conn.users[user_id] = {
                "tg_user_id": user_id,
                "onboarding_completed": True,
                "onboarding_version": 2,
                "start_date": date(2026, 1, 1),
                "base_currency": "UAH",
            }
            conn.access_scopes[user_id] = "personal_full"
            conn.personal_account_counts[user_id] = 1
        return conn

    async def test_get_or_create_settings_defaults_and_updates(self) -> None:
        conn = DummyReminderConn()
        service = ExpenseReminderService(conn)

        settings = await service.get_or_create_settings(123)
        self.assertEqual(settings["mode"], "daily")
        self.assertEqual(settings["reminder_hour"], 21)

        updated = await service.update_settings(123, mode="weekdays", reminder_hour=19)
        self.assertEqual(updated["mode"], "weekdays")
        self.assertEqual(updated["reminder_hour"], 19)

    async def test_list_due_reminders_filters_non_expense_and_access_rules(self) -> None:
        conn = self._build_conn()
        service = ExpenseReminderService(conn)
        today = date(2026, 5, 15)
        now_local = datetime(2026, 5, 15, 21, 5)

        conn.transactions.extend(
            [
                {"tg_user_id": 2, "family_id": None, "date": today, "flow_kind": "normal", "type": "income", "deleted_at": None},
                {"tg_user_id": 3, "family_id": None, "date": today, "flow_kind": "transfer", "type": "transfer", "deleted_at": None},
                {"tg_user_id": 4, "family_id": None, "date": today, "flow_kind": "debt", "type": "transfer", "deleted_at": None},
                {"tg_user_id": 5, "family_id": None, "date": today, "flow_kind": "transfer", "type": "transfer", "deleted_at": None},
                {"tg_user_id": 12, "family_id": None, "date": today, "flow_kind": "normal", "type": "expense", "deleted_at": None},
            ]
        )
        conn.users[6]["onboarding_completed"] = False
        conn.access_scopes[7] = "paywall"
        conn.reminder_settings[8] = {"tg_user_id": 8, "mode": "off", "reminder_hour": 21}
        conn.reminder_settings[9] = {"tg_user_id": 9, "mode": "weekdays", "reminder_hour": 21}
        conn.day_statuses[(10, today)] = {"reminder_sent_at": True}
        conn.day_statuses[(11, today)] = {"no_expenses_confirmed_at": True}

        due_users = await service.list_due_reminders(now_local, min_onboarding_version=2, limit=20)

        self.assertEqual([candidate.tg_user_id for candidate in due_users], [1, 2, 3, 4, 5, 9])

        weekend_due = await service.list_due_reminders(datetime(2026, 5, 16, 21, 5), min_onboarding_version=2, limit=20)
        weekend_ids = [candidate.tg_user_id for candidate in weekend_due]
        self.assertIn(1, weekend_ids)
        self.assertNotIn(8, weekend_ids)
        self.assertNotIn(9, weekend_ids)

    async def test_family_reminder_uses_users_own_expenses_across_today_scope(self) -> None:
        conn = self._build_conn()
        service = ExpenseReminderService(conn)
        today = date(2026, 5, 15)
        now_local = datetime(2026, 5, 15, 21, 5)
        conn.access_scopes[1] = "family_full"
        conn.access_scopes[2] = "family_full"
        conn.family_memberships[1] = {"family_id": 77, "role": "owner"}
        conn.family_memberships[2] = {"family_id": 77, "role": "member"}
        conn.family_account_counts[77] = 1
        conn.transactions.extend(
            [
                {
                    "tg_user_id": 1,
                    "created_by_user_id": 1,
                    "family_id": None,
                    "date": today,
                    "flow_kind": "normal",
                    "type": "expense",
                    "deleted_at": None,
                    "is_deleted": False,
                },
                {
                    "tg_user_id": 2,
                    "created_by_user_id": 99,
                    "family_id": 77,
                    "date": today,
                    "flow_kind": "normal",
                    "type": "expense",
                    "deleted_at": None,
                    "is_deleted": False,
                },
            ]
        )

        due_users = await service.list_due_reminders(now_local, min_onboarding_version=2, limit=20)
        due_ids = [candidate.tg_user_id for candidate in due_users]

        self.assertNotIn(1, due_ids)
        self.assertIn(2, due_ids)

    async def test_list_due_reminders_uses_soft_and_seven_day_copy_by_ignore_streak(self) -> None:
        conn = self._build_conn()
        service = ExpenseReminderService(conn)
        today = date(2026, 5, 14)
        now_local = datetime(2026, 5, 14, 21, 15)

        for offset in range(1, 4):
            conn.day_statuses[(1, date.fromordinal(today.toordinal() - offset))] = {"reminder_sent_at": True}
        for offset in range(1, 7):
            conn.day_statuses[(2, date.fromordinal(today.toordinal() - offset))] = {"reminder_sent_at": True}

        due_users = await service.list_due_reminders(now_local, min_onboarding_version=2, limit=10)
        by_user = {candidate.tg_user_id: candidate for candidate in due_users}

        self.assertEqual(by_user[1].copy_key, DAILY_EXPENSE_REMINDER_SOFT_COPY_KEY)
        self.assertEqual(by_user[1].ignore_streak, 4)
        self.assertEqual(by_user[2].copy_key, DAILY_EXPENSE_REMINDER_7DAY_COPY_KEY)
        self.assertEqual(by_user[2].ignore_streak, 7)
        self.assertTrue(by_user[3].copy_key.startswith("daily_expense_reminder_text_"))

    async def test_confirm_no_expenses_is_idempotent_and_blocks_existing_expense(self) -> None:
        conn = self._build_conn()
        service = ExpenseReminderService(conn)
        today = date(2026, 5, 14)
        conn.transactions.append(
            {"tg_user_id": 2, "family_id": None, "date": today, "flow_kind": "normal", "type": "expense", "deleted_at": None}
        )

        first = await service.confirm_no_expenses_day(1, today)
        second = await service.confirm_no_expenses_day(1, today)
        blocked = await service.confirm_no_expenses_day(2, today)

        self.assertEqual(first, "confirmed")
        self.assertEqual(second, "already_confirmed")
        self.assertEqual(blocked, "expense_recorded")
        self.assertTrue(conn.day_statuses[(1, today)]["no_expenses_confirmed_at"])
        self.assertTrue(conn.day_statuses[(2, today)]["expense_recorded_at"])

    async def test_mark_expense_recorded_clears_no_expenses_marker(self) -> None:
        conn = self._build_conn()
        service = ExpenseReminderService(conn)
        today = date(2026, 5, 14)

        confirmed = await service.confirm_no_expenses_day(1, today)
        await service.mark_expense_recorded(1, today)

        self.assertEqual(confirmed, "confirmed")
        self.assertTrue(conn.day_statuses[(1, today)]["expense_recorded_at"])
        self.assertIsNone(conn.day_statuses[(1, today)]["no_expenses_confirmed_at"])
