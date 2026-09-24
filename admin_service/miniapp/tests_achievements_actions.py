from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from django.test import TestCase

from common.test_helpers import ensure_runtime_finance_tables, ensure_telegram_user_table
from gamification.models import ActivityContribution, GamificationDay, GamificationProfile, StreakState
from gamification.services import current_day_snapshot, record_no_expenses, update_preferences


class NoExpensesActionTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()
        ensure_runtime_finance_tables()

    def test_no_expenses_activates_today_once_for_the_same_idempotency_key(self):
        now = datetime(2026, 9, 10, 9, 0, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=2001,
            timezone_name="Europe/Kyiv",
            started_at=now,
        )
        day = current_day_snapshot(user_id=2001, now=now)

        first = record_no_expenses(
            user_id=2001,
            day_id=str(day["day_id"]),
            idempotency_key="no-expenses-2001-20260910",
            now=now,
        )
        repeated = record_no_expenses(
            user_id=2001,
            day_id=str(day["day_id"]),
            idempotency_key="no-expenses-2001-20260910",
            now=now,
        )

        self.assertFalse(first["idempotent"])
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(
            ActivityContribution.objects.filter(user_id=2001, kind="no_expenses").count(),
            1,
        )
        self.assertEqual(GamificationDay.objects.get(user_id=2001).status, GamificationDay.Status.ACTIVE)
        self.assertEqual(StreakState.objects.get(user_id=2001).lifetime_active_days, 1)

    def test_no_expenses_uses_timezone_after_due_transition(self):
        start = datetime(2030, 1, 15, 12, 0, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=2002,
            timezone_name="Europe/Kyiv",
            started_at=start,
        )
        current_day_snapshot(user_id=2002, now=start)
        profile = update_preferences(
            user_id=2002,
            mascot=None,
            motion_enabled=None,
            timezone_name="Asia/Tokyo",
            now=start,
        )
        after_transition = profile.pending_timezone_effective_at + timedelta(minutes=1)
        expected_day_id = after_transition.astimezone(ZoneInfo("Asia/Tokyo")).date().toordinal()

        result = record_no_expenses(
            user_id=2002,
            day_id=str(expected_day_id),
            idempotency_key="no-expenses-2002-after-timezone-change",
            now=after_transition,
        )

        self.assertEqual(result["day_id"], expected_day_id)
        day = GamificationDay.objects.get(user_id=2002, day_seq=expected_day_id)
        self.assertEqual(day.timezone_name, "Asia/Tokyo")