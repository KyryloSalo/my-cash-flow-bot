from datetime import UTC, datetime, timedelta
from uuid import uuid4

from django.test import TestCase

from gamification.models import (
    GamificationDay,
    GamificationEventOutbox,
    GamificationProfile,
    GamificationTimezoneChange,
)
from gamification.services import (
    GamificationActionError,
    current_day_snapshot,
    finalize_due_profiles,
    process_event,
    update_preferences,
)


class GamificationPreferenceTests(TestCase):
    def test_due_timezone_change_is_applied_by_scheduled_finalization_without_user_traffic(self):
        effective_at = datetime(2026, 9, 10, 21, 5, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=3005,
            timezone_name="Europe/Kyiv",
            pending_timezone_name="Europe/Istanbul",
            pending_timezone_effective_at=effective_at,
            timezone_changed_at=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
            started_at=datetime(2026, 9, 10, 9, 0, tzinfo=UTC),
        )
        change = GamificationTimezoneChange.objects.create(
            user_id=3005,
            previous_timezone_name="Europe/Kyiv",
            new_timezone_name="Europe/Istanbul",
            requested_at=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
            effective_at=effective_at,
        )

        finalize_due_profiles(now=datetime(2026, 9, 10, 21, 6, tzinfo=UTC))

        profile = GamificationProfile.objects.get(user_id=3005)
        change.refresh_from_db()
        self.assertEqual(profile.timezone_name, "Europe/Istanbul")
        self.assertEqual(profile.pending_timezone_name, "")
        self.assertIsNone(profile.pending_timezone_effective_at)
        self.assertEqual(change.applied_at, datetime(2026, 9, 10, 21, 6, tzinfo=UTC))

    def test_timezone_change_is_rate_limited_to_once_per_30_days(self):
        now = datetime(2026, 9, 20, 10, 0, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=3004,
            timezone_name="Europe/Kyiv",
            timezone_changed_at=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )

        with self.assertRaises(GamificationActionError) as raised:
            update_preferences(
                user_id=3004,
                mascot=None,
                motion_enabled=None,
                timezone_name="Europe/Istanbul",
                now=now,
            )

        self.assertEqual(raised.exception.code, "timezone_rate_limited")

    def test_due_timezone_change_is_applied_before_opening_the_next_day(self):
        effective_at = datetime(2026, 9, 10, 21, 5, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=3003,
            timezone_name="Europe/Kyiv",
            pending_timezone_name="Europe/Istanbul",
            pending_timezone_effective_at=effective_at,
            timezone_changed_at=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
            started_at=datetime(2026, 9, 10, 9, 0, tzinfo=UTC),
        )
        change = GamificationTimezoneChange.objects.create(
            user_id=3003,
            previous_timezone_name="Europe/Kyiv",
            new_timezone_name="Europe/Istanbul",
            requested_at=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
            effective_at=effective_at,
        )

        current_day_snapshot(user_id=3003, now=datetime(2026, 9, 10, 21, 6, tzinfo=UTC))

        profile = GamificationProfile.objects.get(user_id=3003)
        change.refresh_from_db()
        self.assertEqual(profile.timezone_name, "Europe/Istanbul")
        self.assertEqual(profile.pending_timezone_name, "")
        self.assertIsNone(profile.pending_timezone_effective_at)
        self.assertIsNotNone(change.applied_at)

    def test_timezone_change_is_scheduled_after_current_day_and_audited(self):
        now = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=3002,
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 10, 9, 0, tzinfo=UTC),
        )

        profile = update_preferences(
            user_id=3002,
            mascot=None,
            motion_enabled=None,
            timezone_name="Europe/Istanbul",
            now=now,
        )

        self.assertEqual(profile.timezone_name, "Europe/Kyiv")
        self.assertEqual(profile.pending_timezone_name, "Europe/Istanbul")
        self.assertEqual(
            profile.pending_timezone_effective_at,
            datetime(2026, 9, 10, 21, 5, tzinfo=UTC),
        )
        change = GamificationTimezoneChange.objects.get(user_id=3002)
        self.assertEqual(change.previous_timezone_name, "Europe/Kyiv")
        self.assertEqual(change.new_timezone_name, "Europe/Istanbul")
        self.assertEqual(change.effective_at, profile.pending_timezone_effective_at)

    def test_timezone_change_uses_non_overlapping_days_of_at_least_twenty_hours(self):
        now = datetime(2030, 1, 15, 12, 0, tzinfo=UTC)
        GamificationProfile.objects.create(
            user_id=3006,
            timezone_name="Europe/Kyiv",
            started_at=now,
        )

        profile = update_preferences(
            user_id=3006,
            mascot=None,
            motion_enabled=None,
            timezone_name="Asia/Tokyo",
            now=now,
        )
        old_day = GamificationDay.objects.get(user_id=profile.user_id)
        self.assertEqual(profile.pending_timezone_effective_at, old_day.ends_at + timedelta(minutes=5))
        self.assertGreaterEqual(old_day.ends_at - old_day.starts_at, timedelta(hours=20))

        before = GamificationEventOutbox.objects.create(
            event_id=uuid4(),
            event_type="transaction.created",
            actor_user_id=profile.user_id,
            entity_type="transaction",
            entity_id="timezone-before",
            accepted_at=profile.pending_timezone_effective_at - timedelta(minutes=1),
            input_method="manual",
            payload={"transaction_type": "expense", "amount": "1.00", "flow_kind": "normal"},
        )
        after = GamificationEventOutbox.objects.create(
            event_id=uuid4(),
            event_type="transaction.created",
            actor_user_id=profile.user_id,
            entity_type="transaction",
            entity_id="timezone-after",
            accepted_at=profile.pending_timezone_effective_at + timedelta(minutes=1),
            input_method="manual",
            payload={"transaction_type": "expense", "amount": "1.00", "flow_kind": "normal"},
        )

        process_event(before.event_id)
        process_event(after.event_id)

        profile.refresh_from_db()
        days = list(GamificationDay.objects.filter(user_id=profile.user_id).order_by("starts_at"))
        self.assertEqual(profile.timezone_name, "Asia/Tokyo")
        self.assertEqual(len(days), 2)
        self.assertLessEqual(days[0].ends_at, days[1].starts_at)
        self.assertGreater(days[1].local_date, days[0].local_date)
        self.assertGreaterEqual(days[1].ends_at - days[1].starts_at, timedelta(hours=20))

    def test_mascot_and_motion_preferences_are_saved(self):
        GamificationProfile.objects.create(user_id=3001, started_at=datetime(2026, 9, 10, 9, 0, tzinfo=UTC))

        profile = update_preferences(
            user_id=3001,
            mascot="capi",
            motion_enabled=False,
            timezone_name=None,
            now=datetime(2026, 9, 10, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(profile.mascot, "capi")
        self.assertIsNotNone(profile.mascot_selected_at)
        self.assertFalse(profile.motion_enabled)
        self.assertEqual(profile.revision, 1)
