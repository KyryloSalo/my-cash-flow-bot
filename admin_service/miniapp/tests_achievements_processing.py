from datetime import UTC, datetime
from uuid import UUID

from django.test import TestCase, override_settings

from gamification.models import (
    AchievementGrant,
    AchievementProgress,
    ActivityContribution,
    GamificationDay,
    GamificationEventOutbox,
    GamificationNotification,
    GamificationProfile,
    ProcessedGamificationEvent,
    ShieldLedger,
    StreakState,
)
from gamification.services import finalize_due_profiles, finalize_user_days, process_event, process_pending_events


class TransactionEventProcessingTests(TestCase):
    def test_due_profile_finalizer_closes_days_without_new_events(self):
        GamificationProfile.objects.create(
            user_id=1010,
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )

        result = finalize_due_profiles(
            now=datetime(2026, 9, 3, 1, 0, tzinfo=UTC),
            limit=10,
        )

        self.assertEqual(result["profiles"], 1)
        self.assertEqual(result["days"], 2)
        self.assertEqual(
            GamificationDay.objects.filter(user_id=1010, status=GamificationDay.Status.MISSED).count(),
            2,
        )

    def test_multiple_grants_from_one_event_create_one_notification_batch(self):
        event_id = UUID("00000000-0000-0000-0007-000000000001")
        GamificationProfile.objects.create(
            user_id=1009,
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )
        GamificationEventOutbox.objects.create(
            event_id=event_id,
            event_type="transaction.created",
            actor_user_id=1009,
            entity_type="transaction",
            entity_id="1301",
            accepted_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
            input_method="voice",
            payload={"transaction_type": "income", "flow_kind": "normal", "amount": "1.00"},
        )

        process_event(event_id)

        notice = GamificationNotification.objects.get(user_id=1009)
        self.assertEqual(notice.source_event_id, event_id)
        self.assertEqual(len(notice.grant_ids), 2)

    def test_event_after_gap_finalizes_missing_days_before_activation(self):
        GamificationProfile.objects.create(
            user_id=1008,
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )
        first_id = UUID("00000000-0000-0000-0006-000000000001")
        later_id = UUID("00000000-0000-0000-0006-000000000005")
        for event_id, day, entity_id in ((first_id, 1, "1201"), (later_id, 5, "1205")):
            GamificationEventOutbox.objects.create(
                event_id=event_id,
                event_type="transaction.created",
                actor_user_id=1008,
                entity_type="transaction",
                entity_id=entity_id,
                accepted_at=datetime(2026, 9, day, 9, 0, tzinfo=UTC),
                input_method="manual",
                payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "1.00"},
            )
            process_event(event_id)

        statuses = dict(
            GamificationDay.objects.filter(user_id=1008).values_list("local_date", "status")
        )
        self.assertEqual(statuses[datetime(2026, 9, 2, tzinfo=UTC).date()], GamificationDay.Status.MISSED)
        self.assertEqual(statuses[datetime(2026, 9, 3, tzinfo=UTC).date()], GamificationDay.Status.MISSED)
        self.assertEqual(statuses[datetime(2026, 9, 4, tzinfo=UTC).date()], GamificationDay.Status.MISSED)
        self.assertEqual(StreakState.objects.get(user_id=1008).current_streak, 1)

    def test_comeback_requires_a_real_break_three_inactive_days_and_three_active_days(self):
        GamificationProfile.objects.create(
            user_id=1007,
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )

        def add_event(day: int, suffix: int) -> None:
            event_id = UUID(f"00000000-0000-0000-0005-{suffix:012d}")
            GamificationEventOutbox.objects.create(
                event_id=event_id,
                event_type="transaction.created",
                actor_user_id=1007,
                entity_type="transaction",
                entity_id=str(1100 + suffix),
                accepted_at=datetime(2026, 9, day, 9, 0, tzinfo=UTC),
                input_method="manual",
                payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "1.00"},
            )
            process_event(event_id)

        add_event(1, 1)
        finalize_user_days(1007, now=datetime(2026, 9, 5, 1, 0, tzinfo=UTC))
        add_event(5, 5)
        add_event(6, 6)
        add_event(7, 7)

        streak = StreakState.objects.get(user_id=1007)
        self.assertEqual(streak.comeback_run, 3)
        self.assertTrue(AchievementGrant.objects.filter(user_id=1007, achievement_key="comeback").exists())
        self.assertEqual(
            AchievementProgress.objects.get(user_id=1007, achievement_key="comeback").current_value,
            3,
        )

    def test_all_input_methods_award_secret_and_store_distinct_day_progress(self):
        GamificationProfile.objects.create(user_id=1006, timezone_name="Europe/Kyiv")
        for index, method in enumerate(("manual", "text", "voice", "screenshot"), start=1):
            event_id = UUID(f"00000000-0000-0000-0004-{index:012d}")
            GamificationEventOutbox.objects.create(
                event_id=event_id,
                event_type="transaction.created",
                actor_user_id=1006,
                entity_type="transaction",
                entity_id=str(1000 + index),
                accepted_at=datetime(2026, 9, index, 9, 0, tzinfo=UTC),
                input_method=method,
                payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "1.00"},
            )
            process_event(event_id)

        self.assertTrue(
            AchievementGrant.objects.filter(user_id=1006, achievement_key="all-input-methods").exists()
        )
        self.assertEqual(
            AchievementProgress.objects.get(user_id=1006, achievement_key="voice-days").current_value,
            1,
        )
        self.assertEqual(
            AchievementProgress.objects.get(user_id=1006, achievement_key="screenshot-days").current_value,
            1,
        )
        self.assertEqual(
            AchievementProgress.objects.get(user_id=1006, achievement_key="all-input-methods").current_value,
            4,
        )

    @override_settings(GAMIFICATION_PROCESSING_ENABLED=False)
    def test_worker_flag_disables_processing_without_consuming_events(self):
        from gamification.tasks import process_gamification_events_task

        GamificationEventOutbox.objects.create(
            event_id=UUID("00000000-0000-0000-0003-000000000001"),
            event_type="transaction.created",
            actor_user_id=1005,
            entity_type="transaction",
            entity_id="901",
            accepted_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
            input_method="manual",
            payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "1.00"},
        )

        result = process_gamification_events_task.run()

        self.assertEqual(result, {"disabled": True, "checked": 0, "processed": 0, "failed": 0})
        self.assertEqual(GamificationEventOutbox.objects.get().status, "pending")

    def test_pending_processor_respects_limit_and_oldest_first(self):
        GamificationProfile.objects.create(user_id=1004, timezone_name="Europe/Kyiv")
        for index, hour in enumerate((10, 9), start=1):
            GamificationEventOutbox.objects.create(
                event_id=UUID(f"00000000-0000-0000-0002-{index:012d}"),
                event_type="transaction.created",
                actor_user_id=1004,
                entity_type="transaction",
                entity_id=str(800 + index),
                accepted_at=datetime(2026, 9, 1, hour, 0, tzinfo=UTC),
                input_method="manual",
                payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "1.00"},
            )

        first = process_pending_events(limit=1)
        processed_entity = GamificationEventOutbox.objects.get(status="processed").entity_id
        second = process_pending_events(limit=10)

        self.assertEqual(first, {"checked": 1, "processed": 1, "failed": 0})
        self.assertEqual(processed_entity, "802")
        self.assertEqual(second, {"checked": 1, "processed": 1, "failed": 0})

    def test_missed_day_consumes_one_shield_and_preserves_streak(self):
        GamificationProfile.objects.create(
            user_id=1003,
            mascot="bob",
            timezone_name="Europe/Kyiv",
            started_at=datetime(2026, 9, 1, 9, 0, tzinfo=UTC),
        )
        for index, day in enumerate((1, 2, 3), start=1):
            event_id = UUID(f"00000000-0000-0000-0001-{index:012d}")
            GamificationEventOutbox.objects.create(
                event_id=event_id,
                event_type="transaction.created",
                actor_user_id=1003,
                entity_type="transaction",
                entity_id=str(700 + index),
                accepted_at=datetime(2026, 9, day, 9, 30, tzinfo=UTC),
                input_method="manual",
                payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "10.00"},
            )
            process_event(event_id)

        first_count = finalize_user_days(1003, now=datetime(2026, 9, 5, 1, 0, tzinfo=UTC))
        second_count = finalize_user_days(1003, now=datetime(2026, 9, 5, 1, 0, tzinfo=UTC))

        streak = StreakState.objects.get(user_id=1003)
        missed_day = GamificationDay.objects.get(user_id=1003, local_date="2026-09-04")
        self.assertEqual((first_count, second_count), (2, 0))
        self.assertEqual(
            GamificationDay.objects.filter(user_id=1003, finalized_at__isnull=False).count(),
            4,
        )
        self.assertEqual(missed_day.status, GamificationDay.Status.FROZEN)
        self.assertEqual((streak.current_streak, streak.shield_balance), (3, 0))
        self.assertEqual(ShieldLedger.objects.filter(user_id=1003, delta=-1).count(), 1)

    def test_three_consecutive_days_award_level_three_and_one_shield(self):
        GamificationProfile.objects.create(
            user_id=1002,
            mascot="capi",
            timezone_name="Europe/Kyiv",
        )
        for index, day in enumerate((1, 2, 3), start=1):
            event_id = UUID(f"00000000-0000-0000-0000-{index:012d}")
            GamificationEventOutbox.objects.create(
                event_id=event_id,
                event_type="transaction.created",
                actor_user_id=1002,
                entity_type="transaction",
                entity_id=str(600 + index),
                accepted_at=datetime(2026, 9, day, 9, 30, tzinfo=UTC),
                input_method="manual",
                payload={"transaction_type": "income", "flow_kind": "normal", "amount": "10.00"},
            )
            process_event(event_id)

        streak = StreakState.objects.get(user_id=1002)
        self.assertEqual((streak.current_streak, streak.best_streak, streak.lifetime_active_days), (3, 3, 3))
        self.assertEqual(streak.shield_balance, 1)
        self.assertTrue(
            AchievementGrant.objects.filter(
                user_id=1002,
                achievement_key="streak-3",
                level_key=3,
            ).exists()
        )
        self.assertEqual(ShieldLedger.objects.filter(user_id=1002, delta=1).count(), 1)

    def test_confirmed_voice_transaction_activates_day_and_awards_once(self):
        GamificationProfile.objects.create(
            user_id=1001,
            mascot="bob",
            timezone_name="Europe/Kyiv",
        )
        event_id = UUID("00000000-0000-0000-0000-000000000111")
        GamificationEventOutbox.objects.create(
            event_id=event_id,
            event_type="transaction.created",
            actor_user_id=1001,
            entity_type="transaction",
            entity_id="501",
            accepted_at=datetime(2026, 9, 1, 9, 30, tzinfo=UTC),
            input_method="voice",
            payload={"transaction_type": "expense", "flow_kind": "normal", "amount": "250.00"},
        )

        first_result = process_event(event_id)
        second_result = process_event(event_id)

        day = GamificationDay.objects.get(user_id=1001)
        streak = StreakState.objects.get(user_id=1001)
        self.assertEqual(first_result, {"processed": True, "grants": 2})
        self.assertEqual(second_result, {"processed": False, "grants": 0})
        self.assertEqual(day.local_date.isoformat(), "2026-09-01")
        self.assertEqual(day.status, GamificationDay.Status.ACTIVE)
        self.assertEqual((streak.current_streak, streak.best_streak, streak.lifetime_active_days), (1, 1, 1))
        self.assertEqual(ActivityContribution.objects.filter(user_id=1001).count(), 1)
        self.assertEqual(
            set(AchievementGrant.objects.values_list("achievement_key", flat=True)),
            {"first-record", "first-voice"},
        )
        self.assertTrue(ProcessedGamificationEvent.objects.filter(event_id=event_id).exists())
        self.assertEqual(GamificationEventOutbox.objects.get(event_id=event_id).status, "processed")
