from datetime import timedelta
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from gamification.models import GamificationProfile
from gamification.services import finalize_due_profiles as finalize_due_profiles_service
from gamification.tasks import finalize_due_profiles_task


class GamificationTaskTests(TestCase):
    @override_settings(GAMIFICATION_PROCESSING_ENABLED=False)
    def test_due_profile_finalizer_is_disabled_with_processing_flag(self):
        self.assertEqual(
            finalize_due_profiles_task.run(),
            {"enabled": False, "profiles": 0, "days": 0, "failed": 0},
        )

    @override_settings(GAMIFICATION_PROCESSING_ENABLED=True)
    @patch("gamification.tasks.finalize_due_profiles", return_value={"profiles": 7, "days": 8, "failed": 0})
    def test_due_profile_finalizer_runs_when_processing_is_enabled(self, finalize):
        self.assertEqual(
            finalize_due_profiles_task.run(),
            {"enabled": True, "profiles": 7, "days": 8, "failed": 0},
        )
        finalize.assert_called_once_with()

    @patch("gamification.services.finalize_user_days")
    def test_due_profile_finalizer_rotates_past_the_first_batch(self, finalize_user_days):
        first = GamificationProfile.objects.create(user_id=101, timezone_name="Europe/Kyiv")
        second = GamificationProfile.objects.create(user_id=202, timezone_name="Europe/Kyiv")
        old = timezone.now() - timedelta(days=2)
        GamificationProfile.objects.filter(pk=first.pk).update(updated_at=old)
        GamificationProfile.objects.filter(pk=second.pk).update(updated_at=old + timedelta(hours=1))

        finalize_due_profiles_service(now=timezone.now(), limit=1)
        finalize_due_profiles_service(now=timezone.now(), limit=1)

        self.assertEqual(
            [call.args[0] for call in finalize_user_days.call_args_list],
            [first.user_id, second.user_id],
        )