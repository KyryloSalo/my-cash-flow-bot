from uuid import UUID

from django.test import TestCase

from gamification.models import AchievementGrant, PinnedAchievement
from gamification.services import set_pinned_achievements


class PinnedAchievementTests(TestCase):
    def test_only_earned_achievements_are_saved_in_requested_order(self):
        for index, key in enumerate(("first-record", "first-voice"), start=1):
            AchievementGrant.objects.create(
                user_id=5001,
                achievement_key=key,
                trigger_event_id=UUID(f"00000000-0000-0000-0009-{index:012d}"),
            )

        result = set_pinned_achievements(
            user_id=5001,
            achievement_keys=["first-voice", "first-record"],
        )

        self.assertEqual(result, ["first-voice", "first-record"])
        self.assertEqual(
            list(
                PinnedAchievement.objects.filter(user_id=5001)
                .order_by("position")
                .values_list("achievement_key", flat=True)
            ),
            ["first-voice", "first-record"],
        )
