from django.db import IntegrityError, transaction
from django.test import TestCase

from gamification.models import AchievementGrant, GamificationProfile


class GamificationModelTests(TestCase):
    def test_grant_subject_key_is_unique_across_retries(self):
        GamificationProfile.objects.create(user_id=1001, mascot="bob")
        AchievementGrant.objects.create(
            user_id=1001,
            achievement_key="first-record",
            level_key=0,
            scope_key="",
            trigger_event_id="00000000-0000-0000-0000-000000000001",
        )

        with self.assertRaises(IntegrityError), transaction.atomic():
            AchievementGrant.objects.create(
                user_id=1001,
                achievement_key="first-record",
                level_key=0,
                scope_key="",
                trigger_event_id="00000000-0000-0000-0000-000000000002",
            )

        self.assertEqual(AchievementGrant.objects.count(), 1)
