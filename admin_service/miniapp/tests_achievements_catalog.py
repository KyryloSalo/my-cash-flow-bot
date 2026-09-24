from django.test import SimpleTestCase

from gamification.catalog import achievement_definitions, public_achievement


class PublicAchievementTests(SimpleTestCase):
    def test_catalog_has_24_families_and_the_shipped_subset_is_explicit(self):
        definitions = achievement_definitions()

        self.assertEqual(len(definitions), 24)
        self.assertEqual(
            {item.key for item in definitions if item.availability == "available"},
            {
                "first-record",
                "first-voice",
                "first-text",
                "first-screenshot",
                "streak-3",
                "lifetime-days",
                "voice-days",
                "screenshot-days",
                "seven-fridays",
                "all-input-methods",
                "comeback",
            },
        )
        streak = next(item for item in definitions if item.key == "streak-3")
        self.assertEqual(streak.thresholds, (3, 7, 14, 30, 60, 100, 180, 365))
        self.assertEqual(streak.scene_numbers, (10, 11, 12, 13, 14, 15, 16, 17))

    def test_locked_secret_does_not_expose_name_or_condition(self):
        payload = public_achievement("category-discovery", mascot="bob", earned=False)

        self.assertEqual(payload["name"], "Секретне досягнення")
        self.assertIsNone(payload["description"])
        self.assertNotIn("condition", payload)
        self.assertTrue(payload["is_secret"])
        self.assertEqual(payload["hint"], "У звітах ховаються відповіді")
        self.assertTrue(payload["asset_locked"].endswith("/34-category-discovery-locked.png"))
