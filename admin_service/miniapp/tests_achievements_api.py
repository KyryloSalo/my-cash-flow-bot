import json
from uuid import uuid4

from django.test import Client, TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from gamification.models import AchievementGrant, GamificationNotification, GamificationProfile
from users.models import TelegramUser


@override_settings(GAMIFICATION_UI_ENABLED=True, GAMIFICATION_PROCESSING_ENABLED=True)
class GamificationApiTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.client = Client()
        TelegramUser.objects.create(
            tg_user_id=6001,
            first_name="Marta",
            username="marta",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        session = self.client.session
        session["miniapp_tg_user_id"] = 6001
        session.save()
        GamificationProfile.objects.create(user_id=6001, mascot="bob", started_at=timezone.now())

    def test_overview_returns_profile_day_and_safe_catalog(self):
        response = self.client.get("/app/api/gamification/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["profile"]["mascot"], "bob")
        self.assertIn("day_id", payload["day"])
        self.assertEqual(len(payload["catalog"]), 24)
        secret = next(item for item in payload["catalog"] if item["key"] == "category-discovery")
        self.assertEqual(secret["name"], "Секретне досягнення")
        self.assertNotIn("condition", secret)
        self.assertTrue(secret["is_secret"])

    def test_overview_initializes_first_time_profile_and_requests_mascot(self):
        GamificationProfile.objects.filter(user_id=6001).delete()

        response = self.client.get("/app/api/gamification/overview")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["profile"]["needs_mascot"])
        self.assertEqual(payload["profile"]["mascot"], "bob")
        self.assertTrue(GamificationProfile.objects.filter(user_id=6001).exists())

    def test_notification_claim_includes_safe_grant_details_for_rendering(self):
        grant = AchievementGrant.objects.create(
            user_id=6001,
            achievement_key="category-discovery",
            level_key=1,
            trigger_event_id=uuid4(),
        )
        notification = GamificationNotification.objects.create(
            user_id=6001,
            source_event_id=uuid4(),
            grant_ids=[grant.id],
        )

        response = self.client.post(
            "/app/api/gamification/notifications/claim",
            data={"device_id": "device-1"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        claimed = response.json()["notification"]
        self.assertEqual(claimed["notification_id"], str(notification.id))
        self.assertEqual(claimed["grants"][0]["key"], "category-discovery")
        self.assertEqual(claimed["grants"][0]["name"], "Так ось куди!")
        self.assertNotIn("condition", claimed["grants"][0])

    @override_settings(GAMIFICATION_UI_ENABLED=False)
    def test_mutating_routes_are_not_exposed_while_ui_flag_is_off(self):
        for method, path, payload in (
            ("PATCH", "/app/api/gamification/preferences", {"mascot": "bob"}),
            ("POST", "/app/api/gamification/no-expenses", {}),
            ("PUT", "/app/api/gamification/pins", {"achievement_keys": []}),
            ("POST", "/app/api/gamification/notifications/claim", {"device_id": "device-1"}),
        ):
            response = self.client.generic(
                method,
                path,
                data=json.dumps(payload),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 404, path)
