from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from polls.models import PollCampaign, PollResponse
from users.models import TelegramUser


class PollFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.admin_user = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        self.telegram_user = TelegramUser.objects.create(
            tg_user_id=4001,
            first_name="Poll",
            username="poll_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

    def test_poll_campaign_and_response_saved(self):
        campaign = PollCampaign.objects.create(
            title="NPS",
            question="Rate us",
            type=PollCampaign.Type.RATING,
            created_by=self.admin_user,
        )
        response = PollResponse.objects.create(campaign=campaign, user=self.telegram_user, answer="5", rating_value=5)
        self.assertEqual(response.rating_value, 5)
        self.assertEqual(response.user_id, self.telegram_user.tg_user_id)
