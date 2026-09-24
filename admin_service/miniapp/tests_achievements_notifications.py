from datetime import UTC, datetime
from uuid import UUID

from django.test import TestCase

from gamification.models import GamificationNotification
from gamification.services import GamificationActionError, acknowledge_notification, claim_next_notification


class GamificationNotificationTests(TestCase):
    def test_acknowledgement_requires_the_active_claim_token(self):
        notice = GamificationNotification.objects.create(
            user_id=4002,
            source_event_id=UUID("00000000-0000-0000-0008-000000000002"),
            grant_ids=[3],
        )
        now = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)
        claim = claim_next_notification(user_id=4002, device_id="device-a", now=now)

        with self.assertRaises(GamificationActionError) as raised:
            acknowledge_notification(
                user_id=4002,
                notification_id=str(notice.id),
                claim_token="wrong-token",
                device_id="device-a",
                now=now,
            )
        self.assertEqual(raised.exception.code, "invalid_claim")

        acknowledge_notification(
            user_id=4002,
            notification_id=str(notice.id),
            claim_token=claim["claim_token"],
            device_id="device-a",
            now=now,
        )
        notice.refresh_from_db()
        self.assertEqual(notice.acknowledged_at, now)

    def test_active_claim_lease_prevents_a_second_device_from_claiming(self):
        notice = GamificationNotification.objects.create(
            user_id=4001,
            source_event_id=UUID("00000000-0000-0000-0008-000000000001"),
            grant_ids=[1, 2],
        )
        now = datetime(2026, 9, 10, 10, 0, tzinfo=UTC)

        first = claim_next_notification(user_id=4001, device_id="device-a", now=now)
        second = claim_next_notification(user_id=4001, device_id="device-b", now=now)

        self.assertEqual(first["notification_id"], str(notice.id))
        self.assertTrue(first["claim_token"])
        self.assertIsNone(second)
