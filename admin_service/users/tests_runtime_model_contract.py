"""The unmanaged Django mirror must preserve the bot's NOT NULL timestamps."""
from datetime import timedelta
from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.utils import timezone
from users.models import TelegramUser


class RuntimeUserModelContract(SimpleTestCase):
    def test_orm_defaults_match_required_runtime_timestamps(self):
        user = TelegramUser(tg_user_id=90700401)
        for name in ("created_at", "last_seen_at"):
            with self.subTest(field=name):
                field = TelegramUser._meta.get_field(name)
                self.assertFalse(field.null)
                self.assertTrue(field.has_default())
                self.assertIsNotNone(getattr(user, name))


class RuntimeUserInsertContract(TestCase):
    def test_orm_insert_without_timestamps_obeys_real_schema(self):
        before = timezone.now()
        user = TelegramUser.objects.create(tg_user_id=90700402)
        user.refresh_from_db()
        self.assertGreaterEqual(user.created_at, before)
        self.assertGreaterEqual(user.last_seen_at, before)

    def test_explicit_historical_activity_is_not_overwritten(self):
        historical = timezone.now() - timedelta(days=30)
        user = TelegramUser.objects.create(tg_user_id=90700403, created_at=historical, last_seen_at=historical)
        user.refresh_from_db()
        self.assertEqual(user.created_at, historical)
        self.assertEqual(user.last_seen_at, historical)
