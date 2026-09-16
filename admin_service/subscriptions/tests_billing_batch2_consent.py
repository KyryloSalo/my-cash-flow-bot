"""Actual bot SQL exercised on the isolated PostgreSQL schema."""
import asyncio
import importlib.util
from pathlib import Path
import sys
from unittest import skipUnless
from django.db import connection
from django.test import TransactionTestCase
from subscriptions.models import TrialRecoveryCampaign, TrialRecoveryRecipient
from users.models import TelegramUser, UserAdminState

BOT_DIR = Path(__file__).resolve().parents[2] / 'bot'
original_path = list(sys.path)
original_config = sys.modules.get('config')
sys.path.insert(0, str(BOT_DIR))
spec = importlib.util.spec_from_file_location('billing_batch2_bot_recovery', BOT_DIR / 'trial_recovery.py')
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
sys.path[:] = original_path
if original_config is None:
    sys.modules.pop('config', None)
else:
    sys.modules['config'] = original_config

@skipUnless(connection.vendor == 'postgresql', 'requires isolated PostgreSQL')
class BillingBatch2Consent(TransactionTestCase):
    def setUp(self):
        from django.utils import timezone
        self.user, _ = TelegramUser.objects.get_or_create(tg_user_id=90223001,defaults={'lang':'uk','created_at':timezone.now()})
        UserAdminState.objects.create(telegram_user=self.user)
        self.recipient = TrialRecoveryRecipient.objects.create(user=self.user,campaign=TrialRecoveryCampaign.objects.create(),status='responded',support_case_id=123)

    def run_sql(self, operation):
        import asyncpg
        async def run():
            config = connection.settings_dict
            options = config.get("OPTIONS", {}).get("options", "")
            schema = options.split("search_path=", 1)[1] if "search_path=" in options else "public"
            conn = await asyncpg.connect(
                host=config.get('HOST') or '127.0.0.1',
                port=int(config.get('PORT') or 5432),
                user=config.get('USER') or None,
                password=config.get('PASSWORD') or None,
                database=config['NAME'],
                server_settings={'search_path':schema},
            )
            try:
                return await operation(conn)
            finally:
                await conn.close()
        return asyncio.run(run())

    def test_bill014_decline_persists_independent_consent(self):
        async def decline(conn):
            await bot.decline_contact(conn,recipient_id=self.recipient.pk,tg_user_id=self.user.pk)
            return await bot.get_recipient_owner_snapshot(conn,recipient_id=self.recipient.pk)
        snapshot=self.run_sql(decline)
        self.recipient.refresh_from_db()
        self.assertIsNotNone(self.recipient.personal_contact_declined_at)
        self.assertFalse(bot.owner_reply_allowed(snapshot))
        async def request(conn):
            await bot.request_contact(conn,recipient_id=self.recipient.pk,tg_user_id=self.user.pk)
            return await bot.get_recipient_owner_snapshot(conn,recipient_id=self.recipient.pk)
        snapshot=self.run_sql(request)
        self.assertTrue(bot.owner_reply_allowed(snapshot))

    def test_bill012_historical_optout_survives_converted_status(self):
        self.assertTrue(callable(getattr(bot,'owner_reply_allowed',None)), 'shared send-time consent guard missing')
        self.assertFalse(bot.owner_reply_allowed({'recovery_status':'converted','opted_out_at':'synthetic timestamp'}))
