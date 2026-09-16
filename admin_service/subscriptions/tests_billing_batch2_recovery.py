"""Recovery regressions: synthetic Telegram transport, real ORM."""
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from django.test import TestCase, override_settings
from subscriptions.models import Subscription, TrialRecoveryCampaign, TrialRecoveryRecipient, TrialRecoveryDelivery
from subscriptions import trial_recovery as recovery
from users.models import TelegramUser, UserAdminState

NOW = datetime(2030, 1, 15, 12, tzinfo=UTC)

@override_settings(TELEGRAM_BOT_TOKEN='synthetic-only', ADMIN_TEST_TELEGRAM_IDS=[])
class BillingBatch2Recovery(TestCase):
    def setUp(self):
        self.campaign = TrialRecoveryCampaign.objects.create(status='running',
            audience_snapshot={'audience_mode':'registered_without_card'})
        self.user = TelegramUser.objects.create(tg_user_id=90222001, created_at=NOW, lang='uk', onboarding_completed=True)
        UserAdminState.objects.create(telegram_user=self.user, timezone='UTC', access_scope='paywall', pending_start_payload='course')
        self.recipient = TrialRecoveryRecipient.objects.create(campaign=self.campaign, user=self.user, timezone='UTC', next_send_at=NOW)

    def test_bill011_stale_claim_becomes_unknown_without_resend(self):
        delivery = TrialRecoveryDelivery.objects.create(recipient=self.recipient,step=1,status='claimed',claimed_at=NOW-timedelta(hours=2))
        with patch('subscriptions.trial_recovery.send_telegram_message') as send:
            recovery.dispatch_trial_recovery(now=NOW)
        delivery.refresh_from_db()
        self.recipient.refresh_from_db()
        send.assert_not_called()
        self.assertEqual(delivery.status, 'unknown')
        self.assertIsNone(self.recipient.next_send_at)

    def test_bill011_rate_limit_retries_same_delivery_with_backoff(self):
        from common.telegram import TelegramSendError
        error = TelegramSendError('limited', payload='{"ok":false,"error_code":429,"parameters":{"retry_after":120}}')
        with patch('subscriptions.trial_recovery.send_telegram_message', side_effect=error):
            recovery.dispatch_trial_recovery(now=NOW)
        delivery = TrialRecoveryDelivery.objects.get(recipient=self.recipient)
        self.assertEqual(delivery.status, 'retry')
        self.assertGreaterEqual(delivery.next_retry_at, NOW + timedelta(seconds=120))
        with patch('subscriptions.trial_recovery.send_telegram_message', return_value={'ok':True,'result':{'message_id':321}}) as send:
            recovery.dispatch_trial_recovery(now=NOW + timedelta(seconds=30))
            send.assert_not_called()
            recovery.dispatch_trial_recovery(now=delivery.next_retry_at)
            send.assert_called_once()
        self.assertEqual(TrialRecoveryDelivery.objects.filter(recipient=self.recipient).count(), 1)
        delivery.refresh_from_db()
        self.assertEqual(delivery.status, 'sent')
        self.assertEqual(delivery.attempt_count, 2)

    def test_bill011_timeout_is_unknown_not_retryable(self):
        with patch('subscriptions.trial_recovery.send_telegram_message', side_effect=TimeoutError('accepted then lost')) as send:
            recovery.dispatch_trial_recovery(now=NOW)
            recovery.dispatch_trial_recovery(now=NOW + timedelta(minutes=1))
        self.assertEqual(send.call_count, 1)
        delivery = TrialRecoveryDelivery.objects.get(recipient=self.recipient)
        self.assertEqual(delivery.status, 'unknown')
        self.assertIsNone(delivery.next_retry_at)

    def test_bill011_sent_receipt_repairs_progress_after_crash(self):
        TrialRecoveryDelivery.objects.create(recipient=self.recipient,step=1,status='sent',sent_at=NOW,telegram_message_id=654)
        with patch('subscriptions.trial_recovery.send_telegram_message') as send:
            recovery.dispatch_trial_recovery(now=NOW + timedelta(minutes=1))
        self.recipient.refresh_from_db()
        send.assert_not_called()
        self.assertEqual(self.recipient.sent_count, 1)
        self.assertEqual(self.recipient.first_sent_at, NOW)

    def test_bill011_response_during_send_does_not_hide_delivered_message(self):
        def send(**kwargs):
            TrialRecoveryRecipient.objects.filter(pk=self.recipient.pk).update(status='responded', next_send_at=None)
            return {'ok':True,'result':{'message_id':999}}
        with patch('subscriptions.trial_recovery.send_telegram_message', side_effect=send):
            recovery.dispatch_trial_recovery(now=NOW)
        self.recipient.refresh_from_db()
        self.assertEqual(self.recipient.status, 'responded')
        self.assertEqual(self.recipient.sent_count, 1)
        self.assertIsNone(self.recipient.next_send_at)

    def test_bill015_batch_rechecks_clock_before_each_send(self):
        clock = [NOW.replace(hour=18, minute=59, second=59)]
        TrialRecoveryRecipient.objects.filter(pk=self.recipient.pk).update(next_send_at=clock[0])
        other = TelegramUser.objects.create(tg_user_id=90222002,created_at=NOW,lang='uk',onboarding_completed=True)
        UserAdminState.objects.create(telegram_user=other, timezone='UTC', access_scope='paywall',pending_start_payload='course')
        second = TrialRecoveryRecipient.objects.create(campaign=self.campaign,user=other,timezone='UTC',next_send_at=clock[0])
        def send(**kwargs):
            clock[0] += timedelta(seconds=2)
            return {'ok':True,'result':{'message_id':123}}
        with patch('subscriptions.trial_recovery.timezone.now', side_effect=lambda:clock[0]), patch(
            'subscriptions.trial_recovery.send_telegram_message', side_effect=send) as sender:
            recovery.dispatch_trial_recovery()
        self.assertEqual(sender.call_count, 1)
        second.refresh_from_db()
        self.assertGreater(second.next_send_at, clock[0])
        delivered = TrialRecoveryDelivery.objects.get(recipient=self.recipient)
        self.assertEqual(delivered.sent_at, NOW.replace(hour=19,minute=0,second=1))

    def test_bill013_subscription_history_cannot_receive_trial_promise(self):
        for status in ('manual','expired'):
            with self.subTest(status=status):
                Subscription.objects.update_or_create(user=self.user, defaults={'status':status,'expires_at':NOW})
                self.assertEqual(recovery._ineligible_reason(self.user, include_existing_recipient=False), 'trial_not_available')

    def test_bill012_conversion_preserves_optout_suppression(self):
        self.recipient.status='opted_out'
        self.recipient.opted_out_at=NOW
        self.recipient.save()
        recovery.mark_trial_recovery_converted(self.user.pk, at=NOW + timedelta(hours=1))
        self.recipient.refresh_from_db()
        self.assertEqual(self.recipient.opted_out_at, NOW)
        self.assertEqual(self.recipient.status, 'opted_out')
        self.assertIsNotNone(self.recipient.converted_at)
