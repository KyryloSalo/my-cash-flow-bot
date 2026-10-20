"""Synthetic recognition storage outbox acceptance, no network or user inputs."""
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.test import TestCase, override_settings
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from common.telegram import TelegramSendError
from miniapp.models import RecognitionCaptureConfig
from miniapp.recognition_storage import _storage_alert


@override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[999001, 999002],
                   TELEGRAM_BOT_TOKEN='synthetic-only')
class RecognitionAlertRetryTests(TestCase):
    def setUp(self):
        self.cfg = RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7])

    def emit_full(self):
        with self.captureOnCommitCallbacks(execute=True):
            _storage_alert(self.cfg, used=10, quota=10)

    def test_broker_outage_keeps_durable_outbox_without_another_capture(self):
        from miniapp.tasks import deliver_recognition_storage_alerts_task
        with patch.object(deliver_recognition_storage_alerts_task, 'apply_async', side_effect=RuntimeError('broker unavailable')):
            self.emit_full()
        self.assertEqual(AdminNotificationLog.objects.filter(status='queued').count(), 2)
        with patch('miniapp.recognition_storage.send_telegram_message') as send:
            result = deliver_recognition_storage_alerts_task.run()
        self.assertEqual(result, {'sent': 2, 'failed': 0})
        self.assertEqual(send.call_count, 2)
        self.assertEqual(AdminNotificationLog.objects.filter(status='sent').count(), 2)
        self.assertEqual(deliver_recognition_storage_alerts_task.run(), {'sent': 0, 'failed': 0})

    def test_telegram_failure_retries_only_failed_recipient(self):
        from miniapp.tasks import deliver_recognition_storage_alerts_task
        with patch.object(deliver_recognition_storage_alerts_task, 'apply_async'):
            self.emit_full()
        with patch('miniapp.recognition_storage.send_telegram_message', side_effect=[None, TelegramSendError('synthetic failure')]):
            self.assertEqual(deliver_recognition_storage_alerts_task.run(), {'sent': 1, 'failed': 1})
        with patch('miniapp.recognition_storage.send_telegram_message') as send:
            self.assertEqual(deliver_recognition_storage_alerts_task.run(), {'sent': 1, 'failed': 0})
        send.assert_called_once()
        for row in AdminNotificationLog.objects.all():
            self.assertEqual(row.status, 'sent')
            self.assertEqual(set(row.payload), {'event_type', 'used_bytes', 'quota_bytes', 'timestamp'})
            self.assertNotIn('synthetic failure', row.error_message)

    def test_beat_retries_independently_of_uploads(self):
        from miniapp.tasks import deliver_recognition_storage_alerts_task
        entry = settings.CELERY_BEAT_SCHEDULE['miniapp-recognition-storage-alert-retry']
        self.assertEqual(entry['task'], deliver_recognition_storage_alerts_task.name)
        self.assertEqual(entry['schedule'].minute, set(range(0, 60, 5)))

    def test_disabled_notification_delivery_leaves_pending_and_deduplicated(self):
        from miniapp.tasks import deliver_recognition_storage_alerts_task
        with patch.object(deliver_recognition_storage_alerts_task, 'apply_async'):
            self.emit_full()
            self.emit_full()
        with override_settings(ADMIN_NOTIFICATIONS_ENABLED=False):
            self.assertEqual(deliver_recognition_storage_alerts_task.run(), {'sent': 0, 'failed': 0})
        self.assertEqual(AdminNotificationLog.objects.count(), 2)
        self.assertFalse(AdminNotificationLog.objects.filter(status='sent').exists())
