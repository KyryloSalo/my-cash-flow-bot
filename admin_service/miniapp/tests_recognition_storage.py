"""Synthetic PostgreSQL acceptance for private disk quota and admin alerts."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Barrier
from unittest.mock import patch

from django.db import connections, transaction
from django.contrib.auth import get_user_model
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import capture_attempt, delete_user_captures, purge_expired
from users.models import TelegramUser


class StorageFixture:
    def setUp(self):
        super().setUp()
        TelegramUser.objects.create(tg_user_id=7)
        self.directory = TemporaryDirectory(dir=os.environ['TMPDIR'])
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / 'private-capture'
        self.settings_override = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root),
                                                  ADMIN_TELEGRAM_IDS=[999001],
                                                  ADMIN_NOTIFICATIONS_ENABLED=True,
                                                  TELEGRAM_BOT_TOKEN='synthetic-only')
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.notify = patch('miniapp.tasks.deliver_recognition_storage_alerts_task.apply_async')
        self.enqueue = self.notify.start()
        self.addCleanup(self.notify.stop)
        self.cfg = RecognitionCaptureConfig.objects.create(
            enabled=True, cohort_ids=[7], expires_at=timezone.now() + timedelta(days=7),
            max_total_bytes=10, max_bytes_per_file=10,
        )

    def capture(self, data, attempt='synthetic'):
        return capture_attempt(user_id=7, source='audio', raw_bytes=data,
                               mime_type='audio/webm', attempt_id=attempt)

    def disk_usage(self):
        return sum(path.stat().st_size for path in self.root.iterdir() if path.is_file())


class RecognitionDiskStorageTests(StorageFixture, TestCase):
    def test_default_is_one_gib_not_ram_or_half_gib(self):
        self.assertEqual(RecognitionCaptureConfig().max_total_bytes, 1024 ** 3)

    def test_media_is_private_disk_file_not_database_blob(self):
        row = self.capture(b'abc')
        self.assertIsInstance(row.raw_media, str)
        path = self.root / row.raw_media
        self.assertEqual(path.read_bytes(), b'abc')
        self.assertEqual(row.raw_media_size, 3)
        self.assertNotIn('/', row.raw_media)
        if os.name != 'nt':
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)

    def test_full_alert_is_sanitized_deduplicated_and_text_still_works(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNotNone(self.capture(b'0123456789'))
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNone(self.capture(b'x', 'rejected-one'))
            self.assertIsNone(self.capture(b'y', 'rejected-two'))
            row = capture_attempt(user_id=7, source='text', raw_text='private synthetic text')
        self.assertIsNotNone(row)
        self.assertEqual(self.disk_usage(), 10)
        self.assertEqual(self.enqueue.call_count, 1)
        from miniapp.recognition_storage import _alert_message
        log = AdminNotificationLog.objects.get()
        event, payload = log.event_type, log.payload
        text = _alert_message(payload)
        self.assertEqual(event, 'recognition_quality_storage_full')
        self.assertEqual(payload['used_bytes'], 10)
        self.assertEqual(payload['quota_bytes'], 10)
        self.assertEqual(set(payload), {'event_type', 'used_bytes', 'quota_bytes', 'timestamp'})
        self.assertNotIn('private synthetic', text)
        self.assertNotIn('0123456789', text)
        self.assertFalse(self.enqueue.call_args.kwargs['retry'])

    def test_warning_then_full_each_notifies_once(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.capture(b'123456789')
        with self.captureOnCommitCallbacks(execute=True):
            self.capture(b'0', 'last-byte')
        self.assertEqual(list(AdminNotificationLog.objects.order_by('pk').values_list('event_type', flat=True)), [
            'recognition_quality_storage_warning', 'recognition_quality_storage_full'])

    def test_actual_unreferenced_files_count_against_quota(self):
        self.root.mkdir(mode=0o700)
        (self.root / ('a' * 32 + '.bin')).write_bytes(b'123456789')
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNone(self.capture(b'xx'))
        self.assertEqual(self.disk_usage(), 9)
        self.assertEqual(AdminNotificationLog.objects.get().payload['used_bytes'], 9)

    def test_notification_broker_failure_is_fail_open_and_rate_limited(self):
        self.enqueue.side_effect = RuntimeError('synthetic broker unavailable')
        with self.captureOnCommitCallbacks(execute=True):
            row = self.capture(b'0123456789')
        self.assertIsNotNone(row)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNone(self.capture(b'x', 'retry'))
        self.assertEqual(self.enqueue.call_count, 1)

    def test_disk_write_failure_leaves_no_database_capture_or_file(self):
        with patch('miniapp.recognition_quality._write_media', side_effect=OSError('synthetic disk full'), create=True):
            self.assertIsNone(self.capture(b'abc'))
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_database_write_failure_cleans_file_and_does_not_poison_caller_transaction(self):
        with patch.object(RecognitionCapture.objects, 'create', side_effect=RuntimeError('synthetic DB error')):
            self.assertIsNone(self.capture(b'abc'))
        self.assertEqual(self.disk_usage(), 0)
        self.assertEqual(RecognitionCaptureConfig.objects.count(), 1)

    def test_account_purge_removes_private_file_after_commit(self):
        row = self.capture(b'abc')
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(delete_user_captures(7), 1)
        self.assertFalse((self.root / row.raw_media).exists())

    def test_retention_purge_frees_files_before_cohort_expiry(self):
        self.cfg.retention_days = 1
        self.cfg.save(update_fields=['retention_days'])
        row = self.capture(b'abc')
        RecognitionCapture.objects.filter(pk=row.pk).update(created_at=timezone.now() - timedelta(days=2))
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(purge_expired(), 1)
        self.assertFalse((self.root / row.raw_media).exists())

    def test_config_cannot_raise_hard_one_gib_ceiling(self):
        self.cfg.max_total_bytes = 2 * 1024 ** 3
        self.cfg.save(update_fields=['max_total_bytes'])
        # Actual file accounting is exercised above; avoid allocating 1 GiB in tests.
        with patch('miniapp.recognition_quality._disk_usage', return_value=1024 ** 3, create=True):
            with self.captureOnCommitCallbacks(execute=True):
                self.assertIsNone(self.capture(b'x'))
        self.assertEqual(AdminNotificationLog.objects.get().payload['quota_bytes'], 1024 ** 3)

    def test_missing_private_root_skips_media_but_not_primary_recognition(self):
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=''):
            self.assertIsNone(self.capture(b'abc'))
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_public_directory_cannot_be_used_for_storage(self):
        with override_settings(STATIC_ROOT=str(self.root)):
            self.assertIsNone(self.capture(b'abc'))
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_rolled_back_delete_preserves_file(self):
        row = self.capture(b'abc')
        with self.captureOnCommitCallbacks(execute=True):
            try:
                with transaction.atomic():
                    delete_user_captures(7)
                    raise RuntimeError('synthetic rollback')
            except RuntimeError:
                pass
        self.assertTrue((self.root / row.raw_media).exists())
        self.assertTrue(RecognitionCapture.objects.filter(pk=row.pk).exists())

    def test_rolled_back_capture_orphan_still_counts_against_quota(self):
        try:
            with transaction.atomic():
                self.capture(b'123456')
                raise RuntimeError('synthetic rollback')
        except RuntimeError:
            pass
        self.assertFalse(RecognitionCapture.objects.exists())
        self.assertEqual(self.disk_usage(), 6)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNone(self.capture(b'123456', 'second'))
        self.assertEqual(self.disk_usage(), 6)

    def test_cleanup_removes_expired_orphan_and_keeps_recent_file(self):
        self.root.mkdir(mode=0o700)
        old = self.root / ('c' * 32 + '.bin')
        old.write_bytes(b'old')
        old_time = (timezone.now() - timedelta(days=8)).timestamp()
        os.utime(old, (old_time, old_time))
        row = self.capture(b'abc')
        with self.captureOnCommitCallbacks(execute=True):
            purge_expired()
        self.assertFalse(old.exists())
        self.assertTrue((self.root / row.raw_media).exists())

    def test_cohort_renewal_does_not_bypass_full_alert_deduplication(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.capture(b'0123456789')
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7], max_total_bytes=10)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertIsNone(self.capture(b'x', 'renewed'))
        self.assertEqual(self.enqueue.call_count, 1)

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN='synthetic-only',
                       ADMIN_TELEGRAM_IDS=[999001])
    def test_full_event_reaches_existing_admin_sender_and_sanitized_delivery_log(self):
        from admin_notifications.models import AdminNotificationLog
        from miniapp.tasks import deliver_recognition_storage_alerts_task

        self.enqueue.side_effect = lambda **options: deliver_recognition_storage_alerts_task.run(*options['args'])
        with patch('miniapp.recognition_storage.send_telegram_message') as send:
            with self.captureOnCommitCallbacks(execute=True):
                self.capture(b'0123456789')
        send.assert_called_once()
        self.assertEqual(send.call_args.kwargs['chat_id'], 999001)
        self.assertIsNone(send.call_args.kwargs['parse_mode'])
        record = AdminNotificationLog.objects.get()
        self.assertEqual(record.status, AdminNotificationLog.Status.SENT)
        self.assertEqual(record.event_type, 'recognition_quality_storage_full')
        self.assertEqual(record.payload['used_bytes'], 10)
        self.assertNotIn('raw', str(record.payload))
        self.assertNotIn('0123456789', send.call_args.kwargs['text'])

    def test_raw_export_reads_private_file_only_with_authentication(self):
        from miniapp.recognition_quality import export_weekly

        operator = get_user_model().objects.create_superuser('storage-operator', password='synthetic-only')
        self.capture(b'abc')
        with override_settings(RECOGNITION_QUALITY_EXPORT_TOKEN='synthetic-export'):
            with self.assertRaises(PermissionError):
                export_weekly(include_raw=True)
            result = export_weekly(requester_id=operator.pk, export_token='synthetic-export', include_raw=True)
        self.assertEqual(result['items'][0]['raw_media'], 'YWJj')

    def test_hourly_cleanup_task_and_schedule_are_wired(self):
        from django.conf import settings
        from miniapp.tasks import purge_expired_recognition_captures_task

        entry = settings.CELERY_BEAT_SCHEDULE['miniapp-purge-expired-recognition-captures']
        self.assertEqual(entry['task'], purge_expired_recognition_captures_task.name)
        row = self.capture(b'abc')
        RecognitionCapture.objects.filter(pk=row.pk).update(created_at=timezone.now() - timedelta(days=8))
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(purge_expired_recognition_captures_task.run(), {'deleted': 1})
        self.assertFalse((self.root / row.raw_media).exists())


class RecognitionDiskConcurrencyTests(StorageFixture, TransactionTestCase):
    def test_two_concurrent_uploads_cannot_exceed_quota(self):
        barrier = Barrier(2)

        def upload(number):
            try:
                barrier.wait(timeout=10)
                return self.capture(b'123456', f'parallel-{number}') is not None
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(upload, [1, 2]))
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.disk_usage(), 6)
        self.assertEqual(RecognitionCapture.objects.count(), 1)
        self.assertEqual(self.enqueue.call_count, 1)
