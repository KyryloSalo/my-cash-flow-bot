from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.db import close_old_connections, connection, connections
from django.test import TransactionTestCase, override_settings
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from miniapp.models import RecognitionCapture
from miniapp.recognition_photo_notifications import deliver_photo_notifications


@override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN='synthetic', ADMIN_TELEGRAM_IDS=[9001])
class PhotoDeliveryReliabilityTests(TransactionTestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.settings_override = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root))
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.original = b'synthetic-private-original'
        key = 'a' * 32 + '.bin'
        (self.root / key).write_bytes(self.original)
        self.capture = RecognitionCapture.objects.create(
            attempt_id='reliability', user_id=42, source='image', raw_media=key,
            raw_media_size=len(self.original), mime_type='image/jpeg', outcome='parsed',
        )
        self.log = AdminNotificationLog.objects.create(
            event_type='recognition_photo', telegram_chat_id=9001,
            payload={'capture_id': self.capture.pk},
        )

    def test_telegram_http_runs_outside_database_transaction(self):
        atomic_at_send = []
        with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                   side_effect=lambda **kwargs: atomic_at_send.append(connection.in_atomic_block)):
            self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 1)
        self.assertEqual(atomic_at_send, [False])

    def test_failure_is_not_retried_before_its_backoff(self):
        with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                   side_effect=OSError('synthetic network failure')) as send:
            self.assertEqual(deliver_photo_notifications([self.log.pk])['failed'], 1)
            self.assertEqual(deliver_photo_notifications([self.log.pk])['failed'], 0)
        self.assertEqual(send.call_count, 1)

    def test_retries_end_in_visible_dead_letter(self):
        now = timezone.now()
        with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                   side_effect=OSError('synthetic network failure')) as send:
            for attempt in range(8):
                with patch('miniapp.recognition_photo_notifications.timezone.now',
                           return_value=now + timedelta(hours=attempt)):
                    deliver_photo_notifications([self.log.pk])
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'dead')
        self.assertLessEqual(send.call_count, 6)

    def test_active_claim_blocks_reentrant_worker(self):
        nested = []

        def send_document(**kwargs):
            if not nested:
                nested.append(None)
                nested[0] = deliver_photo_notifications([self.log.pk])

        with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                   side_effect=send_document) as send:
            self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 1)
        self.assertEqual(nested[0]['sent'], 0)
        self.assertEqual(send.call_count, 1)

    def test_expired_lease_does_not_reclaim_an_active_http_send(self):
        sending, release = Event(), Event()

        def blocking_send(**kwargs):
            if not sending.is_set():
                sending.set()
                if not release.wait(timeout=10):
                    raise AssertionError('synthetic HTTP was not released')

        def deliver():
            close_old_connections()
            try:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT pg_backend_pid()')
                    pid = cursor.fetchone()[0]
                return deliver_photo_notifications([self.log.pk]), pid
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as workers:
            with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                       side_effect=blocking_send) as send:
                first = workers.submit(deliver)
                try:
                    self.assertTrue(sending.wait(timeout=5))
                    self.log.refresh_from_db()
                    original_token = self.log.lease_token
                    AdminNotificationLog.objects.filter(pk=self.log.pk).update(
                        lease_expires_at=timezone.now() - timedelta(seconds=1))
                    second_result, second_pid = workers.submit(deliver).result(timeout=5)
                    self.log.refresh_from_db()
                    active_attempts, active_token = self.log.attempt_count, self.log.lease_token
                finally:
                    release.set()
                first_result, first_pid = first.result(timeout=5)
        self.assertNotEqual(first_pid, second_pid)
        self.assertEqual(second_result['sent'], 0)
        self.assertEqual(active_attempts, 1)
        self.assertEqual(active_token, original_token)
        self.assertEqual(first_result['sent'], 1)
        self.assertEqual(send.call_count, 1)
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'sent')

    def test_expired_worker_lease_is_recovered(self):
        with patch('miniapp.recognition_photo_notifications._send_one', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                deliver_photo_notifications([self.log.pk])
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'processing')
        self.assertEqual(self.log.attempt_count, 1)
        with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
            self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 0)
            AdminNotificationLog.objects.filter(pk=self.log.pk).update(
                lease_expires_at=timezone.now() - timedelta(seconds=1))
            self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 1)
        send.assert_called_once()
        self.log.refresh_from_db()
        self.assertEqual(self.log.attempt_count, 2)
        self.assertEqual(self.log.lease_token, '')
        self.assertIsNone(self.log.lease_expires_at)

    def test_final_crashed_lease_becomes_dead_without_an_extra_send(self):
        AdminNotificationLog.objects.filter(pk=self.log.pk).update(
            status='processing', attempt_count=6, lease_token='b' * 32,
            lease_expires_at=timezone.now() - timedelta(seconds=1))
        with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
            with self.assertLogs('miniapp.recognition_photo_notifications', level='ERROR') as logged:
                self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 0)
        send.assert_not_called()
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'dead')
        self.assertIn('recognition_photo_delivery_exhausted', logged.output[0])

    def test_stale_worker_cannot_finalize_a_reclaimed_lease(self):
        from miniapp.recognition_photo_notifications import _claim, _finish
        first, _ = _claim(self.log.pk, {9001})
        AdminNotificationLog.objects.filter(pk=self.log.pk).update(
            lease_expires_at=timezone.now() - timedelta(seconds=1))
        second, _ = _claim(self.log.pk, {9001})
        _finish(first, sent=True)
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'processing')
        self.assertEqual(self.log.lease_token, second.lease_token)
        _finish(second, sent=True)
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'sent')

    def test_retry_delays_increase_and_terminal_error_is_sanitized(self):
        now = timezone.now()
        with patch('miniapp.recognition_photo_notifications.send_telegram_document',
                   side_effect=OSError('DO NOT LEAK provider secrets')) as send:
            for seconds in (60, 120, 240, 480, 960):
                with patch('miniapp.recognition_photo_notifications.timezone.now', return_value=now):
                    self.assertEqual(deliver_photo_notifications([self.log.pk])['failed'], 1)
                self.log.refresh_from_db()
                self.assertEqual(self.log.next_retry_at, now + timedelta(seconds=seconds))
                now = self.log.next_retry_at
            with self.assertLogs('miniapp.recognition_photo_notifications', level='ERROR') as logged:
                with patch('miniapp.recognition_photo_notifications.timezone.now', return_value=now):
                    self.assertEqual(deliver_photo_notifications([self.log.pk])['failed'], 1)
        self.log.refresh_from_db()
        self.assertEqual(self.log.status, 'dead')
        self.assertIsNone(self.log.next_retry_at)
        self.assertEqual(send.call_count, 6)
        self.assertNotIn('DO NOT LEAK', self.log.error_message + str(logged.output))

    def test_deleted_capture_revokes_claim_before_http(self):
        from miniapp.recognition_photo_notifications import _read_original

        def read_then_delete(capture):
            data = _read_original(capture)
            capture.delete()
            return data

        with patch('miniapp.recognition_photo_notifications._read_original', side_effect=read_then_delete):
            with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
                self.assertEqual(deliver_photo_notifications([self.log.pk])['skipped'], 1)
        send.assert_not_called()
        self.assertFalse(AdminNotificationLog.objects.filter(pk=self.log.pk).exists())

    def test_missing_capture_or_media_reference_becomes_sanitized_terminal_row(self):
        past = timezone.now() - timedelta(seconds=1)
        original_key = self.capture.raw_media
        for missing in ('capture', 'media'):
            for status in ('queued', 'failed', 'processing'):
                with self.subTest(missing=missing, status=status):
                    RecognitionCapture.objects.filter(pk=self.capture.pk).update(
                        raw_media='' if missing == 'media' else original_key)
                    AdminNotificationLog.objects.filter(pk=self.log.pk).update(
                        payload={'capture_id': self.capture.pk + 1 if missing == 'capture' else self.capture.pk},
                        status=status, attempt_count=2, lease_token='c' * 32,
                        lease_expires_at=past, next_retry_at=past,
                    )
                    with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
                        self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 0)
                        self.log.refresh_from_db()
                        self.assertEqual(self.log.status, 'dead')
                        self.assertEqual(self.log.error_message, 'recognition_photo_capture_missing')
                        self.assertEqual(self.log.payload, {})
                        self.assertEqual(self.log.attempt_count, 2)
                        self.assertEqual(self.log.lease_token, '')
                        self.assertIsNone(self.log.lease_expires_at)
                        self.assertIsNone(self.log.next_retry_at)
                        self.assertEqual(deliver_photo_notifications([self.log.pk]),
                                         {'sent': 0, 'failed': 0, 'skipped': 0})
                    send.assert_not_called()

    def test_network_wait_does_not_block_capture_deletion(self):
        sending, release = Event(), Event()

        def blocking_send(**kwargs):
            sending.set()
            if not release.wait(timeout=10):
                raise AssertionError('synthetic HTTP was not released')

        def deliver():
            close_old_connections()
            try:
                return deliver_photo_notifications([self.log.pk])
            finally:
                connections.close_all()

        def delete_capture():
            close_old_connections()
            try:
                RecognitionCapture.objects.get(pk=self.capture.pk).delete()
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as workers:
            with patch('miniapp.recognition_photo_notifications.send_telegram_document', side_effect=blocking_send):
                sender = workers.submit(deliver)
                try:
                    self.assertTrue(sending.wait(timeout=5))
                    workers.submit(delete_capture).result(timeout=3)
                finally:
                    release.set()
                sender.result(timeout=5)
        self.assertFalse(AdminNotificationLog.objects.filter(pk=self.log.pk).exists())

    def test_supported_heif_and_gif_preserve_file_type_and_bytes(self):
        for mime, extension in [('image/heif', '.heif'), ('image/gif', '.gif')]:
            with self.subTest(mime=mime):
                RecognitionCapture.objects.filter(pk=self.capture.pk).update(mime_type=mime)
                AdminNotificationLog.objects.filter(pk=self.log.pk).update(status='queued')
                with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
                    self.assertEqual(deliver_photo_notifications([self.log.pk])['sent'], 1)
                self.assertEqual(send.call_args.kwargs['mime_type'], mime)
                self.assertTrue(send.call_args.kwargs['filename'].endswith(extension))
                self.assertEqual(send.call_args.kwargs['content'], self.original)

    def test_original_captured_while_paused_is_delivered_after_resume(self):
        from miniapp.models import RecognitionCaptureConfig
        from miniapp.recognition_quality import capture_attempt, update_capture
        from users.models import TelegramUser

        TelegramUser.objects.create(tg_user_id=42)
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        with patch('miniapp.recognition_photo_notifications.enqueue_photo_delivery'):
            with override_settings(ADMIN_NOTIFICATIONS_ENABLED=False):
                photo = capture_attempt(user_id=42, source='image', raw_bytes=b'paused-original', mime_type='image/png')
                self.assertIsNotNone(photo)
                update_capture(user_id=42, attempt_id=photo.attempt_id, outcome='failed')
                queued = AdminNotificationLog.objects.get(event_type='recognition_photo', payload__capture_id=photo.pk)
                with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
                    self.assertEqual(deliver_photo_notifications([queued.pk])['sent'], 0)
                send.assert_not_called()
                queued.refresh_from_db()
                self.assertEqual(queued.status, 'queued')
        with patch('miniapp.recognition_photo_notifications.send_telegram_document') as send:
            self.assertEqual(deliver_photo_notifications([queued.pk])['sent'], 1)
        self.assertEqual(send.call_args.kwargs['content'], b'paused-original')
        self.assertIn('outcome=failed', send.call_args.kwargs['caption'])
