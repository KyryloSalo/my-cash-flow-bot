"""Synthetic all-photo capture regressions; no production data or network."""
from pathlib import Path
from datetime import timedelta
import os
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, TestCase, override_settings
from django.utils import timezone
from admin_notifications.models import AdminNotificationLog

from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import capture_attempt, purge_expired, update_capture
from users.models import TelegramUser


class AllPhotoCaptureTests(TestCase):
    def setUp(self):
        TelegramUser.objects.create(tg_user_id=9007001)
        self.directory = TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.directory.cleanup)
        configured = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=self.directory.name)
        configured.enable()
        self.addCleanup(configured.disable)

    def test_all_photos_includes_new_users_outside_frozen_cohort(self):
        cfg = RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[])
        cfg.all_images_enabled = True
        cfg.save()
        photo = capture_attempt(user_id=9007001, source='image', raw_bytes=b'synthetic-original', mime_type='image/png')
        self.assertIsNotNone(photo, 'All-photo collection must include users outside the frozen cohort')
        self.assertEqual((Path(self.directory.name) / photo.raw_media).read_bytes(), b'synthetic-original')
        self.assertIsNone(capture_attempt(user_id=9007001, source='text', raw_text='not opted in'))
        self.assertIsNone(capture_attempt(user_id=9007001, source='voice', raw_bytes=b'not opted in'))

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[99001])
    def test_original_photo_has_durable_admin_outbox_without_data_copy(self):
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        photo = capture_attempt(user_id=9007001, source='image', raw_bytes=b'synthetic-original', mime_type='image/png')
        queued = AdminNotificationLog.objects.filter(event_type='recognition_photo')
        self.assertEqual(queued.count(), 1, 'Every original photo needs a durable admin notification')
        self.assertEqual(queued.get().telegram_chat_id, 99001)
        self.assertEqual(queued.get().payload, {'capture_id': photo.pk})

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[99001])
    def test_deleting_capture_removes_pending_notification_reference(self):
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        photo = capture_attempt(user_id=9007001, source='image', raw_bytes=b'synthetic-original')
        photo.delete()
        self.assertFalse(AdminNotificationLog.objects.filter(event_type='recognition_photo').exists())

    def test_expired_text_cohort_does_not_purge_recent_all_photo_samples(self):
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True,
            expires_at=timezone.now() - timedelta(days=2), retention_days=7)
        photo = capture_attempt(user_id=9007001, source='image', raw_bytes=b'synthetic-original')
        RecognitionCapture.objects.filter(pk=photo.pk).update(created_at=timezone.now() - timedelta(days=3))
        purge_expired()
        self.assertTrue(RecognitionCapture.objects.filter(pk=photo.pk).exists())

    def test_preprocessing_failure_still_captures_the_original_photo(self):
        from miniapp import views
        from miniapp.image_uploads import MiniAppImageUploadError

        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        user = TelegramUser.objects.get(pk=9007001)
        original = b'\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1heic'
        request = RequestFactory().post('/app/api/ai-image/parse', data={
            'image': SimpleUploadedFile('receipt.heic', original, content_type='image/heic')})
        with patch.object(views, '_authorized_write_user_or_blocked', return_value=(user, {})), \
             patch.object(views, 'normalize_ai_image_upload', side_effect=MiniAppImageUploadError('synthetic')):
            response = views.ai_image_parse(request)
        self.assertEqual(response.status_code, 400)
        photo = RecognitionCapture.objects.get()
        self.assertEqual((Path(self.directory.name) / photo.raw_media).read_bytes(), original)
        self.assertEqual(photo.outcome, 'failed')
        self.assertEqual(photo.error_code, 'image_preprocessing_failed')

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[99001])
    def test_new_photo_and_parse_outcome_schedule_delivery(self):
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        with patch('miniapp.recognition_photo_notifications.enqueue_photo_delivery') as enqueue:
            photo = capture_attempt(user_id=9007001, source='image', raw_bytes=b'synthetic-original')
            enqueue.assert_called_once_with(photo.pk)
            enqueue.reset_mock()
            update_capture(user_id=photo.user_id, attempt_id=photo.attempt_id,
                outcome='failed', error_code='ai_image_unavailable')
            enqueue.assert_called_once_with(photo.pk)

    def test_photo_delivery_has_periodic_worker_recovery(self):
        from django.conf import settings
        from miniapp import tasks

        self.assertTrue(hasattr(tasks, 'deliver_recognition_photos_task'))
        configured = [row['task'] for row in settings.CELERY_BEAT_SCHEDULE.values()]
        self.assertIn('miniapp.deliver_recognition_photos', configured)
