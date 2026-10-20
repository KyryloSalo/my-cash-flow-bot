from datetime import timedelta
from io import BytesIO
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from django.core.files.uploadedfile import SimpleUploadedFile
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.utils import timezone
from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import capture_attempt, export_weekly
from users.models import TelegramUser


@override_settings(RECOGNITION_QUALITY_EXPORT_TOKEN='test-token')
class RecognitionQualityCaptureTests(TestCase):
    def setUp(self):
        TelegramUser.objects.create(tg_user_id=7)
        self.operator = get_user_model().objects.create_superuser('recognition-operator', password='synthetic-only')
        self.directory = TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.directory.cleanup)
        configured = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=self.directory.name)
        configured.enable()
        self.addCleanup(configured.disable)

    def test_disabled_capture_is_fail_open(self):
        RecognitionCaptureConfig.objects.create(enabled=False, cohort_ids=[1])
        capture_attempt(user_id=1, source='text', raw_text='x', provider_result={'a': 1})
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_capture_persists_raw_and_correlation(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7], expires_at=timezone.now()+timedelta(days=7))
        capture_attempt(user_id=7, source='audio', raw_bytes=b'abc', mime_type='audio/webm', provider='mock', provider_result={'transcript':'ok'}, normalized_result={'mode':'expense'}, attempt_id='attempt-1')
        row = RecognitionCapture.objects.get()
        self.assertEqual((Path(self.directory.name) / row.raw_media).read_bytes(), b'abc')
        self.assertEqual(row.attempt_id, 'attempt-1')

    def test_expired_and_non_cohort_are_excluded(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7], expires_at=timezone.now()-timedelta(seconds=1))
        capture_attempt(user_id=7, source='text', raw_text='x')
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_export_requires_auth_and_never_includes_raw_by_default(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7])
        capture_attempt(user_id=7, source='audio', raw_bytes=b'secret')
        with self.assertRaises(PermissionError): export_weekly()
        result = export_weekly(requester_id=self.operator.pk, export_token='test-token')
        self.assertNotIn('raw_media', result['items'][0])

    def test_update_capture_is_user_scoped(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7])
        capture_attempt(user_id=7, source='text', attempt_id='u1')
        from miniapp.recognition_quality import update_capture
        self.assertIsNone(update_capture(user_id=8, attempt_id='u1', outcome='confirmed'))
        self.assertEqual(update_capture(user_id=7, attempt_id='u1', outcome='confirmed').outcome, 'confirmed')

    def test_existing_attempt_can_finish_after_disable_expiry_or_cohort_change(self):
        from miniapp.recognition_quality import update_capture

        cfg = RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7])
        for index, change in enumerate((
            {'enabled': False},
            {'expires_at': timezone.now() - timedelta(seconds=1)},
            {'cohort_ids': []},
        )):
            with self.subTest(change=change):
                cfg.enabled, cfg.expires_at, cfg.cohort_ids = True, None, [7]
                cfg.save()
                attempt_id = f'lifecycle-{index}'
                capture_attempt(user_id=7, source='text', attempt_id=attempt_id)
                for field, value in change.items():
                    setattr(cfg, field, value)
                cfg.save()
                self.assertIsNone(update_capture(user_id=8, attempt_id=attempt_id, outcome='confirmed'))
                row = update_capture(user_id=7, attempt_id=attempt_id, outcome='confirmed', correction={'amount': '12'})
                self.assertIsNotNone(row)
                row.refresh_from_db()
                self.assertEqual(row.outcome, 'confirmed')
                self.assertEqual(row.correction, {'amount': '12'})
                self.assertIsNone(capture_attempt(user_id=7, source='text', attempt_id=f'new-{index}'))
    def test_export_is_bounded_and_reports_unreviewed(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7])
        capture_attempt(user_id=7, source='text', raw_text='x', provider_result={'mode':'expense'})
        result = export_weekly(requester_id=self.operator.pk, export_token='test-token', limit=10)
        self.assertEqual(result['counts']['captures'], 1)
        self.assertEqual(result['quality']['unreviewed'], 1)
        self.assertNotIn('accurate', result['quality'])

    def test_failed_provider_update_persists_technical_error(self):
        from miniapp.recognition_quality import update_capture

        RecognitionCaptureConfig.objects.create(
            enabled=True, cohort_ids=[7], expires_at=timezone.now() + timedelta(days=7)
        )
        capture_attempt(user_id=7, source='text', raw_text='synthetic', attempt_id='provider-failure')
        row = update_capture(
            user_id=7, attempt_id='provider-failure', outcome='failed', error_code='ai_text_unavailable'
        )
        self.assertIsNotNone(row)
        row.refresh_from_db()
        self.assertEqual(row.outcome, 'failed')
        self.assertEqual(row.error_code, 'ai_text_unavailable')
