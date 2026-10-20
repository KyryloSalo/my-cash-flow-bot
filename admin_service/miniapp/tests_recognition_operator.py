"""Synthetic operator acceptance; never contacts production or Telegram."""
from datetime import timedelta
from io import StringIO
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings
from django.utils import timezone

from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import export_weekly, purge_expired
from users.models import TelegramUser


@override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[9001],
                   TELEGRAM_BOT_TOKEN='synthetic-only', RECOGNITION_QUALITY_EXPORT_TOKEN='')
class RecognitionOperatorTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        configured = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root))
        configured.enable()
        self.addCleanup(configured.disable)
        self.admin = get_user_model().objects.create_superuser('operator', 'synthetic@example.invalid', 'synthetic-only')

    def enable(self, *args):
        output = StringIO()
        call_command('enable_recognition_quality', *args, stdout=output)
        return output.getvalue()

    @override_settings(RECOGNITION_QUALITY_EXPORT_TOKEN='synthetic-only')
    def test_export_rejects_nonexistent_inactive_and_staff_requesters(self):
        staff = get_user_model().objects.create_user('staff', is_staff=True)
        for requester in (None, 999999, staff.pk):
            with self.subTest(requester=requester), self.assertRaises(PermissionError):
                export_weekly(requester_id=requester, export_token='synthetic-only')
        self.admin.is_active = False
        self.admin.save(update_fields=['is_active'])
        with self.assertRaises(PermissionError):
            export_weekly(requester_id=self.admin.pk)

    def test_export_requires_configured_token_as_well_as_role(self):
        with override_settings(RECOGNITION_QUALITY_EXPORT_TOKEN='synthetic-export'):
            for token in (None, 'wrong'):
                with self.assertRaises(PermissionError):
                    export_weekly(requester_id=self.admin.pk, export_token=token)
            self.assertEqual(export_weekly(requester_id=self.admin.pk, export_token='synthetic-export')['items'], [])

    def test_active_superuser_can_export_without_shared_token(self):
        self.assertEqual(export_weekly(requester_id=self.admin.pk)['items'], [])

    def test_apply_checks_each_prerequisite_before_database_write(self):
        TelegramUser.objects.create(tg_user_id=7001)
        missing = [dict(RECOGNITION_QUALITY_STORAGE_ROOT=''),
                   dict(ADMIN_NOTIFICATIONS_ENABLED=False), dict(ADMIN_TELEGRAM_IDS=[]),
                   dict(TELEGRAM_BOT_TOKEN=''), dict(STATIC_ROOT=str(self.root))]
        for values in missing:
            with self.subTest(values=list(values)), override_settings(**values):
                with self.assertRaises(CommandError):
                    self.enable('--apply')
                self.assertFalse(RecognitionCaptureConfig.objects.exists())

    def test_all_current_users_are_frozen_not_only_admins(self):
        existing = set(TelegramUser.objects.values_list('tg_user_id', flat=True))
        TelegramUser.objects.create(tg_user_id=7001)
        TelegramUser.objects.create(tg_user_id=8001)
        with override_settings(ADMIN_TEST_TELEGRAM_IDS=[7001]):
            self.enable('--apply')
        cfg = RecognitionCaptureConfig.objects.get(enabled=True)
        self.assertEqual(cfg.cohort_ids, sorted(existing | {7001, 8001}))
        self.assertLess(abs((cfg.expires_at - timezone.now() - timedelta(days=7)).total_seconds()), 10)
        TelegramUser.objects.create(tg_user_id=9001)
        with self.assertRaises(CommandError):
            self.enable('--apply')
        cfg.refresh_from_db()
        self.assertEqual(cfg.cohort_ids, sorted(existing | {7001, 8001}))
        self.assertEqual(RecognitionCaptureConfig.objects.count(), 1)

    def test_disable_and_status_are_idempotent_and_sanitized(self):
        self.enable('--disable')
        self.enable('--disable')
        self.assertIn('enabled=false', self.enable('--status').lower())
        self.assertFalse(RecognitionCaptureConfig.objects.filter(enabled=True).exists())

    def test_export_cli_is_private_and_stdout_contains_only_operational_metadata(self):
        RecognitionCapture.objects.create(user_id=7001, source='text', raw_text='synthetic-private-text')
        output = StringIO()
        call_command('recognition_quality_export', '--requester-id', str(self.admin.pk), stdout=output)
        self.assertNotIn('synthetic-private-text', output.getvalue())
        files = list(self.root.glob('*.export.json'))
        self.assertEqual(len(files), 1)
        report = json.loads(files[0].read_text(encoding='utf-8'))
        self.assertEqual(report['items'][0]['raw_text'], 'synthetic-private-text')
        if os.name != 'nt':
            self.assertEqual(files[0].stat().st_mode & 0o777, 0o600)

    def test_export_cli_cannot_bypass_disk_quota(self):
        RecognitionCaptureConfig.objects.create(enabled=False, max_total_bytes=1)
        with self.assertRaises(CommandError):
            call_command('recognition_quality_export', '--requester-id', str(self.admin.pk), stdout=StringIO())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_export_files_are_removed_by_retention_cleanup(self):
        RecognitionCaptureConfig.objects.create(enabled=False)
        old = self.root / ('a' * 32 + '.export.json')
        old.write_text('{}', encoding='utf-8')
        old_time = (timezone.now() - timedelta(hours=2)).timestamp()
        os.utime(old, (old_time, old_time))
        with self.captureOnCommitCallbacks(execute=True):
            purge_expired()
        self.assertFalse(old.exists())

    def test_capture_deletion_invalidates_private_exports_after_commit(self):
        capture = RecognitionCapture.objects.create(user_id=7001, source='text', raw_text='private example')
        call_command('recognition_quality_export', '--requester-id', str(self.admin.pk), stdout=StringIO())
        target = next(self.root.glob('*.export.json'))
        with self.captureOnCommitCallbacks(execute=True):
            capture.delete()
            self.assertTrue(target.exists())
        self.assertFalse(target.exists())
