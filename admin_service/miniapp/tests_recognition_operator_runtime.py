"""Synthetic operator tests; never contacts production or Telegram."""
import json
import os
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command, CommandError
from django.test import TestCase, override_settings

from miniapp.models import RecognitionCaptureConfig
from users.models import TelegramUser


@override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[9001],
                   TELEGRAM_BOT_TOKEN='synthetic-only', RECOGNITION_QUALITY_EXPORT_TOKEN='')
class RecognitionQualityOperatorRuntimeTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        configured = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root))
        configured.enable()
        self.addCleanup(configured.disable)
        self.admin = get_user_model().objects.create_superuser('operator', 'synthetic@example.invalid', 'synthetic-only')
        TelegramUser.objects.create(tg_user_id=7001)
        self.initial_cohort = sorted(TelegramUser.objects.values_list('tg_user_id', flat=True))
        sender = patch('miniapp.management.commands.recognition_quality_operator.send_telegram_message')
        # Start lazily: the RED run must fail on the missing command, not patch import.
        self.sender = sender

    def run_operator(self, action):
        output = StringIO()
        call_command('recognition_quality_operator', action=action, stdout=output)
        return json.loads(output.getvalue())

    def test_status_is_json_and_never_prints_paths_user_ids_or_tokens(self):
        result = self.run_operator('status')
        self.assertFalse(result['enabled'])
        self.assertEqual(result['captures'], 0)
        self.assertTrue(all(result['readiness'].values()))
        for forbidden in ('7001', '9001', 'synthetic-only', str(self.root), 'cohort_ids'):
            self.assertNotIn(forbidden, json.dumps(result))

    def test_activate_freezes_all_current_users_and_probes_private_storage(self):
        with self.sender as send:
            result = self.run_operator('activate')
            send.assert_called_once()
        self.assertTrue(result['enabled'])
        self.assertTrue(result['all_images_enabled'])
        cfg = RecognitionCaptureConfig.objects.get(enabled=True)
        self.assertEqual(cfg.cohort_ids, self.initial_cohort)
        self.assertEqual(cfg.max_total_bytes, 1073741824)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_repeated_activation_preserves_cohort_and_expiry(self):
        with self.sender:
            first = self.run_operator('activate')
            TelegramUser.objects.create(tg_user_id=8001)
            second = self.run_operator('activate')
        self.assertEqual(first['expires_at'], second['expires_at'])
        self.assertEqual(second['cohort_size'], len(self.initial_cohort))
        self.assertEqual(RecognitionCaptureConfig.objects.count(), 1)

    def test_each_missing_prerequisite_prevents_activation(self):
        for setting in ({'RECOGNITION_QUALITY_STORAGE_ROOT': ''}, {'ADMIN_NOTIFICATIONS_ENABLED': False},
                        {'ADMIN_TELEGRAM_IDS': []}, {'TELEGRAM_BOT_TOKEN': ''},
                        {'STATIC_ROOT': str(self.root)}):
            with self.subTest(setting=list(setting)), override_settings(**setting):
                with self.assertRaisesMessage(CommandError, 'recognition_operator_not_ready'):
                    self.run_operator('activate')
                self.assertFalse(RecognitionCaptureConfig.objects.filter(enabled=True).exists())

    def test_delivery_failure_is_sanitized_and_does_not_activate(self):
        with self.sender as send:
            send.side_effect = RuntimeError('synthetic-secret-and-private-text')
            with self.assertRaisesRegex(CommandError, '^recognition_operator_failed$'):
                self.run_operator('activate')
        self.assertFalse(RecognitionCaptureConfig.objects.filter(enabled=True).exists())

    def test_active_superuser_and_private_export_authorization_are_required(self):
        self.admin.is_active = False
        self.admin.save(update_fields=['is_active'])
        result = self.run_operator('status')
        self.assertFalse(result['readiness']['private_export_authorized'])
        with self.assertRaisesMessage(CommandError, 'recognition_operator_not_ready'):
            self.run_operator('activate')

    def test_disable_is_idempotent_even_when_prerequisites_are_unavailable(self):
        RecognitionCaptureConfig.objects.create(enabled=True, cohort_ids=[7001])
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT='', ADMIN_NOTIFICATIONS_ENABLED=False):
            self.assertFalse(self.run_operator('disable')['enabled'])
            self.assertFalse(self.run_operator('disable')['enabled'])

    def test_status_sanitizes_unexpected_database_errors(self):
        with patch('miniapp.management.commands.recognition_quality_operator.RecognitionCaptureConfig.objects.order_by',
                   side_effect=RuntimeError('synthetic-secret-and-private-text')):
            with self.assertRaisesRegex(CommandError, '^recognition_operator_failed$'):
                self.run_operator('status')
