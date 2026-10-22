"""Fixed-purpose production operator. Output is operational metadata only."""
import json
import shutil
from io import StringIO

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management import BaseCommand, CommandError, call_command
from django.db import transaction
from django.utils import timezone

from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_quality import _authorized_export
from miniapp.recognition_storage import (
    MAX_STORAGE_BYTES, _disk_usage, _locked_config, _private_root, _write_media,
)
from common.telegram import send_telegram_message


class Command(BaseCommand):
    help = 'Operate recognition capture without exporting samples or configuration values.'

    def add_arguments(self, parser):
        parser.add_argument('--action', choices=('status', 'activate', 'disable'), required=True)

    def _readiness(self):
        checks = {
            'private_root': False, 'disk_capacity': False,
            'admin_notifications': bool(settings.ADMIN_NOTIFICATIONS_ENABLED),
            'notification_recipients': bool(settings.ADMIN_TELEGRAM_IDS),
            'bot_configured': bool(settings.TELEGRAM_BOT_TOKEN),
            'private_export_authorized': False,
        }
        try:
            root = _private_root()
            used = _disk_usage(root)
            checks['private_root'] = True
            checks['disk_capacity'] = used < MAX_STORAGE_BYTES and shutil.disk_usage(root).free >= MAX_STORAGE_BYTES - used
        except OSError:
            pass
        admin = get_user_model().objects.filter(is_active=True, is_superuser=True).order_by('pk').first()
        if admin:
            checks['private_export_authorized'] = _authorized_export(
                admin.pk, getattr(settings, 'RECOGNITION_QUALITY_EXPORT_TOKEN', ''))
        return checks

    def _status(self, action, readiness=None):
        cfg = RecognitionCaptureConfig.objects.order_by('-pk').first()
        return {
            'action': action,
            'enabled': bool(cfg and cfg.enabled and (cfg.all_images_enabled or (cfg.expires_at and cfg.expires_at > timezone.now()))),
            'all_images_enabled': bool(cfg and cfg.enabled and cfg.all_images_enabled),
            'cohort_size': len(cfg.cohort_ids or []) if cfg else 0,
            'expires_at': cfg.expires_at.isoformat() if cfg and cfg.expires_at else None,
            'captures': RecognitionCapture.objects.count(),
            'quota_bytes': min(cfg.max_total_bytes, MAX_STORAGE_BYTES) if cfg else MAX_STORAGE_BYTES,
            'readiness': readiness or {},
        }

    def _activate(self, readiness):
        if not all(readiness.values()):
            raise CommandError('recognition_operator_not_ready')
        with transaction.atomic():
            cfg = _locked_config()
            if cfg and cfg.enabled and cfg.all_images_enabled:
                return  # Idempotent; never extend the text/voice cohort or expiry.
            root = _private_root()
            used = _disk_usage(root)
            probe_bytes = b'synthetic recognition operator probe'
            if used + len(probe_bytes) > MAX_STORAGE_BYTES:
                raise CommandError('recognition_operator_not_ready')
            key = _write_media(root, probe_bytes)
            try:
                if (root / key).read_bytes() != probe_bytes:
                    raise OSError('storage probe failed')
            finally:
                (root / key).unlink(missing_ok=True)
            # A real bounded delivery probe, containing no user or recognition data.
            for recipient in settings.ADMIN_TELEGRAM_IDS:
                send_telegram_message(
                    bot_token=settings.TELEGRAM_BOT_TOKEN, chat_id=recipient,
                    text='Перевірка готовності приватного збору якості розпізнавання: канал сповіщень доступний.',
                    parse_mode=None,
                )
            call_command('enable_recognition_quality', '--apply', '--all-photos', '--days', '7', stdout=StringIO())

    def handle(self, *args, **options):
        action = options['action']
        try:
            if action == 'disable':
                call_command('enable_recognition_quality', '--disable', stdout=StringIO())
                result = self._status(action)
            else:
                readiness = self._readiness()
                if action == 'activate':
                    self._activate(readiness)
                result = self._status(action, readiness)
        except CommandError as exc:
            if str(exc) == 'recognition_operator_not_ready':
                raise CommandError('recognition_operator_not_ready') from None
            raise CommandError('recognition_operator_failed') from None
        except Exception:
            raise CommandError('recognition_operator_failed') from None
        self.stdout.write(json.dumps(result, separators=(',', ':')))
