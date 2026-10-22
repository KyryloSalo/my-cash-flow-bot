from datetime import timedelta
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from users.models import TelegramUser
from miniapp.models import RecognitionCaptureConfig
from miniapp.recognition_storage import _locked_config, _private_root

class Command(BaseCommand):
    help = 'Enable the frozen recognition cohort, optionally collecting all user photos until disabled.'
    def add_arguments(self, p):
        group = p.add_mutually_exclusive_group()
        group.add_argument('--apply', action='store_true')
        group.add_argument('--disable', action='store_true')
        group.add_argument('--status', action='store_true')
        p.add_argument('--days', type=int, default=7)
        p.add_argument('--all-photos', action='store_true')
    def handle(self, *args, **opts):
        if opts['disable']:
            with transaction.atomic():
                _locked_config()
                count = RecognitionCaptureConfig.objects.filter(enabled=True).update(enabled=False)
            self.stdout.write(f'DISABLED configs={count}')
            return
        if opts['status']:
            cfg = RecognitionCaptureConfig.objects.order_by('-pk').first()
            enabled = bool(cfg and cfg.enabled and (cfg.all_images_enabled or (cfg.expires_at and cfg.expires_at > timezone.now())))
            self.stdout.write(f'STATUS enabled={str(enabled).lower()} cohort_count={len(cfg.cohort_ids or []) if cfg else 0}')
            return
        if opts['days'] != 7: raise CommandError('--days must be exactly 7')
        try:
            _private_root()
        except OSError:
            raise CommandError('private persistent root is missing or unsafe') from None
        if not settings.ADMIN_NOTIFICATIONS_ENABLED or not settings.ADMIN_TELEGRAM_IDS or not settings.TELEGRAM_BOT_TOKEN:
            raise CommandError('admin notification prerequisites are missing')
        ids = list(TelegramUser.objects.order_by('tg_user_id').values_list('tg_user_id', flat=True))
        if not ids: raise CommandError('Refusing empty cohort: no current users found')
        expires = timezone.now() + timedelta(days=7)
        if opts['apply']:
            with transaction.atomic():
                existing = _locked_config()
                if existing and opts['all_photos'] and existing.enabled:
                    existing.all_images_enabled = True
                    existing.save(update_fields=['all_images_enabled'])
                    self.stdout.write(f'APPLIED config={existing.pk} all_images_enabled=true')
                    return
                if existing and existing.enabled and existing.expires_at and existing.expires_at > timezone.now():
                    raise CommandError('An active frozen cohort already exists; disable before replacing')
                RecognitionCaptureConfig.objects.update(enabled=False)
                cfg = RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=opts['all_photos'], cohort_ids=[int(x) for x in ids], expires_at=expires)
            self.stdout.write(f'APPLIED config={cfg.pk} cohort_count={len(ids)} expires_at={expires.isoformat()}')
        else:
            self.stdout.write(f'DRY-RUN cohort_count={len(ids)} expires_at={expires.isoformat()} apply_with=--apply')
