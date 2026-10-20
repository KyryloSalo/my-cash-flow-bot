"""Private disk primitives. No public URLs, media logs or financial writes."""
from datetime import timedelta
import os
from pathlib import Path
import re
import stat
import uuid

from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from common.telegram import send_telegram_message
from miniapp.models import RecognitionCaptureConfig

MAX_STORAGE_BYTES = 1024 * 1024 * 1024
MAX_MEDIA_BYTES = 10 * 1024 * 1024
STORAGE_LOCK_ID = 731904127
ALERT_INTERVAL = timedelta(hours=24)
MEDIA_KEY = re.compile(r'^[0-9a-f]{32}\.bin$')


def _private_root():
    configured = getattr(settings, 'RECOGNITION_QUALITY_STORAGE_ROOT', '')
    if not configured or not Path(configured).is_absolute():
        raise OSError('Private recognition storage is not configured')
    path = Path(configured)
    if path.is_symlink():
        raise OSError('Private recognition root must not be a symlink')
    root = path.resolve()
    public_roots = [getattr(settings, 'STATIC_ROOT', ''), getattr(settings, 'MEDIA_ROOT', '')]
    public_roots.extend(getattr(settings, 'STATICFILES_DIRS', []))
    for public in public_roots:
        if public and root.is_relative_to(Path(public).resolve()):
            raise OSError('Recognition storage must be outside public directories')
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.name != 'nt':
        root.chmod(0o700)
    return root


def _locked_config():
    # One lock across all configs/workers, including weekly cohort replacement.
    # PostgreSQL releases it at the outermost transaction boundary.
    if connection.vendor == 'postgresql':
        with connection.cursor() as cursor:
            cursor.execute('SELECT pg_advisory_xact_lock(%s)', [STORAGE_LOCK_ID])
    return RecognitionCaptureConfig.objects.select_for_update().order_by('-pk').first()


def _disk_usage(root):
    used = 0
    with os.scandir(root) as entries:
        for entry in entries:
            try:
                info = entry.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode):
                raise OSError('Unexpected entry in private recognition storage')
            used += info.st_size
    return used


def _unlink_media(root, key):
    if not MEDIA_KEY.fullmatch(key or ''):
        return
    try:
        (root / key).unlink(missing_ok=True)
    except OSError:
        pass  # Counted against disk quota until a retention retry removes it.


def invalidate_private_exports():
    """Revoke cached review files after a capture/account deletion commits."""
    try:
        root = _private_root()
        with transaction.atomic():
            _locked_config()
            for path in root.glob('*.export.json'):
                if re.fullmatch(r'[0-9a-f]{32}\.export\.json', path.name):
                    path.unlink(missing_ok=True)
    except OSError:
        pass  # The hourly expiration sweep retries cleanup.


def _write_media(root, data):
    key = uuid.uuid4().hex + '.bin'
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(root / key, flags, 0o600)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        _unlink_media(root, key)
        raise
    return key


def _alert_message(payload):
    status = ('заповнено / новий файл не вміщується.' if payload['event_type'].endswith('_full')
              else 'використано щонайменше 90%.')
    return (
        f'Сховище розпізнавання: {status}'
        f'\nЗайнято: {payload["used_bytes"]} байт. Ліміт: {payload["quota_bytes"]} байт.'
        '\nРозпізнавання працює; файли понад ліміт не зберігаються.'
        f'\nЧас: {payload["timestamp"]}'
    )


def _enqueue_storage_alert(log_ids):
    try:
        from miniapp.tasks import deliver_recognition_storage_alerts_task
        deliver_recognition_storage_alerts_task.apply_async(args=[log_ids], retry=False)
    except Exception:
        pass  # Durable DB outbox is independently retried by beat every five minutes.


def deliver_storage_alerts(log_ids=None):
    """Retry sanitized alerts independently of captures; external delivery is at-least-once."""
    result = {'sent': 0, 'failed': 0}
    if not settings.ADMIN_NOTIFICATIONS_ENABLED or not settings.TELEGRAM_BOT_TOKEN:
        return result
    pending = AdminNotificationLog.objects.filter(
        event_type__in=('recognition_quality_storage_warning', 'recognition_quality_storage_full'),
        status__in=(AdminNotificationLog.Status.QUEUED, AdminNotificationLog.Status.FAILED),
        created_at__gte=timezone.now() - timedelta(days=7),
    )
    if log_ids is not None:
        pending = pending.filter(pk__in=log_ids)
    for pk in list(pending.order_by('pk').values_list('pk', flat=True)[:100]):
        with transaction.atomic():
            row = pending.select_for_update(skip_locked=True).filter(pk=pk).first()
            if not row or row.telegram_chat_id not in settings.ADMIN_TELEGRAM_IDS:
                continue
            payload = row.payload
            if set(payload) != {'event_type', 'used_bytes', 'quota_bytes', 'timestamp'}:
                continue
            try:
                send_telegram_message(bot_token=settings.TELEGRAM_BOT_TOKEN,
                                      chat_id=row.telegram_chat_id, text=_alert_message(payload), parse_mode=None)
            except Exception:
                row.status = AdminNotificationLog.Status.FAILED
                row.error_message = 'recognition_storage_delivery_failed'
                result['failed'] += 1
            else:
                row.status = AdminNotificationLog.Status.SENT
                row.sent_at = timezone.now()
                row.error_message = ''
                result['sent'] += 1
            row.save(update_fields=['status', 'sent_at', 'error_message'])
    return result


def _storage_alert(cfg, *, used, quota, full=False):
    if not full and used * 10 < quota * 9:
        return
    field = 'storage_full_at' if full or used >= quota else 'storage_warning_at'
    now = timezone.now()
    # Survives worker restarts and changing the weekly cohort config.
    recent = RecognitionCaptureConfig.objects.filter(**{field + '__gt': now - ALERT_INTERVAL}).exists()
    if recent:
        return
    recipients = set(settings.ADMIN_TELEGRAM_IDS)
    if not recipients:
        return
    setattr(cfg, field, now)
    cfg.save(update_fields=[field])
    event = 'recognition_quality_storage_' + ('full' if field == 'storage_full_at' else 'warning')
    payload = {'event_type': event, 'used_bytes': used, 'quota_bytes': quota, 'timestamp': now.isoformat()}
    rows = [AdminNotificationLog.objects.create(
        event_type=event, telegram_chat_id=chat_id, payload=payload,
        status=AdminNotificationLog.Status.QUEUED,
    ).pk for chat_id in recipients]
    transaction.on_commit(lambda: _enqueue_storage_alert(rows))
