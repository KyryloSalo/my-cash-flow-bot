from __future__ import annotations
import hmac
import base64
import re
import uuid
from datetime import timedelta
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from users.models import TelegramUser
from .models import RecognitionCapture, RecognitionCaptureConfig
from .recognition_storage import (
    MAX_MEDIA_BYTES, MAX_STORAGE_BYTES, MEDIA_KEY, _disk_usage, _locked_config,
    _private_root, _storage_alert, _unlink_media, _write_media,
)
MAX_EXPORT_LIMIT = 1000

def _authorized(token):
    expected = getattr(settings, 'RECOGNITION_QUALITY_EXPORT_TOKEN', '')
    return bool(expected and token and hmac.compare_digest(str(expected), str(token)))

def _authorized_export(requester_id, token):
    try:
        operator = get_user_model().objects.get(pk=requester_id, is_active=True, is_superuser=True)
    except (get_user_model().DoesNotExist, TypeError, ValueError):
        return False
    expected = getattr(settings, 'RECOGNITION_QUALITY_EXPORT_TOKEN', '')
    return bool(operator and (not expected or _authorized(token)))

def capture_attempt(*, user_id, source, raw_text='', raw_bytes=None, mime_type='', provider='', provider_result=None, normalized_result=None, attempt_id=''):
    root = None
    key = ''
    try:
        raw_bytes = raw_bytes or b''
        with transaction.atomic():
            # Same row lock and ordering as hard_delete_user: account first.
            # If deletion wins, PostgreSQL rechecks the row and no capture is made.
            if not TelegramUser.objects.select_for_update().filter(pk=int(user_id)).exists():
                return None
            cfg = _locked_config()
            now = timezone.now()
            cohort = {int(x) for x in (cfg.cohort_ids or [])} if cfg else set()
            if not cfg or not cfg.enabled:
                return None
            all_images = source == 'image' and cfg.all_images_enabled
            if not all_images and ((cfg.expires_at and cfg.expires_at <= now) or int(user_id) not in cohort):
                return None
            if len(raw_bytes) > min(cfg.max_bytes_per_file, MAX_MEDIA_BYTES): return None
            if raw_bytes:
                root = _private_root()
                used = _disk_usage(root)
                quota = min(cfg.max_total_bytes, MAX_STORAGE_BYTES)
                if used + len(raw_bytes) > quota:
                    _storage_alert(cfg, used=used, quota=quota, full=True)
                    return None
                key = _write_media(root, raw_bytes)
                _storage_alert(cfg, used=used + len(raw_bytes), quota=quota)
            row = RecognitionCapture.objects.create(
                attempt_id=attempt_id or uuid.uuid4().hex, user_id=int(user_id), source=source,
                raw_text=(raw_text or '')[:20000], raw_media=key, raw_media_size=len(raw_bytes),
                mime_type=mime_type[:80], provider=provider[:80], provider_result=provider_result or {},
                normalized_result=normalized_result or {})
            if source == 'image' and key and settings.ADMIN_NOTIFICATIONS_ENABLED:
                from admin_notifications.models import AdminNotificationLog
                for recipient in set(settings.ADMIN_TELEGRAM_IDS):
                    AdminNotificationLog.objects.create(
                        event_type='recognition_photo', telegram_chat_id=int(recipient),
                        payload={'capture_id': row.pk},
                    )
                from miniapp.recognition_photo_notifications import enqueue_photo_delivery

                enqueue_photo_delivery(row.pk)
            return row
    except Exception:
        if root and key:
            _unlink_media(root, key)
        return None

def update_capture(*, user_id, attempt_id, outcome, correction=None, provider_result=None, normalized_result=None, error_code=None):
    try:
        with transaction.atomic():
            # Disable/expiry gates new collection, not completion of an owned attempt.
            # A deleted attempt is never recreated by this update-only path.
            row = RecognitionCapture.objects.select_for_update().filter(user_id=int(user_id), attempt_id=attempt_id).first()
            if not row: return None
            row.outcome = outcome
            if correction is not None: row.correction = correction
            if provider_result is not None: row.provider_result = provider_result
            if normalized_result is not None: row.normalized_result = normalized_result
            if error_code is not None: row.error_code = str(error_code)[:128]
            row.reviewed_at = timezone.now()
            row.save(update_fields=['outcome', 'correction', 'provider_result', 'normalized_result', 'error_code', 'reviewed_at'])
            if row.source == 'image' and row.raw_media:
                from miniapp.recognition_photo_notifications import enqueue_photo_delivery

                enqueue_photo_delivery(row.pk)
            return row
    except Exception: return None


def export_weekly(*, requester_id=None, export_token=None, start=None, end=None, limit=1000, include_raw=False):
    if not _authorized_export(requester_id, export_token):
        raise PermissionError('authenticated protected export required')
    end = end or timezone.now()
    start = start or end - timedelta(days=7)
    limit = max(1, min(int(limit), MAX_EXPORT_LIMIT))
    qs = RecognitionCapture.objects.filter(created_at__gte=start, created_at__lt=end).order_by('created_at', 'id')
    total = qs.count()
    rows = list(qs[:limit])
    items = []
    raw_export_bytes = 0
    for r in rows:
        item = {'id': r.id, 'attempt_id': r.attempt_id, 'user_id': r.user_id, 'source': r.source,
                'provider': r.provider, 'provider_result': r.provider_result,
                'normalized_result': r.normalized_result, 'correction': r.correction, 'outcome': r.outcome}
        if include_raw:
            media = None
            if r.raw_media:
                if not MEDIA_KEY.fullmatch(r.raw_media):
                    raise ValueError('Invalid private media key')
                path = _private_root() / r.raw_media
                if path.is_symlink():
                    raise OSError('Invalid private media file')
                with path.open('rb') as handle:
                    data = handle.read(MAX_MEDIA_BYTES + 1)
                raw_export_bytes += len(data)
                if len(data) > MAX_MEDIA_BYTES or raw_export_bytes > 2 * MAX_MEDIA_BYTES:
                    raise ValueError('Raw export exceeds byte limit; use a smaller page')
                media = base64.b64encode(data).decode('ascii')
            item.update(raw_text=r.raw_text, raw_media=media, mime_type=r.mime_type)
        items.append(item)
    reviewed = qs.filter(reviewed_at__isnull=False).count()
    unreviewed = qs.filter(reviewed_at__isnull=True).count()
    return {'counts': {'captures': total, 'by_source': {s: qs.filter(source=s).count() for s in ('text', 'voice', 'image')},
                       'reviewed': reviewed, 'unreviewed': unreviewed},
            'has_more': total > len(rows), 'quality': {'reviewed': reviewed, 'unreviewed': unreviewed}, 'items': items}

def purge_expired(*, now=None):
    now = now or timezone.now()
    with transaction.atomic():
        cfg = _locked_config()
        if not cfg:
            return 0
        cutoff = now - timedelta(days=min(cfg.retention_days, 7))
        expired = Q(created_at__lte=cutoff)
        if cfg.expires_at and cfg.expires_at <= now:
            cohort_expired = Q(created_at__lte=cfg.expires_at)
            if cfg.all_images_enabled:
                cohort_expired &= ~Q(source='image')
            expired |= cohort_expired
        deleted = RecognitionCapture.objects.filter(expired).delete()[0]
        # Crash/rollback orphans still consume quota until this sweep removes them.
        if getattr(settings, 'RECOGNITION_QUALITY_STORAGE_ROOT', ''):
            root = _private_root()
            for path in root.iterdir():
                if (re.fullmatch(r'[0-9a-f]{32}\.export\.json', path.name) and not path.is_symlink()
                        and path.stat().st_mtime <= (now - timedelta(hours=1)).timestamp()):
                    transaction.on_commit(lambda file=path: file.unlink(missing_ok=True))
                if (MEDIA_KEY.fullmatch(path.name) and not path.is_symlink()
                        and path.stat().st_mtime <= cutoff.timestamp()
                        and not RecognitionCapture.objects.filter(raw_media=path.name).exists()):
                    transaction.on_commit(lambda key=path.name: _unlink_media(root, key))
        return deleted

def delete_user_captures(user_id): return RecognitionCapture.objects.filter(user_id=int(user_id)).delete()[0]
