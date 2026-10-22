from __future__ import annotations

from contextlib import contextmanager
from datetime import timedelta
import logging
from pathlib import Path
import re
from uuid import uuid4

from django.conf import settings
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from common.telegram_upload import send_telegram_document

logger = logging.getLogger(__name__)
_EVENT = "recognition_photo"
_KEY = re.compile(r"^[0-9a-f]{32}\.bin$")
_MIME_EXTENSIONS = {"image/heic": ".heic", "image/heif": ".heif", "image/gif": ".gif",
                    "image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}
_MAX_ATTEMPTS = 6
_LEASE_DURATION = timedelta(minutes=5)
_RETRY_BASE_SECONDS = 60


def _caption(capture) -> str:
    outcome = capture.outcome or "pending"
    if outcome not in {"parsed", "failed", "pending", "confirmed", "cancelled", "redirect", "draft"}:
        outcome = "unknown"
    result = capture.normalized_result if isinstance(capture.normalized_result, dict) else {}
    amount = result.get("amount") if isinstance(result.get("amount"), (int, float, str)) else ""
    currency = result.get("currency") if isinstance(result.get("currency"), str) else ""
    mode = capture.source if capture.source in {"photo", "image", "camera", "batch"} else "photo"
    parts = [f"tg_user_id={capture.user_id}", f"outcome={outcome}", f"mode={mode}"]
    if amount: parts.append(f"amount={str(amount)[:32]}")
    if currency: parts.append(f"currency={currency[:8]}")
    if isinstance(result.get("item_count"), int): parts.append(f"item_count={result['item_count']}")
    return " ".join(parts)[:1024]


def _read_original(capture) -> bytes:
    from miniapp.recognition_storage import MAX_MEDIA_BYTES, MEDIA_KEY, _private_root
    key = capture.raw_media or ""
    if not MEDIA_KEY.fullmatch(key) or not _KEY.fullmatch(key):
        raise ValueError("invalid_media_key")
    root = _private_root()
    path = root / key
    if path.is_symlink() or path.parent != root:
        raise ValueError("invalid_media_path")
    with path.open("rb") as handle:
        data = handle.read(MAX_MEDIA_BYTES + 1)
    if len(data) > MAX_MEDIA_BYTES or capture.raw_media_size != len(data):
        raise ValueError("media_size_invalid")
    return data


def enqueue_photo_delivery(capture_id):
    """Queue delivery after commit; DB outbox remains authoritative if broker is down."""
    def dispatch():
        try:
            from miniapp.tasks import deliver_recognition_photos_task
            logs = list(AdminNotificationLog.objects.filter(event_type=_EVENT, payload__capture_id=capture_id,
                                                            status=AdminNotificationLog.Status.QUEUED).values_list("pk", flat=True))
            if logs:
                deliver_recognition_photos_task.delay(logs)
        except Exception:
            logger.warning("recognition_photo_enqueue_failed", exc_info=False)
    transaction.on_commit(dispatch)


def _eligible(now):
    waiting = Q(status__in=(AdminNotificationLog.Status.QUEUED, AdminNotificationLog.Status.FAILED))
    ready = Q(next_retry_at__isnull=True) | Q(next_retry_at__lte=now)
    expired = Q(status=AdminNotificationLog.Status.PROCESSING, lease_expires_at__lte=now)
    return (waiting & ready) | expired


@contextmanager
def _delivery_mutex(pk):
    """Keep one live sender per outbox even if its recovery lease expires."""
    # A separate autocommit session avoids both long row transactions and
    # reentrant acquisition through the caller's Django connection. Negative
    # outbox IDs reserve a namespace distinct from positive storage lock IDs.
    mutex = connection.copy()
    try:
        with mutex.cursor() as cursor:
            cursor.execute('SELECT pg_try_advisory_lock(%s)', [-pk])
            acquired = cursor.fetchone()[0]
        yield acquired
    finally:
        # Closing this dedicated session also releases the lock on exceptions;
        # PostgreSQL releases it after a worker crash/disconnect as well.
        mutex.close()


def _claim(pk, recipients):
    """Short row claim only: no filesystem or network IO under DB locks."""
    from miniapp.models import RecognitionCapture

    now = timezone.now()
    with transaction.atomic():
        row = AdminNotificationLog.objects.select_for_update(skip_locked=True).filter(
            _eligible(now), pk=pk, event_type=_EVENT).first()
        if not row or row.telegram_chat_id not in recipients:
            return None
        if row.attempt_count >= _MAX_ATTEMPTS:
            row.status = AdminNotificationLog.Status.DEAD
            row.error_message = "recognition_photo_delivery_exhausted"
            row.lease_token = ""
            row.lease_expires_at = None
            row.next_retry_at = None
            row.save(update_fields=["status", "error_message", "lease_token", "lease_expires_at", "next_retry_at"])
            logger.error("recognition_photo_delivery_exhausted notification_id=%s", row.pk)
            return None
        capture = RecognitionCapture.objects.filter(pk=(row.payload or {}).get("capture_id")).first()
        if not capture or not capture.raw_media:
            row.status = AdminNotificationLog.Status.DEAD
            row.error_message = "recognition_photo_capture_missing"
            row.payload = {}
            row.lease_token = ""
            row.lease_expires_at = None
            row.next_retry_at = None
            row.save(update_fields=["status", "error_message", "payload", "lease_token",
                                    "lease_expires_at", "next_retry_at"])
            logger.error("recognition_photo_capture_missing notification_id=%s", row.pk)
            return None
        # Final-outcome enqueue handles quick parses; a crashed parser has a bounded fallback.
        if capture.outcome in {"", "pending"} and capture.created_at > now - timedelta(minutes=2):
            return None
        row.status = AdminNotificationLog.Status.PROCESSING
        row.attempt_count += 1
        row.lease_token = uuid4().hex
        row.lease_expires_at = now + _LEASE_DURATION
        row.next_retry_at = None
        row.save(update_fields=["status", "attempt_count", "lease_token", "lease_expires_at", "next_retry_at"])
        return row, capture


def _finish(row, *, sent, permanent=False):
    now = timezone.now()
    dead = not sent and (permanent or row.attempt_count >= _MAX_ATTEMPTS)
    values = {"lease_token": "", "lease_expires_at": None, "next_retry_at": None}
    if sent:
        values.update(status=AdminNotificationLog.Status.SENT, sent_at=now, error_message="")
    elif dead:
        values.update(status=AdminNotificationLog.Status.DEAD, error_message="recognition_photo_delivery_exhausted")
    else:
        delay = _RETRY_BASE_SECONDS * 2 ** (row.attempt_count - 1)
        values.update(status=AdminNotificationLog.Status.FAILED, error_message="recognition_photo_delivery_failed",
                      next_retry_at=now + timedelta(seconds=delay))
    # A stale worker must not overwrite a reclaimed lease or recreate a deleted outbox row.
    changed = AdminNotificationLog.objects.filter(pk=row.pk, status=AdminNotificationLog.Status.PROCESSING,
                                                  lease_token=row.lease_token).update(**values)
    if dead and changed:
        logger.error("recognition_photo_delivery_exhausted notification_id=%s", row.pk)


def _send_one(row, capture):
    try:
        data = _read_original(capture)
    except (OSError, ValueError):
        _finish(row, sent=False, permanent=True)
        return "failed"
    # Deletion can remove both the capture and its outbox while the original is read.
    # Once HTTP starts it cannot be recalled, but never start it for a revoked claim.
    if not AdminNotificationLog.objects.filter(pk=row.pk, status=AdminNotificationLog.Status.PROCESSING,
                                               lease_token=row.lease_token,
                                               lease_expires_at__gt=timezone.now()).exists():
        return "skipped"
    mime_type = capture.mime_type if capture.mime_type in _MIME_EXTENSIONS else "application/octet-stream"
    extension = _MIME_EXTENSIONS.get(mime_type, ".bin")
    try:
        send_telegram_document(bot_token=settings.TELEGRAM_BOT_TOKEN, chat_id=row.telegram_chat_id,
                                filename=f"recognition-{capture.pk}{extension}", content=data,
                                mime_type=mime_type, caption=_caption(capture))
    except Exception:
        _finish(row, sent=False)
        return "failed"
    _finish(row, sent=True)
    return "sent"


def deliver_photo_notifications(log_ids=None):
    result = {"sent": 0, "failed": 0, "skipped": 0}
    if not getattr(settings, "ADMIN_NOTIFICATIONS_ENABLED", False) or not getattr(settings, "TELEGRAM_BOT_TOKEN", ""):
        return result
    recipients = set(getattr(settings, "ADMIN_TELEGRAM_IDS", ()))
    qs = AdminNotificationLog.objects.filter(_eligible(timezone.now()), event_type=_EVENT)
    if log_ids is not None: qs = qs.filter(pk__in=log_ids)
    for pk in list(qs.order_by("pk").values_list("pk", flat=True)[:500]):
        with _delivery_mutex(pk) as acquired:
            if not acquired:
                result["skipped"] += 1
                continue
            claim = _claim(pk, recipients)
            if claim is None:
                result["skipped"] += 1
                continue
            result[_send_one(*claim)] += 1
    return result
