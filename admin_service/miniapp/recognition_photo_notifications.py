from __future__ import annotations

from datetime import timedelta
import logging
from pathlib import Path
import re

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from common.telegram import TelegramSendError
from common.telegram_upload import send_telegram_document

logger = logging.getLogger(__name__)
_EVENT = "recognition_photo"
_KEY = re.compile(r"^[0-9a-f]{32}\.bin$")
_MIME_EXTENSIONS = {"image/heic": ".heic", "image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp"}


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


def _send_one(row, capture):
    try:
        data = _read_original(capture)
        mime_type = capture.mime_type if capture.mime_type in _MIME_EXTENSIONS else "application/octet-stream"
        extension = _MIME_EXTENSIONS.get(mime_type, ".bin")
        send_telegram_document(bot_token=settings.TELEGRAM_BOT_TOKEN, chat_id=row.telegram_chat_id,
                                filename=f"recognition-{capture.pk}{extension}", content=data,
                                mime_type=mime_type, caption=_caption(capture))
    except Exception:
        row.status = AdminNotificationLog.Status.FAILED
        row.error_message = "recognition_photo_delivery_failed"
        row.save(update_fields=["status", "error_message"])
        return False
    row.status = AdminNotificationLog.Status.SENT
    row.sent_at = timezone.now()
    row.error_message = ""
    row.save(update_fields=["status", "sent_at", "error_message"])
    return True


def deliver_photo_notifications(log_ids=None):
    result = {"sent": 0, "failed": 0, "skipped": 0}
    if not getattr(settings, "ADMIN_NOTIFICATIONS_ENABLED", False) or not getattr(settings, "TELEGRAM_BOT_TOKEN", ""):
        return result
    recipients = set(getattr(settings, "ADMIN_TELEGRAM_IDS", ()))
    qs = AdminNotificationLog.objects.filter(event_type=_EVENT,
        status__in=(AdminNotificationLog.Status.QUEUED, AdminNotificationLog.Status.FAILED))
    if log_ids is not None: qs = qs.filter(pk__in=log_ids)
    for pk in list(qs.order_by("pk").values_list("pk", flat=True)[:500]):
        with transaction.atomic():
            row = AdminNotificationLog.objects.select_for_update().filter(pk=pk, event_type=_EVENT,
                status__in=(AdminNotificationLog.Status.QUEUED, AdminNotificationLog.Status.FAILED)).first()
            if not row or row.telegram_chat_id not in recipients:
                result["skipped"] += 1; continue
            capture_id = (row.payload or {}).get("capture_id")
            from miniapp.models import RecognitionCapture
            capture = RecognitionCapture.objects.select_for_update().filter(pk=capture_id).first()
            if not capture or not capture.raw_media:
                result["skipped"] += 1; continue
            # Outcome updates enqueue again. Keep a bounded fallback so a crashed
            # parser still delivers its original through periodic outbox recovery.
            if capture.outcome in {"", "pending"} and capture.created_at > timezone.now() - timedelta(minutes=2):
                result["skipped"] += 1; continue
            if _send_one(row, capture): result["sent"] += 1
            else: result["failed"] += 1
    return result
