from __future__ import annotations

from celery import shared_task
from django.conf import settings
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from common.telegram import TelegramSendError, send_telegram_message


@shared_task
def send_admin_notification_task(event_type: str, message_text: str, payload: dict | None = None) -> None:
    if not settings.ADMIN_NOTIFICATIONS_ENABLED:
        return
    if not settings.TELEGRAM_BOT_TOKEN:
        return

    for chat_id in settings.ADMIN_TELEGRAM_IDS:
        log = AdminNotificationLog.objects.create(
            event_type=event_type,
            telegram_chat_id=chat_id,
            payload=payload or {},
        )
        try:
            send_telegram_message(
                bot_token=settings.TELEGRAM_BOT_TOKEN,
                chat_id=chat_id,
                text=message_text,
                parse_mode=None,
            )
        except TelegramSendError as exc:
            log.status = AdminNotificationLog.Status.FAILED
            log.error_message = str(exc)[:2000]
            log.save(update_fields=["status", "error_message"])
        else:
            log.status = AdminNotificationLog.Status.SENT
            log.sent_at = timezone.now()
            log.save(update_fields=["status", "sent_at"])
