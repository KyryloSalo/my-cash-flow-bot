from __future__ import annotations

import json
import time

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.db.models import Count
from django.utils import timezone

from admin_notifications.tasks import send_admin_notification_task
from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog, Broadcast, BroadcastRecipient
from broadcasts.targets import (
    InvalidAudience, campaign_target_kwargs, delivery_config, delivery_snapshot,
    users_for_target, verified_recipients,
)
from common.telegram_admins import get_primary_admin_telegram_id
from common.telegram import TelegramSendError, send_telegram_message
from users.models import UserAdminState


def _resolve_admin_chat_id() -> int | None:
    return get_primary_admin_telegram_id()


def _button_rows(button_text: str, button_url: str) -> list[list[dict]]:
    if not button_text or not button_url:
        return []
    return [[{"text": button_text, "url": button_url}]]


UNCERTAIN = "uncertain"
UNCERTAIN_DELIVERY = "Delivery in flight or outcome unknown; automatic retry disabled."


def claim_delivery(queryset, **updates) -> bool:
    """Commit an at-most-once claim BEFORE I/O; never reclaim uncertain sends.

    Telegram has no idempotency key. A crash after this update must therefore
    remain visibly uncertain, even if it occurred before the request was sent.
    The caller scopes this compare-and-set to the initial status and campaign.
    """
    with transaction.atomic():
        recipient_id = queryset.select_for_update(skip_locked=True).values_list("pk", flat=True).first()
        if recipient_id is None:
            return False
        return bool(queryset.model._default_manager.filter(pk=recipient_id).update(
            status=UNCERTAIN,
            error_message=UNCERTAIN_DELIVERY,
            **updates,
        ))


def delivery_failure(exc: Exception) -> tuple[str, str]:
    if isinstance(exc, TelegramSendError):
        if exc.blocked:
            return "blocked", "Telegram reports that the actual recipient blocked the bot."
        try:
            payload = json.loads(exc.payload or "{}")
            if isinstance(payload, dict) and payload.get("ok") is False and 400 <= int(payload.get("error_code", 0)) < 500:
                return "failed", "Telegram rejected the delivery; automatic retry disabled."
        except (TypeError, ValueError):
            pass
    return UNCERTAIN, UNCERTAIN_DELIVERY


def refresh_broadcast_counts(broadcast_id: int) -> dict:
    counts = dict(BroadcastRecipient.objects.filter(broadcast_id=broadcast_id).order_by().values_list("status").annotate(total=Count("pk")))
    result = {f"{status}_count": counts.get(status, 0) for status in ("sent", "failed", "blocked", "skipped", UNCERTAIN)}
    Broadcast.objects.filter(pk=broadcast_id).update(**result)
    return result


@shared_task
def send_manual_message_task(message_log_id: int, *, is_test: bool = False) -> None:
    log = AdminMessageLog.objects.select_related("telegram_user").get(pk=message_log_id)
    if log.status != AdminMessageLog.Status.QUEUED:
        return
    bot_token = settings.TELEGRAM_BOT_TOKEN
    if not bot_token:
        log.status = AdminMessageLog.Status.FAILED
        log.error_message = "TELEGRAM_BOT_TOKEN missing"
        log.save(update_fields=["status", "error_message"])
        return

    target_chat_id = _resolve_admin_chat_id() if is_test else log.target_chat_id
    if target_chat_id is None:
        log.status = AdminMessageLog.Status.FAILED
        log.error_message = "ADMIN_TELEGRAM_IDS missing for test message"
        log.save(update_fields=["status", "error_message"])
        return

    if not claim_delivery(
        AdminMessageLog.objects.filter(pk=log.pk, status=AdminMessageLog.Status.QUEUED),
        target_chat_id=int(target_chat_id),
    ):
        return

    try:
        payload = send_telegram_message(
            bot_token=bot_token,
            chat_id=int(target_chat_id),
            text=log.message_text,
            parse_mode=log.parse_mode,
            buttons=log.buttons_payload,
            image=log.image_url or None,
        )
    except Exception as exc:
        log.status, log.error_message = delivery_failure(exc)
        log.save(update_fields=["status", "error_message"])
        if not is_test and log.telegram_user_id == int(target_chat_id) and log.status == AdminMessageLog.Status.BLOCKED:
            UserAdminState.objects.update_or_create(
                telegram_user_id=log.telegram_user_id,
                defaults={"can_receive_messages": False, "blocked_bot": True},
            )
        BotEvent.objects.create(
            user_id=log.telegram_user_id if not is_test and log.telegram_user_id == int(target_chat_id) else None,
            event_type="manual_message_sent",
            source="admin_test" if is_test else "admin",
            raw_input=log.message_text,
            parsed_result={"is_test": is_test, "target_chat_id": int(target_chat_id)},
            success=False,
            error_message=log.error_message,
        )
        return

    log.status = AdminMessageLog.Status.SENT
    log.error_message = ""
    log.response_payload = payload
    log.sent_at = timezone.now()
    log.save(update_fields=["status", "error_message", "response_payload", "sent_at"])
    BotEvent.objects.create(
        user_id=log.telegram_user_id if not is_test and log.telegram_user_id == int(target_chat_id) else None,
        event_type="manual_message_sent",
        source="admin_test" if is_test else "admin",
        raw_input=log.message_text,
        parsed_result={"message_id": payload.get("result", {}).get("message_id"), "is_test": is_test},
        success=True,
    )


@shared_task
def send_broadcast_task(broadcast_id: int, *, dry_run: bool = False) -> None:
    broadcast = Broadcast.objects.select_related("created_by").get(pk=broadcast_id)
    if not dry_run and broadcast.status != Broadcast.Status.SCHEDULED:
        return
    snapshot = (broadcast.last_dry_run_preview or {}).get("delivery_snapshot")
    try:
        if dry_run:
            recipients = list(users_for_target(**campaign_target_kwargs(broadcast)))
            snapshot = delivery_snapshot(broadcast, recipients=recipients)
        else:
            recipients = verified_recipients(broadcast, snapshot)
    except InvalidAudience as exc:
        updates = {"last_error": str(exc)}
        if dry_run:
            updates.update(last_dry_run_preview={}, total_recipients=0)
        else:
            updates.update(status=Broadcast.Status.FAILED, finished_at=timezone.now())
        Broadcast.objects.filter(pk=broadcast.pk, status=broadcast.status).update(**updates)
        return
    broadcast.total_recipients = len(recipients)
    if dry_run:
        broadcast.last_dry_run_preview = {
            "delivery_snapshot": snapshot,
            "count": len(recipients),
            "users": [
                {
                    "telegram_id": user.tg_user_id,
                    "username": user.username or "",
                    "first_name": user.first_name or "",
                }
                for user in recipients[:20]
            ],
        }
        broadcast.save(update_fields=["total_recipients", "last_dry_run_preview", "updated_at"])
        return

    if not Broadcast.objects.filter(pk=broadcast.pk, status=Broadcast.Status.SCHEDULED).update(
        status=Broadcast.Status.SENDING, started_at=timezone.now(), last_error="",
        total_recipients=len(recipients), last_dry_run_preview={}, updated_at=timezone.now(),
    ):
        return

    bot_token = settings.TELEGRAM_BOT_TOKEN
    if not bot_token:
        Broadcast.objects.filter(pk=broadcast.pk, status=Broadcast.Status.SENDING).update(
            status=Broadcast.Status.FAILED, last_error="TELEGRAM_BOT_TOKEN missing", finished_at=timezone.now(),
        )
        send_admin_notification_task.delay("broadcast_failed", f"Broadcast failed: {broadcast.title}", {"broadcast_id": broadcast.id})
        return

    sent = failed = blocked = skipped = 0
    for user in recipients:
        current = Broadcast.objects.get(pk=broadcast.pk)
        current_status = current.status
        if current_status == Broadcast.Status.SENDING:
            try:
                if delivery_config(current) != snapshot["config"]:
                    raise InvalidAudience("Campaign configuration changed during delivery; confirm again.")
            except InvalidAudience as exc:
                Broadcast.objects.filter(pk=broadcast.pk, status=Broadcast.Status.SENDING).update(
                    status=Broadcast.Status.FAILED, last_error=str(exc), finished_at=timezone.now(),
                )
                current_status = Broadcast.Status.FAILED
        if current_status != Broadcast.Status.SENDING:
            recipient, _ = BroadcastRecipient.objects.get_or_create(broadcast=broadcast, user_id=user.tg_user_id)
            BroadcastRecipient.objects.filter(pk=recipient.pk, status=BroadcastRecipient.Status.PENDING).update(
                status=BroadcastRecipient.Status.SKIPPED, error_message="Campaign stopped before delivery",
            )
            skipped += 1
            continue

        recipient, _ = BroadcastRecipient.objects.get_or_create(broadcast=broadcast, user_id=user.tg_user_id)
        if not claim_delivery(BroadcastRecipient.objects.filter(
            pk=recipient.pk, status=BroadcastRecipient.Status.PENDING, broadcast__status=Broadcast.Status.SENDING,
        )):
            continue

        refresh_broadcast_counts(broadcast.pk)
        try:
            payload = send_telegram_message(
                bot_token=bot_token,
                chat_id=user.tg_user_id,
                text=broadcast.message_text,
                parse_mode=broadcast.parse_mode,
                buttons=_button_rows(broadcast.button_text, broadcast.button_url),
                image=broadcast.image_url or None,
            )
        except Exception as exc:
            recipient.status, recipient.error_message = delivery_failure(exc)
            recipient.save(update_fields=["status", "error_message"])
            if recipient.status == BroadcastRecipient.Status.BLOCKED:
                UserAdminState.objects.update_or_create(
                    telegram_user_id=user.tg_user_id,
                    defaults={"can_receive_messages": False, "blocked_bot": True},
                )
                blocked += 1
            else:
                failed += 1
        else:
            recipient.status = BroadcastRecipient.Status.SENT
            recipient.error_message = ""
            recipient.sent_at = timezone.now()
            recipient.save(update_fields=["status", "error_message", "sent_at"])
            sent += 1
            BotEvent.objects.create(
                user_id=user.tg_user_id,
                event_type="broadcast_completed",
                source="broadcast",
                raw_input=broadcast.message_text,
                parsed_result={"broadcast_id": broadcast.id, "message_id": payload.get("result", {}).get("message_id")},
                success=True,
            )

        refresh_broadcast_counts(broadcast.pk)
        time.sleep(0.08)

    counts = refresh_broadcast_counts(broadcast.pk)
    sent, failed, blocked, skipped = (counts[f"{status}_count"] for status in ("sent", "failed", "blocked", "skipped"))
    finalized = Broadcast.objects.filter(pk=broadcast.pk, status=Broadcast.Status.SENDING).update(
        status=Broadcast.Status.FAILED if failed or counts["uncertain_count"] else Broadcast.Status.COMPLETED,
        last_error=UNCERTAIN_DELIVERY if counts["uncertain_count"] else "",
        finished_at=timezone.now(),
    )
    if not finalized:
        return
    broadcast.refresh_from_db()
    event_type = "broadcast_completed" if broadcast.status == Broadcast.Status.COMPLETED else "broadcast_failed"
    send_admin_notification_task.delay(
        event_type,
        f"Broadcast '{broadcast.title}' finished. sent={sent}, failed={failed}, blocked={blocked}, skipped={skipped}, uncertain={counts['uncertain_count']}",
        {"broadcast_id": broadcast.id},
    )
