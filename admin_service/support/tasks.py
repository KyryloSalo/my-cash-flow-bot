from __future__ import annotations

from celery import shared_task
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from admin_notifications.tasks import send_admin_notification_task
from bot_events.models import BotEvent
from broadcasts.tasks import delivery_failure
from common.telegram import TelegramSendError, send_support_reply
from support.models import SupportCase, SupportMessage
from subscriptions.models import TrialRecoveryRecipient
from users.models import UserAdminState


# Telegram message IDs are positive. Reserve -1 as a durable in-flight/unknown
# receipt in the existing nullable column; never clear it or auto-retry it.
SUPPORT_DELIVERY_UNCERTAIN = -1
SUPPORT_DELIVERY_FAILED = -2
SUPPORT_DELIVERY_BLOCKED = -3


@shared_task
def send_support_reply_task(message_id: int) -> None:
    message = SupportMessage.objects.select_related("case", "case__user").get(pk=message_id)
    if message.sender_type != SupportMessage.SenderType.ADMIN or message.telegram_message_id is not None:
        return
    if not settings.TELEGRAM_BOT_TOKEN:
        return

    # No I/O inside the transaction: the claim must survive a worker crash.
    # Reject callers wrapped in a larger transaction that could roll it back.
    with transaction.atomic(durable=True):
        user_state = (
            UserAdminState.objects.select_for_update()
            .filter(telegram_user_id=message.case.user_id)
            .first()
        )
        recovery_recipient = (
            TrialRecoveryRecipient.objects.select_for_update()
            .filter(support_case_id=message.case_id)
            .first()
        )
        suppressed = bool(
            not user_state
            or user_state.blocked_bot
            or not user_state.can_receive_messages
            or (
                recovery_recipient
                and (
                    recovery_recipient.opted_out_at is not None
                    or recovery_recipient.personal_contact_declined_at is not None
                    or recovery_recipient.status == TrialRecoveryRecipient.Status.OPTED_OUT
                )
            )
        )
        if suppressed:
            SupportMessage.objects.filter(
                pk=message.pk,
                sender_type=SupportMessage.SenderType.ADMIN,
                telegram_message_id__isnull=True,
            ).update(telegram_message_id=SUPPORT_DELIVERY_BLOCKED)
            return
        claimed = SupportMessage.objects.filter(
            pk=message.pk, sender_type=SupportMessage.SenderType.ADMIN,
            telegram_message_id__isnull=True,
        ).update(telegram_message_id=SUPPORT_DELIVERY_UNCERTAIN)
    if not claimed:
        return

    try:
        payload = send_support_reply(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=message.case.user_id,
            text=message.text,
        )
        message_id = payload.get("result", {}).get("message_id")
        if not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0:
            raise TelegramSendError("Provider acknowledgement missing")
    except Exception as exc:
        status, error_message = delivery_failure(exc)
        receipt = {"failed": SUPPORT_DELIVERY_FAILED, "blocked": SUPPORT_DELIVERY_BLOCKED}.get(status, SUPPORT_DELIVERY_UNCERTAIN)
        SupportMessage.objects.filter(pk=message.pk, telegram_message_id=SUPPORT_DELIVERY_UNCERTAIN).update(telegram_message_id=receipt)
        if status == "blocked":
            UserAdminState.objects.update_or_create(
                telegram_user_id=message.case.user_id,
                defaults={"can_receive_messages": False, "blocked_bot": True},
            )
        BotEvent.objects.create(
            user_id=message.case.user_id,
            event_type="bot_error",
            source="support_reply",
            parsed_result={"support_case_id": message.case_id, "support_message_id": message.pk, "delivery_status": status},
            success=False,
            error_message=error_message,
        )
        return

    message.telegram_message_id = message_id
    message.save(update_fields=["telegram_message_id"])
    BotEvent.objects.create(
        user_id=message.case.user_id,
        event_type="manual_message_sent",
        source="support_reply",
        parsed_result={"support_case_id": message.case_id, "support_message_id": message.pk, "delivery_status": "sent"},
        success=True,
    )


@shared_task
def notify_new_support_case_task(case_id: int) -> None:
    case = SupportCase.objects.select_related("user").get(pk=case_id)
    user = case.user
    username = f"@{user.username}" if user.username else "-"
    text = (
        f"Новий support case #{case.pk}\n\n"
        f"Username: {username}\n"
        f"Telegram ID: {user.tg_user_id}\n"
        f"Підписка: {getattr(getattr(user, 'admin_state', None), 'subscription_status', '-')}\n"
        f"Текст: {(case.subject or '').strip()[:250] or '-'}"
    )
    send_admin_notification_task.delay("support_case_new", text, {"support_case_id": case.id, "created_at": timezone.now().isoformat()})
