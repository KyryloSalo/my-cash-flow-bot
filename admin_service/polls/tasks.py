from __future__ import annotations

import time

from celery import Task, shared_task
from django.conf import settings
from django.db.models import Count
from django.utils import timezone

from admin_notifications.tasks import send_admin_notification_task
from bot_events.models import BotEvent
from broadcasts.targets import (
    InvalidAudience, campaign_target_kwargs, delivery_config, delivery_snapshot,
    users_for_target, verified_recipients,
)
from broadcasts.tasks import claim_delivery, delivery_failure
from common.telegram_admins import get_primary_admin_telegram_id
from common.telegram import send_telegram_message
from polls.models import PollCampaign, PollRecipient
from users.models import UserAdminState


def _rating_buttons(campaign: PollCampaign) -> list[list[dict]]:
    if campaign.type == PollCampaign.Type.NPS:
        values = [str(item) for item in range(0, 11)]
    elif campaign.type == PollCampaign.Type.RATING:
        values = [str(item) for item in range(1, 6)]
    elif campaign.type == PollCampaign.Type.SINGLE_CHOICE:
        values = [str(item) for item in (campaign.options or [])]
    else:
        values = ["Відповісти"]

    buttons: list[list[dict]] = []
    current_row: list[dict] = []
    for value in values:
        if campaign.type == PollCampaign.Type.TEXT:
            callback_data = f"poll:text:{campaign.id}"
        else:
            callback_data = f"poll:answer:{campaign.id}:{value}"
        current_row.append({"text": value, "callback_data": callback_data})
        if len(current_row) == 4:
            buttons.append(current_row)
            current_row = []
    if current_row:
        buttons.append(current_row)
    return buttons


def _resolve_admin_chat_id() -> int | None:
    return get_primary_admin_telegram_id()


class ConfirmedPollDeliveryTask(Task):
    """Freeze the confirmed POST's audience before publishing to Celery.

    Preserve that snapshot on broker retries; never silently refresh it. Legacy
    messages without a snapshot are refused by the worker, not sent broadly.
    """
    def apply_async(self, args=None, kwargs=None, **options):
        kwargs = dict(kwargs or {})
        if not kwargs.get("dry_run") and not kwargs.get("is_test") and "confirmed_snapshot" not in kwargs:
            campaign_id = args[0] if args else kwargs["campaign_id"]
            campaign = PollCampaign.objects.get(pk=campaign_id)
            try:
                kwargs["confirmed_snapshot"] = delivery_snapshot(campaign)
            except InvalidAudience as exc:
                kwargs["confirmed_snapshot"] = {"error": str(exc)}
        return super().apply_async(args=args, kwargs=kwargs, **options)


@shared_task(base=ConfirmedPollDeliveryTask)
def send_poll_campaign_task(campaign_id: int, *, dry_run: bool = False, is_test: bool = False, confirmed_snapshot=None) -> None:
    campaign = PollCampaign.objects.get(pk=campaign_id)
    if campaign.status in {PollCampaign.Status.COMPLETED, PollCampaign.Status.CANCELLED}:
        return
    if not dry_run and not is_test and campaign.status not in {PollCampaign.Status.DRAFT, PollCampaign.Status.SCHEDULED}:
        return
    question = campaign.question
    buttons = _rating_buttons(campaign)

    if is_test:
        admin_chat_id = _resolve_admin_chat_id()
        if not admin_chat_id or not settings.TELEGRAM_BOT_TOKEN:
            return
        send_telegram_message(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=admin_chat_id,
            text=question,
            buttons=buttons,
        )
        return

    try:
        recipients = (
            list(users_for_target(**campaign_target_kwargs(campaign))) if dry_run
            else verified_recipients(campaign, confirmed_snapshot)
        )
    except InvalidAudience as exc:
        if not dry_run:
            PollCampaign.objects.filter(pk=campaign.pk, status__in=[PollCampaign.Status.DRAFT, PollCampaign.Status.SCHEDULED]).update(
                status=PollCampaign.Status.CANCELLED, completed_at=timezone.now(),
            )
        BotEvent.objects.create(event_type="poll_delivery_stopped", source="poll", success=False,
                                parsed_result={"poll_campaign_id": campaign.pk}, error_message=str(exc))
        return
    campaign.total_recipients = len(recipients)
    if dry_run:
        campaign.save(update_fields=["total_recipients"])
        return

    if not settings.TELEGRAM_BOT_TOKEN:
        PollCampaign.objects.filter(pk=campaign.pk, status__in=[PollCampaign.Status.DRAFT, PollCampaign.Status.SCHEDULED]).update(
            status=PollCampaign.Status.CANCELLED,
        )
        return

    if not PollCampaign.objects.filter(pk=campaign.pk, status__in=[PollCampaign.Status.DRAFT, PollCampaign.Status.SCHEDULED]).update(
        status=PollCampaign.Status.ACTIVE, started_at=timezone.now(), total_recipients=len(recipients),
    ):
        return

    failed = blocked = sent = 0
    for user in recipients:
        current = PollCampaign.objects.get(pk=campaign.pk)
        if current.status != PollCampaign.Status.ACTIVE:
            break
        try:
            if delivery_config(current) != confirmed_snapshot["config"]:
                raise InvalidAudience("Campaign configuration changed during delivery; confirm again.")
        except InvalidAudience as exc:
            PollCampaign.objects.filter(pk=campaign.pk, status=PollCampaign.Status.ACTIVE).update(
                status=PollCampaign.Status.CANCELLED, completed_at=timezone.now(),
            )
            BotEvent.objects.create(event_type="poll_delivery_stopped", source="poll", success=False,
                                    parsed_result={"poll_campaign_id": campaign.pk}, error_message=str(exc))
            break
        recipient, _ = PollRecipient.objects.get_or_create(campaign=campaign, user_id=user.tg_user_id)
        if not claim_delivery(PollRecipient.objects.filter(
            pk=recipient.pk, status=PollRecipient.Status.PENDING, campaign__status=PollCampaign.Status.ACTIVE,
        )):
            continue
        try:
            payload = send_telegram_message(
                bot_token=settings.TELEGRAM_BOT_TOKEN,
                chat_id=user.tg_user_id,
                text=question,
                buttons=buttons,
            )
        except Exception as exc:
            recipient.status, recipient.error_message = delivery_failure(exc)
            recipient.save(update_fields=["status", "error_message"])
            if recipient.status == PollRecipient.Status.BLOCKED:
                UserAdminState.objects.update_or_create(
                    telegram_user_id=user.tg_user_id,
                    defaults={"can_receive_messages": False, "blocked_bot": True},
                )
                blocked += 1
            else:
                failed += 1
        else:
            recipient.status = PollRecipient.Status.SENT
            recipient.error_message = ""
            recipient.sent_at = timezone.now()
            recipient.save(update_fields=["status", "error_message", "sent_at"])
            sent += 1
            BotEvent.objects.create(
                user_id=user.tg_user_id,
                event_type="button_clicked",
                source="poll_sent",
                parsed_result={"poll_campaign_id": campaign.id, "message_id": payload.get("result", {}).get("message_id")},
                success=True,
            )
        time.sleep(0.08)

    counts = dict(PollRecipient.objects.filter(campaign=campaign).order_by().values_list("status").annotate(total=Count("pk")))
    sent = counts.get(PollRecipient.Status.SENT, 0) + counts.get(PollRecipient.Status.RESPONDED, 0)
    failed = counts.get(PollRecipient.Status.FAILED, 0)
    blocked = counts.get(PollRecipient.Status.BLOCKED, 0)
    uncertain = counts.get("uncertain", 0)
    send_admin_notification_task.delay(
        "poll_campaign_sent",
        f"Poll '{campaign.title}' finished sendout. sent={sent}, failed={failed}, blocked={blocked}, uncertain={uncertain}",
        {"poll_campaign_id": campaign.id},
    )
