from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta

from django.db.models import Count, F, Q, QuerySet
from django.urls import reverse
from django.utils import timezone

from subscriptions.models import TrialRecoveryCampaign, TrialRecoveryDelivery, TrialRecoveryRecipient
from subscriptions.trial_recovery import REASON_LABELS, campaign_uses_registered_without_card_audience, safe_zone


UK_MONTHS_GENITIVE = (
    "січня",
    "лютого",
    "березня",
    "квітня",
    "травня",
    "червня",
    "липня",
    "серпня",
    "вересня",
    "жовтня",
    "листопада",
    "грудня",
)

CAMPAIGN_STATUS_LABELS = {
    TrialRecoveryCampaign.Status.DRAFT: "Чернетка",
    TrialRecoveryCampaign.Status.SCHEDULED: "Запланована",
    TrialRecoveryCampaign.Status.RUNNING: "Працює",
    TrialRecoveryCampaign.Status.PAUSED: "Призупинена",
    TrialRecoveryCampaign.Status.COMPLETED: "Завершена",
}

CAMPAIGN_STATUS_TONES = {
    TrialRecoveryCampaign.Status.DRAFT: "neutral",
    TrialRecoveryCampaign.Status.SCHEDULED: "warning",
    TrialRecoveryCampaign.Status.RUNNING: "success",
    TrialRecoveryCampaign.Status.PAUSED: "warning",
    TrialRecoveryCampaign.Status.COMPLETED: "neutral",
}

RECIPIENT_STATUS_LABELS = {
    TrialRecoveryRecipient.Status.SCHEDULED: "У розкладі",
    TrialRecoveryRecipient.Status.RESPONDED: "Відповіли",
    TrialRecoveryRecipient.Status.CONTACT_REQUESTED: "Попросили контакт",
    TrialRecoveryRecipient.Status.CONVERTED: "Активували trial",
    TrialRecoveryRecipient.Status.OPTED_OUT: "Відмовились від recovery",
    TrialRecoveryRecipient.Status.INELIGIBLE: "Втратили відповідність",
    TrialRecoveryRecipient.Status.COMPLETED: "Завершили без відповіді",
    TrialRecoveryRecipient.Status.DELIVERY_FAILED: "Не доставлено",
}

DELIVERY_STATUS_LABELS = {
    TrialRecoveryDelivery.Status.CLAIMED: "Обробляється",
    TrialRecoveryDelivery.Status.SENT: "Надіслано",
    TrialRecoveryDelivery.Status.FAILED: "Не доставлено",
    TrialRecoveryDelivery.Status.SKIPPED: "Пропущено",
}

DELIVERY_STATUS_TONES = {
    TrialRecoveryDelivery.Status.CLAIMED: "warning",
    TrialRecoveryDelivery.Status.SENT: "success",
    TrialRecoveryDelivery.Status.FAILED: "danger",
    TrialRecoveryDelivery.Status.SKIPPED: "neutral",
}

EXCLUSION_LABELS = {
    "has_successful_payment": "Уже мають успішну оплату",
    "not_90_day_offer": "Неактуальний 90-денний офер",
    "onboarding_incomplete": "Не завершили онбординг",
    "has_other_access": "Вже мають family/admin/інший повний доступ",
    "blocked": "Заблоковані",
    "cannot_receive_messages": "Не можуть отримувати повідомлення",
    "test_user": "Тестові користувачі",
    "has_card": "Вже прив’язали картку",
    "has_full_subscription": "Trial або повний доступ уже активний",
    "bind_in_progress": "Оплата 1 грн уже в процесі",
    "already_enrolled": "Уже були в recovery",
}


def recovered_recipient_queryset(queryset: QuerySet | None = None) -> QuerySet:
    """Recipients whose trial activation happened after a delivered recovery message."""

    queryset = queryset if queryset is not None else TrialRecoveryRecipient.objects.all()
    return queryset.filter(
        first_sent_at__isnull=False,
        converted_at__isnull=False,
        converted_at__gte=F("first_sent_at"),
    )


def is_recovered_recipient(recipient: TrialRecoveryRecipient) -> bool:
    return bool(
        recipient.first_sent_at
        and recipient.converted_at
        and recipient.converted_at >= recipient.first_sent_at
    )


def _percent(numerator: int, denominator: int) -> str:
    if not denominator:
        return "0,0%"
    return f"{numerator * 100 / denominator:.1f}%".replace(".", ",")


def _local_datetime(value: datetime | None, timezone_name: str, fallback: str) -> datetime | None:
    if value is None:
        return None
    zone, _ = safe_zone(timezone_name, fallback)
    return value.astimezone(zone)


def _is_local_today(value: datetime | None, timezone_name: str, now: datetime, fallback: str) -> bool:
    local_value = _local_datetime(value, timezone_name, fallback)
    local_now = _local_datetime(now, timezone_name, fallback)
    return bool(local_value and local_now and local_value.date() == local_now.date())


def _date_label(value: datetime, timezone_name: str, fallback: str) -> str:
    local = _local_datetime(value, timezone_name, fallback)
    if local is None:
        return "—"
    return f"{local.day} {UK_MONTHS_GENITIVE[local.month - 1]}, {local:%H:%M}"


def _today_label(now: datetime, fallback: str) -> str:
    local = _local_datetime(now, fallback, fallback)
    if local is None:
        return "сьогодні"
    return f"{local.day} {UK_MONTHS_GENITIVE[local.month - 1]}"


def _delivery_error_label(error_message: str) -> str:
    normalized = str(error_message or "").strip().replace("\n", " ")
    lowered = normalized.lower()
    if "blocked by the user" in lowered:
        return "Користувач заблокував бота"
    if "chat not found" in lowered:
        return "Telegram-чат недоступний"
    return normalized[:180] or "Причина не зафіксована"


def _recipient_label(recipient: TrialRecoveryRecipient) -> str:
    user = recipient.user
    if user.username:
        return f"@{user.username}"
    if user.full_name:
        return user.full_name
    return f"Telegram ID {user.pk}"


def _recent_deliveries(campaign: TrialRecoveryCampaign) -> list[dict]:
    rows = []
    deliveries = (
        TrialRecoveryDelivery.objects.filter(recipient__campaign=campaign)
        .select_related("recipient__user")
        .order_by("-updated_at")[:12]
    )
    step_labels = {1: "Перший пуш", 2: "Другий пуш", 3: "Фінальний пуш"}
    for delivery in deliveries:
        recipient = delivery.recipient
        _zone, normalized_timezone = safe_zone(recipient.timezone, campaign.fallback_timezone)
        rows.append(
            {
                "user": _recipient_label(recipient),
                "user_url": reverse("admin:subscriptions_trialrecoveryrecipient_change", args=[recipient.pk]),
                "step": step_labels.get(delivery.step, f"Крок {delivery.step}"),
                "status": DELIVERY_STATUS_LABELS.get(delivery.status, delivery.status),
                "tone": DELIVERY_STATUS_TONES.get(delivery.status, "neutral"),
                "time": _date_label(delivery.sent_at or delivery.updated_at, recipient.timezone, campaign.fallback_timezone),
                "timezone": normalized_timezone,
                "detail": _delivery_error_label(delivery.error_message) if delivery.status == TrialRecoveryDelivery.Status.FAILED else "",
            }
        )
    return rows


def _recent_signals(campaign: TrialRecoveryCampaign) -> list[dict]:
    rows = []
    recipients = (
        campaign.recipients.filter(
            Q(responded_at__isnull=False)
            | Q(contact_requested_at__isnull=False)
            | Q(opted_out_at__isnull=False)
            | Q(converted_at__isnull=False)
        )
        .select_related("user")
        .order_by("-updated_at")[:20]
    )
    for recipient in recipients:
        events = []
        if recipient.responded_at:
            events.append((recipient.responded_at, "Відповів на recovery"))
        if recipient.contact_requested_at:
            events.append((recipient.contact_requested_at, "Попросив особистий контакт"))
        if recipient.opted_out_at:
            events.append((recipient.opted_out_at, "Відмовився від recovery"))
        if recipient.converted_at:
            label = "Відновлений користувач" if is_recovered_recipient(recipient) else "Активувався до першого recovery-пуша"
            events.append((recipient.converted_at, label))
        if not events:
            continue
        event_at, event_label = max(events, key=lambda item: item[0])
        _zone, normalized_timezone = safe_zone(recipient.timezone, campaign.fallback_timezone)
        details = []
        if recipient.reason:
            details.append(REASON_LABELS["uk"].get(recipient.reason, recipient.reason))
        if recipient.free_text:
            details.append(recipient.free_text.strip()[:180])
        rows.append(
            {
                "user": _recipient_label(recipient),
                "user_url": reverse("admin:subscriptions_trialrecoveryrecipient_change", args=[recipient.pk]),
                "event": event_label,
                "time": _date_label(event_at, recipient.timezone, campaign.fallback_timezone),
                "timezone": normalized_timezone,
                "detail": " · ".join(details),
            }
        )
    return rows[:12]


def build_recovery_campaign_dashboard(
    campaign: TrialRecoveryCampaign,
    *,
    now: datetime | None = None,
) -> dict:
    now = now or timezone.now()
    recipients = campaign.recipients.all()
    deliveries = TrialRecoveryDelivery.objects.filter(recipient__campaign=campaign)
    recovered_qs = recovered_recipient_queryset(recipients)

    total_recipients = recipients.count()
    reached_users = recipients.filter(sent_count__gt=0).count()
    sent_messages = deliveries.filter(status=TrialRecoveryDelivery.Status.SENT).count()
    responses = recipients.filter(responded_at__isnull=False).count()
    contact_requests = recipients.filter(contact_requested_at__isnull=False).count()
    opted_out = recipients.filter(opted_out_at__isnull=False).count()
    converted_total = recipients.filter(converted_at__isnull=False).count()
    recovered_users = recovered_qs.count()
    converted_before_message = max(converted_total - recovered_users, 0)
    failed_deliveries = deliveries.filter(status=TrialRecoveryDelivery.Status.FAILED).count()
    blocked_deliveries = deliveries.filter(
        status=TrialRecoveryDelivery.Status.FAILED,
        error_message__icontains="blocked by the user",
    ).count()

    recent_cutoff = now - timedelta(hours=30)
    sent_today_rows = [
        delivery
        for delivery in deliveries.filter(
            status=TrialRecoveryDelivery.Status.SENT,
            sent_at__gte=recent_cutoff,
            sent_at__isnull=False,
        ).select_related("recipient")
        if _is_local_today(delivery.sent_at, delivery.recipient.timezone, now, campaign.fallback_timezone)
    ]
    failed_today_rows = [
        delivery
        for delivery in deliveries.filter(
            status=TrialRecoveryDelivery.Status.FAILED,
            updated_at__gte=recent_cutoff,
        ).select_related("recipient")
        if _is_local_today(delivery.updated_at, delivery.recipient.timezone, now, campaign.fallback_timezone)
    ]
    recovered_today = sum(
        1
        for recipient in recovered_qs.filter(converted_at__gte=recent_cutoff)
        if _is_local_today(recipient.converted_at, recipient.timezone, now, campaign.fallback_timezone)
    )

    remaining_today = []
    scheduled_recipients = list(
        recipients.filter(
            status=TrialRecoveryRecipient.Status.SCHEDULED,
            next_send_at__isnull=False,
        ).order_by("next_send_at")
    )
    for recipient in scheduled_recipients:
        if _is_local_today(recipient.next_send_at, recipient.timezone, now, campaign.fallback_timezone):
            remaining_today.append(recipient)

    first_message_waiting = recipients.filter(
        status=TrialRecoveryRecipient.Status.SCHEDULED,
        sent_count=0,
    ).count()
    second_message_waiting = recipients.filter(
        status=TrialRecoveryRecipient.Status.SCHEDULED,
        sent_count=1,
    ).count()
    final_message_waiting = recipients.filter(
        status=TrialRecoveryRecipient.Status.SCHEDULED,
        sent_count=2,
    ).count()
    due_now = recipients.filter(
        status=TrialRecoveryRecipient.Status.SCHEDULED,
        next_send_at__lte=now,
    ).count()
    next_recipient = next((recipient for recipient in scheduled_recipients if recipient.next_send_at >= now), None)

    today_sent_users = len({delivery.recipient_id for delivery in sent_today_rows})
    today_plan = today_sent_users + len(remaining_today) + len(failed_today_rows)
    today_processed = today_sent_users + len(failed_today_rows)

    reason_rows = [
        {
            "code": row["reason"],
            "label": REASON_LABELS["uk"].get(row["reason"], row["reason"]),
            "count": row["count"],
            "percent": _percent(row["count"], responses),
        }
        for row in recipients.exclude(reason="")
        .values("reason")
        .annotate(count=Count("id"))
        .order_by("-count", "reason")
    ]

    status_counts = Counter(recipients.values_list("status", flat=True))
    status_rows = [
        {"label": label, "count": status_counts.get(status, 0)}
        for status, label in RECIPIENT_STATUS_LABELS.items()
        if status_counts.get(status, 0)
    ]

    snapshot = campaign.audience_snapshot or {}
    uses_registered_audience = campaign_uses_registered_without_card_audience(campaign)
    excluded = snapshot.get("excluded") or {}
    exclusion_rows = [
        {"label": EXCLUSION_LABELS.get(reason, reason), "count": count}
        for reason, count in sorted(excluded.items(), key=lambda item: (-item[1], item[0]))
    ]

    action = None
    expand_action = None
    if campaign.status == TrialRecoveryCampaign.Status.DRAFT:
        action = {
            "label": "Перевірити й запустити",
            "url": reverse("admin:subscriptions_trialrecoverycampaign_launch", args=[campaign.pk]),
            "tone": "primary",
        }
    elif campaign.status in {TrialRecoveryCampaign.Status.SCHEDULED, TrialRecoveryCampaign.Status.RUNNING}:
        action = {
            "label": "Призупинити кампанію",
            "url": reverse("admin:subscriptions_trialrecoverycampaign_pause", args=[campaign.pk]),
            "tone": "danger",
        }
        if not uses_registered_audience:
            expand_action = {
                "label": "Розширити на всіх без картки",
                "url": reverse("admin:subscriptions_trialrecoverycampaign_expand", args=[campaign.pk]),
            }

    return {
        "status": CAMPAIGN_STATUS_LABELS.get(campaign.status, campaign.status),
        "status_tone": CAMPAIGN_STATUS_TONES.get(campaign.status, "neutral"),
        "action": action,
        "expand_action": expand_action,
        "today_label": _today_label(now, campaign.fallback_timezone),
        "checked_at": _date_label(now, campaign.fallback_timezone, campaign.fallback_timezone),
        "window": f"{campaign.send_window_start:%H:%M}–{campaign.send_window_end:%H:%M}",
        "fallback_timezone": campaign.fallback_timezone,
        "launch_at": _date_label(campaign.launch_at, campaign.fallback_timezone, campaign.fallback_timezone) if campaign.launch_at else "Ще не запускалась",
        "today": {
            "reached": today_sent_users,
            "messages": len(sent_today_rows),
            "remaining": len(remaining_today),
            "remaining_first": sum(1 for recipient in remaining_today if recipient.sent_count == 0),
            "remaining_followups": sum(1 for recipient in remaining_today if recipient.sent_count > 0),
            "failed": len(failed_today_rows),
            "recovered": recovered_today,
            "plan": today_plan,
            "processed": today_processed,
            "progress": _percent(today_processed, today_plan),
            "progress_css": round(today_processed * 100 / today_plan, 2) if today_plan else 0,
        },
        "totals": {
            "recipients": total_recipients,
            "reached": reached_users,
            "messages": sent_messages,
            "responses": responses,
            "contacts": contact_requests,
            "opted_out": opted_out,
            "converted_total": converted_total,
            "recovered": recovered_users,
            "converted_before_message": converted_before_message,
            "failed": failed_deliveries,
            "blocked": blocked_deliveries,
            "technical_failed": max(failed_deliveries - blocked_deliveries, 0),
            "response_rate": _percent(responses, reached_users),
            "recovery_rate": _percent(recovered_users, reached_users),
        },
        "schedule": {
            "first_waiting": first_message_waiting,
            "second_waiting": second_message_waiting,
            "final_waiting": final_message_waiting,
            "due_now": due_now,
            "next_at": _date_label(next_recipient.next_send_at, next_recipient.timezone, campaign.fallback_timezone) if next_recipient else "Немає запланованих доставок",
            "next_timezone": next_recipient.timezone if next_recipient else "",
        },
        "audience": {
            "source_total": snapshot.get("total_registered_users", snapshot.get("total_offer_users", 0)),
            "source_label": "зареєстровані загалом" if uses_registered_audience else "бачили 90-денний офер",
            "mode_label": "Усі зареєстровані без успішної оплати та картки" if uses_registered_audience else "Лише користувачі з 90-денним офером",
            "eligible_at_launch": snapshot.get("eligible", 0),
            "eligible_label": "додано при розширенні" if snapshot.get("initial_audience_snapshot") else "потрапили на старті",
            "current_recipients": total_recipients,
            "live_after_launch": recipients.filter(source=TrialRecoveryRecipient.Source.LIVE).count(),
            "onboarding_complete": recipients.filter(user__onboarding_completed=True).count(),
            "onboarding_incomplete": recipients.filter(user__onboarding_completed=False).count(),
            "excluded": exclusion_rows,
        },
        "reason_rows": reason_rows,
        "status_rows": status_rows,
        "recent_deliveries": _recent_deliveries(campaign),
        "recent_signals": _recent_signals(campaign),
        "recipients_url": reverse("admin:subscriptions_trialrecoveryrecipient_changelist"),
        "deliveries_url": reverse("admin:subscriptions_trialrecoverydelivery_changelist"),
    }
