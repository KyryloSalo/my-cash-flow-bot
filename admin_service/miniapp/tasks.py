from __future__ import annotations

from datetime import timedelta
import logging

from celery import shared_task
from django.db.models import Q
from django.utils import timezone

from miniapp.models import AppNotification, NotificationPreference
from miniapp.push import deliver_notification, emit_notification
from miniapp.retention import purge_expired_auth_token_uses
from subscriptions.models import Subscription
from transactions.models import Debt


logger = logging.getLogger(__name__)


@shared_task(name="miniapp.purge_expired_auth_token_uses")
def purge_expired_auth_token_uses_task() -> dict[str, int]:
    return purge_expired_auth_token_uses()


@shared_task(name="miniapp.dispatch_pending_pushes")
def dispatch_pending_pushes() -> dict[str, int]:
    delivered = 0
    checked = 0
    cutoff = timezone.now() - timedelta(days=7)
    for notification in AppNotification.objects.filter(status=AppNotification.Status.UNREAD, created_at__gte=cutoff).order_by("created_at")[:500]:
        checked += 1
        delivered += deliver_notification(notification)
    return {"checked": checked, "delivered": delivered}


@shared_task(name="miniapp.emit_billing_lifecycle_pushes")
def emit_billing_lifecycle_pushes() -> dict[str, int]:
    now = timezone.now()
    today = timezone.localdate()
    created = 0
    active = Subscription.objects.filter(status__in={Subscription.Status.TRIAL, Subscription.Status.ACTIVE, Subscription.Status.PAID})
    for subscription in active.filter(status=Subscription.Status.TRIAL, expires_at__date=today + timedelta(days=3))[:500]:
        _notice, _delivered, was_created = emit_notification(
            subscription.user_id,
            "trial_ending",
            idempotency_key=f"trial-ending:{subscription.pk}:{subscription.expires_at.date().isoformat()}",
            context={"subscription_id": subscription.pk},
        )
        created += int(was_created)
    for subscription in Subscription.objects.filter(grace_expires_at__date=today + timedelta(days=1))[:500]:
        _notice, _delivered, was_created = emit_notification(
            subscription.user_id,
            "grace_ending",
            idempotency_key=f"grace-ending:{subscription.pk}:{subscription.grace_expires_at.date().isoformat()}",
            context={"subscription_id": subscription.pk},
        )
        created += int(was_created)
    blocked = Subscription.objects.filter(
        Q(grace_expires_at__lt=now) | Q(status=Subscription.Status.EXPIRED, expires_at__lt=now)
    )
    for subscription in blocked[:500]:
        marker = subscription.grace_expires_at or subscription.expires_at or subscription.updated_at
        _notice, _delivered, was_created = emit_notification(
            subscription.user_id,
            "access_blocked",
            idempotency_key=f"access-blocked:{subscription.pk}:{marker.date().isoformat()}",
            context={"subscription_id": subscription.pk},
        )
        created += int(was_created)
    return {"created": created}


@shared_task(name="miniapp.emit_debt_due_pushes")
def emit_debt_due_pushes() -> dict[str, int]:
    today = timezone.localdate()
    created = 0
    debts = Debt.objects.filter(status__in={"active", "partially_paid"}, remaining_amount__gt=0, due_date__isnull=False)
    stages = (
        ("debt_due_soon", debts.filter(due_date=today + timedelta(days=3))),
        ("debt_due_today", debts.filter(due_date=today)),
        ("debt_overdue", debts.filter(due_date__lt=today)),
    )
    for event_type, queryset in stages:
        for debt in queryset[:500]:
            overdue_marker = f":{today.isocalendar().year}-W{today.isocalendar().week}" if event_type == "debt_overdue" else ""
            _notice, _delivered, was_created = emit_notification(
                debt.tg_user_id,
                event_type,
                idempotency_key=f"{event_type}:{debt.pk}:{debt.due_date.isoformat()}{overdue_marker}",
                context={"debt_id": debt.pk},
            )
            created += int(was_created)
    return {"created": created}


@shared_task(name="miniapp.emit_weekly_summary_pushes")
def emit_weekly_summary_pushes() -> dict[str, int]:
    today = timezone.localdate()
    created = 0
    for preference in NotificationPreference.objects.filter(master_enabled=True, weekly_summary_enabled=True)[:1000]:
        _notice, _delivered, was_created = emit_notification(
            preference.tg_user_id,
            "weekly_summary",
            idempotency_key=f"weekly-summary:{today.isocalendar().year}-W{today.isocalendar().week}",
            context={},
        )
        created += int(was_created)
    return {"created": created}
