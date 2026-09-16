from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
import json
import logging
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from miniapp.models import AppNotification, NotificationPreference, PushDelivery, WebPushSubscription
from users.models import TelegramUser


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventCopy:
    category: str
    uk_title: str
    uk_body: str
    en_title: str
    en_body: str
    target: str
    requires_action: bool = True
    badge: bool = True
    critical: bool = False


EVENTS: dict[str, EventCopy] = {
    "daily_expense_missing": EventCopy("daily_expenses", "Закриємо день?", "За сьогодні ще немає витрат. Додайте їх або відмітьте день без витрат.", "Close the day?", "There are no expenses for today yet. Add them or mark a no-spend day.", "/app/?screen=add&notice=daily-expense"),
    "saving_due": EventCopy("savings", "Час поповнити накопичення", "Ваш план готовий. Відкрийте Vydno, коли будете готові підтвердити переказ.", "Time to add to savings", "Your plan is ready. Open Vydno when you are ready to confirm the transfer.", "/app/?screen=money&tab=goals"),
    "debt_due_soon": EventCopy("debts", "Наближається дата повернення", "Перевірте актуальний залишок і деталі боргу.", "Repayment date is approaching", "Review the current balance and debt details.", "/app/?screen=money&tab=debts"),
    "debt_due_today": EventCopy("debts", "Сьогодні дата повернення", "Відкрийте борг, щоб зафіксувати повернення або оновити дані.", "Repayment is due today", "Open the debt to record a repayment or update its details.", "/app/?screen=money&tab=debts"),
    "debt_overdue": EventCopy("debts", "Дата повернення минула", "Перевірте статус боргу та за потреби оновіть запис.", "Repayment date has passed", "Review the debt status and update the record if needed.", "/app/?screen=money&tab=debts"),
    "debt_monthly": EventCopy("debts", "Нагадування про борг", "Перевірте актуальний залишок і деталі повернення.", "Debt reminder", "Review the current balance and repayment details.", "/app/?screen=money&tab=debts"),
    "trial_ending": EventCopy("billing", "Пробний період завершується", "Залишилося 3 дні. Перевірте картку й умови продовження.", "Your trial is ending", "Three days remain. Review your card and renewal settings.", "/app/?screen=settings&section=billing", critical=True),
    "billing_action_required": EventCopy("billing", "Підтвердіть оплату", "Monobank очікує підтвердження. Відкрийте Vydno, щоб завершити оплату.", "Confirm the payment", "Monobank is waiting for confirmation. Open Vydno to finish the payment.", "/app/?screen=settings&section=billing", critical=True),
    "billing_failed": EventCopy("billing", "Не вдалося продовжити підписку", "Доступ поки збережено. Оновіть картку або повторіть оплату.", "Subscription renewal failed", "Your access is temporarily preserved. Update the card or retry the payment.", "/app/?screen=settings&section=billing", critical=True),
    "grace_ending": EventCopy("billing", "Повний доступ скоро буде призупинено", "Повторіть оплату, щоб продовжити користуватися всіма функціями.", "Full access will pause soon", "Retry the payment to keep using all features.", "/app/?screen=settings&section=billing", critical=True),
    "access_blocked": EventCopy("billing", "Потрібно відновити підписку", "Термін оплати минув. Відкрийте Vydno, щоб повернути повний доступ.", "Restore your subscription", "The payment period ended. Open Vydno to restore full access.", "/app/?screen=settings&section=billing", critical=True),
    "billing_paid_expense": EventCopy("billing", "Підписку продовжено", "Оплату отримано. Підтвердіть витрату, щоб додати її в облік.", "Subscription renewed", "Payment received. Confirm the expense to add it to your records.", "/app/?screen=settings&section=billing-expense", critical=True),
    "family_member_joined": EventCopy("family", "Новий учасник бюджету", "До вашого сімейного бюджету приєднався новий учасник.", "New budget member", "A new member joined your family budget.", "/app/?screen=money&tab=family", requires_action=False, badge=False),
    "family_member_left": EventCopy("family", "Змінився склад сім’ї", "Один з учасників вийшов зі спільного бюджету.", "Family membership changed", "A member left the shared budget.", "/app/?screen=money&tab=family", requires_action=False, badge=False),
    "family_access_removed": EventCopy("family", "Доступ до сімейного бюджету змінено", "Ви більше не маєте доступу до спільного бюджету.", "Family budget access changed", "You no longer have access to the shared budget.", "/app/?screen=money&tab=family", critical=True),
    "weekly_summary": EventCopy("weekly_summary", "Ваш тиждень у Vydno", "Фінансовий підсумок готовий: доходи, витрати та чистий потік.", "Your week in Vydno", "Your financial summary is ready: income, expenses, and net flow.", "/app/?screen=analytics", requires_action=False, badge=False),
    "goal_milestone": EventCopy("savings", "Ціль стала ближчою", "Відкрийте Vydno, щоб переглянути оновлений прогрес накопичення.", "Your goal is closer", "Open Vydno to see the updated savings progress.", "/app/?screen=money&tab=goals", requires_action=False, badge=False),
    "low_balance": EventCopy("debts", "Баланс потребує уваги", "Один із рахунків досяг установленого вами порогу.", "Balance needs attention", "An account reached the threshold you configured.", "/app/?screen=money&tab=accounts"),
    "credit_limit": EventCopy("debts", "Наближення до кредитного ліміту", "Перевірте доступний залишок кредитного рахунку.", "Credit limit is near", "Review the available amount on your credit account.", "/app/?screen=money&tab=accounts"),
    "new_device_login": EventCopy("billing", "Новий вхід у Vydno", "Якщо це були не ви, завершіть інші сесії та зверніться в підтримку.", "New Vydno sign-in", "If this was not you, end other sessions and contact support.", "/app/?screen=settings&section=devices", critical=True),
    "test": EventCopy("billing", "Vydno на зв’язку", "Все працює — важливі нагадування приходитимуть сюди.", "Vydno is connected", "Everything works — important reminders will arrive here.", "/app/?screen=notifications", requires_action=False, badge=False, critical=True),
}


CATEGORY_FIELD = {
    "billing": "billing_enabled",
    "daily_expenses": "daily_expenses_enabled",
    "savings": "savings_enabled",
    "debts": "debts_enabled",
    "family": "family_enabled",
    "weekly_summary": "weekly_summary_enabled",
}


def get_preferences(user_id: int) -> NotificationPreference:
    preference, _ = NotificationPreference.objects.get_or_create(tg_user_id=int(user_id))
    return preference


def unread_badge_count(user_id: int) -> int:
    return min(
        AppNotification.objects.filter(
            tg_user_id=int(user_id),
            status=AppNotification.Status.UNREAD,
            contributes_to_badge=True,
        ).count(),
        99,
    )


def _locale_for(user: TelegramUser | None) -> str:
    return "en" if str(getattr(user, "lang", "") or "").lower().startswith("en") else "uk"


def _quiet_now(preference: NotificationPreference) -> bool:
    if not preference.quiet_hours_enabled:
        return False
    local_time = timezone.localtime().time().replace(tzinfo=None)
    start = preference.quiet_hours_start or time(22, 0)
    end = preference.quiet_hours_end or time(8, 0)
    return start <= local_time < end if start < end else local_time >= start or local_time < end


def notification_allowed(preference: NotificationPreference, event: EventCopy) -> bool:
    if not preference.master_enabled:
        return False
    field = CATEGORY_FIELD.get(event.category)
    if field and not bool(getattr(preference, field, True)):
        return False
    return event.critical or not _quiet_now(preference)


def serialize_notification(notification: AppNotification) -> dict[str, Any]:
    return {
        "id": notification.id,
        "event_type": notification.event_type,
        "category": notification.category,
        "title": notification.title,
        "body": notification.body,
        "target_url": notification.target_url,
        "requires_action": notification.requires_action,
        "status": notification.status,
        "created_at": notification.created_at.isoformat(),
    }


def _push_payload(notification: AppNotification) -> dict[str, Any]:
    data = dict(notification.payload or {})
    return {
        "title": notification.title,
        "body": notification.body,
        "icon": "/static/miniapp/icon-192.png",
        "badge": "/static/miniapp/favicon-48.png",
        "tag": f"vydno-{notification.event_type}-{notification.id}",
        "renotify": bool(notification.requires_action),
        "url": notification.target_url,
        "notification_id": notification.id,
        "badge_count": unread_badge_count(notification.tg_user_id),
        "data": data,
    }


def deliver_notification(notification: AppNotification) -> int:
    event = EVENTS.get(notification.event_type)
    if event is None:
        return 0
    preference = get_preferences(notification.tg_user_id)
    if not notification_allowed(preference, event):
        return 0
    private_key = str(getattr(settings, "WEB_PUSH_VAPID_PRIVATE_KEY", "") or "").strip()
    subject = str(getattr(settings, "WEB_PUSH_VAPID_SUBJECT", "mailto:support@vydno.capital") or "").strip()
    if not private_key:
        return 0
    try:
        from pywebpush import WebPushException, webpush
    except ImportError:
        logger.error("pywebpush is not installed; browser push delivery is disabled")
        return 0

    sent = 0
    payload = json.dumps(_push_payload(notification), ensure_ascii=False)
    subscriptions = WebPushSubscription.objects.filter(tg_user_id=notification.tg_user_id, is_active=True)
    for subscription in subscriptions:
        delivery, _ = PushDelivery.objects.get_or_create(notification=notification, subscription=subscription)
        if delivery.status == PushDelivery.Status.SENT:
            continue
        try:
            response = webpush(
                subscription_info={
                    "endpoint": subscription.endpoint,
                    "keys": {"p256dh": subscription.p256dh, "auth": subscription.auth},
                },
                data=payload,
                vapid_private_key=private_key,
                vapid_claims={"sub": subject},
                ttl=int(getattr(settings, "WEB_PUSH_TTL_SECONDS", 86400) or 86400),
            )
            now = timezone.now()
            delivery.status = PushDelivery.Status.SENT
            delivery.response_code = int(getattr(response, "status_code", 201) or 201)
            delivery.error = ""
            delivery.sent_at = now
            delivery.save()
            subscription.failure_count = 0
            subscription.last_success_at = now
            subscription.save(update_fields=["failure_count", "last_success_at", "updated_at"])
            sent += 1
        except WebPushException as exc:
            response = getattr(exc, "response", None)
            status_code = int(getattr(response, "status_code", 0) or 0)
            gone = status_code in {404, 410}
            delivery.status = PushDelivery.Status.GONE if gone else PushDelivery.Status.FAILED
            delivery.response_code = status_code or None
            delivery.error = str(exc)[:1000]
            delivery.save()
            subscription.failure_count = min(subscription.failure_count + 1, 32767)
            subscription.last_failure_at = timezone.now()
            if gone or subscription.failure_count >= 5:
                subscription.is_active = False
            subscription.save(update_fields=["failure_count", "last_failure_at", "is_active", "updated_at"])
            logger.warning("Web Push failed for subscription %s: %s", subscription.pk, exc)
        except (TypeError, ValueError) as exc:
            # Configuration/key parsing errors are server-side and must not
            # deactivate an otherwise valid device subscription or crash Celery/API.
            delivery.status = PushDelivery.Status.FAILED
            delivery.response_code = None
            delivery.error = str(exc)[:1000]
            delivery.save(update_fields=["status", "response_code", "error", "updated_at"])
            logger.exception("Web Push configuration failed for subscription %s", subscription.pk)
    return sent


def emit_notification(
    user_id: int,
    event_type: str,
    *,
    context: dict[str, Any] | None = None,
    idempotency_key: str,
) -> tuple[AppNotification, int, bool]:
    event = EVENTS.get(str(event_type))
    if event is None:
        raise ValueError("Unsupported notification event.")
    user = TelegramUser.objects.filter(tg_user_id=int(user_id)).first()
    locale = _locale_for(user)
    title = event.en_title if locale == "en" else event.uk_title
    body = event.en_body if locale == "en" else event.uk_body
    payload = dict(context or {})
    with transaction.atomic():
        notification, created = AppNotification.objects.get_or_create(
            tg_user_id=int(user_id),
            idempotency_key=str(idempotency_key)[:160],
            defaults={
                "event_type": event_type,
                "category": event.category,
                "title": title,
                "body": body,
                "target_url": event.target,
                "payload": payload,
                "requires_action": event.requires_action,
                "contributes_to_badge": event.badge,
            },
        )
    delivered = deliver_notification(notification) if created else 0
    return notification, delivered, created


def preferences_payload(user_id: int) -> dict[str, Any]:
    preference = get_preferences(user_id)
    return {
        "master_enabled": preference.master_enabled,
        "billing_enabled": preference.billing_enabled,
        "daily_expenses_enabled": preference.daily_expenses_enabled,
        "savings_enabled": preference.savings_enabled,
        "debts_enabled": preference.debts_enabled,
        "family_enabled": preference.family_enabled,
        "weekly_summary_enabled": preference.weekly_summary_enabled,
        "show_sensitive_details": preference.show_sensitive_details,
        "quiet_hours_enabled": preference.quiet_hours_enabled,
        "quiet_hours_start": preference.quiet_hours_start.strftime("%H:%M"),
        "quiet_hours_end": preference.quiet_hours_end.strftime("%H:%M"),
    }


def update_preferences(user_id: int, payload: dict[str, Any]) -> NotificationPreference:
    preference = get_preferences(user_id)
    boolean_fields = set(CATEGORY_FIELD.values()) | {"master_enabled", "show_sensitive_details", "quiet_hours_enabled"}
    changed: list[str] = []
    for field in boolean_fields:
        if field in payload and isinstance(payload[field], bool):
            setattr(preference, field, payload[field])
            changed.append(field)
    for field in ("quiet_hours_start", "quiet_hours_end"):
        if field not in payload:
            continue
        try:
            parsed = datetime.strptime(str(payload[field]), "%H:%M").time()
        except ValueError as exc:
            raise ValueError(f"{field} must use HH:MM.") from exc
        setattr(preference, field, parsed)
        changed.append(field)
    if changed:
        preference.save(update_fields=[*changed, "updated_at"])
    return preference
