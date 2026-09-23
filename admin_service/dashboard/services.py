from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from math import ceil
from functools import partial
import logging

from django.conf import settings
from django.db.models import F, OuterRef, Q, Subquery
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.utils import timezone

from bot_events.models import BotEvent
from broadcasts.models import BroadcastRecipient
from common.admin_pages import can_use_test_tools
from common.healthcheck import _check_redis, _check_telegram_api, _check_worker
from dashboard.funnel import build_pwa_funnel_report
from subscriptions.models import Payment, Subscription, TrialRecoveryRecipient
from subscriptions.recovery_reporting import recovered_recipient_queryset
from users.models import TelegramUser


ZERO = Decimal("0.00")
MAX_CHART_BUCKETS = 31
MAX_CUSTOM_PERIOD_DAYS = 366
UK_MONTHS_SHORT = (
    "січ",
    "лют",
    "бер",
    "кві",
    "тра",
    "чер",
    "лип",
    "сер",
    "вер",
    "жов",
    "лис",
    "гру",
)
SUCCESS_PAYMENT_STATUSES = [
    Payment.Status.PAID,
    Payment.Status.REFUNDED,
]
ACTIVE_SUBSCRIPTION_STATUSES = {
    Subscription.Status.ACTIVE,
    Subscription.Status.PAID,
    Subscription.Status.MANUAL,
    Subscription.Status.LIFETIME,
}
USER_SEGMENT_FREE = "free"
USER_SEGMENT_TRIAL = "trial"
USER_SEGMENT_PAID = "paid"
USER_SEGMENT_GRANTED = "granted"
USER_SEGMENTS = (USER_SEGMENT_FREE, USER_SEGMENT_TRIAL, USER_SEGMENT_PAID, USER_SEGMENT_GRANTED)


logger = logging.getLogger(__name__)


def safe(callable_obj, default=None, *, errors=None):
    """Return an explicit unknown on failure, never the old plausible default.

    Log only failure type and code location: DB exceptions can contain SQL or
    sensitive values. The per-request collector exposes completeness to UI.
    """
    try:
        return callable_obj()
    except Exception as exc:
        failure = {"source": f"dashboard:{getattr(getattr(callable_obj, '__code__', None), 'co_firstlineno', 0)}",
                   "error_type": type(exc).__name__}
        logger.error("Metric read unavailable: %s (%s)", failure["source"], failure["error_type"])
        if errors is not None:
            errors.append(failure)
        return None


def admin_url(name: str, query: str = "") -> str:
    url = reverse(name)
    return f"{url}?{query}" if query else url


def _include_test_users(request) -> bool:
    return str(request.GET.get("include_test_users", "")).strip().lower() in {"1", "true", "yes", "on"}


def _dashboard_url(request, **updates) -> str:
    params = request.GET.copy()
    for key, value in updates.items():
        if value in {None, ""}:
            params.pop(key, None)
        else:
            params[key] = str(value)
    query = params.urlencode()
    base_url = reverse("admin:index")
    return f"{base_url}?{query}" if query else base_url


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _start_of_day(value: date):
    result = datetime.combine(value, time.min)
    if settings.USE_TZ:
        result = timezone.make_aware(result, timezone.get_current_timezone())
    return result


def _short_date(value: date) -> str:
    return f"{value.day} {UK_MONTHS_SHORT[value.month - 1]}"


def _period_label(start_date: date, end_date: date) -> str:
    if start_date == end_date:
        return f"{start_date.day} {UK_MONTHS_SHORT[start_date.month - 1]} {start_date.year}"
    if start_date.year == end_date.year:
        return f"{_short_date(start_date)} — {_short_date(end_date)} {end_date.year}"
    return f"{_short_date(start_date)} {start_date.year} — {_short_date(end_date)} {end_date.year}"


def _resolve_period(request, now) -> dict:
    today = timezone.localdate(now)
    period_key = str(request.GET.get("period", "30d")).strip().lower()
    period_error = ""

    if period_key == "today":
        start_date = today
        end_date = today
    elif period_key == "7d":
        start_date = today - timedelta(days=6)
        end_date = today
    elif period_key == "month":
        start_date = today.replace(day=1)
        end_date = today
    elif period_key == "90d":
        start_date = today - timedelta(days=89)
        end_date = today
    elif period_key == "custom":
        start_date = _parse_date(request.GET.get("start_date"))
        end_date = _parse_date(request.GET.get("end_date"))
        if not start_date or not end_date or start_date > end_date:
            period_error = "Перевірте початкову й кінцеву дати. Показано останні 30 днів."
            period_key = "30d"
            start_date = today - timedelta(days=29)
            end_date = today
        elif end_date > today:
            end_date = today
            period_error = "Майбутні дати не враховуються. Кінець періоду змінено на сьогодні."
            if start_date > end_date:
                period_key = "30d"
                start_date = today - timedelta(days=29)
                period_error = "Обраний період ще не настав. Показано останні 30 днів."
        if start_date and end_date and (end_date - start_date).days + 1 > MAX_CUSTOM_PERIOD_DAYS:
            start_date = end_date - timedelta(days=MAX_CUSTOM_PERIOD_DAYS - 1)
            period_error = f"Для швидкого огляду період обмежено {MAX_CUSTOM_PERIOD_DAYS} днями."
    else:
        period_key = "30d"
        start_date = today - timedelta(days=29)
        end_date = today

    start_at = _start_of_day(start_date)
    end_at = _start_of_day(end_date + timedelta(days=1))
    duration = end_at - start_at
    previous_start_at = start_at - duration
    previous_end_at = start_at

    preset_definitions = (
        ("today", "Сьогодні"),
        ("7d", "7 днів"),
        ("30d", "30 днів"),
        ("month", "Цей місяць"),
        ("90d", "90 днів"),
    )
    preset_links = []
    for key, label in preset_definitions:
        preset_links.append(
            {
                "key": key,
                "label": label,
                "active": key == period_key,
                "url": _dashboard_url(request, period=key, start_date=None, end_date=None),
            }
        )

    return {
        "key": period_key,
        "start_date": start_date,
        "end_date": end_date,
        "start_at": start_at,
        "end_at": end_at,
        "previous_start_at": previous_start_at,
        "previous_end_at": previous_end_at,
        "label": _period_label(start_date, end_date),
        "previous_label": _period_label(previous_start_at.date(), (previous_end_at - timedelta(days=1)).date()),
        "days": (end_date - start_date).days + 1,
        "error": period_error,
        "preset_links": preset_links,
    }


def _percentage(numerator: int | Decimal, denominator: int | Decimal) -> Decimal:
    if numerator is None or denominator is None:
        return None
    if not denominator:
        return ZERO
    return (Decimal(numerator) * Decimal("100") / Decimal(denominator)).quantize(Decimal("0.1"))


def _format_percent(value: Decimal) -> str:
    if value is None:
        return "Недоступно"
    return f"{value:.1f}".replace(".", ",") + "%"


def _change(current: int | Decimal, previous: int | Decimal, *, positive_is_good: bool = True) -> dict:
    if current is None or previous is None:
        return {"delta": "—", "delta_tone": "neutral"}
    current_value = Decimal(current or 0)
    previous_value = Decimal(previous or 0)
    if previous_value == ZERO:
        if current_value == ZERO:
            return {"delta": "0%", "delta_tone": "neutral"}
        return {"delta": "нова база", "delta_tone": "neutral"}
    change = ((current_value - previous_value) * Decimal("100") / abs(previous_value)).quantize(Decimal("1"))
    is_positive = change > 0
    is_negative = change < 0
    if not is_positive and not is_negative:
        tone = "neutral"
    elif (is_positive and positive_is_good) or (is_negative and not positive_is_good):
        tone = "positive"
    else:
        tone = "negative"
    sign = "+" if change > 0 else ""
    return {"delta": f"{sign}{change:.0f}%", "delta_tone": tone}


def _point_change(current: Decimal, previous: Decimal) -> dict:
    if current is None or previous is None:
        return {"delta": "—", "delta_tone": "neutral"}
    change = (current - previous).quantize(Decimal("0.1"))
    if change > 0:
        tone = "positive"
        sign = "+"
    elif change < 0:
        tone = "negative"
        sign = ""
    else:
        tone = "neutral"
        sign = ""
    return {"delta": f"{sign}{str(change).replace('.', ',')} в.п.", "delta_tone": tone}


def _subscription_segment(subscription: Subscription | None, *, payments=(), now=None) -> str:
    """Current personal entitlement, with separate evidence of monetization.

    This is not a family-access resolver, MRR, or an historical paid cohort.
    Manual/lifetime grants never prove payment; grace is not a paid period.
    """
    now = now or timezone.now()
    if subscription is None:
        return USER_SEGMENT_FREE
    if subscription.started_at and subscription.started_at > now:
        return USER_SEGMENT_FREE
    if subscription.status == Subscription.Status.LIFETIME:
        return USER_SEGMENT_GRANTED
    if not subscription.expires_at or subscription.expires_at <= now:
        return USER_SEGMENT_FREE
    if subscription.status == Subscription.Status.TRIAL:
        return USER_SEGMENT_TRIAL
    if subscription.status in ACTIVE_SUBSCRIPTION_STATUSES | {Subscription.Status.CANCELLED}:
        if subscription.status != Subscription.Status.MANUAL and subscription.source == Subscription.Source.PAYMENT:
            for payment in payments:
                if (subscription.pk is not None
                        and payment.subscription_id == subscription.pk
                        and payment.user_id == subscription.user_id
                        and payment.status == Payment.Status.PAID
                        and payment.kind in {Payment.Kind.RENEWAL, Payment.Kind.RETRY}
                        and payment.amount > ZERO
                        and payment.paid_at is not None
                        and subscription.started_at is not None
                        and subscription.started_at <= payment.paid_at <= now):
                    return USER_SEGMENT_PAID
        return USER_SEGMENT_GRANTED
    return USER_SEGMENT_FREE


def _build_user_segment_series(
    start_date: date,
    end_date: date,
    segment_counts: dict[str, dict[date, int]],
) -> list[dict]:
    total_days = (end_date - start_date).days + 1
    step = max(1, ceil(total_days / MAX_CHART_BUCKETS))
    raw_buckets = []
    cursor = start_date
    while cursor <= end_date:
        bucket_end = min(cursor + timedelta(days=step - 1), end_date)
        days = (bucket_end - cursor).days + 1
        values = {
            segment: sum(
                segment_counts.get(segment, {}).get(cursor + timedelta(days=offset), 0)
                for offset in range(days)
            )
            for segment in USER_SEGMENTS
        }
        label = _short_date(cursor) if cursor == bucket_end else f"{_short_date(cursor)}–{_short_date(bucket_end)}"
        raw_buckets.append(
            {
                "label": label,
                **values,
                "title": (
                    f"{label}: {values[USER_SEGMENT_FREE]} без підтвердженої чинної підписки, "
                    f"{values[USER_SEGMENT_TRIAL]} на trial, {values[USER_SEGMENT_PAID]} з оплатою renewal/retry, "
                    f"{values[USER_SEGMENT_GRANTED]} з наданим або непідтвердженим оплатою доступом"
                ),
            }
        )
        cursor = bucket_end + timedelta(days=1)

    scale = max(
        (max(item[segment] for segment in USER_SEGMENTS) for item in raw_buckets),
        default=0,
    ) or 1
    for item in raw_buckets:
        for segment in USER_SEGMENTS:
            item[f"{segment}_percent"] = round(item[segment] * 100 / scale, 2)
    return raw_buckets


def _test_user_q(prefix: str = "") -> Q:
    allowlisted_ids = list(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []))
    filters = Q(**{f"{prefix}admin_state__is_test_user": True})
    if allowlisted_ids:
        filters |= Q(**{f"{prefix}tg_user_id__in": allowlisted_ids})
    return filters


def _exclude_test_users(qs, *, prefix: str, include_test_users: bool):
    if include_test_users:
        return qs
    return qs.exclude(_test_user_q(prefix))


def _test_tool_links(request) -> dict:
    user = getattr(request, "user", None)
    if not user or not can_use_test_tools(user):
        return {"show": False}
    return {
        "show": True,
        "my_test_user_url": reverse("admin:my_test_user"),
        "reset_my_onboarding_url": reverse("admin:reset_my_onboarding"),
        "onboarding_debug_url": reverse("admin:onboarding_debug"),
    }


def _format_decimal(value: Decimal) -> str:
    if value is None:
        return "Недоступно"
    return f"{value.quantize(Decimal('0.01')):,.2f}".replace(",", " ")


def _format_currency_totals(rows: list[dict]) -> str:
    if rows is None:
        return "Недоступно"
    parts = []
    for row in rows:
        total = row.get("total", ZERO) or ZERO
        currency = (row.get("currency") or "").strip() or "UAH"
        parts.append(f"{_format_decimal(total)} {currency}")
    return " | ".join(parts) if parts else "0"


def _cash_summary(payments, start_at, end_at) -> dict:
    """Event-time cash, not recognized revenue or MRR.

    paid_at survives full refund status changes. Only the existing successful
    refund_request.processed_at proves the refund date; updated_at, a manual
    status change, or invoice creation cannot reconstruct it. The billing flow
    records full refunds only. Undated records make the affected total unknown.
    """
    gross, refunds, manual = {}, {}, {}
    kind_cash = {"bind": {}, "recurring": {}, "other": {}}
    kind_users = {kind: set() for kind in kind_cash}
    payers = set()
    captures = refund_count = 0
    gross_known = refunds_known = manual_known = payments is not None
    for payment in payments or ():
        capture = payment.status in SUCCESS_PAYMENT_STATUSES
        manual_confirmation = payment.status == Payment.Status.MANUAL_CONFIRMED
        if not capture and not manual_confirmation:
            continue
        currency = (payment.currency or "").strip()
        valid_amount = payment.amount is not None and payment.amount > ZERO and bool(currency)
        paid_at = payment.paid_at
        dated = paid_at is not None and timezone.is_aware(paid_at) == timezone.is_aware(start_at)
        if not valid_amount or not dated:
            if capture:
                gross_known = False
            else:
                manual_known = False
        elif start_at <= paid_at < end_at:
            target = gross if capture else manual
            target[currency] = target.get(currency, ZERO) + payment.amount
            captures += int(capture)
            if capture:
                kind = ("bind" if payment.kind == Payment.Kind.BIND else
                        "recurring" if payment.kind in {Payment.Kind.RENEWAL, Payment.Kind.RETRY} else "other")
                kind_cash[kind][currency] = kind_cash[kind].get(currency, ZERO) + payment.amount
                kind_users[kind].add(payment.user_id)
                payers.add(payment.user_id)
        if payment.status == Payment.Status.REFUNDED or getattr(payment, "refund_status", None) == "success":
            processed = getattr(payment, "refund_processed_at", None)
            try:
                refunded_at = datetime.fromisoformat(processed) if isinstance(processed, str) else processed
                valid_refund = (getattr(payment, "refund_status", None) == "success"
                                and isinstance(refunded_at, datetime)
                                and timezone.is_aware(refunded_at) == timezone.is_aware(start_at)
                                and valid_amount)
            except (TypeError, ValueError):
                valid_refund = False
            if not valid_refund:
                refunds_known = False
            elif start_at <= refunded_at < end_at:
                refunds[currency] = refunds.get(currency, ZERO) + payment.amount
                refund_count += 1
    net = {currency: gross.get(currency, ZERO) - refunds.get(currency, ZERO)
           for currency in sorted(set(gross) | set(refunds))} if gross_known and refunds_known else None
    return {
        "gross": gross if gross_known else None,
        "refunds": refunds if refunds_known else None,
        "net": net,
        "manual_confirmed": manual if manual_known else None,
        "capture_count": captures if gross_known else None,
        "payer_count": len(payers) if gross_known else None,
        **{f"{kind}_cash": totals if gross_known else None for kind, totals in kind_cash.items()},
        **{f"{kind}_payers": len(users) if gross_known else None for kind, users in kind_users.items()},
        "refund_count": refund_count if refunds_known else None,
        "reason": "" if gross_known and refunds_known and manual_known else (
            "Неповні дані: немає підтвердженої дати оплати / повернення або суми / валюти. "
            "created_at та updated_at не замінюють час руху коштів."
        ),
    }


def _cash_rows(totals):
    return None if totals is None else [{"currency": currency, "total": amount} for currency, amount in sorted(totals.items())]


def _subscription_snapshot(subscriptions_qs, payments_qs, *, now, read):
    """Canonical current classification shared by Overview and operator KPIs."""
    latest_subscription_qs = subscriptions_qs.annotate(
        latest_pk=Subquery(
            Subscription.objects.filter(user_id=OuterRef("user_id")).order_by("-created_at", "-pk").values("pk")[:1]
        )
    ).filter(pk=F("latest_pk"))
    latest_subscriptions = read(lambda: list(latest_subscription_qs), None)
    latest_by_user = {subscription.user_id: subscription for subscription in latest_subscriptions or []}
    payment_evidence = read(lambda: list(payments_qs.filter(
        status=Payment.Status.PAID, kind__in=[Payment.Kind.RENEWAL, Payment.Kind.RETRY],
        paid_at__lte=now, amount__gt=ZERO,
    ).only("subscription_id", "user_id", "status", "kind", "amount", "paid_at")), None)
    payments_by_subscription = {}
    for payment in payment_evidence or []:
        payments_by_subscription.setdefault(payment.subscription_id, []).append(payment)
    segment_by_user = {
        subscription.user_id: _subscription_segment(
            subscription, payments=payments_by_subscription.get(subscription.pk, ()), now=now,
        ) for subscription in latest_subscriptions or []
    }
    return latest_by_user, segment_by_user, latest_subscriptions is not None and payment_evidence is not None


def build_current_user_metrics(*, include_test_users=False, now=None) -> dict:
    """JSON-safe operator integration seam; no health probes or write effects.

    Use metrics directly in operator payload. All hints expose scope/source/time;
    values are nullable and never use the UserAdminState.PAID projection.
    """
    now = now or timezone.now()
    errors = []
    read = partial(safe, errors=errors)
    users = _exclude_test_users(TelegramUser.objects.all(), prefix="", include_test_users=include_test_users)
    subscriptions = _exclude_test_users(Subscription.objects.all(), prefix="user__", include_test_users=include_test_users)
    payments = _exclude_test_users(Payment.objects.all(), prefix="user__", include_test_users=include_test_users)
    _, segments, source_available = _subscription_snapshot(subscriptions, payments, now=now, read=read)
    total = read(users.count)
    available = source_available and total is not None and len(segments) <= total
    paid = sum(segment == USER_SEGMENT_PAID for segment in segments.values()) if available else None
    granted = sum(segment == USER_SEGMENT_GRANTED for segment in segments.values()) if available else None
    last_seen = read(lambda: users.filter(last_seen_at__gte=now-timedelta(hours=24), last_seen_at__lte=now).count())
    incomplete = read(lambda: users.filter(onboarding_completed=False).count())
    scope = "Усі користувачі, включно з тестовими" if include_test_users else "Тільки реальні користувачі"
    rows = [
        ("Користувачі", total, "поточна база users", "admin:users_telegramuser_changelist"),
        ("Останній візит за 24 год", last_seen, "поточний last_seen_at, не історична активність", "admin:users_telegramuser_changelist"),
        ("Чинна підписка з оплатою renewal/retry", paid, "latest Subscription + пов'язаний PAID payment; не MRR", "admin:subscriptions_subscription_changelist"),
        ("Не завершили онбординг зараз", incomplete, "поточний прапорець, не історична конверсія", "admin:users_telegramuser_changelist"),
    ]
    return {
        "total_users": total, "paid_users": paid, "granted_users": granted,
        "include_test_users": include_test_users, "scope_label": scope, "retrieved_at": now.isoformat(),
        "degraded": bool(errors) or not available, "query_errors": errors,
        "metrics": [{"label": label, "value": value, "available": value is not None,
                     "hint": f"{scope} · {source} · запит {now.isoformat()}",
                     "reason": "" if value is not None else "Дані недоступні; не нуль.",
                     "url": admin_url(url)} for label, value, source, url in rows],
    }


def build_dashboard_context(request) -> dict:
    query_errors = []
    read = partial(safe, errors=query_errors)
    now = timezone.now()
    include_test_users = _include_test_users(request)
    period = _resolve_period(request, now)
    try:
        pwa_funnel = build_pwa_funnel_report(
            start_at=period["start_at"],
            end_at=period["end_at"],
            include_test_users=include_test_users,
        )
    except Exception as exc:
        logger.error("Optional PWA funnel read unavailable (%s)", type(exc).__name__)
        pwa_funnel = {
            "available": False,
            "cohort_sessions": None,
            "anonymous_unclassified": None,
            "excluded_internal": None,
            "excluded_automation": None,
            "stages": [],
            "reason": "Дані PWA-воронки недоступні; це не нуль.",
        }
    if pwa_funnel.get("available") is not False:
        pwa_funnel = {**pwa_funnel, "available": True, "reason": ""}

    users_qs = _exclude_test_users(TelegramUser.objects.all(), prefix="", include_test_users=include_test_users)
    subscriptions_qs = _exclude_test_users(
        Subscription.objects.select_related("plan_ref"),
        prefix="user__",
        include_test_users=include_test_users,
    )
    payments_qs = _exclude_test_users(Payment.objects.all(), prefix="user__", include_test_users=include_test_users)
    bot_events_qs = _exclude_test_users(BotEvent.objects.all(), prefix="user__", include_test_users=include_test_users)
    recipient_logs_qs = _exclude_test_users(
        BroadcastRecipient.objects.all(),
        prefix="user__",
        include_test_users=include_test_users,
    )
    recovery_recipients_qs = _exclude_test_users(
        TrialRecoveryRecipient.objects.all(),
        prefix="user__",
        include_test_users=include_test_users,
    )

    latest_subscription_by_user, segment_by_user, segment_source_available = _subscription_snapshot(
        subscriptions_qs, payments_qs, now=now, read=read,
    )

    cash_payments = read(lambda: list(payments_qs.filter(
        status__in=[*SUCCESS_PAYMENT_STATUSES, Payment.Status.MANUAL_CONFIRMED],
    ).annotate(
        refund_status=F("raw_payload__refund_request__status"),
        refund_processed_at=F("raw_payload__refund_request__processed_at"),
    ).only("user_id", "status", "kind", "paid_at", "amount", "currency")), None)
    cash = _cash_summary(cash_payments, period["start_at"], period["end_at"])
    previous_cash = _cash_summary(cash_payments, period["previous_start_at"], period["previous_end_at"])
    successful_payments_qs = payments_qs.filter(
        status__in=SUCCESS_PAYMENT_STATUSES, paid_at__isnull=False,
    ).annotate(effective_paid_at=F("paid_at"))
    period_users_qs = users_qs.filter(created_at__gte=period["start_at"], created_at__lt=period["end_at"])
    previous_period_users_qs = users_qs.filter(
        created_at__gte=period["previous_start_at"],
        created_at__lt=period["previous_end_at"],
    )
    period_payments_qs = successful_payments_qs.filter(
        effective_paid_at__gte=period["start_at"],
        effective_paid_at__lt=period["end_at"],
    )

    latest_payments = read(
        lambda: list(
            period_payments_qs.select_related("user").only(
                "user_id", "user__tg_user_id", "user__first_name", "user__last_name",
                "kind", "status", "amount", "currency", "paid_at", "updated_at",
            )
            .order_by("-effective_paid_at", "-updated_at")[:10]
        ),
        [],
    )
    payment_kind_labels = {
        Payment.Kind.BIND: "Прив’язка картки (bind)",
        Payment.Kind.RENEWAL: "Продовження",
        Payment.Kind.RETRY: "Повторна оплата",
        Payment.Kind.LEGACY: "Інша оплата",
    }
    for payment in latest_payments or []:
        payment.dashboard_kind_label = payment_kind_labels.get(payment.kind, "Оплата")
        payment.dashboard_status_label = (
            "Сплачено" if payment.status == Payment.Status.PAID else "Повернено після оплати"
        )

    total_users_count = read(lambda: users_qs.count(), 0)
    segments_available = (total_users_count is not None and segment_source_available
                          and len(segment_by_user) <= total_users_count)
    period_user_count = read(lambda: period_users_qs.count(), 0)
    previous_period_user_count = read(lambda: previous_period_users_qs.count(), 0)
    # last_seen_at is a mutable snapshot, not an activity event history.
    period_active_user_count = None
    previous_period_active_user_count = None
    period_last_seen_count = read(
        lambda: users_qs.filter(last_seen_at__gte=period["start_at"], last_seen_at__lt=period["end_at"]).count(),
        0,
    )
    period_onboarded_count = None
    previous_period_onboarded_count = None
    cohort_onboarded_now = read(lambda: period_users_qs.filter(onboarding_completed=True).count(), 0)
    period_onboarding_rate = _percentage(period_onboarded_count, period_user_count)
    previous_period_onboarding_rate = _percentage(previous_period_onboarded_count, previous_period_user_count)
    period_payment_count = cash["capture_count"]
    previous_period_payment_count = previous_cash["capture_count"]
    period_payer_count = cash["payer_count"]
    period_recovered_count = read(
        lambda: recovered_recipient_queryset(recovery_recipients_qs).filter(
            converted_at__gte=period["start_at"],
            converted_at__lt=period["end_at"],
        ).count(),
        0,
    )
    previous_period_recovered_count = read(
        lambda: recovered_recipient_queryset(recovery_recipients_qs).filter(
            converted_at__gte=period["previous_start_at"],
            converted_at__lt=period["previous_end_at"],
        ).count(),
        0,
    )

    period_revenue_totals = _cash_rows(cash["gross"])
    period_revenue_by_currency = cash["gross"]
    previous_revenue_by_currency = previous_cash["gross"]
    available_currencies = sorted(set(period_revenue_by_currency or {}) | set(previous_revenue_by_currency or {}))
    primary_currency = "UAH" if "UAH" in available_currencies else (available_currencies[0] if available_currencies else "UAH")
    primary_revenue = period_revenue_by_currency.get(primary_currency, ZERO) if period_revenue_by_currency is not None else None
    previous_primary_revenue = previous_revenue_by_currency.get(primary_currency, ZERO) if previous_revenue_by_currency is not None else None

    segment_daily_counts: dict[str, dict[date, int]] = {segment: {} for segment in USER_SEGMENTS}
    period_user_rows = read(lambda: list(period_users_qs.values("pk", "created_at")), [])
    for user_row in period_user_rows or []:
        created_at = user_row["created_at"]
        created_day = (
            timezone.localtime(created_at).date()
            if timezone.is_aware(created_at)
            else created_at.date()
        )
        segment = segment_by_user.get(user_row["pk"], USER_SEGMENT_FREE)
        segment_daily_counts[segment][created_day] = segment_daily_counts[segment].get(created_day, 0) + 1
    user_segment_series = _build_user_segment_series(
        period["start_date"],
        period["end_date"],
        segment_daily_counts,
    ) if segments_available and period_user_rows is not None else []

    cohort_user_ids = period_users_qs.values("pk")
    cohort_bound_count = read(
        lambda: successful_payments_qs.filter(
            user_id__in=cohort_user_ids,
            kind=Payment.Kind.BIND,
            effective_paid_at__gte=period["start_at"],
            effective_paid_at__lt=period["end_at"],
            amount=Decimal("1.00"),
            currency__iexact="UAH",
        )
        .values("user_id")
        .distinct()
        .count(),
        0,
    )
    cohort_paid_count = read(
        lambda: successful_payments_qs.filter(
            user_id__in=cohort_user_ids,
            effective_paid_at__gte=period["start_at"],
            effective_paid_at__lt=period["end_at"],
            amount__gt=ZERO,
            kind__in=[Payment.Kind.RENEWAL, Payment.Kind.RETRY],
        )
        .values("user_id")
        .distinct()
        .count(),
        0,
    )
    if cash["gross"] is None:
        cohort_bound_count = cohort_paid_count = None
    funnel_stages = [
        {"label": "Зареєструвалися", "value": period_user_count},
        {"label": "Завершили онбординг до кінця періоду", "value": period_onboarded_count,
         "reason": "Немає повної історії завершення онбордингу; поточний прапорець не визначає дату."},
        {"label": "Мали оплату bind 1 грн до кінця періоду", "value": cohort_bound_count},
        {"label": "Мали оплату renewal/retry до кінця періоду", "value": cohort_paid_count},
    ]
    for stage in funnel_stages:
        stage["percent"] = _percentage(stage["value"], period_user_count)
        stage["percent_display"] = _format_percent(stage["percent"])
        stage["width"] = float(stage["percent"]) if stage["percent"] is not None else None

    paid_subscription_count = sum(segment == USER_SEGMENT_PAID for segment in segment_by_user.values())
    trial_subscription_count = sum(segment == USER_SEGMENT_TRIAL for segment in segment_by_user.values())
    granted_subscription_count = sum(segment == USER_SEGMENT_GRANTED for segment in segment_by_user.values())
    free_user_count = (total_users_count - paid_subscription_count - trial_subscription_count - granted_subscription_count) if segments_available else None
    subscription_distribution = [
        {"label": "Без підтвердженої чинної підписки", "value": free_user_count, "color": "#64748b", "tone": "free"},
        {"label": "Чинний trial (оплата окремо)", "value": trial_subscription_count, "color": "#60a5fa", "tone": "trial"},
        {"label": "Чинна підписка з оплатою renewal/retry", "value": paid_subscription_count, "color": "#34d399", "tone": "paid"},
        {"label": "Наданий / непідтверджений оплатою доступ", "value": granted_subscription_count, "color": "#c4b5fd", "tone": "granted"},
    ] if segments_available else []
    donut_parts = []
    donut_cursor = Decimal("0")
    for item in subscription_distribution:
        item["percent"] = _percentage(item["value"], total_users_count)
        item["percent_display"] = _format_percent(item["percent"])
        next_cursor = donut_cursor + item["percent"]
        donut_parts.append(f"{item['color']} {donut_cursor}% {next_cursor}%")
        donut_cursor = next_cursor
    subscription_donut = (
        f"conic-gradient({', '.join(donut_parts)})" if total_users_count and segments_available else "#1e293b"
    )

    subscription_status_labels = {
        Subscription.Status.TRIAL: "Trial",
        Subscription.Status.ACTIVE: "Активна",
        Subscription.Status.PAID: "Оплачена",
        Subscription.Status.MANUAL: "Ручна",
        Subscription.Status.LIFETIME: "Довічна",
        Subscription.Status.EXPIRED: "Завершена",
        Subscription.Status.CANCELLED: "Скасована",
    }
    subscription_status_tones = {
        Subscription.Status.TRIAL: "trial",
        Subscription.Status.ACTIVE: "paid",
        Subscription.Status.PAID: "paid",
        Subscription.Status.MANUAL: "paid",
        Subscription.Status.LIFETIME: "paid",
        Subscription.Status.EXPIRED: "inactive",
        Subscription.Status.CANCELLED: "inactive",
    }
    recent_user_objects = read(
        lambda: list(users_qs.only(
            "tg_user_id", "first_name", "last_name", "username", "created_at", "last_seen_at", "onboarding_completed",
        ).order_by("-created_at")[:8]),
        [],
    )
    recent_users = []
    for user in recent_user_objects or []:
        subscription = latest_subscription_by_user.get(user.pk)
        subscription_status = subscription.status if subscription else "none"
        recent_users.append(
            {
                "name": user.full_name,
                "username": f"@{user.username}" if user.username else "",
                "telegram_id": user.pk,
                "created_at": user.created_at,
                "last_seen_at": user.last_seen_at,
                "onboarding_completed": user.onboarding_completed,
                "subscription_label": subscription_status_labels.get(subscription_status, "Без підписки") if segment_source_available else "Недоступно",
                "subscription_tone": subscription_status_tones.get(subscription_status, "none") if segment_source_available else "none",
                "url": reverse("admin:users_telegramuser_change", args=[user.pk]),
            }
        )

    period_onboarding_errors = read(
        lambda: bot_events_qs.filter(
            created_at__gte=period["start_at"],
            created_at__lt=period["end_at"],
            event_type__in=["onboarding_failed", "parse_error"],
        ).count(),
        0,
    )
    period_delivery_errors = read(
        lambda: recipient_logs_qs.filter(
            created_at__gte=period["start_at"],
            created_at__lt=period["end_at"],
            status__in=[BroadcastRecipient.Status.FAILED, BroadcastRecipient.Status.BLOCKED],
        ).count(),
        0,
    )
    period_bot_errors = read(
        lambda: bot_events_qs.filter(
            created_at__gte=period["start_at"],
            created_at__lt=period["end_at"],
            success=False,
        ).count(),
        0,
    )
    period_failed_payments = read(
        lambda: payments_qs.annotate(effective_at=Coalesce("paid_at", "created_at")).filter(
            effective_at__gte=period["start_at"],
            effective_at__lt=period["end_at"],
            status__in=[Payment.Status.FAILED, Payment.Status.REJECTED],
        ).count(),
        0,
    )

    redis_check = read(_check_redis, {"status": "Помилка", "detail": "Не вдалося перевірити Redis."})
    worker_check = read(_check_worker, {"status": "Помилка", "detail": "Не вдалося перевірити worker."})
    telegram_check = read(_check_telegram_api, {"status": "Помилка", "detail": "Не вдалося перевірити Telegram API."})

    context = {
        "title": "Панель керування",
        "include_test_users": include_test_users,
        "period": period,
        "custom_period_url": reverse("admin:index"),
        "toggle_include_test_url": _dashboard_url(request, include_test_users=1),
        "toggle_exclude_test_url": _dashboard_url(request, include_test_users=None),
        "data_scope_label": "Усі користувачі, включно з тестовими" if include_test_users else "Тільки реальні користувачі",
        "test_tools": _test_tool_links(request),
        "kpi_cards": [
            {
                "label": "Нові користувачі",
                "value": period_user_count,
                "detail": f"Було {previous_period_user_count} у попередньому періоді",
                "url": admin_url("admin:users_telegramuser_changelist"),
                **_change(period_user_count, previous_period_user_count),
            },
            {
                "label": "Історична активність",
                "value": period_active_user_count,
                "detail": "Недоступно: немає повної історії активності. last_seen_at не відновлює DAU/WAU/MAU чи retention.",
                "url": admin_url("admin:users_telegramuser_changelist"),
                **_change(period_active_user_count, previous_period_active_user_count),
            },
            {
                "label": "Останній візит у періоді",
                "value": period_last_seen_count,
                "detail": "Поточний last_seen_at потрапляє у вибраний період; не історична активність і не growth.",
                "url": admin_url("admin:users_telegramuser_changelist"),
                **_change(None, None),
            },
            {
                "label": "Історична конверсія онбордингу",
                "value": _format_percent(period_onboarding_rate),
                "detail": "Недоступно: немає повної історії завершення онбордингу станом на кінець періоду.",
                "url": admin_url("admin:users_telegramuser_changelist"),
                **_point_change(period_onboarding_rate, previous_period_onboarding_rate),
            },
            {
                "label": "Онбординг зараз у когорті",
                "value": cohort_onboarded_now,
                "detail": f"Поточний прапорець у когорти реєстрації ({period_user_count} користувачів); не історична конверсія.",
                "url": admin_url("admin:users_telegramuser_changelist"),
                **_change(None, None),
            },
            {
                "label": "Оплати (включно з поверненими)",
                "value": period_payment_count,
                "detail": f"{period_payer_count if period_payer_count is not None else 'Недоступно'} унікальних платників усіх типів; не кількість передплатників.",
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(period_payment_count, previous_period_payment_count),
            },
            {
                "label": "Платники renewal/retry за період",
                "value": cash["recurring_payers"],
                "detail": _format_currency_totals(_cash_rows(cash["recurring_cash"])) + "; валові оплати renewal/retry, включно з пізніше поверненими; не MRR.",
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(cash["recurring_payers"], previous_cash["recurring_payers"]),
            },
            {
                "label": "Платники bind за період",
                "value": cash["bind_payers"],
                "detail": _format_currency_totals(_cash_rows(cash["bind_cash"])) + "; прив’язка / повторна прив’язка, не регулярні передплатники чи нові trial.",
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(cash["bind_payers"], previous_cash["bind_payers"]),
            },
            {
                "label": "Відновлені користувачі",
                "value": period_recovered_count,
                "detail": "Активували trial після recovery-повідомлення",
                "url": admin_url(
                    "admin:subscriptions_trialrecoveryrecipient_changelist",
                    "status__exact=converted",
                ),
                **_change(period_recovered_count, previous_period_recovered_count),
            },
            {
                "label": f"Валові надходження, {primary_currency}",
                "value": f"{_format_decimal(primary_revenue)} {primary_currency}",
                "detail": _format_currency_totals(period_revenue_totals) + "; за paid_at, до повернень; не MRR. " + cash["reason"],
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(primary_revenue, previous_primary_revenue),
            },
            {
                "label": "Повернення коштів",
                "value": _format_currency_totals(_cash_rows(cash["refunds"])),
                "detail": f"Кількість: {cash['refund_count'] if cash['refund_count'] is not None else 'Недоступно'}. Повні повернення за підтвердженим processed_at. " + cash["reason"],
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(None, None),
            },
            {
                "label": "Чистий рух коштів",
                "value": _format_currency_totals(_cash_rows(cash["net"])),
                "detail": "Надходження мінус повернення за датами подій; валюти не змішуються. " + cash["reason"],
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(None, None),
            },
            {
                "label": "Ручні підтвердження оплат",
                "value": _format_currency_totals(_cash_rows(cash["manual_confirmed"])),
                "detail": "За paid_at; окремо від підтверджених надходжень і grants. Не перевірка банком.",
                "url": admin_url("admin:subscriptions_payment_changelist"),
                **_change(None, None),
            },
        ],
        "attention_items": [
            {
                "label": "Помилки онбордингу",
                "value": period_onboarding_errors,
                "tone": "danger" if period_onboarding_errors else "ok",
                "url": admin_url("admin:bot_events_botevent_changelist", "event_type=onboarding_failed"),
            },
            {
                "label": "Збої оплат",
                "value": period_failed_payments,
                "tone": "danger" if period_failed_payments else "ok",
                "url": admin_url("admin:subscriptions_payment_changelist"),
            },
            {
                "label": "Проблеми доставки",
                "value": period_delivery_errors,
                "tone": "danger" if period_delivery_errors else "ok",
                "url": admin_url("admin:broadcasts_adminmessagelog_changelist"),
            },
            {
                "label": "Помилки бота",
                "value": period_bot_errors,
                "tone": "danger" if period_bot_errors else "ok",
                "url": admin_url("admin:bot_events_botevent_changelist", "success__exact=0"),
            },
        ],
        "user_segment_series": user_segment_series,
        "funnel_stages": funnel_stages,
        "pwa_funnel": pwa_funnel,
        "subscription_distribution": subscription_distribution,
        "subscription_donut": subscription_donut,
        "subscription_total": total_users_count,
        "recent_users": recent_users,
        "health_items": [
            {"label": "Worker", **(worker_check or {"status": "Недоступно"})},
            {"label": "Redis", **(redis_check or {"status": "Недоступно"})},
            {"label": "Telegram API", **(telegram_check or {"status": "Недоступно"})},
        ],
        "latest_payments": latest_payments,
        "cash_summary": cash,
        "segments_available": segments_available,
        "series_unavailable": not segments_available or period_user_rows is None,
        "recent_users_unavailable": recent_user_objects is None,
        "latest_payments_unavailable": latest_payments is None,
        "data_quality": {
            "degraded": bool(query_errors) or bool(cash["reason"]) or not segments_available,
            "query_errors": query_errors,
            "retrieved_at": now,
            "history_available": False,
            "reason": cash["reason"],
        },
    }

    for card in context["kpi_cards"]:
        card["available"] = card["value"] is not None and not str(card["value"]).startswith("Недоступно")
        card["detail"] = card["detail"].replace("None", "Недоступно")
        if not card["available"]:
            card["value"] = None
            card.update(_change(None, None))
            card["reason"] = "Недоступно: дані не отримано або джерело не містить потрібної історії."
    for item in context["attention_items"]:
        item["available"] = item["value"] is not None
        if not item["available"]:
            item["tone"] = "unknown"
            item["reason"] = "Не вдалося отримати дані; це не нуль проблем."

    daily_indexes = {0, 5, 8, 11}
    finance_indexes = {6, 7, 9, 10, 12}
    for index, card in enumerate(context["kpi_cards"]):
        if index in daily_indexes:
            card["group"] = "daily"
        elif index in finance_indexes:
            card["group"] = "finance"
        else:
            card["group"] = "product"

    context["daily_kpis"] = [card for card in context["kpi_cards"] if card["group"] == "daily"]
    context["finance_kpis"] = [card for card in context["kpi_cards"] if card["group"] == "finance"]
    context["product_kpis"] = [card for card in context["kpi_cards"] if card["group"] == "product"]
    context["user_search_url"] = reverse("admin:users_telegramuser_changelist")
    context["quick_actions"] = [
        {"label": "Написати", "url": reverse("admin:manual_message"), "tone": "primary"},
        {"label": "Нова розсилка", "url": reverse("admin:broadcasts_broadcast_add"), "tone": "secondary"},
        {"label": "Звернення", "url": reverse("admin:support_supportcase_changelist"), "tone": "secondary"},
        {"label": "Стан системи", "url": reverse("admin:system_health"), "tone": "secondary"},
    ]
    return context
