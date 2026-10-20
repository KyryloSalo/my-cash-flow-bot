from __future__ import annotations

from collections import defaultdict
from datetime import datetime, time, timedelta
from decimal import Decimal

from django.db.models import F
from django.utils import timezone

from miniapp.models import DurableRegistrationAttribution
from subscriptions.models import Payment
from users.traffic import funnel_excluded_telegram_ids, production_canary_exclusion_q


def partner_payment_report(start_date, end_date, grouping="day", partner_id=None):
    if end_date < start_date:
        raise ValueError("Дата завершення не може бути раніше дати початку")
    if grouping not in {"day", "week", "month"}:
        raise ValueError("Невідома група")
    tz = timezone.get_current_timezone()
    start_at = timezone.make_aware(datetime.combine(start_date, time.min), tz)
    end_at = timezone.make_aware(datetime.combine(end_date, time.max), tz)
    attribution = DurableRegistrationAttribution.objects.select_related("partner_link")
    if partner_id:
        attribution = attribution.filter(partner_link_id=partner_id)
    user_ids = attribution.values_list("user_id", flat=True)
    excluded_ids = funnel_excluded_telegram_ids()
    payments = Payment.objects.filter(
        user_id__in=user_ids,
        user__durable_registration_attribution__registered_at__lte=F("paid_at"),
        status=Payment.Status.PAID,
        paid_at__gte=start_at,
        paid_at__lte=end_at,
        paid_at__isnull=False,
        canary_owned=False,
    ).exclude(user_id__in=excluded_ids).exclude(production_canary_exclusion_q(prefix="user__"))
    payments = payments.filter(amount__gt=0).exclude(currency="").select_related("user")
    links = {row.user_id: row.partner_link for row in attribution}
    totals = defaultdict(lambda: {"count": 0, "amount": Decimal("0.00")})
    by_partner = defaultdict(lambda: {"count": 0, "amounts": defaultdict(lambda: Decimal("0.00")), "payments": []})
    rows = []
    for payment in payments.order_by("paid_at", "id"):
        link = links.get(payment.user_id)
        bucket = payment.paid_at.astimezone(tz).date()
        if grouping == "week": bucket -= timedelta(days=bucket.weekday())
        elif grouping == "month": bucket = bucket.replace(day=1)
        amount = payment.amount.quantize(Decimal("0.01"))
        key = (bucket, payment.currency)
        totals[key]["count"] += 1
        totals[key]["amount"] += amount
        detail = {"id": payment.pk, "tg_user_id": payment.user_id, "paid_at": payment.paid_at, "amount": amount, "currency": payment.currency, "kind": payment.kind, "provider_payment_id": payment.provider_payment_id, "partner": link.name if link else "Органічний", "code": link.code if link else "—"}
        rows.append(detail)
        partner_key = link.pk if link else None
        by_partner[partner_key]["name"] = link.name if link else "Органічний"
        by_partner[partner_key]["code"] = link.code if link else "—"
        by_partner[partner_key]["count"] += 1
        by_partner[partner_key]["amounts"][payment.currency] += amount
        by_partner[partner_key]["payments"].append(detail)
    # Django resolves dictionary keys before attributes. A defaultdict would
    # create an `items` Decimal key instead of exposing amounts.items().
    for summary in by_partner.values():
        summary["amounts"] = dict(summary["amounts"])
    return {"start_date": start_date, "end_date": end_date, "grouping": grouping, "partner_id": str(partner_id or ""), "definition": "Реферальна оплата: лише статус «Оплачено» з paid_at у періоді, додатна сума та непорожня валюта; атрибуція — незмінний серверний знімок першої реєстрації.", "coverage_note": "До звіту потрапляють лише користувачі з довічним серверним знімком реєстрації; історію без такого знімка не приписано партнеру.", "totals": dict(totals), "by_partner": dict(by_partner), "payments": rows}