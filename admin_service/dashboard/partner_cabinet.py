from __future__ import annotations
from collections import defaultdict
from datetime import datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from django.db.models.fields.json import KeyTextTransform
from django.db.models import Exists, F, OuterRef, Q
from miniapp.models import DurableRegistrationAttribution
from subscriptions.models import Payment
from users.traffic import funnel_excluded_telegram_ids, production_canary_exclusion_q

KYIV = ZoneInfo("Europe/Kyiv")
ZERO = Decimal("0.00")
SUCCESS_STATUSES = (Payment.Status.PAID, Payment.Status.REFUNDED)

def empty_metrics():
    return {"successful_count": 0, "successful_amount": ZERO, "first_count": 0, "first_amount": ZERO,
            "repeat_count": 0, "repeat_amount": ZERO, "refund_amount": ZERO, "unknown_refunds": 0,
            "groups": {"day": [], "week": [], "month": []}}

def _bounds(start_date, end_date):
    return datetime.combine(start_date, time.min, KYIV), datetime.combine(end_date + timedelta(days=1), time.min, KYIV)

def _group_key(kind, day):
    return day if kind == "day" else day - timedelta(days=day.weekday()) if kind == "week" else day.replace(day=1)

def _group_row():
    return {"count": 0, "amount": ZERO, "first_count": 0, "repeat_count": 0, "first_amount": ZERO, "repeat_amount": ZERO, "refund_amount": ZERO}

def _refund_datetime(value):
    try:
        result = datetime.fromisoformat(value) if value else None
    except (TypeError, ValueError):
        return None
    return result if result and result.tzinfo else None

def partner_cabinet_metrics(start_date, end_date, partner_ids=None):
    if end_date < start_date:
        raise ValueError("Дата завершення не може бути раніше дати початку")
    start_at, end_at = _bounds(start_date, end_date)
    links_qs = DurableRegistrationAttribution.objects.filter(partner_link__isnull=False)
    if partner_ids is not None:
        links_qs = links_qs.filter(partner_link_id__in=partner_ids)
    links = dict(links_qs.values_list("user_id", "partner_link_id"))
    excluded = funnel_excluded_telegram_ids()
    prior = Payment.objects.filter(user_id=OuterRef("user_id"), status__in=SUCCESS_STATUSES,
        paid_at__isnull=False, amount__gt=0).filter(
        Q(paid_at__lt=OuterRef("paid_at")) | Q(paid_at=OuterRef("paid_at"), pk__lt=OuterRef("pk")))
    prior = prior.exclude(canary_owned=True).exclude(user_id__in=excluded).exclude(production_canary_exclusion_q(prefix="user__"))
    payments = Payment.objects.filter(user_id__in=links, status__in=SUCCESS_STATUSES,
        paid_at__isnull=False, amount__gt=0,
        user__durable_registration_attribution__registered_at__lte=F("paid_at")).filter(
        Q(paid_at__gte=start_at, paid_at__lt=end_at) | Q(status=Payment.Status.REFUNDED)).annotate(
        refund_status=KeyTextTransform("status", KeyTextTransform("refund_request", F("raw_payload"))),
        refund_processed_at=KeyTextTransform("processed_at", KeyTextTransform("refund_request", F("raw_payload"))),
        has_prior=Exists(prior)).exclude(canary_owned=True).exclude(user_id__in=excluded).exclude(production_canary_exclusion_q(prefix="user__"))
    rows, warnings = defaultdict(empty_metrics), set()
    for payment in payments.order_by("paid_at", "pk"):
        refunded_at = _refund_datetime(payment.refund_processed_at) if payment.refund_status == "success" else None
        refund_in_period = refunded_at is not None and start_at <= refunded_at < end_at
        capture_in_period = start_at <= payment.paid_at < end_at
        unknown_refund = payment.status == Payment.Status.REFUNDED and refunded_at is None
        if not capture_in_period and not refund_in_period and not unknown_refund:
            continue
        if payment.currency != "UAH":
            warnings.add("Не-UAH платежі виключено з гривневих підсумків."); continue
        amount = (payment.amount or ZERO).quantize(Decimal("0.01")); row = rows[links[payment.user_id]]
        if isinstance(row["groups"]["day"], list):
            row["groups"] = {"day": {}, "week": {}, "month": {}}
        first, key = not payment.has_prior, ("first" if not payment.has_prior else "repeat")
        if capture_in_period:
            row["successful_count"] += 1; row["successful_amount"] += amount; row[f"{key}_count"] += 1; row[f"{key}_amount"] += amount
            day = payment.paid_at.astimezone(KYIV).date()
            for kind in ("day", "week", "month"):
                bucket = row["groups"].setdefault(kind, {}).setdefault(_group_key(kind, day), _group_row())
                bucket["count"] += 1; bucket["amount"] += amount; bucket[f"{key}_count"] += 1; bucket[f"{key}_amount"] += amount
        if payment.status == Payment.Status.REFUNDED:
            if refunded_at is None:
                row["unknown_refunds"] += 1; warnings.add("Дати деяких підтверджених повернень недоступні.")
            elif refund_in_period:
                row["refund_amount"] += amount; day = refunded_at.astimezone(KYIV).date()
                for kind in ("day", "week", "month"):
                    row["groups"].setdefault(kind, {}).setdefault(_group_key(kind, day), _group_row())["refund_amount"] += amount
    for row in rows.values():
        row["groups"] = {kind: [{"date": day.isoformat(), "label": day.isoformat(),
            "successful_count": bucket["count"], "successful_amount": bucket["amount"], **bucket}
            for day, bucket in sorted(values.items())] for kind, values in row["groups"].items()}
    return {**dict(rows), "warnings": sorted(warnings)}