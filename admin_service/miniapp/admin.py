from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from django.contrib import admin
from django.db.models import Count, IntegerField, OuterRef, Subquery, Value
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.utils.html import format_html
from django.utils import timezone
from unfold.views import ChangeList

from common.admin import AuditedModelAdmin
from common.admin_site import admin_site
from dashboard.partner_cabinet import partner_cabinet_metrics
from miniapp.models import (
    AcquisitionSession,
    FunnelEvent,
    PartnerLink,
    PartnerLinkClick,
)



def _count_subquery(queryset, *, group_field: str, count_field: str, distinct: bool = False):
    counted = (
        queryset.order_by()
        .values(group_field)
        .annotate(total=Count(count_field, distinct=distinct))
        .values("total")[:1]
    )
    return Coalesce(
        Subquery(counted, output_field=IntegerField()),
        Value(0),
        output_field=IntegerField(),
    )


KYIV = ZoneInfo("Europe/Kyiv")


def _period(request):
    today = timezone.now().astimezone(KYIV).date()
    if ("period_start" in request.GET) != ("period_end" in request.GET):
        return None, None, "Вкажіть обидві дати початку та кінця."
    raw_start = request.GET.get("period_start", today.replace(day=1).isoformat())
    raw_end = request.GET.get("period_end", today.isoformat())
    try:
        start, end = date.fromisoformat(raw_start), date.fromisoformat(raw_end)
        if end < start:
            return None, None, "Дата початку має бути не пізнішою за кінець."
        if end == date.max:
            raise ValueError
    except (ValueError, TypeError):
        return None, None, "Вкажіть коректні дати: початок не пізніше завершення."
    return start, end, ""


def _period_bounds(request):
    start, end, error = _period(request)
    if error:
        return None
    return (
        timezone.make_aware(datetime.combine(start, time.min), KYIV),
        timezone.make_aware(datetime.combine(end + timedelta(days=1), time.min), KYIV),
    )


class PartnerPeriodChangeList(ChangeList):
    def get_filters_params(self, params=None):
        lookups = super().get_filters_params(params)
        for key in list(lookups):
            if key in {"period_start", "period_end"} or key.startswith("created_at"):
                lookups.pop(key)
        return lookups


def _human_click_metric(*, unique_sessions: bool, bounds=None):
    queryset = PartnerLinkClick.objects.filter(partner_link_id=OuterRef("pk")).exclude(
        acquisition_session__automation_status=AcquisitionSession.AutomationStatus.AUTOMATED
    )
    if bounds is None:
        queryset = queryset.none()
    else:
        queryset = queryset.filter(clicked_at__gte=bounds[0], clicked_at__lt=bounds[1])
    return _count_subquery(
        queryset,
        group_field="partner_link_id",
        count_field="acquisition_session_id" if unique_sessions else "pk",
        distinct=unique_sessions,
    )


def _human_event_metric(*, touch: str, event_name: str, bounds=None):
    link_field = "first_partner_link_id" if touch == "first" else "last_partner_link_id"
    queryset = FunnelEvent.objects.filter(
        **{
            link_field: OuterRef("pk"),
            "event_name": event_name,
        }
    ).exclude(
        acquisition_session__automation_status=AcquisitionSession.AutomationStatus.AUTOMATED
    )
    if bounds is None:
        queryset = queryset.none()
    else:
        queryset = queryset.filter(recorded_at__gte=bounds[0], recorded_at__lt=bounds[1])
    return _count_subquery(
        queryset,
        group_field=link_field,
        count_field="acquisition_session_id",
        distinct=True,
    )


@admin.register(PartnerLink, site=admin_site)
class PartnerLinkAdmin(AuditedModelAdmin):
    audit_object_type = "partner_link"
    list_display = (
        "name",
        "human_clicks",
        "first_touch_registrations",
        "successful_payments",
        "successful_amount",
        "first_payments",
        "first_amount",
        "repeat_payments",
        "repeat_amount",
        "refund_amount",
    )
    change_list_template = "admin/miniapp/partnerlink/change_list.html"
    list_filter = ("is_active", "destination", "utm_source", "utm_medium")
    list_filter_sheet = True
    actions = None
    search_fields = ("name", "code", "utm_source", "utm_medium", "utm_campaign", "notes")
    date_hierarchy = None
    readonly_fields = (
        "public_link",
        "human_clicks",
        "unique_visitors",
        "auth_conversions",
        "first_touch_registrations",
        "last_touch_registrations",
        "registration_rate",
        "onboarding_conversions",
        "first_transaction_conversions",
        "payment_conversions",
        "created_at",
        "updated_at",
    )
    fields = (
        "name",
        "code",
        "public_link",
        "destination",
        "trial_days",
        "is_active",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_content",
        "utm_term",
        "notes",
        "human_clicks",
        "unique_visitors",
        "auth_conversions",
        "first_touch_registrations",
        "last_touch_registrations",
        "registration_rate",
        "onboarding_conversions",
        "first_transaction_conversions",
        "payment_conversions",
        "created_at",
        "updated_at",
    )

    def get_queryset(self, request):
        bounds = _period_bounds(request)
        return super().get_queryset(request).annotate(
            metric_human_clicks=_human_click_metric(unique_sessions=False, bounds=bounds),
            metric_unique_visitors=_human_click_metric(unique_sessions=True, bounds=bounds),
            metric_auth=_human_event_metric(touch="first", event_name="auth_success", bounds=bounds),
            metric_first_registrations=_human_event_metric(
                touch="first",
                event_name="registration_completed",
                bounds=bounds,
            ),
            metric_last_registrations=_human_event_metric(
                touch="last",
                event_name="registration_completed",
                bounds=bounds,
            ),
            metric_onboarding=_human_event_metric(
                touch="first",
                event_name="onboarding_confirmed",
                bounds=bounds,
            ),
            metric_first_transaction=_human_event_metric(
                touch="first",
                event_name="first_transaction_confirmed",
                bounds=bounds,
            ),
            metric_payment=_human_event_metric(touch="first", event_name="payment_success", bounds=bounds),
        )

    def get_changelist(self, request, **kwargs):
        return PartnerPeriodChangeList

    def changelist_view(self, request, extra_context=None):
        start, end, error = _period(request)
        today = timezone.now().astimezone(KYIV).date()
        presets = []
        for label, first, last in (
            ("Сьогодні", today, today),
            ("Вчора", today - timedelta(days=1), today - timedelta(days=1)),
            ("Останній тиждень", today - timedelta(days=6), today),
            ("Цей місяць", today.replace(day=1), today),
        ):
            params = request.GET.copy()
            params.pop("p", None)
            params["period_start"], params["period_end"] = first.isoformat(), last.isoformat()
            presets.append({"label": label, "url": "?" + params.urlencode(), "active": (start, end) == (first, last)})
        hidden = [(key, value) for key, values in request.GET.lists() for value in values
                  if key not in {"period_start", "period_end", "p"} and not key.startswith("created_at")]
        context = {
            **(extra_context or {}), "period_start": start, "period_end": end,
            "period_start_value": request.GET.get("period_start", today.replace(day=1).isoformat()),
            "period_end_value": request.GET.get("period_end", today.isoformat()),
            "period_error": error, "period_presets": presets, "period_hidden": hidden,
            "partner_period": {"start": request.GET.get("period_start", today.replace(day=1).isoformat()),
                               "end": request.GET.get("period_end", today.isoformat()),
                               "start_date": start, "end_date": end, "error": error},
            "partner_presets": presets, "partner_period_hidden": hidden,
        }
        response = super().changelist_view(request, extra_context=context)
        if not hasattr(response, "context_data") or "cl" not in response.context_data:
            return response
        links = list(response.context_data["cl"].result_list)
        metrics = partner_cabinet_metrics(start, end, partner_ids=[link.pk for link in links]) if not error else {}
        for link in links:
            link.cabinet_edit_url = reverse("admin:miniapp_partnerlink_change", args=[link.pk])
            link.cabinet_public_url = link.public_url
            link.cabinet_registration_rate = self.registration_rate(link)
            link.cabinet = metrics.get(link.pk, {
                "successful_count": 0, "successful_amount": Decimal("0.00"),
                "first_count": 0, "first_amount": Decimal("0.00"),
                "repeat_count": 0, "repeat_amount": Decimal("0.00"),
                "refund_amount": Decimal("0.00"), "groups": {},
            })
        response.context_data.update(partner_rows=links, partner_warnings=metrics.get("warnings", []))
        return response

    @admin.display(description="Успішних оплат за період")
    def successful_payments(self, obj):
        return getattr(obj, "cabinet", {}).get("successful_count", 0)

    @admin.display(description="Сума оплат за період")
    def successful_amount(self, obj):
        return getattr(obj, "cabinet", {}).get("successful_amount", Decimal("0.00"))

    @admin.display(description="Кількість перших оплат")
    def first_payments(self, obj):
        return getattr(obj, "cabinet", {}).get("first_count", 0)

    @admin.display(description="Сума перших оплат")
    def first_amount(self, obj):
        return getattr(obj, "cabinet", {}).get("first_amount", Decimal("0.00"))

    @admin.display(description="Кількість повторних оплат")
    def repeat_payments(self, obj):
        return getattr(obj, "cabinet", {}).get("repeat_count", 0)

    @admin.display(description="Сума повторних оплат")
    def repeat_amount(self, obj):
        return getattr(obj, "cabinet", {}).get("repeat_amount", Decimal("0.00"))

    @admin.display(description="Сума повернень")
    def refund_amount(self, obj):
        return getattr(obj, "cabinet", {}).get("refund_amount", Decimal("0.00"))

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj is not None:
            fields.append("code")
        return tuple(dict.fromkeys(fields))

    def has_delete_permission(self, request, obj=None):
        return False

    @staticmethod
    def _metric(obj: PartnerLink | None, name: str) -> int:
        if obj is None:
            return 0
        return int(getattr(obj, name, 0) or 0)

    @admin.display(description="Посилання")
    def public_link(self, obj: PartnerLink | None):
        if obj is None or not obj.code:
            return "—"
        return format_html(
            '<a href="{}" target="_blank" rel="noopener noreferrer">{}</a>',
            obj.public_url,
            obj.public_url,
        )

    @admin.display(description="Переходи", ordering="metric_human_clicks")
    def human_clicks(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_human_clicks")

    @admin.display(description="Унікальні", ordering="metric_unique_visitors")
    def unique_visitors(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_unique_visitors")

    @admin.display(description="Авторизації", ordering="metric_auth")
    def auth_conversions(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_auth")

    @admin.display(description="Реєстрації (first-touch)", ordering="metric_first_registrations")
    def first_touch_registrations(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_first_registrations")

    @admin.display(description="Реєстрації (last-touch)", ordering="metric_last_registrations")
    def last_touch_registrations(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_last_registrations")

    @admin.display(description="Конверсія в реєстрацію")
    def registration_rate(self, obj: PartnerLink) -> str:
        visitors = self._metric(obj, "metric_unique_visitors")
        registrations = self._metric(obj, "metric_first_registrations")
        if visitors <= 0:
            return "0.0%"
        return f"{registrations * 100 / visitors:.1f}%"

    @admin.display(description="Онбординг", ordering="metric_onboarding")
    def onboarding_conversions(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_onboarding")

    @admin.display(description="Перша транзакція", ordering="metric_first_transaction")
    def first_transaction_conversions(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_first_transaction")

    @admin.display(description="Оплата", ordering="metric_payment")
    def payment_conversions(self, obj: PartnerLink) -> int:
        return self._metric(obj, "metric_payment")


from .models import RecognitionCapture, RecognitionCaptureConfig
from django.contrib import admin

@admin.register(RecognitionCaptureConfig)
class RecognitionCaptureConfigAdmin(admin.ModelAdmin):
    list_display = ('enabled','expires_at','max_total_bytes','retention_days','storage_warning_at','storage_full_at','updated_at')
    readonly_fields = ('storage_warning_at','storage_full_at','updated_at')

@admin.register(RecognitionCapture)
class RecognitionCaptureAdmin(admin.ModelAdmin):
    list_display = ('created_at','user_id','source','provider','outcome','reviewed_at')
    readonly_fields = ('attempt_id','user_id','source','raw_text','raw_media','raw_media_size','mime_type','provider','provider_result','normalized_result','created_at')
    def has_add_permission(self, request): return False
