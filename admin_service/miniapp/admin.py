from __future__ import annotations

from django.contrib import admin
from django.db.models import Count, IntegerField, OuterRef, Subquery, Value
from django.db.models.functions import Coalesce
from django.utils.html import format_html

from common.admin import AuditedModelAdmin
from common.admin_site import admin_site
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


def _human_click_metric(*, unique_sessions: bool):
    queryset = PartnerLinkClick.objects.filter(partner_link_id=OuterRef("pk")).exclude(
        acquisition_session__automation_status=AcquisitionSession.AutomationStatus.AUTOMATED
    )
    return _count_subquery(
        queryset,
        group_field="partner_link_id",
        count_field="acquisition_session_id" if unique_sessions else "pk",
        distinct=unique_sessions,
    )


def _human_event_metric(*, touch: str, event_name: str):
    link_field = "first_partner_link_id" if touch == "first" else "last_partner_link_id"
    queryset = FunnelEvent.objects.filter(
        **{
            link_field: OuterRef("pk"),
            "event_name": event_name,
        }
    ).exclude(
        acquisition_session__automation_status=AcquisitionSession.AutomationStatus.AUTOMATED
    )
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
        "public_link",
        "utm_source",
        "utm_medium",
        "human_clicks",
        "unique_visitors",
        "first_touch_registrations",
        "last_touch_registrations",
        "registration_rate",
        "onboarding_conversions",
        "first_transaction_conversions",
        "payment_conversions",
        "is_active",
        "created_at",
    )
    list_filter = ("is_active", "destination", "utm_source", "utm_medium", "created_at")
    search_fields = ("name", "code", "utm_source", "utm_medium", "utm_campaign", "notes")
    date_hierarchy = "created_at"
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
        return super().get_queryset(request).annotate(
            metric_human_clicks=_human_click_metric(unique_sessions=False),
            metric_unique_visitors=_human_click_metric(unique_sessions=True),
            metric_auth=_human_event_metric(touch="first", event_name="auth_success"),
            metric_first_registrations=_human_event_metric(
                touch="first",
                event_name="registration_completed",
            ),
            metric_last_registrations=_human_event_metric(
                touch="last",
                event_name="registration_completed",
            ),
            metric_onboarding=_human_event_metric(
                touch="first",
                event_name="onboarding_confirmed",
            ),
            metric_first_transaction=_human_event_metric(
                touch="first",
                event_name="first_transaction_confirmed",
            ),
            metric_payment=_human_event_metric(touch="first", event_name="payment_success"),
        )

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
