from __future__ import annotations

from django import forms
from django.contrib import admin, messages
from django.shortcuts import get_object_or_404, redirect
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin
from common.admin_actions import admin_action_permission
from common.admin_site import admin_site
from common.admin_ui import badge
from common.audit import create_audit_log
from polls.models import PollCampaign, PollRecipient, PollResponse
from polls.tasks import send_poll_campaign_task

PollCampaign._meta.verbose_name = "Опитування"
PollCampaign._meta.verbose_name_plural = "Опитування"


class PollConfirmForm(forms.Form):
    confirm = forms.BooleanField(required=True, label="Підтверджую цю дію")


@admin.register(PollCampaign, site=admin_site)
class PollCampaignAdmin(AuditedModelAdmin):
    audit_object_type = "poll_campaign"
    list_display = ("title", "type_display", "status_display", "target_display", "coverage_display", "scheduled_at", "created_at")
    search_fields = ("title", "question")
    list_filter = ("type", "status", "target_type", "created_at", "scheduled_at")
    readonly_fields = ("created_at", "updated_at", "started_at", "completed_at", "action_links")
    fields = (
        "title",
        "question",
        "type",
        "options",
        "target_type",
        "target_segment",
        "target_tag",
        "target_topic",
        "manual_users",
        "status",
        "scheduled_at",
        "started_at",
        "completed_at",
        "total_recipients",
        "response_count",
        "low_rating_threshold",
        "created_by",
        "created_at",
        "updated_at",
        "action_links",
    )

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("<path:object_id>/preview/", self.admin_site.admin_view(self.preview_view), name="polls_pollcampaign_preview"),
            path("<path:object_id>/send-test/", self.admin_site.admin_view(self.send_test_view), name="polls_pollcampaign_send_test"),
            path("<path:object_id>/send/", self.admin_site.admin_view(self.send_view), name="polls_pollcampaign_send"),
            path("<path:object_id>/complete/", self.admin_site.admin_view(self.complete_view), name="polls_pollcampaign_complete"),
        ]
        return custom_urls + urls

    @admin.display(description="Тип", ordering="type")
    def type_display(self, obj: PollCampaign):
        labels = {"rating": "Оцінка", "nps": "NPS", "single_choice": "Один варіант", "text": "Текстова відповідь"}
        return labels.get(obj.type, obj.type or "—")

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: PollCampaign):
        labels = {"draft": ("Чернетка", "warning"), "scheduled": ("Заплановано", "warning"), "active": ("Активне", "positive"), "completed": ("Завершено", "positive"), "cancelled": ("Скасовано", "danger")}
        label, tone = labels.get(obj.status, (obj.status or "Невідомо", "info"))
        return badge(label, tone=tone)

    @admin.display(description="Аудиторія", ordering="target_type")
    def target_display(self, obj: PollCampaign):
        labels = {"all": "Усі користувачі", "segment": "Сегмент", "tag": "Тег", "manual_users": "Обрані вручну", "push_topic": "Тема повідомлень"}
        return labels.get(obj.target_type, obj.target_type or "—")

    @admin.display(description="Відповіді")
    def coverage_display(self, obj: PollCampaign):
        total = obj.total_recipients or 0
        responses = obj.response_count or 0
        percent = round((responses / total) * 100) if total else 0
        return f"{responses} із {total} · {percent}%"

    @admin.display(description="Дії")
    def action_links(self, obj: PollCampaign):
        return format_html(
            '<div class="op-row-actions">'
            '<a class="op-row-action" href="{}">Перерахувати</a>'
            '<a class="op-row-action" href="{}">Тест адміну</a>'
            '<a class="op-row-action" href="{}">Запустити</a>'
            '<a class="op-row-action" href="{}">Завершити</a>'
            "</div>",
            reverse("admin:polls_pollcampaign_preview", args=[obj.pk]),
            reverse("admin:polls_pollcampaign_send_test", args=[obj.pk]),
            reverse("admin:polls_pollcampaign_send", args=[obj.pk]),
            reverse("admin:polls_pollcampaign_complete", args=[obj.pk]),
        )

    def save_model(self, request, obj, form, change):
        if not obj.created_by_id:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    @admin_action_permission("poll_change")
    def preview_view(self, request, object_id):
        campaign = get_object_or_404(PollCampaign, pk=object_id)
        send_poll_campaign_task.delay(campaign.id, dry_run=True)
        messages.success(request, "Перерахунок аудиторії запущено. Кількість отримувачів оновиться незабаром.")
        return redirect("admin:polls_pollcampaign_change", object_id=campaign.pk)

    @admin_action_permission("poll_change")
    def send_test_view(self, request, object_id):
        campaign = get_object_or_404(PollCampaign, pk=object_id)
        send_poll_campaign_task.delay(campaign.id, is_test=True)
        messages.success(request, "Тестове опитування поставлено в чергу для Telegram адміністратора.")
        return redirect("admin:polls_pollcampaign_change", object_id=campaign.pk)

    @admin_action_permission("poll_change")
    def send_view(self, request, object_id):
        campaign = get_object_or_404(PollCampaign, pk=object_id)
        form = PollConfirmForm(request.POST or None)
        if request.method != "POST" or not form.is_valid():
            return TemplateResponse(
                request,
                "admin/object_action_form.html",
                {
                    **self.admin_site.each_context(request),
                    "title": f"Запустити опитування «{campaign.title}»?",
                    "opts": self.model._meta,
                    "form": form,
                    "object": campaign,
                    "object_label": "Опитування",
                    "submit_label": "Запустити опитування",
                    "back_url": reverse("admin:polls_pollcampaign_change", args=[campaign.pk]),
                    "back_label": "Назад до опитування",
                },
            )
        send_poll_campaign_task.delay(campaign.id)
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="poll campaign sent",
            object_type="poll_campaign",
            object_id=campaign.pk,
            before={"status": campaign.status},
            after={"status": PollCampaign.Status.ACTIVE},
        )
        messages.success(request, "Опитування поставлено в чергу на відправлення.")
        return redirect("admin:polls_pollcampaign_change", object_id=campaign.pk)

    @admin_action_permission("poll_change")
    def complete_view(self, request, object_id):
        campaign = get_object_or_404(PollCampaign, pk=object_id)
        form = PollConfirmForm(request.POST or None)
        if request.method != "POST" or not form.is_valid():
            return TemplateResponse(
                request,
                "admin/object_action_form.html",
                {
                    **self.admin_site.each_context(request),
                    "title": f"Завершити опитування «{campaign.title}»?",
                    "opts": self.model._meta,
                    "form": form,
                    "object": campaign,
                    "object_label": "Опитування",
                    "submit_label": "Завершити опитування",
                    "back_url": reverse("admin:polls_pollcampaign_change", args=[campaign.pk]),
                    "back_label": "Назад до опитування",
                },
            )
        before = {"status": campaign.status}
        campaign.status = PollCampaign.Status.COMPLETED
        campaign.completed_at = campaign.completed_at or timezone.now()
        campaign.save(update_fields=["status", "completed_at", "updated_at"])
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="poll campaign completed",
            object_type="poll_campaign",
            object_id=campaign.pk,
            before=before,
            after={"status": campaign.status},
        )
        messages.success(request, "Опитування завершено.")
        return redirect("admin:polls_pollcampaign_change", object_id=campaign.pk)


@admin.register(PollRecipient, site=admin_site)
class PollRecipientAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "poll_recipient"
    list_display = ("campaign", "user", "status", "sent_at", "responded_at")
    search_fields = ("campaign__title", "user__username", "user__tg_user_id", "error_message")
    list_filter = ("status", "sent_at", "responded_at", "created_at")
    readonly_fields = ("created_at",)

    def has_add_permission(self, request):
        return False


@admin.register(PollResponse, site=admin_site)
class PollResponseAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "poll_response"
    list_display = ("created_at", "campaign", "user", "rating_value", "answer", "text_answer")
    search_fields = ("campaign__title", "user__username", "user__tg_user_id", "answer", "text_answer")
    list_filter = ("campaign", "created_at")
    readonly_fields = ("created_at",)

    def has_add_permission(self, request):
        return False
