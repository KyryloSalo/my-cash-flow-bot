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
from common.admin_ui import badge, status_badge, user_identity
from common.audit import create_audit_log
from support.models import SupportCase, SupportMessage
from support.tasks import send_support_reply_task

SupportCase._meta.verbose_name = "Звернення"
SupportCase._meta.verbose_name_plural = "Звернення"
SupportMessage._meta.verbose_name = "Повідомлення звернення"
SupportMessage._meta.verbose_name_plural = "Повідомлення звернень"


class SupportReplyForm(forms.Form):
    text = forms.CharField(label="Відповідь користувачу", widget=forms.Textarea(attrs={"rows": 8}), max_length=4000)


class SupportMessageInline(admin.TabularInline):
    model = SupportMessage
    extra = 0
    fields = ("sender_type", "sender_admin", "text", "telegram_message_id", "created_at")
    readonly_fields = ("sender_type", "sender_admin", "text", "telegram_message_id", "created_at")
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(SupportCase, site=admin_site)
class SupportCaseAdmin(AuditedModelAdmin):
    audit_object_type = "support_case"
    list_display = ("user_display", "subject_display", "status_display", "priority_display", "category_display", "assigned_admin", "updated_at", "reply_link")
    list_display_links = ("subject_display",)
    search_fields = ("subject", "user__username", "user__tg_user_id", "internal_notes")
    list_filter = ("status", "category", "priority", "assigned_admin", "created_at", "updated_at")
    readonly_fields = ("created_at", "updated_at", "closed_at", "reply_actions")
    inlines = (SupportMessageInline,)
    fieldsets = (
        ("Звернення", {"fields": ("user", "subject", "status", "category", "priority", "assigned_admin")}),
        ("Робота оператора", {"fields": ("internal_notes", "reply_actions")}),
        ("Час", {"fields": ("created_at", "updated_at", "closed_at")}),
    )

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user", "assigned_admin")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: SupportCase):
        return user_identity(obj.user)

    @admin.display(description="Тема", ordering="subject")
    def subject_display(self, obj: SupportCase):
        return obj.subject or f"Звернення #{obj.pk}"

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: SupportCase):
        labels = {"new": "Нове", "in_progress": "В роботі", "waiting_user": "Чекаємо користувача", "resolved": "Вирішено", "closed": "Закрито"}
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))

    @admin.display(description="Пріоритет", ordering="priority")
    def priority_display(self, obj: SupportCase):
        labels = {"low": "Низький", "normal": "Звичайний", "high": "Високий", "urgent": "Терміновий"}
        tones = {"low": "info", "normal": "info", "high": "warning", "urgent": "danger"}
        return badge(labels.get(obj.priority, obj.priority or "Невідомо"), tone=tones.get(obj.priority, "info"))

    @admin.display(description="Категорія", ordering="category")
    def category_display(self, obj: SupportCase):
        labels = {"payment": "Оплата", "subscription": "Підписка", "onboarding": "Онбординг", "bug": "Помилка", "question": "Питання", "other": "Інше"}
        return labels.get(obj.category, obj.category or "—")

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path("<path:object_id>/reply/", self.admin_site.admin_view(self.reply_view), name="support_supportcase_reply"),
        ]
        return custom_urls + urls

    @admin.display(description="Дія")
    def reply_link(self, obj: SupportCase):
        return format_html('<a class="op-row-action" href="{}">Відповісти</a>', reverse("admin:support_supportcase_reply", args=[obj.pk]))

    @admin.display(description="Дії")
    def reply_actions(self, obj: SupportCase):
        return self.reply_link(obj)

    def save_model(self, request, obj, form, change):
        if obj.status in {SupportCase.Status.RESOLVED, SupportCase.Status.CLOSED} and not obj.closed_at:
            obj.closed_at = timezone.now()
        elif obj.status not in {SupportCase.Status.RESOLVED, SupportCase.Status.CLOSED}:
            obj.closed_at = None
        super().save_model(request, obj, form, change)

    @admin_action_permission("support_reply")
    def reply_view(self, request, object_id):
        case = get_object_or_404(SupportCase, pk=object_id)
        form = SupportReplyForm(request.POST or None)
        if request.method == "POST" and form.is_valid():
            message = SupportMessage.objects.create(
                case=case,
                sender_type=SupportMessage.SenderType.ADMIN,
                sender_admin=request.user,
                text=form.cleaned_data["text"],
            )
            case.status = SupportCase.Status.WAITING_USER
            case.save(update_fields=["status", "updated_at"])
            send_support_reply_task.delay(message.id)
            create_audit_log(
                request=request,
                admin_user=request.user,
                action="support reply sent",
                object_type="support_case",
                object_id=case.pk,
                before={},
                after={"message_id": message.pk, "status": case.status},
            )
            messages.success(request, "Відповідь поставлено в чергу на доставку в Telegram.")
            return redirect("admin:support_supportcase_change", object_id=case.pk)

        context = {
            **self.admin_site.each_context(request),
            "title": f"Відповідь на звернення #{case.pk}",
            "opts": self.model._meta,
            "form": form,
            "object": case,
            "object_label": "Звернення",
            "submit_label": "Надіслати відповідь",
            "back_url": reverse("admin:support_supportcase_change", args=[case.pk]),
            "back_label": "Назад до звернення",
        }
        return TemplateResponse(request, "admin/object_action_form.html", context)


@admin.register(SupportMessage, site=admin_site)
class SupportMessageAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "support_message"
    list_display = ("created_at", "case", "sender_type", "sender_admin", "telegram_message_id")
    search_fields = ("text", "case__user__username", "case__user__tg_user_id")
    list_filter = ("sender_type", "created_at")
    readonly_fields = ("created_at",)

    def has_add_permission(self, request):
        return False
