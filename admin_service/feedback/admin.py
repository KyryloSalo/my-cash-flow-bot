from __future__ import annotations

from django.contrib import admin, messages
from django.urls import reverse

from common.admin import AuditedModelAdmin
from common.admin_actions import bulk_action_confirmation
from common.admin_site import admin_site
from common.admin_ui import status_badge, user_identity
from common.audit import create_audit_log
from feedback.models import FeedbackItem
from support.models import SupportCase

FeedbackItem._meta.verbose_name = "Відгук"
FeedbackItem._meta.verbose_name_plural = "Відгуки"


@admin.action(description="Створити звернення з вибраних відгуків")
def create_support_case_from_feedback(modeladmin, request, queryset):
    if request.POST.get("confirm_action") != "1":
        return bulk_action_confirmation(
            modeladmin=modeladmin,
            request=request,
            queryset=queryset,
            title="Створити звернення з вибраних відгуків?",
            warning="Для кожного ще не опрацьованого відгуку буде створено окреме звернення підтримки.",
            submit_label="Створити звернення",
            section="Підтримка · Відгуки",
            cancel_url=reverse("admin:feedback_feedbackitem_changelist"),
        )
    for item in queryset:
        if item.support_case_id or item.user_id is None:
            continue
        case = SupportCase.objects.create(
            user=item.user,
            subject=(item.text or "")[:120],
            category=SupportCase.Category.OTHER,
            priority=SupportCase.Priority.NORMAL,
            internal_notes=f"Створено з відгуку #{item.pk}",
        )
        item.support_case = case
        item.status = FeedbackItem.Status.CONVERTED_TO_CASE
        item.save(update_fields=["support_case", "status", "updated_at"])
        create_audit_log(
            request=request,
            admin_user=request.user,
            action="feedback converted to case",
            object_type="feedback_item",
            object_id=item.pk,
            before={},
            after={"support_case_id": case.pk},
        )
    messages.success(request, "Звернення створено для відгуків, які ще не були опрацьовані.")


@admin.register(FeedbackItem, site=admin_site)
class FeedbackItemAdmin(AuditedModelAdmin):
    audit_object_type = "feedback_item"
    list_display = ("created_at", "user_display", "rating", "category", "source", "status_display")
    search_fields = ("text", "user__username", "user__tg_user_id")
    list_filter = ("source", "rating", "category", "status", "created_at")
    filter_horizontal = ("tags",)
    actions = (create_support_case_from_feedback,)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user", "support_case")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: FeedbackItem):
        return user_identity(obj.user)

    @admin.display(description="Статус", ordering="status")
    def status_display(self, obj: FeedbackItem):
        labels = {"new": "Новий", "reviewed": "Переглянуто", "converted_to_case": "Створено звернення", "closed": "Закрито"}
        return status_badge(obj.status, labels.get(obj.status, obj.status or "Невідомо"))
