from __future__ import annotations

from django.contrib import admin

from bot_events.models import BotEvent
from common.admin import AuditedModelAdmin
from common.admin_site import admin_site
from common.admin_ui import compact_text, status_badge, user_identity

BotEvent._meta.verbose_name = "Подія бота"
BotEvent._meta.verbose_name_plural = "Події бота"


@admin.register(BotEvent, site=admin_site)
class BotEventAdmin(AuditedModelAdmin):
    audit_object_type = "bot_event"
    list_display = ("created_at", "user_display", "event_type", "source", "result_display", "error_display")
    search_fields = ("user__username", "user__tg_user_id", "event_type", "source", "error_message")
    list_filter = ("event_type", "source", "success", "created_at")
    exclude = ("raw_input", "parsed_result")
    readonly_fields = ("created_at", "error_message")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("user").defer("raw_input", "parsed_result")

    @admin.display(description="Користувач", ordering="user__username")
    def user_display(self, obj: BotEvent):
        return user_identity(obj.user)

    @admin.display(description="Результат", ordering="success")
    def result_display(self, obj: BotEvent):
        return status_badge("success" if obj.success else "failed", "Успішно" if obj.success else "Помилка")

    @admin.display(description="Деталі помилки")
    def error_display(self, obj: BotEvent):
        return compact_text(obj.error_message, limit=90)
