from __future__ import annotations

from django.contrib import admin

from admin_notifications.models import AdminNotificationLog
from common.admin import AuditedModelAdmin, HiddenFromMenuAdminMixin
from common.admin_site import admin_site


@admin.register(AdminNotificationLog, site=admin_site)
class AdminNotificationLogAdmin(HiddenFromMenuAdminMixin, AuditedModelAdmin):
    audit_object_type = "admin_notification"
    list_display = ("event_type", "telegram_chat_id", "status", "created_at", "sent_at")
    search_fields = ("event_type", "telegram_chat_id")
    list_filter = ("event_type", "status", "created_at")
    readonly_fields = ("created_at", "sent_at")

    def has_add_permission(self, request):
        return False
