from __future__ import annotations

from django.contrib import admin

from audit_log.models import AdminAuditLog
from common.admin import OperationalModelAdmin
from common.admin_actions import can_admin_action
from common.admin_site import admin_site

AdminAuditLog._meta.verbose_name = "Журнал дій адміна"
AdminAuditLog._meta.verbose_name_plural = "Журнал дій адміна"


@admin.register(AdminAuditLog, site=admin_site)
class AdminAuditLogAdmin(OperationalModelAdmin):
    list_display = ("created_at", "admin_user", "action", "object_summary", "target_user_id", "result_preview", "ip_address", "user_agent_preview")
    search_fields = ("action", "object_type", "object_id", "target_user_id", "mode", "reason", "ip_address", "user_agent")
    list_filter = ("action", "object_type", "mode", "created_at")
    readonly_fields = ("created_at", "admin_user", "action", "object_type", "object_id", "target_user_id", "mode", "reason", "before", "after", "ip_address", "user_agent")

    def has_module_permission(self, request):
        return can_admin_action(request.user, "audit_view")

    def has_view_permission(self, request, obj=None):
        return can_admin_action(request.user, "audit_view")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    @admin.display(description="Об'єкт")
    def object_summary(self, obj: AdminAuditLog):
        return f"{obj.object_type} #{obj.object_id}" if obj.object_id else obj.object_type

    @admin.display(description="Результат")
    def result_preview(self, obj: AdminAuditLog):
        if obj.after:
            return str(obj.after)[:120]
        if obj.before:
            return str(obj.before)[:120]
        return "-"

    @admin.display(description="User agent")
    def user_agent_preview(self, obj: AdminAuditLog):
        return (obj.user_agent or "-")[:60]
