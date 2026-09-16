from __future__ import annotations

from django.conf import settings
from django.db import models


class AdminAuditLog(models.Model):
    admin_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="cashflow_audit_logs",
    )
    action = models.CharField(max_length=128)
    object_type = models.CharField(max_length=128)
    object_id = models.CharField(max_length=255, blank=True, default="")
    target_user_id = models.BigIntegerField(blank=True, null=True)
    mode = models.CharField(max_length=64, blank=True, default="")
    reason = models.TextField(blank=True, default="")
    before = models.JSONField(default=dict, blank=True)
    after = models.JSONField(default=dict, blank=True)
    ip_address = models.CharField(max_length=128, blank=True, default="")
    user_agent = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "admin_audit_logs"
        verbose_name = "Admin Audit Log"
        verbose_name_plural = "Admin Audit Logs"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.action} {self.object_type}"
