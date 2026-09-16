from __future__ import annotations

from django.db import models


class AdminNotificationLog(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Queued"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"

    event_type = models.CharField(max_length=64)
    telegram_chat_id = models.BigIntegerField()
    payload = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.QUEUED)
    error_message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "admin_notification_logs"
        verbose_name = "Admin Notification Log"
        verbose_name_plural = "Admin Notification Logs"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.event_type}:{self.telegram_chat_id}"
