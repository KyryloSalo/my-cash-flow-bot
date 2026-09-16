from __future__ import annotations

from django.conf import settings
from django.db import models

from users.models import TelegramUser


class SupportCase(models.Model):
    class Status(models.TextChoices):
        NEW = "new", "New"
        IN_PROGRESS = "in_progress", "In Progress"
        WAITING_USER = "waiting_user", "Waiting User"
        RESOLVED = "resolved", "Resolved"
        CLOSED = "closed", "Closed"

    class Category(models.TextChoices):
        PAYMENT = "payment", "Payment"
        SUBSCRIPTION = "subscription", "Subscription"
        ONBOARDING = "onboarding", "Onboarding"
        BUG = "bug", "Bug"
        QUESTION = "question", "Question"
        OTHER = "other", "Other"

    class Priority(models.TextChoices):
        LOW = "low", "Low"
        NORMAL = "normal", "Normal"
        HIGH = "high", "High"
        URGENT = "urgent", "Urgent"

    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="support_cases",
    )
    subject = models.CharField(max_length=255, blank=True, default="")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.NEW)
    category = models.CharField(max_length=32, choices=Category.choices, default=Category.OTHER)
    priority = models.CharField(max_length=32, choices=Priority.choices, default=Priority.NORMAL)
    assigned_admin = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="assigned_support_cases",
    )
    internal_notes = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    closed_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "support_cases"
        verbose_name = "Support Case"
        verbose_name_plural = "Support Cases"
        ordering = ("-updated_at", "-created_at")

    def __str__(self) -> str:
        return f"#{self.pk} {self.user_id}"


class SupportMessage(models.Model):
    class SenderType(models.TextChoices):
        USER = "user", "User"
        ADMIN = "admin", "Admin"
        SYSTEM = "system", "System"

    case = models.ForeignKey(SupportCase, on_delete=models.CASCADE, related_name="messages")
    sender_type = models.CharField(max_length=32, choices=SenderType.choices)
    sender_admin = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="support_messages",
    )
    text = models.TextField()
    attachment = models.CharField(max_length=512, blank=True, default="")
    telegram_message_id = models.BigIntegerField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "support_messages"
        verbose_name = "Support Message"
        verbose_name_plural = "Support Messages"
        ordering = ("created_at", "id")

    def __str__(self) -> str:
        return f"{self.case_id}:{self.sender_type}"

