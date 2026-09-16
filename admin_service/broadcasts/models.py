from __future__ import annotations

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.text import slugify

from users.models import PushTopic, Tag, TelegramUser


class Segment(models.Model):
    class Type(models.TextChoices):
        SYSTEM = "system", "System"
        TAG = "tag", "Tag"
        SOURCE = "source", "Source"

    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=128, unique=True, blank=True)
    type = models.CharField(max_length=32, choices=Type.choices, default=Type.SYSTEM)
    description = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True)
    is_system = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "segments"
        verbose_name = "Segment"
        verbose_name_plural = "Segments"
        ordering = ("name",)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.name, allow_unicode=True)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class Broadcast(models.Model):
    class ParseMode(models.TextChoices):
        NONE = "none", "None"
        HTML = "HTML", "HTML"
        MARKDOWN = "Markdown", "Markdown"

    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        SCHEDULED = "scheduled", "Scheduled"
        SENDING = "sending", "Sending"
        SENT = "sent", "Sent"
        COMPLETED = "completed", "Completed (Legacy)"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"

    class TargetType(models.TextChoices):
        ALL = "all", "All"
        SEGMENT = "segment", "Segment"
        TAG = "tag", "Tag"
        MANUAL_USERS = "manual_users", "Manual Users"
        PUSH_TOPIC = "push_topic", "Push Topic"

    title = models.CharField(max_length=255)
    message_text = models.TextField()
    image_url = models.CharField(max_length=512, blank=True, default="")
    button_text = models.CharField(max_length=128, blank=True, default="")
    button_url = models.URLField(blank=True, default="")
    parse_mode = models.CharField(max_length=16, choices=ParseMode.choices, default=ParseMode.NONE)
    segment_filter = models.JSONField(default=dict, blank=True)
    target_type = models.CharField(max_length=32, choices=TargetType.choices, default=TargetType.ALL)
    target_segment = models.ForeignKey(
        Segment,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="broadcasts",
    )
    target_tag = models.ForeignKey(
        Tag,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="broadcasts",
    )
    target_topic = models.ForeignKey(
        PushTopic,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="broadcasts",
    )
    manual_users = models.JSONField(default=list, blank=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.DRAFT)
    total_recipients = models.PositiveIntegerField(default=0)
    sent_count = models.PositiveIntegerField(default=0)
    failed_count = models.PositiveIntegerField(default=0)
    blocked_count = models.PositiveIntegerField(default=0)
    skipped_count = models.PositiveIntegerField(default=0)
    uncertain_count = models.PositiveIntegerField(default=0)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="broadcasts")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    scheduled_at = models.DateTimeField(blank=True, null=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)
    last_error = models.TextField(blank=True, default="")
    last_dry_run_preview = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "broadcasts"
        verbose_name = "Broadcast"
        verbose_name_plural = "Broadcasts"
        ordering = ("-created_at",)

    def clean(self):
        super().clean()
        from broadcasts.targets import InvalidAudience, campaign_target_kwargs, validate_target
        try:
            validate_target(**campaign_target_kwargs(self))
        except InvalidAudience as exc:
            raise ValidationError(str(exc)) from exc

    def __str__(self) -> str:
        return self.title


class BroadcastRecipient(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        UNCERTAIN = "uncertain", "In flight / outcome unknown (no automatic retry)"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        BLOCKED = "blocked", "Blocked"
        SKIPPED = "skipped", "Skipped"

    broadcast = models.ForeignKey(Broadcast, on_delete=models.CASCADE, related_name="recipients")
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="broadcast_recipients",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    error_message = models.TextField(blank=True, default="")
    sent_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "broadcast_recipients"
        verbose_name = "Broadcast Recipient"
        verbose_name_plural = "Broadcast Recipients"
        unique_together = ("broadcast", "user")
        ordering = ("id",)

    def __str__(self) -> str:
        return f"{self.broadcast_id}:{self.user_id}"


class AdminMessageLog(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Queued"
        UNCERTAIN = "uncertain", "In flight / outcome unknown (no automatic retry)"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        BLOCKED = "blocked", "Blocked"
        SKIPPED = "skipped", "Skipped"

    telegram_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="admin_messages",
        blank=True,
        null=True,
    )
    admin_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="admin_message_logs",
    )
    message_text = models.TextField()
    parse_mode = models.CharField(max_length=16, choices=Broadcast.ParseMode.choices, default=Broadcast.ParseMode.NONE)
    send_test_to_admin_first = models.BooleanField(default=False)
    target_chat_id = models.BigIntegerField()
    buttons_payload = models.JSONField(default=list, blank=True)
    image_url = models.CharField(max_length=512, blank=True, default="")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.QUEUED)
    response_payload = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    sent_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "admin_message_log"
        verbose_name = "Лог повідомлення"
        verbose_name_plural = "Логи повідомлень"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.telegram_user_id}:{self.status}"
