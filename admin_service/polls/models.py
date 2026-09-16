from __future__ import annotations

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models

from broadcasts.models import Segment
from users.models import PushTopic, Tag, TelegramUser


class PollCampaign(models.Model):
    class Type(models.TextChoices):
        RATING = "rating", "Rating"
        NPS = "nps", "NPS"
        SINGLE_CHOICE = "single_choice", "Single Choice"
        TEXT = "text", "Text"

    class TargetType(models.TextChoices):
        ALL = "all", "All"
        SEGMENT = "segment", "Segment"
        TAG = "tag", "Tag"
        MANUAL_USERS = "manual_users", "Manual Users"
        PUSH_TOPIC = "push_topic", "Push Topic"

    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        SCHEDULED = "scheduled", "Scheduled"
        ACTIVE = "active", "Active"
        COMPLETED = "completed", "Completed"
        CANCELLED = "cancelled", "Cancelled"

    title = models.CharField(max_length=255)
    question = models.TextField()
    type = models.CharField(max_length=32, choices=Type.choices, default=Type.RATING)
    options = models.JSONField(default=list, blank=True)
    target_type = models.CharField(max_length=32, choices=TargetType.choices, default=TargetType.ALL)
    target_segment = models.ForeignKey(
        Segment,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="poll_campaigns",
    )
    target_tag = models.ForeignKey(
        Tag,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="poll_campaigns",
    )
    target_topic = models.ForeignKey(
        PushTopic,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="poll_campaigns",
    )
    manual_users = models.JSONField(default=list, blank=True)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.DRAFT)
    scheduled_at = models.DateTimeField(blank=True, null=True)
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="poll_campaigns")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    total_recipients = models.PositiveIntegerField(default=0)
    response_count = models.PositiveIntegerField(default=0)
    low_rating_threshold = models.PositiveIntegerField(default=3)

    class Meta:
        db_table = "poll_campaigns"
        verbose_name = "Poll Campaign"
        verbose_name_plural = "Poll Campaigns"
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


class PollRecipient(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        UNCERTAIN = "uncertain", "У роботі / результат невідомий (без автоматичного повтору)"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        BLOCKED = "blocked", "Blocked"
        RESPONDED = "responded", "Responded"

    campaign = models.ForeignKey(PollCampaign, on_delete=models.CASCADE, related_name="recipients")
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="poll_recipients",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    error_message = models.TextField(blank=True, default="")
    sent_at = models.DateTimeField(blank=True, null=True)
    responded_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "poll_recipients"
        verbose_name = "Poll Recipient"
        verbose_name_plural = "Poll Recipients"
        unique_together = ("campaign", "user")
        ordering = ("id",)

    def __str__(self) -> str:
        return f"{self.campaign_id}:{self.user_id}"


class PollResponse(models.Model):
    campaign = models.ForeignKey(PollCampaign, on_delete=models.CASCADE, related_name="responses")
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="poll_responses",
    )
    answer = models.TextField(blank=True, default="")
    rating_value = models.IntegerField(blank=True, null=True)
    text_answer = models.TextField(blank=True, default="")
    telegram_poll_id = models.CharField(max_length=255, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "poll_responses"
        verbose_name = "Poll Response"
        verbose_name_plural = "Poll Responses"
        unique_together = ("campaign", "user")
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.campaign_id}:{self.user_id}"

