from __future__ import annotations

from django.db import models

from support.models import SupportCase
from users.models import Tag, TelegramUser


class FeedbackItem(models.Model):
    class Source(models.TextChoices):
        ONBOARDING = "onboarding", "Onboarding"
        TRANSACTION_FLOW = "transaction_flow", "Transaction Flow"
        SUBSCRIPTION = "subscription", "Subscription"
        POLL = "poll", "Poll"
        MANUAL = "manual", "Manual"
        SUPPORT = "support", "Support"

    class Category(models.TextChoices):
        BUG = "bug", "Bug"
        UX = "ux", "UX"
        FEATURE_REQUEST = "feature_request", "Feature Request"
        PAYMENT = "payment", "Payment"
        RECOGNITION = "recognition", "Recognition"
        OTHER = "other", "Other"

    class Status(models.TextChoices):
        NEW = "new", "New"
        REVIEWED = "reviewed", "Reviewed"
        PLANNED = "planned", "Planned"
        REJECTED = "rejected", "Rejected"
        CONVERTED_TO_CASE = "converted_to_case", "Converted to Case"

    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.SET_NULL,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        blank=True,
        null=True,
        related_name="feedback_items",
    )
    source = models.CharField(max_length=32, choices=Source.choices, default=Source.MANUAL)
    rating = models.IntegerField(blank=True, null=True)
    text = models.TextField()
    category = models.CharField(max_length=32, choices=Category.choices, default=Category.OTHER)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.NEW)
    tags = models.ManyToManyField(Tag, blank=True, related_name="feedback_items")
    support_case = models.ForeignKey(
        SupportCase,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="feedback_items",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "feedback_items"
        verbose_name = "Feedback"
        verbose_name_plural = "Feedback"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.category}:{self.user_id or '-'}"

