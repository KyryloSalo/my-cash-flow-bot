from __future__ import annotations

from django.db import models

from users.models import TelegramUser


class BotEvent(models.Model):
    EVENT_CHOICES = [
        ("start", "Start"),
        ("new_user_registered", "New User Registered"),
        ("onboarding_started", "Onboarding Started"),
        ("onboarding_step_completed", "Onboarding Step Completed"),
        ("onboarding_completed", "Onboarding Completed"),
        ("onboarding_failed", "Onboarding Failed"),
        ("onboarding_reset_by_admin", "Onboarding Reset By Admin"),
        ("text_received", "Text Received"),
        ("voice_received", "Voice Received"),
        ("button_clicked", "Button Clicked"),
        ("transaction_created", "Transaction Created"),
        ("transaction_failed", "Transaction Failed"),
        ("category_created", "Category Created"),
        ("category_updated", "Category Updated"),
        ("account_created", "Account Created"),
        ("subscription_started", "Subscription Started"),
        ("subscription_expired", "Subscription Expired"),
        ("payment_success", "Payment Success"),
        ("payment_failed", "Payment Failed"),
        ("parse_error", "Parse Error"),
        ("bot_error", "Bot Error"),
        ("manual_message_sent", "Manual Message Sent"),
        ("broadcast_completed", "Broadcast Completed"),
        ("broadcast_failed", "Broadcast Failed"),
        ("healthcheck_run", "Healthcheck Run"),
    ]

    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.SET_NULL,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="bot_events",
        blank=True,
        null=True,
    )
    event_type = models.CharField(max_length=64, choices=EVENT_CHOICES)
    source = models.CharField(max_length=64, blank=True, default="")
    raw_input = models.TextField(blank=True, default="")
    parsed_result = models.JSONField(default=dict, blank=True)
    success = models.BooleanField(default=True)
    error_message = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "bot_events"
        verbose_name = "Подія бота"
        verbose_name_plural = "Події бота"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.event_type} ({self.user_id or 'system'})"
