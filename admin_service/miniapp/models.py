from __future__ import annotations

from datetime import time

from django.db import models

from users.models import TelegramUser


class BrowserLoginTokenUse(models.Model):
    token_hash = models.CharField(max_length=64, unique=True)
    tg_user_id = models.BigIntegerField(db_index=True)
    expires_at = models.DateTimeField(db_index=True)
    used_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "miniapp_browser_login_token_uses"
        verbose_name = "Mini App browser login token use"
        verbose_name_plural = "Mini App browser login token uses"
        ordering = ("-used_at",)

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{self.used_at:%Y-%m-%d %H:%M:%S}"


class WriteReceipt(models.Model):
    tg_user_id = models.BigIntegerField(db_index=True)
    operation = models.CharField(max_length=64)
    idempotency_key = models.CharField(max_length=128)
    draft_id = models.CharField(max_length=128)
    status = models.CharField(max_length=32, default="pending")
    result = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_write_receipts"
        constraints = [
            models.UniqueConstraint(
                fields=("tg_user_id", "operation", "idempotency_key"),
                name="miniapp_write_receipt_key_unique",
            ),
        ]
        ordering = ("-updated_at",)

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{self.operation}:{self.status}"


class DraftAction(models.Model):
    """Durable reviewed intent; session snapshots cannot reopen its state."""

    tg_user_id = models.BigIntegerField(db_index=True)
    draft_id = models.CharField(max_length=128)
    operation = models.CharField(max_length=64)
    payload_hash = models.CharField(max_length=64)
    status = models.CharField(max_length=32, default="draft")
    result = models.JSONField(default=dict, blank=True)
    expires_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_draft_actions"
        constraints = [
            models.UniqueConstraint(
                fields=("tg_user_id", "draft_id"),
                name="miniapp_draft_action_unique",
            ),
        ]


class InstallNudgeState(models.Model):
    """Persistent Home Screen install campaign state shared with the Telegram bot."""

    tg_user_id = models.BigIntegerField(unique=True, db_index=True)
    prompt_count = models.PositiveSmallIntegerField(default=0)
    telegram_reminder_count = models.PositiveSmallIntegerField(default=0)
    next_prompt_at = models.DateTimeField(blank=True, null=True)
    next_telegram_reminder_at = models.DateTimeField(blank=True, null=True)
    last_prompted_at = models.DateTimeField(blank=True, null=True)
    last_telegram_reminder_at = models.DateTimeField(blank=True, null=True)
    installed_at = models.DateTimeField(blank=True, null=True)
    installed_platform = models.CharField(max_length=24, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_install_nudge_states"
        ordering = ("-updated_at",)

    def __str__(self) -> str:
        return f"{self.tg_user_id}:prompts={self.prompt_count}:installed={bool(self.installed_at)}"


class WebPushSubscription(models.Model):
    """One browser push endpoint for an installed or browser-hosted PWA."""

    tg_user_id = models.BigIntegerField(db_index=True)
    endpoint = models.TextField(unique=True)
    p256dh = models.TextField()
    auth = models.TextField()
    platform = models.CharField(max_length=24, blank=True, default="")
    device_label = models.CharField(max_length=120, blank=True, default="")
    user_agent = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True, db_index=True)
    failure_count = models.PositiveSmallIntegerField(default=0)
    last_success_at = models.DateTimeField(blank=True, null=True)
    last_failure_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_web_push_subscriptions"
        ordering = ("-updated_at",)
        indexes = [models.Index(fields=("tg_user_id", "is_active"), name="miniapp_push_user_active_idx")]

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{self.platform}:{'active' if self.is_active else 'inactive'}"


class NotificationPreference(models.Model):
    """User-facing notification choices shared by every PWA device."""

    tg_user_id = models.BigIntegerField(unique=True, db_index=True)
    master_enabled = models.BooleanField(default=True)
    billing_enabled = models.BooleanField(default=True)
    daily_expenses_enabled = models.BooleanField(default=True)
    savings_enabled = models.BooleanField(default=True)
    debts_enabled = models.BooleanField(default=True)
    family_enabled = models.BooleanField(default=True)
    weekly_summary_enabled = models.BooleanField(default=False)
    show_sensitive_details = models.BooleanField(default=False)
    quiet_hours_enabled = models.BooleanField(default=True)
    quiet_hours_start = models.TimeField(default=time(22, 0))
    quiet_hours_end = models.TimeField(default=time(8, 0))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_notification_preferences"
        ordering = ("tg_user_id",)

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{'on' if self.master_enabled else 'off'}"


class AppNotification(models.Model):
    class Status(models.TextChoices):
        UNREAD = "unread", "Unread"
        READ = "read", "Read"
        RESOLVED = "resolved", "Resolved"
        DISMISSED = "dismissed", "Dismissed"

    tg_user_id = models.BigIntegerField(db_index=True)
    event_type = models.CharField(max_length=64, db_index=True)
    category = models.CharField(max_length=24, db_index=True)
    idempotency_key = models.CharField(max_length=160)
    title = models.CharField(max_length=160)
    body = models.TextField()
    target_url = models.TextField(default="/app/")
    payload = models.JSONField(default=dict, blank=True)
    requires_action = models.BooleanField(default=False)
    contributes_to_badge = models.BooleanField(default=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.UNREAD, db_index=True)
    read_at = models.DateTimeField(blank=True, null=True)
    resolved_at = models.DateTimeField(blank=True, null=True)
    expires_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_app_notifications"
        ordering = ("-created_at", "-id")
        constraints = [
            models.UniqueConstraint(
                fields=("tg_user_id", "idempotency_key"),
                name="miniapp_notification_user_key_unique",
            )
        ]
        indexes = [
            models.Index(fields=("tg_user_id", "status", "created_at"), name="miniapp_notice_inbox_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{self.event_type}:{self.status}"


class PushDelivery(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        GONE = "gone", "Gone"

    notification = models.ForeignKey(AppNotification, on_delete=models.CASCADE, related_name="deliveries")
    subscription = models.ForeignKey(WebPushSubscription, on_delete=models.CASCADE, related_name="deliveries")
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING)
    response_code = models.IntegerField(blank=True, null=True)
    error = models.TextField(blank=True, default="")
    sent_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "miniapp_push_deliveries"
        constraints = [
            models.UniqueConstraint(
                fields=("notification", "subscription"),
                name="miniapp_push_delivery_unique",
            )
        ]

    def __str__(self) -> str:
        return f"{self.notification_id}:{self.subscription_id}:{self.status}"


class DailyExpenseReminderSetting(models.Model):
    """Shared bot setting; the bot owns table creation and scheduled delivery."""

    user = models.OneToOneField(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        to_field="tg_user_id",
        db_column="tg_user_id",
        related_name="+",
    )
    mode = models.CharField(max_length=16, default="daily")
    reminder_hour = models.IntegerField(default=21)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "daily_expense_reminder_settings"
        verbose_name = "Daily expense reminder setting"
        verbose_name_plural = "Daily expense reminder settings"

    def __str__(self) -> str:
        return f"{self.user_id}:{self.mode}@{self.reminder_hour}"


class SavingPromptSetting(models.Model):
    """Shared bot setting for post-income saving prompts."""

    user = models.OneToOneField(
        TelegramUser,
        on_delete=models.DO_NOTHING,
        to_field="tg_user_id",
        db_column="tg_user_id",
        related_name="+",
    )
    enabled = models.BooleanField(default=True)
    default_percent = models.DecimalField(max_digits=5, decimal_places=2, default=10)
    secondary_percent = models.DecimalField(max_digits=5, decimal_places=2, blank=True, null=True)
    default_target_account_id = models.BigIntegerField(blank=True, null=True)
    ask_after_income = models.BooleanField(default=True)
    ask_only_for_salary = models.BooleanField(default=False)
    min_income_amount = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    reminder_enabled = models.BooleanField(default=True)
    default_reminder_delay = models.TextField(blank=True, null=True)
    playful_tone_enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = "saving_prompt_settings"
        verbose_name = "Saving prompt setting"
        verbose_name_plural = "Saving prompt settings"

    def __str__(self) -> str:
        return f"{self.user_id}:{self.default_percent}%"


class AiTransactionDraft(models.Model):
    """Runtime-owned pending draft, including optional billing expense prompts."""

    id = models.BigAutoField(primary_key=True)
    tg_user_id = models.BigIntegerField(db_index=True)
    source = models.TextField()
    telegram_file_unique_id = models.TextField(blank=True, null=True)
    status = models.TextField(default="pending")
    transaction_date = models.DateField(blank=True, null=True)
    tx_type = models.TextField(blank=True, null=True)
    amount = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    currency = models.TextField(blank=True, null=True)
    account_id = models.BigIntegerField(blank=True, null=True)
    category_id = models.BigIntegerField(blank=True, null=True)
    comment = models.TextField(blank=True, null=True)
    confidence = models.DecimalField(max_digits=5, decimal_places=4, blank=True, null=True)
    metadata = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    confirmed_at = models.DateTimeField(blank=True, null=True)
    cancelled_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "ai_transaction_drafts"
        ordering = ("-updated_at", "-id")

    def __str__(self) -> str:
        return f"{self.tg_user_id}:{self.source}:{self.status}"
