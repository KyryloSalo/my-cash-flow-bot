from __future__ import annotations

import uuid

from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.text import slugify


class TelegramUser(models.Model):
    tg_user_id = models.BigIntegerField(primary_key=True, db_column="tg_user_id", verbose_name="Telegram ID")
    first_name = models.TextField(blank=True, null=True)
    last_name = models.TextField(blank=True, null=True)
    username = models.TextField(blank=True, null=True)
    lang = models.TextField(blank=True, null=True)
    base_currency = models.TextField(blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)
    onboarding_completed = models.BooleanField(default=False)
    onboarding_version = models.IntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    last_seen_at = models.DateTimeField(default=timezone.now)

    class Meta:
        managed = False
        db_table = "users"
        verbose_name = "Користувач"
        verbose_name_plural = "Користувачі"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        if self.username:
            return f"@{self.username}"
        return f"{self.tg_user_id}"

    @property
    def internal_id(self) -> int:
        return self.tg_user_id

    @property
    def full_name(self) -> str:
        return " ".join(part for part in [self.first_name, self.last_name] if part).strip() or str(self.tg_user_id)


class UserAuthIdentity(models.Model):
    class Provider(models.TextChoices):
        TELEGRAM_OIDC = "telegram_oidc", "Telegram OIDC"
        TELEGRAM_MINIAPP = "telegram_miniapp", "Telegram Mini App"

    telegram_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="auth_identities",
    )
    provider = models.CharField(max_length=32, choices=Provider.choices)
    subject = models.CharField(max_length=255)
    profile = models.JSONField(default=dict, blank=True)
    first_authenticated_at = models.DateTimeField()
    last_authenticated_at = models.DateTimeField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_auth_identities"
        constraints = [
            models.UniqueConstraint(fields=("provider", "subject"), name="uniq_user_auth_provider_subject"),
            models.UniqueConstraint(fields=("provider", "telegram_user"), name="uniq_user_auth_provider_user"),
        ]

    def __str__(self) -> str:
        return f"{self.provider}:{self.subject}"


class UserAuthSession(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    telegram_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="auth_sessions",
    )
    identity = models.ForeignKey(
        UserAuthIdentity,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="auth_sessions",
    )
    token_hash = models.CharField(max_length=64, unique=True)
    auth_mode = models.CharField(max_length=32)
    user_agent_hash = models.CharField(max_length=64, blank=True, default="")
    expires_at = models.DateTimeField()
    last_seen_at = models.DateTimeField()
    revoked_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_auth_sessions"
        indexes = [
            models.Index(fields=("telegram_user", "revoked_at"), name="user_auth_active_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.telegram_user_id}:{self.auth_mode}:{self.id}"


class UserOidcTokenUse(models.Model):
    token_hash = models.CharField(max_length=64, primary_key=True)
    provider = models.CharField(max_length=32)
    subject = models.CharField(max_length=255)
    tg_user_id = models.BigIntegerField(db_index=True)
    expires_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_oidc_token_uses"

    def __str__(self) -> str:
        return f"{self.provider}:{self.subject}:{self.token_hash[:12]}"


class UserAdminState(models.Model):
    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        BANNED = "banned", "Banned"
        INACTIVE = "inactive", "Inactive"

    class SubscriptionStatus(models.TextChoices):
        NONE = "none", "None"
        TRIAL = "trial", "Trial"
        PAID = "paid", "Paid"
        EXPIRED = "expired", "Expired"
        CANCELLED = "cancelled", "Cancelled"

    class AccessScope(models.TextChoices):
        PERSONAL_FULL = "personal_full", "Personal full"
        FAMILY_FULL = "family_full", "Family full"
        DEBT_ONLY = "debt_only", "Debt only"
        PAYWALL = "paywall", "Paywall"

    telegram_user = models.OneToOneField(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="admin_state",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.ACTIVE)
    subscription_status = models.CharField(
        max_length=32,
        choices=SubscriptionStatus.choices,
        default=SubscriptionStatus.NONE,
    )
    access_scope = models.CharField(
        max_length=32,
        choices=AccessScope.choices,
        default=AccessScope.PAYWALL,
    )
    access_source = models.CharField(max_length=128, blank=True, default="")
    source = models.CharField(max_length=128, blank=True, default="")
    referral_code = models.CharField(max_length=128, blank=True, default="")
    pending_start_payload = models.TextField(blank=True, default="")
    timezone = models.CharField(max_length=64, blank=True, default="")
    last_action_at = models.DateTimeField(blank=True, null=True)
    is_blocked = models.BooleanField(default=False)
    can_receive_messages = models.BooleanField(default=True)
    blocked_bot = models.BooleanField(default=False)
    admin_comment = models.TextField(blank=True, default="")
    is_test_user = models.BooleanField(default=False)
    test_user_notes = models.TextField(blank=True, default="")
    marked_as_test_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="marked_test_users",
    )
    marked_as_test_at = models.DateTimeField(blank=True, null=True)
    current_fsm_state = models.CharField(max_length=255, blank=True, default="")
    onboarding_payload = models.JSONField(default=dict, blank=True)
    last_user_input = models.TextField(blank=True, default="")
    last_bot_response = models.TextField(blank=True, default="")
    last_parse_error = models.TextField(blank=True, default="")
    last_onboarding_event_at = models.DateTimeField(blank=True, null=True)
    pending_admin_reset_mode = models.CharField(max_length=32, blank=True, default="")
    pending_admin_reset_requested_at = models.DateTimeField(blank=True, null=True)
    last_admin_reset_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_admin_states"
        verbose_name = "Стан користувача"
        verbose_name_plural = "Стан користувачів"
        permissions = [
            ("can_use_test_tools", "Може використовувати тестові та QA-інструменти"),
        ]

    def __str__(self) -> str:
        return f"{self.telegram_user_id}: {self.status}"


class AdminNote(models.Model):
    telegram_user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="admin_notes",
    )
    admin_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="cashflow_admin_notes",
    )
    note_text = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "admin_notes"
        verbose_name = "Нотатка адміна"
        verbose_name_plural = "Нотатки адміна"
        ordering = ("-updated_at",)

    def __str__(self) -> str:
        return f"Note for {self.telegram_user_id}"


class Tag(models.Model):
    name = models.CharField(max_length=128, unique=True)
    slug = models.SlugField(max_length=128, unique=True, blank=True)
    description = models.TextField(blank=True, default="")
    color = models.CharField(max_length=32, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_tags"
        verbose_name = "Тег"
        verbose_name_plural = "Теги"
        ordering = ("name",)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.name, allow_unicode=True)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class UserTag(models.Model):
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="user_tags",
    )
    tag = models.ForeignKey(Tag, on_delete=models.CASCADE, related_name="user_links")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="created_user_tags",
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "user_tag_links"
        verbose_name = "Тег користувача"
        verbose_name_plural = "Теги користувачів"
        unique_together = ("user", "tag")
        ordering = ("tag__name",)

    def __str__(self) -> str:
        return f"{self.user_id}:{self.tag_id}"


class PushTopic(models.Model):
    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=128, unique=True, blank=True)
    description = models.TextField(blank=True, default="")
    is_system = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "push_topics"
        verbose_name = "Тема повідомлень"
        verbose_name_plural = "Теми повідомлень"
        ordering = ("name",)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.name, allow_unicode=True)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class UserPushTopic(models.Model):
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="push_topic_settings",
    )
    topic = models.ForeignKey(PushTopic, on_delete=models.CASCADE, related_name="user_links")
    is_enabled = models.BooleanField(default=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "user_push_topic_links"
        verbose_name = "Тема користувача"
        verbose_name_plural = "Теми користувачів"
        unique_together = ("user", "topic")
        ordering = ("topic__name",)

    def __str__(self) -> str:
        return f"{self.user_id}:{self.topic_id}"
