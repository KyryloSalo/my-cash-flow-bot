from __future__ import annotations

from datetime import time

from django.conf import settings
from django.db import models
from django.utils.text import slugify

from users.models import TelegramUser


class Plan(models.Model):
    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=128, unique=True, blank=True)
    price = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    currency = models.CharField(max_length=16, default="USD")
    duration_days = models.PositiveIntegerField(default=30)
    is_active = models.BooleanField(default=True)
    is_public = models.BooleanField(default=True)
    is_archived = models.BooleanField(default=False)
    description = models.TextField(blank=True, default="")
    features = models.JSONField(default=list, blank=True)
    display_order = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "plans"
        verbose_name = "Тарифний план"
        verbose_name_plural = "Тарифні плани"
        ordering = ("display_order", "name")

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.name, allow_unicode=True)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class PromoOffer(models.Model):
    code = models.CharField(max_length=64, unique=True)
    label = models.CharField(max_length=128, blank=True, default="")
    trial_days = models.PositiveIntegerField(default=90)
    source = models.CharField(max_length=128, blank=True, default="")
    is_active = models.BooleanField(default=True)
    max_uses = models.PositiveIntegerField(blank=True, null=True)
    starts_at = models.DateTimeField(blank=True, null=True)
    ends_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "promo_offers"
        verbose_name = "Промо-офер"
        verbose_name_plural = "Промо-офери"
        ordering = ("code",)

    def save(self, *args, **kwargs):
        self.code = (self.code or "").strip().upper()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.label or self.code


class Subscription(models.Model):
    class Status(models.TextChoices):
        TRIAL = "trial", "Trial"
        ACTIVE = "active", "Active"
        PAID = "paid", "Paid (Legacy)"
        EXPIRED = "expired", "Expired"
        CANCELLED = "cancelled", "Cancelled"
        MANUAL = "manual", "Manual"
        LIFETIME = "lifetime", "Lifetime"

    class Source(models.TextChoices):
        SYSTEM = "system", "System"
        PAYMENT = "payment", "Payment"
        ADMIN = "admin", "Admin"
        PROMO = "promo", "Promo"

    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="subscriptions",
    )
    plan = models.CharField(max_length=64, default="solo")
    plan_ref = models.ForeignKey(
        Plan,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="subscriptions",
    )
    promo_offer = models.ForeignKey(
        PromoOffer,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="subscriptions",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.TRIAL)
    provider = models.CharField(max_length=64, blank=True, default="")
    amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    currency = models.CharField(max_length=16, blank=True, default="")
    started_at = models.DateTimeField(blank=True, null=True)
    expires_at = models.DateTimeField(blank=True, null=True)
    next_charge_at = models.DateTimeField(blank=True, null=True)
    grace_expires_at = models.DateTimeField(blank=True, null=True)
    trial_days = models.PositiveIntegerField(default=0)
    payment_id = models.CharField(max_length=255, blank=True, default="")
    auto_renew = models.BooleanField(default=False)
    source = models.CharField(max_length=32, choices=Source.choices, default=Source.SYSTEM)
    comment = models.TextField(blank=True, default="")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="created_subscriptions",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="updated_subscriptions",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "subscriptions"
        verbose_name = "Підписка"
        verbose_name_plural = "Підписки"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.user_id}: {self.status}"

    @property
    def start_at(self):
        return self.started_at

    @property
    def end_at(self):
        return self.expires_at


class BillingProfile(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        ACTIVE = "active", "Active"
        ACTION_REQUIRED = "action_required", "Action required"
        FAILED = "failed", "Failed"
        CANCELLED = "cancelled", "Cancelled"
        MISSING_TOKEN = "missing_token", "Missing token"

    user = models.OneToOneField(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="billing_profile",
    )
    provider = models.CharField(max_length=64, default="monobank")
    wallet_id = models.CharField(max_length=255, unique=True)
    card_token = models.CharField(max_length=512, blank=True, default="")
    masked_pan = models.CharField(max_length=64, blank=True, default="")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    auto_renew_enabled = models.BooleanField(default=True)
    last_charge_status = models.CharField(max_length=64, blank=True, default="")
    last_failure_reason = models.TextField(blank=True, default="")
    last_action_url = models.CharField(max_length=1024, blank=True, default="")
    last_bound_at = models.DateTimeField(blank=True, null=True)
    last_charge_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "billing_profiles"
        verbose_name = "Billing profile"
        verbose_name_plural = "Billing profiles"

    def __str__(self) -> str:
        return f"{self.user_id}: {self.status}"


class Payment(models.Model):
    class Status(models.TextChoices):
        CREATED = "created", "Created (Legacy)"
        PENDING = "pending", "Pending"
        PAID = "paid", "Paid"
        FAILED = "failed", "Failed"
        REJECTED = "rejected", "Rejected"
        REFUNDED = "refunded", "Refunded"
        MANUAL_CONFIRMED = "manual_confirmed", "Manual Confirmed"

    class Kind(models.TextChoices):
        LEGACY = "legacy", "Legacy"
        BIND = "bind", "Bind"
        RENEWAL = "renewal", "Renewal"
        RETRY = "retry", "Retry"

    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="payments",
    )
    subscription = models.ForeignKey(
        Subscription,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="payments",
    )
    plan = models.ForeignKey(
        Plan,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="payments",
    )
    provider = models.CharField(max_length=64, blank=True, default="")
    provider_payment_id = models.CharField(max_length=255, unique=True, blank=True, null=True)
    external_transaction_id = models.CharField(max_length=255, unique=True, blank=True, null=True)
    amount = models.DecimalField(max_digits=12, decimal_places=2)
    currency = models.CharField(max_length=16)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    kind = models.CharField(max_length=32, choices=Kind.choices, default=Kind.LEGACY)
    action_url = models.CharField(max_length=1024, blank=True, default="")
    screenshot = models.CharField(max_length=512, blank=True, default="")
    admin_comment = models.TextField(blank=True, default="")
    paid_at = models.DateTimeField(blank=True, null=True)
    provider_modified_at = models.DateTimeField(blank=True, null=True)
    confirmed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="confirmed_payments",
    )
    raw_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "payments"
        verbose_name = "Платіж"
        verbose_name_plural = "Платежі"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.user_id}: {self.status} {self.amount} {self.currency}"


class SubscriptionEvent(models.Model):
    subscription = models.ForeignKey(
        Subscription,
        on_delete=models.CASCADE,
        related_name="events",
    )
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="subscription_events",
    )
    event_type = models.CharField(max_length=64)
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "subscription_events"
        verbose_name = "Подія підписки"
        verbose_name_plural = "Події підписки"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return f"{self.user_id}: {self.event_type}"


class PromoOfferClaim(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        CONSUMED = "consumed", "Consumed"
        IGNORED = "ignored", "Ignored"

    offer = models.ForeignKey(
        PromoOffer,
        on_delete=models.CASCADE,
        related_name="claims",
    )
    user = models.ForeignKey(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="promo_claims",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.PENDING)
    bind_payment = models.ForeignKey(
        Payment,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="promo_claims",
    )
    subscription = models.ForeignKey(
        Subscription,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="promo_claims",
    )
    consumed_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "promo_offer_claims"
        verbose_name = "Заявка на промо-офер"
        verbose_name_plural = "Заявки на промо-офери"
        unique_together = ("offer", "user")
        ordering = ("-updated_at",)

    def __str__(self) -> str:
        return f"{self.offer.code} -> {self.user_id} ({self.status})"


class TrialRecoveryCampaign(models.Model):
    class Status(models.TextChoices):
        DRAFT = "draft", "Draft"
        SCHEDULED = "scheduled", "Scheduled"
        RUNNING = "running", "Running"
        PAUSED = "paused", "Paused"
        COMPLETED = "completed", "Completed"

    name = models.CharField(max_length=255, default="90-day trial recovery")
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.DRAFT)
    fallback_timezone = models.CharField(max_length=64, default="Europe/Kyiv")
    send_window_start = models.TimeField(default=time(10, 0))
    send_window_end = models.TimeField(default=time(19, 0))
    launch_at = models.DateTimeField(blank=True, null=True)
    started_at = models.DateTimeField(blank=True, null=True)
    completed_at = models.DateTimeField(blank=True, null=True)
    launched_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="launched_trial_recovery_campaigns",
    )
    audience_snapshot = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "trial_recovery_campaigns"
        verbose_name = "Recovery-кампанія trial"
        verbose_name_plural = "Recovery-кампанії trial"
        ordering = ("-created_at",)

    def __str__(self) -> str:
        return self.name


class TrialRecoveryRecipient(models.Model):
    class Status(models.TextChoices):
        SCHEDULED = "scheduled", "Scheduled"
        RESPONDED = "responded", "Responded"
        CONTACT_REQUESTED = "contact_requested", "Contact requested"
        CONVERTED = "converted", "Converted"
        OPTED_OUT = "opted_out", "Opted out"
        INELIGIBLE = "ineligible", "Ineligible"
        COMPLETED = "completed", "Completed"
        DELIVERY_FAILED = "delivery_failed", "Delivery failed"

    class Source(models.TextChoices):
        BACKFILL = "backfill", "Backfill"
        LIVE = "live", "Live"

    class Reason(models.TextChoices):
        CARD = "card", "Does not want to link a card"
        AUTORENEW = "autorenew", "Worried about auto-renewal"
        VALUE = "value", "Does not see the value"
        PRICE = "price", "Price after trial is too high"
        ERROR = "error", "Technical or payment error"
        LATER = "later", "Not the right time"
        OTHER = "other", "Other"

    campaign = models.ForeignKey(
        TrialRecoveryCampaign,
        on_delete=models.CASCADE,
        related_name="recipients",
    )
    user = models.OneToOneField(
        TelegramUser,
        on_delete=models.CASCADE,
        to_field="tg_user_id",
        db_column="telegram_user_id",
        related_name="trial_recovery_recipient",
    )
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.SCHEDULED)
    source = models.CharField(max_length=16, choices=Source.choices, default=Source.LIVE)
    reason = models.CharField(max_length=32, choices=Reason.choices, blank=True, default="")
    free_text = models.TextField(blank=True, default="")
    timezone = models.CharField(max_length=64, default="Europe/Kyiv")
    offered_at = models.DateTimeField(blank=True, null=True)
    shown_at = models.DateTimeField(blank=True, null=True)
    dismissed_at = models.DateTimeField(blank=True, null=True)
    first_sent_at = models.DateTimeField(blank=True, null=True)
    last_sent_at = models.DateTimeField(blank=True, null=True)
    next_send_at = models.DateTimeField(blank=True, null=True, db_index=True)
    last_sent_local_date = models.DateField(blank=True, null=True)
    sent_count = models.PositiveSmallIntegerField(default=0)
    responded_at = models.DateTimeField(blank=True, null=True)
    awaiting_text_until = models.DateTimeField(blank=True, null=True)
    contact_requested_at = models.DateTimeField(blank=True, null=True)
    personal_contact_declined_at = models.DateTimeField(blank=True, null=True)
    opted_out_at = models.DateTimeField(blank=True, null=True)
    converted_at = models.DateTimeField(blank=True, null=True)
    feedback_item_id = models.BigIntegerField(blank=True, null=True)
    support_case_id = models.BigIntegerField(blank=True, null=True)
    ineligible_reason = models.CharField(max_length=128, blank=True, default="")
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "trial_recovery_recipients"
        verbose_name = "Отримувач recovery-кампанії"
        verbose_name_plural = "Отримувачі recovery-кампанії"
        ordering = ("next_send_at", "id")
        indexes = [
            models.Index(fields=("campaign", "status", "next_send_at"), name="trialrec_due_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.user_id}: {self.status}"


class TrialRecoveryDelivery(models.Model):
    class Status(models.TextChoices):
        CLAIMED = "claimed", "Claimed"
        RETRY = "retry", "Retry"
        UNKNOWN = "unknown", "Outcome unknown"
        SENT = "sent", "Sent"
        FAILED = "failed", "Failed"
        SKIPPED = "skipped", "Skipped"

    recipient = models.ForeignKey(
        TrialRecoveryRecipient,
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    step = models.PositiveSmallIntegerField()
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.CLAIMED)
    telegram_message_id = models.BigIntegerField(blank=True, null=True)
    error_message = models.TextField(blank=True, default="")
    attempt_count = models.PositiveSmallIntegerField(default=0)
    claimed_at = models.DateTimeField(blank=True, null=True)
    next_retry_at = models.DateTimeField(blank=True, null=True, db_index=True)
    sent_at = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "trial_recovery_deliveries"
        verbose_name = "Доставка recovery-повідомлення"
        verbose_name_plural = "Доставки recovery-повідомлень"
        ordering = ("-created_at",)
        constraints = [
            models.UniqueConstraint(fields=("recipient", "step"), name="trialrec_delivery_step_unique"),
        ]

    def __str__(self) -> str:
        return f"{self.recipient_id}:{self.step}:{self.status}"
