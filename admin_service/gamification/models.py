import uuid

from django.db import models
from django.utils import timezone


class GamificationProfile(models.Model):
    class Mascot(models.TextChoices):
        BOB = "bob", "Боб"
        CAPI = "capi", "Капі"

    user_id = models.BigIntegerField(primary_key=True)
    mascot = models.CharField(max_length=8, choices=Mascot.choices, blank=True, default="")
    mascot_selected_at = models.DateTimeField(null=True, blank=True)
    timezone_name = models.CharField(max_length=64, default="Europe/Kyiv")
    pending_timezone_name = models.CharField(max_length=64, blank=True, default="")
    pending_timezone_effective_at = models.DateTimeField(null=True, blank=True)
    timezone_changed_at = models.DateTimeField(null=True, blank=True)
    motion_enabled = models.BooleanField(default=True)
    started_at = models.DateTimeField(default=timezone.now)
    rule_version = models.CharField(max_length=16, default="v1")
    revision = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "gamification_profiles"


class GamificationTimezoneChange(models.Model):
    user_id = models.BigIntegerField()
    previous_timezone_name = models.CharField(max_length=64)
    new_timezone_name = models.CharField(max_length=64)
    requested_at = models.DateTimeField(default=timezone.now)
    effective_at = models.DateTimeField()
    applied_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "gamification_timezone_changes"
        indexes = [models.Index(fields=("user_id", "-requested_at"), name="gam_tz_user_time_idx")]


class GamificationDay(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        ACTIVE = "active", "Active"
        FROZEN = "frozen", "Frozen"
        MISSED = "missed", "Missed"

    user_id = models.BigIntegerField()
    day_seq = models.PositiveIntegerField()
    local_date = models.DateField()
    timezone_name = models.CharField(max_length=64)
    starts_at = models.DateTimeField()
    ends_at = models.DateTimeField()
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.PENDING)
    activated_at = models.DateTimeField(null=True, blank=True)
    finalized_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_days"
        constraints = [
            models.UniqueConstraint(fields=("user_id", "day_seq"), name="uniq_gamification_user_day_seq"),
            models.UniqueConstraint(fields=("user_id", "local_date"), name="uniq_gamification_user_local_date"),
        ]
        indexes = [models.Index(fields=("user_id", "-local_date"), name="gam_day_user_date_idx")]


class StreakState(models.Model):
    user_id = models.BigIntegerField(primary_key=True)
    current_streak = models.PositiveIntegerField(default=0)
    best_streak = models.PositiveIntegerField(default=0)
    lifetime_active_days = models.PositiveIntegerField(default=0)
    shield_balance = models.PositiveSmallIntegerField(default=0)
    last_active_date = models.DateField(null=True, blank=True)
    had_broken_streak = models.BooleanField(default=False)
    inactive_gap = models.PositiveIntegerField(default=0)
    comeback_run = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "gamification_streak_states"


class ShieldAccrualState(models.Model):
    user_id = models.BigIntegerField(primary_key=True)
    anchor_lifetime_active_days = models.PositiveIntegerField(null=True, blank=True)
    last_cycle = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "gamification_shield_accrual_states"


class ShieldLedger(models.Model):
    user_id = models.BigIntegerField()
    entitlement_key = models.CharField(max_length=64)
    delta = models.SmallIntegerField()
    balance_after = models.PositiveSmallIntegerField()
    reason = models.CharField(max_length=32)
    day = models.ForeignKey(
        GamificationDay,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="shield_entries",
    )
    trigger_event_id = models.UUIDField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_shield_ledger"
        constraints = [
            models.UniqueConstraint(
                fields=("user_id", "entitlement_key"),
                name="uniq_gamification_shield_entitlement",
            ),
            models.CheckConstraint(
                condition=models.Q(balance_after__lte=2),
                name="gamification_shield_balance_lte_two",
            ),
        ]
        indexes = [models.Index(fields=("user_id", "-created_at"), name="gam_shield_user_time_idx")]


class GamificationEventOutbox(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PROCESSING = "processing", "Processing"
        PROCESSED = "processed", "Processed"
        IGNORED = "ignored", "Ignored"
        FAILED = "failed", "Failed"

    event_id = models.UUIDField(primary_key=True)
    event_type = models.CharField(max_length=64)
    actor_user_id = models.BigIntegerField()
    space_id = models.BigIntegerField(null=True, blank=True)
    entity_type = models.CharField(max_length=32)
    entity_id = models.CharField(max_length=128)
    accepted_at = models.DateTimeField()
    input_method = models.CharField(max_length=24, default="manual")
    payload = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.PENDING)
    attempts = models.PositiveSmallIntegerField(default=0)
    available_at = models.DateTimeField(default=timezone.now)
    processed_at = models.DateTimeField(null=True, blank=True)
    last_error_code = models.CharField(max_length=64, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_event_outbox"
        constraints = [
            models.UniqueConstraint(
                fields=("event_type", "entity_type", "entity_id"),
                name="uniq_gamification_event_entity",
            )
        ]
        indexes = [
            models.Index(fields=("status", "available_at", "accepted_at"), name="gam_outbox_pending_idx"),
            models.Index(fields=("actor_user_id", "accepted_at"), name="gam_outbox_actor_idx"),
        ]


class ProcessedGamificationEvent(models.Model):
    event_id = models.UUIDField(primary_key=True)
    actor_user_id = models.BigIntegerField()
    processed_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_processed_events"


class ActivityContribution(models.Model):
    day = models.ForeignKey(GamificationDay, on_delete=models.CASCADE, related_name="contributions")
    user_id = models.BigIntegerField()
    kind = models.CharField(max_length=24, default="transaction")
    entity_type = models.CharField(max_length=32)
    entity_id = models.CharField(max_length=128)
    input_method = models.CharField(max_length=24, blank=True, default="")
    accepted_at = models.DateTimeField()
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_activity_contributions"
        constraints = [
            models.UniqueConstraint(
                fields=("user_id", "kind", "entity_type", "entity_id"),
                name="uniq_gamification_contribution_entity",
            )
        ]
        indexes = [
            models.Index(fields=("user_id", "input_method", "day"), name="gam_contrib_method_day_idx"),
        ]


class AchievementProgress(models.Model):
    user_id = models.BigIntegerField()
    achievement_key = models.CharField(max_length=64)
    current_value = models.PositiveIntegerField(default=0)
    best_value = models.PositiveIntegerField(default=0)
    context = models.JSONField(default=dict, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "gamification_achievement_progress"
        constraints = [
            models.UniqueConstraint(
                fields=("user_id", "achievement_key"),
                name="uniq_gamification_progress_user_key",
            )
        ]


class GamificationCommandReceipt(models.Model):
    user_id = models.BigIntegerField()
    command = models.CharField(max_length=48)
    idempotency_key = models.CharField(max_length=128)
    request_hash = models.CharField(max_length=64)
    response_payload = models.JSONField(default=dict)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_command_receipts"
        constraints = [
            models.UniqueConstraint(
                fields=("user_id", "command", "idempotency_key"),
                name="gam_cmd_receipt_unique",
            )
        ]
        indexes = [models.Index(fields=("user_id", "-created_at"), name="gam_cmd_receipt_user_idx")]


class GamificationNotification(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.BigIntegerField()
    source_event_id = models.UUIDField(unique=True)
    grant_ids = models.JSONField(default=list)
    claim_token_hash = models.CharField(max_length=64, blank=True, default="")
    claimed_device_hash = models.CharField(max_length=64, blank=True, default="")
    claim_expires_at = models.DateTimeField(null=True, blank=True)
    acknowledged_at = models.DateTimeField(null=True, blank=True)
    seen_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_notifications"
        indexes = [
            models.Index(fields=("user_id", "acknowledged_at", "created_at"), name="gam_notice_user_ack_idx"),
        ]


class PinnedAchievement(models.Model):
    user_id = models.BigIntegerField()
    achievement_key = models.CharField(max_length=64)
    position = models.PositiveSmallIntegerField()
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "gamification_pinned_achievements"
        constraints = [
            models.UniqueConstraint(fields=("user_id", "achievement_key"), name="gam_pin_user_key_unique"),
            models.UniqueConstraint(fields=("user_id", "position"), name="gam_pin_user_position_unique"),
        ]
        indexes = [models.Index(fields=("user_id", "position"), name="gam_pin_user_position_idx")]


class AchievementGrant(models.Model):
    user_id = models.BigIntegerField()
    achievement_key = models.CharField(max_length=64)
    level_key = models.PositiveIntegerField(default=0)
    scope_key = models.CharField(max_length=128, blank=True, default="")
    trigger_event_id = models.UUIDField()
    granted_at = models.DateTimeField(default=timezone.now)
    context = models.JSONField(default=dict, blank=True)

    class Meta:
        db_table = "gamification_achievement_grants"
        constraints = [
            models.UniqueConstraint(
                fields=("user_id", "achievement_key", "level_key", "scope_key"),
                name="uniq_gamification_grant_subject",
            )
        ]
        indexes = [
            models.Index(fields=("user_id", "-granted_at"), name="gam_grant_user_time_idx"),
        ]
