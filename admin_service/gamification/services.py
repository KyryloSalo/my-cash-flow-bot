from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.db import transaction
from django.db.models import F
from django.utils import timezone
from transactions.models import Transaction

from .models import (
    AchievementGrant,
    AchievementProgress,
    ActivityContribution,
    GamificationCommandReceipt,
    GamificationDay,
    GamificationEventOutbox,
    GamificationNotification,
    GamificationProfile,
    GamificationTimezoneChange,
    PinnedAchievement,
    ProcessedGamificationEvent,
    ShieldAccrualState,
    ShieldLedger,
    StreakState,
)

_ACHIEVEMENT_INPUT_METHODS = {"manual", "text", "voice", "screenshot"}
_ALLOWED_INPUT_METHODS = {*_ACHIEVEMENT_INPUT_METHODS, "import"}
_TIMEZONE_CHANGE_GRACE = timedelta(minutes=5)
_MIN_GAMIFICATION_DAY = timedelta(hours=20)
_INPUT_ACHIEVEMENTS = {
    "text": "first-text",
    "voice": "first-voice",
    "screenshot": "first-screenshot",
}


class GamificationActionError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


@transaction.atomic
def claim_next_notification(
    *,
    user_id: int,
    device_id: str,
    now: datetime | None = None,
) -> dict[str, object] | None:
    current = now or timezone.now()
    device = str(device_id or "").strip()
    if not device or len(device) > 128:
        raise GamificationActionError("invalid_device", "Некоректний ідентифікатор пристрою.", status=422)
    device_hash = hashlib.sha256(device.encode("utf-8")).hexdigest()
    notice = (
        GamificationNotification.objects.select_for_update()
        .filter(user_id=user_id, acknowledged_at__isnull=True)
        .order_by("created_at", "id")
        .first()
    )
    if notice is None:
        return None
    if notice.claim_expires_at and notice.claim_expires_at > current:
        return None
    claim_token = secrets.token_urlsafe(32)
    notice.claim_token_hash = hashlib.sha256(claim_token.encode("utf-8")).hexdigest()
    notice.claimed_device_hash = device_hash
    notice.claim_expires_at = current + timedelta(minutes=2)
    notice.save(update_fields=("claim_token_hash", "claimed_device_hash", "claim_expires_at"))
    return {
        "notification_id": str(notice.id),
        "claim_token": claim_token,
        "grant_ids": list(notice.grant_ids),
        "claim_expires_at": notice.claim_expires_at.isoformat(),
    }


@transaction.atomic
def acknowledge_notification(
    *,
    user_id: int,
    notification_id: str,
    claim_token: str,
    device_id: str,
    now: datetime | None = None,
) -> None:
    current = now or timezone.now()
    try:
        notice_id = UUID(str(notification_id))
    except (TypeError, ValueError):
        raise GamificationActionError("invalid_claim", "Підтвердження недійсне.", status=409) from None
    notice = GamificationNotification.objects.select_for_update().filter(
        id=notice_id,
        user_id=user_id,
    ).first()
    if notice is None:
        raise GamificationActionError("invalid_claim", "Підтвердження недійсне.", status=409)
    token_hash = hashlib.sha256(str(claim_token or "").encode("utf-8")).hexdigest()
    device_hash = hashlib.sha256(str(device_id or "").encode("utf-8")).hexdigest()
    claim_valid = bool(
        notice.claim_token_hash
        and notice.claim_expires_at
        and notice.claim_expires_at >= current
        and secrets.compare_digest(notice.claim_token_hash, token_hash)
        and secrets.compare_digest(notice.claimed_device_hash, device_hash)
    )
    if not claim_valid:
        raise GamificationActionError("invalid_claim", "Підтвердження недійсне.", status=409)
    if notice.acknowledged_at is None:
        notice.acknowledged_at = current
        notice.seen_at = current
        notice.save(update_fields=("acknowledged_at", "seen_at"))


@transaction.atomic
def set_pinned_achievements(*, user_id: int, achievement_keys: list[str]) -> list[str]:
    keys = [str(key).strip() for key in achievement_keys if str(key).strip()]
    if len(keys) > 3 or len(set(keys)) != len(keys):
        raise GamificationActionError("invalid_pins", "Можна закріпити до трьох різних досягнень.", status=422)
    earned = set(
        AchievementGrant.objects.filter(user_id=user_id, achievement_key__in=keys).values_list(
            "achievement_key", flat=True
        )
    )
    if earned != set(keys):
        raise GamificationActionError("achievement_not_earned", "Спочатку відкрийте досягнення.", status=422)
    PinnedAchievement.objects.filter(user_id=user_id).delete()
    PinnedAchievement.objects.bulk_create(
        [
            PinnedAchievement(user_id=user_id, achievement_key=key, position=position)
            for position, key in enumerate(keys)
        ]
    )
    return keys


def _qualified_transaction(payload: dict[str, object]) -> bool:
    if payload.get("transaction_type") not in {"income", "expense"}:
        return False
    if payload.get("flow_kind", "normal") != "normal":
        return False
    try:
        return Decimal(str(payload.get("amount", "0"))) > 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def _profile_zone(profile: GamificationProfile) -> ZoneInfo:
    try:
        return ZoneInfo(profile.timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo("Europe/Kyiv")


def _base_day_bounds(local_date, zone: ZoneInfo) -> tuple[datetime, datetime]:
    utc = ZoneInfo("UTC")
    starts_at = datetime.combine(local_date, time.min, tzinfo=zone).astimezone(utc)
    ends_at = datetime.combine(local_date + timedelta(days=1), time.min, tzinfo=zone).astimezone(utc)
    return starts_at, ends_at


def _day_bounds_for_profile(
    profile: GamificationProfile,
    *,
    local_date,
    zone: ZoneInfo,
) -> tuple[datetime, datetime]:
    starts_at, ends_at = _base_day_bounds(local_date, zone)
    previous_day = (
        GamificationDay.objects.filter(
            user_id=profile.user_id,
            day_seq__lt=local_date.toordinal(),
        )
        .order_by("-ends_at", "-day_seq")
        .first()
    )
    if previous_day is not None:
        starts_at = max(starts_at, previous_day.ends_at)
    transition = (
        GamificationTimezoneChange.objects.filter(
            user_id=profile.user_id,
            new_timezone_name=zone.key,
            applied_at__isnull=False,
        )
        .order_by("-effective_at")
        .first()
    )
    if transition is not None and transition.effective_at.astimezone(zone).date() == local_date:
        starts_at = max(starts_at, transition.effective_at)
    return starts_at, ends_at


def _day_for_event(profile: GamificationProfile, accepted_at: datetime) -> GamificationDay:
    existing_day = (
        GamificationDay.objects.filter(
            user_id=profile.user_id,
            starts_at__lte=accepted_at,
            ends_at__gt=accepted_at,
        )
        .order_by("-starts_at")
        .first()
    )
    if existing_day is not None:
        return existing_day
    if (
        profile.pending_timezone_effective_at is not None
        and profile.pending_timezone_effective_at - _TIMEZONE_CHANGE_GRACE <= accepted_at
        and accepted_at < profile.pending_timezone_effective_at
    ):
        grace_day = (
            GamificationDay.objects.filter(
                user_id=profile.user_id,
                ends_at=profile.pending_timezone_effective_at - _TIMEZONE_CHANGE_GRACE,
            )
            .order_by("-starts_at")
            .first()
        )
        if grace_day is not None:
            return grace_day
    zone = _profile_zone(profile)
    local_date = accepted_at.astimezone(zone).date()
    starts_at, ends_at = _day_bounds_for_profile(profile, local_date=local_date, zone=zone)
    day, _created = GamificationDay.objects.get_or_create(
        user_id=profile.user_id,
        day_seq=local_date.toordinal(),
        defaults={
            "local_date": local_date,
            "timezone_name": zone.key,
            "starts_at": starts_at,
            "ends_at": ends_at,
        },
    )
    return day


@transaction.atomic
def finalize_user_days(user_id: int, *, now: datetime | None = None) -> int:
    profile = GamificationProfile.objects.select_for_update().get(user_id=user_id)
    zone = _profile_zone(profile)
    current_time = now or timezone.now()
    cutoff = current_time - timedelta(minutes=5)
    first_date = profile.started_at.astimezone(zone).date()
    latest_finalized = (
        GamificationDay.objects.filter(user_id=user_id, finalized_at__isnull=False)
        .order_by("-local_date")
        .first()
    )
    if latest_finalized is not None:
        first_date = max(
            first_date,
            latest_finalized.local_date + timedelta(days=1),
            latest_finalized.ends_at.astimezone(zone).date(),
        )
    last_candidate_date = cutoff.astimezone(zone).date()
    streak, _created = StreakState.objects.select_for_update().get_or_create(user_id=user_id)
    finalized_count = 0

    local_date = first_date
    while local_date <= last_candidate_date:
        starts_at, ends_at = _day_bounds_for_profile(profile, local_date=local_date, zone=zone)
        day = GamificationDay.objects.select_for_update().filter(
            user_id=user_id,
            day_seq=local_date.toordinal(),
        ).first()
        if day is None:
            if ends_at > cutoff:
                break
            day = GamificationDay.objects.create(
                user_id=user_id,
                day_seq=local_date.toordinal(),
                local_date=local_date,
                timezone_name=zone.key,
                starts_at=starts_at,
                ends_at=ends_at,
            )
        if day.ends_at > cutoff:
            break
        if day.finalized_at is not None:
            local_date += timedelta(days=1)
            continue

        if day.status == GamificationDay.Status.PENDING:
            if streak.current_streak > 0 and streak.shield_balance > 0:
                streak.shield_balance -= 1
                day.status = GamificationDay.Status.FROZEN
                ShieldLedger.objects.get_or_create(
                    user_id=user_id,
                    entitlement_key=f"spent-day-{day.day_seq}",
                    defaults={
                        "delta": -1,
                        "balance_after": streak.shield_balance,
                        "reason": "auto_spend",
                        "day": day,
                    },
                )
            else:
                day.status = GamificationDay.Status.MISSED
                if streak.current_streak > 0:
                    streak.current_streak = 0
                    streak.had_broken_streak = True
                    streak.comeback_run = 0
                if streak.had_broken_streak:
                    streak.inactive_gap += 1
            streak.save(
                update_fields=(
                    "current_streak",
                    "shield_balance",
                    "had_broken_streak",
                    "inactive_gap",
                    "comeback_run",
                    "updated_at",
                )
            )

        day.finalized_at = current_time
        day.save(update_fields=("status", "finalized_at"))
        finalized_count += 1
        local_date += timedelta(days=1)

    # Finalize the last day in the old timezone before applying a scheduled
    # timezone transition. Both operations stay under the same profile lock.
    _apply_due_timezone(profile, current=current_time)
    return finalized_count


def _grant(
    *,
    user_id: int,
    achievement_key: str,
    event_id: UUID,
    level_key: int = 0,
    scope_key: str = "",
    context: dict[str, object] | None = None,
) -> bool:
    _grant_row, created = AchievementGrant.objects.get_or_create(
        user_id=user_id,
        achievement_key=achievement_key,
        level_key=level_key,
        scope_key=scope_key,
        defaults={
            "trigger_event_id": event_id,
            "context": context or {},
        },
    )
    return created


def _activate_day(day: GamificationDay, accepted_at: datetime) -> StreakState:
    streak, _created = StreakState.objects.select_for_update().get_or_create(user_id=day.user_id)
    if day.status == GamificationDay.Status.ACTIVE:
        return streak

    previous_day = (
        GamificationDay.objects.filter(user_id=day.user_id, ends_at__lte=day.starts_at)
        .exclude(pk=day.pk)
        .order_by("-ends_at", "-day_seq")
        .first()
    )
    previous_kept_streak = bool(
        previous_day
        and previous_day.status in {GamificationDay.Status.ACTIVE, GamificationDay.Status.FROZEN}
        and streak.current_streak > 0
    )
    if previous_kept_streak:
        streak.current_streak += 1
    else:
        streak.current_streak = 1
    if streak.had_broken_streak and streak.inactive_gap >= 3:
        streak.comeback_run = 1
    elif streak.comeback_run > 0 and previous_day and previous_day.status == GamificationDay.Status.ACTIVE:
        streak.comeback_run += 1
    else:
        streak.comeback_run = 0
    streak.best_streak = max(streak.best_streak, streak.current_streak)
    streak.lifetime_active_days += 1
    streak.last_active_date = day.local_date
    streak.inactive_gap = 0
    streak.save(
        update_fields=(
            "current_streak",
            "best_streak",
            "lifetime_active_days",
            "last_active_date",
            "inactive_gap",
            "comeback_run",
            "updated_at",
        )
    )
    day.status = GamificationDay.Status.ACTIVE
    day.activated_at = accepted_at
    day.save(update_fields=("status", "activated_at"))
    return streak


def _issue_shield(
    *,
    streak: StreakState,
    day: GamificationDay,
    entitlement_key: str,
    event_id: UUID,
) -> bool:
    if ShieldLedger.objects.filter(user_id=streak.user_id, entitlement_key=entitlement_key).exists():
        return False
    delta = 1 if streak.shield_balance < 2 else 0
    reason = "accrued" if delta else "cap_reached"
    if delta:
        streak.shield_balance += 1
        streak.save(update_fields=("shield_balance", "updated_at"))
    ShieldLedger.objects.create(
        user_id=streak.user_id,
        entitlement_key=entitlement_key,
        delta=delta,
        balance_after=streak.shield_balance,
        reason=reason,
        day=day,
        trigger_event_id=event_id,
    )
    return bool(delta)


def _accrue_shields(streak: StreakState, day: GamificationDay, event_id: UUID) -> None:
    accrual, _created = ShieldAccrualState.objects.select_for_update().get_or_create(user_id=streak.user_id)
    changed_fields: list[str] = []
    if accrual.anchor_lifetime_active_days is None and streak.current_streak >= 3:
        accrual.anchor_lifetime_active_days = streak.lifetime_active_days
        changed_fields.append("anchor_lifetime_active_days")
        _issue_shield(streak=streak, day=day, entitlement_key="first-streak-3", event_id=event_id)

    anchor = accrual.anchor_lifetime_active_days
    if anchor is not None:
        completed_cycles = max(0, (streak.lifetime_active_days - anchor) // 7)
        while accrual.last_cycle < completed_cycles:
            accrual.last_cycle += 1
            _issue_shield(
                streak=streak,
                day=day,
                entitlement_key=f"active-cycle-{accrual.last_cycle}",
                event_id=event_id,
            )
        changed_fields.append("last_cycle")

    if changed_fields:
        accrual.save(update_fields=tuple(dict.fromkeys((*changed_fields, "updated_at"))))


def _award_streak_levels(streak: StreakState, event_id: UUID) -> int:
    grants = 0
    for threshold in (3, 7, 14, 30, 60, 100, 180, 365):
        if streak.current_streak < threshold:
            break
        grants += int(
            _grant(
                user_id=streak.user_id,
                achievement_key="streak-3",
                level_key=threshold,
                event_id=event_id,
                context={"threshold": threshold},
            )
        )
    return grants


def _store_progress(
    *,
    user_id: int,
    achievement_key: str,
    value: int,
    context: dict[str, object] | None = None,
) -> AchievementProgress:
    progress, _created = AchievementProgress.objects.get_or_create(
        user_id=user_id,
        achievement_key=achievement_key,
    )
    progress.current_value = max(0, int(value))
    progress.best_value = max(progress.best_value, progress.current_value)
    if context is not None:
        progress.context = context
    progress.save(update_fields=("current_value", "best_value", "context", "updated_at"))
    return progress


def _award_thresholds(
    *,
    user_id: int,
    achievement_key: str,
    value: int,
    thresholds: tuple[int, ...],
    event_id: UUID,
) -> int:
    grants = 0
    for threshold in thresholds:
        if value < threshold:
            break
        grants += int(
            _grant(
                user_id=user_id,
                achievement_key=achievement_key,
                level_key=threshold,
                event_id=event_id,
                context={"threshold": threshold},
            )
        )
    return grants


def _evaluate_transaction_progress(
    *,
    user_id: int,
    streak: StreakState,
    event_id: UUID,
) -> int:
    grants = 0
    _store_progress(
        user_id=user_id,
        achievement_key="streak-3",
        value=streak.current_streak,
        context={"best": streak.best_streak},
    )
    _store_progress(user_id=user_id, achievement_key="lifetime-days", value=streak.lifetime_active_days)
    grants += _award_thresholds(
        user_id=user_id,
        achievement_key="lifetime-days",
        value=streak.lifetime_active_days,
        thresholds=(30, 100, 365),
        event_id=event_id,
    )

    for method, achievement_key in (("voice", "voice-days"), ("screenshot", "screenshot-days")):
        method_days = (
            ActivityContribution.objects.filter(user_id=user_id, input_method=method)
            .values("day_id")
            .distinct()
            .count()
        )
        _store_progress(user_id=user_id, achievement_key=achievement_key, value=method_days)
        grants += _award_thresholds(
            user_id=user_id,
            achievement_key=achievement_key,
            value=method_days,
            thresholds=(5, 20, 50),
            event_id=event_id,
        )

    methods = sorted(
        set(
            ActivityContribution.objects.filter(
                user_id=user_id,
                input_method__in=_ACHIEVEMENT_INPUT_METHODS,
            ).values_list("input_method", flat=True)
        )
    )
    _store_progress(
        user_id=user_id,
        achievement_key="all-input-methods",
        value=len(methods),
        context={"methods": methods},
    )
    if set(methods) >= _ACHIEVEMENT_INPUT_METHODS:
        grants += int(
            _grant(
                user_id=user_id,
                achievement_key="all-input-methods",
                event_id=event_id,
            )
        )

    friday_count = sum(
        1
        for local_date in GamificationDay.objects.filter(
            user_id=user_id,
            status=GamificationDay.Status.ACTIVE,
        ).values_list("local_date", flat=True)
        if local_date.weekday() == 4
    )
    _store_progress(user_id=user_id, achievement_key="seven-fridays", value=friday_count)
    if friday_count >= 7:
        grants += int(
            _grant(
                user_id=user_id,
                achievement_key="seven-fridays",
                event_id=event_id,
            )
        )
    _store_progress(user_id=user_id, achievement_key="comeback", value=streak.comeback_run)
    if streak.comeback_run >= 3:
        grants += int(
            _grant(
                user_id=user_id,
                achievement_key="comeback",
                event_id=event_id,
            )
        )
    return grants


def _apply_due_timezone(profile: GamificationProfile, *, current: datetime) -> GamificationProfile:
    if (
        not profile.pending_timezone_name
        or profile.pending_timezone_effective_at is None
        or profile.pending_timezone_effective_at > current
    ):
        return profile
    pending_name = profile.pending_timezone_name
    effective_at = profile.pending_timezone_effective_at
    profile.timezone_name = pending_name
    profile.pending_timezone_name = ""
    profile.pending_timezone_effective_at = None
    profile.revision += 1
    profile.save(
        update_fields=(
            "timezone_name",
            "pending_timezone_name",
            "pending_timezone_effective_at",
            "revision",
            "updated_at",
        )
    )
    GamificationTimezoneChange.objects.filter(
        user_id=profile.user_id,
        new_timezone_name=pending_name,
        effective_at=effective_at,
        applied_at__isnull=True,
    ).update(applied_at=current)
    return profile


@transaction.atomic
def update_preferences(
    *,
    user_id: int,
    mascot: str | None,
    motion_enabled: bool | None,
    timezone_name: str | None,
    now: datetime | None = None,
) -> GamificationProfile:
    current = now or timezone.now()
    GamificationProfile.objects.get_or_create(user_id=user_id, defaults={"started_at": current})
    profile = GamificationProfile.objects.select_for_update().get(user_id=user_id)
    changed_fields: list[str] = []
    if mascot is not None:
        if mascot not in {GamificationProfile.Mascot.BOB, GamificationProfile.Mascot.CAPI}:
            raise GamificationActionError("invalid_mascot", "Оберіть Боба або Капі.", status=422)
        if profile.mascot != mascot:
            profile.mascot = mascot
            profile.mascot_selected_at = current
            changed_fields.extend(("mascot", "mascot_selected_at"))
    if motion_enabled is not None:
        if not isinstance(motion_enabled, bool):
            raise GamificationActionError("invalid_motion", "Некоректне значення анімацій.", status=422)
        if profile.motion_enabled != motion_enabled:
            profile.motion_enabled = motion_enabled
            changed_fields.append("motion_enabled")
    if timezone_name is not None and timezone_name != profile.timezone_name:
        if profile.timezone_changed_at and current < profile.timezone_changed_at + timedelta(days=30):
            raise GamificationActionError(
                "timezone_rate_limited",
                "Часовий пояс можна змінювати раз на 30 днів.",
                status=429,
            )
        try:
            ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            raise GamificationActionError("invalid_timezone", "Вкажіть часовий пояс IANA.", status=422) from None
        target_zone = ZoneInfo(timezone_name)
        current_day = _day_for_event(profile, current)
        target_date = max(
            current_day.local_date + timedelta(days=1),
            current.astimezone(target_zone).date(),
        )
        while True:
            boundary, _next_boundary = _base_day_bounds(target_date, target_zone)
            if (
                boundary >= current_day.ends_at
                and boundary > current
                and boundary - current_day.starts_at >= _MIN_GAMIFICATION_DAY
            ):
                break
            target_date += timedelta(days=1)
        if current_day.ends_at != boundary:
            current_day.ends_at = boundary
            current_day.save(update_fields=("ends_at",))
        effective_at = boundary + _TIMEZONE_CHANGE_GRACE
        profile.pending_timezone_name = timezone_name
        profile.pending_timezone_effective_at = effective_at
        profile.timezone_changed_at = current
        changed_fields.extend(
            (
                "pending_timezone_name",
                "pending_timezone_effective_at",
                "timezone_changed_at",
            )
        )
        GamificationTimezoneChange.objects.create(
            user_id=user_id,
            previous_timezone_name=profile.timezone_name,
            new_timezone_name=timezone_name,
            requested_at=current,
            effective_at=effective_at,
        )
    if changed_fields:
        profile.revision += 1
        changed_fields.extend(("revision", "updated_at"))
        profile.save(update_fields=tuple(changed_fields))
    return profile


@transaction.atomic
def current_day_snapshot(*, user_id: int, now: datetime | None = None) -> dict[str, object]:
    current = now or timezone.now()
    GamificationProfile.objects.get_or_create(
        user_id=user_id,
        defaults={"started_at": current},
    )
    profile = GamificationProfile.objects.select_for_update().get(user_id=user_id)
    finalize_user_days(user_id, now=current)
    profile.refresh_from_db()
    day = _day_for_event(profile, current)
    streak, _created = StreakState.objects.get_or_create(user_id=user_id)
    return {
        "day_id": day.day_seq,
        "local_date": day.local_date.isoformat(),
        "status": day.status,
        "current_streak": streak.current_streak,
        "best_streak": streak.best_streak,
        "lifetime_active_days": streak.lifetime_active_days,
        "shield_balance": streak.shield_balance,
    }


@transaction.atomic
def record_no_expenses(
    *,
    user_id: int,
    day_id: str,
    idempotency_key: str,
    now: datetime | None = None,
) -> dict[str, object]:
    current = now or timezone.now()
    key = str(idempotency_key or "").strip()
    if len(key) < 8 or len(key) > 128:
        raise GamificationActionError("invalid_idempotency_key", "Некоректний ключ повтору.")

    profile, _created = GamificationProfile.objects.get_or_create(
        user_id=user_id,
        defaults={"started_at": current},
    )
    profile = GamificationProfile.objects.select_for_update().get(user_id=user_id)
    request_hash = hashlib.sha256(
        json.dumps({"day_id": str(day_id)}, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    receipt = GamificationCommandReceipt.objects.filter(
        user_id=user_id,
        command="no_expenses",
        idempotency_key=key,
    ).first()
    if receipt is not None:
        if receipt.request_hash != request_hash:
            raise GamificationActionError(
                "idempotency_conflict",
                "Цей ключ уже використано для іншого запиту.",
                status=409,
            )
        return {**receipt.response_payload, "idempotent": True}

    finalize_user_days(user_id, now=current)
    profile.refresh_from_db()
    day = _day_for_event(profile, current)
    if str(day.day_seq) != str(day_id):
        raise GamificationActionError("day_mismatch", "День уже змінився. Оновіть екран.", status=422)
    if Transaction.all_objects.filter(
        created_by_user_id=user_id,
        date=day.local_date,
        type="expense",
        flow_kind="normal",
        deleted_at__isnull=True,
    ).exists():
        raise GamificationActionError(
            "expense_exists",
            "Сьогодні вже є підтверджена витрата.",
            status=409,
        )

    event_id = uuid5(NAMESPACE_URL, f"vydno:no-expenses:{user_id}:{day.day_seq}")
    _contribution, contribution_created = ActivityContribution.objects.get_or_create(
        user_id=user_id,
        kind="no_expenses",
        entity_type="day",
        entity_id=str(day.day_seq),
        defaults={
            "day": day,
            "input_method": "",
            "accepted_at": current,
        },
    )
    if contribution_created:
        day_was_active = day.status == GamificationDay.Status.ACTIVE
        streak = _activate_day(day, current)
        if not day_was_active:
            _award_streak_levels(streak=streak, event_id=event_id)
            _accrue_shields(streak=streak, day=day, event_id=event_id)
        _evaluate_transaction_progress(
            user_id=user_id,
            streak=streak,
            event_id=event_id,
        )

    snapshot = current_day_snapshot(user_id=user_id, now=current)
    response_payload = {**snapshot, "ok": True, "idempotent": False}
    GamificationCommandReceipt.objects.create(
        user_id=user_id,
        command="no_expenses",
        idempotency_key=key,
        request_hash=request_hash,
        response_payload=response_payload,
    )
    return response_payload


@transaction.atomic
def process_event(event_id: UUID) -> dict[str, object]:
    if ProcessedGamificationEvent.objects.filter(event_id=event_id).exists():
        return {"processed": False, "grants": 0}

    event = GamificationEventOutbox.objects.select_for_update().get(event_id=event_id)
    if event.status in {
        GamificationEventOutbox.Status.PROCESSED,
        GamificationEventOutbox.Status.IGNORED,
    }:
        return {"processed": False, "grants": 0}

    event.status = GamificationEventOutbox.Status.PROCESSING
    event.attempts += 1
    event.save(update_fields=("status", "attempts"))

    if event.event_type != "transaction.created" or not _qualified_transaction(event.payload):
        ProcessedGamificationEvent.objects.create(event_id=event.event_id, actor_user_id=event.actor_user_id)
        event.status = GamificationEventOutbox.Status.IGNORED
        event.processed_at = timezone.now()
        event.save(update_fields=("status", "processed_at"))
        return {"processed": True, "grants": 0}

    profile, _created = GamificationProfile.objects.select_for_update().get_or_create(
        user_id=event.actor_user_id,
        defaults={"started_at": event.accepted_at},
    )
    finalize_user_days(event.actor_user_id, now=event.accepted_at)
    profile.refresh_from_db()
    day = _day_for_event(profile, event.accepted_at)
    input_method = event.input_method if event.input_method in _ALLOWED_INPUT_METHODS else ""
    _contribution, contribution_created = ActivityContribution.objects.get_or_create(
        user_id=event.actor_user_id,
        kind="transaction",
        entity_type=event.entity_type,
        entity_id=event.entity_id,
        defaults={
            "day": day,
            "input_method": input_method,
            "accepted_at": event.accepted_at,
        },
    )

    grants = 0
    if contribution_created:
        day_was_active = day.status == GamificationDay.Status.ACTIVE
        streak = _activate_day(day, event.accepted_at)
        grants += int(
            _grant(
                user_id=event.actor_user_id,
                achievement_key="first-record",
                event_id=event.event_id,
            )
        )
        method_achievement = _INPUT_ACHIEVEMENTS.get(input_method)
        if method_achievement:
            grants += int(
                _grant(
                    user_id=event.actor_user_id,
                    achievement_key=method_achievement,
                    event_id=event.event_id,
                )
            )
        if not day_was_active:
            grants += _award_streak_levels(streak, event.event_id)
            _accrue_shields(streak, day, event.event_id)
        grants += _evaluate_transaction_progress(
            user_id=event.actor_user_id,
            streak=streak,
            event_id=event.event_id,
        )
    if grants:
        grant_ids = list(
            AchievementGrant.objects.filter(trigger_event_id=event.event_id)
            .order_by("granted_at", "id")
            .values_list("id", flat=True)
        )
        GamificationNotification.objects.get_or_create(
            source_event_id=event.event_id,
            defaults={
                "user_id": event.actor_user_id,
                "grant_ids": grant_ids,
            },
        )

    ProcessedGamificationEvent.objects.create(event_id=event.event_id, actor_user_id=event.actor_user_id)
    event.status = GamificationEventOutbox.Status.PROCESSED
    event.processed_at = timezone.now()
    event.last_error_code = ""
    event.save(update_fields=("status", "processed_at", "last_error_code"))
    return {"processed": True, "grants": grants}


def finalize_due_profiles(*, now: datetime | None = None, limit: int = 500) -> dict[str, int]:
    current = now or timezone.now()
    batch_limit = max(1, min(int(limit), 2000))
    user_ids = list(
        GamificationProfile.objects.order_by("updated_at", "user_id").values_list("user_id", flat=True)[:batch_limit]
    )
    result = {"profiles": 0, "days": 0, "failed": 0}
    for user_id in user_ids:
        try:
            result["days"] += finalize_user_days(user_id, now=current)
        except Exception:  # pragma: no cover - isolated profile failure is reported by the task
            result["failed"] += 1
        else:
            result["profiles"] += 1
        finally:
            GamificationProfile.objects.filter(user_id=user_id).update(updated_at=current)
    return result


def process_pending_events(*, limit: int = 100) -> dict[str, int]:
    batch_limit = max(1, min(int(limit), 500))
    event_ids = list(
        GamificationEventOutbox.objects.filter(
            status__in=(GamificationEventOutbox.Status.PENDING, GamificationEventOutbox.Status.FAILED),
            available_at__lte=timezone.now(),
            attempts__lt=5,
        )
        .order_by("accepted_at", "created_at")
        .values_list("event_id", flat=True)[:batch_limit]
    )
    result = {"checked": len(event_ids), "processed": 0, "failed": 0}
    for event_id in event_ids:
        try:
            outcome = process_event(event_id)
        except Exception as exc:  # pragma: no cover - exercised by operational retry tests
            result["failed"] += 1
            GamificationEventOutbox.objects.filter(event_id=event_id).update(
                status=GamificationEventOutbox.Status.FAILED,
                attempts=F("attempts") + 1,
                available_at=timezone.now() + timedelta(seconds=30),
                last_error_code=type(exc).__name__[:64],
            )
            continue
        if outcome["processed"]:
            result["processed"] += 1
    return result
