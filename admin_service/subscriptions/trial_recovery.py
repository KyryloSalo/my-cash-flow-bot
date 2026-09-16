from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time, timedelta
import logging
import json
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import Case, When, Value, Exists, F, OuterRef, Q
from django.utils import timezone

from common.telegram import TelegramSendError, send_telegram_message
from subscriptions.models import (
    BillingProfile,
    Payment,
    PromoOffer,
    Subscription,
    TrialRecoveryCampaign,
    TrialRecoveryDelivery,
    TrialRecoveryRecipient,
)
from users.models import TelegramUser, UserAdminState


logger = logging.getLogger(__name__)

FIRST_MESSAGE_DELAY = timedelta(hours=6)
SECOND_MESSAGE_DELAY = timedelta(hours=48)
FINAL_MESSAGE_DELAY = timedelta(days=7)
MAX_FRESH_DELIVERY_LAG = timedelta(minutes=20)
BACKLOG_MIN_DELAY = timedelta(minutes=30)
DEFAULT_TIMEZONE = "Europe/Kyiv"
ACTIVE_SUBSCRIPTION_STATUSES = {
    Subscription.Status.TRIAL,
    Subscription.Status.ACTIVE,
    Subscription.Status.PAID,
    Subscription.Status.MANUAL,
    Subscription.Status.LIFETIME,
}
PENDING_BIND_STATUSES = {Payment.Status.CREATED, Payment.Status.PENDING}
SUCCESS_PAYMENT_STATUSES = {Payment.Status.PAID, Payment.Status.MANUAL_CONFIRMED}
REGISTERED_WITHOUT_CARD_AUDIENCE = "registered_without_card"

REASON_LABELS = {
    "uk": {
        TrialRecoveryRecipient.Reason.CARD: "Не хочу прив’язувати картку",
        TrialRecoveryRecipient.Reason.AUTORENEW: "Боюся автосписання",
        TrialRecoveryRecipient.Reason.VALUE: "Не бачу користі",
        TrialRecoveryRecipient.Reason.PRICE: "499 грн потім дорого",
        TrialRecoveryRecipient.Reason.ERROR: "Сталася помилка",
        TrialRecoveryRecipient.Reason.LATER: "Просто не на часі",
        TrialRecoveryRecipient.Reason.OTHER: "Інше",
    },
    "en": {
        TrialRecoveryRecipient.Reason.CARD: "I don’t want to link a card",
        TrialRecoveryRecipient.Reason.AUTORENEW: "I’m worried about auto-renewal",
        TrialRecoveryRecipient.Reason.VALUE: "I don’t see the value",
        TrialRecoveryRecipient.Reason.PRICE: "UAH 499 later is too much",
        TrialRecoveryRecipient.Reason.ERROR: "Something went wrong",
        TrialRecoveryRecipient.Reason.LATER: "It’s not the right time",
        TrialRecoveryRecipient.Reason.OTHER: "Other",
    },
}


def normalize_lang(value: object) -> str:
    return "en" if str(value or "").strip().lower().startswith("en") else "uk"


def safe_zone(name: object, fallback: str = DEFAULT_TIMEZONE) -> tuple[ZoneInfo, str]:
    normalized = str(name or "").strip()
    for candidate in (normalized, fallback, DEFAULT_TIMEZONE, "UTC"):
        if not candidate:
            continue
        try:
            return ZoneInfo(candidate), candidate
        except (ZoneInfoNotFoundError, ValueError):
            continue
    return ZoneInfo("UTC"), "UTC"


def _aware_local(day: date, value: time, zone: ZoneInfo) -> datetime:
    return datetime.combine(day, value, tzinfo=zone)


def coerce_to_send_window(
    moment: datetime,
    *,
    timezone_name: str,
    window_start: time,
    window_end: time,
) -> datetime:
    zone, _ = safe_zone(timezone_name)
    local = moment.astimezone(zone)
    start = _aware_local(local.date(), window_start, zone)
    end = _aware_local(local.date(), window_end, zone)
    if local < start:
        local = start
    elif local >= end:
        local = _aware_local(local.date() + timedelta(days=1), window_start, zone)
    return local.astimezone(ZoneInfo("UTC"))


def _stable_window_slot(
    moment: datetime,
    *,
    timezone_name: str,
    window_start: time,
    window_end: time,
    seed: int,
    force_next_day: bool = False,
) -> datetime:
    zone, _ = safe_zone(timezone_name)
    local = moment.astimezone(zone)
    offset_minutes = abs(int(seed)) % 180
    if force_next_day or local.time() >= window_end:
        target_day = local.date() + timedelta(days=1)
        target = _aware_local(target_day, window_start, zone) + timedelta(minutes=offset_minutes)
        return target.astimezone(ZoneInfo("UTC"))
    if local.time() < window_start:
        target = _aware_local(local.date(), window_start, zone) + timedelta(minutes=offset_minutes)
        return target.astimezone(ZoneInfo("UTC"))
    target = local + BACKLOG_MIN_DELAY + timedelta(minutes=offset_minutes)
    end = _aware_local(local.date(), window_end, zone)
    if target >= end:
        target = _aware_local(local.date() + timedelta(days=1), window_start, zone) + timedelta(minutes=offset_minutes)
    return target.astimezone(ZoneInfo("UTC"))


def _recipient_timezone(user: TelegramUser, fallback: str) -> str:
    state = getattr(user, "admin_state", None)
    _zone, normalized = safe_zone(getattr(state, "timezone", ""), fallback=fallback)
    return normalized


def _promo_payloads_for_90_days() -> set[str]:
    return {
        f"PROMO_{str(code or '').strip().upper()}"
        for code in PromoOffer.objects.filter(trial_days=90, is_active=True).values_list("code", flat=True)
        if str(code or "").strip()
    }


def _is_90_day_payload(payload: object, promo_payloads: set[str]) -> bool:
    normalized = str(payload or "").strip()
    lowered = normalized.lower()
    return lowered == "course" or lowered.startswith("course_") or normalized.upper() in promo_payloads


def _registered_user_queryset():
    successful_payment = Payment.objects.filter(
        user_id=OuterRef("tg_user_id"),
        status__in=SUCCESS_PAYMENT_STATUSES,
    )
    pending_bind = Payment.objects.filter(
        user_id=OuterRef("tg_user_id"),
        kind=Payment.Kind.BIND,
        status__in=PENDING_BIND_STATUSES,
    )
    return (
        TelegramUser.objects.all()
        .select_related("admin_state", "billing_profile")
        .annotate(has_successful_payment=Exists(successful_payment), has_pending_bind=Exists(pending_bind))
        .order_by("tg_user_id")
    )


def _ineligible_reason(
    user: TelegramUser,
    *,
    include_existing_recipient: bool = True,
) -> str:
    state = getattr(user, "admin_state", None)
    if user.tg_user_id in set(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", [])):
        return "test_user"
    if state is not None:
        if state.status == UserAdminState.Status.BANNED or state.is_blocked:
            return "blocked"
        if not state.can_receive_messages or state.blocked_bot:
            return "cannot_receive_messages"
        if state.is_test_user:
            return "test_user"
    has_successful_payment = getattr(user, "has_successful_payment", None)
    if has_successful_payment is None:
        has_successful_payment = Payment.objects.filter(
            user_id=user.tg_user_id,
            status__in=SUCCESS_PAYMENT_STATUSES,
        ).exists()
    if has_successful_payment:
        return "has_successful_payment"
    # Use the checkout trial gate, including manual and expired history.
    from subscriptions.billing import _bind_trial_granted
    if not _bind_trial_granted(user=user, mode="bind"):
        return "trial_not_available"
    try:
        profile = user.billing_profile
    except BillingProfile.DoesNotExist:
        profile = None
    if profile is not None and bool(profile.card_token):
        return "has_card"
    has_pending = getattr(user, "has_pending_bind", None)
    if has_pending is None:
        has_pending = Payment.objects.filter(
            user_id=user.tg_user_id,
            kind=Payment.Kind.BIND,
            status__in=PENDING_BIND_STATUSES,
        ).exists()
    if has_pending:
        return "bind_in_progress"
    if include_existing_recipient and TrialRecoveryRecipient.objects.filter(user_id=user.tg_user_id).exists():
        return "already_enrolled"
    return ""


def _ensure_admin_state(user: TelegramUser) -> UserAdminState:
    """Repair the admin-side projection for an otherwise valid registered user."""

    state = getattr(user, "admin_state", None)
    if state is None:
        state, _created = UserAdminState.objects.get_or_create(telegram_user_id=user.tg_user_id)
        user.admin_state = state
    return state


def _legacy_offer_ineligible_reason(
    user: TelegramUser,
    *,
    include_existing_recipient: bool = True,
) -> str:
    """Preserve the original 90-day-offer gate until an active campaign is explicitly expanded."""

    promo_payloads = _promo_payloads_for_90_days()
    state = getattr(user, "admin_state", None)
    if state is None or not _is_90_day_payload(state.pending_start_payload, promo_payloads):
        return "not_90_day_offer"
    if not user.onboarding_completed:
        return "onboarding_incomplete"
    if state.access_scope != UserAdminState.AccessScope.PAYWALL:
        return "has_other_access"
    broad_reason = _ineligible_reason(user, include_existing_recipient=include_existing_recipient)
    if broad_reason:
        return broad_reason
    if Subscription.objects.filter(
        user_id=user.tg_user_id,
        status__in=ACTIVE_SUBSCRIPTION_STATUSES,
    ).exists():
        return "has_full_subscription"
    return ""


def audience_preview() -> dict[str, object]:
    counts: Counter[str] = Counter()
    eligible_ids: list[int] = []
    for user in _registered_user_queryset():
        reason = _ineligible_reason(user)
        if reason:
            counts[reason] += 1
        else:
            eligible_ids.append(int(user.tg_user_id))
    return {
        "audience_mode": REGISTERED_WITHOUT_CARD_AUDIENCE,
        "eligible": len(eligible_ids),
        "eligible_ids": eligible_ids,
        "excluded": dict(sorted(counts.items())),
        "total_registered_users": len(eligible_ids) + sum(counts.values()),
    }


def eligible_users() -> list[TelegramUser]:
    return [
        user
        for user in _registered_user_queryset()
        if not _ineligible_reason(user)
    ]


def campaign_uses_registered_without_card_audience(campaign: TrialRecoveryCampaign) -> bool:
    return (campaign.audience_snapshot or {}).get("audience_mode") == REGISTERED_WITHOUT_CARD_AUDIENCE


def default_launch_date(campaign: TrialRecoveryCampaign, now: datetime | None = None) -> date:
    now = now or timezone.now()
    zone, _ = safe_zone(campaign.fallback_timezone)
    return now.astimezone(zone).date() + timedelta(days=1)


def combine_campaign_launch(campaign: TrialRecoveryCampaign, launch_date: date, launch_time: time) -> datetime:
    zone, _ = safe_zone(campaign.fallback_timezone)
    return _aware_local(launch_date, launch_time, zone).astimezone(ZoneInfo("UTC"))


def _backfill_slots(
    campaign: TrialRecoveryCampaign,
    users: Iterable[TelegramUser],
    *,
    launch_at: datetime,
) -> list[tuple[TelegramUser, str, datetime]]:
    rows = list(users)
    total = len(rows)
    result: list[tuple[TelegramUser, str, datetime]] = []
    if total == 0:
        return result
    for index, user in enumerate(rows):
        timezone_name = _recipient_timezone(user, campaign.fallback_timezone)
        zone, _ = safe_zone(timezone_name, campaign.fallback_timezone)
        local_launch = launch_at.astimezone(zone)
        day_start = _aware_local(local_launch.date(), campaign.send_window_start, zone)
        day_end = _aware_local(local_launch.date(), campaign.send_window_end, zone)
        if local_launch >= day_end:
            day_start = _aware_local(local_launch.date() + timedelta(days=1), campaign.send_window_start, zone)
            day_end = _aware_local(local_launch.date() + timedelta(days=1), campaign.send_window_end, zone)
        lower_bound = max(local_launch, day_start)
        available = max((day_end - lower_bound).total_seconds(), 60.0)
        fraction = (index + 0.5) / total
        slot = lower_bound + timedelta(seconds=available * fraction)
        if slot >= day_end:
            slot = day_end - timedelta(minutes=1)
        result.append((user, timezone_name, slot.astimezone(ZoneInfo("UTC"))))
    return result


def launch_campaign(
    campaign: TrialRecoveryCampaign,
    *,
    launch_at: datetime,
    admin_user,
) -> dict[str, object]:
    preview = audience_preview()
    users_by_id = {int(user.tg_user_id): user for user in eligible_users()}
    users = [users_by_id[user_id] for user_id in preview["eligible_ids"] if user_id in users_by_id]
    for user in users:
        _ensure_admin_state(user)
    snapshot = {key: value for key, value in preview.items() if key != "eligible_ids"}
    now = timezone.now()
    snapshot["audience_enabled_at"] = now.isoformat()
    with transaction.atomic():
        locked = TrialRecoveryCampaign.objects.select_for_update().get(pk=campaign.pk)
        if locked.status != TrialRecoveryCampaign.Status.DRAFT:
            raise ValueError("Запустити можна лише recovery-кампанію зі статусом «Чернетка».")
        conflict = TrialRecoveryCampaign.objects.filter(
            status__in=[TrialRecoveryCampaign.Status.SCHEDULED, TrialRecoveryCampaign.Status.RUNNING],
        ).exclude(pk=locked.pk)
        if conflict.exists():
            raise ValueError("Інша recovery-кампанія вже запланована або працює.")
        if locked.send_window_start >= locked.send_window_end:
            raise ValueError("Час завершення вікна має бути пізніше за час початку.")
        if launch_at < now:
            launch_at = now
        locked.launch_at = launch_at
        locked.launched_by = admin_user
        locked.audience_snapshot = snapshot
        locked.completed_at = None
        locked.status = (
            TrialRecoveryCampaign.Status.RUNNING
            if launch_at <= now
            else TrialRecoveryCampaign.Status.SCHEDULED
        )
        locked.started_at = now if locked.status == TrialRecoveryCampaign.Status.RUNNING else None
        locked.save(
            update_fields=[
                "launch_at",
                "launched_by",
                "audience_snapshot",
                "completed_at",
                "status",
                "started_at",
                "updated_at",
            ]
        )
        recipients = [
            TrialRecoveryRecipient(
                campaign=locked,
                user=user,
                status=TrialRecoveryRecipient.Status.SCHEDULED,
                source=TrialRecoveryRecipient.Source.BACKFILL,
                timezone=timezone_name,
                offered_at=getattr(user.admin_state, "last_onboarding_event_at", None) or user.last_seen_at or user.created_at,
                next_send_at=slot,
            )
            for user, timezone_name, slot in _backfill_slots(locked, users, launch_at=launch_at)
        ]
        TrialRecoveryRecipient.objects.bulk_create(recipients, ignore_conflicts=True)
    campaign.refresh_from_db()
    return {**snapshot, "created_recipients": len(recipients), "launch_at": launch_at}


def expand_campaign_audience(
    campaign: TrialRecoveryCampaign,
    *,
    admin_user,
    now: datetime | None = None,
) -> dict[str, object]:
    """Explicitly expand a running legacy campaign to every safe registered user without a card/payment."""

    now = now or timezone.now()
    preview = audience_preview()
    users_by_id = {int(user.tg_user_id): user for user in eligible_users()}
    users = [users_by_id[user_id] for user_id in preview["eligible_ids"] if user_id in users_by_id]
    for user in users:
        _ensure_admin_state(user)
    snapshot = {key: value for key, value in preview.items() if key != "eligible_ids"}
    snapshot["audience_enabled_at"] = now.isoformat()

    with transaction.atomic():
        locked = TrialRecoveryCampaign.objects.select_for_update().get(pk=campaign.pk)
        if locked.status not in {
            TrialRecoveryCampaign.Status.SCHEDULED,
            TrialRecoveryCampaign.Status.RUNNING,
        }:
            raise ValueError("Розширити аудиторію можна лише для запланованої або активної recovery-кампанії.")
        if campaign_uses_registered_without_card_audience(locked):
            raise ValueError("Кампанія вже використовує всіх зареєстрованих користувачів без картки.")
        previous_snapshot = locked.audience_snapshot or {}
        snapshot["initial_audience_snapshot"] = previous_snapshot.get(
            "initial_audience_snapshot",
            previous_snapshot,
        )
        locked.audience_snapshot = snapshot
        locked.save(update_fields=["audience_snapshot", "updated_at"])

        created_recipients = 0
        for user, timezone_name, slot in _backfill_slots(locked, users, launch_at=now):
            _recipient, created = TrialRecoveryRecipient.objects.get_or_create(
                user=user,
                defaults={
                    "campaign": locked,
                    "status": TrialRecoveryRecipient.Status.SCHEDULED,
                    "source": TrialRecoveryRecipient.Source.BACKFILL,
                    "timezone": timezone_name,
                    "offered_at": user.created_at,
                    "next_send_at": slot,
                },
            )
            created_recipients += int(created)

    campaign.refresh_from_db()
    return {
        **snapshot,
        "created_recipients": created_recipients,
        "expanded_at": now,
        "expanded_by": getattr(admin_user, "pk", None),
    }


def sync_registered_without_card_audience(
    campaign: TrialRecoveryCampaign,
    *,
    now: datetime | None = None,
) -> int:
    """Enroll post-expansion registrations with a six-hour delay without flushing a backlog."""

    now = now or timezone.now()
    if campaign.status != TrialRecoveryCampaign.Status.RUNNING or not campaign_uses_registered_without_card_audience(campaign):
        return 0
    snapshot = campaign.audience_snapshot or {}
    raw_enabled_at = snapshot.get("audience_enabled_at")
    try:
        enabled_at = datetime.fromisoformat(str(raw_enabled_at)) if raw_enabled_at else campaign.started_at or campaign.launch_at
    except (TypeError, ValueError):
        enabled_at = campaign.started_at or campaign.launch_at
    enabled_at = enabled_at or now
    if timezone.is_naive(enabled_at):
        enabled_at = timezone.make_aware(enabled_at, ZoneInfo("UTC"))
    overlap_start = enabled_at - timedelta(minutes=5)

    created_recipients = 0
    for user in eligible_users():
        if user.created_at < overlap_start:
            continue
        _ensure_admin_state(user)
        timezone_name = _recipient_timezone(user, campaign.fallback_timezone)
        natural_due = user.created_at + FIRST_MESSAGE_DELAY
        if natural_due <= now:
            due = _stable_window_slot(
                now,
                timezone_name=timezone_name,
                window_start=campaign.send_window_start,
                window_end=campaign.send_window_end,
                seed=user.tg_user_id,
            )
        else:
            due = coerce_to_send_window(
                natural_due,
                timezone_name=timezone_name,
                window_start=campaign.send_window_start,
                window_end=campaign.send_window_end,
            )
        _recipient, created = TrialRecoveryRecipient.objects.get_or_create(
            user=user,
            defaults={
                "campaign": campaign,
                "status": TrialRecoveryRecipient.Status.SCHEDULED,
                "source": TrialRecoveryRecipient.Source.LIVE,
                "timezone": timezone_name,
                "offered_at": user.created_at,
                "next_send_at": due,
            },
        )
        created_recipients += int(created)
    return created_recipients


def pause_campaign(campaign: TrialRecoveryCampaign) -> None:
    TrialRecoveryCampaign.objects.filter(
        pk=campaign.pk,
        status__in=[TrialRecoveryCampaign.Status.SCHEDULED, TrialRecoveryCampaign.Status.RUNNING],
    ).update(status=TrialRecoveryCampaign.Status.PAUSED, updated_at=timezone.now())


def record_trial_offer_event(user: TelegramUser, event: str, *, trial_days: int) -> TrialRecoveryRecipient | None:
    if event not in {"shown", "dismissed", "bind_started"}:
        return None
    campaign = (
        TrialRecoveryCampaign.objects.filter(
            status__in=[TrialRecoveryCampaign.Status.SCHEDULED, TrialRecoveryCampaign.Status.RUNNING]
        )
        .order_by("-created_at")
        .first()
    )
    if campaign is None:
        return None
    uses_registered_audience = campaign_uses_registered_without_card_audience(campaign)
    if not uses_registered_audience and int(trial_days or 0) != 90:
        return None
    existing = TrialRecoveryRecipient.objects.filter(user_id=user.tg_user_id).first()
    ineligible = (
        _ineligible_reason(user, include_existing_recipient=False)
        if uses_registered_audience
        else _legacy_offer_ineligible_reason(user, include_existing_recipient=False)
    )
    if existing is None and ineligible:
        return None
    now = timezone.now()
    if existing is None:
        _ensure_admin_state(user)
        timezone_name = _recipient_timezone(user, campaign.fallback_timezone)
        due = max(now + FIRST_MESSAGE_DELAY, campaign.launch_at or now)
        due = coerce_to_send_window(
            due,
            timezone_name=timezone_name,
            window_start=campaign.send_window_start,
            window_end=campaign.send_window_end,
        )
        try:
            existing = TrialRecoveryRecipient.objects.create(
                campaign=campaign,
                user=user,
                status=TrialRecoveryRecipient.Status.SCHEDULED,
                source=TrialRecoveryRecipient.Source.LIVE,
                timezone=timezone_name,
                offered_at=now,
                next_send_at=due,
            )
        except IntegrityError:
            existing = TrialRecoveryRecipient.objects.filter(user_id=user.tg_user_id).first()
    if existing is None:
        return None
    update_fields = ["updated_at"]
    if event == "shown" and existing.shown_at is None:
        existing.shown_at = now
        update_fields.append("shown_at")
    elif event == "dismissed":
        existing.dismissed_at = now
        update_fields.append("dismissed_at")
    existing.save(update_fields=update_fields)
    return existing


def _message_payload(recipient: TrialRecoveryRecipient, step: int) -> tuple[str, list[list[dict]]]:
    lang = normalize_lang(recipient.user.lang)
    onboarding_complete = bool(recipient.user.onboarding_completed)
    if lang == "en" and not onboarding_complete:
        texts = {
            1: (
                "Hi! I’m the founder of Vydno. I noticed that you registered but didn’t finish setup or activate "
                "90 days of access for UAH 1. I don’t want to guess or pressure you — what stopped you?"
            ),
            2: (
                "One more short message — no sales pitch. I’m building Vydno and want to fix whatever prevented you "
                "from finishing setup. One tap below would already help a lot. You can also reply in one message."
            ),
            3: (
                "This is the last message about getting started — I won’t bother you again. If you’d like, I can message "
                "you here personally and listen. If you simply postponed it, you can return to setup and activate "
                "90 days for UAH 1."
            ),
        }
    elif lang == "en":
        texts = {
            1: (
                "Hi! I’m the founder of Vydno. I noticed that your 90 days of full access for UAH 1 weren’t activated. "
                "I don’t want to guess or pressure you — I’d like to understand where we lost you. What stopped you?"
            ),
            2: (
                "One more short message — no sales pitch. I’m building Vydno and want to fix whatever gets in the way of starting. "
                "One tap below would already help a lot. If it’s easier, you can reply in one message."
            ),
            3: (
                "This is the last message about the trial — I won’t bother you again. If you’d like, I can message you here personally "
                "and listen to what didn’t work for you. If it was only a matter of timing, 90 days for UAH 1 are still available."
            ),
        }
    elif not onboarding_complete:
        texts = {
            1: (
                "Привіт! Я засновник Vydno. Бачу, що ви зареєструвалися, але не завершили налаштування й не активували "
                "90 днів доступу за 1 грн. Не хочу переконувати навмання — хочу зрозуміти, що зупинило?"
            ),
            2: (
                "Я ще раз коротко — без продажу. Я роблю Vydno і хочу виправити те, що завадило завершити налаштування. "
                "Один тап нижче вже дуже допоможе. Якщо зручніше — можна відповісти одним повідомленням."
            ),
            3: (
                "Це останнє повідомлення про початок роботи — далі не турбуватиму. Якщо хочете, я особисто напишу тут "
                "і вислухаю, що завадило. А якщо ви просто відклали — можна повернутися до налаштування й активувати "
                "90 днів за 1 грн."
            ),
        }
    else:
        texts = {
            1: (
                "Привіт! Я засновник Vydno. Бачу, що 90 днів повного доступу за 1 грн не активувалися. "
                "Не хочу переконувати навмання — хочу зрозуміти, де ми вас втратили. Що зупинило?"
            ),
            2: (
                "Я ще раз коротко — без продажу. Я роблю Vydno і хочу виправити те, що заважає почати. "
                "Один тап нижче вже дуже допоможе. Якщо зручніше — можна відповісти одним повідомленням."
            ),
            3: (
                "Це останнє повідомлення про пробний доступ — далі не турбуватиму. Якщо хочете, я особисто напишу тут і вислухаю, "
                "що не сподобалося. А якщо справа була лише в моменті — 90 днів за 1 грн усе ще можна активувати."
            ),
        }
    if step in {1, 2}:
        buttons = [
            [{"text": label, "callback_data": f"trialrec:r:{recipient.pk}:{code}"}]
            for code, label in REASON_LABELS[lang].items()
        ]
        if step == 2:
            buttons.append(
                [
                    {
                        "text": "Reply in my own words" if lang == "en" else "Написати своїми словами",
                        "callback_data": f"trialrec:t:{recipient.pk}",
                    }
                ]
            )
        return texts[step], buttons
    return texts[step], [
        [
            {
                "text": "Yes, message me" if lang == "en" else "Так, напишіть мені",
                "callback_data": f"trialrec:c:{recipient.pk}",
            }
        ],
        [
            {
                "text": "Activate 90 days" if lang == "en" else "Активувати 90 днів",
                "callback_data": "settings:billing:bind:90",
            }
        ],
        [
            {
                "text": "Don’t message me about the trial" if lang == "en" else "Не писати про trial",
                "callback_data": f"trialrec:o:{recipient.pk}",
            }
        ],
    ]


def _current_ineligible_reason(recipient: TrialRecoveryRecipient) -> str:
    user = recipient.user
    if campaign_uses_registered_without_card_audience(recipient.campaign):
        return _ineligible_reason(user, include_existing_recipient=False)
    return _legacy_offer_ineligible_reason(user, include_existing_recipient=False)


def _reschedule_without_sending(
    recipient: TrialRecoveryRecipient,
    campaign: TrialRecoveryCampaign,
    now: datetime,
    *,
    force_next_day: bool = False,
) -> None:
    recipient.next_send_at = _stable_window_slot(
        now,
        timezone_name=recipient.timezone,
        window_start=campaign.send_window_start,
        window_end=campaign.send_window_end,
        seed=recipient.user_id + int(recipient.sent_count or 0),
        force_next_day=force_next_day,
    )
    recipient.save(update_fields=["next_send_at", "updated_at"])


@transaction.atomic
def _repair_sent_delivery(delivery: TrialRecoveryDelivery) -> None:
    recipient = TrialRecoveryRecipient.objects.select_for_update().get(pk=delivery.recipient_id)
    if delivery.status != TrialRecoveryDelivery.Status.SENT or not delivery.sent_at or recipient.sent_count >= delivery.step:
        return
    recipient.sent_count = delivery.step
    recipient.first_sent_at = recipient.first_sent_at or delivery.sent_at
    recipient.last_sent_at = delivery.sent_at
    zone, _ = safe_zone(recipient.timezone)
    recipient.last_sent_local_date = delivery.sent_at.astimezone(zone).date()
    recipient.last_error = ""
    # Facts of delivery are recorded even if a reply/opt-out raced the send.
    if recipient.status == TrialRecoveryRecipient.Status.SCHEDULED and not recipient.opted_out_at:
        due = delivery.sent_at + SECOND_MESSAGE_DELAY if delivery.step == 1 else recipient.first_sent_at + FINAL_MESSAGE_DELAY if delivery.step == 2 else None
        if due is None:
            recipient.status = TrialRecoveryRecipient.Status.COMPLETED
            recipient.next_send_at = None
        else:
            recipient.next_send_at = coerce_to_send_window(due, timezone_name=recipient.timezone,
                window_start=recipient.campaign.send_window_start, window_end=recipient.campaign.send_window_end)
    else:
        recipient.next_send_at = None
    recipient.save(update_fields=["sent_count", "first_sent_at", "last_sent_at", "last_sent_local_date",
        "last_error", "status", "next_send_at", "updated_at"])


@transaction.atomic
def _record_unknown_delivery(delivery: TrialRecoveryDelivery, error: str) -> str:
    recipient = TrialRecoveryRecipient.objects.select_for_update().get(pk=delivery.recipient_id)
    delivery = TrialRecoveryDelivery.objects.select_for_update().get(pk=delivery.pk)
    if delivery.status == TrialRecoveryDelivery.Status.SENT:
        _repair_sent_delivery(delivery)
        return "sent"
    delivery.status = "unknown"
    delivery.next_retry_at = None
    delivery.error_message = error[:2000]
    delivery.save(update_fields=["status", "next_retry_at", "error_message", "updated_at"])
    recipient.next_send_at = None
    recipient.last_error = delivery.error_message
    if recipient.status == TrialRecoveryRecipient.Status.SCHEDULED:
        recipient.status = TrialRecoveryRecipient.Status.DELIVERY_FAILED
    recipient.save(update_fields=["status", "next_send_at", "last_error", "updated_at"])
    return "unknown"


def _recover_stale_delivery_claims(now: datetime, *, limit: int) -> int:
    stale = TrialRecoveryDelivery.objects.filter(status=TrialRecoveryDelivery.Status.CLAIMED).filter(
        Q(claimed_at__lte=now - timedelta(minutes=5)) |
        Q(claimed_at__isnull=True, created_at__lte=now - timedelta(minutes=5))
    ).order_by("pk")[:limit]
    count = 0
    for candidate in stale:
        with transaction.atomic():
            _recipient = TrialRecoveryRecipient.objects.select_for_update().get(pk=candidate.recipient_id)
            delivery = TrialRecoveryDelivery.objects.select_for_update().get(pk=candidate.pk)
            if delivery.status != TrialRecoveryDelivery.Status.CLAIMED:
                continue
            _record_unknown_delivery(delivery, "Delivery lease expired; may have been sent. Operator receipt reconciliation required.")
            count += 1
    return count


@transaction.atomic
def _send_claimed_delivery(recipient_id: int, delivery_id: int, *, text: str, buttons: list, clock):
    recipient = TrialRecoveryRecipient.objects.select_for_update().get(pk=recipient_id)
    delivery = TrialRecoveryDelivery.objects.select_for_update().get(pk=delivery_id)
    campaign = recipient.campaign
    now = clock()
    # Re-read consent, offer eligibility and the clock after claim and before I/O.
    if (recipient.status != TrialRecoveryRecipient.Status.SCHEDULED or recipient.opted_out_at
            or campaign.status != TrialRecoveryCampaign.Status.RUNNING or _current_ineligible_reason(recipient)
            or delivery.status != TrialRecoveryDelivery.Status.CLAIMED):
        delivery.status = TrialRecoveryDelivery.Status.SKIPPED
        delivery.save(update_fields=["status", "updated_at"])
        return "skipped"
    zone, _ = safe_zone(recipient.timezone, campaign.fallback_timezone)
    local = now.astimezone(zone)
    if not (campaign.send_window_start <= local.time().replace(tzinfo=None) < campaign.send_window_end) or recipient.last_sent_local_date == local.date():
        _reschedule_without_sending(recipient, campaign, now, force_next_day=recipient.last_sent_local_date == local.date())
        delivery.status = "retry"
        delivery.next_retry_at = recipient.next_send_at
        delivery.attempt_count = max(0, delivery.attempt_count - 1)
        delivery.save(update_fields=["status", "next_retry_at", "attempt_count", "updated_at"])
        return "rescheduled"
    try:
        return send_telegram_message(bot_token=settings.TELEGRAM_BOT_TOKEN, chat_id=recipient.user_id,
            text=text, buttons=buttons)
    except Exception as exc:
        return exc


def _dispatch_recipient(recipient_id: int, now: datetime | None = None, *, clock=None) -> str:
    clock = clock or ((lambda: now) if now is not None else timezone.now)
    now = clock()
    recipient = TrialRecoveryRecipient.objects.select_related("campaign", "user", "user__admin_state", "user__billing_profile").get(pk=recipient_id)
    campaign = recipient.campaign
    if campaign.status != TrialRecoveryCampaign.Status.RUNNING or recipient.status != TrialRecoveryRecipient.Status.SCHEDULED:
        return "skipped"
    if campaign_uses_registered_without_card_audience(campaign):
        _ensure_admin_state(recipient.user)
    ineligible = _current_ineligible_reason(recipient)
    if ineligible:
        recipient.status = TrialRecoveryRecipient.Status.CONVERTED if ineligible in {"has_card", "has_full_subscription", "has_successful_payment"} else TrialRecoveryRecipient.Status.INELIGIBLE
        recipient.ineligible_reason = ineligible
        recipient.next_send_at = None
        if recipient.status == TrialRecoveryRecipient.Status.CONVERTED:
            recipient.converted_at = now
        recipient.save(update_fields=["status", "ineligible_reason", "next_send_at", "converted_at", "updated_at"])
        return "ineligible"

    zone, _ = safe_zone(recipient.timezone, campaign.fallback_timezone)
    local_now = now.astimezone(zone)
    if not (campaign.send_window_start <= local_now.time().replace(tzinfo=None) < campaign.send_window_end):
        _reschedule_without_sending(recipient, campaign, now)
        return "rescheduled"
    if recipient.last_sent_local_date == local_now.date():
        _reschedule_without_sending(recipient, campaign, now, force_next_day=True)
        return "rescheduled"
    if recipient.next_send_at and now - recipient.next_send_at > MAX_FRESH_DELIVERY_LAG:
        _reschedule_without_sending(recipient, campaign, now)
        return "rescheduled"

    step = int(recipient.sent_count or 0) + 1
    with transaction.atomic():
        locked = TrialRecoveryRecipient.objects.select_for_update().get(pk=recipient.pk)
        if locked.status != TrialRecoveryRecipient.Status.SCHEDULED or int(locked.sent_count or 0) + 1 != step:
            return "skipped"
        delivery, created = TrialRecoveryDelivery.objects.get_or_create(
            recipient=locked,
            step=step,
            defaults={"status": TrialRecoveryDelivery.Status.CLAIMED},
        )
        if not created and (delivery.status != "retry" or not delivery.next_retry_at or delivery.next_retry_at > now):
            return "duplicate"
        delivery.status = TrialRecoveryDelivery.Status.CLAIMED
        delivery.claimed_at = now
        delivery.next_retry_at = None
        delivery.attempt_count += 1
        delivery.save(update_fields=["status", "claimed_at", "next_retry_at", "attempt_count", "updated_at"])

    if not settings.TELEGRAM_BOT_TOKEN:
        delivery.status = TrialRecoveryDelivery.Status.FAILED
        delivery.error_message = "TELEGRAM_BOT_TOKEN missing"
        delivery.save(update_fields=["status", "error_message", "updated_at"])
        TrialRecoveryRecipient.objects.filter(pk=recipient.pk, status=TrialRecoveryRecipient.Status.SCHEDULED).update(
            status=TrialRecoveryRecipient.Status.DELIVERY_FAILED,
            next_send_at=None,
            last_error="TELEGRAM_BOT_TOKEN missing",
            updated_at=now,
        )
        return "failed"

    text, buttons = _message_payload(recipient, step)
    try:
        payload = _send_claimed_delivery(recipient.pk, delivery.pk, text=text, buttons=buttons, clock=clock)
        if isinstance(payload, str):
            return payload
        if isinstance(payload, Exception):
            raise payload
        now = clock()
        if not isinstance(payload, dict) or payload.get("ok") is False or not isinstance(payload.get("result", {}).get("message_id"), int):
            return _record_unknown_delivery(delivery, "No valid Telegram receipt; outcome unknown")
    except Exception as exc:
        now = clock()
        try:
            rejected = json.loads(getattr(exc, "payload", "") or "{}")
        except (ValueError, TypeError):
            rejected = {}
        code = rejected.get("error_code")
        if not isinstance(exc, TelegramSendError) or (rejected.get("ok") is not False and not getattr(exc, "blocked", False)):
            return _record_unknown_delivery(delivery, f"{type(exc).__name__}: delivery outcome unknown; receipt reconciliation required")
        if rejected.get("ok") is False and code in {429, 500, 502, 503, 504} and delivery.attempt_count < 5:
            delay = max(60 * 2 ** min(delivery.attempt_count, 6), int((rejected.get("parameters") or {}).get("retry_after") or 0))
            retry_at = coerce_to_send_window(now + timedelta(seconds=delay), timezone_name=recipient.timezone,
                window_start=campaign.send_window_start, window_end=campaign.send_window_end)
            delivery.status = "retry"
            delivery.next_retry_at = retry_at
            delivery.error_message = str(exc)[:2000]
            delivery.save(update_fields=["status", "next_retry_at", "error_message", "updated_at"])
            TrialRecoveryRecipient.objects.filter(pk=recipient.pk, status=TrialRecoveryRecipient.Status.SCHEDULED).update(
                next_send_at=retry_at, last_error=str(exc)[:2000], updated_at=now)
            return "retry"
        delivery.status = TrialRecoveryDelivery.Status.FAILED
        delivery.error_message = str(exc)[:2000]
        delivery.save(update_fields=["status", "error_message", "updated_at"])
        TrialRecoveryRecipient.objects.filter(pk=recipient.pk, status=TrialRecoveryRecipient.Status.SCHEDULED).update(
            status=TrialRecoveryRecipient.Status.DELIVERY_FAILED,
            next_send_at=None,
            last_error=str(exc)[:2000],
            updated_at=now,
        )
        if exc.blocked:
            UserAdminState.objects.update_or_create(
                telegram_user_id=recipient.user_id,
                defaults={"can_receive_messages": False, "blocked_bot": True},
            )
        return "failed"

    message_id = payload.get("result", {}).get("message_id")
    delivery.status = TrialRecoveryDelivery.Status.SENT
    delivery.telegram_message_id = int(message_id) if message_id is not None else None
    delivery.sent_at = now
    delivery.save(update_fields=["status", "telegram_message_id", "sent_at", "updated_at"])

    _repair_sent_delivery(delivery)
    return "sent"


def dispatch_trial_recovery(*, now: datetime | None = None, limit: int = 250, clock=None) -> dict[str, int]:
    # `now` remains a deterministic test compatibility seam. Production takes a
    # fresh clock reading per recipient and after each transport completes.
    clock = clock or ((lambda: now) if now is not None else timezone.now)
    now = clock()
    TrialRecoveryCampaign.objects.filter(
        status=TrialRecoveryCampaign.Status.SCHEDULED,
        launch_at__isnull=False,
        launch_at__lte=now,
    ).update(status=TrialRecoveryCampaign.Status.RUNNING, started_at=now, updated_at=now)
    stats: Counter[str] = Counter()
    stats["unknown"] += _recover_stale_delivery_claims(now, limit=max(1, int(limit)))
    for delivery in TrialRecoveryDelivery.objects.filter(status=TrialRecoveryDelivery.Status.SENT,
            recipient__sent_count__lt=F("step")).order_by("sent_at", "pk")[:max(1, int(limit))]:
        _repair_sent_delivery(delivery)
        stats["repaired"] += 1
    for campaign in TrialRecoveryCampaign.objects.filter(status=TrialRecoveryCampaign.Status.RUNNING):
        stats["enrolled"] += sync_registered_without_card_audience(campaign, now=now)
    due_ids = list(
        TrialRecoveryRecipient.objects.filter(
            campaign__status=TrialRecoveryCampaign.Status.RUNNING,
            status=TrialRecoveryRecipient.Status.SCHEDULED,
            next_send_at__isnull=False,
            next_send_at__lte=now,
        )
        .order_by("next_send_at", "id")
        .values_list("id", flat=True)[: max(1, int(limit))]
    )
    for recipient_id in due_ids:
        try:
            stats[_dispatch_recipient(int(recipient_id), clock=clock)] += 1
        except Exception as exc:  # pragma: no cover - one user must not stop the campaign
            logger.exception("Trial recovery delivery failed for recipient=%s", recipient_id)
            stats["errors"] += 1
            TrialRecoveryRecipient.objects.filter(pk=recipient_id).update(last_error=str(exc)[:2000], updated_at=now)
    stats["selected"] = len(due_ids)
    return dict(stats)


def mark_trial_recovery_converted(user_id: int, *, at: datetime | None = None) -> int:
    at = at or timezone.now()
    return TrialRecoveryRecipient.objects.filter(user_id=user_id).exclude(
        status=TrialRecoveryRecipient.Status.CONVERTED
    ).update(
        status=Case(When(opted_out_at__isnull=False, then=Value(TrialRecoveryRecipient.Status.OPTED_OUT)),
                    default=Value(TrialRecoveryRecipient.Status.CONVERTED)),
        converted_at=at,
        next_send_at=None,
        awaiting_text_until=None,
        updated_at=at,
    )


def campaign_metrics(campaign: TrialRecoveryCampaign) -> dict[str, object]:
    counts = Counter(campaign.recipients.values_list("status", flat=True))
    reasons = Counter(campaign.recipients.exclude(reason="").values_list("reason", flat=True))
    recovered_users = campaign.recipients.filter(
        first_sent_at__isnull=False,
        converted_at__isnull=False,
        converted_at__gte=F("first_sent_at"),
    ).count()
    return {
        "recipients": campaign.recipients.count(),
        "sent_messages": TrialRecoveryDelivery.objects.filter(recipient__campaign=campaign, status=TrialRecoveryDelivery.Status.SENT).count(),
        "responses": campaign.recipients.filter(responded_at__isnull=False).count(),
        "contact_requests": campaign.recipients.filter(contact_requested_at__isnull=False).count(),
        "conversions": campaign.recipients.filter(converted_at__isnull=False).count(),
        "recovered_users": recovered_users,
        "opted_out": campaign.recipients.filter(opted_out_at__isnull=False).count(),
        "completed": counts[TrialRecoveryRecipient.Status.COMPLETED],
        "failed": counts[TrialRecoveryRecipient.Status.DELIVERY_FAILED],
        "reasons": dict(sorted(reasons.items())),
    }
