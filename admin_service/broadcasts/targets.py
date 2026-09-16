from __future__ import annotations

from datetime import timedelta

from django.conf import settings
from django.db.models import QuerySet
from django.db.models import Q
from django.utils import timezone

from subscriptions.models import Subscription
from users.models import TelegramUser, UserAdminState

SYSTEM_SEGMENTS: list[dict[str, str]] = [
    {"name": "All Users", "slug": "all_users", "description": "All users", "type": "system"},
    {"name": "Active 7d", "slug": "active_7d", "description": "Seen within 7 days", "type": "system"},
    {"name": "Active 30d", "slug": "active_30d", "description": "Seen within 30 days", "type": "system"},
    {"name": "Inactive 14d", "slug": "inactive_14d", "description": "Not seen for 14 days", "type": "system"},
    {"name": "Inactive 30d", "slug": "inactive_30d", "description": "Not seen for 30 days", "type": "system"},
    {"name": "Free Users", "slug": "free_users", "description": "Users without subscription", "type": "system"},
    {"name": "Onboarding Not Completed", "slug": "onboarding_not_completed", "description": "Users stuck in onboarding", "type": "system"},
    {"name": "Trial Users", "slug": "trial_users", "description": "Users on trial", "type": "system"},
    {"name": "Trial Expiring 1d", "slug": "trial_expiring_1d", "description": "Trial expires within 1 day", "type": "system"},
    {"name": "Trial Expiring 3d", "slug": "trial_expiring_3d", "description": "Trial expires within 3 days", "type": "system"},
    {"name": "Paid Users", "slug": "paid_users", "description": "Users with paid/manual/lifetime access", "type": "system"},
    {
        "name": "Тестові користувачі",
        "slug": "test_users",
        "description": "Користувачі з позначкою тестового або додані до allowlist для QA",
        "type": "system",
    },
    {"name": "Subscription Expiring Today", "slug": "subscription_expiring_today", "description": "Subscriptions ending today", "type": "system"},
    {"name": "Subscription Expiring 3d", "slug": "subscription_expiring_3d", "description": "Subscriptions ending within 3 days", "type": "system"},
    {"name": "Subscription Expiring 7d", "slug": "subscription_expiring_7d", "description": "Subscriptions ending within 7 days", "type": "system"},
    {"name": "Expired Users", "slug": "expired_users", "description": "Users with expired subscription", "type": "system"},
    {"name": "Users by Tag", "slug": "users_by_tag", "description": "Resolved with target_tag", "type": "tag"},
    {"name": "Users by Source", "slug": "users_by_source", "description": "Resolved with admin source", "type": "source"},
]


def parse_manual_user_ids(raw_value) -> list[int]:
    if isinstance(raw_value, list):
        return [int(item) for item in raw_value if str(item).isdigit()]
    if isinstance(raw_value, str):
        parts = raw_value.replace("\n", ",").split(",")
        return [int(item.strip()) for item in parts if item.strip().isdigit()]
    return []


def base_user_queryset() -> QuerySet[TelegramUser]:
    return TelegramUser.objects.all().order_by("tg_user_id")


class InvalidAudience(ValueError):
    """An absent/unsupported audience must never mean every user."""


def validate_target(*, target_type, target_segment=None, target_tag=None, target_topic=None, manual_users=None):
    if target_type not in {"all", "manual_users", "tag", "push_topic", "segment"}:
        raise InvalidAudience("Unknown audience type; choose an explicit audience.")
    if target_type == "manual_users" and not parse_manual_user_ids(manual_users):
        raise InvalidAudience("Manual audience requires valid Telegram IDs.")
    if target_type == "tag" and (target_tag is None or target_tag.pk is None):
        raise InvalidAudience("Selected tag is missing; confirm a new audience.")
    if target_type == "push_topic" and (target_topic is None or target_topic.pk is None or not target_topic.is_active):
        raise InvalidAudience("Selected push topic is missing or inactive.")
    if target_type == "segment":
        if target_segment is None or target_segment.pk is None or not target_segment.is_active:
            raise InvalidAudience("Selected segment is missing or inactive.")
        if target_segment.slug not in {item["slug"] for item in SYSTEM_SEGMENTS}:
            raise InvalidAudience("Unsupported segment; choose an explicit audience.")
        if target_segment.slug == "users_by_tag" and (target_tag is None or target_tag.pk is None):
            raise InvalidAudience("Tag segment requires a selected tag.")
        if target_segment.slug == "users_by_source":
            raise InvalidAudience("Source segment requires an explicit source; this campaign has none.")


def users_for_segment_slug(slug: str, *, target_tag_id=None, source: str = "") -> QuerySet[TelegramUser]:
    now = timezone.now()
    today = now.date()
    qs = base_user_queryset()
    paid_statuses = [
        Subscription.Status.ACTIVE,
        Subscription.Status.PAID,
        Subscription.Status.MANUAL,
        Subscription.Status.LIFETIME,
    ]
    expiring_1d = now + timedelta(days=1)
    expiring_3d = now + timedelta(days=3)
    expiring_7d = now + timedelta(days=7)

    if slug == "all_users":
        return qs
    if slug == "active_7d":
        return qs.filter(last_seen_at__gte=now - timedelta(days=7))
    if slug == "active_30d":
        return qs.filter(last_seen_at__gte=now - timedelta(days=30))
    if slug == "inactive_14d":
        return qs.filter(last_seen_at__lt=now - timedelta(days=14))
    if slug == "inactive_30d":
        return qs.filter(last_seen_at__lt=now - timedelta(days=30))
    if slug == "free_users":
        return qs.filter(Q(admin_state__subscription_status=UserAdminState.SubscriptionStatus.NONE) | Q(admin_state__isnull=True))
    if slug == "onboarding_not_completed":
        return qs.filter(onboarding_completed=False)
    if slug == "trial_users":
        return qs.filter(admin_state__subscription_status=UserAdminState.SubscriptionStatus.TRIAL)
    if slug == "trial_expiring_1d":
        ids = Subscription.objects.filter(
            status=Subscription.Status.TRIAL,
            expires_at__isnull=False,
            expires_at__lte=expiring_1d,
            expires_at__gte=now,
        ).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "trial_expiring_3d":
        ids = Subscription.objects.filter(
            status=Subscription.Status.TRIAL,
            expires_at__isnull=False,
            expires_at__lte=expiring_3d,
            expires_at__gte=now,
        ).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "paid_users":
        ids = Subscription.objects.filter(status__in=paid_statuses).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "test_users":
        allowlisted_ids = list(getattr(settings, "ADMIN_TEST_TELEGRAM_IDS", []))
        if allowlisted_ids:
            return qs.filter(Q(admin_state__is_test_user=True) | Q(tg_user_id__in=allowlisted_ids))
        return qs.filter(admin_state__is_test_user=True)
    if slug == "subscription_expiring_today":
        ids = Subscription.objects.filter(
            status__in=[Subscription.Status.TRIAL, *paid_statuses],
            expires_at__date=today,
        ).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "subscription_expiring_3d":
        ids = Subscription.objects.filter(
            status__in=[Subscription.Status.TRIAL, *paid_statuses],
            expires_at__isnull=False,
            expires_at__lte=expiring_3d,
            expires_at__gte=now,
        ).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "subscription_expiring_7d":
        ids = Subscription.objects.filter(
            status__in=[Subscription.Status.TRIAL, *paid_statuses],
            expires_at__isnull=False,
            expires_at__lte=expiring_7d,
            expires_at__gte=now,
        ).values_list("user_id", flat=True)
        return qs.filter(tg_user_id__in=ids)
    if slug == "expired_users":
        return qs.filter(admin_state__subscription_status=UserAdminState.SubscriptionStatus.EXPIRED)
    if slug == "users_by_tag" and target_tag_id:
        return qs.filter(user_tags__tag_id=target_tag_id)
    if slug == "users_by_source" and source:
        return qs.filter(admin_state__source__iexact=source)
    raise InvalidAudience("Unknown or incomplete segment; audience was not resolved.")


def users_for_target(
    *,
    target_type: str,
    target_segment=None,
    target_tag=None,
    target_topic=None,
    manual_users=None,
) -> QuerySet[TelegramUser]:
    validate_target(target_type=target_type, target_segment=target_segment, target_tag=target_tag,
                    target_topic=target_topic, manual_users=manual_users)
    qs = base_user_queryset().exclude(admin_state__status=UserAdminState.Status.BANNED)
    if target_type == "manual_users":
        return qs.filter(tg_user_id__in=parse_manual_user_ids(manual_users))
    if target_type == "tag" and target_tag:
        return qs.filter(user_tags__tag=target_tag).distinct()
    if target_type == "push_topic" and target_topic:
        return qs.filter(push_topic_settings__topic=target_topic, push_topic_settings__is_enabled=True)
    if target_type == "segment" and target_segment:
        if target_segment.slug == "users_by_tag" and target_tag:
            return users_for_segment_slug(target_segment.slug, target_tag_id=target_tag.pk).exclude(
                admin_state__status=UserAdminState.Status.BANNED
            )
        return users_for_segment_slug(target_segment.slug).exclude(admin_state__status=UserAdminState.Status.BANNED)
    return qs


def campaign_target_kwargs(campaign) -> dict:
    legacy = getattr(campaign, "segment_filter", {}) or {}
    return {
        "target_type": campaign.target_type,
        "target_segment": campaign.target_segment,
        "target_tag": campaign.target_tag,
        "target_topic": campaign.target_topic,
        "manual_users": campaign.manual_users or legacy.get("telegram_ids", []),
    }


def delivery_config(campaign) -> dict:
    target = campaign_target_kwargs(campaign)
    validate_target(**target)
    fields = ("title", "target_type", "target_segment_id", "target_tag_id", "target_topic_id")
    if campaign._meta.model_name == "broadcast":
        fields += ("message_text", "image_url", "parse_mode", "button_text", "button_url", "segment_filter")
    else:
        fields += ("question", "type", "options", "low_rating_threshold")
    config = {name: getattr(campaign, name) for name in fields}
    config["manual_users"] = sorted(set(parse_manual_user_ids(target["manual_users"])))
    config["segment_slug"] = campaign.target_segment.slug if campaign.target_segment else None
    return config


def delivery_snapshot(campaign, *, recipients=None) -> dict:
    config = delivery_config(campaign)
    if recipients is None:
        recipients = users_for_target(**campaign_target_kwargs(campaign))
    return {
        "version": 1,
        "campaign": campaign._meta.label_lower,
        "campaign_id": campaign.pk,
        "config": config,
        "audience_ids": sorted({user.tg_user_id for user in recipients}),
    }


def verified_recipients(campaign, confirmed_snapshot) -> list:
    recipients = list(users_for_target(**campaign_target_kwargs(campaign)))
    if not confirmed_snapshot or delivery_snapshot(campaign, recipients=recipients) != confirmed_snapshot:
        raise InvalidAudience("Audience or message changed since confirmation; preview and confirm again.")
    if not recipients:
        raise InvalidAudience("Confirmed audience has no eligible recipients.")
    return recipients
