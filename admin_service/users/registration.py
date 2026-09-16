from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from django.db import transaction
from django.utils import timezone

from bot_settings.models import BotSetting
from users.models import TelegramUser, UserAdminState, UserAuthIdentity


class TelegramIdentityAdapter(Protocol):
    provider: str
    subject: str
    tg_user_id: int
    first_name: str
    last_name: str
    username: str
    claims: dict


class RegistrationIdentityConflict(Exception):
    pass


class RegistrationClosed(Exception):
    pass


@dataclass(frozen=True, slots=True)
class RegistrationBootstrapResult:
    user: TelegramUser
    identity: UserAuthIdentity
    created: bool


@dataclass(frozen=True, slots=True)
class RegistrationTelegramIdentity:
    provider: str
    subject: str
    tg_user_id: int
    first_name: str = ""
    last_name: str = ""
    username: str = ""
    claims: dict | None = None


def _identity_profile(identity: TelegramIdentityAdapter) -> dict[str, Any]:
    claims = identity.claims if isinstance(identity.claims, dict) else {}
    profile = {
        "id": int(identity.tg_user_id),
        "name": str(claims.get("name") or "").strip(),
        "given_name": str(identity.first_name or "").strip(),
        "family_name": str(identity.last_name or "").strip(),
        "preferred_username": str(identity.username or "").strip().lstrip("@"),
        "picture": str(claims.get("picture") or "").strip(),
    }
    return {key: value for key, value in profile.items() if value not in ("", None)}


def _registration_is_open() -> bool:
    setting = BotSetting.objects.filter(key="registration_enabled").first()
    return True if setting is None else bool(setting.parsed_value())


@transaction.atomic
def bootstrap_telegram_identity(
    identity: TelegramIdentityAdapter,
    *,
    source: str,
    authenticated_at: datetime | None = None,
) -> RegistrationBootstrapResult:
    provider = str(identity.provider or "").strip()
    subject = str(identity.subject or "").strip()
    try:
        tg_user_id = int(identity.tg_user_id)
    except (TypeError, ValueError) as exc:
        raise RegistrationIdentityConflict("Telegram user ID is invalid.") from exc
    if provider not in UserAuthIdentity.Provider.values or not subject or len(subject) > 255 or tg_user_id <= 0:
        raise RegistrationIdentityConflict("Telegram identity is invalid.")

    existing_identity = (
        UserAuthIdentity.objects.select_for_update()
        .filter(provider=provider, subject=subject)
        .first()
    )
    if existing_identity is not None and int(existing_identity.telegram_user_id) != tg_user_id:
        raise RegistrationIdentityConflict("Telegram identity is already linked to another user.")

    provider_user_identity = (
        UserAuthIdentity.objects.select_for_update()
        .filter(provider=provider, telegram_user_id=tg_user_id)
        .first()
    )
    if provider_user_identity is not None and provider_user_identity.subject != subject:
        raise RegistrationIdentityConflict("Telegram user is already linked to another identity.")

    timestamp = authenticated_at or timezone.now()
    claims = identity.claims if isinstance(identity.claims, dict) else {}
    can_receive_messages = (
        provider == UserAuthIdentity.Provider.TELEGRAM_MINIAPP
        and claims.get("allows_write_to_pm") is True
    )
    user = TelegramUser.objects.select_for_update().filter(tg_user_id=tg_user_id).first()
    created = user is None
    if created:
        if not _registration_is_open():
            raise RegistrationClosed("Registration is temporarily closed.")
        user = TelegramUser.objects.create(
            tg_user_id=tg_user_id,
            first_name=str(identity.first_name or "").strip() or None,
            last_name=str(identity.last_name or "").strip() or None,
            username=str(identity.username or "").strip().lstrip("@") or None,
            base_currency="UAH",
            onboarding_completed=False,
            onboarding_version=0,
            created_at=timestamp,
            last_seen_at=timestamp,
        )
    else:
        changed_fields: list[str] = []
        for field, value in (
            ("first_name", str(identity.first_name or "").strip()),
            ("last_name", str(identity.last_name or "").strip()),
            ("username", str(identity.username or "").strip().lstrip("@")),
        ):
            if value and getattr(user, field) != value:
                setattr(user, field, value)
                changed_fields.append(field)
        user.last_seen_at = timestamp
        changed_fields.append("last_seen_at")
        user.save(update_fields=changed_fields)

    admin_state, state_created = UserAdminState.objects.get_or_create(
        telegram_user_id=tg_user_id,
        defaults={
            "status": UserAdminState.Status.ACTIVE,
            "subscription_status": UserAdminState.SubscriptionStatus.NONE,
            "access_scope": UserAdminState.AccessScope.PAYWALL,
            "source": str(source or "")[:128],
            "last_action_at": timestamp,
            "can_receive_messages": can_receive_messages,
        },
    )
    if not state_created:
        state_fields = ["last_action_at", "updated_at"]
        admin_state.last_action_at = timestamp
        if not admin_state.source and source:
            admin_state.source = str(source)[:128]
            state_fields.append("source")
        if can_receive_messages and not admin_state.can_receive_messages:
            admin_state.can_receive_messages = True
            state_fields.append("can_receive_messages")
        admin_state.save(update_fields=state_fields)

    auth_identity, _ = UserAuthIdentity.objects.update_or_create(
        provider=provider,
        subject=subject,
        defaults={
            "telegram_user_id": tg_user_id,
            "profile": _identity_profile(identity),
            "first_authenticated_at": (
                existing_identity.first_authenticated_at if existing_identity is not None else timestamp
            ),
            "last_authenticated_at": timestamp,
        },
    )
    return RegistrationBootstrapResult(user=user, identity=auth_identity, created=created)
