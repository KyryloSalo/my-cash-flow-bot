from __future__ import annotations

import hashlib
import json
import secrets
from datetime import timedelta

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from common.audit import create_audit_log, create_canary_audit
from miniapp.auth import SESSION_AUTH_MODE_KEY, SESSION_USER_ID_KEY, build_browser_login_token
from users.models import CanaryCapability, TelegramUser, UserAdminState


class CanaryAuthorizationError(PermissionDenied):
    """Generic failure that deliberately does not reveal the failed canary gate."""


_SENSITIVE_METADATA_KEYS = {
    "authorization",
    "card",
    "card_token",
    "cvv",
    "pan",
    "password",
    "secret",
    "token",
}
_MAX_METADATA_BYTES = 4096
_MAX_TTL_SECONDS = 15 * 60


def _deny() -> None:
    raise CanaryAuthorizationError("Canary action is not available.")


def _state_for(user: TelegramUser) -> UserAdminState | None:
    return UserAdminState.objects.filter(telegram_user_id=user.tg_user_id).first()


def is_canary_user(user: TelegramUser | None) -> bool:
    if user is None or not bool(getattr(settings, "PRODUCTION_CANARY_ENABLED", False)):
        return False
    allowlist = {int(value) for value in getattr(settings, "PRODUCTION_CANARY_TELEGRAM_IDS", [])}
    if int(user.tg_user_id) not in allowlist:
        return False
    state = _state_for(user)
    return bool(
        state
        and state.is_test_user
        and state.is_production_canary
        and state.status == UserAdminState.Status.ACTIVE
        and not state.is_blocked
    )


def assert_canary_user(user: TelegramUser | None) -> TelegramUser:
    if not is_canary_user(user):
        _deny()
    return user


def assert_canary_session(request, user: TelegramUser, purpose: str) -> TelegramUser:
    del purpose  # The action's one-time capability enforces purpose.
    assert_canary_user(user)
    try:
        session_user_id = int(request.session.get(SESSION_USER_ID_KEY) or 0)
    except (TypeError, ValueError):
        _deny()
    if session_user_id != int(user.tg_user_id) or request.session.get(SESSION_AUTH_MODE_KEY) != "production_canary":
        _deny()
    return user


def _validate_metadata(value, *, path: str = "metadata"):
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_")
            if any(part in normalized for part in _SENSITIVE_METADATA_KEYS):
                raise ValidationError(f"Sensitive key is forbidden in {path}.")
            sanitized[str(key)[:80]] = _validate_metadata(item, path=f"{path}.{key}")
        return sanitized
    if isinstance(value, list):
        return [_validate_metadata(item, path=path) for item in value[:50]]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:500]


def issue_production_canary_browser_login(
    *,
    user: TelegramUser,
    admin_user,
    reason: str = "",
    request=None,
) -> str:
    """Issue a one-time signed link without persisting the raw token."""
    assert_canary_user(user)
    token = build_browser_login_token(user.tg_user_id, purpose="production_canary_browser_login")
    create_canary_audit(
        admin_user=admin_user,
        request=request,
        action="login_capability_issued",
        target_user_id=user.tg_user_id,
        result="issued",
        reason=reason,
    )
    return token


def issue_canary_capability(
    *,
    user: TelegramUser,
    purpose: str,
    issued_by,
    scenario: str = CanaryCapability.Scenario.SUCCESS,
    related_payment_id: int | None = None,
    metadata: dict | None = None,
    ttl_seconds: int | None = None,
    request=None,
) -> tuple[CanaryCapability, str]:
    assert_canary_user(user)
    if purpose not in CanaryCapability.Purpose.values:
        _deny()
    allowed_scenarios = set(
        getattr(settings, "PRODUCTION_CANARY_ALLOWED_SCENARIOS", CanaryCapability.Scenario.values)
    )
    if scenario not in CanaryCapability.Scenario.values or scenario not in allowed_scenarios:
        _deny()
    default_ttl = int(getattr(settings, "PRODUCTION_CANARY_ACTION_TTL_SECONDS", 300) or 300)
    ttl = default_ttl if ttl_seconds is None else int(ttl_seconds)
    if ttl <= 0 or ttl > _MAX_TTL_SECONDS:
        raise ValidationError("Canary capability TTL must be between 1 and 900 seconds.")
    safe_metadata = _validate_metadata(metadata or {})
    serialized_metadata = json.dumps(safe_metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(serialized_metadata) > _MAX_METADATA_BYTES:
        raise ValidationError("Canary capability metadata is too large.")

    token = secrets.token_urlsafe(32)
    capability = CanaryCapability.objects.create(
        telegram_user=user,
        purpose=purpose,
        scenario=scenario,
        nonce_hash=hashlib.sha256(token.encode("utf-8")).hexdigest(),
        issued_by=issued_by,
        expires_at=timezone.now() + timedelta(seconds=ttl),
        related_payment_id=related_payment_id,
        metadata=safe_metadata,
    )
    create_canary_audit(admin_user=issued_by, request=request, action="billing_capability_issued" if purpose.startswith("billing") else "login_capability_issued", target_user_id=user.tg_user_id, capability_id=capability.pk, payment_id=related_payment_id, scenario=scenario, result="issued")
    return capability, token


def consume_canary_capability(
    *,
    token: str,
    user: TelegramUser,
    purpose: str,
    related_payment_id: int | None = None,
    request=None,
) -> CanaryCapability:
    assert_canary_user(user)
    token_hash = hashlib.sha256(str(token).encode("utf-8")).hexdigest()
    expired = False
    with transaction.atomic():
        capability = CanaryCapability.objects.select_for_update().filter(nonce_hash=token_hash).first()
        if capability is None:
            _deny()
        if capability.status == CanaryCapability.Status.ISSUED and capability.expires_at <= timezone.now():
            capability.status = CanaryCapability.Status.EXPIRED
            capability.save(update_fields=["status"])
            expired = True
        elif (
            capability.status != CanaryCapability.Status.ISSUED
            or capability.telegram_user_id != user.tg_user_id
            or capability.purpose != purpose
            or capability.related_payment_id != related_payment_id
        ):
            _deny()
        else:
            capability.status = CanaryCapability.Status.CONSUMED
            capability.consumed_at = timezone.now()
            capability.save(update_fields=["status", "consumed_at"])
    if expired:
        _deny()
    create_canary_audit(admin_user=capability.issued_by, request=request, action="billing_capability_confirmed" if purpose.startswith("billing") else "login_capability_consumed", target_user_id=user.tg_user_id, capability_id=capability.pk, payment_id=related_payment_id, result="consumed")
    return capability
