from __future__ import annotations

import re
import uuid
import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from django.conf import settings
from django.db import IntegrityError, transaction
from django.http import HttpRequest
from django.utils import timezone

from miniapp.models import AcquisitionSession, FunnelEvent
from users.models import TelegramUser


CLIENT_EVENT_NAMES = frozenset(
    {
        "landing_view",
        "landing_primary_cta_click",
        "product_demo_start",
        "product_demo_complete",
        "onboarding_start",
        "onboarding_step_view",
        "onboarding_step_complete",
        "onboarding_validation_error",
        "onboarding_review",
        "paywall_view",
        "billing_terms_view",
        "checkout_opened",
        "install_education_view",
        "install_cta_click",
        "install_prompt_available",
        "install_prompt_accepted",
        "install_prompt_dismissed",
        "install_manual_steps_view",
        "standalone_launch",
        "install_skip",
        "first_action_started",
        "first_draft_ready",
        "first_dashboard_value_seen",
        "push_education_view",
        "push_permission_granted",
        "push_permission_denied",
        "push_permission_dismissed",
    }
)

SERVER_EVENT_NAMES = frozenset(
    {
        "auth_start",
        "auth_success",
        "auth_failure",
        "onboarding_confirmed",
        "billing_consent_accepted",
        "bind_invoice_created",
        "payment_processing",
        "payment_success",
        "payment_failure",
        "first_transaction_confirmed",
    }
)

_ALLOWED_CLIENT_FIELDS = frozenset(
    {
        "event_name",
        "event_id",
        "funnel_version",
        "landing_variant",
        "offer_variant",
        "platform",
        "container",
        "outcome",
        "placement",
    }
)
_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_DIMENSION_FIELDS = (
    "funnel_version",
    "landing_variant",
    "offer_variant",
    "platform",
    "container",
    "outcome",
    "placement",
)
_ATTRIBUTION_LIMITS = {
    "utm_source": 128,
    "utm_medium": 128,
    "utm_campaign": 128,
    "utm_content": 128,
    "utm_term": 128,
    "referral_code": 128,
    "landing_variant": 64,
}
ACQUISITION_SESSION_KEY = "funnel_acquisition_session_id"


class FunnelValidationError(ValueError):
    def __init__(self, code: str, message: str, *, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


@dataclass(frozen=True, slots=True)
class ValidatedClientEvent:
    event_name: str
    event_id: uuid.UUID
    funnel_version: str = ""
    landing_variant: str = ""
    offer_variant: str = ""
    platform: str = ""
    container: str = ""
    outcome: str = ""
    placement: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "event_name": self.event_name,
            "event_id": str(self.event_id),
            **{field: getattr(self, field) for field in _DIMENSION_FIELDS},
        }


def _bounded_slug(value: Any, *, field: str, required: bool = False) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        if required:
            raise FunnelValidationError("invalid_payload", f"{field} is required.")
        return ""
    if not _SLUG_RE.fullmatch(normalized):
        raise FunnelValidationError("invalid_payload", f"{field} is invalid.")
    return normalized


def normalize_attribution(values: Any) -> dict[str, str]:
    if not isinstance(values, dict):
        return {}
    normalized: dict[str, str] = {}
    for field, limit in _ATTRIBUTION_LIMITS.items():
        value = str(values.get(field) or "").strip()
        if not value or len(value) > limit:
            continue
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
            continue
        normalized[field] = value
    return normalized


def validate_client_event_payload(payload: Any) -> ValidatedClientEvent:
    if not isinstance(payload, dict):
        raise FunnelValidationError("invalid_payload", "A JSON object is required.")

    event_name = _bounded_slug(payload.get("event_name"), field="event_name", required=True)
    if event_name in SERVER_EVENT_NAMES:
        raise FunnelValidationError("server_event_required", "This event can only be recorded by the server.")
    if event_name not in CLIENT_EVENT_NAMES:
        raise FunnelValidationError("unsupported_event", "Unsupported funnel event.")

    unknown_fields = set(payload) - _ALLOWED_CLIENT_FIELDS
    if unknown_fields:
        raise FunnelValidationError("invalid_payload", "Unsupported funnel event fields.")

    try:
        event_id = uuid.UUID(str(payload.get("event_id") or ""))
    except (ValueError, TypeError, AttributeError) as exc:
        raise FunnelValidationError("invalid_event_id", "event_id must be a UUID.") from exc

    values = {
        field: _bounded_slug(payload.get(field), field=field)
        for field in _DIMENSION_FIELDS
    }
    return ValidatedClientEvent(event_name=event_name, event_id=event_id, **values)


def _request_device_dimensions(request: HttpRequest) -> dict[str, str]:
    user_agent = str(request.META.get("HTTP_USER_AGENT") or "").lower()
    if any(marker in user_agent for marker in ("iphone", "ipad", "ipod")):
        platform = "ios"
    elif "android" in user_agent:
        platform = "android"
    elif user_agent:
        platform = "desktop"
    else:
        platform = "other"
    mobile_markers = ("mobile", "iphone", "ipad", "ipod", "android")
    automation_markers = tuple(
        str(marker).strip().lower()
        for marker in getattr(
            settings,
            "FUNNEL_AUTOMATION_USER_AGENT_MARKERS",
            ("playwright", "selenium", "cypress", "headlesschrome", "lighthouse", "puppeteer"),
        )
        if str(marker).strip()
    )
    return {
        "platform": platform,
        "container": "telegram" if "telegram" in user_agent else "browser",
        "device_class": "mobile" if any(marker in user_agent for marker in mobile_markers) else "desktop",
        "automation_status": (
            AcquisitionSession.AutomationStatus.AUTOMATED
            if any(marker in user_agent for marker in automation_markers)
            else AcquisitionSession.AutomationStatus.UNKNOWN
        ),
    }


def _session_ttl_days() -> int:
    return max(1, min(int(getattr(settings, "FUNNEL_SESSION_TTL_DAYS", 30) or 30), 90))


def get_or_create_acquisition_session(
    request: HttpRequest,
    *,
    attribution: Any = None,
) -> AcquisitionSession:
    now = timezone.now()
    session_id = str(request.session.get(ACQUISITION_SESSION_KEY) or "").strip()
    acquisition: AcquisitionSession | None = None
    if session_id:
        try:
            parsed_id = uuid.UUID(session_id)
        except ValueError:
            request.session.pop(ACQUISITION_SESSION_KEY, None)
        else:
            acquisition = AcquisitionSession.objects.filter(pk=parsed_id, expires_at__gt=now).first()
            if acquisition is None:
                request.session.pop(ACQUISITION_SESSION_KEY, None)

    normalized = normalize_attribution(attribution)
    dimensions = _request_device_dimensions(request)
    if acquisition is None:
        landing_variant = normalized.pop("landing_variant", "")
        acquisition = AcquisitionSession.objects.create(
            funnel_version="pwa_v1",
            first_landing_variant=landing_variant,
            last_landing_variant=landing_variant,
            expires_at=now + timedelta(days=_session_ttl_days()),
            **normalized,
            **dimensions,
        )
        request.session[ACQUISITION_SESSION_KEY] = str(acquisition.pk)
        request.session.modified = True
        return acquisition

    update_fields: list[str] = []
    landing_variant = normalized.get("landing_variant", "")
    if landing_variant and landing_variant != acquisition.last_landing_variant:
        acquisition.last_landing_variant = landing_variant
        update_fields.append("last_landing_variant")
    for field, value in dimensions.items():
        if field == "automation_status" and value != AcquisitionSession.AutomationStatus.AUTOMATED:
            continue
        if value and getattr(acquisition, field) != value:
            setattr(acquisition, field, value)
            update_fields.append(field)
    if update_fields:
        acquisition.save(update_fields=sorted(set(update_fields + ["updated_at"])))
    return acquisition


@transaction.atomic
def link_acquisition_session(
    request: HttpRequest,
    *,
    user: TelegramUser,
) -> AcquisitionSession | None:
    session_id = str(request.session.get(ACQUISITION_SESSION_KEY) or "").strip()
    if not session_id:
        return None
    try:
        parsed_id = uuid.UUID(session_id)
    except ValueError:
        request.session.pop(ACQUISITION_SESSION_KEY, None)
        return None
    acquisition = AcquisitionSession.objects.select_for_update().filter(pk=parsed_id).first()
    if acquisition is None:
        request.session.pop(ACQUISITION_SESSION_KEY, None)
        return None
    if acquisition.user_id is not None and int(acquisition.user_id) != int(user.tg_user_id):
        raise FunnelValidationError(
            "acquisition_identity_conflict",
            "Acquisition session is already linked to another user.",
            status=409,
        )
    update_fields: list[str] = []
    if acquisition.user_id is None:
        acquisition.user = user
        acquisition.linked_at = timezone.now()
        update_fields.extend(["user", "linked_at"])
    if acquisition.automation_status == AcquisitionSession.AutomationStatus.UNKNOWN:
        acquisition.automation_status = AcquisitionSession.AutomationStatus.VERIFIED_HUMAN
        update_fields.append("automation_status")
    if update_fields:
        acquisition.save(update_fields=update_fields + ["updated_at"])
    return acquisition


def _event_payload_hash(payload: dict[str, str]) -> str:
    serialized = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _create_event_once(
    *,
    acquisition: AcquisitionSession,
    idempotency_key: str,
    source: str,
    payload: dict[str, str],
) -> tuple[FunnelEvent, bool]:
    payload_hash = _event_payload_hash(payload)
    defaults = {
        "event_name": payload["event_name"],
        "payload_hash": payload_hash,
        "source": source,
        **{field: payload.get(field, "") for field in _DIMENSION_FIELDS},
    }
    try:
        with transaction.atomic():
            event, created = FunnelEvent.objects.get_or_create(
                acquisition_session=acquisition,
                idempotency_key=idempotency_key,
                defaults=defaults,
            )
    except IntegrityError:
        event = FunnelEvent.objects.get(
            acquisition_session=acquisition,
            idempotency_key=idempotency_key,
        )
        created = False
    if event.payload_hash != payload_hash:
        raise FunnelValidationError(
            "idempotency_conflict",
            "The event id was already used for another payload.",
            status=409,
        )
    return event, created


def ingest_client_event(
    request: HttpRequest,
    *,
    payload: Any,
) -> tuple[FunnelEvent, bool]:
    validated = validate_client_event_payload(payload)
    acquisition = get_or_create_acquisition_session(request)
    event_payload = validated.as_dict()
    event_payload["funnel_version"] = event_payload["funnel_version"] or acquisition.funnel_version
    event_payload["landing_variant"] = event_payload["landing_variant"] or acquisition.last_landing_variant
    event_payload["platform"] = event_payload["platform"] or acquisition.platform
    event_payload["container"] = event_payload["container"] or acquisition.container
    return _create_event_once(
        acquisition=acquisition,
        idempotency_key=str(validated.event_id),
        source=FunnelEvent.Source.CLIENT,
        payload=event_payload,
    )


def record_server_event(
    *,
    event_name: str,
    idempotency_key: str,
    request: HttpRequest | None = None,
    acquisition: AcquisitionSession | None = None,
    user: TelegramUser | None = None,
    funnel_version: str = "",
    landing_variant: str = "",
    offer_variant: str = "",
    platform: str = "",
    container: str = "",
    outcome: str = "",
    placement: str = "",
) -> tuple[FunnelEvent, bool] | tuple[None, bool]:
    event_name = _bounded_slug(event_name, field="event_name", required=True)
    if event_name not in SERVER_EVENT_NAMES:
        raise FunnelValidationError("unsupported_event", "Unsupported server funnel event.")
    key = str(idempotency_key or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,159}", key):
        raise FunnelValidationError("invalid_event_id", "Server event id is invalid.")
    if acquisition is None and request is not None:
        acquisition = get_or_create_acquisition_session(request)
    if acquisition is None:
        return None, False
    if user is not None and request is not None:
        acquisition = link_acquisition_session(request, user=user) or acquisition
    payload = {
        "event_name": event_name,
        "funnel_version": _bounded_slug(funnel_version or acquisition.funnel_version, field="funnel_version"),
        "landing_variant": _bounded_slug(landing_variant or acquisition.last_landing_variant, field="landing_variant"),
        "offer_variant": _bounded_slug(offer_variant, field="offer_variant"),
        "platform": _bounded_slug(platform or acquisition.platform, field="platform"),
        "container": _bounded_slug(container or acquisition.container, field="container"),
        "outcome": _bounded_slug(outcome, field="outcome"),
        "placement": _bounded_slug(placement, field="placement"),
    }
    return _create_event_once(
        acquisition=acquisition,
        idempotency_key=key,
        source=FunnelEvent.Source.SERVER,
        payload=payload,
    )


def record_request_server_event(
    request: HttpRequest,
    *,
    user: TelegramUser,
    event_name: str,
    idempotency_key: str,
) -> tuple[FunnelEvent, bool] | None:
    acquisition = link_acquisition_session(request, user=user)
    if acquisition is None:
        return None
    return record_server_event(
        acquisition=acquisition,
        user=user,
        event_name=event_name,
        idempotency_key=idempotency_key,
    )
