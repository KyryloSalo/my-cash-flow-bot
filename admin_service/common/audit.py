from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID
import re

from django.db import models
from django.utils import timezone

AUDIT_METADATA_ALLOWLIST = {"actor", "target_user_id", "capability_uuid", "payment_id", "scenario", "result", "action", "timestamp", "reason", "scope", "count", "fingerprint", "nested", "revoked_count", "state", "status", "effect", "target_id", "financial_warning", "run_id"}
_MAX_AUDIT_STRING = 160
_SECRETISH = re.compile(r"(?:https?://|Bearer\s|token|secret|password|api[_-]?key|card[_-]?token|authorization)", re.I)
_PAN_LIKE = re.compile(r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)")

def sanitize_audit_metadata(value, *, _key=""):
    """Keep only typed, explicitly allowed metadata; never persist arbitrary text."""
    if isinstance(value, dict):
        return {str(k): sanitize_audit_metadata(v, _key=str(k)) for k, v in value.items() if str(k) in AUDIT_METADATA_ALLOWLIST and not _SECRETISH.search(str(k))}
    if isinstance(value, list):
        return [sanitize_audit_metadata(v) for v in value[:20] if isinstance(v, (str, int, float, bool))]
    if isinstance(value, (bool, int, float)) or value is None:
        return value
    if isinstance(value, str) and _key in AUDIT_METADATA_ALLOWLIST:
        if _SECRETISH.search(value) or _PAN_LIKE.search(value):
            return "[redacted]"
        return value[:_MAX_AUDIT_STRING]
    return None
CANARY_AUDIT_ACTIONS = {
    "enabled", "disabled", "login_capability_issued", "login_capability_consumed",
    "login_capability_rejected", "billing_capability_issued", "billing_capability_confirmed",
    "billing_capability_rejected", "scenario_delivered", "delayed_completed",
    "renewal_initiated", "retry_initiated", "reset_previewed", "reset_executed",
    "reset_failed", "forbidden_provider_boundary_attempt",
}

def canary_action(name: str) -> str:
    return f"production_canary_{name}"

def create_canary_audit(*, admin_user=None, request=None, action, target_user_id=None, capability_id=None, payment_id=None, scenario=None, result=None, reason="", actor_id=None):
    if action not in CANARY_AUDIT_ACTIONS:
        raise ValueError(f"Unsupported canary audit action: {action}")
    metadata = {"actor": actor_id or getattr(admin_user, "pk", None), "target_user_id": target_user_id, "capability_uuid": capability_id, "payment_id": payment_id, "scenario": scenario, "result": result, "action": action, "timestamp": timezone.now().isoformat()}
    metadata["reason"] = str(reason or "")[:500]
    safe_reason = sanitize_audit_metadata({"reason": reason}).get("reason", "")
    create_audit_log(request=request, admin_user=admin_user, action=canary_action(action), object_type="production_canary", object_id=capability_id or target_user_id or "", target_user_id=target_user_id, mode="production_canary", reason=safe_reason, after=sanitize_audit_metadata(metadata))


def _json_safe(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, models.Model):
        return _json_safe(value.pk)
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    return value


def serialize_instance(instance) -> dict:
    data = {}
    for field in instance._meta.concrete_fields:
        data[field.name] = _json_safe(getattr(instance, field.name))
    return data


def get_client_ip(request) -> str:
    if request is None:
        return ""
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "")


def create_audit_log(
    *,
    request=None,
    admin_user,
    action: str,
    object_type: str,
    object_id: str | int | None,
    target_user_id: int | None = None,
    mode: str = "",
    reason: str = "",
    before=None,
    after=None,
) -> None:
    from audit_log.models import AdminAuditLog

    AdminAuditLog.objects.create(
        admin_user=admin_user,
        action=action,
        object_type=object_type,
        object_id=str(object_id or ""),
        target_user_id=target_user_id,
        mode=mode,
        reason=reason,
        before=_json_safe(before or {}),
        after=_json_safe(after or {}),
        ip_address=get_client_ip(request),
        user_agent=(request.META.get("HTTP_USER_AGENT", "")[:1000] if request is not None else ""),
    )
