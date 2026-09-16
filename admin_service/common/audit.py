from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from django.db import models


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
