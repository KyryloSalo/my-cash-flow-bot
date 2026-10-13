from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
import hmac
import secrets
from uuid import UUID
from django.core.cache import cache
from django.utils import timezone
from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils.dateparse import parse_datetime

from miniapp.models import CaptureConnection, CaptureEvent

ALLOWED_SOURCES = {"android_notification", "ios_notification_automation", "ios_wallet_automation", "ios_manual_share"}
ALLOWED_HINTS = {"purchase", "transfer", "refund", "unknown"}
ALLOWED_BANKS = {"mono", "privat24", "unknown"}
ALLOWED_PLATFORMS = {"android"}
PLATFORM_SOURCES = {"android": {"android_notification"}}


@dataclass
class CaptureValidationError(ValueError):
    code: str
    message: str = "invalid capture event"

    def __str__(self):
        return self.message


def _validate(payload: dict) -> dict:
    if not isinstance(payload, dict) or any(key in payload for key in ("raw", "raw_notification", "notification_text", "otp", "pan")):
        raise CaptureValidationError("raw_payload_forbidden", "raw notification data is not accepted")
    required = {"schema_version", "event_id", "source", "bank", "observed_at", "occurred_at", "amount", "currency", "merchant", "operation_hint", "parser_version"}
    if set(payload) != required:
        raise CaptureValidationError("schema_invalid")
    if payload["schema_version"] != 1 or payload["source"] not in ALLOWED_SOURCES or payload["bank"] not in ALLOWED_BANKS or payload["operation_hint"] not in ALLOWED_HINTS:
        raise CaptureValidationError("schema_invalid")
    if not isinstance(payload.get("event_id"), str):
        raise CaptureValidationError("schema_invalid")
    try:
        UUID(payload["event_id"])
    except (ValueError, AttributeError):
        raise CaptureValidationError("schema_invalid")
    if not isinstance(payload["observed_at"], str):
        raise CaptureValidationError("schema_invalid")
    observed = parse_datetime(payload["observed_at"])
    if observed is None or observed.tzinfo is None:
        raise CaptureValidationError("schema_invalid")
    if payload["occurred_at"] is not None:
        if not isinstance(payload["occurred_at"], str):
            raise CaptureValidationError("schema_invalid")
        occurred = parse_datetime(payload["occurred_at"])
        if occurred is None or occurred.tzinfo is None:
            raise CaptureValidationError("schema_invalid")
    amount = payload["amount"]
    if amount is not None:
        if not isinstance(amount, str) or not __import__("re").fullmatch(r"(?:0|[1-9]\d*)(?:\.\d{1,2})?", amount):
            raise CaptureValidationError("schema_invalid")
        try:
            amount_decimal = Decimal(amount)
        except (InvalidOperation, TypeError):
            raise CaptureValidationError("schema_invalid")
        if amount_decimal <= 0 or amount_decimal.as_tuple().exponent < -2:
            raise CaptureValidationError("schema_invalid")
        amount = format(amount_decimal, "f")
    if payload["currency"] is not None and (not isinstance(payload["currency"], str) or len(payload["currency"]) != 3 or not payload["currency"].isalpha()):
        raise CaptureValidationError("schema_invalid")
    merchant = payload["merchant"]
    if merchant is not None and (not isinstance(merchant, str) or len(merchant) > 160):
        raise CaptureValidationError("schema_invalid")
    return {**payload, "amount": amount}


def capture_enabled_for_user(tg_user_id: int, *, platform: str | None = None) -> bool:
    if not getattr(settings, "AUTO_CAPTURE_ENABLED", False):
        return False
    allowlist = {int(value) for value in getattr(settings, "AUTO_CAPTURE_ALLOWLIST_TG_IDS", ())}
    if int(tg_user_id) not in allowlist:
        return False
    if platform is None:
        return True
    return platform in ALLOWED_PLATFORMS and bool(
        getattr(settings, f"AUTO_CAPTURE_{platform.upper()}_ENABLED", False)
    )


def ingest_capture_event(connection: CaptureConnection, payload: dict) -> CaptureEvent:
    if connection.status != "active" or connection.revoked_at is not None:
        raise CaptureValidationError("invalid_credential", "capture connection is not active")
    if not capture_enabled_for_user(connection.tg_user_id, platform=connection.platform):
        raise CaptureValidationError("capture_disabled", "capture is disabled")
    normalized = _validate(payload)
    if normalized["source"] not in PLATFORM_SOURCES.get(connection.platform, set()):
        raise CaptureValidationError("source_platform_mismatch", "capture source does not match connection platform")
    fingerprint = hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    conflict = False
    try:
        with transaction.atomic():
            event = CaptureEvent.objects.select_for_update().filter(connection=connection, event_id=normalized["event_id"]).first()
            if event:
                if event.payload_fingerprint != fingerprint:
                    if event.status not in {"confirmed", "dismissed", "expired"}:
                        event.status = "needs_review"
                        event.source_revision += 1
                        event.save(update_fields=["status", "source_revision", "updated_at"])
                    conflict = True
                else:
                    return event
                if conflict:
                    pass
            else:
                status = "pending" if normalized["operation_hint"] == "purchase" and normalized["amount"] else "needs_review"
                event = CaptureEvent.objects.create(connection=connection, tg_user_id=connection.tg_user_id, event_id=normalized["event_id"], source=normalized["source"], bank=normalized["bank"], observed_at=parse_datetime(normalized["observed_at"]), occurred_at=parse_datetime(normalized["occurred_at"]) if normalized["occurred_at"] else None, amount=normalized["amount"], currency=normalized["currency"], merchant=normalized["merchant"], operation_hint=normalized["operation_hint"], parser_version=normalized["parser_version"], payload=normalized, payload_fingerprint=fingerprint, status=status)
    except IntegrityError:
        return CaptureEvent.objects.get(connection=connection, event_id=normalized["event_id"])
    if conflict:
        raise CaptureValidationError("event_payload_conflict", "event payload changed")
    return event

PAIRING_TTL = timedelta(minutes=5)
CREDENTIAL_TTL = timedelta(days=90)
RATE_LIMIT = 60
PAIRING_START_LIMIT = 5
PAIRING_APPROVE_LIMIT = 10

def _limit(scope: str, value: str, limit: int, window: int = 300) -> None:
    key = f"capture-limit:{scope}:{value}"
    count = cache.incr(key) if not cache.add(key, 1, window) else 1
    if count > limit:
        raise CaptureValidationError("rate_limited", "too many pairing attempts")


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def issue_pairing_challenge(*, tg_user_id: int, client_proof: str, rate_key: str | None = None) -> tuple[str, str]:
    if not client_proof or len(client_proof) > 256:
        raise CaptureValidationError("invalid_proof")
    _limit("start-user", str(tg_user_id), PAIRING_START_LIMIT)
    if rate_key:
        _limit("start-ip", rate_key, PAIRING_START_LIMIT)
    challenge = secrets.token_urlsafe(32)
    cache.set(f"capture-pair:{challenge}", {"tg_user_id": tg_user_id, "proof": _hash_secret(client_proof)}, int(PAIRING_TTL.total_seconds()))
    return challenge, challenge


def approve_pairing(*, tg_user_id: int, challenge: str, client_proof: str, platform: str, rate_key: str | None = None) -> tuple[CaptureConnection, str]:
    if platform not in ALLOWED_PLATFORMS:
        raise CaptureValidationError("unsupported_platform")
    _limit("approve-user", str(tg_user_id), PAIRING_APPROVE_LIMIT)
    if rate_key:
        _limit("approve-ip", rate_key, PAIRING_APPROVE_LIMIT)
    record = cache.get(f"capture-pair:{challenge}")
    if not record or record["tg_user_id"] != tg_user_id or not hmac.compare_digest(record["proof"], _hash_secret(client_proof)):
        raise CaptureValidationError("pairing_expired", "pairing challenge is expired or invalid")
    if not cache.add(f"capture-pair-consumed:{challenge}", 1, int(PAIRING_TTL.total_seconds())):
        raise CaptureValidationError("pairing_expired")
    cache.delete(f"capture-pair:{challenge}")
    credential = secrets.token_urlsafe(32)
    connection = CaptureConnection.objects.create(tg_user_id=tg_user_id, platform=platform, status="active", credential_hash=_hash_secret(credential))
    return connection, credential


def authenticate_submit_credential(credential: str, *, rate_key: str | None = None) -> CaptureConnection:
    if not credential or len(credential) > 256:
        raise CaptureValidationError("invalid_credential", "invalid submit credential")
    if rate_key:
        _limit("ingest-ip", rate_key, 120, window=300)
    connection = CaptureConnection.objects.filter(status="active", revoked_at__isnull=True).order_by("id")
    for candidate in connection:
        if candidate.credential_issued_at + CREDENTIAL_TTL <= timezone.now():
            continue
        if hmac.compare_digest(candidate.credential_hash, _hash_secret(credential)):
            key = f"capture-rate:{candidate.pk}:{timezone.now():%Y%m%d%H%M}"
            count = cache.incr(key) if not cache.add(key, 1, 120) else 1
            if count > RATE_LIMIT:
                raise CaptureValidationError("rate_limited", "too many capture events")
            return candidate
    _limit("ingest-invalid", _hash_secret(credential)[:16], 10, window=300)
    raise CaptureValidationError("invalid_credential", "invalid submit credential")


def revoke_connection(connection: CaptureConnection) -> None:
    connection.status = "revoked"
    connection.revoked_at = timezone.now()
    connection.save(update_fields=["status", "revoked_at", "updated_at"])


def purge_capture_data(tg_user_id: int) -> None:
    CaptureEvent.objects.filter(tg_user_id=tg_user_id).delete()
    CaptureConnection.objects.filter(tg_user_id=tg_user_id).delete()
