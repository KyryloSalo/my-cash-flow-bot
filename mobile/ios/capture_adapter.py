"""Reference normalizer for the iOS manual-share feasibility path.

This module deliberately enables only ``ios_manual_share`` while the physical-device
B0 gate is open. It does not perform pairing, authentication, network submission, or
financial confirmation. Those belong to the shared capture-v1 foundation.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

_ALLOWED_SOURCES = {"ios_manual_share"}
_ALLOWED_BANKS = {"unknown", "monobank", "privatbank"}
_ALLOWED_OPERATION_HINTS = {"purchase", "transfer", "refund", "unknown"}
_CURRENCY_RE = re.compile(r"^[A-Z]{3,10}$")
_SENSITIVE_PATTERNS = (
    re.compile(r"\b(?:otp|код|code)\s*[:#-]?\s*\d{4,8}\b", re.IGNORECASE),
    re.compile(r"\b(?:\d[ -]?){12,19}\b"),
    re.compile(r"\b(?:cvv|cvc|pin)\b", re.IGNORECASE),
)
_EVENT_FIELDS = (
    "schema_version",
    "event_id",
    "source",
    "bank",
    "observed_at",
    "occurred_at",
    "amount",
    "currency",
    "merchant",
    "operation_hint",
    "parser_version",
)


class ValidationError(ValueError):
    """The candidate cannot safely be represented by capture schema v1."""


def _normalize_timestamp(value: str | None, *, required: bool) -> str | None:
    if value is None:
        if required:
            raise ValidationError("observed_at is required")
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValidationError("timestamp must be RFC3339") from exc
    if parsed.tzinfo is None:
        raise ValidationError("timestamp must include timezone")
    return value


def _normalize_amount(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    candidate = str(value).strip().replace(" ", "").replace(",", ".")
    try:
        amount = Decimal(candidate)
    except InvalidOperation as exc:
        raise ValidationError("amount must be a decimal string") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValidationError("amount must be positive")
    return format(amount, "f")


def _normalize_merchant(value: str | None) -> str | None:
    if value is None:
        return None
    merchant = " ".join(value.split())
    if not merchant:
        return None
    if len(merchant) > 160:
        raise ValidationError("merchant exceeds 160 characters")
    if any(pattern.search(merchant) for pattern in _SENSITIVE_PATTERNS):
        raise ValidationError("merchant contains sensitive content")
    return merchant


def build_capture_event(
    *,
    source: str,
    bank: str,
    observed_at: str,
    amount: str | None,
    currency: str | None,
    merchant: str | None,
    operation_hint: str,
    parser_version: str,
    occurred_at: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    if source not in _ALLOWED_SOURCES:
        raise ValidationError("automatic iOS source is disabled until the physical-device gate passes")
    if bank not in _ALLOWED_BANKS:
        raise ValidationError("bank must use the local allowlist")
    if operation_hint not in _ALLOWED_OPERATION_HINTS:
        raise ValidationError("unsupported operation_hint")
    if not parser_version or len(parser_version) > 64:
        raise ValidationError("parser_version is required and bounded")

    normalized_currency = currency.upper().strip() if currency else None
    if normalized_currency and not _CURRENCY_RE.fullmatch(normalized_currency):
        raise ValidationError("currency must be an uppercase ISO-style code")

    normalized_event_id = event_id or str(uuid.uuid4())
    try:
        uuid.UUID(normalized_event_id)
    except ValueError as exc:
        raise ValidationError("event_id must be a UUID") from exc

    return {
        "schema_version": 1,
        "event_id": normalized_event_id,
        "source": source,
        "bank": bank,
        "observed_at": _normalize_timestamp(observed_at, required=True),
        "occurred_at": _normalize_timestamp(occurred_at, required=False),
        "amount": _normalize_amount(amount),
        "currency": normalized_currency,
        "merchant": _normalize_merchant(merchant),
        "operation_hint": operation_hint,
        "parser_version": parser_version,
    }


def _payload_digest(event: dict[str, Any]) -> str:
    payload = {key: event.get(key) for key in _EVENT_FIELDS if key != "event_id"}
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class CaptureQueue:
    """Deterministic test/reference queue; not an iOS secure-storage implementation."""

    def __init__(self, path: Path):
        self.path = path

    def _read(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        value = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValidationError("queue storage is malformed")
        return value

    def _write(self, records: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(records, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self.path)

    def enqueue(self, candidate: dict[str, Any], *, local_key: str) -> dict[str, Any]:
        if not local_key or len(local_key) > 128:
            raise ValidationError("local_key is required and bounded")
        unknown_fields = set(candidate) - set(_EVENT_FIELDS)
        if unknown_fields:
            raise ValidationError("unknown fields are forbidden")
        event = build_capture_event(
            source=candidate["source"],
            bank=candidate["bank"],
            observed_at=candidate["observed_at"],
            occurred_at=candidate.get("occurred_at"),
            amount=candidate.get("amount"),
            currency=candidate.get("currency"),
            merchant=candidate.get("merchant"),
            operation_hint=candidate["operation_hint"],
            parser_version=candidate["parser_version"],
            event_id=candidate.get("event_id"),
        )
        records = self._read()
        digest = _payload_digest(event)
        existing = records.get(local_key)
        if existing:
            if existing["payload_digest"] != digest:
                raise ValidationError("payload conflict for existing local_key")
            return existing["event"]
        records[local_key] = {"payload_digest": digest, "event": event}
        self._write(records)
        return event

    def pending(self) -> list[dict[str, Any]]:
        return [record["event"] for record in self._read().values()]
