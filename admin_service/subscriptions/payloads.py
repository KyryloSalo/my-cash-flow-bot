from __future__ import annotations

from typing import Any


REDACTED = "[redacted]"
_SENSITIVE_MONOBANK_KEYS = frozenset({"cardtoken", "pan"})


def sanitize_monobank_payload(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[Any, Any] = {}
        for key, item in value.items():
            if str(key).lower() in _SENSITIVE_MONOBANK_KEYS:
                sanitized[key] = REDACTED if item not in (None, "") else item
            else:
                sanitized[key] = sanitize_monobank_payload(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_monobank_payload(item) for item in value]
    if isinstance(value, tuple):
        return tuple(sanitize_monobank_payload(item) for item in value)
    return value
