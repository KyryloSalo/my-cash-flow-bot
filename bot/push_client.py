from __future__ import annotations

from typing import Any

import httpx

import config


async def dispatch_push_event(
    *,
    user_id: int,
    event_type: str,
    idempotency_key: str,
    context: dict[str, Any] | None = None,
) -> bool:
    """Return True only when at least one PWA endpoint accepted the push."""

    token = str(getattr(config, "PUSH_INTERNAL_TOKEN", "") or "")
    base_url = str(getattr(config, "PUSH_INTERNAL_BASE_URL", "") or "")
    if not token or not base_url:
        return False
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{base_url.rstrip('/')}/internal/push/event",
                json={
                    "user_id": int(user_id),
                    "event_type": str(event_type),
                    "idempotency_key": str(idempotency_key),
                    "context": dict(context or {}),
                },
                headers={"X-Internal-Token": token},
            )
        if response.status_code != 200:
            return False
        return int(response.json().get("delivered") or 0) > 0
    except (httpx.HTTPError, ValueError, TypeError):
        return False
