from __future__ import annotations

from typing import Any

import httpx

import config


class TransactionCancellationAPIError(Exception):
    def __init__(self, message: str, *, code: str = "internal_api_error", status: int = 503) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


async def _post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not config.BILLING_INTERNAL_TOKEN:
        raise TransactionCancellationAPIError("BILLING_INTERNAL_TOKEN not configured")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                f"{config.BILLING_INTERNAL_BASE_URL.rstrip('/')}{path}",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": config.BILLING_INTERNAL_TOKEN,
                },
            )
        data = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise TransactionCancellationAPIError("Admin service is unavailable") from exc
    if response.status_code >= 400 or not data.get("ok"):
        error = data.get("error") if isinstance(data.get("error"), dict) else {}
        raise TransactionCancellationAPIError(
            str(error.get("message") or f"HTTP {response.status_code}"),
            code=str(error.get("code") or "internal_api_error"),
            status=response.status_code,
        )
    return data


async def list_recent_transactions(*, actor_tg_user_id: int, limit: int = 10) -> dict[str, Any]:
    return await _post_json(
        "/internal/transactions/recent",
        {"actor_tg_user_id": actor_tg_user_id, "limit": max(1, min(10, int(limit)))},
    )


async def build_transaction_void_draft(*, actor_tg_user_id: int, transaction_id: int) -> dict[str, Any]:
    return await _post_json(
        "/internal/transactions/void/draft",
        {"actor_tg_user_id": actor_tg_user_id, "transaction_id": transaction_id},
    )


async def confirm_transaction_void(
    *,
    actor_tg_user_id: int,
    draft: dict[str, Any],
    idempotency_key: str,
) -> dict[str, Any]:
    return await _post_json(
        "/internal/transactions/void/confirm",
        {
            "actor_tg_user_id": actor_tg_user_id,
            "draft": draft,
            "idempotency_key": idempotency_key,
        },
    )
