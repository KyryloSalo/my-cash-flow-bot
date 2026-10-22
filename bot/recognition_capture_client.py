from __future__ import annotations

import asyncio
from typing import Any

import httpx

import config


class RecognitionCaptureError(RuntimeError):
    pass


class RecognitionCaptureClient:
    def __init__(self, *, timeout: float = 15.0, retries: int = 2) -> None:
        self.timeout = timeout
        self.retries = max(0, retries)

    @property
    def url(self) -> str:
        return f"{config.BILLING_INTERNAL_BASE_URL.rstrip('/')}/internal/recognition/photo"

    def _headers(self) -> dict[str, str]:
        if not config.BILLING_INTERNAL_TOKEN:
            raise RecognitionCaptureError("internal recognition capture is not configured")
        return {"X-Internal-Token": config.BILLING_INTERNAL_TOKEN}

    async def _request(self, method: str, **kwargs: Any) -> dict[str, Any]:
        headers = dict(kwargs.pop("headers", {}))
        headers.update(self._headers())
        for attempt in range(self.retries + 1):
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.request(method, self.url, headers=headers, **kwargs)
                if response.status_code in {401, 403}:
                    raise RecognitionCaptureError("internal recognition capture rejected")
                response.raise_for_status()
                payload = response.json()
                return payload if isinstance(payload, dict) else {}
            except RecognitionCaptureError:
                raise
            except (httpx.HTTPError, ValueError) as exc:
                if attempt >= self.retries:
                    raise RecognitionCaptureError("internal recognition capture unavailable") from exc
                await asyncio.sleep(0.05 * (attempt + 1))
        raise RecognitionCaptureError("internal recognition capture unavailable")

    async def create(self, *, user_id: int, attempt_id: str, image_bytes: bytes, mime_type: str) -> int | None:
        payload = await self._request(
            "POST",
            files={"image": ("photo", image_bytes, mime_type)},
            data={"action": "create", "user_id": str(user_id), "attempt_id": attempt_id},
        )
        capture_id = payload.get("capture_id")
        return int(capture_id) if capture_id is not None else None

    async def complete(
        self,
        *,
        user_id: int,
        attempt_id: str,
        outcome: str,
        normalized_result: dict[str, Any] | None = None,
        error_code: str | None = None,
    ) -> int | None:
        payload = await self._request(
            "POST",
            json={
                "action": "complete",
                "user_id": user_id,
                "attempt_id": attempt_id,
                "outcome": outcome,
                "normalized_result": normalized_result or {},
                **({"error_code": error_code} if error_code is not None else {}),
            },
        )
        capture_id = payload.get("capture_id")
        return int(capture_id) if capture_id is not None else None
