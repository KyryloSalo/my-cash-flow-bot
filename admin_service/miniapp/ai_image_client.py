from __future__ import annotations

import base64
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.conf import settings


class MiniAppAiImageError(Exception):
    def __init__(self, message: str, *, status: int = 503) -> None:
        super().__init__(message)
        self.status = status


def request_ai_image_parse(
    *,
    image_bytes: bytes,
    mime_type: str,
    default_currency: str,
    catalog: dict[str, object],
) -> dict[str, object]:
    token = str(settings.BILLING_INTERNAL_TOKEN or "").strip()
    if not token:
        raise MiniAppAiImageError("Screenshot recognition is temporarily unavailable.")

    payload = json.dumps(
        {
            "image_base64": base64.b64encode(image_bytes).decode("ascii"),
            "mime_type": mime_type,
            "default_currency": default_currency,
            "accounts": catalog.get("accounts", []),
            "expense_categories": catalog.get("expense_categories", []),
            "income_categories": catalog.get("income_categories", []),
        }
    ).encode("utf-8")
    request = Request(
        f"{str(settings.MINIAPP_BOT_INTERNAL_BASE_URL).rstrip('/')}/internal/miniapp/parse-image",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Internal-Token": token,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=int(settings.MINIAPP_BOT_INTERNAL_AI_TIMEOUT_SECONDS)) as response:
            body = response.read()
    except HTTPError as exc:
        if exc.code in {400, 413}:
            raise MiniAppAiImageError("Screenshot could not be processed.", status=400) from exc
        raise MiniAppAiImageError("Screenshot recognition is temporarily unavailable.") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise MiniAppAiImageError("Screenshot recognition is temporarily unavailable.") from exc

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MiniAppAiImageError("Screenshot recognition returned an invalid response.") from exc
    if not isinstance(parsed, dict):
        raise MiniAppAiImageError("Screenshot recognition returned an invalid response.")
    return parsed
