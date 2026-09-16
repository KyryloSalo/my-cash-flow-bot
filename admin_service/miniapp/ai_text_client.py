from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.conf import settings


class MiniAppAiTextError(Exception):
    def __init__(self, message: str, *, status: int = 503) -> None:
        super().__init__(message)
        self.status = status


def request_ai_text_parse(*, text: str, default_currency: str) -> dict[str, object]:
    token = str(settings.BILLING_INTERNAL_TOKEN or "").strip()
    if not token:
        raise MiniAppAiTextError("Text recognition is temporarily unavailable.")

    payload = json.dumps({"text": text, "default_currency": default_currency}).encode("utf-8")
    request = Request(
        f"{str(settings.MINIAPP_BOT_INTERNAL_BASE_URL).rstrip('/')}/internal/miniapp/parse-text",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Internal-Token": token,
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=int(settings.MINIAPP_BOT_INTERNAL_TIMEOUT_SECONDS)) as response:
            body = response.read()
    except HTTPError as exc:
        if exc.code == 400:
            raise MiniAppAiTextError("Text could not be processed.", status=400) from exc
        raise MiniAppAiTextError("Text recognition is temporarily unavailable.") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise MiniAppAiTextError("Text recognition is temporarily unavailable.") from exc

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MiniAppAiTextError("Text recognition returned an invalid response.") from exc
    if not isinstance(parsed, dict):
        raise MiniAppAiTextError("Text recognition returned an invalid response.")
    return parsed
