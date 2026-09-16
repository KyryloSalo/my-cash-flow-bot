from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.conf import settings


class MiniAppHelpError(Exception):
    def __init__(self, message: str, *, status: int = 503) -> None:
        super().__init__(message)
        self.status = status


def request_help_content(*, locale: str, topic_id: str = "") -> dict[str, object]:
    token = str(settings.BILLING_INTERNAL_TOKEN or "").strip()
    if not token:
        raise MiniAppHelpError("Help service is temporarily unavailable.")

    payload = json.dumps({"locale": locale, "topic_id": topic_id}).encode("utf-8")
    request = Request(
        f"{str(settings.MINIAPP_BOT_INTERNAL_BASE_URL).rstrip('/')}/internal/miniapp/help",
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
        if exc.code == 404:
            raise MiniAppHelpError("Help topic was not found.", status=404) from exc
        raise MiniAppHelpError("Help service is temporarily unavailable.") from exc
    except (URLError, TimeoutError, OSError) as exc:
        raise MiniAppHelpError("Help service is temporarily unavailable.") from exc

    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MiniAppHelpError("Help service returned an invalid response.") from exc
    if not isinstance(parsed, dict):
        raise MiniAppHelpError("Help service returned an invalid response.")
    return parsed
