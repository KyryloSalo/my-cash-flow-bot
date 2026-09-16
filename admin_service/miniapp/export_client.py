from __future__ import annotations

import json
from datetime import date
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from django.conf import settings


class MiniAppExportError(RuntimeError):
    def __init__(self, message: str, *, status: int = 502) -> None:
        super().__init__(message)
        self.status = status


def _request_export(
    *,
    telegram_user_id: int,
    date_from: date,
    date_to_exclusive: date,
    title: str,
    endpoint: str,
    accept: str,
) -> bytes:
    token = str(getattr(settings, "BILLING_INTERNAL_TOKEN", "") or "").strip()
    if not token:
        raise MiniAppExportError("Export service is not configured.", status=503)

    base_url = str(getattr(settings, "MINIAPP_EXPORT_INTERNAL_BASE_URL", "") or "").rstrip("/")
    if not base_url:
        raise MiniAppExportError("Export service URL is not configured.", status=503)

    payload = json.dumps(
        {
            "telegram_user_id": int(telegram_user_id),
            "date_from": date_from.isoformat(),
            "date_to_exclusive": date_to_exclusive.isoformat(),
            "title": str(title or "").strip()[:120],
        }
    ).encode("utf-8")
    request = Request(
        f"{base_url}{endpoint}",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": accept,
            "X-Internal-Token": token,
        },
        method="POST",
    )
    timeout = max(1, int(getattr(settings, "MINIAPP_EXPORT_INTERNAL_TIMEOUT_SECONDS", 90) or 90))
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310 - configured internal service URL
            return response.read()
    except HTTPError as exc:
        if exc.code == 404:
            raise MiniAppExportError("No operations for the selected export period.", status=404) from exc
        if exc.code in {401, 403, 503}:
            raise MiniAppExportError("Export service is temporarily unavailable.", status=503) from exc
        raise MiniAppExportError("Could not create the export file.") from exc
    except URLError as exc:
        raise MiniAppExportError("Export service is temporarily unavailable.", status=503) from exc


def request_export_xlsx(
    *,
    telegram_user_id: int,
    date_from: date,
    date_to_exclusive: date,
    title: str,
) -> bytes:
    return _request_export(
        telegram_user_id=telegram_user_id,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
        title=title,
        endpoint="/internal/miniapp/export",
        accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def request_export_csv(
    *,
    telegram_user_id: int,
    date_from: date,
    date_to_exclusive: date,
    title: str,
) -> bytes:
    return _request_export(
        telegram_user_id=telegram_user_id,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
        title=title,
        endpoint="/internal/miniapp/export-csv",
        accept="text/csv",
    )
