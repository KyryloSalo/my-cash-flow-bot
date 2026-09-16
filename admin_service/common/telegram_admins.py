from __future__ import annotations

from django.conf import settings


def get_primary_admin_telegram_id() -> int | None:
    primary_admin_id = getattr(settings, "PRIMARY_ADMIN_TELEGRAM_ID", None)
    if primary_admin_id is not None:
        try:
            return int(primary_admin_id)
        except (TypeError, ValueError):
            pass

    admin_ids = getattr(settings, "ADMIN_TELEGRAM_IDS", []) or []
    if not admin_ids:
        return None

    try:
        return int(admin_ids[0])
    except (TypeError, ValueError):
        return None
