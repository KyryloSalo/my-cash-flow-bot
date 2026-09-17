from __future__ import annotations

from django.conf import settings


FUNNEL_INTERNAL_ID_SETTINGS = (
    "ADMIN_TEST_TELEGRAM_IDS",
    "MINIAPP_OPERATOR_TELEGRAM_IDS",
    "ADMIN_TELEGRAM_IDS",
    "FUNNEL_EXCLUDED_TELEGRAM_IDS",
)


def funnel_excluded_telegram_ids() -> set[int]:
    result: set[int] = set()
    for setting_name in FUNNEL_INTERNAL_ID_SETTINGS:
        for value in getattr(settings, setting_name, ()) or ():
            try:
                telegram_id = int(value)
            except (TypeError, ValueError):
                continue
            if telegram_id > 0:
                result.add(telegram_id)
    return result
