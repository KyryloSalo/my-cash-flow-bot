from celery import shared_task
from django.conf import settings

from .services import finalize_due_profiles, process_pending_events


@shared_task(name="gamification.process_events", ignore_result=False)
def process_gamification_events_task() -> dict[str, int | bool]:
    if not settings.GAMIFICATION_PROCESSING_ENABLED:
        return {"disabled": True, "checked": 0, "processed": 0, "failed": 0}
    return process_pending_events(limit=200)


@shared_task(name="gamification.finalize_due_profiles", ignore_result=False)
def finalize_due_profiles_task() -> dict[str, int | bool]:
    if not settings.GAMIFICATION_PROCESSING_ENABLED:
        return {"enabled": False, "profiles": 0, "days": 0, "failed": 0}
    result = finalize_due_profiles()
    return {"enabled": True, **result}
