from __future__ import annotations

from datetime import datetime

from django.db import transaction

from miniapp.models import AcquisitionSession, FunnelEvent


def _delete_in_batches(queryset, *, batch_size: int) -> tuple[int, int]:
    deleted_total = 0
    batches = 0
    while True:
        row_ids = list(queryset.order_by("id").values_list("id", flat=True)[:batch_size])
        if not row_ids:
            break
        with transaction.atomic():
            deleted, _details = queryset.model.objects.filter(id__in=row_ids).delete()
        deleted_total += deleted
        batches += 1
    return deleted_total, batches


def purge_funnel_telemetry(
    *,
    cutoff: datetime,
    apply: bool,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Count or purge acquisition telemetry before a caller-supplied cutoff.

    Billing consent and payment records are deliberately outside this deletion graph.
    Sessions are removed only when they predate the cutoff and have no surviving event.
    """

    old_events = FunnelEvent.objects.filter(recorded_at__lt=cutoff)
    old_sessions = (
        AcquisitionSession.objects.filter(created_at__lt=cutoff)
        .exclude(events__recorded_at__gte=cutoff)
        .distinct()
    )
    event_count = old_events.count()
    session_count = old_sessions.count()
    if not apply:
        return {"events": event_count, "sessions": session_count, "batches": 0}

    _deleted_events, event_batches = _delete_in_batches(old_events, batch_size=batch_size)
    orphaned_sessions = AcquisitionSession.objects.filter(
        created_at__lt=cutoff,
        events__isnull=True,
    )
    _deleted_sessions, session_batches = _delete_in_batches(
        orphaned_sessions,
        batch_size=batch_size,
    )
    return {
        "events": event_count,
        "sessions": session_count,
        "batches": event_batches + session_batches,
    }
