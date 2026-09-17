from __future__ import annotations

from typing import Iterable, Mapping

from miniapp.models import AcquisitionSession, FunnelEvent
from users.traffic import funnel_excluded_telegram_ids


PWA_FUNNEL_STAGES = (
    ("landing_view", "Landing view"),
    ("landing_primary_cta_click", "Primary CTA"),
    ("auth_success", "Auth success"),
    ("onboarding_confirmed", "Onboarding confirmed"),
    ("billing_consent_accepted", "Billing consent"),
    ("bind_invoice_created", "Checkout created"),
    ("payment_success", "Payment success"),
    ("first_transaction_confirmed", "First transaction"),
)


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator * 100 / denominator, 1)


def build_stage_rows(
    *,
    cohort_session_ids: Iterable[object],
    stage_session_ids: Mapping[str, Iterable[object]],
) -> list[dict[str, object]]:
    cohort = set(cohort_session_ids)
    rows: list[dict[str, object]] = []
    previous = len(cohort)
    for event_name, label in PWA_FUNNEL_STAGES:
        value = len(set(stage_session_ids.get(event_name, ())) & cohort)
        rows.append(
            {
                "event_name": event_name,
                "label": label,
                "value": value,
                "overall_percent": _percent(value, len(cohort)),
                "previous_percent": _percent(value, previous),
            }
        )
        previous = value
    return rows


def build_pwa_funnel_report_from_rows(
    *,
    session_rows: Iterable[Mapping[str, object]],
    event_rows: Iterable[Mapping[str, object]],
    include_test_users: bool,
    excluded_telegram_ids: set[int],
) -> dict[str, object]:
    sessions = list(session_rows)
    included: list[Mapping[str, object]] = []
    excluded_automation = 0
    excluded_internal = 0
    for session in sessions:
        if session.get("automation_status") == "automated":
            excluded_automation += 1
            continue
        user_id = session.get("user_id")
        internal = bool(session.get("is_test_user")) or user_id in excluded_telegram_ids
        if internal and not include_test_users:
            excluded_internal += 1
            continue
        included.append(session)

    cohort_ids = {session["id"] for session in included}
    stage_session_ids: dict[str, set[object]] = {
        event_name: set() for event_name, _label in PWA_FUNNEL_STAGES
    }
    for event in event_rows:
        event_name = str(event.get("event_name") or "")
        session_id = event.get("acquisition_session_id")
        if event_name in stage_session_ids and session_id in cohort_ids:
            stage_session_ids[event_name].add(session_id)

    return {
        "cohort_sessions": len(cohort_ids),
        "anonymous_unclassified": sum(session.get("user_id") is None for session in included),
        "excluded_internal": excluded_internal,
        "excluded_automation": excluded_automation,
        "stages": build_stage_rows(
            cohort_session_ids=cohort_ids,
            stage_session_ids=stage_session_ids,
        ),
    }


def build_pwa_funnel_report(
    *,
    start_at,
    end_at,
    include_test_users: bool,
) -> dict[str, object]:
    session_rows = list(
        AcquisitionSession.objects.filter(
            created_at__gte=start_at,
            created_at__lt=end_at,
        ).values(
            "id",
            "user_id",
            "user__admin_state__is_test_user",
            "automation_status",
        )
    )
    normalized_sessions = [
        {
            "id": row["id"],
            "user_id": row["user_id"],
            "is_test_user": bool(row["user__admin_state__is_test_user"]),
            "automation_status": row["automation_status"],
        }
        for row in session_rows
    ]
    session_ids = [row["id"] for row in normalized_sessions]
    event_rows = list(
        FunnelEvent.objects.filter(
            acquisition_session_id__in=session_ids,
            recorded_at__lt=end_at,
        ).values("acquisition_session_id", "event_name")
    )
    return build_pwa_funnel_report_from_rows(
        session_rows=normalized_sessions,
        event_rows=event_rows,
        include_test_users=include_test_users,
        excluded_telegram_ids=funnel_excluded_telegram_ids(),
    )
