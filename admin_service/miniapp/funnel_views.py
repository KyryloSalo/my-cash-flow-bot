from __future__ import annotations

import json

from django.http import HttpRequest, JsonResponse
from django.middleware.csrf import get_token
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from miniapp.auth import MiniAppSessionError, get_session_user
from miniapp.funnel import (
    FunnelValidationError,
    get_or_create_acquisition_session,
    ingest_client_event,
    link_acquisition_session,
)
from miniapp.services import resolve_access
from subscriptions.consent import accept_billing_consent, build_server_consent_terms

MAX_FUNNEL_EVENT_BODY_BYTES = 4096
ATTRIBUTION_QUERY_FIELDS = (
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "referral_code",
    "landing_variant",
)


def _error_response(error: FunnelValidationError) -> JsonResponse:
    return JsonResponse(
        {"error": {"code": error.code, "message": str(error)}},
        status=error.status,
    )


def _link_authenticated_request(request: HttpRequest) -> None:
    try:
        user = get_session_user(request)
    except MiniAppSessionError:
        return
    link_acquisition_session(request, user=user)


@never_cache
@ensure_csrf_cookie
@require_GET
def funnel_session(request: HttpRequest) -> JsonResponse:
    attribution = {
        field: request.GET.get(field)
        for field in ATTRIBUTION_QUERY_FIELDS
        if request.GET.get(field)
    }
    acquisition = get_or_create_acquisition_session(request, attribution=attribution)
    try:
        _link_authenticated_request(request)
    except FunnelValidationError as exc:
        return _error_response(exc)
    response = JsonResponse(
        {
            "ok": True,
            "funnel_version": acquisition.funnel_version,
            "landing_variant": acquisition.last_landing_variant,
            "platform": acquisition.platform,
            "container": acquisition.container,
            "csrf_token": get_token(request),
        }
    )
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response["Vary"] = "Cookie"
    return response


@never_cache
@require_POST
def funnel_event(request: HttpRequest) -> JsonResponse:
    if request.content_type != "application/json":
        return _error_response(
            FunnelValidationError("invalid_content_type", "Use application/json.")
        )
    if len(request.body) > MAX_FUNNEL_EVENT_BODY_BYTES:
        return _error_response(
            FunnelValidationError("payload_too_large", "Funnel event payload is too large.", status=413)
        )
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response(FunnelValidationError("invalid_json", "A valid JSON object is required."))
    try:
        _link_authenticated_request(request)
        _event, created = ingest_client_event(request, payload=payload)
    except FunnelValidationError as exc:
        return _error_response(exc)
    response = JsonResponse({"ok": True, "created": created})
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    return response


@never_cache
@require_POST
def billing_consent(request: HttpRequest) -> JsonResponse:
    if request.content_type != "application/json":
        return _error_response(FunnelValidationError("invalid_content_type", "Use application/json."))
    if len(request.body) > MAX_FUNNEL_EVENT_BODY_BYTES:
        return _error_response(
            FunnelValidationError("payload_too_large", "Billing consent payload is too large.", status=413)
        )
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error_response(FunnelValidationError("invalid_json", "A valid JSON object is required."))
    if not isinstance(payload, dict) or set(payload) - {"accepted", "idempotency_key", "copy_locale"}:
        return _error_response(
            FunnelValidationError("invalid_payload", "Unsupported billing consent fields.")
        )
    if payload.get("accepted") is not True:
        return _error_response(
            FunnelValidationError("consent_required", "Explicit billing consent is required.")
        )
    try:
        user = get_session_user(request)
    except MiniAppSessionError as exc:
        return JsonResponse(
            {"error": {"code": "auth_required", "message": str(exc)}},
            status=401,
        )
    access = resolve_access(user)
    trial_days = max(int(access.get("promo_trial_days") or access.get("trial_days") or 30), 1)
    try:
        acquisition = get_or_create_acquisition_session(request)
        acquisition = link_acquisition_session(request, user=user) or acquisition
        consent, created = accept_billing_consent(
            user=user,
            acquisition=acquisition,
            idempotency_key=str(payload.get("idempotency_key") or ""),
            trial_days=trial_days,
            copy_locale=str(payload.get("copy_locale") or getattr(user, "lang", "uk") or "uk"),
        )
    except FunnelValidationError as exc:
        return _error_response(exc)
    response = JsonResponse(
        {
            "ok": True,
            "created": created,
            "consent_id": str(consent.pk),
            "terms": build_server_consent_terms(
                trial_days=consent.trial_days,
                copy_locale=consent.copy_locale,
            ),
        }
    )
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    return response
