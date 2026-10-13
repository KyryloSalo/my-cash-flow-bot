from __future__ import annotations

import json
import secrets
from django.db import transaction
from django.http import JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.views.decorators.http import require_GET, require_POST
from miniapp.auth import MiniAppSessionError, get_session_user
from miniapp.capture_service import (
    CREDENTIAL_TTL, CaptureValidationError, approve_pairing, authenticate_submit_credential,
    ingest_capture_event, issue_pairing_challenge, revoke_connection,
)
from miniapp.models import CaptureConnection, CaptureEvent
from miniapp.models import DraftAction
from miniapp.services import build_transaction_draft, commit_transaction_draft, MiniAppTransactionError
from miniapp.views import _commit_draft_once, _get_session_user_or_response, _persist_draft_action


def _capture_user(request):
    result = _get_session_user_or_response(request)
    return result if not hasattr(result, "tg_user_id") else result


def _user(request):
    try:
        return get_session_user(request)
    except MiniAppSessionError:
        return None


def _error(exc, status=400):
    code = getattr(exc, "code", "invalid_request")
    message = str(exc) if not isinstance(exc, KeyError) else "required field is missing"
    return JsonResponse({"error": {"code": code, "message": message}}, status=status)


def _json_object(request):
    try:
        body = json.loads(request.body or b"{}")
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CaptureValidationError("invalid_json", "request body must be valid JSON") from exc
    if not isinstance(body, dict):
        raise CaptureValidationError("invalid_json", "request body must be a JSON object")
    return body


@csrf_protect
@require_POST
def pairing_start(request):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    try:
        body = _json_object(request)
        challenge, _ = issue_pairing_challenge(tg_user_id=int(user.tg_user_id), client_proof=str(body["client_proof"]), rate_key=request.META.get("REMOTE_ADDR", "unknown"))
        return JsonResponse({"challenge": challenge, "expires_in": 300})
    except (KeyError, CaptureValidationError) as exc:
        return _error(exc)


@csrf_protect
@require_POST
def pairing_approve(request):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    try:
        body = _json_object(request)
        connection, credential = approve_pairing(tg_user_id=int(user.tg_user_id), challenge=str(body["challenge"]), client_proof=str(body["client_proof"]), platform=str(body["platform"]), rate_key=request.META.get("REMOTE_ADDR", "unknown"))
        expires_at = connection.credential_issued_at + CREDENTIAL_TTL
        return JsonResponse({"connection_id": connection.pk, "credential": credential, "scope": ["capture:submit"], "expires_at": expires_at.isoformat(), "expires_in": int((expires_at - timezone.now()).total_seconds())})
    except (KeyError, CaptureValidationError) as exc:
        return _error(exc, 409 if getattr(exc, "code", "") == "pairing_expired" else 400)


@csrf_exempt
@require_POST
def ingest(request):
    try:
        if len(request.body) > 16 * 1024:
            raise CaptureValidationError("request_too_large")
        credential = request.headers.get("Authorization", "").removeprefix("Bearer ")
        connection = authenticate_submit_credential(credential)
        event = ingest_capture_event(connection, json.loads(request.body))
        return JsonResponse({"capture_id": event.pk, "status": event.status}, status=200)
    except (UnicodeDecodeError, json.JSONDecodeError, CaptureValidationError) as exc:
        return _error(exc, 409 if getattr(exc, "code", "") == "event_payload_conflict" else 400)


@require_GET
def inbox(request):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    events = CaptureEvent.objects.filter(tg_user_id=user.tg_user_id).exclude(status="expired").order_by("-created_at")
    return JsonResponse({"events": [{"id": e.pk, "status": e.status, "amount": str(e.amount) if e.amount is not None else None, "currency": e.currency, "merchant": e.merchant, "source_revision": e.source_revision} for e in events]})


@csrf_protect
@require_POST
def review(request, event_id):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    with transaction.atomic():
        event = CaptureEvent.objects.select_for_update().filter(pk=event_id, tg_user_id=user.tg_user_id).first()
        if event is None:
            return JsonResponse({"error": {"code": "not_found"}}, status=404)
        if event.status not in {"pending", "needs_review", "reviewed"}:
            return JsonResponse({"error": {"code": "invalid_state"}}, status=409)
        try:
            body = _json_object(request)
            transaction_payload = body.get("transaction") or {}
            if not isinstance(transaction_payload, dict):
                raise CaptureValidationError("invalid_json", "transaction must be a JSON object")
            payload = dict(transaction_payload)
        except CaptureValidationError as exc:
            return _error(exc)
        payload.setdefault("kind", "expense")
        payload.setdefault("amount", str(event.amount or ""))
        payload.setdefault("currency", event.currency)
        payload.setdefault("comment", event.merchant or "")
        draft_id = secrets.token_urlsafe(16)
        try:
            draft = build_transaction_draft(user, payload, draft_id=draft_id)
        except MiniAppTransactionError as exc:
            return JsonResponse({"error": {"code": exc.code, "message": str(exc)}}, status=exc.status)
        if event.draft_id:
            DraftAction.objects.filter(tg_user_id=user.tg_user_id, draft_id=event.draft_id, status="draft").update(status="invalidated")
        event.draft_id = draft_id
        event.draft_payload = {"draft": draft, "source_revision": event.source_revision}
        _persist_draft_action(user=user, operation="capture_transaction", draft_id=draft_id, entry=event.draft_payload)
        event.status = "reviewed"
        event.save(update_fields=["draft_id", "draft_payload", "status", "updated_at"])
    return JsonResponse({"capture_id": event.pk, "draft_id": draft_id, "source_revision": event.source_revision, "draft": draft})


@csrf_protect
@require_POST
def confirm(request, event_id):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    try:
        body = _json_object(request)
        expected = int(body.get("source_revision") or 0)
    except (CaptureValidationError, TypeError, ValueError) as exc:
        error = exc if isinstance(exc, CaptureValidationError) else CaptureValidationError("invalid_source_revision")
        return _error(error)
    try:
        with transaction.atomic():
            event = CaptureEvent.objects.select_for_update().filter(pk=event_id, tg_user_id=user.tg_user_id).first()
            if event is None or event.status != "reviewed" or not event.draft_id:
                return JsonResponse({"error": {"code": "draft_missing"}}, status=404)
            if event.source_revision != expected:
                return JsonResponse({"error": {"code": "stale_source_revision"}}, status=409)
            entry = dict(event.draft_payload or {})
            result, idempotent = _commit_draft_once(
                user=user, operation="capture_transaction", draft_id=event.draft_id,
                idempotency_key=str(body.get("idempotency_key") or ""), entry=entry,
                commit=lambda: commit_transaction_draft(user, entry.get("draft") or {}),
            )
            event.status = "confirmed"
            event.save(update_fields=["status", "updated_at"])
    except MiniAppTransactionError as exc:
        return JsonResponse({"error": {"code": exc.code, "message": str(exc)}}, status=exc.status)
    return JsonResponse({"ok": True, "idempotent": idempotent, "result": result})


@csrf_protect
@require_POST
def dismiss(request, event_id):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    updated = CaptureEvent.objects.filter(pk=event_id, tg_user_id=user.tg_user_id, status__in=["pending", "needs_review", "reviewed"]).update(status="dismissed")
    return JsonResponse({"ok": bool(updated)})


@csrf_protect
@require_POST
def revoke(request, connection_id):
    user = _user(request)
    if not user:
        return JsonResponse({"error": {"code": "session_required"}}, status=401)
    connection = CaptureConnection.objects.filter(pk=connection_id, tg_user_id=user.tg_user_id).first()
    if not connection:
        return JsonResponse({"error": {"code": "not_found"}}, status=404)
    revoke_connection(connection)
    return JsonResponse({"ok": True})
