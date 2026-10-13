from __future__ import annotations

import json
from urllib.parse import quote
from uuid import uuid4

from django.http import HttpRequest, JsonResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from miniapp.account_deletion import (
    ACCOUNT_DELETION_CHALLENGE_TTL_SECONDS,
    ACCOUNT_DELETION_CONFIRMATION_TEXT,
    AccountDeletionChallengeError,
    consume_account_deletion_challenge,
    issue_account_deletion_challenge,
)
from miniapp.auth import (
    MiniAppSessionError,
    get_session_user,
    logout_session,
    sensitive_action_auth_is_fresh,
)
from users.services import (
    SelfServiceAccountDeletionError,
    delete_own_account,
    validate_self_service_account_deletion,
)


def _reauth_url() -> str:
    return_to = "/app/?screen=settings&account-deletion=1"
    return f"/app/auth/telegram/start?next={quote(return_to, safe='/')}"


@never_cache
@require_GET
def account_deletion_preflight(request: HttpRequest) -> JsonResponse:
    try:
        user = get_session_user(request)
    except MiniAppSessionError as exc:
        return JsonResponse(
            {"error": {"code": "session_required", "message": str(exc)}},
            status=401,
        )
    if not sensitive_action_auth_is_fresh(request):
        return JsonResponse(
            {
                "error": {
                    "code": "reauth_required",
                    "message": "Confirm your identity again before deleting the account.",
                    "reauth_url": _reauth_url(),
                }
            },
            status=409,
        )
    try:
        validate_self_service_account_deletion(int(user.tg_user_id))
    except SelfServiceAccountDeletionError as exc:
        return _error(exc.code, str(exc), status=exc.status)
    return JsonResponse(
        {
            "ok": True,
            "challenge": issue_account_deletion_challenge(request, user=user),
            "confirmation_text": ACCOUNT_DELETION_CONFIRMATION_TEXT,
            "expires_in_seconds": ACCOUNT_DELETION_CHALLENGE_TTL_SECONDS,
            "reauth_url": _reauth_url(),
            "public_deletion_url": "https://vydno.capital/delete-account.html",
        }
    )


def _error(code: str, message: str, *, status: int = 400, **extra) -> JsonResponse:
    return JsonResponse({"error": {"code": code, "message": message, **extra}}, status=status)


@never_cache
@require_POST
def account_deletion_confirm(request: HttpRequest) -> JsonResponse:
    try:
        user = get_session_user(request)
    except MiniAppSessionError as exc:
        return _error("session_required", str(exc), status=401)
    if not sensitive_action_auth_is_fresh(request):
        return _error(
            "reauth_required",
            "Confirm your identity again before deleting the account.",
            status=409,
            reauth_url=_reauth_url(),
        )
    if request.content_type != "application/json":
        return _error("invalid_content_type", "Use application/json.")
    if len(request.body) > 4096:
        return _error("payload_too_large", "Deletion request is too large.", status=413)
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return _error("invalid_json", "A valid JSON object is required.")
    allowed_fields = {"challenge", "confirmation_text", "acknowledged"}
    if not isinstance(payload, dict) or set(payload) - allowed_fields:
        return _error("invalid_payload", "Unsupported account deletion fields.")
    if payload.get("acknowledged") is not True:
        return _error("acknowledgement_required", "Acknowledge that deletion is irreversible.")
    if str(payload.get("confirmation_text") or "") != ACCOUNT_DELETION_CONFIRMATION_TEXT:
        return _error("confirmation_mismatch", "Enter the exact deletion confirmation text.")
    try:
        consume_account_deletion_challenge(
            request,
            user=user,
            challenge=str(payload.get("challenge") or ""),
        )
    except AccountDeletionChallengeError as exc:
        return _error(exc.code, str(exc), status=exc.status)
    request_id = str(uuid4())
    try:
        result = delete_own_account(user_id=int(user.tg_user_id), request_id=request_id)
    except SelfServiceAccountDeletionError as exc:
        retry_metadata = {"retryable": True} if 500 <= exc.status < 600 else {}
        return _error(exc.code, str(exc), status=exc.status, **retry_metadata)
    logout_session(request, all_devices=True)
    return JsonResponse(
        {
            "ok": True,
            "status": "deleted",
            "request_id": result["request_id"],
        }
    )
