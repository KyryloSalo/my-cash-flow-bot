"""Authenticated Telegram ingress into the same private photo/outbox service as PWA."""
from __future__ import annotations

import json
import re
import secrets
from io import BytesIO

from django.conf import settings
from django.db import transaction
from django.http import JsonResponse
from django.http.multipartparser import MultiPartParserError
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from miniapp import recognition_quality
from miniapp.models import RecognitionCapture
from miniapp.recognition_storage import MAX_MEDIA_BYTES, _locked_config
from users.models import TelegramUser

_ATTEMPT = re.compile(r"^tg-[A-Za-z0-9_-]{1,125}$")
_ERROR = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_MIME = re.compile(r"^image/[a-z0-9.+-]{1,60}$")
_OUTCOMES = {"parsed", "failed", "redirect", "draft", "cancelled"}


def _reply(capture=None, *, status=200, error=None):
    payload = {"capture_id": capture.pk if capture is not None else None}
    if error:
        payload["error"] = error
    return JsonResponse(payload, status=status)


def _result(value):
    if not isinstance(value, dict) or set(value) - {"amount", "currency", "item_count"}:
        raise ValueError("invalid_result")
    if "amount" in value and (type(value["amount"]) not in (str, int, float) or len(str(value["amount"])) > 32):
        raise ValueError("invalid_amount")
    if "currency" in value and (not isinstance(value["currency"], str) or not re.fullmatch(r"[A-Z]{3}", value["currency"])):
        raise ValueError("invalid_currency")
    if "item_count" in value and (type(value["item_count"]) is not int or not 0 <= value["item_count"] <= 1000000):
        raise ValueError("invalid_count")
    return value


def _complete(data, user_id, attempt_id):
    outcome = data.get("outcome")
    error_code = data.get("error_code", "")
    if outcome not in _OUTCOMES or not isinstance(error_code, str) or (error_code and not _ERROR.fullmatch(error_code)):
        raise ValueError("invalid_completion")
    result = _result(data.get("normalized_result", {}))
    with transaction.atomic():
        if not TelegramUser.objects.select_for_update().filter(tg_user_id=user_id).exists():
            return _reply(status=404, error="capture_missing")
        capture = RecognitionCapture.objects.filter(user_id=user_id, attempt_id=attempt_id, source="image").first()
        if capture is None:
            return _reply(status=404, error="capture_missing")
        capture = recognition_quality.update_capture(user_id=user_id, attempt_id=attempt_id, outcome=outcome,
                                                     error_code=error_code, normalized_result=result)
        if capture is None:
            return _reply(status=503, error="photo_completion_unavailable")
        return _reply(capture)


def _create(upload, user_id, attempt_id):
    with transaction.atomic():
        # The same user lock used by capture/account deletion serializes request
        # retries without requiring a new uniqueness migration on PWA attempt IDs.
        user = TelegramUser.objects.select_for_update().filter(tg_user_id=user_id).first()
        if user is None:
            return _reply(status=404, error="user_missing")
        capture = RecognitionCapture.objects.filter(user_id=user_id, attempt_id=attempt_id, source="image").first()
        if capture is not None:
            return _reply(capture)
        cfg = _locked_config()
        if cfg is None or not cfg.enabled or not cfg.all_images_enabled:
            return _reply()
        if upload is None or not _MIME.fullmatch(upload.content_type or ""):
            return _reply(status=400, error="invalid_image")
        if upload.size > MAX_MEDIA_BYTES:
            return _reply(status=413, error="image_too_large")
        original = upload.read(MAX_MEDIA_BYTES + 1)
        if not original or len(original) > MAX_MEDIA_BYTES:
            return _reply(status=413, error="image_size_invalid")
        capture = recognition_quality.capture_attempt(user_id=user_id, source="image", attempt_id=attempt_id,
                                                     raw_bytes=original, mime_type=upload.content_type)
        if capture is None:
            return _reply(status=503, error="photo_capture_unavailable")
        capture.outcome = "pending"
        capture.save(update_fields=["outcome"])
        return _reply(capture)


@csrf_exempt
@require_POST
def internal_recognition_photo(request):
    configured = str(getattr(settings, "BILLING_INTERNAL_TOKEN", "") or "")
    supplied = str(request.headers.get("X-Internal-Token") or "")
    if not configured or not supplied or not secrets.compare_digest(configured, supplied):
        return _reply(status=403, error="forbidden")
    files = None
    try:
        is_json = request.content_type == "application/json"
        if not is_json and request.content_type != "multipart/form-data":
            raise ValueError("invalid_content_type")
        # Bound the entire body before Django parses multipart or creates spool
        # files. Do not trust Content-Length alone (e.g. chunked/understated input).
        limit = 4096 if is_json else MAX_MEDIA_BYTES + 65536
        if int(request.META.get("CONTENT_LENGTH") or 0) > limit:
            return _reply(status=413, error="payload_too_large")
        body = request.read(limit + 1)
        if len(body) > limit:
            return _reply(status=413, error="payload_too_large")
        if is_json:
            data = json.loads(body)
        else:
            meta = {**request.META, "CONTENT_LENGTH": str(len(body))}
            data, files = request.parse_file_upload(meta, BytesIO(body))
        if not isinstance(data, dict) and is_json:
            raise ValueError("invalid_payload")
        raw_user = data.get("user_id")
        if isinstance(raw_user, bool) or not str(raw_user).isdigit():
            raise ValueError("invalid_user")
        user_id = int(raw_user)
        attempt_id = data.get("attempt_id", "")
        if not 0 < user_id < 2**63 or not isinstance(attempt_id, str) or not _ATTEMPT.fullmatch(attempt_id):
            raise ValueError("invalid_identity")
        action = data.get("action")
        if action == "create" and not is_json:
            return _create(files.get("image"), user_id, attempt_id)
        if action == "complete" and is_json:
            return _complete(data, user_id, attempt_id)
        raise ValueError("invalid_action")
    except (ValueError, TypeError, MultiPartParserError):
        return _reply(status=400, error="invalid_payload")
    finally:
        if files is not None:
            for _, uploads in files.lists():
                for upload in uploads:
                    upload.close()
