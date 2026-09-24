from __future__ import annotations

import json
from typing import Any

from django.conf import settings
from django.http import HttpRequest, JsonResponse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from miniapp.auth import MiniAppSessionError, get_session_user

from .catalog import achievement_definitions, public_achievement
from .models import AchievementGrant, AchievementProgress, GamificationProfile, PinnedAchievement
from .services import (
    GamificationActionError,
    acknowledge_notification,
    claim_next_notification,
    current_day_snapshot,
    record_no_expenses,
    set_pinned_achievements,
    update_preferences,
)


def _error(code: str, message: str, *, status: int) -> JsonResponse:
    return JsonResponse({"ok": False, "error": {"code": code, "message": message}}, status=status)


def _user_or_response(request: HttpRequest):
    try:
        return get_session_user(request)
    except MiniAppSessionError:
        return _error("session_required", "Потрібна активна сесія Mini App.", status=401)


def _json_body(request: HttpRequest) -> dict[str, Any]:
    try:
        payload = json.loads(request.body or b"{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _action_error(exc: GamificationActionError) -> JsonResponse:
    return _error(exc.code, str(exc), status=exc.status)


def _feature_disabled() -> JsonResponse | None:
    if getattr(settings, "GAMIFICATION_UI_ENABLED", False):
        return None
    return _error("feature_disabled", "Розділ тимчасово недоступний.", status=404)


def _catalog_payload(user_id: int, mascot: str) -> list[dict[str, object]]:
    grants = list(AchievementGrant.objects.filter(user_id=user_id).order_by("granted_at", "id"))
    progress = {
        row.achievement_key: row
        for row in AchievementProgress.objects.filter(user_id=user_id)
    }
    by_key: dict[str, list[AchievementGrant]] = {}
    for grant in grants:
        by_key.setdefault(grant.achievement_key, []).append(grant)
    result: list[dict[str, object]] = []
    for definition in achievement_definitions():
        family_grants = by_key.get(definition.key, [])
        earned = bool(family_grants)
        level = max((grant.level_key for grant in family_grants), default=0) or None
        item = public_achievement(definition.key, mascot=mascot, earned=earned, level=level)
        progress_row = progress.get(definition.key)
        item.update(
            {
                "earned": earned,
                "earned_at": family_grants[-1].granted_at.isoformat() if family_grants else None,
                "level": level,
                "repeat_count": len(family_grants),
                "progress": {
                    "current": progress_row.current_value if progress_row else 0,
                    "best": progress_row.best_value if progress_row else 0,
                    "context": progress_row.context if progress_row else {},
                },
            }
        )
        result.append(item)
    return result


@require_GET
def overview(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    user_id = int(user.tg_user_id)
    day = current_day_snapshot(user_id=user_id)
    profile = GamificationProfile.objects.get(user_id=user_id)
    pins = list(
        PinnedAchievement.objects.filter(user_id=user_id)
        .order_by("position")
        .values_list("achievement_key", flat=True)
    )
    recent = list(
        AchievementGrant.objects.filter(user_id=user_id)
        .order_by("-granted_at", "-id")
        .values("achievement_key", "level_key", "granted_at")[:5]
    )
    for item in recent:
        item["granted_at"] = item["granted_at"].isoformat()
    return JsonResponse(
        {
            "ok": True,
            "version": "v1",
            "profile": {
                "mascot": profile.mascot or "bob",
                "needs_mascot": not bool(profile.mascot),
                "motion_enabled": profile.motion_enabled,
                "timezone": profile.timezone_name,
                "pending_timezone": profile.pending_timezone_name or None,
                "pending_timezone_effective_at": (
                    profile.pending_timezone_effective_at.isoformat()
                    if profile.pending_timezone_effective_at
                    else None
                ),
                "revision": profile.revision,
            },
            "day": day,
            "catalog": _catalog_payload(user_id, profile.mascot or "bob"),
            "pins": pins,
            "recent_grants": recent,
        }
    )


@require_http_methods(["PATCH"])
def preferences(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _json_body(request)
    try:
        profile = update_preferences(
            user_id=int(user.tg_user_id),
            mascot=payload.get("mascot") if "mascot" in payload else None,
            motion_enabled=payload.get("motion_enabled") if "motion_enabled" in payload else None,
            timezone_name=payload.get("timezone") if "timezone" in payload else None,
        )
    except GamificationActionError as exc:
        return _action_error(exc)
    return JsonResponse(
        {
            "ok": True,
            "profile": {
                "mascot": profile.mascot,
                "motion_enabled": profile.motion_enabled,
                "timezone": profile.timezone_name,
                "pending_timezone": profile.pending_timezone_name or None,
                "revision": profile.revision,
            },
        }
    )


@require_POST
def no_expenses(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _json_body(request)
    try:
        result = record_no_expenses(
            user_id=int(user.tg_user_id),
            day_id=str(payload.get("day_id") or ""),
            idempotency_key=str(request.headers.get("Idempotency-Key") or payload.get("idempotency_key") or ""),
        )
    except GamificationActionError as exc:
        return _action_error(exc)
    return JsonResponse(result)


@require_http_methods(["PUT"])
def pins(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _json_body(request)
    raw_keys = payload.get("achievement_keys")
    try:
        keys = set_pinned_achievements(
            user_id=int(user.tg_user_id),
            achievement_keys=raw_keys if isinstance(raw_keys, list) else [],
        )
    except GamificationActionError as exc:
        return _action_error(exc)
    return JsonResponse({"ok": True, "pins": keys})


@require_POST
def notification_claim(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _json_body(request)
    try:
        claim = claim_next_notification(
            user_id=int(user.tg_user_id),
            device_id=str(payload.get("device_id") or ""),
        )
    except GamificationActionError as exc:
        return _action_error(exc)
    if claim is not None:
        profile = GamificationProfile.objects.get(user_id=int(user.tg_user_id))
        grants_by_id = {
            str(grant.id): grant
            for grant in AchievementGrant.objects.filter(
                user_id=int(user.tg_user_id),
                id__in=claim.get("grant_ids", []),
            )
        }
        grants: list[dict[str, object]] = []
        for grant_id in claim.get("grant_ids", []):
            grant = grants_by_id.get(str(grant_id))
            if grant is None:
                continue
            item = public_achievement(
                grant.achievement_key,
                mascot=profile.mascot or "bob",
                earned=True,
                level=grant.level_key or None,
            )
            item.pop("condition", None)
            item.update(
                {
                    "grant_id": str(grant.id),
                    "level": grant.level_key or None,
                    "earned_at": grant.granted_at.isoformat(),
                }
            )
            grants.append(item)
        claim["grants"] = grants
    return JsonResponse({"ok": True, "notification": claim})


@require_POST
def notification_ack(request: HttpRequest) -> JsonResponse:
    disabled = _feature_disabled()
    if disabled is not None:
        return disabled
    user = _user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _json_body(request)
    try:
        acknowledge_notification(
            user_id=int(user.tg_user_id),
            notification_id=str(payload.get("notification_id") or ""),
            claim_token=str(payload.get("claim_token") or ""),
            device_id=str(payload.get("device_id") or ""),
        )
    except GamificationActionError as exc:
        return _action_error(exc)
    return JsonResponse({"ok": True})
