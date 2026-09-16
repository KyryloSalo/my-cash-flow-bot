from __future__ import annotations

from functools import wraps

from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME
from django.core.exceptions import PermissionDenied
from django.template.response import TemplateResponse


# Each tuple is an alternative set of required Django permissions. Use actual
# permission codenames, not ModelAdmin.has_change_permission(): some operational
# models intentionally disable raw editing but still permit their guarded flows.
ACTION_PERMISSIONS = {
    "user_status": (("users.change_telegramuser",),),
    "user_view": (("users.view_telegramuser",), ("users.change_telegramuser",)),
    "subscription_change": (("subscriptions.change_subscription",),),
    "payment_change": (("subscriptions.change_payment",),),
    "message_send": (("broadcasts.add_adminmessagelog",),),
    "note_add": (("users.add_adminnote",),),
    "tag_create": (("users.add_tag",),),
    "tag_assign": (("users.add_usertag",),),
    "tag_remove": (("users.delete_usertag",),),
    "test_mark": (("users.can_use_test_tools", "users.change_useradminstate"),),
    "onboarding_reset": (("users.change_telegramuser",), ("users.can_use_test_tools",)),
    "test_cleanup": (("users.can_use_test_tools", "users.delete_telegramuser"),),
    "broadcast_create": (("broadcasts.add_broadcast",),),
    "broadcast_change": (("broadcasts.change_broadcast",),),
    "broadcast_view": (("broadcasts.view_broadcast",), ("broadcasts.change_broadcast",)),
    "poll_change": (("polls.change_pollcampaign",),),
    "support_reply": (("support.change_supportcase",),),
    "recovery_change": (("subscriptions.change_trialrecoverycampaign",),),
    "recovery_view": (("subscriptions.view_trialrecoverycampaign",), ("subscriptions.change_trialrecoverycampaign",)),
    "qa_tools": (("users.can_use_test_tools",),),
    "audit_view": (("audit_log.view_adminauditlog",),),
    "health_view": (("bot_events.view_botevent",),),
    "notification_view": (("admin_notifications.view_adminnotificationlog",),),
    "notification_send": (("admin_notifications.add_adminnotificationlog",),),
}


def can_admin_action(user, action: str) -> bool:
    requirements = ACTION_PERMISSIONS.get(action)
    if not requirements or not all(getattr(user, flag, False) for flag in ("is_authenticated", "is_active", "is_staff")):
        return False
    if getattr(user, "is_superuser", False):
        return True
    return any(all(user.has_perm(permission) for permission in group) for group in requirements)


def require_admin_action(user, action: str) -> None:
    if not can_admin_action(user, action):
        raise PermissionDenied("Недостатньо прав для цієї дії адміністратора.")


def require_non_operator_target(user_id) -> None:
    """Match the Mini App's protection for ban/reset/delete in every adapter."""
    from django.conf import settings

    protected = {int(value) for value in getattr(settings, "MINIAPP_OPERATOR_TELEGRAM_IDS", [])}
    try:
        target_id = int(user_id)
    except (TypeError, ValueError):
        raise PermissionDenied("Некоректний Telegram ID.")
    if target_id in protected:
        raise PermissionDenied("Обліковий запис оператора захищений від цієї дії.")


def authenticated_operator_id(request) -> int | None:
    """Reuse the authenticated Mini App session, never a caller's role flag."""
    if request is None or not hasattr(request, "session"):
        return None
    from django.conf import settings
    from miniapp.auth import MiniAppSessionError, get_session_tg_user_id

    try:
        operator_id = get_session_tg_user_id(request)
    except MiniAppSessionError:
        return None
    allowed = {str(value) for value in getattr(settings, "MINIAPP_OPERATOR_TELEGRAM_IDS", [])}
    return operator_id if str(operator_id) in allowed else None


def require_service_action(admin_user, action: str, *, request=None) -> int | None:
    # A Django principal must use its own RBAC, even if that browser also has a
    # Mini App session. Only the already-authenticated Mini App adapter passes None.
    if admin_user is None and action in {
        "test_mark", "onboarding_reset", "test_cleanup", "tag_create", "tag_assign", "tag_remove",
    }:
        operator_id = authenticated_operator_id(request)
        if operator_id is not None:
            return operator_id
    require_admin_action(admin_user, action)
    return None


def admin_action_permission(action: str):
    """Guard a model/site handler before lookup, on both GET and POST."""
    def decorate(view):
        @wraps(view)
        def guarded(self, request, *args, **kwargs):
            require_admin_action(getattr(request, "user", None), action)
            return view(self, request, *args, **kwargs)
        return guarded
    return decorate


def bulk_action_confirmation(
    *,
    modeladmin,
    request,
    queryset,
    title: str,
    warning: str,
    submit_label: str,
    section: str,
    cancel_url: str,
):
    """Render a consistent second step while preserving Django action selection."""

    return TemplateResponse(
        request,
        "admin/bulk_action_confirmation.html",
        {
            **modeladmin.admin_site.each_context(request),
            "title": title,
            "warning": warning,
            "submit_label": submit_label,
            "section": section,
            "objects": queryset[:50],
            "selected_count": queryset.count(),
            "selected_ids": request.POST.getlist(ACTION_CHECKBOX_NAME),
            "action_name": request.POST.get("action", ""),
            "action_checkbox_name": ACTION_CHECKBOX_NAME,
            "select_across": request.POST.get("select_across", "0"),
            "cancel_url": cancel_url,
            "opts": modeladmin.model._meta,
        },
    )
