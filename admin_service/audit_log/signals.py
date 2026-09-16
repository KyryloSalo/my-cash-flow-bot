from __future__ import annotations

from common.audit import create_audit_log


def log_admin_login(sender, request, user, **kwargs) -> None:
    if not getattr(user, "is_staff", False):
        return
    create_audit_log(
        request=request,
        admin_user=user,
        action="login",
        object_type="auth.User",
        object_id=user.pk,
        before={},
        after={"username": user.get_username()},
    )


def log_admin_logout(sender, request, user, **kwargs) -> None:
    if user is None or not getattr(user, "is_staff", False):
        return
    create_audit_log(
        request=request,
        admin_user=user,
        action="logout",
        object_type="auth.User",
        object_id=user.pk,
        before={"username": user.get_username()},
        after={},
    )
