from __future__ import annotations

from django.apps import AppConfig
from django.contrib.auth.signals import user_logged_in, user_logged_out


class AuditLogConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "audit_log"

    def ready(self) -> None:
        from .signals import log_admin_login, log_admin_logout

        user_logged_in.connect(log_admin_login, dispatch_uid="cashflow_admin_login")
        user_logged_out.connect(log_admin_logout, dispatch_uid="cashflow_admin_logout")
