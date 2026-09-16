from __future__ import annotations

from django.conf import settings
from django.http import HttpResponseForbidden


def _trusted_remote_ip(request) -> str:
    peer = request.META.get("REMOTE_ADDR", "").strip()
    # Only nginx on the dedicated edge network may assert a client address.
    if peer in getattr(settings, "ADMIN_TRUSTED_PROXY_IPS", []):
        return request.META.get("HTTP_X_REAL_IP", "").strip() or peer
    return peer


class AdminIPAllowlistMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        allowed = getattr(settings, "ADMIN_IP_ALLOWLIST", [])
        if (
            request.path.startswith("/billing/")
            or request.path.startswith("/internal/billing/")
            or request.path.startswith("/internal/transactions/")
            or request.path.startswith("/internal/push/")
            or request.path.startswith("/internal/health/")
            or request.path.startswith("/app/")
        ):
            return self.get_response(request)
        if allowed and not request.path.startswith(settings.STATIC_URL):
            remote_ip = _trusted_remote_ip(request)
            if remote_ip not in allowed:
                return HttpResponseForbidden("IP not allowed")
        return self.get_response(request)
