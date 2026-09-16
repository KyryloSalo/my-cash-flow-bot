from __future__ import annotations

from urllib.parse import urlsplit

from django.conf import settings
from django.core.checks import Error, Tags, register


@register(Tags.security)
def check_telegram_oidc_configuration(app_configs, **kwargs):
    client_id = str(getattr(settings, "TELEGRAM_OIDC_CLIENT_ID", "") or "").strip()
    client_secret = str(getattr(settings, "TELEGRAM_OIDC_CLIENT_SECRET", "") or "").strip()
    if bool(client_id) != bool(client_secret):
        return [
            Error(
                "Telegram OIDC requires both client ID and client secret.",
                hint="Configure TELEGRAM_OIDC_CLIENT_ID and TELEGRAM_OIDC_CLIENT_SECRET together.",
                id="miniapp.E001",
            )
        ]
    if not client_id:
        return []

    redirect_uri = str(getattr(settings, "TELEGRAM_OIDC_REDIRECT_URI", "") or "").strip()
    try:
        parsed = urlsplit(redirect_uri)
    except ValueError:
        parsed = None
    expected_host = str(getattr(settings, "DOMAIN", "") or "").strip().lower()
    if (
        parsed is None
        or parsed.scheme != "https"
        or not parsed.hostname
        or parsed.hostname.lower() != expected_host
        or parsed.path != "/app/auth/telegram/callback"
        or parsed.query
        or parsed.fragment
    ):
        return [
            Error(
                "Telegram OIDC redirect URI must use the canonical HTTPS callback.",
                hint=f"Set TELEGRAM_OIDC_REDIRECT_URI to https://{expected_host}/app/auth/telegram/callback.",
                id="miniapp.E002",
            )
        ]
    return []
