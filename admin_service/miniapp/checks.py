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


@register(Tags.security)
def check_production_canary_configuration(app_configs, **kwargs):
    if not bool(getattr(settings, "PRODUCTION_CANARY_ENABLED", False)):
        return []

    errors = []
    if not getattr(settings, "PRODUCTION_CANARY_TELEGRAM_IDS", []):
        errors.append(Error("Production canary requires a non-empty static allowlist.", id="miniapp.E010"))
    if not str(getattr(settings, "MINIAPP_BROWSER_LOGIN_SECRET", "") or "").strip():
        errors.append(Error("Production canary requires MINIAPP_BROWSER_LOGIN_SECRET.", id="miniapp.E011"))
    for name in ("PRODUCTION_CANARY_LOGIN_TTL_SECONDS", "PRODUCTION_CANARY_ACTION_TTL_SECONDS"):
        ttl = int(getattr(settings, name, 0) or 0)
        if ttl <= 0 or ttl > 15 * 60:
            errors.append(Error(f"{name} must be between 1 and 900 seconds.", id="miniapp.E012"))
    max_amount = int(getattr(settings, "PRODUCTION_CANARY_MAX_AMOUNT", 0) or 0)
    if max_amount <= 0 or max_amount > 100:
        errors.append(Error("PRODUCTION_CANARY_MAX_AMOUNT must be between 1 and 100 minor units.", id="miniapp.E013"))
    return errors
