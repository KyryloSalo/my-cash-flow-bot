from __future__ import annotations

import base64
import hashlib
import os
from datetime import timedelta
from pathlib import Path

from common.log_safety import install_log_safety

install_log_safety()

import dj_database_url
from celery.schedules import crontab
from django.core.exceptions import ImproperlyConfigured

BASE_DIR = Path(__file__).resolve().parent.parent


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def env_file_secret(name: str) -> str | None:
    path = env(name)
    if not path:
        return None
    try:
        value = Path(path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ImproperlyConfigured(f"Unable to read {name}") from exc
    if not value:
        raise ImproperlyConfigured(f"{name} must not be empty")
    return value


def configured_secret(name: str) -> str:
    return env_file_secret(f"{name}_FILE") or env(name, "") or ""


def suitable_secret_key(value: str | None) -> bool:
    return bool(
        value
        and len(value) >= 50
        and len(set(value)) >= 5
        and not value.startswith("django-insecure-")
    )


def env_bool(name: str, default: bool = False) -> bool:
    value = env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int | None = None) -> int | None:
    value = env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_list(name: str, default: list[str] | None = None) -> list[str]:
    value = env(name)
    if value is None:
        return list(default or [])
    return [item.strip() for item in value.split(",") if item.strip()]


DEBUG = env_bool("DJANGO_DEBUG", False)
_environment_secret_key = env("DJANGO_SECRET_KEY")
_file_secret_key = env_file_secret("DJANGO_SECRET_KEY_FILE")
SECRET_KEY = _file_secret_key or _environment_secret_key
if not SECRET_KEY:
    if DEBUG:
        SECRET_KEY = "django-insecure-local-admin-service"
    else:  # pragma: no cover
        raise ImproperlyConfigured("DJANGO_SECRET_KEY is required when DJANGO_DEBUG=false")
SECRET_KEY_FALLBACKS = []
if (
    _file_secret_key
    and _environment_secret_key != _file_secret_key
    and suitable_secret_key(_environment_secret_key)
):
    SECRET_KEY_FALLBACKS.append(_environment_secret_key)

DOMAIN = env("DOMAIN", "vydno.capital") or "vydno.capital"
ADMIN_DOMAIN = env("ADMIN_DOMAIN", "admin.vydno.capital") or "admin.vydno.capital"
LEGACY_ADMIN_DOMAIN = env("LEGACY_ADMIN_DOMAIN")
default_allowed_hosts = ["127.0.0.1", "localhost", "admin", DOMAIN, ADMIN_DOMAIN]
if LEGACY_ADMIN_DOMAIN:
    default_allowed_hosts.append(LEGACY_ADMIN_DOMAIN)
ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS", default_allowed_hosts)
CSRF_TRUSTED_ORIGINS = [f"https://{host}" for host in ALLOWED_HOSTS if "." in host]

INSTALLED_APPS = [
    "unfold",
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django.contrib.postgres",
    "axes",
    "dashboard",
    "users",
    "transactions",
    "categories",
    "accounts",
    "subscriptions",
    "broadcasts",
    "support",
    "feedback",
    "polls",
    "bot_settings",
    "bot_events",
    "miniapp",
    "audit_log.apps.AuditLogConfig",
    "admin_notifications",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "common.middleware.AdminIPAllowlistMiddleware",
    "axes.middleware.AxesMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        env("DATABASE_URL", "postgresql://mcf:mcf@db:5432/mcf"),
        conn_max_age=600,
        ssl_require=False,
    )
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "uk"
TIME_ZONE = env("TZ", "Europe/Istanbul") or "Europe/Istanbul"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [BASE_DIR / "static"]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
TEST_RUNNER = "common.test_runner.RuntimeSchemaTestRunner"

LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/"
LOGOUT_REDIRECT_URL = "/login/"

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = False  # nginx supplies the validated Host header directly

SECURE_SSL_REDIRECT = env_bool("SECURE_SSL_REDIRECT", not DEBUG)
SECURE_REDIRECT_EXEMPT = [r"^internal/billing/", r"^internal/push/", r"^internal/transactions/", r"^internal/health/"]
SESSION_COOKIE_SECURE = env_bool("SESSION_COOKIE_SECURE", not DEBUG)
CSRF_COOKIE_SECURE = env_bool("CSRF_COOKIE_SECURE", not DEBUG)
SECURE_BROWSER_XSS_FILTER = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_HSTS_SECONDS = int(env("SECURE_HSTS_SECONDS", "31536000") or "31536000")
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool("SECURE_HSTS_INCLUDE_SUBDOMAINS", True)
SECURE_HSTS_PRELOAD = False  # Do not enroll the domain in browser preload lists implicitly.
SILENCED_SYSTEM_CHECKS = ["security.W021"]  # Explicit non-preload policy, not a missing TLS gate.
X_FRAME_OPTIONS = "DENY"
CSRF_COOKIE_HTTPONLY = False
SESSION_COOKIE_HTTPONLY = True

REDIS_URL = env("REDIS_URL", "redis://redis:6379/0") or "redis://redis:6379/0"
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_TASK_TIME_LIMIT = 60 * 10
CELERY_TASK_SOFT_TIME_LIMIT = 60 * 8
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_BROKER_CONNECTION_TIMEOUT = 2
CELERY_BROKER_TRANSPORT_OPTIONS = {"socket_connect_timeout": 2, "socket_timeout": 2}
CELERY_BEAT_SCHEDULER = "common.scheduler:HeartbeatScheduler"
CELERY_IMPORTS = ("common.tasks",)
CELERY_BEAT_SCHEDULE = {
    "scheduler-probe": {"task": "common.scheduler_probe", "schedule": 30.0, "options": {"expires": 60}},
    "subscriptions-reconcile-pending-monobank-charges": {
        "task": "subscriptions.reconcile_pending_monobank_charges",
        "schedule": crontab(minute="*/2"),
    },
    "subscriptions-run-due-monobank-charges": {
        "task": "subscriptions.run_due_monobank_charges",
        "schedule": crontab(minute="*/10"),
    },
    "subscriptions-dispatch-trial-recovery": {
        "task": "subscriptions.dispatch_trial_recovery",
        "schedule": crontab(minute="*/5"),
    },
    "miniapp-dispatch-pending-pushes": {
        "task": "miniapp.dispatch_pending_pushes",
        "schedule": crontab(minute="*/10"),
    },
    "miniapp-billing-lifecycle-pushes": {
        "task": "miniapp.emit_billing_lifecycle_pushes",
        "schedule": crontab(minute=15),
    },
    "miniapp-debt-due-pushes": {
        "task": "miniapp.emit_debt_due_pushes",
        "schedule": crontab(hour=9, minute=5),
    },
    "miniapp-weekly-summary-pushes": {
        "task": "miniapp.emit_weekly_summary_pushes",
        "schedule": crontab(day_of_week="monday", hour=9, minute=15),
    },
}

MONO_API_BASE_URL = env("MONO_API_BASE_URL", "https://api.monobank.ua") or "https://api.monobank.ua"
MONO_MERCHANT_TOKEN = env("MONO_MERCHANT_TOKEN", "")
MONO_WEBHOOK_VERIFY_SIGNATURES = env_bool("MONO_WEBHOOK_VERIFY_SIGNATURES", True)
MONO_BIND_AMOUNT = int(env("MONO_BIND_AMOUNT", "100") or "100")
MONO_BIND_INVOICE_VALIDITY_SECONDS = int(env("MONO_BIND_INVOICE_VALIDITY_SECONDS", "3600") or "3600")
MONO_RENEWAL_AMOUNT = int(env("MONO_RENEWAL_AMOUNT", "49900") or "49900")
MONO_RENEWAL_PERIOD_DAYS = int(env("MONO_RENEWAL_PERIOD_DAYS", "30") or "30")
MONO_GRACE_DAYS = int(env("MONO_GRACE_DAYS", "30") or "30")
MONO_SOFT_GRACE_DAYS = int(env("MONO_SOFT_GRACE_DAYS", "1") or "1")
MONO_BILLING_WEBHOOK_URL = env("MONO_BILLING_WEBHOOK_URL", f"https://{DOMAIN}/billing/mono/webhook") or f"https://{DOMAIN}/billing/mono/webhook"
MONO_BILLING_RETURN_URL = env("MONO_BILLING_RETURN_URL", f"https://{DOMAIN}/billing/mono/return") or f"https://{DOMAIN}/billing/mono/return"
BILLING_INTERNAL_TOKEN = env("BILLING_INTERNAL_TOKEN", "") or ""
PUSH_INTERNAL_TOKEN = env("PUSH_INTERNAL_TOKEN", BILLING_INTERNAL_TOKEN) or BILLING_INTERNAL_TOKEN
WEB_PUSH_VAPID_PUBLIC_KEY = env("WEB_PUSH_VAPID_PUBLIC_KEY", "") or ""
WEB_PUSH_VAPID_PRIVATE_KEY = env("WEB_PUSH_VAPID_PRIVATE_KEY", "") or ""
WEB_PUSH_VAPID_SUBJECT = env("WEB_PUSH_VAPID_SUBJECT", "mailto:support@vydno.capital") or "mailto:support@vydno.capital"
WEB_PUSH_TTL_SECONDS = int(env("WEB_PUSH_TTL_SECONDS", "86400") or "86400")
if not WEB_PUSH_VAPID_PRIVATE_KEY or not WEB_PUSH_VAPID_PUBLIC_KEY:
    # Stable, domain-separated fallback that requires no extra production secret file.
    # Explicit WEB_PUSH_VAPID_* variables always take precedence.
    from ecdsa import NIST256p, SigningKey

    _vapid_seed = hashlib.sha256(f"vydno-web-push-v1:{SECRET_KEY}".encode("utf-8")).digest()
    _vapid_exponent = (int.from_bytes(_vapid_seed, "big") % (NIST256p.order - 1)) + 1
    _vapid_signing_key = SigningKey.from_secret_exponent(_vapid_exponent, curve=NIST256p)
    # py-vapid accepts a base64url-encoded 32-byte raw P-256 scalar here.
    # A PEM string is not accepted by Vapid.from_string() and fails before delivery.
    WEB_PUSH_VAPID_PRIVATE_KEY = base64.urlsafe_b64encode(_vapid_signing_key.to_string()).rstrip(b"=").decode("ascii")
    _vapid_public_bytes = b"\x04" + _vapid_signing_key.verifying_key.to_string()
    WEB_PUSH_VAPID_PUBLIC_KEY = base64.urlsafe_b64encode(_vapid_public_bytes).rstrip(b"=").decode("ascii")
MINIAPP_EXPORT_INTERNAL_BASE_URL = env("MINIAPP_EXPORT_INTERNAL_BASE_URL", "http://api:8000") or "http://api:8000"
MINIAPP_EXPORT_INTERNAL_TIMEOUT_SECONDS = int(env("MINIAPP_EXPORT_INTERNAL_TIMEOUT_SECONDS", "90") or "90")
MINIAPP_BOT_INTERNAL_BASE_URL = env("MINIAPP_BOT_INTERNAL_BASE_URL", MINIAPP_EXPORT_INTERNAL_BASE_URL) or MINIAPP_EXPORT_INTERNAL_BASE_URL
MINIAPP_BOT_INTERNAL_TIMEOUT_SECONDS = int(env("MINIAPP_BOT_INTERNAL_TIMEOUT_SECONDS", "20") or "20")
MINIAPP_BOT_INTERNAL_AI_TIMEOUT_SECONDS = int(env("MINIAPP_BOT_INTERNAL_AI_TIMEOUT_SECONDS", "75") or "75")
SUPPORT_CONTACT_URL = env("SUPPORT_CONTACT_URL", "https://t.me/Askills_Support") or "https://t.me/Askills_Support"

TELEGRAM_BOT_TOKEN = env("TELEGRAM_BOT_TOKEN", env("BOT_TOKEN"))
TELEGRAM_OIDC_CLIENT_ID = configured_secret("TELEGRAM_OIDC_CLIENT_ID")
TELEGRAM_OIDC_CLIENT_SECRET = configured_secret("TELEGRAM_OIDC_CLIENT_SECRET")
TELEGRAM_OIDC_REDIRECT_URI = (
    env("TELEGRAM_OIDC_REDIRECT_URI", f"https://{DOMAIN}/app/auth/telegram/callback")
    or f"https://{DOMAIN}/app/auth/telegram/callback"
)
TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT = "https://oauth.telegram.org/auth"
TELEGRAM_OIDC_TOKEN_ENDPOINT = "https://oauth.telegram.org/token"
TELEGRAM_OIDC_JWKS_URI = "https://oauth.telegram.org/.well-known/jwks.json"
TELEGRAM_OIDC_ISSUER = "https://oauth.telegram.org"
TELEGRAM_OIDC_SCOPES = "openid profile"
TELEGRAM_OIDC_FLOW_TTL_SECONDS = int(env("TELEGRAM_OIDC_FLOW_TTL_SECONDS", "600") or "600")
TELEGRAM_OIDC_HTTP_TIMEOUT_SECONDS = int(env("TELEGRAM_OIDC_HTTP_TIMEOUT_SECONDS", "10") or "10")
MINIAPP_TELEGRAM_AUTH_MAX_AGE = int(env("MINIAPP_TELEGRAM_AUTH_MAX_AGE", "86400") or "86400")
MINIAPP_BROWSER_LOGIN_SECRET = env("MINIAPP_BROWSER_LOGIN_SECRET", "")
MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS = int(env("MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS", "900") or "900")
MINIAPP_BROWSER_SESSION_AGE_SECONDS = int(env("MINIAPP_BROWSER_SESSION_AGE_SECONDS", str(180 * 24 * 60 * 60)) or str(180 * 24 * 60 * 60))
MINIAPP_DEV_TG_USER_ID = env_int("MINIAPP_DEV_TG_USER_ID")
ADMIN_TELEGRAM_IDS = [int(item) for item in env_list("ADMIN_TELEGRAM_IDS") if item.isdigit()]
PRIMARY_ADMIN_TELEGRAM_ID = env_int("PRIMARY_ADMIN_TELEGRAM_ID", ADMIN_TELEGRAM_IDS[0] if ADMIN_TELEGRAM_IDS else None)
MINIAPP_OPERATOR_TELEGRAM_IDS = [
    int(item)
    for item in env_list("MINIAPP_OPERATOR_TELEGRAM_IDS", ["7884326049"])
    if item.isdigit()
]
ADMIN_TEST_TELEGRAM_IDS = [int(item) for item in env_list("ADMIN_TEST_TELEGRAM_IDS") if item.isdigit()]
ADMIN_NOTIFICATIONS_ENABLED = env_bool("ADMIN_NOTIFICATIONS_ENABLED", True)
ENABLE_ADMIN_TEST_TOOLS = env_bool("ENABLE_ADMIN_TEST_TOOLS", True)
ENABLE_ADMIN_DEVELOPER_MODE = env_bool("ENABLE_ADMIN_DEVELOPER_MODE", False)
RESET_ONBOARDING_NOTICE_TEXT = (
    env("RESET_ONBOARDING_NOTICE_TEXT", "Тестовий onboarding скинуто. Напиши /start, щоб пройти його заново.")
    or "Тестовий onboarding скинуто. Напиши /start, щоб пройти його заново."
)
ADMIN_IP_ALLOWLIST = env_list("ADMIN_IP_ALLOWLIST")
ADMIN_TRUSTED_PROXY_IPS = env_list("ADMIN_TRUSTED_PROXY_IPS")

AXES_ENABLED = env_bool("AXES_ENABLED", True)
AXES_FAILURE_LIMIT = int(env("AXES_FAILURE_LIMIT", "5") or "5")
AXES_COOLOFF_TIME = timedelta(minutes=int(env("AXES_COOLOFF_MINUTES", "60") or "60"))
AXES_RESET_ON_SUCCESS = True
AXES_LOCKOUT_PARAMETERS = ["ip_address"]
AXES_HTTP_RESPONSE_CODE = 429
AUTHENTICATION_BACKENDS = [
    "axes.backends.AxesStandaloneBackend",
    "django.contrib.auth.backends.ModelBackend",
]

UNFOLD = {
    "SITE_TITLE": "Vydno Control",
    "SITE_HEADER": "Vydno Control",
    "SITE_SYMBOL": "account_balance_wallet",
    "SHOW_HISTORY": True,
    "SHOW_VIEW_ON_SITE": False,
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)s %(name)s: %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}
