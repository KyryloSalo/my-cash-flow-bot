import os

from log_safety import install_log_safety

install_log_safety()


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def env_bool(name: str, default: bool = False) -> bool:
    value = env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    value = env(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def env_list(name: str) -> list[str]:
    value = env(name)
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


BOT_TOKEN = env("BOT_TOKEN")
DATABASE_URL = env("DATABASE_URL")

TZ = env("TZ", "Europe/Istanbul") or "Europe/Istanbul"

ADMIN_TOKEN = env("ADMIN_TOKEN")
API_HOST = env("API_HOST", "0.0.0.0") or "0.0.0.0"
API_PORT = int(env("API_PORT", "8000") or "8000")
BILLING_INTERNAL_BASE_URL = env("BILLING_INTERNAL_BASE_URL", "http://admin:8080") or "http://admin:8080"
BILLING_INTERNAL_TOKEN = env("BILLING_INTERNAL_TOKEN")
PUSH_INTERNAL_BASE_URL = env("PUSH_INTERNAL_BASE_URL", BILLING_INTERNAL_BASE_URL) or BILLING_INTERNAL_BASE_URL
PUSH_INTERNAL_TOKEN = env("PUSH_INTERNAL_TOKEN", BILLING_INTERNAL_TOKEN) or BILLING_INTERNAL_TOKEN
MINIAPP_URL = env("MINIAPP_URL", "https://vydno.capital/app/")
MINIAPP_CACHE_BUST_VERSION = env("MINIAPP_CACHE_BUST_VERSION", "20260525-miniapp-period-summary-bars1")
MINIAPP_BROWSER_LOGIN_SECRET = env("MINIAPP_BROWSER_LOGIN_SECRET")
MINIAPP_BROWSER_LOGIN_TTL_SECONDS = env_int("MINIAPP_BROWSER_LOGIN_TTL_SECONDS", 15 * 60)
PRIVACY_POLICY_URL = env("PRIVACY_POLICY_URL", "https://vydno.capital/privacy.html")
SUPPORT_CONTACT_URL = env("SUPPORT_CONTACT_URL")
ADMIN_TELEGRAM_IDS = [int(item) for item in env_list("ADMIN_TELEGRAM_IDS") if item.isdigit()]
_miniapp_operator_ids = env_list("MINIAPP_OPERATOR_TELEGRAM_IDS") or ["7884326049"]
MINIAPP_OPERATOR_TELEGRAM_IDS = [int(item) for item in _miniapp_operator_ids if item.isdigit()]
ADMIN_TEST_TELEGRAM_IDS = [int(item) for item in env_list("ADMIN_TEST_TELEGRAM_IDS") if item.isdigit()]
ADMIN_NOTIFICATIONS_ENABLED = env_bool("ADMIN_NOTIFICATIONS_ENABLED", True)

OPENAI_API_KEY = env("OPENAI_API_KEY")
OPENAI_STT_MODEL = env("OPENAI_STT_MODEL", "gpt-4o-mini-transcribe") or "gpt-4o-mini-transcribe"
OPENAI_TX_MODEL = env("OPENAI_TX_MODEL", "gpt-4o-mini") or "gpt-4o-mini"
OPENAI_VISION_MODEL = env("OPENAI_VISION_MODEL", "gpt-5.4-nano") or "gpt-5.4-nano"
OPENAI_VISION_FALLBACK_MODEL = env("OPENAI_VISION_FALLBACK_MODEL", OPENAI_VISION_MODEL) or OPENAI_VISION_MODEL
OPENAI_IMAGE_DETAIL = env("OPENAI_IMAGE_DETAIL", "high") or "high"
OPENAI_MAX_OUTPUT_TOKENS = env_int("OPENAI_MAX_OUTPUT_TOKENS", 600)
OPENAI_REQUEST_TIMEOUT_MS = env_int("OPENAI_REQUEST_TIMEOUT_MS", 30_000)
OPENAI_SCREENSHOT_STORE_RAW = env_bool("OPENAI_SCREENSHOT_STORE_RAW", False)
OPENAI_SCREENSHOT_CONFIDENCE_THRESHOLD = env_float("OPENAI_SCREENSHOT_CONFIDENCE_THRESHOLD", 0.7)

FAMILY_ACCESS_ENABLED = env_bool("FAMILY_ACCESS_ENABLED", True)
