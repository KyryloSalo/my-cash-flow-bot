import os


def env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


BOT_TOKEN = env("BOT_TOKEN")
DATABASE_URL = env("DATABASE_URL")

TZ = env("TZ", "Europe/Istanbul") or "Europe/Istanbul"

ADMIN_TOKEN = env("ADMIN_TOKEN")
API_HOST = env("API_HOST", "0.0.0.0") or "0.0.0.0"
API_PORT = int(env("API_PORT", "8000") or "8000")

OPENAI_API_KEY = env("OPENAI_API_KEY")
OPENAI_STT_MODEL = env("OPENAI_STT_MODEL", "gpt-4o-mini-transcribe") or "gpt-4o-mini-transcribe"
