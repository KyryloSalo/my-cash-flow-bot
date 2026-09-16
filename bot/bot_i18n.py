from __future__ import annotations

from contextvars import ContextVar


SUPPORTED_BOT_LOCALES = {"uk", "en"}
DEFAULT_BOT_LOCALE = "uk"
_CURRENT_BOT_LOCALE: ContextVar[str] = ContextVar("CURRENT_BOT_LOCALE", default=DEFAULT_BOT_LOCALE)


def normalize_locale(value: str | None, default: str = DEFAULT_BOT_LOCALE) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    base = raw.split("-", 1)[0].split("_", 1)[0]
    if base.startswith("en"):
        return "en"
    if base in SUPPORTED_BOT_LOCALES:
        return base
    return default


def choose_locale(*values: str | None, default: str = DEFAULT_BOT_LOCALE) -> str:
    for value in values:
        normalized = normalize_locale(value, default="")
        if normalized:
            return normalized
    return default


def set_current_locale(value: str | None) -> str:
    locale = normalize_locale(value)
    _CURRENT_BOT_LOCALE.set(locale)
    return locale


def current_locale(value: str | None = None) -> str:
    if value is not None:
        return normalize_locale(value)
    return normalize_locale(_CURRENT_BOT_LOCALE.get())


def t(uk_text: str, en_text: str, locale: str | None = None, /) -> str:
    return en_text if current_locale(locale) == "en" else uk_text
