from __future__ import annotations

from decimal import Decimal, InvalidOperation

from django.urls import NoReverseMatch, reverse
from django.utils.html import format_html


POSITIVE_STATES = {
    "active",
    "paid",
    "success",
    "sent",
    "completed",
    "open",
    "manual_confirmed",
    "refunded",
}
WARNING_STATES = {
    "pending",
    "draft",
    "trial",
    "grace",
    "scheduled",
    "processing",
    "action_required",
}
DANGER_STATES = {
    "failed",
    "rejected",
    "blocked",
    "cancelled",
    "canceled",
    "error",
    "expired",
}


def status_tone(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in POSITIVE_STATES:
        return "positive"
    if normalized in WARNING_STATES:
        return "warning"
    if normalized in DANGER_STATES:
        return "danger"
    return "info"


def badge(label: object, *, tone: str = "info"):
    return format_html('<span class="op-status op-status--{}">{}</span>', tone, label or "—")


def status_badge(value: object, label: object | None = None):
    return badge(label if label is not None else value, tone=status_tone(value))


def boolean_badge(value: bool, *, true_label: str = "Так", false_label: str = "Ні"):
    return badge(true_label if value else false_label, tone="positive" if value else "info")


def user_identity(user):
    if user is None:
        return "—"

    full_name = " ".join(
        part.strip()
        for part in (str(getattr(user, "first_name", "") or ""), str(getattr(user, "last_name", "") or ""))
        if part.strip()
    )
    username = str(getattr(user, "username", "") or "").strip().lstrip("@")
    user_id = getattr(user, "tg_user_id", getattr(user, "pk", ""))
    name = full_name or (f"@{username}" if username else f"Користувач {user_id}")
    meta = " · ".join(part for part in (f"@{username}" if username and name != f"@{username}" else "", f"ID {user_id}" if user_id else "") if part)

    try:
        url = reverse("admin:users_telegramuser_change", args=[user_id])
    except NoReverseMatch:
        url = ""

    if url:
        return format_html(
            '<a class="op-identity" href="{}"><strong>{}</strong><span>{}</span></a>',
            url,
            name,
            meta,
        )
    return format_html('<span class="op-identity"><strong>{}</strong><span>{}</span></span>', name, meta)


def money_value(amount: object, currency: object = ""):
    try:
        numeric = Decimal(str(amount or 0))
        rendered = f"{numeric:,.2f}".replace(",", " ")
    except (InvalidOperation, TypeError, ValueError):
        rendered = str(amount or "0")
    return format_html(
        '<span class="op-money"><strong>{}</strong><span>{}</span></span>',
        rendered,
        str(currency or ""),
    )


def compact_text(value: object, *, limit: int = 76) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return "—"
    return text if len(text) <= limit else f"{text[: limit - 1]}…"
