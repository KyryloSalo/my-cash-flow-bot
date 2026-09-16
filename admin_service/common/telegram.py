from __future__ import annotations

import json
from urllib import error, request


class TelegramSendError(Exception):
    def __init__(self, message: str, *, blocked: bool = False, payload: str = "") -> None:
        super().__init__(message)
        self.blocked = blocked
        self.payload = payload


def _request_json(url: str, payload: dict) -> dict:
    encoded = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=encoded, headers={"Content-Type": "application/json"})
    try:
        with request.urlopen(req, timeout=20) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        lowered = raw.lower()
        blocked = "blocked by the user" in lowered or "bot was blocked" in lowered
        raise TelegramSendError(raw, blocked=blocked, payload=raw) from exc
    except error.URLError as exc:
        raise TelegramSendError(str(exc)) from exc

    data = json.loads(body)
    if not data.get("ok"):
        description = str(data.get("description", "Telegram API error"))
        lowered = description.lower()
        blocked = "blocked by the user" in lowered or "bot was blocked" in lowered
        raise TelegramSendError(description, blocked=blocked, payload=body)
    return data


def send_telegram_message(
    *,
    bot_token: str,
    chat_id: int,
    text: str,
    parse_mode: str | None = None,
    buttons: list[list[dict]] | None = None,
    image: str | None = None,
) -> dict:
    reply_markup = {"inline_keyboard": buttons} if buttons else None

    payload: dict = {
        "chat_id": chat_id,
        "disable_web_page_preview": True,
    }
    if parse_mode and parse_mode.lower() != "none":
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup

    if image:
        payload["photo"] = image
        payload["caption"] = text
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    else:
        payload["text"] = text
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    return _request_json(url, payload)


def send_admin_notification(*, bot_token: str, chat_id: int, text: str, buttons: list[list[dict]] | None = None) -> dict:
    return send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=text, buttons=buttons)


def send_support_reply(
    *,
    bot_token: str,
    chat_id: int,
    text: str,
    buttons: list[list[dict]] | None = None,
) -> dict:
    return send_telegram_message(bot_token=bot_token, chat_id=chat_id, text=text, buttons=buttons)


def send_broadcast_message(
    *,
    bot_token: str,
    chat_id: int,
    text: str,
    parse_mode: str | None = None,
    buttons: list[list[dict]] | None = None,
    image: str | None = None,
) -> dict:
    return send_telegram_message(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        parse_mode=parse_mode,
        buttons=buttons,
        image=image,
    )
