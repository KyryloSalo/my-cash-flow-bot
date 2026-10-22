from __future__ import annotations

import json
import mimetypes
import secrets
from urllib import error, request

from common.telegram import TelegramSendError


def send_telegram_document(*, bot_token: str, chat_id: int, filename: str, content: bytes, mime_type: str = "application/octet-stream", caption: str = "") -> dict:
    """Send original bytes as Telegram sendDocument using stdlib multipart."""
    if not bot_token or not isinstance(content, (bytes, bytearray)):
        raise TelegramSendError("telegram_document_invalid_request")
    boundary = "----hermes" + secrets.token_hex(12)
    safe_name = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1] or "document.bin"
    safe_name = "".join(ch for ch in safe_name if ch.isalnum() or ch in ".-_")[:120] or "document.bin"
    fields = [("chat_id", str(chat_id)), ("caption", caption[:1024])]
    parts = []
    for name, value in fields:
        parts.append((f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").encode())
    parts.append((f"--{boundary}\r\nContent-Disposition: form-data; name=\"document\"; filename=\"{safe_name}\"\r\nContent-Type: {mime_type or mimetypes.guess_type(safe_name)[0] or 'application/octet-stream'}\r\n\r\n").encode() + bytes(content) + b"\r\n")
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    req = request.Request(
        f"https://api.telegram.org/bot{bot_token}/sendDocument",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}", "Content-Length": str(len(body))},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=20) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except (error.HTTPError, error.URLError, TimeoutError, OSError) as exc:
        raise TelegramSendError("telegram_document_delivery_failed") from exc
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise TelegramSendError("telegram_document_invalid_response") from exc
    if not isinstance(data, dict) or data.get("ok") is not True:
        raise TelegramSendError("telegram_document_rejected")
    return {"ok": True}
