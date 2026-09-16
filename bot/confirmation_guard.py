"""Bind financial buttons to the exact draft and rendered preview version."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

FINANCIAL_CONFIRMATION_STALE_TEXT = "Це підтвердження застаріло. Відкрийте актуальний перегляд операції."

_META_KEY = "_confirmation_preview"
_SUFFIX = ":pv:"
_BILLING_ACTION = re.compile(r"^btx:([1-9][0-9]*):(.*)$")


def _canonical_action(action: str) -> str:
    match = _BILLING_ACTION.fullmatch(action)
    return "ai:" + match.group(2) if match else action


def _is_confirm_action(action: str) -> bool:
    if _canonical_action(action) in {"ai:tx:ok", "ai:batch:ok"}:
        return True
    return action.endswith(":confirm") or action.endswith(":confirm:ok") or bool(
        re.fullmatch(r"saving:task:confirm:ok:[1-9][0-9]*", action)
    )


def _snapshot(flow: dict) -> str:
    payload = {key: value for key, value in flow.items() if key != _META_KEY}
    encoded = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def bind_financial_preview(flow: dict, markup: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    """Leave navigation untouched; every new render revokes the old confirmation."""
    nonce = secrets.token_urlsafe(8)
    actions = []
    rows = []
    for row in markup.inline_keyboard:
        bound_row = []
        for button in row:
            action = str(button.callback_data or "")
            if _is_confirm_action(action):
                data = action + _SUFFIX + nonce
                if len(data.encode("utf-8")) > 64:
                    raise ValueError("Confirmation callback exceeds Telegram's 64-byte limit")
                actions.append(_canonical_action(action))
                bound_row.append(InlineKeyboardButton(button.text, callback_data=data))
            else:
                bound_row.append(button)
        rows.append(bound_row)
    if not actions:
        raise ValueError("Preview keyboard has no confirmation action")
    flow[_META_KEY] = {"nonce": nonce, "snapshot": _snapshot(flow), "actions": actions}
    return InlineKeyboardMarkup(rows)


def is_current_confirmation(flow: dict, callback_data: str | None) -> bool:
    if not flow or not isinstance(callback_data, str):
        return False
    action, separator, nonce = callback_data.rpartition(_SUFFIX)
    metadata = flow.get(_META_KEY)
    if not separator or not nonce.isascii() or not isinstance(metadata, dict):
        return False
    if not hmac.compare_digest(str(metadata.get("nonce") or ""), nonce):
        return False
    match = _BILLING_ACTION.fullmatch(action)
    if match and str(flow.get("draft_id")) != match.group(1):
        return False
    if _canonical_action(action) not in metadata.get("actions", []):
        return False
    try:
        return hmac.compare_digest(str(metadata.get("snapshot") or ""), _snapshot(flow))
    except (TypeError, ValueError):
        return False


def validated_confirmation_action(flow: dict, callback_data: str | None) -> str | None:
    """Normalize only after validation; unsigned legacy financial writes fail closed."""
    if not isinstance(callback_data, str):
        return None
    action = callback_data.partition(_SUFFIX)[0]
    if _is_confirm_action(action) or _SUFFIX in callback_data:
        return action if is_current_confirmation(flow, callback_data) else None
    return action


def consume_financial_confirmation(flow: dict, callback_data: str | None) -> bool:
    """Atomically consume a valid preview in memory at the commit boundary."""
    if not is_current_confirmation(flow, callback_data):
        return False
    flow.pop(_META_KEY, None)
    return True
