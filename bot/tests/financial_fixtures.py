"""Confirmation helpers for legacy tests; never bypass the real guard."""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from confirmation_guard import bind_financial_preview, is_current_confirmation


def confirmation_from_reply(message, action: str) -> str:
    """Use the actual button rendered by the product, not a fabricated nonce."""
    for reply in reversed(message.replies):
        markup = reply.get("reply_markup")
        for row in getattr(markup, "inline_keyboard", ()):
            for button in row:
                data = str(button.callback_data or "")
                if data.startswith(action + ":pv:"):
                    return data
    raise AssertionError(f"No rendered confirmation button for {action!r}")


def prepare_confirmation(flow: dict, action: str) -> str:
    """Bind a deliberately prepared unit fixture with the production guard.

    Use only when a test starts at the confirm seam. End-to-end flows should
    instead take confirmation_from_reply; repeated taps must reuse the token.
    """
    markup = bind_financial_preview(
        flow, InlineKeyboardMarkup([[InlineKeyboardButton("Confirm fixture", callback_data=action)]])
    )
    data = markup.inline_keyboard[0][0].callback_data
    assert is_current_confirmation(flow, data)
    return data
