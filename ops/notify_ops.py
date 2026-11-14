"""Operational Telegram sender; runs inside the existing bot container.

Credentials remain in the configured runtime. Only API-confirmed message IDs are
reported. A partial or ambiguous send fails closed and is never auto-retried here.
"""
from __future__ import annotations

import asyncio
import json
import sys


async def deliver(text: str, *, bot_factory=None, settings=None) -> list[dict]:
    if settings is None:
        import config as settings
    if bot_factory is None:
        from aiogram import Bot
        bot_factory = Bot
    recipients = list(dict.fromkeys(settings.ADMIN_TELEGRAM_IDS))
    if not settings.BOT_TOKEN or not recipients:
        raise RuntimeError('Operational Telegram delivery is not configured')
    if not text.strip() or len(text) > 3800:
        raise ValueError('Operational message must contain 1..3800 characters')
    bot = bot_factory(token=settings.BOT_TOKEN)
    receipts = []
    try:
        for chat_id in recipients:
            message = await bot.send_message(chat_id=chat_id, text=text)
            if not getattr(message, 'message_id', None):
                raise RuntimeError('Telegram did not return a message receipt')
            receipts.append({'chat_id': chat_id, 'message_id': message.message_id})
    finally:
        await bot.session.close()
    return receipts


def main() -> int:
    try:
        receipts = asyncio.run(deliver(sys.stdin.read()))
    except Exception as exc:
        # Never expose SDK exceptions: they can include authenticated URLs.
        print(json.dumps({'ok': False, 'error_type': type(exc).__name__}))
        return 1
    print(json.dumps({'ok': True, 'receipts': receipts}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
