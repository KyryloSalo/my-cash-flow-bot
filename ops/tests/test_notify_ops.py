import asyncio
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notify_ops import deliver


class OperationalNotificationTests(unittest.TestCase):
    def test_existing_runtime_recipients_and_receipts(self):
        bot = SimpleNamespace(session=SimpleNamespace(close=AsyncMock()),
                              send_message=AsyncMock(return_value=SimpleNamespace(message_id=7)))
        factory = Mock(return_value=bot)
        settings = SimpleNamespace(BOT_TOKEN='synthetic-only', ADMIN_TELEGRAM_IDS=[1, 1, 2])
        result = asyncio.run(deliver('test', bot_factory=factory, settings=settings))
        self.assertEqual([{'chat_id': 1, 'message_id': 7}, {'chat_id': 2, 'message_id': 7}], result)
        self.assertEqual(2, bot.send_message.await_count)
        bot.session.close.assert_awaited_once()

    def test_missing_configuration_refuses_silent_success(self):
        settings = SimpleNamespace(BOT_TOKEN=None, ADMIN_TELEGRAM_IDS=[])
        with self.assertRaises(RuntimeError):
            asyncio.run(deliver('test', bot_factory=Mock(), settings=settings))

    def test_failed_send_is_nonzero_and_closes_session(self):
        bot = SimpleNamespace(session=SimpleNamespace(close=AsyncMock()),
                              send_message=AsyncMock(side_effect=RuntimeError('synthetic')))
        with self.assertRaises(RuntimeError):
            asyncio.run(deliver('test', bot_factory=Mock(return_value=bot),
                                settings=SimpleNamespace(BOT_TOKEN='synthetic-only', ADMIN_TELEGRAM_IDS=[1])))
        bot.session.close.assert_awaited_once()

    def test_empty_and_oversized_messages_are_rejected(self):
        settings = SimpleNamespace(BOT_TOKEN='synthetic-only', ADMIN_TELEGRAM_IDS=[1])
        for text in ['', ' ' * 4, 'x' * 3801]:
            with self.subTest(text_length=len(text)), self.assertRaises(ValueError):
                asyncio.run(deliver(text, bot_factory=Mock(), settings=settings))


if __name__ == '__main__':
    unittest.main()
