"""Regression for the reported Telegram review failure; no live network."""
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from contextlib import ExitStack
import test_screenshot_flow as fixture


class TelegramPhotoCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def _exercise_capture(self, api_key):
        helpers = fixture.ScreenshotFlowTests()
        update, message = helpers._make_photo_update(b'literal-original')
        context = helpers._make_context()
        bot = fixture.bot_main
        capture = SimpleNamespace(create=AsyncMock(return_value=41), complete=AsyncMock())
        mocks = {
            '_pool': fixture.DummyPool(fixture.DummyConn()),
            'is_user_banned': AsyncMock(return_value=False),
            'should_block_for_maintenance': AsyncMock(return_value=None),
            'log_bot_event': AsyncMock(), '_user_ready': AsyncMock(return_value=True),
            '_load_ai_catalog': AsyncMock(return_value=helpers._catalog()),
            '_get_user': AsyncMock(return_value={'base_currency': 'UAH'}),
            'AiTransactionDraftService': SimpleNamespace(get_by_media_key=AsyncMock(return_value=None)),
            'RecognitionCaptureClient': capture,
            'analyze_screenshot': AsyncMock(side_effect=fixture.OpenAIVisionInvalidResponseError('Photo evidence requires review')),
            'notify_admins': AsyncMock(),
        }
        with ExitStack() as stack:
            for name, value in mocks.items():
                stack.enter_context(patch.object(bot, name, return_value=value, create=True) if name in ('_pool', 'AiTransactionDraftService', 'RecognitionCaptureClient') else patch.object(bot, name, new=value))
            stack.enter_context(patch.object(bot.config, 'OPENAI_API_KEY', api_key))
            await bot.photo_message(update, context)
        capture.create.assert_awaited_once_with(user_id=123, attempt_id='tg-77', image_bytes=b'literal-original', mime_type='image/jpeg')
        code = 'vision_invalid_response' if api_key else 'vision_config'
        capture.complete.assert_awaited_once_with(user_id=123, attempt_id='tg-77', outcome='failed', error_code=code)
        self.assertNotIn('ai_tx_flow', context.user_data)

    async def test_review_failure_captures_original_before_parser(self):
        await self._exercise_capture('synthetic-only')

    async def test_unconfigured_recognition_still_captures_original(self):
        await self._exercise_capture(None)
