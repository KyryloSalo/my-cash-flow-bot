import sys
import types
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

import vision_tx


class PhotoRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def call(self):
        return await vision_tx.analyze_screenshot(
            b'image', mime_type='image/png', today=date(2026, 10, 15),
            default_currency='UAH', accounts=[], expense_categories=[], income_categories=[])

    async def test_disabled_shadow_never_calls_adapter(self):
        result = object()
        adapter = AsyncMock()
        with patch.object(vision_tx, '_analyze_screenshot_legacy', AsyncMock(return_value=result)), \
             patch.object(vision_tx.config, 'PHOTO_PIPELINE_SHADOW_ENABLED', False), \
             patch.dict(sys.modules, {'photo_shadow': types.SimpleNamespace(analyze_shadow=adapter)}):
            self.assertIs(await self.call(), result)
        adapter.assert_not_awaited()

    async def test_enabled_shadow_does_not_replace_legacy_result(self):
        result = object()
        adapter = AsyncMock(return_value=types.SimpleNamespace(accepted=False, findings=('amount_conflict',)))
        with patch.object(vision_tx, '_analyze_screenshot_legacy', AsyncMock(return_value=result)), \
             patch.object(vision_tx.config, 'PHOTO_PIPELINE_SHADOW_ENABLED', True), \
             patch.dict(sys.modules, {'photo_shadow': types.SimpleNamespace(analyze_shadow=adapter)}):
            self.assertIs(await self.call(), result)
        adapter.assert_awaited_once()

    async def test_shadow_errors_do_not_change_result_or_expose_provider_payload(self):
        result = object()
        adapter = AsyncMock(side_effect=RuntimeError('sensitive model payload'))
        with patch.object(vision_tx, '_analyze_screenshot_legacy', AsyncMock(return_value=result)), \
             patch.object(vision_tx.config, 'PHOTO_PIPELINE_SHADOW_ENABLED', True), \
             patch.dict(sys.modules, {'photo_shadow': types.SimpleNamespace(analyze_shadow=adapter)}), \
             self.assertLogs('mcf.vision_tx', level='WARNING') as logs:
            self.assertIs(await self.call(), result)
        self.assertNotIn('sensitive model payload', str(logs.output))
