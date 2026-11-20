import base64
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

import test_screenshot_flow as screenshot_flow
import vision_tx
from photo_pipeline import ReconciliationResult


class PhotoReviewRejectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_evidence_is_not_a_bot_error_or_a_financial_write(self):
        fixture = screenshot_flow.ScreenshotFlowTests()
        for findings in (("amount_conflict",), ("missing_status_evidence",), ("type_conflict",)):
            with self.subTest(findings=findings):
                evidence = ReconciliationResult(
                    accepted=False, requires_review=True, amount=None, currency=None,
                    status=None, transaction_type=None, evidence_ids=(), findings=findings, trace={},
                )
                conn = screenshot_flow.DummyConn()
                context = fixture._make_context()
                update, message = fixture._make_photo_update()
                drafts = types.SimpleNamespace(
                    get_by_media_key=AsyncMock(return_value=None),
                    upsert_pending_draft=AsyncMock(), update_draft=AsyncMock(),
                )
                bot = screenshot_flow.bot_main
                with (
                    patch.object(bot, "_pool", return_value=screenshot_flow.DummyPool(conn)),
                    patch.object(bot, "is_user_banned", AsyncMock(return_value=False)),
                    patch.object(bot, "should_block_for_maintenance", AsyncMock(return_value=None)),
                    patch.object(bot, "_user_ready", AsyncMock(return_value=True)),
                    patch.object(bot, "_load_ai_catalog", AsyncMock(return_value=fixture._catalog())),
                    patch.object(bot, "_get_user", AsyncMock(return_value={"base_currency": "UAH"})),
                    patch.object(bot, "AiTransactionDraftService", return_value=drafts),
                    patch.object(bot, "notify_admins", AsyncMock()) as notify,
                    patch.object(bot, "log_bot_event", AsyncMock()) as events,
                    patch.object(bot.config, "OPENAI_API_KEY", "test-key"),
                    patch.object(vision_tx.config, "PHOTO_PIPELINE_ENABLED", True),
                    patch.dict(sys.modules, {"photo_shadow": types.SimpleNamespace(
                        analyze_photo=AsyncMock(return_value=evidence))}),
                ):
                    await bot.photo_message(update, context)
                notify.assert_not_awaited()
                self.assertFalse(any(call.args[2] == "bot_error" for call in events.await_args_list))
                drafts.upsert_pending_draft.assert_not_awaited()
                drafts.update_draft.assert_not_awaited()
                self.assertFalse(any(
                    "INSERT INTO transactions" in query or "UPDATE accounts" in query
                    for query, _ in conn.execute_calls
                ))
                self.assertNotIn("ai_tx_flow", context.user_data)
                self.assertIn("не створено", message.replies[-1]["text"])
                self.assertIn("PWA", message.replies[-1]["text"])

    async def test_real_provider_failures_still_alert(self):
        fixture = screenshot_flow.ScreenshotFlowTests()
        bot = screenshot_flow.bot_main
        for error in (vision_tx.OpenAIVisionError("provider unavailable"),
                      vision_tx.OpenAIVisionInvalidResponseError("malformed response")):
            with self.subTest(error=type(error).__name__):
                context = fixture._make_context()
                update, message = fixture._make_photo_update()
                drafts = types.SimpleNamespace(get_by_media_key=AsyncMock(return_value=None))
                with (
                    patch.object(bot, "_pool", return_value=screenshot_flow.DummyPool(screenshot_flow.DummyConn())),
                    patch.object(bot, "is_user_banned", AsyncMock(return_value=False)),
                    patch.object(bot, "should_block_for_maintenance", AsyncMock(return_value=None)),
                    patch.object(bot, "_user_ready", AsyncMock(return_value=True)),
                    patch.object(bot, "_load_ai_catalog", AsyncMock(return_value=fixture._catalog())),
                    patch.object(bot, "_get_user", AsyncMock(return_value={"base_currency": "UAH"})),
                    patch.object(bot, "AiTransactionDraftService", return_value=drafts),
                    patch.object(bot, "analyze_screenshot", AsyncMock(side_effect=error)),
                    patch.object(bot, "notify_admins", AsyncMock()) as notify,
                    patch.object(bot, "log_bot_event", AsyncMock()) as events,
                    patch.object(bot.config, "OPENAI_API_KEY", "test-key"),
                ):
                    await bot.photo_message(update, context)
                notify.assert_awaited_once()
                self.assertTrue(any(call.args[2] == "bot_error" for call in events.await_args_list))

    async def test_pwa_api_separates_review_from_provider_outage(self):
        import api_server

        app = api_server.create_app()
        endpoint = next(route.endpoint for route in app.routes
                        if getattr(route, "path", None) == "/internal/miniapp/parse-image")
        payload = api_server.MiniAppImageParseRequest(
            image_base64=base64.b64encode(b"image").decode("ascii"), mime_type="image/png")
        for error, status in (
            (vision_tx.PhotoEvidenceReviewRequired("Photo evidence requires review"), 400),
            (vision_tx.OpenAIVisionInvalidResponseError("bad response"), 503),
            (vision_tx.OpenAIVisionError("provider down"), 503),
        ):
            with self.subTest(error=type(error).__name__):
                with patch.object(api_server, "analyze_screenshot", AsyncMock(side_effect=error)):
                    with self.assertRaises(api_server.HTTPException) as caught:
                        await endpoint(payload)
                self.assertEqual(caught.exception.status_code, status)


if __name__ == "__main__":
    unittest.main()
