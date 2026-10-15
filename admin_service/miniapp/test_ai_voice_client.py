from __future__ import annotations

from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError

from django.test import SimpleTestCase, override_settings

from miniapp.ai_voice_client import MiniAppAiVoiceError, request_ai_voice_parse


@override_settings(
    BILLING_INTERNAL_TOKEN="test-token",
    MINIAPP_BOT_INTERNAL_BASE_URL="http://bot-api",
    MINIAPP_BOT_INTERNAL_AI_TIMEOUT_SECONDS=10,
)
class AiVoiceClientTests(SimpleTestCase):
    def test_bad_request_preserves_safe_bot_api_detail(self) -> None:
        error = HTTPError(
            "http://bot-api/internal/miniapp/parse-audio",
            400,
            "Bad Request",
            {},
            BytesIO(
                b'{"detail":"I found multiple operations. Please send each operation separately."}'
            ),
        )

        with patch("miniapp.ai_voice_client.urlopen", side_effect=error):
            with self.assertRaises(MiniAppAiVoiceError) as raised:
                request_ai_voice_parse(
                    audio_bytes=b"audio",
                    mime_type="audio/ogg",
                    default_currency="UAH",
                )

        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(
            str(raised.exception),
            "I found multiple operations. Please send each operation separately.",
        )

    def test_bad_request_falls_back_when_bot_api_detail_is_not_json(self) -> None:
        error = HTTPError(
            "http://bot-api/internal/miniapp/parse-audio",
            400,
            "Bad Request",
            {},
            BytesIO(b"not-json"),
        )

        with patch("miniapp.ai_voice_client.urlopen", side_effect=error):
            with self.assertRaises(MiniAppAiVoiceError) as raised:
                request_ai_voice_parse(
                    audio_bytes=b"audio",
                    mime_type="audio/ogg",
                    default_currency="UAH",
                )

        self.assertEqual(str(raised.exception), "Voice message could not be processed.")
