from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from bot import stt


class _Response:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"text": "Комуналка сім триста"}


class _Client:
    def __init__(self, *, timeout: int) -> None:
        self.timeout = timeout
        self.post = AsyncMock(return_value=_Response())

    async def __aenter__(self) -> "_Client":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class SttTests(unittest.IsolatedAsyncioTestCase):
    async def test_transcription_prompt_preserves_spoken_number_words(self) -> None:
        client = _Client(timeout=30)
        with (
            patch.object(stt, "OPENAI_API_KEY", "test-key"),
            patch.object(stt.httpx, "AsyncClient", return_value=client),
        ):
            result = await stt.transcribe_audio_bytes(
                b"audio",
                filename="voice.ogg",
                mime_type="audio/ogg",
            )

        self.assertEqual(result.text, "Комуналка сім триста")
        data = client.post.await_args.kwargs["data"]
        self.assertIn("Preserve number words as spoken", data["prompt"])
        self.assertEqual(data["model"], stt.OPENAI_STT_MODEL)


if __name__ == "__main__":
    unittest.main()
