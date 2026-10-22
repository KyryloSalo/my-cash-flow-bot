from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings


class TelegramUploadTests(SimpleTestCase):
    @override_settings(TELEGRAM_BOT_TOKEN="synthetic-token")
    def test_document_upload_uses_original_bytes_and_bounded_caption(self):
        from common.telegram_upload import send_telegram_document

        seen = {}

        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return b'{"ok": true, "result": {}}'

        def fake_urlopen(req, timeout):
            seen["body"] = req.data
            seen["headers"] = dict(req.headers)
            return Response()

        original = b"\x00\x01synthetic-heic-bytes"
        with patch("common.telegram_upload.request.urlopen", fake_urlopen):
            result = send_telegram_document(bot_token="synthetic-token", chat_id=7,
                                            filename="receipt.heic", content=original,
                                            mime_type="image/heic", caption="x" * 2000)
        self.assertTrue(result["ok"])
        self.assertIn(original, seen["body"])
        self.assertIn(b'filename="receipt.heic"', seen["body"])
        self.assertIn(b"image/heic", seen["body"])
        self.assertLessEqual(seen["body"].count(b"x"), 1024)

    def test_document_upload_sanitizes_provider_errors(self):
        from common.telegram_upload import send_telegram_document
        from common.telegram import TelegramSendError
        from urllib.error import URLError
        with patch("common.telegram_upload.request.urlopen", side_effect=URLError("token-secret")):
            with self.assertRaisesRegex(TelegramSendError, "telegram_document_delivery_failed"):
                send_telegram_document(bot_token="secret", chat_id=7, filename="x", content=b"x")

    def test_document_upload_rejects_nonobject_or_false_success_without_leaking_response(self):
        from common.telegram_upload import send_telegram_document
        from common.telegram import TelegramSendError
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def __init__(self, payload): self.payload = payload
            def read(self): return self.payload
        for payload in (b'[]', b'{"ok": false, "token": "secret"}'):
            with patch("common.telegram_upload.request.urlopen", return_value=Response(payload)):
                with self.assertRaisesRegex(TelegramSendError, "telegram_document_rejected") as ctx:
                    send_telegram_document(bot_token="secret", chat_id=7, filename="x", content=b"x")
            self.assertNotIn("secret", str(ctx.exception))
