"""Actual bot HTTP client -> Django ingress contract, with synthetic local data."""
import asyncio
import importlib.util
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.test import LiveServerTestCase, override_settings

from admin_notifications.models import AdminNotificationLog
from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_photo_notifications import deliver_photo_notifications
from users.models import TelegramUser


@override_settings(BILLING_INTERNAL_TOKEN="synthetic-internal", ADMIN_NOTIFICATIONS_ENABLED=True,
                   ADMIN_TELEGRAM_IDS=[99001], TELEGRAM_BOT_TOKEN="synthetic-only")
class TelegramPhotoHTTPContractTests(LiveServerTestCase):
    def test_bot_client_upload_complete_and_admin_original_over_http(self):
        spec = importlib.util.spec_from_file_location(
            "telegram_capture_wire_client", Path(__file__).resolve().parents[2] / "bot" / "recognition_capture_client.py",
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        TelegramUser.objects.create(tg_user_id=9007531)
        RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        original = b"\xff\xd8original-with-recognition-failure"

        async def exercise():
            client = module.RecognitionCaptureClient(retries=0)
            capture_id = await client.create(user_id=9007531, attempt_id="tg-wire-1", image_bytes=original, mime_type="image/jpeg")
            completed_id = await client.complete(user_id=9007531, attempt_id="tg-wire-1", outcome="failed", error_code="vision_invalid_response")
            return capture_id, completed_id

        with TemporaryDirectory(dir=os.environ.get("TMPDIR")) as storage, override_settings(
            RECOGNITION_QUALITY_STORAGE_ROOT=storage
        ), patch.multiple(module.config, BILLING_INTERNAL_BASE_URL=self.live_server_url,
                          BILLING_INTERNAL_TOKEN="synthetic-internal", create=True):
            capture_id, completed_id = asyncio.run(exercise())
            self.assertEqual(capture_id, completed_id)
            capture = RecognitionCapture.objects.get(pk=capture_id)
            self.assertEqual((Path(storage) / capture.raw_media).read_bytes(), original)
            self.assertEqual(capture.outcome, "failed")
            with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                result = deliver_photo_notifications()
            self.assertEqual(result["sent"], 1)
            self.assertEqual(send.call_args.kwargs["content"], original)
            self.assertIn("9007531", send.call_args.kwargs["caption"])
            self.assertEqual(AdminNotificationLog.objects.get().status, "sent")
