"""Synthetic Telegram ingress acceptance; no live transport or credentials."""
from pathlib import Path
from tempfile import TemporaryDirectory
import os
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, TestCase, override_settings

from admin_notifications.models import AdminNotificationLog
from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
from miniapp.recognition_internal import internal_recognition_photo
from miniapp.recognition_photo_notifications import deliver_photo_notifications
from users.models import TelegramUser


@override_settings(BILLING_INTERNAL_TOKEN="synthetic-internal", ADMIN_NOTIFICATIONS_ENABLED=True,
                   ADMIN_TELEGRAM_IDS=[99001], TELEGRAM_BOT_TOKEN="synthetic-only")
class TelegramPhotoIngressTests(TestCase):
    path = "/internal/recognition/photo"
    user_id = 9007521
    attempt = "tg-9007521-77"
    original = b"unaltered-original-image-not-provider-output"

    def setUp(self):
        TelegramUser.objects.create(tg_user_id=self.user_id)
        self.cfg = RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True)
        self.directory = TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.directory.cleanup)
        configured = override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=self.directory.name)
        configured.enable()
        self.addCleanup(configured.disable)

    def upload(self, **overrides):
        fields = {"action": "create", "user_id": str(self.user_id), "attempt_id": self.attempt,
                  "image": SimpleUploadedFile("original.jpg", self.original, content_type="image/jpeg")}
        fields.update(overrides)
        return self.client.post(self.path, fields, HTTP_X_INTERNAL_TOKEN="synthetic-internal")

    def complete(self, **overrides):
        fields = {"action": "complete", "user_id": self.user_id, "attempt_id": self.attempt,
                  "outcome": "failed", "error_code": "photo_review_required", "normalized_result": {}}
        fields.update(overrides)
        return self.client.post(self.path, fields, content_type="application/json",
                                HTTP_X_INTERNAL_TOKEN="synthetic-internal")

    def test_unauthorized_ingress_has_no_side_effects(self):
        response = self.client.post(self.path, {"action": "create"})
        self.assertEqual(response.status_code, 403)
        self.assertFalse(RecognitionCapture.objects.exists())
        self.assertFalse(AdminNotificationLog.objects.exists())

    def test_disabled_capture_does_not_bypass_activation(self):
        self.cfg.enabled = False
        self.cfg.save()
        response = self.upload()
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["capture_id"])
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_original_delivers_after_exact_recognition_review_failure(self):
        response = self.upload()
        self.assertEqual(response.status_code, 200)
        capture = RecognitionCapture.objects.get(pk=response.json()["capture_id"])
        self.assertEqual(capture.user_id, self.user_id)
        self.assertEqual((Path(self.directory.name) / capture.raw_media).read_bytes(), self.original)
        response = self.complete()
        self.assertEqual(response.status_code, 200)
        capture.refresh_from_db()
        self.assertEqual((capture.outcome, capture.error_code), ("failed", "photo_review_required"))
        with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
            result = deliver_photo_notifications()
        self.assertEqual(result["sent"], 1)
        self.assertEqual(send.call_args.kwargs["content"], self.original)
        self.assertIn("outcome=failed", send.call_args.kwargs["caption"])
        self.assertEqual(AdminNotificationLog.objects.get().status, "sent")

    def test_retried_upload_is_idempotent(self):
        first = self.upload()
        self.assertEqual(first.status_code, 200)
        second = self.upload()
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["capture_id"], second.json()["capture_id"])
        self.assertEqual(RecognitionCapture.objects.count(), 1)
        self.assertEqual(AdminNotificationLog.objects.filter(event_type="recognition_photo").count(), 1)
        self.assertEqual(len(list(Path(self.directory.name).glob("*.bin"))), 1)

    def test_other_user_cannot_complete_existing_capture(self):
        self.assertEqual(self.upload().status_code, 200)
        TelegramUser.objects.create(tg_user_id=9007522)
        response = self.complete(user_id=9007522)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(RecognitionCapture.objects.get().outcome, "pending")

    def test_oversized_upload_rejected_without_capture(self):
        with patch("miniapp.recognition_internal.MAX_MEDIA_BYTES", 8):
            response = self.upload()
        self.assertEqual(response.status_code, 413)
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_oversized_multipart_is_rejected_before_parsing_or_spooling(self):
        request = RequestFactory().post(
            self.path,
            {"action": "create", "user_id": str(self.user_id), "attempt_id": self.attempt,
             "image": SimpleUploadedFile("large.jpg", b"x" * (1024 + 65536), content_type="image/jpeg")},
            HTTP_X_INTERNAL_TOKEN="synthetic-internal",
        )
        with patch("miniapp.recognition_internal.MAX_MEDIA_BYTES", 1024), patch.object(
            request, "parse_file_upload", wraps=request.parse_file_upload
        ) as parse:
            response = internal_recognition_photo(request)
        self.assertEqual(response.status_code, 413)
        parse.assert_not_called()
        self.assertFalse(RecognitionCapture.objects.exists())
        self.assertFalse(AdminNotificationLog.objects.exists())

    def test_body_read_is_bounded_when_content_length_is_missing_or_understated(self):
        for declared_length in (None, "1"):
            with self.subTest(content_length=declared_length):
                request = RequestFactory().post(
                    self.path,
                    {"image": SimpleUploadedFile("large.jpg", b"x" * (1024 + 65536), content_type="image/jpeg")},
                    HTTP_X_INTERNAL_TOKEN="synthetic-internal",
                )
                if declared_length is None:
                    request.META.pop("CONTENT_LENGTH")
                else:
                    request.META["CONTENT_LENGTH"] = declared_length
                with patch("miniapp.recognition_internal.MAX_MEDIA_BYTES", 1024), patch.object(
                    request, "parse_file_upload", wraps=request.parse_file_upload
                ) as parse, patch.object(request, "read", wraps=request.read) as read:
                    response = internal_recognition_photo(request)
                self.assertEqual(response.status_code, 413)
                read.assert_called_once_with(1024 + 65536 + 1)
                parse.assert_not_called()
        self.assertFalse(RecognitionCapture.objects.exists())

    def test_malformed_original_is_preserved_without_image_decoding(self):
        self.original = b"\xff\xd8broken-jpeg-original"
        response = self.upload()
        self.assertEqual(response.status_code, 200)
        capture = RecognitionCapture.objects.get(pk=response.json()["capture_id"])
        self.assertEqual((Path(self.directory.name) / capture.raw_media).read_bytes(), self.original)

    def test_bounded_upload_closes_temporary_files(self):
        with override_settings(FILE_UPLOAD_MAX_MEMORY_SIZE=0, FILE_UPLOAD_TEMP_DIR=self.directory.name):
            response = self.upload()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(list(Path(self.directory.name).iterdir())), 1)
        self.assertEqual(len(list(Path(self.directory.name).glob("*.bin"))), 1)

    def test_paused_admin_delivery_still_creates_durable_outbox(self):
        with override_settings(ADMIN_NOTIFICATIONS_ENABLED=False):
            response = self.upload()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(AdminNotificationLog.objects.get().status, "queued")

    def test_invalid_completion_does_not_save_raw_error_or_arbitrary_result(self):
        self.assertEqual(self.upload().status_code, 200)
        response = self.complete(error_code="Photo evidence requires review")
        self.assertEqual(response.status_code, 400)
        response = self.complete(normalized_result={"raw_provider_response": "private contents"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(RecognitionCapture.objects.get().outcome, "pending")

    def test_storage_failure_is_explicit_not_false_success(self):
        with patch("miniapp.recognition_quality.capture_attempt", return_value=None):
            response = self.upload()
        self.assertEqual(response.status_code, 503)
        self.assertFalse(AdminNotificationLog.objects.exists())
