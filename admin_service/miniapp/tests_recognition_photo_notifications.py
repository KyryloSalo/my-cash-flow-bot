from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.test import TestCase, override_settings
from django.utils import timezone

from admin_notifications.models import AdminNotificationLog
from miniapp.models import RecognitionCapture


class PhotoNotificationTests(TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_delivers_exact_private_original_and_marks_sent(self):
        key = "a" * 32 + ".bin"
        original = b"HEIC\x00synthetic-original"
        (self.root / key).write_bytes(original)
        capture = RecognitionCapture.objects.create(attempt_id="a", user_id=42, source="photo", raw_media=key,
            raw_media_size=len(original), mime_type="image/heic", outcome="parsed", normalized_result={"amount":"12.30", "currency":"TRY"})
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk})
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                from miniapp.recognition_photo_notifications import deliver_photo_notifications
                self.assertEqual(deliver_photo_notifications([log.pk])["sent"], 1)
        send.assert_called_once()
        self.assertEqual(send.call_args.kwargs["content"], original)
        self.assertEqual(AdminNotificationLog.objects.get(pk=log.pk).status, "sent")

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_pending_parse_waits_for_final_outcome_before_sending(self):
        key = "d" * 32 + ".bin"
        original = b"pending-original"
        (self.root / key).write_bytes(original)
        capture = RecognitionCapture.objects.create(attempt_id="pending", user_id=45, source="image", raw_media=key,
            raw_media_size=len(original), mime_type="image/png")
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk})
        from miniapp.recognition_photo_notifications import deliver_photo_notifications
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                result = deliver_photo_notifications([log.pk])
                self.assertEqual(result["sent"], 0)
                send.assert_not_called()
                log.refresh_from_db()
                self.assertEqual(log.status, "queued")
                from miniapp.recognition_quality import update_capture
                update_capture(user_id=45, attempt_id="pending", outcome="parsed",
                    normalized_result={"amount": "17.00", "currency": "UAH"})
                self.assertEqual(deliver_photo_notifications([log.pk])["sent"], 1)
        self.assertIn("outcome=parsed", send.call_args.kwargs["caption"])
        self.assertIn("amount=17.00", send.call_args.kwargs["caption"])

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_crashed_parse_original_is_delivered_after_bounded_wait(self):
        key = "e" * 32 + ".bin"
        original = b"orphaned-original"
        (self.root / key).write_bytes(original)
        capture = RecognitionCapture.objects.create(attempt_id="orphan", user_id=46, source="image", raw_media=key,
            raw_media_size=len(original), mime_type="image/jpeg")
        RecognitionCapture.objects.filter(pk=capture.pk).update(created_at=timezone.now() - timedelta(minutes=3))
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk})
        from miniapp.recognition_photo_notifications import deliver_photo_notifications
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                self.assertEqual(deliver_photo_notifications([log.pk])["sent"], 1)
        self.assertEqual(send.call_args.kwargs["content"], original)
        self.assertIn("outcome=pending", send.call_args.kwargs["caption"])

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_sent_log_is_not_resendable(self):
        capture = RecognitionCapture.objects.create(attempt_id="a", user_id=42, source="photo", raw_media="")
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk}, status="sent")
        from miniapp.recognition_photo_notifications import deliver_photo_notifications
        with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
            self.assertEqual(deliver_photo_notifications([log.pk])["sent"], 0)
        send.assert_not_called()

    def test_caption_preserves_supported_parse_and_draft_outcomes(self):
        from miniapp.recognition_photo_notifications import _caption
        for outcome in ("redirect", "draft"):
            with self.subTest(outcome=outcome):
                capture = RecognitionCapture(user_id=47, source="image", outcome=outcome)
                self.assertIn(f"outcome={outcome}", _caption(capture))

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_extension_caption_and_retryable_failure_are_safe(self):
        key = "b" * 32 + ".bin"
        original = b"png-original"
        (self.root / key).write_bytes(original)
        capture = RecognitionCapture.objects.create(attempt_id="b", user_id=43, source="batch", raw_media=key,
            raw_media_size=len(original), mime_type="image/png", outcome="cancelled",
            normalized_result={"item_count": 3, "raw_text": "DO NOT LEAK"})
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk})
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.send_telegram_document", side_effect=Exception("provider secret")) as send:
                from miniapp.recognition_photo_notifications import deliver_photo_notifications
                self.assertEqual(deliver_photo_notifications([log.pk])["failed"], 1)
        self.assertEqual(AdminNotificationLog.objects.get(pk=log.pk).status, "failed")
        self.assertNotIn("provider", AdminNotificationLog.objects.get(pk=log.pk).error_message)

        AdminNotificationLog.objects.filter(pk=log.pk).update(next_retry_at=timezone.now() - timedelta(seconds=1))
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                from miniapp.recognition_photo_notifications import deliver_photo_notifications
                self.assertEqual(deliver_photo_notifications([log.pk])["sent"], 1)
        self.assertEqual(send.call_args.kwargs["filename"], f"recognition-{capture.pk}.png")
        caption = send.call_args.kwargs["caption"]
        self.assertIn("outcome=cancelled", caption)
        self.assertIn("item_count=3", caption)
        self.assertNotIn("DO NOT LEAK", caption)

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, TELEGRAM_BOT_TOKEN="synthetic", ADMIN_TELEGRAM_IDS=[9001])
    def test_symlink_and_oversized_original_are_rejected(self):
        key = "c" * 32 + ".bin"
        (self.root / key).write_bytes(b"secret")
        capture = RecognitionCapture.objects.create(attempt_id="c", user_id=44, source="photo", raw_media=key,
            raw_media_size=6, mime_type="image/jpeg", outcome="failed")
        log = AdminNotificationLog.objects.create(event_type="recognition_photo", telegram_chat_id=9001,
                                                   payload={"capture_id": capture.pk})
        with override_settings(RECOGNITION_QUALITY_STORAGE_ROOT=str(self.root)):
            with patch("miniapp.recognition_photo_notifications.Path.is_symlink", return_value=True), patch("miniapp.recognition_photo_notifications.send_telegram_document") as send:
                from miniapp.recognition_photo_notifications import deliver_photo_notifications
                self.assertEqual(deliver_photo_notifications([log.pk])["failed"], 1)
        send.assert_not_called()