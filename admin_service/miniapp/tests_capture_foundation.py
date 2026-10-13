import json
import os
import uuid
from datetime import timedelta
from decimal import Decimal

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import TestCase, override_settings
from django.utils import timezone

from miniapp.capture_service import CREDENTIAL_TTL, CaptureValidationError, _hash_secret, authenticate_submit_credential, ingest_capture_event
from miniapp.models import CaptureConnection, CaptureEvent
from users.models import TelegramUser


@override_settings(AUTO_CAPTURE_ENABLED=True, AUTO_CAPTURE_ANDROID_ENABLED=True)
class CaptureFoundationTests(TestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=99101)
        self.connection = CaptureConnection.objects.create(
            tg_user_id=self.user.tg_user_id,
            platform="android",
            status="active",
            credential_hash="hash",
        )

    def event(self, **changes):
        value = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "source": "android_notification",
            "bank": "mono",
            "observed_at": "2026-10-13T10:00:00+03:00",
            "occurred_at": None,
            "amount": "125.50",
            "currency": "UAH",
            "merchant": "Крамниця",
            "operation_hint": "purchase",
            "parser_version": "android-1",
        }
        value.update(changes)
        return value

    def test_ingest_persists_normalized_event_without_ledger_write(self):
        capture = ingest_capture_event(self.connection, self.event())
        self.assertEqual(capture.status, "pending")
        self.assertEqual(CaptureEvent.objects.count(), 1)
        self.assertEqual(capture.amount, Decimal("125.50"))
        self.assertNotIn("raw", capture.payload)

    def test_retry_is_idempotent_and_changed_payload_conflicts(self):
        payload = self.event()
        first = ingest_capture_event(self.connection, payload)
        second = ingest_capture_event(self.connection, payload)
        self.assertEqual(first.pk, second.pk)
        with self.assertRaises(CaptureValidationError) as error:
            ingest_capture_event(self.connection, {**payload, "amount": "999.00"})
        self.assertEqual(error.exception.code, "event_payload_conflict")
        self.assertEqual(CaptureEvent.objects.count(), 1)
        persisted = CaptureEvent.objects.get(pk=first.pk)
        self.assertEqual(persisted.status, "needs_review")
        self.assertEqual(persisted.source_revision, 2)
        self.assertEqual(persisted.payload_fingerprint, first.payload_fingerprint)

    def test_raw_notification_is_rejected(self):
        with self.assertRaises(CaptureValidationError):
            ingest_capture_event(self.connection, {**self.event(), "raw_notification": "secret push"})

    def test_transfer_is_review_only(self):
        capture = ingest_capture_event(self.connection, self.event(operation_hint="transfer"))
        self.assertEqual(capture.status, "needs_review")

    def test_flags_are_off_by_default(self):
        from django.conf import settings
        self.assertFalse(getattr(settings, "AUTO_CAPTURE_ENABLED", False))
        self.assertFalse(getattr(settings, "AUTO_CAPTURE_ANDROID_ENABLED", False))

    def test_expired_credential_is_rejected(self):
        self.connection.credential_hash = _hash_secret("expired-token")
        self.connection.credential_issued_at = timezone.now() - CREDENTIAL_TTL - timedelta(seconds=1)
        self.connection.save(update_fields=["credential_hash", "credential_issued_at"])
        with self.assertRaises(CaptureValidationError) as error:
            authenticate_submit_credential("expired-token")
        self.assertEqual(error.exception.code, "invalid_credential")
