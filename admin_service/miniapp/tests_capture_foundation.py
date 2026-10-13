import json
import os
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.core.cache import cache
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from miniapp.capture_service import (
    CREDENTIAL_TTL,
    CaptureValidationError,
    _hash_secret,
    approve_pairing,
    authenticate_submit_credential,
    capture_enabled_for_user,
    ingest_capture_event,
    issue_pairing_challenge,
)
from miniapp import capture_views
from miniapp.models import CaptureConnection, CaptureEvent
from users.models import TelegramUser
from users.services import HardDeleteStats, _delete_capture_rows


@override_settings(
    AUTO_CAPTURE_ENABLED=True,
    AUTO_CAPTURE_ANDROID_ENABLED=True,
    AUTO_CAPTURE_ALLOWLIST_TG_IDS=(99101,),
)
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
        capture.refresh_from_db()
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

    @override_settings(AUTO_CAPTURE_ALLOWLIST_TG_IDS=())
    def test_ingest_is_closed_when_allowlist_is_empty(self):
        with self.assertRaises(CaptureValidationError) as error:
            ingest_capture_event(self.connection, self.event())
        self.assertEqual(error.exception.code, "capture_disabled")

    def test_expired_credential_is_rejected(self):
        self.connection.credential_hash = _hash_secret("expired-token")
        self.connection.credential_issued_at = timezone.now() - CREDENTIAL_TTL - timedelta(seconds=1)
        self.connection.save(update_fields=["credential_hash", "credential_issued_at"])
        with self.assertRaises(CaptureValidationError) as error:
            authenticate_submit_credential("expired-token")
        self.assertEqual(error.exception.code, "invalid_credential")


class CaptureRolloutTests(SimpleTestCase):
    def setUp(self):
        self.request = RequestFactory().get("/app/api/capture/v1/status")

    def test_flags_are_off_by_default(self):
        from django.conf import settings

        self.assertFalse(getattr(settings, "AUTO_CAPTURE_ENABLED", False))
        self.assertFalse(getattr(settings, "AUTO_CAPTURE_ANDROID_ENABLED", False))

    @override_settings(
        AUTO_CAPTURE_ENABLED=True,
        AUTO_CAPTURE_ANDROID_ENABLED=True,
        AUTO_CAPTURE_ALLOWLIST_TG_IDS=(99101,),
    )
    def test_only_allowlisted_user_and_enabled_platform_are_eligible(self):
        self.assertTrue(capture_enabled_for_user(99101, platform="android"))
        self.assertFalse(capture_enabled_for_user(99102, platform="android"))
        self.assertFalse(capture_enabled_for_user(99101, platform="ios"))

    @override_settings(
        AUTO_CAPTURE_ENABLED=True,
        AUTO_CAPTURE_ANDROID_ENABLED=True,
        AUTO_CAPTURE_ALLOWLIST_TG_IDS=(),
    )
    def test_empty_allowlist_fails_closed(self):
        self.assertFalse(capture_enabled_for_user(99101, platform="android"))

    @override_settings(
        AUTO_CAPTURE_ENABLED=True,
        AUTO_CAPTURE_ANDROID_ENABLED=True,
        AUTO_CAPTURE_ALLOWLIST_TG_IDS=(99101,),
    )
    @patch("miniapp.capture_views._user")
    def test_status_exposes_android_only_to_allowlisted_user(self, session_user):
        session_user.return_value.tg_user_id = 99101
        response = capture_views.status(self.request)
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"enabled": True, "android": True, "ios": False})

    @override_settings(
        AUTO_CAPTURE_ENABLED=True,
        AUTO_CAPTURE_ANDROID_ENABLED=True,
        AUTO_CAPTURE_ALLOWLIST_TG_IDS=(99101,),
    )
    @patch("miniapp.capture_views._user")
    def test_status_fails_closed_for_non_allowlisted_user(self, session_user):
        session_user.return_value.tg_user_id = 99102
        response = capture_views.status(self.request)
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"enabled": False, "android": False, "ios": False})

    @override_settings(
        AUTO_CAPTURE_ENABLED=True,
        AUTO_CAPTURE_ANDROID_ENABLED=True,
        AUTO_CAPTURE_ALLOWLIST_TG_IDS=(99101,),
    )
    def test_capture_access_error_blocks_non_allowlisted_user(self):
        allowed_user = type("User", (), {"tg_user_id": 99101})()
        blocked_user = type("User", (), {"tg_user_id": 99102})()
        self.assertIsNone(capture_views.capture_access_error(allowed_user, platform="android"))
        response = capture_views.capture_access_error(blocked_user, platform="android")
        self.assertEqual(response.status_code, 404)
        self.assertJSONEqual(response.content, {"error": {"code": "capture_unavailable"}})

    @patch("miniapp.capture_service.CaptureConnection.objects.filter")
    @patch("miniapp.capture_service._limit")
    def test_invalid_ingest_credentials_are_rate_limited(self, limit, connections):
        connections.return_value.order_by.return_value = []
        with self.assertRaises(CaptureValidationError) as error:
            authenticate_submit_credential("invalid-token", rate_key="203.0.113.7")
        self.assertEqual(error.exception.code, "invalid_credential")
        limit.assert_any_call("ingest-ip", "203.0.113.7", 120, window=300)
        limit.assert_any_call("ingest-invalid", _hash_secret("invalid-token")[:16], 10, window=300)

    @patch("miniapp.capture_views.authenticate_submit_credential")
    def test_ingest_rejects_invalid_credential_as_unauthorized(self, authenticate):
        authenticate.side_effect = CaptureValidationError("invalid_credential")
        request = RequestFactory().post(
            "/app/api/capture/v1/events",
            data="{}",
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer invalid-token",
            REMOTE_ADDR="203.0.113.7",
        )
        response = capture_views.ingest(request)
        self.assertEqual(response.status_code, 401)
class CaptureFrontendContractTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.source = (Path(__file__).parent / "static" / "miniapp" / "capture.js").read_text(encoding="utf-8")

    def test_account_choices_are_limited_to_capture_currency(self):
        self.assertIn("accounts.filter((account) => account.currency === event.currency)", self.source)
        self.assertNotIn("|| accounts[0]", self.source)

    def test_confirmation_reuses_one_idempotency_key_across_retries(self):
        self.assertIn("const idempotencyKey = crypto.randomUUID", self.source)
        self.assertIn("idempotency_key: idempotencyKey", self.source)

    def test_inbox_failures_remain_visible_and_retryable(self):
        self.assertIn("data-capture-retry", self.source)
        self.assertIn("Не вдалося завантажити inbox", self.source)

    def test_mutating_buttons_are_disabled_during_requests(self):
        self.assertIn("prepareButton.disabled = true", self.source)
        self.assertIn("confirmButton.disabled = true", self.source)
        self.assertIn("dismissButton.disabled = true", self.source)


class CaptureAccountDeletionContractTests(SimpleTestCase):
    @patch("users.services._delete_rows")
    def test_capture_rows_are_deleted_in_dependency_order(self, delete_rows):
        delete_rows.side_effect = [3, 1]
        stats = HardDeleteStats()

        _delete_capture_rows(
            user_id=99101,
            stats=stats,
            existing_tables={"miniapp_capture_events", "miniapp_capture_connections"},
        )

        self.assertEqual(
            [item.kwargs["table_name"] for item in delete_rows.call_args_list],
            ["miniapp_capture_events", "miniapp_capture_connections"],
        )
        self.assertEqual(stats.deleted_capture_events, 3)
        self.assertEqual(stats.deleted_capture_connections, 1)


class PairingChallengeTests(SimpleTestCase):
    def setUp(self):
        cache.clear()

    @patch("miniapp.capture_service.CaptureConnection.objects.create")
    def test_invalid_pairing_proof_does_not_consume_challenge(self, create_connection):
        create_connection.return_value.status = "active"
        challenge, _ = issue_pairing_challenge(
            tg_user_id=99101,
            client_proof="correct-proof",
        )

        with self.assertRaises(CaptureValidationError) as error:
            approve_pairing(
                tg_user_id=99101,
                challenge=challenge,
                client_proof="wrong-proof",
                platform="android",
            )
        self.assertEqual(error.exception.code, "pairing_expired")

        connection, credential = approve_pairing(
            tg_user_id=99101,
            challenge=challenge,
            client_proof="correct-proof",
            platform="android",
        )

        self.assertEqual(connection.status, "active")
        self.assertTrue(credential)
