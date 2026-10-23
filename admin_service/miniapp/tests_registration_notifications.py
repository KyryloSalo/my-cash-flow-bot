from __future__ import annotations

import hashlib
import hmac
import json
import time
from unittest.mock import patch
from urllib.parse import urlencode

from django.test import TestCase, override_settings

from miniapp.models import PartnerLink


@override_settings(
    TELEGRAM_BOT_TOKEN="registration-notification-fixture", DOMAIN="vydno.example",
    ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[9150099],
)
class RegistrationNotificationTests(TestCase):
    def setUp(self):
        self.partner = PartnerLink.objects.create(
            name="Finance partner", code="finance-partner", utm_source="creator",
            destination=PartnerLink.Destination.APP,
        )
        self.send = self.enterContext(patch("admin_notifications.tasks.send_admin_notification_task.delay"))

    def register(self, user_id=9150001):
        payload = {
            "auth_date": str(int(time.time())),
            "user": json.dumps({"id": user_id, "first_name": "Partner user", "username": "partner_user"}),
        }
        secret = hmac.new(b"WebAppData", b"registration-notification-fixture", hashlib.sha256).digest()
        check = "\n".join(f"{key}={value}" for key, value in sorted(payload.items()))
        payload["hash"] = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        return self.client.post(
            "/app/api/auth/telegram", {"init_data": urlencode(payload)}, content_type="application/json",
        )

    def test_partner_registration_notifies_admin_with_original_link(self):
        self.assertEqual(self.client.get("/app/r/finance-partner").status_code, 302)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.register().status_code, 200)
            self.send.assert_not_called()
        self.send.assert_called_once()
        event_type, message, payload = self.send.call_args.args
        self.assertEqual(event_type, "new_user_registered")
        for text in (
            "Новий користувач", "9150001", "@partner_user", "Partner user",
            "Finance partner", "finance-partner", "https://vydno.example/app/r/finance-partner",
        ):
            self.assertIn(text, message)
        self.assertEqual(payload["tg_user_id"], 9150001)
        self.assertEqual(payload["partner_link_code"], "finance-partner")

    def test_direct_registration_notifies_without_partner(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.register().status_code, 200)
        self.send.assert_called_once()
        message = self.send.call_args.args[1]
        self.assertIn("telegram_miniapp", message)
        self.assertIn("прямий вхід", message)
        self.assertNotIn("finance-partner", message)

    def test_repeat_authentication_and_other_provider_do_not_notify_again(self):
        from users.models import UserAuthIdentity
        from users.registration import RegistrationTelegramIdentity, bootstrap_telegram_identity

        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.register().status_code, 200)
            self.assertEqual(self.register().status_code, 200)
            registration = bootstrap_telegram_identity(
                RegistrationTelegramIdentity(
                    provider=UserAuthIdentity.Provider.TELEGRAM_OIDC,
                    subject="fixture-9150001", tg_user_id=9150001,
                ),
                source="browser_oidc",
            )
            self.assertFalse(registration.created)
        self.send.assert_called_once()

    def test_failed_partner_reservation_does_not_send_phantom_registration(self):
        self.client.get("/app/r/finance-partner")
        with self.captureOnCommitCallbacks(execute=True), patch(
            "miniapp.views.claim_partner_offer", side_effect=RuntimeError("test reservation failure"),
        ):
            self.assertEqual(self.register().status_code, 503)
        self.send.assert_not_called()

    def test_first_link_wins_over_later_partner_click(self):
        PartnerLink.objects.create(
            name="Later partner", code="later-partner", utm_source="other",
            destination=PartnerLink.Destination.APP,
        )
        self.client.get("/app/r/finance-partner")
        self.client.get("/app/r/later-partner")
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.register().status_code, 200)
        message = self.send.call_args.args[1]
        self.assertIn("Finance partner", message)
        self.assertNotIn("Later partner", message)

    def test_telemetry_failure_does_not_suppress_registration_notification(self):
        self.client.get("/app/r/finance-partner")
        with self.captureOnCommitCallbacks(execute=True), patch(
            "miniapp.views.queue_registration_event", side_effect=RuntimeError("test telemetry failure"),
        ):
            self.assertEqual(self.register().status_code, 200)
        self.send.assert_called_once()

    def test_broker_failure_does_not_break_successful_registration(self):
        self.send.side_effect = RuntimeError("test broker failure")
        with self.assertLogs("django.test", level="ERROR"):
            with self.captureOnCommitCallbacks(execute=True):
                self.assertEqual(self.register().status_code, 200)

    @override_settings(ADMIN_NOTIFICATIONS_ENABLED=False)
    def test_disabled_notifications_do_not_enqueue_tasks(self):
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.register().status_code, 200)
        self.send.assert_not_called()
