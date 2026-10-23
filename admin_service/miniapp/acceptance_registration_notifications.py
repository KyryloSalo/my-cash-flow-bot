"""Explicit browser acceptance; Telegram identity and delivery are synthetic seams."""
from __future__ import annotations

import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlsplit

from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from django.test import override_settings

from admin_notifications.models import AdminNotificationLog
from admin_notifications.tasks import send_admin_notification_task
from miniapp.models import DurableRegistrationAttribution, PartnerLink
from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
from miniapp.tests_oidc import build_telegram_init_data


@contextmanager
def synthetic_telegram_provider(callback_origin):
    """Use a real loopback redirect: browser routing does not intercept redirect hops."""
    class ProviderHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            state = parse_qs(urlsplit(self.path).query)["state"][0]
            callback = callback_origin + "/app/auth/telegram/callback?" + urlencode({
                "state": state, "code": "synthetic-browser-code",
            })
            self.send_response(302)
            self.send_header("Location", callback)
            self.end_headers()

        def log_message(self, *_args):
            pass  # Do not log authorization query strings.

    server = ThreadingHTTPServer(("127.0.0.1", 0), ProviderHandler)
    worker = Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        worker.join()
        server.server_close()


@override_settings(
    DEBUG=True, ALLOWED_HOSTS=["localhost", "127.0.0.1", "testserver"],
    SECURE_SSL_REDIRECT=False, SESSION_COOKIE_SECURE=False, CSRF_COOKIE_SECURE=False,
    DOMAIN="vydno.example", MINIAPP_DEV_TG_USER_ID=None,
    TELEGRAM_BOT_TOKEN="browser-registration-fixture",
    ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[9150099],
    TELEGRAM_OIDC_CLIENT_ID="123456789",
    TELEGRAM_OIDC_CLIENT_SECRET="browser-registration-fixture",
    TELEGRAM_OIDC_REDIRECT_URI="https://vydno.example/app/auth/telegram/callback",
)
class RegistrationNotificationBrowserAcceptance(StaticLiveServerTestCase):
    def setUp(self):
        self.partner = PartnerLink.objects.create(
            name="Browser finance partner", code="browser-finance", utm_source="creator",
            destination=PartnerLink.Destination.APP, trial_days=51,
        )
        self.delivery = self.enterContext(patch("admin_notifications.tasks.send_telegram_message"))
        # Exercise the real task and delivery log, without a broker or external Telegram message.
        self.enterContext(patch.object(send_admin_notification_task, "delay", side_effect=send_admin_notification_task.run))

    def restrict_network(self, route):
        origins = [self.live_server_url + "/"]
        if getattr(self, "provider_origin", None):
            origins.append(self.provider_origin + "/")
        if route.request.url.startswith(tuple(origins)):
            route.continue_()
        else:
            route.abort()

    def assert_delivery(self, *, user_id, partner):
        logs = AdminNotificationLog.objects.filter(event_type="new_user_registered")
        self.assertEqual(logs.count(), 1)
        log = logs.get()
        self.assertEqual(log.telegram_chat_id, 9150099)
        self.assertEqual(log.status, AdminNotificationLog.Status.SENT)
        self.assertEqual(log.payload["tg_user_id"], user_id)
        self.assertEqual(self.delivery.call_count, 1)
        message = self.delivery.call_args.kwargs["text"]
        self.assertIn("Новий користувач", message)
        self.assertIsNone(self.delivery.call_args.kwargs["parse_mode"])
        if partner:
            self.assertIn("Browser finance partner", message)
            self.assertIn("browser-finance", message)
            self.assertIn("https://vydno.example/app/r/browser-finance", message)
            attribution = DurableRegistrationAttribution.objects.get(user_id=user_id)
            self.assertEqual(attribution.partner_link_id, self.partner.pk)
        else:
            self.assertIn("прямий вхід", message)
            self.assertNotIn("Browser finance partner", message)
        return message

    def test_partner_oidc_browser_registration_and_repeat_login(self):
        from playwright.sync_api import sync_playwright

        now = int(time.time())
        identity = AuthenticatedTelegramIdentity(
            provider="telegram_oidc", subject="browser-fixture-9150011", tg_user_id=9150011,
            first_name="Browser user", last_name="", username="browser_fixture",
            claims={"id": 9150011, "sub": "browser-fixture-9150011", "iat": now, "exp": now + 3600},
        )
        with (
            synthetic_telegram_provider(self.live_server_url) as provider_origin,
            override_settings(TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT=provider_origin + "/authorize"),
            patch("miniapp.views.exchange_authorization_code", return_value="synthetic.browser.registration"),
            patch("miniapp.views.verify_id_token", return_value=identity),
            sync_playwright() as pw,
        ):
            self.provider_origin = provider_origin
            browser = pw.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.route("**/*", self.restrict_network)

                page.goto(self.live_server_url + "/app/r/browser-finance", wait_until="networkidle")
                page.locator("#registrationPartnerOffer").wait_for(state="visible")
                self.assertIn("51", page.locator("#registrationPartnerOffer").inner_text())
                self.assertEqual(self.delivery.call_count, 0)
                page.locator("#telegramOidcRecovery").click()
                page.wait_for_url(lambda url: urlsplit(url).path == "/app/")
                page.locator("#onboardingShell").wait_for(state="visible", timeout=15000)
                profile_url = page.locator("body").get_attribute("data-profile-url")
                profile = page.request.get(self.live_server_url + profile_url)
                self.assertTrue(profile.ok)
                self.assertEqual(profile.json()["user"]["tg_user_id"], 9150011)
                self.assertEqual(self.delivery.call_count, 1)
                page.reload(wait_until="networkidle")
                page.locator("#onboardingShell").wait_for(state="visible")
                self.assertEqual(self.delivery.call_count, 1)
            finally:
                browser.close()
        self.assertIn("telegram_oidc", self.assert_delivery(user_id=9150011, partner=True))
        print("PARTNER_OIDC_BROWSER_PASS: visible partner offer -> login button -> callback -> onboarding; one attributed admin delivery (synthetic Telegram)")

    def test_direct_miniapp_browser_registration_and_repeat_authentication(self):
        from playwright.sync_api import sync_playwright

        init_data = build_telegram_init_data(bot_token="browser-registration-fixture", tg_user_id=9150012)
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                page.route("**/*", self.restrict_network)
                page.goto(self.live_server_url + "/app/", wait_until="networkidle")
                auth_url = page.locator("body").get_attribute("data-auth-url")
                for _ in range(2):
                    status = page.evaluate("""async ({url, data}) => {
                        const response = await fetch(url, {method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({init_data: data})});
                        return response.status;
                    }""", {"url": auth_url, "data": init_data})
                    self.assertEqual(status, 200)
                page.reload(wait_until="networkidle")
                page.locator("#onboardingShell").wait_for(state="visible", timeout=15000)
            finally:
                browser.close()
        self.assertIn("telegram_miniapp", self.assert_delivery(user_id=9150012, partner=False))
        print("DIRECT_MINIAPP_BROWSER_PASS: signed synthetic identity -> onboarding; repeat authentication creates no duplicate admin delivery")
