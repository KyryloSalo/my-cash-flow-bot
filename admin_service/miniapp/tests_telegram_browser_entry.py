from __future__ import annotations

from html import unescape
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings
from miniapp import views


@override_settings(
    ALLOWED_HOSTS=["testserver"],
    SECURE_SSL_REDIRECT=False,
    MINIAPP_BROWSER_LOGIN_SECRET="",
    PRODUCTION_CANARY_ENABLED=False,
)
class TelegramBrowserEntryTests(SimpleTestCase):
    def test_public_login_is_available_without_a_signing_secret_or_identity(self):
        with patch("miniapp.views.build_browser_login_token", side_effect=AssertionError("No magic links")):
            response = self.client.get("/app/login/", secure=True)
        self.assertEqual(response.status_code, 200)
        body = unescape(response.content.decode())
        self.assertIn('href="/app/auth/telegram/start"', body)
        self.assertIn("Увійти через Telegram", body)
        self.assertNotIn("/app/api/browser-login/", body)
        self.assertNotIn("miniapp_tg_user_id", response.wsgi_request.session)
        self.assertIn("no-store", response["Cache-Control"])

    def test_mode_flags_use_fixed_post_login_targets_and_ignore_untrusted_identity(self):
        for query, target in (
            ("install=1", "/app/?install=1"),
            ("operator=1", "/app/operator/"),
            ("operator=1&install=1", "/app/operator/?install=1"),
        ):
            with self.subTest(query=query):
                response = self.client.get(f"/app/login/?{query}&tg_user_id=999&next=https://example.com", secure=True)
                self.assertEqual(response.status_code, 200)
                body = unescape(response.content.decode())
                import re
                match = re.search(r'class="browser-login-action" href="([^"]+)"', body)
                self.assertIsNotNone(match)
                link = urlsplit(match.group(1))
                self.assertEqual(link.path, "/app/auth/telegram/start")
                self.assertEqual(parse_qs(link.query), {"next": [target]})
                self.assertNotIn("example.com", body)
                self.assertNotIn("tg_user_id", body)

    def test_android_telegram_opens_oidc_in_chrome_before_creating_authorization_state(self):
        response = self.client.get(
            "/app/login/?install=1",
            secure=True,
            HTTP_USER_AGENT="Mozilla/5.0 (Linux; Android 14) Telegram/12.0",
        )
        self.assertEqual(response.status_code, 302)
        location = response["Location"]
        self.assertTrue(location.startswith("intent://testserver/app/auth/telegram/start?"))
        self.assertIn("package=com.android.chrome", location)
        self.assertIn("scheme=https", location)
        self.assertNotIn("browser-login/", location)
        self.assertEqual(dict(response.wsgi_request.session), {})

    def test_ios_embedded_browser_keeps_safari_handoff(self):
        response = self.client.get("/app/login/", secure=True, HTTP_USER_AGENT="Mozilla/5.0 (iPhone) Telegram/12.0")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Safari")
        self.assertContains(response, "Натисни компас")

    def test_login_page_does_not_accept_post(self):
        self.assertEqual(self.client.post("/app/login/", secure=True).status_code, 405)

    def test_install_nudge_browser_url_does_not_sign_an_identity_link(self):
        request = self.client.request().wsgi_request
        with patch("miniapp.views.build_browser_login_token", side_effect=AssertionError("No magic links")):
            self.assertEqual(
                views._install_nudge_browser_url(request, SimpleNamespace(tg_user_id=900001)),
                "http://testserver/app/login/?install=1",
            )
