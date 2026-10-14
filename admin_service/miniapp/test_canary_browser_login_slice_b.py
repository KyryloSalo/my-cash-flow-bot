from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase, override_settings
from django.http import HttpResponse

from miniapp import auth, views
from users.admin import TelegramUserAdmin
from users.models import TelegramUser


class ProductionCanaryBrowserLoginSliceBTests(SimpleTestCase):
    @patch("miniapp.auth.UserAuthSession.objects.filter")
    @patch("miniapp.auth.TelegramUser.objects.filter")
    def test_revoked_canary_session_is_rejected(self, users_filter, sessions_filter):
        users_filter.return_value.first.return_value = SimpleNamespace(tg_user_id=1001)
        sessions_filter.return_value.exists.return_value = False
        request = SimpleNamespace(
            session={
                auth.SESSION_USER_ID_KEY: 1001,
                auth.SESSION_AUTH_MODE_KEY: auth.PRODUCTION_CANARY_AUTH_MODE,
                auth.SESSION_RECORD_TOKEN_KEY: "revoked-session-token",
            }
        )

        with self.assertRaises(auth.MiniAppSessionError):
            auth.get_session_user(request)

    @override_settings(MINIAPP_BROWSER_LOGIN_SECRET="secret", TELEGRAM_BOT_TOKEN="bot")
    def test_canary_token_has_distinct_purpose_and_ordinary_token_cannot_be_canary(self):
        ordinary = auth.build_browser_login_token(1001, now=1_700_000_000, nonce="o" * 18)
        canary = auth.build_browser_login_token(
            1001, now=1_700_000_000, nonce="c" * 18,
            purpose="production_canary_browser_login",
        )
        self.assertEqual(auth.validate_browser_login_token(canary, now=1_700_000_001).purpose, "production_canary_browser_login")
        with self.assertRaises(auth.MiniAppAuthError):
            auth.consume_browser_login_token_with_identity(ordinary, expected_purpose="production_canary_browser_login")

    @override_settings(MINIAPP_BROWSER_LOGIN_SECRET="", TELEGRAM_BOT_TOKEN="bot")
    def test_browser_login_secret_never_falls_back_to_bot_token(self):
        with self.assertRaises(auth.MiniAppAuthError):
            auth.build_browser_login_token(1001, now=1_700_000_000, nonce="x" * 18)

    @override_settings(MINIAPP_BROWSER_LOGIN_SECRET="secret")
    @patch("miniapp.views.login_session")
    @patch("miniapp.views.consume_browser_login_token_with_identity")
    @patch("miniapp.views.validate_browser_login_token")
    @patch("miniapp.views.is_canary_user", return_value=True)
    def test_legacy_path_rejects_canary_token_before_consumption(self, _is_canary, validate, consume, login_session):
        identity = SimpleNamespace(purpose="production_canary_browser_login", issued_at=1_700_000_000)
        validate.return_value = identity
        consume.side_effect = auth.MiniAppAuthError("Browser login purpose invalid.")
        request = RequestFactory().get("/app/api/browser-login/raw-token/")
        request.session = SimpleNamespace()
        response = views.browser_login(request, "raw-token")
        self.assertEqual(response.status_code, 400)
        consume.assert_called_once_with("raw-token", expected_purpose="ordinary_browser_login")
        login_session.assert_not_called()
        self.assertIn("no-store", response["Cache-Control"])
        self.assertEqual(response["Referrer-Policy"], "no-referrer")

    @patch("miniapp.views._complete_browser_login")
    def test_fragment_bootstrap_posts_token_to_internal_exchange(self, complete):
        complete.return_value = HttpResponse(status=302)
        request = RequestFactory().post(
            "/app/api/canary/browser-login",
            data='{"token":"raw-token"}',
            content_type="application/json",
        )

        response = views.canary_browser_login_bootstrap(request)

        self.assertEqual(response.status_code, 302)
        complete.assert_called_once_with(
            request,
            "raw-token",
            expected_purpose="production_canary_browser_login",
        )

    @patch("users.admin.issue_production_canary_browser_login", return_value="one-time-token")
    @patch("users.admin.get_object_or_404")
    def test_admin_issues_link_only_after_confirmed_post_with_no_store(self, get_object, issue):
        user = SimpleNamespace(pk=7, tg_user_id=1001, full_name="Canary User")
        get_object.return_value = user
        request = RequestFactory().post(
            "/admin/users/telegramuser/7/production-canary-login/",
            data={
                "confirmation": "CONFIRM PRODUCTION CANARY",
                "comment": "confirmed",
            },
        )
        request.user = SimpleNamespace(
            is_authenticated=True,
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        model_admin = TelegramUserAdmin(TelegramUser, SimpleNamespace(each_context=lambda _request: {}))

        response = model_admin.production_canary_login_view(request, "7")

        issue.assert_called_once_with(
            user=user,
            admin_user=request.user,
            reason="confirmed",
            request=request,
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("one-time-token", response.context_data["login_url"])
        self.assertIn("no-store", response["Cache-Control"])
        self.assertEqual(response["Referrer-Policy"], "no-referrer")
