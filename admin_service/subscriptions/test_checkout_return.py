import time
from decimal import Decimal
from unittest.mock import patch

from django.conf import settings
from django.core import signing
from django.test import Client, RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from miniapp.auth import (
    BROWSER_AUTH_MODE,
    build_browser_login_token,
    login_session,
)
from miniapp.models import BrowserLoginTokenUse
from subscriptions.billing import RETURN_TOKEN_SALT, build_bind_return_token, parse_bind_return_token
from subscriptions.models import Payment
from users.models import TelegramUser


@override_settings(
    MINIAPP_BROWSER_LOGIN_SECRET="synthetic-checkout-return-secret",
    MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS=900,
    TELEGRAM_BOT_TOKEN="",
)
class CheckoutReturnTests(TestCase):
    def setUp(self):
        self.user = TelegramUser.objects.create(
            tg_user_id=904004, first_name="Checkout", lang="uk", base_currency="UAH",
            onboarding_completed=True, onboarding_version=2,
        )

    def checkout(self, *, flow="bind", status=Payment.Status.PAID, lang="uk"):
        token = build_bind_return_token(user_id=self.user.pk, flow=flow, lang=lang)
        context = "recovery_context" if flow == "recovery" else "bind_context"
        payment = Payment.objects.create(
            user=self.user, provider="monobank", provider_payment_id=f"checkout-{Payment.objects.count()}",
            amount=Decimal("1.00"), currency="UAH", status=status,
            kind=Payment.Kind.RETRY if flow == "recovery" else Payment.Kind.BIND,
            paid_at=timezone.now() if status == Payment.Status.PAID else None,
            raw_payload={context: {"return_token": token, "trial_granted": False}},
        )
        return token, payment

    def sign_in(self, user):
        request = RequestFactory().get("/app/")
        request.session = self.client.session
        login_session(request, user, auth_mode=BROWSER_AUTH_MODE)
        request.session.save()
        self.client.cookies[settings.SESSION_COOKIE_NAME] = request.session.session_key

    @patch("subscriptions.billing.send_telegram_message")
    def test_paid_checkout_offers_app_instead_of_telegram(self, send):
        token, _ = self.checkout()
        response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
        self.assertContains(response, "Відкрити застосунок")
        self.assertNotContains(response, "Відкрити бота")
        self.assertNotContains(response, "t.me/")
        send.assert_not_called()

    def test_paid_checkout_button_authenticates_anonymous_browser(self):
        token, _ = self.checkout()
        response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
        self.assertTrue(bool(response.context.get("app_return_token")), "Paid checkout must offer authenticated app handoff")
        self.assertIn(b'<meta name="referrer" content="strict-origin">', response.content.split(b"</head>", 1)[0])
        response = self.client.post("/billing/mono/open-app", {"return_token": token})
        self.assertRedirects(response, "/app/", fetch_redirect_response=False)
        self.assertEqual(self.client.session.get("miniapp_tg_user_id"), self.user.pk)

    @patch("subscriptions.views.sync_pending_bind_status")
    def test_old_paid_return_does_not_poll_a_different_checkout(self, sync):
        token, _ = self.checkout()
        self.checkout(status=Payment.Status.PENDING)
        response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
        self.assertContains(response, "Доступ активовано")
        sync.assert_not_called()

    def test_get_does_not_sign_in_or_consume_capability(self):
        token, _ = self.checkout()
        for _ in range(2):
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
            self.assertTrue(response.context["app_return_token"] == token)
            self.assertIn("no-store", response["Cache-Control"])
        self.assertFalse(BrowserLoginTokenUse.objects.exists())
        self.assertNotIn("miniapp_tg_user_id", self.client.session)
        self.assertEqual(self.client.get(reverse("billing-mono-open-app")).status_code, 405)

    def test_capability_is_single_use_but_same_session_can_continue(self):
        token, _ = self.checkout()
        for _ in range(2):
            self.assertEqual(self.client.post(reverse("billing-mono-open-app"), {"return_token": token}).status_code, 302)
        self.assertEqual(BrowserLoginTokenUse.objects.count(), 1)
        other_browser = Client()
        response = other_browser.post(reverse("billing-mono-open-app"), {"return_token": token})
        self.assertEqual(response.status_code, 403)
        self.assertNotIn("miniapp_tg_user_id", other_browser.session)

    def test_existing_same_account_session_is_preserved(self):
        token, _ = self.checkout()
        self.sign_in(self.user)
        key = self.client.session.session_key
        auth_at = self.client.session["miniapp_auth_at"]
        self.assertEqual(self.client.post(reverse("billing-mono-open-app"), {"return_token": token}).status_code, 302)
        self.assertEqual(self.client.session.session_key, key)
        self.assertEqual(self.client.session["miniapp_auth_at"], auth_at)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_other_signed_in_account_is_not_replaced(self):
        token, _ = self.checkout()
        other = TelegramUser.objects.create(tg_user_id=904005, first_name="Other")
        self.sign_in(other)
        response = self.client.post(reverse("billing-mono-open-app"), {"return_token": token})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.client.session["miniapp_tg_user_id"], other.pk)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_unpaid_and_refunded_invoices_cannot_authorize(self):
        for status in (Payment.Status.PENDING, Payment.Status.FAILED, Payment.Status.REJECTED, Payment.Status.REFUNDED):
            with self.subTest(status=status):
                token, _ = self.checkout(status=status)
                response = self.client.post(reverse("billing-mono-open-app"), {"return_token": token})
                self.assertEqual(response.status_code, 403)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_paid_latest_invoice_does_not_authorize_unpaid_return(self):
        token, _ = self.checkout(status=Payment.Status.FAILED)
        self.checkout()
        response = self.client.post(reverse("billing-mono-open-app"), {"return_token": token})
        self.assertEqual(response.status_code, 403)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_expired_capability_is_not_refreshed_by_return_get(self):
        token, _ = self.checkout()
        with patch("miniapp.auth.time.time", return_value=time.time() + 901):
            response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
            self.assertContains(response, 'href="/app/"')
            self.assertFalse(response.context["app_return_token"])
            self.assertEqual(self.client.post(reverse("billing-mono-open-app"), {"return_token": token}).status_code, 403)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_legacy_status_token_is_not_an_authentication_capability(self):
        payload = {"telegram_user_id": self.user.pk, "flow": "bind", "lang": "uk"}
        token, payment = self.checkout()
        legacy = signing.dumps(payload, salt=RETURN_TOKEN_SALT, compress=True)
        payment.raw_payload["bind_context"]["return_token"] = legacy
        payment.save(update_fields=["raw_payload"])
        response = self.client.get(reverse("billing-mono-return"), {"return_token": legacy})
        self.assertContains(response, 'href="/app/"')
        self.assertEqual(self.client.post(reverse("billing-mono-open-app"), {"return_token": legacy}).status_code, 403)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_checkout_token_cannot_use_ordinary_browser_login_before_payment(self):
        token, _ = self.checkout(status=Payment.Status.PENDING)
        login_token = parse_bind_return_token(token)["app_login_token"]
        response = self.client.get(reverse("miniapp:browser-login-api", args=[login_token]))
        self.assertEqual(response.status_code, 400)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_foreign_identity_or_purpose_does_not_consume_token(self):
        for target, purpose in ((904005, "checkout_return"), (self.user.pk, "ordinary_browser_login")):
            token, payment = self.checkout()
            payload = parse_bind_return_token(token)
            payload["app_login_token"] = build_browser_login_token(target, purpose=purpose)
            modified = signing.dumps(payload, salt=RETURN_TOKEN_SALT, compress=True)
            payment.raw_payload["bind_context"]["return_token"] = modified
            payment.save(update_fields=["raw_payload"])
            self.assertEqual(self.client.post(reverse("billing-mono-open-app"), {"return_token": modified}).status_code, 403)
        self.assertFalse(BrowserLoginTokenUse.objects.exists())

    def test_post_requires_csrf(self):
        token, _ = self.checkout()
        browser = Client(enforce_csrf_checks=True)
        self.assertEqual(browser.post(reverse("billing-mono-open-app"), {"return_token": token}).status_code, 403)
        browser.get(reverse("billing-mono-return"), {"return_token": token})
        response = browser.post(reverse("billing-mono-open-app"), {
            "return_token": token, "csrfmiddlewaretoken": browser.cookies["csrftoken"].value,
        })
        self.assertEqual(response.status_code, 302)

    def test_recovery_and_english_return_open_authenticated_app(self):
        token, _ = self.checkout(flow="recovery", lang="en")
        response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
        self.assertContains(response, "Open app")
        self.assertContains(response, "Access restored")
        self.assertNotContains(response, "Open bot")
        response = self.client.post(reverse("billing-mono-open-app"), {"return_token": token})
        self.assertRedirects(response, "/app/", fetch_redirect_response=False)
        self.assertEqual(self.client.session["miniapp_auth_mode"], BROWSER_AUTH_MODE)

    @override_settings(MINIAPP_BROWSER_LOGIN_SECRET="")
    def test_missing_login_secret_preserves_checkout_and_existing_session(self):
        token, _ = self.checkout()
        self.assertNotIn("app_login_token", parse_bind_return_token(token))
        self.sign_in(self.user)
        response = self.client.get(reverse("billing-mono-return"), {"return_token": token})
        self.assertContains(response, 'href="/app/"')
        self.assertEqual(self.client.session["miniapp_tg_user_id"], self.user.pk)
