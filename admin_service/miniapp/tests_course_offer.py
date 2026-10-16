from __future__ import annotations

import time
import uuid
import json
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from django.contrib.sessions.middleware import SessionMiddleware
from django.test import RequestFactory, SimpleTestCase, TestCase, override_settings

from common.test_helpers import ensure_runtime_finance_tables, ensure_telegram_user_table
from miniapp import telegram_oidc
from miniapp.views import telegram_oidc_callback
from users.models import TelegramUser, UserAdminState


@override_settings(
    TELEGRAM_OIDC_CLIENT_ID="synthetic-client",
    TELEGRAM_OIDC_CLIENT_SECRET="synthetic-client-secret",
    TELEGRAM_OIDC_REDIRECT_URI="https://example.test/app/auth/telegram/callback",
    SECURE_SSL_REDIRECT=False,
)
class CourseWebOfferTests(TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_telegram_user_table()
        ensure_runtime_finance_tables()
        super().setUpClass()

    def setUp(self):
        from bot_settings.models import BotSetting

        BotSetting.objects.update_or_create(
            key="registration_enabled", defaults={"value": "true", "value_type": "bool"},
        )

    def request(self, path):
        request = RequestFactory().get(path)
        SessionMiddleware(lambda req: None).process_request(request)
        return request

    def test_course_survives_oidc_callback_without_starting_browser_cookie(self):
        start = self.request("/app/auth/telegram/start?start=course")
        authorization_url = telegram_oidc.begin_authorization(start)
        state = parse_qs(urlsplit(authorization_url).query)["state"][0]
        callback = self.request("/app/auth/telegram/callback")
        pending = telegram_oidc.consume_pending_authorization(callback, state=state, code="synthetic-code")
        self.assertEqual(getattr(pending, "start_payload", ""), "course")

    def complete_login(self, start_payload="course", *, user_id=19001):
        start = self.request(f"/app/auth/telegram/start?start={start_payload}")
        authorization_url = telegram_oidc.begin_authorization(start)
        state = parse_qs(urlsplit(authorization_url).query)["state"][0]
        callback = self.request(f"/app/auth/telegram/callback?state={state}&code=synthetic-code")
        identity = telegram_oidc.AuthenticatedTelegramIdentity(
            provider="telegram_oidc", subject=f"course-subject-{user_id}", tg_user_id=user_id,
            first_name="Course", last_name="Student", username="",
            claims={"iat": int(time.time()), "exp": int(time.time()) + 3600},
        )
        with (
            patch("miniapp.views.exchange_authorization_code", return_value=f"synthetic-provider-token-{uuid.uuid4()}"),
            patch("miniapp.views.verify_id_token", return_value=identity),
        ):
            response = telegram_oidc_callback(callback)
        self.assertEqual(response.status_code, 302)
        self.auth_session = callback.session
        return TelegramUser.objects.get(tg_user_id=user_id)

    def test_verified_course_login_persists_offer_without_granting_access(self):
        from miniapp.services import resolve_access

        user = self.complete_login()
        state = UserAdminState.objects.get(telegram_user=user)
        self.assertEqual(state.pending_start_payload, "course")
        access = resolve_access(user)
        self.assertEqual(access["promo_trial_days"], 90)
        self.assertEqual(access["mode"], "blocked")

    def test_course_consent_snapshots_the_server_selected_offer(self):
        from subscriptions.consent import accept_billing_consent

        user = self.complete_login()
        consent, created = accept_billing_consent(
            user=user, acquisition=None, idempotency_key=str(uuid.uuid4()),
            trial_days=90, copy_locale="uk",
        )
        self.assertTrue(created)
        self.assertEqual(consent.offer_id, "askills_course_90d")
        self.assertEqual(consent.trial_days, 90)

    def post_authenticated(self, view, path, payload):
        request = RequestFactory().post(path, data=json.dumps(payload), content_type="application/json")
        request.session = self.auth_session
        response = view(request)
        self.assertEqual(response.status_code, 200, response.content)
        return json.loads(response.content)

    def course_checkout(self, start_payload="course"):
        from miniapp.funnel_views import billing_consent
        from miniapp.views import billing_action
        from subscriptions.models import Payment

        user = self.complete_login(start_payload)
        consent = self.post_authenticated(billing_consent, "/app/api/funnel/billing-consent", {
            "accepted": True, "idempotency_key": str(uuid.uuid4()), "copy_locale": "uk",
        })
        with patch("subscriptions.billing.create_invoice", return_value={
            "invoiceId": "synthetic-course-invoice", "pageUrl": "https://example.test/checkout",
        }):
            self.post_authenticated(billing_action, "/app/api/settings/billing", {
                "action": "bind", "consent_id": consent["consent_id"],
            })
        return user, Payment.objects.get(provider_payment_id="synthetic-course-invoice"), consent

    def confirm_checkout(self, payment, status="success", *, with_card=True):
        from subscriptions.billing import process_monobank_event
        from django.utils import timezone

        payload = {
            "invoiceId": payment.provider_payment_id, "status": status,
            "modifiedDate": timezone.now().isoformat(),
        }
        if with_card:
            payload["walletData"] = {"cardToken": "synthetic-card-token", "maskedPan": "444455******1111"}
        with (
            patch("subscriptions.billing.send_bind_activation_notification", return_value=False),
            patch("subscriptions.billing._send_user_message", return_value=False),
            patch("subscriptions.billing._emit_pwa_event", return_value=0),
        ):
            return process_monobank_event(payload)

    @override_settings(TELEGRAM_BOT_TOKEN="", MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900)
    def test_course_bonus_requires_confirmed_card_and_real_ninety_day_trial(self):
        from miniapp.services import resolve_access
        from subscriptions.models import Subscription, BillingConsent

        user, payment, consent = self.course_checkout()
        self.assertIsNone(resolve_access(user).get("course_bonus"))
        self.assertFalse(Subscription.objects.filter(user=user).exists())
        self.assertEqual(payment.raw_payload["bind_context"]["trial_days"], 90)
        self.confirm_checkout(payment, status="processing", with_card=False)
        self.assertIsNone(resolve_access(user).get("course_bonus"))
        self.confirm_checkout(payment)
        subscription = Subscription.objects.get(user=user)
        self.assertEqual(subscription.expires_at - subscription.started_at, timedelta(days=90))
        bonus = resolve_access(user).get("course_bonus")
        self.assertEqual(bonus, {
            "id": consent["consent_id"], "trial_days": 90,
            "course_name": "Фінансова грамотність", "platform": "askills.pro",
        })
        self.assertIsNotNone(BillingConsent.objects.get(pk=consent["consent_id"]).first_renewal_at)
        self.confirm_checkout(payment)
        subscription.refresh_from_db()
        self.assertEqual(subscription.expires_at - subscription.started_at, timedelta(days=90))
        self.assertEqual(resolve_access(user)["course_bonus"], bonus)

    @override_settings(TELEGRAM_BOT_TOKEN="", MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900)
    def test_ordinary_registration_keeps_thirty_days_and_no_course_gift(self):
        from miniapp.services import resolve_access
        from subscriptions.models import Subscription, BillingConsent

        user, payment, consent = self.course_checkout(start_payload="")
        self.assertEqual(payment.raw_payload["bind_context"]["trial_days"], 30)
        self.assertNotEqual(BillingConsent.objects.get(pk=consent["consent_id"]).offer_id, "askills_course_90d")
        self.confirm_checkout(payment)
        subscription = Subscription.objects.get(user=user)
        self.assertEqual(subscription.expires_at - subscription.started_at, timedelta(days=30))
        self.assertIsNone(resolve_access(user).get("course_bonus"))

    @override_settings(TELEGRAM_BOT_TOKEN="", MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900)
    def test_failed_course_checkout_cannot_show_the_gift(self):
        from miniapp.services import resolve_access
        from subscriptions.models import Subscription

        user, payment, _ = self.course_checkout()
        self.confirm_checkout(payment, status="failure", with_card=False)
        self.assertFalse(Subscription.objects.filter(user=user).exists())
        self.assertIsNone(resolve_access(user).get("course_bonus"))

    @override_settings(TELEGRAM_BOT_TOKEN="", MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900)
    def test_success_without_saved_card_cannot_show_the_gift(self):
        from miniapp.services import resolve_access

        user, payment, _ = self.course_checkout()
        self.confirm_checkout(payment, with_card=False)
        self.assertIsNone(resolve_access(user).get("course_bonus"))

    @override_settings(TELEGRAM_BOT_TOKEN="", MONO_BIND_AMOUNT=100, MONO_RENEWAL_AMOUNT=49900)
    def test_renewed_subscription_does_not_repeat_course_gift(self):
        from miniapp.services import resolve_access
        from subscriptions.models import Subscription

        user, payment, _ = self.course_checkout()
        self.confirm_checkout(payment)
        Subscription.objects.filter(user=user).update(status="active")
        self.assertIsNone(resolve_access(user).get("course_bonus"))

    def test_non_course_parameter_is_not_an_offer(self):
        from miniapp.services import resolve_access

        user = self.complete_login("90")
        self.assertEqual(UserAdminState.objects.get(telegram_user=user).pending_start_payload, "")
        self.assertEqual(resolve_access(user)["promo_trial_days"], 0)
        self.assertEqual(resolve_access(user)["trial_days"], 30)

    def test_callback_does_not_overwrite_existing_campaign_or_extend_access(self):
        from miniapp.services import resolve_access

        user = self.complete_login()
        state = UserAdminState.objects.get(telegram_user=user)
        state.pending_start_payload = "promo_existing_campaign"
        state.save(update_fields=["pending_start_payload"])
        self.complete_login()
        state.refresh_from_db()
        self.assertEqual(state.pending_start_payload, "promo_existing_campaign")
        self.assertEqual(resolve_access(user)["mode"], "blocked")


class CourseBonusShellTests(SimpleTestCase):
    def test_course_bonus_script_is_versioned_and_precached(self):
        from miniapp.views import index, service_worker, MINIAPP_ASSET_VERSION

        request = RequestFactory().get("/app/")
        script_url = f"/static/miniapp/course-bonus.js?v={MINIAPP_ASSET_VERSION}"
        self.assertContains(index(request), script_url)
        self.assertContains(service_worker(request), script_url)
