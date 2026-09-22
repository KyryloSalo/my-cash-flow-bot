from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from django.template.loader import get_template
from django.test import RequestFactory, SimpleTestCase, override_settings
from django.urls import resolve

from miniapp import auth, views
from miniapp.funnel import (
    ACQUISITION_SESSION_KEY,
    FunnelValidationError,
    PENDING_REGISTRATION_EVENT_KEY,
    _request_device_dimensions,
    normalize_attribution,
    retry_pending_registration_event,
    validate_client_event_payload,
)
from miniapp import funnel_views
from miniapp.models import AcquisitionSession, FunnelEvent
from subscriptions.consent import build_server_consent_terms
from subscriptions.models import BillingConsent


class Session(dict):
    modified = False

    def cycle_key(self) -> None:
        return None

    def set_expiry(self, value) -> None:
        self["_expiry"] = value


class FunnelPayloadValidationTests(SimpleTestCase):
    def test_known_browser_automation_is_classified_for_default_exclusion(self) -> None:
        request = RequestFactory().get(
            "/app/api/funnel/session",
            HTTP_USER_AGENT="Mozilla/5.0 Playwright automated acceptance",
        )

        dimensions = _request_device_dimensions(request)

        self.assertEqual(dimensions["automation_status"], "automated")

    def test_social_link_preview_is_classified_for_default_exclusion(self) -> None:
        request = RequestFactory().get(
            "/app/r/creator",
            HTTP_USER_AGENT="TelegramBot (like TwitterBot)",
        )

        dimensions = _request_device_dimensions(request)

        self.assertEqual(dimensions["automation_status"], "automated")

    def test_client_cannot_emit_trusted_payment_success(self) -> None:
        with self.assertRaises(FunnelValidationError) as caught:
            validate_client_event_payload(
                {
                    "event_name": "payment_success",
                    "event_id": "8d09ed8c-bccb-4c7f-9c82-c4385853540d",
                }
            )

        self.assertEqual(caught.exception.code, "server_event_required")

    def test_attribution_omits_urls_and_unbounded_values(self) -> None:
        normalized = normalize_attribution(
            {
                "utm_source": "google_ads",
                "utm_campaign": "https://tracker.example/path?secret=value",
                "landing_variant": "hero-v2",
                "referrer": "https://private.example/account",
                "utm_content": "x" * 129,
            }
        )

        self.assertEqual(
            normalized,
            {
                "utm_source": "google_ads",
                "landing_variant": "hero-v2",
            },
        )


class FunnelSchemaContractTests(SimpleTestCase):
    def test_funnel_schema_has_no_raw_request_or_financial_fields(self) -> None:
        acquisition_fields = {field.name for field in AcquisitionSession._meta.fields}
        event_fields = {field.name for field in FunnelEvent._meta.fields}

        self.assertEqual(AcquisitionSession._meta.db_table, "acquisition_sessions")
        self.assertEqual(FunnelEvent._meta.db_table, "funnel_events")
        self.assertFalse({"ip", "ip_address", "user_agent"} & acquisition_fields)
        self.assertFalse({"properties", "amount", "balance", "comment", "raw_payload"} & event_fields)
        self.assertTrue(
            any(
                tuple(constraint.fields) == ("acquisition_session", "idempotency_key")
                for constraint in FunnelEvent._meta.constraints
            )
        )

    def test_billing_consent_is_a_separate_user_scoped_record(self) -> None:
        fields = {field.name for field in BillingConsent._meta.fields}

        self.assertEqual(BillingConsent._meta.db_table, "billing_consents")
        self.assertTrue(
            {
                "user",
                "acquisition_session",
                "payment",
                "idempotency_key",
                "offer_version",
                "terms_version",
                "privacy_version",
                "payload_hash",
                "accepted_at",
            }.issubset(fields)
        )
        self.assertTrue(
            any(
                tuple(constraint.fields) == ("user", "idempotency_key")
                for constraint in BillingConsent._meta.constraints
            )
        )


class FunnelEndpointContractTests(SimpleTestCase):
    def test_ingestion_endpoint_is_not_csrf_exempt(self) -> None:
        match = resolve("/app/api/funnel/event")

        self.assertIs(match.func, funnel_views.funnel_event)
        self.assertFalse(getattr(match.func, "csrf_exempt", False))

    def test_billing_consent_endpoint_is_not_csrf_exempt(self) -> None:
        match = resolve("/app/api/funnel/billing-consent")

        self.assertIs(match.func, funnel_views.billing_consent)
        self.assertFalse(getattr(match.func, "csrf_exempt", False))


class BillingConsentTermsTests(SimpleTestCase):
    @override_settings(
        MONO_BIND_AMOUNT=100,
        MONO_RENEWAL_AMOUNT=49900,
        MONO_RENEWAL_PERIOD_DAYS=30,
    )
    def test_terms_are_built_from_server_configuration(self) -> None:
        terms = build_server_consent_terms(trial_days=30, copy_locale="uk")

        self.assertEqual(terms["bind_amount_minor"], 100)
        self.assertEqual(terms["renewal_amount_minor"], 49900)
        self.assertEqual(terms["renewal_period_days"], 30)
        self.assertEqual(terms["trial_days"], 30)
        self.assertEqual(terms["currency"], "UAH")
        self.assertEqual(terms["renewal_anchor"], "payment_success")
        self.assertIsNone(terms["first_renewal_at"])


class FunnelAuthLinkTests(SimpleTestCase):
    @patch("miniapp.funnel.record_server_event")
    @patch("miniapp.funnel.link_acquisition_session")
    @patch("miniapp.auth.UserAuthSession.objects.create")
    def test_login_session_links_server_verified_user(
        self,
        _session_create,
        link_acquisition_session,
        _record_event,
    ) -> None:
        request = SimpleNamespace(session=Session(), META={"HTTP_USER_AGENT": "Synthetic browser"})
        user = SimpleNamespace(tg_user_id=812345)

        auth.login_session(request, user)

        link_acquisition_session.assert_called_once_with(request, user=user)

    @patch("miniapp.funnel.record_server_event")
    @patch("miniapp.funnel.link_acquisition_session")
    @patch("miniapp.auth.UserAuthSession.objects.create")
    def test_login_session_records_server_auth_success(
        self,
        _session_create,
        link_acquisition_session,
        record_event,
    ) -> None:
        acquisition = SimpleNamespace(pk="0b387247-f643-4e4c-8e6f-55998c22be20")
        link_acquisition_session.return_value = acquisition
        request = SimpleNamespace(session=Session(), META={"HTTP_USER_AGENT": "Synthetic browser"})
        user = SimpleNamespace(tg_user_id=812345)

        auth.login_session(request, user)

        record_event.assert_called_once_with(
            acquisition=acquisition,
            user=user,
            event_name="auth_success",
            idempotency_key="auth_success:812345",
        )


class RequestServerEventTests(SimpleTestCase):
    @patch("miniapp.funnel.record_server_event")
    @patch("miniapp.funnel.link_acquisition_session")
    def test_authenticated_action_uses_existing_acquisition_only(
        self,
        link_acquisition_session,
        record_server_event,
    ) -> None:
        from miniapp.funnel import record_request_server_event

        acquisition = SimpleNamespace(pk="0b387247-f643-4e4c-8e6f-55998c22be20")
        link_acquisition_session.return_value = acquisition
        request = SimpleNamespace(session=Session())
        user = SimpleNamespace(tg_user_id=812345)

        record_request_server_event(
            request,
            user=user,
            event_name="onboarding_confirmed",
            idempotency_key="onboarding:draft-1",
        )

        link_acquisition_session.assert_called_once_with(request, user=user)
        record_server_event.assert_called_once_with(
            acquisition=acquisition,
            user=user,
            event_name="onboarding_confirmed",
            idempotency_key="onboarding:draft-1",
        )

    @patch("miniapp.funnel.AcquisitionSession.objects.select_for_update")
    def test_expired_acquisition_is_not_linked(self, select_for_update) -> None:
        queryset = select_for_update.return_value.filter.return_value
        queryset.first.return_value = None
        session_id = "0b387247-f643-4e4c-8e6f-55998c22be20"
        request = SimpleNamespace(session=Session({ACQUISITION_SESSION_KEY: session_id}))
        user = SimpleNamespace(tg_user_id=812345)

        from miniapp.funnel import link_acquisition_session

        result = link_acquisition_session.__wrapped__(request, user=user)

        self.assertIsNone(result)
        self.assertNotIn(ACQUISITION_SESSION_KEY, request.session)
        filter_kwargs = select_for_update.return_value.filter.call_args.kwargs
        self.assertEqual(str(filter_kwargs["pk"]), session_id)
        self.assertIn("expires_at__gt", filter_kwargs)

    @patch("miniapp.funnel.record_request_server_event", return_value=None)
    def test_pending_registration_is_kept_until_event_can_be_written(self, record_event) -> None:
        user = SimpleNamespace(tg_user_id=812345)
        request = SimpleNamespace(
            session=Session({PENDING_REGISTRATION_EVENT_KEY: user.tg_user_id})
        )

        created = retry_pending_registration_event(request, user=user)

        self.assertFalse(created)
        self.assertEqual(request.session[PENDING_REGISTRATION_EVENT_KEY], user.tg_user_id)
        record_event.assert_called_once()

    @patch("miniapp.funnel.record_request_server_event", return_value=(object(), True))
    def test_pending_registration_is_cleared_after_success(self, _record_event) -> None:
        user = SimpleNamespace(tg_user_id=812345)
        request = SimpleNamespace(
            session=Session({PENDING_REGISTRATION_EVENT_KEY: user.tg_user_id})
        )

        created = retry_pending_registration_event(request, user=user)

        self.assertTrue(created)
        self.assertNotIn(PENDING_REGISTRATION_EVENT_KEY, request.session)


class OnboardingFunnelHookTests(SimpleTestCase):
    @patch("miniapp.views.record_request_server_event")
    @patch("miniapp.views._install_nudge_state")
    @patch("miniapp.views._save_session_drafts")
    @patch("miniapp.views._commit_draft_once", return_value=({"completed": True}, False))
    @patch(
        "miniapp.views._session_drafts",
        return_value={"draft-1": {"status": "draft", "draft": {"currency": "UAH"}}},
    )
    @patch("miniapp.views._onboarding_user_or_response")
    def test_onboarding_event_is_recorded_after_commit(
        self,
        get_user,
        _session_drafts,
        _commit_draft,
        _save_drafts,
        _install_state,
        record_event,
    ) -> None:
        user = SimpleNamespace(tg_user_id=812345)
        get_user.return_value = user
        request = RequestFactory().post(
            "/app/api/onboarding/confirm",
            data=json.dumps({"draft_id": "draft-1", "idempotency_key": "onboarding-key-0001"}),
            content_type="application/json",
        )

        response = views.onboarding_confirm(request)

        self.assertEqual(response.status_code, 200)
        record_event.assert_called_once_with(
            request,
            user=user,
            event_name="onboarding_confirmed",
            idempotency_key="onboarding:draft-1",
        )


class FirstTransactionFunnelHookTests(SimpleTestCase):
    @patch("miniapp.views.record_request_server_event")
    @patch("miniapp.views._commit_ai_image_batch_item", return_value=None)
    @patch("miniapp.views._save_session_transaction_drafts")
    @patch("miniapp.views._commit_draft_once", return_value=({"transaction_id": 77}, False))
    @patch(
        "miniapp.views._session_transaction_drafts",
        return_value={"draft-1": {"status": "draft", "draft": {"amount": "999.00"}}},
    )
    @patch("miniapp.views._authorized_write_user_or_blocked")
    def test_first_transaction_event_contains_no_financial_payload(
        self,
        authorize,
        _session_drafts,
        _commit_draft,
        _save_drafts,
        _commit_batch,
        record_event,
    ) -> None:
        user = SimpleNamespace(tg_user_id=812345)
        authorize.return_value = (user, {"access_scope": "personal_full"})
        request = RequestFactory().post(
            "/app/api/transactions/confirm",
            data=json.dumps({"draft_id": "draft-1", "idempotency_key": "transaction-key-0001"}),
            content_type="application/json",
        )

        response = views.transaction_confirm(request)

        self.assertEqual(response.status_code, 200)
        record_event.assert_called_once_with(
            request,
            user=user,
            event_name="first_transaction_confirmed",
            idempotency_key="first_transaction:812345",
        )


class BillingConsentCheckoutTests(SimpleTestCase):
    @patch("miniapp.views._settings_payload", return_value={})
    @patch("miniapp.views.build_bind_invoice")
    @patch("miniapp.views.resolve_access", return_value={"trial_days": 30, "has_card": False})
    @patch("miniapp.views._get_session_user_or_response")
    def test_bind_action_fails_closed_without_consent(
        self,
        get_user,
        _resolve_access,
        build_bind_invoice,
        _settings_payload,
    ) -> None:
        get_user.return_value = SimpleNamespace(tg_user_id=812345)
        request = RequestFactory().post(
            "/app/api/settings/billing",
            data=json.dumps({"action": "bind"}),
            content_type="application/json",
        )

        response = views.billing_action(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "billing_consent_required")
        build_bind_invoice.assert_not_called()

    @patch("miniapp.views._settings_payload", return_value={})
    @patch("miniapp.views.attach_consent_to_payment_id")
    @patch("miniapp.views.get_billing_consent_for_user")
    @patch("miniapp.views.build_bind_invoice")
    @patch("miniapp.views.resolve_access")
    @patch("miniapp.views._get_session_user_or_response")
    def test_bind_action_attaches_explicit_consent_to_payment(
        self,
        get_user,
        resolve_access,
        build_bind_invoice,
        get_billing_consent,
        attach_consent,
        _settings_payload,
    ) -> None:
        user = SimpleNamespace(tg_user_id=812345)
        get_user.return_value = user
        resolve_access.return_value = {"trial_days": 30, "has_card": False}
        build_bind_invoice.return_value = {
            "payment_id": 41,
            "page_url": "https://pay.example/checkout",
            "trial_days": 30,
            "trial_granted": True,
        }
        request = RequestFactory().post(
            "/app/api/settings/billing",
            data=json.dumps(
                {
                    "action": "bind",
                    "consent_id": "bbaf6f4f-800d-43d5-81d0-9c2e419f561e",
                }
            ),
            content_type="application/json",
        )

        response = views.billing_action(request)

        self.assertEqual(response.status_code, 200)
        attach_consent.assert_called_once_with(
            consent_id="bbaf6f4f-800d-43d5-81d0-9c2e419f561e",
            user=user,
            payment_id=41,
        )


class BillingConsentClientContractTests(SimpleTestCase):
    def test_miniapp_requires_explicit_terms_checkbox_before_bind(self) -> None:
        source = get_template("miniapp/index.html").template.source

        self.assertIn("data-billing-consent-url", source)
        self.assertIn('id="billingConsentAccepted"', source)
        self.assertIn('id="billingConsentTerms"', source)
        self.assertIn("createBillingConsent", source)
        self.assertIn("consent_id: consentPayload.consent_id", source)
