from __future__ import annotations

from datetime import date
import base64
import os
import sys
import unittest
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("DJANGO_SECRET_KEY", "local-dev-key")
os.environ.setdefault("DJANGO_DEBUG", "true")
os.environ.setdefault("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost")

import django  # noqa: E402

django.setup()

from django.core.files.uploadedfile import SimpleUploadedFile  # noqa: E402
from django.conf import settings  # noqa: E402
from django.http import HttpResponse  # noqa: E402
from django.template.loader import get_template  # noqa: E402
from django.test import Client, RequestFactory, SimpleTestCase, override_settings  # noqa: E402

from miniapp import auth, history, image_uploads, onboarding, operator_hub, push, services, views  # noqa: E402
from miniapp.services import PeriodSelection, _bucket_dates_for_period, _bucket_mode  # noqa: E402


class FakeSession(dict):
    def cycle_key(self) -> None:
        return None

    def set_expiry(self, value: int) -> None:
        self["_expiry"] = value


class InternalRouteSecuritySettingsTests(SimpleTestCase):
    def test_transaction_internal_routes_bypass_https_redirect_for_compose_network_calls(self) -> None:
        self.assertIn(r"^internal/transactions/", settings.SECURE_REDIRECT_EXEMPT)


class MiniAppMoneyAggregationUnitTests(SimpleTestCase):
    def test_credit_limit_parser_uses_money_precision_and_database_range(self) -> None:
        value = services._parse_optional_credit_limit("9999999999999999.99")
        self.assertEqual(str(value), "9999999999999999.99")

    def test_current_debt_combines_credit_debt_and_active_payables(self) -> None:
        combined = services._combine_current_debt_payload(
            {"summary": {"total_debt": services.money_payload("250.00", "UAH")}},
            {"summary": {"owed_by_user": services.money_payload("90000.00", "UAH")}},
            preferred_currency="UAH",
            locale="uk",
        )

        self.assertEqual(combined["value"], "90250.00")
        self.assertEqual(combined["display"], "90 250 грн")


class MiniAppImageUploadUnitTests(unittest.TestCase):
    def test_non_heic_image_passes_through_without_reencoding(self) -> None:
        image_bytes = b"\x89PNG\r\n\x1a\nimage"

        normalized_bytes, mime_type = image_uploads.normalize_ai_image_upload(image_bytes, "image/png")

        self.assertIs(normalized_bytes, image_bytes)
        self.assertEqual(mime_type, "image/png")

    def test_heic_with_excessive_pixel_count_is_rejected_before_decode(self) -> None:
        source = MagicMock()
        source.__enter__.return_value = source
        source.size = (5001, 5001)

        with (
            patch.object(image_uploads.Image, "open", return_value=source),
            self.assertRaises(image_uploads.MiniAppImageUploadError),
        ):
            image_uploads.normalize_ai_image_upload(b"heic-bytes", "image/heic")

        source.copy.assert_not_called()


class MiniAppOnboardingUnitTests(unittest.TestCase):
    def test_completion_checks_accounts_in_active_finance_scope(self) -> None:
        user = SimpleNamespace(
            tg_user_id=1001,
            onboarding_completed=True,
            onboarding_version=onboarding.CURRENT_ONBOARDING_VERSION,
            start_date=date(2026, 7, 1),
            base_currency="UAH",
        )

        with (
            patch.object(onboarding, "_scoped_accounts_qs") as scoped_accounts,
            patch.object(onboarding, "_scoped_categories_qs") as scoped_categories,
        ):
            scoped_accounts.return_value.filter.return_value.exists.return_value = True
            scoped_categories.return_value.filter.return_value.values_list.return_value = ["income", "expense"]

            self.assertTrue(onboarding.onboarding_is_complete(user))

        scoped_accounts.assert_called_once_with(user)
        scoped_accounts.return_value.filter.assert_called_once_with(is_active=True)


class MiniAppPostgresLockingUnitTests(unittest.TestCase):
    def test_debt_lock_targets_only_debt_row_not_nullable_account_join(self) -> None:
        queryset = MagicMock()
        queryset.select_related.return_value.filter.return_value = queryset
        queryset.select_for_update.return_value = queryset
        expected = object()
        queryset.first.return_value = expected

        with patch.object(services, "_scoped_debts_qs", return_value=queryset):
            result = services._active_scoped_debt(SimpleNamespace(), 17, for_update=True)

        self.assertIs(result, expected)
        queryset.select_for_update.assert_called_once_with(of=("self",))

    def test_history_lock_targets_only_transaction_row_not_nullable_joins(self) -> None:
        queryset = MagicMock()
        queryset.filter.return_value.select_related.return_value = queryset
        queryset.select_for_update.return_value = queryset
        expected = object()
        queryset.first.return_value = expected

        with patch.object(history, "_scoped_transactions_qs", return_value=queryset):
            result = history._normal_transaction(SimpleNamespace(), 29, lock=True)

        self.assertIs(result, expected)
        queryset.select_for_update.assert_called_once_with(of=("self",))


class OperatorActionServiceUnitTests(SimpleTestCase):
    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_subscription_extension_reuses_canonical_subscription_service(self) -> None:
        operator = SimpleNamespace(tg_user_id=7884326049)
        target = SimpleNamespace(tg_user_id=1001, pk=1001)
        previous = SimpleNamespace(status="trial", expires_at=None, plan="solo")
        updated = SimpleNamespace(pk=44, status="manual", expires_at=None)
        detail = {"tg_user_id": 1001, "subscription_status": "paid"}

        with (
            patch.object(operator_hub, "_target_user", return_value=target),
            patch.object(operator_hub, "latest_subscription_for_user", return_value=previous),
            patch.object(operator_hub, "resolve_subscription_plan_ref", return_value=None),
            patch.object(operator_hub, "apply_subscription_change", return_value=updated) as apply_change,
            patch.object(operator_hub, "_audit_operator_action") as audit,
            patch.object(operator_hub, "build_operator_user_detail", return_value=detail),
        ):
            result = operator_hub.execute_operator_user_action(
                operator=operator,
                tg_user_id=1001,
                action="subscription_extend",
                payload={"days": "45", "reason": "support request"},
            )

        self.assertEqual(result["user"]["subscription_status"], "paid")
        apply_change.assert_called_once_with(
            user=target,
            action="manual",
            admin_user=None,
            plan_slug="solo",
            plan_ref=None,
            days=45,
            comment="support request",
        )
        audit.assert_called_once()

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_non_operator_cannot_execute_user_action(self) -> None:
        with self.assertRaises(operator_hub.OperatorUserError) as raised:
            operator_hub.execute_operator_user_action(
                operator=SimpleNamespace(tg_user_id=123),
                tg_user_id=1001,
                action="subscription_extend",
                payload={"days": 30},
            )

        self.assertEqual(raised.exception.code, "operator_access_denied")

class MiniAppViewUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = RequestFactory()

    def test_install_nudge_is_eligible_only_after_onboarding_with_active_access(self) -> None:
        onboarded = SimpleNamespace(onboarding_completed=True)
        pending = SimpleNamespace(onboarding_completed=False)

        self.assertTrue(views._install_nudge_eligible(onboarded, {"mode": "active"}))
        self.assertFalse(views._install_nudge_eligible(pending, {"mode": "active"}))
        self.assertFalse(views._install_nudge_eligible(onboarded, {"mode": "paywall"}))
        self.assertFalse(views._install_nudge_eligible(onboarded, {}))

    def test_install_device_identifier_is_hashed_and_invalid_values_are_rejected(self) -> None:
        raw_device_id = "00000000-0000-4000-8000-000000000001"
        user = SimpleNamespace(tg_user_id=1001)
        device_hash = views._install_nudge_device_hash(user, raw_device_id)

        self.assertEqual(len(device_hash), 40)
        self.assertNotIn(raw_device_id, device_hash)
        self.assertEqual(device_hash, views._install_nudge_device_hash(user, raw_device_id.upper()))
        self.assertNotEqual(device_hash, views._install_nudge_device_hash(SimpleNamespace(tg_user_id=1002), raw_device_id))
        self.assertEqual(views._install_nudge_device_hash(user, "not-a-device-id"), "")
        self.assertEqual(views._install_nudge_device_hash(user, ""), "")
        self.assertEqual(views._install_nudge_device_hash(SimpleNamespace(), raw_device_id), "")
        self.assertEqual(views._install_nudge_device_hash(SimpleNamespace(tg_user_id=0), raw_device_id), "")

    def test_install_nudge_event_rejects_ineligible_user_before_touching_state(self) -> None:
        request = self.factory.post(
            "/app/api/install-nudge/event",
            data=json.dumps(
                {
                    "event": "shown",
                    "platform": "android",
                    "device_id": "00000000-0000-4000-8000-000000000001",
                }
            ),
            content_type="application/json",
        )
        request.session = FakeSession()
        user = SimpleNamespace(tg_user_id=1001, onboarding_completed=False)

        with (
            patch.object(views, "_get_session_user_or_response", return_value=user),
            patch.object(views, "resolve_access", return_value={"mode": "active", "locale": "uk"}),
            patch.object(
                views.InstallNudgeState.objects,
                "select_for_update",
                side_effect=AssertionError("install state must not be touched"),
            ),
        ):
            response = views.install_nudge_event(request)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(response.content)["error"]["code"], "access_blocked")

    def test_telegram_miniapp_login_uses_validated_auth_date_for_session_freshness(self) -> None:
        request = self.factory.post("/app/api/auth/telegram", data=json.dumps({"init_data": "signed"}), content_type="application/json")
        request.session = FakeSession()
        user = SimpleNamespace(tg_user_id=1001, first_name="Ihor", base_currency="UAH", lang="uk")
        identity = SimpleNamespace(
            tg_user_id=1001,
            first_name="Ihor",
            last_name="",
            username="ihor",
            raw_user={"language_code": "uk"},
            auth_date=1_700_000_000,
        )
        registration = SimpleNamespace(user=user, identity=object())

        with (
            patch.object(views, "validate_telegram_init_data", return_value=identity),
            patch.object(views, "bootstrap_telegram_identity", return_value=registration),
            patch.object(views, "login_session") as login_mock,
            patch.object(views, "resolve_access", return_value={"mode": "active"}),
        ):
            response = views.auth_telegram(request)

        self.assertEqual(response.status_code, 200)
        login_mock.assert_called_once_with(request, user, authenticated_at=identity.auth_date)

    def test_telegram_oidc_login_uses_validated_iat_for_session_freshness(self) -> None:
        request = self.factory.get("/app/auth/telegram/callback?state=state&code=code")
        request.session = FakeSession()
        pending = SimpleNamespace(code="code", code_verifier="verifier", nonce="nonce", return_to="/app/")
        user = SimpleNamespace(tg_user_id=1001)
        registration_identity = object()
        registration = SimpleNamespace(user=user, identity=registration_identity)
        oidc_identity = SimpleNamespace(claims={"iat": 1_700_000_100})

        with (
            patch.object(views, "consume_pending_authorization", return_value=pending),
            patch.object(views, "exchange_authorization_code", return_value="id-token"),
            patch.object(views, "verify_id_token", return_value=oidc_identity),
            patch.object(views, "consume_id_token_once"),
            patch.object(views, "bootstrap_telegram_identity", return_value=registration),
            patch.object(views, "login_session") as login_mock,
        ):
            response = views.telegram_oidc_callback(request)

        self.assertEqual(response.status_code, 302)
        login_mock.assert_called_once_with(
            request,
            user,
            session_age_seconds=auth.browser_session_age_seconds(),
            auth_mode=views.TELEGRAM_OIDC_AUTH_MODE,
            identity=registration_identity,
            authenticated_at=oidc_identity.claims["iat"],
        )

    def test_profile_requires_session(self) -> None:
        request = self.factory.get("/app/api/profile")
        request.session = FakeSession()

        response = views.profile(request)

        self.assertEqual(response.status_code, 401)
        self.assertEqual(json.loads(response.content)["error"]["code"], "session_required")
    def test_profile_returns_user_payload_with_patched_session_user(self) -> None:
        request = self.factory.get("/app/api/profile")
        request.session = FakeSession({"miniapp_tg_user_id": 1001})
        fake_user = SimpleNamespace(tg_user_id=1001, first_name="Ihor", base_currency="UAH")

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "resolve_access", return_value={"mode": "active"}),
        ):
            response = views.profile(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["user"]["tg_user_id"], 1001)
        self.assertEqual(payload["user"]["first_name"], "Ihor")
        self.assertEqual(payload["access"]["mode"], "active")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_bootstrap_rejects_non_operator_session(self) -> None:
        request = self.factory.get("/app/api/operator/bootstrap")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=123)

        with patch.object(views, "get_session_user", return_value=fake_user):
            response = views.operator_bootstrap(request)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(response.content)["error"]["code"], "operator_access_denied")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_bootstrap_returns_read_only_payload_for_operator(self) -> None:
        request = self.factory.get("/app/api/operator/bootstrap?q=askills")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=7884326049)
        payload = {"operator": {"tg_user_id": 7884326049}, "metrics": [], "attention": [], "users": []}

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "build_operator_payload", return_value=payload) as builder,
        ):
            response = views.operator_bootstrap(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["operator"]["tg_user_id"], 7884326049)
        builder.assert_called_once_with(fake_user, query="askills")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_user_detail_stays_inside_allowlisted_api(self) -> None:
        request = self.factory.get("/app/api/operator/users/1001/")
        request.session = FakeSession()
        operator = SimpleNamespace(tg_user_id=7884326049)
        detail = {"tg_user_id": 1001, "name": "Test User", "is_blocked": False}

        with (
            patch.object(views, "get_session_user", return_value=operator),
            patch.object(views, "build_operator_user_detail", return_value=detail) as builder,
        ):
            response = views.operator_user_detail(request, 1001)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["user"]["tg_user_id"], 1001)
        builder.assert_called_once_with(1001)

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_user_status_requires_explicit_confirmation(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/status",
            data=json.dumps({"action": "ban"}),
            content_type="application/json",
        )
        request.session = FakeSession()

        with patch.object(views, "get_session_user", return_value=SimpleNamespace(tg_user_id=7884326049)):
            response = views.operator_user_status(request, 1001)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "confirmation_required")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_user_status_calls_audited_status_service(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/status",
            data=json.dumps({"action": "ban", "confirmed": True, "reason": "support"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        operator = SimpleNamespace(tg_user_id=7884326049)
        detail = {"tg_user_id": 1001, "status": "banned", "is_blocked": True}

        with (
            patch.object(views, "get_session_user", return_value=operator),
            patch.object(views, "update_operator_user_status", return_value=detail) as updater,
        ):
            response = views.operator_user_status(request, 1001)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.content)["user"]["is_blocked"])
        updater.assert_called_once_with(
            operator=operator,
            tg_user_id=1001,
            action="ban",
            reason="support",
            request=request,
        )

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_user_action_requires_explicit_confirmation(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/actions",
            data=json.dumps({"action": "subscription_extend", "days": 30}),
            content_type="application/json",
        )
        request.session = FakeSession()

        with patch.object(views, "get_session_user", return_value=SimpleNamespace(tg_user_id=7884326049)):
            response = views.operator_user_action(request, 1001)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "confirmation_required")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_user_action_calls_shared_operator_service(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/actions",
            data=json.dumps({"action": "subscription_extend", "days": 30, "confirmed": True}),
            content_type="application/json",
        )
        request.session = FakeSession()
        operator = SimpleNamespace(tg_user_id=7884326049)
        action_result = {"message": "done", "user": {"tg_user_id": 1001}}

        with (
            patch.object(views, "get_session_user", return_value=operator),
            patch.object(views, "execute_operator_user_action", return_value=action_result) as executor,
        ):
            response = views.operator_user_action(request, 1001)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["message"], "done")
        executor.assert_called_once_with(
            operator=operator,
            tg_user_id=1001,
            action="subscription_extend",
            payload={"action": "subscription_extend", "days": 30, "confirmed": True},
            request=request,
        )

    @override_settings(DEBUG=True)
    def test_operator_user_action_preview_is_read_only(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/actions?preview=1",
            data=json.dumps({"action": "subscription_extend", "days": 30, "confirmed": True}),
            content_type="application/json",
        )
        request.session = FakeSession()

        response = views.operator_user_action(request, 1001)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(response.content)["error"]["code"], "preview_read_only")

    @override_settings(DEBUG=True)
    def test_operator_user_status_preview_is_read_only(self) -> None:
        request = self.factory.post(
            "/app/api/operator/users/1001/status?preview=1",
            data=json.dumps({"action": "ban", "confirmed": True}),
            content_type="application/json",
        )
        request.session = FakeSession()

        response = views.operator_user_status(request, 1001)

        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(response.content)["error"]["code"], "preview_read_only")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_browser_login_redirects_allowlisted_user_to_admin_hub(self) -> None:
        request = self.factory.get("/app/login/token/?operator=1")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=7884326049)
        fake_identity = SimpleNamespace(issued_at=1_700_000_000)

        with (
            patch.object(
                views,
                "consume_browser_login_token_with_identity",
                return_value=(fake_user, fake_identity),
            ),
            patch.object(views, "login_session"),
        ):
            response = views.browser_login(request, "token")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, "/app/operator/")

    @override_settings(MINIAPP_OPERATOR_TELEGRAM_IDS=[7884326049])
    def test_operator_helper_uses_numeric_allowlist_not_username(self) -> None:
        self.assertTrue(operator_hub.is_operator_user(SimpleNamespace(tg_user_id=7884326049, username="anything")))
        self.assertFalse(operator_hub.is_operator_user(SimpleNamespace(tg_user_id=123, username="Askills_Support")))

    def test_operator_and_dashboard_templates_compile(self) -> None:
        self.assertIsNotNone(get_template("miniapp/operator.html"))
        self.assertIsNotNone(get_template("dashboard/index.html"))
        template_source = get_template("miniapp/operator.html").template.source
        self.assertIn("data-user-url-template", template_source)
        self.assertIn("data-user-action-url-template", template_source)
        self.assertIn('data-operator-action="subscription_extend"', template_source)
        self.assertIn('data-operator-action="send_message"', template_source)
        self.assertIn('data-operator-action="hard_delete"', template_source)
        self.assertIn("openUser(user.tg_user_id)", template_source)
        self.assertNotIn('clickableCard("card user-card", user.admin_url)', template_source)

    @override_settings(DEBUG=True)
    def test_operator_debug_preview_does_not_require_session(self) -> None:
        request = self.factory.get("/app/api/operator/bootstrap?preview=1")
        request.session = FakeSession()

        response = views.operator_bootstrap(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["operator"]["tg_user_id"], 7884326049)
        self.assertTrue(payload["metrics"])

    def test_settings_options_uses_session_user_without_finance_access_gate(self) -> None:
        request = self.factory.get("/app/api/settings")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)
        settings_payload = {"user": {"tg_user_id": 1001}, "access": {"mode": "blocked"}}

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "_settings_payload", return_value=settings_payload) as builder,
        ):
            response = views.settings_options(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["access"]["mode"], "blocked")
        builder.assert_called_once_with(fake_user)

    def test_settings_profile_updates_current_session_user(self) -> None:
        request = self.factory.post(
            "/app/api/settings/profile",
            data=json.dumps({"lang": "en", "base_currency": "USD"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "update_profile_settings", return_value={"lang": "en", "base_currency": "USD"}) as updater,
            patch.object(views, "_settings_payload", return_value={"user": {"lang": "en"}}),
        ):
            response = views.settings_profile(request)

        self.assertEqual(response.status_code, 200)
        updater.assert_called_once_with(fake_user, {"lang": "en", "base_currency": "USD"})

    def test_settings_reminder_rejects_unsupported_schedule(self) -> None:
        request = self.factory.post(
            "/app/api/settings/reminder",
            data=json.dumps({"mode": "monthly", "reminder_hour": 18}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with patch.object(views, "get_session_user", return_value=fake_user):
            response = views.settings_reminder(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_reminder_settings")

    def test_settings_savings_rejects_zero_default_percent(self) -> None:
        request = self.factory.post(
            "/app/api/settings/savings",
            data=json.dumps({"default_percent": "0"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(
                views,
                "update_saving_prompt_settings",
                side_effect=views.MiniAppTransactionError("invalid_saving_settings", "Saving percentage must be a number from 0 to 100."),
            ),
        ):
            response = views.settings_savings(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_saving_settings")

    def test_billing_bind_returns_user_specific_monobank_url(self) -> None:
        request = self.factory.post(
            "/app/api/settings/billing",
            data=json.dumps({"action": "bind", "consent_id": "5e48ce86-24cc-4998-80a3-6e1f34fe81a9"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)
        access = {"mode": "blocked", "access_scope": "paywall", "has_card": False, "trial_days": 30}

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "resolve_access", return_value=access),
            patch.object(views, "get_billing_consent_for_user", return_value=object()),
            patch.object(views, "attach_consent_to_payment_id") as attach_consent,
            patch.object(views, "build_bind_invoice", return_value={"page_url": "https://pay.example/1001", "payment_id": 91, "trial_days": 30, "trial_granted": True}) as bind_invoice,
            patch.object(views, "_settings_payload", return_value={"user": {"tg_user_id": 1001}}),
        ):
            response = views.billing_action(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["action"], "bind")
        self.assertEqual(payload["action_url"], "https://pay.example/1001")
        bind_invoice.assert_called_once_with(user_id=1001, trial_days=30, mode="bind", promo_code="")
        attach_consent.assert_called_once_with(
            consent_id="5e48ce86-24cc-4998-80a3-6e1f34fe81a9",
            user=fake_user,
            payment_id=91,
        )

    def test_help_center_uses_canonical_bot_help_client_for_session_user(self) -> None:
        request = self.factory.get("/app/api/help", {"topic": "getting_started"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, lang="en")
        canonical_payload = {"mode": "topic", "locale": "en", "title": "Getting started", "questions": []}

        with (
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(views, "request_help_content", return_value=canonical_payload) as help_client,
        ):
            response = views.help_center(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["title"], "Getting started")
        help_client.assert_called_once_with(locale="en", topic_id="getting_started")

    def test_billing_expense_options_require_write_access_and_use_billing_draft_builder(self) -> None:
        request = self.factory.get("/app/api/billing-expense/options")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_billing_expense_options", return_value={"draft": None, "options": None}) as builder,
        ):
            response = views.billing_expense_options(request)

        self.assertEqual(response.status_code, 200)
        builder.assert_called_once_with(fake_user)

    def test_ai_text_parse_requires_write_access_and_returns_prefill_suggestion(self) -> None:
        request = self.factory.post(
            "/app/api/ai-text/parse",
            data=json.dumps({"text": "Таксі 250 грн"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {
            "is_candidate_tx": True,
            "intent": "expense",
            "amount": "250.00",
            "currency": "UAH",
            "currency_explicit": True,
            "category_slug": "taxi",
        }
        suggestion = {"mode": "transaction", "kind": "expense", "amount": "250.00"}

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_text_parse", return_value=parsed) as parser_client,
            patch.object(views, "build_ai_text_suggestion", return_value=suggestion) as builder,
        ):
            response = views.ai_text_parse(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["suggestion"], suggestion)
        parser_client.assert_called_once_with(text="Таксі 250 грн", default_currency="UAH")
        builder.assert_called_once_with(fake_user, parsed)

    def test_ai_text_parse_rejects_empty_input_without_calling_internal_parser(self) -> None:
        request = self.factory.post(
            "/app/api/ai-text/parse",
            data=json.dumps({"text": "  "}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_text_parse") as parser_client,
        ):
            response = views.ai_text_parse(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_payload")
        parser_client.assert_not_called()

    def test_ai_text_parse_guards_explicit_transfer_before_form_hydration(self) -> None:
        text = "Переказати 10 грн з ПриватБанк •3882 на ПриватБанк •4956"
        request = self.factory.post(
            "/app/api/ai-text/parse",
            data=json.dumps({"text": text}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {
            "is_candidate_tx": True,
            "intent": "expense",
            "amount": "10.00",
            "currency": "UAH",
            "category_slug": "taxi",
        }

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_text_parse", return_value=parsed),
            patch.object(views, "build_ai_text_suggestion", return_value={"mode": "redirect", "intent": "transfer"}) as builder,
        ):
            response = views.ai_text_parse(request)

        self.assertEqual(response.status_code, 200)
        guarded = builder.call_args.args[1]
        self.assertEqual(guarded["intent"], "transfer")
        self.assertTrue(guarded["is_candidate_tx"])
        self.assertIsNone(guarded["category_slug"])

    def test_ai_text_parse_surfaces_internal_parser_failure(self) -> None:
        request = self.factory.post(
            "/app/api/ai-text/parse",
            data=json.dumps({"text": "Таксі 250 грн"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_text_parse", side_effect=views.MiniAppAiTextError("Text recognition is temporarily unavailable.")),
        ):
            response = views.ai_text_parse(request)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(json.loads(response.content)["error"]["code"], "ai_text_unavailable")

    def test_ai_voice_parse_requires_write_access_and_returns_prefill_without_audio_persistence(self) -> None:
        request = self.factory.post(
            "/app/api/ai-voice/parse",
            data={"audio": SimpleUploadedFile("voice.ogg", b"OggSvoice-data", content_type="audio/ogg")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {"is_candidate_tx": True, "intent": "expense", "amount": "250.00", "currency": "UAH"}
        suggestion = {"mode": "transaction", "kind": "expense", "amount": "250.00"}

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_voice_parse", return_value={"transcript": "Taxi 250 UAH", "parsed": parsed}) as parser_client,
            patch.object(views, "build_ai_text_suggestion", return_value=suggestion) as builder,
        ):
            response = views.ai_voice_parse(request)

        payload = json.loads(response.content)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["transcript"], "Taxi 250 UAH")
        self.assertEqual(payload["suggestion"], suggestion)
        self.assertNotIn("audio", request.session)
        self.assertEqual(parser_client.call_args.kwargs["mime_type"], "audio/ogg")
        builder.assert_called_once_with(fake_user, parsed)

    def test_ai_voice_parse_rejects_unsupported_upload_without_calling_internal_parser(self) -> None:
        request = self.factory.post(
            "/app/api/ai-voice/parse",
            data={"audio": SimpleUploadedFile("voice.txt", b"not-audio", content_type="text/plain")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_voice_parse") as parser_client,
        ):
            response = views.ai_voice_parse(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_payload")
        parser_client.assert_not_called()

    def test_ai_voice_parse_surfaces_internal_parser_failure(self) -> None:
        request = self.factory.post(
            "/app/api/ai-voice/parse",
            data={"audio": SimpleUploadedFile("voice.ogg", b"OggSvoice-data", content_type="audio/ogg")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_voice_parse", side_effect=views.MiniAppAiVoiceError("Voice recognition is temporarily unavailable.")),
        ):
            response = views.ai_voice_parse(request)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(json.loads(response.content)["error"]["code"], "ai_voice_unavailable")

    def test_ai_voice_signature_detection_accepts_only_supported_containers(self) -> None:
        self.assertEqual(services.detect_ai_voice_mime(b"OggSvoice-data"), "audio/ogg")
        self.assertEqual(services.detect_ai_voice_mime(b"\x1aE\xdf\xa3\x00\x00webm"), "audio/webm")
        self.assertEqual(services.detect_ai_voice_mime(b"\x00\x00\x00\x18ftypM4A "), "audio/mp4")
        self.assertIsNone(services.detect_ai_voice_mime(b"not-audio"))

    def test_ai_image_parse_requires_write_access_and_returns_prefill_suggestion(self) -> None:
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("receipt.png", b"\x89PNG\r\n\x1a\nimage", content_type="image/png")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {"mode": "single_tx", "type": "expense", "amount": "250.00"}
        suggestion = {"mode": "transaction", "kind": "expense", "amount": "250.00"}

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_ai_image_catalog", return_value={"accounts": []}) as catalog_builder,
            patch.object(views, "request_ai_image_parse", return_value=parsed) as parser_client,
            patch.object(views, "build_ai_image_suggestion", return_value=suggestion) as builder,
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)["suggestion"], suggestion)
        catalog_builder.assert_called_once_with(fake_user)
        parser_client.assert_called_once()
        self.assertEqual(parser_client.call_args.kwargs["mime_type"], "image/png")
        builder.assert_called_once_with(fake_user, parsed)

    def test_ai_image_parse_normalizes_iphone_heic_before_internal_parser(self) -> None:
        heic_bytes = b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1heic"
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("screenshot.heic", heic_bytes, content_type="image/heic")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {"mode": "single_tx", "type": "expense", "amount": "250.00"}
        suggestion = {"mode": "transaction", "kind": "expense", "amount": "250.00"}

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "normalize_ai_image_upload", return_value=(b"jpeg-bytes", "image/jpeg")) as normalizer,
            patch.object(views, "build_ai_image_catalog", return_value={"accounts": []}),
            patch.object(views, "request_ai_image_parse", return_value=parsed) as parser_client,
            patch.object(views, "build_ai_image_suggestion", return_value=suggestion),
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 200)
        normalizer.assert_called_once_with(heic_bytes, "image/heic")
        self.assertEqual(parser_client.call_args.kwargs["image_bytes"], b"jpeg-bytes")
        self.assertEqual(parser_client.call_args.kwargs["mime_type"], "image/jpeg")

    def test_ai_image_parse_rejects_heic_that_cannot_be_decoded(self) -> None:
        heic_bytes = b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1heic"
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("screenshot.heic", heic_bytes, content_type="image/heic")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(
                views,
                "normalize_ai_image_upload",
                side_effect=image_uploads.MiniAppImageUploadError("bad heic"),
            ),
            patch.object(views, "request_ai_image_parse") as parser_client,
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_payload")
        parser_client.assert_not_called()

    def test_ai_image_signature_detection_accepts_iphone_heic_brand(self) -> None:
        heic_bytes = b"\x00\x00\x00\x18ftypheic\x00\x00\x00\x00mif1heic"

        self.assertEqual(services.detect_ai_image_mime(heic_bytes), "image/heic")
        self.assertIsNone(services.detect_ai_image_mime(b"\x00\x00\x00\x18ftypavif\x00\x00\x00\x00mif1avif"))

    def test_ai_image_parse_rejects_unsupported_upload_without_calling_internal_parser(self) -> None:
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("receipt.svg", b"<svg></svg>", content_type="image/svg+xml")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "request_ai_image_parse") as parser_client,
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(json.loads(response.content)["error"]["code"], "invalid_payload")
        parser_client.assert_not_called()

    def test_ai_image_parse_surfaces_internal_parser_failure(self) -> None:
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("receipt.png", b"\x89PNG\r\n\x1a\nimage", content_type="image/png")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_ai_image_catalog", return_value={"accounts": []}),
            patch.object(views, "request_ai_image_parse", side_effect=views.MiniAppAiImageError("Screenshot recognition is temporarily unavailable.")),
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(json.loads(response.content)["error"]["code"], "ai_image_unavailable")

    def test_ai_image_parse_persists_normalized_statement_batch_without_upload(self) -> None:
        request = self.factory.post(
            "/app/api/ai-image/parse",
            data={"image": SimpleUploadedFile("statement.png", b"\x89PNG\r\n\x1a\nimage", content_type="image/png")},
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, base_currency="UAH")
        parsed = {"mode": "statement_expenses", "statement_items": [{"amount": "250.00"}]}
        batch = {
            "mode": "batch",
            "batch_id": "batch-1",
            "currency": "UAH",
            "currency_explicit": True,
            "items": [{"id": "1", "status": "pending", "suggestion": {"mode": "transaction", "amount": "250.00"}}],
            "item_count": 1,
            "skipped_items_count": 0,
        }

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_ai_image_catalog", return_value={"accounts": []}),
            patch.object(views, "request_ai_image_parse", return_value=parsed),
            patch.object(views, "build_ai_image_batch", return_value=batch) as builder,
        ):
            response = views.ai_image_parse(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(request.session[views.AI_IMAGE_BATCH_SESSION_KEY], batch)
        self.assertNotIn("image", request.session[views.AI_IMAGE_BATCH_SESSION_KEY])
        self.assertEqual(json.loads(response.content)["suggestion"]["mode"], "batch")
        builder.assert_called_once()

    def test_ai_image_batch_item_can_be_reviewed_or_skipped_without_write(self) -> None:
        batch = {
            "batch_id": "batch-1",
            "identity_scope": {"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
            "currency": "UAH",
            "currency_explicit": True,
            "items": [{"id": "1", "status": "pending", "suggestion": {"mode": "transaction", "amount": "250.00"}}],
            "skipped_items_count": 0,
        }
        fake_user = SimpleNamespace(tg_user_id=1001)
        review_request = self.factory.post(
            "/app/api/ai-image/batch/item",
            data=json.dumps({"batch_id": "batch-1", "item_id": "1", "action": "review"}),
            content_type="application/json",
        )
        review_request.session = FakeSession({views.AI_IMAGE_BATCH_SESSION_KEY: batch})
        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(
                views,
                "_identity_scope_binding",
                return_value={"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
            ),
        ):
            review_response = views.ai_image_batch_item(review_request)

        self.assertEqual(review_response.status_code, 200)
        self.assertEqual(json.loads(review_response.content)["suggestion"]["amount"], "250.00")
        self.assertEqual(batch["items"][0]["status"], "pending")

        skip_request = self.factory.post(
            "/app/api/ai-image/batch/item",
            data=json.dumps({"batch_id": "batch-1", "item_id": "1", "action": "skip"}),
            content_type="application/json",
        )
        skip_request.session = review_request.session
        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(
                views,
                "_identity_scope_binding",
                return_value={"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
            ),
        ):
            skip_response = views.ai_image_batch_item(skip_request)

        self.assertEqual(skip_response.status_code, 200)
        self.assertEqual(skip_request.session[views.AI_IMAGE_BATCH_SESSION_KEY]["items"][0]["status"], "skipped")

    def test_saving_task_draft_stays_in_session_until_confirmation(self) -> None:
        request = self.factory.post(
            "/app/api/saving-tasks/draft",
            data=json.dumps({"task_id": 7, "action": "remind", "remind_at": "09:00"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)
        draft = {"draft_id": "saving-draft-1", "action": "remind", "task_id": 7}

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_saving_task_draft", return_value=draft) as builder,
        ):
            response = views.saving_task_draft(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["draft"], draft)
        stored_drafts = request.session[views.SAVING_TASK_DRAFTS_SESSION_KEY]
        self.assertEqual(len(stored_drafts), 1)
        self.assertEqual(next(iter(stored_drafts.values()))["status"], "draft")
        self.assertEqual(next(iter(stored_drafts.values()))["draft"], draft)
        self.assertIs(builder.call_args.args[0], fake_user)
        self.assertEqual(builder.call_args.args[1]["action"], "remind")

    def test_family_create_draft_makes_migration_explicit_before_write(self) -> None:
        request = self.factory.post(
            "/app/api/family/create/draft",
            data=json.dumps({"name": "Shared budget"}),
            content_type="application/json",
        )
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_family_payload", return_value={"mode": "personal"}),
        ):
            response = views.family_create_draft(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertEqual(payload["draft"]["name"], "Shared budget")
        self.assertTrue(payload["draft"]["moves_personal_data"])
        self.assertEqual(
            request.session[views.FAMILY_CREATE_DRAFTS_SESSION_KEY][payload["draft"]["id"]]["status"],
            "draft",
        )

    def test_batch_transaction_confirmation_marks_only_that_item_committed(self) -> None:
        request = self.factory.post(
            "/app/api/transactions/confirm",
            data=json.dumps({"draft_id": "draft-1", "idempotency_key": "x" * 20}),
            content_type="application/json",
        )
        request.session = FakeSession(
            {
                views.TRANSACTION_DRAFTS_SESSION_KEY: {
                    "draft-1": {
                        "status": "draft",
                        "draft": {"draft_id": "draft-1"},
                        "ai_image_batch": {"batch_id": "batch-1", "item_id": "1"},
                    }
                },
                views.AI_IMAGE_BATCH_SESSION_KEY: {
                    "batch_id": "batch-1",
                    "identity_scope": {"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
                    "items": [{"id": "1", "status": "pending", "suggestion": {"mode": "transaction"}}],
                    "skipped_items_count": 0,
                },
            }
        )
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(
                views,
                "_identity_scope_binding",
                return_value={"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
            ),
            patch.object(views, "_commit_draft_once", return_value=({"status": "completed"}, False)) as commit_once,
        ):
            response = views.transaction_confirm(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(request.session[views.AI_IMAGE_BATCH_SESSION_KEY]["items"][0]["status"], "committed")
        self.assertEqual(json.loads(response.content)["batch"]["committed_count"], 1)
        commit_once.assert_called_once()

    def test_skipped_batch_item_cannot_be_confirmed(self) -> None:
        request = self.factory.post(
            "/app/api/transactions/confirm",
            data=json.dumps({"draft_id": "draft-1", "idempotency_key": "x" * 20}),
            content_type="application/json",
        )
        request.session = FakeSession(
            {
                views.TRANSACTION_DRAFTS_SESSION_KEY: {
                    "draft-1": {
                        "status": "draft",
                        "draft": {"draft_id": "draft-1"},
                        "ai_image_batch": {"batch_id": "batch-1", "item_id": "1"},
                    }
                },
                views.AI_IMAGE_BATCH_SESSION_KEY: {
                    "batch_id": "batch-1",
                    "identity_scope": {"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
                    "items": [{"id": "1", "status": "skipped", "suggestion": {"mode": "transaction"}}],
                },
            }
        )
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "get_session_user", return_value=fake_user),
            patch.object(
                views,
                "_identity_scope_binding",
                return_value={"actor_user_id": 1001, "scope_type": "personal", "family_id": None},
            ),
            patch.object(views, "_commit_draft_once") as commit_once,
        ):
            response = views.transaction_confirm(request)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(json.loads(response.content)["error"]["code"], "batch_item_unavailable")
        commit_once.assert_not_called()

    def test_ai_image_batch_builder_keeps_only_normalized_suggestions(self) -> None:
        parsed = {
            "statement_items": [
                {"type": "expense", "amount": "15.00", "currency": "UAH", "comment": "Coffee"},
                {"type": "expense", "amount": "20.00", "currency": "UAH", "comment": "Taxi"},
            ],
            "currency_explicit": True,
            "dominant_currency": "UAH",
            "skipped_items_count": 1,
        }
        suggestion = {"mode": "transaction", "kind": "expense", "amount": "15.00", "currency": "UAH"}

        with patch.object(services, "build_ai_image_suggestion", return_value=suggestion):
            batch = services.build_ai_image_batch(SimpleNamespace(base_currency="UAH"), parsed, batch_id="batch-1")

        self.assertEqual(batch["item_count"], 2)
        self.assertEqual(batch["skipped_items_count"], 1)
        self.assertEqual(batch["items"][0]["status"], "pending")
        self.assertNotIn("raw_response", batch)

    def test_auth_logout_clears_miniapp_session(self) -> None:
        request = self.factory.post("/app/api/auth/logout")
        request.session = FakeSession(
            {
                "miniapp_tg_user_id": 1001,
                "miniapp_auth_at": 1234567890,
                "other": "keep",
            }
        )

        response = views.auth_logout(request)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(json.loads(response.content)["ok"])
        self.assertNotIn("miniapp_tg_user_id", request.session)
        self.assertNotIn("miniapp_auth_at", request.session)
        self.assertEqual(request.session["other"], "keep")

    def test_auth_dev_returns_serialized_user_for_patched_login(self) -> None:
        request = self.factory.post("/app/api/auth/dev")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, first_name="Ihor", base_currency="UAH")

        with (
            patch.object(views, "dev_login_user", return_value=fake_user),
            patch.object(views, "resolve_access", return_value={"mode": "active"}),
        ):
            response = views.auth_dev(request)

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.content)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["user"]["tg_user_id"], 1001)
        self.assertEqual(payload["access"]["mode"], "active")

    def test_read_limit_clamps_values(self) -> None:
        request = self.factory.get("/app/api/categories", {"limit": "999"})
        self.assertEqual(views._read_limit(request, default=5, maximum=30), 30)

    def test_categories_endpoint_passes_custom_limit(self) -> None:
        request = self.factory.get("/app/api/categories", {"preset": "this_month", "limit": "20"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)
        fake_period = SimpleNamespace(preset="this_month")

        with (
            patch.object(views, "_authorized_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "resolve_period", return_value=fake_period),
            patch.object(views, "build_categories_preview", return_value={"state": {"mode": "full"}, "items": []}) as builder,
        ):
            response = views.categories(request)

        self.assertEqual(response.status_code, 200)
        builder.assert_called_once_with(fake_user, fake_period, limit=20)

    def test_category_catalog_endpoint_uses_read_access(self) -> None:
        request = self.factory.get("/app/api/category-catalog")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_category_catalog", return_value={"mode": "view_only", "categories": {}}) as builder,
        ):
            response = views.category_catalog(request)

        self.assertEqual(response.status_code, 200)
        builder.assert_called_once_with(fake_user)

    def test_export_xlsx_uses_current_period_and_returns_download(self) -> None:
        request = self.factory.get("/app/api/export.xlsx", {"preset": "this_month"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, lang="uk")
        period = PeriodSelection(
            preset="this_month",
            date_from=date(2026, 7, 1),
            date_to=date(2026, 7, 14),
            label="Цей місяць",
        )

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "resolve_period", return_value=period),
            patch.object(views, "request_export_xlsx", return_value=b"xlsx-bytes") as export_builder,
        ):
            response = views.export_xlsx(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"xlsx-bytes")
        self.assertEqual(response["Content-Type"], "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        self.assertIn('filename="my-cash-flow-this_month.xlsx"', response["Content-Disposition"])
        export_builder.assert_called_once_with(
            telegram_user_id=1001,
            date_from=date(2026, 7, 1),
            date_to_exclusive=date(2026, 7, 15),
            title="Цей місяць",
        )

    def test_export_csv_uses_current_period_and_returns_download(self) -> None:
        request = self.factory.get("/app/api/export.csv", {"preset": "this_month"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, lang="uk")
        period = PeriodSelection(
            preset="this_month",
            date_from=date(2026, 7, 1),
            date_to=date(2026, 7, 14),
            label="This month",
        )

        with (
            patch.object(views, "_authorized_write_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "resolve_period", return_value=period),
            patch.object(views, "request_export_csv", return_value=b"csv-bytes") as export_builder,
        ):
            response = views.export_csv(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"csv-bytes")
        self.assertEqual(response["Content-Type"], "text/csv; charset=utf-8")
        self.assertIn('filename="my-cash-flow-this_month.csv"', response["Content-Disposition"])
        export_builder.assert_called_once_with(
            telegram_user_id=1001,
            date_from=date(2026, 7, 1),
            date_to_exclusive=date(2026, 7, 15),
            title="This month",
        )

    def test_accounts_endpoint_passes_custom_limit(self) -> None:
        request = self.factory.get("/app/api/accounts", {"limit": "25"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_accounts_preview", return_value={"state": {"mode": "full"}, "items": []}) as builder,
        ):
            response = views.accounts(request)

        self.assertEqual(response.status_code, 200)
        builder.assert_called_once_with(fake_user, limit=25)

    def test_credit_cards_endpoint_passes_custom_limit(self) -> None:
        request = self.factory.get("/app/api/credit-cards", {"limit": "12"})
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)

        with (
            patch.object(views, "_authorized_user_or_blocked", return_value=(fake_user, {"mode": "active"})),
            patch.object(views, "build_credit_cards_preview", return_value={"state": {"mode": "full"}, "items": []}) as builder,
        ):
            response = views.credit_cards(request)

        self.assertEqual(response.status_code, 200)
        builder.assert_called_once_with(fake_user, limit=12)

    def test_access_blocked_response_uses_human_message(self) -> None:
        response = views._access_blocked_response({"mode": "blocked", "recommended_action": "start_trial"})

        self.assertEqual(response.status_code, 403)
        payload = json.loads(response.content)
        self.assertEqual(payload["error"]["code"], "access_blocked")
        self.assertEqual(payload["error"]["message"], "Доступ до кабінету ще не активовано.")
        self.assertEqual(payload["access"]["recommended_action"], "start_trial")

    @override_settings(DEBUG=True)
    def test_index_renders_single_template_on_app_route(self) -> None:
        request = self.factory.get("/app/")
        request.session = FakeSession()

        with patch.object(views, "render", return_value=HttpResponse("ok")) as render_mock:
            response = views.index(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(render_mock.call_args.args[1], views.MINIAPP_TEMPLATE)
        self.assertEqual(render_mock.call_args.args[2]["miniapp_asset_version"], views.MINIAPP_ASSET_VERSION)
        self.assertEqual(
            render_mock.call_args.args[2]["browser_login_recovery_url"],
            "https://t.me/vydnocapital_bot?start=app_login",
        )

    @override_settings(DEBUG=True)
    def test_index_exposes_standalone_oidc_recovery_with_bot_fallback(self) -> None:
        request = self.factory.get("/app/")
        request.session = FakeSession()

        response = views.index(request)
        body = response.content.decode("utf-8")

        self.assertIn('id="telegramOidcRecovery"', body)
        self.assertIn('/app/auth/telegram/start', body)
        self.assertIn('id="browserLoginRecovery"', body)
        self.assertIn("browser_login_required", body)
        self.assertIn("https://t.me/vydnocapital_bot?start=app_login", body)
        self.assertIn("Перевстановлювати Vydno не потрібно", body)

    @override_settings(DEBUG=True)
    def test_index_ignores_stale_v_query_and_renders_same_template(self) -> None:
        request = self.factory.get("/app/", {"v": "stale-shell"})
        request.session = FakeSession()

        with patch.object(views, "render", return_value=HttpResponse("ok")) as render_mock:
            response = views.index(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(render_mock.call_args.args[1], views.MINIAPP_TEMPLATE)

    @override_settings(DEBUG=True)
    def test_index_sets_no_cache_headers(self) -> None:
        request = self.factory.get("/app/", {"preview": "1"})
        request.session = FakeSession()

        response = views.index(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Cache-Control"], "no-store, no-cache, must-revalidate, max-age=0, private")
        self.assertEqual(response["Pragma"], "no-cache")
        self.assertEqual(response["Expires"], "0")

    def test_manifest_returns_installable_app_metadata(self) -> None:
        request = self.factory.get("/app/manifest.webmanifest")

        response = views.manifest(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/manifest+json")
        payload = json.loads(response.content)
        self.assertEqual(payload["start_url"], "/app/?source=pwa")
        self.assertEqual(payload["scope"], "/app/")
        self.assertEqual(payload["display"], "standalone")
        self.assertEqual(payload["theme_color"], views.MINIAPP_THEME_COLOR)
        self.assertEqual(payload["icons"][0]["sizes"], "192x192")
        self.assertEqual(payload["icons"][1]["sizes"], "512x512")
        self.assertEqual(payload["icons"][0]["purpose"], "any")
        self.assertEqual(payload["icons"][1]["purpose"], "any")
        self.assertEqual(payload["icons"][2]["sizes"], "192x192")
        self.assertEqual(payload["icons"][2]["purpose"], "maskable")
        self.assertEqual(payload["icons"][3]["sizes"], "512x512")
        self.assertEqual(payload["icons"][3]["purpose"], "maskable")

    def test_service_worker_skips_api_cache_and_serves_shell_scope(self) -> None:
        request = self.factory.get("/app/sw.js")

        response = views.service_worker(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "application/javascript; charset=utf-8")
        body = response.content.decode("utf-8")
        self.assertIn('const CACHE_NAME = "vydno-miniapp-shell-', body)
        self.assertIn('"/app/"', body)
        self.assertIn('const APP_SHELL_URL = "/app/"', body)
        self.assertIn('url.pathname.startsWith("/app/api/")', body)
        self.assertIn('url.pathname.startsWith("/static/miniapp/")', body)
        self.assertIn(
            'const isAppNavigation = event.request.mode === "navigate" && ["/app/", "/app"].includes(url.pathname)',
            body,
        )
        self.assertEqual(response["Cache-Control"], "no-cache, max-age=0")

    @override_settings(
        TELEGRAM_BOT_TOKEN="123456:test-miniapp-token",
        MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS=900,
    )
    def test_browser_login_token_round_trips_without_query_user_identity(self) -> None:
        token = auth.build_browser_login_token(1001, now=1_700_000_000, nonce="n" * 18)

        identity = auth.validate_browser_login_token(token, now=1_700_000_120)

        self.assertEqual(identity.tg_user_id, 1001)
        self.assertEqual(identity.expires_at, 1_700_000_900)
        self.assertNotIn("tg_user_id=", token)

    @override_settings(
        TELEGRAM_BOT_TOKEN="123456:test-miniapp-token",
        MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS=900,
    )
    def test_browser_login_token_rejects_tampering_and_expiry(self) -> None:
        token = auth.build_browser_login_token(1001, now=1_700_000_000, nonce="n" * 18)

        with self.assertRaises(auth.MiniAppAuthError):
            auth.validate_browser_login_token(f"{token}x", now=1_700_000_120)
        with self.assertRaises(auth.MiniAppAuthError):
            auth.validate_browser_login_token("токен.invalid", now=1_700_000_120)
        with self.assertRaises(auth.MiniAppAuthError):
            auth.validate_browser_login_token(token, now=1_700_000_901)

    @override_settings(MINIAPP_BROWSER_SESSION_AGE_SECONDS=123456)
    def test_browser_login_view_consumes_token_and_sets_long_session(self) -> None:
        request = self.factory.get("/app/login/raw-token/")
        request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001, first_name="Ihor", base_currency="UAH")
        fake_identity = SimpleNamespace(issued_at=1_700_000_000)

        with (
            patch.object(
                views,
                "consume_browser_login_token_with_identity",
                return_value=(fake_user, fake_identity),
            ) as consume_mock,
            patch.object(views, "login_session") as login_mock,
        ):
            response = views.browser_login(request, "raw-token")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/app/")
        consume_mock.assert_called_once_with("raw-token")
        login_mock.assert_called_once_with(
            request,
            fake_user,
            session_age_seconds=123456,
            auth_mode=auth.BROWSER_AUTH_MODE,
            authenticated_at=fake_identity.issued_at,
        )

    def test_browser_login_view_returns_clear_error_for_invalid_link(self) -> None:
        request = self.factory.get("/app/login/raw-token/")
        request.session = FakeSession()

        with patch.object(
            views,
            "consume_browser_login_token_with_identity",
            side_effect=auth.MiniAppAuthError("Browser login link already used."),
        ):
            response = views.browser_login(request, "raw-token")

        self.assertEqual(response.status_code, 400)
        self.assertIn("already used", response.content.decode("utf-8"))
        self.assertIn("Отримати нове посилання", response.content.decode("utf-8"))
        self.assertIn("https://t.me/vydnocapital_bot?start=app_login", response.content.decode("utf-8"))
        self.assertEqual(response["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(response["Cache-Control"], "no-store, no-cache, must-revalidate, max-age=0, private")

    def test_browser_login_handoff_does_not_consume_token(self) -> None:
        request = self.factory.get(
            "/app/browser-login/raw-token/",
            HTTP_USER_AGENT=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
                "AppleWebKit/605.1.15 Mobile/15E148 Telegram"
            ),
        )

        with patch.object(views, "consume_browser_login_token_with_identity") as consume_mock:
            response = views.browser_login_handoff(request, "raw-token")

        self.assertEqual(response.status_code, 200)
        body = response.content.decode("utf-8")
        handoff_copy = body.split("<main>", 1)[1].split("</main>", 1)[0]
        self.assertIn("/app/api/browser-login/raw-token/", body)
        self.assertNotIn('href="/app/login/raw-token/"', body)
        self.assertIn("Safari", handoff_copy)
        self.assertNotIn("Chrome", handoff_copy)
        self.assertIn("натисни компас", handoff_copy)
        self.assertIn("окрем", handoff_copy)
        self.assertNotIn("Так сесія збережеться для іконки", handoff_copy)
        self.assertIn("isKnownEmbedded", body)
        self.assertIn("embedded-browser", body)
        self.assertIn("ios-embedded-browser", body)
        self.assertIn('class="browser-login-action"', body)
        consume_mock.assert_not_called()
        self.assertEqual(response["Cache-Control"], "no-store, no-cache, must-revalidate, max-age=0, private")

    def test_browser_login_handoff_non_ios_copy_is_chrome_oriented(self) -> None:
        request = self.factory.get(
            "/app/browser-login/raw-token/",
            HTTP_USER_AGENT=(
                "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 "
                "Chrome/128.0 Mobile Safari/537.36 Telegram"
            ),
        )

        response = views.browser_login_handoff(request, "raw-token")

        handoff_copy = response.content.decode("utf-8").split("<main>", 1)[1].split("</main>", 1)[0]
        self.assertIn("Chrome", handoff_copy)
        self.assertNotIn("Safari", handoff_copy)
        self.assertIn("меню ⋮", handoff_copy)

    @override_settings(MINIAPP_BROWSER_SESSION_AGE_SECONDS=123456)
    def test_install_handoff_preserves_install_intent_through_browser_login(self) -> None:
        handoff_request = self.factory.get("/app/browser-login/raw-token/?install=1")
        handoff = views.browser_login_handoff(handoff_request, "raw-token")
        self.assertIn("/app/api/browser-login/raw-token/?install=1", handoff.content.decode("utf-8"))

        login_request = self.factory.get("/app/login/raw-token/?install=1")
        login_request.session = FakeSession()
        fake_user = SimpleNamespace(tg_user_id=1001)
        fake_identity = SimpleNamespace(issued_at=1_700_000_000)
        with (
            patch.object(
                views,
                "consume_browser_login_token_with_identity",
                return_value=(fake_user, fake_identity),
            ),
            patch.object(views, "login_session"),
        ):
            response = views.browser_login(login_request, "raw-token")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/app/?install=1")

    @override_settings(
        ALLOWED_HOSTS=["testserver"],
        MINIAPP_BROWSER_SESSION_AGE_SECONDS=123456,
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
    )
    def test_service_worker_bypass_login_route_sets_persistent_browser_session(self) -> None:
        client = Client()
        fake_user = SimpleNamespace(tg_user_id=1001, first_name="Ihor", base_currency="UAH")
        fake_identity = SimpleNamespace(issued_at=1_700_000_000)

        with (
            patch.object(
                views,
                "consume_browser_login_token_with_identity",
                return_value=(fake_user, fake_identity),
            ) as consume_mock,
            patch.object(auth.UserAuthSession.objects, "create"),
        ):
            response = client.get("/app/api/browser-login/raw-token/", secure=True)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/app/")
        self.assertIn(settings.SESSION_COOKIE_NAME, response.cookies)
        self.assertEqual(client.session[auth.SESSION_USER_ID_KEY], 1001)
        self.assertEqual(client.session[auth.SESSION_AUTH_MODE_KEY], auth.BROWSER_AUTH_MODE)
        consume_mock.assert_called_once_with("raw-token")


class MiniAppFlowBucketUnitTests(unittest.TestCase):
    def test_bucket_mode_keeps_month_length_periods_on_daily_granularity(self) -> None:
        period = PeriodSelection(
            preset="this_month",
            date_from=date(2026, 5, 1),
            date_to=date(2026, 5, 25),
            label="Цей місяць",
        )

        self.assertEqual(_bucket_mode(period), "day")

    def test_bucket_dates_fill_quiet_days_inside_daily_period(self) -> None:
        period = PeriodSelection(
            preset="custom",
            date_from=date(2026, 5, 1),
            date_to=date(2026, 5, 4),
            label="01.05 - 04.05",
        )

        self.assertEqual(
            _bucket_dates_for_period(period, "day"),
            [
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 3),
                date(2026, 5, 4),
            ],
        )


@override_settings(
    DEBUG=True,
    ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost"],
    SECURE_SSL_REDIRECT=False,
)
class MiniAppShellTemplateUnitTests(SimpleTestCase):
    def setUp(self) -> None:
        self.client = Client()

    def test_new_shell_renders_dashboard_v2_shell_without_legacy_bundle(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-default-period-label="Цей місяць"')
        self.assertContains(response, 'class="app"')
        self.assertContains(response, 'rel="manifest" href="/app/manifest.webmanifest')
        self.assertContains(response, 'rel="icon" type="image/png" sizes="32x32" href="/static/miniapp/favicon-32.png')
        self.assertContains(response, 'rel="apple-touch-icon" href="/static/miniapp/apple-touch-icon.png')
        self.assertContains(response, 'src="/static/miniapp/brand-icon.png')
        self.assertContains(response, 'name="theme-color"')
        self.assertContains(response, 'name="apple-mobile-web-app-capable" content="yes"')
        self.assertContains(response, f'data-service-worker-url="/app/sw.js?v={views.MINIAPP_ASSET_VERSION}"')
        self.assertContains(response, 'class="calm-fintech-v2"')
        self.assertContains(response, 'data-categories-url="/app/api/categories"')
        self.assertContains(response, 'data-category-catalog-url="/app/api/category-catalog"')
        self.assertContains(response, 'data-activity-url="/app/api/activity"')
        self.assertContains(response, 'data-export-xlsx-url="/app/api/export.xlsx"')
        self.assertContains(response, 'data-transaction-options-url="/app/api/transactions/options"')
        self.assertContains(response, 'data-transaction-draft-url="/app/api/transactions/draft"')
        self.assertContains(response, 'data-transaction-confirm-url="/app/api/transactions/confirm"')
        self.assertContains(response, 'data-transaction-cancel-url="/app/api/transactions/cancel"')
        self.assertContains(response, 'data-ai-text-parse-url="/app/api/ai-text/parse"')
        self.assertContains(response, 'data-ai-voice-parse-url="/app/api/ai-voice/parse"')
        self.assertContains(response, 'data-account-options-url="/app/api/accounts/options"')
        self.assertContains(response, 'data-account-draft-url="/app/api/accounts/draft"')
        self.assertContains(response, 'data-account-confirm-url="/app/api/accounts/confirm"')
        self.assertContains(response, 'data-transfer-options-url="/app/api/transfers/options"')
        self.assertContains(response, 'data-transfer-confirm-url="/app/api/transfers/confirm"')
        self.assertContains(response, 'data-debt-options-url="/app/api/debts/options"')
        self.assertContains(response, 'data-debt-draft-url="/app/api/debts/draft"')
        self.assertContains(response, 'data-debt-confirm-url="/app/api/debts/confirm"')
        self.assertContains(response, 'data-debt-cancel-url="/app/api/debts/cancel"')
        self.assertContains(response, 'data-settings-url="/app/api/settings"')
        self.assertContains(response, 'data-settings-profile-url="/app/api/settings/profile"')
        self.assertContains(response, 'data-settings-reminder-url="/app/api/settings/reminder"')
        self.assertContains(response, 'data-settings-savings-url="/app/api/settings/savings"')
        self.assertContains(response, 'data-billing-action-url="/app/api/settings/billing"')
        self.assertContains(response, 'data-install-nudge-url="/app/api/install-nudge"')
        self.assertContains(response, 'data-install-nudge-event-url="/app/api/install-nudge/event"')
        self.assertContains(response, 'data-billing-expense-options-url="/app/api/billing-expense/options"')
        self.assertContains(response, 'data-billing-expense-confirm-url="/app/api/billing-expense/confirm"')
        self.assertContains(response, 'data-help-url="/app/api/help"')
        self.assertContains(response, 'id="installAppBtn"')
        self.assertContains(response, 'id="cardBindingModal"')
        self.assertContains(response, 'id="installNudgeModal"')
        self.assertContains(response, 'function showCardBindingNudge(settingsPayload)')
        self.assertContains(response, 'function maybeShowInstallNudge(force)')
        self.assertContains(response, 'id="settingsBtn"')
        self.assertContains(response, "navigator.serviceWorker.register(serviceWorkerUrl, {")
        self.assertContains(response, 'scope: "/app/"')
        self.assertContains(response, 'data-screen="overview"')
        self.assertContains(response, 'data-screen="add"')
        self.assertContains(response, 'data-screen="money"')
        self.assertContains(response, 'data-screen="analytics"')
        self.assertContains(response, 'data-screen="status"')
        self.assertContains(response, 'data-screen="settings"')
        self.assertContains(response, 'id="transactionForm"')
        self.assertContains(response, 'id="aiIntakeCard"')
        self.assertContains(response, 'class="ai-intake-secondary-grid"')
        self.assertContains(response, 'id="aiTextForm"')
        self.assertContains(response, 'id="aiTextLaunchBtn"')
        self.assertContains(response, 'id="aiVoiceRecordBtn"')
        self.assertContains(response, 'id="aiVoiceFeedback"')
        self.assertContains(response, 'function setupAiIntakeChooser()')
        self.assertContains(response, 'function setupAiVoice()')
        self.assertContains(response, 'function startAiVoiceRecording()')
        self.assertContains(response, 'function stopAiVoiceRecording()')
        self.assertContains(response, 'data-ai-image-parse-url')
        self.assertContains(response, 'data-ai-image-batch-url')
        self.assertContains(response, 'data-ai-image-batch-item-url')
        self.assertContains(response, 'id="aiImageForm"')
        self.assertContains(response, 'id="aiImageLaunchBtn"')
        self.assertContains(response, 'id="aiImageInput"')
        self.assertContains(
            response,
            'accept="image/jpeg,image/png,image/webp,image/gif,image/heic,image/heif,.heic,.heif"',
        )
        self.assertNotContains(response, 'capture="environment"')
        self.assertContains(response, 'id="aiImageBatchCard"')
        self.assertContains(response, 'Операції з банківського скріншота')
        self.assertContains(response, 'id="aiImageBatchAccount"')
        self.assertContains(response, 'function parseAiImage(event)')
        self.assertContains(response, 'function reviewAiImageBatchItem(itemId)')
        self.assertContains(response, 'function parseAiText(event)')
        self.assertContains(response, 'id="txDraftCard"')
        self.assertContains(response, 'class="tx-confirmation-panel"')
        self.assertNotContains(response, 'tx-confirmation-sheet')
        self.assertContains(response, 'id="txConfirmationStatus"')
        self.assertNotContains(response, 'id="txPreviewBtn"')
        self.assertContains(response, 'function renderTransactionConfirmation(reveal)')
        self.assertContains(response, 'el.draftCard.scrollIntoView({ behavior: "smooth", block: "center" })')
        self.assertContains(response, 'function confirmTransactionDraft()')
        self.assertContains(response, 'postJson(transactionDraftUrl, payload)')
        self.assertNotContains(response, 'function submitTransactionDraft(event)')
        self.assertContains(response, 'id="accountForm"')
        self.assertContains(response, 'id="transferForm"')
        self.assertContains(response, 'id="accountDraftCard"')
        self.assertContains(response, 'id="transferDraftCard"')
        self.assertContains(response, 'id="debtForm"')
        self.assertContains(response, 'id="debtDraftCard"')
        self.assertContains(response, 'id="moneyDebtList"')
        self.assertContains(response, 'id="moneyGoalsList"')
        self.assertContains(response, 'id="analyticsCategoryList"')
        self.assertContains(response, 'id="analyticsCatalogList"')
        self.assertContains(response, 'id="analyticsActivityList"')
        self.assertContains(response, 'id="exportXlsxBtn"')
        self.assertContains(response, 'id="settingsProfileForm"')
        self.assertContains(response, 'id="settingsReminderForm"')
        self.assertContains(response, 'id="settingsSavingsForm"')
        self.assertContains(response, 'id="billingBindBtn"')
        self.assertContains(response, 'id="billingCancelConfirmBtn"')
        self.assertContains(response, 'id="billingExpenseForm"')
        self.assertContains(response, 'id="billingExpenseConfirmBtn"')
        self.assertContains(response, 'id="settingsHelpTopicsBtn"')
        self.assertContains(response, 'id="settingsHelpContent"')
        self.assertContains(response, 'data-money-mode="accounts"')
        self.assertContains(response, 'data-money-mode="debts"')
        self.assertContains(response, 'data-money-mode="goals"')
        self.assertContains(response, 'data-target="add"')
        self.assertContains(response, 'data-target="money"')
        self.assertContains(response, 'class="nav-btn nav-btn--add"')
        shell = response.content.decode("utf-8")
        self.assertLess(shell.index('data-target="money"'), shell.index('data-target="add"'))
        self.assertLess(shell.index('data-target="add"'), shell.index('data-target="analytics"'))
        self.assertRegex(shell, r'(?s)data-target="money".*?<span>Гроші</span>.*?data-target="add".*?<span>Додати</span>')
        self.assertContains(response, 'navLabels[button.dataset.target]')
        self.assertNotContains(response, '["Огляд", "Додати", "Гроші", "Аналітика", "Статус"]')
        self.assertContains(response, 'function setupMoneyForms()')
        self.assertContains(response, 'function loadAnalyticsDetails(force)')
        self.assertContains(response, 'function setupSettingsForms()')
        self.assertContains(response, 'function showBillingGate(access)')
        self.assertContains(response, 'function loadHelpContent(topicId)')
        self.assertContains(response, 'function loadBillingExpenseOptions()')
        self.assertContains(response, 'class="bottom-nav"')
        self.assertContains(response, f"miniapp/app.css?v={views.MINIAPP_ASSET_VERSION}")
        self.assertContains(response, 'class="period-inline"')
        self.assertContains(response, 'data-period-trigger')
        self.assertContains(response, 'data-period-dropdown')
        self.assertContains(response, 'type="date" data-date-from', count=1)
        self.assertContains(response, 'type="date" data-date-to', count=1)
        self.assertContains(response, 'class="period-option" type="button" data-period="today"', count=1)
        self.assertContains(response, 'class="period-option" type="button" data-period="last_30"', count=1)
        self.assertContains(response, 'class="period-option is-active" type="button" data-period="month"', count=1)
        self.assertNotContains(response, 'id="insightList"')
        self.assertNotContains(response, '`r`n')
        self.assertNotContains(response, 'class="period-card"')
        self.assertNotContains(response, "miniapp/app_legacy.css")

    def test_shell_renders_calm_fintech_overview_hierarchy(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="screen is-active calm-overview"', count=1)
        self.assertContains(response, 'class="card hero-card card-pad calm-balance-card"', count=1)
        self.assertContains(response, 'class="grid-2 calm-kpi-grid"', count=1)
        self.assertContains(response, 'class="calm-balance-meta"', count=1)
        self.assertContains(response, 'class="card card-pad calm-activity-card"', count=1)
        self.assertContains(response, 'class="card card-pad calm-summary-card"', count=1)
        self.assertContains(response, 'stroke="currentColor" stroke-width="2.2"')
        self.assertNotContains(response, "Mini App API")

        shell = response.content.decode("utf-8")
        self.assertLess(shell.index("calm-balance-card"), shell.index("calm-kpi-grid"))
        self.assertLess(shell.index("calm-kpi-grid"), shell.index("calm-activity-card"))
        self.assertLess(shell.index("calm-activity-card"), shell.index("calm-summary-card"))

        css_path = os.path.join(os.path.dirname(__file__), "static", "miniapp", "app.css")
        with open(css_path, encoding="utf-8") as css_file:
            css = css_file.read()
        self.assertIn("/* Calm Fintech V2: approved prototype translated to production. */", css)
        self.assertIn(".calm-balance-meta", css)
        self.assertIn(".calm-summary-card .month-summary", css)
        self.assertIn("border-radius: 22px;", css)
        self.assertIn("--canvas: #f4f6f1;", css)
        self.assertIn(".calm-fintech-v2 .bottom-nav", css)

    def test_shell_uses_mobile_safe_header_and_navigation_frame(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="topbar-main"', count=1)
        self.assertContains(response, 'class="topbar-utilities"', count=1)
        self.assertContains(response, 'class="topbar-context"', count=1)
        self.assertContains(response, 'id="topbarContext"', count=1)
        self.assertContains(response, 'topbarContext.hidden = !["overview", "analytics"].includes(target)')
        self.assertContains(response, 'aria-controls="periodDropdown"', count=1)
        self.assertContains(response, 'id="periodDropdown"', count=1)
        self.assertContains(response, 'aria-current="page"', count=1)
        self.assertContains(response, 'button.setAttribute("aria-current", "page")')

        css_path = os.path.join(os.path.dirname(__file__), "static", "miniapp", "app.css")
        with open(css_path, encoding="utf-8") as css_file:
            css = css_file.read()
        self.assertIn("--safe-top:", css)
        self.assertIn("--safe-left:", css)
        self.assertIn("--safe-right:", css)
        self.assertIn("--nav-block-size:", css)
        self.assertIn("min-height: 100dvh;", css)
        self.assertIn("bottom: max(8px, var(--safe-bottom));", css)
        self.assertIn("@media (max-width: 520px)", css)
        self.assertIn("grid-template-columns: minmax(0, 1fr) 44px;", css)
        self.assertIn("min-height: 44px;", css)
        self.assertIn("#debtCaption", css)
        self.assertIn(".topbar-context[hidden]", css)
        self.assertIn("@media (prefers-reduced-motion: reduce)", css)

    def test_shell_mobile_controls_keep_minimum_touch_target_height(self) -> None:
        css_path = os.path.join(os.path.dirname(__file__), "static", "miniapp", "app.css")
        with open(css_path, encoding="utf-8") as css_file:
            css = css_file.read()

        for selector in (
            ".period-trigger",
            ".period-option",
            ".icon-btn",
            ".date-field input",
            ".segment-btn",
            ".money-tab",
            ".secondary-action",
            ".family-member-remove",
            ".onboarding-icon-button",
        ):
            selector_block = css.split(f"{selector} {{", 1)[1].split("}", 1)[0]
            min_height = int(selector_block.split("min-height:", 1)[1].split("px", 1)[0].strip())
            self.assertGreaterEqual(min_height, 44, selector)

    def test_shell_static_settings_heading_matches_default_uk_locale_before_js(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '<p class="eyebrow" id="settingsEyebrow">Налаштування</p>', html=True)
        self.assertContains(response, '<h1 id="settingsTitle">Профіль і підписка</h1>', html=True)
        self.assertContains(
            response,
            '<p id="settingsSubtitle">Мова, валюта, нагадування та доступ до сервісу.</p>',
            html=True,
        )

    def test_shell_forces_fresh_service_worker_script_and_update(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'updateViaCache: "none"')
        self.assertContains(response, "return registration.update();")

    def test_shell_defaults_to_this_month_and_keeps_separate_rolling_30_days_option(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'const defaultPeriodLabel = root.dataset.defaultPeriodLabel || "Цей місяць";')
        self.assertContains(response, 'if (dashboardPeriodState.period === "last_30") {')
        self.assertContains(response, 'return { preset: "last_30" };')
        self.assertContains(response, 'if (dashboardPeriodState.period === "month") {')
        self.assertContains(response, 'return { preset: "this_month" };')
        self.assertContains(response, 'start.setDate(now.getDate() - 29);')
        self.assertContains(response, 'start.setDate(1);')

    def test_shell_keeps_custom_period_errors_local_to_selector(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '<div class="form-status error" data-period-error hidden', count=1)
        self.assertContains(response, 'setDashboardPeriodError(tr("periodInvalidRange"))')
        self.assertContains(response, 'if (error.code === "invalid_period")')

    def test_shell_supports_custom_account_currency_and_stable_form_hydration(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="accountCustomCurrency"', count=1)
        self.assertContains(response, '{ id: "__custom__", label: tr("moneyCustomCurrency") }')
        self.assertContains(response, "function hydrateAccountFormFromTarget()")
        self.assertNotContains(response, 'if (action === "rename" && selected && el.accountLabel) el.accountLabel.value')

    def test_shell_preserves_add_options_and_requires_cross_currency_rate(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "function syncTransferValidation()")
        self.assertContains(response, "el.transferRate.required = needsRate")
        self.assertContains(response, 'if (error.code === "fx_rate_required" || error.code === "invalid_fx_rate")')
        self.assertContains(response, "function invalidateAccountOptions()")

    def test_shell_uses_raw_month_chart_axis_formatter(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "function formatChartAxisValue(value)")
        self.assertContains(response, "function chartSeriesMax(values, extractor)")
        self.assertContains(response, "function scaledChartHeight(value, maxValue, options)")
        self.assertContains(response, 'formatChartAxisValue(seriesMax * step)')
        self.assertContains(response, 'income: Math.max(0, Number(bucket.income) || 0)')
        self.assertContains(response, 'expense: Math.max(0, Number(bucket.expense) || 0)')
        self.assertContains(response, 'scaledChartHeight(incomeValue, seriesMax, { minVisible: 8, maxHeight: 64, zeroHeight: 0 })')
        self.assertContains(response, 'scaledChartHeight(expenseValue, seriesMax, { minVisible: 8, maxHeight: 64, zeroHeight: 0 })')
        self.assertContains(response, 'scaledChartHeight(incomeValue, seriesMax, { minVisible: 6, maxHeight: 100, zeroHeight: 0 })')
        self.assertNotContains(response, 'return Number(bucket.net || 0);')
        self.assertNotContains(response, 'return "<span>" + value + "K</span>";')
        self.assertNotContains(response, 'Math.max(8, Math.min(64, absoluteValue))')

    def test_shell_uses_bucket_net_values_for_hero_chart(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "function monthBarNetValue(value)")
        self.assertContains(response, "accumulator.push(previous + monthBarNetValue(value));")
        self.assertNotContains(response, "accumulator.push(previous + Number(value || 0));")

    def test_shell_shows_bootstrap_before_loading_secondary_month_series(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        shell = response.content.decode("utf-8")
        load_start = shell.index("function loadLiveDashboard()")
        load_end = shell.index("function bootPreview()", load_start)
        load_shell = shell[load_start:load_end]

        render_marker = "renderData(mapBootstrapToDashboardData(bootstrap, []));"
        show_marker = "showDashboard();"
        background_marker = "fetchMonthSeries(bootstrap.period || {})"
        self.assertIn("const loadId = ++dashboardLoadId;", load_shell)
        self.assertLess(load_shell.index(render_marker), load_shell.index(background_marker))
        self.assertLess(load_shell.index(show_marker), load_shell.index(background_marker))
        self.assertIn("if (loadId !== dashboardLoadId) return;", load_shell)
        self.assertIn("}).catch(function () {});", load_shell)

    def test_shell_reuses_session_check_during_initial_boot(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "let sessionPromise = null;")
        self.assertContains(response, "if (sessionPromise) return sessionPromise;")
        self.assertContains(response, "sessionPromise = null;")

    def test_shell_uses_flow_comparison_for_income_change_and_zero_safe_signs(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "incomeChange: percentLabel(flow.income_vs_previous_percent, 1),")
        self.assertContains(response, 'if (!/[1-9]/.test(body)) return estimatePrefix + body;')
        self.assertContains(response, 'if (incomeAmountNode) incomeAmountNode.className = "amount " + incomeTone;')
        self.assertContains(response, 'if (expenseAmountNode) expenseAmountNode.className = "amount " + expenseTone;')

    def test_status_shell_uses_credit_mode_labels_and_explicit_ids(self) -> None:
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'id="statusCreditHeading"', count=1)
        self.assertContains(response, 'id="statusCreditSubtitle"', count=1)
        self.assertContains(response, 'id="statusDebtsHeading"', count=1)
        self.assertContains(response, 'id="statusDebtsSubtitle"', count=1)
        self.assertContains(response, 'id="debtAmountLabel"', count=1)
        self.assertContains(response, 'id="activeCardsLabel"', count=1)
        self.assertContains(response, 'id="owedToUserLabel"', count=1)
        self.assertContains(response, 'id="owedByUserLabel"', count=1)
        self.assertContains(response, "Кредитний режим")
        self.assertContains(response, "Борги")
        self.assertContains(response, "Активні рахунки")
        self.assertContains(response, "Мені винні")
        self.assertContains(response, "Я винен")
        self.assertContains(response, "debtCaptionActive")
        self.assertContains(response, "debtsCaptionActive")
        self.assertNotContains(response, "Кредитних карток поки немає.")
        self.assertContains(response, "const currentDebtPayload = overview.current_debt ||")
        self.assertContains(response, 'setText("heroDebtValue", data.heroDebtAmount || data.debtAmount);')
        self.assertContains(response, 'debtAmount: moneyDisplay(creditDebtPayload, "0 грн"),')


class MiniAppPushUnitTests(SimpleTestCase):
    def test_fallback_vapid_private_key_uses_py_vapid_raw_scalar_format(self) -> None:
        encoded = settings.WEB_PUSH_VAPID_PRIVATE_KEY
        padding = "=" * ((4 - len(encoded) % 4) % 4)
        raw = base64.urlsafe_b64decode(encoded + padding)

        self.assertEqual(len(raw), 32)
        self.assertNotIn("BEGIN", encoded)

    def test_manual_test_notification_bypasses_quiet_hours(self) -> None:
        self.assertTrue(push.EVENTS["test"].critical)

    def test_event_catalog_contains_every_launched_notification_family(self) -> None:
        expected = {
            "daily_expense_missing",
            "saving_due",
            "debt_due_soon",
            "debt_due_today",
            "debt_overdue",
            "trial_ending",
            "billing_action_required",
            "billing_failed",
            "grace_ending",
            "access_blocked",
            "billing_paid_expense",
            "family_member_joined",
            "family_member_left",
            "family_access_removed",
            "weekly_summary",
        }
        self.assertTrue(expected.issubset(push.EVENTS))

    def test_service_worker_handles_push_click_and_app_badge(self) -> None:
        response = views.service_worker(RequestFactory().get("/app/sw.js"))
        script = response.content.decode("utf-8")
        self.assertIn('addEventListener("push"', script)
        self.assertIn('addEventListener("notificationclick"', script)
        self.assertIn("setAppBadge", script)


if __name__ == "__main__":
    unittest.main()
