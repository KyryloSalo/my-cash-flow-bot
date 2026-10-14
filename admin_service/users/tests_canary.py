from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import RequestFactory, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from audit_log.models import AdminAuditLog
from common.test_helpers import ensure_telegram_user_table
from users.canary import (
    CanaryAuthorizationError,
    assert_canary_session,
    consume_canary_capability,
    is_canary_user,
    issue_canary_capability,
)
from users.models import CanaryCapability, TelegramUser, UserAdminState, UserAuthSession
from users.services import disable_production_canary, enable_production_canary


@override_settings(
    PRODUCTION_CANARY_ENABLED=True,
    PRODUCTION_CANARY_TELEGRAM_IDS=[92001],
    PRODUCTION_CANARY_ACTION_TTL_SECONDS=300,
)
class CanaryAuthorizationTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.user = TelegramUser.objects.create(
            tg_user_id=92001,
            first_name="Canary",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.admin = get_user_model().objects.create_superuser(
            username="canary-admin", email="canary@example.com", password="test-password"
        )
        self.state = UserAdminState.objects.create(
            telegram_user=self.user,
            status=UserAdminState.Status.ACTIVE,
            is_test_user=True,
            is_production_canary=True,
            canary_enabled_at=timezone.now(),
            canary_enabled_by=self.admin,
        )

    def test_canary_requires_every_server_side_gate(self):
        self.assertTrue(is_canary_user(self.user))

        with override_settings(PRODUCTION_CANARY_ENABLED=False):
            self.assertFalse(is_canary_user(self.user))
        with override_settings(PRODUCTION_CANARY_TELEGRAM_IDS=[]):
            self.assertFalse(is_canary_user(self.user))

        self.state.is_test_user = False
        self.state.save(update_fields=["is_test_user"])
        self.assertFalse(is_canary_user(self.user))
        self.state.is_test_user = True
        self.state.is_production_canary = False
        self.state.save(update_fields=["is_test_user", "is_production_canary"])
        self.assertFalse(is_canary_user(self.user))
        self.state.is_production_canary = True
        self.state.status = UserAdminState.Status.BANNED
        self.state.save(update_fields=["is_production_canary", "status"])
        self.assertFalse(is_canary_user(self.user))

    def test_issue_stores_only_hash_and_bounded_sanitized_metadata(self):
        capability, token = issue_canary_capability(
            user=self.user,
            purpose=CanaryCapability.Purpose.BILLING_BIND,
            scenario=CanaryCapability.Scenario.SUCCESS,
            issued_by=self.admin,
            related_payment_id=123,
            metadata={"consent_id": 456},
        )

        self.assertNotEqual(capability.nonce_hash, token)
        self.assertEqual(len(capability.nonce_hash), 64)
        self.assertNotIn(token, str(capability.metadata))
        self.assertEqual(capability.related_payment_id, 123)
        self.assertEqual(capability.metadata, {"consent_id": 456})

        with self.assertRaises(ValidationError):
            issue_canary_capability(
                user=self.user,
                purpose=CanaryCapability.Purpose.LOGIN,
                issued_by=self.admin,
                metadata={"card_token": "forbidden"},
            )

    def test_capability_is_exactly_bound_single_use_and_cannot_be_revived(self):
        capability, token = issue_canary_capability(
            user=self.user,
            purpose=CanaryCapability.Purpose.BILLING_BIND,
            issued_by=self.admin,
            related_payment_id=123,
        )

        with self.assertRaises(CanaryAuthorizationError):
            consume_canary_capability(
                token=token,
                user=self.user,
                purpose=CanaryCapability.Purpose.BILLING_RENEWAL,
                related_payment_id=123,
            )

        consumed = consume_canary_capability(
            token=token,
            user=self.user,
            purpose=CanaryCapability.Purpose.BILLING_BIND,
            related_payment_id=123,
        )
        self.assertEqual(consumed.status, CanaryCapability.Status.CONSUMED)
        self.assertIsNotNone(consumed.consumed_at)

        with self.assertRaises(CanaryAuthorizationError):
            consume_canary_capability(
                token=token,
                user=self.user,
                purpose=CanaryCapability.Purpose.BILLING_BIND,
                related_payment_id=123,
            )

        consumed.status = CanaryCapability.Status.ISSUED
        with self.assertRaises(ValidationError):
            consumed.full_clean()

    def test_expired_capability_fails_closed(self):
        capability, token = issue_canary_capability(
            user=self.user,
            purpose=CanaryCapability.Purpose.RESET,
            issued_by=self.admin,
        )
        CanaryCapability.objects.filter(pk=capability.pk).update(expires_at=timezone.now() - timedelta(seconds=1))

        with self.assertRaises(CanaryAuthorizationError):
            consume_canary_capability(
                token=token,
                user=self.user,
                purpose=CanaryCapability.Purpose.RESET,
            )

        capability.refresh_from_db()
        self.assertEqual(capability.status, CanaryCapability.Status.EXPIRED)

    def test_session_requires_canary_auth_mode_and_exact_user(self):
        request = RequestFactory().get("/app/")
        request.session = {
            "miniapp_tg_user_id": self.user.tg_user_id,
            "miniapp_auth_mode": "production_canary",
        }
        self.assertEqual(
            assert_canary_session(request, self.user, CanaryCapability.Purpose.BILLING_BIND),
            self.user,
        )

        request.session["miniapp_auth_mode"] = "browser"
        with self.assertRaises(PermissionDenied):
            assert_canary_session(request, self.user, CanaryCapability.Purpose.BILLING_BIND)

        request.session["miniapp_auth_mode"] = "production_canary"
        request.session["miniapp_tg_user_id"] = 999999
        with self.assertRaises(PermissionDenied):
            assert_canary_session(request, self.user, CanaryCapability.Purpose.BILLING_BIND)


@override_settings(PRODUCTION_CANARY_ENABLED=True, PRODUCTION_CANARY_TELEGRAM_IDS=[92002])
class CanaryAdminControlTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.user = TelegramUser.objects.create(
            tg_user_id=92002,
            first_name="Admin Canary",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.admin = get_user_model().objects.create_superuser(
            username="canary-control-admin", email="control@example.com", password="test-password"
        )
        self.state = UserAdminState.objects.create(
            telegram_user=self.user,
            status=UserAdminState.Status.ACTIVE,
            is_test_user=True,
        )

    def test_enable_requires_test_active_allowlisted_user(self):
        self.state.is_test_user = False
        self.state.save(update_fields=["is_test_user"])
        with self.assertRaises(PermissionDenied):
            enable_production_canary(user=self.user, admin_user=self.admin)

        self.state.is_test_user = True
        self.state.status = UserAdminState.Status.BANNED
        self.state.save(update_fields=["is_test_user", "status"])
        with self.assertRaises(PermissionDenied):
            enable_production_canary(user=self.user, admin_user=self.admin)

        self.state.status = UserAdminState.Status.ACTIVE
        self.state.save(update_fields=["status"])
        with override_settings(PRODUCTION_CANARY_TELEGRAM_IDS=[]):
            with self.assertRaises(PermissionDenied):
                enable_production_canary(user=self.user, admin_user=self.admin)

    def test_enable_records_explicit_actor_and_audit(self):
        state = enable_production_canary(
            user=self.user,
            admin_user=self.admin,
            notes="approved isolated production smoke",
        )

        self.assertTrue(state.is_production_canary)
        self.assertEqual(state.canary_enabled_by, self.admin)
        self.assertIsNotNone(state.canary_enabled_at)
        self.assertEqual(state.canary_notes, "approved isolated production smoke")
        audit = AdminAuditLog.objects.get(action="production_canary_enabled", target_user_id=self.user.tg_user_id)
        self.assertEqual(audit.admin_user, self.admin)

    def test_disable_revokes_active_capabilities_and_only_canary_sessions(self):
        enable_production_canary(user=self.user, admin_user=self.admin)
        capability, _ = issue_canary_capability(
            user=self.user,
            purpose=CanaryCapability.Purpose.LOGIN,
            issued_by=self.admin,
        )
        now = timezone.now()
        canary_session = UserAuthSession.objects.create(
            telegram_user=self.user,
            token_hash="a" * 64,
            auth_mode="production_canary",
            expires_at=now + timedelta(hours=1),
            last_seen_at=now,
        )
        regular_session = UserAuthSession.objects.create(
            telegram_user=self.user,
            token_hash="b" * 64,
            auth_mode="telegram",
            expires_at=now + timedelta(hours=1),
            last_seen_at=now,
        )

        state = disable_production_canary(user=self.user, admin_user=self.admin)

        capability.refresh_from_db()
        canary_session.refresh_from_db()
        regular_session.refresh_from_db()
        self.assertFalse(state.is_production_canary)
        self.assertEqual(capability.status, CanaryCapability.Status.REVOKED)
        self.assertIsNotNone(canary_session.revoked_at)
        self.assertIsNone(regular_session.revoked_at)
        self.assertTrue(
            AdminAuditLog.objects.filter(action="production_canary_disabled", target_user_id=self.user.tg_user_id).exists()
        )

    def test_admin_control_is_object_scoped_and_requires_confirming_post(self):
        self.client.force_login(self.admin)
        enable_url = reverse("admin:users_telegramuser_enable_production_canary", args=[self.user.pk])

        response = self.client.get(enable_url)
        self.assertEqual(response.status_code, 200)
        self.state.refresh_from_db()
        self.assertFalse(self.state.is_production_canary)

        response = self.client.post(enable_url, data={
            "confirmation": "CONFIRM PRODUCTION CANARY",
            "comment": "confirmed",
        })
        self.assertRedirects(response, reverse("admin:users_telegramuser_change", args=[self.user.pk]))
        self.state.refresh_from_db()
        self.assertTrue(self.state.is_production_canary)

        disable_url = reverse("admin:users_telegramuser_disable_production_canary", args=[self.user.pk])
        response = self.client.post(disable_url, data={
            "confirmation": "CONFIRM PRODUCTION CANARY",
            "comment": "finished",
        })
        self.assertRedirects(response, reverse("admin:users_telegramuser_change", args=[self.user.pk]))
        self.state.refresh_from_db()
        self.assertFalse(self.state.is_production_canary)
