from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch
import uuid

from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from miniapp.models import AcquisitionSession, BrowserLoginTokenUse, FunnelEvent, InstallNudgeDeviceState
from subscriptions.models import BillingConsent
from users import services
from users.models import TelegramUser, UserAuthIdentity, UserAuthSession, UserOidcTokenUse


class PrivacyDeletionGraphTests(SimpleTestCase):
    @patch("users.services._delete_rows", return_value=1)
    def test_privacy_graph_deletes_all_user_linked_rows_in_fk_safe_order(self, delete_rows) -> None:
        tables = {
            "billing_consents",
            "funnel_events",
            "acquisition_sessions",
            "user_auth_sessions",
            "user_auth_identities",
            "user_oidc_token_uses",
            "miniapp_browser_login_token_uses",
            "miniapp_write_receipts",
            "miniapp_draft_actions",
            "miniapp_install_nudge_states",
            "miniapp_install_nudge_device_states",
            "miniapp_push_deliveries",
            "miniapp_web_push_subscriptions",
            "miniapp_notification_preferences",
            "miniapp_app_notifications",
            "daily_expense_reminder_settings",
        }
        stats = services.HardDeleteStats()

        services._delete_privacy_identity_rows(
            user_id=812345,
            stats=stats,
            existing_tables=tables,
        )

        called_tables = [call.kwargs["table_name"] for call in delete_rows.call_args_list]
        self.assertEqual(set(called_tables), tables)
        self.assertLess(called_tables.index("billing_consents"), called_tables.index("acquisition_sessions"))
        self.assertLess(called_tables.index("funnel_events"), called_tables.index("acquisition_sessions"))
        self.assertLess(called_tables.index("user_auth_sessions"), called_tables.index("user_auth_identities"))
        self.assertLess(called_tables.index("miniapp_push_deliveries"), called_tables.index("miniapp_app_notifications"))
        self.assertLess(
            called_tables.index("miniapp_push_deliveries"),
            called_tables.index("miniapp_web_push_subscriptions"),
        )
        self.assertEqual(stats.deleted_billing_consents, 1)
        self.assertEqual(stats.deleted_acquisition_sessions, 1)
        self.assertEqual(stats.deleted_auth_sessions, 1)
        self.assertEqual(stats.deleted_install_nudge_device_states, 1)


class PrivacyDeletionGraphDatabaseTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        ensure_telegram_user_table()

    def test_phase2_rows_are_deleted_in_fk_safe_order(self) -> None:
        now = timezone.now()
        user = TelegramUser.objects.create(
            tg_user_id=812345,
            first_name="Phase",
            lang="uk",
            base_currency="UAH",
            created_at=now,
        )
        identity = UserAuthIdentity.objects.create(
            telegram_user=user,
            provider=UserAuthIdentity.Provider.TELEGRAM_OIDC,
            subject="phase2-subject",
            first_authenticated_at=now,
            last_authenticated_at=now,
        )
        UserAuthSession.objects.create(
            telegram_user=user,
            identity=identity,
            token_hash="a" * 64,
            auth_mode="telegram_oidc",
            expires_at=now + timedelta(days=1),
            last_seen_at=now,
        )
        UserOidcTokenUse.objects.create(
            token_hash="b" * 64,
            provider="telegram_oidc",
            subject="phase2-subject",
            tg_user_id=user.tg_user_id,
            expires_at=now + timedelta(days=1),
        )
        BrowserLoginTokenUse.objects.create(
            token_hash="c" * 64,
            tg_user_id=user.tg_user_id,
            expires_at=now + timedelta(days=1),
        )
        install_device = InstallNudgeDeviceState.objects.create(
            tg_user_id=user.tg_user_id,
            device_hash="f" * 40,
            platform="android",
            next_prompt_at=now,
        )
        acquisition = AcquisitionSession.objects.create(
            user=user,
            expires_at=now + timedelta(days=30),
        )
        FunnelEvent.objects.create(
            acquisition_session=acquisition,
            event_name="auth_success",
            idempotency_key="auth:phase2",
            payload_hash="d" * 64,
            source=FunnelEvent.Source.SERVER,
        )
        BillingConsent.objects.create(
            user=user,
            acquisition_session=acquisition,
            idempotency_key=uuid.uuid4(),
            offer_id="organic",
            offer_version="1",
            bind_amount_minor=100,
            currency="UAH",
            trial_days=30,
            renewal_amount_minor=49900,
            renewal_period_days=30,
            terms_version="2026-09",
            privacy_version="2026-09",
            copy_locale="uk",
            payload_hash="e" * 64,
        )
        stats = services.HardDeleteStats()

        services._delete_privacy_identity_rows(
            user_id=user.tg_user_id,
            stats=stats,
            existing_tables=set(connection.introspection.table_names()),
        )

        self.assertFalse(BillingConsent.objects.filter(user=user).exists())
        self.assertFalse(FunnelEvent.objects.filter(acquisition_session=acquisition).exists())
        self.assertFalse(AcquisitionSession.objects.filter(pk=acquisition.pk).exists())
        self.assertFalse(UserAuthSession.objects.filter(telegram_user=user).exists())
        self.assertFalse(UserAuthIdentity.objects.filter(telegram_user=user).exists())
        self.assertFalse(UserOidcTokenUse.objects.filter(tg_user_id=user.tg_user_id).exists())
        self.assertFalse(BrowserLoginTokenUse.objects.filter(tg_user_id=user.tg_user_id).exists())
        self.assertFalse(InstallNudgeDeviceState.objects.filter(pk=install_device.pk).exists())
        self.assertEqual(stats.deleted_install_nudge_device_states, 1)
