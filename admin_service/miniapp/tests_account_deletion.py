from __future__ import annotations

import json
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.db import connection
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from miniapp.auth import SESSION_AUTH_AT_KEY
from audit_log.models import AdminAuditLog
from miniapp.models import BrowserLoginTokenUse, WebPushSubscription
from subscriptions.monobank import MonobankAPIError
from subscriptions.models import BillingProfile
from users.models import TelegramUser, UserAuthIdentity, UserAuthSession, UserOidcTokenUse


@override_settings(
    DEBUG=True,
    MINIAPP_DEV_TG_USER_ID=901001,
    TELEGRAM_BOT_TOKEN="123456:test-account-deletion-token",
)
class AccountDeletionApiTests(TestCase):
    def setUp(self) -> None:
        self.user = TelegramUser.objects.create(
            tg_user_id=901001,
            first_name="Synthetic",
            username="synthetic_delete",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

    def login(self) -> None:
        response = self.client.post(
            "/app/api/auth/dev",
            data="{}",
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)

    def test_preflight_requires_authenticated_session(self) -> None:
        response = self.client.get("/app/api/account-deletion")

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "session_required")

    def test_preflight_issues_expiring_challenge_for_fresh_session(self) -> None:
        self.login()

        response = self.client.get("/app/api/account-deletion")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertGreaterEqual(len(payload["challenge"]), 32)
        self.assertEqual(payload["confirmation_text"], "ВИДАЛИТИ")
        self.assertEqual(payload["expires_in_seconds"], 600)
        self.assertEqual(
            payload["reauth_url"],
            "/app/auth/telegram/start?next=/app/%3Fscreen%3Dsettings%26account-deletion%3D1",
        )
        self.assertEqual(payload["public_deletion_url"], "https://vydno.capital/delete-account.html")

    def test_preflight_requires_recent_reauthentication(self) -> None:
        self.login()
        session = self.client.session
        session[SESSION_AUTH_AT_KEY] = int(time.time()) - 301
        session.save()

        response = self.client.get("/app/api/account-deletion")

        self.assertEqual(response.status_code, 409)
        payload = response.json()
        self.assertEqual(payload["error"]["code"], "reauth_required")
        self.assertIn("/app/auth/telegram/start?next=", payload["error"]["reauth_url"])
        self.assertNotIn("challenge", payload)

    def test_old_telegram_proof_cannot_refresh_sensitive_authentication(self) -> None:
        identity = SimpleNamespace(
            tg_user_id=self.user.tg_user_id,
            first_name="Synthetic",
            last_name="",
            username="synthetic_delete",
            raw_user={"language_code": "uk"},
            auth_date=int(time.time()) - 301,
        )
        registration = SimpleNamespace(user=self.user)
        with (
            patch("miniapp.views.validate_telegram_init_data", return_value=identity),
            patch("miniapp.views.bootstrap_telegram_identity", return_value=registration),
        ):
            auth_response = self.client.post(
                "/app/api/auth/telegram",
                data=json.dumps({"init_data": "synthetic validated old proof"}),
                content_type="application/json",
            )

        self.assertEqual(auth_response.status_code, 200)
        response = self.client.get("/app/api/account-deletion")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "reauth_required")

    def test_preflight_blocks_family_owner_with_other_members(self) -> None:
        member = TelegramUser.objects.create(
            tg_user_id=901002,
            first_name="Member",
            lang="uk",
            base_currency="UAH",
            created_at=timezone.now(),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["Shared", self.user.tg_user_id],
            )
            family_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, 'owner', 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, self.user.tg_user_id, self.user.tg_user_id],
            )
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, 'member', 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, member.tg_user_id, self.user.tg_user_id],
            )
        self.login()

        response = self.client.get("/app/api/account-deletion")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "family_members_must_be_removed")
        self.assertNotIn("challenge", response.json())

    def test_confirm_rejects_missing_csrf_before_deletion(self) -> None:
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(
            client.post("/app/api/auth/dev", data="{}", content_type="application/json").status_code,
            200,
        )
        self.assertEqual(client.get("/app/").status_code, 200)
        challenge = client.get("/app/api/account-deletion").json()["challenge"]

        response = client.post(
            "/app/api/account-deletion/confirm",
            data={
                "challenge": challenge,
                "confirmation_text": "ВИДАЛИТИ",
                "acknowledged": True,
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())

    def test_confirm_rejects_inexact_confirmation_text(self) -> None:
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(
            client.post("/app/api/auth/dev", data="{}", content_type="application/json").status_code,
            200,
        )
        self.assertEqual(client.get("/app/").status_code, 200)
        challenge = client.get("/app/api/account-deletion").json()["challenge"]
        csrf_token = client.cookies["csrftoken"].value

        response = client.post(
            "/app/api/account-deletion/confirm",
            data=json.dumps(
                {
                    "challenge": challenge,
                    "confirmation_text": "видалити",
                    "acknowledged": True,
                }
            ),
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "confirmation_mismatch")
        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())

    def test_confirm_rechecks_recent_authentication(self) -> None:
        self.login()
        challenge = self.client.get("/app/api/account-deletion").json()["challenge"]
        session = self.client.session
        session[SESSION_AUTH_AT_KEY] = int(time.time()) - 301
        session.save()

        response = self.client.post(
            "/app/api/account-deletion/confirm",
            data=json.dumps(
                {
                    "challenge": challenge,
                    "confirmation_text": "ВИДАЛИТИ",
                    "acknowledged": True,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "reauth_required")
        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())

    def test_confirm_rejects_client_supplied_target_user_id(self) -> None:
        other_user = TelegramUser.objects.create(
            tg_user_id=901003,
            first_name="Other",
            username="other_delete_target",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.login()
        challenge = self.client.get("/app/api/account-deletion").json()["challenge"]

        response = self.client.post(
            "/app/api/account-deletion/confirm",
            data=json.dumps(
                {
                    "challenge": challenge,
                    "confirmation_text": "ВИДАЛИТИ",
                    "acknowledged": True,
                    "user_id": other_user.tg_user_id,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_payload")
        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())
        self.assertTrue(TelegramUser.objects.filter(pk=other_user.pk).exists())

    @patch("subscriptions.billing.delete_wallet_card", return_value={"status": "success"})
    def test_confirm_deletes_owned_privacy_graph_and_cannot_be_replayed(self, delete_wallet_card) -> None:
        now = timezone.now()
        identity = UserAuthIdentity.objects.create(
            telegram_user=self.user,
            provider=UserAuthIdentity.Provider.TELEGRAM_OIDC,
            subject="synthetic-delete-subject",
            profile={},
            first_authenticated_at=now,
            last_authenticated_at=now,
        )
        UserAuthSession.objects.create(
            telegram_user=self.user,
            identity=identity,
            token_hash="a" * 64,
            auth_mode="telegram_oidc",
            expires_at=now + timedelta(days=1),
            last_seen_at=now,
        )
        UserOidcTokenUse.objects.create(
            token_hash="b" * 64,
            provider="telegram_oidc",
            subject="synthetic-delete-subject",
            tg_user_id=self.user.tg_user_id,
            expires_at=now + timedelta(minutes=10),
        )
        BrowserLoginTokenUse.objects.create(
            token_hash="c" * 64,
            tg_user_id=self.user.tg_user_id,
            expires_at=now + timedelta(minutes=10),
        )
        WebPushSubscription.objects.create(
            tg_user_id=self.user.tg_user_id,
            endpoint="https://push.example.invalid/synthetic-delete",
            p256dh="synthetic-p256dh",
            auth="synthetic-auth",
            is_active=True,
        )
        BillingProfile.objects.create(
            user=self.user,
            provider="monobank",
            wallet_id="delete-api-wallet",
            card_token="delete-api-token",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )

        client = Client(enforce_csrf_checks=True)
        self.assertEqual(
            client.post("/app/api/auth/dev", data="{}", content_type="application/json").status_code,
            200,
        )
        self.assertEqual(client.get("/app/").status_code, 200)
        challenge = client.get("/app/api/account-deletion").json()["challenge"]
        csrf_token = client.cookies["csrftoken"].value
        body = json.dumps(
            {
                "challenge": challenge,
                "confirmation_text": "ВИДАЛИТИ",
                "acknowledged": True,
            }
        )

        response = client.post(
            "/app/api/account-deletion/confirm",
            data=body,
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "deleted")
        self.assertFalse(TelegramUser.objects.filter(pk=self.user.pk).exists())
        self.assertFalse(UserAuthSession.objects.filter(telegram_user_id=self.user.tg_user_id).exists())
        self.assertFalse(UserAuthIdentity.objects.filter(telegram_user_id=self.user.tg_user_id).exists())
        self.assertFalse(WebPushSubscription.objects.filter(tg_user_id=self.user.tg_user_id).exists())
        self.assertEqual(UserOidcTokenUse.objects.get(token_hash="b" * 64).tg_user_id, 0)
        self.assertEqual(UserOidcTokenUse.objects.get(token_hash="b" * 64).provider, "")
        self.assertEqual(UserOidcTokenUse.objects.get(token_hash="b" * 64).subject, "")
        self.assertEqual(BrowserLoginTokenUse.objects.get(token_hash="c" * 64).tg_user_id, 0)
        delete_wallet_card.assert_called_once_with("delete-api-token")

        receipt = AdminAuditLog.objects.get(action="account_self_deleted", object_id=payload["request_id"])
        self.assertIsNone(receipt.target_user_id)
        self.assertNotIn(str(self.user.tg_user_id), json.dumps(receipt.after))

        replay = client.post(
            "/app/api/account-deletion/confirm",
            data=body,
            content_type="application/json",
            HTTP_X_CSRFTOKEN=csrf_token,
        )
        self.assertEqual(replay.status_code, 401)
        delete_wallet_card.assert_called_once()

    @patch("subscriptions.billing.delete_wallet_card", return_value={"status": "success"})
    def test_other_server_tracked_session_stays_revoked_after_same_id_reregisters(
        self,
        _delete_wallet_card,
    ) -> None:
        deleting_client = Client()
        other_client = Client()
        for client in (deleting_client, other_client):
            response = client.post("/app/api/auth/dev", data="{}", content_type="application/json")
            self.assertEqual(response.status_code, 200)
        self.assertEqual(UserAuthSession.objects.filter(telegram_user=self.user).count(), 2)

        challenge = deleting_client.get("/app/api/account-deletion").json()["challenge"]
        response = deleting_client.post(
            "/app/api/account-deletion/confirm",
            data=json.dumps(
                {
                    "challenge": challenge,
                    "confirmation_text": "ВИДАЛИТИ",
                    "acknowledged": True,
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(UserAuthSession.objects.filter(telegram_user_id=self.user.tg_user_id).exists())

        TelegramUser.objects.create(
            tg_user_id=self.user.tg_user_id,
            first_name="Recreated",
            lang="uk",
            base_currency="UAH",
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        stale_response = other_client.get("/app/api/profile")
        self.assertEqual(stale_response.status_code, 401)
        self.assertEqual(stale_response.json()["error"]["code"], "session_required")

    def test_provider_failure_is_retryable_and_preserves_account(self) -> None:
        billing = BillingProfile.objects.create(
            user=self.user,
            provider="monobank",
            wallet_id="wallet-delete-failure",
            card_token="sensitive-card-token",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(
            client.post("/app/api/auth/dev", data="{}", content_type="application/json").status_code,
            200,
        )
        self.assertEqual(client.get("/app/").status_code, 200)
        challenge = client.get("/app/api/account-deletion").json()["challenge"]
        csrf_token = client.cookies["csrftoken"].value

        with patch(
            "subscriptions.billing.delete_wallet_card",
            side_effect=MonobankAPIError("provider-secret-detail"),
        ):
            response = client.post(
                "/app/api/account-deletion/confirm",
                data=json.dumps(
                    {
                        "challenge": challenge,
                        "confirmation_text": "ВИДАЛИТИ",
                        "acknowledged": True,
                    }
                ),
                content_type="application/json",
                HTTP_X_CSRFTOKEN=csrf_token,
            )

        self.assertEqual(response.status_code, 502)
        self.assertTrue(response.json()["error"].get("retryable", False))
        body = response.content.decode("utf-8")
        self.assertNotIn("provider-secret-detail", body)
        self.assertNotIn("sensitive-card-token", body)
        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())
        billing.refresh_from_db()
        self.assertEqual(billing.card_token, "sensitive-card-token")
        self.assertTrue(billing.auto_renew_enabled)
