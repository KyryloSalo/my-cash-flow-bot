from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from types import SimpleNamespace
from unittest import TestCase as UnitTestCase
from unittest.mock import patch
from urllib.parse import parse_qs, urlencode, urlparse

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
django.setup()

from django.core.checks import Tags, run_checks
from django.db import connection
from django.test import Client, SimpleTestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table


def build_telegram_init_data(*, bot_token: str, tg_user_id: int, allows_write_to_pm: bool = False) -> str:
    payload = {
        "auth_date": str(int(time.time())),
        "query_id": "synthetic-query-id",
        "user": json.dumps(
            {
                "id": tg_user_id,
                "first_name": "Web",
                "last_name": "User",
                "username": "web_user",
                "language_code": "uk",
                "allows_write_to_pm": allows_write_to_pm,
            },
            separators=(",", ":"),
        ),
    }
    check = "\n".join(f"{key}={value}" for key, value in sorted(payload.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    payload["hash"] = hmac.new(secret, check.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(payload)


@override_settings(
    ALLOWED_HOSTS=["testserver"],
    SECURE_SSL_REDIRECT=False,
    SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
    TELEGRAM_OIDC_CLIENT_ID="123456789",
    TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
    TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
)
class TelegramOidcStartTests(SimpleTestCase):
    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
    )
    def test_system_check_rejects_partial_oidc_configuration(self) -> None:
        errors = [item for item in run_checks(tags=[Tags.security]) if item.id == "miniapp.E001"]

        self.assertEqual(len(errors), 1)

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="http://attacker.example/callback",
    )
    def test_system_check_requires_canonical_https_oidc_callback(self) -> None:
        errors = [item for item in run_checks(tags=[Tags.security]) if item.id == "miniapp.E002"]

        self.assertEqual(len(errors), 1)

    def test_start_uses_authorization_code_pkce_and_preserves_safe_return_path(self) -> None:
        client = Client()

        response = client.get("/app/auth/telegram/start", {"next": "/app/?onboarding=1"})

        self.assertEqual(response.status_code, 302)
        target = urlparse(response.headers["Location"])
        self.assertEqual((target.scheme, target.netloc, target.path), ("https", "oauth.telegram.org", "/auth"))
        params = parse_qs(target.query)
        self.assertEqual(params["client_id"], ["123456789"])
        self.assertEqual(params["redirect_uri"], ["https://vydno.capital/app/auth/telegram/callback"])
        self.assertEqual(params["response_type"], ["code"])
        self.assertEqual(params["scope"], ["openid profile"])
        self.assertEqual(params["code_challenge_method"], ["S256"])
        self.assertEqual(len(params["state"][0]), 43)
        self.assertGreaterEqual(len(params["nonce"][0]), 32)
        self.assertNotIn("synthetic-oidc-secret", response.headers["Location"])

        flow = client.session["miniapp_oidc_flow"]
        expected_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(flow["code_verifier"].encode("ascii")).digest()
        ).rstrip(b"=").decode("ascii")
        self.assertEqual(params["code_challenge"], [expected_challenge])
        self.assertEqual(flow["state_hash"], hashlib.sha256(params["state"][0].encode("ascii")).hexdigest())
        self.assertEqual(flow["nonce"], params["nonce"][0])
        self.assertEqual(flow["return_to"], "/app/?onboarding=1")
        self.assertNotIn("state", flow)

    @override_settings(TELEGRAM_OIDC_CLIENT_SECRET="")
    def test_start_fails_closed_before_provider_redirect_when_client_secret_is_missing(self) -> None:
        response = Client().get("/app/auth/telegram/start")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "telegram_oidc_unavailable")

    def test_start_discards_external_return_url(self) -> None:
        client = Client()

        response = client.get("/app/auth/telegram/start", {"next": "https://attacker.example/steal"})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(client.session["miniapp_oidc_flow"]["return_to"], "/app/")

    def test_callback_rejects_state_mismatch_and_consumes_pending_flow(self) -> None:
        client = Client()
        client.get("/app/auth/telegram/start")

        response = client.get(
            "/app/auth/telegram/callback",
            {"state": "attacker-controlled-state", "code": "synthetic-code"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertContains(response, 'data-error-code="telegram_oidc_state_invalid"', status_code=400)
        self.assertNotIn("miniapp_oidc_flow", client.session)
        self.assertEqual(response.headers["Cache-Control"], "no-store, no-cache, must-revalidate, max-age=0, private")

    @patch("miniapp.telegram_oidc.urlopen")
    def test_code_exchange_uses_basic_auth_and_original_pkce_verifier(self, mock_urlopen) -> None:
        from miniapp import telegram_oidc

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def read(self):
                return json.dumps({"id_token": "synthetic.jwt.value", "access_token": "opaque"}).encode("utf-8")

        mock_urlopen.return_value = FakeResponse()

        id_token = telegram_oidc.exchange_authorization_code(
            code="single-use-code",
            code_verifier="v" * 64,
        )

        self.assertEqual(id_token, "synthetic.jwt.value")
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://oauth.telegram.org/token")
        expected_basic = base64.b64encode(
            b"123456789:synthetic-oidc-secret-not-for-deployment"
        ).decode("ascii")
        self.assertEqual(request.headers["Authorization"], f"Basic {expected_basic}")
        form = parse_qs(request.data.decode("ascii"))
        self.assertEqual(form["grant_type"], ["authorization_code"])
        self.assertEqual(form["code"], ["single-use-code"])
        self.assertEqual(form["code_verifier"], ["v" * 64])
        self.assertEqual(form["client_id"], ["123456789"])
        self.assertEqual(form["redirect_uri"], ["https://vydno.capital/app/auth/telegram/callback"])
        self.assertNotIn("client_secret", form)

    def test_id_token_verification_binds_signature_issuer_audience_nonce_and_telegram_id(self) -> None:
        import jwt
        from cryptography.hazmat.primitives.asymmetric import rsa

        from miniapp import telegram_oidc

        now = int(time.time())
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = jwt.encode(
            {
                "iss": "https://oauth.telegram.org",
                "aud": "123456789",
                "sub": "stable-subject-9001",
                "iat": now,
                "exp": now + 3600,
                "nonce": "expected-nonce-value-0000000000000001",
                "id": 9001,
                "given_name": "Ihor",
                "family_name": "Example",
                "preferred_username": "ihor_example",
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "synthetic-key-1"},
        )

        class SyntheticJwksClient:
            def get_signing_key_from_jwt(self, raw_token):
                self.seen_token = raw_token
                return SimpleNamespace(key=private_key.public_key())

        jwks_client = SyntheticJwksClient()
        identity = telegram_oidc.verify_id_token(
            token,
            expected_nonce="expected-nonce-value-0000000000000001",
            jwks_client=jwks_client,
        )

        self.assertEqual(jwks_client.seen_token, token)
        self.assertEqual(identity.provider, "telegram_oidc")
        self.assertEqual(identity.subject, "stable-subject-9001")
        self.assertEqual(identity.tg_user_id, 9001)
        self.assertEqual(identity.first_name, "Ihor")
        self.assertEqual(identity.last_name, "Example")
        self.assertEqual(identity.username, "ihor_example")

    def test_id_token_verification_rejects_nonce_spoof(self) -> None:
        import jwt
        from cryptography.hazmat.primitives.asymmetric import rsa

        from miniapp import telegram_oidc

        now = int(time.time())
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = jwt.encode(
            {
                "iss": "https://oauth.telegram.org",
                "aud": "123456789",
                "sub": "stable-subject-9001",
                "iat": now,
                "exp": now + 3600,
                "nonce": "provider-returned-different-nonce",
                "id": 9001,
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "synthetic-key-2"},
        )

        class SyntheticJwksClient:
            def get_signing_key_from_jwt(self, raw_token):
                return SimpleNamespace(key=private_key.public_key())

        with self.assertRaisesRegex(telegram_oidc.TelegramOidcError, "nonce") as caught:
            telegram_oidc.verify_id_token(
                token,
                expected_nonce="expected-nonce-value-0000000000000001",
                jwks_client=SyntheticJwksClient(),
            )
        self.assertEqual(caught.exception.code, "telegram_oidc_nonce_invalid")

    def test_id_token_verification_accepts_provider_token_without_optional_nonce_claim(self) -> None:
        import jwt
        from cryptography.hazmat.primitives.asymmetric import rsa

        from miniapp import telegram_oidc

        now = int(time.time())
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = jwt.encode(
            {
                "iss": "https://oauth.telegram.org",
                "aud": "123456789",
                "sub": "stable-subject-9007",
                "iat": now,
                "exp": now + 3600,
                "id": 9007,
                "name": "Provider Contract",
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "synthetic-key-no-nonce"},
        )

        class SyntheticJwksClient:
            def get_signing_key_from_jwt(self, raw_token):
                return SimpleNamespace(key=private_key.public_key())

        identity = telegram_oidc.verify_id_token(
            token,
            expected_nonce="request-nonce-not-returned-by-provider",
            jwks_client=SyntheticJwksClient(),
        )

        self.assertEqual(identity.tg_user_id, 9007)


class TelegramRegistrationBootstrapTests(UnitTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        ensure_telegram_user_table()
        from django.contrib.auth import get_user_model

        from bot_settings.models import BotSetting
        from users.models import (
            UserAdminState,
            UserAuthIdentity,
            UserAuthSession,
            UserOidcTokenUse,
        )

        cls._created_models = []
        existing = set(connection.introspection.table_names())
        AuthUser = get_user_model()
        if AuthUser._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(AuthUser)
            cls._created_models.append(AuthUser)
        existing = set(connection.introspection.table_names())
        if BotSetting._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(BotSetting)
            cls._created_models.append(BotSetting)
        existing = set(connection.introspection.table_names())
        if UserAdminState._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(UserAdminState)
            cls._created_models.append(UserAdminState)
        existing = set(connection.introspection.table_names())
        if UserAuthIdentity._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(UserAuthIdentity)
            cls._created_models.append(UserAuthIdentity)
        existing = set(connection.introspection.table_names())
        if UserAuthSession._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(UserAuthSession)
            cls._created_models.append(UserAuthSession)
        existing = set(connection.introspection.table_names())
        if UserOidcTokenUse._meta.db_table not in existing:
            with connection.schema_editor() as schema_editor:
                schema_editor.create_model(UserOidcTokenUse)
            cls._created_models.append(UserOidcTokenUse)

    @classmethod
    def tearDownClass(cls) -> None:
        with connection.schema_editor() as schema_editor:
            for model in reversed(cls._created_models):
                schema_editor.delete_model(model)
        super().tearDownClass()

    def setUp(self) -> None:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM bot_settings WHERE key = %s", ["registration_enabled"])
            for table, column in (
                ("user_oidc_token_uses", "tg_user_id"),
                ("user_auth_sessions", "telegram_user_id"),
                ("user_auth_identities", "telegram_user_id"),
                ("user_admin_states", "telegram_user_id"),
                ("users", "tg_user_id"),
            ):
                cursor.execute(
                    f"DELETE FROM {table} WHERE {column} >= %s AND {column} < %s",
                    [9000, 10000],
                )

    def test_oidc_identity_bootstrap_creates_canonical_user_admin_state_and_identity(self) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import TelegramUser, UserAdminState, UserAuthIdentity
        from users.registration import bootstrap_telegram_identity

        result = bootstrap_telegram_identity(
            AuthenticatedTelegramIdentity(
                provider="telegram_oidc",
                subject="stable-subject-9001",
                tg_user_id=9001,
                first_name="Ihor",
                last_name="Example",
                username="ihor_example",
                claims={"sub": "stable-subject-9001", "id": 9001},
            ),
            source="telegram_oidc",
            authenticated_at=timezone.now(),
        )

        self.assertTrue(result.created)
        user = TelegramUser.objects.get(tg_user_id=9001)
        self.assertEqual(user.first_name, "Ihor")
        self.assertEqual(user.last_name, "Example")
        self.assertEqual(user.username, "ihor_example")
        self.assertEqual(user.base_currency, "UAH")
        self.assertFalse(user.onboarding_completed)
        state = UserAdminState.objects.get(telegram_user_id=9001)
        self.assertEqual(state.status, UserAdminState.Status.ACTIVE)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PAYWALL)
        self.assertEqual(state.source, "telegram_oidc")
        self.assertFalse(state.can_receive_messages)
        auth_identity = UserAuthIdentity.objects.get(provider="telegram_oidc", subject="stable-subject-9001")
        self.assertEqual(auth_identity.telegram_user_id, 9001)
        self.assertEqual(auth_identity.profile["preferred_username"], "ihor_example")

    def test_oidc_bootstrap_reuses_existing_telegram_user_without_resetting_financial_profile(self) -> None:
        from datetime import date

        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import TelegramUser
        from users.registration import bootstrap_telegram_identity

        existing = TelegramUser.objects.create(
            tg_user_id=9010,
            first_name="Old",
            username="old_username",
            lang="en",
            base_currency="EUR",
            start_date=date(2024, 1, 15),
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

        result = bootstrap_telegram_identity(
            AuthenticatedTelegramIdentity(
                provider="telegram_oidc",
                subject="stable-subject-9010",
                tg_user_id=9010,
                first_name="Current",
                last_name="Name",
                username="current_username",
                claims={"id": 9010, "sub": "stable-subject-9010"},
            ),
            source="telegram_oidc",
        )

        self.assertFalse(result.created)
        self.assertEqual(result.user.pk, existing.pk)
        result.user.refresh_from_db()
        self.assertEqual(result.user.base_currency, "EUR")
        self.assertEqual(result.user.lang, "en")
        self.assertEqual(result.user.start_date, date(2024, 1, 15))
        self.assertTrue(result.user.onboarding_completed)
        self.assertEqual(result.user.onboarding_version, 3)
        self.assertEqual(TelegramUser.objects.filter(tg_user_id=9010).count(), 1)

    def test_oidc_subject_cannot_be_rebound_to_another_telegram_user(self) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import TelegramUser
        from users.registration import (
            RegistrationIdentityConflict,
            bootstrap_telegram_identity,
        )

        original = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-shared-subject",
            tg_user_id=9011,
            first_name="Original",
            last_name="User",
            username="original_user",
            claims={"id": 9011, "sub": "stable-shared-subject"},
        )
        bootstrap_telegram_identity(original, source="telegram_oidc")
        spoofed = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-shared-subject",
            tg_user_id=9012,
            first_name="Spoofed",
            last_name="User",
            username="spoofed_user",
            claims={"id": 9012, "sub": "stable-shared-subject"},
        )

        with self.assertRaises(RegistrationIdentityConflict):
            bootstrap_telegram_identity(spoofed, source="telegram_oidc")

        self.assertFalse(TelegramUser.objects.filter(tg_user_id=9012).exists())

    def test_shared_bootstrap_rejects_new_user_when_registration_is_closed(self) -> None:
        from bot_settings.models import BotSetting
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import TelegramUser
        from users.registration import bootstrap_telegram_identity

        BotSetting.objects.create(
            key="registration_enabled",
            value="false",
            value_type=BotSetting.ValueType.BOOL,
        )
        identity = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9020",
            tg_user_id=9020,
            first_name="Closed",
            last_name="Registration",
            username="closed_registration",
            claims={"id": 9020, "sub": "stable-subject-9020"},
        )

        with self.assertRaises(Exception) as caught:
            bootstrap_telegram_identity(identity, source="telegram_oidc")

        self.assertEqual(type(caught.exception).__name__, "RegistrationClosed")
        self.assertFalse(TelegramUser.objects.filter(tg_user_id=9020).exists())

    @override_settings(
        TELEGRAM_BOT_TOKEN="synthetic-miniapp-token-not-for-deployment",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    def test_miniapp_init_data_uses_same_bootstrap_for_a_new_web_user(self) -> None:
        from users.models import TelegramUser, UserAuthIdentity

        client = Client()
        with patch("miniapp.views.resolve_access", return_value={"mode": "blocked"}):
            response = client.post(
                "/app/api/auth/telegram",
                data=json.dumps(
                    {
                        "init_data": build_telegram_init_data(
                            bot_token="synthetic-miniapp-token-not-for-deployment",
                            tg_user_id=9002,
                        )
                    }
                ),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(client.session["miniapp_tg_user_id"], 9002)
        user = TelegramUser.objects.get(tg_user_id=9002)
        self.assertEqual(user.first_name, "Web")
        self.assertEqual(user.last_name, "User")
        self.assertTrue(
            UserAuthIdentity.objects.filter(
                provider="telegram_miniapp",
                subject="9002",
                telegram_user_id=9002,
            ).exists()
        )

    @override_settings(
        TELEGRAM_BOT_TOKEN="synthetic-miniapp-token-not-for-deployment",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    def test_miniapp_write_permission_marks_new_user_messageable(self) -> None:
        from users.models import UserAdminState

        client = Client()
        with patch("miniapp.views.resolve_access", return_value={"mode": "blocked"}):
            response = client.post(
                "/app/api/auth/telegram",
                data=json.dumps(
                    {
                        "init_data": build_telegram_init_data(
                            bot_token="synthetic-miniapp-token-not-for-deployment",
                            tg_user_id=9024,
                            allows_write_to_pm=True,
                        )
                    }
                ),
                content_type="application/json",
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(UserAdminState.objects.get(telegram_user_id=9024).can_receive_messages)

    def test_miniapp_write_permission_upgrades_existing_oidc_user(self) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import UserAdminState, UserAuthIdentity
        from users.registration import (
            RegistrationTelegramIdentity,
            bootstrap_telegram_identity,
        )

        bootstrap_telegram_identity(
            AuthenticatedTelegramIdentity(
                provider="telegram_oidc",
                subject="stable-subject-9025",
                tg_user_id=9025,
                first_name="Existing",
                last_name="OIDC",
                username="existing_oidc",
                claims={"id": 9025, "sub": "stable-subject-9025"},
            ),
            source="telegram_oidc",
        )

        bootstrap_telegram_identity(
            RegistrationTelegramIdentity(
                provider=UserAuthIdentity.Provider.TELEGRAM_MINIAPP,
                subject="9025",
                tg_user_id=9025,
                claims={"allows_write_to_pm": True},
            ),
            source="telegram_miniapp",
        )

        self.assertTrue(UserAdminState.objects.get(telegram_user_id=9025).can_receive_messages)

    @override_settings(
        TELEGRAM_BOT_TOKEN="synthetic-miniapp-token-not-for-deployment",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    def test_miniapp_init_data_returns_registration_closed_for_new_user(self) -> None:
        from bot_settings.models import BotSetting
        from users.models import TelegramUser

        BotSetting.objects.create(
            key="registration_enabled",
            value="false",
            value_type=BotSetting.ValueType.BOOL,
        )
        client = Client(raise_request_exception=False)

        response = client.post(
            "/app/api/auth/telegram",
            data=json.dumps(
                {
                    "init_data": build_telegram_init_data(
                        bot_token="synthetic-miniapp-token-not-for-deployment",
                        tg_user_id=9022,
                    )
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "registration_closed")
        self.assertFalse(TelegramUser.objects.filter(tg_user_id=9022).exists())

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code")
    def test_callback_bootstraps_new_user_and_creates_oidc_session(self, exchange, verify) -> None:
        from miniapp.telegram_oidc import (
            TELEGRAM_OIDC_AUTH_MODE,
            AuthenticatedTelegramIdentity,
        )
        from users.models import TelegramUser, UserAuthIdentity, UserAuthSession

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9003",
            tg_user_id=9003,
            first_name="OIDC",
            last_name="User",
            username="oidc_user",
            claims={"id": 9003, "sub": "stable-subject-9003", "exp": int(time.time()) + 3600},
        )
        exchange.return_value = "synthetic.jwt.value"
        client = Client()
        start = client.get("/app/auth/telegram/start", {"next": "/app/?onboarding=1"})
        state = parse_qs(urlparse(start.headers["Location"]).query)["state"][0]
        flow = dict(client.session["miniapp_oidc_flow"])

        response = client.get(
            "/app/auth/telegram/callback",
            {"state": state, "code": "single-use-code"},
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/app/?onboarding=1")
        exchange.assert_called_once_with(code="single-use-code", code_verifier=flow["code_verifier"])
        verify.assert_called_once_with("synthetic.jwt.value", expected_nonce=flow["nonce"])
        self.assertEqual(client.session["miniapp_tg_user_id"], 9003)
        self.assertEqual(client.session["miniapp_auth_mode"], TELEGRAM_OIDC_AUTH_MODE)
        self.assertTrue(TelegramUser.objects.filter(tg_user_id=9003).exists())
        self.assertTrue(
            UserAuthIdentity.objects.filter(
                provider="telegram_oidc",
                subject="stable-subject-9003",
                telegram_user_id=9003,
            ).exists()
        )
        session_record = UserAuthSession.objects.get(telegram_user_id=9003, revoked_at__isnull=True)
        self.assertEqual(session_record.identity.subject, "stable-subject-9003")

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code")
    def test_repeated_callback_returns_to_app_when_oidc_session_is_already_valid(self, exchange, verify) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9008",
            tg_user_id=9008,
            first_name="Android",
            last_name="Retry",
            username="android_retry",
            claims={"id": 9008, "sub": "stable-subject-9008", "exp": int(time.time()) + 3600},
        )
        exchange.return_value = "synthetic.jwt.android-retry"
        client = Client()
        start = client.get("/app/auth/telegram/start")
        state = parse_qs(urlparse(start.headers["Location"]).query)["state"][0]

        first_callback = client.get(
            "/app/auth/telegram/callback",
            {"state": state, "code": "single-use-android-code"},
        )
        repeated_callback = client.get(
            "/app/auth/telegram/callback",
            {"state": state, "code": "duplicate-mobile-return"},
        )

        self.assertEqual(first_callback.status_code, 302)
        self.assertEqual(repeated_callback.status_code, 302)
        self.assertEqual(repeated_callback.headers["Location"], "/app/")
        self.assertEqual(exchange.call_count, 1)

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code", return_value="synthetic.jwt.registration-closed")
    def test_callback_returns_recovery_page_when_registration_is_closed(self, exchange, verify) -> None:
        from bot_settings.models import BotSetting
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import TelegramUser

        BotSetting.objects.create(
            key="registration_enabled",
            value="false",
            value_type=BotSetting.ValueType.BOOL,
        )
        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9021",
            tg_user_id=9021,
            first_name="Closed",
            last_name="Callback",
            username="closed_callback",
            claims={"id": 9021, "sub": "stable-subject-9021", "exp": int(time.time()) + 3600},
        )
        client = Client(raise_request_exception=False)
        start = client.get("/app/auth/telegram/start")
        state = parse_qs(urlparse(start["Location"]).query)["state"][0]

        response = client.get(
            "/app/auth/telegram/callback",
            {"state": state, "code": "synthetic-registration-closed-code"},
        )

        self.assertEqual(response.status_code, 503)
        self.assertIn(b'data-error-code="registration_closed"', response.content)
        self.assertFalse(TelegramUser.objects.filter(tg_user_id=9021).exists())

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code", return_value="synthetic.jwt.value")
    def test_logout_revokes_current_oidc_device_session(self, exchange, verify) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import UserAuthSession

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9004",
            tg_user_id=9004,
            first_name="Device",
            last_name="Owner",
            username="device_owner",
            claims={"id": 9004, "sub": "stable-subject-9004", "exp": int(time.time()) + 3600},
        )
        client = Client()
        start = client.get("/app/auth/telegram/start")
        state = parse_qs(urlparse(start.headers["Location"]).query)["state"][0]
        callback = client.get(
            "/app/auth/telegram/callback",
            {"state": state, "code": "single-use-device-code"},
        )
        self.assertEqual(callback.status_code, 302)
        auth_session = UserAuthSession.objects.get(telegram_user_id=9004)
        self.assertIsNone(auth_session.revoked_at)

        logout = client.post(
            "/app/api/auth/logout",
            data="{}",
            content_type="application/json",
        )

        self.assertEqual(logout.status_code, 200)
        auth_session.refresh_from_db()
        self.assertIsNotNone(auth_session.revoked_at)
        self.assertNotIn("miniapp_tg_user_id", client.session)

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code")
    def test_logout_can_revoke_all_oidc_device_sessions(self, exchange, verify) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import UserAuthSession

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9005",
            tg_user_id=9005,
            first_name="Multi",
            last_name="Device",
            username="multi_device",
            claims={"id": 9005, "sub": "stable-subject-9005", "exp": int(time.time()) + 3600},
        )
        exchange.side_effect = ["synthetic.jwt.device-1", "synthetic.jwt.device-2"]
        clients = [Client(), Client()]
        for index, client in enumerate(clients):
            start = client.get("/app/auth/telegram/start")
            state = parse_qs(urlparse(start.headers["Location"]).query)["state"][0]
            callback = client.get(
                "/app/auth/telegram/callback",
                {"state": state, "code": f"single-use-device-code-{index}"},
            )
            self.assertEqual(callback.status_code, 302)
        self.assertEqual(UserAuthSession.objects.filter(telegram_user_id=9005, revoked_at__isnull=True).count(), 2)

        logout = clients[0].post(
            "/app/api/auth/logout",
            data=json.dumps({"all_devices": True}),
            content_type="application/json",
        )

        self.assertEqual(logout.status_code, 200)
        self.assertEqual(UserAuthSession.objects.filter(telegram_user_id=9005, revoked_at__isnull=True).count(), 0)
        profile = clients[1].get("/app/api/profile")
        self.assertEqual(profile.status_code, 401)
        self.assertEqual(profile.json()["error"]["code"], "session_required")

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code")
    def test_revoked_session_cannot_revoke_newer_device_sessions(self, exchange, verify) -> None:
        from django.conf import settings

        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import UserAuthSession

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9023",
            tg_user_id=9023,
            first_name="Stale",
            last_name="Session",
            username="stale_session",
            claims={"id": 9023, "sub": "stable-subject-9023", "exp": int(time.time()) + 3600},
        )
        exchange.side_effect = ["synthetic.jwt.stale-device", "synthetic.jwt.current-device"]

        stale_source = Client()
        start = stale_source.get("/app/auth/telegram/start")
        state = parse_qs(urlparse(start["Location"]).query)["state"][0]
        self.assertEqual(
            stale_source.get(
                "/app/auth/telegram/callback",
                {"state": state, "code": "synthetic-stale-device-code"},
            ).status_code,
            302,
        )
        stale_cookie = stale_source.cookies[settings.SESSION_COOKIE_NAME].value
        self.assertEqual(
            stale_source.post(
                "/app/api/auth/logout",
                data="{}",
                content_type="application/json",
            ).status_code,
            200,
        )

        current_client = Client()
        start = current_client.get("/app/auth/telegram/start")
        state = parse_qs(urlparse(start["Location"]).query)["state"][0]
        self.assertEqual(
            current_client.get(
                "/app/auth/telegram/callback",
                {"state": state, "code": "synthetic-current-device-code"},
            ).status_code,
            302,
        )
        current_session = UserAuthSession.objects.get(telegram_user_id=9023, revoked_at__isnull=True)

        attacker = Client()
        attacker.cookies[settings.SESSION_COOKIE_NAME] = stale_cookie
        response = attacker.post(
            "/app/api/auth/logout",
            data=json.dumps({"all_devices": True}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 401)
        current_session.refresh_from_db()
        self.assertIsNone(current_session.revoked_at)

    @override_settings(
        TELEGRAM_OIDC_CLIENT_ID="123456789",
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-oidc-secret-not-for-deployment",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
        SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
        ALLOWED_HOSTS=["testserver"],
        SECURE_SSL_REDIRECT=False,
    )
    @patch("miniapp.views.verify_id_token")
    @patch("miniapp.views.exchange_authorization_code", return_value="same.synthetic.jwt")
    def test_same_verified_id_token_cannot_open_two_sessions(self, exchange, verify) -> None:
        from miniapp.telegram_oidc import AuthenticatedTelegramIdentity
        from users.models import UserAuthSession

        verify.return_value = AuthenticatedTelegramIdentity(
            provider="telegram_oidc",
            subject="stable-subject-9006",
            tg_user_id=9006,
            first_name="Replay",
            last_name="Target",
            username="replay_target",
            claims={
                "id": 9006,
                "sub": "stable-subject-9006",
                "exp": int(time.time()) + 3600,
            },
        )
        clients = [Client(), Client()]
        callbacks = []
        for index, client in enumerate(clients):
            start = client.get("/app/auth/telegram/start")
            state = parse_qs(urlparse(start.headers["Location"]).query)["state"][0]
            callbacks.append(
                client.get(
                    "/app/auth/telegram/callback",
                    {"state": state, "code": f"provider-code-{index}"},
                )
            )

        self.assertEqual(callbacks[0].status_code, 302)
        self.assertEqual(callbacks[1].status_code, 400)
        self.assertIn(
            'data-error-code="telegram_oidc_token_replayed"',
            callbacks[1].content.decode("utf-8"),
        )
        self.assertEqual(UserAuthSession.objects.filter(telegram_user_id=9006).count(), 1)
