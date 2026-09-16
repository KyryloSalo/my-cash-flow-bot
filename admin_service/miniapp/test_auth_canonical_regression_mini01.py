"""MINI-01 regressions with synthetic identities/secrets and no database.

Run under the sanitized remediation runner (PYTHON_DOTENV_DISABLED=1), e.g.:
    python -m unittest miniapp.test_auth_canonical_regression_mini01 -v

The real auth implementation is exercised. Only ORM persistence is replaced by
an in-memory unique-hash seam; this does not prove PostgreSQL concurrency.
"""
from __future__ import annotations

import base64
from datetime import UTC, datetime
import hashlib
import hmac
import json
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote

import django
from django.apps import apps

if not apps.ready:
    django.setup()

from django.db import IntegrityError
from django.test import Client, SimpleTestCase, override_settings

from miniapp import auth


SYNTHETIC_SECRET = "mini01-synthetic-browser-secret-not-for-deployment"
NOW = 1_700_000_000
NONCE = "mini01-synthetic-nonce-0001"
URLSAFE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"


def encode_component(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def decode_component(value: str) -> bytes:
    """Permissive reference decoder, deliberately independent of auth.py."""
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def equivalent_signatures(signature: str) -> dict[str, str]:
    variants = {
        "padding_one": signature + "=",
        "padding_two": signature + "==",
        "ignored_non_alphabet": signature[:4] + "!!!!" + signature[4:],
        "ignored_whitespace": signature[:4] + "\t\r\n " + signature[4:],
    }
    last = URLSAFE_ALPHABET.index(signature[-1])
    for pad_bits in (1, 2, 3):
        variants[f"unused_pad_bits_{pad_bits}"] = (
            signature[:-1] + URLSAFE_ALPHABET[last | pad_bits]
        )
    standard_alphabet = signature.translate(str.maketrans("-_", "+/"))
    if standard_alphabet != signature:
        variants["standard_base64_alphabet"] = standard_alphabet
    return variants


def sign_payload_component(payload_b64: str) -> str:
    """Sign a deliberately malformed component using only a synthetic secret."""
    key = hmac.new(b"MiniAppBrowserLogin", SYNTHETIC_SECRET.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{encode_component(signature)}"


@override_settings(
    MINIAPP_BROWSER_LOGIN_SECRET=SYNTHETIC_SECRET,
    MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS=900,
    MINIAPP_BROWSER_SESSION_AGE_SECONDS=123456,
    TELEGRAM_BOT_TOKEN="mini01-synthetic-telegram-token-not-for-deployment",
    SECRET_KEY="mini01-synthetic-django-secret-not-for-deployment",
    SECRET_KEY_FALLBACKS=[],
    ALLOWED_HOSTS=["testserver"],
    SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
    SECURE_SSL_REDIRECT=False,
)
class BrowserLoginCanonicalRegressionTests(SimpleTestCase):
    def setUp(self) -> None:
        self.users = {
            uid: SimpleNamespace(tg_user_id=uid, first_name="Synthetic", lang="en", base_currency="UAH")
            for uid in (900001, 900002)
        }
        self.uses: dict[str, dict] = {}
        self.clock = self.enterContext(patch.object(auth.time, "time", return_value=NOW))
        self.user_lookup = self.enterContext(
            patch.object(auth.TelegramUser.objects, "filter", side_effect=self._lookup_user)
        )
        self.use_create = self.enterContext(
            patch.object(auth.BrowserLoginTokenUse.objects, "create", side_effect=self._consume_hash)
        )
        self.token = auth.build_browser_login_token(900001, now=NOW, nonce=NONCE)

    def _lookup_user(self, *, tg_user_id: int) -> SimpleNamespace:
        return SimpleNamespace(first=lambda: self.users.get(tg_user_id))

    def _consume_hash(self, **row) -> SimpleNamespace:
        if row["token_hash"] in self.uses:
            raise IntegrityError("synthetic unique token_hash constraint")
        self.uses[row["token_hash"]] = row
        return SimpleNamespace(**row)

    def test_equivalent_signature_spelling_cannot_replay_consumed_token(self) -> None:
        payload, signature = self.token.split(".")
        self.assertIs(auth.consume_browser_login_token(self.token), self.users[900001])
        accepted = []
        for label, alternate in equivalent_signatures(signature).items():
            self.assertNotEqual(alternate, signature)
            self.assertEqual(decode_component(alternate), decode_component(signature))
            try:
                auth.consume_browser_login_token(f"{payload}.{alternate}")
            except auth.MiniAppAuthError:
                pass
            else:
                accepted.append(label)
        self.assertEqual(accepted, [], "Equivalent encodings opened additional logins")
        self.assertEqual(len(self.uses), 1)

    def test_noncanonical_signature_is_rejected_before_any_consumption(self) -> None:
        payload, signature = self.token.split(".")
        for label, alternate in equivalent_signatures(signature).items():
            with self.subTest(encoding=label):
                with self.assertRaises(auth.MiniAppAuthError):
                    auth.validate_browser_login_token(f"{payload}.{alternate}", now=NOW)
        self.assertEqual(self.uses, {})

    def test_noncanonical_payload_is_rejected_even_with_valid_synthetic_hmac(self) -> None:
        payload, _ = self.token.split(".")
        # The signing key is synthetic; no real bearer token is read or forged.
        variants = [payload + "=", payload[:4] + "!!!!" + payload[4:]]
        for alternate in variants:
            with self.subTest(encoding=alternate[-8:]):
                self.assertEqual(decode_component(alternate), decode_component(payload))
                with self.assertRaises(auth.MiniAppAuthError):
                    auth.validate_browser_login_token(sign_payload_component(alternate), now=NOW)

    def test_outer_whitespace_cannot_create_another_consumption_hash(self) -> None:
        auth.consume_browser_login_token(self.token)
        for alternate in (" " + self.token, self.token + " ", "\t" + self.token + "\n"):
            with self.subTest(whitespace=repr(alternate[:1]) + repr(alternate[-1:])):
                with self.assertRaises(auth.MiniAppAuthError):
                    auth.consume_browser_login_token(alternate)
        self.assertEqual(len(self.uses), 1)

    def test_canonical_token_keeps_signed_identity_and_expiry(self) -> None:
        identity = auth.validate_browser_login_token(self.token, now=NOW + 120)
        self.assertEqual(identity.tg_user_id, 900001)
        self.assertEqual(identity.nonce, NONCE)
        self.assertEqual(identity.issued_at, NOW)
        self.assertEqual(identity.expires_at, NOW + 900)
        self.assertIs(auth.consume_browser_login_token(self.token), self.users[900001])
        self.assertEqual(
            list(self.uses.values()),
            [{
                "token_hash": hashlib.sha256(self.token.encode("utf-8")).hexdigest(),
                "tg_user_id": 900001,
                "expires_at": datetime.fromtimestamp(NOW + 900, tz=UTC),
            }],
        )

    def test_exact_replay_is_rejected(self) -> None:
        auth.consume_browser_login_token(self.token)
        with self.assertRaisesRegex(auth.MiniAppAuthError, "already used"):
            auth.consume_browser_login_token(self.token)
        self.assertEqual(len(self.uses), 1)

    def test_pre_upgrade_canonical_consumption_hash_remains_blocked(self) -> None:
        old_hash = hashlib.sha256(self.token.encode("utf-8")).hexdigest()
        self.uses[old_hash] = {"token_hash": old_hash, "tg_user_id": 900001}
        with self.assertRaisesRegex(auth.MiniAppAuthError, "already used"):
            auth.consume_browser_login_token(self.token)
        self.assertEqual(list(self.uses), [old_hash])

    def test_rejected_encoding_does_not_burn_original_token(self) -> None:
        with self.assertRaises(auth.MiniAppAuthError):
            auth.consume_browser_login_token(self.token + "=")
        self.assertEqual(self.uses, {})
        self.user_lookup.assert_not_called()
        self.assertIs(auth.consume_browser_login_token(self.token), self.users[900001])

    def test_actor_and_nonce_cannot_be_rebound_without_the_signature(self) -> None:
        payload_b64, signature = self.token.split(".")
        payload = json.loads(decode_component(payload_b64))
        for field, changed in (("tg_user_id", 900002), ("nonce", "another-synthetic-nonce-0002")):
            with self.subTest(field=field):
                modified = dict(payload, **{field: changed})
                encoded = encode_component(json.dumps(modified, separators=(",", ":"), sort_keys=True).encode())
                with self.assertRaisesRegex(auth.MiniAppAuthError, "signature invalid"):
                    auth.consume_browser_login_token(f"{encoded}.{signature}")
        self.user_lookup.assert_not_called()
        self.assertEqual(self.uses, {})
        self.assertIs(auth.consume_browser_login_token(self.token), self.users[900001])

    def test_new_nonce_produces_an_independent_one_use_token_for_same_actor(self) -> None:
        another = auth.build_browser_login_token(900001, now=NOW, nonce="another-synthetic-nonce-0002")
        self.assertNotEqual(self.token, another)
        for token in (self.token, another):
            self.assertIs(auth.consume_browser_login_token(token), self.users[900001])
            with self.assertRaisesRegex(auth.MiniAppAuthError, "already used"):
                auth.consume_browser_login_token(token)
        self.assertEqual(len(self.uses), 2)

    def test_secret_rotation_rejects_old_token_before_consumption(self) -> None:
        with override_settings(MINIAPP_BROWSER_LOGIN_SECRET="mini01-rotated-synthetic-secret"):
            with self.assertRaisesRegex(auth.MiniAppAuthError, "signature invalid"):
                auth.consume_browser_login_token(self.token)
            self.assertEqual(self.uses, {})
            self.user_lookup.assert_not_called()
            current = auth.build_browser_login_token(900001, now=NOW, nonce=NONCE)
            self.assertNotEqual(current, self.token)
            self.assertIs(auth.consume_browser_login_token(current), self.users[900001])

    def test_telegram_secret_fallback_stays_compatible(self) -> None:
        with override_settings(MINIAPP_BROWSER_LOGIN_SECRET=""):
            fallback_token = auth.build_browser_login_token(900001, now=NOW, nonce=NONCE)
            self.assertIs(auth.consume_browser_login_token(fallback_token), self.users[900001])
            with self.assertRaisesRegex(auth.MiniAppAuthError, "signature invalid"):
                auth.consume_browser_login_token(self.token)
        self.assertEqual(len(self.uses), 1)

    def test_missing_secret_fails_closed(self) -> None:
        with override_settings(MINIAPP_BROWSER_LOGIN_SECRET="", TELEGRAM_BOT_TOKEN=""):
            with self.assertRaisesRegex(auth.MiniAppAuthError, "not configured"):
                auth.build_browser_login_token(900001, now=NOW, nonce=NONCE)
            with self.assertRaisesRegex(auth.MiniAppAuthError, "not configured"):
                auth.consume_browser_login_token(self.token)
        self.assertEqual(self.uses, {})

    def test_expiry_boundary_stays_exclusive(self) -> None:
        auth.validate_browser_login_token(self.token, now=NOW + 899)
        self.clock.return_value = NOW + 900
        with self.assertRaisesRegex(auth.MiniAppAuthError, "expired"):
            auth.consume_browser_login_token(self.token)
        self.user_lookup.assert_not_called()
        self.assertEqual(self.uses, {})

    def test_future_issue_time_is_rejected(self) -> None:
        future = auth.build_browser_login_token(900001, now=NOW + 61, nonce=NONCE)
        with self.assertRaisesRegex(auth.MiniAppAuthError, "future"):
            auth.consume_browser_login_token(future)
        self.assertEqual(self.uses, {})

    def test_lifetime_larger_than_current_ttl_is_rejected(self) -> None:
        with override_settings(MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS=1800):
            long_lived = auth.build_browser_login_token(900001, now=NOW, nonce=NONCE)
        with self.assertRaisesRegex(auth.MiniAppAuthError, "lifetime invalid"):
            auth.consume_browser_login_token(long_lived)
        self.assertEqual(self.uses, {})

    def test_unknown_user_does_not_create_a_consumption_record(self) -> None:
        unknown = auth.build_browser_login_token(900003, now=NOW, nonce=NONCE)
        with self.assertRaisesRegex(auth.MiniAppAuthError, "user not found"):
            auth.consume_browser_login_token(unknown)
        self.assertEqual(self.uses, {})

    def test_malformed_components_raise_controlled_auth_error(self) -> None:
        payload, signature = self.token.split(".")
        for token in ("", "no-dot", ".", payload + ".", "." + signature,
                      "\u044f." + signature, payload + ".\u044f", payload + ".A", self.token + ".junk"):
            with self.subTest(token_kind=token[-6:]):
                with self.assertRaises(auth.MiniAppAuthError):
                    auth.consume_browser_login_token(token)
        self.assertEqual(self.uses, {})

    def test_current_and_legacy_routes_share_one_use_boundary(self) -> None:
        routes = ("/app/api/browser-login/", "/app/login/")
        payload, signature = self.token.split(".")
        alternate_signatures = equivalent_signatures(signature)
        for primary, secondary in (routes, routes[::-1]):
            with self.subTest(first_route=primary):
                self.uses.clear()
                first_client = Client()
                response = first_client.get(f"{primary}{self.token}/?tg_user_id=900002", secure=True)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response["Location"], "/app/")
                self.assertEqual(first_client.session[auth.SESSION_USER_ID_KEY], 900001)
                self.assertEqual(first_client.session[auth.SESSION_AUTH_MODE_KEY], auth.BROWSER_AUTH_MODE)
                self.assertEqual(first_client.session.get_expiry_age(), 123456)
                variants = [self.token] + [
                    f"{payload}.{alternate_signatures[name]}"
                    for name in ("padding_one", "padding_two", "ignored_non_alphabet", "unused_pad_bits_1")
                ]
                for token in variants:
                    second_client = Client()
                    denied = second_client.get(f"{secondary}{quote(token, safe='')}/", secure=True)
                    self.assertEqual(denied.status_code, 400)
                    self.assertNotIn(auth.SESSION_USER_ID_KEY, second_client.session)
                    self.assertIn("no-store", denied["Cache-Control"])
                self.assertEqual(len(self.uses), 1)

    def test_handoff_remains_non_consuming_and_preserves_install_intent(self) -> None:
        client = Client()
        handoff = client.get(f"/app/browser-login/{self.token}/?install=1", secure=True)
        self.assertEqual(handoff.status_code, 200)
        self.assertIn(f"/app/api/browser-login/{self.token}/?install=1", handoff.content.decode())
        self.assertEqual(self.uses, {})
        self.assertNotIn(auth.SESSION_USER_ID_KEY, client.session)
        response = client.get(f"/app/api/browser-login/{self.token}/?install=1", secure=True)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], "/app/?install=1")
        self.assertEqual(client.session[auth.SESSION_USER_ID_KEY], 900001)
        self.assertEqual(len(self.uses), 1)

    def test_both_consumer_routes_still_reject_post_without_consuming(self) -> None:
        for route in ("/app/api/browser-login/", "/app/login/"):
            with self.subTest(route=route):
                response = Client().post(f"{route}{self.token}/", secure=True)
                self.assertEqual(response.status_code, 405)
        self.assertEqual(self.uses, {})
