"""Exercise provider compression over real HTTP, including real RSA verification."""
from __future__ import annotations

import base64
import gzip
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from django.test import SimpleTestCase, override_settings

from miniapp.telegram_oidc import exchange_authorization_code, verify_id_token


@override_settings(TELEGRAM_OIDC_CLIENT_ID="transport-test-client")
class TelegramOidcTransportTests(SimpleTestCase):
    def test_gzipped_signing_keys_verify_a_valid_token(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        numbers = key.public_key().public_numbers()

        def encode_integer(value):
            return base64.urlsafe_b64encode(
                value.to_bytes((value.bit_length() + 7) // 8, "big")
            ).rstrip(b"=").decode("ascii")

        body = gzip.compress(json.dumps({"keys": [{
            "kty": "RSA", "alg": "RS256", "use": "sig", "kid": "gzip-key",
            "n": encode_integer(numbers.n), "e": encode_integer(numbers.e),
        }]}).encode())

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        now = int(time.time())
        token = jwt.encode({
            "iss": "https://oauth.telegram.org", "aud": "transport-test-client",
            "sub": "transport-subject", "id": 9001, "iat": now, "exp": now + 300,
            "nonce": "transport-nonce",
        }, key, algorithm="RS256", headers={"kid": "gzip-key"})
        try:
            with override_settings(TELEGRAM_OIDC_JWKS_URI=f"http://127.0.0.1:{server.server_port}/keys"):
                identity = verify_id_token(token, expected_nonce="transport-nonce")
            self.assertEqual(identity.tg_user_id, 9001)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    @override_settings(
        TELEGRAM_OIDC_CLIENT_SECRET="synthetic-transport-secret",
        TELEGRAM_OIDC_REDIRECT_URI="https://vydno.capital/app/auth/telegram/callback",
    )
    def test_gzipped_token_exchange_returns_id_token(self):
        body = gzip.compress(json.dumps({"id_token": "synthetic.transport.token"}).encode())

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with override_settings(TELEGRAM_OIDC_TOKEN_ENDPOINT=f"http://127.0.0.1:{server.server_port}/token"):
                token = exchange_authorization_code(code="synthetic-code", code_verifier="v" * 64)
            self.assertEqual(token, "synthetic.transport.token")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
