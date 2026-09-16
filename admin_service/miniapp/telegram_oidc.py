from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import jwt
from django.conf import settings
from django.db import IntegrityError
from django.http import HttpRequest
from jwt import InvalidTokenError, PyJWKClient
from jwt.exceptions import PyJWKClientError

from users.models import UserOidcTokenUse

OIDC_FLOW_SESSION_KEY = "miniapp_oidc_flow"
TELEGRAM_OIDC_AUTH_MODE = "telegram_oidc"
DEFAULT_RETURN_PATH = "/app/"


class TelegramOidcError(Exception):
    def __init__(self, message: str, *, code: str = "telegram_oidc_failed") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class PendingAuthorization:
    code: str
    code_verifier: str
    nonce: str
    return_to: str


@dataclass(frozen=True, slots=True)
class AuthenticatedTelegramIdentity:
    provider: str
    subject: str
    tg_user_id: int
    first_name: str
    last_name: str
    username: str
    claims: dict


def _required_setting(name: str) -> str:
    value = str(getattr(settings, name, "") or "").strip()
    if not value:
        raise TelegramOidcError(f"{name} is not configured.")
    return value


def safe_return_path(value: str | None) -> str:
    candidate = str(value or "").strip()
    if not candidate:
        return DEFAULT_RETURN_PATH
    parsed = urlsplit(candidate)
    if parsed.scheme or parsed.netloc or not parsed.path.startswith("/app/"):
        return DEFAULT_RETURN_PATH
    if parsed.path.startswith("/app/auth/telegram/"):
        return DEFAULT_RETURN_PATH
    return candidate


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def begin_authorization(request: HttpRequest, *, return_to: str | None = None, now: int | None = None) -> str:
    client_id = _required_setting("TELEGRAM_OIDC_CLIENT_ID")
    _required_setting("TELEGRAM_OIDC_CLIENT_SECRET")
    redirect_uri = _required_setting("TELEGRAM_OIDC_REDIRECT_URI")
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = _b64url(hashlib.sha256(code_verifier.encode("ascii")).digest())

    request.session[OIDC_FLOW_SESSION_KEY] = {
        "state_hash": hashlib.sha256(state.encode("ascii")).hexdigest(),
        "nonce": nonce,
        "code_verifier": code_verifier,
        "return_to": safe_return_path(return_to),
        "issued_at": int(time.time() if now is None else now),
    }

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": str(getattr(settings, "TELEGRAM_OIDC_SCOPES", "openid profile") or "openid profile"),
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    authorization_endpoint = str(
        getattr(settings, "TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT", "https://oauth.telegram.org/auth")
        or "https://oauth.telegram.org/auth"
    )
    return f"{authorization_endpoint}?{urlencode(params)}"


def consume_pending_authorization(
    request: HttpRequest,
    *,
    state: str | None,
    code: str | None,
    now: int | None = None,
) -> PendingAuthorization:
    flow = request.session.pop(OIDC_FLOW_SESSION_KEY, None)
    if not isinstance(flow, dict):
        raise TelegramOidcError("Login session is missing.", code="telegram_oidc_flow_missing")

    provided_state = str(state or "")
    expected_state_hash = str(flow.get("state_hash") or "")
    provided_state_hash = hashlib.sha256(provided_state.encode("utf-8")).hexdigest()
    if not provided_state or not expected_state_hash or not secrets.compare_digest(provided_state_hash, expected_state_hash):
        raise TelegramOidcError("Login state is invalid.", code="telegram_oidc_state_invalid")

    current = int(time.time() if now is None else now)
    try:
        issued_at = int(flow.get("issued_at") or 0)
    except (TypeError, ValueError) as exc:
        raise TelegramOidcError("Login session is invalid.", code="telegram_oidc_flow_invalid") from exc
    ttl = max(60, int(getattr(settings, "TELEGRAM_OIDC_FLOW_TTL_SECONDS", 600) or 600))
    if issued_at <= 0 or issued_at > current + 60 or current - issued_at >= ttl:
        raise TelegramOidcError("Login session has expired.", code="telegram_oidc_flow_expired")

    authorization_code = str(code or "").strip()
    code_verifier = str(flow.get("code_verifier") or "")
    nonce = str(flow.get("nonce") or "")
    if not authorization_code or len(code_verifier) < 43 or len(nonce) < 32:
        raise TelegramOidcError("Login response is invalid.", code="telegram_oidc_response_invalid")

    return PendingAuthorization(
        code=authorization_code,
        code_verifier=code_verifier,
        nonce=nonce,
        return_to=safe_return_path(flow.get("return_to")),
    )


def exchange_authorization_code(*, code: str, code_verifier: str) -> str:
    client_id = _required_setting("TELEGRAM_OIDC_CLIENT_ID")
    client_secret = _required_setting("TELEGRAM_OIDC_CLIENT_SECRET")
    redirect_uri = _required_setting("TELEGRAM_OIDC_REDIRECT_URI")
    token_endpoint = str(
        getattr(settings, "TELEGRAM_OIDC_TOKEN_ENDPOINT", "https://oauth.telegram.org/token")
        or "https://oauth.telegram.org/token"
    )
    form = urlencode(
        {
            "grant_type": "authorization_code",
            "code": str(code or ""),
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "code_verifier": str(code_verifier or ""),
        }
    ).encode("ascii")
    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode("ascii")
    token_request = Request(
        token_endpoint,
        data=form,
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    timeout = max(1, int(getattr(settings, "TELEGRAM_OIDC_HTTP_TIMEOUT_SECONDS", 10) or 10))
    try:
        with urlopen(token_request, timeout=timeout) as response:  # nosec B310 - fixed provider endpoint
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise TelegramOidcError("Telegram token exchange failed.", code="telegram_oidc_exchange_failed") from exc

    id_token = str(payload.get("id_token") or "") if isinstance(payload, dict) else ""
    if not id_token:
        raise TelegramOidcError("Telegram ID token is missing.", code="telegram_oidc_token_missing")
    return id_token


def verify_id_token(
    token: str,
    *,
    expected_nonce: str,
    jwks_client: PyJWKClient | None = None,
) -> AuthenticatedTelegramIdentity:
    raw_token = str(token or "")
    nonce = str(expected_nonce or "")
    if not raw_token or not nonce:
        raise TelegramOidcError("Telegram ID token is invalid.", code="telegram_oidc_token_invalid")

    client_id = _required_setting("TELEGRAM_OIDC_CLIENT_ID")
    issuer = str(
        getattr(settings, "TELEGRAM_OIDC_ISSUER", "https://oauth.telegram.org")
        or "https://oauth.telegram.org"
    )
    allowed_algorithms = tuple(
        getattr(settings, "TELEGRAM_OIDC_ALLOWED_ALGORITHMS", ("RS256",)) or ("RS256",)
    )
    try:
        header = jwt.get_unverified_header(raw_token)
        if str(header.get("alg") or "") not in allowed_algorithms:
            raise TelegramOidcError("Telegram ID token algorithm is not allowed.", code="telegram_oidc_token_invalid")
        keys = jwks_client or PyJWKClient(
            str(
                getattr(settings, "TELEGRAM_OIDC_JWKS_URI", "https://oauth.telegram.org/.well-known/jwks.json")
                or "https://oauth.telegram.org/.well-known/jwks.json"
            ),
            cache_jwk_set=True,
            lifespan=300,
        )
        signing_key = keys.get_signing_key_from_jwt(raw_token)
        claims = jwt.decode(
            raw_token,
            signing_key.key,
            algorithms=list(allowed_algorithms),
            audience=client_id,
            issuer=issuer,
            leeway=60,
            options={"require": ["iss", "aud", "sub", "iat", "exp", "id"]},
        )
    except TelegramOidcError:
        raise
    except (InvalidTokenError, PyJWKClientError, TypeError, ValueError) as exc:
        raise TelegramOidcError("Telegram ID token is invalid.", code="telegram_oidc_token_invalid") from exc

    token_nonce = str(claims.get("nonce") or "")
    if token_nonce and not secrets.compare_digest(token_nonce, nonce):
        raise TelegramOidcError("Telegram ID token nonce is invalid.", code="telegram_oidc_nonce_invalid")

    subject = str(claims.get("sub") or "").strip()
    try:
        tg_user_id = int(claims.get("id") or 0)
    except (TypeError, ValueError) as exc:
        raise TelegramOidcError("Telegram user ID is invalid.", code="telegram_oidc_identity_invalid") from exc
    if not subject or len(subject) > 255 or tg_user_id <= 0:
        raise TelegramOidcError("Telegram identity is invalid.", code="telegram_oidc_identity_invalid")

    first_name = str(claims.get("given_name") or claims.get("name") or "").strip()
    return AuthenticatedTelegramIdentity(
        provider=TELEGRAM_OIDC_AUTH_MODE,
        subject=subject,
        tg_user_id=tg_user_id,
        first_name=first_name,
        last_name=str(claims.get("family_name") or "").strip(),
        username=str(claims.get("preferred_username") or "").strip().lstrip("@"),
        claims=dict(claims),
    )


def consume_id_token_once(token: str, identity: AuthenticatedTelegramIdentity) -> None:
    raw_token = str(token or "")
    try:
        expires_at = int(identity.claims.get("exp") or 0)
    except (TypeError, ValueError) as exc:
        raise TelegramOidcError("Telegram ID token expiry is invalid.", code="telegram_oidc_token_invalid") from exc
    if not raw_token or expires_at <= int(time.time()):
        raise TelegramOidcError("Telegram ID token expiry is invalid.", code="telegram_oidc_token_invalid")
    try:
        UserOidcTokenUse.objects.create(
            token_hash=hashlib.sha256(raw_token.encode("utf-8")).hexdigest(),
            provider=identity.provider,
            subject=identity.subject,
            tg_user_id=identity.tg_user_id,
            expires_at=datetime.fromtimestamp(expires_at, tz=UTC),
        )
    except IntegrityError as exc:
        raise TelegramOidcError(
            "Telegram ID token was already used.",
            code="telegram_oidc_token_replayed",
        ) from exc
