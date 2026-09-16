from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qsl

from django.conf import settings
from django.db import IntegrityError
from django.http import HttpRequest
from django.utils import timezone

from miniapp.models import BrowserLoginTokenUse
from users.models import TelegramUser, UserAuthIdentity, UserAuthSession

SESSION_USER_ID_KEY = "miniapp_tg_user_id"
SESSION_AUTH_AT_KEY = "miniapp_auth_at"
SESSION_AUTH_MODE_KEY = "miniapp_auth_mode"
SESSION_RECORD_TOKEN_KEY = "miniapp_auth_session_token"
BROWSER_AUTH_MODE = "browser"
TELEGRAM_AUTH_MODE = "telegram"
BROWSER_LOGIN_TOKEN_VERSION = 1
DEFAULT_BROWSER_LOGIN_TOKEN_TTL_SECONDS = 15 * 60
DEFAULT_BROWSER_SESSION_AGE_SECONDS = 180 * 24 * 60 * 60


class MiniAppAuthError(Exception):
    pass


class MiniAppSessionError(MiniAppAuthError):
    pass


@dataclass(slots=True)
class TelegramMiniAppIdentity:
    tg_user_id: int
    first_name: str
    last_name: str
    username: str
    raw_user: dict
    auth_date: int


@dataclass(slots=True)
class BrowserLoginIdentity:
    tg_user_id: int
    expires_at: int
    issued_at: int
    nonce: str


def _build_data_check_string(payload: dict[str, str]) -> str:
    return "\n".join(f"{key}={value}" for key, value in sorted(payload.items()))


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode((value + padding).encode("ascii"), altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise MiniAppAuthError("Browser login token invalid.") from exc
    # Reject padding, alternate alphabets and unused pad bits: one byte string
    # must have exactly one accepted spelling for the one-use token hash.
    if _b64url_encode(decoded) != value:
        raise MiniAppAuthError("Browser login token invalid.")
    return decoded


def _browser_login_secret() -> bytes:
    secret = str(getattr(settings, "MINIAPP_BROWSER_LOGIN_SECRET", "") or settings.TELEGRAM_BOT_TOKEN or "").strip()
    if not secret:
        raise MiniAppAuthError("Browser login secret not configured.")
    return hmac.new(b"MiniAppBrowserLogin", secret.encode("utf-8"), hashlib.sha256).digest()


def _browser_login_token_ttl_seconds() -> int:
    value = int(getattr(settings, "MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS", DEFAULT_BROWSER_LOGIN_TOKEN_TTL_SECONDS) or DEFAULT_BROWSER_LOGIN_TOKEN_TTL_SECONDS)
    return max(60, min(value, 60 * 60))


def browser_session_age_seconds() -> int:
    value = int(getattr(settings, "MINIAPP_BROWSER_SESSION_AGE_SECONDS", DEFAULT_BROWSER_SESSION_AGE_SECONDS) or DEFAULT_BROWSER_SESSION_AGE_SECONDS)
    return max(24 * 60 * 60, value)


def _browser_login_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def build_browser_login_token(tg_user_id: int, *, now: int | None = None, nonce: str | None = None) -> str:
    try:
        user_id = int(tg_user_id)
    except (TypeError, ValueError) as exc:
        raise MiniAppAuthError("Browser login user id invalid.") from exc
    if user_id <= 0:
        raise MiniAppAuthError("Browser login user id missing.")

    issued_at = int(time.time() if now is None else now)
    expires_at = issued_at + _browser_login_token_ttl_seconds()
    payload = {
        "v": BROWSER_LOGIN_TOKEN_VERSION,
        "tg_user_id": user_id,
        "iat": issued_at,
        "exp": expires_at,
        "nonce": nonce or secrets.token_urlsafe(18),
    }
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = hmac.new(_browser_login_secret(), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url_encode(signature)}"


def validate_browser_login_token(token: str, *, now: int | None = None) -> BrowserLoginIdentity:
    raw_token = str(token or "")
    if not raw_token or "." not in raw_token:
        raise MiniAppAuthError("Browser login token missing.")

    try:
        payload_b64, signature_b64 = raw_token.split(".", 1)
    except ValueError as exc:
        raise MiniAppAuthError("Browser login token invalid.") from exc
    if not payload_b64 or not signature_b64:
        raise MiniAppAuthError("Browser login token invalid.")

    payload_bytes = _b64url_decode(payload_b64)
    expected_signature = hmac.new(_browser_login_secret(), payload_b64.encode("ascii"), hashlib.sha256).digest()
    provided_signature = _b64url_decode(signature_b64)
    if not hmac.compare_digest(expected_signature, provided_signature):
        raise MiniAppAuthError("Browser login signature invalid.")

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise MiniAppAuthError("Browser login payload invalid.") from exc

    if int(payload.get("v") or 0) != BROWSER_LOGIN_TOKEN_VERSION:
        raise MiniAppAuthError("Browser login token version invalid.")
    try:
        tg_user_id = int(payload.get("tg_user_id") or 0)
        issued_at = int(payload.get("iat") or 0)
        expires_at = int(payload.get("exp") or 0)
    except (TypeError, ValueError) as exc:
        raise MiniAppAuthError("Browser login payload invalid.") from exc
    nonce = str(payload.get("nonce") or "").strip()
    if tg_user_id <= 0 or issued_at <= 0 or expires_at <= 0 or len(nonce) < 16:
        raise MiniAppAuthError("Browser login payload invalid.")

    current = int(time.time() if now is None else now)
    if expires_at <= current:
        raise MiniAppAuthError("Browser login link expired.")
    if issued_at > current + 60:
        raise MiniAppAuthError("Browser login token issued in the future.")
    if expires_at - issued_at > _browser_login_token_ttl_seconds():
        raise MiniAppAuthError("Browser login token lifetime invalid.")

    return BrowserLoginIdentity(tg_user_id=tg_user_id, expires_at=expires_at, issued_at=issued_at, nonce=nonce)


def validate_telegram_init_data(init_data: str) -> TelegramMiniAppIdentity:
    if not settings.TELEGRAM_BOT_TOKEN:
        raise MiniAppAuthError("TELEGRAM_BOT_TOKEN not configured.")

    parsed = dict(parse_qsl(init_data or "", keep_blank_values=True))
    provided_hash = str(parsed.pop("hash", "") or "").strip().lower()
    if not provided_hash:
        raise MiniAppAuthError("Telegram hash missing.")

    user_raw = str(parsed.get("user") or "")
    if not user_raw:
        raise MiniAppAuthError("Telegram user payload missing.")

    try:
        auth_date = int(parsed.get("auth_date") or 0)
    except (TypeError, ValueError) as exc:
        raise MiniAppAuthError("Telegram auth_date invalid.") from exc
    if auth_date <= 0:
        raise MiniAppAuthError("Telegram auth_date missing.")

    max_age = int(getattr(settings, "MINIAPP_TELEGRAM_AUTH_MAX_AGE", 86400) or 86400)
    if max_age > 0 and int(time.time()) - auth_date > max_age:
        raise MiniAppAuthError("Telegram auth payload expired.")

    secret_key = hmac.new(b"WebAppData", settings.TELEGRAM_BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    calculated_hash = hmac.new(
        secret_key,
        _build_data_check_string(parsed).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(calculated_hash, provided_hash):
        raise MiniAppAuthError("Telegram signature invalid.")

    try:
        user_payload = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise MiniAppAuthError("Telegram user payload invalid.") from exc

    try:
        tg_user_id = int(user_payload.get("id") or 0)
    except (TypeError, ValueError) as exc:
        raise MiniAppAuthError("Telegram user id invalid.") from exc
    if tg_user_id <= 0:
        raise MiniAppAuthError("Telegram user id missing.")

    return TelegramMiniAppIdentity(
        tg_user_id=tg_user_id,
        first_name=str(user_payload.get("first_name") or ""),
        last_name=str(user_payload.get("last_name") or ""),
        username=str(user_payload.get("username") or ""),
        raw_user=user_payload,
        auth_date=auth_date,
    )


def _clear_miniapp_session(request: HttpRequest) -> None:
    # Keep unrelated Django/admin state, never another Mini App actor's data.
    for key in list(request.session.keys()):
        if str(key).startswith("miniapp_"):
            request.session.pop(key, None)


def login_session(
    request: HttpRequest,
    user: TelegramUser,
    *,
    session_age_seconds: int | None = None,
    auth_mode: str = TELEGRAM_AUTH_MODE,
    identity: UserAuthIdentity | None = None,
) -> None:
    previous_session_token = str(request.session.get(SESSION_RECORD_TOKEN_KEY) or "")
    if previous_session_token:
        UserAuthSession.objects.filter(
            token_hash=hashlib.sha256(previous_session_token.encode("utf-8")).hexdigest(),
            revoked_at__isnull=True,
        ).update(revoked_at=timezone.now())
    if str(request.session.get(SESSION_USER_ID_KEY) or "") != str(user.tg_user_id):
        _clear_miniapp_session(request)
    request.session.cycle_key()
    request.session[SESSION_USER_ID_KEY] = int(user.tg_user_id)
    request.session[SESSION_AUTH_AT_KEY] = int(time.time())
    request.session[SESSION_AUTH_MODE_KEY] = auth_mode
    request.session.set_expiry(int(session_age_seconds) if session_age_seconds is not None else None)
    if auth_mode == "telegram_oidc":
        now = timezone.now()
        age = int(session_age_seconds or browser_session_age_seconds())
        session_token = secrets.token_urlsafe(32)
        request.session[SESSION_RECORD_TOKEN_KEY] = session_token
        UserAuthSession.objects.create(
            telegram_user_id=int(user.tg_user_id),
            identity=identity,
            token_hash=hashlib.sha256(session_token.encode("utf-8")).hexdigest(),
            auth_mode=auth_mode,
            user_agent_hash=hashlib.sha256(
                str(request.META.get("HTTP_USER_AGENT") or "").encode("utf-8")
            ).hexdigest(),
            expires_at=now + timedelta(seconds=age),
            last_seen_at=now,
        )


def logout_session(request: HttpRequest, *, all_devices: bool = False) -> None:
    session_token = str(request.session.get(SESSION_RECORD_TOKEN_KEY) or "")
    try:
        tg_user_id = int(request.session.get(SESSION_USER_ID_KEY) or 0)
    except (TypeError, ValueError):
        tg_user_id = 0
    if all_devices and tg_user_id > 0:
        UserAuthSession.objects.filter(
            telegram_user_id=tg_user_id,
            revoked_at__isnull=True,
        ).update(revoked_at=timezone.now())
    elif session_token:
        UserAuthSession.objects.filter(
            token_hash=hashlib.sha256(session_token.encode("utf-8")).hexdigest(),
            revoked_at__isnull=True,
        ).update(revoked_at=timezone.now())
    _clear_miniapp_session(request)
    request.session.set_expiry(None)
    request.session.cycle_key()


def get_session_tg_user_id(request: HttpRequest) -> int:
    raw = request.session.get(SESSION_USER_ID_KEY)
    try:
        tg_user_id = int(raw or 0)
    except (TypeError, ValueError) as exc:
        raise MiniAppSessionError("Mini App session invalid.") from exc
    if tg_user_id <= 0:
        raise MiniAppSessionError("Mini App session missing.")
    return tg_user_id


def get_session_user(request: HttpRequest) -> TelegramUser:
    tg_user_id = get_session_tg_user_id(request)
    user = TelegramUser.objects.filter(tg_user_id=tg_user_id).first()
    if user is None:
        raise MiniAppSessionError("Mini App user not found.")
    auth_mode = request.session.get(SESSION_AUTH_MODE_KEY)
    if auth_mode == "telegram_oidc":
        session_token = str(request.session.get(SESSION_RECORD_TOKEN_KEY) or "")
        if not session_token or not UserAuthSession.objects.filter(
            token_hash=hashlib.sha256(session_token.encode("utf-8")).hexdigest(),
            telegram_user_id=tg_user_id,
            auth_mode=auth_mode,
            revoked_at__isnull=True,
            expires_at__gt=timezone.now(),
        ).exists():
            raise MiniAppSessionError("Mini App session revoked.")
    if auth_mode in {BROWSER_AUTH_MODE, "telegram_oidc"}:
        request.session.set_expiry(browser_session_age_seconds())
    return user


def consume_browser_login_token(token: str) -> TelegramUser:
    # Validation admits only the canonical signed spelling. Keep its existing
    # digest so already-consumed canonical links remain blocked after upgrades.
    token = str(token or "")
    identity = validate_browser_login_token(token)
    user = TelegramUser.objects.filter(tg_user_id=identity.tg_user_id).first()
    if user is None:
        raise MiniAppAuthError("Browser login user not found.")

    try:
        BrowserLoginTokenUse.objects.create(
            token_hash=_browser_login_token_hash(token),
            tg_user_id=identity.tg_user_id,
            expires_at=datetime.fromtimestamp(identity.expires_at, tz=UTC),
        )
    except IntegrityError as exc:
        raise MiniAppAuthError("Browser login link already used.") from exc

    return user


def dev_login_user(request: HttpRequest) -> TelegramUser:
    if not settings.DEBUG:
        raise MiniAppAuthError("Dev Mini App auth disabled.")
    user_id = getattr(settings, "MINIAPP_DEV_TG_USER_ID", None)
    if not user_id:
        raise MiniAppAuthError("MINIAPP_DEV_TG_USER_ID not configured.")
    user = TelegramUser.objects.filter(tg_user_id=int(user_id)).first()
    if user is None:
        raise MiniAppAuthError("Configured dev Mini App user not found.")
    login_session(request, user)
    return user
