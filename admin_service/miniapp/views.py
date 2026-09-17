from __future__ import annotations

import hashlib
import json
import secrets
from datetime import timedelta
from html import escape
from pathlib import Path
from urllib.parse import quote

from django.conf import settings
from django.db import transaction as db_transaction
from django.db.models import Q
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.templatetags.static import static
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST, require_http_methods

from miniapp.auth import (
    BROWSER_AUTH_MODE,
    SESSION_AUTH_MODE_KEY,
    MiniAppAuthError,
    MiniAppSessionError,
    browser_session_age_seconds,
    build_browser_login_token,
    consume_browser_login_token,
    dev_login_user,
    get_session_user,
    login_session,
    logout_session,
    validate_telegram_init_data,
)
from miniapp.ai_text_client import MiniAppAiTextError, request_ai_text_parse
from miniapp.ai_image_client import MiniAppAiImageError, request_ai_image_parse
from miniapp.ai_voice_client import MiniAppAiVoiceError, request_ai_voice_parse
from miniapp.export_client import MiniAppExportError, request_export_csv, request_export_xlsx
from miniapp.family import (
    MiniAppFamilyError,
    accept_family_invite,
    build_family_payload,
    create_family,
    create_family_invite,
    leave_family,
    remove_family_member,
    revoke_family_invite,
)
from miniapp.funnel import FunnelValidationError, record_request_server_event
from miniapp.savings_tasks import (
    MiniAppSavingTaskError,
    build_saving_task_draft,
    build_saving_tasks_payload,
    commit_saving_task_draft,
)
from miniapp.help_client import MiniAppHelpError, request_help_content
from miniapp.history import (
    MiniAppHistoryError,
    TransactionCancellationService,
    build_history_edit_draft,
    build_history_payload,
    build_recent_cancellable_transactions,
    commit_history_edit_draft,
)
from miniapp.image_uploads import MiniAppImageUploadError, normalize_ai_image_upload
from miniapp.services import (
    AI_IMAGE_MAX_BYTES,
    AI_TEXT_MAX_LENGTH,
    AI_VOICE_MAX_BYTES,
    build_account_draft,
    build_account_options,
    build_accounts_preview,
    build_activity_preview,
    build_ai_image_batch,
    build_ai_image_catalog,
    build_ai_image_suggestion,
    build_ai_text_suggestion,
    build_billing_expense_options,
    build_bootstrap,
    build_category_catalog,
    build_categories_preview,
    build_credit_cards_preview,
    build_debt_draft,
    build_debt_options,
    build_debts_preview,
    build_flow_preview,
    build_goals_preview,
    build_investments_preview,
    build_transaction_draft,
    build_transaction_options,
    build_transfer_draft,
    commit_account_draft,
    commit_debt_draft,
    commit_transaction_draft,
    commit_transfer_draft,
    detect_ai_image_mime,
    detect_ai_voice_mime,
    guard_ai_text_intent,
    locale_for_user,
    MiniAppTransactionError,
    mt,
    normalize_locale,
    profile_settings_payload,
    resolve_access,
    resolve_period,
    SUPPORTED_REMINDER_HOURS,
    SUPPORTED_REMINDER_MODES,
    update_profile_settings,
    update_saving_prompt_settings,
)
from miniapp.models import (
    AiTransactionDraft,
    AppNotification,
    DailyExpenseReminderSetting,
    DraftAction,
    InstallNudgeState,
    SavingPromptSetting,
    WebPushSubscription,
    WriteReceipt,
)
from miniapp.onboarding import build_onboarding_draft, commit_onboarding_draft, onboarding_status
from miniapp.push import (
    EVENTS,
    emit_notification,
    preferences_payload,
    serialize_notification,
    unread_badge_count,
    update_preferences,
)
from miniapp.telegram_oidc import (
    DEFAULT_RETURN_PATH,
    TELEGRAM_OIDC_AUTH_MODE,
    TelegramOidcError,
    begin_authorization,
    consume_id_token_once,
    consume_pending_authorization,
    exchange_authorization_code,
    verify_id_token,
)
from miniapp.operator_hub import (
    OperatorUserError,
    build_operator_payload,
    build_operator_preview_payload,
    build_operator_preview_user_detail,
    build_operator_user_detail,
    execute_operator_user_action,
    is_operator_user,
    update_operator_user_status,
)
from subscriptions.billing import BILLING_BOT_URL, build_bind_invoice, build_recovery_invoice, cancel_auto_renew, retry_monobank_charge
from subscriptions.consent import attach_consent_to_payment_id, get_billing_consent_for_user
from subscriptions.monobank import MonobankAPIError
from subscriptions.trial_recovery import record_trial_offer_event
from users.models import TelegramUser, UserAdminState, UserAuthIdentity
from users.registration import (
    RegistrationClosed,
    RegistrationIdentityConflict,
    RegistrationTelegramIdentity,
    bootstrap_telegram_identity,
)


MINIAPP_TEMPLATE = "miniapp/index.html"
MINIAPP_STATIC_DIR = Path(__file__).resolve().parent / "static" / "miniapp"
TRANSACTION_DRAFTS_SESSION_KEY = "miniapp_transaction_drafts"
ACCOUNT_DRAFTS_SESSION_KEY = "miniapp_account_drafts"
TRANSFER_DRAFTS_SESSION_KEY = "miniapp_transfer_drafts"
DEBT_DRAFTS_SESSION_KEY = "miniapp_debt_drafts"
BILLING_EXPENSE_DRAFTS_SESSION_KEY = "miniapp_billing_expense_drafts"
AI_IMAGE_BATCH_SESSION_KEY = "miniapp_ai_image_batch"
FAMILY_CREATE_DRAFTS_SESSION_KEY = "miniapp_family_create_drafts"
SAVING_TASK_DRAFTS_SESSION_KEY = "miniapp_saving_task_drafts"
HISTORY_EDIT_DRAFTS_SESSION_KEY = "miniapp_history_edit_drafts"
HISTORY_VOID_DRAFTS_SESSION_KEY = "miniapp_history_void_drafts"
ONBOARDING_DRAFTS_SESSION_KEY = "miniapp_onboarding_drafts"
MAX_TRANSACTION_DRAFTS = 8
MIN_IDEMPOTENCY_KEY_LENGTH = 16
INSTALL_NUDGE_MAX_PROMPTS = 4
INSTALL_NUDGE_PROMPT_DELAYS = {
    1: timedelta(days=1),
    2: timedelta(days=3),
    3: timedelta(days=7),
}
INSTALL_NUDGE_TELEGRAM_FIRST_DELAY = timedelta(days=1)


def _miniapp_asset_stamp(filename: str) -> int:
    try:
        return (MINIAPP_STATIC_DIR / filename).stat().st_mtime_ns
    except OSError:
        return 0


MINIAPP_ASSET_VERSION = str(
    max(
        _miniapp_asset_stamp("app.css"),
        _miniapp_asset_stamp("app.js"),
        _miniapp_asset_stamp("push.js"),
        _miniapp_asset_stamp("icon-192.png"),
        _miniapp_asset_stamp("icon-512.png"),
        _miniapp_asset_stamp("icon-maskable-192.png"),
        _miniapp_asset_stamp("icon-maskable-512.png"),
        _miniapp_asset_stamp("apple-touch-icon.png"),
        _miniapp_asset_stamp("brand-icon.png"),
        _miniapp_asset_stamp("favicon-32.png"),
        _miniapp_asset_stamp("favicon-48.png"),
        1,
    )
)
MINIAPP_THEME_COLOR = "#f4f6f1"


def _versioned_static(path: str) -> str:
    return f"{static(path)}?v={MINIAPP_ASSET_VERSION}"


def _read_json(request: HttpRequest) -> dict:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


def _access_blocked_response(access: dict[str, object]) -> JsonResponse:
    return JsonResponse(
        {
            "error": {
                "code": "access_blocked",
                "message": mt("Доступ до кабінету ще не активовано.", "Dashboard access is not active yet.", normalize_locale(access.get("locale"))),
            },
            "access": access,
        },
        status=403,
    )


def _transaction_error_response(exc: MiniAppTransactionError) -> JsonResponse:
    payload = {"error": {"code": exc.code, "message": exc.message}}
    payload.update(exc.extra)
    return JsonResponse(payload, status=exc.status)


def _family_error_response(exc: MiniAppFamilyError) -> JsonResponse:
    return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)


def _session_required_response(message: str = "Mini App session required.") -> JsonResponse:
    return JsonResponse({"error": {"code": "session_required", "message": message}}, status=401)


def _json_ok(payload: dict[str, object]) -> JsonResponse:
    return JsonResponse(payload)


def _install_nudge_state(user: TelegramUser) -> InstallNudgeState:
    now = timezone.now()
    state, _created = InstallNudgeState.objects.get_or_create(
        tg_user_id=user.tg_user_id,
        defaults={
            "next_prompt_at": now,
            "next_telegram_reminder_at": now + INSTALL_NUDGE_TELEGRAM_FIRST_DELAY,
        },
    )
    return state


def _install_nudge_browser_url(request: HttpRequest, user: TelegramUser) -> str:
    try:
        token = build_browser_login_token(user.tg_user_id)
    except MiniAppAuthError:
        return ""
    path = reverse("miniapp:browser-login-handoff", kwargs={"token": token})
    return request.build_absolute_uri(f"{path}?install=1")


def _install_nudge_payload(request: HttpRequest, user: TelegramUser, state: InstallNudgeState) -> dict[str, object]:
    now = timezone.now()
    installed = state.installed_at is not None
    due = (
        not installed
        and int(state.prompt_count or 0) < INSTALL_NUDGE_MAX_PROMPTS
        and (state.next_prompt_at is None or state.next_prompt_at <= now)
    )
    return {
        "due": due,
        "installed": installed,
        "prompt_count": int(state.prompt_count or 0),
        "remaining_prompts": max(INSTALL_NUDGE_MAX_PROMPTS - int(state.prompt_count or 0), 0),
        "max_prompts": INSTALL_NUDGE_MAX_PROMPTS,
        "browser_login_url": "" if installed else _install_nudge_browser_url(request, user),
    }


def _read_limit(request: HttpRequest, *, default: int, maximum: int) -> int:
    raw = str(request.GET.get("limit") or "").strip()
    if not raw:
        return default
    try:
        limit = int(raw)
    except ValueError:
        return default
    if limit <= 0:
        return default
    return min(limit, maximum)


def _ui_message(locale: str, uk_text: str, en_text: str) -> str:
    return mt(uk_text, en_text, normalize_locale(locale))


@never_cache
@ensure_csrf_cookie
@require_GET
def index(request: HttpRequest) -> HttpResponse:
    response = render(
        request,
        MINIAPP_TEMPLATE,
        {
            "dev_auth_enabled": bool(settings.DEBUG and getattr(settings, "MINIAPP_DEV_TG_USER_ID", None)),
            "preview_enabled": bool(settings.DEBUG),
            "default_period_label": "Цей місяць",
            "default_locale": "uk",
            "privacy_policy_url": str(
                getattr(settings, "PRIVACY_POLICY_URL", "") or "https://vydno.capital/privacy.html"
            ),
            "miniapp_asset_version": MINIAPP_ASSET_VERSION,
            "miniapp_theme_color": MINIAPP_THEME_COLOR,
            "browser_login_recovery_url": f"{BILLING_BOT_URL}?start=app_login",
        },
    )
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response["Pragma"] = "no-cache"
    response["Expires"] = "0"
    return response


@never_cache
@require_GET
def operator_index(request: HttpRequest) -> HttpResponse:
    response = render(
        request,
        "miniapp/operator.html",
        {
            "operator_api_url": reverse("miniapp:operator-bootstrap"),
            "operator_user_url_template": reverse("miniapp:operator-user-detail", args=[0]).replace("/0/", "/{user_id}/"),
            "operator_user_status_url_template": reverse("miniapp:operator-user-status", args=[0]).replace("/0/", "/{user_id}/"),
            "operator_user_action_url_template": reverse("miniapp:operator-user-action", args=[0]).replace("/0/", "/{user_id}/"),
            "profile_url": reverse("miniapp:profile"),
            "auth_url": reverse("miniapp:auth-telegram"),
        },
    )
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    return response


@never_cache
@require_GET
def family_join(request: HttpRequest, token: str) -> HttpResponse:
    # The shell reads the invite from the route after session authentication.
    return index(request)


@require_GET
def manifest(request: HttpRequest) -> JsonResponse:
    payload = {
        "id": "/app/",
        "name": "Vydno.Capital",
        "short_name": "Vydno",
        "description": "Personal finance dashboard for Vydno.Capital.",
        "start_url": "/app/?source=pwa",
        "scope": "/app/",
        "display": "standalone",
        "display_override": ["standalone", "minimal-ui", "browser"],
        "background_color": MINIAPP_THEME_COLOR,
        "theme_color": MINIAPP_THEME_COLOR,
        "orientation": "portrait",
        "icons": [
            {
                "src": _versioned_static("miniapp/icon-192.png"),
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "any",
            },
            {
                "src": _versioned_static("miniapp/icon-512.png"),
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "any",
            },
            {
                "src": _versioned_static("miniapp/icon-maskable-192.png"),
                "sizes": "192x192",
                "type": "image/png",
                "purpose": "maskable",
            },
            {
                "src": _versioned_static("miniapp/icon-maskable-512.png"),
                "sizes": "512x512",
                "type": "image/png",
                "purpose": "maskable",
            },
        ],
    }
    response = JsonResponse(payload, content_type="application/manifest+json")
    response["Cache-Control"] = "no-cache, max-age=0"
    return response


@require_GET
def service_worker(request: HttpRequest) -> HttpResponse:
    app_css_url = _versioned_static("miniapp/app.css")
    icon_192_url = _versioned_static("miniapp/icon-192.png")
    icon_512_url = _versioned_static("miniapp/icon-512.png")
    icon_maskable_192_url = _versioned_static("miniapp/icon-maskable-192.png")
    icon_maskable_512_url = _versioned_static("miniapp/icon-maskable-512.png")
    apple_icon_url = _versioned_static("miniapp/apple-touch-icon.png")
    brand_icon_url = _versioned_static("miniapp/brand-icon.png")
    favicon_32_url = _versioned_static("miniapp/favicon-32.png")
    favicon_48_url = _versioned_static("miniapp/favicon-48.png")
    script = f"""
const CACHE_NAME = "vydno-miniapp-shell-{MINIAPP_ASSET_VERSION}";
const APP_SHELL_URL = "/app/";
const SHELL_ASSETS = [
  APP_SHELL_URL,
  "{app_css_url}",
  "{icon_192_url}",
  "{icon_512_url}",
  "{icon_maskable_192_url}",
  "{icon_maskable_512_url}",
  "{brand_icon_url}",
  "{favicon_32_url}",
  "{favicon_48_url}",
  "{apple_icon_url}"
];

self.addEventListener("install", (event) => {{
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(SHELL_ASSETS))
      .then(() => self.skipWaiting())
  );
}});

self.addEventListener("activate", (event) => {{
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
}});

self.addEventListener("fetch", (event) => {{
  if (event.request.method !== "GET") return;
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith("/app/api/")) return;

  const isAppNavigation = event.request.mode === "navigate" && ["/app/", "/app"].includes(url.pathname);
  const isMiniAppStatic = url.pathname.startsWith("/static/miniapp/");
  if (!isAppNavigation && !isMiniAppStatic) return;

  const cacheKey = isAppNavigation ? APP_SHELL_URL : event.request;
  const fallback = () => caches.open(CACHE_NAME).then((cache) => cache.match(cacheKey));
  event.respondWith(
    fetch(event.request)
      .then(async (response) => {{
        const mime = response.headers.get("Content-Type") || "";
        const expectedType = isAppNavigation ? mime.startsWith("text/html") : ["text/css", "text/javascript", "application/javascript", "image/"].some((type) => mime.startsWith(type));
        const sameOrigin = !response.url || new URL(response.url).origin === self.location.origin;
        if (response.ok && !response.redirected && sameOrigin && expectedType) {{
          const copy = response.clone();
          event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.put(cacheKey, copy)).catch(() => {{}}));
          return response;
        }}
        return (await fallback()) || response;
      }})
      .catch(async () => (await fallback()) || new Response("Offline", {{status: 503, headers: {{"Content-Type": "text/plain"}}}}))
  );
}});

function setBadgeCount(count) {{
  const value = Math.max(Number(count || 0), 0);
  if (self.navigator && "setAppBadge" in self.navigator) {{
    return value ? self.navigator.setAppBadge(value) : self.navigator.clearAppBadge();
  }}
  return Promise.resolve();
}}

self.addEventListener("push", (event) => {{
  let payload = {{}};
  try {{ payload = event.data ? event.data.json() : {{}}; }} catch (_error) {{ payload = {{}}; }}
  const title = payload.title || "Vydno";
  const options = {{
    body: payload.body || "",
    icon: payload.icon || "{icon_192_url}",
    badge: payload.badge || "{favicon_48_url}",
    tag: payload.tag || "vydno-notification",
    renotify: Boolean(payload.renotify),
    data: {{
      url: payload.url || "/app/?screen=notifications",
      notificationId: payload.notification_id || 0
    }}
  }};
  event.waitUntil(Promise.all([
    self.registration.showNotification(title, options),
    setBadgeCount(payload.badge_count)
  ]));
}});

self.addEventListener("notificationclick", (event) => {{
  event.notification.close();
  const data = event.notification.data || {{}};
  let target = new URL(data.url || "/app/", self.location.origin);
  if (target.origin !== self.location.origin || !["/app/", "/app"].includes(target.pathname)) target = new URL("/app/", self.location.origin);
  if (Number.isSafeInteger(Number(data.notificationId)) && Number(data.notificationId) > 0) target.searchParams.set("notification_id", String(data.notificationId));
  // The authenticated app marks read only after its allowlisted route succeeds.
  const openTarget = self.clients.matchAll({{type: "window", includeUncontrolled: true}}).then((clients) => {{
    const existing = clients.find((client) => client.url.startsWith(self.location.origin + "/app/"));
    if (existing) return existing.navigate(target.href).then(() => existing.focus());
    return self.clients.openWindow(target.href);
  }});
  event.waitUntil(openTarget);
}});

self.addEventListener("message", (event) => {{
  if (event.data && event.data.type === "SET_BADGE") setBadgeCount(event.data.count);
}});
""".strip()
    response = HttpResponse(script, content_type="application/javascript; charset=utf-8")
    response["Cache-Control"] = "no-cache, max-age=0"
    return response


@never_cache
@require_GET
def telegram_oidc_start(request: HttpRequest) -> HttpResponse:
    try:
        authorization_url = begin_authorization(request, return_to=request.GET.get("next"))
    except TelegramOidcError as exc:
        return JsonResponse(
            {"error": {"code": "telegram_oidc_unavailable", "message": str(exc)}},
            status=503,
        )
    return redirect(authorization_url)


def _telegram_oidc_error_response(error: TelegramOidcError, *, status: int = 400) -> HttpResponse:
    body = f"""<!doctype html>
<html lang="uk">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>Vydno.Capital</title></head>
<body data-error-code="{escape(error.code)}">
  <main>
    <h1>Не вдалося увійти через Telegram</h1>
    <p>Спробуйте ще раз. Якщо проблема повторюється, отримайте одноразове посилання в боті.</p>
    <a href="/app/auth/telegram/start">Спробувати ще раз</a>
    <a href="{escape(BILLING_BOT_URL)}?start=app_login">Увійти через бота</a>
  </main>
</body>
</html>"""
    response = HttpResponse(body, status=status, content_type="text/html; charset=utf-8")
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    return response


@never_cache
@require_GET
def telegram_oidc_callback(request: HttpRequest) -> HttpResponse:
    try:
        pending = consume_pending_authorization(
            request,
            state=request.GET.get("state"),
            code=request.GET.get("code"),
        )
        id_token = exchange_authorization_code(
            code=pending.code,
            code_verifier=pending.code_verifier,
        )
        identity = verify_id_token(id_token, expected_nonce=pending.nonce)
        consume_id_token_once(id_token, identity)
        registration = bootstrap_telegram_identity(
            identity,
            source="telegram_oidc",
        )
    except TelegramOidcError as exc:
        if request.session.get(SESSION_AUTH_MODE_KEY) == TELEGRAM_OIDC_AUTH_MODE:
            try:
                get_session_user(request)
            except MiniAppSessionError:
                pass
            else:
                return redirect(DEFAULT_RETURN_PATH)
        return _telegram_oidc_error_response(exc)
    except RegistrationClosed:
        return _telegram_oidc_error_response(
            TelegramOidcError(
                "Registration is temporarily closed.",
                code="registration_closed",
            ),
            status=503,
        )
    except RegistrationIdentityConflict:
        return _telegram_oidc_error_response(
            TelegramOidcError(
                "Telegram identity could not be safely linked.",
                code="telegram_oidc_identity_conflict",
            ),
            status=409,
        )

    login_session(
        request,
        registration.user,
        session_age_seconds=browser_session_age_seconds(),
        auth_mode=TELEGRAM_OIDC_AUTH_MODE,
        identity=registration.identity,
    )
    return redirect(pending.return_to)


def _browser_login_error_message(exc: MiniAppAuthError) -> str:
    reason = str(exc).lower()
    if "expired" in reason:
        return "This login link has expired. Open the Telegram bot and request a new app link."
    if "already used" in reason:
        return "This login link was already used. Open the Telegram bot and request a new app link."
    return "This login link is invalid. Open the Telegram bot and request a new app link."


def _browser_login_error_response(exc: MiniAppAuthError) -> HttpResponse:
    reason = str(exc).lower()
    if "expired" in reason:
        title = "Посилання протермінувалося"
    elif "already used" in reason:
        title = "Посилання вже використане"
    else:
        title = "Посилання не працює"
    refresh_url = f"{BILLING_BOT_URL}?start=app_login"
    body = f"""<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex,nofollow">
  <title>Vydno.Capital</title>
  <style>
    body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #031817; color: #f4fbf7; }}
    main {{ width: min(420px, calc(100vw - 32px)); }}
    h1 {{ margin: 0 0 12px; font-size: 28px; line-height: 1.1; }}
    p {{ margin: 0 0 16px; color: #b7c8c0; line-height: 1.45; }}
    a {{ display: block; padding: 14px 16px; border-radius: 8px; background: #79f2bd; color: #031817; font-weight: 700; text-align: center; text-decoration: none; }}
    small {{ display: block; margin-top: 14px; color: #8fa29a; line-height: 1.4; }}
  </style>
</head>
<body>
  <main>
    <h1>{escape(title)}</h1>
    <p>Поверніться в Telegram — бот одразу сформує нове актуальне посилання для входу у вебдодаток.</p>
    <a href="{escape(refresh_url)}">Отримати нове посилання</a>
    <small>{escape(_browser_login_error_message(exc))}</small>
  </main>
</body>
</html>"""
    response = HttpResponse(body, status=400, content_type="text/html; charset=utf-8")
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    return response


@never_cache
@require_GET
def browser_login_handoff(request: HttpRequest, token: str) -> HttpResponse:
    install_query = "?install=1" if str(request.GET.get("install") or "") == "1" else ""
    operator_query = "operator=1" if str(request.GET.get("operator") or "") == "1" else ""
    query_parts = [part for part in [install_query.removeprefix("?"), operator_query] if part]
    login_query = f"?{'&'.join(query_parts)}" if query_parts else ""
    # Keep the session-setting navigation outside the Service Worker fetch path.
    # Older Safari/WebKit versions can lose SameSite session cookies when a
    # redirecting navigation is fulfilled through a Service Worker. The worker
    # has always bypassed /app/api/*, so this repairs already installed clients
    # without waiting for a Service Worker update.
    login_url = f"/app/api/browser-login/{quote(token, safe='')}/{login_query}"
    body = f"""<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex,nofollow">
  <title>Vydno.Capital</title>
  <script>
    (function () {{
      var ua = navigator.userAgent || "";
      var isIOS = /iPhone|iPad|iPod/i.test(ua) || (/Macintosh/i.test(ua) && navigator.maxTouchPoints > 1);
      var isMobile = isIOS || /Android/i.test(ua);
      var isKnownEmbedded = /Telegram|Instagram|FBAN|FBAV|Line|Twitter|Snapchat|TikTok|MicroMessenger/i.test(ua);
      var hasExternalBrowserMarker = /Safari|CriOS|FxiOS|EdgiOS|OPiOS|Chrome|Firefox|EdgA|OPR/i.test(ua);
      var isEmbeddedBrowser = isMobile && (isKnownEmbedded || !hasExternalBrowserMarker);
      if (isEmbeddedBrowser) document.documentElement.classList.add("embedded-browser");
      if (isIOS && isEmbeddedBrowser) document.documentElement.classList.add("ios-embedded-browser");
    }})();
  </script>
  <style>
    body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #031817; color: #f4fbf7; }}
    main {{ width: min(420px, calc(100vw - 32px)); }}
    h1 {{ margin: 0 0 12px; font-size: 28px; line-height: 1.1; }}
    p {{ margin: 0 0 16px; color: #b7c8c0; line-height: 1.45; }}
    a {{ display: block; padding: 14px 16px; border-radius: 8px; background: #79f2bd; color: #031817; font-weight: 700; text-align: center; text-decoration: none; }}
    small {{ display: block; margin-top: 14px; color: #8fa29a; line-height: 1.4; }}
    .embedded-browser-note {{ display: none; margin: 0 0 16px; color: #79f2bd; font-weight: 700; }}
    .embedded-browser .embedded-browser-note {{ display: block; }}
    .embedded-browser .browser-login-action {{ display: none; }}
    .ios-browser-tip {{ display: none; }}
    .ios-embedded-browser .ios-browser-tip {{ position: fixed; right: 14px; bottom: max(8px, env(safe-area-inset-bottom)); display: flex; align-items: center; gap: 8px; padding: 9px 11px; border: 1px solid rgba(121, 242, 189, .55); border-radius: 12px; background: rgba(3, 24, 23, .96); color: #f4fbf7; font-size: 14px; font-weight: 700; box-shadow: 0 8px 24px rgba(0, 0, 0, .28); }}
    .ios-browser-tip__icon {{ display: inline-grid; width: 22px; height: 22px; place-items: center; border: 2px solid #79f2bd; border-radius: 50%; color: #79f2bd; font-size: 14px; line-height: 1; }}
    .ios-browser-tip__arrow {{ color: #79f2bd; font-size: 26px; line-height: .8; transform: translate(2px, 3px); }}
  </style>
</head>
<body>
  <main>
    <h1>Vydno.Capital</h1>
    <p>Відкрий цю сторінку в Safari або Chrome, потім натисни кнопку входу. Так сесія збережеться для іконки на робочому столі.</p>
    <p class="embedded-browser-note">Кнопка входу з’явиться після відкриття цієї сторінки в Safari або Chrome.</p>
    <a class="browser-login-action" href="{escape(login_url)}">Увійти у веб-додаток</a>
    <small>На iPhone у Telegram натисни компас унизу праворуч. На Android відкрий меню ⋮ і вибери Chrome. Уже в браузері натисни кнопку входу.</small>
  </main>
  <div class="ios-browser-tip" aria-label="Натисни компас унизу праворуч, щоб відкрити Safari">
    <span class="ios-browser-tip__icon" aria-hidden="true">↗</span>
    <span>Натисни компас</span>
    <span class="ios-browser-tip__arrow" aria-hidden="true">↘</span>
  </div>
</body>
</html>"""
    response = HttpResponse(body, content_type="text/html; charset=utf-8")
    response["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0, private"
    response["Pragma"] = "no-cache"
    response["Expires"] = "0"
    return response


@never_cache
@require_GET
def browser_login(request: HttpRequest, token: str) -> HttpResponse:
    try:
        user = consume_browser_login_token(token)
    except MiniAppAuthError as exc:
        return _browser_login_error_response(exc)

    login_session(
        request,
        user,
        session_age_seconds=browser_session_age_seconds(),
        auth_mode=BROWSER_AUTH_MODE,
    )
    if str(request.GET.get("operator") or "") == "1":
        if not is_operator_user(user):
            logout_session(request)
            return JsonResponse(
                {"error": {"code": "operator_access_denied", "message": "Operator access denied."}},
                status=403,
            )
        return redirect("/app/operator/")
    target = "/app/?install=1" if str(request.GET.get("install") or "") == "1" else "/app/"
    return redirect(target)


@csrf_exempt
@require_POST
def auth_telegram(request: HttpRequest) -> JsonResponse:
    payload = _read_json(request)
    init_data = str(payload.get("init_data") or "")
    try:
        identity = validate_telegram_init_data(init_data)
    except MiniAppAuthError as exc:
        return JsonResponse({"error": {"code": "telegram_auth_failed", "message": str(exc)}}, status=401)

    try:
        registration = bootstrap_telegram_identity(
            RegistrationTelegramIdentity(
                provider=UserAuthIdentity.Provider.TELEGRAM_MINIAPP,
                subject=str(identity.tg_user_id),
                tg_user_id=identity.tg_user_id,
                first_name=identity.first_name,
                last_name=identity.last_name,
                username=identity.username,
                claims=identity.raw_user,
            ),
            source="telegram_miniapp",
        )
    except RegistrationClosed:
        auth_locale = normalize_locale((identity.raw_user or {}).get("language_code"))
        return JsonResponse(
            {
                "error": {
                    "code": "registration_closed",
                    "message": _ui_message(
                        auth_locale,
                        "Реєстрацію тимчасово призупинено. Спробуйте пізніше.",
                        "Registration is temporarily closed. Please try again later.",
                    ),
                }
            },
            status=403,
        )
    except RegistrationIdentityConflict:
        auth_locale = normalize_locale((identity.raw_user or {}).get("language_code"))
        return JsonResponse(
            {
                "error": {
                    "code": "telegram_identity_conflict",
                    "message": _ui_message(auth_locale, "Не вдалося безпечно прив’язати Telegram-акаунт.", "Could not safely link the Telegram account."),
                }
            },
            status=409,
        )

    user = registration.user
    login_session(request, user)
    access = resolve_access(user)
    return _json_ok(
        {
            "ok": True,
            "user": {
                "tg_user_id": int(user.tg_user_id),
                "first_name": str(user.first_name or identity.first_name or ""),
                "base_currency": str(user.base_currency or "UAH"),
                "lang": locale_for_user(user),
            },
            "access": access,
        }
    )


@csrf_exempt
@require_POST
def auth_dev(request: HttpRequest) -> JsonResponse:
    try:
        user = dev_login_user(request)
    except MiniAppAuthError as exc:
        return JsonResponse({"error": {"code": "dev_auth_disabled", "message": str(exc)}}, status=403)

    return _json_ok(
        {
            "ok": True,
            "user": {
                "tg_user_id": int(user.tg_user_id),
                "first_name": str(user.first_name or ""),
                "base_currency": str(user.base_currency or "UAH"),
                "lang": locale_for_user(user),
            },
            "access": resolve_access(user),
        }
    )


@csrf_exempt
@require_POST
def auth_logout(request: HttpRequest) -> JsonResponse:
    payload = _read_json(request)
    all_devices = payload.get("all_devices") is True
    if all_devices:
        current_user = _get_session_user_or_response(request)
        if isinstance(current_user, JsonResponse):
            logout_session(request)
            return current_user
    logout_session(request, all_devices=all_devices)
    return _json_ok({"ok": True})


def _notification_user_or_response(request: HttpRequest):
    return _get_session_user_or_response(request)


@require_GET
def notification_status(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    user_id = int(user.tg_user_id)
    endpoint = str(request.GET.get("endpoint") or "").strip()
    device_subscription_active = False
    if endpoint and len(endpoint) <= 2048:
        device_subscription_active = WebPushSubscription.objects.filter(
            tg_user_id=user_id,
            endpoint=endpoint,
            is_active=True,
        ).exists()
    notices = AppNotification.objects.filter(tg_user_id=user_id).order_by("-created_at", "-id")[:50]
    return _json_ok(
        {
            "configured": bool(
                str(getattr(settings, "WEB_PUSH_VAPID_PUBLIC_KEY", "") or "").strip()
                and str(getattr(settings, "WEB_PUSH_VAPID_PRIVATE_KEY", "") or "").strip()
            ),
            "public_key": str(getattr(settings, "WEB_PUSH_VAPID_PUBLIC_KEY", "") or "").strip(),
            "active_subscriptions": WebPushSubscription.objects.filter(tg_user_id=user_id, is_active=True).count(),
            "device_subscription_active": device_subscription_active,
            "badge_count": unread_badge_count(user_id),
            "preferences": preferences_payload(user_id),
            "notifications": [serialize_notification(item) for item in notices],
        }
    )


@csrf_exempt
@require_POST
def notification_subscribe(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _read_json(request)
    endpoint = str(payload.get("endpoint") or "").strip()
    keys = payload.get("keys") if isinstance(payload.get("keys"), dict) else {}
    p256dh = str(keys.get("p256dh") or "").strip()
    auth = str(keys.get("auth") or "").strip()
    if not endpoint.startswith("https://") or not p256dh or not auth:
        return JsonResponse({"error": {"code": "invalid_subscription", "message": "A valid PushSubscription is required."}}, status=400)
    subscription, _ = WebPushSubscription.objects.update_or_create(
        endpoint=endpoint,
        defaults={
            "tg_user_id": int(user.tg_user_id),
            "p256dh": p256dh,
            "auth": auth,
            "platform": str(payload.get("platform") or "")[:24],
            "device_label": str(payload.get("device_label") or "")[:120],
            "user_agent": str(request.META.get("HTTP_USER_AGENT") or "")[:1000],
            "is_active": True,
            "failure_count": 0,
        },
    )
    return _json_ok({"ok": True, "subscription_id": subscription.pk, "badge_count": unread_badge_count(user.tg_user_id)})


@csrf_exempt
@require_POST
def notification_unsubscribe(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    endpoint = str(_read_json(request).get("endpoint") or "").strip()
    if endpoint:
        WebPushSubscription.objects.filter(tg_user_id=user.tg_user_id, endpoint=endpoint).update(is_active=False)
    return _json_ok({"ok": True})


@csrf_exempt
@require_POST
def notification_read(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    payload = _read_json(request)
    try:
        notification_id = int(payload.get("notification_id") or 0)
    except (TypeError, ValueError):
        notification_id = 0
    queryset = AppNotification.objects.filter(tg_user_id=user.tg_user_id, status=AppNotification.Status.UNREAD)
    if bool(payload.get("all")):
        queryset.update(status=AppNotification.Status.READ, read_at=timezone.now())
    elif notification_id > 0:
        queryset.filter(pk=notification_id).update(status=AppNotification.Status.READ, read_at=timezone.now())
    return _json_ok({"ok": True, "badge_count": unread_badge_count(user.tg_user_id)})


@csrf_exempt
@require_http_methods(["GET", "POST"])
def notification_preferences(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    if request.method == "POST":
        try:
            update_preferences(user.tg_user_id, _read_json(request))
        except ValueError as exc:
            return JsonResponse({"error": {"code": "invalid_preferences", "message": str(exc)}}, status=400)
    return _json_ok({"ok": True, "preferences": preferences_payload(user.tg_user_id)})


@csrf_exempt
@require_POST
def notification_test(request: HttpRequest) -> JsonResponse:
    user = _notification_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    key = f"test:{user.tg_user_id}:{timezone.now().strftime('%Y%m%d%H%M%S%f')}"
    notification, delivered, _ = emit_notification(user.tg_user_id, "test", idempotency_key=key)
    return _json_ok({"ok": True, "notification": serialize_notification(notification), "delivered": delivered})


@csrf_exempt
@require_POST
def internal_notification_event(request: HttpRequest) -> JsonResponse:
    configured_token = str(
        getattr(settings, "PUSH_INTERNAL_TOKEN", "")
        or getattr(settings, "BILLING_INTERNAL_TOKEN", "")
        or ""
    )
    supplied_token = str(request.headers.get("X-Internal-Token") or "")
    if not configured_token or not supplied_token or not secrets.compare_digest(configured_token, supplied_token):
        return JsonResponse({"error": {"code": "forbidden", "message": "Invalid internal token."}}, status=403)
    payload = _read_json(request)
    event_type = str(payload.get("event_type") or "").strip()
    if event_type not in EVENTS:
        return JsonResponse({"error": {"code": "unsupported_event", "message": "Unsupported notification event."}}, status=400)
    try:
        user_id = int(payload.get("user_id") or 0)
    except (TypeError, ValueError):
        user_id = 0
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    if user_id <= 0 or not idempotency_key:
        return JsonResponse({"error": {"code": "invalid_event", "message": "user_id and idempotency_key are required."}}, status=400)
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    notification, delivered, created = emit_notification(
        user_id,
        event_type,
        context=context,
        idempotency_key=idempotency_key,
    )
    return _json_ok({"ok": True, "created": created, "delivered": delivered, "notification_id": notification.pk})


def _internal_request_authorized(request: HttpRequest) -> bool:
    configured_token = str(getattr(settings, "BILLING_INTERNAL_TOKEN", "") or "")
    supplied_token = str(request.headers.get("X-Internal-Token") or "")
    return bool(
        configured_token
        and supplied_token
        and secrets.compare_digest(configured_token, supplied_token)
    )


def _internal_actor_or_response(request: HttpRequest, payload: dict[str, object]):
    if not _internal_request_authorized(request):
        return JsonResponse({"error": {"code": "forbidden", "message": "Invalid internal token."}}, status=403)
    try:
        actor_id = int(payload.get("actor_tg_user_id") or 0)
    except (TypeError, ValueError):
        actor_id = 0
    user = TelegramUser.objects.filter(tg_user_id=actor_id).first() if actor_id > 0 else None
    if user is None:
        return JsonResponse({"error": {"code": "actor_missing", "message": "Telegram actor was not found."}}, status=404)
    access = resolve_access(user)
    if str(access.get("mode")) != "active":
        return JsonResponse({"error": {"code": "access_blocked", "message": "Write access is not active."}}, status=403)
    return user


@csrf_exempt
@require_POST
def internal_transactions_recent(request: HttpRequest) -> JsonResponse:
    payload = _read_json(request)
    actor = _internal_actor_or_response(request, payload)
    if isinstance(actor, JsonResponse):
        return actor
    try:
        limit = int(payload.get("limit") or 10)
    except (TypeError, ValueError):
        limit = 10
    return _json_ok({"ok": True, **build_recent_cancellable_transactions(actor, limit=limit)})


@csrf_exempt
@require_POST
def internal_transaction_void_draft(request: HttpRequest) -> JsonResponse:
    payload = _read_json(request)
    actor = _internal_actor_or_response(request, payload)
    if isinstance(actor, JsonResponse):
        return actor
    try:
        draft = TransactionCancellationService(actor).build_draft(payload, draft_id=secrets.token_urlsafe(16))
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    # This internal caller transports the draft rather than a Django session.
    draft["identity_scope"] = _identity_scope_binding(actor)
    _persist_draft_action(user=actor, operation="history_void", draft_id=draft["draft_id"], entry={"status": "draft", "draft": draft})
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def internal_transaction_void_confirm(request: HttpRequest) -> JsonResponse:
    payload = _read_json(request)
    actor = _internal_actor_or_response(request, payload)
    if isinstance(actor, JsonResponse):
        return actor
    draft = payload.get("draft") if isinstance(payload.get("draft"), dict) else {}
    draft_id = str(draft.get("draft_id") or payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    if not draft or not draft_id:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Transaction cancellation draft was not found."}}, status=404)
    try:
        result, idempotent = _commit_draft_once(
            user=actor,
            operation="history_void",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry={"status": "draft", "draft": draft, "identity_scope": draft.get("identity_scope")},
            commit=lambda: TransactionCancellationService(actor).commit_draft(draft),
        )
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


def _get_session_user_or_response(request: HttpRequest):
    try:
        return get_session_user(request)
    except MiniAppSessionError as exc:
        return _session_required_response(str(exc))


def _operator_user_or_response(request: HttpRequest):
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    if not is_operator_user(result):
        return JsonResponse(
            {"error": {"code": "operator_access_denied", "message": "Operator access denied."}},
            status=403,
        )
    return result


def _authorized_user_or_blocked(request: HttpRequest):
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    access = resolve_access(result)
    if str(access.get("mode")) == "blocked":
        return _access_blocked_response(access)
    return result, access


def _authorized_write_user_or_blocked(request: HttpRequest):
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    access = resolve_access(result)
    if str(access.get("mode")) != "active":
        return _access_blocked_response(access)
    return result, access


def _onboarding_user_or_response(request: HttpRequest):
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    access = resolve_access(result)
    if UserAdminState.objects.filter(telegram_user_id=result.tg_user_id).filter(Q(is_blocked=True) | Q(status="banned")).exists():
        return _access_blocked_response(access)
    return result


def _session_transaction_drafts(request: HttpRequest) -> dict[str, dict[str, object]]:
    raw = request.session.get(TRANSACTION_DRAFTS_SESSION_KEY, {})
    return raw if isinstance(raw, dict) else {}


def _save_session_transaction_drafts(request: HttpRequest, drafts: dict[str, dict[str, object]]) -> None:
    trimmed = dict(list(drafts.items())[-MAX_TRANSACTION_DRAFTS:])
    request.session[TRANSACTION_DRAFTS_SESSION_KEY] = trimmed
    request.session.modified = True


def _session_drafts(request: HttpRequest, key: str) -> dict[str, dict[str, object]]:
    raw = request.session.get(key, {})
    return raw if isinstance(raw, dict) else {}


def _save_session_drafts(request: HttpRequest, key: str, drafts: dict[str, dict[str, object]]) -> None:
    request.session[key] = dict(list(drafts.items())[-MAX_TRANSACTION_DRAFTS:])
    request.session.modified = True


def _identity_scope_binding(user: TelegramUser, *, scope=None) -> dict[str, object]:
    from miniapp.services import _resolve_finance_scope

    scope = scope if scope is not None else _resolve_finance_scope(user)
    return {"actor_user_id": int(user.tg_user_id), "scope_type": scope.type, "family_id": scope.family_id}


def _session_ai_image_batch(request: HttpRequest) -> dict[str, object] | None:
    raw = request.session.get(AI_IMAGE_BATCH_SESSION_KEY)
    if not isinstance(raw, dict) or not str(raw.get("batch_id") or "").strip():
        return None
    try:
        binding = _identity_scope_binding(get_session_user(request))
    except MiniAppSessionError:
        binding = None
    if binding is None or raw.get("identity_scope") != binding:
        request.session.pop(AI_IMAGE_BATCH_SESSION_KEY, None)
        return None
    return raw


def _save_session_ai_image_batch(request: HttpRequest, batch: dict[str, object]) -> None:
    request.session[AI_IMAGE_BATCH_SESSION_KEY] = batch
    request.session.modified = True


def _ai_image_batch_payload(batch: dict[str, object] | None) -> dict[str, object] | None:
    if batch is None:
        return None
    raw_items = batch.get("items")
    items = raw_items if isinstance(raw_items, list) else []
    normalized_items: list[dict[str, object]] = []
    committed_count = 0
    skipped_count = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "pending")
        if status == "committed":
            committed_count += 1
        elif status == "skipped":
            skipped_count += 1
        normalized_items.append(
            {
                "id": str(item.get("id") or ""),
                "status": status,
                "suggestion": item.get("suggestion") if isinstance(item.get("suggestion"), dict) else {},
            }
        )
    return {
        "mode": "batch",
        "batch_id": str(batch.get("batch_id") or ""),
        "currency": str(batch.get("currency") or ""),
        "currency_explicit": bool(batch.get("currency_explicit")),
        "item_count": len(normalized_items),
        "committed_count": committed_count,
        "skipped_count": skipped_count,
        "pending_count": len(normalized_items) - committed_count - skipped_count,
        "skipped_items_count": max(0, int(batch.get("skipped_items_count") or 0)),
        "items": normalized_items,
    }


def _find_ai_image_batch_item(batch: dict[str, object], item_id: object) -> dict[str, object] | None:
    requested_id = str(item_id or "").strip()
    raw_items = batch.get("items")
    if not requested_id or not isinstance(raw_items, list):
        return None
    for item in raw_items:
        if isinstance(item, dict) and str(item.get("id") or "") == requested_id:
            return item
    return None


def _batch_context_for_transaction_draft(request: HttpRequest, payload: dict[str, object]) -> dict[str, str] | None:
    batch_id = str(payload.get("ai_image_batch_id") or "").strip()
    item_id = str(payload.get("ai_image_batch_item_id") or "").strip()
    if not batch_id and not item_id:
        return None
    if not batch_id or not item_id:
        raise MiniAppTransactionError("batch_item_missing", "Statement item was not found.", status=404)
    batch = _session_ai_image_batch(request)
    if batch is None or str(batch.get("batch_id") or "") != batch_id:
        raise MiniAppTransactionError("batch_missing", "Statement batch was not found.", status=404)
    item = _find_ai_image_batch_item(batch, item_id)
    if item is None or str(item.get("status") or "pending") != "pending":
        raise MiniAppTransactionError("batch_item_unavailable", "Statement item is no longer available.", status=409)
    return {"batch_id": batch_id, "item_id": item_id}


def _commit_ai_image_batch_item(request: HttpRequest, context: dict[str, object] | None) -> dict[str, object] | None:
    if not isinstance(context, dict):
        return None
    batch_id = str(context.get("batch_id") or "")
    item_id = str(context.get("item_id") or "")
    batch = _session_ai_image_batch(request)
    if batch is None or str(batch.get("batch_id") or "") != batch_id:
        return None
    item = _find_ai_image_batch_item(batch, item_id)
    if item is None:
        return None
    item["status"] = "committed"
    _save_session_ai_image_batch(request, batch)
    return _ai_image_batch_payload(batch)


def _draft_entry_fingerprint(entry: dict[str, object]) -> str:
    # Bind both the preview and its origin, never mutable session receipt data.
    payload = {key: entry[key] for key in ("draft", "ai_image_batch", "billing_draft_id", "identity_scope") if key in entry}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()


def _persist_draft_action(*, user: TelegramUser, operation: str, draft_id: str, entry: dict[str, object]) -> None:
    entry["identity_scope"] = _identity_scope_binding(user)
    DraftAction.objects.create(
        tg_user_id=user.tg_user_id,
        operation=operation,
        draft_id=draft_id,
        payload_hash=_draft_entry_fingerprint(entry),
        status="draft",
        result={},
        expires_at=timezone.now() + timedelta(hours=24),
    )


def _commit_draft_once(
    *,
    user: TelegramUser,
    operation: str,
    draft_id: str,
    idempotency_key: str,
    commit,
    entry: dict[str, object] | None = None,
) -> tuple[dict[str, object], bool]:
    if len(idempotency_key) < MIN_IDEMPOTENCY_KEY_LENGTH:
        raise MiniAppTransactionError(
            "idempotency_key_required",
            "A valid idempotency key is required to confirm this operation.",
        )

    with db_transaction.atomic():
        action = DraftAction.objects.select_for_update().filter(tg_user_id=user.tg_user_id, draft_id=draft_id).first()
        if action is None:
            raise MiniAppTransactionError("draft_expired", "Review this operation again before confirming.", status=409)
        if action.operation != operation or action.payload_hash != _draft_entry_fingerprint(entry or {}):
            raise MiniAppTransactionError("draft_payload_conflict", "This confirmation does not match the reviewed draft.", status=409)
        if action.status == "cancelled":
            raise MiniAppTransactionError("draft_cancelled", "This draft was cancelled. Create a new preview.", status=409)
        if action.status == "expired" or (action.status == "draft" and action.expires_at <= timezone.now()):
            raise MiniAppTransactionError("draft_expired", "This draft expired. Create a new preview.", status=409)
        if action.status not in {"draft", "committed"}:
            raise MiniAppTransactionError("draft_not_pending", "This draft cannot be confirmed.", status=409)
        if action.status == "draft":
            from miniapp.services import _lock_finance_scope

            scope = _lock_finance_scope(user)
            if (entry or {}).get("identity_scope") != _identity_scope_binding(user, scope=scope):
                raise MiniAppTransactionError("scope_changed", "The finance scope changed. Create a new preview.", status=409)
        receipt, _created = WriteReceipt.objects.get_or_create(
            tg_user_id=user.tg_user_id,
            operation=operation,
            idempotency_key=idempotency_key,
            defaults={"draft_id": draft_id, "status": "pending", "result": {}},
        )
        if receipt.draft_id != draft_id:
            raise MiniAppTransactionError("idempotency_key_conflict", "This confirmation key belongs to another draft.", status=409)
        idempotent = action.status == "committed"
        receipts = [receipt]
        batch_context = (entry or {}).get("ai_image_batch")
        if not idempotent and isinstance(batch_context, dict):
            origin_key = hashlib.sha256(json.dumps(batch_context, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            origin, _created = WriteReceipt.objects.get_or_create(
                tg_user_id=user.tg_user_id,
                operation="statement_item",
                idempotency_key=origin_key,
                defaults={"draft_id": draft_id, "status": "pending", "result": {}},
            )
            if origin.draft_id != draft_id:
                raise MiniAppTransactionError("batch_item_unavailable", "Statement item is already confirmed by another draft.", status=409)
            receipts.append(origin)
        result = dict(action.result or {}) if idempotent else commit()
        action.status = "committed"
        action.result = result
        action.save(update_fields=["status", "result", "updated_at"])
        for receipt in receipts:
            receipt.status = "committed"
            receipt.result = result
            receipt.save(update_fields=["status", "result", "updated_at"])
        return result, idempotent


def _cancel_session_draft(request: HttpRequest, user: TelegramUser, key: str, draft_id: str, *, cancel=None) -> None:
    drafts = _session_drafts(request, key)
    entry = drafts.get(draft_id)
    if not isinstance(entry, dict):
        return
    with db_transaction.atomic():
        action = DraftAction.objects.select_for_update().filter(tg_user_id=user.tg_user_id, draft_id=draft_id).first()
        if action is None:
            entry["status"] = "expired"
        else:
            if action.payload_hash != _draft_entry_fingerprint(entry):
                raise MiniAppTransactionError("draft_payload_conflict", "This action does not match the reviewed draft.", status=409)
            if action.status == "draft":
                if action.expires_at <= timezone.now():
                    action.status = "expired"
                else:
                    if cancel is not None:
                        cancel(entry)
                    action.status = "cancelled"
                action.save(update_fields=["status", "updated_at"])
            entry["status"] = action.status
            if action.status == "committed":
                entry["result"] = dict(action.result or {})
    _save_session_drafts(request, key, drafts)


@require_GET
def profile(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    user = result
    return _json_ok(
        {
            "user": {
                "tg_user_id": int(user.tg_user_id),
                "first_name": str(user.first_name or ""),
                "base_currency": str(user.base_currency or "UAH"),
                "lang": locale_for_user(user),
            },
            "access": resolve_access(user),
        }
    )


@require_GET
def operator_bootstrap(request: HttpRequest) -> JsonResponse:
    if settings.DEBUG and str(request.GET.get("preview") or "") == "1":
        return _json_ok(build_operator_preview_payload(query=str(request.GET.get("q") or "")))
    result = _operator_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    return _json_ok(build_operator_payload(result, query=str(request.GET.get("q") or "")))


@require_GET
def operator_user_detail(request: HttpRequest, tg_user_id: int) -> JsonResponse:
    if settings.DEBUG and str(request.GET.get("preview") or "") == "1":
        try:
            return _json_ok({"user": build_operator_preview_user_detail(tg_user_id)})
        except OperatorUserError as exc:
            return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)
    result = _operator_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    try:
        return _json_ok({"user": build_operator_user_detail(tg_user_id)})
    except OperatorUserError as exc:
        return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)


@csrf_exempt
@require_POST
def operator_user_status(request: HttpRequest, tg_user_id: int) -> JsonResponse:
    if settings.DEBUG and str(request.GET.get("preview") or "") == "1":
        return JsonResponse(
            {"error": {"code": "preview_read_only", "message": "Preview mode is read-only."}},
            status=403,
        )
    result = _operator_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    if payload.get("confirmed") is not True:
        return JsonResponse(
            {"error": {"code": "confirmation_required", "message": "Потрібне явне підтвердження дії."}},
            status=400,
        )
    try:
        user_payload = update_operator_user_status(
            operator=result,
            tg_user_id=tg_user_id,
            action=str(payload.get("action") or ""),
            reason=str(payload.get("reason") or ""),
            request=request,
        )
    except OperatorUserError as exc:
        return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)
    return _json_ok({"user": user_payload})


@csrf_exempt
@require_POST
def operator_user_action(request: HttpRequest, tg_user_id: int) -> JsonResponse:
    if settings.DEBUG and str(request.GET.get("preview") or "") == "1":
        return JsonResponse(
            {"error": {"code": "preview_read_only", "message": "Preview mode is read-only."}},
            status=403,
        )
    result = _operator_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    if payload.get("confirmed") is not True:
        return JsonResponse(
            {"error": {"code": "confirmation_required", "message": "Потрібне явне підтвердження дії."}},
            status=400,
        )
    try:
        action_result = execute_operator_user_action(
            operator=result,
            tg_user_id=tg_user_id,
            action=str(payload.get("action") or ""),
            payload=payload,
            request=request,
        )
    except OperatorUserError as exc:
        return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)
    return _json_ok(action_result)


@require_GET
def onboarding(request: HttpRequest) -> JsonResponse:
    result = _onboarding_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    return _json_ok({"onboarding": onboarding_status(result)})


@csrf_exempt
@require_POST
def onboarding_draft(request: HttpRequest) -> JsonResponse:
    result = _onboarding_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    user = result
    if bool(onboarding_status(user).get("completed")):
        return JsonResponse(
            {"error": {"code": "onboarding_completed", "message": "Initial setup is already complete."}},
            status=409,
        )
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_onboarding_draft(user, _read_json(request), draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, ONBOARDING_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation="onboarding_complete", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, ONBOARDING_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def onboarding_confirm(request: HttpRequest) -> JsonResponse:
    result = _onboarding_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    user = result
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, ONBOARDING_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Onboarding draft was not found."}}, status=404)
    draft = entry.get("draft")
    if not isinstance(draft, dict):
        return JsonResponse({"error": {"code": "draft_missing", "message": "Onboarding draft was not found."}}, status=404)
    try:
        commit_result, idempotent = _commit_draft_once(
            user=user,
            operation="onboarding_complete",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_onboarding_draft(user, draft),
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = commit_result
    drafts[draft_id] = entry
    _save_session_drafts(request, ONBOARDING_DRAFTS_SESSION_KEY, drafts)
    _install_nudge_state(user)
    record_request_server_event(
        request,
        user=user,
        event_name="onboarding_confirmed",
        idempotency_key=f"onboarding:{draft_id}",
    )
    return _json_ok({"ok": True, "idempotent": idempotent, "result": commit_result})


def _settings_payload(user: TelegramUser) -> dict[str, object]:
    reminder_setting = DailyExpenseReminderSetting.objects.filter(user_id=user.tg_user_id).first()
    return profile_settings_payload(user, reminder_setting=reminder_setting)


def _billing_recovery_checkout_needed(access: dict[str, object]) -> bool:
    if str(access.get("access_scope") or "") != "paywall":
        return False
    has_history = any(
        access.get(key)
        for key in ("subscription_status", "expires_at", "next_charge_at", "grace_expires_at")
    )
    return has_history and str(access.get("billing_access_mode") or "") in {"read_only", "blocked"}


@require_GET
def settings_options(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    return _json_ok(_settings_payload(result))


@require_GET
def install_nudge_status(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    state = _install_nudge_state(result)
    return _json_ok({"install_nudge": _install_nudge_payload(request, result, state)})


@csrf_exempt
@require_POST
def install_nudge_event(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    event = str(payload.get("event") or "").strip().lower()
    platform = str(payload.get("platform") or "").strip().lower()
    if event not in {"shown", "dismissed", "installed", "confirmed"}:
        return _transaction_error_response(
            MiniAppTransactionError("invalid_install_event", "Unsupported install prompt event.")
        )
    if platform not in {"ios", "android", "desktop", "other", "standalone", "telegram"}:
        platform = "other"

    now = timezone.now()
    with db_transaction.atomic():
        state = InstallNudgeState.objects.select_for_update().filter(tg_user_id=result.tg_user_id).first()
        if state is None:
            state = InstallNudgeState.objects.create(
                tg_user_id=result.tg_user_id,
                next_prompt_at=now,
                next_telegram_reminder_at=now + INSTALL_NUDGE_TELEGRAM_FIRST_DELAY,
            )
        update_fields: list[str] = ["updated_at"]
        if event in {"installed", "confirmed"}:
            state.installed_at = now
            state.installed_platform = platform
            state.next_prompt_at = None
            state.next_telegram_reminder_at = None
            update_fields.extend(["installed_at", "installed_platform", "next_prompt_at", "next_telegram_reminder_at"])
        elif event == "shown" and state.installed_at is None:
            is_due = state.next_prompt_at is None or state.next_prompt_at <= now
            if is_due and int(state.prompt_count or 0) < INSTALL_NUDGE_MAX_PROMPTS:
                state.prompt_count = int(state.prompt_count or 0) + 1
                state.last_prompted_at = now
                state.next_prompt_at = INSTALL_NUDGE_PROMPT_DELAYS.get(state.prompt_count)
                if state.next_prompt_at is not None:
                    state.next_prompt_at = now + state.next_prompt_at
                update_fields.extend(["prompt_count", "last_prompted_at", "next_prompt_at"])
        state.save(update_fields=sorted(set(update_fields)))

    return _json_ok({"ok": True, "install_nudge": _install_nudge_payload(request, result, state)})


@csrf_exempt
@require_POST
def trial_recovery_event(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    event = str(payload.get("event") or "").strip().lower()
    try:
        trial_days = int(payload.get("trial_days") or 0)
    except (TypeError, ValueError):
        trial_days = 0
    if event not in {"shown", "dismissed", "bind_started"}:
        return _transaction_error_response(
            MiniAppTransactionError("invalid_trial_recovery_event", "Unsupported trial recovery event.")
        )
    recipient = record_trial_offer_event(result, event, trial_days=trial_days)
    return _json_ok({"ok": True, "enrolled": recipient is not None})


@csrf_exempt
@require_POST
def settings_profile(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    try:
        update_profile_settings(result, _read_json(request))
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok(_settings_payload(result))


@csrf_exempt
@require_POST
def settings_reminder(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    mode = str(payload.get("mode") or "").strip().lower()
    try:
        reminder_hour = int(payload.get("reminder_hour"))
    except (TypeError, ValueError):
        reminder_hour = -1
    if mode not in SUPPORTED_REMINDER_MODES or reminder_hour not in SUPPORTED_REMINDER_HOURS:
        return _transaction_error_response(
            MiniAppTransactionError("invalid_reminder_settings", "Choose a supported reminder schedule.")
        )
    DailyExpenseReminderSetting.objects.update_or_create(
        user_id=result.tg_user_id,
        defaults={"mode": mode, "reminder_hour": reminder_hour},
    )
    return _json_ok(_settings_payload(result))


@csrf_exempt
@require_POST
def settings_savings(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    try:
        update_saving_prompt_settings(result, _read_json(request))
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok(_settings_payload(result))


@csrf_exempt
@require_POST
def billing_action(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    payload = _read_json(request)
    action = str(payload.get("action") or "").strip().lower()
    consent_id = str(payload.get("consent_id") or "").strip()
    access = resolve_access(result)
    try:
        if action == "bind":
            if not consent_id:
                raise FunnelValidationError(
                    "billing_consent_required",
                    "Accept the current billing terms before opening checkout.",
                )
            get_billing_consent_for_user(consent_id=consent_id, user=result)
            if _billing_recovery_checkout_needed(access):
                invoice = build_recovery_invoice(user_id=result.tg_user_id)
                action_name = "recovery"
            else:
                mode = "rebind" if bool(access.get("has_card")) else "bind"
                invoice = build_bind_invoice(
                    user_id=result.tg_user_id,
                    trial_days=max(int(access.get("promo_trial_days") or access.get("trial_days") or 30), 1),
                    mode=mode,
                    promo_code=str(access.get("promo_code") or ""),
                )
                action_name = mode
            attach_consent_to_payment_id(
                consent_id=consent_id,
                user=result,
                payment_id=int(invoice["payment_id"]),
            )
            response: dict[str, object] = {
                "action": action_name,
                "action_url": str(invoice.get("page_url") or ""),
                "trial_days": int(invoice.get("trial_days") or 0),
                "trial_granted": bool(invoice.get("trial_granted")),
            }
        elif action == "retry":
            if not bool(access.get("has_card")):
                raise MiniAppTransactionError("card_not_bound", "Link a card before retrying the payment.")
            pending_url = str(access.get("last_action_url") or "").strip()
            if _billing_recovery_checkout_needed(access) and pending_url:
                response = {"action": "confirm_payment", "action_url": pending_url}
            else:
                charge = retry_monobank_charge(user_id=result.tg_user_id)
                response = {
                    "action": "retry",
                    "action_url": str(charge.get("action_url") or ""),
                    "charge_status": str(charge.get("status") or ""),
                }
        elif action == "cancel_autorenew":
            profile = cancel_auto_renew(user_id=result.tg_user_id)
            response = {
                "action": "cancel_autorenew",
                "auto_renew_enabled": bool(profile.auto_renew_enabled),
            }
        else:
            raise MiniAppTransactionError("invalid_billing_action", "Unsupported billing action.")
    except FunnelValidationError as exc:
        return _transaction_error_response(
            MiniAppTransactionError(exc.code, str(exc), status=exc.status)
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    except (MonobankAPIError, ValueError) as exc:
        return _transaction_error_response(MiniAppTransactionError("billing_unavailable", str(exc), status=502))

    response["settings"] = _settings_payload(result)
    return _json_ok(response)


@require_GET
def help_center(request: HttpRequest) -> JsonResponse:
    result = _get_session_user_or_response(request)
    if isinstance(result, JsonResponse):
        return result
    try:
        payload = request_help_content(
            locale=locale_for_user(result),
            topic_id=str(request.GET.get("topic") or "").strip(),
        )
    except MiniAppHelpError as exc:
        return JsonResponse(
            {"error": {"code": "help_unavailable", "message": str(exc)}},
            status=exc.status,
        )
    return _json_ok(payload)


@require_GET
def transaction_options(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        options = build_transaction_options(user, kind=request.GET.get("kind"))
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok(options)


@csrf_exempt
@require_POST
def transaction_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = secrets.token_urlsafe(16)
    try:
        batch_context = _batch_context_for_transaction_draft(request, payload)
        draft = build_transaction_draft(user, payload, draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)

    drafts = _session_transaction_drafts(request)
    entry: dict[str, object] = {"status": "draft", "draft": draft}
    if batch_context is not None:
        entry["ai_image_batch"] = batch_context
    _persist_draft_action(user=user, operation="normal_transaction", draft_id=draft_id, entry=entry)
    drafts[draft_id] = entry
    _save_session_transaction_drafts(request, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def ai_text_parse(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    text = str(_read_json(request).get("text") or "").strip()
    if not text:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Text is required."}}, status=400)
    if len(text) > AI_TEXT_MAX_LENGTH:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Text is too long."}}, status=400)
    try:
        parsed = request_ai_text_parse(text=text, default_currency=str(user.base_currency or "UAH"))
        parsed = guard_ai_text_intent(parsed, text)
        suggestion = build_ai_text_suggestion(user, parsed)
    except MiniAppAiTextError as exc:
        return JsonResponse({"error": {"code": "ai_text_unavailable", "message": str(exc)}}, status=exc.status)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True, "suggestion": suggestion})


@csrf_exempt
@require_POST
def ai_voice_parse(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    audio = request.FILES.get("audio")
    if audio is None:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Voice message is required."}}, status=400)
    if audio.size and audio.size > AI_VOICE_MAX_BYTES:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Voice message is too large."}}, status=400)

    audio_bytes = audio.read(AI_VOICE_MAX_BYTES + 1)
    if not audio_bytes or len(audio_bytes) > AI_VOICE_MAX_BYTES:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Voice message is invalid or too large."}}, status=400)
    mime_type = detect_ai_voice_mime(audio_bytes)
    if mime_type is None:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Use a WEBM, OGG, or M4A voice message."}}, status=400)

    try:
        result = request_ai_voice_parse(
            audio_bytes=audio_bytes,
            mime_type=mime_type,
            default_currency=str(user.base_currency or "UAH"),
        )
        parsed = result.get("parsed")
        if not isinstance(parsed, dict):
            raise MiniAppAiVoiceError("Voice recognition returned an invalid response.")
        transcript = str(result.get("transcript") or "").strip()[:AI_TEXT_MAX_LENGTH]
        parsed = guard_ai_text_intent(parsed, transcript)
        suggestion = build_ai_text_suggestion(user, parsed)
    except MiniAppAiVoiceError as exc:
        code = "invalid_payload" if exc.status == 400 else "ai_voice_unavailable"
        return JsonResponse({"error": {"code": code, "message": str(exc)}}, status=exc.status)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True, "transcript": transcript, "suggestion": suggestion})


@csrf_exempt
@require_POST
def ai_image_parse(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    image = request.FILES.get("image")
    if image is None:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Screenshot is required."}}, status=400)
    if image.size and image.size > AI_IMAGE_MAX_BYTES:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Screenshot is too large."}}, status=400)

    image_bytes = image.read(AI_IMAGE_MAX_BYTES + 1)
    if not image_bytes or len(image_bytes) > AI_IMAGE_MAX_BYTES:
        return JsonResponse({"error": {"code": "invalid_payload", "message": "Screenshot is invalid or too large."}}, status=400)
    mime_type = detect_ai_image_mime(image_bytes)
    if mime_type is None:
        return JsonResponse(
            {"error": {"code": "invalid_payload", "message": "Use a JPG, PNG, WEBP, GIF, HEIC, or HEIF screenshot."}},
            status=400,
        )
    try:
        image_bytes, mime_type = normalize_ai_image_upload(image_bytes, mime_type)
    except MiniAppImageUploadError:
        return JsonResponse(
            {
                "error": {
                    "code": "invalid_payload",
                    "message": "This HEIC or HEIF image could not be processed. Try another screenshot.",
                }
            },
            status=400,
        )

    try:
        parsed = request_ai_image_parse(
            image_bytes=image_bytes,
            mime_type=mime_type,
            default_currency=str(user.base_currency or "UAH"),
            catalog=build_ai_image_catalog(user),
        )
        if str(parsed.get("mode") or "").strip().lower() == "statement_expenses":
            batch = build_ai_image_batch(user, parsed, batch_id=secrets.token_urlsafe(16))
            batch["identity_scope"] = _identity_scope_binding(user)
            _save_session_ai_image_batch(request, batch)
            suggestion = _ai_image_batch_payload(batch) or {"mode": "batch"}
        else:
            suggestion = build_ai_image_suggestion(user, parsed)
    except MiniAppAiImageError as exc:
        code = "invalid_payload" if exc.status == 400 else "ai_image_unavailable"
        return JsonResponse({"error": {"code": code, "message": str(exc)}}, status=exc.status)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True, "suggestion": suggestion})


@require_GET
def ai_image_batch(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    return _json_ok({"ok": True, "batch": _ai_image_batch_payload(_session_ai_image_batch(request))})


@csrf_exempt
@require_POST
def ai_image_batch_item(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    payload = _read_json(request)
    batch = _session_ai_image_batch(request)
    batch_id = str(payload.get("batch_id") or "").strip()
    item_id = str(payload.get("item_id") or "").strip()
    action = str(payload.get("action") or "").strip().lower()
    if batch is None or not batch_id or str(batch.get("batch_id") or "") != batch_id:
        return _transaction_error_response(MiniAppTransactionError("batch_missing", "Statement batch was not found.", status=404))
    item = _find_ai_image_batch_item(batch, item_id)
    if item is None or str(item.get("status") or "pending") != "pending":
        return _transaction_error_response(MiniAppTransactionError("batch_item_unavailable", "Statement item is no longer available.", status=409))
    if action == "review":
        return _json_ok(
            {
                "ok": True,
                "suggestion": item.get("suggestion") if isinstance(item.get("suggestion"), dict) else {},
                "batch": _ai_image_batch_payload(batch),
            }
        )
    if action == "skip":
        item["status"] = "skipped"
        _save_session_ai_image_batch(request, batch)
        return _json_ok({"ok": True, "batch": _ai_image_batch_payload(batch)})
    return _transaction_error_response(MiniAppTransactionError("invalid_batch_action", "Unsupported statement action."))


@csrf_exempt
@require_POST
def transaction_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_transaction_drafts(request)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Transaction draft was not found."}}, status=404)
    batch_context = entry.get("ai_image_batch")
    if isinstance(batch_context, dict):
        batch = _session_ai_image_batch(request)
        item = _find_ai_image_batch_item(batch, batch_context.get("item_id")) if batch else None
        if item is not None and item.get("status") == "skipped":
            return _transaction_error_response(MiniAppTransactionError("batch_item_unavailable", "Statement item is no longer available.", status=409))

    def _commit() -> dict[str, object]:
        if isinstance(batch_context, dict):
            batch = _session_ai_image_batch(request)
            batch_item = _find_ai_image_batch_item(batch, batch_context.get("item_id")) if batch is not None else None
            if (
                batch is None
                or str(batch.get("batch_id") or "") != str(batch_context.get("batch_id") or "")
                or batch_item is None
                or str(batch_item.get("status") or "pending") != "pending"
            ):
                raise MiniAppTransactionError("batch_item_unavailable", "Statement item is no longer available.", status=409)
        return commit_transaction_draft(user, entry.get("draft") or {})

    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="normal_transaction",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=_commit,
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)

    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = result
    drafts[draft_id] = entry
    _save_session_transaction_drafts(request, drafts)
    batch_payload = _commit_ai_image_batch_item(request, batch_context)
    record_request_server_event(
        request,
        user=user,
        event_name="first_transaction_confirmed",
        idempotency_key=f"first_transaction:{user.tg_user_id}",
    )
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result, "batch": batch_payload})


@csrf_exempt
@require_POST
def transaction_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], TRANSACTION_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True, "batch": _ai_image_batch_payload(_session_ai_image_batch(request))})


@require_GET
def billing_expense_options(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        return _json_ok(build_billing_expense_options(user))
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)


@csrf_exempt
@require_POST
def billing_expense_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    try:
        billing_draft_id = int(payload.get("billing_draft_id") or 0)
    except (TypeError, ValueError):
        billing_draft_id = 0
    pending = AiTransactionDraft.objects.filter(
        id=billing_draft_id,
        tg_user_id=user.tg_user_id,
        source="billing_subscription",
        status="pending",
    ).first()
    if pending is None:
        return JsonResponse({"error": {"code": "billing_draft_missing", "message": "Billing expense draft was not found."}}, status=404)

    draft_id = secrets.token_urlsafe(16)
    transaction_payload = {
        "kind": "expense",
        "amount": payload.get("amount") or pending.amount,
        "currency": payload.get("currency") or pending.currency,
        "account_id": payload.get("account_id"),
        "category_id": payload.get("category_id"),
        "transaction_date": payload.get("transaction_date") or pending.transaction_date,
        "comment": payload.get("comment") if "comment" in payload else pending.comment,
    }
    try:
        draft = build_transaction_draft(user, transaction_payload, draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    draft["source"] = "miniapp_billing_subscription"
    drafts = _session_drafts(request, BILLING_EXPENSE_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft, "billing_draft_id": billing_draft_id}
    _persist_draft_action(user=user, operation="billing_expense", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, BILLING_EXPENSE_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def billing_expense_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, BILLING_EXPENSE_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Billing expense draft was not found."}}, status=404)

    billing_draft_id = int(entry.get("billing_draft_id") or 0)

    def _commit() -> dict[str, object]:
        pending = AiTransactionDraft.objects.select_for_update().filter(
            id=billing_draft_id,
            tg_user_id=user.tg_user_id,
            source="billing_subscription",
            status="pending",
        ).first()
        if pending is None:
            raise MiniAppTransactionError("billing_draft_inactive", "Billing expense draft is no longer active.", status=409)
        result = commit_transaction_draft(user, entry.get("draft") or {})
        pending.status = "completed"
        pending.confirmed_at = timezone.now()
        pending.save(update_fields=["status", "confirmed_at", "updated_at"])
        return result

    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="billing_expense",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=_commit,
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)

    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = result
    drafts[draft_id] = entry
    _save_session_drafts(request, BILLING_EXPENSE_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def billing_expense_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()

    def _cancel_pending(entry):
        billing_draft_id = int(entry.get("billing_draft_id") or 0)
        AiTransactionDraft.objects.filter(
            id=billing_draft_id,
            tg_user_id=user.tg_user_id,
            source="billing_subscription",
            status="pending",
        ).update(status="cancelled", cancelled_at=timezone.now(), updated_at=timezone.now())

    try:
        _cancel_session_draft(request, user, BILLING_EXPENSE_DRAFTS_SESSION_KEY, draft_id, cancel=_cancel_pending)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def account_options(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    return _json_ok(build_account_options(user))


@csrf_exempt
@require_POST
def account_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_account_draft(user, _read_json(request), draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, ACCOUNT_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation=f"account:{draft.get('action') or 'unknown'}", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, ACCOUNT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def account_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, ACCOUNT_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Account draft was not found."}}, status=404)
    draft = entry.get("draft") or {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation=f"account:{draft.get('action') or 'unknown'}",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_account_draft(user, draft),
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = result
    drafts[draft_id] = entry
    _save_session_drafts(request, ACCOUNT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def account_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    draft_id = str(_read_json(request).get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], ACCOUNT_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def transfer_options(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    return _json_ok(build_account_options(user))


@csrf_exempt
@require_POST
def transfer_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_transfer_draft(user, _read_json(request), draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, TRANSFER_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation="transfer", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, TRANSFER_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def transfer_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, TRANSFER_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Transfer draft was not found."}}, status=404)
    draft = entry.get("draft") or {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="transfer",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_transfer_draft(user, draft),
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = result
    drafts[draft_id] = entry
    _save_session_drafts(request, TRANSFER_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def transfer_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    draft_id = str(_read_json(request).get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], TRANSFER_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def debt_options(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    return _json_ok(build_debt_options(user))


@csrf_exempt
@require_POST
def debt_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_debt_draft(user, _read_json(request), draft_id=draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, DEBT_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation=f"debt:{draft.get('action') or 'unknown'}", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, DEBT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def debt_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, DEBT_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if not draft_id or entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Debt draft was not found."}}, status=404)
    draft = entry.get("draft") or {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation=f"debt:{draft.get('action') or 'unknown'}",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_debt_draft(user, draft),
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["idempotency_key"] = idempotency_key
    entry["result"] = result
    drafts[draft_id] = entry
    _save_session_drafts(request, DEBT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def debt_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    draft_id = str(_read_json(request).get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], DEBT_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def bootstrap(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        period = resolve_period(request.GET, locale=locale_for_user(user))
    except ValueError:
        return JsonResponse(
            {
                "error": {
                    "code": "invalid_period",
                    "message": _ui_message(locale_for_user(user), "Невірні параметри періоду.", "Invalid period parameters."),
                }
            },
            status=400,
        )
    return _json_ok(build_bootstrap(user, period))


def _section_response(request: HttpRequest, builder) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        period = resolve_period(request.GET, locale=locale_for_user(user))
    except ValueError:
        return JsonResponse(
            {
                "error": {
                    "code": "invalid_period",
                    "message": _ui_message(locale_for_user(user), "Невірні параметри періоду.", "Invalid period parameters."),
                }
            },
            status=400,
        )
    return _json_ok(builder(user, period))


def _saving_task_error_response(exc: MiniAppSavingTaskError) -> JsonResponse:
    return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)


def _history_error_response(exc: MiniAppHistoryError) -> JsonResponse:
    return JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)


@require_GET
def flow(request: HttpRequest) -> JsonResponse:
    return _section_response(request, build_flow_preview)


@require_GET
def saving_tasks(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        return _json_ok(build_saving_tasks_payload(user))
    except MiniAppSavingTaskError as exc:
        return _saving_task_error_response(exc)


@csrf_exempt
@require_POST
def saving_task_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_saving_task_draft(user, payload, draft_id=draft_id)
    except MiniAppSavingTaskError as exc:
        return _saving_task_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, SAVING_TASK_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation=f"saving_task_{str(draft.get('action') or 'task').strip().lower()}"[:64], draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, SAVING_TASK_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def saving_task_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, SAVING_TASK_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Planned saving draft was not found."}}, status=404)
    draft = entry.get("draft") if isinstance(entry.get("draft"), dict) else {}
    action = str(draft.get("action") or "task").strip().lower()
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation=f"saving_task_{action}"[:64],
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_saving_task_draft(user, draft),
        )
        tasks = build_saving_tasks_payload(user)
    except MiniAppSavingTaskError as exc:
        return _saving_task_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["result"] = result
    entry["idempotency_key"] = idempotency_key
    drafts[draft_id] = entry
    _save_session_drafts(request, SAVING_TASK_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result, "tasks": tasks})


@csrf_exempt
@require_POST
def saving_task_cancel_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], SAVING_TASK_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def history(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        period = resolve_period(request.GET, locale=locale_for_user(user))
    except ValueError:
        return JsonResponse({"error": {"code": "invalid_period", "message": "Invalid period parameters."}}, status=400)
    limit = _read_limit(request, default=100, maximum=200)
    cursor = str(request.GET.get("cursor") or "").strip()
    if len(cursor) > 4096:
        return JsonResponse({"error": {"code": "invalid_history_cursor", "message": "The history page cursor is invalid."}}, status=400)
    try:
        return _json_ok(
            build_history_payload(
                user,
                date_from=period.date_from,
                date_to=period.date_to,
                limit=limit,
                cursor=cursor or None,
            )
        )
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)


@csrf_exempt
@require_POST
def history_edit_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = build_history_edit_draft(user, _read_json(request), draft_id=draft_id)
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    drafts = _session_drafts(request, HISTORY_EDIT_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation="history_edit", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, HISTORY_EDIT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def history_edit_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, HISTORY_EDIT_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Transaction edit draft was not found."}}, status=404)
    draft = entry.get("draft") if isinstance(entry.get("draft"), dict) else {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="history_edit",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: commit_history_edit_draft(user, draft),
        )
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["result"] = result
    entry["idempotency_key"] = idempotency_key
    drafts[draft_id] = entry
    _save_session_drafts(request, HISTORY_EDIT_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def history_edit_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    draft_id = str(_read_json(request).get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], HISTORY_EDIT_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@csrf_exempt
@require_POST
def history_void_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    draft_id = secrets.token_urlsafe(16)
    try:
        draft = TransactionCancellationService(user).build_draft(_read_json(request), draft_id=draft_id)
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    drafts = _session_drafts(request, HISTORY_VOID_DRAFTS_SESSION_KEY)
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation="history_void", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, HISTORY_VOID_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def history_void_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, HISTORY_VOID_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Transaction cancellation draft was not found."}}, status=404)
    draft = entry.get("draft") if isinstance(entry.get("draft"), dict) else {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="history_void",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: TransactionCancellationService(user).commit_draft(draft),
        )
    except MiniAppHistoryError as exc:
        return _history_error_response(exc)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    entry["status"] = "committed"
    entry["result"] = result
    entry["idempotency_key"] = idempotency_key
    drafts[draft_id] = entry
    _save_session_drafts(request, HISTORY_VOID_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "result": result})


@csrf_exempt
@require_POST
def history_void_cancel(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    draft_id = str(_read_json(request).get("draft_id") or "").strip()
    try:
        _cancel_session_draft(request, authorized[0], HISTORY_VOID_DRAFTS_SESSION_KEY, draft_id)
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    return _json_ok({"ok": True})


@require_GET
def family_overview(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        return _json_ok(build_family_payload(user))
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)


@csrf_exempt
@require_POST
def family_create_draft(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    name = str(_read_json(request).get("name") or "").strip()[:255] or "Family budget"
    try:
        current = build_family_payload(user)
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)
    if str(current.get("mode") or "") == "family":
        return JsonResponse({"error": {"code": "already_in_family", "message": "You already belong to a family."}}, status=409)

    draft_id = secrets.token_urlsafe(16)
    drafts = _session_drafts(request, FAMILY_CREATE_DRAFTS_SESSION_KEY)
    draft = {
        "id": draft_id,
        "name": name,
        "moves_personal_data": True,
    }
    drafts[draft_id] = {"status": "draft", "draft": draft}
    _persist_draft_action(user=user, operation="family_create", draft_id=draft_id, entry=drafts[draft_id])
    _save_session_drafts(request, FAMILY_CREATE_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "draft": draft})


@csrf_exempt
@require_POST
def family_create_confirm(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    payload = _read_json(request)
    draft_id = str(payload.get("draft_id") or "").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip()
    drafts = _session_drafts(request, FAMILY_CREATE_DRAFTS_SESSION_KEY)
    entry = drafts.get(draft_id)
    if entry is None:
        return JsonResponse({"error": {"code": "draft_missing", "message": "Family draft was not found."}}, status=404)

    draft = entry.get("draft") if isinstance(entry.get("draft"), dict) else {}
    try:
        result, idempotent = _commit_draft_once(
            user=user,
            operation="family_create",
            draft_id=draft_id,
            idempotency_key=idempotency_key,
            entry=entry,
            commit=lambda: create_family(user, name=str(draft.get("name") or "")),
        )
    except MiniAppTransactionError as exc:
        return _transaction_error_response(exc)
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)

    entry["status"] = "committed"
    entry["result"] = result
    entry["idempotency_key"] = idempotency_key
    drafts[draft_id] = entry
    _save_session_drafts(request, FAMILY_CREATE_DRAFTS_SESSION_KEY, drafts)
    return _json_ok({"ok": True, "idempotent": idempotent, "family": result})


@csrf_exempt
@require_POST
def family_invite_create(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        return _json_ok({"ok": True, **create_family_invite(user)})
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)


@csrf_exempt
@require_POST
def family_invite_accept(request: HttpRequest) -> JsonResponse:
    user = _onboarding_user_or_response(request)
    if isinstance(user, JsonResponse):
        return user
    try:
        family = accept_family_invite(user, token=str(_read_json(request).get("token") or ""))
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)
    family_data = family.get("family") if isinstance(family.get("family"), dict) else {}
    owner_user_id = int(family_data.get("owner_user_id") or 0)
    family_id = int(family_data.get("id") or 0)
    if owner_user_id > 0:
        try:
            emit_notification(
                owner_user_id,
                "family_member_joined",
                idempotency_key=f"family-member-joined:{family_id}:{int(user.tg_user_id)}",
                context={"family_id": family_id, "member_user_id": int(user.tg_user_id)},
            )
        except Exception:
            pass
    return _json_ok({"ok": True, "family": family})


@csrf_exempt
@require_POST
def family_invite_revoke(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        invite_id = int(_read_json(request).get("invite_id") or 0)
    except (TypeError, ValueError):
        invite_id = 0
    if invite_id <= 0:
        return JsonResponse({"error": {"code": "invite_missing", "message": "The invite was not found."}}, status=404)
    try:
        return _json_ok({"ok": True, "family": revoke_family_invite(user, invite_id=invite_id)})
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)


@csrf_exempt
@require_POST
def family_member_remove(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    try:
        member_user_id = int(_read_json(request).get("member_user_id") or 0)
    except (TypeError, ValueError):
        member_user_id = 0
    if member_user_id <= 0:
        return JsonResponse({"error": {"code": "member_missing", "message": "The member was not found."}}, status=404)
    try:
        family = remove_family_member(user, member_user_id=member_user_id)
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)
    family_data = family.get("family") if isinstance(family.get("family"), dict) else {}
    try:
        emit_notification(
            member_user_id,
            "family_access_removed",
            idempotency_key=f"family-access-removed:{int(family_data.get('id') or 0)}:{member_user_id}:{timezone.now().date().isoformat()}",
            context={"family_id": int(family_data.get("id") or 0)},
        )
    except Exception:
        pass
    return _json_ok({"ok": True, "family": family})


@csrf_exempt
@require_POST
def family_leave(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    previous = build_family_payload(user)
    previous_family = previous.get("family") if isinstance(previous.get("family"), dict) else {}
    try:
        family = leave_family(user)
    except MiniAppFamilyError as exc:
        return _family_error_response(exc)
    owner_user_id = int(previous_family.get("owner_user_id") or 0)
    family_id = int(previous_family.get("id") or 0)
    if owner_user_id > 0:
        try:
            emit_notification(
                owner_user_id,
                "family_member_left",
                idempotency_key=f"family-member-left:{family_id}:{int(user.tg_user_id)}:{timezone.now().date().isoformat()}",
                context={"family_id": family_id, "member_user_id": int(user.tg_user_id)},
            )
        except Exception:
            pass
    return _json_ok({"ok": True, "family": family})


@require_GET
def categories(request: HttpRequest) -> JsonResponse:
    limit = _read_limit(request, default=5, maximum=30)
    return _section_response(request, lambda user, period: build_categories_preview(user, period, limit=limit))


@require_GET
def category_catalog(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    return _json_ok(build_category_catalog(user))


@require_GET
def activity(request: HttpRequest) -> JsonResponse:
    limit = _read_limit(request, default=5, maximum=50)
    return _section_response(request, lambda user, period: build_activity_preview(user, period, limit=limit))


@require_GET
def export_xlsx(request: HttpRequest) -> HttpResponse | JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    locale = locale_for_user(user)
    try:
        period = resolve_period(request.GET, locale=locale)
    except ValueError:
        return JsonResponse(
            {
                "error": {
                    "code": "invalid_period",
                    "message": _ui_message(locale, "Невірні параметри періоду.", "Invalid period parameters."),
                }
            },
            status=400,
        )

    try:
        workbook = request_export_xlsx(
            telegram_user_id=int(user.tg_user_id),
            date_from=period.date_from,
            date_to_exclusive=period.date_to + timedelta(days=1),
            title=period.label,
        )
    except MiniAppExportError as exc:
        is_empty = exc.status == 404
        return JsonResponse(
            {
                "error": {
                    "code": "export_empty" if is_empty else "export_unavailable",
                    "message": _ui_message(
                        locale,
                        "За цей період немає операцій для експорту." if is_empty else "Не вдалося сформувати Excel-файл. Спробуйте ще раз.",
                        "There are no operations to export for this period." if is_empty else "Couldn't create the Excel file. Try again.",
                    ),
                }
            },
            status=exc.status,
        )

    response = HttpResponse(
        workbook,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="my-cash-flow-{period.preset}.xlsx"'
    response["Cache-Control"] = "no-store"
    return response


@require_GET
def export_csv(request: HttpRequest) -> HttpResponse | JsonResponse:
    authorized = _authorized_write_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    locale = locale_for_user(user)
    try:
        period = resolve_period(request.GET, locale=locale)
    except ValueError:
        return JsonResponse(
            {
                "error": {
                    "code": "invalid_period",
                    "message": _ui_message(locale, "Невірні параметри періоду.", "Invalid period parameters."),
                }
            },
            status=400,
        )

    try:
        content = request_export_csv(
            telegram_user_id=int(user.tg_user_id),
            date_from=period.date_from,
            date_to_exclusive=period.date_to + timedelta(days=1),
            title=period.label,
        )
    except MiniAppExportError as exc:
        is_empty = exc.status == 404
        return JsonResponse(
            {
                "error": {
                    "code": "export_empty" if is_empty else "export_unavailable",
                    "message": _ui_message(
                        locale,
                        "За цей період немає операцій для експорту." if is_empty else "Не вдалося сформувати CSV-файл. Спробуйте ще раз.",
                        "There are no operations to export for this period." if is_empty else "Couldn't create the CSV file. Try again.",
                    ),
                }
            },
            status=exc.status,
        )

    response = HttpResponse(content, content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="my-cash-flow-{period.preset}.csv"'
    response["Cache-Control"] = "no-store"
    return response


@require_GET
def accounts(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    limit = _read_limit(request, default=8, maximum=50)
    return _json_ok(build_accounts_preview(user, limit=limit))


@require_GET
def credit_cards(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    limit = _read_limit(request, default=8, maximum=50)
    return _json_ok(build_credit_cards_preview(user, limit=limit))


@require_GET
def debts(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    limit = _read_limit(request, default=5, maximum=50)
    return _json_ok(build_debts_preview(user, limit=limit))


@require_GET
def goals(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    limit = _read_limit(request, default=4, maximum=20)
    return _json_ok(build_goals_preview(user, limit=limit))


@require_GET
def investments(request: HttpRequest) -> JsonResponse:
    authorized = _authorized_user_or_blocked(request)
    if isinstance(authorized, JsonResponse):
        return authorized
    user, _access = authorized
    limit = _read_limit(request, default=4, maximum=20)
    return _json_ok(build_investments_preview(user, limit=limit))
