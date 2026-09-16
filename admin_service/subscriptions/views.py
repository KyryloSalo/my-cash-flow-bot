from __future__ import annotations

import json

from django.core import signing
from django.conf import settings
from django.shortcuts import render
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from subscriptions.billing import (
    BILLING_BOT_URL,
    _payment_bind_trial_granted,
    build_recovery_invoice,
    build_bind_invoice,
    cancel_auto_renew,
    get_latest_bind_payment,
    get_latest_recovery_payment,
    parse_bind_return_token,
    process_monobank_event,
    retry_monobank_charge,
    send_bind_activation_notification,
    send_bind_processing_notification,
    sync_pending_charge_status,
    sync_pending_bind_status,
)
from subscriptions.models import Payment
from subscriptions.monobank import MonobankAPIError, verify_webhook_signature


def _read_json(request: HttpRequest) -> dict:
    if not request.body:
        return {}
    try:
        return json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        return {}


def _normalize_return_lang(value: object, *, default: str = "uk") -> str:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("en"):
        return "en"
    if normalized.startswith("uk"):
        return "uk"
    return default if default in {"uk", "en"} else "uk"


def _request_return_lang(request: HttpRequest, *, default: str = "uk") -> str:
    header_lang = str(request.headers.get("Accept-Language") or "").split(",", 1)[0]
    return _normalize_return_lang(request.GET.get("lang") or header_lang, default=default)


def _return_copy(lang: str, uk_text: str, en_text: str) -> str:
    return en_text if _normalize_return_lang(lang) == "en" else uk_text


def _resolve_return_lang(request: HttpRequest, explicit_lang: object = None) -> str:
    if explicit_lang is not None:
        return _normalize_return_lang(explicit_lang)
    token = str(request.GET.get("return_token") or "").strip()
    if token:
        try:
            payload = parse_bind_return_token(token)
        except (signing.BadSignature, signing.SignatureExpired, TypeError, ValueError):
            pass
        else:
            return _normalize_return_lang(payload.get("lang"))
    return _request_return_lang(request)


def _localized_return_text(lang: str, text: str) -> str:
    if _normalize_return_lang(lang) != "en":
        return text
    translations = {
        "Поверніться в Telegram": "Return to Telegram",
        "Доступ відновлено": "Access restored",
        "Доступ активовано": "Access activated",
        "Не вдалося підтвердити оплату": "Payment not confirmed",
        "Оплата обробляється": "Payment is processing",
        "Ми не змогли визначити статус цієї оплати на сайті. Відкрийте бота, щоб перевірити доступ і продовжити налаштування.": "We couldn't determine the payment status on the website. Open the bot to check access and continue.",
        "Посилання на сторінку оплати вже неактуальне або пошкоджене. Відкрийте бота, щоб перевірити доступ і продовжити налаштування.": "This payment page link is no longer valid or is damaged. Open the bot to check access and continue.",
        "Платіж знайти не вдалося. Відкрийте бота, щоб перевірити доступ або повторити оплату.": "We couldn't find the payment. Open the bot to check access or retry the payment.",
        "Платіж знайти не вдалося. Відкрийте бота, щоб перевірити доступ або повторити прив'язку картки.": "We couldn't find the payment. Open the bot to check access or retry the card binding.",
        "Оплату підтверджено. Відкрийте бота, щоб продовжити роботу.": "Payment confirmed. Open the bot to continue.",
        "Оплату підтверджено. Відкрийте бота, щоб завершити базове налаштування і перейти до кабінету.": "Payment confirmed. Open the bot to finish the basic setup and continue to the dashboard.",
        "Monobank ще не підтвердив успішну оплату. Поверніться в бота, щоб перевірити статус або спробувати ще раз.": "Monobank hasn't confirmed the payment yet. Return to the bot to check the status or try again.",
        "Monobank ще не підтвердив успішну прив'язку картки. Поверніться в бота, щоб перевірити статус або спробувати ще раз.": "Monobank hasn't confirmed the card binding yet. Return to the bot to check the status or try again.",
        "Ми вже перевіряємо оплату. Зазвичай підтвердження з'являється протягом 1-2 хвилин. Відкрийте бота, щоб побачити статус або продовжити після відновлення доступу.": "We're already checking the payment. Confirmation usually appears within 1-2 minutes. Open the bot to view the status or continue after access is restored.",
        "Ми вже перевіряємо платіж. Зазвичай підтвердження з'являється протягом 1-2 хвилин. Відкрийте бота, щоб побачити статус або продовжити після активації.": "We're already checking the payment. Confirmation usually appears within 1-2 minutes. Open the bot to view the status or continue after activation.",
    }
    return translations.get(text, text)


def _require_internal_token(request: HttpRequest) -> bool:
    header = request.headers.get("X-Internal-Token") or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    expected = settings.BILLING_INTERNAL_TOKEN
    return bool(expected and header and header == expected)


def _internal_auth_error(request: HttpRequest) -> JsonResponse | None:
    if not settings.BILLING_INTERNAL_TOKEN:
        return JsonResponse({"ok": False, "error": "BILLING_INTERNAL_TOKEN not configured."}, status=503)
    if not _require_internal_token(request):
        return JsonResponse({"ok": False, "error": "Unauthorized."}, status=401)
    return None


def _render_return_page(
    request: HttpRequest,
    *,
    lang: str | None = None,
    tone: str,
    title: str,
    message: str,
    eyebrow: str = "Vydno.Capital",
) -> HttpResponse:
    resolved_lang = _resolve_return_lang(request, lang)
    return render(
        request,
        "subscriptions/mono_return.html",
        {
            "tone": tone,
            "eyebrow": eyebrow,
            "title": _localized_return_text(resolved_lang, title),
            "message": _localized_return_text(resolved_lang, message),
            "bot_url": BILLING_BOT_URL,
            "html_lang": _normalize_return_lang(resolved_lang),
            "open_bot_label": _return_copy(resolved_lang, "Відкрити бота", "Open bot"),
            "open_bot_hint": _return_copy(
                resolved_lang,
                "Якщо бот уже відкритий, просто поверніться в Telegram і дочекайтеся оновлення статусу.",
                "If the bot is already open, return to Telegram and wait for the status to refresh.",
            ),
        },
    )


def _bind_payment_from_sync_result(sync_result: dict | None, fallback_payment: Payment | None) -> Payment | None:
    payment_id = int((sync_result or {}).get("payment_id") or 0)
    if payment_id > 0:
        payment = (
            Payment.objects.select_related("user", "subscription", "plan")
            .filter(pk=payment_id, kind=Payment.Kind.BIND)
            .first()
        )
        if payment is not None:
            return payment
    return fallback_payment


def _charge_payment_from_sync_result(sync_result: dict | None, fallback_payment: Payment | None) -> Payment | None:
    payment_id = int((sync_result or {}).get("payment_id") or 0)
    if payment_id > 0:
        payment = (
            Payment.objects.select_related("user", "subscription", "plan")
            .filter(pk=payment_id, kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY})
            .first()
        )
        if payment is not None:
            return payment
    return fallback_payment


@csrf_exempt
@require_POST
def monobank_webhook(request: HttpRequest) -> JsonResponse:
    if settings.MONO_WEBHOOK_VERIFY_SIGNATURES:
        signature = request.headers.get("x-sign")
        try:
            is_valid = verify_webhook_signature(body=request.body, signature=signature)
        except MonobankAPIError as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=503)
        if not is_valid:
            return JsonResponse({"ok": False, "error": "Invalid Monobank signature."}, status=401)

    payload = _read_json(request)
    payment, updated = process_monobank_event(payload)
    return JsonResponse(
        {
            "ok": True,
            "updated": updated,
            "payment_id": payment.pk if payment is not None else None,
        }
    )


@require_GET
def monobank_return(request: HttpRequest) -> HttpResponse:
    token = str(request.GET.get("return_token") or "").strip()
    if not token:
        return _render_return_page(
            request,
            tone="fallback",
            title="Поверніться в Telegram",
            message="Ми не змогли визначити статус цієї оплати на сайті. Відкрийте бота, щоб перевірити доступ і продовжити налаштування.",
        )
    try:
        payload = parse_bind_return_token(token)
    except (signing.BadSignature, signing.SignatureExpired, TypeError, ValueError):
        return _render_return_page(
            request,
            tone="fallback",
            title="Поверніться в Telegram",
            message="Посилання на сторінку оплати вже неактуальне або пошкоджене. Відкрийте бота, щоб перевірити доступ і продовжити налаштування.",
        )

    telegram_user_id = int(payload["telegram_user_id"])
    flow = str(payload.get("flow") or "bind")
    sync_result: dict | None = None
    if flow == "recovery":
        payment = get_latest_recovery_payment(user_id=telegram_user_id)
        if payment is not None and payment.status == Payment.Status.PENDING:
            try:
                sync_result = sync_pending_charge_status(user_id=telegram_user_id)
            except (ValueError, MonobankAPIError):
                sync_result = None
            payment = _charge_payment_from_sync_result(sync_result, get_latest_recovery_payment(user_id=telegram_user_id))
    else:
        payment = get_latest_bind_payment(user_id=telegram_user_id)
        if payment is not None and payment.status == Payment.Status.PENDING:
            try:
                sync_result = sync_pending_bind_status(user_id=telegram_user_id)
            except (ValueError, MonobankAPIError):
                sync_result = None
            payment = _bind_payment_from_sync_result(sync_result, get_latest_bind_payment(user_id=telegram_user_id))

    if payment is None:
        return _render_return_page(
            request,
            tone="fallback",
            title="Поверніться в Telegram",
            message=(
                "Платіж знайти не вдалося. Відкрийте бота, щоб перевірити доступ або повторити оплату."
                if flow == "recovery"
                else "Платіж знайти не вдалося. Відкрийте бота, щоб перевірити доступ або повторити прив'язку картки."
            ),
        )

    if payment.status == Payment.Status.PAID:
        if flow == "bind" and _payment_bind_trial_granted(payment):
            send_bind_activation_notification(payment)
        return _render_return_page(
            request,
            tone="success",
            title="Доступ відновлено" if flow == "recovery" else "Доступ активовано",
            message=(
                "Оплату підтверджено. Відкрийте бота, щоб продовжити роботу."
                if flow == "recovery"
                else "Оплату підтверджено. Відкрийте бота, щоб завершити базове налаштування і перейти до кабінету."
            ),
        )

    if payment.status in {Payment.Status.FAILED, Payment.Status.REJECTED, Payment.Status.REFUNDED}:
        return _render_return_page(
            request,
            tone="failure",
            title="Не вдалося підтвердити оплату",
            message=(
                "Monobank ще не підтвердив успішну оплату. Поверніться в бота, щоб перевірити статус або спробувати ще раз."
                if flow == "recovery"
                else "Monobank ще не підтвердив успішну прив'язку картки. Поверніться в бота, щоб перевірити статус або спробувати ще раз."
            ),
        )

    if flow == "bind":
        send_bind_processing_notification(payment)
    return _render_return_page(
        request,
        tone="processing",
        title="Оплата обробляється",
        message=(
            "Ми вже перевіряємо оплату. Зазвичай підтвердження з'являється протягом 1-2 хвилин. Відкрийте бота, щоб побачити статус або продовжити після відновлення доступу."
            if flow == "recovery"
            else "Ми вже перевіряємо платіж. Зазвичай підтвердження з'являється протягом 1-2 хвилин. Відкрийте бота, щоб побачити статус або продовжити після активації."
        ),
    )

@csrf_exempt
@require_POST
def monobank_init_bind(request: HttpRequest) -> JsonResponse:
    auth_error = _internal_auth_error(request)
    if auth_error is not None:
        return auth_error

    payload = _read_json(request)
    try:
        user_id = int(payload.get("telegram_user_id") or 0)
        trial_days = int(payload.get("trial_days") or 30)
        mode = str(payload.get("mode") or "bind")
        promo_code = str(payload.get("promo_code") or "")
        result = build_bind_invoice(user_id=user_id, trial_days=trial_days, mode=mode, promo_code=promo_code)
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)
    except MonobankAPIError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)

    return JsonResponse({"ok": True, **result})


@csrf_exempt
@require_POST
def monobank_init_recovery(request: HttpRequest) -> JsonResponse:
    auth_error = _internal_auth_error(request)
    if auth_error is not None:
        return auth_error

    payload = _read_json(request)
    try:
        user_id = int(payload.get("telegram_user_id") or 0)
        result = build_recovery_invoice(user_id=user_id)
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)
    except MonobankAPIError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)

    return JsonResponse({"ok": True, **result})


@csrf_exempt
@require_POST
def monobank_cancel_autorenew(request: HttpRequest) -> JsonResponse:
    auth_error = _internal_auth_error(request)
    if auth_error is not None:
        return auth_error

    payload = _read_json(request)
    try:
        user_id = int(payload.get("telegram_user_id") or 0)
        profile = cancel_auto_renew(user_id=user_id)
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)

    return JsonResponse(
        {
            "ok": True,
            "status": profile.status,
            "auto_renew_enabled": profile.auto_renew_enabled,
        }
    )


@csrf_exempt
@require_POST
def monobank_retry_renew(request: HttpRequest) -> JsonResponse:
    auth_error = _internal_auth_error(request)
    if auth_error is not None:
        return auth_error

    payload = _read_json(request)
    try:
        user_id = int(payload.get("telegram_user_id") or 0)
        intent = payload.get("intent_key")
        if not isinstance(intent, str) or not intent.strip():
            raise ValueError("Invalid billing intent key.")
        result = retry_monobank_charge(user_id=user_id, intent_key=intent.strip())
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)
    except MonobankAPIError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)

    return JsonResponse({"ok": True, **result})


@csrf_exempt
@require_POST
def monobank_sync_bind_status(request: HttpRequest) -> JsonResponse:
    auth_error = _internal_auth_error(request)
    if auth_error is not None:
        return auth_error

    payload = _read_json(request)
    try:
        user_id = int(payload.get("telegram_user_id") or 0)
        result = sync_pending_bind_status(user_id=user_id)
    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)
    except MonobankAPIError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=502)

    return JsonResponse({"ok": True, **result})
