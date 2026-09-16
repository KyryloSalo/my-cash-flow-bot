from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import html
import json
import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core import signing
from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from common.telegram import send_telegram_message
from miniapp.push import emit_notification
from subscriptions.models import BillingProfile, Payment, PromoOffer, PromoOfferClaim, Subscription, SubscriptionEvent
from subscriptions.monobank import (
    MonobankAPIError,
    cancel_invoice,
    charge_wallet_payment,
    create_invoice,
    delete_wallet_card,
    fetch_invoice_status,
    fetch_wallet_cards,
)
from subscriptions.payloads import sanitize_monobank_payload
from subscriptions.services import latest_subscription_for_user, sync_admin_access_scope, sync_admin_subscription_state
from users.models import TelegramUser, UserAdminState


logger = logging.getLogger(__name__)

MONO_PROVIDER = "monobank"
MONO_CURRENCY = "UAH"
MONO_CURRENCY_CODE = 980
MIN_BIND_INVOICE_VALIDITY_SECONDS = 300
DUPLICATE_BIND_REFUND_WINDOW = timedelta(hours=24)
TRIAL_FLOWS = {30, 90}
RETURN_TOKEN_SALT = "subscriptions.monobank.return"
RETURN_TOKEN_MAX_AGE_SECONDS = 24 * 60 * 60
RETURN_TOKEN_FLOWS = frozenset({"bind", "recovery"})
RETURN_TOKEN_LANGS = frozenset({"uk", "en"})
BOT_NOTIFICATION_PROCESSING = "processing_sent_at"
BOT_NOTIFICATION_ACTIVATION = "activation_sent_at"
BOT_NOTIFICATION_CHARGE_PROCESSING = "charge_processing_sent_at"
BOT_NOTIFICATION_CHARGE_ACTION_REQUIRED = "charge_action_required_sent_at"


def _normalize_ui_lang(value: Any, *, default: str = "uk") -> str:
    normalized = str(value or "").strip().lower()
    if normalized.startswith("en"):
        return "en"
    if normalized.startswith("uk"):
        return "uk"
    return default if default in RETURN_TOKEN_LANGS else "uk"
BOT_NOTIFICATION_CHARGE_PAID = "charge_paid_sent_at"
BOT_NOTIFICATION_CHARGE_FAILED = "charge_failed_sent_at"
BILLING_BOT_URL = "https://t.me/vydnocapital_bot"
BILLING_EXPENSE_DRAFT_SOURCE = "billing_subscription"
BILLING_EXPENSE_CATEGORY_NAME = "Підписки та сервіси"
BILLING_EXPENSE_CATEGORY_SLUG = "subscriptions_services"
BILLING_EXPENSE_COMMENT = "Оплата підписки vydno.capital"
BILLING_EXPENSE_LABEL = "vydno.capital"


def _emit_pwa_event(user_id: int, event_type: str, *, idempotency_key: str, context: dict[str, Any] | None = None) -> int:
    try:
        _notification, delivered, _created = emit_notification(
            user_id,
            event_type,
            idempotency_key=idempotency_key,
            context=context or {},
        )
        return delivered
    except Exception as exc:  # pragma: no cover - billing must never fail because push failed
        logger.warning("Billing PWA notification failed for %s/%s: %s", user_id, event_type, exc)
        return 0


def _minor_to_decimal(amount: int) -> Decimal:
    return (Decimal(amount) / Decimal("100")).quantize(Decimal("0.01"))


def _decimal_to_minor(amount: Decimal) -> int:
    return int((Decimal(amount).quantize(Decimal("0.01")) * Decimal("100")).to_integral_value())


def _bind_invoice_validity_seconds() -> int:
    return max(
        int(getattr(settings, "MONO_BIND_INVOICE_VALIDITY_SECONDS", 60 * 60) or 0),
        MIN_BIND_INVOICE_VALIDITY_SECONDS,
    )


def _trial_days(value: Any) -> int:
    try:
        days = int(value)
    except (TypeError, ValueError):
        days = 30
    return days if days in TRIAL_FLOWS else 30


def _normalize_promo_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_failure_reason(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _is_soft_grace_reason(value: Any) -> bool:
    normalized = _normalize_failure_reason(value)
    if not normalized:
        return False
    soft_markers = (
        "insufficient",
        "not enough",
        "low balance",
        "declin",
        "do not honor",
        "недостат",
        "не вистач",
        "брак кошт",
        "коштів недостат",
        "недостач",
    )
    return any(marker in normalized for marker in soft_markers)


def _grace_days_for_failure_reason(value: Any) -> int:
    soft_grace_days = max(int(getattr(settings, "MONO_SOFT_GRACE_DAYS", 1) or 0), 0)
    if soft_grace_days > 0 and _is_soft_grace_reason(value):
        return soft_grace_days
    return max(int(settings.MONO_GRACE_DAYS or 0), 0)


def _promo_offer_is_available(offer: PromoOffer) -> bool:
    if not offer.is_active:
        return False
    now = timezone.now()
    if offer.starts_at and offer.starts_at > now:
        return False
    if offer.ends_at and offer.ends_at < now:
        return False
    if offer.max_uses is not None:
        used_count = PromoOfferClaim.objects.filter(offer=offer, status=PromoOfferClaim.Status.CONSUMED).count()
        if used_count >= offer.max_uses:
            return False
    return True


def _wallet_id(user_id: int) -> str:
    return f"mono-user-{user_id}"


def build_bind_return_token(*, user_id: int, flow: str = "bind", lang: str | None = None) -> str:
    normalized_flow = str(flow or "").strip().lower() or "bind"
    if normalized_flow not in RETURN_TOKEN_FLOWS:
        raise ValueError(f"Unsupported billing return flow: {normalized_flow}")
    return signing.dumps(
        {
            "telegram_user_id": int(user_id),
            "flow": normalized_flow,
            "lang": _normalize_ui_lang(lang),
            "ts": int(timezone.now().timestamp()),
        },
        salt=RETURN_TOKEN_SALT,
        compress=True,
    )


def parse_bind_return_token(token: str, *, max_age: int = RETURN_TOKEN_MAX_AGE_SECONDS) -> dict[str, Any]:
    payload = signing.loads(token, salt=RETURN_TOKEN_SALT, max_age=max_age)
    if not isinstance(payload, dict):
        raise signing.BadSignature("Invalid return token payload.")
    telegram_user_id = int(payload.get("telegram_user_id") or 0)
    flow = str(payload.get("flow") or "").strip().lower()
    if telegram_user_id <= 0 or flow not in RETURN_TOKEN_FLOWS:
        raise signing.BadSignature("Invalid bind return token.")
    payload["telegram_user_id"] = telegram_user_id
    payload["flow"] = flow
    payload["lang"] = _normalize_ui_lang(payload.get("lang"), default="uk")
    return payload


def _append_query_params(url: str, params: dict[str, Any]) -> str:
    split = urlsplit(str(url))
    query = dict(parse_qsl(split.query, keep_blank_values=True))
    for key, value in params.items():
        if value is None:
            continue
        query[str(key)] = str(value)
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(query), split.fragment))


def _parse_timestamp(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            pass
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return None
    if raw > 10_000_000_000:
        return datetime.fromtimestamp(raw / 1000, tz=UTC)
    return datetime.fromtimestamp(raw, tz=UTC)


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    local_value = timezone.localtime(value, ZoneInfo(settings.TIME_ZONE))
    return local_value.strftime("%d.%m.%Y %H:%M")


def _serialize_datetime(value: datetime | None) -> str:
    return value.isoformat() if value is not None else ""


def _payment_status_from_mono(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "").strip().lower()
    if status in {"success", "paid"}:
        return Payment.Status.PAID
    if status in {"failure", "failed", "expired"}:
        return Payment.Status.FAILED
    if status in {"rejected", "canceled", "cancelled"}:
        return Payment.Status.REJECTED
    if status in {"reversed", "refunded"}:
        return Payment.Status.REFUNDED
    return Payment.Status.PENDING


def _payment_kind_title(kind: str) -> str:
    if kind == Payment.Kind.BIND:
        return "Прив’язка картки"
    if kind == Payment.Kind.RETRY:
        return "Повторна оплата"
    if kind == Payment.Kind.RENEWAL:
        return "Щомісячне списання"
    return "Оплата"


def _format_billing_amount(amount: Decimal | None, currency: str, *, brief_uah: bool = False) -> str:
    normalized_currency = str(currency or MONO_CURRENCY).strip().upper() or MONO_CURRENCY
    normalized_amount = (Decimal(amount or 0)).quantize(Decimal("0.01"))
    if normalized_amount == normalized_amount.to_integral_value():
        amount_text = str(int(normalized_amount))
    else:
        amount_text = f"{normalized_amount:.2f}".rstrip("0").rstrip(".")
    if normalized_currency == "UAH" and brief_uah:
        return f"{amount_text} грн"
    return f"{amount_text} {normalized_currency}"


def _billing_expense_tx_source(payment: Payment) -> str:
    return f"billing_paid_{int(payment.pk)}"


def _billing_expense_media_key(payment: Payment) -> str:
    return str(payment.provider_payment_id or f"payment:{int(payment.pk)}")


def _billing_expense_metadata(payment: Payment) -> dict[str, Any]:
    return {
        "origin": BILLING_EXPENSE_DRAFT_SOURCE,
        "source": _billing_expense_tx_source(payment),
        "payment_id": int(payment.pk),
        "provider_payment_id": str(payment.provider_payment_id or ""),
        "payment_kind": str(payment.kind or ""),
        "category_slug": BILLING_EXPENSE_CATEGORY_SLUG,
        "label": BILLING_EXPENSE_LABEL,
    }


def _billing_expense_buttons(draft_id: int) -> list[list[dict[str, str]]]:
    prefix = f"btx:{int(draft_id)}"
    return [
        [
            {"text": "✅ Підтвердити", "callback_data": f"{prefix}:tx:ok"},
            {"text": "✏️ Змінити суму", "callback_data": f"{prefix}:edit:amount"},
        ],
        [
            {"text": "🔃 Змінити тип", "callback_data": f"{prefix}:edit:type"},
            {"text": "Змінити рахунок", "callback_data": f"{prefix}:edit:account"},
        ],
        [
            {"text": "Змінити категорію", "callback_data": f"{prefix}:edit:category"},
            {"text": "📝 Змінити опис", "callback_data": f"{prefix}:edit:comment"},
        ],
        [
            {"text": "📅 Змінити дату", "callback_data": f"{prefix}:edit:date"},
            {"text": "❌ Скасувати", "callback_data": f"{prefix}:tx:cancel"},
        ],
        [{"text": "🏠 Головне меню", "callback_data": "home:show"}],
    ]


def _billing_expense_prompt_text(payment: Payment) -> str:
    charge_at = payment.paid_at or payment.provider_modified_at or timezone.now()
    charge_date = timezone.localtime(charge_at, ZoneInfo(settings.TIME_ZONE)).date().isoformat()
    amount_header = _format_billing_amount(payment.amount, payment.currency, brief_uah=True)
    amount_line = _format_billing_amount(payment.amount, payment.currency)
    comment = html.escape(BILLING_EXPENSE_COMMENT)
    return "\n".join(
        [
            f"Оплата підписки {html.escape(BILLING_EXPENSE_LABEL)} - {html.escape(amount_header)}",
            "",
            "<b>Перевірте чернетку</b>",
            "",
            f"<b>📅 Дата:</b> {html.escape(charge_date)}",
            "<b>🏷 Тип:</b> Витрата",
            f"<b>💰 Сума:</b> {html.escape(amount_line)}",
            "<b>🏦 Рахунок:</b> —",
            f"<b>📂 Категорія:</b> {html.escape(BILLING_EXPENSE_CATEGORY_NAME)}",
            f"<b>📝 Опис:</b> <i>{comment}</i>",
        ]
    )


def _extract_masked_pan(payload: dict[str, Any]) -> str:
    wallet_data = payload.get("walletData") or {}
    for key in ("maskedPan", "maskedPAN", "cardMask", "pan"):
        value = wallet_data.get(key)
        if value:
            return str(value)
    payment_info = payload.get("paymentInfo") or {}
    for key in ("maskedPan", "maskedPAN", "cardMask", "pan"):
        value = payment_info.get(key)
        if value:
            return str(value)
    for key in ("maskedPan", "maskedPAN", "cardMask", "pan"):
        value = payload.get(key)
        if value:
            return str(value)
    return ""


def _extract_card_token(payload: dict[str, Any]) -> str:
    wallet_data = payload.get("walletData") or {}
    return str(wallet_data.get("cardToken") or "")


def _merge_save_card_payload(payload: dict[str, Any], fallback_payload: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload)
    for field in ("walletData", "paymentInfo", "maskedPan", "maskedPAN", "cardMask", "pan"):
        if not merged.get(field) and fallback_payload.get(field):
            merged[field] = fallback_payload[field]
    return merged


def _wallet_id_from_save_card_payload(payment: Payment, payload: dict[str, Any]) -> str:
    wallet_data = payload.get("walletData") or {}
    wallet_id = str(wallet_data.get("walletId") or "").strip()
    if wallet_id:
        return wallet_id

    for context in (_bind_context(payment), _recovery_context(payment)):
        wallet_id = str(context.get("wallet_id") or "").strip()
        if wallet_id:
            return wallet_id
    return ""


def _wallet_card_token_fallback(
    *,
    payment: Payment,
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    wallet_id = _wallet_id_from_save_card_payload(payment, payload)
    if not wallet_id:
        return "", None

    try:
        wallet_payload = dict(fetch_wallet_cards(wallet_id))
    except MonobankAPIError as exc:
        logger.warning(
            "Monobank wallet token fallback failed invoiceId=%s kind=%s walletId=%s error=%s",
            payment.provider_payment_id,
            payment.kind,
            wallet_id,
            exc,
        )
        return "", None

    cards = wallet_payload.get("wallet") or []
    tokenized_cards = [
        card
        for card in cards
        if isinstance(card, dict) and str(card.get("cardToken") or "").strip()
    ]
    expected_masked_pan = _extract_masked_pan(payload)
    if expected_masked_pan:
        matching_cards = [
            card
            for card in tokenized_cards
            if str(card.get("maskedPan") or "").strip() == expected_masked_pan
        ]
        if len(matching_cards) == 1:
            return str(matching_cards[0]["cardToken"]).strip(), wallet_payload
        return "", wallet_payload
    if len(tokenized_cards) == 1:
        return str(tokenized_cards[0]["cardToken"]).strip(), wallet_payload
    return "", wallet_payload


def _recovery_context(payment: Payment) -> dict[str, Any]:
    raw_payload = payment.raw_payload or {}
    recovery_context = raw_payload.get("recovery_context") or {}
    if isinstance(recovery_context, dict):
        return recovery_context
    return {}


def _payment_is_recovery(payment: Payment) -> bool:
    return bool(_recovery_context(payment))


def _payment_uses_save_card_data(payment: Payment) -> bool:
    return payment.kind == Payment.Kind.BIND or _payment_is_recovery(payment)


def _resolve_paid_save_card_payload(payment: Payment, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    card_token = _extract_card_token(payload)
    if card_token or payment.status != Payment.Status.PAID or not _payment_uses_save_card_data(payment):
        return card_token, payload

    invoice_id = str(payload.get("invoiceId") or payment.provider_payment_id or "").strip()
    if not invoice_id:
        return "", payload

    refreshed_payload: dict[str, Any] | None = None
    try:
        refreshed_payload = dict(fetch_invoice_status(invoice_id))
        refreshed_payload.setdefault("invoiceId", invoice_id)
        payment.raw_payload = {
            **(payment.raw_payload or {}),
            "invoice_status_payload": sanitize_monobank_payload(refreshed_payload),
        }
        payment.save(update_fields=["raw_payload", "updated_at"])
    except MonobankAPIError as exc:
        logger.warning(
            "Monobank save-card token refetch failed invoiceId=%s kind=%s modifiedDate=%s has_wallet_data=%s error=%s",
            invoice_id,
            payment.kind,
            payload.get("modifiedDate"),
            bool(payload.get("walletData")),
            exc,
        )
        return "", payload

    resolved_payload = _merge_save_card_payload(payload, refreshed_payload)
    card_token = _extract_card_token(resolved_payload)
    if not card_token:
        card_token, wallet_payload = _wallet_card_token_fallback(payment=payment, payload=resolved_payload)
        if wallet_payload is not None:
            payment.raw_payload = {
                **(payment.raw_payload or {}),
                "wallet_cards_payload": sanitize_monobank_payload(wallet_payload),
            }
            payment.save(update_fields=["raw_payload", "updated_at"])
        if card_token:
            wallet_data = dict(resolved_payload.get("walletData") or {})
            wallet_data["cardToken"] = card_token
            resolved_payload = {**resolved_payload, "walletData": wallet_data}
    if not card_token:
        logger.warning(
            "Monobank paid save-card flow missing cardToken invoiceId=%s kind=%s modifiedDate=%s has_wallet_data=%s refetch_status=%s refetch_has_wallet_data=%s",
            invoice_id,
            payment.kind,
            payload.get("modifiedDate"),
            bool(payload.get("walletData")),
            refreshed_payload.get("status"),
            bool(refreshed_payload.get("walletData")),
        )
    return card_token, resolved_payload


def _extract_external_transaction_id(payload: dict[str, Any]) -> str:
    for key in ("transId", "transactionId", "paymentId"):
        value = payload.get(key)
        if value:
            return str(value)
    wallet_data = payload.get("walletData") or {}
    for key in ("transId", "transactionId"):
        value = wallet_data.get(key)
        if value:
            return str(value)
    return ""


def _find_payment(invoice_id: str) -> Payment | None:
    if not invoice_id:
        return None
    return (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(provider=MONO_PROVIDER, provider_payment_id=invoice_id)
        .first()
    )


def get_latest_bind_payment(*, user_id: int) -> Payment | None:
    return (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
        )
        .order_by("-created_at", "-pk")
        .first()
    )


def _pending_bind_checkout_for_reuse(*, user_id: int) -> dict[str, Any] | None:
    cutoff = timezone.now() - timedelta(seconds=_bind_invoice_validity_seconds())
    payments = (
        Payment.objects.select_for_update()
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
            status__in={Payment.Status.CREATED, Payment.Status.PENDING},
            created_at__gte=cutoff,
        )
        .order_by("-created_at", "-pk")[:5]
    )
    for payment in payments:
        raw_payload = payment.raw_payload or {}
        bind_context = raw_payload.get("bind_context") or {}
        create_response = raw_payload.get("invoice_create_response") or {}
        page_url = str(bind_context.get("page_url") or create_response.get("pageUrl") or "").strip()
        if not payment.provider_payment_id or not page_url:
            continue
        return {
            "payment_id": payment.pk,
            "invoice_id": payment.provider_payment_id,
            "page_url": page_url,
            "trial_days": int(bind_context.get("trial_days") or 0),
            "trial_granted": bool(bind_context.get("trial_granted")),
            "mode": str(bind_context.get("mode") or "rebind"),
            "promo_code": str(bind_context.get("promo_code") or ""),
            "reused": True,
        }
    return None


def get_latest_recovery_payment(*, user_id: int) -> Payment | None:
    payments = (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.RETRY,
        )
        .order_by("-created_at", "-pk")[:20]
    )
    for payment in payments:
        if _payment_is_recovery(payment):
            return payment
    return None


def _latest_charge_payment_for_user(user_id: int) -> Payment | None:
    return (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
            status__in={Payment.Status.PAID, Payment.Status.REFUNDED},
        )
        .order_by("-paid_at", "-created_at", "-pk")
        .first()
    )


def _latest_pending_charge_payment_for_user(user_id: int) -> Payment | None:
    return (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
            status=Payment.Status.PENDING,
        )
        .order_by("-created_at", "-pk")
        .first()
    )


def _latest_pending_recovery_payment_for_user(user_id: int) -> Payment | None:
    payments = (
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.RETRY,
            status=Payment.Status.PENDING,
        )
        .order_by("-created_at", "-pk")[:20]
    )
    for payment in payments:
        if _payment_is_recovery(payment):
            return payment
    return None


def _refund_already_requested(payment: Payment) -> bool:
    if payment.status == Payment.Status.REFUNDED:
        return True
    raw_payload = payment.raw_payload or {}
    refund_request = raw_payload.get("refund_request") or {}
    request_status = str(refund_request.get("status") or "").strip().lower()
    return request_status in {"processing", "success", "unknown"}


def _duplicate_bind_identity(payment: Payment) -> tuple[str, str] | None:
    bind_context = (payment.raw_payload or {}).get("bind_context") or {}
    if not isinstance(bind_context, dict) or not bind_context.get("wallet_id"):
        return None
    raw_payload = payment.raw_payload or {}
    provider_payload = raw_payload.get("webhook_payload") or raw_payload.get("invoice_status_payload") or {}
    return str(bind_context.get("wallet_id") or ""), _extract_masked_pan(provider_payload)


def _bind_payments_look_duplicate(previous: Payment, candidate: Payment) -> bool:
    previous_identity = _duplicate_bind_identity(previous)
    candidate_identity = _duplicate_bind_identity(candidate)
    if previous_identity is None or candidate_identity is None:
        return False
    previous_wallet, previous_pan = previous_identity
    candidate_wallet, candidate_pan = candidate_identity
    if previous_wallet != candidate_wallet:
        return False
    if previous_pan and candidate_pan and previous_pan != candidate_pan:
        return False
    return True


def _payment_effective_at(payment: Payment) -> datetime:
    return payment.paid_at or payment.created_at


def _latest_duplicate_paid_bind_for_user(user_id: int) -> Payment | None:
    payments = list(
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
            status=Payment.Status.PAID,
            amount=_minor_to_decimal(settings.MONO_BIND_AMOUNT),
            currency=MONO_CURRENCY,
        )
        .order_by("paid_at", "created_at", "pk")
    )
    for candidate_index in range(len(payments) - 1, 0, -1):
        candidate = payments[candidate_index]
        if _refund_already_requested(candidate):
            continue
        if _duplicate_bind_identity(candidate) is None:
            continue
        candidate_at = _payment_effective_at(candidate)
        for previous in reversed(payments[:candidate_index]):
            if not _bind_payments_look_duplicate(previous, candidate):
                continue
            delta = candidate_at - _payment_effective_at(previous)
            if timedelta(0) <= delta <= DUPLICATE_BIND_REFUND_WINDOW:
                return candidate
            break
    return None


def get_latest_refundable_monobank_payment(*, user_id: int) -> Payment | None:
    candidates = [
        payment
        for payment in (
            _latest_charge_payment_for_user(user_id),
            _latest_duplicate_paid_bind_for_user(user_id),
        )
        if payment is not None and payment.status == Payment.Status.PAID and not _refund_already_requested(payment)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda payment: (_payment_effective_at(payment), payment.pk))


def _subscription_snapshot(subscription: Subscription | None) -> dict[str, Any]:
    if subscription is None:
        return {}
    return {
        "status": subscription.status,
        "provider": subscription.provider,
        "source": subscription.source,
        "amount": str(subscription.amount) if subscription.amount is not None else "",
        "currency": subscription.currency or "",
        "started_at": _serialize_datetime(subscription.started_at),
        "expires_at": _serialize_datetime(subscription.expires_at),
        "next_charge_at": _serialize_datetime(subscription.next_charge_at),
        "grace_expires_at": _serialize_datetime(subscription.grace_expires_at),
        "trial_days": int(subscription.trial_days or 0),
        "auto_renew": bool(subscription.auto_renew),
        "payment_id": subscription.payment_id or "",
        "comment": subscription.comment or "",
    }


def _subscription_grants_full_access(subscription: Subscription | None) -> bool:
    if subscription is None:
        return False
    if subscription.status not in {
        Subscription.Status.TRIAL,
        Subscription.Status.ACTIVE,
        Subscription.Status.PAID,
        Subscription.Status.MANUAL,
        Subscription.Status.LIFETIME,
    }:
        return False
    if subscription.expires_at is None:
        return True
    return subscription.expires_at > timezone.now()


def _restore_subscription_from_snapshot(payment: Payment) -> Subscription | None:
    subscription = payment.subscription or latest_subscription_for_user(payment.user)
    if subscription is None:
        return None
    current = _subscription_snapshot(subscription)
    # Replay the explicit grant chain, not a historical snapshot over newer state.
    grants = {p.provider_payment_id: p for p in Payment.objects.filter(subscription=subscription,
        provider=MONO_PROVIDER, raw_payload__has_key="entitlement_effect")}
    chain = []
    cursor = subscription.payment_id
    while cursor in grants and cursor not in {p.provider_payment_id for p in chain}:
        grant = grants[cursor]
        chain.append(grant)
        cursor = grant.raw_payload["entitlement_effect"]["before"].get("payment_id", "")
    fields = ("status", "provider", "source", "expires_at", "payment_id", "started_at", "trial_days", "comment")
    latest = chain[0] if chain else None
    expected = (latest.raw_payload.get("entitlement_projection") or latest.raw_payload["entitlement_effect"]["after"]) if latest else {}
    safe = any(p.pk == payment.pk for p in chain) and all(current.get(k) == expected.get(k) for k in fields)
    if safe:
        snapshot = dict(chain[-1].raw_payload["entitlement_effect"]["before"])
        for grant in reversed(chain):
            if grant.pk == payment.pk or grant.status == Payment.Status.REFUNDED:
                continue
            effect = grant.raw_payload["entitlement_effect"]
            at = _parse_timestamp(effect["granted_at"])
            base = max(_parse_timestamp(snapshot.get("expires_at")) or at, at)
            snapshot = {**effect["after"], "expires_at": _serialize_datetime(base + timedelta(days=effect["days"]))}
    else:
        # Legacy payments have no exact grant ledger. Only a matching latest
        # provider grant with its expected expiry can use a pre-charge snapshot.
        snapshot = ((payment.raw_payload or {}).get("charge_context") or {}).get("subscription_snapshot_before_charge") or {}
        prior_end = _parse_timestamp(snapshot.get("expires_at"))
        paid_at = payment.paid_at
        expected_end = max(prior_end or paid_at, paid_at) + timedelta(days=settings.MONO_RENEWAL_PERIOD_DAYS) if paid_at else None
        safe = (not chain and bool(snapshot) and current["payment_id"] == payment.provider_payment_id
                and subscription.source == Subscription.Source.PAYMENT
                and subscription.status == Subscription.Status.ACTIVE and subscription.expires_at == expected_end)
    if not safe:
        payment.raw_payload = {**payment.raw_payload, "entitlement_refund": {
            "state": "needs_review", "reason": "Independent or unversioned entitlement changed; preserved",
            "recorded_at": _serialize_datetime(timezone.now()),
        }}
        payment.save(update_fields=["raw_payload", "updated_at"])
        logger.warning("Refund entitlement needs operator review: payment_id=%s", payment.pk)
        return subscription
    for field in ("status", "provider", "source", "currency", "payment_id", "comment"):
        setattr(subscription, field, snapshot.get(field) or ("" if field in {"payment_id", "comment"} else getattr(subscription, field)))
    subscription.amount = Decimal(snapshot["amount"]) if snapshot.get("amount") else None
    for field in ("started_at", "expires_at", "grace_expires_at"):
        setattr(subscription, field, _parse_timestamp(snapshot.get(field)))
    subscription.trial_days = int(snapshot.get("trial_days") or 0)
    is_latest_refund = current["payment_id"] == payment.provider_payment_id
    if is_latest_refund:
        subscription.auto_renew = False
        subscription.next_charge_at = None
    elif subscription.auto_renew:
        subscription.next_charge_at = subscription.expires_at
    subscription.save()
    payment.raw_payload = {**payment.raw_payload, "entitlement_refund": {"state": "applied"}}
    payment.save(update_fields=["raw_payload", "updated_at"])
    if latest and latest.pk != payment.pk:
        latest.raw_payload = {**latest.raw_payload, "entitlement_projection": _subscription_snapshot(subscription)}
        latest.save(update_fields=["raw_payload", "updated_at"])
    sync_admin_subscription_state(payment.user_id, subscription=subscription)
    sync_admin_access_scope(payment.user_id, access_scope=(UserAdminState.AccessScope.PERSONAL_FULL
        if _subscription_grants_full_access(subscription) else UserAdminState.AccessScope.PAYWALL), access_source="billing")
    return subscription

def _send_user_message(user_id: int, text: str, *, action_url: str = "") -> bool:
    if not settings.TELEGRAM_BOT_TOKEN:
        return False
    buttons = [[{"text": "Відкрити оплату", "url": action_url}]] if action_url else None
    try:
        send_telegram_message(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
            buttons=buttons,
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Billing Telegram notification failed for %s: %s", user_id, exc)
        return False
    return True


def _payment_bot_notifications(payment: Payment) -> dict[str, Any]:
    raw_payload = payment.raw_payload or {}
    notifications = raw_payload.get("bot_notifications") or {}
    if isinstance(notifications, dict):
        return dict(notifications)
    return {}


def _payment_notification_sent(payment: Payment, key: str) -> bool:
    return bool(str(_payment_bot_notifications(payment).get(key) or "").strip())


def _mark_payment_notification_sent(payment: Payment, key: str) -> None:
    raw_payload = dict(payment.raw_payload or {})
    notifications = _payment_bot_notifications(payment)
    notifications[key] = _serialize_datetime(timezone.now())
    raw_payload["bot_notifications"] = notifications
    payment.raw_payload = raw_payload
    payment.save(update_fields=["raw_payload", "updated_at"])


def _send_payment_notification_once(payment: Payment, key: str, text: str, action_url: str = "") -> bool:
    if _payment_notification_sent(payment, key):
        return False
    if not _send_user_message(payment.user_id, text, action_url=action_url):
        return False
    _mark_payment_notification_sent(payment, key)
    return True


def _create_or_refresh_billing_expense_draft(payment: Payment) -> int | None:
    tx_date = (payment.paid_at or payment.provider_modified_at or timezone.now()).date()
    metadata_json = json.dumps(_billing_expense_metadata(payment), ensure_ascii=False)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ai_transaction_drafts (
                  tg_user_id,
                  source,
                  telegram_file_unique_id,
                  telegram_file_id,
                  telegram_message_id,
                  status,
                  transaction_date,
                  tx_type,
                  amount,
                  currency,
                  account_id,
                  category_id,
                  comment,
                  confidence,
                  metadata,
                  created_at,
                  updated_at
                )
                VALUES (
                  %s, %s, %s, NULL, NULL,
                  'pending',
                  %s, 'expense', %s, %s, NULL, NULL, %s, NULL, %s::jsonb,
                  now(), now()
                )
                ON CONFLICT (tg_user_id, source, telegram_file_unique_id) DO NOTHING
                RETURNING id
                """,
                [
                    int(payment.user_id),
                    BILLING_EXPENSE_DRAFT_SOURCE,
                    _billing_expense_media_key(payment),
                    tx_date,
                    payment.amount,
                    str(payment.currency or MONO_CURRENCY),
                    BILLING_EXPENSE_COMMENT,
                    metadata_json,
                ],
            )
            row = cursor.fetchone()
            if row:
                return int(row[0])
            cursor.execute(
                """
                SELECT id
                FROM ai_transaction_drafts
                WHERE tg_user_id=%s
                  AND source=%s
                  AND telegram_file_unique_id=%s
                LIMIT 1
                """,
                [
                    int(payment.user_id),
                    BILLING_EXPENSE_DRAFT_SOURCE,
                    _billing_expense_media_key(payment),
                ],
            )
            row = cursor.fetchone()
            return int(row[0]) if row else None
    except Exception as exc:  # pragma: no cover
        logger.warning("Billing expense draft create failed for payment=%s: %s", payment.pk, exc)
        return None


def _send_billing_expense_prompt_once(payment: Payment) -> bool:
    if _payment_notification_sent(payment, BOT_NOTIFICATION_CHARGE_PAID):
        return False
    draft_id = _create_or_refresh_billing_expense_draft(payment)
    if draft_id is None:
        return False
    pushed = _emit_pwa_event(
        payment.user_id,
        "billing_paid_expense",
        idempotency_key=f"billing-paid-expense:{payment.pk}",
        context={"payment_id": payment.pk, "draft_id": draft_id},
    )
    telegram_sent = False
    if settings.TELEGRAM_BOT_TOKEN:
        try:
            send_telegram_message(
                bot_token=settings.TELEGRAM_BOT_TOKEN,
                chat_id=payment.user_id,
                text=_billing_expense_prompt_text(payment),
                parse_mode="HTML",
                buttons=_billing_expense_buttons(draft_id),
            )
            telegram_sent = True
        except Exception as exc:  # pragma: no cover
            logger.warning("Billing expense prompt failed for payment=%s: %s", payment.pk, exc)
    if pushed or telegram_sent:
        _mark_payment_notification_sent(payment, BOT_NOTIFICATION_CHARGE_PAID)
    return bool(pushed or telegram_sent)


def _bind_context(payment: Payment) -> dict[str, Any]:
    raw_payload = payment.raw_payload or {}
    bind_context = raw_payload.get("bind_context") or {}
    if isinstance(bind_context, dict):
        return bind_context
    return {}


def _bind_mode(payment: Payment) -> str:
    return str(_bind_context(payment).get("mode") or "bind")


def _payment_bind_trial_granted(payment: Payment) -> bool:
    return bool(_bind_context(payment).get("trial_granted"))


def _bind_activation_text(*, support_note: str = "") -> str:
    lines = [
        "Готово, доступ активовано ✅",
        "",
        "Тепер можна користуватись vydno.capital на повну: додавати витрати, доходи, рахунки, борги, цілі та дивитись звіти без хаосу в голові.",
        "",
        "Почнемо з базового налаштування, це займе кілька хвилин.",
    ]
    if support_note:
        lines.extend(["", support_note])
    return "\n".join(lines)


def send_bind_processing_notification(payment: Payment) -> bool:
    if payment.kind != Payment.Kind.BIND or _payment_notification_sent(payment, BOT_NOTIFICATION_PROCESSING):
        return False
    if not settings.TELEGRAM_BOT_TOKEN:
        return False
    try:
        send_telegram_message(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=payment.user_id,
            text="⏳ Ваша оплата обробляється, зачекайте 1-2 хв",
            buttons=[
                [{"text": "Перевірити оплату", "callback_data": "settings:billing"}],
                [{"text": "Назад", "callback_data": "home:show"}],
            ],
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Billing processing notification failed for %s: %s", payment.user_id, exc)
        return False
    _mark_payment_notification_sent(payment, BOT_NOTIFICATION_PROCESSING)
    return True


def send_bind_activation_notification(payment: Payment, *, support_note: str = "") -> bool:
    if payment.kind != Payment.Kind.BIND or _payment_notification_sent(payment, BOT_NOTIFICATION_ACTIVATION):
        return False
    if not settings.TELEGRAM_BOT_TOKEN:
        return False
    try:
        send_telegram_message(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=payment.user_id,
            text=_bind_activation_text(support_note=support_note),
            buttons=[[{"text": "Продовжити", "callback_data": "billing:continue"}]],
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("Billing activation notification failed for %s: %s", payment.user_id, exc)
        return False
    _mark_payment_notification_sent(payment, BOT_NOTIFICATION_ACTIVATION)
    return True


def _subscription_notice(subscription: Subscription) -> str:
    if subscription.status == Subscription.Status.TRIAL:
        return f"Пробний доступ активовано до {_format_dt(subscription.expires_at)}."
    if subscription.next_charge_at:
        return f"Наступне списання заплановане на {_format_dt(subscription.next_charge_at)}."
    if subscription.expires_at:
        return f"Доступ активний до {_format_dt(subscription.expires_at)}."
    return "Підписка активна."


def _renewal_failure_message(subscription: Subscription | None, *, grace_days: int) -> str:
    soft_grace_days = max(int(getattr(settings, "MONO_SOFT_GRACE_DAYS", 1) or 0), 0)
    if subscription is not None and soft_grace_days > 0 and grace_days == soft_grace_days:
        return (
            "<b>Автосписання не пройшло.</b>\n\n"
            f"Ми залишили повний доступ до {_format_dt(subscription.grace_expires_at)}. "
            "Поповніть картку або оновіть спосіб оплати в меню підписки, "
            "щоб доступ не зупинився."
        )
    return (
        "<b>Автосписання не пройшло.</b>\n\n"
        "Доступ поставлено на паузу до повторної оплати. "
        "У меню підписки можна оновити картку або запустити оплату вручну."
    )


def _upsert_billing_profile(
    *,
    user: TelegramUser,
    status: str,
    auto_renew_enabled: bool,
    card_token: str = "",
    clear_card_token: bool = False,
    masked_pan: str = "",
    last_charge_status: str = "",
    last_failure_reason: str = "",
    last_action_url: str = "",
    last_bound_at: datetime | None = None,
    last_charge_at: datetime | None = None,
) -> BillingProfile:
    profile, _ = BillingProfile.objects.get_or_create(
        user_id=user.tg_user_id,
        defaults={"provider": MONO_PROVIDER, "wallet_id": _wallet_id(user.tg_user_id)},
    )
    if not profile.wallet_id:
        profile.wallet_id = _wallet_id(user.tg_user_id)
    if clear_card_token:
        profile.card_token = ""
    if card_token:
        profile.card_token = card_token
    if masked_pan:
        profile.masked_pan = masked_pan
    profile.provider = MONO_PROVIDER
    profile.status = status
    profile.auto_renew_enabled = auto_renew_enabled
    profile.last_charge_status = last_charge_status
    profile.last_failure_reason = last_failure_reason
    profile.last_action_url = last_action_url
    if last_bound_at is not None:
        profile.last_bound_at = last_bound_at
    if last_charge_at is not None:
        profile.last_charge_at = last_charge_at
    profile.save()
    return profile


def _charge_context_payload(*, subscription: Subscription | None, wallet_id: str, payment_kind: str) -> dict[str, Any]:
    return {
        "kind": payment_kind,
        "wallet_id": wallet_id,
        "subscription_snapshot_before_charge": _subscription_snapshot(subscription),
    }


def _set_external_transaction_id(payment: Payment, payload: dict[str, Any]) -> None:
    value = _extract_external_transaction_id(payload)
    if not value or value == payment.provider_payment_id:
        return
    exists = Payment.objects.exclude(pk=payment.pk).filter(external_transaction_id=value).exists()
    if not exists:
        payment.external_transaction_id = value


def _save_subscription_event(subscription: Subscription | None, *, user_id: int, event_type: str, payload: dict[str, Any]) -> None:
    if subscription is None:
        return
    SubscriptionEvent.objects.create(
        subscription=subscription,
        user_id=user_id,
        event_type=event_type,
        payload=payload,
    )


def _resolve_promo_offer(*, user: TelegramUser, promo_code: str, mode: str) -> PromoOffer | None:
    normalized_code = _normalize_promo_code(promo_code)
    if mode != "bind" or not normalized_code:
        return None
    if Subscription.objects.filter(user_id=user.tg_user_id).exists():
        return None
    offer = PromoOffer.objects.filter(code=normalized_code).first()
    if offer is None or not _promo_offer_is_available(offer):
        return None
    existing_claim = PromoOfferClaim.objects.filter(offer=offer, user=user).first()
    if existing_claim and existing_claim.status == PromoOfferClaim.Status.CONSUMED:
        return None
    return offer


def _consume_promo_claim(
    *,
    payment: Payment,
    subscription: Subscription,
    promo_offer_id: int | None,
    promo_code: str,
) -> None:
    if not promo_offer_id or not promo_code:
        return
    claim, _ = PromoOfferClaim.objects.get_or_create(
        offer_id=promo_offer_id,
        user_id=payment.user_id,
        defaults={"status": PromoOfferClaim.Status.PENDING},
    )
    claim.status = PromoOfferClaim.Status.CONSUMED
    claim.bind_payment = payment
    claim.subscription = subscription
    claim.consumed_at = timezone.now()
    claim.save(update_fields=["status", "bind_payment", "subscription", "consumed_at", "updated_at"])
    sync_admin_access_scope(
        payment.user_id,
        access_scope=UserAdminState.AccessScope.PERSONAL_FULL,
        access_source="promo",
        clear_pending_start_payload=True,
    )


def _user_has_historical_paid_bind(user: TelegramUser) -> bool:
    return Payment.objects.filter(
        user_id=user.tg_user_id,
        kind=Payment.Kind.BIND,
        status__in={Payment.Status.PAID, Payment.Status.MANUAL_CONFIRMED},
    ).exists()


def _bind_trial_granted(*, user: TelegramUser, mode: str) -> bool:
    if mode != "bind":
        return False
    if Subscription.objects.filter(user_id=user.tg_user_id).exists():
        return False
    if _user_has_historical_paid_bind(user):
        return False
    return True


def _subscription_keeps_full_access(subscription: Subscription | None) -> bool:
    if subscription is None:
        return False
    return subscription.status in {
        Subscription.Status.TRIAL,
        Subscription.Status.ACTIVE,
        Subscription.Status.PAID,
        Subscription.Status.MANUAL,
        Subscription.Status.LIFETIME,
    }


def _apply_bind_subscription_state(
    payment: Payment,
    *,
    trial_days: int,
    mode: str,
    trial_granted: bool,
) -> Subscription | None:
    now = timezone.now()
    subscription = payment.subscription or latest_subscription_for_user(payment.user)
    if subscription is None:
        if not trial_granted:
            sync_admin_subscription_state(payment.user_id, subscription=None)
            sync_admin_access_scope(
                payment.user_id,
                access_scope=UserAdminState.AccessScope.PAYWALL,
                access_source="billing",
            )
            return None
        subscription = Subscription(user_id=payment.user_id, provider=MONO_PROVIDER)
    bind_context = (payment.raw_payload or {}).get("bind_context") or {}
    promo_code = _normalize_promo_code(bind_context.get("promo_code"))
    promo_offer_id = bind_context.get("promo_offer_id")
    promo_offer_id = int(promo_offer_id) if promo_offer_id else None

    current_profile = BillingProfile.objects.filter(user_id=payment.user_id).first()
    mandate_enabled = bool(current_profile and current_profile.auto_renew_enabled and current_profile.card_token)
    if trial_granted:
        subscription.status = Subscription.Status.TRIAL
        subscription.provider = MONO_PROVIDER
        subscription.source = Subscription.Source.PROMO if promo_offer_id else Subscription.Source.PAYMENT
        subscription.plan = subscription.plan or "solo"
        subscription.amount = _minor_to_decimal(settings.MONO_RENEWAL_AMOUNT)
        subscription.currency = MONO_CURRENCY
        subscription.started_at = now
        subscription.expires_at = now + timedelta(days=trial_days)
        subscription.next_charge_at = subscription.expires_at if mandate_enabled else None
        subscription.grace_expires_at = None
        subscription.trial_days = trial_days
        subscription.payment_id = payment.provider_payment_id or ""
        subscription.auto_renew = mandate_enabled
        subscription.comment = f"Monobank bind {mode}" + (f" promo={promo_code}" if promo_code else "")
        subscription.promo_offer_id = promo_offer_id
        subscription.save()
        _save_subscription_event(
            subscription,
            user_id=payment.user_id,
            event_type="promo_trial_activated" if promo_offer_id else "mono_trial_activated",
            payload={
                "payment_id": payment.provider_payment_id,
                "trial_days": trial_days,
                "mode": mode,
                "promo_code": promo_code,
                "promo_offer_id": promo_offer_id,
            },
        )
        _consume_promo_claim(
            payment=payment,
            subscription=subscription,
            promo_offer_id=promo_offer_id,
            promo_code=promo_code,
        )
    else:
        subscription.provider = MONO_PROVIDER
        subscription.auto_renew = mandate_enabled
        subscription.next_charge_at = subscription.expires_at if mandate_enabled and _subscription_grants_full_access(subscription) else None
        update_fields = ["provider", "auto_renew", "next_charge_at", "updated_at"]
        if promo_offer_id and not subscription.promo_offer_id:
            subscription.promo_offer_id = promo_offer_id
            update_fields.append("promo_offer")
        subscription.save(update_fields=update_fields)
        _save_subscription_event(
            subscription,
            user_id=payment.user_id,
            event_type="mono_card_rebound",
            payload={
                "payment_id": payment.provider_payment_id,
                "mode": mode,
                "trial_granted": False,
            },
        )

    sync_admin_subscription_state(payment.user_id, subscription=subscription)
    sync_admin_access_scope(
        payment.user_id,
        access_scope=(
            UserAdminState.AccessScope.PERSONAL_FULL
            if _subscription_keeps_full_access(subscription)
            else UserAdminState.AccessScope.PAYWALL
        ),
        access_source="promo" if trial_granted and promo_offer_id else "billing",
    )
    payment.subscription = subscription
    payment.save(update_fields=["subscription", "updated_at"])
    return subscription


def _extend_paid_subscription(payment: Payment, *, auto_renew: bool = True) -> Subscription:
    now = timezone.now()
    subscription = payment.subscription or latest_subscription_for_user(payment.user)
    if subscription is None:
        subscription = Subscription(user_id=payment.user_id, provider=MONO_PROVIDER)
        subscription.started_at = now
        subscription.plan = "solo"

    before = _subscription_snapshot(subscription) if subscription.pk else {}
    base_date = max(subscription.expires_at or now, now)
    subscription.status = Subscription.Status.ACTIVE
    subscription.provider = MONO_PROVIDER
    subscription.source = Subscription.Source.PAYMENT
    subscription.amount = _minor_to_decimal(settings.MONO_RENEWAL_AMOUNT)
    subscription.currency = MONO_CURRENCY
    subscription.started_at = subscription.started_at or now
    subscription.expires_at = base_date + timedelta(days=settings.MONO_RENEWAL_PERIOD_DAYS)
    subscription.next_charge_at = subscription.expires_at if auto_renew else None
    subscription.grace_expires_at = None
    subscription.auto_renew = auto_renew
    subscription.payment_id = payment.provider_payment_id or subscription.payment_id
    subscription.save()
    sync_admin_subscription_state(payment.user_id, subscription=subscription)
    sync_admin_access_scope(
        payment.user_id,
        access_scope=UserAdminState.AccessScope.PERSONAL_FULL,
        access_source="billing",
    )
    _save_subscription_event(
        subscription,
        user_id=payment.user_id,
        event_type="mono_renewal_paid",
        payload={"payment_id": payment.provider_payment_id, "kind": payment.kind},
    )
    payment.subscription = subscription
    payment.raw_payload = {**(payment.raw_payload or {}), "entitlement_effect": {
        "before": before, "after": _subscription_snapshot(subscription),
        "granted_at": _serialize_datetime(now), "days": int(settings.MONO_RENEWAL_PERIOD_DAYS),
    }}
    payment.save(update_fields=["subscription", "raw_payload", "updated_at"])
    return subscription


def _set_subscription_grace(subscription: Subscription, *, failure_reason: str = "") -> int:
    now = timezone.now()
    grace_base = max(subscription.expires_at or now, now)
    grace_days = _grace_days_for_failure_reason(failure_reason)
    keeps_access = _subscription_grants_full_access(subscription)
    if not keeps_access:
        subscription.status = Subscription.Status.EXPIRED
    # A failed additional attempt cannot consume previously purchased days.
    # Keep the original expiry, including indefinite manual/lifetime grants.
    subscription.grace_expires_at = grace_base + timedelta(days=grace_days)
    subscription.next_charge_at = None
    subscription.save(update_fields=["status", "grace_expires_at", "next_charge_at", "updated_at"])
    sync_admin_subscription_state(subscription.user_id, subscription=subscription)
    soft_grace_days = max(int(getattr(settings, "MONO_SOFT_GRACE_DAYS", 1) or 0), 0)
    sync_admin_access_scope(
        subscription.user_id,
        access_scope=(
            UserAdminState.AccessScope.PERSONAL_FULL
            if keeps_access or (soft_grace_days > 0 and grace_days == soft_grace_days)
            else UserAdminState.AccessScope.PAYWALL
        ),
        access_source="billing",
    )
    return grace_days


def _payment_mandate_is_current(payment: Payment, *, newer_pending_blocks: bool = True) -> bool:
    """Intent-local revocation survives callbacks and future explicit rebinds.

    Call under the canonical user lock. New checkout rows are new consent;
    provider timestamps are never used to order independent card intents.
    """
    if (payment.raw_payload or {}).get("mandate_revoked_at"):
        return False
    newer = Payment.objects.filter(user_id=payment.user_id, provider=MONO_PROVIDER, pk__gt=payment.pk).filter(
        Q(kind=Payment.Kind.BIND) | Q(raw_payload__recovery_context__save_card=True)
    )
    if not newer_pending_blocks:
        newer = newer.filter(status=Payment.Status.PAID)
    return not newer.exists()


def _handle_bind_success(payment: Payment, payload: dict[str, Any], *, apply_effect: bool = True) -> None:
    bind_context = _bind_context(payment)
    mode = _bind_mode(payment)
    trial_days = _trial_days(bind_context.get("trial_days"))
    # A checkout can predate another successful bind (or an admin grant).
    # The user lock in process_monobank_event serializes this eligibility check.
    trial_granted = _payment_bind_trial_granted(payment) and not Subscription.objects.filter(user_id=payment.user_id).exists()
    modified_at = payment.provider_modified_at or timezone.now()
    if not _payment_mandate_is_current(payment, newer_pending_blocks=False):
        if apply_effect:
            _apply_bind_subscription_state(payment, trial_days=trial_days, mode=mode, trial_granted=trial_granted)
            from subscriptions.trial_recovery import mark_trial_recovery_converted
            mark_trial_recovery_converted(payment.user_id)
        return
    card_token, resolved_payload = _resolve_paid_save_card_payload(payment, payload)
    masked_pan = _extract_masked_pan(resolved_payload)
    profile_status = BillingProfile.Status.ACTIVE if card_token else BillingProfile.Status.MISSING_TOKEN
    failure_reason = "" if card_token else "Monobank did not return cardToken."
    _upsert_billing_profile(
        user=payment.user,
        status=profile_status,
        auto_renew_enabled=bool(card_token),
        card_token=card_token,
        masked_pan=masked_pan,
        last_charge_status=payment.status,
        last_failure_reason=failure_reason,
        last_action_url="",
        last_bound_at=modified_at,
    )
    if not apply_effect:
        return
    subscription = _apply_bind_subscription_state(
        payment,
        trial_days=trial_days,
        mode=mode,
        trial_granted=trial_granted,
    )
    from subscriptions.trial_recovery import mark_trial_recovery_converted

    mark_trial_recovery_converted(payment.user_id)
    if trial_granted and subscription is not None:
        support_note = ""
        if failure_reason:
            support_note = "Картку для автосписань не вдалося зберегти. Напишіть у підтримку, щоб ми допомогли."
        send_bind_activation_notification(payment, support_note=support_note)
        return
    text = "<b>Картку оновлено.</b>" if mode == "rebind" else "<b>Картку прив’язано.</b>"
    if trial_granted and subscription is not None:
        text = f"{text}\n\n{_subscription_notice(subscription)}"
    elif _subscription_keeps_full_access(subscription):
        text += "\n\nНовий пробний період не нараховується. Поточний доступ збережено."
    else:
        text += (
            "\n\nНовий пробний період не нараховується."
            "\nЩоб відновити доступ, поверніться в меню підписки та натисніть «Оплатити зараз»."
        )
    if failure_reason:
        text += "\n\nToken для автосписань не збережено. Напишіть у підтримку."
    _send_user_message(payment.user_id, text)


def _handle_bind_failure(payment: Payment, payload: dict[str, Any]) -> None:
    if not _payment_mandate_is_current(payment):
        return
    reason = str(payload.get("failureReason") or payload.get("errText") or "Не вдалося завершити оплату.")
    _upsert_billing_profile(
        user=payment.user,
        status=BillingProfile.Status.FAILED,
        auto_renew_enabled=False,
        last_charge_status=payment.status,
        last_failure_reason=reason,
    )
    _send_payment_notification_once(
        payment,
        "bind_failed_sent_at",
        "<b>Не вдалося прив’язати картку.</b>\n\nСпробуйте ще раз у меню підписки.",
    )


def _handle_refunded_bind(payment: Payment, payload: dict[str, Any]) -> None:
    refund_request = ((payment.raw_payload or {}).get("refund_request") or {}).copy()
    if refund_request:
        refund_request["status"] = "success"
        refund_request["processed_at"] = _serialize_datetime(payment.provider_modified_at or timezone.now())
        payment.raw_payload = {
            **(payment.raw_payload or {}),
            "refund_request": refund_request,
        }
        payment.save(update_fields=["raw_payload", "updated_at"])

    if str(refund_request.get("purpose") or "") != "duplicate_bind":
        return

    subscription = payment.subscription or latest_subscription_for_user(payment.user)
    canonical_bind = (
        Payment.objects.filter(
            user_id=payment.user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
            status=Payment.Status.PAID,
        )
        .exclude(pk=payment.pk)
        .order_by("paid_at", "created_at", "pk")
        .first()
    )
    if (
        subscription is not None
        and canonical_bind is not None
        and (not subscription.payment_id or subscription.payment_id == payment.provider_payment_id)
    ):
        subscription.payment_id = canonical_bind.provider_payment_id or ""
        subscription.save(update_fields=["payment_id", "updated_at"])

    _save_subscription_event(
        subscription,
        user_id=payment.user_id,
        event_type="mono_duplicate_bind_refunded",
        payload={
            "payment_id": payment.provider_payment_id,
            "canonical_payment_id": getattr(canonical_bind, "provider_payment_id", ""),
            "status": payload.get("status") or "refunded",
        },
    )
    _send_payment_notification_once(
        payment,
        "duplicate_bind_refunded_sent_at",
        (
            "<b>Зайву оплату 1 грн повернуто.</b>\n\n"
            "Пробний доступ і прив’язка картки залишаються активними."
        ),
    )


def _handle_refunded_charge(payment: Payment, payload: dict[str, Any]) -> None:
    modified_at = payment.provider_modified_at or timezone.now()
    refund_request = ((payment.raw_payload or {}).get("refund_request") or {}).copy()
    if refund_request:
        refund_request["status"] = "success"
        refund_request["processed_at"] = _serialize_datetime(modified_at)
        payment.raw_payload = {
            **(payment.raw_payload or {}),
            "refund_request": refund_request,
        }
        payment.save(update_fields=["raw_payload", "updated_at"])
    before_refund = payment.subscription or latest_subscription_for_user(payment.user)
    clear_mandate = bool(before_refund and before_refund.payment_id == payment.provider_payment_id
                         and _payment_mandate_is_current(payment)
                         and before_refund.source == Subscription.Source.PAYMENT)
    subscription = _restore_subscription_from_snapshot(payment)
    _upsert_billing_profile(
        user=payment.user,
        status=BillingProfile.Status.CANCELLED,
        auto_renew_enabled=False,
        clear_card_token=True,
        last_charge_status=payment.status,
        last_failure_reason="",
        last_action_url="",
        last_charge_at=modified_at,
    ) if clear_mandate else None
    _save_subscription_event(
        subscription,
        user_id=payment.user_id,
        event_type="mono_charge_refunded",
        payload={
            "payment_id": payment.provider_payment_id,
            "kind": payment.kind,
            "status": payload.get("status") or "refunded",
        },
    )
    if subscription is not None and subscription.status in {Subscription.Status.TRIAL, Subscription.Status.ACTIVE} and subscription.expires_at:
        text = (
            "<b>Останнє списання повернуто.</b>\n\n"
            f"Доступ залишився активним до {_format_dt(subscription.expires_at)}. "
            "Автопродовження вимкнено."
        )
    else:
        text = (
            "<b>Останнє списання повернуто.</b>\n\n"
            "Автопродовження вимкнено. Якщо доступ уже завершився, картку можна прив’язати знову в меню підписки."
        )
    _send_user_message(payment.user_id, text)


def _handle_renewal_update(payment: Payment, payload: dict[str, Any], *, apply_effect: bool = True) -> None:
    if payment.status == Payment.Status.REFUNDED:
        _handle_refunded_charge(payment, payload)
        return

    action_url = str(payload.get("tdsUrl") or payment.action_url or "")
    modified_at = payment.provider_modified_at or timezone.now()
    current_profile = BillingProfile.objects.filter(user_id=payment.user_id).first()
    current_auto_renew_enabled = bool(getattr(current_profile, "auto_renew_enabled", False))
    mandate_current = _payment_mandate_is_current(payment)
    if payment.status in {Payment.Status.FAILED, Payment.Status.REJECTED} and not mandate_current:
        # A newer bind/recovery intent owns the current mandate. A late failure
        # may update its own Payment row but cannot alter current access or notify.
        return
    resolved_payload = payload
    resolved_card_token = ""
    masked_pan = ""
    failure_reason = str(payload.get("failureReason") or payload.get("errText") or "")
    if mandate_current and payment.status == Payment.Status.PAID and _payment_uses_save_card_data(payment):
        resolved_card_token, resolved_payload = _resolve_paid_save_card_payload(payment, payload)
        masked_pan = _extract_masked_pan(resolved_payload)
    missing_recovery_token = mandate_current and payment.status == Payment.Status.PAID and _payment_is_recovery(payment) and not resolved_card_token
    if missing_recovery_token:
        failure_reason = "Monobank did not return cardToken."
    if action_url and payment.status == Payment.Status.PENDING:
        profile_status = BillingProfile.Status.ACTION_REQUIRED
    elif payment.status in {Payment.Status.FAILED, Payment.Status.REJECTED}:
        profile_status = BillingProfile.Status.FAILED
    elif missing_recovery_token:
        profile_status = BillingProfile.Status.MISSING_TOKEN
    else:
        profile_status = BillingProfile.Status.ACTIVE
    if payment.status == Payment.Status.PAID:
        auto_renew_enabled = not missing_recovery_token
    else:
        auto_renew_enabled = current_auto_renew_enabled

    profile = _upsert_billing_profile(
        user=payment.user,
        status=profile_status,
        auto_renew_enabled=auto_renew_enabled,
        card_token=resolved_card_token,
        clear_card_token=missing_recovery_token,
        masked_pan=masked_pan,
        last_charge_status="requires_action" if action_url and payment.status == Payment.Status.PENDING else payment.status,
        last_failure_reason=failure_reason,
        last_action_url=action_url,
        last_charge_at=modified_at if payment.status == Payment.Status.PAID else None,
    ) if mandate_current else current_profile

    subscription = payment.subscription or latest_subscription_for_user(payment.user)
    if action_url and payment.status == Payment.Status.PENDING:
        _save_subscription_event(
            subscription,
            user_id=payment.user_id,
            event_type="mono_renewal_action_required",
            payload={"payment_id": payment.provider_payment_id, "action_url": action_url, "kind": payment.kind},
        )
        _send_payment_notification_once(
            payment,
            BOT_NOTIFICATION_CHARGE_ACTION_REQUIRED,
            "<b>Потрібно підтвердити оплату.</b>\n\nMonobank попросив додаткове підтвердження для продовження підписки.",
            action_url=action_url,
        )
        _emit_pwa_event(
            payment.user_id,
            "billing_action_required",
            idempotency_key=f"billing-action-required:{payment.pk}",
            context={"payment_id": payment.pk, "action_url": action_url},
        )
        return

    if payment.status == Payment.Status.PENDING:
        _send_payment_notification_once(
            payment,
            key=BOT_NOTIFICATION_CHARGE_PROCESSING,
            text=(
                "<b>Оплата обробляється.</b>\n\n"
                "Monobank ще не повернув фінальний статус по продовженню підписки. "
                "Ми одразу напишемо, щойно отримаємо підтвердження або відмову."
            ),
        )
        return

    if payment.status == Payment.Status.PAID:
        subscription = _extend_paid_subscription(payment, auto_renew=bool(profile and profile.auto_renew_enabled and profile.card_token)) if apply_effect else payment.subscription
        if not apply_effect:
            if mandate_current and subscription is not None and resolved_card_token:
                subscription.auto_renew = True
                subscription.next_charge_at = subscription.expires_at
                subscription.save(update_fields=["auto_renew", "next_charge_at", "updated_at"])
            return
        if not mandate_current:
            _send_billing_expense_prompt_once(payment)
            return
        profile.last_action_url = ""
        if missing_recovery_token:
            subscription.auto_renew = False
            subscription.next_charge_at = None
            subscription.save(update_fields=["auto_renew", "next_charge_at", "updated_at"])
            sync_admin_subscription_state(payment.user_id, subscription=subscription)
            profile.status = BillingProfile.Status.MISSING_TOKEN
            profile.save(update_fields=["status", "last_action_url", "updated_at"])
            _save_subscription_event(
                subscription,
                user_id=payment.user_id,
                event_type="mono_recovery_paid_missing_token",
                payload={"payment_id": payment.provider_payment_id, "kind": payment.kind},
            )
            if _send_billing_expense_prompt_once(payment):
                _send_user_message(
                    payment.user_id,
                    "Нову картку не вдалося зберегти, тому автопродовження вимкнено. Напишіть у підтримку.",
                )
                return
            _send_payment_notification_once(
                payment,
                BOT_NOTIFICATION_CHARGE_PAID,
                (
                    f"<b>{html.escape(_payment_kind_title(payment.kind))} пройшла успішно.</b>\n\n"
                    f"Доступ відновлено до {_format_dt(subscription.expires_at)}.\n"
                    "Нову картку не вдалося зберегти, тому автопродовження вимкнено. Напишіть у підтримку."
                ),
            )
            return
        profile.status = BillingProfile.Status.ACTIVE
        profile.last_failure_reason = ""
        profile.save(update_fields=["status", "last_action_url", "last_failure_reason", "updated_at"])
        if _send_billing_expense_prompt_once(payment):
            return
        _send_payment_notification_once(
            payment,
            BOT_NOTIFICATION_CHARGE_PAID,
            (
                f"<b>{html.escape(_payment_kind_title(payment.kind))} пройшла успішно.</b>\n\n"
                f"Доступ продовжено до {_format_dt(subscription.expires_at)}."
            ),
        )
        return

    if payment.status in {Payment.Status.FAILED, Payment.Status.REJECTED}:
        if subscription is not None:
            grace_days = _set_subscription_grace(subscription, failure_reason=failure_reason)
            _save_subscription_event(
                subscription,
                user_id=payment.user_id,
                event_type="mono_renewal_failed",
                payload={
                    "payment_id": payment.provider_payment_id,
                    "kind": payment.kind,
                    "failure_reason": failure_reason,
                    "grace_days": grace_days,
                },
            )
        else:
            grace_days = _grace_days_for_failure_reason(failure_reason)
        _send_payment_notification_once(
            payment,
            key=BOT_NOTIFICATION_CHARGE_FAILED,
            text=_renewal_failure_message(subscription, grace_days=grace_days),
        )
        _emit_pwa_event(
            payment.user_id,
            "billing_failed",
            idempotency_key=f"billing-failed:{payment.pk}",
            context={"payment_id": payment.pk, "grace_days": grace_days},
        )
        return


@transaction.atomic
def process_monobank_event(payload: dict[str, Any]) -> tuple[Payment | None, bool]:
    invoice_id = str(payload.get("invoiceId") or "")
    payment = _find_payment(invoice_id)
    if payment is None and invoice_id:
        # Signed callbacks can arrive before wallet/payment returns its invoiceId.
        reference = str(payload.get("reference") or (payload.get("merchantPaymInfo") or {}).get("reference") or "")
        if reference:
            candidate = Payment.objects.filter(
                provider=MONO_PROVIDER, status=Payment.Status.PENDING,
                kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
                raw_payload__charge_context__reference=reference,
            ).first()
            if candidate is not None and not candidate.provider_payment_id and _charge_identity_matches(candidate, payload):
                payment = candidate
    if payment is None:
        logger.warning("Monobank webhook for unknown invoiceId=%s", invoice_id)
        return None, False

    # All billing mutations use user -> profile -> subscription -> payment.
    # Re-read after locking; the initial lookup is only for routing the lock.
    user = TelegramUser.objects.select_for_update().get(pk=payment.user_id)
    profile = BillingProfile.objects.select_for_update().filter(user_id=user.pk).first()
    subscription = Subscription.objects.select_for_update().filter(
        pk=payment.subscription_id,
    ).first() if payment.subscription_id else Subscription.objects.select_for_update().filter(
        user_id=user.pk,
    ).order_by("-created_at", "-pk").first()
    payment = Payment.objects.select_for_update().get(pk=payment.pk)
    payment.user = user
    if not payment.provider_payment_id:
        if not _charge_identity_matches(payment, payload):
            return payment, False
        payment.provider_payment_id = invoice_id
    elif payment.provider_payment_id != invoice_id:
        return payment, False
    # Do not lock nullable joins: PostgreSQL rejects FOR UPDATE on their null side.
    payment.subscription = subscription
    modified_at = _parse_timestamp(payload.get("modifiedDate"))
    incoming_status = _payment_status_from_mono(payload)
    incoming_action_url = str(payload.get("tdsUrl") or "")
    previous_status = payment.status
    paid_was_recorded = payment.paid_at is not None
    if previous_status == Payment.Status.REFUNDED and incoming_status != Payment.Status.REFUNDED:
        return payment, False
    if previous_status == Payment.Status.PAID and incoming_status not in {Payment.Status.PAID, Payment.Status.REFUNDED}:
        return payment, False
    if previous_status in {Payment.Status.FAILED, Payment.Status.REJECTED} and incoming_status == Payment.Status.PENDING:
        return payment, False
    reconcile_card = (
        incoming_status == previous_status == Payment.Status.PAID
        and _payment_uses_save_card_data(payment)
        and profile is not None and profile.status == BillingProfile.Status.MISSING_TOKEN
        and subscription is not None and subscription.payment_id == payment.provider_payment_id
    )
    if payment.provider_modified_at and modified_at:
        if payment.provider_modified_at > modified_at:
            return payment, False
        if (
            payment.provider_modified_at == modified_at
            and payment.status == incoming_status
            and payment.action_url == incoming_action_url
            and not reconcile_card
        ):
            return payment, False

    payment.provider = MONO_PROVIDER
    payment.status = incoming_status
    payment.action_url = incoming_action_url
    payment.provider_modified_at = modified_at or payment.provider_modified_at
    payment.raw_payload = {**(payment.raw_payload or {}), "webhook_payload": sanitize_monobank_payload(payload)}
    if payment.raw_payload.get("charge_transport"):
        payment.raw_payload["charge_transport"] = {"state": "received", "received_at": _serialize_datetime(timezone.now())}
    _set_external_transaction_id(payment, payload)
    if payment.status == Payment.Status.PAID and payment.paid_at is None:
        payment.paid_at = modified_at or timezone.now()
    payment.save()

    # Durable per-invoice effects commit atomically with entitlement changes.
    # Legacy final rows predate the marker and must not be granted/reversed again.
    effects = dict((payment.raw_payload or {}).get("applied_effects") or {})
    already_applied = (bool(effects.get(incoming_status)) or previous_status == incoming_status
                       or (incoming_status == Payment.Status.PAID and paid_was_recorded))
    if incoming_status in {Payment.Status.PAID, Payment.Status.REFUNDED}:
        if not effects.get(incoming_status):
            effects[incoming_status] = _serialize_datetime(timezone.now())
            payment.raw_payload = {**payment.raw_payload, "applied_effects": effects}
            payment.save(update_fields=["raw_payload", "updated_at"])
        if already_applied:
            if reconcile_card:
                if payment.kind == Payment.Kind.BIND:
                    _handle_bind_success(payment, payload, apply_effect=False)
                else:
                    _handle_renewal_update(payment, payload, apply_effect=False)
            return payment, True
    elif (
        previous_status in {Payment.Status.FAILED, Payment.Status.REJECTED}
        and incoming_status in {Payment.Status.FAILED, Payment.Status.REJECTED}
    ):
        # Provider failure labels are one terminal effect class. Preserve the
        # newest provider status/payload, but do not repeat grace or messaging.
        return payment, True

    if payment.kind == Payment.Kind.BIND:
        if payment.status == Payment.Status.PAID:
            _handle_bind_success(payment, payload)
        elif payment.status == Payment.Status.REFUNDED:
            _handle_refunded_bind(payment, payload)
        elif payment.status in {Payment.Status.FAILED, Payment.Status.REJECTED}:
            _handle_bind_failure(payment, payload)
        return payment, True

    _handle_renewal_update(payment, payload)
    return payment, True


@transaction.atomic
def _reserve_monobank_refund(*, user_id: int, reason: str, admin_user=None) -> Payment:
    TelegramUser.objects.select_for_update().get(pk=user_id)
    profile = BillingProfile.objects.select_for_update().filter(user_id=user_id).first()
    payment = get_latest_refundable_monobank_payment(user_id=user_id)
    if payment is None or not payment.provider_payment_id:
        raise ValueError("No refundable Monobank invoice found.")
    payment = Payment.objects.select_for_update().get(pk=payment.pk)
    preserve = payment.kind == Payment.Kind.BIND
    token_delete = {"remote_delete_attempted":False, "remote_delete_ok":False, "remote_delete_error":"", "remote_delete_response":{}}
    if profile is not None and not preserve:
        token_delete = _disable_auto_renew_for_profile(user=payment.user, profile=profile,
            remote_reason="mono_refund_autorenew_disabled", delete_remote_token=True)
        payment.refresh_from_db()
    payment.raw_payload = {**payment.raw_payload, "refund_request": {
        "requested_at": _serialize_datetime(timezone.now()),
        "requested_by": getattr(admin_user, "username", ""), "reason": reason,
        "purpose": "duplicate_bind" if preserve else "subscription_charge",
        "preserve_billing_state": preserve, "status":"unknown",
        "reference": f"refund:payment:{payment.pk}", "token_delete": token_delete,
    }}
    payment.save(update_fields=["raw_payload", "updated_at"])
    return payment


def refund_latest_monobank_payment(*, user_id: int, reason: str = "", admin_user=None) -> dict[str, Any]:
    payment = _reserve_monobank_refund(user_id=user_id, reason=reason, admin_user=admin_user)
    try:
        response = dict(cancel_invoice({"invoiceId": payment.provider_payment_id,
            "amount": _decimal_to_minor(payment.amount), "extRef":payment.raw_payload["refund_request"]["reference"]}))
    except Exception as exc:
        # Intent remains unknown; a new cancel request would not be safe.
        raise MonobankAPIError(f"Refund outcome unknown (payment#{payment.pk}); reconciliation required.") from exc
    with transaction.atomic():
        TelegramUser.objects.select_for_update().get(pk=user_id)
        payment = Payment.objects.select_for_update().get(pk=payment.pk)
        info = dict(payment.raw_payload["refund_request"])
        if payment.status != Payment.Status.REFUNDED:
            info["status"] = str(response.get("status") or "processing").lower()
        info["response"] = sanitize_monobank_payload(response)
        payment.raw_payload = {**payment.raw_payload, "refund_request":info}
        payment.save(update_fields=["raw_payload", "updated_at"])
    token = info["token_delete"]
    return {"payment_id":payment.pk, "invoice_id":payment.provider_payment_id, "refund_status":info["status"],
        "refund_kind":info["purpose"], "preserved_billing_state":info["preserve_billing_state"],
        "token_delete_attempted":token["remote_delete_attempted"], "token_delete_ok":token["remote_delete_ok"],
        "token_delete_error":token["remote_delete_error"]}


def reconcile_monobank_refunds(*, limit: int = 50) -> dict[str, int]:
    stats = {"checked":0, "updated":0, "pending":0, "stuck":0}
    now = timezone.now()
    candidates = Payment.objects.filter(provider=MONO_PROVIDER, status=Payment.Status.PAID,
        raw_payload__refund_request__status__in=["processing", "success", "unknown"]).order_by("updated_at", "pk")
    for candidate in candidates.iterator():
        if stats["checked"] >= max(1, int(limit)):
            break
        with transaction.atomic():
            TelegramUser.objects.select_for_update().get(pk=candidate.user_id)
            payment = Payment.objects.select_for_update().get(pk=candidate.pk)
            info = dict((payment.raw_payload or {}).get("refund_request") or {})
            next_check = _parse_timestamp(info.get("next_check_at"))
            if payment.status != Payment.Status.PAID or (next_check and next_check > now):
                continue
            attempt = int(info.get("checks") or 0) + 1
            info.update(checks=attempt, last_check_at=_serialize_datetime(now),
                next_check_at=_serialize_datetime(now + timedelta(seconds=min(3600, 60 * 2 ** min(attempt, 6)))))
            requested = _parse_timestamp(info.get("requested_at")) or payment.created_at
            if now - requested >= timedelta(days=1):
                info["requires_review"] = True
                stats["stuck"] += 1
                logger.warning("Stuck Monobank refund: payment_id=%s", payment.pk)
            payment.raw_payload = {**payment.raw_payload, "refund_request":info}
            payment.save(update_fields=["raw_payload", "updated_at"])
        stats["checked"] += 1
        try:
            payload = dict(fetch_invoice_status(payment.provider_payment_id))
            if str(payload.get("invoiceId") or payment.provider_payment_id) != payment.provider_payment_id:
                raise ValueError("Refund invoice mismatch")
            payload["invoiceId"] = payment.provider_payment_id
            # A charge success is NOT proof the refund completed.
            if _payment_status_from_mono(payload) == Payment.Status.REFUNDED:
                _, updated = process_monobank_event(payload)
                stats["updated"] += int(updated)
            else:
                stats["pending"] += 1
        except Exception as exc:
            logger.warning("Refund reconciliation unavailable payment_id=%s error=%s", payment.pk, type(exc).__name__)
            stats["pending"] += 1
    return stats

@transaction.atomic
def build_bind_invoice(*, user_id: int, trial_days: int, mode: str = "bind", promo_code: str = "") -> dict[str, Any]:
    user = TelegramUser.objects.select_for_update().filter(tg_user_id=user_id).first()
    if user is None:
        raise ValueError("User not found.")

    profile, _ = BillingProfile.objects.get_or_create(
        user_id=user.tg_user_id,
        defaults={"provider": MONO_PROVIDER, "wallet_id": _wallet_id(user.tg_user_id)},
    )
    if not profile.wallet_id:
        profile.wallet_id = _wallet_id(user.tg_user_id)
        profile.save(update_fields=["wallet_id", "updated_at"])

    pending_checkout = _pending_bind_checkout_for_reuse(user_id=user.tg_user_id)
    if pending_checkout is not None and (Payment.objects.get(pk=pending_checkout["payment_id"]).raw_payload or {}).get("mandate_revoked_at"):
        pending_checkout = None
    if pending_checkout is not None:
        profile.status = BillingProfile.Status.PENDING
        profile.last_action_url = str(pending_checkout["page_url"])
        profile.last_failure_reason = ""
        profile.save(update_fields=["status", "last_action_url", "last_failure_reason", "updated_at"])
        return pending_checkout

    trial_granted = _bind_trial_granted(user=user, mode=mode)
    effective_mode = mode if trial_granted else "rebind"
    promo_offer = _resolve_promo_offer(user=user, promo_code=promo_code, mode=effective_mode)
    promo_code = promo_offer.code if promo_offer is not None else ""
    effective_trial_days = (
        _trial_days(promo_offer.trial_days if promo_offer is not None else trial_days)
        if trial_granted
        else 0
    )
    return_token = build_bind_return_token(user_id=user.tg_user_id, flow="bind", lang=user.lang)
    response = create_invoice(
        {
            "amount": settings.MONO_BIND_AMOUNT,
            "ccy": MONO_CURRENCY_CODE,
            "validity": _bind_invoice_validity_seconds(),
            "redirectUrl": _append_query_params(settings.MONO_BILLING_RETURN_URL, {"return_token": return_token}),
            "webHookUrl": settings.MONO_BILLING_WEBHOOK_URL,
            "merchantPaymInfo": {
                "reference": f"bind:{user_id}:{effective_trial_days}:{effective_mode}",
                "destination": "My Cash Flow Bot card binding",
            },
            "saveCardData": {
                "saveCard": True,
                "walletId": profile.wallet_id,
            },
        }
    )
    invoice_id = str(response.get("invoiceId") or "")
    page_url = str(response.get("pageUrl") or "")
    if not invoice_id or not page_url:
        raise MonobankAPIError("Monobank did not return invoiceId/pageUrl.")

    payment = Payment.objects.create(
        user_id=user.tg_user_id,
        provider=MONO_PROVIDER,
        provider_payment_id=invoice_id,
        amount=_minor_to_decimal(settings.MONO_BIND_AMOUNT),
        currency=MONO_CURRENCY,
        status=Payment.Status.PENDING,
        kind=Payment.Kind.BIND,
        raw_payload={
            "invoice_create_response": response,
            "bind_context": {
                "trial_days": effective_trial_days,
                "mode": effective_mode,
                "trial_granted": trial_granted,
                "wallet_id": profile.wallet_id,
                "promo_code": promo_code,
                "promo_offer_id": promo_offer.pk if promo_offer is not None else None,
                "return_token": return_token,
                "page_url": page_url,
            },
        },
    )
    profile.status = BillingProfile.Status.PENDING
    profile.last_action_url = page_url
    profile.last_failure_reason = ""
    profile.save(update_fields=["status", "last_action_url", "last_failure_reason", "updated_at"])
    if promo_offer is not None and trial_granted:
        PromoOfferClaim.objects.get_or_create(
            offer=promo_offer,
            user=user,
            defaults={"status": PromoOfferClaim.Status.PENDING},
        )
        sync_admin_access_scope(
            user.tg_user_id,
            access_scope=UserAdminState.AccessScope.PAYWALL,
            access_source="promo",
            pending_start_payload=f"promo_{promo_offer.code}",
        )
    return {
        "payment_id": payment.pk,
        "invoice_id": invoice_id,
        "page_url": page_url,
        "trial_days": effective_trial_days,
        "trial_granted": trial_granted,
        "mode": effective_mode,
        "promo_code": promo_code,
        "reused": False,
    }


@transaction.atomic
def build_recovery_invoice(*, user_id: int) -> dict[str, Any]:
    user = TelegramUser.objects.select_for_update().filter(tg_user_id=user_id).first()
    if user is None:
        raise ValueError("User not found.")

    profile, _ = BillingProfile.objects.get_or_create(
        user_id=user.tg_user_id,
        defaults={"provider": MONO_PROVIDER, "wallet_id": _wallet_id(user.tg_user_id)},
    )
    if not profile.wallet_id:
        profile.wallet_id = _wallet_id(user.tg_user_id)
        profile.save(update_fields=["wallet_id", "updated_at"])

    pending_recovery_payment = _latest_pending_recovery_payment_for_user(user.tg_user_id)
    if pending_recovery_payment is not None:
        recovery_context = _recovery_context(pending_recovery_payment)
        page_url = str(recovery_context.get("page_url") or profile.last_action_url or "").strip()
        if page_url:
            return {
                "payment_id": pending_recovery_payment.pk,
                "invoice_id": pending_recovery_payment.provider_payment_id,
                "page_url": page_url,
            }

    pending_payment = _latest_pending_charge_payment_for_user(user.tg_user_id)
    if pending_payment is not None:
        invoice_hint = pending_payment.provider_payment_id or f"payment#{pending_payment.pk}"
        raise ValueError(f"Another Monobank charge is already pending ({invoice_hint}).")

    subscription = Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()
    return_token = build_bind_return_token(user_id=user.tg_user_id, flow="recovery", lang=user.lang)
    response = create_invoice(
        {
            "amount": settings.MONO_RENEWAL_AMOUNT,
            "ccy": MONO_CURRENCY_CODE,
            "redirectUrl": _append_query_params(settings.MONO_BILLING_RETURN_URL, {"return_token": return_token}),
            "webHookUrl": settings.MONO_BILLING_WEBHOOK_URL,
            "merchantPaymInfo": {
                "reference": f"recovery:{user_id}:{int(timezone.now().timestamp())}",
                "destination": "My Cash Flow Bot recovery payment",
            },
            "saveCardData": {
                "saveCard": True,
                "walletId": profile.wallet_id,
            },
        }
    )
    invoice_id = str(response.get("invoiceId") or "")
    page_url = str(response.get("pageUrl") or "")
    if not invoice_id or not page_url:
        raise MonobankAPIError("Monobank did not return invoiceId/pageUrl.")

    payment = Payment.objects.create(
        user_id=user.tg_user_id,
        provider=MONO_PROVIDER,
        provider_payment_id=invoice_id,
        amount=_minor_to_decimal(settings.MONO_RENEWAL_AMOUNT),
        currency=MONO_CURRENCY,
        status=Payment.Status.PENDING,
        kind=Payment.Kind.RETRY,
        raw_payload={
            "invoice_create_response": sanitize_monobank_payload(response),
            "charge_context": _charge_context_payload(
                subscription=subscription,
                wallet_id=profile.wallet_id,
                payment_kind=Payment.Kind.RETRY,
            ),
            "recovery_context": {
                "save_card": True,
                "replace_card": True,
                "wallet_id": profile.wallet_id,
                "return_token": return_token,
                "page_url": page_url,
            },
        },
    )
    _upsert_billing_profile(
        user=user,
        status=BillingProfile.Status.PENDING,
        auto_renew_enabled=bool(profile.card_token) and bool(profile.auto_renew_enabled),
        last_charge_status=Payment.Status.PENDING,
        last_failure_reason="",
        last_action_url=page_url,
    )
    return {
        "payment_id": payment.pk,
        "invoice_id": invoice_id,
        "page_url": page_url,
    }


def sync_pending_bind_status(*, user_id: int) -> dict[str, Any]:
    user = TelegramUser.objects.filter(tg_user_id=user_id).first()
    if user is None:
        raise ValueError("User not found.")

    payments = list(
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
            status=Payment.Status.PENDING,
        )
        .order_by("-created_at", "-pk")
    [:5]
    )
    payment = payments[0] if payments else None
    if payment is None:
        return {
            "payment_id": None,
            "invoice_id": "",
            "monobank_status": "",
            "updated": False,
        }

    latest_pending_result: dict[str, Any] | None = None
    for pending_payment in payments:
        payload = dict(fetch_invoice_status(pending_payment.provider_payment_id))
        payload.setdefault("invoiceId", pending_payment.provider_payment_id)
        monobank_status = str(payload.get("status") or "").strip().lower()
        if monobank_status in {"", "created", "processing", "hold"}:
            if latest_pending_result is None:
                latest_pending_result = {
                    "payment_id": pending_payment.pk,
                    "invoice_id": pending_payment.provider_payment_id,
                    "monobank_status": monobank_status,
                    "updated": False,
                }
            continue

        processed_payment, updated = process_monobank_event(payload)
        return {
            "payment_id": getattr(processed_payment, "pk", pending_payment.pk),
            "invoice_id": pending_payment.provider_payment_id,
            "monobank_status": monobank_status,
            "updated": updated,
        }

    return latest_pending_result or {
        "payment_id": payment.pk,
        "invoice_id": payment.provider_payment_id,
        "monobank_status": "",
        "updated": False,
    }


def sync_pending_charge_status(*, user_id: int) -> dict[str, Any]:
    user = TelegramUser.objects.filter(tg_user_id=user_id).first()
    if user is None:
        raise ValueError("User not found.")

    unresolved = Payment.objects.filter(
        Q(provider_payment_id__isnull=True) | Q(provider_payment_id=""),
        user_id=user_id, provider=MONO_PROVIDER, status=Payment.Status.PENDING,
        kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
    ).first()

    payments = list(
        Payment.objects.select_related("user", "subscription", "plan")
        .filter(
            user_id=user_id,
            provider=MONO_PROVIDER,
            kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
            status=Payment.Status.PENDING,
        )
        .exclude(provider_payment_id__isnull=True)
        .exclude(provider_payment_id="")
        .order_by("-created_at", "-pk")[:5]
    )
    payment = payments[0] if payments else None
    if payment is None:
        if unresolved is not None:
            return {**_charge_result(unresolved), "monobank_status": "unknown", "updated": False,
                    "requires_reconciliation": True}
        return {
            "payment_id": None,
            "invoice_id": "",
            "monobank_status": "",
            "updated": False,
        }

    latest_pending_result: dict[str, Any] | None = None
    for pending_payment in payments:
        payload = dict(fetch_invoice_status(pending_payment.provider_payment_id))
        payload.setdefault("invoiceId", pending_payment.provider_payment_id)
        monobank_status = str(payload.get("status") or "").strip().lower()
        if monobank_status in {"", "created", "processing", "hold"}:
            if latest_pending_result is None:
                latest_pending_result = {
                    "payment_id": pending_payment.pk,
                    "invoice_id": pending_payment.provider_payment_id,
                    "monobank_status": monobank_status,
                    "updated": False,
                }
            continue

        processed_payment, updated = process_monobank_event(payload)
        return {
            "payment_id": getattr(processed_payment, "pk", pending_payment.pk),
            "invoice_id": pending_payment.provider_payment_id,
            "monobank_status": monobank_status,
            "updated": updated,
        }

    return latest_pending_result or {
        "payment_id": payment.pk,
        "invoice_id": payment.provider_payment_id,
        "monobank_status": "",
        "updated": False,
    }


def reconcile_pending_monobank_charges(*, limit: int = 50) -> dict[str, int]:
    unresolved = Payment.objects.filter(
        Q(provider_payment_id__isnull=True) | Q(provider_payment_id=""),
        provider=MONO_PROVIDER, status=Payment.Status.PENDING,
        kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
    )
    requires_reconciliation = unresolved.count()
    if requires_reconciliation:
        logger.warning("Monobank invoice-less pending charges require operator reconciliation: count=%s payment_ids=%s",
                       requires_reconciliation, list(unresolved.values_list("pk", flat=True)[:max(int(limit or 0), 1)]))
    pending_user_ids: list[int] = []
    seen_users: set[int] = set()
    pending_payments = (
        Payment.objects.filter(
            provider=MONO_PROVIDER,
            kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY},
            status=Payment.Status.PENDING,
        )
        .exclude(provider_payment_id__isnull=True)
        .exclude(provider_payment_id="")
        .order_by("created_at", "pk")
        .values_list("user_id", flat=True)
    )
    for user_id in pending_payments:
        normalized_user_id = int(user_id)
        if normalized_user_id in seen_users:
            continue
        seen_users.add(normalized_user_id)
        pending_user_ids.append(normalized_user_id)
        if len(pending_user_ids) >= max(int(limit or 0), 1):
            break

    checked = 0
    updated = 0
    still_pending = 0
    failed = 0
    for user_id in pending_user_ids:
        try:
            result = sync_pending_charge_status(user_id=user_id)
        except (ValueError, MonobankAPIError) as exc:
            failed += 1
            logger.warning("Pending Monobank charge sync failed for %s: %s", user_id, exc)
            continue

        checked += 1
        if result.get("updated"):
            updated += 1
        elif result.get("payment_id"):
            still_pending += 1

    return {
        "checked": checked,
        "updated": updated,
        "still_pending": still_pending,
        "failed": failed,
        **({"requires_reconciliation": requires_reconciliation} if requires_reconciliation else {}),
    }


def reconcile_pending_monobank_binds(*, limit: int = 50) -> dict[str, int]:
    pending_user_ids: list[int] = []
    seen_users: set[int] = set()
    pending_payments = (
        Payment.objects.filter(
            provider=MONO_PROVIDER,
            kind=Payment.Kind.BIND,
            status=Payment.Status.PENDING,
        )
        .exclude(provider_payment_id__isnull=True)
        .exclude(provider_payment_id="")
        .order_by("created_at", "pk")
        .values_list("user_id", flat=True)
    )
    for user_id in pending_payments:
        normalized_user_id = int(user_id)
        if normalized_user_id in seen_users:
            continue
        seen_users.add(normalized_user_id)
        pending_user_ids.append(normalized_user_id)
        if len(pending_user_ids) >= max(int(limit or 0), 1):
            break

    checked = 0
    updated = 0
    still_pending = 0
    failed = 0
    for user_id in pending_user_ids:
        try:
            result = sync_pending_bind_status(user_id=user_id)
        except (ValueError, MonobankAPIError) as exc:
            failed += 1
            logger.warning("Pending Monobank bind sync failed for %s: %s", user_id, exc)
            continue

        checked += 1
        if result.get("updated"):
            updated += 1
        elif result.get("payment_id"):
            still_pending += 1

    return {
        "checked": checked,
        "updated": updated,
        "still_pending": still_pending,
        "failed": failed,
    }


def _disable_auto_renew_for_profile(
    *,
    user: TelegramUser,
    profile: BillingProfile,
    remote_reason: str,
    delete_remote_token: bool,
) -> dict[str, Any]:
    remote_delete_error = ""
    remote_delete_response: dict[str, Any] = {}
    token_was_present = bool(profile.card_token)
    # Revoke every existing intent, including paid rows awaiting a late token.
    # Caller holds user -> profile locks, so new explicit checkouts serialize after it.
    revoked_at = _serialize_datetime(timezone.now())
    for intent in Payment.objects.select_for_update().filter(user_id=user.pk, provider=MONO_PROVIDER):
        intent.raw_payload = {**(intent.raw_payload or {}), "mandate_revoked_at": revoked_at}
        intent.save(update_fields=["raw_payload", "updated_at"])

    if delete_remote_token and profile.card_token:
        try:
            remote_delete_response = delete_wallet_card(profile.card_token)
        except MonobankAPIError as exc:
            remote_delete_error = str(exc)

    profile.auto_renew_enabled = False
    profile.card_token = ""
    profile.status = BillingProfile.Status.CANCELLED
    profile.last_action_url = ""
    profile.last_failure_reason = remote_delete_error
    profile.save()

    subscription = latest_subscription_for_user(user)
    if subscription is not None:
        subscription.auto_renew = False
        subscription.next_charge_at = None
        subscription.save(update_fields=["auto_renew", "next_charge_at", "updated_at"])
        _save_subscription_event(
            subscription,
            user_id=user.tg_user_id,
            event_type=remote_reason,
            payload={"manual": True},
        )
        sync_admin_subscription_state(user.tg_user_id, subscription=subscription)
    sync_admin_access_scope(
        user.tg_user_id,
        access_scope=(
            UserAdminState.AccessScope.PERSONAL_FULL
            if _subscription_grants_full_access(subscription)
            else UserAdminState.AccessScope.PAYWALL
        ),
        access_source="billing",
    )
    return {
        "remote_delete_attempted": bool(delete_remote_token and token_was_present),
        "remote_delete_ok": bool(delete_remote_token and token_was_present and not remote_delete_error),
        "remote_delete_error": remote_delete_error,
        "remote_delete_response": sanitize_monobank_payload(remote_delete_response),
    }


class BillingChargeSkipped(ValueError):
    """No provider request is needed; this is not a declined payment."""


class BillingChargeReplay(BillingChargeSkipped):
    def __init__(self, payment: Payment):
        self.payment = payment
        super().__init__("This manual intent already has an attempt.")


class BillingChargeUnknown(MonobankAPIError):
    """The provider may have accepted a debit. Never retry without reconciliation."""


def _charge_identity_matches(payment: Payment, payload: dict[str, Any]) -> bool:
    reference = str(payload.get("reference") or (payload.get("merchantPaymInfo") or {}).get("reference") or "")
    expected = str(((payment.raw_payload or {}).get("charge_context") or {}).get("reference") or "")
    return bool(expected) and reference == expected and payload.get("amount") == _decimal_to_minor(payment.amount) and payload.get("ccy") == MONO_CURRENCY_CODE


def _charge_result(payment: Payment) -> dict[str, Any]:
    return {
        "payment_id": payment.pk, "invoice_id": payment.provider_payment_id,
        "status": payment.status, "action_url": payment.action_url,
    }


@transaction.atomic
def _record_unknown_charge(payment: Payment, *, error_type: str) -> Payment:
    TelegramUser.objects.select_for_update().get(pk=payment.user_id)
    profile = BillingProfile.objects.select_for_update().filter(user_id=payment.user_id).first()
    payment = Payment.objects.select_for_update().get(pk=payment.pk)
    if payment.provider_payment_id or payment.status != Payment.Status.PENDING:
        return payment  # An early signed callback already supplied the outcome.
    payment.raw_payload = {**(payment.raw_payload or {}), "charge_transport": {
        "state": "unknown", "error_type": error_type,
        "recorded_at": _serialize_datetime(timezone.now()), "requires_reconciliation": True,
    }}
    payment.save(update_fields=["raw_payload", "updated_at"])
    if profile is not None:
        profile.last_charge_status = "unknown"
        profile.last_failure_reason = "Charge outcome unknown; operator reconciliation required."
        profile.save(update_fields=["last_charge_status", "last_failure_reason", "updated_at"])
    return payment


@transaction.atomic
def resolve_unknown_monobank_charge(
    *, payment_id: int, admin_user, reason: str, invoice_id: str = "", confirmed_no_charge: bool = False,
) -> dict[str, Any]:
    """Operator-only repair, never a debit. Caller must preview/confirm evidence.

    Attach only a provider-verified matching invoice, or explicitly attest that
    provider evidence proves no charge occurred. No-charge enables MANUAL retry
    only. Keep ambiguous attempts pending. No public/admin route is exposed here.
    """
    if not (getattr(admin_user, "is_authenticated", False) and getattr(admin_user, "is_staff", False)
            and admin_user.has_perm("subscriptions.change_payment")):
        raise PermissionError("Payment reconciliation permission is required.")
    if not str(reason or "").strip() or bool(invoice_id) == bool(confirmed_no_charge):
        raise ValueError("Supply evidence and exactly one of invoiceId or confirmed no charge.")
    candidate = Payment.objects.get(pk=payment_id, provider=MONO_PROVIDER, kind__in={Payment.Kind.RENEWAL, Payment.Kind.RETRY})
    TelegramUser.objects.select_for_update().get(pk=candidate.user_id)
    BillingProfile.objects.select_for_update().filter(user_id=candidate.user_id).first()
    subscription = Subscription.objects.select_for_update().filter(pk=candidate.subscription_id).first()
    payment = Payment.objects.select_for_update().get(pk=payment_id)
    if payment.status != Payment.Status.PENDING or payment.provider_payment_id:
        raise ValueError("Only unresolved invoice-less pending charges can be reconciled here.")
    payload = None
    if invoice_id:
        payload = dict(fetch_invoice_status(invoice_id))
        if str(payload.get("invoiceId") or "") != invoice_id or not _charge_identity_matches(payment, payload):
            raise ValueError("Provider invoice does not match the persisted charge reference/amount/currency.")
    payment.raw_payload = {**payment.raw_payload, "operator_reconciliation": {
        "admin_user_id": admin_user.pk, "reason": str(reason).strip(),
        "recorded_at": _serialize_datetime(timezone.now()), "invoice_id": invoice_id,
        "confirmed_no_charge": bool(confirmed_no_charge),
    }}
    if confirmed_no_charge:
        payment.status = Payment.Status.REJECTED
        payment.raw_payload["charge_transport"] = {"state": "confirmed_no_charge"}
        if subscription is not None:
            subscription.next_charge_at = None
            subscription.save(update_fields=["next_charge_at", "updated_at"])
        payment.save(update_fields=["status", "raw_payload", "updated_at"])
    else:
        payment.save(update_fields=["raw_payload", "updated_at"])
        payment, _ = process_monobank_event(payload)
    return _charge_result(payment)


def _build_charge_payload(*, profile: BillingProfile, user_id: int, payment_kind: str, reference: str) -> dict[str, Any]:
    return {
        "cardToken": profile.card_token,
        "amount": settings.MONO_RENEWAL_AMOUNT,
        "ccy": MONO_CURRENCY_CODE,
        "initiationKind": "merchant",
        "redirectUrl": settings.MONO_BILLING_RETURN_URL,
        "webHookUrl": settings.MONO_BILLING_WEBHOOK_URL,
        "merchantPaymInfo": {
            "reference": reference,
            "destination": "My Cash Flow Bot monthly renewal",
        },
    }


@transaction.atomic
def _create_charge_attempt(
    *, user_id: int, payment_kind: str,
    expected_subscription_id: int | None = None, expected_next_charge_at: datetime | None = None,
    intent_key: str = "",
) -> tuple[BillingProfile, Payment]:
    import re
    if intent_key and (not isinstance(intent_key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", intent_key)):
        raise ValueError("Invalid billing intent key.")
    TelegramUser.objects.select_for_update().get(pk=user_id)
    if intent_key:
        existing = Payment.objects.filter(user_id=user_id, provider=MONO_PROVIDER,
            kind=payment_kind, raw_payload__charge_context__intent_key=intent_key).first()
        if existing is not None:
            raise BillingChargeReplay(existing)
    profile = BillingProfile.objects.select_for_update().filter(user_id=user_id).first()
    if profile is None or not profile.card_token:
        raise BillingChargeSkipped("No active card token.")
    if payment_kind == Payment.Kind.RENEWAL and not profile.auto_renew_enabled:
        raise BillingChargeSkipped("Auto-renew is disabled.")
    pending_payment = _latest_pending_charge_payment_for_user(user_id)
    if pending_payment is not None:
        invoice_hint = pending_payment.provider_payment_id or f"payment#{pending_payment.pk}"
        raise BillingChargeSkipped(f"Another Monobank charge is already pending ({invoice_hint}).")
    subscription = Subscription.objects.select_for_update().filter(user_id=user_id).order_by("-created_at", "-pk").first()
    if payment_kind == Payment.Kind.RETRY and _subscription_grants_full_access(subscription):
        raise BillingChargeSkipped("Existing access has not expired; an early retry is not allowed.")
    if payment_kind == Payment.Kind.RENEWAL:
        if (
            subscription is None or subscription.provider != MONO_PROVIDER or not subscription.auto_renew
            or subscription.next_charge_at is None or subscription.next_charge_at > timezone.now()
            or (expected_subscription_id is not None and subscription.pk != expected_subscription_id)
            or (expected_next_charge_at is not None and subscription.next_charge_at != expected_next_charge_at)
        ):
            raise BillingChargeSkipped("Subscription billing period is no longer due.")
        if Payment.objects.filter(
            subscription=subscription, provider=MONO_PROVIDER, kind=Payment.Kind.RENEWAL,
            raw_payload__charge_context__period_end=_serialize_datetime(subscription.next_charge_at),
        ).exists():
            raise BillingChargeSkipped("This renewal period already has a charge attempt.")

    payment = Payment.objects.create(
        user_id=user_id,
        subscription=subscription,
        provider=MONO_PROVIDER,
        amount=_minor_to_decimal(settings.MONO_RENEWAL_AMOUNT),
        currency=MONO_CURRENCY,
        status=Payment.Status.PENDING,
        kind=payment_kind,
        raw_payload={
            "charge_context": _charge_context_payload(
                subscription=subscription,
                wallet_id=profile.wallet_id,
                payment_kind=payment_kind,
            )
        },
    )
    payment.raw_payload["charge_context"].update({
        "intent_key": intent_key,
        "subscription_id": getattr(subscription, "pk", None),
        "period_end": _serialize_datetime(subscription.next_charge_at or subscription.expires_at) if subscription else "",
        "reference": f"{payment_kind}:{user_id}:payment:{payment.pk}",
    })
    # Persist before dispatch: a killed worker is ambiguous, not safe to retry.
    payment.raw_payload["charge_transport"] = {
        "state": "unknown", "requires_reconciliation": True,
        "dispatch_started_at": _serialize_datetime(timezone.now()),
    }
    payment.save(update_fields=["raw_payload", "updated_at"])
    return profile, payment


@transaction.atomic
def _dispatch_charge_attempt(payment: Payment) -> dict[str, Any] | Exception | None:
    # Serialize the final consent check and provider dispatch with cancellation.
    # The unknown intent was committed BEFORE entering this transaction.
    TelegramUser.objects.select_for_update().get(pk=payment.user_id)
    profile = BillingProfile.objects.select_for_update().filter(user_id=payment.user_id).first()
    payment = Payment.objects.select_for_update().get(pk=payment.pk)
    if payment.status != Payment.Status.PENDING or payment.provider_payment_id:
        return None
    if not profile or not profile.card_token or not _payment_mandate_is_current(payment) or (
        payment.kind == Payment.Kind.RENEWAL and not profile.auto_renew_enabled
    ):
        payment.status = Payment.Status.REJECTED
        payment.raw_payload = {**payment.raw_payload, "charge_transport": {"state":"not_dispatched", "reason":"mandate_changed"}}
        payment.save(update_fields=["status", "raw_payload", "updated_at"])
        return None
    try:
        return dict(charge_wallet_payment(_build_charge_payload(
            profile=profile, user_id=payment.user_id, payment_kind=payment.kind,
            reference=payment.raw_payload["charge_context"]["reference"],
        )))
    except Exception as exc:
        # Release dispatch serialization without rolling back an early callback.
        return exc


def _run_single_charge(
    *, user_id: int, payment_kind: str,
    expected_subscription_id: int | None = None, expected_next_charge_at: datetime | None = None,
    intent_key: str = "",
) -> dict[str, Any]:
    try:
        profile, payment = _create_charge_attempt(
            user_id=user_id, payment_kind=payment_kind,
            expected_subscription_id=expected_subscription_id, expected_next_charge_at=expected_next_charge_at,
            intent_key=intent_key,
        )
    except BillingChargeReplay as replay:
        return {**_charge_result(replay.payment), "reused": True,
                "requires_reconciliation": replay.payment.status == Payment.Status.PENDING and not replay.payment.provider_payment_id}
    try:
        response = _dispatch_charge_attempt(payment)
        if isinstance(response, Exception):
            raise response
        if response is None:
            payment.refresh_from_db()
            return _charge_result(payment)
        invoice_id = str(response.get("invoiceId") or "")
        if not invoice_id:
            raise BillingChargeUnknown("Monobank response has no invoiceId.")
    except Exception as exc:
        payment = _record_unknown_charge(payment, error_type=type(exc).__name__)
        if payment.provider_payment_id or payment.status != Payment.Status.PENDING:
            return _charge_result(payment)
        raise BillingChargeUnknown(f"Charge outcome unknown (payment#{payment.pk}); explicit reconciliation required.") from exc
    with transaction.atomic():
        TelegramUser.objects.select_for_update().get(pk=user_id)
        payment = Payment.objects.select_for_update().get(pk=payment.pk)
        if payment.provider_payment_id and payment.provider_payment_id != invoice_id:
            raise BillingChargeUnknown("Provider returned a conflicting invoiceId; reconciliation required.")
        # Preserve any effects/notification markers written by an early callback.
        payment.provider_payment_id = invoice_id
        payment.raw_payload = {**(payment.raw_payload or {}), "charge_response": sanitize_monobank_payload(response)}
        payment.save(update_fields=["provider_payment_id", "raw_payload", "updated_at"])
    # Keep invoice correlation durable even if local entitlement handling rolls back.
    processed_payment, _ = process_monobank_event({**response, "invoiceId": invoice_id})
    return _charge_result(processed_payment or payment)


def retry_monobank_charge(*, user_id: int, intent_key: str = "") -> dict[str, Any]:
    normalized_intent = str(intent_key or "").strip()
    if not normalized_intent:
        raise ValueError("Billing retry intent key is required.")
    return _run_single_charge(user_id=user_id, payment_kind=Payment.Kind.RETRY, intent_key=normalized_intent)


@transaction.atomic
def cancel_auto_renew(*, user_id: int) -> BillingProfile:
    user = TelegramUser.objects.select_for_update().filter(tg_user_id=user_id).first()
    if user is None:
        raise ValueError("User not found.")

    profile, _ = BillingProfile.objects.get_or_create(
        user_id=user.tg_user_id,
        defaults={"provider": MONO_PROVIDER, "wallet_id": _wallet_id(user.tg_user_id)},
    )
    _disable_auto_renew_for_profile(
        user=user,
        profile=profile,
        remote_reason="mono_autorenew_cancelled",
        delete_remote_token=True,
    )
    return profile


def run_due_monobank_charges() -> dict[str, int]:
    now = timezone.now()
    processed = 0
    failed = 0
    skipped = 0
    requires_reconciliation = 0
    seen_users: set[int] = set()
    due_subscriptions = (
        Subscription.objects.select_related("user")
        .filter(
            provider=MONO_PROVIDER,
            auto_renew=True,
            next_charge_at__isnull=False,
            next_charge_at__lte=now,
        )
        .order_by("user_id", "-created_at")
    )
    for subscription in due_subscriptions:
        if subscription.user_id in seen_users:
            continue
        seen_users.add(subscription.user_id)
        try:
            result = _run_single_charge(
                user_id=subscription.user_id, payment_kind=Payment.Kind.RENEWAL,
                expected_subscription_id=subscription.pk, expected_next_charge_at=subscription.next_charge_at,
            )
            processed += 1
            if result["status"] in {Payment.Status.FAILED, Payment.Status.REJECTED}:
                failed += 1
        except BillingChargeSkipped:
            skipped += 1
        except BillingChargeUnknown as exc:
            requires_reconciliation += 1
            logger.warning("Monobank renewal needs reconciliation for %s: %s", subscription.user_id, exc)
        except Exception as exc:  # pragma: no cover
            # Infrastructure errors are not evidence of a provider decline.
            # Only process_monobank_event may apply a verified failure/grace.
            failed += 1
            logger.warning("Monobank renewal task error for %s: %s", subscription.user_id, type(exc).__name__)
    return {"processed": processed, "failed": failed, "skipped": skipped,
            "requires_reconciliation": requires_reconciliation}
