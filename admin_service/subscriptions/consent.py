from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from django.conf import settings
from django.db import transaction

from miniapp.funnel import FunnelValidationError, record_server_event
from miniapp.models import AcquisitionSession
from subscriptions.models import BillingConsent, Payment
from users.models import TelegramUser


def build_server_consent_terms(*, trial_days: int, copy_locale: str) -> dict[str, Any]:
    normalized_trial_days = max(1, min(int(trial_days or 0), 365))
    locale = str(copy_locale or "uk").strip().lower()
    if locale not in {"uk", "en"}:
        locale = "uk"
    return {
        "offer_id": str(
            getattr(settings, "PWA_BILLING_OFFER_ID", "organic_30d_1uah_499uah")
            or "organic_30d_1uah_499uah"
        ),
        "offer_version": str(getattr(settings, "PWA_BILLING_OFFER_VERSION", "1") or "1"),
        "bind_amount_minor": int(settings.MONO_BIND_AMOUNT),
        "currency": "UAH",
        "trial_days": normalized_trial_days,
        "renewal_amount_minor": int(settings.MONO_RENEWAL_AMOUNT),
        "renewal_period_days": int(settings.MONO_RENEWAL_PERIOD_DAYS),
        "renewal_anchor": "payment_success",
        "first_renewal_at": None,
        "terms_version": str(getattr(settings, "PWA_BILLING_TERMS_VERSION", "2026-09") or "2026-09"),
        "privacy_version": str(getattr(settings, "PWA_PRIVACY_VERSION", "2026-09") or "2026-09"),
        "copy_locale": locale,
    }


def acquisition_consent_snapshot(acquisition: AcquisitionSession | None) -> dict[str, str]:
    if acquisition is None:
        return {}
    return {
        "funnel_version": acquisition.funnel_version,
        "landing_variant": acquisition.last_landing_variant,
        "utm_source": acquisition.utm_source,
        "utm_medium": acquisition.utm_medium,
        "utm_campaign": acquisition.utm_campaign,
        "referral_code": acquisition.referral_code,
        "platform": acquisition.platform,
        "container": acquisition.container,
    }


def _consent_hash(*, terms: dict[str, Any], acquisition_snapshot: dict[str, str]) -> str:
    serialized = json.dumps(
        {"terms": terms, "acquisition": acquisition_snapshot},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@transaction.atomic
def accept_billing_consent(
    *,
    user: TelegramUser,
    acquisition: AcquisitionSession | None,
    idempotency_key: str,
    trial_days: int,
    copy_locale: str,
) -> tuple[BillingConsent, bool]:
    try:
        parsed_key = uuid.UUID(str(idempotency_key or ""))
    except (ValueError, TypeError, AttributeError) as exc:
        raise FunnelValidationError("invalid_idempotency_key", "idempotency_key must be a UUID.") from exc
    terms = build_server_consent_terms(trial_days=trial_days, copy_locale=copy_locale)
    snapshot = acquisition_consent_snapshot(acquisition)
    payload_hash = _consent_hash(terms=terms, acquisition_snapshot=snapshot)
    consent, created = BillingConsent.objects.get_or_create(
        user=user,
        idempotency_key=parsed_key,
        defaults={
            "acquisition_session": acquisition,
            "acquisition_snapshot": snapshot,
            "payload_hash": payload_hash,
            **terms,
        },
    )
    if consent.payload_hash != payload_hash:
        raise FunnelValidationError(
            "idempotency_conflict",
            "The consent id was already used for another terms snapshot.",
            status=409,
        )
    if acquisition is not None:
        record_server_event(
            acquisition=acquisition,
            user=user,
            event_name="billing_consent_accepted",
            idempotency_key=f"consent:{consent.pk}",
            offer_variant=consent.offer_id,
        )
    return consent, created


@transaction.atomic
def attach_consent_payment(*, consent: BillingConsent, payment: Payment) -> BillingConsent:
    locked = BillingConsent.objects.select_for_update().get(pk=consent.pk)
    if int(locked.user_id) != int(payment.user_id):
        raise FunnelValidationError(
            "consent_user_mismatch",
            "Billing consent does not belong to this payment user.",
            status=409,
        )
    if locked.payment_id is not None and int(locked.payment_id) != int(payment.pk):
        raise FunnelValidationError(
            "consent_payment_conflict",
            "Billing consent is already bound to another payment.",
            status=409,
        )
    if locked.payment_id is None:
        locked.payment = payment
        locked.save(update_fields=["payment"])
        if locked.acquisition_session_id is not None:
            record_server_event(
                acquisition=locked.acquisition_session,
                user=locked.user,
                event_name="bind_invoice_created",
                idempotency_key=f"bind_invoice:{payment.pk}",
                offer_variant=locked.offer_id,
            )
    return locked


def get_billing_consent_for_user(*, consent_id: str, user: TelegramUser) -> BillingConsent:
    try:
        parsed_id = uuid.UUID(str(consent_id or ""))
    except (ValueError, TypeError, AttributeError) as exc:
        raise FunnelValidationError("invalid_consent_id", "consent_id must be a UUID.") from exc
    consent = BillingConsent.objects.filter(pk=parsed_id, user=user).first()
    if consent is None:
        raise FunnelValidationError("billing_consent_missing", "Billing consent was not found.", status=404)
    return consent


def attach_consent_to_payment_id(
    *,
    consent_id: str,
    user: TelegramUser,
    payment_id: int,
) -> BillingConsent:
    consent = get_billing_consent_for_user(consent_id=consent_id, user=user)
    payment = Payment.objects.filter(pk=payment_id, user=user).first()
    if payment is None:
        raise FunnelValidationError("billing_payment_missing", "Billing payment was not found.", status=404)
    return attach_consent_payment(consent=consent, payment=payment)


@transaction.atomic
def finalize_paid_consent(*, payment: Payment, first_renewal_at) -> int:
    updated = 0
    consents = list(
        BillingConsent.objects.select_for_update()
        .filter(payment=payment)
    )
    for consent in consents:
        if consent.first_renewal_at is None and first_renewal_at is not None:
            consent.first_renewal_at = first_renewal_at
            consent.save(update_fields=["first_renewal_at"])
        if consent.acquisition_session_id is not None:
            _event, created = record_server_event(
                acquisition=consent.acquisition_session,
                user=consent.user,
                event_name="payment_success",
                idempotency_key=f"payment:{payment.pk}:success",
                offer_variant=consent.offer_id,
            )
            updated += int(created)
    return updated
