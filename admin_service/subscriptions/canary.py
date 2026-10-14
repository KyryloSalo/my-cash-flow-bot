from dataclasses import dataclass
from decimal import Decimal
import hashlib
import hmac
import json
from typing import Any

from django.db import transaction
from django.utils import timezone
from django.conf import settings

from subscriptions.billing import ProviderPaymentEvent, finalize_payment_event
from subscriptions.consent import attach_consent_to_payment_id, get_billing_consent_for_user
from subscriptions.models import Payment, Plan, BillingConsent
from users.canary import CanaryAuthorizationError, assert_canary_user, issue_canary_capability
from users.models import CanaryCapability, TelegramUser, UserAdminState


class CanaryBindError(ValueError):
    pass


def _assert_canary_bind_user(user: TelegramUser) -> None:
    try:
        assert_canary_user(user)
    except CanaryAuthorizationError as exc:
        raise CanaryBindError("Canary checkout is unavailable.") from exc


def _assert_canary_amount_minor(amount_minor: int) -> None:
    maximum = int(getattr(settings, "PRODUCTION_CANARY_MAX_AMOUNT", 0) or 0)
    if int(amount_minor) > maximum:
        raise CanaryBindError("Canary amount exceeds the production limit.")


def _review_digest(*, capability, payment) -> str:
    metadata = {
        key: value
        for key, value in (capability.metadata or {}).items()
        if key != "review_digest"
    }
    terms = {
        "capability_id": str(capability.pk),
        "purpose": capability.purpose,
        "scenario": capability.scenario,
        "payment_id": payment.pk,
        "amount_minor": int((Decimal(payment.amount) * 100).to_integral_value()),
        "currency": payment.currency,
        "metadata": metadata,
    }
    return hashlib.sha256(json.dumps(terms, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _capability_token_hash(token: str) -> str:
    import hashlib
    return hashlib.sha256(str(token).encode("utf-8")).hexdigest()


def _lock_canary_state(user: TelegramUser) -> UserAdminState:
    """Share reset's serialization point without importing reset services."""
    return UserAdminState.objects.select_for_update().get(telegram_user_id=user.tg_user_id)


def confirm_canary_renewal(*, user: TelegramUser, capability_token: str, confirmed: bool) -> dict[str, Any]:
    result = _confirm_canary_renewal(
        user=user,
        capability_token=capability_token,
        confirmed=confirmed,
    )
    if result is None:
        raise CanaryBindError("Canary renewal capability is unavailable.")
    return result


@transaction.atomic
def _confirm_canary_renewal(
    *, user: TelegramUser, capability_token: str, confirmed: bool
) -> dict[str, Any] | None:
    """Apply one provider-neutral renewal simulation; never dispatches a provider charge."""
    assert_canary_user(user)
    _lock_canary_state(user)
    if not confirmed:
        raise CanaryBindError("Explicit confirmation is required.")
    capability = CanaryCapability.objects.select_for_update().filter(
        nonce_hash=_capability_token_hash(capability_token),
        telegram_user=user,
        purpose=CanaryCapability.Purpose.BILLING_RENEWAL,
    ).first()
    if capability is None or capability.scenario not in _SCENARIO_STATUS:
        raise CanaryBindError("Canary renewal capability is invalid.")
    if capability.status in {CanaryCapability.Status.COMPLETED, CanaryCapability.Status.FAILED}:
        return {"status": "completed", "payment_id": capability.related_payment_id, "idempotent": True}
    if capability.status != CanaryCapability.Status.ISSUED:
        raise CanaryBindError("Canary renewal capability is unavailable.")
    if capability.expires_at <= timezone.now():
        capability.status = CanaryCapability.Status.EXPIRED
        capability.save(update_fields=["status"])
        return None
    subscription = __import__("subscriptions.models", fromlist=["Subscription"]).Subscription.objects.select_for_update().filter(
        user_id=user.tg_user_id, pk=capability.metadata.get("subscription_id")
    ).first()
    if subscription is None:
        raise CanaryBindError("Canary renewal subscription binding is invalid.")
    _assert_canary_amount_minor(int((subscription.amount * 100).to_integral_value()))
    from subscriptions.models import Payment
    payment = Payment.objects.create(
        user=user, subscription=subscription, provider="canary", kind=Payment.Kind.RENEWAL,
        amount=subscription.amount or 0, currency=subscription.currency or "UAH",
        status=Payment.Status.PENDING,
        provider_payment_id=f"canary-renewal:{capability.pk}",
        external_transaction_id=f"canary-renewal:{capability.pk}",
        raw_payload={"canary": {"scenario": capability.scenario, "synthetic": True, "canary_owned": True}},
        canary_owned=True,
    )
    event = ProviderPaymentEvent(
        provider="canary", provider_payment_id=payment.provider_payment_id,
        status=_SCENARIO_STATUS[capability.scenario], amount=int(payment.amount * 100), currency=payment.currency,
        modified_at=timezone.now(), card_token=None, masked_pan=None, action_url=None,
        failure_reason=None,
        raw_payload={"canary": {"scenario": capability.scenario, "result": "success" if _SCENARIO_STATUS[capability.scenario] == Payment.Status.PAID else "failure", "synthetic": True, "canary_owned": True},
                     "invoiceId": payment.provider_payment_id},
    )
    payment, _ = finalize_payment_event(event)
    capability.status = CanaryCapability.Status.COMPLETED if payment.status == Payment.Status.PAID else CanaryCapability.Status.FAILED
    capability.consumed_at = capability.consumed_at or timezone.now()
    capability.completed_at = timezone.now()
    capability.related_payment_id = payment.pk
    capability.save(update_fields=["status", "consumed_at", "completed_at", "related_payment_id"])
    return {"status": payment.status, "payment_id": payment.pk, "idempotent": False}


@dataclass(frozen=True)
class CanaryBindInit:
    payment_id: int
    capability_token: str
    page_url: str


@dataclass(frozen=True)
class CanaryBindResult:
    payment_id: int


_SCENARIO_STATUS = {
    CanaryCapability.Scenario.SUCCESS: Payment.Status.PAID,
    CanaryCapability.Scenario.INSUFFICIENT_FUNDS: Payment.Status.FAILED,
    CanaryCapability.Scenario.DECLINED: Payment.Status.FAILED,
    CanaryCapability.Scenario.PROCESSING: Payment.Status.PROCESSING,
    CanaryCapability.Scenario.DELAYED_SUCCESS: Payment.Status.PROCESSING,
    CanaryCapability.Scenario.DELAYED_FAILURE: Payment.Status.PROCESSING,
    CanaryCapability.Scenario.ACTION_REQUIRED: Payment.Status.ACTION_REQUIRED,
}


def get_canary_bind_preview(*, user: TelegramUser, capability_token: str) -> int:
    """Validate a checkout capability without consuming it or mutating billing state."""
    assert_canary_user(user)
    token_hash = __import__("hashlib").sha256(str(capability_token).encode()).hexdigest()
    capability = CanaryCapability.objects.filter(nonce_hash=token_hash).first()
    return _canary_bind_preview_for_capability(user=user, capability=capability)


def exchange_canary_bind_capability(*, user: TelegramUser, capability_token: str) -> int:
    """Exchange a raw capability for its non-secret database identifier."""
    preview = get_canary_bind_preview(user=user, capability_token=capability_token)
    return int(preview["capability_id"])


def get_canary_bind_preview_by_id(*, user: TelegramUser, capability_id: int) -> dict[str, Any]:
    assert_canary_user(user)
    capability = CanaryCapability.objects.filter(pk=capability_id).first()
    return _canary_bind_preview_for_capability(user=user, capability=capability)


def _canary_bind_preview_for_capability(*, user: TelegramUser, capability) -> dict[str, Any]:
    if (
        capability is None
        or capability.telegram_user_id != user.tg_user_id
        or capability.purpose != CanaryCapability.Purpose.BILLING_BIND
        or capability.scenario not in _SCENARIO_STATUS
        or capability.status != CanaryCapability.Status.ISSUED
        or capability.expires_at <= timezone.now()
    ):
        raise CanaryBindError("Canary capability is unavailable.")
    payment = Payment.objects.filter(
        pk=capability.related_payment_id,
        user=user,
        provider="canary",
        kind=Payment.Kind.BIND,
    ).first()
    if payment is None:
        raise CanaryBindError("Canary capability is unavailable.")
    return {"capability_id": capability.pk, "scenario": capability.scenario, "payment_id": payment.pk, "review_digest": (capability.metadata or {}).get("review_digest", ""), "amount": str(payment.amount), "currency": payment.currency}


@transaction.atomic
def init_canary_bind(*, user: TelegramUser, consent_id: int, plan: Plan, scenario: str = CanaryCapability.Scenario.SUCCESS, issued_by=None) -> CanaryBindInit:
    assert_canary_user(user)
    _lock_canary_state(user)
    allowed = set(getattr(__import__("django.conf", fromlist=["settings"]).settings, "PRODUCTION_CANARY_ALLOWED_SCENARIOS", _SCENARIO_STATUS))
    if scenario not in _SCENARIO_STATUS or scenario not in allowed:
        raise CanaryBindError("Canary scenario is unavailable.")
    consent = BillingConsent.objects.select_for_update().filter(pk=consent_id, user=user).first()
    if consent is None:
        raise CanaryBindError("Billing consent was not found.")
    plan = Plan.objects.select_for_update().filter(pk=plan.pk, is_active=True).first()
    if plan is None or (int(consent.bind_amount_minor), str(consent.currency).upper()) != (int(Decimal(plan.price) * 100), str(plan.currency).upper()):
        raise CanaryBindError("Consent and plan economics do not match.")
    _assert_canary_amount_minor(int(consent.bind_amount_minor))
    if consent.payment_id:
        payment = consent.payment
        if payment.provider != "canary":
            raise CanaryBindError("Consent is already attached.")
    else:
        payment = Payment.objects.create(
            user=user, plan=plan, provider="canary", kind=Payment.Kind.BIND,
            amount=Decimal(consent.bind_amount_minor) / 100, currency=consent.currency,
            status=Payment.Status.PENDING,
            canary_owned=True,
        )
        attach_consent_to_payment_id(consent_id=str(consent_id), user=user, payment_id=payment.pk)
        BillingConsent.objects.filter(pk=consent.pk).update(canary_owned=True)
    capability, token = issue_canary_capability(
        user=user, purpose=CanaryCapability.Purpose.BILLING_BIND,
        issued_by=issued_by, scenario=scenario,
        related_payment_id=payment.pk,
        metadata={"consent_id": str(consent.pk), "plan_id": plan.pk, "payment_id": payment.pk},
    )
    capability.metadata = {**(capability.metadata or {}), "review_digest": _review_digest(capability=capability, payment=payment)}
    capability.save(update_fields=["metadata"])
    return CanaryBindInit(payment.pk, token, f"/app/canary/checkout/#token={token}")


@transaction.atomic
def confirm_canary_bind(*, user: TelegramUser, capability_token: str, confirmed: bool, review_digest: str = "") -> CanaryBindResult:
    _assert_canary_bind_user(user)
    _lock_canary_state(user)
    if not confirmed:
        raise CanaryBindError("Explicit confirmation is required.")
    token_hash = __import__("hashlib").sha256(str(capability_token).encode()).hexdigest()
    capability = CanaryCapability.objects.select_for_update().filter(nonce_hash=token_hash).first()
    return _confirm_canary_bind_capability(user=user, capability=capability, confirmed=confirmed, review_digest=review_digest)


@transaction.atomic
def confirm_canary_bind_by_id(*, user: TelegramUser, capability_id: int, confirmed: bool, review_digest: str = "") -> CanaryBindResult:
    _assert_canary_bind_user(user)
    _lock_canary_state(user)
    capability = CanaryCapability.objects.select_for_update().filter(pk=capability_id).first()
    return _confirm_canary_bind_capability(user=user, capability=capability, confirmed=confirmed, review_digest=review_digest)


def _confirm_canary_bind_capability(*, user, capability, confirmed: bool, review_digest: str) -> CanaryBindResult:
    if not confirmed:
        raise CanaryBindError("Explicit confirmation is required.")
    if (
        capability is None
        or capability.telegram_user_id != user.tg_user_id
        or capability.purpose != CanaryCapability.Purpose.BILLING_BIND
        or capability.scenario not in _SCENARIO_STATUS
    ):
        raise CanaryBindError("Canary capability is invalid.")
    if capability.status == CanaryCapability.Status.COMPLETED:
        return CanaryBindResult(int((capability.metadata or {}).get("payment_id") or capability.related_payment_id))
    if capability.status != CanaryCapability.Status.ISSUED or capability.expires_at <= timezone.now():
        raise CanaryBindError("Canary capability is unavailable.")
    payment = Payment.objects.select_for_update().get(pk=capability.related_payment_id, user=user, provider="canary")
    _assert_canary_amount_minor(int((payment.amount * 100).to_integral_value()))
    expected_digest = _review_digest(capability=capability, payment=payment)
    metadata = capability.metadata or {}
    consent = BillingConsent.objects.select_for_update().filter(pk=metadata.get("consent_id"), user=user, payment=payment).first()
    plan = Plan.objects.select_for_update().filter(pk=metadata.get("plan_id"), is_active=True).first()
    if consent is None or plan is None or payment.plan_id != plan.pk or (int(consent.bind_amount_minor), str(consent.currency).upper()) != (int(Decimal(plan.price) * 100), str(plan.currency).upper()):
        raise CanaryBindError("Canary capability economic binding is invalid.")
    if not review_digest or not hmac.compare_digest(review_digest, expected_digest) or review_digest != (capability.metadata or {}).get("review_digest"):
        raise CanaryBindError("Canary checkout is unavailable.")
    scenario = capability.scenario
    status = _SCENARIO_STATUS[scenario]
    payload = {
        "invoiceId": payment.provider_payment_id or f"canary:{payment.pk}",
        "status": "success" if status == Payment.Status.PAID else ("processing" if status == Payment.Status.PROCESSING else "action_required"), "amount": int(payment.amount * 100), "ccy": 980,
        "modifiedDate": int(timezone.now().timestamp() * 1000),
        "walletData": {"cardToken": f"canary:synthetic:{payment.pk}", "maskedPan": "444455******1111"},
        "canary": {"scenario": scenario, "result": "success" if status == Payment.Status.PAID else "failure", "synthetic": True, "canary_owned": True},
    }
    payment.provider_payment_id = payload["invoiceId"]
    payment.external_transaction_id = f"canary:{payment.pk}"
    payment.save(update_fields=["provider_payment_id", "external_transaction_id", "updated_at"])
    event = ProviderPaymentEvent("canary", payload["invoiceId"], status, payload["amount"], "UAH", timezone.now(), payload["walletData"]["cardToken"], payload["walletData"]["maskedPan"], None, None, payload)
    finalized, _ = finalize_payment_event(event)
    if finalized is None:
        raise CanaryBindError("Canary payment finalization failed.")
    capability.status = CanaryCapability.Status.COMPLETED if status in {Payment.Status.PAID, Payment.Status.FAILED, Payment.Status.ACTION_REQUIRED} else CanaryCapability.Status.CONSUMED
    capability.completed_at = timezone.now()
    capability.consumed_at = capability.consumed_at or timezone.now()
    capability.metadata = {**(capability.metadata or {}), "payment_id": finalized.pk}
    capability.save(update_fields=["status", "completed_at", "consumed_at", "metadata"])
    return CanaryBindResult(finalized.pk)


@transaction.atomic
def complete_delayed_canary(*, user: TelegramUser, capability_token: str, confirmed: bool) -> CanaryBindResult:
    if not confirmed:
        raise CanaryBindError("Explicit confirmation is required.")
    assert_canary_user(user)
    _lock_canary_state(user)
    token_hash = __import__("hashlib").sha256(str(capability_token).encode()).hexdigest()
    capability = CanaryCapability.objects.select_for_update().filter(nonce_hash=token_hash, telegram_user=user).first()
    if capability is None or capability.purpose != CanaryCapability.Purpose.BILLING_BIND or capability.scenario not in {CanaryCapability.Scenario.DELAYED_SUCCESS, CanaryCapability.Scenario.DELAYED_FAILURE}:
        raise CanaryBindError("Delayed canary capability is invalid.")
    if capability.status == CanaryCapability.Status.COMPLETED:
        return CanaryBindResult(int((capability.metadata or {}).get("payment_id") or capability.related_payment_id))
    if capability.status == CanaryCapability.Status.ISSUED and capability.expires_at <= timezone.now():
        capability.status = CanaryCapability.Status.EXPIRED
        capability.completed_at = timezone.now()
        capability.save(update_fields=["status", "completed_at"])
        raise CanaryBindError("Delayed canary capability is unavailable.")
    if capability.status != CanaryCapability.Status.CONSUMED:
        raise CanaryBindError("Delayed canary capability must be consumed before completion.")
    payment = Payment.objects.select_for_update().get(pk=capability.related_payment_id, user=user, provider="canary")
    _assert_canary_amount_minor(int((payment.amount * 100).to_integral_value()))
    if payment.status in {Payment.Status.PAID, Payment.Status.FAILED, Payment.Status.REJECTED}:
        capability.status = CanaryCapability.Status.COMPLETED
        capability.completed_at = capability.completed_at or timezone.now()
        capability.metadata = {**(capability.metadata or {}), "payment_id": payment.pk}
        capability.save(update_fields=["status", "completed_at", "metadata"])
        return CanaryBindResult(payment.pk)
    final_status = Payment.Status.PAID if capability.scenario == CanaryCapability.Scenario.DELAYED_SUCCESS else Payment.Status.FAILED
    event = ProviderPaymentEvent(
        provider="canary", provider_payment_id=payment.provider_payment_id, status=final_status,
        amount=int(payment.amount * 100), currency=payment.currency, modified_at=timezone.now(),
        card_token=None, masked_pan=None, action_url=None, failure_reason=None,
        raw_payload={"canary": {
                         "scenario": capability.scenario,
                         "result": "success" if final_status == Payment.Status.PAID else "failure",
                         "synthetic": True,
                         "canary_owned": True,
                     },
                     "invoiceId": payment.provider_payment_id,
                     "status": "success" if final_status == Payment.Status.PAID else "failure"},
    )
    finalized, _ = finalize_payment_event(event)
    if finalized is None:
        raise CanaryBindError("Canary payment finalization failed.")
    capability.status = CanaryCapability.Status.COMPLETED
    capability.consumed_at = capability.consumed_at or timezone.now()
    capability.completed_at = timezone.now()
    capability.metadata = {**(capability.metadata or {}), "payment_id": finalized.pk}
    capability.save(update_fields=["status", "consumed_at", "completed_at", "metadata"])
    return CanaryBindResult(finalized.pk)
