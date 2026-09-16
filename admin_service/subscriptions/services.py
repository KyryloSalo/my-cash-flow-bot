from __future__ import annotations

from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from subscriptions.models import Payment, Plan, Subscription, SubscriptionEvent
from users.models import TelegramUser, UserAdminState


def _admin_state_defaults() -> dict[str, object]:
    return {
        "status": UserAdminState.Status.ACTIVE,
        "subscription_status": UserAdminState.SubscriptionStatus.NONE,
        "access_scope": UserAdminState.AccessScope.PAYWALL,
        "access_source": "",
        "source": "",
        "referral_code": "",
        "pending_start_payload": "",
        "timezone": "",
        "is_blocked": False,
        "can_receive_messages": True,
        "blocked_bot": False,
        "admin_comment": "",
        "is_test_user": False,
        "test_user_notes": "",
        "current_fsm_state": "",
        "onboarding_payload": {},
        "last_user_input": "",
        "last_bot_response": "",
        "last_parse_error": "",
        "pending_admin_reset_mode": "",
    }


def _map_subscription_status(subscription_status: str) -> str:
    if subscription_status == Subscription.Status.TRIAL:
        return UserAdminState.SubscriptionStatus.TRIAL
    if subscription_status in {Subscription.Status.ACTIVE, Subscription.Status.PAID, Subscription.Status.MANUAL, Subscription.Status.LIFETIME}:
        return UserAdminState.SubscriptionStatus.PAID
    if subscription_status == Subscription.Status.CANCELLED:
        return UserAdminState.SubscriptionStatus.CANCELLED
    if subscription_status == Subscription.Status.EXPIRED:
        return UserAdminState.SubscriptionStatus.EXPIRED
    return UserAdminState.SubscriptionStatus.NONE


def _admin_action_grants_full_access(action: str) -> bool:
    return action in {"trial", "activate", "extend", "reactivate", "manual", "lifetime", "grace"}


def sync_admin_subscription_state(user_id: int, *, subscription: Subscription | None = None) -> UserAdminState:
    state, _ = UserAdminState.objects.get_or_create(
        telegram_user_id=user_id,
        defaults=_admin_state_defaults(),
    )
    if subscription is None:
        subscription = Subscription.objects.filter(user_id=user_id).order_by("-created_at").first()
    if subscription is None:
        state.subscription_status = UserAdminState.SubscriptionStatus.NONE
    else:
        state.subscription_status = _map_subscription_status(subscription.status)
    state.save(update_fields=["subscription_status", "updated_at"])
    return state


def sync_admin_access_scope(
    user_id: int,
    *,
    access_scope: str,
    access_source: str = "",
    pending_start_payload: str | None = None,
    clear_pending_start_payload: bool = False,
) -> UserAdminState:
    state, _ = UserAdminState.objects.get_or_create(
        telegram_user_id=user_id,
        defaults=_admin_state_defaults(),
    )
    state.access_scope = access_scope
    state.access_source = access_source
    update_fields = ["access_scope", "access_source", "updated_at"]
    if pending_start_payload is not None:
        state.pending_start_payload = pending_start_payload
        update_fields.append("pending_start_payload")
    elif clear_pending_start_payload:
        state.pending_start_payload = ""
        update_fields.append("pending_start_payload")
    state.save(update_fields=update_fields)
    return state


def latest_subscription_for_user(user: TelegramUser) -> Subscription | None:
    return Subscription.objects.filter(user_id=user.tg_user_id).order_by("-created_at").first()


def resolve_subscription_plan_ref(subscription: Subscription | None) -> Plan | None:
    if subscription is None or not subscription.plan_ref_id:
        return None
    return Plan.objects.filter(pk=subscription.plan_ref_id).first()


def resolve_payment_plan_ref(payment: Payment) -> Plan | None:
    if not payment.plan_id:
        return None
    return Plan.objects.filter(pk=payment.plan_id).first()


@transaction.atomic
def apply_subscription_change(
    *,
    user: TelegramUser,
    action: str,
    admin_user,
    plan_slug: str | None = None,
    plan_ref: Plan | None = None,
    days: int | None = None,
    amount=None,
    currency: str | None = None,
    source: str = Subscription.Source.ADMIN,
    comment: str = "",
) -> Subscription:
    now = timezone.now()
    subscription = latest_subscription_for_user(user)
    if subscription is None:
        subscription = Subscription(user_id=user.tg_user_id, provider="manual")

    if plan_ref is not None:
        subscription.plan_ref = plan_ref
        plan_slug = plan_ref.slug
    if plan_slug:
        subscription.plan = plan_slug

    if amount is not None:
        subscription.amount = amount
    if currency is not None:
        subscription.currency = currency

    subscription.provider = "manual"
    subscription.source = source
    subscription.comment = comment
    subscription.updated_by = admin_user
    if subscription.pk is None:
        subscription.created_by = admin_user

    if action == "trial":
        duration_days = days or 7
        subscription.status = Subscription.Status.TRIAL
        subscription.started_at = now
        subscription.expires_at = now + timedelta(days=duration_days)
    elif action in {"activate", "extend", "reactivate"}:
        duration_days = days or (plan_ref.duration_days if plan_ref else 30)
        base_date = max(subscription.expires_at or now, now)
        subscription.status = Subscription.Status.ACTIVE
        subscription.started_at = subscription.started_at or now
        subscription.expires_at = base_date + timedelta(days=duration_days)
    elif action == "manual":
        duration_days = days or (plan_ref.duration_days if plan_ref else 30)
        base_date = max(subscription.expires_at or now, now)
        subscription.status = Subscription.Status.MANUAL
        subscription.started_at = subscription.started_at or now
        subscription.expires_at = base_date + timedelta(days=duration_days)
    elif action == "lifetime":
        subscription.status = Subscription.Status.LIFETIME
        subscription.started_at = subscription.started_at or now
        subscription.expires_at = None
    elif action == "expire":
        subscription.status = Subscription.Status.EXPIRED
        subscription.expires_at = now
    elif action == "cancel":
        subscription.status = Subscription.Status.CANCELLED
    elif action == "grace":
        duration_days = days or 3
        base_date = max(subscription.expires_at or now, now)
        if subscription.status == Subscription.Status.EXPIRED:
            subscription.status = Subscription.Status.ACTIVE
        subscription.expires_at = base_date + timedelta(days=duration_days)
    else:
        raise ValueError(f"Unsupported subscription action: {action}")

    subscription.save()
    sync_admin_subscription_state(user.tg_user_id, subscription=subscription)
    sync_admin_access_scope(
        user.tg_user_id,
        access_scope=(
            UserAdminState.AccessScope.PERSONAL_FULL
            if _admin_action_grants_full_access(action)
            else UserAdminState.AccessScope.PAYWALL
        ),
        access_source="billing",
    )
    SubscriptionEvent.objects.create(
        subscription=subscription,
        user_id=user.tg_user_id,
        event_type=f"admin_{action}",
        payload={
            "days": days,
            "plan": subscription.plan,
            "plan_ref_id": subscription.plan_ref_id,
            "amount": str(amount) if amount is not None else "",
            "currency": currency or "",
            "source": source,
            "comment": comment,
        },
    )
    return subscription


@transaction.atomic
def confirm_payment(*, payment: Payment, admin_user) -> Subscription:
    if payment.status in {Payment.Status.PAID, Payment.Status.MANUAL_CONFIRMED}:
        raise ValueError("Payment already confirmed.")

    plan_ref = resolve_payment_plan_ref(payment)
    duration_days = plan_ref.duration_days if plan_ref else 30
    subscription = apply_subscription_change(
        user=payment.user,
        action="manual",
        admin_user=admin_user,
        plan_slug=plan_ref.slug if plan_ref else (payment.subscription.plan if payment.subscription_id else "manual"),
        plan_ref=plan_ref,
        days=duration_days,
        amount=payment.amount,
        currency=payment.currency,
        source=Subscription.Source.PAYMENT,
        comment=f"Payment #{payment.pk} confirmed manually",
    )
    payment.subscription = subscription
    payment.status = Payment.Status.MANUAL_CONFIRMED
    payment.paid_at = timezone.now()
    payment.confirmed_by = admin_user
    payment.save(update_fields=["subscription", "status", "paid_at", "confirmed_by", "updated_at"])
    return subscription
