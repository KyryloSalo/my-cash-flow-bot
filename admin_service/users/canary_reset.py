from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import asdict, dataclass

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from common.audit import create_audit_log, create_canary_audit
from miniapp.models import AcquisitionSession, AiTransactionDraft, AppNotification, FunnelEvent
from subscriptions.models import BillingConsent, BillingProfile, Payment, Subscription, SubscriptionEvent
from users.models import CanaryCapability, TelegramUser, UserAdminState, UserAuthSession


class CanaryResetError(Exception):
    pass


@dataclass(frozen=True)
class CanaryResetPreview:
    tg_user_id: int
    onboarding: int = 0
    sessions: int = 0
    capabilities: int = 0
    acquisition: int = 0
    synthetic_graph: int = 0
    notification_drafts: int = 0
    real_artifacts: tuple[str, ...] = ()

    def as_dict(self):
        return asdict(self)


def preview_fingerprint(preview: CanaryResetPreview) -> str:
    payload = json.dumps(preview.as_dict(), sort_keys=True, separators=(",", ":"), default=list).encode()
    return hmac.new(str(getattr(settings, "SECRET_KEY", "canary-reset")).encode(), payload, hashlib.sha256).hexdigest()


def _require_admin(admin_user):
    if not admin_user or not admin_user.is_authenticated or not admin_user.has_perm("users.can_manage_production_canary"):
        raise PermissionDenied


def _state(user, *, lock=False):
    qs = UserAdminState.objects.select_for_update() if lock else UserAdminState.objects
    return qs.get(telegram_user_id=user.tg_user_id)


def _assert_target(user, state=None):
    state = state or _state(user)
    if not getattr(settings, "PRODUCTION_CANARY_ENABLED", False):
        raise CanaryResetError("Production canary mode is disabled.")
    allowlist = {int(value) for value in getattr(settings, "PRODUCTION_CANARY_TELEGRAM_IDS", [])}
    if not state.is_test_user or not state.is_production_canary or user.tg_user_id not in allowlist:
        raise CanaryResetError("Target is not an eligible production canary.")
    return state


def _real_artifacts(user):
    found = []
    if BillingProfile.objects.filter(user=user, canary_owned=False).exists():
        found.append("billing_profile")
    if Payment.objects.filter(user=user, canary_owned=False).exists():
        found.append("payment")
    if Subscription.objects.filter(user=user, canary_owned=False).exists():
        found.append("subscription")
    if BillingConsent.objects.filter(user=user, canary_owned=False).exists():
        found.append("billing_consent")
    if AcquisitionSession.objects.filter(user=user, canary_owned=False).exists():
        found.append("acquisition_session")
    return tuple(sorted(set(found)))


def _preview_for(user):
    synthetic_payments = Payment.objects.filter(user=user, canary_owned=True)
    synthetic_subs = Subscription.objects.filter(user=user, canary_owned=True)
    payment_ids = synthetic_payments.values_list("pk", flat=True)
    notification_count = AppNotification.objects.filter(
        tg_user_id=user.tg_user_id,
        payload__payment_id__in=payment_ids,
    ).count()
    draft_count = AiTransactionDraft.objects.filter(
        tg_user_id=user.tg_user_id,
        source="billing_subscription",
        metadata__payment_id__in=payment_ids,
    ).count()
    graph = (
        synthetic_payments.count()
        + synthetic_subs.count()
        + SubscriptionEvent.objects.filter(subscription__in=synthetic_subs).count()
        + BillingConsent.objects.filter(user=user, canary_owned=True).count()
        + BillingProfile.objects.filter(user=user, canary_owned=True).count()
        + AcquisitionSession.objects.filter(user=user, canary_owned=True).count()
    )
    resettable_capabilities = CanaryCapability.objects.filter(telegram_user_id=user.tg_user_id)
    return CanaryResetPreview(
        user.tg_user_id,
        1,
        UserAuthSession.objects.filter(telegram_user_id=user.tg_user_id, revoked_at__isnull=True).count(),
        resettable_capabilities.count(),
        AcquisitionSession.objects.filter(user=user, canary_owned=True).count(),
        graph,
        notification_count + draft_count,
        _real_artifacts(user),
    )


def preview_canary_reset(*, user: TelegramUser, admin_user, request=None):
    _require_admin(admin_user)
    _assert_target(user)
    preview = _preview_for(user)
    create_canary_audit(admin_user=admin_user, request=request, action="reset_previewed", target_user_id=user.tg_user_id, result="previewed")
    return preview


def _execute_locked(user, state, *, admin_user, comment, request=None, failure_hook=None):
    preview = _preview_for(user)
    if preview.real_artifacts:
        raise CanaryResetError("Real provider artifacts block reset: " + ", ".join(preview.real_artifacts))
    synthetic_payments = Payment.objects.select_for_update().filter(user=user, canary_owned=True)
    synthetic_subs = Subscription.objects.select_for_update().filter(user=user, canary_owned=True)
    synthetic_consents = BillingConsent.objects.select_for_update().filter(user=user, canary_owned=True)
    synthetic_profiles = BillingProfile.objects.select_for_update().filter(user=user, canary_owned=True)
    synthetic_acquisition = AcquisitionSession.objects.select_for_update().filter(user=user, canary_owned=True)
    capabilities = CanaryCapability.objects.select_for_update().filter(telegram_user_id=user.tg_user_id)
    payment_ids = list(synthetic_payments.values_list("pk", flat=True))
    subscription_ids = list(synthetic_subs.values_list("pk", flat=True))
    acquisition_ids = list(synthetic_acquisition.values_list("pk", flat=True))
    list(synthetic_consents.values_list("pk", flat=True))
    list(synthetic_profiles.values_list("pk", flat=True))
    list(capabilities.values_list("pk", flat=True))

    AppNotification.objects.select_for_update().filter(
        tg_user_id=user.tg_user_id,
        payload__payment_id__in=payment_ids,
    ).delete()
    AiTransactionDraft.objects.select_for_update().filter(
        tg_user_id=user.tg_user_id,
        source="billing_subscription",
        metadata__payment_id__in=payment_ids,
    ).delete()
    SubscriptionEvent.objects.select_for_update().filter(subscription_id__in=subscription_ids).delete()
    synthetic_payments.delete()
    synthetic_consents.delete()
    synthetic_profiles.delete()
    synthetic_subs.delete()
    FunnelEvent.objects.select_for_update().filter(acquisition_session_id__in=acquisition_ids).delete()
    synthetic_acquisition.delete()
    active_sessions = UserAuthSession.objects.select_for_update().filter(
        telegram_user_id=user.tg_user_id,
        revoked_at__isnull=True,
    )
    list(active_sessions.values_list("pk", flat=True))
    active_sessions.update(revoked_at=timezone.now())
    capabilities.delete()
    state.current_fsm_state = ""
    state.onboarding_payload = {}
    state.last_user_input = state.last_bot_response = state.last_parse_error = ""
    state.subscription_status = UserAdminState.SubscriptionStatus.NONE
    state.access_scope = UserAdminState.AccessScope.PAYWALL
    state.access_source = state.canary_notes = ""
    state.save(update_fields=["current_fsm_state", "onboarding_payload", "last_user_input", "last_bot_response", "last_parse_error", "subscription_status", "access_scope", "access_source", "canary_notes", "updated_at"])
    if failure_hook:
        failure_hook()
    create_canary_audit(admin_user=admin_user, request=request, action="reset_executed", target_user_id=user.tg_user_id, result="executed", reason=comment)
    return preview


def execute_canary_reset(*, user: TelegramUser, admin_user, confirmation: str, comment: str, tg_user_id=None, preview_fingerprint: str = "", request=None, failure_hook=None):
    _require_admin(admin_user)
    if confirmation != "RESET PRODUCTION CANARY" or not comment.strip() or tg_user_id is None:
        raise ValidationError("Exact tg_user_id, explicit confirmation, fingerprint, and comment are required.")
    try:
        with transaction.atomic():
            locked = TelegramUser.objects.select_for_update().get(tg_user_id=tg_user_id)
            if locked.tg_user_id != user.tg_user_id:
                raise CanaryResetError("Target user mismatch.")
            state = _state(locked, lock=True)
            _assert_target(locked, state)
            current = _preview_for(locked)
            expected = globals()["preview_fingerprint"](current)
            if not preview_fingerprint or not hmac.compare_digest(preview_fingerprint, expected):
                raise CanaryResetError("Preview is stale or edited.")
            return _execute_locked(locked, state, admin_user=admin_user, comment=comment, request=request, failure_hook=failure_hook)
    except Exception as exc:
        create_canary_audit(admin_user=admin_user, request=request, action="reset_failed", target_user_id=tg_user_id, result="failed", reason=type(exc).__name__)
        raise