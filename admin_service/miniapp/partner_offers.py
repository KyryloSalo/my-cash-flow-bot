from __future__ import annotations

from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from miniapp.models import AcquisitionSession, PartnerTrialClaim
from subscriptions.models import Subscription
from users.models import UserAdminState


class PartnerOfferUnavailable(Exception):
    pass


def public_partner_offer(*, link=None, claim=None, acquisition=None):
    if claim is not None:
        return {"code": claim.code, "name": claim.name, "trial_days": claim.trial_days, "offer_id": claim.offer_id}
    if acquisition is not None:
        if acquisition.expires_at <= timezone.now():
            return None
        link = acquisition.last_partner_link or acquisition.first_partner_link
        if link is None or not link.is_active:
            return None
        snapshot = acquisition.partner_offer_snapshot
        if snapshot is None:
            snapshot = public_partner_offer(link=link) or {}
            snapshot = dict(snapshot, partner_link_id=link.pk) if snapshot else {}
            AcquisitionSession.objects.filter(pk=acquisition.pk, partner_offer_snapshot__isnull=True).update(partner_offer_snapshot=snapshot)
            acquisition.refresh_from_db(fields=['partner_offer_snapshot'])
            snapshot = acquisition.partner_offer_snapshot
        if not snapshot or snapshot.get('partner_link_id') != link.pk:
            return None
        return {key: snapshot[key] for key in ('code', 'name', 'trial_days')}
    if link is not None and link.is_active and link.trial_days is not None and int(link.trial_days) > 0:
        return {"code": link.code, "name": link.name, "trial_days": int(link.trial_days)}
    return None


def partner_offer_for_user(*, user):
    claim = PartnerTrialClaim.objects.filter(user_id=user.tg_user_id).first()
    return public_partner_offer(claim=claim) if claim is not None else None


def claim_partner_offer(*, user, acquisition: AcquisitionSession | None) -> PartnerTrialClaim | None:
    """Freeze and atomically grant the trusted partner offer exactly once."""
    now = timezone.now()
    with transaction.atomic():
        locked_user = type(user).objects.select_for_update().get(pk=user.pk)
        existing = PartnerTrialClaim.objects.select_for_update().filter(user_id=locked_user.tg_user_id).first()
        if existing is not None:
            return existing
        state = UserAdminState.objects.filter(telegram_user_id=locked_user.tg_user_id).first()
        stronger_statuses = {Subscription.Status.ACTIVE, Subscription.Status.PAID, Subscription.Status.MANUAL, Subscription.Status.LIFETIME}
        if state and (state.subscription_status == UserAdminState.SubscriptionStatus.PAID or state.access_scope == UserAdminState.AccessScope.PERSONAL_FULL):
            return None
        if Subscription.objects.filter(user_id=locked_user.tg_user_id, status__in=stronger_statuses, expires_at__gt=now).exists():
            return None
        if acquisition is None or acquisition.expires_at <= now:
            return None
        if acquisition.user_id is not None and int(acquisition.user_id) != int(locked_user.tg_user_id):
            return None
        link = acquisition.last_partner_link or acquisition.first_partner_link
        if link is None or not link.is_active:
            return None
        offer = public_partner_offer(acquisition=acquisition)
        if offer is None:
            return None
        days = int(offer['trial_days'])
        started_at = now
        claim = PartnerTrialClaim.objects.create(
            user_id=locked_user.tg_user_id,
            partner_link=link,
            offer_id=f"partner_link:{link.pk}",
            code=offer['code'],
            name=offer['name'],
            trial_days=days,
        )
        Subscription.objects.create(
            user_id=locked_user.tg_user_id,
            plan="solo",
            status=Subscription.Status.TRIAL,
            provider="",
            started_at=started_at,
            expires_at=started_at + timedelta(days=days),
            trial_days=days,
            auto_renew=False,
            source=Subscription.Source.PROMO,
            comment=f"Partner offer {link.code}",
        )
        state, _ = UserAdminState.objects.get_or_create(telegram_user_id=locked_user.tg_user_id)
        state.subscription_status = UserAdminState.SubscriptionStatus.TRIAL
        state.access_scope = UserAdminState.AccessScope.PERSONAL_FULL
        state.access_source = f"partner:{link.code}"[:128]
        state.save(update_fields=["subscription_status", "access_scope", "access_source", "updated_at"])
        return claim
