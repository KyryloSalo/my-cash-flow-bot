"""Explicitly synthetic data; only used by the disposable acceptance database."""
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.utils import timezone
from miniapp.models import AcquisitionSession, DurableRegistrationAttribution, FunnelEvent, PartnerLink, PartnerLinkClick
from subscriptions.models import Payment
from users.models import TelegramUser


def seed():
    password = os.environ["MCF_ACCEPTANCE_PASSWORD"]
    users = get_user_model()
    users.objects.create_superuser("accept", "accept@example.invalid", password)
    viewer = users.objects.create_user("limited", password=password, is_staff=True)
    viewer.user_permissions.add(Permission.objects.get(content_type__app_label="miniapp", codename="view_partnerlink"))
    users.objects.create_user("denied", password=password, is_staff=True)
    partner = PartnerLink.objects.create(name="Browser Partner", code="browser-partner", utm_source="browser")
    PartnerLink.objects.create(name="Empty Partner", code="empty-partner", utm_source="empty", is_active=False)
    for n in range(101):
        PartnerLink.objects.create(name=f"Pagination {n:03}", code=f"pagination-{n:03}", utm_source="pagination")
    def at(text):
        return datetime.fromisoformat(text).replace(tzinfo=ZoneInfo("Europe/Kyiv"))
    for uid in (88001, 88002):
        user = TelegramUser.objects.create(tg_user_id=uid, first_name="Synthetic")
        DurableRegistrationAttribution.objects.create(user=user, partner_link=partner, registered_at=at("2025-12-01T12:00:00"))
    for uid, amount, when, kind in (
        (88001, "50.00", "2025-12-31T23:59:59.999999", "legacy"),
        (88001, "100.00", "2026-01-01T00:00:00", "renewal"),
        (88001, "120.00", "2026-01-31T12:00:00", "renewal"),
        (88001, "80.00", "2026-02-01T23:59:59.999999", "renewal"),
        (88002, "200.00", "2026-01-10T12:00:00", "legacy"),
        (88002, "150.00", "2026-01-31T12:00:00", "renewal"),
        (88001, "500.00", "2026-02-02T00:00:00", "renewal"),
    ):
        payment = Payment.objects.create(user_id=uid, amount=amount, currency="UAH", status="paid", kind=kind, paid_at=at(when))
        # Refunds use their confirmation date, independently of the capture date.
        refund_at = {"50.00": "2026-01-15T12:00:00+02:00", "150.00": "2026-02-01T12:00:00+02:00", "120.00": "2026-02-02T00:00:00+02:00"}.get(amount)
        if refund_at:
            payment.status = Payment.Status.REFUNDED
            payment.raw_payload = {"refund_request": {"status": "success", "processed_at": refund_at}}
            payment.save(update_fields=["status", "raw_payload"])
    session = AcquisitionSession.objects.create(first_partner_link=partner, last_partner_link=partner, expires_at=timezone.now()+timedelta(days=1))
    for index, when in enumerate(("2025-12-31T23:59:59.999999", "2026-01-01T00:00:00", "2026-02-01T23:59:59.999999", "2026-02-02T00:00:00")):
        click = PartnerLinkClick.objects.create(partner_link=partner, acquisition_session=session)
        PartnerLinkClick.objects.filter(pk=click.pk).update(clicked_at=at(when))
        for name in ("auth_success", "registration_completed", "onboarding_confirmed", "first_transaction_confirmed", "payment_success"):
            event = FunnelEvent.objects.create(acquisition_session=session, first_partner_link=partner, last_partner_link=partner, event_name=name, idempotency_key=f"{name}-{index}", source="server")
            FunnelEvent.objects.filter(pk=event.pk).update(recorded_at=at(when))
