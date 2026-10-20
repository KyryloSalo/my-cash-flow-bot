"""Synthetic fixture for the disposable partner-payment browser acceptance."""
from datetime import datetime

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.utils import timezone

from miniapp.models import DurableRegistrationAttribution, PartnerLink
from subscriptions.models import Payment
from users.models import TelegramUser


def seed():
    users = get_user_model()
    admin = users.objects.create_superuser("accept", "accept@example.invalid", "accept-pass-2026")
    viewer = users.objects.create_user("limited", password="accept-pass-2026", is_staff=True)
    viewer.user_permissions.add(Permission.objects.get(content_type__app_label="miniapp", codename="view_partnerlink"))
    allowed = users.objects.create_user("finance", password="accept-pass-2026", is_staff=True)
    allowed.user_permissions.add(*Permission.objects.filter(content_type__app_label="miniapp", codename="view_partnerlink"), *Permission.objects.filter(content_type__app_label="subscriptions", codename="view_payment"))
    partner = PartnerLink.objects.create(name="Browser Partner", code="browser-partner", utm_source="browser")
    other = PartnerLink.objects.create(name="Other Partner", code="other-partner", utm_source="other")
    PartnerLink.objects.create(name="Empty Partner", code="empty-partner", utm_source="empty")
    for user_id, link in [(81001, partner), (81002, other)]:
        user = TelegramUser.objects.create(tg_user_id=user_id, first_name="Synthetic referral")
        DurableRegistrationAttribution.objects.create(user=user, partner_link=link, registered_at=timezone.make_aware(datetime(2025, 12, 15, 12)))
    def payment(user_id, amount, currency, at, label, status="paid"):
        Payment.objects.create(user_id=user_id, amount=amount, currency=currency, status=status, kind="renewal", paid_at=timezone.make_aware(datetime.fromisoformat(at)), provider_payment_id=label)
    payment(81001, "10.00", "USD", "2026-01-01T00:00:00", "accept-start")
    payment(81001, "12.34", "USD", "2026-01-31T23:59:59", "accept-jan")
    payment(81001, "5.66", "USD", "2026-02-01T23:59:59.999999", "accept-end")
    payment(81001, "7.00", "EUR", "2026-01-15T12:00:00", "accept-eur")
    payment(81001, "8.00", "USD", "2026-02-02T00:00:00", "accept-out")
    payment(81001, "9.00", "USD", "2025-12-31T23:59:59.999999", "accept-before")
    payment(81001, "99.00", "USD", "2026-01-15T12:00:00", "accept-failed", "failed")
    payment(81002, "111.00", "USD", "2026-01-15T12:00:00", "accept-other")
