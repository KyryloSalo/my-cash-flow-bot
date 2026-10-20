from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from dashboard.partner_payments import partner_payment_report
from miniapp.funnel import record_request_server_event
from miniapp.models import DurableRegistrationAttribution, PartnerLink
from subscriptions.models import Payment
from users.models import TelegramUser


class PartnerPaymentReportContractTests(SimpleTestCase):
    def test_rejects_inverted_period(self):
        with self.assertRaisesMessage(ValueError, "Дата завершення"):
            partner_payment_report(date(2026, 2, 2), date(2026, 2, 1))

    def test_rejects_unknown_grouping(self):
        with self.assertRaisesMessage(ValueError, "Невідома група"):
            partner_payment_report(date(2026, 1, 1), date(2026, 1, 31), "quarter")


class PartnerPaymentReportIntegrationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.link = PartnerLink.objects.create(name="Partner A", code="partner-a", utm_source="partner")
        cls.other = PartnerLink.objects.create(name="Partner B", code="partner-b", utm_source="partner2")
        cls.user = TelegramUser.objects.create(tg_user_id=101, first_name="A")
        cls.user2 = TelegramUser.objects.create(tg_user_id=102, first_name="B")
        registered = timezone.make_aware(datetime(2025, 12, 31, 12))
        DurableRegistrationAttribution.objects.create(user=cls.user, partner_link=cls.link, registered_at=registered)
        DurableRegistrationAttribution.objects.create(user=cls.user2, partner_link=cls.other, registered_at=registered)

    def payment(self, user, amount, currency, paid_at, *, status=Payment.Status.PAID, canary_owned=False, suffix=None):
        return Payment.objects.create(user=user, amount=amount, currency=currency, paid_at=paid_at, status=status, kind=Payment.Kind.RENEWAL, canary_owned=canary_owned, provider_payment_id=suffix or f"p-{user.tg_user_id}-{amount}-{currency}-{paid_at.day}")

    def test_inclusive_period_grouping_currency_and_exclusions(self):
        jan1 = timezone.make_aware(datetime(2026, 1, 1, 0, 0))
        feb1_end = timezone.make_aware(datetime(2026, 2, 1, 23, 59, 59, 999999))
        self.payment(self.user, Decimal("10.00"), "USD", jan1, suffix="jan1")
        self.payment(self.user, Decimal("5.50"), "USD", feb1_end, suffix="feb1")
        self.payment(self.user, Decimal("7.00"), "EUR", jan1, suffix="eur")
        self.payment(self.user, Decimal("99.00"), "USD", jan1, status=Payment.Status.REFUNDED, suffix="refund")
        self.payment(self.user, Decimal("99.00"), "USD", jan1, canary_owned=True, suffix="canary")
        self.payment(self.user, Decimal("0.00"), "USD", jan1, suffix="zero")
        report = partner_payment_report(date(2026, 1, 1), date(2026, 2, 1), "month")
        self.assertEqual(report["totals"][(date(2026, 1, 1), "USD")]["count"], 1)
        self.assertEqual(report["totals"][(date(2026, 1, 1), "USD")]["amount"], Decimal("10.00"))
        self.assertEqual(report["totals"][(date(2026, 2, 1), "USD")]["amount"], Decimal("5.50"))
        self.assertEqual(report["totals"][(date(2026, 1, 1), "EUR")]["amount"], Decimal("7.00"))
        self.assertEqual(report["by_partner"][self.link.pk]["count"], 3)

    def test_selected_partner_and_week_boundaries(self):
        self.payment(self.user, Decimal("3.00"), "USD", timezone.make_aware(datetime(2026, 1, 4, 23, 59)), suffix="sun")
        self.payment(self.user, Decimal("4.00"), "USD", timezone.make_aware(datetime(2026, 1, 5, 0, 0)), suffix="mon")
        self.payment(self.user2, Decimal("8.00"), "USD", timezone.make_aware(datetime(2026, 1, 5, 1)), suffix="other")
        report = partner_payment_report(date(2026, 1, 1), date(2026, 1, 31), "week", self.link.pk)
        self.assertEqual(report["totals"][(date(2025, 12, 29), "USD")]["amount"], Decimal("3.00"))
        self.assertEqual(report["totals"][(date(2026, 1, 5), "USD")]["amount"], Decimal("4.00"))

    @override_settings(ADMIN_TEST_TELEGRAM_IDS=[101])
    def test_internal_users_are_excluded(self):
        self.payment(self.user, Decimal("10"), "USD", timezone.make_aware(datetime(2026, 1, 2)), suffix="internal")
        report = partner_payment_report(date(2026, 1, 1), date(2026, 1, 31))
        self.assertEqual(report["payments"], [])

    def test_old_report_redirects_to_existing_partner_links(self):
        user = get_user_model().objects.create_superuser(username="report-root", password="x")
        self.client.force_login(user)
        self.payment(self.user, Decimal("12.34"), "USD", timezone.make_aware(datetime(2026, 1, 2)), suffix="render-usd")
        response = self.client.get("/partner-payments/", {"start": "2026-01-01", "end": "2026-02-01"})
        self.assertRedirects(response, "/miniapp/partnerlink/?period_start=2026-01-01&period_end=2026-02-01", fetch_redirect_response=False)


class PartnerPaymentRegistrationDurabilityTests(TestCase):
    def test_registration_without_session_keeps_organic_snapshot(self):
        user = TelegramUser.objects.create(tg_user_id=555)
        request = self.client.get("/").wsgi_request
        with patch("miniapp.funnel.link_acquisition_session", return_value=None):
            self.assertIsNone(record_request_server_event(request, user=user, event_name="registration_completed", idempotency_key="registration:555"))
        row = DurableRegistrationAttribution.objects.get(user=user)
        self.assertIsNone(row.partner_link_id)
        self.assertIsNone(row.source_event_id)

    def test_later_partner_click_does_not_steal_existing_organic_snapshot(self):
        user = TelegramUser.objects.create(tg_user_id=556)
        row = DurableRegistrationAttribution.objects.create(user=user, partner_link=None, registered_at=timezone.now())
        link = PartnerLink.objects.create(name="Later", code="later-link", utm_source="later")
        DurableRegistrationAttribution.objects.get_or_create(user=user, defaults={"partner_link": link, "registered_at": timezone.now()})
        row.refresh_from_db()
        self.assertIsNone(row.partner_link_id)


class PartnerPaymentPermissionTests(TestCase):
    def test_view_requires_partner_links_access(self):
        user = get_user_model().objects.create_user(username="staff", password="x", is_staff=True)
        self.client.force_login(user)
        self.assertEqual(self.client.get("/partner-payments/").status_code, 403)

    def test_redirect_keeps_empty_partner_in_integrated_table(self):
        from django.contrib.auth.models import Permission

        user = get_user_model().objects.create_user(username="finance", password="x", is_staff=True)
        user.user_permissions.add(
            Permission.objects.get(content_type__app_label="miniapp", codename="view_partnerlink"),
            Permission.objects.get(content_type__app_label="subscriptions", codename="view_payment"),
        )
        link = PartnerLink.objects.create(name="Empty", code="empty", utm_source="empty")
        self.client.force_login(user)
        response = self.client.get("/partner-payments/", {"start": "2026-01-01", "end": "2026-02-01", "partner": link.pk}, follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f'data-partner-id="{link.pk}"')
        self.assertContains(response, "Empty")

    def test_invalid_dates_do_not_display_another_periods_totals(self):
        user = get_user_model().objects.create_superuser(username="root", password="x")
        self.client.force_login(user)
        response = self.client.get("/partner-payments/", {"start": "bad", "end": "2026-02-01"}, follow=True)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Вкажіть коректні дати")
        self.assertTrue(response.context_data["partner_period"]["error"])
