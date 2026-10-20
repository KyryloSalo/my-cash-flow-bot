from datetime import date, datetime
from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from dashboard.partner_cabinet import partner_cabinet_metrics
from miniapp.models import DurableRegistrationAttribution, PartnerLink
from subscriptions.models import Payment
from users.models import TelegramUser


class PartnerCabinetMetricsTests(TestCase):
    def test_counts_first_ever_and_repeat_payments_in_uaH_without_filtering_link_creation(self):
        link = PartnerLink.objects.create(name="Later", code="later", utm_source="later")
        user = TelegramUser.objects.create(tg_user_id=991, first_name="A")
        registered = timezone.make_aware(datetime(2025, 12, 1, 10))
        DurableRegistrationAttribution.objects.create(user=user, partner_link=link, registered_at=registered)
        self.payment(user, "10.00", datetime(2025, 12, 1, 11), "first")
        self.payment(user, "7.00", datetime(2026, 1, 2, 11), "repeat")
        self.payment(user, "9.00", datetime(2026, 1, 3, 11), "usd", currency="USD")

        result = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))

        row = result[link.pk]
        self.assertEqual(row["successful_count"], 1)
        self.assertEqual(row["successful_amount"], Decimal("7.00"))
        self.assertEqual(row["first_count"], 0)
        self.assertEqual(row["repeat_count"], 1)
        self.assertEqual(row["repeat_amount"], Decimal("7.00"))
        self.assertEqual(row["refund_amount"], Decimal("0.00"))
        self.assertTrue(result["warnings"])

    def test_refund_is_counted_in_refund_period_not_capture_period(self):
        link = PartnerLink.objects.create(name="Cross period", code="cross-period", utm_source="cross-period")
        user = TelegramUser.objects.create(tg_user_id=993, first_name="C")
        DurableRegistrationAttribution.objects.create(
            user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2025, 12, 1, 10)),
        )
        payment = self.payment(user, "12.00", datetime(2025, 12, 20, 11), "cross-period")
        payment.status = Payment.Status.REFUNDED
        payment.raw_payload = {"refund_request": {"status": "success", "processed_at": "2026-01-05T12:00:00+02:00"}}
        payment.save(update_fields=["status", "raw_payload"])

        row = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))[link.pk]

        self.assertEqual(row["successful_count"], 0)
        self.assertEqual(row["refund_amount"], Decimal("12.00"))
        self.assertEqual(row["groups"]["day"][0]["refund_amount"], Decimal("12.00"))

    def test_first_ever_uses_all_successful_history_and_same_timestamp_pk_order(self):
        link = PartnerLink.objects.create(name="History", code="history", utm_source="history")
        user = TelegramUser.objects.create(tg_user_id=994, first_name="D")
        DurableRegistrationAttribution.objects.create(
            user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2026, 1, 1, 10)),
        )
        same_time = datetime(2026, 1, 10, 11)
        self.payment(user, "5.00", datetime(2025, 12, 20, 11), "history-usd", currency="USD")
        earlier_pk = self.payment(user, "6.00", same_time, "same-time-first")
        later = self.payment(user, "7.00", same_time, "same-time-repeat")

        row = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))[link.pk]

        self.assertLess(earlier_pk.pk, later.pk)
        self.assertEqual(row["first_count"], 0)
        self.assertEqual(row["repeat_count"], 2)

    def test_group_buckets_expose_successful_aliases(self):
        link = PartnerLink.objects.create(name="Aliases", code="aliases", utm_source="aliases")
        user = TelegramUser.objects.create(tg_user_id=995, first_name="E")
        DurableRegistrationAttribution.objects.create(
            user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2025, 12, 1, 10)),
        )
        self.payment(user, "8.00", datetime(2026, 1, 10, 11), "aliases")

        row = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))[link.pk]

        for bucket in row["groups"]["day"] + row["groups"]["week"] + row["groups"]["month"]:
            self.assertEqual(bucket["successful_count"], bucket["count"])
            self.assertEqual(bucket["successful_amount"], bucket["amount"])

    def test_groups_refunds_unknown_dates_and_empty_metrics(self):
        link = PartnerLink.objects.create(name="Grouped", code="grouped", utm_source="grouped")
        user = TelegramUser.objects.create(tg_user_id=992, first_name="B")
        DurableRegistrationAttribution.objects.create(
            user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2025, 12, 1, 10)),
        )
        payment = self.payment(user, "12.00", datetime(2026, 1, 10, 11), "refund")
        payment.status = Payment.Status.REFUNDED
        payment.raw_payload = {"refund_request": {"status": "success", "processed_at": "2026-01-15T12:00:00+02:00"}}
        payment.save(update_fields=["status", "raw_payload"])
        unknown = self.payment(user, "3.00", datetime(2026, 1, 11, 11), "unknown")
        unknown.status = Payment.Status.REFUNDED
        unknown.raw_payload = {"refund_request": {"status": "success"}}
        unknown.save(update_fields=["status", "raw_payload"])
        result = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31), partner_ids=[link.pk])
        row = result[link.pk]
        self.assertEqual(row["successful_count"], 2)
        self.assertEqual(row["refund_amount"], Decimal("12.00"))
        self.assertEqual(row["unknown_refunds"], 1)
        self.assertEqual([bucket["date"] for bucket in row["groups"]["day"]], ["2026-01-10", "2026-01-11", "2026-01-15"])
        self.assertEqual(row["groups"]["day"][0]["amount"], Decimal("12.00"))

    def test_empty_metrics_has_stable_zero_shape(self):
        from dashboard.partner_cabinet import empty_metrics
        row = empty_metrics()
        self.assertEqual(row["successful_count"], 0)
        self.assertEqual(row["refund_amount"], Decimal("0.00"))
        self.assertEqual(set(row["groups"]), {"day", "week", "month"})

    def test_payment_before_registration_is_not_partner_revenue(self):
        link = PartnerLink.objects.create(name="Attribution", code="attribution", utm_source="attribution")
        user = TelegramUser.objects.create(tg_user_id=996)
        DurableRegistrationAttribution.objects.create(user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2026, 1, 15, 10)))
        self.payment(user, "5.00", datetime(2026, 1, 10, 11), "before-registration")
        self.payment(user, "7.00", datetime(2026, 1, 20, 11), "after-registration")
        row = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))[link.pk]
        self.assertEqual(row["successful_count"], 1)
        self.assertEqual(row["successful_amount"], Decimal("7.00"))
        self.assertEqual(row["first_count"], 0)
        self.assertEqual(row["repeat_count"], 1)

    def test_unknown_historical_refund_cannot_silently_disappear(self):
        link = PartnerLink.objects.create(name="Unknown", code="unknown", utm_source="unknown")
        user = TelegramUser.objects.create(tg_user_id=997)
        DurableRegistrationAttribution.objects.create(user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2025, 12, 1, 10)))
        payment = self.payment(user, "5.00", datetime(2025, 12, 10, 11), "undated-history")
        payment.status = Payment.Status.REFUNDED
        payment.raw_payload = {"refund_request": {"status": "success"}}
        payment.save(update_fields=["status", "raw_payload"])
        result = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))
        self.assertTrue(result["warnings"])
        self.assertEqual(result[link.pk]["unknown_refunds"], 1)
        self.assertEqual(result[link.pk]["successful_count"], 0)

    def test_same_timestamp_without_earlier_history_has_one_first(self):
        link = PartnerLink.objects.create(name="Ties", code="ties", utm_source="ties")
        user = TelegramUser.objects.create(tg_user_id=998)
        DurableRegistrationAttribution.objects.create(user=user, partner_link=link,
            registered_at=timezone.make_aware(datetime(2025, 12, 1, 10)))
        self.payment(user, "5.00", datetime(2026, 1, 10, 11), "tie-1")
        self.payment(user, "7.00", datetime(2026, 1, 10, 11), "tie-2")
        row = partner_cabinet_metrics(date(2026, 1, 1), date(2026, 1, 31))[link.pk]
        self.assertEqual(row["first_count"], 1)
        self.assertEqual(row["first_amount"], Decimal("5.00"))
        self.assertEqual(row["repeat_count"], 1)
        self.assertEqual(row["repeat_amount"], Decimal("7.00"))

    def payment(self, user, amount, when, suffix, currency="UAH"):
        return Payment.objects.create(
            user=user, amount=Decimal(amount), currency=currency,
            paid_at=timezone.make_aware(when), status=Payment.Status.PAID,
            kind=Payment.Kind.RENEWAL, provider_payment_id=f"cab-{suffix}",
        )
