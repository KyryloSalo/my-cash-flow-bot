from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.template.loader import render_to_string
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.utils import timezone
from django.utils.translation import override

from common.test_helpers import ensure_runtime_finance_tables, ensure_telegram_user_table
from dashboard.services import (
    _build_user_segment_series,
    _resolve_period,
    _subscription_segment,
    build_dashboard_context,
)
from subscriptions.models import Payment, Subscription, TrialRecoveryCampaign, TrialRecoveryRecipient
from users.models import TelegramUser, UserAdminState


class DashboardPeriodHelpersTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def test_custom_period_is_inclusive_and_has_equal_comparison_window(self):
        request = self.factory.get(
            "/admin/",
            {"period": "custom", "start_date": "2026-08-01", "end_date": "2026-08-10"},
        )

        period = _resolve_period(request, timezone.now())

        self.assertEqual(period["key"], "custom")
        self.assertEqual(period["start_date"], date(2026, 8, 1))
        self.assertEqual(period["end_date"], date(2026, 8, 10))
        self.assertEqual(period["days"], 10)
        self.assertEqual((period["previous_end_at"] - period["previous_start_at"]).days, 10)

    def test_invalid_custom_period_falls_back_to_30_days(self):
        request = self.factory.get(
            "/admin/",
            {"period": "custom", "start_date": "2026-08-20", "end_date": "2026-08-10"},
        )

        period = _resolve_period(request, timezone.now())

        self.assertEqual(period["key"], "30d")
        self.assertEqual(period["days"], 30)
        self.assertTrue(period["error"])

    def test_long_series_is_grouped_into_readable_number_of_buckets(self):
        series = _build_user_segment_series(
            date(2026, 1, 1),
            date(2026, 3, 31),
            {
                "free": {date(2026, 1, 1): 3, date(2026, 3, 31): 2},
                "trial": {date(2026, 1, 1): 1},
                "paid": {date(2026, 3, 31): 1},
            },
        )

        self.assertLessEqual(len(series), 31)
        self.assertEqual(sum(item["free"] for item in series), 5)
        self.assertEqual(sum(item["trial"] for item in series), 1)
        self.assertEqual(sum(item["paid"] for item in series), 1)

    def test_subscription_segments_require_validity_and_payment_evidence(self):
        now = timezone.now()
        self.assertEqual(_subscription_segment(None), "free")
        self.assertEqual(_subscription_segment(Subscription(status=Subscription.Status.EXPIRED)), "free")
        self.assertEqual(_subscription_segment(Subscription(status=Subscription.Status.TRIAL)), "free")
        trial = Subscription(status=Subscription.Status.TRIAL, started_at=now, expires_at=now + timedelta(days=30))
        self.assertEqual(_subscription_segment(trial, now=now), "trial")
        paid = Subscription(pk=1, user_id=1, status=Subscription.Status.ACTIVE, source=Subscription.Source.PAYMENT,
                            started_at=now, expires_at=now + timedelta(days=30))
        capture = Payment(subscription_id=1, user_id=1, status=Payment.Status.PAID, kind=Payment.Kind.RENEWAL,
                          paid_at=now, amount=Decimal("499"))
        self.assertEqual(_subscription_segment(paid, now=now), "granted")
        self.assertEqual(_subscription_segment(paid, payments=[capture], now=now), "paid")

    def test_dashboard_template_renders_fractional_css_values_with_decimal_points(self):
        context = {
            "period": {"label": "1–10 сер", "previous_label": "22–31 лип", "preset_links": [], "error": ""},
            "quick_actions": [],
            "user_search_url": "/admin/users/telegramuser/",
            "include_test_users": False,
            "custom_period_url": "/admin/",
            "data_scope_label": "Тільки реальні користувачі",
            "toggle_include_test_url": "/admin/?include_test_users=1",
            "attention_items": [],
            "kpi_cards": [],
            "daily_kpis": [],
            "finance_kpis": [],
            "product_kpis": [],
            "user_segment_series": [
                {
                    "title": "Тест",
                    "label": "1 сер",
                    "free": 1,
                    "trial": 1,
                    "paid": 1,
                    "free_percent": 12.5,
                    "trial_percent": 29.5,
                    "paid_percent": 7.25,
                }
            ],
            "funnel_stages": [{"label": "Trial", "value": 1, "percent_display": "29,5%", "width": 29.5}],
            "subscription_donut": "#1e293b",
            "subscription_total": 0,
            "subscription_distribution": [],
            "health_items": [],
            "recent_users": [],
            "latest_payments": [],
            "data_quality": {"degraded": False, "retrieved_at": timezone.now(), "reason": ""},
        }

        with override("uk"):
            html = render_to_string("dashboard/index.html", context)

        self.assertIn("height: 12.5%", html)
        self.assertIn("height: 29.5%", html)
        self.assertIn("height: 7.25%", html)
        self.assertIn("width: 29.5%", html)
        self.assertNotIn("height: 12,5%", html)


class DashboardAnalyticsTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()
        ensure_runtime_finance_tables()

    def setUp(self):
        for check in ("_check_redis", "_check_worker", "_check_telegram_api"):
            stub = patch(f"dashboard.services.{check}", return_value={"status": "OK", "detail": "synthetic"})
            stub.start()
            self.addCleanup(stub.stop)
        self.factory = RequestFactory()
        self.real_user = TelegramUser.objects.create(
            tg_user_id=7001,
            first_name="Real",
            username="real_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.test_user = TelegramUser.objects.create(
            tg_user_id=7002,
            first_name="Test",
            username="test_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.test_user.tg_user_id,
            defaults={"is_test_user": True},
        )

        now = timezone.now()
        Subscription.objects.create(
            user_id=self.real_user.tg_user_id,
            status=Subscription.Status.TRIAL,
            plan="trial_30",
            source=Subscription.Source.SYSTEM,
            started_at=now - timedelta(days=10),
            expires_at=now + timedelta(days=20),
        )
        Subscription.objects.create(
            user_id=self.test_user.tg_user_id,
            status=Subscription.Status.TRIAL,
            plan="trial_90",
            source=Subscription.Source.PROMO,
            started_at=now - timedelta(days=5),
            expires_at=now + timedelta(days=85),
        )
        Payment.objects.create(
            user_id=self.real_user.tg_user_id,
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            paid_at=now,
        )
        Payment.objects.create(
            user_id=self.test_user.tg_user_id,
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            paid_at=now,
        )

    def _card(self, cards, label):
        return next(card for card in cards if card["label"] == label)

    def test_dashboard_excludes_test_users_by_default(self):
        request = self.factory.get("/admin/")
        context = build_dashboard_context(request)

        self.assertFalse(context["include_test_users"])
        self.assertEqual(self._card(context["kpi_cards"], "Нові користувачі")["value"], 1)
        self.assertIsNone(self._card(context["kpi_cards"], "Історична конверсія онбордингу")["value"])
        self.assertEqual(self._card(context["kpi_cards"], "Онбординг зараз у когорті")["value"], 1)
        self.assertEqual(self._card(context["kpi_cards"], "Оплати (включно з поверненими)")["value"], 1)
        self.assertEqual(context["subscription_total"], 1)
        self.assertEqual(sum(item["trial"] for item in context["user_segment_series"]), 1)
        self.assertIn("include_test_users=1", context["toggle_include_test_url"])
        self.assertEqual(
            [len(context["daily_kpis"]), len(context["finance_kpis"]), len(context["product_kpis"])],
            [4, 5, 4],
        )
        grouped_labels = [
            card["label"]
            for group in (context["daily_kpis"], context["finance_kpis"], context["product_kpis"])
            for card in group
        ]
        self.assertCountEqual(grouped_labels, [card["label"] for card in context["kpi_cards"]])
        self.assertEqual(context["user_search_url"], "/users/telegramuser/")

    def test_dashboard_can_include_test_users(self):
        request = self.factory.get("/admin/?include_test_users=1")
        context = build_dashboard_context(request)

        self.assertTrue(context["include_test_users"])
        self.assertEqual(self._card(context["kpi_cards"], "Нові користувачі")["value"], 2)
        self.assertEqual(self._card(context["kpi_cards"], "Оплати (включно з поверненими)")["value"], 2)
        self.assertEqual(context["subscription_total"], 2)
        self.assertNotIn("include_test_users", context["toggle_exclude_test_url"])

    def test_dashboard_recovered_users_kpi_requires_message_before_conversion(self):
        now = timezone.now()
        campaign = TrialRecoveryCampaign.objects.create(name="Dashboard recovery")
        for user in (self.real_user, self.test_user):
            TrialRecoveryRecipient.objects.create(
                campaign=campaign,
                user=user,
                status=TrialRecoveryRecipient.Status.CONVERTED,
                sent_count=1,
                first_sent_at=now - timedelta(hours=2),
                converted_at=now - timedelta(hours=1),
            )
        early_user = TelegramUser.objects.create(
            tg_user_id=7005,
            first_name="Early",
            username="early_conversion",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=now,
            last_seen_at=now,
        )
        TrialRecoveryRecipient.objects.create(
            campaign=campaign,
            user=early_user,
            status=TrialRecoveryRecipient.Status.CONVERTED,
            converted_at=now,
        )

        regular_context = build_dashboard_context(self.factory.get("/admin/"))
        including_tests_context = build_dashboard_context(self.factory.get("/admin/?include_test_users=1"))

        regular_card = self._card(regular_context["kpi_cards"], "Відновлені користувачі")
        including_tests_card = self._card(including_tests_context["kpi_cards"], "Відновлені користувачі")
        self.assertEqual(regular_card["value"], 1)
        self.assertEqual(including_tests_card["value"], 2)
        self.assertIn("після recovery-повідомлення", regular_card["detail"])

    def test_dashboard_exposes_free_trial_and_paid_as_exclusive_segments(self):
        now = timezone.now()
        free_user = TelegramUser.objects.create(
            tg_user_id=7003,
            first_name="Free",
            username="free_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=now,
            last_seen_at=now,
        )
        paid_user = TelegramUser.objects.create(
            tg_user_id=7004,
            first_name="Paid",
            username="paid_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=now,
            last_seen_at=now,
        )
        subscription = Subscription.objects.create(
            user_id=paid_user.tg_user_id,
            status=Subscription.Status.ACTIVE,
            plan="solo",
            source=Subscription.Source.PAYMENT,
            started_at=now - timedelta(days=30),
            expires_at=now + timedelta(days=30),
        )
        Payment.objects.create(
            user=paid_user, subscription=subscription, status=Payment.Status.PAID,
            kind=Payment.Kind.RENEWAL, amount=Decimal("499"), currency="UAH", paid_at=now,
        )

        context = build_dashboard_context(self.factory.get("/admin/"))
        distribution = {item["label"]: item["value"] for item in context["subscription_distribution"]}

        self.assertEqual(distribution, {"Без підтвердженої чинної підписки": 1,
                                      "Чинний trial (оплата окремо)": 1,
                                      "Чинна підписка з оплатою renewal/retry": 1,
                                      "Наданий / непідтверджений оплатою доступ": 0})
        self.assertEqual(sum(item["free"] for item in context["user_segment_series"]), 1)
        self.assertEqual(sum(item["trial"] for item in context["user_segment_series"]), 1)
        self.assertEqual(sum(item["paid"] for item in context["user_segment_series"]), 1)
        self.assertEqual(sum(distribution.values()), context["subscription_total"])
