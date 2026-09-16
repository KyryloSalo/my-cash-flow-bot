"""Real ORM contracts for investor KPIs; synthetic fixtures, no provider calls."""
from datetime import timedelta
from decimal import Decimal
from unittest.mock import patch

from django.template.loader import render_to_string
from django.test import RequestFactory, TestCase, override_settings
from django.utils import timezone

from common.test_helpers import ensure_telegram_user_table
from dashboard import services
from dashboard.tests_investor_metrics import AUGUST, NOW, OCTOBER, SEPTEMBER
from subscriptions.models import Payment, Subscription
from users.models import TelegramUser, UserAdminState


@override_settings(ADMIN_TEST_TELEGRAM_IDS=[7305])
class InvestorDatabaseContractTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.factory = RequestFactory()
        for name in ("_check_redis", "_check_worker", "_check_telegram_api"):
            stub = patch.object(services, name, return_value={"status": "OK", "detail": "synthetic"})
            stub.start()
            self.addCleanup(stub.stop)
        clock = patch.object(services.timezone, "now", return_value=NOW)
        clock.start()
        self.addCleanup(clock.stop)

    def user(self, pk, **changes):
        return TelegramUser.objects.create(**{
            "tg_user_id": pk, "first_name": "Synthetic", "created_at": AUGUST,
            "last_seen_at": NOW, "onboarding_completed": True, **changes,
        })

    def subscription(self, user, **changes):
        return Subscription.objects.create(**{
            "user": user, "status": "active", "source": "payment",
            "started_at": AUGUST, "expires_at": OCTOBER, **changes,
        })

    def payment(self, user, **changes):
        return Payment.objects.create(**{
            "user": user, "status": "paid", "kind": "renewal", "amount": Decimal("499"),
            "currency": "UAH", "paid_at": AUGUST + timedelta(days=4), **changes,
        })

    def context(self, month="08", **params):
        last = "31" if month == "08" else "14"
        return services.build_dashboard_context(self.factory.get("/", {
            "period": "custom", "start_date": f"2026-{month}-01", "end_date": f"2026-{month}-{last}", **params,
        }))

    def test_refund_json_extraction_preserves_capture_period_across_renders(self):
        user = self.user(7301)
        subscription = self.subscription(user)
        payment = self.payment(user, subscription=subscription)
        captured = self.context()["cash_summary"]
        self.assertEqual(captured["gross"], {"UAH": Decimal("499")})
        Payment.objects.filter(pk=payment.pk).update(status="refunded", raw_payload={
            "refund_request": {"status": "success", "processed_at": (SEPTEMBER + timedelta(days=4)).isoformat()},
        })
        august = self.context()
        september = self.context("09")
        self.assertFalse(august["data_quality"]["degraded"])
        self.assertEqual(august["cash_summary"]["gross"], captured["gross"])
        self.assertEqual(august["cash_summary"]["refunds"], {})
        self.assertEqual(september["cash_summary"]["gross"], {})
        self.assertEqual(september["cash_summary"]["refunds"], {"UAH": Decimal("499")})
        self.assertEqual(september["cash_summary"]["net"], {"UAH": Decimal("-499")})
        self.assertEqual(september["cash_summary"]["refund_count"], 1)
        self.assertEqual(self.context()["cash_summary"], august["cash_summary"])
        self.assertIn("499.00 UAH", render_to_string("dashboard/index.html", september))

    def test_sql_scope_and_latest_subscription_match_shared_operator_interface(self):
        users = [self.user(pk) for pk in range(7301, 7306)]
        for user in users:
            UserAdminState.objects.update_or_create(telegram_user=user, defaults={
                "is_test_user": user.pk in (7303, 7304), "subscription_status": "paid",
            })
        paid = self.subscription(users[0])
        self.payment(users[0], subscription=paid)
        older = self.subscription(users[1])
        self.payment(users[1], subscription=older)
        Subscription.objects.filter(pk=older.pk).update(created_at=NOW-timedelta(days=1))
        self.subscription(users[1], status="manual", source="admin")
        for user in users[2:]:
            self.subscription(user, status="lifetime", source="admin", expires_at=None)
        regular = self.context()
        including = self.context(include_test_users=1)
        shared = services.build_current_user_metrics(now=NOW)
        shared_tests = services.build_current_user_metrics(now=NOW, include_test_users=True)
        for context, interface, total, granted in ((regular, shared, 2, 1), (including, shared_tests, 5, 4)):
            self.assertFalse(context["data_quality"]["degraded"])
            self.assertEqual(context["subscription_total"], total)
            self.assertEqual(interface["total_users"], total)
            self.assertEqual(interface["paid_users"], 1)
            self.assertEqual(interface["granted_users"], granted)
            distribution = {item["tone"]: item["value"] for item in context["subscription_distribution"]}
            self.assertEqual(distribution, {"free": 0, "trial": 0, "paid": 1, "granted": granted})
            self.assertEqual(sum(distribution.values()), total)

    def test_ninety_binds_do_not_inflate_recurring_payments(self):
        users = [self.user(pk, created_at=SEPTEMBER) for pk in range(7401, 7492)]
        Payment.objects.bulk_create([
            Payment(user=user, status="paid", kind="bind", amount=Decimal("1"), currency="UAH", paid_at=NOW)
            for user in users[:-1]
        ])
        subscription = self.subscription(users[-1])
        self.payment(users[-1], subscription=subscription, paid_at=NOW)
        context = self.context("09")
        summary = context["cash_summary"]
        self.assertFalse(context["data_quality"]["degraded"])
        self.assertEqual(summary["recurring_payers"], 1)
        self.assertEqual(summary["bind_payers"], 90)
        self.assertEqual(summary["gross"], {"UAH": Decimal("589")})
        self.assertEqual(summary["recurring_cash"], {"UAH": Decimal("499")})
        self.assertEqual([row["value"] for row in context["funnel_stages"]], [91, None, 90, 1])
        self.assertEqual(services.build_current_user_metrics(now=NOW)["paid_users"], 1)

    def test_last_seen_and_current_onboarding_cannot_make_historical_growth(self):
        user = self.user(7301, last_seen_at=AUGUST+timedelta(days=4), onboarding_completed=False)
        before = self.context()
        TelegramUser.objects.filter(pk=user.pk).update(last_seen_at=NOW, onboarding_completed=True)
        after = self.context()
        for context in (before, after):
            cards = {card["label"]: card for card in context["kpi_cards"]}
            for label in ("Історична активність", "Історична конверсія онбордингу"):
                self.assertIsNone(cards[label]["value"])
                self.assertEqual(cards[label]["delta"], "—")
                self.assertTrue(cards[label]["reason"])
        old_cards = {card["label"]: card for card in before["kpi_cards"]}
        new_cards = {card["label"]: card for card in after["kpi_cards"]}
        self.assertEqual(old_cards["Останній візит у періоді"]["value"], 1)
        self.assertEqual(new_cards["Останній візит у періоді"]["value"], 0)
        self.assertEqual(new_cards["Онбординг зараз у когорті"]["value"], 1)
