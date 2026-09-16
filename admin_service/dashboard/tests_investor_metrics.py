"""Synthetic investor-metric regressions; no database or network access."""
from __future__ import annotations

import ast
import copy
from datetime import datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import ExitStack

from django.test import RequestFactory, SimpleTestCase, override_settings
from django.template.loader import render_to_string
from django.db.models.query import QuerySet

from dashboard import services
from subscriptions.models import Payment, Subscription

NOW = datetime(2026, 9, 14, 12, tzinfo=dt_timezone.utc)
AUGUST = datetime(2026, 8, 1, tzinfo=dt_timezone.utc)
SEPTEMBER = datetime(2026, 9, 1, tzinfo=dt_timezone.utc)
OCTOBER = datetime(2026, 10, 1, tzinfo=dt_timezone.utc)


def dashboard_assignments(names, env):
    """Exercise the exact audited query seam without booting persistence."""
    tree = ast.parse(Path(services.__file__).read_text(encoding="utf-8-sig"))
    body = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "build_dashboard_context").body
    nodes = [n for n in body if isinstance(n, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id in names for t in n.targets
    )]
    assert len(nodes) == len(names), (names, len(nodes))
    exec(compile(ast.Module(body=nodes, type_ignores=[]), services.__file__, "exec"), env)
    return env


class LastSeenRows:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, **lookups):
        return LastSeenRows([row for row in self.rows if all(
            (row >= value if key.endswith("__gte") else row < value)
            for key, value in lookups.items()
        )])

    def count(self):
        return len(self.rows)


class InvestorActivityTruthTests(SimpleTestCase):
    def test_returning_users_cannot_be_reported_as_300_percent_growth(self):
        # Audit: 100 active in August, 80 returned in September. Last-seen
        # loses those 80 August facts; true -20% cannot be computed from it.
        rows = LastSeenRows([SEPTEMBER + timedelta(days=4)] * 80 + [AUGUST + timedelta(days=4)] * 20)
        env = dashboard_assignments(
            {"period_active_user_count", "previous_period_active_user_count"},
            {"users_qs": rows, "safe": lambda fn, default: fn(), "period": {
                "start_at": SEPTEMBER, "end_at": OCTOBER,
                "previous_start_at": AUGUST, "previous_end_at": SEPTEMBER,
            }},
        )
        self.assertIsNone(env["period_active_user_count"])
        self.assertIsNone(env["previous_period_active_user_count"])
        self.assertEqual(services._change(80, 100)["delta"], "-20%")
        self.assertEqual(services._change(None, None)["delta"], "—")


class InvestorEntitlementTruthTests(SimpleTestCase):
    def subscription(self, status="active", **changes):
        values = dict(pk=41, user_id=1, status=status, source="payment",
                      started_at=AUGUST, expires_at=OCTOBER)
        return Subscription(**{**values, **changes})

    def payment(self, **changes):
        values = dict(subscription_id=41, user_id=1, status="paid", kind="renewal",
                      paid_at=SEPTEMBER, amount=Decimal("499"), currency="UAH")
        return Payment(**{**values, **changes})

    def test_zero_payment_grants_never_count_as_paid(self):
        with patch.object(services.timezone, "now", return_value=NOW):
            manual = [self.subscription("manual") for _ in range(100)]
            self.assertEqual(sum(services._subscription_segment(s) == "paid" for s in manual), 0)
            self.assertEqual(services._subscription_segment(self.subscription("lifetime", expires_at=None)), "granted")
            self.assertEqual(services._subscription_segment(self.subscription("trial", expires_at=NOW-timedelta(days=1))), "free")

    def test_paid_segment_requires_valid_subscription_and_linked_capture(self):
        classify = services._subscription_segment
        subscription = self.subscription()
        payment = self.payment()
        self.assertEqual(classify(subscription, payments=[payment], now=NOW), "paid")
        for changes in ({"kind": "bind"}, {"status": "manual_confirmed"}, {"status": "refunded"},
                        {"amount": Decimal("0")}, {"subscription_id": 999}, {"user_id": 999},
                        {"paid_at": None}, {"paid_at": NOW+timedelta(days=1)}):
            with self.subTest(changes=changes):
                self.assertEqual(classify(subscription, payments=[self.payment(**changes)], now=NOW), "granted")
        self.assertEqual(classify(self.subscription("cancelled"), payments=[payment], now=NOW), "paid")
        self.assertEqual(classify(self.subscription(expires_at=NOW, grace_expires_at=OCTOBER), payments=[payment], now=NOW), "free")
        self.assertEqual(classify(self.subscription(source="admin"), payments=[payment], now=NOW), "granted")
        self.assertEqual(classify(self.subscription("trial"), payments=[payment], now=NOW), "trial")
        self.assertEqual(classify(None, payments=[payment], now=NOW), "free")


class InvestorCashTruthTests(SimpleTestCase):
    def payment(self, **changes):
        values = dict(pk=1, user_id=1, status="refunded", kind="renewal", provider="monobank",
                      paid_at=AUGUST + timedelta(days=4), amount=Decimal("499"), currency="UAH",
                      refund_status="success", refund_processed_at=(SEPTEMBER+timedelta(days=4)).isoformat())
        return SimpleNamespace(**{**values, **changes})

    def test_august_capture_survives_september_refund(self):
        summary = getattr(services, "_cash_summary", None)
        self.assertTrue(callable(summary), "Need capture/refund event-time cash summary, not status-only revenue")
        payment = self.payment()
        august = summary([payment], AUGUST, SEPTEMBER)
        september = summary([payment], SEPTEMBER, OCTOBER)
        lifetime = summary([payment], AUGUST, OCTOBER)
        self.assertEqual(august["gross"], {"UAH": Decimal("499")})
        self.assertEqual(august["refunds"], {})
        self.assertEqual(september["gross"], {})
        self.assertEqual(september["refunds"], {"UAH": Decimal("499")})
        self.assertEqual(september["net"], {"UAH": Decimal("-499")})
        self.assertEqual(lifetime["net"], {"UAH": Decimal("0")})
        self.assertEqual(summary([self.payment(status="paid", refund_status="", refund_processed_at=None)], AUGUST, SEPTEMBER)["gross"], august["gross"])

    def test_undated_refund_or_capture_is_unavailable_not_zero(self):
        for processed in (None, "broken", "2026-09-05T12:00:00"):
            with self.subTest(processed=processed):
                result = services._cash_summary([self.payment(refund_processed_at=processed)], SEPTEMBER, OCTOBER)
                self.assertIsNone(result["refunds"])
                self.assertIsNone(result["net"])
                self.assertTrue(result["reason"])
        result = services._cash_summary([self.payment(paid_at=None)], AUGUST, SEPTEMBER)
        self.assertIsNone(result["gross"])
        self.assertIsNone(result["net"])

    def test_manual_confirmations_are_not_verified_cash(self):
        result = services._cash_summary([self.payment(status="manual_confirmed", refund_status="", refund_processed_at=None)], AUGUST, SEPTEMBER)
        self.assertEqual(result["gross"], {})
        self.assertEqual(result["manual_confirmed"], {"UAH": Decimal("499")})
        self.assertEqual(result["capture_count"], 0)

    def test_currency_and_half_open_event_boundaries_are_preserved(self):
        rows = [self.payment(status="paid", paid_at=SEPTEMBER, refund_status="", refund_processed_at=None),
                self.payment(status="paid", paid_at=OCTOBER, amount=Decimal("700"), refund_status="", refund_processed_at=None),
                self.payment(status="paid", paid_at=SEPTEMBER, amount=Decimal("2"), currency="USD", refund_status="", refund_processed_at=None)]
        result = services._cash_summary(rows, SEPTEMBER, OCTOBER)
        self.assertEqual(result["gross"], {"UAH": Decimal("499"), "USD": Decimal("2")})
        self.assertEqual(result["capture_count"], 2)


class InvestorRecurringTruthTests(SimpleTestCase):
    def test_ninety_binding_fees_and_one_renewal_are_one_recurring_payer(self):
        payment = InvestorCashTruthTests().payment
        rows = [payment(pk=i, user_id=i, status="paid", kind="bind", paid_at=NOW,
                        amount=Decimal("1"), refund_status="", refund_processed_at=None) for i in range(90)]
        rows.append(payment(pk=901, user_id=901, status="paid", paid_at=NOW, refund_status="", refund_processed_at=None))
        result = services._cash_summary(rows, SEPTEMBER, OCTOBER)
        self.assertEqual(result.get("recurring_payers"), 1)
        self.assertEqual(result["bind_payers"], 90)
        self.assertEqual(result["recurring_cash"], {"UAH": Decimal("499")})
        self.assertEqual(result["bind_cash"], {"UAH": Decimal("90")})
        self.assertEqual(result["gross"], {"UAH": Decimal("589")})
        rows.append(payment(pk=902, user_id=901, status="paid", kind="retry", paid_at=NOW, refund_status="", refund_processed_at=None))
        self.assertEqual(services._cash_summary(rows, SEPTEMBER, OCTOBER)["recurring_payers"], 1)


class InvestorOnboardingTruthTests(SimpleTestCase):
    def test_current_completed_flag_cannot_establish_august_completion(self):
        # Registered August, finished September: a current True gives no August cutoff.
        current_cohort = SimpleNamespace(filter=lambda **kw: SimpleNamespace(count=lambda: 1))
        env = dashboard_assignments({"period_onboarded_count", "previous_period_onboarded_count"}, {
            "period_users_qs": current_cohort, "previous_period_users_qs": current_cohort,
            "safe": lambda fn, default: fn(),
        })
        self.assertIsNone(env["period_onboarded_count"])
        self.assertIsNone(env["previous_period_onboarded_count"])
        self.assertIsNone(services._percentage(None, 1))
        self.assertEqual(services._point_change(None, None)["delta"], "—")

    def test_unavailable_funnel_has_reason_and_no_zero_width_bar(self):
        html = render_to_string("dashboard/index.html", {"funnel_stages": [{
            "label": "Онбординг до кінця періоду", "value": None,
            "percent_display": "Недоступно", "width": None, "reason": "Немає історії завершення",
        }]})
        self.assertIn("Немає історії завершення", html)
        self.assertNotIn('style="width: None%"', html)


class EmptyQuery:
    """Only persistence is stubbed; full dashboard assembly/template stays real."""
    def __init__(self, *, broken=False):
        self.broken = broken

    def all(self): return self
    def filter(self, *args, **kwargs): return self
    def exclude(self, *args, **kwargs): return self
    def annotate(self, *args, **kwargs): return self
    def values(self, *args, **kwargs): return self
    def values_list(self, *args, **kwargs): return self
    def distinct(self): return self
    def only(self, *args): return self
    def order_by(self, *args): return self
    def select_related(self, *args): return self
    def __getitem__(self, key): return self
    def __iter__(self):
        if self.broken:
            raise RuntimeError("synthetic DB outage; secret canary must not be logged")
        return iter(())
    def count(self):
        if self.broken:
            raise RuntimeError("synthetic DB outage; secret canary must not be logged")
        return 0


def synthetic_dashboard(*, broken=False, broken_models=(), rows_by_model=None):
    with ExitStack() as stack:
        for model in (services.TelegramUser, services.Subscription, services.Payment,
                      services.BotEvent, services.BroadcastRecipient, services.TrialRecoveryRecipient):
            query = ScopedRows((rows_by_model or {}).get(model, [])) if rows_by_model is not None else EmptyQuery()
            if broken or model in broken_models:
                query = EmptyQuery(broken=True)
            stack.enter_context(patch.object(model, "objects", query))
        stack.enter_context(patch.object(services, "Subquery", return_value=None))
        stack.enter_context(patch.object(services, "recovered_recipient_queryset", side_effect=lambda qs: qs))
        for health in ("_check_redis", "_check_worker", "_check_telegram_api"):
            stack.enter_context(patch.object(services, health, return_value={"status": "OK", "detail": "synthetic"}))
        stack.enter_context(patch.object(services.timezone, "now", return_value=NOW))
        return services.build_dashboard_context(RequestFactory().get("/"))


class InvestorUnavailableTruthTests(SimpleTestCase):
    def test_query_failure_is_not_a_valid_zero(self):
        def broken():
            raise RuntimeError("private canary")
        with self.assertLogs("dashboard.services", level="ERROR") as logs:
            self.assertIsNone(services.safe(broken, 0))
        self.assertFalse("private canary" in " ".join(logs.output))
        self.assertEqual(services.safe(lambda: 0, 99), 0)

    def test_full_outage_marks_cards_attention_and_charts_unavailable(self):
        with self.assertLogs("dashboard.services", level="ERROR"):
            context = synthetic_dashboard(broken=True)
        self.assertTrue(context.get("data_quality", {}).get("degraded"))
        self.assertIsNone(context["subscription_total"])
        self.assertEqual(context["user_segment_series"], [])
        for item in context["attention_items"]:
            self.assertIsNone(item["value"])
            self.assertEqual(item["tone"], "unknown")
        self.assertIsNone(next(card for card in context["kpi_cards"] if card["label"] == "Нові користувачі")["value"])
        html = render_to_string("dashboard/index.html", context)
        self.assertTrue("Частина даних недоступна" in html)
        self.assertFalse("У цьому сегменті користувачів ще немає" in html)

    def test_payment_outage_preserves_users_but_never_relabels_them_free(self):
        with self.assertLogs("dashboard.services", level="ERROR"):
            context = synthetic_dashboard(broken_models=(Payment,))
        self.assertEqual(context["subscription_total"], 0)
        self.assertFalse(context.get("segments_available", True))
        self.assertIsNone(context["cash_summary"]["gross"])
        self.assertEqual(context["subscription_distribution"], [])

    def test_healthy_zero_is_not_degraded(self):
        context = synthetic_dashboard()
        self.assertFalse(context.get("data_quality", {}).get("degraded", True))
        self.assertEqual(context["subscription_total"], 0)
        self.assertTrue(all(item["value"] == 0 and item["tone"] == "ok" for item in context["attention_items"]))


class ScopedRows(EmptyQuery):
    def __init__(self, rows): self.rows = list(rows)
    def __iter__(self): return iter(self.rows)
    def count(self): return len(self.rows)
    def __getitem__(self, key): return ScopedRows(self.rows[key]) if isinstance(key, slice) else self.rows[key]
    def values(self, *fields):
        return ScopedRows({field: self.value(row, field) for field in fields} for row in self.rows)
    def distinct(self):
        rows = []
        for row in self.rows:
            if row not in rows: rows.append(row)
        return ScopedRows(rows)
    def annotate(self, **expressions):
        rows = []
        for row in self.rows:
            item = copy.copy(row)
            for name, expression in expressions.items():
                if hasattr(expression, "name"):
                    value = self.value(row, expression.name)
                    if isinstance(item, dict): item[name] = value
                    else: setattr(item, name, value)
            rows.append(item)
        return ScopedRows(rows)
    @staticmethod
    def value(row, path):
        for part in path.split("__"):
            row = row.get(part) if isinstance(row, dict) else getattr(row, part, None)
        return row
    @classmethod
    def matches(cls, row, condition):
        if hasattr(condition, "children"):
            checks = [cls.matches(row, child) for child in condition.children]
            result = any(checks) if condition.connector == "OR" else all(checks)
            return not result if condition.negated else result
        key, expected = condition
        if hasattr(expected, "resolve_expression"):
            return True  # latest subscription is unique per fixture user
        parts = key.rsplit("__", 1)
        op = parts[-1] if parts[-1] in {"in", "gte", "lte", "lt", "gt", "isnull", "iexact"} else "exact"
        actual = cls.value(row, parts[0] if op != "exact" else key)
        if op == "exact": return actual == expected
        if op == "iexact": return str(actual).lower() == str(expected).lower()
        if op == "in":
            if isinstance(expected, ScopedRows):
                expected = [next(iter(item.values())) if isinstance(item, dict) else item for item in expected]
            return actual in expected
        if op == "isnull": return (actual is None) == expected
        if actual is None: return False
        return {"gte": lambda: actual >= expected, "lte": lambda: actual <= expected,
                "lt": lambda: actual < expected, "gt": lambda: actual > expected}[op]()
    def filter(self, *args, **kwargs):
        return ScopedRows(row for row in self.rows if all(self.matches(row, c) for c in [*args, *kwargs.items()]))
    def exclude(self, *args, **kwargs):
        return ScopedRows(row for row in self.rows if not all(self.matches(row, c) for c in [*args, *kwargs.items()]))


class InvestorOperatorInterfaceTests(SimpleTestCase):
    @override_settings(ADMIN_TEST_TELEGRAM_IDS=[5])
    def test_shared_current_metrics_exclude_tests_and_stale_paid_projection(self):
        builder = getattr(services, "build_current_user_metrics", None)
        self.assertTrue(callable(builder), "Operator needs a shared scoped metric API, not UserAdminState.PAID")
        users = [SimpleNamespace(pk=i, tg_user_id=i, last_seen_at=NOW, onboarding_completed=True,
                 admin_state=SimpleNamespace(is_test_user=i in (3, 4), subscription_status="paid")) for i in range(1, 6)]
        subs = [Subscription(pk=i, user_id=i, status="manual", started_at=AUGUST, expires_at=OCTOBER) for i in range(3, 6)]
        # Scope joins are synthetic, with no relation descriptor hitting a DB.
        subs = [SimpleNamespace(**{field: getattr(s, field) for field in ("pk", "user_id", "status", "started_at", "expires_at", "source")}, user=users[s.user_id-1]) for s in subs]
        with patch.object(services.TelegramUser, "objects", ScopedRows(users)), \
             patch.object(services.Subscription, "objects", ScopedRows(subs)), \
             patch.object(services.Payment, "objects", ScopedRows([])), \
             patch.object(services, "Subquery", return_value=None):
            regular = builder(now=NOW)
            including = builder(now=NOW, include_test_users=True)
        self.assertEqual(regular["total_users"], 2)
        self.assertEqual(regular["paid_users"], 0)
        self.assertEqual(including["total_users"], 5)
        self.assertEqual(including["paid_users"], 0)
        self.assertEqual(including["granted_users"], 3)
        self.assertFalse(regular["include_test_users"])
        self.assertTrue(including["include_test_users"])
        self.assertTrue("тестов" in including["scope_label"])
        self.assertTrue(all(regular["scope_label"] in item["hint"] for item in regular["metrics"]))

    @override_settings(ADMIN_TEST_TELEGRAM_IDS=[5])
    def test_scope_filter_compiles_without_database_or_projection_paid_filter(self):
        query = services._exclude_test_users(services.TelegramUser.objects.all(), prefix="", include_test_users=False)
        sql, params = query.query.sql_with_params()
        self.assertIn("is_test_user", sql)
        self.assertIn(5, params)
        self.assertNotIn("subscription_status", sql)


class InvestorDashboardAssemblyTests(SimpleTestCase):
    def fixture(self):
        users = [SimpleNamespace(pk=i, tg_user_id=i, first_name="Synthetic", full_name=f"Synthetic {i}",
                                username="", created_at=SEPTEMBER, last_seen_at=NOW, onboarding_completed=True,
                                admin_state=SimpleNamespace(is_test_user=False)) for i in range(1, 92)]
        payments = [SimpleNamespace(pk=i, user_id=i, subscription_id=None, user=users[i-1], kind="bind", status="paid",
                                    paid_at=NOW, amount=Decimal("1"), currency="UAH", raw_payload={}, created_at=NOW,
                                    updated_at=NOW) for i in range(1, 91)]
        payments.append(SimpleNamespace(pk=91, user_id=91, user=users[90], subscription_id=41, kind="renewal", status="paid",
                                        paid_at=NOW, amount=Decimal("499"), currency="UAH", raw_payload={}, created_at=NOW,
                                        updated_at=NOW))
        sub = SimpleNamespace(pk=41, user_id=91, user=users[90], status="active", source="payment", started_at=AUGUST, expires_at=OCTOBER)
        grant = SimpleNamespace(pk=42, user_id=1, user=users[0], status="manual", source="admin", started_at=AUGUST, expires_at=OCTOBER)
        return {services.TelegramUser: users, Subscription: [sub, grant], Payment: payments}

    def test_full_dashboard_reconciles_synthetic_counts_cash_and_segments(self):
        rows = self.fixture()
        context = synthetic_dashboard(rows_by_model=rows)
        self.assertFalse(context["data_quality"]["degraded"])
        self.assertEqual(context["subscription_total"], 91)
        self.assertEqual({row["tone"]: row["value"] for row in context["subscription_distribution"]},
                         {"free": 89, "trial": 0, "paid": 1, "granted": 1})
        self.assertEqual(sum(sum(point[segment] for segment in services.USER_SEGMENTS) for point in context["user_segment_series"]), 91)
        cards = {card["label"]: card for card in context["kpi_cards"]}
        self.assertEqual(cards["Платники renewal/retry за період"]["value"], 1)
        self.assertEqual(cards["Платники bind за період"]["value"], 90)
        self.assertEqual(cards["Валові надходження, UAH"]["value"], "589.00 UAH")
        self.assertEqual(cards["Онбординг зараз у когорті"]["value"], 91)
        self.assertIsNone(cards["Історична конверсія онбордингу"]["value"])
        self.assertEqual([stage["value"] for stage in context["funnel_stages"]], [91, None, 90, 1])
        self.assertTrue("589.00 UAH" in render_to_string("dashboard/index.html", context))

    def test_subscription_outage_does_not_fabricate_no_subscription_in_user_table(self):
        with self.assertLogs("dashboard.services", level="ERROR"):
            context = synthetic_dashboard(rows_by_model=self.fixture(), broken_models=(Subscription,))
        self.assertTrue(all(user["subscription_label"] == "Недоступно" for user in context["recent_users"]))
        self.assertEqual(context["subscription_total"], 91)

    def test_real_orm_queries_compile_without_fetching_private_payloads(self):
        compiled = []
        def no_database_fetch(queryset):
            compiled.append(queryset.query.sql_with_params()[0])
            queryset._result_cache = []
        with patch.object(QuerySet, "_fetch_all", no_database_fetch), \
             patch.object(QuerySet, "count", return_value=0), \
             patch.object(services, "_check_redis", return_value={"status": "OK"}), \
             patch.object(services, "_check_worker", return_value={"status": "OK"}), \
             patch.object(services, "_check_telegram_api", return_value={"status": "OK"}):
            context = services.build_dashboard_context(RequestFactory().get("/"))
        self.assertFalse(context["data_quality"]["degraded"])
        self.assertTrue(any("refund_processed_at" in sql for sql in compiled))
        for sql in compiled:
            self.assertFalse('"last_user_input"' in sql, "Metrics must not select raw user input")
            self.assertFalse('"onboarding_payload"' in sql, "Metrics must not select private onboarding payload")
            self.assertFalse(', "payments"."raw_payload",' in sql, "Select only refund scalar fields, not entire provider payload")

    def test_refund_amount_and_count_are_both_visible(self):
        rows = self.fixture()
        payment = rows[Payment][-1]
        payment.status = "refunded"
        payment.raw_payload = {"refund_request": {"status": "success", "processed_at": NOW.isoformat()}}
        context = synthetic_dashboard(rows_by_model=rows)
        card = next(card for card in context["kpi_cards"] if card["label"] == "Повернення коштів")
        self.assertEqual(card["value"], "499.00 UAH")
        self.assertIn("Кількість: 1", card["detail"])
