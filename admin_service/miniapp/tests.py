from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
import hashlib
import hmac
import json
from unittest.mock import patch
from urllib.parse import urlencode

from django.db import connection
from django.test import Client, TestCase, override_settings
from django.utils import timezone

from accounts.models import Account
from categories.models import Category, CategoryTemplate
from common.test_helpers import ensure_runtime_finance_tables, ensure_telegram_user_table
from miniapp import auth as miniapp_auth
import miniapp.fx as miniapp_fx
from miniapp.fx import FxSnapshot
from miniapp.history import MiniAppHistoryError, build_history_payload
from miniapp.models import AppNotification, InstallNudgeState, NotificationPreference, WebPushSubscription, WriteReceipt
from subscriptions.models import BillingProfile, Subscription
from transactions.models import Debt, DebtPayment, Transaction
from users.models import TelegramUser, UserAdminState


def _build_init_data(*, bot_token: str, tg_user_id: int, first_name: str = "Ihor", username: str = "ihor") -> str:
    payload = {
        "auth_date": str(int(timezone.now().timestamp())),
        "query_id": "AAEAAAE",
        "user": json.dumps({"id": tg_user_id, "first_name": first_name, "username": username}, separators=(",", ":")),
    }
    data_check_string = "\n".join(f"{key}={value}" for key, value in sorted(payload.items()))
    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    payload["hash"] = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    return urlencode(payload)


@override_settings(
    DEBUG=True,
    MINIAPP_DEV_TG_USER_ID=1001,
    TELEGRAM_BOT_TOKEN="123456:test-miniapp-token",
)
class MiniAppTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()
        ensure_runtime_finance_tables()

    def setUp(self):
        self.client = Client()
        self.user = TelegramUser.objects.create(
            tg_user_id=1001,
            first_name="Ihor",
            username="ihor",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

    def _grant_full_access(self):
        now = timezone.now()
        Subscription.objects.create(
            user=self.user,
            status=Subscription.Status.ACTIVE,
            source=Subscription.Source.ADMIN,
            started_at=now,
            expires_at=now + timedelta(days=30),
        )
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.PAID,
                "access_scope": UserAdminState.AccessScope.PERSONAL_FULL,
                "access_source": "test",
                "is_blocked": False,
            },
        )

    def _create_category(self, *, kind: str, name: str = "Food") -> Category:
        return Category.objects.create(
            tg_user=self.user,
            user=self.user,
            kind=kind,
            type=kind,
            name=name,
            aliases=[],
            source="manual",
            is_active=True,
            sort_order=1,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

    def _create_account(self, *, balance: Decimal = Decimal("1000"), currency: str = "UAH") -> Account:
        return Account.objects.create(
            tg_user=self.user,
            label="Mono Black",
            currency=currency,
            account_type="main",
            starting_balance=Decimal("0"),
            balance=balance,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

    def _create_category_template(self, *, kind: str, name: str, slug: str | None = None) -> CategoryTemplate:
        return CategoryTemplate.objects.create(
            type=kind,
            name=name,
            slug=slug,
            aliases=[],
            sort_order=1,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

    def test_onboarding_completes_with_confirmed_account_and_default_categories(self):
        self.user.onboarding_completed = False
        self.user.onboarding_version = 0
        self.user.start_date = None
        self.user.save(update_fields=["onboarding_completed", "onboarding_version", "start_date"])
        self._create_category_template(kind="expense", name="Food", slug="food")
        self._create_category_template(kind="income", name="Salary", slug="salary")

        self.assertEqual(self.client.post("/app/api/auth/dev").status_code, 200)
        status = self.client.get("/app/api/onboarding")
        self.assertEqual(status.status_code, 200)
        self.assertFalse(status.json()["onboarding"]["completed"])

        draft_response = self.client.post(
            "/app/api/onboarding/draft",
            data=json.dumps(
                {
                    "lang": "uk",
                    "base_currency": "UAH",
                    "start_date": "2026-07-01",
                    "accounts": [
                        {
                            "label": "Mono Black",
                            "account_type": "main",
                            "currency": "UAH",
                            "starting_balance": "1250.50",
                        }
                    ],
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)
        draft_id = draft_response.json()["draft"]["draft_id"]
        idempotency_key = "onboarding-confirm-key-0001"

        confirm_response = self.client.post(
            "/app/api/onboarding/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": idempotency_key}),
            content_type="application/json",
        )
        self.assertEqual(confirm_response.status_code, 200)
        self.assertEqual(confirm_response.json()["result"]["status"], "completed")
        self.user.refresh_from_db()
        self.assertTrue(self.user.onboarding_completed)
        self.assertEqual(self.user.onboarding_version, 3)
        self.assertEqual(str(self.user.start_date), "2026-07-01")
        self.assertEqual(Account.objects.filter(tg_user=self.user, family_id__isnull=True, is_active=True).count(), 1)
        self.assertEqual(Category.objects.filter(user=self.user, family_id__isnull=True, is_active=True).count(), 2)
        self.assertTrue(InstallNudgeState.objects.filter(tg_user_id=self.user.tg_user_id).exists())

        retry_response = self.client.post(
            "/app/api/onboarding/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": idempotency_key}),
            content_type="application/json",
        )
        self.assertEqual(retry_response.status_code, 200)
        self.assertTrue(retry_response.json()["idempotent"])
        self.assertEqual(Account.objects.filter(tg_user=self.user, family_id__isnull=True, is_active=True).count(), 1)

    def test_onboarding_completes_with_account_created_in_active_family_scope(self):
        self.user.onboarding_completed = False
        self.user.onboarding_version = 0
        self.user.start_date = None
        self.user.save(update_fields=["onboarding_completed", "onboarding_version", "start_date"])
        self._create_category_template(kind="expense", name="Food", slug="food")
        self._create_category_template(kind="income", name="Salary", slug="salary")
        Category.objects.create(
            tg_user=self.user,
            user=self.user,
            kind="expense",
            type="expense",
            name="Food",
            slug="personal-food",
            source="manual",
            is_active=True,
        )
        owner = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Owner",
            username="owner",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        now = timezone.now()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO families (id, name, owner_user_id, status, created_at, updated_at)
                VALUES (1, %s, %s, 'active', %s, %s)
                """,
                ["Main family", owner.tg_user_id, now, now],
            )
            cursor.execute(
                """
                INSERT INTO family_members (
                  family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at
                )
                VALUES (1, %s, 'member', 'active', %s, %s, %s, %s)
                """,
                [self.user.tg_user_id, owner.tg_user_id, now, now, now],
            )

        self.assertEqual(self.client.post("/app/api/auth/dev").status_code, 200)
        draft_response = self.client.post(
            "/app/api/onboarding/draft",
            data=json.dumps(
                {
                    "lang": "uk",
                    "base_currency": "UAH",
                    "start_date": "2026-07-01",
                    "accounts": [
                        {
                            "label": "Monobank",
                            "account_type": "main",
                            "currency": "UAH",
                            "starting_balance": "0",
                        }
                    ],
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)

        confirm_response = self.client.post(
            "/app/api/onboarding/confirm",
            data=json.dumps(
                {
                    "draft_id": draft_response.json()["draft"]["draft_id"],
                    "idempotency_key": "onboarding-family-confirm-0001",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.onboarding_completed)
        self.assertTrue(Account.objects.filter(tg_user=self.user, family_id=1, label="Monobank", is_active=True).exists())

    def test_install_nudge_tracks_shows_and_stops_after_confirmation(self):
        self.assertEqual(self.client.post("/app/api/auth/dev").status_code, 200)

        status_response = self.client.get("/app/api/install-nudge")
        self.assertEqual(status_response.status_code, 200)
        status = status_response.json()["install_nudge"]
        self.assertTrue(status["due"])
        self.assertEqual(status["prompt_count"], 0)
        self.assertEqual(status["max_prompts"], 4)
        self.assertIn("/app/browser-login/", status["browser_login_url"])

        shown_response = self.client.post(
            "/app/api/install-nudge/event",
            data=json.dumps({"event": "shown", "platform": "android"}),
            content_type="application/json",
        )
        self.assertEqual(shown_response.status_code, 200)
        shown = shown_response.json()["install_nudge"]
        self.assertFalse(shown["due"])
        self.assertEqual(shown["prompt_count"], 1)
        state = InstallNudgeState.objects.get(tg_user_id=self.user.tg_user_id)
        self.assertIsNotNone(state.next_prompt_at)
        self.assertIsNotNone(state.next_telegram_reminder_at)

        confirmed_response = self.client.post(
            "/app/api/install-nudge/event",
            data=json.dumps({"event": "confirmed", "platform": "android"}),
            content_type="application/json",
        )
        self.assertEqual(confirmed_response.status_code, 200)
        confirmed = confirmed_response.json()["install_nudge"]
        self.assertTrue(confirmed["installed"])
        self.assertFalse(confirmed["due"])
        self.assertEqual(confirmed["browser_login_url"], "")
        state.refresh_from_db()
        self.assertEqual(state.installed_platform, "android")
        self.assertIsNone(state.next_prompt_at)
        self.assertIsNone(state.next_telegram_reminder_at)

    def test_onboarding_requires_an_account_before_draft(self):
        self.user.onboarding_completed = False
        self.user.onboarding_version = 0
        self.user.save(update_fields=["onboarding_completed", "onboarding_version"])
        self._create_category_template(kind="expense", name="Food", slug="food")
        self._create_category_template(kind="income", name="Salary", slug="salary")

        self.client.post("/app/api/auth/dev")
        response = self.client.post(
            "/app/api/onboarding/draft",
            data=json.dumps({"lang": "uk", "base_currency": "UAH", "start_date": "2026-07-01", "accounts": []}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "onboarding_account_required")

    @patch("miniapp.fx.urlopen")
    def test_fx_rates_add_usdt_proxy_from_usd(self, mock_urlopen):
        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(
                    [
                        {"cc": "USD", "rate": 41.25, "exchangedate": "21.05.2026"},
                        {"cc": "EUR", "rate": 46.10, "exchangedate": "21.05.2026"},
                    ]
                ).encode("utf-8")

        miniapp_fx._CACHE.clear()
        mock_urlopen.return_value = _FakeResponse()

        snapshot = miniapp_fx.get_latest_rates("UAH")

        self.assertEqual(snapshot.base, "UAH")
        self.assertEqual(snapshot.rates["USD"], Decimal("41.25"))
        self.assertEqual(snapshot.rates["USDT"], Decimal("41.25"))

    @override_settings(ADMIN_IP_ALLOWLIST=["1.2.3.4"])
    def test_app_shell_bypasses_admin_ip_allowlist(self):
        response = self.client.get("/app/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Vydno.Capital")
        self.assertContains(response, "telegram-web-app.js")

    def test_app_shell_uses_single_v2_bundle_by_default(self):
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "miniapp/app.css?v=")
        self.assertContains(response, 'class="app"')
        self.assertContains(response, 'class="bottom-nav"')
        self.assertContains(response, 'class="period-inline"')
        self.assertContains(response, 'data-period-trigger')
        self.assertNotContains(response, "miniapp/app_legacy.css")

    def test_app_shell_sets_no_cache_headers(self):
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, no-cache, must-revalidate, max-age=0, private")
        self.assertEqual(response.headers.get("Pragma"), "no-cache")
        self.assertEqual(response.headers.get("Expires"), "0")

    def test_app_shell_keeps_v2_namespace_and_view_hooks(self):
        response = self.client.get("/app/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "miniapp/app.css?v=")
        self.assertContains(response, 'data-screen="overview"')
        self.assertContains(response, 'data-screen="analytics"')
        self.assertContains(response, 'data-screen="status"')
        self.assertContains(response, 'class="period-option" type="button" data-period="today"', count=1)
        self.assertContains(response, 'class="period-option" type="button" data-period="last_30"', count=1)
        self.assertContains(response, 'class="period-option is-active" type="button" data-period="month"', count=1)
        self.assertContains(response, 'type="date" data-date-from', count=1)
        self.assertContains(response, 'type="date" data-date-to', count=1)
        self.assertNotContains(response, 'id="insightList"')
        self.assertNotContains(response, 'class="period-card"')
        self.assertNotContains(response, "miniapp/app_legacy.css")

    def test_telegram_auth_creates_session(self):
        init_data = _build_init_data(bot_token="123456:test-miniapp-token", tg_user_id=self.user.tg_user_id)
        response = self.client.post(
            "/app/api/auth/telegram",
            data=json.dumps({"init_data": init_data}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        session = self.client.session
        self.assertEqual(session.get("miniapp_tg_user_id"), self.user.tg_user_id)

    def test_browser_login_creates_long_lived_browser_session(self):
        token = miniapp_auth.build_browser_login_token(self.user.tg_user_id, nonce="n" * 18)

        response = self.client.get(f"/app/login/{token}/")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/app/")
        session = self.client.session
        self.assertEqual(session.get("miniapp_tg_user_id"), self.user.tg_user_id)
        self.assertEqual(session.get("miniapp_auth_mode"), miniapp_auth.BROWSER_AUTH_MODE)
        self.assertGreater(session.get_expiry_age(), 170 * 24 * 60 * 60)

    def test_browser_login_link_is_one_use(self):
        token = miniapp_auth.build_browser_login_token(self.user.tg_user_id, nonce="n" * 18)

        first_response = self.client.get(f"/app/login/{token}/")
        second_response = self.client.get(f"/app/login/{token}/")

        self.assertEqual(first_response.status_code, 302)
        self.assertEqual(second_response.status_code, 400)
        self.assertIn("already used", second_response.content.decode("utf-8"))

    def test_profile_requires_session(self):
        response = self.client.get("/app/api/profile")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "session_required")

    def test_profile_returns_authenticated_user(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get("/app/api/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["user"]["tg_user_id"], self.user.tg_user_id)
        self.assertEqual(payload["user"]["lang"], "uk")
        self.assertEqual(payload["access"]["mode"], "blocked")

    def test_logout_clears_session(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        logout_response = self.client.post("/app/api/auth/logout", data="{}", content_type="application/json")
        self.assertEqual(logout_response.status_code, 200)
        self.assertTrue(logout_response.json()["ok"])

        response = self.client.get("/app/api/profile")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "session_required")

    def test_transaction_draft_requires_active_access(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.post(
            "/app/api/transactions/draft",
            data=json.dumps({"kind": "expense", "amount": "100"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "access_blocked")
        self.assertEqual(Transaction.objects.count(), 0)

    def test_transaction_draft_does_not_write_until_confirm(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("1000"))
        category = self._create_category(kind="expense", name="Food")
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.post(
            "/app/api/transactions/draft",
            data=json.dumps(
                {
                    "kind": "expense",
                    "amount": "125.50",
                    "account_id": account.id,
                    "category_id": category.id,
                    "currency": "UAH",
                    "transaction_date": timezone.localdate().isoformat(),
                    "comment": "Groceries",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["draft"]["amount"]["value"], "125.50")
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("1000.00"))
        self.assertEqual(Transaction.objects.count(), 0)

    def test_transaction_confirm_writes_once_and_reuses_committed_draft(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("1000"))
        category = self._create_category(kind="expense", name="Food")
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)
        draft_response = self.client.post(
            "/app/api/transactions/draft",
            data=json.dumps(
                {
                    "kind": "expense",
                    "amount": "125.50",
                    "account_id": account.id,
                    "category_id": category.id,
                    "transaction_date": timezone.localdate().isoformat(),
                    "comment": "Groceries",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)
        draft_id = draft_response.json()["draft"]["draft_id"]

        confirm_response = self.client.post(
            "/app/api/transactions/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": "same-key-12345678"}),
            content_type="application/json",
        )
        second_response = self.client.post(
            "/app/api/transactions/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": "same-key-12345678"}),
            content_type="application/json",
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.assertFalse(confirm_response.json()["idempotent"])
        self.assertEqual(second_response.status_code, 200)
        self.assertTrue(second_response.json()["idempotent"])
        self.assertEqual(Transaction.objects.count(), 1)
        self.assertEqual(WriteReceipt.objects.count(), 1)
        tx = Transaction.objects.get()
        self.assertEqual(tx.source, "miniapp_manual")
        self.assertEqual(tx.flow_kind, "normal")
        self.assertEqual(tx.category_name_snapshot, "Food")
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("874.50"))

    def test_history_keyset_pagination_reaches_every_same_day_transaction(self):
        account = self._create_account(balance=Decimal("1000"))
        category = self._create_category(kind="expense", name="Food")
        now = timezone.now()
        Transaction.all_objects.bulk_create(
            [
                Transaction(
                    tg_user=self.user,
                    date=timezone.localdate(),
                    type="expense",
                    amount=Decimal("1"),
                    currency="UAH",
                    source="manual",
                    account=account,
                    category=category,
                    category_name_snapshot="Food",
                    flow_kind="normal",
                    created_at=now + timedelta(microseconds=index),
                )
                for index in range(205)
            ]
        )

        first = build_history_payload(
            self.user,
            date_from=timezone.localdate(),
            date_to=timezone.localdate(),
            limit=100,
        )
        second = build_history_payload(
            self.user,
            date_from=timezone.localdate(),
            date_to=timezone.localdate(),
            limit=100,
            cursor=first["next_cursor"],
        )
        third = build_history_payload(
            self.user,
            date_from=timezone.localdate(),
            date_to=timezone.localdate(),
            limit=100,
            cursor=second["next_cursor"],
        )

        ids = [item["id"] for page in (first, second, third) for item in page["items"]]
        self.assertEqual(len(ids), 205)
        self.assertEqual(len(set(ids)), 205)
        self.assertTrue(first["has_more"])
        self.assertTrue(second["has_more"])
        self.assertFalse(third["has_more"])
        self.assertIsNone(third["next_cursor"])
        with self.assertRaises(MiniAppHistoryError):
            build_history_payload(
                self.user,
                date_from=timezone.localdate() - timedelta(days=1),
                date_to=timezone.localdate(),
                limit=100,
                cursor=first["next_cursor"],
            )
        other_user = TelegramUser.objects.create(
            tg_user_id=909090,
            first_name="Other",
            username="other-history",
            lang="uk",
            base_currency="UAH",
        )
        with self.assertRaises(MiniAppHistoryError):
            build_history_payload(
                other_user,
                date_from=timezone.localdate(),
                date_to=timezone.localdate(),
                limit=100,
                cursor=first["next_cursor"],
            )

    def test_history_edit_requires_confirmation_and_recalculates_balance_once(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("900"))
        category = self._create_category(kind="expense", name="Food")
        original = Transaction.objects.create(
            tg_user=self.user,
            date=timezone.localdate(),
            type="expense",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            account=account,
            category=category,
            category_name_snapshot="Food",
            flow_kind="normal",
            comment="Initial groceries",
            created_at=timezone.now(),
        )
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        draft_response = self.client.post(
            "/app/api/history/edit/draft",
            data=json.dumps(
                {
                    "transaction_id": original.id,
                    "kind": "expense",
                    "amount": "150",
                    "account_id": account.id,
                    "category_id": category.id,
                    "transaction_date": timezone.localdate().isoformat(),
                    "comment": "Updated groceries",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(draft_response.status_code, 200)
        self.assertEqual(draft_response.json()["draft"]["replacement"]["amount"]["value"], "150.00")
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("900.00"))
        original.refresh_from_db()
        self.assertEqual(original.amount, Decimal("100.00"))

        draft_id = draft_response.json()["draft"]["draft_id"]
        confirm_response = self.client.post(
            "/app/api/history/edit/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": "history-edit-key-1234"}),
            content_type="application/json",
        )
        second_response = self.client.post(
            "/app/api/history/edit/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": "history-edit-key-1234"}),
            content_type="application/json",
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.assertFalse(confirm_response.json()["idempotent"])
        self.assertEqual(second_response.status_code, 200)
        self.assertTrue(second_response.json()["idempotent"])
        self.assertEqual(WriteReceipt.objects.count(), 1)
        original.refresh_from_db()
        account.refresh_from_db()
        self.assertEqual(original.amount, Decimal("150.00"))
        self.assertEqual(original.comment, "Updated groceries")
        self.assertEqual(account.balance, Decimal("850.00"))

    def test_history_void_expense_restores_balance_once_and_soft_deletes(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("900"))
        transaction = Transaction.objects.create(
            tg_user=self.user,
            created_by_user_id=self.user.tg_user_id,
            date=timezone.localdate(),
            type="expense",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            account=account,
            category=None,
            flow_kind="normal",
            comment="Legacy expense",
            created_at=timezone.now(),
        )
        self.assertEqual(self.client.post("/app/api/auth/dev").status_code, 200)

        draft_response = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": transaction.id}),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)
        draft = draft_response.json()["draft"]
        self.assertEqual(draft["current_balance"]["value"], "900.00")
        self.assertEqual(draft["balance_after"]["value"], "1000.00")

        confirm_response = self.client.post(
            "/app/api/history/void/confirm",
            data=json.dumps({"draft_id": draft["draft_id"], "idempotency_key": "history-void-expense-0001"}),
            content_type="application/json",
        )
        retry_response = self.client.post(
            "/app/api/history/void/confirm",
            data=json.dumps({"draft_id": draft["draft_id"], "idempotency_key": "history-void-expense-0001"}),
            content_type="application/json",
        )

        self.assertEqual(confirm_response.status_code, 200)
        self.assertEqual(confirm_response.json()["result"]["new_balance"]["value"], "1000.00")
        self.assertTrue(retry_response.json()["idempotent"])
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("1000.00"))
        self.assertFalse(Transaction.objects.filter(id=transaction.id).exists())
        cancelled = Transaction.all_objects.get(id=transaction.id)
        self.assertTrue(cancelled.is_deleted)
        self.assertIsNotNone(cancelled.deleted_at)
        self.assertEqual(cancelled.deleted_by_user_id, self.user.tg_user_id)

    def test_history_void_income_subtracts_amount_and_updates_credit_mode(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("20"))
        account.non_negative_account_type = "main"
        account.save(update_fields=["non_negative_account_type", "updated_at"])
        transaction = Transaction.objects.create(
            tg_user=self.user,
            created_by_user_id=self.user.tg_user_id,
            date=timezone.localdate(),
            type="income",
            amount=Decimal("50"),
            currency="UAH",
            source="manual",
            account=account,
            flow_kind="normal",
            created_at=timezone.now(),
        )
        self.client.post("/app/api/auth/dev")
        draft = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": transaction.id}),
            content_type="application/json",
        ).json()["draft"]
        response = self.client.post(
            "/app/api/history/void/confirm",
            data=json.dumps({"draft_id": draft["draft_id"], "idempotency_key": "history-void-income-0001"}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("-30.00"))
        self.assertEqual(account.account_type, "credit")

    def test_history_void_rejects_unsupported_and_changed_transactions(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("900"))
        transfer = Transaction.objects.create(
            tg_user=self.user,
            created_by_user_id=self.user.tg_user_id,
            date=timezone.localdate(),
            type="transfer",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            from_account=account,
            flow_kind="transfer",
            created_at=timezone.now(),
        )
        expense = Transaction.objects.create(
            tg_user=self.user,
            created_by_user_id=self.user.tg_user_id,
            date=timezone.localdate(),
            type="expense",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            account=account,
            flow_kind="normal",
            comment="Before",
            created_at=timezone.now(),
        )
        self.client.post("/app/api/auth/dev")
        unsupported = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": transfer.id}),
            content_type="application/json",
        )
        self.assertEqual(unsupported.status_code, 409)

        draft = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": expense.id}),
            content_type="application/json",
        ).json()["draft"]
        Transaction.all_objects.filter(id=expense.id).update(comment="Changed elsewhere")
        stale = self.client.post(
            "/app/api/history/void/confirm",
            data=json.dumps({"draft_id": draft["draft_id"], "idempotency_key": "history-void-stale-0001"}),
            content_type="application/json",
        )
        self.assertEqual(stale.status_code, 409)
        account.refresh_from_db()
        self.assertEqual(account.balance, Decimal("900.00"))
        self.assertTrue(Transaction.objects.filter(id=expense.id).exists())

    def test_history_void_family_member_can_cancel_only_authored_transaction(self):
        self._grant_full_access()
        owner = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Owner",
            username="owner",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        now = timezone.now()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO families (id, name, owner_user_id, status, created_at, updated_at)
                VALUES (77, %s, %s, 'active', %s, %s)
                """,
                ["Family", owner.tg_user_id, now, now],
            )
            cursor.execute(
                """
                INSERT INTO family_members (
                  family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at
                ) VALUES
                  (77, %s, 'owner', 'active', %s, %s, %s, %s),
                  (77, %s, 'member', 'active', %s, %s, %s, %s)
                """,
                [
                    owner.tg_user_id, owner.tg_user_id, now, now, now,
                    self.user.tg_user_id, owner.tg_user_id, now, now, now,
                ],
            )
        account = Account.objects.create(
            tg_user=owner,
            family_id=77,
            label="Family account",
            currency="UAH",
            account_type="main",
            non_negative_account_type="main",
            starting_balance=Decimal("0"),
            balance=Decimal("800"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        other_transaction = Transaction.objects.create(
            tg_user=owner,
            family_id=77,
            created_by_user_id=owner.tg_user_id,
            date=timezone.localdate(),
            type="expense",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            account=account,
            flow_kind="normal",
            created_at=now,
        )
        own_transaction = Transaction.objects.create(
            tg_user=owner,
            family_id=77,
            created_by_user_id=self.user.tg_user_id,
            date=timezone.localdate(),
            type="expense",
            amount=Decimal("100"),
            currency="UAH",
            source="manual",
            account=account,
            flow_kind="normal",
            created_at=now,
        )
        self.client.post("/app/api/auth/dev")
        forbidden = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": other_transaction.id}),
            content_type="application/json",
        )
        allowed = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": own_transaction.id}),
            content_type="application/json",
        )
        self.assertEqual(forbidden.status_code, 403)
        self.assertEqual(allowed.status_code, 200)
        with connection.cursor() as cursor:
            cursor.execute("UPDATE family_members SET role='member' WHERE family_id=77 AND user_id=%s", [owner.tg_user_id])
            cursor.execute("UPDATE family_members SET role='owner' WHERE family_id=77 AND user_id=%s", [self.user.tg_user_id])
            cursor.execute("UPDATE families SET owner_user_id=%s WHERE id=77", [self.user.tg_user_id])
        owner_allowed = self.client.post(
            "/app/api/history/void/draft",
            data=json.dumps({"transaction_id": other_transaction.id}),
            content_type="application/json",
        )
        self.assertEqual(owner_allowed.status_code, 200)

    def test_transaction_draft_rejects_currency_mismatch(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("1000"), currency="UAH")
        category = self._create_category(kind="expense", name="Food")
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.post(
            "/app/api/transactions/draft",
            data=json.dumps(
                {
                    "kind": "expense",
                    "amount": "20",
                    "account_id": account.id,
                    "category_id": category.id,
                    "currency": "USD",
                    "transaction_date": timezone.localdate().isoformat(),
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "currency_account_mismatch")
        self.assertEqual(Transaction.objects.count(), 0)

    def test_account_create_draft_does_not_write_until_confirm(self):
        self._grant_full_access()
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        draft_response = self.client.post(
            "/app/api/accounts/draft",
            data=json.dumps(
                {
                    "action": "create",
                    "label": "Cash reserve",
                    "currency": "USD",
                    "account_type": "savings",
                    "starting_balance": "125.25",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(draft_response.status_code, 200)
        self.assertEqual(Account.objects.count(), 0)
        draft_id = draft_response.json()["draft"]["draft_id"]
        confirm_response = self.client.post(
            "/app/api/accounts/confirm",
            data=json.dumps({"draft_id": draft_id, "idempotency_key": "account-create-123456"}),
            content_type="application/json",
        )

        self.assertEqual(confirm_response.status_code, 200)
        account = Account.objects.get()
        self.assertEqual(account.label, "Cash reserve")
        self.assertEqual(account.balance, Decimal("125.25"))
        self.assertEqual(account.account_type, "savings")

    def test_transfer_confirm_moves_balances_once(self):
        self._grant_full_access()
        source = self._create_account(balance=Decimal("1000"))
        target = Account.objects.create(
            tg_user=self.user,
            label="Cash",
            currency="UAH",
            account_type="cash",
            starting_balance=Decimal("50"),
            balance=Decimal("50"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        draft_response = self.client.post(
            "/app/api/transfers/draft",
            data=json.dumps(
                {
                    "source_account_id": source.id,
                    "target_account_id": target.id,
                    "amount": "325.50",
                    "transaction_date": timezone.localdate().isoformat(),
                    "comment": "Reserve top-up",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(draft_response.status_code, 200)
        self.assertEqual(Transaction.objects.count(), 0)
        draft_id = draft_response.json()["draft"]["draft_id"]

        confirm_payload = json.dumps({"draft_id": draft_id, "idempotency_key": "transfer-confirm-123456"})
        first_response = self.client.post("/app/api/transfers/confirm", data=confirm_payload, content_type="application/json")
        second_response = self.client.post("/app/api/transfers/confirm", data=confirm_payload, content_type="application/json")

        self.assertEqual(first_response.status_code, 200)
        self.assertFalse(first_response.json()["idempotent"])
        self.assertTrue(second_response.json()["idempotent"])
        source.refresh_from_db()
        target.refresh_from_db()
        self.assertEqual(source.balance, Decimal("674.50"))
        self.assertEqual(target.balance, Decimal("375.50"))
        transaction = Transaction.objects.get()
        self.assertEqual(transaction.flow_kind, "transfer")
        self.assertEqual(transaction.type, "transfer")
        self.assertEqual(transaction.source, "miniapp_transfer")
        self.assertEqual(WriteReceipt.objects.count(), 1)

    def test_cross_currency_transfer_requires_manual_fx_rate(self):
        self._grant_full_access()
        source = self._create_account(balance=Decimal("1000"), currency="UAH")
        target = Account.objects.create(
            tg_user=self.user,
            label="Dollar account",
            currency="USD",
            account_type="main",
            starting_balance=Decimal("0"),
            balance=Decimal("0"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")

        response = self.client.post(
            "/app/api/transfers/draft",
            data=json.dumps({"source_account_id": source.id, "target_account_id": target.id, "amount": "400"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "fx_rate_required")
        self.assertEqual(Transaction.objects.count(), 0)

    def test_debt_create_and_repayment_require_confirm_and_are_idempotent(self):
        self._grant_full_access()
        account = self._create_account(balance=Decimal("1000"))
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        create_draft_response = self.client.post(
            "/app/api/debts/draft",
            data=json.dumps(
                {
                    "action": "create",
                    "direction": "receivable",
                    "counterparty_name": "Olena",
                    "amount": "200",
                    "currency": "UAH",
                    "account_id": account.id,
                    "transaction_date": timezone.localdate().isoformat(),
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(create_draft_response.status_code, 200)
        self.assertEqual(Debt.objects.count(), 0)
        self.assertEqual(Transaction.objects.count(), 0)

        create_draft_id = create_draft_response.json()["draft"]["draft_id"]
        create_response = self.client.post(
            "/app/api/debts/confirm",
            data=json.dumps({"draft_id": create_draft_id, "idempotency_key": "debt-create-confirm-0001"}),
            content_type="application/json",
        )
        self.assertEqual(create_response.status_code, 200)
        debt = Debt.objects.get()
        account.refresh_from_db()
        self.assertEqual(debt.remaining_amount, Decimal("200.00"))
        self.assertEqual(account.balance, Decimal("800.00"))
        self.assertEqual(Transaction.objects.get().flow_kind, "debt")

        repay_draft_response = self.client.post(
            "/app/api/debts/draft",
            data=json.dumps(
                {
                    "action": "repay",
                    "debt_id": debt.id,
                    "amount": "60",
                    "account_id": account.id,
                    "transaction_date": timezone.localdate().isoformat(),
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(repay_draft_response.status_code, 200)
        self.assertEqual(DebtPayment.objects.count(), 0)
        repay_draft_id = repay_draft_response.json()["draft"]["draft_id"]
        confirm_payload = json.dumps({"draft_id": repay_draft_id, "idempotency_key": "debt-repay-confirm-0001"})
        first_response = self.client.post("/app/api/debts/confirm", data=confirm_payload, content_type="application/json")
        second_response = self.client.post("/app/api/debts/confirm", data=confirm_payload, content_type="application/json")

        self.assertEqual(first_response.status_code, 200)
        self.assertFalse(first_response.json()["idempotent"])
        self.assertTrue(second_response.json()["idempotent"])
        debt.refresh_from_db()
        account.refresh_from_db()
        self.assertEqual(debt.paid_amount, Decimal("60.00"))
        self.assertEqual(debt.remaining_amount, Decimal("140.00"))
        self.assertEqual(account.balance, Decimal("860.00"))
        self.assertEqual(DebtPayment.objects.count(), 1)
        self.assertEqual(Transaction.objects.filter(flow_kind="debt").count(), 2)
        self.assertEqual(WriteReceipt.objects.count(), 2)

        edit_draft_response = self.client.post(
            "/app/api/debts/draft",
            data=json.dumps(
                {
                    "action": "edit",
                    "debt_id": debt.id,
                    "counterparty_name": "Olena Updated",
                    "amount": "250",
                    "currency": "UAH",
                    "comment": "Extended debt",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(edit_draft_response.status_code, 200)
        edit_draft_id = edit_draft_response.json()["draft"]["draft_id"]
        edit_payload = json.dumps({"draft_id": edit_draft_id, "idempotency_key": "debt-edit-confirm-00001"})
        edit_response = self.client.post("/app/api/debts/confirm", data=edit_payload, content_type="application/json")
        edit_retry_response = self.client.post("/app/api/debts/confirm", data=edit_payload, content_type="application/json")

        self.assertEqual(edit_response.status_code, 200)
        self.assertFalse(edit_response.json()["idempotent"])
        self.assertTrue(edit_retry_response.json()["idempotent"])
        debt.refresh_from_db()
        self.assertEqual(debt.counterparty_name, "Olena Updated")
        self.assertEqual(debt.initial_amount, Decimal("250.00"))
        self.assertEqual(debt.remaining_amount, Decimal("190.00"))

        close_draft_response = self.client.post(
            "/app/api/debts/draft",
            data=json.dumps({"action": "close", "debt_id": debt.id}),
            content_type="application/json",
        )
        self.assertEqual(close_draft_response.status_code, 200)
        close_draft_id = close_draft_response.json()["draft"]["draft_id"]
        close_payload = json.dumps({"draft_id": close_draft_id, "idempotency_key": "debt-close-confirm-001"})
        close_response = self.client.post("/app/api/debts/confirm", data=close_payload, content_type="application/json")
        close_retry_response = self.client.post("/app/api/debts/confirm", data=close_payload, content_type="application/json")

        self.assertEqual(close_response.status_code, 200)
        self.assertFalse(close_response.json()["idempotent"])
        self.assertTrue(close_retry_response.json()["idempotent"])
        debt.refresh_from_db()
        self.assertEqual(debt.status, "closed")
        self.assertIsNotNone(debt.closed_at)
        self.assertEqual(WriteReceipt.objects.count(), 4)

    def test_bootstrap_excludes_transfers_from_expenses(self):
        self._grant_full_access()
        account = Account.objects.create(
            tg_user=self.user,
            label="Mono Black",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("84320"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        target_day = timezone.localdate()
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="income",
            amount=Decimal("50000"),
            currency="UAH",
            source="text",
            account=account,
            flow_kind="normal",
            category_name_snapshot="Зарплата",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="expense",
            amount=Decimal("1240"),
            currency="UAH",
            source="text",
            account=account,
            flow_kind="normal",
            category_name_snapshot="Продукти",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="transfer",
            amount=Decimal("999"),
            currency="UAH",
            source="text",
            from_account=account,
            to_account=account,
            flow_kind="transfer",
            created_at=timezone.now(),
        )
        Debt.objects.create(
            tg_user=self.user,
            counterparty_name="Alice",
            direction="receivable",
            initial_amount=Decimal("400"),
            paid_amount=Decimal("0"),
            remaining_amount=Decimal("400"),
            currency="UAH",
            account=account,
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["overview"]["expenses"]["total"]["value"], "1240.00")
        self.assertEqual(payload["overview"]["net_flow"]["value"], "48760.00")
        self.assertEqual(payload["activity_preview"]["items"][0]["type_code"], "transfer")
        self.assertEqual(payload["user"]["lang"], "uk")
        self.assertEqual(payload["activity_preview"]["items"][0]["type_badge"], "Переказ")
        self.assertEqual(payload["debts_preview"]["summary"]["owed_to_user"]["value"], "400.00")

    def test_bootstrap_marks_empty_activity_as_period_specific_when_history_exists(self):
        self._grant_full_access()
        account = Account.objects.create(
            tg_user=self.user,
            label="Mono Black",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("5000"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        target_day = timezone.localdate()
        previous_day = target_day - timedelta(days=1)
        Transaction.objects.create(
            tg_user=self.user,
            date=previous_day,
            type="expense",
            amount=Decimal("250"),
            currency="UAH",
            source="text",
            account=account,
            flow_kind="normal",
            category_name_snapshot="Groceries",
            created_at=timezone.now(),
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["activity_preview"]["items"], [])
        self.assertEqual(payload["activity_preview"]["state"]["mode"], "empty")
        self.assertEqual(payload["activity_preview"]["state"]["reason"], "no_activity_in_period")

    def test_bootstrap_returns_income_comparison_for_previous_comparable_period(self):
        self._grant_full_access()
        account = Account.objects.create(
            tg_user=self.user,
            label="Mono Black",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("5000"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        target_day = timezone.localdate()
        previous_day = target_day - timedelta(days=1)
        Transaction.objects.create(
            tg_user=self.user,
            date=previous_day,
            type="income",
            amount=Decimal("50"),
            currency="UAH",
            source="text",
            account=account,
            flow_kind="normal",
            category_name_snapshot="Salary",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="income",
            amount=Decimal("100"),
            currency="UAH",
            source="text",
            account=account,
            flow_kind="normal",
            category_name_snapshot="Salary",
            created_at=timezone.now(),
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["flow_preview"]["income_total"]["value"], "100.00")
        self.assertEqual(payload["flow_preview"]["income_vs_previous_percent"], 100)

    def test_bootstrap_includes_credit_cards_preview_and_excludes_credit_from_asset_total(self):
        self._grant_full_access()
        now = timezone.now()
        Account.objects.create(
            tg_user=self.user,
            label="Main account",
            currency="UAH",
            account_type="main",
            starting_balance=Decimal("0"),
            balance=Decimal("1000"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        Account.objects.create(
            tg_user=self.user,
            label="Mono Credit",
            currency="UAH",
            account_type="credit",
            starting_balance=Decimal("0"),
            balance=Decimal("-250"),
            credit_limit=Decimal("5000"),
            monthly_interest_rate=Decimal("3.5"),
            non_negative_account_type="main",
            is_active=True,
            created_at=now,
            updated_at=now,
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["overview"]["total_balance"]["value"], "1000.00")
        self.assertEqual(payload["accounts_preview"]["total_balance"]["value"], "1000.00")
        self.assertEqual(payload["credit_cards_preview"]["summary"]["total_debt"]["value"], "250.00")
        self.assertEqual(payload["credit_cards_preview"]["summary"]["cards_count"], 1)
        self.assertEqual(payload["credit_cards_preview"]["items"][0]["label"], "Mono Credit")

    def test_bootstrap_current_debt_includes_active_payables_without_changing_asset_total(self):
        self._grant_full_access()
        now = timezone.now()
        Account.objects.create(
            tg_user=self.user,
            label="Main account",
            currency="UAH",
            account_type="main",
            starting_balance=Decimal("0"),
            balance=Decimal("179092"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        for counterparty_name, amount, direction, status in (
            ("Person", "10000", "payable", "active"),
            ("Bank", "80000", "payable", "active"),
            ("Friend", "5000", "receivable", "active"),
            ("Closed loan", "7000", "payable", "closed"),
        ):
            Debt.objects.create(
                tg_user=self.user,
                counterparty_name=counterparty_name,
                direction=direction,
                initial_amount=Decimal(amount),
                paid_amount=Decimal("0"),
                remaining_amount=Decimal(amount),
                currency="UAH",
                status=status,
                created_at=now,
                updated_at=now,
            )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["overview"]["total_balance"]["value"], "179092.00")
        self.assertEqual(payload["credit_cards_preview"]["summary"]["total_debt"]["value"], "0.00")
        self.assertEqual(payload["debts_preview"]["summary"]["owed_by_user"]["value"], "90000.00")
        self.assertEqual(payload["overview"]["current_debt"]["value"], "90000.00")

    def test_family_member_sees_family_dashboard_instead_of_personal_data(self):
        owner = TelegramUser.objects.create(
            tg_user_id=2002,
            first_name="Owner",
            username="owner",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        Subscription.objects.create(
            user=owner,
            status=Subscription.Status.ACTIVE,
            source=Subscription.Source.ADMIN,
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
        )
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.PAID,
                "access_scope": UserAdminState.AccessScope.FAMILY_FULL,
                "access_source": "family",
                "is_blocked": False,
            },
        )
        now = timezone.now()
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO families (id, name, owner_user_id, status, created_at, updated_at)
                VALUES (1, %s, %s, 'active', %s, %s)
                """,
                ["Main family", owner.tg_user_id, now, now],
            )
            cursor.execute(
                """
                INSERT INTO family_members (
                  family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at
                )
                VALUES
                  (1, %s, 'owner', 'active', %s, %s, %s, %s),
                  (1, %s, 'member', 'active', %s, %s, %s, %s)
                """,
                [
                    owner.tg_user_id,
                    owner.tg_user_id,
                    now,
                    now,
                    now,
                    self.user.tg_user_id,
                    owner.tg_user_id,
                    now,
                    now,
                    now,
                ],
            )

        family_account = Account.objects.create(
            tg_user=owner,
            family_id=1,
            label="Owner family card",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("10000"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        Account.objects.create(
            tg_user=self.user,
            label="Member personal test",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("333"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        target_day = timezone.localdate()
        Transaction.objects.create(
            tg_user=owner,
            family_id=1,
            date=target_day,
            type="expense",
            amount=Decimal("700"),
            currency="UAH",
            source="text",
            account=family_account,
            flow_kind="normal",
            category_name_snapshot="Family groceries",
            created_at=now,
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="expense",
            amount=Decimal("9999"),
            currency="UAH",
            source="text",
            flow_kind="normal",
            category_name_snapshot="Personal test expense",
            created_at=now,
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["access"]["mode"], "active")
        self.assertEqual(payload["overview"]["total_balance"]["value"], "10000.00")
        self.assertEqual(payload["overview"]["expenses"]["total"]["value"], "700.00")
        self.assertEqual(payload["accounts_preview"]["items"][0]["label"], "Owner family card")
        self.assertEqual(payload["activity_preview"]["items"][0]["title"], "Family groceries")

    def test_grace_user_gets_read_only_dashboard_access(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.EXPIRED,
                "access_scope": UserAdminState.AccessScope.PAYWALL,
                "access_source": "billing",
                "is_blocked": False,
            },
        )
        Subscription.objects.create(
            user=self.user,
            plan="solo",
            status=Subscription.Status.EXPIRED,
            provider="monobank",
            started_at=timezone.now() - timedelta(days=40),
            expires_at=timezone.now() - timedelta(days=1),
            grace_expires_at=timezone.now() + timedelta(days=7),
            trial_days=30,
        )

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["access"]["mode"], "grace_read_only")

    def test_paywall_user_gets_blocked_response(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.NONE,
                "access_scope": UserAdminState.AccessScope.PAYWALL,
                "access_source": "billing",
                "is_blocked": False,
            },
        )

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 403)
        payload = response.json()
        self.assertEqual(payload["error"]["code"], "access_blocked")
        self.assertEqual(payload["error"]["message"], "Доступ до кабінету ще не активовано.")
        self.assertEqual(payload["access"]["mode"], "blocked")
        self.assertFalse(payload["access"]["has_card"])
        self.assertEqual(payload["access"]["recommended_action"], "start_trial")
        self.assertIn("Спробуй кабінет", payload["access"]["entry_title"])
        self.assertTrue(payload["access"]["bind_amount_label"])
        self.assertTrue(payload["access"]["monthly_price_label"])

    def test_paywall_user_with_bound_card_gets_resume_metadata(self):
        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.NONE,
                "access_scope": UserAdminState.AccessScope.PAYWALL,
                "access_source": "billing",
                "is_blocked": False,
            },
        )
        BillingProfile.objects.create(
            user=self.user,
            provider="monobank",
            wallet_id="wallet-1001",
            card_token="tok_1001",
            status=BillingProfile.Status.ACTIVE,
        )

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 403)
        payload = response.json()
        self.assertEqual(payload["access"]["mode"], "blocked")
        self.assertTrue(payload["access"]["has_card"])
        self.assertEqual(payload["access"]["card_status"], "bound")
        self.assertEqual(payload["access"]["recommended_action"], "resume_subscription")
        self.assertEqual(payload["access"]["entry_title"], "Віднови доступ до кабінету")

    @patch("miniapp.services.get_latest_rates")
    def test_bootstrap_uses_estimated_base_currency_totals_for_multi_currency_sections(self, mock_get_latest_rates):
        self._grant_full_access()
        target_day = timezone.localdate()
        previous_day = target_day - timedelta(days=1)
        rates_to_uah = {
            "UAH": Decimal("1"),
            "USD": Decimal("40"),
            "EUR": Decimal("43"),
            "TRY": Decimal("1.2"),
        }

        def snapshot_for(base_currency):
            base = str(base_currency).upper()
            uah_per_base = rates_to_uah[base]
            return FxSnapshot(
                base=base,
                date="20.05.2026",
                rates={code: rate / uah_per_base for code, rate in rates_to_uah.items()},
                fetched_at=0.0,
            )

        mock_get_latest_rates.side_effect = snapshot_for

        uah_account = Account.objects.create(
            tg_user=self.user,
            label="Privat UAH",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("1188"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        try_account = Account.objects.create(
            tg_user=self.user,
            label="TRY Cash",
            currency="TRY",
            account_type="cash",
            starting_balance=Decimal("0"),
            balance=Decimal("1500"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        eur_account = Account.objects.create(
            tg_user=self.user,
            label="EUR Cash",
            currency="EUR",
            account_type="cash",
            starting_balance=Decimal("0"),
            balance=Decimal("700"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        usd_account = Account.objects.create(
            tg_user=self.user,
            label="USD Cash",
            currency="USD",
            account_type="cash",
            starting_balance=Decimal("0"),
            balance=Decimal("400"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        investment_account = Account.objects.create(
            tg_user=self.user,
            label="Broker USD",
            currency="USD",
            account_type="investment",
            starting_balance=Decimal("0"),
            balance=Decimal("50"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="income",
            amount=Decimal("100"),
            currency="UAH",
            source="text",
            account=uah_account,
            flow_kind="normal",
            category_name_snapshot="Salary",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="income",
            amount=Decimal("10"),
            currency="USD",
            source="text",
            account=usd_account,
            flow_kind="normal",
            category_name_snapshot="Client payment",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="expense",
            amount=Decimal("50"),
            currency="TRY",
            source="text",
            account=try_account,
            flow_kind="normal",
            category_name_snapshot="Groceries",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="expense",
            amount=Decimal("10"),
            currency="EUR",
            source="text",
            account=eur_account,
            flow_kind="normal",
            category_name_snapshot="Travel",
            created_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=previous_day,
            type="expense",
            amount=Decimal("5"),
            currency="USD",
            source="text",
            account=usd_account,
            flow_kind="normal",
            category_name_snapshot="Travel",
            created_at=timezone.now(),
        )
        Debt.objects.create(
            tg_user=self.user,
            counterparty_name="Alice",
            direction="receivable",
            initial_amount=Decimal("100"),
            paid_amount=Decimal("0"),
            remaining_amount=Decimal("100"),
            currency="USD",
            account=usd_account,
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Debt.objects.create(
            tg_user=self.user,
            counterparty_name="Bob",
            direction="payable",
            initial_amount=Decimal("50"),
            paid_amount=Decimal("0"),
            remaining_amount=Decimal("50"),
            currency="EUR",
            account=eur_account,
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["access"]["mode"], "active")
        self.assertEqual(payload["overview"]["total_balance"]["value"], "51088.00")
        self.assertTrue(payload["overview"]["total_balance"]["display"].startswith("~ "))
        self.assertIn("EUR", payload["overview"]["total_balance"]["note"])
        self.assertEqual(payload["overview"]["usd_equivalent"]["value"], "1277.20")
        self.assertEqual(payload["overview"]["usd_equivalent"]["currency"], "USD")
        self.assertTrue(payload["overview"]["usd_equivalent"]["display"].startswith("~ "))
        self.assertEqual(payload["accounts_preview"]["total_balance"]["value"], "51088.00")
        self.assertTrue(payload["accounts_preview"]["total_balance"]["display"].startswith("~ "))
        self.assertEqual(payload["accounts_preview"]["usd_equivalent"]["value"], "1277.20")
        self.assertTrue(payload["accounts_preview"]["usd_equivalent"]["display"].startswith("~ "))

        self.assertEqual(payload["overview"]["expenses"]["total"]["value"], "490.00")
        self.assertTrue(payload["overview"]["expenses"]["total"]["display"].startswith("~ "))
        self.assertEqual(payload["overview"]["expenses"]["vs_previous_percent"], 145)
        self.assertEqual(payload["overview"]["net_flow"]["value"], "10.00")
        self.assertTrue(payload["overview"]["net_flow"]["display"].startswith("~ "))
        self.assertEqual(payload["flow_preview"]["state"]["reason"], "fx_estimated")

        self.assertEqual(payload["categories_preview"]["total"]["value"], "490.00")
        self.assertTrue(payload["categories_preview"]["total"]["display"].startswith("~ "))
        self.assertEqual(payload["categories_preview"]["items"][0]["name"], "Travel")
        self.assertEqual(payload["categories_preview"]["items"][0]["amount"]["value"], "430.00")
        self.assertEqual(payload["categories_preview"]["items"][0]["vs_previous_percent"], 115)
        self.assertTrue(payload["categories_preview"]["items"][0]["comparison_available"])

        self.assertEqual(payload["debts_preview"]["summary"]["owed_to_user"]["value"], "4000.00")
        self.assertEqual(payload["debts_preview"]["summary"]["owed_by_user"]["value"], "2150.00")
        self.assertTrue(payload["debts_preview"]["summary"]["owed_to_user"]["display"].startswith("~ "))
        self.assertEqual(payload["overview"]["current_debt"]["value"], "2150.00")
        self.assertTrue(payload["overview"]["current_debt"]["display"].startswith("~ "))

        self.assertEqual(payload["investments_preview"]["summary"]["current_value"]["value"], "2000.00")
        self.assertTrue(payload["investments_preview"]["summary"]["current_value"]["display"].startswith("~ "))
        self.assertIn("Поки що показуємо лише поточний стан", payload["investments_preview"]["state"]["note"])

        self.assertGreaterEqual(mock_get_latest_rates.call_count, 1)
        self.assertEqual(investment_account.currency, "USD")

    @patch("miniapp.services.get_latest_rates", side_effect=RuntimeError("fx-down"))
    def test_bootstrap_marks_multi_currency_sections_unavailable_when_fx_fails(self, _mock_get_latest_rates):
        self._grant_full_access()
        target_day = timezone.localdate()

        usd_account = Account.objects.create(
            tg_user=self.user,
            label="USD Cash",
            currency="USD",
            account_type="cash",
            starting_balance=Decimal("0"),
            balance=Decimal("25"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Account.objects.create(
            tg_user=self.user,
            label="UAH Card",
            currency="UAH",
            account_type="card",
            starting_balance=Decimal("0"),
            balance=Decimal("100"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Account.objects.create(
            tg_user=self.user,
            label="USD Broker",
            currency="USD",
            account_type="investment",
            starting_balance=Decimal("0"),
            balance=Decimal("5"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user=self.user,
            date=target_day,
            type="expense",
            amount=Decimal("10"),
            currency="USD",
            source="text",
            account=usd_account,
            flow_kind="normal",
            category_name_snapshot="Travel",
            created_at=timezone.now(),
        )
        Debt.objects.create(
            tg_user=self.user,
            counterparty_name="Alice",
            direction="receivable",
            initial_amount=Decimal("100"),
            paid_amount=Decimal("0"),
            remaining_amount=Decimal("100"),
            currency="USD",
            account=usd_account,
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get(
            "/app/api/bootstrap",
            {
                "preset": "custom",
                "date_from": target_day.isoformat(),
                "date_to": target_day.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["overview"]["total_balance"]["display"], "-")
        self.assertIsNone(payload["overview"]["total_balance"]["value"])
        self.assertEqual(payload["overview"]["usd_equivalent"]["display"], "-")
        self.assertIsNone(payload["overview"]["usd_equivalent"]["value"])
        self.assertEqual(payload["accounts_preview"]["state"]["reason"], "fx_unavailable")
        self.assertIn("USD", payload["accounts_preview"]["state"]["note"])
        self.assertEqual(payload["accounts_preview"]["usd_equivalent"]["display"], "-")
        self.assertIsNone(payload["accounts_preview"]["usd_equivalent"]["value"])

        self.assertEqual(payload["overview"]["net_flow"]["display"], "-")
        self.assertIsNone(payload["overview"]["net_flow"]["value"])
        self.assertEqual(payload["flow_preview"]["state"]["reason"], "fx_unavailable")

        self.assertEqual(payload["categories_preview"]["state"]["reason"], "fx_unavailable")
        self.assertEqual(payload["categories_preview"]["total"]["display"], "-")
        self.assertEqual(payload["categories_preview"]["items"], [])

        self.assertEqual(payload["debts_preview"]["state"]["reason"], "fx_unavailable")
        self.assertEqual(payload["debts_preview"]["summary"]["owed_to_user"]["display"], "-")

        self.assertEqual(payload["investments_preview"]["state"]["reason"], "fx_unavailable")
        self.assertEqual(payload["investments_preview"]["summary"]["current_value"]["display"], "-")

    def test_bootstrap_returns_direct_usd_equivalent_for_usd_base_accounts(self):
        self._grant_full_access()
        self.user.base_currency = "USD"
        self.user.save(update_fields=["base_currency"])
        now = timezone.now()
        Account.objects.create(
            tg_user=self.user,
            label="USD Wallet",
            currency="USD",
            account_type="cash",
            starting_balance=Decimal("0"),
            balance=Decimal("125.50"),
            is_active=True,
            created_at=now,
            updated_at=now,
        )

        auth_response = self.client.post("/app/api/auth/dev", data="{}", content_type="application/json")
        self.assertEqual(auth_response.status_code, 200)

        response = self.client.get("/app/api/bootstrap", {"preset": "this_month"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["overview"]["total_balance"]["currency"], "USD")
        self.assertEqual(payload["overview"]["usd_equivalent"]["value"], "125.50")
        self.assertEqual(payload["overview"]["usd_equivalent"]["currency"], "USD")
        self.assertFalse(payload["overview"]["usd_equivalent"]["is_estimated"])
        self.assertIsNone(payload["overview"]["usd_equivalent"]["note"])
        self.assertEqual(payload["accounts_preview"]["usd_equivalent"]["value"], "125.50")
        self.assertFalse(payload["accounts_preview"]["usd_equivalent"]["is_estimated"])
@override_settings(
    DEBUG=True,
    MINIAPP_DEV_TG_USER_ID=9901,
    TELEGRAM_BOT_TOKEN="123456:test-miniapp-token",
    PUSH_INTERNAL_TOKEN="push-test-token",
    WEB_PUSH_VAPID_PUBLIC_KEY="BEl0dGVzdC1wdWJsaWMta2V5",
    WEB_PUSH_VAPID_PRIVATE_KEY="",
)
class MiniAppPushTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()

    def setUp(self):
        self.client = Client()
        self.user = TelegramUser.objects.create(
            tg_user_id=9901,
            first_name="Push",
            username="push_user",
            lang="uk",
            base_currency="UAH",
            onboarding_completed=True,
            onboarding_version=3,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        self.assertEqual(self.client.post("/app/api/auth/dev").status_code, 200)

    def test_subscription_inbox_read_and_preferences_flow(self):
        subscription = self.client.post(
            "/app/api/notifications/subscribe",
            data=json.dumps(
                {
                    "endpoint": "https://push.example.test/subscription-1",
                    "keys": {"p256dh": "public-key", "auth": "auth-secret"},
                    "platform": "ios",
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(subscription.status_code, 200)
        self.assertTrue(WebPushSubscription.objects.filter(tg_user_id=self.user.tg_user_id, is_active=True).exists())

        emitted = self.client.post(
            "/internal/push/event",
            data=json.dumps(
                {
                    "user_id": self.user.tg_user_id,
                    "event_type": "daily_expense_missing",
                    "idempotency_key": "daily-expense:2026-07-19",
                    "context": {"day": "2026-07-19"},
                }
            ),
            content_type="application/json",
            HTTP_X_INTERNAL_TOKEN="push-test-token",
        )
        self.assertEqual(emitted.status_code, 200)
        notice = AppNotification.objects.get(tg_user_id=self.user.tg_user_id)

        status = self.client.get("/app/api/notifications")
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["badge_count"], 1)
        self.assertEqual(status.json()["notifications"][0]["title"], "Закриємо день?")

        device_status = self.client.get(
            "/app/api/notifications",
            {"endpoint": "https://push.example.test/subscription-1"},
        )
        self.assertTrue(device_status.json()["device_subscription_active"])
        other_device_status = self.client.get(
            "/app/api/notifications",
            {"endpoint": "https://push.example.test/another-device"},
        )
        self.assertFalse(other_device_status.json()["device_subscription_active"])

        read = self.client.post(
            "/app/api/notifications/read",
            data=json.dumps({"notification_id": notice.pk}),
            content_type="application/json",
        )
        self.assertEqual(read.status_code, 200)
        self.assertEqual(read.json()["badge_count"], 0)

        preferences = self.client.post(
            "/app/api/notifications/preferences",
            data=json.dumps({"weekly_summary_enabled": True, "show_sensitive_details": True}),
            content_type="application/json",
        )
        self.assertEqual(preferences.status_code, 200)
        saved = NotificationPreference.objects.get(tg_user_id=self.user.tg_user_id)
        self.assertTrue(saved.weekly_summary_enabled)
        self.assertTrue(saved.show_sensitive_details)

    def test_service_worker_contains_push_click_and_badge_handlers(self):
        response = self.client.get("/app/sw.js")
        self.assertEqual(response.status_code, 200)
        script = response.content.decode("utf-8")
        self.assertIn('addEventListener("push"', script)
        self.assertIn('addEventListener("notificationclick"', script)
        self.assertIn("setAppBadge", script)
