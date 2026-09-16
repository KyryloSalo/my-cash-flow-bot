from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.db import connection
from django.test import Client
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone

from accounts.models import Account, AccountAdminState
from audit_log.models import AdminAuditLog
from bot_events.models import BotEvent
from broadcasts.models import AdminMessageLog
from categories.models import Category
from common.admin_site import admin_site
from common.test_helpers import ensure_runtime_finance_tables, ensure_telegram_user_table
from feedback.models import FeedbackItem
from polls.models import PollCampaign, PollRecipient, PollResponse
from subscriptions.models import BillingProfile, Payment, Plan, PromoOffer, PromoOfferClaim, Subscription, SubscriptionEvent
from support.models import SupportCase, SupportMessage
from transactions.models import Debt, DebtPayment, Transaction
from users.admin import TelegramUserAdmin
from users.models import AdminNote, PushTopic, Tag, TelegramUser, UserAdminState, UserPushTopic, UserTag
from users.services import (
    HardDeleteUserError,
    REPLAY_RESET_CONFIRMATION,
    ResetOnboardingError,
    assign_tag_to_users,
    get_or_create_user_tag,
    hard_delete_user,
    mark_user_as_test_user,
    remove_tag_from_users,
    reset_user_onboarding,
    unmark_user_as_test_user,
)


class _PreviewRows(list):
    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self


def ensure_hard_delete_runtime_tables() -> None:
    existing = set(connection.introspection.table_names())
    with connection.cursor() as cursor:
        if "families" not in existing:
            cursor.execute(
                """
                CREATE TABLE families (
                  id BIGSERIAL PRIMARY KEY,
                  name TEXT NOT NULL DEFAULT '',
                  owner_user_id BIGINT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        if "family_members" not in existing:
            cursor.execute(
                """
                CREATE TABLE family_members (
                  id BIGSERIAL PRIMARY KEY,
                  family_id BIGINT NOT NULL,
                  user_id BIGINT NOT NULL,
                  role TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'active',
                  invited_by_user_id BIGINT NULL,
                  joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  left_at TIMESTAMPTZ NULL,
                  removed_at TIMESTAMPTZ NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        if "family_invites" not in existing:
            cursor.execute(
                """
                CREATE TABLE family_invites (
                  id BIGSERIAL PRIMARY KEY,
                  family_id BIGINT NOT NULL,
                  created_by_user_id BIGINT NOT NULL,
                  used_by_user_id BIGINT NULL,
                  token_hash TEXT NOT NULL DEFAULT '',
                  status TEXT NOT NULL DEFAULT 'active',
                  max_uses INTEGER NOT NULL DEFAULT 1,
                  used_count INTEGER NOT NULL DEFAULT 0,
                  expires_at TIMESTAMPTZ NOT NULL,
                  used_at TIMESTAMPTZ NULL,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        if "ai_transaction_drafts" not in existing:
            cursor.execute(
                """
                CREATE TABLE ai_transaction_drafts (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NOT NULL,
                  source TEXT NOT NULL,
                  telegram_file_unique_id TEXT NOT NULL,
                  status TEXT NOT NULL DEFAULT 'pending'
                )
                """
            )
        if "saving_prompt_settings" not in existing:
            cursor.execute(
                """
                CREATE TABLE saving_prompt_settings (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NOT NULL UNIQUE
                )
                """
            )
        if "pending_saving_tasks" not in existing:
            cursor.execute(
                """
                CREATE TABLE pending_saving_tasks (
                  id BIGSERIAL PRIMARY KEY,
                  tg_user_id BIGINT NOT NULL,
                  source_account_id BIGINT NULL,
                  target_account_id BIGINT NULL,
                  amount NUMERIC(18,2) NOT NULL,
                  currency TEXT NOT NULL
                )
                """
            )
        if "debt_invites" not in existing:
            cursor.execute(
                """
                CREATE TABLE debt_invites (
                  id BIGSERIAL PRIMARY KEY,
                  debt_id BIGINT NULL,
                  token TEXT NOT NULL DEFAULT '',
                  created_by_user_id BIGINT NOT NULL,
                  used_by_user_id BIGINT NULL
                )
                """
            )

        cursor.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS family_id BIGINT NULL")
        cursor.execute("ALTER TABLE categories ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE categories ADD COLUMN IF NOT EXISTS family_id BIGINT NULL")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS created_by_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS updated_by_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS deleted_by_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE debts ADD COLUMN IF NOT EXISTS borrower_user_id BIGINT NULL")
        cursor.execute("ALTER TABLE debts ADD COLUMN IF NOT EXISTS family_id BIGINT NULL")
        cursor.execute("ALTER TABLE debt_payments ADD COLUMN IF NOT EXISTS family_id BIGINT NULL")


class UserAdminPreviewEscapingTests(SimpleTestCase):
    def setUp(self):
        self.user_admin = TelegramUserAdmin(TelegramUser, admin_site)
        self.user = SimpleNamespace(
            tg_user_id=555001,
            onboarding_completed=True,
            onboarding_version=2,
        )

    def test_user_registry_omits_private_finance_columns_and_previews(self):
        visible_fields = set(self.user_admin.list_display)
        readonly_fields = set(self.user_admin.readonly_fields)

        self.assertTrue(
            {
                "tg_user_id",
                "access_scope_display",
                "onboarding_status_display",
                "next_billing_at",
            }.isdisjoint(visible_fields)
        )
        self.assertTrue(
            {
                "transactions_count",
                "total_expenses",
                "total_income",
                "transactions_preview",
                "accounts_preview",
                "categories_preview",
                "onboarding_debug_preview",
            }.isdisjoint(readonly_fields)
        )

    def test_onboarding_debug_preview_escapes_payload_and_event_fields(self):
        self.user.admin_state = SimpleNamespace(
            current_fsm_state="<script>alert('fsm')</script>",
            onboarding_payload={"message": "<img src=x onerror=alert('payload')>"},
            last_user_input="<script>alert('input')</script>",
            last_bot_response="<script>alert('bot')</script>",
            last_parse_error="<script>alert('parse')</script>",
            last_onboarding_event_at=timezone.datetime(2026, 5, 11, 12, 0),
        )
        onboarding_events = _PreviewRows(
            [
                SimpleNamespace(
                    created_at=timezone.datetime(2026, 5, 11, 12, 30),
                    event_type="onboarding_failed",
                    source="<script>alert('source')</script>",
                    error_message="<script>alert('event')</script>",
                )
            ]
        )

        with patch("users.admin.BotEvent.objects.filter", return_value=onboarding_events):
            html = str(self.user_admin.onboarding_debug_preview(self.user))

        self.assertNotIn("<script>alert('fsm')</script>", html)
        self.assertNotIn("<img src=x onerror=alert('payload')>", html)
        self.assertNotIn("<script>alert('event')</script>", html)
        self.assertIn("&lt;script&gt;alert(&#x27;fsm&#x27;)&lt;/script&gt;", html)
        self.assertIn("&lt;img src=x onerror=alert(&#x27;payload&#x27;)&gt;", html)
        self.assertIn("&lt;script&gt;alert(&#x27;event&#x27;)&lt;/script&gt;", html)


class UserAdminToolsTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_telegram_user_table()
        ensure_runtime_finance_tables()
        ensure_hard_delete_runtime_tables()

    def setUp(self):
        self.client = Client()
        self.superuser = get_user_model().objects.create_superuser("admin", "admin@example.com", "pass12345")
        self.staff_user = get_user_model().objects.create_user("staff", "staff@example.com", "pass12345", is_staff=True)
        self.regular_user = get_user_model().objects.create_user("regular", "regular@example.com", "pass12345", is_staff=False)
        self.user = TelegramUser.objects.create(
            tg_user_id=555001,
            first_name="Ivan",
            last_name="Tester",
            username="ivan_tester",
            lang="uk",
            base_currency="UAH",
            start_date=date(2026, 5, 1),
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

    def test_admin_index_requires_staff_user(self):
        self.client.force_login(self.regular_user)
        response = self.client.get(reverse("admin:index"))
        self.assertIn(response.status_code, {302, 403})

    def test_superuser_can_open_admin_index(self):
        self.client.force_login(self.superuser)
        response = self.client.get(reverse("admin:index"))
        self.assertEqual(response.status_code, 200)

    def test_user_change_page_omits_recent_transactions_and_finance_summary(self):
        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Cash",
            currency="UAH",
            balance=Decimal("100.00"),
            starting_balance=Decimal("100.00"),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        created = []
        for idx in range(4):
            created.append(
                Transaction.objects.create(
                    tg_user_id=self.user.tg_user_id,
                    date=date.today(),
                    type="expense",
                    amount=Decimal(f"{idx + 1}.00"),
                    currency="UAH",
                    category_id=category.id,
                    account_id=account.id,
                    source="text",
                    comment=f"tx-{idx + 1}",
                    created_at=timezone.now() - timedelta(minutes=idx),
                )
            )

        self.client.force_login(self.superuser)
        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.user.pk]))

        self.assertEqual(response.status_code, 200)
        crm = response.context["crm"]
        self.assertNotIn("finance_rows", crm)
        self.assertNotIn("transactions", crm)
        for item in created:
            self.assertNotContains(response, item.comment)
        self.assertNotContains(response, "Фінанси")

    def test_user_change_page_shows_force_charge_action_when_billing_token_exists(self):
        Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.TRIAL,
            plan="solo",
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
            next_charge_at=timezone.now() + timedelta(days=30),
            trial_days=30,
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            wallet_id=f"mono-force-{self.user.tg_user_id}",
            card_token="retry-token",
            masked_pan="444455******1212",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )

        self.client.force_login(self.superuser)
        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.user.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("admin:users_telegramuser_force_charge", args=[self.user.pk]))
        self.assertContains(response, "Форснути списання зараз")

    def test_subscription_action_view_tolerates_missing_related_plan(self):
        class BrokenLatestSubscription:
            plan_ref_id = 999999
            plan = "solo"
            amount = Decimal("499.00")
            currency = "UAH"

            @property
            def plan_ref(self):
                raise Plan.DoesNotExist

        latest_qs = SimpleNamespace(order_by=lambda *args, **kwargs: SimpleNamespace(first=lambda: BrokenLatestSubscription()))
        fake_subscription = SimpleNamespace(
            pk=77,
            status=Subscription.Status.ACTIVE,
            expires_at=timezone.now() + timedelta(days=30),
        )

        self.client.force_login(self.superuser)
        with (
            patch("users.admin.Subscription.objects.filter", return_value=latest_qs),
            patch("users.admin.apply_subscription_change", return_value=fake_subscription) as apply_mock,
        ):
            response = self.client.post(
                reverse("admin:users_telegramuser_subscription_action", args=[self.user.pk, "extend"]),
                data={"days": "30", "comment": "extend despite stale plan"},
                follow=False,
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("admin:users_telegramuser_change", args=[self.user.pk]))
        apply_mock.assert_called_once()
        self.assertEqual(apply_mock.call_args.kwargs["plan_ref"], None)
        self.assertEqual(apply_mock.call_args.kwargs["plan_slug"], "solo")
        self.assertNotIn("amount", apply_mock.call_args.kwargs)
        self.assertNotIn("currency", apply_mock.call_args.kwargs)
        audit_log = AdminAuditLog.objects.get(action="subscription_changed", target_user_id=self.user.tg_user_id)
        self.assertEqual(audit_log.mode, "extend")

    def test_change_page_shows_single_paid_access_button(self):
        UserAdminState.objects.update_or_create(
            telegram_user_id=self.user.tg_user_id,
            defaults={
                "status": UserAdminState.Status.ACTIVE,
                "subscription_status": UserAdminState.SubscriptionStatus.TRIAL,
            },
        )

        self.client.force_login(self.superuser)
        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.user.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Надати / продовжити платний доступ", count=1)
        self.assertNotContains(response, "Продовжити підписку")

    def test_subscription_action_form_hides_amount_and_currency_fields(self):
        self.client.force_login(self.superuser)

        response = self.client.get(reverse("admin:users_telegramuser_subscription_action", args=[self.user.pk, "manual"]))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'name="amount"')
        self.assertNotContains(response, 'name="currency"')

    def test_force_charge_now_view_calls_retry_charge_flow(self):
        Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.TRIAL,
            plan="solo",
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
            next_charge_at=timezone.now() + timedelta(days=30),
            trial_days=30,
            auto_renew=True,
        )
        BillingProfile.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            wallet_id=f"mono-force-run-{self.user.tg_user_id}",
            card_token="retry-token",
            masked_pan="444455******1212",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
            last_failure_reason="insufficient funds",
        )
        payment = Payment.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            provider_payment_id="retry-admin-force-1",
            amount=Decimal("499.00"),
            currency="UAH",
            status=Payment.Status.FAILED,
            kind=Payment.Kind.RETRY,
        )

        self.client.force_login(self.superuser)
        with patch(
            "users.admin.retry_monobank_charge",
            return_value={
                "payment_id": payment.pk,
                "invoice_id": payment.provider_payment_id,
                "status": Payment.Status.FAILED,
                "action_url": "",
            },
        ) as retry_mock:
            response = self.client.post(
                reverse("admin:users_telegramuser_force_charge", args=[self.user.pk]),
                data={
                    "confirm": "on",
                    "reason": "billing smoke",
                    "intent_key": "admin_force_test_intent_001",
                },
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        retry_mock.assert_called_once_with(
            user_id=self.user.tg_user_id,
            intent_key="admin_force_test_intent_001",
        )
        self.assertRedirects(response, reverse("admin:users_telegramuser_change", args=[self.user.pk]))
        audit_log = AdminAuditLog.objects.get(action="billing_force_charge_now", target_user_id=self.user.tg_user_id)
        self.assertEqual(audit_log.mode, "force_charge_now")
        self.assertEqual(audit_log.reason, "billing smoke")
        self.assertEqual(audit_log.after["payment_id"], payment.pk)
        self.assertEqual(audit_log.after["payment_status"], Payment.Status.FAILED)
        self.assertEqual(audit_log.after["last_failure_reason"], "insufficient funds")

    def test_mark_and_unmark_test_user(self):
        state = mark_user_as_test_user(user=self.user, admin_user=self.superuser, notes="QA")
        self.assertTrue(state.is_test_user)
        self.assertEqual(state.test_user_notes, "QA")

        state = unmark_user_as_test_user(user=self.user, admin_user=self.superuser)
        self.assertFalse(state.is_test_user)

    def test_assign_and_remove_user_tag(self):
        tag = Tag.objects.create(name="VIP", slug="vip")

        created_count = assign_tag_to_users(
            tag=tag,
            users=[self.user],
            admin_user=self.superuser,
        )
        removed_count = remove_tag_from_users(
            tag=tag,
            users=[self.user],
            admin_user=self.superuser,
        )

        self.assertEqual(created_count, 1)
        self.assertFalse(UserTag.objects.filter(user_id=self.user.tg_user_id, tag=tag).exists())
        self.assertEqual(removed_count, 1)

    def test_get_or_create_user_tag_reuses_existing_case_insensitively(self):
        existing = Tag.objects.create(name="VIP", slug="vip")

        tag, created = get_or_create_user_tag(name="  vip  ", admin_user=self.superuser)

        self.assertEqual(tag.pk, existing.pk)
        self.assertFalse(created)

    def test_bulk_create_and_assign_tag_from_changelist(self):
        self.client.force_login(self.superuser)

        response = self.client.post(
            reverse("admin:users_telegramuser_changelist"),
            data={
                "bulk_tag_submit": "1",
                "bulk_tag_operation": "create_assign",
                "bulk_tag_new_name": "Premium",
                "_selected_action": [str(self.user.pk)],
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        tag = Tag.objects.get(name="Premium")
        self.assertTrue(UserTag.objects.filter(user_id=self.user.tg_user_id, tag=tag).exists())

    def test_safe_reset_preserves_finance_and_subscription_data(self):
        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Cash",
            currency="UAH",
            balance=Decimal("100.00"),
            starting_balance=Decimal("100.00"),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user_id=self.user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("50.00"),
            currency="UAH",
            category_id=category.id,
            account_id=account.id,
            source="text",
            created_at=timezone.now(),
        )
        Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.ACTIVE,
            plan="solo",
            provider="manual",
            amount=Decimal("10.00"),
            currency="USD",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
        )

        result = reset_user_onboarding(
            self.user.tg_user_id,
            "safe",
            admin_user=self.superuser,
            reason="QA replay",
            options={"double_confirmed": True},
        )

        self.user.refresh_from_db()
        state = UserAdminState.objects.get(telegram_user_id=self.user.tg_user_id)
        self.assertEqual(result["status"], "success")
        self.assertFalse(self.user.onboarding_completed)
        self.assertEqual(self.user.onboarding_version, 0)
        self.assertEqual(Transaction.objects.filter(tg_user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(Account.objects.filter(tg_user_id=self.user.tg_user_id, is_active=True).count(), 1)
        self.assertEqual(Category.objects.filter(user_id=self.user.tg_user_id, is_active=True).count(), 1)
        self.assertEqual(Subscription.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(state.pending_admin_reset_mode, "safe")
        self.assertTrue(BotEvent.objects.filter(user_id=self.user.tg_user_id, event_type="onboarding_reset_by_admin").exists())

    def test_cleanup_reset_requires_test_user(self):
        with self.assertRaises(ResetOnboardingError):
            reset_user_onboarding(
                self.user.tg_user_id,
                "cleanup",
                admin_user=self.superuser,
                reason="cleanup",
                options={"double_confirmed": True},
            )

    def test_full_reset_requires_superuser_and_confirmation_text(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        with self.assertRaises(PermissionDenied):
            reset_user_onboarding(
                self.user.tg_user_id,
                "full",
                admin_user=self.staff_user,
                reason="full reset",
                options={"double_confirmed": True, "confirmation_text": "RESET USER"},
            )
        with self.assertRaises(ResetOnboardingError):
            reset_user_onboarding(
                self.user.tg_user_id,
                "full",
                admin_user=self.superuser,
                reason="full reset",
                options={"double_confirmed": True, "confirmation_text": "WRONG"},
            )

    def test_cleanup_reset_deactivates_test_user_finance_data(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Cash",
            currency="UAH",
            balance=Decimal("100.00"),
            starting_balance=Decimal("100.00"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user_id=self.user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("50.00"),
            currency="UAH",
            category_id=category.id,
            account_id=account.id,
            source="text",
            created_at=timezone.now(),
        )

        reset_user_onboarding(
            self.user.tg_user_id,
            "cleanup",
            admin_user=self.superuser,
            reason="cleanup",
            options={"double_confirmed": True},
        )

        account.refresh_from_db()
        category.refresh_from_db()
        self.assertFalse(account.is_active)
        self.assertFalse(category.is_active)
        self.assertEqual(Transaction.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)

    def test_full_reset_clears_billing_state_for_test_user(self):
        state = mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        state.subscription_status = UserAdminState.SubscriptionStatus.PAID
        state.access_scope = UserAdminState.AccessScope.PERSONAL_FULL
        state.access_source = "billing"
        state.pending_start_payload = "promo_qa"
        state.save(
            update_fields=[
                "subscription_status",
                "access_scope",
                "access_source",
                "pending_start_payload",
                "updated_at",
            ]
        )

        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Card",
            currency="UAH",
            balance=Decimal("10.00"),
            starting_balance=Decimal("10.00"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user_id=self.user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("5.00"),
            currency="UAH",
            category_id=category.id,
            account_id=account.id,
            source="text",
            created_at=timezone.now(),
        )
        subscription = Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.ACTIVE,
            plan="solo",
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
            next_charge_at=timezone.now() + timedelta(days=30),
            trial_days=30,
        )
        payment = Payment.objects.create(
            user_id=self.user.tg_user_id,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="pay-reset-user-1",
            external_transaction_id="txn-reset-user-1",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.BIND,
        )
        BillingProfile.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            wallet_id="wallet-reset-user-1",
            card_token="tok_reset_user_1",
            masked_pan="444455******7777",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        SubscriptionEvent.objects.create(
            subscription=subscription,
            user_id=self.user.tg_user_id,
            event_type="payment_success",
            payload={"payment_id": payment.id},
        )
        offer = PromoOffer.objects.create(code="QA90", label="QA 90", trial_days=90, is_active=True)
        PromoOfferClaim.objects.create(
            offer=offer,
            user_id=self.user.tg_user_id,
            status=PromoOfferClaim.Status.CONSUMED,
            bind_payment=payment,
            subscription=subscription,
            consumed_at=timezone.now(),
        )

        result = reset_user_onboarding(
            self.user.tg_user_id,
            "full",
            admin_user=self.superuser,
            reason="full reset for qa",
            options={"double_confirmed": True, "confirmation_text": "RESET USER"},
        )

        self.user.refresh_from_db()
        state.refresh_from_db()
        account.refresh_from_db()
        category.refresh_from_db()
        self.assertFalse(self.user.onboarding_completed)
        self.assertEqual(self.user.onboarding_version, 0)
        self.assertIsNone(self.user.lang)
        self.assertIsNone(self.user.base_currency)
        self.assertIsNone(self.user.start_date)
        self.assertFalse(account.is_active)
        self.assertFalse(category.is_active)
        self.assertEqual(Transaction.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(BillingProfile.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(Payment.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(Subscription.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(SubscriptionEvent.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(PromoOfferClaim.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.NONE)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PAYWALL)
        self.assertEqual(state.access_source, "")
        self.assertEqual(state.pending_start_payload, "")
        self.assertEqual(result["cleanup"]["deleted_billing_profiles"], 1)
        self.assertEqual(result["cleanup"]["deleted_payments"], 1)
        self.assertEqual(result["cleanup"]["deleted_subscriptions"], 1)
        self.assertEqual(result["cleanup"]["deleted_subscription_events"], 1)
        self.assertEqual(result["cleanup"]["deleted_promo_claims"], 1)

    def test_replay_reset_erases_runtime_data_but_preserves_billing_and_access(self):
        state = mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        state.subscription_status = UserAdminState.SubscriptionStatus.PAID
        state.access_scope = UserAdminState.AccessScope.PERSONAL_FULL
        state.access_source = "admin_manual"
        state.pending_start_payload = "promo_keep"
        state.save(
            update_fields=[
                "subscription_status",
                "access_scope",
                "access_source",
                "pending_start_payload",
                "updated_at",
            ]
        )

        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Main",
            currency="UAH",
            balance=Decimal("100.00"),
            starting_balance=Decimal("100.00"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        AccountAdminState.objects.create(account=account, is_default=True, is_active=True)
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user_id=self.user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("50.00"),
            currency="UAH",
            category_id=category.id,
            account_id=account.id,
            source="text",
            created_at=timezone.now(),
        )
        debt = Debt.objects.create(
            tg_user_id=self.user.tg_user_id,
            counterparty_name="Alice",
            direction="receivable",
            initial_amount=Decimal("40.00"),
            paid_amount=Decimal("0.00"),
            remaining_amount=Decimal("40.00"),
            currency="UAH",
            status="active",
            account=account,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        DebtPayment.objects.create(
            tg_user_id=self.user.tg_user_id,
            debt=debt,
            amount=Decimal("10.00"),
            currency="UAH",
            account=account,
            payment_date=date.today(),
            created_at=timezone.now(),
        )
        BotEvent.objects.create(user_id=self.user.tg_user_id, event_type="start", source="test", success=True)
        AdminMessageLog.objects.create(
            telegram_user_id=self.user.tg_user_id,
            admin_user=self.superuser,
            message_text="hello",
            parse_mode="none",
            send_test_to_admin_first=False,
            target_chat_id=self.user.tg_user_id,
        )
        support_case = SupportCase.objects.create(
            user_id=self.user.tg_user_id,
            subject="Replay me",
            status=SupportCase.Status.NEW,
            category=SupportCase.Category.OTHER,
        )
        SupportMessage.objects.create(case=support_case, sender_type=SupportMessage.SenderType.USER, text="ping")
        FeedbackItem.objects.create(
            user_id=self.user.tg_user_id,
            source=FeedbackItem.Source.SUPPORT,
            text="feedback",
            category=FeedbackItem.Category.OTHER,
            support_case=support_case,
        )
        campaign = PollCampaign.objects.create(
            title="Replay poll",
            question="How are you?",
            type=PollCampaign.Type.RATING,
            created_by=self.superuser,
        )
        PollRecipient.objects.create(campaign=campaign, user_id=self.user.tg_user_id, status=PollRecipient.Status.PENDING)
        PollResponse.objects.create(campaign=campaign, user_id=self.user.tg_user_id, answer="ok")

        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["Replay family", self.user.tg_user_id],
            )
            family_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, self.user.tg_user_id, "owner", self.user.tg_user_id],
            )
            cursor.execute(
                "INSERT INTO family_invites "
                "(family_id, created_by_user_id, used_by_user_id, token_hash, status, max_uses, used_count, "
                "expires_at, used_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, 'used', 1, 1, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [
                    family_id,
                    self.user.tg_user_id,
                    self.user.tg_user_id,
                    "replay-family-token",
                    timezone.now() + timedelta(days=1),
                ],
            )

        subscription = Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.ACTIVE,
            plan="solo",
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
            next_charge_at=timezone.now() + timedelta(days=30),
            trial_days=30,
        )
        payment = Payment.objects.create(
            user_id=self.user.tg_user_id,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="pay-replay-1",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.BIND,
        )
        BillingProfile.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            wallet_id="wallet-replay-1",
            card_token="tok_replay_1",
            masked_pan="444455******0001",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        SubscriptionEvent.objects.create(
            subscription=subscription,
            user_id=self.user.tg_user_id,
            event_type="payment_success",
            payload={"payment_id": payment.id},
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO ai_transaction_drafts (tg_user_id, source, telegram_file_unique_id) VALUES (%s, %s, %s)",
                [self.user.tg_user_id, "test_fixture", "replay-reset-fixture"],
            )
            cursor.execute("INSERT INTO saving_prompt_settings (tg_user_id) VALUES (%s)", [self.user.tg_user_id])
            cursor.execute(
                "INSERT INTO pending_saving_tasks "
                "(tg_user_id, source_account_id, target_account_id, amount, currency) VALUES (%s, %s, %s, %s, %s)",
                [self.user.tg_user_id, account.id, account.id, Decimal("10.00"), "UAH"],
            )
            cursor.execute(
                "INSERT INTO debt_invites (debt_id, token, created_by_user_id, used_by_user_id) VALUES (%s, %s, %s, %s)",
                [debt.id, "replay-token", self.user.tg_user_id, None],
            )

        result = reset_user_onboarding(
            self.user.tg_user_id,
            "replay",
            admin_user=self.superuser,
            reason="erase runtime profile for replay",
            options={
                "double_confirmed": True,
                "confirmation_text": REPLAY_RESET_CONFIRMATION,
                "send_telegram_notice": False,
                "send_admin_notification": False,
            },
        )

        self.user.refresh_from_db()
        state.refresh_from_db()
        self.assertFalse(self.user.onboarding_completed)
        self.assertEqual(self.user.onboarding_version, 0)
        self.assertIsNone(self.user.lang)
        self.assertIsNone(self.user.base_currency)
        self.assertIsNone(self.user.start_date)
        self.assertEqual(state.subscription_status, UserAdminState.SubscriptionStatus.PAID)
        self.assertEqual(state.access_scope, UserAdminState.AccessScope.PERSONAL_FULL)
        self.assertEqual(state.access_source, "admin_manual")
        self.assertEqual(state.pending_start_payload, "")
        self.assertEqual(state.pending_admin_reset_mode, "replay")

        self.assertEqual(Account.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(AccountAdminState.objects.count(), 0)
        self.assertEqual(Category.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(Transaction.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(Debt.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(DebtPayment.objects.filter(tg_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(AdminMessageLog.objects.filter(telegram_user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(SupportCase.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(SupportMessage.objects.count(), 0)
        self.assertEqual(FeedbackItem.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(PollRecipient.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(PollResponse.objects.filter(user_id=self.user.tg_user_id).count(), 0)
        self.assertEqual(BillingProfile.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(Payment.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(Subscription.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(SubscriptionEvent.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertEqual(BotEvent.objects.filter(user_id=self.user.tg_user_id).count(), 1)
        self.assertTrue(BotEvent.objects.filter(user_id=self.user.tg_user_id, event_type="onboarding_reset_by_admin").exists())

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM ai_transaction_drafts WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM saving_prompt_settings WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM pending_saving_tasks WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM debt_invites WHERE created_by_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM family_members WHERE user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM family_invites WHERE created_by_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM families WHERE owner_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)

        self.assertEqual(result["cleanup"]["deleted_accounts"], 1)
        self.assertEqual(result["cleanup"]["deleted_categories"], 1)
        self.assertEqual(result["cleanup"]["deleted_transactions"], 1)
        self.assertEqual(result["cleanup"]["deleted_family_members"], 1)
        self.assertEqual(result["cleanup"]["deleted_family_invites"], 1)
        self.assertEqual(result["cleanup"]["deleted_families"], 1)
        self.assertEqual(result["cleanup"]["deleted_billing_profiles"], 0)
        self.assertEqual(result["cleanup"]["deleted_subscriptions"], 0)

    def test_replay_reset_forces_owned_family_space_cleanup(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        other_user = TelegramUser.objects.create(
            tg_user_id=555003,
            first_name="Other",
            last_name="Member",
            username="other_member",
            lang="uk",
            base_currency="UAH",
            start_date=date(2026, 5, 4),
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        other_account = Account.objects.create(
            tg_user_id=other_user.tg_user_id,
            label="Shared card",
            currency="UAH",
            balance=Decimal("55.00"),
            starting_balance=Decimal("55.00"),
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        other_category = Category.objects.create(
            user_id=other_user.tg_user_id,
            tg_user_id=other_user.tg_user_id,
            name="Shared groceries",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        other_debt = Debt.objects.create(
            tg_user_id=other_user.tg_user_id,
            counterparty_name="Shared lender",
            direction="payable",
            initial_amount=Decimal("20.00"),
            paid_amount=Decimal("0.00"),
            remaining_amount=Decimal("20.00"),
            currency="UAH",
            status="active",
            account=other_account,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        DebtPayment.objects.create(
            tg_user_id=other_user.tg_user_id,
            debt=other_debt,
            amount=Decimal("5.00"),
            currency="UAH",
            account=other_account,
            payment_date=date.today(),
            created_at=timezone.now(),
        )
        other_tx = Transaction.objects.create(
            tg_user_id=other_user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("12.00"),
            currency="UAH",
            category_id=other_category.id,
            account_id=other_account.id,
            source="text",
            created_at=timezone.now(),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["QA Family", self.user.tg_user_id],
            )
            family_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, self.user.tg_user_id, "owner", self.user.tg_user_id],
            )
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, other_user.tg_user_id, "member", self.user.tg_user_id],
            )
            cursor.execute(
                "UPDATE accounts SET family_id = %s WHERE id = %s",
                [family_id, other_account.id],
            )
            cursor.execute(
                "UPDATE categories SET family_id = %s WHERE id = %s",
                [family_id, other_category.id],
            )
            cursor.execute(
                "UPDATE transactions SET family_id = %s WHERE id = %s",
                [family_id, other_tx.id],
            )
            cursor.execute(
                "UPDATE debts SET family_id = %s WHERE id = %s",
                [family_id, other_debt.id],
            )
            cursor.execute(
                "UPDATE debt_payments SET family_id = %s WHERE tg_user_id = %s",
                [family_id, other_user.tg_user_id],
            )

        result = reset_user_onboarding(
            self.user.tg_user_id,
            "replay",
            admin_user=self.superuser,
            reason="force family cleanup",
            options={
                "double_confirmed": True,
                "confirmation_text": REPLAY_RESET_CONFIRMATION,
                "send_telegram_notice": False,
                "send_admin_notification": False,
            },
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM family_members WHERE family_id = %s", [family_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM families WHERE id = %s", [family_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT family_id FROM accounts WHERE id = %s", [other_account.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT family_id FROM categories WHERE id = %s", [other_category.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT family_id FROM transactions WHERE id = %s", [other_tx.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT family_id FROM debts WHERE id = %s", [other_debt.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT family_id FROM debt_payments WHERE tg_user_id = %s", [other_user.tg_user_id])
            self.assertIsNone(cursor.fetchone()[0])

        self.assertTrue(TelegramUser.objects.filter(pk=other_user.pk).exists())
        self.assertEqual(result["cleanup"]["deleted_family_members"], 2)
        self.assertEqual(result["cleanup"]["deleted_families"], 1)

    def test_change_page_shows_erase_user_data_button_for_test_user(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        self.client.force_login(self.superuser)

        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.user.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("admin:users_telegramuser_erase_user_data", args=[self.user.pk]))
        self.assertContains(response, "Стерти дані користувача")

    def test_hard_delete_requires_superuser_and_test_user(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)

        with self.assertRaises(PermissionDenied):
            hard_delete_user(
                self.user.tg_user_id,
                admin_user=self.staff_user,
                reason="qa cleanup",
                options={
                    "double_confirmed": True,
                    "confirmation_text": f"DELETE USER {self.user.tg_user_id}",
                },
            )

        another_user = TelegramUser.objects.create(
            tg_user_id=555099,
            first_name="Petro",
            last_name="Real",
            username="petro_real",
            lang="uk",
            base_currency="UAH",
            start_date=date(2026, 5, 2),
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )
        with self.assertRaises(HardDeleteUserError):
            hard_delete_user(
                another_user.tg_user_id,
                admin_user=self.superuser,
                reason="qa cleanup",
                options={
                    "double_confirmed": True,
                    "confirmation_text": f"DELETE USER {another_user.tg_user_id}",
                },
            )

    def test_hard_delete_removes_test_user_and_related_runtime_records(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser, notes="hard delete")
        other_user = TelegramUser.objects.create(
            tg_user_id=555002,
            first_name="Oleh",
            last_name="Member",
            username="oleh_member",
            lang="uk",
            base_currency="UAH",
            start_date=date(2026, 5, 3),
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

        topic = PushTopic.objects.create(name="Billing QA", slug="billing-qa")
        UserPushTopic.objects.create(user_id=self.user.tg_user_id, topic=topic, is_enabled=False)
        tag = Tag.objects.create(name="QA delete", slug="qa-delete")
        UserTag.objects.create(user_id=self.user.tg_user_id, tag=tag, created_by=self.superuser)
        AdminNote.objects.create(telegram_user_id=self.user.tg_user_id, admin_user=self.superuser, note_text="hard delete me")
        AdminMessageLog.objects.create(
            telegram_user_id=self.user.tg_user_id,
            admin_user=self.superuser,
            message_text="ping",
            parse_mode="none",
            send_test_to_admin_first=False,
            target_chat_id=self.user.tg_user_id,
        )
        BotEvent.objects.create(user_id=self.user.tg_user_id, event_type="start", source="test", success=True)

        support_case = SupportCase.objects.create(
            user_id=self.user.tg_user_id,
            subject="Help",
            status=SupportCase.Status.NEW,
            category=SupportCase.Category.OTHER,
        )
        SupportMessage.objects.create(case=support_case, sender_type=SupportMessage.SenderType.USER, text="hello")
        FeedbackItem.objects.create(
            user_id=self.user.tg_user_id,
            source=FeedbackItem.Source.SUPPORT,
            text="feedback",
            category=FeedbackItem.Category.OTHER,
            support_case=support_case,
        )

        campaign = PollCampaign.objects.create(
            title="QA poll",
            question="How are you?",
            type=PollCampaign.Type.RATING,
            created_by=self.superuser,
        )
        PollRecipient.objects.create(campaign=campaign, user_id=self.user.tg_user_id, status=PollRecipient.Status.PENDING)
        PollResponse.objects.create(campaign=campaign, user_id=self.user.tg_user_id, answer="ok")

        account = Account.objects.create(
            tg_user_id=self.user.tg_user_id,
            label="Card",
            currency="UAH",
            balance=Decimal("100.00"),
            starting_balance=Decimal("100.00"),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        category = Category.objects.create(
            user_id=self.user.tg_user_id,
            tg_user_id=self.user.tg_user_id,
            name="Food",
            type="expense",
            kind="expense",
            source="custom",
            is_system=False,
            is_active=True,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        Transaction.objects.create(
            tg_user_id=self.user.tg_user_id,
            date=date.today(),
            type="expense",
            amount=Decimal("20.00"),
            currency="UAH",
            category_id=category.id,
            account_id=account.id,
            source="text",
            created_at=timezone.now(),
        )
        debt = Debt.objects.create(
            tg_user_id=self.user.tg_user_id,
            counterparty_name="Alice",
            direction="receivable",
            initial_amount=Decimal("50.00"),
            paid_amount=Decimal("0.00"),
            remaining_amount=Decimal("50.00"),
            currency="UAH",
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        DebtPayment.objects.create(
            tg_user_id=self.user.tg_user_id,
            debt=debt,
            amount=Decimal("10.00"),
            currency="UAH",
            payment_date=date.today(),
            created_at=timezone.now(),
        )

        other_account = Account.objects.create(
            tg_user_id=other_user.tg_user_id,
            label="Shared",
            currency="UAH",
            balance=Decimal("10.00"),
            starting_balance=Decimal("10.00"),
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        other_debt = Debt.objects.create(
            tg_user_id=other_user.tg_user_id,
            counterparty_name="Bob",
            direction="payable",
            initial_amount=Decimal("10.00"),
            paid_amount=Decimal("0.00"),
            remaining_amount=Decimal("10.00"),
            currency="UAH",
            status="active",
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        subscription = Subscription.objects.create(
            user_id=self.user.tg_user_id,
            status=Subscription.Status.ACTIVE,
            plan="solo",
            provider="monobank",
            amount=Decimal("499.00"),
            currency="UAH",
            started_at=timezone.now(),
            expires_at=timezone.now() + timedelta(days=30),
            next_charge_at=timezone.now() + timedelta(days=30),
            trial_days=30,
        )
        payment = Payment.objects.create(
            user_id=self.user.tg_user_id,
            subscription=subscription,
            provider="monobank",
            provider_payment_id="pay-hard-delete-1",
            external_transaction_id="txn-hard-delete-1",
            amount=Decimal("1.00"),
            currency="UAH",
            status=Payment.Status.PAID,
            kind=Payment.Kind.BIND,
        )
        BillingProfile.objects.create(
            user_id=self.user.tg_user_id,
            provider="monobank",
            wallet_id="wallet-hard-delete-1",
            card_token="tok_hard_delete_1",
            masked_pan="444455******7777",
            status=BillingProfile.Status.ACTIVE,
            auto_renew_enabled=True,
        )
        SubscriptionEvent.objects.create(
            subscription=subscription,
            user_id=self.user.tg_user_id,
            event_type="payment_success",
            payload={"payment_id": payment.id},
        )
        offer = PromoOffer.objects.create(code="HD90", label="HD 90", trial_days=90, is_active=True)
        PromoOfferClaim.objects.create(
            offer=offer,
            user_id=self.user.tg_user_id,
            status=PromoOfferClaim.Status.CONSUMED,
            bind_payment=payment,
            subscription=subscription,
            consumed_at=timezone.now(),
        )

        old_audit = AdminAuditLog.objects.create(
            admin_user=self.superuser,
            action="old_user_action",
            object_type="telegram_user",
            object_id=str(self.user.pk),
            target_user_id=self.user.tg_user_id,
            before={"username": self.user.username},
        )

        with connection.cursor() as cursor:
            cursor.execute("UPDATE accounts SET created_by_user_id = %s WHERE id = %s", [self.user.tg_user_id, other_account.id])
            cursor.execute("UPDATE debts SET borrower_user_id = %s WHERE id = %s", [self.user.tg_user_id, other_debt.id])
            cursor.execute(
                "INSERT INTO ai_transaction_drafts (tg_user_id, source, telegram_file_unique_id) VALUES (%s, %s, %s)",
                [self.user.tg_user_id, "test_fixture", "hard-delete-fixture"],
            )
            cursor.execute("INSERT INTO saving_prompt_settings (tg_user_id) VALUES (%s)", [self.user.tg_user_id])
            cursor.execute(
                "INSERT INTO pending_saving_tasks "
                "(tg_user_id, source_account_id, target_account_id, amount, currency) VALUES (%s, %s, %s, %s, %s)",
                [self.user.tg_user_id, account.id, account.id, Decimal("10.00"), "UAH"],
            )
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["QA family", self.user.tg_user_id],
            )
            family_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, self.user.tg_user_id, "owner", self.user.tg_user_id],
            )
            cursor.execute(
                "INSERT INTO family_invites "
                "(family_id, created_by_user_id, used_by_user_id, token_hash, status, max_uses, used_count, "
                "expires_at, used_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, 'used', 1, 1, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [
                    family_id,
                    self.user.tg_user_id,
                    self.user.tg_user_id,
                    "token-hard-delete",
                    timezone.now() + timedelta(days=1),
                ],
            )
            cursor.execute("UPDATE accounts SET family_id = %s WHERE id = %s", [family_id, account.id])
            cursor.execute("UPDATE categories SET family_id = %s WHERE id = %s", [family_id, category.id])
            cursor.execute("UPDATE debts SET family_id = %s WHERE id = %s", [family_id, debt.id])
            cursor.execute("UPDATE debt_payments SET family_id = %s WHERE tg_user_id = %s", [family_id, self.user.tg_user_id])
            cursor.execute(
                "INSERT INTO debt_invites (debt_id, token, created_by_user_id, used_by_user_id) VALUES (%s, %s, %s, %s)",
                [debt.id, "debt-token", self.user.tg_user_id, self.user.tg_user_id],
            )

        result = hard_delete_user(
            self.user.tg_user_id,
            admin_user=self.superuser,
            reason="reset QA identity",
            options={
                "double_confirmed": True,
                "confirmation_text": f"DELETE USER {self.user.tg_user_id}",
            },
        )

        self.assertFalse(TelegramUser.objects.filter(pk=self.user.pk).exists())
        self.assertFalse(UserAdminState.objects.filter(telegram_user_id=self.user.tg_user_id).exists())
        self.assertFalse(AdminNote.objects.filter(telegram_user_id=self.user.tg_user_id).exists())
        self.assertFalse(UserTag.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(UserPushTopic.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(AdminMessageLog.objects.filter(telegram_user_id=self.user.tg_user_id).exists())
        self.assertFalse(BotEvent.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(SupportCase.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(SupportMessage.objects.filter(case=support_case).exists())
        self.assertFalse(FeedbackItem.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(PollRecipient.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(PollResponse.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(Account.objects.filter(tg_user_id=self.user.tg_user_id).exists())
        self.assertFalse(Category.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(Transaction.objects.filter(tg_user_id=self.user.tg_user_id).exists())
        self.assertFalse(Debt.objects.filter(tg_user_id=self.user.tg_user_id).exists())
        self.assertFalse(DebtPayment.objects.filter(tg_user_id=self.user.tg_user_id).exists())
        self.assertFalse(BillingProfile.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(Payment.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(Subscription.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(SubscriptionEvent.objects.filter(user_id=self.user.tg_user_id).exists())
        self.assertFalse(PromoOfferClaim.objects.filter(user_id=self.user.tg_user_id).exists())

        with connection.cursor() as cursor:
            cursor.execute("SELECT created_by_user_id FROM accounts WHERE id = %s", [other_account.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT borrower_user_id FROM debts WHERE id = %s", [other_debt.id])
            self.assertIsNone(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM ai_transaction_drafts WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM saving_prompt_settings WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM pending_saving_tasks WHERE tg_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM debt_invites WHERE created_by_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM family_members WHERE user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM family_invites WHERE created_by_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)
            cursor.execute("SELECT COUNT(*) FROM families WHERE owner_user_id = %s", [self.user.tg_user_id])
            self.assertEqual(cursor.fetchone()[0], 0)

        audit_logs = AdminAuditLog.objects.filter(target_user_id=self.user.tg_user_id).order_by("created_at")
        self.assertEqual(audit_logs.count(), 1)
        self.assertEqual(audit_logs.first().action, "telegram_user_hard_deleted")
        self.assertFalse(AdminAuditLog.objects.filter(pk=old_audit.pk).exists())

        self.assertEqual(result["cleanup"]["deleted_user_rows"], 1)
        self.assertEqual(result["cleanup"]["deleted_accounts"], 1)
        self.assertEqual(result["cleanup"]["deleted_categories"], 1)
        self.assertEqual(result["cleanup"]["deleted_transactions"], 1)
        self.assertEqual(result["cleanup"]["deleted_subscriptions"], 1)
        self.assertEqual(result["cleanup"]["deleted_family_members"], 1)
        self.assertEqual(result["cleanup"]["deleted_family_invites"], 1)
        self.assertEqual(result["cleanup"]["deleted_families"], 1)
        self.assertEqual(result["cleanup"]["nullified_account_created_by"], 1)
        self.assertEqual(result["cleanup"]["nullified_debt_borrowers"], 1)
        self.assertEqual(result["cleanup"]["deleted_audit_logs"], 2)

    def test_hard_delete_blocks_owned_family_with_other_member(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        other_user = TelegramUser.objects.create(
            tg_user_id=555003,
            first_name="Marta",
            last_name="Guest",
            username="marta_guest",
            lang="uk",
            base_currency="UAH",
            start_date=date(2026, 5, 4),
            onboarding_completed=True,
            onboarding_version=2,
            created_at=timezone.now(),
            last_seen_at=timezone.now(),
        )

        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO families (name, owner_user_id, status, created_at, updated_at) "
                "VALUES (%s, %s, 'active', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id",
                ["Shared", self.user.tg_user_id],
            )
            family_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, self.user.tg_user_id, "owner", self.user.tg_user_id],
            )
            cursor.execute(
                "INSERT INTO family_members "
                "(family_id, user_id, role, status, invited_by_user_id, joined_at, created_at, updated_at) "
                "VALUES (%s, %s, %s, 'active', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [family_id, other_user.tg_user_id, "member", self.user.tg_user_id],
            )

        with self.assertRaises(HardDeleteUserError):
            hard_delete_user(
                self.user.tg_user_id,
                admin_user=self.superuser,
                reason="should fail",
                options={
                    "double_confirmed": True,
                    "confirmation_text": f"DELETE USER {self.user.tg_user_id}",
                },
            )

        self.assertTrue(TelegramUser.objects.filter(pk=self.user.pk).exists())

    def test_change_page_shows_hard_delete_button_for_test_user(self):
        mark_user_as_test_user(user=self.user, admin_user=self.superuser)
        self.client.force_login(self.superuser)

        response = self.client.get(reverse("admin:users_telegramuser_change", args=[self.user.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, reverse("admin:users_telegramuser_hard_delete", args=[self.user.pk]))
