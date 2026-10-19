"""Real PostgreSQL goal invariants, including stale confirmation safety."""
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from django.db import connections
from django.test import TestCase, TransactionTestCase

from accounts.models import Account
from categories.models import Category
from common.test_helpers import ensure_runtime_finance_tables
from miniapp import services
from transactions.models import Transaction
from users.models import TelegramUser


class GoalsDatabaseTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_runtime_finance_tables()

    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=88901, lang="uk", base_currency="UAH")
        self.wallet = Account.objects.create(tg_user=self.user, label="Wallet", currency="UAH", balance="40000", is_active=True, account_type="main", non_negative_account_type="main")
        self.category = Category.objects.create(tg_user=self.user, user=self.user, name="Purchase", kind="expense", type="expense", is_active=True)

    def create_goal(self, name="Journey", target="9998"):
        draft = services.build_account_draft(self.user, {"action": "goal_create", "goal_name": name, "goal_amount": target}, draft_id=name)
        result = services.commit_account_draft(self.user, draft)
        return Account.objects.get(pk=result["account"]["id"])

    def transfer(self, target, amount):
        draft = services.build_transfer_draft(self.user, {"source_account_id": self.wallet.pk, "target_account_id": target.pk, "amount": amount}, draft_id="transfer")
        result = services.commit_transfer_draft(self.user, draft)
        target.refresh_from_db()
        return result

    def expense_payload(self, account, amount, **kwargs):
        return {"kind": "expense", "account_id": account.pk, "category_id": self.category.pk, "amount": amount, **kwargs}

    def test_stale_normal_expense_confirmation_cannot_bypass_goal_rule(self):
        draft = services.build_transaction_draft(self.user, self.expense_payload(self.wallet, "1"), draft_id="stale-expense")
        self.wallet.account_type = "savings"
        self.wallet.non_negative_account_type = "savings"
        self.wallet.goal_name = "New goal"
        self.wallet.goal_amount = Decimal("50000")
        self.wallet.save()
        with self.assertRaises(services.MiniAppTransactionError) as caught:
            services.commit_transaction_draft(self.user, draft)
        self.assertEqual(caught.exception.code, "goal_spend_requires_full_balance")
        self.wallet.refresh_from_db()
        self.assertEqual(self.wallet.balance, Decimal("40000"))
        self.assertFalse(Transaction.objects.exists())

    def test_draft_is_read_only_and_confirm_creates_only_new_zero_balance_goal(self):
        draft = services.build_account_draft(self.user, {"action": "goal_create", "goal_name": "Journey", "goal_amount": "9998"}, draft_id="create")
        self.assertEqual(Account.objects.count(), 1)
        self.assertFalse(Transaction.objects.exists())
        result = services.commit_account_draft(self.user, draft)
        goal = Account.objects.get(pk=result["account"]["id"])
        self.assertNotEqual(goal.pk, self.wallet.pk)
        self.assertEqual(goal.currency, "UAH")
        self.assertEqual(goal.balance, Decimal("0"))
        self.assertEqual(goal.starting_balance, Decimal("0"))
        self.assertEqual(goal.goal_amount, Decimal("9998"))
        self.assertFalse(Transaction.objects.exists())

    def test_funded_and_overfunded_goal_is_not_completed_and_history_is_transfer(self):
        goal = self.create_goal()
        self.transfer(goal, "10000")
        self.assertEqual(goal.balance, Decimal("10000"))
        self.assertEqual(goal.goal_status, "active")
        self.assertEqual(services._goal_lifecycle_status(goal), "funded")
        self.assertIsNone(goal.goal_completed_at)
        transaction = Transaction.objects.get()
        self.assertEqual(transaction.flow_kind, "transfer")
        self.assertEqual(transaction.to_account_id, goal.pk)
        self.assertEqual(transaction.from_account_id, self.wallet.pk)

    def test_only_full_funded_balance_can_be_spent_and_stale_draft_is_rejected(self):
        goal = self.create_goal()
        with self.assertRaises(services.MiniAppTransactionError):
            services.build_transaction_draft(self.user, self.expense_payload(goal, "1", goal_spend=True), draft_id="underfunded")
        self.transfer(goal, "10000")
        with self.assertRaises(services.MiniAppTransactionError):
            services.build_transaction_draft(self.user, self.expense_payload(goal, "9998", goal_spend=True), draft_id="partial")
        draft = services.build_transaction_draft(self.user, self.expense_payload(goal, "10000", goal_spend=True), draft_id="full")
        self.transfer(goal, "1")
        with self.assertRaises(services.MiniAppTransactionError) as caught:
            services.commit_transaction_draft(self.user, draft)
        self.assertEqual(caught.exception.code, "goal_spend_amount_mismatch")
        goal.refresh_from_db()
        self.assertEqual(goal.balance, Decimal("10001"))
        self.assertEqual(Transaction.objects.count(), 2)
        refreshed = services.build_transaction_draft(self.user, self.expense_payload(goal, "10001", goal_spend=True), draft_id="refreshed")
        services.commit_transaction_draft(self.user, refreshed)
        goal.refresh_from_db()
        self.assertEqual(goal.balance, Decimal("0"))
        self.assertEqual(goal.goal_status, "spent")
        with self.assertRaises(services.MiniAppTransactionError):
            services.commit_transaction_draft(self.user, refreshed)

    def test_completion_changes_no_money_and_account_stays_transferable(self):
        goal = self.create_goal()
        self.transfer(goal, "10000")
        count = Transaction.objects.count()
        draft = services.build_account_draft(self.user, {"action": "goal_complete", "account_id": goal.pk}, draft_id="complete")
        self.assertEqual(Account.objects.get(pk=goal.pk).goal_status, "active")
        services.commit_account_draft(self.user, draft)
        services.commit_account_draft(self.user, draft)
        goal.refresh_from_db()
        self.assertEqual(goal.balance, Decimal("10000"))
        self.assertEqual(goal.goal_status, "completed")
        self.assertTrue(goal.is_active)
        self.assertEqual(Transaction.objects.count(), count)
        self.transfer(goal, "1")
        self.assertEqual(goal.balance, Decimal("10001"))
        self.assertEqual(services._goal_lifecycle_status(goal), "completed")

    def test_completion_commit_rejects_non_goal_or_cleared_goal(self):
        goal = self.create_goal()
        stale = services.build_account_draft(self.user, {"action": "goal_complete", "account_id": goal.pk}, draft_id="stale-completion")
        goal.goal_amount = None
        goal.goal_name = None
        goal.save()
        for draft in ({"action": "goal_complete", "account_id": self.wallet.pk}, stale):
            with self.subTest(account_id=draft["account_id"]):
                with self.assertRaises(services.MiniAppTransactionError) as caught:
                    services.commit_account_draft(self.user, draft)
                self.assertEqual(caught.exception.code, "goal_not_supported")
        self.wallet.refresh_from_db()
        goal.refresh_from_db()
        self.assertNotEqual(self.wallet.goal_status, "completed")
        self.assertNotEqual(goal.goal_status, "completed")
        self.assertFalse(Transaction.objects.exists())

    def test_creation_rollback_leaves_no_partial_account(self):
        draft = services.build_account_draft(self.user, {"action": "goal_create", "goal_name": "Rollback", "goal_amount": "100"}, draft_id="rollback")
        with patch.object(Account, "save", side_effect=RuntimeError("synthetic failure")):
            with self.assertRaises(RuntimeError):
                services.commit_account_draft(self.user, draft)
        self.assertEqual(Account.objects.count(), 1)
        self.assertFalse(Transaction.objects.exists())

    def test_commit_revalidates_goal_target_and_name(self):
        draft = services.build_account_draft(self.user, {"action": "goal_create", "goal_name": "Invalid", "goal_amount": "100"}, draft_id="invalid")
        with self.assertRaises(services.MiniAppTransactionError):
            services.commit_account_draft(self.user, {**draft, "account": {**draft["account"], "goal_amount": "0"}})
        with self.assertRaises(services.MiniAppTransactionError):
            services.commit_account_draft(self.user, {**draft, "account": {**draft["account"], "label": ""}})
        self.assertEqual(Account.objects.count(), 1)


class GoalCreationConcurrencyTests(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_runtime_finance_tables()

    def test_concurrent_confirmations_cannot_create_duplicate_goal_accounts(self):
        user = TelegramUser.objects.create(tg_user_id=8813202, lang="uk", base_currency="UAH")
        draft = services.build_account_draft(user, {"action": "goal_create", "goal_name": "Concurrent", "goal_amount": "100"}, draft_id="race")
        barrier = Barrier(2)

        def confirm():
            connections.close_all()
            try:
                actor = TelegramUser.objects.get(pk=user.pk)
                barrier.wait(timeout=10)
                try:
                    services.commit_account_draft(actor, draft)
                    return "created"
                except services.MiniAppTransactionError as exc:
                    return exc.code
            finally:
                connections.close_all()

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda _: confirm(), range(2)))
        self.assertCountEqual(outcomes, ["created", "account_label_exists"])
        self.assertEqual(Account.objects.filter(tg_user=user, label="Concurrent").count(), 1)
