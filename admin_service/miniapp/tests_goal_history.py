"""Database-backed account-scoped goal history regressions."""
from datetime import date
from decimal import Decimal

from django.test import TestCase

from accounts.models import Account
from common.test_helpers import ensure_runtime_finance_tables
from miniapp.history import MiniAppHistoryError, build_history_payload
from transactions.models import Transaction
from users.models import TelegramUser


class GoalHistoryTests(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ensure_runtime_finance_tables()

    def setUp(self):
        self.user = TelegramUser.objects.create(tg_user_id=88001, lang="uk", base_currency="UAH")
        self.other = TelegramUser.objects.create(tg_user_id=88002, lang="uk", base_currency="UAH")
        self.goal = self.account(self.user, "Goal")
        self.wallet = self.account(self.user, "Wallet")
        self.foreign = self.account(self.other, "Private")

    def account(self, user, label):
        return Account.objects.create(tg_user=user, label=label, currency="UAH", balance=Decimal("0"), is_active=True, account_type="savings", non_negative_account_type="savings")

    def transaction(self, **kwargs):
        return Transaction.objects.create(tg_user=self.user, date=date(2026, 10, 19), type="expense", amount=Decimal("10"), currency="UAH", source="manual_test", **kwargs)

    def history(self, **kwargs):
        return build_history_payload(self.user, date_from=date(2026, 10, 1), date_to=date(2026, 10, 31), **kwargs)

    def test_selected_account_history_includes_transfers_not_unrelated_rows(self):
        self.transaction(account=self.wallet, comment="not goal")
        transfer = self.transaction(flow_kind="transfer", from_account=self.wallet, to_account=self.goal, comment="topup")
        expense = self.transaction(account=self.goal, comment="goal expense")

        result = self.history(account_id=self.goal.pk)
        self.assertEqual({row["id"] for row in result["items"]}, {transfer.pk, expense.pk})
        row = next(row for row in result["items"] if row["id"] == transfer.pk)
        self.assertEqual(row["kind"], "transfer")
        self.assertFalse(row["editable"])
        self.assertFalse(row["voidable"])
        self.assertEqual(row["target_account"]["id"], self.goal.pk)

    def test_scope_denies_another_users_account(self):
        with self.assertRaises(MiniAppHistoryError) as caught:
            self.history(account_id=self.foreign.pk)
        self.assertEqual(caught.exception.status, 404)

    def test_default_history_still_excludes_transfers(self):
        transfer = self.transaction(flow_kind="transfer", from_account=self.wallet, to_account=self.goal)
        expense = self.transaction(account=self.goal)
        result = self.history()
        self.assertEqual([row["id"] for row in result["items"]], [expense.pk])
        self.assertNotIn(transfer.pk, [row["id"] for row in result["items"]])

    def test_scoped_cursor_cannot_be_reused_for_another_account(self):
        self.transaction(account=self.goal)
        self.transaction(account=self.goal)
        first = self.history(account_id=self.goal.pk, limit=1)
        self.assertTrue(first["has_more"])
        with self.assertRaises(MiniAppHistoryError):
            self.history(account_id=self.wallet.pk, cursor=first["next_cursor"])
