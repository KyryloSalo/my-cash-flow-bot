from decimal import Decimal
import os
import sys
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("DJANGO_SECRET_KEY", "local-dev-key")
os.environ.setdefault("DJANGO_DEBUG", "true")
os.environ.setdefault("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost")
import django
django.setup()

from miniapp import services


class GoalsLifecycleTests(TestCase):
    def test_goal_create_draft_uses_dedicated_zero_balance_account_contract(self):
        user = SimpleNamespace(lang="uk", tg_user_id=1)
        with patch.object(services, "_ensure_unique_account_label"), patch.object(
            services, "_base_currency", return_value="UAH"
        ):
            draft = services.build_account_draft(
                user,
                {"action": "goal_create", "goal_name": "Подорож", "goal_amount": "9998", "currency": "UAH"},
                draft_id="g1",
            )
        self.assertEqual(draft["action"], "goal_create")
        self.assertEqual(draft["account"]["starting_balance"], "0.00")
        self.assertEqual(draft["account"]["goal_name"], "Подорож")
        self.assertEqual(draft["account"]["goal_amount"], "9998.00")

    def test_completed_status_is_preserved_and_completion_is_not_funded(self):
        account = SimpleNamespace(goal_status="completed", goal_amount=Decimal("1000"), balance=Decimal("1000"))
        self.assertEqual(services._goal_lifecycle_status(account), "completed")

    def test_goal_create_commit_creates_new_zero_balance_account(self):
        user = SimpleNamespace(lang="uk", tg_user_id=1)
        created = SimpleNamespace(id=9, balance=Decimal("0.00"), goal_status="active")
        with patch.object(services, "_lock_finance_scope", return_value=SimpleNamespace(is_family=False, family_id=None)), \
                patch.object(services.Account.objects, "create", return_value=created) as create, \
                patch.object(services, "_account_payload", return_value={"id": 9}):
            result = services.commit_account_draft(user, {"action": "goal_create", "account": {
                "label": "Подорож", "currency": "UAH", "goal_amount": "9998.00", "goal_date": None
            }})
        self.assertEqual(result["action"], "goal_create")
        self.assertEqual(create.call_args.kwargs["balance"], Decimal("0.00"))
        self.assertEqual(create.call_args.kwargs["goal_status"], "active")

    def test_goal_complete_draft_and_commit_are_idempotent_without_balance_mutation(self):
        account = SimpleNamespace(
            id=7, label="Подорож", currency="UAH", account_type="savings", non_negative_account_type="savings",
            balance=Decimal("1200"), goal_name="Подорож", goal_amount=Decimal("1000"), goal_date=None,
            goal_status="completed", goal_completed_at=None, is_active=True,
        )
        account.save = MagicMock()
        user = SimpleNamespace(lang="uk", tg_user_id=1)
        with patch.object(services, "_active_scoped_account", return_value=account), patch.object(
            services, "_account_payload", return_value={"id": 7, "goal_status": "completed"}
        ):
            draft = services.build_account_draft(user, {"action": "goal_complete", "account_id": 7}, draft_id="c1")
            with patch.object(services, "_lock_finance_scope", return_value=SimpleNamespace(is_family=False, family_id=None)):
                result = services.commit_account_draft(user, draft)
        self.assertEqual(result["action"], "goal_complete")
        self.assertEqual(account.balance, Decimal("1200"))
        self.assertEqual(account.goal_status, "completed")
        account.save.assert_not_called()

    def test_normal_expense_cannot_bypass_open_goal_full_spend_rule(self):
        user = SimpleNamespace(lang="uk", tg_user_id=1)
        account = SimpleNamespace(
            id=7, currency="UAH", balance=Decimal("1000"), goal_amount=Decimal("1000"), goal_status="active",
            account_type="savings", non_negative_account_type="savings", label="Goal",
        )
        with patch.object(services, "_active_scoped_account", return_value=account), patch.object(
            services, "_active_scoped_category", return_value=SimpleNamespace(id=1, name="Їжа", type="expense", kind="expense")
        ):
            with self.assertRaisesRegex(services.MiniAppTransactionError, "goal"):
                services.build_transaction_draft(
                    user,
                    {"kind": "expense", "account_id": 7, "category_id": 1, "amount": "10", "currency": "UAH"},
                    draft_id="e1",
                )
