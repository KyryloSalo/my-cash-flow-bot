"""Real savings/transfer preview logic with isolated persistence seams."""
from __future__ import annotations

import copy
import unittest
from contextlib import ExitStack, nullcontext
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from miniapp import savings_tasks, services


class SavingSnapshotIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.user = SimpleNamespace(tg_user_id=930001, lang="en", base_currency="UAH")
        self.accounts = {
            i: SimpleNamespace(id=i, label=f"Account {i}", currency="UAH", balance=Decimal("2000.00"),
                               account_type="main", non_negative_account_type="main", credit_limit=None,
                               monthly_interest_rate=None, goal_name=None, goal_amount=None, goal_date=None)
            for i in (1, 2, 3)
        }
        self.task = {"id": 17, "tg_user_id": self.user.tg_user_id, "source_account_id": 1,
                     "target_account_id": 2, "amount": Decimal("100.00"), "currency": "UAH",
                     "status": "pending", "source_currency": "UAH", "target_currency": "UAH",
                     "source_label": "Account 1", "target_label": "Account 2"}
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(savings_tasks, "_require_personal_saving_tasks"))
        self.task_lookup = self.stack.enter_context(patch.object(savings_tasks, "_task_row", side_effect=lambda *a, **k: copy.deepcopy(self.task)))
        self.account_lookup = self.stack.enter_context(patch.object(savings_tasks, "_active_personal_account", side_effect=lambda user, aid, **k: self.accounts.get(aid)))
        self.stack.enter_context(patch.object(services, "_active_scoped_account", side_effect=lambda user, aid, **k: self.accounts.get(aid)))
        self.stack.enter_context(patch.object(savings_tasks.db_transaction, "atomic", side_effect=nullcontext))
        self.stack.enter_context(patch.object(savings_tasks, "connection", MagicMock()))
        self.stack.enter_context(patch.object(savings_tasks.Transaction, "objects", MagicMock()))
        self.commit = self.stack.enter_context(patch.object(savings_tasks, "commit_transfer_draft", return_value={"transfer": {"id": 11}}))

    def preview(self):
        return savings_tasks.build_saving_task_draft(self.user, {"task_id": 17, "action": "confirm", "transaction_date": "2026-09-14"}, draft_id="saving-preview-0001")

    def test_changed_task_amount_or_destination_rejects_old_preview(self):
        draft = self.preview()
        self.task.update(amount=Decimal("900.00"), target_account_id=3)
        with self.assertRaises(savings_tasks.MiniAppSavingTaskError) as caught:
            savings_tasks.commit_saving_task_draft(self.user, draft)
        self.assertEqual(caught.exception.code, "saving_task_changed")
        self.assertEqual(caught.exception.status, 409)
        self.commit.assert_not_called()
        self.task_lookup.assert_called_with(self.user, 17, for_update=True)

    def test_account_currency_changed_after_task_read_is_checked_under_lock(self):
        draft = self.preview()
        self.accounts[2].currency = "EUR"
        # The join snapshot can precede acquisition of the account row locks.
        with self.assertRaises((savings_tasks.MiniAppSavingTaskError, services.MiniAppTransactionError)) as caught:
            savings_tasks.commit_saving_task_draft(self.user, draft)
        self.assertEqual(caught.exception.code, "saving_task_changed")
        self.assertEqual(caught.exception.status, 409)
        self.commit.assert_not_called()
        self.assertTrue(any(call.kwargs.get("lock") for call in self.account_lookup.call_args_list))

    def test_fresh_confirmation_executes_reviewed_transfer_not_a_rebuilt_one(self):
        draft = self.preview()
        savings_tasks.commit_saving_task_draft(self.user, draft)
        self.assertIs(self.commit.call_args.args[1], draft["transfer"])
