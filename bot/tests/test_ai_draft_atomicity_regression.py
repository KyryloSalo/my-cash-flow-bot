from __future__ import annotations
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ai_transaction_draft_service import AiTransactionDraftService


class DraftAtomicityTests(unittest.IsolatedAsyncioTestCase):
    def make_service(self):
        state = {"status": "pending", "ledger": []}
        @asynccontextmanager
        async def transaction():
            before = deepcopy(state)
            try:
                yield
            except BaseException:
                state.clear()
                state.update(before)
                raise
        async def fetchrow(sql, *args):
            self.assertIn("FOR UPDATE", sql)
            self.assertEqual(args, (17, 900001))
            return {"status": state["status"]}
        service = AiTransactionDraftService(SimpleNamespace(transaction=transaction, fetchrow=fetchrow))
        async def complete(draft_id, user_id):
            state["status"] = "completed"
            return SimpleNamespace(id=draft_id)
        service.mark_completed = complete
        return service, state

    async def test_completion_failure_rolls_back_ledger_then_retry_writes_once(self):
        service, state = self.make_service()
        self.assertTrue(hasattr(service, "atomic_confirmation"), "Draft and ledger must share one locked transaction")
        normal_complete = service.mark_completed
        async def broken(*args):
            raise RuntimeError("completion failure")
        service.mark_completed = broken
        with self.assertRaisesRegex(RuntimeError, "completion failure"):
            async with service.atomic_confirmation(17, 900001) as guard:
                state["ledger"].extend(["item1", "item2"])
                guard.succeeded = True
        self.assertEqual(state, {"status": "pending", "ledger": []})
        service.mark_completed = normal_complete
        async with service.atomic_confirmation(17, 900001) as guard:
            state["ledger"].extend(["item1", "item2"])
            guard.succeeded = True
        self.assertEqual(state, {"status": "completed", "ledger": ["item1", "item2"]})
        with self.assertRaisesRegex(ValueError, "draft_not_pending"):
            async with service.atomic_confirmation(17, 900001):
                self.fail("Already-completed draft must not reach the writer")

    async def test_failed_financial_validation_keeps_draft_pending(self):
        service, state = self.make_service()
        self.assertTrue(hasattr(service, "atomic_confirmation"))
        async with service.atomic_confirmation(17, 900001):
            pass
        self.assertEqual(state, {"status": "pending", "ledger": []})

if __name__ == "__main__":
    unittest.main()
