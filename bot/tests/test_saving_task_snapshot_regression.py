from __future__ import annotations
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from decimal import Decimal
import sys
import unittest
from unittest.mock import AsyncMock, patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from savings_service import SavingsService


class SavingTaskSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_task_changed_after_preview_is_rejected_without_transfer(self):
        @asynccontextmanager
        async def transaction():
            yield
        original = {"id": 1, "status": "pending", "amount": Decimal("50"), "source_account_id": 1, "target_account_id": 2, "source_currency": "UAH", "target_currency": "UAH"}
        for changed in ({"amount": Decimal("500")}, {"target_account_id": 3}, {"source_currency": "USD"}):
            with self.subTest(changed=changed):
                current = {**original, **changed}
                conn = SimpleNamespace(transaction=transaction, fetchrow=AsyncMock(return_value=current), execute=AsyncMock())
                transfer = SimpleNamespace(execute_transfer=AsyncMock())
                with patch("savings_service.TransferService", return_value=transfer):
                    result = await SavingsService(conn).confirm_pending_task(900001, 1, fx_rate=None, rate_source=None, expected_task=deepcopy(original))
                self.assertEqual(result.status, "stale_preview")
                transfer.execute_transfer.assert_not_awaited()
                conn.execute.assert_not_awaited()

if __name__ == "__main__":
    unittest.main()
