from contextlib import nullcontext
from decimal import Decimal
from types import SimpleNamespace as NS
from unittest.mock import MagicMock, patch
from django.test import SimpleTestCase
from . import services as s


class ReceiptPersistenceTests(SimpleTestCase):
    def test_runtime_schema_owns_metadata_upgrade(self):
        from runtime_schema import CORE_STATEMENTS
        self.assertIn("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb", CORE_STATEMENTS)

    def test_confirm_creates_one_transaction_with_review_metadata(self):
        user = NS(tg_user_id=123)
        self.enterContext(patch.object(s, '_category_payload', return_value={}))
        account = NS(id=7, currency='UAH', balance=Decimal('100'), account_type='main', non_negative_account_type='main', credit_limit=None, save=MagicMock())
        draft = dict(kind='expense', amount_value='12.50', account_id=7, category_id=8, currency='UAH', transaction_date='2026-10-01', receipt_review=[{'name':'Item', 'amount':'12.50'}])
        with patch.object(s, 'locale_for_user', return_value='uk'), patch.object(s, '_resolve_finance_scope', return_value=NS(is_family=False)), patch.object(s.db_transaction, 'atomic', return_value=nullcontext()), patch.object(s, '_active_scoped_account', return_value=account), patch.object(s, '_active_scoped_category', return_value=NS(id=8, name='Category')), patch.object(s.Transaction.objects, 'create', return_value=NS(id=1, created_at=None)) as create, patch.object(s, 'enqueue_transaction_created'), patch.object(s, '_account_payload', return_value={}):
            result = s.commit_transaction_draft(user, draft)
        create.assert_called_once()
        expected = {'receipt_review':[{'description':'Item', 'status':'included', 'line_total':'12.50'}]}
        self.assertEqual(create.call_args.kwargs['metadata'], expected)
        self.assertEqual(result['transaction']['metadata'], expected)
