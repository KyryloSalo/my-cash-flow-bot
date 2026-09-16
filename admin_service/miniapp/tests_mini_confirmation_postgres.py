"""Disposable PostgreSQL proofs for MINI-03/05/06, no synthetic SQL seams."""
from __future__ import annotations

import copy
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from unittest.mock import patch

from django.conf import settings
from django.db import IntegrityError, connection, connections, transaction as db_transaction
from django.test import RequestFactory
from django.utils import timezone

from accounts.models import Account
from categories.models import Category
from transactions.models import Debt, Transaction
from users.models import TelegramUser
from miniapp import models, views
from miniapp.tests_mini_confirmation_integrity import Session


@unittest.skipUnless(getattr(settings, "MINI_CONFIRM_FULL_FINANCE_TESTS", False), "requires disposable PostgreSQL finance schema")
class ConfirmationPostgresTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(connection.vendor, "postgresql")
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_schema()")
            self.assertTrue(cursor.fetchone()[0].startswith("mini_confirm_"))
            cursor.execute("TRUNCATE users, accounts, categories, transactions, debts, debt_payments, pending_saving_tasks, miniapp_draft_actions, miniapp_write_receipts RESTART IDENTITY CASCADE")
        now = timezone.now()
        self.user = TelegramUser.objects.create(tg_user_id=930001, lang="en", base_currency="UAH", onboarding_completed=True, onboarding_version=3, created_at=now)
        self.source = Account.objects.create(tg_user=self.user, label="Source", currency="UAH", account_type="main", non_negative_account_type="main", starting_balance=Decimal("2000"), balance=Decimal("2000"), is_active=True, created_at=now, updated_at=now)
        self.target = Account.objects.create(tg_user=self.user, label="Target", currency="UAH", account_type="savings", non_negative_account_type="savings", starting_balance=Decimal("0"), balance=Decimal("0"), is_active=True, created_at=now, updated_at=now)
        self.category = Category.objects.create(tg_user=self.user, user=self.user, kind="expense", type="expense", name="Food", aliases=[], source="manual", is_active=True, created_at=now, updated_at=now)
        self.factory = RequestFactory()
        gate = patch.object(views, "_authorized_write_user_or_blocked", return_value=(self.user, {"mode": "active"}))
        gate.start()
        self.addCleanup(gate.stop)

    def request(self, payload, session):
        request = self.factory.post("/app/api/confirm", data=json.dumps(payload), content_type="application/json")
        request.session = session
        return request

    def draft(self, domain, payload, session=None):
        session = session if session is not None else Session()
        response = getattr(views, domain + "_draft")(self.request(payload, session))
        self.assertEqual(response.status_code, 200, response.content)
        draft = json.loads(response.content)["draft"]
        return draft["draft_id"], session, draft

    def normal_payload(self):
        return {"kind": "expense", "amount": "100", "currency": "UAH", "account_id": self.source.id, "category_id": self.category.id, "transaction_date": "2026-09-14", "comment": "Reviewed"}

    def confirm(self, domain, draft_id, key, session):
        return getattr(views, domain + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": key}, session))

    def parallel_requests(self, calls):
        barrier = threading.Barrier(len(calls))

        def invoke(call):
            conn = connections["default"]
            reached = False
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT pg_backend_pid()")
                    pid = cursor.fetchone()[0]

                def align(execute, sql, params, many, context):
                    nonlocal reached
                    if not reached and '"miniapp_draft_actions"' in sql and "FOR UPDATE" in sql:
                        reached = True
                        barrier.wait(timeout=10)
                    return execute(sql, params, many, context)

                with conn.execute_wrapper(align):
                    response = call()
                self.assertTrue(reached, "the actual durable action lock was never reached")
                return response.status_code, json.loads(response.content), pid
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            futures = [pool.submit(invoke, call) for call in calls]
            results = [future.result(timeout=30) for future in futures]
        self.assertEqual(len({row[2] for row in results}), len(calls), "requests must use independent DB connections")
        return results

    def assert_source(self, expected):
        self.source.refresh_from_db()
        self.assertEqual(self.source.balance, Decimal(expected))

    def test_normal_transfer_and_debt_distinct_key_races_book_once(self):
        cases = (
            ("transaction", self.normal_payload()),
            ("transfer", {"source_account_id": self.source.id, "target_account_id": self.target.id, "amount": "100", "transaction_date": "2026-09-14"}),
            ("debt", {"action": "create", "direction": "receivable", "counterparty_name": "Synthetic borrower", "amount": "100", "currency": "UAH", "account_id": self.source.id, "transaction_date": "2026-09-14"}),
        )
        for domain, payload in cases:
            with self.subTest(domain=domain):
                before = Transaction.objects.count()
                balance = Account.objects.get(id=self.source.id).balance
                draft_id, session, _ = self.draft(domain, payload)
                first_session, second_session = copy.deepcopy(session), copy.deepcopy(session)
                results = self.parallel_requests([
                    lambda: self.confirm(domain, draft_id, domain + "-race-first-key-0001", first_session),
                    lambda: self.confirm(domain, draft_id, domain + "-race-other-key-0002", second_session),
                ])
                self.assertEqual([row[0] for row in results], [200, 200], results)
                self.assertEqual(sorted(row[1]["idempotent"] for row in results), [False, True])
                self.assertEqual(Transaction.objects.count() - before, 1)
                self.assert_source(balance - Decimal("100"))
                self.assertEqual(models.DraftAction.objects.filter(draft_id=draft_id, status="committed").count(), 1)
        self.assertEqual(Debt.objects.count(), 1)

    def test_same_key_race_returns_one_receipt(self):
        draft_id, session, _ = self.draft("transaction", self.normal_payload())
        results = self.parallel_requests([
            lambda: self.confirm("transaction", draft_id, "same-concurrent-key-0001", copy.deepcopy(session)),
            lambda: self.confirm("transaction", draft_id, "same-concurrent-key-0001", copy.deepcopy(session)),
        ])
        self.assertEqual([row[0] for row in results], [200, 200], results)
        self.assertEqual(models.WriteReceipt.objects.count(), 1)
        self.assertEqual(Transaction.objects.count(), 1)
        self.assert_source("1900")

    def test_one_key_cannot_authorize_two_concurrent_drafts(self):
        first_id, first_session, _ = self.draft("transaction", self.normal_payload())
        second_id, second_session, _ = self.draft("transaction", self.normal_payload())
        results = self.parallel_requests([
            lambda: self.confirm("transaction", first_id, "conflicting-draft-key-0001", first_session),
            lambda: self.confirm("transaction", second_id, "conflicting-draft-key-0001", second_session),
        ])
        self.assertEqual(sorted(row[0] for row in results), [200, 409], results)
        self.assertEqual(next(row[1]["error"]["code"] for row in results if row[0] == 409), "idempotency_key_conflict")
        self.assertEqual(Transaction.objects.count(), 1)
        self.assert_source("1900")

    def test_statement_item_claim_serializes_distinct_drafts(self):
        session = Session({views.AI_IMAGE_BATCH_SESSION_KEY: {"batch_id": "pg-batch", "items": [{"id": "item-1", "status": "pending"}]}})
        payload = dict(self.normal_payload(), ai_image_batch_id="pg-batch", ai_image_batch_item_id="item-1")
        first_id, session, _ = self.draft("transaction", payload, session)
        second_id, session, _ = self.draft("transaction", payload, session)
        results = self.parallel_requests([
            lambda: self.confirm("transaction", first_id, "origin-first-key-0001", copy.deepcopy(session)),
            lambda: self.confirm("transaction", second_id, "origin-other-key-0002", copy.deepcopy(session)),
        ])
        self.assertEqual(sorted(row[0] for row in results), [200, 409], results)
        self.assertEqual(next(row[1]["error"]["code"] for row in results if row[0] == 409), "batch_item_unavailable")
        self.assertEqual(Transaction.objects.count(), 1)
        self.assert_source("1900")

    def test_session_save_failure_after_commit_does_not_duplicate_retry(self):
        draft_id, session, _ = self.draft("transaction", self.normal_payload())
        snapshot = copy.deepcopy(session)
        with patch.object(views, "_save_session_transaction_drafts", side_effect=RuntimeError("injected session save failure")):
            with self.assertRaisesRegex(RuntimeError, "session save failure"):
                self.confirm("transaction", draft_id, "session-failure-key-0001", session)
        replay = self.confirm("transaction", draft_id, "session-retry-key-0002", snapshot)
        self.assertEqual(replay.status_code, 200, replay.content)
        self.assertTrue(json.loads(replay.content)["idempotent"])
        self.assertEqual(Transaction.objects.count(), 1)
        self.assert_source("1900")

    def test_receipt_failure_rolls_back_finance_and_preserves_pending_action(self):
        draft_id, session, _ = self.draft("transaction", self.normal_payload())
        original_save = models.WriteReceipt.save

        def fail_after_finance(receipt, *args, **kwargs):
            if receipt.status == "committed":
                self.assertEqual(Transaction.objects.count(), 1)
                raise IntegrityError("injected receipt failure")
            return original_save(receipt, *args, **kwargs)

        with patch.object(models.WriteReceipt, "save", new=fail_after_finance):
            with self.assertRaisesRegex(IntegrityError, "receipt failure"):
                self.confirm("transaction", draft_id, "failed-receipt-key-0001", session)
        self.assertEqual(models.DraftAction.objects.get(draft_id=draft_id).status, "draft")
        self.assertEqual(models.WriteReceipt.objects.count(), 0)
        self.assertEqual(Transaction.objects.count(), 0)
        self.assert_source("2000")
        result = self.confirm("transaction", draft_id, "retried-receipt-key-0002", session)
        self.assertEqual(result.status_code, 200, result.content)
        self.assertEqual(Transaction.objects.count(), 1)
        self.assert_source("1900")

    def test_cancel_confirm_race_has_one_terminal_outcome(self):
        draft_id, session, _ = self.draft("transaction", self.normal_payload())
        results = self.parallel_requests([
            lambda: self.confirm("transaction", draft_id, "racing-cancel-key-0001", copy.deepcopy(session)),
            lambda: views.transaction_cancel(self.request({"draft_id": draft_id}, copy.deepcopy(session))),
        ])
        action = models.DraftAction.objects.get(draft_id=draft_id)
        if action.status == "cancelled":
            self.assertEqual(results[0][0], 409, results)
            self.assertEqual(Transaction.objects.count(), 0)
            self.assert_source("2000")
        else:
            self.assertEqual(action.status, "committed")
            self.assertEqual(results[0][0], 200, results)
            self.assertEqual(Transaction.objects.count(), 1)
            self.assert_source("1900")

    def create_saving_task(self):
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO pending_saving_tasks (tg_user_id,source_account_id,target_account_id,amount,currency) VALUES (%s,%s,%s,100,'UAH') RETURNING id", [self.user.tg_user_id, self.source.id, self.target.id])
            return cursor.fetchone()[0]

    def cancellable_preview(self, domain):
        if domain == "saving_task":
            payload = {"task_id": self.create_saving_task(), "action": "confirm"}
        else:
            seed_id, seed_session, _ = self.draft("transaction", self.normal_payload())
            seeded = self.confirm("transaction", seed_id, seed_id + "-seed-key", seed_session)
            self.assertEqual(seeded.status_code, 200, seeded.content)
            payload = dict(self.normal_payload(), transaction_id=Transaction.objects.latest("id").id, amount="150")
        draft_id, session, _ = self.draft(domain, payload)
        cancel = views.saving_task_cancel_draft if domain == "saving_task" else getattr(views, domain + "_cancel")
        return draft_id, session, cancel

    def finance_state(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT id,status,amount,target_account_id FROM pending_saving_tasks ORDER BY id")
            tasks = cursor.fetchall()
        return (
            list(Account.objects.order_by("id").values_list("id", "balance")),
            list(Transaction.objects.order_by("id").values_list("id", "amount", "is_deleted", "deleted_at")),
            tasks,
        )

    def test_history_and_saving_cancel_prevents_execution_from_stale_sessions(self):
        for domain in ("history_edit", "history_void", "saving_task"):
            with self.subTest(domain=domain):
                draft_id, session, cancel = self.cancellable_preview(domain)
                snapshot = copy.deepcopy(session)
                before = self.finance_state()
                cancelled = cancel(self.request({"draft_id": draft_id}, session))
                self.assertEqual(cancelled.status_code, 200, cancelled.content)
                for stale_session in (session, snapshot):
                    response = self.confirm(domain, draft_id, draft_id + "-after-cancel-key", stale_session)
                    self.assertEqual(response.status_code, 409, response.content)
                    self.assertEqual(json.loads(response.content)["error"]["code"], "draft_cancelled")
                self.assertEqual(self.finance_state(), before)
                self.assertEqual(models.DraftAction.objects.get(draft_id=draft_id).status, "cancelled")

    def test_history_and_saving_cancel_cannot_reopen_committed_intent(self):
        for domain in ("history_edit", "history_void", "saving_task"):
            with self.subTest(domain=domain):
                draft_id, session, cancel = self.cancellable_preview(domain)
                snapshot = copy.deepcopy(session)
                first = self.confirm(domain, draft_id, draft_id + "-first-key", session)
                self.assertEqual(first.status_code, 200, first.content)
                before = self.finance_state()
                cancelled = cancel(self.request({"draft_id": draft_id}, snapshot))
                self.assertEqual(cancelled.status_code, 200, cancelled.content)
                replay = self.confirm(domain, draft_id, draft_id + "-after-terminal-key", snapshot)
                self.assertEqual(replay.status_code, 200, replay.content)
                self.assertTrue(json.loads(replay.content)["idempotent"])
                self.assertEqual(json.loads(replay.content)["result"], json.loads(first.content)["result"])
                self.assertEqual(self.finance_state(), before)
                self.assertEqual(models.DraftAction.objects.get(draft_id=draft_id).status, "committed")

    def test_history_and_saving_cancel_confirm_races_have_one_terminal_state(self):
        for domain in ("history_edit", "history_void", "saving_task"):
            with self.subTest(domain=domain):
                draft_id, session, cancel = self.cancellable_preview(domain)
                before = self.finance_state()
                results = self.parallel_requests([
                    lambda: self.confirm(domain, draft_id, draft_id + "-race-key", copy.deepcopy(session)),
                    lambda: cancel(self.request({"draft_id": draft_id}, copy.deepcopy(session))),
                ])
                action = models.DraftAction.objects.get(draft_id=draft_id)
                self.assertEqual(results[1][0], 200, results)
                if action.status == "cancelled":
                    self.assertEqual(results[0][0], 409, results)
                    self.assertEqual(results[0][1]["error"]["code"], "draft_cancelled")
                    self.assertEqual(self.finance_state(), before)
                else:
                    self.assertEqual(action.status, "committed")
                    self.assertEqual(results[0][0], 200, results)
                    after = self.finance_state()
                    self.assertNotEqual(after, before)
                    replay = self.confirm(domain, draft_id, draft_id + "-race-replay-key", session)
                    self.assertEqual(replay.status_code, 200, replay.content)
                    self.assertTrue(json.loads(replay.content)["idempotent"])
                    self.assertEqual(self.finance_state(), after)

    def test_saving_currency_refresh_occurs_after_account_lock(self):
        task_id = self.create_saving_task()
        draft_id, session, _ = self.draft("saving_task", {"task_id": task_id, "action": "confirm"})
        task_read = threading.Event()

        def confirm_after_task_read():
            conn = connections["default"]
            try:
                def observe(execute, sql, params, many, context):
                    if 'FROM "accounts"' in sql and "FOR UPDATE" in sql:
                        task_read.set()
                    return execute(sql, params, many, context)

                with conn.execute_wrapper(observe):
                    return self.confirm("saving_task", draft_id, "currency-under-lock-key-0001", session)
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=1) as pool:
            with db_transaction.atomic():
                Account.objects.filter(id=self.target.id).update(currency="EUR")
                future = pool.submit(confirm_after_task_read)
                self.assertTrue(task_read.wait(timeout=10), "confirmation never reached account locking after reading the old task snapshot")
            response = future.result(timeout=30)
        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(json.loads(response.content)["error"]["code"], "saving_task_changed")
        self.assertEqual(Transaction.objects.count(), 0)
        self.assert_source("2000")

    def test_changed_saving_task_returns_409_then_fresh_preview_books_once(self):
        task_id = self.create_saving_task()
        payload = {"task_id": task_id, "action": "confirm", "transaction_date": "2026-09-14"}
        draft_id, session, _ = self.draft("saving_task", payload)
        other = Account.objects.create(tg_user=self.user, label="New target", currency="UAH", account_type="savings", non_negative_account_type="savings", starting_balance=0, balance=0, is_active=True, created_at=timezone.now(), updated_at=timezone.now())
        with connection.cursor() as cursor:
            cursor.execute("UPDATE pending_saving_tasks SET amount=900,target_account_id=%s WHERE id=%s", [other.id, task_id])
        stale = self.confirm("saving_task", draft_id, "stale-saving-key-0001", session)
        self.assertEqual(stale.status_code, 409, stale.content)
        self.assertEqual(json.loads(stale.content)["error"]["code"], "saving_task_changed")
        self.assertEqual(Transaction.objects.count(), 0)
        self.assert_source("2000")
        fresh_id, fresh_session, _ = self.draft("saving_task", payload)
        snapshot = copy.deepcopy(fresh_session)
        fresh = self.confirm("saving_task", fresh_id, "fresh-saving-key-0002", fresh_session)
        replay = self.confirm("saving_task", fresh_id, "fresh-saving-key-0003", snapshot)
        self.assertEqual(fresh.status_code, 200, fresh.content)
        self.assertEqual(replay.status_code, 200, replay.content)
        self.assertTrue(json.loads(replay.content)["idempotent"])
        self.assertEqual(Transaction.objects.count(), 1)
        self.assertEqual(Transaction.objects.get().transfer_subtype, "savings_transfer")
        self.assert_source("1100")
        other.refresh_from_db()
        self.target.refresh_from_db()
        self.assertEqual(other.balance, Decimal("900"))
        self.assertEqual(self.target.balance, Decimal("0"))
        with connection.cursor() as cursor:
            cursor.execute("SELECT status FROM pending_saving_tasks WHERE id=%s", [task_id])
            self.assertEqual(cursor.fetchone()[0], "completed")

    def test_saving_preview_rejects_changed_currency_and_handled_tasks(self):
        for status in ("pending", "cancelled", "completed"):
            with self.subTest(status=status):
                Account.objects.filter(id=self.target.id).update(currency="UAH")
                task_id = self.create_saving_task()
                draft_id, session, _ = self.draft("saving_task", {"task_id": task_id, "action": "confirm"})
                if status == "pending":
                    Account.objects.filter(id=self.target.id).update(currency="EUR")
                else:
                    with connection.cursor() as cursor:
                        cursor.execute("UPDATE pending_saving_tasks SET status=%s WHERE id=%s", [status, task_id])
                result = self.confirm("saving_task", draft_id, status + "-saving-terminal-key-0001", session)
                self.assertEqual(result.status_code, 409, result.content)
        self.assertEqual(Transaction.objects.count(), 0)
        self.assert_source("2000")
