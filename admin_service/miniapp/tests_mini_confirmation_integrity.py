"""Confirmation regressions; run only against an explicitly isolated test DB.

The disposable runner sets MINI_CONFIRM_ISOLATED_TESTS and creates the receipt
schema. No test may fall through to the application's configured database.
"""
from __future__ import annotations

import copy
import json
import unittest
from contextlib import ExitStack
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.conf import settings
from django.db import connection
from django.test import RequestFactory
from django.utils import timezone

from miniapp import models, views


class Session(dict):
    modified = False


@unittest.skipUnless(getattr(settings, "MINI_CONFIRM_ISOLATED_TESTS", False), "requires an isolated confirmation test database")
class ConfirmationIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.user = SimpleNamespace(tg_user_id=930001, lang="en", base_currency="UAH")
        self.factory = RequestFactory()
        models.WriteReceipt.objects.all().delete()
        if hasattr(models, "DraftAction"):
            models.DraftAction.objects.all().delete()
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM mini_confirmation_probe_ledger")
        self.gate = patch.object(views, "_authorized_write_user_or_blocked", return_value=(self.user, {"mode": "active"}))
        self.gate.start()
        self.addCleanup(self.gate.stop)

    def request(self, payload, session):
        request = self.factory.post("/app/api/transactions/confirm", data=json.dumps(payload), content_type="application/json")
        request.session = session
        return request

    def new_transaction(self, session=None, payload=None):
        session = session if session is not None else Session()
        with patch.object(views, "build_transaction_draft", side_effect=lambda user, payload, draft_id: {"draft_id": draft_id, "amount_value": "100.00"}):
            response = views.transaction_draft(self.request(payload or {}, session))
        self.assertEqual(response.status_code, 200)
        return json.loads(response.content)["draft"]["draft_id"], session

    def ledger_commit(self, user, draft):
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO mini_confirmation_probe_ledger (amount) VALUES (%s)", [draft["amount_value"]])
        return {"status": "completed", "amount": draft["amount_value"]}

    def ledger_count(self):
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM mini_confirmation_probe_ledger")
            return cursor.fetchone()[0]

    def test_independent_session_snapshots_with_distinct_keys_write_once(self):
        draft_id, session = self.new_transaction()
        snapshot = copy.deepcopy(session)
        with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
            first = views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": "confirm-first-key-0001"}, session))
            second = views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": "confirm-other-key-0002"}, snapshot))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(self.ledger_count(), 1, "one reviewed draft must not produce two ledger effects")
        self.assertTrue(json.loads(second.content)["idempotent"])
        self.assertEqual(json.loads(first.content)["result"], json.loads(second.content)["result"])

    FLOW_CASES = (
        ("transaction", "TRANSACTION_DRAFTS_SESSION_KEY", "build_transaction_draft", "commit_transaction_draft"),
        ("account", "ACCOUNT_DRAFTS_SESSION_KEY", "build_account_draft", "commit_account_draft"),
        ("transfer", "TRANSFER_DRAFTS_SESSION_KEY", "build_transfer_draft", "commit_transfer_draft"),
        ("debt", "DEBT_DRAFTS_SESSION_KEY", "build_debt_draft", "commit_debt_draft"),
        ("saving_task", "SAVING_TASK_DRAFTS_SESSION_KEY", "build_saving_task_draft", "commit_saving_task_draft"),
        ("history_edit", "HISTORY_EDIT_DRAFTS_SESSION_KEY", "build_history_edit_draft", "commit_history_edit_draft"),
        ("history_void", "HISTORY_VOID_DRAFTS_SESSION_KEY", None, None),
        ("onboarding", "ONBOARDING_DRAFTS_SESSION_KEY", "build_onboarding_draft", "commit_onboarding_draft"),
    )

    def flow_seams(self, stack, name, builder, commit):
        build = lambda *args, draft_id, **kwargs: {"draft_id": draft_id, "action": "create", "amount_value": "100.00"}
        if builder:
            stack.enter_context(patch.object(views, builder, side_effect=build))
            stack.enter_context(patch.object(views, commit, side_effect=self.ledger_commit))
        else:
            stack.enter_context(patch.object(views.TransactionCancellationService, "build_draft", side_effect=build))
            stack.enter_context(patch.object(views.TransactionCancellationService, "commit_draft", side_effect=lambda draft: self.ledger_commit(self.user, draft)))
        stack.enter_context(patch.object(views, "build_saving_tasks_payload", return_value={"tasks": []}))
        stack.enter_context(patch.object(views, "_onboarding_user_or_response", return_value=self.user))
        stack.enter_context(patch.object(views, "onboarding_status", return_value={"completed": False}))
        stack.enter_context(patch.object(views, "_install_nudge_state"))

    def test_sibling_confirmation_paths_share_durable_once_contract(self):
        for name, key, builder, commit in self.FLOW_CASES:
            with self.subTest(flow=name), ExitStack() as stack:
                self.flow_seams(stack, name, builder, commit)
                session = Session()
                created = getattr(views, name + "_draft")(self.request({}, session))
                self.assertEqual(created.status_code, 200)
                draft_id = json.loads(created.content)["draft"]["draft_id"]
                snapshot = copy.deepcopy(session)
                before = self.ledger_count()
                first = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-first-key-0001"}, session))
                second = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-second-key-0002"}, snapshot))
                self.assertEqual(first.status_code, 200, first.content)
                self.assertEqual(second.status_code, 200, second.content)
                self.assertEqual(self.ledger_count() - before, 1)
                self.assertTrue(json.loads(second.content)["idempotent"])

    def test_committed_session_cannot_authorize_another_drafts_key(self):
        first_id, first_session = self.new_transaction()
        second_id, second_session = self.new_transaction()
        with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
            views.transaction_confirm(self.request({"draft_id": first_id, "idempotency_key": "original-first-key-0001"}, first_session))
            views.transaction_confirm(self.request({"draft_id": second_id, "idempotency_key": "original-second-key-0002"}, second_session))
            response = views.transaction_confirm(self.request({"draft_id": first_id, "idempotency_key": "original-second-key-0002"}, first_session))
        self.assertEqual(response.status_code, 409)
        self.assertEqual(json.loads(response.content)["error"]["code"], "idempotency_key_conflict")
        self.assertEqual(self.ledger_count(), 2)

    def test_committed_session_cannot_bypass_payload_binding(self):
        draft_id, session = self.new_transaction()
        with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
            views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": "confirm-first-key-0001"}, session))
            session[views.TRANSACTION_DRAFTS_SESSION_KEY][draft_id]["draft"]["amount_value"] = "900.00"
            response = views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": "confirm-other-key-0002"}, session))
        self.assertEqual(response.status_code, 409)
        self.assertEqual(json.loads(response.content)["error"]["code"], "draft_payload_conflict")
        self.assertEqual(self.ledger_count(), 1)

    def test_cancelled_drafts_and_preloaded_snapshots_are_terminal(self):
        for name, key, builder, commit in self.FLOW_CASES:
            if name == "onboarding":
                continue
            with self.subTest(flow=name), ExitStack() as stack:
                self.flow_seams(stack, name, builder, commit)
                session = Session()
                created = getattr(views, name + "_draft")(self.request({}, session))
                draft_id = json.loads(created.content)["draft"]["draft_id"]
                snapshot = copy.deepcopy(session)
                cancel_name = "saving_task_cancel_draft" if name == "saving_task" else name + "_cancel"
                cancelled = getattr(views, cancel_name)(self.request({"draft_id": draft_id}, session))
                self.assertEqual(cancelled.status_code, 200)
                for current_session in (snapshot, session):
                    response = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": "cancelled-fresh-key-0001"}, current_session))
                    self.assertEqual(response.status_code, 409, response.content)
                    self.assertEqual(json.loads(response.content)["error"]["code"], "draft_cancelled")
        self.assertEqual(self.ledger_count(), 0)

    def test_cancel_after_commit_preserves_terminal_receipt(self):
        for name, key, builder, commit in self.FLOW_CASES:
            if name == "onboarding":
                continue
            with self.subTest(flow=name), ExitStack() as stack:
                self.flow_seams(stack, name, builder, commit)
                session = Session()
                created = getattr(views, name + "_draft")(self.request({}, session))
                draft_id = json.loads(created.content)["draft"]["draft_id"]
                stale_cancel_session = copy.deepcopy(session)
                response = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-committed-key-0001"}, session))
                self.assertEqual(response.status_code, 200)
                cancel_name = "saving_task_cancel_draft" if name == "saving_task" else name + "_cancel"
                getattr(views, cancel_name)(self.request({"draft_id": draft_id}, stale_cancel_session))
                self.assertEqual(stale_cancel_session[getattr(views, key)][draft_id]["status"], "committed")
                replay = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-replay-key-0002"}, stale_cancel_session))
                self.assertEqual(replay.status_code, 200)
                self.assertTrue(json.loads(replay.content)["idempotent"])

    def test_expired_and_unknown_states_cannot_execute(self):
        for status in ("cancelled", "expired", "unexpected", "draft"):
            with self.subTest(status=status):
                draft_id, session = self.new_transaction()
                values = {"status": status}
                if status == "draft":
                    values["expires_at"] = timezone.now() - timedelta(seconds=1)
                models.DraftAction.objects.filter(draft_id=draft_id).update(**values)
                with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
                    response = views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": status + "-terminal-new-key-0001"}, session))
                self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(self.ledger_count(), 0)

    def test_distinct_drafts_for_one_statement_item_do_not_double_book(self):
        session = Session({views.AI_IMAGE_BATCH_SESSION_KEY: {"batch_id": "batch-1", "items": [{"id": "item-1", "status": "pending"}]}})
        payload = {"ai_image_batch_id": "batch-1", "ai_image_batch_item_id": "item-1"}
        first_id, session = self.new_transaction(session, payload)
        second_id, session = self.new_transaction(session, payload)
        snapshot = copy.deepcopy(session)
        with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
            first = views.transaction_confirm(self.request({"draft_id": first_id, "idempotency_key": "statement-first-key-0001"}, session))
            second = views.transaction_confirm(self.request({"draft_id": second_id, "idempotency_key": "statement-second-key-0002"}, snapshot))
            replay = views.transaction_confirm(self.request({"draft_id": first_id, "idempotency_key": "statement-replay-key-0003"}, session))
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 409)
        self.assertEqual(json.loads(second.content)["error"]["code"], "batch_item_unavailable")
        self.assertEqual(replay.status_code, 200)
        self.assertTrue(json.loads(replay.content)["idempotent"])
        self.assertEqual(self.ledger_count(), 1)

    def test_internal_history_confirmation_requires_issued_unchanged_payload(self):
        with ExitStack() as stack:
            self.flow_seams(stack, "history_void", None, None)
            stack.enter_context(patch.object(views, "_internal_actor_or_response", return_value=self.user))
            response = views.internal_transaction_void_draft(self.request({}, Session()))
            draft = json.loads(response.content)["draft"]
            first = views.internal_transaction_void_confirm(self.request({"draft": draft, "idempotency_key": "internal-first-key-0001"}, Session()))
            self.assertEqual(first.status_code, 200, first.content)
            modified = dict(draft, amount_value="900.00")
            second = views.internal_transaction_void_confirm(self.request({"draft": modified, "idempotency_key": "internal-other-key-0002"}, Session()))
            self.assertEqual(second.status_code, 409)
            self.assertEqual(json.loads(second.content)["error"]["code"], "draft_payload_conflict")
            replay = views.internal_transaction_void_confirm(self.request({"draft": draft, "idempotency_key": "internal-replay-key-0003"}, Session()))
            self.assertEqual(replay.status_code, 200)
            self.assertTrue(json.loads(replay.content)["idempotent"])
        self.assertEqual(self.ledger_count(), 1)

    def test_family_creation_uses_same_durable_receipt_contract(self):
        session = Session()
        with patch.object(views, "build_family_payload", return_value={"mode": "personal"}), patch.object(views, "create_family", side_effect=lambda user, name: self.ledger_commit(user, {"amount_value": "100.00"})):
            created = views.family_create_draft(self.request({"name": "Shared"}, session))
            draft_id = json.loads(created.content)["draft"]["id"]
            snapshot = copy.deepcopy(session)
            first = views.family_create_confirm(self.request({"draft_id": draft_id, "idempotency_key": "family-first-key-0001"}, session))
            second = views.family_create_confirm(self.request({"draft_id": draft_id, "idempotency_key": "family-second-key-0002"}, snapshot))
        self.assertEqual(first.status_code, 200, first.content)
        self.assertEqual(second.status_code, 200, second.content)
        self.assertTrue(json.loads(second.content)["idempotent"])
        self.assertEqual(self.ledger_count(), 1)

    def test_every_sibling_rejects_modified_committed_preview(self):
        for name, key, builder, commit in self.FLOW_CASES:
            with self.subTest(flow=name), ExitStack() as stack:
                self.flow_seams(stack, name, builder, commit)
                session = Session()
                created = getattr(views, name + "_draft")(self.request({}, session))
                draft_id = json.loads(created.content)["draft"]["draft_id"]
                first = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-bound-preview-key-0001"}, session))
                self.assertEqual(first.status_code, 200)
                session[getattr(views, key)][draft_id]["draft"]["amount_value"] = "900.00"
                result = getattr(views, name + "_confirm")(self.request({"draft_id": draft_id, "idempotency_key": name + "-changed-preview-key-0002"}, session))
                self.assertEqual(result.status_code, 409)
                self.assertEqual(json.loads(result.content)["error"]["code"], "draft_payload_conflict")

    def test_cancel_survives_failure_to_save_the_session(self):
        draft_id, session = self.new_transaction()
        snapshot = copy.deepcopy(session)
        with patch.object(views, "_save_session_drafts", side_effect=RuntimeError("session save failure")):
            with self.assertRaisesRegex(RuntimeError, "session save failure"):
                views.transaction_cancel(self.request({"draft_id": draft_id}, session))
        with patch.object(views, "commit_transaction_draft", side_effect=self.ledger_commit):
            response = views.transaction_confirm(self.request({"draft_id": draft_id, "idempotency_key": "cancel-session-failed-key-0001"}, snapshot))
        self.assertEqual(response.status_code, 409)
        self.assertEqual(json.loads(response.content)["error"]["code"], "draft_cancelled")
        self.assertEqual(self.ledger_count(), 0)

    def test_billing_expense_also_consumes_one_durable_action(self):
        session = Session()
        pending = SimpleNamespace(amount="100.00", currency="UAH", transaction_date="2026-09-14", comment="Subscription", save=MagicMock())
        manager = MagicMock()
        manager.filter.return_value.first.return_value = pending
        manager.select_for_update.return_value = manager
        with ExitStack() as stack:
            self.flow_seams(stack, "billing_expense", "build_transaction_draft", "commit_transaction_draft")
            stack.enter_context(patch.object(views.AiTransactionDraft, "objects", manager))
            created = views.billing_expense_draft(self.request({"billing_draft_id": 111}, session))
            draft_id = json.loads(created.content)["draft"]["draft_id"]
            snapshot = copy.deepcopy(session)
            first = views.billing_expense_confirm(self.request({"draft_id": draft_id, "idempotency_key": "billing-first-key-0001"}, session))
            second = views.billing_expense_confirm(self.request({"draft_id": draft_id, "idempotency_key": "billing-other-key-0002"}, snapshot))
            self.assertEqual(first.status_code, 200, first.content)
            self.assertEqual(second.status_code, 200, second.content)
            self.assertTrue(json.loads(second.content)["idempotent"])
            views.billing_expense_cancel(self.request({"draft_id": draft_id}, snapshot))
            self.assertEqual(snapshot[views.BILLING_EXPENSE_DRAFTS_SESSION_KEY][draft_id]["status"], "committed")
        self.assertEqual(self.ledger_count(), 1)
