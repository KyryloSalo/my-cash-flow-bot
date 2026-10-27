import json
import unittest
from datetime import timedelta
from unittest.mock import patch
import time

from .tests_unit import SimpleTestCase
from django.core import signing
from django.utils import timezone


from .recognition_review import ReviewError, issue_review, resolve_review


class RecognitionReviewTests(SimpleTestCase):
    def setUp(self):
        self.binding = {"user_id": 7, "family_id": 11}
        self.suggestion = {
            "amount": 12.5,
            "currency": "USD",
            "transaction_date": "2026-10-26",
            "kind": "expense",
            "account_id": 3,
            "category_id": 9,
            "source": "receipt",
            "needs_review": True,
            "field_findings": {"amount": "low confidence"},
            "arithmetic_warnings": ["receipt total differs"],
        }
        self.actual = dict(self.suggestion)
        self.actual.pop("source")
        self.actual.pop("needs_review")
        self.actual.pop("field_findings")
        self.actual.pop("arithmetic_warnings")

    def test_issue_contains_signed_snapshot_and_issues(self):
        result = issue_review(self.binding, self.suggestion)
        self.assertEqual(result["version"], 1)
        self.assertIsInstance(result["token"], str)
        self.assertEqual(result["fields"]["amount"], {"value": 12.5, "state": "suggested", "source": "receipt"})
        self.assertTrue(any(i["field"] == "amount" for i in result["issues"]))

    def test_tampered_token_is_rejected(self):
        issued = issue_review(self.binding, self.suggestion)
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, {"recognition_review": {"token": issued["token"] + "x", "acknowledgements": [], "values": self.actual}})
        self.assertEqual(cm.exception.code, "invalid_token")

    def test_expired_token_is_rejected(self):
        with patch("miniapp.recognition_review.timezone.now", return_value=timezone.now() + timedelta(hours=2)):
            issued = issue_review(self.binding, self.suggestion)
        with patch("time.time", return_value=time.time() + 7200):
            with self.assertRaises(ReviewError) as cm:
                resolve_review(self.binding, {"recognition_review": {"token": issued["token"], "acknowledgements": [], "values": self.actual}})
        self.assertEqual(cm.exception.code, "expired_token")

    def test_different_actor_or_family_is_rejected(self):
        issued = issue_review(self.binding, self.suggestion)
        payload = {"recognition_review": {"token": issued["token"], "acknowledgements": [], "values": self.actual}}
        with self.assertRaises(ReviewError):
            resolve_review({"user_id": 8, "family_id": 11}, payload)
        with self.assertRaises(ReviewError):
            resolve_review({"user_id": 7, "family_id": 12}, payload)

    def test_missing_acknowledgement_and_stale_value_are_rejected(self):
        issued = issue_review(self.binding, self.suggestion)
        payload = {"recognition_review": {"token": issued["token"], "acknowledgements": [], "values": self.actual}}
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, payload)
        self.assertEqual(cm.exception.code, "missing_acknowledgement")
        payload["recognition_review"]["acknowledgements"] = [i["id"] for i in issued["issues"]]
        payload["recognition_review"]["values"] = dict(self.actual, amount=99)
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, payload)
        self.assertEqual(cm.exception.code, "stale_values")

    def test_corrected_receipt_resolves_and_signed_warnings_are_immutable(self):
        issued = issue_review(self.binding, self.suggestion)
        values = dict(self.actual, amount=13.0, comment="reviewed", receipt_items=[{"amount": 13}], receipt_total=13.0)
        payload = {"amount": 13.0, "currency": "USD", "transaction_date": "2026-10-26", "kind": "expense", "account_id": 3, "category_id": 9,
                   "comment": "reviewed", "receipt_items": [{"amount": 13}], "receipt_total": 13.0,
                   "recognition_review": {"token": issued["token"], "acknowledgements": [i["id"] for i in issued["issues"]], "values": values}}
        audit = resolve_review(self.binding, payload)
        self.assertEqual(audit["version"], 1)
        self.assertNotIn("token", audit)
        self.assertEqual(audit["source"], "receipt")
        self.assertEqual(audit["fields"]["amount"]["baseline"], "12.50")
        self.assertEqual(audit["fields"]["amount"]["value"], "13.00")
        self.assertTrue(any(i["code"] == "arithmetic_warning" for i in audit["resolved_issues"]))

    def test_no_review_block_returns_empty_dict(self):
        self.assertEqual(resolve_review(self.binding, {"amount": 1}), {})

    def test_review_binds_receipt_items_comment_and_total(self):
        issued = issue_review(self.binding, self.suggestion)
        payload = dict(self.actual, receipt_items=[{"amount": "12.50"}], receipt_total="12.50", comment="edited")
        values = dict(payload)
        payload["recognition_review"] = {"token": issued["token"], "acknowledgements": [i["id"] for i in issued["issues"]], "values": values}
        for field, stale in (("receipt_items", []), ("receipt_total", "13.00"), ("comment", "old")):
            with self.subTest(field=field):
                payload["recognition_review"]["values"] = dict(values, **{field: stale})
                with self.assertRaises(ReviewError):
                    resolve_review(self.binding, payload)

    def test_list_findings_are_not_silently_lost(self):
        issued = issue_review(self.binding, dict(self.suggestion, field_findings=["status_conflict", "type_conflict"]))
        self.assertTrue(any(i["code"] == "status_conflict" for i in issued["issues"]))

    def test_malformed_acknowledgement_returns_safe_error(self):
        issued = issue_review(self.binding, self.suggestion)
        with self.assertRaises(ReviewError):
            resolve_review(self.binding, {"recognition_review": {
                "token": issued["token"], "acknowledgements": [{}], "values": self.actual}})

    def test_unknown_kind_and_transfer_are_not_transactions(self):
        for kind in ("unknown", "transfer", "declined"):
            suggestion = dict(self.suggestion, kind=kind)
            issued = issue_review(self.binding, suggestion)
            self.assertTrue(any(i["code"] == "invalid_kind" for i in issued["issues"]))

    def test_unknown_status_type_are_manual_choices_and_missing_kind_is_explicit(self):
        suggestion = dict(self.suggestion, field_findings=["status_unknown", "type_unknown"], missing_fields=["kind"])
        issued = issue_review(self.binding, suggestion, attempt_id="attempt-1")
        self.assertTrue(any(i["id"] == "missing_kind" for i in issued["issues"]))
        values = dict(self.actual, kind="expense")
        payload = dict(values, recognition_attempt_id="attempt-1", recognition_review={"token":issued["token"], "acknowledgements":[i["id"] for i in issued["issues"]], "values":values})
        resolve_review(self.binding, payload)

    def test_canonical_equivalent_values_resolve(self):
        issued = issue_review(self.binding, dict(self.suggestion, amount="12.50"), attempt_id="attempt-1")
        values = dict(self.actual, amount="12.500", transaction_date="2026-10-26", receipt_items=[{"name": "x", "amount": "12.50"}], receipt_total="12.500", receipt_resolution="items")
        payload = dict(values, recognition_attempt_id="attempt-1", recognition_review={"token": issued["token"], "acknowledgements":[i["id"] for i in issued["issues"]], "values":values})
        audit = resolve_review(self.binding, payload)
        self.assertEqual(audit["fields"]["amount"]["value"], "12.50")

    def test_unknown_acknowledgement_is_rejected(self):
        issued = issue_review(self.binding, self.suggestion, attempt_id="attempt-1")
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, {"recognition_attempt_id":"attempt-1", "recognition_review":{"token":issued["token"], "acknowledgements":[i["id"] for i in issued["issues"]]+["bogus"], "values":self.actual}})
        self.assertEqual(cm.exception.code, "invalid_acknowledgement")

    def test_server_context_rejects_stale_and_replayed_review(self):
        issued = issue_review(self.binding, dict(self.suggestion, source="text"), attempt_id="attempt-1")
        review = {"token": issued["token"], "acknowledgements":[i["id"] for i in issued["issues"]], "values":dict(self.actual, receipt_resolution="items")}
        context = {"attempt_id":"attempt-1", "review_nonce":issued["review_nonce"], "source_fingerprint":issued["source_fingerprint"], "consumed":False}
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, {"recognition_attempt_id":"attempt-1", "recognition_review":review}, server_context=dict(context, consumed=True))
        self.assertEqual(cm.exception.code, "replayed_review")
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, {"recognition_attempt_id":"attempt-1", "recognition_review":review}, server_context=dict(context, source_fingerprint="other"))
        self.assertEqual(cm.exception.code, "stale_review")

    def test_nested_status_findings_remain_blocking_in_signed_review(self):
        for findings in ({"status": {"code": "pending"}},
                         {"amount": {"code": "status_conflict"}},
                         {"status": "declined"}):
            with self.subTest(findings=findings):
                issued = issue_review(self.binding, dict(self.suggestion, field_findings=findings))
                payload = dict(self.actual, recognition_review={
                    "token": issued["token"],
                    "acknowledgements": [issue["id"] for issue in issued["issues"]],
                    "values": self.actual,
                })
                with self.assertRaises(ReviewError) as cm:
                    resolve_review(self.binding, payload)
                self.assertEqual(cm.exception.code, "blocking_finding")

    def test_malformed_nested_findings_return_safe_error(self):
        for findings in ({"amount": {"code": ["pending"]}},
                         {"amount": []}, [{"code": {"nested": "pending"}}],
                         [{"code": "pending", "field": []}], [{}], "pending", 42):
            with self.subTest(findings=findings):
                with self.assertRaises(ReviewError) as cm:
                    issue_review(self.binding, dict(self.suggestion, field_findings=findings))
                self.assertEqual(cm.exception.code, "invalid_suggestion")

    def test_blocking_status_conflict_is_fail_closed(self):
        issued = issue_review(self.binding, dict(self.suggestion, field_findings=["status_conflict"]), attempt_id="attempt-1")
        with self.assertRaises(ReviewError) as cm:
            resolve_review(self.binding, {"recognition_attempt_id":"attempt-1", "recognition_review":{"token":issued["token"], "acknowledgements":[i["id"] for i in issued["issues"]], "values":self.actual}})
        self.assertEqual(cm.exception.code, "blocking_finding")


if __name__ == "__main__":
    unittest.main()
