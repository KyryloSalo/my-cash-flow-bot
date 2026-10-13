import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from capture_adapter import CaptureQueue, ValidationError, build_capture_event


class BuildCaptureEventTests(unittest.TestCase):
    def test_builds_manual_share_event_with_normalized_decimal(self):
        event = build_capture_event(
            source="ios_manual_share",
            bank="unknown",
            observed_at="2026-10-13T12:30:00+03:00",
            amount="1 234,50",
            currency="uah",
            merchant="  Test   Merchant  ",
            operation_hint="purchase",
            parser_version="ios-manual-share/1",
        )

        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["amount"], "1234.50")
        self.assertEqual(event["currency"], "UAH")
        self.assertEqual(event["merchant"], "Test Merchant")
        self.assertEqual(event["source"], "ios_manual_share")
        self.assertNotIn("raw_text", event)

    def test_incomplete_data_remains_null_and_unknown(self):
        event = build_capture_event(
            source="ios_manual_share",
            bank="unknown",
            observed_at="2026-10-13T09:30:00Z",
            amount=None,
            currency=None,
            merchant=None,
            operation_hint="unknown",
            parser_version="ios-manual-share/1",
        )

        self.assertIsNone(event["amount"])
        self.assertIsNone(event["currency"])
        self.assertIsNone(event["merchant"])
        self.assertEqual(event["operation_hint"], "unknown")

    def test_rejects_automatic_source_until_physical_gate_passes(self):
        with self.assertRaisesRegex(ValidationError, "physical-device gate"):
            build_capture_event(
                source="ios_notification_automation",
                bank="unknown",
                observed_at="2026-10-13T09:30:00Z",
                amount="10",
                currency="UAH",
                merchant="Shop",
                operation_hint="purchase",
                parser_version="ios-notification/1",
            )

    def test_rejects_sensitive_merchant_content(self):
        with self.assertRaisesRegex(ValidationError, "sensitive"):
            build_capture_event(
                source="ios_manual_share",
                bank="unknown",
                observed_at="2026-10-13T09:30:00Z",
                amount="10",
                currency="UAH",
                merchant="OTP 123456",
                operation_hint="purchase",
                parser_version="ios-manual-share/1",
            )


class CaptureQueueTests(unittest.TestCase):
    def test_retry_reuses_event_id_and_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = CaptureQueue(Path(tmp) / "queue.json")
            first = queue.enqueue(
                {
                    "source": "ios_manual_share",
                    "bank": "unknown",
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "amount": "25.00",
                    "currency": "UAH",
                    "merchant": "Cafe",
                    "operation_hint": "purchase",
                    "parser_version": "ios-manual-share/1",
                },
                local_key="share-sheet-item-1",
            )
            second = queue.enqueue(first, local_key="share-sheet-item-1")

            self.assertEqual(first["event_id"], second["event_id"])
            self.assertEqual(len(queue.pending()), 1)
            stored = json.loads((Path(tmp) / "queue.json").read_text(encoding="utf-8"))
            self.assertNotIn("raw_text", json.dumps(stored))

    def test_same_local_key_with_changed_financial_payload_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = CaptureQueue(Path(tmp) / "queue.json")
            base = {
                "source": "ios_manual_share",
                "bank": "unknown",
                "observed_at": "2026-10-13T09:30:00Z",
                "amount": "25.00",
                "currency": "UAH",
                "merchant": "Cafe",
                "operation_hint": "purchase",
                "parser_version": "ios-manual-share/1",
            }
            queue.enqueue(base, local_key="share-sheet-item-1")
            changed = dict(base, amount="30.00")

            with self.assertRaisesRegex(ValidationError, "payload conflict"):
                queue.enqueue(changed, local_key="share-sheet-item-1")

    def test_unknown_or_raw_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = CaptureQueue(Path(tmp) / "queue.json")
            candidate = {
                "source": "ios_manual_share",
                "bank": "unknown",
                "observed_at": "2026-10-13T09:30:00Z",
                "amount": "25.00",
                "currency": "UAH",
                "merchant": "Cafe",
                "operation_hint": "purchase",
                "parser_version": "ios-manual-share/1",
                "raw_text": "full bank notification",
            }

            with self.assertRaisesRegex(ValidationError, "unknown fields"):
                queue.enqueue(candidate, local_key="share-sheet-item-1")


if __name__ == "__main__":
    unittest.main()
