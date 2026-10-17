import json
import tempfile
from decimal import Decimal

import unittest
from unittest import mock

from bot import photo_shadow
from bot.photo_pipeline import ReconciliationResult


def make_result():
    return ReconciliationResult(
        accepted=True,
        requires_review=False,
        amount=Decimal("12.34"),
        currency="USD",
        status="completed",
        transaction_type="expense",
        evidence_ids=("e1",),
        findings=(),
        trace={"source": "test"},
    )


class DurableCacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = mock.patch.object(photo_shadow.config, "PHOTO_PIPELINE_CACHE_DIR", self.tmp.name)
        self.config.start(); self.addCleanup(self.config.stop)
        self.ttl = mock.patch.object(photo_shadow.config, "PHOTO_PIPELINE_CACHE_TTL_SECONDS", 86400)
        self.ttl.start(); self.addCleanup(self.ttl.stop)

    def test_store_writes_versioned_envelope_and_loads_exact_key(self):
        photo_shadow._store_durable("key-a", make_result())
        path = photo_shadow._durable_cache_path("key-a")
        with open(path, encoding="utf-8") as handle:
            envelope = json.load(handle)
        self.assertEqual(envelope["version"], photo_shadow._DURABLE_CACHE_VERSION)
        self.assertEqual(envelope["producer"], photo_shadow._DURABLE_CACHE_PRODUCER)
        self.assertEqual(envelope["key"], "key-a")
        self.assertIn("result", envelope)
        loaded = photo_shadow._load_durable("key-a")
        self.assertEqual(loaded.amount, Decimal("12.34"))
        self.assertEqual(loaded.currency, "USD")
        self.assertEqual(loaded.evidence_ids, ("e1",))
        self.assertIsNone(photo_shadow._load_durable("key-b"))

    def test_load_rejects_legacy_malformed_and_mismatched_envelopes(self):
        path = photo_shadow._durable_cache_path("key-a")
        for payload in (
            {"accepted": True},
            {"version": 999, "key": "key-a", "result": {}},
            {"version": photo_shadow._DURABLE_CACHE_VERSION, "producer": "other", "key": "key-a", "result": {}},
            {"version": photo_shadow._DURABLE_CACHE_VERSION, "key": "other", "result": {}},
            {"version": photo_shadow._DURABLE_CACHE_VERSION, "key": "key-a", "result": "bad"},
        ):
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            self.assertIsNone(photo_shadow._load_durable("key-a"))

    def test_cache_directory_errors_are_best_effort(self):
        with mock.patch.object(photo_shadow.os, "makedirs", side_effect=OSError("read-only")):
            self.assertIsNone(photo_shadow._load_durable("key"))
            photo_shadow._store_durable("key", make_result())

    def test_store_cleanup_error_does_not_override_success(self):
        real_unlink = photo_shadow.os.unlink

        def failing_cleanup(path):
            if path.endswith(".tmp"):
                raise OSError("cleanup denied")
            return real_unlink(path)

        with mock.patch.object(photo_shadow.os, "unlink", side_effect=failing_cleanup):
            photo_shadow._store_durable("key", make_result())
        loaded = photo_shadow._load_durable("key")
        self.assertEqual(loaded.amount, Decimal("12.34"))
        self.assertTrue(loaded.accepted)
