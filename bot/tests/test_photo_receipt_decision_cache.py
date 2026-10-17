"""Rejected receipt decisions from before the crop-review fix must be recomputed."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config
import photo_shadow
from photo_pipeline import reconcile_extractions


class ReceiptDecisionCacheTests(unittest.TestCase):
    def test_pre_crop_review_decision_is_not_reused(self):
        result = reconcile_extractions(())
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(config, 'PHOTO_PIPELINE_CACHE_DIR', directory):
            photo_shadow._store_durable('receipt-cache-regression', result)
            path = Path(photo_shadow._durable_cache_path('receipt-cache-regression'))
            payload = json.loads(path.read_text(encoding='utf-8'))
            payload['version'] = 4
            path.write_text(json.dumps(payload), encoding='utf-8')
            self.assertIsNone(photo_shadow._load_durable('receipt-cache-regression'))

    def test_current_decision_cache_roundtrips(self):
        result = reconcile_extractions(())
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(config, 'PHOTO_PIPELINE_CACHE_DIR', directory):
            photo_shadow._store_durable('current-receipt-cache', result)
            loaded = photo_shadow._load_durable('current-receipt-cache')
            self.assertIsNotNone(loaded)
            self.assertEqual(loaded.trace, result.trace)
            self.assertEqual(loaded.accepted, result.accepted)
