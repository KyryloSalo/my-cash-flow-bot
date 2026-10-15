import json
import tempfile
import unittest
from pathlib import Path

from ops.photo_private_corpus_adapter import inspect_labels


class PrivateCorpusAdapterTests(unittest.TestCase):
    def test_metadata_only_marks_provisional_and_does_not_need_images(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = Path(directory) / "labels.json"
            labels.write_text(json.dumps({
                "file_count": 2,
                "unique_sha256_count": 1,
                "originals_unchanged": True,
                "items": [{"sha256": "a" * 64, "files": ["a.jpg", "b.jpg"], "ground_truth": {"document_type": "unknown"}}],
            }), encoding="utf-8")
            report = inspect_labels(labels)
            self.assertEqual(report["status"], "ready_metadata_only")
            self.assertEqual(report["file_count"], 2)
            self.assertEqual(report["unique_sha256_count"], 1)
            self.assertEqual(report["comparison_metrics"], "not computed")
            self.assertEqual(report["provider_calls"], 0)
            self.assertFalse(report["image_bytes_read"])

    def test_image_root_verifies_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            image = root / "a.jpg"
            image.write_bytes(b"mock-image")
            import hashlib
            digest = hashlib.sha256(b"mock-image").hexdigest()
            labels = root / "labels.json"
            labels.write_text(json.dumps({"items": [{"sha256": digest, "files": ["a.jpg"], "ground_truth": {}}]}), encoding="utf-8")
            report = inspect_labels(labels, root)
            self.assertEqual(report["status"], "ready_offline_corpus")
            self.assertEqual(report["sha256_verified_images"], 1)


if __name__ == "__main__":
    unittest.main()
