import json
import hashlib
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from photo_recognition_benchmark import run_benchmark


class PhotoBenchmarkTests(unittest.TestCase):
    def make_case(self, root, *, expected=None):
        image = root / "photo.json"
        image.write_bytes(b"fixture")
        passes = [{"source": "full", "evidence": [
            {"id": "a", "field": "amount", "raw_text": "10.00", "value": "10"},
            {"id": "c", "field": "currency", "raw_text": "USD", "value": "USD"},
            {"id": "s", "field": "status", "raw_text": "paid", "value": "completed"},
            {"id": "t", "field": "type", "raw_text": "payment", "value": "expense"},
        ]}, {"source": "tile", "evidence": [
            {"id": "a2", "field": "amount", "raw_text": "10.00", "value": "10"},
            {"id": "c2", "field": "currency", "raw_text": "USD", "value": "USD"},
            {"id": "s2", "field": "status", "raw_text": "paid", "value": "completed"},
            {"id": "t2", "field": "type", "raw_text": "payment", "value": "expense"},
        ]}]
        data = root / "passes.json"
        data.write_text(json.dumps(passes), encoding="utf-8")
        return {"id": "ok", "image": image.name, "sha256": hashlib.sha256(b"fixture").hexdigest(), "extractions": data.name, "expected": expected or {"accepted": True, "findings_include": [], "findings_exclude": []}}

    def test_sha_and_expected_results_and_repeat_consistency(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            case = self.make_case(root)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "suite": "test", "cases": [case]}), encoding="utf-8")
            report = run_benchmark(manifest, repeat=10)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["metrics"]["cases"], 1)
            self.assertEqual(report["metrics"]["repeat_consistent_cases"], 1)
            self.assertEqual(report["cases"][0]["sha256_verified"], True)

    def test_missing_corpus_is_nonzero_and_honest(self):
        with tempfile.TemporaryDirectory() as d:
            manifest = Path(d) / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "suite": "not-release", "cases": [{"id": "missing", "image": "no.jpg", "sha256": "0" * 64, "extractions": "x.json", "expected": {"accepted": True, "findings_include": [], "findings_exclude": []}}]}), encoding="utf-8")
            result = subprocess.run([sys.executable, "-m", "photo_recognition_benchmark", str(manifest)], cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("missing corpus", result.stdout.lower())
            self.assertIn("accuracy", result.stdout.lower())
            self.assertIn("not computed", result.stdout.lower())

    def test_expected_include_exclude_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            case = self.make_case(root, expected={"accepted": True, "findings_include": ["made_up"], "findings_exclude": []})
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "suite": "test", "cases": [case]}), encoding="utf-8")
            report = run_benchmark(manifest, repeat=10)
            self.assertEqual(report["status"], "failed")
            self.assertIn("expected_findings_include_missing", report["cases"][0]["failures"])
    def test_rejects_non_positive_or_boolean_repeat(self):
        with tempfile.TemporaryDirectory() as d:
            manifest = Path(d) / "manifest.json"
            manifest.write_text(json.dumps({"version": 1, "cases": []}), encoding="utf-8")
            for repeat in (0, -1, True):
                with self.subTest(repeat=repeat), self.assertRaises(ValueError) as ctx:
                    run_benchmark(manifest, repeat=repeat)
                self.assertIn('"code": "invalid_repeat"', str(ctx.exception))


    def test_requires_strict_expected_shape(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for expected in ({}, {"accepted": 1, "findings_include": [], "findings_exclude": []},
                             {"accepted": True, "findings_include": []},
                             {"accepted": True, "findings_include": [1], "findings_exclude": []}):
                case = dict(self.make_case(root))
                case["expected"] = expected
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
                with self.subTest(expected=expected):
                    with self.assertRaises(ValueError) as ctx:
                        run_benchmark(manifest)
                    self.assertIn('"code":', str(ctx.exception))

    def test_rejects_malformed_pass_and_evidence_with_structured_error(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            case = self.make_case(root)
            for payload in ([{"source": "full"}], [{"source": "full", "evidence": [{"id": "x"}]}]):
                (root / "passes.json").write_text(json.dumps(payload), encoding="utf-8")
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "cases": [case]}), encoding="utf-8")
                with self.subTest(payload=payload), self.assertRaises(ValueError) as ctx:
                    run_benchmark(manifest)
                self.assertIn('"code":', str(ctx.exception))

    def test_rejects_absolute_and_traversing_fixture_paths_before_reading(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            outside = root.parent / "credential-sentinel.env"
            outside.write_text("SECRET=must-not-be-read", encoding="utf-8")
            for image in (str(outside), "../credential-sentinel.env"):
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"version": 1, "cases": [{
                    "id": "unsafe", "image": image, "sha256": "0" * 64, "extractions": "passes.json",
                    "expected": {"accepted": True, "findings_include": [], "findings_exclude": []}}]}), encoding="utf-8")
                with self.subTest(image=image), self.assertRaises(ValueError) as ctx:
                    run_benchmark(manifest)
                self.assertIn('"code": "unsafe_fixture_path"', str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
