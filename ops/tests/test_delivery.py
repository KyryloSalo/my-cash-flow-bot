"""Hermetic CLI contract tests; no credentials, network or product DB."""
import importlib.util
import io
import json
import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("delivery", ROOT / "ops/delivery.py")
delivery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(delivery)


class DeliveryCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "app.py"
        self.source.write_text("print('before')\n", encoding="utf-8")
        for args in (("init", "-q"), ("add", "app.py"),
                     ("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")):
            subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True)

    def cli(self, cmd, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                code = delivery.main([cmd, "--root", str(self.root), "--session-id", "s1", *args])
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 2
                if isinstance(exc.code, str):
                    stderr.write(exc.code)
        return code, stdout.getvalue(), stderr.getvalue()

    def start(self, *extra):
        code, out, err = self.cli("start", "--task-id", "t1", "--request", "Реалізуй точний сценарій",
                                  "--requirement", "PWA result visible", "--acceptance-command", "python smoke.py",
                                  "--no-review-required", "--review-waiver", "tiny fixture only", *extra)
        self.assertEqual(code, 0, err)
        return json.loads(out)

    def state(self):
        return json.loads((self.root / ".hermes/delivery/s1/task.json").read_text(encoding="utf-8"))

    def check(self, name="acceptance", kind="acceptance", candidate=None, environment="local", command=None):
        log = self.root / ".hermes/delivery/s1" / (name + ".log")
        log.write_text("real fixture command passed\n", encoding="utf-8")
        cand = candidate or self.state()["candidate"]
        return self.cli("record-check", "--name", name, "--kind", kind, "--status", "passed",
                        "--candidate", cand, "--environment", environment, "--exit-code", "0",
                        "--command", command or ("python smoke.py" if kind == "acceptance" else "fixture command"), "--evidence", str(log))

    def accepted(self, *start_args, preflight=True):
        self.start(*start_args)
        if preflight:
            self.assertEqual(self.cli("preflight")[0], 0)
        self.assertEqual(self.check("unit", "unit")[0], 0)
        self.assertEqual(self.check()[0], 0)
        self.assertEqual(self.cli("accept", "--requirement-id", "r1", "--check", "acceptance")[0], 0)

    def test_original_contract_survives_additions_and_archived_new_task(self):
        self.start()
        self.cli("clarification", "must preserve 51 days")
        self.cli("requirement", "show 51 days in PWA")
        self.assertEqual(self.state()["request"], "Реалізуй точний сценарій")
        self.assertEqual(len(self.state()["requirements"]), 2)
        self.assertEqual(self.state()["clarifications"], ["must preserve 51 days"])
        self.assertNotEqual(self.cli("start", "--request", "overwrite", "--requirement", "bad")[0], 0)
        self.start("--new-task")
        archives = list((self.root / ".hermes/delivery/s1").glob("task-*.json"))
        self.assertEqual(len(archives), 1)
        self.assertEqual(len(json.loads(archives[0].read_text(encoding="utf-8"))["requirements"]), 2)

    def test_traversal_session_is_rejected(self):
        self.assertRaises(SystemExit, delivery.session_arg, "..")
        self.assertRaises(SystemExit, delivery.session_arg, "../other")

    def test_preflight_reports_live_runtime_and_explicit_acceptance_route(self):
        self.start()
        code, out, err = self.cli("preflight")
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["acceptance_command"], "python smoke.py")
        self.assertTrue(json.loads(out)["interpreter"])

    def test_readiness_rejects_skipped_preflight_even_with_acceptance(self):
        self.accepted(preflight=False)
        self.assertIn("preflight", self.cli("readiness")[2])

    def test_readiness_rejects_missing_named_acceptance_route(self):
        self.accepted("--acceptance-command", "", preflight=False)
        self.assertEqual(self.cli("preflight")[0], 2)
        self.assertNotEqual(self.cli("readiness")[0], 0)

    def test_readiness_rejects_acceptance_for_a_different_route(self):
        self.accepted()
        self.assertEqual(self.check(command="unrelated scenario")[0], 0)
        self.assertIn("route", self.cli("readiness")[2])

    def test_source_change_requires_fresh_preflight(self):
        self.accepted()
        self.source.write_text("print('after')\n", encoding="utf-8")
        self.assertEqual(self.cli("snapshot")[0], 0)
        self.assertEqual(self.check("unit", "unit")[0], 0)
        self.assertEqual(self.check()[0], 0)
        self.assertEqual(self.cli("accept", "--requirement-id", "r1", "--check", "acceptance")[0], 0)
        self.assertIn("preflight", self.cli("readiness")[2])
        self.assertEqual(self.cli("preflight")[0], 0)
        self.assertEqual(self.cli("readiness")[0], 0)

    def test_dirty_and_untracked_source_invalidate_evidence_without_commit(self):
        self.accepted()
        self.assertEqual(self.cli("readiness")[0], 0)
        self.source.write_text("print('after')\n", encoding="utf-8")
        self.assertIn("candidate", self.cli("readiness")[2])
        self.cli("snapshot")
        self.assertEqual(self.state()["requirements"][0]["status"], "unverified")
        self.assertNotEqual(self.check(candidate="HEAD")[0], 0)
        (self.root / "new.py").write_text("pass\n", encoding="utf-8")
        self.assertNotEqual(self.cli("readiness")[0], 0)

    def test_unit_only_never_satisfies_user_requirement(self):
        self.start()
        self.assertEqual(self.check("unit", "unit")[0], 0)
        self.assertNotEqual(self.cli("accept", "--requirement-id", "r1", "--check", "unit")[0], 0)
        self.assertNotEqual(self.cli("readiness")[0], 0)

    def test_tampered_evidence_is_not_reused(self):
        self.accepted()
        (self.root / ".hermes/delivery/s1/acceptance.log").write_text("tampered", encoding="utf-8")
        self.assertIn("evidence", self.cli("readiness")[2])

    def test_explicit_bugfix_requires_real_reproduction_evidence(self):
        self.accepted("--kind", "bugfix")
        self.assertIn("reproduction", self.cli("readiness")[2])
        log = self.root / ".hermes/delivery/s1/red.log"
        log.write_text("old defect reproduced: assertion failed", encoding="utf-8")
        self.assertEqual(self.cli("record-reproduction", "--status", "reproduced", "--command", "old scenario",
                                  "--evidence", str(log))[0], 0)
        self.assertEqual(self.cli("readiness")[0], 0)

    def test_two_disproved_diagnoses_require_reassessment(self):
        self.accepted()
        for text in ("hypothesis one", "hypothesis two"):
            self.assertEqual(self.cli("record-diagnosis", "--text", text, "--result", "disproved")[0], 0)
        self.assertIn("reassess", self.cli("readiness")[2])
        self.assertEqual(self.cli("reassess", "--note", "New evidence disproves shared assumption")[0], 0)
        self.assertEqual(self.cli("readiness")[0], 0)

    def test_production_target_cannot_be_ready_from_local_checks(self):
        self.accepted("--target", "production", "--cohort", "agreed users")
        self.assertIn("production", self.cli("readiness")[2])

    def test_independent_review_is_required_unless_explicitly_waived(self):
        self.accepted("--review-required")
        self.assertIn("review", self.cli("readiness")[2])

    def test_evidence_cannot_read_credentials_or_non_artifacts(self):
        self.start()
        for name in (".env", "credentials.json", "auth.json", "source.py"):
            path = self.root / name
            path.write_text("not an actual secret", encoding="utf-8")
            code, _, _ = self.cli("record-check", "--name", "bad", "--kind", "unit", "--status", "passed",
                                  "--candidate", self.state()["candidate"], "--environment", "local",
                                  "--exit-code", "0", "--command", "bad", "--evidence", str(path))
            self.assertNotEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
