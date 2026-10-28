"""Dedicated hermetic staged-delivery acceptance/regression tests."""
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
spec = importlib.util.spec_from_file_location("delivery_stages_candidate", ROOT / "ops/delivery.py")
delivery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(delivery)


class StagedDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "app.py").write_text("print('fixture')\n", encoding="utf-8")
        for args in (("init", "-q"), ("add", "app.py"),
                     ("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
                      "commit", "-qm", "fixture")):
            subprocess.run(["git", "-C", str(self.root), *args], check=True, capture_output=True)

    def cli(self, cmd, *args):
        argv = [cmd, "--root", str(self.root), "--session-id", "stage1", *args]
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                code = delivery.main(argv)
            except SystemExit as exc:
                code = exc.code if isinstance(exc.code, int) else 2
                if isinstance(exc.code, str):
                    stderr.write(exc.code)
        return subprocess.CompletedProcess(argv, code, stdout.getvalue(), stderr.getvalue())

    def start(self, requirements=("integration-visible", "production-authoritative"), target="production", cohort="cohort-a"):
        args = ["start", "--request", "staged delivery", "--target", target, "--cohort", cohort,
                "--acceptance-command", "python smoke.py", "--no-review-required", "--review-waiver", "fixture"]
        for req in requirements:
            args += ["--requirement", req]
        result = self.cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)

    def state(self):
        return json.loads((self.root / ".hermes/delivery/stage1/task.json").read_text(encoding="utf-8"))

    def configure(self, integration=("r1",), production=("r2",), cohort="cohort-a"):
        args = ["--approval", "human authorized staged plan, NOT production approval", "--cohort", cohort]
        for requirement in integration:
            args += ["--integration-requirement", requirement]
        for requirement in production:
            args += ["--production-requirement", requirement]
        return self.cli("configure-gates", *args)

    def write_state(self, data):
        (self.root / ".hermes/delivery/stage1/task.json").write_text(json.dumps(data), encoding="utf-8")

    def integration_ready(self, target="local", cohort=""):
        self.start(target=target, cohort=cohort)
        self.assertEqual(self.configure().returncode, 0)
        self.assertEqual(self.cli("preflight").returncode, 0)
        self.assertEqual(self.check("unit", "unit").returncode, 0)
        self.assertEqual(self.check("integration", "acceptance", command="python smoke.py").returncode, 0)
        self.assertEqual(self.accept("r1", "integration").returncode, 0)
        ready = self.cli("readiness", "--stage", "integration")
        self.assertEqual(ready.returncode, 0, ready.stderr)

    def production_ready(self):
        self.integration_ready()
        self.assertEqual(self.check("production", "acceptance", "production", "python smoke.py").returncode, 0)
        self.assertEqual(self.accept("r2", "production").returncode, 0)
        for kind in ("runtime", "activation"):
            self.assertEqual(self.check(kind, kind, "production").returncode, 0)
        self.assertEqual(self.cli("record-release", "--active-revision", self.state()["candidate"],
                                  "--cohort", "cohort-a", "--evidence", str(self.evidence("release")),
                                  "--behavior-enabled", "--authoritative").returncode, 0)
        complete = self.cli("readiness")
        self.assertEqual(complete.returncode, 0, complete.stderr)

    def evidence(self, name, text="passed"):
        path = self.root / ".hermes/delivery/stage1" / (name + ".log")
        path.write_text(text + "\n", encoding="utf-8")
        return path

    def check(self, name, kind, environment="local", command="fixture", candidate=None):
        candidate = candidate or self.state()["candidate"]
        return self.cli("record-check", "--name", name, "--kind", kind, "--status", "passed",
                        "--candidate", candidate, "--environment", environment, "--exit-code", "0",
                        "--command", command, "--evidence", str(self.evidence(name)))

    def accept(self, requirement, check):
        return self.cli("accept", "--requirement-id", requirement, "--check", check)

    def test_candidate_supports_opt_in_ten_requirement_staged_integration(self):
        self.start(requirements=tuple("observable outcome " + str(i) for i in range(1, 11)))
        result = self.configure(integration=tuple("r" + str(i) for i in range(1, 10)), production=("r10",))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.cli("preflight").returncode, 0)
        self.assertEqual(self.check("unit", "unit").returncode, 0)
        self.assertEqual(self.check("integration", "acceptance", command="python smoke.py").returncode, 0)
        for i in range(1, 10):
            self.assertEqual(self.accept("r" + str(i), "integration").returncode, 0)
        ready = self.cli("readiness", "--stage", "integration")
        self.assertEqual(ready.returncode, 0, ready.stderr)
        self.assertIn("READY FOR INTEGRATION", ready.stdout)
        self.assertNotIn("COMPLETE", ready.stdout)
        self.assertEqual(self.state()["requirements"][9]["status"], "unverified")
        self.assertEqual(self.state()["requirements"][9]["evidence_ids"], [])
        self.assertNotEqual(self.cli("readiness").returncode, 0)

    def test_default_completion_is_blocked_until_production_acceptance_runtime_activation_and_release(self):
        self.start()
        self.assertEqual(self.configure().returncode, 0)
        self.cli("preflight")
        self.check("unit", "unit")
        self.check("integration", "acceptance", command="python smoke.py")
        self.accept("r1", "integration")
        self.assertNotEqual(self.cli("readiness").returncode, 0)
        self.assertEqual(self.check("production_acceptance", "acceptance", "production", command="python smoke.py").returncode, 0)
        self.assertEqual(self.accept("r2", "production_acceptance").returncode, 0)
        self.assertEqual(self.check("runtime", "runtime", "production").returncode, 0)
        self.assertEqual(self.check("activation", "activation", "production").returncode, 0)
        release = self.cli("record-release", "--active-revision", self.state()["candidate"],
                           "--cohort", "cohort-a", "--evidence", str(self.evidence("release")),
                           "--behavior-enabled", "--authoritative")
        self.assertEqual(release.returncode, 0, release.stderr)
        complete = self.cli("readiness")
        self.assertEqual(complete.returncode, 0, complete.stderr)
        self.assertIn("COMPLETE", complete.stdout)

    def test_staged_completion_uses_configured_cohort_when_original_target_is_local(self):
        self.start(target="local", cohort="")
        self.assertEqual(self.configure(cohort="configured-cohort").returncode, 0)
        self.cli("preflight")
        self.check("unit", "unit")
        self.check("integration", "acceptance", command="python smoke.py")
        self.accept("r1", "integration")
        self.check("production", "acceptance", "production", command="python smoke.py")
        self.accept("r2", "production")
        self.check("runtime", "runtime", "production")
        self.check("activation", "activation", "production")
        self.assertEqual(self.cli("record-release", "--active-revision", self.state()["candidate"],
                                  "--cohort", "configured-cohort", "--evidence", str(self.evidence("release")),
                                  "--behavior-enabled", "--authoritative").returncode, 0)
        result = self.cli("readiness")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("COMPLETE — PRODUCTION", result.stdout)

    def test_staged_completion_rejects_release_cohort_different_from_configured_cohort(self):
        self.start(target="local", cohort="")
        self.assertEqual(self.configure(cohort="configured-cohort").returncode, 0)
        self.cli("preflight")
        for name, kind, env, command in (("unit", "unit", "local", "fixture"),
                                         ("integration", "acceptance", "local", "python smoke.py"),
                                         ("production", "acceptance", "production", "python smoke.py"),
                                         ("runtime", "runtime", "production", "fixture"),
                                         ("activation", "activation", "production", "fixture")):
            self.check(name, kind, env, command)
        self.accept("r1", "integration")
        self.accept("r2", "production")
        self.assertEqual(self.cli("record-release", "--active-revision", self.state()["candidate"],
                                  "--cohort", "wrong-cohort", "--evidence", str(self.evidence("release")),
                                  "--behavior-enabled", "--authoritative").returncode, 0)
        self.assertNotEqual(self.cli("readiness").returncode, 0)

    def test_malformed_gate_dictionary_fails_closed_without_harness_crash(self):
        self.integration_ready()
        state = self.state()
        malformed = [None, {}, [], "legacy", {"integration": None, "production": ["r2"]}]
        for key, values in (("integration", ([], "r1", ["r1", "r1"], [None], [["r1"]])),
                            ("production", ([], "r2", ["r2", "r2"], ["r1"], ["r3"])),
                            ("approval", ("", None, 42)), ("cohort", ("", None, []))):
            for value in values:
                gates = dict(state["gates"])
                gates[key] = value
                malformed.append(gates)
        for gates in malformed:
            with self.subTest(gates=gates):
                self.write_state(dict(state, gates=gates))
                result = self.cli("readiness", "--stage", "integration")
                self.assertNotEqual(result.returncode, 0, gates)
                self.assertIn("staged gate", result.stderr)

    def test_production_checks_for_other_sha_or_wrong_cohort_fail(self):
        self.start()
        self.assertEqual(self.configure().returncode, 0)
        self.cli("preflight")
        for name, kind in (("unit", "unit"), ("integration", "acceptance"), ("production", "acceptance"),
                           ("runtime", "runtime"), ("activation", "activation")):
            self.check(name, kind, "production" if name != "unit" else "local")
        self.assertNotEqual(self.cli("record-release", "--active-revision", "0" * 40, "--cohort", "cohort-a",
                                     "--evidence", str(self.evidence("release")), "--behavior-enabled", "--authoritative").returncode, 0)
        self.assertNotEqual(self.cli("record-release", "--active-revision", "1" * 40, "--cohort", "other",
                                     "--evidence", str(self.evidence("release2")), "--behavior-enabled", "--authoritative").returncode, 0)

    def test_gate_partitions_fail_closed_for_empty_duplicate_overlap_unknown_and_uncovered(self):
        self.start()
        cases = [(("r1",), ("r1",)), (("r1",), ("r3",)), (("r1",), ()), ((), ("r2",)),
                 (("r1", "r1"), ("r2",)), (("r1",), ("r2", "r2"))]
        for integration, production in cases:
            result = self.configure(integration, production)
            self.assertNotEqual(result.returncode, 0, (integration, production, result.stderr))

    def test_adding_requirement_invalidates_existing_gate_configuration(self):
        self.integration_ready()
        self.assertEqual(self.cli("requirement", "new requirement").returncode, 0)
        result = self.cli("readiness", "--stage", "integration")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("partition", result.stderr)
        self.assertEqual(self.configure(integration=("r1", "r3")).returncode, 0)
        self.assertNotEqual(self.cli("readiness", "--stage", "integration").returncode, 0)
        self.assertEqual(self.accept("r3", "integration").returncode, 0)
        self.assertEqual(self.cli("readiness", "--stage", "integration").returncode, 0)

    def test_repeated_accept_preserves_local_acceptance_links(self):
        self.integration_ready()
        self.assertEqual(self.check("second", "acceptance", "production").returncode, 0)
        self.assertEqual(self.accept("r1", "second").returncode, 0)
        self.assertEqual(self.accept("r1", "integration").returncode, 0)
        self.assertEqual(self.state()["requirements"][0]["evidence_ids"], ["integration", "second"])
        self.assertEqual(self.cli("readiness", "--stage", "integration").returncode, 0)

    def test_integration_stage_requires_opt_in_even_for_accepted_legacy_task(self):
        self.integration_ready()
        data = self.state()
        del data["gates"]
        data["requirements"] = data["requirements"][:1]
        self.write_state(data)
        self.assertEqual(self.cli("readiness").returncode, 0)
        result = self.cli("readiness", "--stage", "integration")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("explicitly configured", result.stderr)

    def test_staged_integration_enforces_preflight_named_route_review_and_unit_checks(self):
        self.integration_ready()
        state = self.state()
        for key, expected in (("preflight", "preflight"), ("unit", "checks"), ("integration", "route")):
            with self.subTest(missing=key):
                data = json.loads(json.dumps(state))
                if key == "preflight":
                    del data[key]
                else:
                    data["checks"] = [check for check in data["checks"] if check["name"] != key]
                self.write_state(data)
                result = self.cli("readiness", "--stage", "integration")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
        state["review_required"] = True
        self.write_state(state)
        self.assertIn("review", self.cli("readiness", "--stage", "integration").stderr)
        self.assertEqual(self.cli("record-check", "--name", "review", "--kind", "review", "--status", "passed",
                                  "--candidate", state["candidate"], "--environment", "local", "--exit-code", "0",
                                  "--command", "independent fixture review", "--reviewer", "fixture-reviewer",
                                  "--independent", "--evidence", str(self.evidence("review"))).returncode, 0)
        self.assertEqual(self.cli("readiness", "--stage", "integration").returncode, 0)

    def test_staged_integration_enforces_evidence_hash_and_candidate_binding(self):
        self.integration_ready()
        log = self.root / ".hermes/delivery/stage1/integration.log"
        log.write_text("tampered", encoding="utf-8")
        self.assertIn("evidence", self.cli("readiness", "--stage", "integration").stderr)
        self.check("integration", "acceptance", command="python smoke.py")
        self.assertEqual(self.cli("readiness", "--stage", "integration").returncode, 0)
        (self.root / "app.py").write_text("print('changed')\n", encoding="utf-8")
        self.assertIn("candidate", self.cli("readiness", "--stage", "integration").stderr)
        self.assertNotEqual(self.check("stale", "runtime", "production").returncode, 0)
        self.cli("snapshot")
        self.assertIn("preflight", self.cli("readiness", "--stage", "integration").stderr)

    def test_staged_integration_keeps_bugfix_reproduction_and_reassessment_gates(self):
        self.integration_ready()
        state = self.state()
        state["kind"] = "bugfix"
        self.write_state(state)
        self.assertIn("reproduction", self.cli("readiness", "--stage", "integration").stderr)
        self.assertEqual(self.cli("record-reproduction", "--status", "reproduced", "--command", "synthetic failing fixture",
                                  "--evidence", str(self.evidence("red", "synthetic defect reproduced"))).returncode, 0)
        self.assertEqual(self.cli("readiness", "--stage", "integration").returncode, 0)
        for text in ("disproved one", "disproved two"):
            self.cli("record-diagnosis", "--text", text, "--result", "disproved")
        self.assertIn("reassess", self.cli("readiness", "--stage", "integration").stderr)

    def test_completion_requires_all_production_readback_guards(self):
        self.production_ready()
        state = self.state()
        for field, invalid in (("cohort", "wrong"), ("active_revision", "0" * 40),
                               ("behavior_enabled", False), ("result_authoritative", False)):
            with self.subTest(field=field):
                data = json.loads(json.dumps(state))
                data["deployment"][field] = invalid
                self.write_state(data)
                self.assertIn("readback", self.cli("readiness").stderr)
        for kind in ("runtime", "activation", "production", "integration"):
            with self.subTest(missing=kind):
                data = json.loads(json.dumps(state))
                data["checks"] = [check for check in data["checks"] if check["name"] != kind]
                self.write_state(data)
                self.assertNotEqual(self.cli("readiness").returncode, 0)
        self.write_state(state)
        (self.root / ".hermes/delivery/stage1/release.log").write_text("tampered", encoding="utf-8")
        self.assertIn("evidence", self.cli("readiness").stderr)

    def test_approval_text_and_local_acceptance_never_substitute_for_production(self):
        self.integration_ready()
        self.assertEqual(self.accept("r2", "integration").returncode, 0)
        state = self.state()
        self.assertTrue(state["gates"]["approval"])
        self.assertNotIn("deployment", state)
        self.assertNotEqual(self.cli("readiness").returncode, 0)

    def test_failed_latest_check_attempt_cannot_reuse_earlier_success(self):
        self.integration_ready()
        state = self.state()
        latest = dict(state["checks"][-1], status="failed", exit_code=1)
        state["checks"].append(latest)
        self.write_state(state)
        self.assertNotEqual(self.cli("readiness", "--stage", "integration").returncode, 0)


if __name__ == "__main__":
    unittest.main()
