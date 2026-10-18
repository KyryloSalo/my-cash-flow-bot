"""Exercise the checked-in launcher from a normal outer workspace, not a mock CLI."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2]


class DeliveryEntrypointTests(unittest.TestCase):
    def test_new_task_from_normal_workspace_preserves_contract_and_rejects_stale_evidence(self):
        launcher = SOURCE / "delivery.py"
        self.assertTrue(launcher.is_file(), "normal-workspace delivery entrypoint is missing")
        with tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR")) as temporary:
            workspace = Path(temporary)
            root = workspace / "code"
            (root / "ops").mkdir(parents=True)
            (root / "ops/delivery.py").write_bytes((SOURCE / "ops/delivery.py").read_bytes())
            (workspace / "delivery.py").write_bytes(launcher.read_bytes())
            (root / "app.py").write_text("print('before')\n", encoding="utf-8")
            for args in (("init", "-q"), ("add", "app.py"),
                         ("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")):
                subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)

            def cli(command, *arguments, expected=0):
                result = subprocess.run(
                    [sys.executable, str(workspace / "delivery.py"), command, "--session-id", "entrypoint_acceptance", *arguments],
                    cwd=workspace, capture_output=True, text=True, encoding="utf-8",
                )
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                return result.stdout

            request = "Keep the actual request and show a verified result; never deploy."
            cli("start", "--request", request, "--requirement", "Original request and clarification survive",
                "--acceptance-command", "actual contract readback", "--no-review-required",
                "--review-waiver", "isolated CLI fixture, no application or release changes")
            cli("clarification", "The exact visible duration is 51 days, not a default.")
            cli("readiness", expected=2)
            state = json.loads(cli("status"))
            self.assertEqual(state["request"], request)
            self.assertEqual(state["clarifications"], ["The exact visible duration is 51 days, not a default."])
            cli("preflight")
            session = root / ".hermes/delivery/entrypoint_acceptance"
            log = session / "actual-readback.log"
            log.write_text(json.dumps({"actual_request": state["request"], "actual_clarifications": state["clarifications"]}), encoding="utf-8")

            def record_and_accept():
                candidate = cli("candidate").strip()
                for kind in ("unit", "acceptance"):
                    cli("record-check", "--name", kind, "--kind", kind, "--environment", "local",
                        "--status", "passed", "--exit-code", "0", "--candidate", candidate,
                        "--command", "actual contract readback", "--evidence", str(log))
                cli("accept", "--requirement-id", "r1", "--check", "acceptance")

            record_and_accept()
            self.assertIn("COMPLETE", cli("readiness"))
            (root / "app.py").write_text("print('after')\n", encoding="utf-8")
            cli("readiness", expected=2)
            cli("snapshot")
            record_and_accept()
            cli("readiness", expected=2)  # Preflight belongs to the old source.
            cli("preflight")
            self.assertIn("COMPLETE", cli("readiness"))


if __name__ == "__main__":
    unittest.main()
