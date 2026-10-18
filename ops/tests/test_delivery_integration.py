"""Guard the actual task entrypoint and required CI wiring, not just helper imports."""
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DeliveryIntegrationTests(unittest.TestCase):
    def test_required_validate_runs_browser_before_packaging_without_production_changes(self):
        workflow = (ROOT / ".github/workflows/deploy.yml").read_text(encoding="utf-8")
        self.assertIn("name: Real isolated PWA acceptance", workflow)
        self.assertIn("ops/browser_acceptance_smoke.py --server", workflow)
        self.assertLess(workflow.index("name: Real isolated PWA acceptance"), workflow.index("name: Archive only after all validation gates succeed"))
        self.assertIn("playwright==1.55.0", workflow)
        self.assertIn("python -m unittest discover -s ops/tests -v", workflow)
        self.assertIn("python delivery.py start", (ROOT / "AGENTS.md").read_text(encoding="utf-8"))
        self.assertIn(".hermes/delivery/", (ROOT / ".gitignore").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
