import unittest
from pathlib import Path

import yaml


CODE = Path(__file__).resolve().parents[2]


class PhotoReleaseActivationTests(unittest.TestCase):
    def test_photo_entrypoints_select_active_pipeline_without_shadow(self):
        compose = yaml.safe_load((CODE / "compose.yml").read_text(encoding="utf-8"))
        for name in ("bot", "api"):
            with self.subTest(service=name):
                environment = compose["services"][name]["environment"]
                self.assertEqual(environment.get("PHOTO_PIPELINE_ENABLED"), "true")
                self.assertEqual(environment.get("PHOTO_PIPELINE_SHADOW_ENABLED"), "false")


if __name__ == "__main__":
    unittest.main()
