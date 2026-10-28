from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


class VisionModelDefaultsTests(unittest.TestCase):
    def _vision_models(self, model: str = "", fallback: str = "") -> list[str]:
        env = dict(os.environ, OPENAI_VISION_MODEL=model, OPENAI_VISION_FALLBACK_MODEL=fallback)
        result = subprocess.run(
            [sys.executable, "-c", "import config,json;print(json.dumps([config.OPENAI_VISION_MODEL,config.OPENAI_VISION_FALLBACK_MODEL]))"],
            cwd=Path(__file__).resolve().parents[1], env=env,
            capture_output=True, text=True, check=True,
        )
        return json.loads(result.stdout.strip())

    def test_default_vision_uses_extraction_quality_candidate(self) -> None:
        self.assertEqual(self._vision_models(), ["gpt-5.4-mini", "gpt-5.4-mini"])

    def test_explicit_model_and_fallback_remain_operator_controlled(self) -> None:
        self.assertEqual(self._vision_models("configured-primary", "configured-fallback"),
                         ["configured-primary", "configured-fallback"])


if __name__ == "__main__":
    unittest.main()
