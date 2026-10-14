from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from unittest import TestCase


class ProductionCanaryIdentityConfigTests(TestCase):
    def test_default_allowlist_uses_reserved_synthetic_telegram_id(self):
        env = os.environ.copy()
        env.pop("PRODUCTION_CANARY_TELEGRAM_IDS", None)
        env["DJANGO_DEBUG"] = "true"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from config import settings; print(settings.PRODUCTION_CANARY_TELEGRAM_IDS)",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.stdout.strip(), "[777777]")
