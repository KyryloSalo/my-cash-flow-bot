from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


BOT_DIR = Path(__file__).resolve().parents[1]
if str(BOT_DIR) not in sys.path:
    sys.path.insert(0, str(BOT_DIR))


class ConfigTests(unittest.TestCase):
    def _load_config(self):
        sys.modules.pop("config", None)
        return importlib.import_module("config")

    def test_family_access_enabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            config = self._load_config()
            self.assertTrue(config.FAMILY_ACCESS_ENABLED)

    def test_family_access_can_be_disabled_explicitly(self):
        with patch.dict(os.environ, {"FAMILY_ACCESS_ENABLED": "false"}, clear=True):
            config = self._load_config()
            self.assertFalse(config.FAMILY_ACCESS_ENABLED)

    def test_support_contact_url_defaults_to_none(self):
        with patch.dict(os.environ, {}, clear=True):
            config = self._load_config()
            self.assertIsNone(config.SUPPORT_CONTACT_URL)

    def test_support_contact_url_reads_from_env(self):
        with patch.dict(os.environ, {"SUPPORT_CONTACT_URL": "https://t.me/Askills_Support"}, clear=True):
            config = self._load_config()
            self.assertEqual(config.SUPPORT_CONTACT_URL, "https://t.me/Askills_Support")


if __name__ == "__main__":
    unittest.main()
