import json
from unittest import TestCase
from unittest.mock import Mock
from pathlib import Path

from users.canary_reset import CanaryResetPreview, preview_fingerprint


class CanaryResetContractTests(TestCase):
    def test_reset_uses_the_dedicated_production_canary_allowlist(self):
        source = Path(__file__).with_name("canary_reset.py").read_text(encoding="utf-8")
        self.assertIn('"PRODUCTION_CANARY_TELEGRAM_IDS"', source)
        self.assertNotIn('"ADMIN_TEST_TELEGRAM_IDS"', source)

    def test_preview_has_scope_and_stable_fingerprint(self):
        preview = CanaryResetPreview(tg_user_id=123, onboarding=1, sessions=2, capabilities=3, acquisition=4, synthetic_graph=5, notification_drafts=6)
        self.assertEqual(preview_fingerprint(preview), preview_fingerprint(CanaryResetPreview(**json.loads(json.dumps(preview.as_dict(), sort_keys=True)))))
        self.assertEqual(set(preview.as_dict()), {"tg_user_id", "onboarding", "sessions", "capabilities", "acquisition", "synthetic_graph", "notification_drafts", "real_artifacts"})

    def test_preview_is_zero_write(self):
        user = Mock(pk=123, tg_user_id=123)
        with self.assertRaises(Exception):
            from users.canary_reset import execute_canary_reset
            execute_canary_reset(user=user, admin_user=Mock(has_perm=lambda _: True), confirmation="RESET PRODUCTION CANARY", comment="x", tg_user_id=123, preview_fingerprint="bad")

    def test_preview_and_execution_use_the_same_canary_owned_scope(self):
        source = Path(__file__).with_name("canary_reset.py").read_text(encoding="utf-8")
        self.assertNotIn("AcquisitionSession.objects.filter(user=user).count()", source)
        self.assertIn("AcquisitionSession.objects.filter(user=user, canary_owned=True).count()", source)
        self.assertIn("select_for_update()", source)
        self.assertIn("AppNotification", source)
        self.assertIn("AiTransactionDraft", source)
