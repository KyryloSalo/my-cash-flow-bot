from pathlib import Path

from django.test import SimpleTestCase


ROOT = Path(__file__).resolve().parent
ADMIN_ROOT = ROOT.parent


class CanarySecurityContractTests(SimpleTestCase):
    def test_every_synthetic_finalizer_payload_marks_canary_ownership(self):
        source = (ROOT / "canary.py").read_text(encoding="utf-8")

        self.assertNotIn('"synthetic": True}', source)
        self.assertGreaterEqual(source.count('"canary_owned": True'), 3)
        self.assertGreaterEqual(source.count('"result":'), 3)

    def test_browser_login_serializes_canary_state_and_token_consumption(self):
        source = (ADMIN_ROOT / "miniapp" / "views.py").read_text(encoding="utf-8")

        self.assertIn("UserAdminState.objects.select_for_update()", source)
        self.assertIn("with db_transaction.atomic():", source)

    def test_token_consumption_has_an_atomic_helper_boundary(self):
        source = (ADMIN_ROOT / "miniapp" / "auth.py").read_text(encoding="utf-8")

        self.assertIn("with transaction.atomic():", source)

    def test_reset_locks_auth_sessions_before_revocation(self):
        source = (ADMIN_ROOT / "users" / "canary_reset.py").read_text(encoding="utf-8")

        self.assertIn("UserAuthSession.objects.select_for_update()", source)
