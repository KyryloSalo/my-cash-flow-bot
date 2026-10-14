from pathlib import Path

from django.test import SimpleTestCase


class ProductionCanaryAdminSlice11ContractTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.source = Path(__file__).with_name("admin.py").read_text(encoding="utf-8")

    def test_dedicated_group_exposes_only_per_user_canary_actions_and_status(self):
        for needle in (
            '"Production Canary"',
            "feature_enabled",
            "allowlisted",
            "test_user",
            "canary_eligible",
            "active_eligibility",
            "production_canary_bind",
            "production_canary_delayed",
            "production_canary_retry",
            "production_canary_reset",
        ):
            self.assertIn(needle, self.source)
        self.assertNotIn("delete_queryset", self.source[self.source.find("canary_actions"):self.source.find("canary_actions") + 5000])

    def test_high_risk_canary_endpoints_are_post_gated_and_generic(self):
        for needle in (
            "request.method == \"POST\"",
            "ConfirmActionForm",
            "create_canary_audit",
            "except Exception",
            "complete_delayed_canary",
            "init_canary_bind",
        ):
            self.assertIn(needle, self.source)

    def test_urls_include_all_slice11_actions(self):
        for needle in (
            "production-canary-bind",
            "production-canary-delayed",
            "production-canary-retry",
            "production-canary-reset",
        ):
            self.assertIn(needle, self.source)
