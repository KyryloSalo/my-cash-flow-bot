from pathlib import Path

from django.test import SimpleTestCase


class CanarySlice12BoundaryContractTests(SimpleTestCase):
    def test_canary_boundaries_have_security_contract_helpers(self):
        source = Path(__file__).with_name("views.py").read_text(encoding="utf-8")
        for needle in (
            "canary_boundary",
            "PRODUCTION_CANARY_ENABLED",
            "Cache-Control",
            "Referrer-Policy",
            "rate_limit",
        ):
            self.assertIn(needle, source)

    def test_service_worker_bypasses_canary_and_post(self):
        source = Path(__file__).with_name("views.py").read_text(encoding="utf-8")
        self.assertIn("/app/canary/", source)
        self.assertIn("event.request.method !== \"GET\"", source)
        self.assertIn("never cache post", source.lower())

    def test_canary_routes_use_uuid_or_capability_shape_and_no_public_event_injection(self):
        urls = Path(__file__).with_name("urls.py").read_text(encoding="utf-8")
        funnel = Path(__file__).with_name("funnel_views.py").read_text(encoding="utf-8")
        self.assertIn('path("canary/checkout/"', urls)
        self.assertNotIn("canary/checkout/<str:token>", urls)
        self.assertIn("ingest_client_event", funnel)
