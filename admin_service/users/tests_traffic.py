from __future__ import annotations

from django.test import SimpleTestCase, override_settings

from users.traffic import funnel_excluded_telegram_ids


class FunnelTrafficExclusionTests(SimpleTestCase):
    @override_settings(
        ADMIN_TEST_TELEGRAM_IDS=[7001],
        MINIAPP_OPERATOR_TELEGRAM_IDS=[7002],
        ADMIN_TELEGRAM_IDS=[7003, -10012345],
        FUNNEL_EXCLUDED_TELEGRAM_IDS=[7004],
    )
    def test_only_individual_internal_ids_are_excluded(self) -> None:
        self.assertEqual(funnel_excluded_telegram_ids(), {7001, 7002, 7003, 7004})
