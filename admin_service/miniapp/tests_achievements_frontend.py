from pathlib import Path

from django.conf import settings
from django.test import SimpleTestCase


class AchievementsFrontendContractTests(SimpleTestCase):
    @property
    def miniapp_dir(self):
        return Path(settings.BASE_DIR) / "miniapp"

    def test_template_exposes_widget_status_tab_settings_and_mascot_modal(self):
        template = (self.miniapp_dir / "templates" / "miniapp" / "index.html").read_text(encoding="utf-8")

        for element_id in (
            "achievementsOverviewWidget",
            "statusFinancePane",
            "achievementsStatusPane",
            "achievementsSettingsCard",
            "achievementsMascotModal",
        ):
            self.assertIn(f'id="{element_id}"', template)
        self.assertIn('id="statusAchievementsTab"', template)
        self.assertNotIn("achievements-demo-bar", template)

    def test_production_assets_exist_and_frontend_wires_real_api_flows(self):
        static_dir = self.miniapp_dir / "static" / "miniapp"
        css_path = static_dir / "achievements-ui.css"
        js_path = static_dir / "achievements.js"
        self.assertTrue(css_path.is_file())
        self.assertTrue(js_path.is_file())

        css = css_path.read_text(encoding="utf-8")
        javascript = js_path.read_text(encoding="utf-8")
        for marker in (
            "vydno:ready",
            "prefers-reduced-motion",
            "Idempotency-Key",
            "notificationClaim",
            "notificationAck",
            "pinsUrl",
        ):
            self.assertIn(marker, javascript)
        self.assertIn("min-height: 44px", css)
        self.assertIn(".gamification-ready", css)

        assets_dir = static_dir / "achievements"
        for relative_path in (
            "assets/bob/01-welcome.png",
            "assets/capi/01-welcome.png",
            "previews/bob-motion-poster.png",
            "previews/capi-motion-poster.png",
        ):
            self.assertTrue((assets_dir / relative_path).is_file(), relative_path)
