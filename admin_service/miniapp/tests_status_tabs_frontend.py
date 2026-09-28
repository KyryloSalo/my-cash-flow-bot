from django.template.loader import get_template
from django.test import SimpleTestCase


class StatusTabsFrontendContractTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.source = get_template("miniapp/index.html").template.source

    def test_status_tabs_switch_between_separate_panels(self) -> None:
        self.assertIn('id="statusFinancePane" role="tabpanel"', self.source)
        self.assertIn('id="achievementsStatusPane" role="tabpanel"', self.source)
        self.assertIn("function setupStatusTabs()", self.source)
        self.assertIn("financePane.hidden = showAchievements", self.source)
        self.assertIn("achievementsPane.hidden = !showAchievements", self.source)

    def test_status_tabs_are_initialized_and_keyboard_accessible(self) -> None:
        boot_function = self.source.split("function boot()", 1)[1].split(
            "window.vydnoNavigate", 1
        )[0]
        self.assertIn("setupStatusTabs();", boot_function)
        self.assertIn('["ArrowLeft", "ArrowRight", "Home", "End"]', self.source)
        self.assertIn('setAttribute("aria-selected"', self.source)