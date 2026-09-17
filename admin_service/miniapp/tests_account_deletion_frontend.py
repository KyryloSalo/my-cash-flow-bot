from __future__ import annotations

from pathlib import Path

from django.template.loader import get_template
from django.test import SimpleTestCase


class AccountDeletionFrontendContractTests(SimpleTestCase):
    def _landing_source_path(self, filename: str) -> Path:
        landing_root = Path(__file__).resolve().parents[2] / "nginx" / "landing"
        if not landing_root.is_dir():
            self.skipTest(
                "nginx landing sources are validated before build and are not packaged in the admin image"
            )
        return landing_root / filename

    def test_settings_exposes_accessible_destructive_confirmation_dialog(self) -> None:
        source = get_template("miniapp/index.html").template.source

        self.assertIn('data-account-deletion-preflight-url="{% url \'miniapp:account-deletion-preflight\' %}"', source)
        self.assertIn('data-account-deletion-confirm-url="{% url \'miniapp:account-deletion-confirm\' %}"', source)
        self.assertIn('id="settingsDeleteAccountBtn"', source)
        self.assertIn('aria-controls="accountDeletionModal"', source)
        self.assertIn('aria-haspopup="dialog"', source)
        self.assertIn(
            'id="accountDeletionModal" role="dialog" aria-modal="true" '
            'aria-labelledby="accountDeletionTitle" aria-describedby="accountDeletionDescription" hidden',
            source,
        )
        self.assertIn('id="accountDeletionConfirmation" type="text"', source)
        self.assertIn('id="accountDeletionAcknowledged" type="checkbox"', source)
        self.assertIn('id="accountDeletionConfirmBtn" type="button" disabled', source)
        self.assertIn('id="accountDeletionStatus" role="alert" aria-live="assertive"', source)

    def test_dialog_uses_server_challenge_and_owner_session_only(self) -> None:
        source = get_template("miniapp/index.html").template.source

        self.assertIn("const accountDeletionPreflightUrl = root.dataset.accountDeletionPreflightUrl;", source)
        self.assertIn("const accountDeletionConfirmUrl = root.dataset.accountDeletionConfirmUrl;", source)
        self.assertIn("function openAccountDeletionDialog()", source)
        self.assertIn("getJson(accountDeletionPreflightUrl, false)", source)
        self.assertIn("function updateAccountDeletionConfirmation()", source)
        self.assertIn("el.confirm.disabled = !ready", source)
        self.assertIn("openAccessibleModal(el.modal, closeAccountDeletionDialog", source)
        self.assertIn("postJson(accountDeletionConfirmUrl, {", source)
        self.assertIn("challenge: accountDeletionState.challenge", source)
        self.assertIn("confirmation_text: el.confirmation.value", source)
        self.assertIn("acknowledged: el.acknowledged.checked", source)
        confirm_function = source.split("function confirmAccountDeletion()", 1)[1].split(
            "function setupAccountDeletion", 1
        )[0]
        self.assertNotIn("user_id", confirm_function)

    def test_dialog_is_initialized_and_localized_with_settings(self) -> None:
        source = get_template("miniapp/index.html").template.source

        boot_function = source.split("function boot()", 1)[1].split(
            "window.vydnoNavigate", 1
        )[0]
        self.assertIn("setupAccountDeletion();", boot_function)
        render_settings = source.split("function renderSettings(payload)", 1)[1].split(
            "function loadSettingsOptions", 1
        )[0]
        self.assertIn("renderAccountDeletionCopy();", render_settings)

    def test_dialog_is_mobile_scroll_safe_and_visually_separated(self) -> None:
        css = (Path(__file__).parent / "static" / "miniapp" / "app.css").read_text(encoding="utf-8")

        self.assertIn(".account-danger-card", css)
        self.assertIn(".account-deletion-modal", css)
        self.assertIn(".account-deletion-card", css)
        self.assertIn("max-height: calc(100dvh - var(--safe-top) - var(--safe-bottom) - 28px);", css)
        self.assertIn("overflow-y: auto;", css)
        self.assertIn("overscroll-behavior: contain;", css)
        self.assertIn(".account-deletion-consequences", css)
        privacy_link_rule = css.split(".settings-privacy-link {", 1)[1].split("}", 1)[0]
        self.assertIn("display: inline-flex;", privacy_link_rule)
        self.assertIn("min-height: 44px;", privacy_link_rule)

    def test_plain_app_route_does_not_override_settings_navigation_after_ready(self) -> None:
        push_source = (Path(__file__).parent / "static" / "miniapp" / "push.js").read_text(
            encoding="utf-8"
        )
        deep_link_handler = push_source.split("function handleDeepLink()", 1)[1].split(
            'document.addEventListener("DOMContentLoaded"', 1
        )[0]

        self.assertIn('const routeKeys = ["screen", "tab", "section", "notification_id"]', deep_link_handler)
        self.assertIn("routeKeys.some", deep_link_handler)
        self.assertIn("if (!hasDeepLink) return Promise.resolve(false);", deep_link_handler)

    def test_deletion_deep_link_remains_available_before_onboarding_completion(self) -> None:
        source = get_template("miniapp/index.html").template.source
        boot_function = source.split("function boot()", 1)[1].split(
            "window.vydnoNavigate", 1
        )[0]

        self.assertIn("function accountDeletionResumeRequested()", source)
        self.assertIn("if (setupRequired && accountDeletionResumeRequested())", boot_function)
        self.assertIn("showDashboard();", boot_function)
        self.assertIn("signalAppReady();", boot_function)

    def test_public_account_deletion_page_explains_and_links_in_app_flow(self) -> None:
        page_path = self._landing_source_path("delete-account.html")
        self.assertTrue(page_path.exists(), "public account-deletion page is missing")
        page = page_path.read_text(encoding="utf-8")
        self.assertIn(
            '<link rel="canonical" href="https://vydno.capital/delete-account.html">',
            page,
        )
        self.assertIn(
            'href="https://vydno.capital/app/?screen=settings&amp;account-deletion=1"',
            page,
        )
        self.assertIn("Видалити акаунт Vydno", page)
        self.assertIn("Що буде видалено", page)
        self.assertIn("Що може зберігатися поза Vydno", page)
        self.assertIn("https://t.me/Askills_Support", page)

    def test_privacy_policy_links_to_self_service_deletion_page(self) -> None:
        privacy_path = self._landing_source_path("privacy.html")
        privacy = privacy_path.read_text(encoding="utf-8")

        self.assertIn('href="delete-account.html"', privacy)
        self.assertIn("самостійно видалити акаунт", privacy)
