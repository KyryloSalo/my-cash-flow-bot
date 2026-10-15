from __future__ import annotations

from pathlib import Path

from django.template.loader import get_template
from django.test import SimpleTestCase


class InstallCoachFrontendContractTests(SimpleTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base = Path(__file__).parent
        cls.source = get_template("miniapp/index.html").template.source
        cls.css = (cls.base / "static" / "miniapp" / "app.css").read_text(encoding="utf-8")
        cls.module_path = cls.base / "static" / "miniapp" / "install-coach.js"
        cls.module = cls.module_path.read_text(encoding="utf-8")
        cls.views_source = (cls.base / "views.py").read_text(encoding="utf-8")

    def test_install_coach_module_participates_in_asset_cache_busting(self) -> None:
        self.assertIn('_miniapp_asset_stamp("install-coach.js")', self.views_source)
        self.assertIn('install_coach_url = _versioned_static("miniapp/install-coach.js")', self.views_source)
        self.assertIn('"{install_coach_url}",', self.views_source)

    def test_install_coach_has_platform_storyboard_and_nonblocking_actions(self) -> None:
        self.assertIn('id="installCoachVisual"', self.source)
        self.assertIn('data-install-scene="ios"', self.source)
        self.assertIn('data-install-scene="android"', self.source)
        self.assertIn('data-install-scene="handoff-ios"', self.source)
        self.assertIn('data-install-scene="handoff-android"', self.source)
        self.assertIn('data-install-scene="open-safari"', self.source)
        self.assertIn('class="install-coach-handoff-action" data-install-highlight="0"><span>Відкрити у браузері</span></div>', self.source)
        ios_handoff = self.source.split('data-install-scene="handoff-ios"', 1)[1].split(
            'data-install-scene="handoff-android"', 1
        )[0]
        android_handoff = self.source.split('data-install-scene="handoff-android"', 1)[1].split(
            'data-install-scene="open-safari"', 1
        )[0]
        self.assertIn("Відкрити у браузері", ios_handoff)
        self.assertNotIn("Відкрити у Safari", ios_handoff)
        self.assertNotIn("<span>⋮</span>", ios_handoff)
        self.assertNotIn("install-coach-handoff-menu", ios_handoff)
        self.assertIn("Відкрити у браузері", android_handoff)
        self.assertNotIn("<span>⋮</span>", android_handoff)
        self.assertNotIn("install-coach-handoff-menu", android_handoff)
        self.assertIn(
            '.install-coach-visual[data-install-platform="ios"][data-install-mode="handoff"] .install-coach-scene--handoff-ios',
            self.css,
        )
        self.assertIn(
            '.install-coach-visual[data-install-platform="android"][data-install-mode="handoff"] .install-coach-scene--handoff-android',
            self.css,
        )
        self.assertIn('id="installCoachProgress"', self.source)
        self.assertIn('id="installCoachCommand"', self.source)
        self.assertIn('id="installCoachHint"', self.source)
        self.assertIn('class="install-coach-arrow"', self.source)
        self.assertIn('data-install-stage="add-confirmation"', self.source)
        self.assertIn('data-install-stage="browser-menu"', self.source)
        self.assertIn('id="installCoachHelp"', self.source)
        self.assertIn('id="installNudgeHelp"', self.source)
        self.assertIn('id="installNudgeLater"', self.source)
        self.assertIn('id="installNudgeLater" type="button">Не зараз', self.source)
        self.assertIn('aria-live="polite"', self.source)

    def test_initial_ios_animation_and_android_single_cta_contract(self) -> None:
        self.assertIn('const overview = platform !== "ios" && mode === "manual"', self.module)
        self.assertIn("overview: overview", self.module)
        self.assertIn('modal.dataset.installOverview = model.overview ? "true" : "false"', self.module)
        self.assertIn('#installNudgeModal[data-install-overview="true"] .install-coach-progress-row', self.css)
        self.assertNotIn('#installNudgeModal[data-install-overview="true"] .install-coach-visual,', self.css)
        self.assertIn('modal.dataset.installStatus = model.nativeStatus', self.module)
        for selector in (".install-coach-visual", ".install-coach-command", ".install-coach-progress-row", ".install-coach-steps"):
            self.assertIn('[data-install-status="idle"] ' + selector, self.css)
        self.assertIn('"Open the new Vydno icon on the Home Screen."', self.module)
        self.assertIn('"На головному екрані відкрийте нову іконку Vydno."', self.module)
        self.assertIn('#installNudgeModal[data-install-overview="true"] .install-coach-steps li', self.css)

        handler = self.source.split("function advanceInstallCoach()", 1)[1].split(
            "function setInstallPromptBusy", 1
        )[0]
        self.assertIn("if (model.overview || model.step >= model.steps.length - 1)", handler)
        self.assertIn('refreshInstallCoach({ outcome: "step-advanced" })', handler)
        self.assertIn('recordInstallFunnelEvent("install_cta_click", "manual-instructions-acknowledged")', handler)
        self.assertIn('recordInstallNudge("dismissed").then(hideInstallNudge)', handler)
        self.assertNotIn('recordInstallNudge("confirmed")', handler)

    def test_install_coach_module_and_privacy_safe_funnel_endpoints_are_loaded(self) -> None:
        self.assertIn("{% static 'miniapp/install-coach.js' %}", self.source)
        self.assertIn('data-funnel-session-url="{% url \'miniapp:funnel-session\' %}"', self.source)
        self.assertIn('data-funnel-event-url="{% url \'miniapp:funnel-event\' %}"', self.source)
        self.assertTrue(self.module_path.exists())
        self.assertIn("buildModel", self.module)
        self.assertIn("detectBrowser", self.module)
        self.assertIn("apply", self.module)
        self.assertNotIn("infinite", self.module)

    def test_install_outcomes_are_measured_without_marking_prompt_acceptance_as_installed(self) -> None:
        for event_name in (
            "install_education_view",
            "install_cta_click",
            "install_prompt_available",
            "install_prompt_accepted",
            "install_prompt_dismissed",
            "install_manual_steps_view",
            "standalone_launch",
            "install_skip",
        ):
            self.assertIn(event_name, self.source)
        prompt_handler = self.source.split("function startInstallFromCoach()", 1)[1].split(
            "function setupActivationNudges", 1
        )[0]
        self.assertNotIn('recordInstallNudge("installed")', prompt_handler)
        manual_handler = self.source.split("function advanceInstallCoach()", 1)[1].split(
            "function startInstallFromCoach", 1
        )[0]
        self.assertIn('recordInstallNudge("dismissed")', manual_handler)
        self.assertNotIn('recordInstallNudge("confirmed")', manual_handler)
        self.assertNotIn('recordInstallNudge("installed")', manual_handler)
        self.assertIn('recordInstallNudge("installed")', self.source.split('window.addEventListener("appinstalled"', 1)[1])

    def test_native_prompt_is_consumed_once_busy_guarded_and_falls_back_once(self) -> None:
        handler = self.source.split("function startInstallFromCoach()", 1)[1].split(
            "function setupActivationNudges", 1
        )[0]
        self.assertIn("if (installPromptBusy) return;", handler)
        self.assertIn("const promptEvent = deferredInstallPrompt;", handler)
        self.assertIn("deferredInstallPrompt = null;", handler)
        self.assertLess(handler.index("deferredInstallPrompt = null;"), handler.index("nativePromptRunner.run(promptEvent)"))
        self.assertIn("installPrimary.disabled = installPromptBusy", self.source)
        self.assertIn("createNativePromptRunner", self.module)
        self.assertIn("fallbackOnce", self.module)
        self.assertIn("return promptEvent.prompt();", self.module)
        self.assertIn("return promptEvent.userChoice;", self.module)
        self.assertIn(".catch(function (error)", self.module)

    def test_native_acceptance_keeps_guidance_open_until_install_completion(self) -> None:
        callbacks = self.source.split("function ensureNativePromptRunner()", 1)[1].split(
            "function startInstallFromCoach", 1
        )[0]
        accepted = callbacks.split("onAccepted: function ()", 1)[1].split("onDismissed", 1)[0]
        self.assertIn('installNativeStatus = "accepted"', accepted)
        self.assertIn("installCoachStep = 1", accepted)
        self.assertIn('refreshInstallCoach({ outcome: "accepted" })', accepted)
        self.assertNotIn("hideInstallNudge()", accepted)

        installed = self.source.split('window.addEventListener("appinstalled"', 1)[1]
        self.assertIn('installNativeStatus = "installed"', installed)
        self.assertIn("installCoachStep = 2", installed)
        self.assertIn('refreshInstallCoach({ outcome: "installed" })', installed)
        self.assertIn('recordInstallNudge("installed")', installed)

        start = self.source.split("function startInstallFromCoach()", 1)[1].split(
            "function setupActivationNudges", 1
        )[0]
        self.assertIn('model.nativeStatus !== "idle"', start)
        self.assertIn('recordInstallFunnelEvent("install_cta_click", "native-guidance-acknowledged")', start)

        self.assertIn("function closeInstallNudgeSecondary()", self.source)
        secondary = self.source.split("function closeInstallNudgeSecondary()", 1)[1].split(
            "function renderInstallNudge", 1
        )[0]
        self.assertIn('installNativeStatus !== "idle"', secondary)
        self.assertIn("hideInstallNudge()", secondary)
        self.assertIn("skipInstallNudge()", secondary)
        listener = self.source.split("function setupActivationNudges()", 1)[1].split(
            "function updateNetworkStatus", 1
        )[0]
        self.assertIn('installLater.addEventListener("click", closeInstallNudgeSecondary)', listener)
        render = self.source.split("function renderInstallNudge", 1)[1].split(
            "function maybeShowInstallNudge", 1
        )[0]
        self.assertIn('openAccessibleModal(modal, closeInstallNudgeSecondary, "installNudgePrimary")', render)
        self.assertNotIn('installNativeStatus = "idle"', render)

    def test_telegram_browser_login_handoff_uses_documented_external_browser_api(self) -> None:
        handler = self.source.split("function startInstallFromCoach()", 1)[1].split(
            "function setupActivationNudges", 1
        )[0]
        self.assertIn('model.mode === "handoff"', handler)
        self.assertIn("installNudgePayload.browser_login_url", handler)
        self.assertIn(
            "openInstallBrowserUrl(safeUrl, model.platform, fallback)",
            handler,
        )
        self.assertNotIn("openBillingUrl(", handler)

        self.assertIn("if (!safeUrl)", handler)
        self.assertIn('refreshInstallCoach({ outcome: "handoff-fallback" })', handler)
        self.assertIn("window.navigator.clipboard.writeText(safeUrl)", handler)
        opener = self.source.split("function openInstallBrowserUrl(url, platform, onFallback)", 1)[1].split(
            "function createBillingConsent", 1
        )[0]
        self.assertNotIn("try_browser", opener)
        self.assertIn("window.Telegram.WebApp.openLink(target);", opener)
        self.assertNotIn("window.location.assign", opener)
        self.assertIn("onFallback(error)", opener)

    def test_standalone_session_recovery_uses_oidc_inside_the_installed_app(self) -> None:
        self.assertIn('data-telegram-oidc-recovery-url="{% url \'miniapp:telegram-oidc-start\' %}"', self.source)
        self.assertIn('id="telegramOidcRecovery"', self.source)
        self.assertIn('id="browserLoginRecovery"', self.source)
        self.assertIn("Увійти через Telegram", self.source)
        self.assertIn("Перевстановлювати Vydno не потрібно", self.source)

        opener = self.source.split("function openTelegramOidcRecovery(event)", 1)[1].split(
            "function showDashboard", 1
        )[0]
        self.assertIn("isStandaloneMode()", opener)
        self.assertIn('window.open(telegramOidcRecoveryUrl, "vydnoTelegramLogin")', opener)
        self.assertIn("window.location.assign(telegramOidcRecoveryUrl)", opener)
        self.assertIn('recoveryLink.hidden = !browserLoginRequired || isStandaloneMode()', self.source)

        session = self.source.split("function ensureSession()", 1)[1].split("function buildQuery", 1)[0]
        self.assertIn('isStandaloneMode() ? tr("standaloneLoginRequired") : tr("browserLoginRequired")', session)

    def test_prompt_availability_waits_for_server_eligibility_and_is_telemetred_once(self) -> None:
        before_prompt = self.source.split('window.addEventListener("beforeinstallprompt"', 1)[1].split("});", 1)[0]
        self.assertIn("deferredInstallPrompt = event", before_prompt)
        self.assertIn("revealInstallAvailability()", before_prompt)
        self.assertNotIn("installBtn.hidden = false", before_prompt)
        self.assertNotIn("install_prompt_available", before_prompt)
        reveal = self.source.split("function revealInstallAvailability()", 1)[1].split(
            "function maybeShowInstallNudge", 1
        )[0]
        self.assertIn("installEligibilityKnown", reveal)
        self.assertIn("installNudgePayload.eligible", reveal)
        self.assertIn("installPromptAvailabilityRecorded", reveal)
        self.assertEqual(reveal.count('recordInstallFunnelEvent("install_prompt_available"'), 1)

    def test_install_state_is_device_scoped_without_leaking_device_id_to_funnel_telemetry(self) -> None:
        self.assertIn('const installDeviceStorageKey = "vydno.install-device.v1"', self.source)
        self.assertIn("function installEventId()", self.source)
        self.assertIn("device_id: installDeviceIdentifier()", self.source)
        status_handler = self.source.split("function maybeShowInstallNudge(force)", 1)[1].split(
            "function advanceInstallCoach", 1
        )[0]
        self.assertIn("postJson(installNudgeUrl", status_handler)
        self.assertNotIn("getJson(installNudgeUrl", status_handler)
        self.assertNotIn("device_id=", status_handler)
        self.assertNotIn("encodeURIComponent(installDeviceIdentifier())", status_handler)
        funnel_handler = self.source.split("function recordInstallFunnelEvent", 1)[1].split(
            "function hideInstallNudge", 1
        )[0]
        self.assertNotIn("device_id", funnel_handler)

    def test_motion_is_finite_interaction_driven_and_reduced_motion_safe(self) -> None:
        self.assertIn(".install-coach-card", self.css)
        self.assertIn(".install-coach-visual", self.css)
        self.assertIn("@keyframes install-coach-pulse", self.css)
        self.assertIn("@keyframes install-coach-arrow", self.css)
        self.assertIn(".install-coach-arrow", self.css)
        self.assertIn(".install-coach-command", self.css)
        self.assertIn('[data-install-mode="native"] .install-coach-android-menu', self.css)
        self.assertIn("animation-iteration-count: 2;", self.css)
        reduced = self.css.split("@media (prefers-reduced-motion: reduce)", 1)[1]
        self.assertIn(".install-coach-card", reduced)
        self.assertIn("animation: none", reduced)

    def test_install_coach_uses_accessible_modal_focus_contract(self) -> None:
        render = self.source.split("function renderInstallNudge(payload)", 1)[1].split(
            "function maybeShowInstallNudge", 1
        )[0]
        self.assertIn("openAccessibleModal", render)
        self.assertIn('"installNudgePrimary"', render)
