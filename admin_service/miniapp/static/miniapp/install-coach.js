(function (root, factory) {
  "use strict";

  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.VydnoInstallCoach = api;
})(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const COPY = {
    uk: {
      eyebrow: "Окремий застосунок",
      title: "Додайте Vydno на головний екран",
      message: "Vydno відкриватиметься одним тапом — без пошуку вкладки або чату в Telegram.",
      telegramTitle: "Відкрийте Vydno у браузері",
      telegramMessage: "Перейдіть через захищений вхід у Safari або Chrome. Там ми покажемо встановлення для вашого телефона.",
      iosTelegramTitle: "Відкрийте Vydno у Safari",
      iosTelegramMessage: "Перейдіть через захищений вхід у Safari. Там ми покажемо встановлення для iPhone.",
      safariTitle: "Спочатку відкрийте сторінку в Safari",
      safariMessage: "На iPhone встановлення PWA доступне через меню Safari.",
      androidNativeMessage: "Усі 3 дії видно одразу. Почніть із зеленої кнопки — Chrome відкриє системне вікно встановлення.",
      androidNativeSteps: [
        "Натисніть зелену кнопку «Встановити Vydno» під інструкцією.",
        "Chrome відкриє системне вікно «Установити додаток» із назвою «Vydno.Capital». Натисніть «Установити» й дочекайтеся завершення.",
        "На головному екрані відкрийте нову іконку Vydno.",
      ],
      androidNativeHints: [
        "Це єдина кнопка, яка запускає встановлення; меню Chrome відкривати не потрібно.",
        "Вікно має заголовок «Установити додаток» і показує назву «Vydno.Capital».",
        "Запуск із нової іконки підтвердить, що Vydno відкривається як окремий застосунок.",
      ],
      androidNativePendingCommand: "Дочекайтеся завершення встановлення в Chrome.",
      androidNativePendingHint: "Після завершення Chrome покаже системне сповіщення.",
      iosSteps: [
        "У Safari натисніть кнопку «Поділитися» — квадрат зі стрілкою вгору.",
        "У меню «Поділитися» прокрутіть список і виберіть «На початковий екран».",
        "Залиште «Відкрити як вебдодаток» увімкненим і натисніть «Додати» у правому верхньому куті.",
      ],
      iosHints: [
        "Кнопка розташована в панелі Safari — зверху або знизу залежно від налаштувань браузера.",
        "Якщо пункту не видно, прокрутіть список дій униз.",
        "Після натискання Safari створить окрему іконку Vydno.",
      ],
      androidSteps: [
        "У Chrome натисніть меню ⋮ угорі праворуч.",
        "Натисніть «Додати на головний екран», потім «Установити додаток». Якщо Chrome одразу показує «Установити додаток», виберіть його.",
        "У вікні підтвердження перевірте назву «Vydno.Capital» і натисніть «Установити».",
      ],
      androidHints: [
        "Шукайте три вертикальні крапки поруч з адресним рядком Chrome.",
        "Актуальний шлях Chrome: меню ⋮ → «Додати на головний екран» → «Установити додаток».",
        "Після завершення знайдіть нову іконку Vydno на головному екрані та відкрийте її.",
      ],
      handoffSteps: ["У меню Telegram натисніть «Відкрити у Safari» або «Відкрити у Chrome»."],
      handoffHints: ["Вбудований браузер Telegram не може встановити PWA. У звичайному браузері ця підказка відкриється автоматично."],
      iosHandoffSteps: ["У меню Telegram натисніть «Відкрити у Safari»."],
      iosHandoffHints: ["Вбудований браузер Telegram не може встановити PWA. У Safari ця підказка відкриється автоматично."],
      safariSteps: ["Відкрийте цю сторінку в Safari, а потім поверніться до підказки встановлення."],
      safariHints: ["На iPhone встановлення Vydno гарантовано доступне через Safari."],
      manualCommand: function (total) { return "Виконайте всі " + total + " дії по черзі"; },
      manualHintIos: "Після додавання відкрийте нову іконку Vydno. На iPhone застосунок попросить увійти через Telegram окремо від Safari.",
      manualHintAndroid: "Після встановлення відкрийте нову іконку Vydno на головному екрані.",
      next: "Далі",
      complete: "Готово — Vydno відкрито",
      acknowledge: "Зрозуміло",
      installedDone: "Готово",
      closeGuidance: "Закрити підказку",
      installingTitle: "Vydno встановлюється",
      installedTitle: "Vydno встановлено",
      install: "Встановити Vydno",
      openBrowser: "Відкрити у Safari / Chrome",
      openSafariBrowser: "Відкрити у Safari",
      openSafari: "Як відкрити в Safari",
      help: "Не бачу цієї кнопки",
      skip: "Продовжити без встановлення",
      step: function (current, total) { return "Крок " + current + " із " + total; },
      helpIos: "Якщо пункту немає, переконайтеся, що сторінка відкрита саме в Safari. Прокрутіть список дій униз і натисніть «Редагувати дії», щоб додати «На початковий екран».",
      helpIosBrowser: "Скопіюйте адресу цієї сторінки, відкрийте Safari та вставте її в адресний рядок. Встановлення з Chrome або вбудованого браузера на iPhone може бути недоступним.",
      helpAndroid: "У Chrome відкрийте меню ⋮. Актуальний шлях: «Додати на головний екран» → «Установити додаток». У деяких версіях «Установити додаток» видно відразу в меню.",
      helpTelegram: "Вбудований браузер Telegram не встановлює PWA. Відкрийте захищене посилання у звичайному Safari або Chrome.",
      helpTelegramIos: "Вбудований браузер Telegram не встановлює PWA. Відкрийте захищене посилання у Safari.",
      footnote: "Встановлення не є обов’язковим і не блокує доступ до Vydno.",
    },
    en: {
      eyebrow: "Your own app",
      title: "Add Vydno to your Home Screen",
      message: "Open Vydno in one tap without looking for a browser tab or Telegram chat.",
      telegramTitle: "Open Vydno in your browser",
      telegramMessage: "Continue through the secure login in Safari or Chrome. We will show the right install steps there.",
      iosTelegramTitle: "Open Vydno in Safari",
      iosTelegramMessage: "Continue through the secure login in Safari. We will show the iPhone install steps there.",
      safariTitle: "Open this page in Safari first",
      safariMessage: "On iPhone, PWA installation is available from Safari's menu.",
      androidNativeMessage: "All 3 actions are visible at once. Start with the button below and Chrome will open the system install prompt.",
      androidNativeSteps: [
        "Tap the Install Vydno button below.",
        "Chrome will open the Install app system dialog showing Vydno.Capital. Tap Install and wait for installation to finish.",
        "Open the new Vydno icon on the Home Screen.",
      ],
      androidNativeHints: [
        "This button belongs to Vydno and only asks Chrome to open installation.",
        "The dialog is titled Install app and shows the name Vydno.Capital.",
        "Launching from the new icon confirms that Vydno opens as a standalone app.",
      ],
      androidNativePendingCommand: "Wait for Chrome to finish installing Vydno.",
      androidNativePendingHint: "Chrome will show a system notification when installation finishes.",
      iosSteps: [
        "In Safari, tap Share — the square with an upward arrow.",
        "In the Share menu, scroll and choose Add to Home Screen.",
        "Keep Open as Web App enabled and tap Add in the top-right corner.",
      ],
      iosHints: [
        "The button is in Safari's toolbar at the top or bottom, depending on your settings.",
        "If the action is not visible, scroll down the actions list.",
        "Safari will create a separate Vydno icon.",
      ],
      androidSteps: [
        "In Chrome, tap the ⋮ menu in the top-right corner.",
        "Tap Add to Home screen, then Install app. If Chrome shows Install app directly, choose it.",
        "Confirm the name Vydno.Capital, then tap Install.",
      ],
      androidHints: [
        "Look for three vertical dots next to Chrome's address bar.",
        "Chrome's current path is ⋮ → Add to Home screen → Install app.",
        "When installation finishes, find the new Vydno icon on the Home Screen and open it.",
      ],
      handoffSteps: ["In Telegram's menu, tap Open in Safari or Open in Chrome."],
      handoffHints: ["Telegram's embedded browser cannot install a PWA. This guide will reopen automatically in a regular browser."],
      iosHandoffSteps: ["In Telegram's menu, tap Open in Safari."],
      iosHandoffHints: ["Telegram's embedded browser cannot install a PWA. This guide will reopen automatically in Safari."],
      safariSteps: ["Open this page in Safari, then return to the installation guide."],
      safariHints: ["On iPhone, Vydno installation is reliably available through Safari."],
      manualCommand: function (total) { return "Follow all " + total + " steps in order"; },
      manualHintIos: "Then open the new Vydno icon. On iPhone, the app will ask you to sign in with Telegram separately from Safari.",
      manualHintAndroid: "After installation, open the new Vydno icon on the Home Screen.",
      next: "Next",
      complete: "Done — Vydno is open",
      acknowledge: "Got it",
      installedDone: "Done",
      closeGuidance: "Close guide",
      installingTitle: "Vydno is installing",
      installedTitle: "Vydno is installed",
      install: "Install Vydno",
      openBrowser: "Open in Safari / Chrome",
      openSafariBrowser: "Open in Safari",
      openSafari: "How to open Safari",
      help: "I cannot find this button",
      skip: "Continue without installing",
      step: function (current, total) { return "Step " + current + " of " + total; },
      helpIos: "If the action is missing, make sure this page is open in Safari. Scroll down the action list and choose Edit Actions to add Add to Home Screen.",
      helpIosBrowser: "Copy this page address, open Safari, and paste it into the address bar. Installation may be unavailable in Chrome or an embedded browser on iPhone.",
      helpAndroid: "Open Chrome's ⋮ menu. The current path is Add to Home screen → Install app. Some Chrome versions show Install app directly in the menu.",
      helpTelegram: "Telegram's embedded browser cannot install a PWA. Open the secure link in regular Safari or Chrome.",
      helpTelegramIos: "Telegram's embedded browser cannot install a PWA. Open the secure link in Safari.",
      footnote: "Installation is optional and never blocks access to Vydno.",
    },
  };

  function detectBrowser(navigatorLike) {
    const userAgent = String((navigatorLike && navigatorLike.userAgent) || "");
    if (/CriOS/i.test(userAgent)) return "chrome-ios";
    if (/FxiOS/i.test(userAgent)) return "firefox-ios";
    if (/EdgiOS/i.test(userAgent)) return "edge-ios";
    if (/OPiOS/i.test(userAgent)) return "opera-ios";
    if (/SamsungBrowser/i.test(userAgent)) return "samsung";
    if (/Android/i.test(userAgent) && /Chrome|Chromium/i.test(userAgent)) return "chrome";
    if (/Safari/i.test(userAgent) && /Version\//i.test(userAgent) && !/Android/i.test(userAgent)) return "safari";
    return "other";
  }

  function nextStep(step, total) {
    const last = Math.max(Number(total || 1) - 1, 0);
    return Math.min(Math.max(Number(step || 0) + 1, 0), last);
  }

  function buildModel(options) {
    const settings = options || {};
    const locale = settings.locale === "en" ? "en" : "uk";
    const copy = COPY[locale];
    const platform = String(settings.platform || "other");
    const browser = String(settings.browser || "other");
    const telegram = Boolean(settings.telegram);
    const canPrompt = Boolean(settings.canPrompt);
    const nativeStatus = settings.nativeStatus === "accepted" || settings.nativeStatus === "installed"
      ? settings.nativeStatus
      : "idle";
    let mode = "unsupported";
    let steps = platform === "ios" ? copy.iosSteps : copy.androidSteps;
    let hints = platform === "ios" ? copy.iosHints : copy.androidHints;
    let title = copy.title;
    let message = copy.message;

    if (telegram) {
      mode = "handoff";
      steps = platform === "ios" ? copy.iosHandoffSteps : copy.handoffSteps;
      hints = platform === "ios" ? copy.iosHandoffHints : copy.handoffHints;
      title = platform === "ios" ? copy.iosTelegramTitle : copy.telegramTitle;
      message = platform === "ios" ? copy.iosTelegramMessage : copy.telegramMessage;
    } else if (platform === "ios" && browser === "safari") {
      mode = "manual";
    } else if (platform === "ios") {
      mode = "open-safari";
      steps = copy.safariSteps;
      hints = copy.safariHints;
      title = copy.safariTitle;
      message = copy.safariMessage;
    } else if ((platform === "android" || platform === "desktop") && (canPrompt || nativeStatus !== "idle")) {
      mode = "native";
      message = copy.androidNativeMessage;
      steps = copy.androidNativeSteps;
      hints = copy.androidNativeHints;
    } else if (platform === "android") {
      mode = "manual";
    }

    let requestedStep = Number(settings.step || 0);
    if (mode === "native" && nativeStatus === "accepted") requestedStep = 1;
    if (mode === "native" && nativeStatus === "installed") requestedStep = 2;
    const step = Math.min(Math.max(requestedStep, 0), Math.max(steps.length - 1, 0));
    const overview = mode === "manual" || (mode === "native" && nativeStatus === "idle");
    let primaryLabel = copy.next;
    if (mode === "handoff") primaryLabel = platform === "ios" ? copy.openSafariBrowser : copy.openBrowser;
    else if (mode === "native" && nativeStatus === "accepted") primaryLabel = copy.acknowledge;
    else if (mode === "native" && nativeStatus === "installed") primaryLabel = copy.installedDone;
    else if (mode === "native") primaryLabel = copy.install;
    else if (mode === "open-safari" || mode === "unsupported") primaryLabel = copy.openSafari;
    else if (overview) primaryLabel = copy.acknowledge;
    else if (step >= steps.length - 1) primaryLabel = copy.complete;
    if (mode === "native" && nativeStatus === "accepted") title = copy.installingTitle;
    if (mode === "native" && nativeStatus === "installed") title = copy.installedTitle;

    let helpText = copy.helpAndroid;
    if (telegram) helpText = platform === "ios" ? copy.helpTelegramIos : copy.helpTelegram;
    else if (platform === "ios" && browser !== "safari") helpText = copy.helpIosBrowser;
    else if (platform === "ios") helpText = copy.helpIos;

    let command = overview ? copy.manualCommand(steps.length) : steps[step] || message;
    let hint = overview
      ? (platform === "ios" ? copy.manualHintIos : copy.manualHintAndroid)
      : hints[step] || "";
    if (mode === "native" && nativeStatus === "accepted") {
      command = copy.androidNativePendingCommand;
      hint = copy.androidNativePendingHint;
    }

    return {
      locale: locale,
      platform: platform,
      browser: browser,
      mode: mode,
      nativeStatus: nativeStatus,
      overview: overview,
      step: step,
      steps: steps.slice(),
      hints: hints.slice(),
      command: command,
      hint: hint,
      eyebrow: copy.eyebrow,
      title: title,
      message: message,
      progressLabel: copy.step(step + 1, steps.length),
      primaryLabel: primaryLabel,
      helpLabel: copy.help,
      skipLabel: mode === "native" && nativeStatus !== "idle" ? copy.closeGuidance : copy.skip,
      helpText: helpText,
      footnote: copy.footnote,
      showHelpAction: mode !== "native",
      showSkipAction: !(mode === "native" && nativeStatus !== "idle"),
      complete: false,
    };
  }

  function byId(modal, id) {
    return modal && modal.querySelector ? modal.querySelector("#" + id) : null;
  }

  function text(modal, id, value) {
    const node = byId(modal, id);
    if (node) node.textContent = value || "";
  }

  function renderSteps(modal, model) {
    const list = byId(modal, "installNudgeSteps");
    if (!list || !list.ownerDocument) return;
    while (list.firstChild) list.removeChild(list.firstChild);
    model.steps.forEach(function (label, index) {
      const item = list.ownerDocument.createElement("li");
      const number = list.ownerDocument.createElement("span");
      const copy = list.ownerDocument.createElement("span");
      number.className = "install-coach-step-number";
      number.textContent = String(index + 1);
      copy.className = "install-coach-step-label";
      copy.textContent = label;
      item.appendChild(number);
      item.appendChild(copy);
      item.dataset.installStep = String(index);
      if (!model.overview && index === model.step) item.setAttribute("aria-current", "step");
      list.appendChild(item);
    });
  }

  function apply(modal, options) {
    if (!modal) return null;
    const model = buildModel(options);
    modal.dataset.installPlatform = model.platform;
    modal.dataset.installMode = model.mode;
    modal.dataset.installStep = String(model.step);
    modal.dataset.installOverview = model.overview ? "true" : "false";
    modal.dataset.installOutcome = String((options && options.outcome) || "");
    text(modal, "installNudgeEyebrow", model.eyebrow);
    text(modal, "installNudgeTitle", model.title);
    text(modal, "installNudgeMessage", model.message);
    text(modal, "installCoachCommand", model.command);
    text(modal, "installCoachHint", model.hint);
    text(modal, "installCoachProgress", model.progressLabel);
    text(modal, "installNudgePrimary", model.primaryLabel);
    text(modal, "installNudgeHelp", model.helpLabel);
    text(modal, "installNudgeLater", model.skipLabel);
    text(modal, "installNudgeFootnote", model.footnote);
    text(modal, "installCoachHelp", model.helpText);
    renderSteps(modal, model);

    const helpAction = byId(modal, "installNudgeHelp");
    if (helpAction) {
      helpAction.hidden = !model.showHelpAction;
      if (!model.showHelpAction) helpAction.setAttribute("aria-expanded", "false");
    }
    const skipAction = byId(modal, "installNudgeLater");
    if (skipAction) skipAction.hidden = !model.showSkipAction;

    const visual = byId(modal, "installCoachVisual");
    if (visual) {
      visual.dataset.installPlatform = model.platform;
      visual.dataset.installMode = model.mode;
      visual.dataset.installStep = String(model.step);
      visual.setAttribute("aria-label", model.steps[model.step] || model.message);
      visual.classList.remove("is-step-animating");
      void visual.offsetWidth;
      visual.classList.add("is-step-animating");
    }
    const help = byId(modal, "installCoachHelp");
    if (help) help.hidden = !(options && options.helpOpen);
    const progress = byId(modal, "installCoachProgressBar");
    if (progress) {
      progress.max = Math.max(model.steps.length, 1);
      progress.value = Math.min(model.step + 1, model.steps.length);
    }
    return model;
  }

  function createNativePromptRunner(callbacks) {
    const hooks = callbacks || {};
    let busy = false;
    let fallbackShown = false;

    function notify(name) {
      if (typeof hooks[name] === "function") hooks[name].apply(null, Array.prototype.slice.call(arguments, 1));
    }

    function fallbackOnce(reason, error) {
      if (fallbackShown) return;
      fallbackShown = true;
      notify("onFallback", reason, error);
    }

    function run(promptEvent) {
      if (!promptEvent || typeof promptEvent.prompt !== "function") {
        return Promise.resolve({ status: "unavailable" });
      }
      if (busy) return Promise.resolve({ status: "busy" });
      busy = true;
      notify("onBusy", true);
      return Promise.resolve()
        .then(function () { return promptEvent.prompt(); })
        .then(function () { return promptEvent.userChoice; })
        .then(function (choice) {
          if (choice && choice.outcome === "accepted") {
            notify("onAccepted", choice);
            return { status: "accepted", choice: choice };
          }
          notify("onDismissed", choice || null);
          fallbackOnce("dismissed");
          return { status: "dismissed", choice: choice || null };
        })
        .catch(function (error) {
          notify("onError", error);
          fallbackOnce("error", error);
          return { status: "error", error: error };
        })
        .finally(function () {
          busy = false;
          notify("onBusy", false);
        });
    }

    return Object.freeze({
      isBusy: function () { return busy; },
      run: run,
    });
  }

  return Object.freeze({
    apply: apply,
    buildModel: buildModel,
    createNativePromptRunner: createNativePromptRunner,
    detectBrowser: detectBrowser,
    nextStep: nextStep,
  });
});
