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
      androidNativeMessage: "Натисніть зелену кнопку нижче — Chrome відкриє системне вікно «Установити додаток».",
      androidNativeSteps: [
        "Натисніть зелену кнопку «Встановити Vydno» під інструкцією.",
        "У системному вікні Chrome натисніть «Установити».",
        "Дочекайтеся завершення встановлення в Chrome.",
        "Поверніться на головний екран і відкрийте нову іконку «Vydno».",
      ],
      androidNativeHints: [
        "Це єдина кнопка, яка запускає встановлення; меню Chrome відкривати не потрібно.",
        "Вікно має заголовок «Установити додаток» і показує назву «Vydno.Capital».",
        "Після завершення Chrome покаже системне сповіщення.",
        "Запуск із нової іконки підтвердить, що Vydno відкривається як окремий застосунок.",
      ],
      iosSteps: [
        "У Safari натисніть кнопку «Поділитися» — квадрат зі стрілкою вгору.",
        "У меню «Поділитися» прокрутіть список і виберіть «На початковий екран».",
        "Залиште «Відкрити як вебдодаток» увімкненим і натисніть «Додати» у правому верхньому куті.",
        "На головному екрані знайдіть нову іконку Vydno та відкрийте її.",
      ],
      iosHints: [
        "Кнопка розташована в панелі Safari — зверху або знизу залежно від налаштувань браузера.",
        "Якщо пункту не видно, прокрутіть список дій униз.",
        "Після натискання Safari створить окрему іконку Vydno.",
        "Саме запуск з нової іконки підтверджує, що Vydno відкривається як застосунок.",
      ],
      androidSteps: [
        "У Chrome натисніть меню ⋮ у правому верхньому куті.",
        "У меню виберіть «Встановити додаток». Якщо такого пункту немає — «Додати на головний екран».",
        "У системному вікні Chrome натисніть «Установити».",
        "На головному екрані знайдіть нову іконку Vydno та відкрийте її.",
      ],
      androidHints: [
        "Шукайте три вертикальні крапки поруч з адресним рядком Chrome.",
        "Назва залежить від версії Chrome — обидва варіанти правильні.",
        "Chrome додасть Vydno на головний екран телефона.",
        "Після запуску з іконки Vydno працюватиме без панелі браузера.",
      ],
      handoffSteps: ["У меню Telegram натисніть «Відкрити у Safari» або «Відкрити у Chrome»."],
      handoffHints: ["Вбудований браузер Telegram не може встановити PWA. У звичайному браузері ця підказка відкриється автоматично."],
      iosHandoffSteps: ["У меню Telegram натисніть «Відкрити у Safari»."],
      iosHandoffHints: ["Вбудований браузер Telegram не може встановити PWA. У Safari ця підказка відкриється автоматично."],
      safariSteps: ["Відкрийте цю сторінку в Safari, а потім поверніться до підказки встановлення."],
      safariHints: ["На iPhone встановлення Vydno гарантовано доступне через Safari."],
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
      helpAndroid: "У Chrome відкрийте меню ⋮. Залежно від версії браузера пункт може називатися «Встановити додаток» або «Додати на головний екран».",
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
      androidNativeMessage: "Tap the button below and Chrome will open the system install prompt.",
      androidNativeSteps: [
        "Tap the Install Vydno button below.",
        "Tap Install in Chrome's system dialog.",
        "Wait for Chrome to finish installing Vydno.",
        "Return to the Home Screen and open the new Vydno icon.",
      ],
      androidNativeHints: [
        "This button belongs to Vydno and only asks Chrome to open installation.",
        "Chrome controls the system dialog; Vydno cannot press its button for you.",
        "Chrome will show a system notification when installation finishes.",
        "Launching from the new icon confirms that Vydno opens as a standalone app.",
      ],
      iosSteps: [
        "In Safari, tap Share — the square with an upward arrow.",
        "In the Share menu, scroll and choose Add to Home Screen.",
        "Keep Open as Web App enabled and tap Add in the top-right corner.",
        "Find the new Vydno icon on the Home Screen and open it.",
      ],
      iosHints: [
        "The button is in Safari's toolbar at the top or bottom, depending on your settings.",
        "If the action is not visible, scroll down the actions list.",
        "Safari will create a separate Vydno icon.",
        "Launching from the new icon confirms that Vydno opens as an app.",
      ],
      androidSteps: [
        "In Chrome, tap the ⋮ menu in the top-right corner.",
        "Choose Install app. If it is missing, choose Add to Home screen.",
        "Tap Install in Chrome's system dialog.",
        "Find the new Vydno icon on the Home Screen and open it.",
      ],
      androidHints: [
        "Look for three vertical dots next to Chrome's address bar.",
        "The label depends on your Chrome version; both options are correct.",
        "Chrome will add Vydno to your phone's Home Screen.",
        "After launching from the icon, Vydno will open without browser controls.",
      ],
      handoffSteps: ["In Telegram's menu, tap Open in Safari or Open in Chrome."],
      handoffHints: ["Telegram's embedded browser cannot install a PWA. This guide will reopen automatically in a regular browser."],
      iosHandoffSteps: ["In Telegram's menu, tap Open in Safari."],
      iosHandoffHints: ["Telegram's embedded browser cannot install a PWA. This guide will reopen automatically in Safari."],
      safariSteps: ["Open this page in Safari, then return to the installation guide."],
      safariHints: ["On iPhone, Vydno installation is reliably available through Safari."],
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
      helpAndroid: "Open Chrome's ⋮ menu. Depending on the browser version, the action may be called Install app or Add to Home screen.",
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
    if (mode === "native" && nativeStatus === "accepted") requestedStep = 2;
    if (mode === "native" && nativeStatus === "installed") requestedStep = 3;
    const step = Math.min(Math.max(requestedStep, 0), Math.max(steps.length - 1, 0));
    let primaryLabel = copy.next;
    if (mode === "handoff") primaryLabel = platform === "ios" ? copy.openSafariBrowser : copy.openBrowser;
    else if (mode === "native" && nativeStatus === "accepted") primaryLabel = copy.acknowledge;
    else if (mode === "native" && nativeStatus === "installed") primaryLabel = copy.installedDone;
    else if (mode === "native") primaryLabel = copy.install;
    else if (mode === "open-safari" || mode === "unsupported") primaryLabel = copy.openSafari;
    else if (step >= steps.length - 1) primaryLabel = copy.complete;
    if (mode === "native" && nativeStatus === "accepted") title = copy.installingTitle;
    if (mode === "native" && nativeStatus === "installed") title = copy.installedTitle;

    let helpText = copy.helpAndroid;
    if (telegram) helpText = platform === "ios" ? copy.helpTelegramIos : copy.helpTelegram;
    else if (platform === "ios" && browser !== "safari") helpText = copy.helpIosBrowser;
    else if (platform === "ios") helpText = copy.helpIos;

    return {
      locale: locale,
      platform: platform,
      browser: browser,
      mode: mode,
      nativeStatus: nativeStatus,
      step: step,
      steps: steps.slice(),
      hints: hints.slice(),
      command: steps[step] || message,
      hint: hints[step] || "",
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
      complete: mode === "manual" && step >= steps.length - 1,
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
      if (index === model.step) item.setAttribute("aria-current", "step");
      list.appendChild(item);
    });
  }

  function apply(modal, options) {
    if (!modal) return null;
    const model = buildModel(options);
    modal.dataset.installPlatform = model.platform;
    modal.dataset.installMode = model.mode;
    modal.dataset.installStep = String(model.step);
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
