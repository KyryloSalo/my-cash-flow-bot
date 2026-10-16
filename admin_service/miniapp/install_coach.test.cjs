const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const coach = require("./static/miniapp/install-coach.js");
const templateSource = fs.readFileSync(path.join(__dirname, "templates", "miniapp", "index.html"), "utf8");
const cssSource = fs.readFileSync(path.join(__dirname, "static", "miniapp", "app.css"), "utf8");

function loadInlineFunction(name, context) {
  if (["openBillingUrl", "openInstallBrowserUrl"].includes(name)) {
    context = { ...context, isTelegramHost: loadInlineFunction("isTelegramHost", context) };
  }
  const marker = "function " + name + "(";
  const start = templateSource.indexOf(marker);
  assert.notEqual(start, -1, "missing inline function " + name);
  const bodyStart = templateSource.indexOf("{", start);
  let depth = 0;
  for (let index = bodyStart; index < templateSource.length; index += 1) {
    if (templateSource[index] === "{") depth += 1;
    if (templateSource[index] === "}") depth -= 1;
    if (depth === 0) {
      const declaration = templateSource.slice(start, index + 1);
      return vm.runInNewContext("(" + declaration + ")", context);
    }
  }
  throw new Error("unterminated inline function " + name);
}

test("detects Safari, iOS alternate browsers, and Android Chrome", () => {
  assert.equal(coach.detectBrowser({ userAgent: "Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 Version/18.0 Mobile Safari/604.1" }), "safari");
  assert.equal(coach.detectBrowser({ userAgent: "Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 CriOS/128.0 Mobile/15E148 Safari/604.1" }), "chrome-ios");
  assert.equal(coach.detectBrowser({ userAgent: "Mozilla/5.0 (Linux; Android 15) AppleWebKit/537.36 Chrome/128.0 Mobile Safari/537.36" }), "chrome");
});

test("shows the four-step iOS animation immediately without claiming installation", () => {
  const first = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 0 });
  assert.equal(first.mode, "manual");
  assert.equal(first.overview, false);
  assert.equal(first.steps.length, 4);
  assert.equal(first.step, 0);
  assert.match(first.command, /Поділитися/iu);
  assert.match(first.steps[0], /Поділитися/iu);
  assert.match(first.steps[1], /На початковий екран/iu);
  assert.match(first.steps[2], /Додати/iu);
  assert.match(first.steps[3], /іконк.+Vydno/iu);
  assert.equal(first.primaryLabel, "Далі");
  assert.equal(first.complete, false);
  assert.equal(first.skipLabel, "Не зараз");
  const last = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 3 });
  assert.equal(last.primaryLabel, "Зрозуміло");
  assert.equal(last.complete, false);
  const english = coach.buildModel({ locale: "en", platform: "ios", browser: "safari", step: 3 });
  assert.equal(english.steps.length, 4);
  assert.equal(english.hints.length, 4);
  assert.match(english.command, /new Vydno icon/u);
  assert.equal(english.primaryLabel, "Got it");
  assert.match(
    cssSource,
    /data-install-overview="true"[^}]+\.install-coach-step-label[^}]+position:\s*static/su,
  );
});

test("uses the native Android prompt only when a real prompt is available", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true });
  assert.equal(native.mode, "native");
  assert.equal(native.overview, false);
  assert.equal(native.primaryLabel, "Встановити і додати на головний екран");
  assert.equal(native.command, "");

  assert.match(native.steps[0], /Встановити і додати/iu);
  assert.doesNotMatch(native.steps[0], /системн/i);
  assert.match(native.steps[1], /системному вікні|системне вікно/i);
  assert.match(native.steps[1], /підтвердьте встановлення/iu);
  assert.match(native.steps[2], /іконк.+Vydno/i);
  assert.match(native.primaryLabel, /Встановити/);

  const manual = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: false });
  assert.equal(manual.mode, "manual");
  assert.equal(manual.overview, true);
  assert.equal(manual.steps.length, 3);
  assert.match(manual.steps[0], /угорі праворуч/iu);
  assert.match(manual.steps[1], /Додати на головний екран.+Установити додаток/iu);
  assert.match(manual.steps[2], /Vydno\.Capital.+Установити/iu);
  assert.match(manual.command, /3 дії/iu);
  assert.match(manual.hint, /іконк.+Vydno/iu);
  assert.doesNotMatch(manual.hint, /iPhone|Safari/iu);
  assert.equal(manual.primaryLabel, "Зрозуміло");
});

test("native Android coach exposes one product action before opening Chrome's prompt", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true });
  assert.equal(native.showHelpAction, false);
  assert.equal(native.showSkipAction, true);
  assert.doesNotMatch(
    templateSource,
    /<button class="install-coach-browser-install"[^>]*>Встановити Vydno<\/button>/u,
  );
  assert.match(templateSource, /class="install-coach-native-ready"/u);
  assert.match(cssSource, /#installNudgeHelp\[hidden\]/u);
  for (const selector of [".install-coach-visual", ".install-coach-command", ".install-coach-progress-row", ".install-coach-steps"]) {
    assert.ok(cssSource.includes('[data-install-status="idle"] ' + selector));
  }
  assert.match(cssSource, /data-install-mode="native"[^}]+#installNudgePrimary::before/su);
});

test("native Android does not promise a particular system dialog label", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true, step: 1 });
  assert.match(native.steps[1], /системному вікні/iu);
  assert.doesNotMatch(native.steps.join(" "), /Установити додаток|Vydno\.Capital/iu);
  assert.equal(native.overview, false);
  assert.match(native.hints[1], /залежать від браузера/iu);
  assert.match(
    templateSource,
    /<span><strong>Vydno\.Capital<\/strong><small>Установити додаток<\/small><\/span><button[^>]*>Установити<\/button>/u,
  );
});

test("native Android keeps an honest installation-pending scene after prompt acceptance", () => {
  const accepted = coach.buildModel({
    locale: "uk",
    platform: "android",
    browser: "chrome",
    canPrompt: false,
    nativeStatus: "accepted",
  });
  assert.equal(accepted.mode, "native");
  assert.equal(accepted.nativeStatus, "accepted");
  assert.equal(accepted.overview, false);
  assert.equal(accepted.step, 1);
  assert.equal(accepted.title, "Vydno встановлюється");
  assert.match(accepted.command, /Дочекайтеся/iu);
  assert.match(accepted.hint, /Chrome|сповіщення/iu);
  assert.equal(accepted.primaryLabel, "Зрозуміло");
  assert.equal(accepted.skipLabel, "Закрити підказку");
  assert.equal(accepted.showHelpAction, false);
  assert.equal(accepted.showSkipAction, false);
  assert.match(templateSource, /class="install-coach-installing"/u);
  assert.match(
    cssSource,
    /data-install-mode="native"\]\[data-install-step="1"\] \.install-coach-native-card\s*\{[^}]*opacity:\s*0/su,
  );
});

test("native Android shows the installed icon scene only after appinstalled", () => {
  const installed = coach.buildModel({
    locale: "uk",
    platform: "android",
    browser: "chrome",
    canPrompt: false,
    nativeStatus: "installed",
  });
  assert.equal(installed.mode, "native");
  assert.equal(installed.nativeStatus, "installed");
  assert.equal(installed.step, 2);
  assert.equal(installed.title, "Vydno встановлено");
  assert.match(installed.command, /головн.+екран/iu);
  assert.match(installed.command, /іконк/iu);
  assert.equal(installed.primaryLabel, "Готово");
  assert.equal(installed.skipLabel, "Закрити підказку");
  assert.equal(installed.showSkipAction, false);
  assert.match(cssSource, /#installNudgeLater\[hidden\]\s*\{[^}]*display:\s*none/su);
});

test("does not pretend alternate iOS browsers can show the Safari install action", () => {
  const model = coach.buildModel({ locale: "uk", platform: "ios", browser: "chrome-ios", canPrompt: false });
  assert.equal(model.mode, "open-safari");
  assert.match(model.primaryLabel, /Safari/);
  assert.match(model.helpText, /скопіюйте/iu);
  assert.doesNotMatch(model.command, /Chrome/iu);
});

test("Telegram handoff points to the product button, not a nonexistent Telegram menu action", () => {
  for (const locale of ["uk", "en"]) {
    const ios = coach.buildModel({ locale, platform: "ios", telegram: true });
    assert.equal(ios.mode, "handoff");
    assert.match(ios.command, locale === "uk" ? /зелену кнопку нижче/iu : /green button below/iu);
    assert.doesNotMatch(ios.command, /меню Telegram|Telegram's menu/iu);
    assert.equal(ios.primaryLabel, locale === "uk" ? "Відкрити у браузері" : "Open in browser");
  }

  const android = coach.buildModel({ locale: "en", platform: "android", telegram: true });
  assert.equal(android.mode, "handoff");
  assert.match(android.command, /green button below/iu);
  assert.doesNotMatch(android.command, /Telegram's menu/iu);
  assert.equal(android.primaryLabel, "Open in browser");

  const iosScene = templateSource.split('data-install-scene="handoff-ios"', 2)[1].split('data-install-scene="handoff-android"', 1)[0];
  const androidScene = templateSource.split('data-install-scene="handoff-android"', 2)[1].split('data-install-scene="open-safari"', 1)[0];
  for (const scene of [iosScene, androidScene]) {
    assert.match(scene, /install-coach-handoff-action/u);
    assert.doesNotMatch(scene, /install-coach-handoff-menu|<span>⋮<\/span>/u);
  }
});

test("Telegram install handoff uses the supported external-browser API without invented browser hints", () => {
  const handoffUrl = "https://vydno.example/app/browser-login/short-lived-token/?install=1";
  for (const platform of ["ios", "android"]) {
    const calls = [];
    const assignments = [];
    const context = {
      window: {
        Telegram: { WebApp: { initData: "telegram-host-fixture", openLink(...args) { calls.push(args); } } },
        location: { assign(url) { assignments.push(url); } },
      },
    };
    const openInstallBrowserUrl = loadInlineFunction("openInstallBrowserUrl", context);

    openInstallBrowserUrl(handoffUrl, platform);

    assert.equal(calls.length, 1);
    assert.equal(calls[0].length, 1, platform + " must use Telegram's documented external-browser call");
    assert.equal(calls[0][0], handoffUrl);
    assert.deepEqual(assignments, []);
  }
});

test("Telegram handoff failure stays in the guide instead of navigating internally", () => {
  for (const api of [null, { initData: "telegram-host-fixture", openLink() { throw new Error("blocked"); } }]) {
    const failures = [];
    const open = loadInlineFunction("openInstallBrowserUrl", {
      window: {
        Telegram: api ? { WebApp: api } : undefined,
        location: { assign() { throw new Error("internal navigation forbidden"); } },
      },
    });
    assert.equal(open("https://vydno.example/fresh-login", "ios", (error) => failures.push(error.message)), false);
    assert.equal(failures.length, 1);
  }
});

test("billing checkout navigates in browsers and installed PWAs even when Telegram SDK is loaded", () => {
  const checkoutUrl = "https://pay.example.test/checkout-fixture";
  for (const container of ["browser", "standalone"]) {
    const calls = [];
    const assignments = [];
    const open = loadInlineFunction("openBillingUrl", {
      window: {
        Telegram: { WebApp: { platform: "unknown", initData: "", openLink(url) { calls.push(url); } } },
        navigator: { standalone: container === "standalone" },
        location: { assign(url) { assignments.push(url); } },
      },
    });

    open(checkoutUrl);

    assert.deepEqual(assignments, [checkoutUrl], container + " must not send checkout to an absent Telegram host");
    assert.deepEqual(calls, []);
  }
});

test("billing checkout preserves Telegram-host navigation and SDK-free browser fallback", () => {
  const checkoutUrl = "https://pay.example.test/checkout-fixture";
  for (const telegramHost of [false, true]) {
    const calls = [];
    const assignments = [];
    const open = loadInlineFunction("openBillingUrl", {
      window: {
        Telegram: telegramHost
          ? { WebApp: { initData: "telegram-host-fixture", openLink(url) { calls.push(url); } } }
          : undefined,
        location: { assign(url) { assignments.push(url); } },
      },
    });

    open(checkoutUrl);

    assert.deepEqual(calls, telegramHost ? [checkoutUrl] : []);
    assert.deepEqual(assignments, telegramHost ? [] : [checkoutUrl]);
  }
});

test("SDK-only browser cannot falsely acknowledge a Telegram install handoff", () => {
  const calls = [];
  const failures = [];
  const open = loadInlineFunction("openInstallBrowserUrl", {
    window: {
      Telegram: { WebApp: { platform: "unknown", initData: "", openLink(url) { calls.push(url); } } },
      location: { assign() { throw new Error("install handoff must not navigate internally"); } },
    },
  });

  assert.equal(open("https://vydno.example/fresh-login", "ios", (error) => failures.push(error.message)), false);
  assert.deepEqual(calls, []);
  assert.equal(failures.length, 1);
});

test("Telegram unavailable or failed browser handoff never dismisses the guide", () => {
  for (const url of ["", "https://vydno.example/fresh-login"]) {
    const refreshes = [];
    let hidden = false;
    const start = loadInlineFunction("startInstallFromCoach", {
      installPromptBusy: false,
      installNudgePayload: { browser_login_url: url },
      installCoachHelpOpen: false,
      window: {
        VydnoInstallCoach: { buildModel: () => ({ mode: "handoff", platform: "ios" }) },
        navigator: {},
      },
      installCoachOptions: () => ({}),
      recordInstallFunnelEvent: () => {},
      refreshInstallCoach: (options) => refreshes.push(options.outcome),
      openInstallBrowserUrl: (target, platform, fallback) => { fallback(); return false; },
      hideInstallNudge: () => { hidden = true; },
      advanceInstallCoach: () => { throw new Error("must not acknowledge a handoff failure"); },
    });
    start();
    assert.deepEqual(refreshes, ["handoff-fallback"]);
    assert.equal(hidden, false);
  }
});

test("manual step progression is bounded", () => {
  assert.equal(coach.nextStep(0, 4), 1);
  assert.equal(coach.nextStep(2, 4), 3);
  assert.equal(coach.nextStep(3, 4), 3);
});

test("native prompt runner is non-reentrant and releases its busy state", async () => {
  let resolveChoice;
  let promptCalls = 0;
  const busyStates = [];
  const accepted = [];
  const fallback = [];
  const event = {
    prompt() { promptCalls += 1; },
    userChoice: new Promise((resolve) => { resolveChoice = resolve; }),
  };
  const runner = coach.createNativePromptRunner({
    onBusy(value) { busyStates.push(value); },
    onAccepted(choice) { accepted.push(choice.outcome); },
    onFallback(reason) { fallback.push(reason); },
  });

  const first = runner.run(event);
  const second = await runner.run(event);
  assert.equal(second.status, "busy");
  assert.equal(promptCalls, 1);
  resolveChoice({ outcome: "accepted" });
  assert.equal((await first).status, "accepted");
  assert.deepEqual(busyStates, [true, false]);
  assert.deepEqual(accepted, ["accepted"]);
  assert.deepEqual(fallback, []);
});

test("native prompt runner falls back once for prompt and choice rejection", async () => {
  for (const failure of ["prompt", "choice"]) {
    const fallback = [];
    const runner = coach.createNativePromptRunner({ onFallback(reason) { fallback.push(reason); } });
    const event = failure === "prompt"
      ? { prompt() { return Promise.reject(new Error("prompt failed")); }, userChoice: Promise.resolve({ outcome: "accepted" }) }
      : { prompt() {}, userChoice: Promise.reject(new Error("choice failed")) };

    assert.equal((await runner.run(event)).status, "error");
    assert.equal(fallback.length, 1);
    assert.equal((await runner.run(null)).status, "unavailable");
    assert.equal(fallback.length, 1);
  }
});
