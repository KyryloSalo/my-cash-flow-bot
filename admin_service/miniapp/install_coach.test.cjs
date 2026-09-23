const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const coach = require("./static/miniapp/install-coach.js");
const templateSource = fs.readFileSync(path.join(__dirname, "templates", "miniapp", "index.html"), "utf8");
const cssSource = fs.readFileSync(path.join(__dirname, "static", "miniapp", "app.css"), "utf8");

function loadInlineFunction(name, context) {
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

test("shows the complete three-step iOS install instruction at once", () => {
  const first = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 0 });
  assert.equal(first.mode, "manual");
  assert.equal(first.overview, true);
  assert.equal(first.steps.length, 3);
  assert.equal(first.step, 0);
  assert.match(first.command, /усі 3 дії|всі 3 дії/iu);
  assert.match(first.steps[0], /Поділитися/iu);
  assert.match(first.steps[1], /На початковий екран/iu);
  assert.match(first.steps[2], /Додати/iu);
  assert.match(first.hint, /іконк.+Vydno/iu);
  assert.equal(first.primaryLabel, "Зрозуміло");
  assert.equal(first.complete, false);
  assert.match(first.skipLabel, /без встановлення/iu);
  assert.match(
    cssSource,
    /data-install-overview="true"[^}]+\.install-coach-step-label[^}]+position:\s*static/su,
  );
});

test("uses the native Android prompt only when a real prompt is available", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true });
  assert.equal(native.mode, "native");
  assert.equal(native.overview, true);
  assert.equal(native.steps.length, 3);
  assert.match(native.steps[0], /Встановити Vydno/i);
  assert.doesNotMatch(native.steps[0], /системн/i);
  assert.match(native.steps[1], /системному вікні|системне вікно/i);
  assert.match(native.steps[1], /Установити додаток.+Установити/iu);
  assert.match(native.steps[2], /іконк.+Vydno/i);
  assert.match(native.command, /3 дії/iu);
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
  assert.match(cssSource, /data-install-mode="native"[^}]+#installNudgePrimary::before/su);
});

test("native Android coach matches the Ukrainian Chrome dialog seen on device", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true, step: 1 });
  assert.equal(native.steps[1], "Chrome відкриє системне вікно «Установити додаток» із назвою «Vydno.Capital». Натисніть «Установити» й дочекайтеся завершення.");
  assert.match(native.steps.join(" "), /Установити додаток.+Vydno\.Capital.+Установити/iu);
  assert.equal(native.overview, true);
  assert.match(native.hints[1], /«Установити додаток»/u);
  assert.match(native.hints[1], /«Vydno\.Capital»/u);
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

test("uses Safari only for the iOS Telegram handoff while Android can use Chrome", () => {
  for (const locale of ["uk", "en"]) {
    const ios = coach.buildModel({ locale, platform: "ios", telegram: true });
    assert.equal(ios.mode, "handoff");
    assert.match(JSON.stringify(ios), /Safari/u);
    assert.doesNotMatch(JSON.stringify(ios), /Chrome/iu);
    assert.match(ios.primaryLabel, /Safari/u);
  }

  const android = coach.buildModel({ locale: "en", platform: "android", telegram: true });
  assert.equal(android.mode, "handoff");
  assert.match(JSON.stringify(android), /Chrome/u);
  assert.match(android.primaryLabel, /Chrome/u);
});

test("Telegram install handoff requests Safari on iOS and Chrome on Android", () => {
  const handoffUrl = "https://vydno.example/app/browser-login/short-lived-token/?install=1";
  for (const [platform, expectedBrowser] of [["ios", "safari"], ["android", "chrome"]]) {
    const calls = [];
    const assignments = [];
    const context = {
      window: {
        Telegram: { WebApp: { openLink(...args) { calls.push(args); } } },
        location: { assign(url) { assignments.push(url); } },
      },
    };
    const openInstallBrowserUrl = loadInlineFunction("openInstallBrowserUrl", context);

    openInstallBrowserUrl(handoffUrl, platform);

    assert.equal(calls.length, 1);
    assert.equal(calls[0].length, 2, platform + " must not use the generic openLink call");
    assert.equal(calls[0][0], handoffUrl);
    assert.deepEqual(Object.keys(calls[0][1]), ["try_browser"]);
    assert.equal(calls[0][1].try_browser, expectedBrowser);
    assert.deepEqual(assignments, []);
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
