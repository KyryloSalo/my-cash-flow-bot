const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const coach = require("./static/miniapp/install-coach.js");
const templateSource = fs.readFileSync(path.join(__dirname, "templates", "miniapp", "index.html"), "utf8");

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

test("builds a four-step iOS coach with exact actions and an explicit completion", () => {
  const first = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 0 });
  assert.equal(first.mode, "manual");
  assert.equal(first.steps.length, 4);
  assert.equal(first.step, 0);
  assert.match(first.command, /Поділитися/);
  assert.match(first.hint, /панелі Safari/iu);
  assert.match(first.primaryLabel, /Далі/);

  const add = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 2 });
  assert.match(add.command, /Додати/);
  assert.match(add.command, /правому верхньому/iu);

  const last = coach.buildModel({ locale: "uk", platform: "ios", browser: "safari", step: 3 });
  assert.equal(last.step, 3);
  assert.match(last.primaryLabel, /Vydno|Готово/iu);
  assert.match(last.skipLabel, /без встановлення/iu);
});

test("uses the native Android prompt only when a real prompt is available", () => {
  const native = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: true });
  assert.equal(native.mode, "native");
  assert.equal(native.steps.length, 2);
  assert.match(native.steps[0], /Встановити Vydno/i);
  assert.doesNotMatch(native.steps[0], /системн/i);
  assert.match(native.steps[1], /системному вікні|системне вікно/i);
  assert.match(native.primaryLabel, /Встановити/);

  const manual = coach.buildModel({ locale: "uk", platform: "android", browser: "chrome", canPrompt: false });
  assert.equal(manual.mode, "manual");
  assert.equal(manual.steps.length, 4);
  assert.match(manual.steps[0], /правому верхньому/iu);
  assert.match(manual.steps[1], /Встановити додаток/iu);
  assert.match(manual.primaryLabel, /Далі/);
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
