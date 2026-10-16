const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const bonus = require("./static/miniapp/course-bonus.js");

test("only exposes a verified course bonus", () => {
  assert.equal(bonus.normalize({}), null);
  assert.equal(bonus.normalize({ trial_days: 90, course_name: "Фінансова грамотність", platform: "askills.pro" }), null);
  assert.deepEqual(bonus.normalize({ course_bonus: { id: "consent-1", trial_days: 90, course_name: "Фінансова грамотність", platform: "askills.pro" } }), {
    id: "consent-1", trial_days: 90, course_name: "Фінансова грамотність", platform: "askills.pro"
  });
});

test("dismissal is scoped to user and consent and survives storage failure", () => {
  const memory = {};
  const storage = { getItem: (key) => memory[key] || null, setItem: (key, value) => { memory[key] = value; } };
  const first = bonus.createDismissalStore(storage, "user-1");
  assert.equal(first.isDismissed("consent-1"), false);
  first.dismiss("consent-1");
  assert.equal(first.isDismissed("consent-1"), true);
  assert.equal(bonus.createDismissalStore(storage, "user-2").isDismissed("consent-1"), false);
  const broken = bonus.createDismissalStore({ getItem() { throw new Error("blocked"); }, setItem() { throw new Error("blocked"); } }, "user-1");
  assert.doesNotThrow(() => broken.dismiss("consent-2"));
  assert.equal(broken.isDismissed("consent-2"), true);
  assert.equal(bonus.createDismissalStore(null, "user-1").isDismissed("consent-2"), true, "recreated controllers retain dismissal for the page session");
  assert.equal(bonus.createDismissalStore(null, "user-2").isDismissed("consent-2"), false, "fallback remains scoped to the user");
});

test("copy contains exact Ukrainian and English bonus meaning", () => {
  assert.equal(bonus.copy("uk"), 'Дякую, що обрали нас, ось вам 90 днів безкоштовного користування оскільки ви є студентом курсу "Фінансова грамотність" на платформі askills.pro');
  assert.match(bonus.copy("en"), /Thank you for choosing us![\s\S]*90 days[\s\S]*Financial Literacy[\s\S]*askills\.pro/u);
});

test("production controller shows the settings bonus, dismisses per real user, then continues to install", () => {
  const template = fs.readFileSync(path.join(__dirname, "templates/miniapp/index.html"), "utf8");
  const start = template.indexOf("let courseBonusDismissalStore = null;");
  const end = template.indexOf("function setCardBindingStatus", start);
  assert.ok(start > 0 && end > start);
  const values = {};
  const calls = [];
  const modal = {};
  const context = vm.createContext({
    window: { VydnoCourseBonus: bonus, localStorage: { getItem: key => values[key] || null, setItem: (key, value) => { values[key] = value; } } },
    document: { getElementById: id => id === "courseBonusModal" ? modal : null },
    settingsState: { data: null }, activeLocale: "uk", activationModalOpen: false,
    setText: (id, text) => calls.push([id, text]),
    openAccessibleModal: (element, close, focus) => calls.push(["open", focus]),
    closeAccessibleModal: () => calls.push(["close"]),
    maybeShowInstallNudge: () => calls.push(["install"]),
  });
  vm.runInContext(template.slice(start, end), context);
  const settings = { user: { tg_user_id: 101 }, access: { course_bonus: { id: "course-consent", trial_days: 90, course_name: "Фінансова грамотність", platform: "askills.pro" } } };
  assert.equal(context.showCourseBonusNudge({ user: settings.user, access: {} }), false);
  assert.equal(context.showCourseBonusNudge(settings), true);
  assert.equal(context.activationModalOpen, true);
  assert.deepEqual(calls.at(-1), ["open", "courseBonusPrimary"]);
  assert.equal(context.showCourseBonusNudge(settings), false, "do not overlap activation dialogs");
  context.dismissCourseBonus();
  assert.deepEqual(calls.at(-1), ["install"]);
  assert.equal(context.showCourseBonusNudge(settings), false, "dismissal persists after refresh");
  settings.user.tg_user_id = 102;
  assert.equal(context.showCourseBonusNudge(settings), true, "a different user must not inherit dismissal");
});

test("popup still opens and hands off to install when the browser denies localStorage access", () => {
  const template = fs.readFileSync(path.join(__dirname, "templates/miniapp/index.html"), "utf8");
  const start = template.indexOf("let courseBonusDismissalStore = null;");
  const end = template.indexOf("function setCardBindingStatus", start);
  const calls = [];
  const browser = { VydnoCourseBonus: bonus };
  Object.defineProperty(browser, "localStorage", { get() { throw new Error("SecurityError: storage denied"); } });
  const context = vm.createContext({
    window: browser,
    document: { getElementById: () => ({}) },
    settingsState: { data: null }, activeLocale: "uk", activationModalOpen: false,
    setText() {},
    openAccessibleModal: () => calls.push("open"),
    closeAccessibleModal: () => calls.push("close"),
    maybeShowInstallNudge: () => calls.push("install"),
  });
  vm.runInContext(template.slice(start, end), context);
  const settings = { user: { tg_user_id: 103 }, access: { course_bonus: { id: "blocked-storage-consent", trial_days: 90, course_name: "Фінансова грамотність", platform: "askills.pro" } } };
  assert.equal(context.showCourseBonusNudge(settings), true);
  context.dismissCourseBonus();
  assert.deepEqual(calls, ["open", "close", "install"]);
  assert.equal(context.showCourseBonusNudge(settings), false, "blocked storage must not repeat a dismissed bonus in the same page session");
});
