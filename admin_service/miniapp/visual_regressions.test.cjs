const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const template = fs.readFileSync(path.join(__dirname, "templates", "miniapp", "index.html"), "utf8");
const css = fs.readFileSync(path.join(__dirname, "static", "miniapp", "app.css"), "utf8");

test("achievement timezone save action has explicit separation from the field", () => {
  assert.match(template, /class="[^"]*settings-save-action[^"]*" id="achievementSettingsSave"/);
  assert.match(css, /\.settings-save-action\s*\{[^}]*margin-top:\s*(?:12|13|14|15|16)px/s);
});

test("fixed bottom navigation reserves a comfortable content clearance", () => {
  assert.match(css, /\.app\s*\{[^}]*padding:[^;]*calc\(var\(--nav-height\) \+ (?:3[2-9]|[4-9]\d)px\)/s);
  assert.match(css, /\.calm-fintech-v2 \.app\s*\{[^}]*padding:[^;]*calc\(var\(--nav-height\) \+ (?:3[2-9]|[4-9]\d)px\)/s);
});

test("inactive Add navigation is not styled as the active destination", () => {
  const baseRule = css.match(/\.nav-btn--add\s*\{([^}]*)\}/s);
  assert.ok(baseRule);
  assert.doesNotMatch(baseRule[1], /background:\s*linear-gradient\([^;]*var\(--green-2\)/);
  assert.match(css, /\.nav-btn--add\.is-active\s*\{[^}]*background:\s*linear-gradient/s);
  const themedInactiveRule = css.match(/\.calm-fintech-v2 \.nav-btn--add\s*\{([^}]*)\}/s);
  assert.ok(themedInactiveRule, "the production theme must have its own inactive Add style");
  assert.match(themedInactiveRule[1], /background:\s*var\(--surface-soft\)/);
  assert.doesNotMatch(themedInactiveRule[1], /background:\s*var\(--forest\)/);
  assert.match(css, /\.calm-fintech-v2 \.nav-btn--add\.is-active\s*\{[^}]*background:\s*var\(--forest\)/s);
});

test("sound settings copy is localized instead of bilingual static labels", () => {
  assert.match(template, /id="soundSettingsHeading"/);
  assert.match(template, /id="soundEnabledSettingLabel"/);
  const soundCard = template.match(/<article class="card card-pad" id="soundSettingsCard">([\s\S]*?)<\/article>/);
  assert.ok(soundCard);
  assert.doesNotMatch(soundCard[1], /Звуки \/ Sounds|Interface sounds|Achievement sounds|Haptics|Прослухати \/ Preview/);
});
