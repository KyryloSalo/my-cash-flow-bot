const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const root = __dirname;
const landingHtml = fs.readFileSync(path.join(root, "index.html"), "utf8");
const landingScript = fs.readFileSync(path.join(root, "script.js"), "utf8");
const productCss = fs.readFileSync(path.join(root, "product.css"), "utf8");
const quizHtml = fs.readFileSync(path.join(root, "quiz", "index.html"), "utf8");
const quizScript = fs.readFileSync(path.join(root, "quiz", "script.js"), "utf8");
const quizEditorialCss = fs.readFileSync(path.join(root, "quiz", "editorial.css"), "utf8");
const oidcTarget = "https://vydno.capital/app/auth/telegram/start?next=/app/";

const localReferences = (html, base) => {
  const refs = [...html.matchAll(/(?:href|src)="([^"]+)"/g)].map((match) => match[1]);
  return refs
    .filter((ref) => !/^(?:https?:|#|mailto:|tel:|data:)/.test(ref))
    .map((ref) => path.resolve(base, ref.split("#", 1)[0]));
};

const relativeLuminance = (hex) => {
  const channels = hex.slice(1).match(/.{2}/g).map((channel) => Number.parseInt(channel, 16) / 255);
  const linear = channels.map((channel) => channel <= 0.04045
    ? channel / 12.92
    : ((channel + 0.055) / 1.055) ** 2.4);
  return (0.2126 * linear[0]) + (0.7152 * linear[1]) + (0.0722 * linear[2]);
};

const contrastRatio = (foreground, background) => {
  const values = [relativeLuminance(foreground), relativeLuminance(background)].sort((a, b) => b - a);
  return (values[0] + 0.05) / (values[1] + 0.05);
};

test("candidate keeps one verified OIDC conversion destination", () => {
  const links = [...`${landingHtml}\n${quizHtml}`.matchAll(/href="([^"]+)"/g)]
    .map((match) => match[1])
    .filter((href) => href.includes("/app/auth/telegram/start"));

  assert.equal(links.length, 8);
  assert.deepEqual(new Set(links), new Set([oidcTarget]));
});

test("landing and quiz use only resolvable local assets", () => {
  const refs = [
    ...localReferences(landingHtml, root),
    ...localReferences(quizHtml, path.join(root, "quiz")),
  ];

  assert.equal(refs.length > 0, true);
  refs.forEach((reference) => assert.equal(fs.existsSync(reference), true, reference));
  assert.equal(/(?:cdn|fonts\.googleapis|unpkg|jsdelivr)/i.test(`${landingHtml}\n${quizHtml}`), false);
});

test("quiz contract has ten named questions and no answer persistence", () => {
  const questionNumbers = [...quizHtml.matchAll(/data-question="(\d+)"/g)].map((match) => Number(match[1]));
  const radioNames = [...new Set([...quizHtml.matchAll(/type="radio" name="(q\d+)"/g)].map((match) => match[1]))];

  assert.deepEqual(questionNumbers, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.deepEqual(radioNames, ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"]);
  assert.equal(/localStorage|sessionStorage|indexedDB/.test(quizScript), false);
});

test("quiz keeps answers and derived financial profile out of telemetry", () => {
  assert.match(quizScript, /placement:\s*["`]quiz-result["`]/);
  assert.doesNotMatch(quizScript, /placement:\s*`quiz-result-\$\{/);
  assert.doesNotMatch(quizScript, /dataset\.resultType|data-result-type/);
});

test("quiz exposes a no-JavaScript conversion fallback", () => {
  const noScript = quizHtml.match(/<noscript>([\s\S]*?)<\/noscript>/)?.[1] || "";

  assert.match(noScript, /JavaScript/);
  assert.match(noScript, new RegExp(oidcTarget.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(noScript, /30 днів за 1 грн/);
  assert.match(noScript, /\.quiz-progress[^}]*display:\s*none/);
  assert.match(noScript, /\[data-quiz-form\][^}]*display:\s*none/);
  assert.match(noScript, /\[data-result\][^}]*display:\s*none/);
});

test("quiz moves focus to a visible result container", () => {
  assert.match(quizHtml, /<section class="result" data-result hidden tabindex="-1"/);
  assert.match(quizScript, /resultScreen\.focus\?\.\(\{ preventScroll: true \}\)/);
});

test("product tour uses ordinary pressed buttons instead of incomplete tabs", () => {
  assert.match(landingHtml, /class="tour-rail" role="group" aria-label="Екрани Vydno"/);
  assert.doesNotMatch(landingHtml, /role="tablist"|role="tab"/);
  assert.match(landingHtml, /class="tour-tab is-active"[^>]*aria-pressed="true"/);
  assert.match(landingScript, /setAttribute\("aria-pressed", String\(isActive\)\)/);
  assert.doesNotMatch(landingScript, /aria-selected/);
});

test("candidate emits only existing allowlisted presentation events", () => {
  const allowed = new Set([
    "landing_view",
    "landing_primary_cta_click",
    "product_demo_start",
    "product_demo_complete",
  ]);
  const scripts = `${landingScript}\n${quizScript}`;
  const emitted = [
    ...[...scripts.matchAll(/emitDemoEvent\(\s*["`]([^"`]+)["`]/g)].map((match) => match[1]),
    ...[...scripts.matchAll(/emitFunnelEvent\(\s*[^,]+,\s*["`]([^"`]+)["`]/g)].map((match) => match[1]),
  ];

  emitted.forEach((eventName) => assert.equal(allowed.has(eventName), true, eventName));
  assert.equal(landingScript.includes("product_demo_complete"), true);
  assert.equal(quizScript.includes("landing_primary_cta_click"), true);
});

test("polish motion is progressive, finite, and reduced-motion safe", () => {
  assert.match(landingHtml, /data-scroll-progress/);
  assert.match(landingScript, /IntersectionObserver/);
  assert.match(landingScript, /prefers-reduced-motion: reduce/);
  assert.match(productCss, /@media \(prefers-reduced-motion: reduce\)/);
  assert.match(productCss, /\.motion-ready \[data-reveal\]\.is-visible/);
  assert.match(productCss, /\.site-nav a\s*\{[^}]*min-width:\s*44px;[^}]*min-height:\s*44px;/s);
  assert.match(quizScript, /prefers-reduced-motion: reduce/);
  assert.match(quizScript, /behavior:\s*preferredScrollBehavior\(\)/g);
  assert.equal(/setInterval|requestAnimationFrame\([^)]*requestAnimationFrame/s.test(landingScript), false);
});

test("small muted copy meets WCAG AA contrast", () => {
  const quizMuted = quizEditorialCss.match(/--muted:\s*(#[0-9a-f]{6})/i)?.[1];
  const tourNumber = productCss.match(/\.tour-tab > span\s*\{[^}]*color:\s*(#[0-9a-f]{6})/s)?.[1];

  assert.ok(quizMuted);
  assert.ok(tourNumber);
  ["#ebe8df", "#f4f1e8", "#edf4dc", "#faf9f5"].forEach((background) => {
    assert.ok(contrastRatio(quizMuted, background) >= 4.5, `${quizMuted} on ${background}`);
  });
  assert.ok(contrastRatio(tourNumber, "#102f25") >= 4.5, `${tourNumber} on #102f25`);
  assert.doesNotMatch(landingHtml, /variant-marker|PWA-first candidate|product UI/);
});

test("customer-facing copy keeps the product voice and the brand mark", () => {
  assert.match(landingHtml, /Без щоденної бухгалтерії/);
  assert.match(landingHtml, /class="brand"/);
  assert.match(landingHtml, /assets\/brand-icon\.png/);
  assert.doesNotMatch(landingHtml, /Реальний інтерфейс|Контрольний preview|синтетичні дані|не намальований для лендінгу|Реальний UI|Так він виглядає у застосунку/);
  assert.doesNotMatch(landingScript, /Показано реальний інтерфейс|контрольних даних/);
});
