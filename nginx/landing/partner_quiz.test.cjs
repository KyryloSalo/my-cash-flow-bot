"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const Offer = require("../../admin_service/miniapp/static/miniapp/partner-offer.js");
const quizPath = path.join(__dirname, "pwa-v2/quiz");

function element() {
  return {
    textContent: "", innerHTML: "", hidden: false, style: {}, dataset: {},
    listeners: {}, children: [], classList: { toggle() {} },
    addEventListener(name, callback) { this.listeners[name] = callback; },
    setAttribute() {}, removeAttribute() {}, focus() {}, scrollIntoView() {},
    replaceChildren() { this.children = []; },
    appendChild(child) { this.children.push(child); },
    reset() {},
  };
}

function bootQuiz({ score = 0, family = 0 } = {}) {
  const nodes = new Map();
  const node = (selector) => {
    if (!nodes.has(selector)) nodes.set(selector, element());
    return nodes.get(selector);
  };
  node("[data-result]").hidden = true;
  const input = Object.assign(element(), {
    value: "answer", dataset: { score, family },
    closest: () => ({ querySelector: () => ({ textContent: "Actual answer" }) }),
  });
  const question = Object.assign(element(), {
    querySelector: (selector) => selector === "input[type=radio]:checked" ? input : element(),
    querySelectorAll: () => [input],
  });
  let resolveBootstrap;
  const response = new Promise((resolve) => { resolveBootstrap = resolve; });
  const requests = [];
  const document = {
    querySelector: node,
    querySelectorAll: (selector) => selector === "[data-question]" ? [question] : [],
    addEventListener() {}, createElement: element,
  };
  const context = {
    document, URLSearchParams, Date, Math,
    window: {
      location: { protocol: "https:", hostname: "vydno.capital", search: "" },
      VydnoPartnerOffer: Offer, matchMedia: () => ({ matches: true }),
    },
    fetch(url) {
      requests.push(url);
      return url.startsWith("/app/api/funnel/session") ? response : Promise.resolve({ ok: true });
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(quizPath, "script.js"), "utf8"), context);
  return {
    node, requests,
    finish: () => node("[data-next]").listeners.click(),
    restart: () => node("[data-restart]").listeners.click(),
    async bootstrap(offer) {
      resolveBootstrap({ ok: true, json: async () => ({ partner_offer: offer, csrf_token: "synthetic-only" }) });
      await new Promise((resolve) => setImmediate(resolve));
    },
  };
}

test("quiz loads the server-owned partner renderer before its application script", () => {
  const html = fs.readFileSync(path.join(quizPath, "index.html"), "utf8");
  const offerScript = html.indexOf('src="/static/miniapp/partner-offer.js"');
  assert.ok(offerScript >= 0, "quiz must load the shared partner renderer");
  assert.ok(offerScript < html.indexOf('src="script.js"'));
  assert.ok(html.includes("data-result-terms"));
});

for (const [type, score, family] of [["simple", 0, 0], ["guided", 8, 0], ["complex", 14, 0], ["family", 0, 4]]) {
  test(`${type} quiz result shows exactly 7889 free days without paid-trial promises`, async () => {
    const quiz = bootQuiz({ score, family });
    await quiz.bootstrap({ code: "custom_hash_7889", trial_days: 7889 });
    quiz.finish();
    assert.equal(quiz.node("[data-result]").hidden, false);
    assert.equal(quiz.node("[data-result-next]").textContent, Offer.copy({ code: "custom_hash_7889", trial_days: 7889 }, "uk"));
    assert.match(quiz.node("[data-result-cta]").textContent, /7889.*безкоштовно/);
    assert.match(quiz.node("[data-result-terms]").textContent, /Без оплати/);
    const visible = ["[data-result-next]", "[data-result-cta]", "[data-result-terms]"]
      .map((selector) => quiz.node(selector).textContent).join(" ");
    assert.doesNotMatch(visible, /1 грн|499 грн|30 днів|автопродовження/);
    quiz.restart();
    quiz.finish();
    assert.match(quiz.node("[data-result-cta]").textContent, /7889/);
  });
}

test("late funnel response updates an already-visible quiz result", async () => {
  const quiz = bootQuiz();
  quiz.finish();
  await quiz.bootstrap({ code: "custom_hash_7889", trial_days: 7889 });
  assert.match(quiz.node("[data-result-cta]").textContent, /7889.*безкоштовно/);
  assert.match(quiz.node("[data-result-next]").textContent, /7889 днів/);
});

test("missing or malformed partner terms leave the organic paid offer intact", async () => {
  for (const offer of [null, {}, { code: "custom_hash_7889", trial_days: "7889" }]) {
    const quiz = bootQuiz();
    await quiz.bootstrap(offer);
    quiz.finish();
    assert.match(quiz.node("[data-result-cta]").innerHTML, /1 грн/);
    assert.match(quiz.node("[data-result-next]").textContent, /30 днів.*1 грн/);
  }
});
