"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const modulePath = path.join(__dirname, "partner-offer.js");
const Offer = fs.existsSync(modulePath) ? require(modulePath) : {};

test("51-day partner terms preserve the manually specified hash and exact days", function () {
  assert.equal(typeof Offer.normalize, "function");
  const offer = { code: "manual_hash_51", name: "Partner", trial_days: 51 };
  assert.deepEqual(Offer.normalize(offer), offer);
});

test("registration copy shows exactly 51 days in either locale, not the legacy 90", function () {
  assert.equal(typeof Offer.copy, "function");
  const offer = { code: "manual_hash_51", trial_days: 51 };
  assert.equal(Offer.copy(offer, "uk"), "Ваша партнерська пропозиція: 51 день безкоштовного доступу.");
  assert.equal(Offer.copy(offer, "en"), "Your partner offer: 51 days of free access.");
});

test("the real PWA registration shell renders server-owned partner terms before sign-in and in onboarding", function () {
  const template = fs.readFileSync(path.join(__dirname, "../../templates/miniapp/index.html"), "utf8");
  assert.ok(template.includes("miniapp/partner-offer.js"));
  assert.ok(template.includes('partner_offer|json_script:"registration-partner-offer"'));
  assert.ok(template.includes('id="registrationPartnerOffer"'));
  assert.ok(template.includes('id="onboardingPartnerOffer"'));
  assert.ok(template.includes("setRegistrationPartnerOffer(status.partner_offer)"));
});

test("day counts are not silently capped at 30, 90 or 365", function () {
  for (const days of [1, 2, 11, 21, 51, 366, 1000]) {
    const value = { code: "manual_hash_51", trial_days: days };
    assert.equal(Offer.normalize(value).trial_days, days);
    assert.ok(Offer.copy(value, "uk").includes(String(days)));
  }
  assert.equal(Offer.copy({ code: "manual_hash_51", trial_days: 11 }, "uk"), "Ваша партнерська пропозиція: 11 днів безкоштовного доступу.");
  assert.equal(Offer.copy({ code: "manual_hash_51", trial_days: 22 }, "uk"), "Ваша партнерська пропозиція: 22 дні безкоштовного доступу.");
});

test("missing, malformed and untrusted terms do not become an offer", function () {
  for (const value of [null, {}, { code: "bad!code", trial_days: 51 }, { code: "manual_hash_51", trial_days: 0 }, { code: "manual_hash_51", trial_days: -1 }, { code: "manual_hash_51", trial_days: 1.5 }, { code: "manual_hash_51", trial_days: "51" }]) {
    assert.equal(Offer.normalize(value), null);
    assert.equal(Offer.copy(value, "uk"), "");
  }
});

test("manual hashes use the exact backend syntax, including a three-character hash", function () {
  assert.equal(Offer.normalize({ code: "abc", trial_days: 51 }).code, "abc");
  assert.equal(Offer.normalize({ code: "AbC123", trial_days: 51 }), null);
  assert.equal(Offer.normalize({ code: "_ab123", trial_days: 51 }), null);
});

test("partner landing replaces paid-trial prompts without changing organic visitors", function () {
  const nodes = new Map();
  const doc = { querySelectorAll(selector) { if (!nodes.has(selector)) nodes.set(selector, [{ textContent: "original" }]); return nodes.get(selector); } };
  assert.equal(typeof Offer.renderLanding, "function");
  assert.equal(Offer.renderLanding(doc, null), false);
  assert.equal(nodes.size, 0);
  assert.equal(Offer.renderLanding(doc, { code: "manual_hash_51", trial_days: 51 }), true);
  assert.ok(nodes.get(".offer-note")[0].textContent.includes("51 день"));
  assert.equal(nodes.get(".price-value strong")[0].textContent, "0");
  assert.ok(nodes.get(".price-card > small")[0].textContent.includes("окремо"));
});

test("both production landing variants load the shared offer renderer", function () {
  const landing = path.resolve(__dirname, "../../../../nginx/landing");
  for (const relative of ["index.html", "pwa-v2/index.html"]) {
    assert.ok(fs.readFileSync(path.join(landing, relative), "utf8").includes('/static/miniapp/partner-offer.js'));
  }
  assert.ok(fs.readFileSync(path.join(landing, "pwa-v2/script.js"), "utf8").includes("renderLanding(document, bootstrap.partner_offer)"));
});
