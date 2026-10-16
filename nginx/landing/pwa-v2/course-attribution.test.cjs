const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const script = fs.readFileSync(path.join(__dirname, "course-attribution.js"), "utf8");

function visit(address, hrefs, storage) {
  const location = new URL(address);
  const links = hrefs.map(href => ({ href: new URL(href, location).href }));
  vm.runInNewContext(script, {
    URL, URLSearchParams,
    window: { location, localStorage: storage },
    document: { querySelectorAll: () => links },
  });
  return links.map(link => new URL(link.href));
}

const cta = "/app/auth/telegram/start?next=/app/";
const blocked = { getItem() { throw Error("blocked"); }, setItem() { throw Error("blocked"); } };

test("all root registration CTAs keep course and next without local storage", () => {
  const html = fs.readFileSync(path.join(__dirname, "..", "index.html"), "utf8");
  const hrefs = [...html.matchAll(/href="([^"]*\/app\/auth\/telegram\/start[^"]*)"/g)].map(m => m[1]);
  assert.equal(hrefs.length, 6);
  for (const url of visit("https://vydno.capital/?start=course", hrefs, blocked)) {
    assert.equal(url.searchParams.get("start"), "course");
    assert.equal(url.searchParams.get("next"), "/app/");
  }
});

test("course reaches quiz registration even when local storage is blocked", () => {
  const [quiz] = visit("https://vydno.capital/?start=course", ["/pwa-v2/quiz/index.html"], blocked);
  assert.equal(quiz.searchParams.get("start"), "course");
  const [registration] = visit(quiz.href, [cta], blocked);
  assert.equal(registration.searchParams.get("start"), "course");
});

test("normal visitors and unrelated outbound links are unchanged", () => {
  assert.equal(visit("https://vydno.capital/", [cta], blocked)[0].searchParams.has("start"), false);
  const [external] = visit("https://vydno.capital/?start=course", ["https://example.org/"], blocked);
  assert.equal(external.search, "");
});

test("stored course survives returning without query parameters", () => {
  const state = {};
  const storage = { getItem: key => state[key], setItem: (key, value) => { state[key] = value; }, removeItem: key => { delete state[key]; } };
  visit("https://vydno.capital/?start=course", [cta], storage);
  assert.equal(visit("https://vydno.capital/pwa-v2/quiz/index.html", [cta], storage)[0].searchParams.get("start"), "course");
});
