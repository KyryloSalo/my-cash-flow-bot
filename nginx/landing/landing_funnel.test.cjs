const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const source = fs.readFileSync(path.join(__dirname, "pwa-v2", "script.js"), "utf8");
const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
const oidcTarget = "https://vydno.capital/app/auth/telegram/start?next=/app/";

const flush = () => new Promise((resolve) => setImmediate(resolve));

test("landing view bootstraps a CSRF session and emits an allowlisted event", async () => {
  const calls = [];
  const document = {
    body: { classList: { add() {}, remove() {}, toggle() {} } },
    documentElement: { scrollHeight: 2000 },
    querySelector() {
      return null;
    },
    querySelectorAll(selector) {
      if (selector === ".site-header" || selector === "[data-funnel-cta]") {
        return [];
      }
      return [];
    },
  };
  const window = {
    innerWidth: 390,
    innerHeight: 844,
    scrollY: 0,
    addEventListener() {},
    requestAnimationFrame(callback) {
      setImmediate(callback);
      return 1;
    },
    matchMedia: () => ({ matches: true, addEventListener() {} }),
    location: {
      protocol: "https:",
      hostname: "vydno.capital",
      search: "?utm_source=store&utm_campaign=launch",
    },
    crypto: { randomUUID: () => "8c778eec-5448-4cca-9447-04088fea24e8" },
  };
  const fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (String(url).startsWith("/app/api/funnel/session")) {
      return {
        ok: true,
        json: async () => ({ csrf_token: "csrf-1", funnel_version: "pwa-v1" }),
      };
    }
    return { ok: true, json: async () => ({ ok: true }) };
  };
  const context = {
    URLSearchParams,
    console,
    document,
    fetch,
    setTimeout,
    window,
  };
  context.globalThis = context;

  vm.runInNewContext(source, context, { filename: "landing.js" });
  await flush();
  await flush();

  assert.equal(calls.length, 2);
  assert.match(String(calls[0].url), /^\/app\/api\/funnel\/session\?/);
  assert.equal(calls[0].options.credentials, "same-origin");
  const event = JSON.parse(calls[1].options.body);
  assert.equal(event.event_name, "landing_view");
  assert.equal(event.event_id, "8c778eec-5448-4cca-9447-04088fea24e8");
  assert.equal(event.funnel_version, "pwa-v1");
  assert.equal("url" in event, false);
  assert.equal("referrer" in event, false);
  assert.equal(calls[1].options.headers["X-CSRFToken"], "csrf-1");
});

test("CTA telemetry records placement without blocking Telegram navigation", async () => {
  const calls = [];
  const listeners = {};
  const cta = {
    dataset: { funnelCta: "hero" },
    addEventListener(name, callback) {
      listeners[name] = callback;
    },
  };
  const document = {
    body: { classList: { add() {}, remove() {}, toggle() {} } },
    documentElement: { scrollHeight: 2000 },
    querySelector() {
      return null;
    },
    querySelectorAll(selector) {
      if (selector === "[data-funnel-cta]") {
        return [cta];
      }
      return [];
    },
  };
  const window = {
    innerWidth: 390,
    innerHeight: 844,
    scrollY: 0,
    addEventListener() {},
    requestAnimationFrame(callback) {
      setImmediate(callback);
      return 1;
    },
    matchMedia: () => ({ matches: true, addEventListener() {} }),
    location: { protocol: "https:", hostname: "vydno.capital", search: "" },
    crypto: { randomUUID: () => `00000000-0000-4000-8000-${String(++sequence).padStart(12, "0")}` },
  };
  let sequence = 0;
  const fetch = async (url, options = {}) => {
    calls.push({ url, options });
    if (String(url).startsWith("/app/api/funnel/session")) {
      return {
        ok: true,
        json: async () => ({ csrf_token: "csrf-1", funnel_version: "pwa-v1" }),
      };
    }
    return { ok: true, json: async () => ({ ok: true }) };
  };
  const context = {
    URLSearchParams,
    console,
    document,
    fetch,
    setTimeout,
    window,
  };
  context.globalThis = context;

  vm.runInNewContext(source, context, { filename: "landing.js" });
  await flush();
  await flush();
  assert.equal(typeof listeners.click, "function");

  let prevented = false;
  listeners.click({ preventDefault: () => { prevented = true; } });
  await flush();
  await flush();

  assert.equal(prevented, false);
  assert.equal(calls.length, 3);
  const event = JSON.parse(calls[2].options.body);
  assert.equal(event.event_name, "landing_primary_cta_click");
  assert.equal(event.placement, "hero");
  assert.equal(calls[2].options.keepalive, true);
});

test("all primary Telegram CTAs expose stable placement identifiers", () => {
  const placements = [...html.matchAll(/data-funnel-cta="([a-z-]+)"/g)].map((match) => match[1]);
  assert.deepEqual(placements, ["header", "hero", "after-demo", "pricing", "final", "sticky-mobile"]);
  const conversionLinks = [...html.matchAll(/href="([^"]+)"/g)]
    .map((match) => match[1])
    .filter((href) => href.includes("/app/auth/telegram/start"));
  assert.equal(conversionLinks.length, 6);
  assert.deepEqual(new Set(conversionLinks), new Set([oidcTarget]));
});

test("production root promotes the approved PWA-first landing", () => {
  assert.doesNotMatch(html, /noindex|nofollow/);
  assert.match(html, /<link rel="canonical" href="https:\/\/vydno\.capital\/">/);
  assert.match(html, /href="\/pwa-v2\/product\.css"/);
  assert.match(html, /src="\/pwa-v2\/script\.js"/);
  assert.match(html, /src="\/pwa-v2\/assets\/product-ui\/overview\.webp"/);
  assert.match(html, /href="\/pwa-v2\/quiz\/index\.html"/);
  assert.doesNotMatch(html, /Telegram — точка входу|30 або 90 днів|Have perfect control/);

  const promotedFiles = [...html.matchAll(/(?:href|src|data-product-src)="(\/pwa-v2\/[^"#?]+)"/g)]
    .map((match) => match[1]);
  assert.ok(promotedFiles.length >= 18);
  for (const urlPath of promotedFiles) {
    const relativePath = urlPath.replace(/^\/pwa-v2\//, "");
    assert.equal(fs.existsSync(path.join(__dirname, "pwa-v2", relativePath)), true, urlPath);
  }
});
