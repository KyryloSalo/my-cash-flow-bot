// Run with: PLAYWRIGHT_MODULE=/path/to/playwright node --test install_coach_layout.browser.cjs
// Uses the shipped template fragment, stylesheet and renderer; no replacement UI or backend.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { before, after, test } = require("node:test");
const playwright = require(process.env.PLAYWRIGHT_MODULE || "playwright");

const staticDir = path.join(__dirname, "static", "miniapp");
const source = fs.readFileSync(path.join(__dirname, "templates", "miniapp", "index.html"), "utf8");
const start = source.indexOf('  <div class="activation-modal install-coach-modal"');
const end = source.indexOf('  <div class="activation-modal" id="pushPermissionModal"', start);
assert.ok(start >= 0 && end > start, "use the actual install modal from the product template");
const modal = source.slice(start, end)
  .replace(/{% static 'miniapp\/([^']+)' %}/g, "/static/$1")
  .replace(/{{ miniapp_asset_version }}/g, "layout-test");
const html = `<!doctype html><html lang="uk"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<link rel="stylesheet" href="/static/app.css"><script src="/static/install-coach.js"></script>
</head><body class="calm-fintech-v2">${modal}</body></html>`;
const artifacts = process.env.INSTALL_LAYOUT_ARTIFACTS || path.join(os.tmpdir(), "vydno-install-layout");
const viewports = [
  { width: 320, height: 568 },
  { width: 360, height: 640 },
  { width: 390, height: 844 },
  { width: 430, height: 932 },
  { width: 694, height: 306 },
  { width: 732, height: 740 },
  { width: 1440, height: 900 },
];
const flows = [
  ["native-idle", { platform: "android", browser: "chrome", canPrompt: true }, null],
  ["native-accepted", { platform: "android", browser: "chrome", nativeStatus: "accepted" }, "installing"],
  ["native-installed", { platform: "android", browser: "chrome", nativeStatus: "installed" }, "installed-icon"],
  ["ios-share", { platform: "ios", browser: "safari", step: 0 }, "share"],
  ["ios-home-action", { platform: "ios", browser: "safari", step: 1 }, "home-screen-action"],
  ["ios-confirm", { platform: "ios", browser: "safari", step: 2 }, "add-confirmation"],
  ["ios-icon", { platform: "ios", browser: "safari", step: 3 }, "installed-icon"],
  ["android-menu", { platform: "android", browser: "chrome", step: 0 }, "browser-menu"],
  ["android-home-action", { platform: "android", browser: "chrome", step: 1 }, "install-menu-action"],
  ["android-confirm", { platform: "android", browser: "chrome", step: 2 }, "native-prompt"],
  ["android-clamped-final", { platform: "android", browser: "chrome", step: 999 }, "native-prompt"],
  ["telegram-ios", { platform: "ios", inTelegram: true }, null],
  ["telegram-android", { platform: "android", inTelegram: true }, null],
  ["open-safari", { platform: "ios", browser: "chrome-ios" }, "safari-only-handoff"],
  ["desktop-installed", { platform: "desktop", browser: "chrome", nativeStatus: "installed" }, "installed-icon"],
];
let browser;
before(async () => {
  fs.mkdirSync(artifacts, { recursive: true });
  const engine = process.env.INSTALL_LAYOUT_ENGINE || "chromium";
  browser = await playwright[engine].launch({ headless: true });
  console.log(`Browser: ${engine} ${browser.version()}; artifacts: ${artifacts}`);
});
after(async () => { if (browser) await browser.close(); });

for (const viewport of viewports) {
  for (const locale of ["uk", "en"]) {
    for (const [name, options, stage] of flows) {
      const caseName = `${name}-${locale}-${viewport.width}x${viewport.height}`;
      test(caseName, async () => {
        const page = await browser.newPage({ viewport, reducedMotion: "reduce" });
        const errors = [];
        page.on("pageerror", error => errors.push(error.message));
        await page.route("http://install-coach.test/**", async route => {
          const url = new URL(route.request().url());
          if (url.pathname === "/") return route.fulfill({ contentType: "text/html", body: html });
          const filename = path.basename(url.pathname);
          if (!["app.css", "install-coach.js", "icon-192.png"].includes(filename)) return route.abort();
          return route.fulfill({ path: path.join(staticDir, filename) });
        });
        try {
          await page.goto("http://install-coach.test/");
          await page.evaluate(options => {
            const modal = document.getElementById("installNudgeModal");
            window.VydnoInstallCoach.apply(modal, options);
            modal.hidden = false;
          }, { ...options, locale });
          await page.locator(".install-coach-app-icon").evaluate(img => img.decode());
          // Capture before asserting so failing cases retain reproducible evidence.
          if ([390, 732, 1440].includes(viewport.width) && locale === "uk") {
            await page.screenshot({ path: path.join(artifacts, `${caseName}.png`) });
          }
          const geometry = await page.evaluate(() => {
            const rect = element => {
              const r = element.getBoundingClientRect();
              return { x: r.x, y: r.y, width: r.width, height: r.height, right: r.right, bottom: r.bottom };
            };
            const visible = element => {
              const style = getComputedStyle(element);
              return element.getClientRects().length > 0 && style.visibility !== "hidden" && Number(style.opacity) > 0.01;
            };
            const card = document.querySelector(".install-coach-card");
            const visual = document.getElementById("installCoachVisual");
            const phone = [...document.querySelectorAll(".install-coach-phone")].find(visible);
            const stages = [...document.querySelectorAll("[data-install-stage]")].filter(visible);
            const icon = stages.find(element => element.dataset.installStage === "installed-icon");
            const appIcon = icon && icon.querySelector("img");
            const shownArrow = [...document.querySelectorAll(".install-coach-arrow")].some(visible);
            const home = icon ? {
              appIcon: rect(appIcon),
              imageLoaded: appIcon.complete && appIcon.naturalWidth > 0,
              highlightedByOrangeAnimation: Boolean(appIcon.closest("[data-install-highlight]")),
              grid: icon.querySelector(".install-coach-home-grid") ? rect(icon.querySelector(".install-coach-home-grid")) : null,
              tiles: [...icon.querySelectorAll(".install-coach-home-tile")].filter(visible).map(rect),
              dock: icon.querySelector(".install-coach-home-dock") ? rect(icon.querySelector(".install-coach-home-dock")) : null,
              status: icon.querySelector(".install-coach-home-status") ? rect(icon.querySelector(".install-coach-home-status")) : null,
              browserbarVisible: [...phone.querySelectorAll(".install-coach-browser-bar")].some(visible),
              uncovered: [...[0.1, 0.5, 0.9]].every(x => [...[0.1, 0.5, 0.9]].every(y =>
                document.elementFromPoint(rect(appIcon).x + rect(appIcon).width * x,
                  rect(appIcon).y + rect(appIcon).height * y) === appIcon)),
            } : null;
            return {
              card: rect(card),
              cardScrollWidth: card.scrollWidth,
              cardClientWidth: card.clientWidth,
              title: rect(document.getElementById("installNudgeTitle")),
              heading: rect(document.querySelector(".install-coach-heading")),
              actions: rect(document.querySelector(".install-coach-actions")),
              before: getComputedStyle(document.getElementById("installNudgePrimary"), "::before").content,
              visualVisible: visible(visual),
              visual: rect(visual),
              phone: phone ? rect(phone) : null,
              icon: icon ? rect(icon) : null,
              shownArrow,
              home,
              stages: stages.map(element => ({ name: element.dataset.installStage,
                scrollWidth: element.scrollWidth, clientWidth: element.clientWidth, ...rect(element) })),
              circles: document.getElementById("installNudgeModal").dataset.installOverview !== "true"
                ? [...document.querySelectorAll(".install-coach-steps li")].map(rect) : [],
            };
          });
          assert.deepEqual(errors, [], "no browser JS errors");
          assert.ok(geometry.card.x >= 0 && geometry.card.right <= viewport.width + 1, "modal fits viewport width");
          assert.ok(geometry.card.y >= 0 && geometry.card.bottom <= viewport.height + 1, "modal fits viewport height");
          assert.ok(geometry.cardScrollWidth <= geometry.cardClientWidth + 1, "no hidden horizontal modal overflow");
          assert.ok(geometry.title.right <= geometry.card.right + 1, "localized title stays inside the card");
          for (const circle of geometry.circles) {
            assert.ok(Math.abs(circle.width - circle.height) <= 1, `step badge must be circular: ${JSON.stringify(circle)}`);
          }
          if (name === "native-idle") {
            assert.equal(geometry.visualVisible, false);
            assert.ok(["none", "normal", '""'].includes(geometry.before), "no arrow outside the CTA over the heading");
            assert.ok(geometry.actions.y - geometry.heading.bottom >= 16, "heading and CTA need breathing room");
          } else if (stage) {
            assert.deepEqual(geometry.stages.map(item => item.name), [stage], "only the requested illustration stage is visible");
            assert.ok(geometry.phone.x >= geometry.visual.x && geometry.phone.right <= geometry.visual.right, "phone fits illustration");
            for (const item of geometry.stages) {
              assert.ok(item.x >= geometry.phone.x && item.right <= geometry.phone.right + 1, `${item.name} fits phone width`);
              assert.ok(item.y >= geometry.phone.y && item.bottom <= geometry.phone.bottom + 1, `${item.name} fits phone height`);
              assert.ok(item.scrollWidth <= item.clientWidth + 1, `${item.name} has no clipped inner content`);
            }
          }
          if (geometry.icon) {
            assert.equal(geometry.shownArrow, false, "installed home screen has no decorative arrow covering the app");
            assert.equal(geometry.home.browserbarVisible, false, "home screen is not presented as a browser tab");
            assert.ok(geometry.home.grid && geometry.home.tiles.length >= 6, "home screen has a recognizable grid of app icons");
            assert.ok(geometry.home.dock && geometry.home.status, "phone has a separate bottom dock and status bar");
            assert.ok(geometry.home.status.bottom <= geometry.home.grid.y, "status bar does not overlap the app grid");
            assert.ok(geometry.home.grid.bottom <= geometry.home.dock.y, "dock is below, not over, the app grid");
            assert.ok(geometry.home.imageLoaded, "the real application icon loads");
            assert.equal(geometry.home.highlightedByOrangeAnimation, false, "no orange instructional highlight on the installed icon");
            assert.ok(geometry.home.appIcon.width >= 40 && geometry.home.appIcon.height >= 40, "Vydno logo is large enough to recognize");
            assert.ok(geometry.home.uncovered, "no overlay covers any sampled area of the Vydno logo");
            assert.ok(geometry.home.appIcon.y >= geometry.icon.y && geometry.home.appIcon.bottom <= geometry.icon.bottom, "entire Vydno icon fits the home screen");
            assert.ok(geometry.home.dock.bottom <= geometry.icon.bottom, "bottom dock is not clipped");
            const offset = geometry.home.appIcon.x + geometry.home.appIcon.width / 2 - (geometry.phone.x + geometry.phone.width / 2);
            assert.ok(Math.abs(offset) <= 1, `installed icon must be centered, not shifted by ${offset}px`);
          }
          // Short/landscape windows may scroll, but every real action must remain reachable.
          for (const id of ["installNudgePrimary", "installNudgeHelp", "installNudgeLater"]) {
            const button = page.locator(`#${id}`);
            if (await button.isVisible()) {
              await button.scrollIntoViewIfNeeded();
              const r = await button.boundingBox();
              assert.ok(r.width >= 44 && r.height >= 44, `${id} has a 44px touch target`);
              assert.ok(r.y >= 0 && r.y + r.height <= viewport.height + 1, `${id} is reachable by scrolling`);
            }
          }
          await page.locator("#installNudgeModal").evaluate(element => { element.hidden = true; });
          assert.equal(await page.locator("#installNudgePrimary").isVisible(), false, "hidden modal does not leak actions");
        } finally {
          await page.close();
        }
      });
    }
  }
}

for (const reducedMotion of ["reduce", "no-preference"]) {
  test(`native state transitions with motion ${reducedMotion}`, async () => {
    const page = await browser.newPage({ viewport: { width: 390, height: 844 }, reducedMotion });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    await page.route("http://install-coach.test/**", async route => {
      const url = new URL(route.request().url());
      if (url.pathname === "/") return route.fulfill({ contentType: "text/html", body: html });
      const filename = path.basename(url.pathname);
      if (!["app.css", "install-coach.js", "icon-192.png"].includes(filename)) return route.abort();
      return route.fulfill({ path: path.join(staticDir, filename) });
    });
    try {
      await page.goto("http://install-coach.test/");
      for (const [nativeStatus, expected] of [["idle", null], ["accepted", "installing"], ["installed", "installed-icon"]]) {
        await page.evaluate(nativeStatus => {
          const modal = document.getElementById("installNudgeModal");
          window.VydnoInstallCoach.apply(modal, {
            platform: "android", browser: "chrome", canPrompt: nativeStatus === "idle", nativeStatus, locale: "uk",
          });
          modal.hidden = false;
        }, nativeStatus);
        if (expected) {
          await page.waitForFunction(expected => {
            const active = document.querySelector(`.install-coach-scene--android [data-install-stage="${expected}"]`);
            return getComputedStyle(active).visibility === "visible" && Number(getComputedStyle(active).opacity) > 0.99;
          }, expected);
        }
        const visible = await page.locator("[data-install-stage]").evaluateAll(elements => elements.filter(element => {
          const style = getComputedStyle(element);
          return element.getClientRects().length && style.visibility === "visible" && Number(style.opacity) > 0.01;
        }).map(element => element.dataset.installStage));
        assert.deepEqual(visible, expected ? [expected] : [], "state changes never leak the previous illustration layer");
      }
      assert.deepEqual(errors, [], "no browser errors during animated transitions");
    } finally {
      await page.close();
    }
  });
}
