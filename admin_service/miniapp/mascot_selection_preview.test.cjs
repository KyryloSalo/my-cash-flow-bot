const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");

const root = __dirname;
const template = fs.readFileSync(path.join(root, "templates/miniapp/index.html"), "utf8");
const css = fs.readFileSync(path.join(root, "static/miniapp/achievements-ui.css"), "utf8");
const transparentAssets = ["bob", "capi"].map((mascot) =>
  path.join(root, `static/miniapp/achievements/assets/${mascot}/01-welcome-transparent.png`),
);

for (const mascot of ["bob", "capi"]) {
  assert.match(
    template,
    new RegExp(`achievements/assets/${mascot}/01-welcome-transparent\\.png`),
    `${mascot} selection card must use the transparent preview asset`,
  );
}
assert.match(
  css,
  /\.achievement-mascot-picker strong\s*\{[^}]*color:\s*#fff(?:fff)?\s*;/s,
  "Both mascot labels must be explicitly white",
);
assert.match(
  css,
  /\.achievement-mascot-picker img\s*\{[^}]*width:\s*144px;[^}]*height:\s*144px;/s,
  "Both mascot images must use the same square visual area",
);
for (const transparentAsset of transparentAssets) {
  assert.ok(fs.existsSync(transparentAsset), `Transparent asset must exist: ${transparentAsset}`);
  const png = fs.readFileSync(transparentAsset);
  assert.equal(png.toString("ascii", 1, 4), "PNG", "Mascot preview must be a PNG");
  assert.equal(png[25], 6, "Mascot preview PNG must use RGBA color type");
}

console.log("PASS: both mascots use normalized transparent assets, equal visual areas, and white labels.");
