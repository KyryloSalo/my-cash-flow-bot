const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '../..');
const read = name => fs.readFileSync(path.join(root, name), 'utf8');
test('public trial copy has no hardcoded 90-day promise', () => {
  for (const file of ['nginx/landing/index.html', 'nginx/landing/pwa-v2/index.html', 'nginx/landing/pwa-v2/quiz/index.html', 'nginx/landing/offer.html', 'admin_service/miniapp/templates/miniapp/index.html', 'admin_service/miniapp/static/miniapp/course-bonus.js']) {
    assert.doesNotMatch(read(file), /90\s+(?:днів|days)/u, file);
  }
  assert.match(read('admin_service/miniapp/templates/miniapp/index.html'), /id="courseBonusTitle">30 днів безкоштовно/);
});
