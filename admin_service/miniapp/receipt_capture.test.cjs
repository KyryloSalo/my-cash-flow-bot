const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const {test} = require('node:test');
const html = fs.readFileSync(path.join(__dirname, 'templates/miniapp/index.html'), 'utf8');

function source(name) {
  const start = html.indexOf(`      function ${name}(`);
  assert.notEqual(start, -1, `${name} exists in shipped PWA`);
  const end = html.indexOf('\n      function ', start + 1);
  return html.slice(start, end);
}
function context(names, extras) {
  const sandbox = vm.createContext(extras);
  vm.runInContext(names.map(source).join('\n'), sandbox);
  return sandbox;
}

test('photo entry offers native rear camera and independent gallery, without launching gallery implicitly', () => {
  const camera = html.match(/<input\b[^>]*id="aiImageCameraInput"[^>]*>/);
  assert.ok(camera, 'camera input exists');
  assert.match(camera[0], /capture="environment"/);
  assert.doesNotMatch(html.match(/<input\b[^>]*id="aiImageInput"[^>]*>/)[0], /capture=/);
  assert.doesNotMatch(source('setupAiIntakeChooser'), /image\.input\.click/);
});

test('camera and gallery share selection; cancel preserves it and replacement clears old findings', () => {
  const state = {file: null, busy: false};
  const findings = {innerHTML: 'old receipt'};
  const calls = [];
  const c = context(['isValidAiImageFile', 'selectAiImageFile'], {
    aiImageCaptureState: state, transactionState: {busy: false},
    aiImageElements: () => ({findings}),
    setAiIntakeMode: (mode) => calls.push(mode), syncAiImageFileName: () => calls.push('preview'),
    setAiImageStatus: (message) => calls.push(message), tr: (key) => key,
  });
  const camera = {name: 'camera.jpg', type: 'image/jpeg', size: 1024};
  c.selectAiImageFile({files: [camera]});
  assert.equal(state.file, camera);
  assert.equal(findings.innerHTML, '');
  c.selectAiImageFile({files: []});
  assert.equal(state.file, camera, 'cancelling picker retains current photo');
  c.selectAiImageFile({files: [{name: 'script.svg', type: 'image/svg+xml', size: 20}], value: 'x'});
  assert.equal(state.file, camera, 'invalid replacement retains prior valid selection');
  assert.ok(calls.includes('aiImageInvalid'));
  const heic = {name: 'IMG.HEIC', type: '', size: 20};
  c.selectAiImageFile({files: [heic]});
  assert.equal(state.file, heic, 'iOS HEIC with empty MIME accepted by extension');
  assert.equal(c.isValidAiImageFile({name: 'big.jpg', type: 'image/jpeg', size: 10485761}), false);
  assert.equal(c.isValidAiImageFile({name: 'empty.jpg', type: 'image/jpeg', size: 0}), false);
  state.busy = true;
  c.selectAiImageFile({files: [camera]});
  assert.equal(state.file, heic, 'cannot replace photo during request');
});

test('scanning follows pending request, locks both pickers and respects reduced motion', () => {
  const state = {busy: false};
  const classes = new Set();
  const el = {
    submit: {}, input: {}, cameraInput: {},
    review: {classList: {toggle: (name, on) => on ? classes.add(name) : classes.delete(name)},
      setAttribute: (name, value) => { el.review[name] = value; }},
  };
  const c = context(['setAiImageBusy'], {aiImageElements: () => el, aiImageCaptureState: state});
  c.setAiImageBusy(true);
  assert.equal(state.busy, true);
  assert.equal(el.cameraInput.disabled, true);
  assert.equal(el.input.disabled, true);
  assert.equal(el.review['aria-busy'], 'true');
  assert.ok(classes.has('is-scanning'));
  c.setAiImageBusy(false);
  assert.equal(state.busy, false);
  assert.equal(el.cameraInput.disabled, false);
  assert.equal(el.review['aria-busy'], 'false');
  assert.equal(classes.has('is-scanning'), false);
  const css = fs.readFileSync(path.join(__dirname, 'static/miniapp/app.css'), 'utf8');
  assert.match(css, /@media\s*\(prefers-reduced-motion:\s*reduce\)[\s\S]*?\.ai-image-scan-beam[\s\S]*?animation:\s*none/);
});

test('parse failure and success always stop scan, double submits do not start another request', async () => {
  for (const fail of [false, true]) {
    const state = {busy: false, file: {type: 'image/jpeg', name: 'receipt.jpg', size: 10}};
    let resolve, reject, requests = 0, prevented = 0;
    const pending = new Promise((yes, no) => { resolve = yes; reject = no; });
    const c = context(['parseAiImage'], {
      aiImageCaptureState: state, transactionState: {busy: false},
      aiImageElements: () => ({}), isValidAiImageFile: () => true,
      aiImageParseUrl: '/parse', ensureCsrfToken: () => 'csrf',
      window: {FormData: class {append() {}}},
      FormData: class {append() {}},
      ensureSession: () => Promise.resolve(),
      postFormData: () => { requests++; return pending; },
      setAiImageBusy: (busy) => { state.busy = busy; },
      setAiImageStatus: () => {}, tr: (key) => key, trf: (key) => key,
      renderAiImageFindings: () => {}, renderAiImageReview: () => false,
      transactionErrorMessage: () => 'Network error',
    });
    const event = {preventDefault: () => {prevented++;}};
    c.parseAiImage(event);
    assert.equal(state.busy, true);
    c.parseAiImage(event);
    await Promise.resolve();
    assert.equal(requests, 1);
    assert.equal(prevented, 2, 'even a blocked submit must prevent page navigation');
    if (fail) reject(new Error('offline')); else resolve({suggestion: {}});
    await new Promise(setImmediate);
    assert.equal(state.busy, false);
  }
});
