'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createSoundManager } = require('./sound-manager.js');

function makeStorage(seed = {}) {
  const values = new Map(Object.entries(seed));
  return {
    getItem(key) { return values.has(key) ? values.get(key) : null; },
    setItem(key, value) { values.set(key, String(value)); },
    snapshot() { return Object.fromEntries(values); },
  };
}

function makeDocument() {
  const listeners = new Map();
  return {
    hidden: false,
    addEventListener(type, callback) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(callback);
    },
    removeEventListener(type, callback) {
      if (listeners.has(type)) listeners.get(type).delete(callback);
    },
    getElementById() { return null; },
    fire(type, detail, target, extra = {}) {
      Array.from(listeners.get(type) || []).forEach((callback) => callback(Object.assign({ type, detail, target }, extra)));
    },
  };
}

function makeControl() {
  const listeners = new Map();
  return {
    checked: false,
    hidden: true,
    textContent: '',
    className: '',
    addEventListener(type, callback) { listeners.set(type, callback); },
    fire(type) {
      const callback = listeners.get(type);
      if (callback) callback({ preventDefault() {} });
    },
  };
}

class FakeAudio {
  static plays = [];
  static loads = [];
  static rejectPlayWith = null;
  static rejectLoadFor = new Set();

  constructor(src) {
    this.src = src;
    this.preload = '';
    this.volume = 1;
    this.currentTime = 0;
  }

  cloneNode() {
    const clone = new FakeAudio(this.src);
    clone.volume = this.volume;
    return clone;
  }

  load() {
    FakeAudio.loads.push(this.src);
    if (FakeAudio.rejectLoadFor.has(this.src)) throw new Error('load failed');
  }

  play() {
    FakeAudio.plays.push({ src: this.src, volume: this.volume });
    return FakeAudio.rejectPlayWith ? Promise.reject(FakeAudio.rejectPlayWith) : Promise.resolve();
  }
}

function manager(options = {}) {
  FakeAudio.plays = [];
  FakeAudio.loads = [];
  FakeAudio.rejectPlayWith = null;
  FakeAudio.rejectLoadFor = new Set();
  return createSoundManager(Object.assign({
    AudioClass: FakeAudio,
    document: makeDocument(),
    storage: makeStorage(),
    navigator: {},
    now: () => 1000,
    assetBase: '/static/miniapp/sounds/',
    autoBind: false,
  }, options));
}

test('defaults all three preferences on and persists exact local keys', () => {
  const storage = makeStorage();
  const sound = manager({ storage });
  assert.deepEqual(sound.getSettings(), { enabled: true, achievementSoundsEnabled: true, hapticsEnabled: true });
  sound.setEnabled(false);
  sound.setAchievementSoundsEnabled(false);
  sound.setHapticsEnabled(false);
  assert.deepEqual(storage.snapshot(), {
    'vydno.sound.enabled': 'false',
    'vydno.sound.achievements': 'false',
    'vydno.haptics.enabled': 'false',
  });
});

test('settings controls hydrate, persist independently, and preview through the public manager', async () => {
  const storage = makeStorage({
    'vydno.sound.enabled': 'false',
    'vydno.sound.achievements': 'true',
    'vydno.haptics.enabled': 'true',
  });
  const controls = {
    soundEnabledSetting: makeControl(),
    achievementSoundsEnabledSetting: makeControl(),
    hapticsEnabledSetting: makeControl(),
    soundPreviewButton: makeControl(),
    soundSettingsStatus: makeControl(),
  };
  const document = makeDocument();
  document.getElementById = (id) => controls[id] || null;
  const sound = manager({ document, storage, autoBind: true });

  assert.equal(controls.soundEnabledSetting.checked, false);
  assert.equal(controls.achievementSoundsEnabledSetting.checked, true);
  assert.equal(controls.hapticsEnabledSetting.checked, true);

  controls.soundEnabledSetting.checked = true;
  controls.soundEnabledSetting.fire('change');
  controls.achievementSoundsEnabledSetting.checked = false;
  controls.achievementSoundsEnabledSetting.fire('change');
  controls.hapticsEnabledSetting.checked = false;
  controls.hapticsEnabledSetting.fire('change');
  assert.deepEqual(sound.getSettings(), {
    enabled: true,
    achievementSoundsEnabled: false,
    hapticsEnabled: false,
  });
  assert.deepEqual(storage.snapshot(), {
    'vydno.sound.enabled': 'true',
    'vydno.sound.achievements': 'false',
    'vydno.haptics.enabled': 'false',
  });

  controls.soundPreviewButton.fire('click');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(FakeAudio.plays.at(-1).src, '/static/miniapp/sounds/notify.mp3');
  assert.equal(controls.soundSettingsStatus.hidden, false);
});

test('disabled UI audio is silent while achievement preference affects only achievement audio', async () => {
  const sound = manager();
  sound.setEnabled(false);
  assert.equal(await sound.play('transaction-success'), false);
  assert.equal(await sound.play('achievement-unlocked'), true);
  sound.setAchievementSoundsEnabled(false);
  assert.equal(await sound.play('achievement-unlocked', { eventKey: 'achievement:2' }), false);
  assert.equal(FakeAudio.plays.length, 1);
});

test('unknown sounds and unavailable storage never throw', async () => {
  const storage = { getItem() { throw new Error('blocked'); }, setItem() { throw new Error('blocked'); } };
  const sound = manager({ storage });
  assert.equal(await sound.play('missing'), false);
  assert.doesNotThrow(() => sound.setEnabled(false));
});

test('cooldown prevents rapid repeats without blocking different cues', async () => {
  let current = 1000;
  const sound = manager({ now: () => current, cooldownMs: 180 });
  assert.equal(await sound.play('voice-start'), true);
  current += 50;
  assert.equal(await sound.play('voice-start'), false);
  assert.equal(await sound.play('voice-stop'), true);
  current += 180;
  assert.equal(await sound.play('voice-start'), true);
  assert.equal(FakeAudio.plays.length, 3);
});

test('event keys deduplicate retries while distinct keys remain independent', async () => {
  let current = 1000;
  const sound = manager({ now: () => current, cooldownMs: 0 });
  assert.equal(await sound.play('transaction-success', { eventKey: 'transaction:draft-1' }), true);
  current += 1;
  assert.equal(await sound.play('transaction-success', { eventKey: 'transaction:draft-1' }), false);
  assert.equal(await sound.play('transaction-success', { eventKey: 'transaction:draft-2' }), true);
  assert.equal(FakeAudio.plays.length, 2);
});

test('failed preload is isolated and later playback still works', async () => {
  const sound = manager();
  FakeAudio.rejectLoadFor.add('/static/miniapp/sounds/voice-start.mp3');
  const result = await sound.preload(['voice-start', 'voice-stop', 'missing']);
  assert.deepEqual(result, { loaded: ['voice-stop'], failed: ['voice-start', 'missing'] });
  FakeAudio.rejectLoadFor.clear();
  assert.equal(await sound.play('voice-start'), true);
});

test('haptics stay enabled when audio is disabled and prefer Telegram over vibration', async () => {
  const calls = [];
  const telegramHaptics = {
    impactOccurred(value) { calls.push(['impact', value]); },
    notificationOccurred(value) { calls.push(['notification', value]); },
    selectionChanged() { calls.push(['selection']); },
  };
  const navigator = { vibrate() { calls.push(['vibrate']); return true; } };
  const sound = manager({ telegramHaptics, navigator });
  sound.setEnabled(false);
  assert.equal(await sound.play('voice-stop', { eventKey: 'voice:1:stop' }), true);
  assert.deepEqual(calls, [['impact', 'light']]);
  sound.setHapticsEnabled(false);
  assert.equal(await sound.play('error-soft', { eventKey: 'voice:1:error' }), false);
});

test('vibration fallback and missing haptic APIs are safe no-ops', async () => {
  const patterns = [];
  const fallback = manager({ navigator: { vibrate(pattern) { patterns.push(pattern); return true; } } });
  fallback.setEnabled(false);
  assert.equal(await fallback.play('transaction-success'), true);
  assert.deepEqual(patterns, [[18, 24, 28]]);
  const unavailable = manager({ AudioClass: null, navigator: {} });
  assert.equal(await unavailable.play('transaction-success'), false);
});

test('background tabs, missing audio support, and autoplay rejection fail silently', async () => {
  const document = makeDocument();
  document.hidden = true;
  assert.equal(await manager({ document }).play('transaction-success'), false);
  assert.equal(await manager({ AudioClass: null }).play('transaction-success'), false);
  const sound = manager();
  FakeAudio.rejectPlayWith = Object.assign(new Error('blocked'), { name: 'NotAllowedError' });
  await assert.doesNotReject(() => sound.play('transaction-success'));
  assert.equal(sound.isReady(), false);
});

test('unlock is inaudible and achievement event deduplicates by notification id', async () => {
  const document = makeDocument();
  const sound = manager({ document, now: (() => { let n = 1000; return () => n += 500; })() });
  await sound.unlock();
  assert.equal(FakeAudio.plays[0].volume, 0);
  document.fire('vydno:achievement-celebration', { notificationId: 42 });
  document.fire('vydno:achievement-celebration', { notificationId: 42 });
  document.fire('vydno:achievement-celebration', { notificationId: 43 });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(FakeAudio.plays.filter((entry) => entry.src.endsWith('/achievement.mp3')).length, 2);
  assert.equal(sound.isReady(), true);
});

test('the first pointer press on an enabled button plays audible tap feedback', async () => {
  const document = makeDocument();
  manager({ document, autoBind: true });
  const button = {
    disabled: false,
    dataset: {},
    getAttribute() { return null; },
    closest(selector) { return selector.includes('button') ? this : null; },
  };

  document.fire('pointerdown', null, button);
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(FakeAudio.plays.length, 1);
  assert.equal(FakeAudio.plays[0].src, '/static/miniapp/sounds/notify.mp3');
  assert.ok(FakeAudio.plays[0].volume > 0);
});

test('later button presses keep playing tap feedback', async () => {
  let current = 1000;
  const document = makeDocument();
  manager({ document, autoBind: true, now: () => current });
  const button = {
    disabled: false,
    dataset: {},
    getAttribute() { return null; },
    closest(selector) { return selector.includes('button') ? this : null; },
  };

  document.fire('pointerdown', null, button);
  await new Promise((resolve) => setImmediate(resolve));
  current += 500;
  document.fire('pointerdown', null, button);
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(FakeAudio.plays.filter((entry) => entry.volume > 0).length, 2);
});

test('button tap feedback is loud enough for a phone speaker', async () => {
  const document = makeDocument();
  manager({ document, autoBind: true });
  const button = {
    disabled: false,
    dataset: {},
    getAttribute() { return null; },
    closest(selector) { return selector.includes('button') ? this : null; },
  };

  document.fire('pointerdown', null, button);
  await new Promise((resolve) => setImmediate(resolve));

  assert.ok(FakeAudio.plays[0].volume >= 0.6);
});
