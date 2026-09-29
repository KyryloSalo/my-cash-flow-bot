(function (root, factory) {
  "use strict";

  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (!root || !root.document) return;

  root.VydnoSound = api;
  const script = root.document.currentScript;
  root.SoundManager = api.createSoundManager({
    window: root,
    document: root.document,
    storage: root.localStorage,
    navigator: root.navigator,
    telegramHaptics: root.Telegram && root.Telegram.WebApp && root.Telegram.WebApp.HapticFeedback,
    AudioClass: root.Audio,
    assetBase: script && script.dataset.soundBase || "/static/miniapp/sounds/",
    assetVersion: script && script.dataset.assetVersion || "",
    autoBind: true,
  });
})(typeof globalThis !== "undefined" ? globalThis : this, function () {
  "use strict";

  const STORAGE_KEYS = Object.freeze({
    enabled: "vydno.sound.enabled",
    achievementSoundsEnabled: "vydno.sound.achievements",
    hapticsEnabled: "vydno.haptics.enabled",
  });
  const DEFAULTS = Object.freeze({
    enabled: true,
    achievementSoundsEnabled: true,
    hapticsEnabled: true,
  });
  const SOUND_DEFINITIONS = Object.freeze({
    "tap-soft": Object.freeze({ file: "notify.mp3", volume: 0.70, haptic: "selection" }),
    "voice-start": Object.freeze({ file: "voice-start.mp3", volume: 0.45, haptic: "light" }),
    "voice-stop": Object.freeze({ file: "voice-stop.mp3", volume: 0.40, haptic: "light" }),
    "transaction-success": Object.freeze({ file: "success.mp3", volume: 0.55, haptic: "success" }),
    "achievement-unlocked": Object.freeze({ file: "achievement.mp3", volume: 0.65, haptic: "success", achievement: true }),
    "error-soft": Object.freeze({ file: "error-soft.mp3", volume: 0.45, haptic: "error" }),
  });
  const VIBRATION_PATTERNS = Object.freeze({
    selection: 8,
    light: 12,
    medium: 24,
    success: Object.freeze([18, 24, 28]),
    error: Object.freeze([35, 30, 35]),
  });

  function readBoolean(storage, key, fallback) {
    if (!storage || typeof storage.getItem !== "function") return fallback;
    try {
      const value = storage.getItem(key);
      if (value === "true") return true;
      if (value === "false") return false;
    } catch (_error) {
      return fallback;
    }
    return fallback;
  }

  function writeBoolean(storage, key, value) {
    if (!storage || typeof storage.setItem !== "function") return;
    try {
      storage.setItem(key, value ? "true" : "false");
    } catch (_error) {
      // Storage is optional; private browsing must not affect product flows.
    }
  }

  function createSoundManager(options) {
    const config = options || {};
    const documentRef = config.document || null;
    const storage = config.storage || null;
    const navigatorRef = config.navigator || null;
    const telegramHaptics = config.telegramHaptics || null;
    const AudioClass = config.AudioClass === undefined
      ? (typeof Audio === "function" ? Audio : null)
      : config.AudioClass;
    const now = typeof config.now === "function" ? config.now : Date.now;
    const cooldownMs = Number.isFinite(Number(config.cooldownMs)) ? Math.max(0, Number(config.cooldownMs)) : 180;
    const assetBase = String(config.assetBase || "/static/miniapp/sounds/").replace(/\/?$/, "/");
    const assetVersion = String(config.assetVersion || "").trim();
    const settings = {
      enabled: readBoolean(storage, STORAGE_KEYS.enabled, DEFAULTS.enabled),
      achievementSoundsEnabled: readBoolean(storage, STORAGE_KEYS.achievementSoundsEnabled, DEFAULTS.achievementSoundsEnabled),
      hapticsEnabled: readBoolean(storage, STORAGE_KEYS.hapticsEnabled, DEFAULTS.hapticsEnabled),
    };
    const audioPool = {};
    const lastPlayedAt = {};
    const consumedEventKeys = new Set();
    const eventKeyOrder = [];
    let ready = false;
    let unlockBound = false;

    function assetUrl(filename) {
      return assetBase + filename + (assetVersion ? "?v=" + encodeURIComponent(assetVersion) : "");
    }

    function getAudio(name) {
      const definition = SOUND_DEFINITIONS[name];
      if (!AudioClass || !definition) return null;
      if (!audioPool[name]) {
        try {
          audioPool[name] = new AudioClass(assetUrl(definition.file));
          audioPool[name].preload = "auto";
        } catch (_error) {
          audioPool[name] = null;
        }
      }
      return audioPool[name];
    }

    function rememberEventKey(eventKey) {
      const key = String(eventKey || "").trim();
      if (!key) return true;
      if (consumedEventKeys.has(key)) return false;
      consumedEventKeys.add(key);
      eventKeyOrder.push(key);
      if (eventKeyOrder.length > 256) consumedEventKeys.delete(eventKeyOrder.shift());
      return true;
    }

    function audioAllowed(definition, forceAudio) {
      if (forceAudio) return true;
      return definition.achievement ? settings.achievementSoundsEnabled : settings.enabled;
    }

    function triggerHaptic(intent) {
      if (!settings.hapticsEnabled || !intent) return false;
      try {
        if (telegramHaptics) {
          if ((intent === "success" || intent === "error") && typeof telegramHaptics.notificationOccurred === "function") {
            telegramHaptics.notificationOccurred(intent);
            return true;
          }
          if (intent === "selection" && typeof telegramHaptics.selectionChanged === "function") {
            telegramHaptics.selectionChanged();
            return true;
          }
          if (typeof telegramHaptics.impactOccurred === "function") {
            telegramHaptics.impactOccurred(intent === "medium" ? "medium" : "light");
            return true;
          }
        }
        if (navigatorRef && typeof navigatorRef.vibrate === "function") {
          return navigatorRef.vibrate(VIBRATION_PATTERNS[intent] || VIBRATION_PATTERNS.light) !== false;
        }
      } catch (_error) {
        return false;
      }
      return false;
    }

    function playAudio(name, definition) {
      const source = getAudio(name);
      if (!source || typeof source.play !== "function") return Promise.resolve(false);
      let audio;
      try {
        audio = typeof source.cloneNode === "function" ? source.cloneNode(true) : source;
        audio.currentTime = 0;
        audio.volume = definition.volume;
      } catch (_error) {
        return Promise.resolve(false);
      }
      try {
        return Promise.resolve(audio.play())
          .then(function () {
            ready = true;
            unbindUnlock();
            return true;
          })
          .catch(function () {
            ready = false;
            bindUnlock();
            return false;
          });
      } catch (_error) {
        ready = false;
        bindUnlock();
        return Promise.resolve(false);
      }
    }

    function play(name, playOptions) {
      const definition = SOUND_DEFINITIONS[name];
      const eventOptions = playOptions || {};
      if (!definition || (documentRef && documentRef.hidden)) return Promise.resolve(false);
      if (!rememberEventKey(eventOptions.eventKey)) return Promise.resolve(false);

      const timestamp = Number(now());
      if (timestamp - Number(lastPlayedAt[name] || 0) < cooldownMs) return Promise.resolve(false);
      lastPlayedAt[name] = timestamp;

      const hapticPlayed = eventOptions.skipHaptic ? false : triggerHaptic(definition.haptic);
      if (!audioAllowed(definition, Boolean(eventOptions.forceAudio))) return Promise.resolve(hapticPlayed);
      return playAudio(name, definition).then(function (audioPlayed) {
        return audioPlayed || hapticPlayed;
      });
    }

    function preload(names) {
      const loaded = [];
      const failed = [];
      (Array.isArray(names) ? names : Object.keys(SOUND_DEFINITIONS)).forEach(function (name) {
        const audio = getAudio(name);
        if (!audio) {
          failed.push(name);
          return;
        }
        try {
          audio.preload = "auto";
          if (typeof audio.load === "function") audio.load();
          loaded.push(name);
        } catch (_error) {
          delete audioPool[name];
          failed.push(name);
        }
      });
      return Promise.resolve({ loaded: loaded, failed: failed });
    }

    function preview() {
      lastPlayedAt["tap-soft"] = 0;
      return play("tap-soft", { forceAudio: true, skipHaptic: true });
    }

    function unlock() {
      const source = getAudio("tap-soft");
      if (!source || typeof source.play !== "function") return Promise.resolve(false);
      try {
        const audio = typeof source.cloneNode === "function" ? source.cloneNode(true) : source;
        audio.currentTime = 0;
        audio.volume = 0;
        return Promise.resolve(audio.play())
          .then(function () {
            ready = true;
            unbindUnlock();
            return true;
          })
          .catch(function () {
            ready = false;
            unbindUnlock();
            bindUnlock();
            return false;
          });
      } catch (_error) {
        ready = false;
        unbindUnlock();
        bindUnlock();
        return Promise.resolve(false);
      }
    }

    function buttonFromGesture(event) {
      let target = event && event.target;
      if (target && typeof target.closest === "function") {
        target = target.closest("button, [role='button'], a[href]");
      }
      if (!target || target.disabled || target.getAttribute && target.getAttribute("aria-disabled") === "true") return null;
      return target;
    }

    function handleButtonGesture(event) {
      if (!buttonFromGesture(event)) return;
      play("tap-soft");
    }

    function bindButtonFeedback() {
      if (!documentRef || typeof documentRef.addEventListener !== "function") return;
      documentRef.addEventListener("pointerdown", handleButtonGesture, { passive: true });
    }

    function unlockFromGesture(event) {
      if (buttonFromGesture(event)) {
        unbindUnlock();
        return;
      }
      unlockBound = false;
      unlock();
    }

    function bindUnlock() {
      if (!documentRef || unlockBound || typeof documentRef.addEventListener !== "function") return;
      unlockBound = true;
      ["pointerdown", "touchstart", "keydown"].forEach(function (eventName) {
        documentRef.addEventListener(eventName, unlockFromGesture, { once: true, passive: true });
      });
    }

    function unbindUnlock() {
      if (!documentRef || typeof documentRef.removeEventListener !== "function") return;
      unlockBound = false;
      ["pointerdown", "touchstart", "keydown"].forEach(function (eventName) {
        documentRef.removeEventListener(eventName, unlockFromGesture);
      });
    }

    function setEnabled(value) {
      settings.enabled = Boolean(value);
      writeBoolean(storage, STORAGE_KEYS.enabled, settings.enabled);
      syncControls();
      return settings.enabled;
    }

    function setAchievementSoundsEnabled(value) {
      settings.achievementSoundsEnabled = Boolean(value);
      writeBoolean(storage, STORAGE_KEYS.achievementSoundsEnabled, settings.achievementSoundsEnabled);
      syncControls();
      return settings.achievementSoundsEnabled;
    }

    function setHapticsEnabled(value) {
      settings.hapticsEnabled = Boolean(value);
      writeBoolean(storage, STORAGE_KEYS.hapticsEnabled, settings.hapticsEnabled);
      syncControls();
      return settings.hapticsEnabled;
    }

    function getSettings() {
      return {
        enabled: settings.enabled,
        achievementSoundsEnabled: settings.achievementSoundsEnabled,
        hapticsEnabled: settings.hapticsEnabled,
      };
    }

    function isReady() {
      return ready;
    }

    function setStatus(message, tone) {
      if (!documentRef || typeof documentRef.getElementById !== "function") return;
      const status = documentRef.getElementById("soundSettingsStatus");
      if (!status) return;
      status.hidden = !message;
      status.textContent = message || "";
      status.className = "form-status " + (tone || "muted");
    }

    function syncControls() {
      if (!documentRef || typeof documentRef.getElementById !== "function") return;
      const enabled = documentRef.getElementById("soundEnabledSetting");
      const achievements = documentRef.getElementById("achievementSoundsEnabledSetting");
      const haptics = documentRef.getElementById("hapticsEnabledSetting");
      if (enabled) enabled.checked = settings.enabled;
      if (achievements) achievements.checked = settings.achievementSoundsEnabled;
      if (haptics) haptics.checked = settings.hapticsEnabled;
    }

    function bindControls() {
      if (!documentRef || typeof documentRef.getElementById !== "function") return;
      const enabled = documentRef.getElementById("soundEnabledSetting");
      const achievements = documentRef.getElementById("achievementSoundsEnabledSetting");
      const haptics = documentRef.getElementById("hapticsEnabledSetting");
      const previewButton = documentRef.getElementById("soundPreviewButton");
      syncControls();
      if (enabled) enabled.addEventListener("change", function () {
        setEnabled(enabled.checked);
        setStatus("Збережено на цьому пристрої.", "success");
      });
      if (achievements) achievements.addEventListener("change", function () {
        setAchievementSoundsEnabled(achievements.checked);
        setStatus("Збережено на цьому пристрої.", "success");
      });
      if (haptics) haptics.addEventListener("change", function () {
        setHapticsEnabled(haptics.checked);
        setStatus("Збережено на цьому пристрої.", "success");
      });
      if (previewButton) previewButton.addEventListener("click", function () {
        preview().then(function (played) {
          setStatus(played ? "Тестовий звук відтворено." : "Торкніться екрана та спробуйте ще раз.", played ? "success" : "muted");
        });
      });
    }

    function handleAchievementCelebration(event) {
      const detail = event && event.detail || {};
      const notificationId = Number(detail.notificationId || detail.notification_id || 0);
      if (!Number.isSafeInteger(notificationId) || notificationId <= 0) return;
      play("achievement-unlocked", { eventKey: "achievement:" + notificationId });
    }

    if (documentRef && typeof documentRef.addEventListener === "function") {
      documentRef.addEventListener("vydno:achievement-celebration", handleAchievementCelebration);
    }
    if (config.autoBind !== false) {
      bindControls();
      bindButtonFeedback();
      bindUnlock();
    }

    return Object.freeze({
      play: play,
      preview: preview,
      preload: preload,
      unlock: unlock,
      setEnabled: setEnabled,
      setAchievementSoundsEnabled: setAchievementSoundsEnabled,
      setHapticsEnabled: setHapticsEnabled,
      getSettings: getSettings,
      isReady: isReady,
    });
  }

  return Object.freeze({
    STORAGE_KEYS: STORAGE_KEYS,
    SOUND_DEFINITIONS: SOUND_DEFINITIONS,
    createSoundManager: createSoundManager,
  });
});
