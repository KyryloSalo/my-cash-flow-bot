(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.VydnoCourseBonus = api;
})(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const COPY = {
    uk: days => 'Дякую, що обрали нас, ось вам ' + days + ' днів безкоштовного користування оскільки ви є студентом курсу "Фінансова грамотність" на платформі askills.pro',
    en: days => "Thank you for choosing us! We are giving you " + days + " days of free use because you are a student of the Financial Literacy course on the askills.pro platform."
  };
  const PREFIX = "vydno.course-bonus.dismissed.v1.";
  const sessionDismissals = new Map();

  function normalize(access) {
    const value = access && access.course_bonus;
    if (!value || value.trial_days !== 90 || !value.id || value.course_name !== "Фінансова грамотність" || value.platform !== "askills.pro") return null;
    return { id: String(value.id), trial_days: 90, course_name: value.course_name, platform: value.platform };
  }

  function createDismissalStore(storage, userId) {
    const key = PREFIX + String(userId || "anonymous");
    if (!sessionDismissals.has(key)) sessionDismissals.set(key, new Set());
    const dismissed = sessionDismissals.get(key);
    function read() {
      try { return JSON.parse(storage.getItem(key) || "{}"); } catch (_) { return {}; }
    }
    return {
      isDismissed: function (consentId) { return Boolean(consentId && (dismissed.has(String(consentId)) || read()[String(consentId)])); },
      dismiss: function (consentId) {
        if (!consentId) return;
        dismissed.add(String(consentId));
        try { const state = read(); state[String(consentId)] = true; storage.setItem(key, JSON.stringify(state)); } catch (_) {}
      }
    };
  }

  return Object.freeze({ normalize, createDismissalStore, copy: function (locale, trialDays = 30) { return COPY[locale === "en" ? "en" : "uk"](trialDays); } });
});
