(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.VydnoPartnerOffer = api;
})(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  function normalize(value) {
    if (!value || typeof value !== "object" || !Number.isSafeInteger(value.trial_days) || value.trial_days < 1) return null;
    if (typeof value.code !== "string" || !/^[a-z0-9][a-z0-9_-]{2,63}$/.test(value.code)) return null;
    return { code: value.code, trial_days: value.trial_days, name: typeof value.name === "string" ? value.name : "" };
  }

  function copy(value, locale) {
    const offer = normalize(value);
    if (!offer) return "";
    const days = offer.trial_days;
    if (locale === "en") return "Your partner offer: " + days + (days === 1 ? " day" : " days") + " of free access.";
    const lastTwo = days % 100;
    const last = days % 10;
    const noun = lastTwo >= 11 && lastTwo <= 14 ? "днів" : last === 1 ? "день" : last >= 2 && last <= 4 ? "дні" : "днів";
    return "Ваша партнерська пропозиція: " + days + " " + noun + " безкоштовного доступу.";
  }

  function renderLanding(doc, value) {
    const offer = normalize(value);
    if (!offer) return false;
    const terms = copy(offer, "uk");
    const duration = offer.trial_days + " дн.";
    const replacements = {
      ".offer-note": terms + " Без оплати та прив’язки картки.",
      ".section-cta > span": terms,
      ".final-action > span": terms,
      '[data-funnel-cta="header"], [data-funnel-cta="pricing"], [data-funnel-cta="sticky-mobile"]': "Отримати " + duration + " безкоштовно",
      ".price-badge": duration + " партнерського доступу",
      ".price-value strong": "0",
      ".price-card > p": "Без оплати та прив’язки картки",
      ".price-card > small": "Без списань під час партнерського періоду. Платну підписку можна оформити окремо.",
      ".price-timeline li:nth-child(1) strong": "0 грн",
      ".price-timeline li:nth-child(1) small": "без прив’язки картки",
      ".price-timeline li:nth-child(2) > span": "Наступні " + duration,
      ".price-timeline li:nth-child(3) small": "лише після окремого підтвердження платної підписки",
      ".faq-list details:nth-child(3) summary": "Що відбудеться після партнерського періоду?",
      ".faq-list details:nth-child(3) p": "Безкоштовний доступ діє " + duration + " Після його завершення платну підписку можна оформити окремо. Без вашого підтвердження списань не буде."
    };
    Object.keys(replacements).forEach(function (selector) {
      doc.querySelectorAll(selector).forEach(function (node) { node.textContent = replacements[selector]; });
    });
    return true;
  }

  return Object.freeze({ normalize, copy, renderLanding });
});
