(function () {
  "use strict";

  const root = document.body;
  const urls = {
    overview: root.dataset.achievementsOverviewUrl,
    preferences: root.dataset.achievementsPreferencesUrl,
    noExpenses: root.dataset.achievementsNoExpensesUrl,
    pinsUrl: root.dataset.achievementsPinsUrl,
    notificationClaim: root.dataset.achievementsNotificationClaimUrl,
    notificationAck: root.dataset.achievementsNotificationAckUrl,
  };
  if (!urls.overview) return;

  const state = {
    enabled: false,
    loading: false,
    profile: null,
    day: null,
    catalog: [],
    pins: [],
    category: "all",
  };
  const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");
  const elements = {
    widget: document.getElementById("achievementsOverviewWidget"),
    widgetOpen: document.getElementById("achievementsOverviewOpen"),
    widgetMascot: document.getElementById("achievementsOverviewMascot"),
    widgetStreak: document.getElementById("achievementsOverviewStreak"),
    widgetLatest: document.getElementById("achievementsOverviewLatest"),
    financeTab: document.getElementById("statusFinanceTab"),
    achievementsTab: document.getElementById("statusAchievementsTab"),
    financePane: document.getElementById("statusFinancePane"),
    achievementsPane: document.getElementById("achievementsStatusPane"),
    heroMascot: document.getElementById("achievementHeroMascot"),
    currentStreak: document.getElementById("achievementCurrentStreak"),
    bestStreak: document.getElementById("achievementBestStreak"),
    shields: document.getElementById("achievementShieldBalance"),
    lifetimeDays: document.getElementById("achievementLifetimeDays"),
    heroSummary: document.getElementById("achievementHeroSummary"),
    noExpenses: document.getElementById("achievementNoExpensesBtn"),
    chips: document.getElementById("achievementCategoryChips"),
    grid: document.getElementById("achievementGrid"),
    status: document.getElementById("achievementStatusMessage"),
    settingsCard: document.getElementById("achievementsSettingsCard"),
    motion: document.getElementById("achievementMotionSetting"),
    timezone: document.getElementById("achievementTimezoneSetting"),
    settingsSave: document.getElementById("achievementSettingsSave"),
    settingsStatus: document.getElementById("achievementSettingsStatus"),
    mascotModal: document.getElementById("achievementsMascotModal"),
    mascotStatus: document.getElementById("achievementMascotStatus"),
  };

  function csrfToken() {
    const match = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  function requestId() {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
    return `gam-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function deviceId() {
    const key = "vydno-gamification-device";
    let value = window.localStorage.getItem(key);
    if (!value) {
      value = requestId();
      window.localStorage.setItem(key, value);
    }
    return value;
  }

  async function api(url, options) {
    const config = Object.assign({ credentials: "same-origin" }, options || {});
    config.headers = Object.assign({ Accept: "application/json" }, config.headers || {});
    if (config.body && !config.headers["Content-Type"]) config.headers["Content-Type"] = "application/json";
    if (config.method && config.method !== "GET") {
      const token = csrfToken();
      if (token) config.headers["X-CSRFToken"] = token;
    }
    const response = await window.fetch(url, config);
    let payload = null;
    try {
      payload = await response.json();
    } catch (_error) {
      payload = null;
    }
    if (!response.ok || !payload || payload.ok === false) {
      const error = new Error(payload && payload.error && payload.error.message ? payload.error.message : "Не вдалося виконати запит.");
      error.status = response.status;
      error.code = payload && payload.error ? payload.error.code : "request_failed";
      throw error;
    }
    return payload;
  }

  function setMessage(element, message, tone) {
    if (!element) return;
    element.hidden = !message;
    element.textContent = message || "";
    element.dataset.tone = tone || "neutral";
  }

  function preferredAsset(item) {
    const motionAllowed = state.profile && state.profile.motion_enabled && !prefersReducedMotion.matches;
    if (item.earned && motionAllowed && item.asset_gif) return item.asset_gif;
    return item.earned ? item.asset_color : item.asset_locked;
  }

  function mascotPoster(mascot) {
    const safeMascot = mascot === "capi" ? "capi" : "bob";
    return `/static/miniapp/achievements/previews/${safeMascot}-motion-poster.png`;
  }

  function selectStatusTab(target) {
    const achievementsSelected = target === "achievements";
    if (elements.financeTab) {
      elements.financeTab.classList.toggle("is-active", !achievementsSelected);
      elements.financeTab.setAttribute("aria-selected", String(!achievementsSelected));
    }
    if (elements.achievementsTab) {
      elements.achievementsTab.classList.toggle("is-active", achievementsSelected);
      elements.achievementsTab.setAttribute("aria-selected", String(achievementsSelected));
    }
    if (elements.financePane) elements.financePane.hidden = achievementsSelected;
    if (elements.achievementsPane) elements.achievementsPane.hidden = !achievementsSelected;
  }

  function openAchievements() {
    const statusNav = document.querySelector('.nav-btn[data-target="status"]');
    if (statusNav) statusNav.click();
    selectStatusTab("achievements");
    if (elements.achievementsPane) elements.achievementsPane.scrollIntoView({ block: "start", behavior: prefersReducedMotion.matches ? "auto" : "smooth" });
  }

  function renderChips() {
    if (!elements.chips) return;
    const categories = ["all"].concat(Array.from(new Set(state.catalog.map((item) => item.category).filter(Boolean))));
    elements.chips.replaceChildren();
    const labels = {
      all: "Усі",
      onboarding: "Старт",
      habit: "Ритм",
      reporting: "Звіти",
      goals: "Цілі",
      family: "Сім'я",
      secret: "Секретні",
    };
    categories.forEach((category) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "achievement-category-chip";
      button.classList.toggle("is-active", state.category === category);
      button.textContent = labels[category] || category;
      button.setAttribute("aria-pressed", String(state.category === category));
      button.addEventListener("click", function () {
        state.category = category;
        renderChips();
        renderGrid();
      });
      elements.chips.appendChild(button);
    });
  }

  function makeCard(item) {
    const card = document.createElement("article");
    card.className = `achievement-card${item.earned ? " is-earned" : " is-locked"}`;
    card.dataset.category = item.category || "";

    const art = document.createElement("img");
    art.className = "achievement-card-art";
    art.src = preferredAsset(item);
    art.alt = "";
    art.width = 128;
    art.height = 128;
    art.loading = "lazy";

    const body = document.createElement("div");
    body.className = "achievement-card-body";
    const eyebrow = document.createElement("small");
    eyebrow.textContent = item.earned ? "Відкрито" : item.is_secret ? "Секретне" : "Попереду";
    const title = document.createElement("h3");
    title.textContent = item.name;
    const description = document.createElement("p");
    description.textContent = item.description || item.hint || "";
    const progress = document.createElement("div");
    progress.className = "achievement-card-progress";
    const current = Number(item.progress && item.progress.current) || 0;
    const thresholds = Array.isArray(item.thresholds) ? item.thresholds.filter(Number.isFinite) : [];
    const nextThreshold = thresholds.find((threshold) => threshold > current);
    progress.textContent = item.earned
      ? item.level ? `Рівень ${item.level}` : "Готово"
      : nextThreshold ? `${current} / ${nextThreshold}` : item.hint || "Продовжуйте";
    body.append(eyebrow, title, description, progress);

    card.append(art, body);
    if (item.earned) {
      const pin = document.createElement("button");
      const isPinned = state.pins.includes(item.key);
      pin.type = "button";
      pin.className = `achievement-pin${isPinned ? " is-active" : ""}`;
      pin.textContent = isPinned ? "Закріплено" : "Закріпити";
      pin.setAttribute("aria-pressed", String(isPinned));
      pin.addEventListener("click", function () { togglePin(item.key); });
      card.appendChild(pin);
    }
    return card;
  }

  function renderGrid() {
    if (!elements.grid) return;
    const items = state.category === "all"
      ? state.catalog
      : state.catalog.filter((item) => item.category === state.category);
    elements.grid.replaceChildren();
    items.forEach((item) => elements.grid.appendChild(makeCard(item)));
  }

  function renderOverview(data) {
    const earned = data.catalog.filter((item) => item.earned);
    const latest = earned.sort((a, b) => String(b.earned_at || "").localeCompare(String(a.earned_at || "")))[0];
    if (elements.widgetStreak) elements.widgetStreak.textContent = String(data.day.current_streak || 0);
    if (elements.widgetLatest) elements.widgetLatest.textContent = latest ? latest.name : "Відкрийте перше досягнення";
    if (elements.widgetMascot) {
      elements.widgetMascot.src = latest ? preferredAsset(latest) : mascotPoster(data.profile.mascot);
      elements.widgetMascot.alt = data.profile.mascot === "capi" ? "Капі" : "Боб";
    }
    if (elements.widget) elements.widget.hidden = false;
  }

  function renderHero() {
    if (!state.day || !state.profile) return;
    if (elements.heroMascot) elements.heroMascot.src = mascotPoster(state.profile.mascot);
    if (elements.currentStreak) elements.currentStreak.textContent = String(state.day.current_streak || 0);
    if (elements.bestStreak) elements.bestStreak.textContent = String(state.day.best_streak || 0);
    if (elements.shields) elements.shields.textContent = String(state.day.shield_balance || 0);
    if (elements.lifetimeDays) elements.lifetimeDays.textContent = String(state.day.lifetime_active_days || 0);
    if (elements.heroSummary) {
      elements.heroSummary.textContent = state.day.status === "active"
        ? "Сьогоднішній день уже підтримує вашу серію."
        : "Зробіть підтверджений запис або відмітьте день без витрат.";
    }
    if (elements.noExpenses) elements.noExpenses.disabled = state.day.status === "active";
  }

  function renderSettings() {
    if (!state.profile) return;
    document.querySelectorAll("[data-achievement-mascot]").forEach((button) => {
      const selected = button.dataset.achievementMascot === state.profile.mascot;
      button.classList.toggle("is-active", selected);
      button.setAttribute("aria-checked", String(selected));
    });
    if (elements.motion) elements.motion.checked = Boolean(state.profile.motion_enabled) && !prefersReducedMotion.matches;
    if (elements.timezone) elements.timezone.value = state.profile.pending_timezone || state.profile.timezone || "Europe/Kyiv";
  }

  function render(data) {
    state.enabled = true;
    state.profile = data.profile;
    state.day = data.day;
    state.catalog = data.catalog || [];
    state.pins = data.pins || [];
    root.classList.add("gamification-ready");
    renderOverview(data);
    renderHero();
    renderChips();
    renderGrid();
    renderSettings();
    if (data.profile.needs_mascot && elements.mascotModal) {
      elements.mascotModal.hidden = false;
      elements.mascotModal.querySelector("button")?.focus();
    }
  }

  function disableFeature() {
    state.enabled = false;
    root.classList.remove("gamification-ready");
    if (elements.widget) elements.widget.hidden = true;
    if (elements.achievementsTab) elements.achievementsTab.hidden = true;
    if (elements.achievementsPane) elements.achievementsPane.hidden = true;
    if (elements.settingsCard) elements.settingsCard.hidden = true;
    if (elements.mascotModal) elements.mascotModal.hidden = true;
    selectStatusTab("finance");
  }

  async function loadOverview() {
    if (state.loading) return;
    state.loading = true;
    try {
      render(await api(urls.overview));
      await claimNotification();
    } catch (error) {
      if (error.status === 404 || error.status === 401) disableFeature();
      else setMessage(elements.status, error.message, "error");
    } finally {
      state.loading = false;
    }
  }

  async function savePreferences(changes, statusElement) {
    try {
      const response = await api(urls.preferences, {
        method: "PATCH",
        body: JSON.stringify(changes),
      });
      state.profile = Object.assign({}, state.profile || {}, response.profile);
      renderSettings();
      setMessage(statusElement, "Збережено.", "success");
      return true;
    } catch (error) {
      setMessage(statusElement, error.message, "error");
      return false;
    }
  }

  async function togglePin(key) {
    let next = state.pins.includes(key) ? state.pins.filter((item) => item !== key) : state.pins.concat(key);
    if (next.length > 3) {
      setMessage(elements.status, "Можна закріпити до трьох досягнень.", "error");
      return;
    }
    try {
      const response = await api(urls.pinsUrl, {
        method: "PUT",
        body: JSON.stringify({ achievement_keys: next }),
      });
      state.pins = response.pins || [];
      renderGrid();
      setMessage(elements.status, "Закріплення оновлено.", "success");
    } catch (error) {
      setMessage(elements.status, error.message, "error");
    }
  }

  async function recordNoExpenses() {
    if (!state.day || !elements.noExpenses) return;
    elements.noExpenses.disabled = true;
    try {
      await api(urls.noExpenses, {
        method: "POST",
        headers: { "Idempotency-Key": requestId() },
        body: JSON.stringify({ day_id: String(state.day.day_id) }),
      });
      setMessage(elements.status, "День без витрат зараховано.", "success");
      await loadOverview();
    } catch (error) {
      elements.noExpenses.disabled = false;
      setMessage(elements.status, error.message, "error");
    }
  }

  function notificationDialog(grants, onClose) {
    const shell = document.createElement("div");
    shell.className = "achievement-notification";
    shell.setAttribute("role", "dialog");
    shell.setAttribute("aria-modal", "true");
    const card = document.createElement("section");
    card.className = "achievement-notification-card";
    const title = document.createElement("h2");
    title.textContent = grants.length > 1 ? "Нові досягнення" : "Нове досягнення";
    card.appendChild(title);
    grants.forEach((grant) => {
      const row = document.createElement("div");
      row.className = "achievement-notification-grant";
      const image = document.createElement("img");
      image.src = preferredAsset(Object.assign({ earned: true }, grant));
      image.alt = "";
      const copy = document.createElement("div");
      const name = document.createElement("strong");
      name.textContent = grant.name;
      const description = document.createElement("p");
      description.textContent = grant.description || grant.hint || "";
      copy.append(name, description);
      row.append(image, copy);
      card.appendChild(row);
    });
    const close = document.createElement("button");
    close.type = "button";
    close.className = "primary-action";
    close.textContent = "Готово";
    close.addEventListener("click", function () {
      shell.remove();
      onClose();
    });
    card.appendChild(close);
    shell.appendChild(card);
    document.body.appendChild(shell);
    close.focus();
  }

  async function claimNotification() {
    if (!state.enabled) return;
    try {
      const response = await api(urls.notificationClaim, {
        method: "POST",
        body: JSON.stringify({ device_id: deviceId() }),
      });
      const claim = response.notification;
      if (!claim || !Array.isArray(claim.grants) || !claim.grants.length) return;
      notificationDialog(claim.grants, async function () {
        try {
          await api(urls.notificationAck, {
            method: "POST",
            body: JSON.stringify({
              notification_id: claim.notification_id,
              claim_token: claim.claim_token,
              device_id: deviceId(),
            }),
          });
        } finally {
          await loadOverview();
        }
      });
    } catch (error) {
      if (error.status !== 404 && error.status !== 401) console.warn("Achievement notification unavailable", error.code);
    }
  }

  if (elements.financeTab) elements.financeTab.addEventListener("click", function () { selectStatusTab("finance"); });
  if (elements.achievementsTab) elements.achievementsTab.addEventListener("click", function () { selectStatusTab("achievements"); });
  if (elements.widgetOpen) elements.widgetOpen.addEventListener("click", openAchievements);
  if (elements.noExpenses) elements.noExpenses.addEventListener("click", recordNoExpenses);
  if (elements.settingsSave) {
    elements.settingsSave.addEventListener("click", function () {
      const selected = document.querySelector("[data-achievement-mascot].is-active");
      savePreferences({
        mascot: selected ? selected.dataset.achievementMascot : state.profile && state.profile.mascot,
        motion_enabled: Boolean(elements.motion && elements.motion.checked && !prefersReducedMotion.matches),
        timezone: elements.timezone ? elements.timezone.value.trim() : "",
      }, elements.settingsStatus);
    });
  }
  document.querySelectorAll("[data-achievement-mascot]").forEach((button) => {
    button.addEventListener("click", function () {
      document.querySelectorAll("[data-achievement-mascot]").forEach((item) => {
        const selected = item === button;
        item.classList.toggle("is-active", selected);
        item.setAttribute("aria-checked", String(selected));
      });
    });
  });
  document.querySelectorAll("[data-achievement-first-mascot]").forEach((button) => {
    button.addEventListener("click", async function () {
      const saved = await savePreferences({ mascot: button.dataset.achievementFirstMascot }, elements.mascotStatus);
      if (saved && elements.mascotModal) {
        elements.mascotModal.hidden = true;
        await loadOverview();
      }
    });
  });
  prefersReducedMotion.addEventListener?.("change", function () {
    renderOverview({ profile: state.profile || {}, day: state.day || {}, catalog: state.catalog || [] });
    renderGrid();
  });
  window.addEventListener("vydno:ready", loadOverview);
  window.addEventListener("vydno:session-reset", disableFeature);
  if (window.vydnoAppReady) loadOverview();
}());
