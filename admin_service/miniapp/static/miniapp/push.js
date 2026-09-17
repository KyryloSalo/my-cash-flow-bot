(function () {
  "use strict";

  const root = document.body;
  const urls = {
    status: root.dataset.notificationUrl,
    subscribe: root.dataset.notificationSubscribeUrl,
    unsubscribe: root.dataset.notificationUnsubscribeUrl,
    read: root.dataset.notificationReadUrl,
    preferences: root.dataset.notificationPreferencesUrl,
    test: root.dataset.notificationTestUrl
  };
  let statusPayload = null;
  let permissionMode = "request";
  let localEndpoint = null;
  let confirmedEndpoint = null;
  let routedLocation = null;

  function isEnglish() {
    return String(document.documentElement.lang || root.dataset.defaultLocale || "uk").toLowerCase().startsWith("en");
  }

  function copy(uk, en) { return isEnglish() ? en : uk; }
  function setText(id, value) { const node = document.getElementById(id); if (node) node.textContent = value; }
  function setStatus(id, value, kind) {
    const node = document.getElementById(id);
    if (!node) return;
    node.textContent = value || "";
    node.dataset.kind = kind || "";
    node.hidden = !value;
  }

  function jsonRequest(url, options) {
    return fetch(url, Object.assign({credentials: "same-origin"}, options || {})).then(function (response) {
      return response.json().catch(function () { return {}; }).then(function (payload) {
        if (!response.ok) throw payload;
        return payload;
      });
    });
  }

  function post(url, payload) {
    return jsonRequest(url, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(payload || {})});
  }

  function applicationServerKey(value) {
    const padding = "=".repeat((4 - value.length % 4) % 4);
    const raw = atob((value + padding).replace(/-/g, "+").replace(/_/g, "/"));
    return Uint8Array.from(Array.prototype.map.call(raw, function (char) { return char.charCodeAt(0); }));
  }

  function platformName() {
    const ua = navigator.userAgent || "";
    if (/iPhone|iPad|iPod/i.test(ua)) return "ios";
    if (/Android/i.test(ua)) return "android";
    return "desktop";
  }

  function standalone() {
    return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  }

  function sendBadge(count) {
    const value = Math.max(Number(count || 0), 0);
    const countNode = document.getElementById("notificationCount");
    if (countNode) {
      countNode.textContent = value > 99 ? "99+" : String(value);
      countNode.hidden = value === 0;
    }
    if (navigator.setAppBadge) {
      if (value) navigator.setAppBadge(value).catch(function () {});
      else if (navigator.clearAppBadge) navigator.clearAppBadge().catch(function () {});
    }
    if (navigator.serviceWorker && navigator.serviceWorker.controller) {
      navigator.serviceWorker.controller.postMessage({type: "SET_BADGE", count: value});
    }
  }

  function currentSubscription() {
    if (!("serviceWorker" in navigator)) return Promise.resolve(null);
    return navigator.serviceWorker.getRegistration().then(function (registration) {
      return registration && registration.pushManager ? registration.pushManager.getSubscription() : null;
    });
  }

  function notificationState() {
    if (!("Notification" in window) || !("serviceWorker" in navigator) || !("PushManager" in window)) return "unsupported";
    return Notification.permission;
  }

  function renderState() {
    const state = notificationState();
    const active = Boolean(localEndpoint && confirmedEndpoint === localEndpoint && statusPayload && Number(statusPayload.active_subscriptions || 0) > 0 && state === "granted");
    setText("notificationState", active ? copy("Підписку цього пристрою збережено", "This device subscription was registered") : state === "denied" ? copy("Заблоковані", "Blocked") : state === "unsupported" ? copy("Не підтримуються", "Unsupported") : localEndpoint ? copy("Потребує перевірки на цьому пристрої", "Needs verification on this device") : copy("Вимкнені на цьому пристрої", "Disabled on this device"));
    const enable = document.getElementById("notificationEnableBtn");
    const disable = document.getElementById("notificationDisableBtn");
    const test = document.getElementById("notificationTestBtn");
    const preferences = document.getElementById("notificationPreferences");
    if (enable) enable.textContent = localEndpoint ? copy("Перевірити / увімкнути", "Verify / enable") : copy("Увімкнути на цьому пристрої", "Enable on this device");
    if (enable) enable.hidden = active || state === "unsupported";
    if (disable) disable.hidden = !active;
    if (test) test.hidden = !active;
    if (preferences) preferences.hidden = !statusPayload;
    const values = statusPayload && statusPayload.preferences ? statusPayload.preferences : {};
    document.querySelectorAll("[data-push-pref]").forEach(function (input) {
      input.checked = Boolean(values[input.dataset.pushPref]);
    });
  }

  function formatWhen(value) {
    try { return new Intl.DateTimeFormat(isEnglish() ? "en" : "uk", {day: "numeric", month: "short", hour: "2-digit", minute: "2-digit"}).format(new Date(value)); }
    catch (_error) { return ""; }
  }

  function renderInbox() {
    const list = document.getElementById("notificationList");
    if (!list) return;
    const items = statusPayload && Array.isArray(statusPayload.notifications) ? statusPayload.notifications : [];
    if (!items.length) {
      list.innerHTML = '<div class="notification-empty"><strong>' + copy("Усе спокійно", "All quiet") + '</strong><p>' + copy("Нових повідомлень немає.", "There are no new notifications.") + '</p></div>';
      return;
    }
    list.innerHTML = "";
    items.forEach(function (item) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "notification-item" + (item.status === "unread" ? " is-unread" : "");
      button.innerHTML = '<span class="notification-item-dot"></span><span><strong></strong><span class="notification-item-body"></span><small></small></span>';
      button.querySelector("strong").textContent = item.title || "Vydno";
      button.querySelector(".notification-item-body").textContent = item.body || "";
      button.querySelector("small").textContent = formatWhen(item.created_at);
      button.addEventListener("click", function () {
        if (!window.vydnoAppReady || !window.vydnoNavigate) return;
        window.vydnoNavigate(item.target_url || "/app/").then(function (success) {
          if (!success) return null;
          closeDrawer();
          return post(urls.read, {notification_id: item.id}).then(function (payload) { sendBadge(payload.badge_count); return refresh(); });
        }).catch(function () {});
      });
      list.appendChild(button);
    });
  }

  function refresh() {
    if (!urls.status) return Promise.resolve(null);
    return currentSubscription().then(function (subscription) {
      localEndpoint = subscription && subscription.endpoint || null;
      const statusUrl = localEndpoint
        ? urls.status + (urls.status.includes("?") ? "&" : "?") + "endpoint=" + encodeURIComponent(localEndpoint)
        : urls.status;
      return jsonRequest(statusUrl);
    }).then(function (payload) {
      statusPayload = payload;
      if (payload.device_subscription_active === true) confirmedEndpoint = localEndpoint;
      else if (payload.device_subscription_active === false) confirmedEndpoint = null;
      else if (confirmedEndpoint !== localEndpoint) confirmedEndpoint = null;
      sendBadge(payload.badge_count);
      renderState();
      renderInbox();
      return payload;
    }).catch(function () { statusPayload = null; confirmedEndpoint = null; renderState(); return null; });
  }

  function showPermissionModal(mode) {
    permissionMode = mode || "request";
    const modal = document.getElementById("pushPermissionModal");
    if (!modal) return;
    if (permissionMode === "install") {
      setText("pushPermissionTitle", copy("Спочатку встановіть Vydno", "Install Vydno first"));
      setText("pushPermissionMessage", copy("На iPhone сповіщення працюють для Vydno, доданого на Домашній екран.", "On iPhone, notifications work for Vydno added to the Home Screen."));
      setText("pushPermissionPrimary", copy("Як встановити", "How to install"));
    } else if (permissionMode === "blocked") {
      setText("pushPermissionTitle", copy("Сповіщення заблоковані", "Notifications are blocked"));
      setText("pushPermissionMessage", copy("Увімкніть сповіщення для Vydno в системних налаштуваннях пристрою.", "Enable notifications for Vydno in your device settings."));
      setText("pushPermissionPrimary", copy("Зрозуміло", "Got it"));
    } else {
      setText("pushPermissionTitle", copy("Не пропускайте важливе", "Do not miss important updates"));
      setText("pushPermissionMessage", copy("Vydno може нагадувати про витрати, накопичення, борги та оплату. Без реклами.", "Vydno can remind you about expenses, savings, debts, and billing. No ads."));
      setText("pushPermissionPrimary", copy("Увімкнути сповіщення", "Enable notifications"));
    }
    if (window.vydnoModals) window.vydnoModals.open(modal, hidePermissionModal, "pushPermissionLater");
    else modal.hidden = false;
  }

  function hidePermissionModal() { const modal = document.getElementById("pushPermissionModal"); if (window.vydnoModals) window.vydnoModals.close(modal); else if (modal) modal.hidden = true; }

  function requestAndSubscribe() {
    if (platformName() === "ios" && !standalone()) { showPermissionModal("install"); return; }
    if (Notification.permission === "denied") { showPermissionModal("blocked"); return; }
    setStatus("pushPermissionStatus", copy("Очікуємо дозволу…", "Waiting for permission…"));
    return Notification.requestPermission().then(function (permission) {
      if (permission !== "granted") throw new Error("permission_denied");
      if (!statusPayload || !statusPayload.configured || !statusPayload.public_key) throw new Error("push_not_configured");
      return navigator.serviceWorker.ready;
    }).then(function (registration) {
      return registration.pushManager.subscribe({userVisibleOnly: true, applicationServerKey: applicationServerKey(statusPayload.public_key)});
    }).then(function (subscription) {
      const value = subscription.toJSON();
      return post(urls.subscribe, {endpoint: value.endpoint, keys: value.keys, platform: platformName(), device_label: platformName() === "ios" ? "iPhone / iPad" : platformName() === "android" ? "Android" : "Browser"}).then(function () {
        confirmedEndpoint = value.endpoint;
      });
    }).then(function () {
      hidePermissionModal();
      return refresh();
    }).then(function () {
      setStatus("notificationSettingsStatus", copy("Сповіщення увімкнено.", "Notifications enabled."), "success");
    }).catch(function (error) {
      const message = String(error && error.message || "");
      setStatus("pushPermissionStatus", message === "push_not_configured" ? copy("Push ще не активовано на сервері.", "Push is not activated on the server yet.") : copy("Не вдалося увімкнути сповіщення.", "Could not enable notifications."), "error");
    });
  }

  function disableOnDevice() {
    return currentSubscription().then(function (subscription) {
      if (!subscription) return null;
      return post(urls.unsubscribe, {endpoint: subscription.endpoint}).then(function () { return subscription.unsubscribe(); });
    }).then(function () { confirmedEndpoint = null; localEndpoint = null; return refresh(); }).catch(function () {
      setStatus("notificationSettingsStatus", copy("Не вдалося вимкнути підписку цього пристрою.", "Could not disable this device subscription."), "error");
    });
  }

  function savePreference(input) {
    const payload = {}; payload[input.dataset.pushPref] = Boolean(input.checked);
    post(urls.preferences, payload).then(function (result) {
      if (statusPayload) statusPayload.preferences = result.preferences;
      setStatus("notificationSettingsStatus", copy("Налаштування збережено.", "Settings saved."), "success");
    }).catch(function () { setStatus("notificationSettingsStatus", copy("Не вдалося зберегти.", "Could not save."), "error"); });
  }

  function openDrawer() {
    const drawer = document.getElementById("notificationDrawer");
    if (drawer) drawer.hidden = false;
    refresh();
  }

  function closeDrawer() { const drawer = document.getElementById("notificationDrawer"); if (drawer) drawer.hidden = true; }

  function handleDeepLink() {
    if (!window.vydnoAppReady || !window.vydnoNavigate || routedLocation === window.location.href) return Promise.resolve(false);
    const location = window.location.href;
    const route = new URL(location);
    const routeKeys = ["screen", "tab", "section", "notification_id"];
    const hasDeepLink = routeKeys.some(function (key) { return route.searchParams.has(key); });
    routedLocation = location;
    if (!hasDeepLink) return Promise.resolve(false);
    return window.vydnoNavigate(location).then(function (success) {
      if (!success) { routedLocation = null; return false; }
      const id = route.searchParams.get("notification_id");
      if (id && /^\d+$/.test(id)) return post(urls.read, {notification_id: Number(id)}).then(function (payload) { sendBadge(payload.badge_count); return true; });
      return true;
    }).catch(function () { routedLocation = null; return false; });
  }

  document.addEventListener("DOMContentLoaded", function () {
    const bell = document.getElementById("notificationBell");
    const close = document.getElementById("notificationDrawerClose");
    const backdrop = document.getElementById("notificationDrawerBackdrop");
    const enable = document.getElementById("notificationEnableBtn");
    const disable = document.getElementById("notificationDisableBtn");
    const test = document.getElementById("notificationTestBtn");
    const primary = document.getElementById("pushPermissionPrimary");
    const later = document.getElementById("pushPermissionLater");
    const readAll = document.getElementById("notificationReadAll");
    if (bell) bell.addEventListener("click", openDrawer);
    if (close) close.addEventListener("click", closeDrawer);
    if (backdrop) backdrop.addEventListener("click", closeDrawer);
    if (enable) enable.addEventListener("click", function () { showPermissionModal(notificationState() === "denied" ? "blocked" : "request"); });
    if (disable) disable.addEventListener("click", disableOnDevice);
    if (test) test.addEventListener("click", function () { post(urls.test, {}).then(function (payload) { setStatus("notificationSettingsStatus", payload.delivered ? copy("Тест надіслано.", "Test sent.") : copy("Підписка збережена, але сервер не доставив тест.", "Subscription saved, but the server did not deliver the test.")); return refresh(); }).catch(function () {}); });
    if (primary) primary.addEventListener("click", function () { if (permissionMode === "request") requestAndSubscribe(); else hidePermissionModal(); });
    if (later) later.addEventListener("click", hidePermissionModal);
    if (readAll) readAll.addEventListener("click", function () { post(urls.read, {all: true}).then(function (payload) { sendBadge(payload.badge_count); return refresh(); }); });
    document.querySelectorAll("[data-push-pref]").forEach(function (input) { input.addEventListener("change", function () { savePreference(input); }); });
    const ready = function () { refresh().then(handleDeepLink); };
    window.addEventListener("vydno:ready", ready);
    window.addEventListener("vydno:notifications", refresh);
    window.addEventListener("vydno:session-reset", function () { confirmedEndpoint = null; localEndpoint = null; statusPayload = null; renderState(); });
    if (window.vydnoAppReady) ready();
  });
})();
