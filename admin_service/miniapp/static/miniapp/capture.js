(() => {
  "use strict";

  const API_ROOT = "/app/api";

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (character) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    })[character]);
  }

  function csrfToken() {
    const match = document.cookie.match(/(?:^|;\s*)csrftoken=([^;]+)/);
    return match ? decodeURIComponent(match[1]) : "";
  }

  async function api(path, options = {}) {
    const headers = {
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.method && options.method !== "GET" ? { "X-CSRFToken": csrfToken() } : {}),
      ...(options.headers || {}),
    };
    const response = await fetch(path, {
      credentials: "same-origin",
      ...options,
      headers,
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(data.error?.message || data.error?.code || "request_failed");
      error.code = data.error?.code || "request_failed";
      throw error;
    }
    return data;
  }

  function randomToken() {
    const bytes = new Uint8Array(32);
    crypto.getRandomValues(bytes);
    return btoa(String.fromCharCode(...bytes))
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");
  }

  function optionMarkup(items, labelKey) {
    return items.map((item) => `<option value="${Number(item.id)}">${escapeHtml(item[labelKey])}</option>`).join("");
  }

  function eventMarkup(event) {
    const actionable = ["pending", "needs_review", "reviewed"].includes(event.status);
    const amount = event.amount ? `${escapeHtml(event.amount)} ${escapeHtml(event.currency || "")}` : "Сума потребує перевірки";
    return `
      <article class="transaction-row" data-capture-event="${Number(event.id)}">
        <div>
          <strong>${amount}</strong>
          <p class="tx-meta">${escapeHtml(event.merchant || "Невідомий продавець")} · ${escapeHtml(event.status)}</p>
        </div>
        ${actionable ? `
          <div class="settings-action-row">
            <button class="secondary-action" type="button" data-capture-dismiss>Відхилити</button>
            <button class="primary-action" type="button" data-capture-review>Перевірити</button>
          </div>
          <div data-capture-editor hidden></div>
        ` : ""}
      </article>`;
  }

  async function renderEditor(container, event, options) {
    const accounts = options.accounts || [];
    const currencyAccounts = accounts.filter((account) => account.currency === event.currency);
    const categories = options.categories?.expense || [];
    const matchingAccount = currencyAccounts[0];
    if (!matchingAccount || !categories.length) {
      container.hidden = false;
      container.innerHTML = `<p class="form-error" role="alert">${!matchingAccount ? `Створіть рахунок у валюті ${escapeHtml(event.currency || "події")}.` : "Спочатку створіть категорію витрат."}</p>`;
      return;
    }
    container.hidden = false;
    container.innerHTML = `
      <div class="tx-form" tabindex="-1">
        <label class="form-field"><span>Сума</span><input data-capture-amount inputmode="decimal" value="${escapeHtml(event.amount || "")}" required></label>
        <label class="form-field"><span>Рахунок</span><select data-capture-account>${optionMarkup(currencyAccounts, "label")}</select></label>
        <label class="form-field"><span>Категорія</span><select data-capture-category>${optionMarkup(categories, "name")}</select></label>
        <label class="form-field"><span>Коментар</span><input data-capture-comment value="${escapeHtml(event.merchant || "")}"></label>
        <p class="form-error" data-capture-error role="alert" aria-live="polite" hidden></p>
        <button class="primary-action" type="button" data-capture-prepare>Підготувати до підтвердження</button>
      </div>`;
    container.querySelector("[data-capture-account]").value = String(matchingAccount.id);
    container.querySelector(".tx-form").focus();

    const prepareButton = container.querySelector("[data-capture-prepare]");
    prepareButton.onclick = async () => {
      const error = container.querySelector("[data-capture-error]");
      error.hidden = true;
      prepareButton.disabled = true;
      try {
        const review = await api(`${API_ROOT}/capture/v1/events/${event.id}/review`, {
          method: "POST",
          body: JSON.stringify({
            transaction: {
              kind: "expense",
              amount: container.querySelector("[data-capture-amount]").value,
              currency: event.currency,
              account_id: Number(container.querySelector("[data-capture-account]").value),
              category_id: Number(container.querySelector("[data-capture-category]").value),
              comment: container.querySelector("[data-capture-comment]").value,
            },
          }),
        });
        const summary = review.draft?.confirmation?.summary || [];
        const idempotencyKey = crypto.randomUUID ? crypto.randomUUID() : randomToken();
        container.innerHTML = `
          <div class="tx-form" tabindex="-1">
            <strong>Підтвердьте одну операцію</strong>
            <p class="tx-meta">${summary.map(escapeHtml).join(" · ")}</p>
            <p class="form-error" data-capture-error role="alert" aria-live="polite" hidden></p>
            <button class="primary-action" type="button" data-capture-confirm>Підтвердити операцію</button>
          </div>`;
        container.querySelector(".tx-form").focus();
        const confirmButton = container.querySelector("[data-capture-confirm]");
        confirmButton.onclick = async () => {
          const confirmError = container.querySelector("[data-capture-error]");
          confirmError.hidden = true;
          confirmButton.disabled = true;
          try {
            await api(`${API_ROOT}/capture/v1/events/${event.id}/confirm`, {
              method: "POST",
              body: JSON.stringify({
                source_revision: review.source_revision,
                idempotency_key: idempotencyKey,
              }),
            });
            await loadInbox();
          } catch (confirmFailure) {
            confirmError.textContent = confirmFailure.message;
            confirmError.hidden = false;
          } finally {
            confirmButton.disabled = false;
          }
        };
      } catch (reviewFailure) {
        error.textContent = reviewFailure.message;
        error.hidden = false;
      } finally {
        prepareButton.disabled = false;
      }
    };
  }

  let transactionOptions;

  async function loadInbox() {
    const inbox = document.getElementById("captureInbox");
    if (!inbox) return;
    let payload;
    try {
      payload = await api(`${API_ROOT}/capture/v1/inbox`);
    } catch (failure) {
      inbox.innerHTML = `<p class="form-error" role="alert">Не вдалося завантажити inbox: ${escapeHtml(failure.message)}</p><button class="secondary-action" type="button" data-capture-retry>Спробувати ще раз</button>`;
      inbox.querySelector("[data-capture-retry]").onclick = () => loadInbox();
      return;
    }
    const events = payload.events || [];
    inbox.innerHTML = events.length ? events.map(eventMarkup).join("") : '<p class="tx-meta">Нових банківських сповіщень немає.</p>';

    inbox.querySelectorAll("[data-capture-event]").forEach((row) => {
      const event = events.find((candidate) => String(candidate.id) === row.dataset.captureEvent);
      const dismissButton = row.querySelector("[data-capture-dismiss]");
      dismissButton?.addEventListener("click", async () => {
        dismissButton.disabled = true;
        try {
          await api(`${API_ROOT}/capture/v1/events/${event.id}/dismiss`, { method: "POST", body: "{}" });
          await loadInbox();
        } catch (failure) {
          dismissButton.disabled = false;
          inbox.insertAdjacentHTML("afterbegin", `<p class="form-error" role="alert">Не вдалося відхилити подію: ${escapeHtml(failure.message)}</p>`);
        }
      });
      const reviewButton = row.querySelector("[data-capture-review]");
      reviewButton?.addEventListener("click", async () => {
        reviewButton.disabled = true;
        try {
          transactionOptions ||= await api(`${API_ROOT}/transactions/options?kind=expense`);
          await renderEditor(row.querySelector("[data-capture-editor]"), event, transactionOptions);
        } catch (failure) {
          inbox.insertAdjacentHTML("afterbegin", `<p class="form-error" role="alert">Не вдалося відкрити подію: ${escapeHtml(failure.message)}</p>`);
        } finally {
          reviewButton.disabled = false;
        }
      });
    });
  }

  async function pairAndroid(statusElement) {
    const bridge = window.AndroidCapture;
    if (!bridge?.postMessage) {
      statusElement.textContent = "Відкрийте цей екран в Android-додатку Vydno.";
      return;
    }
    const clientProof = randomToken();
    const started = await api(`${API_ROOT}/capture/v1/pairing/start`, {
      method: "POST",
      body: JSON.stringify({ client_proof: clientProof }),
    });
    const approved = await api(`${API_ROOT}/capture/v1/pairing/approve`, {
      method: "POST",
      body: JSON.stringify({ challenge: started.challenge, client_proof: clientProof, platform: "android" }),
    });
    bridge.postMessage(`credential:${approved.credential}`);
    statusElement.textContent = "Запит на підключення надіслано Android-додатку. Дозвольте доступ до сповіщень.";
  }

  async function init() {
    const panel = document.getElementById("capturePanel");
    const statusElement = document.getElementById("captureStatus");
    const pairButton = document.getElementById("capturePair");
    const settingsButton = document.getElementById("captureNotificationSettings");
    if (!panel || !statusElement || !pairButton || !settingsButton) return;

    try {
      const rollout = await api(`${API_ROOT}/capture/v1/status`);
      if (!rollout.android) return;
      panel.hidden = false;
      const bridge = window.AndroidCapture;
      statusElement.textContent = bridge ? "Android pilot доступний для вашого акаунта." : "Inbox доступний. Підключення виконується лише в Android-додатку.";
      pairButton.hidden = !bridge?.postMessage;
      settingsButton.hidden = !bridge?.postMessage;
      pairButton.onclick = async () => {
        pairButton.disabled = true;
        try {
          await pairAndroid(statusElement);
        } catch (error) {
          statusElement.textContent = `Не вдалося підключити Android: ${error.message}`;
        } finally {
          pairButton.disabled = false;
        }
      };
      settingsButton.onclick = () => bridge.postMessage("open-notification-settings");
      await loadInbox();
    } catch (_error) {
      panel.hidden = true;
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();
