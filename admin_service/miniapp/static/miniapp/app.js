(function () {
  const root = document.body;
  const authUrl = root.dataset.authUrl;
  const devAuthUrl = root.dataset.devAuthUrl;
  const profileUrl = root.dataset.profileUrl;
  const bootstrapUrl = root.dataset.bootstrapUrl;
  const flowUrl = root.dataset.flowUrl;
  const devAuthEnabled = root.dataset.devAuthEnabled === "true";
  const previewEnabled = root.dataset.previewEnabled === "true";
  const defaultPeriodLabel = root.dataset.defaultPeriodLabel || "Цей місяць";
  const previewRequested = new URLSearchParams(window.location.search).get("preview") === "1";
  const botTelegramUrl = "https://t.me/vydnocapital_bot";

  const PRESET_LABELS = {
    today: "Сьогодні",
    yesterday: "Вчора",
    last_7_days: "7 днів",
    current_month: "Цей місяць",
    last_30_days: "30 днів",
    custom: "Вибрати",
  };

  const PRESET_TO_API = {
    today: "today",
    yesterday: "yesterday",
    last_7_days: "last_7",
    current_month: "this_month",
    last_30_days: "last_30",
    custom: "custom",
  };

  const API_TO_PRESET = {
    today: "today",
    yesterday: "yesterday",
    last_7: "last_7_days",
    this_month: "current_month",
    last_30: "last_30_days",
    custom: "custom",
  };

  const MONTHS_SHORT = ["січ.", "лют.", "бер.", "кві.", "трав.", "чер.", "лип.", "сер.", "вер.", "жов.", "лис.", "гру."];
  const MONTHS_TICK = ["Січ", "Лют", "Бер", "Кві", "Тра", "Чер", "Лип", "Сер", "Вер", "Жов", "Лис", "Гру"];
  const PREVIEW_REFERENCE_DATE = new Date("2026-05-21T12:00:00");
  const PREVIEW_FX = { UAH: 1, USD: 41.8, USDT: 41.8, EUR: 45.1, TRY: 1.3 };

  const elements = {
    statusPanel: document.getElementById("statusPanel"),
    loadingPanel: document.getElementById("loadingPanel"),
    dashboardRoot: document.getElementById("dashboardRoot"),
    inlineAlert: document.getElementById("inlineAlert"),
    dateChip: document.getElementById("dateChip"),
    dateChipLabel: document.getElementById("dateChipLabel"),
    overviewTab: document.getElementById("overviewTab"),
    analyticsTab: document.getElementById("analyticsTab"),
    overviewView: document.getElementById("overviewView"),
    analyticsView: document.getElementById("analyticsView"),
    filterBackdrop: document.getElementById("filterBackdrop"),
    filterSheet: document.getElementById("filterSheet"),
    filterCloseButton: document.getElementById("filterCloseButton"),
    presetList: document.getElementById("presetList"),
    dateFromInput: document.getElementById("dateFromInput"),
    dateToInput: document.getElementById("dateToInput"),
    applyCustomRangeButton: document.getElementById("applyCustomRangeButton"),
    balanceValue: document.getElementById("balanceValue"),
    balanceUsd: document.getElementById("balanceUsd"),
    rateNote: document.getElementById("rateNote"),
    netFlowValue: document.getElementById("netFlowValue"),
    netFlowDelta: document.getElementById("netFlowDelta"),
    balanceSparkline: document.getElementById("balanceSparkline"),
    incomeValue: document.getElementById("incomeValue"),
    incomeDelta: document.getElementById("incomeDelta"),
    expenseValue: document.getElementById("expenseValue"),
    expenseDelta: document.getElementById("expenseDelta"),
    transactionsList: document.getElementById("transactionsList"),
    summaryPeriodLabel: document.getElementById("summaryPeriodLabel"),
    summaryIncomeValue: document.getElementById("summaryIncomeValue"),
    summaryExpenseValue: document.getElementById("summaryExpenseValue"),
    summaryNetValue: document.getElementById("summaryNetValue"),
    periodMicroChart: document.getElementById("periodMicroChart"),
    periodRangeStart: document.getElementById("periodRangeStart"),
    periodRangeEnd: document.getElementById("periodRangeEnd"),
    expenseStructureChip: document.getElementById("expenseStructureChip"),
    expenseDonutLayout: document.getElementById("expenseDonutLayout"),
    expenseDonutTotal: document.getElementById("expenseDonutTotal"),
    expenseDonutCurrency: document.getElementById("expenseDonutCurrency"),
    expenseCategoryList: document.getElementById("expenseCategoryList"),
    donutSegments: Array.from(document.querySelectorAll(".v2-donut-segment")),
    singleCategoryState: document.getElementById("singleCategoryState"),
    singleCategoryName: document.getElementById("singleCategoryName"),
    singleCategoryAmount: document.getElementById("singleCategoryAmount"),
    singleCategoryPercent: document.getElementById("singleCategoryPercent"),
    singleCategoryBar: document.getElementById("singleCategoryBar"),
    expenseStructureEmpty: document.getElementById("expenseStructureEmpty"),
    accountsTotalValue: document.getElementById("accountsTotalValue"),
    accountsSubValue: document.getElementById("accountsSubValue"),
    accountsList: document.getElementById("accountsList"),
    creditDebtValue: document.getElementById("creditDebtValue"),
    creditCardsCount: document.getElementById("creditCardsCount"),
    debtProgressBar: document.getElementById("debtProgressBar"),
    debtProgressLabel: document.getElementById("debtProgressLabel"),
    goalTitle: document.getElementById("goalTitle"),
    goalPercentValue: document.getElementById("goalPercentValue"),
    goalCaption: document.getElementById("goalCaption"),
    goalProgressBar: document.getElementById("goalProgressBar"),
    investmentValue: document.getElementById("investmentValue"),
    investmentNote: document.getElementById("investmentNote"),
    analyticsChip: document.getElementById("analyticsChip"),
    monthChart: document.getElementById("monthChart"),
  };

  const PREVIEW_SOURCE = {
    accounts: [
      { id: 1, label: "Основний", typeLabel: "Основний • UAH", currency: "UAH", balance: 149480.64 },
      { id: 2, label: "Заощадження", typeLabel: "Заощадження • USDT", currency: "USDT", balance: 3500 },
      { id: 3, label: "Подорожі", typeLabel: "Основний • TRY", currency: "TRY", balance: 1500 },
      { id: 4, label: "USD wallet", typeLabel: "Основний • USD", currency: "USD", balance: 1200 },
      { id: 5, label: "Готівка", typeLabel: "Готівка • UAH", currency: "UAH", balance: 4677 },
    ],
    creditCards: [{ id: 101, label: "Mono Credit", currency: "UAH", debt: 2500, creditLimit: 4800, monthlyRate: 3.1 }],
    debts: { owedToUser: 6800, owedByUser: 1200 },
    goals: [{ label: "Подушка безпеки", currentAmount: 62000, targetAmount: 100000, progressPercent: 62 }],
    investments: { currentValue: 0, items: [], note: "Інвест-рахунків поки немає." },
    monthlySeries: [
      { month: "2026-01-01", income: 42500, expense: 34200 },
      { month: "2026-02-01", income: 67900, expense: 39100 },
      { month: "2026-03-01", income: 63200, expense: 38400 },
      { month: "2026-04-01", income: 54000, expense: 41770 },
      { month: "2026-05-01", income: 58430, expense: 45746 },
    ],
    monthlyCategoryTotals: {
      "2026-04": { "Продукти": 14820, "Транспорт": 9318, "Житло": 6950, "Розваги": 4783, "Інше": 5899 },
      "2026-05": { "Продукти": 14200, "Транспорт": 8200, "Житло": 7300, "Розваги": 5500, "Інше": 10546 },
    },
    transactions: [
      { id: 1, date: "2026-05-21", flowKind: "normal", type: "income", title: "Зарплата", subtitle: "21.05.2026 • Основний", category: "Зарплата", amount: 58000, currency: "UAH" },
      { id: 2, date: "2026-05-20", flowKind: "transfer", type: "transfer", title: "Переказ", subtitle: "20.05.2026 • Готівка", category: "Переказ", amount: -3200, currency: "USD" },
      { id: 3, date: "2026-05-20", flowKind: "normal", type: "income", title: "Фриланс", subtitle: "20.05.2026 • Основний", category: "Фриланс", amount: 430, currency: "UAH" },
      { id: 4, date: "2026-05-19", flowKind: "normal", type: "expense", title: "Netflix", subtitle: "19.05.2026 • Підписки", category: "Інше", amount: -219, currency: "UAH" },
      { id: 5, date: "2026-05-19", flowKind: "normal", type: "expense", title: "АТБ Маркет", subtitle: "19.05.2026 • Продукти", category: "Продукти", amount: -736, currency: "UAH" },
      { id: 6, date: "2026-05-18", flowKind: "normal", type: "expense", title: "Таксі Bolt", subtitle: "18.05.2026 • Транспорт", category: "Транспорт", amount: -128, currency: "UAH" },
      { id: 7, date: "2026-05-17", flowKind: "normal", type: "expense", title: "Оренда", subtitle: "17.05.2026 • Житло", category: "Житло", amount: -7300, currency: "UAH" },
      { id: 8, date: "2026-05-16", flowKind: "normal", type: "expense", title: "Сільпо", subtitle: "16.05.2026 • Продукти", category: "Продукти", amount: -4600, currency: "UAH" },
      { id: 9, date: "2026-05-15", flowKind: "normal", type: "expense", title: "Пальне", subtitle: "15.05.2026 • Транспорт", category: "Транспорт", amount: -4200, currency: "UAH" },
      { id: 10, date: "2026-05-14", flowKind: "normal", type: "expense", title: "Кіно", subtitle: "14.05.2026 • Розваги", category: "Розваги", amount: -1500, currency: "UAH" },
      { id: 11, date: "2026-05-13", flowKind: "normal", type: "expense", title: "Вечеря з друзями", subtitle: "13.05.2026 • Розваги", category: "Розваги", amount: -4000, currency: "UAH" },
      { id: 12, date: "2026-05-12", flowKind: "normal", type: "expense", title: "Аптека", subtitle: "12.05.2026 • Інше", category: "Інше", amount: -2500, currency: "UAH" },
      { id: 13, date: "2026-05-09", flowKind: "normal", type: "expense", title: "Одяг", subtitle: "09.05.2026 • Інше", category: "Інше", amount: -1800, currency: "UAH" },
      { id: 14, date: "2026-05-08", flowKind: "normal", type: "expense", title: "Metro Cash", subtitle: "08.05.2026 • Продукти", category: "Продукти", amount: -6200, currency: "UAH" },
      { id: 15, date: "2026-05-07", flowKind: "normal", type: "expense", title: "Таксі та метро", subtitle: "07.05.2026 • Транспорт", category: "Транспорт", amount: -3872, currency: "UAH" },
      { id: 16, date: "2026-05-06", flowKind: "normal", type: "expense", title: "Подарунки", subtitle: "06.05.2026 • Інше", category: "Інше", amount: -6027, currency: "UAH" },
      { id: 17, date: "2026-05-05", flowKind: "normal", type: "expense", title: "АТБ великий чек", subtitle: "05.05.2026 • Продукти", category: "Продукти", amount: -2664, currency: "UAH" },
      { id: 18, date: "2026-04-30", flowKind: "normal", type: "income", title: "Фриланс", subtitle: "30.04.2026 • Основний", category: "Фриланс", amount: 12000, currency: "UAH" },
      { id: 19, date: "2026-04-28", flowKind: "normal", type: "expense", title: "Продукти тижня", subtitle: "28.04.2026 • Продукти", category: "Продукти", amount: -4900, currency: "UAH" },
      { id: 20, date: "2026-04-27", flowKind: "normal", type: "expense", title: "Таксі", subtitle: "27.04.2026 • Транспорт", category: "Транспорт", amount: -2200, currency: "UAH" },
    ],
  };

  const state = {
    mode: previewEnabled && previewRequested ? "preview" : "live",
    view: "overview",
    period: { preset: "current_month" },
    access: null,
    presentation: null,
    dashboardShown: false,
  };

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function asNumber(value) {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function moneyValue(payload) {
    if (!payload) return 0;
    if (typeof payload === "number") return payload;
    if (typeof payload.value !== "undefined" && payload.value !== null) return asNumber(payload.value);
    return asNumber(payload);
  }

  function pad(value) {
    return String(value).padStart(2, "0");
  }

  function toDate(value) {
    if (value instanceof Date) return new Date(value.getFullYear(), value.getMonth(), value.getDate());
    return new Date(`${value}T12:00:00`);
  }

  function toIsoDate(date) {
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  }

  function formatDateFull(dateLike) {
    const date = toDate(dateLike);
    return `${pad(date.getDate())}.${pad(date.getMonth() + 1)}.${date.getFullYear()}`;
  }

  function formatShortRange(dateLike) {
    const date = toDate(dateLike);
    return `${pad(date.getDate())}.${pad(date.getMonth() + 1)}`;
  }

  function formatAxisDate(dateLike) {
    const date = toDate(dateLike);
    return `${date.getDate()} ${MONTHS_SHORT[date.getMonth()]}`;
  }

  function formatMonthKey(dateLike) {
    const date = toDate(dateLike);
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}`;
  }

  function monthTickLabel(dateLike) {
    return MONTHS_TICK[toDate(dateLike).getMonth()];
  }

  function startOfMonth(dateLike) {
    const date = toDate(dateLike);
    return new Date(date.getFullYear(), date.getMonth(), 1);
  }

  function endOfMonth(dateLike) {
    const date = toDate(dateLike);
    return new Date(date.getFullYear(), date.getMonth() + 1, 0);
  }

  function shiftDays(dateLike, amount) {
    const date = toDate(dateLike);
    date.setDate(date.getDate() + amount);
    return date;
  }

  function shiftMonths(dateLike, amount) {
    const date = toDate(dateLike);
    return new Date(date.getFullYear(), date.getMonth() + amount, 1);
  }

  function eachDay(start, end) {
    const days = [];
    let current = toDate(start);
    const finish = toDate(end);
    while (current <= finish) {
      days.push(new Date(current.getFullYear(), current.getMonth(), current.getDate()));
      current = shiftDays(current, 1);
    }
    return days;
  }

  function presetRange(preset, referenceDate) {
    const reference = toDate(referenceDate);
    if (preset === "today") return { start: reference, end: reference };
    if (preset === "yesterday") {
      const day = shiftDays(reference, -1);
      return { start: day, end: day };
    }
    if (preset === "last_7_days") return { start: shiftDays(reference, -6), end: reference };
    if (preset === "last_30_days") return { start: shiftDays(reference, -29), end: reference };
    return { start: startOfMonth(reference), end: reference };
  }

  function toApiPreset(uiPreset) {
    return PRESET_TO_API[uiPreset] || "this_month";
  }

  function toUiPreset(apiPreset) {
    return API_TO_PRESET[apiPreset] || "current_month";
  }

  function formatMoney(amount, currency, options) {
    const settings = options || {};
    const rounded = Math.round((amount + Number.EPSILON) * 100) / 100;
    const absolute = Math.abs(rounded);
    const hasDecimals = Math.round(absolute * 100) % 100 !== 0;
    const fixed = hasDecimals ? absolute.toFixed(2) : absolute.toFixed(0);
    const parts = fixed.split(".");
    const whole = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, " ");
    const fraction = parts[1] ? `,${parts[1]}` : "";
    const sign = rounded < 0 ? "-" : settings.forcePlus && rounded > 0 ? "+" : "";
    const symbol = currency === "UAH" ? "грн" : currency === "USD" ? "$" : currency === "EUR" ? "€" : currency === "TRY" ? "₺" : currency;
    return `${sign}${whole}${fraction} ${symbol}`.trim();
  }

  function formatPercent(value, options) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "Без порівняння";
    const digits = options && typeof options.digits === "number" ? options.digits : 1;
    const number = Number(value);
    const sign = number > 0 ? "+" : "";
    const text = number.toFixed(digits).replace(".", ",").replace(/,0$/, "");
    return `${sign}${text}%`;
  }

  function toneClass(value) {
    if (Number(value) > 0) return "is-positive";
    if (Number(value) < 0) return "is-negative";
    return "";
  }

  function splitMoneyDisplay(display) {
    const text = String(display || "").trim();
    if (!text || text === "-") {
      return { amount: "-", currency: "" };
    }
    const parts = text.split(/\s+/);
    if (parts.length === 1) {
      return { amount: text, currency: "" };
    }
    return {
      amount: parts.slice(0, -1).join(" "),
      currency: parts[parts.length - 1],
    };
  }

  function currencyGlyph(codeOrDisplay) {
    const text = String(codeOrDisplay || "");
    if (text.includes("$") || /\bUSD\b/i.test(text)) return "$";
    if (text.includes("€") || /\bEUR\b/i.test(text)) return "€";
    if (text.includes("₺") || /\bTRY\b/i.test(text)) return "₺";
    if (/\bUSDT\b/i.test(text)) return "₮";
    if (text.includes("грн") || /\bUAH\b/i.test(text)) return "₴";
    return text.trim().charAt(0).toUpperCase() || "•";
  }

  function shortPeriodRange(period) {
    if (!period || !period.dateFrom || !period.dateTo) return defaultPeriodLabel;
    return `${formatShortRange(period.dateFrom)}–${formatShortRange(period.dateTo)}`;
  }

  function buildQuery(period) {
    const params = new URLSearchParams();
    params.set("preset", toApiPreset(period.preset));
    if (period.preset === "custom") {
      params.set("date_from", period.dateFrom);
      params.set("date_to", period.dateTo);
    }
    return params.toString();
  }

  async function postJson(url, payload) {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "same-origin",
      body: JSON.stringify(payload || {}),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw data;
    return data;
  }

  async function getJson(url) {
    const response = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw data;
    return data;
  }

  function telegramWebApp() {
    return window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  }

  function attachTelegramShell() {
    const webApp = telegramWebApp();
    if (!webApp) return false;
    try {
      webApp.ready();
      webApp.expand();
      webApp.setHeaderColor("#061f1f");
      webApp.setBackgroundColor("#061f1f");
    } catch (_error) {
      // ignore shell cosmetics failures
    }
    return true;
  }

  function resetViewportPosition() {
    try {
      if (window.history && "scrollRestoration" in window.history) {
        window.history.scrollRestoration = "manual";
      }
    } catch (_error) {
      // ignore scroll restoration failures
    }

    const applyReset = () => {
      try {
        window.scrollTo(0, 0);
      } catch (_error) {
        // ignore
      }
      try {
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
      } catch (_error) {
        // ignore
      }
      if (elements.dashboardRoot && !elements.dashboardRoot.hidden) {
        try {
          elements.dashboardRoot.scrollIntoView({ block: "start", inline: "nearest" });
        } catch (_error) {
          // ignore
        }
      }
    };

    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(applyReset);
    });
  }

  function openBotDeepLink() {
    const webApp = telegramWebApp();
    if (webApp && typeof webApp.openTelegramLink === "function") {
      try {
        webApp.openTelegramLink(botTelegramUrl);
        return;
      } catch (_error) {
        // ignore
      }
    }
    if (webApp && typeof webApp.close === "function") {
      try {
        webApp.close();
        return;
      } catch (_error) {
        // ignore
      }
    }
    window.location.href = botTelegramUrl;
  }

  function showStatus(config) {
    elements.loadingPanel.hidden = true;
    elements.dashboardRoot.hidden = true;
    elements.statusPanel.hidden = false;
    const actions = (config.actions || [])
      .map(
        (action) => `
          <button
            type="button"
            class="v2-status-action ${action.primary ? "v2-status-action-primary" : ""}"
            data-status-action="${escapeHtml(action.action)}"
          >
            ${escapeHtml(action.label)}
          </button>
        `
      )
      .join("");
    elements.statusPanel.innerHTML = `
      <div class="v2-status-copy">
        <p class="v2-kicker">${escapeHtml(config.kicker || "Vydno.Capital")}</p>
        <strong>${escapeHtml(config.title || "Mini App")}</strong>
        <p>${escapeHtml(config.message || "")}</p>
      </div>
      ${actions ? `<div class="v2-status-actions">${actions}</div>` : ""}
    `;
  }

  function showLoading() {
    elements.statusPanel.hidden = true;
    elements.dashboardRoot.hidden = true;
    elements.loadingPanel.hidden = false;
  }

  function showDashboard() {
    elements.statusPanel.hidden = true;
    elements.loadingPanel.hidden = true;
    elements.dashboardRoot.hidden = false;
    if (!state.dashboardShown) {
      state.dashboardShown = true;
      resetViewportPosition();
    }
  }

  function setBusy(value) {
    elements.dashboardRoot.classList.toggle("is-busy", Boolean(value));
  }

  function showInlineAlert(message) {
    if (!elements.inlineAlert) return;
    if (!message) {
      elements.inlineAlert.hidden = true;
      elements.inlineAlert.textContent = "";
      return;
    }
    elements.inlineAlert.hidden = false;
    elements.inlineAlert.textContent = message;
  }

  function restorePeriodDates() {
    if (state.period.preset === "custom" && state.period.dateFrom && state.period.dateTo) {
      return { dateFrom: state.period.dateFrom, dateTo: state.period.dateTo };
    }
    const range = presetRange(state.period.preset || "current_month", state.mode === "preview" ? PREVIEW_REFERENCE_DATE : new Date());
    return { dateFrom: toIsoDate(range.start), dateTo: toIsoDate(range.end) };
  }

  function computePreviewTotals(items) {
    return items.reduce(
      (accumulator, item) => {
        if (item.flowKind !== "normal") return accumulator;
        if (item.type === "income") accumulator.income += Math.abs(item.amount);
        if (item.type === "expense") accumulator.expense += Math.abs(item.amount);
        return accumulator;
      },
      { income: 0, expense: 0 }
    );
  }

  function normalizeRecentTransactions(items) {
    return items
      .slice()
      .sort((left, right) => toDate(right.date) - toDate(left.date) || right.id - left.id)
      .slice(0, 5)
      .map((item) => ({
        title: item.title,
        meta: item.subtitle,
        amountDisplay: formatMoney(item.amount, item.currency, { forcePlus: item.amount > 0 }),
        amountRaw: item.amount,
        badge: item.flowKind === "transfer" ? "transfer" : item.type === "income" ? "income" : "expense",
        currency: item.currency,
      }));
  }

  function groupExpenseCategories(items) {
    const grouped = {};
    items.forEach((item) => {
      if (item.flowKind !== "normal" || item.type !== "expense") return;
      const key = item.category || "Інше";
      grouped[key] = (grouped[key] || 0) + Math.abs(item.amount);
    });
    const total = Object.values(grouped).reduce((sum, value) => sum + value, 0);
    return Object.entries(grouped)
      .sort((left, right) => right[1] - left[1])
      .map(([name, amount], index) => ({
        name,
        amount,
        share: total ? Math.round((amount / total) * 100) : 0,
        amountDisplay: formatMoney(amount, "UAH"),
        color: ["#20d6b4", "#6aa7ff", "#ff6174", "#f2b84b", "#7f9499"][index % 5],
      }));
  }

  function buildDailyFlowBuckets(items, period) {
    const perDay = new Map();
    eachDay(period.dateFrom, period.dateTo).forEach((day) => {
      const key = toIsoDate(day);
      perDay.set(key, { date: key, label: formatAxisDate(day), income: 0, expense: 0, net: 0 });
    });
    items.forEach((item) => {
      const bucket = perDay.get(item.date);
      if (!bucket || item.flowKind !== "normal") return;
      if (item.type === "income") bucket.income += Math.abs(item.amount);
      if (item.type === "expense") bucket.expense += Math.abs(item.amount);
      bucket.net = bucket.income - bucket.expense;
    });
    return Array.from(perDay.values());
  }

  function cumulativeSeries(points) {
    let running = 0;
    return points.map((point) => {
      running += asNumber(point.net);
      return running;
    });
  }

  function hasUsefulSparkline(points) {
    if (!Array.isArray(points) || points.length < 3) return false;

    const values = points
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));

    if (values.length < 3) return false;

    const min = Math.min(...values);
    const max = Math.max(...values);

    return max !== min;
  }

  function sparklineVisibilityState(points) {
    if (!Array.isArray(points) || points.length < 3) return "empty";

    const values = points
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));

    if (values.length < 3) return "empty";
    return hasUsefulSparkline(values) ? "ready" : "flat";
  }

  function buildPreviewPresentation() {
    const resolved = restorePeriodDates();
    const period = {
      preset: state.period.preset,
      dateFrom: resolved.dateFrom,
      dateTo: resolved.dateTo,
      label: state.period.preset === "custom" ? `${formatDateFull(resolved.dateFrom)} - ${formatDateFull(resolved.dateTo)}` : PRESET_LABELS[state.period.preset] || defaultPeriodLabel,
      chipLabel: state.period.preset === "custom" ? `${formatShortRange(resolved.dateFrom)}–${formatShortRange(resolved.dateTo)}` : PRESET_LABELS[state.period.preset] || defaultPeriodLabel,
    };

    const filtered = PREVIEW_SOURCE.transactions.filter((item) => item.date >= period.dateFrom && item.date <= period.dateTo);
    const normal = filtered.filter((item) => item.flowKind === "normal");
    const totals = computePreviewTotals(normal);
    const net = totals.income - totals.expense;
    const summaryBars = buildDailyFlowBuckets(normal, period);
    const sparkline = cumulativeSeries(summaryBars);
    const categories = groupExpenseCategories(normal);

    const accountsTotal = PREVIEW_SOURCE.accounts.reduce((sum, account) => sum + account.balance * (PREVIEW_FX[account.currency] || 1), 0);
    const usdEquivalent = accountsTotal / PREVIEW_FX.USD;
    const netFlowPercent = accountsTotal ? (net / accountsTotal) * 100 : null;
    const previewCurrencies = Array.from(new Set(PREVIEW_SOURCE.accounts.map((account) => account.currency).filter((currency) => currency !== "UAH"))).sort();
    const previewRateNote = previewCurrencies.length ? `Курс НБУ на ${formatDateFull(PREVIEW_REFERENCE_DATE)}: ${previewCurrencies.join(", ")}` : "";

    const monthSeries = PREVIEW_SOURCE.monthlySeries.map((item) => ({
      label: monthTickLabel(item.month),
      income: item.income,
      expense: item.expense,
      net: item.income - item.expense,
    }));
    const currentMonth = monthSeries[monthSeries.length - 1];
    const previousMonth = monthSeries[monthSeries.length - 2];
    const incomeChange = previousMonth && previousMonth.income ? ((currentMonth.income - previousMonth.income) / previousMonth.income) * 100 : null;
    const expenseChange = previousMonth && previousMonth.expense ? ((currentMonth.expense - previousMonth.expense) / previousMonth.expense) * 100 : null;

    const credit = PREVIEW_SOURCE.creditCards[0];
    const utilization = credit && credit.creditLimit ? Math.round((credit.debt / credit.creditLimit) * 100) : 0;
    const totalDebtValue = credit ? credit.debt : PREVIEW_SOURCE.debts.owedByUser;
    const totalDebtDisplay = formatMoney(totalDebtValue, "UAH");

    return {
      mode: "preview",
      period,
      overview: {
        totalBalance: formatMoney(accountsTotal, "UAH"),
        totalBalanceRaw: accountsTotal,
        usdEquivalent: `≈ ${formatMoney(usdEquivalent, "USD").replace("$", "USD")}`,
        rateNote: previewRateNote,
        netFlow: formatMoney(net, "UAH", { forcePlus: net > 0 }),
        netFlowRaw: net,
        netFlowPercent,
        sparkline,
        income: { value: formatMoney(totals.income, "UAH"), change: incomeChange },
        expense: { value: formatMoney(totals.expense, "UAH"), change: expenseChange },
        transactions: normalizeRecentTransactions(filtered),
        summary: {
          income: formatMoney(totals.income, "UAH"),
          expense: formatMoney(totals.expense, "UAH"),
          net: formatMoney(net, "UAH", { forcePlus: net > 0 }),
          netRaw: net,
          bars: summaryBars,
          rangeStart: formatAxisDate(period.dateFrom),
          rangeEnd: formatAxisDate(period.dateTo),
        },
      },
      analytics: {
        monthSeries,
        expenseCategories: categories,
        expenseTotal: formatMoney(totals.expense, "UAH"),
      },
      status: {
        accounts: {
          total: formatMoney(accountsTotal, "UAH"),
          subValue: `≈ ${formatMoney(usdEquivalent, "USD").replace("$", "USD")}`,
          items: PREVIEW_SOURCE.accounts.map((account) => ({
            title: account.label,
            meta: account.typeLabel,
            amount: formatMoney(account.balance, account.currency),
            currency: account.currency,
          })),
        },
        debts: {
          totalDebt: totalDebtDisplay,
          totalDebtRaw: totalDebtValue,
          cardsCount: PREVIEW_SOURCE.creditCards.length,
          utilization,
          owedByUser: formatMoney(PREVIEW_SOURCE.debts.owedByUser, "UAH"),
          note: PREVIEW_SOURCE.creditCards.length ? `Використано ${utilization}% від ліміту` : "Кредитних карток поки немає.",
        },
        goals: PREVIEW_SOURCE.goals.map((goal) => ({
          title: goal.label,
          current: formatMoney(goal.currentAmount, "UAH"),
          target: formatMoney(goal.targetAmount, "UAH"),
          progressPercent: goal.progressPercent,
        })),
        investments: {
          currentValue: formatMoney(PREVIEW_SOURCE.investments.currentValue, "UAH"),
          currentValueRaw: PREVIEW_SOURCE.investments.currentValue,
          note: PREVIEW_SOURCE.investments.note,
          items: PREVIEW_SOURCE.investments.items,
        },
      },
      notices: [],
    };
  }

  function normalizeLiveMoney(payload, options) {
    if (!payload) return "-";
    if (payload.display) {
      const value = moneyValue(payload);
      if (Number.isFinite(value)) {
        return formatMoney(value, payload.currency || "UAH", options);
      }
      return payload.display;
    }
    return formatMoney(moneyValue(payload), payload.currency || "UAH", options);
  }

  function normalizeLiveTransactions(items) {
    return (items || []).slice(0, 5).map((item) => ({
      title: item.title,
      meta: item.subtitle,
      amountDisplay: normalizeLiveMoney(item.amount, { forcePlus: String(item.type_badge || "").toLowerCase() === "income" }),
      amountRaw:
        String(item.type_badge || "").toLowerCase() === "income"
          ? Math.abs(moneyValue(item.amount))
          : String(item.type_badge || "").toLowerCase() === "expense"
            ? -Math.abs(moneyValue(item.amount))
            : moneyValue(item.amount),
      badge: item.type_badge,
      currency: item.amount && item.amount.currency ? item.amount.currency : "",
    }));
  }

  function normalizeLiveCategories(items) {
    return (items || []).map((item) => ({
      name: item.name,
      amount: moneyValue(item.amount),
      share: item.share_percent || 0,
      amountDisplay: normalizeLiveMoney(item.amount),
      color: item.color || "#20d6b4",
      vsPreviousPercent: item.vs_previous_percent,
    }));
  }

  function secondaryMoneyText(payload, fallback) {
    if (!payload) return fallback;
    const display = normalizeLiveMoney(payload);
    if (display && display !== "-") return display;
    return payload.note || fallback;
  }

  function isRateEstimateNote(note) {
    return /^Орієнтовно за курсом НБУ(?:\s+на\s+\d{2}\.\d{2}\.\d{4})?(?:\.\s*Є валюти:\s*.+)?\.?$/u.test(String(note || "").trim());
  }

  function formatRateNote(note) {
    const trimmed = String(note || "").trim();
    if (!isRateEstimateNote(trimmed)) return "";
    const match = trimmed.match(/^Орієнтовно за курсом НБУ(?:\s+на\s+([^.]+))?(?:\.\s*Є валюти:\s*(.+?))?\.?$/u);
    if (!match) return "";
    const datePart = match[1] ? ` на ${match[1].trim()}` : "";
    const currenciesPart = match[2] ? `: ${match[2].trim().replace(/\.$/, "")}` : "";
    return `Курс НБУ${datePart}${currenciesPart}`;
  }

  async function fetchLiveMonthSeries(period) {
    const anchor = toDate(period.date_to);
    const months = [4, 3, 2, 1, 0].map((offset) => shiftMonths(anchor, -offset));
    return Promise.all(
      months.map(async (monthStart) => {
        const start = toIsoDate(startOfMonth(monthStart));
        const isAnchorMonth = monthStart.getMonth() === anchor.getMonth() && monthStart.getFullYear() === anchor.getFullYear();
        const end = toIsoDate(isAnchorMonth ? anchor : endOfMonth(monthStart));
        const response = await getJson(`${flowUrl}?preset=custom&date_from=${start}&date_to=${end}`);
        return {
          label: monthTickLabel(monthStart),
          income: moneyValue(response.income_total),
          expense: moneyValue(response.expense_total),
          net: moneyValue(response.net_total),
        };
      })
    );
  }

  function normalizeLivePresentation(payload, monthSeries) {
    const periodPayload = payload.period || {};
    const overviewPayload = payload.overview || {};
    const flow = payload.flow_preview || {};
    const categoriesPreview = payload.categories_preview || {};
    const activityPreview = payload.activity_preview || {};
    const accountsPreview = payload.accounts_preview || {};
    const creditPreview = payload.credit_cards_preview || {};
    const debtsPreview = payload.debts_preview || {};
    const goalsPreview = payload.goals_preview || {};
    const investmentsPreview = payload.investments_preview || {};

    const summaryBars = (flow.buckets || []).map((bucket) => ({
      label: bucket.label,
      income: asNumber(bucket.income),
      expense: asNumber(bucket.expense),
      net: asNumber(bucket.net),
    }));
    const sparkline = cumulativeSeries(summaryBars);
    const currentMonth = monthSeries[monthSeries.length - 1];
    const previousMonth = monthSeries[monthSeries.length - 2];
    const incomeChange = currentMonth && previousMonth && previousMonth.income ? ((currentMonth.income - previousMonth.income) / previousMonth.income) * 100 : null;

    const creditItems = creditPreview.items || [];
    const cardDebtRaw = moneyValue(creditPreview.summary && creditPreview.summary.total_debt);
    const totalLimit = creditItems.reduce((sum, item) => sum + moneyValue(item.credit_limit), 0);
    const utilization = totalLimit ? Math.round((cardDebtRaw / totalLimit) * 100) : 0;
    const overviewUsdEquivalent = overviewPayload.usd_equivalent || accountsPreview.usd_equivalent || null;
    const accountsUsdEquivalent = accountsPreview.usd_equivalent || overviewPayload.usd_equivalent || null;
    const rateNote = formatRateNote(
      (overviewUsdEquivalent && overviewUsdEquivalent.note) || (accountsUsdEquivalent && accountsUsdEquivalent.note) || ""
    );
    const totalBalanceRaw = moneyValue(overviewPayload.total_balance);
    const netFlowRaw = moneyValue(overviewPayload.net_flow);
    const netFlowPercent = totalBalanceRaw ? (netFlowRaw / totalBalanceRaw) * 100 : null;
    const payableRaw = moneyValue(debtsPreview.summary && debtsPreview.summary.owed_by_user);
    const totalDebtPayload =
      cardDebtRaw > 0
        ? creditPreview.summary && creditPreview.summary.total_debt
        : payableRaw > 0
          ? debtsPreview.summary && debtsPreview.summary.owed_by_user
          : { value: "0.00", currency: "UAH", display: "0 грн" };

    const periodLabel =
      periodPayload.preset === "custom"
        ? `${formatDateFull(periodPayload.date_from)} - ${formatDateFull(periodPayload.date_to)}`
        : periodPayload.label || defaultPeriodLabel;

    return {
      mode: "live",
      period: {
        preset: toUiPreset(periodPayload.preset || "this_month"),
        dateFrom: periodPayload.date_from,
        dateTo: periodPayload.date_to,
        label: periodLabel,
        chipLabel: periodPayload.preset === "custom" ? `${formatShortRange(periodPayload.date_from)}–${formatShortRange(periodPayload.date_to)}` : periodPayload.label || defaultPeriodLabel,
      },
      overview: {
        totalBalance: normalizeLiveMoney(overviewPayload.total_balance),
        totalBalanceRaw,
        usdEquivalent: secondaryMoneyText(overviewUsdEquivalent, "USD-еквівалент поки недоступний"),
        rateNote,
        netFlow: normalizeLiveMoney(overviewPayload.net_flow),
        netFlowRaw,
        netFlowPercent,
        sparkline,
        income: { value: normalizeLiveMoney(flow.income_total), change: incomeChange },
        expense: {
          value: normalizeLiveMoney(flow.expense_total),
          change: overviewPayload.expenses && overviewPayload.expenses.vs_previous_percent !== null ? Number(overviewPayload.expenses.vs_previous_percent) : null,
        },
        transactions: normalizeLiveTransactions(activityPreview.items),
        summary: {
          income: normalizeLiveMoney(flow.income_total),
          expense: normalizeLiveMoney(flow.expense_total),
          net: normalizeLiveMoney(flow.net_total),
          netRaw: moneyValue(flow.net_total),
          bars: summaryBars,
          rangeStart: periodPayload.date_from ? formatAxisDate(periodPayload.date_from) : "",
          rangeEnd: periodPayload.date_to ? formatAxisDate(periodPayload.date_to) : "",
        },
      },
      analytics: {
        monthSeries,
        expenseCategories: normalizeLiveCategories(categoriesPreview.items),
        expenseTotal: normalizeLiveMoney(categoriesPreview.total),
      },
      status: {
        accounts: {
          total: normalizeLiveMoney(accountsPreview.total_balance),
          subValue: secondaryMoneyText(
            accountsUsdEquivalent,
            accountsPreview.state && accountsPreview.state.note ? accountsPreview.state.note : "USD-еквівалент активних рахунків"
          ),
          items: (accountsPreview.items || []).map((account) => ({
            title: account.label,
            meta: `${account.type_label} • ${account.currency}`,
            amount: normalizeLiveMoney(account.balance),
            currency: account.currency,
          })),
        },
        debts: {
          totalDebt: normalizeLiveMoney(totalDebtPayload),
          totalDebtRaw: cardDebtRaw > 0 ? cardDebtRaw : payableRaw,
          cardsCount: creditPreview.summary ? creditPreview.summary.cards_count || 0 : 0,
          utilization,
          owedByUser: normalizeLiveMoney(debtsPreview.summary && debtsPreview.summary.owed_by_user),
          note:
            creditItems.length > 0
              ? `Використано ${utilization}% від ліміту`
              : payableRaw > 0
                ? `Людські борги: ${normalizeLiveMoney(debtsPreview.summary && debtsPreview.summary.owed_by_user)}`
                : "Кредитних карток поки немає.",
        },
        goals: (goalsPreview.items || []).map((goal) => ({
          title: goal.label,
          current: normalizeLiveMoney(goal.current_amount),
          target: normalizeLiveMoney(goal.target_amount),
          progressPercent: goal.progress_percent || 0,
        })),
        investments: {
          currentValue: normalizeLiveMoney(investmentsPreview.summary && investmentsPreview.summary.current_value),
          currentValueRaw: moneyValue(investmentsPreview.summary && investmentsPreview.summary.current_value),
          note:
            (investmentsPreview.items || []).length === 0
              ? "Інвест-рахунків поки немає."
              : (investmentsPreview.state && investmentsPreview.state.note) || `${(investmentsPreview.items || []).length} інвест-рахунки в огляді.`,
          items: investmentsPreview.items || [],
        },
      },
      notices: [
        flow.state && flow.state.note,
        categoriesPreview.state && categoriesPreview.state.note,
        accountsPreview.state && accountsPreview.state.note,
      ].filter((note) => note && !isRateEstimateNote(note)),
    };
  }

  function createLinePath(values, width, height, padding) {
    const series = values && values.length ? values : [0, 0, 0, 0];
    const min = Math.min(...series);
    const max = Math.max(...series);
    const range = max - min || 1;
    const points = series.map((value, index) => {
      const x = (index / Math.max(series.length - 1, 1)) * width;
      const y = height - padding - ((value - min) / range) * (height - padding * 2);
      return { x, y };
    });
    const line = points.map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
    const fill = `${line} L${width} ${height} L0 ${height} Z`;
    return { line, fill };
  }

  function renderSparkline(container, values, negative) {
    const color = negative ? "#ff6174" : "#20d6b4";
    const gradientId = negative ? "v2SparkFillNegative" : "v2SparkFillPositive";
    const paths = createLinePath(values, 320, 74, 10);
    container.innerHTML = `
      <svg viewBox="0 0 320 74" preserveAspectRatio="none">
        <defs>
          <linearGradient id="${gradientId}" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stop-color="${color}" stop-opacity="0.30"></stop>
            <stop offset="100%" stop-color="${color}" stop-opacity="0"></stop>
          </linearGradient>
        </defs>
        <path class="v2-spark-fill" d="${paths.fill}" fill="url(#${gradientId})"></path>
        <path class="v2-spark-line" d="${paths.line}" stroke="${color}"></path>
      </svg>
    `;
  }

  function renderMicroBars(bars) {
    if (!bars.length) {
      elements.periodMicroChart.innerHTML = `<div class="v2-empty-state">За цей період ще немає руху.</div>`;
      return;
    }
    const limited = bars.slice(-16);
    const max = Math.max(1, ...limited.map((bar) => Math.abs(asNumber(bar.net))));
    elements.periodMicroChart.innerHTML = limited
      .map((bar) => {
        const value = asNumber(bar.net);
        const height = Math.max(6, Math.round((Math.abs(value) / max) * 54));
        const direction = value < 0 ? "minus" : "plus";
        return `<span class="v2-bar ${direction}" style="height: ${height}px"></span>`;
      })
      .join("");
  }

  function renderMonthChart(series) {
    if (!series.length) {
      elements.monthChart.innerHTML = `<div class="v2-empty-state">Недостатньо даних для аналітики по місяцях.</div>`;
      return;
    }
    const max = Math.max(1, ...series.flatMap((item) => [asNumber(item.income), asNumber(item.expense)]));
    elements.monthChart.innerHTML = series
      .map((item) => {
        const incomeHeight = Math.max(8, Math.round((asNumber(item.income) / max) * 100));
        const expenseHeight = Math.max(8, Math.round((asNumber(item.expense) / max) * 100));
        return `
          <div class="v2-month-col">
            <div class="v2-month-bars">
              <span class="income" style="height: ${incomeHeight}%"></span>
              <span class="expense" style="height: ${expenseHeight}%"></span>
            </div>
            <small>${escapeHtml(item.label)}</small>
          </div>
        `;
      })
      .join("");
  }

  function transactionIcon(item) {
    if (item.badge === "transfer" || /переказ/i.test(String(item.badge || ""))) return "↔";
    if (item.badge === "income" || /дох/i.test(String(item.badge || ""))) return currencyGlyph(item.currency || item.amountDisplay);
    if (item.badge === "expense" || /витрат/i.test(String(item.badge || ""))) {
      return String(item.title || "").trim().charAt(0).toUpperCase() || "•";
    }
    return "•";
  }

  function renderTransactions(items) {
    if (!items.length) {
      elements.transactionsList.innerHTML = `<div class="v2-empty-state">За обраний період транзакцій ще немає.</div>`;
      return;
    }
    elements.transactionsList.innerHTML = items
      .slice(0, 5)
      .map(
        (item) => `
          <div class="v2-tx">
            <div class="v2-tx-icon">${escapeHtml(transactionIcon(item))}</div>
            <div class="v2-tx-main">
              <div class="v2-tx-title">${escapeHtml(item.title)}</div>
              <div class="v2-tx-meta">${escapeHtml(item.meta || "")}</div>
            </div>
            <div class="v2-tx-amount ${toneClass(item.amountRaw)}">${escapeHtml(item.amountDisplay)}</div>
          </div>
        `
      )
      .join("");
  }

  function expenseCategoryPercent(item) {
    return Math.max(0, asNumber(item && (item.percent ?? item.share)));
  }

  function expenseCategoryLabel(item, fallback) {
    return item && (item.amountDisplay || item.amount || fallback) ? item.amountDisplay || item.amount || fallback : fallback || "0 грн";
  }

  function renderExpenseStructure(categories, totalLabel) {
    const empty = elements.expenseStructureEmpty;
    const donutLayout = elements.expenseDonutLayout;
    const single = elements.singleCategoryState;
    const safeCategories = Array.isArray(categories)
      ? categories.filter((item) => expenseCategoryPercent(item) > 0)
      : [];

    if (safeCategories.length === 0) {
      empty.hidden = false;
      donutLayout.hidden = true;
      single.hidden = true;
      elements.expenseCategoryList.innerHTML = "";
      elements.donutSegments.forEach((segment) => {
        segment.style.display = "none";
      });
      return;
    }

    if (safeCategories.length === 1) {
      const item = safeCategories[0];
      const percent = Math.min(100, Math.max(0, Math.round(expenseCategoryPercent(item) || 100)));
      empty.hidden = true;
      donutLayout.hidden = true;
      single.hidden = false;
      elements.singleCategoryName.textContent = item.name || "Категорія";
      elements.singleCategoryAmount.textContent = expenseCategoryLabel(item, totalLabel);
      elements.singleCategoryPercent.textContent = `${percent}%`;
      elements.singleCategoryBar.style.width = `${percent}%`;
      elements.expenseCategoryList.innerHTML = "";
      elements.donutSegments.forEach((segment) => {
        segment.style.display = "none";
      });
      return;
    }

    empty.hidden = true;
    single.hidden = true;
    donutLayout.hidden = false;

    const topCategories = safeCategories.slice(0, 5);
    const circumference = 2 * Math.PI * 44;
    let shareOffset = 0;
    elements.donutSegments.forEach((segment, index) => {
      const item = topCategories[index];
      if (!item) {
        segment.style.display = "none";
        return;
      }
      const share = Math.max(0, Math.min(expenseCategoryPercent(item), 100 - shareOffset));
      const arcLength = (share / 100) * circumference;
      const rotation = -90 + (shareOffset / 100) * 360;
      shareOffset += share;
      segment.style.display = "";
      segment.style.stroke = item.color || "#20d6b4";
      segment.style.strokeDasharray = `${arcLength.toFixed(3)} ${(circumference - arcLength).toFixed(3)}`;
      segment.setAttribute("transform", `rotate(${rotation.toFixed(2)} 60 60)`);
    });

    const center = splitMoneyDisplay(totalLabel);
    elements.expenseDonutTotal.textContent = center.amount;
    elements.expenseDonutCurrency.textContent = center.currency || "грн";
    elements.expenseCategoryList.innerHTML = topCategories
      .map(
        (item, index) => `
          <div class="v2-category-row">
            <span class="v2-category-dot c${index + 1}" style="background: ${escapeHtml(item.color || "#20d6b4")}"></span>
            <span>${escapeHtml(item.name || "Категорія")}</span>
            <strong>${escapeHtml(`${Math.round(expenseCategoryPercent(item))}%`)}</strong>
          </div>
        `
      )
      .join("");
  }

  function renderAccounts(items) {
    if (!items.length) {
      elements.accountsList.innerHTML = `<div class="v2-empty-state">Активних рахунків поки не знайдено.</div>`;
      return;
    }
    elements.accountsList.innerHTML = items
      .map(
        (item) => `
          <div class="v2-account-row">
            <div class="v2-account-badge">${escapeHtml(currencyGlyph(item.currency || item.meta || item.amount))}</div>
            <div class="v2-account-main">
              <div class="v2-account-title">${escapeHtml(item.title)}</div>
              <div class="v2-account-meta">${escapeHtml(item.meta || "")}</div>
            </div>
            <div class="v2-account-amount">${escapeHtml(item.amount)}</div>
          </div>
        `
      )
      .join("");
  }

  function renderGoalStatus(goals) {
    const primaryGoal = goals && goals.length ? goals[0] : null;
    if (!primaryGoal) {
      elements.goalTitle.textContent = "Цілі не налаштовані";
      elements.goalPercentValue.textContent = "0%";
      elements.goalCaption.textContent = "Додайте ціль у накопичувальному рахунку.";
      elements.goalProgressBar.style.width = "0%";
      return;
    }
    const percent = Math.max(0, Math.min(100, Math.round(asNumber(primaryGoal.progressPercent))));
    elements.goalTitle.textContent = primaryGoal.title;
    elements.goalPercentValue.textContent = `${percent}%`;
    elements.goalCaption.textContent = `${primaryGoal.current} з ${primaryGoal.target}`;
    elements.goalProgressBar.style.width = `${percent}%`;
  }

  function renderInvestmentStatus(investments) {
    elements.investmentValue.textContent = investments.currentValue || "0 грн";
    if (!investments.items || investments.items.length === 0) {
      elements.investmentNote.textContent = "Інвест-рахунків поки немає.";
      return;
    }
    elements.investmentNote.textContent = investments.note || `${investments.items.length} інвест-рахунки в огляді.`;
  }

  function applyPeriodCopy(period) {
    const shortLabel = shortPeriodRange(period);
    elements.dateChipLabel.textContent = period.chipLabel || period.label || defaultPeriodLabel;
    elements.summaryPeriodLabel.textContent = shortLabel;
    elements.expenseStructureChip.textContent = period.chipLabel || period.label || defaultPeriodLabel;
    elements.analyticsChip.textContent = period.chipLabel || period.label || defaultPeriodLabel;
  }

  function renderPresentation(presentation) {
    state.presentation = presentation;
    state.period = {
      preset: presentation.period.preset,
      dateFrom: presentation.period.dateFrom,
      dateTo: presentation.period.dateTo,
    };

    applyPeriodCopy(presentation.period);

    elements.balanceValue.textContent = presentation.overview.totalBalance;
    elements.balanceUsd.textContent = presentation.overview.usdEquivalent || "≈ - USD";
    if (presentation.overview.rateNote) {
      elements.rateNote.hidden = false;
      elements.rateNote.textContent = presentation.overview.rateNote;
    } else {
      elements.rateNote.hidden = true;
      elements.rateNote.textContent = "";
    }
    elements.netFlowValue.textContent = presentation.overview.netFlow;
    elements.netFlowValue.className = `v2-net-value ${toneClass(presentation.overview.netFlowRaw)}`.trim();
    elements.netFlowDelta.textContent = formatPercent(presentation.overview.netFlowPercent, { digits: 2 });
    elements.netFlowDelta.className = `v2-percent-pill ${toneClass(presentation.overview.netFlowPercent)}`.trim();
    const sparklinePoints = presentation.overview.sparkline;
    if (elements.balanceSparkline) {
      const sparklineState = sparklineVisibilityState(sparklinePoints);
      elements.balanceSparkline.classList.remove("is-empty", "is-flat");

      if (sparklineState !== "ready") {
        elements.balanceSparkline.classList.add(sparklineState === "flat" ? "is-flat" : "is-empty");
        elements.balanceSparkline.innerHTML = "";
      } else {
        renderSparkline(elements.balanceSparkline, sparklinePoints, presentation.overview.netFlowRaw < 0);
      }
    }

    elements.incomeValue.textContent = presentation.overview.income.value;
    elements.incomeDelta.textContent = formatPercent(presentation.overview.income.change, { digits: 1 });
    elements.incomeDelta.className = `v2-metric-change ${toneClass(presentation.overview.income.change)}`.trim();
    elements.expenseValue.textContent = presentation.overview.expense.value;
    elements.expenseDelta.textContent = formatPercent(presentation.overview.expense.change, { digits: 1 });
    elements.expenseDelta.className = `v2-metric-change ${toneClass(presentation.overview.expense.change)}`.trim();

    renderTransactions(presentation.overview.transactions);
    elements.summaryIncomeValue.textContent = presentation.overview.summary.income;
    elements.summaryExpenseValue.textContent = presentation.overview.summary.expense;
    elements.summaryNetValue.textContent = presentation.overview.summary.net;
    elements.summaryNetValue.className = toneClass(presentation.overview.summary.netRaw);
    renderMicroBars(presentation.overview.summary.bars);
    elements.periodRangeStart.textContent = presentation.overview.summary.rangeStart || "-";
    elements.periodRangeEnd.textContent = presentation.overview.summary.rangeEnd || "-";

    renderExpenseStructure(presentation.analytics.expenseCategories, presentation.analytics.expenseTotal);

    elements.accountsTotalValue.textContent = presentation.status.accounts.total;
    elements.accountsSubValue.textContent = presentation.status.accounts.subValue;
    renderAccounts(presentation.status.accounts.items);

    elements.creditDebtValue.textContent = presentation.status.debts.totalDebt || "0 грн";
    elements.creditDebtValue.className = presentation.status.debts.totalDebtRaw > 0 ? "is-negative" : "";
    elements.creditCardsCount.textContent = String(presentation.status.debts.cardsCount || 0);
    elements.debtProgressBar.style.width = `${Math.min(Math.max(presentation.status.debts.utilization || 0, 0), 100)}%`;
    elements.debtProgressLabel.textContent = presentation.status.debts.note;

    renderGoalStatus(presentation.status.goals);
    renderInvestmentStatus(presentation.status.investments);

    renderMonthChart(presentation.analytics.monthSeries);

    showInlineAlert((presentation.notices || []).join(" "));
    syncPresetState();
    showDashboard();
  }

  async function authenticate() {
    if (attachTelegramShell()) {
      const webApp = telegramWebApp();
      const initData = webApp ? webApp.initData || "" : "";
      if (initData) {
        return postJson(authUrl, { init_data: initData });
      }
    }
    if (devAuthEnabled) {
      return postJson(devAuthUrl, {});
    }
    throw { error: { code: "telegram_required", message: "Відкрий mini-app із Telegram або використовуй локальний preview." } };
  }

  async function restoreSessionProfile() {
    try {
      return await getJson(profileUrl);
    } catch (errorPayload) {
      const error = errorPayload && errorPayload.error ? errorPayload.error : {};
      if (error.code === "session_required") return null;
      throw errorPayload;
    }
  }

  async function ensureSession() {
    const profile = await restoreSessionProfile();
    if (profile) {
      state.access = profile.access || null;
      return profile;
    }
    const authPayload = await authenticate();
    state.access = authPayload.access || null;
    return authPayload;
  }

  async function loadLiveDashboard(initialLoad) {
    if (initialLoad) showLoading();
    else setBusy(true);
    try {
      await ensureSession();
      const bootstrap = await getJson(`${bootstrapUrl}?${buildQuery(state.period)}`);
      if (bootstrap.access && bootstrap.access.mode === "blocked") {
        showStatus({
          title: bootstrap.access.entry_title || "Доступ до кабінету заблоковано",
          message: bootstrap.access.entry_message || "Повернись у бот та активуй доступ, щоб відкрити mini-app.",
          actions: [
            { action: "return-to-bot", label: "Повернутися в бот", primary: true },
            { action: "retry-auth", label: "Оновити статус" },
          ],
        });
        return;
      }
      const monthSeries = await fetchLiveMonthSeries(bootstrap.period || {});
      renderPresentation(normalizeLivePresentation(bootstrap, monthSeries));
    } catch (errorPayload) {
      const error = errorPayload && errorPayload.error ? errorPayload.error : {};
      if (error.code === "telegram_required") {
        showStatus({
          title: "Відкрий mini-app із Telegram",
          message: "Для live-режиму потрібна Telegram-сесія. Для локального макета використовуй ?preview=1.",
          actions: [{ action: "return-to-bot", label: "Повернутися в бот", primary: true }],
        });
        return;
      }
      if (error.code === "bot_user_not_found") {
        showStatus({
          title: "Спочатку створи акаунт у боті",
          message: "Mini-app знайшов auth, але не знайшов user runtime. Спочатку заверши перший запуск у боті.",
          actions: [
            { action: "return-to-bot", label: "Повернутися в бот", primary: true },
            { action: "retry-auth", label: "Спробувати ще раз" },
          ],
        });
        return;
      }
      showStatus({
        title: "Не вдалося оновити дашборд",
        message: error.message || "Спробуй ще раз за кілька секунд.",
        actions: [
          { action: "retry-auth", label: "Повторити", primary: true },
          { action: "return-to-bot", label: "Повернутися в бот" },
        ],
      });
    } finally {
      setBusy(false);
    }
  }

  function loadPreviewDashboard(initialLoad) {
    if (initialLoad) showLoading();
    else setBusy(true);
    window.setTimeout(() => {
      renderPresentation(buildPreviewPresentation());
      setBusy(false);
    }, initialLoad ? 240 : 100);
  }

  function syncPresetState() {
    Array.from(elements.presetList.querySelectorAll("button[data-preset]")).forEach((button) => {
      const isActive = button.dataset.preset === state.period.preset;
      button.classList.toggle("is-active", isActive);
      const existingCheck = button.querySelector("span");
      if (existingCheck) existingCheck.remove();
      if (isActive) {
        const check = document.createElement("span");
        check.textContent = "✓";
        button.appendChild(check);
      }
    });
    const restored = restorePeriodDates();
    elements.dateFromInput.value = state.period.dateFrom || restored.dateFrom;
    elements.dateToInput.value = state.period.dateTo || restored.dateTo;
    elements.dateChip.setAttribute("aria-expanded", elements.filterSheet.hidden ? "false" : "true");
    elements.filterSheet.setAttribute("aria-hidden", elements.filterSheet.hidden ? "true" : "false");
  }

  function openFilterSheet() {
    syncPresetState();
    elements.filterSheet.hidden = false;
    elements.filterBackdrop.hidden = false;
    document.body.style.overflow = "hidden";
    elements.dateChip.setAttribute("aria-expanded", "true");
  }

  function closeFilterSheet() {
    elements.filterSheet.hidden = true;
    elements.filterBackdrop.hidden = true;
    document.body.style.overflow = "";
    elements.filterSheet.setAttribute("aria-hidden", "true");
    elements.dateChip.setAttribute("aria-expanded", "false");
  }

  async function reloadDashboard(initialLoad) {
    if (state.mode === "preview") {
      loadPreviewDashboard(initialLoad);
      return;
    }
    await loadLiveDashboard(initialLoad);
  }

  function applyPreset(preset) {
    state.period = { preset };
    const label = PRESET_LABELS[preset] || defaultPeriodLabel;
    elements.dateChipLabel.textContent = label;
    if (preset !== "custom") {
      closeFilterSheet();
      reloadDashboard(false);
    } else {
      syncPresetState();
    }
  }

  function applyCustomRange() {
    if (!elements.dateFromInput.value || !elements.dateToInput.value) return;
    if (elements.dateFromInput.value > elements.dateToInput.value) {
      showInlineAlert("Початок діапазону не може бути пізніше за кінець.");
      return;
    }
    showInlineAlert("");
    state.period = {
      preset: "custom",
      dateFrom: elements.dateFromInput.value,
      dateTo: elements.dateToInput.value,
    };
    elements.dateChipLabel.textContent = `${formatShortRange(state.period.dateFrom)}–${formatShortRange(state.period.dateTo)}`;
    closeFilterSheet();
    reloadDashboard(false);
  }

  function switchView(view) {
    state.view = view;
    const overviewActive = view === "overview";
    elements.overviewTab.classList.toggle("is-active", overviewActive);
    elements.analyticsTab.classList.toggle("is-active", !overviewActive);
    elements.overviewView.hidden = !overviewActive;
    elements.analyticsView.hidden = overviewActive;
  }

  async function init() {
    attachTelegramShell();
    state.period = { preset: "current_month" };
    syncPresetState();
    if (state.mode === "preview") {
      loadPreviewDashboard(true);
      return;
    }
    await loadLiveDashboard(true);
  }

  elements.dateChip.addEventListener("click", openFilterSheet);
  elements.filterCloseButton.addEventListener("click", closeFilterSheet);
  elements.filterBackdrop.addEventListener("click", closeFilterSheet);
  elements.presetList.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-preset]");
    if (!button) return;
    applyPreset(button.dataset.preset);
  });
  elements.applyCustomRangeButton.addEventListener("click", applyCustomRange);
  elements.overviewTab.addEventListener("click", () => switchView("overview"));
  elements.analyticsTab.addEventListener("click", () => switchView("analytics"));
  elements.statusPanel.addEventListener("click", async (event) => {
    const actionButton = event.target.closest("[data-status-action]");
    if (!actionButton) return;
    if (actionButton.dataset.statusAction === "return-to-bot") {
      openBotDeepLink();
      return;
    }
    if (actionButton.dataset.statusAction === "retry-auth") {
      await init();
    }
  });

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden && state.dashboardShown) {
      resetViewportPosition();
    }
  });
  window.addEventListener("pageshow", () => {
    if (state.dashboardShown) {
      resetViewportPosition();
    }
  });

  switchView("overview");
  init();
})();
