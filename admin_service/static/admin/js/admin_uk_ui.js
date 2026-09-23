(function () {
  const textReplacements = new Map([
    ["Dashboard", "Огляд"],
    ["Filters", "Фільтри"],
    ["Select action", "Оберіть дію"],
    ["No results found", "Нічого не знайдено"],
    ["Searching…", "Пошук…"],
    ["Searching...", "Пошук…"],
    ["Loading more results…", "Завантаження…"],
    ["Loading more results...", "Завантаження…"],
    ["Remove all items", "Прибрати все"],
    ["Save", "Зберегти"],
    ["Save and continue editing", "Зберегти й продовжити"],
    ["Save and add another", "Зберегти й створити ще"],
    ["Delete", "Видалити"],
    ["History", "Історія"],
    ["Add", "Створити"],
    ["View", "Переглянути"],
    ["Change", "Редагувати"],
    ["Go", "Виконати"],
    ["Run the selected action", "Виконати обрану дію"],
    ["User", "Користувач"],
    ["Users", "Користувачі"],
    ["Status", "Статус"],
    ["Source", "Джерело"],
    ["Type", "Тип"],
    ["Category", "Категорія"],
    ["Priority", "Пріоритет"],
    ["Provider", "Провайдер"],
    ["Plan", "Тариф"],
    ["Amount", "Сума"],
    ["Currency", "Валюта"],
    ["Comment", "Коментар"],
    ["Created at", "Створено"],
    ["Updated at", "Оновлено"],
    ["Started at", "Початок"],
    ["Expires at", "Діє до"],
    ["Next charge at", "Наступне списання"],
    ["Scheduled at", "Заплановано на"],
    ["Active", "Активний"],
    ["Inactive", "Неактивний"],
    ["Pending", "Очікує"],
    ["Paid", "Оплачено"],
    ["Failed", "Помилка"],
    ["Rejected", "Відхилено"],
    ["Refunded", "Повернено"],
    ["Cancelled", "Скасовано"],
    ["Expired", "Завершено"],
    ["Trial", "Пробний період"],
    ["Draft", "Чернетка"],
    ["Scheduled", "Заплановано"],
    ["Completed", "Завершено"],
    ["Resolved", "Вирішено"],
    ["Closed", "Закрито"],
    ["In Progress", "В роботі"],
    ["Waiting User", "Чекаємо користувача"],
    ["Low", "Низький"],
    ["Normal", "Звичайний"],
    ["High", "Високий"],
    ["Urgent", "Терміновий"],
    ["System", "Система"],
    ["Manual", "Вручну"],
    ["Admin", "Адміністратор"],
    ["Promo", "Промо"],
    ["All", "Усі"],
    ["Segment", "Сегмент"],
    ["Tag", "Тег"],
    ["Manual Users", "Обрані вручну"],
    ["Push Topic", "Тема повідомлень"],
    ["Payment", "Оплата"],
    ["Subscription", "Підписка"],
    ["Onboarding", "Онбординг"],
    ["Bug", "Помилка"],
    ["Question", "Питання"],
    ["Other", "Інше"],
  ]);

  const placeholderReplacements = new Map([
    ["Type to search", "Пошук"],
    ["Search", "Пошук"],
  ]);

  function normalize(value) {
    return (value || "").replace(/\s+/g, " ").trim();
  }

  function replaceTextNodes(root) {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();

    while (node) {
      const normalized = normalize(node.nodeValue);
      const replacement = textReplacements.get(normalized);

      if (replacement) {
        node.nodeValue = node.nodeValue.replace(normalized, replacement);
      }

      node = walker.nextNode();
    }
  }

  function replacePlaceholders(root) {
    root.querySelectorAll?.("input[placeholder], textarea[placeholder]").forEach((element) => {
      const replacement = placeholderReplacements.get(element.getAttribute("placeholder"));
      if (replacement) {
        element.setAttribute("placeholder", replacement);
      }
    });

    root.querySelectorAll?.("option").forEach((option) => {
      const normalized = normalize(option.textContent);
      const replacement = textReplacements.get(normalized);
      if (replacement) {
        option.textContent = replacement;
      }
    });
  }

  function translate(root) {
    if (!(root instanceof Element || root instanceof Document)) {
      return;
    }

    replaceTextNodes(root);
    replacePlaceholders(root);
  }

  function statusTone(value) {
    const normalized = normalize(value).toLowerCase();
    const positive = ["active", "paid", "success", "sent", "completed", "open", "активна", "активний", "успішно", "надіслано", "завершено"];
    const danger = ["failed", "rejected", "blocked", "cancelled", "error", "помилка", "відхилено", "заблоковано", "скасовано"];
    const warning = ["pending", "draft", "trial", "grace", "scheduled", "очікує", "чернетка", "тріал", "заплановано"];
    if (positive.some((item) => normalized === item || normalized.includes(item))) return "positive";
    if (danger.some((item) => normalized === item || normalized.includes(item))) return "danger";
    if (warning.some((item) => normalized === item || normalized.includes(item))) return "warning";
    return "info";
  }

  function enhanceResultTables(root) {
    root.querySelectorAll?.("table#result_list").forEach((table) => {
      if (table.dataset.operationalEnhanced === "1") return;
      table.dataset.operationalEnhanced = "1";

      const labels = Array.from(table.querySelectorAll("thead th")).map((cell) => normalize(cell.textContent));
      table.querySelectorAll("tbody tr").forEach((row) => {
        Array.from(row.children).forEach((cell, index) => {
          cell.dataset.label = cell.classList.contains("action-checkbox") ? "Обрати" : (labels[index] || "");
        });
      });

      table.querySelectorAll("tbody td[class*='field-status'], tbody td[class*='field-success'], tbody td[class*='field-state']").forEach((cell) => {
        if (cell.querySelector("input, select, .op-status")) return;
        const value = normalize(cell.textContent);
        if (!value || value === "—" || value === "-") return;
        const badge = document.createElement("span");
        badge.className = `op-status op-status--${statusTone(value)}`;
        while (cell.firstChild) badge.appendChild(cell.firstChild);
        cell.appendChild(badge);
      });

      const emptyCell = table.querySelector("tbody tr:only-child td[colspan]");
      if (emptyCell) {
        emptyCell.classList.add("op-empty-table");
        if (["0 objects", "No results found", "Нічого не знайдено"].includes(normalize(emptyCell.textContent))) {
          emptyCell.textContent = "За вибраними умовами записів немає";
        }
      }

      if (table.querySelectorAll("thead th").length <= 9) {
        table.classList.add("op-responsive-table");
      }
    });
  }

  function enableKeyboardShortcuts() {
    document.addEventListener("keydown", (event) => {
      if (event.key !== "/" || event.ctrlKey || event.metaKey || event.altKey) return;
      const active = document.activeElement;
      if (active?.matches?.("input, textarea, select, [contenteditable='true']")) return;

      const search = document.querySelector("[data-admin-global-search] input[type='search']");
      if (!search) return;
      event.preventDefault();
      search.focus();
      search.select();
    });
  }

  function start() {
    translate(document);
    enhanceResultTables(document);
    enableKeyboardShortcuts();

    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
          if (node.nodeType === Node.TEXT_NODE && node.parentElement) {
            translate(node.parentElement);
            return;
          }
          translate(node);
          if (node instanceof Element) enhanceResultTables(node);
        });
      });
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true,
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start, { once: true });
  } else {
    start();
  }
})();
