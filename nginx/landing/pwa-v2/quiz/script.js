(() => {
  const form = document.querySelector("[data-quiz-form]");
  const questions = [...document.querySelectorAll("[data-question]")];
  const nextButton = document.querySelector("[data-next]");
  const backButton = document.querySelector("[data-back]");
  const validation = document.querySelector("[data-validation]");
  const progressWrap = document.querySelector("[data-progress-wrap]");
  const progressLabel = document.querySelector("[data-progress-label]");
  const progressPercent = document.querySelector("[data-progress-percent]");
  const progressTrack = document.querySelector("[data-progress-track]");
  const progressBar = document.querySelector("[data-progress-bar]");
  const resultScreen = document.querySelector("[data-result]");
  const restartButton = document.querySelector("[data-restart]");
  let currentIndex = 0;
  let resultType = "simple";

  const preferredScrollBehavior = () => window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches
    ? "auto"
    : "smooth";

  const recommendations = {
    simple: {
      kicker: "Низька складність",
      title: "Вам може вистачити простішої системи.",
      lead: "Схоже, ваші гроші зосереджені в небагатьох місцях, а поточний банківський огляд уже відповідає на більшість запитань.",
      fit: "Vydno не обов’язковий, якщо вам достатньо балансу одного рахунку й майже немає готівки, боргів, валют або спільних фінансів.",
      next: "Якщо хочете перевірити, чи голосовий ввід, скріншоти й окремі цілі все ж зроблять облік зручнішим, повний доступ на 30 днів коштує 1 грн.",
      cta: "Перевірити Vydno за 1 грн",
    },
    guided: {
      kicker: "Є практичний fit",
      title: "Vydno може прибрати головну перешкоду — регулярність.",
      lead: "Вам не бракує бажання розуміти фінанси. Слабке місце — шлях від реальної оплати до запису, який легко відкласти на потім.",
      fit: "Найкорисніший сценарій для вас: додавати операції голосом, текстом або зі скріншота, перевіряти готову чернетку й одразу бачити оновлений огляд.",
      next: "Почніть не з налаштування ідеальної системи, а з кількох реальних операцій. За місяць стане зрозуміло, чи формується звичка без зайвої рутини.",
      cta: "Спробувати 30 днів за 1 грн",
    },
    complex: {
      kicker: "Високий fit",
      title: "Вам потрібна одна картина, а не ще одна таблиця.",
      lead: "Кілька рахунків, джерел доходу, валют або довгострокових цілей створюють контекст, якого не видно в одному банківському застосунку.",
      fit: "Vydno найбільше допоможе як єдиний фінансовий огляд: окремі рахунки та операції залишаються деталями, але складаються в зрозумілий баланс, динаміку, цілі й зобов’язання.",
      next: "Створіть свої реальні рахунки, додайте кілька типових операцій і перевірте, чи відповідає огляд на головне запитання: куди йдуть гроші та що можна зробити далі.",
      cta: "Зібрати свою картину за 1 грн",
    },
    family: {
      kicker: "Сімейний fit",
      title: "Найбільша цінність для вас — спільний фінансовий контекст.",
      lead: "Коли рішення й витрати розподілені між двома людьми, окремі банківські історії дають лише частину картини.",
      fit: "Сімейний простір Vydno дає спільний огляд рахунків, операцій, боргів і цілей. Запрошення та перехід у цей режим відбуваються лише після явної дії користувача.",
      next: "Спочатку налаштуйте власний простір, потім запросіть партнера й домовтеся, що саме ви хочете бачити разом. Перші 30 днів дадуть час перевірити цей сценарій без довгого зобов’язання.",
      cta: "Спробувати сімейний простір за 1 грн",
    },
  };

  const activeQuestion = () => questions[currentIndex];

  const updateProgress = () => {
    const number = currentIndex + 1;
    const percent = Math.round((number / questions.length) * 100);
    progressLabel.textContent = `Питання ${number} із ${questions.length}`;
    progressPercent.textContent = `${percent}%`;
    progressTrack.setAttribute("aria-valuenow", String(number));
    progressBar.style.width = `${percent}%`;
    backButton.disabled = currentIndex === 0;
    nextButton.innerHTML = currentIndex === questions.length - 1
      ? "Показати результат <span aria-hidden=\"true\">→</span>"
      : "Далі <span aria-hidden=\"true\">→</span>";
  };

  const showQuestion = (index, shouldFocus = true) => {
    currentIndex = Math.max(0, Math.min(index, questions.length - 1));
    questions.forEach((question, questionIndex) => {
      const isActive = questionIndex === currentIndex;
      question.hidden = !isActive;
      question.classList.toggle("is-active", isActive);
      const legend = question.querySelector("legend");
      if (legend) {
        if (isActive) legend.id = "question-title";
        else legend.removeAttribute("id");
      }
    });
    validation.hidden = true;
    updateProgress();
    if (shouldFocus) {
      const legend = activeQuestion().querySelector("legend");
      legend?.setAttribute("tabindex", "-1");
      legend?.focus({ preventScroll: true });
      activeQuestion().scrollIntoView({ behavior: preferredScrollBehavior(), block: "center" });
    }
  };

  const getSelected = (question) => question.querySelector("input[type=radio]:checked");

  const getAnswerText = (input) => {
    const label = input.closest("label");
    return label?.querySelector("strong")?.textContent?.trim() || input.value;
  };

  const classify = (answers) => {
    const totalScore = answers.reduce((sum, input) => sum + Number(input.dataset.score || 0), 0);
    const familyScore = answers.reduce((sum, input) => sum + Number(input.dataset.family || 0), 0);
    if (familyScore >= 4) return "family";
    if (totalScore >= 13) return "complex";
    if (totalScore >= 7) return "guided";
    return "simple";
  };

  const renderResult = () => {
    const answers = questions.map((question) => getSelected(question)).filter(Boolean);
    resultType = classify(answers);
    const recommendation = recommendations[resultType];

    document.querySelector("[data-result-kicker]").textContent = recommendation.kicker;
    document.querySelector("[data-result-title]").textContent = recommendation.title;
    document.querySelector("[data-result-lead]").textContent = recommendation.lead;
    document.querySelector("[data-result-fit]").innerHTML = `<strong>Що це означає</strong>${recommendation.fit}`;
    document.querySelector("[data-result-next]").textContent = recommendation.next;

    const cta = document.querySelector("[data-result-cta]");
    cta.innerHTML = `${recommendation.cta} <span aria-hidden="true">→</span>`;

    const summary = document.querySelector("[data-answer-summary]");
    summary.replaceChildren();
    answers.forEach((answer) => {
      const item = document.createElement("li");
      item.textContent = getAnswerText(answer);
      summary.appendChild(item);
    });

    form.hidden = true;
    progressWrap.hidden = true;
    resultScreen.hidden = false;
    resultScreen.focus?.({ preventScroll: true });
    resultScreen.scrollIntoView({ behavior: preferredScrollBehavior(), block: "start" });
  };

  nextButton?.addEventListener("click", () => {
    const selected = getSelected(activeQuestion());
    if (!selected) {
      validation.hidden = false;
      activeQuestion().querySelector("label")?.focus?.();
      return;
    }
    if (currentIndex === questions.length - 1) renderResult();
    else showQuestion(currentIndex + 1);
  });

  backButton?.addEventListener("click", () => {
    if (currentIndex > 0) showQuestion(currentIndex - 1);
  });

  questions.forEach((question) => {
    question.querySelectorAll("input[type=radio]").forEach((input) => {
      input.addEventListener("change", () => {
        validation.hidden = true;
      });
    });
  });

  document.addEventListener("keydown", (event) => {
    if (resultScreen && !resultScreen.hidden) return;
    if (!["a", "b", "c", "A", "B", "C"].includes(event.key)) return;
    const index = { a: 0, b: 1, c: 2 }[event.key.toLowerCase()];
    const input = activeQuestion()?.querySelectorAll("input[type=radio]")[index];
    if (input) {
      input.checked = true;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
  });

  restartButton?.addEventListener("click", () => {
    form.reset();
    resultScreen.hidden = true;
    form.hidden = false;
    progressWrap.hidden = false;
    showQuestion(0);
  });

  document.querySelectorAll("[data-current-year]").forEach((node) => {
    node.textContent = String(new Date().getFullYear());
  });

  const FUNNEL_ATTRIBUTION_FIELDS = ["utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term", "referral_code"];
  const isFunnelAvailable = window.location.protocol === "https:" || window.location.hostname === "localhost";
  const randomId = () => window.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const bootstrapFunnel = async () => {
    if (!isFunnelAvailable) return null;
    const sourceParams = new URLSearchParams(window.location.search);
    const attribution = new URLSearchParams({ landing_variant: "calm-ledger-quiz-v1" });
    FUNNEL_ATTRIBUTION_FIELDS.forEach((field) => {
      const value = sourceParams.get(field);
      if (value) attribution.set(field, value);
    });
    const response = await fetch(`/app/api/funnel/session?${attribution.toString()}`, {
      credentials: "same-origin",
      cache: "no-store",
    });
    if (!response.ok) return null;
    return response.json();
  };

  const emitFunnelEvent = async (bootstrap, eventName, dimensions = {}, keepalive = false) => {
    if (!bootstrap?.csrf_token) return;
    await fetch("/app/api/funnel/event", {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      keepalive,
      headers: { "Content-Type": "application/json", "X-CSRFToken": bootstrap.csrf_token },
      body: JSON.stringify({
        event_name: eventName,
        event_id: randomId(),
        funnel_version: bootstrap.funnel_version || "pwa-v1",
        landing_variant: bootstrap.landing_variant || "calm-ledger-quiz-v1",
        ...dimensions,
      }),
    });
  };

  const funnelBootstrap = bootstrapFunnel().catch(() => null);
  funnelBootstrap.then((bootstrap) => emitFunnelEvent(bootstrap, "landing_view")).catch(() => undefined);

  document.querySelector("[data-result-cta]")?.addEventListener("click", () => {
    funnelBootstrap
      .then((bootstrap) => emitFunnelEvent(
        bootstrap,
        "landing_primary_cta_click",
        { placement: "quiz-result" },
        true,
      ))
      .catch(() => undefined);
  });

  showQuestion(0, false);
})();
