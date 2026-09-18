(() => {
  const header = document.querySelector("[data-site-header]");
  const navToggle = document.querySelector("[data-nav-toggle]");
  const siteNav = document.querySelector("[data-site-nav]");
  const scrollProgress = document.querySelector("[data-scroll-progress]");
  const mobileStickyCta = document.querySelector(".mobile-sticky-cta");
  const hero = document.querySelector(".hero");
  const footer = document.querySelector(".site-footer");
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

  const closeNavigation = () => {
    if (!navToggle || !siteNav) return;
    navToggle.setAttribute("aria-expanded", "false");
    siteNav.classList.remove("is-open");
    document.body.classList.remove("nav-open");
  };

  if (navToggle && siteNav) {
    navToggle.addEventListener("click", () => {
      const willOpen = navToggle.getAttribute("aria-expanded") !== "true";
      navToggle.setAttribute("aria-expanded", String(willOpen));
      siteNav.classList.toggle("is-open", willOpen);
      document.body.classList.toggle("nav-open", willOpen);
    });

    siteNav.querySelectorAll("a").forEach((link) => link.addEventListener("click", closeNavigation));
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closeNavigation();
    });
    window.addEventListener("resize", () => {
      if (window.innerWidth > 900) closeNavigation();
    });
  }

  let scrollableHeight = 1;
  let stickyStart = 480;
  let stickyEnd = Number.POSITIVE_INFINITY;

  const updateScrollMetrics = () => {
    scrollableHeight = Math.max(document.documentElement.scrollHeight - window.innerHeight, 1);
    if (hero) stickyStart = Math.max(480, hero.offsetTop + hero.offsetHeight - 240);
    if (footer) stickyEnd = footer.offsetTop + 72;
  };

  const updateScrollUi = () => {
    header?.classList.toggle("is-scrolled", window.scrollY > 12);

    if (scrollProgress) {
      const progress = Math.min(Math.max(window.scrollY / scrollableHeight, 0), 1);
      scrollProgress.style.transform = `scaleX(${progress})`;
    }

    if (mobileStickyCta && hero && footer) {
      const beforeFooter = window.scrollY + window.innerHeight < stickyEnd;
      const isVisible = window.innerWidth <= 720 && window.scrollY > stickyStart && beforeFooter;
      mobileStickyCta.classList.toggle("is-visible", isVisible);
    }
  };

  const refreshScrollUi = () => {
    updateScrollMetrics();
    updateScrollUi();
  };

  refreshScrollUi();
  window.addEventListener("scroll", updateScrollUi, { passive: true });
  window.addEventListener("resize", refreshScrollUi, { passive: true });
  window.addEventListener("load", refreshScrollUi, { once: true });

  document.querySelectorAll("[data-current-year]").forEach((node) => {
    node.textContent = String(new Date().getFullYear());
  });

  const revealGroups = [
    ".trust-grid > div",
    ".problem-grid > *",
    ".demo-section .section-heading > *",
    ".product-tour > *",
    ".demo-section .section-cta",
    ".outcomes-section .split-heading > *",
    ".outcome-card",
    ".features-section .section-heading > *",
    ".feature-group",
    ".family-grid > *",
    ".fit-card > *",
    ".pricing-grid > *",
    ".trust-section .split-heading > *",
    ".trust-cards article",
    ".trust-links a",
    ".faq-grid > *",
    ".faq-list details",
    ".final-card > *",
    ".footer-grid > *",
  ];

  if (!reduceMotion.matches && "IntersectionObserver" in window) {
    const targets = [];
    const seen = new Set();

    revealGroups.forEach((selector) => {
      document.querySelectorAll(selector).forEach((node, index) => {
        if (seen.has(node)) return;
        seen.add(node);
        node.dataset.reveal = "";
        node.style.setProperty("--reveal-delay", `${Math.min(index * 65, 195)}ms`);
        targets.push(node);
      });
    });

    const revealObserver = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (!entry.isIntersecting) return;
        entry.target.classList.add("is-visible");
        revealObserver.unobserve(entry.target);
      });
    }, { rootMargin: "0px 0px -9% 0px", threshold: 0.08 });

    document.documentElement.classList.add("motion-ready");
    window.requestAnimationFrame(() => targets.forEach((target) => revealObserver.observe(target)));
  }

  const demoRoot = document.querySelector("[data-product-demo]");
  if (demoRoot) {
    const buttons = [...demoRoot.querySelectorAll("[data-product-screen]")];
    const image = demoRoot.querySelector("[data-tour-image]");
    const title = demoRoot.querySelector("[data-tour-title]");
    const text = demoRoot.querySelector("[data-tour-text]");
    const step = demoRoot.querySelector("[data-tour-step]");
    const next = demoRoot.querySelector("[data-tour-next]");
    const message = demoRoot.querySelector("[data-demo-message]");
    let activeIndex = 0;

    buttons.forEach((button) => {
      const preload = new Image();
      preload.src = button.dataset.productSrc;
    });

    const renderScreen = (index) => {
      if (!buttons.length) return;
      const previousIndex = activeIndex;
      activeIndex = (index + buttons.length) % buttons.length;
      const activeButton = buttons[activeIndex];
      buttons.forEach((button) => {
        const isActive = button === activeButton;
        button.classList.toggle("is-active", isActive);
        button.setAttribute("aria-pressed", String(isActive));
      });
      if (image) {
        image.src = activeButton.dataset.productSrc;
        image.alt = activeButton.dataset.productAlt || "Екран Vydno";
      }
      if (title) title.textContent = activeButton.dataset.productTitle;
      if (text) text.textContent = activeButton.dataset.productText;
      if (step) step.textContent = `Крок ${activeIndex + 1} з ${buttons.length}`;
      if (next) next.firstChild.textContent = activeIndex === buttons.length - 1
        ? "Повернутися на початок "
        : "Наступний екран ";
      if (message) {
        message.textContent = activeIndex === 0
          ? "Записи створюються лише після окремого підтвердження."
          : "Після підтвердження запис одразу з’являється в огляді та аналітиці.";
      }
      if (previousIndex !== activeIndex && !reduceMotion.matches) {
        image?.animate([
          { opacity: 0.24, transform: "translateY(10px) scale(0.995)" },
          { opacity: 1, transform: "translateY(0) scale(1)" },
        ], { duration: 420, easing: "cubic-bezier(0.22, 1, 0.36, 1)" });
        [title, text, step].forEach((node, nodeIndex) => node?.animate([
          { opacity: 0, transform: "translateY(7px)" },
          { opacity: 1, transform: "translateY(0)" },
        ], {
          duration: 340,
          delay: nodeIndex * 35,
          easing: "cubic-bezier(0.22, 1, 0.36, 1)",
          fill: "both",
        }));
      }
      emitDemoEvent("product_demo_start", `product-tour-${activeButton.dataset.productScreen}`);
      if (activeIndex === buttons.length - 1) {
        emitDemoEvent("product_demo_complete", "product-tour-analytics");
      }
    };

    buttons.forEach((button, index) => {
      button.addEventListener("click", () => renderScreen(index));
    });
    next?.addEventListener("click", () => renderScreen(activeIndex + 1));
  }

  const FUNNEL_ATTRIBUTION_FIELDS = [
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "utm_term",
    "referral_code",
  ];

  const isFunnelAvailable = window.location.protocol === "https:" || window.location.hostname === "localhost";
  const randomId = () => {
    if (window.crypto?.randomUUID) return window.crypto.randomUUID();
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  };

  const bootstrapFunnel = async () => {
    if (!isFunnelAvailable) return null;
    const sourceParams = new URLSearchParams(window.location.search);
    const attribution = new URLSearchParams({ landing_variant: "calm-ledger-v1" });
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
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": bootstrap.csrf_token,
      },
      body: JSON.stringify({
        event_name: eventName,
        event_id: randomId(),
        funnel_version: bootstrap.funnel_version || "pwa-v1",
        landing_variant: bootstrap.landing_variant || "calm-ledger-v1",
        ...dimensions,
      }),
    });
  };

  const funnelBootstrap = bootstrapFunnel().catch(() => null);

  const emitDemoEvent = (eventName, placement) => {
    funnelBootstrap
      .then((bootstrap) => emitFunnelEvent(bootstrap, eventName, { placement }))
      .catch(() => undefined);
  };

  funnelBootstrap
    .then((bootstrap) => emitFunnelEvent(bootstrap, "landing_view"))
    .catch(() => undefined);

  document.querySelectorAll("[data-funnel-cta]").forEach((link) => {
    link.addEventListener("click", () => {
      funnelBootstrap
        .then((bootstrap) => emitFunnelEvent(
          bootstrap,
          "landing_primary_cta_click",
          { placement: link.dataset.funnelCta || "unknown" },
          true,
        ))
        .catch(() => undefined);
    });
  });
})();
