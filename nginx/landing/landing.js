document.querySelectorAll(".site-header").forEach((header) => {
  const button = header.querySelector(".nav-toggle-button");
  const nav = header.querySelector(".site-nav");

  if (!button || !nav) {
    return;
  }

  const closeMenu = () => {
    button.classList.remove("is-open");
    button.setAttribute("aria-expanded", "false");
    nav.classList.remove("is-open");
  };

  button.addEventListener("click", () => {
    const isOpen = button.classList.toggle("is-open");
    button.setAttribute("aria-expanded", String(isOpen));
    nav.classList.toggle("is-open", isOpen);
  });

  nav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", closeMenu);
  });

  document.addEventListener("click", (event) => {
    if (window.innerWidth > 860 || header.contains(event.target)) {
      return;
    }

    closeMenu();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMenu();
    }
  });

  window.addEventListener("resize", () => {
    if (window.innerWidth > 860) {
      closeMenu();
    }
  });
});

const FUNNEL_ATTRIBUTION_FIELDS = [
  "utm_source",
  "utm_medium",
  "utm_campaign",
  "utm_content",
  "utm_term",
  "referral_code",
];

const bootstrapFunnel = async () => {
  const sourceParams = new URLSearchParams(window.location.search);
  const attribution = new URLSearchParams({ landing_variant: "home-v1" });
  FUNNEL_ATTRIBUTION_FIELDS.forEach((field) => {
    const value = sourceParams.get(field);
    if (value) {
      attribution.set(field, value);
    }
  });
  const response = await fetch(`/app/api/funnel/session?${attribution.toString()}`, {
    credentials: "same-origin",
    cache: "no-store",
  });
  if (!response.ok) {
    return null;
  }
  return response.json();
};

const emitFunnelEvent = async (bootstrap, eventName, dimensions = {}, keepalive = false) => {
  if (!bootstrap || !bootstrap.csrf_token) {
    return;
  }
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
      event_id: crypto.randomUUID(),
      funnel_version: bootstrap.funnel_version || "pwa-v1",
      landing_variant: bootstrap.landing_variant || "home-v1",
      ...dimensions,
    }),
  });
};

const funnelBootstrap = bootstrapFunnel();

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
