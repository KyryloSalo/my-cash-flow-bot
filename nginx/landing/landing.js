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
