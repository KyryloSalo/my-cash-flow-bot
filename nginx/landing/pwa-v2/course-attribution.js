(() => {
  const COURSE_KEY = "vydno_course_attribution";
  const MAX_STORAGE_VALUE = 32;
  const COURSE_VALUE = "course";

  const safeStorage = () => {
    try {
      const storage = window.localStorage;
      const probe = `${COURSE_KEY}_probe`;
      storage.setItem(probe, "1");
      storage.removeItem(probe);
      return storage;
    } catch { return null; }
  };

  const storage = safeStorage();
  const params = new URLSearchParams(window.location.search);
  let course = params.get("start") === COURSE_VALUE;
  if (course && storage) {
    try { storage.setItem(COURSE_KEY, COURSE_VALUE); } catch { /* no-storage path */ }
  } else if (storage) {
    try { course = storage.getItem(COURSE_KEY)?.slice(0, MAX_STORAGE_VALUE) === COURSE_VALUE; } catch { /* no-storage path */ }
  }
  if (!course) return;

  document.querySelectorAll("a[href]").forEach((link) => {
    try {
      const url = new URL(link.href, window.location.href);
      const sameOriginStart = url.origin === window.location.origin && url.pathname === "/app/auth/telegram/start";
      const sameOriginFunnel = url.origin === window.location.origin && /^(?:\/|\/pwa-v2\/(?:index\.html|quiz\/(?:index\.html)?)?)$/.test(url.pathname);
      const telegramStart = url.hostname === "t.me" && url.pathname.includes("vydnocapital_bot");
      if (!sameOriginStart && !sameOriginFunnel && !telegramStart) return;
      url.searchParams.set("start", COURSE_VALUE);
      link.href = url.toString();
    } catch { /* ignore malformed links */ }
  });
})();
