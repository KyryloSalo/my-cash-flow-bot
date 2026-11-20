"""Browser acceptance for the local read-only audit dashboard, never the finance PWA."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path


def verify_browser(here: Path, root: Path) -> dict:
    from playwright.sync_api import Error, expect, sync_playwright

    register = json.loads((here / "register.json").read_text(encoding="utf-8"))
    session = os.environ.get("HERMES_SESSION_ID", "local")
    assert re.fullmatch(r"[A-Za-z0-9_-]+", session), "Unsafe session ID"
    evidence = root / ".hermes" / "delivery" / session / "project-health"
    evidence.mkdir(parents=True, exist_ok=True)
    errors: list[str] = []
    screenshots: list[str] = []
    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch(headless=True)
            engine = "bundled-chromium"
        except Error:
            browser = playwright.chromium.launch(channel="msedge", headless=True)
            engine = "msedge"
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
            page.goto((here / "dashboard.html").as_uri(), wait_until="networkidle")
            expect(page.get_by_role("heading", name="Що справді доведено?")).to_be_visible()
            expect(page.locator("#rows tr")).to_have_count(len(register["backlog"]))
            page.get_by_label("Пріоритет").select_option("P0")
            p0 = sum(row["priority"] == "P0" for row in register["backlog"])
            expect(page.locator("#rows tr")).to_have_count(p0)
            page.get_by_label("Пріоритет").select_option("")
            page.get_by_label("Знайти ID, сценарій чи вимогу").fill("normal transaction")
            expect(page.locator("#rows tr")).to_have_count(1)
            page.locator("#rows summary").click()
            expect(page.locator("#rows details")).to_have_attribute("open", "")
            page.get_by_label("Знайти ID, сценарій чи вимогу").fill("no-match-4d90c7")
            expect(page.locator("#rows tr")).to_have_count(0)
            expect(page.locator("#empty")).to_be_visible()
            page.get_by_label("Знайти ID, сценарій чи вимогу").fill("")
            page.get_by_role("button", name="Сценарії").click()
            expect(page.locator("#rows tr")).to_have_count(len(register["flows"]))
            page.get_by_role("button", name="Знахідки").click()
            expect(page.locator("#rows tr")).to_have_count(len(register["findings"]))
            expect(page.locator("#decisions article")).to_have_count(len(register["decisions"]))
            for index, decision in enumerate(register["decisions"]):
                expect(page.locator("#decisions article").nth(index)).to_contain_text(decision["status"])
            expect(page.get_by_role("link", name="Рекомендовані рішення")).to_have_attribute("href", "DECISIONS.md")
            expect(page.get_by_role("link", name="Наступний acceptance")).to_have_attribute("href", "HEALTH-05.md")
            page.get_by_role("button", name="Беклог").click()
            desktop = evidence / "dashboard-desktop.png"
            page.screenshot(path=str(desktop), full_page=True)
            screenshots.append(desktop.relative_to(root).as_posix())
            page.set_viewport_size({"width": 390, "height": 844})
            expect(page.get_by_label("Знайти ID, сценарій чи вимогу")).to_be_visible()
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), "Mobile page overflow"
            mobile = evidence / "dashboard-mobile.png"
            page.screenshot(path=str(mobile), full_page=True)
            screenshots.append(mobile.relative_to(root).as_posix())
            assert not errors, errors
        finally:
            browser.close()
    report = {
        "status": "passed", "engine": engine, "console_errors": errors,
        "checks": ["initial_rows", "priority_filter", "search", "details", "empty_state",
                   "flow_tab", "findings_tab", "decision_cards", "decision_statuses", "next_step_links", "mobile_no_overflow"],
        "screenshots": screenshots, "scope": "local_read_only_audit_dashboard",
    }
    (evidence / "browser.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report
