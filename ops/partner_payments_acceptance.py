#!/usr/bin/env python
"""Exercise partner reports in Chromium against a disposable local PostgreSQL.

Never uses the configured application database or real payment providers.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from playwright.sync_api import sync_playwright


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(url, process):
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError("Acceptance server exited; inspect server.log")
        try:
            urllib.request.urlopen(url, timeout=1).close()
            return
        except urllib.error.HTTPError as exc:
            if exc.code < 500:
                return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.25)
    raise RuntimeError("Acceptance server did not become ready")


def browser_acceptance(base, output):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            # Admin fonts can be external; acceptance does not depend on CDN access.
            page.route("**/*", lambda route: route.continue_() if route.request.url.startswith(base) else route.abort())
            response = page.goto(base + "/partner-payments/", wait_until="domcontentloaded")
            assert response.status == 200 and "/login/" in page.url
            page.goto(base + "/login/?next=/partner-payments/", wait_until="domcontentloaded")
            page.fill('input[name="username"]', "accept")
            page.fill('input[name="password"]', "accept-pass-2026")
            page.locator('form button[type="submit"], form input[type="submit"]').first.click()
            page.wait_for_url("**/partner-payments/")
            page.locator('input[name="start"]').fill("2026-01-01")
            page.locator('input[name="end"]').fill("2026-02-01")
            page.select_option('select[name="grouping"]', "month")
            page.select_option('select[name="partner"]', label="Browser Partner (browser-partner)")
            page.get_by_role("button", name="Показати", exact=True).click()
            page.wait_for_load_state("domcontentloaded")
            detail = page.locator("#payment-details")
            assert detail.locator("tbody tr").count() == 4
            body = detail.inner_text()
            assert all(label in body for label in ["accept-start", "accept-jan", "accept-end", "accept-eur"])
            assert all(label not in body for label in ["accept-out", "accept-before", "accept-failed", "accept-other"])
            summary = page.locator("#partner-summary").inner_text().replace(",", ".").replace("\u00a0", " ")
            assert "28.00 USD" in summary and "7.00 EUR" in summary and "4" in summary, summary
            assert page.locator('select[name="partner"]').input_value() != ""
            assert page.locator('select[name="partner"] option').count() == 4
            for grouping in ("day", "week", "month"):
                page.select_option('select[name="grouping"]', grouping)
                page.get_by_role("button", name="Показати", exact=True).click()
                page.wait_for_load_state("domcontentloaded")
                assert page.locator("#payment-details tbody tr").count() == 4
                assert "28.00 USD" in page.locator("#partner-summary").inner_text().replace(",", ".")
            page.screenshot(path=str(output / "partner-payments.png"), full_page=True)
            page.select_option('select[name="partner"]', label="Empty Partner (empty-partner)")
            page.get_by_role("button", name="Показати", exact=True).click()
            page.wait_for_load_state("domcontentloaded")
            assert "Немає оплат" in page.locator("#payment-details").inner_text()
            # A staff member with only one of the required permissions cannot read it.
            limited = browser.new_context()
            limited_page = limited.new_page()
            limited_page.goto(base + "/login/?next=/partner-payments/", wait_until="domcontentloaded")
            limited_page.fill('input[name="username"]', "limited")
            limited_page.fill('input[name="password"]', "accept-pass-2026")
            limited_page.locator('form button[type="submit"], form input[type="submit"]').first.click()
            limited_page.wait_for_load_state("domcontentloaded")
            response = limited_page.goto(base + "/partner-payments/", wait_until="domcontentloaded")
            assert response.status == 403, response.status
            limited.close()
        finally:
            browser.close()


def main(*, seed_module="ops.partner_payments_acceptance_support", browser_scenario=browser_acceptance, default_tests=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unit-only", action="store_true")
    parser.add_argument("--tests", nargs="+", default=default_tests or ["dashboard.tests_partner_payments", "miniapp.tests_partner_attribution"])
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    app = root / "admin_service"
    run = uuid.uuid4().hex[:10]
    scratch = Path.home() / "AppData/Local/hermes/cache/scratch" if os.name == "nt" else Path(os.environ.get("TMPDIR", tempfile.gettempdir()))
    scratch.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="partner-payments-", dir=scratch))
    container = f"mcf-partner-accept-{run}"
    server = None
    env = os.environ.copy()
    env.update(DJANGO_DEBUG="true", DJANGO_SECRET_KEY=f"accept-{run}", DJANGO_ALLOWED_HOSTS="127.0.0.1,localhost,testserver", SECURE_SSL_REDIRECT="false", SESSION_COOKIE_SECURE="false", CSRF_COOKIE_SECURE="false", REDIS_URL="redis://127.0.0.1:63999/0", TZ="Europe/Kyiv")
    def manage(*commands):
        subprocess.run([sys.executable, "manage.py", *commands], cwd=app, env=env, check=True)
    try:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", container, "-e", "POSTGRES_USER=mcf", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=mcf", "-p", "127.0.0.1::5432", "postgres:16-alpine"], check=True, capture_output=True)
        host_port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = f"postgresql://mcf@127.0.0.1:{host_port}/mcf"
        for _ in range(120):
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "mcf", "-d", "mcf"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("Disposable PostgreSQL did not become ready")
        manage("check")
        manage("makemigrations", "--check", "--dry-run")
        manage("test", *args.tests, "--noinput", "--verbosity", "1")
        if not args.unit_only:
            manage("migrate", "--noinput", "--verbosity", "0")
            manage("shell", "-c", f"import sys; sys.path.insert(0, {str(root)!r}); from {seed_module} import seed; seed()")
            web_port = free_port()
            with (output / "server.log").open("w", encoding="utf-8") as log:
                server = subprocess.Popen([sys.executable, "manage.py", "runserver", f"127.0.0.1:{web_port}", "--noreload"], cwd=app, env=env, stdout=log, stderr=subprocess.STDOUT)
                base = f"http://127.0.0.1:{web_port}"
                wait_ready(base + "/login/", server)
                browser_scenario(base, output)
            print("PARTNER_ACCEPTANCE_PASS: real PostgreSQL migrations, regression tests and Chromium scenario completed")
        else:
            print("PARTNER_PAYMENT_UNIT_PASS")
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        print(f"Evidence directory: {output}")


if __name__ == "__main__":
    main()