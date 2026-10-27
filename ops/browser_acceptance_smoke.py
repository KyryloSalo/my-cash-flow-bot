#!/usr/bin/env python
"""Real local PWA acceptance smoke with an isolated synthetic PostgreSQL database."""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

from playwright.sync_api import sync_playwright

SYNTHETIC_DB_USER = "mcf_smoke"
SYNTHETIC_DB_PASSWORD = "mcf_smoke_new_20261018"
SYNTHETIC_DB_NAME = "mcf_smoke"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(url: str, process: subprocess.Popen[str], timeout: float = 45) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"Django server exited early:\n{output}")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status < 500:
                    return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError(f"Django server did not become ready at {url}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--server", action="store_true", help="start an isolated local Django server")
    parser.add_argument("--log", type=Path, help="write the smoke result to this path")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    app_root = root / "admin_service"
    entrypoint = app_root / "miniapp" / "templates" / "miniapp" / "index.html"
    if args.preflight:
        if not (app_root / "manage.py").exists() or not entrypoint.exists():
            raise SystemExit("browser acceptance preflight failed: shipped Django PWA entrypoint is missing")
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            browser.close()
        print("BROWSER_ACCEPTANCE_PREFLIGHT_PASS: Playwright Chromium and shipped PWA entrypoint available")
        return 0
    if not args.server:
        raise SystemExit("choose --preflight or --server")

    run_id = uuid.uuid4().hex[:12]
    container = f"mcfbot-browser-smoke-{run_id}"
    port = free_port()
    server: subprocess.Popen[str] | None = None
    result = ""
    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env.pop("DJANGO_SETTINGS_MODULE", None)
    env.update({
        "DATABASE_URL": f"postgresql://{SYNTHETIC_DB_USER}:{SYNTHETIC_DB_PASSWORD}@127.0.0.1:{{port}}/{SYNTHETIC_DB_NAME}",
        "DJANGO_DEBUG": "true",
        "DJANGO_SECRET_KEY": f"browser-smoke-only-{run_id}",
        "DJANGO_ALLOWED_HOSTS": "127.0.0.1,localhost",
        "MINIAPP_DEV_TG_USER_ID": "777777",
        "SECURE_SSL_REDIRECT": "false",
        "SESSION_COOKIE_SECURE": "false",
        "CSRF_COOKIE_SECURE": "false",
        "REDIS_URL": "redis://127.0.0.1:63999/0",
    })
    try:
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", container,
             "-e", f"POSTGRES_USER={SYNTHETIC_DB_USER}", "-e", f"POSTGRES_PASSWORD={SYNTHETIC_DB_PASSWORD}",
             "-e", f"POSTGRES_DB={SYNTHETIC_DB_NAME}", "-p", "127.0.0.1::5432", "postgres:16-alpine"],
            check=True, capture_output=True, text=True,
        )
        host_port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = env["DATABASE_URL"].format(port=host_port)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            ready = subprocess.run(["docker", "exec", container, "pg_isready", "-U", SYNTHETIC_DB_USER, "-d", SYNTHETIC_DB_NAME], capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("isolated PostgreSQL container did not become ready")
        subprocess.run([sys.executable, "manage.py", "migrate", "--noinput"], cwd=app_root, env=env, check=True)
        server = subprocess.Popen(
            [sys.executable, "manage.py", "runserver", f"127.0.0.1:{port}", "--noreload"],
            cwd=app_root, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, text=True,
        )
        base_url = f"http://127.0.0.1:{port}/app"
        wait_for(base_url + "/", server)
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            console_errors: list[str] = []
            page_errors: list[str] = []
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" and not msg.text.startswith("Failed to load resource:") else None)
            page.on("pageerror", lambda exc: page_errors.append(str(exc)))
            page.goto(base_url + "/", wait_until="domcontentloaded")
            assert page.locator('link[rel="manifest"]').count() == 1
            assert page.locator("#loadingState").count() == 1
            dev_auth_url = page.locator("body").get_attribute("data-dev-auth-url")
            page.evaluate("url => fetch(url, {method: 'POST'}).then(r => { if (!r.ok) throw new Error('dev auth ' + r.status); return r.json(); })", dev_auth_url)
            page.reload(wait_until="networkidle")
            page.locator("#onboardingShell").wait_for(state="visible", timeout=15000)
            assert page.locator("#dashboardApp").count() == 1
            assert page.locator('nav[aria-label="Навігація"]').count() == 1
            assert page.locator('nav[aria-label="Навігація"] button').count() >= 1
            assert page.request.get(base_url + "/manifest.webmanifest").ok
            assert page.request.get(base_url + "/sw.js").ok
            assert not console_errors, f"browser console errors: {console_errors}"
            assert not page_errors, f"browser page errors: {page_errors}"
            browser.close()
        result = "REAL_PWA_SMOKE_PASS: /app shell, isolated dev-auth dashboard, onboarding, navigation, manifest, service worker"
        print(result)
        return 0
    finally:
        if server:
            server.terminate()
            server.wait(timeout=10)
        subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True, text=True)
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            args.log.write_text(result + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
