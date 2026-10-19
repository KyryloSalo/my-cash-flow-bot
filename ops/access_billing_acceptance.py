#!/usr/bin/env python
"""Browser acceptance for cardless administrator grants and self-service card gating."""
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

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

SYNTHETIC_DB_USER = "mcf_access_acceptance"
SYNTHETIC_DB_PASSWORD = "mcf_access_acceptance_local"
SYNTHETIC_DB_NAME = "mcf_access_acceptance"
DEV_TG_USER_ID = "288834786"


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


def manage(app_root: Path, env: dict[str, str], *args: str) -> None:
    subprocess.run([sys.executable, "manage.py", *args], cwd=app_root, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true", help="run against an isolated local Django/PostgreSQL stack")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    if not args.server:
        raise SystemExit("choose --server")

    root = Path(__file__).resolve().parents[1]
    app_root = root / "admin_service"
    run_id = uuid.uuid4().hex[:12]
    container = f"mcfbot-access-acceptance-{run_id}"
    port = free_port()
    server: subprocess.Popen[str] | None = None
    result = ""
    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env.pop("DJANGO_SETTINGS_MODULE", None)
    env.update(
        {
            "DATABASE_URL": f"postgresql://{SYNTHETIC_DB_USER}:{SYNTHETIC_DB_PASSWORD}@127.0.0.1:{{port}}/{SYNTHETIC_DB_NAME}",
            "DJANGO_DEBUG": "true",
            "DJANGO_SECRET_KEY": f"access-acceptance-only-{run_id}",
            "DJANGO_ALLOWED_HOSTS": "127.0.0.1,localhost",
            "MINIAPP_DEV_TG_USER_ID": DEV_TG_USER_ID,
            "SECURE_SSL_REDIRECT": "false",
            "SESSION_COOKIE_SECURE": "false",
            "CSRF_COOKIE_SECURE": "false",
            "REDIS_URL": "redis://127.0.0.1:63999/0",
        }
    )
    try:
        subprocess.run(
            [
                "docker", "run", "-d", "--rm", "--name", container,
                "-e", f"POSTGRES_USER={SYNTHETIC_DB_USER}",
                "-e", f"POSTGRES_PASSWORD={SYNTHETIC_DB_PASSWORD}",
                "-e", f"POSTGRES_DB={SYNTHETIC_DB_NAME}",
                "-p", "127.0.0.1::5432", "postgres:16-alpine",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        host_port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = env["DATABASE_URL"].format(port=host_port)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-U", SYNTHETIC_DB_USER, "-d", SYNTHETIC_DB_NAME],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("isolated PostgreSQL container did not become ready")

        manage(app_root, env, "migrate", "--noinput")
        seed = (
            "from django.utils import timezone; "
            "from accounts.models import Account; "
            "from categories.models import Category; "
            "from users.models import TelegramUser; "
            "from subscriptions.services import apply_subscription_change; "
            f"u=TelegramUser.objects.create(tg_user_id={DEV_TG_USER_ID}, first_name='Access acceptance', "
            "base_currency='UAH', lang='uk', start_date=timezone.localdate(), onboarding_completed=True, onboarding_version=3); "
            "Account.objects.create(tg_user=u, label='Main', currency='UAH', account_type='cash', starting_balance=0, balance=0); "
            "Category.objects.create(user=u, type='income', name='Salary', slug='salary', source='acceptance'); "
            "Category.objects.create(user=u, type='expense', name='Other', slug='other', source='acceptance'); "
            "apply_subscription_change(user=u, action='manual', days=30, admin_user=None)"
        )
        manage(app_root, env, "shell", "-c", seed)
        server = subprocess.Popen(
            [sys.executable, "manage.py", "runserver", f"127.0.0.1:{port}", "--noreload"],
            cwd=app_root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        base_url = f"http://127.0.0.1:{port}/app"
        wait_for(base_url + "/", server)

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.goto(base_url + "/", wait_until="domcontentloaded")
            dev_auth_url = page.locator("body").get_attribute("data-dev-auth-url")
            page.evaluate(
                "url => fetch(url, {method: 'POST'}).then(r => { if (!r.ok) throw new Error('dev auth ' + r.status); return r.json(); })",
                dev_auth_url,
            )
            settings = page.evaluate("url => fetch(url).then(r => r.json())", base_url + "/api/settings")
            access = settings["access"]
            assert access["subscription_status"] == "manual"
            assert access["access_scope"] == "personal_full"
            assert access["mode"] == "active"
            assert access["has_card"] is False
            assert page.locator("#cardBindingModal").is_hidden()

            self_service = (
                "from subscriptions.models import Subscription; "
                f"s=Subscription.objects.filter(user_id={DEV_TG_USER_ID}).latest('created_at'); "
                "s.status=Subscription.Status.TRIAL; s.source=Subscription.Source.PROMO; s.provider='monobank'; "
                "s.save(update_fields=['status','source','provider'])"
            )
            manage(app_root, env, "shell", "-c", self_service)
            page = context.new_page()
            page.goto(base_url + "/", wait_until="domcontentloaded")
            settings = page.evaluate("url => fetch(url).then(r => r.json())", base_url + "/api/settings")
            access = settings["access"]
            assert access["subscription_status"] == "trial"
            assert access["access_scope"] == "paywall"
            assert access["mode"] == "blocked"
            assert access["has_card"] is False
            try:
                page.locator("#cardBindingModal").wait_for(state="visible", timeout=15000)
            except PlaywrightTimeoutError:
                state = page.evaluate(
                    """() => ({
                      ready: window.vydnoAppReady,
                      dashboardHidden: document.querySelector('main.app').style.visibility,
                      onboardingHidden: document.getElementById('onboardingShell').hidden,
                      loadingHidden: document.getElementById('loadingState').hidden,
                      errorHidden: document.getElementById('errorState').hidden,
                      errorText: document.getElementById('errorState').innerText,
                      visibleModals: Array.from(document.querySelectorAll('.activation-modal')).filter(el => !el.hidden).map(el => el.id)
                    })"""
                )
                raise AssertionError(f"card binding modal did not open: {state}")
            # Reproduce the incident without a real card or payment-provider call:
            # a previously activated trial must not be revoked by losing its mandate.
            activated = (
                "from subscriptions.models import Subscription, Payment; "
                f"s=Subscription.objects.filter(user_id={DEV_TG_USER_ID}).latest('created_at'); "
                "s.source=Subscription.Source.PAYMENT; s.payment_id='incident-browser-synthetic-bind'; "
                "s.save(update_fields=['source', 'payment_id']); "
                "Payment.objects.create(user=s.user, subscription=s, provider='monobank', "
                "provider_payment_id='incident-browser-synthetic-bind', kind='bind', status='paid', amount=1, currency='UAH')"
            )
            manage(app_root, env, "shell", "-c", activated)
            for status in ("trial", "active", "paid"):
                if status != "trial":
                    manage(app_root, env, "shell", "-c", (
                        "from subscriptions.models import Subscription; "
                        f"s=Subscription.objects.filter(user_id={DEV_TG_USER_ID}).latest('created_at'); "
                        f"s.status='{status}'; s.save(update_fields=['status'])"
                    ))
                page = context.new_page()
                page.goto(base_url + "/", wait_until="networkidle")
                actual = page.evaluate("url => fetch(url).then(r => r.json())", base_url + "/api/settings")["access"]
                assert actual["subscription_status"] == status, actual
                assert actual["mode"] == "active", actual
                assert actual["access_scope"] == "personal_full", actual
                assert actual["has_card"] is False, actual
                assert actual["recommended_action"] is None, actual
                assert page.locator("#cardBindingModal").is_hidden()
                assert page.locator("main.app").is_visible()
            browser.close()

        result = "ACCESS_BILLING_ACCEPTANCE_PASS: cardless admin grant, previously activated trial, active and paid subscriptions open the real PWA without a card popup; unactivated self-service trial remains blocked with popup; no provider calls or real charges"
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
