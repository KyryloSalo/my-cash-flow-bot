#!/usr/bin/env python
"""Exercise financial goals in the real PWA against a throwaway PostgreSQL DB.

No production credentials, requests or data are used. --server owns and removes
its Docker container. Browser requests use Django's configured local dev login.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request
import uuid

from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "admin_service"
USER_ID = 888221


def seed():
    sys.path.insert(0, str(APP))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django
    django.setup()
    from django.utils import timezone
    from common.test_helpers import ensure_runtime_finance_tables
    from users.models import TelegramUser
    from accounts.models import Account
    from categories.models import Category
    from miniapp.onboarding import CURRENT_ONBOARDING_VERSION
    from subscriptions.models import Subscription, BillingProfile

    ensure_runtime_finance_tables()
    user = TelegramUser.objects.create(tg_user_id=USER_ID, first_name="Synthetic goal acceptance", lang="uk", base_currency="UAH", start_date=timezone.localdate(), onboarding_completed=True, onboarding_version=CURRENT_ONBOARDING_VERSION)
    Subscription.objects.create(user=user, status=Subscription.Status.ACTIVE, source=Subscription.Source.ADMIN, started_at=timezone.now(), expires_at=timezone.now() + timedelta(days=30))
    BillingProfile.objects.create(user=user, provider="monobank", wallet_id="synthetic-goals-wallet", card_token="synthetic-not-a-real-token", status=BillingProfile.Status.ACTIVE, auto_renew_enabled=False)
    Account.objects.create(tg_user=user, label="Acceptance wallet", currency="UAH", balance="40000", account_type="main", non_negative_account_type="main", is_active=True)
    Account.objects.create(tg_user=user, label="Existing savings NOT a goal", currency="UAH", balance="0", account_type="savings", non_negative_account_type="savings", is_active=True)
    Category.objects.create(tg_user=user, user=user, name="Goal purchase", kind="expense", type="expense", is_active=True)
    Category.objects.create(tg_user=user, user=user, name="Synthetic income", kind="income", type="income", is_active=True)
    print("SEEDED_SYNTHETIC_GOALS_USER")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_server(url, process):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("local Django server exited before readiness")
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("local Django server readiness timed out")


def api(page, path, data=None, *, expected=200):
    response = page.request.get(path) if data is None else page.request.post(path, data=data)
    body = response.json()
    assert response.status == expected, (path, response.status, body)
    return body


def browser_scenarios(base, evidence, channel=None):
    """Use the real UI and API; never intercept or synthesize financial responses."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel=channel)
        page = browser.new_page(viewport={"width": 390, "height": 844}, locale="uk-UA")
        errors = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        try:
            api(page, base + "/app/api/auth/dev", {})
            page.goto(base + "/app/", wait_until="networkidle")
            expect(page.locator("#dashboardApp")).to_be_visible(timeout=20000)
            profile = api(page, base + "/app/api/profile")
            assert profile["access"]["mode"] == "active", profile["access"]["mode"]
            page.screenshot(path=str(evidence / "goals-initial.png"))
            endpoints = page.locator('body').evaluate('(node) => ({...node.dataset})')

            def accounts():
                return api(page, base + endpoints['accountOptionsUrl'])['accounts']

            def account(account_id):
                return next(row for row in accounts() if str(row['id']) == str(account_id))

            def confirm(button, endpoint):
                with page.expect_response(lambda response: response.url.endswith(endpoint) and response.request.method == 'POST') as pending:
                    page.locator(button).click()
                response = pending.value
                assert response.ok, (endpoint, response.status, response.json())
                return response.json()

            def details(name):
                page.get_by_role('button', name='Гроші', exact=True).click()
                page.locator('[data-money-mode="goals"]').click()
                card = page.locator('#moneyGoalsList .money-goal-row, #moneyClosedGoalsList .money-goal-row').filter(has_text=name)
                expect(card).to_be_visible()
                with page.expect_response(lambda response: response.url.startswith(base + endpoints['historyUrl'] + '?account_id=')) as pending_history:
                    card.click()
                history_items = pending_history.value.json()['items']
                history_text = '\n'.join(' · '.join(str(value) for value in [item.get('date') or item.get('transaction_date'), item.get('comment') or item.get('description'), item['amount']['display']] if value) for item in history_items) if history_items else 'Історія порожня'
                expect(page.locator('#goalDetailsName')).to_have_text(name)
                expect(page.locator('#goalDetailsCard')).to_be_visible()
                expect(page.locator('#goalDetailsHistory')).to_have_text(history_text)

            def finish_create(name, target):
                initial = accounts()
                expect(page.locator('#accountGoalName')).to_be_visible()
                expect(page.locator('#accountLabel')).to_be_disabled()
                expect(page.locator('#accountTarget')).to_be_hidden()
                expect(page.locator('#accountCurrency')).to_be_visible()
                expect(page.locator('#accountCurrency')).to_have_value('UAH')
                page.locator('#accountGoalName').fill(name)
                page.locator('#accountGoalAmount').fill(str(target))
                page.locator('#accountGoalDate').fill('2027-12-31')
                page.locator('#accountPreviewBtn').click()
                expect(page.locator('#accountDraftCard')).to_be_visible()
                assert accounts() == initial, 'draft must not write an account or money'
                confirm('#accountConfirmBtn', endpoints['accountConfirmUrl'])
                expect(page.locator('#accountDraftCard')).to_be_hidden()
                current = accounts()
                assert len(current) == len(initial) + 1
                goal = next(row for row in current if row.get('goal_name') == name)
                assert goal['id'] not in [row['id'] for row in initial]
                assert goal['balance']['value'] == '0.00'
                assert goal['currency'] == 'UAH'
                assert goal['goal_status'] == 'active'
                return goal

            def topup(name, goal_id, amount):
                details(name)
                page.locator('[data-goal-action="topup"]').click()
                expect(page.locator('#transferTarget')).to_have_value(str(goal_id))
                wallet = next(row for row in accounts() if row['label'] == 'Acceptance wallet')
                page.locator('#transferSource').select_option(str(wallet['id']))
                page.locator('#transferAmount').fill(str(amount))
                initial = accounts()
                page.locator('#transferPreviewBtn').click()
                expect(page.locator('#transferDraftCard')).to_be_visible()
                assert accounts() == initial, 'transfer draft must not move money'
                confirm('#transferConfirmBtn', endpoints['transferConfirmUrl'])
                expect(page.locator('#transferDraftCard')).to_be_hidden()

            page.locator('#overviewGoalsCreate').click()
            goal = finish_create('test 22', 9998)
            print('PASS create: dedicated zero-balance account, confirmation-only write')
            details('test 22')
            expect(page.locator('#goalDetailsMeta')).to_contain_text('2027-12-31')
            expect(page.locator('[data-goal-action="spend"]')).to_be_disabled()
            topup('test 22', goal['id'], 5000)
            assert account(goal['id'])['balance']['value'] == '5000.00'
            assert account(goal['id'])['goal_status'] == 'active'
            details('test 22')
            expect(page.locator('[data-goal-action="spend"]')).to_be_disabled()
            topup('test 22', goal['id'], 5500)
            assert account(goal['id'])['balance']['value'] == '10500.00'
            assert account(goal['id'])['goal_status'] == 'funded'
            details('test 22')
            expect(page.locator('[data-goal-action="spend"]')).to_be_enabled()
            history = api(page, base + endpoints['historyUrl'] + '?account_id=' + str(goal['id']))
            assert len(history['items']) == 2, history
            print('PASS funding: transfers, underfunded spend blocked, overfunded goal visible')

            page.locator('[data-goal-action="edit"]').click()
            expect(page.locator('#accountTarget')).to_have_value(str(goal['id']))
            page.locator('#accountGoalName').fill('test 22 edited')
            page.locator('#accountPreviewBtn').click()
            expect(page.locator('#accountDraftCard')).to_be_visible()
            confirm('#accountConfirmBtn', endpoints['accountConfirmUrl'])
            expect(page.locator('#accountDraftCard')).to_be_hidden()
            assert account(goal['id'])['balance']['value'] == '10500.00'
            details('test 22 edited')
            page.screenshot(path=str(evidence / 'goals-overfunded.png'), full_page=True)

            page.locator('[data-goal-action="spend"]').click()
            expect(page.locator('#txAmount')).to_have_value('10500.00')
            page.locator('#txCategory').select_option(label='Goal purchase')
            expect(page.locator('#txDraftCard')).to_be_visible(timeout=15000)
            assert account(goal['id'])['balance']['value'] == '10500.00'
            confirm('#txConfirmBtn', endpoints['transactionConfirmUrl'])
            expect(page.locator('#txDraftCard')).to_be_hidden()
            assert account(goal['id'])['balance']['value'] == '0.00'
            assert account(goal['id'])['goal_status'] == 'spent'
            scoped = api(page, base + endpoints['historyUrl'] + '?account_id=' + str(goal['id']))
            assert len(scoped['items']) == 3, scoped
            details('test 22 edited')
            assert len(page.locator('#goalDetailsHistory').inner_text().splitlines()) == 3
            print('PASS spending: entire actual balance, confirmed once, scoped transaction/transfer history')

            page.get_by_role('button', name='Гроші', exact=True).click()
            page.locator('[data-money-mode="goals"]').click()
            page.locator('#moneyGoalsAdd').click()
            second = finish_create('Keep remaining funds', 100)
            topup('Keep remaining funds', second['id'], 150)
            details('Keep remaining funds')
            initial = accounts()
            history_fields = ('id', 'date', 'kind', 'amount_value', 'currency', 'comment', 'account_id', 'category_id')
            before_history = [{key: item[key] for key in history_fields} for item in api(page, base + endpoints['historyUrl'] + '?account_id=' + str(second['id']))['items']]
            before_balances = {account['id']: account['balance']['value'] for account in accounts()}
            page.locator('[data-goal-action="complete"]').click()
            expect(page.locator('#accountDraftCard')).to_be_visible()
            assert accounts() == initial, 'completion draft must not change lifecycle or money'
            confirm('#accountConfirmBtn', endpoints['accountConfirmUrl'])
            expect(page.locator('#accountDraftCard')).to_be_hidden()
            completed = account(second['id'])
            assert completed['goal_status'] == 'completed'
            assert completed['balance']['value'] == '150.00'
            assert len(accounts()) == len(initial), 'completion must not archive/delete its account'
            after_history = [{key: item[key] for key in history_fields} for item in api(page, base + endpoints['historyUrl'] + '?account_id=' + str(second['id']))['items']]
            assert after_history == before_history
            assert {account['id']: account['balance']['value'] for account in accounts()} == before_balances
            page.locator('[data-money-mode="accounts"]').click()
            expect(page.locator('#accountTarget option').filter(has_text='Keep remaining funds')).to_have_count(1)
            print('PASS completion: separate confirmation, retained balance and usable account, no transaction')
            (evidence / 'browser-acceptance.json').write_text(json.dumps({'status': 'passed', 'synthetic': True, 'goal_ids': [goal['id'], second['id']], 'browser_errors': errors}, indent=2), encoding='utf-8')
        finally:
            try:
                page.screenshot(path=str(evidence / "goals-last-state.png"), timeout=5000)
            except Exception as exc:
                print("LAST_SCREENSHOT_UNAVAILABLE:", type(exc).__name__)
            browser.close()
        assert not errors, errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--seed", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--browser-channel", default="msedge", help="Use an installed Chromium channel, e.g. msedge")
    parser.add_argument("--evidence-dir", type=Path, default=ROOT / ".hermes/delivery/20261019_172020_5b2ab3")
    args = parser.parse_args()
    if args.seed:
        seed()
        return 0
    if args.preflight:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            browser.close()
        subprocess.run(["docker", "version", "--format", "{{.Server.Version}}"], check=True)
        print("GOALS_ACCEPTANCE_PREFLIGHT_PASS")
        return 0
    if not args.server:
        parser.error("choose --server or --preflight")
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    container = "vydno-goals-acceptance-" + run_id
    server = None
    server_log = None
    env = os.environ.copy()
    for key in list(env):
        if key.endswith("_FILE") and key.startswith(("DJANGO_", "DATABASE_", "BOT_")):
            env.pop(key)
    env.update(DJANGO_SETTINGS_MODULE="config.settings", DJANGO_DEBUG="true", DJANGO_SECRET_KEY="synthetic-goals-only-" + run_id, DJANGO_ALLOWED_HOSTS="127.0.0.1,localhost,testserver", MINIAPP_DEV_TG_USER_ID=str(USER_ID), SESSION_COOKIE_SECURE="false", CSRF_COOKIE_SECURE="false", SECURE_SSL_REDIRECT="false", REDIS_URL="redis://127.0.0.1:63999/0")
    try:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", container, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=goals_acceptance", "-p", "127.0.0.1::5432", "postgres:16-alpine"], check=True, capture_output=True)
        db_port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = f"postgresql://postgres@127.0.0.1:{db_port}/goals_acceptance"
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError("local PostgreSQL readiness timed out")
        subprocess.run([sys.executable, "manage.py", "migrate", "--noinput"], cwd=APP, env=env, check=True, stdout=subprocess.DEVNULL)
        subprocess.run([sys.executable, str(Path(__file__).resolve()), "--seed"], cwd=APP, env=env, check=True)
        port = free_port()
        server_log = (args.evidence_dir / "goals-server.log").open("w", encoding="utf-8")
        server = subprocess.Popen([sys.executable, "manage.py", "runserver", f"127.0.0.1:{port}", "--noreload"], cwd=APP, env=env, stdout=server_log, stderr=subprocess.STDOUT)
        base = f"http://127.0.0.1:{port}"
        wait_for_server(base + "/app/", server)
        browser_scenarios(base, args.evidence_dir, args.browser_channel)
        print("GOALS_REAL_PWA_ACCEPTANCE_PASS")
        return 0
    finally:
        if server:
            server.terminate()
            server.wait(timeout=15)
        if server_log:
            server_log.close()
        subprocess.run(["docker", "rm", "-f", container], capture_output=True, check=False)


if __name__ == "__main__":
    raise SystemExit(main())
