#!/usr/bin/env python
"""Real credit-account PWA acceptance; disposable local PostgreSQL, no production.

Run: python ops/credit_account_browser_acceptance.py --server
Financial responses are never intercepted or synthesized.
"""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from playwright.sync_api import sync_playwright, expect
from goals_browser_acceptance import free_port, wait_for_server

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "admin_service"
USER_ID = 888223


def seed():
    sys.path.insert(0, str(APP))
    import django
    django.setup()
    from django.utils import timezone
    from common.test_helpers import ensure_runtime_finance_tables
    from users.models import TelegramUser
    from categories.models import CategoryTemplate
    from subscriptions.models import Subscription, BillingProfile
    ensure_runtime_finance_tables()
    user = TelegramUser.objects.create(tg_user_id=USER_ID, first_name="Synthetic credit acceptance", lang="uk", base_currency="UAH", start_date=timezone.localdate(), onboarding_completed=False)
    Subscription.objects.create(user=user, status=Subscription.Status.ACTIVE, source=Subscription.Source.ADMIN, started_at=timezone.now(), expires_at=timezone.now() + timedelta(days=30))
    BillingProfile.objects.create(user=user, provider="monobank", wallet_id="synthetic-credit-wallet", card_token="synthetic-not-real", status=BillingProfile.Status.ACTIVE, auto_renew_enabled=False)
    for kind in ("expense", "income"):
        CategoryTemplate.objects.get_or_create(type=kind, slug="synthetic-credit-" + kind, defaults=dict(name="Synthetic " + kind, is_active=True))
    print("CREDIT_SYNTHETIC_SEED_PASS")


def scenarios(base, evidence):
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel="msedge")
        page = browser.new_page(viewport={"width": 390, "height": 844}, locale="uk-UA")
        errors = []
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        def api(path, data=None, expected=200):
            response = page.request.get(base + path) if data is None else page.request.post(base + path, data=data)
            body = response.json()
            assert response.status == expected, (path, response.status, body)
            return body
        try:
            api("/app/api/auth/dev", {})
            page.goto(base + "/app/", wait_until="networkidle")
            endpoints = page.locator("body").evaluate("node => ({...node.dataset})")
            expect(page.locator("#onboardingShell")).to_be_visible()
            page.locator("[data-onboarding-next]").click()
            page.locator('[data-onboarding-field="account_type"]').select_option("credit")
            expect(page.locator('[data-onboarding-field="balance_input_mode"]')).to_have_value("net")
            page.locator('[data-onboarding-field="label"]').fill("Onboarding credit")
            page.locator('[data-onboarding-field="starting_balance"]').fill("100000")
            page.locator('[data-onboarding-field="credit_limit"]').fill("300000")
            page.locator('[data-onboarding-field="monthly_interest_rate"]').fill("3,5")
            expect(page.locator('[data-onboarding-field="balance_input_mode"]')).to_have_value("available_credit")
            available_label = page.locator('[data-onboarding-field="starting_balance"]').locator("..").locator("span").inner_text()
            page.locator('[data-onboarding-field="balance_input_mode"]').select_option("net")
            assert page.locator('[data-onboarding-field="starting_balance"]').locator("..").locator("span").inner_text() != available_label
            page.locator('[data-onboarding-field="balance_input_mode"]').select_option("available_credit")
            expect(page.locator('[data-onboarding-field="starting_balance"]')).to_have_value("100000")
            page.locator('[data-onboarding-add]').click()
            no_limit_card = page.locator('.onboarding-account-card').nth(1)
            no_limit_card.locator('[data-onboarding-field="account_type"]').select_option("credit")
            expect(no_limit_card.locator('[data-onboarding-field="balance_input_mode"]')).to_have_value("net")
            no_limit_card.locator('[data-onboarding-field="label"]').fill("Onboarding no limit")
            no_limit_card.locator('[data-onboarding-field="starting_balance"]').fill("0")
            page.locator("[data-onboarding-review]").click()
            expect(page.locator("[data-onboarding-confirm]")).to_be_visible()
            assert "-200000" in page.locator(".onboarding-review").inner_text().replace(" ", "").replace("\u00a0", "")
            page.screenshot(path=str(evidence / "credit-onboarding-review-390.png"), full_page=True)
            page.locator("[data-onboarding-confirm]").click()
            expect(page.locator("#onboardingShell")).to_be_hidden(timeout=20000)

            def accounts():
                return api(endpoints["accountOptionsUrl"])["accounts"]
            def account(account_id):
                return next(row for row in accounts() if row["id"] == account_id)
            def bootstrap():
                return api("/app/api/bootstrap")
            def prepare_confirm(endpoint, payload):
                draft = api(endpoints[endpoint + "DraftUrl"], payload)["draft"]
                result = api(endpoints[endpoint + "ConfirmUrl"], dict(draft_id=draft["draft_id"], idempotency_key=uuid.uuid4().hex))
                return draft, result
            first = next(row for row in accounts() if row["label"] == "Onboarding credit")
            assert next(row for row in accounts() if row["label"] == "Onboarding no limit")["balance"]["value"] == "0.00"
            assert first["balance"]["value"] == "-200000.00"
            b = bootstrap()
            assert b["overview"]["total_balance"]["value"] == "0.00", b["overview"]
            assert b["credit_cards_preview"]["summary"]["total_debt"]["value"] == "200000.00"
            assert b["credit_cards_preview"]["items"][0]["monthly_interest_forecast"]["value"] == "7000.00"
            prepare_confirm("account", dict(action="balance_correction", account_id=first["id"], target_balance="0"))

            # Main account form, actual prepare/confirm UI and no write before confirm.
            page.reload(wait_until="networkidle")
            page.locator('[data-target="money"]').click()
            page.locator('[data-money-mode="accounts"]').click()
            page.locator("#accountType").select_option("credit")
            expect(page.locator("#accountBalanceMode")).to_have_value("net")
            page.locator("#accountLabel").fill("No limit credit")
            page.locator("#accountStartingBalance").fill("-10")
            page.locator("#accountPreviewBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_visible()
            assert not any(row["label"] == "No limit credit" for row in accounts())
            page.locator("#accountConfirmBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_hidden(timeout=20000)
            no_limit = next(row for row in accounts() if row["label"] == "No limit credit")
            assert no_limit["balance"]["value"] == "-10.00"
            prepare_confirm("account", dict(action="archive", account_id=no_limit["id"]))
            page.reload(wait_until="networkidle")
            page.locator('[data-target="money"]').click()
            page.locator('[data-money-mode="accounts"]').click()
            page.locator("#accountType").select_option("credit")
            page.locator("#accountLabel").fill("Credit exact case")
            page.locator("#accountStartingBalance").fill("100000")
            page.locator("#accountCreditLimit").fill("300000")
            expect(page.locator("#accountBalanceMode")).to_have_value("available_credit")
            page.locator("#accountInterestRate").fill("3,5")
            assert "%" in page.locator('[data-money-i18n="moneyInterest"]').first.inner_text()
            page.locator("#accountPreviewBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_visible()
            assert not any(row["label"] == "Credit exact case" for row in accounts())
            page.screenshot(path=str(evidence / "credit-create-confirmation-390.png"), full_page=True)
            page.locator("#accountConfirmBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_hidden(timeout=20000)
            credit = next(row for row in accounts() if row["label"] == "Credit exact case")
            credit_id = credit["id"]
            assert credit["balance"]["value"] == "-200000.00", credit
            b = bootstrap()
            assert b["overview"]["total_balance"]["value"] == "0.00"
            assert b["overview"]["current_debt"]["value"] == "200000.00"
            assert b["credit_cards_preview"]["items"][0]["monthly_interest_rate"] == "3.5"
            page.reload(wait_until="networkidle")
            page.locator('[data-target="money"]').click()
            page.locator('[data-target="status"]').click()
            expect(page.locator("#creditCardDetails")).to_contain_text("3.5%")
            expect(page.locator("#creditCardDetails")).to_contain_text("7 000")
            page.screenshot(path=str(evidence / "credit-debt-forecast-390.png"), full_page=True)

            # Legacy/net balance is not migrated by loading or renaming.
            _, legacy_result = prepare_confirm("account", dict(action="create", label="Legacy net credit", currency="UAH", account_type="credit", starting_balance="100000", credit_limit="300000"))
            legacy = next(row for row in accounts() if row["label"] == "Legacy net credit")
            prepare_confirm("account", dict(action="rename", account_id=legacy["id"], label="Legacy unchanged"))
            assert account(legacy["id"])["balance"]["value"] == "100000.00"
            prepare_confirm("account", dict(action="archive", account_id=legacy["id"]))
            # Existing wrong net values require an explicit correction confirmation.
            page.locator('[data-target="money"]').click()
            page.locator('[data-money-mode="accounts"]').click()
            page.locator("#accountAction").select_option("balance_correction")
            page.locator("#accountTarget").select_option(str(credit_id))
            expect(page.locator("#accountBalanceMode")).to_have_value("net")
            page.locator("#accountBalanceMode").select_option("available_credit")
            page.locator("#accountTargetBalance").fill("150000")
            page.locator("#accountPreviewBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_visible()
            assert account(credit_id)["balance"]["value"] == "-200000.00"
            page.locator("#accountConfirmBtn").click()
            expect(page.locator("#accountDraftCard")).to_be_hidden(timeout=20000)
            assert account(credit_id)["balance"]["value"] == "-150000.00"

            # Real financial API confirmations, limits, repayment, and both zero crossings.
            prepare_confirm("account", dict(action="create", label="Repayment wallet", currency="UAH", account_type="main", starting_balance="500000"))
            wallet = next(row for row in accounts() if row["label"] == "Repayment wallet")
            options = api(endpoints["transactionOptionsUrl"])
            expense = options["categories"]["expense"][0]["id"]
            income = options["categories"]["income"][0]["id"]
            today = time.strftime("%Y-%m-%d")
            prepare_confirm("transaction", dict(kind="expense", account_id=credit_id, category_id=expense, amount="10000", date=today))
            assert account(credit_id)["balance"]["value"] == "-160000.00"
            rejected = api(endpoints["transactionDraftUrl"], dict(kind="expense", account_id=credit_id, category_id=expense, amount="140001", date=today))["draft"]
            api(endpoints["transactionConfirmUrl"], dict(draft_id=rejected["draft_id"], idempotency_key=uuid.uuid4().hex), expected=400)
            assert account(credit_id)["balance"]["value"] == "-160000.00"
            prepare_confirm("transfer", dict(source_account_id=wallet["id"], target_account_id=credit_id, amount="160000", date=today))
            assert account(credit_id)["balance"]["value"] == "0.00"
            assert account(wallet["id"])["balance"]["value"] == "340000.00"
            item = next(row for row in bootstrap()["credit_cards_preview"]["items"] if row["id"] == credit_id)
            assert item["monthly_interest_forecast"]["value"] == "0.00"
            page.reload(wait_until="networkidle")
            page.locator('[data-target="status"]').click()
            expect(page.locator("#creditCardDetails")).to_contain_text("Credit exact case")
            page.screenshot(path=str(evidence / "credit-repaid-visible-390.png"), full_page=True)
            prepare_confirm("transaction", dict(kind="income", account_id=credit_id, category_id=income, amount="10000", date=today))
            assert account(credit_id)["balance"]["value"] == "10000.00"
            item = next(row for row in bootstrap()["credit_cards_preview"]["items"] if row["id"] == credit_id)
            assert item["monthly_interest_forecast"]["value"] == "0.00"
            prepare_confirm("transaction", dict(kind="expense", account_id=credit_id, category_id=expense, amount="20000", date=today))
            assert item["available_credit"]["value"] == "310000.00"
            assert account(credit_id)["balance"]["value"] == "-10000.00"
            item = next(row for row in bootstrap()["credit_cards_preview"]["items"] if row["id"] == credit_id)
            assert item["monthly_interest_forecast"]["value"] == "350.00"
            page.reload(wait_until="networkidle")
            page.locator('[data-target="money"]').click()
            page.locator('[data-target="status"]').click()
            expect(page.locator("#creditCardDetails")).to_contain_text("350")
            page.screenshot(path=str(evidence / "credit-final-390.png"), full_page=True)
            assert not errors, errors
            (evidence / "credit-assertions.json").write_text(json.dumps(dict(status="passed", synthetic=True, production=False, scenarios=["onboarding available credit", "PWA create exact 300000/100000", "percentage and actual-debt forecast", "legacy net preserved", "confirmed correction", "expense and limit rejection", "repayment to zero", "income above zero", "expense crossing below zero"]), indent=2), encoding="utf-8")
            print("CREDIT_REAL_PWA_SCENARIOS_PASS")
        finally:
            try:
                page.screenshot(path=str(evidence / "credit-last-state.png"), full_page=True, timeout=5000)
            except Exception as exc:
                print("LAST_SCREENSHOT_UNAVAILABLE", type(exc).__name__)
            browser.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--seed", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--evidence-dir", type=Path, default=ROOT / ".hermes/delivery/20261115_180038_0d2dc2")
    args = parser.parse_args()
    if args.seed:
        seed()
        return 0
    if not args.server:
        parser.error("choose --server")
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    container = "vydno-credit-acceptance-" + uuid.uuid4().hex[:12]
    server = log = None
    env = os.environ.copy()
    for key in list(env):
        if key in ("PYTHONHOME", "PYTHONPATH") or key.endswith("_FILE") and key.startswith(("DJANGO_", "DATABASE_", "BOT_")):
            env.pop(key)
    env.update(DJANGO_SETTINGS_MODULE="config.settings", DJANGO_DEBUG="true", DJANGO_SECRET_KEY="synthetic-credit-only", DJANGO_ALLOWED_HOSTS="127.0.0.1,localhost,testserver", MINIAPP_DEV_TG_USER_ID=str(USER_ID), SESSION_COOKIE_SECURE="false", CSRF_COOKIE_SECURE="false", SECURE_SSL_REDIRECT="false", REDIS_URL="redis://127.0.0.1:63999/0")
    try:
        subprocess.run(["docker", "run", "-d", "--rm", "--name", container, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=credit_acceptance", "-p", "127.0.0.1::5432", "postgres:16-alpine"], check=True, capture_output=True)
        port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = f"postgresql://postgres@127.0.0.1:{port}/credit_acceptance"
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError("disposable PostgreSQL not ready")
        subprocess.run([sys.executable, "manage.py", "migrate", "--noinput"], cwd=APP, env=env, check=True, stdout=subprocess.DEVNULL)
        subprocess.run([sys.executable, str(Path(__file__).resolve()), "--seed"], cwd=APP, env=env, check=True)
        with (args.evidence_dir / "credit-regression.log").open("w", encoding="utf-8") as regression_log:
            subprocess.run([sys.executable, "-c", "import django,unittest; django.setup(); suite=unittest.defaultTestLoader.loadTestsFromNames(['miniapp.tests_unit','miniapp.tests_credit_account_semantics']); result=unittest.TextTestRunner(verbosity=2).run(suite); raise SystemExit(not result.wasSuccessful())"], cwd=APP, env=env, check=True, stdout=regression_log, stderr=subprocess.STDOUT)
        with (args.evidence_dir / "history-regression.log").open("w", encoding="utf-8") as history_log:
            subprocess.run([sys.executable, "manage.py", "test", "miniapp.tests.MiniAppTests.test_history_void_expense_restores_balance_once_and_soft_deletes", "miniapp.tests.MiniAppTests.test_history_void_income_subtracts_amount_and_updates_credit_mode", "miniapp.tests.MiniAppTests.test_history_void_rejects_unsupported_and_changed_transactions", "miniapp.tests.MiniAppTests.test_history_void_family_member_can_cancel_only_authored_transaction", "--noinput", "-v", "2"], cwd=APP, env=env, check=True, stdout=history_log, stderr=subprocess.STDOUT)
        log = (args.evidence_dir / "credit-server.log").open("w", encoding="utf-8")
        port = free_port()
        server = subprocess.Popen([sys.executable, "manage.py", "runserver", f"127.0.0.1:{port}", "--noreload"], cwd=APP, env=env, stdout=log, stderr=subprocess.STDOUT)
        base = f"http://127.0.0.1:{port}"
        wait_for_server(base + "/app/", server)
        scenarios(base, args.evidence_dir)
    finally:
        if server:
            server.terminate()
            server.wait(timeout=15)
        if log:
            log.close()
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        remaining = subprocess.check_output(["docker", "ps", "-aq", "--filter", "name=^/" + container + "$"], text=True).strip()
        assert not remaining, "disposable container cleanup failed"
    print("CREDIT_ACCOUNT_BROWSER_ACCEPTANCE_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
