#!/usr/bin/env python
"""Real PWA + bot entitlement acceptance using disposable synthetic PostgreSQL data.

No real identity, payment provider, production database or credential files are used.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from access_billing_acceptance import free_port, manage, wait_for

MEMBER = 999541428261
OWNER = 999541428260
SEED = f"""
from datetime import timedelta
from django.db import connection
from django.utils import timezone
from users.models import TelegramUser
from subscriptions.models import Subscription
from accounts.models import Account
from categories.models import Category
for uid in ({OWNER}, {MEMBER}):
    u = TelegramUser.objects.create(tg_user_id=uid, first_name='Synthetic family acceptance', base_currency='UAH', lang='uk', start_date=timezone.localdate(), onboarding_completed=True, onboarding_version=3)
    Account.objects.create(tg_user=u, label='Synthetic cash', currency='UAH', account_type='cash', starting_balance=0, balance=0)
    Category.objects.create(user=u, type='expense', name='Other', slug='other', source='acceptance')
    Category.objects.create(user=u, type='income', name='Salary', slug='salary', source='acceptance')
Subscription.objects.create(user_id={OWNER}, status='paid', source='payment', expires_at=timezone.now()+timedelta(days=30))
with connection.cursor() as c:
    c.execute("INSERT INTO families (name,owner_user_id,status,created_at,updated_at) VALUES ('Synthetic family',%s,'active',now(),now()) RETURNING id", [{OWNER}])
    fid=c.fetchone()[0]
    Account.objects.filter(tg_user_id={OWNER}).update(family_id=fid)
    Category.objects.filter(user_id={OWNER}).update(family_id=fid)
    for uid,role in (({OWNER},'owner'),({MEMBER},'member')):
        c.execute("INSERT INTO family_members (family_id,user_id,role,status,joined_at,created_at,updated_at) VALUES (%s,%s,%s,'active',now(),now(),now())", [fid,uid,role])
"""


def isolated_environment() -> dict[str, str]:
    # Do not inherit database URLs, secret-file settings, PYTHONPATH or provider access.
    allowed = {'PATH', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'TMPDIR', 'LOCALAPPDATA',
               'APPDATA', 'USERPROFILE', 'COMSPEC', 'PATHEXT', 'PROGRAMDATA', 'PROGRAMFILES'}
    return {k: v for k, v in os.environ.items() if k.upper() in allowed}


def bot_probe(root: Path, env: dict[str, str], expected: str) -> None:
    code = f"""
import asyncio, os, sys
sys.path.insert(0, {str(root / 'bot')!r})
import asyncpg
from billing_client import get_access_state
async def main():
    c = await asyncpg.connect(os.environ['DATABASE_URL'])
    try:
        async with c.transaction(readonly=True):
            state = await get_access_state(c, {MEMBER})
            assert state['access_scope'] == {expected!r}, state['access_scope']
            assert not state['billing_state']['has_card']
        print('BOT_SCOPE_PASS: {expected}')
    finally:
        await c.close()
asyncio.run(main())
"""
    subprocess.run([sys.executable, '-I', '-c', code], env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', action='store_true', required=True)
    args = parser.parse_args()
    from playwright.sync_api import sync_playwright

    root = Path(__file__).resolve().parents[1]
    app = root / 'admin_service'
    container = 'mcf-family-acceptance-' + uuid.uuid4().hex[:12]
    env = isolated_environment()
    env.update(DJANGO_DEBUG='true', DJANGO_SECRET_KEY='synthetic-family-only-key',
               DJANGO_ALLOWED_HOSTS='127.0.0.1,localhost', MINIAPP_DEV_TG_USER_ID=str(MEMBER),
               SECURE_SSL_REDIRECT='false', SESSION_COOKIE_SECURE='false', CSRF_COOKIE_SECURE='false',
               REDIS_URL='redis://127.0.0.1:63999/0')
    server = None
    server_log = None
    try:
        subprocess.run(['docker', 'run', '-d', '--rm', '--name', container,
                        '-e', 'POSTGRES_USER=synthetic_family', '-e', 'POSTGRES_PASSWORD=synthetic_local_only',
                        '-e', 'POSTGRES_DB=synthetic_family', '-p', '127.0.0.1::5432', 'postgres:16-alpine'],
                       capture_output=True, text=True, check=True)
        db_port = subprocess.check_output(['docker', 'port', container, '5432/tcp'], text=True).strip().rsplit(':', 1)[-1]
        env['DATABASE_URL'] = f'postgresql://synthetic_family:synthetic_local_only@127.0.0.1:{db_port}/synthetic_family'
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if subprocess.run(['docker', 'exec', container, 'pg_isready', '-U', 'synthetic_family'], capture_output=True).returncode == 0:
                break
            time.sleep(.25)
        else:
            raise RuntimeError('owned PostgreSQL readiness timed out')
        manage(app, env, 'migrate', '--noinput')
        manage(app, env, 'shell', '-c', SEED)
        port = free_port()
        evidence = root / '.hermes/delivery/family-subscription-acceptance'
        evidence.mkdir(parents=True, exist_ok=True)
        server_log = (evidence / 'family-server.log').open('w', encoding='utf-8')
        server = subprocess.Popen([sys.executable, 'manage.py', 'runserver', f'127.0.0.1:{port}', '--noreload'],
                                  cwd=app, env=env, stdout=server_log, stderr=subprocess.STDOUT)
        base = f'http://127.0.0.1:{port}/app'
        wait_for(base + '/', server)
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True, args=['--disable-gpu', '--renderer-process-limit=2'])
            context = browser.new_context(reduced_motion='reduce', service_workers='block')
            # Local-only browser transport: no Telegram/payment/provider traffic.
            context.route('**/*', lambda route: route.continue_() if route.request.url.startswith(f'http://127.0.0.1:{port}/') else route.abort())
            page = context.new_page()
            page.goto(base + '/', wait_until='networkidle')
            auth = page.locator('body').get_attribute('data-dev-auth-url')
            assert auth, 'dev auth entry missing'
            page.evaluate("url => fetch(url, {method:'POST'}).then(r => {if(!r.ok) throw Error('dev auth');})", auth)

            def scenario(name: str, active: bool) -> None:
                bot_probe(root, env, 'family_full' if active else 'paywall')
                page.goto(base + '/', wait_until='networkidle')
                access = page.evaluate("url => fetch(url).then(r=>r.json()).then(r=>r.access)", base + '/api/settings')
                assert access['has_card'] is False, name
                assert access['access_scope'] == ('family_full' if active else 'paywall'), (name, access)
                assert access['mode'] == ('active' if active else 'blocked'), (name, access)
                if active:
                    assert access['recommended_action'] is None, name
                    page.wait_for_function('window.vydnoAppReady === true')
                    assert page.locator('#cardBindingModal').is_hidden(), name
                    assert page.locator('main.app').is_visible(), name
                else:
                    page.locator('#cardBindingModal').wait_for(state='visible', timeout=15000)
                print('PWA_SCENARIO_PASS:', name, flush=True)

            scenario('cardless member / paid owner without saved card', True)
            manage(app, env, 'shell', '-c', f"from subscriptions.models import Subscription; from django.utils import timezone; from datetime import timedelta; Subscription.objects.filter(user_id={OWNER}).update(status='expired',expires_at=timezone.now()-timedelta(days=1))")
            scenario('owner expired', False)
            manage(app, env, 'shell', '-c', f"from subscriptions.models import Subscription; from django.utils import timezone; from datetime import timedelta; Subscription.objects.filter(user_id={OWNER}).update(status='paid',expires_at=timezone.now()+timedelta(days=30))")
            scenario('owner restored', True)
            manage(app, env, 'shell', '-c', f"from django.db import connection; c=connection.cursor(); c.execute(\"UPDATE family_members SET status='removed' WHERE user_id=%s\", [{MEMBER}]); c.close()")
            scenario('revoked membership / unpaid nonfamily', False)
            browser.close()
        manage(app, env, 'shell', '-c', "from subscriptions.models import Payment; from transactions.models import Transaction; assert Payment.objects.count()==0; assert Transaction.objects.count()==0; print('NO_FINANCIAL_WRITES_PASS')")
        print('FAMILY_SUBSCRIPTION_ACCEPTANCE_PASS: real PWA and bot, synthetic data only; no provider calls', flush=True)
        return 0
    finally:
        if server:
            server.terminate()
            server.wait(timeout=10)
        if server_log:
            server_log.close()
        subprocess.run(['docker', 'rm', '-f', container], capture_output=True, text=True)
        remaining = subprocess.check_output(['docker', 'ps', '-a', '--filter', 'name=^/' + container + '$', '--format', '{{.Names}}'], text=True).strip()
        if remaining:
            raise RuntimeError('owned container cleanup failed')
        print('OWNED_CONTAINER_CLEANUP_PASS', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
