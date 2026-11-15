#!/usr/bin/env python
"""Disposable PostgreSQL + real Chromium consent regression. NOT real card/provider acceptance.

Run: python ops/billing_lifecycle_acceptance.py --server
No credentials are inherited; provider/network traffic is denied outside loopback.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "admin_service"
DEFAULT_LOG = ROOT / ".hermes/delivery/20261114_032642_d18c51/frontend-browser.log"
BOOTSTRAP = '''import sys, socket, runpy
sys.path.insert(0, sys.argv.pop(1))
_original_connect = socket.socket.connect
def local_connect(self, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', 'localhost', '::1'):
        raise RuntimeError('acceptance denied non-loopback network')
    return _original_connect(self, address)
socket.socket.connect = local_connect
sys.argv[0] = 'manage.py'
if 'runserver' in sys.argv:
    import django, os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    django.setup()
    from miniapp import views
    from subscriptions.models import Subscription
    counter = {'invoice_creations': 0}
    if os.environ.get('ACCEPTANCE_PROVIDER_ORIGIN'):
        sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..', 'ops')))
        from billing_lifecycle_browser import install_provider_seams
        counter = install_provider_seams(os.environ['ACCEPTANCE_PROVIDER_ORIGIN'])
    original_settings = views._settings_payload
    def diagnostic_settings(user):
        payload = original_settings(user)
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute('select current_database(), current_schema(), inet_server_port(), pg_backend_pid()')
            db_identity = cursor.fetchone()
        payload['acceptance_diagnostic'] = {'user': user.pk, 'db': db_identity, 'subscriptions': list(Subscription.objects.filter(user_id=user.pk).values_list('status', flat=True)), **counter}
        return payload
    views._settings_payload = diagnostic_settings
runpy.run_path('manage.py', run_name='__main__')
'''
SEED = """
from users.models import TelegramUser
from subscriptions.models import Payment
from django.utils import timezone
from accounts.models import Account
from categories.models import Category
from miniapp.tests import ensure_runtime_finance_tables
ensure_runtime_finance_tables()
u=TelegramUser.objects.create(tg_user_id=788881, first_name='Acceptance', lang='en', base_currency='UAH', onboarding_completed=True, onboarding_version=3, start_date=timezone.localdate())
Account.objects.create(tg_user=u,label='Synthetic',currency='UAH')
for kind in ['income','expense']:
    Category.objects.create(user=u,tg_user=u,type=kind,name='Synthetic '+kind)
Payment.objects.create(user=u,provider='monobank',kind='bind',status='paid',amount='1.00',currency='UAH')
print('FIXTURE=paid-no-token')
"""
VERIFY = """
from subscriptions.models import Payment, BillingProfile
assert Payment.objects.count()==1
assert BillingProfile.objects.count()==0
print('DATABASE_PASS: no duplicate payment or billing profile created')
"""



def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', action='store_true', required=True)
    parser.add_argument('--log', type=Path, default=DEFAULT_LOG)
    args = parser.parse_args()
    lines = ['SYNTHETIC LOCAL ACCEPTANCE: NOT real card/provider acceptance']
    def report(text):
        lines.append(text)
        print(text, flush=True)
    # Allowlist system plumbing only: never inherit credential values/file selectors.
    allowed = {'PATH','SYSTEMROOT','WINDIR','COMSPEC','PATHEXT','TEMP','TMP','TMPDIR','LOCALAPPDATA','APPDATA','USERPROFILE','HOME','PLAYWRIGHT_BROWSERS_PATH'}
    env = {k:v for k,v in os.environ.items() if k.upper() in allowed}
    env.update(DJANGO_DEBUG='true', DJANGO_SECRET_KEY='synthetic-acceptance-only-'+uuid.uuid4().hex,
               DJANGO_ALLOWED_HOSTS='127.0.0.1,localhost', MINIAPP_DEV_TG_USER_ID='788881',
               MINIAPP_BROWSER_LOGIN_SECRET='synthetic-local-browser-login-only',
               SECURE_SSL_REDIRECT='false', SESSION_COOKIE_SECURE='false', CSRF_COOKIE_SECURE='false',
               MONO_API_BASE_URL='http://127.0.0.1:9', MONO_MERCHANT_TOKEN='', REDIS_URL='redis://127.0.0.1:63999/0')
    container = 'mcf-lifecycle-ui-'+uuid.uuid4().hex[:12]
    server = None
    attempted = False
    success = False
    stage = 'bootstrap'
    def command(argv, *, django=False):
        result = subprocess.run(argv, cwd=APP if django else ROOT, env=env, capture_output=True, text=True, timeout=180)
        if result.returncode:
            report(result.stdout); report(result.stderr); raise AssertionError('subprocess failed at '+stage+' (exit '+str(result.returncode)+')')
        return result.stdout + result.stderr
    def manage(*argv):
        return command([sys.executable,'-I','-c',BOOTSTRAP,str(APP),*argv],django=True)
    try:
        attempted = True
        command(['docker','run','-d','--rm','--name',container,'-e','POSTGRES_USER=acceptance','-e','POSTGRES_PASSWORD=synthetic-local-only','-e','POSTGRES_DB=acceptance','-p','127.0.0.1::5432','postgres:16-alpine'])
        dbport = command(['docker','port',container,'5432/tcp']).strip().rsplit(':',1)[-1]
        env['DATABASE_URL']='postgresql://acceptance:synthetic-local-only@127.0.0.1:'+dbport+'/acceptance'
        for _ in range(90):
            ready=subprocess.run(['docker','exec',container,'pg_isready','-U','acceptance','-d','acceptance'],env=env,capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(.5)
        else:
            raise AssertionError('PostgreSQL readiness timeout')
        stage='canonical migrations'
        manage('migrate','--noinput')
        stage='database identity'
        output=manage('shell','-c',"from django.db import connection; c=connection.cursor(); c.execute(\"select current_database(), current_schema(), current_setting('server_version')\"); print('POSTGRESQL_IDENTITY='+str(c.fetchone()))")
        for line in output.splitlines():
            if line.startswith('POSTGRESQL_IDENTITY='):
                report(line)
        stage='frontend and lifecycle unit regressions'
        report(manage('test','miniapp.test_billing_activation_ui','subscriptions.tests_bind_lifecycle','subscriptions.tests_bind_concurrency','--verbosity','2'))
        stage='paid fixture'
        output=manage('shell','-c',SEED)
        offer=next(line.split('=',1)[1] for line in output.splitlines() if line.startswith('FIXTURE='))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0))
            port=sock.getsockname()[1]
        base='http://127.0.0.1:'+str(port)
        server=subprocess.Popen([sys.executable,'-I','-c',BOOTSTRAP,str(APP),'runserver','127.0.0.1:'+str(port),'--noreload'],cwd=APP,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        stage='server readiness'
        for _ in range(180):
            assert server.poll() is None, 'local server exited'
            try:
                with urllib.request.urlopen(base+'/app/',timeout=1) as response:
                    if response.status == 200:
                        break
            except Exception:
                time.sleep(.25)
        else:
            raise AssertionError('server readiness timeout')
        stage='Chromium activation'
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True)
            try:
                context=browser.new_context(service_workers='block')
                context.route('**/*',lambda route: route.continue_() if route.request.url.startswith(base+'/') else route.abort())
                page = context.new_page()
                def inspect_settings(response):
                    if 'settings' in response.url and response.request.method == 'GET':
                        payload = response.json()
                        report('REQUEST_DIAGNOSTIC '+json.dumps((payload.get('data') or payload).get('acceptance_diagnostic')))
                        access = (payload.get('data') or payload).get('access') or {}
                        report('SETTINGS_RESPONSE status='+str(response.status)+' mode='+str(access.get('mode'))+' activation='+str(access.get('activation_state')))
                page.on('response', inspect_settings)
                page.goto(base+'/app/',wait_until='networkidle')
                assert page.locator('link[rel="manifest"]').count() == 1
                auth=page.locator('body').get_attribute('data-dev-auth-url')
                def post(url,payload):
                    return page.evaluate('''async ([url,payload]) => {
                        const token=document.cookie.split('; ').find(x=>x.startsWith('csrftoken='))?.split('=').slice(1).join('=');
                        const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json','X-CSRFToken':decodeURIComponent(token||'')},body:JSON.stringify(payload)});
                        return {status:r.status,body:await r.json()};
                    }''',[url,payload])
                assert post(auth,{})['status'] == 200, 'dev auth failed'
                page.reload(wait_until='networkidle')
                assert page.url == base+'/app/'
                from playwright.sync_api import expect
                expect(page.locator('#settingsBillingMessage')).to_contain_text('Do not pay again')
                expect(page.locator('#billingBindBtn')).to_be_hidden()
                expect(page.locator('#cardBindingModal')).to_be_hidden()
                # A real click on the non-charging refresh action, not evaluate(fetch).
                with page.expect_response(lambda response: 'api/settings' in response.url and response.request.method == 'GET') as pending_settings:
                    page.get_by_role('button',name='Check activation',exact=True).click()
                pending_settings.value.finished()
                page.wait_for_load_state('networkidle')
                expect(page.locator('#settingsBillingMessage')).to_contain_text('Do not pay again')
                expect(page.locator('#billingBindBtn')).to_be_hidden()
                report('BROWSER_PASS: real paid/no-token gate; no charge CTA; check activation click retains honest pending state')
                report(manage('shell','-c',"from subscriptions.models import Subscription; from django.utils import timezone; from datetime import timedelta; from miniapp.services import resolve_access; from users.models import TelegramUser; Subscription.objects.create(user_id=788881,status='manual',source='admin',expires_at=timezone.now()+timedelta(days=30)); print('MANUAL_MODE='+resolve_access(TelegramUser.objects.get(pk=788881))['mode'])"))
                report(manage('shell','-c',"from subscriptions.models import Subscription; from django.db import connection; print('COMMITTED_SUBSCRIPTIONS='+str(list(Subscription.objects.values_list('user_id','status')))); c=connection.cursor(); c.execute('select current_database(), current_schema(), inet_server_port(), pg_backend_pid()'); print('GRANT_DB='+str(c.fetchone()))"))
                with page.expect_response(lambda response: 'api/settings' in response.url and response.request.method == 'GET') as pending_settings:
                    page.get_by_role('button',name='Check activation',exact=True).click()
                pending_settings.value.finished()
                page.wait_for_load_state('networkidle')
                expect(page.locator('#billingActivationCheck')).to_be_hidden()
                expect(page.locator('#billingBindBtn')).to_be_visible()
                expect(page.locator('#cardBindingModal')).to_be_hidden()
                report('BROWSER_PASS: manual access refresh removes pending state and retains explicit rebind')
            finally:
                browser.close()
        stage='database assertions'
        output=manage('shell','-c',VERIFY)
        for line in output.splitlines():
            if line.startswith('DATABASE_PASS:'):
                report(line)
        stage='Chromium canonical lifecycle'
        # Keep the legacy scenario and every assertion above; exercise a second
        # synthetic identity through real consent and canonical invoice creation.
        from billing_lifecycle_browser import SEED_LIFECYCLE, run_browser_lifecycle
        server.terminate()
        server.wait(timeout=15)
        env['MINIAPP_DEV_TG_USER_ID']='788882'
        env['ACCEPTANCE_PROVIDER_ORIGIN']=base
        env['MONO_BILLING_RETURN_URL']=base+'/billing/mono/return'
        report(manage('shell','-c',SEED_LIFECYCLE))
        server=subprocess.Popen([sys.executable,'-I','-c',BOOTSTRAP,str(APP),'runserver','127.0.0.1:'+str(port),'--noreload'],cwd=APP,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        for _ in range(180):
            if server.poll() is not None:
                raise AssertionError('lifecycle local server exited')
            try:
                with urllib.request.urlopen(base+'/app/',timeout=1) as response:
                    if response.status == 200:
                        break
            except Exception:
                time.sleep(.25)
        else:
            raise AssertionError('lifecycle server readiness timeout')
        run_browser_lifecycle(base, manage, report)
        stage='Chromium unpaid continuation and retry'
        from billing_lifecycle_browser import SEED_UNPAID, run_unpaid_continuation
        server.terminate()
        server.wait(timeout=15)
        env['MINIAPP_DEV_TG_USER_ID']='788883'
        report(manage('shell','-c',SEED_UNPAID))
        server=subprocess.Popen([sys.executable,'-I','-c',BOOTSTRAP,str(APP),'runserver','127.0.0.1:'+str(port),'--noreload'],cwd=APP,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)
        for _ in range(180):
            assert server.poll() is None, 'unpaid local server exited'
            try:
                with urllib.request.urlopen(base+'/app/',timeout=1) as response:
                    if response.status == 200:
                        break
            except Exception:
                time.sleep(.25)
        else:
            raise AssertionError('unpaid server readiness timeout')
        run_unpaid_continuation(base, manage, report)
        success=True
    except Exception as exc:
        # Avoid child stderr, cookies, connection strings, auth URLs and browser traces.
        report('FAIL stage='+stage+' type='+type(exc).__name__)
        if stage.startswith('Chromium') and isinstance(exc, (AssertionError,)):
            report(str(exc))
    finally:
        try:
            if server is not None:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=10)
                assert server.poll() is not None
            if attempted:
                removed=subprocess.run(['docker','rm','-f',container],env=env,capture_output=True,timeout=45)
                remaining=command(['docker','ps','-a','--filter','name=^/'+container+'$','--format','{{.Names}}']).strip()
                assert not remaining, 'owned container remains'
            report('CLEANUP_PASS: server stopped; exact owned container absent')
        except Exception as exc:
            success=False
            report('CLEANUP_FAIL type='+type(exc).__name__)
        report('BILLING_LIFECYCLE_ACCEPTANCE_'+('PASS' if success else 'FAIL'))
        args.log.parent.mkdir(parents=True,exist_ok=True)
        with args.log.open('a',encoding='utf-8') as log:
            log.write('\n'.join(lines)+'\n')
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
