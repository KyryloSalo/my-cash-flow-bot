#!/usr/bin/env python
"""Disposable PostgreSQL + real Chromium consent regression. NOT real card/provider acceptance.

Run: python ops/billing_offer_variant_acceptance.py --server
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
DEFAULT_LOG = ROOT / ".hermes/delivery/20261114_032642_d18c51/browser.log"
BOOTSTRAP = '''import sys, socket, runpy
sys.path.insert(0, sys.argv.pop(1))
_original_connect = socket.socket.connect
def local_connect(self, address):
    if isinstance(address, tuple) and address[0] not in ('127.0.0.1', 'localhost', '::1'):
        raise RuntimeError('acceptance denied non-loopback network')
    return _original_connect(self, address)
socket.socket.connect = local_connect
sys.argv[0] = 'manage.py'
runpy.run_path('manage.py', run_name='__main__')
'''
SEED = '''
from datetime import timedelta
from django.utils import timezone
from miniapp.models import AcquisitionSession, PartnerLink
from miniapp.partner_offers import claim_partner_offer
from users.models import TelegramUser
u, _ = TelegramUser.objects.get_or_create(tg_user_id=777777, defaults={'first_name':'Synthetic acceptance'})
l = PartnerLink.objects.create(name='Synthetic acceptance only', code='acceptance_partner', trial_days=51)
a = AcquisitionSession.objects.create(user=u, first_partner_link=l, last_partner_link=l, expires_at=timezone.now()+timedelta(days=1))
c = claim_partner_offer(user=u, acquisition=a)
assert c.offer_id == 'partner_link:'+str(l.pk)
print('FIXTURE='+c.offer_id)
'''
VERIFY = '''
import uuid
from datetime import timedelta
from django.utils import timezone
from miniapp.models import FunnelEvent, AcquisitionSession
from miniapp.funnel import record_server_event, FunnelValidationError, validate_client_event_payload
from subscriptions.models import BillingConsent, Payment, BillingProfile
from subscriptions.consent import attach_consent_payment, finalize_paid_consent, accept_billing_consent
from users.models import TelegramUser, UserAdminState
u=TelegramUser.objects.get(tg_user_id=777777)
c=BillingConsent.objects.get(user=u)
assert c.offer_id.startswith('partner_link:') and c.trial_days == 51
assert Payment.objects.count() == 0 and BillingProfile.objects.count() == 0
assert FunnelEvent.objects.filter(acquisition_session=c.acquisition_session,event_name='billing_consent_accepted',offer_variant=c.offer_id).count() == 1
p=Payment.objects.create(user=u,amount='1.00',currency='UAH',kind=Payment.Kind.BIND,status=Payment.Status.PAID,provider='synthetic-local-only')
attach_consent_payment(consent=c,payment=p)
attach_consent_payment(consent=c,payment=p)
now=timezone.now()
assert finalize_paid_consent(payment=p,first_renewal_at=now) == 1
assert finalize_paid_consent(payment=p,first_renewal_at=now+timedelta(days=1)) == 0
c.refresh_from_db()
assert c.first_renewal_at == now and c.payment_id == p.pk
for name,key in [('billing_consent_accepted','consent:'+str(c.pk)),('bind_invoice_created','bind_invoice:'+str(p.pk)),('payment_success','payment:'+str(p.pk)+':success')]:
    e=FunnelEvent.objects.get(acquisition_session=c.acquisition_session,event_name=name)
    assert e.offer_variant == c.offer_id and e.idempotency_key == key
for bad in ['https://private.example/secret', 'x@y.test', '<script>', 'x'*65]:
    try:
        record_server_event(acquisition=c.acquisition_session,event_name='payment_success',idempotency_key='bad:'+uuid.uuid4().hex,offer_variant=bad)
    except FunnelValidationError:
        pass
    else:
        raise AssertionError('invalid server offer accepted')
    try:
        validate_client_event_payload({'event_name':'landing_view','event_id':str(uuid.uuid4()),'offer_variant':bad})
    except FunnelValidationError:
        pass
    else:
        raise AssertionError('invalid client offer accepted')
for identity,course,expected in [(777778,False,'organic_30d_1uah_499uah'),(777779,True,'askills_course_90d')]:
    person=TelegramUser.objects.create(tg_user_id=identity,first_name='Synthetic')
    if course:
        UserAdminState.objects.update_or_create(telegram_user=person,defaults={'pending_start_payload':'course'})
    acq=AcquisitionSession.objects.create(user=person,expires_at=timezone.now()+timedelta(days=1))
    kwargs=dict(user=person,acquisition=acq,idempotency_key=str(uuid.uuid4()),trial_days=90 if course else 30,copy_locale='uk')
    consent,created=accept_billing_consent(**kwargs)
    replay,created_again=accept_billing_consent(**kwargs)
    assert created and not created_again and replay.pk == consent.pk and consent.offer_id == expected
    assert FunnelEvent.objects.filter(acquisition_session=acq,event_name='billing_consent_accepted',offer_variant=expected).count() == 1
assert Payment.objects.count() == 1 and BillingProfile.objects.count() == 0
print('DATABASE_PASS: exact partner ID; three event names/keys; replay deduplication; confirmation timestamp preserved; organic/course; invalid/oversize rejected; synthetic payment only')
'''


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
               DJANGO_ALLOWED_HOSTS='127.0.0.1,localhost', MINIAPP_DEV_TG_USER_ID='777777',
               SECURE_SSL_REDIRECT='false', SESSION_COOKIE_SECURE='false', CSRF_COOKIE_SECURE='false',
               MONO_API_BASE_URL='http://127.0.0.1:9', MONO_MERCHANT_TOKEN='', REDIS_URL='redis://127.0.0.1:63999/0')
    container = 'mcf-billing-acceptance-'+uuid.uuid4().hex[:12]
    server = None
    attempted = False
    success = False
    stage = 'bootstrap'
    def command(argv, *, django=False):
        result = subprocess.run(argv, cwd=APP if django else ROOT, env=env, capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise AssertionError('subprocess failed at '+stage+' (exit '+str(result.returncode)+')')
        return result.stdout
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
        stage='partner fixture'
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
        stage='Chromium consent'
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True)
            try:
                context=browser.new_context(service_workers='block')
                context.route('**/*',lambda route: route.continue_() if route.request.url.startswith(base+'/') else route.abort())
                page=context.new_page()
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
                endpoint='/app/api/funnel/billing-consent'
                key=str(uuid.uuid4())
                rejected=post(endpoint,{'accepted':False,'idempotency_key':key})
                assert rejected['status'] == 400 and rejected['body']['error']['code'] == 'consent_required', 'explicit confirmation rejection changed'
                invalid=post(endpoint,{'accepted':True,'idempotency_key':key,'offer_variant':'forged'})
                assert invalid['status'] == 400 and invalid['body']['error']['code'] == 'invalid_payload'
                oversized=post(endpoint,{'accepted':True,'idempotency_key':key,'copy_locale':'x'*20000})
                assert oversized['status'] == 413 and oversized['body']['error']['code'] == 'payload_too_large'
                payload={'accepted':True,'idempotency_key':key,'copy_locale':'uk'}
                first=post(endpoint,payload)
                assert first['status'] == 200, 'partner consent expected 200; got '+str(first)
                assert first['body']['created'] is True and first['body']['terms']['offer_id'] == offer
                assert first['body']['terms']['trial_days'] == 51
                replay=post(endpoint,payload)
                assert replay['status'] == 200 and replay['body']['created'] is False
                assert replay['body']['consent_id'] == first['body']['consent_id'] and replay['body']['terms'] == first['body']['terms']
                report('BROWSER_PASS: real /app/; dev-auth; accepted=false rejected; accepted=true 200; exact '+offer+'; identical replay terms/consent ID')
            finally:
                browser.close()
        stage='database assertions'
        output=manage('shell','-c',VERIFY)
        for line in output.splitlines():
            if line.startswith('DATABASE_PASS:'):
                report(line)
        success=True
    except Exception as exc:
        # Avoid child stderr, cookies, connection strings, auth URLs and browser traces.
        report('FAIL stage='+stage+' type='+type(exc).__name__)
        if isinstance(exc,AssertionError) and stage == 'Chromium consent':
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
        report('BILLING_OFFER_VARIANT_ACCEPTANCE_'+('PASS' if success else 'FAIL'))
        args.log.parent.mkdir(parents=True,exist_ok=True)
        with args.log.open('a',encoding='utf-8') as log:
            log.write('\n'.join(lines)+'\n')
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
