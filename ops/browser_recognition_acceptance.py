#!/usr/bin/env python
"""Real Chromium PWA statement batch acceptance; synthetic parser only."""
from __future__ import annotations
import json, os, socket, subprocess, sys, time, urllib.request, uuid
from pathlib import Path
from playwright.sync_api import sync_playwright, expect
ROOT=Path(__file__).resolve().parents[1]; APP=ROOT/'admin_service'; EVID=ROOT/'.hermes/delivery/20261026_151435_2c225a'; USER='888221'; PNG=b'\x89PNG\r\n\x1a\n'+b'0'*64
def free_port():
    with socket.socket() as s: s.bind(('127.0.0.1',0)); return s.getsockname()[1]
def wait(url, proc):
    end=time.time()+60
    while time.time()<end:
        if proc.poll() is not None: raise RuntimeError('server_exited')
        try:
            if urllib.request.urlopen(url,timeout=1).status < 500: return
        except Exception: time.sleep(.25)
    raise RuntimeError('server_timeout')
def main():
    EVID.mkdir(parents=True,exist_ok=True); log=EVID/'browser-recognition-statement.log'; shot=EVID/'browser-recognition-statement-final.png'; run=uuid.uuid4().hex[:10]; db='mcf-recognition-'+run; redis=db+'-redis'; server=None; result=[]
    env=os.environ.copy(); env.update(DJANGO_SETTINGS_MODULE='config.settings',DJANGO_DEBUG='true',DJANGO_SECRET_KEY='synthetic-'+run,DJANGO_ALLOWED_HOSTS='127.0.0.1,localhost',MINIAPP_DEV_TG_USER_ID=USER,SESSION_COOKIE_SECURE='false',CSRF_COOKIE_SECURE='false',SECURE_SSL_REDIRECT='false',OPS_SYNTHETIC_TESTS='1',RECOGNITION_ACCEPTANCE_SOURCE='statement')
    try:
        subprocess.run(['docker','run','-d','--rm','--name',db,'-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','POSTGRES_DB=recognition','-p','127.0.0.1::5432','postgres:16-alpine'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); subprocess.run(['docker','run','-d','--rm','--name',redis,'-p','127.0.0.1::6379','redis:7-bookworm'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        env['REDIS_URL']='redis://127.0.0.1:'+subprocess.check_output(['docker','port',redis,'6379/tcp'],text=True).strip().rsplit(':',1)[-1]+'/0'; hp=subprocess.check_output(['docker','port',db,'5432/tcp'],text=True).strip().rsplit(':',1)[-1]; env['DATABASE_URL']=f'postgresql://postgres@127.0.0.1:{hp}/recognition'
        while subprocess.run(['docker','exec',db,'pg_isready','-U','postgres'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode: time.sleep(.3)
        subprocess.run([sys.executable,'manage.py','migrate','--noinput'],cwd=APP,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); seed=f"import sys;sys.path.insert(0,{str(ROOT/'ops')!r});sys.path.insert(0,{str(ROOT)!r});from recognition_quality_browser_acceptance import seed;seed()"; subprocess.run([sys.executable,'manage.py','shell','-c',seed],cwd=APP,env=env,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        p=free_port(); server=subprocess.Popen([sys.executable,str(ROOT/'ops/recognition_acceptance_server.py'),str(p)],cwd=APP,env=env); base=f'http://127.0.0.1:{p}'; wait(base+'/app/',server); os.environ.update(env); os.environ['DJANGO_ALLOW_ASYNC_UNSAFE']='true'; sys.path.insert(0,str(APP)); import django; django.setup()
        from transactions.models import Transaction
        before=Transaction.objects.filter(tg_user_id=int(USER)).count()
        with sync_playwright() as pw:
            browser=pw.chromium.launch(headless=True,channel='msedge',args=['--disable-gpu']); page=browser.new_page(viewport={'width':390,'height':844},locale='uk-UA'); assert page.request.post(base+'/app/api/auth/dev').ok; page.goto(base+'/app/',wait_until='networkidle'); expect(page.locator('#dashboardApp')).to_be_visible(timeout=20000); page.locator('[data-target="add"]').click(); page.locator('#aiImageLaunchBtn').click(); page.locator('#aiImageInput').set_input_files({'name':'statement.png','mimeType':'image/png','buffer':PNG})
            with page.expect_response(lambda r:r.url.endswith('/api/ai-image/parse')) as parsed: page.locator('#aiImageParseBtn').click()
            assert parsed.value.ok; expect(page.locator('#aiImageBatchCard')).to_be_visible(); ack=page.locator('#aiImageBatchAcknowledge'); expect(ack).to_be_visible(); assert page.locator('.ai-image-batch-row.is-review').count()==1; batch=page.request.get(base+'/app/api/ai-image/batch').json()['batch']; assert batch['review_rows'] and batch['skipped_items_count']==1 and not batch.get('review_acknowledged', False)
            uncertain=page.request.get(base+'/app/api/ai-image/batch').json()['batch']['items'][0]['suggestion']
            assert uncertain['amount']=='125.00' and uncertain['comment']=='Synthetic coffee'
            assert uncertain.get('transaction_date') is None and uncertain.get('currency') == ''
            first=page.locator('.ai-image-batch-row').nth(0)
            first.locator('.ai-image-batch-amount').fill('130')
            first.locator('.ai-image-batch-name').fill('Edited coffee')
            page.route('**/app/api/ai-image/batch/item',lambda route: route.fulfill(status=503,content_type='application/json',body=json.dumps({'error':{'code':'synthetic_ack_failure'}})) if route.request.method=='POST' else route.continue_())
            ack.click()
            expect(page.locator('#aiImageBatchStatus')).to_have_class('form-status error')
            expect(ack).to_be_enabled()
            assert page.request.get(base+'/app/api/ai-image/batch').json()['batch']['review_acknowledged'] is False
            assert Transaction.objects.filter(tg_user_id=int(USER)).count()==before
            page.unroute('**/app/api/ai-image/batch/item')
            with page.expect_response(lambda r:r.url.endswith('/api/ai-image/batch/item') and r.request.method=='POST') as acknowledged:
                ack.click()
            assert acknowledged.value.ok
            assert acknowledged.value.json()['batch']['review_acknowledged'] is True
            expect(ack).to_have_text('Перевірку виписки підтверджено')
            expect(ack).to_be_disabled()
            assert page.request.get(base+'/app/api/ai-image/batch').json()['batch']['review_acknowledged'] is True
            first=page.locator('.ai-image-batch-row').nth(0)
            expect(first.locator('.ai-image-batch-amount')).to_have_value('130')
            expect(first.locator('.ai-image-batch-name')).to_have_value('Edited coffee')
            page.locator('#aiImageBatchAccount').select_option(index=1)
            before_review=Transaction.objects.filter(tg_user_id=int(USER)).count()
            first.get_by_role('button',name='Перевірити').click()
            expect(page.locator('#txAmount')).to_have_value('130.00')
            page.locator('#txAccount').select_option(index=0)
            page.locator('#txCategory').select_option(index=0)
            page.locator('#txDate').fill('2026-10-19')
            expect(page.locator('#txDraftCard')).to_be_visible()
            with page.expect_response(lambda r:r.url.endswith('/api/transactions/draft')) as draft, page.expect_response(lambda r:r.url.endswith('/api/transactions/confirm')) as confirmed:
                page.locator('#txConfirmBtn').click()
            assert draft.value.ok
            assert confirmed.value.ok
            assert Transaction.objects.filter(tg_user_id=int(USER)).count()==before_review+1
            retry=page.request.post(base+'/app/api/transactions/confirm',data=confirmed.value.request.post_data_json)
            assert retry.status in (200,400,409)
            assert Transaction.objects.filter(tg_user_id=int(USER)).count()==before_review+1
            tx=Transaction.objects.filter(tg_user_id=int(USER)).latest('id')
            assert str(tx.amount)=='130.00' and str(tx.date)=='2026-10-19' and tx.account.currency=='UAH'
            assert 'Edited coffee' in json.dumps(tx.metadata or {}) or 'Edited coffee' in str(tx.comment)
            final_batch=page.request.get(base+'/app/api/ai-image/batch').json()['batch']
            assert final_batch['committed_count']==1 and final_batch['pending_count']==1, json.dumps(final_batch,ensure_ascii=False)
            assert final_batch['review_rows'] and final_batch['skipped_items_count']==1
            page.screenshot(path=str(shot))
            browser.close()
        result.append('PASS statement batch: visible acknowledgement; failed ack retained lock/error; successful ack authoritative; edited valid item preserved; one valid expense confirmed; review row excluded; isolated DB')
    except Exception as e: result.append('FAIL '+json.dumps({'stage':'browser_acceptance','type':type(e).__name__,'error_code':getattr(e,'code','acceptance_failure')})); raise
    finally:
        if server: server.terminate(); server.wait(timeout=10)
        subprocess.run(['docker','rm','-f',db],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); subprocess.run(['docker','rm','-f',redis],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); log.write_text('\n'.join(result)+'\ncleanup=server_stopped,own_postgres_removed\nscope=synthetic parser fixture; no OCR/external API/model accuracy\n',encoding='utf-8')
    print(result[0]); return 0
if __name__=='__main__': raise SystemExit(main())