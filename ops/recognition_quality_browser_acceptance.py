#!/usr/bin/env python
"""Real local PWA capture acceptance with an explicitly synthetic HTTP provider.

No PWA API interception, production access, real inference or user data. The
provider fixture proves capture/correlation, NOT recognition model accuracy.
"""
from __future__ import annotations

import argparse
import base64
from datetime import timedelta
from email.parser import BytesParser
from email.policy import default as email_policy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import tempfile
import time
import uuid
from unittest.mock import patch
from urllib.request import Request, urlopen

from playwright.sync_api import sync_playwright, expect
from goals_browser_acceptance import APP, ROOT, USER_ID, free_port, seed as seed_finance, wait_for_server


class SyntheticProvider(BaseHTTPRequestHandler):
    calls = 0

    def log_message(self, *args):
        pass  # Never log recognition inputs.

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        type(self).calls += 1
        image = base64.b64decode(request.get('image_base64', ''))
        failed = request.get('text') == 'synthetic provider failure' or image.endswith(b'synthetic provider failure')
        status = 503 if failed else 200
        parsed = {'intent': 'expense', 'is_candidate_tx': True, 'amount': '125.00',
                  'currency': 'UAH', 'currency_explicit': True, 'category_slug': 'food',
                  'comment': 'Synthetic coffee', 'confidence': 0.95}
        body = json.dumps(parsed).encode()
        if self.path.endswith('/parse-audio'):
            body = json.dumps({'transcript': 'Synthetic coffee 125 UAH', 'parsed': parsed}).encode()
        elif self.path.endswith('/parse-image'):
            body = json.dumps(dict(parsed, mode='single_tx', type='expense', source='receipt', category_hint='food',
                                   transaction_date='2026-10-19')).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class SyntheticTelegram(BaseHTTPRequestHandler):
    documents = []
    fail_next = False

    def log_message(self, *args):
        pass

    def do_POST(self):
        assert self.path == '/botSYNTHETIC-LOCAL-ONLY/sendDocument'
        raw = self.rfile.read(int(self.headers['Content-Length']))
        message = BytesParser(policy=email_policy).parsebytes(
            b'Content-Type: ' + self.headers['Content-Type'].encode() + bytes([13, 10, 13, 10]) + raw)
        fields = {part.get_param('name', header='content-disposition'): part.get_payload(decode=True)
                  for part in message.iter_parts()}
        failed = type(self).fail_next
        type(self).fail_next = False
        if not failed:
            type(self).documents.append(fields)
        body = json.dumps({'ok': not failed, 'result': {'message_id': len(type(self).documents)}}).encode()
        self.send_response(503 if failed else 200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def verify_photo_delivery(capture, original, *, fail_first=False):
    """Real task + multipart HTTP; only the external Telegram endpoint is synthetic."""
    from admin_notifications.models import AdminNotificationLog
    from django.test import override_settings
    from miniapp.tasks import deliver_recognition_photos_task
    outbox = AdminNotificationLog.objects.get(event_type='recognition_photo', payload__capture_id=capture.pk)
    endpoint = ThreadingHTTPServer(('127.0.0.1', 0), SyntheticTelegram)
    threading.Thread(target=endpoint.serve_forever, daemon=True).start()
    SyntheticTelegram.documents = []
    SyntheticTelegram.fail_next = fail_first

    def local_transport(request, *args, **kwargs):
        assert request.full_url == 'https://api.telegram.org/botSYNTHETIC-LOCAL-ONLY/sendDocument'
        local = Request(f'http://127.0.0.1:{endpoint.server_port}/botSYNTHETIC-LOCAL-ONLY/sendDocument',
                        data=request.data, headers=dict(request.header_items()), method='POST')
        return urlopen(local, *args, **kwargs)

    try:
        with override_settings(ADMIN_NOTIFICATIONS_ENABLED=True, ADMIN_TELEGRAM_IDS=[900001],
                               TELEGRAM_BOT_TOKEN='SYNTHETIC-LOCAL-ONLY'):
            with patch('common.telegram_upload.request.urlopen', side_effect=local_transport):
                if fail_first:
                    result = deliver_recognition_photos_task.run([outbox.pk])
                    assert result['failed'] == 1, result
                    outbox.refresh_from_db()
                    assert outbox.status == 'failed'
                    assert deliver_recognition_photos_task.run([outbox.pk])['failed'] == 0
                    assert not SyntheticTelegram.documents
                    with patch('miniapp.recognition_photo_notifications.timezone.now', return_value=outbox.next_retry_at):
                        result = deliver_recognition_photos_task.run([outbox.pk])
                else:
                    result = deliver_recognition_photos_task.run([outbox.pk])
                assert result['sent'] == 1, result
                assert deliver_recognition_photos_task.run([outbox.pk])['sent'] == 0
        outbox.refresh_from_db()
        assert outbox.status == 'sent' and outbox.sent_at
        assert len(SyntheticTelegram.documents) == 1
        fields = SyntheticTelegram.documents[0]
        assert fields['document'] == original
        assert fields['chat_id'] == b'900001'
        caption = fields['caption'].decode()
        assert f'tg_user_id={USER_ID}' in caption and capture.outcome in caption, caption
        if capture.outcome == 'parsed':
            assert '125' in caption and 'UAH' in caption, caption
    finally:
        endpoint.shutdown()
        endpoint.server_close()


def seed():
    seed_finance()
    from accounts.models import Account
    from categories.models import Category
    from django.utils import timezone
    from miniapp.models import RecognitionCaptureConfig
    Account.objects.filter(tg_user_id=USER_ID, account_type='savings').update(is_active=False)
    Category.objects.filter(tg_user_id=USER_ID, kind='expense').update(slug='food')
    RecognitionCaptureConfig.objects.create(enabled=True, all_images_enabled=True, cohort_ids=[USER_ID],
                                            expires_at=timezone.now() + timedelta(days=7))


def scenarios(base, evidence, channel):
    # Playwright sync API owns an event loop; this single-threaded acceptance
    # process reads only its disposable synthetic database, never production.
    os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'
    import django
    django.setup()
    from miniapp.models import RecognitionCapture, RecognitionCaptureConfig
    from transactions.models import Transaction

    def captures():
        return RecognitionCapture.objects.filter(user_id=USER_ID).order_by('id')

    before = Transaction.objects.filter(tg_user_id=USER_ID).count()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, channel=channel, args=[
            '--use-fake-device-for-media-stream', '--use-fake-ui-for-media-stream'])
        page = browser.new_page(viewport={'width': 390, 'height': 844}, locale='uk-UA')
        errors = []
        page.on('pageerror', lambda exc: errors.append(str(exc)))
        try:
            response = page.request.post(base + '/app/api/auth/dev', data={})
            assert response.ok, response.status
            page.goto(base + '/app/', wait_until='networkidle')
            expect(page.locator('#dashboardApp')).to_be_visible(timeout=20000)
            page.locator('[data-target="add"]').click()
            page.locator('#aiTextLaunchBtn').click()

            def recognize(text):
                page.locator('#aiTextInput').fill(text)
                with page.expect_response(lambda r: r.url.endswith('/ai-text/parse')) as pending:
                    page.locator('#aiTextParseBtn').click()
                return pending.value

            response = recognize('Synthetic coffee 125 UAH')
            assert response.ok, response.json()
            expect(page.locator('#txDraftCard')).to_be_visible()
            row = captures().latest('id')
            assert row.raw_text == 'Synthetic coffee 125 UAH'
            assert row.provider_result
            assert row.normalized_result
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before
            page.locator('#txAmount').fill('130')
            with page.expect_response(lambda r: r.url.endswith('/transactions/confirm')) as pending:
                page.locator('#txConfirmBtn').click()
            assert pending.value.ok, pending.value.json()
            row.refresh_from_db()
            assert row.outcome == 'confirmed', row.outcome
            assert row.correction and '130' in json.dumps(row.correction), row.correction
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before + 1
            print('PASS real PWA text -> edited amount -> confirmation -> linked capture; no pre-confirm ledger write')

            response = recognize('Synthetic cancelled coffee')
            assert response.ok
            expect(page.locator('#txDraftCard')).to_be_visible()
            cancelled = captures().latest('id')
            with page.expect_response(lambda r: r.url.endswith('/transactions/cancel')) as pending:
                page.locator('#txCancelBtn').click()
            assert pending.value.ok, pending.value.json()
            expect(page.locator('#txDraftCard')).to_be_hidden()
            # Wait for the actual cancellation request before DB readback.
            page.wait_for_load_state('networkidle')
            cancelled.refresh_from_db()
            assert cancelled.outcome == 'cancelled', cancelled.outcome
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before + 1
            print('PASS real PWA cancellation remains separate from recognition correctness')

            response = recognize('synthetic provider failure')
            assert response.status == 503, response.status
            failed = captures().latest('id')
            assert failed.outcome in {'failed', 'error'}, failed.outcome
            print('PASS provider failure captured without ledger write')

            # Real image upload through PWA -> provider fixture -> private file.
            RecognitionCaptureConfig.objects.update(cohort_ids=[])
            page.locator('#aiImageLaunchBtn').click()
            png = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aA1cAAAAASUVORK5CYII=')
            page.locator('#aiImageInput').set_input_files({'name': 'synthetic.png', 'mimeType': 'image/png', 'buffer': png})
            with page.expect_response(lambda r: r.url.endswith('/ai-image/parse')) as pending:
                page.locator('#aiImageParseBtn').click()
            assert pending.value.ok, pending.value.json()
            expect(page.locator('#txDraftCard')).to_be_visible()
            image = captures().latest('id')
            assert image.source == 'image' and image.provider_result and image.normalized_result
            assert (Path(os.environ['RECOGNITION_QUALITY_STORAGE_ROOT']) / image.raw_media).read_bytes() == png
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before + 1
            verify_photo_delivery(image, png)
            with page.expect_response(lambda r: r.url.endswith('/transactions/cancel')) as pending:
                page.locator('#txCancelBtn').click()
            assert pending.value.ok
            image.refresh_from_db()
            assert image.outcome == 'cancelled'
            print('PASS PWA photo outside cohort -> private original -> Telegram multipart task -> one delivery; no pre-confirm ledger write')

            page.locator('#aiImageLaunchBtn').click()
            failed_png = png + b'synthetic provider failure'
            page.locator('#aiImageInput').set_input_files({'name': 'synthetic-failed.png', 'mimeType': 'image/png', 'buffer': failed_png})
            with page.expect_response(lambda r: r.url.endswith('/ai-image/parse')) as pending:
                page.locator('#aiImageParseBtn').click()
            assert pending.value.status == 503, pending.value.status
            failed_image = captures().latest('id')
            assert failed_image.source == 'image' and failed_image.outcome == 'failed'
            assert (Path(os.environ['RECOGNITION_QUALITY_STORAGE_ROOT']) / failed_image.raw_media).read_bytes() == failed_png
            verify_photo_delivery(failed_image, failed_png, fail_first=True)
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before + 1
            RecognitionCaptureConfig.objects.update(cohort_ids=[USER_ID])
            print('PASS failed PWA photo outside cohort -> original retained -> transient Telegram error -> successful retry, no duplicate')

            voice_button = page.locator('#aiVoiceRecordBtn')
            voice_button.click()
            expect(voice_button).to_have_attribute('aria-pressed', 'true', timeout=15000)
            expect(page.locator('#aiVoiceTimer')).not_to_have_text('0:00', timeout=10000)
            with page.expect_response(lambda r: r.url.endswith('/ai-voice/parse')) as pending:
                voice_button.click()
            assert pending.value.ok, pending.value.json()
            expect(page.locator('#txDraftCard')).to_be_visible()
            voice = captures().latest('id')
            assert voice.source == 'voice' and voice.raw_media and voice.raw_media_size > 0
            assert voice.provider_result['transcript'] and voice.normalized_result['transcript']
            assert Transaction.objects.filter(tg_user_id=USER_ID).count() == before + 1
            with page.expect_response(lambda r: r.url.endswith('/transactions/cancel')) as pending:
                page.locator('#txCancelBtn').click()
            assert pending.value.ok
            voice.refresh_from_db()
            assert voice.outcome == 'cancelled'
            print('PASS real PWA microphone fixture -> voice/transcript/private bytes -> linked cancellation')

            page.locator('#aiTextLaunchBtn').click()
            count = captures().count()
            RecognitionCaptureConfig.objects.update(enabled=False)
            response = recognize('Synthetic disabled collection')
            assert response.ok
            assert captures().count() == count
            assert SyntheticProvider.calls == 7, SyntheticProvider.calls
            assert not errors, errors
            page.screenshot(path=str(evidence / 'recognition-quality-pwa.png'))
            print('PASS kill switch stops capture; exactly one provider call per attempt; no JS errors')
        finally:
            browser.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', action='store_true')
    parser.add_argument('--seed', action='store_true')
    parser.add_argument('--browser-channel', default='msedge')
    parser.add_argument('--evidence-dir', type=Path, default=ROOT / '.hermes/delivery/20261019_192649_e04eb4')
    args = parser.parse_args()
    if args.seed:
        seed()
        return 0
    if not args.server:
        parser.error('choose --server')
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:12]
    container = 'vydno-recognition-browser-' + run_id
    redis_container = container + '-redis'
    storage = tempfile.TemporaryDirectory(prefix='recognition-browser-', dir=os.environ['TMPDIR'])
    server = None
    provider = ThreadingHTTPServer(('127.0.0.1', 0), SyntheticProvider)
    threading.Thread(target=provider.serve_forever, daemon=True).start()
    env = os.environ.copy()
    for key in list(env):
        if key.endswith('_FILE') and key.startswith(('DJANGO_', 'DATABASE_', 'BOT_')):
            env.pop(key)
    env.update(DJANGO_SETTINGS_MODULE='config.settings', DJANGO_DEBUG='true',
               OPS_SYNTHETIC_TESTS='1', ADMIN_NOTIFICATIONS_ENABLED='true',
               ADMIN_TELEGRAM_IDS='900001', TELEGRAM_BOT_TOKEN='SYNTHETIC-LOCAL-ONLY',
               RECOGNITION_QUALITY_STORAGE_ROOT=storage.name,
               DJANGO_SECRET_KEY='synthetic-recognition-browser-' + run_id,
               DJANGO_ALLOWED_HOSTS='127.0.0.1,localhost,testserver', MINIAPP_DEV_TG_USER_ID=str(USER_ID),
               SESSION_COOKIE_SECURE='false', CSRF_COOKIE_SECURE='false', SECURE_SSL_REDIRECT='false',
               REDIS_URL='redis://127.0.0.1:63999/0', BILLING_INTERNAL_TOKEN='synthetic-local-only',
               MINIAPP_BOT_INTERNAL_BASE_URL=f'http://127.0.0.1:{provider.server_port}')
    try:
        subprocess.run(['docker', 'run', '-d', '--rm', '--name', redis_container, '-p', '127.0.0.1::6379',
                        'redis:7-bookworm'], check=True, capture_output=True)
        redis_port = subprocess.check_output(['docker', 'port', redis_container, '6379/tcp'], text=True).strip().rsplit(':', 1)[-1]
        env['REDIS_URL'] = f'redis://127.0.0.1:{redis_port}/0'
        subprocess.run(['docker', 'run', '-d', '--rm', '--name', container, '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-e', 'POSTGRES_DB=recognition_browser', '-p', '127.0.0.1::5432', 'postgres:16-alpine'], check=True, capture_output=True)
        port = subprocess.check_output(['docker', 'port', container, '5432/tcp'], text=True).strip().rsplit(':', 1)[-1]
        env['DATABASE_URL'] = f'postgresql://postgres@127.0.0.1:{port}/recognition_browser'
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if subprocess.run(['docker', 'exec', container, 'pg_isready', '-U', 'postgres'], capture_output=True).returncode == 0:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError('local PostgreSQL readiness timed out')
        subprocess.run([sys.executable, 'manage.py', 'migrate', '--noinput'], cwd=APP, env=env, check=True, stdout=subprocess.DEVNULL)
        subprocess.run([sys.executable, str(Path(__file__).resolve()), '--seed'], cwd=APP, env=env, check=True)
        os.environ.update(env)
        sys.path.insert(0, str(APP))
        port = free_port()
        with (args.evidence_dir / 'recognition-browser-server.log').open('w', encoding='utf-8') as log:
            server = subprocess.Popen([sys.executable, 'manage.py', 'runserver', f'127.0.0.1:{port}', '--noreload'], cwd=APP, env=env, stdout=log, stderr=subprocess.STDOUT)
            base = f'http://127.0.0.1:{port}'
            wait_for_server(base + '/app/', server)
            scenarios(base, args.evidence_dir, args.browser_channel)
        print('RECOGNITION_REAL_PWA_CAPTURE_ACCEPTANCE_PASS (synthetic provider; not model accuracy)')
        return 0
    finally:
        if server:
            server.terminate()
            server.wait(timeout=15)
        provider.shutdown()
        provider.server_close()
        subprocess.run(['docker', 'rm', '-f', container], capture_output=True, check=False)
        subprocess.run(['docker', 'rm', '-f', redis_container], capture_output=True, check=False)
        storage.cleanup()


if __name__ == '__main__':
    raise SystemExit(main())
