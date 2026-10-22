#!/usr/bin/env python
"""Real Chromium acceptance for Monobank checkout return -> authenticated PWA."""
from __future__ import annotations

import argparse, json, os, secrets, socket, subprocess, sys, time
import urllib.error
import urllib.request
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

# Reuse the proven disposable-service/TLS/HTTP primitives without changing them.
from browser_login_acceptance import HOST, NoRedirect, check, port, run, shell, http, https_proxy

USERS = (910001, 910002)


def enable_checkout_post(proxy, base):
    """Forward form POSTs through the same loopback TLS boundary as GETs."""
    def do_POST(self):
        check(self.headers.get("Host") == f"{HOST}:{proxy.server_port}", "unexpected TLS host")
        headers = dict(self.headers.items())
        headers["X-Forwarded-Proto"] = "https"
        headers["Accept-Encoding"] = "identity"
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        request = urllib.request.Request(base + self.path, data=body, headers=headers, method="POST")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        try:
            response = opener.open(request, timeout=20)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            content = response.read()
            if response.status >= 400:
                print(f"CHECKOUT_POST_STATUS {response.status}")
                if response.status == 403:
                    reason = next((label for needle, label in (
                        (b"Origin checking failed", "origin-rejected"),
                        (b"Referer checking failed", "referer-rejected"),
                        (b"CSRF cookie not set", "csrf-cookie-missing"),
                        (b"CSRF token missing", "csrf-token-missing"),
                    ) if needle in content), "other")
                    print("CHECKOUT_POST_CSRF " + reason)
            self.send_response(response.status)
            for name, value in response.headers.items():
                if name.lower() not in {"connection", "transfer-encoding", "content-length"}:
                    self.send_header(name, value)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    proxy.RequestHandlerClass.do_POST = do_POST

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--log", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    app = root / "admin_service"
    if args.preflight:
        check((app / "manage.py").is_file(), "Django entrypoint missing")
        run(["docker", "info", "--format", "{{.ServerVersion}}"])
        with sync_playwright() as p: p.chromium.launch(headless=True).close()
        print("CHECKOUT_RETURN_ACCEPTANCE_PREFLIGHT_PASS")
        return 0

    events, containers = [], []
    server = proxy = proxy_thread = None
    stage, result = "setup", 1
    run_id = secrets.token_hex(6)
    db_password, internal_token = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
    tls_port = port()
    origin = f"https://{HOST}:{tls_port}"
    env = os.environ.copy()
    env.update(DJANGO_SETTINGS_MODULE="config.settings", DJANGO_DEBUG="true",
               DJANGO_SECRET_KEY=secrets.token_urlsafe(32),
               DJANGO_ALLOWED_HOSTS=f"127.0.0.1,localhost,admin,{HOST}",
               DOMAIN=f"{HOST}:{tls_port}", MINIAPP_URL=origin+"/app/",
               SECURE_SSL_REDIRECT="true", SESSION_COOKIE_SECURE="true",
               CSRF_COOKIE_SECURE="true", MINIAPP_DEV_AUTH_ENABLED="false",
               MINIAPP_BROWSER_LOGIN_SECRET=secrets.token_urlsafe(32),
               MINIAPP_BROWSER_LOGIN_TOKEN_TTL_SECONDS="900", BILLING_INTERNAL_TOKEN=internal_token,
               PUSH_INTERNAL_TOKEN=internal_token, BOT_TOKEN="0:local-acceptance-only")
    def passed(label):
        events.append("PASS " + label); print(events[-1])
    def fail(label):
        raise AssertionError(label)
    def counts():
        return shell(app, env, "import json\nfrom django.apps import apps\nkeys=('payment','ledger','transaction','subscription','invoice')\nprint(json.dumps({m._meta.label:m.objects.count() for m in apps.get_models() if any(k in m._meta.model_name.lower() for k in keys)}))")
    try:
        # Disposable Postgres/Redis and Django are identical to browser_login_acceptance.py.
        pg_name = f"mcf-checkout-pg-{run_id}"; redis_name = f"mcf-checkout-redis-{run_id}"
        containers += [pg_name, redis_name]
        run(["docker", "run", "-d", "--rm", "--name", pg_name, "-e", "POSTGRES_USER=acceptance", "-e", f"POSTGRES_PASSWORD={db_password}", "-e", "POSTGRES_DB=acceptance", "-p", "127.0.0.1::5432", "postgres:16"])
        pg_port = run(["docker", "port", pg_name, "5432/tcp"]).strip().rsplit(":", 1)[-1]
        run(["docker", "run", "-d", "--rm", "--name", redis_name, "-p", "127.0.0.1::6379", "redis:7-alpine"])
        redis_port = run(["docker", "port", redis_name, "6379/tcp"]).strip().rsplit(":", 1)[-1]
        for name, cmd in ((pg_name,["pg_isready","-U","acceptance","-d","acceptance"]),(redis_name,["redis-cli","ping"])):
            for _ in range(100):
                if subprocess.run(["docker","exec",name,*cmd],capture_output=True).returncode == 0: break
                time.sleep(.2)
            else: fail("isolated service readiness timeout")
        env.update(DATABASE_URL=f"postgresql://acceptance:{db_password}@127.0.0.1:{pg_port}/acceptance", REDIS_URL=f"redis://127.0.0.1:{redis_port}/0")
        run([sys.executable,"manage.py","migrate","--noinput"],cwd=app,env=env)
        shell(app,env,"import json\nfrom users.models import TelegramUser\nfor uid in (910001,910002): TelegramUser.objects.create(tg_user_id=uid,first_name='Acceptance',lang='uk',base_currency='UAH',onboarding_completed=True,onboarding_version=2)\nprint(json.dumps(True))")
        listen=port(); base=f"http://127.0.0.1:{listen}"; env["BILLING_INTERNAL_BASE_URL"]=base
        server=subprocess.Popen([sys.executable,"manage.py","runserver",f"127.0.0.1:{listen}","--noreload"],cwd=app,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        for _ in range(200):
            if server.poll() is not None: fail("Django exited before readiness")
            try:
                if http(base+"/app/",headers={"X-Forwarded-Proto":"https"})[0] == 200: break
            except OSError: pass
            time.sleep(.2)
        else: fail("Django readiness timeout")
        proxy,proxy_thread=https_proxy(base,tls_port,os.environ["TMPDIR"])
        enable_checkout_post(proxy, base)
        public={"Host":f"{HOST}:{tls_port}","X-Forwarded-Proto":"https"}
        stage="fixtures"
        fixture_code="""import json\nfrom decimal import Decimal\nfrom django.utils import timezone\nfrom users.models import TelegramUser\nfrom subscriptions.models import Payment\nfrom subscriptions.billing import build_bind_return_token\nout={}\nfor uid,flow,lang in ((910001,'bind','uk'),(910001,'recovery','en'),(910002,'bind','uk')):\n t=build_bind_return_token(user_id=uid,flow=flow,lang=lang); key='recovery_context' if flow=='recovery' else 'bind_context'\n Payment.objects.create(user_id=uid,provider='monobank',provider_payment_id='accept-'+flow+'-'+str(uid),amount=Decimal('1.00'),currency='UAH',status=Payment.Status.PAID,kind=Payment.Kind.RETRY if flow=='recovery' else Payment.Kind.BIND,paid_at=timezone.now(),raw_payload={key:{'return_token':t,'trial_granted':False}})\n out[f'{uid}:{flow}']=t\nprint(json.dumps(out))"""
        tokens=shell(app,env,fixture_code)
        tokens.update(shell(app, env, """import json, time
from decimal import Decimal
from unittest.mock import patch
from django.utils import timezone
from subscriptions.models import Payment
from subscriptions.billing import build_bind_return_token
out = {}
for label, status in (('pending', Payment.Status.PENDING), ('expired', Payment.Status.PAID)):
    issued_at = time.time() - (901 if label == 'expired' else 0)
    with patch('miniapp.auth.time.time', return_value=issued_at):
        token = build_bind_return_token(user_id=910001, flow='bind', lang='uk')
    Payment.objects.create(user_id=910001, provider='monobank', provider_payment_id='accept-' + label,
        amount=Decimal('1.00'), currency='UAH', status=status, kind=Payment.Kind.BIND,
        paid_at=timezone.now() if status == Payment.Status.PAID else None,
        raw_payload={'bind_context': {'return_token': token, 'trial_granted': False}})
    out[label] = token
print(json.dumps(out))"""))
        before = counts(); passed("paid bind/recovery and negative fixtures seeded in isolated database")
        stage="browser"
        with sync_playwright() as p:
            browser=p.chromium.launch(headless=True,args=[f"--host-resolver-rules=MAP {HOST} 127.0.0.1, MAP * ~NOTFOUND","--no-proxy-server"])
            try:
                def page():
                    c=browser.new_context(ignore_https_errors=True,service_workers="block"); c.set_default_timeout(15000)
                    c.route("**/*",lambda r: r.continue_() if urlsplit(r.request.url).scheme=="https" and urlsplit(r.request.url).netloc==f"{HOST}:{tls_port}" else r.abort())
                    return c,c.new_page()
                def profile(pg,uid):
                    data=pg.evaluate("fetch('/app/api/profile').then(async r=>({s:r.status,b:await r.json()}))")
                    if data["s"]!=200 or data["b"].get("user",{}).get("tg_user_id") != uid: fail("exact profile identity mismatch")
                def anonymous(pg):
                    check(pg.evaluate("fetch('/app/api/profile').then(r=>r.status)") == 401, "anonymous browser acquired identity")
                def submit(pg):
                    with pg.expect_response(lambda response: urlsplit(response.url).path == '/billing/mono/open-app') as response:
                        pg.locator("form[action$='/billing/mono/open-app']").get_by_role("button").click()
                    return response.value.status
                for key,uid,lang in (("910001:bind",910001,"uk"),("910001:recovery",910001,"en")):
                    stage = f"browser-{key.split(':')[1]}-return"
                    c,pg=page(); response=pg.goto(origin+"/billing/mono/return?return_token="+tokens[key],wait_until="domcontentloaded")
                    if response.status!=200: fail("paid return page did not render")
                    check(pg.get_by_role("button", name="Open app" if lang == "en" else "Відкрити застосунок", exact=True).count() == 1, "localized app action missing")
                    anonymous(pg)
                    stage = f"browser-{key.split(':')[1]}-submit"
                    pg.locator("form[action$='/billing/mono/open-app']").locator("button,input[type=submit]").first.click()
                    stage = f"browser-{key.split(':')[1]}-destination"
                    pg.wait_for_url(origin+"/app/")
                    profile(pg,uid)
                    passed(f"{lang} paid {key.split(':')[1]} opens exact authenticated /app/")
                    if lang == "uk":
                        pg.goto(origin+"/billing/mono/return?return_token="+tokens[key], wait_until="domcontentloaded")
                        check(submit(pg) == 302, "same-user continuation rejected")
                        pg.wait_for_url(origin+"/app/"); profile(pg, uid)
                        passed("same-user continuation preserves authenticated identity")
                        pg.goto(origin+"/billing/mono/return?return_token="+tokens["910002:bind"], wait_until="domcontentloaded")
                        check(submit(pg) == 403, "cross-account handoff was not rejected")
                        profile(pg, uid); passed("cross-account handoff rejected without switching identity")
                    c.close()
                c,pg=page(); pg.goto(origin+"/billing/mono/return?return_token="+tokens["910002:bind"],wait_until="domcontentloaded"); pg.locator("form[action$='/billing/mono/open-app']").locator("button,input[type=submit]").first.click(); pg.wait_for_url(origin+"/app/"); profile(pg,910002); passed("second user authenticated without cross-account switch")
                c.close()
                c,pg=page()
                pg.goto(origin+"/billing/mono/return?return_token="+tokens["910001:bind"], wait_until="domcontentloaded")
                check(submit(pg) == 403, "anonymous capability replay was not rejected")
                anonymous(pg); passed("anonymous replay rejected without authentication")
                pg.goto(origin+"/billing/mono/return?return_token="+tokens["910001:bind"], wait_until="domcontentloaded")
                for label in ("pending", "expired"):
                    status = pg.evaluate("""async token => {
                        const body = new FormData(document.querySelector('form'));
                        body.set('return_token', token);
                        const response = await fetch('/billing/mono/open-app', {method:'POST', body, redirect:'manual'});
                        return response.status;
                    }""", tokens[label])
                    check(status == 403, label + " capability was not rejected")
                    anonymous(pg); passed(label + " capability rejected without authentication")
                pg.goto(origin+"/billing/mono/return?return_token="+tokens["expired"], wait_until="domcontentloaded")
                check(pg.locator("form[action$='/billing/mono/open-app']").count() == 0, "expired capability was refreshed")
                c.close()
                c,pg=page(); tampered=tokens["910001:bind"][:-1]+("A" if tokens["910001:bind"][-1]!="A" else "B"); r=pg.goto(origin+"/billing/mono/return?return_token="+tampered); (r.status in (200,302,403,404)) or fail("tampered token unsafe status"); fail("tampered token must not expose app action") if pg.locator("form[action$='/billing/mono/open-app']").count() else None; anonymous(pg); passed("tampered return token has no authentication action"); c.close()
            finally: browser.close()
        check(before==counts(),"financial rows changed"); passed("financial row counts unchanged")
        if proxy.errors: fail("TLS proxy errors")
        result=0
    except Exception as exc:
        message = f"CHECKOUT_RETURN_ACCEPTANCE_FAIL stage={stage} type={type(exc).__name__}"
        if isinstance(exc, AssertionError): message += " check=" + str(exc)
        events.append(message); print(message)
    finally:
        if proxy is not None: proxy.shutdown(); proxy.server_close(); proxy_thread.join(5)
        if server is not None: server.terminate(); server.wait(10)
        for name in reversed(containers): subprocess.run(["docker","rm","-f",name],capture_output=True)
        if result==0: events.append("REAL_CHECKOUT_RETURN_ACCEPTANCE_PASS"); print(events[-1])
        if args.log: args.log.parent.mkdir(parents=True,exist_ok=True); args.log.write_text("\n".join(events)+"\n",encoding="utf-8")
    return result

if __name__ == "__main__": raise SystemExit(main())
