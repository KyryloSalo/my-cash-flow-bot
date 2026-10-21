#!/usr/bin/env python
"""Real local HTTPS/Postgres/Chromium acceptance for public Telegram login entry.

The only provider involved is an explicit synthetic Telegram OIDC double: browser
authorization, token exchange, and JWKS/claims are local test fixtures. This does
not prove native Telegram or Android OS handoff. Authentication, cookies, Django,
Postgres, and Chromium remain real; no magic-link secret or BrowserLoginTokenUse
is used.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import jwt
from playwright.sync_api import sync_playwright

from browser_login_acceptance import http, https_proxy, shell

HOST = "public.acceptance.test"
USERS = (900001, 900002)


def check(ok, label):
    if not ok:
        raise AssertionError(label)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(args, *, cwd=None, env=None):
    p = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    # Neither commands nor child stderr are safe: both can carry credentials.
    check(p.returncode == 0, "child process failed")
    return p.stdout


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class OidcProvider:
    """A real local authorization-code provider; no application code is mocked."""

    def __init__(self, port, issuer):
        from cryptography.hazmat.primitives.asymmetric import rsa
        self.port = port
        self.issuer = issuer
        self.user_id = USERS[0]
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public = self.key.public_key().public_numbers()
        self.jwk = {"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "acceptance-key",
                    "n": b64(public.n.to_bytes((public.n.bit_length() + 7) // 8, "big")),
                    "e": b64(public.e.to_bytes((public.e.bit_length() + 7) // 8, "big"))}
        self.codes = {}
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def send_json(self, status, payload):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parsed = urlsplit(self.path)
                if parsed.path == "/auth":
                    q = parse_qs(parsed.query)
                    required = ("client_id", "redirect_uri", "state", "nonce", "code_challenge")
                    check(all(q.get(k, [""])[0] for k in required), "provider authorization parameters missing")
                    code = secrets.token_urlsafe(24)
                    provider.codes[code] = {k: q[k][0] for k in required}
                    provider.codes[code]["code_challenge_method"] = q.get("code_challenge_method", [""])[0]
                    provider.codes[code]["user_id"] = str(provider.user_id)
                    location = provider.codes[code]["redirect_uri"] + "?" + urlencode({"code": code, "state": provider.codes[code]["state"]})
                    body = ("<!doctype html><title>Telegram confirmation</title>"
                            "<p>Local synthetic Telegram confirmation</p>"
                            f'<a id="confirm" href="{location}">Continue</a>').encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if parsed.path == "/.well-known/jwks.json":
                    self.send_json(200, {"keys": [provider.jwk]})
                    return
                self.send_error(404)

            def do_POST(self):
                if urlsplit(self.path).path != "/token":
                    self.send_error(404); return
                length = int(self.headers.get("Content-Length", "0"))
                form = parse_qs(self.rfile.read(length).decode())
                code = form.get("code", [""])[0]
                record = provider.codes.pop(code, None)
                verifier = form.get("code_verifier", [""])[0]
                check(record is not None, "provider rejected unknown authorization code")
                check(record["code_challenge_method"] == "S256", "provider requires S256 PKCE")
                check(form.get("grant_type") == ["authorization_code"], "provider requires code grant")
                check(form.get("client_id") == [record["client_id"]], "provider client mismatch")
                check(form.get("redirect_uri") == [record["redirect_uri"]], "provider redirect mismatch")
                check(b64(hashlib.sha256(verifier.encode()).digest()) == record["code_challenge"], "PKCE verifier was not checked")
                now = int(time.time())
                claims = {"iss": provider.issuer, "aud": record["client_id"], "sub": "telegram:" + record["user_id"],
                          "id": int(record["user_id"]), "given_name": "Acceptance", "iat": now,
                          "exp": now + 300, "nonce": record["nonce"]}
                token = jwt.encode(claims, provider.key, algorithm="RS256", headers={"kid": provider.jwk["kid"]})
                self.send_json(200, {"access_token": "synthetic-access-token", "token_type": "Bearer", "id_token": token})

        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=5)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--log", type=Path)
    ap.add_argument("--django-tests", action="store_true", help="Run isolated Django regression suites against the disposable Postgres fixture")
    args = ap.parse_args()
    root = Path(__file__).resolve().parents[1]
    app = root / "admin_service"
    events = []
    containers = []
    server = None
    provider = None
    proxy = None
    stage = "setup"
    result = 1
    if args.preflight:
        check((app / "manage.py").is_file(), "Django entrypoint missing")
        run(["docker", "info", "--format", "{{.ServerVersion}}"])
        with sync_playwright() as p:
            p.chromium.launch(headless=True).close()
        print("TELEGRAM_LOGIN_ACCEPTANCE_PREFLIGHT_PASS")
        return 0

    run_id = secrets.token_hex(5)
    password = secrets.token_urlsafe(18)
    db_port = free_port()
    http_port = free_port()
    tls_port = free_port()
    provider_port = free_port()
    origin = f"https://{HOST}:{tls_port}"
    provider_issuer = f"http://127.0.0.1:{provider_port}"
    env = os.environ.copy()
    env.update({
        "DJANGO_SETTINGS_MODULE": "config.settings", "DJANGO_DEBUG": "true",
        "DJANGO_SECRET_KEY": secrets.token_urlsafe(32),
        "DJANGO_ALLOWED_HOSTS": f"127.0.0.1,localhost,{HOST}",
        "DOMAIN": f"{HOST}:{tls_port}", "SECURE_SSL_REDIRECT": "true",
        "SESSION_COOKIE_SECURE": "true", "CSRF_COOKIE_SECURE": "true",
        "MINIAPP_DEV_AUTH_ENABLED": "false",
        "PRODUCTION_CANARY_ENABLED": "false", "REDIS_URL": "",
        # Deliberately clear the legacy browser-login signing secret.
        "MINIAPP_BROWSER_LOGIN_SECRET": "",
        "BOT_TOKEN": "0:local-acceptance-only", "MINIAPP_URL": origin + "/app/",
        "TELEGRAM_OIDC_CLIENT_ID": "synthetic-client",
        "TELEGRAM_OIDC_CLIENT_SECRET": "synthetic-provider-secret",
        "TELEGRAM_OIDC_REDIRECT_URI": origin + "/app/auth/telegram/callback",
        "TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT": provider_issuer + "/auth",
        "TELEGRAM_OIDC_TOKEN_ENDPOINT": provider_issuer + "/token",
        "TELEGRAM_OIDC_JWKS_URI": provider_issuer + "/.well-known/jwks.json",
        "TELEGRAM_OIDC_ISSUER": provider_issuer,
        "DATABASE_URL": f"postgresql://acceptance:{password}@127.0.0.1:{db_port}/acceptance",
    })

    def passed(label):
        events.append("PASS " + label)
        print(events[-1])

    def docker(*args):
        name = f"mcf-telegram-login-{run_id}"
        containers.append(name)
        run(["docker", "run", "-d", "--rm", "--name", name,
             "-e", "POSTGRES_USER=acceptance", "-e", f"POSTGRES_PASSWORD={password}",
             "-e", "POSTGRES_DB=acceptance", "-p", f"127.0.0.1:{db_port}:5432", "postgres:16"])
        return name

    try:
        stage = "postgres"
        pg = docker()
        stage = "oidc-provider"
        provider = OidcProvider(provider_port, provider_issuer)
        provider.start()
        passed("isolated OIDC authorization, PKCE token, and RS256 JWKS provider started")
        for _ in range(100):
            if subprocess.run(["docker", "exec", pg, "pg_isready", "-U", "acceptance", "-d", "acceptance"], capture_output=True).returncode == 0:
                break
            time.sleep(.2)
        else:
            raise RuntimeError("postgres readiness timeout")
        run([sys.executable, "manage.py", "migrate", "--noinput", "--skip-checks"], cwd=app, env=env)
        seed = (
            "from users.models import TelegramUser; from accounts.models import Account; "
            "[TelegramUser.objects.get_or_create(tg_user_id=i, defaults={'first_name':'Acceptance','base_currency':'UAH'}) for i in (900001,900002)]; "
            "[Account.objects.create(tg_user_id=i, label='Synthetic acceptance', currency='UAH', starting_balance=100, balance=100) for i in (900001,900002)]"
        )
        run([sys.executable, "manage.py", "shell", "-c", seed], cwd=app, env=env)
        passed("isolated PostgreSQL migrated and synthetic users seeded")

        if args.django_tests:
            stage = "django-tests"
            test_names = ["miniapp.tests_telegram_browser_entry", "miniapp.tests_oidc"]
            test_env = dict(env)
            test_env["DOMAIN"] = HOST
            test_env["TELEGRAM_OIDC_REDIRECT_URI"] = f"https://{HOST}/app/auth/telegram/callback"
            test_result = subprocess.run([sys.executable, "manage.py", "test", *test_names, "--noinput"], cwd=app, env=test_env, capture_output=True, text=True)
            # Failure tracebacks may expose token-bearing URLs. Keep only the
            # standard runner summary, never raw stdout/stderr.
            summaries = [line.strip() for line in test_result.stderr.splitlines()
                         if line.startswith("Ran ") and " tests in " in line]
            check(test_result.returncode == 0 and bool(summaries), "Django regression suite failed")
            passed("Django browser-entry and OIDC regression suites: " + summaries[-1])

        financial_snapshot = (
            "import hashlib,json\nfrom django.apps import apps\n"
            "from django.core.serializers.json import DjangoJSONEncoder\n"
            "keys=('account','transaction','subscription','payment','ledger','invoice')\n"
            "result={}\n"
            "for model in apps.get_models():\n"
            " if model._meta.app_label in ('accounts','transactions','subscriptions') or any(key in model._meta.model_name.lower() for key in keys):\n"
            "  rows=list(model._base_manager.order_by(model._meta.pk.name).values())\n"
            "  result[model._meta.label]={'count':len(rows),'sha256':hashlib.sha256(json.dumps(rows,cls=DjangoJSONEncoder,sort_keys=True).encode()).hexdigest()}\n"
            "print(json.dumps(result))"
        )
        before = shell(app, env, financial_snapshot)
        check(before["accounts.Account"]["count"] == len(USERS), "financial fixture accounts missing")
        token_counts = (
            "import json; from users.models import UserOidcTokenUse; "
            "from miniapp.models import BrowserLoginTokenUse; "
            "print(json.dumps([UserOidcTokenUse.objects.count(),BrowserLoginTokenUse.objects.count()]))"
        )
        check(shell(app, env, token_counts) == [0, 0], "authentication fixture must start unused")

        stage = "django"
        bootstrap = (
            "import os,sys,django; os.environ['DJANGO_SETTINGS_MODULE']='config.settings'; "
            "django.setup(); from django.conf import settings; "
            f"settings.TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT={provider_issuer + '/auth'!r}; "
            f"settings.TELEGRAM_OIDC_TOKEN_ENDPOINT={provider_issuer + '/token'!r}; "
            f"settings.TELEGRAM_OIDC_JWKS_URI={provider_issuer + '/.well-known/jwks.json'!r}; "
            f"settings.TELEGRAM_OIDC_ISSUER={provider_issuer!r}; "
            "from django.core.management import execute_from_command_line; "
            "sys.argv=['manage.py','runserver',sys.argv[1],'--noreload','--skip-checks']; execute_from_command_line(sys.argv)"
        )
        server = subprocess.Popen([sys.executable, "-c", bootstrap, f"127.0.0.1:{http_port}"], cwd=app, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            check(server.poll() is None, "Django server exited before readiness")
            try:
                if http(f"http://127.0.0.1:{http_port}/app/login/")[0] in (200, 301, 302):
                    break
            except OSError:
                pass
            time.sleep(.2)
        else:
            raise RuntimeError("Django readiness timeout")
        proxy, proxy_thread = https_proxy(f"http://127.0.0.1:{http_port}", tls_port, os.environ["TMPDIR"])
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=[
                f"--host-resolver-rules=MAP {HOST} 127.0.0.1, EXCLUDE 127.0.0.1, MAP * ~NOTFOUND",
                "--no-proxy-server",
            ])
            allowed = {f"{HOST}:{tls_port}", f"127.0.0.1:{provider_port}"}
            def new_page():
                context = browser.new_context(ignore_https_errors=True, service_workers="block")
                context.route("**/*", lambda route: route.continue_() if urlsplit(route.request.url).netloc in allowed else route.abort())
                return context, context.new_page()

            def profile(page, expected):
                result = page.evaluate("fetch('/app/api/profile').then(async r => ({status:r.status,body:await r.json()}))")
                if expected is None:
                    check(result["status"] == 401, "anonymous profile must remain unauthorized")
                else:
                    check(result["status"] == 200 and result["body"]["user"]["tg_user_id"] == expected, "OIDC callback identity mismatch")

            context, page = new_page()
            response = page.goto(origin + "/app/login/?install=1", wait_until="domcontentloaded")
            check(response.status == 200, "public login entry did not render")
            href = page.locator("a.browser-login-action").get_attribute("href")
            check(href and "/app/auth/telegram/start" in href, "Continue must target Telegram OIDC start")
            check("install" in href, "install target was not preserved")
            passed("anonymous public entry renders and delegates through browser OIDC start")
            profile(page, None)
            passed("anonymous profile remains 401")
            page.locator("a.browser-login-action").click()
            callback = page.locator("#confirm").get_attribute("href")
            check(callback and callback.startswith(origin + "/app/auth/telegram/callback?"), "provider callback origin mismatch")
            page.locator("#confirm").click()
            page.wait_for_url(origin + "/app/?install=1")
            profile(page, USERS[0])
            cookies = {c["name"]: c for c in context.cookies()}
            check(cookies.get("sessionid", {}).get("secure") is True, "session cookie must be Secure")
            check(cookies["sessionid"]["httpOnly"] is True, "session cookie must be HttpOnly")
            passed("Chromium followed synthetic Telegram confirmation through real HTTPS callback and authenticated user 900001")

            stage = "callback-replay"
            page.goto(callback, wait_until="domcontentloaded")
            check(page.url == origin + "/app/", "authenticated replay must return to existing PWA session")
            profile(page, USERS[0])
            _, anonymous = new_page()
            response = anonymous.goto(callback, wait_until="domcontentloaded")
            check(response.status == 400, "anonymous callback replay must fail")
            profile(anonymous, None)
            passed("callback replay cannot authenticate an anonymous browser or duplicate authenticated identity")

            stage = "invalid-state"
            _, invalid = new_page()
            invalid.goto(origin + "/app/login/", wait_until="domcontentloaded")
            invalid.locator("a.browser-login-action").click()
            pending = urlsplit(invalid.locator("#confirm").get_attribute("href"))
            query = parse_qs(pending.query)
            query["state"] = ["synthetic-invalid-state"]
            response = invalid.goto(origin + pending.path + "?" + urlencode(query, doseq=True), wait_until="domcontentloaded")
            check(response.status == 400, "invalid callback state must fail")
            check(invalid.locator("body").get_attribute("data-error-code") == "telegram_oidc_state_invalid", "invalid state was not rejected by application")
            profile(invalid, None)
            passed("invalid state is rejected without authenticating the browser")

            stage = "cross-user-replay"
            provider.user_id = USERS[1]
            _, other = new_page()
            other.goto(origin + "/app/login/?tg_user_id=900001&next=https://invalid.test/", wait_until="domcontentloaded")
            other.locator("a.browser-login-action").click()
            other.locator("#confirm").click()
            other.wait_for_url(origin + "/app/")
            profile(other, USERS[1])
            other.goto(callback, wait_until="domcontentloaded")
            check(other.url == origin + "/app/", "cross-user replay must retain existing PWA session")
            profile(other, USERS[1])
            check(shell(app, env, token_counts) == [2, 0], "replay must not consume another OIDC or magic-link token")
            passed("second Telegram-confirmed identity ignores URL identity spoofing and retains its own session on replay")
            browser.close()

        stage = "financial-invariance"
        check(shell(app, env, financial_snapshot) == before, "login changed financial rows or values")
        check(not proxy.errors, "HTTPS proxy failed")
        passed("financial table row counts and content hashes unchanged; zero magic-link tokens consumed")
        result = 0
    except Exception as exc:
        msg = f"TELEGRAM_LOGIN_ACCEPTANCE_FAIL stage={stage} type={type(exc).__name__}"
        if isinstance(exc, AssertionError):
            msg += " check=" + str(exc)
        events.append(msg)
        print(msg)
    finally:
        if proxy is not None:
            proxy.shutdown(); proxy.server_close(); proxy_thread.join(timeout=5)
            if proxy_thread.is_alive():
                result = 1
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill(); server.wait(timeout=10)
        if provider is not None:
            provider.close()
            if provider.thread.is_alive():
                result = 1
        for name in reversed(containers):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            if subprocess.run(["docker", "container", "inspect", name], capture_output=True).returncode == 0:
                result = 1
        if result != 0 and not any(event.startswith("TELEGRAM_LOGIN_ACCEPTANCE_FAIL") for event in events):
            events.append("TELEGRAM_LOGIN_ACCEPTANCE_FAIL stage=cleanup")
            print(events[-1])
        if result == 0:
            passed("disposable server, provider, TLS proxy and PostgreSQL container cleaned up")
            events.append("LIMIT synthetic Telegram provider double; native Telegram/OS Chrome not covered")
            events.append("SYNTHETIC_PROVIDER_TELEGRAM_LOGIN_ACCEPTANCE_PASS")
            print(events[-2]); print(events[-1])
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            args.log.write_text("\n".join(events) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
