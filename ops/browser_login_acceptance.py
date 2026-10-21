#!/usr/bin/env python
"""Exercise real login issuance and Chromium against disposable local services.

Only a reserved .test HTTPS origin is proxied to loopback. No production calls,
credentials files, browser storage exports, traces or token-bearing logs are used.
Native Telegram/Android app launching remains a separate device acceptance gate.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from playwright.sync_api import sync_playwright

USERS = (900001, 900002)
HOST = "public.acceptance.test"
ANDROID_TELEGRAM = "Mozilla/5.0 (Linux; Android 14) Telegram/11.0 Chrome/130.0.0.0 Mobile Safari/537.36"


def check(condition, label):
    if not condition:
        raise AssertionError(label)


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(args, *, cwd=None, env=None):
    # Commands and child stderr can contain local credentials or token paths.
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    check(result.returncode == 0, "child process failed")
    return result.stdout


def shell(app, env, code):
    output = run([sys.executable, "manage.py", "shell", "-c", code], cwd=app, env=env)
    return json.loads(output.strip().splitlines()[-1])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http(url, *, headers=None, payload=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers=headers or {},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=10)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, response.headers, response.read()


def https_proxy(base, listen, scratch):
    """Use real loopback TLS; Chromium must follow redirects without rewriting."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, HOST)])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - timedelta(minutes=1))
                   .not_valid_after(now + timedelta(hours=1))
                   .add_extension(x509.SubjectAlternativeName([x509.DNSName(HOST)]), critical=False)
                   .sign(key, hashes.SHA256()))
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    # Generated synthetic test credentials, never existing user keys. Delete
    # temporary files immediately after loading the in-memory TLS context.
    with tempfile.TemporaryDirectory(prefix="mcf-browser-tls-", dir=scratch) as directory:
        certificate_path = Path(directory) / "certificate.crt"
        key_path = Path(directory) / "synthetic.private"
        certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                                             serialization.PrivateFormat.PKCS8,
                                             serialization.NoEncryption()))
        tls.load_cert_chain(certificate_path, key_path)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # Request paths can carry one-time tokens.

        def do_GET(self):
            check(self.headers.get("Host") == f"{HOST}:{listen}", "unexpected TLS host")
            headers = dict(self.headers.items())
            headers["X-Forwarded-Proto"] = "https"
            headers["Accept-Encoding"] = "identity"
            request = urllib.request.Request(base + self.path, headers=headers)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
            try:
                response = opener.open(request, timeout=20)
            except urllib.error.HTTPError as exc:
                response = exc
            except OSError:
                raise RuntimeError("loopback upstream request failed") from None
            with response:
                body = response.read()
                self.send_response(response.status)
                for header, value in response.headers.items():
                    if header.lower() not in {"connection", "transfer-encoding", "content-length"}:
                        self.send_header(header, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

    class QuietServer(ThreadingHTTPServer):
        daemon_threads = True

        def handle_error(self, _request, _client_address):
            if isinstance(sys.exception(), (ConnectionAbortedError, ConnectionResetError, BrokenPipeError)):
                return  # Chromium cancels resource sockets when the next navigation starts.
            self.errors.append(type(sys.exception()).__name__)

    proxy = QuietServer(("127.0.0.1", listen), Handler)
    proxy.errors = []
    proxy.socket = tls.wrap_socket(proxy.socket, server_side=True)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    return proxy, thread


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--log", type=Path, help="Write sanitized checks, never URLs or tokens")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    app = root / "admin_service"
    tls_port = port()
    origin = f"https://{HOST}:{tls_port}"
    authority = urlsplit(origin).netloc
    if args.preflight:
        check((app / "manage.py").is_file(), "Django entrypoint missing")
        run(["docker", "info", "--format", "{{.ServerVersion}}"])
        with sync_playwright() as playwright:
            playwright.chromium.launch(headless=True).close()
        print("BROWSER_LOGIN_ACCEPTANCE_PREFLIGHT_PASS")
        return 0

    events = []
    stage = "setup"
    server = None
    proxy_server = None
    proxy_thread = None
    containers = []
    result = 1
    run_id = secrets.token_hex(6)
    internal_token = secrets.token_urlsafe(24)
    db_password = secrets.token_urlsafe(24)
    env = os.environ.copy()
    env.update(
        DJANGO_SETTINGS_MODULE="config.settings",
        DJANGO_DEBUG="true",
        DJANGO_SECRET_KEY=secrets.token_urlsafe(32),
        DJANGO_ALLOWED_HOSTS="127.0.0.1,localhost,admin,public.acceptance.test",
        DOMAIN=authority,
        SECURE_SSL_REDIRECT="true",
        SESSION_COOKIE_SECURE="true",
        CSRF_COOKIE_SECURE="true",
        MINIAPP_DEV_AUTH_ENABLED="false",
        MINIAPP_BROWSER_LOGIN_SECRET=secrets.token_urlsafe(32),
        MINIAPP_BROWSER_LOGIN_TTL_SECONDS="900",
        BILLING_INTERNAL_TOKEN=internal_token,
        PUSH_INTERNAL_TOKEN=internal_token,
        BOT_TOKEN="0:local-acceptance-only",
        MINIAPP_URL=origin + "/app/",
    )

    def passed(label):
        events.append("PASS " + label)
        print(events[-1])

    def container(image, service_port, options):
        name = f"mcf-browser-login-{service_port}-{run_id}"
        # Register before starting so even ambiguous Docker errors get cleanup.
        containers.append(name)
        run(["docker", "run", "-d", "--rm", "--name", name, *options,
             "-p", f"127.0.0.1::{service_port}", image])
        mapped = run(["docker", "port", name, f"{service_port}/tcp"]).strip().rsplit(":", 1)[-1]
        return name, mapped

    try:
        pg, pg_port = container("postgres:16", 5432, [
            "-e", "POSTGRES_USER=acceptance", "-e", f"POSTGRES_PASSWORD={db_password}",
            "-e", "POSTGRES_DB=acceptance",
        ])
        redis, redis_port = container("redis:7-alpine", 6379, [])
        for name, command in ((pg, ["pg_isready", "-U", "acceptance", "-d", "acceptance"]),
                              (redis, ["redis-cli", "ping"])):
            for _ in range(100):
                probe = subprocess.run(["docker", "exec", name, *command], capture_output=True)
                if probe.returncode == 0:
                    break
                time.sleep(0.2)
            else:
                raise RuntimeError("isolated service readiness timeout")
        env["DATABASE_URL"] = f"postgresql://acceptance:{db_password}@127.0.0.1:{pg_port}/acceptance"
        env["REDIS_URL"] = f"redis://127.0.0.1:{redis_port}/0"
        run([sys.executable, "manage.py", "migrate", "--noinput"], cwd=app, env=env)
        shell(app, env, "import json\nfrom users.models import TelegramUser\n"
              "for uid in (900001, 900002): TelegramUser.objects.create(tg_user_id=uid, first_name='Acceptance', base_currency='UAH')\n"
              "print(json.dumps(True))")
        financial_counts = (
            "import json\nfrom django.apps import apps\n"
            "keys=('account','transaction','subscription','payment','ledger','invoice')\n"
            "print(json.dumps({m._meta.label:m.objects.count() for m in apps.get_models() "
            "if any(key in m._meta.model_name.lower() for key in keys)}))"
        )
        before = shell(app, env, financial_counts)
        listen = port()
        base = f"http://127.0.0.1:{listen}"
        env["BILLING_INTERNAL_BASE_URL"] = base
        server = subprocess.Popen(
            [sys.executable, "manage.py", "runserver", f"127.0.0.1:{listen}", "--noreload"],
            cwd=app, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(200):
            check(server.poll() is None, "Django exited before readiness")
            try:
                status, _, _ = http(base + "/app/", headers={"X-Forwarded-Proto": "https"})
                if status == 200:
                    break
            except OSError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("Django readiness timeout")
        proxy_server, proxy_thread = https_proxy(base, tls_port, os.environ["TMPDIR"])

        stage = "security-and-issuance"
        status, _, _ = http(base + "/app/")
        check(status in (301, 302), "normal HTTP request must redirect to HTTPS")
        issue_headers = {"Host": "admin:8080", "Content-Type": "application/json"}
        for token in (None, "incorrect-acceptance-token"):
            headers = dict(issue_headers)
            if token:
                headers["X-Internal-Token"] = token
            status, _, _ = http(base + "/internal/auth/browser-login-link", headers=headers,
                                payload={"tg_user_id": USERS[0]})
            check(status == 403, "unauthorized internal issuance must be forbidden without redirect")

        def issue(uid):
            status, _, body = http(base + "/internal/auth/browser-login-link",
                                  headers={**issue_headers, "X-Internal-Token": internal_token},
                                  payload={"tg_user_id": uid, "locale": "uk"})
            check(status == 200, "internal HTTP issuance failed")
            url = json.loads(body)["url"]
            parts = urlsplit(url)
            check(parts.scheme == "https" and parts.netloc == authority
                  and parts.path.startswith("/app/browser-login/"), "issued URL must use public HTTPS origin")
            return url

        issue(USERS[0])
        passed("HTTPS redirect remains enabled; internal HTTP issuance is protected and public HTTPS")
        bot_code = (
            "import asyncio,json,sys\n"
            f"sys.path.insert(0,{str(root / 'bot')!r})\n"
            "import bot_main\n"
            "url=asyncio.run(bot_main._server_issued_browser_login_url(900001))\n"
            "markup=bot_main._miniapp_launch_markup(browser_login_url=url,include_home_button=True)\n"
            "button=bot_main._inline_keyboard_rows(markup)[0][0]\n"
            "assert url and button.url==url and button.web_app is None\n"
            "print(json.dumps({'url':button.url}))"
        )
        bot_result = json.loads(run([sys.executable, "-c", bot_code], cwd=root, env=env).strip().splitlines()[-1])
        url = bot_result["url"]
        check(url.startswith(origin + "/app/browser-login/"), "bot must preserve canonical server-issued URL")
        passed("real bot issuance client and normal inline URL button")

        def consumption_count():
            return shell(app, env, "import json\nfrom miniapp.models import BrowserLoginTokenUse\n"
                         "print(json.dumps(BrowserLoginTokenUse.objects.count()))")

        public_headers = {"Host": authority, "X-Forwarded-Proto": "https"}
        consumed_before = consumption_count()
        parts = urlsplit(url)
        status, _, _ = http(base + parts.path, headers=public_headers)
        check(status == 200, "ordinary handoff page must render")
        status, headers, _ = http(base + parts.path + "?install=1",
                                  headers={**public_headers, "User-Agent": ANDROID_TELEGRAM})
        check(status == 302, "Android Telegram handoff must redirect to Chrome intent")
        intent = headers["Location"]
        intent_path, separator, directives = intent.partition("#Intent;")
        check(separator and intent_path.startswith(f"intent://{authority}/"), "invalid Chrome intent")
        check("package=com.android.chrome" in directives and "scheme=https" in directives,
              "Chrome intent must select HTTPS Chrome")
        fallback = next(item.split("=", 1)[1] for item in directives.split(";")
                        if item.startswith("S.browser_fallback_url="))
        target = unquote(fallback)
        check(target == "https://" + intent_path.removeprefix("intent://"), "intent fallback token roundtrip mismatch")
        token = parts.path.removeprefix("/app/browser-login/").rstrip("/")
        check(target == origin + "/app/api/browser-login/" + token + "/?install=1", "intent must preserve exact token and install flag")
        check(consumption_count() == consumed_before, "handoff must not consume login token")
        passed("handoff is non-consuming; Android Chrome intent preserves token and install flag")

        stage = "chromium-login-and-replay"
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, args=[
                f"--host-resolver-rules=MAP {HOST} 127.0.0.1, MAP * ~NOTFOUND",
                "--no-proxy-server",
            ])
            try:
                def new_page():
                    context = browser.new_context(service_workers="block", ignore_https_errors=True)
                    context.set_default_timeout(15000)

                    def proxy(route):
                        request = route.request
                        parsed = urlsplit(request.url)
                        if parsed.scheme != "https" or parsed.netloc != authority:
                            route.abort()
                            return
                        route.continue_()

                    context.route("**/*", proxy)
                    return context, context.new_page()

                def profile(page, uid):
                    data = page.evaluate("fetch('/app/api/profile').then(async r => ({status:r.status,body:await r.json()}))")
                    check(data["status"] == 200 and data["body"].get("user", {}).get("tg_user_id") == uid,
                          "authenticated profile identity mismatch")

                def enter(page, entry, uid):
                    response = page.goto(entry, wait_until="domcontentloaded")
                    check(response.status == 200, "login handoff failed")
                    page.locator(".browser-login-action").click()
                    page.wait_for_url(origin + "/app/")
                    profile(page, uid)

                context, page = new_page()
                enter(page, url, USERS[0])
                cookies = {cookie["name"]: cookie for cookie in context.cookies()}
                check(cookies.get("sessionid", {}).get("secure") is True, "session cookie must remain Secure")
                check(cookies["sessionid"]["httpOnly"] is True, "session cookie must remain HttpOnly")
                check(consumption_count() == consumed_before + 1, "login must consume exactly once")
                passed("Chromium handoff click enters PWA; real profile is correct user with Secure HttpOnly session")

                login = origin + "/app/api/browser-login/" + token + "/"
                page.goto(login, wait_until="domcontentloaded")
                check(page.url == origin + "/app/", "same-user replay must enter PWA")
                profile(page, USERS[0])
                check(consumption_count() == consumed_before + 1, "same-user replay must not consume twice")
                passed("authenticated same-user replay allowed without second consumption")

                _, anonymous = new_page()
                response = anonymous.goto(login, wait_until="domcontentloaded")
                check(response.status == 400 and "використане" in anonymous.locator("h1").inner_text(),
                      "anonymous consumed token must be rejected")
                check(anonymous.locator('a[href$="?start=app_login"]').count() == 1, "consumed error needs new-link action")
                passed("anonymous consumed replay rejected with actionable new-link button")

                _, other = new_page()
                enter(other, issue(USERS[1]), USERS[1])
                response = other.goto(login, wait_until="domcontentloaded")
                check(response.status == 400, "cross-user consumed replay must be rejected")
                profile(other, USERS[1])
                passed("cross-user replay rejected and original authenticated identity preserved")

                response = anonymous.goto(origin + "/app/api/browser-login/not-a-token/", wait_until="domcontentloaded")
                check(response.status == 400 and "не працює" in anonymous.locator("h1").inner_text(), "invalid link needs error UX")
                check(anonymous.locator('a[href$="?start=app_login"]').count() == 1, "invalid link needs new-link action")
                expired = shell(app, env, "import json,time\nfrom miniapp.auth import build_browser_login_token\n"
                                "print(json.dumps(build_browser_login_token(900001,now=int(time.time())-1000)))")
                response = anonymous.goto(origin + "/app/api/browser-login/" + expired + "/", wait_until="domcontentloaded")
                check(response.status == 400 and "протермінувалося" in anonymous.locator("h1").inner_text(), "expired link needs error UX")
                check(anonymous.locator('a[href$="?start=app_login"]').count() == 1, "expired link needs new-link action")
                passed("invalid and expired links rejected with actionable new-link UX")

                _, chrome = new_page()
                response = chrome.goto(target, wait_until="domcontentloaded")
                check(response.status == 400, "consumed Android target must reject anonymous replay")
                fresh = urlsplit(issue(USERS[0]))
                status, headers, _ = http(base + fresh.path + "?install=1",
                                          headers={**public_headers, "User-Agent": ANDROID_TELEGRAM})
                check(status == 302, "fresh Android Chrome intent missing")
                fresh_intent = headers["Location"].partition("#Intent;")[0]
                chrome.goto("https://" + fresh_intent.removeprefix("intent://"), wait_until="domcontentloaded")
                check(chrome.url == origin + "/app/?install=1", "Chrome intent target must preserve install mode in PWA")
                profile(chrome, USERS[0])
                passed("decoded Android Chrome intent target logs correct user into install flow")
            finally:
                browser.close()
        stage = "financial-readback"
        check(not proxy_server.errors, "TLS proxy failures: " + ",".join(proxy_server.errors))
        check(before == shell(app, env, financial_counts), "financial rows changed")
        passed("financial row counts unchanged in disposable database")
        events.append("LIMIT native Telegram-to-OS-Chrome launch and production login are not covered")
        print(events[-1])
        result = 0
    except Exception as exc:
        # Playwright/HTTP exceptions may include a credential-bearing URL.
        message = f"BROWSER_LOGIN_ACCEPTANCE_FAIL stage={stage} type={type(exc).__name__}"
        if isinstance(exc, AssertionError):
            message += " check=" + str(exc)
        events.append(message)
        print(message)
    finally:
        if proxy_server is not None:
            proxy_server.shutdown()
            proxy_server.server_close()
            proxy_thread.join(timeout=5)
            if proxy_thread.is_alive():
                result = 1
                events.append("BROWSER_LOGIN_CLEANUP_FAIL TLS thread")
                print(events[-1])
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=10)
        for name in reversed(containers):
            cleanup = subprocess.run(["docker", "rm", "-f", name], capture_output=True)
            if cleanup.returncode != 0:
                result = 1
                events.append("BROWSER_LOGIN_CLEANUP_FAIL container")
                print(events[-1])
        if result == 0:
            events.append("REAL_BROWSER_LOGIN_ACCEPTANCE_PASS")
            print(events[-1])
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            args.log.write_text("\n".join(events) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
