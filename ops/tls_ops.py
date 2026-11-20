#!/usr/bin/env python3
"""Host-owned ACME operations; no credential or certificate files are read.

Public certificate metadata comes from TLS handshakes. Certbot uses its existing
configured access in place. All changing operations share the deployment lock.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import time
import uuid
from urllib.request import HTTPRedirectHandler, build_opener

ROOT = Path('/opt/my-cash-flow-bot')
ACME_ROOT = ROOT / 'shared/acme-webroot'
CONTROL_ROOT = ROOT / 'shared/tls-ops'
DOMAINS = ('vydno.capital', 'www.vydno.capital', 'admin.vydno.capital', 'cashflowbot.mygpttest.space')
NGINX = 'my-cash-flow-bot-nginx-1'


def run(argv: list[str]) -> str:
    result = subprocess.run(argv, capture_output=True, text=True, timeout=180)
    if result.returncode:
        # No raw SDK/Certbot diagnostics: credentials may appear in exceptions.
        raise RuntimeError(f'Operation failed: {Path(argv[0]).name} (exit {result.returncode})')
    return result.stdout


@contextmanager
def deployment_lock():
    import fcntl
    with (ROOT / '.deploy.lock').open('a') as handle:
        deadline = time.monotonic() + 300
        while True:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Timed out waiting for deployment lock')
                time.sleep(1)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def acme_check(*, root: Path = ACME_ROOT, domains=DOMAINS, opener=None) -> dict:
    directory = root / '.well-known/acme-challenge'
    directory.mkdir(parents=True, exist_ok=True, mode=0o755)
    directory.parent.chmod(0o755)
    directory.chmod(0o755)
    token = 'vydno-' + uuid.uuid4().hex
    path = directory / token
    proof = (uuid.uuid4().hex + '\n').encode('ascii')
    path.write_bytes(proof)
    path.chmod(0o644)
    opener = opener or build_opener(NoRedirect())
    checked = []
    try:
        for domain in domains:
            with opener.open(f'http://{domain}/.well-known/acme-challenge/{token}', timeout=15) as response:
                if response.status != 200 or response.read(len(proof) + 1) != proof:
                    raise RuntimeError(f'ACME exact-body proof failed for {domain}')
                checked.append(domain)
    finally:
        path.unlink(missing_ok=True)
    return {'acme': 'passed', 'domains': checked}


def certificate(host: str, *, address: str | None = None, verified=True) -> dict:
    context = ssl.create_default_context() if verified else ssl._create_unverified_context()
    with socket.create_connection((address or host, 443), timeout=12) as raw:
        with context.wrap_socket(raw, server_hostname=host) as connection:
            der = connection.getpeercert(binary_form=True)
            info = connection.getpeercert()
    return {'sha256': hashlib.sha256(der).hexdigest(), 'expires': info.get('notAfter')}


def baseline() -> dict:
    result = {}
    for domain in DOMAINS:
        try:
            result[domain] = certificate(domain, address='127.0.0.1', verified=False)['sha256']
        except (OSError, ssl.SSLError):
            result[domain] = None
    return result


def post_renew(*, before=None, allow_unchanged=False, renewed_domains=()) -> dict:
    before = before or {}
    run(['docker', 'exec', NGINX, 'nginx', '-t'])
    run(['docker', 'exec', NGINX, 'nginx', '-s', 'reload'])
    result = []
    for domain in DOMAINS:
        last_error = None
        for attempt in range(12):
            try:
                local = certificate(domain, address='127.0.0.1')
                external = certificate(domain)
                if local['sha256'] != external['sha256']:
                    raise RuntimeError('Public and local certificate differ')
                if not allow_unchanged and domain in renewed_domains and before.get(domain) == local['sha256']:
                    raise RuntimeError('Renewed certificate has not replaced the previous certificate')
                result.append({'domain': domain, **external})
                break
            except (OSError, ssl.SSLError, RuntimeError) as exc:
                last_error = type(exc).__name__
                if attempt == 11:
                    raise RuntimeError(f'TLS readback failed for {domain}: {last_error}') from None
                time.sleep(2)
    return {'reload': 'passed', 'public_certificates': result}


def renew(*, dry_run=False) -> dict:
    with deployment_lock():
        CONTROL_ROOT.mkdir(parents=True, exist_ok=True, mode=0o755)
        before_path = CONTROL_ROOT / 'before.json'
        before_path.write_text(json.dumps(baseline()), encoding='utf-8')
        env = os.environ.copy()
        env['VYDNO_TLS_LOCK_HELD'] = '1'
        env['VYDNO_TLS_DRY_RUN'] = '1' if dry_run else '0'
        argv = ['/usr/bin/certbot', 'renew', '--non-interactive', '--no-random-sleep-on-renew',
                '--webroot', '--webroot-path', str(ACME_ROOT),
                '--deploy-hook', f'/usr/bin/python3 {CONTROL_ROOT}/tls_ops.py post-renew']
        if dry_run:
            argv += ['--dry-run', '--run-deploy-hooks']
        # Certbot writes detailed diagnostics to its existing protected journal/log.
        result = subprocess.run(argv, env=env, timeout=1500)
        if result.returncode:
            raise RuntimeError(f'Certbot {"dry-run" if dry_run else "renewal"} failed (exit {result.returncode})')
        return {'certbot': 'dry-run passed' if dry_run else 'renew passed'}


def status() -> dict:
    for unit in ('certbot.timer', 'vydno-tls-dry-run.timer', 'vydno-public-monitor.timer'):
        run(['systemctl', 'is-enabled', unit])
        run(['systemctl', 'is-active', unit])
    command = run(['systemctl', 'show', 'certbot.service', '--property=ExecStart', '--value'])
    if str(CONTROL_ROOT / 'tls_ops.py') not in command or 'renew' not in command:
        raise RuntimeError('Certbot service is not using the persistent renewal controller')
    mounts = json.loads(run(['docker', 'inspect', NGINX, '--format', '{{json .Mounts}}']))
    if not any(m['Destination'] == '/var/www/certbot' and m['Source'] == str(ACME_ROOT) and not m['RW'] for m in mounts):
        raise RuntimeError('Nginx ACME mount is not the persistent read-only host directory')
    return {'controls': 'active', 'acme_root': str(ACME_ROOT)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('acme-check', 'post-renew', 'renew', 'dry-run', 'status'))
    args = parser.parse_args(argv)
    try:
        if args.command == 'acme-check':
            result = acme_check()
        elif args.command == 'post-renew':
            before_path = CONTROL_ROOT / 'before.json'
            before = json.loads(before_path.read_text(encoding='utf-8')) if before_path.exists() else {}
            kwargs = {'before': before, 'allow_unchanged': os.environ.get('VYDNO_TLS_DRY_RUN') == '1',
                      'renewed_domains': os.environ.get('RENEWED_DOMAINS', '').split()}
            if os.environ.get('VYDNO_TLS_LOCK_HELD') == '1':
                result = post_renew(**kwargs)
            else:
                with deployment_lock():
                    result = post_renew(**kwargs)
        elif args.command in ('renew', 'dry-run'):
            result = renew(dry_run=args.command == 'dry-run')
        else:
            result = status()
    except Exception as exc:
        print(json.dumps({'ok': False, 'operation': args.command, 'error_type': type(exc).__name__}), file=sys.stderr)
        return 1
    print(json.dumps({'ok': True, **result}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
