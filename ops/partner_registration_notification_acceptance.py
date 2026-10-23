#!/usr/bin/env python
"""Repeatable local partner-registration acceptance; no real Telegram credentials."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", action="store_true", required=True, help="run isolated PostgreSQL and browser acceptance")
    parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    container = "mcf-partner-notification-" + uuid.uuid4().hex[:12]
    # Inherit OS/tool paths only; never reuse configured application credentials.
    os_keys = {
        "PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "USERPROFILE",
        "HOMEDRIVE", "HOMEPATH", "LOCALAPPDATA", "APPDATA", "PROGRAMDATA",
        "PROGRAMFILES", "PROGRAMFILES(X86)", "TMP", "TEMP", "TMPDIR", "HOME",
        "PLAYWRIGHT_BROWSERS_PATH",
    }
    env = {key: value for key, value in os.environ.items() if key.upper() in os_keys}
    env.update({
        "DJANGO_DEBUG": "true", "PYTHON_DOTENV_DISABLED": "1",
        "SECURE_SSL_REDIRECT": "false", "SESSION_COOKIE_SECURE": "false", "CSRF_COOKIE_SECURE": "false",
        "REDIS_URL": "redis://127.0.0.1:63999/0", "ADMIN_NOTIFICATIONS_ENABLED": "false",
    })
    try:
        subprocess.run([
            "docker", "run", "-d", "--rm", "--name", container,
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_DB=mcf_partner_acceptance",
            "-p", "127.0.0.1::5432", "postgres:16-alpine",
        ], check=True, capture_output=True, text=True)
        port = subprocess.check_output(["docker", "port", container, "5432/tcp"], text=True).strip().rsplit(":", 1)[-1]
        env["DATABASE_URL"] = f"postgresql://postgres@127.0.0.1:{port}/mcf_partner_acceptance"
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if subprocess.run(["docker", "exec", container, "pg_isready", "-U", "postgres"], capture_output=True).returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("isolated PostgreSQL readiness failed")
        subprocess.run([
            sys.executable, "admin_service/manage.py", "test",
            "miniapp.acceptance_registration_notifications", "--noinput", "--verbosity", "1",
        ], cwd=root, env=env, check=True)
        env["PYTHONPATH"] = str(root / "bot")
        subprocess.run([
            sys.executable, "-m", "unittest",
            "tests.test_account_lifecycle_regression", "tests.test_admin_integrations", "-q",
        ], cwd=root, env=env, check=True)
    finally:
        cleanup = subprocess.run(["docker", "rm", "-f", container], capture_output=True)
        if cleanup.returncode != 0:
            raise RuntimeError("isolated acceptance container cleanup failed")
    print("PARTNER_NOTIFICATION_ACCEPTANCE_PASS: real Chromium + PostgreSQL, attributed PWA registration, direct registration, repeat-login deduplication, bot regressions; identity and Telegram delivery simulated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
