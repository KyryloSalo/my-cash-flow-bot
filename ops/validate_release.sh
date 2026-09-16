#!/usr/bin/env bash
set -euo pipefail
# Only CI/local synthetic configuration, never server .env.
[[ "${OPS_SYNTHETIC_TESTS:-}" == 1 ]] || { printf '%s\n' 'Synthetic validation opt-in required' >&2; exit 2; }
python -c 'from urllib.parse import urlsplit; import os; u=urlsplit(os.environ["DATABASE_URL"]); assert u.hostname in {"127.0.0.1","localhost"} and u.path=="/mcf_ci"'
python -m unittest discover -s ops/tests -v
python -m unittest discover -s bot/tests -v
(
  cd admin_service
  python manage.py migrate --noinput
  python manage.py migrate --noinput
  python manage.py migrate --check
  python manage.py check --deploy --fail-level WARNING
  DJANGO_DEBUG=true python manage.py test --noinput
)
