"""Bounded concurrency; client <= 90s, gunicorn 120s, nginx 130s."""
import os
bind = "0.0.0.0:8080"
worker_class = "gthread"
workers = 2
threads = 4
timeout = 120
graceful_timeout = 120
keepalive = 5
max_requests = 1000
max_requests_jitter = 100
# Fail startup rather than silently accepting incompatible application overrides.
for name, default in [("MINIAPP_EXPORT_INTERNAL_TIMEOUT_SECONDS",90), ("MINIAPP_BOT_INTERNAL_AI_TIMEOUT_SECONDS",75)]:
    if not 0 < int(os.environ.get(name,default)) <= 90:
        raise RuntimeError("Application HTTP deadline must be between 1 and 90 seconds")
