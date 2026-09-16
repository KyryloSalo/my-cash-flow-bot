"""No external APIs, financial writes or customer data in readiness checks."""
from __future__ import annotations
import time

BEAT_KEY = "vydno:health:beat"
QUEUE_KEY = "vydno:health:queue"


def _redis_client():
    from django.conf import settings
    from redis import Redis
    return Redis.from_url(settings.REDIS_URL, socket_connect_timeout=1, socket_timeout=1)


def _database():
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
        if cursor.fetchone()[0] != 1:
            return False
    executor = MigrationExecutor(connection)
    if executor.migration_plan(executor.loader.graph.leaf_nodes()):
        return False
    return {'users','accounts','transactions','feedback_items_tags'} <= set(connection.introspection.table_names())


def _redis():
    return bool(_redis_client().ping())


def _worker():
    from config.celery import app
    responses = app.control.inspect(timeout=1).ping() or {}
    return any(value.get('ok') == 'pong' for value in responses.values())


def _fresh(key, limit):
    value = _redis_client().get(key)
    if value is None:
        return False
    age = time.time() - float(value)
    return 0 <= age <= limit


def _beat():
    return _fresh(BEAT_KEY, 60)


def _queue_lag():
    # A short-expiry no-op must be consumed on the default queue every 30s.
    # This is bounded queue progress, not an exact oldest-message-age metric.
    return _fresh(QUEUE_KEY, 120)


def _billing_lag():
    from datetime import timedelta
    from django.utils import timezone
    from subscriptions.models import Payment, Subscription
    cutoff=timezone.now()-timedelta(minutes=20)
    return not (
        Payment.objects.filter(provider='monobank',status='pending',created_at__lt=cutoff).exists()
        or Subscription.objects.filter(provider='monobank',auto_renew=True,next_charge_at__lt=cutoff).exists()
    )


def readiness_status(*, checks=None):
    checks = checks if checks is not None else {
        'database': _database, 'redis': _redis, 'worker': _worker,
        'beat': _beat, 'queue_lag': _queue_lag, 'billing_lag': _billing_lag,
    }
    results = {}
    for name, check in checks.items():
        try:
            results[name] = 'ok' if check() else 'failed'
        except Exception:
            results[name] = 'failed'
    return {'ready': bool(results) and all(v=='ok' for v in results.values()), 'checks':results}


def ready(request):
    from django.http import JsonResponse
    result=readiness_status()
    return JsonResponse(result,status=200 if result['ready'] else 503)


def live(request):
    from django.http import JsonResponse
    return JsonResponse({'alive':True})


if __name__ == '__main__':
    import os, sys
    os.environ.setdefault('DJANGO_SETTINGS_MODULE','config.settings')
    import django
    django.setup()
    mode=sys.argv[1] if len(sys.argv)>1 else 'ready'
    if mode=='beat':
        result=readiness_status(checks={'redis':_redis,'beat':_beat})
    elif mode=='worker':
        result=readiness_status(checks={'redis':_redis,'worker':_worker})
    else:
        result=readiness_status()
    import json
    print(json.dumps(result))
    raise SystemExit(0 if result['ready'] else 1)
