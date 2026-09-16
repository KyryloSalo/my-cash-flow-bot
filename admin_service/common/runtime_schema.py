"""Django adapter for the shared core-only schema package."""
from pathlib import Path
import sys

from django.db import connections, router, transaction

try:
    from runtime_schema import CORE_TABLES as CORE_TABLES, bootstrap
except ModuleNotFoundError as exc:
    if exc.name != "runtime_schema":
        raise
    # Source checkout: sibling of admin_service. Images copy it into /app.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from runtime_schema import CORE_TABLES as CORE_TABLES, bootstrap


def ensure_runtime_schema(using="default"):
    connection = connections[using]
    if connection.vendor != "postgresql":
        # SQLite-only unit suites do not claim runtime-schema parity.
        return
    if not router.allow_migrate(using, "users", model_name="telegramuser"):
        return
    with transaction.atomic(using=using), connection.cursor() as cursor:
        bootstrap(cursor.execute)


def bootstrap_before_migrate(sender, using, **kwargs):
    ensure_runtime_schema(using)
