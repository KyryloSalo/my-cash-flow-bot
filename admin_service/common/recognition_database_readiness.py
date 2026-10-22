"""Fail-closed proof that the worker uses the verified PostgreSQL container."""
from __future__ import annotations


def _same_address(value, verified_addresses):
    normalized = {str(address).split('/', 1)[0] for address in verified_addresses}
    return str(value).split('/', 1)[0] in normalized


def check_connection(connection, verified_addresses, verified_port, verified_server_port=None):
    """Return true only for a direct PostgreSQL wire connection to the target."""
    verified_server_port = verified_port if verified_server_port is None else verified_server_port
    try:
        if hasattr(connection, "ensure_connection"):
            connection.ensure_connection()
        raw = getattr(connection, "connection", None) or connection
        if getattr(connection, "vendor", None) != "postgresql" and getattr(raw, "vendor", None) != "postgresql":
            return False
        info = getattr(raw, "info", None)
        if info is None or not getattr(info, "host", None):
            return False
        if not _same_address(getattr(info, "hostaddr", None), verified_addresses):
            return False
        if int(getattr(info, "port", 0)) != int(verified_port):
            return False
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT inet_server_addr()::text, inet_server_port(), "
                "current_setting('server_version_num')"
            )
            row = cursor.fetchone()
        if not row or not _same_address(row[0], verified_addresses):
            return False
        if int(row[1]) != int(verified_server_port):
            return False
        return str(row[2]).startswith("16")
    except Exception:
        return False


def worker_database_ready(verified_addresses, verified_port):
    from django.db import connection
    return check_connection(connection, set(verified_addresses), verified_port)


if __name__ == "__main__":
    import os
    import sys
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django
    django.setup()
    addresses = {value for value in sys.argv[1].split(",") if value}
    port = int(sys.argv[2])
    server_port = int(sys.argv[3]) if len(sys.argv) > 3 else port
    ready = check_connection(__import__('django.db', fromlist=['connection']).connection, addresses, port, server_port)
    print("READY" if ready else "NOT_READY")
    raise SystemExit(0 if ready else 1)