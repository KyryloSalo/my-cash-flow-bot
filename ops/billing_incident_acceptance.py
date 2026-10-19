#!/usr/bin/env python
"""Read-only entitlement audit. Run inside the configured admin runtime.

No card fields, provider payloads, personal data or account identifiers are
printed. This tool never calls a payment provider or changes a subscription.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-only", action="store_true", required=True)
    args = parser.parse_args()
    if not args.read_only:
        parser.error("--read-only is required")
    root = Path(__file__).resolve().parents[1]
    if (root / "admin_service").is_dir():
        sys.path.insert(0, str(root / "admin_service"))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django
    django.setup()
    from django.db import connection, transaction
    from django.utils import timezone
    from miniapp.services import resolve_access
    from subscriptions.models import Payment, Subscription
    from users.models import UserAdminState

    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
        now = timezone.now()
        seen = set()
        counts = Counter()
        for sub in Subscription.objects.select_related("user").order_by("user_id", "-created_at", "-pk"):
            if sub.user_id in seen:
                continue
            seen.add(sub.user_id)
            counts["users_with_subscription"] += 1
            if sub.status not in ("trial", "active", "paid", "manual", "lifetime"):
                continue
            if sub.expires_at is not None and sub.expires_at <= now:
                continue
            if UserAdminState.objects.filter(telegram_user_id=sub.user_id, is_blocked=True).exists() or UserAdminState.objects.filter(telegram_user_id=sub.user_id, status="banned").exists():
                counts["operator_blocked"] += 1
                continue
            confirmed_activation = Payment.objects.filter(subscription=sub, user_id=sub.user_id, provider="monobank", status="paid", kind__in=("bind", "retry", "renewal")).exists()
            entitled = sub.status != "trial" or sub.source == "admin" or confirmed_activation
            access = resolve_access(sub.user)
            if entitled:
                counts["valid_activated_entitlements"] += 1
                counts["active_entitlements" if access["mode"] == "active" else "unexpectedly_blocked_entitlements"] += 1
            else:
                counts["unactivated_trials"] += 1
        result = {"check": "billing_incident_read_only", "read_only": True, "counts": dict(counts), "passed": counts["unexpectedly_blocked_entitlements"] == 0}
        print(json.dumps(result, sort_keys=True))
        return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
