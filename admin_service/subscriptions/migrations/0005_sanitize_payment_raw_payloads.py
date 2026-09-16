from __future__ import annotations

from django.db import migrations


REDACTED = "[redacted]"
SENSITIVE_KEYS = frozenset({"cardtoken", "pan"})


def _sanitize(value):
    if isinstance(value, dict):
        sanitized = {}
        for key, item in value.items():
            if str(key).lower() in SENSITIVE_KEYS:
                sanitized[key] = REDACTED if item not in (None, "") else item
            else:
                sanitized[key] = _sanitize(item)
        return sanitized
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize(item) for item in value)
    return value


def sanitize_payment_raw_payloads(apps, schema_editor):
    Payment = apps.get_model("subscriptions", "Payment")
    for payment in Payment.objects.all().iterator():
        raw_payload = payment.raw_payload or {}
        sanitized = _sanitize(raw_payload)
        if sanitized != raw_payload:
            Payment.objects.filter(pk=payment.pk).update(raw_payload=sanitized)


class Migration(migrations.Migration):
    dependencies = [
        ("subscriptions", "0004_promo_offers_and_access_claims"),
    ]

    operations = [
        migrations.RunPython(sanitize_payment_raw_payloads, migrations.RunPython.noop),
    ]
