from __future__ import annotations

import uuid

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0007_acquisition_funnel"),
        ("subscriptions", "0008_alter_model_options"),
        ("users", "0012_useroidctokenuse"),
    ]

    operations = [
        migrations.CreateModel(
            name="BillingConsent",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("idempotency_key", models.UUIDField()),
                ("offer_id", models.CharField(max_length=128)),
                ("offer_version", models.CharField(max_length=64)),
                ("bind_amount_minor", models.PositiveIntegerField()),
                ("currency", models.CharField(max_length=3)),
                ("trial_days", models.PositiveSmallIntegerField()),
                ("renewal_amount_minor", models.PositiveIntegerField()),
                ("renewal_period_days", models.PositiveSmallIntegerField()),
                ("renewal_anchor", models.CharField(default="payment_success", max_length=32)),
                ("first_renewal_at", models.DateTimeField(blank=True, null=True)),
                ("terms_version", models.CharField(max_length=64)),
                ("privacy_version", models.CharField(max_length=64)),
                ("copy_locale", models.CharField(max_length=8)),
                ("acquisition_snapshot", models.JSONField(blank=True, default=dict)),
                ("payload_hash", models.CharField(max_length=64)),
                ("accepted_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "acquisition_session",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="billing_consents",
                        to="miniapp.acquisitionsession",
                    ),
                ),
                (
                    "payment",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="billing_consents",
                        to="subscriptions.payment",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        db_column="telegram_user_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="billing_consents",
                        to="users.telegramuser",
                    ),
                ),
            ],
            options={
                "db_table": "billing_consents",
                "ordering": ("-accepted_at",),
                "indexes": [
                    models.Index(fields=["user", "accepted_at"], name="consent_user_time_idx"),
                    models.Index(fields=["payment", "accepted_at"], name="consent_payment_time_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("user", "idempotency_key"),
                        name="billing_consent_user_key_unique",
                    ),
                    models.CheckConstraint(
                        condition=(
                            models.Q(("bind_amount_minor__gt", 0))
                            & models.Q(("trial_days__gt", 0))
                            & models.Q(("renewal_amount_minor__gt", 0))
                            & models.Q(("renewal_period_days__gt", 0))
                        ),
                        name="billing_consent_positive_terms",
                    ),
                ],
            },
        ),
    ]
