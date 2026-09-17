from __future__ import annotations

import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0006_runtime_model_state"),
        ("users", "0012_useroidctokenuse"),
    ]

    operations = [
        migrations.CreateModel(
            name="AcquisitionSession",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("funnel_version", models.CharField(default="pwa_v1", max_length=32)),
                ("first_landing_variant", models.CharField(blank=True, default="", max_length=64)),
                ("last_landing_variant", models.CharField(blank=True, default="", max_length=64)),
                ("utm_source", models.CharField(blank=True, default="", max_length=128)),
                ("utm_medium", models.CharField(blank=True, default="", max_length=128)),
                ("utm_campaign", models.CharField(blank=True, default="", max_length=128)),
                ("utm_content", models.CharField(blank=True, default="", max_length=128)),
                ("utm_term", models.CharField(blank=True, default="", max_length=128)),
                ("referral_code", models.CharField(blank=True, default="", max_length=128)),
                ("platform", models.CharField(blank=True, default="", max_length=16)),
                ("container", models.CharField(blank=True, default="", max_length=16)),
                ("device_class", models.CharField(blank=True, default="", max_length=16)),
                (
                    "automation_status",
                    models.CharField(
                        choices=[
                            ("unknown", "Unknown"),
                            ("verified_human", "Verified human"),
                            ("automated", "Automated"),
                        ],
                        db_index=True,
                        default="unknown",
                        max_length=16,
                    ),
                ),
                ("linked_at", models.DateTimeField(blank=True, null=True)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "user",
                    models.ForeignKey(
                        blank=True,
                        db_column="telegram_user_id",
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="acquisition_sessions",
                        to="users.telegramuser",
                    ),
                ),
            ],
            options={
                "db_table": "acquisition_sessions",
                "ordering": ("-created_at",),
                "indexes": [
                    models.Index(fields=["user", "created_at"], name="acq_user_created_idx"),
                    models.Index(fields=["automation_status", "created_at"], name="acq_auto_created_idx"),
                ],
            },
        ),
        migrations.CreateModel(
            name="FunnelEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_name", models.CharField(db_index=True, max_length=64)),
                ("idempotency_key", models.CharField(max_length=160)),
                ("payload_hash", models.CharField(max_length=64)),
                ("source", models.CharField(choices=[("client", "Client"), ("server", "Server")], max_length=16)),
                ("funnel_version", models.CharField(blank=True, default="", max_length=32)),
                ("landing_variant", models.CharField(blank=True, default="", max_length=64)),
                ("offer_variant", models.CharField(blank=True, default="", max_length=64)),
                ("platform", models.CharField(blank=True, default="", max_length=16)),
                ("container", models.CharField(blank=True, default="", max_length=16)),
                ("outcome", models.CharField(blank=True, default="", max_length=64)),
                ("placement", models.CharField(blank=True, default="", max_length=64)),
                ("recorded_at", models.DateTimeField(auto_now_add=True)),
                (
                    "acquisition_session",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="events",
                        to="miniapp.acquisitionsession",
                    ),
                ),
            ],
            options={
                "db_table": "funnel_events",
                "ordering": ("recorded_at", "id"),
                "indexes": [
                    models.Index(fields=["event_name", "recorded_at"], name="funnel_name_time_idx"),
                    models.Index(fields=["acquisition_session", "recorded_at"], name="funnel_session_time_idx"),
                ],
                "constraints": [
                    models.UniqueConstraint(
                        fields=("acquisition_session", "idempotency_key"),
                        name="funnel_event_session_key_unique",
                    )
                ],
            },
        ),
    ]
