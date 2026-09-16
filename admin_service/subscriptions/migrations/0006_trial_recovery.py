from datetime import time

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


def seed_draft_campaign(apps, schema_editor):
    Campaign = apps.get_model("subscriptions", "TrialRecoveryCampaign")
    Campaign.objects.get_or_create(
        name="90-day trial recovery",
        defaults={
            "status": "draft",
            "fallback_timezone": "Europe/Kyiv",
            "send_window_start": time(10, 0),
            "send_window_end": time(19, 0),
        },
    )


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("subscriptions", "0005_sanitize_payment_raw_payloads"),
        ("users", "0005_useradminstate_access_scope"),
    ]

    operations = [
        migrations.CreateModel(
            name="TrialRecoveryCampaign",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(default="90-day trial recovery", max_length=255)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("draft", "Draft"),
                            ("scheduled", "Scheduled"),
                            ("running", "Running"),
                            ("paused", "Paused"),
                            ("completed", "Completed"),
                        ],
                        default="draft",
                        max_length=32,
                    ),
                ),
                ("fallback_timezone", models.CharField(default="Europe/Kyiv", max_length=64)),
                ("send_window_start", models.TimeField(default=time(10, 0))),
                ("send_window_end", models.TimeField(default=time(19, 0))),
                ("launch_at", models.DateTimeField(blank=True, null=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                ("audience_snapshot", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "launched_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="launched_trial_recovery_campaigns",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "db_table": "trial_recovery_campaigns",
                "verbose_name": "Recovery-кампанія trial",
                "verbose_name_plural": "Recovery-кампанії trial",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="TrialRecoveryRecipient",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("scheduled", "Scheduled"),
                            ("responded", "Responded"),
                            ("contact_requested", "Contact requested"),
                            ("converted", "Converted"),
                            ("opted_out", "Opted out"),
                            ("ineligible", "Ineligible"),
                            ("completed", "Completed"),
                            ("delivery_failed", "Delivery failed"),
                        ],
                        default="scheduled",
                        max_length=32,
                    ),
                ),
                ("source", models.CharField(choices=[("backfill", "Backfill"), ("live", "Live")], default="live", max_length=16)),
                (
                    "reason",
                    models.CharField(
                        blank=True,
                        choices=[
                            ("card", "Does not want to link a card"),
                            ("autorenew", "Worried about auto-renewal"),
                            ("value", "Does not see the value"),
                            ("price", "Price after trial is too high"),
                            ("error", "Technical or payment error"),
                            ("later", "Not the right time"),
                            ("other", "Other"),
                        ],
                        default="",
                        max_length=32,
                    ),
                ),
                ("free_text", models.TextField(blank=True, default="")),
                ("timezone", models.CharField(default="Europe/Kyiv", max_length=64)),
                ("offered_at", models.DateTimeField(blank=True, null=True)),
                ("shown_at", models.DateTimeField(blank=True, null=True)),
                ("dismissed_at", models.DateTimeField(blank=True, null=True)),
                ("first_sent_at", models.DateTimeField(blank=True, null=True)),
                ("last_sent_at", models.DateTimeField(blank=True, null=True)),
                ("next_send_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("last_sent_local_date", models.DateField(blank=True, null=True)),
                ("sent_count", models.PositiveSmallIntegerField(default=0)),
                ("responded_at", models.DateTimeField(blank=True, null=True)),
                ("awaiting_text_until", models.DateTimeField(blank=True, null=True)),
                ("contact_requested_at", models.DateTimeField(blank=True, null=True)),
                ("opted_out_at", models.DateTimeField(blank=True, null=True)),
                ("converted_at", models.DateTimeField(blank=True, null=True)),
                ("feedback_item_id", models.BigIntegerField(blank=True, null=True)),
                ("support_case_id", models.BigIntegerField(blank=True, null=True)),
                ("ineligible_reason", models.CharField(blank=True, default="", max_length=128)),
                ("last_error", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "campaign",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="recipients", to="subscriptions.trialrecoverycampaign"),
                ),
                (
                    "user",
                    models.OneToOneField(
                        db_column="telegram_user_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="trial_recovery_recipient",
                        to="users.telegramuser",
                    ),
                ),
            ],
            options={
                "db_table": "trial_recovery_recipients",
                "verbose_name": "Отримувач recovery-кампанії",
                "verbose_name_plural": "Отримувачі recovery-кампанії",
                "ordering": ("next_send_at", "id"),
            },
        ),
        migrations.AddIndex(
            model_name="trialrecoveryrecipient",
            index=models.Index(fields=["campaign", "status", "next_send_at"], name="trialrec_due_idx"),
        ),
        migrations.CreateModel(
            name="TrialRecoveryDelivery",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("step", models.PositiveSmallIntegerField()),
                (
                    "status",
                    models.CharField(
                        choices=[("claimed", "Claimed"), ("sent", "Sent"), ("failed", "Failed"), ("skipped", "Skipped")],
                        default="claimed",
                        max_length=16,
                    ),
                ),
                ("telegram_message_id", models.BigIntegerField(blank=True, null=True)),
                ("error_message", models.TextField(blank=True, default="")),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "recipient",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="deliveries", to="subscriptions.trialrecoveryrecipient"),
                ),
            ],
            options={
                "db_table": "trial_recovery_deliveries",
                "verbose_name": "Доставка recovery-повідомлення",
                "verbose_name_plural": "Доставки recovery-повідомлень",
                "ordering": ("-created_at",),
            },
        ),
        migrations.AddConstraint(
            model_name="trialrecoverydelivery",
            constraint=models.UniqueConstraint(fields=("recipient", "step"), name="trialrec_delivery_step_unique"),
        ),
        migrations.RunPython(seed_draft_campaign, migrations.RunPython.noop),
    ]
