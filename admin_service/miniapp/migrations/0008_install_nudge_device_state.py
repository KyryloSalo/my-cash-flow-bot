from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0007_acquisition_funnel"),
    ]

    operations = [
        migrations.AddField(
            model_name="installnudgestate",
            name="manual_confirmed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name="InstallNudgeDeviceState",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("device_hash", models.CharField(max_length=64)),
                ("platform", models.CharField(blank=True, default="", max_length=24)),
                ("prompt_count", models.PositiveSmallIntegerField(default=0)),
                ("next_prompt_at", models.DateTimeField(blank=True, null=True)),
                ("last_prompted_at", models.DateTimeField(blank=True, null=True)),
                ("manual_confirmed_at", models.DateTimeField(blank=True, null=True)),
                ("installed_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "miniapp_install_nudge_device_states",
                "ordering": ("-updated_at",),
                "constraints": [
                    models.UniqueConstraint(
                        fields=("tg_user_id", "device_hash"),
                        name="miniapp_install_device_unique",
                    )
                ],
            },
        ),
    ]
