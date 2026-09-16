from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0004_web_push_notifications")]

    operations = [
        migrations.CreateModel(
            name="DraftAction",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("draft_id", models.CharField(max_length=128)),
                ("operation", models.CharField(max_length=64)),
                ("payload_hash", models.CharField(max_length=64)),
                ("status", models.CharField(default="draft", max_length=32)),
                ("result", models.JSONField(blank=True, default=dict)),
                ("expires_at", models.DateTimeField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "miniapp_draft_actions",
                "constraints": [
                    models.UniqueConstraint(fields=("tg_user_id", "draft_id"), name="miniapp_draft_action_unique"),
                ],
            },
        ),
    ]
