import uuid

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("users", "0010_userauthidentity")]

    operations = [
        migrations.CreateModel(
            name="UserAuthSession",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("token_hash", models.CharField(max_length=64, unique=True)),
                ("auth_mode", models.CharField(max_length=32)),
                ("user_agent_hash", models.CharField(blank=True, default="", max_length=64)),
                ("expires_at", models.DateTimeField()),
                ("last_seen_at", models.DateTimeField()),
                ("revoked_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "telegram_user",
                    models.ForeignKey(
                        db_column="telegram_user_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="auth_sessions",
                        to="users.telegramuser",
                    ),
                ),
                (
                    "identity",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="auth_sessions",
                        to="users.userauthidentity",
                    ),
                ),
            ],
            options={"db_table": "user_auth_sessions"},
        ),
        migrations.AddIndex(
            model_name="userauthsession",
            index=models.Index(
                fields=["telegram_user", "revoked_at"],
                name="user_auth_active_idx",
            ),
        ),
    ]
