import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("users", "0009_alter_useradminstate_options")]

    operations = [
        migrations.CreateModel(
            name="UserAuthIdentity",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "provider",
                    models.CharField(
                        choices=[
                            ("telegram_oidc", "Telegram OIDC"),
                            ("telegram_miniapp", "Telegram Mini App"),
                        ],
                        max_length=32,
                    ),
                ),
                ("subject", models.CharField(max_length=255)),
                ("profile", models.JSONField(blank=True, default=dict)),
                ("first_authenticated_at", models.DateTimeField()),
                ("last_authenticated_at", models.DateTimeField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "telegram_user",
                    models.ForeignKey(
                        db_column="telegram_user_id",
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="auth_identities",
                        to="users.telegramuser",
                    ),
                ),
            ],
            options={"db_table": "user_auth_identities"},
        ),
        migrations.AddConstraint(
            model_name="userauthidentity",
            constraint=models.UniqueConstraint(
                fields=("provider", "subject"),
                name="uniq_user_auth_provider_subject",
            ),
        ),
        migrations.AddConstraint(
            model_name="userauthidentity",
            constraint=models.UniqueConstraint(
                fields=("provider", "telegram_user"),
                name="uniq_user_auth_provider_user",
            ),
        ),
    ]
