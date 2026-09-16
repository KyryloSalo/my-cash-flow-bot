from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="TelegramUser",
            fields=[
                ("tg_user_id", models.BigIntegerField(db_column="tg_user_id", primary_key=True, serialize=False, verbose_name="Telegram ID")),
                ("first_name", models.TextField(blank=True, null=True)),
                ("username", models.TextField(blank=True, null=True)),
                ("lang", models.TextField(blank=True, null=True)),
                ("base_currency", models.TextField(blank=True, null=True)),
                ("start_date", models.DateField(blank=True, null=True)),
                ("onboarding_completed", models.BooleanField(default=False)),
                ("onboarding_version", models.IntegerField(default=0)),
                ("created_at", models.DateTimeField()),
                ("last_seen_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "users",
                "managed": False,
                "verbose_name": "Telegram User",
                "verbose_name_plural": "Telegram Users",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="UserAdminState",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("active", "Active"), ("banned", "Banned"), ("inactive", "Inactive")], default="active", max_length=32)),
                ("subscription_status", models.CharField(choices=[("none", "None"), ("trial", "Trial"), ("paid", "Paid"), ("expired", "Expired"), ("cancelled", "Cancelled")], default="none", max_length=32)),
                ("timezone", models.CharField(blank=True, default="", max_length=64)),
                ("last_action_at", models.DateTimeField(blank=True, null=True)),
                ("is_blocked", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("telegram_user", models.OneToOneField(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="admin_state", to="users.telegramuser")),
            ],
            options={
                "db_table": "user_admin_states",
                "verbose_name": "User Admin State",
                "verbose_name_plural": "User Admin States",
            },
        ),
        migrations.CreateModel(
            name="AdminNote",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("note_text", models.TextField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("admin_user", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="cashflow_admin_notes", to=settings.AUTH_USER_MODEL)),
                ("telegram_user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="admin_notes", to="users.telegramuser")),
            ],
            options={
                "db_table": "admin_notes",
                "verbose_name": "Admin Note",
                "verbose_name_plural": "Admin Notes",
                "ordering": ("-updated_at",),
            },
        ),
    ]
