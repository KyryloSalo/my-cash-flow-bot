from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Account",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("label", models.TextField()),
                ("currency", models.TextField()),
                ("starting_balance", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("created_at", models.DateTimeField()),
                ("tg_user", models.ForeignKey(db_column="tg_user_id", on_delete=models.deletion.DO_NOTHING, related_name="bot_accounts", to="users.telegramuser")),
            ],
            options={
                "db_table": "accounts",
                "managed": False,
                "verbose_name": "Account",
                "verbose_name_plural": "Accounts",
                "ordering": ("label",),
            },
        ),
        migrations.CreateModel(
            name="AccountAdminState",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("is_default", models.BooleanField(default=False)),
                ("is_active", models.BooleanField(default=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("account", models.OneToOneField(on_delete=models.deletion.CASCADE, related_name="admin_state", to="accounts.account")),
            ],
            options={
                "db_table": "account_admin_states",
                "verbose_name": "Account Admin State",
                "verbose_name_plural": "Account Admin States",
            },
        ),
    ]
