from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("accounts", "0001_initial"),
        ("categories", "0001_initial"),
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Transaction",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("date", models.DateField()),
                ("type", models.TextField()),
                ("amount", models.DecimalField(decimal_places=2, max_digits=18)),
                ("currency", models.TextField()),
                ("to_amount", models.DecimalField(blank=True, decimal_places=2, max_digits=18, null=True)),
                ("to_currency", models.TextField(blank=True, null=True)),
                ("fx_rate", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("comment", models.TextField(blank=True, null=True)),
                ("source", models.TextField()),
                ("flow_kind", models.TextField(default="normal")),
                ("counterparty", models.TextField(blank=True, null=True)),
                ("debt_action", models.TextField(blank=True, null=True)),
                ("category_name_snapshot", models.TextField(blank=True, null=True)),
                ("created_at", models.DateTimeField()),
                ("account", models.ForeignKey(blank=True, db_column="account_id", null=True, on_delete=models.deletion.SET_NULL, related_name="legacy_account_transactions", to="accounts.account")),
                ("category", models.ForeignKey(blank=True, db_column="category_id", null=True, on_delete=models.deletion.SET_NULL, related_name="transactions", to="categories.category")),
                ("from_account", models.ForeignKey(blank=True, db_column="from_account_id", null=True, on_delete=models.deletion.SET_NULL, related_name="outgoing_transfers", to="accounts.account")),
                ("tg_user", models.ForeignKey(db_column="tg_user_id", on_delete=models.deletion.DO_NOTHING, related_name="transactions", to="users.telegramuser")),
                ("to_account", models.ForeignKey(blank=True, db_column="to_account_id", null=True, on_delete=models.deletion.SET_NULL, related_name="incoming_transfers", to="accounts.account")),
            ],
            options={
                "db_table": "transactions",
                "managed": False,
                "verbose_name": "Transaction",
                "verbose_name_plural": "Transactions",
                "ordering": ("-created_at",),
            },
        ),
    ]
