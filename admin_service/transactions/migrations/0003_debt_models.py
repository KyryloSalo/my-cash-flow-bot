from __future__ import annotations

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("transactions", "0002_transaction_fx_rate_text"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.CreateModel(
                    name="Debt",
                    fields=[
                        ("id", models.BigAutoField(primary_key=True, serialize=False)),
                        ("counterparty_name", models.TextField()),
                        ("direction", models.TextField()),
                        ("initial_amount", models.DecimalField(decimal_places=2, max_digits=18)),
                        ("paid_amount", models.DecimalField(decimal_places=2, max_digits=18)),
                        ("remaining_amount", models.DecimalField(decimal_places=2, max_digits=18)),
                        ("currency", models.TextField()),
                        ("status", models.TextField()),
                        ("due_date", models.DateField(blank=True, null=True)),
                        ("comment", models.TextField(blank=True, null=True)),
                        ("created_at", models.DateTimeField()),
                        ("updated_at", models.DateTimeField()),
                        ("closed_at", models.DateTimeField(blank=True, null=True)),
                        (
                            "tg_user",
                            models.ForeignKey(
                                db_column="tg_user_id",
                                on_delete=models.deletion.DO_NOTHING,
                                related_name="debts",
                                to="users.telegramuser",
                            ),
                        ),
                        (
                            "account",
                            models.ForeignKey(
                                blank=True,
                                db_column="account_id",
                                null=True,
                                on_delete=models.deletion.SET_NULL,
                                related_name="debts",
                                to="accounts.account",
                            ),
                        ),
                    ],
                    options={
                        "db_table": "debts",
                        "managed": False,
                        "verbose_name": "Debt",
                        "verbose_name_plural": "Debts",
                        "ordering": ("-created_at", "-id"),
                    },
                ),
                migrations.CreateModel(
                    name="DebtPayment",
                    fields=[
                        ("id", models.BigAutoField(primary_key=True, serialize=False)),
                        ("amount", models.DecimalField(decimal_places=2, max_digits=18)),
                        ("currency", models.TextField()),
                        ("payment_date", models.DateField()),
                        ("comment", models.TextField(blank=True, null=True)),
                        ("created_at", models.DateTimeField()),
                        (
                            "tg_user",
                            models.ForeignKey(
                                db_column="tg_user_id",
                                on_delete=models.deletion.DO_NOTHING,
                                related_name="debt_payments",
                                to="users.telegramuser",
                            ),
                        ),
                        (
                            "debt",
                            models.ForeignKey(
                                db_column="debt_id",
                                on_delete=models.deletion.CASCADE,
                                related_name="payments",
                                to="transactions.debt",
                            ),
                        ),
                        (
                            "account",
                            models.ForeignKey(
                                blank=True,
                                db_column="account_id",
                                null=True,
                                on_delete=models.deletion.SET_NULL,
                                related_name="debt_payments",
                                to="accounts.account",
                            ),
                        ),
                    ],
                    options={
                        "db_table": "debt_payments",
                        "managed": False,
                        "verbose_name": "Debt payment",
                        "verbose_name_plural": "Debt payments",
                        "ordering": ("-created_at", "-id"),
                    },
                ),
                migrations.AddField(
                    model_name="transaction",
                    name="debt",
                    field=models.ForeignKey(
                        blank=True,
                        db_column="debt_id",
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="transactions",
                        to="transactions.debt",
                    ),
                ),
                migrations.AddField(
                    model_name="transaction",
                    name="debt_payment",
                    field=models.ForeignKey(
                        blank=True,
                        db_column="debt_payment_id",
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="transactions",
                        to="transactions.debtpayment",
                    ),
                ),
                migrations.AddField(
                    model_name="transaction",
                    name="exchange_rate",
                    field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
                ),
                migrations.AddField(
                    model_name="transaction",
                    name="original_amount",
                    field=models.DecimalField(blank=True, decimal_places=2, max_digits=18, null=True),
                ),
                migrations.AddField(
                    model_name="transaction",
                    name="original_currency",
                    field=models.TextField(blank=True, null=True),
                ),
            ],
        ),
    ]
