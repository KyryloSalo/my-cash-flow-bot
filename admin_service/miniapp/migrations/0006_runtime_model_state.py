from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0005_confirmation_intent_integrity")]

    operations = [
        migrations.CreateModel(
            name="AiTransactionDraft",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("source", models.TextField()),
                ("telegram_file_unique_id", models.TextField(blank=True, null=True)),
                ("status", models.TextField(default="pending")),
                ("transaction_date", models.DateField(blank=True, null=True)),
                ("tx_type", models.TextField(blank=True, null=True)),
                ("amount", models.DecimalField(blank=True, decimal_places=2, max_digits=18, null=True)),
                ("currency", models.TextField(blank=True, null=True)),
                ("account_id", models.BigIntegerField(blank=True, null=True)),
                ("category_id", models.BigIntegerField(blank=True, null=True)),
                ("comment", models.TextField(blank=True, null=True)),
                ("confidence", models.DecimalField(blank=True, decimal_places=4, max_digits=5, null=True)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField()),
                ("updated_at", models.DateTimeField()),
                ("confirmed_at", models.DateTimeField(blank=True, null=True)),
                ("cancelled_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={"db_table": "ai_transaction_drafts", "ordering": ("-updated_at", "-id"), "managed": False},
        ),
        migrations.CreateModel(
            name="DailyExpenseReminderSetting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("mode", models.CharField(default="daily", max_length=16)),
                ("reminder_hour", models.IntegerField(default=21)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Daily expense reminder setting",
                "verbose_name_plural": "Daily expense reminder settings",
                "db_table": "daily_expense_reminder_settings",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="SavingPromptSetting",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("enabled", models.BooleanField(default=True)),
                ("default_percent", models.DecimalField(decimal_places=2, default=10, max_digits=5)),
                ("secondary_percent", models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True)),
                ("default_target_account_id", models.BigIntegerField(blank=True, null=True)),
                ("ask_after_income", models.BooleanField(default=True)),
                ("ask_only_for_salary", models.BooleanField(default=False)),
                ("min_income_amount", models.DecimalField(blank=True, decimal_places=2, max_digits=18, null=True)),
                ("reminder_enabled", models.BooleanField(default=True)),
                ("default_reminder_delay", models.TextField(blank=True, null=True)),
                ("playful_tone_enabled", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Saving prompt setting",
                "verbose_name_plural": "Saving prompt settings",
                "db_table": "saving_prompt_settings",
                "managed": False,
            },
        ),
    ]
