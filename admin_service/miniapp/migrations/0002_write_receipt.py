from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("miniapp", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="WriteReceipt",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("operation", models.CharField(max_length=64)),
                ("idempotency_key", models.CharField(max_length=128)),
                ("draft_id", models.CharField(max_length=128)),
                ("status", models.CharField(default="pending", max_length=32)),
                ("result", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "miniapp_write_receipts",
                "ordering": ("-updated_at",),
            },
        ),
        migrations.AddConstraint(
            model_name="writereceipt",
            constraint=models.UniqueConstraint(
                fields=("tg_user_id", "operation", "idempotency_key"),
                name="miniapp_write_receipt_key_unique",
            ),
        ),
    ]
