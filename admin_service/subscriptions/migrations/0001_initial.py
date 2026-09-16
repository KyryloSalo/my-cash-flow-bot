from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Subscription",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("plan", models.CharField(default="solo", max_length=64)),
                ("status", models.CharField(choices=[("trial", "Trial"), ("paid", "Paid"), ("expired", "Expired"), ("cancelled", "Cancelled")], default="trial", max_length=32)),
                ("provider", models.CharField(blank=True, default="", max_length=64)),
                ("amount", models.DecimalField(blank=True, decimal_places=2, max_digits=12, null=True)),
                ("currency", models.CharField(blank=True, default="", max_length=16)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("expires_at", models.DateTimeField(blank=True, null=True)),
                ("payment_id", models.CharField(blank=True, default="", max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="subscriptions", to="users.telegramuser")),
            ],
            options={
                "db_table": "subscriptions",
                "verbose_name": "Subscription",
                "verbose_name_plural": "Subscriptions",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="Payment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("provider", models.CharField(blank=True, default="", max_length=64)),
                ("provider_payment_id", models.CharField(blank=True, max_length=255, null=True, unique=True)),
                ("amount", models.DecimalField(decimal_places=2, max_digits=12)),
                ("currency", models.CharField(max_length=16)),
                ("status", models.CharField(choices=[("created", "Created"), ("paid", "Paid"), ("failed", "Failed"), ("refunded", "Refunded")], default="created", max_length=32)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("subscription", models.ForeignKey(blank=True, null=True, on_delete=models.deletion.SET_NULL, related_name="payments", to="subscriptions.subscription")),
                ("user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="payments", to="users.telegramuser")),
            ],
            options={
                "db_table": "payments",
                "verbose_name": "Payment",
                "verbose_name_plural": "Payments",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="SubscriptionEvent",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_type", models.CharField(max_length=64)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("subscription", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="events", to="subscriptions.subscription")),
                ("user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="subscription_events", to="users.telegramuser")),
            ],
            options={
                "db_table": "subscription_events",
                "verbose_name": "Subscription Event",
                "verbose_name_plural": "Subscription Events",
                "ordering": ("-created_at",),
            },
        ),
    ]
