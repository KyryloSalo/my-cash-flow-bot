import datetime

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("miniapp", "0003_installnudgestate")]

    operations = [
        migrations.CreateModel(
            name="AppNotification",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("event_type", models.CharField(db_index=True, max_length=64)),
                ("category", models.CharField(db_index=True, max_length=24)),
                ("idempotency_key", models.CharField(max_length=160)),
                ("title", models.CharField(max_length=160)),
                ("body", models.TextField()),
                ("target_url", models.TextField(default="/app/")),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("requires_action", models.BooleanField(default=False)),
                ("contributes_to_badge", models.BooleanField(default=True)),
                ("status", models.CharField(choices=[("unread", "Unread"), ("read", "Read"), ("resolved", "Resolved"), ("dismissed", "Dismissed")], db_index=True, default="unread", max_length=16)),
                ("read_at", models.DateTimeField(blank=True, null=True)),
                ("resolved_at", models.DateTimeField(blank=True, null=True)),
                ("expires_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "miniapp_app_notifications", "ordering": ("-created_at", "-id")},
        ),
        migrations.CreateModel(
            name="NotificationPreference",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True, unique=True)),
                ("master_enabled", models.BooleanField(default=True)),
                ("billing_enabled", models.BooleanField(default=True)),
                ("daily_expenses_enabled", models.BooleanField(default=True)),
                ("savings_enabled", models.BooleanField(default=True)),
                ("debts_enabled", models.BooleanField(default=True)),
                ("family_enabled", models.BooleanField(default=True)),
                ("weekly_summary_enabled", models.BooleanField(default=False)),
                ("show_sensitive_details", models.BooleanField(default=False)),
                ("quiet_hours_enabled", models.BooleanField(default=True)),
                ("quiet_hours_start", models.TimeField(default=datetime.time(22, 0))),
                ("quiet_hours_end", models.TimeField(default=datetime.time(8, 0))),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "miniapp_notification_preferences", "ordering": ("tg_user_id",)},
        ),
        migrations.CreateModel(
            name="WebPushSubscription",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tg_user_id", models.BigIntegerField(db_index=True)),
                ("endpoint", models.TextField(unique=True)),
                ("p256dh", models.TextField()),
                ("auth", models.TextField()),
                ("platform", models.CharField(blank=True, default="", max_length=24)),
                ("device_label", models.CharField(blank=True, default="", max_length=120)),
                ("user_agent", models.TextField(blank=True, default="")),
                ("is_active", models.BooleanField(db_index=True, default=True)),
                ("failure_count", models.PositiveSmallIntegerField(default=0)),
                ("last_success_at", models.DateTimeField(blank=True, null=True)),
                ("last_failure_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"db_table": "miniapp_web_push_subscriptions", "ordering": ("-updated_at",)},
        ),
        migrations.CreateModel(
            name="PushDelivery",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("pending", "Pending"), ("sent", "Sent"), ("failed", "Failed"), ("gone", "Gone")], default="pending", max_length=16)),
                ("response_code", models.IntegerField(blank=True, null=True)),
                ("error", models.TextField(blank=True, default="")),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("notification", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="deliveries", to="miniapp.appnotification")),
                ("subscription", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="deliveries", to="miniapp.webpushsubscription")),
            ],
            options={"db_table": "miniapp_push_deliveries"},
        ),
        migrations.AddConstraint(model_name="appnotification", constraint=models.UniqueConstraint(fields=("tg_user_id", "idempotency_key"), name="miniapp_notification_user_key_unique")),
        migrations.AddIndex(model_name="appnotification", index=models.Index(fields=["tg_user_id", "status", "created_at"], name="miniapp_notice_inbox_idx")),
        migrations.AddIndex(model_name="webpushsubscription", index=models.Index(fields=["tg_user_id", "is_active"], name="miniapp_push_user_active_idx")),
        migrations.AddConstraint(model_name="pushdelivery", constraint=models.UniqueConstraint(fields=("notification", "subscription"), name="miniapp_push_delivery_unique")),
    ]
