from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AdminNotificationLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("event_type", models.CharField(max_length=64)),
                ("telegram_chat_id", models.BigIntegerField()),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("status", models.CharField(choices=[("queued", "Queued"), ("sent", "Sent"), ("failed", "Failed")], default="queued", max_length=16)),
                ("error_message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "db_table": "admin_notification_logs",
                "verbose_name": "Admin Notification Log",
                "verbose_name_plural": "Admin Notification Logs",
                "ordering": ("-created_at",),
            },
        ),
    ]
