from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="Broadcast",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("title", models.CharField(max_length=255)),
                ("message_text", models.TextField()),
                ("parse_mode", models.CharField(choices=[("none", "None"), ("HTML", "HTML"), ("Markdown", "Markdown")], default="none", max_length=16)),
                ("segment_filter", models.JSONField(blank=True, default=dict)),
                ("status", models.CharField(choices=[("draft", "Draft"), ("scheduled", "Scheduled"), ("sending", "Sending"), ("completed", "Completed"), ("failed", "Failed"), ("cancelled", "Cancelled")], default="draft", max_length=32)),
                ("total_recipients", models.PositiveIntegerField(default=0)),
                ("sent_count", models.PositiveIntegerField(default=0)),
                ("failed_count", models.PositiveIntegerField(default=0)),
                ("blocked_count", models.PositiveIntegerField(default=0)),
                ("skipped_count", models.PositiveIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("scheduled_at", models.DateTimeField(blank=True, null=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                ("last_error", models.TextField(blank=True, default="")),
                ("created_by", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="broadcasts", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "db_table": "broadcasts",
                "verbose_name": "Broadcast",
                "verbose_name_plural": "Broadcasts",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="AdminMessageLog",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("message_text", models.TextField()),
                ("parse_mode", models.CharField(choices=[("none", "None"), ("HTML", "HTML"), ("Markdown", "Markdown")], default="none", max_length=16)),
                ("send_test_to_admin_first", models.BooleanField(default=False)),
                ("target_chat_id", models.BigIntegerField()),
                ("status", models.CharField(choices=[("queued", "Queued"), ("sent", "Sent"), ("failed", "Failed"), ("blocked", "Blocked")], default="queued", max_length=32)),
                ("response_payload", models.JSONField(blank=True, default=dict)),
                ("error_message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
                ("admin_user", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="admin_message_logs", to=settings.AUTH_USER_MODEL)),
                ("telegram_user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="admin_messages", to="users.telegramuser")),
            ],
            options={
                "db_table": "admin_message_log",
                "verbose_name": "Admin Message Log",
                "verbose_name_plural": "Admin Message Logs",
                "ordering": ("-created_at",),
            },
        ),
        migrations.CreateModel(
            name="BroadcastRecipient",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("pending", "Pending"), ("sent", "Sent"), ("failed", "Failed"), ("blocked", "Blocked"), ("skipped", "Skipped")], default="pending", max_length=32)),
                ("error_message", models.TextField(blank=True, default="")),
                ("sent_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("broadcast", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="recipients", to="broadcasts.broadcast")),
                ("user", models.ForeignKey(db_column="telegram_user_id", on_delete=models.deletion.CASCADE, related_name="broadcast_recipients", to="users.telegramuser")),
            ],
            options={
                "db_table": "broadcast_recipients",
                "verbose_name": "Broadcast Recipient",
                "verbose_name_plural": "Broadcast Recipients",
                "ordering": ("id",),
                "unique_together": {("broadcast", "user")},
            },
        ),
    ]
